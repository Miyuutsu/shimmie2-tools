# pylint: disable=line-too-long disable=too-many-locals disable=too-many-branches disable=too-many-statements disable=too-many-arguments disable=too-many-positional-arguments
"""Bridge script for integrating the SD-Tag-Editor submodule safely via JSON."""
import os
import sqlite3
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
import orjson
import psycopg2
import psycopg2.extras
import tqdm

from functions.db_cache import get_shimmie_db_credentials
from functions.tags_curation import rating_from_score

SUBMODULE_PATH = Path(__file__).parent.parent / "SD-Tag-Editor"
STAGING_DB = Path(__file__).parent.parent / "database" / "ai_audit_staging.db"
QUARANTINE_DB = Path(__file__).parent.parent / "database" / "ai_quarantine_cache.db"
REVERT_DB = Path(__file__).parent.parent / "database" / "ai_revert_log.db"

def _init_databases():
    """Initializes the quarantine, staging, and revert databases with full schema and indexes."""
    STAGING_DB.parent.mkdir(parents=True, exist_ok=True)

    # Quarantine: Stores AI tags with confidence scores
    q_conn = sqlite3.connect(QUARANTINE_DB)
    q_conn.execute("""
        CREATE TABLE IF NOT EXISTS ai_tags (
            image_hash TEXT, tag TEXT, confidence REAL, UNIQUE(image_hash, tag)
        )
    """)
    q_conn.execute("CREATE INDEX IF NOT EXISTS idx_ai_tags_hash ON ai_tags(image_hash)")

    # Staging: Proposals awaiting application
    s_conn = sqlite3.connect(STAGING_DB)
    s_conn.execute("""
        CREATE TABLE IF NOT EXISTS pending_upgrades (
            image_id INTEGER PRIMARY KEY, image_hash TEXT, current_rating TEXT, new_rating TEXT
        )
    """)
    s_conn.execute("""
        CREATE TABLE IF NOT EXISTS pending_tags (
            image_id INTEGER, tag TEXT, UNIQUE(image_id, tag)
        )
    """)
    s_conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_tags_id ON pending_tags(image_id)")

    # Revert Ledger: Records for rollback
    r_conn = sqlite3.connect(REVERT_DB)
    r_conn.execute("CREATE TABLE IF NOT EXISTS runs (batch_id TEXT PRIMARY KEY, ts DATETIME DEFAULT CURRENT_TIMESTAMP)")
    r_conn.execute("CREATE TABLE IF NOT EXISTS rating_reverts (batch_id TEXT, image_id INTEGER, old_rating TEXT)")
    r_conn.execute("CREATE TABLE IF NOT EXISTS tag_reverts (batch_id TEXT, image_id INTEGER, tag_id INTEGER)")

    return q_conn, s_conn, r_conn

def _get_safe_images(pg_cur, all_ratings=False):
    """Optimized fetch using a single JOIN, completely eliminating N+1 query stalls."""
    query = """
        SELECT i.id, i.hash, i.rating, t.tag
        FROM images i
        LEFT JOIN image_tags it ON i.id = it.image_id
        LEFT JOIN tags t ON it.tag_id = t.id
    """
    if not all_ratings:
        query += " WHERE i.rating = 's'"

    pg_cur.execute(query)

    safe_images = {}
    for img_id, img_hash, rating, tag in pg_cur.fetchall():
        if img_hash not in safe_images:
            safe_images[img_hash] = {"id": img_id, "rating": rating, "tags": set()}
        if tag:
            safe_images[img_hash]["tags"].add(tag)

    return safe_images

def _resolve_shimmie_thumb_path(base_dir: Path, md5_hash: str) -> Path:
    return base_dir / md5_hash[0:2] / md5_hash[2:4] / md5_hash

def _ensure_submodule_installed():
    venv_python = SUBMODULE_PATH / "venv" / "bin" / "python"
    if not venv_python.exists():
        print("[INFO] SD-Tag-Editor venv not found. Running install.sh...")
        subprocess.run(["bash", "install.sh"], cwd=SUBMODULE_PATH, check=True)
    return venv_python

def _ensure_tag_exists(pg_cur, tag_name):
    pg_cur.execute("SELECT id FROM tags WHERE tag = %s", (tag_name,))
    row = pg_cur.fetchone()
    if row:
        return row[0]
    pg_cur.execute("INSERT INTO tags (tag, count) VALUES (%s, 0) RETURNING id", (tag_name,))
    return pg_cur.fetchone()[0]

def _run_scan(args, pg_cur, s_cur, q_cur):
    """Executes the AI model and saves proposals to the staging DB."""
    venv_python = _ensure_submodule_installed()

    print(f"Fetching {'all' if args.all else 'Safe'} images from Shimmie...")
    safe_images = _get_safe_images(pg_cur, all_ratings=args.all)
    print(f"Found {len(safe_images)} images to audit.")

    thumbs_base_dir = Path(args.thumbs) if args.thumbs else Path(args.spath) / "data" / "thumbs"
    if not thumbs_base_dir.exists():
        print(f"[ERROR] Thumbnails directory not found at {thumbs_base_dir}")
        return

    with tempfile.TemporaryDirectory(prefix="ai_audit_") as temp_dir_str:
        staging_dir = Path(temp_dir_str)
        staged_count = 0

        print(f"Staging read-only symlinks to {staging_dir}...")
        for img_hash in tqdm.tqdm(safe_images, desc="Staging Symlinks"):
            real_thumb = _resolve_shimmie_thumb_path(thumbs_base_dir, img_hash).resolve() # Force absolute
            if real_thumb.exists():
                os.symlink(str(real_thumb), str(staging_dir / f"{img_hash}.jpg"))
                staged_count += 1

        if staged_count == 0:
            print("[ERROR] No thumbnails found matching the database records.")
            return

        tagger_script = SUBMODULE_PATH / "run.py"
        cmd = [
            str(venv_python), "-u", str(tagger_script),
            f"--model={args.model}",
            f"--batch_size={args.batch}",
            f"--gen_threshold={args.gen_threshold}",
            f"--char_threshold={args.char_threshold}",
            "--output_json",
            str(staging_dir)
        ]

        print(f"[INFO] Launching Inference (Model: {args.model})...")
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Tagger submodule failed: {e}")
            return

        print("[INFO] Processing JSON sidecars into Staging DB...")

        # --- 1. LOAD YOUR DOMINANT RATING DB ---
        tag_rating_map = {}
        tag_db_path = STAGING_DB.parent / "tag_rating_dominant.db"
        if tag_db_path.exists():
            with sqlite3.connect(tag_db_path) as conn:
                tag_rating_map.update({
                    t.strip(): int(r) for t, r in conn.execute(
                        "SELECT tag_name, dominant_rating FROM dominant_tag_ratings"
                    )
                })
            print(f"[INFO] Loaded {len(tag_rating_map)} dominant rating rules from SQLite.")
        else:
            print("[WARNING] tag_rating_dominant.db not found. Falling back to AI ratings.")

        queued_upgrades = 0
        queued_tags = 0

        for json_file in staging_dir.glob("*.json"):
            img_hash = json_file.stem
            if img_hash not in safe_images:
                continue

            data = safe_images[img_hash]
            current_rating = data['rating']

            with open(json_file, 'r', encoding='utf-8') as f:
                output = orjson.loads(f.read())

            ratings = output.get("rating", {})
            char_tags = output.get("character", {})
            gen_tags = output.get("general", {})
            all_tags = {**char_tags, **gen_tags}

            # --- 2. DETERMINISTIC RATING LOGIC (Mirroring db.py) ---
            new_rating = current_rating

            if tag_rating_map:
                # Combine existing Postgres tags and the new AI tags for a total mathematical score
                combined_tags = set(data.get('tags', [])) | set(all_tags.keys())
                total_score = 0

                for tag in combined_tags:
                    clean_tag = tag.replace(" ", "_").lower()
                    # Strip tagai: prefix if it exists so it matches your DB rules
                    clean_tag = clean_tag[6:] if clean_tag.startswith("tagai:") else clean_tag

                    weight = tag_rating_map.get(clean_tag)
                    if weight is None:
                        continue
                    if weight == 1 and total_score == 0:
                        total_score = 1
                    elif weight > 1:
                        total_score += weight

                if total_score > 0:
                    # ai_auditor doesn't take smax/qmax args, so fallback to your CLI defaults
                    smax = getattr(args, 'smax', 50)
                    qmax = getattr(args, 'qmax', 250)
                    calc_rating = rating_from_score(total_score, smax, qmax)

                    # Ensure we only upgrade hierarchically (Never downgrade)
                    hierarchy = {'s': 0, 'q': 1, 'e': 2}
                    if hierarchy.get(calc_rating, 0) > hierarchy.get(current_rating, 0):
                        new_rating = calc_rating
            else:
                # Fallback to AI Logic if the DB is missing
                if ratings.get("explicit", 0) > args.gen_threshold:
                    new_rating = 'e'
                elif ratings.get("questionable", 0) > args.gen_threshold or ratings.get("sensitive", 0) > args.gen_threshold:
                    new_rating = 'q'

            if new_rating != current_rating:
                s_cur.execute(
                    "INSERT OR REPLACE INTO pending_upgrades (image_id, image_hash, current_rating, new_rating) VALUES (?, ?, ?, ?)",
                    (data['id'], img_hash, current_rating, new_rating)
                )
                queued_upgrades += 1

            # --- CONSOLIDATED TAG LOGIC ---
            for tag, confidence in all_tags.items():
                tag = tag.replace(" ", "_").lower()

                # 1. Log every AI thought to quarantine for future analysis
                q_cur.execute("INSERT OR IGNORE INTO ai_tags (image_hash, tag, confidence) VALUES (?, ?, ?)",
                              (img_hash, tag, confidence))

                # 2. Stage new tags for Shimmie application
                s_cur.execute("INSERT OR IGNORE INTO pending_tags (image_id, tag) VALUES (?, ?)",
                              (data['id'], tag))
                queued_tags += 1

    print(f"\n[✓] Scan Complete. Staged in {STAGING_DB.name}:")
    print(f"    - Proposed Rating Upgrades: {queued_upgrades}")
    print(f"    - Proposed Shadow Tags: {queued_tags}")

def _run_review(s_cur):
    """Prints a high-level summary of the staging database."""
    print("\n=== AI Audit Staging Review ===")

    s_cur.execute("SELECT current_rating, new_rating, COUNT(*) FROM pending_upgrades GROUP BY current_rating, new_rating")
    rows = s_cur.fetchall()
    print("\n[Proposed Rating Changes]")
    if not rows:
        print("  None")
    for old_r, new_r, count in rows:
        print(f"  {old_r.upper()} -> {new_r.upper()}: {count} images")

    s_cur.execute("SELECT tag, COUNT(*) as c FROM pending_tags GROUP BY tag ORDER BY c DESC LIMIT 15")
    rows = s_cur.fetchall()
    print("\n[Top 15 Proposed Shadow Tags]")
    if not rows:
        print("  None")
    for tag, count in rows:
        print(f"  tagai:{tag} (Added to {count} images)")

    s_cur.execute("SELECT COUNT(DISTINCT image_id) FROM pending_tags")
    row = s_cur.fetchone()
    total_tagged = row[0] if row else 0
    print(f"\n  Total Images receiving new tags: {total_tagged}")


def _run_apply(pg_conn, pg_cur, s_conn, s_cur, r_conn, r_cur, tags_only=False):
    """Reads the staging DB, logs the state for reverting, and pushes to Postgres."""
    print(f"\nReading staging database ({STAGING_DB.name})...")

    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    r_cur.execute("INSERT INTO runs (batch_id) VALUES (?)", (batch_id,))

    # 1. Apply Ratings & Log
    upgrades = []
    if not tags_only:
        s_cur.execute("SELECT image_id, current_rating, new_rating FROM pending_upgrades")
        upgrades = s_cur.fetchall()

        if upgrades:
            # We can leave ratings as a loop since there are usually very few of them (86k takes seconds)
            for img_id, old_rating, new_rating in tqdm.tqdm(upgrades, desc="Upgrading Ratings"):
                r_cur.execute("INSERT INTO rating_reverts (batch_id, image_id, old_rating) VALUES (?, ?, ?)", (batch_id, img_id, old_rating))
                pg_cur.execute("UPDATE images SET rating = %s WHERE id = %s", (new_rating, img_id))

    # 2. Apply Tags & Log
    s_cur.execute("SELECT image_id, tag FROM pending_tags")
    tags = s_cur.fetchall()

    if tags:
        tag_id_cache = {}
        pg_insert_records = set() # A Set automatically destroys exact duplicates!
        sqlite_revert_records = []

        # Phase A: Resolve the IDs entirely in Python
        for img_id, raw_tag in tqdm.tqdm(tags, desc="Resolving Tag IDs"):
            prefixed_tag = f"tagai:{raw_tag}"

            # The cache prevents us from querying the DB 3 million times for the same tags
            if prefixed_tag in tag_id_cache:
                tag_id = tag_id_cache[prefixed_tag]
            else:
                tag_id = _ensure_tag_exists(pg_cur, prefixed_tag)
                tag_id_cache[prefixed_tag] = tag_id

            pg_insert_records.add((img_id, tag_id))
            sqlite_revert_records.append((batch_id, img_id, tag_id))

        print("\nPushing tags to SQLite Ledger...")
        # Phase B: Bulk Insert into SQLite Ledger
        r_cur.executemany(
            "INSERT INTO tag_reverts (batch_id, image_id, tag_id) VALUES (?, ?, ?)",
            sqlite_revert_records
        )

        print(f"Pushing {len(pg_insert_records)} tags to Postgres...")
        # Phase C: Bulk Insert into Shimmie (This executes thousands of rows at a time)
        psycopg2.extras.execute_values(
            pg_cur,
            "INSERT INTO image_tags (image_id, tag_id) VALUES %s ON CONFLICT DO NOTHING",
            list(pg_insert_records),
            page_size=10000
        )

        print("Recalculating global tag counts...")
        # Phase D: Fix the Shimmie counts in one massive, perfectly accurate query
        pg_cur.execute("""
            UPDATE tags
            SET count = (
                SELECT COUNT(image_id)
                FROM image_tags
                WHERE tag_id = tags.id
            )
            WHERE tag LIKE 'tagai:%'
        """)

    pg_conn.commit()
    r_conn.commit()

    # 3. Clear Staging DB
    s_cur.execute("DELETE FROM pending_upgrades")
    s_cur.execute("DELETE FROM pending_tags")
    s_conn.commit()

    print(f"\n[✓] Apply Complete (Batch ID: {batch_id}).")


def _run_revert(pg_conn, pg_cur, r_conn, r_cur):
    """Finds the most recently applied batch and rolls it back using bulk operations."""
    r_cur.execute("SELECT batch_id, ts FROM runs ORDER BY ts DESC LIMIT 1")
    row = r_cur.fetchone()
    if not row:
        print("\n[INFO] No revert history found in ledger.")
        return

    batch_id, ts = row
    print(f"\n[⚠️ REVERTING] Rolling back Batch {batch_id} (Applied at {ts})...")

    # 1. Revert Ratings (Bulk)
    r_cur.execute("SELECT image_id, old_rating FROM rating_reverts WHERE batch_id = ?", (batch_id,))
    rating_rows = r_cur.fetchall()
    if rating_rows:
        print(f"Reverting {len(rating_rows)} ratings...")
        pg_cur.execute("CREATE TEMP TABLE tmp_ratings (img_id INT, old_rating VARCHAR) ON COMMIT DROP")
        psycopg2.extras.execute_values(pg_cur, "INSERT INTO tmp_ratings VALUES %s", rating_rows, page_size=10000)
        pg_cur.execute("""
            UPDATE images
            SET rating = tmp_ratings.old_rating
            FROM tmp_ratings
            WHERE images.id = tmp_ratings.img_id
        """)

    # 2. Revert Tags (Bulk)
    r_cur.execute("SELECT image_id, tag_id FROM tag_reverts WHERE batch_id = ?", (batch_id,))
    tag_rows = r_cur.fetchall()
    if tag_rows:
        print(f"Removing {len(tag_rows)} tag links...")
        pg_cur.execute("CREATE TEMP TABLE tmp_revert (img_id INT, tag_id INT) ON COMMIT DROP")
        psycopg2.extras.execute_values(pg_cur, "INSERT INTO tmp_revert VALUES %s", tag_rows, page_size=10000)

        pg_cur.execute("""
            DELETE FROM image_tags
            USING tmp_revert
            WHERE image_tags.image_id = tmp_revert.img_id AND image_tags.tag_id = tmp_revert.tag_id
        """)

        print("Recalculating global tag counts...")
        pg_cur.execute("""
            UPDATE tags
            SET count = (
                SELECT COUNT(image_id)
                FROM image_tags
                WHERE tag_id = tags.id
            )
            WHERE tag LIKE 'tagai:%'
        """)

    # 3. Clean up Orphaned AI Tags
    print("Sweeping orphaned AI tags...")
    pg_cur.execute("""
        DELETE FROM tags
        WHERE tag LIKE 'tagai:%'
        AND NOT EXISTS (SELECT 1 FROM image_tags WHERE tag_id = tags.id)
    """)

    # 4. Remove Batch from Ledger
    r_cur.execute("DELETE FROM rating_reverts WHERE batch_id = ?", (batch_id,))
    r_cur.execute("DELETE FROM tag_reverts WHERE batch_id = ?", (batch_id,))
    r_cur.execute("DELETE FROM runs WHERE batch_id = ?", (batch_id,))

    pg_conn.commit()
    r_conn.commit()
    print(f"[✓] Revert Complete. Rolled back {len(rating_rows)} ratings and {len(tag_rows)} tag links.")

def run_auditor(args):
    """Main CLI router for the Auditor."""
    if not any([args.scan, args.review, args.apply, args.revert]):
        print("[ERROR] You must specify --scan, --review, --apply, or --revert.")
        return

    if not SUBMODULE_PATH.exists():
        print("[ERROR] SD-Tag-Editor submodule not found.")
        return

    actions = [a for a, flag in zip(["SCAN", "REVIEW", "APPLY", "REVERT"], [args.scan, args.review, args.apply, args.revert], strict=True) if flag]
    print(f"=== AI Safety Auditor ({' & '.join(actions)}) ===")

    db_config = get_shimmie_db_credentials(args.spath)
    if db_config:
        pg_conn = psycopg2.connect(**db_config)
        pg_cur = pg_conn.cursor()

        q_conn, s_conn, r_conn = _init_databases()
        q_cur, s_cur, r_cur = q_conn.cursor(), s_conn.cursor(), r_conn.cursor()

        try:
            if args.scan:
                _run_scan(args, pg_cur, s_cur, q_cur)
            if args.review:
                _run_review(s_cur)
            if args.apply:
                _run_apply(pg_conn, pg_cur, s_conn, s_cur, r_conn, r_cur, tags_only=args.tags_only)
            if args.revert:
                _run_revert(pg_conn, pg_cur, r_conn, r_cur)
        finally:
            pg_conn.close()
            for conn in (q_conn, s_conn, r_conn):
                conn.commit()
                conn.close()
