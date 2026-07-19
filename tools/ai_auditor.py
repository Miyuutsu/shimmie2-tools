# pylint: disable=line-too-long disable=too-many-locals disable=too-many-branches disable=too-many-statements disable=too-many-arguments disable=too-many-positional-arguments
"""Bridge script for integrating the SD-Tag-Editor submodule safely via JSON."""
import os
import json
import sqlite3
import subprocess
import tempfile
from pathlib import Path

import psycopg2
import psycopg2.extras
import tqdm

from functions.db_cache import get_shimmie_db_credentials

SUBMODULE_PATH = Path(__file__).parent.parent / "SD-Tag-Editor"
QUARANTINE_DB = Path(__file__).parent.parent / "database" / "ai_quarantine_cache.db"

def _init_databases():
    """Initializes the quarantine database for logging AI confidences."""
    QUARANTINE_DB.parent.mkdir(parents=True, exist_ok=True)

    q_conn = sqlite3.connect(QUARANTINE_DB)
    q_conn.execute("""
        CREATE TABLE IF NOT EXISTS ai_tags (
            image_hash TEXT, tag TEXT, confidence REAL, UNIQUE(image_hash, tag)
        )
    """)
    q_conn.execute("CREATE INDEX IF NOT EXISTS idx_ai_tags_hash ON ai_tags(image_hash)")
    return q_conn

def _get_image_batches(pg_conn, batch_size=50000):
    """Yields manageable dictionaries of images using a server-side cursor to prevent OOM."""
    # A named cursor forces psycopg2 to keep the results on the server and stream them
    with pg_conn.cursor(name="audit_cursor") as cur:
        query = """
            SELECT i.id, i.hash, t.tag
            FROM images i
            LEFT JOIN image_tags it ON i.id = it.image_id
            LEFT JOIN tags t ON it.tag_id = t.id
        """
        cur.execute(query)

        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break

            images_data = {}
            for img_id, img_hash, tag in rows:
                if img_hash not in images_data:
                    images_data[img_hash] = {"id": img_id, "tags": set()}
                if tag:
                    images_data[img_hash]["tags"].add(tag)

            yield {
                img_hash: data for img_hash, data in images_data.items()
                if not any(t.startswith('tagai:') for t in data['tags'])
            }

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

def _run_audit_pipeline(args, pg_conn, pg_cur, q_conn, q_cur):
    venv_python = _ensure_submodule_installed()

    thumbs_base_dir = Path(args.thumbs) if args.thumbs else Path(args.spath) / "data" / "thumbs"
    if not thumbs_base_dir.exists():
        print(f"[ERROR] Thumbnails directory not found at {thumbs_base_dir}")
        return

    print("Initiating streaming database cursor...")

    # Process the database in discrete batches from start to finish
    for batch_index, images_data in enumerate(_get_image_batches(pg_conn)):
        if not images_data:
            continue

        print(f"\n--- Processing DB Chunk {batch_index + 1} ({len(images_data)} images) ---")

        # Create the temporary directory inside the loop so it wipes clean after every batch
        with tempfile.TemporaryDirectory(prefix="ai_audit_") as temp_dir_str:
            staging_dir = Path(temp_dir_str)
            staged_count = 0

            print(f"Staging read-only symlinks to {staging_dir}...")
            for img_hash in tqdm.tqdm(images_data, desc="Staging Symlinks"):
                real_thumb = _resolve_shimmie_thumb_path(thumbs_base_dir, img_hash).resolve()
                if real_thumb.exists():
                    os.symlink(str(real_thumb), str(staging_dir / f"{img_hash}.jpg"))
                    staged_count += 1

            if staged_count == 0:
                print("[WARNING] No thumbnails found matching this DB chunk. Skipping.")
                continue

            tagger_script = SUBMODULE_PATH / "run.py"
            cmd = [
                str(venv_python), "-u", str(tagger_script),
                f"--model={args.model}", f"--batch_size={args.batch}",
                f"--gen_threshold={args.gen_threshold}", f"--char_threshold={args.char_threshold}",
                "--output_json", str(staging_dir)
            ]

            print("[INFO] Launching Inference...")
            try:
                subprocess.run(cmd, check=True)
            except subprocess.CalledProcessError as e:
                print(f"[ERROR] Tagger submodule failed: {e}")
                return

            print("[INFO] Resolving tags and updating Postgres...")
            tag_id_cache = {}
            pg_insert_records = set()
            queued_tags = 0

            for json_file in tqdm.tqdm(list(staging_dir.glob("*.json")), desc="Parsing Output"):
                img_hash = json_file.stem
                if img_hash not in images_data:
                    continue

                data = images_data[img_hash]
                with open(json_file, 'r', encoding='utf-8') as f:
                    output = json.load(f)

                char_tags = output.get("character", {})
                gen_tags = output.get("general", {})
                all_tags = {**char_tags, **gen_tags}
                base_db_tags = {t.split(':', 1)[-1] if ':' in t else t for t in data.get('tags', set())}

                for tag, confidence in all_tags.items():
                    tag = tag.replace(" ", "_").lower()

                    q_cur.execute("INSERT OR IGNORE INTO ai_tags (image_hash, tag, confidence) VALUES (?, ?, ?)",
                                  (img_hash, tag, confidence))

                    if tag in base_db_tags:
                        continue

                    prefixed_tag = f"tagai:{tag}"
                    if prefixed_tag in tag_id_cache:
                        tag_id = tag_id_cache[prefixed_tag]
                    else:
                        tag_id = _ensure_tag_exists(pg_cur, prefixed_tag)
                        tag_id_cache[prefixed_tag] = tag_id

                    pg_insert_records.add((data['id'], tag_id))
                    queued_tags += 1

            q_conn.commit()

            if pg_insert_records:
                psycopg2.extras.execute_values(
                    pg_cur,
                    "INSERT INTO image_tags (image_id, tag_id) VALUES %s ON CONFLICT DO NOTHING",
                    list(pg_insert_records),
                    page_size=10000
                )
                print(f"Applied {queued_tags} shadow tags for this chunk.")
            else:
                print("No new tags to apply for this chunk.")

    print("\nRecalculating global tag counts...")
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
    print("\n[✓] Global Audit Complete.")


def run_auditor(args):
    """Main CLI router for the Auditor."""
    if not SUBMODULE_PATH.exists():
        print("[ERROR] SD-Tag-Editor submodule not found.")
        return

    print("=== AI Shadow Tag Auditor (Direct Apply) ===")

    db_config = get_shimmie_db_credentials(args.spath)
    pg_conn = psycopg2.connect(**db_config)
    pg_cur = pg_conn.cursor()

    q_conn = _init_databases()
    q_cur = q_conn.cursor()

    try:
        _run_audit_pipeline(args, pg_conn, pg_cur, q_conn, q_cur)
    finally:
        pg_conn.close()
        q_conn.close()
