# pylint: disable=duplicate-code,line-too-long,too-many-locals,too-many-branches,too-many-statements
"""Database management tools (SQLite Conversions, Precaching, Rating Updates)."""
import csv
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg2
import tqdm

from functions.db_cache import get_shimmie_db_credentials
from functions.tags_curation import rating_from_score

# Try to use orjson for speed, fallback to standard json
try:
    import orjson as fastjson
    def json_loads(x):
        """Loads JSON fast."""
        return fastjson.loads(x) # pylint: disable=no-member,c-extension-no-member
except ImportError:
    fastjson = None
    def json_loads(x):
        """Loads JSON."""
        return json.loads(x)

# ==========================================
# Tool 1: CSV to SQLite
# ==========================================
def csv_to_sqlite(args):
    """Converts a CSV file to an SQLite database."""
    csv_path = Path(args.csv)
    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)

        safe_table = f'"{args.table.replace("\"", "\"\"")}"'
        create_cols = ', '.join([f'"{h.replace("\"", "\"\"")}" TEXT' for h in headers])
        insert_cols = ', '.join([f'"{h.replace("\"", "\"\"")}"' for h in headers])
        placeholders = ', '.join(['?'] * len(headers))

        with sqlite3.connect(args.db) as conn:
            cursor = conn.cursor()
            if args.drop_table:
                cursor.execute(f"DROP TABLE IF EXISTS {safe_table}")
            cursor.execute(f'CREATE TABLE {safe_table} ({create_cols})')
            cursor.executemany(
                f'INSERT INTO {safe_table} ({insert_cols}) VALUES ({placeholders})',
                reader
            )

    print(f"[✓] Converted '{args.csv}' to '{args.db}' in table '{args.table}'.")

# ==========================================
# Tool 2: Precache Posts to SQLite
# ==========================================
def _parse_post_line(line: str):
    """Parses a single line of Danbooru posts.json."""
    try:
        post = json_loads(line)
    except Exception as e: # pylint: disable=broad-exception-caught
        print(f"[SKIP] JSON decode error: {e}")
        return None

    md5 = post.get("md5") or post.get("media_asset", {}).get("md5")
    pixel_hash = post.get("media_asset", {}).get("pixel_hash")
    raw_key = md5 or pixel_hash

    if not isinstance(raw_key, str):
        return None

    cache_key = raw_key.lower()
    general_tags = post.get("tag_string_general", "").split()
    character_tags = post.get("tag_string_character", "").split()
    series_tags = post.get("tag_string_copyright", "").split()
    artist_tags = post.get("tag_string_artist", "").split()
    rating = post.get("rating", "")
    source = post.get("source", None)

    has_data = any([
        general_tags, character_tags, series_tags,
        artist_tags, rating, source, pixel_hash
    ])
    if not has_data:
        return None

    return (cache_key, {
        "pixel_hash": pixel_hash or "",
        "rating": rating,
        "source": source,
        "general": general_tags,
        "character": character_tags,
        "artist": artist_tags,
        "series": series_tags
    })

def precache_posts(args):
    """Pre-caches JSON dump to SQLite for fast lookups."""
    posts_path = Path(args.posts_json)
    db_out = Path(args.output)

    print("=== Precache Run Summary ===")
    print(f"📁 Input File:      {posts_path}")
    print(f"🗄️ Output DB:       {db_out}")
    print(f"🧵 Threads:         {args.threads}\n")

    # FIX: Use 'with' to safely open and read line counts
    with posts_path.open("r", encoding="utf-8", errors="ignore") as file_obj:
        total_lines = sum(1 for _ in file_obj)

    results = []

    with posts_path.open("r", encoding="utf-8", errors="ignore") as f:
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            mapper = executor.map(_parse_post_line, f, chunksize=100)
            for result in tqdm.tqdm(mapper, total=total_lines, desc="Parsing JSON"):
                if result:
                    results.append(result)

    with sqlite3.connect(db_out) as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                md5 TEXT PRIMARY KEY, pixel_hash TEXT, rating TEXT, source TEXT,
                general TEXT, character TEXT, artist TEXT, series TEXT
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pixel_hash ON posts(pixel_hash)")

        print(f"[INFO] Writing {len(results):,} records to SQLite...")
        for md5, post in results:
            cur.execute("""
                INSERT OR REPLACE INTO posts (
                    md5, pixel_hash, rating, source, general, character, artist, series
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                md5, post.get("pixel_hash"), post.get("rating"), post.get("source"),
                ",".join(post.get("general", [])), ",".join(post.get("character", [])),
                ",".join(post.get("artist", [])), ",".join(post.get("series", []))
            ))

        conn.commit()
    print(f"[✓] SQLite DB written to: {db_out}")

# ==========================================
# Tool 3: Update Postgres Ratings
# ==========================================
def _update_single_image(pg_cur, image_id, tag_rating_map, smax, qmax):
    """Helper to process a single image's rating to reduce local variables."""
    pg_cur.execute("""
        SELECT t.tag FROM tags t JOIN image_tags it ON t.id = it.tag_id
        WHERE it.image_id = %s
    """, (image_id,))
    tags = [t[0] for t in pg_cur.fetchall()]

    total_score = 0
    for tag in tags:
        clean_tag = tag[6:] if tag.startswith("tagai:") else tag

        weight = tag_rating_map.get(clean_tag)
        if weight is None:
            continue
        if weight == 1 and total_score == 0:
            total_score = 1
        elif weight > 1:
            total_score += weight

    rating_letter = None
    if total_score > 0:
        rating_letter = rating_from_score(total_score, smax, qmax)

    pg_cur.execute("SELECT rating FROM images WHERE id = %s", (image_id,))
    current_rating = pg_cur.fetchone()[0]

    if rating_letter is None:
        rating_letter = current_rating if current_rating is not None else "?"

    if current_rating != rating_letter:
        pg_cur.execute(
            "UPDATE images SET rating = %s WHERE id = %s",
            (rating_letter, image_id)
        )
        return 1
    return 0

def update_ratings(args):
    """Updates existing ratings in shimmiedb based on dominant tags."""
    script_dir = Path(__file__).parent.parent.resolve()
    db_path = script_dir / "database" / "tag_rating_dominant.db"
    tag_rating_map = {}

    with sqlite3.connect(db_path) as conn:
        tag_rating_map.update({
            t.strip(): int(r) for t, r in conn.execute(
                "SELECT tag_name, dominant_rating FROM dominant_tag_ratings"
            )
        })

    pg_config = get_shimmie_db_credentials(args.spath)
    if not pg_config:
        print(f"[ERROR] Could not load DB credentials from {args.spath}")
        return

    with psycopg2.connect(**pg_config) as pg_conn:
        pg_cur = pg_conn.cursor()
        pg_cur.execute("SELECT id FROM images")
        ids = [row[0] for row in pg_cur.fetchall()]
        updated = 0

        for i, image_id in enumerate(ids, start=1):
            print(f"Processing image {i}/{len(ids)}", end="\r")
            updated += _update_single_image(pg_cur, image_id, tag_rating_map, args.smax, args.qmax)

        print(" " * 60, end="\r")
        print(f"[✓] Updated {updated} image rating{'s' if updated != 1 else ''}.\n")
        pg_conn.commit()

# ==========================================
# Tool 4: Purge Blacklisted Images
# ==========================================
def purge_images(args):
    """Purges images containing blacklisted tags from DB and disk."""
    blacklist_path = Path(args.blacklist)
    if not blacklist_path.is_file():
        print(f"[ERROR] Blacklist not found: {args.blacklist}")
        return

    # Load tags to delete
    rules = []
    prefilter_tags = set()
    preserve_tags = set()
    ignore_tags = set()

    for line in blacklist_path.read_text(encoding="utf-8").splitlines():
        raw_line = line.strip().lower()

        # 1. Catch the whitelist directive BEFORE stripping comments
        if raw_line.replace(" ", "").startswith("#//whitelist:"):
            # Extract everything after "whitelist:" and split by comma
            whitelist_content = raw_line.split("whitelist:", 1)[1]

            clean_whitelist = whitelist_content.split("#//")[0]
            for pt in clean_whitelist.split(","):
                clean_pt = pt.strip()
                if clean_pt:
                    preserve_tags.add(clean_pt)
            continue

        # 2. Catch the ignore directive
        if raw_line.replace(" ", "").startswith("#//ignore:"):
            ignore_content = raw_line.split("ignore:", 1)[1]
            clean_ignore = ignore_content.split("#//")[0]
            for pt in clean_ignore.split(","):
                clean_pt = pt.strip()
                if clean_pt:
                    ignore_tags.add(clean_pt)
            continue

        # 2. Safely strip normal comments and grab what's left
        clean_line = raw_line.split("#//")[0].strip()

        # 3. Skip if the line is now empty
        if not clean_line:
            continue

        pos_tags = []
        neg_tags = []
        for token in clean_line.split():
            if token.startswith("-"):
                neg_tags.append(token[1:])
            else:
                pos_tags.append(token)
                prefilter_tags.add(token)

        if not pos_tags:
            print(f"[ERROR] Invalid rule '{clean_line}'. You must include at least one positive tag.")
            return

        rules.append({'pos': pos_tags, 'neg': neg_tags, 'raw': clean_line})

    print(f"[INFO] Loaded {len(rules)} rules and {len(preserve_tags)} global preservation tags. Hunting...")
    prefilter_list = list(prefilter_tags)

    db_config = get_shimmie_db_credentials(args.spath)
    shimmie_root = Path(args.spath)

    with psycopg2.connect(**db_config) as conn:
        cur = conn.cursor()

        # Step 1: Fetch suspects and ALL their tags using a CTE
        cur.execute("""
            WITH suspect_images AS (
                SELECT DISTINCT i.id, i.hash
                FROM images i
                JOIN image_tags it ON i.id = it.image_id
                JOIN tags t ON it.tag_id = t.id
                WHERE t.tag = ANY(%s)
                   OR (
                       t.tag NOT LIKE 'tagai:%%'
                       AND t.tag NOT LIKE 'booru:%%'
                       AND substring(t.tag from position(':' in t.tag) + 1) = ANY(%s)
                   )
            )
            SELECT s.id, s.hash, string_agg(t.tag, ' ') as all_tags
            FROM suspect_images s
            JOIN image_tags it ON s.id = it.image_id
            JOIN tags t ON it.tag_id = t.id
            GROUP BY s.id, s.hash
        """, (prefilter_list, prefilter_list))

        suspects = cur.fetchall()

        # Step 2: Evaluate the suspects locally against the complex rules
        trash_images = []
        breakdown_counts = {r['raw']: 0 for r in rules}

        for img_id, hsh, tag_string in suspects:
            raw_tags = tag_string.split(' ')

            # Build an evaluation set that handles your broad sweep (stripping prefixes)
            clean_tags = set(raw_tags)
            for t in raw_tags:
                if not t.startswith(("tagai:", "booru:")) and ":" in t:
                    clean_tags.add(t.split(":", 1)[1])

            # --- The Immunity Shield ---
            if preserve_tags and any(pt in clean_tags for pt in preserve_tags):
                continue  # Image is instantly spared!

            # Evaluate against all blacklist rules
            triggered_rules = []
            for rule in rules:
                has_all_pos = all(p in clean_tags for p in rule['pos'])
                has_any_neg = any(n in clean_tags for n in rule['neg'])

                if has_all_pos and not has_any_neg:
                    triggered_rules.append(rule['raw'])

            if triggered_rules:
                for r in triggered_rules:
                    breakdown_counts[r] += 1
                trash_images.append((img_id, hsh, tag_string, triggered_rules))

        # Sort breakdown by count descending
        sorted_breakdown = sorted(breakdown_counts.items(), key=lambda item: item[1], reverse=True)
        sorted_breakdown = [item for item in sorted_breakdown if item[1] > 0]

        if not trash_images:
            print("[✓] Database is clean. No images found matching the blacklist.")
            return

        print(f"\n[⚠️ WARNING] Found {len(trash_images)} unique images to permanently delete.")
        print("\n=== CASUALTY BREAKDOWN BY RULE ===")
        for rule_text, count in sorted_breakdown[:20]:
            print(f"  - '{rule_text}': {count} images")
        if len(sorted_breakdown) > 20:
            print(f"  ... and {len(sorted_breakdown) - 20} more rules.")
        print("==================================\n")

        # --- Dry Run Handler ---
        if args.dry_run:
            report_path = Path("purge_dry_run.txt")

            # --- TOP 100 NOISE FILTERS ---
            ignore_prefixes = ("booru:", "tagai:")

            # Tally up associated tags and track the rules that condemned them
            associated_tags = {}
            for _, _, tag_string, triggered_rules in trash_images:
                for t in tag_string.split():
                    if t.startswith(ignore_prefixes) or t in ignore_tags:
                        continue

                    base_t = t.split(":", 1)[1] if not t.startswith(("tagai:", "booru:")) and ":" in t else t

                    if t not in prefilter_tags and base_t not in prefilter_tags:
                        # Initialize the nested dictionary if this is a new tag
                        if t not in associated_tags:
                            associated_tags[t] = {'total': 0, 'rules': {}}

                        associated_tags[t]['total'] += 1

                        # Tally up which specific rules brought this tag down
                        for rule in triggered_rules:
                            associated_tags[t]['rules'][rule] = associated_tags[t]['rules'].get(rule, 0) + 1

            # 1. Sort by total frequency descending to find the 100 heaviest casualties
            top_by_purge = sorted(associated_tags.items(), key=lambda x: x[1]['total'], reverse=True)[:100]

            # 2. Fetch total database counts for those Top 100
            top_tag_names = [t[0] for t in top_by_purge]
            if top_tag_names:
                cur.execute("SELECT tag, count FROM tags WHERE tag = ANY(%s)", (top_tag_names,))
                db_totals = dict(cur.fetchall())
            else:
                db_totals = {}

            # 3. Calculate remaining counts and build a final sortable list
            final_top_100 = []
            for tag_name, tag_data in top_by_purge:
                purge_count = tag_data['total']
                total = db_totals.get(tag_name, 0)
                remaining = max(0, total - purge_count)

                # Find the single rule that caused the most damage to this tag
                top_rule, top_rule_count = max(tag_data['rules'].items(), key=lambda item: item[1])

                final_top_100.append({
                    'tag_name': tag_name,
                    'purge_count': purge_count,
                    'remaining': remaining,
                    'top_rule': top_rule,
                    'top_rule_count': top_rule_count
                })

            # 4. Re-sort the Top 100 strictly by what is remaining (Ascending, so 0 is at the top)
            final_top_100.sort(key=lambda x: x['remaining'], reverse=True)

            with open(report_path, "w", encoding="utf-8") as f:
                f.write("=== CASUALTY BREAKDOWN BY RULE ===\n")
                for rule_text, count in sorted_breakdown:
                    f.write(f"  - '{rule_text}': {count} images\n")

                f.write("\n=== TOP 100 MOST COMMON ASSOCIATED TAGS ===\n")
                f.write("(Sorted by highest remaining count. Shows tags heavily impacted by the crossfire.)\n")
                for item in final_top_100:
                    f.write(f"  - {item['tag_name']}: {item['remaining']} remaining images ({item['purge_count']} purging) (Primary Offender: '{item['top_rule']}' with {item['top_rule_count']} hits)\n")

                f.write("\n=== IMAGES FLAGGED FOR DELETION ===\n")
                for img_id, hsh, tag_string, triggered_rules in trash_images:
                    f.write(f"ID: {img_id} | Hash: {hsh} | Triggered By: [{', '.join(triggered_rules)}] | Tags: {tag_string}\n")

            print(f"[ℹ️ DRY RUN] Safe abort. Wrote full breakdown, collateral tags, and {len(trash_images)} targets to {report_path.resolve()}")
            return

        confirm = input("Type 'YES' to delete files and database records: ")
        if confirm != "YES":
            print("Aborting.")
            return

        deleted_files = 0
        img_ids_to_drop = []

        # 1. Delete physical files off the hard drive
        print("\n[Step 1/3] Deleting physical files from disk...")
        for img_id, hsh, *_ in tqdm.tqdm(trash_images, desc="Files Purged", unit="file"):
            img_ids_to_drop.append(img_id)

            # Shimmie2 path logic: data/images/ab/cd/hash (NO EXTENSIONS)
            prefix1, prefix2 = hsh[0:2], hsh[2:4]
            img_file = shimmie_root / "data" / "images" / prefix1 / prefix2 / hsh
            thumb_file = shimmie_root / "data" / "thumbs" / prefix1 / prefix2 / hsh

            if img_file.exists():
                img_file.unlink()
                deleted_files += 1
            if thumb_file.exists():
                thumb_file.unlink()

        # 2. Delete from Postgres
        print("\n[Step 2/3] Scrubbing Postgres database...")
        print("  -> Dropping metadata links (image_tags)...")
        cur.execute("DELETE FROM image_tags WHERE image_id = ANY(%s)", (img_ids_to_drop,))
        print("  -> Dropping core image records (images)...")
        cur.execute("DELETE FROM images WHERE id = ANY(%s)", (img_ids_to_drop,))

        # 3. Recalculate tag counts and drop orphaned tags (count = 0)
        print("\n[Step 3/3] Fixing Shimmie tag UI counts...")
        print("  -> Recalculating tag usage counts (this may take a moment)...")
        cur.execute("""
            UPDATE tags
            SET count = (SELECT COUNT(image_id) FROM image_tags WHERE tag_id = tags.id)
        """)
        print("  -> Pruning orphaned tags with 0 count...")
        cur.execute("DELETE FROM tags WHERE count = 0")

        conn.commit()
        print(f"\n[✓] Purge Complete: Destroyed {deleted_files} files and {len(img_ids_to_drop)} database entries.")
