# pylint: disable=duplicate-code
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
        weight = tag_rating_map.get(tag)
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
