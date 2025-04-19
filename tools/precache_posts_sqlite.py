import re
import json
import sqlite3
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import tqdm

try:
    import orjson as fastjson
    def json_loads(x): return fastjson.loads(x)
except ImportError:
    fastjson = None
    def json_loads(x): return json.loads(x)

def parse_line(line: str) -> tuple[str, dict] | None:
    try:
        post = json_loads(line)
    except Exception as e:
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

    if not any([general_tags, character_tags, series_tags, artist_tags, rating, source, pixel_hash]):
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


def write_to_sqlite(results: list[tuple[str, dict]], db_path: Path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS posts (
        md5 TEXT PRIMARY KEY,
        pixel_hash TEXT,
        rating TEXT,
        source TEXT,
        general TEXT,
        character TEXT,
        artist TEXT,
        series TEXT
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pixel_hash ON posts(pixel_hash)")

    print(f"[INFO] Writing {len(results):,} records to SQLite...")
    for md5, post in results:
        cur.execute("""
        INSERT OR REPLACE INTO posts (
            md5, pixel_hash, rating, source,
            general, character, artist, series
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            md5,
            post.get("pixel_hash"),
            post.get("rating"),
            post.get("source"),
            ",".join(post.get("general", [])),
            ",".join(post.get("character", [])),
            ",".join(post.get("artist", [])),
            ",".join(post.get("series", [])),
        ))
    conn.commit()
    conn.close()
    print(f"[✓] SQLite DB written to: {db_path}")


def main(posts_path: Path, db_out: Path, threads: int = 16):
    print("=== Precache Run Summary ===")
    print(f"📄  Input File:      {posts_path}")
    print(f"💾  Output DB:       {db_out}")
    print(f"🧵  Threads:         {threads}")
    print()

    print(f"[INFO] Reading from {posts_path} using {threads} threads...")
    total_lines = sum(1 for _ in posts_path.open("r", encoding="utf-8", errors="ignore"))

    results = []
    with posts_path.open("r", encoding="utf-8", errors="ignore") as f:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            for result in tqdm.tqdm(executor.map(parse_line, f, chunksize=100), total=total_lines, desc="Pre-caching to SQLite"):
                if result:
                    results.append(result)

    write_to_sqlite(results, db_out)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pre-cache Danbooru posts.json directly into an SQLite DB.")
    parser.add_argument("posts_json", nargs="?", default="input/posts.json", help="Path to posts.json")
    parser.add_argument("-o", "--output", default="tools/data/posts_cache.db", help="Where to write the SQLite DB")
    parser.add_argument("--threads", type=int, default=8, help="Number of threads to use")
    args = parser.parse_args()

    main(Path(args.posts_json), Path(args.output), args.threads)

