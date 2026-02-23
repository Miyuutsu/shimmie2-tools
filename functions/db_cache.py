"""Database and caching functions."""
import os
import re
import sys
import sqlite3
import threading
import subprocess
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
import tqdm

from functions.common import compute_md5, VIDEO_EXTS
from functions.media import compute_danbooru_pixel_hash
from functions.tags_curation import parse_tags

@contextmanager
def get_cache_conn(cache):
    '''Use a connection cache'''
    thread_local = threading.local()
    conn = getattr(thread_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(cache, check_same_thread=False)
        thread_local.conn = conn
    yield conn

def row_to_post_dict(row: tuple) -> dict:
    """Formats SQLite rows to post dicts"""
    def split_field(field):
        if not field:
            return []
        return [part.strip() for part in field.split(",") if part.strip()]

    return {
        "md5": row[0], "pixel_hash": row[1], "rating": row[2], "source": row[3],
        "general": split_field(row[4]), "character": split_field(row[5]),
        "artist": split_field(row[6]), "series": split_field(row[7]),
    }

def _check_shimmie_for_md5(md5, shimmie_path, dbuser):
    """Helper to check if MD5 exists in Shimmie via PHP subprocess."""
    try:
        cmd = [
            "php", str(Path(shimmie_path) / "index.php"),
            "-u", dbuser or "dbuser", "search", f"md5:{md5}"
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, check=False, cwd=shimmie_path)
        return md5 in res.stdout
    except Exception: # pylint: disable=broad-exception-caught
        print(f"Error checking Shimmie2 database! ({sys.exc_info()[0].__name__})")
        return "error"

def resolve_post(image: Path, shimmie_path, skip_existing, dbuser, cache) -> tuple:
    """Resolves the post information from the database or adds it to cache."""
    with get_cache_conn(cache) as conn:
        cur = conn.cursor()
        post = None

        match = re.compile(r"[a-fA-F0-9]{32}").search(image.stem)
        md5 = match.group(0).lower() if match else compute_md5(image)
        is_video = image.suffix.lower() in VIDEO_EXTS
        px_hash = None

        if md5:
            cur.execute("SELECT * FROM posts WHERE md5 = ?", (md5,))
            if row := cur.fetchone():
                post = row_to_post_dict(row)
                cur.execute("SELECT pixel_hash FROM posts WHERE md5 = ?", (md5,))
                px_res = cur.fetchone()
                px_hash = px_res[0] if px_res else None

        if not px_hash:
            px_hash = md5 if is_video else compute_danbooru_pixel_hash(image)

        if not post:
            cur.execute("SELECT * FROM posts WHERE pixel_hash = ?", (px_hash,))
            if row := cur.fetchone():
                post = row_to_post_dict(row)
            else:
                add_post_to_cache(md5, px_hash, cache)
                cur.execute("SELECT * FROM posts WHERE pixel_hash = ?", (px_hash,))
                post = row_to_post_dict(cur.fetchone())

        exists = False
        if skip_existing and shimmie_path:
            exists = _check_shimmie_for_md5(md5, shimmie_path, dbuser)

        return image, post, md5, px_hash, exists

def add_post_to_cache(md5, px_hash, cache):
    """For adding new images to cache"""
    with get_cache_conn(cache) as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                md5 TEXT PRIMARY KEY, pixel_hash TEXT, rating TEXT, source TEXT,
                general TEXT, character TEXT, artist TEXT, series TEXT
            )
        """)
        cur.execute("""
            INSERT OR REPLACE INTO posts
            (md5, pixel_hash, rating, source, general, character, artist, series)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (md5, px_hash, "?", "", "", "", "", ""))
        conn.commit()

def save_post_to_cache(res_data, rating_letter, tags: list[str], pure_source_link, cache):
    """For updating the cache"""
    with get_cache_conn(cache) as conn:
        cur = conn.cursor()
        parsed_tags = parse_tags(tags)
        new_data = (
            rating_letter, pure_source_link or "",
            parsed_tags[0], parsed_tags[1], parsed_tags[2], parsed_tags[3]
        )

        cur.execute("""
            SELECT rating, source, general, character, artist, series
            FROM posts WHERE md5 = ?""", (res_data.md5,))
        existing = cur.fetchone()

        if existing is None or existing != new_data:
            cur.execute("""
                INSERT OR REPLACE INTO posts
                (md5, pixel_hash, rating, source, general, character, artist, series)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (res_data.md5, res_data.px_hash, *new_data))
        conn.commit()

def get_shimmie_db_credentials(spath):
    """Parses Shimmie2 config.php to extract PostgreSQL credentials."""
    if not spath:
        return None

    config_path = Path(spath) / "data" / "config" / "shimmie.conf.php"
    if not config_path.is_file():
        print(f"[WARNING] Could not find Shimmie config at {spath}")
        return None

    content = config_path.read_text(encoding='utf-8')
    dsn_match = re.search(r"['\"]pgsql:([^'\"]+)['\"]", content)

    user_match = (
        re.search(r"\$database_user\s*=\s*['\"]([^'\"]+)['\"]", content) or
        re.search(r"define\s*\(\s*['\"]DATABASE_USER['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", content)
    )
    pass_match = (
        re.search(r"\$database_pass\s*=\s*['\"]([^'\"]+)['\"]", content) or
        re.search(r"define\s*\(\s*['\"]DATABASE_PASS['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", content)
    )

    if not dsn_match:
        return None

    dsn_parts = dict(part.split('=') for part in dsn_match.group(1).split(';') if '=' in part)
    return {
        "host": dsn_parts.get('host', 'localhost'),
        "dbname": dsn_parts.get('dbname', 'shimmie'),
        "user": user_match.group(1) if user_match else 'postgres',
        "password": pass_match.group(1) if pass_match else ''
    }

def _fetch_sqlite_tags(md5_list, sqlite_conn, results):
    """Helper to fetch tags from SQLite cache to reduce local variables."""
    if not sqlite_conn:
        return

    cur = sqlite_conn.cursor()
    for i in range(0, len(md5_list), 999):
        chunk = md5_list[i:i+999]
        placeholders = ','.join(['?'] * len(chunk))

        query = (
            f"SELECT md5, general, character, artist, series "
            f"FROM posts WHERE md5 IN ({placeholders})"
        )

        cur.execute(query, chunk)
        for row in cur.fetchall():
            for field in row[1:]:
                if field:
                    clean_tags = [t.strip() for t in field.split(",") if t.strip()]
                    results[row[0]].update(clean_tags)

def _fetch_postgres_tags(md5_list, db_conn, chunk_size, results):
    """Helper to fetch tags from PostgreSQL to reduce local variables."""
    if not db_conn:
        return

    env = os.environ.copy()
    if db_conn.get('password'):
        env['PGPASSWORD'] = db_conn['password']

    missing = [m for m in md5_list if m not in results]
    for i in tqdm.tqdm(range(0, len(missing), chunk_size), desc="Querying Postgres", leave=False):
        chunk = missing[i:i+chunk_size]
        if not chunk:
            continue

        query = (
            "SELECT i.hash, t.tag FROM tags t "
            "JOIN image_tags it ON t.id = it.tag_id JOIN images i ON i.id = it.image_id "
            "WHERE i.hash IN ('" + "', '".join(chunk) + "');"
        )
        try:
            cmd = [
                "psql", "-d", db_conn['dbname'], "-U", db_conn['user'],
                "-h", db_conn['host'], "-t", "-A", "-c", query
            ]
            res = subprocess.run(
                cmd, env=env, capture_output=True, text=True, check=True
            )
            for line in res.stdout.strip().split('\n'):
                if '|' in line:
                    hsh, tag = line.split('|', 1)
                    results[hsh].add(tag.strip())
        except Exception as e: # pylint: disable=broad-exception-caught
            print(f"\n[WARNING] Bulk DB query failed for a chunk: {e}")

def get_bulk_canonical_tags(md5_set, db_conn, sqlite_conn, chunk_size=1000):
    """Fetches canonical tags for a large set of MD5s in bulk to avoid N+1 queries."""
    results = defaultdict(set)
    md5_list = list(md5_set)
    _fetch_sqlite_tags(md5_list, sqlite_conn, results)
    _fetch_postgres_tags(md5_list, db_conn, chunk_size, results)
    return results
