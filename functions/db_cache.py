import re
import sqlite3
import threading
import subprocess
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
import psycopg2
import psycopg2.extras
import tqdm
from urllib.parse import urlparse, parse_qs

from functions.common import compute_md5
from functions.media import compute_danbooru_pixel_hash
from functions.tags_curation import parse_tags

thread_local = threading.local()

@contextmanager
def get_cache_conn(cache):
    '''Use a connection cache'''
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
    except Exception as e:
        print(f"Error checking Shimmie2 database! ({type(e).__name__})")
        return "error"

def resolve_post(image: Path, shimmie_path, skip_existing, dbuser, cache) -> tuple:
    """Resolves the post information from the database or adds it to cache."""
    video_exts = {".gif", ".webm", ".mp4", ".flv", ".m4v", ".f4v", ".f4p", ".ogv"}
    with get_cache_conn(cache) as conn:
        cur = conn.cursor()
        post = None

        match = re.compile(r"[a-fA-F0-9]{32}").search(image.stem)
        md5 = match.group(0).lower() if match else compute_md5(image)
        is_video = image.suffix.lower() in video_exts
        px_hash = None

        if md5:
            cur.execute("SELECT * FROM posts WHERE md5 = ?", (md5,))
            if row := cur.fetchone():
                post = row_to_post_dict(row)
                cur.execute("SELECT pixel_hash FROM posts WHERE md5 = ?", (md5,))
                px_res = cur.fetchone()
                px_hash = px_res[0] if px_res else None

        try:
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
        except Exception as e:
            print(f"\n[ERROR] Skipping corrupt or unreadable image: {image}")
            print(f"        Reason: {type(e).__name__} - {e}")
            return image, {}, md5, None, "error"

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
    """Parses Shimmie2 config.php to extract PostgreSQL credentials across all DSN formats."""
    if not spath:
        return None

    config_path = Path(spath) / "data" / "config" / "shimmie.conf.php"
    if not config_path.is_file():
        print(f"[WARNING] Could not find Shimmie config at {spath}")
        return None

    content = config_path.read_text(encoding="utf-8")

    # Optional standalone user/pass definitions (Legacy configs)
    user_match = (
        re.search(r"\$database_user\s*=\s*['\"]([^'\"]+)['\"]", content) or
        re.search(r"define\s*\(\s*['\"]DATABASE_USER['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", content)
    )
    pass_match = (
        re.search(r"\$database_pass\s*=\s*['\"]([^'\"]+)['\"]", content) or
        re.search(r"define\s*\(\s*['\"]DATABASE_PASS['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", content)
    )

    # Case 1: URI-style DSN (e.g., pgsql://user:pass@localhost:5432/shimmie?host=/run/postgresql)
    uri_match = re.search(r"['\"]((?:pgsql|postgres|postgresql)://[^'\"]+)['\"]", content)
    if uri_match:
        parsed = urlparse(uri_match.group(1))
        query_params = parse_qs(parsed.query)
        host = query_params.get("host", [parsed.hostname or "localhost"])[0]
        res = {
            "host": host,
            "dbname": parsed.path.lstrip("/") or "shimmie",
            "user": parsed.username or (user_match.group(1) if user_match else "postgres"),
            "password": parsed.password or (pass_match.group(1) if pass_match else ""),
        }
        if parsed.port:
            res["port"] = parsed.port
        return res

    # Case 2: PDO key-value DSN (e.g., pgsql:user=miyuu;host=/run/postgresql;dbname=shimmie)
    dsn_match = re.search(r"['\"]pgsql:([^'\"]+)['\"]", content)
    if not dsn_match:
        return None

    dsn_parts = dict(
        part.split("=", 1) for part in dsn_match.group(1).split(";") if "=" in part
    )
    res = {
        "host": dsn_parts.get("host", "localhost"),
        "dbname": dsn_parts.get("dbname", "shimmie"),
        "user": dsn_parts.get("user") or (user_match.group(1) if user_match else "postgres"),
        "password": (
            dsn_parts["password"]
            if "password" in dsn_parts
            else (pass_match.group(1) if pass_match else "")
        ),
    }
    if "port" in dsn_parts:
        res["port"] = dsn_parts["port"]
    return res

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
    """Helper to fetch tags from PostgreSQL in a single bulk pass."""
    if not db_conn:
        return

    missing = [m for m in md5_list if m not in results]
    if not missing:
        return

    try:
        with psycopg2.connect(**db_conn) as conn, conn.cursor() as cur:
            print("  -> Uploading hash list to temporary Postgres table...")
            cur.execute("CREATE TEMP TABLE tmp_hashes (hash VARCHAR(32)) ON COMMIT DROP")
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO tmp_hashes (hash) VALUES %s",
                [(m,) for m in missing],
                page_size=10000
            )
            cur.execute("ANALYZE tmp_hashes")

            print("  -> Streaming matched tags from Postgres...")
            cur.execute("""
                SELECT i.hash, array_agg(t.tag)
                FROM tmp_hashes th
                JOIN images i ON i.hash = th.hash
                JOIN image_tags it ON i.id = it.image_id
                JOIN tags t ON it.tag_id = t.id
                GROUP BY i.hash;
            """)

            for hsh, tag_list in tqdm.tqdm(cur.fetchall(), desc="Reading Postgres tags", leave=False):
                results[hsh].update(t.strip() for t in tag_list if t)
    except Exception as e:
        print(f"\n[WARNING] Bulk DB query failed: {e}")

def get_bulk_canonical_tags(md5_set, db_conn, sqlite_conn, chunk_size=1000):
    """Fetches canonical tags for a large set of MD5s in bulk to avoid N+1 queries."""
    results = defaultdict(set)
    md5_list = list(md5_set)
    _fetch_sqlite_tags(md5_list, sqlite_conn, results)
    _fetch_postgres_tags(md5_list, db_conn, chunk_size, results)
    return results
