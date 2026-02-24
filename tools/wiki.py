# pylint: disable=duplicate-code
"""Wiki management tools (Indexing and Danbooru Imports)."""
import re
import html
import sqlite3
from collections import namedtuple, defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import quote, urljoin

import psycopg2
import requests

from functions.db_cache import get_shimmie_db_credentials

# Optional import to allow modularity without crashing if file is missing
try:
    from functions.captcha import get_protected_session, AntiBotSolver
except ImportError:
    get_protected_session = None
    AntiBotSolver = None

BASE_BOORU_URL = "https://danbooru.donmai.us/"
SQLITE_DB = Path("database/danbooru_wiki_cache.db")
POSTS_DB = Path("database/posts_cache.db")

CategoryConfig = namedtuple('CategoryConfig', ['prefix', 'title', 'col'])

# ==========================================
# Static Site Generator Helpers
# ==========================================
def _sanitize_fs_name(name):
    """
    Sanitizes a wiki title for use as a filename.
    Replaces unsafe FS chars (/, \\, :, *, ?, ", <, >, |) with underscore.
    Keeps ' and ; as requested, but handles them carefully.
    """
    # Replace dangerous characters
    clean = re.sub(r'[<>:"/\\|?*]', '_', name)
    # Strip leading/trailing spaces or dots (Windows doesn't like trailing dots)
    return clean.strip(" .")

def _get_bucket(name):
    """Determines the subdirectory bucket (a, b, ..., #) for a file."""
    if not name:
        return "misc"
    char = name[0].lower()
    if 'a' <= char <= 'z':
        return char
    if '0' <= char <= '9':
        return "#"
    return "misc"

def _make_link(target, label=None, relative_to_bucket=None):
    """Creates a relative HTML link to another wiki page."""
    safe_target = _sanitize_fs_name(target.replace(' ', '_'))
    bucket = _get_bucket(safe_target)

    if relative_to_bucket:
        # Link from pages/X/file.html -> pages/Y/target.html
        # Path is ../Y/target.html
        href = f"../{bucket}/{quote(safe_target)}.html"
    else:
        # Link from root index.html -> pages/Y/target.html
        href = f"pages/{bucket}/{quote(safe_target)}.html"

    return f'<a href="{href}">{html.escape(label or target)}</a>'

def _shimmie_to_html(text, current_bucket):
    """
    Converts Shimmie markup (BBCode-ish) to simple HTML for static viewing.
    """
    if not text:
        return ""

    # Escape HTML first
    text = html.escape(text)

    # Headers
    text = re.sub(r'\[h(\d)\](.*?)\[/h\1\]', r'<h\1>\2</h\1>', text)

    # Formatting
    text = re.sub(r'\[b\](.*?)\[/b\]', r'<strong>\1</strong>', text)
    text = re.sub(r'\[i\](.*?)\[/i\]', r'<em>\1</em>', text)
    text = re.sub(r'\[u\](.*?)\[/u\]', r'<u>\1</u>', text)
    text = re.sub(r'\[s\](.*?)\[/s\]', r'<s>\1</s>', text)

    # Links: [[Target]] or [[Target|Label]]
    def link_repl(match):
        content = match.group(1)
        if '|' in content:
            target, label = content.split('|', 1)
            return _make_link(target, label, current_bucket)
        return _make_link(content, content, current_bucket)

    text = re.sub(r'\[\[(.*?)\]\]', link_repl, text)

    # External Links [url=...]...[/url]
    text = re.sub(
        r'\[url=([^\]]+)\](.*?)\[/url\]',
        r'<a href="\1" target="_blank">\2</a>',
        text
    )

    # Newlines to <br>
    text = text.replace('\n', '<br>')

    return text

def _write_static_page(out_dir, title, body):
    """Writes a single HTML page."""
    safe_name = _sanitize_fs_name(title.replace(' ', '_'))
    bucket = _get_bucket(safe_name)

    bucket_dir = out_dir / "pages" / bucket
    bucket_dir.mkdir(parents=True, exist_ok=True)

    file_path = bucket_dir / f"{safe_name}.html"

    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{html.escape(title)}</title>
    <style>
        body {{
            font-family: sans-serif;
            max-width: 800px;
            margin: 20px auto;
            padding: 0 10px;
            line-height: 1.6;
        }}
        a {{ color: #007bff; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        h1 {{ border-bottom: 2px solid #eee; padding-bottom: 10px; }}
        .nav {{ margin-bottom: 20px; font-size: 0.9em; }}
    </style>
</head>
<body>
    <div class="nav">
        <a href="../../index.html">← Back to Index</a>
    </div>
    <h1>{html.escape(title)}</h1>
    <div>
        {_shimmie_to_html(body, bucket)}
    </div>
</body>
</html>"""

    with file_path.open("w", encoding="utf-8") as f:
        f.write(html_content)

# ==========================================
# Data Fetchers
# ==========================================
def _get_entries_pg(spath):
    """Fetch all (title, body) pairs from Postgres."""
    db_config = get_shimmie_db_credentials(spath)
    if not db_config:
        print(f"[ERROR] Could not load DB credentials from {spath}")
        return []
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT title, body FROM wiki_pages ORDER BY title ASC")
                return cursor.fetchall()
    except psycopg2.Error as err:
        print(f"[ERROR] Postgres error: {err}")
        return []

def _get_entries_sqlite():
    """Fetch all (title, body) pairs from SQLite cache."""
    if not SQLITE_DB.exists():
        print(f"[ERROR] No Shimmie path provided and {SQLITE_DB} not found.")
        return []
    try:
        with sqlite3.connect(SQLITE_DB) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT title, body FROM wiki_cache ORDER BY title ASC")
            return cursor.fetchall()
    except sqlite3.Error as err:
        print(f"[ERROR] SQLite error: {err}")
        return []

def _get_sorted_index_html(entries, args):
    """Generates the main index.html content."""

    # 1. Build Dictionary of Links
    links = []
    for title, _ in entries:
        links.append(_make_link(title, title, relative_to_bucket=None))

    content = ""

    if args.sort:
        buckets = defaultdict(list)
        for title, _ in entries:
            bk = _get_bucket(title)
            link = _make_link(title, title, relative_to_bucket=None)
            buckets[bk].append(link)

        keys = sorted(buckets.keys())
        if '#' in keys:
            keys.remove('#')
            keys.insert(0, '#')

        for k in keys:
            content += f"<h2>{k.upper()}</h2>\n<ul>\n"
            for link in buckets[k]:
                content += f"<li>{link}</li>\n"
            content += "</ul>\n"

    else:
        content = "<ul>\n" + "\n".join([f"<li>{l}</li>" for l in links]) + "\n</ul>"

    return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Wiki Index</title>
    <style>
        body {{ font-family: sans-serif; max-width: 900px; margin: 20px auto; padding: 20px; }}
        h1 {{ text-align: center; }}
        h2 {{ border-bottom: 1px solid #ccc; margin-top: 30px; }}
        ul {{ list-style-type: none; padding: 0; display: flex; flex-wrap: wrap; gap: 10px; }}
        li {{ flex: 1 0 200px; }}
        a {{ text-decoration: none; color: #333; }}
        a:hover {{ color: #007bff; }}
    </style>
</head>
<body>
    <h1>Wiki Index</h1>
    {content}
</body>
</html>"""

def create_index(args):
    """Main execution for creating the static wiki site."""
    out_dir = Path(args.output)

    # 1. Fetch Data
    print("[INFO] Fetching wiki data...")
    if args.spath:
        entries = _get_entries_pg(args.spath)
    else:
        entries = _get_entries_sqlite()

    if not entries:
        print("[ERROR] No wiki entries found.")
        return

    print(f"[INFO] Found {len(entries)} pages. Generating static site...")

    # 2. Create Directory Structure
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "pages").mkdir(exist_ok=True)

    # 3. Generate Individual Pages
    count = 0
    for title, body in entries:
        _write_static_page(out_dir, title, body)
        count += 1
        if count % 1000 == 0:
            print(f"   ...processed {count} pages", end="\r")

    print(f"[✓] Generated {count} HTML pages in '{out_dir}/pages/'")

    # 4. Generate Main Index
    index_html = _get_sorted_index_html(entries, args)
    with (out_dir / "index.html").open("w", encoding="utf-8") as f:
        f.write(index_html)

    print(f"[✓] Main index created at '{out_dir}/index.html'")


# ==========================================
# Tool 2: Import Danbooru Wikis
# ==========================================
def _init_cache():
    """Initializes SQLite cache for Danbooru wikis."""
    SQLITE_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(SQLITE_DB)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wiki_cache (
            id INTEGER PRIMARY KEY, title TEXT UNIQUE, body TEXT,
            updated_at TEXT, imported BOOLEAN DEFAULT 0
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_title ON wiki_cache(title)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_imported ON wiki_cache(imported)")
    conn.commit()
    return conn, cur

def _process_toc_lines(toc_lines, title):
    """Processes Table of Contents block to reduce locals."""
    toc_processed = ["[h3][b]Table of Contents[/b][/h3]"]
    for toc_line in toc_lines:
        match = re.match(
            r'^(\*+)\s*(?:([\dA-Za-z.]+)\.\s*)?"(.+?)":#([^\s]+)', toc_line.strip()
        )
        if match:
            stars, number, label, anchor = match.groups()
            indentation = '\u00A0' * (len(stars) - 1)
            separator = '' if '.' in (number or '') else '.'
            anchor = anchor.replace('dtext-', '')
            formatted = (
                f"{indentation}• {number or ''}{separator} "
                f"[url=site://wiki/{title}#bb-{anchor}]{label}[/url]"
            )
            toc_processed.append(formatted)
    return toc_processed

# FIX: Removed the unused 'line' argument
def _process_header(match, is_anchor):
    """Processes headers to reduce locals."""
    level = match.group(1)
    label = match.group(3) if is_anchor else match.group(2)

    if level == '2':
        level = '1'
    elif level == '3':
        level = '2'
    elif level == '4':
        level = '3'
    elif level in ('5', '6'):
        level = '4'

    if is_anchor:
        anchor = match.group(2)
        return f"[anchor={anchor}][/anchor][h{level}]{label}[/h{level}]"
    return f"[h{level}]{label}[/h{level}]"

def _clean_wiki_body(text: str, title: str) -> str:
    """Sanitize and convert Danbooru wiki body to Shimmie2-friendly format."""
    lines = []
    in_toc, toc_lines = False, []

    for line in text.splitlines():
        line = line.rstrip()
        if line.strip() == "":
            lines.append(line)
            continue

        if line.strip().lower() == "[expand=table of contents]":
            in_toc, toc_lines = True, []
            continue

        if in_toc:
            if line.strip().lower() == "[/expand]":
                in_toc = False
                lines.extend(_process_toc_lines(toc_lines, title))
            else:
                toc_lines.append(line)
            continue

        if any(substr in line.lower() for substr in (
            "/user_upgrades/new", "gold+ account", "premium users",
            "see also: forum", "available to supporters",
        )):
            continue

        line = re.sub(r'<a href="([^"]+)">(.+?)</a>', r'[url=\1]\2[/url]', line)
        line = re.sub(r'"([^"]+?)":/wiki/([a-zA-Z0-9_:]+)', r'[[\2|\1]]', line)
        line = re.sub(r'\[/forum_topics/\d+\]', '', line)
        line = re.sub(r'<(ul|ol|p)>\s*</\1>', '', line)

        header_anchor = re.match(r'^h([1-6])#([a-zA-Z0-9_-]+)\.\s*(.+)', line)
        header_plain = re.match(r'^h([1-6])\.\s*(.+)', line)

        if header_anchor or header_plain:
            line = _process_header(header_anchor or header_plain, bool(header_anchor))

        line = re.sub(r'!post\s+#\d+', '>>0', line)

        list_match = re.match(r'^(\*+)\s*(.*)', line)
        if list_match:
            asterisks, content = list_match.groups()
            spaces = '  ' * (len(asterisks) - 1) if len(asterisks) > 1 else ''
            line = f"{spaces}• {content}"

        lines.append(line)

    cleaned = "\n".join(lines)

    def repl_spaces(m):
        c = m.group(1)
        if '|' in c:
            t, l = c.split('|', 1)
            return f"[[{t.replace(' ', '_')}|{l}]]"
        return f"[[{c.replace(' ', '_')}]]"

    cleaned = re.sub(r'\[\[([^\]]+)\]\]', repl_spaces, cleaned)
    return re.sub(r'\n{3,}', '\n\n', cleaned).strip()

def _markdown_to_html(text):
    """Converts basic markdown elements to HTML."""
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"__(.+?)__", r"<strong>\1</strong>", text)
    text = re.sub(r"\*(.+?)\*", r"<em>\1</em>", text)
    text = re.sub(r"_(.+?)_", r"<em>\1</em>", text)
    text = re.sub(r"(?m)^[-*]\s+(.*)", r"<li>\1</li>", text)
    text = re.sub(r"(?s)(<li>.*?</li>)", r"<ul>\1</ul>", text)
    return text

def _insert_or_update_pg(pg_cur, title, body, existing_titles, update_existing=False):
    """Handles insertion or revision updates in Postgres."""
    if title not in existing_titles:
        pg_cur.execute("""
            INSERT INTO wiki_pages (owner_id, owner_ip, date, title, revision, locked, body)
            VALUES (1, '127.0.0.1', %s, %s, 1, false, %s)
        """, (datetime.now(), title, body))
        return "inserted"

    if update_existing:
        pg_cur.execute(
            "SELECT revision, body FROM wiki_pages WHERE title = %s ORDER BY revision DESC LIMIT 1",
            (title,)
        )
        current = pg_cur.fetchone()
        if current:
            current_rev, current_body = current
            if '[[shimmie:lock]]' in current_body:
                print(f"🔒 Skipped locked entry: {title}")
                return "skipped"
            if body.strip() != current_body.strip():
                pg_cur.execute("""
                    INSERT INTO wiki_pages (owner_id, owner_ip, date, title, revision, locked, body)
                    VALUES (1, '127.0.0.1', %s, %s, %s, false, %s)
                """, (datetime.now(), title, current_rev + 1, body))
                return "updated"
    return "skipped"

def _get_page_data(session, page, args, solver):
    """Helper to fetch a single page of data, handling retries and captcha."""
    # Construct the full target URL based on the selected endpoint
    target_url = urljoin(BASE_BOORU_URL, args.endpoint)

    try:
        resp = session.get(target_url, params={"page": page, "limit": 1000}, timeout=30)

        # === CAPTCHA INTERCEPT ===
        if args.captcha and solver:
            # Check start of response for captcha markers
            if solver.detect(resp.text[:2000]):
                if solver.solve(session, resp.text, resp.url):
                    print("[INFO] Retrying original request...")
                    resp = session.get(
                        target_url, params={"page": page, "limit": 1000}, timeout=30
                    )
                else:
                    print("[ERROR] Failed to solve captcha. Skipping page.")
                    return None
        # =========================

        resp.raise_for_status()
        return resp.json()

    except requests.RequestException as e:
        print(f"[ERROR] Failed fetching page {page}: {e}")
        return None

def _process_wiki_entry(cursor, entry, args):
    """Processes a single wiki entry and updates the cache."""
    # Support both standard wiki format and artist_version format
    title = entry.get("title") or entry.get("name")
    entry_id = entry.get("id")

    if not title or not entry_id:
        return

    # Handle body: standard wiki vs artist versions (urls list)
    body = entry.get("body", "")
    if not body and "urls" in entry and isinstance(entry["urls"], list):
        body = "\n".join(entry["urls"])

    body = body.replace('\r\n', '\n').strip()
    title = title.strip()

    if args.convert == "markdown":
        body = _markdown_to_html(body)
    elif args.convert == "shimmie":
        body = _clean_wiki_body(body, title)

    cursor.execute("SELECT body FROM wiki_cache WHERE title = ?", (title,))
    row = cursor.fetchone()

    if row is None:
        cursor.execute("""
            INSERT OR REPLACE INTO wiki_cache (id, title, body, updated_at, imported)
            VALUES (?, ?, ?, ?, 0)
        """, (entry_id, title, body, entry.get("updated_at", "")))
    else:
        if args.update_cache and body.strip() != row[0].strip():
            cursor.execute(
                "UPDATE wiki_cache SET body = ?, updated_at = ? WHERE title = ?",
                (body, entry.get("updated_at", ""), title)
            )
        else:
            cursor.execute(
                "UPDATE wiki_cache SET imported = 0 WHERE title = ?", (title,)
            )

def _fetch_and_cache(args):
    """Pulls directly from Danbooru API and caches in SQLite."""
    conn, cur = _init_cache()
    if not args.update_cache:
        cur.execute("UPDATE wiki_cache SET imported = 1")

    # === CAPTCHA / SESSION LOGIC ===
    if args.captcha:
        if not get_protected_session:
            print("[ERROR] Captcha module not found.")
            return conn
        session = get_protected_session()
        solver = AntiBotSolver()
    else:
        session = requests.Session()
        solver = None
    # ===============================

    for page in range(args.start_page, args.start_page + args.pages):
        print(f"📦 Fetching API page {page}...")

        data = _get_page_data(session, page, args, solver)
        if not data:
            continue

        for entry in data:
            _process_wiki_entry(cur, entry, args)

    conn.commit()
    return conn

def import_danbooru(args):
    """Main execution for fetching and importing Danbooru wikis."""
    if args.spath:
        db_config = get_shimmie_db_credentials(args.spath)
        if not db_config:
            print(f"[ERROR] Could not load DB credentials from {args.spath}")
            return
        print("=== Wiki Import Summary (Online) ===")
        print(f"📚  Database:       {db_config['dbname']}")
    else:
        print("=== Wiki Import Summary (Offline Cache) ===")
        print("💽  Target:         SQLite Cache Only")

    print(f"📄  Pages:          {args.start_page} to {args.start_page + args.pages}")
    print(f"🔗  Endpoint:       {args.endpoint}")
    print(f"🔄  Update Cache:   {'Yes' if args.update_cache else 'No'}")

    if args.clear_cache and SQLITE_DB.exists():
        SQLITE_DB.unlink()
        print("🧹 Cleared wiki cache.")

    cache_conn = _fetch_and_cache(args)

    # If no spath provided, stop here (Cache creation complete)
    if not args.spath:
        print(f"\n[✓] Wiki data cached to {SQLITE_DB}")
        cache_conn.close()
        return

    # Continue with Postgres import if spath exists
    cache_cur = cache_conn.cursor()

    pg_conn = psycopg2.connect(**db_config)
    pg_conn.set_client_encoding('UTF8')
    pg_cur = pg_conn.cursor()

    pg_cur.execute("SELECT title FROM wiki_pages")
    existing_titles = {row[0] for row in pg_cur.fetchall()}

    cache_cur.execute("SELECT title, body FROM wiki_cache WHERE imported = 0")
    rows = cache_cur.fetchall()
    print(f"📥 {len(rows)} wiki pages queued for import into Postgres.")

    results = {"inserted": 0, "updated": 0, "skipped": 0}
    for title, body in rows:
        result = _insert_or_update_pg(pg_cur, title, body, existing_titles, args.update_existing)
        results[result] += 1
        if result != "skipped":
            cache_cur.execute("UPDATE wiki_cache SET imported = 1 WHERE title = ?", (title,))

    pg_conn.commit()
    cache_conn.commit()
    pg_conn.close()
    cache_conn.close()

    print(
        f"\n✅ Inserted: {results['inserted']} | "
        f"🔁 Updated: {results['updated']} | "
        f"⏭️ Skipped: {results['skipped']}"
    )
