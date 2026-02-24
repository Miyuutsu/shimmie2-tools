# pylint: disable=duplicate-code
"""Wiki management tools (Indexing and Danbooru Imports)."""
import re
import html
import sqlite3
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote, urljoin

import requests

# Optional import to allow modularity without crashing if file is missing
try:
    from functions.captcha import get_protected_session, AntiBotSolver
except ImportError:
    get_protected_session = None
    AntiBotSolver = None

BASE_BOORU_URL = "https://danbooru.donmai.us"
SQLITE_DB = Path("database/danbooru_wiki_cache.db")

# ==========================================
# CSS & Static Assets
# ==========================================
def _fetch_site_css(session, out_dir):
    """
    Attempts to scrape the original CSS from the site homepage.
    Falls back to a default style if extraction fails.
    """
    css_path = out_dir / "style.css"
    print("[INFO] Attempting to fetch original site CSS...")

    try:
        # 1. Scrape Homepage for CSS link
        resp = session.get(BASE_BOORU_URL, timeout=10)
        resp.raise_for_status()

        # Regex to find <link rel="stylesheet" href="...">
        # Matches href="style.css" or href="/css/style.css?v=2"
        pattern = (
            r'<link[^>]+rel=["\']?stylesheet["\']?[^>]+href=["\']?'
            r'([^"\'>]+)["\']?'
        )
        match = re.search(pattern, resp.text)

        if match:
            css_url = match.group(1)
            # Handle relative URLs
            if not css_url.startswith('http'):
                css_url = urljoin(BASE_BOORU_URL, css_url)

            print(f"[INFO] Found CSS at: {css_url}")
            css_resp = session.get(css_url, timeout=10)
            css_resp.raise_for_status()

            with css_path.open("w", encoding="utf-8") as f:
                f.write(css_resp.text)
            print("[✓] Saved original CSS.")
            return "style.css"

    except (requests.RequestException, OSError) as e:
        print(f"[WARNING] CSS fetch failed ({e}). Using default style.")

    # 2. Fallback Default CSS
    default_css = """
    body { font-family: sans-serif; max-width: 900px; margin: 20px auto;
           padding: 0 10px; line-height: 1.6; background: #f0f0f0; color: #333; }
    .page-container { background: #fff; padding: 20px; border: 1px solid #ccc;
                      box-shadow: 2px 2px 5px rgba(0,0,0,0.1); }
    a { color: #007bff; text-decoration: none; }
    a:hover { text-decoration: underline; }
    h1 { border-bottom: 2px solid #eee; padding-bottom: 10px; }
    .nav { margin-bottom: 20px; font-size: 0.9em; padding: 5px;
           background: #e9ecef; border-radius: 4px; }
    .history-section { margin-top: 40px; padding-top: 20px;
                       border-top: 1px dashed #ccc; font-size: 0.9em; }
    .history-list { max-height: 200px; overflow-y: auto; }
    """
    with css_path.open("w", encoding="utf-8") as f:
        f.write(default_css)
    return "style.css"


# ==========================================
# Static Site Generator Helpers
# ==========================================
def _sanitize_fs_name(name):
    """
    Sanitizes a wiki title for use as a filename.
    Replaces unsafe FS chars (/, \\, :, *, ?, ", <, >, |) with underscore.
    Keeps ' and ; as requested.
    """
    clean = re.sub(r'[<>:"/\\|?*]', '_', name)
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

def _make_link(target, label=None, relative_to_bucket=None, rev_id=None):
    """Creates a relative HTML link to another wiki page or specific revision."""
    safe_target = _sanitize_fs_name(target.replace(' ', '_'))
    bucket = _get_bucket(safe_target)

    filename = f"{safe_target}.html"
    if rev_id:
        filename = f"{safe_target}_rev{rev_id}.html"

    if relative_to_bucket:
        # Link from pages/X/file.html -> pages/Y/target.html
        href = f"../{bucket}/{quote(filename)}"
    else:
        # Link from root index.html -> pages/Y/target.html
        href = f"pages/{bucket}/{quote(filename)}"

    return f'<a href="{href}">{html.escape(label or target)}</a>'

def _shimmie_to_html(text, current_bucket):
    """Converts Shimmie markup (BBCode-ish) to simple HTML."""
    if not text:
        return ""
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

    # External Links
    text = re.sub(
        r'\[url=([^\]]+)\](.*?)\[/url\]',
        r'<a href="\1" target="_blank">\2</a>',
        text
    )
    text = text.replace('\n', '<br>')
    return text

def _write_static_page(out_dir, entry, revisions, css_file):
    """
    Writes a single HTML page (and its revisions).
    entry: The 'Main' entry (latest/current).
    revisions: A list of other entries (historical).
    """
    title = entry['title']
    safe_name = _sanitize_fs_name(title.replace(' ', '_'))
    bucket = _get_bucket(safe_name)
    bucket_dir = out_dir / "pages" / bucket
    bucket_dir.mkdir(parents=True, exist_ok=True)

    # 1. Write Main Page
    main_ctx = {
        "title": title,
        "body": entry['body'],
        "bucket": bucket,
        "revisions": revisions,
        "is_rev": False,
        "parent_link": None,
        "meta_info": ""
    }
    _write_html_file(bucket_dir / f"{safe_name}.html", main_ctx, css_file)

    # 2. Write Revision Pages
    for rev in revisions:
        rev_id = rev['remote_id']
        rev_ctx = {
            "title": f"{title} (Rev {rev_id})",
            "body": rev['body'],
            "bucket": bucket,
            "revisions": [],
            "is_rev": True,
            "parent_link": f"{safe_name}.html",
            "meta_info": f"Source: {rev['source']} | Date: {rev['updated_at']}"
        }
        _write_html_file(
            bucket_dir / f"{safe_name}_rev{rev_id}.html", rev_ctx, css_file
        )

def _build_history_html(revisions, title, bucket):
    """Generates the history section HTML list."""
    if not revisions:
        return ""

    history_list = []
    # Sort revisions by ID descending (newest first)
    for r in sorted(revisions, key=lambda x: x['remote_id'], reverse=True):
        link = _make_link(title, f"Revision {r['remote_id']}", bucket, r['remote_id'])
        src = r['source']
        date = r['updated_at'] or "N/A"
        history_list.append(f"<li>{link} <small>({src} - {date})</small></li>")

    return f"""
    <div class="history-section">
        <h3>History & Versions</h3>
        <ul class="history-list">{''.join(history_list)}</ul>
    </div>
    """

def _write_html_file(path, ctx, css_file):
    """Inner helper to dump the HTML string."""
    title = ctx['title']
    bucket = ctx['bucket']

    history_html = _build_history_html(ctx['revisions'], title, bucket)

    nav_links = '<a href="../../index.html">← Back to Index</a>'
    if ctx['is_rev'] and ctx['parent_link']:
        # Use quote for the parent link filename to handle special chars like '
        nav_links += f' | <a href="{quote(ctx["parent_link"])}">Current Version</a>'

    meta_html = ""
    if ctx['meta_info']:
        meta_html = f'<p><small>{ctx["meta_info"]}</small></p>'

    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{html.escape(title)}</title>
    <link rel="stylesheet" href="../../{css_file}">
</head>
<body>
    <div class="page-container">
        <div class="nav">{nav_links}</div>
        <h1>{html.escape(title)}</h1>
        {meta_html}
        <div>{_shimmie_to_html(ctx['body'], bucket)}</div>
        {history_html}
    </div>
</body>
</html>"""

    with path.open("w", encoding="utf-8") as f:
        f.write(html_content)


# ==========================================
# Data Fetchers
# ==========================================
def _get_entries_sqlite():
    """Fetch all entries from SQLite and group them by title."""
    if not SQLITE_DB.exists():
        print(f"[ERROR] {SQLITE_DB} not found.")
        return {}

    entries = defaultdict(list)
    try:
        with sqlite3.connect(SQLITE_DB) as conn:
            # Use Row factory to access columns by name
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM wiki_cache ORDER BY title ASC")
            for row in cursor:
                entries[row['title']].append(dict(row))
    except sqlite3.Error as err:
        print(f"[ERROR] SQLite error: {err}")
        return {}
    return entries

def _get_sorted_index_html(titles, args, css_file):
    """Generates the main index.html content."""
    content = ""

    # Build links dict
    # We only link to the Main page (no rev_id)
    links = [_make_link(t, t, relative_to_bucket=None) for t in titles]

    if args.sort:
        buckets = defaultdict(list)
        for t in titles:
            bk = _get_bucket(t)
            link = _make_link(t, t, relative_to_bucket=None)
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
    <link rel="stylesheet" href="{css_file}">
</head>
<body>
    <div class="page-container">
        <h1>Wiki Index</h1>
        {content}
    </div>
</body>
</html>"""

def _score_wiki_entry(row):
    """Scores an entry to determine if it should be the 'Main' page."""
    src = row['source']
    if 'wiki_pages' in src:
        return 2
    if 'artist' in src and 'version' not in src:
        return 2  # theoretical artist main endpoint
    if 'artist_version' in src:
        return 1
    return 0

def create_index(args):
    """Main execution for creating the static wiki site."""
    out_dir = Path(args.output)

    # 1. Fetch Data
    # For this advanced multi-version logic, we strictly use the SQLite cache
    print("[INFO] Fetching wiki data from SQLite...")
    grouped_entries = _get_entries_sqlite()

    if not grouped_entries:
        print("[ERROR] No wiki entries found.")
        return

    print(f"[INFO] Found {len(grouped_entries)} unique titles. Generating static site...")

    # 2. Setup Directory
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "pages").mkdir(exist_ok=True)

    # 3. Fetch CSS
    session = requests.Session()
    # If using captcha module:
    if args.spath or Path("cookies.txt").exists():
        # Simple check if we can upgrade session
        if get_protected_session:
            session = get_protected_session()

    css_file = _fetch_site_css(session, out_dir)

    # 4. Generate Pages
    count = 0

    for _, rows in grouped_entries.items():
        # Determine Main vs History
        # Priority: source='wiki_pages.json' > newest updated_at

        # Sort by Score DESC, then Date DESC
        rows.sort(
            key=lambda x: (_score_wiki_entry(x), x['updated_at'] or ""),
            reverse=True
        )

        main_entry = rows[0]
        revisions = rows[1:] if len(rows) > 1 else []

        _write_static_page(out_dir, main_entry, revisions, css_file)

        count += 1
        if count % 1000 == 0:
            print(f"   ...processed {count} titles", end="\r")

    print(f"[✓] Generated pages for {count} titles in '{out_dir}/pages/'")

    # 5. Generate Main Index
    index_html = _get_sorted_index_html(sorted(grouped_entries.keys()), args, css_file)
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

    # SCHEMA CHECK & MIGRATION
    # We moved from PK(id) -> PK(title) -> PK(unique_key string)
    needs_rebuild = False
    try:
        cur.execute("PRAGMA table_info(wiki_cache)")
        cols = {row[1] for row in cur.fetchall()}
        # If 'source' column is missing, it's an old schema
        if cols and 'source' not in cols:
            needs_rebuild = True
    except sqlite3.Error:
        pass

    if needs_rebuild:
        print("[WARN] Schema change detected (Multi-Endpoint support). Dropping old table...")
        cur.execute("DROP TABLE IF EXISTS wiki_cache")

    # New Schema: Composite Key to handle multiple endpoints sharing IDs
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wiki_cache (
            unique_key TEXT PRIMARY KEY,
            remote_id INTEGER,
            title TEXT,
            body TEXT,
            updated_at TEXT,
            source TEXT,
            imported BOOLEAN DEFAULT 0
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_title ON wiki_cache(title)")
    conn.commit()
    return conn, cur

def _get_page_data(session, page, args, solver):
    """Helper to fetch a single page of data."""
    target_url = urljoin(BASE_BOORU_URL, args.endpoint)
    try:
        resp = session.get(target_url, params={"page": page, "limit": 1000}, timeout=30)

        if args.captcha and solver:
            if solver.detect(resp.text[:2000]):
                if solver.solve(session, resp.text, resp.url):
                    print("[INFO] Captcha solved. Retrying...")
                    resp = session.get(
                        target_url, params={"page": page, "limit": 1000}, timeout=30
                    )
                else:
                    return None

        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"[ERROR] Fetch failed: {e}")
        return None

def _process_wiki_entry(cursor, entry, args):
    """Processes a single wiki entry."""
    title = entry.get("title") or entry.get("name")
    entry_id = entry.get("id")

    if not title or not entry_id:
        return

    # Handle body variations
    body = entry.get("body", "")
    if not body and "urls" in entry and isinstance(entry["urls"], list):
        body = "\n".join(entry["urls"]) # Artist versions often behave this way

    body = body.replace('\r\n', '\n').strip()
    title = title.strip()

    # Generate Unique Key: "wiki_pages.json_50" or "artist_versions.json_50"
    unique_key = f"{args.endpoint}_{entry_id}"

    cursor.execute("SELECT body FROM wiki_cache WHERE unique_key = ?", (unique_key,))
    row = cursor.fetchone()

    if row is None:
        cursor.execute("""
            INSERT INTO wiki_cache (
                unique_key, remote_id, title, body, updated_at, source, imported
            ) VALUES (?, ?, ?, ?, ?, ?, 0)
        """, (
            unique_key, entry_id, title, body,
            entry.get("updated_at", ""), args.endpoint
        ))
    else:
        # Only update if changed
        if args.update_cache and body.strip() != row[0].strip():
            cursor.execute("""
                UPDATE wiki_cache
                SET body = ?, title = ?, updated_at = ?, source = ?
                WHERE unique_key = ?
            """, (
                body, title, entry.get("updated_at", ""),
                args.endpoint, unique_key
            ))

def _fetch_and_cache(args):
    """Pulls directly from Danbooru API and caches in SQLite."""
    conn, cur = _init_cache()

    # Session Setup
    session = requests.Session()
    solver = None
    if args.captcha and get_protected_session:
        session = get_protected_session()
        solver = AntiBotSolver()

    for page in range(args.start_page, args.start_page + args.pages):
        print(f"📦 Fetching {args.endpoint} page {page}...")
        data = _get_page_data(session, page, args, solver)
        if not data:
            continue

        for entry in data:
            _process_wiki_entry(cur, entry, args)

    conn.commit()
    return conn

def import_danbooru(args):
    """Main execution for fetching and importing Danbooru wikis."""
    print("=== Wiki Import Summary (Multi-Source) ===")
    print(f"🔗  Endpoint:       {args.endpoint}")
    print(f"💽  Database:       {SQLITE_DB}")
    print(f"📄  Pages:          {args.start_page} to {args.start_page + args.pages}")

    if args.clear_cache and SQLITE_DB.exists():
        SQLITE_DB.unlink()
        print("🧹 Cleared wiki cache.")

    cache_conn = _fetch_and_cache(args)
    cache_conn.close()

    print(f"\n[✓] Data cached to {SQLITE_DB}")
    print("    Note: To push to Shimmie Postgres, use 'make-csv' or custom scripts.")
    print("    This tool now optimizes for the 'wiki-index' Static Site Generator.")
