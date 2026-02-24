# pylint: disable=duplicate-code
"""Wiki management tools (Universal Indexing, Static Site Gen, and Archiving)."""
import re
import html
import json
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
def _get_custom_css():
    """Returns a 'pretty' custom CSS for the static site."""
    return """
    :root {
        --bg-color: #1e1e2e;
        --card-bg: #27273a;
        --text-main: #e2e2e5;
        --text-muted: #a5a5ad;
        --accent: #8ab4f8;
        --accent-hover: #aec9ff;
        --border: #3b3b4f;
    }
    body {
        font-family: 'Segoe UI', system-ui, sans-serif;
        background-color: var(--bg-color);
        color: var(--text-main);
        line-height: 1.6;
        margin: 0;
        padding: 20px;
    }
    .page-container {
        max-width: 900px;
        margin: 0 auto;
        background: var(--card-bg);
        padding: 40px;
        border-radius: 12px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.4);
        border: 1px solid var(--border);
    }
    h1, h2, h3 {
        color: #fff; margin-top: 0;
        border-bottom: 1px solid var(--border); padding-bottom: 10px;
    }
    h1 { font-size: 2em; margin-bottom: 20px; }
    a { color: var(--accent); text-decoration: none; transition: color 0.2s; }
    a:hover { color: var(--accent-hover); text-decoration: underline; }
    .nav {
        margin-bottom: 30px;
        font-size: 0.9em;
        background: rgba(0,0,0,0.2);
        padding: 10px 15px;
        border-radius: 8px;
        border-left: 4px solid var(--accent);
    }
    .meta-info {
        color: var(--text-muted); font-size: 0.85em;
        margin-bottom: 25px; font-family: monospace;
    }
    .content-body { font-size: 1.05em; }
    .history-section {
        margin-top: 60px; padding-top: 20px;
        border-top: 2px dashed var(--border);
    }
    .history-list {
        list-style: none; padding: 0;
        max-height: 300px; overflow-y: auto;
    }
    .history-list li {
        padding: 8px 0;
        border-bottom: 1px solid rgba(255,255,255,0.05);
        display: flex;
        justify-content: space-between;
    }
    .tag-block {
        background: rgba(255,255,255,0.05);
        padding: 2px 6px;
        border-radius: 4px;
        font-family: monospace;
        font-size: 0.9em;
    }
    """

def _write_css_file(out_dir):
    """Writes the custom CSS to file."""
    css_path = out_dir / "style.css"
    with css_path.open("w", encoding="utf-8") as f:
        f.write(_get_custom_css())
    return "style.css"


# ==========================================
# Static Site Generator Helpers
# ==========================================
def _sanitize_fs_name(name):
    """Sanitizes a title for use as a filename."""
    if not name:
        return "unnamed_entity"
    # Replace dangerous characters with underscore
    clean = re.sub(r'[<>:"/\\|?*]', '_', str(name))
    return clean.strip(" .")

def _get_bucket(name):
    """Determines the subdirectory bucket (a, b, ..., #)."""
    if not name:
        return "misc"
    char = name[0].lower()
    if 'a' <= char <= 'z':
        return char
    if '0' <= char <= '9':
        return "#"
    return "misc"

def _make_link(target, label=None, relative_to_bucket=None, rev_id=None):
    """Creates a relative HTML link."""
    safe_target = _sanitize_fs_name(str(target).replace(' ', '_'))
    bucket = _get_bucket(safe_target)

    filename = f"{safe_target}.html"
    if rev_id:
        filename = f"{safe_target}_rev{rev_id}.html"

    if relative_to_bucket:
        href = f"../{bucket}/{quote(filename)}"
    else:
        href = f"pages/{bucket}/{quote(filename)}"

    return f'<a href="{href}">{html.escape(str(label or target))}</a>'

def _shimmie_to_html(text, current_bucket):
    """Converts Shimmie markup (BBCode-ish) to simple HTML."""
    if not text:
        return "<p><em>No content available.</em></p>"
    text = html.escape(str(text))

    # Headers
    text = re.sub(r'\[h(\d)\](.*?)\[/h\1\]', r'<h\1>\2</h\1>', text)
    # Formatting
    text = re.sub(r'\[b\](.*?)\[/b\]', r'<strong>\1</strong>', text)
    text = re.sub(r'\[i\](.*?)\[/i\]', r'<em>\1</em>', text)
    text = re.sub(r'\[u\](.*?)\[/u\]', r'<u>\1</u>', text)
    text = re.sub(r'\[s\](.*?)\[/s\]', r'<s>\1</s>', text)
    # Blockquote
    text = re.sub(
        r'\[quote\](.*?)\[/quote\]',
        r'<blockquote style="border-left: 3px solid #666; padding-left: 10px; '
        r'margin: 10px 0;">\1</blockquote>',
        text, flags=re.DOTALL
    )

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
    """Writes a single HTML page (and its revisions)."""
    title = entry['title']
    safe_name = _sanitize_fs_name(title.replace(' ', '_'))
    bucket = _get_bucket(safe_name)
    bucket_dir = out_dir / "pages" / bucket
    bucket_dir.mkdir(parents=True, exist_ok=True)

    # 1. Write Main Page
    meta_str = f"Source: {entry['source']} | Last Updated: {entry['updated_at']}"
    main_ctx = {
        "title": title,
        "body": entry['body'],
        "bucket": bucket,
        "revisions": revisions,
        "is_rev": False,
        "parent_link": None,
        "meta_info": meta_str
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
    for r in sorted(revisions, key=lambda x: x['remote_id'], reverse=True):
        link = _make_link(title, f"Rev {r['remote_id']}", bucket, r['remote_id'])
        src = r['source'].replace('.json', '')
        date = r['updated_at'] or "N/A"
        item = (
            f"<li><span>{link} <span class='tag-block'>{src}</span></span> "
            f"<small>{date}</small></li>"
        )
        history_list.append(item)

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
        nav_links += f' | <a href="{quote(ctx["parent_link"])}">Current Version</a>'

    meta_html = ""
    if ctx['meta_info']:
        meta_html = f'<div class="meta-info">{ctx["meta_info"]}</div>'

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
        <div class="content-body">{_shimmie_to_html(ctx['body'], bucket)}</div>
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

    # Simple list vs Sorted buckets
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
            style = (
                "list-style: none; padding: 0; "
                "display: flex; flex-wrap: wrap; gap: 10px;"
            )
            content += f"<h2>{k.upper()}</h2>\n<ul style='{style}'>\n"
            for link in buckets[k]:
                content += f"<li style='flex: 1 0 200px;'>{link}</li>\n"
            content += "</ul>\n"
    else:
        links = [_make_link(t, t, relative_to_bucket=None) for t in titles]
        style = "column-count: 3;"
        # Split line to satisfy pylint 100 char limit
        list_items = "\n".join([f"<li>{l}</li>" for l in links])
        content = f"<ul style='{style}'>\n{list_items}\n</ul>"

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
        return 3
    if 'artist' in src and 'version' not in src:
        return 2
    if 'pool' in src and 'version' not in src:
        return 2
    if 'version' in src:
        return 0
    return 1

def create_index(args):
    """Main execution for creating the static wiki site."""
    out_dir = Path(args.output)
    print("[INFO] Fetching wiki data from SQLite...")
    grouped_entries = _get_entries_sqlite()

    if not grouped_entries:
        print("[ERROR] No wiki entries found.")
        return

    print(f"[INFO] Found {len(grouped_entries)} unique titles. Generating static site...")

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "pages").mkdir(exist_ok=True)

    css_file = _write_css_file(out_dir)

    count = 0
    for _, rows in grouped_entries.items():
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

    # Schema Migration Check
    try:
        cur.execute("PRAGMA table_info(wiki_cache)")
        cols = {row[1] for row in cur.fetchall()}
        if cols and 'source' not in cols:
            print("[WARN] Migrating DB schema...")
            cur.execute("DROP TABLE IF EXISTS wiki_cache")
    except sqlite3.Error:
        pass

    # Generic schema to hold ANY textual content
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

def _format_entry_body(entry, endpoint):
    """
    Intelligently formats JSON data into a readable 'body' string based on the endpoint.
    This creates the "textual content" for our static site.
    """
    body_parts = []

    # 1. Base Body (if present)
    if entry.get("body"):
        body_parts.append(entry["body"])
    elif entry.get("description"):
        body_parts.append(entry["description"])
    elif entry.get("original_description"): # Artist Commentary
        body_parts.append(f"[h4]Description[/h4]\n{entry['original_description']}")

    # 2. Metadata Extraction
    meta = []

    # Artists
    if "artist" in endpoint:
        if "group_name" in entry and entry["group_name"]:
            meta.append(f"Group: {entry['group_name']}")
        if "other_names" in entry and entry["other_names"]:
            meta.append(f"Aliases: {entry['other_names']}")
        if "urls" in entry:
            urls = entry["urls"]
            if isinstance(urls, list):
                ulist = "\n".join(
                    [f" * {u.get('url', u) if isinstance(u, dict) else u}" for u in urls]
                )
                body_parts.append(f"[h4]Links[/h4]\n{ulist}")

    # Pools
    if "pool" in endpoint and "post_ids" in entry:
        pids = entry["post_ids"]
        if isinstance(pids, list):
            count = len(pids)
            body_parts.append(f"\n[b]Contains {count} posts.[/b]")

    # Notes
    if "note" in endpoint and "x" in entry and "y" in entry:
        meta.append(f"Coordinates: X={entry['x']}, Y={entry['y']}")
        meta.append(f"Size: {entry.get('width', '?')}x{entry.get('height', '?')}")

    # Commentary
    if "commentary" in endpoint:
        if "translated_description" in entry and entry["translated_description"]:
            body_parts.append(f"[h4]Translation[/h4]\n{entry['translated_description']}")

    # Prepend Metadata
    if meta:
        body_parts.insert(0, "[b]Metadata:[/b]\n" + "\n".join(meta) + "\n\n---")

    return "\n\n".join(body_parts)

def _get_entry_title(entry, endpoint):
    """Determines the display title based on endpoint type."""
    if "name" in entry:
        return entry["name"]
    if "title" in entry:
        return entry["title"]

    # Fallbacks for nameless items
    eid = entry.get("id", "Unknown")
    if "note" in endpoint:
        return f"Note #{eid}"
    if "artist_commentary" in endpoint:
        return f"Commentary #{entry.get('post_id', eid)}"
    if "pool" in endpoint:
        return f"Pool #{eid}"
    return f"Item #{eid}"

def _process_wiki_entry(cursor, entry, endpoint, update_cache):
    """Processes a single API entry."""
    entry_id = entry.get("id")
    if not entry_id:
        return

    title = _get_entry_title(entry, endpoint)
    body = _format_entry_body(entry, endpoint)

    # Sanitization
    body = body.replace('\r\n', '\n').strip()
    title = title.strip()

    # Unique Key: e.g. "pools.json_123"
    unique_key = f"{endpoint}_{entry_id}"

    cursor.execute("SELECT body FROM wiki_cache WHERE unique_key = ?", (unique_key,))
    row = cursor.fetchone()

    if row is None:
        cursor.execute("""
            INSERT INTO wiki_cache (
                unique_key, remote_id, title, body, updated_at, source, imported
            ) VALUES (?, ?, ?, ?, ?, ?, 0)
        """, (
            unique_key, entry_id, title, body,
            entry.get("updated_at", ""), endpoint
        ))
    else:
        if update_cache and body.strip() != row[0].strip():
            cursor.execute("""
                UPDATE wiki_cache
                SET body = ?, title = ?, updated_at = ?
                WHERE unique_key = ?
            """, (body, title, entry.get("updated_at", ""), unique_key))

def _get_page_data(session, page, endpoint, args, solver):
    """Helper to fetch a single page of data."""
    target_url = urljoin(BASE_BOORU_URL, endpoint)
    try:
        # Standard User-Agent to avoid blocking
        if "User-Agent" not in session.headers:
            session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/115.0.0.0 Safari/537.36"
            })

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

        # Robust JSON Parsing with Fallback for Leading Whitespace
        try:
            return resp.json()
        except ValueError:
            # Server might return \n[...] or other whitespace noise
            if resp.text and resp.text.strip():
                try:
                    return json.loads(resp.text.strip())
                except ValueError:
                    return None
            return None

    except Exception as e:
        print(f"[ERROR] Fetch failed: {e}")
        return None

def _fetch_and_cache(args, endpoint):
    """Main fetch loop for a single endpoint."""
    conn, cur = _init_cache()
    session = requests.Session()
    solver = None
    if args.captcha and get_protected_session:
        session = get_protected_session()
        solver = AntiBotSolver()

    for page in range(args.start_page, args.start_page + args.pages):
        print(f"📦 Fetching {endpoint} page {page}...")
        data = _get_page_data(session, page, endpoint, args, solver)

        # Break loop if data is empty (End of Results)
        if not data:
            print(f"[INFO] Page {page} is empty. Moving to next endpoint.")
            break

        for entry in data:
            _process_wiki_entry(cur, entry, endpoint, args.update_cache)
    conn.commit()
    return conn

def import_danbooru(args):
    """Main CLI Entry Point."""
    print("=== Wiki Import Summary (Multi-Source) ===")
    print(f"🔗  Endpoints:      {args.endpoint}")
    print(f"💽  Database:       {SQLITE_DB}")
    print(f"📄  Pages:          {args.start_page} to {args.start_page + args.pages}")

    if args.clear_cache and SQLITE_DB.exists():
        SQLITE_DB.unlink()
        print("🧹 Cleared wiki cache.")

    # Split comma-separated endpoints
    endpoints = [e.strip() for e in args.endpoint.split(',')]

    for ep in endpoints:
        print(f"\n--- Processing Endpoint: {ep} ---")
        cache_conn = _fetch_and_cache(args, ep)
        cache_conn.close()

    print(f"\n[✓] Data cached to {SQLITE_DB}")
    if args.spath:
        print("[INFO] Shimmie sync skipped for non-standard endpoints (safe mode).")
        print("       Use 'wiki-index' to view this data.")
