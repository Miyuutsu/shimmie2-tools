# pylint: disable=duplicate-code
"""Wiki management tools (Indexing and Danbooru Imports)."""
import re
import sqlite3
from collections import namedtuple
from datetime import datetime
from pathlib import Path

import psycopg2
import requests

from functions.db_cache import get_shimmie_db_credentials

# Optional import to allow modularity without crashing if file is missing
try:
    from functions.captcha import get_protected_session, AntiBotSolver
except ImportError:
    get_protected_session = None
    AntiBotSolver = None

DANBOORU_URL = "https://danbooru.donmai.us/wiki_pages.json"
SQLITE_DB = Path("database/danbooru_wiki_cache.db")
POSTS_DB = Path("database/posts_cache.db")

CategoryConfig = namedtuple('CategoryConfig', ['prefix', 'title', 'col'])

# ==========================================
# Shared DB Helper
# ==========================================
def _unique_names(names):
    """Return unique names while maintaining the order."""
    seen = set()
    return [name for name in names if not (name in seen or seen.add(name))]

# ==========================================
# Tool 1: Create Wiki Index
# ==========================================
def _fetch_tag_dict_pg(cursor, prefix):
    """Helper to fetch dict of tags from Postgres."""
    if prefix:
        cursor.execute(f"SELECT tag FROM tags WHERE tag ILIKE '{prefix}%' ORDER BY tag ASC;")
        return {t[0][len(prefix):]: t[0] for t in cursor.fetchall()}

    cursor.execute(
        "SELECT tag FROM tags WHERE tag NOT ILIKE 'artist:%' "
        "AND tag NOT ILIKE 'character:%' AND tag NOT ILIKE 'series:%' ORDER BY tag ASC;"
    )
    return {t[0]: t[0] for t in cursor.fetchall()}

def _fetch_tag_dict_sqlite(cursor, column, prefix):
    """Helper to fetch unique tags from posts_cache.db column."""
    tags = set()
    try:
        cursor.execute(f"SELECT {column} FROM posts")
        for row in cursor:
            if row[0]:
                for tag in row[0].split(','):
                    t = tag.strip()
                    if t:
                        tags.add(t)
    except sqlite3.Error:
        return {}

    return {t: f"{prefix}{t}" if prefix else t for t in tags}

def _sort_category(textfile, cursor, cat_config, source_type="pg"):
    """A unified function to sort and replace tags for any category."""
    prefix, title_header, col = cat_config

    if source_type == "sqlite":
        tag_dict = _fetch_tag_dict_sqlite(cursor, col, prefix)
    else:
        tag_dict = _fetch_tag_dict_pg(cursor, prefix)

    with textfile.open("r", encoding="utf-8") as file:
        content = file.read()

    def replace_tag(match):
        name = match.group(1).strip()
        return f"[[{tag_dict[name]}]]" if name in tag_dict else match.group(0)

    pattern = re.compile(r'\[\[([^\(\)]+?)\]\]')

    if prefix:
        lines = pattern.sub(replace_tag, content).splitlines()
        unique_lines = _unique_names([line for line in lines if line.startswith(f'[[{prefix}')])
        return f"== {title_header} ==\n\n" + "\n".join(unique_lines)

    combined_tags = set(list(tag_dict.keys()) + [t.strip() for t in pattern.findall(content)])
    return f"== {title_header} ==\n" + "\n".join(sorted([f'[[{tag}]]' for tag in combined_tags]))

def _generate_sorted_output(out_path, conn, source_type):
    """Generates the sorted output using the appropriate DB connection."""
    cursor = conn.cursor()

    # Map (prefix, title, sqlite_column)
    categories = {
        "c": CategoryConfig("character:", "Characters", "character"),
        "s": CategoryConfig("series:", "Series", "series"),
        "a": CategoryConfig("artist:", "Artists", "artist"),
        "g": CategoryConfig("", "General", "general")
    }

    sections = {}
    for key, config in categories.items():
        print(f"   - Processing {config.title}...")
        sections[key] = _sort_category(out_path, cursor, config, source_type)

    return sections

def _get_titles_pg(spath):
    """Fetch wiki titles from Postgres."""
    db_config = get_shimmie_db_credentials(spath)
    if not db_config:
        print(f"[ERROR] Could not load DB credentials from {spath}")
        return None
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT title FROM wiki_pages ORDER BY title ASC")
                # HTML Version
                return [
                    f'<a href="/wiki/{p[0].replace(" ", "_")}">{p[0]}</a><br>'
                    for p in cursor.fetchall()
                ]
    except psycopg2.Error as err:
        print(f"[ERROR] Postgres error: {err}")
        return None

def _get_titles_sqlite():
    """Fetch wiki titles from local SQLite cache."""
    if not SQLITE_DB.exists():
        print(f"[ERROR] No Shimmie path provided and {SQLITE_DB} not found.")
        print("Run 'import-wikis' first to populate the cache.")
        return None
    print(f"[INFO] Running in Offline Mode using {SQLITE_DB}")
    try:
        with sqlite3.connect(SQLITE_DB) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT title FROM wiki_cache ORDER BY title ASC")
            # HTML Version
            return [
                f'<a href="/wiki/{p[0].replace(" ", "_")}">{p[0]}</a><br>'
                for p in cursor.fetchall()
            ]
    except sqlite3.Error as err:
        print(f"[ERROR] SQLite error: {err}")
        return None

def _handle_sorting(args, out_path):
    """Handles the optional sorting logic."""
    print(f"Sorting tags from {args.output}...")
    sections = {}

    if args.spath:
        db_config = get_shimmie_db_credentials(args.spath)
        if not db_config:
            return
        try:
            with psycopg2.connect(**db_config) as conn:
                sections = _generate_sorted_output(out_path, conn, "pg")
        except psycopg2.Error as err:
            print(f"[ERROR] Postgres sorting error: {err}")
            return
    else:
        if not POSTS_DB.exists():
            print(f"[WARNING] Skipping sort: {POSTS_DB} not found.")
            return

        print(f"[INFO] Sorting using tags from {POSTS_DB}...")
        try:
            with sqlite3.connect(POSTS_DB) as conn:
                sections = _generate_sorted_output(out_path, conn, "sqlite")
        except sqlite3.Error as err:
            print(f"[ERROR] SQLite sorting error: {err}")
            return

    if sections:
        order = args.order.split(",") if args.order else []
        final_output = "\n\n".join(sections[s] for s in order if s in sections)
        sorted_path = out_path.with_name(out_path.stem + "_sorted.txt")
        with sorted_path.open("w", encoding="utf-8") as file:
            file.write(final_output)
        print(f"[✓] Sorted tags written to {sorted_path}.")

def create_index(args):
    """Main execution for creating and sorting the wiki index."""
    out_path = Path(args.output)

    # 1. Fetch Titles
    if args.spath:
        wiki_links = _get_titles_pg(args.spath)
    else:
        wiki_links = _get_titles_sqlite()

    if wiki_links is None:
        return

    # 2. Write Index File
    with out_path.open('w', encoding="utf-8") as f:
        f.write("\n".join(wiki_links))
    print(f"[✓] Wiki index created at {args.output}.")

    # 3. Sort Tags (Optional)
    if args.sort:
        _handle_sorting(args, out_path)


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
    try:
        resp = session.get(DANBOORU_URL, params={"page": page, "limit": 1000}, timeout=30)

        # === CAPTCHA INTERCEPT ===
        if args.captcha and solver:
            # Check start of response for captcha markers
            if solver.detect(resp.text[:2000]):
                if solver.solve(session, resp.text, resp.url):
                    print("[INFO] Retrying original request...")
                    resp = session.get(
                        DANBOORU_URL, params={"page": page, "limit": 1000}, timeout=30
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
    title, body, entry_id = entry.get("title", ""), entry.get("body", ""), entry.get("id")
    if not title or not body or not entry_id:
        return

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
        """, (entry_id, title, body, entry["updated_at"]))
    else:
        if args.update_cache and body.strip() != row[0].strip():
            cursor.execute(
                "UPDATE wiki_cache SET body = ?, updated_at = ? WHERE title = ?",
                (body, entry["updated_at"], title)
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
