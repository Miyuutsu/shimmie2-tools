import requests
import sqlite3
import psycopg2
import argparse
import re
from datetime import datetime
from pathlib import Path

DANBOORU_URL = "https://danbooru.donmai.us/wiki_pages.json"
WIKI_LINK_BASE = "/wiki/"
SQLITE_DB = Path("danbooru_wiki_cache.db")
SQLITE_DB.parent.mkdir(parents=True, exist_ok=True)
DB_CONFIG = {}
OWNER_ID = 1
OWNER_IP = "127.0.0.1"

def init_cache():
    conn = sqlite3.connect(SQLITE_DB)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wiki_cache (
            id INTEGER PRIMARY KEY,
            title TEXT UNIQUE,
            body TEXT,
            updated_at TEXT,
            imported BOOLEAN DEFAULT 0
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_title ON wiki_cache(title)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_imported ON wiki_cache(imported)")
    conn.commit()
    return conn, cur

def clear_cache():
    if SQLITE_DB.exists():
        SQLITE_DB.unlink()
        print("🧹 Cleared wiki cache.")
    else:
        print("ℹ️ No cache found to clear.")

import re

import re

def clean_wiki_body(text: str, title: str) -> str:
    """
    Sanitize and convert Danbooru wiki body content to Shimmie2-friendly BBCode-like format.
    Preserves <!--shimmie:lock--> if present.
    """

    lines = []
    previous_line_blank = False
    in_toc = False
    toc_lines = []

    for line in text.splitlines():
        original_line = line  # Keep the original line for reference
        line = line.rstrip()

        # Preserve hidden lock marker
        if line.strip() == "<!--shimmie:lock-->":
            lines.append(line)
            continue

        # Handle Table of Contents block
        if line.strip().lower() == "[expand=table of contents]":
            in_toc = True
            toc_lines = []
            continue
        if in_toc:
            if line.strip().lower() == "[/expand]":
                in_toc = False
                # Process ToC lines
                toc_processed = ["[h3][b]Table of Contents[/b][/h3]"]
                for toc_line in toc_lines:
                    match = re.match(r'\*\s*([\dA-Za-z]+(?:\.[\dA-Za-z]+)*)\.\s*"(.+?)":#([^\s]+)', toc_line.strip())
                    if match:
                        number = match.group(1)
                        label = match.group(2)
                        anchor = match.group(3)
                        # Determine the indentation level based on the number format
                        indentation = ''
                        separator = '.'
                        if '.' in number:
                            # If the number contains a decimal, add a space for indentation
                            indentation = ' ' * (number.count('.') - 1)  # Adjust based on the number of decimal points
                            separator = ''
                        # Filter out '-dtext' from the anchor
                        if '-dtext' in anchor:
                            anchor = anchor.replace('-dtext', '')
                        formatted_entry = f"{indentation}• {number}{separator} [url=site://wiki/{title}#bb-{anchor}]{label}[/url]"
                        #toc_processed.append(f"•{number}. [url=site://wiki/{title}#bb-{anchor}]{label}[/url]")
                        toc_processed.append(formatted_entry)
                lines.extend(toc_processed)
                continue
            else:
                toc_lines.append(line)
                continue

        # Skip meta/monetization lines
        if any(substr in line.lower() for substr in (
            "/user_upgrades/new",
            "gold+ account",
            "premium users",
            "see also: forum",
            "available to supporters",
        )):
            continue

        # Convert <a href="...">label</a> to [url=...]label[/url]
        def replace_html_links(match):
            href = match.group(1).strip()
            label = match.group(2).strip()
            return f"[url={href}]{label}[/url]"

        line = re.sub(r'<a href="([^"]+)">(.+?)</a>', replace_html_links, line)

        # Convert "text":/wiki/target to [[target|text]]
        line = re.sub(r'"([^"]+?)":/wiki/([a-zA-Z0-9_:]+)', r'[[\2|\1]]', line)

        # Strip forum_topics links
        line = re.sub(r'\[/forum_topics/\d+\]', '', line)

        # Remove empty tags like <ul></ul> or <p></p>
        line = re.sub(r'<(ul|ol|p)>\s*</\1>', '', line)

        # Convert headers with anchors (e.g., h2#anchor. Title)
        header_anchor_match = re.match(r'^h([1-6])#([a-zA-Z0-9_-]+)\.\s*(.+)', line)
        if header_anchor_match:
            level = header_anchor_match.group(1)
            anchor = header_anchor_match.group(2)
            label = header_anchor_match.group(3)
            # Should h1 be removed?
            # Downgrade h2 to h1
            if level in ['2']:
                level = 1
            # Downgrade h3 to h2
            if level in ['3']:
                level = 2
            # Downgrade h4 to h3
            if level in ['4']:
                level = 3
            # Downgrade h5 and h6 to h4
            if level in ['5', '6']:
                level = '4'
            line = f"[anchor={anchor}][/anchor][h{level}]{label}[/h{level}]"
        else:
            # Convert headers without anchors (e.g., h2. Title)
            header_match = re.match(r'^h([1-6])\.\s*(.+)', line)
            if header_match:
                level = header_match.group(1)
                label = header_match.group(2)
                # Downgrade h2 to h1
                if level in ['2']:
                    level = 1
                # Downgrade h3 to h2
                if level in ['3']:
                    level = 2
                # Downgrade h4 to h3
                if level in ['4']:
                    level = 3
                # Downgrade h5 and h6 to h4
                if level in ['5', '6']:
                    level = '4'
                line = f"[h{level}]{label}[/h{level}]"

        # Replace !post #123 with >>0
        line = re.sub(r'!post\s+#\d+', '>>0', line)

        # Convert unordered list items from * to • with appropriate nesting
        list_match = re.match(r'^(\*+)\s*(.*)', line)
        if list_match:
            asterisks = list_match.group(1)
            content = list_match.group(2)
            #bullet = '•' * len(asterisks)
            #line = f"{bullet} {content}"
            # Create the new line with spaces and a bullet
            if len(asterisks) > 1:
                # Replace all but the last asterisk with two spaces
                spaces = '  ' * (len(asterisks) - 1)
                bullet = '•'
                line = f"{spaces}{bullet} {content}"
            else:
                # If there's only one asterisk, replace it with a bullet
                line = f"• {content}"

        lines.append(line)

    cleaned = "\n".join(lines)

    # Replace spaces with underscores in [[...]] tags
    def replace_spaces_in_wiki_links(match):
        content = match.group(1)
        if '|' in content:
            target, label = content.split('|', 1)
            target = target.replace(' ', '_')
            return f"[[{target}|{label}]]"
        else:
            return f"[[{content.replace(' ', '_')}]]"

    cleaned = re.sub(r'\[\[([^\]]+)\]\]', replace_spaces_in_wiki_links, cleaned)

    # Collapse 3+ blank lines into max 2
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)

    return cleaned.strip()


#def convert_links(text):
#    text = re.sub(r"\[\[([^\]|]+?)\|(.+?)\]\]", rf'<a href="{WIKI_LINK_BASE}\1">\2</a>', text)
#    text = re.sub(r"\[\[([^\]|]+?)\]\]", rf'<a href="{WIKI_LINK_BASE}\1">\1</a>', text)
#    return text

def markdown_to_html(text):
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"__(.+?)__", r"<strong>\1</strong>", text)
    text = re.sub(r"\*(.+?)\*", r"<em>\1</em>", text)
    text = re.sub(r"_(.+?)_", r"<em>\1</em>", text)
    text = re.sub(r"(?m)^[-*]\s+(.*)", r"<li>\1</li>", text)
    text = re.sub(r"(?s)(<li>.*?</li>)", r"<ul>\1</ul>", text)
    return text

def insert_or_update_pg(pg_cur, title, body, existing_titles, update_existing=False):
    if title not in existing_titles:
        pg_cur.execute("""
            INSERT INTO wiki_pages (owner_id, owner_ip, date, title, revision, locked, body)
            VALUES (%s, %s, %s, %s, 1, false, %s)
        """, (OWNER_ID, OWNER_IP, datetime.now(), title, body))
        return "inserted"

    elif update_existing:
        pg_cur.execute("SELECT revision, body FROM wiki_pages WHERE title = %s ORDER BY revision DESC LIMIT 1", (title,))
        current = pg_cur.fetchone()
        if current:
            current_rev, current_body = current
            if '[[shimmie:lock]]' in current_body:
                print(f"🔒 Skipped locked entry: {title}")
                return "skipped"
            if body.strip() != current_body.strip():
                next_rev = current_rev + 1
                pg_cur.execute("""
                    INSERT INTO wiki_pages (owner_id, owner_ip, date, title, revision, locked, body)
                    VALUES (%s, %s, %s, %s, %s, false, %s)
                """, (OWNER_ID, OWNER_IP, datetime.now(), title, next_rev, body))
                return "updated"

    return "skipped"

def fetch_and_cache(start_page, page_count, update_cache=False, convert_mode="shimmie"):
    conn, cur = init_cache()

    if not update_cache:
        # Reset all imported flags first to avoid accumulating stale entries
        cur.execute("UPDATE wiki_cache SET imported = 1")

    for page in range(start_page, start_page + page_count):
        print(f"📦 Fetching page {page}")
        resp = requests.get(DANBOORU_URL, params={"page": page, "limit": 1000})
        resp.raise_for_status()
        for entry in resp.json():
            title = entry.get("title", "").strip()
            body = entry.get("body", "").strip()
            body = body.replace('\r\n', '\n').strip()
            entry_id = entry.get("id")
            if not title or not body or not entry_id:
                continue
            if convert_mode == "markdown":
                body = markdown_to_html(body, title)
            elif convert_mode == "html":
                pass  # use as-is
            elif convert_mode == "shimmie":
                body = clean_wiki_body(body, title)
            elif convert_mode == "raw":
                body = body.strip()

            cur.execute("SELECT body FROM wiki_cache WHERE title = ?", (title,))
            row = cur.fetchone()
            if row is None:
                cur.execute("""
                    INSERT OR IGNORE INTO wiki_cache (id, title, body, updated_at, imported)
                    VALUES (?, ?, ?, ?, 0)
                """, (entry_id, title, body, entry["updated_at"]))
            else:
                # Mark as unimported, update only if body changed
                if update_cache and body.strip() != row[0].strip():
                    cur.execute("""
                        UPDATE wiki_cache SET body = ?, updated_at = ? WHERE title = ?
                    """, (body, entry["updated_at"], title))
                else:
                    # Force reset imported flag for matching title from current page
                    cur.execute("UPDATE wiki_cache SET imported = 0 WHERE title = ?", (title,))

    conn.commit()
    return conn

def get_existing_titles(pg_cur):
    pg_cur.execute("SELECT title FROM wiki_pages")
    return {row[0] for row in pg_cur.fetchall()}

def main(args):
    if args.clear_cache:
        clear_cache()

    print("⏳ Loading cache...")
    cache_conn = fetch_and_cache(args.start_page, args.pages, update_cache=args.update_cache, convert_mode=args.convert)
    cache_cur = cache_conn.cursor()

    pg_conn = psycopg2.connect(**DB_CONFIG)
    pg_cur = pg_conn.cursor()
    existing_titles = get_existing_titles(pg_cur)

    # Stream pages row-by-row instead of fetchall
    cache_cur.execute("SELECT title, body FROM wiki_cache WHERE imported = 0")
    rows = cache_cur.fetchall()
    print(f"📦 {len(rows)} wiki pages queued for import.")

    results = {"inserted": 0, "updated": 0, "skipped": 0}
    for title, body in rows:
        result = insert_or_update_pg(pg_cur, title, body, existing_titles, update_existing=args.update_existing)
        results[result] += 1
        if result != "skipped":
            cache_cur.execute("UPDATE wiki_cache SET imported = 1 WHERE title = ?", (title,))

    pg_conn.commit()
    cache_conn.commit()

    print(f"\n✅ Inserted: {results['inserted']}")
    print(f"🔁 Updated: {results['updated']}")
    print(f"⏭️ Skipped: {results['skipped']}")

    pg_conn.close()
    cache_conn.close()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Import Danbooru wiki pages into Shimmie2.")
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument("--pages", type=int, default=200)
    parser.add_argument("--update-existing", action="store_true")
    parser.add_argument("--convert", choices=["raw", "markdown", "html", "shimmie"], default="shimmie", help="Content formatting mode")
    parser.add_argument("--update-cache", action="store_true")
    parser.add_argument("--clear-cache", action="store_true")
    parser.add_argument("--user", type=str, default="miyuu", help="PostgreSQL user")
    parser.add_argument("--db", type=str, default="shimmiedb", help="PostgreSQL database name")
    args = parser.parse_args()

    # Set DB_CONFIG after parsing
    DB_CONFIG = {
        "dbname": args.db,
        "user": args.user,
        "host": "localhost",
        "port": 5432
    }

    print("=== Import Summary ===")
    print(f"📚  Database:       {DB_CONFIG['dbname']}")
    print(f"👤  User:           {DB_CONFIG['user']}")
    print(f"📄  Start Page:     {args.start_page}")
    print(f"📄  Page Count:     {args.pages}")
    print(f"🔄  Update Cache:   {'Yes' if args.update_cache else 'No'}")
    print(f"📝  Update Existing:{'Yes' if args.update_existing else 'No'}")
    print(f"🧼  Clear Cache:    {'Yes' if args.clear_cache else 'No'}")
    print(f"🎨  Convert Mode:   {args.convert}")
    print()

    main(args)
