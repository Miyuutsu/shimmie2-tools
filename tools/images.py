"""
Image Downloader with Threaded API, Subfolders, and Graceful Shutdown.
"""
import time
import sqlite3
import re
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import NamedTuple, Optional, Tuple, List, Union
from urllib.parse import urlparse, parse_qs, unquote

import requests

from functions.captcha import get_protected_session, AntiBotSolver

# Global event for safe shutdown
SHUTDOWN_EVENT = threading.Event()
PAGINATION_LIMIT = 1000

# Schema for the local resume DB
CREATE_LOCAL_DB = """
CREATE TABLE IF NOT EXISTS downloads (
    id INTEGER PRIMARY KEY,
    post_id TEXT,
    filepath TEXT,
    search_query TEXT,
    md5 TEXT,
    status TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(post_id, filepath)
);
CREATE INDEX IF NOT EXISTS idx_post_id ON downloads(post_id);
"""

# Schema for Gallery-DL compatibility
CREATE_GDL_DB = """
CREATE TABLE IF NOT EXISTS archive (
    entry TEXT UNIQUE
);
"""

class DbContext(NamedTuple):
    """Context container for database connections."""
    local: sqlite3.Connection
    gdl: Optional[sqlite3.Connection]
    sitename: str
    global_dedup: bool

class DownloadTask(NamedTuple):
    """Container for download worker arguments."""
    post: dict
    args: object
    output_path: Path
    gdl_db_path: Optional[str]
    sitename: str
    base_url: str
    search_query: str

class FetchContext(NamedTuple):
    """Container for API fetch arguments to reduce complexity."""
    session: requests.Session
    args: object
    solver: Optional[AntiBotSolver]
    tags: str
    base_url: str
    end_page: Optional[int]
    end_id: Optional[int]

def _init_dbs(root_output_path, gdl_db_path):
    """Initialize the global tracking DB in the root output folder."""
    root_output_path.mkdir(parents=True, exist_ok=True)
    local_db_path = root_output_path / "global_downloads.db"

    local_conn = sqlite3.connect(local_db_path, check_same_thread=False)
    local_conn.executescript(CREATE_LOCAL_DB)

    gdl_conn = None
    if gdl_db_path:
        gdl_path = Path(gdl_db_path)
        gdl_path.parent.mkdir(parents=True, exist_ok=True)
        gdl_conn = sqlite3.connect(gdl_path, check_same_thread=False)
        gdl_conn.executescript(CREATE_GDL_DB)

    return local_conn, gdl_conn, local_db_path

def _get_site_details(session, url):
    """Fetches the site title using regex."""
    parsed = urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    clean_title = "danbooru"

    try:
        resp = session.get(base_url, timeout=10)
        resp.raise_for_status()

        match = re.search(
            r'<meta\s+property=["\']og:site_name["\']\s+content=["\']([^"\']+)["\']',
            resp.text,
            re.IGNORECASE
        )
        if match:
            raw_title = match.group(1).strip()
        else:
            title_match = re.search(r'<title>(.*?)</title>', resp.text, re.IGNORECASE)
            raw_title = (
                title_match.group(1).split(':')[0].split('-')[0].strip()
                if title_match else "danbooru"
            )

        clean_title = "".join(x for x in raw_title if x.isalnum() or x in "_-")

    except Exception as e: # pylint: disable=broad-exception-caught
        print(f"[Warning] Could not fetch site title: {e}")

    return clean_title, base_url

def _parse_input_query(query, default_base):
    """Parses the input query (URL or tags)."""
    if "://" not in query and "/posts" not in query:
        return query, None, None, default_base

    parsed = urlparse(query)
    params = parse_qs(parsed.query)
    detected_base = f"{parsed.scheme}://{parsed.netloc}"

    tags = unquote(params['tags'][0]) if 'tags' in params else ""
    page_param = params.get('page', [None])[0]

    start_page = 1
    start_id = None

    if page_param:
        if page_param.startswith('b') or page_param.startswith('a'):
            start_id = page_param
            start_page = None
        else:
            try:
                start_page = int(page_param)
            except ValueError:
                pass

    return tags, start_page, start_id, detected_base

def _parse_end_condition(value):
    """
    Parses the end condition argument.
    - "10" -> Page 10 (int)
    - "a12345" -> Stop at ID 12345 (str)
    """
    if not value:
        return None, None

    value = str(value).strip()
    if value.isdigit():
        return int(value), None # It's a page number

    if value.lower().startswith('a') and value[1:].isdigit():
        return None, int(value[1:]) # It's an ID limit

    return None, None

def _check_exists(post_id, target_path, db_ctx: DbContext):
    """Checks if a post should be skipped."""
    cur = db_ctx.local.cursor()

    # 1. Local DB Check
    if db_ctx.global_dedup:
        cur.execute(
            "SELECT filepath FROM downloads WHERE post_id = ? AND status = 'completed'",
            (str(post_id),)
        )
        row = cur.fetchone()
        if row:
            return True, f"[Skip] Global Dedup: Found in {row[0]}"
    else:
        cur.execute(
            "SELECT 1 FROM downloads WHERE post_id = ? AND filepath = ? AND status = 'completed'",
            (str(post_id), str(target_path))
        )
        if cur.fetchone():
            return True, "[Skip] Already downloaded in this search."

    # 2. Gallery-DL DB Check
    if db_ctx.gdl:
        gdl_cur = db_ctx.gdl.cursor()
        entry_key = f"{db_ctx.sitename} {post_id}"
        gdl_cur.execute("SELECT 1 FROM archive WHERE entry = ?", (entry_key,))
        if gdl_cur.fetchone():
            return True, "[Skip] Found in Gallery-DL archive."

    return False, None

def _record_success(task, md5, filepath, db_ctx: DbContext):
    """Marks download as complete in both DBs."""
    with db_ctx.local:
        db_ctx.local.execute("""
            INSERT OR REPLACE INTO downloads (post_id, filepath, search_query, md5, status)
            VALUES (?, ?, ?, ?, 'completed')
        """, (str(task.post['id']), str(filepath), task.search_query, md5))

    if db_ctx.gdl:
        with db_ctx.gdl:
            entry_key = f"{db_ctx.sitename} {task.post['id']}"
            db_ctx.gdl.execute("INSERT OR IGNORE INTO archive (entry) VALUES (?)", (entry_key,))

def _construct_tag_string(post):
    """Parses category fields and constructs a newline-separated tag string."""
    categories = {
        "artist": post.get("tag_string_artist", ""),
        "series": post.get("tag_string_copyright", ""),
        "character": post.get("tag_string_character", ""),
        "meta": post.get("tag_string_meta", ""),
        "general": post.get("tag_string_general", "")
    }

    if not any(categories.values()) and "tag_string" in post:
        return post["tag_string"].replace(" ", "\n")

    final_tags = []
    for cat, string in categories.items():
        if not string:
            continue
        for tag in string.split():
            prefix = "" if cat == "general" else f"{cat}:"
            final_tags.append(f"{prefix}{tag}")

    return "\n".join(final_tags)

def _download_file(task, db_ctx):
    """Handles the actual file I/O."""
    post = task.post
    file_url = post.get('file_url') or post.get('large_file_url')

    if not file_url:
        return f"[Skip] ID {post['id']} has no file_url."

    if file_url.startswith("/"):
        file_url = f"{task.base_url}{file_url}"

    ext = post.get('file_ext') or Path(file_url).suffix.strip('.')
    filename = task.args.filename_fmt.format(
        id=post['id'], md5=post.get('md5', ''), sitename=task.sitename, ext=ext
    )
    filename = "".join(x for x in filename if x.isalnum() or x in "._-")

    safe_folder = "".join(x for x in task.search_query[:50] if x.isalnum() or x in "_-") or "misc"
    target_dir = task.output_path / safe_folder
    target_dir.mkdir(parents=True, exist_ok=True)

    out_path = target_dir / filename

    exists, msg = _check_exists(post['id'], out_path, db_ctx)
    if exists:
        return msg

    if out_path.exists():
        _record_success(task, post.get('md5', ''), out_path, db_ctx)
        return f"[Found] ID {post['id']} exists on disk."

    try:
        resp = requests.get(file_url, stream=True, timeout=60)
        resp.raise_for_status()

        with open(out_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if SHUTDOWN_EVENT.is_set():
                    return "[Aborted] Shutdown triggered."
                f.write(chunk)

        return out_path
    except Exception as e: # pylint: disable=broad-exception-caught
        if out_path.exists():
            out_path.unlink()
        raise e

def _download_worker(task: DownloadTask, db_path):
    """Worker function."""
    if SHUTDOWN_EVENT.is_set():
        return "[Aborted] Shutdown pending."

    local_conn = sqlite3.connect(db_path)
    gdl_conn = sqlite3.connect(task.gdl_db_path) if task.gdl_db_path else None

    do_dedup = getattr(task.args, 'global_dedup', False)
    db_ctx = DbContext(local_conn, gdl_conn, task.sitename, do_dedup)

    try:
        res = _download_file(task, db_ctx)

        if isinstance(res, Path):
            if task.args.sidecar:
                tag_str = _construct_tag_string(task.post)
                with res.with_name(f"{res.name}.txt").open('w', encoding='utf-8') as f:
                    f.write(tag_str)

            _record_success(task, task.post.get('md5', ''), res, db_ctx)
            return f"[Downloaded] {res.name}"

        return res

    except Exception as e: # pylint: disable=broad-exception-caught
        return f"[Error] ID {task.post.get('id')}: {e}"
    finally:
        local_conn.close()
        if gdl_conn:
            gdl_conn.close()

def _fetch_metadata_page(session, url, params, args, solver) -> Union[dict, str, None]:
    """Fetches a single page of metadata."""
    if SHUTDOWN_EVENT.is_set():
        return None

    try:
        resp = session.get(url, params=params, timeout=30)
        if args.captcha and solver and solver.detect(resp.text[:2000]):
            if solver.solve(session, resp.text, resp.url):
                resp = session.get(url, params=params, timeout=30)
            else:
                return None
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 410:
            print(f"\n[!] API Limit Reached (410 Gone). Page {params.get('page')} is too deep.")
            print("    Switching to ID-based pagination...")
            return "410_GONE"
        print(f"\n[Error] HTTP Error: {e}")
        return None
    except (requests.RequestException, ValueError) as e:
        print(f"\n[Error] API fetch failed: {e}")
        return None

def _reached_id_limit(posts_batch, end_id) -> Tuple[bool, List]:
    """Filters posts that are beyond the end_id limit."""
    if not end_id:
        return False, posts_batch

    filtered = []
    hit_limit = False
    for p in posts_batch:
        # If current ID <= End ID, we have reached the "After" point.
        if p.get('id', 0) <= end_id:
            hit_limit = True
            break # Stop processing this batch
        filtered.append(p)
    return hit_limit, filtered

def _fetch_threaded_loop(ctx: FetchContext, start_page: int) -> Tuple[List[dict], Optional[str]]:
    """Handles the threaded page-based fetching loop."""
    all_posts = []
    batch_size = 5
    current_page = start_page
    base_api = f"{ctx.base_url}/posts.json"
    next_start_id = None

    print("\n--- Fetching API Metadata (Threaded Page Mode) ---")

    with ThreadPoolExecutor(max_workers=batch_size) as executor:
        while not SHUTDOWN_EVENT.is_set():
            if ctx.end_page and current_page > ctx.end_page:
                break

            futures = {}
            for i in range(batch_size):
                page_num = current_page + i
                if ctx.end_page and page_num > ctx.end_page:
                    break

                if page_num >= PAGINATION_LIMIT:
                    print(f"\n[Info] Page {page_num} reached. Switching to ID Mode.")
                    break

                print(f"Queueing page {page_num}...", end="\r")
                params = {"tags": ctx.tags, "page": page_num, "limit": ctx.args.limit}
                future = executor.submit(
                    _fetch_metadata_page, ctx.session, base_api, params, ctx.args, ctx.solver
                )
                futures[future] = page_num

            if not futures:
                break

            batch_has_data = False
            # Sort by page number to maintain order
            batch_results = sorted(
                [(futures[f], f.result()) for f in as_completed(futures)],
                key=lambda x: x[0]
            )

            last_batch_min_id = None

            for p_num, data in batch_results:
                if data == "410_GONE":
                    print(f"\n[Info] Page {p_num} hit limit. Switching modes...")
                    break

                if not data:
                    print(f"\n[Info] Page {p_num} is empty or failed. Stopping fetch.")
                    return all_posts, None

                hit_limit, filtered_data = _reached_id_limit(data, ctx.end_id)
                all_posts.extend(filtered_data)

                if hit_limit:
                    print(f"\n[✓] Reached End-ID limit ({ctx.end_id}). Stopping.")
                    return all_posts, None

                batch_has_data = True
                if filtered_data:
                    last_batch_min_id = filtered_data[-1].get('id')

                if len(data) < ctx.args.limit:
                    print(f"\n[Info] Page {p_num} has partial data. End of results.")
                    return all_posts, None

            if not batch_has_data:
                # If we got no data but need to switch to sequential
                if last_batch_min_id or (all_posts and all_posts[-1].get('id')):
                    last_id = last_batch_min_id if last_batch_min_id else all_posts[-1]['id']
                    next_start_id = f"b{last_id}"
                    break
                return all_posts, None

            current_page += batch_size
            time.sleep(ctx.args.sleep)

            if current_page >= PAGINATION_LIMIT and last_batch_min_id:
                next_start_id = f"b{last_batch_min_id}"
                break

    return all_posts, next_start_id

def _fetch_sequential_loop(ctx: FetchContext, start_id: str) -> List[dict]:
    """Handles the sequential ID-based fetching loop."""
    all_posts = []
    base_api = f"{ctx.base_url}/posts.json"
    current_id_param = start_id

    print("\n--- Fetching API Metadata (Sequential ID Mode) ---")

    while not SHUTDOWN_EVENT.is_set():
        clean_id = current_id_param.strip('b')
        print(f"Fetching posts before ID {clean_id}...", end="\r")
        params = {"tags": ctx.tags, "page": current_id_param, "limit": ctx.args.limit}

        data = _fetch_metadata_page(ctx.session, base_api, params, ctx.args, ctx.solver)

        if not data or data == "410_GONE":
            print("\n[Info] No more posts found.")
            break

        hit_limit, filtered_data = _reached_id_limit(data, ctx.end_id)
        all_posts.extend(filtered_data)

        if hit_limit:
            print(f"\n[✓] Reached End-ID limit ({ctx.end_id}). Stopping.")
            break

        if not filtered_data:
            break

        last_id = filtered_data[-1].get('id')
        if not last_id:
            break
        current_id_param = f"b{last_id}"

        if len(data) < ctx.args.limit:
            print("\n[Info] Partial page returned. End of results.")
            break

        time.sleep(ctx.args.sleep)

    return all_posts

def _fetch_all_posts_threaded(ctx: FetchContext, start_page: int, start_id: Optional[str]):
    """Orchestrates the hybrid fetching strategy."""
    all_posts = []

    use_sequential = (
        (start_page is not None and start_page >= PAGINATION_LIMIT) or
        (start_id is not None)
    )

    if not use_sequential:
        threaded_posts, next_start_id = _fetch_threaded_loop(ctx, start_page)
        all_posts.extend(threaded_posts)

        if next_start_id:
            use_sequential = True
            start_id = next_start_id
        else:
            use_sequential = False

    if use_sequential:
        # Determine start ID if not explicitly provided
        if not start_id:
            if all_posts and all_posts[-1].get('id'):
                start_id = f"b{all_posts[-1]['id']}"
            else:
                start_id = "b999999999"

        seq_posts = _fetch_sequential_loop(ctx, start_id)
        all_posts.extend(seq_posts)

    print(f"\n[✓] Metadata fetched. Found {len(all_posts)} posts.")
    return all_posts

def _configure_download(args):
    """Parses arguments and sets up configuration for the run."""
    session = get_protected_session()
    solver = AntiBotSolver() if args.captcha else None

    # Input Parsing
    raw_query = args.tags or args.query
    if not raw_query:
        print("[Error] You must provide a URL or tags.")
        return None

    # Parse initial inputs from URL or default args
    tags, start_page, start_id, base_url = _parse_input_query(raw_query, args.base_url)

    # CLI Override Logic:
    # args.start_page comes in as a string ("1", "b12345") or None/default "1"

    cli_start_arg = str(args.start_page).strip()

    if cli_start_arg.isdigit():
        # It's a standard page number
        start_page = int(cli_start_arg)
        # Only reset start_id if user specifically requested a page number
        if args.start_page != "1":
            start_id = None
    elif cli_start_arg.lower().startswith(('a', 'b')):
        # It's an ID-based start
        start_id = cli_start_arg
        start_page = None # Disable page-based logic

    end_page_limit, end_id_limit = _parse_end_condition(args.end_page)

    sitename = args.sitename
    if sitename == "auto":
        sitename, base_url = _get_site_details(session, base_url)

    root_output_path = Path(args.output)
    if args.output == "downloads" and sitename:
        root_output_path = Path(sitename)

    ctx = FetchContext(
        session=session, args=args, solver=solver, tags=tags,
        base_url=base_url, end_page=end_page_limit, end_id=end_id_limit
    )

    return ctx, start_page, start_id, sitename, root_output_path

def run(args):
    """Main entry point."""
    config = _configure_download(args)
    if not config:
        return

    ctx, start_page, start_id, sitename, root_output_path = config

    print(f"=== Image Downloader ({sitename}) ===")
    print(f"🌍 Base URL: {ctx.base_url}")
    print(f"📂 Output:   {root_output_path}")
    print(f"🏷️  Tags:     {ctx.tags}")

    if start_id:
        print(f"📄 Start:    ID {start_id}")
    else:
        print(f"📄 Start:    Page {start_page}")

    if ctx.end_id:
        print(f"🛑 End:      After ID {ctx.end_id}")
    elif ctx.end_page:
        print(f"🛑 End:      Page {ctx.end_page}")
    else:
        print("🛑 End:      None")

    try:
        all_posts = _fetch_all_posts_threaded(ctx, start_page, start_id)
    except KeyboardInterrupt:
        print("\n[!] Fetch cancelled.")
        return

    if not all_posts:
        return

    l_conn, g_conn, db_path = _init_dbs(root_output_path, args.gdl_db)
    l_conn.close()
    if g_conn:
        g_conn.close()

    print("\n--- Starting Downloads (Ctrl+C to stop safely) ---")

    tasks = []
    for p in all_posts:
        tasks.append(DownloadTask(
            post=p, args=args, output_path=root_output_path,
            gdl_db_path=args.gdl_db, sitename=sitename, base_url=ctx.base_url,
            search_query=ctx.tags
        ))

    executor = ThreadPoolExecutor(max_workers=args.threads)
    futures = {executor.submit(_download_worker, t, str(db_path)): t for t in tasks}
    completed = 0

    try:
        for future in as_completed(futures):
            print(f"[{completed + 1}/{len(tasks)}] {future.result()}")
            completed += 1
    except KeyboardInterrupt:
        print("\n\n[!] SHUTDOWN TRIGGERED. Waiting for active downloads to finish...")
        SHUTDOWN_EVENT.set()
        executor.shutdown(wait=True)
        print("[✓] Safe shutdown complete. Database is consistent.")
