"""
Image Downloader with Threaded API, Subfolders, Checkpoints, WAL Support, and Error Logging.
"""
import time
import sqlite3
import re
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import NamedTuple, Optional, Tuple, List, Union, Dict
from urllib.parse import urlparse, parse_qs, unquote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from functions.captcha import get_protected_session, AntiBotSolver

# Global event for safe shutdown and logging lock
SHUTDOWN_EVENT = threading.Event()
LOG_LOCK = threading.Lock()
PAGINATION_LIMIT = 1000

# Schema for the local resume DB
CREATE_LOCAL_DB = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

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

-- New Table: Checkpoints for Deep Jumping
CREATE TABLE IF NOT EXISTS checkpoints (
    search_query TEXT,
    page_num INTEGER,
    post_id TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (search_query, page_num)
);
"""

# Schema for Gallery-DL compatibility
CREATE_GDL_DB = """
PRAGMA journal_mode=WAL;
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
    source_context: str
    cookies: Dict
    headers: Dict

class FetchContext(NamedTuple):
    """Container for API fetch arguments to reduce complexity."""
    session: requests.Session
    args: object
    solver: Optional[AntiBotSolver]
    tags: str
    base_url: str
    end_page: Optional[int]
    end_id: Optional[int]
    db_path: Path

def _init_dbs(root_output_path, gdl_db_path):
    """Initialize the global tracking DB in the root output folder."""
    root_output_path.mkdir(parents=True, exist_ok=True)
    local_db_path = root_output_path / "global_downloads.db"

    local_conn = sqlite3.connect(local_db_path, check_same_thread=False, timeout=30)
    local_conn.executescript(CREATE_LOCAL_DB)

    gdl_conn = None
    if gdl_db_path:
        gdl_path = Path(gdl_db_path)
        gdl_path.parent.mkdir(parents=True, exist_ok=True)
        gdl_conn = sqlite3.connect(gdl_path, check_same_thread=False, timeout=30)
        gdl_conn.executescript(CREATE_GDL_DB)

    return local_conn, gdl_conn, local_db_path

def _log_error(root_path, context, post_id, message):
    """Writes an error to the error.log file in a thread-safe manner."""
    log_file = root_path / "error.log"
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{timestamp}] [Ctx: {context}] [ID: {post_id}] {message}\n"

    with LOG_LOCK:
        try:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(entry)
        except Exception as e: # pylint: disable=broad-exception-caught
            print(f"[!] Failed to write to error log: {e}")

def _save_checkpoint(db_path, tags, page_num, post_id):
    """Records a mapping of Page -> ID to allow deep jumping later."""
    try:
        with sqlite3.connect(db_path, timeout=30) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO checkpoints (search_query, page_num, post_id) VALUES (?, ?, ?)",
                (tags, page_num, str(post_id))
            )
    except Exception as e: # pylint: disable=broad-exception-caught
        print(f"[Warning] Failed to save checkpoint: {e}")

def _get_checkpoint_id(db_path, tags, page_num):
    """Attempts to find a post ID for a given page number from history."""
    try:
        with sqlite3.connect(db_path, timeout=30) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT post_id, page_num FROM checkpoints WHERE search_query = ? AND page_num <= ? "
                "ORDER BY page_num DESC LIMIT 1",
                (tags, page_num)
            )
            row = cur.fetchone()
            if row:
                return row[0], row[1]
    except Exception: # pylint: disable=broad-exception-caught
        pass
    return None, None

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
    """Parses the end condition argument."""
    if not value:
        return None, None

    value = str(value).strip()
    if value.isdigit():
        return int(value), None

    if value.lower().startswith('a') and value[1:].isdigit():
        return None, int(value[1:])

    return None, None

def _check_exists(post_id, target_path, db_ctx: DbContext):
    """Checks if a post should be skipped."""
    cur = db_ctx.local.cursor()

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
    """Handles the actual file I/O with retries, logging, and auth passing."""
    post = task.post
    file_url = post.get('file_url') or post.get('large_file_url')

    if not file_url:
        msg = "No file_url found (Access Denied or Deleted)"
        _log_error(task.output_path, task.source_context, post['id'], msg)
        return f"[Skip] ID {post['id']} has no file_url."

    if file_url.startswith("/"):
        file_url = f"{task.base_url}{file_url}"

    ext = post.get('file_ext') or Path(file_url).suffix.strip('.')
    filename = task.args.filename_fmt.format(
        id=post['id'], md5=post.get('md5', ''), sitename=task.sitename, ext=ext
    )
    filename = "".join(x for x in filename if x.isalnum() or x in "._-")

    safe_folder = "".join(x for x in task.search_query[:50] if x.isalnum() or x in " ._-").strip() or "misc"
    target_dir = task.output_path / safe_folder
    target_dir.mkdir(parents=True, exist_ok=True)

    out_path = target_dir / filename

    exists, msg = _check_exists(post['id'], out_path, db_ctx)
    if exists:
        return msg

    if out_path.exists():
        _record_success(task, post.get('md5', ''), out_path, db_ctx)
        return f"[Found] ID {post['id']} exists on disk."

    max_retries = 3
    for attempt in range(max_retries):
        try:
            resp = requests.get(
                file_url,
                stream=True,
                timeout=60,
                cookies=task.cookies,
                headers=task.headers
            )
            resp.raise_for_status()

            content_type = resp.headers.get('Content-Type', '')
            if 'text/html' in content_type:
                _log_error(task.output_path, task.source_context, post['id'], "Got HTML instead of image (Possible Block/Captcha)")
                return f"[Error] ID {post['id']} returned HTML (blocked)."

            with open(out_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if SHUTDOWN_EVENT.is_set():
                        return "[Aborted] Shutdown triggered."
                    f.write(chunk)

            if out_path.stat().st_size < 1024 and 'text/html' not in content_type:
                _log_error(task.output_path, task.source_context, post['id'], "File too small (<1KB). Suspicious.")

            return out_path

        except (requests.RequestException, ConnectionError, OSError) as e:
            is_reset = "Connection reset by peer" in str(e) or "104" in str(e)

            if attempt < max_retries - 1:
                sleep_time = 2 * (attempt + 1)
                reason = "Connection Reset" if is_reset else "Network Error"
                print(f"[!] Retry {attempt+1}/{max_retries} for ID {post['id']} ({reason}). Sleeping {sleep_time}s...")
                time.sleep(sleep_time)
                if out_path.exists():
                    out_path.unlink()
            else:
                err_msg = f"Failed after {max_retries} retries. Last error: {str(e)}"
                _log_error(task.output_path, task.source_context, post['id'], err_msg)
                if out_path.exists():
                    out_path.unlink()
                raise e

    return f"[Error] ID {post['id']} failed to download."

def _download_worker(task: DownloadTask, db_path):
    """Worker function."""
    if SHUTDOWN_EVENT.is_set():
        return "[Aborted] Shutdown pending."

    local_conn = sqlite3.connect(db_path, timeout=30)
    gdl_conn = sqlite3.connect(task.gdl_db_path, timeout=30) if task.gdl_db_path else None

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
        _log_error(task.output_path, task.source_context, task.post.get('id'), str(e))
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
        if p.get('id', 0) <= end_id:
            hit_limit = True
            break
        filtered.append(p)
    return hit_limit, filtered

def _probe_smart_resume(ctx: FetchContext, target_id_str) -> bool:
    """Checks Page 1000 for shallow/deep determination."""
    if not target_id_str:
        return False

    try:
        target_id = int(target_id_str.strip('ab'))
    except ValueError:
        return False

    print(f"\n[?] Probing Page {PAGINATION_LIMIT} for smart resume...")
    params = {"tags": ctx.tags, "page": PAGINATION_LIMIT, "limit": 1}
    data = _fetch_metadata_page(ctx.session, f"{ctx.base_url}/posts.json", params, ctx.args, ctx.solver)

    if isinstance(data, list) and data:
        limit_id = data[0].get('id', 0)
        if target_id > limit_id:
            print(f"[✓] Target ID {target_id} is shallow (>{limit_id}). Using Fast Threaded Mode.")
            return True
        print(f"[!] Target ID {target_id} is deep (<{limit_id}). Using Safe Sequential Mode.")
        return False

    print("[!] Probe failed or page empty. Defaulting to sequential.")
    return False

def _fetch_threaded_loop(ctx: FetchContext, start_page: int) -> Tuple[List[dict], Optional[str]]:
    """Handles the threaded page-based fetching loop."""
    all_posts = []
    batch_size = max(5, min(ctx.args.threads, 50))

    # FIX: Ensure we have a valid int for math operations
    current_page = start_page if start_page is not None else 1
    base_api = f"{ctx.base_url}/posts.json"
    next_start_id = None

    print(f"\n--- Fetching API Metadata (Threaded Page Mode - Batch {batch_size}) ---")

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

                for p in filtered_data:
                    p['_source_page'] = f"Page {p_num}"

                all_posts.extend(filtered_data)

                if filtered_data and p_num % 10 == 0:
                    _save_checkpoint(ctx.db_path, ctx.tags, p_num, filtered_data[0]['id'])

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

        for p in filtered_data:
            p['_source_page'] = f"ID {current_id_param}"

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

    forced_threaded = False
    if start_id:
        if _probe_smart_resume(ctx, start_id):
            forced_threaded = True
            start_page = 1
            start_id = None

    use_sequential = (
        (start_page is not None and start_page >= PAGINATION_LIMIT) or
        (start_id is not None)
    )

    if not use_sequential or forced_threaded:
        threaded_posts, next_start_id = _fetch_threaded_loop(ctx, start_page)
        all_posts.extend(threaded_posts)

        if next_start_id:
            use_sequential = True
            start_id = next_start_id
        else:
            use_sequential = False

    if use_sequential:
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

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    pool_size = args.threads + 5
    adapter = HTTPAdapter(
        pool_connections=pool_size,
        pool_maxsize=pool_size,
        max_retries=retry_strategy
    )
    session.mount('https://', adapter)
    session.mount('http://', adapter)

    if not (args.tags or args.query):
        print("[Error] You must provide a URL or tags.")
        return None

    tags, start_page, start_id, base_url = _parse_input_query(args.tags or args.query, args.base_url)

    cli_start_arg = str(args.start_page).strip()

    if cli_start_arg.isdigit():
        req_page = int(cli_start_arg)
        if req_page != 1 or start_page is None:
            start_page = req_page
            start_id = None

            if req_page >= PAGINATION_LIMIT:
                sitename_temp, _ = _get_site_details(session, base_url)
                out_path_temp = Path(args.output)
                if args.output == "downloads": out_path_temp = out_path_temp / sitename_temp
                db_path_temp = out_path_temp / "global_downloads.db"

                if db_path_temp.exists():
                    print(f"[?] Looking for checkpoint near Page {req_page}...")
                    cp_id, cp_page = _get_checkpoint_id(db_path_temp, tags, req_page)
                    if cp_id:
                        print(f"[✓] Found checkpoint! Page {cp_page} -> ID {cp_id}")
                        start_page = None
                        start_id = f"b{cp_id}"
                    else:
                        print(f"[!] No checkpoint found. Cannot jump to Page {req_page}. Starting from 1.")
                        start_page = 1

    elif cli_start_arg.lower().startswith(('a', 'b')):
        start_id = cli_start_arg
        start_page = None

    end_page_limit, end_id_limit = _parse_end_condition(args.end_page)

    sitename = args.sitename
    if sitename == "auto":
        sitename, base_url = _get_site_details(session, base_url)

    root_output_path = Path(args.output)
    if args.output == "downloads" and sitename:
        root_output_path = Path(sitename)

    l_conn, _, db_path = _init_dbs(root_output_path, args.gdl_db)
    l_conn.close()

    ctx = FetchContext(
        session=session, args=args, solver=solver, tags=tags,
        base_url=base_url, end_page=end_page_limit, end_id=end_id_limit,
        db_path=db_path
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
    if g_conn: g_conn.close()

    print("\n--- Starting Downloads (Ctrl+C to stop safely) ---")

    # FIX: Correctly extract cookies from MozillaCookieJar
    cookies = requests.utils.dict_from_cookiejar(ctx.session.cookies)
    headers = dict(ctx.session.headers)

    tasks = []
    for p in all_posts:
        tasks.append(DownloadTask(
            post=p, args=args, output_path=root_output_path,
            gdl_db_path=args.gdl_db, sitename=sitename, base_url=ctx.base_url,
            search_query=ctx.tags,
            source_context=p.get('_source_page', 'Unknown'),
            cookies=cookies,
            headers=headers
        ))

    executor = ThreadPoolExecutor(max_workers=args.threads)
    futures = {executor.submit(_download_worker, t, str(db_path)): t for t in tasks}
    completed = 0
    consecutive_skips = 0  # Initialize counter

    try:
        for future in as_completed(futures):
            res = future.result()
            print(f"[{completed + 1}/{len(tasks)}] {res}")
            completed += 1

            # [NEW] Check for abort condition
            if res.startswith("[Skip]") or res.startswith("[Found]"):
                consecutive_skips += 1
            else:
                consecutive_skips = 0

            if args.abort > 0 and consecutive_skips >= args.abort:
                print(f"\n[!] Abort limit reached ({args.abort} consecutive skips). Stopping...")
                raise KeyboardInterrupt  # Trigger safe shutdown

    except KeyboardInterrupt:
        print("\n\n[!] SHUTDOWN TRIGGERED. Waiting for active downloads to finish...")
        SHUTDOWN_EVENT.set()
        executor.shutdown(wait=True)
        try:
            with sqlite3.connect(db_path) as conn:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            print("[✓] WAL Checkpointed.")
        except Exception as e: # pylint: disable=broad-exception-caught
            print(f"[!] Failed to checkpoint WAL: {e}")

        print("[✓] Safe shutdown complete.")
