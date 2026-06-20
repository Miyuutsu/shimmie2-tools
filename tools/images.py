# pylint: disable=too-many-lines,line-too-long,too-many-locals,too-many-branches
"""
The 'too big to manage again' edition.
Image Downloader with Threaded API, Subfolders, Checkpoints, WAL Support, and Error Logging.
"""
import hashlib
import time
import sqlite3
import re
import threading
import uuid
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import NamedTuple, Optional, Tuple, List, Union, Dict, Any
from urllib.parse import urlparse, parse_qs, unquote
from http.cookiejar import MozillaCookieJar

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from functions.captcha import get_protected_session, AntiBotSolver

SHUTDOWN_EVENT = threading.Event()
LOG_LOCK = threading.Lock()
PAGINATION_LIMIT = 1000

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

CREATE TABLE IF NOT EXISTS checkpoints (
    search_query TEXT,
    page_num INTEGER,
    post_id TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (search_query, page_num)
);
"""

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
    blacklist: set

class V1PostsAPI:
    """Client for the v1 Posts API."""

    def __init__(self, base_url: str, session=None):
        self.base_url = base_url.rstrip('/')
        self.session = session or requests.Session()

    def _format_tags(self, tags: Optional[Union[str, List[str]]]) -> Optional[str]:
        if not tags:
            return None
        if isinstance(tags, list):
            return ",".join([t.replace('_', ' ') for t in tags])
        if ',' in tags:
            return tags
        parts = tags.split(' ')
        return ",".join([t.replace('_', ' ') for t in parts if t])

    # pylint: disable=too-many-arguments, too-many-positional-arguments, too-many-locals
    def get_posts(
        self, tags=None, or_tags=None, filter_tags=None, unless_tags=None,
        limit=None, offset=None, order=None, mime_types=None, mimes=None,
        incl_tags=None, comb_tag_namespace=None
    ) -> Dict[str, Any]:
        """Fetch posts based on tags, offset, mime parameters, and filters via V1 API."""
        url = f"{self.base_url}/api/v1/posts"
        params = {}

        formatted_tags = self._format_tags(tags)
        if formatted_tags is not None:
            params["tags"] = formatted_tags

        formatted_or = self._format_tags(or_tags)
        if formatted_or is not None:
            params["or"] = formatted_or

        formatted_filter = self._format_tags(filter_tags)
        if formatted_filter is not None:
            params["filter"] = formatted_filter

        formatted_unless = self._format_tags(unless_tags)
        if formatted_unless is not None:
            params["unless"] = formatted_unless

        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if order is not None:
            params["order"] = order

        if mime_types is not None:
            params["mime-type"] = mime_types
        if mimes is not None:
            params["mime"] = mimes
        if incl_tags is not None:
            params["inclTags"] = str(incl_tags).lower()
        if comb_tag_namespace is not None:
            params["combTagNamespace"] = str(comb_tag_namespace).lower()

        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    # pylint: disable=too-many-arguments, too-many-positional-arguments
    def get_post(
        self,
        post_id: Optional[int] = None,
        ipfs: Optional[str] = None,
        md5: Optional[str] = None,
        sha256: Optional[str] = None,
        comb_tag_namespace: Optional[bool] = None
    ) -> Dict[str, Any]:
        """Retrieve details for a specific post via V1 API."""
        url = f"{self.base_url}/api/v1/post"
        params = {}

        if post_id is not None:
            params["id"] = post_id
        if ipfs is not None:
            params["ipfs"] = ipfs
        if md5 is not None:
            params["md5"] = md5
        if sha256 is not None:
            params["sha256"] = sha256

        if not params:
            raise ValueError("Must provide either post_id, ipfs, md5, or sha256.")

        if comb_tag_namespace is not None:
            params["combTagNamespace"] = str(comb_tag_namespace).lower()

        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

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
    is_v1: bool = False
    api_client: Optional[Any] = None
    sitename: str = ""

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
                """INSERT OR REPLACE INTO checkpoints (search_query, page_num, post_id)
                VALUES (?, ?, ?);""",
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
                "SELECT post_id, page_num FROM checkpoints WHERE search_query = ? AND page_num <= ?"
                " ORDER BY page_num DESC LIMIT 1",
                (tags, page_num)
            )
            row = cur.fetchone()
            if row:
                return row[0], row[1]
    except Exception: # pylint: disable=broad-exception-caught
        pass
    return None, None

def _get_last_checkpoint_page(db_path, tags):
    """Finds the highest page number saved for a query to enable auto-resume."""
    try:
        with sqlite3.connect(db_path, timeout=30) as conn:
            cur = conn.cursor()
            cur.execute("SELECT MAX(page_num) FROM checkpoints WHERE search_query = ?", (tags,))
            row = cur.fetchone()
            if row and row[0]:
                return row[0]
    except Exception: # pylint: disable=broad-exception-caught
        pass
    return None

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

    if "Tags" in post and isinstance(post["Tags"], list):
        tags = []
        for t in post["Tags"]:
            ns = t.get("Namespace", "")
            tag_name = t.get("Tag", "")

            # Format as 'namespace:tag' (e.g., 'artist:bkub'), but keep general tags clean
            if ns and ns != "general":
                tags.append(f"{ns}:{tag_name}")
            else:
                tags.append(tag_name)

        # Return immediately so it doesn't hit the Danbooru logic below
        return "\n".join(tags)

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

def _load_blacklist(filepath: Optional[str]) -> set:
    """Reads the blacklist file into a memory set."""
    if not filepath:
        return set()
    path = Path(filepath)
    if not path.exists():
        return set()
    with open(path, 'r', encoding='utf-8') as f:
        # Ignore empty lines and comments
        return {line.strip().lower() for line in f if line.strip() and not line.startswith('#')}

def _is_blacklisted(post: dict, blacklist: set) -> bool:
    """Checks if any of the post's tags exist in the blacklist."""
    if not blacklist:
        return False

    tag_string = post.get('tag_string', '')
    if not tag_string:
        tag_string = _construct_tag_string(post)

    post_tags = set()
    # Replace newlines (from _construct_tag_string) with spaces to separate tags
    for t in tag_string.replace('\n', ' ').split():
        clean_tag = t.strip().lower()
        post_tags.add(clean_tag)
        # If it's a namespaced tag (e.g., artist:bkub), add the bare tag ('bkub') too
        # so the user's blacklist catches it either way.
        if ':' in clean_tag:
            post_tags.add(clean_tag.split(':', 1)[-1])

    return bool(post_tags.intersection(blacklist))

def _setup_file_path(task, post):
    """Handles path determination and initial setup."""
    post_id = post['id']

    file_url = post.get('file_url') or post.get('large_file_url')
    if not file_url:
        msg = "No file_url found (Access Denied or Deleted)"
        _log_error(task.output_path, task.source_context, post_id, msg)
        return None, None, f"[Skip] ID {post_id} has no file_url."

    if file_url.startswith("/"):
        file_url = f"{task.base_url}{file_url}"

    ext = post.get('file_ext') or Path(file_url).suffix.strip('.')

    filename = task.args.filename_fmt.format(
        id=post_id, md5=post.get('md5', ''), sitename=task.sitename, ext=ext
    )
    filename = "".join(x for x in filename if x.isalnum() or x in "._-")

    safe_folder = "".join(
        x for x in task.search_query[:50] if x.isalnum() or x in " ._-").strip() or "misc"

    target_dir = task.output_path / safe_folder
    target_dir.mkdir(parents=True, exist_ok=True)

    out_path = target_dir / filename

    return file_url, out_path, f"[Ready] ID {post_id} path set."

def _check_existing(task, post, out_path, db_ctx):
    """Checks if the file is already recorded or exists locally."""
    exists_db, msg_db = _check_exists(post['id'], out_path, db_ctx)
    if exists_db:
        return msg_db

    if out_path.exists():
        _record_success(task, post.get('md5', ''), out_path, db_ctx)
        return f"[Found] ID {post['id']} exists on disk."

    return None

def _perform_single_download(task, post, file_url, out_path):
    """Attempts one file download and performs immediate integrity checks."""
    post_id = post['id']
    proxies = None

    if getattr(task.args, 'proxy', None):
        if 'socks5h://' in task.args.proxy:
            # Force Tor to build a separate circuit for this download
            auth_id = uuid.uuid4().hex[:8]
            clean_proxy = task.args.proxy.replace('socks5h://', '')
            iso_proxy = f"socks5h://{auth_id}:circuit@{clean_proxy}"
            proxies = {'http': iso_proxy, 'https': iso_proxy}
        else:
            proxies = {'http': task.args.proxy, 'https': task.args.proxy}

    try:
        resp = requests.get(
            file_url,
            stream=True,
            timeout=60,
            cookies=task.cookies,
            headers=task.headers,
            proxies=proxies
        )
        resp.raise_for_status()

        content_type = resp.headers.get('Content-Type', '')
        if 'text/html' in content_type:
            _log_error(
                task.output_path, task.source_context, post_id,
                "Got HTML instead of image (Possible Block/Captcha)"
            )
            return f"[Error] ID {post_id} returned HTML (blocked)."

        expected_size = int(resp.headers.get('Content-Length', 0))

        with open(out_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if SHUTDOWN_EVENT.is_set():
                    return "[Aborted] Shutdown triggered."
                f.write(chunk)

        actual_size = out_path.stat().st_size

        if actual_size < 1024 and 'text/html' not in content_type:
            _log_error(
                task.output_path, task.source_context, post_id,
                "File too small (<1KB). Suspicious."
            )
            return f"[Suspicious] ID {post_id} file is suspiciously small."

        # --- NEW: INTEGRITY CHECK LOGIC ---
        if expected_size > 0 and actual_size != expected_size:
            # Size mismatch detected! Fallback to MD5 verification to ensure it's not just gzip compression.
            expected_md5 = post.get('md5')
            if expected_md5:
                hasher = hashlib.md5()
                with open(out_path, 'rb') as f:
                    for chunk in iter(lambda: f.read(8192), b""):
                        hasher.update(chunk)

                if hasher.hexdigest() != expected_md5:
                    # Throwing an OSError cleanly triggers your existing retry loop
                    raise OSError("Truncated download: Size mismatch AND MD5 failed.")
            else:
                # If the post data doesn't have an MD5 to fallback on, trust the Content-Length failure
                raise OSError(f"Truncated download: Expected {expected_size} bytes, got {actual_size}.")

        return out_path

    except (requests.RequestException, ConnectionError, OSError) as e:
        return e

def _handle_retry_or_final_failure(task, out_path, e, attempt, max_retries):
    """Manages the logic for retrying or handling final failure/cleanup."""
    post_id = task.post['id']
    is_reset = "Connection reset by peer" in str(e) or "104" in str(e)

    if attempt < max_retries - 1:
        sleep_time = 2 * (attempt + 1)
        reason = "Connection Reset" if is_reset else "Network Error"
        print(f"[!] Retry {attempt+1}/{max_retries} for ID {post_id} ({reason}).")
        print(f"Sleeping {sleep_time}s...")

        if out_path.exists():
            out_path.unlink()
    else:
        err_msg = f"Failed after {max_retries} retries. Last error: {str(e)}"
        _log_error(task.output_path, task.source_context, post_id, err_msg)

        if out_path.exists():
            out_path.unlink()

        raise e

def _attempt_download(task, post, file_url, out_path):
    """Handles the actual file I/O with retries, logging, and auth passing."""
    post_id = post['id']
    max_retries = 3

    for attempt in range(max_retries):
        result = _perform_single_download(task, post, file_url, out_path)

        if isinstance(result, Path):
            return result

        if isinstance(result, str):
            return result

        if isinstance(result, Exception):
            _handle_retry_or_final_failure(task, out_path, result, attempt, max_retries)

    return f"[Error] ID {post_id} failed to download."

def _download_file(task, db_ctx):
    """Handles the actual file I/O with retries, logging, and auth passing."""
    post = task.post

    file_url, out_path, msg = _setup_file_path(task, post)
    if not file_url:
        return msg

    existing_msg = _check_existing(task, post, out_path, db_ctx)
    if existing_msg:
        return existing_msg

    if _is_blacklisted(post, task.blacklist):
        # Fake a success in the database so it gets skipped on future runs
        _record_success(task, post.get('md5', ''), out_path, db_ctx)
        return f"[Skip] ID {post['id']} ignored (Blacklisted)."

    return _attempt_download(task, post, file_url, out_path)

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

def _map_v1_to_danbooru(posts: List[Dict[str, Any]], base_url: str) -> List[Dict[str, Any]]:
    """Translates V1 API response dictionaries into standard Danbooru formats."""
    for p in posts:
        p["id"] = p.get("ID")
        file_info = p.get("File", {})
        p["md5"] = file_info.get("Md5")
        raw_url = file_info.get("Url", "")

        if raw_url:
            base = base_url.rstrip('/')
            if raw_url.startswith('/'):
                p["file_url"] = f"{base}{raw_url}"
            else:
                p["file_url"] = f"{base}/{raw_url}"
            p["file_ext"] = raw_url.split('.')[-1] if '.' in raw_url else ""
        else:
            p["file_ext"] = ""

    return posts

# pylint: disable=too-many-return-statements
def _fetch_metadata_page(url, params, ctx) -> Union[dict, list, str, None]:
    """Fetches a single page of metadata."""
    if SHUTDOWN_EVENT.is_set():
        return None

    if getattr(ctx, 'is_v1', False) and ctx.api_client:
        try:
            page_param = params.get("page", 1)
            offset = int(page_param) - 1 if str(page_param).isdigit() else 0

            data = ctx.api_client.get_posts(
                tags=params.get("tags"),
                or_tags=getattr(ctx.args, 'or_tags', None),
                filter_tags=getattr(ctx.args, 'filter_tags', None),
                unless_tags=getattr(ctx.args, 'unless_tags', None),
                limit=params.get("limit"),
                offset=offset,
                order=getattr(ctx.args, 'order', None),
                mime_types=getattr(ctx.args, 'mime_types', None),
                mimes=getattr(ctx.args, 'mimes', None)
            )

            total = data.get("TotalPosts")
            if total is not None and str(page_param) == "1":
                print(f"\n[✓] API reports EXACTLY {total} matching posts on the server!")

            return _map_v1_to_danbooru(data.get("Posts", []), ctx.api_client.base_url)

        except requests.exceptions.RequestException as e:
            print(f"\n[Error] V1 API fetch failed: {e}")
            return None

    session = ctx.session
    args = ctx.args
    solver = ctx.solver

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
    data = _fetch_metadata_page(f"{ctx.base_url}/posts.json", params, ctx)

    if isinstance(data, list) and data:
        limit_id = data[0].get('id', 0)
        if target_id > limit_id:
            print(f"[✓] Target ID {target_id} is shallow (>{limit_id}). Using Fast Threaded Mode.")
            return True
        print(f"[!] Target ID {target_id} is deep (<{limit_id}). Using Safe Sequential Mode.")
        return False

    print("[!] Probe failed or page empty. Defaulting to sequential.")
    return False

def _submit_batch(executor: ThreadPoolExecutor, ctx, current_page, batch_size):
    """Submits futures for a range of pages up to batch_size."""
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
            _fetch_metadata_page,
            ctx.base_url + "/posts.json",
            params,
            ctx
        )
        futures[future] = page_num
    return futures

def _are_all_posts_downloaded(ctx: FetchContext, posts_batch: List[dict]) -> bool:
    """Checks if an entire batch of posts already exists locally to allow early fetch aborts."""
    if not posts_batch:
        return False
    ids = [str(p.get('id')) for p in posts_batch if p.get('id')]
    if not ids:
        return False

    existing = set()
    try:
        with sqlite3.connect(ctx.db_path, timeout=30) as conn:
            cur = conn.cursor()
            placeholders = ','.join('?' for _ in ids)
            cur.execute(f"SELECT post_id FROM downloads WHERE status='completed' AND post_id IN ({placeholders})", ids) # pylint: disable=line-too-long
            for row in cur.fetchall():
                existing.add(str(row[0]))

        gdl_path = getattr(ctx.args, 'gdl_db', None)
        if gdl_path and len(existing) < len(ids):
            with sqlite3.connect(gdl_path, timeout=30) as gdl_conn:
                gdl_cur = gdl_conn.cursor()
                keys = [f"{ctx.sitename} {i}" for i in ids if i not in existing]
                placeholders = ','.join('?' for _ in keys)
                gdl_cur.execute(f"SELECT entry FROM archive WHERE entry IN ({placeholders})", keys)
                for row in gdl_cur.fetchall():
                    existing.add(str(row[0].split(' ', 1)[1]))
    except Exception: # pylint: disable=broad-exception-caught
        pass

    return len(existing) >= len(ids)

def _process_batch_results(ctx, batch_results, last_batch_min_id):
    """Processes a sorted batch of results, checking limits and accumulating data."""
    batch_posts = []
    batch_has_data = False
    abort_fetch = False

    for p_num, data in batch_results:
        if data == "410_GONE":
            print(f"\n[Info] Page {p_num} hit limit. Switching modes...")
            return batch_posts, last_batch_min_id, True, False

        if not data:
            print(f"\n[Info] Page {p_num} is empty or failed. Stopping fetch.")
            return batch_posts, None, False, True

        hit_limit, filtered_data = _reached_id_limit(data, ctx.end_id)

        if filtered_data and _are_all_posts_downloaded(ctx, filtered_data):
            print(
                f"\n[!] Page {p_num} contains all previously downloaded posts. Aborting API fetch.")
            batch_posts.extend(filtered_data)
            return batch_posts, None, True, True

        batch_posts.extend(filtered_data)
        batch_has_data = True

        for p in filtered_data:
            p['_source_page'] = f"Page {p_num}"

        if filtered_data and p_num % 10 == 0:
            _save_checkpoint(ctx.db_path, ctx.tags, p_num, filtered_data[0]['id'])

        if hit_limit:
            print(f"\n[✓] Reached End-ID limit ({ctx.end_id}). Stopping.")
            return batch_posts, None, True, True

        last_batch_min_id = filtered_data[-1].get('id')

        if len(data) < ctx.args.limit:
            print(f"\n[Info] Page {p_num} has partial data. End of results.")
            return batch_posts, None, batch_has_data, True

    return batch_posts, last_batch_min_id, batch_has_data, abort_fetch

def _fetch_threaded_loop(ctx: FetchContext, start_page: int) -> Tuple[List[dict], Optional[str]]:
    """Handles the threaded page-based fetching loop."""
    all_posts = []
    batch_size = max(5, min(ctx.args.threads, 50))

    current_page = start_page if start_page is not None else 1
    next_start_id = None

    print(f"\n--- Fetching API Metadata (Threaded Page Mode - Batch {batch_size}) ---")

    with ThreadPoolExecutor(max_workers=batch_size) as executor:
        while not SHUTDOWN_EVENT.is_set():
            if ctx.end_page and current_page > ctx.end_page:
                break

            futures = _submit_batch(executor, ctx, current_page, batch_size)

            if not futures:
                break

            last_batch_min_id = None
            batch_has_data = False

            batch_results = sorted(
                [(futures[f], f.result()) for f in as_completed(futures)],
                key=lambda x: x[0]
            )

            batch_posts, last_batch_min_id, batch_has_data, abort_fetch = \
                _process_batch_results(ctx, batch_results, last_batch_min_id)

            all_posts.extend(batch_posts)

            if abort_fetch:
                break

            if not batch_has_data:
                if last_batch_min_id:
                    next_start_id = f"b{last_batch_min_id}"
                elif all_posts and all_posts[-1].get('id'):
                    last_id = all_posts[-1]['id']
                    next_start_id = f"b{last_id}"
                else:
                    break

            if current_page >= PAGINATION_LIMIT and last_batch_min_id:
                next_start_id = f"b{last_batch_min_id}"
                break

            if batch_has_data and next_start_id:
                break

            current_page += batch_size
            time.sleep(ctx.args.sleep)

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

        data = _fetch_metadata_page(base_api, params, ctx)

        if not data or data == "410_GONE":
            print("\n[Info] No more posts found.")
            break

        hit_limit, filtered_data = _reached_id_limit(data, ctx.end_id)

        if filtered_data and _are_all_posts_downloaded(ctx, filtered_data):
            print("\n[!] Batch contains all previously downloaded posts. Aborting API fetch early.")
            break

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

def _resolve_start_state(session, args):
    """
    Parses the command-line start argument (page or ID) and resolves
    the starting state, including checkpoint lookups if pagination is used.
    """
    cli_start_arg = str(args.start_page).strip()

    if cli_start_arg.isdigit():
        req_page = int(cli_start_arg)

        if req_page != 1:
            sitename, _ = _get_site_details(session, args.base_url)

            root_path = Path(args.output)
            if args.output == "downloads" and sitename:
                root_path = Path(sitename)

            db_path = root_path / "global_downloads.db"

            if db_path.exists():
                print(f"[?] Looking for checkpoint near Page {req_page}...")
                cp_id, cp_page = _get_checkpoint_id(db_path, args.tags or args.query, req_page)
                if cp_id:
                    print(f"[✓] Found checkpoint! Page {cp_page} -> ID {cp_id}")
                    return None, f"b{cp_id}"
                print(f"[!] No checkpoint found. Cannot jump to Page {req_page}. Starting from 1.")
                return 1, None

            return req_page, None

    elif cli_start_arg.lower().startswith(('a', 'b')):
        return None, cli_start_arg

    return 1, None

def _setup_network_and_parse_input(args):
    """Handles session setup, anti-bot solver, and initial URL parsing."""
    session = get_protected_session()
    solver = AntiBotSolver() if args.captcha else None

    if getattr(args, 'proxy', None):
        session.proxies = {'http': args.proxy, 'https': args.proxy}

    if getattr(args, 'cookies', None):
        cookie_path = Path(args.cookies)
        if cookie_path.exists():
            try:
                cj = MozillaCookieJar(str(cookie_path))
                cj.load(ignore_discard=True, ignore_expires=True)

                for cookie in cj:
                    session.cookies.set_cookie(cookie)

                print(f"[INFO] Loaded custom cookies from {cookie_path}")
            except Exception as e: # pylint: disable=broad-exception-caught
                print(f"[WARNING] Failed to load cookies: {e}")

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
        return session, solver, None, None, None, None

    tags, start_page_parsed, start_id_parsed, base_url = _parse_input_query(
        args.tags or args.query, args.base_url
    )

    return session, solver, tags, start_page_parsed, start_id_parsed, base_url

def _resolve_state_and_conditions(session, args, start_page_parsed, start_id_parsed):
    """Handles resolving initial state and setting end conditions."""
    resolved_start_page, resolved_start_id = _resolve_start_state(session, args)

    start_page = resolved_start_page if resolved_start_page is not None else start_page_parsed
    start_id = resolved_start_id if resolved_start_id else start_id_parsed

    end_page_limit, end_id_limit = _parse_end_condition(args.end_page)

    return start_page, start_id, end_page_limit, end_id_limit

def _setup_site_paths(session, args, base_url):
    """Handles sitename determination and root path calculation/DB init."""
    sitename = args.sitename
    if sitename == "auto":
        sitename, base_url = _get_site_details(session, base_url)

    root_output_path = Path(args.output)
    if args.output == "downloads" and sitename:
        root_output_path = Path(sitename)

    l_conn, _, db_path = _init_dbs(root_output_path, args.gdl_db)
    l_conn.close()

    return sitename, root_output_path, db_path

def _detect_v1_api(session, base_url) -> Tuple[bool, Optional[Any]]:
    """Probes the target server to see if it supports the Permabooru V1 API."""
    try:
        probe = session.get(f"{base_url}/api/v1/posts", params={"limit": 1}, timeout=5)
        if probe.status_code == 200:
            data = probe.json()
            if isinstance(data, dict) and "Posts" in data:
                print(f"[✓] Auto-Detected V1 API endpoint on {base_url}")
                return True, V1PostsAPI(base_url, session=session)

    except (requests.RequestException, ValueError):
        # ValueError handles cases where the response isn't valid JSON
        pass
    return False, None

# pylint: disable=too-many-locals
def _configure_download(args):
    """Parses arguments and sets up configuration for the run."""

    # TRICK 1: Star unpack (*) intermediate variables!
    # This automatically captures start_page_parsed and start_id_parsed into a single
    # lightweight list without changing the helper function's signature.
    session, solver, tags, *parsed_starts, base_url = \
        _setup_network_and_parse_input(args)

    if tags is None:
        return None

    # We then unpack them right back out using the star!
    start_page, start_id, end_page_limit, end_id_limit = \
        _resolve_state_and_conditions(
            session, args, *parsed_starts
        )

    sitename, root_output_path, db_path = _setup_site_paths(
        session, args, base_url
    )

    if getattr(args, 'resume', False):
        last_page = _get_last_checkpoint_page(db_path, tags)
        if last_page:
            print(f"[✓] Auto-Resume triggered: Jumping to Page {last_page} for '{tags}'")
            start_page = last_page
            start_id = None

    is_v1, api_client = _detect_v1_api(session, base_url)

    return FetchContext(
        session=session,
        args=args,
        solver=solver,
        tags=tags,
        base_url=base_url,
        end_page=end_page_limit,
        end_id=end_id_limit,
        db_path=db_path,
        is_v1=is_v1,
        api_client=api_client,
        sitename=sitename
    ), start_page, start_id, sitename, root_output_path

def _prepare_tasks(all_posts, args, root_output_path, sitename, ctx, blacklist_tags):
    """Generates and queues all DownloadTask objects."""
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
            headers=headers,
            blacklist=blacklist_tags
        ))
    return tasks

def _monitor_and_execute_tasks(tasks, args, db_path):
    """Submits tasks, monitors results for completion/abort, and handles KeyboardInterrupt."""
    executor = ThreadPoolExecutor(max_workers=args.threads)
    futures = {executor.submit(_download_worker, t, str(db_path)): t for t in tasks}
    completed = 0
    consecutive_skips = 0

    try:
        for future in as_completed(futures):
            res = future.result()
            print(f"[{completed + 1}/{len(tasks)}] {res}")
            completed += 1

            if "(Blacklisted)" in res:
                pass
            elif res.startswith("[Skip]") or res.startswith("[Found]"):
                consecutive_skips += 1
            else:
                consecutive_skips = 0

            if args.abort > 0 and consecutive_skips >= args.abort:
                print(f"\n[!] Abort limit reached ({args.abort} consecutive skips). Stopping...")
                raise KeyboardInterrupt

        executor.shutdown(wait=True)

    except KeyboardInterrupt:
        _execute_shutdown_sequence(executor, db_path)

def _execute_shutdown_sequence(executor, db_path):
    """Performs graceful shutdown, event signaling, and WAL checkpointing."""
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

def run(args):
    """Main entry point. Orchestrates config, fetch, and download."""
    config = _configure_download(args)
    if not config:
        return

    ctx, start_page, start_id, sitename, root_output_path = config

    print(f"=== Image Downloader ({sitename}) ===")
    print(f"🌍 Base URL: {ctx.base_url}")
    print(f"📂 Output:   {root_output_path}")
    print(f"🏷️ Tags:     {ctx.tags}")

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

    blacklist_tags = _load_blacklist(getattr(args, 'blacklist', None))
    if blacklist_tags:
        print(f"🚫 Loaded {len(blacklist_tags)} tags from blacklist.")

    tasks = _prepare_tasks(all_posts, args, root_output_path, sitename, ctx, blacklist_tags)

    print("\n--- Starting Downloads (Ctrl+C to stop safely) ---")

    _monitor_and_execute_tasks(tasks, args, db_path)
