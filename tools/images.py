"""
Image Downloader with Threaded API, Subfolders, Checkpoints, WAL Support, and Error Logging.
"""
import time
import sqlite3
import re
import threading
import uuid
import itertools
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import NamedTuple, Optional, Tuple, List, Union, Dict, Any
from urllib.parse import urlparse, parse_qs, unquote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from functions.captcha import get_protected_session, AntiBotSolver

# Global event for safe shutdown and logging lock
SHUTDOWN_EVENT = threading.Event()
END_OF_RESULTS_EVENT = threading.Event() # [NEW] Tells all threads to stop if we hit an empty page
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
    is_v1: bool = False             # [NEW] Flags if site is using new API
    api_client: Optional[Any] = None # [NEW] Holds the V1PostsAPI instance

class V1PostsAPI:
    """
    Client for the v1 Posts API.
    Supports fetching lists of posts and single posts by identifiers.
    """

    def __init__(self, base_url: str, session: Optional[requests.Session] = None):
        """
        Initialize the API client.

        :param base_url: The root URL of the API (e.g., 'http://127.0.0.1:8080')
        :param session: Optional requests.Session object.
                        Use this to handle Tor/I2P proxies externally.
        """
        self.base_url = base_url.rstrip('/')
        # Utilizing a Session object ensures that proxy settings, headers, and
        # connection pooling are cleanly manageable from outside the class.
        self.session = session or requests.Session()

    def _format_tags(self, tags: Optional[Union[str, List[str]]]) -> Optional[str]:
        """
        Converts tags from standard Booru format (space separated, underscore for spaces)
        to the target API format (comma separated, spaces allowed).
        """
        if not tags:
            return None

        if isinstance(tags, list):
            # Convert list to comma-separated string, replacing underscores with spaces
            return ",".join([t.replace('_', ' ') for t in tags])

        # If it's already comma-separated, assume it's correctly formatted for the new API
        if ',' in tags:
            return tags

        # If it's space-separated (standard booru style), split, replace _, and join by comma
        # E.g., "tag_1 tag_2" -> "tag 1,tag 2"
        parts = tags.split(' ')
        return ",".join([t.replace('_', ' ') for t in parts if t])

    def get_posts(
        self,
        tags: Optional[Union[str, List[str]]] = None,
        or_tags: Optional[Union[str, List[str]]] = None,
        filter_tags: Optional[Union[str, List[str]]] = None,
        unless_tags: Optional[Union[str, List[str]]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order: Optional[str] = None,
        mime_types: Optional[Union[str, List[str]]] = None,
        mimes: Optional[Union[str, List[str]]] = None,
        inclTags: Optional[bool] = None,
        combTagNamespace: Optional[bool] = None
    ) -> Dict[str, Any]:
        """
        Fetch posts based on tags, offset, mime parameters, and filters.
        GET /api/v1/posts

        Returns a dict containing 'TotalPosts' and 'Posts' array.
        """
        url = f"{self.base_url}/api/v1/posts"
        params = {}

        # Format tag-related arguments
        formatted_tags = self._format_tags(tags)
        if formatted_tags is not None: params["tags"] = formatted_tags

        formatted_or = self._format_tags(or_tags)
        if formatted_or is not None: params["or"] = formatted_or

        formatted_filter = self._format_tags(filter_tags)
        if formatted_filter is not None: params["filter"] = formatted_filter

        formatted_unless = self._format_tags(unless_tags)
        if formatted_unless is not None: params["unless"] = formatted_unless

        if limit is not None: params["limit"] = limit
        if offset is not None: params["offset"] = offset
        if order is not None: params["order"] = order

        if mime_types is not None: params["mime-type"] = mime_types
        if mimes is not None: params["mime"] = mimes

        # APIs usually expect lower-case booleans natively
        if inclTags is not None: params["inclTags"] = str(inclTags).lower()
        if combTagNamespace is not None: params["combTagNamespace"] = str(combTagNamespace).lower()

        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_post(
        self,
        id: Optional[int] = None,
        ipfs: Optional[str] = None,
        md5: Optional[str] = None,
        sha256: Optional[str] = None,
        combTagNamespace: Optional[bool] = None
    ) -> Dict[str, Any]:
        """
        Retrieve details for a specific post.
        At least one identifier (id, ipfs, md5, sha256) must be provided.
        GET /api/v1/post

        Returns a Post dictionary.
        """
        url = f"{self.base_url}/api/v1/post"
        params = {}

        if id is not None: params["id"] = id
        if ipfs is not None: params["ipfs"] = ipfs
        if md5 is not None: params["md5"] = md5
        if sha256 is not None: params["sha256"] = sha256

        if not params:
            raise ValueError("At least one identifier (id, ipfs, md5, sha256) must be provided.")

        if combTagNamespace is not None:
            params["combTagNamespace"] = str(combTagNamespace).lower()

        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

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

def _get_last_checkpoint_page(db_path, tags):
    """Finds the highest page number saved for a query to enable auto-resume."""
    try:
        with sqlite3.connect(db_path, timeout=30) as conn:
            cur = conn.cursor()
            cur.execute("SELECT MAX(page_num) FROM checkpoints WHERE search_query = ?", (tags,))
            row = cur.fetchone()
            if row and row[0]:
                return row[0]
    except Exception:
        pass
    return None

def _get_existing_post_ids(ctx: FetchContext, root_output_path, sitename, db_ctx: DbContext, posts):
    """Bulk queries databases AND the filesystem to instantly find existing posts."""
    if not posts: return set()
    ids = [str(p.get('id')) for p in posts if p.get('id')]
    if not ids: return set()

    existing = set()

    # --- 1. Database Checks ---
    try:
        cur = db_ctx.local.cursor()
        placeholders = ','.join('?' for _ in ids)

        # We loosen the search_query restriction here. If you have the file, skip it.
        cur.execute(f"SELECT post_id FROM downloads WHERE status='completed' AND post_id IN ({placeholders})", ids)
        for row in cur.fetchall(): existing.add(str(row[0]))

        if db_ctx.gdl and len(existing) < len(ids):
            gdl_cur = db_ctx.gdl.cursor()
            gdl_keys = [f"{sitename} {i}" for i in ids]
            gdl_placeholders = ','.join('?' for _ in gdl_keys)
            gdl_cur.execute(f"SELECT entry FROM archive WHERE entry IN ({gdl_placeholders})", gdl_keys)
            for row in gdl_cur.fetchall():
                existing.add(str(row[0].split(' ', 1)[1]))
    except Exception as e:
        pass

    # --- 2. Lightning Fast Filesystem Check ---
    # Python checks Path.exists() in ~1 microsecond. This catches any DB desyncs instantly.
    safe_folder = "".join(x for x in ctx.tags[:50] if x.isalnum() or x in " ._-").strip() or "misc"
    target_dir = Path(root_output_path) / safe_folder

    for p in posts:
        pid = str(p.get('id'))
        if pid in existing: continue # Already caught by DB

        ext = p.get('file_ext', 'jpg')
        filename = ctx.args.filename_fmt.format(
            sitename=sitename, id=pid, md5=p.get('md5', ''), ext=ext
        )
        filename = "".join(x for x in filename if x.isalnum() or x in "._-")

        # If it's physically on disk, throw it out of memory!
        if (target_dir / filename).exists():
            existing.add(pid)
            # Self-heal the database silently so future DB checks catch it
            try:
                with db_ctx.local:
                    db_ctx.local.execute(
                        "INSERT OR REPLACE INTO downloads (post_id, filepath, search_query, md5, status) VALUES (?, ?, ?, ?, 'completed')",
                        (pid, str(target_dir / filename), ctx.tags, p.get('md5', ''))
                    )
            except Exception:
                pass

    return existing

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

    # [NEW] Handle V1 API 'Tags' array format
    if "Tags" in post and isinstance(post["Tags"], list):
        tags = []
        for t in post["Tags"]:
            ns = t.get("Namespace", "")
            tag_name = t.get("Tag", "")
            if ns and ns != "general":
                tags.append(f"{ns}:{tag_name}")
            else:
                tags.append(tag_name)
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

def _download_file(task, db_ctx, session):
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
            resp = session.get(
                file_url,
                stream=True,
                timeout=60
                # Note: cookies and headers are already loaded into the session object!
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
                return err_msg

    return f"[Error] ID {post['id']} failed to download."

def _download_worker(task: DownloadTask, db_path):
    """Worker function."""
    if SHUTDOWN_EVENT.is_set():
        return "[Aborted] Shutdown pending."

    session = requests.Session()

    # Setup retries for connection stability
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(pool_connections=1, pool_maxsize=1, max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    session.headers.update(task.headers)
    requests.utils.add_dict_to_cookiejar(session.cookies, task.cookies)

    proxy = getattr(task.args, 'proxy', None)
    if proxy:
        if 'socks5h://' in proxy:
            # Force Tor to build a separate circuit for this thread using SOCKS auth isolation
            auth_id = uuid.uuid4().hex[:8]
            clean_proxy = proxy.replace('socks5h://', '')
            iso_proxy = f"socks5h://{auth_id}:circuit@{clean_proxy}"
            session.proxies = {'http': iso_proxy, 'https': iso_proxy}
        else:
            session.proxies = {'http': proxy, 'https': proxy}

    local_conn = sqlite3.connect(db_path, timeout=30)
    gdl_conn = sqlite3.connect(task.gdl_db_path, timeout=30) if task.gdl_db_path else None

    do_dedup = getattr(task.args, 'global_dedup', False)
    db_ctx = DbContext(local_conn, gdl_conn, task.sitename, do_dedup)

    try:
        res = _download_file(task, db_ctx, session)

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

def _fetch_metadata_page(ctx: FetchContext, page_param) -> Union[list, str, None]:
    """Fetches a single page of metadata, routing automatically between V1 and Danbooru APIs."""
    if SHUTDOWN_EVENT.is_set():
        return None

    # --- [NEW] V1 API Logic ---
    if ctx.is_v1:
        try:
            # [FIXED] Revert to Page Index! The API uses offset to skip pages, not items.
            offset = int(page_param) - 1 if isinstance(page_param, int) else 0

            data = ctx.api_client.get_posts(
                tags=ctx.tags,
                or_tags=getattr(ctx.args, 'or_tags', None),
                filter_tags=getattr(ctx.args, 'filter_tags', None),
                unless_tags=getattr(ctx.args, 'unless_tags', None),
                limit=ctx.args.limit,
                offset=offset,
                order=getattr(ctx.args, 'order', None),
                mime_types=getattr(ctx.args, 'mime_types', None),
                mimes=getattr(ctx.args, 'mimes', None)
            )

            # [UI FIX] Print the exact number of total posts so you never have to guess!
            total = data.get("TotalPosts")
            if total is not None and isinstance(page_param, int) and page_param == 1:
                print(f"\n[✓] API reports EXACTLY {total} matching posts on the server!")

            posts = data.get("Posts", [])

            # Map V1 keys to standard Danbooru keys so the downloader handles them seamlessly
            for p in posts:
                p["id"] = p.get("ID")

                file_info = p.get("File", {})
                p["md5"] = file_info.get("Md5")

                raw_url = file_info.get("Url", "")
                if raw_url:
                    base = ctx.base_url.rstrip('/')
                    p["file_url"] = f"{base}{raw_url}" if raw_url.startswith('/') else f"{base}/{raw_url}"
                    # [FIXED] Grab true extension from the URL (e.g., .jpg instead of mime jpeg)
                    ext = raw_url.split('.')[-1] if '.' in raw_url else ""
                else:
                    ext = ""

                if not ext:
                    mime = file_info.get("MimeType", "")
                    ext = mime.split("/")[-1] if "/" in mime else mime

                p["file_ext"] = ext

                # Use the exact URL provided by the API
                raw_url = file_info.get("Url")
                if raw_url:
                    base = ctx.base_url.rstrip('/')
                    p["file_url"] = f"{base}{raw_url}" if raw_url.startswith('/') else f"{base}/{raw_url}"

            return posts
        except Exception as e:
            print(f"\n[Error] V1 API fetch failed: {e}")
            return None

    # --- [ORIGINAL] Danbooru Logic ---
    # ... [Keep your existing Danbooru logic below here]

    # --- [ORIGINAL] Danbooru Logic ---
    url = f"{ctx.base_url}/posts.json"
    params = {"tags": ctx.tags, "page": page_param, "limit": ctx.args.limit}

    try:
        resp = ctx.session.get(url, params=params, timeout=30)
        if ctx.args.captcha and ctx.solver and ctx.solver.detect(resp.text[:2000]):
            if ctx.solver.solve(ctx.session, resp.text, resp.url):
                resp = ctx.session.get(url, params=params, timeout=30)
            else:
                return None
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 410:
            print(f"\n[!] API Limit Reached (410 Gone). Page {params.get('page')} is too deep.")
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
    data = _fetch_metadata_page(ctx, PAGINATION_LIMIT)

    if isinstance(data, list) and data:
        limit_id = data[0].get('id', 0)
        if target_id > limit_id:
            print(f"[✓] Target ID {target_id} is shallow (>{limit_id}). Using Fast Threaded Mode.")
            return True
        print(f"[!] Target ID {target_id} is deep (<{limit_id}). Using Safe Sequential Mode.")
        return False

    print("[!] Probe failed or page empty. Defaulting to sequential.")
    return False

def _fetch_threaded_loop(ctx: FetchContext, start_page: int) -> Optional[str]:
    """Handles the threaded page-based fetching loop (Generator)."""
    batch_size = max(5, min(ctx.args.threads, 50))
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
                if ctx.end_page and page_num > ctx.end_page: break
                if page_num >= PAGINATION_LIMIT:
                    print(f"\n[Info] Page {page_num} reached. Switching to ID Mode.")
                    break

                print(f"Queueing page {page_num}...", end="\r")
                futures[executor.submit(_fetch_metadata_page, ctx, page_num)] = page_num

            if not futures: break

            batch_has_data = False
            batch_results = sorted([(futures[f], f.result()) for f in as_completed(futures)], key=lambda x: x[0])
            last_batch_min_id = None

            for p_num, data in batch_results:
                if data == "410_GONE": break
                if not data: return None

                hit_limit, filtered_data = _reached_id_limit(data, ctx.end_id)
                for p in filtered_data: p['_source_page'] = f"Page {p_num}"

                if filtered_data and p_num % 10 == 0:
                    _save_checkpoint(ctx.db_path, ctx.tags, p_num, filtered_data[0]['id'])

                if filtered_data and _is_page_fully_downloaded(ctx, ctx.args.sitename, getattr(ctx.args, 'gdl_db', None), filtered_data):
                    print(f"\n[!] Page {p_num} contains all previously downloaded posts. Aborting API fetch early.")
                    return None

                if filtered_data:
                    yield filtered_data # [NEW] Send batch straight to the downloader pipeline!

                if hit_limit:
                    print(f"\n[✓] Reached End-ID limit ({ctx.end_id}). Stopping.")
                    return None

                batch_has_data = True
                if filtered_data:
                    last_batch_min_id = filtered_data[-1].get('id')

            if not batch_has_data:
                if last_batch_min_id: next_start_id = f"b{last_batch_min_id}"
                break

            current_page += batch_size
            time.sleep(ctx.args.sleep)

            if current_page >= PAGINATION_LIMIT and last_batch_min_id:
                next_start_id = f"b{last_batch_min_id}"
                break

    return next_start_id

def _fetch_sequential_loop(ctx: FetchContext, start_id: str):
    """Handles the sequential ID-based fetching loop (Generator)."""
    current_id_param = start_id

    print("\n--- Fetching API Metadata (Sequential ID Mode) ---")

    while not SHUTDOWN_EVENT.is_set():
        clean_id = current_id_param.strip('b')
        print(f"Fetching posts before ID {clean_id}...", end="\r")

        data = _fetch_metadata_page(ctx, current_id_param)

        if not data or data == "410_GONE":
            print("\n[Info] No more posts found.")
            break

        hit_limit, filtered_data = _reached_id_limit(data, ctx.end_id)
        for p in filtered_data: p['_source_page'] = f"ID {current_id_param}"

        if filtered_data and _is_page_fully_downloaded(ctx, ctx.args.sitename, getattr(ctx.args, 'gdl_db', None), filtered_data):
            print(f"\n[!] Reached known previously downloaded posts. Aborting API fetch early.")
            break

        if filtered_data:
            yield filtered_data # [NEW] Yield batch immediately

        if hit_limit or not filtered_data:
            print(f"\n[✓] Reached End-ID limit ({ctx.end_id}). Stopping.")
            break

        last_id = filtered_data[-1].get('id')
        if not last_id: break
        current_id_param = f"b{last_id}"

        time.sleep(ctx.args.sleep)

def _fetch_all_posts_threaded(ctx: FetchContext, start_page: int, start_id: Optional[str]):
    """Orchestrates the hybrid fetching strategy as a seamless generator."""
    forced_threaded = False
    if start_id:
        if _probe_smart_resume(ctx, start_id):
            forced_threaded = True
            start_page = 1
            start_id = None

    use_sequential = ((start_page is not None and start_page >= PAGINATION_LIMIT) or (start_id is not None))

    if not use_sequential or forced_threaded:
        # yield from natively handles generators and catches their return values!
        next_start_id = yield from _fetch_threaded_loop(ctx, start_page)

        if next_start_id:
            use_sequential = True
            start_id = next_start_id
        else:
            use_sequential = False

    if use_sequential:
        if not start_id:
            start_id = "b999999999" # Safe fallback
        yield from _fetch_sequential_loop(ctx, start_id)

    print(f"\n[✓] Search pipeline completed.")

def _configure_download(args):
    """Parses arguments and sets up configuration for the run."""
    cookie_path = getattr(args, 'cookies', None)
    session = get_protected_session(cookie_path)
    solver = AntiBotSolver() if args.captcha else None

    proxy = getattr(args, 'proxy', None)
    if proxy:
        session.proxies = {'http': proxy, 'https': proxy}

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

    has_search_params = (
        args.query or
        args.tags or
        getattr(args, 'or_tags', None) or
        getattr(args, 'filter_tags', None) or
        getattr(args, 'unless_tags', None)
    )

    if not has_search_params:
        print("[Error] You must provide a URL, or at least one tag parameter (--tags, --or-tags, etc).")
        return None

    input_str = args.query or args.tags or ""
    tags, start_page, start_id, base_url = _parse_input_query(input_str, args.base_url)

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

    if getattr(args, 'resume', False):
        last_page = _get_last_checkpoint_page(db_path, tags)
        if last_page:
            print(f"[✓] Auto-Resume triggered: Jumping to Page {last_page} for '{tags}'")
            start_page = last_page
            start_id = None

    is_v1 = False
    api_client = None
    try:
        # Probe the new endpoint to see if it responds natively
        probe = session.get(f"{base_url}/api/v1/posts", params={"limit": 1}, timeout=5)
        if probe.status_code == 200:
            is_v1 = True
            api_client = V1PostsAPI(base_url, session=session)
            print(f"[✓] Auto-Detected V1 API endpoint on {base_url}")
    except Exception:
        pass

    ctx = FetchContext(
        session=session, args=args, solver=solver, tags=tags,
        base_url=base_url, end_page=end_page_limit, end_id=end_id_limit,
        db_path=db_path, is_v1=is_v1, api_client=api_client # Include new flags here
    )

    return ctx, start_page, start_id, sitename, root_output_path

# Pass worker_name explicitly in the arguments
def _page_worker(ctx: FetchContext, page_counter, root_output_path, sitename, worker_name):
    """An independent thread worker that handles an entire page from fetch to download."""
    if SHUTDOWN_EVENT.is_set(): return

    # ... [Keep the session, proxy, and database setup exactly the same] ...
    # (Copy your existing session/proxy setup here down to the try block)
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(pool_connections=1, pool_maxsize=1, max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    proxy = getattr(ctx.args, 'proxy', None)
    if proxy:
        if 'socks5h://' in proxy:
            auth_id = uuid.uuid4().hex[:8]
            clean_proxy = proxy.replace('socks5h://', '')
            iso_proxy = f"socks5h://{auth_id}:circuit@{clean_proxy}"
            session.proxies = {'http': iso_proxy, 'https': iso_proxy}
        else:
            session.proxies = {'http': proxy, 'https': proxy}

    api_client = V1PostsAPI(ctx.base_url, session=session) if ctx.is_v1 else None
    thread_ctx = ctx._replace(session=session, api_client=api_client)

    local_conn = sqlite3.connect(thread_ctx.db_path, timeout=30)
    gdl_path = getattr(thread_ctx.args, 'gdl_db', None)
    gdl_conn = sqlite3.connect(gdl_path, timeout=30) if gdl_path else None
    db_ctx = DbContext(local_conn, gdl_conn, sitename, getattr(thread_ctx.args, 'global_dedup', False))

    try:
        while not SHUTDOWN_EVENT.is_set() and not END_OF_RESULTS_EVENT.is_set():
            page_num = next(page_counter)

            if thread_ctx.end_page and page_num > thread_ctx.end_page: break

            print(f"[{worker_name}] Fetching metadata for Page {page_num}...")
            posts = _fetch_metadata_page(thread_ctx, page_num)

            print(f"[{worker_name}] Fetching metadata for Page {page_num}...")

            # [FIXED] Retry the JSON fetch if Tor drops the connection
            max_meta_retries = 3
            posts = None
            for attempt in range(max_meta_retries):
                posts = _fetch_metadata_page(thread_ctx, page_num)
                if posts is not None:
                    break
                print(f"[{worker_name}] Network failed on Page {page_num}. Retrying (Attempt {attempt+2}/{max_meta_retries})...")
                time.sleep(3)

            # [FIXED] If it completely fails, skip the page, but DO NOT kill the whole script!
            if posts is None:
                print(f"\n[!] Skipping Page {page_num} after {max_meta_retries} API fetch failures.")
                continue

            # [FIXED] Only trigger the kill switch if the API successfully returns an empty array
            if posts == "410_GONE" or len(posts) == 0:
                if not END_OF_RESULTS_EVENT.is_set():
                    print(f"\n[Info] Page {page_num} returned 0 posts. Reached true end of results.")
                    END_OF_RESULTS_EVENT.set()
                break

            # --- [NEW] BULK FILTERING ---
            existing_ids = _get_existing_post_ids(thread_ctx, root_output_path, sitename, db_ctx, posts)

            # Check Early Abort limit (only if the whole page is duplicates and abort is enabled)
            if len(existing_ids) == len(posts):
                if getattr(ctx.args, 'abort', 10) > 0:
                    if not END_OF_RESULTS_EVENT.is_set():
                        print(f"\n[!] Page {page_num} is fully downloaded. Aborting search.")
                        END_OF_RESULTS_EVENT.set()
                    break

            if page_num % 10 == 0:
                _save_checkpoint(thread_ctx.db_path, thread_ctx.tags, page_num, posts[0].get('id'))

            cookies = requests.utils.dict_from_cookiejar(session.cookies)
            headers = dict(session.headers)

            # Pre-populate the Skip stat with our bulk findings
            stats = {"DL": 0, "Skip": len(existing_ids), "Err": 0}

            # Filter the list down to ONLY posts we do not own
            posts_to_download = [p for p in posts if str(p.get('id')) not in existing_ids]

            # Process only the missing files
            for idx, p in enumerate(posts_to_download):
                if SHUTDOWN_EVENT.is_set(): break

                task = DownloadTask(
                    post=p, args=thread_ctx.args, output_path=root_output_path,
                    gdl_db_path=gdl_path, sitename=sitename,
                    base_url=thread_ctx.base_url, search_query=thread_ctx.tags,
                    source_context=f"Page {page_num}",
                    cookies=cookies, headers=headers
                )

                try:
                    res = str(_download_file(task, db_ctx, session))
                    # Note: We still safely tally skips here just in case the file
                    # exists on disk but wasn't in the DB for some reason.
                    if "[Skip]" in res or "[Found]" in res or "Exists" in res:
                        stats["Skip"] += 1
                    elif "Error" in res or "Failed" in res:
                        stats["Err"] += 1
                    else:
                        stats["DL"] += 1
                except Exception as unhandled_err:
                    _log_error(task.output_path, f"Page {page_num}", p.get('id'), str(unhandled_err))
                    stats["Err"] += 1

                # [Sidecar logic remains the same]
                if getattr(thread_ctx.args, 'sidecar', False):
                    try:
                        ext = p.get('file_ext', 'jpg')
                        filename = thread_ctx.args.filename_fmt.format(
                            sitename=sitename, id=p.get('id', ''),
                            md5=p.get('md5', ''), ext=ext
                        )
                        safe_folder = "".join(x for x in thread_ctx.tags[:50] if x.isalnum() or x in " ._-").strip() or "misc"
                        target_dir = Path(root_output_path) / safe_folder
                        target_dir.mkdir(parents=True, exist_ok=True)

                        txt_path = (target_dir / filename).with_suffix('.txt')
                        tags_text = _construct_tag_string(p)
                        if tags_text: txt_path.write_text(tags_text, encoding='utf-8')
                    except Exception:
                        pass

                if (idx + 1) % 10 == 0 and not SHUTDOWN_EVENT.is_set():
                    print(f"[{worker_name}] Page {page_num} DL Progress: {idx + 1}/{len(posts_to_download)}")

            print(f"[{worker_name}] Page {page_num} Complete | DL: {stats['DL']} | Skip: {stats['Skip']} | Err: {stats['Err']}")

    finally:
        local_conn.close()
        if gdl_conn: gdl_conn.close()

def run(args):
    """Main entry point."""
    config = _configure_download(args)
    if not config: return
    ctx, start_page, start_id, sitename, root_output_path = config

    # ... [Keep your print statements for === Image Downloader ===] ...
#
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

    print("\n--- Starting Pipeline: Autonomous Thread Mode (Ctrl+C to stop) ---")

    # This creates an atomic, thread-safe counter.
    # Every time a thread calls next(page_counter), it gets a unique page number.
    page_counter = itertools.count(start_page if start_page else 1)

    executor = ThreadPoolExecutor(max_workers=args.threads)
    futures = []

    try:
        # Launch exactly ONE task per thread
        for i in range(args.threads):
            worker_name = f"Worker-{i+1:02d}"
            futures.append(executor.submit(_page_worker, ctx, page_counter, root_output_path, sitename, worker_name))

        for future in as_completed(futures):
            future.result()

    except KeyboardInterrupt:
        print("\n\n[!] SHUTDOWN TRIGGERED. Waiting for active downloads to finish...")
        SHUTDOWN_EVENT.set()
    finally:
        executor.shutdown(wait=True)
        try:
            with sqlite3.connect(ctx.db_path) as conn:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            print("[✓] WAL Checkpointed.")
        except Exception as e:
            print(f"[!] Failed to checkpoint WAL: {e}")
        print("[✓] Safe shutdown complete.")
