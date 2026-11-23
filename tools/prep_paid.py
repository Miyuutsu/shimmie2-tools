"""Streaming, checkpointed paid-content scanner, timeline indexer, and thumbnail fallback stager."""

import os
import re
import shlex
import shutil
import sqlite3
import stat
import subprocess
import tarfile
import zipfile
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import orjson
import tqdm

from functions.common import (
    IMAGE_EXTS,
    ROOT_DIR,
    STAGING_STATE_DB_PATH,
    TIMELINE_DB_PATH,
    VIDEO_EXTS,
    compute_hashes,
)
from functions.db_cache import load_mappings

MEDIA_EXTS = IMAGE_EXTS | VIDEO_EXTS
ARCHIVE_EXTS = {
    ".zip",
    ".rar",
    ".7z",
    ".tar",
    ".gz",
    ".bz2",
    ".xz",
    ".tgz",
    ".tbz2",
    ".txz",
    ".cbz",
    ".cbr",
}
KNOWN_PLATFORMS = {"fanbox", "fantia", "patreon", "gumroad", "subscribestar", "boosty", "pixiv"}
_SEVEN_ZIP_BIN = shutil.which("7z") or shutil.which("7zz")

ARTIST_ID_SUFFIX_RE = re.compile(r"^(?P<name>.+?)(?:[\s_]+)\((?P<cid>\d+)\)$")
POST_DIR_ID_RE = re.compile(r"^(\d{4,12})(?:[_\s-]|$)")
FILENAME_POST_PAGE_RE = re.compile(r"^(\d{4,12})(?:[_-](?:p)?(\d{1,4}))?(?:[_\s.-]|$)")
LEADING_NUM_RE = re.compile(r"^(\d{1,5})(?:[_\s.-]|$)")
PAGE_TOKEN_RE = re.compile(r"(?:^|[_-])p?(\d{1,4})(?:[_.-]|$)", re.IGNORECASE)
NATURAL_SPLIT_RE = re.compile(r"(\d+)")


@dataclass(slots=True)
class ManifestSlot:
    """Represents a single expected media file parsed from metadata.json or post.json."""

    index: int
    stem: str
    ext: str
    sha256: str = ""
    item_id: str = ""
    is_cover: bool = False


@dataclass(slots=True)
class ParsedPostMeta:
    """Normalized metadata extracted from metadata.json or post.json."""

    platform: str = ""
    post_id: str = ""
    creator_id: str = ""
    public_id: str = ""
    artist_name: str = ""
    published_ts: float | None = None
    cover_stems: set[str] = field(default_factory=set)
    manifest: list[ManifestSlot] = field(default_factory=list)
    extra_anchors: list[tuple[str, int, float]] = field(default_factory=list)


@dataclass(slots=True)
class PostGroup:
    """Groups main files, thumbnail fallback files, archives, and JSON metadata for a single post."""

    artist_raw: str
    creator_id: str
    platform: str
    post_id: str
    post_dir: Path
    thumb_dir: Path | None = None
    json_path: Path | None = None
    main_files: list[Path] = field(default_factory=list)
    thumb_files: list[Path] = field(default_factory=list)
    archives: list[Path] = field(default_factory=list)
    meta: ParsedPostMeta = field(default_factory=ParsedPostMeta)


@dataclass(slots=True)
class StagedRecord:
    """Represents one staged media file recorded in timeline_index.db and staging_state.db."""

    md5: str
    original_path: str
    staged_path: str
    source_root: str
    staging_dir: str
    unit_path: str
    file_size: int
    mtime_ns: int
    artist: str
    platform: str
    post_id: str
    post_id_num: int
    published_ts: float
    date_source: str
    page_index: int
    source_url: str
    is_thumbnail: bool
    extra_tags: list[str]


def init_timeline_db(db_path: Path = TIMELINE_DB_PATH) -> None:
    """Initializes the shareable timeline_index.db (zero local filesystem paths)."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS platform_anchors (
                platform TEXT NOT NULL,
                post_id INTEGER NOT NULL,
                published_ts REAL NOT NULL,
                PRIMARY KEY (platform, post_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS timeline_items (
                md5 TEXT PRIMARY KEY,
                artist TEXT NOT NULL,
                platform TEXT NOT NULL,
                post_id TEXT NOT NULL,
                post_id_num INTEGER NOT NULL,
                published_ts REAL NOT NULL,
                date_source TEXT NOT NULL,
                page_index INTEGER NOT NULL,
                source_url TEXT NOT NULL,
                is_thumbnail INTEGER NOT NULL DEFAULT 0,
                extra_tags TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_timeline_sort "
            "ON timeline_items(artist, published_ts, post_id_num, page_index)"
        )


def init_staging_state_db(db_path: Path = STAGING_STATE_DB_PATH) -> None:
    """Initializes the local-only staging_state.db (paths, resume cache, cleanup tracking)."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS staging_batches (
                staging_dir TEXT PRIMARY KEY,
                source_root TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS completed_units (
                unit_path TEXT PRIMARY KEY,
                staging_dir TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                files_count INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS staged_files (
                staging_dir TEXT NOT NULL,
                original_path TEXT NOT NULL,
                staged_path TEXT NOT NULL,
                source_root TEXT NOT NULL,
                unit_path TEXT NOT NULL,
                md5 TEXT NOT NULL,
                file_size INTEGER NOT NULL DEFAULT 0,
                mtime_ns INTEGER NOT NULL DEFAULT 0,
                artist TEXT NOT NULL,
                source_url TEXT NOT NULL DEFAULT '',
                extra_tags TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (staging_dir, original_path)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS empty_posts (
                post_dir TEXT PRIMARY KEY,
                staging_dir TEXT NOT NULL,
                unit_path TEXT NOT NULL,
                artist TEXT NOT NULL,
                platform TEXT NOT NULL,
                post_id TEXT NOT NULL,
                status TEXT NOT NULL,
                expected_media INTEGER NOT NULL DEFAULT 0,
                archives_count INTEGER NOT NULL DEFAULT 0,
                source_url TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_empty_posts_staging ON empty_posts(staging_dir, status)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_staged_dir ON staged_files(staging_dir)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_staged_unit ON staged_files(unit_path)")


def _parse_timestamp(raw_val: Any) -> float | None:
    """Parses ISO-8601 or space-separated datetime strings into a UTC Unix timestamp."""
    if not raw_val or not isinstance(raw_val, str):
        return None
    val = raw_val.strip()
    if not val:
        return None
    if val.endswith("Z"):
        val = val[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(val)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(val, fmt)
                break
            except ValueError:
                continue
        else:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _clean_artist_slug(raw_name: str, artist_map: dict[str, str]) -> str:
    """Normalizes an artist folder/username and maps known aliases via artists.db."""
    cleaned = raw_name.strip()
    if match := ARTIST_ID_SUFFIX_RE.match(cleaned):
        cleaned = match.group("name").strip()
    slug = re.sub(r"\s+", "_", cleaned.lower()).strip("_")
    return artist_map.get(slug, slug)


def _extract_artist_and_cid(dir_name: str) -> tuple[str, str]:
    """Splits 'baabuu (15706595)' or 'FourierKaERu_(25107225)' into (name, creator_id)."""
    if match := ARTIST_ID_SUFFIX_RE.match(dir_name.strip()):
        return match.group("name").strip(), match.group("cid").strip()
    return dir_name.strip(), ""


def _detect_platform_from_path(path: Path) -> str:
    """Infers platform from ancestor directory names (e.g. fanbox, fantia, patreon)."""
    for part in reversed(path.parts):
        low = part.lower()
        if low in KNOWN_PLATFORMS:
            return low
    return ""


def _natural_sort_key(path: Path) -> list[int | str]:
    """Natural sort key so 2.jpg precedes 10.jpg."""
    return [int(tok) if tok.isdigit() else tok.lower() for tok in NATURAL_SPLIT_RE.split(path.name)]


def _parse_kemono_metadata(data: dict[str, Any]) -> ParsedPostMeta:
    """Parses gallery-dl Kemono/Pawchive metadata.json."""
    profile = data.get("user_profile") or {}
    platform = str(data.get("service") or data.get("subcategory") or "").strip().lower()
    post_id = str(data.get("id") or "").strip()
    creator_id = str(data.get("user") or profile.get("id") or "").strip()
    public_id = str(profile.get("public_id") or "").strip()
    artist_name = public_id or str(data.get("username") or profile.get("name") or "").strip()
    published_ts = _parse_timestamp(data.get("published") or data.get("date") or data.get("added"))

    manifest: list[ManifestSlot] = []
    raw_entries: list[dict[str, Any]] = []
    if isinstance(data.get("file"), dict) and (
        data["file"].get("name") or data["file"].get("path")
    ):
        raw_entries.append(data["file"])
    if isinstance(data.get("attachments"), list):
        raw_entries.extend(a for a in data["attachments"] if isinstance(a, dict))

    ext_aliases = {".jpe": ".jpg", ".jfif": ".jpg"}
    idx = 0
    for entry in raw_entries:
        raw_ext = str(entry.get("extension") or "").strip().lower().lstrip(".")
        raw_name = str(entry.get("name") or entry.get("filename") or "").strip()
        ext = f".{raw_ext}" if raw_ext else Path(raw_name).suffix.lower()
        ext = ext_aliases.get(ext, ext)
        if ext not in MEDIA_EXTS:
            path_ext = Path(str(entry.get("path") or "")).suffix.lower()
            path_ext = ext_aliases.get(path_ext, path_ext)
            if path_ext not in MEDIA_EXTS:
                continue
            ext = path_ext
        stem = str(entry.get("filename") or Path(raw_name).stem).strip().lower()
        sha256 = str(entry.get("hash") or "").strip().lower()
        manifest.append(
            ManifestSlot(
                index=idx,
                stem=stem,
                ext=ext,
                sha256=sha256,
                is_cover=(entry.get("type") == "file"),
            )
        )
        idx += 1

    return ParsedPostMeta(
        platform=platform,
        post_id=post_id,
        creator_id=creator_id,
        public_id=public_id,
        artist_name=artist_name,
        published_ts=published_ts,
        manifest=manifest,
    )


def _parse_fanbox_post_json(data: dict[str, Any]) -> ParsedPostMeta:
    """Parses gallery-dl direct Fanbox post.json."""
    user_obj = data.get("user") if isinstance(data.get("user"), dict) else {}
    platform = str(data.get("category") or "fanbox").strip().lower()
    post_id = str(data.get("id") or "").strip()
    creator_id = str(user_obj.get("userId") or "").strip()
    public_id = str(data.get("creatorId") or "").strip()
    artist_name = public_id or str(user_obj.get("name") or "").strip()
    published_ts = _parse_timestamp(data.get("publishedDatetime") or data.get("date"))

    cover_stems: set[str] = set()
    for cover_key in ("coverImageUrl", "imageForShare"):
        c_url = data.get(cover_key)
        if isinstance(c_url, str) and c_url.strip():
            cover_stems.add(Path(c_url.split("?", 1)[0]).stem.lower())

    extra_anchors: list[tuple[str, int, float]] = []
    for rel_key in ("prevPost", "nextPost"):
        rel = data.get(rel_key)
        if isinstance(rel, dict):
            rel_id = str(rel.get("id") or "").strip()
            rel_ts = _parse_timestamp(rel.get("publishedDatetime"))
            if rel_id.isdigit() and rel_ts is not None:
                extra_anchors.append((platform, int(rel_id), rel_ts))

    manifest: list[ManifestSlot] = []
    seen_ids: set[str] = set()

    def _add_slot(item_id: str, stem: str, ext_raw: str) -> None:
        ext = f".{ext_raw.strip().lower().lstrip('.')}" if ext_raw else ""
        if ext not in MEDIA_EXTS:
            return
        if item_id and item_id in seen_ids:
            return
        if item_id:
            seen_ids.add(item_id)
        manifest.append(
            ManifestSlot(
                index=len(manifest),
                stem=stem.strip().lower(),
                ext=ext,
                item_id=item_id.strip().lower(),
            )
        )

    article_body = data.get("articleBody") or data.get("body") or {}
    if isinstance(article_body, dict):
        img_map = article_body.get("imageMap") or {}
        file_map = article_body.get("fileMap") or {}

        for block in article_body.get("blocks") or ():
            if not isinstance(block, dict):
                continue
            if (img_id := block.get("imageId")) and img_id in img_map:
                info = img_map[img_id]
                _add_slot(str(img_id), str(img_id), str(info.get("extension") or ""))
            elif (file_id := block.get("fileId")) and file_id in file_map:
                info = file_map[file_id]
                _add_slot(
                    str(file_id),
                    str(info.get("name") or file_id),
                    str(info.get("extension") or ""),
                )

        for img_id, info in img_map.items():
            if isinstance(info, dict):
                _add_slot(str(img_id), str(img_id), str(info.get("extension") or ""))
        for file_id, info in file_map.items():
            if isinstance(info, dict):
                _add_slot(
                    str(file_id),
                    str(info.get("name") or file_id),
                    str(info.get("extension") or ""),
                )
        for img_item in article_body.get("images") or ():
            if isinstance(img_item, dict):
                iid = str(img_item.get("id") or "")
                _add_slot(iid, iid, str(img_item.get("extension") or ""))
        for file_item in article_body.get("files") or ():
            if isinstance(file_item, dict):
                fid = str(file_item.get("id") or "")
                _add_slot(
                    fid, str(file_item.get("name") or fid), str(file_item.get("extension") or "")
                )

    return ParsedPostMeta(
        platform=platform,
        post_id=post_id,
        creator_id=creator_id,
        public_id=public_id,
        artist_name=artist_name,
        published_ts=published_ts,
        cover_stems=cover_stems,
        manifest=manifest,
        extra_anchors=extra_anchors,
    )


def _load_json_meta(json_path: Path) -> ParsedPostMeta:
    """Dispatches metadata.json or post.json to the matching parser via orjson."""
    try:
        data = orjson.loads(json_path.read_bytes())
    except Exception:
        return ParsedPostMeta()
    if not isinstance(data, dict):
        return ParsedPostMeta()
    if "publishedDatetime" in data or "articleBody" in data or data.get("category") == "fanbox":
        return _parse_fanbox_post_json(data)
    return _parse_kemono_metadata(data)


def _discover_artist_units(root: Path, artist_override: str | None) -> list[Path]:
    """Lists top-level artist directories in O(1) scandir calls without walking the whole tree."""
    if artist_override:
        return [root]

    is_platform_root = root.name.lower() in KNOWN_PLATFORMS
    has_direct_content = False
    has_thumbs_child = False
    child_dirs: list[Path] = []

    with os.scandir(root) as it:
        for entry in it:
            if entry.name.startswith("."):
                continue
            if entry.is_dir(follow_symlinks=False):
                if entry.name.lower() == "thumbnails":
                    has_thumbs_child = True
                else:
                    child_dirs.append(Path(entry.path))
            elif entry.is_file(follow_symlinks=False):
                low = entry.name.lower()
                if low in {"metadata.json", "post.json"} or os.path.splitext(low)[1] in (
                    MEDIA_EXTS | ARCHIVE_EXTS
                ):
                    has_direct_content = True

    if not is_platform_root and (
        has_direct_content
        or has_thumbs_child
        or ARTIST_ID_SUFFIX_RE.match(root.name)
        or (child_dirs and all(POST_DIR_ID_RE.match(d.name) for d in child_dirs[:20]))
    ):
        return [root]

    # If root contains platform subfolders (e.g. pawchive/fanbox, pawchive/fantia), expand one level
    units: list[Path] = []
    for child in sorted(child_dirs, key=lambda p: p.name.lower()):
        if child.name.lower() in KNOWN_PLATFORMS:
            with os.scandir(child) as sub_it:
                sub_dirs = [
                    Path(e.path)
                    for e in sub_it
                    if not e.name.startswith(".") and e.is_dir(follow_symlinks=False)
                ]
            units.extend(sorted(sub_dirs, key=lambda p: p.name.lower()))
        else:
            units.append(child)
    return units


def _scan_post_dir(directory: Path) -> tuple[list[Path], list[Path], Path | None]:
    """Scans a single post directory via os.scandir (recursing only if nested subdirs exist)."""
    media: list[Path] = []
    archives: list[Path] = []
    json_path: Path | None = None
    stack = [directory]

    while stack:
        cur = stack.pop()
        try:
            with os.scandir(cur) as it:
                for entry in it:
                    if entry.name.startswith("."):
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        if entry.name.lower() != "thumbnails":
                            stack.append(Path(entry.path))
                        continue
                    if not entry.is_file(follow_symlinks=False):
                        continue
                    low = entry.name.lower()
                    if cur == directory and low in {"metadata.json", "post.json"}:
                        if json_path is None or low == "metadata.json":
                            json_path = Path(entry.path)
                        continue
                    suffix = os.path.splitext(low)[1]
                    if suffix in MEDIA_EXTS:
                        media.append(Path(entry.path))
                    elif suffix in ARCHIVE_EXTS:
                        archives.append(Path(entry.path))
        except OSError:
            continue

    media.sort(key=_natural_sort_key)
    archives.sort(key=lambda p: p.name.lower())
    return media, archives, json_path


def _scan_artist_unit(
    artist_dir: Path,
    default_platform: str,
    artist_override: str | None,
) -> list[PostGroup]:
    """Scans a single artist folder into PostGroups using fast os.scandir calls."""
    raw_artist, cid = _extract_artist_and_cid(artist_override or artist_dir.name)
    base_platform = _detect_platform_from_path(artist_dir) or default_platform

    post_subdirs: list[Path] = []
    flat_media: list[Path] = []
    thumb_map: dict[str, Path] = {}

    try:
        with os.scandir(artist_dir) as it:
            for entry in it:
                if entry.name.startswith("."):
                    continue
                if entry.is_dir(follow_symlinks=False):
                    if entry.name.lower() == "thumbnails":
                        try:
                            with os.scandir(entry.path) as t_it:
                                for t_entry in t_it:
                                    if t_entry.is_dir(follow_symlinks=False):
                                        thumb_map[t_entry.name] = Path(t_entry.path)
                        except OSError:
                            pass
                    else:
                        post_subdirs.append(Path(entry.path))
                elif (
                    entry.is_file(follow_symlinks=False)
                    and os.path.splitext(entry.name.lower())[1] in MEDIA_EXTS
                ):
                    flat_media.append(Path(entry.path))
    except OSError:
        return []

    post_subdirs.sort(key=lambda p: p.name.lower())
    groups: list[PostGroup] = []

    # 1. Structured post directories
    short_name = artist_dir.name[:22]
    scan_iter = (
        tqdm.tqdm(
            post_subdirs,
            desc=f"Scan  {short_name:<22}",
            unit="post",
            position=1,
            leave=False,
        )
        if len(post_subdirs) >= 5
        else post_subdirs
    )
    for p_dir in scan_iter:
        main_files, archives, json_path = _scan_post_dir(p_dir)
        t_dir = thumb_map.pop(p_dir.name, None)
        thumb_files: list[Path] = []
        if t_dir is not None:
            thumb_files, _, _ = _scan_post_dir(t_dir)

        if not main_files and not thumb_files and not json_path:
            continue

        meta = _load_json_meta(json_path) if json_path else ParsedPostMeta()
        post_id = meta.post_id
        if not post_id and (m := POST_DIR_ID_RE.match(p_dir.name)):
            post_id = m.group(1)

        groups.append(
            PostGroup(
                artist_raw=artist_override or meta.artist_name or raw_artist,
                creator_id=meta.creator_id or cid,
                platform=meta.platform or base_platform,
                post_id=post_id,
                post_dir=p_dir,
                thumb_dir=t_dir,
                json_path=json_path,
                main_files=main_files,
                thumb_files=thumb_files,
                archives=archives,
                meta=meta,
            )
        )

    # 2. Orphan thumbnail directories (no matching main post folder)
    for folder_name, t_dir in sorted(thumb_map.items()):
        thumb_files, _, _ = _scan_post_dir(t_dir)
        if not thumb_files:
            continue
        m = POST_DIR_ID_RE.match(folder_name)
        groups.append(
            PostGroup(
                artist_raw=artist_override or raw_artist,
                creator_id=cid,
                platform=base_platform,
                post_id=m.group(1) if m else "",
                post_dir=artist_dir / folder_name,
                thumb_dir=t_dir,
                thumb_files=thumb_files,
            )
        )

    # 3. Flat media directly inside artist_dir (e.g. Paid/Chr/4285616_02 - Juju.png)
    if flat_media:
        flat_media.sort(key=_natural_sort_key)
        by_post: dict[str, list[Path]] = defaultdict(list)
        for f in flat_media:
            m = FILENAME_POST_PAGE_RE.match(f.stem)
            pid = m.group(1) if m else f.stem
            by_post[pid].append(f)

        for pid, f_list in sorted(by_post.items()):
            groups.append(
                PostGroup(
                    artist_raw=artist_override or raw_artist,
                    creator_id=cid,
                    platform=base_platform,
                    post_id=pid if pid.isdigit() else "",
                    post_dir=artist_dir,
                    main_files=f_list,
                )
            )

    return groups


def _load_anchors(conn: sqlite3.Connection) -> dict[str, list[tuple[int, float]]]:
    """Loads sorted (post_id, published_ts) anchors grouped by platform."""
    by_platform: dict[str, list[tuple[int, float]]] = defaultdict(list)
    for plat, pid, ts in conn.execute(
        "SELECT platform, post_id, published_ts FROM platform_anchors ORDER BY platform, post_id"
    ):
        by_platform[plat].append((int(pid), float(ts)))
    return by_platform


def _upsert_group_anchors(
    conn: sqlite3.Connection,
    groups: list[PostGroup],
    anchors_by_platform: dict[str, list[tuple[int, float]]],
) -> None:
    """Upserts new anchors from an artist's groups and keeps in-memory anchor lists sorted."""
    new_anchors: list[tuple[str, int, float]] = []
    for g in groups:
        if g.platform and g.post_id.isdigit() and g.meta.published_ts is not None:
            new_anchors.append((g.platform, int(g.post_id), g.meta.published_ts))
        new_anchors.extend(g.meta.extra_anchors)

    if not new_anchors:
        return

    conn.executemany(
        """
        INSERT INTO platform_anchors (platform, post_id, published_ts)
        VALUES (?, ?, ?)
        ON CONFLICT(platform, post_id) DO UPDATE SET
            published_ts = excluded.published_ts
        """,
        new_anchors,
    )
    touched_platforms: set[str] = set()
    for plat, pid, ts in new_anchors:
        anchors_by_platform[plat].append((pid, ts))
        touched_platforms.add(plat)
    for plat in touched_platforms:
        dedup = dict(anchors_by_platform[plat])
        anchors_by_platform[plat] = sorted(dedup.items())


def _resolve_post_timestamp(
    group: PostGroup,
    anchors_by_platform: dict[str, list[tuple[int, float]]],
) -> tuple[float, str]:
    """Returns (utc_timestamp, date_source) using JSON exact date, anchor interpolation, or mtime."""
    if group.meta.published_ts is not None:
        return group.meta.published_ts, "json"

    if group.platform and group.post_id.isdigit():
        pid = int(group.post_id)
        anchors = anchors_by_platform.get(group.platform) or []
        if anchors:
            lo_idx, hi_idx = 0, len(anchors)
            while lo_idx < hi_idx:
                mid = (lo_idx + hi_idx) // 2
                if anchors[mid][0] < pid:
                    lo_idx = mid + 1
                else:
                    hi_idx = mid

            if lo_idx < len(anchors) and anchors[lo_idx][0] == pid:
                return anchors[lo_idx][1], "anchor_exact"

            if 0 < lo_idx < len(anchors):
                id_lo, ts_lo = anchors[lo_idx - 1]
                id_hi, ts_hi = anchors[lo_idx]
                ratio = (pid - id_lo) / (id_hi - id_lo)
                return ts_lo + (ts_hi - ts_lo) * ratio, "interpolated"

            nearest_id, nearest_ts = anchors[0] if lo_idx == 0 else anchors[-1]
            if len(anchors) >= 2:
                a_id, a_ts = anchors[0] if lo_idx == 0 else anchors[-2]
                b_id, b_ts = anchors[1] if lo_idx == 0 else anchors[-1]
                if b_id != a_id:
                    rate = (b_ts - a_ts) / (b_id - a_id)
                    est = nearest_ts + (pid - nearest_id) * rate
                    return max(
                        0.0, min(est, datetime.now(timezone.utc).timestamp())
                    ), "extrapolated"
            return nearest_ts, "anchor_nearest"

    sample_files = group.main_files or group.thumb_files
    if sample_files:
        return min(f.stat().st_mtime for f in sample_files), "mtime"
    return 0.0, "unknown"


def _build_source_url(group: PostGroup) -> str:
    """Constructs the highest-priority canonical source URL for a post group."""
    plat = group.platform
    pid = group.post_id
    if not plat or not pid:
        return ""
    if plat == "fanbox":
        if group.meta.public_id:
            return f"https://www.fanbox.cc/@{group.meta.public_id}/posts/{pid}"
        if group.creator_id:
            return f"https://pawchive.pw/fanbox/user/{group.creator_id}/post/{pid}"
        return ""
    if plat == "fantia":
        return f"https://fantia.jp/posts/{pid}"
    if plat == "patreon":
        return f"https://www.patreon.com/posts/{pid}"
    if plat == "pixiv":
        return f"https://www.pixiv.net/en/artworks/{pid}"
    if group.creator_id:
        return f"https://pawchive.pw/{plat}/user/{group.creator_id}/post/{pid}"
    return ""


def _inspect_with_7z(arc: Path) -> tuple[bool, bool]:
    """Lists .7z/.rar/.cbr entries via 7z/7zz without extracting or prompting for passwords."""
    if not _SEVEN_ZIP_BIN:
        return True, False
    try:
        proc = subprocess.run(
            [_SEVEN_ZIP_BIN, "l", "-slt", "-p-", "--", str(arc)],
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return True, False

    combined = f"{proc.stdout}\n{proc.stderr}".lower()
    if proc.returncode != 0:
        is_enc = "wrong password" in combined or "encrypted" in combined
        return True, is_enc

    cur_is_media = False
    has_media = False
    is_encrypted = False
    for line in proc.stdout.splitlines():
        if line.startswith("Path = "):
            entry_path = line[7:].strip().lower()
            ext = os.path.splitext(entry_path)[1]
            cur_is_media = ext in MEDIA_EXTS or ext in ARCHIVE_EXTS
            if cur_is_media:
                has_media = True
        elif line.startswith("Encrypted = +") and cur_is_media:
            is_encrypted = True

    return has_media, is_encrypted


def _archives_may_contain_media(archives: list[Path]) -> tuple[bool, bool]:
    """Returns (may_contain_media, has_encrypted). Peeks inside zip, tar, and (with 7z) 7z/rar."""
    if not archives:
        return False, False

    for arc in archives:
        suffix = arc.suffix.lower()

        if suffix in {".zip", ".cbz"}:
            try:
                with zipfile.ZipFile(arc) as zf:
                    for info in zf.infolist():
                        if info.is_dir():
                            continue
                        ext = os.path.splitext(info.filename.lower())[1]
                        if ext in MEDIA_EXTS or ext in ARCHIVE_EXTS:
                            return True, bool(info.flag_bits & 0x1)
            except (OSError, zipfile.BadZipFile):
                return True, False
            continue

        if suffix in {".tar", ".tgz", ".tbz2", ".txz"} or tarfile.is_tarfile(arc):
            try:
                with tarfile.open(arc, "r:*") as tf:
                    for member in tf.getmembers():
                        if not member.isfile():
                            continue
                        ext = os.path.splitext(member.name.lower())[1]
                        if ext in MEDIA_EXTS or ext in ARCHIVE_EXTS:
                            return True, False
            except (OSError, tarfile.TarError):
                return True, False
            continue

        if suffix in {".7z", ".rar", ".cbr"}:
            has_media, is_enc = _inspect_with_7z(arc)
            if has_media:
                return True, is_enc
            continue

        return True, False

    return False, False


def _dedup_files_by_md5(
    files: list[Path],
    hash_cache: dict[Path, tuple[str, str]],
) -> tuple[list[Path], list[tuple[Path, str]]]:
    """Deduplicates byte-identical files within a directory by MD5, caching results."""
    if len(files) <= 1:
        return files, []

    unique: list[Path] = []
    dups: list[tuple[Path, str]] = []
    seen: set[str] = set()
    for f in files:
        if f not in hash_cache:
            try:
                hash_cache[f] = compute_hashes(f, need_sha256=False)
            except OSError:
                continue
        md5 = hash_cache[f][0]
        if md5 in seen:
            dups.append((f, md5))
        else:
            seen.add(md5)
            unique.append(f)
    return unique, dups


def _resolve_thumbnails_with_safeguards(
    group: PostGroup,
    review_log: list[str],
    hash_cache: dict[Path, tuple[str, str]],
    promote_archive_thumbs: bool = False,
) -> tuple[list[tuple[Path, bool]], list[tuple[Path, str]]]:
    """Returns ([(file_path, is_thumbnail)], [(dup_thumb_path, md5)])."""
    selected: list[tuple[Path, bool]] = [(f, False) for f in group.main_files]
    if not group.thumb_files:
        return selected, []

    cover_stems = group.meta.cover_stems
    thumb_files = [t for t in group.thumb_files if t.stem.lower() not in cover_stems]
    if not thumb_files:
        return selected, []

    expected_count = len(group.meta.manifest)
    has_media_archives, has_encrypted = _archives_may_contain_media(group.archives)
    enc_note = " [encrypted]" if has_encrypted else ""

    if not group.main_files:
        if has_media_archives and not promote_archive_thumbs:
            review_log.append(
                f"[ARCHIVE_SHADOW]{enc_note} {group.post_dir} | 0 main media, "
                f"{len(group.archives)} archive(s), {len(thumb_files)} thumb(s) skipped "
                "(originals may be inside archive)"
            )
            return selected, []

        unique_thumbs, dup_thumbs = _dedup_files_by_md5(thumb_files, hash_cache)
        if expected_count > 0 and len(unique_thumbs) > expected_count + 1:
            review_log.append(
                f"[COUNT_EXCEEDS_MANIFEST] {group.post_dir} | 0 main media, "
                f"{len(unique_thumbs)} unique thumb(s) > {expected_count} expected in JSON"
            )
            return selected, []

        selected.extend((t, True) for t in unique_thumbs)
        return selected, dup_thumbs

    if expected_count > 0:
        attachment_hashes = {
            slot.sha256 for slot in group.meta.manifest if slot.sha256 and not slot.is_cover
        }
        all_manifest_hashes = {slot.sha256 for slot in group.meta.manifest if slot.sha256}
        required_hashes = attachment_hashes or all_manifest_hashes
        if required_hashes:
            main_hashes: set[str] = set()
            for f in group.main_files:
                if f not in hash_cache:
                    hash_cache[f] = compute_hashes(f, need_sha256=False)
                main_hashes.add(hash_cache[f][1])
            if required_hashes.issubset(main_hashes):
                return selected, []
        elif len(group.main_files) >= expected_count:
            return selected, []

    main_stems = {f.stem.lower() for f in group.main_files}
    thumb_stems = {t.stem.lower() for t in thumb_files}
    unmatched_thumbs = [t for t in thumb_files if t.stem.lower() not in main_stems]

    if not unmatched_thumbs:
        return selected, []

    if has_media_archives and not promote_archive_thumbs:
        review_log.append(
            f"[ARCHIVE_SHADOW]{enc_note} {group.post_dir} | {len(group.main_files)} main, "
            f"{len(group.archives)} archive(s), {len(unmatched_thumbs)} unmatched thumb(s) skipped"
        )
        return selected, []

    if main_stems.isdisjoint(thumb_stems):
        review_log.append(
            f"[ZERO_OVERLAP_MISMATCH] {group.post_dir} | {len(group.main_files)} main, "
            f"{len(thumb_files)} thumb(s), 0 matching stems (JSON expected={expected_count})"
        )
        return selected, []

    unique_unmatched, dup_thumbs = _dedup_files_by_md5(unmatched_thumbs, hash_cache)

    if expected_count > 0 and (len(group.main_files) + len(unique_unmatched)) > expected_count + 1:
        review_log.append(
            f"[COUNT_EXCEEDS_MANIFEST] {group.post_dir} | {len(group.main_files)} main + "
            f"{len(unique_unmatched)} unique unmatched thumb(s) > {expected_count} expected in JSON"
        )
        return selected, []

    if expected_count == 0 and len(unique_unmatched) > max(10, len(group.main_files) * 3):
        review_log.append(
            f"[THUMB_COUNT_EXPLOSION] {group.post_dir} | {len(group.main_files)} main vs "
            f"{len(unique_unmatched)} unique unmatched thumb(s) (no JSON to verify)"
        )
        return selected, []

    selected.extend((t, True) for t in unique_unmatched)
    return selected, dup_thumbs


def _resolve_page_index(
    file_path: Path,
    fallback_pos: int,
    group: PostGroup,
    file_sha256: str = "",
) -> int:
    """Determines page ordering within a post using JSON manifest slots, filename indices, or order."""
    stem_lower = file_path.stem.lower()
    if group.meta.manifest:
        for slot in group.meta.manifest:
            if file_sha256 and slot.sha256 and file_sha256 == slot.sha256:
                return slot.index
            if stem_lower == slot.stem or (slot.item_id and stem_lower == slot.item_id):
                return slot.index

    if (m := FILENAME_POST_PAGE_RE.match(file_path.stem)) and m.group(2):
        return int(m.group(2))
    if (m := LEADING_NUM_RE.match(file_path.stem)) and (
        not group.post_id or m.group(1) != group.post_id
    ):
        return int(m.group(1))
    if (m := PAGE_TOKEN_RE.search(file_path.stem)) and (
        not group.post_id or m.group(1) != group.post_id
    ):
        return int(m.group(1))
    return fallback_pos


def _link_or_copy(src: Path, dst: Path, link_mode: str) -> None:
    """Stages a single file into dst without modifying src."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() or dst.is_symlink():
        return
    if link_mode == "copy":
        shutil.copy2(src, dst)
    elif link_mode == "hardlink":
        try:
            os.link(src, dst)
        except OSError:
            os.symlink(src.resolve(), dst)
    else:
        os.symlink(src.resolve(), dst)


def _merge_sidecar_tags(media_path: Path, required_tags: list[str]) -> bool:
    """Appends missing metadata tags to an existing .txt sidecar (or creates one if absent)."""
    sidecar = media_path.with_suffix(".txt")
    existing_lines: list[str] = []
    existing_tags: set[str] = set()

    if sidecar.is_file():
        existing_lines = sidecar.read_text(encoding="utf-8").splitlines()
        for line in existing_lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            for tok in stripped.split(","):
                if clean := tok.strip().lower():
                    existing_tags.add(clean)

    missing = [t for t in required_tags if t and t.lower() not in existing_tags]
    if not missing and sidecar.is_file():
        return False

    existing_lines.append(", ".join(missing) if missing else "")
    sidecar.write_text("\n".join(line for line in existing_lines if line) + "\n", encoding="utf-8")
    return True


def _write_cleanup_wrapper(out_dir: Path) -> Path:
    """Writes a thin cleanup_staged.sh wrapper that delegates to shimmie_tool.py cleanup-paid."""
    out_dir.mkdir(parents=True, exist_ok=True)
    script_path = out_dir / "cleanup_staged.sh"
    tool_path = shlex.quote(str(ROOT_DIR / "shimmie_tool.py"))
    staging_quoted = shlex.quote(str(out_dir))
    wrapper = (
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f'exec {tool_path} cleanup-paid --staging-dir {staging_quoted} "$@"\n'
    )
    script_path.write_text(wrapper, encoding="utf-8")
    script_path.chmod(script_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return script_path


def _commit_artist_records(
    timeline_conn: sqlite3.Connection,
    state_conn: sqlite3.Connection,
    records: list[StagedRecord],
    empty_posts: list[tuple[str, str, str, str, str, str, str, int, int, str]],
    unit_path: str,
    staging_dir: str,
) -> None:
    """Commits portable rows to timeline_index.db and local paths/checkpoints to staging_state.db."""
    primary_records = [r for r in records if r.staged_path]
    if primary_records:
        timeline_conn.executemany(
            """
            INSERT INTO timeline_items (
                md5, artist, platform, post_id, post_id_num, published_ts,
                date_source, page_index, source_url, is_thumbnail, extra_tags
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(md5) DO UPDATE SET
                artist = excluded.artist,
                platform = excluded.platform,
                post_id = excluded.post_id,
                post_id_num = excluded.post_id_num,
                published_ts = CASE
                    WHEN excluded.date_source = 'json' THEN excluded.published_ts
                    ELSE timeline_items.published_ts
                END,
                date_source = CASE
                    WHEN excluded.date_source = 'json' THEN excluded.date_source
                    ELSE timeline_items.date_source
                END,
                page_index = excluded.page_index,
                source_url = COALESCE(NULLIF(excluded.source_url, ''), timeline_items.source_url),
                is_thumbnail = MIN(timeline_items.is_thumbnail, excluded.is_thumbnail),
                extra_tags = excluded.extra_tags
            """,
            [
                (
                    r.md5,
                    r.artist,
                    r.platform,
                    r.post_id,
                    r.post_id_num,
                    r.published_ts,
                    r.date_source,
                    r.page_index,
                    r.source_url,
                    int(r.is_thumbnail),
                    ",".join(r.extra_tags),
                )
                for r in primary_records
            ],
        )

    if records:
        state_conn.executemany(
            """
            INSERT INTO staged_files (
                staging_dir, original_path, staged_path, source_root, unit_path,
                md5, file_size, mtime_ns, artist, source_url, extra_tags
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(staging_dir, original_path) DO UPDATE SET
                staged_path = excluded.staged_path,
                source_root = excluded.source_root,
                unit_path = excluded.unit_path,
                md5 = excluded.md5,
                file_size = excluded.file_size,
                mtime_ns = excluded.mtime_ns,
                artist = excluded.artist,
                source_url = excluded.source_url,
                extra_tags = excluded.extra_tags
            """,
            [
                (
                    r.staging_dir,
                    r.original_path,
                    r.staged_path,
                    r.source_root,
                    r.unit_path,
                    r.md5,
                    r.file_size,
                    r.mtime_ns,
                    r.artist,
                    r.source_url,
                    ",".join(r.extra_tags),
                )
                for r in records
            ],
        )

    if empty_posts:
        state_conn.executemany(
            """
            INSERT INTO empty_posts (
                post_dir, staging_dir, unit_path, artist, platform, post_id,
                status, expected_media, archives_count, source_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(post_dir) DO UPDATE SET
                staging_dir = excluded.staging_dir,
                unit_path = excluded.unit_path,
                artist = excluded.artist,
                platform = excluded.platform,
                post_id = excluded.post_id,
                status = excluded.status,
                expected_media = excluded.expected_media,
                archives_count = excluded.archives_count,
                source_url = excluded.source_url
            """,
            empty_posts,
        )

    state_conn.execute(
        """
        INSERT INTO completed_units (unit_path, staging_dir, completed_at, files_count)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(unit_path) DO UPDATE SET
            staging_dir = excluded.staging_dir,
            completed_at = excluded.completed_at,
            files_count = excluded.files_count
        """,
        (unit_path, staging_dir, datetime.now(timezone.utc).isoformat(), len(primary_records)),
    )
    timeline_conn.commit()
    state_conn.commit()


def _stamp_existing_staging_sidecars(
    out_dir: Path, state_db_path: Path = STAGING_STATE_DB_PATH
) -> None:
    """Merges staging_state.db metadata tags into surviving staged files after triage/auto-tag."""
    if not state_db_path.is_file():
        print(f"[ERROR] Staging state DB not found: {state_db_path}")
        return

    out_resolved = str(out_dir.resolve())
    stamped = 0
    with sqlite3.connect(state_db_path) as conn:
        cur = conn.execute(
            "SELECT staged_path, artist, source_url, extra_tags "
            "FROM staged_files WHERE staging_dir = ?",
            (out_resolved,),
        )
        while batch := cur.fetchmany(5000):
            for staged_str, artist, source_url, extra_csv in batch:
                if not staged_str:
                    continue
                staged_file = Path(staged_str)
                if not staged_file.is_file():
                    continue
                tags = [t.strip() for t in extra_csv.split(",") if t.strip()]
                if artist and f"artist:{artist}" not in tags:
                    tags.append(f"artist:{artist}")
                if source_url and f"source:{source_url}" not in tags:
                    tags.append(f"source:{source_url}")
                if _merge_sidecar_tags(staged_file, tags):
                    stamped += 1

    print(f"[✓] Stamped metadata tags onto {stamped:,} surviving sidecars in {out_resolved}")


def run_prep_paid(args) -> None:
    """Main CLI entry point for prep-paid (streaming per artist with SQLite checkpointing)."""
    out_dir = Path(args.out).resolve()
    init_timeline_db(TIMELINE_DB_PATH)
    init_staging_state_db(STAGING_STATE_DB_PATH)

    if getattr(args, "stamp_sidecars", False):
        _stamp_existing_staging_sidecars(out_dir, STAGING_STATE_DB_PATH)
        return

    if not getattr(args, "path", None):
        print("[ERROR] --path is required unless --stamp-sidecars is set.")
        return

    src_root = Path(args.path).resolve()
    if not src_root.is_dir():
        print(f"[ERROR] Source path not found: {src_root}")
        return

    mappings = load_mappings()
    default_platform = (getattr(args, "platform", "") or "").strip().lower()
    artist_override = getattr(args, "artist", None)
    thumb_tag = (getattr(args, "thumb_tag", "thumbnail_only") or "thumbnail_only").strip().lower()
    max_files = max(0, int(getattr(args, "max_files", 0) or 0))
    max_artists = max(0, int(getattr(args, "max_artists", 0) or 0))
    rescan = bool(getattr(args, "rescan", False))

    base_pretags = [t.strip().lower() for t in (args.pretags or "").split(",") if t.strip()]
    if "paid_reward" not in base_pretags:
        base_pretags.insert(0, "paid_reward")

    print(f"[*] Listing artist units under {src_root}...")
    artist_units = _discover_artist_units(src_root, artist_override)
    if not artist_units:
        print("[!] No artist or post directories found.")
        return

    out_str = str(out_dir)
    src_str = str(src_root)
    review_path = out_dir / "needs_review.log"
    empty_log_path = out_dir / "empty_posts.log"

    total_staged = 0
    total_thumbs = 0
    total_covers = 0
    total_anomalies = 0
    total_dups = 0
    artists_done = 0
    artists_skipped = 0
    empty_counts: dict[str, int] = defaultdict(int)

    with (
        sqlite3.connect(TIMELINE_DB_PATH) as timeline_conn,
        sqlite3.connect(STAGING_STATE_DB_PATH) as state_conn,
    ):
        for c in (timeline_conn, state_conn):
            c.execute("PRAGMA journal_mode=WAL;")
            c.execute("PRAGMA synchronous=NORMAL;")

        anchors = _load_anchors(timeline_conn)

        completed_set: set[str] = set()
        if not rescan:
            completed_set = {
                row[0] for row in state_conn.execute("SELECT unit_path FROM completed_units")
            }

        pending_units = [u for u in artist_units if str(u) not in completed_set]
        artists_skipped = len(artist_units) - len(pending_units)

        print(
            f"[INFO] Found {len(artist_units):,} artist unit(s): "
            f"{len(pending_units):,} pending, {artists_skipped:,} already completed."
        )
        if not pending_units:
            print("[✓] All artist units are already indexed. Pass --rescan to re-check them.")
            return

        if not args.dry_run:
            state_conn.execute(
                """
                INSERT INTO staging_batches (staging_dir, source_root, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(staging_dir) DO UPDATE SET
                    source_root = excluded.source_root,
                    created_at = excluded.created_at
                """,
                (out_str, src_str, datetime.now(timezone.utc).isoformat()),
            )
            state_conn.commit()
            script_path = _write_cleanup_wrapper(out_dir)
            print(f"[INFO] Safe cleanup wrapper ready at {script_path}")

        target_artists = min(max_artists, len(pending_units)) if max_artists else len(pending_units)
        overall_pbar = (
            tqdm.tqdm(total=max_files, desc="Batch Files", unit="file", position=0)
            if max_files > 0
            else tqdm.tqdm(total=target_artists, desc="Artists", unit="artist", position=0)
        )

        try:
            for artist_dir in pending_units:
                if (max_files and total_staged >= max_files) or (
                    max_artists and artists_done >= max_artists
                ):
                    break

                unit_str = str(artist_dir)
                short_name = artist_dir.name[:22]
                if max_files > 0:
                    overall_pbar.set_postfix(
                        artists=f"{artists_done}/{len(pending_units)}",
                        artist=short_name,
                    )
                else:
                    overall_pbar.set_postfix(staged=total_staged, artist=short_name)

                groups = _scan_artist_unit(artist_dir, default_platform, artist_override)
                if not groups:
                    if not args.dry_run:
                        _commit_artist_records(timeline_conn, state_conn, [], [], unit_str, out_str)
                    artists_done += 1
                    if max_files == 0:
                        overall_pbar.update(1)
                    continue

                if not args.dry_run:
                    _upsert_group_anchors(timeline_conn, groups, anchors)

                cached_files = {
                    row[0]: (int(row[1]), int(row[2]), str(row[3]))
                    for row in state_conn.execute(
                        "SELECT original_path, file_size, mtime_ns, md5 "
                        "FROM staged_files WHERE unit_path = ?",
                        (unit_str,),
                    )
                }

                artist_records: list[StagedRecord] = []
                artist_review: list[str] = []
                artist_empty_posts: list[
                    tuple[str, str, str, str, str, str, str, int, int, str]
                ] = []
                artist_empty_lines: list[str] = []
                seen_md5: set[str] = set()
                artist_bytes_read = 0

                est_files = sum(
                    len(g.main_files) + (len(g.thumb_files) if not g.main_files else 0)
                    for g in groups
                )
                with tqdm.tqdm(
                    total=est_files or None,
                    desc=f"Stage {short_name:<22}",
                    unit="file",
                    position=1,
                    leave=False,
                ) as file_pbar:
                    for group in groups:
                        artist_slug = _clean_artist_slug(group.artist_raw, mappings.artist)
                        pub_ts, date_src = _resolve_post_timestamp(group, anchors)
                        source_url = _build_source_url(group)
                        plat_label = group.platform or "paid"
                        pid_num = int(group.post_id) if group.post_id.isdigit() else 0
                        pid_pad = f"{pid_num:010d}" if pid_num else "0000000000"

                        if not group.main_files and not group.thumb_files and group.json_path:
                            expected_media = len(group.meta.manifest)
                            if group.archives:
                                status = "ARCHIVE_ONLY"
                            elif expected_media > 0:
                                status = "MISSING_MEDIA"
                            else:
                                status = "TEXT_ONLY"
                            empty_counts[status] += 1
                            artist_empty_posts.append(
                                (
                                    str(group.post_dir),
                                    out_str,
                                    unit_str,
                                    artist_slug,
                                    plat_label,
                                    group.post_id,
                                    status,
                                    expected_media,
                                    len(group.archives),
                                    source_url,
                                )
                            )
                            if status != "TEXT_ONLY":
                                artist_empty_lines.append(
                                    f"[{status}] {group.post_dir} | expected_media={expected_media}, "
                                    f"archives={len(group.archives)}"
                                    + (f" | {source_url}" if source_url else "")
                                )
                            continue

                        hash_cache: dict[Path, tuple[str, str]] = {}
                        candidates, dup_thumbs = _resolve_thumbnails_with_safeguards(
                            group,
                            artist_review,
                            hash_cache,
                            promote_archive_thumbs=getattr(args, "promote_archive_thumbs", False),
                        )

                        for dup_path, dup_md5 in dup_thumbs:
                            try:
                                dup_st = dup_path.stat()
                            except OSError:
                                continue
                            total_dups += 1
                            artist_records.append(
                                StagedRecord(
                                    md5=dup_md5,
                                    original_path=str(dup_path),
                                    staged_path="",
                                    source_root=src_str,
                                    staging_dir=out_str,
                                    unit_path=unit_str,
                                    file_size=dup_st.st_size,
                                    mtime_ns=dup_st.st_mtime_ns,
                                    artist=artist_slug,
                                    platform=plat_label,
                                    post_id=group.post_id,
                                    post_id_num=pid_num,
                                    published_ts=pub_ts,
                                    date_source=date_src,
                                    page_index=0,
                                    source_url=source_url,
                                    is_thumbnail=True,
                                    extra_tags=[],
                                )
                            )

                        if not candidates:
                            continue

                        if file_pbar.total is not None:
                            promoted_extra = max(
                                0,
                                len(candidates) - (len(group.main_files) or len(group.thumb_files)),
                            )
                            if promoted_extra:
                                file_pbar.total += promoted_extra

                        manifest_stems = {s.stem for s in group.meta.manifest} | {
                            s.item_id for s in group.meta.manifest if s.item_id
                        }
                        has_manifest_hashes = any(s.sha256 for s in group.meta.manifest)

                        for idx, (file_path, is_thumb) in enumerate(candidates):
                            file_pbar.update(1)
                            if (
                                group.meta.cover_stems
                                and file_path.stem.lower() in group.meta.cover_stems
                            ):
                                total_covers += 1
                                continue

                            try:
                                st = file_path.stat()
                            except OSError:
                                continue
                            if st.st_size <= 0:
                                continue

                            orig_str = str(file_path)
                            need_sha = (
                                has_manifest_hashes
                                and not is_thumb
                                and len(candidates) > 1
                                and file_path.stem.lower() not in manifest_stems
                            )

                            if file_path in hash_cache:
                                md5, f_sha256 = hash_cache[file_path]
                            elif (
                                not need_sha
                                and (cached := cached_files.get(orig_str))
                                and cached[0] == st.st_size
                                and cached[1] == st.st_mtime_ns
                            ):
                                md5, f_sha256 = cached[2], ""
                            else:
                                md5, f_sha256 = compute_hashes(
                                    file_path, need_sha256=need_sha, st=st
                                )
                                artist_bytes_read += st.st_size
                                file_pbar.set_postfix(
                                    read=f"{artist_bytes_read / 1_048_576:.0f}MB",
                                    refresh=False,
                                )

                            if md5 in seen_md5:
                                total_dups += 1
                                artist_records.append(
                                    StagedRecord(
                                        md5=md5,
                                        original_path=orig_str,
                                        staged_path="",
                                        source_root=src_str,
                                        staging_dir=out_str,
                                        unit_path=unit_str,
                                        file_size=st.st_size,
                                        mtime_ns=st.st_mtime_ns,
                                        artist=artist_slug,
                                        platform=plat_label,
                                        post_id=group.post_id,
                                        post_id_num=pid_num,
                                        published_ts=pub_ts,
                                        date_source=date_src,
                                        page_index=idx,
                                        source_url=source_url,
                                        is_thumbnail=is_thumb,
                                        extra_tags=[],
                                    )
                                )
                                continue
                            seen_md5.add(md5)

                            page_idx = _resolve_page_index(
                                file_path, idx, group, file_sha256=f_sha256
                            )
                            staged_name = (
                                f"{plat_label}_{pid_pad}_p{page_idx:04d}_{md5[:8]}"
                                f"{file_path.suffix.lower()}"
                            )
                            staged_path = out_dir / artist_slug / staged_name

                            extra_tags = list(base_pretags)
                            if is_thumb and thumb_tag:
                                extra_tags.append(thumb_tag)

                            if not args.dry_run:
                                _link_or_copy(file_path, staged_path, args.link_mode)
                                if args.write_sidecars:
                                    sidecar_tags = [*extra_tags, f"artist:{artist_slug}"]
                                    if source_url:
                                        sidecar_tags.append(f"source:{source_url}")
                                    _merge_sidecar_tags(staged_path, sidecar_tags)

                            artist_records.append(
                                StagedRecord(
                                    md5=md5,
                                    original_path=orig_str,
                                    staged_path=str(staged_path),
                                    source_root=src_str,
                                    staging_dir=out_str,
                                    unit_path=unit_str,
                                    file_size=st.st_size,
                                    mtime_ns=st.st_mtime_ns,
                                    artist=artist_slug,
                                    platform=plat_label,
                                    post_id=group.post_id,
                                    post_id_num=pid_num,
                                    published_ts=pub_ts,
                                    date_source=date_src,
                                    page_index=page_idx,
                                    source_url=source_url,
                                    is_thumbnail=is_thumb,
                                    extra_tags=extra_tags,
                                )
                            )
                            total_staged += 1
                            if is_thumb:
                                total_thumbs += 1
                            if max_files > 0:
                                overall_pbar.update(1)
                            else:
                                overall_pbar.set_postfix(
                                    staged=total_staged,
                                    artist=short_name,
                                    refresh=False,
                                )

                if artist_review:
                    total_anomalies += len(artist_review)
                    out_dir.mkdir(parents=True, exist_ok=True)
                    with review_path.open("a", encoding="utf-8") as rf:
                        rf.write("\n".join(artist_review) + "\n")

                if artist_empty_lines:
                    out_dir.mkdir(parents=True, exist_ok=True)
                    with empty_log_path.open("a", encoding="utf-8") as ef:
                        ef.write("\n".join(artist_empty_lines) + "\n")

                if not args.dry_run:
                    _commit_artist_records(
                        timeline_conn,
                        state_conn,
                        artist_records,
                        artist_empty_posts,
                        unit_str,
                        out_str,
                    )
                artists_done += 1
                if max_files == 0:
                    overall_pbar.update(1)
        finally:
            overall_pbar.close()
            timeline_conn.commit()
            timeline_conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")

    mode_lbl = "DRY RUN" if args.dry_run else "STAGED"
    remaining = max(0, len(pending_units) - artists_done)
    print(
        f"\n[✓] prep-paid complete ({mode_lbl}): {artists_done:,} artist(s) processed "
        f"({remaining:,} remaining) | {total_staged:,} staged "
        f"({total_thumbs:,} thumbs, {total_dups:,} dup originals tracked, "
        f"{total_covers:,} covers skipped, {total_anomalies:,} anomalies)."
    )
    if empty_counts:
        summary = ", ".join(f"{k}={v:,}" for k, v in sorted(empty_counts.items()))
        print(f"[INFO] JSON-only posts (0 media/thumbs): {summary}")
        if empty_log_path.is_file():
            print(f"[INFO] Missing/archive-only post log: {empty_log_path}")
    if total_anomalies:
        print(f"[⚠️] Anomaly log: {review_path}")
