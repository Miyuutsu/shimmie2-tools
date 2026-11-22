"""Read-only paid-content scanner, timeline indexer, thumbnail fallback stager, and safe cleanup generator."""

import hashlib
import json
import os
import re
import shlex
import shutil
import sqlite3
import stat
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import tqdm

from functions.common import (
    IMAGE_EXTS,
    ROOT_DIR,
    TIMELINE_DB_PATH,
    VIDEO_EXTS,
    compute_md5,
)
from functions.db_cache import load_mappings

MEDIA_EXTS = IMAGE_EXTS | VIDEO_EXTS
ARCHIVE_EXTS = {".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".xz", ".cbz", ".cbr"}
KNOWN_PLATFORMS = {"fanbox", "fantia", "patreon", "gumroad", "subscribestar", "boosty", "pixiv"}

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
    """Represents one staged media file recorded in timeline_index.db and cleanup_manifest.json."""

    md5: str
    original_path: str
    staged_path: str
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
    """Creates timeline_items and platform_anchors tables in database/timeline_index.db."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
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
                extra_tags TEXT NOT NULL DEFAULT '',
                original_path TEXT NOT NULL DEFAULT '',
                staged_path TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_timeline_artist ON timeline_items(artist)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_timeline_sort ON timeline_items(artist, published_ts, post_id_num, page_index)"
        )


def _compute_sha256(file_path: Path) -> str:
    """Computes SHA-256 hex digest of a file from disk."""
    h = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


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

    idx = 0
    for entry in raw_entries:
        raw_ext = str(entry.get("extension") or "").strip().lower().lstrip(".")
        raw_name = str(entry.get("name") or entry.get("filename") or "").strip()
        ext = f".{raw_ext}" if raw_ext else Path(raw_name).suffix.lower()
        if ext not in MEDIA_EXTS:
            continue
        stem = str(entry.get("filename") or Path(raw_name).stem).strip().lower()
        sha256 = str(entry.get("hash") or "").strip().lower()
        manifest.append(ManifestSlot(index=idx, stem=stem, ext=ext, sha256=sha256))
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
    """Dispatches metadata.json or post.json to the matching parser."""
    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception:
        return ParsedPostMeta()
    if not isinstance(data, dict):
        return ParsedPostMeta()
    if "publishedDatetime" in data or "articleBody" in data or data.get("category") == "fanbox":
        return _parse_fanbox_post_json(data)
    return _parse_kemono_metadata(data)


def _discover_post_groups(
    root: Path,
    default_platform: str,
    artist_override: str | None,
) -> list[PostGroup]:
    """Walks root to pair post directories, flat artist files, JSON metadata, and sibling thumbnails/."""
    thumb_map: dict[tuple[Path, str], Path] = {}
    post_dirs: dict[Path, list[Path]] = {}
    flat_dirs: list[Path] = []

    for dirpath, dirnames, filenames in os.walk(root):
        cur_dir = Path(dirpath)
        dirnames[:] = sorted(d for d in dirnames if not d.startswith("."))

        # Register sibling thumbnail post directories under <artist>/thumbnails/<post_dir>
        if cur_dir.name.lower() == "thumbnails":
            artist_parent = cur_dir.parent
            for sub in list(dirnames):
                thumb_map[(artist_parent, sub)] = cur_dir / sub
            dirnames.clear()
            continue

        file_paths = [cur_dir / fn for fn in filenames]
        has_json = any(fn in {"metadata.json", "post.json"} for fn in filenames)
        has_media_or_arc = any(p.suffix.lower() in (MEDIA_EXTS | ARCHIVE_EXTS) for p in file_paths)
        if not has_json and not has_media_or_arc:
            continue

        if has_json or POST_DIR_ID_RE.match(cur_dir.name):
            post_dirs[cur_dir] = file_paths
        else:
            flat_dirs.append(cur_dir)

    groups: list[PostGroup] = []
    consumed_thumbs: set[tuple[Path, str]] = set()

    # 1. Structured post directories (with optional sibling thumbnails/<post_dir>)
    for p_dir, files in sorted(post_dirs.items()):
        artist_dir = p_dir.parent
        raw_artist, cid = _extract_artist_and_cid(artist_override or artist_dir.name)
        platform = _detect_platform_from_path(p_dir) or default_platform

        json_path = next(
            (p_dir / name for name in ("metadata.json", "post.json") if (p_dir / name).is_file()),
            None,
        )
        meta = _load_json_meta(json_path) if json_path else ParsedPostMeta()

        post_id = meta.post_id
        if not post_id and (m := POST_DIR_ID_RE.match(p_dir.name)):
            post_id = m.group(1)

        t_key = (artist_dir, p_dir.name)
        t_dir = thumb_map.get(t_key)
        thumb_files: list[Path] = []
        if t_dir and t_dir.is_dir():
            consumed_thumbs.add(t_key)
            thumb_files = sorted(
                (f for f in t_dir.iterdir() if f.is_file() and f.suffix.lower() in MEDIA_EXTS),
                key=_natural_sort_key,
            )

        main_files = sorted(
            (f for f in files if f.is_file() and f.suffix.lower() in MEDIA_EXTS),
            key=_natural_sort_key,
        )
        archives = sorted(f for f in files if f.is_file() and f.suffix.lower() in ARCHIVE_EXTS)

        groups.append(
            PostGroup(
                artist_raw=artist_override or meta.artist_name or raw_artist,
                creator_id=meta.creator_id or cid,
                platform=meta.platform or platform,
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

    # 2. Orphan thumbnail directories whose main post directory does not exist
    for t_key, t_dir in sorted(thumb_map.items()):
        if t_key in consumed_thumbs or not t_dir.is_dir():
            continue
        artist_dir, post_folder_name = t_key
        thumb_files = sorted(
            (f for f in t_dir.iterdir() if f.is_file() and f.suffix.lower() in MEDIA_EXTS),
            key=_natural_sort_key,
        )
        if not thumb_files:
            continue
        raw_artist, cid = _extract_artist_and_cid(artist_override or artist_dir.name)
        platform = _detect_platform_from_path(t_dir) or default_platform
        m = POST_DIR_ID_RE.match(post_folder_name)
        post_id = m.group(1) if m else ""
        groups.append(
            PostGroup(
                artist_raw=artist_override or raw_artist,
                creator_id=cid,
                platform=platform,
                post_id=post_id,
                post_dir=artist_dir / post_folder_name,
                thumb_dir=t_dir,
                thumb_files=thumb_files,
            )
        )

    # 3. Flat artist directories (e.g. Paid/Chr/4285616_02 - Juju.png)
    for f_dir in sorted(flat_dirs):
        raw_artist, cid = _extract_artist_and_cid(artist_override or f_dir.name)
        platform = _detect_platform_from_path(f_dir) or default_platform
        by_post: dict[str, list[Path]] = defaultdict(list)

        for f in sorted(f_dir.iterdir(), key=_natural_sort_key):
            if not f.is_file() or f.suffix.lower() not in MEDIA_EXTS:
                continue
            m = FILENAME_POST_PAGE_RE.match(f.stem)
            pid = m.group(1) if m else f.stem
            by_post[pid].append(f)

        for pid, f_list in sorted(by_post.items()):
            groups.append(
                PostGroup(
                    artist_raw=artist_override or raw_artist,
                    creator_id=cid,
                    platform=platform,
                    post_id=pid if pid.isdigit() else "",
                    post_dir=f_dir,
                    main_files=f_list,
                )
            )

    return groups


def _save_and_load_anchors(
    groups: list[PostGroup], db_path: Path = TIMELINE_DB_PATH
) -> dict[str, list[tuple[int, float]]]:
    """Upserts newly discovered (platform, post_id, published_ts) anchors and loads sorted tables."""
    new_anchors: list[tuple[str, int, float]] = []
    for g in groups:
        if g.platform and g.post_id.isdigit() and g.meta.published_ts is not None:
            new_anchors.append((g.platform, int(g.post_id), g.meta.published_ts))
        new_anchors.extend(g.meta.extra_anchors)

    with sqlite3.connect(db_path) as conn:
        if new_anchors:
            conn.executemany(
                """
                INSERT INTO platform_anchors (platform, post_id, published_ts)
                VALUES (?, ?, ?)
                ON CONFLICT(platform, post_id) DO UPDATE SET
                    published_ts = excluded.published_ts
                """,
                new_anchors,
            )
        by_platform: dict[str, list[tuple[int, float]]] = defaultdict(list)
        for plat, pid, ts in conn.execute(
            "SELECT platform, post_id, published_ts FROM platform_anchors ORDER BY platform, post_id"
        ):
            by_platform[plat].append((int(pid), float(ts)))
    return by_platform


def _resolve_post_timestamp(
    group: PostGroup,
    anchors_by_platform: dict[str, list[tuple[int, float]]],
) -> tuple[float, str]:
    """Returns (utc_timestamp, date_source) using JSON exact date, anchor interpolation, or file mtime."""
    if group.meta.published_ts is not None:
        return group.meta.published_ts, "json"

    if group.platform and group.post_id.isdigit():
        pid = int(group.post_id)
        anchors = anchors_by_platform.get(group.platform) or []
        if anchors:
            # Binary search nearest surrounding anchors
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

            # Outside anchor range: extrapolate gently using nearest boundary rate or anchor time
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


def _resolve_thumbnails_with_safeguards(
    group: PostGroup,
    review_log: list[str],
    promote_archive_thumbs: bool = False,
) -> list[tuple[Path, bool]]:
    """Returns [(file_path, is_thumbnail)] applying JSON reconciliation and anomaly safeguards."""
    selected: list[tuple[Path, bool]] = [(f, False) for f in group.main_files]
    if not group.thumb_files:
        return selected

    expected_count = len(group.meta.manifest)
    has_archives = bool(group.archives)

    # Case 1: No main media files exist at all
    if not group.main_files:
        if has_archives and not promote_archive_thumbs:
            review_log.append(
                f"[ARCHIVE_SHADOW] {group.post_dir} | 0 main media, {len(group.archives)} archive(s), "
                f"{len(group.thumb_files)} thumb(s) skipped (originals may be inside archive)"
            )
            return selected
        if expected_count > 0 and len(group.thumb_files) > expected_count:
            review_log.append(
                f"[COUNT_EXCEEDS_MANIFEST] {group.post_dir} | 0 main media, "
                f"{len(group.thumb_files)} thumb(s) > {expected_count} expected in JSON"
            )
            return selected
        selected.extend((t, True) for t in group.thumb_files)
        return selected

    # Case 2: Both main and thumbnail files exist
    # 2a. Check JSON manifest coverage (immune to local filename differences)
    if expected_count > 0:
        manifest_hashes = {slot.sha256 for slot in group.meta.manifest if slot.sha256}
        if manifest_hashes:
            main_hashes = {_compute_sha256(f) for f in group.main_files}
            if manifest_hashes.issubset(main_hashes):
                return selected
        elif len(group.main_files) >= expected_count:
            return selected

    main_stems = {f.stem.lower() for f in group.main_files}
    thumb_stems = {t.stem.lower() for t in group.thumb_files}
    unmatched_thumbs = [t for t in group.thumb_files if t.stem.lower() not in main_stems]

    if not unmatched_thumbs:
        return selected

    # Safeguard A: Archive shadow
    if has_archives and not promote_archive_thumbs:
        review_log.append(
            f"[ARCHIVE_SHADOW] {group.post_dir} | {len(group.main_files)} main, "
            f"{len(group.archives)} archive(s), {len(unmatched_thumbs)} unmatched thumb(s) skipped"
        )
        return selected

    # Safeguard B: Zero filename overlap when both main and thumbs have files
    if main_stems.isdisjoint(thumb_stems):
        review_log.append(
            f"[ZERO_OVERLAP_MISMATCH] {group.post_dir} | {len(group.main_files)} main, "
            f"{len(group.thumb_files)} thumb(s), 0 matching stems (JSON expected={expected_count})"
        )
        return selected

    # Safeguard C: Count explosion
    if expected_count > 0 and (len(group.main_files) + len(unmatched_thumbs)) > expected_count:
        review_log.append(
            f"[COUNT_EXCEEDS_MANIFEST] {group.post_dir} | {len(group.main_files)} main + "
            f"{len(unmatched_thumbs)} unmatched thumb(s) > {expected_count} expected in JSON"
        )
        return selected

    if expected_count == 0 and len(unmatched_thumbs) > max(10, len(group.main_files) * 3):
        review_log.append(
            f"[THUMB_COUNT_EXPLOSION] {group.post_dir} | {len(group.main_files)} main vs "
            f"{len(unmatched_thumbs)} unmatched thumb(s) (no JSON to verify)"
        )
        return selected

    selected.extend((t, True) for t in unmatched_thumbs)
    return selected


def _resolve_page_index(
    file_path: Path,
    fallback_pos: int,
    group: PostGroup,
    file_sha256: str | None = None,
) -> int:
    """Determines page ordering within a post using JSON manifest slots, filename indices, or natural order."""
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


def _write_cleanup_bundle(out_dir: Path, records: list[StagedRecord], source_root: Path) -> Path:
    """Writes cleanup_manifest.json and a thin cleanup_staged.sh wrapper calling shimmie_tool.py."""
    manifest_path = out_dir / "cleanup_manifest.json"
    script_path = out_dir / "cleanup_staged.sh"

    manifest_data = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_root": str(source_root),
        "staging_dir": str(out_dir),
        "items": [
            {
                "original_path": r.original_path,
                "staged_path": r.staged_path,
                "md5": r.md5,
                "is_thumbnail": r.is_thumbnail,
            }
            for r in records
        ],
    }
    manifest_path.write_text(json.dumps(manifest_data, indent=2), encoding="utf-8")

    tool_path = shlex.quote(str(ROOT_DIR / "shimmie_tool.py"))
    manifest_quoted = shlex.quote(str(manifest_path))
    wrapper = (
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f'exec {tool_path} cleanup-paid --manifest {manifest_quoted} "$@"\n'
    )
    script_path.write_text(wrapper, encoding="utf-8")
    script_path.chmod(script_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return script_path


def _stamp_existing_staging_sidecars(out_dir: Path, db_path: Path = TIMELINE_DB_PATH) -> None:
    """Merges timeline_index.db metadata tags into surviving staged files after triage/auto-tag."""
    if not db_path.is_file():
        print(f"[ERROR] Timeline DB not found: {db_path}")
        return

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT staged_path, artist, source_url, extra_tags FROM timeline_items WHERE staged_path != ''"
        ).fetchall()

    out_resolved = out_dir.resolve()
    stamped = 0
    for staged_str, artist, source_url, extra_csv in rows:
        staged_file = Path(staged_str)
        if not staged_file.is_file():
            continue
        if not Path(os.path.abspath(staged_file)).is_relative_to(out_resolved):
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
    """Main CLI entry point for prep-paid."""
    out_dir = Path(args.out).resolve()
    init_timeline_db(TIMELINE_DB_PATH)

    if getattr(args, "stamp_sidecars", False):
        _stamp_existing_staging_sidecars(out_dir, TIMELINE_DB_PATH)
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
    base_pretags = [t.strip().lower() for t in (args.pretags or "").split(",") if t.strip()]
    if "paid_reward" not in base_pretags:
        base_pretags.insert(0, "paid_reward")

    print(f"[*] Scanning {src_root} (read-only)...")
    groups = _discover_post_groups(src_root, default_platform, artist_override)
    if not groups:
        print("[!] No paid media posts discovered.")
        return

    anchors = _save_and_load_anchors(groups, TIMELINE_DB_PATH)
    print(
        f"[INFO] Found {len(groups):,} post groups; loaded anchors across "
        f"{', '.join(f'{k}={len(v):,}' for k, v in sorted(anchors.items())) or 'no platforms yet'}."
    )

    records: list[StagedRecord] = []
    review_log: list[str] = []
    seen_md5: set[str] = set()
    skipped_covers = 0

    for group in tqdm.tqdm(groups, desc="Staging & Indexing", unit="post"):
        artist_slug = _clean_artist_slug(group.artist_raw, mappings.artist)
        pub_ts, date_src = _resolve_post_timestamp(group, anchors)
        source_url = _build_source_url(group)
        candidates = _resolve_thumbnails_with_safeguards(
            group,
            review_log,
            promote_archive_thumbs=getattr(args, "promote_archive_thumbs", False),
        )
        if not candidates:
            continue

        has_manifest_hashes = any(s.sha256 for s in group.meta.manifest)
        plat_label = group.platform or "paid"
        pid_num = int(group.post_id) if group.post_id.isdigit() else 0
        pid_pad = f"{pid_num:010d}" if pid_num else "0000000000"

        for idx, (file_path, is_thumb) in enumerate(candidates):
            if group.meta.cover_stems and file_path.stem.lower() in group.meta.cover_stems:
                skipped_covers += 1
                continue

            md5 = compute_md5(file_path)
            if md5 in seen_md5:
                continue
            seen_md5.add(md5)

            f_sha256 = (
                _compute_sha256(file_path) if (has_manifest_hashes and not is_thumb) else None
            )
            page_idx = _resolve_page_index(file_path, idx, group, file_sha256=f_sha256)

            staged_name = (
                f"{plat_label}_{pid_pad}_p{page_idx:04d}_{md5[:8]}{file_path.suffix.lower()}"
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

            records.append(
                StagedRecord(
                    md5=md5,
                    original_path=str(file_path),
                    staged_path=str(staged_path),
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

    if not args.dry_run and records:
        with sqlite3.connect(TIMELINE_DB_PATH) as conn:
            conn.executemany(
                """
                INSERT INTO timeline_items (
                    md5, artist, platform, post_id, post_id_num, published_ts,
                    date_source, page_index, source_url, is_thumbnail,
                    extra_tags, original_path, staged_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    extra_tags = excluded.extra_tags,
                    original_path = excluded.original_path,
                    staged_path = excluded.staged_path
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
                        r.original_path,
                        r.staged_path,
                    )
                    for r in records
                ],
            )
        script_path = _write_cleanup_bundle(out_dir, records, src_root)
        print(f"[INFO] Safe cleanup helper written to {script_path}")

    if review_log:
        out_dir.mkdir(parents=True, exist_ok=True)
        review_path = out_dir / "needs_review.log"
        review_path.write_text("\n".join(review_log) + "\n", encoding="utf-8")
        print(f"[⚠️] Logged {len(review_log):,} thumbnail/archive anomalies to {review_path}")

    thumb_cnt = sum(1 for r in records if r.is_thumbnail)
    mode_lbl = "DRY RUN" if args.dry_run else "STAGED"
    print(
        f"\n[✓] prep-paid complete ({mode_lbl}): {len(records):,} media files "
        f"({thumb_cnt:,} promoted thumbnails, {skipped_covers:,} cover banners skipped)."
    )
