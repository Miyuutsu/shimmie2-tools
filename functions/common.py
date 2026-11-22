"""Shared constants, paths, hashing utilities, and blacklist parsing."""

import hashlib
import os
import re
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.resolve()
DB_DIR = ROOT_DIR / "database"
SUBMODULE_PATH = ROOT_DIR / "SD-Tag-Editor"

ADB_PATH = DB_DIR / "artists.db"
CACHE_PATH = DB_DIR / "posts_cache.db"
CDB_PATH = DB_DIR / "characters.db"
QUARANTINE_DB_PATH = DB_DIR / "ai_quarantine_cache.db"
TAGMAP_DB_PATH = DB_DIR / "tagmap.db"
TAG_DB_PATH = DB_DIR / "tag_rating_dominant.db"
TIMELINE_DB_PATH = DB_DIR / "timeline_index.db"
STAGING_STATE_DB_PATH = DB_DIR / "staging_state.db"

IMAGE_EXTS = {".jpg", ".jpeg", ".jpe", ".png", ".webp"}
VIDEO_EXTS = {".gif", ".webm", ".mp4", ".flv", ".m4v", ".f4v", ".f4p", ".ogv"}

MD5_REGEX = re.compile(r"[a-fA-F0-9]{32}")

_XATTR_MD5_KEY = "user.shimmie.md5"
_XATTR_DHASH_KEY = "user.shimmie.dhash"
_XATTR_SHA256_KEY = "user.shimmie.sha256"
_XATTR_META_KEY = "user.shimmie.meta"
_XATTR_META_VERSION = "v1"


@dataclass(slots=True)
class _XattrConfig:
    read: bool = False
    write: bool = False


_XATTR = _XattrConfig()


@dataclass(slots=True)
class BlacklistConfig:
    """Holds parsed blacklist rules, whitelist preservation tags, and dry-run ignore tags."""

    rules: list[dict[str, list[str] | str]] = field(default_factory=list)
    preserve_tags: set[str] = field(default_factory=set)
    ignore_tags: set[str] = field(default_factory=set)
    prefilter_tags: set[str] = field(default_factory=set)


def set_xattr_mode(read: bool = False, write: bool = False) -> None:
    """Configures process-wide filesystem xattr hash caching."""
    _XATTR.read = bool(read)
    _XATTR.write = bool(write)


def _read_xattr_digest(file_path: Path, key: str, expected_len: int, st: os.stat_result) -> str:
    """Reads and validates a cached hex digest from filesystem xattr against size and mtime_ns."""
    if not _XATTR.read or not hasattr(os, "getxattr"):
        return ""
    with suppress(OSError, UnicodeDecodeError, ValueError):
        raw = os.getxattr(file_path, key, follow_symlinks=True).decode("ascii")
        digest, size_s, mtime_s = raw.split(":", 2)
        if (
            len(digest) == expected_len
            and int(size_s) == st.st_size
            and int(mtime_s) == st.st_mtime_ns
        ):
            return digest
    return ""


def _write_xattr_digest(file_path: Path, key: str, digest: str, st: os.stat_result) -> None:
    """Writes a validated hex digest + size + mtime_ns to the target file's filesystem xattr."""
    if not _XATTR.write or not digest or not hasattr(os, "setxattr"):
        return
    payload = f"{digest}:{st.st_size}:{st.st_mtime_ns}".encode("ascii")
    with suppress(OSError):
        os.setxattr(file_path, key, payload, follow_symlinks=True)


def read_xattr_dhash(
    file_path: Path, st: os.stat_result | None = None
) -> tuple[int, int, int] | None:
    """Reads and validates a cached (width, height, 64-bit dHash) from filesystem xattr."""
    if not _XATTR.read or not hasattr(os, "getxattr"):
        return None
    stat_res = st
    if stat_res is None:
        with suppress(OSError):
            stat_res = file_path.stat()
    if stat_res is None:
        return None
    with suppress(OSError, UnicodeDecodeError, ValueError):
        raw = os.getxattr(file_path, _XATTR_DHASH_KEY, follow_symlinks=True).decode("ascii")
        hex_str, w_s, h_s, size_s, mtime_s = raw.split(":", 4)
        if (
            len(hex_str) == 16
            and int(size_s) == stat_res.st_size
            and int(mtime_s) == stat_res.st_mtime_ns
        ):
            w, h = int(w_s), int(h_s)
            if w > 0 and h > 0:
                return w, h, int(hex_str, 16)
    return None


def write_xattr_dhash(
    file_path: Path,
    width: int,
    height: int,
    dhash: int,
    st: os.stat_result | None = None,
) -> None:
    """Writes a validated (width, height, 64-bit dHash) + size + mtime_ns to filesystem xattr."""
    if not _XATTR.write or width <= 0 or height <= 0 or not hasattr(os, "setxattr"):
        return
    stat_res = st
    if stat_res is None:
        with suppress(OSError):
            stat_res = file_path.stat()
    if stat_res is None:
        return
    payload = (f"{dhash:016x}:{width}:{height}:{stat_res.st_size}:{stat_res.st_mtime_ns}").encode(
        "ascii"
    )
    with suppress(OSError):
        os.setxattr(file_path, _XATTR_DHASH_KEY, payload, follow_symlinks=True)


def read_xattr_media_meta(
    file_path: Path, st: os.stat_result | None = None
) -> tuple[int, int, str | None] | None:
    """Reads and validates cached (width, height, camera_exif) from filesystem xattr."""
    if not _XATTR.read or not hasattr(os, "getxattr"):
        return None
    stat_res = st
    if stat_res is None:
        with suppress(OSError):
            stat_res = file_path.stat()
    if stat_res is None:
        return None
    with suppress(OSError, UnicodeDecodeError, ValueError):
        raw = os.getxattr(file_path, _XATTR_META_KEY, follow_symlinks=True).decode("utf-8")
        ver, w_s, h_s, size_s, mtime_s, cam_s = raw.split(":", 5)
        if (
            ver == _XATTR_META_VERSION
            and int(size_s) == stat_res.st_size
            and int(mtime_s) == stat_res.st_mtime_ns
        ):
            w, h = int(w_s), int(h_s)
            if w > 0 and h > 0:
                return w, h, (cam_s or None)
    return None


def write_xattr_media_meta(
    file_path: Path,
    width: int,
    height: int,
    camera_info: str | None = None,
    st: os.stat_result | None = None,
) -> None:
    """Writes validated (width, height, camera_exif) + size + mtime_ns to filesystem xattr."""
    if not _XATTR.write or width <= 0 or height <= 0 or not hasattr(os, "setxattr"):
        return
    stat_res = st
    if stat_res is None:
        with suppress(OSError):
            stat_res = file_path.stat()
    if stat_res is None:
        return
    clean_cam = (
        (camera_info or "").replace("\x00", "").replace("\r", " ").replace("\n", " ").strip()
    )
    payload = (
        f"{_XATTR_META_VERSION}:{width}:{height}:{stat_res.st_size}:{stat_res.st_mtime_ns}:{clean_cam}"
    ).encode()
    with suppress(OSError):
        os.setxattr(file_path, _XATTR_META_KEY, payload, follow_symlinks=True)


def compute_hashes(
    file_path: Path,
    need_sha256: bool = False,
    st: os.stat_result | None = None,
) -> tuple[str, str]:
    """Computes MD5 (and optionally SHA-256) in one 1 MB-buffered pass with optional xattr caching."""
    stat_res = st
    if (_XATTR.read or _XATTR.write) and stat_res is None:
        with suppress(OSError):
            stat_res = file_path.stat()

    md5_hex = ""
    sha_hex = ""
    if _XATTR.read and stat_res is not None:
        md5_hex = _read_xattr_digest(file_path, _XATTR_MD5_KEY, 32, stat_res)
        if need_sha256:
            sha_hex = _read_xattr_digest(file_path, _XATTR_SHA256_KEY, 64, stat_res)
        if md5_hex and (not need_sha256 or sha_hex):
            return md5_hex, sha_hex

    calc_md5 = hashlib.md5() if not md5_hex else None
    calc_sha = hashlib.sha256() if (need_sha256 and not sha_hex) else None

    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(1048576), b""):
            if calc_md5 is not None:
                calc_md5.update(chunk)
            if calc_sha is not None:
                calc_sha.update(chunk)

    if calc_md5 is not None:
        md5_hex = calc_md5.hexdigest()
        if _XATTR.write and stat_res is not None:
            _write_xattr_digest(file_path, _XATTR_MD5_KEY, md5_hex, stat_res)

    if calc_sha is not None:
        sha_hex = calc_sha.hexdigest()
        if _XATTR.write and stat_res is not None:
            _write_xattr_digest(file_path, _XATTR_SHA256_KEY, sha_hex, stat_res)

    return md5_hex, sha_hex


def compute_md5(file_path: Path) -> str:
    """Calculates the MD5 hash of a file (using filesystem xattr cache if enabled)."""
    return compute_hashes(file_path, need_sha256=False)[0]


def extract_or_compute_md5(file_path: Path) -> str:
    """Extracts a 32-character hex MD5 from the filename stem if present, otherwise computes it."""
    if match := MD5_REGEX.search(file_path.stem):
        return match.group(0).lower()
    return compute_md5(file_path)


def load_blacklist(blacklist_path: Path) -> BlacklistConfig:
    """Parses a blacklist.txt file including #//whitelist: and #//ignore: directives."""
    if not blacklist_path.is_file():
        raise FileNotFoundError(f"Blacklist file not found: {blacklist_path.resolve()}")

    config = BlacklistConfig()

    for line in blacklist_path.read_text(encoding="utf-8").splitlines():
        raw_line = line.strip().lower()
        compact = raw_line.replace(" ", "")

        if compact.startswith("#//whitelist:"):
            whitelist_body = raw_line.split("whitelist:", 1)[1].split("#//", 1)[0]
            for tag in whitelist_body.split(","):
                if clean_tag := tag.strip():
                    config.preserve_tags.add(clean_tag)
            continue

        if compact.startswith("#//ignore:"):
            ignore_body = raw_line.split("ignore:", 1)[1].split("#//", 1)[0]
            for tag in ignore_body.split(","):
                if clean_tag := tag.strip():
                    config.ignore_tags.add(clean_tag)
            continue

        clean_line = raw_line.split("#//", 1)[0].strip()
        if not clean_line:
            continue

        pos_tags: list[str] = []
        neg_tags: list[str] = []
        for token in clean_line.split():
            if token.startswith("-"):
                if token[1:]:
                    neg_tags.append(token[1:])
            else:
                pos_tags.append(token)
                config.prefilter_tags.add(token)

        if not pos_tags:
            raise ValueError(
                f"Invalid blacklist rule '{clean_line}': must include at least one positive tag."
            )

        config.rules.append(
            {
                "pos": pos_tags,
                "neg": neg_tags,
                "raw": clean_line,
            }
        )

    return config


def expand_tags_for_blacklist(tags: Iterable[str]) -> set[str]:
    """Expands a tag iterable with unprefixed and un-qualified stems for blacklist evaluation."""
    clean_tags: set[str] = set()
    for raw_tag in tags:
        tag = raw_tag.lower()
        clean_tags.add(tag)
        if tag.startswith(("tagai:", "booru:")):
            continue
        base_tag = tag.split(":", 1)[1] if ":" in tag else tag
        clean_tags.add(base_tag)
        if "_(" in base_tag and base_tag.endswith(")"):
            clean_tags.add(base_tag.split("_(", 1)[0])
    return clean_tags
