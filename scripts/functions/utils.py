"""
functions for shimmie2-tools
"""

import os
import sys
from contextlib import contextmanager
from pathlib import Path
import hashlib
import io
import re
import sqlite3
import subprocess
import threading

import pyvips
from PIL import Image

def rating_from_score(total_score: int, safe_max: int, questionable_max: int) -> str:
    """Map a numeric total to the rating letter."""
    if total_score <= safe_max:
        return "s"          # safe
    if total_score <= questionable_max:
        return "q"          # questionable
    return "e"              # explicit

def validate_float(value):
    """Validate if a number is a float between 0.00 and 1.00 and multiple of 0.01"""
    value = float(value)  # Ensure the value is a float
    if value < 0.00 or value > 1.00:
        raise ValueError(f"Value must be between 0.00 and 1.00. Given: {value}")
    if round(value*100) % 1 != 0:
        raise ValueError(f"Value must be a multiple of 0.01. Given: {value}")
    return value

def get_cpu_threads():
    """
    Determine CPU threads for multithreading
    """
    return os.cpu_count()

def add_module_path(relative_path: str):
    """
    Adds the given relative path to sys.path if it's not already present.

    Args:
        relative_path (str): The relative path from the current script to the module folder.
    """
    script_dir = Path(__file__).parent.resolve()
    module_path = (script_dir / relative_path).resolve()
    if not module_path.exists():
        raise FileNotFoundError(f"Module path does not exist: {module_path}")
    if str(module_path) not in sys.path:
        sys.path.append(str(module_path))

def convert_cdn_links(image_url):
    """
    Converts CDN links to more descriptive URLs. Currently supports Pixiv and Fantia.

    Args:
        image_url (str): The image URL.

    Returns:
        str: The converted URL.  If the URL is not a known CDN link, it is returned as it.

    Raises:
        TypeError: if input is not a string.
    """
    if not isinstance(image_url, str):
        raise TypeError(f"Input image URL must be a string. Received {type(image_url)}")

    # satisfy the linter
    pixiv_pattern = (
        r"(?:i|img)\d{0,5}\.(?:pximg|pixiv)\.net/"
        r"(?:(?:img-original|img\d{1,5})/img/|img/)"
        r"(?:\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2}/)?"
        r"(?:[^/]+/)?"
        r"(\d+)"
        r"(?:_(?:[\w]+_)?p\d{1,3})?"
        r"\.(?:jpg|jpeg|png|webp)"
    )

    fantia_pattern = r"c\.fantia\.jp/uploads/post/file/(\d+)/"
    pixiv_match = re.search(pixiv_pattern, image_url)
    if pixiv_match:
        artwork_id = pixiv_match.group(1)
        return f"https://www.pixiv.net/en/artworks/{artwork_id}"
    fantia_match = re.search(fantia_pattern, image_url)
    if fantia_match:
        post_id = fantia_match.group(1)
        return f"https://fantia.jp/posts/{post_id}"

    return image_url

def compute_md5(image_path: Path) -> str:
    """
    Calculates the MD5 hash of a file.

    Args:
        image_path (Path): Path to the image.

    Returns:
        str: md5 hash of the file
    """
    hash_md5 = hashlib.md5()
    with open(image_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    md5_hash = hash_md5.hexdigest()
    return md5_hash

@contextmanager
def get_cache_conn(cache):
    '''Use a connection cache'''
    thread_local = threading.local()
    conn = getattr(thread_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(cache, check_same_thread=False)
        thread_local.conn = conn
    yield conn

def resolve_post(image: Path, shimmie_path,
                 skip_existing, dbuser, cache)-> tuple[Path, dict | None]:
    """Resolves the post information from the database or adds it to cache."""
    with get_cache_conn(cache) as conn:
        cur = conn.cursor()
        post = None

        # Try MD5 from filename
        match = re.compile(r"[a-fA-F0-9]{32}").search(image.stem)
        md5 = match.group(0).lower() if match else compute_md5(image)

        if md5:
            cur.execute("SELECT * FROM posts WHERE md5 = ?", (md5,))
            row = cur.fetchone()
            if row:
                post = row_to_post_dict(row)
                cur.execute("SELECT pixel_hash FROM posts WHERE md5 = ?", (md5,))
                result = cur.fetchone()
                if result:
                    px_hash = result[0]  # Get pixel hash from the db
                else:
                    px_hash = compute_danbooru_pixel_hash(image)  # Calculate if needed

        if not post:
            px_hash = compute_danbooru_pixel_hash(image)
            cur.execute("SELECT * FROM posts WHERE pixel_hash = ?", (px_hash,))
            row = cur.fetchone()
            if row:
                post = row_to_post_dict(row)
            else:
                add_post_to_cache(md5, px_hash, cache)
                cur.execute("SELECT * FROM posts WHERE pixel_hash = ?", (px_hash,))
                row = cur.fetchone()
                post = row_to_post_dict(row)

        exists = False
        if skip_existing and shimmie_path:
            try:
                result = subprocess.run(
                    [
                        "php",
                        str(Path(shimmie_path) / "index.php"),
                        "-u",
                        dbuser or "dbuser",
                        "search",
                        f"md5:{md5}",
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                    cwd=shimmie_path,
                )
                exists = md5 in result.stdout

            except Exception:# pylint: disable=broad-exception-caught
                # Catch all non-system exceptions cleanly
                exists = "error"
                print(f"Error checking Shimmie2 database! ({sys.exc_info()[0].__name__})")

        return image, post, md5, px_hash, exists

def add_post_to_cache(md5, px_hash, cache):
    """For adding new images to cache"""
    with get_cache_conn(cache) as conn:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                md5 TEXT PRIMARY KEY,
                pixel_hash TEXT,
                rating TEXT,
                source TEXT,
                general TEXT,
                character TEXT,
                artist TEXT,
                series TEXT
            )
        """)

        cur.execute("""
            INSERT OR REPLACE INTO posts
            (md5, pixel_hash, rating, source, general, character, artist, series)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            md5, px_hash, "?", "",
            "", "", "", ""
        ))

        conn.commit()

def parse_tags(tags: list[str]) -> tuple[str, str, str, str, str]:
    """Parses tags"""
    tag_lists = {'general': [], 'character': [], 'artist': [], 'series': []}
    source_tag = None
    for tag in tags:
        prefix, _, value = tag.partition(':')
        if prefix in tag_lists:
            tag_lists[prefix].append(value)
        elif prefix == "source":
            source_tag = value
        else:
            tag_lists["general"].append(tag)
    return (
        ",".join(tag_lists["general"]) or "tagme",
        ",".join(tag_lists["character"]),
        ",".join(tag_lists["artist"]),
        ",".join(tag_lists["series"]),
        source_tag or ""
    )

def save_post_to_cache(rating_letter, tags: list[str], md5, px_hash, cache):
    """For updating the cache"""
    with get_cache_conn(cache) as conn:
        cur = conn.cursor()

        general, character, artist, series, source = parse_tags(tags)
        rating = rating_letter

        # Fetch existing row, if any
        cur.execute("""
            SELECT rating, source, general, character, artist, series
            FROM posts WHERE md5 = ?""", (md5,))
        existing = cur.fetchone()

        # Only update if something differs
        new_data = (rating, source, general, character, artist, series)
        if existing is None or any(existing[i] != new_data[i] for i in range(len(new_data))):
            cur.execute("""
                INSERT OR REPLACE INTO posts
                (md5, pixel_hash, rating, source, general, character, artist, series)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (md5, px_hash, *new_data))
        conn.commit()

def row_to_post_dict(row: tuple) -> dict:
    """This does something with the csv, I don't remember what right now"""
    def split_field(field):
        if not field:
            return []
        # split on comma, strip whitespace and ignore empty pieces
        return [part.strip() for part in field.split(",") if part.strip()]

    return {
        "md5": row[0],
        "pixel_hash": row[1],
        "rating": row[2],
        "source": row[3],
        "general": split_field(row[4]),
        "character": split_field(row[5]),
        "artist": split_field(row[6]),
        "series": split_field(row[7]),
    }

def compute_danbooru_pixel_hash(image_path: Path) -> str:
    """because the original database contained pixel hash I kept this"""
    image = pyvips.Image.new_from_file(str(image_path), access="sequential")

    # Match Danbooru's ICC transform and color space normalization
    if image.get_typeof("icc-profile-data") != 0:
        image = image.icc_transform("srgb")
    if image.interpretation != "srgb":
        image = image.colourspace("srgb")
    if not image.hasalpha():
        image = image.addalpha()

    # Write raw P7 header
    header = (
        b"P7\n"
        + f"WIDTH {image.width}\n".encode()
        + f"HEIGHT {image.height}\n".encode()
        + f"DEPTH {image.bands}\n".encode()
        + b"MAXVAL 255\n"
        + b"TUPLTYPE RGB_ALPHA\n"
        + b"ENDHDR\n"
    )

    # Get raw RGBA pixel bytes in memory
    raw_bytes = image.write_to_memory()

    # Concatenate and hash
    buffer = header + raw_bytes
    return hashlib.md5(buffer).hexdigest()

def process_webp(task):
    '''for some reason this was necessary'''
    src_path, dst_path = task
    try:
        convert_to_webp(src_path, dst_path)
    except subprocess.CalledProcessError:
        # ImageMagick failed — fallback to in-memory Pillow resize
        try:
            fallback_to_webp(src_path, dst_path)
        except Exception as e: # pylint: disable=broad-exception-caught
            # Catch all non-system exceptions cleanly
            print(f"Error creating thumbnail of {src_path}! ({type(e).__name__}: {e})")

def convert_to_webp(src_path: Path, dst_path: Path):
    """Convert images using ImageMagick."""
    res = "512x512>"
    ftype = "webp"
    src_path = Path(src_path)
    dst_path = Path(dst_path)
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "magick",
        str(src_path),
        "-resize", res,
        "-quality", "92",
        f"{ftype}:{dst_path}"
    ]

    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def fallback_to_webp(src_path: Path, dst_path: Path):
    """In-memory fallback using Pillow."""
    dst_path = Path(dst_path)
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    ftype = "webp"
    fbres = 512

    with open(src_path, "rb") as f:
        data = f.read()

    # Load via Pillow (lenient zlib)
    im = Image.open(io.BytesIO(data))
    im.load()  # force full decode in-memory

    # Resize while preserving aspect
    im.thumbnail((fbres, fbres), Image.Resampling.LANCZOS)

    # Save to WebP
    im.save(dst_path, ftype, quality=92, method=6)

def dedup_prefixed(tags,
                   prefixes=('artist', 'character', 'series', 'source')):
    """
    In‑place removal of plain tags that have a prefixed counterpart.
    """
    tag_set = set(tags)                 # fast look‑ups
    to_remove = set()

    for tag in tags:
        if ':' in tag:                   # already prefixed → skip
            continue
        plain = tag
        for pref in prefixes:
            if f"{pref}:{plain}" in tag_set:
                to_remove.add(plain)
                break                    # one match is enough

    # rewrite the original list (keeps external references valid)
    tags[:] = [t for t in tags if t not in to_remove]
