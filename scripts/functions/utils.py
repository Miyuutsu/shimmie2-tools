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

VIDEO_EXTS = {".gif", ".webm", ".mp4", ".flv", ".m4v", ".f4v", ".f4p", ".ogv"}

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

def get_source_score(url):
    """Returns the priority score for a given URL. Lower is better."""
    source_priority = {
        "pixiv.net": 1,
        "fantia.jp": 2,
        "tumblr.com": 3,
        "baraag.net": 4,   # Mastodon 1
        "misskey.io": 5,   # Mastodon 2
        "pawoo.net": 6,    # Mastodon 3
        "twitter.com": 7,
        "x.com": 7,
        "gelbooru.com": 8,
        "konachan.com": 9,
        "kemono.cr": 10,
        "danbooru.donmai.us": 11,
        "twimg.com": 12,
        "yande.re": 13
    }
    if not url:
        return 999

    url_lower = url.lower()
    for domain, score in source_priority.items():
        if domain in url_lower:
            return score

    return 100 # Default score for unknown but valid URLs

def convert_filename_to_source(filename):
    """
    Extracts canonical source URLs from standardized filename paths.
    """
    if not isinstance(filename, str):
        return None

    # --- Gelbooru Logic ---
    # Matches filename patterns
    # Example: 'gelbooru_123456_hash.jpg'
    gelbooru_match = re.search(r"gelbooru_(\d+)_", filename)
    if gelbooru_match:
        return f"https://gelbooru.com/index.php?page=post&s=view&id={gelbooru_match.group(1)}"

    # --- Konachan Logic ---
    # Matches filename patterns
    # Example: 'konachan_123456_hash.png'
    konachan_match = re.search(r"konachan_(\d+)_", filename)
    if konachan_match:
        return f"https://konachan.com/post/show/{konachan_match.group(1)}"

    # --- Kemono/Fanbox Logic ---
    # Matches file paths
    # Example: 'fanbox/user_id/post_id_optional_text.jpg'
    kemono_match = re.search(r"fanbox/(\d+)/(\d+)_", filename)
    if kemono_match:
        user_id = kemono_match.group(1)
        post_id = kemono_match.group(2)
        return f"https://kemono.cr/fanbox/user/{user_id}/post/{post_id}"

    # --- Yande.re Logic ---
    # Matches filename patterns
    # Example: 'yandere_123456_hash.jpg'
    yandere_match = re.search(r"yandere_(\d+)_", filename)
    if yandere_match:
        return f"https://yande.re/post/show/{yandere_match.group(1)}"

    return None

def convert_cdn_url(image_url):
    """
    Converts CDN links to more descriptive URLs. Currently supports Pixiv and Fantia.

    Args:
        image_url (str): The image URL.

    Returns:
        str: The converted URL.  If the URL is not a known CDN link, it is returned as is.

    Raises:
        TypeError: if input is not a string.
    """
    if not isinstance(image_url, str):
        raise TypeError(f"Input image URL must be a string. Received {type(image_url)}")

    # --- Pixiv Logic ---
    # Matches original or CDN links
    # Example: 'https://i.pximg.net/img-original/img/yyyy/mm/dd/00/00/00/id_p0.png'
    pixiv_pattern = (
        r"(?:i|img)\d{0,5}\.(?:pximg|pixiv)\.net/"
        r"(?:(?:img-original|img\d{1,5})/img/|img/)"
        r"(?:\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2}/)?"
        r"(?:[^/]+/)?"
        r"(\d+)"
        r"(?:_(?:[\w]+_)?p\d{1,3})?"
        r"\.(?:jpg|jpeg|png|webp)"
    )
    pixiv_match = re.search(pixiv_pattern, image_url)
    if pixiv_match:
        artwork_id = pixiv_match.group(1)
        return f"https://www.pixiv.net/en/artworks/{artwork_id}"

    # --- Fantia Logic ---
    # Matches post file uploads
    # Example: 'https://c.fantia.jp/uploads/post/file/id/main_image.jpg'
    fantia_match = re.search(r"c\.fantia\.jp/uploads/post/file/(\d+)/", image_url)
    if fantia_match:
        post_id = fantia_match.group(1)
        return f"https://fantia.jp/posts/{post_id}"

    # --- Tumblr Logic ---
    # Matches post URLs
    # Example: 'https://username.tumblr.com/post/id/slug'
    tumblr_match = re.search(r"([\w-]+)\.tumblr\.com/post/(\d+)", image_url)
    if tumblr_match:
        username = tumblr_match.group(1)
        post_id = tumblr_match.group(2)
        return f"https://{username}.tumblr.com/post/{post_id}"

    # --- Gelbooru Logic ---
    # Matches filenames
    # Example: 'gelbooru_id_hash.jpg'
    gelbooru_match = re.search(r"gelbooru_(\d+)_", image_url)
    if gelbooru_match:
        post_id = gelbooru_match.group(1)
        return f"https://gelbooru.com/index.php?page=post&s=view&id={post_id}"

    # --- Yande.re Logic ---
    # Matches file URLs
    # Example: 'https://files.yande.re/image/hash/yande.re/id/tags.jpg'
    yandere_match = re.search(r"files\.yande\.re/.*?/yande\.re(?:%20|\s|\+)(\d+)", image_url)
    if yandere_match:
        post_id = yandere_match.group(1)
        return f"https://yande.re/post/show/{post_id}"

    return image_url

def resolve_best_source(post_source, filename):
    """
    Evaluates both the metadata source and filename, returning the highest priority URL.
    """
    candidates = []

    if post_source:
        if isinstance(post_source, list):
            candidates.extend([convert_cdn_url(src) for src in post_source])
        else:
            candidates.append(convert_cdn_url(post_source))

    file_src = convert_filename_to_source(str(filename))
    if file_src:
        candidates.append(file_src)

    candidates = [c for c in candidates if c]

    if not candidates:
        return None

    candidates.sort(key=get_source_score)
    return candidates[0]

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

        is_video = image.suffix.lower() in VIDEO_EXTS

        if md5:
            cur.execute("SELECT * FROM posts WHERE md5 = ?", (md5,))
            row = cur.fetchone()
            if row:
                post = row_to_post_dict(row)
                cur.execute("SELECT pixel_hash FROM posts WHERE md5 = ?", (md5,))
                result = cur.fetchone()
                if result:
                    px_hash = result[0]
                else:
                    px_hash = md5 if is_video else compute_danbooru_pixel_hash(image)

        if not post:
            px_hash = md5 if is_video else compute_danbooru_pixel_hash(image)
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

def save_post_to_cache(res_data, rating_letter, tags: list[str], pure_source_link, cache):
    """For updating the cache"""
    with get_cache_conn(cache) as conn:
        cur = conn.cursor()

        parsed_tags = parse_tags(tags)
        new_data = (
            rating_letter,
            pure_source_link or "",
            parsed_tags[0],  # general
            parsed_tags[1],  # character
            parsed_tags[2],  # artist
            parsed_tags[3]   # series
        )

        # Fetch existing row, if any
        cur.execute("""
            SELECT rating, source, general, character, artist, series
            FROM posts WHERE md5 = ?""", (res_data.md5,))
        existing = cur.fetchone()

        # Only update if something differs
        if existing is None or existing != new_data:
            cur.execute("""
                INSERT OR REPLACE INTO posts
                (md5, pixel_hash, rating, source, general, character, artist, series)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (res_data.md5, res_data.px_hash, *new_data))
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

    if Path(src_path).suffix.lower() in VIDEO_EXTS:
        try:
            extract_video_thumbnail(src_path, dst_path)
        except subprocess.CalledProcessError as e:
            print(f"Error creating video thumbnail for {src_path}! ({e})")
    else:
        try:
            convert_to_webp(src_path, dst_path)
        except subprocess.CalledProcessError:
            try:
                fallback_to_webp(src_path, dst_path)
            except Exception as e: # pylint: disable=broad-exception-caught
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

def apply_tag_curation(tags):
    """
    In‑place fixing of tags that need messing.
    """
    prefixes = ('artist:', 'character:', 'series:', 'source:')
    master_merge_list = {
        "character:samurai_(7th_dragon_series)": "character:samurai_(7th_dragon)",
        "deep-blue_series": "series:deep-blue",
        "samurai_(7th_dragon)": "character:samurai_(7th_dragon)",
        "series:fate_(series)": "series:fate",
        "series:pokemon_(anime)": "series:pokemon",
        "series:pokemon_(classic_anime)": "series:pokemon",
        "series:pokemon_(game)": "series:pokemon",
        "series:pokemon_bw_(anime)": "series:pokemon_bw",
        "series:pokemon_dppt_(anime)": "series:pokemon_dppt",
        "series:pokemon_emerald": "series:pokemon_rse",
        "series:pokemon_rse_(anime)": "series:pokemon_rse",
        "series:pokemon_sm_(anime)": "series:pokemon_sm",
        "series:pokemon_xy_(anime)": "series:pokemon_xy",
        "series:x-men:_the_animated_series": "series:x-men",
        "x-men:_the_animated_series": "series:x-men",
        "x-men_film_series": "series:x-men"
        # You can keep adding your custom merges here
        # just make sure the last one has no comma
    }

    original_set = set(tags)
    step1_tags = []

    for tag in tags:
        if ':' not in tag:
            if any(f"{p}{tag}" in original_set for p in prefixes):
                continue
        step1_tags.append(tag)

    step2_tags = [master_merge_list.get(tag, tag) for tag in step1_tags]
    merged_set = set(step2_tags)
    step3_tags = []

    for tag in step2_tags:
        if ':' not in tag:
            if any(f"{p}{tag}" in merged_set for p in prefixes):
                continue
        step3_tags.append(tag)

    step3_set = set(step3_tags)
    step4_tags = []
    for tag in step3_tags:
        if tag.endswith("_(cosplay)"):
            base_name = tag[:-10]

            if f"character:{base_name}" in step3_set:
                step4_tags.append("cosplay")
                continue
        step4_tags.append(tag)

    tags[:] = [t for t in step4_tags if t != "tagme"]

def get_video_resolution(file_path: Path):
    """Extracts resolution from a video/gif using ffprobe."""
    try:
        cmd = [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=width,height", "-of", "csv=s=x:p=0",
            str(file_path)
        ]
        output = subprocess.check_output(cmd, text=True).strip()
        if output and 'x' in output:
            parts = output.split('\n', maxsplit=1)[0].split('x')
            return int(parts[0]), int(parts[1])
    except Exception as e: #pylint: disable=broad-exception-caught
        print(f"Error getting resolution for {file_path}: {e}")
    return None, None

def extract_video_thumbnail(src_path: Path, dst_path: Path):
    """Extracts the first frame of a video and saves it directly as a WebP thumbnail via ffmpeg."""
    src_path = Path(src_path)
    dst_path = Path(dst_path)
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    temp_webp_path = dst_path.with_name(f"{dst_path.name}.webp")

    cmd = [
        "ffmpeg", "-y", "-v", "error", "-i", str(src_path),
        "-ss", "00:00:00.000", "-vframes", "1",
        "-vf", "scale='if(gt(iw,ih),512,-1)':'if(gt(iw,ih),-1,512)'",
        "-c:v", "libwebp", "-lossless", "0", "-q:v", "92",
        str(temp_webp_path)
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    temp_webp_path.replace(dst_path)
