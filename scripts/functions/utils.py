"""
functions for shimmie2-tools
"""

import os
import sys
from collections import defaultdict, Counter
from contextlib import contextmanager
from pathlib import Path
import csv
import hashlib
import html
import io
import re
import sqlite3
import subprocess
import threading
import tqdm

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

def _check_shimmie_for_md5(md5, shimmie_path, dbuser):
    """Helper to check if MD5 exists in Shimmie via PHP subprocess."""
    try:
        cmd = [
            "php", str(Path(shimmie_path) / "index.php"),
            "-u", dbuser or "dbuser", "search", f"md5:{md5}"
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, check=False, cwd=shimmie_path)
        return md5 in res.stdout
    except Exception: # pylint: disable=broad-exception-caught
        print(f"Error checking Shimmie2 database! ({sys.exc_info()[0].__name__})")
        return "error"

def resolve_post(image: Path, shimmie_path, skip_existing, dbuser, cache) -> tuple:
    """Resolves the post information from the database or adds it to cache."""
    with get_cache_conn(cache) as conn:
        cur = conn.cursor()
        post = None

        match = re.compile(r"[a-fA-F0-9]{32}").search(image.stem)
        md5 = match.group(0).lower() if match else compute_md5(image)
        is_video = image.suffix.lower() in VIDEO_EXTS
        px_hash = None

        if md5:
            cur.execute("SELECT * FROM posts WHERE md5 = ?", (md5,))
            row = cur.fetchone()
            if row:
                post = row_to_post_dict(row)
                cur.execute("SELECT pixel_hash FROM posts WHERE md5 = ?", (md5,))
                px_res = cur.fetchone()
                px_hash = px_res[0] if px_res else None

        if not px_hash:
            px_hash = md5 if is_video else compute_danbooru_pixel_hash(image)

        if not post:
            cur.execute("SELECT * FROM posts WHERE pixel_hash = ?", (px_hash,))
            row = cur.fetchone()
            if row:
                post = row_to_post_dict(row)
            else:
                add_post_to_cache(md5, px_hash, cache)
                cur.execute("SELECT * FROM posts WHERE pixel_hash = ?", (px_hash,))
                post = row_to_post_dict(cur.fetchone())

        exists = False
        if skip_existing and shimmie_path:
            exists = _check_shimmie_for_md5(md5, shimmie_path, dbuser)

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

def apply_tag_curation(tags, dynamic_mappings=None):
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

    # Merge the mined data!
    if dynamic_mappings:
        master_merge_list.update(dynamic_mappings)

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

    tags[:] = [t for t in step4_tags if t not in ("tagme", "_DROP_")]

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

def _extract_hashes(image_list):
    """Helper to extract MD5s rapidly."""
    md5_regex = re.compile(r"[a-fA-F0-9]{32}")
    img_to_md5 = {}
    md5_set = set()

    for img_path in tqdm.tqdm(image_list, desc="1/3: Extracting Hashes", unit="img"):
        match = md5_regex.search(img_path.stem)
        md5 = match.group(0).lower() if match else compute_md5(img_path)
        img_to_md5[img_path] = md5
        md5_set.add(md5)

    return img_to_md5, md5_set

class TagCategoryGuard:
    """Helper to enforce category rules during tag mining."""
    def __init__(self, mappings):
        self.artists = set(mappings.artist.keys()) if mappings else set()
        self.chars = set(mappings.char.keys()) if mappings else set()
        self.series = set()
        if mappings:
            for val in mappings.char.values():
                if isinstance(val, (list, tuple, set)):
                    self.series.update(val)
                else:
                    self.series.add(val)
        self.strict = {'character', 'artist', 'series'}

    def get_category(self, tag):
        """Determines the category of a given tag."""
        if tag in self.chars:
            return 'character'
        if tag in self.artists:
            return 'artist'
        if tag in self.series:
            return 'series'
        return 'general'

    def shares_lexical_root(self, tag1, tag2):
        """Checks if two tags share a significant root word (useful for low confidence)."""
        w1 = {w for w in re.findall(r'[a-z0-9]+', re.sub(r'\([^)]+\)', '', tag1)) if len(w) > 2}
        w2 = {w for w in re.findall(r'[a-z0-9]+', re.sub(r'\([^)]+\)', '', tag2)) if len(w) > 2}
        return bool(w1 & w2)

    def check(self, s_tag, c_tag, s_count):
        """Returns True if the mapping is permitted."""
        cat_s = self.get_category(s_tag)
        cat_c = self.get_category(c_tag)

        # Block strict mismatch (e.g., character -> artist)
        if cat_s in self.strict and cat_c in self.strict and cat_s != cat_c:
            return False

        # Block strict -> general (Do not downgrade known artists/characters)
        if cat_s in self.strict and cat_c == 'general':
            return False

        if cat_s == 'character' and cat_c == 'character' and s_count < 50:
            b1 = re.sub(r'_\([^)]+\)$', '', s_tag)
            b2 = re.sub(r'_\([^)]+\)$', '', c_tag)
            if b1 != b2 and b1 not in b2 and b2 not in b1:
                return False

        return True

    def can_drop(self, s_tag, c_tag):
        """Returns True if it's safe to drop a tag for redundancy."""
        cat_s = self.get_category(s_tag)
        cat_c = self.get_category(c_tag)

        if cat_s in self.strict and cat_c in self.strict and cat_s != cat_c:
            return False

        if cat_s in self.strict and cat_c == 'general':
            return False

        return True

def _calculate_co_occurrences(image_list, img_to_md5, bulk_tags):
    """Helper to compute co-occurrences of sidecar vs canonical tags."""
    sidecar_counts = Counter()
    canonical_counts = Counter()
    co_occurrences = defaultdict(Counter)
    sidecar_overlap = defaultdict(Counter)
    valid_pairs = 0

    for img_path in tqdm.tqdm(image_list, desc="3/3: Mapping Co-occurrences", unit="img"):
        md5 = img_to_md5[img_path]
        canonical = bulk_tags.get(md5)

        if not canonical:
            continue

        valid_pairs += 1
        sidecars = set(get_sidecar_tags(img_path))

        for s_tag in sidecars:
            sidecar_counts[s_tag] += 1
            for c_tag in canonical:
                co_occurrences[s_tag][c_tag] += 1
                # Track if the canonical target is ALREADY in the sidecar organically
                if c_tag in sidecars:
                    sidecar_overlap[s_tag][c_tag] += 1

        for c_tag in canonical:
            canonical_counts[c_tag] += 1

    return valid_pairs, sidecar_counts, canonical_counts, co_occurrences, sidecar_overlap

def build_tag_frequencies(image_list, db_conn, sqlite_conn):
    """Scans image list, compares sidecar tags to DB canonical tags using bulk queries."""
    img_to_md5, md5_set = _extract_hashes(image_list)

    print(f"\n[INFO] Fetching database tags for {len(md5_set)} unique files...")
    bulk_tags = get_bulk_canonical_tags(md5_set, db_conn, sqlite_conn)

    return _calculate_co_occurrences(image_list, img_to_md5, bulk_tags)

def _fetch_global_context(tags_set, db_conn, chunk_size=1000):
    """Fetches total database counts, wiki existence, and deprecation status."""
    g_counts, deprecated, has_wiki = {}, set(), set()
    if not db_conn:
        return g_counts, deprecated, has_wiki

    env = os.environ.copy()
    if db_conn.get('password'):
        env['PGPASSWORD'] = db_conn['password']

    tags_set = list(tags_set)
    print("\n[INFO] Fetching global DB stats and wiki context...")

    # 1. Strip out comma-prefixed conversational phrases dynamically
    # 2. Check the remaining text against your strict deprecation patterns
    dep_sql = (
        r"REGEXP_REPLACE(body, ',\s*(do not use|ambiguous)', 'SAFE', 'ig') ~* '("
        r"deprecated tag\.|"
        r"ambiguous tag\. do not use\.|"
        r"ambiguous\. do not use\.|"
        r"do not use\. use|"
        r"do not use this tag\. instead|"
        r"do not use this tag\. use|"
        r"\. do not use this tag\.</p>|"
        r"; do not use this tag\.</p>|"
        r"<p>do not use this tag\.</p>|"
        r"\ndo not use this tag\.</p>|"
        r"^do not use this tag\.</p>"
        r")'"
    )

    for i in tqdm.tqdm(range(0, len(tags_set), chunk_size), leave=False):
        escaped = [t.replace("'", "''") for t in tags_set[i:i+chunk_size]]
        if not escaped:
            continue

        # 1. Fetch True Global Counts
        try:
            res = subprocess.run(
                [
                    "psql", "-d", db_conn['dbname'], "-U", db_conn['user'],
                    "-h", db_conn['host'], "-t", "-A", "-c",
                    "SELECT tag, count FROM tags WHERE tag IN ('"
                    + "', '".join(escaped) + "');"
                ],
                env=env, capture_output=True, text=True, check=True
            )
            for line in res.stdout.strip().split('\n'):
                if '|' in line:
                    parts = line.rsplit('|', 1)
                    g_counts[parts[0]] = int(parts[1])
        except Exception as e: # pylint: disable=broad-exception-caught
            print(f"\n[WARNING] Global counts query failed: {e}")

        # 2. Fetch Wiki Existence & Deprecation Status
        try:
            res = subprocess.run(
                [
                    "psql", "-d", db_conn['dbname'], "-U", db_conn['user'],
                    "-h", db_conn['host'], "-t", "-A", "-c",
                    "SELECT REPLACE(LOWER(title), ' ', '_'), "
                    f"CASE WHEN {dep_sql} THEN 1 ELSE 0 END "
                    "FROM wiki_pages "
                    "WHERE REPLACE(LOWER(title), ' ', '_') IN ('"
                    + "', '".join(escaped) + "');"
                ],
                env=env, capture_output=True, text=True, check=True
            )
            for line in res.stdout.strip().split('\n'):
                if '|' in line:
                    parts = line.rsplit('|', 1)
                    has_wiki.add(parts[0])
                    if parts[1] == '1':
                        deprecated.add(parts[0])
        except Exception as e: # pylint: disable=broad-exception-caught
            print(f"\n[WARNING] Wiki check query failed: {e}")

    return g_counts, deprecated, has_wiki

def mine_tag_equivalencies(image_list, conns, output_path, mappings, thresholds=(10, 0.5)):
    """Scans images to discover 1:1 tag mappings using Jaccard similarity."""
    print(f"\n[⛏️ MINING MODE] Analyzing {len(image_list)} images for 1:1 equivalencies...")

    db_conn, sqlite_conn = conns
    freqs = build_tag_frequencies(image_list, db_conn, sqlite_conn)

    missing = len(image_list) - freqs[0]
    if len(image_list) > 0 and (missing / len(image_list)) >= 0.5:
        print(f"\n[⚠️ ALERT] High Missing Rate: {missing}/{len(image_list)} images "
              f"({(missing/len(image_list))*100:.1f}%) were not found in the DB!")
    else:
        print(f"Successfully aligned {freqs[0]} images with database records.")

    # Fetch global context before doing the math
    global_ctx = _fetch_global_context(freqs[1].keys(), db_conn)

    guard = TagCategoryGuard(mappings)
    calculated = calculate_equivalencies(freqs, global_ctx, guard, thresholds)
    calculated.sort(key=lambda x: x["Sample_Size"], reverse=True)

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["Sidecar_Tag", "Canonical_Tag", "Confidence", "Sample_Size"]
        )
        writer.writeheader()
        writer.writerows(calculated)

    print(f"[✓] Mined {len(calculated)} highly confident equivalencies! Saved to {output_path}")

def calculate_equivalencies(freqs, global_ctx, guard, thresholds):
    """Calculates Jaccard similarity scores to map tags safely under local limits."""
    results = []

    for s_tag, s_count in freqs[1].items():
        if s_count < thresholds[0]:
            continue

        # GUARD 0: Wiki Deprecation Forced Drop
        if s_tag in global_ctx[1]:
            results.append({
                "Sidecar_Tag": s_tag,
                "Canonical_Tag": "_DROP_",
                "Confidence": 1.0,
                "Sample_Size": s_count
            })
            continue

        best_match, hi_score = None, 0
        for c_tag, shared in freqs[3][s_tag].items():
            union = s_count + freqs[2][c_tag] - shared
            if union > 0 and (shared / union) > hi_score:
                hi_score = shared / union
                best_match = c_tag

        if hi_score >= thresholds[1] and best_match != s_tag:

            # GUARD 1: Modifier exclusion
            if re.sub(r'_\([^)]+\)', '', s_tag) == re.sub(r'_\([^)]+\)', '', best_match):
                continue

            # GUARD 1.2: Parenthetical Context Protection
            # Prevents OC/Character tags from merging into their Artist/Series tags
            if f"({best_match})" in s_tag:
                continue

            # GUARD 1.5: Lexical Overlap Check for Low Confidence Mappings
            # (Requires a shared word like "daughter" if confidence is under 75%)
            if hi_score < 0.75 and not guard.shares_lexical_root(s_tag, best_match):
                continue

            # Pre-calculate native DB robustness to save local variables
            db_cnt = global_ctx[0].get(s_tag, freqs[2].get(s_tag, 0))

            # GUARD 2: Subset / Redundancy Check
            if (freqs[4][s_tag][best_match] / s_count) >= thresholds[1]:
                if not guard.can_drop(s_tag, best_match):
                    continue
                if db_cnt > (s_count * 0.5) or db_cnt > 500:
                    continue
                if s_tag in global_ctx[2]:
                    continue
                best_match = "_DROP_"

            # GUARD 3: General Category Mismatch & Strict Downgrades
            elif not guard.check(s_tag, best_match, s_count):
                continue

            # GUARD 4: Established Tag Protection
            # (Do not rename heavily used tags to other tags unless they are near-identical)
            if best_match != "_DROP_" and (db_cnt > 500 or db_cnt > (s_count * 0.5)):
                if hi_score < 0.95:
                    continue

            results.append({
                "Sidecar_Tag": s_tag,
                "Canonical_Tag": best_match,
                "Confidence": round(hi_score, 4),
                "Sample_Size": s_count
            })

    return results

def load_dynamic_mappings(csv_path):
    """Reads the mined tag map CSV and returns a dictionary of mappings."""
    dynamic_map = {}
    csv_file = Path(csv_path)

    if not csv_file.is_file():
        return dynamic_map

    with csv_file.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Enforce a strict confidence check just in case the CSV was hand-edited poorly
            if float(row.get("Confidence", 1.0)) >= 0.75:
                dynamic_map[row["Sidecar_Tag"]] = row["Canonical_Tag"]

    return dynamic_map

def get_shimmie_db_credentials(spath):
    """Parses Shimmie2 config.php to extract PostgreSQL credentials."""
    if not spath:
        return None

    config_path = Path(spath) / "data" / "config" / "shimmie.conf.php"

    if not config_path.is_file():
        print(f"[WARNING] Could not find Shimmie config at {spath}")
        return None

    content = config_path.read_text(encoding='utf-8')
    dsn_match = re.search(r"['\"]pgsql:([^'\"]+)['\"]", content)

    user_re_1 = r"\$database_user\s*=\s*['\"]([^'\"]+)['\"]"
    user_re_2 = r"define\s*\(\s*['\"]DATABASE_USER['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)"
    user_match = re.search(user_re_1, content) or re.search(user_re_2, content)

    pass_re_1 = r"\$database_pass\s*=\s*['\"]([^'\"]+)['\"]"
    pass_re_2 = r"define\s*\(\s*['\"]DATABASE_PASS['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)"
    pass_match = re.search(pass_re_1, content) or re.search(pass_re_2, content)

    if not dsn_match:
        return None

    dsn_parts = dict(part.split('=') for part in dsn_match.group(1).split(';') if '=' in part)
    return {
        "host": dsn_parts.get('host', 'localhost'),
        "dbname": dsn_parts.get('dbname', 'shimmie'),
        "user": user_match.group(1) if user_match else 'postgres',
        "password": pass_match.group(1) if pass_match else ''
    }

def _fetch_sqlite_tags(md5_list, sqlite_conn, results):
    """Helper to fetch tags from SQLite cache to reduce local variables."""
    if not sqlite_conn:
        return
    cur = sqlite_conn.cursor()
    for i in range(0, len(md5_list), 999):
        chunk = md5_list[i:i+999]
        placeholders = ','.join(['?'] * len(chunk))

        # Split the query string across two lines
        query = (
            "SELECT md5, general, character, artist, series "
            f"FROM posts WHERE md5 IN ({placeholders})"
        )

        cur.execute(query, chunk)
        for row in cur.fetchall():
            md5 = row[0]
            for field in row[1:]:
                if field:
                    results[md5].update([t.strip() for t in field.split(",") if t.strip()])

def _fetch_postgres_tags(md5_list, db_conn, chunk_size, results):
    """Helper to fetch tags from PostgreSQL to reduce local variables."""
    if not db_conn:
        return
    env = os.environ.copy()
    if db_conn.get('password'):
        env['PGPASSWORD'] = db_conn['password']

    missing = [m for m in md5_list if m not in results]

    for i in tqdm.tqdm(range(0, len(missing), chunk_size), desc="Querying Postgres", leave=False):
        chunk = missing[i:i+chunk_size]
        if not chunk:
            continue

        query = (
            "SELECT i.hash, t.tag FROM tags t "
            "JOIN image_tags it ON t.id = it.tag_id "
            "JOIN images i ON i.id = it.image_id "
            "WHERE i.hash IN ('" + "', '".join(chunk) + "');"
        )

        try:
            res = subprocess.run(
                [
                    "psql", "-d", db_conn['dbname'], "-U", db_conn['user'],
                    "-h", db_conn['host'], "-t", "-A", "-c", query
                ],
                env=env, capture_output=True, text=True, check=True
            )
            for line in res.stdout.strip().split('\n'):
                if '|' in line:
                    hsh, tag = line.split('|', 1)
                    results[hsh].add(tag.strip())
        except Exception as e: # pylint: disable=broad-exception-caught
            print(f"\n[WARNING] Bulk DB query failed for a chunk: {e}")

def get_bulk_canonical_tags(md5_set, db_conn, sqlite_conn, chunk_size=1000):
    """Fetches canonical tags for a large set of MD5s in bulk to avoid N+1 queries."""
    results = defaultdict(set)
    md5_list = list(md5_set)

    _fetch_sqlite_tags(md5_list, sqlite_conn, results)
    _fetch_postgres_tags(md5_list, db_conn, chunk_size, results)

    return results

def get_sidecar_tags(image_path):
    """Scans for .txt files associated with the image and parses tags."""
    extra_tags = []
    txt_candidates = [
        image_path.with_suffix(".txt"),
        image_path.with_name(image_path.name + ".txt")
    ]

    for txt_path in txt_candidates:
        if not txt_path.is_file():
            continue

        with txt_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = html.unescape(line.strip())
                if not line or line.startswith("#"):
                    continue
                parts = [t.strip() for t in re.split(r"[,;]", line) if t.strip()]
                extra_tags.extend(re.sub(r"\s+", "_", t) for t in parts if t)
    return extra_tags
