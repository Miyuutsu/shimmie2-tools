"""This is designed to help with batch importing into shimmie2"""
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from contextlib import contextmanager
from pathlib import Path
import argparse
import csv
import hashlib
import io
import re
import sqlite3
import subprocess
import threading
import tqdm

import pyvips
from PIL import Image
from functions.utils import get_cpu_threads, convert_cdn_links, compute_md5

RES = "512x512>"
FTYPE = "webp"
FBRES = 512

#####
#####
Image.MAX_IMAGE_PIXELS = None
ALLOWED_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".jxl", ".avif"}
MD5_RE = re.compile(r"[a-fA-F0-9]{32}")

script_dir = Path(__file__).parent.resolve()
db_dir = script_dir / ".." / "database"
cdb_path = db_dir / "characters.db"
db_path = db_dir / "tag_rating_dominant.db"
cache = db_dir / "posts_cache.db"
thread_local = threading.local()

@contextmanager
def get_cache_conn():
    '''Use a connection cache'''
    conn = getattr(thread_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(cache, check_same_thread=False)
        thread_local.conn = conn
    yield conn

def resolve_post(image: Path, shimmie_path, check_existing, dbuser) -> tuple[Path, dict | None]:
    """Resolves the post information from the database or adds it to cache."""
    with get_cache_conn() as conn:
        cur = conn.cursor()
        post = None

        # Try MD5 from filename
        match = MD5_RE.search(image.stem)
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
                add_post_to_cache(md5, px_hash)
                cur.execute("SELECT * FROM posts WHERE pixel_hash = ?", (px_hash,))
                row = cur.fetchone()
                post = row_to_post_dict(row)

        exists = False
        if check_existing and shimmie_path:
            try:
                cmd = [
                    "php",
                    str(Path(shimmie_path) / "index.php"),
                    "-u", dbuser or "dbuser",
                    "search", f"md5:{md5}"
                ]

                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=False,  # don't raise exceptions, handle manually
                    cwd=shimmie_path
                )

                if md5 in result.stdout:
                    exists = True

            except Exception as e:# pylint: disable=broad-exception-caught
                # Catch all non-system exceptions cleanly
                exists = "error"
                print(f"Error checking Shimmie2 database! ({type(e).__name__}: {e})")
                print(cmd)


        return image, post, md5, px_hash, exists

# used with resolve_post to ensure everything goes smoothly
def add_post_to_cache(md5, px_hash):
    """For adding new images to cache"""
    with get_cache_conn() as conn:
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

def save_post_to_cache(rating_letter, tags: list[str], md5, px_hash):
    """For updating the cache"""
    with get_cache_conn() as conn:
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
    src_path = Path(src_path)
    dst_path = Path(dst_path)
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "magick",
        str(src_path),
        "-resize", RES,
        "-quality", "92",
        f"{FTYPE}:{dst_path}"
    ]

    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def fallback_to_webp(src_path: Path, dst_path: Path):
    """In-memory fallback using Pillow."""
    dst_path = Path(dst_path)
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    with open(src_path, "rb") as f:
        data = f.read()

    # Load via Pillow (lenient zlib)
    im = Image.open(io.BytesIO(data))
    im.load()  # force full decode in-memory

    # Resize while preserving aspect
    im.thumbnail((FBRES, FBRES), Image.Resampling.LANCZOS)

    # Save to WebP
    im.save(dst_path, FTYPE, quality=92, method=6)

# run it
def main(args):
    """the main code"""
    print("=== Tagger Run Summary ===")
    print(f"📁  Input:           {args.image_path}")
    print(f"📥  Input Cache:     {cache}")
    print(f"🗄️  Update Cache:    {args.update_cache}")
    print(f"📦  Batch Size:      {args.batch}")
    print(f"🧵  Threads:         {args.threads}")
    print(f"📂  Prefix:          {args.prefix}")
    print()

    if not Path(cdb_path).is_file:
        raise FileNotFoundError(f"Character DB not found: {cdb_path}")

    if not Path(cache).is_file:
        raise FileNotFoundError(f"Cache not found: {cache}")

    if not Path(args.image_path).is_dir:
        raise FileNotFoundError(f"Path not found: {args.image_path}")

    if not Path(db_path).is_file:
        raise FileNotFoundError("Tag DB not found")

    character_series_map = {}
    with sqlite3.connect(cdb_path) as cdb_conn:
        cdb_cursor = cdb_conn.cursor()
        cdb_cursor.execute("SELECT * FROM data")
        rows = cdb_cursor.fetchall()
        for row in rows:
            if len(row) >= 2:
                char, series = row[0].strip(), row[1].strip()
                if char and series:
                    character_series_map[char] = series
        print(f"[INFO] Loaded {len(character_series_map):,} character→series mappings from SQLite.")

    tag_rating_map = {}
    with sqlite3.connect(db_path) as tag_db_conn:
        tag_db_cursor = tag_db_conn.cursor()
        tag_db_cursor.execute("SELECT * FROM dominant_tag_ratings")
        rows = tag_db_cursor.fetchall()
        for row in rows:
            if len(row) >= 2:
                tag_rating_map[row[0].strip()] = row[1].strip()

    files = Path(args.image_path).rglob("*")
    images = [
        f for f in files
        if f.suffix.lower() in ALLOWED_EXTS and f.is_file()
        and "thumbnails" not in f.relative_to(args.image_path).parts
    ]
    batches = [images[i:i + args.batch] for i in range(0, len(images), args.batch)]

    rating_priority = {'e': 5, 'q': 4, 's': 3, 'g': 2, '?': 1}
    existing_thumbs = set()
    csv_rows = []
    for batch in tqdm.tqdm(batches, desc="Image batches", position=1, leave=False):
        # === Multi-threaded post resolution ===
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            results = list(tqdm.tqdm(
                executor.map(
                    lambda img:
                        resolve_post(img, args.shimmie_path, args.check_existing, args.dbuser),
                        batch
                ),
                total=len(batch),
                desc="Resolving posts",
                position=2,
                leave=False
            ))

            for image, post, md5, px_hash, exists in results:
                if not exists:
                    rel_path = image.relative_to(args.image_path)
                    tags = []
                    tags.extend(post.get("general", []))
                    tags.extend(f"character:{t}" for t in post.get("character", []))
                    tags.extend(f"series:{t}" for t in post.get("series", []))
                    tags.extend(f"artist:{t}" for t in post.get("artist", []))

                    txt_path = image.with_suffix(".txt")
                    if txt_path.is_file():
                        with txt_path.open("r", encoding="utf-8") as f:
                            extra_tags = []
                            for line in f:
                                line = line.strip()
                                if not line or line.startswith("#"):
                                    continue
                                # Split comma-separated tags
                                for tag in line.split(","):
                                    tag = tag.strip()
                                    if tag:
                                        # Replace internal spaces with underscores
                                        tag = re.sub(r'\s+', '_', tag)
                                        extra_tags.append(tag)
                            tags.extend(extra_tags)

                    new_tags = []
                    for tag in tags:
                        new_tags.append(tag)
                        if tag in character_series_map:
                            inferred_series = character_series_map[tag]
                            new_tags.append(f"character:{tag}")

                            if isinstance(inferred_series, (list, tuple, set)):
                                for t in inferred_series:
                                    new_tags.append(f"series:{t}")
                            else:
                                new_tags.append(f"series:{inferred_series}")
                            new_tags = [t for t in new_tags if t != tag] # Removed tag

                    tags[:] = new_tags # at the end

                    rating_letter = post.get("rating", None)

                    for gen_tag in tags:
                        db_rating = tag_rating_map.get(gen_tag)
                        if db_rating:
                            if rating_letter is None:
                                rating_letter = db_rating
                            elif rating_priority[db_rating] > rating_priority[rating_letter]:
                                rating_letter = db_rating
                        if rating_letter == 'e':
                            break  # highest rating, no need to continue

                    if not rating_letter:
                        if "explicit" in post.get("rating", []):
                            rating_letter = "e"
                        elif "questionable" in post.get("rating", []):
                            rating_letter = "q"
                        elif "sensitive" in post.get("rating", []):
                            rating_letter = "q"
                        elif "general" in post.get("rating", []):
                            rating_letter = "s"
                        else:
                            rating_letter = "?"

                    if rating_letter == "g":
                        rating_letter = "s"

                    if post.get("source"):
                        source = post["source"]
                        source = convert_cdn_links(source)
                        tags.append(f"source:{source}")

                    tags = [re.sub(r'\s+', '_', tag.strip()) for tag in tags]
                    tags = sorted(set(tags))
                    tag_str = ", ".join(tags)

                    if args.update_cache:
                        save_post_to_cache(rating_letter, tags, md5, px_hash)

                    rel_path = image.relative_to(args.image_path)
                    if args.thumbnail:
                        thumbpath = Path(args.prefix) / "thumbnails" / rel_path

                    csv_rows.append([
                        f"{args.prefix}/{rel_path}",
                        tag_str,
                        "",
                        rating_letter,
                        str(thumbpath) if args.thumbnail else '""'
                    ])

                elif exists == "error":
                    print(f"{image} skipped due to error!")

                if exists and args.thumbnail:
                    rel_path = image.relative_to(args.image_path)
                    thumb_src = Path(args.image_path) / "thumbnails" / rel_path
                    existing_thumbs.add(str(thumb_src))

            if args.thumbnail:
                tasks = []
                for image in batch:
                    rel_path = image.relative_to(args.image_path)
                    thumb_src = Path(args.image_path) / "thumbnails" / rel_path
                    if str(thumb_src) not in existing_thumbs and not thumb_src.is_file():
                        tasks.append((image, thumb_src))
            if tasks:
                with ProcessPoolExecutor(max_workers=args.threads) as imgpro:
                    list(imgpro.map(process_webp, tasks))

    csv_path = Path(args.image_path)
    csv_path =  csv_path / "import.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerows(csv_rows)
    print(f"[✓] Shimmie CSV written to {csv_path}")
    print(f"\n[✓] Processed {len(images)} image(s) across {len(batches)} batch(es).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Creates a CSV suitable for input into Shimmie2.")
    parser.add_argument("--update-cache", action="store_true")
    parser.add_argument("--images", dest="image_path", default="", help="Path to images")
    parser.add_argument("--prefix", default="import",
                        help="What directory name will be used inside Shimmie directory.")
    parser.add_argument("--batch", type=int, default=20,
                        help="How many images should be processed simultaneously (default is 20)")
    parser.add_argument("--threads", type=int, default=get_cpu_threads() // 2,
                        help="Number of threads to use (default half)")
    parser.add_argument("--thumbnail", action="store_true")
    parser.add_argument("--shimmie-path", default="", help="Path to shimmie root")
    parser.add_argument("--check-existing", action="store_true",
                        help="Check if Shimmie already has image")
    parser.add_argument("--dbuser", default=None, help="Shimmie user for reading database")

    main(parser.parse_args())
