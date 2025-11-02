from concurrent.futures import ThreadPoolExecutor

from pathlib import Path
import argparse
import csv
import hashlib
import re
import sqlite3
import tqdm

import pyvips
from functions.utils import get_cpu_threads, convert_cdn_links, compute_md5
from PIL import Image

Image.MAX_IMAGE_PIXELS = None
ALLOWED_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".jxl", ".avif"}
MD5_RE = re.compile(r"[a-fA-F0-9]{32}")

script_dir = Path(__file__).parent.resolve()
db_dir = script_dir / ".." / "database"

def resolve_post(image: Path, cache: str) -> tuple[Path, dict | None]: # batch processing
    if Path(cache).is_file():
        with sqlite3.connect(cache) as conn:
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

            if not post:
                px_hash = compute_danbooru_pixel_hash(image)
                cur.execute("SELECT * FROM posts WHERE pixel_hash = ?", (px_hash,))
                row = cur.fetchone()
                if row:
                    post = row_to_post_dict(row)
                else:
                    add_post_to_cache(image, args.cache)
                    cur.execute("SELECT * FROM posts WHERE pixel_hash = ?", (px_hash,))
                    row = cur.fetchone()
                    post = row_to_post_dict(row)

            return image, post
    else:
        raise FileNotFoundError(
            "Warning: Database cache not found."
            "Database cache is considered mandatory due to speed and resource requirements."
            )

# used with resolve_post to ensure everything goes smoothly
def add_post_to_cache(image: Path, cache: Path):
    if not Path(cache).is_file():
        raise FileNotFoundError(f"Cannot save to cache: {cache} does not exist.")

    # Use MD5 from filename if possible
    match = MD5_RE.search(image.stem)
    md5 = match.group(0).lower() if match else compute_md5(image)

    # Compute fallback pixel hash
    px_hash = compute_danbooru_pixel_hash(image)

    # Extract and format each tag category
    general = ""
    rating = "?"
    source = ""
    character = ""
    artist = ""
    series = ""

    # Connect and insert
    with sqlite3.connect(cache) as conn:
        cur = conn.cursor()

        # Make sure your schema is ready
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
            md5, px_hash, rating, source,
            general, character, artist, series
        ))

        conn.commit()

# used with update-cache
def save_post_to_cache(image: Path, cache: Path, rating_letter, tags: list[str]):
    if not Path(cache).is_file():
        raise FileNotFoundError(f"Cannot save to cache: {cache} does not exist.")

    # Ensure tags is a proper list
    if isinstance(tags, str):
        tags = [tags]
    tags = [str(t).strip() for t in tags if t is not None]

    # Use MD5 from filename if possible
    match = MD5_RE.search(image.stem)
    md5 = match.group(0).lower() if match else compute_md5(image)

    # Compute fallback pixel hash
    px_hash = compute_danbooru_pixel_hash(image)

    general_tags, character_tags, artist_tags, series_tags = [], [], [], []
    source_tag = None

    for tag in tags:
        if tag.startswith("character:"):
            character_tags.append(tag.split(":", 1)[1])
        elif tag.startswith("series:"):
            series_tags.append(tag.split(":", 1)[1])
        elif tag.startswith("artist:"):
            artist_tags.append(tag.split(":", 1)[1])
        elif tag.startswith("source:"):
            source_tag = tag.split(":", 1)[1]
        else:
            general_tags.append(tag)

    # Always keep tag lists as lists (no accidental strings)
    def normalize_list(lst):
        return [str(x).strip() for x in lst if x is not None and str(x).strip()]

    general_tags = normalize_list(general_tags)
    character_tags = normalize_list(character_tags)
    artist_tags = normalize_list(artist_tags)
    series_tags = normalize_list(series_tags)

    general = ",".join(general_tags) or "tagme"
    character = ",".join(character_tags)
    artist = ",".join(artist_tags)
    series = ",".join(series_tags)
    source = source_tag or ""
    rating = rating_letter

    # Insert into DB
    with sqlite3.connect(cache) as conn:
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
        # Fetch existing row, if any
        cur.execute("SELECT rating, source, general, character, artist, series FROM posts WHERE md5 = ?", (md5,))
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

# I don't even remember what this does, something with the csv
def row_to_post_dict(row: tuple) -> dict:
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

# because the original database contained pixel hash I kept this
def compute_danbooru_pixel_hash(image_path: Path) -> str:
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

# run it
def main(args):
    print("=== Tagger Run Summary ===")
    print(f"📁  Input:           {args.image_path}")
    print(f"📥  Input Cache:     {args.cache}")
    print(f"🗄️  Update Cache:    {args.update_cache}")
    print(f"📦  Batch Size:      {args.batch}")
    print(f"🧵  Threads:         {args.threads}")
    print(f"📂  Prefix:          {args.prefix}")
    print()


    cdb_path = Path(args.cdb)
    cdb_conn = sqlite3.connect(cdb_path)
    cdb_cursor = cdb_conn.cursor()

    if not Path(args.image_path).is_dir:
        raise FileNotFoundError(f"Path not found: {args.image_path}")

    files = Path(args.image_path).rglob("*")

    images = [f for f in files if f.suffix.lower() in ALLOWED_EXTS and f.is_file()]

    # Step 1: Load cache if present
    if cdb_conn:
        print(f"[INFO] Using SQLite cache from {args.cache}...")

    csv_rows = []

    # Create batches using the --batch option
    batches = [images[i:i + args.batch] for i in range(0, len(images), args.batch)]

    character_series_map = {}

    if cdb_path.exists():
        cdb_cursor.execute("SELECT * FROM data")
        rows = cdb_cursor.fetchall()
        for row in rows:
            if len(row) >= 2:
                char, series = row[0].strip(), row[1].strip()
                if char and series:
                    character_series_map[char] = series
        print(f"[INFO] Loaded {len(character_series_map):,} character→series mappings from SQLite.")

    for batch in tqdm.tqdm(batches, desc="Tagging images", position=1, leave=False):

        # Preprocess and store md5s and images
        # === Multi-threaded post resolution ===
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            results = list(tqdm.tqdm(
                executor.map(lambda img: resolve_post(img, args.cache), batch),
                total=len(batch),
                desc="Resolving posts",
                position=2,
                leave=False
            ))

        rating_priority = {'e': 5, 'q': 4, 's': 3, 'g': 2, '?': 1}

        # Load tag rating DB once, if it exists
        tag_rating_map = {}
        db_path = db_dir / "tag_rating_dominant.db"
        if db_path.is_file():
            tag_db_conn = sqlite3.connect(db_path)
            tag_db_cursor = tag_db_conn.cursor()
            tag_db_cursor.execute("SELECT * FROM dominant_tag_ratings")
            rows = tag_db_cursor.fetchall()
            for row in rows:
                if len(row) >= 2:
                    tag_rating_map[row[0].strip()] = row[1].strip()
            tag_db_conn.close()

        for image, post in results:
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

            for char_tag in tags:
                if char_tag in character_series_map:
                    inferred_series = character_series_map[char_tag]
                    tags.append(f"character:{char_tag}")
                    if isinstance(inferred_series, (list, tuple, set)):
                        tags.extend(f"series:{t}" for t in inferred_series)
                    else:
                        tags.append(f"series:{inferred_series}")

            rating_letter = post.get("rating", None)

            for gen_tag in tags:
                db_rating = tag_rating_map.get(gen_tag)
                if db_rating:
                    # Pick highest rating per priority
                    if (rating_letter is None) or (rating_priority[db_rating] > rating_priority[rating_letter]):
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
                save_post_to_cache(image, args.cache, rating_letter, tags)

            rel_path = image.relative_to(args.image_path)
            csv_rows.append([
                f"{args.prefix}/{rel_path}",
                tag_str,
                "",
                rating_letter,
                ""
            ])

    if cdb_conn:
        cdb_conn.close()
    if csv_rows:
        csv_path = Path(args.image_path)
        csv_path =  csv_path / "import.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerows(csv_rows)
        print(f"[✓] Shimmie CSV written to {csv_path}")
    print(f"\n[✓] Processed {len(images)} image(s) across {len(batches)} batch(es).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Creates a CSV suitable for input into Shimmie2.")
    parser.add_argument("--cache", default=str(db_dir / "posts_cache.db"), help="Path to sqlite database with posts cache.")
    parser.add_argument("--update-cache", action="store_true")
    parser.add_argument("--character_db", "--cdb", dest="cdb", default=str(db_dir / "characters.db"), help="Path to characters/series mapping database.")
    parser.add_argument("--images", dest="image_path", default="", help="Path to images")
    parser.add_argument("--prefix", default="import", help="What directory name will be used inside Shimmie directory.")
    parser.add_argument("--batch", type=int, default=20, help="How many images should be processed simultaneously (default is 20)")
    parser.add_argument("--threads", type=int, default=get_cpu_threads() // 2, help="Number of threads to use (default is half of the detected CPU threads)")

    main(parser.parse_args())
