"""This is designed to help with batch importing into shimmie2"""
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pathlib import Path
import argparse
import csv
import html
import re
import sqlite3
import tqdm

from PIL import Image
from functions.utils import (get_cpu_threads, convert_cdn_links, rating_from_score, resolve_post,
                             save_post_to_cache, process_webp, dedup_prefixed)

Image.MAX_IMAGE_PIXELS = None
ALLOWED_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".jxl", ".avif"}

# Paths setup
SCRIPT_DIR = Path(__file__).parent.resolve()
DB_DIR = SCRIPT_DIR / ".." / "database"
CDB_PATH = DB_DIR / "characters.db"
ADB_PATH = DB_DIR / "artists.db"
TAG_DB_PATH = DB_DIR / "tag_rating_dominant.db"
CACHE_PATH = DB_DIR / "posts_cache.db"

# Data Structures
ResolutionData = namedtuple('ResolutionData', ['image', 'post', 'md5', 'px_hash', 'exists'])
Mappings = namedtuple('Mappings', ['char', 'artist', 'rating'])

def check_paths():
    """Validates existence of required database files."""
    if not CDB_PATH.is_file():
        raise FileNotFoundError(f"Character DB not found: {CDB_PATH}")
    if not ADB_PATH.is_file():
        raise FileNotFoundError(f"Artist DB not found: {ADB_PATH}")
    if not TAG_DB_PATH.is_file():
        raise FileNotFoundError("Tag DB not found")
    if not CACHE_PATH.is_file():
        raise FileNotFoundError(f"Cache not found: {CACHE_PATH}")

def load_mappings():
    """Loads character, artist, and tag rating mappings from SQLite."""
    char_map = {}
    with sqlite3.connect(CDB_PATH) as conn:
        for row in conn.execute("SELECT * FROM data"):
            if len(row) >= 2 and row[0].strip() and row[1].strip():
                char_map[row[0].strip()] = row[1].strip()

    artist_map = {}
    with sqlite3.connect(ADB_PATH) as conn:
        for row in conn.execute("SELECT * FROM data"):
            if len(row) >= 2 and row[0].strip() and row[1].strip():
                artist_map[row[0].strip()] = row[1].strip()

    rating_map = {}
    with sqlite3.connect(TAG_DB_PATH) as conn:
        for row in conn.execute("SELECT * FROM dominant_tag_ratings"):
            if len(row) >= 2:
                rating_map[row[0].strip()] = row[1]

    print(f"[INFO] Loaded {len(char_map):,} chars, {len(artist_map):,} artists.")
    return Mappings(char_map, artist_map, rating_map)

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

def enrich_tags(initial_tags, mappings):
    """Adds prefixes (series:, character:, artist:) based on mappings."""
    # First pass: Add inferred tags, then remove the bare tag if it was successfully prefixed
    temp_tags = []
    for tag in initial_tags:
        temp_tags.append(tag)
        if tag in mappings.char:
            inferred = mappings.char[tag]
            temp_tags.append(f"character:{tag}")
            if isinstance(inferred, (list, tuple, set)):
                temp_tags.extend(f"series:{t}" for t in inferred)
            else:
                temp_tags.append(f"series:{inferred}")

    # Second pass: remove bare tags that were identified as characters
    stage_1 = [t for t in temp_tags if t not in mappings.char]

    # Third pass: Handle artists
    final_tags = []
    for tag in stage_1:
        final_tags.append(tag)
        if tag in mappings.artist:
            final_tags.append(f"artist:{tag}")

    return [t for t in final_tags if t not in mappings.artist]

def calculate_rating(tags, post_rating_list, rating_map, smax, qmax):
    """Determines rating based on tag weights or fallback to database if available"""
    total_score = 0
    for tag in tags:
        weight = rating_map.get(tag, 0)
        if weight > 1:
            total_score += weight
        elif weight == 1 and total_score == 0:
            # Matches original logic: a weight of 1 ensures the score is at least 1
            total_score = 1

    rating_letter = None
    if total_score > 0:
        rating_letter = rating_from_score(total_score, smax, qmax)

    if rating_letter is None:
        # Check database fallbacks
        if "explicit" in post_rating_list:
            rating_letter = "e"
        elif any(r in post_rating_list for r in ["questionable", "sensitive"]):
            rating_letter = "q"
        elif "general" in post_rating_list:
            rating_letter = "s"
        else:
            rating_letter = "?"

    return "s" if rating_letter == "g" else rating_letter

def compile_metadata(image, post, mappings, args):
    """Generates the final tag string and rating letter."""
    tags = []
    tags.extend(post.get("general", []))
    tags.extend(f"character:{t}" for t in post.get("character", []))
    tags.extend(f"series:{t}" for t in post.get("series", []))
    tags.extend(f"artist:{t}" for t in post.get("artist", []))
    tags.extend(get_sidecar_tags(image))

    tags = enrich_tags(tags, mappings)

    if post.get("source"):
        tags.append(f"source:{convert_cdn_links(post['source'])}")

    rating = calculate_rating(tags, post.get("rating", []), mappings.rating, args.smax, args.qmax)

    tags = [re.sub(r'\s+', '_', tag.strip()) for tag in tags]
    dedup_prefixed(tags)
    return ", ".join(sorted(set(tags))), rating, tags

def process_image_result(image, res_data, args, mappings):
    """
    Processes a single resolved image.
    Args:
        image (Path): Path to image.
        res_data (ResolutionData): NamedTuple (post, md5, px_hash, exists).
        args (Namespace): CLI arguments.
        mappings (Mappings): NamedTuple (char, artist, rating).
    Returns:
        tuple: (csv_row_list, thumb_path_str_or_None)
    """
    if res_data.exists == "error":
        print(f"{image} skipped due to error!")
        return None, None

    rel_path = image.relative_to(args.image_path)
    thumb_path = Path(args.prefix) / "thumbnails" / rel_path if args.thumbnail else ""

    # Check if exists in DB (skip metadata gen if so)
    if res_data.exists:
        thumb_file = Path(args.image_path) / "thumbnails" / rel_path
        return None, str(thumb_file) if args.thumbnail else None

    # New Image Logic
    tag_str, rating, tag_list = compile_metadata(image, res_data.post, mappings, args)

    if args.update_cache:
        save_post_to_cache(rating, tag_list, res_data.md5, res_data.px_hash, CACHE_PATH)

    row = [
        f"{args.prefix}/{rel_path}",
        tag_str,
        "",
        rating,
        str(thumb_path) if args.thumbnail else '""'
    ]
    return row, None

def collect_images(path, batch_size):
    """Finds all valid images and chunks them."""
    files = Path(path).rglob("*")
    images = [
        f for f in files
        if f.suffix.lower() in ALLOWED_EXTS and f.is_file()
        and "thumbnails" not in f.relative_to(path).parts
    ]
    batches = [images[i:i + batch_size] for i in range(0, len(images), batch_size)]
    return images, batches

def print_summary(args):
    """Prints run configuration."""
    print("=== Tagger Run Summary ===")
    print(f"📁  Input:           {args.image_path}")
    print(f"📥  Input Cache:     {CACHE_PATH}")
    print(f"🗄️  Update Cache:    {args.update_cache}")
    print(f"📦  Batch Size:      {args.batch}")
    print(f"🧵  Threads:         {args.threads}")
    print(f"📂  Prefix:          {args.prefix}")
    print()

def write_output(base_path, rows):
    """Writes the CSV file."""
    csv_path = Path(base_path) / "import.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerows(rows)
    print(f"[✓] Shimmie CSV written to {csv_path}")

def resolve_batch_metadata(batch, args):
    """Handles the IO-bound task of resolving posts for a batch."""
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        resolver = executor.map(
            lambda img: resolve_post(img, args.spath, args.skip_existing,
                                     args.dbuser, CACHE_PATH),
            batch
        )
        return list(tqdm.tqdm(resolver, total=len(batch),
                              desc="Resolving", position=2, leave=False))

def generate_thumbnails(tasks, threads):
    """Handles the CPU-bound task of processing images."""
    if not tasks:
        return
    with ProcessPoolExecutor(max_workers=threads) as imgpro:
        list(imgpro.map(process_webp, tasks))

def process_batches(batches, mappings, args):
    """
    Handles the batch processing logic.
    """
    csv_rows = []
    existing_thumbs = set()

    for batch in tqdm.tqdm(batches, desc="Image batches", position=1, leave=False):
        results = resolve_batch_metadata(batch, args)
        thumb_tasks = []

        for img, res_tuple in zip(batch, results):
            res_data = ResolutionData(*res_tuple)
            row, thumb_key = process_image_result(img, res_data, args, mappings)

            if thumb_key:
                existing_thumbs.add(thumb_key)

            if row:
                csv_rows.append(row)
                if args.thumbnail:
                    t_src = Path(args.image_path)/"thumbnails"/img.relative_to(args.image_path)
                    if str(t_src) not in existing_thumbs and not t_src.is_file():
                        thumb_tasks.append((img, t_src))

        generate_thumbnails(thumb_tasks, args.threads)

    return csv_rows

def main(args):
    """The main execution flow."""
    check_paths()
    if not Path(args.image_path).is_dir():
        raise FileNotFoundError(f"Path not found: {args.image_path}")

    print_summary(args)
    mappings = load_mappings()
    images, batches = collect_images(args.image_path, args.batch)

    # Process batches and get results
    csv_rows = process_batches(batches, mappings, args)
    csv_rows.sort()

    write_output(args.image_path, csv_rows)
    print(f"\n[✓] Processed {len(images)} image(s) across {len(batches)} batch(es).")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Creates a CSV suitable for input into Shimmie2.")
    parser.add_argument("--batch", type=int, default=20, help="Batch size")
    parser.add_argument("--dbuser", default=None, help="Shimmie DB user")
    parser.add_argument("--images", dest="image_path", required=True, help="Path to images")
    parser.add_argument("--prefix", default="import", help="Dir name inside Shimmie")
    parser.add_argument("--qmax", default=250, help="Max questionable rating.")
    parser.add_argument("--skip-existing", action="store_true", help="Check Shimmie for image")
    parser.add_argument("--smax", default=50, help="Max safe rating.")
    parser.add_argument("--spath", help="Path to shimmie root")
    parser.add_argument("--threads", type=int, default=get_cpu_threads() // 2, help="Thread count")
    parser.add_argument("--thumbnail", action="store_true", help="Generate thumbnails")
    parser.add_argument("--update-cache", action="store_true", help="Flag to update the cache")

    preargs = parser.parse_args()
    if preargs.skip_existing and not preargs.spath:
        parser.error("--spath is required when --skip-existing is set.")

    main(preargs)
