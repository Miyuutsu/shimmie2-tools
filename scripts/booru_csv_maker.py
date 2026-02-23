"""This is designed to help with batch importing into shimmie2"""
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pathlib import Path
import argparse
import csv
import re
import sqlite3
import tqdm

from PIL import Image
from functions.utils import (
    get_cpu_threads, resolve_best_source, rating_from_score,
    resolve_post, save_post_to_cache, process_webp, apply_tag_curation,
    get_video_resolution, VIDEO_EXTS, get_sidecar_tags,
    get_shimmie_db_credentials, get_cache_conn, mine_tag_equivalencies,
    load_dynamic_mappings
)

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

def collect_files(image_path, video_path, batch_size):
    """Finds all valid files from provided paths, handles duplicates, and chunks them."""
    files = []

    if image_path:
        img_dir = Path(image_path)
        if img_dir.is_dir():
            files.extend([f for f in img_dir.rglob("*") if f.suffix.lower() in ALLOWED_EXTS])

    if video_path:
        vid_dir = Path(video_path)
        if vid_dir.is_dir():
            files.extend([f for f in vid_dir.rglob("*") if f.suffix.lower() in VIDEO_EXTS])

    grouped_files = {}
    for f in files:
        if f.is_file() and "thumbnails" not in f.parts:
            stem = f.with_suffix('')
            if stem not in grouped_files:
                grouped_files[stem] = []
            grouped_files[stem].append(f)

    final_files = []
    for stem, group in grouped_files.items():
        if len(group) == 1:
            final_files.append(group[0])
        else:
            with_sidecars = [f for f in group if f.with_name(f.name + ".txt").is_file()]
            if len(with_sidecars) == 1:
                final_files.append(with_sidecars[0])
            else:
                print(f"[WARNING] Skipping {stem}.* - Ambiguous multiple formats without tags.")

    batches = [final_files[i:i + batch_size] for i in range(0, len(final_files), batch_size)]
    return final_files, batches

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
    if 0 < total_score <= smax and "tagme" in tags:
        rating_letter = "?"
    elif total_score > 0:
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

def clean_resolution_tags(tags, image_path):
    """Calculates resolution tags based on pixel count and dimensions."""
    res_group = {"lowres", "highres", "absurdres",
                 "incredibly_absurdres", "wide_image", "tall_image"}
    res_tags = [t for t in tags if not t in res_group]

    # Branch based on the file type
    if image_path.suffix.lower() in VIDEO_EXTS:
        width, height = get_video_resolution(image_path)
        if not width or not height:
            return res_tags
    else:
        with Image.open(image_path) as img:
            width, height = img.size

    pixels = width * height
    ratio = width / height

    # Dimension Check
    if width > 10000 or height > 10000:
        res_tags.append("incredibly_absurdres")

    # Pixel Count Checks
    if pixels >= 7680000:
        res_tags.append("absurdres")
    elif pixels >= 3686400:
        res_tags.append("highres")
    elif pixels <= 589824:
        res_tags.append("lowres")

    if ratio >= 4.0:
        res_tags.append("wide_image")
    elif ratio <= 0.25:
        res_tags.append("tall_image")

    return res_tags

def compile_metadata(image, post, mappings, args, dynamic_mappings=None):
    """Generates the final tag string and rating letter."""
    tags = []
    tags.extend(args.pretags)
    tags.extend(post.get("general", []))
    tags.extend(f"character:{t}" for t in post.get("character", []))
    tags.extend(f"series:{t}" for t in post.get("series", []))
    tags.extend(f"artist:{t}" for t in post.get("artist", []))
    tags.extend(get_sidecar_tags(image))

    sidecar_tags = get_sidecar_tags(image)
    tags.extend(sidecar_tags)

    tags = enrich_tags(tags, mappings)
    tags = clean_resolution_tags(tags, image)

    # --- Unified Source Resolution ---
    best_source = resolve_best_source(post.get("source"), image)
    if best_source:
        tags.append(f"source:{best_source}")

    # Clean whitespace and strip redundant _series) suffixes
    tags = [re.sub(r'\s+', '_', tag.strip()) for tag in tags]
    tags = [re.sub(r'_series\)$', ')', tag.strip()) for tag in tags]

    apply_tag_curation(tags, dynamic_mappings)

    if len(tags) < 15:
        tags.append("tagme")
    rating = calculate_rating(tags, post.get("rating", []), mappings.rating, args.smax, args.qmax)
    if not any(tag.startswith("artist:") for tag in tags):
        tags.append("artist:tagme")
    if not any(tag.startswith("character:") for tag in tags):
        tags.append("character:tagme")
    if not any(tag.startswith("series:") for tag in tags):
        tags.append("series:tagme")

    return ", ".join(sorted(set(tags))), rating, tags, best_source

def process_image_result(image, res_data, args, mappings, dynamic_mappings):
    """
    Processes a single resolved file.
    Args:
        file (Path): Path to file.
        res_data (ResolutionData): NamedTuple (post, md5, px_hash, exists).
        args (Namespace): CLI arguments.
        mappings (Mappings): NamedTuple (char, artist, rating).
    Returns:
        tuple: (csv_row_list, thumb_path_str_or_None)
    """
    if res_data.exists == "error":
        print(f"{image} skipped due to error!")
        return None, None

    if args.image_path and image.is_relative_to(args.image_path):
        base_path = Path(args.image_path)
    else:
        base_path = Path(args.video_path)
    rel_path = image.relative_to(base_path)
    thumb_path = Path(args.prefix) / "thumbnails" / rel_path if args.thumbnail else ""

    # Check if exists in DB (skip metadata gen if so)
    if res_data.exists:
        thumb_file = Path(args.image_path) / "thumbnails" / rel_path
        return None, str(thumb_file) if args.thumbnail else None

    # New Image Logic
    tag_str, rating, tag_list, best_source = compile_metadata(image, res_data.post,
                                                              mappings, args, dynamic_mappings)

    if args.update_cache:
        save_post_to_cache(res_data, rating, tag_list, best_source, CACHE_PATH)

    row = [
        f"{args.prefix}/{rel_path}",
        tag_str,
        "",
        rating,
        str(thumb_path) if args.thumbnail else '""'
    ]
    return row, None

def print_summary(args):
    """Prints run configuration."""
    print("=== Tagger Run Summary ===")
    if args.image_path:
        print(f"📁  Images:          {args.image_path}")
    if args.video_path:
        print(f"🎞️  Videos:          {args.video_path}")
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
    print(f"\n[✓] Shimmie CSV written to {csv_path}")

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

def get_thumbnail_path(img, args):
    """Helper to determine the correct thumbnail path to save local variables."""
    if args.image_path and img.is_relative_to(args.image_path):
        return Path(args.image_path) / "thumbnails" / img.relative_to(args.image_path)
    return Path(args.video_path) / "thumbnails" / img.relative_to(args.video_path)

def process_batches(batches, mappings, args, dynamic_mappings):
    """Handles the batch processing logic."""
    csv_rows = []
    existing_thumbs = set()

    for batch in tqdm.tqdm(batches, desc="Image batches", position=1, leave=False):
        results = resolve_batch_metadata(batch, args)
        thumb_tasks = []

        for img, res_tuple in zip(batch, results):
            res_data = ResolutionData(*res_tuple)

            # Line break added here to fix line-too-long
            row, thumb_key = process_image_result(
                img, res_data, args, mappings, dynamic_mappings
            )

            if thumb_key:
                existing_thumbs.add(thumb_key)

            if row:
                csv_rows.append(row)
                if args.thumbnail:
                    t_src = get_thumbnail_path(img, args)
                    if str(t_src) not in existing_thumbs and not t_src.is_file():
                        thumb_tasks.append((img, t_src))

        generate_thumbnails(thumb_tasks, args.threads)

    return csv_rows

def run_mining_mode(args, files, mappings):
    """Isolates the mining phase to reduce local variables in main()."""
    db_conn = get_shimmie_db_credentials(args.spath)
    with get_cache_conn(CACHE_PATH) as sqlite_conn:
        mine_tag_equivalencies(
            image_list=files,
            conns=(db_conn, sqlite_conn),
            output_path=args.create_map_csv,
            mappings=mappings
        )
    print("Mining complete. Exiting before standard import processing.")

def main(args):
    """The main execution flow."""
    check_paths()
    if args.image_path and not Path(args.image_path).is_dir():
        raise FileNotFoundError(f"Image path not found: {args.image_path}")
    if args.video_path and not Path(args.video_path).is_dir():
        raise FileNotFoundError(f"Video path not found: {args.video_path}")

    print_summary(args)
    mappings = load_mappings()
    files, batches = collect_files(args.image_path, args.video_path, args.batch)

    # --- MINING MODE INTERCEPT ---
    if args.create_map_csv:
        run_mining_mode(args, files, mappings)
        return
    # -----------------------------

    dynamic_mappings = {}
    if args.use_map_csv:
        dynamic_mappings = load_dynamic_mappings(args.use_map_csv)
        print(f"[INFO] Loaded {len(dynamic_mappings)} dynamic tag mappings.")
    # Process batches and get results
    csv_rows = process_batches(batches, mappings, args, dynamic_mappings)
    csv_rows.sort()

    out_dir = args.image_path if args.image_path else args.video_path
    write_output(out_dir, csv_rows)
    print(f"\n[✓] Processed {len(files)} file(s) across {len(batches)} batch(es).")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Creates a CSV suitable for input into Shimmie2.")
    parser.add_argument("--batch", type=int, default=20, help="Batch size")
    parser.add_argument("--create-map", dest="create_map_csv",
                        help="Mine tags and create a CSV map at this path")
    parser.add_argument("--dbuser", default=None, help="Shimmie DB user")
    parser.add_argument("--images", dest="image_path", help="Path to images directory")
    parser.add_argument("--prefix", default="import", help="Dir name inside Shimmie")
    parser.add_argument("--pretags", type=str, default="",
                        help="Comma-separated list of tags to prepend to all posts")
    parser.add_argument("--qmax", default=250, help="Max questionable rating.")
    parser.add_argument("--skip-existing", action="store_true", help="Check Shimmie for image")
    parser.add_argument("--smax", default=50, help="Max safe rating.")
    parser.add_argument("--spath", help="Path to shimmie root")
    parser.add_argument("--threads", type=int, default=get_cpu_threads() // 2, help="Thread count")
    parser.add_argument("--thumbnail", action="store_true", help="Generate thumbnails")
    parser.add_argument("--update-cache", action="store_true", help="Flag to update the cache")
    parser.add_argument("--use-map", dest="use_map_csv",
                        help="Load an existing CSV map from this path and apply it")
    parser.add_argument("--videos", dest="video_path", help="Path to videos directory")

    preargs = parser.parse_args()
    if not preargs.image_path and not preargs.video_path:
        parser.error("You must provide at least one input path: --images or --videos")
    if preargs.skip_existing and not preargs.spath:
        parser.error("--spath is required when --skip-existing is set.")

    if preargs.pretags:
        preargs.pretags = [t.strip() for t in preargs.pretags.split(",") if t.strip()]
    else:
        preargs.pretags = []

    main(preargs)
