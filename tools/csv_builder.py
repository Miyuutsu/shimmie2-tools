"""This is designed to help with batch importing into shimmie2"""
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pathlib import Path
import csv
import re
import sqlite3
import warnings
import tqdm

from PIL import Image

from functions.common import VIDEO_EXTS
from functions.source_resolver import resolve_best_source
from functions.db_cache import (
    resolve_post, save_post_to_cache, get_shimmie_db_credentials, get_cache_conn
)
from functions.media import process_webp, get_video_resolution, get_image_resolution
from functions.tags_curation import (
    rating_from_score, apply_tag_curation, get_sidecar_tags, load_dynamic_mappings
)
from functions.tags_mining import mine_tag_equivalencies

Image.MAX_IMAGE_PIXELS = None
ALLOWED_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".jxl", ".avif"}

# Suppress DecompressionBombWarning if you deal with massive images
warnings.simplefilter('ignore', Image.DecompressionBombWarning)
# Suppress the Corrupt EXIF warning
warnings.filterwarnings("ignore", "(?s).*Corrupt EXIF data.*", category=UserWarning)

# Paths setup
SCRIPT_DIR = Path(__file__).parent.parent.resolve()
DB_DIR = SCRIPT_DIR / "database"
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

    stage_1 = [t for t in temp_tags if t not in mappings.char]

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
            total_score = 1

    rating_letter = None
    if 0 < total_score <= smax and "tagme" in tags:
        rating_letter = "?"
    elif total_score > 0:
        rating_letter = rating_from_score(total_score, smax, qmax)

    if rating_letter is None:
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

    if image_path.suffix.lower() in VIDEO_EXTS:
        width, height = get_video_resolution(image_path)
    else:
        width, height = get_image_resolution(image_path)

    if not width or not height:
        return res_tags

    pixels = width * height
    ratio = width / height

    if width > 10000 or height > 10000:
        res_tags.append("incredibly_absurdres")

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

    sidecar_tags = get_sidecar_tags(image)
    tags.extend(sidecar_tags)

    tags = enrich_tags(tags, mappings)
    tags = clean_resolution_tags(tags, image)

    best_source = resolve_best_source(post.get("source"), image)
    if best_source:
        tags.append(f"source:{best_source}")

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
    """Processes a single resolved file."""
    if res_data.exists == "error":
        print(f"{image} skipped due to error!")
        return None, None

    if args.image_path and image.is_relative_to(args.image_path):
        base_path = Path(args.image_path)
    else:
        base_path = Path(args.video_path)
    rel_path = image.relative_to(base_path)
    thumb_path = Path(args.prefix) / "thumbnails" / rel_path if args.thumbnail else ""

    if res_data.exists:
        thumb_file = Path(args.image_path) / "thumbnails" / rel_path
        return None, str(thumb_file) if args.thumbnail else None

    tag_str, rating, tag_list, best_source = compile_metadata(
        image, res_data.post, mappings, args, dynamic_mappings
    )

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

def resolve_batch_metadata(batch, args, dbuser):
    """Handles the IO-bound task of resolving posts for a batch."""
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        resolver = executor.map(
            lambda img: resolve_post(
                img, args.spath, args.skip_existing, dbuser, CACHE_PATH
            ),
            batch
        )
        return list(
            tqdm.tqdm(resolver, total=len(batch), desc="Resolving", position=2, leave=False)
        )

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

def _process_single_batch(batch, maps, args, dbuser, existing_thumbs):
    """Helper to process a single batch to reduce local variables."""
    mappings, dynamic_mappings = maps
    results = resolve_batch_metadata(batch, args, dbuser)
    batch_rows = []
    thumb_tasks = []

    for img, res_tuple in zip(batch, results):
        row, thumb_key = process_image_result(
            img, ResolutionData(*res_tuple), args, mappings, dynamic_mappings
        )

        if thumb_key:
            existing_thumbs.add(thumb_key)

        if row:
            batch_rows.append(row)
            if args.thumbnail:
                t_src = get_thumbnail_path(img, args)
                if str(t_src) not in existing_thumbs and not t_src.is_file():
                    thumb_tasks.append((img, t_src))

    return batch_rows, thumb_tasks

def process_batches(batches, mappings, args, dynamic_mappings, dbuser):
    """Handles the batch processing logic."""
    csv_rows = []
    existing_thumbs = set()
    maps = (mappings, dynamic_mappings)

    for batch in tqdm.tqdm(batches, desc="Image batches", position=1, leave=False):
        batch_rows, thumb_tasks = _process_single_batch(
            batch, maps, args, dbuser, existing_thumbs
        )
        csv_rows.extend(batch_rows)
        generate_thumbnails(thumb_tasks, args.threads)

    return csv_rows

def run_mining_mode(args, files, mappings):
    """Isolates the mining phase to reduce local variables in run()."""
    db_conn = get_shimmie_db_credentials(args.spath)
    with get_cache_conn(CACHE_PATH) as sqlite_conn:
        mine_tag_equivalencies(
            image_list=files,
            conns=(db_conn, sqlite_conn),
            output_path=args.create_map_csv,
            mappings=mappings
        )
    print("Mining complete. Exiting before standard import processing.")

def run(args):
    """The main execution flow for csv building."""
    check_paths()
    if args.image_path and not Path(args.image_path).is_dir():
        raise FileNotFoundError(f"Image path not found: {args.image_path}")
    if args.video_path and not Path(args.video_path).is_dir():
        raise FileNotFoundError(f"Video path not found: {args.video_path}")

    print_summary(args)
    mappings = load_mappings()
    files, batches = collect_files(args.image_path, args.video_path, args.batch)

    if args.create_map_csv:
        run_mining_mode(args, files, mappings)
        return

    dbuser = None
    if getattr(args, 'skip_existing', False) and getattr(args, 'spath', None):
        creds = get_shimmie_db_credentials(args.spath)
        if creds:
            dbuser = creds.get('user')

    dynamic_mappings = {}
    if args.use_map_csv:
        dynamic_mappings = load_dynamic_mappings(args.use_map_csv)
        print(f"[INFO] Loaded {len(dynamic_mappings)} dynamic tag mappings.")

    csv_rows = process_batches(batches, mappings, args, dynamic_mappings, dbuser)
    csv_rows.sort()

    out_dir = args.image_path if args.image_path else args.video_path
    write_output(out_dir, csv_rows)
    print(f"\n[✓] Processed {len(files)} file(s) across {len(batches)} batch(es).")
