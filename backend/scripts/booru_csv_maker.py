from PIL import Image, UnidentifiedImageError
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from functions.utils import validate_float, get_cpu_threads, model_map, add_module_path

add_module_path("../../sd_tag_editor")
from tag_tree_functions import flatten_tags, load_groups, GroupTree, prune

from huggingface_hub import hf_hub_download
from huggingface_hub.utils import HfHubHTTPError
from io import BytesIO
from pathlib import Path
from pyvips import Image as VipsImage
from simple_parsing import field, parse_known_args
from tempfile import NamedTemporaryFile
from timm.data import create_transform, resolve_data_config
from torch import Tensor, nn
from torch.nn import functional as F
from typing import Optional, Tuple
import csv
import hashlib
import json
import numpy as np
import pandas as pd
import pyvips
import re
import sqlite3
import sys
import tempfile
import timm
import torch
import tqdm

Image.MAX_IMAGE_PIXELS = None
torch_device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
ALLOWED_EXTS = {".jpg", ".jpeg", ".png", ".webp"}
MD5_RE = re.compile(r"[a-fA-F0-9]{32}")

script_dir = Path(__file__).parent.resolve()
db_dir = script_dir / ".." / "database"

@dataclass
class LabelData:
    names: list[str]
    rating: list[int]
    general: list[int]
    character: list[int]
    artist: list[int]
    copyright: list[int]

### I forgot all of these, they still need the rewrite
def resolve_post(image: Path, cache) -> tuple[Path, dict | None]:
    if Path(cache).is_file:
        path = Path(cache)
        conn = sqlite3.connect(path)
        cur = conn.cursor()
    else:
        raise FileNotFoundError(f"Warning: Dabase cache not found. Database cache is considered mandatory due to speed and resource requirements.")

    # Try MD5 from filename
    match = MD5_RE.search(image.stem)
    md5 = match.group(0).lower() if match else None
    post = None

    if cur and md5:
        cur.execute("SELECT * FROM posts WHERE md5 = ?", (md5,))
        row = cur.fetchone()
        if row:
            post = row_to_post_dict(row)

    if not post and cur:
        try:
            px_hash = compute_danbooru_pixel_hash(image)
            cur.execute("SELECT * FROM posts WHERE pixel_hash = ?", (px_hash,))
            row = cur.fetchone()
            if row:
                post = row_to_post_dict(row)
        except Exception as e:
            print(f"[WARN] Pixel hash failed for {image.name}: {e}")

    conn.close()
    return image, post

def save_post_to_cache(image: Path, post: dict, cache: Path):
    if not Path(cache).is_file():
        raise FileNotFoundError(f"Cannot save to cache: {cache} does not exist.")

    # Use MD5 from filename if possible
    match = MD5_RE.search(image.stem)
    md5 = match.group(0).lower() if match else None

    # Compute fallback pixel hash
    px_hash = compute_danbooru_pixel_hash(image)

    # Extract and format each tag category
    rating = post.get("rating", "?")
    source = post.get("source", None)

    general = ", ".join(sorted(set(post.get("general", []))))
    character = ", ".join(sorted(set(post.get("character", []))))
    artist = ", ".join(sorted(set(post.get("artist", []))))
    series = ", ".join(sorted(set(post.get("series", []))))

    # Connect and insert
    conn = sqlite3.connect(cache)
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
    conn.close()

def row_to_post_dict(row: tuple) -> dict:
    return {
        "md5": row[0],
        "pixel_hash": row[1],
        "rating": row[2],
        "source": row[3],
        "general": row[4].split(",") if row[4] else [],
        "character": row[5].split(",") if row[5] else [],
        "artist": row[6].split(",") if row[6] else [],
        "series": row[7].split(",") if row[7] else [],
    }

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

def md5sum(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def pil_ensure_rgb(image: Image.Image) -> Image.Image:
    if image.mode not in ["RGB", "RGBA"]:
        image = image.convert("RGBA") if "transparency" in image.info else image.convert("RGB")
    if image.mode == "RGBA":
        canvas = Image.new("RGBA", image.size, (255, 255, 255))
        canvas.alpha_composite(image)
        image = canvas.convert("RGB")
    return image

def pil_pad_square(image: Image.Image) -> Image.Image:
    w, h = image.size
    px = max(w, h)
    canvas = Image.new("RGB", (px, px), (255, 255, 255))
    canvas.paste(image, ((px - w) // 2, (px - h) // 2))
    return canvas

def load_and_process_image(path: Path, transform) -> Optional[Tensor]:
    try:
        img = Image.open(path)
        img = pil_ensure_rgb(img)
        img = pil_pad_square(img)
        img_tensor = transform(img).unsqueeze(0)[:, [2, 1, 0]]
        return img_tensor
    except (UnidentifiedImageError, OSError):
        return None

def process_batch(batch: list[Path], transform) -> torch.Tensor:
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(lambda p: load_and_process_image(p, transform), batch))
    return torch.cat([r for r in results if r is not None])

def get_tags(probs: Tensor, labels, gen_threshold, char_threshold, rating_threshold):
    probs = list(zip(labels.names, probs.numpy()))
    gen = {x[0]: x[1] for x in (probs[i] for i in labels.general) if x[1] > gen_threshold}
    char = {x[0]: x[1] for x in (probs[i] for i in labels.character) if x[1] > char_threshold}
    artist = {x[0]: x[1] for x in (probs[i] for i in labels.artist) if x[1] > char_threshold}
    series = {x[0]: x[1] for x in (probs[i] for i in labels.copyright) if x[1] > char_threshold}
    rating = {x[0]: x[1] for x in (probs[i] for i in labels.rating) if x[1] > rating_threshold}
    return char, gen, artist, series, rating

def flatten_all_tags(char, gen, artist, series, rating, group_tree, no_prune):
    pruned_gen = list(gen.items())
    if no_prune:
        pruned = pruned_gen
    else:
        pruned = flatten_tags(prune(group_tree, dict(pruned_gen)), True)
    pruned_char = [(f"character:{k}", v) for k, v in char.items()]
    pruned_artist = [(f"artist:{k}", v) for k, v in artist.items()]
    pruned_series = [(f"series:{k}", v) for k, v in series.items()]
    pruned_rating = [(k, v) for k, v in rating.items()]
    combined = pruned + pruned_char + pruned_artist + pruned_series + pruned_rating
    seen = set()
    final = []
    for tag, score in sorted(combined, key=lambda x: x[1], reverse=True):
        if tag not in seen:
            seen.add(tag)
            final.append(tag)
    return final

###

def main(args):

    if Path(args.cdb).is_file:
        cdb_path = Path(args.cdb)
        cdb_conn = sqlite3.connect(cdb_path)
        cdb_cursor = cdb_conn.cursor()
    else:
        raise FileNotFoundError(f"Character database is mandatory now.")

    if not Path(args.image_path).is_dir:
        raise FileNotFoundError(f"Path not found: {images}")

    files = Path(args.image_path).rglob("*")

    images = [f for f in files if f.suffix.lower() in ALLOWED_EXTS and f.is_file()]


    # Step 1: Load cache if present
    if cdb_conn:
        print(f"[INFO] Using SQLite cache from {args.cache}...")

    csv_rows = []

    # Create batches using the --batch option
    batches = [images[i:i + args.batch] for i in range(0, len(images), args.batch)]

    character_series_map = {}

    ### tagger related, let's only call this if needed
    model_id = model_map.get(args.model)
    model: nn.Module = timm.create_model("hf-hub:" + model_id).eval().to(torch_device)
    model.load_state_dict(timm.models.load_state_dict_from_hf(model_id))
    print("Loading tag list...")
    try:
        csv_path = hf_hub_download(repo_id=model_id, filename="selected_tags.csv")
        df = pd.read_csv(csv_path, usecols=["name", "category"])
        labels = LabelData(
            names=df["name"].tolist(),
            rating=list(np.where(df["category"] == 9)[0]),
            general=list(np.where(df["category"] == 0)[0]),
            character=list(np.where(df["category"] == 4)[0]),
            artist=list(np.where(df["category"] == 1)[0]),
            copyright=list(np.where(df["category"] == 3)[0]),
        )
    except HfHubHTTPError as e:
        raise FileNotFoundError(f"selected_tags.csv failed to download from {model_id}") from e

    if cdb_path.exists():
        cdb_cursor.execute(f"SELECT * FROM data")
        rows = cdb_cursor.fetchall()
        for row in rows:
            if len(row) >= 2:
                char, series = row[0].strip(), row[1].strip()
                if char and series:
                    character_series_map[char] = series
        print(f"[INFO] Loaded {len(character_series_map):,} character→series mappings from SQLite.")

    for batch in tqdm.tqdm(batches, desc="Tagging images"):

        # Preprocess and store md5s and images
        # === Multi-threaded post resolution ===
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            results = list(tqdm.tqdm(
                executor.map(lambda img: resolve_post(img, args.cache), batch),
                total=len(batch),
                desc="Resolving posts"
            ))

        transform = create_transform(**resolve_data_config(model.pretrained_cfg, model=model))
        group_tree: GroupTree = load_groups()

        # Get images that need tagging
        tag_needed = [img for img, post in results if post is None]
        img_inputs = []
        if tag_needed:
            img_inputs = process_batch(tag_needed, transform)

        if img_inputs is not None and len(img_inputs) > 0:
            with torch.inference_mode():
                batched_tensor = img_inputs.to(torch_device)
                raw_outputs = F.sigmoid(model(batched_tensor)).cpu()
                raw_outputs = list(torch.unbind(raw_outputs, dim=0))
        else:
            raw_outputs = []

        out_idx = 0
        processed_results = []

        for image, post in results:
            # If post is still missing, run the tagger
            if not post:
                if out_idx >= len(img_inputs):
                    print(f"[WARN] Out-of-bounds image tensor for: {image.name}")
                    continue
                img_tensor = img_inputs[out_idx]
                if img_tensor is not None:
                    probs = raw_outputs[out_idx]
                    out_idx += 1
                else:
                    continue  # skip if failed to load


                char, gen, artist, series, rating = get_tags(
                    probs,
                    labels,
                    args.gt,
                    args.ct,
                    args.rt
                )

                general_tags = [t[0] for t in gen.items()]
                rating_tags = [t[0] for t in rating.items()]

                rating_letter = None

                if  (db_dir / "tag_rating_dominant.db").is_file():
                    rating_priority = {'e': 3, 'q': 2, 's': 1}

                    tag_db_path = db_dir / "tag_rating_dominant.db"
                    tag_db_conn = sqlite3.connect(tag_db_path)
                    tag_db_cursor = tag_db_conn.cursor()
                    tag_db_cursor.execute(f"SELECT * FROM dominant_tag_ratings")
                    rows = tag_db_cursor.fetchall()
                    for row in rows:
                        if len(row) >= 2:
                            db_tag, db_rating = row[0].strip(), row[1].strip()
                            for gen_tag in general_tags:
                                if gen_tag in db_tag:
                                    if db_rating == 'e':
                                        rating_letter = 'e'
                                        break
                                    elif db_rating == 'q' and rating_letter not in ['e']:
                                        rating_letter = 'q'
                                    elif db_rating == 's' and rating_letter not in ['e', 'q']:
                                        rating_letter = 's'
                    tag_db_conn.close()

                if not rating_letter:
                    if "explicit" in rating:
                        rating_letter = "e"
                    elif "questionable" in rating or "sensitive" in rating:
                        rating_letter = "q"
                    elif "general" in rating:
                        rating_letter = "s"
                    else:
                        rating_letter = "?"

                post = {
                    "character": [t[0] for t in char.items()],
                    "general": [t[0] for t in gen.items()],
                    "artist": [t[0] for t in artist.items()],
                    "series": [t[0] for t in series.items()],
                    "rating": rating_letter,
                    "source": None
                }
                character_tags = [t[0] for t in char.items()]
                series_tags = set()

                for char_tag in character_tags:
                    if char_tag in character_series_map:
                        inferred_series = character_series_map[char_tag]
                        series_tags.add(inferred_series)
                        # ✅ Append inferred series to post
                        post["series"].extend(sorted(series_tags))

                processed_results.append((image, post))
                save_post_to_cache(image, post, args.cache)
            else:
                processed_results.append((image, post))
        # Build tags from post
        for image, post in processed_results:
            if not post:
                continue  # skip if post is still None

            tags = []
            tags.extend(post.get("general", []))
            tags.extend(f"character:{t}" for t in post.get("character", []))
            tags.extend(f"series:{t}" for t in post.get("series", []))
            tags.extend(f"artist:{t}" for t in post.get("artist", []))

            rating_letter = post.get("rating", "?")

            if post.get("source"):
                source = post["source"]
                source = convert_pixiv_link(source)
                tags.append(f"source:{source}")

            tags = [re.sub(r'\s+', ' ', tag).strip() for tag in tags]

            tags = sorted(set(tags))
            tag_str = ", ".join(tags)
            if rating_letter == "g":
                rating_letter = "s"

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
    if out_idx > 0:
        print(f"[✓] Ran tagger on {out_idx} of {len(images)} images and stored them in the database.")
    else:
        print(f"[✓] Ran tagger on {out_idx} of {len(images)} images.")

    print(f"\n[✓] Processed {len(images)} image(s) across {len(batches)} batch(es).")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Creates a CSV suitable for input into Shimmie2.")
    # input and path related
    parser.add_argument("--cache", default=str(db_dir / "posts_cache.db"), help="Path to sqlite database with posts cache.")
    parser.add_argument("--character_db", "--cdb", dest="cdb", default=str(db_dir / "characters.db"), help="Path to characters/series mapping database.")
    parser.add_argument("--images", dest="image_path", default="", help="Path to images")
    parser.add_argument("--model", choices=list(model_map.keys()), default="vit-large", help="Which inference model to use for fallback to image tagging. Choices are vit and vit-large.")
    parser.add_argument("--prefix", default="import", help="What directory name will be used inside Shimmie directory.")
    # batch related integers
    parser.add_argument("--batch", type=int, default=20, help="How many images should be processed simultaneously (default is 20)")
    parser.add_argument("--threads", type=int, default=get_cpu_threads() // 2, help="Number of threads to use (default is half of the detected CPU threads)")
    # input thresholds
    parser.add_argument("--character_threshold", "--ct", dest="ct", type=validate_float, default=0.35, help="Threshold to use for character tags. Default is 0.35.")
    parser.add_argument("--general_threshold", "--gt", dest="gt", type=validate_float, default=0.5, help="Threshold to use for general tags. Default is 0.5.")
    parser.add_argument("--rating_threshold", "--rt", dest="rt", type=validate_float, default=0.3, help="Threshold to use for rating tags. Default is 0.3.")

    # misc actions
    #parser.add_argument("--subfolder", action="store_true", default=False)

    # Parse arguments
    args = parser.parse_args()

    print("=== Tagger Run Summary ===")
    print(f"📁  Input:           {args.image_path}")
    if args.cache is not None:
        print(f"📥  Input Cache:     {args.cache}")
    print(f"📦  Batch Size:       {args.batch}")
    print(f"🧠  Model:            {args.model}")
    print(f"✨  Gen Threshold:    {args.gt:.2f}")
    print(f"🔞  Rating Threshold: {args.rt:.2f}")
    print(f"👤  Char Threshold:   {args.ct:.2f}")
    print(f"🧵  Threads:          {args.threads}")
    #print(f"📂  Subfolders:       {'Yes' if args.subfolder else 'No'}")
    print(f"📂   Prefix:           {args.prefix}")
    print()

    if args.image_path is not None:
        main(args)
