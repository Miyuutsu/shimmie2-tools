from PIL import Image, UnidentifiedImageError
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from huggingface_hub import hf_hub_download
from huggingface_hub.utils import HfHubHTTPError
from io import BytesIO
from pathlib import Path
from pyvips import Image as VipsImage
from simple_parsing import field, parse_known_args
from sd_tag_editor.tag_tree_functions import flatten_tags, load_groups, GroupTree, prune
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

script_dir = Path(__file__).parent.resolve()
db_dir = script_dir / ".." / "database"
func_dir = script_dir / "functions"

from func_dir.utils import validate_float, get_cpu_threads, model_map as vfloat, cputhreads, model_map

def main(args):

    if file_path.is_file(Path(args.cache)):
        cache_path = Path(args.cache)
        cache_conn = sqlite3.connect(cache_path)
        cache_cursor = cache_conn.cursor()
    else:
        raise FileNotFoundError(f"Warning: Dabase cache not found. Database cache is considered mandatory due to speed and resource requirements.")

    if file_path.is_file(Path(args.cdb)):
        cdb_path = Path(args.cdb)
        cdb_conn = sqlite3.connect(cdb_path)
        cdb_cursor = cdb_conn.cursor()
    else:
        raise FileNotFoundError(f"Character database is mandatory now.")

    if not file_path.is_dir(Path(args.images))
        raise FileNotFoundError(f"Path not found: {images}")

    images = [f for f in (args.image_path.rglob("*") if args.subfolder else image_path.iterdir()) if f.suffix.lower() in ALLOWED_EXTS and f.is_file()]

    # Step 1: Load cache if present
    if conn:
        print(f"[INFO] Using SQLite cache from {args.cache}...")

    csv_rows = []

    # Create batches using the --batch option
    batches = [images[i:i + args.batch] for i in range(0, len(images), args.batch)]

    character_series_map = {}

    for batch in tqdm.tqdm(batches, desc="Tagging images"):

    # Preprocess and store md5s and images
    # === Multi-threaded post resolution ===
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        results = list(tqdm.tqdm(
            executor.map(lambda img: resolve_post(img, cache_path), batch),
            total=len(batch),
            desc="Resolving posts"
        ))

    # Get images that need tagging
    tag_needed = [img for img, post in results if post is None]
    img_inputs = process_batch(tag_needed, transform)

    if img_inputs is not None and len(img_inputs) > 0:
        with torch.inference_mode():
            batched_tensor = img_inputs.to(torch_device)
            raw_outputs = F.sigmoid(model(batched_tensor)).cpu()
        raw_outputs = list(torch.unbind(raw_outputs, dim=0))
    else:
        raw_outputs = []

    out_idx = 0
    for image, post in results:
        # If post is still missing, run the tagger
        if not post:
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


            transform = create_transform(**resolve_data_config(model.pretrained_cfg, model=model))
            group_tree: GroupTree = load_groups()

            if out_idx >= len(img_inputs):
                print(f"[WARN] Out-of-bounds image tensor for: {image.name}")
                continue
            img_tensor = img_inputs[out_idx]
            if img_tensor is None:
                continue  # skip if failed to load
            probs = raw_outputs[out_idx]
            out_idx += 1

            char, gen, artist, series, rating = get_tags(
                probs,
                labels,
                args.gt,
                args.ct,
                args.rt
            )

            general_tags = [t[0] for t in gen.items()]
            rating_tags = [t[0] for t in rating.items()]

            if file_path.is_file(db_dir / "tag_rating_dominant.db"):
                rating_priority = {'e': 3, 'q': 2, 's': 1}
                tag_db_path = db_dir / "tag_rating_dominant.db"
                tag_db_conn = sqlite3.connect(tag_db_path)
                tag_db_cursor = tag_db_conn.cursor()
                tag_db_cursor.execute(f"SELECT * FROM dominant_tag_ratings")
                rows = cursor.fetchall()

                for row in rows:
                    if len(row) >= 2:
                        db_tag, db_rating = row[0].strip(), row[1].strip()
                        for gen_tag in general_tags:
                            if gen_tag in db_tag:
                                if db_rating == 'e' and rating_letter != 'e':
                                    rating_letter = 'e'
                                    break  # Stop further checks, as 'e' is the highest priority
                                elif db_rating == 'q' and rating_letter not in ['e', 'q']:
                                    rating_letter = 'q'
                                    break  # Stop further checks if 'q' is found and no 'e'
                                elif db_rating == 's' and rating_letter not in ['e', 'q', 's']:
                                    rating_letter = 's'
                                    break  # Stop further checks if 's' is found and no higher priority
                if rating_letter:
                    break

                else:
                    if not rating_letter:
                        rating_letter = "?"
                    if "explicit" in rating:
                        rating_letter = "e"
                    elif "questionable" in rating or "sensitive" in rating:
                        rating_letter = "q"
                    elif "general" in rating:
                        rating_letter = "s"

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

                tag_db_conn.close()

        # Build tags from post
        tags = []
        tags.extend(post.get("general", []))
        tags.extend(f"character:{t}" for t in post.get("character", []))
        tags.extend(f"series:{t}" for t in post.get("series", []))
        tags.extend(f"artist:{t}" for t in post.get("artist", []))

        # Clean rating-related tags and append a single normalized one
        tags = [t for t in tags if t not in ("general", "sensitive", "questionable", "explicit")]
        tags = [t for t in tags if not t.startswith("rating=")]

        rating_letter = post.get("rating", "?")
        tags.append(f"rating={rating_letter}")

        if post.get("source"):
            tags.append(f"source:{post['source']}")

        tags = sorted(set(tags))
        tag_str = ", ".join(tags)

        rel_path = image.relative_to(image_path)
        csv_rows.append([
            f"{prefix}/{rel_path}",
            tag_str,
            "",
            rating_letter,
            ""
        ])
    print(f"[✓] Ran tagger on {out_idx} of {len(images)} images.")


    if cdb_path.exists():
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        for row in rows:
            if len(row) >= 2:
                char, series = row[0].strip(), row[1].strip()
                if char and series:
                    character_series_map[char] = series

        print(f"[INFO] Loaded {len(character_series_map):,} character→series mappings from SQLite.")

    if csv_rows:
        csv_path = args.image_path / "import.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerows(csv_rows)
        print(f"[✓] Shimmie CSV written to {csv_path}")

    if cache_conn or cdb_conn:
        print("[✓] Done.")
        if cache_conn:
            cache_conn.close()
        if cdb_conn:
            cdb_conn.close()
    print(f"\n[✓] Processed {len(images)} image(s) across {len(batches)} batch(es).")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Creates a CSV suitable for input into Shimmie2.")
    # input and path related
    parser.add_argument("--cache", default=str(db_dir / "posts_cache.db"), help="Path to sqlite database with posts cache.")
    parser.add_argument("--character_db", "--cdb", dest="cdb", default=str(db_dir / "characters.db"), help="Path to characters/series mapping database.")
    parser.add_argument("--images", dest="image_path", default="", help="Path to images")
    parser.add_argument("--model", choices=model_map.keys(), default="vit-large", help="Which inference model to use for fallback to image tagging. Choices are vit and vit-large.")
    parser.add_argument("--prefix", default="import", help="What directory name will be used inside Shimmie directory.")
    # batch related integers
    parser.add_argument("--batch", type=int, default=20, help="How many images should be processed simultaneously (default is 20)")
    parser.add_argument("--threads", type=int, default=cputhreads // 2, help="Number of threads to use (default is half of the detected CPU threads)")
    # input thresholds
    parser.add_argument("--character_threshold", "--ct", dest="ct", type=vfloat, default=0.35, help="Threshold to use for character tags. Default is 0.35.")
    parser.add_argument("--general_threshold", "--gt", dest="gt", type=vfloat, default=0.5, help="Threshold to use for general tags. Default is 0.5.")
    parser.add_argument("--rating_threshold", "--rt", dest="rt", type=vfloat, default=0.3, help="Threshold to use for rating tags. Default is 0.3.")

    # misc actions
    parser.add_argument("--subfolder", action="store_true", default=False)

    # Parse arguments
    args = parser.parse_args()

    print("=== Tagger Run Summary ===")
    print(f"📁  Input:           {args.images}")
    if args.cache is not None:
        print(f"📥  Input Cache:     {args.cache}")
    print(f"📦  Batch Size:       {args.batch}")
    print(f"🧠  Model:            {args.model}")
    print(f"✨  Gen Threshold:    {args.gt:.2f}")
    print(f"🔞  Rating Threshold: {args.rt:.2f}")
    print(f"👤  Char Threshold:   {args.ct:.2f}")
    print(f"🧵  Threads:          {args.threads}")
    print(f"📂  Subfolders:       {'Yes' if args.subfolder else 'No'}")
    print(f"    Prefix:           {args.prefix}")
    print()

    if args.images is not None:
        main(args)
