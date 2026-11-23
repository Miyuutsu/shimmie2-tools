"""Tool to automatically generate missing .txt sidecars for images and videos using in-process AI."""

import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import tqdm

from functions.ai_tagger import (
    SmilingWolfTagger,
    aggregate_frame_predictions,
    decode_media_for_tagger,
)
from functions.common import (
    CACHE_PATH,
    IMAGE_EXTS,
    VIDEO_EXTS,
    extract_or_compute_md5,
    sort_paths_by_disk_order,
)
from functions.db_cache import (
    get_existing_shimmie_hashes,
    get_shimmie_db_credentials,
    load_rating_weights,
    post_has_tags,
    resolve_post,
)
from functions.tags_curation import load_tag_groups_metadata


def _collect_missing_sidecars(target_dir: Path, allowed_extensions: set[str]) -> list[Path]:
    """Finds all valid media files missing a .txt sidecar without touching HDD targets unnecessarily."""
    if not target_dir or not target_dir.is_dir():
        return []
    missing: list[Path] = []
    for f in target_dir.rglob("*"):
        if "thumbnails" in f.parts or f.suffix.lower() not in allowed_extensions:
            continue
        if f.with_suffix(".txt").exists() or f.with_name(f"{f.name}.txt").exists():
            continue
        if not f.is_file():
            continue
        missing.append(f)
    return missing


def _tag_file_group(
    files: list[Path],
    tagger: SmilingWolfTagger,
    tag_prefix: str,
    args,
    threads: int,
    rating_weights: dict[str, int],
    group_info: tuple[dict[str, set[str]], set[str]],
) -> int:
    """Runs overlapped CPU/HDD decode + in-process GPU inference and writes .txt sidecars."""
    if not files:
        return 0

    chunk_size = max(64, args.batch * 8)
    char_prefix = tag_prefix or "character:"
    written_count = 0

    with ThreadPoolExecutor(max_workers=threads) as pool:
        chunk_starts = range(0, len(files), chunk_size)
        next_futs = [pool.submit(decode_media_for_tagger, p, 5) for p in files[0:chunk_size]]

        for c_idx in tqdm.tqdm(range(len(chunk_starts)), desc="GPU Chunks"):
            decoded_items = [fut.result() for fut in next_futs]

            if c_idx + 1 < len(chunk_starts):
                n_start = chunk_starts[c_idx + 1]
                next_futs = [
                    pool.submit(decode_media_for_tagger, p, 5)
                    for p in files[n_start : n_start + chunk_size]
                ]
            else:
                next_futs = []

            flat_arrays: list[np.ndarray] = []
            slices: list[tuple[Path, int, int]] = []
            for src_path, arrays in decoded_items:
                if not arrays:
                    print(f"\n[WARNING] Could not decode {src_path}. Skipping.")
                    continue
                start_i = len(flat_arrays)
                flat_arrays.extend(arrays)
                slices.append((src_path, start_i, len(flat_arrays)))

            if not flat_arrays:
                continue

            probs_list = tagger.infer_batch(flat_arrays, args.batch)
            for src_path, start_i, end_i in slices:
                preds = aggregate_frame_predictions(
                    probs_list[start_i:end_i],
                    tagger,
                    gen_threshold=args.gen_threshold,
                    char_threshold=args.char_threshold,
                    rating_weights=rating_weights,
                    group_info=group_info,
                )

                final_tags: list[str] = []
                if args.dir_as_artist:
                    artist_name = src_path.parent.name.lower().replace(" ", "_")
                    final_tags.append(f"artist:{artist_name}")

                final_tags.extend(f"{char_prefix}{c}" for c in preds.approved_chars)
                final_tags.extend(f"{tag_prefix}{g}" for g in preds.approved_gens)

                sidecar_path = src_path.with_suffix(".txt")
                sidecar_path.write_text(", ".join(final_tags), encoding="utf-8")
                written_count += 1

    return written_count


def run_auto_tagger(args) -> None:
    """Main CLI entry point for auto-tag."""
    if getattr(args, "path", None):
        args.images = args.images or args.path
        args.videos = args.videos or args.path

    if not getattr(args, "images", None) and not getattr(args, "videos", None):
        print("[ERROR] You must provide at least one target path: --path, --images, or --videos")
        return

    img_dir = Path(args.images).resolve() if getattr(args, "images", None) else None
    vid_dir = Path(args.videos).resolve() if getattr(args, "videos", None) else None

    missing_sidecars: list[Path] = []
    if img_dir and vid_dir and img_dir == vid_dir:
        if not img_dir.is_dir():
            print(f"[ERROR] Invalid media directory: {img_dir}")
            return
        missing_sidecars.extend(_collect_missing_sidecars(img_dir, IMAGE_EXTS | VIDEO_EXTS))
    else:
        if img_dir:
            if not img_dir.is_dir():
                print(f"[ERROR] Invalid image directory: {img_dir}")
                return
            missing_sidecars.extend(_collect_missing_sidecars(img_dir, IMAGE_EXTS))
        if vid_dir:
            if not vid_dir.is_dir():
                print(f"[ERROR] Invalid video directory: {vid_dir}")
                return
            missing_sidecars.extend(_collect_missing_sidecars(vid_dir, VIDEO_EXTS))

    if not missing_sidecars:
        print("[✓] All target files already have sidecars. No action needed.")
        return

    missing_sidecars = sort_paths_by_disk_order(missing_sidecars)
    threads = getattr(args, "threads", max(1, (os.cpu_count() or 1) // 2))

    print(f"[INFO] Found {len(missing_sidecars):,} files missing sidecars. Checking DB status...")
    with ThreadPoolExecutor(max_workers=threads) as pool:
        md5_list = list(
            tqdm.tqdm(
                pool.map(extract_or_compute_md5, missing_sidecars),
                total=len(missing_sidecars),
                desc="Extracting Hashes",
                leave=False,
            )
        )
    md5_by_file = dict(zip(missing_sidecars, md5_list, strict=True))

    db_config = get_shimmie_db_credentials(args.spath) if getattr(args, "spath", None) else None
    existing_hashes = get_existing_shimmie_hashes(md5_by_file.values(), db_config)

    known_files: list[Path] = []
    unknown_files: list[Path] = []

    with ThreadPoolExecutor(max_workers=threads) as pool:
        res_iter = pool.map(
            lambda f: resolve_post(
                f,
                cache_path=CACHE_PATH,
                existing_hashes=existing_hashes,
                precomputed_md5=md5_by_file[f],
            ),
            missing_sidecars,
        )
        for media_file, res in zip(
            missing_sidecars,
            tqdm.tqdm(res_iter, total=len(missing_sidecars), desc="Querying Cache & DB"),
            strict=True,
        ):
            if res.exists == "error":
                continue
            if res.exists or post_has_tags(res.post):
                known_files.append(media_file)
            else:
                unknown_files.append(media_file)

    print("\n[INFO] Segregation complete:")
    print(f"  - Known (Receives 'tagai:' shield): {len(known_files):,}")
    print(f"  - Unknown (Fully trusted AI tags):  {len(unknown_files):,}\n")

    if not known_files and not unknown_files:
        return

    tagger = SmilingWolfTagger(args.model)
    rating_weights = load_rating_weights()
    child_to_parents, _, _, sex_group_tags = load_tag_groups_metadata()
    group_info = (child_to_parents, sex_group_tags)
    total_created = 0

    if known_files:
        print("=== Processing Known Files ===")
        total_created += _tag_file_group(
            known_files, tagger, "tagai:", args, threads, rating_weights, group_info
        )

    if unknown_files:
        print("\n=== Processing Unknown Files ===")
        total_created += _tag_file_group(
            unknown_files, tagger, "", args, threads, rating_weights, group_info
        )

    print(f"\n[✓] Sidecar generation complete! Created {total_created:,} new files.")
