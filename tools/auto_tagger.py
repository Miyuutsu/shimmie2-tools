"""Tool to automatically generate missing .txt sidecars for images and videos using AI and DB state."""

from collections import Counter, defaultdict
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile

import tqdm

from functions.common import (
	CACHE_PATH,
	IMAGE_EXTS,
	SUBMODULE_PATH,
	TAG_DB_PATH,
	VIDEO_EXTS,
	extract_or_compute_md5,
)
from functions.db_cache import (
	get_existing_shimmie_hashes,
	get_shimmie_db_credentials,
	post_has_tags,
	resolve_post,
)
from functions.media import extract_video_frames
from functions.tags_curation import load_tag_groups_metadata

SMIN_QUESTIONABLE = 51  # Scores > 50 (smax default) fall under questionable/explicit


def _load_rating_weights() -> dict[str, int]:
	"""Loads tag rating weights from tag_rating_dominant.db."""
	weights: dict[str, int] = {}
	if not TAG_DB_PATH.is_file():
		return weights
	with sqlite3.connect(TAG_DB_PATH) as conn:
		for row in conn.execute('SELECT tag_name, dominant_rating FROM dominant_tag_ratings'):
			if len(row) >= 2 and row[0]:
				weights[row[0].strip().lower()] = int(row[1])
	return weights


def _load_verbose_group_parents() -> tuple[dict[str, set[str]], set[str]]:
	"""Loads shared parent expansion rules and sex category tags from tag_groups.json."""
	child_to_parents, _, _, sex_group_tags = load_tag_groups_metadata()
	return child_to_parents, sex_group_tags


def _collect_missing_sidecars(target_dir: Path, allowed_extensions: set[str]) -> list[Path]:
	"""Finds all valid media files missing a .txt sidecar."""
	if not target_dir or not target_dir.is_dir():
		return []
	all_files = [
		f for f in target_dir.rglob('*')
		if f.is_file() and 'thumbnails' not in f.parts and f.suffix.lower() in allowed_extensions
	]
	return [
		f for f in all_files
		if not f.with_suffix('.txt').exists() and not f.with_name(f'{f.name}.txt').exists()
	]


def _stage_media_files(media_list: list[Path], stage_dir: Path) -> dict[int, dict[str, Path | int | list[str]]]:
	"""Stages symlinks for static images and extracts up to 5 frames for videos/GIFs."""
	registry: dict[int, dict[str, Path | int | list[str]]] = {}
	for idx, media_path in enumerate(tqdm.tqdm(media_list, desc=f'Staging {stage_dir.name}', leave=False)):
		is_video = media_path.suffix.lower() in VIDEO_EXTS
		if not is_video:
			stem = f'item_{idx}_f0'
			sym_path = stage_dir / f'{stem}{media_path.suffix}'
			os.symlink(media_path.resolve(), sym_path)
			registry[idx] = {'source': media_path, 'frames': 1, 'stems': [stem]}
			continue

		try:
			extracted = extract_video_frames(media_path, max_frames=5)
		except Exception as e:
			print(f'\n[WARNING] Could not extract frames from {media_path}: {e}')
			continue

		if not extracted:
			print(f'\n[WARNING] 0 frames decoded for {media_path}. Skipping.')
			continue

		stems: list[str] = []
		for f_idx, pil_img in enumerate(extracted):
			stem = f'item_{idx}_f{f_idx}'
			frame_path = stage_dir / f'{stem}.jpg'
			rgb_img = pil_img.convert('RGB') if pil_img.mode != 'RGB' else pil_img
			rgb_img.save(frame_path, 'JPEG', quality=95)
			stems.append(stem)

		registry[idx] = {'source': media_path, 'frames': len(stems), 'stems': stems}

	return registry


def _consolidate_and_write_sidecars(
	stage_dir: Path,
	registry: dict[int, dict[str, Path | int | list[str]]],
	tag_prefix: str,
	dir_as_artist: bool,
	rating_weights: dict[str, int],
	group_info: tuple[dict[str, set[str]], set[str]],
) -> int:
	"""Reads JSON outputs from SD-Tag-Editor, applies frame consensus rules, and writes .txt sidecars."""
	child_to_bases, sex_group_tags = group_info
	written_count = 0

	for info in registry.values():
		src_path: Path = info['source']  # type: ignore[assignment]
		stems: list[str] = info['stems']  # type: ignore[assignment]

		char_counts: Counter[str] = Counter()
		char_max_conf: dict[str, float] = defaultdict(float)
		gen_counts: Counter[str] = Counter()
		gen_max_conf: dict[str, float] = defaultdict(float)
		valid_frames = 0

		for stem in stems:
			json_path = stage_dir / f'{stem}.json'
			if not json_path.is_file():
				continue

			valid_frames += 1
			with json_path.open('r', encoding='utf-8') as f:
				data = json.load(f)

			for c_tag, conf in data.get('character', {}).items():
				clean_c = c_tag.replace(' ', '_').lower()
				char_counts[clean_c] += 1
				char_max_conf[clean_c] = max(char_max_conf[clean_c], float(conf))

			for g_tag, conf in data.get('general', {}).items():
				clean_g = g_tag.replace(' ', '_').lower()
				gen_counts[clean_g] += 1
				gen_max_conf[clean_g] = max(gen_max_conf[clean_g], float(conf))

		if valid_frames == 0:
			continue

		char_min_hits = 1 if valid_frames == 1 else min(3, valid_frames)
		gen_min_hits = 1 if valid_frames == 1 else min(2, valid_frames)
		qe_min_hits = 1 if valid_frames == 1 else min(2, valid_frames)

		approved_chars = [c for c, cnt in char_counts.items() if cnt >= char_min_hits]
		approved_chars.sort(key=lambda c: char_max_conf[c], reverse=True)

		approved_gens: set[str] = set()
		for g, cnt in gen_counts.items():
			is_qe = rating_weights.get(g, 0) >= SMIN_QUESTIONABLE or g in sex_group_tags
			required_hits = qe_min_hits if is_qe else gen_min_hits

			if cnt >= required_hits:
				approved_gens.add(g)
				for parent_base in child_to_bases.get(g, ()):
					approved_gens.add(parent_base)
					gen_max_conf[parent_base] = max(gen_max_conf[parent_base], gen_max_conf[g])

		sorted_gens = sorted(approved_gens, key=lambda g: gen_max_conf[g], reverse=True)

		final_tags: list[str] = []
		if dir_as_artist:
			artist_name = src_path.resolve().parent.name.lower().replace(' ', '_')
			final_tags.append(f'artist:{artist_name}')

		final_tags.extend(f'{tag_prefix}{c}' for c in approved_chars)
		final_tags.extend(f'{tag_prefix}{g}' for g in sorted_gens)

		sidecar_path = src_path.with_suffix('.txt')
		sidecar_path.write_text(', '.join(final_tags), encoding='utf-8')
		written_count += 1

	return written_count


def run_auto_tagger(args) -> None:
	"""Main CLI entry point for auto-tag."""
	if not getattr(args, 'images', None) and not getattr(args, 'videos', None):
		print('[ERROR] You must provide at least one target path: --images or --videos')
		return

	venv_python = SUBMODULE_PATH / 'venv' / 'bin' / 'python'
	if not venv_python.exists():
		print('[ERROR] SD-Tag-Editor venv not found.')
		return

	missing_sidecars: list[Path] = []
	if getattr(args, 'images', None):
		img_dir = Path(args.images)
		if not img_dir.is_dir():
			print(f'[ERROR] Invalid image directory: {img_dir}')
			return
		missing_sidecars.extend(_collect_missing_sidecars(img_dir, IMAGE_EXTS))

	if getattr(args, 'videos', None):
		vid_dir = Path(args.videos)
		if not vid_dir.is_dir():
			print(f'[ERROR] Invalid video directory: {vid_dir}')
			return
		missing_sidecars.extend(_collect_missing_sidecars(vid_dir, VIDEO_EXTS))

	if not missing_sidecars:
		print('[✓] All target files already have sidecars. No action needed.')
		return

	print(f'[INFO] Found {len(missing_sidecars):,} files missing sidecars. Checking DB status...')
	md5_by_file = {
		f: extract_or_compute_md5(f)
		for f in tqdm.tqdm(missing_sidecars, desc='Extracting Hashes', leave=False)
	}

	db_config = get_shimmie_db_credentials(args.spath) if getattr(args, 'spath', None) else None
	existing_hashes = get_existing_shimmie_hashes(md5_by_file.values(), db_config)

	known_files: list[Path] = []
	unknown_files: list[Path] = []

	for media_file in tqdm.tqdm(missing_sidecars, desc='Querying Cache & DB'):
		res = resolve_post(
			media_file,
			cache_path=CACHE_PATH,
			existing_hashes=existing_hashes,
			precomputed_md5=md5_by_file[media_file],
		)
		if res.exists == 'error':
			continue
		if res.exists or post_has_tags(res.post):
			known_files.append(media_file)
		else:
			unknown_files.append(media_file)

	print('\n[INFO] Segregation complete:')
	print(f"  - Known (Receives 'tagai:' shield): {len(known_files):,}")
	print(f'  - Unknown (Fully trusted AI tags):  {len(unknown_files):,}\n')

	rating_weights = _load_rating_weights()
	group_info = _load_verbose_group_parents()

	base_cmd = [
		str(venv_python), '-u', str(SUBMODULE_PATH / 'run.py'),
		f'--model={args.model}', f'--batch_size={args.batch}',
		f'--gen_threshold={args.gen_threshold}', f'--char_threshold={args.char_threshold}',
		'--output_json',
	]

	total_created = 0

	with tempfile.TemporaryDirectory(prefix='auto_tag_') as tmpdir:
		tmp_path = Path(tmpdir)
		known_dir = tmp_path / 'known'
		unknown_dir = tmp_path / 'unknown'
		known_dir.mkdir()
		unknown_dir.mkdir()

		if known_files:
			print('=== Processing Known Files ===')
			if known_reg := _stage_media_files(known_files, known_dir):
				subprocess.run([*base_cmd, str(known_dir)], check=True)
				total_created += _consolidate_and_write_sidecars(
					known_dir, known_reg, 'tagai:', args.dir_as_artist, rating_weights, group_info
				)

		if unknown_files:
			print('\n=== Processing Unknown Files ===')
			if unknown_reg := _stage_media_files(unknown_files, unknown_dir):
				subprocess.run([*base_cmd, str(unknown_dir)], check=True)
				total_created += _consolidate_and_write_sidecars(
					unknown_dir, unknown_reg, '', args.dir_as_artist, rating_weights, group_info
				)

	print(f'\n[✓] Sidecar generation complete! Created {total_created:,} new files.')
