"""Batch CSV and thumbnail generator for importing media into Shimmie2."""

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import csv
from pathlib import Path
import warnings

from PIL import Image
import tqdm

from functions.common import (
	CACHE_PATH,
	IMAGE_EXTS,
	TAGMAP_DB_PATH,
	VIDEO_EXTS,
	BlacklistConfig,
	expand_tags_for_blacklist,
	extract_or_compute_md5,
	load_blacklist,
)
from functions.db_cache import (
	Mappings,
	ResolutionData,
	get_cache_conn,
	get_existing_shimmie_hashes,
	get_shimmie_db_credentials,
	init_posts_cache,
	load_mappings,
	load_tagmap,
	resolve_post,
	save_post_to_cache,
)
from functions.media import get_image_resolution, get_video_resolution, process_webp
from functions.source_resolver import resolve_best_source
from functions.tags_curation import (
	apply_tag_curation,
	calculate_rating,
	get_curation_context,
	get_sidecar_tags,
)
from functions.tags_mining import mine_tag_equivalencies

Image.MAX_IMAGE_PIXELS = None
warnings.simplefilter('ignore', Image.DecompressionBombWarning)
warnings.filterwarnings('ignore', '(?s).*Corrupt EXIF data.*', category=UserWarning)

RESOLUTION_TAGS = {
	'lowres', 'highres', 'absurdres',
	'incredibly_absurdres', 'wide_image', 'tall_image',
}


def collect_files(
	image_path: str | Path | None,
	video_path: str | Path | None,
	batch_size: int,
) -> tuple[list[Path], list[list[Path]]]:
	"""Finds all valid media files from provided paths, resolves stem duplicates, and chunks them."""
	files: list[Path] = []

	if image_path:
		img_dir = Path(image_path)
		if img_dir.is_dir():
			files.extend(
				f for f in img_dir.rglob('*')
				if f.is_file() and f.suffix.lower() in IMAGE_EXTS and f.stat().st_size > 0
			)

	if video_path:
		vid_dir = Path(video_path)
		if vid_dir.is_dir():
			files.extend(
				f for f in vid_dir.rglob('*')
				if f.is_file() and f.suffix.lower() in VIDEO_EXTS and f.stat().st_size > 0
			)

	grouped_files: dict[Path, list[Path]] = defaultdict(list)
	for f in files:
		if 'thumbnails' not in f.parts:
			grouped_files[f.with_suffix('')].append(f)

	final_files: list[Path] = []
	for stem, group in grouped_files.items():
		if len(group) == 1:
			final_files.append(group[0])
			continue

		with_sidecars = [
			f for f in group
			if f.with_suffix('.txt').is_file() or f.with_name(f'{f.name}.txt').is_file()
		]
		if len(with_sidecars) == 1:
			final_files.append(with_sidecars[0])
		else:
			print(f'[WARNING] Skipping {stem}.* - Ambiguous multiple formats without unique sidecar.')

	batches = [final_files[i : i + batch_size] for i in range(0, len(final_files), batch_size)]
	return final_files, batches


def clean_resolution_tags(tags: list[str], media_path: Path) -> list[str]:
	"""Strips stale resolution tags and appends accurate ones based on dimensions and aspect ratio."""
	res_tags = [t for t in tags if t not in RESOLUTION_TAGS]

	width, height = (
		get_video_resolution(media_path)
		if media_path.suffix.lower() in VIDEO_EXTS
		else get_image_resolution(media_path)
	)
	if not width or not height:
		return res_tags

	pixels = width * height
	ratio = width / height

	if width > 10000 or height > 10000:
		res_tags.append('incredibly_absurdres')

	if pixels >= 7680000:
		res_tags.append('absurdres')
	elif pixels >= 3686400:
		res_tags.append('highres')
	elif pixels <= 589824:
		res_tags.append('lowres')

	if ratio >= 4.0:
		res_tags.append('wide_image')
	elif ratio <= 0.25:
		res_tags.append('tall_image')

	return res_tags


def compile_metadata(
	image: Path,
	post: dict[str, str | list[str]],
	mappings: Mappings,
	args,
	dynamic_mappings: dict[str, str],
) -> tuple[str, str, list[str], str | None]:
	"""Curates tags, resolves the highest-priority source URL, and calculates the post rating."""
	tags: list[str] = []
	tags.extend(args.pretags)
	tags.extend(post.get('general', ()))  # type: ignore[arg-type]
	tags.extend(f'character:{t}' for t in post.get('character', ()))
	tags.extend(f'series:{t}' for t in post.get('series', ()))
	tags.extend(f'artist:{t}' for t in post.get('artist', ()))
	tags.extend(get_sidecar_tags(image))

	tags = clean_resolution_tags(tags, image)

	ctx = get_curation_context(mappings)
	apply_tag_curation(tags, dynamic_mappings, context=ctx)

	rescued_sources = [
		t.split(':', 1)[1].strip()
		for t in tags
		if t.startswith('source:') and len(t) > 7
	]
	best_source = resolve_best_source(post.get('source'), image, extra_sources=rescued_sources)
	if best_source and f'source:{best_source}' not in tags:
		tags.append(f'source:{best_source}')

	rating = calculate_rating(tags, post.get('rating'), mappings.rating, args.smax, args.qmax)
	tags = [t for t in tags if t.lower() not in {'rating:e', 'rating:q', 'rating:s'}]

	if len(tags) < 15:
		tags.append('tagme')
	if not any(tag.startswith('artist:') for tag in tags):
		tags.append('artist:tagme')
	if not any(tag.startswith('character:') for tag in tags):
		tags.append('character:tagme')
	if not any(tag.startswith('series:') for tag in tags):
		tags.append('series:tagme')

	return ', '.join(sorted(set(tags))), rating, tags, best_source


def _get_thumbnail_path(img: Path, args) -> Path:
	"""Determines the output thumbnail path for a given media file."""
	if args.image_path and img.is_relative_to(args.image_path):
		return Path(args.image_path) / 'thumbnails' / img.relative_to(args.image_path)
	return Path(args.video_path) / 'thumbnails' / img.relative_to(args.video_path)


def _matches_blacklist(tag_list: list[str], blacklist_cfg: BlacklistConfig | None) -> list[str]:
	"""Evaluates a curated tag list against the blacklist, returning up to 5 matched rule strings."""
	if not blacklist_cfg or not blacklist_cfg.rules:
		return []

	clean_tags = expand_tags_for_blacklist(tag_list)
	if blacklist_cfg.preserve_tags and not blacklist_cfg.preserve_tags.isdisjoint(clean_tags):
		return []

	matched_rules: list[str] = []
	for rule in blacklist_cfg.rules:
		has_all_pos = all(p in clean_tags for p in rule['pos'])
		has_any_neg = any(n in clean_tags for n in rule['neg'])
		if has_all_pos and not has_any_neg:
			matched_rules.append(str(rule['raw']))
			if len(matched_rules) >= 5:
				break
	return matched_rules


def process_image_result(
	image: Path,
	res_data: ResolutionData,
	args,
	mappings: Mappings,
	dynamic_mappings: dict[str, str],
	blacklist_cfg: BlacklistConfig | None,
) -> tuple[list[str] | None, str | None, tuple[str, str] | None]:
	"""Processes a single resolved media file into a CSV row, thumbnail key, or skip log entry."""
	if res_data.exists == 'error':
		print(f'\n{image} skipped due to error!')
		return None, None, ('corrupt or unreadable', image.name)

	base_path = (
		Path(args.image_path)
		if args.image_path and image.is_relative_to(args.image_path)
		else Path(args.video_path)
	)
	rel_path = image.relative_to(base_path)
	thumb_path = Path(args.prefix) / 'thumbnails' / rel_path if args.thumbnail else ''

	if res_data.exists:
		thumb_file = _get_thumbnail_path(image, args)
		return None, str(thumb_file) if args.thumbnail else None, None

	tag_str, rating, tag_list, best_source = compile_metadata(
		image, res_data.post, mappings, args, dynamic_mappings
	)

	if matched_rules := _matches_blacklist(tag_list, blacklist_cfg):
		skip_entry = f"{image.name} | {' | '.join(matched_rules)}"
		return None, None, ('blacklist', skip_entry)

	if args.update_cache:
		save_post_to_cache(res_data, rating, tag_list, best_source, CACHE_PATH)

	row = [
		f'{args.prefix}/{rel_path}',
		tag_str,
		best_source or '',
		rating,
		str(thumb_path) if args.thumbnail else '',
	]
	return row, None, None


def _print_summary(args) -> None:
	"""Prints the active make-csv configuration."""
	map_target = args.use_map_csv if getattr(args, 'use_map_csv', None) else TAGMAP_DB_PATH
	print('=== Tagger Run Summary ===')
	if args.image_path:
		print(f'📁  Images:          {args.image_path}')
	if args.video_path:
		print(f'🎞️  Videos:          {args.video_path}')
	print(f'📥  Input Cache:     {CACHE_PATH}')
	print(f'🗺️  Tagmap Source:   {map_target}')
	print(f'🗄️  Update Cache:    {args.update_cache}')
	print(f'📦  Batch Size:      {args.batch}')
	print(f'🧵  Threads:         {args.threads}')
	print(f'📂  Prefix:          {args.prefix}\n')


def write_output(base_path: str | Path, rows: list[list[str]], chunk_size: int) -> None:
	"""Writes the CSV file(s), splitting into numbered parts if len(rows) exceeds chunk_size."""
	if not rows:
		return

	out_dir = Path(base_path)
	if len(rows) <= chunk_size:
		csv_path = out_dir / 'import.csv'
		with csv_path.open('w', encoding='utf-8', newline='') as f:
			writer = csv.writer(f, quoting=csv.QUOTE_ALL)
			writer.writerows(rows)
		print(f'\n[✓] Shimmie CSV written to {csv_path}')
		return

	chunks = [rows[i : i + chunk_size] for i in range(0, len(rows), chunk_size)]
	print(f'\n[INFO] Splitting {len(rows):,} rows into {len(chunks):,} chunked CSVs...')

	for idx, chunk in enumerate(chunks, 1):
		csv_path = out_dir / f'import_part_{idx:03d}.csv'
		with csv_path.open('w', encoding='utf-8', newline='') as f:
			writer = csv.writer(f, quoting=csv.QUOTE_ALL)
			writer.writerows(chunk)

	print(f'[✓] Successfully wrote {len(chunks):,} chunked files to {out_dir}')


def write_skips_log(base_path: str | Path, skips: list[tuple[str, str]]) -> None:
	"""Writes a log of skipped files grouped by reason."""
	if not skips:
		return

	log_path = Path(base_path) / 'skipped_files.txt'
	grouped: dict[str, list[str]] = defaultdict(list)
	for reason, filename in skips:
		grouped[reason].append(filename)

	with log_path.open('w', encoding='utf-8') as f:
		for reason, filenames in grouped.items():
			f.write(f'=== Skipped due to {reason} ===\n')
			for name in sorted(filenames):
				f.write(f'{name}\n')
			f.write('\n')

	print(f'[INFO] Skipped files log written to {log_path}')


def _resolve_batch_metadata(
	batch: list[Path],
	threads: int,
	existing_hashes: set[str],
	md5_by_file: dict[Path, str],
) -> list[ResolutionData]:
	"""Resolves post metadata across worker threads for a single batch."""
	with ThreadPoolExecutor(max_workers=threads) as executor:
		resolver = executor.map(
			lambda img: resolve_post(
				img,
				cache_path=CACHE_PATH,
				existing_hashes=existing_hashes,
				precomputed_md5=md5_by_file.get(img),
			),
			batch,
		)
		return list(tqdm.tqdm(resolver, total=len(batch), desc='Resolving', position=2, leave=False))


def _generate_thumbnails(tasks: list[tuple[Path, Path]], threads: int) -> None:
	"""Generates WebP thumbnails in parallel across worker threads."""
	if not tasks:
		return
	with ThreadPoolExecutor(max_workers=threads) as imgpro:
		list(imgpro.map(process_webp, tasks))


def run(args) -> None:
	"""Main execution flow for make-csv."""
	init_posts_cache(CACHE_PATH)
	if args.image_path and not Path(args.image_path).is_dir():
		raise FileNotFoundError(f'Image path not found: {args.image_path}')
	if args.video_path and not Path(args.video_path).is_dir():
		raise FileNotFoundError(f'Video path not found: {args.video_path}')

	_print_summary(args)

	blacklist_cfg: BlacklistConfig | None = None
	if getattr(args, 'blacklist', None):
		blacklist_path = Path(args.blacklist)
		if blacklist_path.is_file() or args.blacklist != 'blacklist.txt':
			blacklist_cfg = load_blacklist(blacklist_path)
			print(
				f'[INFO] Loaded {len(blacklist_cfg.rules):,} blacklist rules and '
				f'{len(blacklist_cfg.preserve_tags):,} whitelist tags.'
			)

	mappings = load_mappings()
	files, batches = collect_files(args.image_path, args.video_path, args.batch)

	if args.create_map_csv:
		db_conn = get_shimmie_db_credentials(args.spath)
		with get_cache_conn(CACHE_PATH) as sqlite_conn:
			mine_tag_equivalencies(
				image_list=files,
				conns=(db_conn, sqlite_conn),
				output_path=args.create_map_csv,
				mappings=mappings,
			)
		print('Mining complete. Exiting before standard import processing.')
		return

	md5_by_file: dict[Path, str] = {}
	existing_hashes: set[str] = set()
	if getattr(args, 'skip_existing', False) and getattr(args, 'spath', None):
		print('[INFO] Extracting MD5 hashes to check against Shimmie PostgreSQL database...')
		md5_by_file = {
			f: extract_or_compute_md5(f)
			for f in tqdm.tqdm(files, desc='Hashing Files', leave=False)
		}
		db_config = get_shimmie_db_credentials(args.spath)
		existing_hashes = get_existing_shimmie_hashes(md5_by_file.values(), db_config)
		print(f'[INFO] Found {len(existing_hashes):,} existing files in Shimmie to skip.')

	dynamic_mappings = load_tagmap(getattr(args, 'use_map_csv', None))
	print(f'[INFO] Loaded {len(dynamic_mappings):,} dynamic tag mappings.')

	csv_rows: list[list[str]] = []
	existing_thumbs: set[str] = set()
	all_skips: list[tuple[str, str]] = []

	for batch in tqdm.tqdm(batches, desc='Media batches', position=1, leave=False):
		results = _resolve_batch_metadata(batch, args.threads, existing_hashes, md5_by_file)
		thumb_tasks: list[tuple[Path, Path]] = []

		for img, res_data in zip(batch, results, strict=True):
			row, thumb_key, skip_info = process_image_result(
				img, res_data, args, mappings, dynamic_mappings, blacklist_cfg
			)
			if skip_info:
				all_skips.append(skip_info)
			if thumb_key:
				existing_thumbs.add(thumb_key)
			if row:
				csv_rows.append(row)
				if args.thumbnail:
					t_src = _get_thumbnail_path(img, args)
					if str(t_src) not in existing_thumbs and not t_src.is_file():
						thumb_tasks.append((img, t_src))

		_generate_thumbnails(thumb_tasks, args.threads)

	csv_rows.sort()
	out_dir = args.image_path or args.video_path
	write_output(out_dir, csv_rows, args.chunk_size)

	if all_skips:
		write_skips_log(out_dir, all_skips)

	print(f'\n[✓] Processed {len(files):,} file(s) across {len(batches):,} batch(es).')
