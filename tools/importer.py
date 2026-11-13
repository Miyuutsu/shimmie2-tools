"""Native Shimmie2 importer supporting direct batch imports, CSV imports, and duplicate MD5 tag merging."""

from concurrent.futures import ThreadPoolExecutor
import csv
from dataclasses import dataclass
from pathlib import Path
import shutil

import av
from PIL import Image
import psycopg2
import psycopg2.extras
import tqdm

from functions.common import VIDEO_EXTS, extract_or_compute_md5
from functions.db_cache import get_shimmie_db_credentials
from functions.media import get_image_resolution, get_video_resolution, process_webp
from functions.source_resolver import get_source_score, is_recognized_prefix

MIME_BY_EXT = {
	'avif': 'image/avif',
	'f4p': 'video/x-flv',
	'f4v': 'video/x-flv',
	'flv': 'video/x-flv',
	'gif': 'image/gif',
	'jpg': 'image/jpeg',
	'jxl': 'image/jxl',
	'm4v': 'video/mp4',
	'mp4': 'video/mp4',
	'ogv': 'video/ogg',
	'png': 'image/png',
	'webm': 'video/webm',
	'webp': 'image/webp',
}


@dataclass(slots=True)
class ImportItem:
	"""Represents a single media item prepared for Shimmie2 import or tag merge."""

	file_path: Path
	tags: list[str]
	source: str
	rating: str
	thumb_path: Path | None = None
	md5: str = ''


@dataclass(slots=True)
class MediaSpecs:
	"""Holds physical file metadata matching Shimmie2's `public.images` columns."""

	filename: str
	filesize: int
	ext: str
	width: int
	height: int
	mime: str
	is_image: bool
	is_video: bool
	has_audio: bool
	is_lossless: bool
	length_ms: int | None
	video_codec: str | None


def _normalize_ext(suffix: str) -> str:
	"""Normalizes a file extension to Shimmie2's 4-char lowercase format (e.g. .jpeg -> jpg)."""
	ext = suffix.lower().lstrip('.')
	return 'jpg' if ext == 'jpeg' else ext[:4]


def _inspect_webp_or_gif(file_path: Path, ext: str) -> tuple[bool, bool, bool, int, int]:
	"""Returns (is_image, is_video, is_lossless, width, height) for GIF and WebP files."""
	is_animated = False
	width, height = 0, 0
	is_lossless = ext == 'gif'

	if ext == 'webp':
		try:
			with file_path.open('rb') as f:
				header = f.read(64)
			is_lossless = b'VP8L' in header
		except Exception:
			pass

	try:
		with Image.open(file_path) as im:
			width, height = im.size
			is_animated = bool(getattr(im, 'is_animated', False) and getattr(im, 'n_frames', 1) > 1)
	except Exception:
		w, h = get_image_resolution(file_path)
		width, height = w or 0, h or 0

	return (not is_animated), is_animated, is_lossless, width, height


def _inspect_video_container(file_path: Path) -> tuple[int, int, bool, int | None, str | None]:
	"""Extracts (width, height, has_audio, length_ms, video_codec) from a video container via PyAV."""
	width, height = 0, 0
	has_audio = False
	length_ms: int | None = None
	video_codec: str | None = None

	try:
		with av.open(str(file_path)) as container:
			has_audio = bool(container.streams.audio)
			if container.streams.video:
				v_stream = container.streams.video[0]
				width = v_stream.width or 0
				height = v_stream.height or 0
				video_codec = v_stream.codec_context.name if v_stream.codec_context else None
				if v_stream.duration and v_stream.time_base:
					length_ms = int(float(v_stream.duration * v_stream.time_base) * 1000)

			if length_ms is None and container.duration and container.duration > 0:
				length_ms = int(container.duration / 1000)
	except Exception:
		pass

	if not width or not height:
		w, h = get_video_resolution(file_path)
		width, height = w or 0, h or 0

	return width, height, has_audio, length_ms, video_codec


def probe_media_specs(file_path: Path) -> MediaSpecs | None:
	"""Extracts Shimmie2-compatible media column values for a physical file."""
	if not file_path.is_file():
		return None

	filesize = file_path.stat().st_size
	if filesize <= 0:
		return None

	ext = _normalize_ext(file_path.suffix)
	mime = MIME_BY_EXT.get(ext, 'application/octet-stream')
	filename = file_path.name[:255]

	if ext in {'gif', 'webp'}:
		is_image, is_video, is_lossless, width, height = _inspect_webp_or_gif(file_path, ext)
		if not width or not height:
			return None
		return MediaSpecs(
			filename=filename,
			filesize=filesize,
			ext=ext,
			width=width,
			height=height,
			mime=mime,
			is_image=is_image,
			is_video=is_video,
			has_audio=False,
			is_lossless=is_lossless,
			length_ms=None,
			video_codec=None,
		)

	if file_path.suffix.lower() in VIDEO_EXTS:
		width, height, has_audio, length_ms, video_codec = _inspect_video_container(file_path)
		if not width or not height:
			return None
		return MediaSpecs(
			filename=filename,
			filesize=filesize,
			ext=ext,
			width=width,
			height=height,
			mime=mime,
			is_image=False,
			is_video=True,
			has_audio=has_audio,
			is_lossless=False,
			length_ms=length_ms,
			video_codec=video_codec[:512] if video_codec else None,
		)

	w, h = get_image_resolution(file_path)
	if not w or not h:
		return None

	return MediaSpecs(
		filename=filename,
		filesize=filesize,
		ext=ext,
		width=w,
		height=h,
		mime=mime,
		is_image=True,
		is_video=False,
		has_audio=False,
		is_lossless=(ext == 'png'),
		length_ms=None,
		video_codec=None,
	)


def _resolve_user_id(cur, username: str | None) -> int:
	"""Resolves the Shimmie2 user_id from public.users, raising RuntimeError if missing."""
	if not username or not username.strip():
		raise RuntimeError(
			'No username found in shimmie.conf.php or --user argument. '
			'Pass --user <shimmie_username> or verify your Shimmie2 configuration.'
		)

	cur.execute('SELECT id FROM users WHERE LOWER(name) = LOWER(%s) LIMIT 1', (username.strip(),))
	if row := cur.fetchone():
		return int(row[0])

	raise RuntimeError(
		f"User '{username}' not found in Shimmie2 'public.users' table. "
		'Verify your shimmie.conf.php credentials or pass a valid Shimmie2 username via --user.'
	)


def _stage_new_file_and_thumb(
	item: ImportItem,
	shimmie_root: Path,
) -> tuple[ImportItem, MediaSpecs] | None:
	"""Copies a new media file and its WebP thumbnail into Shimmie2's data/ fanout directories."""
	specs = probe_media_specs(item.file_path)
	if specs is None:
		print(f'\n[WARNING] Could not read media dimensions for {item.file_path}. Skipping.')
		return None

	p1, p2 = item.md5[0:2], item.md5[2:4]
	dst_img = shimmie_root / 'data' / 'images' / p1 / p2 / item.md5
	dst_thumb = shimmie_root / 'data' / 'thumbs' / p1 / p2 / item.md5

	dst_img.parent.mkdir(parents=True, exist_ok=True)
	dst_thumb.parent.mkdir(parents=True, exist_ok=True)

	if not dst_img.is_file():
		shutil.copy2(item.file_path, dst_img)

	if not dst_thumb.is_file():
		if item.thumb_path and item.thumb_path.is_file():
			shutil.copy2(item.thumb_path, dst_thumb)
		else:
			process_webp((item.file_path, dst_thumb))

	return item, specs


def _clean_merged_tags(existing_tags: set[str], incoming_tags: list[str]) -> list[str]:
	"""Merges incoming tags with existing DB tags, removing resolved tagme and redundant tagai:/unprefixed tags."""
	combined = (existing_tags | set(incoming_tags)) - {
		'tagme', 'artist:tagme', 'character:tagme', 'series:tagme',
	}

	prefixes = ('artist:', 'character:', 'series:', 'source:', 'language:', 'meta:')
	real_bases: set[str] = set()
	prefixed_bases: set[str] = set()

	for t in combined:
		if t.startswith('tagai:'):
			continue
		if not t.startswith(('booru:', 'source:', 'rating:')):
			real_bases.add(t)
			if is_recognized_prefix(t):
				base_val = t.split(':', 1)[1]
				real_bases.add(base_val)
				if t.startswith(prefixes):
					prefixed_bases.add(base_val)

	cleaned = [
		t for t in combined
		if not (not is_recognized_prefix(t) and t in prefixed_bases)
		and not (t.startswith('tagai:') and t.split(':', 1)[1] in real_bases)
	]

	if len(cleaned) < 15:
		cleaned.append('tagme')
	if not any(t.startswith('artist:') for t in cleaned):
		cleaned.append('artist:tagme')
	if not any(t.startswith('character:') for t in cleaned):
		cleaned.append('character:tagme')
	if not any(t.startswith('series:') for t in cleaned):
		cleaned.append('series:tagme')

	return sorted(set(cleaned))


def _ensure_tags_in_db(cur, tag_names: set[str], tag_id_map: dict[str, int]) -> None:
	"""Inserts any new tag strings into public.tags and updates tag_id_map in place."""
	missing = [t[:255] for t in tag_names if t and t[:255] not in tag_id_map]
	if not missing:
		return

	psycopg2.extras.execute_values(
		cur,
		'INSERT INTO tags (tag, count) VALUES %s ON CONFLICT (tag) DO NOTHING',
		[(t, 0) for t in missing],
		page_size=10000,
	)
	cur.execute('SELECT tag, id FROM tags WHERE tag = ANY(%s)', (missing,))
	tag_id_map.update(dict(cur.fetchall()))


def import_items_to_shimmie(
	items: list[ImportItem],
	spath: str | Path,
	username: str | None = None,
	threads: int = 4,
	skip_existing: bool = False,
) -> tuple[int, int]:
	"""Imports new media items and merges tags/sources onto existing MD5s in Shimmie2."""
	if not items:
		return 0, 0

	db_config = get_shimmie_db_credentials(spath)
	if not db_config:
		raise RuntimeError(f'Invalid or missing Shimmie2 DB configuration at {spath}')

	effective_user = username or str(db_config.get('user') or '')
	shimmie_root = Path(spath)

	# 1. Ensure MD5s are populated and deduplicate items within the batch by MD5
	by_md5: dict[str, ImportItem] = {}
	for item in tqdm.tqdm(items, desc='Verifying Hashes', leave=False):
		if not item.md5:
			item.md5 = extract_or_compute_md5(item.file_path)
		if item.md5 in by_md5:
			prev = by_md5[item.md5]
			merged = sorted(set(prev.tags) | set(item.tags))
			prev.tags = merged
			if item.source and (not prev.source or get_source_score(item.source) < get_source_score(prev.source)):
				prev.source = item.source
		else:
			by_md5[item.md5] = item

	with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
		user_id = _resolve_user_id(cur, effective_user)
		user_ip = '127.0.0.1'

		cur.execute('SELECT tag, id FROM tags')
		tag_id_map: dict[str, int] = dict(cur.fetchall())

		all_hashes = list(by_md5.keys())
		existing_rows: dict[str, tuple[int, str | None, str | None, set[str]]] = {}
		for idx in range(0, len(all_hashes), 10000):
			chunk = all_hashes[idx : idx + 10000]
			cur.execute(
				"""
				SELECT i.hash, i.id, i.source, i.rating, array_agg(t.tag)
				FROM images i
				LEFT JOIN image_tags it ON i.id = it.image_id
				LEFT JOIN tags t ON it.tag_id = t.id
				WHERE i.hash = ANY(%s)
				GROUP BY i.hash, i.id, i.source, i.rating
				""",
				(chunk,),
			)
			for hsh, img_id, src, rating, tag_arr in cur.fetchall():
				clean_h = hsh.strip()
				db_tags = {t for t in (tag_arr or ()) if t is not None}
				existing_rows[clean_h] = (img_id, src, rating, db_tags)

		new_candidates = [it for h, it in by_md5.items() if h not in existing_rows]
		existing_candidates = [] if skip_existing else [it for h, it in by_md5.items() if h in existing_rows]

		print(
			f'[INFO] Batch breakdown: {len(new_candidates):,} new files to import, '
			f'{len(existing_candidates):,} existing posts to merge.'
		)

		affected_tag_ids: set[int] = set()
		inserted_count = 0
		merged_count = 0

		# --- A. Process Brand-New Files ---
		if new_candidates:
			staged_results: list[tuple[ImportItem, MediaSpecs]] = []
			with ThreadPoolExecutor(max_workers=threads) as pool:
				futures = pool.map(lambda it: _stage_new_file_and_thumb(it, shimmie_root), new_candidates)
				for res in tqdm.tqdm(futures, total=len(new_candidates), desc='Copying Files & Thumbs'):
					if res is not None:
						staged_results.append(res)

			if staged_results:
				all_new_tags = {t[:255] for it, _ in staged_results for t in it.tags if t}
				_ensure_tags_in_db(cur, all_new_tags, tag_id_map)

				image_insert_rows = [
					(
						user_id,
						user_ip,
						specs.filename,
						specs.filesize,
						it.md5,
						specs.ext,
						it.source[:255] if it.source else None,
						specs.width,
						specs.height,
						specs.is_lossless,
						specs.is_video,
						specs.has_audio,
						specs.length_ms,
						specs.mime,
						specs.is_image,
						specs.video_codec,
						(it.rating or '?')[0],
					)
					for it, specs in staged_results
				]

				returned = psycopg2.extras.execute_values(
					cur,
					"""
					INSERT INTO images (
						owner_id, owner_ip, filename, filesize, hash, ext, source,
						width, height, lossless, video, audio, length, mime, image,
						video_codec, rating
					) VALUES %s
					ON CONFLICT (hash) DO NOTHING
					RETURNING id, hash
					""",
					image_insert_rows,
					page_size=5000,
					fetch=True,
				)
				hash_to_new_id = {h.strip(): img_id for img_id, h in returned}

				new_image_tags: list[tuple[int, int]] = []
				new_tag_histories: list[tuple[int, int, str, str]] = []
				new_source_histories: list[tuple[int, int, str, str]] = []

				for it, _ in staged_results:
					new_id = hash_to_new_id.get(it.md5)
					if new_id is None:
						continue
					inserted_count += 1
					clean_tags = sorted({t[:255] for t in it.tags if t})
					for t in clean_tags:
						tid = tag_id_map[t]
						new_image_tags.append((new_id, tid))
						affected_tag_ids.add(tid)

					new_tag_histories.append((new_id, user_id, user_ip, ' '.join(clean_tags)))
					if it.source:
						new_source_histories.append((new_id, user_id, user_ip, it.source[:255]))

				if new_image_tags:
					psycopg2.extras.execute_values(
						cur,
						'INSERT INTO image_tags (image_id, tag_id) VALUES %s ON CONFLICT DO NOTHING',
						new_image_tags,
						page_size=10000,
					)
				if new_tag_histories:
					psycopg2.extras.execute_values(
						cur,
						'INSERT INTO tag_histories (image_id, user_id, user_ip, tags) VALUES %s',
						new_tag_histories,
						page_size=5000,
					)
				if new_source_histories:
					psycopg2.extras.execute_values(
						cur,
						'INSERT INTO source_histories (image_id, user_id, user_ip, source) VALUES %s',
						new_source_histories,
						page_size=5000,
					)

		# --- B. Merge Tags & Sources onto Existing Posts ---
		if existing_candidates:
			merge_tags_vocab: set[str] = set()
			merge_plans: list[tuple[int, set[str], set[str], list[str], str | None, str | None]] = []

			for it in existing_candidates:
				img_id, curr_src, curr_rating, db_tags = existing_rows[it.md5]
				merged_list = _clean_merged_tags(db_tags, it.tags)
				merged_set = set(merged_list)

				added = merged_set - db_tags
				removed = db_tags - merged_set

				# Upgrade source if the incoming source is higher priority
				curr_src_clean = (curr_src or '').strip()
				inc_src_clean = (it.source or '').strip()[:255]
				upgraded_src: str | None = None
				if (
                    inc_src_clean
                    and inc_src_clean != curr_src_clean
                    and (not curr_src_clean or get_source_score(inc_src_clean) < get_source_score(curr_src_clean))
                ):
                    upgraded_src = inc_src_clean

				# Upgrade '?' rating if incoming has a concrete s/q/e rating
				upgraded_rating: str | None = None
				if (not curr_rating or curr_rating.strip() == '?') and it.rating in {'s', 'q', 'e'}:
					upgraded_rating = it.rating

				if added or removed or upgraded_src or upgraded_rating:
					merge_tags_vocab.update(added)
					merge_plans.append((img_id, added, removed, merged_list, upgraded_src, upgraded_rating))

			if merge_plans:
				_ensure_tags_in_db(cur, merge_tags_vocab, tag_id_map)

				tags_to_add: list[tuple[int, int]] = []
				tags_to_remove: list[tuple[int, int]] = []
				merge_tag_histories: list[tuple[int, int, str, str]] = []
				merge_source_updates: list[tuple[int, str]] = []
				merge_source_histories: list[tuple[int, int, str, str]] = []
				merge_rating_updates: list[tuple[int, str]] = []

				for img_id, added, removed, merged_list, up_src, up_rating in merge_plans:
					merged_count += 1
					if added or removed:
						for t in added:
							tid = tag_id_map[t[:255]]
							tags_to_add.append((img_id, tid))
							affected_tag_ids.add(tid)
						for t in removed:
							if t in tag_id_map:
								tid = tag_id_map[t]
								tags_to_remove.append((img_id, tid))
								affected_tag_ids.add(tid)
						merge_tag_histories.append((img_id, user_id, user_ip, ' '.join(merged_list)))

					if up_src:
						merge_source_updates.append((img_id, up_src))
						merge_source_histories.append((img_id, user_id, user_ip, up_src))

					if up_rating:
						merge_rating_updates.append((img_id, up_rating))

				if tags_to_remove:
					cur.execute('CREATE TEMP TABLE tmp_merge_del (img_id INT, tag_id INT) ON COMMIT DROP')
					psycopg2.extras.execute_values(cur, 'INSERT INTO tmp_merge_del VALUES %s', tags_to_remove, page_size=10000)
					cur.execute("""
						DELETE FROM image_tags
						USING tmp_merge_del
						WHERE image_tags.image_id = tmp_merge_del.img_id AND image_tags.tag_id = tmp_merge_del.tag_id
					""")

				if tags_to_add:
					psycopg2.extras.execute_values(
						cur,
						'INSERT INTO image_tags (image_id, tag_id) VALUES %s ON CONFLICT DO NOTHING',
						tags_to_add,
						page_size=10000,
					)

				if merge_tag_histories:
					psycopg2.extras.execute_values(
						cur,
						'INSERT INTO tag_histories (image_id, user_id, user_ip, tags) VALUES %s',
						merge_tag_histories,
						page_size=5000,
					)

				if merge_source_updates:
					psycopg2.extras.execute_values(
						cur,
						"""
						UPDATE images AS i
						SET source = v.new_source
						FROM (VALUES %s) AS v(id, new_source)
						WHERE i.id = v.id
						""",
						merge_source_updates,
						page_size=5000,
					)
					psycopg2.extras.execute_values(
						cur,
						'INSERT INTO source_histories (image_id, user_id, user_ip, source) VALUES %s',
						merge_source_histories,
						page_size=5000,
					)

				if merge_rating_updates:
					psycopg2.extras.execute_values(
						cur,
						"""
						UPDATE images AS i
						SET rating = v.new_rating
						FROM (VALUES %s) AS v(id, new_rating)
						WHERE i.id = v.id
						""",
						merge_rating_updates,
						page_size=5000,
					)

		# --- C. Recalculate Affected Tag Counts ---
		if affected_tag_ids:
			print(f'[INFO] Updating tag usage counts for {len(affected_tag_ids):,} touched tags...')
			cur.execute('CREATE TEMP TABLE tmp_touched_tags (tag_id INT PRIMARY KEY) ON COMMIT DROP')
			psycopg2.extras.execute_values(
				cur,
				'INSERT INTO tmp_touched_tags (tag_id) VALUES %s',
				[(tid,) for tid in affected_tag_ids],
				page_size=10000,
			)
			cur.execute("""
				WITH new_counts AS (
					SELECT ttt.tag_id, COUNT(it.image_id) AS cnt
					FROM tmp_touched_tags ttt
					LEFT JOIN image_tags it ON ttt.tag_id = it.tag_id
					GROUP BY ttt.tag_id
				)
				UPDATE tags AS t
				SET count = nc.cnt
				FROM new_counts nc
				WHERE t.id = nc.tag_id
			""")
			cur.execute('DELETE FROM tags WHERE count <= 0')

		conn.commit()

	print(f'\n[✓] Import complete! Inserted {inserted_count:,} new posts and merged {merged_count:,} existing posts.')
	return inserted_count, merged_count


def _resolve_csv_item_path(raw_path: str, csv_dir: Path, shimmie_root: Path) -> Path:
	"""Resolves a relative or absolute path from an import CSV."""
	cand = Path(raw_path.strip())
	if cand.is_file():
		return cand
	for base in (shimmie_root, csv_dir, csv_dir.parent):
		resolved = base / str(cand).lstrip('/')
		if resolved.is_file():
			return resolved
	return cand


def run_import_csv(args) -> None:
	"""Main CLI entry point for import-csv."""
	target = Path(args.csv)
	shimmie_root = Path(args.spath)

	if target.is_dir():
		csv_files = sorted(target.glob('import*.csv'))
	elif target.is_file():
		csv_files = [target]
	else:
		print(f'[ERROR] CSV path not found: {target}')
		return

	if not csv_files:
		print(f'[ERROR] No import*.csv files found in {target}')
		return

	items: list[ImportItem] = []
	for csv_file in csv_files:
		csv_dir = csv_file.parent
		with csv_file.open('r', encoding='utf-8', newline='') as f:
			for row in csv.reader(f):
				if not row or not row[0].strip():
					continue
				img_path = _resolve_csv_item_path(row[0], csv_dir, shimmie_root)
				if not img_path.is_file():
					print(f'[WARNING] File not found on disk, skipping: {row[0]}')
					continue

				raw_tags = row[1] if len(row) > 1 else ''
				tags = [t.strip() for t in raw_tags.replace(',', ' ').split() if t.strip()]
				source = row[2].strip() if len(row) > 2 else ''
				rating = row[3].strip() if len(row) > 3 and row[3].strip() else '?'
				thumb_str = row[4].strip() if len(row) > 4 else ''
				thumb_path = _resolve_csv_item_path(thumb_str, csv_dir, shimmie_root) if thumb_str else None

				items.append(ImportItem(
					file_path=img_path,
					tags=tags,
					source=source,
					rating=rating,
					thumb_path=thumb_path,
				))

	print(f'[INFO] Loaded {len(items):,} rows from {len(csv_files):,} CSV file(s).')
	import_items_to_shimmie(
		items,
		spath=shimmie_root,
		username=args.user,
		threads=args.threads,
		skip_existing=args.skip_existing,
	)
