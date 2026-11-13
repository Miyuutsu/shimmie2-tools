"""Bridge script for auditing Shimmie2 images with SD-Tag-Editor and injecting shadow tags."""

from collections.abc import Iterator
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile

import psycopg2
import psycopg2.extras
import tqdm

from functions.common import QUARANTINE_DB_PATH, SUBMODULE_PATH, VIDEO_EXTS
from functions.db_cache import get_shimmie_db_credentials


def _init_quarantine_db() -> sqlite3.Connection:
	"""Initializes the SQLite quarantine database for logging raw AI tag confidences."""
	QUARANTINE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
	q_conn = sqlite3.connect(QUARANTINE_DB_PATH)
	q_conn.execute("""
		CREATE TABLE IF NOT EXISTS ai_tags (
			image_hash TEXT,
			tag TEXT,
			confidence REAL,
			UNIQUE(image_hash, tag)
		)
	""")
	q_conn.execute('CREATE INDEX IF NOT EXISTS idx_ai_tags_hash ON ai_tags(image_hash)')
	return q_conn


def _get_image_batches(pg_conn, batch_size: int = 5000) -> Iterator[dict[str, dict[str, int | set[str]]]]:
	"""Yields dictionaries of untagged static images using a withhold server-side cursor to prevent OOM."""
	excluded_exts = tuple(ext.lstrip('.') for ext in VIDEO_EXTS)
	with pg_conn.cursor(name='audit_cursor', withhold=True) as cur:
		cur.execute(
			"""
			SELECT i.id, i.hash, array_agg(t.tag)
			FROM images i
			LEFT JOIN image_tags it ON i.id = it.image_id
			LEFT JOIN tags t ON it.tag_id = t.id
			WHERE LOWER(i.ext) NOT IN %s
			GROUP BY i.id, i.hash
			""",
			(excluded_exts,),
		)

		while rows := cur.fetchmany(batch_size):
			batch_data: dict[str, dict[str, int | set[str]]] = {}
			for img_id, img_hash, tag_list in rows:
				clean_tags = {t for t in (tag_list or ()) if t is not None}
				if not any(t.startswith('tagai:') for t in clean_tags):
					batch_data[img_hash] = {'id': img_id, 'tags': clean_tags}
			yield batch_data


def _resolve_shimmie_thumb_path(base_dir: Path, md5_hash: str) -> Path:
	"""Resolves the two-level fanout path for a Shimmie2 thumbnail."""
	return base_dir / md5_hash[0:2] / md5_hash[2:4] / md5_hash


def _ensure_submodule_installed() -> Path:
	"""Ensures the SD-Tag-Editor virtual environment exists."""
	venv_python = SUBMODULE_PATH / 'venv' / 'bin' / 'python'
	if not venv_python.exists():
		print('[INFO] SD-Tag-Editor venv not found. Running install.sh...')
		subprocess.run(['bash', 'install.sh'], cwd=SUBMODULE_PATH, check=True)
	return venv_python


def _ensure_tag_exists(pg_cur, tag_name: str, tag_id_cache: dict[str, int]) -> int:
	"""Fetches or creates a tag ID in PostgreSQL, caching results across chunks."""
	if tag_name in tag_id_cache:
		return tag_id_cache[tag_name]

	pg_cur.execute('SELECT id FROM tags WHERE tag = %s', (tag_name,))
	if row := pg_cur.fetchone():
		tag_id_cache[tag_name] = row[0]
		return row[0]

	pg_cur.execute('INSERT INTO tags (tag, count) VALUES (%s, 0) RETURNING id', (tag_name,))
	tag_id = pg_cur.fetchone()[0]
	tag_id_cache[tag_name] = tag_id
	return tag_id


def _run_audit_pipeline(args, pg_conn, pg_cur, q_conn: sqlite3.Connection, q_cur: sqlite3.Cursor) -> None:
	"""Streams image chunks from Postgres, runs AI tagger inference, and commits shadow tags per chunk."""
	venv_python = _ensure_submodule_installed()
	thumbs_base_dir = Path(args.thumbs) if args.thumbs else Path(args.spath) / 'data' / 'thumbs'
	if not thumbs_base_dir.exists():
		print(f'[ERROR] Thumbnails directory not found at {thumbs_base_dir}')
		return

	print('Initiating streaming database cursor...')
	tag_id_cache: dict[str, int] = {}
	tagger_script = SUBMODULE_PATH / 'run.py'

	for batch_index, images_data in enumerate(_get_image_batches(pg_conn)):
		if not images_data:
			continue

		print(f'\n--- Processing DB Chunk {batch_index + 1} ({len(images_data):,} images) ---')

		with tempfile.TemporaryDirectory(prefix='ai_audit_') as temp_dir_str:
			staging_dir = Path(temp_dir_str)
			staged_count = 0

			for img_hash in tqdm.tqdm(images_data, desc='Staging Symlinks'):
				real_thumb = _resolve_shimmie_thumb_path(thumbs_base_dir, img_hash).resolve()
				if real_thumb.exists():
					os.symlink(real_thumb, staging_dir / f'{img_hash}.jpg')
					staged_count += 1

			if staged_count == 0:
				print('[WARNING] No thumbnails found matching this DB chunk. Skipping.')
				continue

			cmd = [
				str(venv_python),
				'-u',
				str(tagger_script),
				str(staging_dir),
				f'--model={args.model}',
				f'--batch_size={args.batch}',
				f'--gen_threshold={args.gen_threshold}',
				f'--char_threshold={args.char_threshold}',
				'--output_json',
				'True',
			]

			print('[INFO] Launching Inference...')
			try:
				subprocess.run(cmd, cwd=SUBMODULE_PATH, check=True)
			except subprocess.CalledProcessError as e:
				print(f'[ERROR] Tagger submodule failed: {e}')
				return

			print('[INFO] Resolving tags and updating Postgres...')
			pg_insert_records: set[tuple[int, int]] = set()
			quarantine_records: list[tuple[str, str, float]] = []

			for json_file in tqdm.tqdm(list(staging_dir.glob('*.json')), desc='Parsing Output'):
				img_hash = json_file.stem
				if img_hash not in images_data:
					continue

				data = images_data[img_hash]
				with json_file.open('r', encoding='utf-8') as f:
					output = json.load(f)

				all_tags = {**output.get('character', {}), **output.get('general', {})}
				raw_db_tags: set[str] = data['tags']  # type: ignore[assignment]
				base_db_tags = {t.split(':', 1)[-1] if ':' in t else t for t in raw_db_tags}

				for tag, confidence in all_tags.items():
					tag_clean = tag.replace(' ', '_').lower()
					quarantine_records.append((img_hash, tag_clean, float(confidence)))

					if tag_clean in base_db_tags:
						continue

					prefixed_tag = f'tagai:{tag_clean}'
					tag_id = _ensure_tag_exists(pg_cur, prefixed_tag, tag_id_cache)
					pg_insert_records.add((int(data['id']), tag_id))

			if quarantine_records:
				q_cur.executemany(
					'INSERT OR IGNORE INTO ai_tags (image_hash, tag, confidence) VALUES (?, ?, ?)',
					quarantine_records,
				)
				q_conn.commit()

			if pg_insert_records:
				psycopg2.extras.execute_values(
					pg_cur,
					'INSERT INTO image_tags (image_id, tag_id) VALUES %s ON CONFLICT DO NOTHING',
					list(pg_insert_records),
					page_size=10000,
				)
				pg_conn.commit()
				print(f'Applied {len(pg_insert_records):,} shadow tags for this chunk.')
			else:
				print('No new tags to apply for this chunk.')

	print('\nRecalculating global tagai:* counts...')
	pg_cur.execute("""
		UPDATE tags
		SET count = (
			SELECT COUNT(image_id)
			FROM image_tags
			WHERE tag_id = tags.id
		)
		WHERE tag LIKE 'tagai:%'
	""")
	pg_conn.commit()
	print('\n[✓] Global Audit Complete.')


def run_auditor(args) -> None:
	"""Main CLI entry point for audit-ratings."""
	if not SUBMODULE_PATH.exists():
		print('[ERROR] SD-Tag-Editor submodule not found.')
		return

	print('=== AI Shadow Tag Auditor (Direct Apply) ===')
	db_config = get_shimmie_db_credentials(args.spath)
	if not db_config:
		print(f'[ERROR] Could not load DB credentials from {args.spath}')
		return

	pg_conn = psycopg2.connect(**db_config)
	q_conn = _init_quarantine_db()

	try:
		with pg_conn.cursor() as pg_cur:
			q_cur = q_conn.cursor()
			_run_audit_pipeline(args, pg_conn, pg_cur, q_conn, q_cur)
	finally:
		pg_conn.close()
		q_conn.close()
