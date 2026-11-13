"""Database connections, PostgreSQL queries, SQLite post caching, and tagmap.db management."""

from collections import defaultdict
from collections.abc import Iterable
from contextlib import contextmanager
import csv
from dataclasses import dataclass
from pathlib import Path
import re
import sqlite3
import threading
from urllib.parse import parse_qs, urlparse

import psycopg2
import psycopg2.extras
import tqdm

from functions.common import (
	ADB_PATH,
	CACHE_PATH,
	CDB_PATH,
	ROOT_DIR,
	TAG_DB_PATH,
	TAGMAP_DB_PATH,
	VIDEO_EXTS,
	extract_or_compute_md5,
)
from functions.media import compute_danbooru_pixel_hash
from functions.source_resolver import is_recognized_prefix

_thread_local = threading.local()
_cache_write_lock = threading.Lock()
_initialized_caches: set[Path] = set()


@dataclass(slots=True)
class ResolutionData:
	"""Represents the resolved cache and database state for a single media file."""

	image: Path
	post: dict[str, str | list[str]]
	md5: str
	px_hash: str | None
	exists: bool | str


@dataclass(slots=True)
class Mappings:
	"""Holds character->series, artist->canonical_artist, and tag->rating_weight dictionaries."""

	char: dict[str, str]
	artist: dict[str, str]
	rating: dict[str, int]


def init_posts_cache(cache_path: Path = CACHE_PATH) -> None:
	"""Initializes the SQLite posts_cache schema and enables WAL mode once per process."""
	resolved = Path(cache_path).resolve()
	if resolved in _initialized_caches:
		return

	with _cache_write_lock:
		if resolved in _initialized_caches:
			return
		resolved.parent.mkdir(parents=True, exist_ok=True)
		with sqlite3.connect(resolved) as conn:
			conn.execute('PRAGMA journal_mode=WAL;')
			conn.execute("""
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
			conn.execute('CREATE INDEX IF NOT EXISTS idx_posts_pixel_hash ON posts(pixel_hash)')
		_initialized_caches.add(resolved)


@contextmanager
def get_cache_conn(cache_path: Path = CACHE_PATH):
	"""Yields a thread-local SQLite connection to posts_cache.db."""
	init_posts_cache(cache_path)
	conns: dict[Path, sqlite3.Connection] = getattr(_thread_local, 'conns', None)
	if conns is None:
		conns = {}
		_thread_local.conns = conns

	resolved = Path(cache_path).resolve()
	conn = conns.get(resolved)
	if conn is None:
		conn = sqlite3.connect(resolved, timeout=30.0, check_same_thread=False)
		conns[resolved] = conn
	yield conn


def row_to_post_dict(row: tuple) -> dict[str, str | list[str]]:
	"""Converts a SQLite `posts` table row into a structured post dictionary."""
	def _split(field: str | None) -> list[str]:
		if not field:
			return []
		return [part.strip() for part in field.split(',') if part.strip()]

	return {
		'md5': row[0],
		'pixel_hash': row[1],
		'rating': row[2] or '?',
		'source': row[3] or '',
		'general': _split(row[4]),
		'character': _split(row[5]),
		'artist': _split(row[6]),
		'series': _split(row[7]),
	}


def post_has_tags(post: dict[str, str | list[str]] | None) -> bool:
	"""Returns True if a cached post dictionary contains actual non-placeholder tags."""
	if not post:
		return False
	for key in ('character', 'artist', 'series'):
		if post.get(key):
			return True
	general = [t for t in post.get('general', ()) if t and t != 'tagme']
	return bool(general)


def parse_tags_for_cache(tags: Iterable[str]) -> tuple[str, str, str, str, str]:
	"""Splits a curated tag list into (general, character, artist, series, source) strings for SQLite."""
	buckets: dict[str, list[str]] = {'general': [], 'character': [], 'artist': [], 'series': []}
	source_val = ''

	for tag in tags:
		if tag in {'tagme', 'artist:tagme', 'character:tagme', 'series:tagme'}:
			continue
		if is_recognized_prefix(tag):
			prefix, _, value = tag.partition(':')
			if prefix in buckets:
				if value and value != 'tagme':
					buckets[prefix].append(value)
			elif prefix == 'source':
				source_val = value
			else:
				buckets['general'].append(tag)
		else:
			buckets['general'].append(tag)

	return (
		','.join(buckets['general']) or 'tagme',
		','.join(buckets['character']),
		','.join(buckets['artist']),
		','.join(buckets['series']),
		source_val,
	)


def add_post_to_cache(md5: str, px_hash: str, cache_path: Path = CACHE_PATH) -> dict[str, str | list[str]]:
	"""Inserts a blank stub record into posts_cache.db in a thread-safe manner and returns its dict."""
	row = (md5, px_hash, '?', '', '', '', '', '')
	with get_cache_conn(cache_path) as conn, _cache_write_lock:
		conn.execute(
			"""
			INSERT OR IGNORE INTO posts
			(md5, pixel_hash, rating, source, general, character, artist, series)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			""",
			row,
		)
		conn.commit()
	return row_to_post_dict(row)


def save_post_to_cache(
	res_data: ResolutionData,
	rating_letter: str,
	tags: list[str],
	pure_source_link: str | None,
	cache_path: Path = CACHE_PATH,
) -> None:
	"""Updates posts_cache.db with curated tags, rating, and source URL."""
	parsed = parse_tags_for_cache(tags)
	new_data = (
		rating_letter,
		pure_source_link or parsed[4] or '',
		parsed[0],
		parsed[1],
		parsed[2],
		parsed[3],
	)

	with get_cache_conn(cache_path) as conn:
		cur = conn.cursor()
		cur.execute(
			'SELECT rating, source, general, character, artist, series FROM posts WHERE md5 = ?',
			(res_data.md5,),
		)
		existing = cur.fetchone()
		if existing is None or existing != new_data:
			with _cache_write_lock:
				cur.execute(
					"""
					INSERT OR REPLACE INTO posts
					(md5, pixel_hash, rating, source, general, character, artist, series)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?)
					""",
					(res_data.md5, res_data.px_hash, *new_data),
				)
				conn.commit()


def get_existing_shimmie_hashes(md5_iterable: Iterable[str], db_config: dict | None) -> set[str]:
	"""Returns the subset of MD5 hashes that already exist in Shimmie2's PostgreSQL `images` table."""
	md5_list = [m.lower() for m in md5_iterable if m]
	if not md5_list or not db_config:
		return set()

	existing: set[str] = set()
	try:
		with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
			for idx in range(0, len(md5_list), 10000):
				chunk = md5_list[idx : idx + 10000]
				cur.execute('SELECT hash FROM images WHERE hash = ANY(%s)', (chunk,))
				existing.update(row[0] for row in cur.fetchall() if row[0])
	except Exception as e:
		print(f'\n[WARNING] Could not query existing hashes from PostgreSQL: {e}')

	return existing


def resolve_post(
	image: Path,
	cache_path: Path = CACHE_PATH,
	existing_hashes: set[str] | None = None,
	precomputed_md5: str | None = None,
) -> ResolutionData:
	"""Resolves post metadata from posts_cache.db by MD5 or Danbooru pixel hash."""
	md5 = precomputed_md5 or extract_or_compute_md5(image)
	is_video = image.suffix.lower() in VIDEO_EXTS
	post: dict[str, str | list[str]] | None = None
	px_hash: str | None = None

	with get_cache_conn(cache_path) as conn:
		cur = conn.cursor()
		if md5:
			cur.execute('SELECT * FROM posts WHERE md5 = ?', (md5,))
			if row := cur.fetchone():
				post = row_to_post_dict(row)
				px_hash = row[1]

		try:
			if not px_hash:
				px_hash = md5 if is_video else compute_danbooru_pixel_hash(image)

			if post is None:
				cur.execute('SELECT * FROM posts WHERE pixel_hash = ?', (px_hash,))
				post = row_to_post_dict(row) if (row := cur.fetchone()) else add_post_to_cache(md5, px_hash, cache_path)

		except Exception as e:
			print(f'\n[ERROR] Skipping corrupt or unreadable file: {image}')
			print(f'        Reason: {type(e).__name__} - {e}')
			return ResolutionData(image, {}, md5, None, 'error')

	exists = bool(existing_hashes and md5 in existing_hashes)
	return ResolutionData(image, post, md5, px_hash, exists)


def get_shimmie_db_credentials(spath: str | Path | None) -> dict[str, str | int] | None:
	"""Parses Shimmie2 shimmie.conf.php to extract PostgreSQL credentials across all DSN formats."""
	if not spath:
		return None

	config_path = Path(spath) / 'data' / 'config' / 'shimmie.conf.php'
	if not config_path.is_file():
		print(f'[WARNING] Could not find Shimmie config at {config_path}')
		return None

	content = config_path.read_text(encoding='utf-8')

	user_match = (
		re.search(r"\$database_user\s*=\s*['\"]([^'\"]+)['\"]", content)
		or re.search(r"define\s*\(\s*['\"]DATABASE_USER['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", content)
	)
	pass_match = (
		re.search(r"\$database_pass\s*=\s*['\"]([^'\"]+)['\"]", content)
		or re.search(r"define\s*\(\s*['\"]DATABASE_PASS['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", content)
	)

	if uri_match := re.search(r"['\"]((?:pgsql|postgres|postgresql)://[^'\"]+)['\"]", content):
		parsed = urlparse(uri_match.group(1))
		query_params = parse_qs(parsed.query)
		host = query_params.get('host', [parsed.hostname or 'localhost'])[0]
		res: dict[str, str | int] = {
			'host': host,
			'dbname': parsed.path.lstrip('/') or 'shimmie',
			'user': parsed.username or (user_match.group(1) if user_match else 'postgres'),
			'password': parsed.password or (pass_match.group(1) if pass_match else ''),
		}
		if parsed.port:
			res['port'] = parsed.port
		return res

	if dsn_match := re.search(r"['\"]pgsql:([^'\"]+)['\"]", content):
		dsn_parts = dict(part.split('=', 1) for part in dsn_match.group(1).split(';') if '=' in part)
		res = {
			'host': dsn_parts.get('host', 'localhost'),
			'dbname': dsn_parts.get('dbname', 'shimmie'),
			'user': dsn_parts.get('user') or (user_match.group(1) if user_match else 'postgres'),
			'password': (
				dsn_parts['password']
				if 'password' in dsn_parts
				else (pass_match.group(1) if pass_match else '')
			),
		}
		if 'port' in dsn_parts:
			res['port'] = dsn_parts['port']
		return res

	return None


def _fetch_postgres_tags(
	md5_list: list[str],
	db_conn: dict | None,
	chunk_size: int,
	results: dict[str, set[str]],
) -> None:
	"""Fetches authoritative tags from PostgreSQL in a single bulk pass."""
	if not db_conn or not md5_list:
		return

	try:
		with psycopg2.connect(**db_conn) as conn, conn.cursor() as cur:
			print('  -> Uploading hash list to temporary Postgres table...')
			cur.execute('CREATE TEMP TABLE tmp_hashes (hash VARCHAR(32)) ON COMMIT DROP')
			psycopg2.extras.execute_values(
				cur,
				'INSERT INTO tmp_hashes (hash) VALUES %s',
				[(m,) for m in md5_list],
				page_size=chunk_size,
			)
			cur.execute('ANALYZE tmp_hashes')

			print('  -> Streaming matched tags from Postgres...')
			cur.execute("""
				SELECT i.hash, array_agg(t.tag)
				FROM tmp_hashes th
				JOIN images i ON i.hash = th.hash
				JOIN image_tags it ON i.id = it.image_id
				JOIN tags t ON it.tag_id = t.id
				GROUP BY i.hash;
			""")

			for hsh, tag_list in tqdm.tqdm(cur.fetchall(), desc='Reading Postgres tags', leave=False):
				results[hsh].update(t.strip() for t in tag_list if t)
	except Exception as e:
		print(f'\n[WARNING] Bulk DB query failed: {e}')


def _fetch_sqlite_tags(
	md5_list: list[str],
	sqlite_conn: sqlite3.Connection | None,
	results: dict[str, set[str]],
) -> None:
	"""Fetches tags from SQLite cache ONLY for hashes missing from Postgres."""
	if not sqlite_conn:
		return

	missing = [m for m in md5_list if m not in results]
	if not missing:
		return

	cur = sqlite_conn.cursor()
	for idx in range(0, len(missing), 999):
		chunk = missing[idx : idx + 999]
		placeholders = ','.join(['?'] * len(chunk))
		query = f'SELECT md5, general, character, artist, series FROM posts WHERE md5 IN ({placeholders})'
		cur.execute(query, chunk)
		for row in cur.fetchall():
			for field in row[1:]:
				if field:
					results[row[0]].update(t.strip() for t in field.split(',') if t.strip())


def get_bulk_canonical_tags(
	md5_set: Iterable[str],
	db_conn: dict | None,
	sqlite_conn: sqlite3.Connection | None,
	chunk_size: int = 10000,
) -> dict[str, set[str]]:
	"""Fetches canonical tags for a set of MD5s, prioritizing live Postgres over SQLite cache."""
	results: dict[str, set[str]] = defaultdict(set)
	md5_list = list(md5_set)
	_fetch_postgres_tags(md5_list, db_conn, chunk_size, results)
	_fetch_sqlite_tags(md5_list, sqlite_conn, results)
	return results


def load_mappings() -> Mappings:
	"""Loads character, artist, and dominant tag rating dictionaries from SQLite."""
	for db_file, label in ((CDB_PATH, 'Character DB'), (ADB_PATH, 'Artist DB'), (TAG_DB_PATH, 'Tag Rating DB')):
		if not db_file.is_file():
			raise FileNotFoundError(f'{label} not found: {db_file}')

	char_map: dict[str, str] = {}
	with sqlite3.connect(CDB_PATH) as conn:
		for row in conn.execute('SELECT * FROM data'):
			if len(row) >= 2 and row[0] and row[1] and row[0].strip() and row[1].strip():
				char_map[row[0].strip()] = row[1].strip()

	artist_map: dict[str, str] = {}
	with sqlite3.connect(ADB_PATH) as conn:
		for row in conn.execute('SELECT * FROM data'):
			if len(row) >= 2 and row[0] and row[1] and row[0].strip() and row[1].strip():
				artist_map[row[0].strip()] = row[1].strip()

	rating_map: dict[str, int] = {}
	with sqlite3.connect(TAG_DB_PATH) as conn:
		for row in conn.execute('SELECT tag_name, dominant_rating FROM dominant_tag_ratings'):
			if len(row) >= 2 and row[0]:
				rating_map[row[0].strip().lower()] = int(row[1])

	print(f'[INFO] Loaded {len(char_map):,} chars, {len(artist_map):,} artists, {len(rating_map):,} rating weights.')
	return Mappings(char_map, artist_map, rating_map)


# ==========================================
# Tagmap SQLite & CSV Helpers (tagmap.db)
# ==========================================
SEED_TAGMAP_RULES = (
	('character:samurai_(7th_dragon_series)', 'character:samurai_(7th_dragon)', 1.0, 0),
	('deep-blue_series', 'series:deep-blue', 1.0, 0),
	('samurai_(7th_dragon)', 'character:samurai_(7th_dragon)', 1.0, 0),
	('series:fate_(series)', 'series:fate', 1.0, 0),
	('series:pokemon_(anime)', 'series:pokemon', 1.0, 0),
	('series:pokemon_(classic_anime)', 'series:pokemon', 1.0, 0),
	('series:pokemon_(game)', 'series:pokemon', 1.0, 0),
	('series:pokemon_bw_(anime)', 'series:pokemon_bw series:pokemon', 1.0, 0),
	('series:pokemon_dppt_(anime)', 'series:pokemon_dppt series:pokemon', 1.0, 0),
	('series:pokemon_emerald', 'series:pokemon_rse series:pokemon', 1.0, 0),
	('series:pokemon_rse_(anime)', 'series:pokemon_rse series:pokemon', 1.0, 0),
	('series:pokemon_sm_(anime)', 'series:pokemon_sm series:pokemon', 1.0, 0),
	('series:pokemon_xy_(anime)', 'series:pokemon_xy series:pokemon', 1.0, 0),
	('series:x-men:_the_animated_series', 'series:x-men', 1.0, 0),
	('x-men:_the_animated_series', 'series:x-men', 1.0, 0),
	('x-men_film_series', 'series:x-men', 1.0, 0),
	('series:sen_no_kiseki', 'series:sen_no_kiseki series:eiyuu_densetsu', 1.0, 0),
	('series:sora_no_kiseki', 'series:sora_no_kiseki series:eiyuu_densetsu', 1.0, 0),
	('series:zero_no_kiseki', 'series:zero_no_kiseki series:eiyuu_densetsu', 1.0, 0),
	('series:ao_no_kiseki', 'series:ao_no_kiseki series:eiyuu_densetsu', 1.0, 0),
	('series:hajimari_no_kiseki', 'series:hajimari_no_kiseki series:eiyuu_densetsu', 1.0, 0),
	('series:kuro_no_kiseki', 'series:kuro_no_kiseki series:eiyuu_densetsu', 1.0, 0),
	('series:kai_no_kiseki', 'series:kai_no_kiseki series:eiyuu_densetsu', 1.0, 0),
	('series:pokemon_bw', 'series:pokemon_bw series:pokemon', 1.0, 0),
	('series:pokemon_dppt', 'series:pokemon_dppt series:pokemon', 1.0, 0),
	('series:pokemon_rse', 'series:pokemon_rse series:pokemon', 1.0, 0),
	('series:pokemon_sm', 'series:pokemon_sm series:pokemon', 1.0, 0),
	('series:pokemon_xy', 'series:pokemon_xy series:pokemon', 1.0, 0),
	('series:pokemon_rgby', 'series:pokemon_rgby series:pokemon', 1.0, 0),
	('series:pokemon_gsc', 'series:pokemon_gsc series:pokemon', 1.0, 0),
	('series:pokemon_swsh', 'series:pokemon_swsh series:pokemon', 1.0, 0),
	('series:pokemon_sv', 'series:pokemon_sv series:pokemon', 1.0, 0),
	('series:pokemon_legends:_arceus', 'series:pokemon_legends:_arceus series:pokemon', 1.0, 0),
	('series:fate/grand_order', 'series:fate/grand_order series:fate', 1.0, 0),
	('series:fate/stay_night', 'series:fate/stay_night series:fate', 1.0, 0),
	('series:fate/zero', 'series:fate/zero series:fate', 1.0, 0),
	('series:fate/extra', 'series:fate/extra series:fate', 1.0, 0),
	('series:fate/extra_ccc', 'series:fate/extra_ccc series:fate series:fate/extra', 1.0, 0),
	('series:fate/apocrypha', 'series:fate/apocrypha series:fate', 1.0, 0),
	('series:fate/kaleid_liner_prisma_illya', 'series:fate/kaleid_liner_prisma_illya series:fate', 1.0, 0),
	('series:fate/prototype', 'series:fate/prototype series:fate', 1.0, 0),
	('series:fate/strange_fake', 'series:fate/strange_fake series:fate', 1.0, 0),
	('series:fate/hollow_ataraxia', 'series:fate/hollow_ataraxia series:fate', 1.0, 0),
)

def init_tagmap_db(db_path: Path = TAGMAP_DB_PATH) -> None:
	"""Ensures the tagmap SQLite table exists, seeding base rules and migrating complete_tagmap.csv on first creation."""
	db_path = Path(db_path)
	db_path.parent.mkdir(parents=True, exist_ok=True)
	is_new = not db_path.is_file()

	with sqlite3.connect(db_path) as conn:
		conn.execute("""
			CREATE TABLE IF NOT EXISTS tagmap (
				sidecar_tag TEXT PRIMARY KEY,
				canonical_tag TEXT NOT NULL,
				confidence REAL NOT NULL DEFAULT 1.0,
				sample_size INTEGER NOT NULL DEFAULT 0
			)
		""")

	if is_new:
		save_tagmap_entries(SEED_TAGMAP_RULES, db_path)
		legacy_csv = ROOT_DIR / 'complete_tagmap.csv'
		if legacy_csv.is_file():
			imported = import_csv_to_tagmap_db(legacy_csv, db_path)
			print(f'[INFO] Auto-migrated {imported:,} mappings from {legacy_csv.name} -> {db_path.name}')


def import_csv_to_tagmap_db(csv_path: Path, db_path: Path = TAGMAP_DB_PATH, min_confidence: float = 0.60) -> int:
	"""Imports mappings from a CSV file into tagmap.db, returning the number of upserted rows."""
	csv_path = Path(csv_path)
	if not csv_path.is_file():
		return 0

	init_tagmap_db(db_path)
	records: list[tuple[str, str, float, int]] = []

	with csv_path.open('r', encoding='utf-8', newline='') as f:
		reader = csv.DictReader(f)
		for row in reader:
			s_tag = (row.get('Sidecar_Tag') or '').strip()
			c_tag = (row.get('Canonical_Tag') or '').strip()
			if not s_tag or not c_tag or s_tag == 'Sidecar_Tag':
				continue

			conf_raw = (row.get('Confidence') or '').strip()
			sample_raw = (row.get('Sample_Size') or row.get('Count') or '').strip()
			try:
				confidence = float(conf_raw) if conf_raw else 1.0
			except ValueError:
				continue

			try:
				sample_size = int(float(sample_raw)) if sample_raw else 0
			except ValueError:
				sample_size = 0

			if confidence >= min_confidence:
				records.append((s_tag, c_tag, confidence, sample_size))

	if records:
		save_tagmap_entries(records, db_path)
	return len(records)


def save_tagmap_entries(
	entries: Iterable[tuple[str, str, float, int] | dict],
	db_path: Path = TAGMAP_DB_PATH,
) -> int:
	"""Upserts (sidecar_tag, canonical_tag, confidence, sample_size) rows into tagmap.db."""
	init_tagmap_db(db_path)
	rows: list[tuple[str, str, float, int]] = []

	for item in entries:
		if isinstance(item, dict):
			s_tag = (item.get('Sidecar_Tag') or item.get('sidecar_tag') or '').strip()
			c_tag = (item.get('Canonical_Tag') or item.get('canonical_tag') or '').strip()
			conf = float(item.get('Confidence', item.get('confidence', 1.0)))
			sample = int(item.get('Sample_Size', item.get('sample_size', 0)))
		else:
			s_tag, c_tag, conf, sample = item[0].strip(), item[1].strip(), float(item[2]), int(item[3])

		if s_tag and c_tag:
			rows.append((s_tag, c_tag, conf, sample))

	if not rows:
		return 0

	with sqlite3.connect(db_path) as conn:
		conn.executemany(
			"""
			INSERT INTO tagmap (sidecar_tag, canonical_tag, confidence, sample_size)
			VALUES (?, ?, ?, ?)
			ON CONFLICT(sidecar_tag) DO UPDATE SET
				canonical_tag = excluded.canonical_tag,
				confidence = excluded.confidence,
				sample_size = excluded.sample_size
			""",
			rows,
		)
	return len(rows)


def load_tagmap(source_path: str | Path | None = None, min_confidence: float = 0.60) -> dict[str, str]:
	"""Loads dynamic tag mappings from database/tagmap.db (default) or a specified .db/.csv path."""
	target = Path(source_path) if source_path else TAGMAP_DB_PATH

	if target.suffix.lower() == '.csv':
		temp_map: dict[str, str] = {}
		if not target.is_file():
			return temp_map
		with target.open('r', encoding='utf-8', newline='') as f:
			for row in csv.DictReader(f):
				s_tag = (row.get('Sidecar_Tag') or '').strip()
				c_tag = (row.get('Canonical_Tag') or '').strip()
				if not s_tag or not c_tag or s_tag == 'Sidecar_Tag':
					continue
				conf_raw = (row.get('Confidence') or '').strip()
				try:
					conf = float(conf_raw) if conf_raw else 1.0
				except ValueError:
					continue
				if conf >= min_confidence:
					temp_map[s_tag] = c_tag
		return temp_map

	init_tagmap_db(target)
	dynamic_map: dict[str, str] = {}
	with sqlite3.connect(target) as conn:
		cur = conn.execute(
			'SELECT sidecar_tag, canonical_tag FROM tagmap WHERE confidence >= ?',
			(min_confidence,),
		)
		for s_tag, c_tag in cur.fetchall():
			if s_tag and c_tag:
				dynamic_map[s_tag.strip()] = c_tag.strip()
	return dynamic_map
