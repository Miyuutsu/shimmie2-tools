"""Updates existing image ratings in Shimmie2 PostgreSQL based on dominant tag weights."""

from collections.abc import Iterable
import sqlite3

import psycopg2
import psycopg2.extras
import tqdm

from functions.common import TAG_DB_PATH
from functions.db_cache import get_shimmie_db_credentials
from functions.tags_curation import rating_from_score


def _load_tag_rating_map() -> dict[str, int]:
	"""Loads tag rating weights from tag_rating_dominant.db."""
	if not TAG_DB_PATH.is_file():
		raise FileNotFoundError(f'Tag rating DB not found: {TAG_DB_PATH}')

	with sqlite3.connect(TAG_DB_PATH) as conn:
		return {
			t.strip().lower(): int(r)
			for t, r in conn.execute('SELECT tag_name, dominant_rating FROM dominant_tag_ratings')
			if t and t.strip()
		}


def _calculate_new_rating(
	tags: Iterable[str | None],
	current_rating: str,
	tag_rating_map: dict[str, int],
	smax: int,
	qmax: int,
) -> str:
	"""Calculates a single image's new rating from its non-AI, non-booru tags."""
	total_score = 0
	for tag in tags:
		if not tag or tag.startswith(('booru:', 'tagai:')):
			continue

		base_tag = (tag.split(':', 1)[-1] if ':' in tag else tag).lower()
		weight = tag_rating_map.get(base_tag)
		if weight is None:
			continue
		if weight == 1 and total_score == 0:
			total_score = 1
		elif weight > 1:
			total_score += weight

	return rating_from_score(total_score, smax, qmax) if total_score > 0 else current_rating


def update_ratings(args) -> None:
	"""Evaluates and updates existing ratings in Shimmie2's PostgreSQL `images` table."""
	tag_rating_map = _load_tag_rating_map()

	pg_config = get_shimmie_db_credentials(args.spath)
	if not pg_config:
		print(f'[ERROR] Could not load DB credentials from {args.spath}')
		return

	with psycopg2.connect(**pg_config) as pg_conn, pg_conn.cursor() as pg_cur:
		print('Fetching images and tags from Postgres...')
		pg_cur.execute("""
			SELECT i.id, i.rating, array_agg(t.tag)
			FROM images i
			LEFT JOIN image_tags it ON i.id = it.image_id
			LEFT JOIN tags t ON it.tag_id = t.id
			GROUP BY i.id, i.rating
		""")
		rows = pg_cur.fetchall()

		updates: list[tuple[int, str]] = []
		for image_id, current_rating, tags in tqdm.tqdm(rows, desc='Evaluating Ratings', leave=False):
			new_rating = _calculate_new_rating(
				tags or (), current_rating, tag_rating_map, args.smax, args.qmax
			)
			if new_rating != current_rating:
				updates.append((image_id, new_rating))

		if updates:
			psycopg2.extras.execute_values(
				pg_cur,
				"""
				UPDATE images AS i
				SET rating = v.new_rating
				FROM (VALUES %s) AS v(id, new_rating)
				WHERE i.id = v.id
				""",
				updates,
				page_size=10000,
			)
			pg_conn.commit()

		updated = len(updates)
		print(f"[✓] Updated {updated:,} image rating{'s' if updated != 1 else ''}.\n")
