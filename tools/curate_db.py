"""Retroactively applies tag curation, source upgrades, and rating rules to Shimmie2 PostgreSQL."""

from dataclasses import dataclass, field

import psycopg2
import psycopg2.extras
import tqdm

from functions.db_cache import get_shimmie_db_credentials, load_mappings, load_tagmap
from functions.source_resolver import get_source_score
from functions.tags_curation import TagCurationContext, apply_tag_curation, get_curation_context

RATING_PRIORITY = {'e': 3, 'q': 2, 's': 1}
CANONICAL_RATING_TAGS = {'rating:e', 'rating:q', 'rating:s'}


@dataclass(slots=True)
class CurationChangeset:
	"""Holds all pending tag, source, and rating updates discovered during image evaluation."""

	tags_to_add: list[tuple[int, str]] = field(default_factory=list)
	tags_to_remove: list[tuple[int, int]] = field(default_factory=list)
	source_updates: list[tuple[int, str]] = field(default_factory=list)
	rating_updates: list[tuple[int, str]] = field(default_factory=list)
	new_unique_tags: set[str] = field(default_factory=set)

	def is_empty(self) -> bool:
		"""Returns True if no database modifications are required."""
		return not (
			self.tags_to_add
			or self.tags_to_remove
			or self.source_updates
			or self.rating_updates
		)


def _fetch_target_images(cur, target_tag: str | None) -> list[tuple[int, str | None, str | None, list[str | None]]]:
	"""Fetches image IDs, sources, ratings, and tag arrays from PostgreSQL."""
	if target_tag:
		print(f"Fetching image tags from Postgres strictly for batch '{target_tag}'...")
		cur.execute(
			"""
			SELECT i.id, i.source, i.rating, array_agg(t.tag)
			FROM images i
			JOIN image_tags it ON i.id = it.image_id
			JOIN tags t ON it.tag_id = t.id
			WHERE i.id IN (
				SELECT it2.image_id
				FROM image_tags it2
				JOIN tags t2 ON it2.tag_id = t2.id
				WHERE t2.tag = %s
			)
			GROUP BY i.id, i.source, i.rating
			""",
			(target_tag,),
		)
	else:
		print('Fetching all images and tags from Postgres...')
		cur.execute("""
			SELECT i.id, i.source, i.rating, array_agg(t.tag)
			FROM images i
			LEFT JOIN image_tags it ON i.id = it.image_id
			LEFT JOIN tags t ON it.tag_id = t.id
			GROUP BY i.id, i.source, i.rating
		""")
	return cur.fetchall()


def _evaluate_curation_changes(
	images_data: list[tuple[int, str | None, str | None, list[str | None]]],
	merged_map: dict[str, str],
	ctx: TagCurationContext,
	single_tag_cache: dict[str, tuple[str, ...]],
	tag_id_map: dict[str, int],
) -> CurationChangeset:
	"""Evaluates all fetched images against curation rules and builds a CurationChangeset."""
	changeset = CurationChangeset()

	for img_id, current_source, current_rating, raw_tags in tqdm.tqdm(images_data, desc='Curating Tags'):
		current_tags = [t for t in (raw_tags or ()) if t is not None]
		curated_tags = list(current_tags)

		apply_tag_curation(
			curated_tags,
			dynamic_mappings=merged_map,
			context=ctx,
			single_tag_cache=single_tag_cache,
		)

		extracted_ratings = [
			t.split(':', 1)[1].lower()
			for t in curated_tags
			if t.lower() in CANONICAL_RATING_TAGS
		]
		if extracted_ratings:
			if not (current_rating and current_rating.strip()):
				best_rating = max(extracted_ratings, key=lambda r: RATING_PRIORITY.get(r, 0))
				changeset.rating_updates.append((img_id, best_rating))
			curated_tags = [t for t in curated_tags if t.lower() not in CANONICAL_RATING_TAGS]

		clean_curr_src = current_source.strip() if current_source else ''
		candidate_urls = {
			t.split(':', 1)[1].strip()
			for t in curated_tags
			if t.lower().startswith('source:') and len(t) > 7 and t.split(':', 1)[1].strip()
		}
		if clean_curr_src:
			candidate_urls.add(clean_curr_src)

		if candidate_urls:
			best_url = min(candidate_urls, key=lambda u: (get_source_score(u), u))
			if best_url != clean_curr_src:
				changeset.source_updates.append((img_id, best_url))

		if len(curated_tags) < 15:
			curated_tags.append('tagme')
		if not any(t.startswith('artist:') for t in curated_tags):
			curated_tags.append('artist:tagme')
		if not any(t.startswith('character:') for t in curated_tags):
			curated_tags.append('character:tagme')
		if not any(t.startswith('series:') for t in curated_tags):
			curated_tags.append('series:tagme')

		curr_set = set(current_tags)
		cur_set = set(curated_tags)

		for t in cur_set - curr_set:
			changeset.tags_to_add.append((img_id, t))
			if t not in tag_id_map:
				changeset.new_unique_tags.add(t)

		for t in curr_set - cur_set:
			changeset.tags_to_remove.append((img_id, tag_id_map[t]))

	return changeset


def _apply_curation_updates(
	conn,
	cur,
	changeset: CurationChangeset,
	tag_id_map: dict[str, int],
	db_config: dict,
) -> None:
	"""Executes bulk SQL updates, tag insertions/deletions, count sync, and VACUUM ANALYZE."""
	if changeset.source_updates:
		print('Updating image source fields...')
		psycopg2.extras.execute_values(
			cur,
			"""
			UPDATE images AS i
			SET source = v.new_source
			FROM (VALUES %s) AS v(id, new_source)
			WHERE i.id = v.id
            AND i.source IS DISTINCT FROM v.new_source
			""",
			changeset.source_updates,
			page_size=10000,
		)

	if changeset.rating_updates:
		print('Populating null image rating fields...')
		psycopg2.extras.execute_values(
			cur,
			"""
			UPDATE images AS i
			SET rating = v.new_rating
			FROM (VALUES %s) AS v(id, new_rating)
			WHERE i.id = v.id
            AND (i.rating IS NULL OR BTRIM(i.rating) = '')
			""",
			changeset.rating_updates,
			page_size=10000,
		)

	if changeset.new_unique_tags:
		print(f'Injecting {len(changeset.new_unique_tags):,} entirely new tags into the database...')
		psycopg2.extras.execute_values(
			cur,
			'INSERT INTO tags (tag, count) VALUES %s ON CONFLICT (tag) DO NOTHING',
			[(t, 0) for t in changeset.new_unique_tags],
			page_size=10000,
		)
		cur.execute('SELECT tag, id FROM tags WHERE tag = ANY(%s)', (list(changeset.new_unique_tags),))
		tag_id_map.update(dict(cur.fetchall()))

	if changeset.tags_to_remove:
		print('Purging old tag links...')
		cur.execute('CREATE TEMP TABLE tmp_del (img_id INT, tag_id INT) ON COMMIT DROP')
		psycopg2.extras.execute_values(
			cur, 'INSERT INTO tmp_del VALUES %s', changeset.tags_to_remove, page_size=10000
		)
		cur.execute("""
			DELETE FROM image_tags
			USING tmp_del
			WHERE image_tags.image_id = tmp_del.img_id AND image_tags.tag_id = tmp_del.tag_id
		""")

	if changeset.tags_to_add:
		print('Applying new tag links...')
		insert_records = [(img_id, tag_id_map[t]) for img_id, t in changeset.tags_to_add]
		psycopg2.extras.execute_values(
			cur,
			'INSERT INTO image_tags (image_id, tag_id) VALUES %s ON CONFLICT DO NOTHING',
			insert_records,
			page_size=10000,
		)

	affected_tag_ids = list(
		{tag_id_map[t] for _, t in changeset.tags_to_add}
		| {tid for _, tid in changeset.tags_to_remove}
	)
	if affected_tag_ids:
		print(f'Recalculating UI tag counts for {len(affected_tag_ids):,} affected tags...')
		cur.execute('CREATE TEMP TABLE tmp_affected_tags (tag_id INT PRIMARY KEY) ON COMMIT DROP')
		psycopg2.extras.execute_values(
			cur,
			'INSERT INTO tmp_affected_tags (tag_id) VALUES %s',
			[(tid,) for tid in affected_tag_ids],
			page_size=10000,
		)
		cur.execute("""
			WITH new_counts AS (
				SELECT tat.tag_id, COUNT(it.image_id) AS cnt
				FROM tmp_affected_tags tat
				LEFT JOIN image_tags it ON tat.tag_id = it.tag_id
				GROUP BY tat.tag_id
			)
			UPDATE tags AS t
			SET count = nc.cnt
			FROM new_counts nc
			WHERE t.id = nc.tag_id
		""")

	print('Dropping orphaned tags...')
	cur.execute('DELETE FROM tags WHERE count <= 0')
	conn.commit()

	print('Running VACUUM ANALYZE on tags, image_tags, and images to rebuild visibility maps...')
	vac_conn = psycopg2.connect(**db_config)
	try:
		vac_conn.autocommit = True
		with vac_conn.cursor() as vac_cur:
			vac_cur.execute('VACUUM ANALYZE tags, image_tags, images;')
	finally:
		vac_conn.close()


def curate_existing_tags(args) -> None:
	"""Main CLI entry point for curate-db."""
	merged_map = load_tagmap(getattr(args, 'use_map_csv', None))
	print(f'[INFO] Loaded {len(merged_map):,} dynamic tag mappings.')

	db_config = get_shimmie_db_credentials(args.spath)
	if not db_config:
		print(f'[ERROR] Could not load DB credentials from {args.spath}')
		return

	mappings = load_mappings()
	ctx = get_curation_context(mappings)
	single_tag_cache: dict[str, tuple[str, ...]] = {}

	with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
		print('Fetching global tag map...')
		cur.execute('SELECT tag, id, count FROM tags')
		tag_rows = cur.fetchall()
		tag_id_map = {t: tid for t, tid, _ in tag_rows}
		tag_counts = {t: (cnt or 0) for t, _, cnt in tag_rows}

		ctx.register_db_vocabulary(tag_id_map.keys(), tag_counts=tag_counts)

		print(f'Pre-computing tag expansion cache across {len(tag_id_map):,} unique DB tags...')
		for tag_name in tag_id_map:
			single_tag_cache[tag_name] = ctx.expand_single_tag(tag_name, merged_map)

		images_data = _fetch_target_images(cur, getattr(args, 'target_tag', None))
		if not images_data:
			print('\n[✓] No images matched for curation.')
			return

		print(f'Evaluating {len(images_data):,} images...')
		changeset = _evaluate_curation_changes(
			images_data, merged_map, ctx, single_tag_cache, tag_id_map
		)

		if changeset.is_empty():
			print('\n[✓] All tags are already clean. No changes needed.')
			return

		print(
			f'\n[INFO] Found {len(changeset.tags_to_add):,} missing links to add, '
			f'{len(changeset.tags_to_remove):,} junk links to drop, '
			f'{len(changeset.source_updates):,} image sources to update, and '
			f'{len(changeset.rating_updates):,} null image ratings to populate.'
		)

		_apply_curation_updates(conn, cur, changeset, tag_id_map, db_config)
		print('\n[✓] Retroactive curation complete.')
