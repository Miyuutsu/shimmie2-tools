"""Generates tag mappings by diffing a target batch tag against a reference tag in PostgreSQL."""

import csv
from pathlib import Path
import re

import psycopg2
import tqdm

from functions.db_cache import (
	get_shimmie_db_credentials,
	load_mappings,
	load_tagmap,
	save_tagmap_entries,
)
from functions.source_resolver import resolve_prefixed_tag
from functions.tags_mining import (
	TagCategoryGuard,
	TagFrequencies,
	calculate_equivalencies,
	fetch_global_context,
)

IGNORED_META_PREFIXES = ('booru:', 'tagai:', 'source:', 'site:')
PUNCT_NORM_RE = re.compile(r'[_()-]')


def _fetch_rosetta_data(
	cur,
	target_tag: str,
	ref_tag: str,
) -> tuple[list[tuple[str, int, int, int]], list[tuple[int, str | None, str | None]]]:
	"""Fetches target vs. pure-reference tag counts and overlapping Rosetta Stone images."""
	print(f"[*] Auditing tag distribution across '{target_tag}' vs pure '{ref_tag}'...")
	cur.execute(
		"""
		WITH target_tag_id AS (
			SELECT id FROM tags WHERE tag = %s LIMIT 1
		),
		ref_tag_id AS (
			SELECT id FROM tags WHERE tag = %s LIMIT 1
		),
		target_imgs AS MATERIALIZED (
			SELECT image_id FROM image_tags WHERE tag_id = (SELECT id FROM target_tag_id)
		),
		ref_imgs AS MATERIALIZED (
			SELECT image_id FROM image_tags WHERE tag_id = (SELECT id FROM ref_tag_id)
		),
		pure_ref_imgs AS MATERIALIZED (
			SELECT r.image_id
			FROM ref_imgs r
			LEFT JOIN target_imgs t ON r.image_id = t.image_id
			WHERE t.image_id IS NULL
		),
		target_tag_counts AS (
			SELECT it.tag_id, COUNT(*) AS target_cnt
			FROM image_tags it
			JOIN target_imgs ti ON it.image_id = ti.image_id
			GROUP BY it.tag_id
		),
		pure_ref_tag_counts AS (
			SELECT it.tag_id, COUNT(*) AS pure_ref_cnt
			FROM image_tags it
			JOIN pure_ref_imgs pri ON it.image_id = pri.image_id
			GROUP BY it.tag_id
		)
		SELECT
			t.tag,
			t.count AS global_cnt,
			ttc.target_cnt,
			COALESCE(prtc.pure_ref_cnt, 0) AS pure_ref_cnt
		FROM target_tag_counts ttc
		JOIN tags t ON t.id = ttc.tag_id
		LEFT JOIN pure_ref_tag_counts prtc ON prtc.tag_id = ttc.tag_id
		WHERE t.count > 0;
		""",
		(target_tag, ref_tag),
	)
	all_target_tag_rows = cur.fetchall()

	print(f"[*] Locating Rosetta Stones (Images with both '{target_tag}' and '{ref_tag}')...")
	cur.execute(
		"""
		WITH rosetta_images AS (
			SELECT it1.image_id
			FROM image_tags it1
			JOIN tags t1 ON it1.tag_id = t1.id
			JOIN image_tags it2 ON it1.image_id = it2.image_id
			JOIN tags t2 ON it2.tag_id = t2.id
			WHERE t1.tag = %s AND t2.tag = %s
		)
		SELECT
			r.image_id,
			(
				SELECT th.tags
				FROM tag_histories th
				WHERE th.image_id = r.image_id
				AND %s = ANY(string_to_array(th.tags, ' '))
				AND NOT (%s = ANY(string_to_array(th.tags, ' ')))
				ORDER BY th.id DESC LIMIT 1
			) AS pure_ref_history,
			(
				SELECT string_agg(t.tag, ' ')
				FROM image_tags it
				JOIN tags t ON it.tag_id = t.id
				WHERE it.image_id = r.image_id
			) AS current_tags
		FROM rosetta_images r;
		""",
		(target_tag, ref_tag, ref_tag, target_tag),
	)
	rosetta_rows = cur.fetchall()
	print(f'	-> Found {len(rosetta_rows):,} overlapping images to analyze.')
	return all_target_tag_rows, rosetta_rows


def _build_rosetta_frequencies(
	rosetta_rows: list[tuple[int, str | None, str | None]],
	established_canonical: set[str],
	target_tag: str,
	is_booru_batch: bool,
) -> TagFrequencies:
	"""Builds TagFrequencies from overlapping Rosetta Stone images."""
	freqs = TagFrequencies()

	for _, pure_ref_history, current_tags in tqdm.tqdm(rosetta_rows, desc='Mapping Frequencies'):
		if not current_tags:
			continue

		current_set = {t for t in current_tags.split() if not t.startswith(IGNORED_META_PREFIXES)}

		if pure_ref_history:
			ref_hist_set = {t for t in pure_ref_history.split() if not t.startswith(IGNORED_META_PREFIXES)}
			canonical = ref_hist_set | (current_set & established_canonical)
			if not is_booru_batch:
				canonical.discard(target_tag)
		else:
			canonical = {t for t in current_set if t in established_canonical}

		botched_tags = current_set - canonical
		if not canonical or not botched_tags:
			continue

		freqs.valid_pairs += 1
		for s_tag in botched_tags:
			freqs.sidecar_counts[s_tag] += 1
			for c_tag in canonical:
				freqs.co_occurrences[s_tag][c_tag] += 1

		for c_tag in canonical:
			freqs.canonical_counts[c_tag] += 1

	return freqs


def _run_pass2_rescues(
	all_target_tag_rows: list[tuple[str, int, int, int]],
	calculated: list[dict[str, str | float | int]],
	existing_map: dict[str, str],
	guard: TagCategoryGuard,
	known_valid_general: set[str],
	target_tag: str,
	ref_tag: str,
	is_booru_batch: bool,
) -> tuple[list[dict[str, str | float | int]], int]:
	"""Normalizes foreign prefixes, rescues punctuation variants, and flags known junk for _DROP_."""
	mapped_tags = {str(r['Sidecar_Tag']) for r in calculated} | {str(r['Canonical_Tag']) for r in calculated}
	drop_results: list[dict[str, str | float | int]] = []
	skipped_protected = 0

	for tag_name, _, target_cnt, pure_ref_cnt in all_target_tag_rows:
		tag_lower = tag_name.lower()

		if tag_name in existing_map:
			if tag_name not in mapped_tags:
				target_val = existing_map[tag_name]
				bucket = drop_results if target_val == '_DROP_' else calculated
				bucket.append({
					'Sidecar_Tag': tag_name,
					'Canonical_Tag': target_val,
					'Confidence': 1.0,
					'Sample_Size': target_cnt,
				})
				mapped_tags.add(tag_name)
			continue

		if tag_name == target_tag and not is_booru_batch and target_tag != ref_tag:
			if tag_name not in mapped_tags:
				calculated.append({
					'Sidecar_Tag': target_tag,
					'Canonical_Tag': ref_tag,
					'Confidence': 1.0,
					'Sample_Size': target_cnt,
				})
				mapped_tags.add(tag_name)
			continue

		if tag_name in mapped_tags:
			continue

		resolved_prefix_tag = resolve_prefixed_tag(tag_name)
		if resolved_prefix_tag == '_DROP_':
			drop_results.append({
				'Sidecar_Tag': tag_name,
				'Canonical_Tag': '_DROP_',
				'Confidence': 1.0,
				'Sample_Size': target_cnt,
			})
			mapped_tags.add(tag_name)
			continue

		if resolved_prefix_tag:
			final_target = existing_map.get(resolved_prefix_tag, resolved_prefix_tag)
			if ':' not in final_target:
				norm_res = PUNCT_NORM_RE.sub('', final_target.lower())
				if len(norm_res) >= 3 and norm_res in guard.norm_general_map:
					final_target = guard.norm_general_map[norm_res]

			if final_target != tag_name:
				calculated.append({
					'Sidecar_Tag': tag_name,
					'Canonical_Tag': final_target,
					'Confidence': 1.0,
					'Sample_Size': target_cnt,
				})
				mapped_tags.add(tag_name)
			continue

		if (
			':' in tag_name
			or guard.get_category(tag_name) in {'artist', 'character', 'series'}
			or tag_name in known_valid_general
			or pure_ref_cnt > 0
		):
			skipped_protected += 1
			continue

		norm_key = PUNCT_NORM_RE.sub('', tag_lower)
		if len(norm_key) >= 3 and norm_key in guard.norm_general_map:
			canonical_match = guard.norm_general_map[norm_key]
			if canonical_match != tag_name:
				calculated.append({
					'Sidecar_Tag': tag_name,
					'Canonical_Tag': canonical_match,
					'Confidence': 1.0,
					'Sample_Size': target_cnt,
				})
				mapped_tags.add(tag_name)

	return drop_results, skipped_protected


def generate_db_curation_map(args) -> None:
	"""Main CLI entry point for generate-db-map."""
	db_config = get_shimmie_db_credentials(args.spath)
	if not db_config:
		print(f'[ERROR] Could not load DB credentials from {args.spath}')
		return

	target_tag = args.target_tag
	ref_tag = args.reference_tag
	is_booru_batch = target_tag.lower().startswith('booru:') and ref_tag.lower().startswith('booru:')
	out_path = Path(getattr(args, 'out', None) or 'botched_map.csv')

	existing_map = load_tagmap(getattr(args, 'use_map_csv', None))
	print(f'[INFO] Loaded {len(existing_map):,} existing mappings to respect.')

	print('[*] Loading canonical dictionaries (tag_groups.json, characters, artists, ratings)...')
	mappings = load_mappings()
	guard = TagCategoryGuard(mappings)
	known_valid_general = set(guard.known_general) | set(mappings.rating)
	if not is_booru_batch:
		known_valid_general.discard(target_tag)

	with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
		all_target_tag_rows, rosetta_rows = _fetch_rosetta_data(cur, target_tag, ref_tag)

	established_canonical = set(known_valid_general)
	for t_name, _, _, pure_ref_cnt in all_target_tag_rows:
		if not is_booru_batch and t_name == target_tag:
			continue
		if resolve_prefixed_tag(t_name) is not None:
			continue
		if pure_ref_cnt >= 5 or guard.get_category(t_name) in {'artist', 'character', 'series'}:
			established_canonical.add(t_name)

	print('[*] Translating DB overlap into frequency tables...')
	freqs = _build_rosetta_frequencies(rosetta_rows, established_canonical, target_tag, is_booru_batch)
	print(
		f'	-> Extracted {freqs.valid_pairs:,} valid tag diffs '
		f'({len(freqs.sidecar_counts):,} unique suspect tags vs {len(freqs.canonical_counts):,} canonical tags).'
	)

	all_tags = set(freqs.sidecar_counts) | set(freqs.canonical_counts)
	global_counts = fetch_global_context(all_tags, db_config)

	print('[*] Pass 1: Calculating 1:1 synonym equivalencies from Rosetta Stones...')
	calculated = calculate_equivalencies(freqs, global_counts, guard, thresholds=(5, 0.50), mode='retro')
	print(f'	-> Discovered {len(calculated):,} 1:1 synonym mappings.')

	print(f"[*] Pass 2: Normalizing foreign prefixes and filtering known junk on '{target_tag}'...")
	drop_results, skipped_protected = _run_pass2_rescues(
		all_target_tag_rows, calculated, existing_map, guard,
		known_valid_general, target_tag, ref_tag, is_booru_batch,
	)

	print(
		f'	-> Total 1:1 mappings & prefix rescues: {len(calculated):,} | '
		f"Known junk flagged for '_DROP_': {len(drop_results):,} "
		f'(protected {skipped_protected:,} valid tags).'
	)

	all_results = calculated + drop_results
	all_results.sort(key=lambda x: (x['Canonical_Tag'] == '_DROP_', -int(x['Sample_Size'])))

	if out_path.suffix.lower() == '.db':
		saved = save_tagmap_entries(all_results, out_path)
		print(f'\n[✓] Upserted {saved:,} rules directly into {out_path.resolve()}')
		return

	print(f'[*] Writing {out_path.name}...')
	with out_path.open('w', encoding='utf-8', newline='') as f:
		writer = csv.DictWriter(f, fieldnames=['Sidecar_Tag', 'Canonical_Tag', 'Confidence', 'Sample_Size'])
		writer.writeheader()
		writer.writerows(all_results)

	print(
		f'\n[✓] Smart mapping complete! Wrote {len(all_results):,} total rules '
		f'({len(calculated):,} 1:1 equivalencies + {len(drop_results):,} _DROP_ tags) to {out_path.resolve()}.'
	)
