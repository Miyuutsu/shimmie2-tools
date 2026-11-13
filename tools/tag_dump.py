"""Dumps low-frequency PostgreSQL tags to a CSV or tagmap.db with a diagnostic impact log."""

from collections import defaultdict
import csv
from dataclasses import dataclass
from pathlib import Path
import re

import psycopg2

from functions.db_cache import get_shimmie_db_credentials, save_tagmap_entries

NAMESPACE_PREFIX_RE = re.compile(r'[a-z0-9_-]{1,20}')


@dataclass(slots=True)
class DumpMetadata:
	"""Holds configuration and skip counters for diagnostic log generation."""

	min_count: int
	drop_threshold: int
	exclude_prefixes: tuple[str, ...]
	exclude_substrings: tuple[str, ...]
	skipped_counts: tuple[int, int]


def _classify_namespace(tag_name: str) -> str:
	"""Returns the namespace prefix bucket for a tag so foreign prefixes are visible in the .log."""
	if ':' not in tag_name:
		return 'general (unprefixed)'
	prefix = tag_name.split(':', 1)[0].lower()
	if NAMESPACE_PREFIX_RE.fullmatch(prefix):
		return f'{prefix}:'
	return 'other (prefixed)'


def _parse_comma_list(raw_val: str | None) -> tuple[str, ...]:
	"""Parses a comma-separated CLI string into a lowercase tuple."""
	if not raw_val:
		return ()
	return tuple(item.strip().lower() for item in raw_val.split(',') if item.strip())


def _write_tag_dump_log(
	cur,
	filtered_rows: list[tuple[int, str, int]],
	meta: DumpMetadata,
	log_path: Path,
) -> None:
	"""Queries impact metrics for the filtered tag set and writes a diagnostic .log file."""
	print(f'[*] Generating impact report for {log_path.name}...')

	ns_stats: dict[str, dict[str, int]] = defaultdict(
		lambda: {
			'dumped_tags': 0,
			'drop_tags': 0,
			'review_tags': 0,
			'dumped_links': 0,
			'drop_links': 0,
		}
	)
	top_drop_candidates: list[tuple[str, int]] = []
	dumped_ids: list[int] = []
	drop_ids: list[int] = []

	for tag_id, tag_name, count in filtered_rows:
		ns = _classify_namespace(tag_name)
		ns_stats[ns]['dumped_tags'] += 1
		ns_stats[ns]['dumped_links'] += count
		dumped_ids.append(tag_id)

		if count <= meta.drop_threshold:
			ns_stats[ns]['drop_tags'] += 1
			ns_stats[ns]['drop_links'] += count
			drop_ids.append(tag_id)
			if len(top_drop_candidates) < 100:
				top_drop_candidates.append((tag_name, count))
		else:
			ns_stats[ns]['review_tags'] += 1

	booru_rows = []
	if dumped_ids:
		cur.execute(
			"""
			WITH target_dumped_tags AS (
				SELECT id, count AS global_tag_count
				FROM tags
				WHERE id = ANY(%s)
				AND tag NOT LIKE 'booru:%%'
			),
			booru_tags AS (
				SELECT id, tag, count AS total_images
				FROM tags
				WHERE tag LIKE 'booru:%%' AND count > 0
			),
			booru_tag_pairs AS (
				SELECT
					bt.tag AS booru_tag,
					bt.total_images,
					dt.id AS dumped_tag_id,
					dt.global_tag_count,
					COUNT(it_dumped.image_id) AS booru_occurrences
				FROM booru_tags bt
				JOIN image_tags it_booru ON bt.id = it_booru.tag_id
				JOIN image_tags it_dumped ON it_booru.image_id = it_dumped.image_id
				JOIN target_dumped_tags dt ON it_dumped.tag_id = dt.id
				GROUP BY bt.tag, bt.total_images, dt.id, dt.global_tag_count
			)
			SELECT
				booru_tag,
				total_images,
				COUNT(dumped_tag_id) AS unique_dumped_tags,
				COUNT(dumped_tag_id) FILTER (WHERE global_tag_count <= %s) AS unique_drop_tags,
				COUNT(dumped_tag_id) FILTER (
					WHERE global_tag_count <= %s AND booru_occurrences = global_tag_count
				) AS exclusive_drop_tags,
				COALESCE(SUM(booru_occurrences), 0) AS total_dumped_links,
				COALESCE(SUM(booru_occurrences) FILTER (WHERE global_tag_count <= %s), 0) AS total_drop_links
			FROM booru_tag_pairs
			GROUP BY booru_tag, total_images
			ORDER BY unique_drop_tags DESC, total_drop_links DESC;
			""",
			(dumped_ids, meta.drop_threshold, meta.drop_threshold, meta.drop_threshold),
		)
		booru_rows = cur.fetchall()

	global_impact_rows = []
	if drop_ids:
		cur.execute(
			"""
			WITH target_drop_tags AS (
				SELECT id
				FROM tags
				WHERE id = ANY(%s)
			),
			drop_per_image AS (
				SELECT
					it.image_id,
					COUNT(*) AS drop_links
				FROM image_tags it
				JOIN target_drop_tags tdt ON it.tag_id = tdt.id
				GROUP BY it.image_id
			)
			SELECT
				t.tag,
				t.count AS total_images,
				COUNT(dpi.image_id) AS impacted_images,
				SUM(dpi.drop_links) AS total_drop_links
			FROM drop_per_image dpi
			JOIN image_tags it ON dpi.image_id = it.image_id
			JOIN tags t ON it.tag_id = t.id
			WHERE t.count > %s
			AND t.tag NOT LIKE 'tagai:%%'
			GROUP BY t.id, t.tag, t.count
			ORDER BY total_drop_links DESC, impacted_images DESC
			LIMIT 100;
			""",
			(drop_ids, meta.drop_threshold),
		)
		global_impact_rows = cur.fetchall()

	sorted_ns = sorted(ns_stats.items(), key=lambda x: x[1]['drop_tags'], reverse=True)

	with log_path.open('w', encoding='utf-8') as f:
		f.write('=== TAG DUMP SUMMARY & NAMESPACE BREAKDOWN ===\n')
		f.write(f'Max Dump Threshold (--min): <= {meta.min_count:,} uses\n')
		f.write(f'Auto-_DROP_ Threshold:      <= {meta.drop_threshold:,} uses\n')
		f.write(
			f"Excluded Prefixes:          {', '.join(meta.exclude_prefixes) if meta.exclude_prefixes else 'None'} "
			f'({meta.skipped_counts[0]:,} tags skipped)\n'
		)
		f.write(
			f"Excluded Substrings:        {', '.join(meta.exclude_substrings) if meta.exclude_substrings else 'None'} "
			f'({meta.skipped_counts[1]:,} tags skipped)\n'
		)
		f.write(f'Total Dumped Unique Tags:   {len(filtered_rows):,}\n\n')

		for ns, data in sorted_ns:
			f.write(
				f'  - {ns:<22} | '
				f"Total: {data['dumped_tags']:>8,} tags ({data['dumped_links']:>10,} links) | "
				f"_DROP_: {data['drop_tags']:>8,} tags ({data['drop_links']:>10,} links) | "
				f"Review: {data['review_tags']:>7,} tags\n"
			)

		f.write('\n=== BOORU / SOURCE BATCH IMPACT (booru:*) ===\n')
		f.write('(Shows which booru imports contain the dumped/_DROP_ tags after exclusions.)\n')
		if booru_rows:
			for b_tag, total_imgs, u_dumped, u_drop, excl_drop, l_dumped, l_drop in booru_rows:
				f.write(
					f'  - {b_tag} ({total_imgs:,} total images):\n'
					f'      Unique Dumped Tags: {u_dumped:,} ({l_dumped:,} tag links)\n'
					f'      Unique _DROP_ Tags: {u_drop:,} ({l_drop:,} tag links) '
					f'[{excl_drop:,} tags are 100% EXCLUSIVE to {b_tag}]\n'
				)
		else:
			f.write('  No matching booru:* tag overlaps found.\n')

		f.write('\n=== TOP 100 GLOBAL TAGS MOST IMPACTED BY _DROP_ ===\n')
		f.write('(Surviving tags whose images will have the highest number of _DROP_ tags stripped.)\n')
		for tag_name, total_imgs, impacted_imgs, drop_links in global_impact_rows:
			pct = (impacted_imgs / total_imgs * 100.0) if total_imgs > 0 else 0.0
			avg_stripped = (drop_links / impacted_imgs) if impacted_imgs > 0 else 0.0
			f.write(
				f'  - {tag_name}: {drop_links:,} _DROP_ links across {impacted_imgs:,}/{total_imgs:,} images '
				f'({pct:.1f}% of tag\'s images affected, avg {avg_stripped:.1f} dropped/img)\n'
			)

		f.write(f'\n=== TOP 100 HIGHEST-COUNT TAGS ASSIGNED _DROP_ (<= {meta.drop_threshold:,} USES) ===\n')
		f.write('(Sanity check: highest-usage tags that will be deleted if not manually cleared.)\n')
		for tag_name, count in top_drop_candidates:
			f.write(f'  - {tag_name}: {count:,} uses\n')

	print(f'[✓] Wrote diagnostic log to {log_path.resolve()}')


def tag_dump(args) -> None:
	"""Main CLI entry point for tag-dump."""
	db_config = get_shimmie_db_credentials(args.spath)
	if not db_config:
		print(f'[ERROR] Could not load DB credentials from {args.spath}')
		return

	min_count = args.min
	dv_ratio = args.dv / 100.0 if args.dv > 1.0 else args.dv
	drop_threshold = int(min_count * dv_ratio)
	exclude_prefixes = _parse_comma_list(getattr(args, 'exclude_prefix', ''))
	exclude_substrings = _parse_comma_list(getattr(args, 'exclude', ''))

	out_path = Path(args.out)
	log_path = out_path.with_suffix('.log')

	print(f'[*] Querying tags with count <= {min_count:,} (auto-_DROP_ threshold: <= {drop_threshold:,})...')

	with psycopg2.connect(**db_config) as conn:
		conn.set_session(readonly=True)
		with conn.cursor() as cur:
			cur.execute(
				"""
				SELECT id, tag, count
				FROM tags
				WHERE count > 0
				AND count <= %s
				AND tag NOT LIKE 'tagai:%%'
				ORDER BY count DESC, tag ASC
				""",
				(min_count,),
			)
			raw_rows = cur.fetchall()

			filtered_rows: list[tuple[int, str, int]] = []
			skipped_prefix = 0
			skipped_substr = 0

			for tag_id, tag_name, count in raw_rows:
				tag_lower = tag_name.lower()
				if exclude_prefixes and tag_lower.startswith(exclude_prefixes):
					skipped_prefix += 1
					continue
				if exclude_substrings and any(sub in tag_lower for sub in exclude_substrings):
					skipped_substr += 1
					continue
				filtered_rows.append((tag_id, tag_name, count))

			if not filtered_rows:
				print(
					f'[✓] No tags matched criteria after exclusions '
					f'(skipped {skipped_prefix:,} by prefix, {skipped_substr:,} by substring).'
				)
				return

			drop_assigned = 0
			manual_review = 0

			if out_path.suffix.lower() == '.db':
				db_entries = [
					(tag_name, '_DROP_', 1.0, count)
					for _, tag_name, count in filtered_rows
					if count <= drop_threshold
				]
				drop_assigned = save_tagmap_entries(db_entries, out_path)
				manual_review = len(filtered_rows) - drop_assigned
				print(f'[✓] Upserted {drop_assigned:,} auto-_DROP_ tags into {out_path.resolve()}')
			else:
				with out_path.open('w', encoding='utf-8', newline='') as f:
					writer = csv.writer(f)
					writer.writerow(['Sidecar_Tag', 'Canonical_Tag'])
					for _, tag_name, count in filtered_rows:
						if count <= drop_threshold:
							writer.writerow([tag_name, '_DROP_'])
							drop_assigned += 1
						else:
							writer.writerow([tag_name, ''])
							manual_review += 1
				print(f'[✓] Dumped {len(filtered_rows):,} tags to {out_path.resolve()}')

			if skipped_prefix or skipped_substr:
				print(f'	-> Excluded {skipped_prefix:,} tags via --exclude-prefix and {skipped_substr:,} via --exclude')
			print(f'	-> {manual_review:,} tags in review range ({drop_threshold + 1:,} to {min_count:,} uses)')
			print(f"	-> {drop_assigned:,} tags assigned '_DROP_' (<= {drop_threshold:,} uses)")

			meta = DumpMetadata(
				min_count=min_count,
				drop_threshold=drop_threshold,
				exclude_prefixes=exclude_prefixes,
				exclude_substrings=exclude_substrings,
				skipped_counts=(skipped_prefix, skipped_substr),
			)
			_write_tag_dump_log(cur, filtered_rows, meta, log_path)
