"""Purges images matching blacklist.txt rules from Shimmie2 PostgreSQL and disk."""

from collections import defaultdict
from pathlib import Path
import re

import psycopg2
import psycopg2.extras
import tqdm

from functions.common import BlacklistConfig, expand_tags_for_blacklist, load_blacklist
from functions.db_cache import get_shimmie_db_credentials

SAFE_HASH_RE = re.compile(r'^[a-fA-F0-9]{32,64}$')


def _is_safe_hash(hsh: str) -> bool:
	"""Verifies a database hash is a valid hex digest before unlinking files on disk."""
	return bool(hsh and SAFE_HASH_RE.fullmatch(hsh))


def _fetch_suspects(
	cur,
	rules: list[dict[str, list[str] | str]],
) -> tuple[list[tuple[int, str, str]], dict[str, list[tuple[set[str], set[str], str]]]]:
	"""Indexes database tag frequencies to anchor each rule on its rarest positive tag and fetches suspect images."""
	cur.execute('SELECT tag, id, count FROM tags')
	tag_counts: dict[str, int] = defaultdict(int)
	tag_to_ids: dict[str, list[int]] = defaultdict(list)

	for t_name, t_id, t_count in cur.fetchall():
		if not t_name:
			continue
		safe_count = t_count or 0
		for expanded in expand_tags_for_blacklist((t_name.lower(),)):
			tag_counts[expanded] += safe_count
			tag_to_ids[expanded].append(t_id)

	rules_by_anchor: dict[str, list[tuple[set[str], set[str], str]]] = defaultdict(list)
	anchor_tag_ids: set[int] = set()

	for rule in rules:
		pos_list: list[str] = rule['pos']  # type: ignore[assignment]
		neg_list: list[str] = rule['neg']  # type: ignore[assignment]
		raw_rule = str(rule['raw'])

		if any(p not in tag_to_ids for p in pos_list):
			continue

		anchor = min(pos_list, key=lambda p: tag_counts[p])
		rules_by_anchor[anchor].append((set(pos_list), set(neg_list), raw_rule))
		anchor_tag_ids.update(tag_to_ids[anchor])

	if not anchor_tag_ids:
		return [], rules_by_anchor

	cur.execute(
		"""
		WITH suspect_images AS (
			SELECT DISTINCT it.image_id
			FROM image_tags it
			WHERE it.tag_id = ANY(%s)
		)
		SELECT i.id, i.hash, string_agg(t.tag, ' ') AS all_tags
		FROM suspect_images s
		JOIN images i ON s.image_id = i.id
		JOIN image_tags it ON i.id = it.image_id
		JOIN tags t ON it.tag_id = t.id
		GROUP BY i.id, i.hash
		""",
		(list(anchor_tag_ids),),
	)
	return cur.fetchall(), rules_by_anchor


def _evaluate_suspects(
	suspects: list[tuple[int, str, str]],
	rules: list[dict[str, list[str] | str]],
	rules_by_anchor: dict[str, list[tuple[set[str], set[str], str]]],
	preserve_tags: set[str],
) -> tuple[list[tuple[int, str, str, list[str]]], list[tuple[str, int]]]:
	"""Evaluates suspect images against anchored blacklist rules and global preservation tags."""
	trash_images: list[tuple[int, str, str, list[str]]] = []
	breakdown_counts = {str(r['raw']): 0 for r in rules}

	for img_id, hsh, tag_string in tqdm.tqdm(suspects, desc='Evaluating Suspects', leave=False):
		raw_tags = (tag_string or '').lower().split()
		clean_tags = expand_tags_for_blacklist(raw_tags)

		if preserve_tags and not preserve_tags.isdisjoint(clean_tags):
			continue

		triggered_rules: list[str] = []
		for tag in clean_tags:
			if tag not in rules_by_anchor:
				continue
			for pos_set, neg_set, raw_rule in rules_by_anchor[tag]:
				if pos_set.issubset(clean_tags) and neg_set.isdisjoint(clean_tags):
					triggered_rules.append(raw_rule)

		if triggered_rules:
			unique_triggered = list(dict.fromkeys(triggered_rules))
			for r in unique_triggered:
				breakdown_counts[r] += 1
			trash_images.append((img_id, hsh, tag_string, unique_triggered))

	sorted_breakdown = [
		item for item in sorted(breakdown_counts.items(), key=lambda x: x[1], reverse=True)
		if item[1] > 0
	]
	return trash_images, sorted_breakdown


def _write_dry_run_report(
	cur,
	trash_images: list[tuple[int, str, str, list[str]]],
	sorted_breakdown: list[tuple[str, int]],
	blacklist_cfg: BlacklistConfig,
	report_path: Path,
) -> None:
	"""Generates the dry-run casualty and collateral tag report without modifying disk or DB."""
	ignore_prefixes = ('booru:', 'tagai:')
	associated_tags: dict[str, dict[str, int | dict[str, int]]] = {}

	for _, _, tag_string, triggered_rules in trash_images:
		for t in tag_string.split():
			t_lower = t.lower()
			if t_lower.startswith(ignore_prefixes) or t_lower in blacklist_cfg.ignore_tags:
				continue

			expanded_t = expand_tags_for_blacklist((t_lower,))
			if not expanded_t.isdisjoint(blacklist_cfg.prefilter_tags):
				continue

			if t not in associated_tags:
				associated_tags[t] = {'total': 0, 'rules': {}}

			associated_tags[t]['total'] = int(associated_tags[t]['total']) + 1
			rules_dict: dict[str, int] = associated_tags[t]['rules']  # type: ignore[assignment]
			for rule in triggered_rules:
				rules_dict[rule] = rules_dict.get(rule, 0) + 1

	top_by_purge = sorted(associated_tags.items(), key=lambda x: int(x[1]['total']), reverse=True)[:100]
	top_tag_names = [t[0] for t in top_by_purge]

	db_totals: dict[str, int] = {}
	if top_tag_names:
		cur.execute('SELECT tag, count FROM tags WHERE tag = ANY(%s)', (top_tag_names,))
		db_totals = dict(cur.fetchall())

	final_top_100: list[dict[str, str | int]] = []
	for tag_name, tag_data in top_by_purge:
		purge_count = int(tag_data['total'])
		total = db_totals.get(tag_name, 0)
		remaining = max(0, total - purge_count)
		rules_dict = tag_data['rules']  # type: ignore[assignment]
		top_rule, top_rule_count = max(rules_dict.items(), key=lambda item: item[1])

		final_top_100.append({
			'tag_name': tag_name,
			'purge_count': purge_count,
			'remaining': remaining,
			'top_rule': top_rule,
			'top_rule_count': top_rule_count,
		})

	final_top_100.sort(key=lambda x: (int(x['remaining']), -int(x['purge_count'])))

	with report_path.open('w', encoding='utf-8') as f:
		f.write('=== CASUALTY BREAKDOWN BY RULE ===\n')
		for rule_text, count in sorted_breakdown:
			f.write(f"  - '{rule_text}': {count:,} images\n")

		f.write('\n=== TOP 100 MOST COMMON ASSOCIATED TAGS ===\n')
		f.write('(Sorted by lowest remaining count. Shows tags heavily impacted by the crossfire.)\n')
		for item in final_top_100:
			f.write(
				f"  - {item['tag_name']}: {item['remaining']:,} remaining images "
				f"({item['purge_count']:,} purging) "
				f"(Primary Offender: '{item['top_rule']}' with {item['top_rule_count']:,} hits)\n"
			)

		f.write('\n=== IMAGES FLAGGED FOR DELETION ===\n')
		for img_id, hsh, tag_string, triggered_rules in trash_images:
			f.write(f"ID: {img_id} | Hash: {hsh} | Triggered By: [{', '.join(triggered_rules)}] | Tags: {tag_string}\n")

	print(
		f'[ℹ️ DRY RUN] Safe abort. Wrote full breakdown, collateral tags, '
		f'and {len(trash_images):,} targets to {report_path.resolve()}'
	)


def _execute_purge(
	conn,
	cur,
	trash_images: list[tuple[int, str, str, list[str]]],
	shimmie_root: Path,
) -> None:
	"""Commits PostgreSQL deletions and tag count updates first, then unlinks physical files from disk."""
	images_dir = shimmie_root / 'data' / 'images'
	thumbs_dir = shimmie_root / 'data' / 'thumbs'
	if not images_dir.is_dir():
		raise FileNotFoundError(f'Shimmie images directory not found: {images_dir}')

	img_ids_to_drop = [img_id for img_id, *_ in trash_images]

	print('\n[Step 1/3] Scrubbing PostgreSQL database...')
	print('  -> Dropping metadata links (image_tags)...')
	cur.execute(
		'DELETE FROM image_tags WHERE image_id = ANY(%s) RETURNING tag_id',
		(img_ids_to_drop,),
	)
	affected_tag_ids = list({row[0] for row in cur.fetchall() if row[0] is not None})

	print('  -> Dropping core image records (images)...')
	cur.execute('DELETE FROM images WHERE id = ANY(%s)', (img_ids_to_drop,))

	print('\n[Step 2/3] Updating Shimmie tag UI counts...')
	if affected_tag_ids:
		cur.execute('CREATE TEMP TABLE tmp_purged_tags (tag_id INT PRIMARY KEY) ON COMMIT DROP')
		psycopg2.extras.execute_values(
			cur,
			'INSERT INTO tmp_purged_tags (tag_id) VALUES %s',
			[(tid,) for tid in affected_tag_ids],
			page_size=10000,
		)
		cur.execute("""
			WITH new_counts AS (
				SELECT tpt.tag_id, COUNT(it.image_id) AS cnt
				FROM tmp_purged_tags tpt
				LEFT JOIN image_tags it ON tpt.tag_id = it.tag_id
				GROUP BY tpt.tag_id
			)
			UPDATE tags AS t
			SET count = nc.cnt
			FROM new_counts nc
			WHERE t.id = nc.tag_id
		""")

	print('  -> Pruning orphaned tags with 0 count...')
	cur.execute('DELETE FROM tags WHERE count <= 0')
	conn.commit()

	print('\n[Step 3/3] Deleting physical files from disk...')
	deleted_files = 0
	for _, hsh, *_ in tqdm.tqdm(trash_images, desc='Files Purged', unit='file'):
		if not _is_safe_hash(hsh):
			print(f'\n[WARNING] Skipping invalid hash format on disk: {hsh!r}')
			continue

		prefix1, prefix2 = hsh[0:2], hsh[2:4]
		img_file = images_dir / prefix1 / prefix2 / hsh
		thumb_file = thumbs_dir / prefix1 / prefix2 / hsh

		if img_file.is_file():
			img_file.unlink()
			deleted_files += 1
		if thumb_file.is_file():
			thumb_file.unlink()

	print(f'\n[✓] Purge Complete: Destroyed {len(img_ids_to_drop):,} DB entries and {deleted_files:,} image files.')


def purge_images(args) -> None:
	"""Main CLI entry point for purge."""
	try:
		blacklist_cfg = load_blacklist(Path(args.blacklist))
	except (FileNotFoundError, ValueError) as e:
		print(f'[ERROR] {e}')
		return

	print(
		f'[INFO] Loaded {len(blacklist_cfg.rules):,} rules and '
		f'{len(blacklist_cfg.preserve_tags):,} global preservation tags. Hunting...'
	)

	db_config = get_shimmie_db_credentials(args.spath)
	if not db_config:
		print(f'[ERROR] Could not load DB credentials from {args.spath}')
		return

	shimmie_root = Path(args.spath)

	with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
		suspects, rules_by_anchor = _fetch_suspects(cur, blacklist_cfg.rules)
		if not suspects:
			print('[✓] Database is clean. No images found matching the blacklist.')
			return

		trash_images, sorted_breakdown = _evaluate_suspects(
			suspects, blacklist_cfg.rules, rules_by_anchor, blacklist_cfg.preserve_tags
		)
		if not trash_images:
			print('[✓] Database is clean. No images found matching the blacklist.')
			return

		print(f'\n[⚠️ WARNING] Found {len(trash_images):,} unique images to permanently delete.')
		print('\n=== CASUALTY BREAKDOWN BY RULE ===')
		for rule_text, count in sorted_breakdown[:20]:
			print(f"  - '{rule_text}': {count:,} images")
		if len(sorted_breakdown) > 20:
			print(f'  ... and {len(sorted_breakdown) - 20:,} more rules.')
		print('==================================\n')

		if args.dry_run:
			_write_dry_run_report(cur, trash_images, sorted_breakdown, blacklist_cfg, Path('purge_dry_run.txt'))
			return

		confirm = input("Type 'YES' to delete files and database records: ")
		if confirm != 'YES':
			print('Aborting.')
			return

		_execute_purge(conn, cur, trash_images, shimmie_root)
