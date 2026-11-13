"""Audits and repairs tag mappings in tagmap.db or CSV files, and reverts poisoned tags in PostgreSQL."""

import csv
from pathlib import Path
import re
import shutil
import sqlite3

import psycopg2
import psycopg2.extras

from functions.common import TAGMAP_DB_PATH
from functions.db_cache import (
	get_shimmie_db_credentials,
	init_tagmap_db,
	load_mappings,
	load_tagmap,
	save_tagmap_entries,
)
from functions.tags_curation import TagCurationContext, apply_tag_curation, get_curation_context

PUNCT_NORM_RE = re.compile(r'[_()-]')


def _is_number(val: str) -> bool:
	"""Returns True if val is a valid integer or float string."""
	try:
		float(val)
		return True
	except ValueError:
		return False


def _is_legitimate_history_tag(bad_tag: str, hist_tokens: set[str], ctx: TagCurationContext) -> bool:
	"""Returns True if the image's pre-curation tag_histories genuinely contained bad_tag."""
	bad_lower = bad_tag.lower()
	if bad_lower.startswith('artist:'):
		base_a = bad_lower.split(':', 1)[1]
		if base_a not in ctx.artist_set:
			return False
	elif bad_lower.startswith('character:'):
		base_c = bad_lower.split(':', 1)[1]
		if base_c not in ctx.char_map and '_(' not in base_c:
			return False
	elif bad_lower.startswith('series:'):
		base_s = bad_lower.split(':', 1)[1]
		if base_s not in ctx.series_set:
			return False

	return any(
		tok == bad_lower or bad_tag in ctx.expand_single_tag(tok, {})
		for tok in hist_tokens
	)


def _revert_single_tag(cur, raw_spec: str, ctx: TagCurationContext, merged_map: dict[str, str]) -> None:
	"""Reverts a single poisoned tag in Postgres using tag_histories (or an explicit =replacement)."""
	if '=' in raw_spec:
		target_input, explicit_restore = [p.strip() for p in raw_spec.split('=', 1)]
	else:
		target_input, explicit_restore = raw_spec.strip(), None

	candidates = [target_input]
	if ':' not in target_input:
		candidates.extend([f'artist:{target_input}', f'character:{target_input}', f'series:{target_input}'])

	bad_id: int | None = None
	bad_tag: str | None = None
	for cand in candidates:
		cur.execute('SELECT id, tag FROM tags WHERE tag = %s', (cand,))
		if row := cur.fetchone():
			bad_id, bad_tag = row[0], row[1]
			break

	if bad_id is None or bad_tag is None:
		print(f"[!] Tag '{target_input}' not found in PostgreSQL tags table.")
		return

	cur.execute('SELECT image_id FROM image_tags WHERE tag_id = %s', (bad_id,))
	affected_ids = [r[0] for r in cur.fetchall()]
	if not affected_ids:
		print(f"[✓] Tag '{bad_tag}' has 0 linked images.")
		return

	print(f"\n[*] Auditing {len(affected_ids):,} images tagged '{bad_tag}' against tag_histories...")
	cur.execute('CREATE TEMP TABLE tmp_rev_cand (img_id INT PRIMARY KEY) ON COMMIT DROP')
	psycopg2.extras.execute_values(
		cur,
		'INSERT INTO tmp_rev_cand (img_id) VALUES %s',
		[(iid,) for iid in affected_ids],
		page_size=10000,
	)

	cur.execute("""
		SELECT DISTINCT ON (th.image_id) th.image_id, th.tags
		FROM tag_histories th
		JOIN tmp_rev_cand trc ON th.image_id = trc.img_id
		ORDER BY th.image_id, th.id DESC
	""")
	hist_map = dict(cur.fetchall())

	poisoned_ids: list[int] = []
	restore_pairs: list[tuple[int, str]] = []
	touched_tag_names = {bad_tag, 'artist:tagme', 'character:tagme', 'series:tagme'}
	single_cache: dict[str, tuple[str, ...]] = {}

	for img_id in affected_ids:
		hist_str = hist_map.get(img_id) or ''
		hist_tokens = {t.strip().lower() for t in hist_str.split() if t.strip()}

		if not explicit_restore and hist_tokens and _is_legitimate_history_tag(bad_tag, hist_tokens, ctx):
			continue

		poisoned_ids.append(img_id)
		if explicit_restore:
			for r_tok in explicit_restore.split():
				restore_pairs.append((img_id, r_tok))
				touched_tag_names.add(r_tok)
		elif hist_tokens:
			curated_hist = list(hist_tokens)
			apply_tag_curation(curated_hist, merged_map, context=ctx, single_tag_cache=single_cache)
			for c_tok in curated_hist:
				if c_tok != bad_tag and not c_tok.startswith(('source:', 'rating:')):
					restore_pairs.append((img_id, c_tok))
					touched_tag_names.add(c_tok)
		elif ':' in bad_tag:
			fallback_base = bad_tag.split(':', 1)[1]
			if fallback_base in ctx.known_general:
				restore_pairs.append((img_id, fallback_base))
				touched_tag_names.add(fallback_base)

	cur.execute('DROP TABLE tmp_rev_cand')

	if not poisoned_ids:
		print(f"[✓] All {len(affected_ids):,} images on '{bad_tag}' match tag_histories; nothing to revert.")
		return

	keep_count = len(affected_ids) - len(poisoned_ids)
	print(f"	-> Reverting '{bad_tag}' on {len(poisoned_ids):,} poisoned images (keeping {keep_count:,} legitimate)...")

	psycopg2.extras.execute_values(
		cur,
		'INSERT INTO tags (tag, count) VALUES %s ON CONFLICT (tag) DO NOTHING',
		[(t_name, 0) for t_name in touched_tag_names],
		page_size=5000,
	)
	cur.execute('SELECT tag, id FROM tags WHERE tag = ANY(%s)', (list(touched_tag_names),))
	local_tag_ids = dict(cur.fetchall())

	cur.execute('CREATE TEMP TABLE tmp_rev_fix (img_id INT PRIMARY KEY) ON COMMIT DROP')
	psycopg2.extras.execute_values(
		cur,
		'INSERT INTO tmp_rev_fix (img_id) VALUES %s',
		[(iid,) for iid in poisoned_ids],
		page_size=10000,
	)

	cur.execute(
		"""
		DELETE FROM image_tags it
		USING tmp_rev_fix trf
		WHERE it.image_id = trf.img_id AND it.tag_id = %s
		""",
		(bad_id,),
	)

	if restore_pairs:
		id_pairs = [
			(iid, local_tag_ids[t_name])
			for iid, t_name in restore_pairs
			if t_name in local_tag_ids
		]
		psycopg2.extras.execute_values(
			cur,
			'INSERT INTO image_tags (image_id, tag_id) VALUES %s ON CONFLICT DO NOTHING',
			id_pairs,
			page_size=10000,
		)

	for ns_prefix in ('artist', 'character', 'series'):
		tagme_tid = local_tag_ids[f'{ns_prefix}:tagme']
		cur.execute(
			"""
			INSERT INTO image_tags (image_id, tag_id)
			SELECT trf.img_id, %s
			FROM tmp_rev_fix trf
			WHERE NOT EXISTS (
				SELECT 1
				FROM image_tags it
				JOIN tags t ON it.tag_id = t.id
				WHERE it.image_id = trf.img_id AND t.tag LIKE %s
			)
			ON CONFLICT DO NOTHING
			""",
			(tagme_tid, f'{ns_prefix}:%'),
		)

	touched_ids = list(local_tag_ids.values())
	cur.execute(
		"""
		UPDATE tags t
		SET count = (SELECT COUNT(it.image_id) FROM image_tags it WHERE it.tag_id = t.id)
		WHERE t.id = ANY(%s)
		""",
		(touched_ids,),
	)
	cur.execute('DELETE FROM tags WHERE id = %s AND count <= 0', (bad_id,))
	cur.execute('DROP TABLE tmp_rev_fix')
	print(f"[✓] Successfully reverted '{bad_tag}' on {len(poisoned_ids):,} images!")


def _revert_poisoned_tags(
	db_config: dict,
	raw_revert_args: list[str],
	ctx: TagCurationContext,
	merged_map: dict[str, str],
) -> None:
	"""Reverts one or more poisoned tags in PostgreSQL."""
	specs = [
		sub.strip()
		for item in raw_revert_args
		for sub in item.split(',')
		if sub.strip()
	]
	with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
		for spec in specs:
			_revert_single_tag(cur, spec, ctx, merged_map)
		conn.commit()


def _try_rescue_target(s_tag: str, ctx: TagCurationContext) -> str | None:
	"""Attempts to rescue a _DROP_ or blank tag into a canonical/prefixed or punctuation-normalized target."""
	expanded = ctx.expand_single_tag(s_tag, {})
	if expanded and (len(expanded) > 1 or expanded[0] != s_tag):
		return ' '.join(expanded)

	norm_key = PUNCT_NORM_RE.sub('', s_tag.lower())
	if len(norm_key) >= 3 and norm_key in ctx.norm_general_map:
		canonical_match = ctx.norm_general_map[norm_key]
		if canonical_match != s_tag:
			return canonical_match
	return None


def _load_input_rows(in_path: Path) -> tuple[list[dict[str, str]], list[str]]:
	"""Loads raw mapping rows from either an SQLite .db file or a .csv file."""
	if in_path.suffix.lower() == '.db':
		init_tagmap_db(in_path)
		fieldnames = ['Sidecar_Tag', 'Canonical_Tag', 'Confidence', 'Sample_Size']
		with sqlite3.connect(in_path) as conn:
			rows = [
				{
					'Sidecar_Tag': s_tag,
					'Canonical_Tag': c_tag,
					'Confidence': str(conf),
					'Sample_Size': str(sample),
				}
				for s_tag, c_tag, conf, sample in conn.execute(
					'SELECT sidecar_tag, canonical_tag, confidence, sample_size FROM tagmap'
				)
			]
		return rows, fieldnames

	with in_path.open('r', encoding='utf-8', newline='') as f:
		reader = csv.DictReader(f)
		fieldnames = [fn for fn in (reader.fieldnames or ['Sidecar_Tag', 'Canonical_Tag']) if fn is not None]
		return list(reader), fieldnames


def _write_cleaned_output(
	in_path: Path,
	out_path: Path,
	fieldnames: list[str],
	cleaned_rows: list[dict[str, str]],
) -> None:
	"""Writes cleaned mapping rows to an SQLite .db table or a .csv file (creating a .bak if overwriting)."""
	if out_path.resolve() == in_path.resolve() and in_path.is_file():
		bak_path = in_path.with_suffix(f'{in_path.suffix}.bak')
		shutil.copy2(in_path, bak_path)
		print(f'[INFO] Backed up original file to {bak_path.resolve()}')

	if out_path.suffix.lower() == '.db':
		init_tagmap_db(out_path)
		with sqlite3.connect(out_path) as conn:
			conn.execute('DELETE FROM tagmap')
		entries = []
		for r in cleaned_rows:
			s_t = (r.get('Sidecar_Tag') or '').strip()
			c_t = (r.get('Canonical_Tag') or '').strip()
			if not s_t or not c_t:
				continue
			conf_raw = (r.get('Confidence') or '').strip()
			samp_raw = (r.get('Sample_Size') or r.get('Count') or '').strip()
			conf = float(conf_raw) if conf_raw and _is_number(conf_raw) else 1.0
			samp = int(float(samp_raw)) if samp_raw and _is_number(samp_raw) else 0
			entries.append((s_t, c_t, conf, samp))
		save_tagmap_entries(entries, out_path)
		return

	with out_path.open('w', encoding='utf-8', newline='') as f:
		writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
		writer.writeheader()
		writer.writerows(cleaned_rows)


def fix_curation_map(args) -> None:
	"""Main CLI entry point for fix-map."""
	raw_in = getattr(args, 'csv', None)
	in_path = Path(raw_in) if raw_in else TAGMAP_DB_PATH
	if not in_path.is_file():
		print(f'[ERROR] Map file not found: {in_path}')
		return

	out_path = Path(args.out) if getattr(args, 'out', None) else in_path
	log_path = out_path.with_suffix('.fix.log')

	mappings = load_mappings()
	ctx = get_curation_context(mappings)

	db_config = get_shimmie_db_credentials(args.spath) if getattr(args, 'spath', None) else None
	if db_config:
		with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
			cur.execute('SELECT tag, count FROM tags WHERE count > 0')
			db_rows = cur.fetchall()
			ctx.register_db_vocabulary(
				(r[0] for r in db_rows),
				tag_counts={r[0]: r[1] for r in db_rows},
			)

	raw_rows, fieldnames = _load_input_rows(in_path)

	cleaned_rows: list[dict[str, str]] = []
	purged_poison: list[tuple[str, str, str]] = []
	rescued_mappings: list[tuple[str, str, str]] = []
	repaired_comma_rows: list[tuple[str, str, str]] = []

	for row in raw_rows:
		extra_cols = [e.strip() for e in (row.pop(None, None) or []) if e and e.strip()]  # type: ignore[arg-type]
		s_tag = (row.get('Sidecar_Tag') or '').strip()
		c_tag = (row.get('Canonical_Tag') or '').strip()

		if not s_tag or s_tag == 'Sidecar_Tag':
			continue

		if '_DROP_' in extra_cols or (row.get('Count') or '').strip() == '_DROP_':
			reconstructed_sidecar = f'{s_tag},{c_tag}' if c_tag else s_tag
			repaired_comma_rows.append((s_tag, c_tag, f'{reconstructed_sidecar} -> _DROP_'))
			s_tag = reconstructed_sidecar
			c_tag = '_DROP_'
			row['Sidecar_Tag'] = s_tag
			row['Canonical_Tag'] = c_tag
			if 'Count' in row and (row.get('Count') or '').strip() == '_DROP_':
				nums = [e for e in extra_cols if e.isdigit()]
				row['Count'] = nums[0] if nums else '1'
		else:
			conf_candidates: list[float] = []
			if row.get('Confidence') and _is_number(row['Confidence'].strip()):
				conf_candidates.append(float(row['Confidence'].strip()))
			for e_val in extra_cols:
				if '.' in e_val and _is_number(e_val):
					conf_candidates.append(float(e_val))

			if conf_candidates and conf_candidates[0] < 0.60:
				purged_poison.append((s_tag, c_tag, f'Low confidence ({conf_candidates[0]:.4f} < 0.60)'))
				continue

			non_numeric_extra = [
				e for e in extra_cols
				if not _is_number(e) and e not in {'Confidence', 'Count', 'Canonical_Tag'}
			]
			if non_numeric_extra and c_tag not in {'_DROP_', ''}:
				merged_target = ' '.join([c_tag, *non_numeric_extra])
				repaired_comma_rows.append((s_tag, f"{c_tag},{','.join(non_numeric_extra)}", merged_target))
				c_tag = merged_target
				row['Canonical_Tag'] = c_tag

		if c_tag and c_tag != '_DROP_' and not ctx.is_safe_mapping(s_tag, c_tag):
			purged_poison.append((s_tag, c_tag, 'Poisoned general -> artist/character/series rule'))
			continue

		if c_tag in {'_DROP_', ''} and (rescued_target := _try_rescue_target(s_tag, ctx)):
			row['Canonical_Tag'] = rescued_target
			rescued_mappings.append((s_tag, c_tag or '<BLANK>', rescued_target))
			cleaned_rows.append(row)
			continue

		cleaned_rows.append(row)

	_write_cleaned_output(in_path, out_path, fieldnames, cleaned_rows)

	with log_path.open('w', encoding='utf-8') as f:
		f.write('=== CURATION MAP AUDIT & REPAIR LOG ===\n')
		f.write(f'Input Rows:               {len(raw_rows):,}\n')
		f.write(f'Output Rows:              {len(cleaned_rows):,}\n')
		f.write(f'Repaired Comma-Shift Rows:{len(repaired_comma_rows):,}\n')
		f.write(f'Purged Poisoned Rules:    {len(purged_poison):,}\n')
		f.write(f'Rescued to Real Mappings: {len(rescued_mappings):,}\n\n')

		f.write('=== 0. REPAIRED UNQUOTED COMMA / EXTRA COLUMN ROWS ===\n')
		for s_t, old_c, fixed in repaired_comma_rows:
			f.write(f"  - '{s_t}' | '{old_c}' -> '{fixed}'\n")

		f.write('\n=== 1. PURGED POISONED RULES ===\n')
		for s_t, c_t, reason in purged_poison:
			f.write(f'  - {s_t} -> {c_t} ({reason})\n')

		f.write('\n=== 2. RESCUED MAPPINGS (FROM _DROP_ / BLANK) ===\n')
		for s_t, old_c, new_c in rescued_mappings:
			f.write(f"  - {s_t}: '{old_c}' -> '{new_c}'\n")

	print(f'[✓] Fixed map written to {out_path.resolve()} (Log: {log_path.resolve()})')
	print(f'	-> Repaired {len(repaired_comma_rows):,} comma-shifted rows')
	print(f'	-> Purged {len(purged_poison):,} poisoned rules')
	print(f'	-> Rescued {len(rescued_mappings):,} tags into canonical/prefixed mappings')

	if revert_list := getattr(args, 'revert', None):
		if not db_config:
			print('[ERROR] --spath is required with --revert.')
		else:
			merged_map = load_tagmap(out_path)
			_revert_poisoned_tags(db_config, revert_list, ctx, merged_map)
