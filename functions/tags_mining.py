"""Tools and classes for mining tags to discover equivalent canonical mappings."""

from collections import Counter, defaultdict
from collections.abc import Iterable
import csv
from dataclasses import dataclass, field
from pathlib import Path
import re
import sqlite3

import psycopg2
import tqdm

from functions.common import extract_or_compute_md5
from functions.db_cache import Mappings, get_bulk_canonical_tags, save_tagmap_entries
from functions.tags_curation import get_sidecar_tags, load_tag_groups_metadata

E621_NUMERIC_TAG_RE = re.compile(r'^\d+_\(e621\)$')
TRAILING_PAREN_RE = re.compile(r'_\([^)]+\)$')
WORD_TOKEN_RE = re.compile(r'[a-z0-9]+')


@dataclass(slots=True)
class TagFrequencies:
	"""Holds co-occurrence and frequency counters for tag equivalency mining."""

	valid_pairs: int = 0
	sidecar_counts: Counter[str] = field(default_factory=Counter)
	canonical_counts: Counter[str] = field(default_factory=Counter)
	co_occurrences: dict[str, Counter[str]] = field(default_factory=lambda: defaultdict(Counter))
	sidecar_overlap: dict[str, Counter[str]] = field(default_factory=lambda: defaultdict(Counter))


class TagCategoryGuard:
	"""Enforces namespace and category safety rules during tag mining."""

	def __init__(self, mappings: Mappings | None):
		self.artists = set(mappings.artist) if mappings else set()
		self.chars = set(mappings.char) if mappings else set()
		self.series: set[str] = set()
		if mappings:
			for val in mappings.char.values():
				if isinstance(val, (list, tuple, set)):
					self.series.update(val)
				else:
					for item in str(val).split(','):
						if clean := item.strip():
							self.series.add(clean)

		self.strict = {'character', 'artist', 'series', 'studio', 'meta'}
		_, self.known_general, copyright_series, _ = load_tag_groups_metadata()
		self.series.update(copyright_series)

		self.norm_general_map = {
			re.sub(r'[_()-]', '', t.lower()): t
			for t in self.known_general
			if re.fullmatch(r'[a-z0-9_()-]{3,}', t.lower())
			and len(re.sub(r'[_()-]', '', t.lower())) >= 3
		}

	def get_category(self, tag: str) -> str:
		"""Determines the category of a given tag based on prefix or loaded DB mappings."""
		if tag.startswith(('character:', 'artist:', 'series:', 'studio:', 'meta:')):
			return tag.split(':', 1)[0]
		if tag in self.chars:
			return 'character'
		if tag in self.artists:
			return 'artist'
		clean_series = TRAILING_PAREN_RE.sub('', tag)
		if tag in self.series or clean_series in self.series:
			return 'series'
		return 'general'

	def shares_lexical_root(self, tag1: str, tag2: str) -> bool:
		"""Returns True if two tags share at least one 3+ character alphanumeric word outside parentheses."""
		b1 = tag1.split(':', 1)[-1]
		b2 = tag2.split(':', 1)[-1]
		w1 = {w for w in WORD_TOKEN_RE.findall( re.sub(r'\([^)]+\)', '', b1)) if len(w) > 2}
		w2 = {w for w in WORD_TOKEN_RE.findall( re.sub(r'\([^)]+\)', '', b2)) if len(w) > 2}
		return bool(w1 & w2)

	def is_normalized_alias(self, tag1: str, tag2: str) -> bool:
		"""Returns True ONLY if tags share identical core word stems or differ only by a structural token."""
		b1 = TRAILING_PAREN_RE.sub('', tag1.split(':', 1)[-1].lower())
		b2 = TRAILING_PAREN_RE.sub('', tag2.split(':', 1)[-1].lower())

		def _stem(word: str) -> str:
			if len(word) > 5 and word.endswith('ing'):
				return word[:-3]
			if len(word) > 3 and word.endswith('s'):
				return word[:-1]
			return word

		t1 = [_stem(w) for w in WORD_TOKEN_RE.findall(b1)]
		t2 = [_stem(w) for w in WORD_TOKEN_RE.findall(b2)]

		if not t1 or not t2:
			return False
		if set(t1) == set(t2) or ''.join(t1) == ''.join(t2):
			return True

		structural_tokens = {'language', 'text', 'artwork', 'media', 'version', 'style', 'theme'}
		diff = set(t1) ^ set(t2)
		return bool(diff and diff.issubset(structural_tokens) and (set(t1) & set(t2)))

	def check(self, s_tag: str, c_tag: str, s_count: int) -> bool:
		"""Returns True if the category mapping is structurally permitted."""
		if s_tag in self.known_general and c_tag in self.known_general:
			return False

		cat_s = self.get_category(s_tag)
		cat_c = self.get_category(c_tag)

		if cat_s in self.strict and (cat_c == 'general' or (cat_c in self.strict and cat_s != cat_c)):
			return False

		if cat_s == 'character' and cat_c == 'character' and s_count < 50:
			b1 = TRAILING_PAREN_RE.sub('', s_tag.split(':', 1)[-1])
			b2 = TRAILING_PAREN_RE.sub('', c_tag.split(':', 1)[-1])
			if b1 != b2 and b1 not in b2 and b2 not in b1:
				return False

		return True

	def can_drop(self, s_tag: str, c_tag: str) -> bool:
		"""Returns True if it is safe to drop s_tag for redundancy against c_tag."""
		cat_s = self.get_category(s_tag)
		cat_c = self.get_category(c_tag)

		if cat_s in self.strict and cat_c in self.strict and cat_s != cat_c:
			return False
		return not (cat_s in self.strict and cat_c == 'general')


def _extract_hashes(image_list: Iterable[Path]) -> tuple[dict[Path, str], set[str]]:
	"""Extracts MD5s for all media files using filename regex or file hashing fallback."""
	img_to_md5: dict[Path, str] = {}
	md5_set: set[str] = set()

	for img_path in tqdm.tqdm(image_list, desc='1/2: Extracting Hashes', unit='img'):
		md5 = extract_or_compute_md5(img_path)
		img_to_md5[img_path] = md5
		md5_set.add(md5)

	return img_to_md5, md5_set


def _calculate_co_occurrences(
	image_list: Iterable[Path],
	img_to_md5: dict[Path, str],
	bulk_tags: dict[str, set[str]],
) -> TagFrequencies:
	"""Calculates overlaps between sidecar tags and canonical database tags."""
	freqs = TagFrequencies()

	for img_path in tqdm.tqdm(image_list, desc='2/2: Mapping Co-occurrences', unit='img'):
		canonical_raw = bulk_tags.get(img_to_md5[img_path])
		if not canonical_raw:
			continue

		canonical = {t for t in canonical_raw if not t.startswith('tagai:')}
		if not canonical:
			continue

		freqs.valid_pairs += 1
		sidecars = {t for t in get_sidecar_tags(img_path) if not t.startswith('tagai:')}

		for s_tag in sidecars:
			freqs.sidecar_counts[s_tag] += 1
			for c_tag in canonical:
				freqs.co_occurrences[s_tag][c_tag] += 1
				if c_tag in sidecars:
					freqs.sidecar_overlap[s_tag][c_tag] += 1

		for c_tag in canonical:
			freqs.canonical_counts[c_tag] += 1

	return freqs


def build_tag_frequencies(
	image_list: list[Path],
	db_conn: dict | None,
	sqlite_conn: sqlite3.Connection | None,
) -> TagFrequencies:
	"""Builds tag frequency and co-occurrence tables across a media list."""
	img_to_md5, md5_set = _extract_hashes(image_list)
	print(f'\n[INFO] Fetching database tags for {len(md5_set):,} unique files...')
	bulk_tags = get_bulk_canonical_tags(md5_set, db_conn, sqlite_conn)
	return _calculate_co_occurrences(image_list, img_to_md5, bulk_tags)


def fetch_global_context(tags_set: set[str], db_conn: dict | None) -> dict[str, int]:
	"""Fetches full PostgreSQL tag usage counts in a single pass."""
	g_counts: dict[str, int] = {}
	if not db_conn or not tags_set:
		return g_counts

	print('\n[INFO] Fetching global DB stats...')
	target_set = set(tags_set) | {t.split(':', 1)[-1] for t in tags_set if ':' in t}

	try:
		with psycopg2.connect(**db_conn) as conn, conn.cursor() as cur:
			cur.execute('SELECT tag, count FROM tags WHERE count > 0;')
			for raw_tag, count in cur.fetchall():
				cnt = int(count)
				base_tag = raw_tag.split(':', 1)[-1] if ':' in raw_tag else raw_tag
				if raw_tag in target_set or base_tag in target_set:
					g_counts[base_tag] = g_counts.get(base_tag, 0) + cnt
					if raw_tag != base_tag:
						g_counts[raw_tag] = g_counts.get(raw_tag, 0) + cnt
	except Exception as e:
		print(f'\n[WARNING] Global counts query failed: {e}')

	return g_counts


def _evaluate_match_guards(
	s_tag: str,
	best_match: str,
	s_count: int,
	inclusion: float,
	jaccard: float,
	db_cnt: int,
	guard: TagCategoryGuard,
	mode: str,
	freqs: TagFrequencies,
	thresholds: tuple[int, float],
) -> str | None:
	"""Evaluates namespace, lexical, sample-size, and panic-limit guards for a candidate mapping."""
	if E621_NUMERIC_TAG_RE.match(s_tag):
		return None

	cat_s = guard.get_category(s_tag)
	cat_c = guard.get_category(best_match)

	if (
		(cat_s in guard.strict and cat_c in guard.strict and cat_s != cat_c)
		or (':' in s_tag and ':' not in best_match and mode != 'retro')
	):
		return None

	if cat_s != cat_c:
		b_s = TRAILING_PAREN_RE.sub('', s_tag.split(':', 1)[-1])
		b_c = TRAILING_PAREN_RE.sub('', best_match.split(':', 1)[-1])
		if b_s != b_c:
			return None

	shares_root = guard.shares_lexical_root(s_tag, best_match)
	is_alias = guard.is_normalized_alias(s_tag, best_match)

	if is_alias:
		effective_score = max(jaccard, inclusion)
		jaccard_threshold = 0.60
	elif shares_root:
		if s_count < 15:
			return None
		effective_score = jaccard
		jaccard_threshold = 0.85
	else:
		if cat_s == 'general' and cat_c == 'general' and s_count < 15:
			return None
		effective_score = jaccard
		jaccard_threshold = 0.80 if (cat_s == 'general' and cat_c == 'general') else 0.75

	if effective_score < jaccard_threshold or not guard.check(s_tag, best_match, s_count):
		return None

	is_proven_subset = is_alias and inclusion >= 0.80
	if (db_cnt > 500 or db_cnt > (s_count * 0.5)) and not is_proven_subset and jaccard < 0.95:
		return None

	if mode == 'mining' and (freqs.sidecar_overlap[s_tag][best_match] / s_count) >= thresholds[1]:
		return '_DROP_' if guard.can_drop(s_tag, best_match) else None

	return best_match


def calculate_equivalencies(
	freqs: TagFrequencies,
	global_counts: dict[str, int],
	guard: TagCategoryGuard,
	thresholds: tuple[int, float],
	mode: str = 'mining',
) -> list[dict[str, str | float | int]]:
	"""Calculates Jaccard similarity scores and returns verified tag mappings."""
	results: list[dict[str, str | float | int]] = []

	for s_tag, s_count in freqs.sidecar_counts.items():
		if s_tag in guard.known_general or s_count < thresholds[0]:
			continue

		best_match: str | None = None
		best_jaccard = 0.0
		best_inclusion = 0.0

		for c_tag, shared in freqs.co_occurrences[s_tag].items():
			if c_tag == s_tag:
				continue

			c_count = (
				freqs.canonical_counts.get(c_tag, 0)
				if mode == 'retro'
				else max(global_counts.get(c_tag, 0), freqs.canonical_counts.get(c_tag, 0))
			)

			union = s_count + c_count - shared
			jaccard = shared / union if union > 0 else 0.0

			if jaccard > best_jaccard:
				best_jaccard = jaccard
				best_match = c_tag
				best_inclusion = shared / s_count

		if not best_match:
			continue

		adjusted_db_cnt = (
			freqs.canonical_counts.get(s_tag, 0)
			if mode == 'retro'
			else global_counts.get(s_tag, freqs.canonical_counts.get(s_tag, 0))
		)

		final_match = _evaluate_match_guards(
			s_tag, best_match, s_count, best_inclusion, best_jaccard,
			adjusted_db_cnt, guard, mode, freqs, thresholds,
		)
		if not final_match:
			continue

		out_s = s_tag
		out_c = final_match
		confidence = (
			max(best_jaccard, best_inclusion)
			if guard.is_normalized_alias(s_tag, final_match)
			else best_jaccard
		)

		if mode == 'retro':
			s_parts = s_tag.split(':', 1)
			c_parts = final_match.split(':', 1)
			if len(s_parts) == 2 and len(c_parts) == 1:
				s_base_clean = TRAILING_PAREN_RE.sub('', s_parts[1])
				if s_parts[1] == final_match or s_base_clean == final_match:
					out_s = final_match
					out_c = s_tag
				else:
					continue
		elif final_match == '_DROP_':
			confidence = 1.0

		results.append({
			'Sidecar_Tag': out_s,
			'Canonical_Tag': out_c,
			'Confidence': round(confidence, 4),
			'Sample_Size': s_count,
		})

	non_root_targets = Counter(
		str(r['Canonical_Tag'])
		for r in results
		if r['Canonical_Tag'] != '_DROP_'
		and not guard.shares_lexical_root(str(r['Sidecar_Tag']), str(r['Canonical_Tag']))
	)
	return [
		r for r in results
		if non_root_targets[str(r['Canonical_Tag'])] <= 2
		or guard.shares_lexical_root(str(r['Sidecar_Tag']), str(r['Canonical_Tag']))
	]


def mine_tag_equivalencies(
	image_list: list[Path],
	conns: tuple[dict | None, sqlite3.Connection | None],
	output_path: str | Path,
	mappings: Mappings,
	thresholds: tuple[int, float] = (10, 0.5),
) -> None:
	"""Scans images to discover 1:1 tag mappings using Jaccard similarity."""
	print(f'\n[⛏️ MINING MODE] Analyzing {len(image_list):,} images for 1:1 equivalencies...')
	db_conn, sqlite_conn = conns
	freqs = build_tag_frequencies(image_list, db_conn, sqlite_conn)

	missing = len(image_list) - freqs.valid_pairs
	if image_list and (missing / len(image_list)) >= 0.5:
		pct = (missing / len(image_list)) * 100
		print(f'\n[⚠️ ALERT] High Missing Rate: {missing:,}/{len(image_list):,} images ({pct:.1f}%) were not found in the DB!')
	else:
		print(f'Successfully aligned {freqs.valid_pairs:,} images with database records.')

	all_tags = set(freqs.sidecar_counts) | set(freqs.canonical_counts)
	global_counts = fetch_global_context(all_tags, db_conn)
	guard = TagCategoryGuard(mappings)

	calculated = calculate_equivalencies(freqs, global_counts, guard, thresholds, mode='mining')
	calculated.sort(key=lambda x: int(x['Sample_Size']), reverse=True)

	out_file = Path(output_path)
	if out_file.suffix.lower() == '.db':
		saved = save_tagmap_entries(calculated, out_file)
		print(f'[✓] Mined and upserted {saved:,} equivalencies into {out_file.resolve()}')
	else:
		with out_file.open('w', encoding='utf-8', newline='') as f:
			writer = csv.DictWriter(f, fieldnames=['Sidecar_Tag', 'Canonical_Tag', 'Confidence', 'Sample_Size'])
			writer.writeheader()
			writer.writerows(calculated)
		print(f'[✓] Mined {len(calculated):,} highly confident equivalencies! Saved to {out_file.resolve()}')
