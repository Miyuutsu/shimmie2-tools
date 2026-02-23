"""Tools and classes for Mining tags to find equivalent mappings."""
import os
import re
import csv
import subprocess
from collections import defaultdict, Counter
import tqdm

from functions.common import compute_md5
from functions.tags_curation import get_sidecar_tags
from functions.db_cache import get_bulk_canonical_tags

class TagCategoryGuard:
    """Helper to enforce category rules during tag mining."""
    def __init__(self, mappings):
        self.artists = set(mappings.artist.keys()) if mappings else set()
        self.chars = set(mappings.char.keys()) if mappings else set()
        self.series = set()
        if mappings:
            for val in mappings.char.values():
                if isinstance(val, (list, tuple, set)):
                    self.series.update(val)
                else:
                    self.series.add(val)
        self.strict = {'character', 'artist', 'series'}

    def get_category(self, tag):
        """Determines the category of a given tag based on mappings."""
        if tag in self.chars:
            return 'character'
        if tag in self.artists:
            return 'artist'
        if tag in self.series:
            return 'series'
        return 'general'

    def shares_lexical_root(self, tag1, tag2):
        """Checks if two tags share a significant root word."""
        w1 = {w for w in re.findall(r'[a-z0-9]+', re.sub(r'\([^)]+\)', '', tag1)) if len(w) > 2}
        w2 = {w for w in re.findall(r'[a-z0-9]+', re.sub(r'\([^)]+\)', '', tag2)) if len(w) > 2}
        return bool(w1 & w2)

    def check(self, s_tag, c_tag, s_count):
        """Returns True if the category mapping is structurally permitted."""
        cat_s = self.get_category(s_tag)
        cat_c = self.get_category(c_tag)

        if cat_s in self.strict and cat_c in self.strict and cat_s != cat_c:
            return False

        if cat_s in self.strict and cat_c == 'general':
            return False

        if cat_s == 'character' and cat_c == 'character' and s_count < 50:
            b1 = re.sub(r'_\([^)]+\)$', '', s_tag)
            b2 = re.sub(r'_\([^)]+\)$', '', c_tag)
            if b1 != b2 and b1 not in b2 and b2 not in b1:
                return False

        return True

    def can_drop(self, s_tag, c_tag):
        """Returns True if it's safe to drop a tag for redundancy against another."""
        cat_s = self.get_category(s_tag)
        cat_c = self.get_category(c_tag)

        if cat_s in self.strict and cat_c in self.strict and cat_s != cat_c:
            return False

        if cat_s in self.strict and cat_c == 'general':
            return False

        return True

def _extract_hashes(image_list):
    """Helper to extract MD5s rapidly using regex and fallback."""
    md5_regex = re.compile(r"[a-fA-F0-9]{32}")
    img_to_md5 = {}
    md5_set = set()

    for img_path in tqdm.tqdm(image_list, desc="1/2: Extracting Hashes", unit="img"):
        match = md5_regex.search(img_path.stem)
        md5 = match.group(0).lower() if match else compute_md5(img_path)
        img_to_md5[img_path] = md5
        md5_set.add(md5)

    return img_to_md5, md5_set

def _calculate_co_occurrences(image_list, img_to_md5, bulk_tags):
    """Calculates overlaps between sidecars and canonical db tags."""
    sidecar_counts = Counter()
    canonical_counts = Counter()
    co_occurrences = defaultdict(Counter)
    sidecar_overlap = defaultdict(Counter)
    valid_pairs = 0

    for img_path in tqdm.tqdm(image_list, desc="2/2: Mapping Co-occurrences", unit="img"):
        canonical = bulk_tags.get(img_to_md5[img_path])
        if not canonical:
            continue

        valid_pairs += 1
        sidecars = set(get_sidecar_tags(img_path))

        for s_tag in sidecars:
            sidecar_counts[s_tag] += 1
            for c_tag in canonical:
                co_occurrences[s_tag][c_tag] += 1
                if c_tag in sidecars:
                    sidecar_overlap[s_tag][c_tag] += 1

        for c_tag in canonical:
            canonical_counts[c_tag] += 1

    return valid_pairs, sidecar_counts, canonical_counts, co_occurrences, sidecar_overlap

def build_tag_frequencies(image_list, db_conn, sqlite_conn):
    """Orchestrates building frequency tables for tags."""
    img_to_md5, md5_set = _extract_hashes(image_list)
    print(f"\n[INFO] Fetching database tags for {len(md5_set)} unique files...")
    bulk_tags = get_bulk_canonical_tags(md5_set, db_conn, sqlite_conn)
    return _calculate_co_occurrences(image_list, img_to_md5, bulk_tags)

def _process_context_chunk(escaped, db_conn, env, dep_sql):
    """Helper to process a chunk of tags and return context sets/dicts."""
    esc_str = "', '".join(escaped)
    chunk_counts = {}
    chunk_deprecated = set()
    chunk_wiki = set()

    try:
        cmd = [
            "psql", "-d", db_conn['dbname'], "-U", db_conn['user'],
            "-h", db_conn['host'], "-t", "-A", "-c",
            f"SELECT tag, count FROM tags WHERE tag IN ('{esc_str}');"
        ]
        res = subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)
        for line in res.stdout.strip().split('\n'):
            if '|' in line:
                parts = line.rsplit('|', 1)
                chunk_counts[parts[0]] = int(parts[1])
    except Exception as e: # pylint: disable=broad-exception-caught
        print(f"\n[WARNING] Global counts query failed: {e}")

    try:
        cmd_wiki = [
            "psql", "-d", db_conn['dbname'], "-U", db_conn['user'],
            "-h", db_conn['host'], "-t", "-A", "-c",
            "SELECT REPLACE(LOWER(title), ' ', '_'), "
            f"CASE WHEN {dep_sql} THEN 1 ELSE 0 END "
            f"FROM wiki_pages WHERE REPLACE(LOWER(title), ' ', '_') IN ('{esc_str}');"
        ]
        res = subprocess.run(cmd_wiki, env=env, capture_output=True, text=True, check=True)
        for line in res.stdout.strip().split('\n'):
            if '|' in line:
                parts = line.rsplit('|', 1)
                chunk_wiki.add(parts[0])
                if parts[1] == '1':
                    chunk_deprecated.add(parts[0])
    except Exception as e: # pylint: disable=broad-exception-caught
        print(f"\n[WARNING] Wiki check query failed: {e}")

    return chunk_counts, chunk_deprecated, chunk_wiki


def _fetch_global_context(tags_set, db_conn, chunk_size=1000):
    """Fetches full DB counts and wiki deprecation status."""
    g_counts = {}
    deprecated = set()
    has_wiki = set()

    if not db_conn:
        return g_counts, deprecated, has_wiki

    env = os.environ.copy()
    if db_conn.get('password'):
        env['PGPASSWORD'] = db_conn['password']

    tags_set = list(tags_set)
    print("\n[INFO] Fetching global DB stats and wiki context...")

    dep_sql = (
        r"REGEXP_REPLACE(body, ',\s*(do not use|ambiguous)', 'SAFE', 'ig') ~* '("
        r"deprecated tag\.|ambiguous tag\. do not use\.|ambiguous\. do not use\.|"
        r"do not use\. use|do not use this tag\. instead|do not use this tag\. use|"
        r"\. do not use this tag\.</p>|; do not use this tag\.</p>|"
        r"<p>do not use this tag\.</p>|\ndo not use this tag\.</p>|"
        r"^do not use this tag\.</p>)'"
    )

    for i in tqdm.tqdm(range(0, len(tags_set), chunk_size), leave=False):
        escaped = [t.replace("'", "''") for t in tags_set[i:i+chunk_size]]
        if escaped:
            c_counts, c_dep, c_wiki = _process_context_chunk(escaped, db_conn, env, dep_sql)
            g_counts.update(c_counts)
            deprecated.update(c_dep)
            has_wiki.update(c_wiki)

    return g_counts, deprecated, has_wiki

# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-return-statements
def _evaluate_match_guards(
    s_tag, best_match, s_count, hi_score, freqs, global_ctx, db_cnt, guard, thresholds
):
    """Helper evaluating strict rules before tag matches are confirmed."""
    if re.sub(r'_\([^)]+\)', '', s_tag) == re.sub(r'_\([^)]+\)', '', best_match):
        return None

    if f"({best_match})" in s_tag:
        return None

    if hi_score < 0.75 and not guard.shares_lexical_root(s_tag, best_match):
        return None

    if (freqs[4][s_tag][best_match] / s_count) >= thresholds[1]:
        if not guard.can_drop(s_tag, best_match):
            return None
        if db_cnt > (s_count * 0.5) or db_cnt > 500:
            return None
        if s_tag in global_ctx[2]:
            return None
        return "_DROP_"

    if not guard.check(s_tag, best_match, s_count):
        return None

    if (db_cnt > 500 or db_cnt > (s_count * 0.5)) and hi_score < 0.95:
        return None

    return best_match

def calculate_equivalencies(freqs, global_ctx, guard, thresholds):
    """Calculates Jaccard similarity scores to map tags safely under local limits."""
    results = []
    for s_tag, s_count in freqs[1].items():
        if s_count < thresholds[0]:
            continue

        if s_tag in global_ctx[1]:
            results.append({
                "Sidecar_Tag": s_tag,
                "Canonical_Tag": "_DROP_",
                "Confidence": 1.0,
                "Sample_Size": s_count
            })
            continue

        best_match = None
        hi_score = 0

        for c_tag, shared in freqs[3][s_tag].items():
            union = s_count + freqs[2][c_tag] - shared
            if union > 0 and (shared / union) > hi_score:
                hi_score = shared / union
                best_match = c_tag

        if hi_score >= thresholds[1] and best_match != s_tag:
            db_cnt = global_ctx[0].get(s_tag, freqs[2].get(s_tag, 0))

            final_match = _evaluate_match_guards(
                s_tag, best_match, s_count, hi_score, freqs,
                global_ctx, db_cnt, guard, thresholds
            )

            if final_match:
                results.append({
                    "Sidecar_Tag": s_tag,
                    "Canonical_Tag": final_match,
                    "Confidence": round(hi_score, 4),
                    "Sample_Size": s_count
                })

    return results

def mine_tag_equivalencies(image_list, conns, output_path, mappings, thresholds=(10, 0.5)):
    """Scans images to discover 1:1 tag mappings using Jaccard similarity."""
    print(f"\n[⛏️ MINING MODE] Analyzing {len(image_list)} images for 1:1 equivalencies...")
    db_conn, sqlite_conn = conns
    freqs = build_tag_frequencies(image_list, db_conn, sqlite_conn)

    missing = len(image_list) - freqs[0]
    if len(image_list) > 0 and (missing / len(image_list)) >= 0.5:
        print(f"\n[⚠️ ALERT] High Missing Rate: {missing}/{len(image_list)} images "
              f"({(missing/len(image_list))*100:.1f}%) were not found in the DB!")
    else:
        print(f"Successfully aligned {freqs[0]} images with database records.")

    global_ctx = _fetch_global_context(freqs[1].keys(), db_conn)
    guard = TagCategoryGuard(mappings)
    calculated = calculate_equivalencies(freqs, global_ctx, guard, thresholds)
    calculated.sort(key=lambda x: x["Sample_Size"], reverse=True)

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["Sidecar_Tag", "Canonical_Tag", "Confidence", "Sample_Size"]
        )
        writer.writeheader()
        writer.writerows(calculated)

    print(f"[✓] Mined {len(calculated)} highly confident equivalencies! Saved to {output_path}")
