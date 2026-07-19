# pylint: disable=line-too-long,too-many-locals,too-many-branches,too-many-statements,too-many-nested-blocks
"""Tools and classes for Mining tags to find equivalent mappings."""
import csv
import os
import psycopg2
import re
import tqdm
from collections import defaultdict, Counter

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

        return not (cat_s in self.strict and cat_c == 'general')

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
        canonical_raw = bulk_tags.get(img_to_md5[img_path])
        if not canonical_raw:
            continue

        # SHIELD 1: Strip AI tags from the Database canonical list
        canonical = {t for t in canonical_raw if not t.startswith('tagai:')}
        if not canonical:
            continue

        valid_pairs += 1

        # SHIELD 2: Strip AI tags from the Sidecar list
        sidecars = {t for t in get_sidecar_tags(img_path) if not t.startswith('tagai:')}

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

def _process_context_chunk(tags_chunk, db_conn):
    """Helper to process a chunk of tags and return context dicts."""
    chunk_counts = {}

    try:
        with psycopg2.connect(**db_conn) as conn, conn.cursor() as cur:
            # Safely pass the tags_chunk list directly into the execute parameters
            cur.execute("""
                SELECT
                    CASE WHEN position(':' in tag) > 0
                            THEN substring(tag from position(':' in tag) + 1)
                            ELSE tag END,
                    SUM(count)
                FROM tags
                WHERE tag = ANY(%s) OR substring(tag from position(':' in tag) + 1) = ANY(%s)
                GROUP BY 1;
            """, (tags_chunk, tags_chunk))

            for tag_name, total_count in cur.fetchall():
                chunk_counts[tag_name] = int(total_count)
    except Exception as e:
        print(f"\n[WARNING] Global counts query failed: {e}")

    return chunk_counts


def _fetch_global_context(tags_set, db_conn, chunk_size=1000):
    """Fetches full DB counts, entirely ignoring wiki deprecation."""
    g_counts = {}

    if not db_conn:
        # Return empty sets for wiki outputs to maintain tuple compatibility
        return g_counts, set(), set()

    env = os.environ.copy()
    if db_conn.get('password'):
        env['PGPASSWORD'] = db_conn['password']

    tags_set = list(tags_set)
    print("\n[INFO] Fetching global DB stats...")

    for i in tqdm.tqdm(range(0, len(tags_set), chunk_size), leave=False):
        chunk = tags_set[i:i+chunk_size]
        if chunk:
            c_counts = _process_context_chunk(chunk, db_conn)
            g_counts.update(c_counts)

    return g_counts, set(), set()

# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-return-statements
def _evaluate_match_guards(s_tag, best_match, s_count, inclusion, jaccard, global_ctx, db_cnt, guard, mode, freqs, thresholds):
    """Helper evaluating strict rules, segregated into Mining and Retro modes."""

    # Namespace Firewall
    cat_s = guard.get_category(s_tag)
    cat_c = guard.get_category(best_match)
    if s_tag.startswith(('character:', 'artist:', 'series:', 'studio:', 'meta:')):
        cat_s = s_tag.split(':', 1)[0]
    if best_match.startswith(('character:', 'artist:', 'series:', 'studio:', 'meta:')):
        cat_c = best_match.split(':', 1)[0]

    shares_root = guard.shares_lexical_root(s_tag, best_match)

    # 1. Strict Namespace Protection
    if cat_s != cat_c and not shares_root:
        return None

    # Calculate an effective score: Subsets get to use their inclusion score, others rely purely on Jaccard
    effective_score = max(jaccard, inclusion) if shares_root else jaccard

    # 2. Mode-Specific Lexical & Redundancy Guards
    if mode == "mining":
        if (freqs[4][s_tag][best_match] / s_count) >= thresholds[1]:
            if not guard.can_drop(s_tag, best_match):
                return None
            return "_DROP_"

        jaccard_threshold = 0.60 if shares_root else 0.75
        if effective_score < jaccard_threshold:
            return None
    else:
        jaccard_threshold = 0.60 if shares_root else 0.75
        if effective_score < jaccard_threshold:
            return None

    # 3. Structural Guard
    if not guard.check(s_tag, best_match, s_count):
        return None

    # 4. The Absolute Panic Limit (Revised)
    # We relax the strict Jaccard requirement if it is a proven alias/subset
    is_proven_subset = shares_root and inclusion >= 0.85

    if (db_cnt > 500 or db_cnt > (s_count * 0.5)) and not is_proven_subset and jaccard < 0.95:
        return None

    return best_match

def calculate_equivalencies(freqs, global_ctx, guard, thresholds, mode="mining"):
    """Calculates pure Jaccard similarity scores to map tags accurately."""
    results = []
    for s_tag, s_count in freqs[1].items():
        if s_count < thresholds[0]:
            continue

        best_match = None
        best_jaccard = 0.0
        best_inclusion = 0.0

        for c_tag, shared in freqs[3][s_tag].items():
            if c_tag == s_tag:
                continue

            global_c_count = global_ctx[0].get(c_tag, freqs[2].get(c_tag, 0))
            union = s_count + global_c_count - shared
            jaccard = shared / union if union > 0 else 0

            if jaccard > best_jaccard:
                best_jaccard = jaccard
                best_match = c_tag
                best_inclusion = shared / s_count

        if best_match:
            raw_db_cnt = global_ctx[0].get(s_tag, freqs[2].get(s_tag, 0))

            # Database counts must only be artificially adjusted during retro-curation
            adjusted_db_cnt = max(0, raw_db_cnt - s_count) if mode == "retro" else raw_db_cnt

            final_match = _evaluate_match_guards(
                s_tag, best_match, s_count, best_inclusion, best_jaccard, global_ctx, adjusted_db_cnt, guard, mode, freqs, thresholds
            )

            if final_match:
                out_s = s_tag
                out_c = final_match
                # Use max() to ensure subsets like 'japanese_language' output high confidence (~0.95)
                confidence = max(best_jaccard, best_inclusion)

                if mode == "retro":
                    # Namespace inversion to upgrade DB tags
                    s_parts = s_tag.split(':', 1)
                    c_parts = final_match.split(':', 1)
                    if len(s_parts) == 2 and len(c_parts) == 1 and s_parts[1] == final_match:
                        out_s = final_match
                        out_c = s_tag
                else:
                    if final_match == "_DROP_":
                        confidence = 1.0

                results.append({
                    "Sidecar_Tag": out_s,
                    "Canonical_Tag": out_c,
                    "Confidence": round(confidence, 4),
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

    # FIX: Explicitly pass mode="mining" to trigger the standard import defenses
    calculated = calculate_equivalencies(freqs, global_ctx, guard, thresholds, mode="mining")
    calculated.sort(key=lambda x: x["Sample_Size"], reverse=True)

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["Sidecar_Tag", "Canonical_Tag", "Confidence", "Sample_Size"]
        )
        writer.writeheader()
        writer.writerows(calculated)

    print(f"[✓] Mined {len(calculated)} highly confident equivalencies! Saved to {output_path}")
