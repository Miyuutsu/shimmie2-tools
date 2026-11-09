# pylint: disable=line-too-long,too-many-locals,too-many-branches,too-many-statements,too-many-nested-blocks
"""Tools and classes for Mining tags to find equivalent mappings."""
import csv
import json
from pathlib import Path
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
        self.strict = {'character', 'artist', 'series', 'studio', 'meta'}
        self.known_general = self._load_known_general()

    @staticmethod
    def _load_known_general():
        """Loads canonical general tags from tag_groups.json so valid tags don't overwrite each other."""
        known = set()
        root_dir = Path(__file__).parent.parent
        candidates = [root_dir / "SD-Tag-Editor" / "tag_groups.json", root_dir / "tag_groups.json"]
        group_file = next((p for p in candidates if p.is_file()), None)
        if not group_file:
            return known

        with group_file.open("r", encoding="utf-8") as f:
            data = json.load(f)

        for subgroups in data.values():
            if not isinstance(subgroups, dict):
                continue
            for content in subgroups.values():
                if isinstance(content, list):
                    known.update(content)
                elif isinstance(content, dict):
                    for sub_list in content.values():
                        if isinstance(sub_list, list):
                            known.update(sub_list)
        return known

    def get_category(self, tag):
        """Determines the category of a given tag based on prefix or mappings."""
        if tag.startswith(('character:', 'artist:', 'series:', 'studio:', 'meta:')):
            return tag.split(':', 1)[0]
        if tag in self.chars:
            return 'character'
        if tag in self.artists:
            return 'artist'
        if tag in self.series:
            return 'series'
        return 'general'

    def shares_lexical_root(self, tag1, tag2):
        """Checks if two tags share a significant root word."""
        b1 = tag1.split(':', 1)[-1]
        b2 = tag2.split(':', 1)[-1]
        w1 = {w for w in re.findall(r'[a-z0-9]+', re.sub(r'\([^)]+\)', '', b1)) if len(w) > 2}
        w2 = {w for w in re.findall(r'[a-z0-9]+', re.sub(r'\([^)]+\)', '', b2)) if len(w) > 2}
        return bool(w1 & w2)

    def check(self, s_tag, c_tag, s_count):
        """Returns True if the category mapping is structurally permitted."""
        cat_s = self.get_category(s_tag)
        cat_c = self.get_category(c_tag)

        if s_tag in self.known_general and c_tag in self.known_general:
            return False

        if cat_s in self.strict and cat_c in self.strict and cat_s != cat_c:
            return False

        if cat_s in self.strict and cat_c == 'general':
            return False

        if cat_s == 'character' and cat_c == 'character' and s_count < 50:
            b1 = re.sub(r'_\([^)]+\)$', '', s_tag.split(':', 1)[-1])
            b2 = re.sub(r'_\([^)]+\)$', '', c_tag.split(':', 1)[-1])
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

def _fetch_global_context(tags_set, db_conn):
    """Fetches full DB counts in a single pass, entirely ignoring wiki deprecation."""
    g_counts = {}

    if not db_conn or not tags_set:
        # Return empty sets for wiki outputs to maintain tuple compatibility
        return g_counts, set(), set()

    print("\n[INFO] Fetching global DB stats...")

    # Pre-strip any prefixes in Python so we have a clean set of lookup targets
    target_set = set(tags_set) | {t.split(":", 1)[-1] for t in tags_set if ":" in t}

    try:
        with psycopg2.connect(**db_conn) as conn, conn.cursor() as cur:
            # Scan the `tags` table only ONCE instead of once per 1,000-tag chunk
            cur.execute("SELECT tag, count FROM tags WHERE count > 0;")
            for raw_tag, count in cur.fetchall():
                cnt = int(count)
                base_tag = raw_tag.split(":", 1)[-1] if ":" in raw_tag else raw_tag
                if raw_tag in target_set or base_tag in target_set:
                    g_counts[base_tag] = g_counts.get(base_tag, 0) + cnt
                    if raw_tag != base_tag:
                        g_counts[raw_tag] = g_counts.get(raw_tag, 0) + cnt
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"\n[WARNING] Global counts query failed: {e}")

    return g_counts, set(), set()

def _evaluate_match_guards(s_tag, best_match, s_count, inclusion, jaccard, db_cnt, guard, mode, freqs, thresholds):
    """Helper evaluating strict rules, segregated into Mining and Retro modes."""

    # Namespace Firewall
    cat_s = guard.get_category(s_tag)
    cat_c = guard.get_category(best_match)

    shares_root = guard.shares_lexical_root(s_tag, best_match)

    # 1. Strict Namespace Protection
    if cat_s in guard.strict and cat_c in guard.strict and cat_s != cat_c:
        return None
    # Never downgrade a prefixed tag into an unprefixed general tag
    if ':' in s_tag and ':' not in best_match and mode != "retro":
        return None
    # If crossing between general and a strict category, require identical base root
    if cat_s != cat_c:
        b_s = re.sub(r'_\([^)]+\)$', '', s_tag.split(':', 1)[-1])
        b_c = re.sub(r'_\([^)]+\)$', '', best_match.split(':', 1)[-1])
        if b_s != b_c:
            return None

    # Calculate an effective score: Subsets get to use their inclusion score, others rely purely on Jaccard
    effective_score = max(jaccard, inclusion) if shares_root else jaccard

    # 2. Lexical / Jaccard Threshold Guard
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

    # 5. Redundancy Drop Check (ONLY runs after passing Jaccard + Panic Limit!)
    if mode == "mining" and (freqs[4][s_tag][best_match] / s_count) >= thresholds[1]:
        if not guard.can_drop(s_tag, best_match):
            return None
        return "_DROP_"

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

            # Ensure global_c_count is never smaller than the local sample count (prevents >1.0 Jaccard when SQLite > Postgres)
            global_c_count = max(global_ctx[0].get(c_tag, 0), freqs[2].get(c_tag, 0))
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
                s_tag, best_match, s_count, best_inclusion, best_jaccard, adjusted_db_cnt, guard, mode, freqs, thresholds
            )

            if final_match:
                out_s = s_tag
                out_c = final_match
                # Use max() to ensure subsets like 'japanese_language' output high confidence (~0.95)
                confidence = max(best_jaccard, best_inclusion)

                if mode == "retro":
                    s_parts = s_tag.split(':', 1)
                    c_parts = final_match.split(':', 1)
                    if len(s_parts) == 2 and len(c_parts) == 1:
                        s_base_clean = re.sub(r'_\([^)]+\)$', '', s_parts[1])
                        if s_parts[1] == final_match or s_base_clean == final_match:
                            out_s = final_match
                            out_c = s_tag
                        else:
                            # Never downgrade a prefixed tag to a different unprefixed tag!
                            continue
                else:
                    if final_match == "_DROP_":
                        confidence = 1.0

                results.append({
                    "Sidecar_Tag": out_s,
                    "Canonical_Tag": out_c,
                    "Confidence": round(confidence, 4),
                    "Sample_Size": s_count
                })

    non_root_targets = Counter(
        r["Canonical_Tag"] for r in results
        if r["Canonical_Tag"] != "_DROP_"
        and not guard.shares_lexical_root(r["Sidecar_Tag"], r["Canonical_Tag"])
    )
    return [
        r for r in results
        if non_root_targets[r["Canonical_Tag"]] <= 2
        or guard.shares_lexical_root(r["Sidecar_Tag"], r["Canonical_Tag"])
    ]

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

    global_ctx = _fetch_global_context(set(freqs[1].keys()) | set(freqs[2].keys()), db_conn)
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
