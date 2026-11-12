# pylint: disable=too-many-locals,too-many-branches,too-many-statements,line-too-long
"""Functions for parsing, sorting, formatting, enriching, and expanding tags."""
from collections import defaultdict
import csv
import html
import json
from pathlib import Path
import re
import sqlite3

from functions.source_resolver import (
    is_recognized_prefix,
    resolve_kiseki_series,
    resolve_prefixed_tag,
)

ROOT_DIR = Path(__file__).parent.parent.resolve()
DB_DIR = ROOT_DIR / "database"
CDB_PATH = DB_DIR / "characters.db"
ADB_PATH = DB_DIR / "artists.db"

MASTER_MERGE_LIST = {
    "character:samurai_(7th_dragon_series)": "character:samurai_(7th_dragon)",
    "deep-blue_series": "series:deep-blue",
    "samurai_(7th_dragon)": "character:samurai_(7th_dragon)",
    "series:fate_(series)": "series:fate",
    "series:pokemon_(anime)": "series:pokemon",
    "series:pokemon_(classic_anime)": "series:pokemon",
    "series:pokemon_(game)": "series:pokemon",
    "series:pokemon_bw_(anime)": "series:pokemon_bw",
    "series:pokemon_dppt_(anime)": "series:pokemon_dppt",
    "series:pokemon_emerald": "series:pokemon_rse",
    "series:pokemon_rse_(anime)": "series:pokemon_rse",
    "series:pokemon_sm_(anime)": "series:pokemon_sm",
    "series:pokemon_xy_(anime)": "series:pokemon_xy",
    "series:x-men:_the_animated_series": "series:x-men",
    "x-men:_the_animated_series": "series:x-men",
    "x-men_film_series": "series:x-men",
}

SERIES_PARENT_MAP = {
    "series:sen_no_kiseki": ("series:eiyuu_densetsu",),
    "series:sora_no_kiseki": ("series:eiyuu_densetsu",),
    "series:zero_no_kiseki": ("series:eiyuu_densetsu",),
    "series:ao_no_kiseki": ("series:eiyuu_densetsu",),
    "series:hajimari_no_kiseki": ("series:eiyuu_densetsu",),
    "series:kuro_no_kiseki": ("series:eiyuu_densetsu",),
    "series:kai_no_kiseki": ("series:eiyuu_densetsu",),
    "series:pokemon_bw": ("series:pokemon",),
    "series:pokemon_dppt": ("series:pokemon",),
    "series:pokemon_rse": ("series:pokemon",),
    "series:pokemon_sm": ("series:pokemon",),
    "series:pokemon_xy": ("series:pokemon",),
    "series:pokemon_rgby": ("series:pokemon",),
    "series:pokemon_gsc": ("series:pokemon",),
    "series:pokemon_swsh": ("series:pokemon",),
    "series:pokemon_sv": ("series:pokemon",),
    "series:pokemon_legends:_arceus": ("series:pokemon",),
    "series:fate/grand_order": ("series:fate",),
    "series:fate/stay_night": ("series:fate",),
    "series:fate/zero": ("series:fate",),
    "series:fate/extra": ("series:fate",),
    "series:fate/extra_ccc": ("series:fate", "series:fate/extra"),
    "series:fate/apocrypha": ("series:fate",),
    "series:fate/kaleid_liner_prisma_illya": ("series:fate",),
    "series:fate/prototype": ("series:fate",),
    "series:fate/strange_fake": ("series:fate",),
    "series:fate/hollow_ataraxia": ("series:fate",),
}

EXPLICIT_GROUP_PARENTS = {
    "eyewear", "headwear", "footwear", "legwear",
    "pushing", "lipstick_mark",
}
NO_GROUP_PARENT = {
    "hair", "skin", "pupils", "scales", "group",
}

_CONTEXT_CACHE = {}


def rating_from_score(total_score: int, safe_max: int, questionable_max: int) -> str:
    """Map a numeric total to the rating letter."""
    if total_score <= safe_max:
        return "s"
    if total_score <= questionable_max:
        return "q"
    return "e"


def parse_tags(tags: list[str]) -> tuple[str, str, str, str, str]:
    """Parses tags into their respective namespaces for SQLite cache storage."""
    tag_lists = {"general": [], "character": [], "artist": [], "series": []}
    source_tag = None
    for tag in tags:
        if tag in ("tagme", "artist:tagme", "character:tagme", "series:tagme"):
            continue
        if is_recognized_prefix(tag):
            prefix, _, value = tag.partition(":")
            if prefix in tag_lists:
                if value and value != "tagme":
                    tag_lists[prefix].append(value)
            elif prefix == "source":
                source_tag = value
            else:
                tag_lists["general"].append(tag)
        else:
            tag_lists["general"].append(tag)
    return (
        ",".join(tag_lists["general"]) or "tagme",
        ",".join(tag_lists["character"]),
        ",".join(tag_lists["artist"]),
        ",".join(tag_lists["series"]),
        source_tag or "",
    )


def _is_excluded_from_expansion(tag: str) -> bool:
    """Returns True if a tag is a negation/removed state that must not trigger parent tags."""
    return (
        tag.startswith("no_")
        or tag.endswith(("_removed", "_partially_removed"))
        or tag == "flat_chest"
    )


def _is_plural_variant(tag1: str, tag2: str) -> bool:
    """Returns True if two tags only differ by plural 's' on their words."""
    w1 = [w[:-1] if len(w) > 2 and w.endswith("s") else w for w in tag1.split("_")]
    w2 = [w[:-1] if len(w) > 2 and w.endswith("s") else w for w in tag2.split("_")]
    return w1 == w2


def load_tag_groups_metadata() -> tuple[dict[str, set[str]], set[str], set[str], set[str]]:
    """
    Parses tag_groups.json to build:
      1. child_to_parents: maps specific tags to all ancestor base/type tags (transitively closed).
      2. known_general: all valid general tags in tag_groups.json (plus explicit group parents).
      3. copyright_series: all tags under 'copyright -> other' to recognize as series.
      4. sex_group_tags: all tags under the 'sex' category for rating calculation.
    """
    child_to_parents: dict[str, set[str]] = defaultdict(set)
    known_general: set[str] = set(EXPLICIT_GROUP_PARENTS)
    copyright_series: set[str] = set()
    sex_group_tags: set[str] = set()

    candidates = [ROOT_DIR / "SD-Tag-Editor" / "tag_groups.json", ROOT_DIR / "tag_groups.json"]
    group_file = next((p for p in candidates if p.is_file()), None)
    if not group_file:
        return child_to_parents, known_general, copyright_series, sex_group_tags

    with group_file.open("r", encoding="utf-8") as f:
        data = json.load(f)

    for category, subgroups in data.items():
        if not isinstance(subgroups, dict):
            continue

        if category == "copyright":
            other_list = subgroups.get("other", [])
            if isinstance(other_list, list):
                copyright_series.update(other_list)
            continue

        for group_name, content in subgroups.items():
            if group_name == "other" and isinstance(content, list):
                known_general.update(content)
                if category == "sex":
                    sex_group_tags.update(content)
                continue

            if not isinstance(content, dict):
                continue

            bases = content.get("base", [])
            canonical_parent = (
                group_name
                if (group_name in bases or group_name in EXPLICIT_GROUP_PARENTS)
                and group_name not in NO_GROUP_PARENT
                else None
            )
            if canonical_parent:
                known_general.add(canonical_parent)

            group_tags = set()
            for sub_list in content.values():
                if not isinstance(sub_list, list):
                    continue
                for raw_t in sub_list:
                    known_general.add(raw_t)
                    if category == "sex":
                        sex_group_tags.add(raw_t)
                    if not _is_excluded_from_expansion(raw_t):
                        group_tags.add(raw_t)

            for t in group_tags:
                if canonical_parent and t != canonical_parent and not _is_plural_variant(t, canonical_parent):
                    child_to_parents[t].add(canonical_parent)

                for sub_t in group_tags:
                    if sub_t in (t, canonical_parent) or len(sub_t) >= len(t):
                        continue
                    if _is_plural_variant(t, sub_t):
                        continue
                    if "_under_" in t or "_over_" in t:
                        if t.startswith((f"{sub_t}_under_", f"{sub_t}_over_")):
                            child_to_parents[t].add(sub_t)
                    elif t.endswith(f"_{sub_t}") or t.startswith(f"{sub_t}_"):
                        child_to_parents[t].add(sub_t)

    for n in ("2", "3", "4", "5", "6+"):
        child_to_parents[f"{n}girls"].add("multiple_girls")
        child_to_parents[f"{n}boys"].add("multiple_boys")
        child_to_parents[f"{n}others"].add("multiple_others")

    child_to_parents["dark-skinned_female"].add("dark_skin")
    child_to_parents["dark-skinned_male"].add("dark_skin")
    child_to_parents["very_dark_skin"].add("dark_skin")
    child_to_parents["symphogear_pendant"].add("pendant")
    child_to_parents["dark_nipples"].add("nipples")

    for tag_key in list(child_to_parents.keys()):
        expanded = set(child_to_parents[tag_key])
        queue = list(expanded)
        while queue:
            curr = queue.pop()
            for parent in child_to_parents.get(curr, ()):
                if parent != tag_key and parent not in expanded:
                    expanded.add(parent)
                    queue.append(parent)
        child_to_parents[tag_key] = expanded

    return child_to_parents, known_general, copyright_series, sex_group_tags


class TagCurationContext:
    """Pre-loads dictionaries and memoizes single-tag normalization, enrichment,
    contextual character qualification, and parent expansion."""

    def __init__(self, mappings=None):
        if mappings is not None:
            self.char_map = dict(mappings.char)
            self.artist_map = (
                dict(mappings.artist)
                if isinstance(mappings.artist, dict)
                else {a: a for a in mappings.artist}
            )
        else:
            self.char_map, self.artist_map = self._load_char_and_artist_dbs()

        self.artist_set = set(self.artist_map.keys()) | set(self.artist_map.values())

        (
            self.child_to_parents,
            self.known_general,
            copyright_series,
            self.sex_group_tags,
        ) = load_tag_groups_metadata()

        self.series_set = {"original", "eiyuu_densetsu", "sen_no_kiseki", "sora_no_kiseki", \
            "zero_no_kiseki", "ao_no_kiseki", "hajimari_no_kiseki", "kuro_no_kiseki", "kai_no_kiseki"}
        for s_item in copyright_series:
            self.series_set.add(self._clean_series_name(s_item))

        for val in self.char_map.values():
            for s_item in self._iter_series_values(val):
                if s_item:
                    self.series_set.add(s_item)

        # Index qualified characters: (base_name, qualifier) -> full character name (e.g. ("kohaku", "dagasi") -> "kohaku_(dagasi)")
        self.qualified_char_map: dict[tuple[str, str], str] = {}
        self.char_stems: set[str] = set()
        for char_name in list(self.char_map.keys()):
            self._index_qualified_character(char_name)

        self.norm_general_map = {
            re.sub(r"[_()-]", "", t.lower()): t
            for t in self.known_general
            if re.fullmatch(r"[a-z0-9_()-]{3,}", t.lower())
            and len(re.sub(r"[_()-]", "", t.lower())) >= 3
        }

        self.bogus_prefixed_tags: set[str] = set()

    def _index_qualified_character(self, char_name: str):
        """Indexes 'base_(qualifier)' character names so unprefixed 'base' + artist/series upgrades automatically."""
        clean_c = char_name.strip()
        if clean_c.startswith("character:"):
            clean_c = clean_c.split(":", 1)[1]
        if not clean_c or clean_c == "tagme":
            return
        self.char_stems.add(clean_c)
        if "_(" in clean_c and clean_c.endswith(")"):
            base_part, qual_part = clean_c.rsplit("_(", 1)
            qual = qual_part[:-1].strip()
            if base_part and qual:
                self.char_stems.add(base_part)
                self.qualified_char_map[(base_part, qual)] = clean_c

    def register_db_vocabulary(self, tag_names, tag_counts=None):
        """
        1. Indexes 'character:base_(qualifier)' when 'qualifier' is a verified artist/series.
        2. If tag_counts is provided, detects dormant mistagged 'artist:X'/'character:X' tags
           where X is not in artists.db/characters.db and is a known/high-count general tag.
        """
        verified_qualifiers = self.artist_set | self.series_set
        counts = tag_counts if tag_counts is not None else {}

        for t_name in tag_names:
            if t_name.startswith("character:"):
                c_val = t_name.split(":", 1)[1].strip()
                if "_(" in c_val and c_val.endswith(")"):
                    base_part, qual_part = c_val.rsplit("_(", 1)
                    qual = qual_part[:-1].strip()
                    if base_part and qual in verified_qualifiers:
                        self.char_stems.add(base_part)
                        self.qualified_char_map[(base_part, qual)] = c_val
                    continue

                # Unqualified character:X not in characters.db -> check if it's a mistagged general tag
                if c_val not in self.char_map:
                    p_cnt = counts.get(t_name, 0)
                    u_cnt = counts.get(c_val, 0)
                    if c_val in self.known_general or (u_cnt >= 100 and u_cnt >= p_cnt * 10):
                        self.bogus_prefixed_tags.add(t_name)

            elif t_name.startswith("artist:"):
                a_val = t_name.split(":", 1)[1].strip()
                if "_(" not in a_val and a_val not in self.artist_set:
                    p_cnt = counts.get(t_name, 0)
                    u_cnt = counts.get(a_val, 0)
                    if a_val in self.known_general or (u_cnt >= 100 and u_cnt >= p_cnt * 10):
                        self.bogus_prefixed_tags.add(t_name)

    def _expand_series_with_parents(self, series_tag: str) -> list[str]:
        """Normalizes a series:... tag and appends any parent franchise series."""
        if kiseki := resolve_kiseki_series(series_tag.lower()):
            return list(kiseki)
        base_s = self._clean_series_name(series_tag.split(":", 1)[1] if ":" in series_tag else series_tag)
        if not base_s or base_s == "tagme":
            return []
        if base_s in {"none", "original"}:
            return ["series:original"]
        full_s = f"series:{base_s}"
        out = [full_s]
        for parent_s in SERIES_PARENT_MAP.get(full_s, ()):
            if parent_s not in out:
                out.append(parent_s)
        # If sub-series is formatted like 'the_legend_of_zelda:_phantom_hourglass', also add 'series:the_legend_of_zelda'
        if ":_" in base_s:
            parent_prefix = base_s.split(":_", 1)[0].strip("_")
            if parent_prefix in self.series_set:
                parent_tag = f"series:{parent_prefix}"
                if parent_tag not in out:
                    out.append(parent_tag)
        return out

    def _iter_series_values(self, raw_val) -> list[str]:
        """Splits comma-separated series strings from characters.db into clean series names."""
        if not raw_val:
            return []
        items = raw_val if isinstance(raw_val, (list, tuple, set)) else [p.strip() for p in str(raw_val).split(",") if p.strip()]

        out = []
        for item in items:
            cleaned = self._clean_series_name(item)
            if cleaned and cleaned != "tagme":
                out.append(cleaned)
        return out

    @staticmethod
    def _clean_series_name(name: str) -> str:
        clean = re.sub(r"\s+", "_", name.strip().strip("\"'"))
        clean = re.sub(r"_series\)$", ")", clean)
        return re.sub(r"_\(series\)$", "", clean)

    @classmethod
    def _load_char_and_artist_dbs(cls) -> tuple[dict[str, str], dict[str, str]]:
        char_map = {}
        if CDB_PATH.is_file():
            with sqlite3.connect(CDB_PATH) as conn:
                for row in conn.execute("SELECT * FROM data"):
                    if not row or not row[0] or not row[0].strip():
                        continue
                    c_raw = row[0].strip()
                    s_raw = row[1].strip() if len(row) >= 2 and row[1] else ""
                    char_map[c_raw] = s_raw
                    c_unquoted = c_raw.strip("\"'")
                    if c_unquoted and c_unquoted not in char_map:
                        char_map[c_unquoted] = s_raw

        artist_map = {}
        if ADB_PATH.is_file():
            with sqlite3.connect(ADB_PATH) as conn:
                for row in conn.execute("SELECT * FROM data"):
                    if not row or not row[0] or not row[0].strip():
                        continue
                    a_raw = row[0].strip()
                    a_fixed = row[1].strip() if len(row) >= 2 and row[1] and row[1].strip() else a_raw
                    artist_map[a_raw] = a_fixed
                    artist_map[a_fixed] = a_fixed

        return char_map, artist_map

    def is_safe_mapping(self, sidecar_tag: str, canonical_tag: str) -> bool:
        """Guards against poisoned dynamic mappings like 'eyes -> mizumizuni' or 'kohaku -> _DROP_'."""
        s_clean = sidecar_tag.strip()
        c_clean = canonical_tag.strip()
        s_lower = s_clean.lower()

        if c_clean == "_DROP_":
            return not (
                s_clean in self.char_map
                or s_clean in self.artist_set
                or s_clean in self.series_set
                or resolve_kiseki_series(s_lower) is not None
                or s_lower.endswith(("_series", "_(series)", "_(artist)", "_(character)", "_(cosplay)"))
            )

        # Prevent known general tags (like 'eyes') from mapping to an artist, character, or series
        if s_clean in self.known_general and not is_recognized_prefix(s_clean):
            for target_token in c_clean.split():
                if (
                    target_token.startswith(("artist:", "character:", "series:"))
                    or target_token in self.artist_set
                    or target_token in self.char_map
                ):
                    return False

        return True

    def _expand_series_with_parents(self, series_tag: str) -> list[str]:
        """Normalizes a series:... tag and appends any parent franchise series (e.g. series:eiyuu_densetsu)."""
        if kiseki := resolve_kiseki_series(series_tag.lower()):
            return list(kiseki)
        base_s = self._clean_series_name(series_tag.split(":", 1)[1] if ":" in series_tag else series_tag)
        if not base_s or base_s == "tagme":
            return []
        if base_s in {"none", "original"}:
            return ["series:original"]
        full_s = f"series:{base_s}"
        out = [full_s]
        for parent_s in SERIES_PARENT_MAP.get(full_s, ()):
            if parent_s not in out:
                out.append(parent_s)
        return out

    def _extract_character_series_and_artist(self, base_c: str) -> list[str]:
        """Extracts series (and parenthetical series) for a character name."""
        extra = []
        if base_c in self.char_map:
            for s_name in self._iter_series_values(self.char_map[base_c]):
                extra.extend(self._expand_series_with_parents(f"series:{s_name}"))

        # Also check parenthetical qualifiers on character:name_(qualifier)
        for paren_match in re.findall(r"_\(([^()]+)\)", base_c):
            qual_clean = self._clean_series_name(paren_match.lower())
            if qual_clean in self.series_set or resolve_kiseki_series(qual_clean) is not None:
                extra.extend(self._expand_series_with_parents(f"series:{qual_clean}"))

        return extra

    def _normalize_single_token(self, tag: str, lookup_map: dict) -> tuple[str, ...]:
        """Applies lookup_map (supporting multi-tag targets), MASTER_MERGE_LIST, and resolve_prefixed_tag."""
        mapped = MASTER_MERGE_LIST.get(tag, tag)
        if tag in lookup_map and self.is_safe_mapping(tag, lookup_map[tag]):
            mapped = lookup_map[tag]

        if mapped in ("_DROP_", "tagme", "artist:tagme", "character:tagme", "series:tagme"):
            return ()

        tokens = mapped.split() if " " in mapped else [mapped]
        final_tokens = []

        for tok in tokens:
            resolved = resolve_prefixed_tag(tok)
            norm_tok = resolved if resolved is not None else tok
            if norm_tok == "_DROP_":
                continue

            for raw_sub in (norm_tok.split() if " " in norm_tok else [norm_tok]):
                cur_tok = (
                    lookup_map[raw_sub]
                    if raw_sub != tag and raw_sub in lookup_map and self.is_safe_mapping(raw_sub, lookup_map[raw_sub])
                    else raw_sub
                )
                cur_tok = MASTER_MERGE_LIST.get(cur_tok, cur_tok)
                if cur_tok in ("_DROP_", "tagme", "artist:tagme", "character:tagme", "series:tagme"):
                    continue
                second_res = resolve_prefixed_tag(cur_tok)
                if second_res == "_DROP_":
                    continue
                if second_res is not None:
                    final_tokens.extend(second_res.split())
                else:
                    final_tokens.append(cur_tok)

        return tuple(final_tokens)

    def expand_single_tag(self, raw_tag: str, lookup_map: dict) -> tuple[str, ...]:
        """Deterministically normalizes, enriches (char/artist/series/language),
        and expands tag_groups.json parent tags for a single tag."""
        tag = re.sub(r"\s+", "_", raw_tag.strip())
        if not tag or tag in ("tagme", "artist:tagme", "character:tagme", "series:tagme", "_DROP_"):
            return ()

        if tag.startswith("tagai:"):
            inner_raw = tag.split(":", 1)[1].strip()
            if not inner_raw:
                return ()
            inner_norms = self._normalize_single_token(inner_raw, lookup_map)
            out_ai = []
            for inner_norm in inner_norms:
                if inner_norm.startswith(("source:", "rating:", "booru:")):
                    continue
                inner_base = inner_norm.split(":", 1)[1] if is_recognized_prefix(inner_norm) else inner_norm
                out_ai.append(f"tagai:{inner_base}")
                for p_tag in self.child_to_parents.get(inner_base, ()):
                    out_ai.append(f"tagai:{p_tag}")
            return tuple(out_ai)

        norm_tokens = self._normalize_single_token(tag, lookup_map)
        if not norm_tokens:
            return ()

        results = []
        for raw_norm in norm_tokens:
            if raw_norm.startswith(("booru:", "source:", "rating:", "meta:", "language:", "site:", "studio:")):
                results.append(raw_norm)
                continue

            norm_tag = (
                raw_norm.split(":", 1)[1].strip()
                if raw_norm in self.bogus_prefixed_tags
                else raw_norm
            )

            if norm_tag.startswith("character:"):
                base_c = norm_tag.split(":", 1)[1].strip()
                if not base_c or base_c == "tagme":
                    continue
                results.append(f"character:{base_c}")
                results.extend(self._extract_character_series_and_artist(base_c))
                continue

            if norm_tag.startswith("artist:"):
                base_a = norm_tag.split(":", 1)[1].strip()
                if not base_a or base_a == "tagme":
                    continue
                canonical_a = self.artist_map.get(base_a, base_a)
                results.append(f"artist:{canonical_a}")
                continue

            if norm_tag.startswith("series:"):
                results.extend(self._expand_series_with_parents(norm_tag))
                continue

            # Unprefixed tags: check Kiseki / known series / characters / artists
            if kiseki_series := resolve_kiseki_series(norm_tag.lower()):
                results.extend(kiseki_series)
                continue

            is_known_gen = norm_tag in self.known_general
            matched_entity = False

            if not is_known_gen:
                if norm_tag in self.char_map:
                    matched_entity = True
                    results.append(f"character:{norm_tag}")
                    results.extend(self._extract_character_series_and_artist(norm_tag))

                if norm_tag in self.artist_map:
                    matched_entity = True
                    results.append(f"artist:{self.artist_map[norm_tag]}")

                clean_s = self._clean_series_name(norm_tag)
                if not matched_entity and (
                    norm_tag in self.series_set
                    or clean_s in self.series_set
                    or norm_tag.endswith("_series")
                ):
                    matched_entity = True
                    results.extend(self._expand_series_with_parents(f"series:{clean_s}"))

            if matched_entity:
                continue

            results.append(norm_tag)
            for parent_tag in self.child_to_parents.get(norm_tag, ()):
                results.append(parent_tag)

        return tuple(results)


def get_curation_context(mappings=None) -> TagCurationContext:
    """Returns a cached TagCurationContext instance."""
    if "default" not in _CONTEXT_CACHE:
        _CONTEXT_CACHE["default"] = TagCurationContext(mappings=mappings)
    return _CONTEXT_CACHE["default"]


def apply_tag_curation(tags, dynamic_mappings=None, context=None, single_tag_cache=None):
    """In-place curation, prefix normalization, character/artist/series enrichment,
    contextual character qualification (e.g., kohaku + artist:dagasi -> character:kohaku_(dagasi)),
    tag_groups.json parent expansion, and redundant tagai: removal."""
    ctx = context if context is not None else get_curation_context()
    lookup_map = dynamic_mappings if dynamic_mappings is not None else {}

    expanded_tags = []
    for raw_tag in tags:
        if single_tag_cache is not None:
            res = single_tag_cache.get(raw_tag)
            if res is None:
                res = ctx.expand_single_tag(raw_tag, lookup_map)
                single_tag_cache[raw_tag] = res
        else:
            res = ctx.expand_single_tag(raw_tag, lookup_map)
        expanded_tags.extend(res)

    # Contextual Character Qualification (runs against raw `tags` too so `kohaku,_DROP_`
    # still upgrades to `character:kohaku_(dagasi)` when `artist:dagasi` is on the image,
    # while dropping bare `kohaku` on images without a matching qualifier)
    image_qualifiers = set()
    for t in expanded_tags:
        if t.startswith(("artist:", "series:")):
            image_qualifiers.add(t.split(":", 1)[1])

    if image_qualifiers and ctx.qualified_char_map:
        for raw_t in tags:
            clean_raw = re.sub(r"\s+", "_", raw_t.strip())
            base_cand = None
            if not is_recognized_prefix(clean_raw):
                base_cand = clean_raw
            elif clean_raw.startswith("character:") and "_(" not in clean_raw:
                base_cand = clean_raw.split(":", 1)[1]

            if base_cand and base_cand in ctx.char_stems:
                for q in image_qualifiers:
                    if (base_cand, q) in ctx.qualified_char_map:
                        qc = ctx.qualified_char_map[(base_cand, q)]
                        expanded_tags.append(f"character:{qc}")
                        expanded_tags.extend(ctx._extract_character_series_and_artist(qc))  # pylint: disable=protected-access

    expanded_set = set(expanded_tags)
    prefixes = ("artist:", "character:", "series:", "source:", "language:", "meta:")

    real_bases = set()
    prefixed_bases = set()
    for tag in expanded_set:
        if tag.startswith("tagai:"):
            continue
        if not tag.startswith(("booru:", "source:", "rating:")):
            real_bases.add(tag)
            if is_recognized_prefix(tag):
                base_val = tag.split(":", 1)[1]
                real_bases.add(base_val)
                if tag.startswith(prefixes):
                    prefixed_bases.add(base_val)
                if tag.startswith("character:") and "_(" in base_val and base_val.endswith(")"):
                    stem_val = base_val.rsplit("_(", 1)[0]
                    prefixed_bases.add(stem_val)
                    real_bases.add(stem_val)
                if tag.startswith("language:"):
                    real_bases.add(f"{base_val}_text")
                    real_bases.add(f"{base_val}_language")

    final_tags = []
    seen = set()
    for tag in expanded_tags:
        if tag in ("tagme", "artist:tagme", "character:tagme", "series:tagme", "_DROP_"):
            continue

        if not is_recognized_prefix(tag) and tag in prefixed_bases:
            continue

        if tag.endswith("_(cosplay)") and f"character:{tag[:-10]}" in expanded_set:
            if "cosplay" not in seen:
                final_tags.append("cosplay")
                seen.add("cosplay")
            continue

        if tag.startswith("tagai:"):
            ai_base = tag.split(":", 1)[1]
            if ai_base in real_bases:
                continue
            ai_resolved = resolve_prefixed_tag(ai_base)
            if ai_resolved and (ai_resolved in real_bases or ai_resolved.split(":", 1)[-1] in real_bases):
                continue

        if tag not in seen:
            final_tags.append(tag)
            seen.add(tag)

    tags[:] = final_tags


def get_sidecar_tags(image_path):
    """Scans for .txt files associated with the image and parses tags."""
    extra_tags = []
    txt_candidates = [
        image_path.with_suffix(".txt"),
        image_path.with_name(image_path.name + ".txt")
    ]

    for txt_path in txt_candidates:
        if not txt_path.is_file():
            continue

        with txt_path.open("r", encoding="utf-8") as f:
            for line in f:
                line_clean = html.unescape(line.strip())
                if not line_clean or line_clean.startswith("#"):
                    continue
                parts = [t.strip().lower() for t in re.split(r"[,;]", line_clean) if t.strip()]
                extra_tags.extend(re.sub(r"\s+", "_", t) for t in parts if t)
    return extra_tags


def load_dynamic_mappings(csv_path):
    """Reads the mined tag map CSV and returns a dictionary of mappings."""
    dynamic_map = {}
    csv_file = Path(csv_path)

    if not csv_file.is_file():
        return dynamic_map

    with csv_file.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sidecar_tag = (row.get("Sidecar_Tag") or "").strip()
            canonical_tag = (row.get("Canonical_Tag") or "").strip()
            if not sidecar_tag or not canonical_tag:
                continue

            conf_raw = row.get("Confidence")
            confidence = float(conf_raw) if conf_raw not in (None, "") else 1.0
            if confidence >= 0.60:
                dynamic_map[sidecar_tag] = canonical_tag

    return dynamic_map
