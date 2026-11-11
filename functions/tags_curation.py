"""Functions for parsing, sorting, formatting, and reading text tags."""
import csv
import html
from pathlib import Path
import re

from functions.source_resolver import resolve_prefixed_tag

def rating_from_score(total_score: int, safe_max: int, questionable_max: int) -> str:
    """Map a numeric total to the rating letter."""
    if total_score <= safe_max:
        return "s"
    if total_score <= questionable_max:
        return "q"
    return "e"

def parse_tags(tags: list[str]) -> tuple[str, str, str, str, str]:
    """Parses tags into their respective namespaces."""
    tag_lists = {'general': [], 'character': [], 'artist': [], 'series': []}
    source_tag = None
    for tag in tags:
        prefix, _, value = tag.partition(':')
        if prefix in tag_lists:
            tag_lists[prefix].append(value)
        elif prefix == "source":
            source_tag = value
        else:
            tag_lists["general"].append(tag)
    return (
        ",".join(tag_lists["general"]) or "tagme",
        ",".join(tag_lists["character"]),
        ",".join(tag_lists["artist"]),
        ",".join(tag_lists["series"]),
        source_tag or ""
    )

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


def _normalize_structural_tag(tag, lookup_map=None):
    """Normalizes foreign prefixes (female:, copyright:, rating:, *_(artist), etc.)
    and re-checks lookup_map if the normalized form maps to a canonical tag."""
    resolved = resolve_prefixed_tag(tag)
    norm_tag = resolved if resolved is not None else tag
    if lookup_map and norm_tag != tag:
        return lookup_map.get(norm_tag, MASTER_MERGE_LIST.get(norm_tag, norm_tag))
    return norm_tag


def apply_tag_curation(tags, dynamic_mappings=None):
    """In-place fixing of tags without copying dynamic_mappings per call."""
    prefixes = ("artist:", "character:", "series:", "source:")
    original_set = set(tags)
    step1_tags = [
        tag for tag in tags
        if ":" in tag or not any(f"{p}{tag}" in original_set for p in prefixes)
    ]

    if dynamic_mappings:
        step2_tags = [
            _normalize_structural_tag(
                dynamic_mappings.get(tag, MASTER_MERGE_LIST.get(tag, tag)),
                dynamic_mappings,
            )
            for tag in step1_tags
        ]
    else:
        step2_tags = [
            _normalize_structural_tag(
                MASTER_MERGE_LIST.get(tag, tag),
                MASTER_MERGE_LIST,
            )
            for tag in step1_tags
        ]

    merged_set = set(step2_tags)
    step3_tags = [
        tag for tag in step2_tags
        if ":" in tag or not any(f"{p}{tag}" in merged_set for p in prefixes)
    ]

    step3_set = set(step3_tags)
    step4_tags = []
    for tag in step3_tags:
        if tag.endswith("_(cosplay)") and f"character:{tag[:-10]}" in step3_set:
            step4_tags.append("cosplay")
            continue
        step4_tags.append(tag)

    # Collect all real base tags on the image to strip redundant tagai:* tags
    real_bases = set()
    for tag in step4_tags:
        if not tag.startswith(("tagai:", "booru:", "source:", "rating:")):
            real_bases.add(tag)
            if ":" in tag:
                real_bases.add(tag.split(":", 1)[1])

    final_tags = []
    for tag in step4_tags:
        if tag in ("tagme", "_DROP_"):
            continue
        if tag.startswith("tagai:"):
            ai_base = tag.split(":", 1)[1]
            ai_mapped = (
                dynamic_mappings.get(ai_base, MASTER_MERGE_LIST.get(ai_base, ai_base))
                if dynamic_mappings
                else MASTER_MERGE_LIST.get(ai_base, ai_base)
            )
            if ai_base in real_bases or ai_mapped in real_bases:
                continue
        final_tags.append(tag)

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
