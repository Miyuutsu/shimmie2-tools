"""Functions for parsing, sorting, formatting, and reading text tags."""
import re
import html
import csv
from pathlib import Path

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

def apply_tag_curation(tags, dynamic_mappings=None):
    """In‑place fixing of tags that need messing."""
    prefixes = ('artist:', 'character:', 'series:', 'source:')
    master_merge_list = {
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
        "x-men_film_series": "series:x-men"
    }

    if dynamic_mappings:
        master_merge_list.update(dynamic_mappings)

    original_set = set(tags)
    step1_tags = []

    for tag in tags:
        if ':' not in tag:
            if any(f"{p}{tag}" in original_set for p in prefixes):
                continue
        step1_tags.append(tag)

    step2_tags = [master_merge_list.get(tag, tag) for tag in step1_tags]
    merged_set = set(step2_tags)
    step3_tags = []

    for tag in step2_tags:
        if ':' not in tag:
            if any(f"{p}{tag}" in merged_set for p in prefixes):
                continue
        step3_tags.append(tag)

    step3_set = set(step3_tags)
    step4_tags = []
    for tag in step3_tags:
        if tag.endswith("_(cosplay)"):
            if f"character:{tag[:-10]}" in step3_set:
                step4_tags.append("cosplay")
                continue
        step4_tags.append(tag)

    tags[:] = [t for t in step4_tags if t not in ("tagme", "_DROP_")]

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
                line = html.unescape(line.strip())
                if not line or line.startswith("#"):
                    continue
                parts = [t.strip() for t in re.split(r"[,;]", line) if t.strip()]
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
            if float(row.get("Confidence", 1.0)) >= 0.75:
                dynamic_map[row["Sidecar_Tag"]] = row["Canonical_Tag"]

    return dynamic_map
