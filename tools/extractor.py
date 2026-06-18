"""Tool for extracting specific images from Shimmie based on tags and resolution."""
import shutil
from pathlib import Path
from collections import defaultdict
import psycopg2
import tqdm

from functions.db_cache import get_shimmie_db_credentials

# Strict namespace mapping to protect emoticons (e.g., :3, <:)
VALID_NAMESPACES = {
    "character", "artist", "series", "meta", "general",
    "copyright", "studio", "pool", "lore", "species"
}

def _write_extraction_report(out_dir, character_tracking, selected_images):
    """Generates a detailed text report of successes and skipped characters."""
    report_path = out_dir / "extraction_report.txt"
    with open(report_path, "w", encoding="utf-8") as rf:
        rf.write("=== EXTRACTION REPORT ===\n\n")
        rf.write(f"Total Successfully Extracted: {len(selected_images)}\n")
        rf.write("--- SUCCESSFULLY EXTRACTED ---\n")

        selected_images.sort(key=lambda x: x['char_name'])
        for cand in selected_images:
            char_clean = cand['char_name'].replace("character:", "")
            artist_clean = cand['artist'].replace("artist:", "@")
            rf.write(f"[{char_clean}] -> Artist: {artist_clean} | File: {cand['dest_filename']}\n")

        rf.write("\n\n--- SKIPPED CHARACTERS ---\n")
        skipped_chars = [c for c, d in character_tracking.items() if not d['extracted']]
        skipped_chars.sort()

        rf.write(f"Total Skipped Characters: {len(skipped_chars)}\n")

        for sc in skipped_chars:
            data = character_tracking[sc]
            char_clean = sc.replace("character:", "")
            rf.write(f"\n{char_clean}:\n")

            for reason, count in sorted(data['reasons'].items(), key=lambda x: x[1], reverse=True):
                rf.write(f"  - {count} images skipped: {reason}\n")

            if data['available_artists']:
                artists = [a.replace("artist:", "@") for a in sorted(data['available_artists'])]
                rf.write(f"  -> Artist Conflict: Competed for and lost {', '.join(artists)}\n")

    print(f"\n[✓] Detailed extraction report written to {report_path.resolve()}")

def run_extractor(args):
    """Main execution flow for extracting character images."""
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    shimmie_root = Path(args.spath)

    # Optional Uniqueness Toggles
    enforce_uchar = getattr(args, 'uchar', False)
    enforce_uartist = getattr(args, 'uartist', False)

    positive_tags = set()
    negative_tags = set()
    or_groups = []

    if args.require_tags:
        for tag in args.require_tags:
            if tag.startswith("-") or tag.startswith("!"):
                negative_tags.add(tag[1:])
            elif "|" in tag or " or " in tag:
                # Normalize both syntax styles and split into a required group
                normalized_tag = tag.replace(" or ", "|")
                group = set(t.strip() for t in normalized_tag.split("|") if t.strip())
                if group:
                    or_groups.append(group)
            else:
                positive_tags.add(tag)

    db_config = get_shimmie_db_credentials(args.spath)
    if not db_config:
        print(f"[ERROR] Could not load DB credentials from {args.spath}")
        return

    print(f"[*] Connecting to database... (Min Pixels: {args.min_pixels})")
    with psycopg2.connect(**db_config) as conn:
        conn.set_session(readonly=True)

        print("[*] Calculating total eligible images...")
        count_cur = conn.cursor()
        # Fast, aggregate-free count query
        count_cur.execute("""
            SELECT COUNT(DISTINCT i.id)
            FROM images i
            WHERE (i.width * i.height) >= %s
            AND i.ext IN ('jpg', 'jpeg', 'png', 'JPG', 'JPEG', 'PNG')
        """, (args.min_pixels,))
        total_eligible = count_cur.fetchone()[0]
        count_cur.close()

        if total_eligible == 0:
            print("[!] No images meet the resolution and format requirements.")
            return

        # Stable, known-good data fetch query
        cur = conn.cursor(name='fetch_cursor')
        cur.itersize = 5000
        cur.execute("""
            SELECT i.id, i.hash, i.ext, (i.width * i.height) as pixels, string_agg(t.tag, ' ') as all_tags
            FROM images i
            JOIN image_tags it ON i.id = it.image_id
            JOIN tags t ON it.tag_id = t.id
            WHERE (i.width * i.height) >= %s
            AND i.ext IN ('jpg', 'jpeg', 'png', 'JPG', 'JPEG', 'PNG')
            GROUP BY i.id, i.hash, i.ext, i.width, i.height
        """, (args.min_pixels,))

        character_tracking = defaultdict(lambda: {
            'extracted': False,
            'valid_candidates': [],
            'reasons': defaultdict(int),
            'available_artists': set()
        })

        for img_id, hsh, ext, pixels, tag_string in tqdm.tqdm(cur, total=total_eligible, desc="Analyzing Metadata"):
            tags = set(tag_string.split())

            # Create a clean set for filter evaluation that strips known namespaces (like meta:)
            filter_test_tags = set(tags)
            for t in tags:
                if ":" in t:
                    parts = t.split(":", 1)
                    if parts[0] in VALID_NAMESPACES:
                        filter_test_tags.add(parts[1])

            char_tags = [t for t in tags if t.startswith("character:")]
            if not char_tags:
                continue

            is_solo = len(char_tags) == 1
            artist_tags = [t for t in tags if t.startswith("artist:")]
            has_valid_artist = len(artist_tags) == 1 and "artist:tagme" not in tags

            has_req_tags = not positive_tags or positive_tags.issubset(filter_test_tags)
            ignored_tags_found = negative_tags.intersection(filter_test_tags) if negative_tags else set()

            # --- THE NEW OR-GROUP LOGIC ---
            # Evaluates each term independently to support internal negative logic
            failed_or_group = None
            for group in or_groups:
                group_satisfied = False
                for term in group:
                    if term.startswith("-") or term.startswith("!"):
                        neg_term = term[1:]
                        if neg_term not in filter_test_tags:
                            group_satisfied = True
                            break
                    else:
                        if term in filter_test_tags:
                            group_satisfied = True
                            break

                if not group_satisfied:
                    failed_or_group = " | ".join(group)
                    break

            for c_tag in char_tags:
                if not is_solo:
                    character_tracking[c_tag]['reasons']['Multiple characters in image'] += 1
                    continue
                if not has_req_tags:
                    character_tracking[c_tag]['reasons']['Missing required positive tags'] += 1
                    continue
                if failed_or_group:
                    character_tracking[c_tag]['reasons'][f'Failed OR condition ({failed_or_group})'] += 1
                    continue
                if ignored_tags_found:
                    violators = ", ".join(ignored_tags_found)
                    character_tracking[c_tag]['reasons'][f'Contains ignored tags ({violators})'] += 1
                    continue
                if not has_valid_artist:
                    character_tracking[c_tag]['reasons']['Invalid artist tag (multiple, none, or tagme)'] += 1
                    continue

                artist_name = artist_tags[0]
                character_tracking[c_tag]['available_artists'].add(artist_name)
                character_tracking[c_tag]['valid_candidates'].append({
                    'id': img_id, 'hash': hsh, 'ext': ext, 'tags': tags,
                    'artist': artist_name, 'pixels': pixels, 'char_name': c_tag
                })

        cur.close()

    valid_chars = [c for c, data in character_tracking.items() if data['valid_candidates']]

    if not valid_chars:
        print("\n[!] No characters passed the tag filters.")
        _write_extraction_report(out_dir, character_tracking, [])
        return

    # Keep sorting by available artists just in case constraints are toggled on,
    # as it remains harmless when they are off.
    sorted_chars = sorted(
        valid_chars,
        key=lambda c: len(character_tracking[c]['available_artists'])
    )

    seen_artists = set()
    selected_images = []

    print(f"\n[*] Strategizing extraction for {len(sorted_chars)} valid characters...")

    for char_name in sorted_chars:
        candidates = character_tracking[char_name]['valid_candidates']
        candidates.sort(key=lambda x: x['pixels'], reverse=True)

        extracted_for_this_char = 0

        for cand in candidates:
            # Handle the unique artist constraint
            if enforce_uartist and cand['artist'] in seen_artists:
                continue

            # Selection confirmed
            seen_artists.add(cand['artist'])
            character_tracking[char_name]['extracted'] = True
            selected_images.append(cand)
            extracted_for_this_char += 1

            # Handle the unique character constraint
            if enforce_uchar:
                break

        # Log failures cleanly if no candidates survived the loop
        if extracted_for_this_char == 0:
            character_tracking[char_name]['reasons']['Artists claimed by other characters (uartist conflict)'] = len(candidates)

    print(f"[*] Ready to extract {len(selected_images)} images.")
    copied_count = 0

    for cand in tqdm.tqdm(selected_images, desc="Copying & Tagging"):
        hsh = cand['hash']
        ext = cand['ext']
        tags = cand['tags']
        character_name = cand['char_name']

        prefix1, prefix2 = hsh[0:2], hsh[2:4]
        src_path = shimmie_root / "data" / "images" / prefix1 / prefix2 / hsh

        if not src_path.exists():
            src_path = shimmie_root / "data" / "images" / prefix1 / prefix2 / f"{hsh}.{ext}"
            if not src_path.exists():
                character_tracking[character_name]['extracted'] = False
                character_tracking[character_name]['reasons']['File missing from hard drive'] += 1
                selected_images.remove(cand)
                continue

        clean_char_name = character_name.replace("character:", "").replace("/", "_")
        dest_filename = f"{clean_char_name}_{hsh}.{ext}"
        dest_img_path = out_dir / dest_filename

        cand['dest_filename'] = dest_filename

        shutil.copy2(src_path, dest_img_path)

        formatted_chars = []
        formatted_artists = []
        formatted_others = []

        for t in tags:
            if t.startswith(("tagai:", "booru:")):
                continue

            if ":" in t:
                parts = t.split(":", 1)
                # PROTECT EMOTICONS AND TIMES
                if parts[0] in VALID_NAMESPACES:
                    namespace, base_tag = parts
                else:
                    namespace, base_tag = "", t
            else:
                namespace, base_tag = "", t

            clean_tag = base_tag.replace("_", " ").strip()

            if not clean_tag:
                continue

            if namespace == "character":
                formatted_chars.append(clean_tag)
            elif namespace == "artist":
                formatted_artists.append(f"@{clean_tag}")
            else:
                formatted_others.append(clean_tag)

        formatted_chars.sort()
        formatted_artists.sort()
        formatted_others.sort()

        final_tag_list = formatted_chars + formatted_artists + formatted_others

        dest_txt_path = dest_img_path.with_suffix('.txt')
        dest_txt_path.write_text(", ".join(final_tag_list), encoding='utf-8')

        copied_count += 1

    _write_extraction_report(out_dir, character_tracking, selected_images)
