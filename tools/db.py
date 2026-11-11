# pylint: disable=duplicate-code,line-too-long,too-many-locals,too-many-branches,too-many-statements
"""Database management tools (SQLite Conversions, Precaching, Rating Updates, Tag Curation)."""
from collections import Counter, defaultdict
import csv
import re
from pathlib import Path
import sqlite3

import psycopg2
import psycopg2.extras
import tqdm

from functions.db_cache import get_shimmie_db_credentials
from functions.source_resolver import get_source_score, resolve_prefixed_tag
from functions.tags_curation import (
    MASTER_MERGE_LIST,
    apply_tag_curation,
    load_dynamic_mappings,
    rating_from_score,
)
from functions.tags_mining import TagCategoryGuard, _fetch_global_context, calculate_equivalencies
from tools.csv_builder import load_mappings


# ==========================================
# Tool 1: CSV to SQLite
# ==========================================
def csv_to_sqlite(args):
    """Converts a CSV file to an SQLite database."""
    csv_path = Path(args.csv)
    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)

        safe_table = f'"{args.table.replace("\"", "\"\"")}"'
        create_cols = ", ".join([f'"{h.replace("\"", "\"\"")}" TEXT' for h in headers])
        insert_cols = ", ".join([f'"{h.replace("\"", "\"\"")}"' for h in headers])
        placeholders = ", ".join(["?"] * len(headers))

        with sqlite3.connect(args.db) as conn:
            cursor = conn.cursor()
            if args.drop_table:
                cursor.execute(f"DROP TABLE IF EXISTS {safe_table}")
            cursor.execute(f"CREATE TABLE {safe_table} ({create_cols})")
            cursor.executemany(
                f"INSERT INTO {safe_table} ({insert_cols}) VALUES ({placeholders})",
                reader
            )

    print(f"[✓] Converted '{args.csv}' to '{args.db}' in table '{args.table}'.")


# ==========================================
# Tool 2: Update Postgres Ratings
# ==========================================
def _calculate_new_rating(tags, current_rating, tag_rating_map, smax, qmax):
    """Calculates a single image's new rating from its tag list."""
    total_score = 0
    for tag in tags:
        if not tag or tag.startswith(("booru:", "tagai:")):
            continue

        base_tag = tag.split(":", 1)[-1] if ":" in tag else tag
        weight = tag_rating_map.get(base_tag)
        if weight is None:
            continue
        if weight == 1 and total_score == 0:
            total_score = 1
        elif weight > 1:
            total_score += weight

    calc_rating = rating_from_score(total_score, smax, qmax) if total_score > 0 else None
    return calc_rating if calc_rating else current_rating


def update_ratings(args):
    """Updates existing ratings in shimmiedb based on dominant tags."""
    script_dir = Path(__file__).parent.parent.resolve()
    db_path = script_dir / "database" / "tag_rating_dominant.db"
    tag_rating_map = {}

    with sqlite3.connect(db_path) as conn:
        tag_rating_map.update({
            t.strip(): int(r) for t, r in conn.execute(
                "SELECT tag_name, dominant_rating FROM dominant_tag_ratings"
            )
        })

    pg_config = get_shimmie_db_credentials(args.spath)
    if not pg_config:
        print(f"[ERROR] Could not load DB credentials from {args.spath}")
        return

    with psycopg2.connect(**pg_config) as pg_conn:
        pg_cur = pg_conn.cursor()
        print("Fetching images and tags from Postgres...")
        pg_cur.execute("""
            SELECT i.id, i.rating, array_agg(t.tag)
            FROM images i
            LEFT JOIN image_tags it ON i.id = it.image_id
            LEFT JOIN tags t ON it.tag_id = t.id
            GROUP BY i.id, i.rating
        """)
        rows = pg_cur.fetchall()

        updates = []
        for image_id, current_rating, tags in tqdm.tqdm(rows, desc="Evaluating Ratings", leave=False):
            new_rating = _calculate_new_rating(
                tags or (), current_rating, tag_rating_map, args.smax, args.qmax
            )
            if new_rating != current_rating:
                updates.append((image_id, new_rating))

        if updates:
            psycopg2.extras.execute_values(
                pg_cur,
                """
                UPDATE images AS i
                SET rating = v.new_rating
                FROM (VALUES %s) AS v(id, new_rating)
                WHERE i.id = v.id
                """,
                updates,
                page_size=10000
            )
            pg_conn.commit()

        updated = len(updates)
        print(f"[✓] Updated {updated} image rating{'s' if updated != 1 else ''}.\n")


# ==========================================
# Tool 3: Purge Blacklisted Images
# ==========================================
def purge_images(args):
    """Purges images containing blacklisted tags from DB and disk."""
    blacklist_path = Path(args.blacklist)
    if not blacklist_path.is_file():
        print(f"[ERROR] Blacklist not found: {args.blacklist}")
        return

    rules = []
    prefilter_tags = set()
    preserve_tags = set()
    ignore_tags = set()

    for line in blacklist_path.read_text(encoding="utf-8").splitlines():
        raw_line = line.strip().lower()

        # 1. Catch the whitelist directive BEFORE stripping comments
        if raw_line.replace(" ", "").startswith("#//whitelist:"):
            whitelist_content = raw_line.split("whitelist:", 1)[1]
            clean_whitelist = whitelist_content.split("#//")[0]
            for pt in clean_whitelist.split(","):
                clean_pt = pt.strip()
                if clean_pt:
                    preserve_tags.add(clean_pt)
            continue

        # 2. Catch the ignore directive
        if raw_line.replace(" ", "").startswith("#//ignore:"):
            ignore_content = raw_line.split("ignore:", 1)[1]
            clean_ignore = ignore_content.split("#//")[0]
            for pt in clean_ignore.split(","):
                clean_pt = pt.strip()
                if clean_pt:
                    ignore_tags.add(clean_pt)
            continue

        # 3. Safely strip normal comments and grab what's left
        clean_line = raw_line.split("#//")[0].strip()
        if not clean_line:
            continue

        pos_tags = []
        neg_tags = []
        for token in clean_line.split():
            if token.startswith("-"):
                neg_tags.append(token[1:])
            else:
                pos_tags.append(token)
                prefilter_tags.add(token)

        if not pos_tags:
            print(f"[ERROR] Invalid rule '{clean_line}'. You must include at least one positive tag.")
            return

        rules.append({"pos": pos_tags, "neg": neg_tags, "raw": clean_line})

    print(f"[INFO] Loaded {len(rules)} rules and {len(preserve_tags)} global preservation tags. Hunting...")

    db_config = get_shimmie_db_credentials(args.spath)
    if not db_config:
        print(f"[ERROR] Could not load DB credentials from {args.spath}")
        return

    shimmie_root = Path(args.spath)

    with psycopg2.connect(**db_config) as conn:
        cur = conn.cursor()

        # Fetch all tag IDs and counts once so we can pick the rarest tag per rule
        cur.execute("SELECT tag, id, count FROM tags")
        tag_counts = defaultdict(int)
        tag_to_ids = defaultdict(list)
        for t_name, t_id, t_count in cur.fetchall():
            safe_count = t_count or 0
            tag_counts[t_name] += safe_count
            tag_to_ids[t_name].append(t_id)
            if not t_name.startswith(("tagai:", "booru:")):
                base_name = t_name
                if ":" in t_name:
                    base_name = t_name.split(":", 1)[1]
                    tag_counts[base_name] += safe_count
                    tag_to_ids[base_name].append(t_id)
                if "_(" in base_name and base_name.endswith(")"):
                    root_name = base_name.split("_(", 1)[0]
                    tag_counts[root_name] += safe_count
                    tag_to_ids[root_name].append(t_id)

        # Group active rules by their single rarest positive tag
        rules_by_anchor = defaultdict(list)
        anchor_tag_ids = set()
        for rule in rules:
            if any(p not in tag_to_ids for p in rule["pos"]):
                continue
            anchor = min(rule["pos"], key=lambda p: tag_counts[p])
            rules_by_anchor[anchor].append((set(rule["pos"]), set(rule["neg"]), rule["raw"]))
            anchor_tag_ids.update(tag_to_ids[anchor])

        if not anchor_tag_ids:
            print("[✓] Database is clean. No images found matching the blacklist.")
            return

        # Step 1: Fetch ONLY suspects that contain at least one rule's rarest anchor tag_id
        cur.execute("""
            WITH suspect_images AS (
                SELECT DISTINCT it.image_id
                FROM image_tags it
                WHERE it.tag_id = ANY(%s)
            )
            SELECT i.id, i.hash, string_agg(t.tag, ' ') as all_tags
            FROM suspect_images s
            JOIN images i ON s.image_id = i.id
            JOIN image_tags it ON i.id = it.image_id
            JOIN tags t ON it.tag_id = t.id
            GROUP BY i.id, i.hash
        """, (list(anchor_tag_ids),))

        suspects = cur.fetchall()

        # Step 2: Evaluate suspects ONLY against rules whose anchor tag is on the image
        trash_images = []
        breakdown_counts = {r["raw"]: 0 for r in rules}

        for img_id, hsh, tag_string in tqdm.tqdm(suspects, desc="Evaluating Suspects", leave=False):
            raw_tags = tag_string.split(" ")

            clean_tags = set(raw_tags)
            for t in raw_tags:
                if not t.startswith(("tagai:", "booru:")):
                    base_t = t
                    if ":" in t:
                        base_t = t.split(":", 1)[1]
                        clean_tags.add(base_t)
                    if "_(" in base_t and base_t.endswith(")"):
                        clean_tags.add(base_t.split("_(", 1)[0])

            # --- The Immunity Shield ---
            if preserve_tags and not preserve_tags.isdisjoint(clean_tags):
                continue

            triggered_rules = []
            for tag in clean_tags:
                if tag in rules_by_anchor:
                    for pos_set, neg_set, raw_rule in rules_by_anchor[tag]:
                        if pos_set.issubset(clean_tags) and neg_set.isdisjoint(clean_tags):
                            triggered_rules.append(raw_rule)

            if triggered_rules:
                for r in triggered_rules:
                    breakdown_counts[r] += 1
                trash_images.append((img_id, hsh, tag_string, triggered_rules))

        sorted_breakdown = sorted(breakdown_counts.items(), key=lambda item: item[1], reverse=True)
        sorted_breakdown = [item for item in sorted_breakdown if item[1] > 0]

        if not trash_images:
            print("[✓] Database is clean. No images found matching the blacklist.")
            return

        print(f"\n[⚠️ WARNING] Found {len(trash_images)} unique images to permanently delete.")
        print("\n=== CASUALTY BREAKDOWN BY RULE ===")
        for rule_text, count in sorted_breakdown[:20]:
            print(f"  - '{rule_text}': {count} images")
        if len(sorted_breakdown) > 20:
            print(f"  ... and {len(sorted_breakdown) - 20} more rules.")
        print("==================================\n")

        # --- Dry Run Handler ---
        if args.dry_run:
            report_path = Path("purge_dry_run.txt")
            ignore_prefixes = ("booru:", "tagai:")

            associated_tags = {}
            for _, _, tag_string, triggered_rules in trash_images:
                for t in tag_string.split():
                    if t.startswith(ignore_prefixes) or t in ignore_tags:
                        continue

                    base_t = t.split(":", 1)[1] if ":" in t else t
                    root_t = base_t.split("_(", 1)[0] if "_(" in base_t and base_t.endswith(")") else base_t

                    if t not in prefilter_tags and base_t not in prefilter_tags and root_t not in prefilter_tags:
                        if t not in associated_tags:
                            associated_tags[t] = {"total": 0, "rules": {}}

                        associated_tags[t]["total"] += 1

                        for rule in triggered_rules:
                            associated_tags[t]["rules"][rule] = associated_tags[t]["rules"].get(rule, 0) + 1

            # 1. Sort by total frequency descending to find the 100 heaviest casualties
            top_by_purge = sorted(associated_tags.items(), key=lambda x: x[1]["total"], reverse=True)[:100]

            # 2. Fetch total database counts for those Top 100
            top_tag_names = [t[0] for t in top_by_purge]
            if top_tag_names:
                cur.execute("SELECT tag, count FROM tags WHERE tag = ANY(%s)", (top_tag_names,))
                db_totals = dict(cur.fetchall())
            else:
                db_totals = {}

            # 3. Calculate remaining counts and build a final sortable list
            final_top_100 = []
            for tag_name, tag_data in top_by_purge:
                purge_count = tag_data["total"]
                total = db_totals.get(tag_name, 0)
                remaining = max(0, total - purge_count)

                top_rule, top_rule_count = max(tag_data["rules"].items(), key=lambda item: item[1])

                final_top_100.append({
                    "tag_name": tag_name,
                    "purge_count": purge_count,
                    "remaining": remaining,
                    "top_rule": top_rule,
                    "top_rule_count": top_rule_count
                })

            # 4. Re-sort the Top 100 strictly by what is remaining (Ascending, so 0 is at the top)
            final_top_100.sort(key=lambda x: (x["remaining"], -x["purge_count"]))

            with open(report_path, "w", encoding="utf-8") as f:
                f.write("=== CASUALTY BREAKDOWN BY RULE ===\n")
                for rule_text, count in sorted_breakdown:
                    f.write(f"  - '{rule_text}': {count} images\n")

                f.write("\n=== TOP 100 MOST COMMON ASSOCIATED TAGS ===\n")
                f.write("(Sorted by lowest remaining count. Shows tags heavily impacted by the crossfire.)\n")
                for item in final_top_100:
                    f.write(
                        f"  - {item['tag_name']}: {item['remaining']} remaining images "
                        f"({item['purge_count']} purging) "
                        f"(Primary Offender: '{item['top_rule']}' with {item['top_rule_count']} hits)\n"
                    )

                f.write("\n=== IMAGES FLAGGED FOR DELETION ===\n")
                for img_id, hsh, tag_string, triggered_rules in trash_images:
                    f.write(f"ID: {img_id} | Hash: {hsh} | Triggered By: [{', '.join(triggered_rules)}] | Tags: {tag_string}\n")

            print(f"[ℹ️ DRY RUN] Safe abort. Wrote full breakdown, collateral tags, and {len(trash_images)} targets to {report_path.resolve()}")
            return

        confirm = input("Type 'YES' to delete files and database records: ")
        if confirm != "YES":
            print("Aborting.")
            return

        deleted_files = 0
        img_ids_to_drop = []

        # 1. Delete physical files off the hard drive
        print("\n[Step 1/3] Deleting physical files from disk...")
        for img_id, hsh, *_ in tqdm.tqdm(trash_images, desc="Files Purged", unit="file"):
            img_ids_to_drop.append(img_id)

            prefix1, prefix2 = hsh[0:2], hsh[2:4]
            img_file = shimmie_root / "data" / "images" / prefix1 / prefix2 / hsh
            thumb_file = shimmie_root / "data" / "thumbs" / prefix1 / prefix2 / hsh

            if img_file.exists():
                img_file.unlink()
                deleted_files += 1
            if thumb_file.exists():
                thumb_file.unlink()

        # 2. Delete from Postgres
        print("\n[Step 2/3] Scrubbing Postgres database...")
        print("  -> Dropping metadata links (image_tags)...")
        cur.execute("DELETE FROM image_tags WHERE image_id = ANY(%s)", (img_ids_to_drop,))
        print("  -> Dropping core image records (images)...")
        cur.execute("DELETE FROM images WHERE id = ANY(%s)", (img_ids_to_drop,))

        # 3. Recalculate tag counts and drop orphaned tags (count <= 0)
        print("\n[Step 3/3] Fixing Shimmie tag UI counts...")
        print("  -> Recalculating tag usage counts (this may take a moment)...")
        cur.execute("""
            UPDATE tags
            SET count = (SELECT COUNT(image_id) FROM image_tags WHERE tag_id = tags.id)
        """)
        print("  -> Pruning orphaned tags with 0 count...")
        cur.execute("DELETE FROM tags WHERE count <= 0")

        conn.commit()
        print(f"\n[✓] Purge Complete: Destroyed {deleted_files} files and {len(img_ids_to_drop)} database entries.")


# ==========================================
# Tool 4: Retroactive Database Tag Curator (Helper)
# ==========================================
def _collect_trigger_tag_ids(tag_id_map, merged_map):
    """Finds all tag IDs that could trigger a curation change on an image."""
    trigger_ids = set()
    prefixes = ("artist:", "character:", "series:", "source:")

    for tag_name, tag_id in tag_id_map.items():
        if (
            tag_name in merged_map
            or tag_name.endswith("_(cosplay)")
            or resolve_prefixed_tag(tag_name) is not None
        ):
            trigger_ids.add(tag_id)
            continue
        if tag_name.startswith(prefixes):
            base_tag = tag_name.split(":", 1)[1]
            if base_tag in tag_id_map:
                trigger_ids.add(tag_id_map[base_tag])

    return list(trigger_ids)


def _collect_tagai_overlap_pairs(tag_id_map, merged_map):
    """Finds (tagai_id, real_tag_id) pairs where both exist in the tags table."""
    pairs = set()
    prefixes = ("", "character:", "artist:", "series:")

    for tag_name, ai_id in tag_id_map.items():
        if not tag_name.startswith("tagai:"):
            continue
        ai_base = tag_name.split(":", 1)[1]
        ai_mapped = merged_map.get(ai_base, ai_base)
        for base in {ai_base, ai_mapped}:
            for p in prefixes:
                real_id = tag_id_map.get(f"{p}{base}")
                if real_id is not None:
                    pairs.add((ai_id, real_id))

    return list(pairs)


def curate_existing_tags(args):
    """Applies hardcoded and dynamic curation rules to images already in Postgres."""
    merged_map = dict(MASTER_MERGE_LIST)
    if getattr(args, "use_map_csv", None):
        dynamic_mappings = load_dynamic_mappings(args.use_map_csv)
        merged_map.update(dynamic_mappings)
        print(f"[INFO] Loaded {len(dynamic_mappings):,} dynamic tag mappings.")

    db_config = get_shimmie_db_credentials(args.spath)
    if not db_config:
        print(f"[ERROR] Could not load DB credentials from {args.spath}")
        return

    with psycopg2.connect(**db_config) as conn:
        cur = conn.cursor()

        print("Fetching global tag map...")
        cur.execute("SELECT tag, id FROM tags")
        tag_id_map = dict(cur.fetchall())

        if getattr(args, "target_tag", None):
            print(f"Fetching image tags from Postgres strictly for batch '{args.target_tag}'...")
            cur.execute("""
                SELECT i.id, i.source, i.rating, array_agg(t.tag)
                FROM images i
                JOIN image_tags it ON i.id = it.image_id
                JOIN tags t ON it.tag_id = t.id
                WHERE i.id IN (
                    SELECT it2.image_id FROM image_tags it2
                    JOIN tags t2 ON it2.tag_id = t2.id
                    WHERE t2.tag = %s
                )
                GROUP BY i.id, i.source, i.rating
            """, (args.target_tag,))
            images_data = cur.fetchall()
        else:
            trigger_tag_ids = _collect_trigger_tag_ids(tag_id_map, merged_map)
            tagai_pairs = _collect_tagai_overlap_pairs(tag_id_map, merged_map)
            if not trigger_tag_ids and not tagai_pairs:
                print("\n[✓] No matching trigger tags or redundant tagai: tags exist. No changes needed.")
                return

            print(
                f"Filtering database for images matching {len(trigger_tag_ids):,} trigger tags "
                f"or {len(tagai_pairs):,} redundant tagai: pairs..."
            )
            cur.execute("CREATE TEMP TABLE tmp_trigger_tags (tag_id INT PRIMARY KEY) ON COMMIT DROP")
            if trigger_tag_ids:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO tmp_trigger_tags (tag_id) VALUES %s",
                    [(tid,) for tid in trigger_tag_ids],
                    page_size=10000
                )
            cur.execute("ANALYZE tmp_trigger_tags")

            cur.execute("CREATE TEMP TABLE tmp_tagai_pairs (ai_id INT, real_id INT) ON COMMIT DROP")
            if tagai_pairs:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO tmp_tagai_pairs (ai_id, real_id) VALUES %s",
                    tagai_pairs,
                    page_size=10000
                )
            cur.execute("CREATE INDEX idx_tmp_tagai_ai ON tmp_tagai_pairs (ai_id)")
            cur.execute("ANALYZE tmp_tagai_pairs")

            cur.execute("""
                WITH candidate_images AS (
                    SELECT it.image_id
                    FROM image_tags it
                    JOIN tmp_trigger_tags tt ON it.tag_id = tt.tag_id
                    UNION
                    SELECT it_ai.image_id
                    FROM tmp_tagai_pairs tp
                    JOIN image_tags it_ai ON it_ai.tag_id = tp.ai_id
                    JOIN image_tags it_real ON it_real.image_id = it_ai.image_id
                                           AND it_real.tag_id = tp.real_id
                )
                SELECT i.id, i.source, i.rating, array_agg(t.tag)
                FROM candidate_images ci
                JOIN images i ON ci.image_id = i.id
                JOIN image_tags it ON i.id = it.image_id
                JOIN tags t ON it.tag_id = t.id
                GROUP BY i.id, i.source, i.rating
            """)
            images_data = cur.fetchall()

        if not images_data:
            print("\n[✓] No candidate images matched for curation.")
            return

        print(f"Evaluating {len(images_data):,} candidate images...")

        tags_to_add = []
        tags_to_remove = []
        source_updates = []
        rating_updates = []
        new_unique_tags = set()
        rating_priority = {"e": 3, "q": 2, "s": 1}
        canonical_rating_tags = {"rating:e", "rating:q", "rating:s"}

        for img_id, current_source, current_rating, raw_tags in tqdm.tqdm(images_data, desc="Curating Tags"):
            current_tags = list(raw_tags)
            curated_tags = list(current_tags)

            apply_tag_curation(curated_tags, merged_map)

            # 1. Promote rating:e/q/s into images.rating ONLY when images.rating is NULL/empty, then strip the tag
            extracted_ratings = [
                t.split(":", 1)[1].lower()
                for t in curated_tags
                if t.lower() in canonical_rating_tags
            ]
            if extracted_ratings:
                if not (current_rating and current_rating.strip()):
                    best_rating = max(extracted_ratings, key=lambda r: rating_priority.get(r, 0))
                    rating_updates.append((img_id, best_rating))
                curated_tags = [t for t in curated_tags if t.lower() not in canonical_rating_tags]

            # 2. Promote the best source:... tag into images.source ONLY when images.source is NULL/empty
            if not (current_source and current_source.strip()):
                source_tags = sorted(
                    {t for t in curated_tags if t.lower().startswith("source:") and len(t) > 7},
                    key=lambda t: (get_source_score(t.split(":", 1)[1]), t)
                )
                if source_tags:
                    best_src_tag = source_tags[0]
                    best_url = best_src_tag.split(":", 1)[1].strip()
                    if best_url:
                        source_updates.append((img_id, best_url))
                        curated_tags = [t for t in curated_tags if t != best_src_tag]

            if "tagme" in current_tags and len(curated_tags) < 15:
                curated_tags.append("tagme")

            curr_set = set(current_tags)
            cur_set = set(curated_tags)

            added = cur_set - curr_set
            removed = curr_set - cur_set

            for t in added:
                tags_to_add.append((img_id, t))
                if t not in tag_id_map:
                    new_unique_tags.add(t)

            for t in removed:
                tags_to_remove.append((img_id, tag_id_map[t]))

        if not tags_to_add and not tags_to_remove and not source_updates and not rating_updates:
            print("\n[✓] All tags are already clean. No changes needed.")
            return

        print(
            f"\n[INFO] Found {len(tags_to_add):,} missing links to add, "
            f"{len(tags_to_remove):,} junk links to drop, "
            f"{len(source_updates):,} empty image sources to populate, and "
            f"{len(rating_updates):,} null image ratings to populate."
        )

        # 1a. Populate empty images.source columns
        if source_updates:
            print("Populating empty image source fields...")
            psycopg2.extras.execute_values(
                cur,
                """
                UPDATE images AS i
                SET source = v.new_source
                FROM (VALUES %s) AS v(id, new_source)
                WHERE i.id = v.id
                  AND (i.source IS NULL OR BTRIM(i.source) = '')
                """,
                source_updates,
                page_size=10000
            )

        # 1b. Populate null/empty images.rating columns
        if rating_updates:
            print("Populating null image rating fields...")
            psycopg2.extras.execute_values(
                cur,
                """
                UPDATE images AS i
                SET rating = v.new_rating
                FROM (VALUES %s) AS v(id, new_rating)
                WHERE i.id = v.id
                  AND (i.rating IS NULL OR BTRIM(i.rating) = '')
                """,
                rating_updates,
                page_size=10000
            )

        # 2. Create missing tags in the master tags table
        if new_unique_tags:
            print(f"Injecting {len(new_unique_tags):,} entirely new tags into the database...")
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO tags (tag, count) VALUES %s ON CONFLICT (tag) DO NOTHING",
                [(t, 0) for t in new_unique_tags],
                page_size=10000
            )
            cur.execute("SELECT tag, id FROM tags WHERE tag = ANY(%s)", (list(new_unique_tags),))
            tag_id_map.update(dict(cur.fetchall()))

        # 3. Bulk Remove Junk Tags
        if tags_to_remove:
            print("Purging old tag links...")
            cur.execute("CREATE TEMP TABLE tmp_del (img_id INT, tag_id INT) ON COMMIT DROP")
            psycopg2.extras.execute_values(cur, "INSERT INTO tmp_del VALUES %s", tags_to_remove, page_size=10000)
            cur.execute("""
                DELETE FROM image_tags
                USING tmp_del
                WHERE image_tags.image_id = tmp_del.img_id AND image_tags.tag_id = tmp_del.tag_id
            """)

        # 4. Bulk Insert Clean Tags
        if tags_to_add:
            print("Applying new tag links...")
            insert_records = [(img_id, tag_id_map[t]) for img_id, t in tags_to_add]
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO image_tags (image_id, tag_id) VALUES %s ON CONFLICT DO NOTHING",
                insert_records,
                page_size=10000
            )

        # 5. Fast Single-Pass Sweep and Sync
        affected_tag_ids = list({tag_id_map[t] for _, t in tags_to_add} | {tid for _, tid in tags_to_remove})
        if affected_tag_ids:
            print(f"Recalculating UI tag counts for {len(affected_tag_ids):,} affected tags...")
            cur.execute("CREATE TEMP TABLE tmp_affected_tags (tag_id INT PRIMARY KEY) ON COMMIT DROP")
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO tmp_affected_tags (tag_id) VALUES %s",
                [(tid,) for tid in affected_tag_ids],
                page_size=10000
            )
            cur.execute("""
                WITH new_counts AS (
                    SELECT tat.tag_id, COUNT(it.image_id) AS cnt
                    FROM tmp_affected_tags tat
                    LEFT JOIN image_tags it ON tat.tag_id = it.tag_id
                    GROUP BY tat.tag_id
                )
                UPDATE tags AS t
                SET count = nc.cnt
                FROM new_counts nc
                WHERE t.id = nc.tag_id
            """)

        print("Dropping orphaned tags...")
        cur.execute("DELETE FROM tags WHERE count <= 0")

        conn.commit()
        print("\n[✓] Retroactive curation complete.")

# ==========================================
# Tool 5: Retroactive Database Mapper
# ==========================================
def generate_db_curation_map(args):
    """Generates a curation CSV using live DB tags, canonical dictionaries, and prefix normalization."""
    db_config = get_shimmie_db_credentials(args.spath)
    if not db_config:
        print(f"[ERROR] Could not load DB credentials from {args.spath}")
        return

    target_tag = args.target_tag
    ref_tag = args.reference_tag
    is_booru_batch = target_tag.lower().startswith("booru:") and ref_tag.lower().startswith("booru:")
    out_csv = Path("botched_map.csv")

    existing_map = {}
    if getattr(args, "use_map_csv", None):
        existing_map = load_dynamic_mappings(args.use_map_csv)
        print(f"[INFO] Loaded {len(existing_map):,} existing mappings to respect.")

    print("[*] Loading canonical dictionaries (tag_groups.json, characters, artists, ratings)...")
    mappings = load_mappings()
    guard = TagCategoryGuard(mappings)
    known_valid_general = set(guard.known_general) | set(mappings.rating.keys())
    if not is_booru_batch:
        known_valid_general.discard(target_tag)

    with psycopg2.connect(**db_config) as conn:
        cur = conn.cursor()

        print(f"[*] Auditing tag distribution across '{target_tag}' vs pure '{ref_tag}'...")
        cur.execute("""
            WITH target_tag_id AS (
                SELECT id FROM tags WHERE tag = %s LIMIT 1
            ),
            ref_tag_id AS (
                SELECT id FROM tags WHERE tag = %s LIMIT 1
            ),
            target_imgs AS MATERIALIZED (
                SELECT image_id FROM image_tags WHERE tag_id = (SELECT id FROM target_tag_id)
            ),
            ref_imgs AS MATERIALIZED (
                SELECT image_id FROM image_tags WHERE tag_id = (SELECT id FROM ref_tag_id)
            ),
            pure_ref_imgs AS MATERIALIZED (
                SELECT r.image_id
                FROM ref_imgs r
                LEFT JOIN target_imgs t ON r.image_id = t.image_id
                WHERE t.image_id IS NULL
            ),
            target_tag_counts AS (
                SELECT it.tag_id, COUNT(*) AS target_cnt
                FROM image_tags it
                JOIN target_imgs ti ON it.image_id = ti.image_id
                GROUP BY it.tag_id
            ),
            pure_ref_tag_counts AS (
                SELECT it.tag_id, COUNT(*) AS pure_ref_cnt
                FROM image_tags it
                JOIN pure_ref_imgs pri ON it.image_id = pri.image_id
                GROUP BY it.tag_id
            )
            SELECT
                t.tag,
                t.count AS global_cnt,
                ttc.target_cnt,
                COALESCE(prtc.pure_ref_cnt, 0) AS pure_ref_cnt
            FROM target_tag_counts ttc
            JOIN tags t ON t.id = ttc.tag_id
            LEFT JOIN pure_ref_tag_counts prtc ON prtc.tag_id = ttc.tag_id
            WHERE t.count > 0;
        """, (target_tag, ref_tag))
        all_target_tag_rows = cur.fetchall()

        # Build live canonical vocabulary so we don't depend on corrupted tag_histories
        established_canonical = set(known_valid_general)
        for t_name, _, _, pure_ref_cnt in all_target_tag_rows:
            if not is_booru_batch and t_name == target_tag:
                continue
            if resolve_prefixed_tag(t_name) is not None:
                continue
            if pure_ref_cnt >= 5 or guard.get_category(t_name) in {"artist", "character", "series"}:
                established_canonical.add(t_name)

        print(f"[*] Locating Rosetta Stones (Images with both '{target_tag}' and '{ref_tag}')...")
        cur.execute("""
            WITH rosetta_images AS (
                SELECT it1.image_id
                FROM image_tags it1
                JOIN tags t1 ON it1.tag_id = t1.id
                JOIN image_tags it2 ON it1.image_id = it2.image_id
                JOIN tags t2 ON it2.tag_id = t2.id
                WHERE t1.tag = %s AND t2.tag = %s
            )
            SELECT
                r.image_id,
                (
                    SELECT th.tags
                    FROM tag_histories th
                    WHERE th.image_id = r.image_id
                      AND %s = ANY(string_to_array(th.tags, ' '))
                      AND NOT (%s = ANY(string_to_array(th.tags, ' ')))
                    ORDER BY th.id DESC LIMIT 1
                ) as pure_ref_history,
                (
                    SELECT string_agg(t.tag, ' ')
                    FROM image_tags it
                    JOIN tags t ON it.tag_id = t.id
                    WHERE it.image_id = r.image_id
                ) as current_tags
            FROM rosetta_images r;
        """, (target_tag, ref_tag, ref_tag, target_tag))

        rows = cur.fetchall()
        print(f"    -> Found {len(rows):,} overlapping images to analyze.")

    print("[*] Translating DB overlap into tags_mining.py frequency format...")
    valid_pairs = 0
    sidecar_counts = Counter()
    canonical_counts = Counter()
    co_occurrences = defaultdict(Counter)
    sidecar_overlap = defaultdict(Counter)
    ignored_meta = ("booru:", "tagai:", "source:", "site:")

    for _, pure_ref_history, current_tags in tqdm.tqdm(rows, desc="Mapping Frequencies"):
        if not current_tags:
            continue

        current_set = {t for t in current_tags.split() if not t.startswith(ignored_meta)}

        if pure_ref_history:
            ref_hist_set = {t for t in pure_ref_history.split() if not t.startswith(ignored_meta)}
            canonical = (ref_hist_set | (current_set & established_canonical))
            if not is_booru_batch:
                canonical.discard(target_tag)
            botched_tags = current_set - canonical
        else:
            # Live fallback independent of corrupted/edited tag_histories
            canonical = {t for t in current_set if t in established_canonical}
            botched_tags = current_set - canonical

        if not canonical or not botched_tags:
            continue

        valid_pairs += 1

        for s_tag in botched_tags:
            sidecar_counts[s_tag] += 1
            for c_tag in canonical:
                co_occurrences[s_tag][c_tag] += 1

        for c_tag in canonical:
            canonical_counts[c_tag] += 1

    print(
        f"    -> Extracted {valid_pairs:,} valid tag diffs "
        f"({len(sidecar_counts):,} unique suspect tags vs {len(canonical_counts):,} canonical tags)."
    )

    freqs = (valid_pairs, sidecar_counts, canonical_counts, co_occurrences, sidecar_overlap)
    global_ctx = _fetch_global_context(set(sidecar_counts.keys()) | set(canonical_counts.keys()), db_config)

    print("[*] Pass 1: Calculating 1:1 synonym equivalencies from Rosetta Stones...")
    calculated = calculate_equivalencies(freqs, global_ctx, guard, thresholds=(5, 0.50), mode="retro")

    mapped_tags = {r["Sidecar_Tag"] for r in calculated} | {r["Canonical_Tag"] for r in calculated}
    print(f"    -> Discovered {len(calculated):,} 1:1 synonym mappings.")

    # =========================================================================
    # Pass 2: Prefix Normalization, Variant Rescue & Known-Junk-Only _DROP_
    # =========================================================================
    print(f"[*] Pass 2: Normalizing foreign prefixes and filtering known junk on '{target_tag}'...")
    drop_results = []
    skipped_protected = 0

    for tag_name, _, target_cnt, pure_ref_cnt in all_target_tag_rows:
        tag_lower = tag_name.lower()

        # 0. Respect existing user map (no 1000-count cap)
        if tag_name in existing_map:
            if tag_name not in mapped_tags:
                target_val = existing_map[tag_name]
                if target_val == "_DROP_":
                    drop_results.append({
                        "Sidecar_Tag": tag_name,
                        "Canonical_Tag": "_DROP_",
                        "Confidence": 1.0,
                        "Sample_Size": target_cnt,
                    })
                else:
                    calculated.append({
                        "Sidecar_Tag": tag_name,
                        "Canonical_Tag": target_val,
                        "Confidence": 1.0,
                        "Sample_Size": target_cnt,
                    })
                mapped_tags.add(tag_name)
            continue

        # 1. If comparing non-booru content tags (e.g., spats vs bike_shorts), map target_tag -> ref_tag
        if tag_name == target_tag and not is_booru_batch and target_tag != ref_tag:
            if tag_name not in mapped_tags:
                calculated.append({
                    "Sidecar_Tag": target_tag,
                    "Canonical_Tag": ref_tag,
                    "Confidence": 1.0,
                    "Sample_Size": target_cnt,
                })
                mapped_tags.add(tag_name)
            continue

        # 2. Skip if already mapped in Pass 1
        if tag_name in mapped_tags:
            continue

        # 3. Resolve structured & foreign prefixed tags (female:, male:, copyright:, parody:, rating:, pixiv_*, *_(artist))
        resolved_prefix_tag = resolve_prefixed_tag(tag_name)
        if resolved_prefix_tag == "_DROP_":
            drop_results.append({
                "Sidecar_Tag": tag_name,
                "Canonical_Tag": "_DROP_",
                "Confidence": 1.0,
                "Sample_Size": target_cnt,
            })
            mapped_tags.add(tag_name)
            continue
        if resolved_prefix_tag:
            # Snap stripped tag to existing_map, MASTER_MERGE_LIST, or tag_groups.json punctuation if applicable
            final_target = existing_map.get(
                resolved_prefix_tag,
                MASTER_MERGE_LIST.get(resolved_prefix_tag, resolved_prefix_tag),
            )
            if ":" not in final_target:
                norm_res = re.sub(r"[_()-]", "", final_target.lower())
                if len(norm_res) >= 3 and norm_res in guard.norm_general_map:
                    final_target = guard.norm_general_map[norm_res]

            if final_target != tag_name:
                calculated.append({
                    "Sidecar_Tag": tag_name,
                    "Canonical_Tag": final_target,
                    "Confidence": 1.0,
                    "Sample_Size": target_cnt,
                })
                mapped_tags.add(tag_name)
            continue

        # 4. Protect core and unknown prefixes
        if ":" in tag_name:
            skipped_protected += 1
            continue

        # 5. Protect known characters, artists, series, tag_groups.json, and tag_rating_dominant.db tags
        if (
            guard.get_category(tag_name) in {"artist", "character", "series"}
            or tag_name in known_valid_general
            or pure_ref_cnt > 0
        ):
            skipped_protected += 1
            continue

        # 6. Rescue punctuation/hyphen variants (e.g., upside_down -> upside-down) via tag_groups.json
        norm_key = re.sub(r"[_()-]", "", tag_lower)
        if len(norm_key) >= 3 and norm_key in guard.norm_general_map:
            canonical_match = guard.norm_general_map[norm_key]
            if canonical_match != tag_name:
                calculated.append({
                    "Sidecar_Tag": tag_name,
                    "Canonical_Tag": canonical_match,
                    "Confidence": 1.0,
                    "Sample_Size": target_cnt,
                })
                mapped_tags.add(tag_name)
                continue

    print(
        f"    -> Total 1:1 mappings & prefix rescues: {len(calculated):,} | "
        f"Known junk flagged for '_DROP_': {len(drop_results):,} "
        f"(protected {skipped_protected:,} valid tags)."
    )

    all_results = calculated + drop_results
    all_results.sort(key=lambda x: (x["Canonical_Tag"] == "_DROP_", -x["Sample_Size"]))

    print(f"[*] Writing {out_csv.name}...")
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["Sidecar_Tag", "Canonical_Tag", "Confidence", "Sample_Size"]
        )
        writer.writeheader()
        writer.writerows(all_results)

    print(
        f"\n[✓] Smart mapping complete! Wrote {len(all_results):,} total rules "
        f"({len(calculated):,} 1:1 equivalencies + {len(drop_results):,} _DROP_ tags)."
    )


# ==========================================
# Tool 6: Low-Frequency Tag Dumper (Helper)
# ==========================================
def _classify_namespace(tag_name):
    """Returns the exact namespace prefix bucket for a tag so all foreign prefixes are visible in .log."""
    if ":" not in tag_name:
        return "general (unprefixed)"
    prefix = tag_name.split(":", 1)[0].lower()
    if re.fullmatch(r"[a-z0-9_-]{1,20}", prefix):
        return f"{prefix}:"
    return "other (prefixed)"


def _parse_comma_list(raw_val):
    """Parses a comma-separated CLI string into a clean lowercase tuple, ignoring spaces around commas."""
    if not raw_val:
        return ()
    return tuple(item.strip().lower() for item in raw_val.split(",") if item.strip())


def _write_tag_dump_log(cur, filtered_rows, dump_meta, log_path):
    """Queries impact metrics for the filtered tag set and writes a diagnostic .log file."""
    min_count, drop_threshold, exclude_prefixes, exclude_substrings, skipped_counts = dump_meta
    print(f"[*] Generating impact report for {log_path.name}...")

    ns_stats = defaultdict(lambda: {
        "dumped_tags": 0, "drop_tags": 0, "review_tags": 0,
        "dumped_links": 0, "drop_links": 0
    })
    top_drop_candidates = []
    dumped_ids = []
    drop_ids = []

    for tag_id, tag_name, count in filtered_rows:
        ns = _classify_namespace(tag_name)
        ns_stats[ns]["dumped_tags"] += 1
        ns_stats[ns]["dumped_links"] += count
        dumped_ids.append(tag_id)

        if count <= drop_threshold:
            ns_stats[ns]["drop_tags"] += 1
            ns_stats[ns]["drop_links"] += count
            drop_ids.append(tag_id)
            if len(top_drop_candidates) < 100:
                top_drop_candidates.append((tag_name, count))
        else:
            ns_stats[ns]["review_tags"] += 1

    # 2. Booru / Batch Tag Breakdown (booru:*) restricted to the filtered dumped_ids
    booru_rows = []
    if dumped_ids:
        cur.execute("""
            WITH target_dumped_tags AS (
                SELECT id, count AS global_tag_count
                FROM tags
                WHERE id = ANY(%s)
                  AND tag NOT LIKE 'booru:%%'
            ),
            booru_tags AS (
                SELECT id, tag, count AS total_images
                FROM tags
                WHERE tag LIKE 'booru:%%' AND count > 0
            ),
            booru_tag_pairs AS (
                SELECT
                    bt.tag AS booru_tag,
                    bt.total_images,
                    dt.id AS dumped_tag_id,
                    dt.global_tag_count,
                    COUNT(it_dumped.image_id) AS booru_occurrences
                FROM booru_tags bt
                JOIN image_tags it_booru ON bt.id = it_booru.tag_id
                JOIN image_tags it_dumped ON it_booru.image_id = it_dumped.image_id
                JOIN target_dumped_tags dt ON it_dumped.tag_id = dt.id
                GROUP BY bt.tag, bt.total_images, dt.id, dt.global_tag_count
            )
            SELECT
                booru_tag,
                total_images,
                COUNT(dumped_tag_id) AS unique_dumped_tags,
                COUNT(dumped_tag_id) FILTER (WHERE global_tag_count <= %s) AS unique_drop_tags,
                COUNT(dumped_tag_id) FILTER (
                    WHERE global_tag_count <= %s AND booru_occurrences = global_tag_count
                ) AS exclusive_drop_tags,
                COALESCE(SUM(booru_occurrences), 0) AS total_dumped_links,
                COALESCE(SUM(booru_occurrences) FILTER (WHERE global_tag_count <= %s), 0) AS total_drop_links
            FROM booru_tag_pairs
            GROUP BY booru_tag, total_images
            ORDER BY unique_drop_tags DESC, total_drop_links DESC;
        """, (dumped_ids, drop_threshold, drop_threshold, drop_threshold))
        booru_rows = cur.fetchall()

    # 3. Top 100 Surviving Global Tags Most Impacted by _DROP_
    global_impact_rows = []
    if drop_ids:
        cur.execute("""
            WITH target_drop_tags AS (
                SELECT id
                FROM tags
                WHERE id = ANY(%s)
            ),
            drop_per_image AS (
                SELECT
                    it.image_id,
                    COUNT(*) AS drop_links
                FROM image_tags it
                JOIN target_drop_tags tdt ON it.tag_id = tdt.id
                GROUP BY it.image_id
            )
            SELECT
                t.tag,
                t.count AS total_images,
                COUNT(dpi.image_id) AS impacted_images,
                SUM(dpi.drop_links) AS total_drop_links
            FROM drop_per_image dpi
            JOIN image_tags it ON dpi.image_id = it.image_id
            JOIN tags t ON it.tag_id = t.id
            WHERE t.count > %s
              AND t.tag NOT LIKE 'tagai:%%'
            GROUP BY t.id, t.tag, t.count
            ORDER BY total_drop_links DESC, impacted_images DESC
            LIMIT 100;
        """, (drop_ids, drop_threshold))
        global_impact_rows = cur.fetchall()

    sorted_ns = sorted(ns_stats.items(), key=lambda x: x[1]["drop_tags"], reverse=True)

    with log_path.open("w", encoding="utf-8") as f:
        f.write("=== TAG DUMP SUMMARY & NAMESPACE BREAKDOWN ===\n")
        f.write(f"Max Dump Threshold (--min): <= {min_count:,} uses\n")
        f.write(f"Auto-_DROP_ Threshold:      <= {drop_threshold:,} uses\n")
        f.write(f"Excluded Prefixes:          {', '.join(exclude_prefixes) if exclude_prefixes else 'None'} "
                f"({skipped_counts[0]:,} tags skipped)\n")
        f.write(f"Excluded Substrings:        {', '.join(exclude_substrings) if exclude_substrings else 'None'} "
                f"({skipped_counts[1]:,} tags skipped)\n")
        f.write(f"Total Dumped Unique Tags:   {len(filtered_rows):,}\n\n")

        for ns, data in sorted_ns:
            f.write(
                f"  - {ns:<22} | "
                f"Total: {data['dumped_tags']:>8,} tags ({data['dumped_links']:>10,} links) | "
                f"_DROP_: {data['drop_tags']:>8,} tags ({data['drop_links']:>10,} links) | "
                f"Review: {data['review_tags']:>7,} tags\n"
            )

        f.write("\n=== BOORU / SOURCE BATCH IMPACT (booru:*) ===\n")
        f.write("(Shows which booru imports contain the dumped/_DROP_ tags after exclusions.)\n")
        if booru_rows:
            for (
                b_tag, total_imgs, u_dumped, u_drop, excl_drop, l_dumped, l_drop
            ) in booru_rows:
                f.write(
                    f"  - {b_tag} ({total_imgs:,} total images):\n"
                    f"      Unique Dumped Tags: {u_dumped:,} ({l_dumped:,} tag links)\n"
                    f"      Unique _DROP_ Tags: {u_drop:,} ({l_drop:,} tag links) "
                    f"[{excl_drop:,} tags are 100% EXCLUSIVE to {b_tag}]\n"
                )
        else:
            f.write("  No matching booru:* tag overlaps found.\n")

        f.write("\n=== TOP 100 GLOBAL TAGS MOST IMPACTED BY _DROP_ ===\n")
        f.write("(Surviving tags whose images will have the highest number of _DROP_ tags stripped.)\n")
        for tag_name, total_imgs, impacted_imgs, drop_links in global_impact_rows:
            pct = (impacted_imgs / total_imgs * 100.0) if total_imgs > 0 else 0.0
            avg_stripped = (drop_links / impacted_imgs) if impacted_imgs > 0 else 0.0
            f.write(
                f"  - {tag_name}: {drop_links:,} _DROP_ links across {impacted_imgs:,}/{total_imgs:,} images "
                f"({pct:.1f}% of tag's images affected, avg {avg_stripped:.1f} dropped/img)\n"
            )

        f.write(f"\n=== TOP 100 HIGHEST-COUNT TAGS ASSIGNED _DROP_ (<= {drop_threshold:,} USES) ===\n")
        f.write("(Sanity check: highest-usage tags that will be deleted if not manually cleared.)\n")
        for tag_name, count in top_drop_candidates:
            f.write(f"  - {tag_name}: {count:,} uses\n")

    print(f"[✓] Wrote diagnostic log to {log_path.resolve()}")


def tag_dump(args):
    """Dumps tags used <= --min times (excluding tagai:* and user exclusions) to a CSV and .log."""
    db_config = get_shimmie_db_credentials(args.spath)
    if not db_config:
        print(f"[ERROR] Could not load DB credentials from {args.spath}")
        return

    min_count = args.min
    dv_ratio = args.dv / 100.0 if args.dv > 1.0 else args.dv
    drop_threshold = int(min_count * dv_ratio)
    exclude_prefixes = _parse_comma_list(getattr(args, "exclude_prefix", ""))
    exclude_substrings = _parse_comma_list(getattr(args, "exclude", ""))

    out_path = Path(args.out)
    log_path = out_path.with_suffix(".log")

    print(f"[*] Querying tags with count <= {min_count:,} (auto-_DROP_ threshold: <= {drop_threshold:,})...")

    with psycopg2.connect(**db_config) as conn:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, tag, count
                FROM tags
                WHERE count > 0
                  AND count <= %s
                  AND tag NOT LIKE 'tagai:%%'
                ORDER BY count DESC, tag ASC
            """, (min_count,))
            raw_rows = cur.fetchall()

            filtered_rows = []
            skipped_prefix = 0
            skipped_substr = 0

            for tag_id, tag_name, count in raw_rows:
                tag_lower = tag_name.lower()
                if exclude_prefixes and tag_lower.startswith(exclude_prefixes):
                    skipped_prefix += 1
                    continue
                if exclude_substrings and any(sub in tag_lower for sub in exclude_substrings):
                    skipped_substr += 1
                    continue
                filtered_rows.append((tag_id, tag_name, count))

            if not filtered_rows:
                print(f"[✓] No tags matched criteria after exclusions (skipped {skipped_prefix:,} by prefix, {skipped_substr:,} by substring).")
                return

            drop_assigned = 0
            manual_review = 0

            with out_path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Sidecar_Tag", "Canonical_Tag"])

                for _, tag_name, count in filtered_rows:
                    if count <= drop_threshold:
                        writer.writerow([tag_name, "_DROP_"])
                        drop_assigned += 1
                    else:
                        writer.writerow([tag_name, ""])
                        manual_review += 1

            print(f"[✓] Dumped {len(filtered_rows):,} tags to {out_path.resolve()}")
            if skipped_prefix or skipped_substr:
                print(f"    -> Excluded {skipped_prefix:,} tags via --exclude-prefix and {skipped_substr:,} via --exclude")
            print(f"    -> {manual_review:,} tags left blank for manual review ({drop_threshold + 1:,} to {min_count:,} uses)")
            print(f"    -> {drop_assigned:,} tags auto-assigned '_DROP_' (<= {drop_threshold:,} uses)")

            dump_meta = (
                min_count,
                drop_threshold,
                exclude_prefixes,
                exclude_substrings,
                (skipped_prefix, skipped_substr),
            )
            _write_tag_dump_log(cur, filtered_rows, dump_meta, log_path)
