"""Builds and chronologically orders per-artist Shimmie2 Pools from database/timeline_index.db."""

import sqlite3
from collections import defaultdict
from dataclasses import dataclass

import psycopg2
import psycopg2.extras
import tqdm

from functions.common import TIMELINE_DB_PATH
from functions.db_cache import get_shimmie_db_credentials
from tools.importer import _resolve_user_id


@dataclass(slots=True)
class PoolEntry:
    """Single image entry with its chronological sort key."""

    image_id: int
    published_ts: float
    platform: str
    post_id_num: int
    page_index: int


def run_sync_pools(args) -> None:
    """Main CLI entry point for sync-pools."""
    if not TIMELINE_DB_PATH.is_file():
        print(f"[ERROR] Timeline DB not found: {TIMELINE_DB_PATH}. Run prep-paid first.")
        return

    db_config = get_shimmie_db_credentials(args.spath)
    if not db_config:
        print(f"[ERROR] Could not load DB credentials from {args.spath}")
        return

    artist_filter = (getattr(args, "artist", "") or "").strip().lower()
    with sqlite3.connect(TIMELINE_DB_PATH) as sconn:
        if artist_filter:
            rows = sconn.execute(
                """
                SELECT md5, artist, platform, post_id_num, published_ts, page_index
                FROM timeline_items
                WHERE LOWER(artist) = ?
                """,
                (artist_filter,),
            ).fetchall()
        else:
            rows = sconn.execute(
                """
                SELECT md5, artist, platform, post_id_num, published_ts, page_index
                FROM timeline_items
                """
            ).fetchall()

    if not rows:
        print("[!] No matching timeline entries found in timeline_index.db.")
        return

    by_md5 = {r[0].lower(): (r[1], r[2], int(r[3]), float(r[4]), int(r[5])) for r in rows}
    md5_list = list(by_md5.keys())
    effective_user = getattr(args, "user", None) or str(db_config.get("user") or "")

    with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
        user_id = _resolve_user_id(cur, effective_user)

        # Fetch matching imported images and their current live artist:* tags
        matched_ids: dict[str, int] = {}
        for i in tqdm.tqdm(
            range(0, len(md5_list), 10000),
            desc="Matching MD5s in Shimmie2",
            leave=False,
        ):
            chunk = md5_list[i : i + 10000]
            cur.execute("SELECT hash, id FROM images WHERE hash = ANY(%s)", (chunk,))
            for hsh, img_id in cur.fetchall():
                matched_ids[hsh.strip().lower()] = int(img_id)

        if not matched_ids:
            print("[!] None of the indexed MD5s exist in Shimmie2 yet.")
            return

        # 2. Fetch ONLY artist:* tags for matched image IDs
        artist_tags_by_img: dict[int, set[str]] = defaultdict(set)
        id_list = list(matched_ids.values())
        for i in tqdm.tqdm(
            range(0, len(id_list), 10000),
            desc="Loading artist tags",
            leave=False,
        ):
            chunk = id_list[i : i + 10000]
            cur.execute(
                """
                WITH artist_tags AS MATERIALIZED (
                    SELECT id, tag
                    FROM tags
                    WHERE tag LIKE 'artist:%%' AND tag != 'artist:tagme'
                )
                SELECT it.image_id, at.tag
                FROM image_tags it
                JOIN artist_tags at ON at.id = it.tag_id
                WHERE it.image_id = ANY(%s)
                """,
                (chunk,),
            )
            for img_id, tag in cur.fetchall():
                artist_tags_by_img[int(img_id)].add(tag.split(":", 1)[1])

        matched_images: dict[str, tuple[int, list[str]]] = {
            hsh: (img_id, sorted(artist_tags_by_img.get(img_id, ())))
            for hsh, img_id in matched_ids.items()
        }

        if not matched_images:
            print("[!] None of the indexed MD5s exist in Shimmie2 yet.")
            return

        # Group into one pool per artist (preferring the live curated artist:* tag when unique)
        pools_by_artist: dict[str, list[PoolEntry]] = defaultdict(list)
        for hsh, (img_id, live_artists) in matched_images.items():
            idx_artist, platform, post_id_num, pub_ts, page_idx = by_md5[hsh]
            canonical_artist = live_artists[0] if len(live_artists) == 1 else idx_artist
            if artist_filter and canonical_artist.lower() != artist_filter:
                continue
            pools_by_artist[canonical_artist].append(
                PoolEntry(
                    image_id=img_id,
                    published_ts=pub_ts,
                    platform=platform,
                    post_id_num=post_id_num,
                    page_index=page_idx,
                )
            )

        title_fmt = args.title_template
        reverse_order = bool(getattr(args, "desc", False))
        is_public = bool(getattr(args, "public", False))

        print(
            f"[INFO] Matched {len(matched_images):,}/{len(by_md5):,} indexed files "
            f"across {len(pools_by_artist):,} artist pool(s)."
        )

        for artist, entries in sorted(pools_by_artist.items()):
            entries.sort(
                key=lambda e: (e.published_ts, e.platform, e.post_id_num, e.page_index, e.image_id),
                reverse=reverse_order,
            )
            # Deduplicate by image_id preserving sort order
            ordered_ids = list(dict.fromkeys(e.image_id for e in entries))
            title = title_fmt.format(artist=artist)[:255]

            if args.dry_run:
                print(f"  [DRY RUN] Pool '{title}': {len(ordered_ids):,} posts")
                continue

            cur.execute(
                """
                INSERT INTO pools (user_id, public, title, description, posts)
                VALUES (%s, %s, %s, %s, 0)
                ON CONFLICT (title) DO UPDATE SET
                    lastupdated = CURRENT_TIMESTAMP
                RETURNING id
                """,
                (user_id, is_public, title, f"Chronological archive for artist:{artist}"),
            )
            pool_id = int(cur.fetchone()[0])

            # Preserve any manually added images in the pool that aren't in timeline_index.db
            cur.execute(
                "SELECT image_id FROM pool_images WHERE pool_id = %s ORDER BY image_order ASC",
                (pool_id,),
            )
            existing_ids = [int(r[0]) for r in cur.fetchall()]
            indexed_set = set(ordered_ids)
            manual_tail = [iid for iid in existing_ids if iid not in indexed_set]
            final_ids = ordered_ids + list(dict.fromkeys(manual_tail))

            cur.execute("DELETE FROM pool_images WHERE pool_id = %s", (pool_id,))
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO pool_images (pool_id, image_id, image_order) VALUES %s",
                [(pool_id, iid, pos) for pos, iid in enumerate(final_ids, start=1)],
                page_size=5000,
            )
            cur.execute(
                """
                UPDATE pools
                SET posts = %s, lastupdated = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (len(final_ids), pool_id),
            )
            print(f"  [✓] Synced pool '{title}' (id={pool_id}): {len(final_ids):,} posts")

        if not args.dry_run:
            conn.commit()
            print("\n[✓] Pool synchronization complete.")
