"""Updates existing image ratings in Shimmie2 PostgreSQL based on dominant tag weights."""

from collections.abc import Iterable

import psycopg2
import psycopg2.extras
import tqdm

from functions.db_cache import get_shimmie_db_credentials, load_rating_weights
from functions.source_resolver import is_recognized_prefix
from functions.tags_curation import rating_from_score


def _calculate_new_rating(
    tags: Iterable[str | None],
    current_rating: str,
    tag_rating_map: dict[str, int],
    smax: int,
    qmax: int,
) -> str:
    """Calculates a single image's new rating from its non-AI, non-booru tags."""
    total_score = 0
    for tag in tags:
        if not tag or tag.startswith(("booru:", "tagai:")):
            continue

        if is_recognized_prefix(tag):
            continue

        weight = tag_rating_map.get(tag.lower())
        if weight is None:
            continue
        if weight == 1 and total_score == 0:
            total_score = 1
        elif weight > 1:
            total_score += weight

    return rating_from_score(total_score, smax, qmax) if total_score > 0 else current_rating


def update_ratings(args) -> None:
    """Evaluates and updates existing ratings in Shimmie2's PostgreSQL `images` table."""
    tag_rating_map = load_rating_weights(require_file=True)

    pg_config = get_shimmie_db_credentials(args.spath)
    if not pg_config:
        print(f"[ERROR] Could not load DB credentials from {args.spath}")
        return

    with psycopg2.connect(**pg_config) as pg_conn, pg_conn.cursor() as pg_cur:
        print("Fetching images and tags from Postgres...")
        updates: list[tuple[int, str]] = []
        with pg_conn.cursor(name="ratings_cursor", withhold=True) as read_cur:
            read_cur.execute("""
                SELECT i.id, i.rating, array_agg(t.tag)
                FROM images i
                LEFT JOIN image_tags it ON i.id = it.image_id
                LEFT JOIN tags t ON it.tag_id = t.id
                GROUP BY i.id, i.rating
            """)
            while batch := read_cur.fetchmany(5000):
                for image_id, current_rating, tags in tqdm.tqdm(
                    batch, desc="Evaluating Ratings", leave=False
                ):
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
                page_size=10000,
            )
            pg_conn.commit()

        updated = len(updates)
        print(f"[✓] Updated {updated:,} image rating{'s' if updated != 1 else ''}.\n")
