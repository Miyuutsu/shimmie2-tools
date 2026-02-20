'''For updating existing ratings in shimmiedb'''
import sqlite3
from pathlib import Path
import argparse
import psycopg2

from functions.utils import rating_from_score

script_dir = Path(__file__).parent.resolve()
db_path = script_dir.parent / "database" / "tag_rating_dominant.db"
tag_rating_map = {}

def main(args):
    '''For updating existing ratings in shimmiedb'''

    with sqlite3.connect(db_path) as conn:
        tag_rating_map.update(
            {t.strip(): int(r) for t, r in conn.execute(
                "SELECT tag_name, dominant_rating FROM dominant_tag_ratings"
            )}
        )

    pg_config = {
        "dbname": args.db,
        "user": args.user,
        "host": "localhost",
        "port": 5432
    }

    with psycopg2.connect(**pg_config) as pg_conn:
        pg_cur = pg_conn.cursor()

        # Get all image IDs
        pg_cur.execute("SELECT id FROM images")
        ids = [row[0] for row in pg_cur.fetchall()]

        updated = 0

        for i, image_id in enumerate(ids, start=1):
            print(f"Processing image {i}/{len(ids)}", end="\r")

            # Get tags for this image
            pg_cur.execute("""
                SELECT t.tag
                FROM tags t
                JOIN image_tags it ON t.id = it.tag_id
                WHERE it.image_id = %s
            """, (image_id,))
            tags = [t[0] for t in pg_cur.fetchall()]

            total_score = 0

            for tag in tags:
                weight = tag_rating_map.get(tag)
                if weight is None:
                    continue
                if weight == 1:
                    if total_score == 0:
                        total_score = 1
                elif weight > 1:
                    total_score += weight

            rating_letter = None
            if total_score > 0:
                rating_letter = rating_from_score(
                    total_score, args.smax, args.qmax
                )

            pg_cur.execute(
                "SELECT rating FROM images WHERE id = %s", (image_id,)
            )
            current_rating = pg_cur.fetchone()[0]

            if rating_letter is None:
                rating_letter = current_rating if current_rating is not None else "?"

            # Update the image rating if needed
            if current_rating != rating_letter:
                pg_cur.execute(
                    "UPDATE images SET rating = %s WHERE id = %s",
                    (rating_letter, image_id)
                )
                updated += 1

        print(" " * 60, end="\r")
        print(f"Updated {updated} image rating{'s' if updated != 1 else ''}.")
        print()

        pg_conn.commit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Auto‑correct image ratings in shimmiedb using the numeric weight system"
    )
    parser.add_argument(
        "--db",
        type=str,
        default="shimmiedb",
        help="PostgreSQL database name (default: shimmiedb)",
    )
    parser.add_argument(
        "-q", "--qmax",
        type=int,
        default=250,
        help="Max questionable rating.",
    )
    parser.add_argument(
        "-s", "--smax",
        type=int,
        default=50,
        help="Max safe rating.",
    )
    parser.add_argument(
        "-u", "--user",
        type=str,
        required=True,
        help="PostgreSQL user (default: None)",
    )


    parsed_args = parser.parse_args()
    if parsed_args.user is None:
        parsed_args.user = input("dbuser: ").strip()

    main(parsed_args)
