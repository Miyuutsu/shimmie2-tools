'''For updating existing ratings in shimmiedb'''
import sqlite3
from pathlib import Path
import argparse
import psycopg2

def main(args):
    '''The main script'''
    script_dir = Path(__file__).parent.resolve()
    db_dir = script_dir / ".." / "database"
    db_path = db_dir / "tag_rating_dominant.db"

    db_config = {
        "dbname": args.db,
        "user": args.user,
        "host": "localhost",
        "port": 5432
    }
    rating_priority = {'e': 5, 'q': 4, 's': 3, 'g': 2, '?': 1}
    tag_rating_map = {}
    with sqlite3.connect(db_path) as tag_db_conn:
        tag_db_cursor = tag_db_conn.cursor()
        tag_db_cursor.execute("SELECT * FROM dominant_tag_ratings")
        rows = tag_db_cursor.fetchall()
        for row in rows:
            if len(row) >= 2:
                tag_rating_map[row[0].strip()] = row[1].strip()

    with psycopg2.connect(**db_config) as pg_conn:
        pg_cursor = pg_conn.cursor()

        # Get all image IDs
        pg_cursor.execute("SELECT id FROM images")
        image_ids = [row[0] for row in pg_cursor.fetchall()]

        updated = 0
        for image_id in image_ids:
            # Get tags for this image
            pg_cursor.execute("""
                SELECT t.tag
                FROM tags t
                JOIN image_tags it ON t.id = it.tag_id
                WHERE it.image_id = %s
            """, (image_id,))
            tags = [t[0] for t in pg_cursor.fetchall()]

            # Determine dominant rating
            rating_letter = '?'
            for tag in tags:
                db_rating = tag_rating_map.get(tag)
                if not db_rating:
                    continue
                if rating_priority[db_rating] > rating_priority[rating_letter]:
                    rating_letter = db_rating
                    if rating_letter == 'e':  # no higher possible
                        break

            # Update the image rating if needed
            pg_cursor.execute("SELECT rating FROM images WHERE id = %s", (image_id,))
            current_rating = pg_cursor.fetchone()[0]
            if current_rating != rating_letter:
                pg_cursor.execute(
                    "UPDATE images SET rating = %s WHERE id = %s",
                    (rating_letter, image_id)
                )
                updated += 1

        print(f"Updated {updated} image ratings.")
        pg_conn.commit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Auto correct database ratings")
    parser.add_argument("--user", type=str, default="miyuu",
                        help="(Default: miyuu) PostgreSQL user")
    parser.add_argument("--db", type=str, default="shimmiedb",
                        help="(Default: shimmiedb) PostgreSQL database name")

    main(parser.parse_args())
