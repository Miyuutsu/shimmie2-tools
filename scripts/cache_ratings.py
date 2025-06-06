import sqlite3
from pathlib import Path

# Function to import tag ratings from the dominant_tag_ratings database
def import_tag_ratings(tag_rating_db_path: Path):
    conn = sqlite3.connect(tag_rating_db_path)
    cur = conn.cursor()

    # Fetch all tag names and their ratings
    cur.execute("SELECT tag_name, dominant_rating FROM dominant_tag_ratings")
    tag_ratings = {row[0]: row[1] for row in cur.fetchall()}
    conn.close()

    return tag_ratings

# Function to apply the dominant rating based on the tags
def apply_dominant_ratings_to_posts(db_path: Path, tag_ratings: dict):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Fetch posts with rating NULL, blank, or "?"
    cur.execute("SELECT md5, general, character, artist, series, rating FROM posts WHERE rating IS NULL OR rating = '' OR rating = ' ' OR rating = '?'")
    posts = cur.fetchall()

    print(f"[INFO] Found {len(posts)} posts to update...")

    for post in posts:
        md5, general_tags, character_tags, artist_tags, series_tags, current_rating = post

        # Combine all tags (only if no rating is already set)
        tags = general_tags.split(",") + character_tags.split(",") + artist_tags.split(",") + series_tags.split(",")

        dominant_rating = None
        for tag in tags:
            if tag in tag_ratings:
                tag_rating = tag_ratings[tag]
                if tag_rating == "e":
                    dominant_rating = "e"
                    break  # Once we find an 'e', no need to continue

                elif tag_rating == "q" and dominant_rating != "e":
                    dominant_rating = "q"

                elif tag_rating == "s" and dominant_rating not in ["e", "q"]:
                    dominant_rating = "s"

        # If no dominant rating was found, default to '?' instead of 's'
        if dominant_rating is None:
            dominant_rating = "?"

        # If the current rating is not already set to a valid value, apply the new rating
        if current_rating in [None, "", " ", "?"]:
            # Update the post with the applied rating
            cur.execute("""
            UPDATE posts
            SET rating = ?
            WHERE md5 = ?
            """, (dominant_rating, md5))
            print(f"[INFO] Applied rating '{dominant_rating}' to post with MD5: {md5}")

    # Commit changes to the database
    conn.commit()
    conn.close()
    print("[✓] Ratings applied to posts successfully.")

if __name__ == "__main__":
    # Set the paths to your SQLite databases
    posts_cache_db_path = Path("posts_cache.db")
    dominant_tag_ratings_db_path = Path("tag_rating_dominant.db")

    # Import tag ratings from the dominant_tag_ratings database
    tag_ratings = import_tag_ratings(dominant_tag_ratings_db_path)

    # Apply dominant ratings to the posts in the posts_cache.db
    apply_dominant_ratings_to_posts(posts_cache_db_path, tag_ratings)
