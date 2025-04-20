import psycopg2
import re

def fix_bracket_tags(text):
    def replace_spaces(match):
        inner = match.group(1)
        return f"[[{inner.replace(' ', '_')}]]"

    return re.sub(r'\[\[([^\[\]]+?)\]\]', replace_spaces, text)

def connect_db(db_config):

    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    return conn, cursor

def main():

    conn = None

    DB_CONFIG = {
        "dbname": "shimmiedb",
        "user": "miyuu",
        "host": "localhost",
        "port": 5432
    }

    try:
        # Connect to db
        conn, cursor = connect_db(DB_CONFIG)
        # Query to fetch all wiki page titles
        cursor.execute("SELECT id, body FROM wiki_pages WHERE body LIKE '%%[[%% %%]]%%';")
        wiki_pages = cursor.fetchall()
        for page_id, body in wiki_pages:
            fixed_body = fix_bracket_tags(body)
            if fixed_body != body:
                cursor.execute("UPDATE wiki_pages SET body = %s WHERE id = %s;", (fixed_body, page_id))

        # Commit the changes to the database
        conn.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
