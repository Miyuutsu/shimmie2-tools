import argparse
import psycopg2
import re
from pathlib import Path

def connect_db(db_config):

    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    return conn, cursor

def unique_names(names):
    # Return unique names while maintaining the order
    seen = set()
    unique_list = []
    for name in names:
        if name not in seen:
            seen.add(name)
            unique_list.append(name)
    return unique_list


def sort_artists(args):
    conn, cursor = connect_db(DB_CONFIG)

    try:
        # Fetch artist tags from the database
        cursor.execute("SELECT tag FROM public.tags WHERE tag ILIKE 'artist:%' ORDER BY tag ASC;")
        artist_tags = cursor.fetchall()

        # Create a mapping from artist names to their full tags
        artist_dict = {tag[0][7:]: tag[0] for tag in artist_tags}

        # Compile a regex pattern to match any artist name
        pattern = re.compile(r'\[\[([^\(\)]+?)\]\]')

        # Read the content of the text file
        textfile = Path(args.output)
        with textfile.open("r", encoding="utf-8") as file:
            content = file.read()

        # Replace artist names with their corresponding tags
        def replace_artist(match):
            name = match.group(1).strip()  # Get the inner text and remove any extra whitespace
            # If the plain name exists in our artist mapping, replace it with the full tag
            if name in artist_dict:
                return f"[[{artist_dict[name]}]]"
            # Otherwise, return the original text unchanged
            return match.group(0)

        # Replace artist names in the content
        sorted_content = pattern.sub(replace_artist, content)

        # Split the content into lines
        lines = sorted_content.splitlines()

        # Separate artist lines and other lines
        artist_lines = [line for line in lines if line.startswith('[[artist:')]
        other_lines = [line for line in lines if not line.startswith('[[artist:')]

        # Get unique names while maintaining order for other lines
        unique_other_lines = unique_names(other_lines)

        unique_artist_lines = unique_names(artist_lines)

        # Prepare the final content
        final_artists = "== Artists ==\n\\n" + "\n".join(unique_artist_lines)

        # Write the modified content to a new file
#        output_path = textfile.with_name(textfile.stem + "_sorted.txt")
#        with output_path.open("w", encoding="utf-8") as file:
#            file.write(final_content)

#        print(f"Sorted artist tags have been written to {output_path}.")

    except Exception as e:
        print(f"An error occurred during sorting: {e}")

    finally:
        cursor.close()
        conn.close()

    return final_artists

def sort_characters(args):
    conn, cursor = connect_db(DB_CONFIG)

    try:
        # Fetch character tags from the database
        cursor.execute("SELECT tag FROM public.tags WHERE tag ILIKE 'character:%' ORDER BY tag ASC;")
        character_tags = cursor.fetchall()

        # Create a mapping from character names to their full tags
        character_dict = {tag[0][10:]: tag[0] for tag in character_tags}

        # Compile a regex pattern to match any character name
        pattern = re.compile(r'\[\[([^\(\)]+?)\]\]')

        # Read the content of the text file
        textfile = Path(args.output)
        with textfile.open("r", encoding="utf-8") as file:
            content = file.read()

        # Replace character names with their corresponding tags
        def replace_character(match):
            name = match.group(1).strip()  # Get the inner text and remove any extra whitespace
            # If the plain name exists in our character mapping, replace it with the full tag
            if name in character_dict:
                return f"[[{character_dict[name]}]]"
            # Otherwise, return the original text unchanged
            return match.group(0)

        # Replace character names in the content
        sorted_content = pattern.sub(replace_character, content)

        # Split the content into lines
        lines = sorted_content.splitlines()

        # Separate character lines and other lines
        character_lines = [line for line in lines if line.startswith('[[character:')]
        other_lines = [line for line in lines if not line.startswith('[[character:')]

        # Get unique names while maintaining order for other lines
        unique_other_lines = unique_names(other_lines)

        unique_character_lines = unique_names(character_lines)

        # Prepare the final content
        final_characters = "== Characters ==\n\n" + "\n".join(unique_character_lines)

        # Write the modified content to a new file
#        output_path = textfile.with_name(textfile.stem + "_sorted.txt")
#        with output_path.open("w", encoding="utf-8") as file:
#            file.write(final_content)

#        print(f"Sorted character tags have been written to {output_path}.")

    except Exception as e:
        print(f"An error occurred during sorting: {e}")

    finally:
        cursor.close()
        conn.close()

    return final_characters

def sort_series(args):
    conn, cursor = connect_db(DB_CONFIG)

    try:
        # Fetch series tags from the database
        cursor.execute("SELECT tag FROM public.tags WHERE tag ILIKE 'series:%' ORDER BY tag ASC;")
        series_tags = cursor.fetchall()

        # Create a mapping from series names to their full tags
        series_dict = {tag[0][7:]: tag[0] for tag in series_tags}

        # Compile a regex pattern to match any series name
        pattern = re.compile(r'\[\[([^\(\)]+?)\]\]')

        # Read the content of the text file
        textfile = Path(args.output)
        with textfile.open("r", encoding="utf-8") as file:
            content = file.read()

        # Replace series names with their corresponding tags
        def replace_series(match):
            name = match.group(1).strip()  # Get the inner text and remove any extra whitespace
            # If the plain name exists in our series mapping, replace it with the full tag
            if name in series_dict:
                return f"[[{series_dict[name]}]]"
            # Otherwise, return the original text unchanged
            return match.group(0)

        # Replace series names in the content
        sorted_content = pattern.sub(replace_series, content)

        # Split the content into lines
        lines = sorted_content.splitlines()

        # Separate series lines and other lines
        series_lines = [line for line in lines if line.startswith('[[series:')]
        other_lines = [line for line in lines if not line.startswith('[[series:')]

        # Get unique names while maintaining order for other lines
        unique_other_lines = unique_names(other_lines)

        unique_series_lines = unique_names(series_lines)

        # Prepare the final content
        final_series = "== Series ==\n" + "\n".join(unique_series_lines)

        # Write the modified content to a new file
#        output_path = textfile.with_name(textfile.stem + "_sorted.txt")
#        with output_path.open("w", encoding="utf-8") as file:
#            file.write(final_content)

#        print(f"Sorted series tags have been written to {output_path}.")

    except Exception as e:
        print(f"An error occurred during sorting: {e}")

    finally:
        cursor.close()
        conn.close()

    return final_series

def sort_general(args):
    conn, cursor = connect_db(DB_CONFIG)

    try:
        # Fetch tags that do not contain 'artist:', 'character:', or 'series:'
        cursor.execute("SELECT tag FROM public.tags WHERE tag NOT ILIKE 'artist:%' AND tag NOT ILIKE 'character:%' AND tag NOT ILIKE 'series:%' ORDER BY tag ASC;")
        general_tags = cursor.fetchall()

        general_dict = [tag[0] for tag in general_tags]

        # Read the content of the text file
        textfile = Path(args.output)
        with textfile.open("r", encoding="utf-8") as file:
            content = file.read()

        # Extract tags from the text file
        pattern = re.compile(r'$$\[([^\[$$]+?)\]\]')  # Corrected regex pattern
        file_tags = pattern.findall(content)
        file_tags = [tag.strip() for tag in file_tags]  # Clean up whitespace

        # Combine and deduplicate tags from both sources
        combined_tags = set(general_dict + file_tags)  # Use a set to remove duplicates

        # Format tags back to [[tag]] format
        formatted_tags = [f'[[{tag}]]' for tag in combined_tags]

        # Sort the tags alphabetically
        sorted_tags = sorted(formatted_tags)

        # Prepare the final content with the header
        final_general = "== General ==\n" + "\n".join(sorted_tags)

#        # Write the modified content to a new file
#        output_path = textfile.with_name(textfile.stem + "_sorted.txt")
#        with output_path.open("w", encoding="utf-8") as file:
#            file.write(final_content)

#        print(f"Sorted general tags have been written to {output_path}.")

    except Exception as e:
        print(f"An error occurred during sorting: {e}")

    finally:
        cursor.close()
        conn.close()
    return final_general

def main(args):

    conn = None

    # Connect to the PostgreSQL database
    if not Path(args.output).exists():
        try:
            # Connect to db
            conn, cursor = connect_db(DB_CONFIG)
            # Query to fetch all wiki page titles
            cursor.execute("SELECT title FROM wiki_pages ORDER BY title ASC")
            wiki_urls = cursor.fetchall()

            # Initialize an array to store the formatted wiki links
            wiki_links = []

            # Process each title and format it
            for page in wiki_urls:
                formatted_title = page[0].replace(' ', '_')  # Replace spaces with underscores
                wiki_links.append(f"[[{formatted_title}]]")

            # Write the output to a .txt file
            with open(args.output, 'w', encoding="utf-8") as f:
                f.write("\n".join(wiki_links))

            print(f"Wiki index has been successfully created and saved to {args.output}.")

        except psycopg2.Error as err:
            print(f"Error: {err}")

        finally:
            if conn is not None:
                cursor.close()
                conn.close()

    if Path(args.output).exists() and args.sort:
        print(f"Now sorting the tags from {args.output}.")
        all_artists = sort_artists(args)
        all_characters = sort_characters(args)
        all_general = sort_general(args)
        all_series = sort_series(args)

        # Create a dictionary to map section names to their content
        sections = {
            "c": all_characters,
            "s": all_series,
            "a": all_artists,
            "g": all_general
        }

        order = args.order.split(",") if args.order else []

        # Properly join the outputs with newlines
        final_output = "\n\n".join(sections[section] for section in order if section in sections)

        # Write the modified content to a new file
        output_path = Path(args.output).with_name(Path(args.output).stem + "_sorted.txt")
        with output_path.open("w", encoding="utf-8") as file:
            file.write(final_output)

        print(f"Sorted tags have been written to {output_path}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create a wiki index for shimmie2")
    parser.add_argument("--user", type=str, default="miyuu", help="(Default: miyuu) PostgreSQL user")
    parser.add_argument("--db", type=str, default="shimmiedb", help="(Default: shimmiedb) PostgreSQL database name")
    parser.add_argument("--output", type=str, default="wiki_index.txt", help=" (Default: wiki_index.txt) Where to save the resulting .txt file")
    parser.add_argument("--sort", dest="sort", action="store_true", help="Enable sorting of artist tags. (Default: False)")
    parser.add_argument("--order", type=str, default="c,s,a,g", help="Comma-separated order of Characters, Series, Artists and General to output when using --sort. (Default: c,s,a,g)")
    args = parser.parse_args()

    # Set DB_CONFIG after parsing
    DB_CONFIG = {
        "dbname": args.db,
        "user": args.user,
        "host": "localhost",
        "port": 5432
    }

    # Call the main function
    main(args)
