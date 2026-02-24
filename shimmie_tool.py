"""Master CLI Tool for Shimmie2 Batch Importing and Database Management."""
import argparse
import sys

from tools import csv_builder
from tools import db, wiki

from functions.common import get_cpu_threads

def _add_csv_parser(subparsers):
    """Adds the make-csv command."""
    parser = subparsers.add_parser("make-csv", help="Create a CSV for Shimmie2 import")
    parser.add_argument("--batch", type=int, default=20, help="Batch size")
    parser.add_argument(
        "--create-map", dest="create_map_csv", help="Mine tags and create a CSV map"
    )
    parser.add_argument("--images", dest="image_path", help="Path to images directory")
    parser.add_argument("--prefix", default="import", help="Dir name inside Shimmie")
    parser.add_argument("--pretags", type=str, default="", help="Tags to prepend to all posts")
    parser.add_argument("--qmax", default=250, help="Max questionable rating.")
    parser.add_argument("--skip-existing", action="store_true", help="Check Shimmie for image")
    parser.add_argument("--smax", default=50, help="Max safe rating.")
    parser.add_argument("--spath", help="Path to shimmie root")
    parser.add_argument(
        "--threads", type=int, default=get_cpu_threads() // 2, help="Thread count"
    )
    parser.add_argument("--thumbnail", action="store_true", help="Generate thumbnails")
    parser.add_argument("--update-cache", action="store_true", help="Flag to update the cache")
    parser.add_argument("--use-map", dest="use_map_csv", help="Load an existing CSV map")
    parser.add_argument("--videos", dest="video_path", help="Path to videos directory")
    return parser

def _add_wiki_index_parser(subparsers):
    """Adds the wiki-index command."""
    parser = subparsers.add_parser("wiki-index", help="Create static HTML wiki site")
    parser.add_argument("--spath", help="Path to shimmie root (Optional for offline mode)")
    parser.add_argument("--output", type=str, default="wiki_html", help="Output directory path")
    parser.add_argument("--sort", action="store_true", help="Enable sorting of tags in index")
    parser.add_argument(
        "--order", type=str, default="c,s,a,g", help="Sort order (Default: c,s,a,g)"
    )

def _add_import_wikis_parser(subparsers):
    """Adds the import-wikis command."""
    parser = subparsers.add_parser("import-wikis", help="Import Danbooru wikis")
    parser.add_argument("--spath", help="Path to shimmie root (Optional for cache-only mode)")
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument("--pages", type=int, default=200)
    parser.add_argument("--update-existing", action="store_true")
    parser.add_argument(
        "--convert", choices=["raw", "markdown", "html", "shimmie"], default="shimmie"
    )
    parser.add_argument("--update-cache", action="store_true")
    parser.add_argument("--clear-cache", action="store_true")
    parser.add_argument("--captcha", action="store_true", help="Enable Anti-Bot/PoW solver")
    # Added endpoint argument
    parser.add_argument(
        "--endpoint",
        default="wiki_pages.json",
        help="Comma-separated endpoints (e.g. wiki_pages.json,pools.json)"
    )

def _add_csv2sqlite_parser(subparsers):
    """Adds the csv2sqlite command."""
    parser = subparsers.add_parser("csv2sqlite", help="Convert CSV to SQLite")
    parser.add_argument("--csv", required=True, help="Path to CSV")
    parser.add_argument("--db", required=True, help="Path to output SQLite Database")
    parser.add_argument("--drop_table", action="store_true", help="Drop table if exists")
    parser.add_argument("--table", default="data", help="Table name")

def _add_precache_parser(subparsers):
    """Adds the precache command."""
    parser = subparsers.add_parser("precache", help="Pre-cache Danbooru posts.json to SQLite")
    parser.add_argument("posts_json", nargs="?", default="input/posts.json", help="Path to JSON")
    parser.add_argument("-o", "--output", default="database/posts_cache.db", help="Output DB")
    parser.add_argument("--threads", type=int, default=8, help="Number of threads")

def _add_update_ratings_parser(subparsers):
    """Adds the update-ratings command."""
    parser = subparsers.add_parser("update-ratings", help="Update image ratings in Shimmie")
    parser.add_argument("--spath", required=True, help="Path to shimmie root")
    parser.add_argument("-q", "--qmax", type=int, default=250, help="Max questionable rating")
    parser.add_argument("-s", "--smax", type=int, default=50, help="Max safe rating")

def setup_parser():
    """Constructs the argument parser."""
    parser = argparse.ArgumentParser(
        description="=== Shimmie2 Master Toolsuite ===\nA collection of tools to manage data.",
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "--help-all", action="store_true", help="Show full help for all commands and exit"
    )

    subparsers = parser.add_subparsers(
        dest="command",
        title="Available Commands",
        metavar=""
    )

    parser_csv = _add_csv_parser(subparsers)
    _add_wiki_index_parser(subparsers)
    _add_import_wikis_parser(subparsers)
    _add_csv2sqlite_parser(subparsers)
    _add_precache_parser(subparsers)
    _add_update_ratings_parser(subparsers)

    return parser, parser_csv, subparsers

def _handle_make_csv(args, parser_csv):
    """Input validation wrapper for make-csv."""
    if not args.image_path and not args.video_path:
        parser_csv.error("You must provide at least one input path: --images or --videos")
    if args.skip_existing and not args.spath:
        parser_csv.error("--spath is required when --skip-existing is set.")

    if args.pretags:
        args.pretags = [t.strip() for t in args.pretags.split(",") if t.strip()]
    else:
        args.pretags = []

    csv_builder.run(args)

def main():
    """Main CLI router."""
    parser, parser_csv, subparsers = setup_parser()

    if "--help-all" in sys.argv:
        parser.print_help()
        print("\n" + "="*60)
        for name, subparser in subparsers.choices.items():
            print(f"\n[Command: {name}]\n" + "-"*30)
            subparser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    dispatch = {
        "wiki-index": wiki.create_index,
        "import-wikis": wiki.import_danbooru,
        "csv2sqlite": db.csv_to_sqlite,
        "precache": db.precache_posts,
        "update-ratings": db.update_ratings,
    }

    if args.command == "make-csv":
        _handle_make_csv(args, parser_csv)
    else:
        dispatch[args.command](args)

if __name__ == "__main__":
    main()
