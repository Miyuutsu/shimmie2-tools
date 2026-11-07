# pylint: disable=line-too-long
"""Master CLI Tool for Shimmie2 Batch Importing and Database Management."""
import argparse
import os
import sys

from tools import ai_auditor, auto_tagger, csv_builder, db

def _add_audit_ratings_parser(subparsers):
    """Adds the audit-ratings command."""
    parser = subparsers.add_parser("audit-ratings", help="Audit images using AI Tagger submodule to inject shadow tags")

    parser.add_argument("--spath", required=True, help="Path to Shimmie root")
    parser.add_argument("--thumbs", help="Path to thumbnails (Defaults to spath/data/thumbs)")

    # Model and Thresholds
    parser.add_argument("--model", choices=["vit", "vit-large", "swinv2", "convnext", "eva02"], default="eva02", help="SmilingWolf model to use")
    parser.add_argument("--batch", type=int, default=20, help="Batch size for the AI inference")
    parser.add_argument("--gen-threshold", type=float, default=0.35, help="General tag confidence threshold")
    parser.add_argument("--char-threshold", type=float, default=0.75, help="Character tag confidence threshold")

def _add_auto_tag_parser(subparsers):
    """Adds the auto-tag command."""
    parser = subparsers.add_parser("auto-tag", help="Auto-generate missing sidecars using AI")
    parser.add_argument("--images", help="Path to target images directory")
    parser.add_argument("--videos", help="Path to target videos/GIFs directory")
    parser.add_argument("--spath", help="Path to Shimmie root (for Postgres DB checking)")
    parser.add_argument("--dir-as-artist", action="store_true", help="Use parent directory name as an un-prefixed artist tag")

    # Model and Thresholds
    parser.add_argument("--model", choices=["vit", "vit-large", "swinv2", "convnext", "eva02"], default="eva02", help="SmilingWolf model to use")
    parser.add_argument("--batch", type=int, default=10, help="Batch size for AI inference")
    parser.add_argument("--gen-threshold", type=float, default=0.35, help="General tag threshold")
    parser.add_argument("--char-threshold", type=float, default=0.75, help="Character tag threshold")

def _add_csv_parser(subparsers):
    """Adds the make-csv command."""
    parser = subparsers.add_parser(
        "make-csv", help="Create a CSV for Shimmie2 import"
    )
    parser.add_argument("--batch", type=int, default=20, help="Batch size")
    parser.add_argument(
        "--create-map",
        dest="create_map_csv",
        help="Mine tags and create a CSV map"
    )
    parser.add_argument("--images", dest="image_path", help="Path to images directory")
    parser.add_argument("--prefix", default="import", help="Dir name inside Shimmie")
    parser.add_argument("--pretags", type=str, default="", help="Tags to prepend to all posts")
    parser.add_argument("--qmax", type=int, default=250, help="Max questionable rating.")
    parser.add_argument("--skip-existing", action="store_true", help="Check Shimmie for image")
    parser.add_argument("--smax", type=int, default=50, help="Max safe rating.")
    parser.add_argument("--spath", help="Path to shimmie root")
    parser.add_argument(
        "--threads",
        type=int,
        default=max(1, (os.cpu_count() or 1) // 2),
        help="Thread count"
    )
    parser.add_argument("--thumbnail", action="store_true", help="Generate thumbnails")
    parser.add_argument("--update-cache", action="store_true", help="Flag to update the cache")
    parser.add_argument("--use-map", dest="use_map_csv", help="Load an existing CSV map")
    parser.add_argument("--videos", dest="video_path", help="Path to videos directory")
    parser.add_argument("--blacklist", default="blacklist.txt", help="Text file containing tags to drop (one per line)")
    parser.add_argument("--chunk-size", type=int, default=500, help="Split csv into chunks (Default: 500 lines)")

def _add_csv2sqlite_parser(subparsers):
    """Adds the csv2sqlite command."""
    parser = subparsers.add_parser("csv2sqlite", help="Convert CSV to SQLite")
    parser.add_argument("--csv", required=True, help="Path to CSV")
    parser.add_argument("--db", required=True, help="Path to output SQLite Database")
    parser.add_argument("--drop_table", action="store_true", help="Drop table if exists")
    parser.add_argument("--table", default="data", help="Table name")

def _add_curate_db_parser(subparsers):
    """Adds the curate-db command."""
    parser = subparsers.add_parser("curate-db", help="Retroactively apply curation rules to existing images in Shimmie2")
    parser.add_argument("--spath", required=True, help="Path to your Shimmie2 web root")
    parser.add_argument("--use-map", dest="use_map_csv", help="Load an existing CSV map for dynamic curation")
    parser.add_argument("--target-tag", help="Only curate images containing this specific tag")

def _add_generate_map_parser(subparsers):
    """Adds the generate-db-map command."""
    parser = subparsers.add_parser("generate-db-map", help="Generate a curation CSV map by diffing a botched batch against pure DB vocabulary")
    parser.add_argument("--spath", required=True, help="Path to Shimmie root")
    parser.add_argument("--target-tag", required=True, help="The tag identifying the botched batch (e.g., booru:bad_import)")
    parser.add_argument("--reference-tag", required=True, help="The pure historical tag (e.g., booru:gelbooru)")

def _add_purge_parser(subparsers):
    parser = subparsers.add_parser("purge", help="Permanently delete images based on blacklisted tags")
    parser.add_argument("--spath", required=True, help="Path to your Shimmie2 web root")
    parser.add_argument("--blacklist", default="blacklist.txt", help="Path to your blacklist text file")
    parser.add_argument("--dry-run", action="store_true", help="Generate a report without deleting anything")

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
    _add_audit_ratings_parser(subparsers)
    _add_auto_tag_parser(subparsers)
    _add_csv2sqlite_parser(subparsers)
    _add_curate_db_parser(subparsers)
    _add_generate_map_parser(subparsers)
    _add_purge_parser(subparsers)
    _add_update_ratings_parser(subparsers)

    return parser, parser_csv, subparsers

def _handle_make_csv(args, parser_csv):
    """Input validation wrapper for make-csv."""
    if not args.image_path and not args.video_path:
        parser_csv.error("You must provide at least one input path: --images or --videos")
    if args.skip_existing and not args.spath:
        parser_csv.error("--spath is required when --skip-existing is set.")

    if args.pretags:
        args.pretags = [t.strip().lower() for t in args.pretags.split(",") if t.strip()]
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
        "audit-ratings": ai_auditor.run_auditor,
        "auto-tag": auto_tagger.run_auto_tagger,
        "csv2sqlite": db.csv_to_sqlite,
        "curate-db": db.curate_existing_tags,
        "generate-db-map": db.generate_db_curation_map,
        "purge": db.purge_images,
        "update-ratings": db.update_ratings,
    }

    if args.command == "make-csv":
        _handle_make_csv(args, parser_csv)
    else:
        dispatch[args.command](args)

if __name__ == "__main__":
    main()
