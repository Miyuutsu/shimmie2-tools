"""Master CLI Tool for Shimmie2 Batch Importing and Database Management."""

import argparse
import os
import sys

from tools import (
	ai_auditor,
	auto_tagger,
	csv2sqlite,
	csv_builder,
	curate_db,
	fix_map,
	generate_db_map,
	purge,
	tag_dump,
	update_ratings,
)


def _add_audit_ratings_parser(subparsers) -> None:
	"""Adds the audit-ratings command."""
	parser = subparsers.add_parser(
		'audit-ratings',
		help='Audit images using AI Tagger submodule to inject shadow tags',
	)
	parser.add_argument('--spath', required=True, help='Path to Shimmie root')
	parser.add_argument('--thumbs', help='Path to thumbnails (Defaults to spath/data/thumbs)')
	parser.add_argument(
		'--model',
		choices=['vit', 'vit-large', 'swinv2', 'convnext', 'eva02'],
		default='eva02',
		help='SmilingWolf model to use',
	)
	parser.add_argument('--batch', type=int, default=20, help='Batch size for AI inference')
	parser.add_argument('--gen-threshold', type=float, default=0.35, help='General tag confidence threshold')
	parser.add_argument('--char-threshold', type=float, default=0.75, help='Character tag confidence threshold')


def _add_auto_tag_parser(subparsers) -> None:
	"""Adds the auto-tag command."""
	parser = subparsers.add_parser('auto-tag', help='Auto-generate missing sidecars using AI')
	parser.add_argument('--images', help='Path to target images directory')
	parser.add_argument('--videos', help='Path to target videos/GIFs directory')
	parser.add_argument('--spath', help='Path to Shimmie root (for Postgres DB checking)')
	parser.add_argument(
		'--dir-as-artist',
		action='store_true',
		help='Use parent directory name as an artist:<name> tag',
	)
	parser.add_argument(
		'--model',
		choices=['vit', 'vit-large', 'swinv2', 'convnext', 'eva02'],
		default='eva02',
		help='SmilingWolf model to use',
	)
	parser.add_argument('--batch', type=int, default=10, help='Batch size for AI inference')
	parser.add_argument('--gen-threshold', type=float, default=0.35, help='General tag threshold')
	parser.add_argument('--char-threshold', type=float, default=0.75, help='Character tag threshold')


def _add_csv_parser(subparsers):
	"""Adds the make-csv command."""
	parser = subparsers.add_parser('make-csv', help='Create a CSV for Shimmie2 import')
	parser.add_argument('--batch', type=int, default=20, help='Batch size')
	parser.add_argument('--create-map', dest='create_map_csv', help='Mine tags and write to a .db or .csv map')
	parser.add_argument('--images', dest='image_path', help='Path to images directory')
	parser.add_argument('--prefix', default='import', help='Dir name inside Shimmie')
	parser.add_argument('--pretags', type=str, default='', help='Comma-separated tags to prepend to all posts')
	parser.add_argument('--qmax', type=int, default=250, help='Max questionable rating')
	parser.add_argument('--skip-existing', action='store_true', help='Check Shimmie DB for existing images')
	parser.add_argument('--smax', type=int, default=50, help='Max safe rating')
	parser.add_argument('--spath', help='Path to Shimmie root')
	parser.add_argument(
		'--threads',
		type=int,
		default=max(1, (os.cpu_count() or 1) // 2),
		help='Thread count',
	)
	parser.add_argument('--thumbnail', action='store_true', help='Generate thumbnails')
	parser.add_argument('--update-cache', action='store_true', help='Update posts_cache.db')
	parser.add_argument('--use-map', dest='use_map_csv', help='Override tagmap path (Defaults to database/tagmap.db)')
	parser.add_argument('--videos', dest='video_path', help='Path to videos directory')
	parser.add_argument('--blacklist', default='blacklist.txt', help='Text file containing blacklist rules')
	parser.add_argument('--chunk-size', type=int, default=500, help='Split CSV into chunks (Default: 500 lines)')
	return parser


def _add_csv2sqlite_parser(subparsers) -> None:
	"""Adds the csv2sqlite command."""
	parser = subparsers.add_parser('csv2sqlite', help='Convert CSV to SQLite')
	parser.add_argument('--csv', required=True, help='Path to input CSV')
	parser.add_argument('--db', required=True, help='Path to output SQLite Database')
	parser.add_argument('--drop_table', action='store_true', help='Drop table if exists')
	parser.add_argument('--table', default='data', help='Table name (use "tagmap" for tagmap.db schema)')


def _add_curate_db_parser(subparsers) -> None:
	"""Adds the curate-db command."""
	parser = subparsers.add_parser('curate-db', help='Retroactively apply curation rules to existing images in Shimmie2')
	parser.add_argument('--spath', required=True, help='Path to your Shimmie2 web root')
	parser.add_argument('--use-map', dest='use_map_csv', help='Override tagmap path (Defaults to database/tagmap.db)')
	parser.add_argument('--target-tag', help='Only curate images containing this specific tag')


def _add_fix_map_parser(subparsers) -> None:
	"""Adds the fix-map command."""
	parser = subparsers.add_parser(
		'fix-map',
		help='Audit and repair tagmap.db or a CSV map, and optionally revert poisoned DB tags',
	)
	parser.add_argument(
		'--csv',
		'--map',
		dest='csv',
		help='Path to input .db or .csv map (Defaults to database/tagmap.db)',
	)
	parser.add_argument('-o', '--out', help='Output .db or .csv path (Defaults to overwriting input after .bak backup)')
	parser.add_argument('--spath', help='Optional path to Shimmie2 root to cross-check live DB tags and revert poisoned tags')
	parser.add_argument(
		'--revert',
		nargs='+',
		metavar='TAG',
		help='Revert one or more poisoned tags in Postgres using tag_histories (e.g., --revert artist:mizumizuni=eyes)',
	)


def _add_generate_map_parser(subparsers) -> None:
	"""Adds the generate-db-map command."""
	parser = subparsers.add_parser(
		'generate-db-map',
		help='Generate tag mappings by diffing a target batch tag against pure DB vocabulary',
	)
	parser.add_argument('--spath', required=True, help='Path to Shimmie root')
	parser.add_argument('--target-tag', required=True, help='The tag identifying the target batch (e.g., booru:bad_import)')
	parser.add_argument('--reference-tag', required=True, help='The pure reference tag (e.g., booru:gelbooru)')
	parser.add_argument('--use-map', dest='use_map_csv', help='Override existing tagmap path (Defaults to database/tagmap.db)')
	parser.add_argument('-o', '--out', default='botched_map.csv', help='Output .csv or .db path (Default: botched_map.csv)')


def _add_purge_parser(subparsers) -> None:
	"""Adds the purge command."""
	parser = subparsers.add_parser('purge', help='Permanently delete images based on blacklisted tags')
	parser.add_argument('--spath', required=True, help='Path to your Shimmie2 web root')
	parser.add_argument('--blacklist', default='blacklist.txt', help='Path to your blacklist text file')
	parser.add_argument('--dry-run', action='store_true', help='Generate a report without deleting anything')


def _add_tag_dump_parser(subparsers) -> None:
	"""Adds the tag-dump command."""
	parser = subparsers.add_parser('tag-dump', help='Dump low-frequency DB tags to a CSV or tagmap.db')
	parser.add_argument('--spath', required=True, help='Path to your Shimmie2 web root')
	parser.add_argument('--min', type=int, default=5000, help='Dump tags used this many times or fewer (Default: 5000)')
	parser.add_argument(
		'--dv',
		type=float,
		default=0.20,
		help='Ratio of --min at or below which tags are auto-assigned _DROP_ (Default: 0.20)',
	)
	parser.add_argument(
		'--exclude-prefix',
		default='',
		help="Comma-separated prefixes to exclude from the start of a tag (e.g., 'artist:, character:, series:')",
	)
	parser.add_argument(
		'--exclude',
		default='',
		help='Comma-separated substrings to exclude if matched anywhere in a tag',
	)
	parser.add_argument('-o', '--out', default='tag_dump.csv', help='Output .csv or .db path (Default: tag_dump.csv)')


def _add_update_ratings_parser(subparsers) -> None:
	"""Adds the update-ratings command."""
	parser = subparsers.add_parser('update-ratings', help='Update image ratings in Shimmie')
	parser.add_argument('--spath', required=True, help='Path to Shimmie root')
	parser.add_argument('-q', '--qmax', type=int, default=250, help='Max questionable rating')
	parser.add_argument('-s', '--smax', type=int, default=50, help='Max safe rating')


def setup_parser():
	"""Constructs the master argument parser."""
	parser = argparse.ArgumentParser(
		description='=== Shimmie2 Master Toolsuite ===\nA collection of tools to manage data.',
		formatter_class=argparse.RawTextHelpFormatter,
	)
	parser.add_argument('--help-all', action='store_true', help='Show full help for all commands and exit')

	subparsers = parser.add_subparsers(dest='command', title='Available Commands', metavar='')

	parser_csv = _add_csv_parser(subparsers)
	_add_audit_ratings_parser(subparsers)
	_add_auto_tag_parser(subparsers)
	_add_csv2sqlite_parser(subparsers)
	_add_curate_db_parser(subparsers)
	_add_fix_map_parser(subparsers)
	_add_generate_map_parser(subparsers)
	_add_purge_parser(subparsers)
	_add_tag_dump_parser(subparsers)
	_add_update_ratings_parser(subparsers)

	return parser, parser_csv, subparsers


def _handle_make_csv(args, parser_csv) -> None:
	"""Validates make-csv arguments and launches csv_builder."""
	if not args.image_path and not args.video_path:
		parser_csv.error('You must provide at least one input path: --images or --videos')
	if args.skip_existing and not args.spath:
		parser_csv.error('--spath is required when --skip-existing is set.')

	args.pretags = [t.strip().lower() for t in args.pretags.split(',') if t.strip()] if args.pretags else []
	csv_builder.run(args)


def main() -> None:
	"""Main CLI router."""
	parser, parser_csv, subparsers = setup_parser()

	if '--help-all' in sys.argv:
		parser.print_help()
		print('\n' + '=' * 60)
		for name, subparser in subparsers.choices.items():
			print(f'\n[Command: {name}]\n' + '-' * 30)
			subparser.print_help()
		sys.exit(0)

	args = parser.parse_args()
	if not args.command:
		parser.print_help()
		sys.exit(1)

	dispatch = {
		'audit-ratings': ai_auditor.run_auditor,
		'auto-tag': auto_tagger.run_auto_tagger,
		'csv2sqlite': csv2sqlite.csv_to_sqlite,
		'curate-db': curate_db.curate_existing_tags,
		'fix-map': fix_map.fix_curation_map,
		'generate-db-map': generate_db_map.generate_db_curation_map,
		'purge': purge.purge_images,
		'tag-dump': tag_dump.tag_dump,
		'update-ratings': update_ratings.update_ratings,
	}

	if args.command == 'make-csv':
		_handle_make_csv(args, parser_csv)
	else:
		dispatch[args.command](args)


if __name__ == '__main__':
	main()
