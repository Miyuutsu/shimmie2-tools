#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK
"""Master CLI Tool for Shimmie2 Batch Importing and Database Management."""

import argparse
import os
from pathlib import Path
import re
import subprocess
import sys

RESET = '\033[0m'
BOLD = '\033[1m'
DIM = '\033[2m'
GREEN = '\033[1;32m'
YELLOW = '\033[1;33m'
BLUE = '\033[1;34m'
MAGENTA = '\033[35m'
CYAN = '\033[1;36m'

USE_COLOR = sys.stdout.isatty() and not os.environ.get('NO_COLOR')


def _c(text: str, color: str) -> str:
	"""Wraps text in an ANSI color sequence when connected to a TTY."""
	return f'{color}{text}{RESET}' if USE_COLOR else text


class ColorHelpFormatter(argparse.RawTextHelpFormatter):
	"""Wide-aligned help formatter with post-layout ANSI syntax highlighting."""

	def __init__(self, prog: str) -> None:
		super().__init__(prog, max_help_position=52, width=130)

	def format_help(self) -> str:
		raw = super().format_help()
		if not USE_COLOR:
			return raw

		# 1. Banner & section headers
		raw = raw.replace(
			'=== Shimmie2 Master Toolsuite ===',
			_c('=== Shimmie2 Master Toolsuite ===', CYAN),
		)
		raw = re.sub(
			r'^(usage:|options:|Available Commands:)',
			lambda m: _c(m.group(1), YELLOW),
			raw,
			flags=re.MULTILINE,
		)

		# 2. Subcommand list entries (4-space indent under Available Commands)
		raw = re.sub(
			r'^(\s{4})([a-z0-9-]+)(\s{2,})',
			lambda m: f'{m.group(1)}{_c(m.group(2), GREEN)}{m.group(3)}',
			raw,
			flags=re.MULTILINE,
		)

		# 3. Option flags (-h, --spath, etc.) and their metavars/choices
		def _color_flag_line(match: re.Match[str]) -> str:
			indent, flags_part, gap = match.groups()
			colored_flags = re.sub(
				r'(?:^|(?<=[\s,]))(--?[a-zA-Z0-9_-]+)',
				lambda fm: _c(fm.group(1), CYAN),
				flags_part,
			)
			colored_flags = re.sub(
				r'(\{[a-z0-9_,-]+\}|\b[A-Z][A-Z0-9_]*\b(?:\s\[[A-Z0-9_.\s]+\])?)',
				lambda mm: _c(mm.group(1), MAGENTA),
				colored_flags,
			)
			return f'{indent}{colored_flags}{gap}'

		raw = re.sub(
			r'^(\s{2})(-[^\n]+?)(\s{2,}|\n)',
			_color_flag_line,
			raw,
			flags=re.MULTILINE,
		)

		# 4. Highlight (Default: ...) and (Defaults to ...) notes
		return re.sub(
			r'(\(Defaults?[^)]+\))',
			lambda m: _c(m.group(1), YELLOW),
			raw,
		)


def _add_audit_ratings_parser(subparsers) -> None:
	"""Adds the audit-ratings command."""
	parser = subparsers.add_parser(
		'audit-ratings',
		help='Audit images using AI Tagger submodule to inject shadow tags',
		formatter_class=ColorHelpFormatter,
	)
	parser.add_argument('--spath', required=True, help='Path to Shimmie root')
	parser.add_argument('--thumbs', help='Path to thumbnails (Defaults to spath/data/thumbs)')
	parser.add_argument(
		'--model',
		choices=['vit', 'vit-large', 'swinv2', 'convnext', 'eva02'],
		default='eva02',
		help='SmilingWolf model to use (Default: eva02)',
	)
	parser.add_argument('--batch', type=int, default=20, help='Batch size for AI inference (Default: 20)')
	parser.add_argument('--gen-threshold', type=float, default=0.35, help='General tag confidence threshold (Default: 0.35)')
	parser.add_argument('--char-threshold', type=float, default=0.75, help='Character tag confidence threshold (Default: 0.75)')


def _add_auto_tag_parser(subparsers) -> None:
	"""Adds the auto-tag command."""
	parser = subparsers.add_parser(
		'auto-tag',
		help='Auto-generate missing sidecars using AI',
		formatter_class=ColorHelpFormatter,
	)
	parser.add_argument('--path', help='Path to target directory (scans both images and videos)')
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
		help='SmilingWolf model to use (Default: eva02)',
	)
	parser.add_argument('--batch', type=int, default=10, help='Batch size for AI inference (Default: 10)')
	parser.add_argument('--gen-threshold', type=float, default=0.35, help='General tag threshold (Default: 0.35)')
	parser.add_argument('--char-threshold', type=float, default=0.75, help='Character tag threshold (Default: 0.75)')


def _add_csv_parser(subparsers):
	"""Adds the make-csv command."""
	parser = subparsers.add_parser(
		'make-csv',
		help='Create a CSV for Shimmie2 import (or import directly with --direct-import)',
		formatter_class=ColorHelpFormatter,
	)
	parser.add_argument('--path', help='Path to media directory (scans both images and videos)')
	parser.add_argument('--images', dest='image_path', help='Path to images directory')
	parser.add_argument('--videos', dest='video_path', help='Path to videos directory')
	parser.add_argument('--batch', type=int, default=20, help='Batch size (Default: 20)')
	parser.add_argument('--create-map', dest='create_map_csv', help='Mine tags and write to a .db or .csv map')
	parser.add_argument('--prefix', default='import', help='Dir name inside Shimmie (Default: import)')
	parser.add_argument('--pretag', '--pretags', dest='pretags', type=str, default='', help='Comma-separated tags to prepend to all posts')
	parser.add_argument('--qmax', type=int, default=250, help='Max questionable rating (Default: 250)')
	parser.add_argument('--smax', type=int, default=50, help='Max safe rating (Default: 50)')
	parser.add_argument('--skip-existing', action='store_true', help='Skip existing Shimmie MD5s instead of merging tags')
	parser.add_argument('--direct-import', action='store_true', help='Import directly into Shimmie2 Postgres & data/ without CSVs')
	parser.add_argument('--user', default=None, help='Shimmie2 username for --direct-import (Defaults to config user)')
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
	parser.add_argument('--blacklist', default='blacklist.txt', help='Blacklist file (Default: blacklist.txt)')
	parser.add_argument('--chunk-size', type=int, default=500, help='Split CSV into chunks (Default: 500 lines)')
	return parser


def _add_import_csv_parser(subparsers) -> None:
	"""Adds the import-csv command."""
	parser = subparsers.add_parser(
		'import-csv',
		help='Natively import Shimmie2 CSV file(s) and merge tags onto existing MD5s',
		formatter_class=ColorHelpFormatter,
	)
	parser.add_argument('--csv', required=True, help='Path to an import.csv file or directory containing import*.csv files')
	parser.add_argument('--spath', required=True, help='Path to Shimmie2 root')
	parser.add_argument('--user', default=None, help='Shimmie2 username for post ownership/history (Defaults to config user)')
	parser.add_argument('--skip-existing', action='store_true', help='Skip existing MD5s completely instead of merging tags')
	parser.add_argument(
		'--threads',
		type=int,
		default=max(1, (os.cpu_count() or 1) // 2),
		help='Thread count for file copying and thumbnailing',
	)


def _add_csv2sqlite_parser(subparsers) -> None:
	"""Adds the csv2sqlite command."""
	parser = subparsers.add_parser(
		'csv2sqlite',
		help='Convert CSV to SQLite',
		formatter_class=ColorHelpFormatter,
	)
	parser.add_argument('--csv', required=True, help='Path to input CSV')
	parser.add_argument('--db', required=True, help='Path to output SQLite Database')
	parser.add_argument('--drop_table', action='store_true', help='Drop table if exists')
	parser.add_argument('--table', default='data', help='Table name (Default: data, or "tagmap" for tagmap.db)')


def _add_curate_db_parser(subparsers) -> None:
	"""Adds the curate-db command."""
	parser = subparsers.add_parser(
		'curate-db',
		help='Retroactively apply curation rules to existing images in Shimmie2',
		formatter_class=ColorHelpFormatter,
	)
	parser.add_argument('--spath', required=True, help='Path to your Shimmie2 web root')
	parser.add_argument('--use-map', dest='use_map_csv', help='Override tagmap path (Defaults to database/tagmap.db)')
	parser.add_argument('--target-tag', help='Only curate images containing this specific tag')


def _add_fix_map_parser(subparsers) -> None:
	"""Adds the fix-map command."""
	parser = subparsers.add_parser(
		'fix-map',
		help='Audit and repair tagmap.db or a CSV map, and optionally revert poisoned DB tags',
		formatter_class=ColorHelpFormatter,
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
		formatter_class=ColorHelpFormatter,
	)
	parser.add_argument('--spath', required=True, help='Path to Shimmie root')
	parser.add_argument('--target-tag', required=True, help='The tag identifying the target batch (e.g., booru:bad_import)')
	parser.add_argument('--reference-tag', required=True, help='The pure reference tag (e.g., booru:gelbooru)')
	parser.add_argument('--use-map', dest='use_map_csv', help='Override existing tagmap path (Defaults to database/tagmap.db)')
	parser.add_argument('-o', '--out', default='botched_map.csv', help='Output .csv or .db path (Default: botched_map.csv)')


def _add_purge_parser(subparsers) -> None:
	"""Adds the purge command."""
	parser = subparsers.add_parser(
		'purge',
		help='Permanently delete images based on blacklisted tags',
		formatter_class=ColorHelpFormatter,
	)
	parser.add_argument('--spath', required=True, help='Path to your Shimmie2 web root')
	parser.add_argument('--blacklist', default='blacklist.txt', help='Path to your blacklist file (Default: blacklist.txt)')
	parser.add_argument('--dry-run', action='store_true', help='Generate a report without deleting anything')


def _add_tag_dump_parser(subparsers) -> None:
	"""Adds the tag-dump command."""
	parser = subparsers.add_parser(
		'tag-dump',
		help='Dump low-frequency DB tags to a CSV or tagmap.db',
		formatter_class=ColorHelpFormatter,
	)
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
	parser = subparsers.add_parser(
		'update-ratings',
		help='Update image ratings in Shimmie',
		formatter_class=ColorHelpFormatter,
	)
	parser.add_argument('--spath', required=True, help='Path to Shimmie root')
	parser.add_argument('-q', '--qmax', type=int, default=250, help='Max questionable rating (Default: 250)')
	parser.add_argument('-s', '--smax', type=int, default=50, help='Max safe rating (Default: 50)')


def _generate_completion_script(shell: str, subparsers) -> str:
	"""Generates a native bash or zsh tab-completion script for shimmie_tool.py."""
	commands = sorted(subparsers.choices.keys())
	cmd_str = ' '.join(commands)

	case_branches: list[str] = []
	for cmd_name, subparser in subparsers.choices.items():
		flags: list[str] = []
		for action in subparser._actions:
			flags.extend(action.option_strings)
		flag_str = ' '.join(sorted(set(flags)))
		case_branches.append(f'\t\t{cmd_name}) opts="{flag_str}" ;;')

	cases_block = '\n'.join(case_branches)

	if shell == 'zsh':
		return (
			'#compdef shimmie_tool.py ./shimmie_tool.py\n'
			'_shimmie_tool() {\n'
			'\tlocal cur="${words[CURRENT]}"\n'
			'\tlocal prev="${words[CURRENT-1]}"\n'
			'\tif (( CURRENT == 2 )); then\n'
			f'\t\tcompadd -- {cmd_str} --help --help-all --completion\n'
			'\t\treturn\n'
			'\tfi\n'
			'\tif [[ "$prev" == "--model" ]]; then\n'
			'\t\tcompadd -- vit vit-large swinv2 convnext eva02\n'
			'\t\treturn\n'
			'\tfi\n'
			'\tlocal opts=""\n'
			'\tcase "${words[2]}" in\n'
			f'{cases_block}\n'
			'\tesac\n'
			'\tif [[ "$cur" == -* ]]; then\n'
			'\t\tcompadd -- ${(z)opts}\n'
			'\telse\n'
			'\t\t_files\n'
			'\tfi\n'
			'}\n'
			'compdef _shimmie_tool shimmie_tool.py ./shimmie_tool.py\n'
		)

	return (
		'_shimmie_tool_complete() {\n'
		'\tlocal cur prev cmd opts\n'
		'\tcur="${COMP_WORDS[COMP_CWORD]}"\n'
		'\tprev="${COMP_WORDS[COMP_CWORD-1]}"\n'
		'\tcmd="${COMP_WORDS[1]}"\n'
		'\tif [[ ${COMP_CWORD} -eq 1 ]]; then\n'
		f'\t\tCOMPREPLY=( $(compgen -W "{cmd_str} --help --help-all --completion" -- "${{cur}}") )\n'
		'\t\treturn 0\n'
		'\tfi\n'
		'\tif [[ "${prev}" == "--model" ]]; then\n'
		'\t\tCOMPREPLY=( $(compgen -W "vit vit-large swinv2 convnext eva02" -- "${cur}") )\n'
		'\t\treturn 0\n'
		'\tfi\n'
		'\tcase "${cmd}" in\n'
		f'{cases_block}\n'
		'\tesac\n'
		'\tif [[ "${cur}" == -* ]]; then\n'
		'\t\tCOMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )\n'
		'\telse\n'
		'\t\tCOMPREPLY=( $(compgen -f -- "${cur}") )\n'
		'\tfi\n'
		'}\n'
		'complete -o default -F _shimmie_tool_complete shimmie_tool.py ./shimmie_tool.py\n'
	)


def setup_parser():
	"""Constructs the master argument parser."""
	parser = argparse.ArgumentParser(
		description='=== Shimmie2 Master Toolsuite ===\nA collection of tools to manage data.',
		formatter_class=ColorHelpFormatter,
	)
	parser.add_argument('--help-all', action='store_true', help='Show full help for all commands and exit')
	parser.add_argument(
		'--completion',
		choices=['bash', 'zsh'],
		help='Print shell tab-completion script for bash or zsh and exit',
	)

	subparsers = parser.add_subparsers(dest='command', title='Available Commands', metavar='')

	parser_csv = _add_csv_parser(subparsers)
	_add_import_csv_parser(subparsers)
	_add_audit_ratings_parser(subparsers)
	_add_auto_tag_parser(subparsers)
	_add_csv2sqlite_parser(subparsers)
	_add_curate_db_parser(subparsers)
	_add_fix_map_parser(subparsers)
	_add_generate_map_parser(subparsers)
	_add_purge_parser(subparsers)
	_add_tag_dump_parser(subparsers)
	_add_update_ratings_parser(subparsers)

	try:
		import argcomplete  # type: ignore[import-untyped]
		argcomplete.autocomplete(parser)
	except ImportError:
		pass

	return parser, parser_csv, subparsers


def _dispatch(args, parser_csv) -> None:
	"""Lazily imports and runs the selected tool command."""
	if args.command == 'make-csv':
		from tools import csv_builder

		if args.path:
			args.image_path = args.image_path or args.path
			args.video_path = args.video_path or args.path
		if not args.image_path and not args.video_path:
			parser_csv.error('You must provide at least one input path: --path, --images, or --videos')
		if (args.skip_existing or args.direct_import) and not args.spath:
			parser_csv.error('--spath is required when --skip-existing or --direct-import is set.')

		args.pretags = [t.strip().lower() for t in args.pretags.split(',') if t.strip()] if args.pretags else []
		csv_builder.run(args)
		return

	if args.command == 'import-csv':
		from tools import importer

		importer.run_import_csv(args)
	elif args.command == 'audit-ratings':
		from tools import ai_auditor

		ai_auditor.run_auditor(args)
	elif args.command == 'auto-tag':
		from tools import auto_tagger

		if getattr(args, 'path', None):
			args.images = args.images or args.path
			args.videos = args.videos or args.path
		auto_tagger.run_auto_tagger(args)
	elif args.command == 'csv2sqlite':
		from tools import csv2sqlite

		csv2sqlite.csv_to_sqlite(args)
	elif args.command == 'curate-db':
		from tools import curate_db

		curate_db.curate_existing_tags(args)
	elif args.command == 'fix-map':
		from tools import fix_map

		fix_map.fix_curation_map(args)
	elif args.command == 'generate-db-map':
		from tools import generate_db_map

		generate_db_map.generate_db_curation_map(args)
	elif args.command == 'purge':
		from tools import purge

		purge.purge_images(args)
	elif args.command == 'tag-dump':
		from tools import tag_dump

		tag_dump.tag_dump(args)
	elif args.command == 'update-ratings':
		from tools import update_ratings

		update_ratings.update_ratings(args)


def _ensure_venv() -> None:
	"""Ensures the local ./venv exists, activates it to install requirements, and re-execs inside it."""
	root_dir = Path(__file__).resolve().parent
	venv_dir = root_dir / 'venv'
	venv_python = venv_dir / 'bin' / 'python'

	if Path(sys.prefix).resolve() == venv_dir.resolve():
		return

	if not venv_python.is_file():
		print('[INFO] Virtual environment not found. Creating venv and installing requirements via uv...')
		subprocess.run(
			['bash', '-c', 'uv venv venv && source venv/bin/activate && uv pip install -r requirements.txt'],
			cwd=root_dir,
			check=True,
		)

	os.environ['VIRTUAL_ENV'] = str(venv_dir)
	os.environ['PATH'] = f"{venv_dir / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"
	os.execv(str(venv_python), [str(venv_python), *sys.argv])


def main() -> None:
	"""Main CLI router."""
	_ensure_venv()
	parser, parser_csv, subparsers = setup_parser()

	if '--help-all' in sys.argv:
		parser.print_help()
		divider = _c('=' * 80, DIM)
		print(f'\n{divider}')
		for name, subparser in subparsers.choices.items():
			header = _c(f'● Command: {name}', f'{BOLD}{BLUE}')
			sub_div = _c('─' * 40, DIM)
			print(f'\n{header}\n{sub_div}')
			subparser.print_help()
		sys.exit(0)

	args = parser.parse_args()

	if args.completion:
		print(_generate_completion_script(args.completion, subparsers))
		sys.exit(0)

	if not args.command:
		parser.print_help()
		sys.exit(1)

	_dispatch(args, parser_csv)


if __name__ == '__main__':
	main()
