"""Utility for converting CSV files into SQLite database tables."""

import csv
from pathlib import Path
import sqlite3

from functions.common import TAGMAP_DB_PATH
from functions.db_cache import import_csv_to_tagmap_db


def _quote_ident(identifier: str) -> str:
	"""Safely double-quotes an SQLite identifier."""
	escaped = identifier.replace('"', '""')
	return f'"{escaped}"'


def csv_to_sqlite(args) -> None:
	"""Converts a CSV file to an SQLite database table."""
	csv_path = Path(args.csv)
	if not csv_path.is_file():
		raise FileNotFoundError(f'CSV file not found: {csv_path}')

	db_path = Path(args.db)
	db_path.parent.mkdir(parents=True, exist_ok=True)

	if db_path.resolve() == TAGMAP_DB_PATH.resolve() or args.table == 'tagmap':
		if args.drop_table and db_path.is_file():
			with sqlite3.connect(db_path) as conn:
				conn.execute('DROP TABLE IF EXISTS tagmap')
		imported = import_csv_to_tagmap_db(csv_path, db_path)
		print(f"[✓] Imported {imported:,} tagmap rows from '{csv_path}' into '{db_path}'.")
		return

	with csv_path.open('r', newline='', encoding='utf-8') as f:
		reader = csv.reader(f)
		headers = next(reader, None)
		if not headers:
			raise ValueError(f'CSV file is empty or missing headers: {csv_path}')

		safe_table = _quote_ident(args.table)
		create_cols = ', '.join(f'{_quote_ident(h)} TEXT' for h in headers)
		insert_cols = ', '.join(_quote_ident(h) for h in headers)
		placeholders = ', '.join(['?'] * len(headers))

		with sqlite3.connect(db_path) as conn:
			cursor = conn.cursor()
			if args.drop_table:
				cursor.execute(f'DROP TABLE IF EXISTS {safe_table}')
			cursor.execute(f'CREATE TABLE IF NOT EXISTS {safe_table} ({create_cols})')
			cursor.executemany(
				f'INSERT INTO {safe_table} ({insert_cols}) VALUES ({placeholders})',
				reader,
			)

	print(f"[✓] Converted '{csv_path}' to '{db_path}' in table '{args.table}'.")
