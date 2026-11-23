"""Utility for converting CSV files into SQLite database tables."""

import csv
import sqlite3
from pathlib import Path

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
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if db_path.resolve() == TAGMAP_DB_PATH.resolve() or args.table == "tagmap":
        if args.drop_table and db_path.is_file():
            with sqlite3.connect(db_path) as conn:
                conn.execute("DROP TABLE IF EXISTS tagmap")
        imported = import_csv_to_tagmap_db(csv_path, db_path)
        print(f"[✓] Imported {imported:,} tagmap rows from '{csv_path}' into '{db_path}'.")
        return

    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader, None)
        if not headers:
            raise ValueError(f"CSV file is empty or missing headers: {csv_path}")

        num_cols = len(headers)
        adjusted_rows = 0

        def _normalized_rows():
            nonlocal adjusted_rows
            for row in reader:
                if not row:
                    continue
                if len(row) != num_cols:
                    adjusted_rows += 1
                    if len(row) < num_cols:
                        yield [*row, *([""] * (num_cols - len(row)))]
                    else:
                        yield row[:num_cols]
                else:
                    yield row

        safe_table = _quote_ident(args.table)
        create_cols = ", ".join(f"{_quote_ident(h)} TEXT" for h in headers)
        insert_cols = ", ".join(_quote_ident(h) for h in headers)
        placeholders = ", ".join(["?"] * num_cols)

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            if args.drop_table:
                cursor.execute(f"DROP TABLE IF EXISTS {safe_table}")
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {safe_table} ({create_cols})")
            cursor.executemany(
                f"INSERT INTO {safe_table} ({insert_cols}) VALUES ({placeholders})",
                _normalized_rows(),
            )

    if adjusted_rows:
        print(
            f"[WARNING] Normalized {adjusted_rows:,} row(s) whose column count did not match headers."
        )
    print(f"[✓] Converted '{csv_path}' to '{db_path}' in table '{args.table}'.")
