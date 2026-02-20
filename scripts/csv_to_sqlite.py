"""Converts any valid CSV file into a TEXT-only SQLite Database"""
import argparse
import csv
import sqlite3
from pathlib import Path

def main(csv_file, sqlite_file, table_name="data", drop_table=False):
    """The script"""
    csv_path = Path(csv_file)
    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    # Read CSV headers and rows
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)

        # Sanitize headers and table name to prevent injection
        safe_table = f'"{table_name.replace('"', '""')}"'
        create_cols = ', '.join([f'"{h.replace('"', '""')}" TEXT' for h in headers])
        insert_cols = ', '.join([f'"{h.replace('"', '""')}"' for h in headers])

        placeholders = ', '.join(['?'] * len(headers))

        # Create SQLite connection and cursor
        with sqlite3.connect(sqlite_file) as conn:
            cursor = conn.cursor()

            # Drop table if it exists
            if drop_table:
                cursor.execute(f"DROP TABLE IF EXISTS {safe_table}")

            # Create table with columns based on CSV headers (default: TEXT type)
            cursor.execute(f'CREATE TABLE {safe_table} ({create_cols})')

            # Insert data
            cursor.executemany(
                f'INSERT INTO {safe_table} ({insert_cols}) VALUES ({placeholders})',
                reader
            )

    print(f"✅ Converted '{csv_file}' to '{sqlite_file}'")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Converts any valid CSV file into an SQLite Database.")
    parser.add_argument("--csv", required=True, help="Path to CSV")
    parser.add_argument("--db", required=True, help="Path to output SQLite Database")
    parser.add_argument("--drop_table", action="store_true", help="Drop table if exists")
    parser.add_argument("--table", default="data", help="Table name")

    args = parser.parse_args()

    main(args.csv, args.db, args.table, args.drop_table)
