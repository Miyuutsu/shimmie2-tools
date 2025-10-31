import argparse
import csv
import sqlite3
from pathlib import Path
import sys

def csv_to_sqlite(csv_file, sqlite_file, table_name="data", drop_table=False):
    csv_path = Path(csv_file)
    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    # Read CSV headers and rows
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = list(reader)

    # Create SQLite connection and cursor
    conn = sqlite3.connect(sqlite_file)
    cursor = conn.cursor()

    # Drop table if it exists
    if drop_table is not False:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Create table with columns based on CSV headers (default: TEXT type)
    columns = ', '.join([f'"{h}" TEXT' for h in headers])
    cursor.execute(f'CREATE TABLE {table_name} ({columns})')

    # Insert data
    placeholders = ', '.join(['?'] * len(headers))
    cursor.executemany(
        f'INSERT INTO {table_name} VALUES ({placeholders})',
        rows
    )

    conn.commit()
    conn.close()
    print(f"✅ Converted '{csv_file}' to '{sqlite_file}'")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Converts any valid CSV file into an SQLite Database.")
    parser.add_argument("--csv_file", "--csv", dest="csv", default="", help="Path to CSV")
    parser.add_argument("--sqlite_file", "--sql", dest="db", default="", help="Path to output SQLite Database")
    parser.add_argument("--table", default="data", help="Table name")
    parser.add_argument("--drop_table", action="store_true")

    args = parser.parse_args()

    csv_to_sqlite(args.csv, args.db, args.table, args.drop_table)
