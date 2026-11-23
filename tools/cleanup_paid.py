"""Safe post-import review mover for staged paid media.

NEVER unlinks source media files. Streams MD5-verified records from database,
moves verified originals into a review directory on the source drive, and appends each move to
undo_manifest.jsonl for crash-safe full reversal.
"""

import shutil
import sqlite3
import sys
from contextlib import suppress
from pathlib import Path

import orjson
import psycopg2

from functions.common import STAGING_STATE_DB_PATH
from functions.db_cache import get_shimmie_db_credentials


def _fetch_imported_hashes(md5s: list[str], conn) -> set[str]:
    """Returns the subset of MD5 hashes confirmed present in Shimmie2's images table."""
    if not md5s:
        return set()
    with conn.cursor() as cur:
        cur.execute("SELECT hash FROM images WHERE hash = ANY(%s)", (md5s,))
        return {row[0].strip().lower() for row in cur.fetchall() if row[0]}


def _lookup_source_root(staging_dir: Path) -> Path:
    """Looks up the source_root registered for a staging_dir in staging_state.db."""
    if not STAGING_STATE_DB_PATH.is_file():
        print(f"[ERROR] Staging state DB not found: {STAGING_STATE_DB_PATH}")
        sys.exit(1)

    with sqlite3.connect(STAGING_STATE_DB_PATH) as conn:
        row = conn.execute(
            "SELECT source_root FROM staging_batches WHERE staging_dir = ?",
            (str(staging_dir),),
        ).fetchone()
        if row and row[0]:
            return Path(row[0]).resolve()

        fallback = conn.execute(
            "SELECT source_root FROM staged_files WHERE staging_dir = ? AND source_root != '' LIMIT 1",
            (str(staging_dir),),
        ).fetchone()
        if fallback and fallback[0]:
            return Path(fallback[0]).resolve()

    print(f"[ERROR] No staging batch found in {STAGING_STATE_DB_PATH} for: {staging_dir}")
    sys.exit(1)


def _resolve_review_dir(args, source_root: Path) -> Path:
    """Defaults review dir beside source_root so moves stay an instant same-drive rename."""
    if getattr(args, "review_dir", None):
        return Path(args.review_dir).resolve()
    return source_root.with_name(f"{source_root.name}-reviewed")


def _run_move_or_dry_run(args, apply: bool) -> None:
    if not getattr(args, "spath", None):
        print("[ERROR] --spath is required for dry-run and apply.")
        sys.exit(1)

    staging_dir = Path(args.staging_dir).resolve()
    source_root = _lookup_source_root(staging_dir)
    fluff_dir = staging_dir.with_name(f"{staging_dir.name}-fluff")
    review_dir = _resolve_review_dir(args, source_root)

    db_cfg = get_shimmie_db_credentials(args.spath)
    if not db_cfg:
        print(f"[ERROR] Could not load DB credentials from {args.spath}")
        sys.exit(1)

    ready_count = 0
    moved_count = 0
    skipped_unimported = 0
    skipped_missing = 0

    dry_log_path = staging_dir / "cleanup_dry_run.log"
    undo_path = review_dir / "undo_manifest.jsonl"

    dry_handle = dry_log_path.open("w", encoding="utf-8") if not apply else None
    undo_handle = None

    try:
        with (
            sqlite3.connect(STAGING_STATE_DB_PATH) as sconn,
            psycopg2.connect(**db_cfg) as pg_conn,
        ):
            scur = sconn.execute(
                "SELECT original_path, staged_path, md5 FROM staged_files WHERE staging_dir = ?",
                (str(staging_dir),),
            )

            while batch := scur.fetchmany(10000):
                imported = _fetch_imported_hashes([r[2].lower() for r in batch], pg_conn)

                for orig_str, staged_str, md5_raw in batch:
                    orig = Path(orig_str)
                    md5 = md5_raw.lower()
                    if not orig.is_file():
                        skipped_missing += 1
                        continue

                    reason = ""
                    if md5 in imported:
                        reason = "imported_in_shimmie"
                    elif args.include_fluff and staged_str:
                        with suppress(ValueError):
                            staged_rel = Path(staged_str).relative_to(staging_dir)
                            fluff_cand = fluff_dir / staged_rel
                            if fluff_cand.exists() or fluff_cand.is_symlink():
                                reason = "triaged_fluff"

                    if not reason:
                        skipped_unimported += 1
                        continue

                    try:
                        rel = orig.resolve().relative_to(source_root)
                    except ValueError:
                        rel = Path(orig.name)
                    dest = review_dir / rel
                    ready_count += 1

                    if not apply:
                        assert dry_handle is not None
                        dry_handle.write(f"[{reason}] {orig} -> {dest} ({md5})\n")
                        continue

                    if undo_handle is None:
                        review_dir.mkdir(parents=True, exist_ok=True)
                        undo_handle = undo_path.open("ab")

                    dest.parent.mkdir(parents=True, exist_ok=True)
                    if dest.exists():
                        print(f"[SKIP] Destination already exists: {dest}")
                        continue

                    shutil.move(str(orig), str(dest))
                    entry = {
                        "staging_dir": str(staging_dir),
                        "original": str(orig),
                        "moved_to": str(dest),
                        "md5": md5,
                        "reason": reason,
                    }
                    undo_handle.write(orjson.dumps(entry) + b"\n")
                    moved_count += 1

                if undo_handle is not None:
                    undo_handle.flush()
    finally:
        if dry_handle is not None:
            dry_handle.close()
        if undo_handle is not None:
            undo_handle.close()

    print(f"Verified ready to move to review dir: {ready_count:,}")
    print(f"Left untouched (not yet imported):    {skipped_unimported:,}")
    print(f"Already absent from original path:    {skipped_missing:,}")
    print(f"Review directory:                     {review_dir}")

    if not apply:
        print(f"\n[DRY RUN] No files moved. Detailed plan written to {dry_log_path}")
    else:
        print(f"\n[✓] Moved {moved_count:,} verified files into {review_dir}")
        if moved_count:
            print(f"    Undo log saved to {undo_path}")
            print("    Review and delete manually when satisfied, or run `undo` to restore.")


def _run_undo(args) -> None:
    staging_dir = Path(args.staging_dir).resolve()
    source_root = _lookup_source_root(staging_dir)
    review_dir = _resolve_review_dir(args, source_root)
    undo_path = review_dir / "undo_manifest.jsonl"

    if not undo_path.is_file():
        print(f"[ERROR] Undo log not found: {undo_path}")
        sys.exit(1)

    staging_str = str(staging_dir)
    raw_lines = [line for line in undo_path.read_bytes().splitlines() if line.strip()]
    restored = 0
    remaining: list[bytes] = []

    for line in reversed(raw_lines):
        entry = orjson.loads(line)
        entry_staging = entry.get("staging_dir")
        if entry_staging and entry_staging != staging_str:
            remaining.append(line)
            continue
        src = Path(entry["moved_to"])
        dst = Path(entry["original"])
        if not src.exists():
            continue
        if dst.exists():
            print(f"[WARNING] Original path already exists, skipping restore: {dst}")
            remaining.append(line)
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))
        restored += 1

    if remaining:
        undo_path.write_bytes(b"\n".join(reversed(remaining)) + b"\n")
    else:
        undo_path.unlink()
        for d in sorted((p for p in review_dir.rglob("*") if p.is_dir()), reverse=True):
            with suppress(OSError):
                d.rmdir()
        with suppress(OSError):
            review_dir.rmdir()

    print(f"[✓] Restored {restored:,} files back to their original paths.")


def _run_prune_db(args) -> None:
    """Deletes finished batch rows from staging_state.db (retaining completed_units) and VACUUMs."""
    if not STAGING_STATE_DB_PATH.is_file():
        print(f"[ERROR] Staging state DB not found: {STAGING_STATE_DB_PATH}")
        sys.exit(1)

    staging_str = str(Path(args.staging_dir).resolve())
    size_before = STAGING_STATE_DB_PATH.stat().st_size

    with sqlite3.connect(STAGING_STATE_DB_PATH) as conn:
        del_files = conn.execute(
            "DELETE FROM staged_files WHERE staging_dir = ?", (staging_str,)
        ).rowcount
        del_empty = conn.execute(
            "DELETE FROM empty_posts WHERE staging_dir = ?", (staging_str,)
        ).rowcount
        del_batch = conn.execute(
            "DELETE FROM staging_batches WHERE staging_dir = ?", (staging_str,)
        ).rowcount
        conn.commit()
        conn.execute("VACUUM")

    size_after = STAGING_STATE_DB_PATH.stat().st_size
    print(
        f"[✓] Pruned staging_state.db for {staging_str}: "
        f"{del_files:,} staged_files, {del_empty:,} empty_posts, {del_batch:,} staging_batches removed "
        f"(completed_units preserved)."
    )
    print(f"    DB size: {size_before / 1_048_576:.2f} MB -> {size_after / 1_048_576:.2f} MB")


def run_cleanup_paid(args) -> None:
    """Main CLI entry point for cleanup-paid."""
    if args.action == "apply":
        _run_move_or_dry_run(args, apply=True)
    elif args.action == "dry-run":
        _run_move_or_dry_run(args, apply=False)
    elif args.action == "undo":
        _run_undo(args)
    elif args.action == "prune-db":
        _run_prune_db(args)
