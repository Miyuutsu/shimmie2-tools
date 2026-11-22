"""Safe post-import review mover for staged paid media.

NEVER unlinks source media files. Moves only PostgreSQL MD5-verified originals into a review
directory on the source drive and logs every move to undo_manifest.json for full reversal.
"""

import json
import shutil
import sys
from contextlib import suppress
from pathlib import Path

import psycopg2

from functions.db_cache import get_shimmie_db_credentials


def _fetch_imported_hashes(md5s: list[str], db_cfg: dict) -> set[str]:
    """Returns the subset of MD5 hashes confirmed present in Shimmie2's images table."""
    found: set[str] = set()
    with psycopg2.connect(**db_cfg) as conn, conn.cursor() as cur:
        for i in range(0, len(md5s), 10000):
            cur.execute("SELECT hash FROM images WHERE hash = ANY(%s)", (md5s[i : i + 10000],))
            found.update(row[0].strip().lower() for row in cur.fetchall() if row[0])
    return found


def _resolve_review_dir(args, source_root: Path) -> Path:
    """Defaults review dir beside source_root so moves stay an instant same-drive rename."""
    if getattr(args, "review_dir", None):
        return Path(args.review_dir).resolve()
    return source_root.with_name(f"{source_root.name}-reviewed")


def _run_move_or_dry_run(args, apply: bool) -> None:
    if not getattr(args, "spath", None):
        print("[ERROR] --spath is required for dry-run and apply.")
        sys.exit(1)

    manifest_path = Path(args.manifest).resolve()
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    source_root = Path(data["source_root"]).resolve()
    staging_dir = Path(data["staging_dir"]).resolve()
    fluff_dir = staging_dir.with_name(f"{staging_dir.name}-fluff")
    review_dir = _resolve_review_dir(args, source_root)

    db_cfg = get_shimmie_db_credentials(args.spath)
    if not db_cfg:
        print(f"[ERROR] Could not load DB credentials from {args.spath}")
        sys.exit(1)

    items = data.get("items", [])
    imported_hashes = _fetch_imported_hashes([it["md5"] for it in items], db_cfg)

    to_move: list[tuple[Path, Path, str, str]] = []
    skipped_unimported = 0
    skipped_missing = 0

    for it in items:
        orig = Path(it["original_path"])
        md5 = it["md5"].lower()
        if not orig.is_file():
            skipped_missing += 1
            continue

        reason = ""
        if md5 in imported_hashes:
            reason = "imported_in_shimmie"
        elif args.include_fluff:
            staged_rel = Path(it["staged_path"]).relative_to(staging_dir)
            if (fluff_dir / staged_rel).exists() or (fluff_dir / staged_rel).is_symlink():
                reason = "triaged_fluff"

        if not reason:
            skipped_unimported += 1
            continue

        try:
            rel = orig.resolve().relative_to(source_root)
        except ValueError:
            rel = Path(orig.name)
        to_move.append((orig, review_dir / rel, md5, reason))

    print(f"Verified ready to move to review dir: {len(to_move):,}")
    print(f"Left untouched (not yet imported):    {skipped_unimported:,}")
    print(f"Already absent from original path:    {skipped_missing:,}")
    print(f"Review directory:                     {review_dir}")

    if not apply:
        log_path = staging_dir / "cleanup_dry_run.log"
        with log_path.open("w", encoding="utf-8") as f:
            for orig, dest, md5, reason in to_move:
                f.write(f"[{reason}] {orig} -> {dest} ({md5})\n")
        print(f"\n[DRY RUN] No files moved. Detailed plan written to {log_path}")
        return

    if not to_move:
        print("\n[✓] Nothing to move.")
        return

    review_dir.mkdir(parents=True, exist_ok=True)
    undo_path = review_dir / "undo_manifest.json"
    undo_entries: list[dict[str, str]] = (
        json.loads(undo_path.read_text(encoding="utf-8")) if undo_path.is_file() else []
    )

    moved = 0
    try:
        for orig, dest, md5, reason in to_move:
            dest.parent.mkdir(parents=True, exist_ok=True)
            if dest.exists():
                print(f"[SKIP] Destination already exists: {dest}")
                continue
            shutil.move(str(orig), str(dest))
            undo_entries.append(
                {"original": str(orig), "moved_to": str(dest), "md5": md5, "reason": reason}
            )
            moved += 1
    finally:
        undo_path.write_text(json.dumps(undo_entries, indent=2), encoding="utf-8")

    print(f"\n[✓] Moved {moved:,} verified files into {review_dir}")
    print(f"    Undo manifest saved to {undo_path}")
    print("    Review the folder and delete it manually when satisfied, or run `undo` to restore.")


def _run_undo(args) -> None:
    manifest_path = Path(args.manifest).resolve()
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    source_root = Path(data["source_root"]).resolve()
    review_dir = _resolve_review_dir(args, source_root)
    undo_path = review_dir / "undo_manifest.json"

    if not undo_path.is_file():
        print(f"[ERROR] Undo manifest not found: {undo_path}")
        sys.exit(1)

    entries = json.loads(undo_path.read_text(encoding="utf-8"))
    restored = 0
    remaining: list[dict[str, str]] = []

    for entry in reversed(entries):
        src = Path(entry["moved_to"])
        dst = Path(entry["original"])
        if not src.exists():
            continue
        if dst.exists():
            print(f"[WARNING] Original path already exists, skipping restore: {dst}")
            remaining.append(entry)
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))
        restored += 1

    if remaining:
        undo_path.write_text(json.dumps(list(reversed(remaining)), indent=2), encoding="utf-8")
    else:
        undo_path.unlink()
        for d in sorted((p for p in review_dir.rglob("*") if p.is_dir()), reverse=True):
            with suppress(OSError):
                d.rmdir()
        with suppress(OSError):
            review_dir.rmdir()

    print(f"[✓] Restored {restored:,} files back to their original paths.")


def run_cleanup_paid(args) -> None:
    """Main CLI entry point for cleanup-paid."""
    if args.action == "apply":
        _run_move_or_dry_run(args, apply=True)
    elif args.action == "dry-run":
        _run_move_or_dry_run(args, apply=False)
    elif args.action == "undo":
        _run_undo(args)
