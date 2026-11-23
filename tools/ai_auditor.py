"""In-process AI auditor for Shimmie2 thumbnails that injects tagai:* shadow tags."""

import os
import sqlite3
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import psycopg2
import psycopg2.extras
import tqdm

from functions.ai_tagger import (
    SmilingWolfTagger,
    aggregate_frame_predictions,
    decode_media_for_tagger,
)
from functions.common import QUARANTINE_DB_PATH, VIDEO_EXTS
from functions.db_cache import (
    ensure_tags_in_db,
    get_shimmie_db_credentials,
    recalculate_tag_counts,
)


def _init_quarantine_db() -> sqlite3.Connection:
    """Initializes the SQLite quarantine database for logging raw AI tag confidences."""
    QUARANTINE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    q_conn = sqlite3.connect(QUARANTINE_DB_PATH)
    q_conn.execute("PRAGMA journal_mode=WAL;")
    q_conn.execute("PRAGMA synchronous=NORMAL;")
    q_conn.execute("""
        CREATE TABLE IF NOT EXISTS ai_tags (
            image_hash TEXT,
            tag TEXT,
            confidence REAL,
            UNIQUE(image_hash, tag)
        )
    """)
    q_conn.execute("CREATE INDEX IF NOT EXISTS idx_ai_tags_hash ON ai_tags(image_hash)")
    return q_conn


def _get_image_batches(
    pg_conn, batch_size: int = 5000
) -> Iterator[dict[str, tuple[int, set[str]]]]:
    """Yields {md5: (image_id, existing_tags)} for untagged static images via a server-side cursor."""
    excluded_exts = tuple(ext.lstrip(".") for ext in VIDEO_EXTS)
    with pg_conn.cursor(name="audit_cursor", withhold=True) as cur:
        cur.execute(
            """
            WITH tagai_tags AS MATERIALIZED (
                SELECT id FROM tags WHERE tag LIKE 'tagai:%%'
            )
            SELECT i.id, i.hash, array_agg(t.tag)
            FROM images i
            LEFT JOIN image_tags it ON i.id = it.image_id
            LEFT JOIN tags t ON it.tag_id = t.id
            WHERE LOWER(i.ext) NOT IN %s
              AND NOT EXISTS (
                  SELECT 1
                  FROM image_tags it2
                  WHERE it2.image_id = i.id
                    AND it2.tag_id IN (SELECT id FROM tagai_tags)
              )
            GROUP BY i.id, i.hash
            """,
            (excluded_exts,),
        )

        while rows := cur.fetchmany(batch_size):
            batch_data: dict[str, tuple[int, set[str]]] = {}
            for img_id, img_hash, tag_list in rows:
                clean_h = img_hash.strip().lower()
                clean_tags = {t for t in (tag_list or ()) if t is not None}
                batch_data[clean_h] = (int(img_id), clean_tags)
            yield batch_data


def _resolve_shimmie_thumb_path(base_dir: Path, md5_hash: str) -> Path:
    """Resolves the two-level fanout path for a Shimmie2 thumbnail."""
    return base_dir / md5_hash[0:2] / md5_hash[2:4] / md5_hash


def _run_audit_pipeline(
    args, pg_conn, pg_cur, q_conn: sqlite3.Connection, q_cur: sqlite3.Cursor
) -> None:
    """Streams image chunks from Postgres, runs in-process GPU inference, and commits shadow tags."""
    thumbs_base_dir = Path(args.thumbs) if args.thumbs else Path(args.spath) / "data" / "thumbs"
    if not thumbs_base_dir.is_dir():
        print(f"[ERROR] Thumbnails directory not found at {thumbs_base_dir}")
        return

    threads = getattr(args, "threads", max(1, (os.cpu_count() or 1) // 2))
    tagger = SmilingWolfTagger(args.model)

    print("Loading existing tag map from Postgres...")
    pg_cur.execute("SELECT tag, id FROM tags")
    tag_id_map: dict[str, int] = dict(pg_cur.fetchall())
    affected_tag_ids: set[int] = set()

    gpu_chunk_size = max(64, args.batch * 8)
    print("Initiating streaming database cursor...")

    with ThreadPoolExecutor(max_workers=threads) as pool:
        for batch_index, images_data in enumerate(_get_image_batches(pg_conn)):
            if not images_data:
                continue

            # Sort by hash so fanout reads (data/thumbs/p1/p2/hash) are sequential on disk
            work_items: list[tuple[str, Path]] = []
            for img_hash in sorted(images_data):
                thumb_path = _resolve_shimmie_thumb_path(thumbs_base_dir, img_hash)
                if thumb_path.is_file():
                    work_items.append((img_hash, thumb_path))

            if not work_items:
                print(f"[WARNING] Chunk {batch_index + 1}: no thumbnails found on disk. Skipping.")
                continue

            print(
                f"\n--- Processing DB Chunk {batch_index + 1} "
                f"({len(work_items):,}/{len(images_data):,} thumbnails) ---"
            )

            pending_tag_pairs: set[tuple[int, str]] = set()
            quarantine_records: list[tuple[str, str, float]] = []

            chunk_starts = range(0, len(work_items), gpu_chunk_size)
            next_futs = [
                (hsh, pool.submit(decode_media_for_tagger, p, 1))
                for hsh, p in work_items[0:gpu_chunk_size]
            ]

            for c_idx in tqdm.tqdm(range(len(chunk_starts)), desc="GPU Inference", leave=False):
                decoded = [(hsh, fut.result()[1]) for hsh, fut in next_futs]

                if c_idx + 1 < len(chunk_starts):
                    n_start = chunk_starts[c_idx + 1]
                    next_futs = [
                        (hsh, pool.submit(decode_media_for_tagger, p, 1))
                        for hsh, p in work_items[n_start : n_start + gpu_chunk_size]
                    ]
                else:
                    next_futs = []

                flat_arrays: list[np.ndarray] = []
                valid_hashes: list[str] = []
                for hsh, arrays in decoded:
                    if arrays:
                        flat_arrays.append(arrays[0])
                        valid_hashes.append(hsh)

                if not flat_arrays:
                    continue

                probs_list = tagger.infer_batch(flat_arrays, args.batch)
                for hsh, probs in zip(valid_hashes, probs_list, strict=True):
                    preds = aggregate_frame_predictions(
                        [probs],
                        tagger,
                        gen_threshold=args.gen_threshold,
                        char_threshold=args.char_threshold,
                    )
                    img_id, raw_db_tags = images_data[hsh]
                    base_db_tags = {
                        (t.split(":", 1)[-1] if ":" in t else t).lower() for t in raw_db_tags
                    }

                    for tag_name, conf in {**preds.char_scores, **preds.gen_scores}.items():
                        quarantine_records.append((hsh, tag_name, conf))
                        if tag_name not in base_db_tags:
                            pending_tag_pairs.add((img_id, f"tagai:{tag_name}"[:255]))

            if quarantine_records:
                q_cur.executemany(
                    "INSERT OR IGNORE INTO ai_tags (image_hash, tag, confidence) VALUES (?, ?, ?)",
                    quarantine_records,
                )
                q_conn.commit()

            if pending_tag_pairs:
                new_tag_names = {t_name for _, t_name in pending_tag_pairs}
                ensure_tags_in_db(pg_cur, new_tag_names, tag_id_map)

                pg_insert_records = [
                    (img_id, tag_id_map[t_name]) for img_id, t_name in pending_tag_pairs
                ]
                affected_tag_ids.update(tid for _, tid in pg_insert_records)

                psycopg2.extras.execute_values(
                    pg_cur,
                    "INSERT INTO image_tags (image_id, tag_id) VALUES %s ON CONFLICT DO NOTHING",
                    pg_insert_records,
                    page_size=10000,
                )
                pg_conn.commit()
                print(f"Applied {len(pg_insert_records):,} shadow tags for this chunk.")
            else:
                print("No new tags to apply for this chunk.")

    if affected_tag_ids:
        print(f"\nRecalculating tag usage counts for {len(affected_tag_ids):,} tagai:* tags...")
        recalculate_tag_counts(pg_cur, affected_tag_ids, prune_orphans=False)
        pg_conn.commit()

    print("\n[✓] Global Audit Complete.")


def run_auditor(args) -> None:
    """Main CLI entry point for audit-ratings."""
    print("=== AI Shadow Tag Auditor (In-Process Direct Apply) ===")
    db_config = get_shimmie_db_credentials(args.spath)
    if not db_config:
        print(f"[ERROR] Could not load DB credentials from {args.spath}")
        return

    pg_conn = psycopg2.connect(**db_config)
    q_conn = _init_quarantine_db()

    try:
        with pg_conn.cursor() as pg_cur:
            q_cur = q_conn.cursor()
            _run_audit_pipeline(args, pg_conn, pg_cur, q_conn, q_cur)
    finally:
        pg_conn.close()
        q_conn.close()
