# Shimmie2 Master Toolsuite

A modular Python CLI suite (`shimmie_tool.py`) for staging, AI-triaging, curating, importing, and maintaining large multi-terabyte media collections in [Shimmie2](https://github.com/shish/shimmie2) (PostgreSQL).

---

## ✨ Commands

Run `./shimmie_tool.py --help-all` for full options on every subcommand.

### Paid Content & Staging Pipeline
- **`prep-paid`**: Streams read-only paid archive directories (Fanbox, Fantia, Patreon, Kemono/Pawchive, gallery-dl), resolves thumbnail fallbacks with archive/manifest safeguards, indexes chronological metadata into `timeline_index.db`, tracks resume checkpoints in `staging_state.db`, and emits a safe `cleanup_staged.sh` wrapper.
- **`triage`**: 3-stage content classifier (`KEEP`, `-comic`, `-fluff`) combining bulk MD5 lookup, single-open resolution/EXIF/downscale-duplicate pre-filtering (`--dedup-downscales`, `--reso-fluff`), and cascaded in-process GPU inference (`SigLIP-B-16-256` -> SmilingWolf v3 taggers) with reusable `# Verdict:` diagnostic sidecars (`--reset`, `--retriage`, `--full-reset`, `--full-retriage`).
- **`sync-pools`**: Builds and chronologically orders per-artist Shimmie2 Pools from `timeline_index.db`, preserving any manually added pool posts and skipping unchanged pools.
- **`cleanup-paid`**: Safely moves MD5-verified originals from the source drive into a same-drive `<source>-reviewed` folder with a crash-safe `undo_manifest.jsonl` (`dry-run`, `apply`, `undo`), and trims completed batch rows from `staging_state.db` (`prune-db`) while preserving `completed_units`.

### Import & Tagging
- **`make-csv`**: Curates tags, resolves canonical source URLs, calculates ratings, filters against `blacklist.txt`, updates `posts_cache.db`, and either writes chunked import CSVs or imports straight into Shimmie2 (`--direct-import`).
- **`import-csv`**: Natively imports Shimmie2 `import*.csv` files into PostgreSQL and `data/images`/`data/thumbs`, merging tags/sources/ratings onto existing MD5s.
- **`auto-tag`**: Generates missing `.txt` sidecars for images and videos using in-process SmilingWolf v3 GPU inference, automatically shielding already-known MD5s with `tagai:`.
- **`audit-ratings`**: Streams untagged static Shimmie2 thumbnails from PostgreSQL and injects `tagai:*` shadow tags in-process while logging confidences to `ai_quarantine_cache.db`.

### Database Maintenance & Mapping
- **`curate-db`**: Retroactively applies `tagmap.db`, character/artist/series enrichment, `tag_groups.json` parent expansion, CDN URL normalization, and rating upgrades to live Shimmie2 posts.
- **`update-ratings`**: Recalculates existing Shimmie2 post ratings from `tag_rating_dominant.db`.
- **`purge`**: Permanently deletes posts matching `blacklist.txt` rules from PostgreSQL (syncing tag and pool counts) and disk, with detailed `--dry-run` collateral reports.
- **`generate-db-map`**: Mines 1:1 synonym mappings and prefix rescues by diffing a target batch tag against a reference tag in PostgreSQL.
- **`fix-map`**: Audits and repairs `tagmap.db` or CSV maps, purges poisoned mappings, rescues `_DROP_` entries, and optionally reverts poisoned DB tags via `tag_histories` (`--revert`).
- **`tag-dump`**: Dumps low-frequency PostgreSQL tags to CSV or `tagmap.db` alongside a namespace and booru impact `.log`.
- **`csv2sqlite`**: Converts generic CSV files (or tagmap CSVs) into SQLite tables.

---

## 🧠 Requirements

- **Python 3.10+** (`uv` recommended — `shimmie_tool.py` auto-bootstraps `./venv` via `uv` if missing)
- **OS**: Linux (filesystem `user.shimmie.*` xattr caching works on ext4, btrfs, xfs, etc.)
- **Database**: Shimmie2 backed by PostgreSQL
- **GPU**: CUDA-capable GPU recommended for `triage`, `auto-tag`, and `audit-ratings` (falls back to CPU)
- **System Libraries & Optional CLI Fallbacks**:
  - `libvips` (required by `pyvips` for fast WebP thumbnails and Danbooru pixel hashing)
  - `7z` / `7zz` (optional: peeks inside `.7z`/`.rar`/`.cbr` archives during `prep-paid`)
  - `magick` (ImageMagick) and `ffmpeg` / `ffprobe` (optional last-resort fallbacks if `pyvips`/PyAV fail on an odd file)

---

## 💿 Installation

```bash
git clone [https://github.com/Miyuutsu/shimmie2-tools.git](https://github.com/Miyuutsu/shimmie2-tools.git)
cd shimmie2-tools
uv venv venv
source venv/bin/activate
uv pip install -r requirements.txt

```

*(GPU setup: install the PyTorch wheel matching your hardware — NVIDIA CUDA, AMD ROCm on Linux via PyTorch's ROCm index, or `torch-directml` on Windows. `shimmie_tool.py` auto-detects CUDA/ROCm, Intel XPU, Apple MPS, DirectML, and CPU, and supports `SHIMMIE_DEVICE=cpu|cuda|dml` and `SHIMMIE_DTYPE=fp32|bf16|fp16` overrides if needed.)*

### Optional Shell Tab-Completion

```bash
# Zsh (add to ~/.zshrc)
eval "$(./shimmie_tool.py --completion zsh)"

# Bash (add to ~/.bashrc)
eval "$(./shimmie_tool.py --completion bash)"

```

---

## 🚀 Common Workflows

### 1. Paid Archive Pipeline (`prep-paid` -> `triage` -> `make-csv` -> `sync-pools` -> `cleanup-paid`)

Pass `--xattr` (read/write) or `--xattr-read` (read-only) to cache validated MD5, SHA-256, dHash, media metadata, and Danbooru pixel hashes in filesystem extended attributes (`user.shimmie.*`), eliminating repeat mechanical HDD reads across runs.

```bash
# 1. Stage up to 50k files (stops cleanly at an artist boundary)
./shimmie_tool.py prep-paid \
  --path "/path/to/pawchive/fanbox" \
  --out "/path/to/staging/fanbox_01" \
  --max-files 50000 \
  --xattr

# 2. Triage banners, fluff, comics, and downscaled duplicates + generate AI sidecars
./shimmie_tool.py triage \
  --path "/path/to/staging/fanbox_01" \
  --spath "/path/to/shimmie2" \
  --threads 12 \
  --dedup-downscales \
  --xattr

# 3. Stamp paid_reward, artist:<name>, and source:<url> onto surviving sidecars
./shimmie_tool.py prep-paid \
  --stamp-sidecars \
  --out "/path/to/staging/fanbox_01"

# 4. Import directly into Shimmie2 (sorted sequentially for HDD throughput)
./shimmie_tool.py make-csv \
  --path "/path/to/staging/fanbox_01" \
  --spath "/path/to/shimmie2" \
  --threads 4 --batch 100 \
  --direct-import --update-cache \
  --xattr

# 5. Build/update chronological per-artist Pools
./shimmie_tool.py sync-pools --spath "/path/to/shimmie2" --dry-run
./shimmie_tool.py sync-pools --spath "/path/to/shimmie2"

# 6. Safely move MD5-verified originals to <source>-reviewed (preview -> apply -> prune DB)
"/path/to/staging/fanbox_01/cleanup_staged.sh" dry-run  --spath "/path/to/shimmie2"
"/path/to/staging/fanbox_01/cleanup_staged.sh" apply    --spath "/path/to/shimmie2"
"/path/to/staging/fanbox_01/cleanup_staged.sh" prune-db

```

### 2. Standard Batch Import (`make-csv` or `import-csv`)

```bash
# Direct import into Shimmie2
./shimmie_tool.py make-csv --path "/path/to/media" --spath "/path/to/shimmie2" --direct-import --xattr

# Or emit chunked CSVs + thumbnails and import separately
./shimmie_tool.py make-csv --path "/path/to/media" --spath "/path/to/shimmie2" --thumbnail
./shimmie_tool.py import-csv --csv "/path/to/media" --spath "/path/to/shimmie2"

```

### 3. Retroactive Curation & Rating Audit

```bash
./shimmie_tool.py curate-db --spath "/path/to/shimmie2"
./shimmie_tool.py audit-ratings --spath "/path/to/shimmie2" --model eva02 --batch 20
./shimmie_tool.py update-ratings --spath "/path/to/shimmie2"

```

---

## 🔗 Folder Structure

```text
shimmie2-tools/
├── database/                      # SQLite reference dictionaries & local state DBs
│   ├── artists.db                 # Artist alias -> canonical artist map
│   ├── characters.db              # Character -> series map
│   ├── tag_rating_dominant.db     # Tag rating weights
│   ├── tagmap.db                  # Dynamic sidecar -> canonical tag mappings
│   ├── posts_cache.db             # Local Danbooru/post metadata cache (external download)
│   ├── timeline_index.db          # Shareable chronological paid-media index (auto-created)
│   ├── staging_state.db           # Local staging/resume/cleanup state (auto-created)
│   └── ai_quarantine_cache.db     # Raw AI confidence log from audit-ratings (auto-created)
├── functions/                     # Core shared libraries
│   ├── ai_tagger.py               # In-process SmilingWolf v3 GPU tagger & frame aggregator
│   ├── common.py                  # Paths, xattr cache, hashing, disk sorting, blacklist parser
│   ├── db_cache.py                # Postgres & SQLite helpers, tagmap.db & dictionary loaders
│   ├── media.py                   # PyVips, Pillow, PyAV, ImageMagick & FFmpeg utilities
│   ├── source_resolver.py         # Canonical URL resolver & structured prefix normalization
│   ├── tags_curation.py           # Tag expansion, enrichment, parent expansion & ratings
│   └── tags_mining.py             # Jaccard co-occurrence tag equivalency miner
├── tools/                         # CLI command handlers
│   ├── ai_auditor.py
│   ├── auto_tagger.py
│   ├── cleanup_paid.py
│   ├── csv2sqlite.py
│   ├── csv_builder.py
│   ├── curate_db.py
│   ├── fix_map.py
│   ├── generate_db_map.py
│   ├── importer.py
│   ├── prep_paid.py
│   ├── purge.py
│   ├── sync_pools.py
│   ├── tag_dump.py
│   ├── triage.py
│   └── update_ratings.py
├── tag_groups.json                # Danbooru tag hierarchy for parent expansion & curation
├── pyproject.toml                 # Project metadata & Ruff configuration
├── requirements.txt               # Python dependencies
└── shimmie_tool.py                # Master CLI entry point

```

---

## 🗄️ Database Files

Pre-built reference database files are available on Google Drive (current as of **April 19, 2025**, using the Danbooru API and the [nyanko7/danbooru2023 `posts.json` dump](https://huggingface.co/datasets/nyanko7/danbooru2023/blob/main/metadata/posts.json)):

* `posts_cache.db` (~4.1 GB)

Place downloaded `.db` files directly into `database/`:

🔗 **[Download Database files on Google Drive](https://drive.google.com/drive/folders/106pn_tpW4QgpPj-kwHC4x6cvdiqw5MaH?usp=drive_link)**

---

## 📜 License

This project is licensed under the **GNU General Public License v3.0**. See the `LICENSE` file for details.

---

## 💖 Credits

* **[Danbooru](https://danbooru.donmai.us/)** — for their rich metadata, API, and tag taxonomy
* **[Shimmie2](https://github.com/shish/shimmie2)** — for the core imageboard framework
* **[SmilingWolf](https://huggingface.co/SmilingWolf)** & **[OpenCLIP / timm](https://github.com/mlfoundations/open_clip)** — for the WD v3 tagging models and SigLIP zero-shot classification
* **[SD-Tag-Editor](https://github.com/derrian-distro/SD-Tag-Editor)** — for the original `tag_groups.json` hierarchy
* **[Babyforce](https://github.com/Babyforce)** — for the original `tag_rating_dominant.db`
* **[ChatGPT](https://chatgpt.com) & Gemini** — for keeping the code clean and the refactoring heavily caffeinated
* All contributors and users 💜
