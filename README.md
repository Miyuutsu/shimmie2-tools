# Shimmie2 Master Toolsuite

A consolidated, modular CLI application designed to manage, extend, and automate Shimmie2 database workflows. This toolsuite handles everything from massive batch CSV generation to local SQLite precaching and Danbooru wiki migrations.

---

## ✨ Features

The original standalone scripts have been unified into a single powerful entry point (`shimmie_tool.py`) with specialized commands:

- **`audit-ratings`**: AI-powered safety auditor using the `SD-Tag-Editor` submodule to scan thumbnails, upgrade ratings, and apply shadow tags (`tagai:`).
- **`csv2sqlite`**: Utility command to convert any standard CSV into an SQLite database.
- **`import-wikis`**: Fetches Danbooru wiki pages, converts them to Shimmie-compatible BBCode/HTML, and imports them seamlessly.
- **`make-csv`**: Generates bulk import CSVs for Shimmie2 with automated tag curation, source resolution, thumbnail generation, and dynamic tag mining.
- **`precache`**: Parses massive Danbooru `posts.json` dumps into a fast, indexed SQLite database for rapid local tag lookups.
- **`sync-wikis`**: Synchronizes offline wiki cache to your live Postgres database using custom HTML formatting.
- **`update-ratings`**: Batch updates existing Shimmie image ratings based on dominant tag weights.
- **`wiki-index`**: Creates and alphabetically sorts wiki index pages directly from the Shimmie2 database.

---

## 🧠 Requirements

- **Python 3.12+**
- **OS**: Linux, Windows (Unsupported but should work), or WSL
- **Dependencies**: `pyvips`, `Pillow`, `psycopg2`, `requests`, `tqdm` and of course Shimmie2
- **System Tools**: ImageMagick and FFmpeg (required for `make-csv` thumbnail generation)

---

## 💿 Installation

Clone the repository and set up your virtual environment:

```bash
git clone [https://github.com/Miyuutsu/shimmie2-tools.git](https://github.com/Miyuutsu/shimmie2-tools.git)
cd shimmie2-tools
git submodule update --init --recursive
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

```

---

## 🚀 Usage

All tools are accessed through the master router: `shimmie_tool.py`.

You can view the global help menu and a list of all commands:

```bash
python shimmie_tool.py --help-all

```

### Common Workflows

**1. Create a Shimmie2 Import CSV (with thumbnails)**

```bash
python shimmie_tool.py make-csv --images "/path/to/images/" --spath "/path/to/shimmie2/" --thumbnail --batch 50

```

**2. Precache Danbooru posts.json**

```bash
python shimmie_tool.py precache "input/posts.json" -o "database/posts_cache.db" --threads 8

```

**3. Import Danbooru Wikis**

```bash
python shimmie_tool.py import-wikis --spath "/path/to/shimmie2/" --start-page 1 --pages 20 --convert shimmie

```

**4. Audit Ratings with AI**

```bash
# Scan thumbnails and stage AI changes
python shimmie_tool.py audit-ratings --scan --spath "/path/to/shimmie2"
# Review staged changes
python shimmie_tool.py audit-ratings --review --spath "/path/to/shimmie2"
# Apply changes to Shimmie
python shimmie_tool.py audit-ratings --apply --spath "/path/to/shimmie2"
```

---

## 🔗 Folder Structure

```text
shimmie2-tools/
├── database/                   # SQLite caches and mapping databases
│   ├── artists.db
│   ├── characters.db
│   ├── danbooru_wiki_cache.db
│   ├── posts_cache.db
│   └── tag_rating_dominant.db
├── functions/                  # Core processing modules
│   ├── captcha.py
│   ├── common.py
│   ├── db_cache.py
│   ├── media.py
│   ├── source_resolver.py
│   ├── tags_curation.py
│   └── tags_mining.py
├── tools/                      # CLI command handlers
│   ├── ai_auditor.py
│   ├── csv_builder.py
│   ├── db.py
│   ├── images.py
│   └── wiki.py
├── SD-Tag-Editor/              # AI submodule (Fork)
├── requirements.txt
└── shimmie_tool.py             # Master CLI entry point

```

---

## 🗄️ Database Files

Pre-built database files are current as of **April 19, 2025**, using the Danbooru API and the [nyanko7/danbooru2023 `posts.json` dump](https://huggingface.co/datasets/nyanko7/danbooru2023/blob/main/metadata/posts.json).

Place these directly into your `database/` directory:

* `posts_cache.db`: 4.1GB
* `danbooru_wiki_cache.db`: 89.6MB

🔗 **[Download Database files on Google Drive](https://drive.google.com/drive/folders/106pn_tpW4QgpPj-kwHC4x6cvdiqw5MaH?usp=drive_link)**

---

## 📜 License

This project is licensed under the **GNU General Public License v3.0**. You are free to use, modify, and distribute it under the same terms. See the `LICENSE` file for details.

---

## 💖 Credits

* **[Danbooru](https://danbooru.donmai.us/)** — for their rich metadata and API
* **[Shimmie2](https://github.com/shish/shimmie2)** — for the core imageboard framework
* **[SD-Tag-Editor](https://github.com/derrian-distro/SD-Tag-Editor)** — for the AI tagging engine
* **[ChatGPT](https://chatgpt.com) & Gemini** — for keeping the code clean and the refactoring heavily caffeinated
* **[Babyforce](https://github.com/Babyforce)** — for the original `tag_rating_dominant.db`
* All contributors and users 💜

---

❤️☕ Built with care and plenty of caffeinated determination.
