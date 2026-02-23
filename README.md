# Shimmie2 Master Toolsuite

A consolidated, modular CLI application designed to manage, extend, and automate Shimmie2 database workflows. This toolsuite handles everything from massive batch CSV generation to local SQLite precaching and Danbooru wiki migrations.

---

## ✨ Features

The original standalone scripts have been unified into a single powerful entry point (`shimmie_tool.py`) with specialized commands:

- **`make-csv`**: Generates bulk import CSVs for Shimmie2 with automated tag curation, source resolution, thumbnail generation, and dynamic tag mining.
- **`wiki-index`**: Creates and alphabetically sorts wiki index pages directly from the Shimmie2 database.
- **`import-wikis`**: Fetches Danbooru wiki pages, converts them to Shimmie-compatible BBCode/HTML, and imports them seamlessly.
- **`precache`**: Parses massive Danbooru `posts.json` dumps into a fast, indexed SQLite database for rapid local tag lookups.
- **`csv2sqlite`**: Utility command to convert any standard CSV into an SQLite database.
- **`update-ratings`**: Batch updates existing Shimmie image ratings based on dominant tag weights.

---

## 🧠 Requirements

- **Python 3.12+**
- **OS**: Linux (perfect for an Arch setup), Windows (partial support), or WSL
- **Dependencies**: `pyvips`, `Pillow`, `psycopg2`, `requests`, `tqdm` (See `requirements.txt`)
- **System Tools**: ImageMagick and FFmpeg (required for `make-csv` thumbnail generation)

---

## 💿 Installation

Clone the repository and set up your virtual environment:

```bash
git clone [https://github.com/Miyuutsu/shimmie2-tools.git](https://github.com/Miyuutsu/shimmie2-tools.git)
cd shimmie2-tools
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
python shimmie_tool.py make-csv --images /path/to/images/ --spath /var/www/shimmie2/ --thumbnail --batch 50

```

**2. Precache Danbooru posts.json**

```bash
python shimmie_tool.py precache input/posts.json -o database/posts_cache.db --threads 8

```

**3. Import Danbooru Wikis**

```bash
python shimmie_tool.py import-wikis --spath /var/www/shimmie2/ --start-page 1 --pages 20 --convert shimmie

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
│   ├── common.py
│   ├── db_cache.py
│   ├── media.py
│   ├── source_resolver.py
│   ├── tags_curation.py
│   └── tags_mining.py
├── tools/                      # CLI command handlers
│   ├── csv_builder.py
│   ├── db.py
│   └── wiki.py
├── shimmie_tool.py             # Master CLI entry point
└── requirements.txt

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

* **[suspicious link removed]** — for their rich metadata and API
* **[Shimmie2](https://github.com/shish/shimmie2)** — for the core imageboard framework
* **[ChatGPT](https://chatgpt.com) & Gemini** — for keeping the code clean and the refactoring heavily caffeinated
* **[Babyforce](https://github.com/Babyforce)** — for the original `tag_rating_dominant.db`
* All contributors and users 💜

---

❤️☕ Built with care and plenty of caffeinated determination.
