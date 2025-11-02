# Shimmie2 Tools

A modular suite of utilities and scripts designed to extend and enhance the functionality of Shimmie2, with a focus on Danbooru-based metadata and large-scale automation workflows.

---

## ✨ Features

- **📄 Danbooru Wiki Importer**
  Converts and imports Danbooru wiki pages into Shimmie2’s database with support for Shimmie-style formatting and cleanup rules.

- **⚡ Metadata Caching**
  Parses Danbooru `posts.json` into a fast SQLite lookup for tag fallback and offline support.

---

## 🧠 Requirements

- Python 3.12
- Linux, Windows (partial support, not regularly maintained), or WSL

---

## 💿 Manual Installation

```bash
git clone https://github.com/Miyuutsu/shimmie2-tools.git
cd shimmie2-tools
```
Create a venv and install the requirements.

---

## 🚀 Usage

### 🛠 CLI Tools

All scripts are located in the `scripts/` directory.

#### Create CSV from various data

```bash
python scripts/booru_csv_maker.py --batch_size=20 \
--threads=8 --cache=database/posts_cache.db \
--character_db=database/characters.db
```

#### Precache posts.json into SQLite

```bash
python backend/scripts/precache_posts_sqlite.py input/posts.json \
-o backend/database/posts_cache.db --threads 8
```

#### Import Danbooru wikis

```bash
python backend/scripts/import_danbooru_wikis.py --user={dbuser} \
--db={shimmiedb} --pages=20 --convert=shimmie
```

---

## 🔗 Example Folder Structure

```
shimmie2-tools/
├── database/
│   ├── characters.db
│   ├── danbooru_wiki_cache.db
│   ├── posts_cache.db
│   └── tag_rating_dominant.db
├── scripts/
│   ├── booru_csv_maker.py
│   ├── import_danbooru_wikis.py
│   └── precache_posts_sqlite.py
├── requirements.txt
```

---

## 📘 Additional Documentation

### 🧪 Development Notes

- Wiki imports support resume and smart `--update-existing`

### 🗄️ Database Files

Pre-built database files are current as of **April 19, 2025**, using
[nyanko7/danbooru2023 `posts.json`](https://huggingface.co/datasets/nyanko7/danbooru2023/blob/main/metadata/posts.json)
and the Danbooru API.

- `posts_cache.db`: 4.1GB
- `danbooru_wiki_cache.db`: 89.6MB
Place them into `database/`.

🔗 [Database files on Google Drive](https://drive.google.com/drive/folders/106pn_tpW4QgpPj-kwHC4x6cvdiqw5MaH?usp=drive_link)

---

## 📜 License

This project is licensed under the **GNU General Public License v3.0**.
You are free to use, modify, and distribute it under the same terms.
See the [License](LICENSE) for details.

---

## 💖 Credits

- [Danbooru](https://danbooru.donmai.us/) — for their rich metadata and API
- [Shimmie2](https://github.com/shish/shimmie2) — for the core imageboard framework
- [ChatGPT](https://chatgpt.com) — because odds are good I couldn't have done this without you
- [Babyforce](https://github.com/Babyforce) - For the tag_rating_dominant.db that was used
- All contributors and users 💜

---

❤️☕ Built with care and plenty of caffeinated determination.
