# Shimmie2 Tools

A modular suite of utilities and scripts designed to extend and enhance the functionality of Shimmie2, with a focus on Danbooru-based metadata and large-scale automation workflows.

## ✨ Features

- **📄 Danbooru Wiki Importer**  
  Converts and imports Danbooru wiki pages into Shimmie2’s database with support for Shimmie-style formatting and cleanup rules.

- **⚡ Metadata Caching**  
  Parses Danbooru `posts.json` into a fast SQLite lookup for tag fallback and offline support.

- **🖼️ Tagger Interface**  
  Leverages advanced taggers to annotate images using fallback caches, external `.txt` files, or Danbooru metadata. Includes `--shimmie` export mode for CSV compatibility.

- **🪟 Unified GUI**  
  Launch tools, monitor logs, and manage processes safely — all from a Tkinter-based desktop interface.

## 🧠 Requirements

- Python 3.11 for tagger submodule
- Python 3.8+ for everything else
- Linux, Windows (partial support, not regularly maintained), or WSL
- Git (to clone the SD-Tag-Editor submodule)

## 📦 Installation

Save `run.sh` or `run.bat` to the directory of your choosing and run it. It won't work unless the directory is empty.

### 📦 Manual Installation

```bash
git clone https://github.com/Miyuutsu/shimmie2-tools.git
cd shimmie2-tools
chmod +x run.sh
```

## 🚀 Usage

### 🖥 GUI Mode

```bash
./run.sh
```

Or on Windows:

```bat
run.bat
```

This will:

- Install required dependencies (if missing)
- Initialize the SD-Tag-Editor submodule (if missing)
- Launch the Tkinter GUI

### 🛠 CLI Tools

All scripts are located in the `tools/` directory. Examples:

```bash
# Create CSV from various data
python tools/booru_csv_maker.py --batch_size=20 --model=vit-large --gen_threshold=0.35 --rating_threshold=0.35 --char_threshold=0.75 --subfolder=True --shimmie=True --no_prune=True --threads=16 --input_cache=tools/data/posts_cache.db
# Precache posts.json into SQLite
python tools/precache_posts_sqlite.py input/posts.json -o tools/data/posts_cache.db --threads 8

# Import Danbooru wikis
python tools/import_danbooru_wikis.py --user={dbuser} --db={shimmiedb} --pages=20 --convert=shimmie
```

### 🔗 Example Folder Structure

```
shimmie2-tools/
├── input/
│   └──  posts.json
├── tools/
│   ├── data/
│   │   ├── danbooru_character_webui.csv
│   │   ├── danbooru_wiki_cache.db
│   │   ├── posts_cache.db
│   │   └── SD-Tag-Editor/
│   ├── booru_csv_maker.py
│   ├── gui.py
│   ├── import_danbooru_wikis.py
│   ├── precache_posts_sqlite.py
│   └── requirements.txt
├── run.sh
└── run.bat
```

## 🧪 Development Notes

- The GUI supports safe abortion of long-running processes
- Wiki imports support resume and smart `--update-existing`
- Tags, metadata, and thresholds are configurable in GUI mode
- `.gitignore` excludes all runtime cache files and submodule-generated artifacts

## 📜 License

This project is licensed under the GNU General Public License v3.0.

You are free to use, modify, and distribute it under the same terms.
See the [License](LICENSE) for details.

---

💖 Credits
- [Danbooru](https://danbooru.donmai.us/) for their rich metadata and API
- [Shimmie2](https://github.com/shish/shimmie2) for the core imageboard framework
- [SD-Tag-Editor](https://github.com/derrian-distro/SD-Tag-Editor)
- All contributors and users 💜

🛠 Built with care and plenty of caffeinated determination.
