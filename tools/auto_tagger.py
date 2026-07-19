# pylint: disable=line-too-long,missing-function-docstring,too-many-locals,too-many-branches,too-many-statements
"""Tool to automatically generate missing sidecars using AI and DB state."""
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
import tqdm

from functions.db_cache import resolve_post, get_shimmie_db_credentials

SUBMODULE_PATH = Path(__file__).parent.parent / "SD-Tag-Editor"
CACHE_PATH = Path(__file__).parent.parent / "database" / "posts_cache.db"
ALLOWED_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".jxl", ".avif"}

def run_auto_tagger(args):
    image_dir = Path(args.images)
    if not image_dir.is_dir():
        print(f"[ERROR] Invalid image directory: {image_dir}")
        return

    venv_python = SUBMODULE_PATH / "venv" / "bin" / "python"
    if not venv_python.exists():
        print("[ERROR] SD-Tag-Editor venv not found.")
        return

    dbuser = None
    if args.spath:
        creds = get_shimmie_db_credentials(args.spath)
        if creds:
            dbuser = creds.get('user')

    all_images = [f for f in image_dir.rglob("*") if f.is_file() and f.suffix.lower() in ALLOWED_EXTS]
    missing_sidecars = [
        f for f in all_images
        if not f.with_suffix(".txt").exists() and not f.with_name(f.name + ".txt").exists()
    ]

    if not missing_sidecars:
        print("[✓] All images already have sidecars. No action needed.")
        return

    print(f"[INFO] Found {len(missing_sidecars)} images missing sidecars. Checking DB status...")

    known_images = []
    unknown_images = []

    for img in tqdm.tqdm(missing_sidecars, desc="Querying Databases"):
        # Resolve against your local SQLite Cache and Postgres
        _, post, _, _, exists = resolve_post(img, args.spath, True, dbuser, CACHE_PATH)
        if exists or post:
            known_images.append(img)
        else:
            unknown_images.append(img)

    print("\n[INFO] Segregation complete:")
    print(f"  - Known (Receives 'tagai:' shield): {len(known_images)}")
    print(f"  - Unknown (Fully trusted AI tags):  {len(unknown_images)}\n")

    with tempfile.TemporaryDirectory(prefix="auto_tag_") as tmpdir:
        tmp_path = Path(tmpdir)
        known_dir = tmp_path / "known"
        unknown_dir = tmp_path / "unknown"
        known_dir.mkdir()
        unknown_dir.mkdir()

        mapping = {} # Maps the generated .txt filename back to the original source path

        print("Staging symlinks...")
        for i, img in enumerate(known_images):
            sym_name = f"{img.stem}_{i}{img.suffix}"
            txt_name = f"{img.stem}_{i}.txt"
            os.symlink(img.resolve(), known_dir / sym_name)
            mapping[txt_name] = img.with_suffix(".txt")

        for i, img in enumerate(unknown_images):
            sym_name = f"{img.stem}_{i}{img.suffix}"
            txt_name = f"{img.stem}_{i}.txt"
            os.symlink(img.resolve(), unknown_dir / sym_name)
            mapping[txt_name] = img.with_suffix(".txt")

        base_cmd = [
            str(venv_python), "-u", str(SUBMODULE_PATH / "run.py"),
            f"--model={args.model}", f"--batch_size={args.batch}",
            f"--gen_threshold={args.gen_threshold}", f"--char_threshold={args.char_threshold}"
        ]

        if args.dir_as_artist:
            base_cmd.append("--dir_as_artist=True")

        if known_images:
            print("\n=== Processing Known Images ===")
            cmd = base_cmd + ["--tag_prefix=tagai:", str(known_dir)]
            subprocess.run(cmd, check=True)

            for txt_file in known_dir.glob("*.txt"):
                shutil.move(str(txt_file), str(mapping[txt_file.name]))

        if unknown_images:
            print("\n=== Processing Unknown Images ===")
            cmd = base_cmd + [str(unknown_dir)]
            subprocess.run(cmd, check=True)

            for txt_file in unknown_dir.glob("*.txt"):
                shutil.move(str(txt_file), str(mapping[txt_file.name]))

    print(f"\n[✓] Sidecar generation complete! Created {len(missing_sidecars)} new files.")
