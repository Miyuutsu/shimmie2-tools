import sys
import hashlib
from pathlib import Path

def add_module_path(relative_path: str):
    """Adds the given relative path to sys.path if it's not already present."""
    script_dir = Path(__file__).parent.resolve()
    module_path = (script_dir / relative_path).resolve()
    if not module_path.exists():
        raise FileNotFoundError(f"Module path does not exist: {module_path}")
    if str(module_path) not in sys.path:
        sys.path.append(str(module_path))

def compute_md5(image_path: Path) -> str:
    """Calculates the MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(image_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
