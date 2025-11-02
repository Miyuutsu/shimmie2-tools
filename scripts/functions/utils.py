import os
import sys
from pathlib import Path

def validate_float(value):
    """Validate the float value."""
    value = float(value)  # Ensure the value is a float
    # Check if value is between 0.00 and 1.00
    if value < 0.00 or value > 1.00:
        raise ValueError(f"Value must be between 0.00 and 1.00. Given: {value}")

    # Check if the value is a multiple of 0.01
    if round(value * 100) % 1 != 0:
        raise ValueError(f"Value must be between 0.00 and 1.00. Given: {value}")

    return value

def get_cpu_threads():
    return os.cpu_count()

def add_module_path(relative_path: str):
    """
    Adds the given relative path to sys.path if it's not already present.

    Args:
        relative_path (str): The relative path from the current script to the module folder.
    """
    script_dir = Path(__file__).parent.resolve()
    module_path = (script_dir / relative_path).resolve()

    if not module_path.exists():
        raise FileNotFoundError(f"Module path does not exist: {module_path}")

    if str(module_path) not in sys.path:
        sys.path.append(str(module_path))

def convert_cdn_links(image_url):
    import re

    # Check for Pixiv CDN link
    pixiv_pattern = r"(?:i|img)\d{0,5}\.(?:pximg|pixiv)\.net/(?:(?:img-original|img\d{1,5})/img/|img/)(?:\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2}/)?(?:[^/]+/)?(\d+)(?:_(?:[\w]+_)?p\d{1,3})?\.(?:jpg|jpeg|png|webp)"
    pixiv_match = re.search(pixiv_pattern, image_url)
    if pixiv_match:
        artwork_id = pixiv_match.group(1)
        return f"https://www.pixiv.net/en/artworks/{artwork_id}"

    # Check for Fantia CDN link
    fantia_pattern = (
    r"c\.fantia\.jp/uploads/post/file/(\d+)/"
    )
    fantia_match = re.search(fantia_pattern, image_url)
    if fantia_match:
        post_id = fantia_match.group(1)
        return f"https://fantia.jp/posts/{post_id}"

    # If no match, return the original URL
    return image_url  # return the original if no match

def compute_md5(image_path: Path) -> str:
    hash_md5 = hashlib.md5()
    with open(image_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
