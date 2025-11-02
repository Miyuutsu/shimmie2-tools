import os
import sys
import hashlib
from pathlib import Path
import sqlite3

def validate_float(value):
    """Validate if a number is a float between 0.00 and 1.00 and multiple of 0.01"""
    value = float(value)  # Ensure the value is a float
    if value < 0.00 or value > 1.00:
        raise ValueError(f"Value must be between 0.00 and 1.00. Given: {value}")
    if round(value*100) % 1 != 0:
        raise ValueError(f"Value must be a multiple of 0.01. Given: {value}")
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
    """
    Converts CDN links to more descriptive URLs. Currently supports Pixiv and Fantia.

    Args:
        image_url (str): The image URL.

    Returns:
        str: The converted URL.  If the URL is not a known CDN link, it is returned as it.

    Raises:
        TypeError: if input is not a string.
    """
    import re
    if not isinstance(image_url, str):
        raise TypeError(f"Input image URL must be a string. Received {type(image_url)}")

    pixiv_pattern = r"(?:i|img)\d{0,5}\.(?:pximg|pixiv)\.net/(?:(?:img-original|img\d{1,5})/img/|img/)(?:\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2}/)?(?:[^/]+/)?(\d+)(?:_(?:[\w]+_)?p\d{1,3})?\.(?:jpg|jpeg|png|webp)"
    fantia_pattern = r"c\.fantia\.jp/uploads/post/file/(\d+)/"
    pixiv_match = re.search(pixiv_pattern, image_url)
    if pixiv_match:
        artwork_id = pixiv_match.group(1)
        return f"https://www.pixiv.net/en/artworks/{artwork_id}"
    fantia_match = re.search(fantia_pattern, image_url)
    if fantia_match:
        post_id = fantia_match.group(1)
        return f"https://fantia.jp/posts/{post_id}"

    return image_url

def compute_md5(image_path: Path) -> str:
    """
    Calculates the MD5 hash of a file.

    Args:
        image_path (Path): Path to the image.

    Returns:
        str: md5 hash of the file
    """
    hash_md5 = hashlib.md5()
    with open(image_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    md5_hash = hash_md5.hexdigest()
    return md5_hash
