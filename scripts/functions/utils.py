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


# Dictionaries
model_map = {
"vit": "SmilingWolf/wd-vit-tagger-v3",
"vit-large": "SmilingWolf/wd-vit-large-tagger-v3",
"swinv2": "SmilingWolf/wd-swinv2-tagger-v3",
"convnext": "SmilingWolf/wd-convnext-tagger-v3",
}

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

def convert_pixiv_link(image_url):
    import re
    #Converts a Pixiv CDN image URL to its corresponding artwork page URL.
    pattern = r"i\.pximg\.net/img-original/img/\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2}/(\d+)_p\d{1,3}\.(?:jpg|jpeg|png|webp)"
    match = re.search(pattern, image_url)
    if match:
        artwork_id = match.group(1)
        return f"https://www.pixiv.net/en/artworks/{artwork_id}"
    else:
        return image_url  # return the original if no match
