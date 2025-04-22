import os

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
