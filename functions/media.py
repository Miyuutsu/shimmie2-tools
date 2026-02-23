"""Media processing functions (ImageMagick, FFmpeg, PyVips)."""
import io
import hashlib
import subprocess
from pathlib import Path
import pyvips
from PIL import Image

from functions.common import VIDEO_EXTS

def compute_danbooru_pixel_hash(image_path: Path) -> str:
    """Compute Danbooru's specific pixel hash for image deduplication."""
    image = pyvips.Image.new_from_file(str(image_path), access="sequential")

    if image.get_typeof("icc-profile-data") != 0:
        image = image.icc_transform("srgb")
    if image.interpretation != "srgb":
        image = image.colourspace("srgb")
    if not image.hasalpha():
        image = image.addalpha()

    header = (
        b"P7\n"
        + f"WIDTH {image.width}\n".encode()
        + f"HEIGHT {image.height}\n".encode()
        + f"DEPTH {image.bands}\n".encode()
        + b"MAXVAL 255\n"
        + b"TUPLTYPE RGB_ALPHA\n"
        + b"ENDHDR\n"
    )

    raw_bytes = image.write_to_memory()
    return hashlib.md5(header + raw_bytes).hexdigest()

def get_image_resolution(image_path: Path):
    """Get image dimensions using PIL, falling back to ImageMagick on corruption."""
    # Try PIL first (Fastest)
    try:
        with Image.open(image_path) as img:
            return img.size
    except Exception: # pylint: disable=broad-exception-caught
        pass

    # Fallback to ImageMagick (More robust for corrupt headers)
    try:
        cmd = ["magick", "identify", "-format", "%w,%h", str(image_path)]
        # stderr=DEVNULL prevents console spam on truly broken files
        res = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip()
        parts = res.split(',')
        if len(parts) >= 2:
            return int(parts[0]), int(parts[1])
    except Exception as e: # pylint: disable=broad-exception-caught
        print(f"Error getting resolution for {image_path}: {e}")

    return None, None

def process_webp(task):
    """Process an image or video into a WebP thumbnail."""
    src_path, dst_path = task

    if Path(src_path).suffix.lower() in VIDEO_EXTS:
        try:
            extract_video_thumbnail(src_path, dst_path)
        except subprocess.CalledProcessError as e:
            print(f"Error creating video thumbnail for {src_path}! ({e})")
    else:
        try:
            convert_to_webp(src_path, dst_path)
        except subprocess.CalledProcessError:
            try:
                fallback_to_webp(src_path, dst_path)
            except Exception as e: # pylint: disable=broad-exception-caught
                print(f"Error creating thumbnail of {src_path}! ({type(e).__name__}: {e})")

def convert_to_webp(src_path: Path, dst_path: Path):
    """Convert images using ImageMagick."""
    dst_path = Path(dst_path)
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = ["magick", str(src_path), "-resize", "512x512>", "-quality", "92", f"webp:{dst_path}"]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def fallback_to_webp(src_path: Path, dst_path: Path):
    """In-memory fallback using Pillow."""
    dst_path = Path(dst_path)
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    with open(src_path, "rb") as f:
        data = f.read()

    im = Image.open(io.BytesIO(data))
    im.load()
    im.thumbnail((512, 512), Image.Resampling.LANCZOS)
    im.save(dst_path, "webp", quality=92, method=6)

def get_video_resolution(file_path: Path):
    """Extracts resolution from a video/gif using ffprobe."""
    try:
        cmd = [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=width,height", "-of", "csv=s=x:p=0",
            str(file_path)
        ]
        output = subprocess.check_output(cmd, text=True).strip()
        if output and 'x' in output:
            parts = output.split('\n', maxsplit=1)[0].split('x')
            return int(parts[0]), int(parts[1])
    except Exception as e: #pylint: disable=broad-exception-caught
        print(f"Error getting resolution for {file_path}: {e}")
    return None, None

def extract_video_thumbnail(src_path: Path, dst_path: Path):
    """Extracts the first frame of a video and saves it directly as a WebP thumbnail via ffmpeg."""
    src_path = Path(src_path)
    dst_path = Path(dst_path)
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    temp_webp_path = dst_path.with_name(f"{dst_path.name}.webp")

    cmd = [
        "ffmpeg", "-y", "-v", "error", "-i", str(src_path),
        "-ss", "00:00:00.000", "-vframes", "1",
        "-vf", "scale='if(gt(iw,ih),512,-1)':'if(gt(iw,ih),-1,512)'",
        "-c:v", "libwebp", "-lossless", "0", "-q:v", "92",
        str(temp_webp_path)
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    temp_webp_path.replace(dst_path)
