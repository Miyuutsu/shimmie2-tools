"""Media processing functions (PyVips, Pillow, PyAV, ImageMagick, FFmpeg)."""
import hashlib
import io
import json
import random
import subprocess
from pathlib import Path
import av
import pyvips
from PIL import Image, ImageFile

ImageFile.LOAD_TRUNCATED_IMAGES = True

video_exts = {".gif", ".webm", ".mp4", ".flv", ".m4v", ".f4v", ".f4p", ".ogv"}


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
    """Get image dimensions using pyvips -> PIL -> ImageMagick fallback."""
    try:
        img = pyvips.Image.new_from_file(str(image_path), fail_on="none")
        return img.width, img.height
    except Exception:  # pylint: disable=broad-exception-caught
        pass

    try:
        with Image.open(image_path) as img:
            return img.size
    except Exception:  # pylint: disable=broad-exception-caught
        pass

    try:
        cmd = ["magick", "identify", "-format", "%w,%h", str(image_path)]
        res = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip()
        parts = res.split(",")
        if len(parts) >= 2:
            return int(parts[0]), int(parts[1])
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"\nError getting resolution for {image_path}: {e}")

    return None, None


def process_webp(task):
    """Process an image or video into a WebP thumbnail."""
    src_path, dst_path = task

    if Path(src_path).suffix.lower() in video_exts:
        try:
            extract_video_thumbnail(src_path, dst_path)
        except Exception as e:  # pylint: disable=broad-exception-caught
            print(f"\nError creating video thumbnail for {src_path}! ({e})")
    else:
        try:
            convert_to_webp(src_path, dst_path)
            return
        except Exception:  # pylint: disable=broad-exception-caught
            pass

        try:
            fallback_to_webp(src_path, dst_path)
            return
        except Exception:  # pylint: disable=broad-exception-caught
            pass

        try:
            magick_to_webp(src_path, dst_path)
        except Exception as e:  # pylint: disable=broad-exception-caught
            print(f"\nError creating thumbnail of {src_path}! ({type(e).__name__}: {e})")


def convert_to_webp(src_path: Path, dst_path: Path):
    """Tier 1: Convert images natively using pyvips."""
    dst_path = Path(dst_path)
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    image = pyvips.Image.thumbnail(
        str(src_path), 512, height=512, size="down", fail_on="none"
    )
    image.webpsave(str(dst_path), Q=92)


def fallback_to_webp(src_path: Path, dst_path: Path):
    """Tier 2: In-memory fallback using Pillow."""
    dst_path = Path(dst_path)
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    with open(src_path, "rb") as f:
        data = f.read()

    im = Image.open(io.BytesIO(data))
    im.load()
    im.thumbnail((512, 512), Image.Resampling.LANCZOS)
    im.save(dst_path, "webp", quality=92, method=6)


def magick_to_webp(src_path: Path, dst_path: Path):
    """Tier 3: Last-resort fallback using ImageMagick CLI."""
    dst_path = Path(dst_path)
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = ["magick", str(src_path), "-resize", "512x512>", "-quality", "92", f"webp:{dst_path}"]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def get_video_resolution(file_path: Path):
    """Extracts resolution from a video/gif using PyAV, falling back to ffprobe."""
    try:
        with av.open(str(file_path)) as container:
            if container.streams.video:
                stream = container.streams.video[0]
                return stream.width, stream.height
    except Exception:  # pylint: disable=broad-exception-caught
        pass

    try:
        cmd = [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=width,height", "-of", "json",
            str(file_path)
        ]
        output = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip()
        data = json.loads(output)

        if "streams" in data and len(data["streams"]) > 0:
            stream = data["streams"][0]
            return int(stream.get("width")), int(stream.get("height"))
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"\nError getting resolution for {file_path}: {e}")

    return None, None


def extract_video_frames(src_path: Path, max_frames: int = 5) -> list[Image.Image]:
    """Extracts up to `max_frames` PIL Images from a video or GIF natively using PyAV."""
    src_path = Path(src_path)
    frames = []

    with av.open(str(src_path)) as container:
        if not container.streams.video:
            return frames

        stream = container.streams.video[0]
        #stream.thread_type = "AUTO"

        # Single slightly-randomized thumbnail (15% to 35% into the video)
        pcts = (
            [random.uniform(0.15, 0.35)] if max_frames == 1
            else [0.10 + (0.80 * i / (max_frames - 1)) for i in range(max_frames)]
        )

        # Fast-path seeking if the container exposes a valid stream or container duration
        if stream.duration and stream.duration > 0:
            for pct in pcts:
                target_pts = int(stream.duration * pct)
                container.seek(target_pts, stream=stream, backward=True, any_frame=False)
                for frame in container.decode(stream):
                    frames.append(frame.to_image())
                    break
        elif container.duration and container.duration > 0:
            for pct in pcts:
                # container.duration is in AV_TIME_BASE (microseconds)
                target_ts = int(container.duration * pct)
                container.seek(target_ts, backward=True, any_frame=False)
                for frame in container.decode(stream):
                    frames.append(frame.to_image())
                    break

        # Fallback for GIFs / WebMs without seekable duration headers:
        # Decode frames sequentially and sample up to `max_frames` evenly
        if not frames:
            container.seek(0)
            all_decoded = []
            for idx, frame in enumerate(container.decode(stream)):
                all_decoded.append(frame.to_image())
                if idx >= 150:  # Cap sequential decode to prevent OOM on long headerless streams
                    break

            if all_decoded:
                if max_frames == 1:
                    pick_idx = int(len(all_decoded) * random.uniform(0.15, 0.35))
                    frames.append(all_decoded[min(pick_idx, len(all_decoded) - 1)])
                elif len(all_decoded) <= max_frames:
                    frames = all_decoded
                else:
                    step = (len(all_decoded) - 1) / (max_frames - 1)
                    frames = [all_decoded[int(round(i * step))] for i in range(max_frames)]

    return frames


def extract_video_thumbnail(src_path: Path, dst_path: Path):
    """Extracts a slightly randomized frame (15%-35%) and saves it as a 512x512 WebP thumbnail."""
    src_path = Path(src_path)
    dst_path = Path(dst_path)
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    # Tier 1: Native PyAV + Pillow
    try:
        frames = extract_video_frames(src_path, max_frames=1)
        if frames:
            im = frames[0]
            im.thumbnail((512, 512), Image.Resampling.LANCZOS)
            im.save(dst_path, "webp", quality=92, method=6)
            return
    except Exception:  # pylint: disable=broad-exception-caught
        pass

    # Tier 2: FFmpeg CLI Fallback
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
