"""Media processing functions (PyVips, Pillow, PyAV, ImageMagick, FFmpeg)."""

import hashlib
import json
from pathlib import Path
import random
import subprocess

import av
from PIL import Image, ImageFile
import pyvips

from functions.common import VIDEO_EXTS

ImageFile.LOAD_TRUNCATED_IMAGES = True


def compute_danbooru_pixel_hash(image_path: Path) -> str:
	"""Computes Danbooru's specific pixel hash for image deduplication."""
	image = pyvips.Image.new_from_file(str(image_path), access='sequential')

	if image.get_typeof('icc-profile-data') != 0:
		image = image.icc_transform('srgb')
	if image.interpretation != 'srgb':
		image = image.colourspace('srgb')
	if not image.hasalpha():
		image = image.addalpha()

	header = (
		b'P7\n'
		+ f'WIDTH {image.width}\n'.encode()
		+ f'HEIGHT {image.height}\n'.encode()
		+ f'DEPTH {image.bands}\n'.encode()
		+ b'MAXVAL 255\n'
		+ b'TUPLTYPE RGB_ALPHA\n'
		+ b'ENDHDR\n'
	)

	raw_bytes = image.write_to_memory()
	return hashlib.md5(header + raw_bytes).hexdigest()


def get_image_resolution(image_path: Path) -> tuple[int | None, int | None]:
	"""Gets image dimensions using pyvips -> Pillow -> ImageMagick fallback."""
	try:
		img = pyvips.Image.new_from_file(str(image_path), fail_on='none')
		return img.width, img.height
	except Exception:
		pass

	try:
		with Image.open(image_path) as img:
			return img.size
	except Exception:
		pass

	try:
		cmd = ['magick', 'identify', '-format', '%w,%h', str(image_path)]
		res = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip()
		parts = res.split(',')
		if len(parts) >= 2:
			return int(parts[0]), int(parts[1])
	except Exception as e:
		print(f'\nError getting resolution for {image_path}: {e}')

	return None, None


def process_webp(task: tuple[Path, Path]) -> None:
	"""Processes an image or video into a 512x512 WebP thumbnail."""
	src_path, dst_path = Path(task[0]), Path(task[1])

	if src_path.suffix.lower() in VIDEO_EXTS:
		try:
			extract_video_thumbnail(src_path, dst_path)
		except Exception as e:
			print(f'\nError creating video thumbnail for {src_path}! ({e})')
		return

	for converter in (convert_to_webp, fallback_to_webp, magick_to_webp):
		try:
			converter(src_path, dst_path)
			return
		except Exception as e:
			last_err = e

	print(f'\nError creating thumbnail of {src_path}! ({type(last_err).__name__}: {last_err})')


def convert_to_webp(src_path: Path, dst_path: Path) -> None:
	"""Tier 1: Converts images natively using pyvips."""
	dst_path.parent.mkdir(parents=True, exist_ok=True)
	image = pyvips.Image.thumbnail(
		str(src_path), 512, height=512, size='down', fail_on='none'
	)
	image.webpsave(str(dst_path), Q=92)


def fallback_to_webp(src_path: Path, dst_path: Path) -> None:
	"""Tier 2: Fallback using Pillow."""
	dst_path.parent.mkdir(parents=True, exist_ok=True)
	with Image.open(src_path) as im:
		im.load()
		im.thumbnail((512, 512), Image.Resampling.LANCZOS)
		im.save(dst_path, 'webp', quality=92, method=6)


def magick_to_webp(src_path: Path, dst_path: Path) -> None:
	"""Tier 3: Last-resort fallback using ImageMagick CLI."""
	dst_path.parent.mkdir(parents=True, exist_ok=True)
	cmd = ['magick', str(src_path), '-resize', '512x512>', '-quality', '92', f'webp:{dst_path}']
	subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def get_video_resolution(file_path: Path) -> tuple[int | None, int | None]:
	"""Extracts resolution from a video/GIF using PyAV, falling back to ffprobe."""
	try:
		with av.open(str(file_path)) as container:
			if container.streams.video:
				stream = container.streams.video[0]
				return stream.width, stream.height
	except Exception:
		pass

	try:
		cmd = [
			'ffprobe', '-v', 'error', '-select_streams', 'v:0',
			'-show_entries', 'stream=width,height', '-of', 'json',
			str(file_path),
		]
		output = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip()
		data = json.loads(output)
		streams = data.get('streams') or []
		if streams:
			width = streams[0].get('width')
			height = streams[0].get('height')
			if width is not None and height is not None:
				return int(width), int(height)
	except Exception as e:
		print(f'\nError getting resolution for {file_path}: {e}')

	return None, None


def extract_video_frames(src_path: Path, max_frames: int = 5) -> list[Image.Image]:
	"""Extracts up to `max_frames` PIL Images from a video or GIF natively using PyAV."""
	src_path = Path(src_path)
	frames: list[Image.Image] = []

	with av.open(str(src_path)) as container:
		if not container.streams.video:
			return frames

		stream = container.streams.video[0]
		pcts = (
			[random.uniform(0.15, 0.35)]
			if max_frames == 1
			else [0.10 + (0.80 * i / (max_frames - 1)) for i in range(max_frames)]
		)

		if stream.duration and stream.duration > 0:
			for pct in pcts:
				target_pts = int(stream.duration * pct)
				container.seek(target_pts, stream=stream, backward=True, any_frame=False)
				for frame in container.decode(stream):
					frames.append(frame.to_image())
					break
		elif container.duration and container.duration > 0:
			for pct in pcts:
				target_ts = int(container.duration * pct)
				container.seek(target_ts, backward=True, any_frame=False)
				for frame in container.decode(stream):
					frames.append(frame.to_image())
					break

		# Fallback for GIFs / WebMs without seekable duration headers
		if not frames:
			container.seek(0)
			all_decoded: list[Image.Image] = []
			for idx, frame in enumerate(container.decode(stream)):
				all_decoded.append(frame.to_image())
				if idx >= 150:
					break

			if all_decoded:
				if max_frames == 1:
					pick_idx = int(len(all_decoded) * random.uniform(0.15, 0.35))
					frames.append(all_decoded[min(pick_idx, len(all_decoded) - 1)])
				elif len(all_decoded) <= max_frames:
					frames = all_decoded
				else:
					step = (len(all_decoded) - 1) / (max_frames - 1)
					frames = [all_decoded[round(i * step)] for i in range(max_frames)]

	return frames


def extract_video_thumbnail(src_path: Path, dst_path: Path) -> None:
	"""Extracts a slightly randomized frame (15%-35%) and saves it as a 512x512 WebP thumbnail."""
	src_path = Path(src_path)
	dst_path = Path(dst_path)
	dst_path.parent.mkdir(parents=True, exist_ok=True)

	try:
		frames = extract_video_frames(src_path, max_frames=1)
		if frames:
			im = frames[0]
			im.thumbnail((512, 512), Image.Resampling.LANCZOS)
			im.save(dst_path, 'webp', quality=92, method=6)
			return
	except Exception:
		pass

	temp_webp_path = dst_path.with_name(f'{dst_path.name}.webp')
	cmd = [
		'ffmpeg', '-y', '-v', 'error', '-i', str(src_path),
		'-ss', '00:00:00.000', '-vframes', '1',
		'-vf', "scale='if(gt(iw,ih),512,-1)':'if(gt(iw,ih),-1,512)'",
		'-c:v', 'libwebp', '-lossless', '0', '-q:v', '92',
		str(temp_webp_path),
	]
	subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
	temp_webp_path.replace(dst_path)
