"""Shared constants, paths, hashing utilities, and blacklist parsing."""

from collections.abc import Iterable
from dataclasses import dataclass, field
import hashlib
from pathlib import Path
import re

ROOT_DIR = Path(__file__).parent.parent.resolve()
DB_DIR = ROOT_DIR / 'database'
SUBMODULE_PATH = ROOT_DIR / 'SD-Tag-Editor'

ADB_PATH = DB_DIR / 'artists.db'
CACHE_PATH = DB_DIR / 'posts_cache.db'
CDB_PATH = DB_DIR / 'characters.db'
QUARANTINE_DB_PATH = DB_DIR / 'ai_quarantine_cache.db'
TAGMAP_DB_PATH = DB_DIR / 'tagmap.db'
TAG_DB_PATH = DB_DIR / 'tag_rating_dominant.db'

IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.webp', '.jxl', '.avif'}
VIDEO_EXTS = {'.gif', '.webm', '.mp4', '.flv', '.m4v', '.f4v', '.f4p', '.ogv'}

MD5_REGEX = re.compile(r'[a-fA-F0-9]{32}')


@dataclass(slots=True)
class BlacklistConfig:
	"""Holds parsed blacklist rules, whitelist preservation tags, and dry-run ignore tags."""

	rules: list[dict[str, list[str] | str]] = field(default_factory=list)
	preserve_tags: set[str] = field(default_factory=set)
	ignore_tags: set[str] = field(default_factory=set)
	prefilter_tags: set[str] = field(default_factory=set)


def compute_md5(file_path: Path) -> str:
	"""Calculates the MD5 hash of a file from disk."""
	hash_md5 = hashlib.md5()
	with file_path.open('rb') as f:
		for chunk in iter(lambda: f.read(65536), b''):
			hash_md5.update(chunk)
	return hash_md5.hexdigest()


def extract_or_compute_md5(file_path: Path) -> str:
	"""Extracts a 32-character hex MD5 from the filename stem if present, otherwise computes it."""
	if match := MD5_REGEX.search(file_path.stem):
		return match.group(0).lower()
	return compute_md5(file_path)


def load_blacklist(blacklist_path: Path) -> BlacklistConfig:
	"""Parses a blacklist.txt file including #//whitelist: and #//ignore: directives."""
	if not blacklist_path.is_file():
		raise FileNotFoundError(f'Blacklist file not found: {blacklist_path.resolve()}')

	config = BlacklistConfig()

	for line in blacklist_path.read_text(encoding='utf-8').splitlines():
		raw_line = line.strip().lower()
		compact = raw_line.replace(' ', '')

		if compact.startswith('#//whitelist:'):
			whitelist_body = raw_line.split('whitelist:', 1)[1].split('#//', 1)[0]
			for tag in whitelist_body.split(','):
				if clean_tag := tag.strip():
					config.preserve_tags.add(clean_tag)
			continue

		if compact.startswith('#//ignore:'):
			ignore_body = raw_line.split('ignore:', 1)[1].split('#//', 1)[0]
			for tag in ignore_body.split(','):
				if clean_tag := tag.strip():
					config.ignore_tags.add(clean_tag)
			continue

		clean_line = raw_line.split('#//', 1)[0].strip()
		if not clean_line:
			continue

		pos_tags: list[str] = []
		neg_tags: list[str] = []
		for token in clean_line.split():
			if token.startswith('-'):
				if token[1:]:
					neg_tags.append(token[1:])
			else:
				pos_tags.append(token)
				config.prefilter_tags.add(token)

		if not pos_tags:
			raise ValueError(f"Invalid blacklist rule '{clean_line}': must include at least one positive tag.")

		config.rules.append({
			'pos': pos_tags,
			'neg': neg_tags,
			'raw': clean_line,
		})

	return config


def expand_tags_for_blacklist(tags: Iterable[str]) -> set[str]:
	"""Expands a tag iterable with unprefixed and un-qualified stems for blacklist evaluation."""
	clean_tags = set(tags)
	for tag in tags:
		if tag.startswith(('tagai:', 'booru:')):
			continue
		clean_tags.add(tag)
		base_tag = tag.split(':', 1)[1] if ':' in tag else tag
		clean_tags.add(base_tag)
		if '_(' in base_tag and base_tag.endswith(')'):
			clean_tags.add(base_tag.split('_(', 1)[0])
	return clean_tags
