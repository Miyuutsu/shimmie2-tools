"""Functions for parsing and resolving source URLs and normalizing structured tags."""

from collections.abc import Iterable
import re

STRIP_GENERAL_PREFIXES = {
	'female', 'male', 'mixed', 'other', 'general', 'tag', 'tags',
	'fetish', 'species', 'body', 'clothing', 'pose', 'setting',
	'location', 'genre', 'theme', 'lore', 'reclass',
	'action', 'sex', 'bodypart', 'splatoon_weapon',
}
SERIES_PREFIXES = {'copyright', 'parody', 'series'}
ARTIST_PREFIXES = {'creator', 'author', 'illustrator', 'circle', 'group', 'cosplayer', 'artist'}
FILE_FORMAT_PREFIXES = {'file_type', 'filetype', 'ext', 'format'}
JUNK_METADATA_PREFIXES = {
	'id', 'post', 'pool', 'parent', 'width', 'height',
	'filesize', 'score', 'favcount', 'md5', 'sha256', 'status',
	'filename', 'file_name', 'path',
	'page', 'comm', 'commission', 'cub_comm', 'ych', 'adopt_extra',
	'sr', 'ss', 'subreddit', 'date', 'e621_pool', 'uploader',
	'board', 'password_hex', 'f_upload_time', 'f_theme', 'chapter',
	'comic', 'tnc', 'title_full', 'title_jp', 'volume', 'color',
	'mention', 'type', 'unknown_tag', 'spoiler', 'album',
	'mastodon_id', 'invalid',
}
SOURCE_ID_PREFIXES = {
	'pixiv_work', 'pixiv_illust_id', 'pixiv_illust', 'pixiv_id', 'pixiv_illustration',
	'pixiv_profile', 'pixiv_user', 'pixiv_user_id', 'pixiv_artist_id',
	'nijie_work', 'gelbooru_id', 'e621_id', 'sankaku_id', 'patreon_id',
	'f_id', 'tweeter_id',
}
CORE_PASSTHROUGH_PREFIXES = {'character', 'source', 'booru', 'tagai', 'site', 'studio'}
RECOGNIZED_PREFIXES = (
	STRIP_GENERAL_PREFIXES
	| SERIES_PREFIXES
	| ARTIST_PREFIXES
	| FILE_FORMAT_PREFIXES
	| JUNK_METADATA_PREFIXES
	| SOURCE_ID_PREFIXES
	| CORE_PASSTHROUGH_PREFIXES
	| {
		'hydrus_invalid_tag', 'language', 'meta', 'metadata',
		'rating', 'http', 'https', 'website', 'meme', 'tagme',
		'[filename]',
	}
)

EH_SYNONYM_MAP = {
	'sole_female': '1girl',
	'sole_male': '1boy',
}
KNOWN_LANGUAGES = {
	'arabic', 'chinese', 'dutch', 'english', 'engrish', 'esperanto',
	'finnish', 'french', 'german', 'greek', 'hebrew', 'hindi',
	'hungarian', 'indonesian', 'inkling', 'italian', 'japanese',
	'korean', 'latin', 'polish', 'portuguese', 'romaji', 'russian',
	'simplified_chinese', 'spanish', 'swedish', 'tagalog', 'thai',
	'traditional_chinese', 'turkish', 'ukrainian', 'vietnamese',
}
BARE_LANGUAGES = KNOWN_LANGUAGES - {'latin', 'inkling'}

VALID_META_TAGS = {
	'mp4', 'webm', 'gif', 'apng', 'ugoira', 'swf', 'flv', 'm4v', 'ogv',
	'mov', 'mkv', 'avi', 'animated', 'animated_gif', 'animated_png',
	'video', 'audio', 'sound', 'voiced', 'no_sound', 'silent',
	'loop', 'looping', '30fps', '60fps', 'high_framerate', 'low_framerate',
	'lossless', 'lossy',
	'corrupted', 'has_censored_revision', 'has_uncensored_version',
	'jpeg_artifacts', 'md5_mismatch', 'resolution_mismatch',
	'paid_reward', 'paid_reward_available', 'patreon_username',
	'third-party_edit',
}
META_VALUE_REMAP = {
	'corrupted_twitter_file': 'meta:corrupted',
	'5koma': '5koma comic',
}
DROP_META_JUNK = {
	'bad_id', 'bad_pixiv_id', 'bad_twitter_id', 'bad_source', 'bad_link',
	'duplicate', 'non-repeating_animation',
	'asanagi_(style)', 'dagasi_(style)',
}
UNPREFIXED_CONTAINER_META = {
	'mp4', 'webm', 'apng', 'ugoira', 'swf', 'flv', 'm4v', 'ogv', 'mov', 'mkv', 'avi',
	'30fps', '60fps', 'lossless', 'lossy',
	'corrupted', 'has_censored_revision', 'has_uncensored_version',
	'md5_mismatch', 'resolution_mismatch',
}
KNOWN_BOORU_NAMES = {
	'danbooru', 'gelbooru', 'safebooru', 'realbooru', 'derpibooru',
	'manebooru', 'twibooru', 'atfbooru', 'e621', 'konachan',
	'yandere', 'sankaku', 'sankakucomplex', 'zerochan', 'rule34',
	'paheal', 'tbib', 'ponerpics',
}

SOURCE_PRIORITY = {
	'pixiv.net': 1, 'fantia.jp': 2, 'patreon.com': 3, 'nijie.info': 4,
	'misskey.io': 5, 'baraag.net': 6, 'pawoo.net': 7, 'x.com': 8,
	'twitter.com': 9, 'twimg.com': 10, 'tumblr.com': 11, 'gelbooru.com': 12,
	'e621.net': 13, 'danbooru.donmai.us': 14, 'furaffinity.net': 15, 'konachan.com': 16,
	'sankakucomplex.com': 17, 'yande.re': 18, 'pawchive.pw': 19, 'kemono.cr': 19,
}

FILENAME_SOURCE_PATTERNS = (
	(re.compile(r'fanbox/(\d+)/(\d+)_'), 'https://pawchive.pw/fanbox/user/{0}/post/{1}'),
	(re.compile(r'gelbooru_(\d+)_'), 'https://gelbooru.com/index.php?page=post&s=view&id={}'),
	(re.compile(r'konachan_(\d+)_'), 'https://konachan.com/post/show/{}'),
	(re.compile(r'yandere_(\d+)_'), 'https://yande.re/post/show/{}'),
	(re.compile(r'(?:^|/)(\d{5,12})_(?:p?\d{1,3})(?:[_.\s-]|$)'), 'https://www.pixiv.net/en/artworks/{}'),
	(re.compile(r'pixiv_?(?:id_?)?(\d+)'), 'https://www.pixiv.net/en/artworks/{}'),
)

PIXIV_CDN_RE = re.compile(
	r'(?:i|img)\d{0,5}\.(?:pximg|pixiv)\.net/'
	r'(?:(?:img-original|img\d{1,5})/img/|img/)'
	r'(?:\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2}/)?'
	r'(?:[^/]+/)?'
	r'(\d+)'
	r'(?:_(?:[\w]+_)?p\d{1,3})?'
	r'\.(?:jpg|jpeg|png|webp)'
)
YEAR_RE = re.compile(r'(?:19|20)\d{2}')
YEAR_TAG_RE = re.compile(r'((?:19|20)\d{2})_\(year\)')
E621_ID_TAG_RE = re.compile(r'\d+_\(e621\)')
RATING_ARTIST_TAG_RE = re.compile(r'rating_[a-z_]+_\(artist\)')


def is_recognized_prefix(tag: str) -> bool:
	"""Returns True ONLY if the tag starts with a real namespace prefix (not a colon inside a title)."""
	if ':' not in tag:
		return False
	prefix, val = tag.split(':', 1)
	p_lower = prefix.lstrip("\"'").lower()
	if p_lower in JUNK_METADATA_PREFIXES or p_lower == '[filename]':
		return True
	if val.startswith('_') and p_lower not in RECOGNIZED_PREFIXES:
		return False
	return p_lower in RECOGNIZED_PREFIXES


def get_source_score(url: str | None) -> tuple[int, int]:
	"""Returns the priority score tuple (domain_score, is_profile_url) for a given URL. Lower is better."""
	if not url:
		return (999, 1)

	url_lower = url.lower()
	is_profile = (
		'/users/' in url_lower
		or 'member.php?id=' in url_lower
		or '/i/user/' in url_lower
		or bool(re.fullmatch(r'https?://[\w-]+\.tumblr\.com/?', url_lower))
	)
	profile_penalty = 1 if is_profile else 0

	for domain, score in SOURCE_PRIORITY.items():
		if domain in url_lower:
			return (score, profile_penalty)
	return (100, profile_penalty)


def convert_filename_to_source(filename: str) -> str | None:
	"""Extracts canonical source URLs from standardized filename paths."""
	if not isinstance(filename, str):
		return None

	for pattern, url_fmt in FILENAME_SOURCE_PATTERNS:
		if match := pattern.search(filename):
			return url_fmt.format(*match.groups())
	return None


def convert_cdn_url(image_url: str) -> str:
	"""Converts CDN links and legacy archiver domains to canonical URLs."""
	if not isinstance(image_url, str):
		raise TypeError(f'Input image URL must be a string. Received {type(image_url)}')

	if match := PIXIV_CDN_RE.search(image_url):
		return f'https://www.pixiv.net/en/artworks/{match.group(1)}'
	if match := re.search(r'c\.fantia\.jp/uploads/post/file/(\d+)/', image_url):
		return f'https://fantia.jp/posts/{match.group(1)}'
	if match := re.search(r'([\w-]+)\.tumblr\.com/post/(\d+)', image_url):
		return f'https://{match.group(1)}.tumblr.com/post/{match.group(2)}'
	if match := re.search(r'gelbooru_(\d+)_', image_url):
		return f'https://gelbooru.com/index.php?page=post&s=view&id={match.group(1)}'
	if match := re.search(r'files\.yande\.re/.*?/yande\.re(?:%20|\s|\+)(\d+)', image_url):
		return f'https://yande.re/post/show/{match.group(1)}'
	if match := re.search(r'https?://(?:www\.)?(?:kemono\.(?:cr|su|party)|pawchive\.st)(/.*)', image_url, re.IGNORECASE):
		return f'https://pawchive.pw{match.group(1)}'
	if match := re.search(r'pixiv\.net/(?:en/)?member_illust\.php\?(?:[^#]*&)?illust_id=(\d+)', image_url):
		return f'https://www.pixiv.net/en/artworks/{match.group(1)}'
	if match := re.search(r'pixiv\.net/(?:en/)?member\.php\?id=(\d+)', image_url):
		return f'https://www.pixiv.net/en/users/{match.group(1)}'
	return image_url


def resolve_best_source(
	post_source: str | list[str] | None,
	filename: str | object = None,
	extra_sources: Iterable[str] = (),
) -> str | None:
	"""Evaluates metadata source, filename, and any extra source URLs, returning the highest-priority URL."""
	candidates: set[str] = set()

	if post_source:
		if isinstance(post_source, list):
			for src in post_source:
				if src and str(src).strip():
					candidates.add(convert_cdn_url(str(src).strip()))
		elif str(post_source).strip():
			candidates.add(convert_cdn_url(str(post_source).strip()))

	if filename is not None and (file_src := convert_filename_to_source(str(filename))):
		candidates.add(file_src)

	for extra in extra_sources:
		if extra and extra.strip():
			candidates.add(convert_cdn_url(extra.strip()))

	if not candidates:
		return None

	return min(candidates, key=lambda u: (get_source_score(u), u))


def _resolve_language_token(token_lower: str) -> str | None:
	"""If token_lower represents a known language tag, returns 'language:<lang>'."""
	if token_lower in BARE_LANGUAGES:
		return f'language:{token_lower}'
	for suffix in ('_text', '_language', '_(language)'):
		if token_lower.endswith(suffix):
			stem = token_lower[:-len(suffix)]
			if stem in KNOWN_LANGUAGES:
				return f'language:{stem}'
	return None


def _resolve_website_prefix(val_lower: str) -> str:
	"""Maps website:<val> to booru:<name>, site:<name>, or '_DROP_'."""
	clean = val_lower.strip("\"'_ /")
	if (
		not clean
		or '_' in clean
		or ' ' in clean
		or clean in {'booru', 'website', 'none', 'unknown', 'null', 'chan', 'imageboard'}
	):
		return '_DROP_'
	if clean == 'yande.re':
		return 'booru:yandere'
	if clean.startswith('danbooru.'):
		return 'booru:danbooru'
	base_domain = re.sub(
		r'\.(?:ninja|com|net|org|moe|us|jp|cc|io|cr|re|xxx|tv|me|co|info|ru|de|pl|to)$',
		'',
		clean,
	)
	if 'booru' in base_domain or base_domain in KNOWN_BOORU_NAMES:
		return f'booru:{base_domain}'
	return f'site:{base_domain}'


def _resolve_source_id_prefix(prefix_lower: str, val_clean: str) -> str:
	"""Maps scraper ID prefixes (pixiv_*, nijie_work, gelbooru_id, f_id, tweeter_id, etc.) to source: URLs."""
	clean_id = val_clean.strip("\"'_ ")
	if prefix_lower in {'pixiv_work', 'pixiv_illust_id', 'pixiv_illust', 'pixiv_illustration'}:
		return f'source:https://www.pixiv.net/en/artworks/{clean_id}' if clean_id.isdigit() else '_DROP_'
	if prefix_lower in {'pixiv_id', 'pixiv_profile', 'pixiv_user', 'pixiv_user_id', 'pixiv_artist_id'}:
		return f'source:https://www.pixiv.net/en/users/{clean_id}' if clean_id.isdigit() else '_DROP_'
	if prefix_lower == 'nijie_work':
		return f'source:https://nijie.info/view.php?id={clean_id}' if clean_id.isdigit() else '_DROP_'
	if prefix_lower == 'gelbooru_id':
		return f'source:https://gelbooru.com/index.php?page=post&s=view&id={clean_id}' if clean_id.isdigit() else '_DROP_'
	if prefix_lower == 'e621_id':
		return f'source:https://e621.net/posts/{clean_id}' if clean_id.isdigit() else '_DROP_'
	if prefix_lower == 'f_id':
		return f'source:https://www.furaffinity.net/view/{clean_id}' if clean_id.isdigit() else '_DROP_'
	if prefix_lower == 'patreon_id':
		return f'source:https://www.patreon.com/posts/{clean_id}' if clean_id.isdigit() else '_DROP_'
	if prefix_lower == 'sankaku_id':
		return f'source:https://chan.sankakucomplex.com/post/show/{clean_id}' if clean_id.isalnum() else '_DROP_'
	if prefix_lower == 'tweeter_id':
		if not clean_id.isdigit():
			return '_DROP_'
		if len(clean_id) >= 15:
			return f'source:https://x.com/i/web/status/{clean_id}'
		return f'source:https://x.com/i/user/{clean_id}'
	return '_DROP_'


def _resolve_suffix_tag(tag_clean: str, tag_lower: str) -> tuple[bool, str | None]:
	"""Handles tags ending in _(artist), _(character), _(series), _(cosplay), or _(year)."""
	if tag_lower.endswith('_(artist)'):
		base_artist = tag_clean[:-9]
		if is_recognized_prefix(base_artist):
			base_artist = base_artist.split(':', 1)[1]
		base_artist = base_artist.strip("\"'_:").lower()
		return True, f'artist:{base_artist}' if base_artist else '_DROP_'

	if tag_lower.endswith('_(character)'):
		base_char = tag_clean[:-12]
		if is_recognized_prefix(base_char):
			base_char = base_char.split(':', 1)[1]
		base_char = base_char.strip("\"'_:").lower()
		return True, f'character:{base_char}' if base_char else '_DROP_'

	if tag_lower.endswith('_(series)') and not is_recognized_prefix(tag_clean):
		base_series = tag_lower[:-9].strip("\"'_:")
		return True, f'series:{base_series}' if base_series else None

	if tag_lower.endswith('_(cosplay)'):
		base_cos = tag_clean[:-10]
		if is_recognized_prefix(base_cos):
			base_cos = base_cos.split(':', 1)[1]
		base_cos = base_cos.strip("\"'_:").lower()
		if base_cos and base_cos not in {'official', 'self', 'cross-species'}:
			return True, f'character:{base_cos} cosplay'
		return True, None

	if match := YEAR_TAG_RE.fullmatch(tag_lower):
		return True, f'meta:{match.group(1)}'

	return False, None


def _resolve_unprefixed_tag(tag_lower: str) -> str | None:
	"""Handles unprefixed language, container/year meta, and bare pixiv_* tags."""
	if lang_tag := _resolve_language_token(tag_lower):
		return lang_tag
	if tag_lower in UNPREFIXED_CONTAINER_META or YEAR_RE.fullmatch(tag_lower):
		return f'meta:{tag_lower}'
	if match := re.fullmatch(r'pixiv_(?:user_id|user|profile|artist_id|id)_?(\d+)', tag_lower):
		return f'source:https://www.pixiv.net/en/users/{match.group(1)}'
	if match := re.fullmatch(r'pixiv_(?:illust_id|illust|illustration|work)_?(\d+)', tag_lower):
		return f'source:https://www.pixiv.net/en/artworks/{match.group(1)}'
	if match := re.fullmatch(r'pixiv_(\d+)', tag_lower):
		return f'source:https://www.pixiv.net/en/artworks/{match.group(1)}'
	return None


def _resolve_meta_prefix(val_clean: str, val_lower: str) -> str:
	"""Handles meta: and metadata: prefixed tags."""
	if val_lower in DROP_META_JUNK:
		return '_DROP_'
	if val_lower in META_VALUE_REMAP:
		return META_VALUE_REMAP[val_lower]
	if val_lower in VALID_META_TAGS or YEAR_RE.fullmatch(val_lower):
		return f'meta:{val_lower}'
	nested = resolve_prefixed_tag(val_clean)
	return nested if nested is not None else val_lower


def _resolve_rating_prefix(val_clean: str, val_lower: str) -> str:
	"""Handles rating: prefixed tags, rescuing accidentally misplaced URLs/filenames."""
	if val_lower in {'e', 'explicit'}:
		return 'rating:e'
	if val_lower in {'q', 'questionable', 'sensitive'}:
		return 'rating:q'
	if val_lower in {'s', 'g', 'safe', 'general'}:
		return 'rating:s'
	if val_lower.startswith(('http://', 'https://', 'www.')):
		raw_url = f'https://{val_clean}' if val_lower.startswith('www.') else val_clean
		return f'source:{convert_cdn_url(raw_url)}'
	if resolved_src := convert_filename_to_source(val_clean):
		return f'source:{resolved_src}'
	return '_DROP_'


def resolve_prefixed_tag(tag: str) -> str | None:
	"""Inspects prefixed or structured tags and returns a canonical replacement
	(or space-separated multi-tags), '_DROP_', or None.
	"""
	tag_clean = tag.strip()
	tag_lower = tag_clean.lower()

	if E621_ID_TAG_RE.fullmatch(tag_lower) or RATING_ARTIST_TAG_RE.fullmatch(tag_lower):
		return '_DROP_'
	if tag_lower in {'original', 'original_(series)'}:
		return 'series:original'
	if tag_lower in DROP_META_JUNK:
		return '_DROP_'
	if tag_lower in META_VALUE_REMAP:
		return META_VALUE_REMAP[tag_lower]


	handled, suffix_res = _resolve_suffix_tag(tag_clean, tag_lower)
	if handled:
		return suffix_res

	if not is_recognized_prefix(tag_clean):
		return _resolve_unprefixed_tag(tag_lower)

	prefix, val = tag_clean.split(':', 1)
	prefix_lower = prefix.lstrip("\"'").lower()
	val_clean = val.strip().strip('_')
	val_lower = val_clean.lower()

	if not val_clean:
		return '_DROP_'

	if prefix_lower == 'hydrus_invalid_tag':
		inner_tag = val_clean.strip("\"'_ ")
		if not inner_tag:
			return '_DROP_'
		rescued = resolve_prefixed_tag(inner_tag)
		if rescued is not None:
			return rescued
		return inner_tag.lower() if ':' not in inner_tag else '_DROP_'

	if prefix_lower in {'filename', 'file_name', 'path', '[filename]'}:
		if resolved_src := convert_filename_to_source(val_clean.strip("\"'")):
			return f'source:{resolved_src}'
		return '_DROP_'

	if prefix_lower in JUNK_METADATA_PREFIXES:
		return '_DROP_'
	if prefix_lower in SOURCE_ID_PREFIXES:
		return _resolve_source_id_prefix(prefix_lower, val_clean)
	if prefix_lower == 'website':
		return _resolve_website_prefix(val_lower)

	if prefix_lower == 'tagme':
		if val_lower in {'artist', 'creator', 'author'}:
			return 'artist:tagme'
		if val_lower == 'character':
			return 'character:tagme'
		if val_lower in {'series', 'copyright', 'parody'}:
			return 'series:tagme'
		return '_DROP_'

	if prefix_lower == 'meme':
		clean_meme = val_lower.strip("\"'_:")
		return f'{clean_meme} meme' if clean_meme and clean_meme != 'meme' else 'meme'

	if prefix_lower in FILE_FORMAT_PREFIXES:
		clean_fmt = val_lower.lstrip('.')
		return f'meta:{clean_fmt}' if clean_fmt in VALID_META_TAGS else '_DROP_'

	if prefix_lower == 'character':
		clean_char = val_lower.strip("\"'_:")
		if not clean_char:
			return '_DROP_'
		if clean_char == 'tagme':
			return 'character:tagme' if prefix != 'character' else None
		target_char = f'character:{clean_char}'
		return target_char if target_char != tag_clean else None

	if prefix_lower == 'language':
		if lang_tag := _resolve_language_token(val_lower):
			return lang_tag
		if val_lower in KNOWN_LANGUAGES:
			return f'language:{val_lower}'
		if val_lower in {'translated', 'untranslated', 'poorly_translated', 'machine_translated', 'speechless'}:
			return val_lower
		if val_lower in {'rewrite', 'textless', 'none', 'na', 'n/a'}:
			return '_DROP_'
		return f'language:{val_lower}'

	if prefix_lower in {'meta', 'metadata'}:
		return _resolve_meta_prefix(val_clean, val_lower)

	if prefix_lower in STRIP_GENERAL_PREFIXES:
		mapped_val = EH_SYNONYM_MAP.get(val_lower, val_lower)
		nested = resolve_prefixed_tag(mapped_val)
		return nested if nested is not None else mapped_val

	if prefix_lower in SERIES_PREFIXES:
		if val_lower in {'none', 'original'}:
			return 'series:original'
		if val_lower == 'tagme':
			return 'series:tagme' if prefix_lower != 'series' else None
		clean_series = re.sub(r'_series\)$', ')', val_lower)
		clean_series = re.sub(r'_\(series\)$', '', clean_series)
		target_series = f'series:{clean_series}'
		return target_series if target_series != tag_clean else None

	if prefix_lower in ARTIST_PREFIXES:
		if val_lower == 'tagme':
			return 'artist:tagme' if prefix_lower != 'artist' else None
		clean_artist = val_lower[:-9] if val_lower.endswith('_(artist)') else val_lower
		clean_artist = clean_artist.strip("\"'_:")
		if not clean_artist:
			return '_DROP_'
		target_artist = f'artist:{clean_artist}'
		return target_artist if target_artist != tag_clean else None

	if prefix_lower in {'http', 'https'} and val_lower.startswith('//'):
		return f'source:{convert_cdn_url(tag_clean)}'

	if prefix_lower == 'rating':
		return _resolve_rating_prefix(val_clean, val_lower)

	return None
