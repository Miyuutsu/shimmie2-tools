"""Functions for parsing and resolving source URLs and normalizing structured tags."""
import re

STRIP_GENERAL_PREFIXES = {
    "female", "male", "mixed", "other", "general", "tag", "tags",
    "fetish", "species", "body", "clothing", "pose", "setting",
    "location", "genre", "theme", "lore", "reclass",
}
SERIES_PREFIXES = {"copyright", "parody", "series"}
ARTIST_PREFIXES = {"creator", "author", "illustrator", "circle", "group", "cosplayer", "artist"}
JUNK_METADATA_PREFIXES = {
    "id", "post", "pool", "parent", "width", "height",
    "filesize", "score", "favcount", "md5", "sha256", "status",
}
CORE_PASSTHROUGH_PREFIXES = {"character", "source", "booru", "tagai", "site", "studio"}
RECOGNIZED_PREFIXES = (
    STRIP_GENERAL_PREFIXES
    | SERIES_PREFIXES
    | ARTIST_PREFIXES
    | JUNK_METADATA_PREFIXES
    | CORE_PASSTHROUGH_PREFIXES
    | {
        "language", "meta", "rating", "http", "https",
        "pixiv_work", "pixiv_illust_id", "pixiv_illust", "pixiv_id",
        "pixiv_profile", "pixiv_user", "pixiv_user_id",
    }
)
EH_SYNONYM_MAP = {
    "sole_female": "1girl",
    "sole_male": "1boy",
}
KNOWN_LANGUAGES = {
    "arabic", "chinese", "dutch", "english", "engrish", "esperanto",
    "finnish", "french", "german", "greek", "hebrew", "hindi",
    "hungarian", "indonesian", "inkling", "italian", "japanese",
    "korean", "latin", "polish", "portuguese", "romaji", "russian",
    "simplified_chinese", "spanish", "swedish", "tagalog", "thai",
    "traditional_chinese", "turkish", "ukrainian", "vietnamese",
}
BARE_LANGUAGES = KNOWN_LANGUAGES - {"latin", "inkling"}
VALID_META_TAGS = {
    "mp4", "webm", "gif", "apng", "ugoira", "swf", "flv", "m4v", "ogv",
    "mov", "mkv", "avi", "animated", "animated_gif", "animated_png",
    "video", "audio", "sound", "voiced", "no_sound", "silent",
    "loop", "looping", "30fps", "60fps", "high_framerate", "low_framerate",
    "lossless", "lossy",
}
UNPREFIXED_CONTAINER_META = {
    "mp4", "webm", "apng", "ugoira", "swf", "flv", "m4v", "ogv", "mov", "mkv", "avi",
    "30fps", "60fps", "lossless", "lossy",
}


def is_recognized_prefix(tag: str) -> bool:
    """Returns True ONLY if the tag starts with a real namespace prefix (not a colon inside a title)."""
    if ":" not in tag:
        return False
    prefix, val = tag.split(":", 1)
    if val.startswith("_") and prefix.lower() not in RECOGNIZED_PREFIXES:
        return False
    return prefix.lower() in RECOGNIZED_PREFIXES


def get_source_score(url):
    """Returns the priority score for a given URL. Lower is better."""
    source_priority = {
        "pixiv.net": 1, "fantia.jp": 2, "tumblr.com": 3, "baraag.net": 4,
        "misskey.io": 5, "pawoo.net": 6, "twitter.com": 7, "x.com": 7,
        "gelbooru.com": 8, "konachan.com": 9, "kemono.cr": 10,
        "danbooru.donmai.us": 11, "twimg.com": 12, "yande.re": 13
    }
    if not url:
        return 999

    url_lower = url.lower()
    for domain, score in source_priority.items():
        if domain in url_lower:
            return score
    return 100


def convert_filename_to_source(filename):
    """Extracts canonical source URLs from standardized filename paths."""
    if not isinstance(filename, str):
        return None

    patterns = [
        (r"fanbox/(\d+)/(\d+)_", "https://kemono.cr/fanbox/user/{0}/post/{1}"),
        (r"gelbooru_(\d+)_", "https://gelbooru.com/index.php?page=post&s=view&id={}"),
        (r"konachan_(\d+)_", "https://konachan.com/post/show/{}"),
        (r"yandere_(\d+)_", "https://yande.re/post/show/{}"),
        (r"(?:^|/)(\d{5,12})_(?:p?\d{1,3})(?:[_.\s-]|$)", "https://www.pixiv.net/en/artworks/{}"),
        (r"pixiv_?(?:id_?)?(\d+)", "https://www.pixiv.net/en/artworks/{}"),
    ]

    for pattern, url_fmt in patterns:
        match = re.search(pattern, filename)
        if match:
            return url_fmt.format(*match.groups())
    return None


def convert_cdn_url(image_url):
    """Converts CDN links to more descriptive URLs."""
    if not isinstance(image_url, str):
        raise TypeError(f"Input image URL must be a string. Received {type(image_url)}")

    pixiv_pattern = (
        r"(?:i|img)\d{0,5}\.(?:pximg|pixiv)\.net/"
        r"(?:(?:img-original|img\d{1,5})/img/|img/)"
        r"(?:\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2}/)?"
        r"(?:[^/]+/)?"
        r"(\d+)"
        r"(?:_(?:[\w]+_)?p\d{1,3})?"
        r"\.(?:jpg|jpeg|png|webp)"
    )

    if match := re.search(pixiv_pattern, image_url):
        return f"https://www.pixiv.net/en/artworks/{match.group(1)}"
    if match := re.search(r"c\.fantia\.jp/uploads/post/file/(\d+)/", image_url):
        return f"https://fantia.jp/posts/{match.group(1)}"
    if match := re.search(r"([\w-]+)\.tumblr\.com/post/(\d+)", image_url):
        return f"https://{match.group(1)}.tumblr.com/post/{match.group(2)}"
    if match := re.search(r"gelbooru_(\d+)_", image_url):
        return f"https://gelbooru.com/index.php?page=post&s=view&id={match.group(1)}"
    if match := re.search(r"files\.yande\.re/.*?/yande\.re(?:%20|\s|\+)(\d+)", image_url):
        return f"https://yande.re/post/show/{match.group(1)}"
    return image_url


def resolve_best_source(post_source, filename):
    """Evaluates both the metadata source and filename, returning the highest priority URL."""
    candidates = []

    if post_source:
        if isinstance(post_source, list):
            candidates.extend([convert_cdn_url(src) for src in post_source])
        else:
            candidates.append(convert_cdn_url(post_source))

    if file_src := convert_filename_to_source(str(filename)):
        candidates.append(file_src)

    candidates = [c for c in candidates if c]
    if not candidates:
        return None

    candidates.sort(key=get_source_score)
    return candidates[0]


def _resolve_language_token(token_lower):
    """If token_lower represents a known language tag, returns 'language:<lang>'."""
    if token_lower in BARE_LANGUAGES:
        return f"language:{token_lower}"
    for suffix in ("_text", "_language", "_(language)"):
        if token_lower.endswith(suffix):
            stem = token_lower[:-len(suffix)]
            if stem in KNOWN_LANGUAGES:
                return f"language:{stem}"
    return None


def resolve_kiseki_series(tag_lower: str) -> tuple[str, ...] | None:
    """Normalizes Trails / Kiseki / Legend of Heroes variants and numbered sub-series
    into their canonical series tag(s) plus parent series:eiyuu_densetsu."""
    clean = tag_lower.split(":", 1)[1] if tag_lower.startswith(("series:", "copyright:", "parody:")) else tag_lower
    clean = re.sub(r"_\(series\)$", "", clean)
    clean = re.sub(r"_series$", "", clean)

    if clean in {"the_legend_of_heroes", "legend_of_heroes", "eiyuu_densetsu", "kiseki"}:
        return ("series:eiyuu_densetsu",)

    if any(k in clean for k in ("trails_of_cold_steel", "trails_of_cold_sky", "sen_no_kiseki")):
        return ("series:sen_no_kiseki", "series:eiyuu_densetsu")
    if any(k in clean for k in ("trails_in_the_sky", "sora_no_kiseki")):
        return ("series:sora_no_kiseki", "series:eiyuu_densetsu")
    if any(k in clean for k in ("trails_from_zero", "zero_no_kiseki")):
        return ("series:zero_no_kiseki", "series:eiyuu_densetsu")
    if any(k in clean for k in ("trails_to_azure", "ao_no_kiseki")):
        return ("series:ao_no_kiseki", "series:eiyuu_densetsu")
    if any(k in clean for k in ("trails_into_reverie", "hajimari_no_kiseki")):
        return ("series:hajimari_no_kiseki", "series:eiyuu_densetsu")
    if any(k in clean for k in ("trails_through_daybreak", "kuro_no_kiseki")):
        return ("series:kuro_no_kiseki", "series:eiyuu_densetsu")
    if any(k in clean for k in ("trails_beyond_the_horizon", "kai_no_kiseki")):
        return ("series:kai_no_kiseki", "series:eiyuu_densetsu")

    if clean.startswith("the_legend_of_heroes"):
        return ("series:eiyuu_densetsu",)
    return None


def resolve_prefixed_tag(tag):
    """Inspects prefixed or structured tags (female:, male:, copyright:, parody:,
    language:, meta:, rating:, pixiv_*, *_(artist), Kiseki/series, etc.) and returns
    a canonical replacement (or space-separated multi-tags), '_DROP_', or None."""
    tag_clean = tag.strip()
    tag_lower = tag_clean.lower()

    # 1. Known unprefixed junk patterns (e.g., 34764_(e621), rating_sensitive_(artist))
    if re.fullmatch(r"\d+_\(e621\)", tag_lower):
        return "_DROP_"
    if re.fullmatch(r"rating_[a-z_]+_\(artist\)", tag_lower):
        return "_DROP_"

    # 2. Unprefixed original -> series:original
    if tag_lower in {"original", "original_(series)"}:
        return "series:original"

    # 3. Kiseki / Trails of Cold Steel / Legend of Heroes multi-series normalization
    if kiseki_tags := resolve_kiseki_series(tag_lower):
        joined = " ".join(kiseki_tags)
        return joined if joined != tag_clean else None

    # 4. Normalize *_(artist), *_(character), and *_(series) / *_series suffixes
    if tag_lower.endswith("_(artist)"):
        base_artist = tag_clean[:-9]
        if is_recognized_prefix(base_artist):
            base_artist = base_artist.split(":", 1)[1]
        base_artist = base_artist.strip("_:").lower()
        return f"artist:{base_artist}" if base_artist else "_DROP_"

    if tag_lower.endswith("_(character)"):
        base_char = tag_clean[:-12]
        if is_recognized_prefix(base_char):
            base_char = base_char.split(":", 1)[1]
        base_char = base_char.strip("_:").lower()
        return f"character:{base_char}" if base_char else "_DROP_"

    if tag_lower.endswith("_(series)") and not is_recognized_prefix(tag_clean):
        base_series = tag_lower[:-9].strip("_:")
        return f"series:{base_series}" if base_series else None

    if tag_lower.endswith("_(cosplay)"):
        base_cos = tag_clean[:-10]
        if is_recognized_prefix(base_cos):
            base_cos = base_cos.split(":", 1)[1]
        base_cos = base_cos.strip("_:").lower()
        if base_cos and base_cos not in {"official", "self", "cross-species"}:
            return f"character:{base_cos} cosplay"
        return None

    if match := re.fullmatch(r"((?:19|20)\d{2})_\(year\)", tag_lower):
        return f"meta:{match.group(1)}"

    # 5. Unprefixed tags OR tags with a colon inside the title (e.g. the_legend_of_heroes:_...)
    if not is_recognized_prefix(tag_clean):
        if lang_tag := _resolve_language_token(tag_lower):
            return lang_tag
        if tag_lower in UNPREFIXED_CONTAINER_META or re.fullmatch(r"(?:19|20)\d{2}", tag_lower):
            return f"meta:{tag_lower}"
        if match := re.fullmatch(r"pixiv_(?:user_id|user|profile)_?(\d+)", tag_lower):
            return f"source:https://www.pixiv.net/en/users/{match.group(1)}"
        if match := re.fullmatch(r"pixiv_(?:illust_id|illust|work|id)_?(\d+)", tag_lower):
            return f"source:https://www.pixiv.net/en/artworks/{match.group(1)}"
        if match := re.fullmatch(r"pixiv_(\d+)", tag_lower):
            return f"source:https://www.pixiv.net/en/artworks/{match.group(1)}"
        return None

    prefix, val = tag_clean.split(":", 1)
    prefix_lower = prefix.lower()
    val_clean = val.strip().strip("_")
    val_lower = val_clean.lower()

    if not val_clean:
        return "_DROP_"

    # 6. Language prefix normalization
    if prefix_lower == "language":
        if lang_tag := _resolve_language_token(val_lower):
            return lang_tag
        if val_lower in KNOWN_LANGUAGES:
            return f"language:{val_lower}"
        if val_lower in {"translated", "untranslated", "poorly_translated", "machine_translated", "speechless"}:
            return val_lower
        if val_lower in {"rewrite", "textless", "none", "na", "n/a"}:
            return "_DROP_"
        return f"language:{val_lower}"

    # 7. Meta prefix cleanup: keep only valid technical/container/year metadata in meta:
    if prefix_lower == "meta":
        if val_lower in VALID_META_TAGS or re.fullmatch(r"(?:19|20)\d{2}", val_lower):
            return f"meta:{val_lower}"
        nested = resolve_prefixed_tag(val_clean)
        return nested if nested is not None else val_lower

    # 8. E-Hentai / Hydrus / gallery-dl general prefixes -> strip prefix and re-check inner token
    if prefix_lower in STRIP_GENERAL_PREFIXES:
        mapped_val = EH_SYNONYM_MAP.get(val_lower, val_lower)
        nested = resolve_prefixed_tag(mapped_val)
        return nested if nested is not None else mapped_val

    # 9. Copyright / Parody / Series -> series:<val>
    if prefix_lower in SERIES_PREFIXES:
        if val_lower in {"none", "original"}:
            return "series:original"
        if val_lower == "tagme":
            return "series:tagme" if prefix_lower != "series" else None
        if kiseki_tags := resolve_kiseki_series(val_lower):
            joined = " ".join(kiseki_tags)
            return joined if joined != tag_clean else None
        clean_series = re.sub(r"_series\)$", ")", val_lower)
        clean_series = re.sub(r"_\(series\)$", "", clean_series)
        target_series = f"series:{clean_series}"
        return target_series if target_series != tag_clean else None

    # 10. Creator / Circle / Group / Cosplayer / Artist -> artist:<val>
    if prefix_lower in ARTIST_PREFIXES:
        if val_lower == "tagme":
            return "artist:tagme" if prefix_lower != "artist" else None
        clean_artist = val_lower[:-9] if val_lower.endswith("_(artist)") else val_lower
        clean_artist = clean_artist.strip("_:")
        if not clean_artist:
            return "_DROP_"
        target_artist = f"artist:{clean_artist}"
        return target_artist if target_artist != tag_clean else None

    # 11. Colon-prefixed Pixiv tag variants -> canonical source: URL
    if prefix_lower in {"pixiv_work", "pixiv_illust_id", "pixiv_illust", "pixiv_id"} and val_clean.isdigit():
        return f"source:https://www.pixiv.net/en/artworks/{val_clean}"
    if prefix_lower in {"pixiv_profile", "pixiv_user", "pixiv_user_id"} and val_clean.isdigit():
        return f"source:https://www.pixiv.net/en/users/{val_clean}"

    # 12. Bare http:// or https:// URLs stored directly as tags -> source:<url>
    if prefix_lower in {"http", "https"} and val_lower.startswith("//"):
        return f"source:{convert_cdn_url(tag_clean)}"

    # 13. Normalize valid rating: tags; rescue URLs to source:; drop bogus rating: tags
    if prefix_lower == "rating":
        if val_lower in {"e", "explicit"}:
            return "rating:e"
        if val_lower in {"q", "questionable", "sensitive"}:
            return "rating:q"
        if val_lower in {"s", "g", "safe", "general"}:
            return "rating:s"
        if val_lower.startswith(("http://", "https://", "www.")):
            raw_url = f"https://{val_clean}" if val_lower.startswith("www.") else val_clean
            return f"source:{convert_cdn_url(raw_url)}"
        if resolved_src := convert_filename_to_source(val_clean):
            return f"source:{resolved_src}"
        return "_DROP_"

    # 14. Known scraper metadata junk prefixes
    if prefix_lower in JUNK_METADATA_PREFIXES:
        return "_DROP_"

    return None
