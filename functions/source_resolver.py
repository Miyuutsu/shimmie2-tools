"""Functions for parsing and resolving source URLs."""
import re

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

STRIP_GENERAL_PREFIXES = {
    "female", "male", "mixed", "other", "general", "tag", "tags",
    "fetish", "species", "body", "clothing", "pose", "setting",
    "location", "genre", "theme", "lore", "language", "reclass",
}
SERIES_PREFIXES = {"copyright", "parody"}
ARTIST_PREFIXES = {"creator", "author", "illustrator", "circle", "group", "cosplayer"}
JUNK_METADATA_PREFIXES = {
    "id", "post", "pool", "parent", "width", "height",
    "filesize", "score", "favcount", "md5", "sha256", "status",
}
EH_SYNONYM_MAP = {
    "sole_female": "1girl",
    "sole_male": "1boy",
}


def resolve_prefixed_tag(tag):
    """Inspects prefixed or structured tags (female:, male:, copyright:, parody:,
    rating:, pixiv_*, *_(artist), etc.) and returns a canonical replacement,
    '_DROP_' (strictly for known junk), or None to leave untouched."""
    tag_clean = tag.strip()
    tag_lower = tag_clean.lower()

    # 1. Known unprefixed junk patterns (e.g., 34764_(e621), rating_sensitive_(artist))
    if re.fullmatch(r"\d+_\(e621\)", tag_lower):
        return "_DROP_"
    if re.fullmatch(r"rating_[a-z_]+_\(artist\)", tag_lower):
        return "_DROP_"

    # 2. Normalize *_(artist) suffix -> artist:<name>
    if tag_lower.endswith("_(artist)"):
        base_artist = tag_clean[:-9]
        if ":" in base_artist:
            base_artist = base_artist.split(":", 1)[1]
        base_artist = base_artist.strip("_:")
        if base_artist:
            return f"artist:{base_artist}"
        return "_DROP_"

    # 3. Unprefixed underscore Pixiv tags (e.g., pixiv_id_21283777, pixiv_user_12345)
    if ":" not in tag_clean:
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

    # 4. E-Hentai / Hydrus / gallery-dl general prefixes -> strip prefix
    if prefix_lower in STRIP_GENERAL_PREFIXES:
        mapped_val = EH_SYNONYM_MAP.get(val_lower, val_lower)
        if mapped_val.endswith("_(artist)"):
            return f"artist:{mapped_val[:-9].strip('_:')}"
        return mapped_val

    # 5. Copyright / Parody -> series:<val>
    if prefix_lower in SERIES_PREFIXES:
        if val_lower in {"none", "original"}:
            return "original"
        clean_series = re.sub(r"_series\)$", ")", val_lower)
        clean_series = re.sub(r"_\(series\)$", "", clean_series)
        return f"series:{clean_series}"

    # 6. Creator / Circle / Group / Cosplayer -> artist:<val>
    if prefix_lower in ARTIST_PREFIXES:
        clean_artist = val_lower[:-9] if val_lower.endswith("_(artist)") else val_lower
        clean_artist = clean_artist.strip("_:")
        return f"artist:{clean_artist}" if clean_artist else "_DROP_"

    # 7. Colon-prefixed Pixiv tag variants -> canonical source: URL
    if prefix_lower in {"pixiv_work", "pixiv_illust_id", "pixiv_illust", "pixiv_id"} and val_clean.isdigit():
        return f"source:https://www.pixiv.net/en/artworks/{val_clean}"
    if prefix_lower in {"pixiv_profile", "pixiv_user", "pixiv_user_id"} and val_clean.isdigit():
        return f"source:https://www.pixiv.net/en/users/{val_clean}"

    # 8. Bare http:// or https:// URLs stored directly as tags -> source:<url>
    if prefix_lower in {"http", "https"} and val_lower.startswith("//"):
        return f"source:{convert_cdn_url(tag_clean)}"

    # 9. Malformed rating: tags -> rescue URLs to source:, drop all other rating:* tags
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

    return None
