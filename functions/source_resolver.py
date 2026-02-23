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
        (r"gelbooru_(\d+)_", "https://gelbooru.com/index.php?page=post&s=view&id={}"),
        (r"konachan_(\d+)_", "https://konachan.com/post/show/{}"),
        (r"fanbox/(\d+)/(\d+)_", "https://kemono.cr/fanbox/user/{0}/post/{1}"),
        (r"yandere_(\d+)_", "https://yande.re/post/show/{}")
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
