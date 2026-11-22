"""High-throughput content identification, 3-way routing (keep / comic / fluff), and sidecar generator."""

import logging
import os
import shutil
import sqlite3
import warnings
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import open_clip
import pandas as pd
import timm
import torch
import tqdm
from huggingface_hub import hf_hub_download
from huggingface_hub.errors import LocalEntryNotFoundError
from PIL import Image
from torch.nn import functional as F

from functions.common import (
    CACHE_PATH,
    IMAGE_EXTS,
    STAGING_STATE_DB_PATH,
    TAG_DB_PATH,
    VIDEO_EXTS,
    extract_or_compute_md5,
)
from functions.db_cache import (
    get_cache_conn,
    get_existing_shimmie_hashes,
    get_shimmie_db_credentials,
)
from functions.media import extract_video_frames, get_image_resolution, get_video_resolution
from functions.tags_curation import load_tag_groups_metadata

Image.MAX_IMAGE_PIXELS = None
warnings.filterwarnings("ignore", message=".*unauthenticated requests.*")
logging.getLogger("huggingface_hub").setLevel(logging.ERROR)

MODEL_REPO_MAP = {
    "vit": "SmilingWolf/wd-vit-tagger-v3",
    "vit-large": "SmilingWolf/wd-vit-large-tagger-v3",
    "swinv2": "SmilingWolf/wd-swinv2-tagger-v3",
    "convnext": "SmilingWolf/wd-convnext-tagger-v3",
    "eva02": "SmilingWolf/wd-eva02-large-tagger-v3",
}
SIGLIP_REPO = "hf-hub:timm/ViT-B-16-SigLIP-256"
SIGLIP_SIZE = 256
EVA_SIZE = 448
SMIN_QUESTIONABLE = 51
RAW_TAG_FLOOR = 0.15
TRIAGE_HEADER_PREFIX = b"# Verdict:"

# EXIF Tag IDs for physical camera detection
EXIF_MAKE = 271
EXIF_MODEL = 272
EXIF_SUB_IFD = 0x8769
EXIF_CAMERA_PARAMS = {33434, 33437, 34855, 37386, 42036}
IGNORED_EXIF_MODELS = {"scanner", "virtual", "canon canoscan", "epson", "hp scanjet"}

CORE_SUBJECT_TAGS = {
    "1girl",
    "2girls",
    "3girls",
    "4girls",
    "5girls",
    "6+girls",
    "multiple_girls",
    "1boy",
    "2boys",
    "3boys",
    "4boys",
    "5boys",
    "6+boys",
    "multiple_boys",
    "1other",
    "2others",
    "multiple_others",
    "furry",
    "anthro",
    "kemonomimi_mode",
    "monster_girl",
    "monster_boy",
    "pokemon_(creature)",
    "digimon_(creature)",
}

VISIBLE_EYE_COLOR_TAGS = {
    "blue_eyes",
    "red_eyes",
    "brown_eyes",
    "green_eyes",
    "purple_eyes",
    "yellow_eyes",
    "pink_eyes",
    "aqua_eyes",
    "orange_eyes",
    "black_eyes",
    "grey_eyes",
    "silver_eyes",
    "white_eyes",
    "multicolored_eyes",
    "heterochromia",
    "gradient_eyes",
    "two-tone_eyes",
}

VISIBLE_FACE_ANCHOR_TAGS = VISIBLE_EYE_COLOR_TAGS | {
    "closed_eyes",
    "one_eye_closed",
    "wink",
    "half-closed_eyes",
    "empty_eyes",
}

HEAVY_CROP_TAGS = {
    "head_out_of_frame",
    "cropped_torso",
    "cropped_legs",
    "cropped_arms",
    "cropped_shoulders",
    "eyes_out_of_frame",
}

CLOSEUP_FOCUS_TAGS = {
    "close-up",
    "breast_focus",
    "ass_focus",
    "thigh_focus",
    "foot_focus",
    "hand_focus",
    "hip_focus",
    "navel_focus",
    "groin",
    "crotch_focus",
}

EVA_HARD_FLUFF_TAGS = {
    "real_life",
    "real_life_insert",
    "photo_(medium)",
    "screenshot",
    "interface",
    "website",
    "text_focus",
    "calendar",
    "chart",
    "action_figure",
    "merchandise",
    "acrylic_stand",
    "tin_badge",
    "dakimakura_(object)",
    "chat_log",
    "fake_screenshot",
    "fake_phone_screenshot",
    "dialogue_box",
    "gameplay_mechanics",
    "health_bar",
    "card_(medium)",
    "copyright_notice",
    "timestamp",
    "text_messaging",
    "ad",
    "logo",
    "logo_parody",
    "brand_name_imitation",
    "identity_censor",
    "before_and_after",
}

COMIC_BLOCKER_TAGS = {
    "chat_log",
    "fake_screenshot",
    "fake_phone_screenshot",
    "gameplay_mechanics",
    "interface",
    "screenshot",
    "website",
    "card_(medium)",
    "calendar",
    "chart",
}

COMIC_DANBOORU_TAGS = {
    "comic",
    "4koma",
    "2koma",
    "3koma",
    "5koma",
    "manga",
    "silent_comic",
    "multiple_pages",
}

TEXT_LANG_TAGS = {
    "english_text",
    "japanese_text",
    "chinese_text",
    "korean_text",
}

PROTECTED_RESOS = (
    (1280, 720),
    (1600, 900),
    (1920, 1080),
    (2560, 1440),
    (3840, 2160),
    (1200, 900),
)
PROTECTED_VARIANCE = 0.10


def _hits_protected_reso(w: int, h: int) -> tuple[int, int] | None:
    """Returns the protected (pw, ph) if (w, h) falls within ±10% of a protected standard resolution."""
    for pw, ph in PROTECTED_RESOS:
        if abs(w - pw) <= pw * PROTECTED_VARIANCE and abs(h - ph) <= ph * PROTECTED_VARIANCE:
            return (pw, ph)
    return None


def _load_protected_thumb_paths(target_dir: Path) -> set[str]:
    """Loads staged symlink paths marked thumbnail_only in staging_state.db for target_dir."""
    if not STAGING_STATE_DB_PATH.is_file():
        return set()
    target_str = str(target_dir.resolve())
    with sqlite3.connect(STAGING_STATE_DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT staged_path FROM staged_files
            WHERE staging_dir = ?
              AND staged_path != ''
              AND (',' || extra_tags || ',') LIKE '%,thumbnail_only,%'
            """,
            (target_str,),
        ).fetchall()
    return {os.path.abspath(r[0]) for r in rows if r[0]}


# ==========================================
# SigLIP Zero-Shot Semantic Prompts
# ==========================================
SIGLIP_KEEP_PROMPTS = {
    "anime_illustration": "a 2d anime character illustration or digital fanart drawing",
    "kemono_furry_art": "a 2d furry or kemono character illustration or digital drawing",
    "reference_sheet": "a character reference sheet, turnaround views, or character design sheet",
    "3d_character_portrait": "a high quality 3d anime character portrait or posed 3d avatar render",
}

SIGLIP_COMIC_PROMPTS = {
    "comic_strip": "a multi-panel manga page, 4koma comic strip, or monochrome comic with speech bubbles",
}

SIGLIP_FLUFF_PROMPTS = {
    "ui_screenshot": "a screenshot of a web browser, computer desktop, google search bar, or software interface",
    "social_media_or_chat": "a screenshot of a social media profile, twitter feed, follower list, or chat message bubbles",
    "game_roster_or_menu": "a video game menu, character selection grid, level roster screen, or dialogue box screenshot",
    "promo_or_price_sheet": "an advertisement poster, event promotional graphic, schedule banner, or commission price menu",
    "merch_on_desk": "a photograph of printed stickers, acrylic keychains, badges, or merchandise on a desk",
    "doll_or_figurine": "a real life camera photograph of a physical plastic toy or doll in a room",
    "irl_photo": "a real life photograph of food, pets, a room, computer monitor, street, or outdoor scenery",
    "vrchat_hud_or_doodle": "a vrchat camera interface with sliders, a simple line scribble in a 3d room, or an empty 3d world",
    "fps_gameplay": "a first-person shooter video game, gameplay footage, or 3d action game screenshot",
}


@dataclass(slots=True)
class TriageVerdict:
    """Represents the routing decision and diagnostic data for a single media file."""

    file_path: Path
    category: str  # 'KEEP', 'COMIC', or 'FLUFF'
    stage: str
    reason: str
    siglip_scores: dict[str, float] = field(default_factory=dict)
    raw_char_tags: dict[str, float] = field(default_factory=dict)
    raw_gen_tags: dict[str, float] = field(default_factory=dict)
    raw_ratings: dict[str, float] = field(default_factory=dict)
    import_tags: list[str] = field(default_factory=list)

ALL_SIGLIP_KEYS = (
    set(SIGLIP_KEEP_PROMPTS) | set(SIGLIP_COMIC_PROMPTS) | set(SIGLIP_FLUFF_PROMPTS)
)


@dataclass(slots=True)
class CachedTriageData:
    """Cached SigLIP scores, EVA02 raw tags, and import tags parsed from a '# Verdict:' sidecar."""

    siglip_scores: dict[str, float] = field(default_factory=dict)
    raw_char_tags: dict[str, float] = field(default_factory=dict)
    raw_gen_tags: dict[str, float] = field(default_factory=dict)
    raw_ratings: dict[str, float] = field(default_factory=dict)
    import_tags: list[str] = field(default_factory=list)


def _parse_triage_sidecar(media_path: Path) -> CachedTriageData | None:
    """Parses cached SigLIP and EVA02 scores from an existing '# Verdict:' .txt sidecar."""
    txt_path = media_path.with_suffix(".txt")
    if not txt_path.is_file() or not _is_triage_sidecar(txt_path):
        return None

    data = CachedTriageData()
    try:
        lines = txt_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("# SigLIP:"):
            parsed: dict[str, float] = {}
            for tok in line[len("# SigLIP:") :].split(","):
                if "=" not in tok:
                    continue
                k, v_str = tok.rsplit("=", 1)
                with suppress(ValueError):
                    parsed[k.strip()] = float(v_str.strip())
            if ALL_SIGLIP_KEYS.issubset(parsed):
                data.siglip_scores = parsed
        elif line.startswith("# Full Tags:"):
            for tok in line[len("# Full Tags:") :].split(","):
                if "=" not in tok:
                    continue
                k, v_str = tok.rsplit("=", 1)
                k = k.strip()
                try:
                    val = float(v_str.strip())
                except ValueError:
                    continue
                if k.startswith("rating:"):
                    data.raw_ratings[k[7:]] = val
                elif k.startswith("char:"):
                    data.raw_char_tags[k[5:]] = val
                else:
                    data.raw_gen_tags[k] = val
        elif not line.startswith("#"):
            data.import_tags.extend(t.strip() for t in line.split(",") if t.strip())

    return data


def _safe_rel_path(file_path: Path, base_dir: Path) -> Path:
    """Computes relative path safely even if intermediate directories or files are symlinks."""
    try:
        return file_path.relative_to(base_dir)
    except ValueError:
        return file_path.resolve().relative_to(base_dir.resolve())


def _is_triage_sidecar(txt_path: Path) -> bool:
    """Returns True ONLY if the .txt file was generated by triage (starts with '# Verdict:')."""
    try:
        with txt_path.open("rb") as f:
            return f.read(len(TRIAGE_HEADER_PREFIX)) == TRIAGE_HEADER_PREFIX
    except OSError:
        return False


def _reset_triage_state(
    base_dir: Path,
    fluff_dir: Path,
    comic_dir: Path,
    delete_triage_sidecars: bool = True,
) -> tuple[int, int]:
    """Restores moved media from fluff_dir/comic_dir back to base_dir.

    When delete_triage_sidecars is False, '# Verdict:' sidecars are moved back and preserved
    so --retriage can re-evaluate rules without re-running GPU inference.
    """
    allowed_exts = IMAGE_EXTS | VIDEO_EXTS
    restored_files = 0
    removed_sidecars = 0

    for side_dir in (fluff_dir, comic_dir):
        if not side_dir.is_dir():
            continue

        for f in sorted(side_dir.rglob("*")):
            if not f.is_file() or f.suffix.lower() not in allowed_exts:
                continue
            rel = _safe_rel_path(f, side_dir)
            dst = base_dir / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(f), str(dst))
            restored_files += 1

            for cand_txt in (f.with_suffix(".txt"), f.with_name(f"{f.name}.txt")):
                if cand_txt.is_file():
                    if delete_triage_sidecars and _is_triage_sidecar(cand_txt):
                        cand_txt.unlink()
                        removed_sidecars += 1
                    else:
                        shutil.move(str(cand_txt), str(dst.parent / cand_txt.name))

        log_file = side_dir / "triage.log"
        if log_file.is_file():
            log_file.unlink()

        for d in sorted((p for p in side_dir.rglob("*") if p.is_dir()), reverse=True):
            with suppress(OSError):
                d.rmdir()
        with suppress(OSError):
            side_dir.rmdir()

    if delete_triage_sidecars:
        for txt_file in base_dir.rglob("*.txt"):
            if txt_file.is_file() and _is_triage_sidecar(txt_file):
                txt_file.unlink()
                removed_sidecars += 1

    return restored_files, removed_sidecars


def _parse_reso_fluff(raw_spec: str | None) -> set[tuple[int, int]]:
    """Parses comma-separated 'WxH' strings into a set of (width, height) integer tuples."""
    if not raw_spec:
        return set()
    res_set: set[tuple[int, int]] = set()
    for item in raw_spec.split(","):
        clean = item.strip().lower()
        if not clean or "x" not in clean:
            continue
        w_str, h_str = clean.split("x", 1)
        try:
            w, h = int(w_str.strip()), int(h_str.strip())
            if prot := _hits_protected_reso(w, h):
                print(
                    f"[WARNING] Ignoring --reso-fluff entry {w}x{h}: "
                    f"within ±{int(PROTECTED_VARIANCE * 100)}% of protected resolution {prot[0]}x{prot[1]}."
                )
                continue
            if w > 0 and h > 0:
                res_set.add((w, h))
        except ValueError:
            pass
    return res_set


def _collect_candidate_files(
    target_dir: Path,
    allowed_exts: set[str],
    fluff_dir: Path,
    comic_dir: Path,
    include_triage_sidecars: bool = False,
) -> list[Path]:
    """Finds valid media files missing a non-triage .txt sidecar (optionally including '# Verdict:' files)."""
    if not target_dir or not target_dir.is_dir():
        return []

    res_fluff = fluff_dir.resolve()
    res_comic = comic_dir.resolve()
    candidates: list[Path] = []

    for f in target_dir.rglob("*"):
        if not f.is_file() or "thumbnails" in f.parts or f.suffix.lower() not in allowed_exts:
            continue
        resolved = f.resolve()
        if resolved.is_relative_to(res_fluff) or resolved.is_relative_to(res_comic):
            continue

        stem_txt = f.with_suffix(".txt")
        full_txt = f.with_name(f"{f.name}.txt")
        if stem_txt.exists():
            if not (include_triage_sidecars and _is_triage_sidecar(stem_txt)):
                continue
        elif full_txt.exists():
            continue

        candidates.append(f)

    return sorted(candidates)


def _get_cached_tagged_hashes(
    md5_iterable: Iterable[str], cache_path: Path = CACHE_PATH
) -> set[str]:
    """Performs a fast bulk MD5-only lookup against posts_cache.db without decoding images."""
    if not cache_path.is_file():
        return set()

    md5_list = [m.lower() for m in md5_iterable if m]
    if not md5_list:
        return set()

    tagged_hashes: set[str] = set()
    with get_cache_conn(cache_path) as conn:
        cur = conn.cursor()
        for idx in range(0, len(md5_list), 999):
            chunk = md5_list[idx : idx + 999]
            placeholders = ",".join(["?"] * len(chunk))
            cur.execute(
                f"""
                SELECT md5, general, character, artist, series
                FROM posts
                WHERE md5 IN ({placeholders})
                """,
                chunk,
            )
            for md5, gen, char, art, ser in cur.fetchall():
                if char or art or ser or (gen and gen.strip() != "tagme"):
                    tagged_hashes.add(md5)
    return tagged_hashes


def _extract_exif_camera(im: Image.Image) -> str | None:
    """Extracts physical camera make/model or exposure parameters from an open PIL Image."""
    try:
        exif = im.getexif()
        if not exif:
            return None
        make = str(exif.get(EXIF_MAKE, "")).strip().strip("\x00")
        model = str(exif.get(EXIF_MODEL, "")).strip().strip("\x00")
        combined_lower = f"{make} {model}".lower()
        if (make or model) and not any(ign in combined_lower for ign in IGNORED_EXIF_MODELS):
            return f"{make} {model}".strip()

        if sub_ifd := exif.get_ifd(EXIF_SUB_IFD):
            matched = [k for k in EXIF_CAMERA_PARAMS if k in sub_ifd]
            if len(matched) >= 2:
                lens = str(sub_ifd.get(42036, "")).strip().strip("\x00")
                return f"lens={lens}" if lens else f"{len(matched)}_exposure_params"
    except Exception:
        pass
    return None


def _evaluate_stage2_file(
    file_path: Path,
    min_size: int,
    max_ratio: float,
    reso_fluff: set[tuple[int, int]],
) -> tuple[TriageVerdict | None, float]:
    """Single-open header inspection returning (fluff_verdict_or_None, aspect_ratio)."""
    is_video = file_path.suffix.lower() in VIDEO_EXTS
    camera_info: str | None = None

    if is_video:
        width, height = get_video_resolution(file_path)
    else:
        width, height = None, None
        try:
            with Image.open(file_path) as im:
                width, height = im.size
                camera_info = _extract_exif_camera(im)
        except Exception:
            width, height = get_image_resolution(file_path)

    if not width or not height:
        return (
            TriageVerdict(file_path, "FLUFF", "Stage 2 (Geometry)", "corrupt_or_unreadable"),
            1.0,
        )

    w_over_h = width / height

    if any(abs(width - rw) <= 2 and abs(height - rh) <= 2 for rw, rh in reso_fluff):
        return (
            TriageVerdict(
                file_path, "FLUFF", "Stage 2 (Resolution)", f"matched_reso_fluff ({width}x{height})"
            ),
            w_over_h,
        )

    shortest = min(width, height)
    longest = max(width, height)
    ratio = longest / shortest

    if shortest < min_size:
        return (
            TriageVerdict(
                file_path,
                "FLUFF",
                "Stage 2 (Geometry)",
                f"small_dimensions ({width}x{height}, shortest={shortest}px < {min_size}px)",
            ),
            w_over_h,
        )

    if ratio > max_ratio or w_over_h >= 2.65:
        return (
            TriageVerdict(
                file_path,
                "FLUFF",
                "Stage 2 (Geometry)",
                f"banner_or_extreme_aspect_ratio ({width}x{height}, w/h={w_over_h:.2f})",
            ),
            w_over_h,
        )

    if camera_info:
        return (
            TriageVerdict(
                file_path,
                "FLUFF",
                "Stage 2 (EXIF)",
                f"camera_exif_detected ({camera_info})",
            ),
            w_over_h,
        )

    return None, w_over_h


def _pil_ensure_rgb_448(image: Image.Image) -> Image.Image:
    """Materializes an RGB PIL Image scaled so its longest side is EVA_SIZE (448px)."""
    if image.mode == "RGBA" or "transparency" in image.info:
        rgba = image.convert("RGBA")
        canvas = Image.new("RGBA", rgba.size, (255, 255, 255))
        canvas.alpha_composite(rgba)
        rgb = canvas.convert("RGB")
    else:
        rgb = image.convert("RGB")

    w, h = rgb.size
    scale = EVA_SIZE / max(w, h)
    new_w, new_h = max(1, int(w * scale)), max(1, int(h * scale))
    if (new_w, new_h) != (w, h):
        return rgb.resize((new_w, new_h), Image.Resampling.BICUBIC)
    return rgb


def _decode_and_prep_siglip(
    media_path: Path,
) -> tuple[Path, list[Image.Image], list[np.ndarray]]:
    """Worker function: decodes 1 image (or up to 5 video frames) at 448-max-side and preps 256x256 SigLIP arrays."""
    frames: list[Image.Image] = []
    if media_path.suffix.lower() in VIDEO_EXTS:
        try:
            frames = [
                _pil_ensure_rgb_448(f) for f in extract_video_frames(media_path, max_frames=5)
            ]
        except Exception:
            return media_path, [], []
    else:
        try:
            with Image.open(media_path) as im:
                frames = [_pil_ensure_rgb_448(im)]
        except Exception:
            return media_path, [], []

    siglip_arrays = [
        np.asarray(f.resize((SIGLIP_SIZE, SIGLIP_SIZE), Image.Resampling.BILINEAR), dtype=np.uint8)
        for f in frames
    ]
    return media_path, frames, siglip_arrays


def _prep_eva_pad_uint8(frames: list[Image.Image]) -> list[np.ndarray]:
    """Worker function: pads 448-scaled RGB PIL frames onto a 448x448 white square uint8 array for EVA02."""
    out: list[np.ndarray] = []
    for image in frames:
        w, h = image.size
        if (w, h) == (EVA_SIZE, EVA_SIZE):
            out.append(np.asarray(image, dtype=np.uint8))
            continue
        canvas = Image.new("RGB", (EVA_SIZE, EVA_SIZE), (255, 255, 255))
        canvas.paste(image, ((EVA_SIZE - w) // 2, (EVA_SIZE - h) // 2))
        out.append(np.asarray(canvas, dtype=np.uint8))
    return out


class DualTriageEngine:
    """Holds in-VRAM SigLIP-B-16-256 and EVA02 models with GPU-side uint8 tensor normalization."""

    def __init__(self, tagger_model_name: str, device: torch.device) -> None:
        self.device = device
        self.dtype = torch.float16 if device.type == "cuda" else torch.float32
        repo_id = MODEL_REPO_MAP[tagger_model_name]

        print(f"[INFO] Loading SigLIP zero-shot classifier ({SIGLIP_REPO}) on {device}...")
        self.siglip, _ = open_clip.create_model_from_pretrained(SIGLIP_REPO)
        self.siglip = self.siglip.eval().to(device, dtype=self.dtype)
        tokenizer = open_clip.get_tokenizer(SIGLIP_REPO)

        self.prompt_keys: list[str] = []
        prompt_texts: list[str] = []
        for group in (SIGLIP_KEEP_PROMPTS, SIGLIP_COMIC_PROMPTS, SIGLIP_FLUFF_PROMPTS):
            for k, text in group.items():
                self.prompt_keys.append(k)
                prompt_texts.append(text)

        with torch.inference_mode():
            tokens = tokenizer(prompt_texts).to(device)
            text_feat = self.siglip.encode_text(tokens)
            self.text_features = F.normalize(text_feat, dim=-1)

        print(f"[INFO] Loading SmilingWolf tagger ({repo_id}) on {device}...")
        self.tagger = timm.create_model(f"hf-hub:{repo_id}", pretrained=True).eval()
        self.tagger = self.tagger.to(device, dtype=self.dtype)
        self._load_tagger_labels(repo_id)

    def _load_tagger_labels(self, repo_id: str) -> None:
        try:
            csv_path = hf_hub_download(
                repo_id=repo_id, filename="selected_tags.csv", local_files_only=True
            )
        except (LocalEntryNotFoundError, FileNotFoundError):
            csv_path = hf_hub_download(repo_id=repo_id, filename="selected_tags.csv")

        df = pd.read_csv(csv_path, usecols=["name", "category"])
        self.clean_names: list[str] = [
            str(n).replace(" ", "_").lower() for n in df["name"].tolist()
        ]
        self.rating_idx: np.ndarray = np.where(df["category"] == 9)[0]
        self.general_idx: np.ndarray = np.where(df["category"] == 0)[0]
        self.char_idx: np.ndarray = np.where(df["category"] == 4)[0]

    def infer_siglip(
        self, uint8_frames: list[np.ndarray], batch_size: int
    ) -> list[dict[str, float]]:
        """Runs fast batched SigLIP inference on (256, 256, 3) uint8 arrays."""
        if not uint8_frames:
            return []

        all_siglip_dicts: list[dict[str, float]] = []
        sig_batch_size = max(16, batch_size * 4)

        for i in range(0, len(uint8_frames), sig_batch_size):
            chunk = np.stack(uint8_frames[i : i + sig_batch_size], axis=0)
            with torch.inference_mode():
                s_batch = torch.from_numpy(chunk).to(self.device).permute(0, 3, 1, 2).to(self.dtype)
                s_batch = s_batch.div_(127.5).sub_(1.0)

                img_feat = F.normalize(self.siglip.encode_image(s_batch), dim=-1)
                logits = img_feat @ self.text_features.T
                if getattr(self.siglip, "logit_scale", None) is not None:
                    logits = logits * self.siglip.logit_scale.exp()
                if getattr(self.siglip, "logit_bias", None) is not None:
                    logits = logits + self.siglip.logit_bias

                s_probs = torch.softmax(logits.float(), dim=-1).cpu().numpy()
                for row in s_probs:
                    all_siglip_dicts.append(
                        {k: float(v) for k, v in zip(self.prompt_keys, row, strict=True)}
                    )

        return all_siglip_dicts

    def infer_eva02(self, uint8_frames: list[np.ndarray], batch_size: int) -> list[np.ndarray]:
        """Runs batched EVA02 inference on (448, 448, 3) uint8 arrays with GPU BGR flip and normalization."""
        if not uint8_frames:
            return []

        all_tag_probs: list[np.ndarray] = []
        for i in range(0, len(uint8_frames), batch_size):
            chunk = np.stack(uint8_frames[i : i + batch_size], axis=0)
            with torch.inference_mode():
                t_batch = (
                    torch.from_numpy(chunk)
                    .to(self.device)
                    .permute(0, 3, 1, 2)
                    .flip(1)
                    .to(self.dtype)
                )
                t_batch = t_batch.div_(127.5).sub_(1.0)
                t_logits = self.tagger(t_batch)
                t_probs = torch.sigmoid(t_logits).float().cpu().numpy()
                all_tag_probs.extend(list(t_probs))

        return all_tag_probs


def _load_rating_weights() -> dict[str, int]:
    """Loads tag rating weights from tag_rating_dominant.db."""
    weights: dict[str, int] = {}
    if not TAG_DB_PATH.is_file():
        return weights
    with sqlite3.connect(TAG_DB_PATH) as conn:
        for row in conn.execute("SELECT tag_name, dominant_rating FROM dominant_tag_ratings"):
            if len(row) >= 2 and row[0]:
                weights[row[0].strip().lower()] = int(row[1])
    return weights


def _check_stage3a_siglip_fluff(
    src_path: Path,
    siglip_frames: list[dict[str, float]],
) -> tuple[TriageVerdict | None, dict[str, float]]:
    """Short-circuits obvious non-art in Stage 3A only when SigLIP fluff confidence is decisive (>= 0.65)."""
    avg_siglip = {
        k: sum(f[k] for f in siglip_frames) / len(siglip_frames) for k in siglip_frames[0]
    }
    sorted_siglip = dict(sorted(avg_siglip.items(), key=lambda kv: kv[1], reverse=True))

    best_keep_key = max(SIGLIP_KEEP_PROMPTS, key=lambda k: avg_siglip[k])
    best_keep_val = avg_siglip[best_keep_key]
    keep_sum = sum(avg_siglip[k] for k in SIGLIP_KEEP_PROMPTS)

    best_comic_key = max(SIGLIP_COMIC_PROMPTS, key=lambda k: avg_siglip[k])
    best_comic_val = avg_siglip[best_comic_key]

    best_fluff_key = max(SIGLIP_FLUFF_PROMPTS, key=lambda k: avg_siglip[k])
    best_fluff_val = avg_siglip[best_fluff_key]
    fluff_sum = sum(avg_siglip[k] for k in SIGLIP_FLUFF_PROMPTS)

    # Only short-circuit in Stage 3A when fluff probability mass is high (>= 0.65) and beats keep & comic;
    # borderline cases advance to Stage 3B so EVA02 can inspect exact tags.
    irl_is_borderline = best_fluff_key == "irl_photo" and best_fluff_val < 0.85 and keep_sum >= 0.12
    if (
        fluff_sum >= 0.65
        and best_fluff_val > best_keep_val
        and best_fluff_val > best_comic_val
        and best_fluff_key != "doll_or_figurine"
        and not irl_is_borderline
    ):
        return (
            TriageVerdict(
                src_path,
                "FLUFF",
                "Stage 3A (SigLIP)",
                f"{best_fluff_key} ({best_fluff_key}={best_fluff_val:.2f}, fluff_sum={fluff_sum:.2f}, keep_sum={keep_sum:.2f})",
                siglip_scores=sorted_siglip,
            ),
            sorted_siglip,
        )

    return None, sorted_siglip


def _build_import_tags_and_raw_maps(
    src_path: Path,
    frame_probs: list[np.ndarray],
    engine: DualTriageEngine,
    dir_as_artist: bool,
    gen_threshold: float,
    char_threshold: float,
    rating_weights: dict[str, int],
    group_info: tuple[dict[str, set[str]], set[str]],
) -> tuple[list[str], dict[str, float], dict[str, float], dict[str, float]]:
    """Vectorized NumPy aggregation of frame predictions into (import_tags, raw_chars, raw_gens, raw_ratings)."""
    child_to_bases, sex_group_tags = group_info
    valid_frames = len(frame_probs)
    names = engine.clean_names

    probs_2d = np.stack(frame_probs, axis=0)
    max_probs = probs_2d.max(axis=0)

    # 1. Ratings
    raw_ratings = {names[idx]: float(max_probs[idx]) for idx in engine.rating_idx}
    sorted_ratings = dict(sorted(raw_ratings.items(), key=lambda kv: kv[1], reverse=True))

    # 2. Characters (vectorized filter)
    char_indices = engine.char_idx
    char_max_arr = max_probs[char_indices]
    char_hits_arr = (probs_2d[:, char_indices] >= char_threshold).sum(axis=0)
    char_min_hits = 1 if valid_frames == 1 else min(3, valid_frames)

    raw_char_tags: dict[str, float] = {}
    approved_chars: list[str] = []
    for pos in np.flatnonzero(char_max_arr >= RAW_TAG_FLOOR):
        c_name = names[char_indices[pos]]
        c_val = float(char_max_arr[pos])
        raw_char_tags[c_name] = c_val
        if char_hits_arr[pos] >= char_min_hits:
            approved_chars.append(c_name)

    sorted_raw_chars = dict(sorted(raw_char_tags.items(), key=lambda kv: kv[1], reverse=True))
    approved_chars.sort(key=lambda c: raw_char_tags[c], reverse=True)

    # 3. General Tags (vectorized filter)
    gen_indices = engine.general_idx
    gen_max_arr = max_probs[gen_indices]
    gen_hits_arr = (probs_2d[:, gen_indices] >= gen_threshold).sum(axis=0)
    gen_min_hits = 1 if valid_frames == 1 else min(2, valid_frames)
    qe_min_hits = 1 if valid_frames == 1 else min(2, valid_frames)

    raw_gen_tags: dict[str, float] = {}
    gen_max_conf: dict[str, float] = {}
    approved_gens: set[str] = set()

    for pos in np.flatnonzero(gen_max_arr >= RAW_TAG_FLOOR):
        g_name = names[gen_indices[pos]]
        g_val = float(gen_max_arr[pos])
        raw_gen_tags[g_name] = g_val

        hits = int(gen_hits_arr[pos])
        if hits > 0:
            gen_max_conf[g_name] = g_val
            is_qe = rating_weights.get(g_name, 0) >= SMIN_QUESTIONABLE or g_name in sex_group_tags
            if hits >= (qe_min_hits if is_qe else gen_min_hits):
                approved_gens.add(g_name)
                for parent_base in child_to_bases.get(g_name, ()):
                    approved_gens.add(parent_base)
                    gen_max_conf[parent_base] = max(gen_max_conf.get(parent_base, 0.0), g_val)

    sorted_raw_gens = dict(sorted(raw_gen_tags.items(), key=lambda kv: kv[1], reverse=True))
    sorted_gens = sorted(approved_gens, key=lambda g: gen_max_conf.get(g, 0.0), reverse=True)

    import_tags: list[str] = []
    if dir_as_artist:
        artist_name = src_path.resolve().parent.name.lower().replace(" ", "_")
        import_tags.append(f"artist:{artist_name}")
    import_tags.extend(f"character:{c}" for c in approved_chars)
    import_tags.extend(sorted_gens)

    return import_tags, sorted_raw_chars, sorted_raw_gens, sorted_ratings


def _classify_stage3b_verdict(
    src_path: Path,
    sorted_siglip: dict[str, float],
    raw_chars: dict[str, float],
    raw_gens: dict[str, float],
    raw_ratings: dict[str, float],
    import_tags: list[str],
    gen_threshold: float,
    aspect_ratio: float = 1.0,
) -> TriageVerdict:
    """Applies EVA02 + SigLIP rules on Stage 3A survivors to classify KEEP, COMIC, or FLUFF."""
    best_keep_key = max(SIGLIP_KEEP_PROMPTS, key=lambda k: sorted_siglip[k])
    best_keep_val = sorted_siglip[best_keep_key]
    keep_sum = sum(sorted_siglip[k] for k in SIGLIP_KEEP_PROMPTS)

    best_comic_val = max(sorted_siglip[k] for k in SIGLIP_COMIC_PROMPTS)

    best_fluff_key = max(SIGLIP_FLUFF_PROMPTS, key=lambda k: sorted_siglip[k])
    best_fluff_val = sorted_siglip[best_fluff_key]
    fluff_sum = sum(sorted_siglip[k] for k in SIGLIP_FLUFF_PROMPTS)

    # 1. Reference Sheet Protection (when not a commission price menu or UI screenshot)
    is_ref_sheet = (
        raw_gens.get("reference_sheet", 0.0) >= 0.35
        or sorted_siglip.get("reference_sheet", 0.0) >= 0.35
    )
    if is_ref_sheet and sorted_siglip.get("promo_or_price_sheet", 0.0) < 0.30:
        return TriageVerdict(
            src_path,
            "KEEP",
            "Stage 3B (RefSheet)",
            f"reference_sheet (siglip={sorted_siglip.get('reference_sheet', 0.0):.2f}, eva={raw_gens.get('reference_sheet', 0.0):.2f})",
            sorted_siglip,
            raw_chars,
            raw_gens,
            raw_ratings,
            import_tags,
        )

    # 2. Comic / 4koma / Manga Routing (Evaluated BEFORE text_focus / crop rules so comics aren't lost to fluff)
    danbooru_comic_conf = max((raw_gens.get(t, 0.0) for t in COMIC_DANBOORU_TAGS), default=0.0)
    comic_blocker_conf = max((raw_gens.get(t, 0.0) for t in COMIC_BLOCKER_TAGS), default=0.0)
    has_dialogue = (
        raw_gens.get("speech_bubble", 0.0) >= 0.28 or raw_gens.get("thought_bubble", 0.0) >= 0.28
    )
    has_comic_layout = (
        raw_gens.get("multiple_views", 0.0) >= 0.25
        or raw_gens.get("monochrome", 0.0) >= 0.35
        or raw_gens.get("greyscale", 0.0) >= 0.35
    )
    if comic_blocker_conf < 0.22 and (
        danbooru_comic_conf >= 0.30
        or (best_comic_val >= 0.35 and best_comic_val >= best_keep_val * 0.70)
        or (has_dialogue and (danbooru_comic_conf >= 0.15 or has_comic_layout))
    ):
        return TriageVerdict(
            src_path,
            "COMIC",
            "Stage 3B (Comic)",
            f"comic_or_manga (eva_comic={danbooru_comic_conf:.2f}, siglip_comic={best_comic_val:.2f})",
            sorted_siglip,
            raw_chars,
            raw_gens,
            raw_ratings,
            import_tags,
        )

    # 3. EVA02 Hard Fluff Tags (UI, Fake Screenshots, Cards, Logos, Chat Logs, Merch, IRL Photos)
    hard_fluff_hits = [
        (t, raw_gens[t]) for t in EVA_HARD_FLUFF_TAGS if raw_gens.get(t, 0.0) >= 0.22
    ]
    if hard_fluff_hits:
        fluff_tag, fluff_conf = max(hard_fluff_hits, key=lambda kv: kv[1])
        return TriageVerdict(
            src_path,
            "FLUFF",
            "Stage 3B (EVA02)",
            f"eva_fluff_tag ({fluff_tag}={fluff_conf:.2f})",
            sorted_siglip,
            raw_chars,
            raw_gens,
            raw_ratings,
            import_tags,
        )

    fig_conf = raw_gens.get("figure", 0.0)
    is_bar_censored = raw_gens.get("bar_censor", 0.0) >= 0.30
    is_censored_cg = (
        is_bar_censored
        or raw_gens.get("mosaic_censoring", 0.0) >= 0.35
        or raw_gens.get("censored", 0.0) >= 0.50
    )
    has_photo_signal = (
        raw_gens.get("photo_(medium)", 0.0) >= 0.15
        or raw_gens.get("real_life", 0.0) >= 0.15
        or raw_gens.get("cosplay", 0.0) >= 0.30
    )
    if (
        not is_censored_cg
        and fig_conf >= 0.30
        and (
            fig_conf >= 0.80
            or has_photo_signal
            or sorted_siglip.get("doll_or_figurine", 0.0) >= 0.35
        )
    ):
        return TriageVerdict(
            src_path,
            "FLUFF",
            "Stage 3B (EVA02)",
            f"eva_figure_fluff (figure={fig_conf:.2f}, doll={sorted_siglip.get('doll_or_figurine', 0.0):.2f})",
            sorted_siglip,
            raw_chars,
            raw_gens,
            raw_ratings,
            import_tags,
        )

    # Rough sketch / WIP filter (high threshold to spare finished loose-lineart styles)
    sketch_conf = raw_gens.get("sketch", 0.0)
    is_draft_style = (
        raw_gens.get("lineart", 0.0) >= 0.35
        or raw_gens.get("monochrome", 0.0) >= 0.50
        or raw_gens.get("greyscale", 0.0) >= 0.50
        or raw_gens.get("wip", 0.0) >= 0.25
    )
    if sketch_conf >= 0.70 or (sketch_conf >= 0.55 and is_draft_style):
        return TriageVerdict(
            src_path,
            "FLUFF",
            "Stage 3B (EVA02)",
            f"eva_sketch_fluff (sketch={sketch_conf:.2f})",
            sorted_siglip,
            raw_chars,
            raw_gens,
            raw_ratings,
            import_tags,
        )

    # 4. Schedule Banners, Roster Grids, Diagram Arrows & Silhouette Memes
    max_text_lang = max((raw_gens.get(t, 0.0) for t in TEXT_LANG_TAGS), default=0.0)
    if sorted_siglip.get("promo_or_price_sheet", 0.0) >= 0.12 and max_text_lang >= 0.70:
        return TriageVerdict(
            src_path,
            "FLUFF",
            "Stage 3B (Schedule/Promo)",
            f"schedule_or_promo_banner (promo={sorted_siglip.get('promo_or_price_sheet', 0.0):.2f}, text={max_text_lang:.2f})",
            sorted_siglip,
            raw_chars,
            raw_gens,
            raw_ratings,
            import_tags,
        )

    if (raw_gens.get("6+girls", 0.0) >= 0.50 and raw_gens.get("6+boys", 0.0) >= 0.50) or (
        raw_gens.get("4girls", 0.0) >= 0.85
        and raw_gens.get("character_name", 0.0) >= 0.35
        and raw_gens.get("upper_body", 0.0) < 0.30
    ):
        return TriageVerdict(
            src_path,
            "FLUFF",
            "Stage 3B (RosterGrid)",
            f"character_roster_or_icon_grid (4girls={raw_gens.get('4girls', 0.0):.2f}, 6+girls={raw_gens.get('6+girls', 0.0):.2f})",
            sorted_siglip,
            raw_chars,
            raw_gens,
            raw_ratings,
            import_tags,
        )

    if (
        raw_gens.get("arrow_(symbol)", 0.0) >= 0.50
        and raw_gens.get("simple_background", 0.0) >= 0.40
    ):
        return TriageVerdict(
            src_path,
            "FLUFF",
            "Stage 3B (Diagram)",
            f"diagram_or_comparison_arrows (arrow={raw_gens.get('arrow_(symbol)', 0.0):.2f})",
            sorted_siglip,
            raw_chars,
            raw_gens,
            raw_ratings,
            import_tags,
        )

    if (
        raw_gens.get("silhouette", 0.0) >= 0.65
        and raw_gens.get("simple_background", 0.0) >= 0.50
        and raw_gens.get("blank_eyes", 0.0) >= 0.30
    ):
        return TriageVerdict(
            src_path,
            "FLUFF",
            "Stage 3B (Silhouette)",
            f"silhouette_doodle (silhouette={raw_gens.get('silhouette', 0.0):.2f}, blank_eyes={raw_gens.get('blank_eyes', 0.0):.2f})",
            sorted_siglip,
            raw_chars,
            raw_gens,
            raw_ratings,
            import_tags,
        )

    # 5. Disembodied Crop / Torso-Only / PNG Grid Head-Cutout Rules
    has_eye_color = any(raw_gens.get(t, 0.0) >= gen_threshold for t in VISIBLE_EYE_COLOR_TAGS)
    has_face_anchor = any(raw_gens.get(t, 0.0) >= gen_threshold for t in VISIBLE_FACE_ANCHOR_TAGS)

    crop_hits = [(t, raw_gens[t]) for t in HEAVY_CROP_TAGS if raw_gens.get(t, 0.0) >= 0.25]
    best_crop_tag, best_crop_conf = max(crop_hits, key=lambda kv: kv[1], default=("", 0.0))
    closeup_hits = [(t, raw_gens[t]) for t in CLOSEUP_FOCUS_TAGS if raw_gens.get(t, 0.0) >= 0.30]
    best_closeup_tag, best_closeup_conf = max(closeup_hits, key=lambda kv: kv[1], default=("", 0.0))

    if not has_eye_color and (
        best_crop_conf >= 0.25 or (best_closeup_conf >= 0.30 and not has_face_anchor)
    ):
        return TriageVerdict(
            src_path,
            "FLUFF",
            "Stage 3B (Crop)",
            f"disembodied_crop ({best_crop_tag or best_closeup_tag}={max(best_crop_conf, best_closeup_conf):.2f}, no_eye_color)",
            sorted_siglip,
            raw_chars,
            raw_gens,
            raw_ratings,
            import_tags,
        )

    body_framing_conf = max(
        raw_gens.get("upper_body", 0.0),
        raw_gens.get("full_body", 0.0),
        raw_gens.get("cowboy_shot", 0.0),
        raw_gens.get("lower_body", 0.0),
    )
    if (
        raw_gens.get("checkered_background", 0.0) >= 0.60
        and raw_gens.get("portrait", 0.0) >= 0.50
        and body_framing_conf < 0.20
    ):
        return TriageVerdict(
            src_path,
            "FLUFF",
            "Stage 3B (Crop)",
            f"png_grid_head_crop (checkered_bg={raw_gens.get('checkered_background', 0.0):.2f}, portrait={raw_gens.get('portrait', 0.0):.2f})",
            sorted_siglip,
            raw_chars,
            raw_gens,
            raw_ratings,
            import_tags,
        )

    if (
        aspect_ratio >= 1.80
        and raw_gens.get("portrait", 0.0) >= 0.55
        and body_framing_conf < 0.20
        and (
            raw_gens.get("simple_background", 0.0) >= 0.50 or raw_gens.get("close-up", 0.0) >= 0.20
        )
    ):
        return TriageVerdict(
            src_path,
            "FLUFF",
            "Stage 3B (Crop)",
            f"wide_header_face_crop (w/h={aspect_ratio:.2f}, portrait={raw_gens.get('portrait', 0.0):.2f})",
            sorted_siglip,
            raw_chars,
            raw_gens,
            raw_ratings,
            import_tags,
        )

    subject_conf = max((raw_gens.get(t, 0.0) for t in CORE_SUBJECT_TAGS), default=0.0)
    confident_gen_count = sum(1 for v in raw_gens.values() if v >= gen_threshold)

    # 6. SigLIP Combined Fluff vs Keep Balance (for borderline cases that passed Stage 3A)
    is_strong_cg_override = best_fluff_key == "irl_photo" and (
        is_bar_censored
        or (
            best_fluff_val < 0.85
            and not has_photo_signal
            and subject_conf >= 0.80
            and confident_gen_count >= 12
            and keep_sum >= 0.20
        )
    )
    if (
        (fluff_sum >= keep_sum * 0.85 or best_fluff_val > best_keep_val)
        and best_fluff_key != "doll_or_figurine"
        and not is_strong_cg_override
    ):
        return TriageVerdict(
            src_path,
            "FLUFF",
            "Stage 3B (SigLIP)",
            f"{best_fluff_key} ({best_fluff_key}={best_fluff_val:.2f}, fluff_sum={fluff_sum:.2f} vs keep_sum={keep_sum:.2f})",
            sorted_siglip,
            raw_chars,
            raw_gens,
            raw_ratings,
            import_tags,
        )

    # 7. Strict Minimum Booru Subject & Feature Density Gate
    no_humans_conf = raw_gens.get("no_humans", 0.0)

    if (
        confident_gen_count < 6
        or subject_conf < 0.45
        or (
            no_humans_conf >= 0.35
            and raw_gens.get("furry", 0.0) < 0.35
            and raw_gens.get("pokemon_(creature)", 0.0) < 0.35
        )
    ):
        return TriageVerdict(
            src_path,
            "FLUFF",
            "Stage 3B (Density)",
            f"weak_booru_subject (subj={subject_conf:.2f}, tags={confident_gen_count}, no_humans={no_humans_conf:.2f})",
            sorted_siglip,
            raw_chars,
            raw_gens,
            raw_ratings,
            import_tags,
        )

    return TriageVerdict(
        src_path,
        "KEEP",
        "Stage 3B (SigLIP+EVA02)",
        f"{best_keep_key} ({best_keep_key}={best_keep_val:.2f}, subj={subject_conf:.2f}, tags={confident_gen_count})",
        sorted_siglip,
        raw_chars,
        raw_gens,
        raw_ratings,
        import_tags,
    )


def _write_unified_sidecar(target_media_path: Path, verdict: TriageVerdict) -> None:
    """Writes the unified sidecar with commented diagnostic headers and the clean import tag line."""
    lines: list[str] = [
        f"# Verdict: {verdict.category} | {verdict.stage} | {verdict.reason}",
    ]

    if verdict.siglip_scores:
        sig_str = ", ".join(f"{k}={v:.3f}" for k, v in verdict.siglip_scores.items())
        lines.append(f"# SigLIP: {sig_str}")

    if verdict.raw_ratings or verdict.raw_char_tags or verdict.raw_gen_tags:
        parts: list[str] = []
        parts.extend(f"rating:{k}={v:.3f}" for k, v in verdict.raw_ratings.items())
        parts.extend(f"char:{k}={v:.3f}" for k, v in verdict.raw_char_tags.items())
        parts.extend(f"{k}={v:.3f}" for k, v in verdict.raw_gen_tags.items())
        lines.append(f"# Full Tags: {', '.join(parts)}")

    if verdict.import_tags:
        lines.append(", ".join(verdict.import_tags))

    sidecar_path = target_media_path.with_suffix(".txt")
    sidecar_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _route_file_and_sidecar(
    verdict: TriageVerdict,
    base_dir: Path,
    fluff_dir: Path,
    comic_dir: Path,
) -> None:
    """Moves COMIC/FLUFF files to their mirrored side-directories and writes the unified .txt sidecar."""
    src_path = verdict.file_path
    if not src_path.exists():
        return

    if verdict.category == "KEEP":
        _write_unified_sidecar(src_path, verdict)
        return

    dest_root = comic_dir if verdict.category == "COMIC" else fluff_dir
    rel_path = _safe_rel_path(src_path, base_dir)
    dst_path = dest_root / rel_path
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src_path), str(dst_path))

    old_sidecar = src_path.with_suffix(".txt")
    if old_sidecar.is_file() and _is_triage_sidecar(old_sidecar):
        old_sidecar.unlink()

    _write_unified_sidecar(dst_path, verdict)


def _write_triage_log(
    log_path: Path,
    base_dir: Path,
    fluff_dir: Path,
    comic_dir: Path,
    total_scanned: int,
    kept_stage1: int,
    verdicts: list[TriageVerdict],
    dry_run: bool,
) -> None:
    """Writes the master triage summary log."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    kept_s3 = [v for v in verdicts if v.category == "KEEP"]
    comics = [v for v in verdicts if v.category == "COMIC"]
    fluffs = [v for v in verdicts if v.category == "FLUFF"]

    with log_path.open("w", encoding="utf-8") as f:
        mode_str = "DRY RUN (No files moved or sidecars written)" if dry_run else "LIVE RUN"
        f.write(f"=== SHIMMIE2 TRIAGE REPORT ({mode_str}) ===\n")
        f.write(f"Source Directory:      {base_dir}\n")
        f.write(f"Comic Destination:     {comic_dir}\n")
        f.write(f"Fluff Destination:     {fluff_dir}\n")
        f.write(f"Total Files Scanned:   {total_scanned:,}\n")
        f.write(f"Kept (Stage 1 MD5):    {kept_stage1:,}\n")
        f.write(f"Kept (Stage 3 Art):    {len(kept_s3):,}\n")
        f.write(f"Routed to Comic:       {len(comics):,}\n")
        f.write(f"Routed to Fluff:       {len(fluffs):,}\n\n")

        for label, bucket in (("COMIC", comics), ("FLUFF", fluffs), ("KEEP (STAGE 3)", kept_s3)):
            f.write(f"=== {label} ({len(bucket):,} files) ===\n")
            for item in sorted(bucket, key=lambda x: str(x.file_path)):
                rel = _safe_rel_path(item.file_path, base_dir)
                top_sig = ", ".join(f"{k}={v:.2f}" for k, v in list(item.siglip_scores.items())[:3])
                top_eva = ", ".join(f"{k}={v:.2f}" for k, v in list(item.raw_gen_tags.items())[:6])
                sig_part = f" | SigLIP: [{top_sig}]" if top_sig else ""
                eva_part = f" | EVA02: [{top_eva}]" if top_eva else ""
                f.write(f"  - {rel} | {item.stage} | {item.reason}{sig_part}{eva_part}\n")
            f.write("\n")

    print(f"\n[INFO] Master triage report written to {log_path.resolve()}")


def run_triage(args) -> None:
    """Main CLI entry point for triage."""
    base_dir = Path(args.path).resolve()
    if not base_dir.is_dir():
        print(f"[ERROR] Target directory not found: {base_dir}")
        return

    fluff_dir = (
        Path(args.fluff_dir).resolve()
        if getattr(args, "fluff_dir", None)
        else base_dir.with_name(f"{base_dir.name}-fluff")
    )
    comic_dir = (
        Path(args.comic_dir).resolve()
        if getattr(args, "comic_dir", None)
        else base_dir.with_name(f"{base_dir.name}-comic")
    )

    is_full_reset = bool(getattr(args, "full_reset", False))
    is_full_retriage = bool(getattr(args, "full_retriage", False))
    is_soft_reset = bool(getattr(args, "reset", False))
    is_fast_retriage = bool(getattr(args, "retriage", False))

    if is_full_reset or is_full_retriage:
        print("[*] Full reset: restoring moved files and deleting '# Verdict:' sidecars...")
        restored, removed_txt = _reset_triage_state(
            base_dir, fluff_dir, comic_dir, delete_triage_sidecars=True
        )
        print(
            f"  -> Restored {restored:,} moved files and removed {removed_txt:,} triage (# Verdict:) sidecars.\n"
        )
        if is_full_reset:
            return
    elif is_soft_reset or is_fast_retriage:
        print("[*] Soft reset: restoring moved files while retaining '# Verdict:' sidecars...")
        restored, _ = _reset_triage_state(
            base_dir, fluff_dir, comic_dir, delete_triage_sidecars=False
        )
        print(f"  -> Restored {restored:,} moved files (triage sidecars retained).\n")
        if is_soft_reset:
            return

    protected_thumbs: set[str] = set()
    if getattr(args, "protect_thumbs", False):
        protected_thumbs = _load_protected_thumb_paths(base_dir)

    reso_fluff = _parse_reso_fluff(getattr(args, "reso_fluff", ""))

    candidates = _collect_candidate_files(
        base_dir,
        IMAGE_EXTS | VIDEO_EXTS,
        fluff_dir,
        comic_dir,
        include_triage_sidecars=is_fast_retriage,
    )
    if not candidates:
        print("[✓] No untagged candidate media files found to triage.")
        return

    print("=== Content Identification & 3-Way Triage ===")
    print(f"📁  Source Path:      {base_dir}")
    print(f"📚  Comic Side-Dir:   {comic_dir}")
    print(f"🗑️  Fluff Side-Dir:   {fluff_dir}")
    if is_fast_retriage:
        print("⚡  Mode:             Fast Retriage (re-evaluating cached sidecar scores)")
    if protected_thumbs:
        print(f"🛡️  Protected Thumbs: {len(protected_thumbs):,} thumbnail_only preview(s)")
    if reso_fluff:
        print(f"📐  Reso Fluff:       {', '.join(f'{w}x{h}' for w, h in sorted(reso_fluff))}")
    print(f"🔍  Candidates:       {len(candidates):,} files")
    print(f"🧵  Worker Threads:   {args.threads}")
    print(f"🧪  Dry Run:          {args.dry_run}\n")

    all_verdicts: list[TriageVerdict] = []
    kept_stage1 = 0
    cached_reused = 0

    try:
        # --- Stage 1: Fast Bulk MD5-Only Lookup (Zero Image Decoding) ---
        print("[Stage 1/3] Checking MD5 hashes against posts_cache.db and Shimmie2...")
        with ThreadPoolExecutor(max_workers=args.threads) as pool:
            md5_list = list(
                tqdm.tqdm(
                    pool.map(extract_or_compute_md5, candidates),
                    total=len(candidates),
                    desc="Extracting MD5s",
                    leave=False,
                )
            )

        db_config = get_shimmie_db_credentials(args.spath) if getattr(args, "spath", None) else None
        known_hashes = get_existing_shimmie_hashes(md5_list, db_config) | _get_cached_tagged_hashes(
            md5_list, CACHE_PATH
        )

        stage2_candidates: list[Path] = []
        for f_path, md5 in zip(candidates, md5_list, strict=True):
            if md5 in known_hashes:
                kept_stage1 += 1
            else:
                stage2_candidates.append(f_path)

        print(
            f"  -> Stage 1 complete: {kept_stage1:,} known MD5s kept in-place, {len(stage2_candidates):,} unknown files advancing to Stage 2."
        )

        # --- Stage 2: Single-Open Header Geometry, Fixed Resolution & Camera EXIF ---
        stage3_candidates: list[Path] = []
        if stage2_candidates:
            print(
                "\n[Stage 2/3] Running single-open resolution, geometry, and camera EXIF checks..."
            )
            with ThreadPoolExecutor(max_workers=args.threads) as pool:
                s2_iter = pool.map(
                    lambda f: (
                        f,
                        *_evaluate_stage2_file(f, args.min_size, args.max_ratio, reso_fluff),
                    ),
                    stage2_candidates,
                )
                for f_path, verdict, aspect_ratio in tqdm.tqdm(
                    s2_iter, total=len(stage2_candidates), desc="Pre-filtering", leave=False
                ):
                    is_protected = os.path.abspath(f_path) in protected_thumbs
                    if verdict is not None and (
                        verdict.reason == "corrupt_or_unreadable"
                        or verdict.stage in {"Stage 2 (Resolution)", "Stage 2 (EXIF)"}
                        or not is_protected
                    ):
                        all_verdicts.append(verdict)
                        if not args.dry_run:
                            _route_file_and_sidecar(verdict, base_dir, fluff_dir, comic_dir)
                        continue

                    # Fast retriage: re-evaluate cached SigLIP + EVA02 sidecar scores on CPU
                    if is_fast_retriage and (cached := _parse_triage_sidecar(f_path)):
                        if cached.siglip_scores:
                            fluff_v, sorted_sig = _check_stage3a_siglip_fluff(
                                f_path, [cached.siglip_scores]
                            )
                            if fluff_v is not None and not is_protected:
                                all_verdicts.append(fluff_v)
                                if not args.dry_run:
                                    _route_file_and_sidecar(fluff_v, base_dir, fluff_dir, comic_dir)
                                cached_reused += 1
                                continue

                            has_eva = bool(
                                cached.raw_gen_tags or cached.raw_char_tags or cached.raw_ratings
                            )
                            if has_eva:
                                cached_v = _classify_stage3b_verdict(
                                    f_path,
                                    sorted_sig,
                                    cached.raw_char_tags,
                                    cached.raw_gen_tags,
                                    cached.raw_ratings,
                                    cached.import_tags,
                                    args.gen_threshold,
                                    aspect_ratio=aspect_ratio,
                                )
                                if cached_v.category == "FLUFF" and is_protected:
                                    cached_v = TriageVerdict(
                                        f_path,
                                        "KEEP",
                                        "Stage 3B (Protected Thumb)",
                                        f"protected_thumb (bypassed: {cached_v.stage} - {cached_v.reason})",
                                        cached_v.siglip_scores,
                                        cached_v.raw_char_tags,
                                        cached_v.raw_gen_tags,
                                        cached_v.raw_ratings,
                                        cached_v.import_tags,
                                    )
                                all_verdicts.append(cached_v)
                                if not args.dry_run:
                                    _route_file_and_sidecar(
                                        cached_v, base_dir, fluff_dir, comic_dir
                                    )
                                cached_reused += 1
                                continue

                    stage3_candidates.append(f_path)

            s2_rejected = len(stage2_candidates) - len(stage3_candidates) - cached_reused
            print(
                f"  -> Stage 2 complete: {s2_rejected:,} routed to fluff, "
                f"{cached_reused:,} re-evaluated from cached sidecars, "
                f"{len(stage3_candidates):,} advancing to GPU Stage 3."
            )

        # --- Stage 3: Cascaded SigLIP (3A) -> EVA02 (3B) ---
        if stage3_candidates:
            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            engine = DualTriageEngine(args.model, device)
            rating_weights = _load_rating_weights()
            child_to_parents, _, _, sex_group_tags = load_tag_groups_metadata()
            group_info = (child_to_parents, sex_group_tags)

            chunk_size = max(64, args.batch * 8)
            print(
                f"\n[Stage 3/3] Running cascaded SigLIP -> {args.model} on {len(stage3_candidates):,} files (chunk={chunk_size})..."
            )

            with ThreadPoolExecutor(max_workers=args.threads) as pool:
                for start in tqdm.tqdm(
                    range(0, len(stage3_candidates), chunk_size), desc="GPU Chunks"
                ):
                    sub_files = stage3_candidates[start : start + chunk_size]

                    # Parallel decode + 256x256 SigLIP prep
                    decoded_items = list(pool.map(_decode_and_prep_siglip, sub_files))

                    siglip_flat_arrays: list[np.ndarray] = []
                    valid_decoded: list[tuple[Path, list[Image.Image], int, int]] = []

                    for f_path, pil_frames, sig_arrays in decoded_items:
                        if not sig_arrays:
                            v_err = TriageVerdict(
                                f_path, "FLUFF", "Stage 3 (Decode)", "unreadable_media"
                            )
                            all_verdicts.append(v_err)
                            if not args.dry_run:
                                _route_file_and_sidecar(v_err, base_dir, fluff_dir, comic_dir)
                            continue
                        i_start = len(siglip_flat_arrays)
                        siglip_flat_arrays.extend(sig_arrays)
                        valid_decoded.append((f_path, pil_frames, i_start, len(siglip_flat_arrays)))

                    if not siglip_flat_arrays:
                        continue

                    # Stage 3A: Fast SigLIP Medium Filter
                    siglip_out_list = engine.infer_siglip(siglip_flat_arrays, args.batch)
                    eva_survivors: list[tuple[Path, list[Image.Image], dict[str, float]]] = []

                    for f_path, pil_frames, i_start, i_end in valid_decoded:
                        f_siglip = siglip_out_list[i_start:i_end]
                        fluff_verdict, sorted_siglip = _check_stage3a_siglip_fluff(f_path, f_siglip)
                        if (
                            fluff_verdict is not None
                            and os.path.abspath(f_path) not in protected_thumbs
                        ):
                            all_verdicts.append(fluff_verdict)
                            if not args.dry_run:
                                _route_file_and_sidecar(
                                    fluff_verdict, base_dir, fluff_dir, comic_dir
                                )
                        else:
                            eva_survivors.append((f_path, pil_frames, sorted_siglip))

                    if not eva_survivors:
                        continue

                    # Stage 3B: Parallel 448x448 pad + EVA02 inference on survivors only
                    eva_padded_lists = list(
                        pool.map(lambda item: _prep_eva_pad_uint8(item[1]), eva_survivors)
                    )
                    eva_flat_arrays: list[np.ndarray] = []
                    eva_slices: list[tuple[Path, dict[str, float], float, int, int]] = []

                    for (f_path, pil_frames, sorted_siglip), pad_arrays in zip(
                        eva_survivors, eva_padded_lists, strict=True
                    ):
                        fw, fh = pil_frames[0].size
                        aspect_ratio = fw / fh if fh > 0 else 1.0
                        e_start = len(eva_flat_arrays)
                        eva_flat_arrays.extend(pad_arrays)
                        eva_slices.append(
                            (f_path, sorted_siglip, aspect_ratio, e_start, len(eva_flat_arrays))
                        )

                    eva_probs_list = engine.infer_eva02(eva_flat_arrays, args.batch)

                    for f_path, sorted_siglip, aspect_ratio, e_start, e_end in eva_slices:
                        f_tag_probs = eva_probs_list[e_start:e_end]
                        import_tags, raw_chars, raw_gens, raw_ratings = (
                            _build_import_tags_and_raw_maps(
                                f_path,
                                f_tag_probs,
                                engine,
                                args.dir_as_artist,
                                args.gen_threshold,
                                args.char_threshold,
                                rating_weights,
                                group_info,
                            )
                        )
                        verdict = _classify_stage3b_verdict(
                            f_path,
                            sorted_siglip,
                            raw_chars,
                            raw_gens,
                            raw_ratings,
                            import_tags,
                            args.gen_threshold,
                            aspect_ratio=aspect_ratio,
                        )
                        if (
                            verdict.category == "FLUFF"
                            and os.path.abspath(f_path) in protected_thumbs
                        ):
                            verdict = TriageVerdict(
                                f_path,
                                "KEEP",
                                "Stage 3B (Protected Thumb)",
                                f"protected_thumb (bypassed: {verdict.stage} - {verdict.reason})",
                                verdict.siglip_scores,
                                verdict.raw_char_tags,
                                verdict.raw_gen_tags,
                                verdict.raw_ratings,
                                verdict.import_tags,
                            )
                        all_verdicts.append(verdict)
                        if not args.dry_run:
                            _route_file_and_sidecar(verdict, base_dir, fluff_dir, comic_dir)

    finally:
        if all_verdicts or args.dry_run:
            log_dest = (
                (base_dir / "triage_dry_run.log") if args.dry_run else (fluff_dir / "triage.log")
            )
            _write_triage_log(
                log_dest,
                base_dir,
                fluff_dir,
                comic_dir,
                len(candidates),
                kept_stage1,
                all_verdicts,
                args.dry_run,
            )

    kept_s3_cnt = sum(1 for v in all_verdicts if v.category == "KEEP")
    comic_cnt = sum(1 for v in all_verdicts if v.category == "COMIC")
    fluff_cnt = sum(1 for v in all_verdicts if v.category == "FLUFF")
    action_word = "Classified" if args.dry_run else "Routed"

    print(
        f"\n[✓] Triage complete ({action_word}): "
        f"Kept {kept_stage1 + kept_s3_cnt:,} ({kept_stage1:,} MD5 + {kept_s3_cnt:,} AI) | "
        f"Comics: {comic_cnt:,} | Fluff: {fluff_cnt:,}"
    )
