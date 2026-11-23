"""Shared in-process SmilingWolf GPU tagger, frame preprocessor, and consensus aggregator."""

import logging
import os
import warnings
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import timm
import torch
from huggingface_hub import hf_hub_download
from huggingface_hub.errors import LocalEntryNotFoundError
from PIL import Image

from functions.common import VIDEO_EXTS
from functions.media import extract_video_frames

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
TAGGER_SIZE = 448
SMIN_QUESTIONABLE = 51
RAW_TAG_FLOOR = 0.15


@dataclass(slots=True)
class TagPredictions:
    """Aggregated multi-frame predictions returned by aggregate_frame_predictions()."""

    approved_chars: list[str]
    approved_gens: list[str]
    char_scores: dict[str, float]
    gen_scores: dict[str, float]
    rating_scores: dict[str, float]


def pil_ensure_rgb_448(image: Image.Image) -> Image.Image:
    """Materializes an RGB PIL Image scaled so its longest side is TAGGER_SIZE (448px)."""
    if image.mode in ("RGBA", "LA") or "transparency" in image.info:
        rgba = image.convert("RGBA")
        canvas = Image.new("RGBA", rgba.size, (255, 255, 255, 255))
        canvas.alpha_composite(rgba)
        rgb = canvas.convert("RGB")
    else:
        rgb = image.convert("RGB")

    w, h = rgb.size
    scale = TAGGER_SIZE / max(w, h)
    new_w, new_h = max(1, int(w * scale)), max(1, int(h * scale))
    if (new_w, new_h) != (w, h):
        return rgb.resize((new_w, new_h), Image.Resampling.BICUBIC)
    return rgb


def prep_tagger_pad_uint8(frames: list[Image.Image]) -> list[np.ndarray]:
    """Pads 448-scaled RGB PIL frames onto a 448x448 white square uint8 array."""
    out: list[np.ndarray] = []
    for image in frames:
        w, h = image.size
        if (w, h) == (TAGGER_SIZE, TAGGER_SIZE):
            out.append(np.asarray(image, dtype=np.uint8))
            continue
        canvas = Image.new("RGB", (TAGGER_SIZE, TAGGER_SIZE), (255, 255, 255))
        canvas.paste(image, ((TAGGER_SIZE - w) // 2, (TAGGER_SIZE - h) // 2))
        out.append(np.asarray(canvas, dtype=np.uint8))
    return out


def decode_media_for_tagger(
    media_path: Path, max_video_frames: int = 5
) -> tuple[Path, list[np.ndarray]]:
    """Worker function: decodes an image (or up to N video frames) into 448x448 uint8 arrays."""
    frames: list[Image.Image] = []
    if media_path.suffix.lower() in VIDEO_EXTS:
        try:
            frames = [
                pil_ensure_rgb_448(f)
                for f in extract_video_frames(media_path, max_frames=max_video_frames)
            ]
        except Exception:
            return media_path, []
    else:
        try:
            with Image.open(media_path) as im:
                frames = [pil_ensure_rgb_448(im)]
        except Exception:
            return media_path, []

    if not frames:
        return media_path, []
    return media_path, prep_tagger_pad_uint8(frames)


def resolve_torch_device() -> tuple[torch.device, torch.dtype]:
    """Auto-detects the best PyTorch accelerator (CUDA/ROCm, XPU, MPS, DirectML, CPU) and dtype.

    Optional environment overrides (no code edits needed):
      - SHIMMIE_DEVICE: 'cuda', 'cuda:0', 'xpu', 'mps', 'dml', or 'cpu'
      - SHIMMIE_DTYPE:  'fp16', 'bf16', or 'fp32'
    """
    override_dev = os.environ.get("SHIMMIE_DEVICE", "").strip().lower()
    override_dtype = os.environ.get("SHIMMIE_DTYPE", "").strip().lower()

    if override_dev == "dml":
        import torch_directml  # type: ignore[import-untyped]

        device = torch_directml.device()
    elif override_dev:
        device = torch.device(override_dev)
    elif torch.cuda.is_available():
        # Covers both NVIDIA CUDA and AMD ROCm (HIP exposes AMD GPUs via torch.cuda)
        device = torch.device("cuda")
    elif hasattr(torch, "xpu") and torch.xpu.is_available():
        device = torch.device("xpu")
    elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        device = torch.device("mps")
    else:
        try:
            import torch_directml  # type: ignore[import-untyped]

            device = torch_directml.device()
        except ImportError:
            device = torch.device("cpu")

    dtype_map = {
        "fp16": torch.float16,
        "float16": torch.float16,
        "bf16": torch.bfloat16,
        "bfloat16": torch.bfloat16,
        "fp32": torch.float32,
        "float32": torch.float32,
    }
    if override_dtype in dtype_map:
        dtype = dtype_map[override_dtype]
    elif device.type in {"cuda", "xpu", "mps"}:
        dtype = torch.float16
    else:
        # CPU and DirectML (privateuseone) default to fp32 for stability
        dtype = torch.float32

    return device, dtype


class SmilingWolfTagger:
    """In-process SmilingWolf v3 tagger with GPU-side uint8 BGR flip and fp16/fp32 inference."""

    def __init__(
        self,
        model_name: str,
        device: torch.device | None = None,
        dtype: torch.dtype | None = None,
    ) -> None:
        if device is None or dtype is None:
            auto_dev, auto_dtype = resolve_torch_device()
            device = device or auto_dev
            dtype = dtype or auto_dtype
        self.device = device
        self.dtype = dtype
        repo_id = MODEL_REPO_MAP[model_name]

        print(f"[INFO] Loading SmilingWolf tagger ({repo_id}) on {self.device} ({self.dtype})...")
        self.model = timm.create_model(f"hf-hub:{repo_id}", pretrained=True).eval()
        self.model = self.model.to(self.device, dtype=self.dtype)
        self._load_labels(repo_id)

    def _load_labels(self, repo_id: str) -> None:
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

    def infer_batch(self, uint8_frames: list[np.ndarray], batch_size: int) -> list[np.ndarray]:
        """Runs batched inference on (448, 448, 3) RGB uint8 arrays (converted to BGR on GPU)."""
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
                t_logits = self.model(t_batch)
                t_probs = torch.sigmoid(t_logits).float().cpu().numpy()
                all_tag_probs.extend(list(t_probs))

        return all_tag_probs


def aggregate_frame_predictions(
    frame_probs: list[np.ndarray],
    tagger: SmilingWolfTagger,
    gen_threshold: float,
    char_threshold: float,
    rating_weights: dict[str, int] | None = None,
    group_info: tuple[dict[str, set[str]], set[str]] | None = None,
    raw_floor: float | None = None,
) -> TagPredictions:
    """Vectorized NumPy aggregation of 1+ frame probability vectors with optional parent expansion."""
    child_to_bases, sex_group_tags = group_info if group_info is not None else ({}, set())
    weights = rating_weights if rating_weights is not None else {}
    valid_frames = len(frame_probs)
    names = tagger.clean_names

    probs_2d = np.stack(frame_probs, axis=0)
    max_probs = probs_2d.max(axis=0)

    # 1. Ratings
    raw_ratings = {names[idx]: float(max_probs[idx]) for idx in tagger.rating_idx}
    sorted_ratings = dict(sorted(raw_ratings.items(), key=lambda kv: kv[1], reverse=True))

    # 2. Characters
    char_indices = tagger.char_idx
    char_max_arr = max_probs[char_indices]
    char_hits_arr = (probs_2d[:, char_indices] >= char_threshold).sum(axis=0)
    char_min_hits = 1 if valid_frames == 1 else min(3, valid_frames)
    char_scan_floor = min(raw_floor, char_threshold) if raw_floor is not None else char_threshold

    char_scores: dict[str, float] = {}
    approved_chars: list[str] = []
    char_conf_for_sort: dict[str, float] = {}

    for pos in np.flatnonzero(char_max_arr >= char_scan_floor):
        c_name = names[char_indices[pos]]
        c_val = float(char_max_arr[pos])
        is_approved = int(char_hits_arr[pos]) >= char_min_hits
        if is_approved:
            approved_chars.append(c_name)
            char_conf_for_sort[c_name] = c_val
        if (raw_floor is not None and c_val >= raw_floor) or (raw_floor is None and is_approved):
            char_scores[c_name] = c_val

    sorted_char_scores = dict(sorted(char_scores.items(), key=lambda kv: kv[1], reverse=True))
    approved_chars.sort(key=lambda c: char_conf_for_sort[c], reverse=True)

    # 3. General Tags
    gen_indices = tagger.general_idx
    gen_max_arr = max_probs[gen_indices]
    gen_hits_arr = (probs_2d[:, gen_indices] >= gen_threshold).sum(axis=0)
    gen_min_hits = 1 if valid_frames == 1 else min(2, valid_frames)
    qe_min_hits = 1 if valid_frames == 1 else min(2, valid_frames)
    gen_scan_floor = min(raw_floor, gen_threshold) if raw_floor is not None else gen_threshold

    gen_scores: dict[str, float] = {}
    gen_max_conf: dict[str, float] = {}
    approved_gens: set[str] = set()

    for pos in np.flatnonzero(gen_max_arr >= gen_scan_floor):
        g_name = names[gen_indices[pos]]
        g_val = float(gen_max_arr[pos])
        hits = int(gen_hits_arr[pos])
        is_qe = weights.get(g_name, 0) >= SMIN_QUESTIONABLE or g_name in sex_group_tags
        is_approved = hits >= (qe_min_hits if is_qe else gen_min_hits)

        if is_approved:
            approved_gens.add(g_name)
            gen_max_conf[g_name] = max(gen_max_conf.get(g_name, 0.0), g_val)
            for parent_base in child_to_bases.get(g_name, ()):
                approved_gens.add(parent_base)
                gen_max_conf[parent_base] = max(gen_max_conf.get(parent_base, 0.0), g_val)

        if (raw_floor is not None and g_val >= raw_floor) or (raw_floor is None and is_approved):
            gen_scores[g_name] = g_val

    sorted_gen_scores = dict(sorted(gen_scores.items(), key=lambda kv: kv[1], reverse=True))
    sorted_gens = sorted(approved_gens, key=lambda g: gen_max_conf.get(g, 0.0), reverse=True)

    return TagPredictions(
        approved_chars=approved_chars,
        approved_gens=sorted_gens,
        char_scores=sorted_char_scores,
        gen_scores=sorted_gen_scores,
        rating_scores=sorted_ratings,
    )
