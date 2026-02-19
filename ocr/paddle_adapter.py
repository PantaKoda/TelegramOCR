"""Thin PaddleOCR adapter: Paddle output -> parser Box objects."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from parser.layout_parser import Box

try:
    from paddleocr import PaddleOCR
except ModuleNotFoundError:
    PaddleOCR = None


@dataclass(frozen=True)
class OCRBox(Box):
    confidence: float


def ensure_paddle_available() -> None:
    if PaddleOCR is None:
        raise RuntimeError("Missing dependency `paddleocr`. Run `uv sync` (or `uv add paddleocr paddlepaddle`).")


def create_paddle_ocr(*, lang: str = "sv") -> Any:
    ensure_paddle_available()
    normalized_lang = (lang or "").strip()
    if not normalized_lang:
        raise RuntimeError("OCR language must be a non-empty value.")
    return PaddleOCR(
        text_detection_model_name="PP-OCRv5_mobile_det",
        text_recognition_model_name="PP-OCRv5_mobile_rec",
        lang=normalized_lang,
        # Keep CPU inference on Paddle runtime path; oneDNN has shown
        # unsupported PIR attribute conversions in container deployments.
        enable_mkldnn=False,
        device="cpu",
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
    )


def paddle_page_to_boxes(page_result: Any) -> list[OCRBox]:
    dt_polys = page_result.get("dt_polys") if hasattr(page_result, "get") else None
    rec_texts = page_result.get("rec_texts") if hasattr(page_result, "get") else None
    rec_scores = page_result.get("rec_scores") if hasattr(page_result, "get") else None

    if not isinstance(dt_polys, list) or not isinstance(rec_texts, list) or not isinstance(rec_scores, list):
        return []

    count = min(len(dt_polys), len(rec_texts), len(rec_scores))
    boxes: list[OCRBox] = []
    for index in range(count):
        poly = dt_polys[index]
        text = rec_texts[index]
        score = rec_scores[index]

        coords = _normalize_polygon(poly)
        if coords is None:
            continue
        xs = [point[0] for point in coords]
        ys = [point[1] for point in coords]
        min_x = min(xs)
        max_x = max(xs)
        min_y = min(ys)
        max_y = max(ys)

        boxes.append(
            OCRBox(
                text=str(text),
                x=float(min_x),
                y=float(min_y),
                w=float(max_x - min_x),
                h=float(max_y - min_y),
                confidence=float(score),
            )
        )
    return boxes


def legacy_ocr_result_to_boxes(records: list[Any]) -> list[OCRBox]:
    """Convert legacy `PaddleOCR.ocr(...)` page output to OCRBox list."""
    boxes: list[OCRBox] = []
    for item in records:
        if not isinstance(item, list) or len(item) != 2:
            continue
        poly, text_info = item
        if not isinstance(text_info, (list, tuple)) or len(text_info) != 2:
            continue
        text, score = text_info

        coords = _normalize_polygon(poly)
        if coords is None:
            continue
        xs = [point[0] for point in coords]
        ys = [point[1] for point in coords]
        min_x = min(xs)
        max_x = max(xs)
        min_y = min(ys)
        max_y = max(ys)

        boxes.append(
            OCRBox(
                text=str(text),
                x=float(min_x),
                y=float(min_y),
                w=float(max_x - min_x),
                h=float(max_y - min_y),
                confidence=float(score),
            )
        )
    return boxes


def run_paddle_on_image(image_path: str | Path, ocr: Any | None = None) -> list[OCRBox]:
    resolved = Path(image_path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Image not found: {resolved}")

    client = ocr or create_paddle_ocr()
    pages = client.predict(str(resolved))
    boxes: list[OCRBox] = []
    for page in pages:
        boxes.extend(paddle_page_to_boxes(page))
    return boxes


def _normalize_polygon(poly: Any) -> list[tuple[float, float]] | None:
    if isinstance(poly, np.ndarray):
        try:
            poly = poly.tolist()
        except Exception:
            return None

    if not isinstance(poly, list):
        return None

    result: list[tuple[float, float]] = []
    for point in poly:
        if not isinstance(point, list) or len(point) != 2:
            return None
        try:
            x = float(point[0])
            y = float(point[1])
        except (TypeError, ValueError):
            return None
        result.append((x, y))
    if len(result) < 4:
        return None
    return result
