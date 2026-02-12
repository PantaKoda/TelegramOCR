#!/usr/bin/env python3
"""Extract OCR blocks from a mobile screenshot and group them into rows by geometry."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent
SUPPORTED_IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run PaddleOCR and output raw OCR JSON + row-grouped JSON."
    )
    parser.add_argument(
        "--image",
        type=Path,
        default=None,
        help="Input screenshot path. If omitted, auto-detects a single image in project root.",
    )
    parser.add_argument(
        "--raw-output",
        type=Path,
        default=PROJECT_ROOT / "ocr_raw.json",
        help="Output path for raw OCR JSON.",
    )
    parser.add_argument(
        "--rows-output",
        type=Path,
        default=PROJECT_ROOT / "ocr_rows.json",
        help="Output path for row-grouped JSON.",
    )
    parser.add_argument(
        "--row-y-threshold",
        type=float,
        default=None,
        help="Optional fixed Y-center threshold in pixels for row grouping.",
    )
    parser.add_argument(
        "--row-y-threshold-ratio",
        type=float,
        default=0.6,
        help="Relative threshold multiplier when --row-y-threshold is not set.",
    )
    return parser.parse_args()


def find_default_image() -> Path:
    images = sorted(
        path
        for path in PROJECT_ROOT.iterdir()
        if path.is_file() and path.suffix.lower() in SUPPORTED_IMAGE_SUFFIXES
    )
    if len(images) == 1:
        return images[0]
    if not images:
        raise RuntimeError("No image found in project root. Use --image <path>.")

    names = "\n".join(f"- {item.name}" for item in images)
    raise RuntimeError(f"Multiple images found. Pick one with --image:\n{names}")


def load_paddleocr_class():
    if sys.version_info >= (3, 13):
        raise RuntimeError(
            "Python 3.13 is not supported by PaddlePaddle wheels yet. "
            "Use Python 3.11 or 3.12."
        )

    try:
        import paddle  # noqa: F401
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "Missing dependency 'paddlepaddle'. Install it in this environment first."
        ) from error

    try:
        from paddleocr import PaddleOCR
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "Missing dependency 'paddleocr'. Install it in this environment first."
        ) from error

    return PaddleOCR


def normalize_polygon(poly: Any) -> List[List[float]]:
    if not isinstance(poly, list):
        return []

    points: List[List[float]] = []
    for point in poly:
        if isinstance(point, (list, tuple)) and len(point) >= 2:
            points.append([float(point[0]), float(point[1])])
    return points


def make_block(page_index: int, block_index: int, text: Any, score: Any, poly: Any) -> Dict[str, Any]:
    bbox = normalize_polygon(poly)
    xs = [p[0] for p in bbox] or [0.0]
    ys = [p[1] for p in bbox] or [0.0]

    x_min = min(xs)
    x_max = max(xs)
    y_min = min(ys)
    y_max = max(ys)

    return {
        "page_index": page_index,
        "block_index": block_index,
        "text": "" if text is None else str(text),
        "confidence": float(score) if score is not None else 0.0,
        "bbox": bbox,
        "x_min": x_min,
        "x_max": x_max,
        "y_min": y_min,
        "y_max": y_max,
        "x_center": (x_min + x_max) / 2.0,
        "y_center": (y_min + y_max) / 2.0,
        "width": x_max - x_min,
        "height": y_max - y_min,
    }


def flatten_pages(pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    for page_index, page in enumerate(pages):
        texts = page.get("rec_texts", [])
        scores = page.get("rec_scores", [])
        polys = page.get("rec_polys", [])

        count = min(len(texts), len(scores), len(polys))
        for block_index in range(count):
            blocks.append(
                make_block(
                    page_index,
                    block_index,
                    texts[block_index],
                    scores[block_index],
                    polys[block_index],
                )
            )
    return blocks


def row_threshold(current_avg_height: float, candidate_height: float, px: float | None, ratio: float) -> float:
    if px is not None:
        return px
    return max(current_avg_height, candidate_height, 1.0) * ratio


def group_rows(blocks: List[Dict[str, Any]], px: float | None, ratio: float) -> List[Dict[str, Any]]:
    sorted_blocks = sorted(blocks, key=lambda b: (b["page_index"], b["y_center"], b["x_min"]))

    rows: List[Dict[str, Any]] = []
    current: Dict[str, Any] | None = None

    for block in sorted_blocks:
        if current is None:
            current = {
                "page_index": block["page_index"],
                "items": [block],
                "mean_y": block["y_center"],
                "avg_height": block["height"],
            }
            rows.append(current)
            continue

        same_page = block["page_index"] == current["page_index"]
        y_delta = abs(block["y_center"] - current["mean_y"])
        limit = row_threshold(current["avg_height"], block["height"], px, ratio)

        if same_page and y_delta <= limit:
            current["items"].append(block)
            n = len(current["items"])
            current["mean_y"] = (current["mean_y"] * (n - 1) + block["y_center"]) / n
            current["avg_height"] = (current["avg_height"] * (n - 1) + block["height"]) / n
        else:
            current = {
                "page_index": block["page_index"],
                "items": [block],
                "mean_y": block["y_center"],
                "avg_height": block["height"],
            }
            rows.append(current)

    out: List[Dict[str, Any]] = []
    for i, row in enumerate(rows):
        items = sorted(row["items"], key=lambda b: (b["x_min"], b["x_center"]))
        out.append(
            {
                "row_index": i,
                "page_index": row["page_index"],
                "y_center": row["mean_y"],
                "items": items,
                "texts": [item["text"] for item in items],
            }
        )
    return out


def run_ocr(image_path: Path) -> List[Dict[str, Any]]:
    PaddleOCR = load_paddleocr_class()
    ocr = PaddleOCR(
        text_detection_model_name="PP-OCRv5_mobile_det",
        text_recognition_model_name="PP-OCRv5_mobile_rec",
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
    )

    results = ocr.predict(
        str(image_path),
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
    )

    pages: List[Dict[str, Any]] = []
    for result in results:
        page = result.json.get("res", {}) if hasattr(result, "json") else {}
        pages.append(page)
    return pages


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)


def main() -> int:
    args = parse_args()
    if args.row_y_threshold_ratio <= 0:
        raise RuntimeError("--row-y-threshold-ratio must be greater than 0.")

    image_path = (args.image if args.image is not None else find_default_image()).resolve()
    if not image_path.exists():
        raise RuntimeError(f"Input image not found: {image_path}")
    if image_path.suffix.lower() not in SUPPORTED_IMAGE_SUFFIXES:
        raise RuntimeError("Input file must be PNG/JPG/JPEG/WEBP.")

    pages = run_ocr(image_path)
    blocks = flatten_pages(pages)
    rows = group_rows(blocks, args.row_y_threshold, args.row_y_threshold_ratio)

    raw_payload = {
        "image_path": str(image_path),
        "pipeline": {
            "text_detection_model_name": "PP-OCRv5_mobile_det",
            "text_recognition_model_name": "PP-OCRv5_mobile_rec",
            "use_doc_orientation_classify": False,
            "use_doc_unwarping": False,
            "use_textline_orientation": False,
        },
        "pages": pages,
        "blocks": blocks,
    }

    rows_payload = {
        "image_path": str(image_path),
        "grouping": {
            "row_y_threshold_px": args.row_y_threshold,
            "row_y_threshold_ratio": args.row_y_threshold_ratio,
        },
        "rows": rows,
        "row_texts": [row["texts"] for row in rows],
    }

    write_json(args.raw_output.resolve(), raw_payload)
    write_json(args.rows_output.resolve(), rows_payload)

    print(f"Input image: {image_path}")
    print(f"Raw OCR JSON: {args.raw_output.resolve()}")
    print(f"Grouped rows JSON: {args.rows_output.resolve()}")
    print(f"Extracted text blocks: {len(blocks)}")
    print(f"Grouped rows: {len(rows)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1)
