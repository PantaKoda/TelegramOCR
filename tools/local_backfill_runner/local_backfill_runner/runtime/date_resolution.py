"""Schedule date extraction helpers for OCR session images."""

from __future__ import annotations

import re
import unicodedata
from datetime import date
from statistics import median
from typing import Any

DATE_WITH_WEEKDAY_RE = re.compile(r"\b([A-Za-zÅÄÖåäö]+)\s+(\d{1,2})\s+([A-Za-zÅÄÖåäö]+)(?:\s+(\d{4}))?\b")
DATE_DAY_MONTH_RE = re.compile(r"\b(\d{1,2})\s+([A-Za-zÅÄÖåäö]+)(?:\s+(\d{4}))?\b")

WEEKDAY_NAMES = {
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
    "mandag",
    "tisdag",
    "onsdag",
    "torsdag",
    "fredag",
    "lordag",
    "sondag",
}

MONTH_MAP = {
    "jan": 1,
    "january": 1,
    "januari": 1,
    "feb": 2,
    "february": 2,
    "februari": 2,
    "mar": 3,
    "march": 3,
    "mars": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "maj": 5,
    "jun": 6,
    "june": 6,
    "juni": 6,
    "jul": 7,
    "july": 7,
    "juli": 7,
    "aug": 8,
    "august": 8,
    "augusti": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "okt": 10,
    "oktober": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


def extract_schedule_date_from_boxes(boxes: list[Any], *, default_year: int | None) -> date:
    options: list[dict[str, Any]] = []
    for candidate in _extract_date_candidate_texts(boxes):
        for parsed in _parse_schedule_date_candidates_from_text(candidate["text"], default_year=default_year):
            options.append(
                {
                    "date": parsed["date"],
                    "has_weekday": parsed["has_weekday"],
                    "has_explicit_year": parsed["has_explicit_year"],
                    "source_priority": candidate["source_priority"],
                    "text_length": len(candidate["text"]),
                    "h": candidate["h"],
                    "y": candidate["y"],
                }
            )
    if options:
        best = max(
            options,
            key=lambda item: (
                1 if item["has_weekday"] else 0,
                1 if item["has_explicit_year"] else 0,
                item["source_priority"],
                float(item["h"]),
                item["text_length"],
                -float(item["y"]),
            ),
        )
        return best["date"]
    raise RuntimeError("Could not resolve schedule date from OCR UI text.")


def resolve_session_schedule_dates(values: list[date | None]) -> tuple[date, list[date], int]:
    if not values:
        raise RuntimeError("No session images available for schedule date resolution.")

    explicit_dates = [value for value in values if value is not None]
    if not explicit_dates:
        raise RuntimeError("No schedule date detected from OCR output.")

    anchor_date = _ensure_single_schedule_date(explicit_dates)
    resolved = [value if value is not None else anchor_date for value in values]
    inherited_count = sum(1 for value in values if value is None)
    return anchor_date, resolved, inherited_count


def _extract_date_candidate_texts(boxes: list[Any]) -> list[dict[str, Any]]:
    normalized_boxes: list[dict[str, Any]] = []
    for box in boxes:
        text = str(getattr(box, "text", ""))
        cleaned = " ".join(text.split())
        if not cleaned:
            continue
        try:
            x = float(getattr(box, "x", 0.0))
            y = float(getattr(box, "y", 0.0))
            h = float(getattr(box, "h", 0.0))
        except (TypeError, ValueError):
            x = 0.0
            y = 0.0
            h = 0.0
        normalized_boxes.append({"text": cleaned, "x": x, "y": y, "h": max(h, 1.0)})

    if not normalized_boxes:
        return []

    normalized_boxes.sort(key=lambda item: (item["y"], item["x"]))
    min_y = min(item["y"] for item in normalized_boxes)
    max_y = max(item["y"] + item["h"] for item in normalized_boxes)
    vertical_span = max(1.0, max_y - min_y)
    top_band_limit = min_y + max(400.0, vertical_span * 0.45)

    line_threshold = max(8.0, median(item["h"] for item in normalized_boxes) * 0.6)
    current_line: list[dict[str, Any]] = []
    current_center = 0.0
    line_candidates: list[dict[str, Any]] = []
    for item in normalized_boxes:
        center = item["y"] + (item["h"] / 2.0)
        if not current_line:
            current_line = [item]
            current_center = center
            continue
        if abs(center - current_center) <= line_threshold:
            current_line.append(item)
            current_center = (current_center * (len(current_line) - 1) + center) / len(current_line)
            continue
        line_text = " ".join(part["text"] for part in sorted(current_line, key=lambda value: value["x"]))
        line_y = min(part["y"] for part in current_line)
        line_h = median(part["h"] for part in current_line)
        if line_text and line_y <= top_band_limit:
            line_candidates.append({"text": line_text, "y": line_y, "h": line_h, "source_priority": 1})
        current_line = [item]
        current_center = center
    if current_line:
        line_text = " ".join(part["text"] for part in sorted(current_line, key=lambda value: value["x"]))
        line_y = min(part["y"] for part in current_line)
        line_h = median(part["h"] for part in current_line)
        if line_text and line_y <= top_band_limit:
            line_candidates.append({"text": line_text, "y": line_y, "h": line_h, "source_priority": 1})

    box_candidates = [
        {"text": item["text"], "y": item["y"], "h": item["h"], "source_priority": 0}
        for item in normalized_boxes
        if item["y"] <= top_band_limit
    ]
    return [*line_candidates, *box_candidates]


def _parse_schedule_date_candidates_from_text(text: str, *, default_year: int | None) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for match in DATE_WITH_WEEKDAY_RE.finditer(text):
        weekday_token = _normalize_date_token(match.group(1))
        if weekday_token not in WEEKDAY_NAMES:
            continue
        resolved = _build_date_from_parts(match.group(2), match.group(3), match.group(4), default_year=default_year)
        if resolved is not None:
            candidates.append(
                {
                    "date": resolved,
                    "has_weekday": True,
                    "has_explicit_year": bool(match.group(4)),
                }
            )

    for match in DATE_DAY_MONTH_RE.finditer(text):
        resolved = _build_date_from_parts(match.group(1), match.group(2), match.group(3), default_year=default_year)
        if resolved is not None:
            candidates.append(
                {
                    "date": resolved,
                    "has_weekday": False,
                    "has_explicit_year": bool(match.group(3)),
                }
            )
    return candidates


def _build_date_from_parts(day_value: str, month_value: str, year_value: str | None, *, default_year: int | None) -> date | None:
    month_key = _normalize_date_token(month_value)
    month = MONTH_MAP.get(month_key)
    if month is None:
        return None
    try:
        day = int(day_value)
    except ValueError:
        return None
    try:
        year = default_year if year_value is None else int(year_value)
    except ValueError:
        return None
    if year is None:
        raise RuntimeError("Date text is missing year and OCR_DEFAULT_YEAR is not configured.")
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _normalize_date_token(value: str) -> str:
    collapsed = " ".join(value.split())
    normalized = unicodedata.normalize("NFKD", collapsed)
    without_marks = "".join(char for char in normalized if not unicodedata.combining(char))
    return without_marks.lower()


def _ensure_single_schedule_date(values: list[date]) -> date:
    unique = sorted(set(values))
    if not unique:
        raise RuntimeError("No schedule date detected from OCR output.")
    if len(unique) > 1:
        rendered = ", ".join(value.isoformat() for value in unique)
        raise RuntimeError(f"Inconsistent schedule dates detected across session images: {rendered}")
    return unique[0]
