"""Deterministic layout parser for OCR-like text boxes."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from statistics import median
from typing import Any

TIME_RANGE_RE = re.compile(r"\b(\d{1,2})[:.](\d{2})(?:\s*-\s*(\d{1,2})[:.](\d{2}))?\b")
LEADING_SINGLE_TIME_RE = re.compile(r"^\s*(\d{1,2})[:.](\d{2})(?:\s+(.*\S))?\s*$")
DURATION_RE = re.compile(r"^\s*\d+\s*h(?:\s*\d+\s*m)?\s*$|^\s*\d+\s*m(?:in)?\s*$", re.IGNORECASE)
NOISE_PREFIX_RE = re.compile(r"^(?:on\s*time|collaborators?(?:\s*\+?\d+)?)\b[:\-]?\s*", re.IGNORECASE)


@dataclass(frozen=True)
class Box:
    text: str
    x: float
    y: float
    w: float
    h: float


@dataclass(frozen=True)
class Entry:
    start: str
    end: str
    title: str
    location: str
    address: str


@dataclass(frozen=True)
class _Line:
    text: str
    x: float
    y: float
    h: float


@dataclass(frozen=True)
class _ParsedTime:
    start: str
    end: str
    is_range: bool


def parse_layout(boxes: list[Box]) -> list[Entry]:
    """Parse OCR-like text boxes into normalized schedule entries."""
    parsed_boxes = [_coerce_box(box) for box in boxes]
    parsed_boxes = [box for box in parsed_boxes if _clean_text(box.text)]
    if not parsed_boxes:
        return []

    columns = _split_columns(parsed_boxes)
    parsed_entries: list[tuple[float, float, Entry]] = []

    for column in columns:
        lines = _cluster_lines(column)
        cards = _group_cards(lines)
        for card_lines in cards:
            for entry, anchor_y, anchor_x in _parse_card_entries(card_lines):
                parsed_entries.append((anchor_y, anchor_x, entry))

    parsed_entries.sort(key=lambda item: (item[0], item[1]))
    return [item[2] for item in parsed_entries]


def _coerce_box(value: Any) -> Box:
    if isinstance(value, Box):
        return value
    if isinstance(value, dict):
        return Box(
            text=str(value.get("text", "")),
            x=float(value.get("x", 0)),
            y=float(value.get("y", 0)),
            w=float(value.get("w", 0)),
            h=float(value.get("h", 0)),
        )
    raise TypeError(f"Unsupported box value: {type(value)!r}")


def _clean_text(value: str) -> str:
    return " ".join(value.split())


def _time_or_none(text: str) -> _ParsedTime | None:
    match = TIME_RANGE_RE.search(text)
    if match is None:
        return None

    start = _normalize_time(int(match.group(1)), int(match.group(2)))
    if start is None:
        return None

    if match.group(3) is None or match.group(4) is None:
        return _ParsedTime(start=start, end=start, is_range=False)

    end = _normalize_time(int(match.group(3)), int(match.group(4)))
    if end is None:
        return None
    return _ParsedTime(start=start, end=end, is_range=True)


def _normalize_time(hour: int, minute: int) -> str | None:
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return f"{hour:02d}:{minute:02d}"


def _split_columns(boxes: list[Box]) -> list[list[Box]]:
    if len(boxes) < 4:
        return [sorted(boxes, key=lambda box: (box.y, box.x))]

    centers = sorted(box.x + (box.w / 2.0) for box in boxes)
    widths = [max(box.w, 1.0) for box in boxes]
    median_width = median(widths)

    largest_gap = -1.0
    split_index = -1
    for index in range(len(centers) - 1):
        gap = centers[index + 1] - centers[index]
        if gap > largest_gap:
            largest_gap = gap
            split_index = index

    threshold = max(120.0, median_width * 1.8)
    if split_index < 0 or largest_gap <= threshold:
        return [sorted(boxes, key=lambda box: (box.y, box.x))]

    boundary = (centers[split_index] + centers[split_index + 1]) / 2.0
    left = [box for box in boxes if (box.x + (box.w / 2.0)) <= boundary]
    right = [box for box in boxes if (box.x + (box.w / 2.0)) > boundary]
    if not left or not right or min(len(left), len(right)) < 2:
        return [sorted(boxes, key=lambda box: (box.y, box.x))]

    columns = [
        sorted(left, key=lambda box: (box.y, box.x)),
        sorted(right, key=lambda box: (box.y, box.x)),
    ]
    columns.sort(key=lambda column: min(box.x for box in column))
    return columns


def _cluster_lines(boxes: list[Box]) -> list[_Line]:
    if not boxes:
        return []

    boxes_sorted = sorted(boxes, key=lambda box: (box.y, box.x))
    median_height = median(max(box.h, 1.0) for box in boxes_sorted)
    threshold = max(8.0, median_height * 0.6)

    lines: list[list[Box]] = []
    current_line: list[Box] = []
    current_center = 0.0

    for box in boxes_sorted:
        center = box.y + (box.h / 2.0)
        if not current_line:
            current_line = [box]
            current_center = center
            continue

        if abs(center - current_center) <= threshold:
            current_line.append(box)
            current_center = (current_center * (len(current_line) - 1) + center) / len(current_line)
        else:
            lines.append(current_line)
            current_line = [box]
            current_center = center

    if current_line:
        lines.append(current_line)

    merged: list[_Line] = []
    for line_boxes in lines:
        line_boxes_sorted = sorted(line_boxes, key=lambda box: box.x)
        text = _clean_text(" ".join(_clean_text(box.text) for box in line_boxes_sorted))
        if not text:
            continue
        merged.append(
            _Line(
                text=text,
                x=min(box.x for box in line_boxes_sorted),
                y=median(box.y for box in line_boxes_sorted),
                h=median(max(box.h, 1.0) for box in line_boxes_sorted),
            )
        )

    merged.sort(key=lambda line: (line.y, line.x))
    return merged


def _group_cards(lines: list[_Line]) -> list[list[_Line]]:
    if not lines:
        return []

    median_height = median(max(line.h, 1.0) for line in lines)
    gap_threshold = max(24.0, median_height * 1.8)

    cards: list[list[_Line]] = []
    current_card: list[_Line] = []
    previous_line: _Line | None = None

    for line in lines:
        if not current_card:
            current_card = [line]
            previous_line = line
            continue

        gap = line.y - (previous_line.y if previous_line is not None else line.y)
        if gap > gap_threshold:
            cards.append(current_card)
            current_card = [line]
        else:
            current_card.append(line)
        previous_line = line

    if current_card:
        cards.append(current_card)

    return cards


def _parse_card_entries(lines: list[_Line]) -> list[tuple[Entry, float, float]]:
    if not lines:
        return []

    time_indices: list[tuple[int, _ParsedTime]] = []
    for index, line in enumerate(lines):
        parsed_time = _time_or_none(line.text)
        if parsed_time is not None:
            time_indices.append((index, parsed_time))

    # Card without a time line is ignored (e.g., top UI chrome/header).
    if not time_indices:
        return []

    markers = _consolidate_time_markers(time_indices, lines)
    occupied_time_indexes: set[int] = set()
    for marker in markers:
        occupied_time_indexes.update(range(marker["start_index"], marker["end_index"] + 1))

    results: list[tuple[Entry, float, float]] = []
    for position, marker in enumerate(markers):
        previous_end = markers[position - 1]["end_index"] if position > 0 else -1
        next_start = markers[position + 1]["start_index"] if position + 1 < len(markers) else len(lines)

        before_indices = [
            index
            for index in range(previous_end + 1, marker["start_index"])
            if index not in occupied_time_indexes and _clean_text(lines[index].text) and not _is_noise_line(lines[index].text)
        ]
        after_indices = [
            index
            for index in range(marker["end_index"] + 1, next_start)
            if index not in occupied_time_indexes and _clean_text(lines[index].text) and not _is_noise_line(lines[index].text)
        ]

        title = ""
        trailing_indices: list[int] = []
        prefixed_title = _strip_noise_prefix(str(marker.get("prefill_title", "")))
        if prefixed_title and not _is_noise_line(prefixed_title):
            title = prefixed_title
            trailing_indices = after_indices
        else:
            title_parts: list[str] = []
            if before_indices and (position == 0 or not after_indices):
                # Handles title lines that appear above the first time line.
                title_parts = [_strip_noise_prefix(lines[index].text) for index in before_indices]
                trailing_indices = after_indices
            elif after_indices:
                title_parts = [_strip_noise_prefix(lines[after_indices[0]].text)]
                trailing_indices = after_indices[1:]
            elif before_indices:
                title_parts = [_strip_noise_prefix(lines[before_indices[-1]].text)]
                trailing_indices = []

            title = _clean_text(" ".join(title_parts))
        if not title:
            continue

        trailing_line_objects = [lines[index] for index in trailing_indices]
        trailing_line_objects = _prune_far_right_metadata_lines(trailing_line_objects)
        trailing_lines = [_strip_noise_prefix(line.text) for line in trailing_line_objects]
        trailing_lines = [line for line in trailing_lines if line and not _is_noise_line(line)]
        if not trailing_lines:
            address = ""
            location = ""
        elif len(trailing_lines) == 1:
            if _looks_like_address(trailing_lines[0]):
                address = trailing_lines[0]
                location = ""
            else:
                address = ""
                location = trailing_lines[0]
        else:
            address = " ".join(trailing_lines[:-1])
            location = trailing_lines[-1]

        entry = Entry(
            start=marker["time"].start,
            end=marker["time"].end,
            title=title,
            location=location,
            address=address,
        )
        if _should_drop_single_time_entry(entry):
            continue
        anchor = lines[marker["anchor_index"]]
        results.append((entry, anchor.y, anchor.x))

    return results


def _consolidate_time_markers(markers: list[tuple[int, _ParsedTime]], lines: list[_Line]) -> list[dict[str, Any]]:
    combined: list[dict[str, Any]] = []
    if not markers:
        return combined

    median_height = median(max(line.h, 1.0) for line in lines) if lines else 20.0
    max_time_column_delta = max(16.0, median_height * 1.1)
    max_vertical_gap = max(52.0, median_height * 4.2)
    max_intermediate_lines = 4

    index = 0
    while index < len(markers):
        current_index, current_time = markers[index]
        current_leading = _leading_single_time(lines[current_index].text)
        current_prefill = current_leading[1] if current_leading is not None else ""
        if not current_time.is_range and index + 1 < len(markers):
            next_index, next_time = markers[index + 1]
            next_leading = _leading_single_time(lines[next_index].text)
            if _can_merge_stacked_single_times(
                current_index=current_index,
                next_index=next_index,
                current_time=current_time,
                next_time=next_time,
                current_leading=current_leading,
                next_leading=next_leading,
                lines=lines,
                max_time_column_delta=max_time_column_delta,
                max_vertical_gap=max_vertical_gap,
                max_intermediate_lines=max_intermediate_lines,
            ):
                next_prefill = next_leading[1]
                between_prefill = _prefill_from_between_lines(
                    lines=lines,
                    start_index=current_index + 1,
                    end_index=next_index,
                    time_column_x=lines[current_index].x,
                    max_time_column_delta=max_time_column_delta,
                )
                combined.append(
                    {
                        "start_index": current_index,
                        "end_index": next_index,
                        "anchor_index": current_index,
                        "time": _ParsedTime(start=current_time.start, end=next_time.start, is_range=True),
                        "prefill_title": _choose_prefill_title(current_prefill, next_prefill, between_prefill),
                    }
                )
                index += 2
                continue
        combined.append(
            {
                "start_index": current_index,
                "end_index": current_index,
                "anchor_index": current_index,
                "time": current_time,
                "prefill_title": current_prefill,
            }
        )
        index += 1
    return combined


def _can_merge_stacked_single_times(
    *,
    current_index: int,
    next_index: int,
    current_time: _ParsedTime,
    next_time: _ParsedTime,
    current_leading: tuple[str, str] | None,
    next_leading: tuple[str, str] | None,
    lines: list[_Line],
    max_time_column_delta: float,
    max_vertical_gap: float,
    max_intermediate_lines: int,
) -> bool:
    if next_time.is_range:
        return False
    if current_leading is None or next_leading is None:
        return False
    if current_leading[0] != current_time.start or next_leading[0] != next_time.start:
        return False

    if next_index <= current_index:
        return False
    if (next_index - current_index - 1) > max_intermediate_lines:
        return False

    current_line = lines[current_index]
    next_line = lines[next_index]
    if abs(next_line.x - current_line.x) > max_time_column_delta:
        return False

    vertical_gap = next_line.y - current_line.y
    if vertical_gap <= 0 or vertical_gap > max_vertical_gap:
        return False

    return _between_lines_are_nonblocking(
        lines=lines,
        start_index=current_index + 1,
        end_index=next_index,
        time_column_x=current_line.x,
        max_time_column_delta=max_time_column_delta,
    )


def _between_lines_are_nonblocking(
    *,
    lines: list[_Line],
    start_index: int,
    end_index: int,
    time_column_x: float,
    max_time_column_delta: float,
) -> bool:
    # Between start/end times we allow status/noise lines and right-side content,
    # but we block if another left-column semantic line appears.
    blocking_x_threshold = max_time_column_delta * 2.5
    for index in range(start_index, end_index):
        line = lines[index]
        text = _clean_text(line.text)
        if not text:
            continue
        if _is_noise_line(text):
            continue
        if abs(line.x - time_column_x) > blocking_x_threshold:
            continue
        return False
    return True


def _prefill_from_between_lines(
    *,
    lines: list[_Line],
    start_index: int,
    end_index: int,
    time_column_x: float,
    max_time_column_delta: float,
) -> str:
    blocking_x_threshold = max_time_column_delta * 2.5
    candidates: list[str] = []
    for index in range(start_index, end_index):
        line = lines[index]
        if abs(line.x - time_column_x) <= blocking_x_threshold:
            continue
        cleaned = _strip_noise_prefix(line.text)
        if not cleaned or _is_noise_line(cleaned):
            continue
        candidates.append(cleaned)
    if not candidates:
        return ""
    return _clean_text(" ".join(candidates))


def _prune_far_right_metadata_lines(lines: list[_Line]) -> list[_Line]:
    if len(lines) < 2:
        return lines
    base_x = min(line.x for line in lines)
    threshold = max(140.0, median(max(line.h, 1.0) for line in lines) * 7.0)
    kept: list[_Line] = []
    for line in lines:
        if (line.x - base_x) > threshold and not _looks_like_address(line.text):
            continue
        kept.append(line)
    return kept if kept else lines


def _choose_prefill_title(*candidates: str) -> str:
    for candidate in candidates:
        cleaned = _strip_noise_prefix(candidate)
        if not cleaned:
            continue
        if _is_noise_line(cleaned):
            continue
        return cleaned
    for candidate in candidates:
        cleaned = _strip_noise_prefix(candidate)
        if cleaned:
            return cleaned
    return ""


def _leading_single_time(value: str) -> tuple[str, str] | None:
    match = LEADING_SINGLE_TIME_RE.fullmatch(value)
    if match is None:
        return None
    parsed = _normalize_time(int(match.group(1)), int(match.group(2)))
    if parsed is None:
        return None
    remainder = _clean_text(match.group(3) or "")
    return parsed, remainder


def _normalize_for_match(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    without_marks = "".join(char for char in normalized if not unicodedata.combining(char))
    return " ".join(without_marks.lower().split())


def _is_noise_line(value: str) -> bool:
    normalized = _normalize_for_match(_strip_noise_prefix(value))
    if not normalized:
        return True
    if len(normalized) <= 1:
        return True
    if "collaborator" in normalized:
        return True
    if re.search(r"\+\s*\d+\b", normalized):
        return True
    if normalized in {"on time", "ontime", "thank you for today", "thank you for today!"}:
        return True
    if DURATION_RE.fullmatch(normalized):
        return True
    if re.fullmatch(r"\+?\d+", normalized):
        return True
    return False


def _should_drop_single_time_entry(entry: Entry) -> bool:
    # Single-point cards are typically UI chrome/footer artifacts, not shifts.
    if entry.start != entry.end:
        return False
    if _clean_text(entry.location) or _clean_text(entry.address):
        return False
    return True


def _looks_like_address(value: str) -> bool:
    normalized = _normalize_for_match(value)
    if any(char.isdigit() for char in normalized):
        return True
    if "," in value:
        return True
    return bool(re.search(r"\b(vagen|vag|gatan|street|road|avenyn|alle|plats|gr[aä]nd)\b", normalized))


def _strip_noise_prefix(value: str) -> str:
    cleaned = _clean_text(value)
    if not cleaned:
        return ""
    previous = None
    current = cleaned
    while previous != current:
        previous = current
        current = NOISE_PREFIX_RE.sub("", current).strip()
    return _clean_text(current)
