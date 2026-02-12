"""Deterministic layout parser for OCR-like text boxes."""

from __future__ import annotations

import re
from dataclasses import dataclass
from statistics import median
from typing import Any

TIME_RANGE_RE = re.compile(r"\b(\d{1,2})[:.](\d{2})(?:\s*-\s*(\d{1,2})[:.](\d{2}))?\b")


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


def _time_or_none(text: str) -> tuple[str, str] | None:
    match = TIME_RANGE_RE.search(text)
    if match is None:
        return None

    start = _normalize_time(int(match.group(1)), int(match.group(2)))
    if start is None:
        return None

    if match.group(3) is None or match.group(4) is None:
        return (start, start)

    end = _normalize_time(int(match.group(3)), int(match.group(4)))
    if end is None:
        return None
    return (start, end)


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
                y=min(box.y for box in line_boxes_sorted),
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

    time_indices: list[tuple[int, tuple[str, str]]] = []
    for index, line in enumerate(lines):
        parsed_time = _time_or_none(line.text)
        if parsed_time is not None:
            time_indices.append((index, parsed_time))

    # Card without a time line is ignored (e.g., top UI chrome/header).
    if not time_indices:
        return []

    results: list[tuple[Entry, float, float]] = []
    for position, (time_index, parsed_time) in enumerate(time_indices):
        previous_time = time_indices[position - 1][0] if position > 0 else -1
        next_time = time_indices[position + 1][0] if position + 1 < len(time_indices) else len(lines)

        before_indices = [
            index
            for index in range(previous_time + 1, time_index)
            if _time_or_none(lines[index].text) is None and _clean_text(lines[index].text)
        ]
        after_indices = [
            index
            for index in range(time_index + 1, next_time)
            if _time_or_none(lines[index].text) is None and _clean_text(lines[index].text)
        ]

        title_parts: list[str] = []
        trailing_indices: list[int] = []
        if before_indices and (position == 0 or not after_indices):
            # Handles title lines that appear above the first time line.
            title_parts = [_clean_text(lines[index].text) for index in before_indices]
            trailing_indices = after_indices
        elif after_indices:
            title_parts = [_clean_text(lines[after_indices[0]].text)]
            trailing_indices = after_indices[1:]
        elif before_indices:
            title_parts = [_clean_text(lines[before_indices[-1]].text)]
            trailing_indices = []

        title = _clean_text(" ".join(title_parts))
        if not title:
            continue

        trailing_lines = [_clean_text(lines[index].text) for index in trailing_indices]
        if not trailing_lines:
            address = ""
            location = ""
        elif len(trailing_lines) == 1:
            address = ""
            location = trailing_lines[0]
        else:
            address = " ".join(trailing_lines[:-1])
            location = trailing_lines[-1]

        entry = Entry(
            start=parsed_time[0],
            end=parsed_time[1],
            title=title,
            location=location,
            address=address,
        )
        anchor = lines[time_index]
        results.append((entry, anchor.y, anchor.x))

    return results
