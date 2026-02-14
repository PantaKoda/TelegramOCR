"""Deterministic aggregation of multi-image session observations."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
import re

from parser.entity_identity import customer_fingerprint, location_fingerprint
from parser.semantic_normalizer import CanonicalShift, SHIFT_TYPE_PRIORITY

NOISY_LOCATION_TOKENS = {
    "schedule",
    "helphub",
    "account",
    "collaborators",
    "profile",
}


@dataclass(frozen=True)
class AggregatedShift:
    shift: CanonicalShift
    source_count: int
    notes: tuple[str, ...]


@dataclass(frozen=True)
class AggregatedDaySchedule:
    schedule_date: str
    shifts: list[AggregatedShift]


@dataclass
class _ShiftRef:
    image_index: int
    shift_index: int
    shift: CanonicalShift


@dataclass
class _Cluster:
    shift: CanonicalShift
    source_count: int = 1
    notes: set[str] = field(default_factory=set)


def aggregate_session_shifts(
    session_images: list[list[CanonicalShift]],
    *,
    schedule_date: str,
    time_tolerance_minutes: int = 5,
) -> AggregatedDaySchedule:
    _validate_schedule_date(schedule_date)
    if time_tolerance_minutes < 0:
        raise ValueError("time_tolerance_minutes must be >= 0")

    refs: list[_ShiftRef] = []
    for image_index, image_shifts in enumerate(session_images):
        for shift_index, shift in enumerate(image_shifts):
            refs.append(_ShiftRef(image_index=image_index, shift_index=shift_index, shift=shift))

    refs.sort(
        key=lambda ref: (
            ref.shift.location_fingerprint,
            _minutes(ref.shift.start),
            _minutes(ref.shift.end),
            ref.shift.customer_fingerprint,
            ref.image_index,
            ref.shift_index,
        )
    )

    grouped: dict[str, list[_ShiftRef]] = defaultdict(list)
    for ref in refs:
        grouped[ref.shift.location_fingerprint].append(ref)

    merged: list[_Cluster] = []
    for location_key in sorted(grouped.keys()):
        clusters = _merge_location_group(grouped[location_key], time_tolerance_minutes=time_tolerance_minutes)
        merged.extend(clusters)

    aggregated = [
        AggregatedShift(
            shift=cluster.shift,
            source_count=cluster.source_count,
            notes=tuple(sorted(cluster.notes)),
        )
        for cluster in merged
    ]
    aggregated = _dedupe_exact_identity_time(aggregated)
    aggregated.sort(
        key=lambda item: (
            _minutes(item.shift.start),
            _minutes(item.shift.end),
            item.shift.location_fingerprint,
            item.shift.customer_fingerprint,
            item.shift.customer_name.casefold(),
        )
    )
    return AggregatedDaySchedule(schedule_date=schedule_date, shifts=aggregated)


def _merge_location_group(refs: list[_ShiftRef], *, time_tolerance_minutes: int) -> list[_Cluster]:
    refs_sorted = sorted(
        refs,
        key=lambda ref: (
            _minutes(ref.shift.start),
            _minutes(ref.shift.end),
            ref.shift.customer_fingerprint,
            ref.image_index,
            ref.shift_index,
        ),
    )
    clusters: list[_Cluster] = []
    for ref in refs_sorted:
        candidate_index = _best_cluster_for_shift(
            clusters,
            ref.shift,
            tolerance=time_tolerance_minutes,
        )
        if candidate_index is None:
            cluster = _Cluster(shift=ref.shift, notes=set(_extract_notes(ref.shift)))
            clusters.append(cluster)
            continue

        cluster = clusters[candidate_index]
        cluster.shift = _merge_shift(cluster.shift, ref.shift)
        cluster.source_count += 1
        cluster.notes.update(_extract_notes(ref.shift))

    return clusters


def _best_cluster_for_shift(clusters: list[_Cluster], shift: CanonicalShift, *, tolerance: int) -> int | None:
    best_index: int | None = None
    best_distance: int | None = None
    best_key: tuple | None = None

    for index, cluster in enumerate(clusters):
        distance = _time_distance_minutes(cluster.shift, shift)
        contains = (
            cluster.shift.customer_fingerprint == shift.customer_fingerprint
            and (_range_contains(cluster.shift, shift) or _range_contains(shift, cluster.shift))
        )
        if distance > tolerance and not contains:
            continue
        key = _cluster_match_priority_key(cluster.shift, shift, distance=distance, tolerance=tolerance)
        if (
            best_distance is None
            or distance < best_distance
            or (distance == best_distance and key < best_key)
        ):
            best_index = index
            best_distance = distance
            best_key = key

    return best_index


def _merge_shift(base: CanonicalShift, incoming: CanonicalShift) -> CanonicalShift:
    anchor = _minutes(base.start)
    base_start, base_end = _unwrap_interval(base, anchor_minutes=anchor)
    incoming_start, incoming_end = _unwrap_interval(incoming, anchor_minutes=anchor)
    start_minutes = min(base_start, incoming_start)
    end_minutes = max(base_end, incoming_end)

    selected_customer_name = _select_better_customer_name(base.customer_name, incoming.customer_name)

    base_address_quality = _address_quality_score(base)
    incoming_address_quality = _address_quality_score(incoming)
    if incoming_address_quality > base_address_quality:
        selected_street = incoming.street
        selected_street_number = incoming.street_number
        selected_postal_code = incoming.postal_code
        selected_postal_area = incoming.postal_area
        selected_city = incoming.city
    elif incoming_address_quality < base_address_quality:
        selected_street = base.street
        selected_street_number = base.street_number
        selected_postal_code = base.postal_code
        selected_postal_area = base.postal_area
        selected_city = base.city
    else:
        base_address_len = _address_length(base)
        incoming_address_len = _address_length(incoming)
        if incoming_address_len > base_address_len:
            selected_street = incoming.street
            selected_street_number = incoming.street_number
            selected_postal_code = incoming.postal_code
            selected_postal_area = incoming.postal_area
            selected_city = incoming.city
        else:
            selected_street = base.street
            selected_street_number = base.street_number
            selected_postal_code = base.postal_code
            selected_postal_area = base.postal_area
            selected_city = base.city

    selected_shift_type = _select_shift_type(base.shift_type, incoming.shift_type)
    selected_raw_type_label = _select_better_raw_type_label(base.raw_type_label, incoming.raw_type_label)
    identity_anchor = selected_customer_name.strip() or selected_raw_type_label.strip() or selected_shift_type
    selected_customer_fingerprint = customer_fingerprint(identity_anchor)

    selected_location_fingerprint = location_fingerprint(
        street=selected_street,
        street_number=selected_street_number,
        postal_area=selected_postal_area,
        city=selected_city,
    )

    return CanonicalShift(
        start=_from_minutes_mod(start_minutes),
        end=_from_minutes_mod(end_minutes),
        customer_name=selected_customer_name,
        customer_fingerprint=selected_customer_fingerprint,
        street=selected_street,
        street_number=selected_street_number,
        postal_code=selected_postal_code,
        postal_area=selected_postal_area,
        city=selected_city,
        location_fingerprint=selected_location_fingerprint,
        shift_type=selected_shift_type,
        raw_type_label=selected_raw_type_label,
    )


def _dedupe_exact_identity_time(values: list[AggregatedShift]) -> list[AggregatedShift]:
    grouped: dict[tuple[str, str, str, str, str], list[AggregatedShift]] = defaultdict(list)
    for item in values:
        key = (
            item.shift.start,
            item.shift.end,
            item.shift.customer_fingerprint,
            item.shift.shift_type,
            item.shift.raw_type_label.casefold(),
        )
        grouped[key].append(item)

    deduped: list[AggregatedShift] = []
    for key in sorted(grouped.keys()):
        items = grouped[key]
        if len(items) == 1:
            deduped.append(items[0])
            continue

        merged_shift = items[0].shift
        merged_notes = set(items[0].notes)
        merged_source_count = items[0].source_count
        for item in items[1:]:
            merged_shift = _merge_shift(merged_shift, item.shift)
            merged_source_count += item.source_count
            merged_notes.update(item.notes)

        deduped.append(
            AggregatedShift(
                shift=merged_shift,
                source_count=merged_source_count,
                notes=tuple(sorted(merged_notes)),
            )
        )
    return deduped


def _select_better_customer_name(left: str, right: str) -> str:
    left_key = (len(left.strip()), left.casefold())
    right_key = (len(right.strip()), right.casefold())
    return right if right_key > left_key else left


def _select_shift_type(left: str, right: str) -> str:
    if left == right:
        return left
    if left == "UNKNOWN":
        return right
    if right == "UNKNOWN":
        return left
    left_priority = SHIFT_TYPE_PRIORITY.get(left, 0)
    right_priority = SHIFT_TYPE_PRIORITY.get(right, 0)
    if left_priority == right_priority:
        return min(left, right)
    return left if left_priority > right_priority else right


def _select_better_raw_type_label(left: str, right: str) -> str:
    left_key = (len(left.strip()), left.casefold())
    right_key = (len(right.strip()), right.casefold())
    return right if right_key > left_key else left


def _address_length(shift: CanonicalShift) -> int:
    value = " ".join(
        token for token in [shift.street, shift.street_number, shift.postal_code, shift.postal_area, shift.city] if token
    )
    return len(value)


def _address_quality_score(shift: CanonicalShift) -> int:
    score = 0
    if shift.street.strip():
        score += 40 + min(len(shift.street.strip()), 40)
    if shift.street_number.strip():
        score += 12
    if shift.postal_code.strip():
        score += 10
    if shift.postal_area.strip():
        score += 8
    if shift.city.strip():
        score += 12 + min(len(shift.city.strip()), 20)

    text = " ".join(
        token
        for token in [shift.street, shift.street_number, shift.postal_code, shift.postal_area, shift.city]
        if token
    ).casefold()
    text = re.sub(r"\s+", " ", text).strip()

    for token in NOISY_LOCATION_TOKENS:
        if re.search(rf"\b{re.escape(token)}\b", text):
            score -= 80
    if "?" in text or "+" in text:
        score -= 15
    return score


def _extract_notes(shift: CanonicalShift) -> tuple[str, ...]:
    raw = getattr(shift, "notes", None)
    if raw is None:
        return ()
    if isinstance(raw, str):
        note = raw.strip()
        return (note,) if note else ()
    if isinstance(raw, (list, tuple, set)):
        notes: list[str] = []
        for item in raw:
            text = str(item).strip()
            if text:
                notes.append(text)
        return tuple(notes)
    text = str(raw).strip()
    return (text,) if text else ()


def _time_distance_minutes(left: CanonicalShift, right: CanonicalShift) -> int:
    return _clock_distance(_minutes(left.start), _minutes(right.start)) + _clock_distance(
        _minutes(left.end), _minutes(right.end)
    )


def _range_contains(container: CanonicalShift, candidate: CanonicalShift) -> bool:
    container_start = _minutes(container.start)
    candidate_start = _minutes(candidate.start)
    container_duration = _duration_minutes(container)
    candidate_duration = _duration_minutes(candidate)

    if container_duration < candidate_duration:
        return False

    start_distance = _clockwise_distance(container_start, candidate_start)
    if start_distance > container_duration:
        return False

    if candidate_duration == 0:
        return True

    candidate_end = _minutes(candidate.end)
    end_distance = _clockwise_distance(container_start, candidate_end)
    return end_distance <= container_duration


def _duration_minutes(shift: CanonicalShift) -> int:
    start = _minutes(shift.start)
    end = _minutes(shift.end)
    return (end - start) % 1440


def _cluster_match_priority_key(
    cluster_shift: CanonicalShift,
    incoming_shift: CanonicalShift,
    *,
    distance: int,
    tolerance: int,
) -> tuple:
    by_distance = 0 if distance <= tolerance else 1
    return (
        by_distance,
        distance,
        _minutes(cluster_shift.start),
        _minutes(cluster_shift.end),
        cluster_shift.customer_fingerprint,
        incoming_shift.customer_fingerprint,
    )


def _unwrap_interval(shift: CanonicalShift, *, anchor_minutes: int) -> tuple[int, int]:
    start = _unwrap_minutes_near(_minutes(shift.start), anchor_minutes=anchor_minutes)
    duration = _duration_minutes(shift)
    return start, start + duration


def _unwrap_minutes_near(value: int, *, anchor_minutes: int) -> int:
    candidates = (value - 1440, value, value + 1440)
    return min(candidates, key=lambda candidate: (abs(candidate - anchor_minutes), candidate))


def _clock_distance(left: int, right: int) -> int:
    diff = abs(left - right)
    return min(diff, 1440 - diff)


def _clockwise_distance(start: int, point: int) -> int:
    return (point - start) % 1440


def _minutes(value: str) -> int:
    hour_text, minute_text = value.split(":", 1)
    return int(hour_text) * 60 + int(minute_text)


def _from_minutes_mod(total: int) -> str:
    normalized = total % 1440
    hour = normalized // 60
    minute = normalized % 60
    return f"{hour:02d}:{minute:02d}"


def _validate_schedule_date(value: str) -> None:
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid schedule_date: {value}") from exc
