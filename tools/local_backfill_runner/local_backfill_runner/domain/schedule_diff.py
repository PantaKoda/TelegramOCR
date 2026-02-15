"""Deterministic change detection between canonical schedule versions."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Callable

from local_backfill_runner.parser.semantic_normalizer import CanonicalShift


@dataclass(frozen=True)
class ShiftAdded:
    schedule_date: str
    shift: CanonicalShift


@dataclass(frozen=True)
class ShiftRemoved:
    schedule_date: str
    shift: CanonicalShift


@dataclass(frozen=True)
class ShiftTimeChanged:
    schedule_date: str
    before: CanonicalShift
    after: CanonicalShift


@dataclass(frozen=True)
class ShiftRelocated:
    schedule_date: str
    before: CanonicalShift
    after: CanonicalShift


@dataclass(frozen=True)
class ShiftRetitled:
    schedule_date: str
    before: CanonicalShift
    after: CanonicalShift


@dataclass(frozen=True)
class ShiftReclassified:
    schedule_date: str
    before: CanonicalShift
    after: CanonicalShift


ScheduleDiffEvent = ShiftAdded | ShiftRemoved | ShiftTimeChanged | ShiftRelocated | ShiftRetitled | ShiftReclassified


@dataclass(frozen=True)
class _ShiftRef:
    sequence: int
    shift: CanonicalShift


def diff_schedules(
    previous_version: list[CanonicalShift],
    current_version: list[CanonicalShift],
    *,
    schedule_date: str,
) -> list[ScheduleDiffEvent]:
    _validate_schedule_date(schedule_date)

    old_refs = [_ShiftRef(sequence=index, shift=value) for index, value in enumerate(previous_version)]
    new_refs = [_ShiftRef(sequence=index, shift=value) for index, value in enumerate(current_version)]

    events: list[ScheduleDiffEvent] = []

    # Stage 1: stable identity match (location + customer + date).
    exact_pairs, old_refs, new_refs = _pair_by_key(
        old_refs,
        new_refs,
        key_fn=lambda ref: (schedule_date, ref.shift.location_fingerprint, ref.shift.customer_fingerprint),
        pair_mode="time_distance",
    )
    for old_ref, new_ref in exact_pairs:
        if (old_ref.shift.start, old_ref.shift.end) != (new_ref.shift.start, new_ref.shift.end):
            events.append(ShiftTimeChanged(schedule_date=schedule_date, before=old_ref.shift, after=new_ref.shift))
        elif old_ref.shift.customer_name != new_ref.shift.customer_name:
            events.append(ShiftRetitled(schedule_date=schedule_date, before=old_ref.shift, after=new_ref.shift))
        elif old_ref.shift.shift_type != new_ref.shift.shift_type:
            events.append(ShiftReclassified(schedule_date=schedule_date, before=old_ref.shift, after=new_ref.shift))

    # Stage 2: relocation detection (same customer + time + date, moved location).
    relocation_pairs, old_refs, new_refs = _pair_by_key(
        old_refs,
        new_refs,
        key_fn=lambda ref: (
            schedule_date,
            ref.shift.customer_fingerprint,
            ref.shift.start,
            ref.shift.end,
        ),
    )
    for old_ref, new_ref in relocation_pairs:
        if old_ref.shift.location_fingerprint != new_ref.shift.location_fingerprint:
            events.append(ShiftRelocated(schedule_date=schedule_date, before=old_ref.shift, after=new_ref.shift))
        elif old_ref.shift.customer_name != new_ref.shift.customer_name:
            events.append(ShiftRetitled(schedule_date=schedule_date, before=old_ref.shift, after=new_ref.shift))

    # Stage 3: retitle detection (same location + time + date, renamed customer).
    retitle_pairs, old_refs, new_refs = _pair_by_key(
        old_refs,
        new_refs,
        key_fn=lambda ref: (
            schedule_date,
            ref.shift.location_fingerprint,
            ref.shift.start,
            ref.shift.end,
        ),
    )
    for old_ref, new_ref in retitle_pairs:
        if old_ref.shift.customer_fingerprint != new_ref.shift.customer_fingerprint:
            events.append(ShiftRetitled(schedule_date=schedule_date, before=old_ref.shift, after=new_ref.shift))

    for ref in sorted(old_refs, key=_ref_sort_key):
        events.append(ShiftRemoved(schedule_date=schedule_date, shift=ref.shift))

    for ref in sorted(new_refs, key=_ref_sort_key):
        events.append(ShiftAdded(schedule_date=schedule_date, shift=ref.shift))

    return events


def _pair_by_key(
    old_refs: list[_ShiftRef],
    new_refs: list[_ShiftRef],
    *,
    key_fn: Callable[[_ShiftRef], tuple],
    pair_mode: str = "index",
) -> tuple[list[tuple[_ShiftRef, _ShiftRef]], list[_ShiftRef], list[_ShiftRef]]:
    old_by_key: dict[tuple, list[_ShiftRef]] = defaultdict(list)
    new_by_key: dict[tuple, list[_ShiftRef]] = defaultdict(list)

    for ref in old_refs:
        old_by_key[key_fn(ref)].append(ref)
    for ref in new_refs:
        new_by_key[key_fn(ref)].append(ref)

    paired: list[tuple[_ShiftRef, _ShiftRef]] = []
    consumed_old: set[int] = set()
    consumed_new: set[int] = set()

    for key in sorted(set(old_by_key.keys()) & set(new_by_key.keys())):
        old_values = sorted(old_by_key[key], key=_ref_sort_key)
        new_values = sorted(new_by_key[key], key=_ref_sort_key)

        if pair_mode == "time_distance":
            pairs = _pair_group_by_time_distance(old_values, new_values)
        else:
            pairs = _pair_group_by_index(old_values, new_values)

        for old_ref, new_ref in pairs:
            paired.append((old_ref, new_ref))
            consumed_old.add(old_ref.sequence)
            consumed_new.add(new_ref.sequence)

    remaining_old = [ref for ref in old_refs if ref.sequence not in consumed_old]
    remaining_new = [ref for ref in new_refs if ref.sequence not in consumed_new]
    return paired, remaining_old, remaining_new


def _ref_sort_key(ref: _ShiftRef) -> tuple:
    shift = ref.shift
    return (
        shift.location_fingerprint,
        shift.customer_fingerprint,
        shift.start,
        shift.end,
        shift.customer_name.casefold(),
        shift.street.casefold(),
        shift.street_number.casefold(),
        shift.city.casefold(),
        ref.sequence,
    )


def _pair_group_by_index(old_values: list[_ShiftRef], new_values: list[_ShiftRef]) -> list[tuple[_ShiftRef, _ShiftRef]]:
    pair_count = min(len(old_values), len(new_values))
    pairs: list[tuple[_ShiftRef, _ShiftRef]] = []
    for index in range(pair_count):
        pairs.append((old_values[index], new_values[index]))
    return pairs


def _pair_group_by_time_distance(
    old_values: list[_ShiftRef],
    new_values: list[_ShiftRef],
) -> list[tuple[_ShiftRef, _ShiftRef]]:
    # Greedy minimal pairing for duplicate-identity same-day instances.
    remaining_old = list(old_values)
    remaining_new = list(new_values)
    pairs: list[tuple[_ShiftRef, _ShiftRef]] = []

    while remaining_old and remaining_new:
        best_old_index = 0
        best_new_index = 0
        best_score: tuple[int, tuple, tuple] | None = None

        for old_index, old_ref in enumerate(remaining_old):
            for new_index, new_ref in enumerate(remaining_new):
                score = (
                    _time_distance_minutes(old_ref.shift, new_ref.shift),
                    _ref_sort_key(old_ref),
                    _ref_sort_key(new_ref),
                )
                if best_score is None or score < best_score:
                    best_score = score
                    best_old_index = old_index
                    best_new_index = new_index

        pairs.append((remaining_old.pop(best_old_index), remaining_new.pop(best_new_index)))

    return pairs


def _time_distance_minutes(old: CanonicalShift, new: CanonicalShift) -> int:
    return abs(_minutes(old.start) - _minutes(new.start)) + abs(_minutes(old.end) - _minutes(new.end))


def _minutes(value: str) -> int:
    hour_text, minute_text = value.split(":", 1)
    return int(hour_text) * 60 + int(minute_text)


def _validate_schedule_date(value: str) -> None:
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid schedule_date: {value}") from exc
