"""Deterministic notification message generation from persisted schedule events."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

EVENT_TYPE_SHIFT_ADDED = "shift_added"
EVENT_TYPE_SHIFT_REMOVED = "shift_removed"
EVENT_TYPE_SHIFT_TIME_CHANGED = "shift_time_changed"
EVENT_TYPE_SHIFT_RELOCATED = "shift_relocated"
EVENT_TYPE_SHIFT_RETITLED = "shift_retitled"
EVENT_TYPE_SHIFT_RECLASSIFIED = "shift_reclassified"


@dataclass(frozen=True)
class ScheduleEvent:
    event_id: str
    user_id: int
    schedule_date: date
    event_type: str
    location_fingerprint: str
    customer_fingerprint: str
    old_value: dict[str, Any] | None
    new_value: dict[str, Any] | None
    source_session_id: str
    detected_at: datetime | None = None


@dataclass(frozen=True)
class UserNotification:
    notification_id: str
    user_id: int
    schedule_date: date
    source_session_id: str
    message: str
    notification_type: str
    event_ids: tuple[str, ...]


def build_notifications(
    events: list[ScheduleEvent | dict[str, Any]],
    *,
    summary_threshold: int = 3,
    today: date | None = None,
    already_notified_event_ids: set[str] | None = None,
) -> list[UserNotification]:
    if summary_threshold <= 0:
        raise ValueError("summary_threshold must be > 0")

    baseline_day = today
    seen = already_notified_event_ids if already_notified_event_ids is not None else set()

    normalized_events = sorted((_coerce_event(value) for value in events), key=_event_sort_key)

    fresh_events: list[ScheduleEvent] = []
    for event in normalized_events:
        dedupe_key = event.event_id or _semantic_event_key(event)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        fresh_events.append(event)

    groups: dict[tuple[int, date, str], list[ScheduleEvent]] = {}
    for event in fresh_events:
        key = (event.user_id, event.schedule_date, event.source_session_id)
        groups.setdefault(key, []).append(event)

    notifications: list[UserNotification] = []
    for group_key in sorted(groups.keys()):
        user_id, schedule_date, source_session_id = group_key
        grouped_events = groups[group_key]

        if len(grouped_events) >= summary_threshold:
            message = f"{len(grouped_events)} shifts updated for {_day_label(schedule_date, baseline_day)}"
            notifications.append(
                UserNotification(
                    notification_id=_notification_id(user_id, schedule_date, source_session_id, ("summary",) + tuple(e.event_id for e in grouped_events)),
                    user_id=user_id,
                    schedule_date=schedule_date,
                    source_session_id=source_session_id,
                    message=message,
                    notification_type="summary",
                    event_ids=tuple(event.event_id for event in grouped_events),
                )
            )
            continue

        for event in grouped_events:
            message = _event_message(event, baseline_day)
            notifications.append(
                UserNotification(
                    notification_id=_notification_id(user_id, schedule_date, source_session_id, (event.event_id,)),
                    user_id=user_id,
                    schedule_date=schedule_date,
                    source_session_id=source_session_id,
                    message=message,
                    notification_type="event",
                    event_ids=(event.event_id,),
                )
            )

    return notifications


def _coerce_event(value: ScheduleEvent | dict[str, Any]) -> ScheduleEvent:
    if isinstance(value, ScheduleEvent):
        return value
    if not isinstance(value, dict):
        raise TypeError(f"Unsupported event type: {type(value)!r}")

    schedule_date = value.get("schedule_date")
    if isinstance(schedule_date, date):
        parsed_date = schedule_date
    elif isinstance(schedule_date, str):
        parsed_date = date.fromisoformat(schedule_date)
    else:
        raise ValueError("schedule_date must be date or ISO date string")

    detected_at = value.get("detected_at")
    parsed_detected_at: datetime | None
    if detected_at is None:
        parsed_detected_at = None
    elif isinstance(detected_at, datetime):
        parsed_detected_at = detected_at
    elif isinstance(detected_at, str):
        parsed_detected_at = datetime.fromisoformat(detected_at.replace("Z", "+00:00"))
    else:
        raise ValueError("detected_at must be datetime or ISO datetime string")

    return ScheduleEvent(
        event_id=str(value.get("event_id", "")),
        user_id=int(value["user_id"]),
        schedule_date=parsed_date,
        event_type=str(value["event_type"]),
        location_fingerprint=str(value.get("location_fingerprint", "")),
        customer_fingerprint=str(value.get("customer_fingerprint", "")),
        old_value=value.get("old_value"),
        new_value=value.get("new_value"),
        source_session_id=str(value.get("source_session_id", "")),
        detected_at=parsed_detected_at,
    )


def _event_sort_key(event: ScheduleEvent) -> tuple:
    shift = event.new_value if event.new_value is not None else (event.old_value if event.old_value is not None else {})
    start = str(shift.get("start", "99:99"))
    return (
        event.user_id,
        event.schedule_date.isoformat(),
        start,
        event.location_fingerprint,
        event.event_type,
        event.source_session_id,
        event.detected_at.isoformat() if event.detected_at is not None else "",
        event.event_id,
    )


def _event_message(event: ScheduleEvent, baseline_day: date | None) -> str:
    day_upper = _day_label_capitalized(event.schedule_date, baseline_day)
    day_lower = _day_label(event.schedule_date, baseline_day)
    old_shift = event.old_value or {}
    new_shift = event.new_value or {}

    if event.event_type == EVENT_TYPE_SHIFT_ADDED:
        return (
            f"New shift added {day_lower} "
            f"{new_shift.get('start', '--:--')}–{new_shift.get('end', '--:--')} "
            f"in {new_shift.get('city', 'unknown location')}"
        )
    if event.event_type == EVENT_TYPE_SHIFT_REMOVED:
        return (
            f"Shift removed {day_lower} "
            f"{old_shift.get('start', '--:--')}–{old_shift.get('end', '--:--')} "
            f"in {old_shift.get('city', 'unknown location')}"
        )
    if event.event_type == EVENT_TYPE_SHIFT_TIME_CHANGED:
        time_delta = _time_change_phrase(old_shift, new_shift)
        return (
            f"{day_upper} {new_shift.get('city', old_shift.get('city', 'shift'))} shift moved "
            f"{time_delta}"
        )
    if event.event_type == EVENT_TYPE_SHIFT_RELOCATED:
        return (
            f"{day_upper} {new_shift.get('start', old_shift.get('start', '--:--'))} shift moved to "
            f"{new_shift.get('city', 'unknown location')}"
        )
    if event.event_type == EVENT_TYPE_SHIFT_RECLASSIFIED:
        type_text = new_shift.get("raw_type_label") or _shift_type_label(new_shift.get("shift_type", "UNKNOWN"))
        return (
            f"{day_upper} job updated to "
            f"{type_text}"
        )
    if event.event_type == EVENT_TYPE_SHIFT_RETITLED:
        return (
            f"{day_upper} shift updated for "
            f"{new_shift.get('customer_name', old_shift.get('customer_name', 'customer'))}"
        )
    return f"{day_upper} schedule updated"


def _time_change_phrase(old_shift: dict[str, Any], new_shift: dict[str, Any]) -> str:
    old_start = str(old_shift.get("start", "--:--"))
    old_end = str(old_shift.get("end", "--:--"))
    new_start = str(new_shift.get("start", "--:--"))
    new_end = str(new_shift.get("end", "--:--"))

    start_changed = old_start != new_start
    end_changed = old_end != new_end

    if start_changed and not end_changed:
        return f"{old_start} → {new_start}"
    if end_changed and not start_changed:
        return f"ends {old_end} → {new_end}"
    return f"{old_start}–{old_end} → {new_start}–{new_end}"


def _shift_type_label(value: str) -> str:
    mapping = {
        "WORK": "Work shift",
        "TRAVEL": "Travel",
        "TRAINING": "Training",
        "BREAK": "Break",
        "MEETING": "Meeting",
        "ADMIN": "Administrative task",
        "LEAVE": "Leave",
        "UNAVAILABLE": "Unavailable",
        # Backward compatibility for older snapshots/events.
        "SCHOOL": "Work shift",
        "OFFICE": "Work shift",
        "HOME_VISIT": "Work shift",
        "UNKNOWN": "Unknown job type",
    }
    return mapping.get(value, value.title())


def _day_label(schedule_date: date, baseline_day: date | None) -> str:
    if baseline_day is None:
        return f"on {schedule_date.isoformat()}"
    if schedule_date == baseline_day:
        return "today"
    if schedule_date == baseline_day.fromordinal(baseline_day.toordinal() + 1):
        return "tomorrow"
    return f"on {schedule_date.isoformat()}"


def _day_label_capitalized(schedule_date: date, baseline_day: date | None) -> str:
    value = _day_label(schedule_date, baseline_day)
    return value[:1].upper() + value[1:]


def _notification_id(user_id: int, schedule_date: date, source_session_id: str, parts: tuple[str, ...]) -> str:
    payload = "|".join([str(user_id), schedule_date.isoformat(), source_session_id, *parts])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _semantic_event_key(event: ScheduleEvent) -> str:
    payload = "|".join(
        [
            str(event.user_id),
            event.schedule_date.isoformat(),
            event.source_session_id,
            event.event_type,
            event.location_fingerprint,
            event.customer_fingerprint,
            _value_key(event.old_value),
            _value_key(event.new_value),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _value_key(value: dict[str, Any] | None) -> str:
    if value is None:
        return "null"
    keys = sorted(value.keys())
    parts = [f"{key}:{value[key]}" for key in keys]
    return "|".join(parts)
