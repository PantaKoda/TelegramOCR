"""Durable event history and day snapshot persistence."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict
from datetime import date, datetime, timezone
import hashlib
from typing import Any

from psycopg import sql
from psycopg.rows import dict_row

from local_backfill_runner.domain.schedule_diff import (
    ShiftAdded,
    ShiftReclassified,
    ShiftRelocated,
    ShiftRemoved,
    ShiftRetitled,
    ShiftTimeChanged,
    diff_schedules,
)
from local_backfill_runner.parser.semantic_normalizer import CanonicalShift

EVENT_TYPE_SHIFT_ADDED = "shift_added"
EVENT_TYPE_SHIFT_REMOVED = "shift_removed"
EVENT_TYPE_SHIFT_TIME_CHANGED = "shift_time_changed"
EVENT_TYPE_SHIFT_RELOCATED = "shift_relocated"
EVENT_TYPE_SHIFT_RETITLED = "shift_retitled"
EVENT_TYPE_SHIFT_RECLASSIFIED = "shift_reclassified"


def load_day_snapshot(
    conn: Any,
    schema: str,
    *,
    user_id: int,
    schedule_date: date,
) -> list[CanonicalShift]:
    query = sql.SQL(
        """
        SELECT snapshot_payload
        FROM {}.day_snapshot
        WHERE user_id = %s
          AND schedule_date = %s
        """
    ).format(sql.Identifier(schema))

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, (user_id, schedule_date))
        row = cur.fetchone()

    if row is None:
        return []

    payload = row["snapshot_payload"]
    if not isinstance(payload, list):
        raise RuntimeError("day_snapshot.snapshot_payload must be a JSON array.")
    return [_canonical_shift_from_dict(item) for item in payload]


def persist_events_and_snapshot(
    conn: Any,
    schema: str,
    *,
    user_id: int,
    schedule_date: date,
    source_session_id: str,
    events: list[Any],
    snapshot: list[CanonicalShift],
    detected_at: datetime | None = None,
) -> int:
    timestamp = detected_at or datetime.now(timezone.utc)
    event_rows = [_event_row(user_id=user_id, schedule_date=schedule_date, source_session_id=source_session_id, event=e) for e in events]

    insert_event_query = sql.SQL(
        """
        INSERT INTO {}.schedule_event (
            event_id,
            user_id,
            schedule_date,
            event_type,
            location_fingerprint,
            customer_fingerprint,
            old_value_hash,
            new_value_hash,
            old_value,
            new_value,
            detected_at,
            source_session_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s)
        ON CONFLICT (
            user_id,
            schedule_date,
            location_fingerprint,
            event_type,
            old_value_hash,
            new_value_hash
        )
        DO NOTHING
        """
    ).format(sql.Identifier(schema))

    upsert_snapshot_query = sql.SQL(
        """
        INSERT INTO {}.day_snapshot (
            user_id,
            schedule_date,
            snapshot_payload,
            source_session_id,
            updated_at
        )
        VALUES (%s, %s, %s::jsonb, %s, %s)
        ON CONFLICT (user_id, schedule_date)
        DO UPDATE
        SET snapshot_payload = EXCLUDED.snapshot_payload,
            source_session_id = EXCLUDED.source_session_id,
            updated_at = EXCLUDED.updated_at
        """
    ).format(sql.Identifier(schema))

    inserted_count = 0
    with conn.cursor() as cur:
        for row in event_rows:
            cur.execute(
                insert_event_query,
                (
                    row["event_id"],
                    row["user_id"],
                    row["schedule_date"],
                    row["event_type"],
                    row["location_fingerprint"],
                    row["customer_fingerprint"],
                    row["old_value_hash"],
                    row["new_value_hash"],
                    json.dumps(row["old_value"], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
                    if row["old_value"] is not None
                    else None,
                    json.dumps(row["new_value"], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
                    if row["new_value"] is not None
                    else None,
                    timestamp,
                    row["source_session_id"],
                ),
            )
            inserted_count += cur.rowcount

        snapshot_payload = [_canonical_shift_to_dict(shift) for shift in snapshot]
        cur.execute(
            upsert_snapshot_query,
            (
                user_id,
                schedule_date,
                json.dumps(snapshot_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
                source_session_id,
                timestamp,
            ),
        )

    return inserted_count


def process_observation(
    conn: Any,
    schema: str,
    *,
    user_id: int,
    schedule_date: date,
    source_session_id: str,
    current_snapshot: list[CanonicalShift],
    detected_at: datetime | None = None,
) -> list[Any]:
    previous_snapshot = load_day_snapshot(conn, schema, user_id=user_id, schedule_date=schedule_date)
    events = diff_schedules(previous_snapshot, current_snapshot, schedule_date=schedule_date.isoformat())
    persist_events_and_snapshot(
        conn,
        schema,
        user_id=user_id,
        schedule_date=schedule_date,
        source_session_id=source_session_id,
        events=events,
        snapshot=current_snapshot,
        detected_at=detected_at,
    )
    return events


def _event_row(
    *,
    user_id: int,
    schedule_date: date,
    source_session_id: str,
    event: Any,
) -> dict[str, Any]:
    event_type, old_shift, new_shift = _event_shape(event)
    location_source = new_shift if new_shift is not None else old_shift
    customer_source = new_shift if new_shift is not None else old_shift
    if location_source is None or customer_source is None:
        raise RuntimeError(f"Invalid event payload for {event_type}: missing shift identity.")

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "schedule_date": schedule_date,
        "event_type": event_type,
        "location_fingerprint": location_source.location_fingerprint,
        "customer_fingerprint": customer_source.customer_fingerprint,
        "old_value_hash": _value_hash(_canonical_shift_to_dict(old_shift) if old_shift is not None else None),
        "new_value_hash": _value_hash(_canonical_shift_to_dict(new_shift) if new_shift is not None else None),
        "old_value": _canonical_shift_to_dict(old_shift) if old_shift is not None else None,
        "new_value": _canonical_shift_to_dict(new_shift) if new_shift is not None else None,
        "source_session_id": source_session_id,
    }


def _event_shape(event: Any) -> tuple[str, CanonicalShift | None, CanonicalShift | None]:
    if isinstance(event, ShiftAdded):
        return EVENT_TYPE_SHIFT_ADDED, None, event.shift
    if isinstance(event, ShiftRemoved):
        return EVENT_TYPE_SHIFT_REMOVED, event.shift, None
    if isinstance(event, ShiftTimeChanged):
        return EVENT_TYPE_SHIFT_TIME_CHANGED, event.before, event.after
    if isinstance(event, ShiftRelocated):
        return EVENT_TYPE_SHIFT_RELOCATED, event.before, event.after
    if isinstance(event, ShiftRetitled):
        return EVENT_TYPE_SHIFT_RETITLED, event.before, event.after
    if isinstance(event, ShiftReclassified):
        return EVENT_TYPE_SHIFT_RECLASSIFIED, event.before, event.after
    raise TypeError(f"Unsupported diff event type: {type(event)!r}")


def _canonical_shift_to_dict(shift: CanonicalShift) -> dict[str, Any]:
    return asdict(shift)


def _value_hash(value: dict[str, Any] | None) -> str:
    if value is None:
        payload = "null"
    else:
        payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _canonical_shift_from_dict(value: Any) -> CanonicalShift:
    if not isinstance(value, dict):
        raise RuntimeError("Snapshot item must be an object.")

    required = {
        "start",
        "end",
        "customer_name",
        "customer_fingerprint",
        "street",
        "street_number",
        "postal_code",
        "postal_area",
        "city",
        "location_fingerprint",
        "shift_type",
    }
    missing = required - set(value.keys())
    if missing:
        raise RuntimeError(f"Snapshot item missing required fields: {sorted(missing)}")

    return CanonicalShift(
        start=str(value["start"]),
        end=str(value["end"]),
        customer_name=str(value["customer_name"]),
        customer_fingerprint=str(value["customer_fingerprint"]),
        street=str(value["street"]),
        street_number=str(value["street_number"]),
        postal_code=str(value["postal_code"]),
        postal_area=str(value["postal_area"]),
        city=str(value["city"]),
        location_fingerprint=str(value["location_fingerprint"]),
        shift_type=str(value["shift_type"]),
        raw_type_label=str(value.get("raw_type_label", "")),
    )
