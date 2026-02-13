"""Durable storage for user-facing schedule notifications."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

from psycopg import sql

from domain.notification_rules import UserNotification

NOTIFICATION_STATUS_PENDING = "pending"


def persist_notifications(
    conn: Any,
    schema: str,
    *,
    notifications: list[UserNotification | dict[str, Any]],
    created_at: datetime | None = None,
) -> int:
    timestamp = created_at or datetime.now(timezone.utc)
    rows = [_coerce_notification(value) for value in notifications]
    if not rows:
        return 0

    query = sql.SQL(
        """
        INSERT INTO {}.schedule_notification (
            notification_id,
            user_id,
            schedule_date,
            source_session_id,
            status,
            notification_type,
            message,
            event_ids,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
        ON CONFLICT (notification_id)
        DO NOTHING
        """
    ).format(sql.Identifier(schema))

    inserted = 0
    with conn.cursor() as cur:
        for item in rows:
            cur.execute(
                query,
                (
                    item.notification_id,
                    item.user_id,
                    item.schedule_date,
                    item.source_session_id,
                    NOTIFICATION_STATUS_PENDING,
                    item.notification_type,
                    item.message,
                    json.dumps(list(item.event_ids), separators=(",", ":"), ensure_ascii=False),
                    timestamp,
                ),
            )
            inserted += cur.rowcount
    return inserted


def _coerce_notification(value: UserNotification | dict[str, Any]) -> UserNotification:
    if isinstance(value, UserNotification):
        return value
    if not isinstance(value, dict):
        raise TypeError(f"Unsupported notification value type: {type(value)!r}")

    schedule_date = value.get("schedule_date")
    if isinstance(schedule_date, date):
        parsed_date = schedule_date
    elif isinstance(schedule_date, str):
        parsed_date = date.fromisoformat(schedule_date)
    else:
        raise ValueError("schedule_date must be date or ISO date string")

    event_ids_raw = value.get("event_ids")
    if event_ids_raw is None:
        event_ids: tuple[str, ...] = ()
    elif isinstance(event_ids_raw, tuple):
        event_ids = tuple(str(item) for item in event_ids_raw)
    elif isinstance(event_ids_raw, list):
        event_ids = tuple(str(item) for item in event_ids_raw)
    else:
        raise ValueError("event_ids must be a list or tuple of strings")

    notification_id = str(value.get("notification_id", ""))
    if not notification_id:
        raise ValueError("notification_id is required")

    return UserNotification(
        notification_id=notification_id,
        user_id=int(value["user_id"]),
        schedule_date=parsed_date,
        source_session_id=str(value["source_session_id"]),
        message=str(value["message"]),
        notification_type=str(value["notification_type"]),
        event_ids=event_ids,
    )
