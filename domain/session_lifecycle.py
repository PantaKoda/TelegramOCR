"""Session idle/finalization gating for safe observation processing."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping

from psycopg import sql
from psycopg.rows import dict_row


@dataclass(frozen=True)
class SessionLifecycleConfig:
    idle_timeout_seconds: int = 25
    open_state: str = "closed"
    processing_state: str = "processing"
    processed_state: str = "done"
    failed_state: str = "failed"


def load_lifecycle_config_from_env(
    default: SessionLifecycleConfig | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> SessionLifecycleConfig:
    base = default or SessionLifecycleConfig()
    source = env or os.environ
    idle_timeout_seconds = base.idle_timeout_seconds
    raw_timeout = source.get("SESSION_IDLE_TIMEOUT_SECONDS")
    if raw_timeout is not None:
        try:
            idle_timeout_seconds = int(raw_timeout)
        except ValueError as error:
            raise ValueError("SESSION_IDLE_TIMEOUT_SECONDS must be an integer.") from error
        if idle_timeout_seconds < 0:
            raise ValueError("SESSION_IDLE_TIMEOUT_SECONDS must be >= 0.")

    open_state = _read_state_env(source, "OPEN_STATE", "PENDING_STATE") or base.open_state
    processing_state = _read_state_env(source, "PROCESSING_STATE") or base.processing_state
    processed_state = _read_state_env(source, "PROCESSED_STATE", "DONE_STATE") or base.processed_state
    failed_state = _read_state_env(source, "FAILED_STATE") or base.failed_state

    return SessionLifecycleConfig(
        idle_timeout_seconds=idle_timeout_seconds,
        open_state=open_state,
        processing_state=processing_state,
        processed_state=processed_state,
        failed_state=failed_state,
    )


def find_finalizable_sessions(
    conn: Any,
    schema: str,
    now: datetime,
    *,
    config: SessionLifecycleConfig | None = None,
) -> list[str]:
    lifecycle = config or SessionLifecycleConfig()
    _validate_now(now)
    if lifecycle.idle_timeout_seconds < 0:
        raise ValueError("idle_timeout_seconds must be >= 0")

    cutoff = now - timedelta(seconds=lifecycle.idle_timeout_seconds)
    query = sql.SQL(
        """
        SELECT cs.id::text AS id
        FROM {}.capture_session cs
        JOIN {}.capture_image ci ON ci.session_id = cs.id
        WHERE cs.state::text = %s
        GROUP BY cs.id
        HAVING MAX(ci.created_at) <= %s
        ORDER BY MAX(ci.created_at), cs.id
        """
    ).format(sql.Identifier(schema), sql.Identifier(schema))

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, (lifecycle.open_state, cutoff))
        rows = cur.fetchall()
    return [row["id"] for row in rows]


def finalize_session(
    conn: Any,
    schema: str,
    session_id: str,
    *,
    config: SessionLifecycleConfig | None = None,
) -> bool:
    lifecycle = config or SessionLifecycleConfig()
    query = sql.SQL(
        """
        UPDATE {}.capture_session
        SET state = %s
        WHERE id = %s
          AND state::text = %s
        """
    ).format(sql.Identifier(schema))

    with conn.cursor() as cur:
        cur.execute(query, (lifecycle.processing_state, session_id, lifecycle.open_state))
        return cur.rowcount == 1


def mark_session_processed(
    conn: Any,
    schema: str,
    session_id: str,
    *,
    config: SessionLifecycleConfig | None = None,
) -> bool:
    lifecycle = config or SessionLifecycleConfig()
    query = sql.SQL(
        """
        UPDATE {}.capture_session
        SET state = %s
        WHERE id = %s
          AND state::text = %s
        """
    ).format(sql.Identifier(schema))

    with conn.cursor() as cur:
        cur.execute(query, (lifecycle.processed_state, session_id, lifecycle.processing_state))
        return cur.rowcount == 1


def mark_session_failed(
    conn: Any,
    schema: str,
    session_id: str,
    *,
    config: SessionLifecycleConfig | None = None,
) -> bool:
    lifecycle = config or SessionLifecycleConfig()
    query = sql.SQL(
        """
        UPDATE {}.capture_session
        SET state = %s
        WHERE id = %s
          AND state::text = %s
        """
    ).format(sql.Identifier(schema))

    with conn.cursor() as cur:
        cur.execute(query, (lifecycle.failed_state, session_id, lifecycle.processing_state))
        return cur.rowcount == 1


def process_finalized_session(
    conn: Any,
    schema: str,
    session_id: str,
    *,
    load_session_images: Callable[[Any, str, str], list[Any]],
    run_full_pipeline: Callable[[list[Any]], Any],
    persist_events_and_snapshot: Callable[[Any, str, str, Any], list[Any]],
    build_notifications: Callable[[list[Any]], list[Any]],
    store_notifications: Callable[[Any, str, str, list[Any]], int] | None = None,
    mark_processed: Callable[[Any, str, str], bool] | None = None,
) -> list[Any] | None:
    images = load_session_images(conn, schema, session_id)
    pipeline_output = run_full_pipeline(images)
    events = persist_events_and_snapshot(conn, schema, session_id, pipeline_output)
    notifications = build_notifications(events)

    marker = mark_processed or (lambda inner_conn, inner_schema, inner_session_id: mark_session_processed(inner_conn, inner_schema, inner_session_id))
    applied = marker(conn, schema, session_id)
    if not applied:
        return None
    if store_notifications is not None:
        store_notifications(conn, schema, session_id, notifications)
    return notifications


def run_lifecycle_once(
    conn: Any,
    schema: str,
    now: datetime,
    *,
    load_session_images: Callable[[Any, str, str], list[Any]],
    run_full_pipeline: Callable[[list[Any]], Any],
    persist_events_and_snapshot: Callable[[Any, str, str, Any], list[Any]],
    build_notifications: Callable[[list[Any]], list[Any]],
    store_notifications: Callable[[Any, str, str, list[Any]], int] | None = None,
    on_session_finalized: Callable[[str], None] | None = None,
    on_session_processed: Callable[[str, list[Any]], None] | None = None,
    on_session_failed: Callable[[str, Exception, bool], None] | None = None,
    config: SessionLifecycleConfig | None = None,
) -> list[tuple[str, list[Any]]]:
    lifecycle = config or SessionLifecycleConfig()
    finalized: list[tuple[str, list[Any]]] = []

    session_ids = find_finalizable_sessions(conn, schema, now, config=lifecycle)
    for session_id in session_ids:
        claimed = finalize_session(conn, schema, session_id, config=lifecycle)
        if not claimed:
            continue
        if on_session_finalized is not None:
            on_session_finalized(session_id)
        try:
            notifications = process_finalized_session(
                conn,
                schema,
                session_id,
                load_session_images=load_session_images,
                run_full_pipeline=run_full_pipeline,
                persist_events_and_snapshot=persist_events_and_snapshot,
                build_notifications=build_notifications,
                store_notifications=store_notifications,
                mark_processed=lambda inner_conn, inner_schema, inner_session_id: mark_session_processed(
                    inner_conn,
                    inner_schema,
                    inner_session_id,
                    config=lifecycle,
                ),
            )
        except Exception as error:
            marked_failed = mark_session_failed(conn, schema, session_id, config=lifecycle)
            if on_session_failed is not None:
                on_session_failed(session_id, error, marked_failed)
            continue
        if notifications is None:
            continue
        finalized.append((session_id, notifications))
        if on_session_processed is not None:
            on_session_processed(session_id, notifications)
    return finalized


def _validate_now(value: datetime) -> None:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError("now must be timezone-aware")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _read_state_env(source: Mapping[str, str], *names: str) -> str | None:
    for name in names:
        raw = source.get(name)
        if raw is None:
            continue
        value = raw.strip()
        if not value:
            raise ValueError(f"{name} must be a non-empty state value.")
        return value
    return None
