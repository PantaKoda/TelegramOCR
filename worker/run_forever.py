"""Continuously running background worker loop for session processing."""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from domain import schedule_diff
from domain.notification_rules import build_notifications
from domain.session_lifecycle import (
    SessionLifecycleConfig,
    load_lifecycle_config_from_env,
    run_lifecycle_once,
    utc_now,
)
from infra.event_store import load_day_snapshot, process_observation
from infra.notification_store import persist_notifications
from parser.semantic_normalizer import CanonicalShift, normalize_entries

DEFAULT_FIXTURE_PAYLOAD_PATH = "fixtures/sample_schedule.json"
SERVICE_NAME = "python-worker"


class JsonFormatter(logging.Formatter):
    _RESERVED = {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "message",
        "asctime",
    }

    def format(self, record: logging.LogRecord) -> str:
        event_name = getattr(record, "event", "log")
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "service": SERVICE_NAME,
            "level": record.levelname,
            "event": event_name,
            "message": record.getMessage(),
            "logger": record.name,
            "session_id": getattr(record, "session_id", None),
            "user_id": getattr(record, "user_id", None),
            "correlation_id": getattr(record, "correlation_id", None),
        }
        for key, value in record.__dict__.items():
            if key not in self._RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, sort_keys=True, default=str)


@dataclass(frozen=True)
class WorkerRuntimeConfig:
    database_url: str
    db_schema: str
    poll_seconds: float
    fixture_payload_path: str
    summary_threshold: int


class WorkerStageError(RuntimeError):
    def __init__(self, stage: str, message: str, *, cause: Exception | None = None) -> None:
        super().__init__(message)
        self.stage = stage
        self.__cause__ = cause


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("ocr-worker-loop")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def load_runtime_config() -> WorkerRuntimeConfig:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("Missing required environment variable: DATABASE_URL")

    poll_seconds = _parse_positive_float_env("WORKER_POLL_SECONDS", 5.0)
    summary_threshold = _parse_positive_int_env("NOTIFICATION_SUMMARY_THRESHOLD", 3)

    return WorkerRuntimeConfig(
        database_url=database_url,
        db_schema=os.getenv("DB_SCHEMA", "schedule_ingest"),
        poll_seconds=poll_seconds,
        fixture_payload_path=os.getenv("FIXTURE_PAYLOAD_PATH", DEFAULT_FIXTURE_PAYLOAD_PATH),
        summary_threshold=summary_threshold,
    )


def run_iteration(
    conn: psycopg.Connection,
    config: WorkerRuntimeConfig,
    lifecycle_config: SessionLifecycleConfig,
    *,
    logger: logging.Logger,
) -> dict[str, int]:
    stored_notification_count = 0
    session_context: dict[str, dict[str, Any]] = {}
    iteration_now = utc_now()
    skipped_idle_count = _count_sessions_waiting_for_idle(
        conn,
        config.db_schema,
        lifecycle_config=lifecycle_config,
        now=iteration_now,
    )
    if skipped_idle_count > 0:
        logger.info(
            "Sessions waiting for idle timeout",
            extra={
                "event": "session.skipped_idle",
                "session_id": None,
                "user_id": None,
                "correlation_id": None,
                "skipped_session_count": skipped_idle_count,
            },
        )

    def load_session_images(inner_conn: Any, schema: str, session_id: str) -> list[dict[str, Any]]:
        query = sql.SQL(
            """
            SELECT id::text AS id, session_id::text AS session_id, sequence, r2_key, created_at
            FROM {}.capture_image
            WHERE session_id = %s
            ORDER BY sequence ASC
            """
        ).format(sql.Identifier(schema))
        with inner_conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (session_id,))
            rows = list(cur.fetchall())
        if not rows:
            raise WorkerStageError("lifecycle", f"Session {session_id} has no capture images.")

        context = _ensure_session_context(inner_conn, schema, session_id, session_context)
        logger.info(
            "Session images loaded",
            extra={
                "event": "session.images_loaded",
                "session_id": session_id,
                "user_id": context["user_id"],
                "correlation_id": context["correlation_id"],
                "image_count": len(rows),
            },
        )
        return rows

    def run_full_pipeline(images: list[dict[str, Any]]) -> dict[str, Any]:
        session_id = str(images[0].get("session_id", "")) if images else ""
        context = session_context.get(session_id, {"user_id": None, "correlation_id": session_id or None})
        try:
            fixture_payload = _load_fixture_payload(config.fixture_payload_path)
            entries = _coerce_fixture_entries(fixture_payload)
        except Exception as error:
            raise WorkerStageError("ocr", "Failed loading OCR payload.", cause=error) from error

        logger.info(
            "OCR stage completed",
            extra={
                "event": "ocr.completed",
                "session_id": session_id or None,
                "user_id": context["user_id"],
                "correlation_id": context["correlation_id"],
                "text_block_count": len(entries),
                "mode": "fixture",
            },
        )

        try:
            schedule_date = _parse_schedule_date(fixture_payload)
            canonical_shifts = normalize_entries(entries)
            canonical_shifts.sort(key=_canonical_shift_sort_key)
        except Exception as error:
            raise WorkerStageError("layout", "Failed layout/semantic normalization.", cause=error) from error

        logger.info(
            "Layout shifts detected",
            extra={
                "event": "layout.shifts_detected",
                "session_id": session_id or None,
                "user_id": context["user_id"],
                "correlation_id": context["correlation_id"],
                "shift_count": len(canonical_shifts),
                "schedule_date": schedule_date.isoformat(),
            },
        )
        logger.info(
            "Aggregation completed",
            extra={
                "event": "aggregation.completed",
                "session_id": session_id or None,
                "user_id": context["user_id"],
                "correlation_id": context["correlation_id"],
                "input_shift_count": len(canonical_shifts),
                "output_shift_count": len(canonical_shifts),
            },
        )
        return {
            "session_id": session_id,
            "schedule_date": schedule_date,
            "canonical_shifts": canonical_shifts,
            "image_count": len(images),
        }

    def persist_events_and_snapshot(inner_conn: Any, schema: str, session_id: str, pipeline_output: dict[str, Any]) -> list[dict[str, Any]]:
        context = _ensure_session_context(inner_conn, schema, session_id, session_context)
        schedule_date_value = pipeline_output["schedule_date"]
        canonical_shifts_value = pipeline_output["canonical_shifts"]
        if not isinstance(schedule_date_value, date):
            raise WorkerStageError("diff", "Pipeline output is missing schedule_date.")
        if not isinstance(canonical_shifts_value, list) or not all(
            isinstance(item, CanonicalShift) for item in canonical_shifts_value
        ):
            raise WorkerStageError("diff", "Pipeline output is missing canonical_shifts.")

        old_snapshot = load_day_snapshot(
            inner_conn,
            schema,
            user_id=context["user_id"],
            schedule_date=schedule_date_value,
        )
        try:
            domain_events = process_observation(
                inner_conn,
                schema,
                user_id=context["user_id"],
                schedule_date=schedule_date_value,
                source_session_id=session_id,
                current_snapshot=canonical_shifts_value,
            )
        except Exception as error:
            raise WorkerStageError("db", "Failed persisting events/snapshot.", cause=error) from error

        event_types = sorted({_domain_event_type_name(item) for item in domain_events})
        logger.info(
            "Diff computed",
            extra={
                "event": "diff.computed",
                "session_id": session_id,
                "user_id": context["user_id"],
                "correlation_id": context["correlation_id"],
                "old_shift_count": len(old_snapshot),
                "new_shift_count": len(canonical_shifts_value),
                "event_count": len(domain_events),
                "event_types": event_types,
            },
        )

        events = _load_session_events(inner_conn, schema, session_id)
        logger.info(
            "Events persisted",
            extra={
                "event": "events.persisted",
                "session_id": session_id,
                "user_id": context["user_id"],
                "correlation_id": context["correlation_id"],
                "event_count": len(events),
                "event_types": sorted({str(row["event_type"]) for row in events}),
            },
        )
        return events

    def store_notifications_callback(inner_conn: Any, schema: str, _session_id: str, notifications: list[Any]) -> int:
        nonlocal stored_notification_count
        context = _ensure_session_context(inner_conn, schema, _session_id, session_context)
        inserted = persist_notifications(inner_conn, schema, notifications=notifications)
        stored_notification_count += inserted
        logger.info(
            "Notifications stored",
            extra={
                "event": "notifications.stored",
                "session_id": _session_id,
                "user_id": context["user_id"],
                "correlation_id": context["correlation_id"],
                "notification_count": len(notifications),
                "stored_count": inserted,
            },
        )
        return inserted

    def build_notifications_callback(events: list[Any]) -> list[Any]:
        notifications = build_notifications(events, summary_threshold=config.summary_threshold)
        session_id: str | None = None
        user_id: int | None = None
        if events:
            first = events[0]
            if isinstance(first, dict):
                session_id = str(first.get("source_session_id") or "") or None
                raw_user_id = first.get("user_id")
                user_id = int(raw_user_id) if raw_user_id is not None else None
            else:
                session_id = str(getattr(first, "source_session_id", "") or "") or None
                raw_user_id = getattr(first, "user_id", None)
                user_id = int(raw_user_id) if raw_user_id is not None else None
        if session_id:
            context = session_context.get(session_id, {"user_id": user_id, "correlation_id": session_id})
        else:
            context = {"user_id": user_id, "correlation_id": session_id}
        summary_used = any(
            (item.get("notification_type") if isinstance(item, dict) else getattr(item, "notification_type", "")) == "summary"
            for item in notifications
        )
        logger.info(
            "Notifications generated",
            extra={
                "event": "notifications.generated",
                "session_id": session_id,
                "user_id": context["user_id"],
                "correlation_id": context["correlation_id"],
                "notification_count": len(notifications),
                "summary_used": summary_used,
            },
        )
        return notifications

    def on_session_finalized(session_id: str) -> None:
        context = _ensure_session_context(conn, config.db_schema, session_id, session_context)
        logger.info(
            "Session finalized",
            extra={
                "event": "session.finalized",
                "session_id": session_id,
                "user_id": context["user_id"],
                "correlation_id": context["correlation_id"],
            },
        )

    def on_session_processed(session_id: str, notifications: list[Any]) -> None:
        context = _ensure_session_context(conn, config.db_schema, session_id, session_context)
        logger.info(
            "Session processed",
            extra={
                "event": "session.processed",
                "session_id": session_id,
                "user_id": context["user_id"],
                "correlation_id": context["correlation_id"],
                "notification_count": len(notifications),
            },
        )

    processed = run_lifecycle_once(
        conn,
        config.db_schema,
        iteration_now,
        load_session_images=load_session_images,
        run_full_pipeline=run_full_pipeline,
        persist_events_and_snapshot=persist_events_and_snapshot,
        build_notifications=build_notifications_callback,
        store_notifications=store_notifications_callback,
        on_session_finalized=on_session_finalized,
        on_session_processed=on_session_processed,
        config=lifecycle_config,
    )

    notification_count = sum(len(item[1]) for item in processed)
    return {
        "processed_sessions": len(processed),
        "generated_notifications": notification_count,
        "stored_notifications": stored_notification_count,
    }


def run_forever() -> None:
    logger = setup_logger()
    config = load_runtime_config()
    lifecycle_config = load_lifecycle_config_from_env()

    logger.info(
        "Worker loop started",
        extra={
            "event": "worker.loop.started",
            "db_schema": config.db_schema,
            "poll_seconds": config.poll_seconds,
            "idle_timeout_seconds": lifecycle_config.idle_timeout_seconds,
        },
    )

    while True:
        logger.info("Lifecycle iteration started", extra={"event": "worker.iteration.start"})
        try:
            with psycopg.connect(config.database_url) as conn:
                with conn.transaction():
                    result = run_iteration(conn, config, lifecycle_config, logger=logger)
            logger.info(
                "Lifecycle iteration finished",
                extra={
                    "event": "worker.iteration.finish",
                    "processed_sessions": result["processed_sessions"],
                    "generated_notifications": result["generated_notifications"],
                    "stored_notifications": result["stored_notifications"],
                },
            )
        except Exception as error:
            stage = error.stage if isinstance(error, WorkerStageError) else "lifecycle"
            logger.exception(
                "Lifecycle iteration failed",
                extra={
                    "event": "worker.iteration.error",
                    "error.type": type(error).__name__,
                    "error.message": str(error),
                    "error.stage": stage,
                },
            )
        time.sleep(config.poll_seconds)


def _ensure_session_context(
    conn: Any,
    schema: str,
    session_id: str,
    cache: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    cached = cache.get(session_id)
    if cached is not None:
        return cached
    user_id = _load_session_user_id(conn, schema, session_id)
    value = {"user_id": user_id, "correlation_id": session_id}
    cache[session_id] = value
    return value


def _count_sessions_waiting_for_idle(
    conn: Any,
    schema: str,
    *,
    lifecycle_config: SessionLifecycleConfig,
    now: datetime,
) -> int:
    cutoff = now - timedelta(seconds=lifecycle_config.idle_timeout_seconds)
    query = sql.SQL(
        """
        SELECT COUNT(*) AS waiting_count
        FROM (
            SELECT cs.id
            FROM {}.capture_session cs
            LEFT JOIN {}.capture_image ci ON ci.session_id = cs.id
            WHERE cs.state::text = %s
            GROUP BY cs.id
            HAVING MAX(ci.created_at) IS NULL
               OR MAX(ci.created_at) > %s
        ) waiting
        """
    ).format(sql.Identifier(schema), sql.Identifier(schema))
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, (lifecycle_config.open_state, cutoff))
        row = cur.fetchone()
    return int(row["waiting_count"]) if row is not None else 0


def _domain_event_type_name(event: Any) -> str:
    if isinstance(event, schedule_diff.ShiftAdded):
        return "shift_added"
    if isinstance(event, schedule_diff.ShiftRemoved):
        return "shift_removed"
    if isinstance(event, schedule_diff.ShiftTimeChanged):
        return "shift_time_changed"
    if isinstance(event, schedule_diff.ShiftRelocated):
        return "shift_relocated"
    if isinstance(event, schedule_diff.ShiftRetitled):
        return "shift_retitled"
    if isinstance(event, schedule_diff.ShiftReclassified):
        return "shift_reclassified"
    return type(event).__name__


def _parse_positive_float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed = float(value)
    except ValueError as error:
        raise RuntimeError(f"{name} must be a number.") from error
    if parsed <= 0:
        raise RuntimeError(f"{name} must be > 0.")
    return parsed


def _parse_positive_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError as error:
        raise RuntimeError(f"{name} must be an integer.") from error
    if parsed <= 0:
        raise RuntimeError(f"{name} must be > 0.")
    return parsed


def _load_fixture_payload(path: str) -> dict[str, Any]:
    payload_path = Path(path)
    if not payload_path.exists():
        raise RuntimeError(f"Fixture payload file not found: {payload_path}")
    try:
        raw = json.loads(payload_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise RuntimeError(f"Fixture payload is not valid JSON: {payload_path}") from error
    if not isinstance(raw, dict):
        raise RuntimeError("Fixture payload must be a JSON object.")
    return raw


def _parse_schedule_date(payload: dict[str, Any]) -> date:
    value = payload.get("schedule_date")
    if not isinstance(value, str):
        raise RuntimeError("Fixture payload must include `schedule_date` as ISO date string.")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise RuntimeError(f"Invalid schedule_date in fixture payload: {value}") from error


def _coerce_fixture_entries(payload: dict[str, Any]) -> list[dict[str, str]]:
    raw_entries = payload.get("entries")
    if not isinstance(raw_entries, list):
        raise RuntimeError("Fixture payload must include `entries` as a list.")

    normalized: list[dict[str, str]] = []
    for item in raw_entries:
        if not isinstance(item, dict):
            raise RuntimeError("Each fixture entry must be an object.")
        normalized.append(
            {
                "start": str(item.get("start", "")),
                "end": str(item.get("end", "")),
                "title": str(item.get("title", "")),
                "location": str(item.get("location", "")),
                "address": str(item.get("address", "")),
            }
        )
    return normalized


def _canonical_shift_sort_key(shift: CanonicalShift) -> tuple[str, str, str, str, str, str]:
    return (
        shift.location_fingerprint,
        shift.customer_fingerprint,
        shift.start,
        shift.end,
        shift.city,
        shift.customer_name,
    )


def _load_session_user_id(conn: Any, schema: str, session_id: str) -> int:
    query = sql.SQL(
        """
        SELECT user_id
        FROM {}.capture_session
        WHERE id = %s
        """
    ).format(sql.Identifier(schema))
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, (session_id,))
        row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Session not found: {session_id}")
    return int(row["user_id"])


def _load_session_events(conn: Any, schema: str, session_id: str) -> list[dict[str, Any]]:
    query = sql.SQL(
        """
        SELECT
            event_id::text AS event_id,
            user_id,
            schedule_date,
            event_type,
            location_fingerprint,
            customer_fingerprint,
            old_value,
            new_value,
            source_session_id::text AS source_session_id,
            detected_at
        FROM {}.schedule_event
        WHERE source_session_id = %s
        ORDER BY detected_at ASC, event_id ASC
        """
    ).format(sql.Identifier(schema))
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, (session_id,))
        rows = list(cur.fetchall())
    return rows


def main() -> None:
    run_forever()


if __name__ == "__main__":
    main()
