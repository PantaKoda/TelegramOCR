"""Continuously running background worker loop for session processing."""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from domain.notification_rules import build_notifications
from domain.session_lifecycle import (
    SessionLifecycleConfig,
    load_lifecycle_config_from_env,
    run_lifecycle_once,
    utc_now,
)
from infra.event_store import process_observation
from infra.notification_store import persist_notifications
from parser.semantic_normalizer import CanonicalShift, normalize_entries

DEFAULT_FIXTURE_PAYLOAD_PATH = "fixtures/sample_schedule.json"


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
        payload: dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
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

    def load_session_images(inner_conn: Any, schema: str, session_id: str) -> list[dict[str, Any]]:
        query = sql.SQL(
            """
            SELECT id::text AS id, sequence, r2_key, created_at
            FROM {}.capture_image
            WHERE session_id = %s
            ORDER BY sequence ASC
            """
        ).format(sql.Identifier(schema))
        with inner_conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (session_id,))
            rows = list(cur.fetchall())
        if not rows:
            raise RuntimeError(f"Session {session_id} has no capture images.")
        return rows

    def run_full_pipeline(images: list[dict[str, Any]]) -> dict[str, Any]:
        fixture_payload = _load_fixture_payload(config.fixture_payload_path)
        schedule_date = _parse_schedule_date(fixture_payload)
        entries = _coerce_fixture_entries(fixture_payload)
        canonical_shifts = normalize_entries(entries)
        canonical_shifts.sort(key=_canonical_shift_sort_key)
        logger.info(
            "Pipeline payload prepared",
            extra={
                "event": "worker.pipeline.prepared",
                "image_count": len(images),
                "canonical_shift_count": len(canonical_shifts),
                "schedule_date": schedule_date.isoformat(),
            },
        )
        return {
            "schedule_date": schedule_date,
            "canonical_shifts": canonical_shifts,
            "image_count": len(images),
        }

    def persist_events_and_snapshot(inner_conn: Any, schema: str, session_id: str, pipeline_output: dict[str, Any]) -> list[dict[str, Any]]:
        schedule_date_value = pipeline_output["schedule_date"]
        canonical_shifts_value = pipeline_output["canonical_shifts"]
        if not isinstance(schedule_date_value, date):
            raise RuntimeError("Pipeline output is missing schedule_date.")
        if not isinstance(canonical_shifts_value, list) or not all(
            isinstance(item, CanonicalShift) for item in canonical_shifts_value
        ):
            raise RuntimeError("Pipeline output is missing canonical_shifts.")

        user_id = _load_session_user_id(inner_conn, schema, session_id)
        process_observation(
            inner_conn,
            schema,
            user_id=user_id,
            schedule_date=schedule_date_value,
            source_session_id=session_id,
            current_snapshot=canonical_shifts_value,
        )
        events = _load_session_events(inner_conn, schema, session_id)
        logger.info(
            "Events loaded for session",
            extra={
                "event": "worker.pipeline.events_loaded",
                "session_id": session_id,
                "event_count": len(events),
            },
        )
        return events

    def store_notifications_callback(inner_conn: Any, schema: str, _session_id: str, notifications: list[Any]) -> int:
        nonlocal stored_notification_count
        inserted = persist_notifications(inner_conn, schema, notifications=notifications)
        stored_notification_count += inserted
        return inserted

    processed = run_lifecycle_once(
        conn,
        config.db_schema,
        utc_now(),
        load_session_images=load_session_images,
        run_full_pipeline=run_full_pipeline,
        persist_events_and_snapshot=persist_events_and_snapshot,
        build_notifications=lambda events: build_notifications(
            events,
            summary_threshold=config.summary_threshold,
        ),
        store_notifications=store_notifications_callback,
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
        except Exception:
            logger.exception("Lifecycle iteration failed", extra={"event": "worker.iteration.error"})
        time.sleep(config.poll_seconds)


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
