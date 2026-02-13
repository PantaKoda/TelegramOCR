"""Continuously running background worker loop for session processing."""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import tempfile
import time
import unicodedata
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from domain import schedule_diff
from domain.notification_rules import UserNotification, build_notifications
from domain.session_aggregate import aggregate_session_shifts
from domain.session_lifecycle import (
    SessionLifecycleConfig,
    load_lifecycle_config_from_env,
    run_lifecycle_once,
    utc_now,
)
from infra.event_store import load_day_snapshot, process_observation
from infra.notification_store import persist_notifications
from parser.layout_parser import parse_layout
from parser.semantic_normalizer import CanonicalShift, normalize_entries

DEFAULT_FIXTURE_PAYLOAD_PATH = "fixtures/sample_schedule.json"
SERVICE_NAME = "python-worker"
INPUT_MODE_FIXTURE = "fixture"
INPUT_MODE_OCR = "ocr"

DATE_WITH_WEEKDAY_RE = re.compile(r"\b([A-Za-zÅÄÖåäö]+)\s+(\d{1,2})\s+([A-Za-zÅÄÖåäö]+)(?:\s+(\d{4}))?\b")
DATE_DAY_MONTH_RE = re.compile(r"\b(\d{1,2})\s+([A-Za-zÅÄÖåäö]+)(?:\s+(\d{4}))?\b")

WEEKDAY_NAMES = {
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
    "mandag",
    "tisdag",
    "onsdag",
    "torsdag",
    "fredag",
    "lordag",
    "sondag",
}

MONTH_MAP = {
    "jan": 1,
    "january": 1,
    "januari": 1,
    "feb": 2,
    "february": 2,
    "februari": 2,
    "mar": 3,
    "march": 3,
    "mars": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "maj": 5,
    "jun": 6,
    "june": 6,
    "juni": 6,
    "jul": 7,
    "july": 7,
    "juli": 7,
    "aug": 8,
    "august": 8,
    "augusti": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "okt": 10,
    "oktober": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


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
    input_mode: str
    ocr_default_year: int | None
    r2_config: "R2Config | None"
    idle_log_every: int = 12


@dataclass(frozen=True)
class R2Config:
    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    bucket: str
    region: str
    key_prefix: str


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
    idle_log_every = _parse_positive_int_env("WORKER_IDLE_LOG_EVERY", 12)
    input_mode = _parse_input_mode(os.getenv("WORKER_INPUT_MODE", INPUT_MODE_FIXTURE))
    ocr_default_year = _parse_optional_int_env("OCR_DEFAULT_YEAR")
    r2_config = _load_r2_config() if input_mode == INPUT_MODE_OCR else None

    return WorkerRuntimeConfig(
        database_url=database_url,
        db_schema=os.getenv("DB_SCHEMA", "schedule_ingest"),
        poll_seconds=poll_seconds,
        fixture_payload_path=os.getenv("FIXTURE_PAYLOAD_PATH", DEFAULT_FIXTURE_PAYLOAD_PATH),
        summary_threshold=summary_threshold,
        input_mode=input_mode,
        ocr_default_year=ocr_default_year,
        r2_config=r2_config,
        idle_log_every=idle_log_every,
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
    ocr_client: Any | None = None
    r2_client: Any | None = None
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
        image_names = _extract_image_names(rows)
        context["image_names"] = image_names
        logger.info(
            "Session images loaded",
            extra={
                "event": "session.images_loaded",
                "session_id": session_id,
                "user_id": context["user_id"],
                "correlation_id": context["correlation_id"],
                "image_count": len(rows),
                "image_names": list(image_names),
            },
        )
        return rows

    def run_full_pipeline(images: list[dict[str, Any]]) -> dict[str, Any]:
        nonlocal ocr_client
        nonlocal r2_client
        session_id = str(images[0].get("session_id", "")) if images else ""
        context = session_context.get(session_id, {"user_id": None, "correlation_id": session_id or None})
        if config.input_mode == INPUT_MODE_FIXTURE:
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
                    "mode": INPUT_MODE_FIXTURE,
                },
            )

            try:
                schedule_date = _parse_schedule_date(fixture_payload)
                canonical_shifts = normalize_entries(entries)
                canonical_shifts.sort(key=_canonical_shift_sort_key)
            except Exception as error:
                raise WorkerStageError("layout", "Failed layout/semantic normalization.", cause=error) from error
        else:
            if config.r2_config is None:
                raise WorkerStageError("ocr", "WORKER_INPUT_MODE=ocr requires R2 configuration.")
            try:
                from ocr.paddle_adapter import create_paddle_ocr, run_paddle_on_image
            except Exception as error:
                raise WorkerStageError("ocr", "Failed importing PaddleOCR adapter dependencies.", cause=error) from error
            if ocr_client is None:
                try:
                    ocr_client = create_paddle_ocr()
                except Exception as error:
                    raise WorkerStageError("ocr", "Failed creating PaddleOCR client.", cause=error) from error
            if r2_client is None:
                try:
                    r2_client = _create_r2_client(config.r2_config)
                except Exception as error:
                    raise WorkerStageError("ocr", "Failed creating R2 client.", cause=error) from error

            image_shifts: list[list[CanonicalShift]] = []
            image_dates: list[date] = []
            total_boxes = 0

            for image in images:
                key = str(image.get("r2_key", "") or "")
                if not key:
                    raise WorkerStageError("ocr", f"Session {session_id} image is missing r2_key.")
                try:
                    image_bytes = _download_r2_object(r2_client, config.r2_config, key)
                except Exception as error:
                    raise WorkerStageError("ocr", f"Failed downloading R2 object: {key}", cause=error) from error

                suffix = Path(key).suffix or ".png"
                try:
                    with tempfile.NamedTemporaryFile(suffix=suffix) as temp_image:
                        temp_image.write(image_bytes)
                        temp_image.flush()
                        boxes = run_paddle_on_image(temp_image.name, ocr=ocr_client)
                except Exception as error:
                    raise WorkerStageError("ocr", f"Failed OCR on image: {key}", cause=error) from error

                total_boxes += len(boxes)
                try:
                    image_date = _extract_schedule_date_from_boxes(boxes, default_year=config.ocr_default_year)
                    layout_entries = parse_layout(boxes)
                    canonical = normalize_entries(layout_entries)
                    canonical.sort(key=_canonical_shift_sort_key)
                except Exception as error:
                    raise WorkerStageError("layout", f"Failed layout parsing for image: {key}", cause=error) from error

                image_dates.append(image_date)
                image_shifts.append(canonical)

            schedule_date = _ensure_single_schedule_date(image_dates)
            aggregated = aggregate_session_shifts(image_shifts, schedule_date=schedule_date.isoformat())
            canonical_shifts = [item.shift for item in aggregated.shifts]
            canonical_shifts.sort(key=_canonical_shift_sort_key)

            logger.info(
                "OCR stage completed",
                extra={
                    "event": "ocr.completed",
                    "session_id": session_id or None,
                    "user_id": context["user_id"],
                    "correlation_id": context["correlation_id"],
                    "text_block_count": total_boxes,
                    "image_count": len(images),
                    "mode": INPUT_MODE_OCR,
                },
            )

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
        image_names = tuple(str(value) for value in context.get("image_names", ()))
        notifications = _with_source_image_labels(notifications, image_names)
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
                "image_names": list(image_names),
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

    failed_session_count = 0

    def on_session_failed(session_id: str, error: Exception, marked_failed: bool) -> None:
        nonlocal failed_session_count
        failed_session_count += 1
        context = _ensure_session_context(conn, config.db_schema, session_id, session_context)
        stage = error.stage if isinstance(error, WorkerStageError) else "lifecycle"
        logger.error(
            "Session processing failed",
            extra={
                "event": "session.failed",
                "session_id": session_id,
                "user_id": context["user_id"],
                "correlation_id": context["correlation_id"],
                "error.type": type(error).__name__,
                "error.message": str(error),
                "error.stage": stage,
                "marked_failed": marked_failed,
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
        on_session_failed=on_session_failed,
        config=lifecycle_config,
    )

    notification_count = sum(len(item[1]) for item in processed)
    return {
        "processed_sessions": len(processed),
        "failed_sessions": failed_session_count,
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
            "idle_log_every": config.idle_log_every,
            "input_mode": config.input_mode,
            "ocr_default_year": config.ocr_default_year,
            "open_state": lifecycle_config.open_state,
            "processing_state": lifecycle_config.processing_state,
            "processed_state": lifecycle_config.processed_state,
            "failed_state": lifecycle_config.failed_state,
        },
    )

    idle_iteration_streak = 0
    while True:
        logger.debug("Lifecycle iteration started", extra={"event": "worker.iteration.start"})
        try:
            with psycopg.connect(config.database_url) as conn:
                with conn.transaction():
                    result = run_iteration(conn, config, lifecycle_config, logger=logger)
            has_activity = (
                result["processed_sessions"] > 0
                or result["failed_sessions"] > 0
                or result["generated_notifications"] > 0
                or result["stored_notifications"] > 0
            )
            if has_activity:
                idle_iteration_streak = 0
                logger.info(
                    "Lifecycle iteration finished",
                    extra={
                        "event": "worker.iteration.finish",
                        "processed_sessions": result["processed_sessions"],
                        "failed_sessions": result["failed_sessions"],
                        "generated_notifications": result["generated_notifications"],
                        "stored_notifications": result["stored_notifications"],
                    },
                )
            else:
                idle_iteration_streak += 1
                if _should_log_idle_iteration(idle_iteration_streak, config.idle_log_every):
                    logger.info(
                        "Lifecycle iteration idle",
                        extra={
                            "event": "worker.iteration.idle",
                            "idle_iteration_streak": idle_iteration_streak,
                            "poll_seconds": config.poll_seconds,
                        },
                    )
        except Exception as error:
            idle_iteration_streak = 0
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


def _parse_optional_int_env(name: str) -> int | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return None
    try:
        return int(value)
    except ValueError as error:
        raise RuntimeError(f"{name} must be an integer.") from error


def _parse_input_mode(value: str) -> str:
    normalized = value.strip().lower()
    if normalized in {INPUT_MODE_FIXTURE, INPUT_MODE_OCR}:
        return normalized
    raise RuntimeError(f"WORKER_INPUT_MODE must be one of: {INPUT_MODE_FIXTURE}, {INPUT_MODE_OCR}.")


def _load_r2_config() -> R2Config:
    endpoint_url = _get_required_env("R2_ENDPOINT_URL", "S3_ENDPOINT_URL")
    access_key_id = _get_required_env("R2_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID")
    secret_access_key = _get_required_env("R2_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY")
    bucket = _get_required_env("R2_BUCKET", "R2_BUCKET_NAME", "S3_BUCKET")
    region = os.getenv("R2_REGION", "auto")
    key_prefix = os.getenv("R2_KEY_PREFIX", "")
    return R2Config(
        endpoint_url=endpoint_url,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        bucket=bucket,
        region=region,
        key_prefix=key_prefix,
    )


def _get_required_env(*names: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value.strip()
    joined = ", ".join(names)
    raise RuntimeError(f"Missing required environment variable (one of: {joined}).")


def _create_r2_client(config: R2Config) -> Any:
    try:
        import boto3  # type: ignore
    except ModuleNotFoundError as error:
        raise RuntimeError("Missing dependency `boto3`. Run `uv sync` and rebuild the worker image.") from error

    return boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
        region_name=config.region,
    )


def _download_r2_object(client: Any, config: R2Config, key: str) -> bytes:
    resolved_key = _resolve_r2_key(key, config.key_prefix)
    response = client.get_object(Bucket=config.bucket, Key=resolved_key)
    body = response.get("Body")
    if body is None:
        raise RuntimeError(f"R2 object body missing for key: {resolved_key}")
    return bytes(body.read())


def _resolve_r2_key(key: str, key_prefix: str) -> str:
    normalized_key = key.strip().lstrip("/")
    if not key_prefix:
        return normalized_key
    prefix = key_prefix.strip().strip("/")
    if not prefix:
        return normalized_key
    if normalized_key.startswith(prefix + "/") or normalized_key == prefix:
        return normalized_key
    return f"{prefix}/{normalized_key}"


def _extract_schedule_date_from_boxes(boxes: list[Any], *, default_year: int | None) -> date:
    texts = _extract_date_candidate_texts(boxes)
    for text in texts:
        parsed = _parse_schedule_date_from_text(text, default_year=default_year)
        if parsed is not None:
            return parsed
    raise RuntimeError("Could not resolve schedule date from OCR UI text.")


def _extract_date_candidate_texts(boxes: list[Any]) -> list[str]:
    normalized_boxes: list[dict[str, Any]] = []
    for box in boxes:
        text = str(getattr(box, "text", ""))
        cleaned = " ".join(text.split())
        if not cleaned:
            continue
        try:
            x = float(getattr(box, "x", 0.0))
            y = float(getattr(box, "y", 0.0))
            h = float(getattr(box, "h", 0.0))
        except (TypeError, ValueError):
            x = 0.0
            y = 0.0
            h = 0.0
        normalized_boxes.append({"text": cleaned, "x": x, "y": y, "h": max(h, 1.0)})

    if not normalized_boxes:
        return []

    normalized_boxes.sort(key=lambda item: (item["y"], item["x"]))
    candidates = [item["text"] for item in normalized_boxes]

    # Build line candidates so split OCR tokens on one date line can still be parsed.
    line_threshold = max(8.0, median(item["h"] for item in normalized_boxes) * 0.6)
    current_line: list[dict[str, Any]] = []
    current_center = 0.0
    line_texts: list[str] = []
    for item in normalized_boxes:
        center = item["y"] + (item["h"] / 2.0)
        if not current_line:
            current_line = [item]
            current_center = center
            continue
        if abs(center - current_center) <= line_threshold:
            current_line.append(item)
            current_center = (current_center * (len(current_line) - 1) + center) / len(current_line)
            continue
        line_texts.append(" ".join(part["text"] for part in sorted(current_line, key=lambda value: value["x"])))
        current_line = [item]
        current_center = center
    if current_line:
        line_texts.append(" ".join(part["text"] for part in sorted(current_line, key=lambda value: value["x"])))

    return [*line_texts, *candidates]


def _parse_schedule_date_from_text(text: str, *, default_year: int | None) -> date | None:
    for match in DATE_WITH_WEEKDAY_RE.finditer(text):
        weekday_token = _normalize_date_token(match.group(1))
        if weekday_token not in WEEKDAY_NAMES:
            continue
        resolved = _build_date_from_parts(match.group(2), match.group(3), match.group(4), default_year=default_year)
        if resolved is not None:
            return resolved

    for match in DATE_DAY_MONTH_RE.finditer(text):
        resolved = _build_date_from_parts(match.group(1), match.group(2), match.group(3), default_year=default_year)
        if resolved is not None:
            return resolved
    return None


def _build_date_from_parts(day_value: str, month_value: str, year_value: str | None, *, default_year: int | None) -> date | None:
    month_key = _normalize_date_token(month_value)
    month = MONTH_MAP.get(month_key)
    if month is None:
        return None
    try:
        day = int(day_value)
    except ValueError:
        return None
    try:
        year = default_year if year_value is None else int(year_value)
    except ValueError:
        return None
    if year is None:
        raise RuntimeError("Date text is missing year and OCR_DEFAULT_YEAR is not configured.")
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _normalize_date_token(value: str) -> str:
    collapsed = " ".join(value.split())
    normalized = unicodedata.normalize("NFKD", collapsed)
    without_marks = "".join(char for char in normalized if not unicodedata.combining(char))
    return without_marks.lower()


def _ensure_single_schedule_date(values: list[date]) -> date:
    unique = sorted(set(values))
    if not unique:
        raise RuntimeError("No schedule date detected from OCR output.")
    if len(unique) > 1:
        rendered = ", ".join(value.isoformat() for value in unique)
        raise RuntimeError(f"Inconsistent schedule dates detected across session images: {rendered}")
    return unique[0]


def _should_log_idle_iteration(idle_iteration_streak: int, idle_log_every: int) -> bool:
    if idle_iteration_streak <= 0:
        return False
    if idle_iteration_streak == 1:
        return True
    return idle_iteration_streak % idle_log_every == 0


def _extract_image_names(image_rows: list[dict[str, Any]]) -> tuple[str, ...]:
    names: list[str] = []
    seen: set[str] = set()
    for row in image_rows:
        key = str(row.get("r2_key", "") or "")
        name = Path(key).name if key else ""
        if not name:
            sequence = row.get("sequence")
            name = f"sequence-{sequence}" if sequence is not None else "unknown-image"
        if name in seen:
            continue
        seen.add(name)
        names.append(name)
    return tuple(names)


def _with_source_image_labels(notifications: list[Any], image_names: tuple[str, ...]) -> list[Any]:
    if not notifications or not image_names:
        return notifications
    label = "image" if len(image_names) == 1 else "images"
    suffix = f" ({label}: {', '.join(image_names)})"
    annotated: list[Any] = []
    for notification in notifications:
        if isinstance(notification, UserNotification):
            message = notification.message
            if not message.endswith(suffix):
                message = f"{message}{suffix}"
            annotated.append(replace(notification, message=message))
            continue
        if isinstance(notification, dict):
            updated = dict(notification)
            message = str(updated.get("message", ""))
            if not message.endswith(suffix):
                updated["message"] = f"{message}{suffix}"
            annotated.append(updated)
            continue
        annotated.append(notification)
    return annotated


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
