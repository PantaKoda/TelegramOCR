#!/usr/bin/env python3
"""Phase 1 OCR worker skeleton: DB + R2 + stub schedule_version + state transitions."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

try:
    import boto3
except ModuleNotFoundError:
    boto3 = None

try:
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
except ModuleNotFoundError:
    psycopg = None
    sql = None
    dict_row = None

DUMMY_PAYLOAD = {"stub": True}


class JsonFormatter(logging.Formatter):
    """Structured JSON logger for deterministic, grep-friendly worker logs."""

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
class WorkerConfig:
    database_url: str
    db_schema: str
    r2_endpoint_url: str
    r2_bucket: str
    r2_access_key_id: str
    r2_secret_access_key: str
    r2_region: str
    keep_processing_on_failure: bool


@dataclass(frozen=True)
class ProcessingSession:
    id: str
    user_id: int
    created_at: datetime
    closed_at: datetime | None


@dataclass(frozen=True)
class CaptureImage:
    id: str
    sequence: int
    r2_key: str


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("ocr-worker")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def ensure_dependencies_installed() -> None:
    missing: list[str] = []
    if psycopg is None:
        missing.append("psycopg[binary]")
    if boto3 is None:
        missing.append("boto3")
    if missing:
        raise RuntimeError(
            "Missing dependencies: "
            + ", ".join(missing)
            + ". Run `uv sync` and retry."
        )


def getenv_first(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def parse_bool_env(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"Invalid boolean env value: {value}")


def load_config() -> WorkerConfig:
    database_url = getenv_first("DATABASE_URL", "POSTGRES_DSN", "TEST_DATABASE_URL")
    r2_endpoint_url = os.getenv("R2_ENDPOINT_URL")
    r2_bucket = os.getenv("R2_BUCKET")
    r2_access_key_id = os.getenv("R2_ACCESS_KEY_ID")
    r2_secret_access_key = os.getenv("R2_SECRET_ACCESS_KEY")

    missing = [
        name
        for name, value in [
            ("DATABASE_URL|POSTGRES_DSN|TEST_DATABASE_URL", database_url),
            ("R2_ENDPOINT_URL", r2_endpoint_url),
            ("R2_BUCKET", r2_bucket),
            ("R2_ACCESS_KEY_ID", r2_access_key_id),
            ("R2_SECRET_ACCESS_KEY", r2_secret_access_key),
        ]
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    return WorkerConfig(
        database_url=database_url,
        db_schema=os.getenv("DB_SCHEMA", "schedule_ingest"),
        r2_endpoint_url=r2_endpoint_url,
        r2_bucket=r2_bucket,
        r2_access_key_id=r2_access_key_id,
        r2_secret_access_key=r2_secret_access_key,
        r2_region=os.getenv("R2_REGION", "auto"),
        keep_processing_on_failure=parse_bool_env(
            os.getenv("KEEP_PROCESSING_ON_FAILURE"),
            default=False,
        ),
    )


def make_payload_hash(payload: dict[str, Any]) -> str:
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def resolve_stub_schedule_date(session: ProcessingSession) -> date:
    anchor = session.closed_at or session.created_at
    return anchor.date()


def create_r2_client(config: WorkerConfig):
    return boto3.client(
        "s3",
        endpoint_url=config.r2_endpoint_url,
        aws_access_key_id=config.r2_access_key_id,
        aws_secret_access_key=config.r2_secret_access_key,
        region_name=config.r2_region,
    )


def fetch_processing_sessions(conn: psycopg.Connection, schema: str) -> list[ProcessingSession]:
    query = sql.SQL(
        """
        SELECT id::text AS id, user_id, created_at, closed_at
        FROM {}.capture_session
        WHERE state = 'processing'
        ORDER BY created_at
        """
    ).format(sql.Identifier(schema))

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query)
        rows = cur.fetchall()

    return [
        ProcessingSession(
            id=row["id"],
            user_id=row["user_id"],
            created_at=row["created_at"],
            closed_at=row["closed_at"],
        )
        for row in rows
    ]


def fetch_session_images(conn: psycopg.Connection, schema: str, session_id: str) -> list[CaptureImage]:
    query = sql.SQL(
        """
        SELECT id::text AS id, sequence, r2_key
        FROM {}.capture_image
        WHERE session_id = %s
        ORDER BY sequence
        """
    ).format(sql.Identifier(schema))

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, (session_id,))
        rows = cur.fetchall()

    return [CaptureImage(id=row["id"], sequence=row["sequence"], r2_key=row["r2_key"]) for row in rows]


def download_session_images(
    *,
    r2_client: Any,
    bucket: str,
    session: ProcessingSession,
    images: list[CaptureImage],
    logger: logging.Logger,
) -> None:
    for image in images:
        response = r2_client.get_object(Bucket=bucket, Key=image.r2_key)
        body = response["Body"]
        content = body.read()
        body.close()

        logger.info(
            "Downloaded capture image",
            extra={
                "event": "r2.image_downloaded",
                "session_id": session.id,
                "capture_image_id": image.id,
                "sequence": image.sequence,
                "r2_key": image.r2_key,
                "bytes": len(content),
            },
        )


def compute_next_version(
    conn: psycopg.Connection,
    schema: str,
    *,
    user_id: int,
    schedule_date: date,
) -> int:
    query = sql.SQL(
        """
        SELECT current_version
        FROM {}.day_schedule
        WHERE user_id = %s AND schedule_date = %s
        FOR UPDATE
        """
    ).format(sql.Identifier(schema))

    with conn.cursor() as cur:
        cur.execute(query, (user_id, schedule_date))
        row = cur.fetchone()

    return 1 if row is None else int(row[0]) + 1


def insert_stub_schedule_version(
    conn: psycopg.Connection,
    schema: str,
    *,
    session: ProcessingSession,
    schedule_date: date,
    version: int,
    payload: dict[str, Any],
    payload_hash: str,
) -> None:
    query = sql.SQL(
        """
        INSERT INTO {}.schedule_version (
            user_id,
            schedule_date,
            version,
            session_id,
            payload,
            payload_hash
        ) VALUES (%s, %s, %s, %s, %s::jsonb, %s)
        """
    ).format(sql.Identifier(schema))

    with conn.cursor() as cur:
        cur.execute(
            query,
            (
                session.user_id,
                schedule_date,
                version,
                session.id,
                json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
                payload_hash,
            ),
        )


def mark_session_done(conn: psycopg.Connection, schema: str, session_id: str) -> None:
    query = sql.SQL(
        """
        UPDATE {}.capture_session
        SET state = 'done', error = NULL
        WHERE id = %s AND state = 'processing'
        """
    ).format(sql.Identifier(schema))

    with conn.cursor() as cur:
        cur.execute(query, (session_id,))
        if cur.rowcount != 1:
            raise RuntimeError(
                f"Expected to transition exactly one session to done; updated {cur.rowcount}."
            )


def mark_session_failed(conn: psycopg.Connection, schema: str, session_id: str, error_text: str) -> bool:
    query = sql.SQL(
        """
        UPDATE {}.capture_session
        SET state = 'failed', error = %s
        WHERE id = %s AND state = 'processing'
        """
    ).format(sql.Identifier(schema))

    with conn.cursor() as cur:
        cur.execute(query, (error_text, session_id))
        return cur.rowcount == 1


def truncate_error(message: str, limit: int = 4000) -> str:
    if len(message) <= limit:
        return message
    return message[: limit - 3] + "..."


def process_session(
    *,
    conn: psycopg.Connection,
    config: WorkerConfig,
    session: ProcessingSession,
    r2_client: Any,
    logger: logging.Logger,
) -> None:
    images = fetch_session_images(conn, config.db_schema, session.id)
    if not images:
        raise RuntimeError("No capture_image rows found for processing session.")

    logger.info(
        "Loaded session images",
        extra={
            "event": "session.images_loaded",
            "session_id": session.id,
            "image_count": len(images),
        },
    )

    download_session_images(
        r2_client=r2_client,
        bucket=config.r2_bucket,
        session=session,
        images=images,
        logger=logger,
    )

    schedule_date = resolve_stub_schedule_date(session)
    payload_hash = make_payload_hash(DUMMY_PAYLOAD)
    version = compute_next_version(
        conn,
        config.db_schema,
        user_id=session.user_id,
        schedule_date=schedule_date,
    )

    insert_stub_schedule_version(
        conn,
        config.db_schema,
        session=session,
        schedule_date=schedule_date,
        version=version,
        payload=DUMMY_PAYLOAD,
        payload_hash=payload_hash,
    )

    mark_session_done(conn, config.db_schema, session.id)

    logger.info(
        "Session processed successfully",
        extra={
            "event": "session.done",
            "session_id": session.id,
            "user_id": session.user_id,
            "schedule_date": schedule_date.isoformat(),
            "version": version,
            "payload_hash": payload_hash,
        },
    )


def run_once(config: WorkerConfig, logger: logging.Logger) -> int:
    processed = 0

    with psycopg.connect(config.database_url) as conn:
        sessions = fetch_processing_sessions(conn, config.db_schema)

        logger.info(
            "Fetched processing sessions",
            extra={
                "event": "worker.sessions_fetched",
                "count": len(sessions),
            },
        )

        if not sessions:
            return 0

        r2_client = create_r2_client(config)

        for session in sessions:
            logger.info(
                "Starting session",
                extra={
                    "event": "session.start",
                    "session_id": session.id,
                    "user_id": session.user_id,
                },
            )

            try:
                with conn.transaction():
                    process_session(
                        conn=conn,
                        config=config,
                        session=session,
                        r2_client=r2_client,
                        logger=logger,
                    )
                processed += 1
            except Exception as exc:
                error_text = truncate_error(str(exc))

                logger.error(
                    "Session failed",
                    extra={
                        "event": "session.failed",
                        "session_id": session.id,
                        "user_id": session.user_id,
                        "error": error_text,
                    },
                )

                if config.keep_processing_on_failure:
                    logger.warning(
                        "Leaving failed session in processing state by configuration",
                        extra={
                            "event": "session.failed_left_processing",
                            "session_id": session.id,
                            "user_id": session.user_id,
                        },
                    )
                    continue

                try:
                    with conn.transaction():
                        updated = mark_session_failed(
                            conn,
                            config.db_schema,
                            session.id,
                            error_text,
                        )
                    logger.info(
                        "Failure state transition applied",
                        extra={
                            "event": "session.failed_transition",
                            "session_id": session.id,
                            "updated": updated,
                        },
                    )
                except Exception:
                    logger.exception(
                        "Failed to transition session to failed",
                        extra={
                            "event": "session.failed_transition_error",
                            "session_id": session.id,
                        },
                    )

    return processed


def main() -> int:
    logger = setup_logger()

    try:
        ensure_dependencies_installed()
        config = load_config()
    except Exception as exc:
        logger.error(
            "Configuration error",
            extra={
                "event": "worker.config_error",
                "error": str(exc),
            },
        )
        return 1

    logger.info(
        "Worker started",
        extra={
            "event": "worker.start",
            "db_schema": config.db_schema,
            "r2_bucket": config.r2_bucket,
            "keep_processing_on_failure": config.keep_processing_on_failure,
        },
    )

    try:
        processed = run_once(config, logger)
    except Exception:
        logger.exception("Worker crashed", extra={"event": "worker.crash"})
        return 1

    logger.info(
        "Worker finished",
        extra={
            "event": "worker.finish",
            "processed_sessions": processed,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
