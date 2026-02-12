#!/usr/bin/env python3
"""Phase 2 session finalization stub: write one schedule version and close one session."""

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
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
except ModuleNotFoundError:
    psycopg = None
    sql = None
    dict_row = None

STUB_SCHEDULE_DATE = date(2099, 1, 1)
STUB_VERSION = 1
DUMMY_PAYLOAD = {"stub": True}


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
class WorkerConfig:
    database_url: str
    db_schema: str


@dataclass(frozen=True)
class ProcessingSession:
    id: str
    user_id: int


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
    if psycopg is None:
        raise RuntimeError("Missing dependency: psycopg[binary]. Run `uv sync` and retry.")


def getenv_first(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def load_config() -> WorkerConfig:
    database_url = getenv_first("DATABASE_URL", "POSTGRES_DSN", "TEST_DATABASE_URL")
    if not database_url:
        raise RuntimeError("Missing required environment variable: DATABASE_URL")

    return WorkerConfig(
        database_url=database_url,
        db_schema=os.getenv("DB_SCHEMA", "schedule_ingest"),
    )


def make_payload_hash(payload: dict[str, Any]) -> str:
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def fetch_one_processing_session(conn: psycopg.Connection, schema: str) -> ProcessingSession | None:
    query = sql.SQL(
        """
        SELECT id::text AS id, user_id
        FROM {}.capture_session
        WHERE state = 'processing'
        ORDER BY created_at
        LIMIT 1
        """
    ).format(sql.Identifier(schema))

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query)
        row = cur.fetchone()

    if row is None:
        return None
    return ProcessingSession(id=row["id"], user_id=row["user_id"])


def insert_stub_schedule_version(
    conn: psycopg.Connection,
    schema: str,
    *,
    session: ProcessingSession,
    schedule_date: date,
    version: int,
    payload: dict[str, Any] | None,
    payload_hash: str,
) -> None:
    if version != 1:
        raise ValueError("Phase 2 only supports version=1.")
    if payload is None or not isinstance(payload, dict):
        raise ValueError("Payload must be a JSON object (dict).")

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
            raise RuntimeError(f"Expected one session transition to done, got {cur.rowcount}.")


def mark_session_failed(conn: psycopg.Connection, schema: str, session_id: str, error_text: str) -> None:
    query = sql.SQL(
        """
        UPDATE {}.capture_session
        SET state = 'failed', error = %s
        WHERE id = %s AND state = 'processing'
        """
    ).format(sql.Identifier(schema))

    with conn.cursor() as cur:
        cur.execute(query, (error_text, session_id))


def truncate_error(message: str, limit: int = 4000) -> str:
    if len(message) <= limit:
        return message
    return message[: limit - 3] + "..."


def process_one_session(
    *,
    conn: psycopg.Connection,
    config: WorkerConfig,
    session: ProcessingSession,
    logger: logging.Logger,
) -> None:
    payload_hash = make_payload_hash(DUMMY_PAYLOAD)

    insert_stub_schedule_version(
        conn,
        config.db_schema,
        session=session,
        schedule_date=STUB_SCHEDULE_DATE,
        version=STUB_VERSION,
        payload=DUMMY_PAYLOAD,
        payload_hash=payload_hash,
    )

    mark_session_done(conn, config.db_schema, session.id)

    logger.info(
        "Session finalized",
        extra={
            "event": "session.done",
            "session_id": session.id,
            "user_id": session.user_id,
            "schedule_date": STUB_SCHEDULE_DATE.isoformat(),
            "version": STUB_VERSION,
            "payload_hash": payload_hash,
        },
    )


def run_once(config: WorkerConfig, logger: logging.Logger) -> int:
    with psycopg.connect(config.database_url) as conn:
        session = fetch_one_processing_session(conn, config.db_schema)

        if session is None:
            logger.info(
                "No processing session found",
                extra={"event": "worker.no_session"},
            )
            return 0

        logger.info(
            "Picked processing session",
            extra={"event": "session.start", "session_id": session.id, "user_id": session.user_id},
        )

        try:
            with conn.transaction():
                process_one_session(conn=conn, config=config, session=session, logger=logger)
            return 1
        except Exception as exc:
            error_text = truncate_error(str(exc))

            logger.error(
                "Session finalization failed",
                extra={
                    "event": "session.failed",
                    "session_id": session.id,
                    "user_id": session.user_id,
                    "error": error_text,
                },
            )

            with conn.transaction():
                mark_session_failed(conn, config.db_schema, session.id, error_text)
            return 0


def main() -> int:
    logger = setup_logger()

    try:
        ensure_dependencies_installed()
        config = load_config()
    except Exception as exc:
        logger.error(
            "Configuration error",
            extra={"event": "worker.config_error", "error": str(exc)},
        )
        return 1

    logger.info(
        "Worker started",
        extra={
            "event": "worker.start",
            "db_schema": config.db_schema,
            "mode": "phase2_stub_finalization",
        },
    )

    try:
        processed = run_once(config, logger)
    except Exception:
        logger.exception("Worker crashed", extra={"event": "worker.crash"})
        return 1

    logger.info(
        "Worker finished",
        extra={"event": "worker.finish", "processed_sessions": processed},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
