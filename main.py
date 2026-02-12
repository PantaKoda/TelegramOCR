#!/usr/bin/env python3
"""Session finalization worker with lease-based claiming and ownership-guarded finalization."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

try:
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
except ModuleNotFoundError:
    psycopg = None
    sql = None
    dict_row = None

DEFAULT_FIXTURE_PAYLOAD_PATH = "fixtures/sample_schedule.json"


class LeaseLostError(RuntimeError):
    """Raised when worker no longer owns lease for the session."""


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
    fixture_payload_path: str
    enable_chaos_parser: bool
    chaos_seed: int
    worker_id: str
    pending_state: str
    processing_state: str
    done_state: str
    failed_state: str
    lease_timeout_seconds: int
    lease_heartbeat_seconds: float
    simulated_work_seconds: float
    enable_lease_heartbeat: bool


@dataclass(frozen=True)
class ClaimedSession:
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


def parse_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise RuntimeError(f"Invalid integer for {name}: {value}") from exc
    if parsed <= 0:
        raise RuntimeError(f"{name} must be > 0.")
    return parsed


def parse_positive_float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed = float(value)
    except ValueError as exc:
        raise RuntimeError(f"Invalid float for {name}: {value}") from exc
    if parsed <= 0:
        raise RuntimeError(f"{name} must be > 0.")
    return parsed


def parse_non_negative_float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed = float(value)
    except ValueError as exc:
        raise RuntimeError(f"Invalid float for {name}: {value}") from exc
    if parsed < 0:
        raise RuntimeError(f"{name} must be >= 0.")
    return parsed


def parse_bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"Invalid boolean for {name}: {value}")


def parse_any_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"Invalid integer for {name}: {value}") from exc


def load_config() -> WorkerConfig:
    database_url = getenv_first("DATABASE_URL", "POSTGRES_DSN", "TEST_DATABASE_URL")
    if not database_url:
        raise RuntimeError("Missing required environment variable: DATABASE_URL")

    config = WorkerConfig(
        database_url=database_url,
        db_schema=os.getenv("DB_SCHEMA", "schedule_ingest"),
        fixture_payload_path=os.getenv("FIXTURE_PAYLOAD_PATH", DEFAULT_FIXTURE_PAYLOAD_PATH),
        enable_chaos_parser=parse_bool_env("ENABLE_CHAOS_PARSER", False),
        chaos_seed=parse_any_int_env("CHAOS_SEED", 0),
        worker_id=os.getenv("WORKER_ID", f"worker-{os.getpid()}"),
        pending_state=os.getenv("PENDING_STATE", "pending"),
        processing_state=os.getenv("PROCESSING_STATE", "processing"),
        done_state=os.getenv("DONE_STATE", "done"),
        failed_state=os.getenv("FAILED_STATE", "failed"),
        lease_timeout_seconds=parse_int_env("LEASE_TIMEOUT_SECONDS", 300),
        lease_heartbeat_seconds=parse_positive_float_env("LEASE_HEARTBEAT_SECONDS", 10.0),
        simulated_work_seconds=parse_non_negative_float_env("SIMULATED_WORK_SECONDS", 0.0),
        enable_lease_heartbeat=parse_bool_env("ENABLE_LEASE_HEARTBEAT", True),
    )
    if config.enable_lease_heartbeat and config.lease_heartbeat_seconds >= (config.lease_timeout_seconds / 3):
        raise RuntimeError(
            "Unsafe lease settings: LEASE_HEARTBEAT_SECONDS must be < LEASE_TIMEOUT_SECONDS / 3."
        )
    return config


def make_payload_hash(payload: dict[str, Any]) -> str:
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def load_fixture_payload(path: str) -> dict[str, Any]:
    payload_path = Path(path)
    if not payload_path.exists():
        raise RuntimeError(f"Fixture payload file not found: {payload_path}")

    try:
        raw = json.loads(payload_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Fixture payload is not valid JSON: {payload_path}") from exc

    if not isinstance(raw, dict):
        raise RuntimeError("Fixture payload must be a JSON object.")
    return raw


def parse_schedule_date(payload: dict[str, Any]) -> date:
    value = payload.get("schedule_date")
    if not isinstance(value, str):
        raise RuntimeError("Fixture payload must include `schedule_date` as ISO date string.")

    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise RuntimeError(f"Invalid schedule_date in fixture payload: {value}") from exc


def collapse_whitespace(value: str) -> str:
    return " ".join(value.split())


def normalize_text_field(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f"Entry field `{field_name}` must be a string.")
    normalized = collapse_whitespace(value)
    if not normalized:
        raise RuntimeError(f"Entry field `{field_name}` cannot be empty.")
    return normalized


def normalize_time_value(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f"Entry field `{field_name}` must be a string time.")

    match = re.fullmatch(r"\s*(\d{1,2})\s*[:.\s]\s*(\d{1,2})\s*", value)
    if match is None:
        raise RuntimeError(f"Invalid time format for `{field_name}`: {value}")

    hour = int(match.group(1))
    minute = int(match.group(2))
    if hour < 0 or hour > 23:
        raise RuntimeError(f"Invalid hour for `{field_name}`: {value}")
    if minute < 0 or minute > 59:
        raise RuntimeError(f"Invalid minute for `{field_name}`: {value}")
    return f"{hour:02d}:{minute:02d}"


def canonicalize_entry(entry: Any) -> dict[str, str]:
    if not isinstance(entry, dict):
        raise RuntimeError("Each `entries` item must be an object.")

    title = normalize_text_field(entry.get("title"), "title")
    location = normalize_text_field(entry.get("location"), "location")

    return {
        "start": normalize_time_value(entry.get("start"), "start"),
        "end": normalize_time_value(entry.get("end"), "end"),
        "title": title,
        "location": location.lower().title(),
    }


def normalize_schedule_payload(payload: dict[str, Any]) -> dict[str, Any]:
    schedule_date = parse_schedule_date(payload).isoformat()
    raw_entries = payload.get("entries")
    if not isinstance(raw_entries, list):
        raise RuntimeError("Fixture payload must include `entries` as a list.")

    normalized_entries = [canonicalize_entry(entry) for entry in raw_entries]
    normalized_entries.sort(key=lambda item: (item["start"], item["end"], item["title"].casefold(), item["location"].casefold()))

    return {
        "schedule_date": schedule_date,
        "entries": normalized_entries,
    }


def noisy_time_format(canonical_time: str, rng: random.Random) -> str:
    hour_str, minute_str = canonical_time.split(":")
    hour = int(hour_str)
    minute = int(minute_str)

    variants = [
        f"{hour:02d}:{minute:02d}",
        f"{hour:02d}.{minute:02d}",
        f"{hour:02d} {minute:02d}",
        f"{hour}:{minute:02d}",
        f"{hour:02d}:{minute}",
    ]
    return rng.choice(variants)


def noisy_title(value: str, rng: random.Random) -> str:
    parts = value.split(" ")
    joiner = "  " if rng.random() < 0.5 else " "
    noisy = joiner.join(parts)
    if rng.random() < 0.7:
        noisy = noisy + (" " * (1 + rng.randint(0, 1)))
    if rng.random() < 0.3:
        noisy = " " + noisy
    return noisy


def noisy_location(value: str, rng: random.Random) -> str:
    case_variant = rng.choice([str.lower, str.upper, str.title])
    base = case_variant(value)
    parts = base.split(" ")
    joiner = "  " if rng.random() < 0.6 else " "
    noisy = joiner.join(parts)
    if rng.random() < 0.5:
        noisy = noisy + " "
    return noisy


def apply_chaos_parser(payload: dict[str, Any], seed: int) -> dict[str, Any]:
    canonical = normalize_schedule_payload(payload)
    rng = random.Random(seed)

    noisy_entries: list[dict[str, str]] = []
    for entry in canonical["entries"]:
        noisy_entries.append(
            {
                "start": noisy_time_format(entry["start"], rng),
                "end": noisy_time_format(entry["end"], rng),
                "title": noisy_title(entry["title"], rng),
                "location": noisy_location(entry["location"], rng),
            }
        )

    rng.shuffle(noisy_entries)
    return {
        "schedule_date": canonical["schedule_date"],
        "entries": noisy_entries,
    }


def capture_session_state_type(schema: str):
    return sql.Identifier(schema, "capture_session_state")


def claim_one_session(conn: psycopg.Connection, config: WorkerConfig) -> ClaimedSession | None:
    state_type = capture_session_state_type(config.db_schema)
    query = sql.SQL(
        """
        WITH candidate AS (
            SELECT id
            FROM {}.capture_session
            WHERE state = %s::{}
               OR (
                    state = %s::{}
                    AND (
                        locked_at IS NULL
                        OR locked_at < NOW() - make_interval(secs => %s)
                    )
               )
            ORDER BY
                CASE WHEN state = %s::{} THEN 0 ELSE 1 END,
                created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE {}.capture_session AS cs
        SET state = %s::{},
            locked_at = NOW(),
            locked_by = %s,
            error = NULL
        FROM candidate
        WHERE cs.id = candidate.id
        RETURNING cs.id::text AS id, cs.user_id
        """
    ).format(
        sql.Identifier(config.db_schema),
        state_type,
        state_type,
        state_type,
        sql.Identifier(config.db_schema),
        state_type,
    )

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            query,
            (
                config.pending_state,
                config.processing_state,
                config.lease_timeout_seconds,
                config.pending_state,
                config.processing_state,
                config.worker_id,
            ),
        )
        row = cur.fetchone()

    if row is None:
        return None
    return ClaimedSession(id=row["id"], user_id=row["user_id"])


def refresh_lease(conn: psycopg.Connection, config: WorkerConfig, session_id: str) -> None:
    state_type = capture_session_state_type(config.db_schema)
    query = sql.SQL(
        """
        UPDATE {}.capture_session
        SET locked_at = NOW()
        WHERE id = %s
          AND state = %s::{}
          AND locked_by = %s
        """
    ).format(sql.Identifier(config.db_schema), state_type)

    with conn.cursor() as cur:
        cur.execute(query, (session_id, config.processing_state, config.worker_id))
        if cur.rowcount != 1:
            raise LeaseLostError("Lease no longer owned; heartbeat failed.")


def get_next_schedule_version(
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
        WHERE user_id = %s
          AND schedule_date = %s
        FOR UPDATE
        """
    ).format(sql.Identifier(schema))

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, (user_id, schedule_date))
        row = cur.fetchone()

    if row is None:
        return 1
    return int(row["current_version"]) + 1


def get_latest_schedule_version(
    conn: psycopg.Connection,
    schema: str,
    *,
    user_id: int,
    schedule_date: date,
) -> dict[str, Any] | None:
    query = sql.SQL(
        """
        SELECT version, payload_hash
        FROM {}.schedule_version
        WHERE user_id = %s
          AND schedule_date = %s
        ORDER BY version DESC
        LIMIT 1
        FOR UPDATE
        """
    ).format(sql.Identifier(schema))

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, (user_id, schedule_date))
        return cur.fetchone()


def get_schedule_version_by_hash(
    conn: psycopg.Connection,
    schema: str,
    *,
    user_id: int,
    schedule_date: date,
    payload_hash: str,
) -> dict[str, Any] | None:
    query = sql.SQL(
        """
        SELECT version, payload_hash
        FROM {}.schedule_version
        WHERE user_id = %s
          AND schedule_date = %s
          AND payload_hash = %s
        ORDER BY version DESC
        LIMIT 1
        """
    ).format(sql.Identifier(schema))

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, (user_id, schedule_date, payload_hash))
        return cur.fetchone()


def advisory_lock_key(user_id: int, schedule_date: date) -> int:
    digest = hashlib.sha256(f"{user_id}:{schedule_date.isoformat()}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


def acquire_schedule_date_lock(conn: psycopg.Connection, *, user_id: int, schedule_date: date) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (advisory_lock_key(user_id, schedule_date),))


def insert_schedule_version(
    conn: psycopg.Connection,
    schema: str,
    *,
    session: ClaimedSession,
    schedule_date: date,
    version: int,
    payload: dict[str, Any] | None,
    payload_hash: str,
    processing_state: str,
    worker_id: str,
) -> int | None:
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
        )
        SELECT
            cs.user_id,
            %s,
            %s,
            cs.id,
            %s::jsonb,
            %s
        FROM {}.capture_session cs
        WHERE cs.id = %s
          AND cs.state = %s::{}
          AND cs.locked_by = %s
        ON CONFLICT ON CONSTRAINT schedule_version_pkey
        DO NOTHING
        RETURNING version
        """
    ).format(
        sql.Identifier(schema),
        sql.Identifier(schema),
        capture_session_state_type(schema),
    )

    with conn.cursor() as cur:
        cur.execute(
            query,
            (
                schedule_date,
                version,
                json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
                payload_hash,
                session.id,
                processing_state,
                worker_id,
            ),
        )
        row = cur.fetchone()
        if cur.rowcount == 0:
            return None
        if cur.rowcount != 1 or row is None:
            raise LeaseLostError("Lease lost before schedule_version insert.")
        return int(row[0])


def mark_session_done(conn: psycopg.Connection, config: WorkerConfig, session_id: str) -> None:
    state_type = capture_session_state_type(config.db_schema)
    query = sql.SQL(
        """
        UPDATE {}.capture_session
        SET state = %s::{},
            error = NULL,
            locked_at = NULL,
            locked_by = NULL
        WHERE id = %s
          AND state = %s::{}
          AND locked_by = %s
        """
    ).format(
        sql.Identifier(config.db_schema),
        state_type,
        state_type,
    )

    with conn.cursor() as cur:
        cur.execute(query, (config.done_state, session_id, config.processing_state, config.worker_id))
        if cur.rowcount != 1:
            raise LeaseLostError("Lease lost before done transition.")


def mark_session_failed(
    conn: psycopg.Connection,
    config: WorkerConfig,
    session_id: str,
    error_text: str,
) -> bool:
    state_type = capture_session_state_type(config.db_schema)
    query = sql.SQL(
        """
        UPDATE {}.capture_session
        SET state = %s::{},
            error = %s,
            locked_at = NULL,
            locked_by = NULL
        WHERE id = %s
          AND state = %s::{}
          AND locked_by = %s
        """
    ).format(
        sql.Identifier(config.db_schema),
        state_type,
        state_type,
    )

    with conn.cursor() as cur:
        cur.execute(
            query,
            (
                config.failed_state,
                error_text,
                session_id,
                config.processing_state,
                config.worker_id,
            ),
        )
        return cur.rowcount == 1


def fetch_session_status(conn: psycopg.Connection, config: WorkerConfig, session_id: str) -> dict[str, Any] | None:
    query = sql.SQL(
        """
        SELECT state::text AS state, locked_by, error
        FROM {}.capture_session
        WHERE id = %s
        """
    ).format(sql.Identifier(config.db_schema))

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, (session_id,))
        return cur.fetchone()


def classify_lease_loss(
    *,
    conn: psycopg.Connection,
    config: WorkerConfig,
    session: ClaimedSession,
    logger: logging.Logger,
    reason: str,
) -> None:
    status = fetch_session_status(conn, config, session.id)
    if status is None:
        logger.error(
            "Lease lost and session row not found",
            extra={
                "event": "session.lease_lost_missing_row",
                "session_id": session.id,
                "user_id": session.user_id,
                "worker_id": config.worker_id,
                "reason": reason,
            },
        )
        return

    state = status["state"]
    locked_by = status["locked_by"]
    if state == config.done_state:
        logger.info(
            "Session already done after lease loss",
            extra={
                "event": "session.lease_lost_already_done",
                "session_id": session.id,
                "user_id": session.user_id,
                "worker_id": config.worker_id,
                "reason": reason,
            },
        )
        return
    if state == config.failed_state:
        logger.info(
            "Session already failed after lease loss",
            extra={
                "event": "session.lease_lost_already_failed",
                "session_id": session.id,
                "user_id": session.user_id,
                "worker_id": config.worker_id,
                "reason": reason,
                "error": status["error"],
            },
        )
        return
    if state == config.processing_state and locked_by != config.worker_id:
        logger.warning(
            "Lease ownership transferred to another worker",
            extra={
                "event": "session.lease_lost_transferred",
                "session_id": session.id,
                "user_id": session.user_id,
                "worker_id": config.worker_id,
                "new_locked_by": locked_by,
                "reason": reason,
            },
        )
        return

    logger.error(
        "Lease lost with unexpected session status",
        extra={
            "event": "session.lease_lost_unexpected_status",
            "session_id": session.id,
            "user_id": session.user_id,
            "worker_id": config.worker_id,
            "reason": reason,
            "state": state,
            "locked_by": locked_by,
            "error": status["error"],
        },
    )


def truncate_error(message: str, limit: int = 4000) -> str:
    if len(message) <= limit:
        return message
    return message[: limit - 3] + "..."


def maybe_sleep_with_heartbeat(conn: psycopg.Connection, config: WorkerConfig, session: ClaimedSession) -> None:
    remaining = config.simulated_work_seconds
    if remaining <= 0:
        return

    if not config.enable_lease_heartbeat:
        time.sleep(remaining)
        return

    with conn.transaction():
        refresh_lease(conn, config, session.id)

    while remaining > 0:
        chunk = min(config.lease_heartbeat_seconds, remaining)
        time.sleep(chunk)
        remaining -= chunk

        with conn.transaction():
            refresh_lease(conn, config, session.id)


def perform_fixture_work(
    conn: psycopg.Connection,
    config: WorkerConfig,
    session: ClaimedSession,
) -> tuple[dict[str, Any], date, str]:
    maybe_sleep_with_heartbeat(conn, config, session)
    fixture_payload = load_fixture_payload(config.fixture_payload_path)
    parsed_payload = (
        apply_chaos_parser(fixture_payload, config.chaos_seed) if config.enable_chaos_parser else fixture_payload
    )
    payload = normalize_schedule_payload(parsed_payload)
    schedule_date = parse_schedule_date(payload)
    payload_hash = make_payload_hash(payload)
    return payload, schedule_date, payload_hash


def run_once(config: WorkerConfig, logger: logging.Logger) -> int:
    with psycopg.connect(config.database_url) as conn:
        with conn.transaction():
            session = claim_one_session(conn, config)

        if session is None:
            logger.info(
                "No claimable session found",
                extra={"event": "worker.no_session"},
            )
            return 0

        logger.info(
            "Claimed session",
            extra={
                "event": "session.claimed",
                "session_id": session.id,
                "user_id": session.user_id,
                "worker_id": config.worker_id,
            },
        )

        try:
            payload, schedule_date, payload_hash = perform_fixture_work(conn, config, session)
            inserted_version: int | None = None
            change_detected = False

            with conn.transaction():
                acquire_schedule_date_lock(
                    conn,
                    user_id=session.user_id,
                    schedule_date=schedule_date,
                )
                latest = get_latest_schedule_version(
                    conn,
                    config.db_schema,
                    user_id=session.user_id,
                    schedule_date=schedule_date,
                )
                if latest is not None and latest["payload_hash"] == payload_hash:
                    inserted_version = int(latest["version"])
                else:
                    version = get_next_schedule_version(
                        conn,
                        config.db_schema,
                        user_id=session.user_id,
                        schedule_date=schedule_date,
                    )
                    inserted = insert_schedule_version(
                        conn,
                        config.db_schema,
                        session=session,
                        schedule_date=schedule_date,
                        version=version,
                        payload=payload,
                        payload_hash=payload_hash,
                        processing_state=config.processing_state,
                        worker_id=config.worker_id,
                    )
                    if inserted is None:
                        existing = get_schedule_version_by_hash(
                            conn,
                            config.db_schema,
                            user_id=session.user_id,
                            schedule_date=schedule_date,
                            payload_hash=payload_hash,
                        )
                        if existing is None:
                            raise RuntimeError("Schedule version insert skipped but matching hash row was not found.")
                        inserted_version = int(existing["version"])
                        change_detected = False
                    else:
                        inserted_version = inserted
                        change_detected = True
                mark_session_done(conn, config, session.id)

            logger.info(
                "Session finalized",
                extra={
                    "event": "session.done",
                    "session_id": session.id,
                    "user_id": session.user_id,
                    "worker_id": config.worker_id,
                    "schedule_date": schedule_date.isoformat(),
                    "version": inserted_version,
                    "change_detected": change_detected,
                    "payload_hash": payload_hash,
                },
            )
            return 1
        except LeaseLostError as exc:
            classify_lease_loss(
                conn=conn,
                config=config,
                session=session,
                logger=logger,
                reason=str(exc),
            )
            return 0
        except Exception as exc:
            error_text = truncate_error(str(exc))
            failed_updated = False
            with conn.transaction():
                failed_updated = mark_session_failed(conn, config, session.id, error_text)

            logger.error(
                "Session finalization failed",
                extra={
                    "event": "session.failed",
                    "session_id": session.id,
                    "user_id": session.user_id,
                    "worker_id": config.worker_id,
                    "error": error_text,
                    "failed_transition_applied": failed_updated,
                },
            )
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
            "mode": "phase3_5_chaos_normalization",
            "fixture_payload_path": config.fixture_payload_path,
            "enable_chaos_parser": config.enable_chaos_parser,
            "chaos_seed": config.chaos_seed,
            "worker_id": config.worker_id,
            "lease_timeout_seconds": config.lease_timeout_seconds,
            "lease_heartbeat_seconds": config.lease_heartbeat_seconds,
            "enable_lease_heartbeat": config.enable_lease_heartbeat,
        },
    )

    try:
        processed = run_once(config, logger)
    except Exception:
        logger.exception("Worker crashed", extra={"event": "worker.crash", "worker_id": config.worker_id})
        return 1

    logger.info(
        "Worker finished",
        extra={
            "event": "worker.finish",
            "processed_sessions": processed,
            "worker_id": config.worker_id,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
