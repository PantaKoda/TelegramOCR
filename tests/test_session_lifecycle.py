import os
import threading
import uuid
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg
from psycopg.rows import dict_row

from domain.session_lifecycle import (
    SessionLifecycleConfig,
    finalize_session,
    find_finalizable_sessions,
    load_lifecycle_config_from_env,
    run_lifecycle_once,
)

DB_URL = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")


class SessionLifecycleConfigTests(unittest.TestCase):
    def test_loads_idle_timeout_from_env(self) -> None:
        config = load_lifecycle_config_from_env(env={"SESSION_IDLE_TIMEOUT_SECONDS": "45"})
        self.assertEqual(config.idle_timeout_seconds, 45)

    def test_uses_default_when_env_is_missing(self) -> None:
        config = load_lifecycle_config_from_env()
        self.assertEqual(config.idle_timeout_seconds, 25)
        self.assertEqual(config.open_state, "closed")
        self.assertEqual(config.failed_state, "failed")

    def test_rejects_invalid_idle_timeout_value(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be an integer"):
            load_lifecycle_config_from_env(env={"SESSION_IDLE_TIMEOUT_SECONDS": "abc"})

    def test_rejects_negative_idle_timeout(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be >= 0"):
            load_lifecycle_config_from_env(env={"SESSION_IDLE_TIMEOUT_SECONDS": "-1"})

    def test_loads_state_overrides_from_env(self) -> None:
        config = load_lifecycle_config_from_env(
            env={
                "OPEN_STATE": "closed",
                "PROCESSING_STATE": "processing",
                "PROCESSED_STATE": "done",
                "FAILED_STATE": "failed",
            }
        )
        self.assertEqual(config.open_state, "closed")
        self.assertEqual(config.processing_state, "processing")
        self.assertEqual(config.processed_state, "done")
        self.assertEqual(config.failed_state, "failed")

    def test_supports_pending_state_alias(self) -> None:
        config = load_lifecycle_config_from_env(env={"PENDING_STATE": "closed"})
        self.assertEqual(config.open_state, "closed")


@unittest.skipUnless(DB_URL, "Integration test requires TEST_DATABASE_URL or DATABASE_URL")
class SessionLifecycleIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = f"it_session_lifecycle_{uuid.uuid4().hex[:12]}"
        self.config = SessionLifecycleConfig(idle_timeout_seconds=25)
        self._create_schema()

    def tearDown(self) -> None:
        with psycopg.connect(DB_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {self.schema} CASCADE")

    def _create_schema(self) -> None:
        with psycopg.connect(DB_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA {self.schema}")
                cur.execute(
                    f"""
                    CREATE TABLE {self.schema}.capture_session (
                        id UUID PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        state TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        closed_at TIMESTAMPTZ NULL,
                        error TEXT NULL,
                        CONSTRAINT capture_session_closed_at_state_chk CHECK (
                            (state = 'open' AND closed_at IS NULL)
                            OR
                            (state <> 'open' AND closed_at IS NOT NULL)
                        ),
                        CONSTRAINT capture_session_error_failed_chk CHECK (
                            (state = 'failed' AND error IS NOT NULL)
                            OR
                            (state <> 'failed' AND error IS NULL)
                        )
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE {self.schema}.capture_image (
                        id UUID PRIMARY KEY,
                        session_id UUID NOT NULL REFERENCES {self.schema}.capture_session(id),
                        sequence INTEGER NOT NULL,
                        r2_key TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )

    def _seed_session(self, *, state: str = "closed", user_id: int = 8225717176) -> str:
        session_id = str(uuid.uuid4())
        closed_at = None if state == "open" else datetime.now(timezone.utc)
        with psycopg.connect(DB_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.schema}.capture_session (id, user_id, state, closed_at, error)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (session_id, user_id, state, closed_at, None),
                )
        return session_id

    def _add_image(self, *, session_id: str, sequence: int, created_at: datetime) -> None:
        with psycopg.connect(DB_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.schema}.capture_image (id, session_id, sequence, r2_key, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (str(uuid.uuid4()), session_id, sequence, f"r2/{session_id}/{sequence}.png", created_at),
                )

    def _session_state(self, session_id: str) -> str:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"SELECT state FROM {self.schema}.capture_session WHERE id = %s",
                    (session_id,),
                )
                row = cur.fetchone()
        return row["state"]

    def test_uploading_images_quickly_is_not_finalized(self) -> None:
        now = datetime.now(timezone.utc)
        session_id = self._seed_session(state="closed")
        self._add_image(session_id=session_id, sequence=1, created_at=now - timedelta(seconds=5))

        with psycopg.connect(DB_URL) as conn:
            finalizable = find_finalizable_sessions(conn, self.schema, now, config=self.config)

        self.assertEqual(finalizable, [])

    def test_idle_timeout_passes_session_becomes_finalizable_and_finalized(self) -> None:
        now = datetime.now(timezone.utc)
        session_id = self._seed_session(state="closed")
        self._add_image(session_id=session_id, sequence=1, created_at=now - timedelta(seconds=40))

        with psycopg.connect(DB_URL) as conn:
            with conn.transaction():
                finalizable = find_finalizable_sessions(conn, self.schema, now, config=self.config)
                self.assertEqual(finalizable, [session_id])
                claimed = finalize_session(conn, self.schema, session_id, config=self.config)

        self.assertTrue(claimed)
        self.assertEqual(self._session_state(session_id), "processing")

    def test_double_worker_race_processes_once(self) -> None:
        now = datetime.now(timezone.utc)
        session_id = self._seed_session(state="closed")
        self._add_image(session_id=session_id, sequence=1, created_at=now - timedelta(seconds=40))

        results: list[bool] = []
        errors: list[Exception] = []
        barrier = threading.Barrier(2)

        def worker_finalize() -> None:
            try:
                with psycopg.connect(DB_URL) as conn:
                    with conn.transaction():
                        barrier.wait(timeout=5)
                        results.append(finalize_session(conn, self.schema, session_id, config=self.config))
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        first = threading.Thread(target=worker_finalize)
        second = threading.Thread(target=worker_finalize)
        first.start()
        second.start()
        first.join(timeout=10)
        second.join(timeout=10)

        self.assertEqual(errors, [])
        self.assertEqual(sorted(results), [False, True])
        self.assertEqual(self._session_state(session_id), "processing")

    def test_notifications_emitted_only_once_for_finalized_session(self) -> None:
        now = datetime.now(timezone.utc)
        session_id = self._seed_session(state="closed")
        self._add_image(session_id=session_id, sequence=1, created_at=now - timedelta(seconds=60))
        self._add_image(session_id=session_id, sequence=2, created_at=now - timedelta(seconds=50))

        emitted_notifications: list[Any] = []
        process_calls: list[str] = []

        def load_session_images(_conn: Any, _schema: str, loaded_session_id: str) -> list[str]:
            process_calls.append(f"load:{loaded_session_id}")
            return [f"{loaded_session_id}-1.png", f"{loaded_session_id}-2.png"]

        def run_full_pipeline(images: list[str]) -> dict[str, Any]:
            process_calls.append(f"pipeline:{len(images)}")
            return {"images": images}

        def persist_events_and_snapshot(_conn: Any, _schema: str, _session_id: str, _pipeline: Any) -> list[dict[str, str]]:
            process_calls.append("persist")
            return [{"event_id": "evt-1"}]

        def build_notifications(events: list[dict[str, str]]) -> list[dict[str, str]]:
            notifications = [{"id": f"n-{event['event_id']}"} for event in events]
            process_calls.append("notify")
            return notifications

        def store_notifications(_conn: Any, _schema: str, _session_id: str, notifications: list[dict[str, str]]) -> int:
            emitted_notifications.extend(notifications)
            process_calls.append("store")
            return len(notifications)

        with psycopg.connect(DB_URL) as conn:
            with conn.transaction():
                first = run_lifecycle_once(
                    conn,
                    self.schema,
                    now,
                    load_session_images=load_session_images,
                    run_full_pipeline=run_full_pipeline,
                    persist_events_and_snapshot=persist_events_and_snapshot,
                    build_notifications=build_notifications,
                    store_notifications=store_notifications,
                    config=self.config,
                )
            with conn.transaction():
                second = run_lifecycle_once(
                    conn,
                    self.schema,
                    now + timedelta(seconds=10),
                    load_session_images=load_session_images,
                    run_full_pipeline=run_full_pipeline,
                    persist_events_and_snapshot=persist_events_and_snapshot,
                    build_notifications=build_notifications,
                    store_notifications=store_notifications,
                    config=self.config,
                )

        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])
        self.assertEqual(len(emitted_notifications), 1)
        self.assertEqual(self._session_state(session_id), "done")
        self.assertEqual(process_calls.count("notify"), 1)
        self.assertEqual(process_calls.count("store"), 1)

    def test_pipeline_failure_marks_session_failed_once(self) -> None:
        now = datetime.now(timezone.utc)
        session_id = self._seed_session(state="closed")
        self._add_image(session_id=session_id, sequence=1, created_at=now - timedelta(seconds=60))

        failure_calls: list[str] = []

        def load_session_images(_conn: Any, _schema: str, loaded_session_id: str) -> list[str]:
            return [f"{loaded_session_id}-1.png"]

        def run_full_pipeline(_images: list[str]) -> dict[str, Any]:
            raise RuntimeError("forced failure")

        def persist_events_and_snapshot(_conn: Any, _schema: str, _session_id: str, _pipeline: Any) -> list[dict[str, str]]:
            self.fail("persist_events_and_snapshot should not run when pipeline fails")

        def build_notifications(_events: list[dict[str, str]]) -> list[dict[str, str]]:
            self.fail("build_notifications should not run when pipeline fails")

        def on_session_failed(failed_session_id: str, error: Exception, marked_failed: bool) -> None:
            failure_calls.append(f"{failed_session_id}:{type(error).__name__}:{marked_failed}")

        with psycopg.connect(DB_URL) as conn:
            with conn.transaction():
                first = run_lifecycle_once(
                    conn,
                    self.schema,
                    now,
                    load_session_images=load_session_images,
                    run_full_pipeline=run_full_pipeline,
                    persist_events_and_snapshot=persist_events_and_snapshot,
                    build_notifications=build_notifications,
                    on_session_failed=on_session_failed,
                    config=self.config,
                )
            with conn.transaction():
                second = run_lifecycle_once(
                    conn,
                    self.schema,
                    now + timedelta(seconds=10),
                    load_session_images=load_session_images,
                    run_full_pipeline=run_full_pipeline,
                    persist_events_and_snapshot=persist_events_and_snapshot,
                    build_notifications=build_notifications,
                    on_session_failed=on_session_failed,
                    config=self.config,
                )

        self.assertEqual(first, [])
        self.assertEqual(second, [])
        self.assertEqual(self._session_state(session_id), "failed")
        self.assertEqual(len(failure_calls), 1)


if __name__ == "__main__":
    unittest.main()
