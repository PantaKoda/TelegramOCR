import os
import tempfile
import uuid
import unittest
from datetime import date
from unittest.mock import patch

import psycopg
from psycopg.rows import dict_row

from domain.session_lifecycle import SessionLifecycleConfig
from worker.run_forever import (
    WorkerRuntimeConfig,
    _coerce_fixture_entries,
    _parse_schedule_date,
    load_runtime_config,
    run_iteration,
    setup_logger,
)

DB_URL = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")


class RunForeverConfigTests(unittest.TestCase):
    def test_load_runtime_config_defaults(self) -> None:
        env = {
            "DATABASE_URL": "postgresql://user:pass@localhost:5432/db",
        }
        with patch.dict(os.environ, env, clear=True):
            config = load_runtime_config()
        self.assertEqual(config.database_url, env["DATABASE_URL"])
        self.assertEqual(config.db_schema, "schedule_ingest")
        self.assertEqual(config.poll_seconds, 5.0)
        self.assertEqual(config.summary_threshold, 3)
        self.assertEqual(config.fixture_payload_path, "fixtures/sample_schedule.json")

    def test_load_runtime_config_parses_custom_values(self) -> None:
        env = {
            "DATABASE_URL": "postgresql://user:pass@localhost:5432/db",
            "DB_SCHEMA": "custom_schema",
            "WORKER_POLL_SECONDS": "2.5",
            "NOTIFICATION_SUMMARY_THRESHOLD": "5",
            "FIXTURE_PAYLOAD_PATH": "/tmp/fixture.json",
        }
        with patch.dict(os.environ, env, clear=True):
            config = load_runtime_config()
        self.assertEqual(config.db_schema, "custom_schema")
        self.assertEqual(config.poll_seconds, 2.5)
        self.assertEqual(config.summary_threshold, 5)
        self.assertEqual(config.fixture_payload_path, "/tmp/fixture.json")

    def test_load_runtime_config_requires_database_url(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "DATABASE_URL"):
                load_runtime_config()

    def test_load_runtime_config_rejects_non_positive_poll(self) -> None:
        env = {
            "DATABASE_URL": "postgresql://user:pass@localhost:5432/db",
            "WORKER_POLL_SECONDS": "0",
        }
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(RuntimeError, "WORKER_POLL_SECONDS"):
                load_runtime_config()


class RunForeverFixtureParsingTests(unittest.TestCase):
    def test_parse_schedule_date(self) -> None:
        value = _parse_schedule_date({"schedule_date": "2026-08-22"})
        self.assertEqual(value, date(2026, 8, 22))

    def test_coerce_fixture_entries_preserves_address(self) -> None:
        payload = {
            "entries": [
                {
                    "start": "10:00",
                    "end": "14:00",
                    "title": "Marie Sjoberg",
                    "location": "Billdal",
                    "address": "Valebergsvagen 316",
                }
            ]
        }
        rows = _coerce_fixture_entries(payload)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["address"], "Valebergsvagen 316")


@unittest.skipUnless(DB_URL, "Integration test requires TEST_DATABASE_URL or DATABASE_URL")
class RunForeverIterationIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = f"it_run_forever_{uuid.uuid4().hex[:12]}"
        self.fixture_file = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        self.fixture_file.write(
            """
            {
              "schedule_date": "2026-08-22",
              "entries": [
                {"start": "10:00", "end": "14:00", "title": "Marie Sjoberg", "location": "Billdal", "address": "Valebergsvagen 316"}
              ]
            }
            """
        )
        self.fixture_file.flush()
        self.fixture_file.close()
        self._create_schema()

    def tearDown(self) -> None:
        os.unlink(self.fixture_file.name)
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
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
                cur.execute(
                    f"""
                    CREATE TABLE {self.schema}.day_snapshot (
                        user_id BIGINT NOT NULL,
                        schedule_date DATE NOT NULL,
                        snapshot_payload JSONB NOT NULL CHECK (jsonb_typeof(snapshot_payload) = 'array'),
                        source_session_id UUID NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (user_id, schedule_date)
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE {self.schema}.schedule_event (
                        event_id UUID PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        schedule_date DATE NOT NULL,
                        event_type TEXT NOT NULL,
                        location_fingerprint TEXT NOT NULL,
                        customer_fingerprint TEXT NOT NULL,
                        old_value_hash TEXT NOT NULL,
                        new_value_hash TEXT NOT NULL,
                        old_value JSONB NULL,
                        new_value JSONB NULL,
                        detected_at TIMESTAMPTZ NOT NULL,
                        source_session_id UUID NOT NULL
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE UNIQUE INDEX {self.schema}_schedule_event_dedupe
                    ON {self.schema}.schedule_event (
                        user_id,
                        schedule_date,
                        location_fingerprint,
                        event_type,
                        old_value_hash,
                        new_value_hash
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE {self.schema}.schedule_notification (
                        notification_id TEXT PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        schedule_date DATE NOT NULL,
                        source_session_id UUID NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending',
                        notification_type TEXT NOT NULL,
                        message TEXT NOT NULL,
                        event_ids JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        sent_at TIMESTAMPTZ NULL
                    )
                    """
                )

    def _seed_session(self) -> str:
        session_id = str(uuid.uuid4())
        with psycopg.connect(DB_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.schema}.capture_session (id, user_id, state)
                    VALUES (%s, %s, %s)
                    """,
                    (session_id, 8225717176, "pending"),
                )
                cur.execute(
                    f"""
                    INSERT INTO {self.schema}.capture_image (id, session_id, sequence, r2_key, created_at)
                    VALUES
                        (%s, %s, 1, %s, NOW() - INTERVAL '60 seconds'),
                        (%s, %s, 2, %s, NOW() - INTERVAL '50 seconds')
                    """,
                    (str(uuid.uuid4()), session_id, f"r2/{session_id}/1.png", str(uuid.uuid4()), session_id, f"r2/{session_id}/2.png"),
                )
        return session_id

    def _count_notifications(self) -> int:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self.schema}.schedule_notification")
                return int(cur.fetchone()[0])

    def _session_state(self, session_id: str) -> str:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"SELECT state FROM {self.schema}.capture_session WHERE id = %s",
                    (session_id,),
                )
                row = cur.fetchone()
        return row["state"]

    def test_run_iteration_processes_idle_session_once(self) -> None:
        session_id = self._seed_session()
        logger = setup_logger()
        runtime_config = WorkerRuntimeConfig(
            database_url=DB_URL,
            db_schema=self.schema,
            poll_seconds=5.0,
            fixture_payload_path=self.fixture_file.name,
            summary_threshold=3,
        )
        lifecycle_config = SessionLifecycleConfig(idle_timeout_seconds=25)

        with psycopg.connect(DB_URL) as conn:
            with conn.transaction():
                first = run_iteration(conn, runtime_config, lifecycle_config, logger=logger)
            with conn.transaction():
                second = run_iteration(conn, runtime_config, lifecycle_config, logger=logger)

        self.assertEqual(first["processed_sessions"], 1)
        self.assertEqual(first["stored_notifications"], 1)
        self.assertEqual(second["processed_sessions"], 0)
        self.assertEqual(second["stored_notifications"], 0)
        self.assertEqual(self._session_state(session_id), "done")
        self.assertEqual(self._count_notifications(), 1)


if __name__ == "__main__":
    unittest.main()
