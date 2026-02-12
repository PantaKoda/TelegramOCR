import os
import subprocess
import sys
import time
import uuid
import unittest
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

REPO_ROOT = Path(__file__).resolve().parents[1]
MAIN_SCRIPT = REPO_ROOT / "main.py"
DB_URL = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")


@unittest.skipUnless(DB_URL, "Integration test requires TEST_DATABASE_URL or DATABASE_URL")
class WorkerClaimLockingIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = f"it_claim_{uuid.uuid4().hex[:12]}"
        self.pending_session_id = str(uuid.uuid4())
        self._create_schema()
        self._seed_session(session_id=self.pending_session_id, state="pending")

    def tearDown(self) -> None:
        with psycopg.connect(DB_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {self.schema} CASCADE")

    def _create_schema(self) -> None:
        with psycopg.connect(DB_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA {self.schema}")
                cur.execute(
                    f"CREATE TYPE {self.schema}.capture_session_state AS ENUM ('pending', 'processing', 'done', 'failed')"
                )
                cur.execute(
                    f"""
                    CREATE TABLE {self.schema}.capture_session (
                        id uuid PRIMARY KEY,
                        user_id bigint NOT NULL,
                        state {self.schema}.capture_session_state NOT NULL,
                        created_at timestamptz NOT NULL DEFAULT now(),
                        error text NULL,
                        locked_at timestamptz NULL,
                        locked_by text NULL
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE {self.schema}.day_schedule (
                        user_id bigint NOT NULL,
                        schedule_date date NOT NULL,
                        current_version integer NOT NULL,
                        PRIMARY KEY (user_id, schedule_date)
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE {self.schema}.schedule_version (
                        user_id bigint NOT NULL,
                        schedule_date date NOT NULL,
                        version integer NOT NULL,
                        session_id uuid NOT NULL UNIQUE REFERENCES {self.schema}.capture_session(id),
                        payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
                        payload_hash text NOT NULL,
                        created_at timestamptz NOT NULL DEFAULT now(),
                        PRIMARY KEY (user_id, schedule_date, version)
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE FUNCTION {self.schema}.on_schedule_version_insert() RETURNS trigger
                    LANGUAGE plpgsql
                    AS $$
                    BEGIN
                        INSERT INTO {self.schema}.day_schedule (user_id, schedule_date, current_version)
                        VALUES (NEW.user_id, NEW.schedule_date, NEW.version)
                        ON CONFLICT (user_id, schedule_date)
                        DO UPDATE SET current_version = EXCLUDED.current_version;
                        RETURN NEW;
                    END;
                    $$
                    """
                )
                cur.execute(
                    f"""
                    CREATE TRIGGER trg_schedule_version_insert
                    AFTER INSERT ON {self.schema}.schedule_version
                    FOR EACH ROW
                    EXECUTE FUNCTION {self.schema}.on_schedule_version_insert()
                    """
                )

    def _seed_session(
        self,
        *,
        session_id: str,
        state: str,
        user_id: int = 8225717176,
        created_at_interval: str | None = None,
        locked_at_interval: str | None = None,
        locked_by: str | None = None,
    ) -> None:
        created_expr = "NOW()" if created_at_interval is None else f"NOW() - INTERVAL '{created_at_interval}'"
        locked_expr = "NULL" if locked_at_interval is None else f"NOW() - INTERVAL '{locked_at_interval}'"

        with psycopg.connect(DB_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.schema}.capture_session (id, user_id, state, created_at, locked_at, locked_by)
                    VALUES (%s, %s, %s::{self.schema}.capture_session_state, {created_expr}, {locked_expr}, %s)
                    """,
                    (session_id, user_id, state, locked_by),
                )

    def _start_worker(
        self,
        *,
        worker_id: str,
        simulated_work_seconds: float,
        lease_timeout_seconds: int = 300,
        enable_lease_heartbeat: bool = True,
        lease_heartbeat_seconds: float = 10.0,
    ) -> subprocess.Popen:
        env = os.environ.copy()
        env["DATABASE_URL"] = DB_URL
        env["DB_SCHEMA"] = self.schema
        env["WORKER_ID"] = worker_id
        env["SIMULATED_WORK_SECONDS"] = str(simulated_work_seconds)
        env["LEASE_TIMEOUT_SECONDS"] = str(lease_timeout_seconds)
        env["LEASE_HEARTBEAT_SECONDS"] = str(lease_heartbeat_seconds)
        env["ENABLE_LEASE_HEARTBEAT"] = "true" if enable_lease_heartbeat else "false"

        return subprocess.Popen(
            [sys.executable, str(MAIN_SCRIPT)],
            cwd=str(REPO_ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def _run_worker_once(self, *, worker_id: str) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["DATABASE_URL"] = DB_URL
        env["DB_SCHEMA"] = self.schema
        env["WORKER_ID"] = worker_id
        env["SIMULATED_WORK_SECONDS"] = "0"
        env["LEASE_TIMEOUT_SECONDS"] = "300"
        env["LEASE_HEARTBEAT_SECONDS"] = "10"
        env["ENABLE_LEASE_HEARTBEAT"] = "true"

        return subprocess.run(
            [sys.executable, str(MAIN_SCRIPT)],
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )

    def _get_session(self, session_id: str) -> dict:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT id::text AS id, state::text AS state, locked_at, locked_by, error
                    FROM {self.schema}.capture_session
                    WHERE id = %s
                    """,
                    (session_id,),
                )
                return cur.fetchone()

    def _wait_until_locked_by(self, session_id: str, worker_id: str, timeout_seconds: float = 5.0) -> None:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            row = self._get_session(session_id)
            if row and row["state"] == "processing" and row["locked_by"] == worker_id:
                return
            time.sleep(0.05)
        self.fail(f"Timed out waiting for session {session_id} locked_by={worker_id}")

    def test_two_workers_only_one_claims_with_skip_locked(self) -> None:
        worker_a = self._start_worker(worker_id="it-worker-a", simulated_work_seconds=3.0)
        worker_b = None
        try:
            self._wait_until_locked_by(self.pending_session_id, "it-worker-a")

            started = time.monotonic()
            worker_b = self._start_worker(worker_id="it-worker-b", simulated_work_seconds=0.0)
            stdout_b, stderr_b = worker_b.communicate(timeout=15)
            worker_b_elapsed = time.monotonic() - started

            stdout_a, stderr_a = worker_a.communicate(timeout=20)

            self.assertEqual(worker_a.returncode, 0, msg=f"worker_a failed\nstdout:\n{stdout_a}\nstderr:\n{stderr_a}")
            self.assertEqual(worker_b.returncode, 0, msg=f"worker_b failed\nstdout:\n{stdout_b}\nstderr:\n{stderr_b}")
            self.assertLess(worker_b_elapsed, 2.0, msg=f"worker_b elapsed too long: {worker_b_elapsed}")
            self.assertIn('"event": "worker.no_session"', stdout_b)

            with psycopg.connect(DB_URL) as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(
                        f"SELECT COUNT(*) AS cnt FROM {self.schema}.schedule_version WHERE session_id = %s",
                        (self.pending_session_id,),
                    )
                    schedule_version_count = cur.fetchone()["cnt"]

            session_row = self._get_session(self.pending_session_id)
            self.assertEqual(schedule_version_count, 1)
            self.assertEqual(session_row["state"], "done")
            self.assertIsNone(session_row["locked_at"])
            self.assertIsNone(session_row["locked_by"])
            self.assertIsNone(session_row["error"])
        finally:
            for proc in (worker_a, worker_b):
                if proc is not None and proc.poll() is None:
                    proc.kill()
                    proc.communicate(timeout=5)

    def test_pending_is_prioritized_over_stale_processing(self) -> None:
        stale_session_id = str(uuid.uuid4())
        self._seed_session(
            session_id=stale_session_id,
            state="processing",
            created_at_interval="1 day",
            locked_at_interval="1 day",
            locked_by="old-worker",
        )

        result = self._run_worker_once(worker_id="it-priority")
        self.assertEqual(result.returncode, 0, msg=f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}")

        pending_row = self._get_session(self.pending_session_id)
        stale_row = self._get_session(stale_session_id)

        self.assertEqual(pending_row["state"], "done")
        self.assertEqual(stale_row["state"], "processing")

    def test_late_worker_cannot_finalize_after_lease_is_stolen(self) -> None:
        worker_a = self._start_worker(
            worker_id="it-worker-a",
            simulated_work_seconds=3.0,
            lease_timeout_seconds=1,
            enable_lease_heartbeat=False,
        )
        worker_b = None
        try:
            self._wait_until_locked_by(self.pending_session_id, "it-worker-a")
            time.sleep(1.6)

            worker_b = self._start_worker(
                worker_id="it-worker-b",
                simulated_work_seconds=0.0,
                lease_timeout_seconds=1,
                enable_lease_heartbeat=True,
                lease_heartbeat_seconds=0.3,
            )

            stdout_b, stderr_b = worker_b.communicate(timeout=15)
            stdout_a, stderr_a = worker_a.communicate(timeout=20)

            self.assertEqual(worker_b.returncode, 0, msg=f"worker_b failed\nstdout:\n{stdout_b}\nstderr:\n{stderr_b}")
            self.assertEqual(worker_a.returncode, 0, msg=f"worker_a failed\nstdout:\n{stdout_a}\nstderr:\n{stderr_a}")
            self.assertIn('"event": "session.done"', stdout_b)
            self.assertRegex(
                stdout_a,
                r'"event": "session\.lease_lost_(transferred|already_done|already_failed|unexpected_status|missing_row)"',
            )

            with psycopg.connect(DB_URL) as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(
                        f"SELECT COUNT(*) AS cnt FROM {self.schema}.schedule_version WHERE session_id = %s",
                        (self.pending_session_id,),
                    )
                    schedule_version_count = cur.fetchone()["cnt"]

            session_row = self._get_session(self.pending_session_id)
            self.assertEqual(schedule_version_count, 1)
            self.assertEqual(session_row["state"], "done")
            self.assertIsNone(session_row["locked_at"])
            self.assertIsNone(session_row["locked_by"])
        finally:
            for proc in (worker_a, worker_b):
                if proc is not None and proc.poll() is None:
                    proc.kill()
                    proc.communicate(timeout=5)


if __name__ == "__main__":
    unittest.main()
