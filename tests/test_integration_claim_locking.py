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
        self.session_id = str(uuid.uuid4())
        self._create_schema()
        self._seed_pending_session()

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
                        closed_at timestamptz NULL,
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

    def _seed_pending_session(self) -> None:
        with psycopg.connect(DB_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.schema}.capture_session (id, user_id, state, created_at)
                    VALUES (%s, %s, 'pending', now())
                    """,
                    (self.session_id, 8225717176),
                )

    def _start_worker(self, worker_id: str, simulated_work_seconds: float) -> subprocess.Popen:
        env = os.environ.copy()
        env["DATABASE_URL"] = DB_URL
        env["DB_SCHEMA"] = self.schema
        env["WORKER_ID"] = worker_id
        env["SIMULATED_WORK_SECONDS"] = str(simulated_work_seconds)
        env["LEASE_TIMEOUT_SECONDS"] = "300"

        return subprocess.Popen(
            [sys.executable, str(MAIN_SCRIPT)],
            cwd=str(REPO_ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def test_two_workers_only_one_claims_with_skip_locked(self) -> None:
        worker_a = self._start_worker(worker_id="it-worker-a", simulated_work_seconds=3.0)
        worker_b = None
        try:
            # Give worker A time to claim and enter simulated work while holding the row lock.
            time.sleep(0.8)
            if worker_a.poll() is not None:
                stdout_a, stderr_a = worker_a.communicate(timeout=5)
                self.fail(
                    "worker_a exited before worker_b started.\n"
                    f"stdout:\n{stdout_a}\nstderr:\n{stderr_a}"
                )

            started = time.monotonic()
            worker_b = self._start_worker(worker_id="it-worker-b", simulated_work_seconds=0.0)
            stdout_b, stderr_b = worker_b.communicate(timeout=15)
            worker_b_elapsed = time.monotonic() - started

            stdout_a, stderr_a = worker_a.communicate(timeout=20)

            self.assertEqual(worker_a.returncode, 0, msg=f"worker_a failed\nstdout:\n{stdout_a}\nstderr:\n{stderr_a}")
            self.assertEqual(worker_b.returncode, 0, msg=f"worker_b failed\nstdout:\n{stdout_b}\nstderr:\n{stderr_b}")

            # Skip-locked claim should make worker B return quickly instead of waiting on worker A.
            self.assertLess(worker_b_elapsed, 2.0, msg=f"worker_b elapsed too long: {worker_b_elapsed}")
            self.assertIn('"event": "worker.no_session"', stdout_b)

            with psycopg.connect(DB_URL) as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(
                        f"SELECT COUNT(*) AS cnt FROM {self.schema}.schedule_version WHERE session_id = %s",
                        (self.session_id,),
                    )
                    schedule_version_count = cur.fetchone()["cnt"]

                    cur.execute(
                        f"SELECT state::text AS state, locked_at, locked_by, error FROM {self.schema}.capture_session WHERE id = %s",
                        (self.session_id,),
                    )
                    session_row = cur.fetchone()

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


if __name__ == "__main__":
    unittest.main()
