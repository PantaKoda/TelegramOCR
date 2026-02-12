import json
import os
import subprocess
import sys
import tempfile
import uuid
import unittest
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

REPO_ROOT = Path(__file__).resolve().parents[1]
MAIN_SCRIPT = REPO_ROOT / "main.py"
DB_URL = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")


@unittest.skipUnless(DB_URL, "Integration test requires TEST_DATABASE_URL or DATABASE_URL")
class WorkerFixtureVersioningIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = f"it_fixture_{uuid.uuid4().hex[:12]}"
        self.fixture_dir = tempfile.TemporaryDirectory()
        self.fixture_path = Path(self.fixture_dir.name) / "payload.json"
        self.user_id = 8225717176
        self._create_schema()

    def tearDown(self) -> None:
        self.fixture_dir.cleanup()
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
                    CREATE FUNCTION {self.schema}.schedule_version_validate_insert() RETURNS trigger
                    LANGUAGE plpgsql
                    AS $$
                    DECLARE
                        existing_current_version INTEGER;
                    BEGIN
                        SELECT ds.current_version
                        INTO existing_current_version
                        FROM {self.schema}.day_schedule ds
                        WHERE ds.user_id = NEW.user_id
                          AND ds.schedule_date = NEW.schedule_date
                        FOR UPDATE;

                        IF existing_current_version IS NULL THEN
                            IF NEW.version <> 1 THEN
                                RAISE EXCEPTION
                                    'First version for user % date % must be 1 (received %)',
                                    NEW.user_id,
                                    NEW.schedule_date,
                                    NEW.version;
                            END IF;
                        ELSIF NEW.version <> existing_current_version + 1 THEN
                            RAISE EXCEPTION
                                'Version for user % date % must be % (received %)',
                                NEW.user_id,
                                NEW.schedule_date,
                                existing_current_version + 1,
                                NEW.version;
                        END IF;

                        RETURN NEW;
                    END;
                    $$
                    """
                )
                cur.execute(
                    f"""
                    CREATE FUNCTION {self.schema}.schedule_version_sync_day_schedule() RETURNS trigger
                    LANGUAGE plpgsql
                    AS $$
                    BEGIN
                        INSERT INTO {self.schema}.day_schedule (user_id, schedule_date, current_version)
                        VALUES (NEW.user_id, NEW.schedule_date, NEW.version)
                        ON CONFLICT (user_id, schedule_date)
                        DO UPDATE SET current_version = GREATEST(
                            {self.schema}.day_schedule.current_version,
                            EXCLUDED.current_version
                        );
                        RETURN NEW;
                    END;
                    $$
                    """
                )
                cur.execute(
                    f"""
                    CREATE TRIGGER trg_schedule_version_validate_insert
                    BEFORE INSERT ON {self.schema}.schedule_version
                    FOR EACH ROW
                    EXECUTE FUNCTION {self.schema}.schedule_version_validate_insert()
                    """
                )
                cur.execute(
                    f"""
                    CREATE TRIGGER trg_schedule_version_sync_day_schedule
                    AFTER INSERT ON {self.schema}.schedule_version
                    FOR EACH ROW
                    EXECUTE FUNCTION {self.schema}.schedule_version_sync_day_schedule()
                    """
                )

    def _write_fixture(self, payload: dict) -> None:
        self.fixture_path.write_text(json.dumps(payload), encoding="utf-8")

    def _seed_pending_session(self, session_id: str) -> None:
        with psycopg.connect(DB_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.schema}.capture_session (id, user_id, state)
                    VALUES (%s, %s, 'pending'::{self.schema}.capture_session_state)
                    """,
                    (session_id, self.user_id),
                )

    def _run_worker_once(
        self,
        *,
        worker_id: str,
        enable_chaos_parser: bool = False,
        chaos_seed: int = 0,
    ) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["DATABASE_URL"] = DB_URL
        env["DB_SCHEMA"] = self.schema
        env["WORKER_ID"] = worker_id
        env["FIXTURE_PAYLOAD_PATH"] = str(self.fixture_path)
        env["ENABLE_CHAOS_PARSER"] = "true" if enable_chaos_parser else "false"
        env["CHAOS_SEED"] = str(chaos_seed)
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

    def _fetch_versions(self, schedule_date: str) -> list[dict]:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT version, payload
                    FROM {self.schema}.schedule_version
                    WHERE user_id = %s
                      AND schedule_date = %s
                    ORDER BY version
                    """,
                    (self.user_id, schedule_date),
                )
                return list(cur.fetchall())

    def _fetch_current_version(self, schedule_date: str) -> int | None:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT current_version
                    FROM {self.schema}.day_schedule
                    WHERE user_id = %s
                      AND schedule_date = %s
                    """,
                    (self.user_id, schedule_date),
                )
                row = cur.fetchone()
                return None if row is None else row["current_version"]

    def _fetch_session_state(self, session_id: str) -> str:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"SELECT state::text AS state FROM {self.schema}.capture_session WHERE id = %s",
                    (session_id,),
                )
                row = cur.fetchone()
                return row["state"]

    def test_worker_writes_fixture_payload_and_marks_done(self) -> None:
        session_id = str(uuid.uuid4())
        payload = {
            "schedule_date": "2026-02-10",
            "entries": [{"start": "10:00", "end": "14:00", "title": "Cleaning", "location": "Billdal"}],
        }
        self._write_fixture(payload)
        self._seed_pending_session(session_id)

        result = self._run_worker_once(worker_id="it-fixture-a")
        self.assertEqual(result.returncode, 0, msg=f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}")
        self.assertIn('"event": "session.done"', result.stdout)

        versions = self._fetch_versions("2026-02-10")
        self.assertEqual(len(versions), 1)
        self.assertEqual(versions[0]["version"], 1)
        self.assertEqual(versions[0]["payload"], payload)
        self.assertEqual(self._fetch_current_version("2026-02-10"), 1)
        self.assertEqual(self._fetch_session_state(session_id), "done")

    def test_fixture_timeline_updates_increment_version_and_keep_history(self) -> None:
        schedule_date = "2026-02-10"
        payload_a = {
            "schedule_date": schedule_date,
            "entries": [
                {"start": "10:00", "end": "14:00", "title": "Cleaning", "location": "Billdal"},
                {"start": "15:00", "end": "19:00", "title": "Office", "location": "Molndal"},
            ],
        }
        payload_b = {
            "schedule_date": schedule_date,
            "entries": [
                {"start": "11:00", "end": "15:00", "title": "Cleaning", "location": "Billdal"},
                {"start": "15:00", "end": "19:00", "title": "Office", "location": "Molndal"},
            ],
        }
        payload_c = {
            "schedule_date": schedule_date,
            "entries": [
                {"start": "11:00", "end": "15:00", "title": "Cleaning", "location": "Billdal"},
            ],
        }

        session_a = str(uuid.uuid4())
        self._write_fixture(payload_a)
        self._seed_pending_session(session_a)
        first = self._run_worker_once(worker_id="it-fixture-1")
        self.assertEqual(first.returncode, 0, msg=f"stdout:\n{first.stdout}\nstderr:\n{first.stderr}")

        session_b = str(uuid.uuid4())
        self._write_fixture(payload_b)
        self._seed_pending_session(session_b)
        second = self._run_worker_once(worker_id="it-fixture-2")
        self.assertEqual(second.returncode, 0, msg=f"stdout:\n{second.stdout}\nstderr:\n{second.stderr}")

        session_c = str(uuid.uuid4())
        self._write_fixture(payload_c)
        self._seed_pending_session(session_c)
        third = self._run_worker_once(worker_id="it-fixture-3")
        self.assertEqual(third.returncode, 0, msg=f"stdout:\n{third.stdout}\nstderr:\n{third.stderr}")

        versions = self._fetch_versions(schedule_date)
        self.assertEqual([row["version"] for row in versions], [1, 2, 3])
        self.assertEqual(versions[0]["payload"], payload_a)
        self.assertEqual(versions[1]["payload"], payload_b)
        self.assertEqual(versions[2]["payload"], payload_c)
        self.assertEqual(self._fetch_current_version(schedule_date), 3)
        self.assertEqual(len(versions[0]["payload"]["entries"]), 2)
        self.assertEqual(len(versions[2]["payload"]["entries"]), 1)
        self.assertEqual(self._fetch_session_state(session_a), "done")
        self.assertEqual(self._fetch_session_state(session_b), "done")
        self.assertEqual(self._fetch_session_state(session_c), "done")

    def test_chaos_representation_noise_does_not_create_new_versions(self) -> None:
        schedule_date = "2026-02-10"
        payload = {
            "schedule_date": schedule_date,
            "entries": [
                {"start": "10:00", "end": "14:00", "title": "Cleaning", "location": "Billdal"},
                {"start": "15:00", "end": "19:00", "title": "Office Shift", "location": "Molndal"},
            ],
        }
        self._write_fixture(payload)

        session_ids: list[str] = []
        for seed in range(20):
            session_id = str(uuid.uuid4())
            session_ids.append(session_id)
            self._seed_pending_session(session_id)
            result = self._run_worker_once(
                worker_id=f"it-chaos-{seed}",
                enable_chaos_parser=True,
                chaos_seed=seed,
            )
            self.assertEqual(result.returncode, 0, msg=f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}")
            self.assertIn('"event": "session.done"', result.stdout)

        versions = self._fetch_versions(schedule_date)
        self.assertEqual(len(versions), 1)
        self.assertEqual(versions[0]["version"], 1)
        self.assertEqual(
            versions[0]["payload"],
            {
                "schedule_date": schedule_date,
                "entries": [
                    {"start": "10:00", "end": "14:00", "title": "Cleaning", "location": "Billdal"},
                    {"start": "15:00", "end": "19:00", "title": "Office Shift", "location": "Molndal"},
                ],
            },
        )
        self.assertEqual(self._fetch_current_version(schedule_date), 1)
        for session_id in session_ids:
            self.assertEqual(self._fetch_session_state(session_id), "done")

    def test_chaos_runs_preserve_single_version_until_semantic_change(self) -> None:
        schedule_date = "2026-02-10"
        payload_a = {
            "schedule_date": schedule_date,
            "entries": [
                {"start": "10:00", "end": "14:00", "title": "Cleaning", "location": "Billdal"},
                {"start": "15:00", "end": "19:00", "title": "Office Shift", "location": "Molndal"},
            ],
        }
        payload_b = {
            "schedule_date": schedule_date,
            "entries": [
                {"start": "10:00", "end": "15:00", "title": "Cleaning", "location": "Billdal"},
                {"start": "15:00", "end": "19:00", "title": "Office Shift", "location": "Molndal"},
            ],
        }

        self._write_fixture(payload_a)
        for seed in range(20):
            self._seed_pending_session(str(uuid.uuid4()))
            result = self._run_worker_once(
                worker_id=f"it-chaos-a-{seed}",
                enable_chaos_parser=True,
                chaos_seed=seed,
            )
            self.assertEqual(result.returncode, 0, msg=f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}")

        versions_after_a = self._fetch_versions(schedule_date)
        self.assertEqual(len(versions_after_a), 1)
        self.assertEqual(versions_after_a[0]["version"], 1)

        self._write_fixture(payload_b)
        for seed in range(20, 40):
            self._seed_pending_session(str(uuid.uuid4()))
            result = self._run_worker_once(
                worker_id=f"it-chaos-b-{seed}",
                enable_chaos_parser=True,
                chaos_seed=seed,
            )
            self.assertEqual(result.returncode, 0, msg=f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}")

        versions_after_b = self._fetch_versions(schedule_date)
        self.assertEqual(len(versions_after_b), 2)
        self.assertEqual([row["version"] for row in versions_after_b], [1, 2])
        self.assertEqual(versions_after_b[0]["payload"], payload_a)
        self.assertEqual(versions_after_b[1]["payload"], payload_b)
        self.assertEqual(self._fetch_current_version(schedule_date), 2)


if __name__ == "__main__":
    unittest.main()
