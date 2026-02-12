import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import main as worker


class FixturePayloadTests(unittest.TestCase):
    def test_load_fixture_payload_reads_object(self) -> None:
        payload = {
            "schedule_date": "2026-02-10",
            "entries": [{"start": "10:00", "end": "14:00", "title": "Cleaning", "location": "Billdal"}],
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture_path = Path(temp_dir) / "sample.json"
            fixture_path.write_text(json.dumps(payload), encoding="utf-8")

            loaded = worker.load_fixture_payload(str(fixture_path))

        self.assertEqual(loaded, payload)

    def test_parse_schedule_date_requires_iso_date(self) -> None:
        parsed = worker.parse_schedule_date({"schedule_date": "2026-02-10"})
        self.assertEqual(parsed, date(2026, 2, 10))

        with self.assertRaises(RuntimeError):
            worker.parse_schedule_date({"schedule_date": "2026/02/10"})

        with self.assertRaises(RuntimeError):
            worker.parse_schedule_date({"entries": []})


class PayloadNormalizationTests(unittest.TestCase):
    def test_normalize_schedule_payload_canonicalizes_times_text_and_order(self) -> None:
        payload = {
            "schedule_date": "2026-02-10",
            "entries": [
                {"start": "9:0", "end": "18.00", "title": " Office  Shift ", "location": "BILLDAL"},
                {"start": "10 00", "end": "14:00", "title": "Cleaning", "location": " billdal "},
            ],
        }

        normalized = worker.normalize_schedule_payload(payload)

        self.assertEqual(
            normalized,
            {
                "schedule_date": "2026-02-10",
                "entries": [
                    {"start": "09:00", "end": "18:00", "title": "Office Shift", "location": "Billdal"},
                    {"start": "10:00", "end": "14:00", "title": "Cleaning", "location": "Billdal"},
                ],
            },
        )

    def test_chaos_parser_is_seed_deterministic_and_roundtrips_to_same_canonical(self) -> None:
        canonical = {
            "schedule_date": "2026-02-10",
            "entries": [
                {"start": "10:00", "end": "14:00", "title": "Cleaning", "location": "Billdal"},
                {"start": "15:00", "end": "19:00", "title": "Office", "location": "Molndal"},
            ],
        }

        noisy_a = worker.apply_chaos_parser(canonical, seed=7)
        noisy_b = worker.apply_chaos_parser(canonical, seed=7)
        noisy_c = worker.apply_chaos_parser(canonical, seed=8)

        self.assertEqual(noisy_a, noisy_b)
        self.assertNotEqual(noisy_a, noisy_c)
        self.assertEqual(worker.normalize_schedule_payload(noisy_a), canonical)
        self.assertEqual(worker.normalize_schedule_payload(noisy_c), canonical)


class ScheduleVersionWriteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.session = worker.ClaimedSession(id="session-1", user_id=8225717176)

    def test_insert_schedule_version_executes_insert(self) -> None:
        conn = MagicMock()
        cur = MagicMock()
        cur.rowcount = 1
        cur.fetchone.return_value = (2,)
        conn.cursor.return_value.__enter__.return_value = cur

        inserted_version = worker.insert_schedule_version(
            conn,
            "schedule_ingest",
            session=self.session,
            schedule_date=date(2026, 2, 10),
            version=2,
            payload={"schedule_date": "2026-02-10", "entries": []},
            payload_hash="abc123",
            processing_state="processing",
            worker_id="worker-1",
        )
        self.assertEqual(inserted_version, 2)

        cur.execute.assert_called_once()
        _, params = cur.execute.call_args[0]
        self.assertEqual(params[0], date(2026, 2, 10))
        self.assertEqual(params[1], 2)
        self.assertEqual(params[2], '{"entries":[],"schedule_date":"2026-02-10"}')
        self.assertEqual(params[3], "abc123")
        self.assertEqual(params[4], self.session.id)
        self.assertEqual(params[5], "processing")
        self.assertEqual(params[6], "worker-1")

    def test_insert_schedule_version_rejects_missing_payload(self) -> None:
        conn = MagicMock()

        with self.assertRaises(ValueError):
            worker.insert_schedule_version(
                conn,
                "schedule_ingest",
                session=self.session,
                schedule_date=date(2026, 2, 10),
                version=1,
                payload=None,
                payload_hash="abc123",
                processing_state="processing",
                worker_id="worker-1",
            )

        conn.cursor.assert_not_called()

    def test_insert_schedule_version_returns_none_when_conflict_skips_insert(self) -> None:
        conn = MagicMock()
        cur = MagicMock()
        cur.rowcount = 0
        cur.fetchone.return_value = None
        conn.cursor.return_value.__enter__.return_value = cur

        inserted_version = worker.insert_schedule_version(
            conn,
            "schedule_ingest",
            session=self.session,
            schedule_date=date(2026, 2, 10),
            version=2,
            payload={"schedule_date": "2026-02-10", "entries": []},
            payload_hash="abc123",
            processing_state="processing",
            worker_id="worker-1",
        )

        self.assertIsNone(inserted_version)


class NextVersionTests(unittest.TestCase):
    def test_get_next_schedule_version_returns_one_when_absent(self) -> None:
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchone.return_value = None
        conn.cursor.return_value.__enter__.return_value = cur

        value = worker.get_next_schedule_version(
            conn,
            "schedule_ingest",
            user_id=8225717176,
            schedule_date=date(2026, 2, 10),
        )

        self.assertEqual(value, 1)

    def test_get_next_schedule_version_increments_existing(self) -> None:
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchone.return_value = {"current_version": 4}
        conn.cursor.return_value.__enter__.return_value = cur

        value = worker.get_next_schedule_version(
            conn,
            "schedule_ingest",
            user_id=8225717176,
            schedule_date=date(2026, 2, 10),
        )

        self.assertEqual(value, 5)


class LatestVersionTests(unittest.TestCase):
    def test_get_latest_schedule_version_returns_row(self) -> None:
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchone.return_value = {"version": 4, "payload_hash": "h1"}
        conn.cursor.return_value.__enter__.return_value = cur

        row = worker.get_latest_schedule_version(
            conn,
            "schedule_ingest",
            user_id=8225717176,
            schedule_date=date(2026, 2, 10),
        )

        self.assertEqual(row, {"version": 4, "payload_hash": "h1"})

    def test_get_schedule_version_by_hash_returns_row(self) -> None:
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchone.return_value = {"version": 2, "payload_hash": "h2"}
        conn.cursor.return_value.__enter__.return_value = cur

        row = worker.get_schedule_version_by_hash(
            conn,
            "schedule_ingest",
            user_id=8225717176,
            schedule_date=date(2026, 2, 10),
            payload_hash="h2",
        )

        self.assertEqual(row, {"version": 2, "payload_hash": "h2"})


class AdvisoryLockTests(unittest.TestCase):
    def test_advisory_lock_key_is_stable(self) -> None:
        key_a = worker.advisory_lock_key(8225717176, date(2026, 2, 10))
        key_b = worker.advisory_lock_key(8225717176, date(2026, 2, 10))
        key_c = worker.advisory_lock_key(8225717176, date(2026, 2, 11))

        self.assertEqual(key_a, key_b)
        self.assertNotEqual(key_a, key_c)


if __name__ == "__main__":
    unittest.main()
