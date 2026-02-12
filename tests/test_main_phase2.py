import unittest
from datetime import date
from unittest.mock import MagicMock

import main as worker


class InsertStubScheduleVersionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.session = worker.ClaimedSession(id="session-1", user_id=8225717176)

    def test_insert_stub_schedule_version_success_executes_insert(self) -> None:
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur

        worker.insert_stub_schedule_version(
            conn,
            "schedule_ingest",
            session=self.session,
            schedule_date=date(2099, 1, 1),
            version=1,
            payload={"stub": True},
            payload_hash="abc123",
        )

        cur.execute.assert_called_once()
        _, params = cur.execute.call_args[0]
        self.assertEqual(params[0], self.session.user_id)
        self.assertEqual(params[1], date(2099, 1, 1))
        self.assertEqual(params[2], 1)
        self.assertEqual(params[3], self.session.id)
        self.assertEqual(params[4], '{"stub":true}')
        self.assertEqual(params[5], "abc123")

    def test_insert_stub_schedule_version_rejects_bad_version(self) -> None:
        conn = MagicMock()

        with self.assertRaises(ValueError):
            worker.insert_stub_schedule_version(
                conn,
                "schedule_ingest",
                session=self.session,
                schedule_date=date(2099, 1, 1),
                version=2,
                payload={"stub": True},
                payload_hash="abc123",
            )

        conn.cursor.assert_not_called()

    def test_insert_stub_schedule_version_rejects_missing_payload(self) -> None:
        conn = MagicMock()

        with self.assertRaises(ValueError):
            worker.insert_stub_schedule_version(
                conn,
                "schedule_ingest",
                session=self.session,
                schedule_date=date(2099, 1, 1),
                version=1,
                payload=None,
                payload_hash="abc123",
            )

        conn.cursor.assert_not_called()


if __name__ == "__main__":
    unittest.main()
