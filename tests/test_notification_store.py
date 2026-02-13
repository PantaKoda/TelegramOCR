import os
import uuid
import unittest
from datetime import date, datetime, timezone

import psycopg
from psycopg.rows import dict_row

from domain.notification_rules import UserNotification
from infra.notification_store import persist_notifications

DB_URL = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")


def _notification(notification_id: str, *, message: str = "Shift moved", event_ids: tuple[str, ...] = ("evt-1",)) -> UserNotification:
    return UserNotification(
        notification_id=notification_id,
        user_id=8225717176,
        schedule_date=date(2026, 8, 22),
        source_session_id=str(uuid.uuid4()),
        message=message,
        notification_type="event",
        event_ids=event_ids,
    )


@unittest.skipUnless(DB_URL, "Integration test requires TEST_DATABASE_URL or DATABASE_URL")
class NotificationStoreIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = f"it_notification_store_{uuid.uuid4().hex[:12]}"
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
                    CREATE TABLE {self.schema}.schedule_notification (
                        notification_id TEXT PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        schedule_date DATE NOT NULL,
                        source_session_id UUID NOT NULL,
                        notification_type TEXT NOT NULL,
                        message TEXT NOT NULL,
                        event_ids JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        sent_at TIMESTAMPTZ NULL
                    )
                    """
                )

    def _rows(self) -> list[dict]:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT notification_id, user_id, schedule_date, notification_type, message, event_ids
                    FROM {self.schema}.schedule_notification
                    ORDER BY created_at ASC, notification_id ASC
                    """
                )
                return list(cur.fetchall())

    def test_persist_notifications_inserts_rows(self) -> None:
        notifications = [
            _notification("n-1", message="New shift added", event_ids=("evt-1",)),
            _notification("n-2", message="Shift moved", event_ids=("evt-2", "evt-3")),
        ]
        created_at = datetime(2026, 8, 22, 10, 0, tzinfo=timezone.utc)

        with psycopg.connect(DB_URL) as conn:
            with conn.transaction():
                inserted = persist_notifications(
                    conn,
                    self.schema,
                    notifications=notifications,
                    created_at=created_at,
                )

        self.assertEqual(inserted, 2)
        rows = self._rows()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["notification_id"], "n-1")
        self.assertEqual(rows[1]["notification_id"], "n-2")
        self.assertEqual(rows[1]["event_ids"], ["evt-2", "evt-3"])

    def test_persist_notifications_is_idempotent_by_notification_id(self) -> None:
        item = _notification("n-1", message="Shift moved", event_ids=("evt-1",))

        with psycopg.connect(DB_URL) as conn:
            with conn.transaction():
                first = persist_notifications(conn, self.schema, notifications=[item])
            with conn.transaction():
                second = persist_notifications(conn, self.schema, notifications=[item])

        self.assertEqual(first, 1)
        self.assertEqual(second, 0)
        self.assertEqual(len(self._rows()), 1)

    def test_persist_notifications_accepts_dict_payload(self) -> None:
        notification = {
            "notification_id": "n-1",
            "user_id": 8225717176,
            "schedule_date": "2026-08-22",
            "source_session_id": str(uuid.uuid4()),
            "message": "3 shifts updated for tomorrow",
            "notification_type": "summary",
            "event_ids": ["evt-1", "evt-2", "evt-3"],
        }

        with psycopg.connect(DB_URL) as conn:
            with conn.transaction():
                inserted = persist_notifications(conn, self.schema, notifications=[notification])

        self.assertEqual(inserted, 1)
        rows = self._rows()
        self.assertEqual(rows[0]["notification_type"], "summary")
        self.assertEqual(rows[0]["event_ids"], ["evt-1", "evt-2", "evt-3"])


if __name__ == "__main__":
    unittest.main()
