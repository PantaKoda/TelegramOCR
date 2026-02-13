import unittest
from datetime import date

from domain.notification_rules import (
    EVENT_TYPE_SHIFT_ADDED,
    EVENT_TYPE_SHIFT_TIME_CHANGED,
    build_notifications,
)


def _added_event(*, event_id: str, source_session_id: str, schedule_date: str = "2026-08-22") -> dict:
    return {
        "event_id": event_id,
        "user_id": 8225717176,
        "schedule_date": schedule_date,
        "event_type": EVENT_TYPE_SHIFT_ADDED,
        "location_fingerprint": "loc-1",
        "customer_fingerprint": "cust-1",
        "old_value": None,
        "new_value": {
            "start": "10:00",
            "end": "14:00",
            "city": "Billdal",
            "shift_type": "HOME_VISIT",
            "customer_name": "Marie Sjoberg",
        },
        "source_session_id": source_session_id,
    }


class NotificationRulesTests(unittest.TestCase):
    def test_single_event_returns_single_message(self) -> None:
        events = [_added_event(event_id="e1", source_session_id="s1")]

        notifications = build_notifications(events, today=date(2026, 8, 21))

        self.assertEqual(len(notifications), 1)
        self.assertEqual(
            notifications[0].message,
            "New shift added tomorrow 10:00–14:00 in Billdal",
        )
        self.assertEqual(notifications[0].notification_type, "event")
        self.assertEqual(notifications[0].event_ids, ("e1",))

    def test_multiple_same_day_session_events_emit_summary(self) -> None:
        events = [
            _added_event(event_id="e1", source_session_id="s1"),
            {
                **_added_event(event_id="e2", source_session_id="s1"),
                "event_type": EVENT_TYPE_SHIFT_TIME_CHANGED,
                "old_value": {
                    "start": "10:00",
                    "end": "14:00",
                    "city": "Billdal",
                    "shift_type": "HOME_VISIT",
                    "customer_name": "Marie Sjoberg",
                },
            },
            _added_event(event_id="e3", source_session_id="s1"),
        ]

        notifications = build_notifications(events, today=date(2026, 8, 21), summary_threshold=3)

        self.assertEqual(len(notifications), 1)
        self.assertEqual(notifications[0].notification_type, "summary")
        self.assertEqual(notifications[0].message, "3 shifts updated for tomorrow")
        self.assertEqual(notifications[0].event_ids, ("e1", "e2", "e3"))

    def test_no_events_returns_no_notifications(self) -> None:
        notifications = build_notifications([], today=date(2026, 8, 21))
        self.assertEqual(notifications, [])

    def test_replay_same_events_produces_no_duplicates(self) -> None:
        events = [_added_event(event_id="e1", source_session_id="s1")]
        seen: set[str] = set()

        first = build_notifications(events, today=date(2026, 8, 21), already_notified_event_ids=seen)
        second = build_notifications(events, today=date(2026, 8, 21), already_notified_event_ids=seen)

        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 0)
        self.assertIn("e1", seen)


if __name__ == "__main__":
    unittest.main()

