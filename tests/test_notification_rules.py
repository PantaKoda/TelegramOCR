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


def _time_changed_event(
    *,
    event_id: str,
    source_session_id: str,
    old_start: str,
    old_end: str,
    new_start: str,
    new_end: str,
) -> dict:
    return {
        "event_id": event_id,
        "user_id": 8225717176,
        "schedule_date": "2026-08-22",
        "event_type": EVENT_TYPE_SHIFT_TIME_CHANGED,
        "location_fingerprint": "loc-1",
        "customer_fingerprint": "cust-1",
        "old_value": {
            "start": old_start,
            "end": old_end,
            "city": "Billdal",
            "shift_type": "HOME_VISIT",
            "customer_name": "Marie Sjoberg",
        },
        "new_value": {
            "start": new_start,
            "end": new_end,
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
        self.assertEqual(notifications[0].event_ids, ("e1", "e3", "e2"))

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

    def test_default_wording_is_clock_independent_absolute_date(self) -> None:
        events = [_added_event(event_id="e1", source_session_id="s1", schedule_date="2026-08-22")]

        notifications = build_notifications(events)

        self.assertEqual(len(notifications), 1)
        self.assertEqual(
            notifications[0].message,
            "New shift added on 2026-08-22 10:00–14:00 in Billdal",
        )

    def test_notification_order_is_stable(self) -> None:
        # Intentionally shuffled input order across dates and start times.
        events = [
            _added_event(event_id="e4", source_session_id="s2", schedule_date="2026-08-23"),
            _added_event(event_id="e2", source_session_id="s1", schedule_date="2026-08-22"),
            _added_event(event_id="e3", source_session_id="s1", schedule_date="2026-08-22"),
            _added_event(event_id="e1", source_session_id="s1", schedule_date="2026-08-22"),
        ]
        events[1]["new_value"]["start"] = "12:00"
        events[2]["new_value"]["start"] = "08:00"
        events[3]["new_value"]["start"] = "10:00"

        notifications = build_notifications(events, summary_threshold=10)

        self.assertEqual([item.event_ids[0] for item in notifications], ["e3", "e1", "e2", "e4"])

    def test_shift_time_changed_message_for_start_only_change(self) -> None:
        events = [
            _time_changed_event(
                event_id="e1",
                source_session_id="s1",
                old_start="10:00",
                old_end="14:00",
                new_start="11:00",
                new_end="14:00",
            )
        ]
        notifications = build_notifications(events, today=date(2026, 8, 21))
        self.assertEqual(
            notifications[0].message,
            "Tomorrow Billdal shift moved 10:00 → 11:00",
        )

    def test_shift_time_changed_message_for_end_only_change(self) -> None:
        events = [
            _time_changed_event(
                event_id="e1",
                source_session_id="s1",
                old_start="10:00",
                old_end="14:00",
                new_start="10:00",
                new_end="15:00",
            )
        ]
        notifications = build_notifications(events, today=date(2026, 8, 21))
        self.assertEqual(
            notifications[0].message,
            "Tomorrow Billdal shift moved ends 14:00 → 15:00",
        )

    def test_shift_time_changed_message_for_full_range_change(self) -> None:
        events = [
            _time_changed_event(
                event_id="e1",
                source_session_id="s1",
                old_start="10:00",
                old_end="14:00",
                new_start="11:00",
                new_end="15:00",
            )
        ]
        notifications = build_notifications(events, today=date(2026, 8, 21))
        self.assertEqual(
            notifications[0].message,
            "Tomorrow Billdal shift moved 10:00–14:00 → 11:00–15:00",
        )


if __name__ == "__main__":
    unittest.main()
