import unittest

from domain.schedule_diff import (
    ShiftAdded,
    ShiftReclassified,
    ShiftRelocated,
    ShiftRemoved,
    ShiftRetitled,
    ShiftTimeChanged,
    diff_schedules,
)
from parser.entity_identity import customer_fingerprint, location_fingerprint
from parser.semantic_normalizer import CanonicalShift


def _shift(
    *,
    start: str = "10:00",
    end: str = "14:00",
    customer_name: str = "Marie Sjoberg",
    street: str = "Valebergsvagen",
    street_number: str = "316",
    city: str = "Billdal",
    postal_area: str = "",
    postal_code: str = "",
    shift_type: str = "HOME_VISIT",
) -> CanonicalShift:
    return CanonicalShift(
        start=start,
        end=end,
        customer_name=customer_name,
        customer_fingerprint=customer_fingerprint(customer_name),
        street=street,
        street_number=street_number,
        postal_code=postal_code,
        postal_area=postal_area,
        city=city,
        location_fingerprint=location_fingerprint(
            street=street,
            street_number=street_number,
            postal_area=postal_area,
            city=city,
        ),
        shift_type=shift_type,
    )


class ScheduleDiffTests(unittest.TestCase):
    def test_shift_added(self) -> None:
        old = [_shift()]
        added = _shift(start="15:00", end="18:00", customer_name="Jonas Hagenfeldt")
        new = [old[0], added]

        events = diff_schedules(old, new, schedule_date="2026-08-22")

        self.assertEqual(len(events), 1)
        self.assertIsInstance(events[0], ShiftAdded)
        self.assertEqual(events[0].shift, added)

    def test_shift_removed(self) -> None:
        kept = _shift()
        removed = _shift(start="15:00", end="18:00", customer_name="Jonas Hagenfeldt")
        old = [kept, removed]
        new = [kept]

        events = diff_schedules(old, new, schedule_date="2026-08-22")

        self.assertEqual(len(events), 1)
        self.assertIsInstance(events[0], ShiftRemoved)
        self.assertEqual(events[0].shift, removed)

    def test_shift_time_changed(self) -> None:
        before = _shift(start="10:00", end="14:00")
        after = _shift(start="11:00", end="15:00")

        events = diff_schedules([before], [after], schedule_date="2026-08-22")

        self.assertEqual(len(events), 1)
        self.assertIsInstance(events[0], ShiftTimeChanged)
        self.assertEqual(events[0].before, before)
        self.assertEqual(events[0].after, after)

    def test_shift_relocated(self) -> None:
        before = _shift(street="Valebergsvagen", street_number="316", city="Billdal")
        after = _shift(street="Nordhemsgatan", street_number="66A", city="Goteborg")

        events = diff_schedules([before], [after], schedule_date="2026-08-22")

        self.assertEqual(len(events), 1)
        self.assertIsInstance(events[0], ShiftRelocated)
        self.assertEqual(events[0].before, before)
        self.assertEqual(events[0].after, after)

    def test_customer_renamed_same_place_is_retitled(self) -> None:
        before = _shift(customer_name="Marie Sjoberg", street="Valebergsvagen", street_number="316", city="Billdal")
        after = _shift(customer_name="Sara Andersson", street="Valebergsvagen", street_number="316", city="Billdal")

        events = diff_schedules([before], [after], schedule_date="2026-08-22")

        self.assertEqual(len(events), 1)
        self.assertIsInstance(events[0], ShiftRetitled)
        self.assertEqual(events[0].before, before)
        self.assertEqual(events[0].after, after)

    def test_reorder_only_produces_no_events(self) -> None:
        a = _shift(start="08:00", end="11:00", customer_name="Pia Lindkvist")
        b = _shift(start="12:00", end="14:00", customer_name="Jonas Hagenfeldt")
        c = _shift(start="15:00", end="19:00", customer_name="Anna Larsson")

        old = [a, b, c]
        new = [c, a, b]

        events = diff_schedules(old, new, schedule_date="2026-08-22")
        self.assertEqual(events, [])

    def test_duplicate_identity_with_one_time_move_is_time_changed_not_add_remove(self) -> None:
        old = [
            _shift(start="08:00", end="10:00", customer_name="Marie Sjoberg", street="Valebergsvagen"),
            _shift(start="15:00", end="17:00", customer_name="Marie Sjoberg", street="Valebergsvagen"),
        ]
        new = [
            _shift(start="09:00", end="11:00", customer_name="Marie Sjoberg", street="Valebergsvagen"),
            _shift(start="15:00", end="17:00", customer_name="Marie Sjoberg", street="Valebergsvagen"),
        ]

        events = diff_schedules(old, new, schedule_date="2026-08-22")

        self.assertEqual(len(events), 1)
        self.assertIsInstance(events[0], ShiftTimeChanged)
        self.assertEqual(events[0].before.start, "08:00")
        self.assertEqual(events[0].before.end, "10:00")
        self.assertEqual(events[0].after.start, "09:00")
        self.assertEqual(events[0].after.end, "11:00")

    def test_shift_reclassified_when_type_changes_only(self) -> None:
        before = _shift(
            start="10:00",
            end="12:00",
            customer_name="Office Shift",
            street="Kontorsgatan",
            street_number="8",
            city="Molndal",
            shift_type="OFFICE",
        )
        after = _shift(
            start="10:00",
            end="12:00",
            customer_name="Office Shift",
            street="Kontorsgatan",
            street_number="8",
            city="Molndal",
            shift_type="HOME_VISIT",
        )

        events = diff_schedules([before], [after], schedule_date="2026-08-22")

        self.assertEqual(len(events), 1)
        self.assertIsInstance(events[0], ShiftReclassified)
        self.assertEqual(events[0].before.shift_type, "OFFICE")
        self.assertEqual(events[0].after.shift_type, "HOME_VISIT")


if __name__ == "__main__":
    unittest.main()
