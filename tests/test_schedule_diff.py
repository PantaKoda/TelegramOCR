import unittest

from domain.schedule_diff import (
    ShiftAdded,
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


if __name__ == "__main__":
    unittest.main()

