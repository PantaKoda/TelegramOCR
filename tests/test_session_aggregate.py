import unittest

from domain.session_aggregate import aggregate_session_shifts
from parser.entity_identity import customer_fingerprint, location_fingerprint
from parser.semantic_normalizer import CanonicalShift


def _shift(
    *,
    start: str,
    end: str,
    customer_name: str,
    street: str,
    street_number: str,
    city: str,
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


class SessionAggregateTests(unittest.TestCase):
    def test_overlapping_screenshots_do_not_duplicate_shifts(self) -> None:
        a_1 = _shift(
            start="08:00",
            end="10:00",
            customer_name="Marie Sjoberg",
            street="Valebergsvagen",
            street_number="316",
            city="Billdal",
        )
        a_2 = _shift(
            start="08:02",
            end="10:01",
            customer_name="Marie Sjoberg",
            street="Valebergsvagen",
            street_number="316",
            city="Billdal",
        )
        b = _shift(
            start="11:00",
            end="12:00",
            customer_name="Lunch",
            street="Officegatan",
            street_number="2",
            city="Goteborg",
            shift_type="OFFICE",
        )
        c = _shift(
            start="13:00",
            end="14:00",
            customer_name="Jonas Hagenfeldt",
            street="Nordhemsgatan",
            street_number="66A",
            city="Goteborg",
        )

        result = aggregate_session_shifts([[a_1, b], [a_2, c]], schedule_date="2026-08-22")

        self.assertEqual(len(result.shifts), 3)
        merged = [item for item in result.shifts if item.shift.location_fingerprint == a_1.location_fingerprint][0]
        self.assertEqual(merged.source_count, 2)
        self.assertEqual(merged.shift.start, "08:00")
        self.assertEqual(merged.shift.end, "10:01")

    def test_partial_coverage_merges_into_full_day_union(self) -> None:
        morning = _shift(
            start="08:00",
            end="10:00",
            customer_name="A",
            street="A Street",
            street_number="1",
            city="Billdal",
        )
        noon = _shift(
            start="10:30",
            end="12:00",
            customer_name="B",
            street="B Street",
            street_number="2",
            city="Billdal",
        )
        afternoon = _shift(
            start="13:00",
            end="15:00",
            customer_name="C",
            street="C Street",
            street_number="3",
            city="Billdal",
        )
        evening = _shift(
            start="16:00",
            end="18:00",
            customer_name="D",
            street="D Street",
            street_number="4",
            city="Billdal",
        )

        result = aggregate_session_shifts([[morning, noon], [afternoon, evening]], schedule_date="2026-08-22")

        self.assertEqual(len(result.shifts), 4)
        self.assertEqual([item.shift.start for item in result.shifts], ["08:00", "10:30", "13:00", "16:00"])

    def test_small_ocr_time_jitter_is_merged(self) -> None:
        base = _shift(
            start="10:00",
            end="14:00",
            customer_name="Pia Lindkvist",
            street="Kyrkogatan",
            street_number="3",
            city="Molndal",
        )
        jitter = _shift(
            start="10:03",
            end="13:58",
            customer_name="Pia Lindkvist",
            street="Kyrkogatan",
            street_number="3",
            city="Molndal",
        )

        result = aggregate_session_shifts([[base], [jitter]], schedule_date="2026-08-22")

        self.assertEqual(len(result.shifts), 1)
        self.assertEqual(result.shifts[0].source_count, 2)
        self.assertEqual(result.shifts[0].shift.start, "10:00")
        self.assertEqual(result.shifts[0].shift.end, "14:00")

    def test_same_time_different_locations_are_not_merged(self) -> None:
        left = _shift(
            start="10:00",
            end="12:00",
            customer_name="Shift One",
            street="Left Street",
            street_number="1",
            city="Billdal",
        )
        right = _shift(
            start="10:00",
            end="12:00",
            customer_name="Shift Two",
            street="Right Street",
            street_number="9",
            city="Molndal",
        )

        result = aggregate_session_shifts([[left], [right]], schedule_date="2026-08-22")
        self.assertEqual(len(result.shifts), 2)
        self.assertTrue(all(item.source_count == 1 for item in result.shifts))

    def test_same_shift_seen_three_times_stays_single(self) -> None:
        a = _shift(
            start="15:00",
            end="17:00",
            customer_name="Anna Larsson",
            street="Parkgatan",
            street_number="12",
            city="Goteborg",
        )
        b = _shift(
            start="15:01",
            end="16:59",
            customer_name="Anna Larsson",
            street="Parkgatan",
            street_number="12",
            city="Goteborg",
        )
        c = _shift(
            start="15:00",
            end="17:00",
            customer_name="Anna Larsson",
            street="Parkgatan",
            street_number="12",
            city="Goteborg",
        )

        result = aggregate_session_shifts([[a], [b], [c]], schedule_date="2026-08-22")

        self.assertEqual(len(result.shifts), 1)
        self.assertEqual(result.shifts[0].source_count, 3)
        self.assertEqual(result.shifts[0].shift.start, "15:00")
        self.assertEqual(result.shifts[0].shift.end, "17:00")

    def test_cross_midnight_jitter_is_merged(self) -> None:
        first = _shift(
            start="23:55",
            end="00:30",
            customer_name="Night Visit",
            street="Nattgatan",
            street_number="5",
            city="Goteborg",
        )
        second = _shift(
            start="23:50",
            end="00:35",
            customer_name="Night Visit",
            street="Nattgatan",
            street_number="5",
            city="Goteborg",
        )

        result = aggregate_session_shifts([[first], [second]], schedule_date="2026-08-22")

        self.assertEqual(len(result.shifts), 1)
        self.assertEqual(result.shifts[0].source_count, 2)
        self.assertEqual(result.shifts[0].shift.start, "23:50")
        self.assertEqual(result.shifts[0].shift.end, "00:35")

    def test_partial_time_observation_is_merged_when_contained(self) -> None:
        complete = _shift(
            start="10:00",
            end="14:00",
            customer_name="Pia Lindkvist",
            street="Kyrkogatan",
            street_number="3",
            city="Molndal",
        )
        # Represents an OCR cut-off line such as "10:00-" that degrades to a point-time observation.
        partial = _shift(
            start="10:00",
            end="10:00",
            customer_name="Pia Lindkvist",
            street="Kyrkogatan",
            street_number="3",
            city="Molndal",
        )

        result = aggregate_session_shifts([[complete], [partial]], schedule_date="2026-08-22")

        self.assertEqual(len(result.shifts), 1)
        self.assertEqual(result.shifts[0].source_count, 2)
        self.assertEqual(result.shifts[0].shift.start, "10:00")
        self.assertEqual(result.shifts[0].shift.end, "14:00")

    def test_exact_time_duplicate_with_noisy_location_is_collapsed(self) -> None:
        noisy = _shift(
            start="12:30",
            end="13:30",
            customer_name="Gustaf Agrenius",
            street="Caro",
            street_number="2",
            city="Schedule Helphub Account",
            shift_type="WORK",
        )
        clean = _shift(
            start="12:30",
            end="13:30",
            customer_name="Gustaf Agrenius",
            street="Saro Sanna Dalstigen",
            street_number="9",
            city="",
            shift_type="WORK",
        )

        result = aggregate_session_shifts([[noisy], [clean]], schedule_date="2026-08-22")

        self.assertEqual(len(result.shifts), 1)
        self.assertEqual(result.shifts[0].source_count, 2)
        self.assertEqual(result.shifts[0].shift.street, "Saro Sanna Dalstigen")
        self.assertEqual(result.shifts[0].shift.street_number, "9")

    def test_exact_time_same_customer_but_different_shift_type_prefers_higher_priority(self) -> None:
        work = _shift(
            start="16:00",
            end="17:00",
            customer_name="Helena Johansson",
            street="Kullaviks Angsvag",
            street_number="16",
            city="Kullavik",
            shift_type="WORK",
        )
        training = _shift(
            start="16:00",
            end="17:00",
            customer_name="Helena Johansson",
            street="Kullaviks Angsvag",
            street_number="16",
            city="Kullavik",
            shift_type="TRAINING",
        )

        result = aggregate_session_shifts([[work], [training]], schedule_date="2026-08-22")

        self.assertEqual(len(result.shifts), 1)
        self.assertEqual(result.shifts[0].source_count, 2)
        self.assertEqual(result.shifts[0].shift.shift_type, "WORK")

    def test_activity_merge_keeps_raw_label_based_identity_fingerprint(self) -> None:
        shared_location = location_fingerprint(street="", street_number="", postal_area="", city="")
        activity_a = CanonicalShift(
            start="12:00",
            end="12:30",
            customer_name="",
            customer_fingerprint=customer_fingerprint("Restid"),
            street="",
            street_number="",
            postal_code="",
            postal_area="",
            city="",
            location_fingerprint=shared_location,
            shift_type="TRAVEL",
            raw_type_label="Restid",
        )
        activity_b = CanonicalShift(
            start="12:01",
            end="12:29",
            customer_name="",
            customer_fingerprint=customer_fingerprint("Restid"),
            street="",
            street_number="",
            postal_code="",
            postal_area="",
            city="",
            location_fingerprint=shared_location,
            shift_type="TRAVEL",
            raw_type_label="Restid",
        )

        result = aggregate_session_shifts([[activity_a], [activity_b]], schedule_date="2026-08-22")

        self.assertEqual(len(result.shifts), 1)
        self.assertEqual(result.shifts[0].source_count, 2)
        self.assertEqual(result.shifts[0].shift.customer_fingerprint, customer_fingerprint("Restid"))


if __name__ == "__main__":
    unittest.main()
