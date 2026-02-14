import unittest

from parser.layout_parser import Entry
from parser.semantic_normalizer import normalize_entry


class SemanticNormalizerTests(unittest.TestCase):
    def test_accent_loss_normalizes_to_same_city(self) -> None:
        with_accent = Entry(
            start="08:00",
            end="12:00",
            title="Pia Lindkvist Städservice",
            location="Mölndal",
            address="Kyrkogatan 3",
        )
        without_accent = Entry(
            start="08:00",
            end="12:00",
            title="PIA LINDKVIST STADSERVICE",
            location="MOLNDAL",
            address="Kyrkogatan 3",
        )

        normalized_a = normalize_entry(with_accent)
        normalized_b = normalize_entry(without_accent)

        self.assertEqual(normalized_a.city, "Molndal")
        self.assertEqual(normalized_a.city, normalized_b.city)
        self.assertEqual(normalized_a.customer_name, "Pia Lindkvist")
        self.assertEqual(normalized_b.customer_name, "Pia Lindkvist")

    def test_missing_postal_code_is_supported(self) -> None:
        entry = Entry(
            start="10:00",
            end="14:00",
            title="Marie Sjöberg",
            location="",
            address="Valebergsvägen 316 Billdal",
        )

        normalized = normalize_entry(entry)

        self.assertEqual(normalized.street, "Valebergsvagen")
        self.assertEqual(normalized.street_number, "316")
        self.assertEqual(normalized.postal_code, "")
        self.assertEqual(normalized.city, "Billdal")
        self.assertEqual(normalized.shift_type, "WORK")

    def test_multiline_address_join_is_decomposed(self) -> None:
        entry = Entry(
            start="11:00",
            end="15:00",
            title="Anna Larsson",
            location="Göteborg",
            address="Storgatan\n12A",
        )

        normalized = normalize_entry(entry)

        self.assertEqual(normalized.street, "Storgatan")
        self.assertEqual(normalized.street_number, "12A")
        self.assertEqual(normalized.city, "Goteborg")

    def test_noisy_ocr_l_vs_i_is_normalized(self) -> None:
        clean = Entry(
            start="09:00",
            end="13:00",
            title="Pia Lindkvist",
            location="Billdal",
            address="Kyrkogatan 3",
        )
        noisy = Entry(
            start="09:00",
            end="13:00",
            title="Pia L1ndkv1st",
            location="BIlldal",
            address="Kyrkogatan 3",
        )

        normalized_clean = normalize_entry(clean)
        normalized_noisy = normalize_entry(noisy)

        self.assertEqual(normalized_clean.customer_name, normalized_noisy.customer_name)
        self.assertEqual(normalized_clean.city, normalized_noisy.city)

    def test_same_location_variants_produce_same_canonical_fields(self) -> None:
        variants = [
            Entry(
                start="12:00",
                end="16:00",
                title="Office Shift",
                location="",
                address="Kyrkogatan 3 43137 MOLNDAL",
            ),
            Entry(
                start="12:00",
                end="16:00",
                title="Office Shift",
                location="",
                address="Kyrkogatan 3 431 37 Mölndal",
            ),
            Entry(
                start="12:00",
                end="16:00",
                title="Office Shift",
                location="",
                address="Kyrkogatan 3 43137 Molndal",
            ),
        ]

        normalized = [normalize_entry(entry) for entry in variants]
        baseline = normalized[0]

        for value in normalized[1:]:
            self.assertEqual(value.street, baseline.street)
            self.assertEqual(value.street_number, baseline.street_number)
            self.assertEqual(value.postal_code, baseline.postal_code)
            self.assertEqual(value.postal_area, baseline.postal_area)
            self.assertEqual(value.city, baseline.city)
            self.assertEqual(value.shift_type, baseline.shift_type)

    def test_title_bullet_and_duration_extracts_customer_and_job_type(self) -> None:
        entry = Entry(
            start="08:00",
            end="12:00",
            title="Emma Gårdmark • Storstädning 4h",
            location="",
            address="Häcklehagsvägen 1 Onsala",
        )

        normalized = normalize_entry(entry)

        self.assertEqual(normalized.customer_name, "Emma Gardmark")
        self.assertEqual(normalized.shift_type, "WORK")
        self.assertEqual(normalized.raw_type_label, "Storstadning")

    def test_trailing_job_type_without_bullet_extracts_customer(self) -> None:
        entry = Entry(
            start="12:00",
            end="17:00",
            title="Jonas Hagenfeldt Stadservice 5h",
            location="",
            address="Stenmursvagen 44 Kallered",
        )

        normalized = normalize_entry(entry)

        self.assertEqual(normalized.customer_name, "Jonas Hagenfeldt")
        self.assertEqual(normalized.shift_type, "WORK")
        self.assertEqual(normalized.raw_type_label, "Stadservice")

    def test_activity_row_populates_raw_type_and_not_customer(self) -> None:
        entry = Entry(
            start="13:00",
            end="13:15",
            title="Lunch 15m",
            location="",
            address="",
        )

        normalized = normalize_entry(entry)

        self.assertEqual(normalized.customer_name, "")
        self.assertEqual(normalized.raw_type_label, "Lunch")
        self.assertEqual(normalized.shift_type, "BREAK")

    def test_training_row_without_customer_is_training_type(self) -> None:
        entry = Entry(
            start="16:00",
            end="17:00",
            title="Utbildning",
            location="",
            address="",
        )

        normalized = normalize_entry(entry)

        self.assertEqual(normalized.customer_name, "")
        self.assertEqual(normalized.raw_type_label, "Utbildning")
        self.assertEqual(normalized.shift_type, "TRAINING")


if __name__ == "__main__":
    unittest.main()
