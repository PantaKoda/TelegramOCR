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

        self.assertEqual(normalized_a.city, "Mölndal")
        self.assertEqual(normalized_b.city, "Mölndal")
        self.assertEqual(normalized_a.location_fingerprint, normalized_b.location_fingerprint)
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

        self.assertEqual(normalized.street, "Valebergsvägen")
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
        self.assertEqual(normalized.city, "Göteborg")

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
            self.assertEqual(value.shift_type, baseline.shift_type)
            self.assertEqual(value.location_fingerprint, baseline.location_fingerprint)

    def test_title_bullet_and_duration_extracts_customer_and_job_type(self) -> None:
        entry = Entry(
            start="08:00",
            end="12:00",
            title="Emma Gårdmark • Storstädning 4h",
            location="",
            address="Häcklehagsvägen 1 Onsala",
        )

        normalized = normalize_entry(entry)

        self.assertEqual(normalized.customer_name, "Emma Gårdmark")
        self.assertEqual(normalized.shift_type, "WORK")
        self.assertEqual(normalized.raw_type_label, "Storstädning")

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
        self.assertEqual(normalized.raw_type_label, "Städservice")

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

    def test_leave_row_tjanstledig_del_av_dag_is_leave_type(self) -> None:
        entry = Entry(
            start="09:00",
            end="12:00",
            title="Tjänstledig del av dag",
            location="",
            address="",
        )

        normalized = normalize_entry(entry)

        self.assertEqual(normalized.customer_name, "")
        self.assertEqual(normalized.raw_type_label, "Tjänstledig Del Av Dag")
        self.assertEqual(normalized.shift_type, "LEAVE")

    def test_leave_row_sjukdom_dag_1_14_is_leave_type(self) -> None:
        entry = Entry(
            start="09:00",
            end="17:00",
            title="Sjukdom dag 1-14",
            location="",
            address="",
        )

        normalized = normalize_entry(entry)

        self.assertEqual(normalized.customer_name, "")
        self.assertEqual(normalized.raw_type_label, "Sjukdom Dag 1-14")
        self.assertEqual(normalized.shift_type, "LEAVE")

    def test_activity_label_with_trailing_counter_strips_counter(self) -> None:
        entry = Entry(
            start="12:00",
            end="12:30",
            title="Lunch 1",
            location="",
            address="",
        )

        normalized = normalize_entry(entry)

        self.assertEqual(normalized.customer_name, "")
        self.assertEqual(normalized.raw_type_label, "Lunch")
        self.assertEqual(normalized.shift_type, "BREAK")

    def test_work_type_label_with_trailing_ocr_token_is_canonicalized(self) -> None:
        entry = Entry(
            start="13:00",
            end="14:30",
            title="Lena Falk • Fonsterputs D",
            location="",
            address="Kullavik Sandlyckans Vag 104",
        )

        normalized = normalize_entry(entry)

        self.assertEqual(normalized.customer_name, "Lena Falk")
        self.assertEqual(normalized.raw_type_label, "Fönsterputs")
        self.assertEqual(normalized.shift_type, "WORK")

    def test_split_unavailable_label_rejoins_and_clears_customer_name(self) -> None:
        entry = Entry(
            start="14:30",
            end="17:00",
            title="Ej Disponibel",
            location="",
            address="",
        )

        normalized = normalize_entry(entry)

        self.assertEqual(normalized.customer_name, "")
        self.assertEqual(normalized.raw_type_label, "Ej Disponibel")
        self.assertEqual(normalized.shift_type, "UNAVAILABLE")

    def test_wrapped_reklamation_omstadning_with_inline_duration_keeps_full_type(self) -> None:
        entry = Entry(
            start="13:30",
            end="16:30",
            title="Maria Bjarsmyr · Reklamation/ ① 3h omstadning",
            location="",
            address="Kungsbacka Halgardsvagen 10",
        )

        normalized = normalize_entry(entry)

        self.assertEqual(normalized.customer_name, "Maria Bjarsmyr")
        self.assertEqual(normalized.raw_type_label, "Reklamation Omstädning")
        self.assertEqual(normalized.shift_type, "WORK")

    def test_raw_type_label_can_be_recovered_from_shifted_context_lines(self) -> None:
        entry = Entry(
            start="08:00",
            end="11:45",
            title="Frida Haagg Snellman",
            location="Stadservice",
            address="Asa Henriks Vag 16",
        )

        normalized = normalize_entry(entry)

        self.assertEqual(normalized.customer_name, "Frida Haagg Snellman")
        self.assertEqual(normalized.raw_type_label, "Städservice")
        self.assertEqual(normalized.shift_type, "WORK")

    def test_numeric_job_type_hint_falls_back_to_context_label(self) -> None:
        entry = Entry(
            start="08:00",
            end="12:00",
            title="Mattias Rondolph • 1",
            location="Stadservice",
            address="Asa Henriks Vag 2",
        )

        normalized = normalize_entry(entry)

        self.assertEqual(normalized.customer_name, "Mattias Rondolph")
        self.assertEqual(normalized.raw_type_label, "Städservice")
        self.assertEqual(normalized.shift_type, "WORK")

    def test_fuzzy_work_type_label_is_canonicalized(self) -> None:
        entry = Entry(
            start="08:00",
            end="12:00",
            title="Frida Haagg Snellman • Stadservic",
            location="",
            address="Asa Henriks Vag 16",
        )

        normalized = normalize_entry(entry)

        self.assertEqual(normalized.customer_name, "Frida Haagg Snellman")
        self.assertEqual(normalized.raw_type_label, "Städservice")
        self.assertEqual(normalized.shift_type, "WORK")

    def test_noisy_duration_token_still_extracts_clickandgo_type(self) -> None:
        entry = Entry(
            start="08:00",
            end="11:45",
            title="frida Haagg Snellman • ? 3h45m ClickAndGo",
            location="",
            address="Asa Henriks Vag 16",
        )

        normalized = normalize_entry(entry)

        self.assertEqual(normalized.customer_name, "Frida Haagg Snellman")
        self.assertEqual(normalized.raw_type_label, "ClickAndGo")
        self.assertEqual(normalized.shift_type, "WORK")

    def test_city_ascii_variant_is_canonicalized_to_swedish_spelling(self) -> None:
        entry = Entry(
            start="12:00",
            end="17:00",
            title="Jonas Hagenfeldt Stadservice 5h",
            location="KALLERED",
            address="Böletvägen 13",
        )

        normalized = normalize_entry(entry)

        self.assertEqual(normalized.city, "Kållered")


if __name__ == "__main__":
    unittest.main()
