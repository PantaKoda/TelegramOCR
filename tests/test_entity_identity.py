import unittest

from parser.entity_identity import customer_fingerprint, location_fingerprint


class EntityIdentityTests(unittest.TestCase):
    def test_address_variants_share_same_location_fingerprint(self) -> None:
        variants = [
            {
                "street": "Valebergsvägen",
                "street_number": "316",
                "postal_area": "",
                "city": "Billdal",
            },
            {
                "street": "VALEBERGSVAGEN",
                "street_number": "316",
                "postal_area": "",
                "city": "BILLDAL",
            },
            {
                "street": "valebergsvagen",
                "street_number": " 316 ",
                "postal_area": "",
                "city": "billdal",
            },
            {
                "street": "Valebergsvagen",
                "street_number": "3I6",
                "postal_area": "",
                "city": "Billdal",
            },
            {
                "street": "Vålebergsvägen",
                "street_number": "316",
                "postal_area": "",
                "city": "Bi11dal",
            },
        ]

        values = [location_fingerprint(**item) for item in variants]
        self.assertEqual(len(set(values)), 1)

    def test_city_typo_confusion_keeps_same_location_fingerprint(self) -> None:
        baseline = location_fingerprint(
            street="Kyrkogatan",
            street_number="3",
            postal_area="",
            city="Billdal",
        )
        typo = location_fingerprint(
            street="Kyrkogatan",
            street_number="3",
            postal_area="",
            city="BiIldal",
        )
        self.assertEqual(baseline, typo)

    def test_different_address_has_different_location_fingerprint(self) -> None:
        a = location_fingerprint(
            street="Kyrkogatan",
            street_number="3",
            postal_area="",
            city="Billdal",
        )
        b = location_fingerprint(
            street="Kyrkogatan",
            street_number="5",
            postal_area="",
            city="Billdal",
        )
        self.assertNotEqual(a, b)

    def test_same_place_different_customer_spelling(self) -> None:
        place_a = location_fingerprint(
            street="Valebergsvagen",
            street_number="316",
            postal_area="",
            city="Billdal",
        )
        place_b = location_fingerprint(
            street="Valebergsvägen",
            street_number="316",
            postal_area="",
            city="Bi11dal",
        )
        self.assertEqual(place_a, place_b)

        customer_a = customer_fingerprint("Marie Sjöberg Städservice")
        customer_b = customer_fingerprint("Sjöberg M.")
        self.assertEqual(customer_a, customer_b)

        other_customer = customer_fingerprint("Jonas Hagenfeldt")
        self.assertNotEqual(customer_a, other_customer)


if __name__ == "__main__":
    unittest.main()

