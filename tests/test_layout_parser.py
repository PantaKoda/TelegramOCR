import hashlib
import json
import random
import unittest
from dataclasses import asdict

from parser.layout_parser import Box, parse_layout


class LayoutParserTests(unittest.TestCase):
    def assert_layout(self, boxes: list[Box], expected: list[dict]) -> None:
        parsed = [asdict(entry) for entry in parse_layout(boxes)]
        self.assertEqual(parsed, expected)

        shuffled = list(boxes)
        random.Random(17).shuffle(shuffled)
        shuffled_parsed = [asdict(entry) for entry in parse_layout(shuffled)]
        self.assertEqual(shuffled_parsed, expected)

        jittered = self.jitter_boxes(boxes)
        jittered_parsed = [asdict(entry) for entry in parse_layout(jittered)]
        self.assertEqual(jittered_parsed, expected)

    def jitter_boxes(self, boxes: list[Box]) -> list[Box]:
        result: list[Box] = []
        for index, box in enumerate(boxes):
            dx = 1 if index % 2 == 0 else -1
            dy = -1 if index % 3 == 0 else 1
            result.append(
                Box(
                    text=box.text,
                    x=box.x + dx,
                    y=box.y + dy,
                    w=box.w,
                    h=box.h,
                )
            )
        return result

    def test_single_card(self) -> None:
        boxes = [
            Box(text="10:00-14:00", x=118, y=240, w=120, h=22),
            Box(text="Marie Sjoberg", x=120, y=210, w=180, h=22),
            Box(text="Valebergsvagen 316", x=130, y=270, w=220, h=22),
            Box(text="Billdal", x=132, y=295, w=120, h=22),
        ]
        expected = [
            {
                "start": "10:00",
                "end": "14:00",
                "title": "Marie Sjoberg",
                "location": "Billdal",
                "address": "Valebergsvagen 316",
            }
        ]
        self.assert_layout(boxes, expected)

    def test_multiple_cards_stacked(self) -> None:
        boxes = [
            Box(text="10:00-14:00", x=100, y=200, w=120, h=22),
            Box(text="Marie Sjoberg", x=104, y=228, w=180, h=22),
            Box(text="Valebergsvagen 316", x=108, y=255, w=220, h=22),
            Box(text="Billdal", x=112, y=282, w=120, h=22),
            Box(text="16.30-20.00", x=100, y=365, w=120, h=22),
            Box(text="Anna Larsson", x=104, y=392, w=180, h=22),
            Box(text="Kungsgatan 4", x=108, y=418, w=170, h=22),
            Box(text="Goteborg", x=112, y=444, w=140, h=22),
        ]
        expected = [
            {
                "start": "10:00",
                "end": "14:00",
                "title": "Marie Sjoberg",
                "location": "Billdal",
                "address": "Valebergsvagen 316",
            },
            {
                "start": "16:30",
                "end": "20:00",
                "title": "Anna Larsson",
                "location": "Goteborg",
                "address": "Kungsgatan 4",
            },
        ]
        self.assert_layout(boxes, expected)

    def test_address_wrapped_across_lines(self) -> None:
        boxes = [
            Box(text="09:00-13:00", x=100, y=220, w=120, h=22),
            Box(text="Karl Nyberg", x=100, y=246, w=170, h=22),
            Box(text="Storgatan", x=104, y=273, w=120, h=22),
            Box(text="12A", x=225, y=274, w=40, h=22),
            Box(text="3 tr", x=104, y=299, w=60, h=22),
            Box(text="Munkedal", x=104, y=326, w=120, h=22),
        ]
        expected = [
            {
                "start": "09:00",
                "end": "13:00",
                "title": "Karl Nyberg",
                "location": "Munkedal",
                "address": "Storgatan 12A 3 tr",
            }
        ]
        self.assert_layout(boxes, expected)

    def test_header_top_is_ignored(self) -> None:
        boxes = [
            Box(text="Schedule Week 7", x=70, y=16, w=220, h=20),
            Box(text="Tuesday", x=74, y=42, w=110, h=20),
            Box(text="11:00-15:00", x=100, y=210, w=120, h=22),
            Box(text="Erik Hammar", x=102, y=238, w=170, h=22),
            Box(text="Havsvagen 1", x=104, y=265, w=160, h=22),
            Box(text="Saro", x=104, y=292, w=100, h=22),
        ]
        expected = [
            {
                "start": "11:00",
                "end": "15:00",
                "title": "Erik Hammar",
                "location": "Saro",
                "address": "Havsvagen 1",
            }
        ]
        self.assert_layout(boxes, expected)

    def test_slightly_staggered_x_positions(self) -> None:
        boxes = [
            Box(text="12:00-18:00", x=118, y=230, w=122, h=22),
            Box(text="Lina", x=121, y=257, w=60, h=22),
            Box(text="Svensson", x=190, y=258, w=120, h=22),
            Box(text="Avenyn", x=130, y=285, w=90, h=22),
            Box(text="22", x=228, y=284, w=30, h=22),
            Box(text="Molndal", x=132, y=311, w=120, h=22),
        ]
        expected = [
            {
                "start": "12:00",
                "end": "18:00",
                "title": "Lina Svensson",
                "location": "Molndal",
                "address": "Avenyn 22",
            }
        ]
        self.assert_layout(boxes, expected)

    def test_landscape_two_columns(self) -> None:
        boxes = [
            Box(text="08:00-12:00", x=80, y=210, w=130, h=22),
            Box(text="Mikael Ek", x=84, y=238, w=140, h=22),
            Box(text="Ringvagen 5", x=86, y=266, w=150, h=22),
            Box(text="Kungsbacka", x=88, y=292, w=140, h=22),
            Box(text="13:00-17:00", x=620, y=212, w=130, h=22),
            Box(text="Sofia Lind", x=624, y=238, w=140, h=22),
            Box(text="Parkgatan 9", x=626, y=266, w=150, h=22),
            Box(text="Lindome", x=628, y=292, w=120, h=22),
        ]
        expected = [
            {
                "start": "08:00",
                "end": "12:00",
                "title": "Mikael Ek",
                "location": "Kungsbacka",
                "address": "Ringvagen 5",
            },
            {
                "start": "13:00",
                "end": "17:00",
                "title": "Sofia Lind",
                "location": "Lindome",
                "address": "Parkgatan 9",
            },
        ]
        self.assert_layout(boxes, expected)

    def test_floating_orphan_title_above_time(self) -> None:
        boxes = [
            Box(text="Marie Sjoberg", x=110, y=206, w=170, h=22),
            Box(text="Stadservice", x=112, y=232, w=130, h=22),
            Box(text="10:00-14:00", x=108, y=260, w=130, h=22),
            Box(text="Valebergsvagen 316", x=114, y=288, w=200, h=22),
            Box(text="Billdal", x=116, y=315, w=120, h=22),
        ]
        expected = [
            {
                "start": "10:00",
                "end": "14:00",
                "title": "Marie Sjoberg Stadservice",
                "location": "Billdal",
                "address": "Valebergsvagen 316",
            }
        ]
        self.assert_layout(boxes, expected)

    def test_two_time_ranges_in_single_card_create_two_entries(self) -> None:
        boxes = [
            Box(text="10:00-12:00", x=100, y=210, w=130, h=22),
            Box(text="Lunch break", x=104, y=238, w=150, h=22),
            Box(text="13:00-17:00", x=100, y=267, w=130, h=22),
            Box(text="Office", x=104, y=295, w=90, h=22),
            Box(text="Molndal", x=106, y=322, w=100, h=22),
        ]
        expected = [
            {
                "start": "10:00",
                "end": "12:00",
                "title": "Lunch break",
                "location": "",
                "address": "",
            },
            {
                "start": "13:00",
                "end": "17:00",
                "title": "Office",
                "location": "Molndal",
                "address": "",
            },
        ]
        self.assert_layout(boxes, expected)

    def test_repeated_shuffle_and_jitter_is_stable(self) -> None:
        boxes = [
            Box(text="10:00-14:00", x=100, y=200, w=130, h=22),
            Box(text="Anna Lind", x=104, y=228, w=150, h=22),
            Box(text="Storgatan 4", x=108, y=255, w=170, h=22),
            Box(text="Goteborg", x=110, y=283, w=120, h=22),
            Box(text="15:00-19:00", x=100, y=365, w=130, h=22),
            Box(text="Mikael Ek", x=104, y=392, w=140, h=22),
            Box(text="Ringvagen 5", x=108, y=419, w=170, h=22),
            Box(text="Kungsbacka", x=110, y=446, w=130, h=22),
        ]
        baseline = [asdict(entry) for entry in parse_layout(boxes)]
        baseline_hash = hashlib.sha256(json.dumps(baseline, sort_keys=True).encode("utf-8")).hexdigest()

        for seed in range(50):
            rng = random.Random(seed)
            variant = [
                Box(
                    text=box.text,
                    x=box.x + rng.choice([-1, 0, 1]),
                    y=box.y + rng.choice([-1, 0, 1]),
                    w=box.w,
                    h=box.h,
                )
                for box in boxes
            ]
            rng.shuffle(variant)
            parsed = [asdict(entry) for entry in parse_layout(variant)]
            parsed_hash = hashlib.sha256(json.dumps(parsed, sort_keys=True).encode("utf-8")).hexdigest()
            self.assertEqual(parsed_hash, baseline_hash)

    def test_stacked_time_lines_and_ui_noise_are_parsed_correctly(self) -> None:
        boxes = [
            Box(text="08:00", x=38, y=210, w=60, h=22),
            Box(text="12:00", x=40, y=236, w=60, h=22),
            Box(text="On time", x=42, y=262, w=80, h=22),
            Box(text="Pia Lindkvist Stadservice", x=118, y=238, w=250, h=24),
            Box(text="LINDOME, Diabasvagen 7", x=120, y=268, w=250, h=24),
            Box(text="Collaborators", x=124, y=296, w=130, h=22),
            Box(text="12:00", x=38, y=390, w=60, h=22),
            Box(text="15:45", x=40, y=416, w=60, h=22),
            Box(text="Jonas Hagenfeldt Stadservice", x=118, y=418, w=290, h=24),
            Box(text="KALLERED, Stenmursvagen 44", x=120, y=448, w=300, h=24),
            Box(text="Collaborators +3", x=124, y=476, w=170, h=22),
        ]
        expected = [
            {
                "start": "08:00",
                "end": "12:00",
                "title": "Pia Lindkvist Stadservice",
                "location": "",
                "address": "LINDOME, Diabasvagen 7",
            },
            {
                "start": "12:00",
                "end": "15:45",
                "title": "Jonas Hagenfeldt Stadservice",
                "location": "",
                "address": "KALLERED, Stenmursvagen 44",
            },
        ]
        self.assert_layout(boxes, expected)

    def test_stacked_times_allow_noise_line_between_start_and_end(self) -> None:
        boxes = [
            Box(text="08:00", x=38, y=210, w=60, h=22),
            Box(text="Emma Gardmark • Storstadning 4h", x=118, y=210, w=320, h=24),
            Box(text="On time", x=42, y=224, w=80, h=20),
            Box(text="12:00", x=40, y=238, w=60, h=22),
            Box(text="Onsala, Hacklehagsvagen 1", x=120, y=268, w=320, h=24),
            Box(text="Collaborators +3", x=124, y=296, w=170, h=22),
        ]
        expected = [
            {
                "start": "08:00",
                "end": "12:00",
                "title": "Emma Gardmark • Storstadning 4h",
                "location": "",
                "address": "Onsala, Hacklehagsvagen 1",
            }
        ]
        self.assert_layout(boxes, expected)

    def test_stacked_times_keep_type_line_pushed_right_of_long_customer_name(self) -> None:
        boxes = [
            Box(text="08:00", x=38, y=210, w=60, h=22),
            Box(text="11:45", x=40, y=238, w=60, h=22),
            Box(text="Frida Haagg Snellman", x=118, y=238, w=260, h=24),
            Box(text="Stadservice", x=320, y=266, w=130, h=24),
            Box(text="Asa Henriks Vag 16", x=120, y=296, w=220, h=24),
            Box(text="Collaborators +2", x=124, y=324, w=170, h=22),
        ]
        expected = [
            {
                "start": "08:00",
                "end": "11:45",
                "title": "Frida Haagg Snellman Stadservice",
                "location": "",
                "address": "Asa Henriks Vag 16",
            }
        ]
        self.assert_layout(boxes, expected)

    def test_stacked_times_keep_type_line_when_numeric_noise_precedes_it(self) -> None:
        boxes = [
            Box(text="08:00", x=38, y=210, w=60, h=22),
            Box(text="12:00", x=40, y=238, w=60, h=22),
            Box(text="Mattias Rondolph", x=118, y=238, w=240, h=24),
            Box(text="1", x=290, y=266, w=20, h=22),
            Box(text="Stadservice", x=320, y=266, w=130, h=24),
            Box(text="Asa Henriks Vag 2", x=120, y=296, w=220, h=24),
        ]
        expected = [
            {
                "start": "08:00",
                "end": "12:00",
                "title": "Mattias Rondolph Stadservice",
                "location": "",
                "address": "Asa Henriks Vag 2",
            }
        ]
        self.assert_layout(boxes, expected)

    def test_stacked_times_keep_type_line_when_duration_suffix_present(self) -> None:
        boxes = [
            Box(text="08:00", x=38, y=210, w=60, h=22),
            Box(text="12:00", x=40, y=238, w=60, h=22),
            Box(text="Mattias Rondolph", x=118, y=238, w=240, h=24),
            Box(text="Stadservice 4h", x=320, y=266, w=150, h=24),
            Box(text="Asa Henriks Vag 2", x=120, y=296, w=220, h=24),
        ]
        expected = [
            {
                "start": "08:00",
                "end": "12:00",
                "title": "Mattias Rondolph Stadservice 4h",
                "location": "",
                "address": "Asa Henriks Vag 2",
            }
        ]
        self.assert_layout(boxes, expected)

    def test_stacked_times_keep_type_from_end_time_line_when_customer_row_has_noise(self) -> None:
        boxes = [
            Box(text="08:00 frida Haagg Snellman • ? 3h45m", x=36, y=473, w=360, h=24),
            Box(text="11:45 ClickAndGo", x=38, y=507, w=170, h=24),
            Box(text="Asa, Henriks vag 16", x=193, y=540, w=240, h=24),
            Box(text="Collaborators +2", x=480, y=575, w=170, h=22),
        ]
        expected = [
            {
                "start": "08:00",
                "end": "11:45",
                "title": "frida Haagg Snellman • ? 3h45m ClickAndGo",
                "location": "",
                "address": "Asa, Henriks vag 16",
            }
        ]
        self.assert_layout(boxes, expected)

    def test_stacked_times_keep_fuzzy_type_line_with_small_ocr_typo(self) -> None:
        boxes = [
            Box(text="08:00", x=38, y=210, w=60, h=22),
            Box(text="12:00", x=40, y=238, w=60, h=22),
            Box(text="Frida Haagg Snellman", x=118, y=238, w=260, h=24),
            Box(text="Stadservic", x=320, y=266, w=130, h=24),
            Box(text="Asa Henriks Vag 16", x=120, y=296, w=220, h=24),
        ]
        expected = [
            {
                "start": "08:00",
                "end": "12:00",
                "title": "Frida Haagg Snellman Stadservic",
                "location": "",
                "address": "Asa Henriks Vag 16",
            }
        ]
        self.assert_layout(boxes, expected)

    def test_address_line_with_trailing_collaborators_suffix_is_preserved(self) -> None:
        boxes = [
            Box(text="08:00", x=38, y=210, w=60, h=22),
            Box(text="09:20", x=40, y=238, w=60, h=22),
            Box(text="Catarina Berne Bjornhede Stadservice", x=118, y=238, w=360, h=24),
            Box(text="KULLAVIK, Sjostigen 8 Collaborators +3", x=120, y=268, w=380, h=24),
        ]
        expected = [
            {
                "start": "08:00",
                "end": "09:20",
                "title": "Catarina Berne Bjornhede Stadservice",
                "location": "",
                "address": "KULLAVIK, Sjostigen 8",
            }
        ]
        self.assert_layout(boxes, expected)


if __name__ == "__main__":
    unittest.main()
