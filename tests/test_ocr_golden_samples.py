import json
import os
import unittest
from dataclasses import asdict
from pathlib import Path

from ocr.paddle_adapter import create_paddle_ocr, ensure_paddle_available, run_paddle_on_image
from parser.layout_parser import parse_layout

SAMPLES_DIR = Path(__file__).resolve().parent / "ocr_samples"


def _sample_names() -> list[str]:
    names: list[str] = []
    for candidate in sorted(SAMPLES_DIR.glob("sample*.png")):
        expected = candidate.with_suffix(".expected.json")
        if expected.exists():
            names.append(candidate.stem)
    return names


@unittest.skipUnless(SAMPLES_DIR.exists(), "OCR golden samples directory is missing")
class OCRGoldenSamplesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ensure_paddle_available()
        os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")
        cls.ocr = create_paddle_ocr()
        cls.samples = _sample_names()
        if not cls.samples:
            raise unittest.SkipTest("No OCR sample PNG + expected JSON pairs found.")

    def test_image_to_entries_matches_expected(self) -> None:
        for name in self.samples:
            with self.subTest(sample=name):
                image = SAMPLES_DIR / f"{name}.png"
                expected_path = SAMPLES_DIR / f"{name}.expected.json"
                expected = json.loads(expected_path.read_text(encoding="utf-8"))

                boxes = run_paddle_on_image(image, ocr=self.ocr)
                entries = [asdict(entry) for entry in parse_layout(boxes)]

                self.assertGreater(len(boxes), 0)
                self.assertEqual(entries, expected)


if __name__ == "__main__":
    unittest.main()

