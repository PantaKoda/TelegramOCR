import json
import os
import re
import tempfile
import unittest
from dataclasses import asdict
from hashlib import sha256
from pathlib import Path

from PIL import Image

from ocr.paddle_adapter import create_paddle_ocr, ensure_paddle_available, run_paddle_on_image
from parser.layout_parser import parse_layout

SAMPLES_DIR = Path(__file__).resolve().parent / "ocr_samples"
TIME_RANGE_RE = re.compile(r"\b\d{1,2}[:.]\d{2}(?:\s*-\s*\d{1,2}[:.]\d{2})?\b")


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

    def test_scaled_image_pipeline_is_resolution_invariant(self) -> None:
        name = "sample1"
        image = SAMPLES_DIR / f"{name}.png"
        baseline_boxes = run_paddle_on_image(image, ocr=self.ocr)
        baseline_entries = parse_layout(baseline_boxes)
        baseline_signature = _entry_structure_signature(baseline_entries)
        scales = (0.75, 1.25, 1.5)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)
            for scale in scales:
                with self.subTest(scale=scale):
                    resized_path = temp_dir_path / f"scaled_{str(scale).replace('.', '_')}.png"
                    with Image.open(image) as source:
                        width = max(1, int(round(source.width * scale)))
                        height = max(1, int(round(source.height * scale)))
                        resized = source.resize((width, height), resample=Image.Resampling.BICUBIC)
                        resized.save(resized_path)

                    boxes = run_paddle_on_image(resized_path, ocr=self.ocr)
                    entries = parse_layout(boxes)
                    self.assertGreater(len(boxes), 0)
                    self.assertEqual(_entry_structure_signature(entries), baseline_signature)

    def test_repeated_ocr_runs_keep_same_parsed_hash(self) -> None:
        image = SAMPLES_DIR / "sample1.png"
        hashes: list[str] = []

        for _ in range(5):
            boxes = run_paddle_on_image(image, ocr=self.ocr)
            entries = [asdict(entry) for entry in parse_layout(boxes)]
            payload = json.dumps(entries, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
            hashes.append(sha256(payload.encode("utf-8")).hexdigest())

        self.assertEqual(len(set(hashes)), 1)

    def test_layout_grouping_is_stable_when_non_time_text_is_corrupted(self) -> None:
        image = SAMPLES_DIR / "sample1.png"
        boxes = run_paddle_on_image(image, ocr=self.ocr)
        baseline_entries = parse_layout(boxes)

        corrupted_boxes: list[dict[str, float | str]] = []
        for box in boxes:
            text = box.text
            if not TIME_RANGE_RE.search(text):
                text = _corrupt_non_time_text(text)
            corrupted_boxes.append({"text": text, "x": box.x, "y": box.y, "w": box.w, "h": box.h})

        corrupted_entries = parse_layout(corrupted_boxes)
        self.assertEqual(_entry_structure_signature(baseline_entries), _entry_structure_signature(corrupted_entries))


def _corrupt_non_time_text(text: str) -> str:
    result: list[str] = []
    for char in text:
        if char.isalpha():
            result.append("x")
        elif char.isdigit():
            result.append("7")
        else:
            result.append(char)
    return "".join(result)


def _entry_structure_signature(entries: list) -> list[tuple[str, str, bool, bool, bool]]:
    signature: list[tuple[str, str, bool, bool, bool]] = []
    for entry in entries:
        signature.append(
            (
                entry.start,
                entry.end,
                bool(entry.title.strip()),
                bool(entry.location.strip()),
                bool(entry.address.strip()),
            )
        )
    return signature


if __name__ == "__main__":
    unittest.main()
