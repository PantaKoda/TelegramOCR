import unittest

import numpy as np

from ocr.paddle_adapter import OCRBox, legacy_ocr_result_to_boxes, paddle_page_to_boxes


class PaddleAdapterConversionTests(unittest.TestCase):
    def test_paddle_page_to_boxes_converts_geometry_and_confidence(self) -> None:
        page = {
            "dt_polys": [
                np.array([[10, 20], [30, 20], [30, 40], [10, 40]], dtype=np.int16),
                np.array([[50, 60], [70, 60], [70, 80], [50, 80]], dtype=np.int16),
            ],
            "rec_texts": ["10:00-14:00", "Billdal"],
            "rec_scores": [0.98, 0.95],
        }

        boxes = paddle_page_to_boxes(page)
        self.assertEqual(
            boxes,
            [
                OCRBox(text="10:00-14:00", x=10.0, y=20.0, w=20.0, h=20.0, confidence=0.98),
                OCRBox(text="Billdal", x=50.0, y=60.0, w=20.0, h=20.0, confidence=0.95),
            ],
        )

    def test_legacy_ocr_result_to_boxes_converts_expected_shape(self) -> None:
        records = [
            [[[10, 20], [30, 20], [30, 40], [10, 40]], ("10:00-14:00", 0.98)],
            [[[50, 60], [70, 60], [70, 80], [50, 80]], ("Billdal", 0.95)],
        ]

        boxes = legacy_ocr_result_to_boxes(records)
        self.assertEqual(
            boxes,
            [
                OCRBox(text="10:00-14:00", x=10.0, y=20.0, w=20.0, h=20.0, confidence=0.98),
                OCRBox(text="Billdal", x=50.0, y=60.0, w=20.0, h=20.0, confidence=0.95),
            ],
        )


if __name__ == "__main__":
    unittest.main()

