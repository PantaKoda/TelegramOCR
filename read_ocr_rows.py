#!/usr/bin/env python3

import json
from pathlib import Path

json_path = Path(__file__).resolve().parent / "ocr_rows.json"

with json_path.open("r", encoding="utf-8") as f:
    jsondata = json.load(f)

row_text_array = jsondata["row_texts"]

if __name__ == "__main__":
    print(f"Loaded {len(row_text_array)} rows from {json_path}")
