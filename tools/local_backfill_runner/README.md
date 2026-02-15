# Local Backfill Runner (Standalone)

`local_backfill_runner` is a standalone OCR backfill tool intended to be split into its own repository.

It reads existing session/image metadata from PostgreSQL, resolves local image files, runs OCR + parsing, and writes JSON outputs. In apply mode it can also write events/snapshots/notifications back to PostgreSQL.

## What It Uses

- PostgreSQL (`psycopg`)
- PaddleOCR (`paddleocr`, `numpy`)
- Cloud/R2 metadata only as DB keys (no runtime R2 fetch required)

## Install (inside this folder)

```bash
cd tools/local_backfill_runner
uv sync
```

## Run

From repository root:

```bash
uv run --project tools/local_backfill_runner python tools/local_backfill_runner/backfill_runner.py \
  --database-url "$DATABASE_URL" \
  --db-schema schedule_ingest \
  --local-root /absolute/path/to/downloaded/images \
  --user-id 8225717176 \
  --states done,failed \
  --output-file /absolute/path/backfill_report.json \
  --output-dir /absolute/path/backfill_sessions \
  --ocr-lang sv \
  --ocr-default-year 2026
```

Or run the package entrypoint:

```bash
uv run --project tools/local_backfill_runner local-backfill-runner --help
```

## Output

- Summary report JSON (`--output-file`)
- Per-session JSON files (`--output-dir/<session_id>.json`)

## Safety

- Dry-run is default.
- `--apply` enables DB writes (`day_snapshot`, `schedule_event`).
- `--store-notifications` additionally writes `schedule_notification` (requires `--apply`).
