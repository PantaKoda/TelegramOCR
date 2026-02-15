# Local Backfill Runner

This tool is a one-shot OCR backfill runner for local machine execution.

It is designed for reruns/backfills without changing `capture_session` states in PostgreSQL.

## What It Does

- Reads session metadata from PostgreSQL:
  - `capture_session.id`
  - `capture_session.user_id`
  - `capture_session.state`
  - `capture_image.sequence`
  - `capture_image.r2_key`
- Resolves each `r2_key` to a local downloaded file (by exact relative path or basename).
- Runs the same OCR + layout + semantic normalization pipeline used by the worker.
- Aggregates multi-image sessions by ordered `sequence`.
- Writes local JSON outputs:
  - one file per session
  - one summary report
- Optional apply mode:
  - updates `day_snapshot` and `schedule_event` in remote DB
  - optional notification persistence

## What It Does Not Do

- Does not mutate `capture_session` states.
- Does not require inserting new `capture_image` rows.
- Does not require reuploading screenshots.
- Does not require Telegram/C# runtime.

## Why This Exists

Your production schema enforces session/image transition constraints (`open`, `closed`, unique `r2_key`, etc.), which make DB state replay difficult for large reruns.

This tool bypasses that by reusing existing metadata and local images directly.

## Usage

From repo root:

```bash
uv run python tools/local_backfill_runner/backfill_runner.py \
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

Dry run (default): reads and parses, writes JSON output, no DB writes.

Apply mode:

```bash
uv run python tools/local_backfill_runner/backfill_runner.py \
  --database-url "$DATABASE_URL" \
  --local-root /absolute/path/to/images \
  --user-id 8225717176 \
  --states done,failed \
  --apply
```

Apply + store notifications:

```bash
uv run python tools/local_backfill_runner/backfill_runner.py \
  --database-url "$DATABASE_URL" \
  --local-root /absolute/path/to/images \
  --user-id 8225717176 \
  --states done,failed \
  --apply \
  --store-notifications
```

## Selection Flags

- `--session-id <id>` (repeatable): process exact sessions only.
- `--user-id <id>`: filter by user.
- `--states done,failed`: state filter.
- `--from-date YYYY-MM-DD`: created-at lower bound.
- `--to-date YYYY-MM-DD`: created-at upper bound (exclusive next day).
- `--limit N`: limit session count.

## Image Resolution Rules

Given a DB `r2_key`, resolution order is:

1. exact relative path under `--local-root` (if downloaded with directory structure)
2. basename match under `--local-root` recursive index

If basename matches multiple files, run fails for that session as ambiguous.

## Output Files

- Summary: `--output-file` (default `backfill_report.json`)
- Per-session: `--output-dir/<session_id>.json` (default `backfill_sessions/`)

Each session JSON includes:

- image mapping details (`r2_key` -> local path)
- detected date details
- canonical shifts payload
- preview/persisted event counts
- optional notification details

## Safety Notes

- Start with dry run first.
- Enable `--apply` only after validating JSON output.
- Use `--store-notifications` only when you explicitly want notification rows created.
