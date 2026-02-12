# OCR Worker (Phase 1 Skeleton)

Phase 1 validates integration correctness only:

- PostgreSQL connectivity
- session selection: `capture_session.state = 'processing'`
- ordered `capture_image` lookup by `sequence`
- R2 download by `r2_key`
- stub `schedule_version` insert with payload `{"stub": true}`
- deterministic payload hash
- session transition: `processing -> done | failed`

No OCR or parsing is performed in this phase.

## Setup

```bash
uv sync
```

## Required Environment Variables

- `DATABASE_URL` (or `POSTGRES_DSN` or `TEST_DATABASE_URL`)
- `R2_ENDPOINT_URL`
- `R2_BUCKET`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`

Optional:

- `DB_SCHEMA` (default: `schedule_ingest`)
- `R2_REGION` (default: `auto`)
- `KEEP_PROCESSING_ON_FAILURE` (default: `false`; when `true`, worker logs failures but leaves session state as `processing`)

## Run Once

```bash
uv run python main.py
```

## How To Test

1. Ensure C# dispatcher has moved at least one session to `state = processing`.
2. Run the worker once.
3. Verify for each processed session:
   - exactly one new row in `schedule_version`
   - `day_schedule.current_version` set/advanced by DB trigger
   - `capture_session.state = done`
4. Force a failure case (e.g., invalid `r2_key`) and verify:
   - `capture_session.state = failed`
   - `capture_session.error` populated
5. Run worker again with no `processing` sessions and verify no new writes occur.
