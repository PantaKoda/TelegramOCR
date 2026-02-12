# OCR Worker (Phase 2 Session Finalization Stub)

Phase 2 validates DB finalization correctness only:

- PostgreSQL connectivity
- picks at most one `capture_session` row where `state = 'processing'` per run
- stub `schedule_version` insert with payload `{"stub": true}`
- fixed `schedule_date` (hardcoded) and fixed `version = 1`
- deterministic payload hash
- session transition: `processing -> done | failed`

No OCR, image download, or parsing is performed in this phase.

## Setup

```bash
uv sync
```

## Required Environment Variables

- `DATABASE_URL` (or `POSTGRES_DSN` or `TEST_DATABASE_URL`)

Optional:

- `DB_SCHEMA` (default: `schedule_ingest`)

## Run Once

```bash
uv run python main.py
```

## How To Test

1. Ensure C# dispatcher has moved at least one session to `state = processing`.
2. Run the worker once.
3. Verify for the processed session:
   - exactly one new row in `schedule_version`
   - `day_schedule.current_version` set/advanced by DB trigger
   - `capture_session.state = done`
4. Force a failure case (e.g., invalid stub version/payload path) and verify:
   - `capture_session.state = failed`
   - `capture_session.error` populated
5. Run worker again and verify no duplicate writes for already-completed sessions.
