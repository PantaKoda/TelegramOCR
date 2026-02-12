# OCR Worker (Phase 2 Session Finalization Stub)

Phase 2 validates DB finalization + claim concurrency correctness:

- PostgreSQL connectivity
- claims at most one session per run using `FOR UPDATE SKIP LOCKED`
- lease semantics:
  - prefers `state = 'pending'`
  - reclaims stale `state = 'processing'` when lease is expired
- stub `schedule_version` insert with payload `{"stub": true}`
- fixed `schedule_date` (hardcoded) and fixed `version = 1`
- deterministic payload hash
- session transition: `processing -> done | failed`, clearing lease fields

No OCR, image download, or parsing is performed in this phase.

## Setup

```bash
uv sync
```

Apply DB lease migration (once per environment):

```bash
psql "$DATABASE_URL" -f database/migrations/20260212_add_session_leases.sql
```

## Required Environment Variables

- `DATABASE_URL` (or `POSTGRES_DSN` or `TEST_DATABASE_URL`)

Optional:

- `DB_SCHEMA` (default: `schedule_ingest`)
- `WORKER_ID` (default: `worker-<pid>`)
- `LEASE_TIMEOUT_SECONDS` (default: `300`)
- `SIMULATED_WORK_SECONDS` (default: `0`, test hook)
- `PENDING_STATE` (default: `pending`)
- `PROCESSING_STATE` (default: `processing`)
- `DONE_STATE` (default: `done`)
- `FAILED_STATE` (default: `failed`)

## Run Once

```bash
uv run python main.py
```

The worker runs one claim/process cycle and exits.

## How To Test

1. Ensure at least one session is claimable:
   - `state = pending`, or
   - stale `state = processing` (`locked_at` older than lease timeout)
2. Run the worker once.
3. Verify for the processed session:
   - exactly one new row in `schedule_version`
   - `day_schedule.current_version` set/advanced by DB trigger
   - lease fields (`locked_at`, `locked_by`) cleared
   - `capture_session.state = done`
4. Force a failure case (e.g., invalid stub version/payload path) and verify:
   - `capture_session.state = failed`
   - `capture_session.error` populated
5. Run worker again and verify no duplicate writes for already-completed sessions.

### Integration Race Test

If you provide `TEST_DATABASE_URL` (or `DATABASE_URL`), run:

```bash
uv run python -m unittest tests/test_integration_claim_locking.py
```

This test starts two worker processes against a temporary schema and asserts only one claims and finalizes the same session.
