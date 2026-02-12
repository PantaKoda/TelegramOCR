# OCR Worker (Phase 3 Fixture Payload)

Phase 3 validates deterministic payload/version mechanics before OCR:

- PostgreSQL connectivity
- at-most-one session claim per run using `FOR UPDATE SKIP LOCKED`
- lease semantics:
  - prefers `state = 'pending'`
  - reclaims stale `state = 'processing'` when lease is expired
- fixture-driven payload write from local JSON (`fixtures/sample_schedule.json`)
- deterministic payload hash from normalized JSON
- per-date version progression (`current_version + 1`, else `1`)
- ownership-guarded insert/finalization (`locked_by` must match claimer)
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
- `FIXTURE_PAYLOAD_PATH` (default: `fixtures/sample_schedule.json`)
- `WORKER_ID` (default: `worker-<pid>`)
- `LEASE_TIMEOUT_SECONDS` (default: `300`)
- `LEASE_HEARTBEAT_SECONDS` (default: `10`)
- `ENABLE_LEASE_HEARTBEAT` (default: `true`)
- `SIMULATED_WORK_SECONDS` (default: `0`, test hook)
- `PENDING_STATE` (default: `pending`)
- `PROCESSING_STATE` (default: `processing`)
- `DONE_STATE` (default: `done`)
- `FAILED_STATE` (default: `failed`)

## Fixture Payload Contract

Fixture payload must be a JSON object and include:

- `schedule_date`: ISO date string (`YYYY-MM-DD`)
- any additional deterministic fields (for now, sample uses `entries`)

Example:

```json
{
  "schedule_date": "2026-02-10",
  "entries": [
    {"start": "10:00", "end": "14:00", "title": "Cleaning", "location": "Billdal"}
  ]
}
```

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
4. Edit fixture payload (e.g., change an entry time), enqueue a new session, rerun.
5. Verify:
   - new `schedule_version` row inserted with incremented version
   - previous versions remain immutable
   - `day_schedule.current_version` points to latest version

### Integration Tests

If you provide `TEST_DATABASE_URL` (or `DATABASE_URL`), run:

```bash
uv run python -m unittest tests/test_main_worker.py tests/test_integration_claim_locking.py tests/test_integration_fixture_versioning.py
```

Coverage:
- claim/finalize race safety (`tests/test_integration_claim_locking.py`)
- fixture payload version timeline (`tests/test_integration_fixture_versioning.py`)
- unit checks for fixture parsing + insert SQL parameterization (`tests/test_main_worker.py`)

## Invariants Enforced

- finalization requires lease ownership (`locked_by` guard in SQL)
- schedule version insert requires lease ownership (`locked_by` guard in SQL)
- one schedule_version per session (`UNIQUE(session_id)`)
- heartbeat safety check: `LEASE_HEARTBEAT_SECONDS < LEASE_TIMEOUT_SECONDS / 3`
