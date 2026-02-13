# OCR Worker (Phase 3.5 Worker + Phase 5 OCR Adapter + Phase 6/6.5 Semantics + Phase 7 Diff)

Current state:

- Phase 3.5 worker validates noise-tolerant versioning before OCR (`main.py`)
- Phase 5 adds real PaddleOCR box extraction adapter + golden sample tests (`ocr/paddle_adapter.py`)
- Phase 6 adds deterministic semantic normalization utilities (`parser/semantic_normalizer.py`)
- Phase 6.5 adds deterministic entity fingerprinting (`parser/entity_identity.py`)
- Phase 7 adds deterministic schedule change detection (`domain/schedule_diff.py`)

Phase 3.5 worker capabilities:

- PostgreSQL connectivity
- at-most-one session claim per run using `FOR UPDATE SKIP LOCKED`
- lease semantics:
  - prefers `state = 'pending'`
  - reclaims stale `state = 'processing'` when lease is expired
- fixture-driven payload input from local JSON (`fixtures/sample_schedule.json`)
- optional deterministic chaos parser (seeded format noise)
- canonical normalization before hashing/writes
- deterministic payload hash from canonical normalized JSON
- per-date version progression (`current_version + 1`, else `1`)
- no-change dedupe: if canonical payload hash matches latest version, no new `schedule_version` is inserted
- per-date transactional advisory lock ensures deterministic dedupe under concurrency
- inserts use `ON CONFLICT ... DO NOTHING RETURNING` to classify create-vs-existing outcomes via DB result
- ownership-guarded insert/finalization (`locked_by` must match claimer)
- session transition: `processing -> done | failed`, clearing lease fields
- deterministic pre-OCR layout parser module for OCR-like boxes (`parser/layout_parser.py`)

Phase 5 adapter capabilities (not yet wired into `main.py` DB write path):

- PaddleOCR models:
  - `PP-OCRv5_mobile_det`
  - `PP-OCRv5_mobile_rec`
- Paddle options:
  - `use_doc_orientation_classify=False`
  - `use_doc_unwarping=False`
  - `use_textline_orientation=False`
- conversion only: Paddle polygons/text/scores -> box geometry (`x`, `y`, `w`, `h`) + confidence
- no adapter-side filtering/grouping/normalization; parser remains source of layout grouping truth

Phase 6 semantic normalization capabilities (module-level, deterministic):

- normalize parsed entries into canonical shift fields:
  - `customer_name`, `street`, `street_number`, `postal_code`, `postal_area`, `city`, `shift_type`
- Swedish-oriented address decomposition heuristics:
  - postal code pattern `NNN NN`
  - street + house number extraction
  - trailing city/place normalization
- title/customer cleanup:
  - collapse whitespace
  - normalize case/diacritics
  - remove common company suffix noise tokens (`AB`, `HB`, `Städservice`)
- deterministic shift classification tags:
  - `SCHOOL`, `OFFICE`, `HOME_VISIT`, `UNKNOWN`
- identity fields included in canonical shift output:
  - `location_fingerprint`
  - `customer_fingerprint`

Phase 6.5 identity fingerprinting capabilities:

- location identity key built from semantic location fields:
  - normalized `street`, `street_number`, and (`postal_area` or `city`)
- customer identity key built from normalized name tokens:
  - company suffix noise removed
  - token order stabilized by surname + initials
- OCR confusion tolerance in keys:
  - accent stripping, case/whitespace collapse
  - `0/O` and `1/l/I` canonicalization

Phase 7 change-detection capabilities:

- compares canonical shift lists between schedule versions
- matching key is identity-first and date-scoped:
  - `(location_fingerprint, customer_fingerprint, schedule_date)`
- emits deterministic events:
  - `ShiftAdded`
  - `ShiftRemoved`
  - `ShiftTimeChanged`
  - `ShiftRelocated`
  - `ShiftRetitled`
- order-insensitive: pure reorder of unchanged shifts emits no events

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
- `ENABLE_CHAOS_PARSER` (default: `false`)
- `CHAOS_SEED` (default: `0`)
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

## Layout Parser (Phase 4 Pre-OCR)

Module: `parser/layout_parser.py`

Main API:

```python
parse_layout(boxes: list[Box]) -> list[Entry]
```

Behavior:
- sorts boxes by geometry
- clusters boxes into lines
- groups lines into cards by vertical gaps
- extracts time/title/address/location per card
- ignores top UI chrome cards with no time line

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
6. Noise stability check:
   - enqueue multiple sessions with identical semantic fixture data
   - run with `ENABLE_CHAOS_PARSER=true` and varying `CHAOS_SEED`
   - verify `schedule_version` count for that date stays `1`

### Integration Tests

If you provide `TEST_DATABASE_URL` (or `DATABASE_URL`), run:

```bash
uv run python -m unittest tests/test_layout_parser.py tests/test_main_worker.py tests/test_integration_claim_locking.py tests/test_integration_fixture_versioning.py
```

Coverage:
- claim/finalize race safety (`tests/test_integration_claim_locking.py`)
- fixture payload version timeline (`tests/test_integration_fixture_versioning.py`)
- chaos noise does not create phantom versions (`tests/test_integration_fixture_versioning.py`)
- synthetic layout reconstruction (single card, stacked cards, wrapped address, header ignore, jitter, landscape columns) (`tests/test_layout_parser.py`)
- unit checks for fixture parsing + insert SQL parameterization (`tests/test_main_worker.py`)

### OCR Adapter Golden Tests (No DB)

Uses real screenshot fixtures and verifies:
`image -> PaddleOCR boxes -> parse_layout -> expected entries`.

```bash
PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK=true uv run python -m unittest tests/test_paddle_adapter.py tests/test_ocr_golden_samples.py
```

### Semantic Normalizer Tests (No DB)

```bash
uv run python -m unittest tests/test_semantic_normalizer.py
```

### Entity Identity Tests (No DB)

```bash
uv run python -m unittest tests/test_entity_identity.py
```

### Schedule Diff Tests (No DB)

```bash
uv run python -m unittest tests/test_schedule_diff.py
```

## Invariants Enforced

- finalization requires lease ownership (`locked_by` guard in SQL)
- schedule version insert requires lease ownership (`locked_by` guard in SQL)
- one schedule_version per session (`UNIQUE(session_id)`)
- heartbeat safety check: `LEASE_HEARTBEAT_SECONDS < LEASE_TIMEOUT_SECONDS / 3`
