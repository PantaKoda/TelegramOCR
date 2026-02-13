# OCR Worker – Agent Guidelines

## AGENTS.md Authority & Update Policy (Mandatory)

This document is the **authoritative description** for the Python OCR worker.

Any change that affects one or more of the following **MUST update this file in the same commit or PR**:

- OCR worker architecture or data flow
- Interaction contract with PostgreSQL or Cloudflare R2
- Session state transitions (`processing → done | failed`)
- Parsing rules or output payload structure
- New non-negotiable constraints or assumptions

When implementing a feature:
- Update **Current Implementation Status** if behavior changes
- Update **Target Architecture & Intent** if scope expands
- Update **Non-Negotiable Design Rules** if new constraints are introduced

If a change does **not** require an update, state this explicitly in the PR description.

---

## Branching & Workflow Rules (Mandatory)

- Never commit directly to `main`
- All work must be done in **feature branches**
- One feature branch = one conceptual change
- Feature branches must be complete before merge
- The agent **may create pull requests** using GitHub CLI (`gh`) from the terminal

---

## Testing & Verification Rules (Mandatory)

### Baseline guarantees (must never break)

Before merge, the OCR worker must:

- Connect successfully to PostgreSQL
- Claim sessions safely with transactional locking
- Transition sessions to `done` or `failed`
- Never modify sessions outside its responsibility

### Test expectations

- Prefer **unit tests** for:
  - date extraction
  - layout parsing
  - OCR normalization
- Prefer **integration tests** for:
  - DB read/write
  - session state transitions
- Tests must NOT require:
  - Telegram
  - the C# backend
- PaddleOCR may be mocked in unit tests

Every PR must include a **“How to test”** section.

---

## Technology & Tooling (Mandatory)

The following tools and libraries **must be used** unless explicitly discussed and approved:

### Python Environment
- Use **UV** for environment and dependency management
- Project must be reproducible via `uv sync`
- Do not use system Python or ad-hoc `pip install`

### OCR
- Use **PaddleOCR**
- OCR must preserve:
  - text
  - confidence
  - bounding boxes
- No alternative OCR engines without explicit approval

### Database
- PostgreSQL only
- Use a lightweight driver (`psycopg`, `asyncpg`, or equivalent)
- No ORMs

### Object Storage
- Cloudflare R2 via **S3-compatible API**
- Use `boto3`
- Images are read-only inputs

### Parsing & Data Handling
- Use standard Python libraries where possible
- JSON output must be deterministic and normalized
- Avoid heavy frameworks

### Logging
- Use standard Python logging
- Logs must include:
  - session id
  - success / failure
  - high-level parsing status

---

## Project Scope (Read Carefully)

You are implementing the **Python OCR worker only**.

You are responsible for:
- Polling PostgreSQL for sessions in `state = processing`
- Loading session images (ordered by sequence)
- Running OCR using PaddleOCR
- Parsing UI layout into structured schedule data
- Writing results to PostgreSQL
- Transitioning session state to `done` or `failed`

You are **NOT** responsible for:
- Telegram handling
- Session creation or grouping
- Deciding when OCR starts
- Version numbering or update detection logic
- Any C# code

PostgreSQL is the **only integration boundary**.

---

## Session Lifecycle (Authoritative)

```

pending → processing → done | failed

```

Rules:
- Workers claim with `FOR UPDATE SKIP LOCKED`
- `processing` rows may be reclaimed only when lease is stale
- Each session must be finalized exactly once

---

## Non-Negotiable Design Rules

- Do NOT run OCR per image (only per session)
- Do NOT use ML/LLM grouping; use deterministic geometry-based grouping rules only
- Do NOT stitch images together
- Do NOT overwrite existing schedule versions
- Do NOT guess dates from timestamps or filenames
- During pre-OCR phases, date identity comes from deterministic fixture payload
- During OCR phases, date identity must come from OCR UI text
- On failure, mark session `failed` with a clear error
- On success, mark session `done`
- Cloudflare R2 is blob storage only
- PostgreSQL is the source of truth

---

## Output Contract

For each processed session, produce **one immutable JSON payload** stored in `schedule_versions.payload`.

The payload must:
- represent exactly one calendar date
- contain normalized schedule entries

If the date cannot be resolved or is inconsistent:
- fail the session

---

## Current Implementation Status

- C# backend complete
- Sessions grouped and claimed atomically
- Dispatcher transitions session into worker-claimable queue state
- Phase 3.5 chaos-normalization worker implemented with DB lease claim (`main.py`)
- Phase 4 pre-OCR layout parser implemented (`parser/layout_parser.py`)
- Current worker behavior:
  - claims at most one session per run with `FOR UPDATE SKIP LOCKED`
  - claim policy: `pending` first, stale `processing` lease reclaim
  - sets lease fields (`locked_at`, `locked_by`) on claim
  - refreshes lease heartbeat via `locked_at` while processing long-running work
  - guards heartbeat/finalization with `locked_by` ownership checks
  - loads deterministic JSON fixture payload from disk
  - optional seeded chaos parser introduces deterministic representation noise (format/casing/whitespace/order)
  - canonicalizes payload before hashing/persistence (time/text normalization + deterministic entry ordering)
  - serializes per `(user_id, schedule_date)` writes with transactional advisory lock
  - insert path uses `ON CONFLICT ... DO NOTHING RETURNING` to classify created vs existing row
  - requires fixture payload field `schedule_date` (ISO date string)
  - computes next version per `(user_id, schedule_date)` from `day_schedule`
  - inserts one immutable `schedule_version` row only when canonical payload changed
  - when canonical payload hash matches latest version, marks session done without inserting new version
  - computes deterministic `payload_hash`
  - transitions `processing → done` on success and clears lease fields
  - transitions `processing → failed` with error on failure and clears lease fields
- Layout parser behavior (pre-OCR):
  - input: flat list of OCR-like boxes (`text`, `x`, `y`, `w`, `h`)
  - deterministic pipeline: sort -> line clustering -> card grouping -> field extraction
  - time detection via regex (`HH:MM`/`HH.MM` ranges), normalized to `HH:MM`
  - output entry fields: `start`, `end`, `title`, `location`, `address`
  - top chrome/header cards without time lines are ignored
- Phase 5 OCR adapter (pre-worker wiring):
  - PaddleOCR adapter implemented in `ocr/paddle_adapter.py`
  - configured models: `PP-OCRv5_mobile_det` + `PP-OCRv5_mobile_rec`
  - adapter contract is thin conversion only: Paddle polygon/text/score -> `Box` geometry (`x`, `y`, `w`, `h`) + confidence
  - no filtering, grouping, normalization, or semantic cleanup in adapter
  - real-screenshot golden tests added in `tests/test_ocr_golden_samples.py` with fixtures under `tests/ocr_samples/`
- Phase 6 semantic normalizer (pre-worker wiring):
  - deterministic semantic normalization module in `parser/semantic_normalizer.py`
  - address decomposition into `street`, `street_number`, `postal_code`, `postal_area`, `city`
  - customer/title cleanup with whitespace collapse, casing normalization, and company-noise token removal
  - deterministic shift classification tags: `SCHOOL`, `OFFICE`, `HOME_VISIT`, `UNKNOWN`
  - canonical output now includes deterministic identity keys: `location_fingerprint`, `customer_fingerprint`
  - normalization tests in `tests/test_semantic_normalizer.py` (accent loss, missing postal code, multiline address join, OCR noise, canonical-location variants)
- Phase 6.5 entity identity (pre-worker wiring):
  - deterministic fingerprint module in `parser/entity_identity.py`
  - `location_fingerprint` from normalized semantic location fields (`street`, `street_number`, `postal_area|city`)
  - OCR confusion normalization in fingerprinting (`0/O`, `1/l/I`, accents, whitespace/case)
  - `customer_fingerprint` from normalized customer tokens with company-noise removal and order-insensitive initial folding
  - identity tests in `tests/test_entity_identity.py` (address variants, city typo confusion, different address separation, customer spelling variants)
- Phase 7 schedule diff (domain-level interpretation):
  - deterministic diff engine in `domain/schedule_diff.py`
  - compares canonical shifts across versions using identity-first matching (`location_fingerprint`, `customer_fingerprint`, `schedule_date`)
  - duplicate identity instances on the same date are paired by greedy minimum time distance before classifying events
  - emits typed change events: `ShiftAdded`, `ShiftRemoved`, `ShiftTimeChanged`, `ShiftRelocated`, `ShiftRetitled`
  - event detection stages separate identity/time/relocation/retitle concerns to reduce false positives from ordering noise
  - order-only changes do not emit events
  - tests in `tests/test_schedule_diff.py` cover add/remove/time-change/relocation/retitle/reorder cases
- Phase 8 session aggregation (domain-level observation merge):
  - deterministic session aggregator in `domain/session_aggregate.py`
  - input shape: `list[list[CanonicalShift]]` (one list per screenshot in the same capture session)
  - shifts merge when `location_fingerprint` matches and time distance is within tolerance (default 5 minutes)
  - time distance uses circular 24h math so cross-midnight observations merge correctly
  - containment fallback merges partial time observations when one range fully contains the other and identity matches
  - merge policy keeps earliest start and latest end, prefers longer address fields, preserves identity keys, and tracks per-shift `source_count`
  - output shape: `AggregatedDaySchedule` with deduplicated `AggregatedShift` entries
  - tests in `tests/test_session_aggregate.py` cover overlap dedupe, partial coverage union, jitter merge, same-time different-location separation, and triple-observation dedupe
- Phase 9 event store (durable history persistence):
  - infrastructure module in `infra/event_store.py`
  - pipeline helper performs: load previous snapshot -> diff -> persist events -> upsert day snapshot
  - persists immutable semantic events in `schedule_event` with identity anchors and old/new canonical values
  - persists latest canonical day state in `day_snapshot` for future diffs
  - supported persisted event types: `shift_added`, `shift_removed`, `shift_time_changed`, `shift_relocated`, `shift_retitled`, `shift_reclassified`
  - idempotency enforced with DB dedupe key (`user_id`, `schedule_date`, `location_fingerprint`, `event_type`, `old_value_hash`, `new_value_hash`) and conflict-ignore insert
  - monotonic history invariant validated in tests by replaying persisted events and comparing reconstructed snapshot to stored snapshot
  - DB migration added: `database/migrations/20260213_add_schedule_event_history.sql`
  - integration tests added: `tests/test_event_store.py`
- Phase 10 notification rules (human-facing interpretation layer):
  - deterministic notification mapper in `domain/notification_rules.py`
  - translates semantic events into user-facing sentences using canonical fields only
  - `shift_time_changed` wording is time-range aware:
    - start-only change: `old_start -> new_start`
    - end-only change: `ends old_end -> new_end`
    - full-range change: `old_start-old_end -> new_start-new_end`
  - supports per-day/session summary suppression to avoid notification storms (default threshold: 3 changes)
  - supports replay dedupe via `already_notified_event_ids` so repeated event fetches do not re-notify
  - tests in `tests/test_notification_rules.py` cover single-event messaging, summary mode, no-change output, and replay dedupe
- Phase 11 session lifecycle finalization gate (pre-worker wiring):
  - deterministic lifecycle module in `domain/session_lifecycle.py`
  - detects finalizable sessions by idle timeout from latest image timestamp (`MAX(capture_image.created_at)`)
  - idle timeout is environment-configurable via `SESSION_IDLE_TIMEOUT_SECONDS` (default: `25`)
  - lifecycle query requires at least one image and a configurable `open_state` (default: `pending` for current DB contract)
  - atomic finalize gate: `open_state -> processing_state` only when session is still open
  - deterministic once-per-session processing helper: finalizable scan -> finalize -> process callbacks -> processed-state mark
  - processed transition is ownership-safe at SQL update level by requiring `processing_state` at update time
  - tests in `tests/test_session_lifecycle.py` cover: not-finalized-while-active, idle-finalizable transition, finalize race safety, and single notification emission across reruns
- Phase 12 notification persistence (pre-Telegram delivery wiring):
  - infrastructure module in `infra/notification_store.py`
  - persists deterministic Phase 10 notifications to `schedule_notification`
  - notification rows include delivery state fields (`status`: `pending|sent|failed`, `sent_at`: nullable timestamp)
  - idempotency enforced by deterministic `notification_id` primary key + conflict-ignore insert behavior
  - lifecycle orchestration supports callback flow:
    - `events -> build_notifications(events) -> store_notifications(notifications)`
  - DB migration added: `database/migrations/20260213_add_schedule_notifications.sql`
  - integration tests added: `tests/test_notification_store.py`
- Phase 13 background deployment worker:
  - continuously running loop implemented in `worker/run_forever.py`
  - executes `run_lifecycle_once()` periodically (`WORKER_POLL_SECONDS`, default: `5`)
  - uses `SESSION_IDLE_TIMEOUT_SECONDS` lifecycle config to process only idle/finalizable sessions
  - loop catches/logs iteration errors and continues running (stdout-only logs)
  - all runtime logs are structured JSON with core fields:
    - `timestamp` (UTC), `service`, `level`, `event`, `session_id`, `user_id`, `correlation_id`
  - semantic runtime events logged:
    - `session.skipped_idle`, `session.finalized`, `session.processed`, `session.images_loaded`
    - `ocr.completed`, `layout.shifts_detected`, `aggregation.completed`
    - `diff.computed`, `events.persisted`, `notifications.generated`, `notifications.stored`
  - iteration error logs include:
    - `error.type`, `error.message`, `error.stage` (`ocr|layout|diff|db|lifecycle`)
  - persists notifications via `infra/notification_store.py` after successful session processing
  - runtime remains deterministic and fixture-driven for payload generation (`FIXTURE_PAYLOAD_PATH`)
  - Docker runtime added (`Dockerfile`) with `python:3.11-slim` base and `uv sync --frozen` dependency install

---

## Target Architecture & Intent

The OCR worker is a **pure interpretation engine**:
- input: images + session id
- output: structured schedule data
- no orchestration responsibilities

Keep the worker:
- deterministic
- idempotent
- debuggable
- replaceable

---

## Final Rule

If it interprets pixels, it belongs here.  
If it manages state or orchestration, it does not.
