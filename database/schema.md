# Database Schema (Authoritative)

This document describes the `schedule_ingest` PostgreSQL schema contract used by the Python OCR worker.

## Scope

- Schema owner: C# backend
- Schema consumer: Python worker
- Worker must follow this contract and rely on DB constraints/triggers

## Enum: `capture_session_state`

Allowed values:

- `pending`
- `processing`
- `done`
- `failed`

Lifecycle:

- `pending -> processing | failed`
- `processing -> done | failed`
- `processing -> processing` (only when stale lease is reclaimed)

## Table: `capture_session`

Purpose: unit of worker processing.

Columns:

- `id` uuid PK
- `user_id` bigint NOT NULL
- `state` capture_session_state NOT NULL
- `created_at` timestamptz NOT NULL
- `error` text NULL
- `locked_at` timestamptz NULL
- `locked_by` text NULL

Rules:

- `error` is populated only when state is `failed`
- worker claims with transactional lease:
  - claim `pending`, or stale `processing`
  - prioritize `pending` ahead of stale `processing`
  - on claim set `state=processing`, `locked_at=now()`, `locked_by=<worker>`
- worker heartbeat:
  - refresh `locked_at` while still processing long jobs
  - refresh/update must be guarded by `locked_by=<worker>`
- worker finalization:
  - success: `state=done`, clear lease fields (only if `locked_by=<worker>`)
  - failure: `state=failed`, set `error`, clear lease fields (only if `locked_by=<worker>`)

## Table: `capture_image`

Purpose: ordered image metadata for a session.

Columns:

- `id` uuid PK
- `session_id` uuid FK -> `capture_session.id`
- `sequence` integer NOT NULL (> 0)
- `r2_key` text NOT NULL (unique)
- `telegram_message_id` bigint NULL
- `created_at` timestamptz NOT NULL

Rules:

- `(session_id, sequence)` unique
- Images are immutable session inputs

## Table: `day_schedule`

Purpose: current version pointer for `(user_id, schedule_date)`.

Columns:

- `user_id` bigint PK part
- `schedule_date` date PK part
- `current_version` integer NOT NULL (> 0)

Rules:

- Worker does not update this table directly
- Trigger updates it when `schedule_version` rows are inserted

## Table: `schedule_version`

Purpose: immutable schedule payload versions.

Columns:

- `user_id` bigint PK part
- `schedule_date` date PK part
- `version` integer PK part
- `session_id` uuid UNIQUE FK -> `capture_session.id`
- `payload` jsonb NOT NULL (must be JSON object)
- `payload_hash` text NOT NULL
- `created_at` timestamptz NOT NULL

Rules:

- One `schedule_version` row per session (`session_id` unique)
- Versioning/order constraints enforced by DB logic and triggers

## Table: `day_snapshot` (Phase 9)

Purpose: latest canonical day state for `(user_id, schedule_date)` used as diff baseline.

Columns:

- `user_id` bigint PK part
- `schedule_date` date PK part
- `snapshot_payload` jsonb NOT NULL (array of canonical shifts)
- `source_session_id` uuid NOT NULL
- `updated_at` timestamptz NOT NULL

Rules:

- Upserted once per processed observation session
- Stores meaning-level canonical shifts, not raw OCR output
- Used to compute new events against prior known day state

## Table: `schedule_event` (Phase 9)

Purpose: immutable event history derived from schedule diffs.

Columns:

- `event_id` uuid PK
- `user_id` bigint NOT NULL
- `schedule_date` date NOT NULL
- `event_type` text NOT NULL
- `location_fingerprint` text NOT NULL
- `customer_fingerprint` text NOT NULL
- `old_value_hash` text NOT NULL
- `new_value_hash` text NOT NULL
- `old_value` jsonb NULL
- `new_value` jsonb NULL
- `detected_at` timestamptz NOT NULL
- `source_session_id` uuid NOT NULL

Allowed `event_type` values:

- `shift_added`
- `shift_removed`
- `shift_time_changed`
- `shift_relocated`
- `shift_retitled`
- `shift_reclassified`

Idempotency rule:

- Unique dedupe key on:
  - `(user_id, schedule_date, location_fingerprint, event_type, old_value_hash, new_value_hash)`
- Prevents duplicate semantic events when a session/process is retried

## Table: `schedule_notification` (Phase 12/13)

Purpose: durable outbound message queue for bot delivery.

Columns:

- `notification_id` text PK
- `user_id` bigint NOT NULL
- `schedule_date` date NOT NULL
- `source_session_id` uuid NOT NULL
- `status` text NOT NULL (`pending|sent|failed`)
- `notification_type` text NOT NULL (`event|summary`)
- `message` text NOT NULL
- `event_ids` jsonb NOT NULL (array)
- `created_at` timestamptz NOT NULL
- `sent_at` timestamptz NULL

Rules:

- Python worker inserts pending notifications only (idempotent on `notification_id`)
- C# delivery layer consumes rows with `status='pending'`
- Delivery updates row state to:
  - `sent` + `sent_at` when delivery succeeds
  - `failed` when delivery fails
- Worker restart must not duplicate rows (same deterministic `notification_id` + conflict-ignore insert)

## Worker Responsibilities (Current Phase)

- Claim one session with `FOR UPDATE SKIP LOCKED` and lease rules
- Load deterministic payload from a local fixture JSON file
- Optionally apply deterministic seeded noise parser (representation-only changes)
- Canonicalize payload before hashing/insertion:
  - normalize time strings
  - normalize whitespace/casing fields
  - sort entries deterministically
- Serialize per `(user_id, schedule_date)` writes via transactional advisory lock
- Parse `schedule_date` from the fixture payload
- Compute next version for `(user_id, schedule_date)` as:
  - `1` when `day_schedule` row does not exist
  - `current_version + 1` when `day_schedule` row exists
- Insert one immutable `schedule_version` payload only when canonical payload changed
- If canonical payload hash matches latest version for `(user_id, schedule_date)`, skip insert and mark session done
- Insert query uses `ON CONFLICT ... DO NOTHING RETURNING` for deterministic create-vs-existing classification
- Mark session `done` on success
- Mark session `failed` with `error` on failure
- Let DB triggers manage `day_schedule`

## Background Runtime Responsibilities (Phase 13)

- Run forever (no HTTP endpoint, no exposed ports)
- Per iteration:
  - detect finalizable sessions by idle timeout
  - process finalized session once
  - persist semantic events/snapshot
  - persist pending notifications
- Sleep between iterations (`WORKER_POLL_SECONDS`)
- Log iteration start/end/errors to stdout

## Event Store Responsibilities (Phase 9)

- Load previous day snapshot from `day_snapshot`
- Compute semantic diff against new canonical observation
- Persist immutable `schedule_event` rows
- Upsert latest day snapshot in `day_snapshot`
- Keep event payloads semantic (old/new canonical shift values), not raw OCR text
