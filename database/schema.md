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

## Worker Responsibilities (Current Phase)

- Claim one session with `FOR UPDATE SKIP LOCKED` and lease rules
- Insert one `schedule_version` stub payload
- Mark session `done` on success
- Mark session `failed` with `error` on failure
- Let DB triggers manage `day_schedule`
