Perfect — thanks for pasting the **actual executed schema**.
Based on this, below is a **clean, copy-paste-ready `database/schema.md`** that you can commit and then point the Python OCR agent to.

This is **documentation**, not SQL, and reflects **exactly** what you have live now.

---

```md
# Database Schema (Authoritative)

This document describes the PostgreSQL schema under the `schedule_ingest` schema.

It is the **authoritative contract** between:
- the C# backend (schema owner)
- the Python OCR worker (schema consumer)

The Python OCR worker **must not infer schema by probing a live database**.
All queries must be written according to this document.

---

## Schema: schedule_ingest

All tables, types, functions, and triggers live under the `schedule_ingest` schema.

---

## Enum: capture_session_state

Possible values:

- `open`
- `closed`
- `processing`
- `done`
- `failed`

This enum drives the capture session lifecycle and is enforced by database triggers.

---

## Table: capture_session

**Purpose**  
Represents a logical grouping of screenshots and the unit of OCR work.

Each session corresponds to **exactly one OCR attempt**.

**Columns**

- `id` (uuid, PK)  
  Unique identifier for the capture session.

- `user_id` (bigint, NOT NULL)  
  Telegram user ID.

- `state` (capture_session_state, NOT NULL)  
  Current lifecycle state of the session.

- `created_at` (timestamptz, NOT NULL)  
  Session creation timestamp.

- `closed_at` (timestamptz, NULLABLE)  
  Timestamp when the session transitioned out of `open`.

- `error` (text, NULLABLE)  
  Error message when state is `failed`.

**Constraints & Rules**

- `closed_at` must be NULL while state = `open`
- `closed_at` must be NOT NULL for all other states
- `error` is allowed only when state = `failed`
- Only valid state transitions are allowed:
```

open       → closed | failed
closed     → processing | failed
processing → done | failed

```

**Indexes**

- `(user_id, created_at DESC)`
- `(state, created_at)`
- Partial index on `(created_at)` where `state = 'closed'`

**OCR Worker Permissions**

- READ session metadata
- UPDATE `state` from:
- `processing → done`
- `processing → failed`
- UPDATE `error` when marking `failed`

The OCR worker must **never** transition a session into `processing`.

---

## Table: capture_image

**Purpose**  
Stores metadata for screenshots belonging to a capture session.

Images are **ordered and immutable**.

**Columns**

- `id` (uuid, PK)
- `session_id` (uuid, FK → capture_session.id)
- `sequence` (integer, NOT NULL)  
Order of images within the session (1-based, immutable).

- `r2_key` (text, NOT NULL)  
Object key in Cloudflare R2.

- `telegram_message_id` (bigint, NULLABLE)

- `created_at` (timestamptz, NOT NULL)

**Constraints & Rules**

- `(session_id, sequence)` is unique
- `sequence` must be > 0
- `r2_key` is globally unique
- Images may only be inserted while the parent session is in state `open`
- Images are deleted automatically if the session is deleted

**Indexes**

- `(session_id, created_at)`
- Unique `(r2_key)`
- Unique `(session_id, telegram_message_id)` when message ID is present

**OCR Worker Permissions**

- READ ONLY  
- Images must be read ordered by `sequence`

---

## Table: day_schedule

**Purpose**  
Tracks the current version number for a given user and calendar date.

Acts as the **current pointer** for schedule versions.

**Columns**

- `user_id` (bigint, PK part)
- `schedule_date` (date, PK part)
- `current_version` (integer, NOT NULL)

**Constraints & Rules**

- `(user_id, schedule_date)` is the primary key
- `current_version` must be > 0
- `current_version` is monotonic and cannot decrease

**OCR Worker Permissions**

- WRITE via trigger only  
- The worker must not update this table directly

---

## Table: schedule_version

**Purpose**  
Stores **immutable parsed schedule versions** produced by OCR.

Each capture session may produce **at most one** schedule version.

**Columns**

- `user_id` (bigint, PK part)
- `schedule_date` (date, PK part)
- `version` (integer, PK part)
- `session_id` (uuid, UNIQUE, FK → capture_session.id)
- `payload` (jsonb, NOT NULL)  
Parsed schedule data (must be a JSON object).

- `payload_hash` (text, NOT NULL)  
Deterministic hash of normalized payload.

- `created_at` (timestamptz, NOT NULL)

**Constraints & Rules**

- `(user_id, schedule_date, version)` is the primary key
- Each `session_id` may appear only once
- `version` must be:
- `1` if no previous versions exist
- `current_version + 1` otherwise
- Insertion automatically updates `day_schedule.current_version`
- Payload must be a JSON object
- Payload hash must be non-blank

**OCR Worker Permissions**

- INSERT exactly one row per processed session
- Must supply:
- correct `user_id`
- resolved `schedule_date`
- correct `version`
- `payload`
- `payload_hash`

---

## OCR Worker Query Summary

The OCR worker is expected to:

1. Select sessions where:
 - `state = 'processing'`
2. Load all related `capture_image` rows ordered by `sequence`
3. Download images from R2 using `r2_key`
4. Produce exactly one `schedule_version`
5. Transition session:
 - `processing → done` on success
 - `processing → failed` with `error` on failure

---

## Final Rule

> The database schema is **owned by SQL and enforced by triggers**.  
> The OCR worker must assume constraints are real and violations will raise errors.
```

---
