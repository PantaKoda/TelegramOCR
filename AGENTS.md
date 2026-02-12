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
- Select sessions in `state = processing`
- Download images from Cloudflare R2
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

open → closed → processing → done | failed

```

Rules:
- You will only see sessions in `processing`
- A `processing` session is frozen and exclusive
- Each session must be processed **exactly once**

---

## Non-Negotiable Design Rules

- Do NOT run OCR per image (only per session)
- Do NOT infer grouping heuristically
- Do NOT stitch images together
- Do NOT overwrite existing schedule versions
- Do NOT guess dates from timestamps or filenames
- Date identity must come from OCR UI text
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
- Dispatcher transitions `closed → processing`
- Phase 2 session-finalization stub implemented in Python (`main.py`)
- Current worker behavior:
  - selects at most one `capture_session` row where `state = processing` per run
  - inserts one deterministic stub `schedule_version` payload (`{"stub": true}`)
  - uses fixed hardcoded `schedule_date` and fixed `version = 1` (Phase 2 stub behavior)
  - computes deterministic `payload_hash`
  - transitions session `processing → done` on success
  - transitions session `processing → failed` with error on failure
- OCR extraction, image download, and schedule parsing are not implemented in Phase 2

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
