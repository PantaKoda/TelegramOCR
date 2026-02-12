-- Add lease support for SKIP LOCKED claiming.
-- Safe to run multiple times.

ALTER TYPE schedule_ingest.capture_session_state
ADD VALUE IF NOT EXISTS 'pending';

ALTER TABLE schedule_ingest.capture_session
ADD COLUMN IF NOT EXISTS locked_at timestamptz NULL,
ADD COLUMN IF NOT EXISTS locked_by text NULL;

CREATE INDEX IF NOT EXISTS ix_capture_session_claim
ON schedule_ingest.capture_session (state, created_at, locked_at);
