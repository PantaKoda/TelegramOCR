BEGIN;

CREATE TABLE IF NOT EXISTS schedule_ingest.schedule_notification (
    notification_id TEXT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    schedule_date DATE NOT NULL,
    source_session_id UUID NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'failed')),
    notification_type TEXT NOT NULL CHECK (notification_type IN ('event', 'summary')),
    message TEXT NOT NULL,
    event_ids JSONB NOT NULL CHECK (jsonb_typeof(event_ids) = 'array'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ NULL
);

ALTER TABLE schedule_ingest.schedule_notification
    ADD COLUMN IF NOT EXISTS status TEXT;

UPDATE schedule_ingest.schedule_notification
SET status = 'pending'
WHERE status IS NULL;

ALTER TABLE schedule_ingest.schedule_notification
    ALTER COLUMN status SET DEFAULT 'pending',
    ALTER COLUMN status SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'schedule_notification_status_check'
    ) THEN
        ALTER TABLE schedule_ingest.schedule_notification
            ADD CONSTRAINT schedule_notification_status_check
            CHECK (status IN ('pending', 'sent', 'failed'));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_schedule_notification_unsent_created
    ON schedule_ingest.schedule_notification (created_at ASC)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_schedule_notification_user_date_created
    ON schedule_ingest.schedule_notification (user_id, schedule_date, created_at DESC);

COMMIT;
