BEGIN;

CREATE TABLE IF NOT EXISTS schedule_ingest.schedule_notification (
    notification_id TEXT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    schedule_date DATE NOT NULL,
    source_session_id UUID NOT NULL,
    notification_type TEXT NOT NULL CHECK (notification_type IN ('event', 'summary')),
    message TEXT NOT NULL,
    event_ids JSONB NOT NULL CHECK (jsonb_typeof(event_ids) = 'array'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_schedule_notification_unsent_created
    ON schedule_ingest.schedule_notification (created_at ASC)
    WHERE sent_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_schedule_notification_user_date_created
    ON schedule_ingest.schedule_notification (user_id, schedule_date, created_at DESC);

COMMIT;
