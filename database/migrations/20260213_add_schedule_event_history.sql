BEGIN;

CREATE TABLE IF NOT EXISTS schedule_ingest.day_snapshot (
    user_id BIGINT NOT NULL,
    schedule_date DATE NOT NULL,
    snapshot_payload JSONB NOT NULL CHECK (jsonb_typeof(snapshot_payload) = 'array'),
    source_session_id UUID NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, schedule_date)
);

CREATE TABLE IF NOT EXISTS schedule_ingest.schedule_event (
    event_id UUID PRIMARY KEY,
    user_id BIGINT NOT NULL,
    schedule_date DATE NOT NULL,
    event_type TEXT NOT NULL CHECK (
        event_type IN (
            'shift_added',
            'shift_removed',
            'shift_time_changed',
            'shift_relocated',
            'shift_retitled',
            'shift_reclassified'
        )
    ),
    location_fingerprint TEXT NOT NULL,
    customer_fingerprint TEXT NOT NULL,
    old_value JSONB NULL,
    new_value JSONB NULL,
    detected_at TIMESTAMPTZ NOT NULL,
    source_session_id UUID NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_schedule_event_user_date_detected
    ON schedule_ingest.schedule_event (user_id, schedule_date, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_schedule_event_source_session
    ON schedule_ingest.schedule_event (source_session_id);

COMMIT;

