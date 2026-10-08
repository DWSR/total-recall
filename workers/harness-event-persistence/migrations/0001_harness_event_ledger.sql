CREATE TABLE session_events (
    receipt_id UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
    session_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('session_start', 'session_end')),
    project_name TEXT NOT NULL,
    current_working_directory TEXT NOT NULL,
    source_timestamp_rfc3339 TEXT NOT NULL,
    source_timestamp_utc TIMESTAMPTZ(3) NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp()
);

CREATE INDEX session_events_session_source_timestamp_idx
    ON session_events USING btree
    (session_id ASC, source_timestamp_utc ASC);

CREATE TABLE raw_observations (
    receipt_id UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
    session_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type = 'observation'),
    hook_type TEXT NOT NULL,
    project_name TEXT NOT NULL,
    current_working_directory TEXT NOT NULL,
    source_timestamp_rfc3339 TEXT NOT NULL,
    source_timestamp_utc TIMESTAMPTZ(3) NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    data JSON NOT NULL
);

CREATE INDEX raw_observations_session_source_timestamp_idx
    ON raw_observations USING btree
    (session_id ASC, source_timestamp_utc ASC);
