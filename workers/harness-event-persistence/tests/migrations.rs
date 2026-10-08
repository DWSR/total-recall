use std::fs;

fn migration(name: &str) -> String {
    fs::read_to_string(format!("{}/migrations/{name}", env!("CARGO_MANIFEST_DIR")))
        .expect("migration should exist")
}

const REQUIRED_LEDGER_SQL: &str = r#"CREATE TABLE session_events (
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
"#;

const OPTIONAL_SESSION_EMBEDDINGS_SQL: &str = r#"CREATE TABLE session_embeddings (
    receipt_id UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
    session_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    dimensions INTEGER NOT NULL CHECK (dimensions > 0),
    embedding vector NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    CHECK (vector_dims(embedding) = dimensions)
);
"#;

#[test]
fn ledger_migration_matches_the_approved_physical_schema() {
    let migration = migration("0001_harness_event_ledger.sql");

    assert_eq!(migration, REQUIRED_LEDGER_SQL);
}

#[test]
fn required_ledger_has_only_the_approved_event_indexes_and_no_vector_dependency() {
    let ledger_migration = migration("0001_harness_event_ledger.sql");

    assert!(ledger_migration.contains("CREATE TABLE raw_observations ("));
    assert_eq!(ledger_migration.matches("uuid_generate_v7()").count(), 2);
    assert_eq!(
        ledger_migration
            .matches("source_timestamp_rfc3339 TEXT NOT NULL")
            .count(),
        2
    );
    assert_eq!(
        ledger_migration
            .matches("source_timestamp_utc TIMESTAMPTZ(3) NOT NULL")
            .count(),
        2
    );

    let expected_indexes = [
        "CREATE INDEX session_events_session_source_timestamp_idx\n    ON session_events USING btree\n    (session_id ASC, source_timestamp_utc ASC);",
        "CREATE INDEX raw_observations_session_source_timestamp_idx\n    ON raw_observations USING btree\n    (session_id ASC, source_timestamp_utc ASC);",
    ];
    assert_eq!(ledger_migration.matches("CREATE INDEX ").count(), 2);
    for index in expected_indexes {
        assert_eq!(ledger_migration.matches(index).count(), 1);
    }

    for forbidden in ["pgvector", "session_embeddings", "vector"] {
        assert!(
            !ledger_migration.contains(forbidden),
            "the required ledger must not reference optional vector storage"
        );
    }
}

#[test]
fn optional_embedding_migration_is_exact_and_keeps_the_ledger_independent() {
    let ledger_migration = migration("0001_harness_event_ledger.sql");
    let embedding_migration = migration("0002_session_embeddings.sql");

    assert_eq!(embedding_migration, OPTIONAL_SESSION_EMBEDDINGS_SQL);
    assert!(
        !ledger_migration.contains("session_embeddings"),
        "the required ledger must not depend on the optional embedding table"
    );
}
