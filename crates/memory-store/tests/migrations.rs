const RECENCY_MIGRATION: &str = include_str!("../migrations/0002_memory_search_head_recency.sql");

#[test]
fn recency_migration_is_forward_transactional_and_finite() {
    assert!(RECENCY_MIGRATION.contains("SET LOCAL lock_timeout"));
    assert!(RECENCY_MIGRATION.contains("SET LOCAL statement_timeout"));
    assert!(RECENCY_MIGRATION.contains("ADD COLUMN updated_at TIMESTAMPTZ"));
    assert!(RECENCY_MIGRATION.contains("VALIDATE CONSTRAINT"));
    assert!(RECENCY_MIGRATION.contains("SET NOT NULL"));
    assert!(RECENCY_MIGRATION.contains("SECURITY DEFINER"));
    assert!(RECENCY_MIGRATION.contains("SET search_path = pg_catalog, pg_temp"));
    assert!(RECENCY_MIGRATION.contains("WHERE memory_search_head.version < EXCLUDED.version"));
    assert!(RECENCY_MIGRATION.contains("REVOKE ALL ON FUNCTION"));
    assert!(!RECENCY_MIGRATION.contains("CREATE INDEX"));
    assert!(RECENCY_MIGRATION.contains("CREATE OR REPLACE FUNCTION"));
}

#[test]
fn recency_migration_backfills_the_canonical_row_selected_by_head_identity() {
    let backfill = RECENCY_MIGRATION
        .split("UPDATE public.memory_search_heads")
        .nth(1)
        .expect("migration should backfill existing heads");

    assert!(backfill.contains("memory.id = memory_search_head.id"));
    assert!(backfill.contains("memory.version = memory_search_head.version"));
    assert!(backfill.contains("SET updated_at = memory.updated_at"));
}
