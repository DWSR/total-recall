use std::{
    collections::VecDeque,
    fmt::{Debug, Display},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use iii_sdk::{Error as IiiError, protocol::TriggerRequest};
use memory_store::{
    MemoryStore,
    contracts::{
        DatabaseError, DatabaseOperation, DatabaseTarget, MemorySearchResult, MemoryVersionInput,
        ValidatedBm25Search, ValidatedEmbedding, ValidatedMemoryVersion, ValidatedVectorSearch,
    },
    database::MemoryDatabase,
};
use serde::Serialize;
use serde_json::{Value, json};
use session_post_processing::{
    ensure_memory_with, lookup_exact_with,
    ports::{EnsureMemoryOutcome, MemoryPublishError},
};

const DATABASE: &str = "session-post-processing-memory-test";
const MEMORY_ID: &str = "33333333-3333-5333-8333-333333333333";
const TITLE: &str = "memory-title-secret-sentinel";
const CONTENT: &str = "memory-content-secret-sentinel";
const SESSION_ID: &str = "session-id-secret-sentinel";
const RECEIPT_ID: &str = "44444444-4444-4444-8444-444444444444";
const TOKEN: &str = "provider-token-secret-sentinel";
const DATABASE_BODY: &str = "database-body-secret-sentinel";

#[tokio::test]
async fn confirmed_insert_is_reported_without_a_lookup() {
    let database = ScriptedMemoryDatabase::new([Ok(())]);
    let memory_store = MemoryStore::new(database.clone());

    let outcome = ensure_memory_with(&memory_store, staged_memory(), |_, _| async {
        panic!("a confirmed insert must not perform an exact lookup")
    })
    .await
    .expect("a confirmed canonical insert should succeed");

    assert_eq!(outcome, EnsureMemoryOutcome::Inserted);
    assert_eq!(database.insert_calls(), 1);
}

#[tokio::test]
async fn exact_equivalence_recovers_conflicts_and_timeout_after_commit() {
    for insert_outcome in [
        DatabaseError::conflict(DatabaseOperation::InsertMemory),
        DatabaseError::database_failure(DatabaseOperation::InsertMemory),
    ] {
        let database = ScriptedMemoryDatabase::new([Err(insert_outcome)]);
        let memory_store = MemoryStore::new(database.clone());
        let staged = staged_memory();
        let expected = staged.clone();

        let outcome = ensure_memory_with(&memory_store, staged.clone(), move |id, version| {
            let expected = expected.clone();
            async move {
                assert_eq!(id, staged.id);
                assert_eq!(version, staged.version);
                Ok(Some(expected))
            }
        })
        .await
        .expect("an exact committed row should recover the unknown insert outcome");

        assert_eq!(outcome, EnsureMemoryOutcome::ExistingEquivalent);
        assert_eq!(database.insert_calls(), 1);
    }
}

#[tokio::test]
async fn conflict_with_a_missing_exact_row_remains_a_failure() {
    let database = ScriptedMemoryDatabase::new([Err(DatabaseError::conflict(
        DatabaseOperation::InsertMemory,
    ))]);
    let memory_store = MemoryStore::new(database.clone());

    let error = ensure_memory_with(&memory_store, staged_memory(), |_, _| async { Ok(None) })
        .await
        .expect_err("a conflict without a matching stored row must not be successful");

    assert_eq!(error, MemoryPublishError::mismatch());
    assert_eq!(database.insert_calls(), 1);
    assert_safe_error(&error, protected_values());
}

#[tokio::test]
async fn conflicts_reject_mismatches_in_every_canonical_field() {
    let staged = staged_memory();

    for stored in canonical_field_mismatches(&staged) {
        let database = ScriptedMemoryDatabase::new([Err(DatabaseError::conflict(
            DatabaseOperation::InsertMemory,
        ))]);
        let memory_store = MemoryStore::new(database.clone());

        let error = ensure_memory_with(&memory_store, staged.clone(), move |_, _| {
            let stored = stored.clone();
            async move { Ok(Some(stored)) }
        })
        .await
        .expect_err("a mismatched canonical field must not be accepted as equivalent");

        assert_eq!(error, MemoryPublishError::mismatch());
        assert_eq!(database.insert_calls(), 1);
        assert_safe_error(&error, protected_values());
    }
}

#[tokio::test]
async fn conflicts_compare_ordered_canonical_arrays_without_normalizing_them() {
    let staged = staged_memory();

    for stored in ordered_array_mismatches(&staged) {
        let database = ScriptedMemoryDatabase::new([Err(DatabaseError::conflict(
            DatabaseOperation::InsertMemory,
        ))]);
        let memory_store = MemoryStore::new(database.clone());

        let error = ensure_memory_with(&memory_store, staged.clone(), move |_, _| {
            let stored = stored.clone();
            async move { Ok(Some(stored)) }
        })
        .await
        .expect_err("reordered canonical arrays must not be accepted as equivalent");

        assert_eq!(error, MemoryPublishError::mismatch());
        assert_eq!(database.insert_calls(), 1);
    }
}

#[tokio::test]
async fn lookup_transport_and_malformed_envelope_failures_remain_opaque() {
    let database = database_target();
    let transport_error = lookup_exact_with(&database, MEMORY_ID, 1, |_| async {
        Err(IiiError::Remote {
            code: "database-code-secret-sentinel".to_owned(),
            message: DATABASE_BODY.to_owned(),
            stacktrace: Some(TOKEN.to_owned()),
        })
    })
    .await
    .expect_err("lookup transport failures must be opaque");
    assert_eq!(transport_error, MemoryPublishError::unknown());
    assert_safe_error(&transport_error, protected_values());

    let malformed_error = lookup_exact_with(&database, MEMORY_ID, 1, |_| async {
        Ok(json!({
            "affected_rows": 1,
            "last_insert_id": null,
            "returned_rows": [],
            "backend_body": DATABASE_BODY,
        }))
    })
    .await
    .expect_err("lookup envelopes with unexpected fields must fail");
    assert_eq!(malformed_error, MemoryPublishError::unknown());
    assert_safe_error(&malformed_error, protected_values());

    let insert_database = ScriptedMemoryDatabase::new([Err(DatabaseError::conflict(
        DatabaseOperation::InsertMemory,
    ))]);
    let memory_store = MemoryStore::new(insert_database);
    let error = ensure_memory_with(&memory_store, staged_memory(), move |_, _| {
        let transport_error = transport_error;
        async move { Err(transport_error) }
    })
    .await
    .expect_err("a lookup failure must not be converted into publication success");
    assert_eq!(error, MemoryPublishError::unknown());
    assert_safe_error(&error, protected_values());
}

#[tokio::test]
async fn exact_lookup_uses_static_parameterized_read_only_sql_and_revalidates_one_row() {
    let staged = staged_memory();
    let expected = staged.clone();
    let requested = staged.clone();
    let id = staged.id.clone();

    let result = lookup_exact_with(&database_target(), &id, staged.version, move |request| {
        let expected = expected.clone();
        let requested = requested.clone();
        async move {
            assert_exact_lookup_request(&request, &requested);
            Ok(exact_lookup_response(vec![canonical_row(&expected)]))
        }
    })
    .await
    .expect("one valid exact row should decode");

    assert_eq!(result, Some(staged));
}

#[tokio::test]
async fn exact_lookup_distinguishes_missing_malformed_and_multiple_rows_safely() {
    let staged = staged_memory();
    let valid_row = canonical_row(&staged);
    let mut malformed_row = valid_row.clone();
    malformed_row["title"] = json!("");

    let missing = lookup_exact_with(&database_target(), &staged.id, staged.version, |_| async {
        Ok(exact_lookup_response(Vec::new()))
    })
    .await
    .expect("a successful empty exact lookup should remain distinguishable from failure");
    assert_eq!(missing, None);

    for response in [
        json!(null),
        json!({
            "affected_rows": 1,
            "last_insert_id": null,
            "returned_rows": [valid_row.clone()],
            "backend_body": DATABASE_BODY,
        }),
        exact_lookup_response(vec![malformed_row]),
        exact_lookup_response(vec![valid_row.clone(), valid_row]),
    ] {
        let error = lookup_exact_with(&database_target(), &staged.id, staged.version, |_| async {
            Ok(response)
        })
        .await
        .expect_err("malformed and ambiguous lookup responses must fail");
        assert_eq!(error, MemoryPublishError::unknown());
        assert_safe_error(&error, protected_values());
    }
}

#[tokio::test]
async fn invalid_payload_fails_without_insert_or_lookup() {
    let database = ScriptedMemoryDatabase::new([Ok(())]);
    let memory_store = MemoryStore::new(database.clone());
    let mut invalid = staged_memory();
    invalid.title.clear();
    let lookup_calls = Arc::new(Mutex::new(0_usize));
    let calls = Arc::clone(&lookup_calls);

    let error = ensure_memory_with(&memory_store, invalid, move |_, _| {
        let calls = Arc::clone(&calls);
        async move {
            *calls
                .lock()
                .expect("lookup count lock should not be poisoned") += 1;
            Ok(None)
        }
    })
    .await
    .expect_err("invalid canonical inputs must not be published");

    assert_eq!(error, MemoryPublishError::unknown());
    assert_eq!(database.insert_calls(), 0);
    assert_eq!(*lookup_calls.lock().unwrap(), 0);
    assert_safe_error(&error, protected_values());
}

#[derive(Clone)]
struct ScriptedMemoryDatabase {
    state: Arc<Mutex<ScriptedMemoryDatabaseState>>,
}

struct ScriptedMemoryDatabaseState {
    insert_outcomes: VecDeque<Result<(), DatabaseError>>,
    insert_calls: usize,
}

impl ScriptedMemoryDatabase {
    fn new(outcomes: impl IntoIterator<Item = Result<(), DatabaseError>>) -> Self {
        Self {
            state: Arc::new(Mutex::new(ScriptedMemoryDatabaseState {
                insert_outcomes: outcomes.into_iter().collect(),
                insert_calls: 0,
            })),
        }
    }

    fn insert_calls(&self) -> usize {
        self.state
            .lock()
            .expect("scripted database lock should not be poisoned")
            .insert_calls
    }
}

#[async_trait]
impl MemoryDatabase for ScriptedMemoryDatabase {
    async fn insert_memory(&self, _: &ValidatedMemoryVersion) -> Result<(), DatabaseError> {
        let mut state = self
            .state
            .lock()
            .expect("scripted database lock should not be poisoned");
        state.insert_calls += 1;
        state
            .insert_outcomes
            .pop_front()
            .expect("scripted database received an unexpected insert")
    }

    async fn insert_embedding(&self, _: &ValidatedEmbedding) -> Result<(), DatabaseError> {
        panic!("memory publisher must not insert embeddings")
    }

    async fn search_bm25(
        &self,
        _: &ValidatedBm25Search,
    ) -> Result<Vec<MemorySearchResult>, DatabaseError> {
        panic!("memory publisher must not search canonical memories")
    }

    async fn search_vector(
        &self,
        _: &ValidatedVectorSearch,
    ) -> Result<Vec<MemorySearchResult>, DatabaseError> {
        panic!("memory publisher must not search canonical memories")
    }
}

fn database_target() -> DatabaseTarget {
    DatabaseTarget::try_from(DATABASE.to_owned()).expect("test database target should be valid")
}

fn staged_memory() -> MemoryVersionInput {
    MemoryVersionInput {
        id: MEMORY_ID.to_owned(),
        version: 1,
        memory_type: "session-derived".to_owned(),
        title: TITLE.to_owned(),
        content: CONTENT.to_owned(),
        created_at: timestamp("2026-09-22T12:34:56Z"),
        updated_at: timestamp("2026-09-22T12:35:56Z"),
        concepts: vec!["concept-first".to_owned(), "concept-second".to_owned()],
        files: vec!["file-first".to_owned(), "file-second".to_owned()],
        session_ids: vec![
            SESSION_ID.to_owned(),
            "session-second-secret-sentinel".to_owned(),
        ],
        source_observation_ids: vec![
            RECEIPT_ID.to_owned(),
            "55555555-5555-4555-8555-555555555555".to_owned(),
        ],
    }
}

fn canonical_field_mismatches(staged: &MemoryVersionInput) -> Vec<MemoryVersionInput> {
    let mut id = staged.clone();
    id.id = "other-memory-id-secret-sentinel".to_owned();
    let mut version = staged.clone();
    version.version = 2;
    let mut memory_type = staged.clone();
    memory_type.memory_type = "other-memory-type".to_owned();
    let mut title = staged.clone();
    title.title = "other-memory-title-secret-sentinel".to_owned();
    let mut content = staged.clone();
    content.content = "other-memory-content-secret-sentinel".to_owned();
    let mut created_at = staged.clone();
    created_at.created_at = timestamp("2026-09-22T12:34:55Z");
    let mut updated_at = staged.clone();
    updated_at.updated_at = timestamp("2026-09-22T12:35:57Z");
    let mut concepts = staged.clone();
    concepts.concepts = vec!["other-concept".to_owned()];
    let mut files = staged.clone();
    files.files = vec!["other-file".to_owned()];
    let mut session_ids = staged.clone();
    session_ids.session_ids = vec!["other-session-id-secret-sentinel".to_owned()];
    let mut source_observation_ids = staged.clone();
    source_observation_ids.source_observation_ids =
        vec!["other-receipt-id-secret-sentinel".to_owned()];

    vec![
        id,
        version,
        memory_type,
        title,
        content,
        created_at,
        updated_at,
        concepts,
        files,
        session_ids,
        source_observation_ids,
    ]
}

fn ordered_array_mismatches(staged: &MemoryVersionInput) -> Vec<MemoryVersionInput> {
    let mut concepts = staged.clone();
    concepts.concepts.reverse();
    let mut files = staged.clone();
    files.files.reverse();
    let mut session_ids = staged.clone();
    session_ids.session_ids.reverse();
    let mut source_observation_ids = staged.clone();
    source_observation_ids.source_observation_ids.reverse();

    vec![concepts, files, session_ids, source_observation_ids]
}

fn exact_lookup_response(rows: Vec<Value>) -> Value {
    json!({
        "affected_rows": rows.len(),
        "last_insert_id": null,
        "returned_rows": rows,
    })
}

fn canonical_row(memory: &MemoryVersionInput) -> Value {
    json!({
        "id": memory.id,
        "version": memory.version.to_string(),
        "memory_type": memory.memory_type,
        "title": memory.title,
        "content": memory.content,
        "created_at": memory.created_at.to_rfc3339(),
        "updated_at": memory.updated_at.to_rfc3339(),
        "concepts": memory.concepts,
        "files": memory.files,
        "session_ids": memory.session_ids,
        "source_observation_ids": memory.source_observation_ids,
    })
}

fn assert_exact_lookup_request(request: &TriggerRequest, memory: &MemoryVersionInput) {
    assert_eq!(request.function_id, "database::execute");
    assert_eq!(request.payload["db"], DATABASE);
    assert_eq!(
        request.payload["params"],
        json!([memory.id, memory.version.to_string()])
    );
    assert!(request.action.is_none());
    assert_eq!(request.timeout_ms, None);

    let sql = request.payload["sql"]
        .as_str()
        .expect("exact lookup must use text SQL")
        .to_ascii_lowercase();
    assert!(sql.trim_start().starts_with("select"));
    assert!(sql.contains("from public.memories"));
    assert!(sql.contains("memory.id = $1::text"));
    assert!(sql.contains("memory.version = $2::text::bigint"));
    for forbidden in ["insert", "update", "delete", "merge"] {
        assert!(
            !sql.split(|character: char| !character.is_ascii_alphabetic())
                .any(|token| token == forbidden),
            "exact lookup SQL must not contain {forbidden}"
        );
    }
    for forbidden in [
        "memory_embeddings",
        "embedding",
        "search_document",
        "relevance",
    ] {
        assert!(
            !sql.contains(forbidden),
            "exact lookup SQL must not contain {forbidden}"
        );
    }
}

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("timestamp fixture should be valid")
        .with_timezone(&Utc)
}

fn protected_values() -> &'static [&'static str] {
    &[
        MEMORY_ID,
        TITLE,
        CONTENT,
        SESSION_ID,
        RECEIPT_ID,
        TOKEN,
        DATABASE_BODY,
        "other-memory-id-secret-sentinel",
        "other-memory-title-secret-sentinel",
        "other-memory-content-secret-sentinel",
        "other-session-id-secret-sentinel",
        "other-receipt-id-secret-sentinel",
    ]
}

fn assert_safe_error<E>(error: &E, values: &[&str])
where
    E: Debug + Display + Serialize,
{
    let display = error.to_string();
    let debug = format!("{error:?}");
    let serialized = serde_json::to_string(error).expect("error should serialize safely");

    for value in values {
        assert!(
            !display.contains(value),
            "Display error leaked protected value: {display}"
        );
        assert!(
            !debug.contains(value),
            "Debug error leaked protected value: {debug}"
        );
        assert!(
            !serialized.contains(value),
            "serialized error leaked protected value: {serialized}"
        );
    }
}
