use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use chrono::{DateTime, Utc};
use mcp_worker::{
    contracts::{MemoryDto, MemoryResults, MemorySearchInput, SaveInput},
    embedding::{
        FailingQueryEmbeddingGenerator, QueryEmbeddingError, RecordingQueryEmbeddingGenerator,
    },
    operations::{
        BackendResponseField, BackendSource, FailingOperations, MemoryOperationFailures,
        MemoryOperationResponses, Operation, OperationCall, OperationError, OperationInputField,
        RecordingOperations,
    },
    service::{CreateContext, MemoryToolService, ProductionCreateContext, ToolError},
};
use memory_store::contracts::{
    Bm25Search, EmbeddingVector, MemorySearchResult, MemoryVersionInput, VectorSearch,
};
use serde_json::json;
use uuid::{Uuid, Version};

const ID: &str = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
const TITLE: &str = " title-secret-sentinel ";
const CONTENT: &str = " content-secret-sentinel ";
const SESSION_ID: &str = " session-secret-sentinel ";
const SEARCH_QUERY: &str = " \tsearch-query-secret-sentinel ";
const SEARCH_LIMIT: u32 = 4;
const GENERATED_VECTOR: &[f64] = &[0.25, -0.5];

#[tokio::test]
async fn save_assigns_worker_defaults_and_returns_after_insert_confirmation() {
    for generator in generator_compositions() {
        let generator_observer = generator.clone();
        let expected_memory = expected_memory();
        let operations = RecordingOperations::new(successful_responses());
        let observer = operations.clone();
        let context = DeterministicCreateContext::new(ID, timestamp());
        let service = MemoryToolService::new(operations, context.clone(), generator);

        let memory = service
            .save(valid_input())
            .await
            .expect("configured insert should return the canonical memory");

        assert_eq!(memory, MemoryDto::from(expected_memory.clone()));
        assert_eq!(context.id_calls(), 1);
        assert_eq!(context.now_calls(), 1);
        assert_eq!(
            observer.calls(),
            vec![OperationCall::InsertMemory(expected_memory)]
        );
        assert_generator_not_invoked(&generator_observer);
    }
}

#[tokio::test]
async fn save_rejects_each_empty_field_without_context_or_persistence() {
    for generator in generator_compositions() {
        for (field, input) in [
            (
                OperationInputField::Title,
                SaveInput {
                    title: String::new(),
                    content: CONTENT.to_owned(),
                    session_id: SESSION_ID.to_owned(),
                },
            ),
            (
                OperationInputField::Content,
                SaveInput {
                    title: TITLE.to_owned(),
                    content: String::new(),
                    session_id: SESSION_ID.to_owned(),
                },
            ),
            (
                OperationInputField::SessionId,
                SaveInput {
                    title: TITLE.to_owned(),
                    content: CONTENT.to_owned(),
                    session_id: String::new(),
                },
            ),
        ] {
            let operations = RecordingOperations::new(successful_responses());
            let observer = operations.clone();
            let context = DeterministicCreateContext::new(ID, timestamp());
            let service = MemoryToolService::new(operations, context.clone(), generator.clone());

            let error = service
                .save(input)
                .await
                .expect_err("empty create input should be rejected");

            assert_eq!(error, ToolError::InvalidInput { field });
            assert_eq!(error.code(), "invalid_input");
            assert!(observer.calls().is_empty());
            assert_eq!(context.id_calls(), 0);
            assert_eq!(context.now_calls(), 0);
        }
        assert_generator_not_invoked(&generator);
    }
}

#[tokio::test]
async fn save_maps_closed_port_errors_to_stable_content_safe_tool_errors() {
    for generator in generator_compositions() {
        for (operation_error, expected_error) in [
            (
                OperationError::invalid_input(Operation::InsertMemory, OperationInputField::Memory),
                ToolError::InvalidInput {
                    field: OperationInputField::Memory,
                },
            ),
            (
                OperationError::not_found(Operation::InsertMemory),
                ToolError::NotFound,
            ),
            (
                OperationError::conflict(Operation::InsertMemory),
                ToolError::Conflict,
            ),
            (
                OperationError::backend_failure(
                    Operation::InsertMemory,
                    BackendSource::MemoryStore,
                ),
                ToolError::BackendFailure,
            ),
            (
                OperationError::invalid_backend_response(
                    Operation::InsertMemory,
                    BackendResponseField::InsertConfirmation,
                ),
                ToolError::InternalFailure,
            ),
        ] {
            let operations = FailingOperations::new(failing_responses(operation_error));
            let observer = operations.clone();
            let context = DeterministicCreateContext::new(ID, timestamp());
            let service = MemoryToolService::new(operations, context.clone(), generator.clone());

            let error = service
                .save(valid_input())
                .await
                .expect_err("a port failure must not report a created memory");

            assert_eq!(error, expected_error);
            assert_eq!(error.to_string(), error.code());
            assert_error_is_opaque(&error);
            assert_eq!(context.id_calls(), 1);
            assert_eq!(context.now_calls(), 1);
            assert_eq!(
                observer.calls(),
                vec![OperationCall::InsertMemory(expected_memory())]
            );
        }
        assert_generator_not_invoked(&generator);
    }
}

#[test]
fn save_input_does_not_accept_an_embedding() {
    assert!(
        serde_json::from_value::<SaveInput>(json!({
            "title": TITLE,
            "content": CONTENT,
            "session_id": SESSION_ID,
            "embedding": [1.0],
        }))
        .is_err()
    );
}

#[tokio::test]
async fn search_returns_bm25_results_alone_when_embedding_is_disabled_or_generation_fails() {
    for generator in [
        None,
        Some(FailingQueryEmbeddingGenerator::new(
            QueryEmbeddingError::Unavailable,
        )),
        Some(FailingQueryEmbeddingGenerator::new(
            QueryEmbeddingError::InvalidResponse,
        )),
    ] {
        let generator_observer = generator.clone();
        let lexical_b = search_memory("search-memory-b", 2, "lexical-b");
        let lexical_a = search_memory("search-memory-a", 1, "lexical-a");
        let mut responses = successful_responses();
        responses.search_lexical = Ok(vec![search_result(&lexical_b), search_result(&lexical_a)]);
        responses.search_vector = Ok(vec![search_result(&search_memory(
            "search-memory-0",
            9,
            "unrequested-vector",
        ))]);
        let operations = RecordingOperations::new(responses);
        let observer = operations.clone();
        let service = MemoryToolService::new(operations, ProductionCreateContext, generator);

        let results = service
            .search(search_input())
            .await
            .expect("BM25 results should be returned without a generated vector");

        assert_eq!(
            results,
            MemoryResults {
                results: vec![MemoryDto::from(lexical_a), MemoryDto::from(lexical_b)],
            }
        );
        assert_eq!(observer.calls(), vec![lexical_search_call()]);
        if let Some(generator) = generator_observer {
            assert_eq!(generator.calls(), vec![SEARCH_QUERY.to_owned()]);
        }
    }
}

#[tokio::test]
async fn search_merges_bm25_and_generated_vector_results_when_embedding_succeeds() {
    let generator = RecordingQueryEmbeddingGenerator::new(Ok(generated_embedding()));
    let generator_observer = generator.clone();
    let lexical_b = search_memory("search-memory-b", 1, "lexical-b");
    let vector_a = search_memory("search-memory-a", 1, "vector-a");
    let vector_b = search_memory("search-memory-b", 2, "vector-b");
    let mut responses = successful_responses();
    responses.search_lexical = Ok(vec![search_result(&lexical_b)]);
    responses.search_vector = Ok(vec![search_result(&vector_b), search_result(&vector_a)]);
    let operations = RecordingOperations::new(responses);
    let observer = operations.clone();
    let service = MemoryToolService::new(operations, ProductionCreateContext, Some(generator));

    let results = service
        .search(search_input())
        .await
        .expect("BM25 and generated-vector results should be merged");

    assert_eq!(
        results,
        MemoryResults {
            results: vec![MemoryDto::from(vector_a), MemoryDto::from(vector_b)],
        }
    );
    assert_eq!(
        observer.calls(),
        vec![
            lexical_search_call(),
            OperationCall::SearchVector(VectorSearch {
                vector: GENERATED_VECTOR.to_vec(),
                limit: SEARCH_LIMIT,
            }),
        ]
    );
    assert_eq!(generator_observer.calls(), vec![SEARCH_QUERY.to_owned()]);
}

#[test]
fn search_input_does_not_accept_a_caller_vector() {
    assert!(
        serde_json::from_value::<MemorySearchInput>(json!({
            "query": SEARCH_QUERY,
            "vector": [1.25, -2.5],
            "limit": SEARCH_LIMIT,
        }))
        .is_err()
    );
}

#[test]
fn production_create_context_uses_uuid_v4_and_second_aligned_utc() {
    let context = ProductionCreateContext;
    let id = Uuid::parse_str(&context.new_id()).expect("production IDs should be UUIDs");

    assert_eq!(id.get_version(), Some(Version::Random));
    assert_eq!(context.now().timestamp_subsec_nanos(), 0);
}

fn valid_input() -> SaveInput {
    SaveInput {
        title: TITLE.to_owned(),
        content: CONTENT.to_owned(),
        session_id: SESSION_ID.to_owned(),
    }
}

fn expected_memory() -> MemoryVersionInput {
    MemoryVersionInput {
        id: ID.to_owned(),
        version: 1,
        memory_type: "unclassified".to_owned(),
        title: TITLE.to_owned(),
        content: CONTENT.to_owned(),
        created_at: timestamp(),
        updated_at: timestamp(),
        concepts: Vec::new(),
        files: Vec::new(),
        session_ids: vec![SESSION_ID.to_owned()],
        source_observation_ids: Vec::new(),
    }
}

fn successful_responses() -> MemoryOperationResponses {
    MemoryOperationResponses {
        insert_memory: Ok(()),
        search_lexical: Ok(Vec::new()),
        search_vector: Ok(Vec::new()),
        get_latest: Ok(expected_memory()),
        get_exact: Ok(expected_memory()),
        list_versions: Ok(Vec::new()),
    }
}

fn failing_responses(error: OperationError) -> MemoryOperationFailures {
    MemoryOperationFailures {
        insert_memory: error.clone(),
        search_lexical: error.clone(),
        search_vector: error.clone(),
        get_latest: error.clone(),
        get_exact: error.clone(),
        list_versions: error,
    }
}

fn timestamp() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-09-20T12:34:56Z")
        .expect("test timestamp should parse")
        .with_timezone(&Utc)
}

fn generator_compositions() -> [Option<RecordingQueryEmbeddingGenerator>; 2] {
    [
        None,
        Some(RecordingQueryEmbeddingGenerator::new(Ok(
            generated_embedding(),
        ))),
    ]
}

fn generated_embedding() -> EmbeddingVector {
    EmbeddingVector::try_from(GENERATED_VECTOR.to_vec())
        .expect("the generated test vector should be a canonical embedding")
}

fn search_input() -> MemorySearchInput {
    MemorySearchInput {
        query: SEARCH_QUERY.to_owned(),
        limit: SEARCH_LIMIT,
    }
}

fn lexical_search_call() -> OperationCall {
    OperationCall::SearchLexical(Bm25Search {
        query: SEARCH_QUERY.to_owned(),
        limit: SEARCH_LIMIT,
    })
}

fn search_memory(id: &str, version: i64, marker: &str) -> MemoryVersionInput {
    MemoryVersionInput {
        id: id.to_owned(),
        version,
        memory_type: "unclassified".to_owned(),
        title: format!("{marker} title"),
        content: format!("{marker} content"),
        created_at: timestamp(),
        updated_at: timestamp(),
        concepts: vec![format!("{marker} concept")],
        files: Vec::new(),
        session_ids: vec![format!("{marker} session")],
        source_observation_ids: Vec::new(),
    }
}

fn search_result(memory: &MemoryVersionInput) -> MemorySearchResult {
    MemorySearchResult::try_new(
        memory
            .clone()
            .try_into()
            .expect("test memory should be a valid search result"),
        0.5,
    )
    .expect("test relevance should be valid")
}

fn assert_generator_not_invoked(generator: &Option<RecordingQueryEmbeddingGenerator>) {
    if let Some(generator) = generator {
        assert!(
            generator.calls().is_empty(),
            "the query embedding generator should not be invoked"
        );
    }
}

#[derive(Clone)]
struct DeterministicCreateContext {
    id: String,
    now: DateTime<Utc>,
    id_calls: Arc<AtomicUsize>,
    now_calls: Arc<AtomicUsize>,
}

impl DeterministicCreateContext {
    fn new(id: &str, now: DateTime<Utc>) -> Self {
        Self {
            id: id.to_owned(),
            now,
            id_calls: Arc::new(AtomicUsize::new(0)),
            now_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn id_calls(&self) -> usize {
        self.id_calls.load(Ordering::SeqCst)
    }

    fn now_calls(&self) -> usize {
        self.now_calls.load(Ordering::SeqCst)
    }
}

impl CreateContext for DeterministicCreateContext {
    fn new_id(&self) -> String {
        self.id_calls.fetch_add(1, Ordering::SeqCst);
        self.id.clone()
    }

    fn now(&self) -> DateTime<Utc> {
        self.now_calls.fetch_add(1, Ordering::SeqCst);
        self.now
    }
}

fn assert_error_is_opaque(error: &ToolError) {
    for protected_value in [
        TITLE,
        CONTENT,
        SESSION_ID,
        "backend-secret-sentinel",
        "memory_store",
    ] {
        assert!(
            !error.to_string().contains(protected_value),
            "Display error leaked protected value: {error}"
        );
        assert!(
            !format!("{error:?}").contains(protected_value),
            "Debug error leaked protected value: {error:?}"
        );
    }
}
