use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Timelike, Utc};
use memory_store::{
    MemoryStore,
    contracts::{
        Bm25Search, DatabaseError, DatabaseOperation, EmbeddingInput, MemorySearchResult,
        MemoryStoreError, MemoryVersionInput, ValidatedBm25Search, ValidatedEmbedding,
        ValidatedMemoryVersion, ValidatedVectorSearch, VectorSearch,
    },
    database::{IiiMemoryDatabase, MemoryDatabase},
};

#[derive(Default)]
struct RecordingState {
    operations: Vec<DatabaseOperation>,
    bm25_results: Vec<MemorySearchResult>,
    vector_results: Vec<MemorySearchResult>,
}

#[derive(Clone, Default)]
struct RecordingDatabase {
    state: Arc<Mutex<RecordingState>>,
}

impl RecordingDatabase {
    fn with_results(
        bm25_results: Vec<MemorySearchResult>,
        vector_results: Vec<MemorySearchResult>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(RecordingState {
                operations: Vec::new(),
                bm25_results,
                vector_results,
            })),
        }
    }

    fn record(&self, operation: DatabaseOperation) {
        self.state
            .lock()
            .expect("recording database lock should not be poisoned")
            .operations
            .push(operation);
    }

    fn operations(&self) -> Vec<DatabaseOperation> {
        self.state
            .lock()
            .expect("recording database lock should not be poisoned")
            .operations
            .clone()
    }

    fn bm25_results(&self) -> Vec<MemorySearchResult> {
        self.state
            .lock()
            .expect("recording database lock should not be poisoned")
            .bm25_results
            .clone()
    }

    fn vector_results(&self) -> Vec<MemorySearchResult> {
        self.state
            .lock()
            .expect("recording database lock should not be poisoned")
            .vector_results
            .clone()
    }
}

#[async_trait]
impl MemoryDatabase for RecordingDatabase {
    async fn insert_memory(&self, _: &ValidatedMemoryVersion) -> Result<(), DatabaseError> {
        self.record(DatabaseOperation::InsertMemory);
        Ok(())
    }

    async fn insert_embedding(&self, _: &ValidatedEmbedding) -> Result<(), DatabaseError> {
        self.record(DatabaseOperation::InsertEmbedding);
        Ok(())
    }

    async fn search_bm25(
        &self,
        _: &ValidatedBm25Search,
    ) -> Result<Vec<MemorySearchResult>, DatabaseError> {
        self.record(DatabaseOperation::SearchBm25);
        Ok(self.bm25_results())
    }

    async fn search_vector(
        &self,
        _: &ValidatedVectorSearch,
    ) -> Result<Vec<MemorySearchResult>, DatabaseError> {
        self.record(DatabaseOperation::SearchVector);
        Ok(self.vector_results())
    }
}

#[derive(Clone, Default)]
struct FailingDatabase {
    operations: Arc<Mutex<Vec<DatabaseOperation>>>,
}

impl FailingDatabase {
    fn record(&self, operation: DatabaseOperation) {
        self.operations
            .lock()
            .expect("failing database lock should not be poisoned")
            .push(operation);
    }

    fn operations(&self) -> Vec<DatabaseOperation> {
        self.operations
            .lock()
            .expect("failing database lock should not be poisoned")
            .clone()
    }
}

#[async_trait]
impl MemoryDatabase for FailingDatabase {
    async fn insert_memory(&self, _: &ValidatedMemoryVersion) -> Result<(), DatabaseError> {
        self.record(DatabaseOperation::InsertMemory);
        Err(DatabaseError::database_failure(
            DatabaseOperation::InsertMemory,
        ))
    }

    async fn insert_embedding(&self, _: &ValidatedEmbedding) -> Result<(), DatabaseError> {
        self.record(DatabaseOperation::InsertEmbedding);
        Err(DatabaseError::database_failure(
            DatabaseOperation::InsertEmbedding,
        ))
    }

    async fn search_bm25(
        &self,
        _: &ValidatedBm25Search,
    ) -> Result<Vec<MemorySearchResult>, DatabaseError> {
        self.record(DatabaseOperation::SearchBm25);
        Err(DatabaseError::database_failure(
            DatabaseOperation::SearchBm25,
        ))
    }

    async fn search_vector(
        &self,
        _: &ValidatedVectorSearch,
    ) -> Result<Vec<MemorySearchResult>, DatabaseError> {
        self.record(DatabaseOperation::SearchVector);
        Err(DatabaseError::database_failure(
            DatabaseOperation::SearchVector,
        ))
    }
}

fn timestamp(day: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, day, 12, 34, 56)
        .single()
        .expect("test timestamp should be valid")
}

fn timestamp_with_nanoseconds(day: u32, nanoseconds: u32) -> DateTime<Utc> {
    timestamp(day)
        .with_nanosecond(nanoseconds)
        .expect("test timestamp nanoseconds should be valid")
}

fn timestamp_in_year(year: i32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0)
        .single()
        .expect("test timestamp year must be valid in Chrono")
}

fn valid_memory() -> MemoryVersionInput {
    MemoryVersionInput {
        id: "memory-id".to_owned(),
        version: 7,
        memory_type: "memory-type".to_owned(),
        title: "memory-title".to_owned(),
        content: "memory-content".to_owned(),
        created_at: timestamp(18),
        updated_at: timestamp(19),
        concepts: vec!["memory-concept".to_owned()],
        files: vec!["memory-file".to_owned()],
        session_ids: vec!["memory-session".to_owned()],
        source_observation_ids: vec!["memory-observation".to_owned()],
    }
}

fn valid_embedding() -> EmbeddingInput {
    EmbeddingInput {
        id: "memory-id".to_owned(),
        version: 7,
        embedding: vec![1.25, -2.5],
    }
}

fn valid_bm25_search() -> Bm25Search {
    Bm25Search {
        query: "memory query".to_owned(),
        limit: 2,
    }
}

fn valid_vector_search() -> VectorSearch {
    VectorSearch {
        vector: vec![1.25, -2.5],
        limit: 2,
    }
}

fn search_result(
    id: &str,
    version: i64,
    title: &str,
    content: &str,
    relevance: f64,
) -> MemorySearchResult {
    let memory = ValidatedMemoryVersion::try_from(MemoryVersionInput {
        id: id.to_owned(),
        version,
        memory_type: "result-type".to_owned(),
        title: title.to_owned(),
        content: content.to_owned(),
        created_at: timestamp(18),
        updated_at: timestamp(19),
        concepts: vec!["result-concept".to_owned()],
        files: vec!["result-file".to_owned()],
        session_ids: vec!["result-session".to_owned()],
        source_observation_ids: vec!["result-observation".to_owned()],
    })
    .expect("test result memory should be valid");

    MemorySearchResult::try_new(memory, relevance).expect("test relevance should be valid")
}

fn assert_invalid_input(error: MemoryStoreError, field: &str, code: &str) {
    match error {
        MemoryStoreError::InvalidInput(error) => {
            assert_eq!(error.field(), field);
            assert_eq!(error.code(), code);
        }
        other => panic!("expected invalid input error, received {other:?}"),
    }
}

fn assert_database_failure(error: MemoryStoreError, operation: DatabaseOperation) {
    assert_eq!(
        error,
        MemoryStoreError::Database(DatabaseError::database_failure(operation))
    );
}

fn assert_error_is_opaque(error: &MemoryStoreError, sentinels: &[&str]) {
    let display = error.to_string();
    let debug = format!("{error:?}");

    for sentinel in sentinels {
        assert!(
            !display.contains(sentinel),
            "Display error leaked protected value: {display}"
        );
        assert!(
            !debug.contains(sentinel),
            "Debug error leaked protected value: {debug}"
        );
    }
}

#[tokio::test]
async fn valid_memory_request_calls_only_the_memory_port_once() {
    let database = RecordingDatabase::default();
    let store = MemoryStore::new(database.clone());

    let result = store.insert_memory(valid_memory()).await;

    assert_eq!(
        database.operations(),
        [DatabaseOperation::InsertMemory],
        "valid memory should return success; result: {result:?}"
    );
    result.expect("valid memory should be inserted");
}

#[tokio::test]
async fn valid_embedding_request_calls_only_the_embedding_port_once() {
    let database = RecordingDatabase::default();
    let store = MemoryStore::new(database.clone());

    let result = store.insert_embedding(valid_embedding()).await;

    assert_eq!(
        database.operations(),
        [DatabaseOperation::InsertEmbedding],
        "valid embedding should return success; result: {result:?}"
    );
    result.expect("valid embedding should be inserted");
}

#[tokio::test]
async fn valid_bm25_request_returns_the_complete_port_results_unchanged() {
    let expected = vec![
        search_result(
            "bm25-first",
            2,
            "bm25 first title",
            "bm25 first content",
            2.5,
        ),
        search_result(
            "bm25-second",
            1,
            "bm25 second title",
            "bm25 second content",
            1.25,
        ),
    ];
    let database = RecordingDatabase::with_results(expected.clone(), Vec::new());
    let store = MemoryStore::new(database.clone());

    let result = store.search_bm25(valid_bm25_search()).await;

    assert_eq!(
        database.operations(),
        [DatabaseOperation::SearchBm25],
        "valid BM25 query should return success; result: {result:?}"
    );
    assert_eq!(
        result.expect("valid BM25 query should return port results"),
        expected
    );
}

#[tokio::test]
async fn valid_vector_request_returns_the_complete_port_results_unchanged() {
    let expected = vec![
        search_result(
            "vector-first",
            2,
            "vector first title",
            "vector first content",
            0.95,
        ),
        search_result(
            "vector-second",
            1,
            "vector second title",
            "vector second content",
            0.75,
        ),
    ];
    let database = RecordingDatabase::with_results(Vec::new(), expected.clone());
    let store = MemoryStore::new(database.clone());

    let result = store.search_vector(valid_vector_search()).await;

    assert_eq!(
        database.operations(),
        [DatabaseOperation::SearchVector],
        "valid vector query should return success; result: {result:?}"
    );
    assert_eq!(
        result.expect("valid vector query should return port results"),
        expected
    );
}

#[tokio::test]
async fn invalid_memory_requests_do_not_call_the_port() {
    let database = RecordingDatabase::default();
    let store = MemoryStore::new(database.clone());
    let mut empty_id = valid_memory();
    empty_id.id.clear();
    let mut empty_type = valid_memory();
    empty_type.memory_type.clear();
    let mut empty_title = valid_memory();
    empty_title.title.clear();
    let mut empty_content = valid_memory();
    empty_content.content.clear();
    let mut zero_version = valid_memory();
    zero_version.version = 0;
    let mut negative_version = valid_memory();
    negative_version.version = -1;
    let mut inverted_timestamps = valid_memory();
    inverted_timestamps.updated_at = timestamp(17);

    for (input, field, code) in [
        (empty_id, "id", "empty"),
        (empty_type, "memory_type", "empty"),
        (empty_title, "title", "empty"),
        (empty_content, "content", "empty"),
        (zero_version, "version", "not_positive"),
        (negative_version, "version", "not_positive"),
        (inverted_timestamps, "updated_at", "before_created_at"),
    ] {
        let error = store
            .insert_memory(input)
            .await
            .expect_err("invalid memory should be rejected");

        assert_invalid_input(error, field, code);
        assert!(database.operations().is_empty());
    }
}

#[tokio::test]
async fn nonrepresentable_memory_requests_do_not_call_the_port_or_leak_values() {
    const NUL_SENTINEL: &str = "memory-nul-secret\0sentinel";

    let database = RecordingDatabase::default();
    let store = MemoryStore::new(database.clone());
    let mut nul_id = valid_memory();
    nul_id.id = NUL_SENTINEL.to_owned();
    let mut nul_type = valid_memory();
    nul_type.memory_type = NUL_SENTINEL.to_owned();
    let mut nul_title = valid_memory();
    nul_title.title = NUL_SENTINEL.to_owned();
    let mut nul_content = valid_memory();
    nul_content.content = NUL_SENTINEL.to_owned();
    let mut nul_concepts = valid_memory();
    nul_concepts.concepts = vec![NUL_SENTINEL.to_owned()];
    let mut nul_files = valid_memory();
    nul_files.files = vec![NUL_SENTINEL.to_owned()];
    let mut nul_session_ids = valid_memory();
    nul_session_ids.session_ids = vec![NUL_SENTINEL.to_owned()];
    let mut nul_source_observation_ids = valid_memory();
    nul_source_observation_ids.source_observation_ids = vec![NUL_SENTINEL.to_owned()];

    for (input, field) in [
        (nul_id, "id"),
        (nul_type, "memory_type"),
        (nul_title, "title"),
        (nul_content, "content"),
        (nul_concepts, "concepts"),
        (nul_files, "files"),
        (nul_session_ids, "session_ids"),
        (nul_source_observation_ids, "source_observation_ids"),
    ] {
        let error = store
            .insert_memory(input)
            .await
            .expect_err("NUL-bearing memory input should be rejected");

        assert_invalid_input(error.clone(), field, "contains_nul");
        assert_error_is_opaque(&error, &[NUL_SENTINEL]);
        assert!(database.operations().is_empty());
    }

    let mut submicrosecond_created_at = valid_memory();
    submicrosecond_created_at.created_at = timestamp_with_nanoseconds(18, 123_456_001);
    let created_at_sentinel = submicrosecond_created_at.created_at.to_rfc3339();
    let created_at_error = store
        .insert_memory(submicrosecond_created_at)
        .await
        .expect_err("submicrosecond created_at should be rejected");
    assert_invalid_input(
        created_at_error.clone(),
        "created_at",
        "not_microsecond_aligned",
    );
    assert_error_is_opaque(&created_at_error, &[&created_at_sentinel]);
    assert!(database.operations().is_empty());

    let mut submicrosecond_updated_at = valid_memory();
    submicrosecond_updated_at.updated_at = timestamp_with_nanoseconds(19, 654_321_001);
    let updated_at_sentinel = submicrosecond_updated_at.updated_at.to_rfc3339();
    let updated_at_error = store
        .insert_memory(submicrosecond_updated_at)
        .await
        .expect_err("submicrosecond updated_at should be rejected");
    assert_invalid_input(
        updated_at_error.clone(),
        "updated_at",
        "not_microsecond_aligned",
    );
    assert_error_is_opaque(&updated_at_error, &[&updated_at_sentinel]);
    assert!(database.operations().is_empty());

    for year in [0, -1, 10_000] {
        let unsupported = timestamp_in_year(year);
        let mut input = valid_memory();
        input.created_at = unsupported;
        input.updated_at = unsupported;
        let sentinel = unsupported.to_rfc3339();
        let error = store
            .insert_memory(input)
            .await
            .expect_err("unsupported timestamp years should be rejected");
        assert_invalid_input(error.clone(), "created_at", "unsupported_year");
        assert_error_is_opaque(&error, &[&sentinel]);
        assert!(database.operations().is_empty());
    }

    let mut exact_microseconds = valid_memory();
    exact_microseconds.created_at = timestamp_with_nanoseconds(18, 123_456_000);
    exact_microseconds.updated_at = timestamp_with_nanoseconds(19, 654_321_000);
    store
        .insert_memory(exact_microseconds)
        .await
        .expect("exact microsecond timestamps should reach the memory database");
    assert_eq!(database.operations(), vec![DatabaseOperation::InsertMemory]);
}

#[tokio::test]
async fn invalid_embedding_requests_do_not_call_the_port() {
    let database = RecordingDatabase::default();
    let store = MemoryStore::new(database.clone());
    let mut empty_id = valid_embedding();
    empty_id.id.clear();
    let mut zero_version = valid_embedding();
    zero_version.version = 0;
    let mut negative_version = valid_embedding();
    negative_version.version = -1;

    for (input, field, code) in [
        (empty_id, "id", "empty"),
        (zero_version, "version", "not_positive"),
        (negative_version, "version", "not_positive"),
        (
            EmbeddingInput {
                embedding: Vec::new(),
                ..valid_embedding()
            },
            "embedding",
            "empty",
        ),
        (
            EmbeddingInput {
                embedding: vec![0.0, -0.0],
                ..valid_embedding()
            },
            "embedding",
            "zero_norm",
        ),
        (
            EmbeddingInput {
                embedding: vec![1.0, f64::NAN],
                ..valid_embedding()
            },
            "embedding",
            "non_finite",
        ),
        (
            EmbeddingInput {
                embedding: vec![f64::INFINITY],
                ..valid_embedding()
            },
            "embedding",
            "non_finite",
        ),
        (
            EmbeddingInput {
                embedding: vec![f64::NEG_INFINITY],
                ..valid_embedding()
            },
            "embedding",
            "non_finite",
        ),
    ] {
        let error = store
            .insert_embedding(input)
            .await
            .expect_err("invalid embedding should be rejected");

        assert_invalid_input(error, field, code);
        assert!(database.operations().is_empty());
    }
}

#[tokio::test]
async fn nonrepresentable_embedding_requests_do_not_call_the_port_or_leak_values() {
    const NUL_SENTINEL: &str = "embedding-id-nul-secret\0sentinel";
    const VECTOR_SENTINEL: &str = "987654.25";

    let database = RecordingDatabase::default();
    let store = MemoryStore::new(database.clone());
    let mut nul_id = valid_embedding();
    nul_id.id = NUL_SENTINEL.to_owned();

    for (input, field, code) in [
        (nul_id, "id", "contains_nul"),
        (
            EmbeddingInput {
                embedding: vec![987_654.25, f64::MAX],
                ..valid_embedding()
            },
            "embedding",
            "not_float4_exact",
        ),
        (
            EmbeddingInput {
                embedding: vec![987_654.25, f64::MIN_POSITIVE],
                ..valid_embedding()
            },
            "embedding",
            "not_float4_exact",
        ),
        (
            EmbeddingInput {
                embedding: vec![987_654.25, 1.000_000_000_000_000_2],
                ..valid_embedding()
            },
            "embedding",
            "not_float4_exact",
        ),
        (
            EmbeddingInput {
                embedding: vec![987_654.25; 16_001],
                ..valid_embedding()
            },
            "embedding",
            "too_many_components",
        ),
    ] {
        let error = store
            .insert_embedding(input)
            .await
            .expect_err("nonrepresentable embedding input should be rejected");

        assert_invalid_input(error.clone(), field, code);
        assert_error_is_opaque(&error, &[NUL_SENTINEL, VECTOR_SENTINEL]);
        assert!(database.operations().is_empty());
    }
}

#[tokio::test]
async fn invalid_bm25_requests_do_not_call_the_port() {
    let database = RecordingDatabase::default();
    let store = MemoryStore::new(database.clone());

    for (input, field, code) in [
        (
            Bm25Search {
                query: String::new(),
                ..valid_bm25_search()
            },
            "query",
            "empty",
        ),
        (
            Bm25Search {
                limit: 0,
                ..valid_bm25_search()
            },
            "limit",
            "not_positive",
        ),
    ] {
        let error = store
            .search_bm25(input)
            .await
            .expect_err("invalid BM25 request should be rejected");

        assert_invalid_input(error, field, code);
        assert!(database.operations().is_empty());
    }
}

#[tokio::test]
async fn nul_bm25_requests_do_not_call_the_port_or_leak_values() {
    const QUERY_SENTINEL: &str = "bm25-nul-secret\0sentinel";

    let database = RecordingDatabase::default();
    let store = MemoryStore::new(database.clone());
    let error = store
        .search_bm25(Bm25Search {
            query: QUERY_SENTINEL.to_owned(),
            ..valid_bm25_search()
        })
        .await
        .expect_err("NUL-bearing lexical queries should be rejected");

    assert_invalid_input(error.clone(), "query", "contains_nul");
    assert_error_is_opaque(&error, &[QUERY_SENTINEL]);
    assert!(database.operations().is_empty());
}

#[tokio::test]
async fn invalid_vector_requests_do_not_call_the_port() {
    let database = RecordingDatabase::default();
    let store = MemoryStore::new(database.clone());

    for (input, field, code) in [
        (
            VectorSearch {
                vector: Vec::new(),
                ..valid_vector_search()
            },
            "vector",
            "empty",
        ),
        (
            VectorSearch {
                vector: vec![0.0, -0.0],
                ..valid_vector_search()
            },
            "vector",
            "zero_norm",
        ),
        (
            VectorSearch {
                vector: vec![1.0, f64::NAN],
                ..valid_vector_search()
            },
            "vector",
            "non_finite",
        ),
        (
            VectorSearch {
                vector: vec![f64::INFINITY],
                ..valid_vector_search()
            },
            "vector",
            "non_finite",
        ),
        (
            VectorSearch {
                vector: vec![f64::NEG_INFINITY],
                ..valid_vector_search()
            },
            "vector",
            "non_finite",
        ),
        (
            VectorSearch {
                limit: 0,
                ..valid_vector_search()
            },
            "limit",
            "not_positive",
        ),
    ] {
        let error = store
            .search_vector(input)
            .await
            .expect_err("invalid vector request should be rejected");

        assert_invalid_input(error, field, code);
        assert!(database.operations().is_empty());
    }
}

#[tokio::test]
async fn nonrepresentable_vector_requests_do_not_call_the_port_or_leak_values() {
    const VECTOR_SENTINEL: &str = "987654.25";

    let database = RecordingDatabase::default();
    let store = MemoryStore::new(database.clone());

    for (input, code) in [
        (
            VectorSearch {
                vector: vec![987_654.25, f64::MAX],
                ..valid_vector_search()
            },
            "not_float4_exact",
        ),
        (
            VectorSearch {
                vector: vec![987_654.25, f64::MIN_POSITIVE],
                ..valid_vector_search()
            },
            "not_float4_exact",
        ),
        (
            VectorSearch {
                vector: vec![987_654.25, 1.000_000_000_000_000_2],
                ..valid_vector_search()
            },
            "not_float4_exact",
        ),
        (
            VectorSearch {
                vector: vec![987_654.25; 16_001],
                ..valid_vector_search()
            },
            "too_many_components",
        ),
    ] {
        let error = store
            .search_vector(input)
            .await
            .expect_err("nonrepresentable vector query should be rejected");

        assert_invalid_input(error.clone(), "vector", code);
        assert_error_is_opaque(&error, &[VECTOR_SENTINEL]);
        assert!(database.operations().is_empty());
    }
}

#[tokio::test]
async fn failed_port_calls_are_database_errors_without_protected_values() {
    const TITLE_SENTINEL: &str = "title-secret-sentinel";
    const CONTENT_SENTINEL: &str = "content-secret-sentinel";
    const COLLECTION_SENTINEL: &str = "collection-secret-sentinel";
    const QUERY_SENTINEL: &str = "query-secret-sentinel";
    const VECTOR_SENTINEL: &str = "987654.25";

    let database = FailingDatabase::default();
    let store = MemoryStore::new(database.clone());
    let mut memory = valid_memory();
    memory.title = TITLE_SENTINEL.to_owned();
    memory.content = CONTENT_SENTINEL.to_owned();
    memory.concepts = vec![COLLECTION_SENTINEL.to_owned()];

    let memory_error = store
        .insert_memory(memory)
        .await
        .expect_err("failing memory port should return an error");
    assert_database_failure(memory_error.clone(), DatabaseOperation::InsertMemory);
    assert_error_is_opaque(
        &memory_error,
        &[
            TITLE_SENTINEL,
            CONTENT_SENTINEL,
            COLLECTION_SENTINEL,
            QUERY_SENTINEL,
            VECTOR_SENTINEL,
        ],
    );

    let embedding_error = store
        .insert_embedding(EmbeddingInput {
            embedding: vec![987_654.25],
            ..valid_embedding()
        })
        .await
        .expect_err("failing embedding port should return an error");
    assert_database_failure(embedding_error.clone(), DatabaseOperation::InsertEmbedding);
    assert_error_is_opaque(
        &embedding_error,
        &[
            TITLE_SENTINEL,
            CONTENT_SENTINEL,
            COLLECTION_SENTINEL,
            QUERY_SENTINEL,
            VECTOR_SENTINEL,
        ],
    );

    let bm25_error = store
        .search_bm25(Bm25Search {
            query: QUERY_SENTINEL.to_owned(),
            ..valid_bm25_search()
        })
        .await
        .expect_err("failing BM25 port should return an error");
    assert_database_failure(bm25_error.clone(), DatabaseOperation::SearchBm25);
    assert_error_is_opaque(
        &bm25_error,
        &[
            TITLE_SENTINEL,
            CONTENT_SENTINEL,
            COLLECTION_SENTINEL,
            QUERY_SENTINEL,
            VECTOR_SENTINEL,
        ],
    );

    let vector_error = store
        .search_vector(VectorSearch {
            vector: vec![987_654.25],
            ..valid_vector_search()
        })
        .await
        .expect_err("failing vector port should return an error");
    assert_database_failure(vector_error.clone(), DatabaseOperation::SearchVector);
    assert_error_is_opaque(
        &vector_error,
        &[
            TITLE_SENTINEL,
            CONTENT_SENTINEL,
            COLLECTION_SENTINEL,
            QUERY_SENTINEL,
            VECTOR_SENTINEL,
        ],
    );
    assert_eq!(
        database.operations(),
        [
            DatabaseOperation::InsertMemory,
            DatabaseOperation::InsertEmbedding,
            DatabaseOperation::SearchBm25,
            DatabaseOperation::SearchVector,
        ]
    );
}

#[test]
fn iii_memory_database_implements_the_memory_database_port_without_a_worker() {
    fn assert_memory_database<T: MemoryDatabase>() {}

    assert_memory_database::<IiiMemoryDatabase>();
}
