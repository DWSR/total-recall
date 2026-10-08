use std::{
    future::pending,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use memory_embedding::{
    IiiEmbeddingWriter,
    contracts::{
        Deadline, EmbeddingError, EmbeddingWriter, MemoryKey, WriteOutcome, WriterFailure,
    },
};
use memory_store::{
    MemoryStore,
    contracts::{
        DatabaseError, DatabaseOperation, MemoryId, MemorySearchResult, MemoryVersion,
        MemoryVersionKey, ValidatedBm25Search, ValidatedEmbedding, ValidatedMemoryVersion,
        ValidatedVectorSearch,
    },
    database::MemoryDatabase,
};

const KEY_SENTINEL: &str = "writer-key-sentinel";
const SECOND_KEY_SENTINEL: &str = "writer-other-key-sentinel";
const RESPONSE_SENTINEL: &str = "writer-response-sentinel";
const OUTER_TIMEOUT: Duration = Duration::from_millis(500);
const EARLIER_DEADLINE: Duration = Duration::from_millis(50);
const DATABASE_TIMEOUT: Duration = Duration::from_secs(1);
const INVOCATION_TIMEOUT: Duration = Duration::from_secs(2);
const EARLIER_DATABASE_TIMEOUT: Duration = Duration::from_millis(50);

#[derive(Clone)]
struct FakeMemoryDatabase {
    state: Arc<Mutex<FakeMemoryDatabaseState>>,
}

struct FakeMemoryDatabaseState {
    response: FakeMemoryDatabaseResponse,
    insertions: Vec<CapturedEmbedding>,
}

#[derive(Clone)]
enum FakeMemoryDatabaseResponse {
    Inserted,
    Error(DatabaseError),
    Pending,
}

#[derive(Clone, Debug, PartialEq)]
struct CapturedEmbedding {
    id: String,
    version: i64,
    vector: Vec<f64>,
}

impl FakeMemoryDatabase {
    fn new(response: FakeMemoryDatabaseResponse) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeMemoryDatabaseState {
                response,
                insertions: Vec::new(),
            })),
        }
    }

    fn insertions(&self) -> Vec<CapturedEmbedding> {
        self.state
            .lock()
            .expect("test memory database state lock should not be poisoned")
            .insertions
            .clone()
    }
}

#[async_trait]
impl MemoryDatabase for FakeMemoryDatabase {
    async fn insert_memory(&self, _: &ValidatedMemoryVersion) -> Result<(), DatabaseError> {
        panic!("the embedding writer must not insert a memory")
    }

    async fn insert_embedding(&self, embedding: &ValidatedEmbedding) -> Result<(), DatabaseError> {
        let response = {
            let mut state = self
                .state
                .lock()
                .expect("test memory database state lock should not be poisoned");
            state.insertions.push(CapturedEmbedding {
                id: embedding.id().as_str().to_owned(),
                version: embedding.version().get(),
                vector: embedding.embedding().as_slice().to_vec(),
            });
            state.response.clone()
        };

        match response {
            FakeMemoryDatabaseResponse::Inserted => Ok(()),
            FakeMemoryDatabaseResponse::Error(error) => Err(error),
            FakeMemoryDatabaseResponse::Pending => pending().await,
        }
    }

    async fn search_bm25(
        &self,
        _: &ValidatedBm25Search,
    ) -> Result<Vec<MemorySearchResult>, DatabaseError> {
        panic!("the embedding writer must not search memory")
    }

    async fn search_vector(
        &self,
        _: &ValidatedVectorSearch,
    ) -> Result<Vec<MemorySearchResult>, DatabaseError> {
        panic!("the embedding writer must not search embeddings")
    }
}

fn writer(
    database: FakeMemoryDatabase,
    database_timeout: Duration,
) -> IiiEmbeddingWriter<FakeMemoryDatabase> {
    IiiEmbeddingWriter::with_store(MemoryStore::new(database), database_timeout)
}

fn key() -> MemoryKey {
    MemoryKey::try_new(KEY_SENTINEL, "7").expect("test key should be valid")
}

fn vector() -> Vec<f64> {
    [16_777_217.0_f32, -2.5_f32, 3.25_f32]
        .into_iter()
        .map(f64::from)
        .collect()
}

fn assert_opaque(error: &EmbeddingError) {
    let display = error.to_string();
    let debug = format!("{error:?}");

    for sentinel in [KEY_SENTINEL, SECOND_KEY_SENTINEL, RESPONSE_SENTINEL] {
        assert!(
            !display.contains(sentinel),
            "Display error leaked protected sentinel {sentinel}: {display}"
        );
        assert!(
            !debug.contains(sentinel),
            "Debug error leaked protected sentinel {sentinel}: {debug}"
        );
    }
}

#[tokio::test]
async fn insert_delegates_one_exact_immutable_association_and_preserves_float32_widening() {
    let database = FakeMemoryDatabase::new(FakeMemoryDatabaseResponse::Inserted);
    let adapter = writer(database.clone(), DATABASE_TIMEOUT);
    let expected_vector = vector();

    let outcome = EmbeddingWriter::insert(
        &adapter,
        key(),
        expected_vector.clone(),
        Deadline::after(Duration::from_secs(1)),
    )
    .await
    .expect("a valid canonical embedding association should be stored");

    assert_eq!(outcome, WriteOutcome::Stored);
    assert_eq!(
        database.insertions(),
        vec![CapturedEmbedding {
            id: KEY_SENTINEL.to_owned(),
            version: 7,
            vector: expected_vector,
        }],
        "the writer must make one exact immutable association"
    );
}

#[tokio::test]
async fn insert_maps_conflicts_to_already_present_without_retry() {
    let database = FakeMemoryDatabase::new(FakeMemoryDatabaseResponse::Error(
        DatabaseError::conflict(DatabaseOperation::InsertEmbedding),
    ));
    let adapter = writer(database.clone(), DATABASE_TIMEOUT);

    let outcome = EmbeddingWriter::insert(
        &adapter,
        key(),
        vector(),
        Deadline::after(Duration::from_secs(1)),
    )
    .await
    .expect("an immutable conflict should be reported as already present");

    assert_eq!(outcome, WriteOutcome::AlreadyPresent);
    assert_eq!(database.insertions().len(), 1, "the writer must not retry");
}

#[tokio::test]
async fn insert_maps_missing_parent_malformed_and_backend_failures_without_retry_or_leaks() {
    let missing_parent = DatabaseError::missing_memory_version(MemoryVersionKey::new(
        MemoryId::try_from(SECOND_KEY_SENTINEL.to_owned()).expect("test memory ID should be valid"),
        MemoryVersion::try_from(9).expect("test memory version should be valid"),
    ));

    for (store_error, expected) in [
        (missing_parent, WriterFailure::MissingParent),
        (
            DatabaseError::invalid_response(DatabaseOperation::InsertEmbedding, RESPONSE_SENTINEL),
            WriterFailure::MalformedResponse,
        ),
        (
            DatabaseError::database_failure(DatabaseOperation::InsertEmbedding),
            WriterFailure::Backend,
        ),
    ] {
        let database = FakeMemoryDatabase::new(FakeMemoryDatabaseResponse::Error(store_error));
        let adapter = writer(database.clone(), DATABASE_TIMEOUT);

        let error = EmbeddingWriter::insert(
            &adapter,
            key(),
            vector(),
            Deadline::after(Duration::from_secs(1)),
        )
        .await
        .expect_err("store failures must remain opaque writer failures");

        assert_eq!(error, EmbeddingError::writer(expected));
        assert_eq!(database.insertions().len(), 1, "the writer must not retry");
        assert_opaque(&error);
    }
}

#[tokio::test]
async fn insert_uses_the_invocation_deadline_when_it_precedes_the_configured_database_timeout() {
    let database = FakeMemoryDatabase::new(FakeMemoryDatabaseResponse::Pending);
    let adapter = writer(database.clone(), DATABASE_TIMEOUT);

    let inner = tokio::time::timeout(
        OUTER_TIMEOUT,
        EmbeddingWriter::insert(&adapter, key(), vector(), Deadline::after(EARLIER_DEADLINE)),
    )
    .await
    .expect("the absolute deadline must complete before the outer timeout");

    let error = inner.expect_err("the deadline must stop waiting on the store");
    assert_eq!(error, EmbeddingError::writer(WriterFailure::Timeout));
    assert_eq!(database.insertions().len(), 1, "the writer must not retry");
    assert_opaque(&error);
}

#[tokio::test]
async fn insert_rejects_an_expired_deadline_without_admitting_a_store_call() {
    let database = FakeMemoryDatabase::new(FakeMemoryDatabaseResponse::Inserted);
    let adapter = writer(database.clone(), DATABASE_TIMEOUT);

    let error = EmbeddingWriter::insert(&adapter, key(), vector(), Deadline::at(Instant::now()))
        .await
        .expect_err("an expired deadline must not admit a store call");

    assert_eq!(error, EmbeddingError::writer(WriterFailure::Timeout));
    assert!(database.insertions().is_empty());
    assert_opaque(&error);
}

#[tokio::test]
async fn insert_uses_the_configured_database_timeout_before_the_invocation_deadline() {
    let database = FakeMemoryDatabase::new(FakeMemoryDatabaseResponse::Pending);
    let adapter = writer(database.clone(), EARLIER_DATABASE_TIMEOUT);
    let error = tokio::time::timeout(
        OUTER_TIMEOUT,
        EmbeddingWriter::insert(
            &adapter,
            key(),
            vector(),
            Deadline::after(INVOCATION_TIMEOUT),
        ),
    )
    .await
    .expect("the configured database timeout must precede the invocation deadline")
    .expect_err("the local database timeout should stop the writer");

    assert_eq!(error, EmbeddingError::writer(WriterFailure::Timeout));
    assert_eq!(database.insertions().len(), 1, "the writer must not retry");
    assert_opaque(&error);
}
