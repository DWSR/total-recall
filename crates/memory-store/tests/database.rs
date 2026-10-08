use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use iii_sdk::IIIClient;
use memory_store::{
    contracts::{
        Bm25Search, DatabaseError, DatabaseOperation, DatabaseTarget, EmbeddingInput,
        MemoryVersionInput, ValidatedBm25Search, ValidatedEmbedding, ValidatedMemoryVersion,
        ValidatedVectorSearch, VectorSearch,
    },
    database::{IiiMemoryDatabase, MemoryDatabase},
};

#[derive(Default)]
struct RecordingDatabase {
    operations: Mutex<Vec<DatabaseOperation>>,
}

impl RecordingDatabase {
    fn record(&self, operation: DatabaseOperation) {
        self.operations
            .lock()
            .expect("recording database lock should not be poisoned")
            .push(operation);
    }

    fn operations(&self) -> Vec<DatabaseOperation> {
        self.operations
            .lock()
            .expect("recording database lock should not be poisoned")
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
    ) -> Result<Vec<memory_store::contracts::MemorySearchResult>, DatabaseError> {
        self.record(DatabaseOperation::SearchBm25);
        Ok(Vec::new())
    }

    async fn search_vector(
        &self,
        _: &ValidatedVectorSearch,
    ) -> Result<Vec<memory_store::contracts::MemorySearchResult>, DatabaseError> {
        self.record(DatabaseOperation::SearchVector);
        Ok(Vec::new())
    }
}

struct FailingDatabase;

#[async_trait]
impl MemoryDatabase for FailingDatabase {
    async fn insert_memory(&self, _: &ValidatedMemoryVersion) -> Result<(), DatabaseError> {
        Err(DatabaseError::database_failure(
            DatabaseOperation::InsertMemory,
        ))
    }

    async fn insert_embedding(&self, _: &ValidatedEmbedding) -> Result<(), DatabaseError> {
        Err(DatabaseError::database_failure(
            DatabaseOperation::InsertEmbedding,
        ))
    }

    async fn search_bm25(
        &self,
        _: &ValidatedBm25Search,
    ) -> Result<Vec<memory_store::contracts::MemorySearchResult>, DatabaseError> {
        Err(DatabaseError::database_failure(
            DatabaseOperation::SearchBm25,
        ))
    }

    async fn search_vector(
        &self,
        _: &ValidatedVectorSearch,
    ) -> Result<Vec<memory_store::contracts::MemorySearchResult>, DatabaseError> {
        Err(DatabaseError::database_failure(
            DatabaseOperation::SearchVector,
        ))
    }
}

fn timestamp() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 19, 12, 34, 56)
        .single()
        .expect("test timestamp should be valid")
}

fn memory(title: &str, content: &str) -> ValidatedMemoryVersion {
    ValidatedMemoryVersion::try_from(MemoryVersionInput {
        id: "memory-id-sentinel".to_owned(),
        version: 1,
        memory_type: "memory-type-sentinel".to_owned(),
        title: title.to_owned(),
        content: content.to_owned(),
        created_at: timestamp(),
        updated_at: timestamp(),
        concepts: vec!["concept-sentinel".to_owned()],
        files: vec!["file-sentinel".to_owned()],
        session_ids: vec!["session-sentinel".to_owned()],
        source_observation_ids: vec!["source-sentinel".to_owned()],
    })
    .expect("test memory should be valid")
}

fn embedding(component: f64) -> ValidatedEmbedding {
    ValidatedEmbedding::try_from(EmbeddingInput {
        id: "memory-id-sentinel".to_owned(),
        version: 1,
        embedding: vec![component],
    })
    .expect("test embedding should be valid")
}

fn bm25_search(query: &str) -> ValidatedBm25Search {
    ValidatedBm25Search::try_from(Bm25Search {
        query: query.to_owned(),
        limit: 1,
    })
    .expect("test BM25 search should be valid")
}

fn vector_search(component: f64) -> ValidatedVectorSearch {
    ValidatedVectorSearch::try_from(VectorSearch {
        vector: vec![component],
        limit: 1,
    })
    .expect("test vector search should be valid")
}

async fn use_database_port<D: MemoryDatabase>(database: &D) -> Result<(), DatabaseError> {
    database
        .insert_memory(&memory("title-sentinel", "content-sentinel"))
        .await?;
    database.insert_embedding(&embedding(42.5)).await?;
    database.search_bm25(&bm25_search("query-sentinel")).await?;
    database.search_vector(&vector_search(42.5)).await?;
    Ok(())
}

fn assert_error_is_opaque(error: &DatabaseError, sentinels: &[&str]) {
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
async fn recording_database_substitutes_all_four_port_operations() {
    let database = RecordingDatabase::default();

    use_database_port(&database)
        .await
        .expect("recording database should fulfill every port operation");

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

#[tokio::test]
async fn failing_database_returns_typed_opaque_errors_for_all_port_operations() {
    const TITLE_SENTINEL: &str = "title-secret-sentinel";
    const CONTENT_SENTINEL: &str = "content-secret-sentinel";
    const QUERY_SENTINEL: &str = "query-secret-sentinel";
    const VECTOR_SENTINEL: &str = "987654.25";
    const COLLECTION_SENTINEL: &str = "concept-sentinel";

    let database = FailingDatabase;
    let errors = [
        database
            .insert_memory(&memory(TITLE_SENTINEL, CONTENT_SENTINEL))
            .await
            .expect_err("failing database should reject memory insertion"),
        database
            .insert_embedding(&embedding(987_654.25))
            .await
            .expect_err("failing database should reject embedding insertion"),
        database
            .search_bm25(&bm25_search(QUERY_SENTINEL))
            .await
            .expect_err("failing database should reject BM25 search"),
        database
            .search_vector(&vector_search(987_654.25))
            .await
            .expect_err("failing database should reject vector search"),
    ];

    for (error, operation) in errors.into_iter().zip([
        DatabaseOperation::InsertMemory,
        DatabaseOperation::InsertEmbedding,
        DatabaseOperation::SearchBm25,
        DatabaseOperation::SearchVector,
    ]) {
        assert!(matches!(
            error,
            DatabaseError::DatabaseFailure {
                operation: actual_operation
            } if actual_operation == operation
        ));
        assert_error_is_opaque(
            &error,
            &[
                TITLE_SENTINEL,
                CONTENT_SENTINEL,
                QUERY_SENTINEL,
                VECTOR_SENTINEL,
                COLLECTION_SENTINEL,
            ],
        );
    }
}

#[test]
fn iii_adapter_constructs_without_a_runtime_worker() {
    let client = IIIClient::new("ws://127.0.0.1:0");
    let database = DatabaseTarget::try_from("memory-store-test".to_owned())
        .expect("database target should be valid");

    let _adapter = IiiMemoryDatabase::new(client, database);
}
