use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use memory_store::{
    MemoryContextDatabase, MemoryContextStore,
    contracts::{
        ContextCandidate, ContextMemory, ContextQuery, DatabaseError, DatabaseOperation,
        MemoryStoreError, ValidatedContextQuery,
    },
};

#[derive(Default)]
struct State {
    calls: usize,
    result: Vec<ContextCandidate>,
}

#[derive(Clone, Default)]
struct RecordingDatabase {
    state: Arc<Mutex<State>>,
}

impl RecordingDatabase {
    fn with_result(result: Vec<ContextCandidate>) -> Self {
        let db = Self::default();
        db.state.lock().unwrap().result = result;
        db
    }

    fn calls(&self) -> usize {
        self.state.lock().unwrap().calls
    }
}

#[async_trait]
impl MemoryContextDatabase for RecordingDatabase {
    async fn list_recent_context(
        &self,
        _: &ValidatedContextQuery,
    ) -> Result<Vec<ContextCandidate>, DatabaseError> {
        let mut state = self.state.lock().unwrap();
        state.calls += 1;
        Ok(state.result.clone())
    }
}

#[derive(Clone, Default)]
struct FailingDatabase(Arc<Mutex<usize>>);

impl FailingDatabase {
    fn calls(&self) -> usize {
        *self.0.lock().unwrap()
    }
}

#[async_trait]
impl MemoryContextDatabase for FailingDatabase {
    async fn list_recent_context(
        &self,
        _: &ValidatedContextQuery,
    ) -> Result<Vec<ContextCandidate>, DatabaseError> {
        *self.0.lock().unwrap() += 1;
        Err(DatabaseError::database_failure(
            DatabaseOperation::SearchContext,
        ))
    }
}

fn query(limit: u32, bytes: usize) -> ContextQuery {
    ContextQuery {
        limit,
        max_candidate_bytes: bytes,
    }
}

fn record(title: &str) -> ContextCandidate {
    ContextCandidate::Memory(ContextMemory {
        memory_type: "fact".into(),
        title: title.into(),
        content: "body".into(),
        concepts: vec!["concept".into()],
    })
}

#[tokio::test]
async fn invalid_context_queries_do_not_call_database() {
    let database = RecordingDatabase::default();
    let store = MemoryContextStore::new(database.clone());
    for invalid in [query(0, 10), query(1, 0), query(51, 10)] {
        assert!(matches!(
            store.list_recent(invalid).await,
            Err(MemoryStoreError::InvalidInput(_))
        ));
        assert_eq!(database.calls(), 0);
    }
}

#[tokio::test]
async fn valid_context_query_returns_ordered_candidates_and_sentinel() {
    let expected = vec![record("first"), ContextCandidate::Oversized];
    let database = RecordingDatabase::with_result(expected.clone());
    let store = MemoryContextStore::new(database.clone());
    assert_eq!(store.list_recent(query(2, 128)).await.unwrap(), expected);
    assert_eq!(database.calls(), 1);
}

#[tokio::test]
async fn context_database_failure_is_opaque_and_testable() {
    let protected = "protected-content-sentinel";
    let database = FailingDatabase::default();
    let store = MemoryContextStore::new(database.clone());
    let error = store.list_recent(query(2, 128)).await.unwrap_err();
    assert_eq!(database.calls(), 1);
    assert!(!error.to_string().contains(protected));
    assert!(!format!("{error:?}").contains(protected));
}
