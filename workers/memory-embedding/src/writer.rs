//! Immutable embedding persistence.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use iii_sdk::IIIClient;
use memory_store::{
    MemoryStore,
    contracts::{DatabaseError, DatabaseTarget, EmbeddingInput, MemoryStoreError},
    database::{IiiMemoryDatabase, MemoryDatabase},
};

use crate::contracts::{
    Deadline, EmbeddingWriter, MemoryKey, WriteOutcome, WriterError, WriterFailure,
};

pub struct IiiEmbeddingWriter<D: MemoryDatabase = IiiMemoryDatabase> {
    store: MemoryStore<D>,
    database_timeout: Duration,
}

impl IiiEmbeddingWriter<IiiMemoryDatabase> {
    pub fn new(client: IIIClient, database: DatabaseTarget, database_timeout: Duration) -> Self {
        Self::with_store(
            MemoryStore::new(IiiMemoryDatabase::new(client, database)),
            database_timeout,
        )
    }
}

impl<D> IiiEmbeddingWriter<D>
where
    D: MemoryDatabase,
{
    #[doc(hidden)]
    pub fn with_store(store: MemoryStore<D>, database_timeout: Duration) -> Self {
        Self {
            store,
            database_timeout,
        }
    }

    pub async fn insert(
        &self,
        key: MemoryKey,
        vector: Vec<f64>,
        deadline: Deadline,
    ) -> Result<WriteOutcome, WriterError> {
        let request_deadline = request_deadline(self.database_timeout, deadline);
        if request_deadline
            .saturating_duration_since(Instant::now())
            .is_zero()
        {
            return Err(writer_error(WriterFailure::Timeout));
        }

        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(request_deadline),
            self.store.insert_embedding(EmbeddingInput {
                id: key.id().to_owned(),
                version: key.version(),
                embedding: vector,
            }),
        )
        .await
        .map_err(|_| writer_error(WriterFailure::Timeout))?;

        map_store_result(result)
    }
}

fn request_deadline(timeout: Duration, deadline: Deadline) -> Instant {
    let local_deadline = Instant::now()
        .checked_add(timeout)
        .unwrap_or_else(|| deadline.instant());

    local_deadline.min(deadline.instant())
}

#[async_trait]
impl<D> EmbeddingWriter for IiiEmbeddingWriter<D>
where
    D: MemoryDatabase,
{
    async fn insert(
        &self,
        key: MemoryKey,
        vector: Vec<f64>,
        deadline: Deadline,
    ) -> Result<WriteOutcome, WriterError> {
        IiiEmbeddingWriter::insert(self, key, vector, deadline).await
    }
}

const fn writer_error(reason: WriterFailure) -> WriterError {
    crate::contracts::EmbeddingError::writer(reason)
}

fn map_store_result(result: Result<(), MemoryStoreError>) -> Result<WriteOutcome, WriterError> {
    match result {
        Ok(()) => Ok(WriteOutcome::Stored),
        Err(MemoryStoreError::Database(DatabaseError::Conflict { .. })) => {
            Ok(WriteOutcome::AlreadyPresent)
        }
        Err(MemoryStoreError::Database(DatabaseError::MissingMemoryVersion { .. })) => {
            Err(writer_error(WriterFailure::MissingParent))
        }
        Err(MemoryStoreError::InvalidInput(_))
        | Err(MemoryStoreError::Database(DatabaseError::InvalidResponse { .. })) => {
            Err(writer_error(WriterFailure::MalformedResponse))
        }
        Err(MemoryStoreError::Database(DatabaseError::DatabaseFailure { .. })) => {
            Err(writer_error(WriterFailure::Backend))
        }
    }
}
