use async_trait::async_trait;

use crate::contracts::{
    ContextCandidate, ContextQuery, DatabaseError, MemoryStoreError, ValidatedContextQuery,
};

#[async_trait]
pub trait MemoryContextDatabase: Send + Sync {
    async fn list_recent_context(
        &self,
        query: &ValidatedContextQuery,
    ) -> Result<Vec<ContextCandidate>, DatabaseError>;
}

pub struct MemoryContextStore<D: MemoryContextDatabase> {
    database: D,
}

impl<D: MemoryContextDatabase> MemoryContextStore<D> {
    pub fn new(database: D) -> Self {
        Self { database }
    }

    pub async fn list_recent(
        &self,
        query: ContextQuery,
    ) -> Result<Vec<ContextCandidate>, MemoryStoreError> {
        let query = ValidatedContextQuery::try_from(query)?;
        Ok(self.database.list_recent_context(&query).await?)
    }
}
