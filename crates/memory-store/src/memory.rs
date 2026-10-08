use crate::{
    contracts::{
        Bm25Search, EmbeddingInput, MemorySearchResult, MemoryStoreError, MemoryVersionInput,
        ValidatedBm25Search, ValidatedEmbedding, ValidatedMemoryVersion, ValidatedVectorSearch,
        VectorSearch,
    },
    database::MemoryDatabase,
};

pub struct MemoryStore<D: MemoryDatabase> {
    database: D,
}

impl<D: MemoryDatabase> MemoryStore<D> {
    pub fn new(database: D) -> Self {
        Self { database }
    }

    pub async fn insert_memory(&self, input: MemoryVersionInput) -> Result<(), MemoryStoreError> {
        let memory = ValidatedMemoryVersion::try_from(input)?;

        self.database.insert_memory(&memory).await?;
        Ok(())
    }

    pub async fn insert_embedding(&self, input: EmbeddingInput) -> Result<(), MemoryStoreError> {
        let embedding = ValidatedEmbedding::try_from(input)?;

        self.database.insert_embedding(&embedding).await?;
        Ok(())
    }

    pub async fn search_bm25(
        &self,
        query: Bm25Search,
    ) -> Result<Vec<MemorySearchResult>, MemoryStoreError> {
        let query = ValidatedBm25Search::try_from(query)?;

        Ok(self.database.search_bm25(&query).await?)
    }

    pub async fn search_vector(
        &self,
        query: VectorSearch,
    ) -> Result<Vec<MemorySearchResult>, MemoryStoreError> {
        let query = ValidatedVectorSearch::try_from(query)?;

        Ok(self.database.search_vector(&query).await?)
    }
}
