use chrono::{DateTime, Datelike, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MemoryVersionInput {
    pub id: String,
    pub version: i64,
    pub memory_type: String,
    pub title: String,
    pub content: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub concepts: Vec<String>,
    pub files: Vec<String>,
    pub session_ids: Vec<String>,
    pub source_observation_ids: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EmbeddingInput {
    pub id: String,
    pub version: i64,
    pub embedding: Vec<f64>,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid input: {field} ({code})")]
pub struct ContractError {
    field: &'static str,
    code: &'static str,
}

impl ContractError {
    pub const fn field(&self) -> &'static str {
        self.field
    }

    pub const fn code(&self) -> &'static str {
        self.code
    }

    const fn invalid(field: &'static str, code: &'static str) -> Self {
        Self { field, code }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DatabaseTarget(String);

impl DatabaseTarget {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for DatabaseTarget {
    type Error = ContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_non_empty(&value, "database_target").map(|()| Self(value))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Bm25Search {
    pub query: String,
    pub limit: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct VectorSearch {
    pub vector: Vec<f64>,
    pub limit: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MemoryId(String);

impl MemoryId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for MemoryId {
    type Error = ContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_non_empty(&value, "id").map(|()| Self(value))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct MemoryVersion(i64);

impl MemoryVersion {
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl TryFrom<i64> for MemoryVersion {
    type Error = ContractError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value <= 0 {
            return Err(ContractError::invalid("version", "not_positive"));
        }

        Ok(Self(value))
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct MemoryVersionKey {
    id: MemoryId,
    version: MemoryVersion,
}

impl MemoryVersionKey {
    pub fn new(id: MemoryId, version: MemoryVersion) -> Self {
        Self { id, version }
    }

    pub fn id(&self) -> &MemoryId {
        &self.id
    }

    pub const fn version(&self) -> MemoryVersion {
        self.version
    }
}

impl fmt::Debug for MemoryVersionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MemoryVersionKey(<redacted>)")
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EmbeddingVector(Vec<f64>);

impl EmbeddingVector {
    pub fn as_slice(&self) -> &[f64] {
        &self.0
    }
}

impl TryFrom<Vec<f64>> for EmbeddingVector {
    type Error = ContractError;

    fn try_from(value: Vec<f64>) -> Result<Self, Self::Error> {
        validate_embedding(&value).map(|()| Self(value))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ValidatedBm25Search {
    query: String,
    limit: u32,
}

impl ValidatedBm25Search {
    pub fn query(&self) -> &str {
        &self.query
    }

    pub const fn limit(&self) -> u32 {
        self.limit
    }
}

impl TryFrom<Bm25Search> for ValidatedBm25Search {
    type Error = ContractError;

    fn try_from(search: Bm25Search) -> Result<Self, Self::Error> {
        validate_non_empty(&search.query, "query")?;
        validate_positive_limit(search.limit)?;

        Ok(Self {
            query: search.query,
            limit: search.limit,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ValidatedVectorSearch {
    vector: Vec<f64>,
    limit: u32,
}

impl ValidatedVectorSearch {
    pub fn vector(&self) -> &[f64] {
        &self.vector
    }

    pub const fn limit(&self) -> u32 {
        self.limit
    }
}

impl TryFrom<VectorSearch> for ValidatedVectorSearch {
    type Error = ContractError;

    fn try_from(search: VectorSearch) -> Result<Self, Self::Error> {
        validate_vector(&search.vector, "vector")?;
        validate_positive_limit(search.limit)?;

        Ok(Self {
            vector: search.vector,
            limit: search.limit,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ValidatedMemoryVersion {
    id: MemoryId,
    version: MemoryVersion,
    memory_type: String,
    title: String,
    content: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    concepts: Vec<String>,
    files: Vec<String>,
    session_ids: Vec<String>,
    source_observation_ids: Vec<String>,
}

impl ValidatedMemoryVersion {
    pub fn id(&self) -> &MemoryId {
        &self.id
    }

    pub const fn version(&self) -> MemoryVersion {
        self.version
    }

    pub fn memory_type(&self) -> &str {
        &self.memory_type
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn content(&self) -> &str {
        &self.content
    }

    pub fn created_at(&self) -> &DateTime<Utc> {
        &self.created_at
    }

    pub fn updated_at(&self) -> &DateTime<Utc> {
        &self.updated_at
    }

    pub fn concepts(&self) -> &[String] {
        &self.concepts
    }

    pub fn files(&self) -> &[String] {
        &self.files
    }

    pub fn session_ids(&self) -> &[String] {
        &self.session_ids
    }

    pub fn source_observation_ids(&self) -> &[String] {
        &self.source_observation_ids
    }
}

impl TryFrom<MemoryVersionInput> for ValidatedMemoryVersion {
    type Error = ContractError;

    fn try_from(input: MemoryVersionInput) -> Result<Self, Self::Error> {
        let MemoryVersionInput {
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
        } = input;

        let id = MemoryId::try_from(id)?;
        let version = MemoryVersion::try_from(version)?;
        validate_non_empty(&memory_type, "memory_type")?;
        validate_non_empty(&title, "title")?;
        validate_non_empty(&content, "content")?;
        validate_timestamp(&created_at, "created_at")?;
        validate_timestamp(&updated_at, "updated_at")?;
        validate_nul_free_collection(&concepts, "concepts")?;
        validate_nul_free_collection(&files, "files")?;
        validate_nul_free_collection(&session_ids, "session_ids")?;
        validate_nul_free_collection(&source_observation_ids, "source_observation_ids")?;

        if updated_at < created_at {
            return Err(ContractError::invalid("updated_at", "before_created_at"));
        }

        Ok(Self {
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
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MemorySearchResult {
    #[serde(flatten)]
    memory: ValidatedMemoryVersion,
    relevance: f64,
}

impl MemorySearchResult {
    pub fn try_new(memory: ValidatedMemoryVersion, relevance: f64) -> Result<Self, ContractError> {
        if !relevance.is_finite() {
            return Err(ContractError::invalid("relevance", "non_finite"));
        }

        Ok(Self { memory, relevance })
    }

    pub fn memory(&self) -> &ValidatedMemoryVersion {
        &self.memory
    }

    pub fn id(&self) -> &MemoryId {
        self.memory.id()
    }

    pub fn version(&self) -> MemoryVersion {
        self.memory.version()
    }

    pub fn memory_type(&self) -> &str {
        self.memory.memory_type()
    }

    pub fn title(&self) -> &str {
        self.memory.title()
    }

    pub fn content(&self) -> &str {
        self.memory.content()
    }

    pub fn created_at(&self) -> &DateTime<Utc> {
        self.memory.created_at()
    }

    pub fn updated_at(&self) -> &DateTime<Utc> {
        self.memory.updated_at()
    }

    pub fn concepts(&self) -> &[String] {
        self.memory.concepts()
    }

    pub fn files(&self) -> &[String] {
        self.memory.files()
    }

    pub fn session_ids(&self) -> &[String] {
        self.memory.session_ids()
    }

    pub fn source_observation_ids(&self) -> &[String] {
        self.memory.source_observation_ids()
    }

    pub const fn relevance(&self) -> f64 {
        self.relevance
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ValidatedEmbedding {
    id: MemoryId,
    version: MemoryVersion,
    embedding: EmbeddingVector,
}

impl ValidatedEmbedding {
    pub fn id(&self) -> &MemoryId {
        &self.id
    }

    pub const fn version(&self) -> MemoryVersion {
        self.version
    }

    pub fn embedding(&self) -> &EmbeddingVector {
        &self.embedding
    }
}

impl TryFrom<EmbeddingInput> for ValidatedEmbedding {
    type Error = ContractError;

    fn try_from(input: EmbeddingInput) -> Result<Self, Self::Error> {
        let EmbeddingInput {
            id,
            version,
            embedding,
        } = input;

        Ok(Self {
            id: MemoryId::try_from(id)?,
            version: MemoryVersion::try_from(version)?,
            embedding: EmbeddingVector::try_from(embedding)?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatabaseOperation {
    InsertMemory,
    InsertEmbedding,
    SearchBm25,
    SearchVector,
}

impl DatabaseOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InsertMemory => "insert_memory",
            Self::InsertEmbedding => "insert_embedding",
            Self::SearchBm25 => "search_bm25",
            Self::SearchVector => "search_vector",
        }
    }
}

impl fmt::Display for DatabaseOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum DatabaseError {
    MissingMemoryVersion {
        key: MemoryVersionKey,
    },
    Conflict {
        operation: DatabaseOperation,
    },
    DatabaseFailure {
        operation: DatabaseOperation,
    },
    InvalidResponse {
        operation: DatabaseOperation,
        column: &'static str,
    },
}

impl DatabaseError {
    pub fn missing_memory_version(key: MemoryVersionKey) -> Self {
        Self::MissingMemoryVersion { key }
    }

    pub const fn conflict(operation: DatabaseOperation) -> Self {
        Self::Conflict { operation }
    }

    pub const fn database_failure(operation: DatabaseOperation) -> Self {
        Self::DatabaseFailure { operation }
    }

    pub const fn invalid_response(operation: DatabaseOperation, column: &'static str) -> Self {
        Self::InvalidResponse { operation, column }
    }
}

impl fmt::Debug for DatabaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingMemoryVersion { .. } => {
                formatter.write_str("DatabaseError::MissingMemoryVersion")
            }
            Self::Conflict { operation } => formatter
                .debug_struct("DatabaseError::Conflict")
                .field("operation", operation)
                .finish(),
            Self::DatabaseFailure { operation } => formatter
                .debug_struct("DatabaseError::DatabaseFailure")
                .field("operation", operation)
                .finish(),
            Self::InvalidResponse { operation, column } => formatter
                .debug_struct("DatabaseError::InvalidResponse")
                .field("operation", operation)
                .field("column", column)
                .finish(),
        }
    }
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingMemoryVersion { .. } => formatter.write_str("missing memory version"),
            Self::Conflict { operation } => write!(formatter, "database conflict: {operation}"),
            Self::DatabaseFailure { operation } => {
                write!(formatter, "database failure: {operation}")
            }
            Self::InvalidResponse { operation, column } => {
                write!(
                    formatter,
                    "invalid database response: {operation} ({column})"
                )
            }
        }
    }
}

impl std::error::Error for DatabaseError {}

#[derive(Clone, Debug, Error, PartialEq)]
pub enum MemoryStoreError {
    #[error(transparent)]
    InvalidInput(#[from] ContractError),
    #[error(transparent)]
    Database(#[from] DatabaseError),
}

fn validate_non_empty(value: &str, field: &'static str) -> Result<(), ContractError> {
    if value.is_empty() {
        return Err(ContractError::invalid(field, "empty"));
    }

    validate_nul_free(value, field)
}

fn validate_nul_free(value: &str, field: &'static str) -> Result<(), ContractError> {
    if value.contains('\0') {
        return Err(ContractError::invalid(field, "contains_nul"));
    }

    Ok(())
}

fn validate_embedding(value: &[f64]) -> Result<(), ContractError> {
    validate_vector(value, "embedding")
}

fn validate_nul_free_collection(
    values: &[String],
    field: &'static str,
) -> Result<(), ContractError> {
    if values.iter().any(|value| value.contains('\0')) {
        return Err(ContractError::invalid(field, "contains_nul"));
    }

    Ok(())
}

fn validate_timestamp(timestamp: &DateTime<Utc>, field: &'static str) -> Result<(), ContractError> {
    let subsecond_nanos = timestamp.timestamp_subsec_nanos();
    if !subsecond_nanos.is_multiple_of(1_000) {
        return Err(ContractError::invalid(field, "not_microsecond_aligned"));
    }

    if !(1..=9999).contains(&timestamp.year()) {
        return Err(ContractError::invalid(field, "unsupported_year"));
    }

    Ok(())
}

fn validate_positive_limit(value: u32) -> Result<(), ContractError> {
    if value == 0 {
        return Err(ContractError::invalid("limit", "not_positive"));
    }

    Ok(())
}

fn validate_vector(value: &[f64], field: &'static str) -> Result<(), ContractError> {
    if value.is_empty() {
        return Err(ContractError::invalid(field, "empty"));
    }

    if value.iter().any(|component| !component.is_finite()) {
        return Err(ContractError::invalid(field, "non_finite"));
    }

    if value.iter().all(|component| *component == 0.0) {
        return Err(ContractError::invalid(field, "zero_norm"));
    }

    if value.len() > 16_000 {
        return Err(ContractError::invalid(field, "too_many_components"));
    }

    if value
        .iter()
        .any(|component| f64::from(*component as f32) != *component)
    {
        return Err(ContractError::invalid(field, "not_float4_exact"));
    }

    Ok(())
}
