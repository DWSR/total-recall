use std::{
    fmt,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use memory_store::contracts::{
    Bm25Search, MemoryId, MemorySearchResult, MemoryVersion, MemoryVersionInput, VectorSearch,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionPageQuery {
    pub id: MemoryId,
    pub offset: u64,
    pub limit: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryVersionSummary {
    pub version: MemoryVersion,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    InsertMemory,
    SearchLexical,
    SearchVector,
    GetLatest,
    GetExact,
    ListVersions,
}

impl Operation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InsertMemory => "insert_memory",
            Self::SearchLexical => "search_lexical",
            Self::SearchVector => "search_vector",
            Self::GetLatest => "get_latest",
            Self::GetExact => "get_exact",
            Self::ListVersions => "list_versions",
        }
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationInputField {
    Memory,
    Title,
    Content,
    SessionId,
    Query,
    Vector,
    Id,
    Version,
    Offset,
    Limit,
}

impl OperationInputField {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Title => "title",
            Self::Content => "content",
            Self::SessionId => "session_id",
            Self::Query => "query",
            Self::Vector => "vector",
            Self::Id => "id",
            Self::Version => "version",
            Self::Offset => "offset",
            Self::Limit => "limit",
        }
    }
}

impl fmt::Display for OperationInputField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendResponseField {
    InsertConfirmation,
    SearchResult,
    Memory,
    VersionSummary,
}

impl BackendResponseField {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InsertConfirmation => "insert_confirmation",
            Self::SearchResult => "search_result",
            Self::Memory => "memory",
            Self::VersionSummary => "version_summary",
        }
    }
}

impl fmt::Display for BackendResponseField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendSource {
    MemoryStore,
    Database,
}

impl BackendSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MemoryStore => "memory_store",
            Self::Database => "database",
        }
    }
}

impl fmt::Display for BackendSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationError {
    InvalidInput {
        operation: Operation,
        field: OperationInputField,
    },
    NotFound {
        operation: Operation,
    },
    Conflict {
        operation: Operation,
    },
    BackendFailure {
        operation: Operation,
        source: BackendSource,
    },
    InvalidBackendResponse {
        operation: Operation,
        field: BackendResponseField,
    },
}

impl OperationError {
    pub const fn invalid_input(operation: Operation, field: OperationInputField) -> Self {
        Self::InvalidInput { operation, field }
    }

    pub const fn not_found(operation: Operation) -> Self {
        Self::NotFound { operation }
    }

    pub const fn conflict(operation: Operation) -> Self {
        Self::Conflict { operation }
    }

    pub const fn backend_failure(operation: Operation, source: BackendSource) -> Self {
        Self::BackendFailure { operation, source }
    }

    pub const fn invalid_backend_response(
        operation: Operation,
        field: BackendResponseField,
    ) -> Self {
        Self::InvalidBackendResponse { operation, field }
    }
}

impl fmt::Display for OperationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput { operation, field } => {
                write!(formatter, "invalid input: {operation} ({field})")
            }
            Self::NotFound { operation } => write!(formatter, "not found: {operation}"),
            Self::Conflict { operation } => write!(formatter, "conflict: {operation}"),
            Self::BackendFailure { operation, source } => {
                write!(formatter, "backend failure: {operation} ({source})")
            }
            Self::InvalidBackendResponse { operation, field } => {
                write!(formatter, "invalid backend response: {operation} ({field})")
            }
        }
    }
}

impl std::error::Error for OperationError {}

#[async_trait]
pub trait MemoryOperations: Send + Sync {
    async fn insert_memory(&self, memory: MemoryVersionInput) -> Result<(), OperationError>;

    async fn search_lexical(
        &self,
        query: Bm25Search,
    ) -> Result<Vec<MemorySearchResult>, OperationError>;

    async fn search_vector(
        &self,
        query: VectorSearch,
    ) -> Result<Vec<MemorySearchResult>, OperationError>;

    async fn get_latest(&self, id: MemoryId) -> Result<MemoryVersionInput, OperationError>;

    async fn get_exact(
        &self,
        id: MemoryId,
        version: MemoryVersion,
    ) -> Result<MemoryVersionInput, OperationError>;

    async fn list_versions(
        &self,
        query: VersionPageQuery,
    ) -> Result<Vec<MemoryVersionSummary>, OperationError>;
}

#[derive(Clone, Debug, PartialEq)]
pub enum OperationCall {
    InsertMemory(MemoryVersionInput),
    SearchLexical(Bm25Search),
    SearchVector(VectorSearch),
    GetLatest(MemoryId),
    GetExact {
        id: MemoryId,
        version: MemoryVersion,
    },
    ListVersions(VersionPageQuery),
}

#[derive(Clone, Debug, PartialEq)]
pub struct MemoryOperationResponses {
    pub insert_memory: Result<(), OperationError>,
    pub search_lexical: Result<Vec<MemorySearchResult>, OperationError>,
    pub search_vector: Result<Vec<MemorySearchResult>, OperationError>,
    pub get_latest: Result<MemoryVersionInput, OperationError>,
    pub get_exact: Result<MemoryVersionInput, OperationError>,
    pub list_versions: Result<Vec<MemoryVersionSummary>, OperationError>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryOperationFailures {
    pub insert_memory: OperationError,
    pub search_lexical: OperationError,
    pub search_vector: OperationError,
    pub get_latest: OperationError,
    pub get_exact: OperationError,
    pub list_versions: OperationError,
}

#[derive(Clone, Default)]
struct CallHistory {
    calls: Arc<Mutex<Vec<OperationCall>>>,
}

impl CallHistory {
    fn record(&self, call: OperationCall) {
        self.calls
            .lock()
            .expect("operation call history lock should not be poisoned")
            .push(call);
    }

    fn calls(&self) -> Vec<OperationCall> {
        self.calls
            .lock()
            .expect("operation call history lock should not be poisoned")
            .clone()
    }
}

#[derive(Clone)]
pub struct RecordingOperations {
    history: CallHistory,
    responses: MemoryOperationResponses,
}

impl RecordingOperations {
    pub fn new(responses: MemoryOperationResponses) -> Self {
        Self {
            history: CallHistory::default(),
            responses,
        }
    }

    pub fn calls(&self) -> Vec<OperationCall> {
        self.history.calls()
    }
}

#[async_trait]
impl MemoryOperations for RecordingOperations {
    async fn insert_memory(&self, memory: MemoryVersionInput) -> Result<(), OperationError> {
        self.history.record(OperationCall::InsertMemory(memory));
        self.responses.insert_memory.clone()
    }

    async fn search_lexical(
        &self,
        query: Bm25Search,
    ) -> Result<Vec<MemorySearchResult>, OperationError> {
        self.history.record(OperationCall::SearchLexical(query));
        self.responses.search_lexical.clone()
    }

    async fn search_vector(
        &self,
        query: VectorSearch,
    ) -> Result<Vec<MemorySearchResult>, OperationError> {
        self.history.record(OperationCall::SearchVector(query));
        self.responses.search_vector.clone()
    }

    async fn get_latest(&self, id: MemoryId) -> Result<MemoryVersionInput, OperationError> {
        self.history.record(OperationCall::GetLatest(id));
        self.responses.get_latest.clone()
    }

    async fn get_exact(
        &self,
        id: MemoryId,
        version: MemoryVersion,
    ) -> Result<MemoryVersionInput, OperationError> {
        self.history.record(OperationCall::GetExact { id, version });
        self.responses.get_exact.clone()
    }

    async fn list_versions(
        &self,
        query: VersionPageQuery,
    ) -> Result<Vec<MemoryVersionSummary>, OperationError> {
        self.history.record(OperationCall::ListVersions(query));
        self.responses.list_versions.clone()
    }
}

#[derive(Clone)]
pub struct FailingOperations {
    history: CallHistory,
    failures: MemoryOperationFailures,
}

impl FailingOperations {
    pub fn new(failures: MemoryOperationFailures) -> Self {
        Self {
            history: CallHistory::default(),
            failures,
        }
    }

    pub fn calls(&self) -> Vec<OperationCall> {
        self.history.calls()
    }
}

#[async_trait]
impl MemoryOperations for FailingOperations {
    async fn insert_memory(&self, memory: MemoryVersionInput) -> Result<(), OperationError> {
        self.history.record(OperationCall::InsertMemory(memory));
        Err(self.failures.insert_memory.clone())
    }

    async fn search_lexical(
        &self,
        query: Bm25Search,
    ) -> Result<Vec<MemorySearchResult>, OperationError> {
        self.history.record(OperationCall::SearchLexical(query));
        Err(self.failures.search_lexical.clone())
    }

    async fn search_vector(
        &self,
        query: VectorSearch,
    ) -> Result<Vec<MemorySearchResult>, OperationError> {
        self.history.record(OperationCall::SearchVector(query));
        Err(self.failures.search_vector.clone())
    }

    async fn get_latest(&self, id: MemoryId) -> Result<MemoryVersionInput, OperationError> {
        self.history.record(OperationCall::GetLatest(id));
        Err(self.failures.get_latest.clone())
    }

    async fn get_exact(
        &self,
        id: MemoryId,
        version: MemoryVersion,
    ) -> Result<MemoryVersionInput, OperationError> {
        self.history.record(OperationCall::GetExact { id, version });
        Err(self.failures.get_exact.clone())
    }

    async fn list_versions(
        &self,
        query: VersionPageQuery,
    ) -> Result<Vec<MemoryVersionSummary>, OperationError> {
        self.history.record(OperationCall::ListVersions(query));
        Err(self.failures.list_versions.clone())
    }
}
