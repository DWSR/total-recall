//! Worker boundary contracts.

use std::{
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ErrorStage {
    Config,
    Event,
    Repository,
    Render,
    Router,
    Writer,
    Runtime,
}

impl ErrorStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Config => "config",
            Self::Event => "event",
            Self::Repository => "repository",
            Self::Render => "render",
            Self::Router => "router",
            Self::Writer => "writer",
            Self::Runtime => "runtime",
        }
    }
}

impl fmt::Debug for ErrorStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl fmt::Display for ErrorStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum FailureReason {
    Missing,
    Blank,
    Malformed,
    Inconsistent,
    Unrelated,
    Truncated,
    Oversized,
    Duplicate,
    InputTooLarge,
    Serialization,
    Timeout,
    Backend,
    MalformedResponse,
    Remote,
    ProviderMismatch,
    ModelMismatch,
    CountMismatch,
    InvalidVector,
    MissingParent,
    Registration,
    Ownership,
    Trigger,
    Shutdown,
}

impl FailureReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Blank => "blank",
            Self::Malformed => "malformed",
            Self::Inconsistent => "inconsistent",
            Self::Unrelated => "unrelated",
            Self::Truncated => "truncated",
            Self::Oversized => "oversized",
            Self::Duplicate => "duplicate",
            Self::InputTooLarge => "input_too_large",
            Self::Serialization => "serialization",
            Self::Timeout => "timeout",
            Self::Backend => "backend",
            Self::MalformedResponse => "malformed_response",
            Self::Remote => "remote",
            Self::ProviderMismatch => "provider_mismatch",
            Self::ModelMismatch => "model_mismatch",
            Self::CountMismatch => "count_mismatch",
            Self::InvalidVector => "invalid_vector",
            Self::MissingParent => "missing_parent",
            Self::Registration => "registration",
            Self::Ownership => "ownership",
            Self::Trigger => "trigger",
            Self::Shutdown => "shutdown",
        }
    }
}

impl fmt::Debug for FailureReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl fmt::Display for FailureReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigFailure {
    Missing,
    Blank,
    Malformed,
    Inconsistent,
}

impl ConfigFailure {
    const fn as_reason(self) -> FailureReason {
        match self {
            Self::Missing => FailureReason::Missing,
            Self::Blank => FailureReason::Blank,
            Self::Malformed => FailureReason::Malformed,
            Self::Inconsistent => FailureReason::Inconsistent,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventFailure {
    Malformed,
    Unrelated,
    Truncated,
    Oversized,
    Duplicate,
}

impl EventFailure {
    const fn as_reason(self) -> FailureReason {
        match self {
            Self::Malformed => FailureReason::Malformed,
            Self::Unrelated => FailureReason::Unrelated,
            Self::Truncated => FailureReason::Truncated,
            Self::Oversized => FailureReason::Oversized,
            Self::Duplicate => FailureReason::Duplicate,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepositoryFailure {
    Missing,
    Timeout,
    Backend,
    MalformedResponse,
}

impl RepositoryFailure {
    const fn as_reason(self) -> FailureReason {
        match self {
            Self::Missing => FailureReason::Missing,
            Self::Timeout => FailureReason::Timeout,
            Self::Backend => FailureReason::Backend,
            Self::MalformedResponse => FailureReason::MalformedResponse,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenderFailure {
    InputTooLarge,
    Serialization,
}

impl RenderFailure {
    const fn as_reason(self) -> FailureReason {
        match self {
            Self::InputTooLarge => FailureReason::InputTooLarge,
            Self::Serialization => FailureReason::Serialization,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouterFailure {
    Timeout,
    Remote,
    ProviderMismatch,
    ModelMismatch,
    CountMismatch,
    InvalidVector,
}

impl RouterFailure {
    const fn as_reason(self) -> FailureReason {
        match self {
            Self::Timeout => FailureReason::Timeout,
            Self::Remote => FailureReason::Remote,
            Self::ProviderMismatch => FailureReason::ProviderMismatch,
            Self::ModelMismatch => FailureReason::ModelMismatch,
            Self::CountMismatch => FailureReason::CountMismatch,
            Self::InvalidVector => FailureReason::InvalidVector,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriterFailure {
    MissingParent,
    Timeout,
    Backend,
    MalformedResponse,
}

impl WriterFailure {
    const fn as_reason(self) -> FailureReason {
        match self {
            Self::MissingParent => FailureReason::MissingParent,
            Self::Timeout => FailureReason::Timeout,
            Self::Backend => FailureReason::Backend,
            Self::MalformedResponse => FailureReason::MalformedResponse,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFailure {
    Registration,
    Ownership,
    Trigger,
    Shutdown,
}

impl RuntimeFailure {
    const fn as_reason(self) -> FailureReason {
        match self {
            Self::Registration => FailureReason::Registration,
            Self::Ownership => FailureReason::Ownership,
            Self::Trigger => FailureReason::Trigger,
            Self::Shutdown => FailureReason::Shutdown,
        }
    }
}

/// Only stage-specific constructors can create an embedding error.
///
/// ```compile_fail
/// use memory_embedding::contracts::{EmbeddingError, ErrorStage, FailureReason};
///
/// let _ = EmbeddingError::new(ErrorStage::Router, FailureReason::MissingParent);
/// ```
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct EmbeddingError {
    stage: ErrorStage,
    reason: FailureReason,
}

impl EmbeddingError {
    const fn new(stage: ErrorStage, reason: FailureReason) -> Self {
        Self { stage, reason }
    }

    pub const fn config(reason: ConfigFailure) -> Self {
        Self::new(ErrorStage::Config, reason.as_reason())
    }

    pub const fn event(reason: EventFailure) -> Self {
        Self::new(ErrorStage::Event, reason.as_reason())
    }

    pub const fn repository(reason: RepositoryFailure) -> Self {
        Self::new(ErrorStage::Repository, reason.as_reason())
    }

    pub const fn render(reason: RenderFailure) -> Self {
        Self::new(ErrorStage::Render, reason.as_reason())
    }

    /// ```compile_fail
    /// use memory_embedding::contracts::{EmbeddingError, FailureReason};
    ///
    /// let _ = EmbeddingError::router(FailureReason::MissingParent);
    /// ```
    pub const fn router(reason: RouterFailure) -> Self {
        Self::new(ErrorStage::Router, reason.as_reason())
    }

    pub const fn writer(reason: WriterFailure) -> Self {
        Self::new(ErrorStage::Writer, reason.as_reason())
    }

    pub const fn runtime(reason: RuntimeFailure) -> Self {
        Self::new(ErrorStage::Runtime, reason.as_reason())
    }

    pub const fn stage(self) -> ErrorStage {
        self.stage
    }

    pub const fn reason(self) -> FailureReason {
        self.reason
    }
}

impl fmt::Debug for EmbeddingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "EmbeddingError {{ stage: {}, reason: {} }}",
            self.stage, self.reason
        )
    }
}

impl fmt::Display for EmbeddingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.stage, self.reason)
    }
}

impl std::error::Error for EmbeddingError {}

pub type RepositoryError = EmbeddingError;
pub type RouterError = EmbeddingError;
pub type WriterError = EmbeddingError;

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub struct Deadline(Instant);

impl Deadline {
    pub fn at(instant: Instant) -> Self {
        Self(instant)
    }

    pub fn after(duration: Duration) -> Self {
        Self(Instant::now() + duration)
    }

    pub fn instant(self) -> Instant {
        self.0
    }
}

impl fmt::Debug for Deadline {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Deadline(<opaque>)")
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct MemoryKey {
    id: String,
    version: i64,
}

impl MemoryKey {
    pub fn try_new(
        id: impl Into<String>,
        version: impl AsRef<str>,
    ) -> Result<Self, EmbeddingError> {
        let id = id.into();
        let version = version.as_ref();
        if id.is_empty()
            || id.contains('\0')
            || !is_positive_canonical_decimal(version)
            || version.parse::<i64>().is_err()
        {
            return Err(EmbeddingError::event(EventFailure::Malformed));
        }

        let version = version
            .parse()
            .map_err(|_| EmbeddingError::event(EventFailure::Malformed))?;

        Ok(Self { id, version })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub const fn version(&self) -> i64 {
        self.version
    }
}

impl fmt::Debug for MemoryKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MemoryKey(<redacted>)")
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct RowChangedEvent {
    db: String,
    table: String,
    op: String,
    affected_rows: u64,
    returning: Vec<MemoryKey>,
    at: i64,
    truncated: Option<bool>,
}

impl RowChangedEvent {
    pub fn db(&self) -> &str {
        &self.db
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn op(&self) -> &str {
        &self.op
    }

    pub const fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    pub fn returning(&self) -> &[MemoryKey] {
        &self.returning
    }

    pub const fn at(&self) -> i64 {
        self.at
    }

    pub const fn truncated(&self) -> Option<bool> {
        self.truncated
    }
}

impl TryFrom<Value> for RowChangedEvent {
    type Error = EmbeddingError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let event = serde_json::from_value(value)
            .map_err(|_| EmbeddingError::event(EventFailure::Malformed))?;

        Ok(event)
    }
}

impl<'de> Deserialize<'de> for RowChangedEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let input = RowChangedEventInput::deserialize(deserializer)
            .map_err(|_| serde::de::Error::custom("event:malformed"))?;
        input
            .try_into()
            .map_err(|_| serde::de::Error::custom("event:malformed"))
    }
}

impl fmt::Debug for RowChangedEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RowChangedEvent(<redacted>)")
    }
}

pub fn parse_row_changed_event(value: Value) -> Result<RowChangedEvent, EmbeddingError> {
    RowChangedEvent::try_from(value)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RowChangedEventInput {
    db: String,
    table: String,
    op: String,
    affected_rows: u64,
    returning: Vec<RowChangedKeyInput>,
    at: i64,
    #[serde(default)]
    truncated: TruncationInput,
}

#[derive(Default)]
enum TruncationInput {
    #[default]
    Absent,
    Value(bool),
}

impl<'de> Deserialize<'de> for TruncationInput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        bool::deserialize(deserializer).map(Self::Value)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RowChangedKeyInput {
    id: String,
    version: String,
}

impl TryFrom<RowChangedEventInput> for RowChangedEvent {
    type Error = EmbeddingError;

    fn try_from(input: RowChangedEventInput) -> Result<Self, Self::Error> {
        let returning = input
            .returning
            .into_iter()
            .map(|key| MemoryKey::try_new(key.id, key.version))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            db: input.db,
            table: input.table,
            op: input.op,
            affected_rows: input.affected_rows,
            returning,
            at: input.at,
            truncated: match input.truncated {
                TruncationInput::Absent => None,
                TruncationInput::Value(value) => Some(value),
            },
        })
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct EmbeddingWorkItem {
    key: MemoryKey,
    title: String,
    content: String,
    concepts: Vec<String>,
}

impl EmbeddingWorkItem {
    pub fn new(
        key: MemoryKey,
        title: impl Into<String>,
        content: impl Into<String>,
        concepts: Vec<String>,
    ) -> Self {
        Self {
            key,
            title: title.into(),
            content: content.into(),
            concepts,
        }
    }

    pub fn key(&self) -> &MemoryKey {
        &self.key
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn content(&self) -> &str {
        &self.content
    }

    pub fn concepts(&self) -> &[String] {
        &self.concepts
    }
}

impl fmt::Debug for EmbeddingWorkItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EmbeddingWorkItem(<redacted>)")
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum LoadedEmbeddingWork {
    Pending(EmbeddingWorkItem),
    AlreadyPresent(MemoryKey),
    Missing(MemoryKey),
}

impl fmt::Debug for LoadedEmbeddingWork {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending(_) => formatter.write_str("LoadedEmbeddingWork::Pending(<redacted>)"),
            Self::AlreadyPresent(_) => {
                formatter.write_str("LoadedEmbeddingWork::AlreadyPresent(<redacted>)")
            }
            Self::Missing(_) => formatter.write_str("LoadedEmbeddingWork::Missing(<redacted>)"),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct CanonicalEmbeddingInput {
    key: MemoryKey,
    text: String,
}

impl CanonicalEmbeddingInput {
    pub fn new(key: MemoryKey, text: impl Into<String>) -> Self {
        Self {
            key,
            text: text.into(),
        }
    }

    pub fn key(&self) -> &MemoryKey {
        &self.key
    }

    pub fn text(&self) -> &str {
        &self.text
    }
}

impl fmt::Debug for CanonicalEmbeddingInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CanonicalEmbeddingInput(<redacted>)")
    }
}

#[derive(Clone, PartialEq)]
pub struct GeneratedEmbedding {
    key: MemoryKey,
    vector: Vec<f32>,
}

impl GeneratedEmbedding {
    pub fn try_new(key: MemoryKey, vector: Vec<f32>) -> Result<Self, EmbeddingError> {
        if vector.is_empty()
            || vector.len() > 16_000
            || vector.iter().any(|component| !component.is_finite())
            || vector.iter().all(|component| *component == 0.0)
        {
            return Err(EmbeddingError::router(RouterFailure::InvalidVector));
        }

        Ok(Self { key, vector })
    }

    pub fn key(&self) -> &MemoryKey {
        &self.key
    }

    pub fn vector(&self) -> &[f32] {
        &self.vector
    }

    pub fn into_vector(self) -> Vec<f32> {
        self.vector
    }
}

impl fmt::Debug for GeneratedEmbedding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("GeneratedEmbedding(<redacted>)")
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct EmbeddingOutcome {
    selected: u32,
    generated: u32,
    already_present: u32,
    stored: u32,
}

impl EmbeddingOutcome {
    pub const fn new(selected: u32, generated: u32, already_present: u32, stored: u32) -> Self {
        Self {
            selected,
            generated,
            already_present,
            stored,
        }
    }

    pub const fn selected(&self) -> u32 {
        self.selected
    }

    pub const fn generated(&self) -> u32 {
        self.generated
    }

    pub const fn already_present(&self) -> u32 {
        self.already_present
    }

    pub const fn stored(&self) -> u32 {
        self.stored
    }
}

pub type ReconciliationOutcome = EmbeddingOutcome;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteOutcome {
    Stored,
    AlreadyPresent,
}

#[async_trait]
pub trait EmbeddingWorkRepository: Send + Sync {
    async fn load_keys(
        &self,
        keys: &[MemoryKey],
        deadline: Deadline,
    ) -> Result<Vec<LoadedEmbeddingWork>, RepositoryError>;

    async fn list_missing(
        &self,
        limit: u32,
        deadline: Deadline,
    ) -> Result<Vec<EmbeddingWorkItem>, RepositoryError>;
}

#[async_trait]
pub trait EmbeddingRouter: Send + Sync {
    async fn embed(
        &self,
        inputs: &[CanonicalEmbeddingInput],
        deadline: Deadline,
    ) -> Result<Vec<GeneratedEmbedding>, RouterError>;
}

#[async_trait]
pub trait EmbeddingWriter: Send + Sync {
    async fn insert(
        &self,
        key: MemoryKey,
        vector: Vec<f64>,
        deadline: Deadline,
    ) -> Result<WriteOutcome, WriterError>;
}

#[derive(Clone, Eq, PartialEq)]
pub struct RepositoryResponses {
    pub load_keys: Result<Vec<LoadedEmbeddingWork>, RepositoryError>,
    pub list_missing: Result<Vec<EmbeddingWorkItem>, RepositoryError>,
}

impl fmt::Debug for RepositoryResponses {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RepositoryResponses(<redacted>)")
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct RepositoryFailures {
    pub load_keys: RepositoryError,
    pub list_missing: RepositoryError,
}

impl fmt::Debug for RepositoryFailures {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RepositoryFailures(<redacted>)")
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum RepositoryCall {
    LoadKeys {
        keys: Vec<MemoryKey>,
        deadline: Deadline,
    },
    ListMissing {
        limit: u32,
        deadline: Deadline,
    },
}

impl fmt::Debug for RepositoryCall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LoadKeys { .. } => formatter.write_str("RepositoryCall::LoadKeys(<redacted>)"),
            Self::ListMissing { limit, .. } => formatter
                .debug_struct("RepositoryCall::ListMissing")
                .field("limit", limit)
                .finish(),
        }
    }
}

#[derive(Clone)]
pub struct RecordingEmbeddingWorkRepository {
    history: CallHistory<RepositoryCall>,
    responses: RepositoryResponses,
}

impl RecordingEmbeddingWorkRepository {
    pub fn new(responses: RepositoryResponses) -> Self {
        Self {
            history: CallHistory::default(),
            responses,
        }
    }

    pub fn calls(&self) -> Vec<RepositoryCall> {
        self.history.calls()
    }
}

#[async_trait]
impl EmbeddingWorkRepository for RecordingEmbeddingWorkRepository {
    async fn load_keys(
        &self,
        keys: &[MemoryKey],
        deadline: Deadline,
    ) -> Result<Vec<LoadedEmbeddingWork>, RepositoryError> {
        self.history.record(RepositoryCall::LoadKeys {
            keys: keys.to_vec(),
            deadline,
        });
        self.responses.load_keys.clone()
    }

    async fn list_missing(
        &self,
        limit: u32,
        deadline: Deadline,
    ) -> Result<Vec<EmbeddingWorkItem>, RepositoryError> {
        self.history
            .record(RepositoryCall::ListMissing { limit, deadline });
        self.responses.list_missing.clone()
    }
}

#[derive(Clone)]
pub struct FailingEmbeddingWorkRepository {
    history: CallHistory<RepositoryCall>,
    failures: RepositoryFailures,
}

impl FailingEmbeddingWorkRepository {
    pub fn new(failures: RepositoryFailures) -> Self {
        Self {
            history: CallHistory::default(),
            failures,
        }
    }

    pub fn calls(&self) -> Vec<RepositoryCall> {
        self.history.calls()
    }
}

#[async_trait]
impl EmbeddingWorkRepository for FailingEmbeddingWorkRepository {
    async fn load_keys(
        &self,
        keys: &[MemoryKey],
        deadline: Deadline,
    ) -> Result<Vec<LoadedEmbeddingWork>, RepositoryError> {
        self.history.record(RepositoryCall::LoadKeys {
            keys: keys.to_vec(),
            deadline,
        });
        Err(self.failures.load_keys)
    }

    async fn list_missing(
        &self,
        limit: u32,
        deadline: Deadline,
    ) -> Result<Vec<EmbeddingWorkItem>, RepositoryError> {
        self.history
            .record(RepositoryCall::ListMissing { limit, deadline });
        Err(self.failures.list_missing)
    }
}

#[derive(Clone, PartialEq)]
pub struct RouterResponses {
    pub embed: Result<Vec<GeneratedEmbedding>, RouterError>,
}

impl fmt::Debug for RouterResponses {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RouterResponses(<redacted>)")
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct RouterFailures {
    pub embed: RouterError,
}

impl fmt::Debug for RouterFailures {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RouterFailures(<redacted>)")
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum RouterCall {
    Embed {
        inputs: Vec<CanonicalEmbeddingInput>,
        deadline: Deadline,
    },
}

impl fmt::Debug for RouterCall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RouterCall::Embed(<redacted>)")
    }
}

#[derive(Clone)]
pub struct RecordingEmbeddingRouter {
    history: CallHistory<RouterCall>,
    responses: RouterResponses,
}

impl RecordingEmbeddingRouter {
    pub fn new(responses: RouterResponses) -> Self {
        Self {
            history: CallHistory::default(),
            responses,
        }
    }

    pub fn calls(&self) -> Vec<RouterCall> {
        self.history.calls()
    }
}

#[async_trait]
impl EmbeddingRouter for RecordingEmbeddingRouter {
    async fn embed(
        &self,
        inputs: &[CanonicalEmbeddingInput],
        deadline: Deadline,
    ) -> Result<Vec<GeneratedEmbedding>, RouterError> {
        self.history.record(RouterCall::Embed {
            inputs: inputs.to_vec(),
            deadline,
        });
        self.responses.embed.clone()
    }
}

#[derive(Clone)]
pub struct FailingEmbeddingRouter {
    history: CallHistory<RouterCall>,
    failures: RouterFailures,
}

impl FailingEmbeddingRouter {
    pub fn new(failures: RouterFailures) -> Self {
        Self {
            history: CallHistory::default(),
            failures,
        }
    }

    pub fn calls(&self) -> Vec<RouterCall> {
        self.history.calls()
    }
}

#[async_trait]
impl EmbeddingRouter for FailingEmbeddingRouter {
    async fn embed(
        &self,
        inputs: &[CanonicalEmbeddingInput],
        deadline: Deadline,
    ) -> Result<Vec<GeneratedEmbedding>, RouterError> {
        self.history.record(RouterCall::Embed {
            inputs: inputs.to_vec(),
            deadline,
        });
        Err(self.failures.embed)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct WriterResponses {
    pub insert: Result<WriteOutcome, WriterError>,
}

impl fmt::Debug for WriterResponses {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WriterResponses(<redacted>)")
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct WriterFailures {
    pub insert: WriterError,
}

impl fmt::Debug for WriterFailures {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WriterFailures(<redacted>)")
    }
}

#[derive(Clone, PartialEq)]
pub enum WriterCall {
    Insert {
        key: MemoryKey,
        vector: Vec<f64>,
        deadline: Deadline,
    },
}

impl fmt::Debug for WriterCall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WriterCall::Insert(<redacted>)")
    }
}

#[derive(Clone)]
pub struct RecordingEmbeddingWriter {
    history: CallHistory<WriterCall>,
    responses: WriterResponses,
}

impl RecordingEmbeddingWriter {
    pub fn new(responses: WriterResponses) -> Self {
        Self {
            history: CallHistory::default(),
            responses,
        }
    }

    pub fn calls(&self) -> Vec<WriterCall> {
        self.history.calls()
    }
}

#[async_trait]
impl EmbeddingWriter for RecordingEmbeddingWriter {
    async fn insert(
        &self,
        key: MemoryKey,
        vector: Vec<f64>,
        deadline: Deadline,
    ) -> Result<WriteOutcome, WriterError> {
        self.history.record(WriterCall::Insert {
            key,
            vector,
            deadline,
        });
        self.responses.insert
    }
}

#[derive(Clone)]
pub struct FailingEmbeddingWriter {
    history: CallHistory<WriterCall>,
    failures: WriterFailures,
}

impl FailingEmbeddingWriter {
    pub fn new(failures: WriterFailures) -> Self {
        Self {
            history: CallHistory::default(),
            failures,
        }
    }

    pub fn calls(&self) -> Vec<WriterCall> {
        self.history.calls()
    }
}

#[async_trait]
impl EmbeddingWriter for FailingEmbeddingWriter {
    async fn insert(
        &self,
        key: MemoryKey,
        vector: Vec<f64>,
        deadline: Deadline,
    ) -> Result<WriteOutcome, WriterError> {
        self.history.record(WriterCall::Insert {
            key,
            vector,
            deadline,
        });
        Err(self.failures.insert)
    }
}

#[derive(Clone)]
struct CallHistory<T> {
    calls: Arc<Mutex<Vec<T>>>,
}

impl<T> Default for CallHistory<T> {
    fn default() -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl<T> CallHistory<T> {
    fn record(&self, call: T) {
        self.calls
            .lock()
            .expect("contract call history lock should not be poisoned")
            .push(call);
    }
}

impl<T: Clone> CallHistory<T> {
    fn calls(&self) -> Vec<T> {
        self.calls
            .lock()
            .expect("contract call history lock should not be poisoned")
            .clone()
    }
}

fn is_positive_canonical_decimal(value: &str) -> bool {
    let Some((&first, rest)) = value.as_bytes().split_first() else {
        return false;
    };

    matches!(first, b'1'..=b'9') && rest.iter().all(u8::is_ascii_digit)
}
