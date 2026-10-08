use std::{collections::BTreeSet, fmt};

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, Serializer, ser::SerializeStruct};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, Error, PartialEq, Serialize)]
#[error("invalid processing contract: {field} ({code})")]
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

    pub(crate) const fn invalid(field: &'static str, code: &'static str) -> Self {
        Self { field, code }
    }
}

macro_rules! uuid_identifier {
    ($name:ident, $field:literal, $version:expr, $code:literal) => {
        #[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Serialize)]
        pub struct $name(String);

        impl $name {
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = ContractError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                validate_uuid_version(&value, $field, $version, $code).map(|()| Self(value))
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($name), "(<redacted>)"))
            }
        }
    };
}

uuid_identifier!(AttemptId, "attempt_id", Some(7), "not_uuid_v7");
uuid_identifier!(LeaseToken, "lease_token", Some(7), "not_uuid_v7");
uuid_identifier!(ReceiptId, "receipt_id", None, "not_uuid_v7");

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct SourceRevision(String);

impl SourceRevision {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SourceRevision {
    type Error = ContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let revision = parse_uuid(&value, "source_revision")?;
        if revision.get_version_num() != 5 {
            return Err(ContractError::invalid("source_revision", "not_uuid_v5"));
        }

        Ok(Self(value))
    }
}

impl fmt::Debug for SourceRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SourceRevision(<redacted>)")
    }
}

#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct CorrelationId(String);

impl CorrelationId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for CorrelationId {
    type Error = ContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_uuid(&value, "correlation_id").map(|()| Self(value))
    }
}

impl fmt::Debug for CorrelationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("CorrelationId")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for CorrelationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct SessionId(String);

impl SessionId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SessionId {
    type Error = ContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_non_empty(&value, "session_id").map(|()| Self(value))
    }
}

impl fmt::Debug for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionId(<redacted>)")
    }
}

#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ClaimInput {
    pub attempt_id: String,
    pub session_id: String,
    pub source_revision: String,
    pub lifecycle_count: u64,
    pub observation_count: u64,
    pub state: AttemptState,
    pub lease_token: String,
    pub lease_expires_at: DateTime<Utc>,
}

#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct Claim {
    attempt_id: AttemptId,
    session_id: SessionId,
    source_revision: SourceRevision,
    lifecycle_count: u64,
    observation_count: u64,
    state: AttemptState,
    lease_token: LeaseToken,
    lease_expires_at: DateTime<Utc>,
}

impl Claim {
    pub fn attempt_id(&self) -> &AttemptId {
        &self.attempt_id
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub fn source_revision(&self) -> &SourceRevision {
        &self.source_revision
    }

    pub const fn lifecycle_count(&self) -> u64 {
        self.lifecycle_count
    }

    pub const fn observation_count(&self) -> u64 {
        self.observation_count
    }

    pub const fn state(&self) -> AttemptState {
        self.state
    }

    pub fn lease_token(&self) -> &LeaseToken {
        &self.lease_token
    }

    pub fn lease_expires_at(&self) -> &DateTime<Utc> {
        &self.lease_expires_at
    }
}

impl fmt::Debug for Claim {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Claim")
            .field("lifecycle_count", &self.lifecycle_count)
            .field("observation_count", &self.observation_count)
            .field("state", &self.state)
            .field("lease_expires_at", &self.lease_expires_at)
            .finish()
    }
}

impl TryFrom<ClaimInput> for Claim {
    type Error = ContractError;

    fn try_from(input: ClaimInput) -> Result<Self, Self::Error> {
        let ClaimInput {
            attempt_id,
            session_id,
            source_revision,
            lifecycle_count,
            observation_count,
            state,
            lease_token,
            lease_expires_at,
        } = input;

        let source_count = lifecycle_count
            .checked_add(observation_count)
            .ok_or(ContractError::invalid("source_counts", "overflow"))?;
        if source_count == 0 {
            return Err(ContractError::invalid("source_counts", "empty"));
        }
        if !state.is_active() {
            return Err(ContractError::invalid("state", "not_active"));
        }
        if lease_expires_at.timestamp_subsec_nanos() != 0 {
            return Err(ContractError::invalid(
                "lease_expires_at",
                "not_whole_second",
            ));
        }

        Ok(Self {
            attempt_id: AttemptId::try_from(attempt_id)?,
            session_id: SessionId::try_from(session_id)?,
            source_revision: SourceRevision::try_from(source_revision)?,
            lifecycle_count,
            observation_count,
            state,
            lease_token: LeaseToken::try_from(lease_token)?,
            lease_expires_at,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceEventType {
    SessionStart,
    SessionEnd,
    Observation,
}

impl SourceEventType {
    const fn transcript_entry_rank(self) -> u8 {
        match self {
            Self::SessionStart => 0,
            Self::Observation => 1,
            Self::SessionEnd => 2,
        }
    }
}

#[derive(Clone, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TranscriptEntryInput {
    Lifecycle {
        receipt_id: String,
        event_type: SourceEventType,
        session_id: String,
        project_name: String,
        current_working_directory: String,
        source_timestamp_rfc3339: String,
        source_timestamp_utc: DateTime<Utc>,
        ingested_at: DateTime<Utc>,
    },
    Observation {
        receipt_id: String,
        event_type: SourceEventType,
        session_id: String,
        hook_type: String,
        project_name: String,
        current_working_directory: String,
        source_timestamp_rfc3339: String,
        source_timestamp_utc: DateTime<Utc>,
        ingested_at: DateTime<Utc>,
        data: Value,
    },
}

#[derive(Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TranscriptEntry {
    Lifecycle {
        receipt_id: ReceiptId,
        event_type: SourceEventType,
        session_id: SessionId,
        project_name: String,
        current_working_directory: String,
        source_timestamp_rfc3339: String,
        source_timestamp_utc: DateTime<Utc>,
        ingested_at: DateTime<Utc>,
    },
    Observation {
        receipt_id: ReceiptId,
        event_type: SourceEventType,
        session_id: SessionId,
        hook_type: String,
        project_name: String,
        current_working_directory: String,
        source_timestamp_rfc3339: String,
        source_timestamp_utc: DateTime<Utc>,
        ingested_at: DateTime<Utc>,
        data: Value,
    },
}

impl TranscriptEntry {
    pub fn receipt_id(&self) -> &ReceiptId {
        match self {
            Self::Lifecycle { receipt_id, .. } | Self::Observation { receipt_id, .. } => receipt_id,
        }
    }

    pub const fn event_type(&self) -> SourceEventType {
        match self {
            Self::Lifecycle { event_type, .. } | Self::Observation { event_type, .. } => {
                *event_type
            }
        }
    }

    pub fn session_id(&self) -> &SessionId {
        match self {
            Self::Lifecycle { session_id, .. } | Self::Observation { session_id, .. } => session_id,
        }
    }

    pub fn project_name(&self) -> &str {
        match self {
            Self::Lifecycle { project_name, .. } | Self::Observation { project_name, .. } => {
                project_name
            }
        }
    }

    pub fn current_working_directory(&self) -> &str {
        match self {
            Self::Lifecycle {
                current_working_directory,
                ..
            }
            | Self::Observation {
                current_working_directory,
                ..
            } => current_working_directory,
        }
    }

    pub fn source_timestamp_rfc3339(&self) -> &str {
        match self {
            Self::Lifecycle {
                source_timestamp_rfc3339,
                ..
            }
            | Self::Observation {
                source_timestamp_rfc3339,
                ..
            } => source_timestamp_rfc3339,
        }
    }

    pub fn source_timestamp_utc(&self) -> &DateTime<Utc> {
        match self {
            Self::Lifecycle {
                source_timestamp_utc,
                ..
            }
            | Self::Observation {
                source_timestamp_utc,
                ..
            } => source_timestamp_utc,
        }
    }

    pub fn ingested_at(&self) -> &DateTime<Utc> {
        match self {
            Self::Lifecycle { ingested_at, .. } | Self::Observation { ingested_at, .. } => {
                ingested_at
            }
        }
    }

    pub fn hook_type(&self) -> Option<&str> {
        match self {
            Self::Lifecycle { .. } => None,
            Self::Observation { hook_type, .. } => Some(hook_type),
        }
    }

    pub fn observation_data(&self) -> Option<&Value> {
        match self {
            Self::Lifecycle { .. } => None,
            Self::Observation { data, .. } => Some(data),
        }
    }

    pub const fn is_observation(&self) -> bool {
        matches!(self, Self::Observation { .. })
    }
}

impl fmt::Debug for TranscriptEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TranscriptEntry")
            .field("event_type", &self.event_type())
            .finish()
    }
}

impl TryFrom<TranscriptEntryInput> for TranscriptEntry {
    type Error = ContractError;

    fn try_from(input: TranscriptEntryInput) -> Result<Self, Self::Error> {
        match input {
            TranscriptEntryInput::Lifecycle {
                receipt_id,
                event_type,
                session_id,
                project_name,
                current_working_directory,
                source_timestamp_rfc3339,
                source_timestamp_utc,
                ingested_at,
            } => {
                if matches!(event_type, SourceEventType::Observation) {
                    return Err(ContractError::invalid("entries", "invalid_event_type"));
                }
                validate_source_metadata(
                    &project_name,
                    &current_working_directory,
                    &source_timestamp_rfc3339,
                    &source_timestamp_utc,
                )?;

                Ok(Self::Lifecycle {
                    receipt_id: ReceiptId::try_from(receipt_id)?,
                    event_type,
                    session_id: SessionId::try_from(session_id)?,
                    project_name,
                    current_working_directory,
                    source_timestamp_rfc3339,
                    source_timestamp_utc,
                    ingested_at,
                })
            }
            TranscriptEntryInput::Observation {
                receipt_id,
                event_type,
                session_id,
                hook_type,
                project_name,
                current_working_directory,
                source_timestamp_rfc3339,
                source_timestamp_utc,
                ingested_at,
                data,
            } => {
                if event_type != SourceEventType::Observation {
                    return Err(ContractError::invalid("entries", "invalid_event_type"));
                }
                if !data.is_object() {
                    return Err(ContractError::invalid("data", "not_object"));
                }
                validate_nul_free(&hook_type, "hook_type")?;
                validate_source_metadata(
                    &project_name,
                    &current_working_directory,
                    &source_timestamp_rfc3339,
                    &source_timestamp_utc,
                )?;

                Ok(Self::Observation {
                    receipt_id: ReceiptId::try_from(receipt_id)?,
                    event_type,
                    session_id: SessionId::try_from(session_id)?,
                    hook_type,
                    project_name,
                    current_working_directory,
                    source_timestamp_rfc3339,
                    source_timestamp_utc,
                    ingested_at,
                    data,
                })
            }
        }
    }
}

#[derive(Clone, PartialEq, Serialize)]
pub struct Transcript {
    entries: Vec<TranscriptEntry>,
}

impl Transcript {
    pub fn try_new(mut entries: Vec<TranscriptEntry>) -> Result<Self, ContractError> {
        let Some(first_entry) = entries.first() else {
            return Err(ContractError::invalid("entries", "empty"));
        };
        let session_id = first_entry.session_id();
        let mut receipt_ids = BTreeSet::new();

        for entry in &entries {
            if entry.session_id() != session_id {
                return Err(ContractError::invalid("entries", "mixed_session_ids"));
            }
            if !receipt_ids.insert(entry.receipt_id().clone()) {
                return Err(ContractError::invalid("entries", "duplicate_receipt_id"));
            }
        }

        entries.sort_unstable_by(|left, right| {
            left.source_timestamp_utc()
                .cmp(right.source_timestamp_utc())
                .then_with(|| left.ingested_at().cmp(right.ingested_at()))
                .then_with(|| {
                    left.event_type()
                        .transcript_entry_rank()
                        .cmp(&right.event_type().transcript_entry_rank())
                })
                .then_with(|| left.receipt_id().cmp(right.receipt_id()))
        });

        Ok(Self { entries })
    }

    pub fn entries(&self) -> &[TranscriptEntry] {
        &self.entries
    }

    pub fn lifecycle_count(&self) -> u64 {
        self.entries
            .iter()
            .filter(|entry| !entry.is_observation())
            .count()
            .try_into()
            .expect("transcript entries cannot exceed u64::MAX")
    }

    pub fn observation_count(&self) -> u64 {
        self.entries
            .iter()
            .filter(|entry| entry.is_observation())
            .count()
            .try_into()
            .expect("transcript entries cannot exceed u64::MAX")
    }
}

impl fmt::Debug for Transcript {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Transcript")
            .field("entry_count", &self.entries.len())
            .finish()
    }
}

#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SourceSnapshotInput {
    pub session_id: String,
    pub source_revision: String,
    pub source_cutoff: DateTime<Utc>,
    pub entries: Vec<TranscriptEntryInput>,
}

#[derive(Clone, PartialEq, Serialize)]
pub struct SourceSnapshot {
    session_id: SessionId,
    source_revision: SourceRevision,
    source_cutoff: DateTime<Utc>,
    #[serde(flatten)]
    transcript: Transcript,
}

impl SourceSnapshot {
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub fn source_revision(&self) -> &SourceRevision {
        &self.source_revision
    }

    pub fn source_cutoff(&self) -> &DateTime<Utc> {
        &self.source_cutoff
    }

    pub fn transcript(&self) -> &Transcript {
        &self.transcript
    }

    pub fn matches_claim(&self, claim: &Claim) -> bool {
        self.session_id == claim.session_id
            && self.source_revision == claim.source_revision
            && self.transcript.lifecycle_count() == claim.lifecycle_count
            && self.transcript.observation_count() == claim.observation_count
    }
}

impl fmt::Debug for SourceSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceSnapshot")
            .field("entry_count", &self.transcript.entries.len())
            .field("source_cutoff", &self.source_cutoff)
            .finish()
    }
}

impl TryFrom<SourceSnapshotInput> for SourceSnapshot {
    type Error = ContractError;

    fn try_from(input: SourceSnapshotInput) -> Result<Self, Self::Error> {
        let SourceSnapshotInput {
            session_id,
            source_revision,
            source_cutoff,
            entries,
        } = input;
        let session_id = SessionId::try_from(session_id)?;
        let source_revision = SourceRevision::try_from(source_revision)?;
        let entries = entries
            .into_iter()
            .map(TranscriptEntry::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let transcript = Transcript::try_new(entries)?;

        if transcript
            .entries()
            .iter()
            .any(|entry| entry.session_id() != &session_id)
        {
            return Err(ContractError::invalid("entries", "mixed_session_ids"));
        }
        if transcript
            .entries()
            .iter()
            .any(|entry| entry.ingested_at() > &source_cutoff)
        {
            return Err(ContractError::invalid("entries", "after_source_cutoff"));
        }

        Ok(Self {
            session_id,
            source_revision,
            source_cutoff,
            transcript,
        })
    }
}

#[derive(Clone, PartialEq)]
pub struct LoadedSnapshot {
    snapshot: SourceSnapshot,
    memory_scope: MemoryScope,
}

impl LoadedSnapshot {
    pub fn try_new(
        snapshot: SourceSnapshot,
        memory_scope: MemoryScope,
    ) -> Result<Self, ContractError> {
        for receipt_id in memory_scope.receipt_ids() {
            if !snapshot
                .transcript()
                .entries()
                .iter()
                .any(|entry| entry.is_observation() && entry.receipt_id() == receipt_id)
            {
                return Err(ContractError::invalid(
                    "memory_scope",
                    "not_observation_receipt",
                ));
            }
        }

        Ok(Self {
            snapshot,
            memory_scope,
        })
    }

    pub fn snapshot(&self) -> &SourceSnapshot {
        &self.snapshot
    }

    pub fn memory_scope(&self) -> &MemoryScope {
        &self.memory_scope
    }
}

impl fmt::Debug for LoadedSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LoadedSnapshot")
            .field(
                "source_entry_count",
                &self.snapshot.transcript().entries().len(),
            )
            .field("memory_scope_count", &self.memory_scope.0.len())
            .finish()
    }
}

impl Serialize for LoadedSnapshot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut value = serializer.serialize_struct("LoadedSnapshot", 2)?;
        value.serialize_field(
            "source_entry_count",
            &self.snapshot.transcript().entries().len(),
        )?;
        value.serialize_field("memory_scope_count", &self.memory_scope.0.len())?;
        value.end()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum SnapshotLoadOutcome {
    Current(LoadedSnapshot),
    Superseded,
}

#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct MemoryScope(BTreeSet<ReceiptId>);

impl MemoryScope {
    pub fn contains(&self, receipt_id: &ReceiptId) -> bool {
        self.0.contains(receipt_id)
    }

    pub fn receipt_ids(&self) -> impl Iterator<Item = &ReceiptId> {
        self.0.iter()
    }
}

impl TryFrom<Vec<String>> for MemoryScope {
    type Error = ContractError;

    fn try_from(receipt_ids: Vec<String>) -> Result<Self, Self::Error> {
        let mut values = BTreeSet::new();
        for receipt_id in receipt_ids {
            let receipt_id = ReceiptId::try_from(receipt_id)?;
            if !values.insert(receipt_id) {
                return Err(ContractError::invalid(
                    "memory_scope",
                    "duplicate_receipt_id",
                ));
            }
        }

        Ok(Self(values))
    }
}

impl fmt::Debug for MemoryScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryScope")
            .field("receipt_count", &self.0.len())
            .finish()
    }
}

#[derive(Clone, PartialEq, Serialize)]
pub struct GenerationInput {
    transcript: Transcript,
    memory_scope: MemoryScope,
}

impl GenerationInput {
    pub fn initial(transcript: Transcript) -> Result<Self, ContractError> {
        let memory_scope = MemoryScope::try_from(
            transcript
                .entries()
                .iter()
                .filter(|entry| entry.is_observation())
                .map(|entry| entry.receipt_id().as_str().to_owned())
                .collect::<Vec<_>>(),
        )?;

        Self::try_new(transcript, memory_scope)
    }

    pub fn refresh(
        transcript: Transcript,
        newly_recorded_observation_receipt_ids: Vec<String>,
    ) -> Result<Self, ContractError> {
        Self::try_new(
            transcript,
            MemoryScope::try_from(newly_recorded_observation_receipt_ids)?,
        )
    }

    pub fn try_new(
        transcript: Transcript,
        memory_scope: MemoryScope,
    ) -> Result<Self, ContractError> {
        for receipt_id in memory_scope.receipt_ids() {
            if !transcript
                .entries()
                .iter()
                .any(|entry| entry.is_observation() && entry.receipt_id() == receipt_id)
            {
                return Err(ContractError::invalid(
                    "memory_scope",
                    "not_observation_receipt",
                ));
            }
        }

        Ok(Self {
            transcript,
            memory_scope,
        })
    }

    pub fn transcript(&self) -> &Transcript {
        &self.transcript
    }

    pub fn memory_scope(&self) -> &MemoryScope {
        &self.memory_scope
    }
}

impl fmt::Debug for GenerationInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GenerationInput")
            .field("transcript_entries", &self.transcript.entries.len())
            .field("memory_scope_count", &self.memory_scope.0.len())
            .finish()
    }
}

#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MemoryCandidateInput {
    pub title: String,
    pub content: String,
    pub concepts: Vec<String>,
    pub supporting_receipt_ids: Vec<String>,
}

#[derive(Clone, PartialEq, Serialize)]
pub struct MemoryCandidate {
    title: String,
    content: String,
    concepts: Vec<String>,
    supporting_receipt_ids: Vec<ReceiptId>,
}

impl MemoryCandidate {
    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn content(&self) -> &str {
        &self.content
    }

    pub fn concepts(&self) -> &[String] {
        &self.concepts
    }

    pub fn supporting_receipt_ids(&self) -> &[ReceiptId] {
        &self.supporting_receipt_ids
    }
}

impl fmt::Debug for MemoryCandidate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryCandidate")
            .field("concept_count", &self.concepts.len())
            .field(
                "supporting_receipt_count",
                &self.supporting_receipt_ids.len(),
            )
            .finish()
    }
}

impl TryFrom<MemoryCandidateInput> for MemoryCandidate {
    type Error = ContractError;

    fn try_from(input: MemoryCandidateInput) -> Result<Self, Self::Error> {
        let MemoryCandidateInput {
            title,
            content,
            concepts,
            supporting_receipt_ids,
        } = input;
        validate_non_empty(&title, "title")?;
        validate_non_empty(&content, "content")?;
        validate_nul_free_collection(&concepts, "concepts")?;
        if supporting_receipt_ids.is_empty() {
            return Err(ContractError::invalid("supporting_receipt_ids", "empty"));
        }

        let mut known_receipts = BTreeSet::new();
        let mut receipts = Vec::with_capacity(supporting_receipt_ids.len());
        for receipt_id in supporting_receipt_ids {
            let receipt_id = ReceiptId::try_from(receipt_id)?;
            if !known_receipts.insert(receipt_id.clone()) {
                return Err(ContractError::invalid(
                    "supporting_receipt_ids",
                    "duplicate",
                ));
            }
            receipts.push(receipt_id);
        }

        Ok(Self {
            title,
            content,
            concepts,
            supporting_receipt_ids: receipts,
        })
    }
}

#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StructuredGenerationResponseInput {
    pub summary_sentences: Vec<String>,
    pub concepts: Vec<String>,
    pub memory_candidates: Vec<MemoryCandidateInput>,
}

#[derive(Clone, PartialEq, Serialize)]
pub struct StructuredGenerationResponse {
    summary_sentences: Vec<String>,
    concepts: Vec<String>,
    memory_candidates: Vec<MemoryCandidate>,
}

impl StructuredGenerationResponse {
    pub fn summary_sentences(&self) -> &[String] {
        &self.summary_sentences
    }

    pub fn concepts(&self) -> &[String] {
        &self.concepts
    }

    pub fn memory_candidates(&self) -> &[MemoryCandidate] {
        &self.memory_candidates
    }
}

impl fmt::Debug for StructuredGenerationResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StructuredGenerationResponse")
            .field("summary_sentence_count", &self.summary_sentences.len())
            .field("concept_count", &self.concepts.len())
            .field("memory_candidate_count", &self.memory_candidates.len())
            .finish()
    }
}

impl TryFrom<StructuredGenerationResponseInput> for StructuredGenerationResponse {
    type Error = ContractError;

    fn try_from(input: StructuredGenerationResponseInput) -> Result<Self, Self::Error> {
        let StructuredGenerationResponseInput {
            summary_sentences,
            concepts,
            memory_candidates,
        } = input;
        validate_summary_sentences(&summary_sentences)?;
        let concepts = normalize_concepts(concepts)?;
        let memory_candidates = memory_candidates
            .into_iter()
            .map(MemoryCandidate::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            summary_sentences,
            concepts,
            memory_candidates,
        })
    }
}

#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GeneratedRevisionInput {
    pub source_revision: String,
    pub source_cutoff: DateTime<Utc>,
    pub generated_at: DateTime<Utc>,
    pub summary_sentences: Vec<String>,
    pub concepts: Vec<String>,
    pub memory_candidates: Vec<MemoryCandidateInput>,
}

#[derive(Clone, PartialEq, Serialize)]
pub struct GeneratedRevision {
    source_revision: SourceRevision,
    source_cutoff: DateTime<Utc>,
    generated_at: DateTime<Utc>,
    #[serde(flatten)]
    response: StructuredGenerationResponse,
}

impl GeneratedRevision {
    pub fn source_revision(&self) -> &SourceRevision {
        &self.source_revision
    }

    pub fn source_cutoff(&self) -> &DateTime<Utc> {
        &self.source_cutoff
    }

    pub fn generated_at(&self) -> &DateTime<Utc> {
        &self.generated_at
    }

    pub fn summary_sentences(&self) -> &[String] {
        self.response.summary_sentences()
    }

    pub fn summary(&self) -> String {
        self.response.summary_sentences.join(" ")
    }

    pub fn concepts(&self) -> &[String] {
        self.response.concepts()
    }

    pub fn memory_candidates(&self) -> &[MemoryCandidate] {
        self.response.memory_candidates()
    }
}

impl fmt::Debug for GeneratedRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GeneratedRevision")
            .field("source_cutoff", &self.source_cutoff)
            .field("generated_at", &self.generated_at)
            .field(
                "summary_sentence_count",
                &self.response.summary_sentences.len(),
            )
            .field("concept_count", &self.response.concepts.len())
            .field(
                "memory_candidate_count",
                &self.response.memory_candidates.len(),
            )
            .finish()
    }
}

impl GeneratedRevisionInput {
    pub fn try_into_generated_revision(
        self,
        memory_scope: &MemoryScope,
    ) -> Result<GeneratedRevision, ContractError> {
        let GeneratedRevisionInput {
            source_revision,
            source_cutoff,
            generated_at,
            summary_sentences,
            concepts,
            memory_candidates,
        } = self;
        if generated_at < source_cutoff {
            return Err(ContractError::invalid(
                "generated_at",
                "before_source_cutoff",
            ));
        }
        let source_revision = SourceRevision::try_from(source_revision)?;
        let response = StructuredGenerationResponse::try_from(StructuredGenerationResponseInput {
            summary_sentences,
            concepts,
            memory_candidates,
        })?;

        if response.concepts.len() != 10 {
            return Err(ContractError::invalid("concepts", "wrong_count"));
        }
        if response.memory_candidates.iter().any(|candidate| {
            candidate
                .supporting_receipt_ids()
                .iter()
                .any(|receipt_id| !memory_scope.contains(receipt_id))
        }) {
            return Err(ContractError::invalid(
                "supporting_receipt_ids",
                "outside_memory_scope",
            ));
        }

        Ok(GeneratedRevision {
            source_revision,
            source_cutoff,
            generated_at,
            response,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderOperation {
    Map,
    Reduce,
}

#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StructuredGenerationRequestInput {
    pub operation: ProviderOperation,
    pub prompt: String,
}

#[derive(Clone, PartialEq, Serialize)]
pub struct ProviderPrompt(String);

impl ProviderPrompt {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ProviderPrompt {
    type Error = ContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_non_empty(&value, "prompt").map(|()| Self(value))
    }
}

impl fmt::Debug for ProviderPrompt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProviderPrompt(<redacted>)")
    }
}

#[derive(Clone, PartialEq, Serialize)]
pub struct StructuredGenerationRequest {
    operation: ProviderOperation,
    prompt: ProviderPrompt,
}

impl StructuredGenerationRequest {
    pub const fn operation(&self) -> ProviderOperation {
        self.operation
    }

    pub fn prompt(&self) -> &ProviderPrompt {
        &self.prompt
    }
}

impl fmt::Debug for StructuredGenerationRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StructuredGenerationRequest")
            .field("operation", &self.operation)
            .finish()
    }
}

impl TryFrom<StructuredGenerationRequestInput> for StructuredGenerationRequest {
    type Error = ContractError;

    fn try_from(input: StructuredGenerationRequestInput) -> Result<Self, Self::Error> {
        Ok(Self {
            operation: input.operation,
            prompt: ProviderPrompt::try_from(input.prompt)?,
        })
    }
}

#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProviderExchangeInput {
    pub request: StructuredGenerationRequestInput,
    pub response: StructuredGenerationResponseInput,
}

#[derive(Clone, PartialEq, Serialize)]
pub struct ProviderExchange {
    request: StructuredGenerationRequest,
    response: StructuredGenerationResponse,
}

impl ProviderExchange {
    pub fn request(&self) -> &StructuredGenerationRequest {
        &self.request
    }

    pub fn response(&self) -> &StructuredGenerationResponse {
        &self.response
    }
}

impl fmt::Debug for ProviderExchange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderExchange")
            .field("operation", &self.request.operation)
            .field(
                "summary_sentence_count",
                &self.response.summary_sentences.len(),
            )
            .field("concept_count", &self.response.concepts.len())
            .field(
                "memory_candidate_count",
                &self.response.memory_candidates.len(),
            )
            .finish()
    }
}

impl TryFrom<ProviderExchangeInput> for ProviderExchange {
    type Error = ContractError;

    fn try_from(input: ProviderExchangeInput) -> Result<Self, Self::Error> {
        let request = StructuredGenerationRequest::try_from(input.request)?;
        let response = StructuredGenerationResponse::try_from(input.response)?;

        if request.operation == ProviderOperation::Reduce && response.concepts.len() != 10 {
            return Err(ContractError::invalid("concepts", "wrong_count"));
        }

        Ok(Self { request, response })
    }
}

#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SweepOutcomeInput {
    pub attempted: u32,
    pub staged: u32,
    pub completed: u32,
    pub retryable: u32,
    pub skipped: u32,
}

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize)]
pub struct SweepOutcome {
    attempted: u32,
    staged: u32,
    completed: u32,
    retryable: u32,
    skipped: u32,
}

impl SweepOutcome {
    pub const fn attempted(&self) -> u32 {
        self.attempted
    }

    pub const fn staged(&self) -> u32 {
        self.staged
    }

    pub const fn completed(&self) -> u32 {
        self.completed
    }

    pub const fn retryable(&self) -> u32 {
        self.retryable
    }

    pub const fn skipped(&self) -> u32 {
        self.skipped
    }
}

impl TryFrom<SweepOutcomeInput> for SweepOutcome {
    type Error = ContractError;

    fn try_from(input: SweepOutcomeInput) -> Result<Self, Self::Error> {
        let outcome_count = input
            .staged
            .checked_add(input.completed)
            .and_then(|count| count.checked_add(input.retryable))
            .and_then(|count| count.checked_add(input.skipped))
            .ok_or(ContractError::invalid("sweep_outcome", "count_overflow"))?;
        if outcome_count != input.attempted {
            return Err(ContractError::invalid(
                "sweep_outcome",
                "counts_do_not_balance",
            ));
        }

        Ok(Self {
            attempted: input.attempted,
            staged: input.staged,
            completed: input.completed,
            retryable: input.retryable,
            skipped: input.skipped,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptState {
    Claimed,
    Staged,
    Publishing,
    Complete,
    Superseded,
    Retryable,
}

impl AttemptState {
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Claimed | Self::Staged | Self::Publishing)
    }

    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Claimed, Self::Staged)
                | (Self::Claimed, Self::Superseded)
                | (Self::Claimed, Self::Retryable)
                | (Self::Staged, Self::Publishing)
                | (Self::Staged, Self::Retryable)
                | (Self::Publishing, Self::Complete)
                | (Self::Publishing, Self::Retryable)
                | (Self::Retryable, Self::Claimed)
        )
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Complete | Self::Superseded)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureStage {
    Startup,
    Source,
    Lease,
    Provider,
    Memory,
    Repository,
}

impl FailureStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Source => "source",
            Self::Lease => "lease",
            Self::Provider => "provider",
            Self::Memory => "memory",
            Self::Repository => "repository",
        }
    }
}

impl fmt::Display for FailureStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Retryability {
    Never,
    AfterBackoff,
    AfterLeaseExpiry,
}

impl Retryability {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::AfterBackoff => "after_backoff",
            Self::AfterLeaseExpiry => "after_lease_expiry",
        }
    }
}

impl fmt::Display for Retryability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureCategory {
    InvalidConfiguration,
    RegistrationMismatch,
    SourceConflict,
    LeaseLost,
    ProviderTransient,
    ProviderContract,
    ProviderAuthentication,
    ProviderUnsupportedContract,
    ProviderRequestSize,
    MemoryUnknown,
    MemoryMismatch,
    RepositoryInvocation,
    RepositoryUnconfirmedCommit,
    RepositoryInvalidResponse,
}

impl FailureCategory {
    pub const fn stage(self) -> FailureStage {
        match self {
            Self::InvalidConfiguration | Self::RegistrationMismatch => FailureStage::Startup,
            Self::SourceConflict => FailureStage::Source,
            Self::LeaseLost => FailureStage::Lease,
            Self::ProviderTransient
            | Self::ProviderContract
            | Self::ProviderAuthentication
            | Self::ProviderUnsupportedContract
            | Self::ProviderRequestSize => FailureStage::Provider,
            Self::MemoryUnknown | Self::MemoryMismatch => FailureStage::Memory,
            Self::RepositoryInvocation
            | Self::RepositoryUnconfirmedCommit
            | Self::RepositoryInvalidResponse => FailureStage::Repository,
        }
    }

    pub const fn retryability(self) -> Retryability {
        match self {
            Self::InvalidConfiguration | Self::RegistrationMismatch | Self::SourceConflict => {
                Retryability::Never
            }
            Self::LeaseLost
            | Self::RepositoryInvocation
            | Self::RepositoryUnconfirmedCommit
            | Self::RepositoryInvalidResponse => Retryability::AfterLeaseExpiry,
            Self::ProviderTransient
            | Self::ProviderContract
            | Self::ProviderAuthentication
            | Self::ProviderUnsupportedContract
            | Self::ProviderRequestSize
            | Self::MemoryUnknown
            | Self::MemoryMismatch => Retryability::AfterBackoff,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidConfiguration => "invalid_configuration",
            Self::RegistrationMismatch => "registration_mismatch",
            Self::SourceConflict => "source_conflict",
            Self::LeaseLost => "lease_lost",
            Self::ProviderTransient => "provider_transient",
            Self::ProviderContract => "provider_contract",
            Self::ProviderAuthentication => "provider_authentication",
            Self::ProviderUnsupportedContract => "provider_unsupported_contract",
            Self::ProviderRequestSize => "provider_request_size",
            Self::MemoryUnknown => "memory_unknown",
            Self::MemoryMismatch => "memory_mismatch",
            Self::RepositoryInvocation => "repository_invocation",
            Self::RepositoryUnconfirmedCommit => "repository_unconfirmed_commit",
            Self::RepositoryInvalidResponse => "repository_invalid_response",
        }
    }
}

impl fmt::Display for FailureCategory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ProcessingError {
    stage: FailureStage,
    category: FailureCategory,
    retryability: Retryability,
    correlation_id: CorrelationId,
}

impl ProcessingError {
    pub fn new(category: FailureCategory, correlation_id: CorrelationId) -> Self {
        Self {
            stage: category.stage(),
            category,
            retryability: category.retryability(),
            correlation_id,
        }
    }

    pub const fn stage(&self) -> FailureStage {
        self.stage
    }

    pub const fn category(&self) -> FailureCategory {
        self.category
    }

    pub const fn retryability(&self) -> Retryability {
        self.retryability
    }

    pub fn correlation_id(&self) -> &CorrelationId {
        &self.correlation_id
    }
}

impl fmt::Debug for ProcessingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProcessingError")
            .field("stage", &self.stage)
            .field("category", &self.category)
            .field("retryability", &self.retryability)
            .field("correlation_id", &self.correlation_id)
            .finish()
    }
}

impl fmt::Display for ProcessingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} failure: {} ({}, correlation={})",
            self.stage, self.category, self.retryability, self.correlation_id
        )
    }
}

impl std::error::Error for ProcessingError {}

fn validate_source_metadata(
    project_name: &str,
    current_working_directory: &str,
    source_timestamp_rfc3339: &str,
    source_timestamp_utc: &DateTime<Utc>,
) -> Result<(), ContractError> {
    validate_nul_free(project_name, "project_name")?;
    validate_nul_free(current_working_directory, "current_working_directory")?;
    validate_non_empty(source_timestamp_rfc3339, "source_timestamp_rfc3339")?;
    let parsed = parse_source_timestamp(source_timestamp_rfc3339)?;
    if parsed != *source_timestamp_utc {
        return Err(ContractError::invalid(
            "source_timestamp_rfc3339",
            "does_not_match_utc",
        ));
    }

    Ok(())
}

fn parse_source_timestamp(value: &str) -> Result<DateTime<Utc>, ContractError> {
    let bytes = value.as_bytes();
    let is_utc = bytes.len() == 24 && matches!(bytes[23], b'Z' | b'z');
    let is_offset = bytes.len() == 29 && matches!(bytes[23], b'+' | b'-') && bytes[26] == b':';
    if !(is_utc || is_offset)
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !matches!(bytes[10], b'T' | b't')
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'.'
        || !digits(bytes, 0, 4)
        || !digits(bytes, 5, 7)
        || !digits(bytes, 8, 10)
        || !digits(bytes, 11, 13)
        || !digits(bytes, 14, 16)
        || !digits(bytes, 17, 19)
        || !digits(bytes, 20, 23)
    {
        return Err(ContractError::invalid(
            "source_timestamp_rfc3339",
            "invalid_rfc3339",
        ));
    }

    if is_offset {
        if !digits(bytes, 24, 26) || !digits(bytes, 27, 29) {
            return Err(ContractError::invalid(
                "source_timestamp_rfc3339",
                "invalid_rfc3339",
            ));
        }
        let offset_hours = value[24..26]
            .parse::<u8>()
            .map_err(|_| ContractError::invalid("source_timestamp_rfc3339", "invalid_rfc3339"))?;
        let offset_minutes = value[27..29]
            .parse::<u8>()
            .map_err(|_| ContractError::invalid("source_timestamp_rfc3339", "invalid_rfc3339"))?;
        if offset_hours > 23 || offset_minutes > 59 {
            return Err(ContractError::invalid(
                "source_timestamp_rfc3339",
                "invalid_rfc3339",
            ));
        }
    }

    let mut normalized = bytes.to_vec();
    normalized[10] = b'T';
    if is_utc {
        normalized[23] = b'Z';
    }
    let normalized = String::from_utf8(normalized)
        .map_err(|_| ContractError::invalid("source_timestamp_rfc3339", "invalid_rfc3339"))?;
    DateTime::parse_from_rfc3339(&normalized)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|_| ContractError::invalid("source_timestamp_rfc3339", "invalid_rfc3339"))
}

fn validate_summary_sentences(sentences: &[String]) -> Result<(), ContractError> {
    if sentences.is_empty() {
        return Err(ContractError::invalid("summary_sentences", "empty"));
    }
    if sentences.len() > 5 {
        return Err(ContractError::invalid("summary_sentences", "too_many"));
    }
    if sentences.iter().any(|sentence| sentence.is_empty()) {
        return Err(ContractError::invalid(
            "summary_sentences",
            "empty_sentence",
        ));
    }
    validate_nul_free_collection(sentences, "summary_sentences")
}

fn normalize_concepts(concepts: Vec<String>) -> Result<Vec<String>, ContractError> {
    let mut normalized = Vec::with_capacity(concepts.len());
    let mut distinct = BTreeSet::new();

    for concept in concepts {
        validate_nul_free(&concept, "concepts")?;
        let concept = concept.trim().to_owned();
        if concept.is_empty() {
            return Err(ContractError::invalid("concepts", "empty"));
        }
        if !distinct.insert(concept.to_lowercase()) {
            return Err(ContractError::invalid("concepts", "duplicate"));
        }
        normalized.push(concept);
    }

    Ok(normalized)
}

fn validate_uuid(value: &str, field: &'static str) -> Result<(), ContractError> {
    parse_uuid(value, field).map(|_| ())
}

fn validate_uuid_version(
    value: &str,
    field: &'static str,
    expected_version: Option<usize>,
    code: &'static str,
) -> Result<(), ContractError> {
    let uuid = parse_uuid(value, field)?;
    if expected_version.is_some_and(|version| uuid.get_version_num() != version) {
        return Err(ContractError::invalid(field, code));
    }

    Ok(())
}

fn parse_uuid(value: &str, field: &'static str) -> Result<Uuid, ContractError> {
    let value =
        Uuid::parse_str(value).map_err(|_| ContractError::invalid(field, "invalid_uuid"))?;
    if value.is_nil() {
        return Err(ContractError::invalid(field, "nil_uuid"));
    }

    Ok(value)
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

fn validate_nul_free_collection(
    values: &[String],
    field: &'static str,
) -> Result<(), ContractError> {
    if values.iter().any(|value| value.contains('\0')) {
        return Err(ContractError::invalid(field, "contains_nul"));
    }

    Ok(())
}

fn digits(bytes: &[u8], start: usize, end: usize) -> bool {
    bytes[start..end].iter().all(|byte| byte.is_ascii_digit())
}
