use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use memory_store::contracts::{MemoryVersionInput, ValidatedMemoryVersion};
use serde::Serialize;
use uuid::Uuid;

use crate::contracts::{
    Claim, ContractError, FailureCategory, FailureStage, GeneratedRevision, LeaseToken,
    ProcessingError, ReceiptId, Retryability, SnapshotLoadOutcome, StructuredGenerationRequest,
    StructuredGenerationResponse, SweepOutcome, Transcript,
};

pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[async_trait]
pub trait SessionProcessingService: Send + Sync {
    async fn run_sweep(&self, now: DateTime<Utc>) -> Result<SweepOutcome, ProcessingError>;
}

#[async_trait]
pub trait SessionRepository: Send + Sync {
    async fn claim_eligible(
        &self,
        now: DateTime<Utc>,
        limit: u32,
        lease: Duration,
    ) -> Result<Vec<Claim>, RepositoryError>;

    async fn renew(
        &self,
        claim: &Claim,
        now: DateTime<Utc>,
        lease: Duration,
    ) -> Result<bool, RepositoryError>;

    async fn load_snapshot(&self, claim: &Claim) -> Result<SnapshotLoadOutcome, RepositoryError>;

    async fn stage(&self, claim: &Claim, output: StagedRevision) -> Result<(), RepositoryError>;

    async fn load_unpublished_candidates(
        &self,
        claim: &Claim,
    ) -> Result<Vec<StagedMemoryCandidate>, RepositoryError>;

    async fn mark_memory_published(
        &self,
        claim: &Claim,
        memory_id: &str,
    ) -> Result<(), RepositoryError>;

    async fn promote(&self, claim: &Claim) -> Result<(), RepositoryError>;

    async fn retry(
        &self,
        claim: &Claim,
        failure: FailureCategory,
        next_at: DateTime<Utc>,
    ) -> Result<(), RepositoryError>;
}

#[derive(Clone, PartialEq)]
pub struct StagedMemoryCandidate {
    content_fingerprint: String,
    canonical_payload: MemoryVersionInput,
    supporting_receipt_ids: Vec<ReceiptId>,
}

impl StagedMemoryCandidate {
    pub fn try_new(
        content_fingerprint: String,
        canonical_payload: MemoryVersionInput,
        supporting_receipt_ids: Vec<String>,
    ) -> Result<Self, ContractError> {
        if content_fingerprint.is_empty() {
            return Err(ContractError::invalid("content_fingerprint", "empty"));
        }
        if content_fingerprint.contains('\0') {
            return Err(ContractError::invalid(
                "content_fingerprint",
                "contains_nul",
            ));
        }
        ValidatedMemoryVersion::try_from(canonical_payload.clone())
            .map_err(|_| ContractError::invalid("canonical_payload", "invalid"))?;

        let memory_id = Uuid::parse_str(&canonical_payload.id)
            .map_err(|_| ContractError::invalid("canonical_payload", "memory_id_not_uuid_v5"))?;
        if memory_id.is_nil() || memory_id.get_version_num() != 5 {
            return Err(ContractError::invalid(
                "canonical_payload",
                "memory_id_not_uuid_v5",
            ));
        }
        if canonical_payload.memory_type != "session-derived" {
            return Err(ContractError::invalid(
                "canonical_payload",
                "wrong_memory_type",
            ));
        }
        if canonical_payload.version != 1 {
            return Err(ContractError::invalid("canonical_payload", "wrong_version"));
        }
        if !canonical_payload.files.is_empty() {
            return Err(ContractError::invalid(
                "canonical_payload",
                "files_not_empty",
            ));
        }

        let supporting_receipt_ids =
            parse_supporting_receipt_ids(&supporting_receipt_ids, "supporting_receipt_ids")?;
        let payload_receipt_ids = parse_supporting_receipt_ids(
            &canonical_payload.source_observation_ids,
            "canonical_payload",
        )?;
        if supporting_receipt_ids != payload_receipt_ids {
            return Err(ContractError::invalid(
                "supporting_receipt_ids",
                "does_not_match_canonical_payload",
            ));
        }

        Ok(Self {
            content_fingerprint,
            canonical_payload,
            supporting_receipt_ids,
        })
    }

    pub fn content_fingerprint(&self) -> &str {
        &self.content_fingerprint
    }

    pub fn canonical_payload(&self) -> &MemoryVersionInput {
        &self.canonical_payload
    }

    pub fn supporting_receipt_ids(&self) -> &[ReceiptId] {
        &self.supporting_receipt_ids
    }

    pub fn memory_id(&self) -> &str {
        &self.canonical_payload.id
    }

    fn validate_for_claim(&self, claim: &Claim) -> Result<(), ContractError> {
        if self.canonical_payload.session_ids.len() != 1
            || self.canonical_payload.session_ids[0] != claim.session_id().as_str()
        {
            return Err(ContractError::invalid(
                "canonical_payload",
                "session_id_does_not_match_claim",
            ));
        }

        Ok(())
    }

    fn matches_claim(&self, claim: &Claim) -> bool {
        self.canonical_payload.session_ids.len() == 1
            && self.canonical_payload.session_ids[0] == claim.session_id().as_str()
    }

    fn merge(&mut self, other: Self) -> Result<(), ContractError> {
        if !canonical_payloads_match_except_supporting_receipts(
            &self.canonical_payload,
            &other.canonical_payload,
        ) {
            return Err(ContractError::invalid(
                "candidate_payloads",
                "conflicting_fingerprint",
            ));
        }

        let mut supporting_receipt_ids = self
            .supporting_receipt_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        supporting_receipt_ids.extend(other.supporting_receipt_ids);
        self.supporting_receipt_ids = supporting_receipt_ids.into_iter().collect();
        self.canonical_payload.source_observation_ids = self
            .supporting_receipt_ids
            .iter()
            .map(|receipt_id| receipt_id.as_str().to_owned())
            .collect();

        Ok(())
    }
}

impl fmt::Debug for StagedMemoryCandidate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StagedMemoryCandidate")
            .field(
                "supporting_receipt_count",
                &self.supporting_receipt_ids.len(),
            )
            .finish()
    }
}

#[derive(Clone, PartialEq)]
pub struct StagedRevision {
    transcript: Transcript,
    generated_revision: GeneratedRevision,
    candidates: Vec<StagedMemoryCandidate>,
}

impl StagedRevision {
    pub fn try_new(
        claim: &Claim,
        transcript: Transcript,
        generated_revision: GeneratedRevision,
        candidates: Vec<StagedMemoryCandidate>,
    ) -> Result<Self, ContractError> {
        let Some(first_entry) = transcript.entries().first() else {
            return Err(ContractError::invalid("transcript", "empty"));
        };
        if first_entry.session_id() != claim.session_id() {
            return Err(ContractError::invalid(
                "transcript",
                "session_id_does_not_match_claim",
            ));
        }
        if transcript.lifecycle_count() != claim.lifecycle_count()
            || transcript.observation_count() != claim.observation_count()
        {
            return Err(ContractError::invalid(
                "transcript",
                "source_counts_do_not_match_claim",
            ));
        }
        if generated_revision.source_revision() != claim.source_revision() {
            return Err(ContractError::invalid(
                "source_revision",
                "does_not_match_claim",
            ));
        }

        let candidates = merge_candidates(candidates)?;
        for candidate in &candidates {
            candidate.validate_for_claim(claim)?;
        }

        Ok(Self {
            transcript,
            generated_revision,
            candidates,
        })
    }

    pub fn transcript(&self) -> &Transcript {
        &self.transcript
    }

    pub fn generated_revision(&self) -> &GeneratedRevision {
        &self.generated_revision
    }

    pub fn candidates(&self) -> &[StagedMemoryCandidate] {
        &self.candidates
    }

    pub(crate) fn matches_claim(&self, claim: &Claim) -> bool {
        self.transcript
            .entries()
            .first()
            .is_some_and(|entry| entry.session_id() == claim.session_id())
            && self.transcript.lifecycle_count() == claim.lifecycle_count()
            && self.transcript.observation_count() == claim.observation_count()
            && self.generated_revision.source_revision() == claim.source_revision()
            && self
                .candidates
                .iter()
                .all(|candidate| candidate.matches_claim(claim))
    }
}

impl fmt::Debug for StagedRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StagedRevision")
            .field("transcript_entry_count", &self.transcript.entries().len())
            .field("generated_revision", &self.generated_revision)
            .field("candidate_count", &self.candidates.len())
            .finish()
    }
}

fn parse_supporting_receipt_ids(
    values: &[String],
    field: &'static str,
) -> Result<Vec<ReceiptId>, ContractError> {
    if values.is_empty() {
        return Err(ContractError::invalid(field, "empty"));
    }

    let mut known = BTreeSet::new();
    let mut receipt_ids = Vec::with_capacity(values.len());
    for value in values {
        let receipt_id = ReceiptId::try_from(value.clone())
            .map_err(|_| ContractError::invalid(field, "invalid_uuid"))?;
        if !known.insert(receipt_id.clone()) {
            return Err(ContractError::invalid(field, "duplicate"));
        }
        receipt_ids.push(receipt_id);
    }

    Ok(receipt_ids)
}

fn canonical_payloads_match_except_supporting_receipts(
    left: &MemoryVersionInput,
    right: &MemoryVersionInput,
) -> bool {
    left.id == right.id
        && left.version == right.version
        && left.memory_type == right.memory_type
        && left.title == right.title
        && left.content == right.content
        && left.created_at == right.created_at
        && left.updated_at == right.updated_at
        && left.concepts == right.concepts
        && left.files == right.files
        && left.session_ids == right.session_ids
}

fn merge_candidates(
    candidates: Vec<StagedMemoryCandidate>,
) -> Result<Vec<StagedMemoryCandidate>, ContractError> {
    let mut indexes: BTreeMap<String, usize> = BTreeMap::new();
    let mut merged: Vec<StagedMemoryCandidate> = Vec::new();

    for candidate in candidates {
        let fingerprint = candidate.content_fingerprint().to_owned();
        if let Some(index) = indexes.get(&fingerprint) {
            merged[*index].merge(candidate)?;
        } else {
            indexes.insert(fingerprint, merged.len());
            merged.push(candidate);
        }
    }

    Ok(merged)
}

#[async_trait]
pub trait ExternalCallLease: Send + Sync {
    async fn permit(&self, maximum: Duration) -> Result<LeasePermit, LeaseError>;
}

#[derive(Clone, Eq, PartialEq)]
pub struct LeasePermit {
    lease_token: LeaseToken,
    deadline: DateTime<Utc>,
}

impl LeasePermit {
    pub fn new(lease_token: LeaseToken, deadline: DateTime<Utc>) -> Self {
        Self {
            lease_token,
            deadline,
        }
    }

    pub fn lease_token(&self) -> &LeaseToken {
        &self.lease_token
    }

    pub fn deadline(&self) -> &DateTime<Utc> {
        &self.deadline
    }
}

impl fmt::Debug for LeasePermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeasePermit")
            .field("lease_token", &self.lease_token)
            .field("deadline", &self.deadline)
            .finish()
    }
}

#[async_trait]
pub trait ModelProvider: Send + Sync {
    async fn generate(
        &self,
        request: StructuredGenerationRequest,
    ) -> Result<StructuredGenerationResponse, ProviderError>;
}

#[async_trait]
pub trait CanonicalMemorySink: Send + Sync {
    async fn ensure_memory(
        &self,
        memory: MemoryVersionInput,
    ) -> Result<EnsureMemoryOutcome, MemoryPublishError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnsureMemoryOutcome {
    Inserted,
    ExistingEquivalent,
}

macro_rules! fixed_port_error {
    ($name:ident { $($constructor:ident => $category:ident),+ $(,)? }) => {
        #[derive(Clone, Copy, Eq, PartialEq, Serialize)]
        pub struct $name {
            stage: FailureStage,
            category: FailureCategory,
            retryability: Retryability,
        }

        impl $name {
            $(
                pub const fn $constructor() -> Self {
                    Self::new(FailureCategory::$category)
                }
            )+

            const fn new(category: FailureCategory) -> Self {
                Self {
                    stage: category.stage(),
                    category,
                    retryability: category.retryability(),
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
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .field("stage", &self.stage)
                    .field("category", &self.category)
                    .field("retryability", &self.retryability)
                    .finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(
                    formatter,
                    "{} failure: {} ({})",
                    self.stage, self.category, self.retryability
                )
            }
        }

        impl std::error::Error for $name {}
    };
}

fixed_port_error!(RepositoryError {
    invocation => RepositoryInvocation,
    unconfirmed_commit => RepositoryUnconfirmedCommit,
    invalid_response => RepositoryInvalidResponse,
});

fixed_port_error!(LeaseError {
    lost => LeaseLost,
});

fixed_port_error!(ProviderError {
    transient => ProviderTransient,
    contract => ProviderContract,
    authentication => ProviderAuthentication,
    unsupported_contract => ProviderUnsupportedContract,
    request_size => ProviderRequestSize,
});

fixed_port_error!(MemoryPublishError {
    unknown => MemoryUnknown,
    mismatch => MemoryMismatch,
});
