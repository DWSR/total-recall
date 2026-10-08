use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use memory_store::contracts::MemoryVersionInput;
use session_post_processing::{
    contracts::{
        Claim, FailureCategory, SnapshotLoadOutcome, StructuredGenerationRequest,
        StructuredGenerationResponse,
    },
    ports::{
        CanonicalMemorySink, Clock, EnsureMemoryOutcome, ExternalCallLease, LeaseError,
        LeasePermit, MemoryPublishError, ModelProvider, ProviderError, RepositoryError,
        SessionRepository, StagedMemoryCandidate, StagedRevision,
    },
};
use tokio::sync::watch;

#[derive(Clone)]
pub(crate) struct ManualClock {
    now: Arc<Mutex<DateTime<Utc>>>,
}

impl ManualClock {
    pub(crate) fn new(now: DateTime<Utc>) -> Self {
        Self {
            now: Arc::new(Mutex::new(now)),
        }
    }

    pub(crate) fn set(&self, now: DateTime<Utc>) {
        *self
            .now
            .lock()
            .expect("manual clock lock should not be poisoned") = now;
    }

    pub(crate) fn advance(&self, duration: ChronoDuration) {
        let mut now = self
            .now
            .lock()
            .expect("manual clock lock should not be poisoned");
        *now = now
            .checked_add_signed(duration)
            .expect("manual clock fixture should remain within chrono's range");
    }
}

impl Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        *self
            .now
            .lock()
            .expect("manual clock lock should not be poisoned")
    }
}

#[derive(Clone)]
pub(crate) struct RepositoryResponses {
    pub(crate) claims: Vec<Claim>,
    pub(crate) renewed: bool,
    pub(crate) snapshot: SnapshotLoadOutcome,
    pub(crate) unpublished_candidates: Vec<StagedMemoryCandidate>,
}

#[derive(Clone, PartialEq)]
pub(crate) enum RepositoryCall {
    ClaimEligible {
        now: DateTime<Utc>,
        limit: u32,
        lease: Duration,
    },
    Renew {
        claim: Claim,
        now: DateTime<Utc>,
        lease: Duration,
    },
    LoadSnapshot {
        claim: Claim,
    },
    Stage {
        claim: Claim,
        output: StagedRevision,
    },
    LoadUnpublishedCandidates {
        claim: Claim,
    },
    MarkMemoryPublished {
        claim: Claim,
        memory_id: String,
    },
    Promote {
        claim: Claim,
    },
    Retry {
        claim: Claim,
        failure: FailureCategory,
        next_at: DateTime<Utc>,
    },
}

impl fmt::Debug for RepositoryCall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClaimEligible { now, limit, lease } => formatter
                .debug_struct("RepositoryCall::ClaimEligible")
                .field("now", now)
                .field("limit", limit)
                .field("lease", lease)
                .finish(),
            Self::Renew { claim, now, lease } => formatter
                .debug_struct("RepositoryCall::Renew")
                .field("claim", claim)
                .field("now", now)
                .field("lease", lease)
                .finish(),
            Self::LoadSnapshot { claim } => formatter
                .debug_struct("RepositoryCall::LoadSnapshot")
                .field("claim", claim)
                .finish(),
            Self::Stage { claim, output } => formatter
                .debug_struct("RepositoryCall::Stage")
                .field("claim", claim)
                .field("output", output)
                .finish(),
            Self::LoadUnpublishedCandidates { claim } => formatter
                .debug_struct("RepositoryCall::LoadUnpublishedCandidates")
                .field("claim", claim)
                .finish(),
            Self::MarkMemoryPublished { claim, memory_id } => formatter
                .debug_struct("RepositoryCall::MarkMemoryPublished")
                .field("claim", claim)
                .field("memory_id_length", &memory_id.len())
                .finish(),
            Self::Promote { claim } => formatter
                .debug_struct("RepositoryCall::Promote")
                .field("claim", claim)
                .finish(),
            Self::Retry {
                claim,
                failure,
                next_at,
            } => formatter
                .debug_struct("RepositoryCall::Retry")
                .field("claim", claim)
                .field("failure", failure)
                .field("next_at", next_at)
                .finish(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct RecordingRepository {
    state: Arc<Mutex<RecordingRepositoryState>>,
}

struct RecordingRepositoryState {
    responses: RepositoryResponses,
    calls: Vec<RepositoryCall>,
}

impl RecordingRepository {
    pub(crate) fn new(responses: RepositoryResponses) -> Self {
        Self {
            state: Arc::new(Mutex::new(RecordingRepositoryState {
                responses,
                calls: Vec::new(),
            })),
        }
    }

    pub(crate) fn calls(&self) -> Vec<RepositoryCall> {
        self.state
            .lock()
            .expect("recording repository lock should not be poisoned")
            .calls
            .clone()
    }
}

#[async_trait]
impl SessionRepository for RecordingRepository {
    async fn claim_eligible(
        &self,
        now: DateTime<Utc>,
        limit: u32,
        lease: Duration,
    ) -> Result<Vec<Claim>, RepositoryError> {
        let mut state = self
            .state
            .lock()
            .expect("recording repository lock should not be poisoned");
        state
            .calls
            .push(RepositoryCall::ClaimEligible { now, limit, lease });
        Ok(state.responses.claims.clone())
    }

    async fn renew(
        &self,
        claim: &Claim,
        now: DateTime<Utc>,
        lease: Duration,
    ) -> Result<bool, RepositoryError> {
        let mut state = self
            .state
            .lock()
            .expect("recording repository lock should not be poisoned");
        state.calls.push(RepositoryCall::Renew {
            claim: claim.clone(),
            now,
            lease,
        });
        Ok(state.responses.renewed)
    }

    async fn load_snapshot(&self, claim: &Claim) -> Result<SnapshotLoadOutcome, RepositoryError> {
        let mut state = self
            .state
            .lock()
            .expect("recording repository lock should not be poisoned");
        state.calls.push(RepositoryCall::LoadSnapshot {
            claim: claim.clone(),
        });
        Ok(state.responses.snapshot.clone())
    }

    async fn stage(&self, claim: &Claim, output: StagedRevision) -> Result<(), RepositoryError> {
        self.state
            .lock()
            .expect("recording repository lock should not be poisoned")
            .calls
            .push(RepositoryCall::Stage {
                claim: claim.clone(),
                output,
            });
        Ok(())
    }

    async fn load_unpublished_candidates(
        &self,
        claim: &Claim,
    ) -> Result<Vec<StagedMemoryCandidate>, RepositoryError> {
        let mut state = self
            .state
            .lock()
            .expect("recording repository lock should not be poisoned");
        state.calls.push(RepositoryCall::LoadUnpublishedCandidates {
            claim: claim.clone(),
        });
        Ok(state.responses.unpublished_candidates.clone())
    }

    async fn mark_memory_published(
        &self,
        claim: &Claim,
        memory_id: &str,
    ) -> Result<(), RepositoryError> {
        self.state
            .lock()
            .expect("recording repository lock should not be poisoned")
            .calls
            .push(RepositoryCall::MarkMemoryPublished {
                claim: claim.clone(),
                memory_id: memory_id.to_owned(),
            });
        Ok(())
    }

    async fn promote(&self, claim: &Claim) -> Result<(), RepositoryError> {
        self.state
            .lock()
            .expect("recording repository lock should not be poisoned")
            .calls
            .push(RepositoryCall::Promote {
                claim: claim.clone(),
            });
        Ok(())
    }

    async fn retry(
        &self,
        claim: &Claim,
        failure: FailureCategory,
        next_at: DateTime<Utc>,
    ) -> Result<(), RepositoryError> {
        self.state
            .lock()
            .expect("recording repository lock should not be poisoned")
            .calls
            .push(RepositoryCall::Retry {
                claim: claim.clone(),
                failure,
                next_at,
            });
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct FailingRepository {
    state: Arc<Mutex<FailingRepositoryState>>,
}

struct FailingRepositoryState {
    failure: RepositoryError,
    calls: Vec<RepositoryCall>,
}

impl FailingRepository {
    pub(crate) fn new(failure: RepositoryError) -> Self {
        Self {
            state: Arc::new(Mutex::new(FailingRepositoryState {
                failure,
                calls: Vec::new(),
            })),
        }
    }

    pub(crate) fn calls(&self) -> Vec<RepositoryCall> {
        self.state
            .lock()
            .expect("failing repository lock should not be poisoned")
            .calls
            .clone()
    }

    fn fail(&self, call: RepositoryCall) -> RepositoryError {
        let mut state = self
            .state
            .lock()
            .expect("failing repository lock should not be poisoned");
        state.calls.push(call);
        state.failure
    }
}

#[async_trait]
impl SessionRepository for FailingRepository {
    async fn claim_eligible(
        &self,
        now: DateTime<Utc>,
        limit: u32,
        lease: Duration,
    ) -> Result<Vec<Claim>, RepositoryError> {
        Err(self.fail(RepositoryCall::ClaimEligible { now, limit, lease }))
    }

    async fn renew(
        &self,
        claim: &Claim,
        now: DateTime<Utc>,
        lease: Duration,
    ) -> Result<bool, RepositoryError> {
        Err(self.fail(RepositoryCall::Renew {
            claim: claim.clone(),
            now,
            lease,
        }))
    }

    async fn load_snapshot(&self, claim: &Claim) -> Result<SnapshotLoadOutcome, RepositoryError> {
        Err(self.fail(RepositoryCall::LoadSnapshot {
            claim: claim.clone(),
        }))
    }

    async fn stage(&self, claim: &Claim, output: StagedRevision) -> Result<(), RepositoryError> {
        Err(self.fail(RepositoryCall::Stage {
            claim: claim.clone(),
            output,
        }))
    }

    async fn load_unpublished_candidates(
        &self,
        claim: &Claim,
    ) -> Result<Vec<StagedMemoryCandidate>, RepositoryError> {
        Err(self.fail(RepositoryCall::LoadUnpublishedCandidates {
            claim: claim.clone(),
        }))
    }

    async fn mark_memory_published(
        &self,
        claim: &Claim,
        memory_id: &str,
    ) -> Result<(), RepositoryError> {
        Err(self.fail(RepositoryCall::MarkMemoryPublished {
            claim: claim.clone(),
            memory_id: memory_id.to_owned(),
        }))
    }

    async fn promote(&self, claim: &Claim) -> Result<(), RepositoryError> {
        Err(self.fail(RepositoryCall::Promote {
            claim: claim.clone(),
        }))
    }

    async fn retry(
        &self,
        claim: &Claim,
        failure: FailureCategory,
        next_at: DateTime<Utc>,
    ) -> Result<(), RepositoryError> {
        Err(self.fail(RepositoryCall::Retry {
            claim: claim.clone(),
            failure,
            next_at,
        }))
    }
}

#[derive(Clone, PartialEq)]
pub(crate) struct ProviderCall {
    request: StructuredGenerationRequest,
}

impl ProviderCall {
    pub(crate) fn request(&self) -> &StructuredGenerationRequest {
        &self.request
    }
}

impl fmt::Debug for ProviderCall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderCall")
            .field("operation", &self.request.operation())
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct ScriptedModelProvider {
    state: Arc<Mutex<ScriptedModelProviderState>>,
}

struct ScriptedModelProviderState {
    outcomes: VecDeque<Result<StructuredGenerationResponse, ProviderError>>,
    calls: Vec<ProviderCall>,
}

impl ScriptedModelProvider {
    pub(crate) fn new(
        outcomes: impl IntoIterator<Item = Result<StructuredGenerationResponse, ProviderError>>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(ScriptedModelProviderState {
                outcomes: outcomes.into_iter().collect(),
                calls: Vec::new(),
            })),
        }
    }

    pub(crate) fn calls(&self) -> Vec<ProviderCall> {
        self.state
            .lock()
            .expect("scripted model provider lock should not be poisoned")
            .calls
            .clone()
    }

    pub(crate) fn remaining(&self) -> usize {
        self.state
            .lock()
            .expect("scripted model provider lock should not be poisoned")
            .outcomes
            .len()
    }
}

#[async_trait]
impl ModelProvider for ScriptedModelProvider {
    async fn generate(
        &self,
        request: StructuredGenerationRequest,
    ) -> Result<StructuredGenerationResponse, ProviderError> {
        let mut state = self
            .state
            .lock()
            .expect("scripted model provider lock should not be poisoned");
        state.calls.push(ProviderCall { request });
        state
            .outcomes
            .pop_front()
            .expect("model provider received an unexpected request")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LeaseCall {
    pub(crate) maximum: Duration,
}

#[derive(Clone)]
pub(crate) struct ScriptedLease {
    state: Arc<Mutex<ScriptedLeaseState>>,
}

struct ScriptedLeaseState {
    outcomes: VecDeque<Result<LeasePermit, LeaseError>>,
    calls: Vec<LeaseCall>,
}

impl ScriptedLease {
    pub(crate) fn new(outcomes: impl IntoIterator<Item = Result<LeasePermit, LeaseError>>) -> Self {
        Self {
            state: Arc::new(Mutex::new(ScriptedLeaseState {
                outcomes: outcomes.into_iter().collect(),
                calls: Vec::new(),
            })),
        }
    }

    pub(crate) fn calls(&self) -> Vec<LeaseCall> {
        self.state
            .lock()
            .expect("scripted lease lock should not be poisoned")
            .calls
            .clone()
    }

    pub(crate) fn remaining(&self) -> usize {
        self.state
            .lock()
            .expect("scripted lease lock should not be poisoned")
            .outcomes
            .len()
    }
}

#[async_trait]
impl ExternalCallLease for ScriptedLease {
    async fn permit(&self, maximum: Duration) -> Result<LeasePermit, LeaseError> {
        let mut state = self
            .state
            .lock()
            .expect("scripted lease lock should not be poisoned");
        state.calls.push(LeaseCall { maximum });
        state
            .outcomes
            .pop_front()
            .expect("lease received an unexpected permit request")
    }
}

#[derive(Clone, PartialEq)]
pub(crate) struct MemoryCall {
    memory: MemoryVersionInput,
}

impl MemoryCall {
    pub(crate) fn memory(&self) -> &MemoryVersionInput {
        &self.memory
    }
}

impl fmt::Debug for MemoryCall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryCall")
            .field("version", &self.memory.version)
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct RecordingMemorySink {
    state: Arc<Mutex<RecordingMemorySinkState>>,
}

struct RecordingMemorySinkState {
    outcome: EnsureMemoryOutcome,
    calls: Vec<MemoryCall>,
}

impl RecordingMemorySink {
    pub(crate) fn new(outcome: EnsureMemoryOutcome) -> Self {
        Self {
            state: Arc::new(Mutex::new(RecordingMemorySinkState {
                outcome,
                calls: Vec::new(),
            })),
        }
    }

    pub(crate) fn calls(&self) -> Vec<MemoryCall> {
        self.state
            .lock()
            .expect("recording memory sink lock should not be poisoned")
            .calls
            .clone()
    }
}

#[async_trait]
impl CanonicalMemorySink for RecordingMemorySink {
    async fn ensure_memory(
        &self,
        memory: MemoryVersionInput,
    ) -> Result<EnsureMemoryOutcome, MemoryPublishError> {
        let mut state = self
            .state
            .lock()
            .expect("recording memory sink lock should not be poisoned");
        state.calls.push(MemoryCall { memory });
        Ok(state.outcome)
    }
}

#[derive(Clone)]
pub(crate) struct FailingMemorySink {
    state: Arc<Mutex<FailingMemorySinkState>>,
}

struct FailingMemorySinkState {
    failure: MemoryPublishError,
    calls: Vec<MemoryCall>,
}

impl FailingMemorySink {
    pub(crate) fn new(failure: MemoryPublishError) -> Self {
        Self {
            state: Arc::new(Mutex::new(FailingMemorySinkState {
                failure,
                calls: Vec::new(),
            })),
        }
    }

    pub(crate) fn calls(&self) -> Vec<MemoryCall> {
        self.state
            .lock()
            .expect("failing memory sink lock should not be poisoned")
            .calls
            .clone()
    }
}

#[async_trait]
impl CanonicalMemorySink for FailingMemorySink {
    async fn ensure_memory(
        &self,
        memory: MemoryVersionInput,
    ) -> Result<EnsureMemoryOutcome, MemoryPublishError> {
        let mut state = self
            .state
            .lock()
            .expect("failing memory sink lock should not be poisoned");
        state.calls.push(MemoryCall { memory });
        Err(state.failure)
    }
}

#[derive(Clone)]
pub(crate) struct DeterministicGate {
    state: Arc<Mutex<GateState>>,
    changed: watch::Sender<GateUpdate>,
}

struct GateState {
    entered: usize,
    released: bool,
}

#[derive(Clone, Copy)]
struct GateUpdate {
    entered: usize,
    released: bool,
}

impl DeterministicGate {
    pub(crate) fn new() -> Self {
        let (changed, _) = watch::channel(GateUpdate {
            entered: 0,
            released: false,
        });
        Self {
            state: Arc::new(Mutex::new(GateState {
                entered: 0,
                released: false,
            })),
            changed,
        }
    }

    pub(crate) async fn enter(&self) {
        let mut changed = self.changed.subscribe();
        let released = {
            let mut state = self
                .state
                .lock()
                .expect("deterministic gate lock should not be poisoned");
            state.entered += 1;
            self.changed.send_replace(GateUpdate {
                entered: state.entered,
                released: state.released,
            });
            state.released
        };
        if released {
            return;
        }

        loop {
            if changed.borrow_and_update().released {
                return;
            }
            changed
                .changed()
                .await
                .expect("deterministic gate sender should remain alive");
        }
    }

    pub(crate) async fn wait_for_entered(&self, expected: usize) {
        let mut changed = self.changed.subscribe();
        loop {
            if changed.borrow_and_update().entered >= expected {
                return;
            }
            changed
                .changed()
                .await
                .expect("deterministic gate sender should remain alive");
        }
    }

    pub(crate) fn entered(&self) -> usize {
        self.state
            .lock()
            .expect("deterministic gate lock should not be poisoned")
            .entered
    }

    pub(crate) fn release(&self) {
        let mut state = self
            .state
            .lock()
            .expect("deterministic gate lock should not be poisoned");
        if state.released {
            return;
        }
        state.released = true;
        self.changed.send_replace(GateUpdate {
            entered: state.entered,
            released: true,
        });
    }
}
