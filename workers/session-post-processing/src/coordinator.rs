use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::{
    contracts::{
        AttemptState, Claim, ClaimInput, CorrelationId, FailureCategory, GeneratedRevisionInput,
        GenerationInput, MemoryCandidateInput, ProcessingError, SnapshotLoadOutcome, SweepOutcome,
        SweepOutcomeInput,
    },
    generation::{GenerationPipeline, normalize_memory_candidates},
    ports::{
        CanonicalMemorySink, Clock, EnsureMemoryOutcome, ExternalCallLease, LeaseError,
        LeasePermit, ModelProvider, SessionProcessingService, SessionRepository,
        StagedMemoryCandidate, StagedRevision,
    },
};

const MAX_RETRY_JITTER_SECONDS: u8 = 10;

#[derive(Clone)]
pub struct SweepCoordinator {
    clock: Arc<dyn Clock>,
    repository: Arc<dyn SessionRepository>,
    provider: Arc<dyn ModelProvider>,
    memory_sink: Arc<dyn CanonicalMemorySink>,
    batch_limit: u32,
    concurrency: u32,
    lease_duration: Duration,
    model_timeout: Duration,
    model_chunk_bytes: usize,
    jitter: Arc<dyn Fn() -> u8 + Send + Sync>,
}

#[derive(Clone, Copy, Debug)]
pub struct SweepCoordinatorLimits {
    pub batch_limit: u32,
    pub concurrency: u32,
    pub lease_duration: Duration,
    pub model_timeout: Duration,
    pub model_chunk_bytes: usize,
}

impl SweepCoordinator {
    pub fn new(
        clock: Arc<dyn Clock>,
        repository: Arc<dyn SessionRepository>,
        provider: Arc<dyn ModelProvider>,
        memory_sink: Arc<dyn CanonicalMemorySink>,
        limits: SweepCoordinatorLimits,
    ) -> Self {
        Self::with_jitter(clock, repository, provider, memory_sink, limits, || {
            (Uuid::new_v4().as_u128() % 11) as u8
        })
    }

    #[doc(hidden)]
    pub fn with_jitter<F>(
        clock: Arc<dyn Clock>,
        repository: Arc<dyn SessionRepository>,
        provider: Arc<dyn ModelProvider>,
        memory_sink: Arc<dyn CanonicalMemorySink>,
        limits: SweepCoordinatorLimits,
        jitter: F,
    ) -> Self
    where
        F: Fn() -> u8 + Send + Sync + 'static,
    {
        let SweepCoordinatorLimits {
            batch_limit,
            concurrency,
            lease_duration,
            model_timeout,
            model_chunk_bytes,
        } = limits;
        assert!(batch_limit > 0);
        assert!(concurrency > 0 && concurrency <= batch_limit);
        assert!(!lease_duration.is_zero());
        assert!(!model_timeout.is_zero() && model_timeout < lease_duration);
        assert!(model_chunk_bytes > 0);

        Self {
            clock,
            repository,
            provider,
            memory_sink,
            batch_limit,
            concurrency,
            lease_duration,
            model_timeout,
            model_chunk_bytes,
            jitter: Arc::new(jitter),
        }
    }

    fn failure(category: FailureCategory) -> ProcessingError {
        ProcessingError::new(
            category,
            CorrelationId::try_from(Uuid::new_v4().to_string())
                .expect("a UUIDv4 is a valid correlation ID"),
        )
    }

    async fn process_claim(&self, claim: Claim) -> ClaimOutcome {
        match claim.state() {
            AttemptState::Claimed => self.process_claimed(&claim).await,
            AttemptState::Staged | AttemptState::Publishing => self.publish_claim(&claim).await,
            AttemptState::Complete | AttemptState::Superseded | AttemptState::Retryable => {
                ClaimOutcome::Skipped
            }
        }
    }

    async fn process_claimed(&self, claim: &Claim) -> ClaimOutcome {
        if self.renew(claim).await.is_none() {
            return ClaimOutcome::Skipped;
        }

        let loaded = match self.repository.load_snapshot(claim).await {
            Ok(SnapshotLoadOutcome::Current(loaded)) => loaded,
            Ok(SnapshotLoadOutcome::Superseded) => return ClaimOutcome::Skipped,
            Err(error) => return self.retry_after_repository(claim, error.category()).await,
        };
        let input = match GenerationInput::try_new(
            loaded.snapshot().transcript().clone(),
            loaded.memory_scope().clone(),
        ) {
            Ok(input) => input,
            Err(_) => {
                return self
                    .retry_after_backoff(claim, FailureCategory::ProviderContract)
                    .await;
            }
        };

        let lease = RepositoryExternalCallLease {
            clock: Arc::clone(&self.clock),
            repository: Arc::clone(&self.repository),
            claim: claim.clone(),
            lease_duration: self.lease_duration,
        };
        let pipeline = GenerationPipeline::new(
            self.clock.as_ref(),
            &lease,
            self.provider.as_ref(),
            self.model_timeout,
            self.model_chunk_bytes,
        );
        let map_results = match pipeline.map(&input).await {
            Ok(results) => results,
            Err(error) => return self.retry_after_backoff(claim, error.category()).await,
        };
        let reduced = match pipeline.reduce(&input, &map_results).await {
            Ok(result) => result,
            Err(error) => return self.retry_after_backoff(claim, error.category()).await,
        };
        let map_candidates = map_results
            .iter()
            .flat_map(|result| result.memory_candidates().iter().cloned())
            .collect::<Vec<_>>();
        let generated_at = self.clock.now();
        let generated = match (GeneratedRevisionInput {
            source_revision: claim.source_revision().as_str().to_owned(),
            source_cutoff: loaded.snapshot().source_cutoff().to_owned(),
            generated_at,
            summary_sentences: reduced.summary_sentences().to_vec(),
            concepts: reduced.concepts().to_vec(),
            memory_candidates: map_candidates
                .iter()
                .map(|candidate| MemoryCandidateInput {
                    title: candidate.title().to_owned(),
                    content: candidate.content().to_owned(),
                    concepts: candidate.concepts().to_vec(),
                    supporting_receipt_ids: candidate
                        .supporting_receipt_ids()
                        .iter()
                        .map(|receipt_id| receipt_id.as_str().to_owned())
                        .collect(),
                })
                .collect(),
        })
        .try_into_generated_revision(input.memory_scope())
        {
            Ok(generated) => generated,
            Err(_) => {
                return self
                    .retry_after_backoff(claim, FailureCategory::ProviderContract)
                    .await;
            }
        };
        let candidates = match normalize_memory_candidates(&input, generated_at, &map_candidates) {
            Ok(candidates) => candidates,
            Err(error) => return self.retry_after_backoff(claim, error.category()).await,
        };
        let staged =
            match StagedRevision::try_new(claim, input.transcript().clone(), generated, candidates)
            {
                Ok(staged) => staged,
                Err(_) => {
                    return self
                        .retry_after_backoff(claim, FailureCategory::ProviderContract)
                        .await;
                }
            };

        if self.renew(claim).await.is_none() {
            return ClaimOutcome::Skipped;
        }
        match self.repository.stage(claim, staged).await {
            Ok(()) => {
                let staged_claim = Claim::try_from(ClaimInput {
                    attempt_id: claim.attempt_id().as_str().to_owned(),
                    session_id: claim.session_id().as_str().to_owned(),
                    source_revision: claim.source_revision().as_str().to_owned(),
                    lifecycle_count: claim.lifecycle_count(),
                    observation_count: claim.observation_count(),
                    state: AttemptState::Staged,
                    lease_token: claim.lease_token().as_str().to_owned(),
                    lease_expires_at: *claim.lease_expires_at(),
                })
                .expect("a staged claim preserves an already validated claim identity");
                self.publish_claim(&staged_claim).await
            }
            Err(error) => self.retry_after_repository(claim, error.category()).await,
        }
    }

    async fn publish_claim(&self, claim: &Claim) -> ClaimOutcome {
        if self.renew(claim).await.is_none() {
            return ClaimOutcome::Staged;
        }
        let candidates = match self.repository.load_unpublished_candidates(claim).await {
            Ok(candidates) => candidates,
            Err(error) => {
                return staged_or_retry(self.retry_after_repository(claim, error.category()).await);
            }
        };
        let lease = RepositoryExternalCallLease {
            clock: Arc::clone(&self.clock),
            repository: Arc::clone(&self.repository),
            claim: claim.clone(),
            lease_duration: self.lease_duration,
        };

        for candidate in candidates {
            let permit = match lease.permit(self.model_timeout).await {
                Ok(permit) => permit,
                Err(_) => return ClaimOutcome::Staged,
            };
            match self.ensure_candidate_memory(&candidate, &permit).await {
                Ok(()) => {}
                Err(MemoryCallFailure::LeaseLost) => return ClaimOutcome::Staged,
                Err(MemoryCallFailure::Failed(category)) => {
                    return staged_or_retry(self.retry_after_backoff(claim, category).await);
                }
            }
            if self.renew(claim).await.is_none() {
                return ClaimOutcome::Staged;
            }
            if let Err(error) = self
                .repository
                .mark_memory_published(claim, candidate.memory_id())
                .await
            {
                return staged_or_retry(self.retry_after_repository(claim, error.category()).await);
            }
        }

        if self.renew(claim).await.is_none() {
            return ClaimOutcome::Staged;
        }
        match self.repository.promote(claim).await {
            Ok(()) => ClaimOutcome::Completed,
            Err(error) => {
                staged_or_retry(self.retry_after_repository(claim, error.category()).await)
            }
        }
    }

    async fn ensure_candidate_memory(
        &self,
        candidate: &StagedMemoryCandidate,
        permit: &LeasePermit,
    ) -> Result<(), MemoryCallFailure> {
        let timeout = permit
            .deadline()
            .signed_duration_since(self.clock.now())
            .to_std()
            .ok()
            .filter(|duration| !duration.is_zero())
            .ok_or(MemoryCallFailure::LeaseLost)?;

        match tokio::time::timeout(
            timeout,
            self.memory_sink
                .ensure_memory(candidate.canonical_payload().clone()),
        )
        .await
        {
            Ok(Ok(EnsureMemoryOutcome::Inserted | EnsureMemoryOutcome::ExistingEquivalent)) => {
                Ok(())
            }
            Ok(Err(error)) => Err(MemoryCallFailure::Failed(error.category())),
            Err(_) => Err(MemoryCallFailure::Failed(FailureCategory::MemoryUnknown)),
        }
    }

    async fn retry_after_backoff(&self, claim: &Claim, failure: FailureCategory) -> ClaimOutcome {
        if failure == FailureCategory::LeaseLost {
            return ClaimOutcome::Skipped;
        }
        if self.renew(claim).await.is_none() {
            return ClaimOutcome::Skipped;
        }
        let Some(next_at) = provider_retry_at(self.clock.now(), (self.jitter)()) else {
            return ClaimOutcome::Skipped;
        };

        match self.repository.retry(claim, failure, next_at).await {
            Ok(()) => ClaimOutcome::Retryable,
            Err(_) => ClaimOutcome::Skipped,
        }
    }

    async fn retry_after_repository(
        &self,
        claim: &Claim,
        failure: FailureCategory,
    ) -> ClaimOutcome {
        let Some(next_at) = self.renew(claim).await else {
            return ClaimOutcome::Skipped;
        };

        match self.repository.retry(claim, failure, next_at).await {
            Ok(()) => ClaimOutcome::Retryable,
            Err(_) => ClaimOutcome::Skipped,
        }
    }

    async fn renew(&self, claim: &Claim) -> Option<DateTime<Utc>> {
        let now = self.clock.now();
        let expires_at = lease_expiry_at(now, self.lease_duration)?;
        match self.repository.renew(claim, now, self.lease_duration).await {
            Ok(true) => Some(expires_at),
            Ok(false) | Err(_) => None,
        }
    }
}

#[async_trait]
impl SessionProcessingService for SweepCoordinator {
    async fn run_sweep(&self, now: DateTime<Utc>) -> Result<SweepOutcome, ProcessingError> {
        let claims = self
            .repository
            .claim_eligible(now, self.batch_limit, self.lease_duration)
            .await
            .map_err(|error| Self::failure(error.category()))?;
        let attempted = u32::try_from(claims.len())
            .map_err(|_| Self::failure(FailureCategory::RepositoryInvalidResponse))?;
        if attempted > self.batch_limit {
            return Err(Self::failure(FailureCategory::RepositoryInvalidResponse));
        }

        let mut jobs = JoinSet::new();
        let mut outcomes = OutcomeCounts::default();
        let concurrency = self.concurrency as usize;
        for claim in claims {
            if jobs.len() == concurrency {
                let outcome = jobs
                    .join_next()
                    .await
                    .expect("non-empty JoinSet must yield one task")
                    .map_err(|_| Self::failure(FailureCategory::RepositoryInvalidResponse))?;
                outcomes.add(outcome);
            }

            let coordinator = self.clone();
            jobs.spawn(async move { coordinator.process_claim(claim).await });
        }
        while let Some(outcome) = jobs.join_next().await {
            outcomes.add(
                outcome.map_err(|_| Self::failure(FailureCategory::RepositoryInvalidResponse))?,
            );
        }

        SweepOutcome::try_from(SweepOutcomeInput {
            attempted,
            staged: outcomes.staged,
            completed: outcomes.completed,
            retryable: outcomes.retryable,
            skipped: outcomes.skipped,
        })
        .map_err(|_| Self::failure(FailureCategory::RepositoryInvalidResponse))
    }
}

#[derive(Clone, Copy)]
enum ClaimOutcome {
    Staged,
    Completed,
    Retryable,
    Skipped,
}

#[derive(Default)]
struct OutcomeCounts {
    staged: u32,
    completed: u32,
    retryable: u32,
    skipped: u32,
}

impl OutcomeCounts {
    fn add(&mut self, outcome: ClaimOutcome) {
        match outcome {
            ClaimOutcome::Staged => self.staged += 1,
            ClaimOutcome::Completed => self.completed += 1,
            ClaimOutcome::Retryable => self.retryable += 1,
            ClaimOutcome::Skipped => self.skipped += 1,
        }
    }
}

fn staged_or_retry(outcome: ClaimOutcome) -> ClaimOutcome {
    match outcome {
        ClaimOutcome::Skipped => ClaimOutcome::Staged,
        outcome => outcome,
    }
}

enum MemoryCallFailure {
    LeaseLost,
    Failed(FailureCategory),
}

struct RepositoryExternalCallLease {
    clock: Arc<dyn Clock>,
    repository: Arc<dyn SessionRepository>,
    claim: Claim,
    lease_duration: Duration,
}

#[async_trait]
impl ExternalCallLease for RepositoryExternalCallLease {
    async fn permit(&self, maximum: Duration) -> Result<LeasePermit, LeaseError> {
        let now = self.clock.now();
        let expires_at = lease_expiry_at(now, self.lease_duration).ok_or_else(LeaseError::lost)?;
        match self
            .repository
            .renew(&self.claim, now, self.lease_duration)
            .await
        {
            Ok(true) => {}
            Ok(false) | Err(_) => return Err(LeaseError::lost()),
        }
        let deadline = permit_deadline(now, expires_at, maximum).ok_or_else(LeaseError::lost)?;

        Ok(LeasePermit::new(self.claim.lease_token().clone(), deadline))
    }
}

fn lease_expiry_at(now: DateTime<Utc>, lease_duration: Duration) -> Option<DateTime<Utc>> {
    let lease_seconds = i64::try_from(lease_duration.as_secs()).ok()?;
    if lease_seconds == 0 {
        return None;
    }
    let expires_at = now.timestamp().checked_add(lease_seconds)?;
    DateTime::from_timestamp(expires_at, 0)
}

fn permit_deadline(
    now: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    maximum: Duration,
) -> Option<DateTime<Utc>> {
    let maximum = ChronoDuration::from_std(maximum).ok()?;
    let maximum_deadline = now.checked_add_signed(maximum)?;
    let lease_deadline = expires_at.checked_sub_signed(ChronoDuration::seconds(1))?;
    let deadline = if maximum_deadline < lease_deadline {
        maximum_deadline
    } else {
        lease_deadline
    };

    (deadline > now).then_some(deadline)
}

fn provider_retry_at(now: DateTime<Utc>, jitter: u8) -> Option<DateTime<Utc>> {
    let delay_seconds = 60_i64.checked_add(i64::from(jitter.min(MAX_RETRY_JITTER_SECONDS)))?;
    let target = now.checked_add_signed(ChronoDuration::seconds(delay_seconds))?;
    let seconds = target
        .timestamp()
        .checked_add(i64::from(target.timestamp_subsec_nanos() != 0))?;
    DateTime::from_timestamp(seconds, 0)
}
