use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use memory_store::contracts::MemoryVersionInput;
use serde_json::{Value, json};
use session_post_processing::{
    contracts::{
        AttemptState, Claim, ClaimInput, FailureCategory, LoadedSnapshot, MemoryCandidateInput,
        MemoryScope, SnapshotLoadOutcome, SourceEventType, SourceSnapshot, SourceSnapshotInput,
        StructuredGenerationRequest, StructuredGenerationResponse,
        StructuredGenerationResponseInput, TranscriptEntryInput,
    },
    coordinator::{SweepCoordinator, SweepCoordinatorLimits},
    ports::{
        CanonicalMemorySink, Clock, EnsureMemoryOutcome, MemoryPublishError, ModelProvider,
        ProviderError, SessionProcessingService, SessionRepository, StagedMemoryCandidate,
        StagedRevision,
    },
};
use tokio::sync::watch;
use uuid::Uuid;

const LIFECYCLE_RECEIPT: &str = "11111111-1111-4111-8111-111111111111";
const OBSERVATION_ONE: &str = "22222222-2222-4222-8222-222222222222";
const OBSERVATION_TWO: &str = "33333333-3333-4333-8333-333333333333";

#[tokio::test]
async fn empty_batch_claims_once_and_returns_no_outcomes() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let repository = FakeRepository::new(Vec::new(), []);
    let provider = ScriptedProvider::new([]);
    let coordinator = coordinator(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(provider.clone()),
        3,
        2,
        10_000,
        0,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("an empty claim batch should be a successful sweep");

    assert_counts(outcome, 0, 0, 0, 0, 0);
    assert!(matches!(
        repository.calls().as_slice(),
        [RepositoryCall::ClaimEligible {
            now: claimed_now,
            limit: 3,
            lease,
        }] if *claimed_now == now && *lease == Duration::from_secs(60)
    ));
    assert!(provider.calls().is_empty());
}

#[tokio::test]
async fn successful_initial_claim_publishes_staged_output_and_completes() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("initial-session", AttemptState::Claimed, 1);
    let loaded = loaded_snapshot(&claim, &[OBSERVATION_ONE], &[OBSERVATION_ONE]);
    let repository = FakeRepository::new(
        vec![claim.clone()],
        [(claim.clone(), Ok(SnapshotLoadOutcome::Current(loaded)))],
    );
    let candidate = staged_candidate(&claim, "initial");
    repository.set_unpublished_candidate_results([Ok(vec![candidate.clone()])]);
    let provider = ScriptedProvider::new([
        Ok(map_response(OBSERVATION_ONE, "memory title")),
        Ok(final_response()),
    ]);
    let memory = ScriptedMemorySink::new([Ok(EnsureMemoryOutcome::Inserted)]);
    let coordinator = coordinator_with_memory(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(provider.clone()),
        Arc::new(memory.clone()),
        coordinator_limits(3, 1, 10_000),
        0,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("a current initial snapshot should publish and promote successfully");

    assert_counts(outcome, 1, 0, 1, 0, 0);
    assert_eq!(provider.calls(), vec!["map", "reduce"]);
    assert_eq!(memory.calls(), vec![candidate.memory_id().to_owned()]);
    assert_eq!(
        repository.marked_memory_ids(),
        vec![candidate.memory_id().to_owned()]
    );
    assert_eq!(repository.promote_call_count(), 1);
    assert_eq!(repository.renew_call_count(), 8);
    assert_staged_publication_claim(&claim, &repository.publication_claims());
    let staged = repository.staged();
    assert_eq!(staged.len(), 1);
    assert_eq!(
        staged[0].generated_revision().source_revision(),
        claim.source_revision()
    );
    assert_eq!(staged[0].transcript().observation_count(), 1);
    assert_eq!(staged[0].candidates().len(), 1);
    assert_eq!(
        staged[0].candidates()[0]
            .supporting_receipt_ids()
            .iter()
            .map(|receipt_id| receipt_id.as_str())
            .collect::<Vec<_>>(),
        [OBSERVATION_ONE]
    );
}

#[tokio::test]
async fn refresh_stages_only_candidates_supported_by_the_repository_receipt_difference() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("refresh-session", AttemptState::Claimed, 2);
    let loaded = loaded_snapshot(
        &claim,
        &[OBSERVATION_ONE, OBSERVATION_TWO],
        &[OBSERVATION_TWO],
    );
    let repository = FakeRepository::new(
        vec![claim.clone()],
        [(claim.clone(), Ok(SnapshotLoadOutcome::Current(loaded)))],
    );
    let candidate = staged_candidate(&claim, "refresh");
    repository.set_unpublished_candidate_results([Ok(vec![candidate.clone()])]);
    let provider = ScriptedProvider::new([
        Ok(map_response(OBSERVATION_TWO, "new memory")),
        Ok(final_response()),
    ]);
    let memory = ScriptedMemorySink::new([Ok(EnsureMemoryOutcome::ExistingEquivalent)]);
    let coordinator = coordinator_with_memory(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(provider),
        Arc::new(memory.clone()),
        coordinator_limits(1, 1, 10_000),
        0,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("a refresh with new evidence should publish and promote");

    assert_counts(outcome, 1, 0, 1, 0, 0);
    assert_eq!(memory.calls(), vec![candidate.memory_id().to_owned()]);
    assert_eq!(repository.promote_call_count(), 1);
    let staged = repository.staged();
    assert_eq!(staged.len(), 1);
    assert_eq!(
        staged[0].candidates()[0]
            .supporting_receipt_ids()
            .iter()
            .map(|receipt_id| receipt_id.as_str())
            .collect::<Vec<_>>(),
        [OBSERVATION_TWO]
    );
}

#[tokio::test]
async fn refresh_does_not_recompute_scope_and_retries_invalid_prior_evidence() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("refresh-scope-session", AttemptState::Claimed, 2);
    let loaded = loaded_snapshot(
        &claim,
        &[OBSERVATION_ONE, OBSERVATION_TWO],
        &[OBSERVATION_TWO],
    );
    let repository = FakeRepository::new(
        vec![claim.clone()],
        [(claim.clone(), Ok(SnapshotLoadOutcome::Current(loaded)))],
    );
    repository.set_renewals([Ok(true), Ok(true), Ok(true)]);
    let provider = ScriptedProvider::new([Ok(map_response(OBSERVATION_ONE, "old memory"))]);
    let coordinator = coordinator(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(provider),
        1,
        1,
        10_000,
        4,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("invalid refresh evidence should persist a retry");

    assert_counts(outcome, 1, 0, 0, 1, 0);
    assert!(repository.staged().is_empty());
    assert_eq!(repository.renew_call_count(), 3);
    assert_retry_follows_fresh_renewal(&repository.calls());
    assert_eq!(
        repository.retry_calls(),
        vec![(
            FailureCategory::ProviderContract,
            timestamp("2026-09-23T12:01:05Z")
        )]
    );
}

#[tokio::test]
async fn zero_candidates_still_promote_the_complete_generated_revision() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("zero-candidate-session", AttemptState::Claimed, 1);
    let loaded = loaded_snapshot(&claim, &[OBSERVATION_ONE], &[OBSERVATION_ONE]);
    let repository = FakeRepository::new(
        vec![claim.clone()],
        [(claim.clone(), Ok(SnapshotLoadOutcome::Current(loaded)))],
    );
    let provider =
        ScriptedProvider::new([Ok(map_response_without_candidates()), Ok(final_response())]);
    let memory = ScriptedMemorySink::new([]);
    let coordinator = coordinator_with_memory(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(provider),
        Arc::new(memory.clone()),
        coordinator_limits(1, 1, 10_000),
        0,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("zero candidate output should promote");

    assert_counts(outcome, 1, 0, 1, 0, 0);
    assert_eq!(repository.staged()[0].candidates().len(), 0);
    assert!(memory.calls().is_empty());
    assert_eq!(repository.promote_call_count(), 1);
    assert_staged_publication_claim(&claim, &repository.publication_claims());
}

#[tokio::test]
async fn superseded_claim_is_skipped_without_generation_or_retry() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("superseded-session", AttemptState::Claimed, 1);
    let repository = FakeRepository::new(
        vec![claim.clone()],
        [(claim, Ok(SnapshotLoadOutcome::Superseded))],
    );
    let provider = ScriptedProvider::new([]);
    let coordinator = coordinator(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(provider.clone()),
        1,
        1,
        10_000,
        0,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("supersession is a terminal, safe skip");

    assert_counts(outcome, 1, 0, 0, 0, 1);
    assert!(provider.calls().is_empty());
    assert!(repository.staged().is_empty());
    assert!(repository.retry_calls().is_empty());
}

#[tokio::test]
async fn reclaimed_staged_and_publishing_claims_resume_publication_and_promote() {
    let now = timestamp("2026-09-23T12:00:00.123Z");

    for state in [AttemptState::Staged, AttemptState::Publishing] {
        let claim = claim("retained-progress-session", state, 1);
        let candidate = staged_candidate(&claim, "retained-progress");
        let repository = FakeRepository::new(vec![claim.clone()], []);
        repository.set_unpublished_candidate_results([Ok(vec![candidate.clone()])]);
        let provider = ScriptedProvider::new([]);
        let memory = ScriptedMemorySink::new([Ok(EnsureMemoryOutcome::ExistingEquivalent)]);
        let coordinator = coordinator_with_memory(
            Arc::new(FixedClock::new(now)),
            Arc::new(repository.clone()),
            Arc::new(provider.clone()),
            Arc::new(memory.clone()),
            coordinator_limits(1, 1, 10_000),
            0,
        );

        let outcome = coordinator
            .run_sweep(now)
            .await
            .expect("retained durable progress should resume");

        assert_counts(outcome, 1, 0, 1, 0, 0);
        assert!(provider.calls().is_empty());
        assert_eq!(memory.calls(), vec![candidate.memory_id().to_owned()]);
        assert_eq!(
            repository.marked_memory_ids(),
            vec![candidate.memory_id().to_owned()]
        );
        assert_eq!(repository.promote_call_count(), 1);
    }
}

#[tokio::test]
async fn publication_ensures_each_unpublished_candidate_with_a_fresh_lease_permit() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("publication-session", AttemptState::Staged, 1);
    let first = staged_candidate(&claim, "first");
    let second = staged_candidate(&claim, "second");
    let repository = FakeRepository::new(vec![claim.clone()], []);
    repository.set_unpublished_candidate_results([Ok(vec![first.clone(), second.clone()])]);
    let memory = PermitCheckingMemorySink::new(
        repository.clone(),
        [
            Ok(EnsureMemoryOutcome::Inserted),
            Ok(EnsureMemoryOutcome::ExistingEquivalent),
        ],
    );
    let coordinator = coordinator_with_memory(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(ScriptedProvider::new([])),
        Arc::new(memory.clone()),
        coordinator_limits(1, 1, 10_000),
        0,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("both canonical ensure outcomes should complete publication");

    assert_counts(outcome, 1, 0, 1, 0, 0);
    assert_eq!(
        memory.calls(),
        vec![first.memory_id().to_owned(), second.memory_id().to_owned(),]
    );
    assert_eq!(
        repository.marked_memory_ids(),
        vec![first.memory_id().to_owned(), second.memory_id().to_owned(),]
    );
    assert_eq!(repository.renew_call_count(), 6);
    assert_eq!(memory.renewal_counts_at_calls(), vec![2, 4]);
    assert_eq!(repository.promote_call_count(), 1);
}

#[tokio::test]
async fn partial_publication_resumes_only_the_unpublished_candidate() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("partial-publication-session", AttemptState::Publishing, 1);
    let published = staged_candidate(&claim, "published");
    let unpublished = staged_candidate(&claim, "unpublished");
    let repository = FakeRepository::new(vec![claim.clone()], []);
    repository.set_unpublished_candidate_results([Ok(vec![unpublished.clone()])]);
    let memory = ScriptedMemorySink::new([Ok(EnsureMemoryOutcome::ExistingEquivalent)]);
    let coordinator = coordinator_with_memory(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(ScriptedProvider::new([])),
        Arc::new(memory.clone()),
        coordinator_limits(1, 1, 10_000),
        0,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("a reclaimed publication should resume its remaining work");

    assert_counts(outcome, 1, 0, 1, 0, 0);
    assert_eq!(memory.calls(), vec![unpublished.memory_id().to_owned()]);
    assert!(!memory.calls().contains(&published.memory_id().to_owned()));
    assert_eq!(
        repository.marked_memory_ids(),
        vec![unpublished.memory_id().to_owned()]
    );
    assert_eq!(repository.promote_call_count(), 1);
}

#[tokio::test]
async fn memory_unknown_or_mismatch_retries_without_replacing_the_prior_refresh_record() {
    let now = timestamp("2026-09-23T12:00:00.123Z");

    for (suffix, error, category) in [
        (
            "unknown",
            MemoryPublishError::unknown(),
            FailureCategory::MemoryUnknown,
        ),
        (
            "mismatch",
            MemoryPublishError::mismatch(),
            FailureCategory::MemoryMismatch,
        ),
    ] {
        let claim = claim(
            &format!("failed-refresh-{suffix}"),
            AttemptState::Publishing,
            1,
        );
        let candidate = staged_candidate(&claim, suffix);
        let repository = FakeRepository::new(vec![claim.clone()], []);
        repository.set_current_record("prior-current-revision");
        repository.set_unpublished_candidate_results([Ok(vec![candidate.clone()])]);
        let memory = ScriptedMemorySink::new([Err(error)]);
        let coordinator = coordinator_with_memory(
            Arc::new(FixedClock::new(now)),
            Arc::new(repository.clone()),
            Arc::new(ScriptedProvider::new([])),
            Arc::new(memory.clone()),
            coordinator_limits(1, 1, 10_000),
            5,
        );

        let outcome = coordinator
            .run_sweep(now)
            .await
            .expect("memory publication failures should be retained for retry");

        assert_counts(outcome, 1, 0, 0, 1, 0);
        assert_eq!(memory.calls(), vec![candidate.memory_id().to_owned()]);
        assert!(repository.marked_memory_ids().is_empty());
        assert_eq!(repository.promote_call_count(), 0);
        assert_eq!(
            repository.retry_calls(),
            vec![(category, timestamp("2026-09-23T12:01:06Z"))]
        );
        assert_eq!(
            repository.current_record(),
            Some("prior-current-revision".to_owned())
        );
    }
}

#[tokio::test]
async fn lease_loss_before_memory_publication_blocks_external_calls_and_mutations() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("lost-before-memory", AttemptState::Staged, 1);
    let candidate = staged_candidate(&claim, "lost-before-memory");
    let repository = FakeRepository::new(vec![claim], []);
    repository.set_unpublished_candidate_results([Ok(vec![candidate])]);
    repository.set_renewals([Ok(true), Ok(false)]);
    let memory = ScriptedMemorySink::new([]);
    let coordinator = coordinator_with_memory(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(ScriptedProvider::new([])),
        Arc::new(memory.clone()),
        coordinator_limits(1, 1, 10_000),
        0,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("lease loss should abandon publication without later calls");

    assert_counts(outcome, 1, 1, 0, 0, 0);
    assert!(memory.calls().is_empty());
    assert!(repository.marked_memory_ids().is_empty());
    assert_eq!(repository.promote_call_count(), 0);
    assert!(repository.retry_calls().is_empty());
}

#[tokio::test]
async fn failed_fenced_marker_retries_without_reporting_completion() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("marker-failure", AttemptState::Staged, 1);
    let candidate = staged_candidate(&claim, "marker-failure");
    let repository = FakeRepository::new(vec![claim.clone()], []);
    repository.set_current_record("prior-current-revision");
    repository.set_unpublished_candidate_results([Ok(vec![candidate.clone()])]);
    repository.set_publication_marker_results([Err(
        session_post_processing::ports::RepositoryError::unconfirmed_commit(),
    )]);
    let memory = ScriptedMemorySink::new([Ok(EnsureMemoryOutcome::Inserted)]);
    let coordinator = coordinator_with_memory(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(ScriptedProvider::new([])),
        Arc::new(memory.clone()),
        coordinator_limits(1, 1, 10_000),
        0,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("an unconfirmed marker should remain recoverable");

    assert_counts(outcome, 1, 0, 0, 1, 0);
    assert_eq!(memory.calls(), vec![candidate.memory_id().to_owned()]);
    assert_eq!(
        repository.marked_memory_ids(),
        vec![candidate.memory_id().to_owned()]
    );
    assert_eq!(repository.promote_call_count(), 0);
    assert_eq!(
        repository.retry_calls(),
        vec![(
            FailureCategory::RepositoryUnconfirmedCommit,
            timestamp("2026-09-23T12:01:00Z"),
        )]
    );
    assert_eq!(
        repository.current_record(),
        Some("prior-current-revision".to_owned())
    );
}

#[tokio::test]
async fn failed_fenced_promotion_retries_without_reporting_completion() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("promotion-failure", AttemptState::Staged, 1);
    let repository = FakeRepository::new(vec![claim], []);
    repository.set_current_record("prior-current-revision");
    repository.set_promotion_results([Err(
        session_post_processing::ports::RepositoryError::unconfirmed_commit(),
    )]);
    let coordinator = coordinator_with_memory(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(ScriptedProvider::new([])),
        Arc::new(ScriptedMemorySink::new([])),
        coordinator_limits(1, 1, 10_000),
        0,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("an unconfirmed promotion should remain recoverable");

    assert_counts(outcome, 1, 0, 0, 1, 0);
    assert_eq!(repository.promote_call_count(), 1);
    assert_eq!(
        repository.retry_calls(),
        vec![(
            FailureCategory::RepositoryUnconfirmedCommit,
            timestamp("2026-09-23T12:01:00Z"),
        )]
    );
    assert_eq!(
        repository.current_record(),
        Some("prior-current-revision".to_owned())
    );
}

#[tokio::test]
async fn overlapping_sweeps_publish_one_reclaimed_attempt_without_duplicate_memories() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("overlapping-publication", AttemptState::Staged, 1);
    let candidate = staged_candidate(&claim, "overlapping-publication");
    let repository = FakeRepository::new(Vec::new(), []);
    let source_revision = claim.source_revision().as_str().to_owned();
    repository.set_claim_batches([vec![claim], Vec::new()]);
    repository.set_unpublished_candidate_results([Ok(vec![candidate.clone()])]);
    let gate = Gate::new();
    let memory = BlockingMemorySink::new(gate.clone());
    let coordinator = coordinator_with_memory(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(ScriptedProvider::new([])),
        Arc::new(memory.clone()),
        coordinator_limits(1, 1, 10_000),
        0,
    );

    let first_coordinator = coordinator.clone();
    let first = tokio::spawn(async move { first_coordinator.run_sweep(now).await });
    tokio::time::timeout(Duration::from_millis(100), gate.wait_for_entered(1))
        .await
        .expect("the first sweep should own the memory publication");

    let second = coordinator
        .run_sweep(now)
        .await
        .expect("an overlapping sweep should find no second claim");
    assert_counts(second, 0, 0, 0, 0, 0);

    gate.release();
    let first = first
        .await
        .expect("the first sweep should not panic")
        .expect("the first sweep should promote after publication");

    assert_counts(first, 1, 0, 1, 0, 0);
    assert_eq!(memory.calls(), vec![candidate.memory_id().to_owned()]);
    assert_eq!(repository.promote_call_count(), 1);
    assert_eq!(repository.current_record(), Some(source_revision));
}

#[tokio::test]
async fn expired_claim_renews_before_snapshot_load_and_skips_when_ownership_is_lost() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim_with_lease_expiry(
        "expired-before-load",
        AttemptState::Claimed,
        1,
        timestamp("2026-09-23T11:59:00Z"),
    );
    let repository = FakeRepository::new(
        vec![claim.clone()],
        [(claim, Ok(SnapshotLoadOutcome::Superseded))],
    );
    repository.set_renewals([Ok(false)]);
    let provider = ScriptedProvider::new([]);
    let coordinator = coordinator(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(provider.clone()),
        1,
        1,
        10_000,
        0,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("a lost expired claim should be skipped without mutation");

    assert_counts(outcome, 1, 0, 0, 0, 1);
    assert!(matches!(
        repository.calls().as_slice(),
        [RepositoryCall::ClaimEligible { .. }, RepositoryCall::Renew,]
    ));
    assert!(provider.calls().is_empty());
    assert!(repository.staged().is_empty());
    assert!(repository.retry_calls().is_empty());
    assert_no_task_4_5_calls(&repository.calls());
}

#[tokio::test]
async fn lease_loss_before_the_first_provider_call_skips_without_mutation() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("lost-before-provider", AttemptState::Claimed, 1);
    let loaded = loaded_snapshot(&claim, &[OBSERVATION_ONE], &[OBSERVATION_ONE]);
    let repository = FakeRepository::new(
        vec![claim.clone()],
        [(claim.clone(), Ok(SnapshotLoadOutcome::Current(loaded)))],
    );
    repository.set_renewals([Ok(true), Ok(false)]);
    let provider = ScriptedProvider::new([]);
    let coordinator = coordinator(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(provider.clone()),
        1,
        1,
        10_000,
        0,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("lease loss is represented as a skipped claim");

    assert_counts(outcome, 1, 0, 0, 0, 1);
    assert!(provider.calls().is_empty());
    assert!(repository.staged().is_empty());
    assert!(repository.retry_calls().is_empty());
}

#[tokio::test]
async fn lease_error_between_map_calls_stops_later_provider_and_retry_calls() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("lost-between-provider-calls", AttemptState::Claimed, 1);
    let loaded = loaded_snapshot_with_data(
        &claim,
        &[OBSERVATION_ONE],
        &[OBSERVATION_ONE],
        json!({"opaque": "x".repeat(8_192)}),
    );
    let repository = FakeRepository::new(
        vec![claim.clone()],
        [(claim.clone(), Ok(SnapshotLoadOutcome::Current(loaded)))],
    );
    repository.set_renewals([
        Ok(true),
        Ok(true),
        Err(session_post_processing::ports::RepositoryError::invocation()),
    ]);
    let provider = ScriptedProvider::new([Ok(map_response(OBSERVATION_ONE, "first map result"))]);
    let coordinator = coordinator(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(provider.clone()),
        1,
        1,
        512,
        0,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("lease loss between map calls should not surface as a provider error");

    assert_counts(outcome, 1, 0, 0, 0, 1);
    assert_eq!(provider.calls(), vec!["map"]);
    assert!(repository.staged().is_empty());
    assert!(repository.retry_calls().is_empty());
}

#[tokio::test]
async fn expired_permit_prevents_the_provider_call_and_retry_mutation() {
    let now = timestamp("2026-09-23T12:00:00Z");
    let claim = claim("expired-permit", AttemptState::Claimed, 1);
    let loaded = loaded_snapshot(&claim, &[OBSERVATION_ONE], &[OBSERVATION_ONE]);
    let repository = FakeRepository::new(
        vec![claim.clone()],
        [(claim.clone(), Ok(SnapshotLoadOutcome::Current(loaded)))],
    );
    let provider = ScriptedProvider::new([]);
    let clock = ScriptedClock::new([now, now, now + ChronoDuration::seconds(61)]);
    let coordinator = coordinator(
        Arc::new(clock),
        Arc::new(repository.clone()),
        Arc::new(provider.clone()),
        1,
        1,
        10_000,
        0,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("an expired permit should safely abandon the claim");

    assert_counts(outcome, 1, 0, 0, 0, 1);
    assert!(provider.calls().is_empty());
    assert!(repository.staged().is_empty());
    assert!(repository.retry_calls().is_empty());
}

#[tokio::test]
async fn lease_loss_before_stage_does_not_stage_or_persist_a_retry() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("lost-before-stage", AttemptState::Claimed, 1);
    let loaded = loaded_snapshot(&claim, &[OBSERVATION_ONE], &[OBSERVATION_ONE]);
    let repository = FakeRepository::new(
        vec![claim.clone()],
        [(claim.clone(), Ok(SnapshotLoadOutcome::Current(loaded)))],
    );
    repository.set_renewals([Ok(true), Ok(true), Ok(true), Ok(false)]);
    let provider = ScriptedProvider::new([
        Ok(map_response(OBSERVATION_ONE, "candidate")),
        Ok(final_response()),
    ]);
    let coordinator = coordinator(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(provider.clone()),
        1,
        1,
        10_000,
        0,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("a lost staging lease should not mutate the attempt");

    assert_counts(outcome, 1, 0, 0, 0, 1);
    assert_eq!(provider.calls(), vec!["map", "reduce"]);
    assert!(repository.staged().is_empty());
    assert!(repository.retry_calls().is_empty());
}

#[tokio::test]
async fn transient_provider_failure_retries_at_the_ceiling_with_injected_jitter() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("transient-provider", AttemptState::Claimed, 1);
    let loaded = loaded_snapshot(&claim, &[OBSERVATION_ONE], &[OBSERVATION_ONE]);
    let repository = FakeRepository::new(
        vec![claim.clone()],
        [(claim.clone(), Ok(SnapshotLoadOutcome::Current(loaded)))],
    );
    repository.set_renewals([Ok(true), Ok(true), Ok(true)]);
    let provider = ScriptedProvider::new([Err(ProviderError::transient())]);
    let coordinator = coordinator(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(provider),
        1,
        1,
        10_000,
        4,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("provider failure should persist a retry");

    assert_counts(outcome, 1, 0, 0, 1, 0);
    assert_eq!(repository.renew_call_count(), 3);
    assert_retry_follows_fresh_renewal(&repository.calls());
    assert_eq!(
        repository.retry_calls(),
        vec![(
            FailureCategory::ProviderTransient,
            timestamp("2026-09-23T12:01:05Z")
        )]
    );
}

#[tokio::test]
async fn provider_contract_failure_retries_at_the_ceiling_with_injected_jitter() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("contract-provider", AttemptState::Claimed, 1);
    let loaded = loaded_snapshot(&claim, &[OBSERVATION_ONE], &[OBSERVATION_ONE]);
    let repository = FakeRepository::new(
        vec![claim.clone()],
        [(claim.clone(), Ok(SnapshotLoadOutcome::Current(loaded)))],
    );
    repository.set_renewals([Ok(true), Ok(true), Ok(true)]);
    let provider = ScriptedProvider::new([Err(ProviderError::contract())]);
    let coordinator = coordinator(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(provider),
        1,
        1,
        10_000,
        4,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("provider contract failure should persist a retry");

    assert_counts(outcome, 1, 0, 0, 1, 0);
    assert_eq!(repository.renew_call_count(), 3);
    assert_retry_follows_fresh_renewal(&repository.calls());
    assert_eq!(
        repository.retry_calls(),
        vec![(
            FailureCategory::ProviderContract,
            timestamp("2026-09-23T12:01:05Z")
        )]
    );
}

#[tokio::test]
async fn candidate_normalization_failure_retries_at_the_ceiling_with_injected_jitter() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("normalization-failure", AttemptState::Claimed, 1);
    let loaded = loaded_snapshot(&claim, &[OBSERVATION_ONE], &[OBSERVATION_ONE]);
    let repository = FakeRepository::new(
        vec![claim.clone()],
        [(claim.clone(), Ok(SnapshotLoadOutcome::Current(loaded)))],
    );
    repository.set_renewals([Ok(true), Ok(true), Ok(true), Ok(true)]);
    let provider = ScriptedProvider::new([
        Ok(map_response(OBSERVATION_ONE, "   ")),
        Ok(final_response()),
    ]);
    let coordinator = coordinator(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(provider),
        1,
        1,
        10_000,
        4,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("normalization failure should persist a retry");

    assert_counts(outcome, 1, 0, 0, 1, 0);
    assert!(repository.staged().is_empty());
    assert_eq!(repository.renew_call_count(), 4);
    assert_retry_follows_fresh_renewal(&repository.calls());
    assert_eq!(
        repository.retry_calls(),
        vec![(
            FailureCategory::ProviderContract,
            timestamp("2026-09-23T12:01:05Z")
        )]
    );
}

#[tokio::test]
async fn repository_load_failure_retries_at_the_freshly_renewed_lease_expiry() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("load-repository-failure", AttemptState::Claimed, 1);
    let repository = FakeRepository::new(
        vec![claim.clone()],
        [(
            claim.clone(),
            Err(session_post_processing::ports::RepositoryError::invocation()),
        )],
    );
    repository.set_renewals([Ok(true), Ok(true)]);
    let provider = ScriptedProvider::new([]);
    let coordinator = coordinator(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(provider.clone()),
        1,
        1,
        10_000,
        0,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("repository load failure should be retained for recovery");

    assert_counts(outcome, 1, 0, 0, 1, 0);
    assert_eq!(repository.renew_call_count(), 2);
    assert_retry_follows_fresh_renewal(&repository.calls());
    assert_eq!(
        repository.retry_calls(),
        vec![(
            FailureCategory::RepositoryInvocation,
            timestamp("2026-09-23T12:01:00Z")
        )]
    );
    assert!(provider.calls().is_empty());
}

#[tokio::test]
async fn repository_stage_failure_retries_at_the_freshly_renewed_lease_expiry() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("stage-repository-failure", AttemptState::Claimed, 1);
    let loaded = loaded_snapshot(&claim, &[OBSERVATION_ONE], &[OBSERVATION_ONE]);
    let repository = FakeRepository::new(
        vec![claim.clone()],
        [(claim.clone(), Ok(SnapshotLoadOutcome::Current(loaded)))],
    );
    repository.set_renewals([Ok(true), Ok(true), Ok(true), Ok(true), Ok(true)]);
    repository.set_stage_results([Err(
        session_post_processing::ports::RepositoryError::unconfirmed_commit(),
    )]);
    let provider = ScriptedProvider::new([
        Ok(map_response(OBSERVATION_ONE, "candidate")),
        Ok(final_response()),
    ]);
    let coordinator = coordinator(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(provider),
        1,
        1,
        10_000,
        0,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("repository stage failure should be retained for recovery");

    assert_counts(outcome, 1, 0, 0, 1, 0);
    assert_eq!(
        repository.retry_calls(),
        vec![(
            FailureCategory::RepositoryUnconfirmedCommit,
            timestamp("2026-09-23T12:01:00Z")
        )]
    );
    assert_eq!(repository.renew_call_count(), 5);
    assert_retry_follows_fresh_renewal(&repository.calls());
    assert_no_task_4_5_calls(&repository.calls());
}

#[tokio::test]
async fn failed_retry_persistence_does_not_trigger_a_recursive_mutation() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claim = claim("retry-persistence-failure", AttemptState::Claimed, 1);
    let loaded = loaded_snapshot(&claim, &[OBSERVATION_ONE], &[OBSERVATION_ONE]);
    let repository = FakeRepository::new(
        vec![claim.clone()],
        [(claim.clone(), Ok(SnapshotLoadOutcome::Current(loaded)))],
    );
    repository.set_renewals([Ok(true), Ok(true), Ok(true)]);
    repository.set_retry_results([Err(
        session_post_processing::ports::RepositoryError::invocation(),
    )]);
    let provider = ScriptedProvider::new([Err(ProviderError::transient())]);
    let coordinator = coordinator(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(provider),
        1,
        1,
        10_000,
        4,
    );

    let outcome = coordinator
        .run_sweep(now)
        .await
        .expect("a failed retry write remains recoverable after lease expiry");

    assert_counts(outcome, 1, 0, 0, 0, 1);
    assert_eq!(repository.retry_calls().len(), 1);
    assert_eq!(repository.renew_call_count(), 3);
    assert_retry_follows_fresh_renewal(&repository.calls());
    assert!(repository.staged().is_empty());
}

#[tokio::test]
async fn configured_batch_and_concurrency_bound_active_claims() {
    let now = timestamp("2026-09-23T12:00:00.123Z");
    let claims = [
        claim("concurrency-one", AttemptState::Claimed, 1),
        claim("concurrency-two", AttemptState::Claimed, 1),
        claim("concurrency-three", AttemptState::Claimed, 1),
    ];
    let snapshots = claims
        .iter()
        .cloned()
        .map(|claim| {
            let loaded = loaded_snapshot(&claim, &[OBSERVATION_ONE], &[OBSERVATION_ONE]);
            (claim, Ok(SnapshotLoadOutcome::Current(loaded)))
        })
        .collect::<Vec<_>>();
    let repository = FakeRepository::new(claims.to_vec(), snapshots);
    let gate = Gate::new();
    let provider = BlockingProvider::new(gate.clone());
    let coordinator = coordinator(
        Arc::new(FixedClock::new(now)),
        Arc::new(repository.clone()),
        Arc::new(provider.clone()),
        3,
        2,
        10_000,
        0,
    );

    let sweep = tokio::spawn(async move { coordinator.run_sweep(now).await });
    tokio::time::timeout(Duration::from_secs(1), gate.wait_for_entered(2))
        .await
        .expect("two configured concurrent claims should reach their map calls");
    tokio::task::yield_now().await;
    assert_eq!(gate.entered(), 2, "a third claim must not be admitted yet");
    assert_eq!(provider.map_calls(), 2);
    assert!(matches!(
        repository.calls().first(),
        Some(RepositoryCall::ClaimEligible { limit: 3, .. })
    ));

    gate.release();
    let outcome = sweep
        .await
        .expect("coordinator task should not panic")
        .expect("all claims should promote after the gate opens");

    assert_counts(outcome, 3, 0, 3, 0, 0);
    assert_eq!(provider.map_calls(), 3);
    assert_eq!(repository.promote_call_count(), 3);
}

#[derive(Clone, Debug)]
enum RepositoryCall {
    ClaimEligible {
        now: DateTime<Utc>,
        limit: u32,
        lease: Duration,
    },
    Renew,
    LoadSnapshot,
    Stage {
        output: StagedRevision,
    },
    LoadUnpublishedCandidates {
        claim: Claim,
    },
    MarkMemoryPublished {
        memory_id: String,
    },
    Promote,
    Retry {
        failure: FailureCategory,
        next_at: DateTime<Utc>,
    },
}

#[derive(Clone)]
struct FakeRepository {
    state: Arc<Mutex<FakeRepositoryState>>,
}

struct FakeRepositoryState {
    claims: Vec<Claim>,
    claim_batches: VecDeque<Vec<Claim>>,
    snapshots: BTreeMap<
        String,
        Result<SnapshotLoadOutcome, session_post_processing::ports::RepositoryError>,
    >,
    renewals: VecDeque<Result<bool, session_post_processing::ports::RepositoryError>>,
    stages: VecDeque<Result<(), session_post_processing::ports::RepositoryError>>,
    unpublished_candidates: VecDeque<
        Result<Vec<StagedMemoryCandidate>, session_post_processing::ports::RepositoryError>,
    >,
    publication_markers: VecDeque<Result<(), session_post_processing::ports::RepositoryError>>,
    promotions: VecDeque<Result<(), session_post_processing::ports::RepositoryError>>,
    retries: VecDeque<Result<(), session_post_processing::ports::RepositoryError>>,
    current_record: Option<String>,
    calls: Vec<RepositoryCall>,
}

impl FakeRepository {
    fn new(
        claims: Vec<Claim>,
        snapshots: impl IntoIterator<
            Item = (
                Claim,
                Result<SnapshotLoadOutcome, session_post_processing::ports::RepositoryError>,
            ),
        >,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeRepositoryState {
                claims,
                claim_batches: VecDeque::new(),
                snapshots: snapshots
                    .into_iter()
                    .map(|(claim, outcome)| (claim.attempt_id().as_str().to_owned(), outcome))
                    .collect(),
                renewals: VecDeque::new(),
                stages: VecDeque::new(),
                unpublished_candidates: VecDeque::new(),
                publication_markers: VecDeque::new(),
                promotions: VecDeque::new(),
                retries: VecDeque::new(),
                current_record: None,
                calls: Vec::new(),
            })),
        }
    }

    fn set_renewals(
        &self,
        outcomes: impl IntoIterator<
            Item = Result<bool, session_post_processing::ports::RepositoryError>,
        >,
    ) {
        self.state
            .lock()
            .expect("repository fake lock should not be poisoned")
            .renewals = outcomes.into_iter().collect();
    }

    fn set_stage_results(
        &self,
        outcomes: impl IntoIterator<Item = Result<(), session_post_processing::ports::RepositoryError>>,
    ) {
        self.state
            .lock()
            .expect("repository fake lock should not be poisoned")
            .stages = outcomes.into_iter().collect();
    }

    fn set_claim_batches(&self, batches: impl IntoIterator<Item = Vec<Claim>>) {
        self.state
            .lock()
            .expect("repository fake lock should not be poisoned")
            .claim_batches = batches.into_iter().collect();
    }

    fn set_unpublished_candidate_results(
        &self,
        outcomes: impl IntoIterator<
            Item = Result<
                Vec<StagedMemoryCandidate>,
                session_post_processing::ports::RepositoryError,
            >,
        >,
    ) {
        self.state
            .lock()
            .expect("repository fake lock should not be poisoned")
            .unpublished_candidates = outcomes.into_iter().collect();
    }

    fn set_publication_marker_results(
        &self,
        outcomes: impl IntoIterator<Item = Result<(), session_post_processing::ports::RepositoryError>>,
    ) {
        self.state
            .lock()
            .expect("repository fake lock should not be poisoned")
            .publication_markers = outcomes.into_iter().collect();
    }

    fn set_promotion_results(
        &self,
        outcomes: impl IntoIterator<Item = Result<(), session_post_processing::ports::RepositoryError>>,
    ) {
        self.state
            .lock()
            .expect("repository fake lock should not be poisoned")
            .promotions = outcomes.into_iter().collect();
    }

    fn set_current_record(&self, source_revision: &str) {
        self.state
            .lock()
            .expect("repository fake lock should not be poisoned")
            .current_record = Some(source_revision.to_owned());
    }

    fn set_retry_results(
        &self,
        outcomes: impl IntoIterator<Item = Result<(), session_post_processing::ports::RepositoryError>>,
    ) {
        self.state
            .lock()
            .expect("repository fake lock should not be poisoned")
            .retries = outcomes.into_iter().collect();
    }

    fn calls(&self) -> Vec<RepositoryCall> {
        self.state
            .lock()
            .expect("repository fake lock should not be poisoned")
            .calls
            .clone()
    }

    fn staged(&self) -> Vec<StagedRevision> {
        self.calls()
            .into_iter()
            .filter_map(|call| match call {
                RepositoryCall::Stage { output, .. } => Some(output),
                _ => None,
            })
            .collect()
    }

    fn renew_call_count(&self) -> usize {
        self.calls()
            .into_iter()
            .filter(|call| matches!(call, RepositoryCall::Renew))
            .count()
    }

    fn retry_calls(&self) -> Vec<(FailureCategory, DateTime<Utc>)> {
        self.calls()
            .into_iter()
            .filter_map(|call| match call {
                RepositoryCall::Retry {
                    failure, next_at, ..
                } => Some((failure, next_at)),
                _ => None,
            })
            .collect()
    }

    fn marked_memory_ids(&self) -> Vec<String> {
        self.calls()
            .into_iter()
            .filter_map(|call| match call {
                RepositoryCall::MarkMemoryPublished { memory_id } => Some(memory_id),
                _ => None,
            })
            .collect()
    }

    fn promote_call_count(&self) -> usize {
        self.calls()
            .into_iter()
            .filter(|call| matches!(call, RepositoryCall::Promote))
            .count()
    }

    fn publication_claims(&self) -> Vec<Claim> {
        self.calls()
            .into_iter()
            .filter_map(|call| match call {
                RepositoryCall::LoadUnpublishedCandidates { claim } => Some(claim),
                _ => None,
            })
            .collect()
    }

    fn current_record(&self) -> Option<String> {
        self.state
            .lock()
            .expect("repository fake lock should not be poisoned")
            .current_record
            .clone()
    }
}

#[async_trait]
impl SessionRepository for FakeRepository {
    async fn claim_eligible(
        &self,
        now: DateTime<Utc>,
        limit: u32,
        lease: Duration,
    ) -> Result<Vec<Claim>, session_post_processing::ports::RepositoryError> {
        let mut state = self
            .state
            .lock()
            .expect("repository fake lock should not be poisoned");
        state
            .calls
            .push(RepositoryCall::ClaimEligible { now, limit, lease });
        Ok(state
            .claim_batches
            .pop_front()
            .unwrap_or_else(|| state.claims.clone()))
    }

    async fn renew(
        &self,
        _claim: &Claim,
        _now: DateTime<Utc>,
        _lease: Duration,
    ) -> Result<bool, session_post_processing::ports::RepositoryError> {
        let mut state = self
            .state
            .lock()
            .expect("repository fake lock should not be poisoned");
        state.calls.push(RepositoryCall::Renew);
        state.renewals.pop_front().unwrap_or(Ok(true))
    }

    async fn load_snapshot(
        &self,
        claim: &Claim,
    ) -> Result<SnapshotLoadOutcome, session_post_processing::ports::RepositoryError> {
        let mut state = self
            .state
            .lock()
            .expect("repository fake lock should not be poisoned");
        state.calls.push(RepositoryCall::LoadSnapshot);
        state
            .snapshots
            .get(claim.attempt_id().as_str())
            .cloned()
            .unwrap_or_else(|| {
                Err(session_post_processing::ports::RepositoryError::invalid_response())
            })
    }

    async fn stage(
        &self,
        _claim: &Claim,
        output: StagedRevision,
    ) -> Result<(), session_post_processing::ports::RepositoryError> {
        let mut state = self
            .state
            .lock()
            .expect("repository fake lock should not be poisoned");
        state.calls.push(RepositoryCall::Stage { output });
        state.stages.pop_front().unwrap_or(Ok(()))
    }

    async fn load_unpublished_candidates(
        &self,
        claim: &Claim,
    ) -> Result<Vec<StagedMemoryCandidate>, session_post_processing::ports::RepositoryError> {
        let mut state = self
            .state
            .lock()
            .expect("repository fake lock should not be poisoned");
        state.calls.push(RepositoryCall::LoadUnpublishedCandidates {
            claim: claim.clone(),
        });
        if !matches!(
            claim.state(),
            AttemptState::Staged | AttemptState::Publishing
        ) {
            return Err(session_post_processing::ports::RepositoryError::invalid_response());
        }
        state
            .unpublished_candidates
            .pop_front()
            .unwrap_or(Ok(Vec::new()))
    }

    async fn mark_memory_published(
        &self,
        _claim: &Claim,
        memory_id: &str,
    ) -> Result<(), session_post_processing::ports::RepositoryError> {
        let mut state = self
            .state
            .lock()
            .expect("repository fake lock should not be poisoned");
        state.calls.push(RepositoryCall::MarkMemoryPublished {
            memory_id: memory_id.to_owned(),
        });
        state.publication_markers.pop_front().unwrap_or(Ok(()))
    }

    async fn promote(
        &self,
        claim: &Claim,
    ) -> Result<(), session_post_processing::ports::RepositoryError> {
        let mut state = self
            .state
            .lock()
            .expect("repository fake lock should not be poisoned");
        state.calls.push(RepositoryCall::Promote);
        let result = state.promotions.pop_front().unwrap_or(Ok(()));
        if result.is_ok() {
            state.current_record = Some(claim.source_revision().as_str().to_owned());
        }
        result
    }

    async fn retry(
        &self,
        _claim: &Claim,
        failure: FailureCategory,
        next_at: DateTime<Utc>,
    ) -> Result<(), session_post_processing::ports::RepositoryError> {
        let mut state = self
            .state
            .lock()
            .expect("repository fake lock should not be poisoned");
        state.calls.push(RepositoryCall::Retry { failure, next_at });
        state.retries.pop_front().unwrap_or(Ok(()))
    }
}

#[derive(Clone)]
struct ScriptedProvider {
    state: Arc<Mutex<ScriptedProviderState>>,
}

struct ScriptedProviderState {
    outcomes: VecDeque<Result<StructuredGenerationResponse, ProviderError>>,
    calls: Vec<&'static str>,
}

impl ScriptedProvider {
    fn new(
        outcomes: impl IntoIterator<Item = Result<StructuredGenerationResponse, ProviderError>>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(ScriptedProviderState {
                outcomes: outcomes.into_iter().collect(),
                calls: Vec::new(),
            })),
        }
    }

    fn calls(&self) -> Vec<&'static str> {
        self.state
            .lock()
            .expect("provider fake lock should not be poisoned")
            .calls
            .clone()
    }
}

#[async_trait]
impl ModelProvider for ScriptedProvider {
    async fn generate(
        &self,
        request: StructuredGenerationRequest,
    ) -> Result<StructuredGenerationResponse, ProviderError> {
        let mut state = self
            .state
            .lock()
            .expect("provider fake lock should not be poisoned");
        state.calls.push(match request.operation() {
            session_post_processing::contracts::ProviderOperation::Map => "map",
            session_post_processing::contracts::ProviderOperation::Reduce => "reduce",
        });
        state
            .outcomes
            .pop_front()
            .expect("provider fake received an unexpected request")
    }
}

#[derive(Clone)]
struct ScriptedMemorySink {
    state: Arc<Mutex<ScriptedMemorySinkState>>,
}

struct ScriptedMemorySinkState {
    outcomes: VecDeque<Result<EnsureMemoryOutcome, MemoryPublishError>>,
    calls: Vec<String>,
}

impl ScriptedMemorySink {
    fn new(
        outcomes: impl IntoIterator<Item = Result<EnsureMemoryOutcome, MemoryPublishError>>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(ScriptedMemorySinkState {
                outcomes: outcomes.into_iter().collect(),
                calls: Vec::new(),
            })),
        }
    }

    fn calls(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("memory sink lock should not be poisoned")
            .calls
            .clone()
    }
}

#[async_trait]
impl CanonicalMemorySink for ScriptedMemorySink {
    async fn ensure_memory(
        &self,
        memory: MemoryVersionInput,
    ) -> Result<EnsureMemoryOutcome, MemoryPublishError> {
        let mut state = self
            .state
            .lock()
            .expect("memory sink lock should not be poisoned");
        state.calls.push(memory.id);
        state
            .outcomes
            .pop_front()
            .expect("memory sink received an unexpected request")
    }
}

#[derive(Clone)]
struct PermitCheckingMemorySink {
    repository: FakeRepository,
    state: Arc<Mutex<PermitCheckingMemorySinkState>>,
}

struct PermitCheckingMemorySinkState {
    outcomes: VecDeque<Result<EnsureMemoryOutcome, MemoryPublishError>>,
    calls: Vec<String>,
    renewal_counts_at_calls: Vec<usize>,
}

impl PermitCheckingMemorySink {
    fn new(
        repository: FakeRepository,
        outcomes: impl IntoIterator<Item = Result<EnsureMemoryOutcome, MemoryPublishError>>,
    ) -> Self {
        Self {
            repository,
            state: Arc::new(Mutex::new(PermitCheckingMemorySinkState {
                outcomes: outcomes.into_iter().collect(),
                calls: Vec::new(),
                renewal_counts_at_calls: Vec::new(),
            })),
        }
    }

    fn calls(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("memory sink lock should not be poisoned")
            .calls
            .clone()
    }

    fn renewal_counts_at_calls(&self) -> Vec<usize> {
        self.state
            .lock()
            .expect("memory sink lock should not be poisoned")
            .renewal_counts_at_calls
            .clone()
    }
}

#[async_trait]
impl CanonicalMemorySink for PermitCheckingMemorySink {
    async fn ensure_memory(
        &self,
        memory: MemoryVersionInput,
    ) -> Result<EnsureMemoryOutcome, MemoryPublishError> {
        let renewal_count = self.repository.renew_call_count();
        let mut state = self
            .state
            .lock()
            .expect("memory sink lock should not be poisoned");
        state.calls.push(memory.id);
        state.renewal_counts_at_calls.push(renewal_count);
        state
            .outcomes
            .pop_front()
            .expect("memory sink received an unexpected request")
    }
}

#[derive(Clone)]
struct BlockingMemorySink {
    gate: Gate,
    calls: Arc<Mutex<Vec<String>>>,
}

impl BlockingMemorySink {
    fn new(gate: Gate) -> Self {
        Self {
            gate,
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls
            .lock()
            .expect("memory sink lock should not be poisoned")
            .clone()
    }
}

#[async_trait]
impl CanonicalMemorySink for BlockingMemorySink {
    async fn ensure_memory(
        &self,
        memory: MemoryVersionInput,
    ) -> Result<EnsureMemoryOutcome, MemoryPublishError> {
        self.calls
            .lock()
            .expect("memory sink lock should not be poisoned")
            .push(memory.id);
        self.gate.enter().await;
        Ok(EnsureMemoryOutcome::Inserted)
    }
}

#[derive(Clone)]
struct BlockingProvider {
    gate: Gate,
    calls: Arc<Mutex<Vec<&'static str>>>,
}

impl BlockingProvider {
    fn new(gate: Gate) -> Self {
        Self {
            gate,
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn map_calls(&self) -> usize {
        self.calls
            .lock()
            .expect("provider fake lock should not be poisoned")
            .iter()
            .filter(|operation| **operation == "map")
            .count()
    }
}

#[async_trait]
impl ModelProvider for BlockingProvider {
    async fn generate(
        &self,
        request: StructuredGenerationRequest,
    ) -> Result<StructuredGenerationResponse, ProviderError> {
        let operation = match request.operation() {
            session_post_processing::contracts::ProviderOperation::Map => "map",
            session_post_processing::contracts::ProviderOperation::Reduce => "reduce",
        };
        self.calls
            .lock()
            .expect("provider fake lock should not be poisoned")
            .push(operation);
        if operation == "map" {
            self.gate.enter().await;
            Ok(map_response(OBSERVATION_ONE, "candidate"))
        } else {
            Ok(final_response())
        }
    }
}

#[derive(Clone)]
struct Gate {
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

impl Gate {
    fn new() -> Self {
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

    async fn enter(&self) {
        let mut changed = self.changed.subscribe();
        let released = {
            let mut state = self.state.lock().expect("gate lock should not be poisoned");
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
                .expect("gate sender should remain alive");
        }
    }

    async fn wait_for_entered(&self, expected: usize) {
        let mut changed = self.changed.subscribe();
        loop {
            if changed.borrow_and_update().entered >= expected {
                return;
            }
            changed
                .changed()
                .await
                .expect("gate sender should remain alive");
        }
    }

    fn entered(&self) -> usize {
        self.state
            .lock()
            .expect("gate lock should not be poisoned")
            .entered
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("gate lock should not be poisoned");
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

#[derive(Clone)]
struct FixedClock {
    now: DateTime<Utc>,
}

impl FixedClock {
    fn new(now: DateTime<Utc>) -> Self {
        Self { now }
    }
}

impl Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.now
    }
}

struct ScriptedClock {
    values: Mutex<VecDeque<DateTime<Utc>>>,
    fallback: DateTime<Utc>,
}

impl ScriptedClock {
    fn new(values: impl IntoIterator<Item = DateTime<Utc>>) -> Self {
        let values = values.into_iter().collect::<VecDeque<_>>();
        let fallback = *values
            .back()
            .expect("scripted clock requires at least one instant");
        Self {
            values: Mutex::new(values),
            fallback,
        }
    }
}

impl Clock for ScriptedClock {
    fn now(&self) -> DateTime<Utc> {
        self.values
            .lock()
            .expect("clock fake lock should not be poisoned")
            .pop_front()
            .unwrap_or(self.fallback)
    }
}

fn coordinator(
    clock: Arc<dyn Clock>,
    repository: Arc<dyn SessionRepository>,
    provider: Arc<dyn ModelProvider>,
    batch_limit: u32,
    concurrency: u32,
    model_chunk_bytes: usize,
    jitter: u8,
) -> SweepCoordinator {
    coordinator_with_memory(
        clock,
        repository,
        provider,
        Arc::new(ScriptedMemorySink::new([])),
        coordinator_limits(batch_limit, concurrency, model_chunk_bytes),
        jitter,
    )
}

fn coordinator_limits(
    batch_limit: u32,
    concurrency: u32,
    model_chunk_bytes: usize,
) -> SweepCoordinatorLimits {
    SweepCoordinatorLimits {
        batch_limit,
        concurrency,
        lease_duration: Duration::from_secs(60),
        model_timeout: Duration::from_secs(10),
        model_chunk_bytes,
    }
}

fn coordinator_with_memory(
    clock: Arc<dyn Clock>,
    repository: Arc<dyn SessionRepository>,
    provider: Arc<dyn ModelProvider>,
    memory_sink: Arc<dyn CanonicalMemorySink>,
    limits: SweepCoordinatorLimits,
    jitter: u8,
) -> SweepCoordinator {
    SweepCoordinator::with_jitter(
        clock,
        repository,
        provider,
        memory_sink,
        limits,
        move || jitter,
    )
}

fn claim(session_id: &str, state: AttemptState, observation_count: u64) -> Claim {
    claim_with_lease_expiry(
        session_id,
        state,
        observation_count,
        timestamp("2026-09-23T12:00:30Z"),
    )
}

fn claim_with_lease_expiry(
    session_id: &str,
    state: AttemptState,
    observation_count: u64,
    lease_expires_at: DateTime<Utc>,
) -> Claim {
    Claim::try_from(ClaimInput {
        attempt_id: Uuid::now_v7().to_string(),
        session_id: session_id.to_owned(),
        source_revision: Uuid::new_v5(&Uuid::NAMESPACE_URL, session_id.as_bytes()).to_string(),
        lifecycle_count: 1,
        observation_count,
        state,
        lease_token: Uuid::now_v7().to_string(),
        lease_expires_at,
    })
    .expect("claim fixture should be valid")
}

fn loaded_snapshot(
    claim: &Claim,
    observation_receipts: &[&str],
    memory_scope: &[&str],
) -> LoadedSnapshot {
    loaded_snapshot_with_data(
        claim,
        observation_receipts,
        memory_scope,
        json!({"opaque": "source-observation-secret-sentinel"}),
    )
}

fn loaded_snapshot_with_data(
    claim: &Claim,
    observation_receipts: &[&str],
    memory_scope: &[&str],
    observation_data: Value,
) -> LoadedSnapshot {
    assert_eq!(
        claim.observation_count(),
        observation_receipts.len() as u64,
        "test snapshot must exactly match the claim"
    );
    let mut entries = vec![TranscriptEntryInput::Lifecycle {
        receipt_id: LIFECYCLE_RECEIPT.to_owned(),
        event_type: SourceEventType::SessionStart,
        session_id: claim.session_id().as_str().to_owned(),
        project_name: "project".to_owned(),
        current_working_directory: "/work".to_owned(),
        source_timestamp_rfc3339: "2026-09-22T10:00:00.000Z".to_owned(),
        source_timestamp_utc: timestamp("2026-09-22T10:00:00Z"),
        ingested_at: timestamp("2026-09-22T10:01:00Z"),
    }];
    for (index, receipt_id) in observation_receipts.iter().enumerate() {
        entries.push(TranscriptEntryInput::Observation {
            receipt_id: (*receipt_id).to_owned(),
            event_type: SourceEventType::Observation,
            session_id: claim.session_id().as_str().to_owned(),
            hook_type: "post_tool_use".to_owned(),
            project_name: "project".to_owned(),
            current_working_directory: "/work".to_owned(),
            source_timestamp_rfc3339: format!("2026-09-22T10:0{}:00.000Z", index + 2),
            source_timestamp_utc: timestamp(&format!("2026-09-22T10:0{}:00Z", index + 2)),
            ingested_at: timestamp(&format!("2026-09-22T10:1{}:00Z", index + 2)),
            data: observation_data.clone(),
        });
    }
    let snapshot = SourceSnapshot::try_from(SourceSnapshotInput {
        session_id: claim.session_id().as_str().to_owned(),
        source_revision: claim.source_revision().as_str().to_owned(),
        source_cutoff: timestamp("2026-09-22T11:00:00Z"),
        entries,
    })
    .expect("snapshot fixture should be valid");
    let memory_scope = MemoryScope::try_from(
        memory_scope
            .iter()
            .map(|receipt_id| (*receipt_id).to_owned())
            .collect::<Vec<_>>(),
    )
    .expect("memory scope fixture should be valid");

    LoadedSnapshot::try_new(snapshot, memory_scope)
        .expect("loaded snapshot fixture should be valid")
}

fn map_response(receipt_id: &str, title: &str) -> StructuredGenerationResponse {
    StructuredGenerationResponse::try_from(StructuredGenerationResponseInput {
        summary_sentences: vec!["Map sentence.".to_owned()],
        concepts: vec!["map concept".to_owned()],
        memory_candidates: vec![MemoryCandidateInput {
            title: title.to_owned(),
            content: "durable content".to_owned(),
            concepts: vec!["candidate concept".to_owned()],
            supporting_receipt_ids: vec![receipt_id.to_owned()],
        }],
    })
    .expect("map response fixture should be valid")
}

fn map_response_without_candidates() -> StructuredGenerationResponse {
    StructuredGenerationResponse::try_from(StructuredGenerationResponseInput {
        summary_sentences: vec!["Map sentence.".to_owned()],
        concepts: vec!["map concept".to_owned()],
        memory_candidates: Vec::new(),
    })
    .expect("zero-candidate map response fixture should be valid")
}

fn final_response() -> StructuredGenerationResponse {
    StructuredGenerationResponse::try_from(StructuredGenerationResponseInput {
        summary_sentences: vec!["Final sentence.".to_owned()],
        concepts: (1..=10).map(|number| format!("concept {number}")).collect(),
        memory_candidates: Vec::new(),
    })
    .expect("final reduction response fixture should be valid")
}

fn staged_candidate(claim: &Claim, suffix: &str) -> StagedMemoryCandidate {
    let memory_id = Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("session-post-processing-coordinator-{suffix}").as_bytes(),
    )
    .to_string();
    StagedMemoryCandidate::try_new(
        format!("fingerprint-{suffix}"),
        MemoryVersionInput {
            id: memory_id,
            version: 1,
            memory_type: "session-derived".to_owned(),
            title: format!("memory-title-{suffix}"),
            content: format!("memory-content-{suffix}"),
            created_at: timestamp("2026-09-23T12:00:00Z"),
            updated_at: timestamp("2026-09-23T12:00:00Z"),
            concepts: vec![format!("memory-concept-{suffix}")],
            files: Vec::new(),
            session_ids: vec![claim.session_id().as_str().to_owned()],
            source_observation_ids: vec![OBSERVATION_ONE.to_owned()],
        },
        vec![OBSERVATION_ONE.to_owned()],
    )
    .expect("staged candidate fixture should be valid")
}

fn assert_staged_publication_claim(original: &Claim, publication_claims: &[Claim]) {
    assert_eq!(publication_claims.len(), 1);
    let publication_claim = &publication_claims[0];
    assert_eq!(publication_claim.state(), AttemptState::Staged);
    assert_eq!(publication_claim.attempt_id(), original.attempt_id());
    assert_eq!(publication_claim.session_id(), original.session_id());
    assert_eq!(
        publication_claim.source_revision(),
        original.source_revision()
    );
    assert_eq!(
        publication_claim.lifecycle_count(),
        original.lifecycle_count()
    );
    assert_eq!(
        publication_claim.observation_count(),
        original.observation_count()
    );
    assert_eq!(publication_claim.lease_token(), original.lease_token());
    assert_eq!(
        publication_claim.lease_expires_at(),
        original.lease_expires_at()
    );
}

fn assert_counts(
    outcome: session_post_processing::contracts::SweepOutcome,
    attempted: u32,
    staged: u32,
    completed: u32,
    retryable: u32,
    skipped: u32,
) {
    assert_eq!(outcome.attempted(), attempted);
    assert_eq!(outcome.staged(), staged);
    assert_eq!(outcome.completed(), completed);
    assert_eq!(outcome.retryable(), retryable);
    assert_eq!(outcome.skipped(), skipped);
}

fn assert_no_task_4_5_calls(calls: &[RepositoryCall]) {
    assert!(
        calls.iter().all(|call| {
            !matches!(
                call,
                RepositoryCall::LoadUnpublishedCandidates { .. }
                    | RepositoryCall::MarkMemoryPublished { .. }
                    | RepositoryCall::Promote
            )
        }),
        "task 4.4 must not call task 4.5 publication or promotion APIs"
    );
}

fn assert_retry_follows_fresh_renewal(calls: &[RepositoryCall]) {
    let retry_index = calls
        .iter()
        .position(|call| matches!(call, RepositoryCall::Retry { .. }))
        .expect("the retry fixture should persist exactly one retry");
    assert!(
        matches!(
            calls.get(retry_index.checked_sub(1).expect("retry cannot be first")),
            Some(RepositoryCall::Renew)
        ),
        "retry persistence must be preceded by a fresh lease renewal"
    );
}

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("timestamp fixture should be valid")
        .with_timezone(&Utc)
}
