mod support;

use std::{
    fmt::{Debug, Display},
    time::Duration,
};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use memory_store::contracts::MemoryVersionInput;
use serde::Serialize;
use serde_json::json;
use session_post_processing::{
    contracts::{
        Claim, ClaimInput, ContractError, FailureCategory, FailureStage, GeneratedRevision,
        GeneratedRevisionInput, LeaseToken, LoadedSnapshot, MemoryCandidateInput, MemoryScope,
        ProviderOperation, Retryability, SnapshotLoadOutcome, SourceEventType, SourceSnapshot,
        SourceSnapshotInput, StructuredGenerationRequest, StructuredGenerationRequestInput,
        StructuredGenerationResponse, StructuredGenerationResponseInput, TranscriptEntryInput,
    },
    ports::{
        CanonicalMemorySink, Clock, EnsureMemoryOutcome, ExternalCallLease, LeaseError,
        LeasePermit, MemoryPublishError, ModelProvider, ProviderError, RepositoryError,
        SessionRepository, StagedMemoryCandidate, StagedRevision,
    },
};
use support::ports::{
    DeterministicGate, FailingMemorySink, FailingRepository, ManualClock, RecordingMemorySink,
    RecordingRepository, RepositoryCall, RepositoryResponses, ScriptedLease, ScriptedModelProvider,
};

const ATTEMPT_ID: &str = "018f5d00-0000-7000-8000-000000000001";
const SOURCE_REVISION: &str = "22222222-2222-5222-8222-222222222222";
const LEASE_TOKEN: &str = "018f5d00-0000-7000-8000-000000000002";
const LIFECYCLE_RECEIPT: &str = "44444444-4444-4444-8444-444444444444";
const OBSERVATION_RECEIPT: &str = "55555555-5555-4555-8555-555555555555";
const SESSION_ID: &str = "session-1";
const MEMORY_ID: &str = "33333333-3333-5333-8333-333333333333";

#[test]
fn manual_clock_controls_time_without_wall_clock() {
    let start = timestamp("2026-09-18T12:00:00Z");
    let clock = ManualClock::new(start);

    assert_eq!(read_clock(&clock), start);
    clock.advance(ChronoDuration::minutes(5));
    assert_eq!(clock.now(), timestamp("2026-09-18T12:05:00Z"));
    clock.set(timestamp("2026-09-19T00:00:00Z"));
    assert_eq!(clock.now(), timestamp("2026-09-19T00:00:00Z"));
}

#[tokio::test]
async fn recording_and_failing_repositories_record_deterministic_calls() {
    let claim = claim();
    let loaded_snapshot = loaded_snapshot();
    let now = timestamp("2026-09-18T16:00:00Z");
    let retry_at = timestamp("2026-09-18T16:05:00Z");
    let repository = RecordingRepository::new(RepositoryResponses {
        claims: vec![claim.clone()],
        renewed: false,
        snapshot: SnapshotLoadOutcome::Current(loaded_snapshot.clone()),
        unpublished_candidates: vec![staged_candidate()],
    });

    assert_eq!(
        repository
            .claim_eligible(now, 3, Duration::from_secs(60))
            .await
            .expect("recording repository should return configured claims"),
        vec![claim.clone()]
    );
    assert!(
        !repository
            .renew(&claim, now, Duration::from_secs(90))
            .await
            .expect("recording repository should return configured renewal result")
    );
    assert_eq!(
        repository
            .load_snapshot(&claim)
            .await
            .expect("recording repository should return configured snapshot"),
        SnapshotLoadOutcome::Current(loaded_snapshot)
    );
    repository
        .stage(&claim, staged_revision())
        .await
        .expect("recording repository should stage");
    assert_eq!(
        repository
            .load_unpublished_candidates(&claim)
            .await
            .expect("recording repository should return unpublished candidates")
            .len(),
        1
    );
    repository
        .mark_memory_published(&claim, MEMORY_ID)
        .await
        .expect("recording repository should mark publication");
    repository
        .promote(&claim)
        .await
        .expect("recording repository should promote");
    repository
        .retry(&claim, FailureCategory::ProviderTransient, retry_at)
        .await
        .expect("recording repository should persist retry");

    let calls = repository.calls();
    assert_eq!(calls.len(), 8);
    assert!(matches!(
        calls[0],
        RepositoryCall::ClaimEligible {
            now: recorded_now,
            limit: 3,
            lease,
        } if recorded_now == now && lease == Duration::from_secs(60)
    ));
    assert!(matches!(
        calls[3],
        RepositoryCall::Stage { ref output, .. }
            if output.candidates().len() == 1
    ));
    assert!(matches!(
        calls[4],
        RepositoryCall::LoadUnpublishedCandidates { claim: ref recorded_claim }
            if recorded_claim == &claim
    ));
    assert!(matches!(
        calls[5],
        RepositoryCall::MarkMemoryPublished { ref memory_id, .. }
            if memory_id == MEMORY_ID
    ));
    assert!(matches!(
        calls[7],
        RepositoryCall::Retry {
            failure: FailureCategory::ProviderTransient,
            next_at,
            ..
        } if next_at == retry_at
    ));

    let failing = FailingRepository::new(RepositoryError::invocation());
    let error = failing
        .claim_eligible(now, 1, Duration::from_secs(10))
        .await
        .expect_err("failing repository should return its configured failure");
    assert_eq!(error, RepositoryError::invocation());
    assert_eq!(failing.calls().len(), 1);
}

#[tokio::test]
async fn scripted_provider_and_lease_preserve_ordered_retry_outcomes() {
    let first_response = provider_response("first");
    let provider =
        ScriptedModelProvider::new([Ok(first_response.clone()), Err(ProviderError::transient())]);
    let first_request = provider_request(ProviderOperation::Map);
    let second_request = provider_request(ProviderOperation::Reduce);

    assert_eq!(
        provider
            .generate(first_request)
            .await
            .expect("first scripted provider result should succeed"),
        first_response
    );
    assert_eq!(
        provider
            .generate(second_request)
            .await
            .expect_err("second scripted provider result should fail"),
        ProviderError::transient()
    );
    assert_eq!(provider.remaining(), 0);
    assert_eq!(provider.calls().len(), 2);
    assert_eq!(
        provider.calls()[1].request().operation(),
        ProviderOperation::Reduce
    );

    let deadline = timestamp("2026-09-18T16:09:00Z");
    let lease = ScriptedLease::new([
        Ok(LeasePermit::new(lease_token(), deadline)),
        Err(LeaseError::lost()),
    ]);
    let permit = lease
        .permit(Duration::from_secs(30))
        .await
        .expect("first scripted lease result should succeed");
    assert_eq!(permit.lease_token().as_str(), LEASE_TOKEN);
    assert_eq!(permit.deadline(), &deadline);
    assert_eq!(
        lease
            .permit(Duration::from_secs(15))
            .await
            .expect_err("second scripted lease result should fail"),
        LeaseError::lost()
    );
    assert_eq!(lease.remaining(), 0);
    assert_eq!(
        lease
            .calls()
            .into_iter()
            .map(|call| call.maximum)
            .collect::<Vec<_>>(),
        [Duration::from_secs(30), Duration::from_secs(15)]
    );
}

#[tokio::test]
async fn recording_and_failing_memory_sinks_return_canonical_outcomes() {
    let sink = RecordingMemorySink::new(EnsureMemoryOutcome::ExistingEquivalent);
    assert_eq!(
        sink.ensure_memory(memory())
            .await
            .expect("recording memory sink should return configured outcome"),
        EnsureMemoryOutcome::ExistingEquivalent
    );
    let calls = sink.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].memory().id, MEMORY_ID);

    let failing = FailingMemorySink::new(MemoryPublishError::unknown());
    assert_eq!(
        failing
            .ensure_memory(memory())
            .await
            .expect_err("failing memory sink should return its configured failure"),
        MemoryPublishError::unknown()
    );
    assert_eq!(failing.calls().len(), 1);
}

#[tokio::test]
async fn deterministic_gate_coordinates_concurrent_tasks_without_sleeping_or_polling() {
    let gate = DeterministicGate::new();
    let first_gate = gate.clone();
    let second_gate = gate.clone();
    let first = tokio::spawn(async move {
        first_gate.enter().await;
        "first"
    });
    let second = tokio::spawn(async move {
        second_gate.enter().await;
        "second"
    });

    gate.wait_for_entered(2).await;
    assert_eq!(gate.entered(), 2);
    assert!(!first.is_finished());
    assert!(!second.is_finished());

    gate.release();
    assert_eq!(first.await.expect("first task should complete"), "first");
    assert_eq!(second.await.expect("second task should complete"), "second");
}

#[test]
fn port_values_and_errors_are_content_safe() {
    let staged = staged_revision();
    assert_eq!(staged.transcript().entries().len(), 2);
    assert_eq!(
        staged.generated_revision().source_revision().as_str(),
        SOURCE_REVISION
    );
    assert_eq!(staged.candidates().len(), 1);
    assert_safe_rendering(
        &staged,
        &[
            "raw-observation-secret-sentinel",
            "summary-secret-sentinel",
            "memory-title-secret-sentinel",
            "memory-content-secret-sentinel",
            OBSERVATION_RECEIPT,
        ],
    );

    let permit = LeasePermit::new(lease_token(), timestamp("2026-09-18T16:09:00Z"));
    assert_safe_rendering(&permit, &[LEASE_TOKEN]);

    assert_port_error(
        RepositoryError::unconfirmed_commit(),
        FailureCategory::RepositoryUnconfirmedCommit,
        FailureStage::Repository,
        Retryability::AfterLeaseExpiry,
    );
    assert_port_error(
        LeaseError::lost(),
        FailureCategory::LeaseLost,
        FailureStage::Lease,
        Retryability::AfterLeaseExpiry,
    );
    assert_port_error(
        ProviderError::authentication(),
        FailureCategory::ProviderAuthentication,
        FailureStage::Provider,
        Retryability::AfterBackoff,
    );
    assert_port_error(
        MemoryPublishError::mismatch(),
        FailureCategory::MemoryMismatch,
        FailureStage::Memory,
        Retryability::AfterBackoff,
    );
}

#[test]
fn staged_candidates_validate_canonical_structure_and_keep_payloads_out_of_diagnostics() {
    let candidate = staged_candidate();
    assert_eq!(candidate.content_fingerprint(), "normalized-fingerprint");
    assert_eq!(candidate.memory_id(), MEMORY_ID);
    assert_eq!(candidate.supporting_receipt_ids().len(), 1);
    assert_safe_rendering(
        &candidate,
        &[
            "normalized-fingerprint",
            "memory-title-secret-sentinel",
            "memory-content-secret-sentinel",
            OBSERVATION_RECEIPT,
        ],
    );

    let mut invalid_payload = memory();
    invalid_payload.id = "11111111-1111-4111-8111-111111111111".to_owned();
    assert_staged_candidate_rejected(
        StagedMemoryCandidate::try_new(
            "fingerprint-secret-sentinel".to_owned(),
            invalid_payload,
            vec![OBSERVATION_RECEIPT.to_owned()],
        ),
        "canonical_payload",
        "memory_id_not_uuid_v5",
    );

    let mut invalid_payload = memory();
    invalid_payload.id = "00000000-0000-0000-0000-000000000000".to_owned();
    assert_staged_candidate_rejected(
        StagedMemoryCandidate::try_new(
            "fingerprint-secret-sentinel".to_owned(),
            invalid_payload,
            vec![OBSERVATION_RECEIPT.to_owned()],
        ),
        "canonical_payload",
        "memory_id_not_uuid_v5",
    );

    let mut invalid_payload = memory();
    invalid_payload.title = "".to_owned();
    assert_staged_candidate_rejected(
        StagedMemoryCandidate::try_new(
            "fingerprint-secret-sentinel".to_owned(),
            invalid_payload,
            vec![OBSERVATION_RECEIPT.to_owned()],
        ),
        "canonical_payload",
        "invalid",
    );

    let mut invalid_payload = memory();
    invalid_payload.memory_type = "other-type".to_owned();
    assert_staged_candidate_rejected(
        StagedMemoryCandidate::try_new(
            "fingerprint-secret-sentinel".to_owned(),
            invalid_payload,
            vec![OBSERVATION_RECEIPT.to_owned()],
        ),
        "canonical_payload",
        "wrong_memory_type",
    );

    let mut invalid_payload = memory();
    invalid_payload.version = 2;
    assert_staged_candidate_rejected(
        StagedMemoryCandidate::try_new(
            "fingerprint-secret-sentinel".to_owned(),
            invalid_payload,
            vec![OBSERVATION_RECEIPT.to_owned()],
        ),
        "canonical_payload",
        "wrong_version",
    );

    let mut invalid_payload = memory();
    invalid_payload.files = vec!["file-secret-sentinel".to_owned()];
    assert_staged_candidate_rejected(
        StagedMemoryCandidate::try_new(
            "fingerprint-secret-sentinel".to_owned(),
            invalid_payload,
            vec![OBSERVATION_RECEIPT.to_owned()],
        ),
        "canonical_payload",
        "files_not_empty",
    );

    for (fingerprint, supports, field, code) in [
        (
            "",
            vec![OBSERVATION_RECEIPT.to_owned()],
            "content_fingerprint",
            "empty",
        ),
        (
            "fingerprint\0secret",
            vec![OBSERVATION_RECEIPT.to_owned()],
            "content_fingerprint",
            "contains_nul",
        ),
        (
            "fingerprint-secret-sentinel",
            vec![],
            "supporting_receipt_ids",
            "empty",
        ),
        (
            "fingerprint-secret-sentinel",
            vec![
                OBSERVATION_RECEIPT.to_owned(),
                OBSERVATION_RECEIPT.to_owned(),
            ],
            "supporting_receipt_ids",
            "duplicate",
        ),
        (
            "fingerprint-secret-sentinel",
            vec!["receipt-secret-sentinel".to_owned()],
            "supporting_receipt_ids",
            "invalid_uuid",
        ),
    ] {
        assert_staged_candidate_rejected(
            StagedMemoryCandidate::try_new(fingerprint.to_owned(), memory(), supports),
            field,
            code,
        );
    }

    let mut wrong_evidence_payload = memory();
    wrong_evidence_payload.source_observation_ids =
        vec!["66666666-6666-4666-8666-666666666666".to_owned()];
    assert_staged_candidate_rejected(
        StagedMemoryCandidate::try_new(
            "fingerprint-secret-sentinel".to_owned(),
            wrong_evidence_payload,
            vec![OBSERVATION_RECEIPT.to_owned()],
        ),
        "supporting_receipt_ids",
        "does_not_match_canonical_payload",
    );

    let mut wrong_session_payload = memory();
    wrong_session_payload.session_ids = vec!["other-session-secret-sentinel".to_owned()];
    let wrong_session_candidate = StagedMemoryCandidate::try_new(
        "other-fingerprint".to_owned(),
        wrong_session_payload,
        vec![OBSERVATION_RECEIPT.to_owned()],
    )
    .expect("a candidate without a claim remains structurally valid");
    let error = StagedRevision::try_new(
        &claim(),
        snapshot().transcript().clone(),
        generated_revision(),
        vec![wrong_session_candidate],
    )
    .expect_err("staged candidates must have the exact claim session");
    assert_contract_error(
        error,
        "canonical_payload",
        "session_id_does_not_match_claim",
    );
}

fn read_clock(clock: &dyn Clock) -> DateTime<Utc> {
    clock.now()
}

fn claim() -> Claim {
    Claim::try_from(ClaimInput {
        attempt_id: ATTEMPT_ID.to_owned(),
        session_id: SESSION_ID.to_owned(),
        source_revision: SOURCE_REVISION.to_owned(),
        lifecycle_count: 1,
        observation_count: 1,
        state: session_post_processing::contracts::AttemptState::Claimed,
        lease_token: LEASE_TOKEN.to_owned(),
        lease_expires_at: timestamp("2026-09-18T16:10:00Z"),
    })
    .expect("claim fixture should be valid")
}

fn snapshot() -> SourceSnapshot {
    SourceSnapshot::try_from(SourceSnapshotInput {
        session_id: SESSION_ID.to_owned(),
        source_revision: SOURCE_REVISION.to_owned(),
        source_cutoff: timestamp("2026-09-18T14:00:00Z"),
        entries: vec![
            TranscriptEntryInput::Lifecycle {
                receipt_id: LIFECYCLE_RECEIPT.to_owned(),
                event_type: SourceEventType::SessionStart,
                session_id: SESSION_ID.to_owned(),
                project_name: "project".to_owned(),
                current_working_directory: "/work".to_owned(),
                source_timestamp_rfc3339: "2026-09-18T10:00:00.000Z".to_owned(),
                source_timestamp_utc: timestamp("2026-09-18T10:00:00Z"),
                ingested_at: timestamp("2026-09-18T11:00:00Z"),
            },
            TranscriptEntryInput::Observation {
                receipt_id: OBSERVATION_RECEIPT.to_owned(),
                event_type: SourceEventType::Observation,
                session_id: SESSION_ID.to_owned(),
                hook_type: "post_tool_use".to_owned(),
                project_name: "project".to_owned(),
                current_working_directory: "/work".to_owned(),
                source_timestamp_rfc3339: "2026-09-18T12:00:00.000Z".to_owned(),
                source_timestamp_utc: timestamp("2026-09-18T12:00:00Z"),
                ingested_at: timestamp("2026-09-18T13:00:00Z"),
                data: json!({"opaque": "raw-observation-secret-sentinel"}),
            },
        ],
    })
    .expect("source snapshot fixture should be valid")
}

fn loaded_snapshot() -> LoadedSnapshot {
    LoadedSnapshot::try_new(snapshot(), memory_scope())
        .expect("loaded snapshot fixture should be valid")
}

fn staged_revision() -> StagedRevision {
    StagedRevision::try_new(
        &claim(),
        snapshot().transcript().clone(),
        generated_revision(),
        vec![staged_candidate()],
    )
    .expect("staged revision fixture should be valid")
}

fn staged_candidate() -> StagedMemoryCandidate {
    StagedMemoryCandidate::try_new(
        "normalized-fingerprint".to_owned(),
        memory(),
        vec![OBSERVATION_RECEIPT.to_owned()],
    )
    .expect("staged candidate fixture should be valid")
}

fn generated_revision() -> GeneratedRevision {
    GeneratedRevisionInput {
        source_revision: SOURCE_REVISION.to_owned(),
        source_cutoff: timestamp("2026-09-18T14:00:00Z"),
        generated_at: timestamp("2026-09-18T15:00:00Z"),
        summary_sentences: vec!["summary-secret-sentinel".to_owned()],
        concepts: (1..=10).map(|number| format!("concept-{number}")).collect(),
        memory_candidates: vec![MemoryCandidateInput {
            title: "memory-title-secret-sentinel".to_owned(),
            content: "memory-content-secret-sentinel".to_owned(),
            concepts: vec!["candidate-concept".to_owned()],
            supporting_receipt_ids: vec![OBSERVATION_RECEIPT.to_owned()],
        }],
    }
    .try_into_generated_revision(&memory_scope())
    .expect("generated revision fixture should be valid")
}

fn memory_scope() -> MemoryScope {
    MemoryScope::try_from(vec![OBSERVATION_RECEIPT.to_owned()])
        .expect("memory scope fixture should be valid")
}

fn provider_request(operation: ProviderOperation) -> StructuredGenerationRequest {
    StructuredGenerationRequest::try_from(StructuredGenerationRequestInput {
        operation,
        prompt: "provider-prompt-secret-sentinel".to_owned(),
    })
    .expect("provider request fixture should be valid")
}

fn provider_response(summary: &str) -> StructuredGenerationResponse {
    StructuredGenerationResponse::try_from(StructuredGenerationResponseInput {
        summary_sentences: vec![summary.to_owned()],
        concepts: vec!["provider-concept".to_owned()],
        memory_candidates: vec![],
    })
    .expect("provider response fixture should be valid")
}

fn memory() -> MemoryVersionInput {
    MemoryVersionInput {
        id: MEMORY_ID.to_owned(),
        version: 1,
        memory_type: "session-derived".to_owned(),
        title: "memory-title-secret-sentinel".to_owned(),
        content: "memory-content-secret-sentinel".to_owned(),
        created_at: timestamp("2026-09-18T15:00:00Z"),
        updated_at: timestamp("2026-09-18T15:00:00Z"),
        concepts: vec!["candidate-concept".to_owned()],
        files: vec![],
        session_ids: vec![SESSION_ID.to_owned()],
        source_observation_ids: vec![OBSERVATION_RECEIPT.to_owned()],
    }
}

fn lease_token() -> LeaseToken {
    LeaseToken::try_from(LEASE_TOKEN.to_owned()).expect("lease token fixture should be valid")
}

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("timestamp fixture should be valid")
        .with_timezone(&Utc)
}

fn assert_port_error<E>(
    error: E,
    category: FailureCategory,
    stage: FailureStage,
    retryability: Retryability,
) where
    E: Debug + Display + Serialize + PortFailure,
{
    assert_eq!(error.category(), category);
    assert_eq!(error.stage(), stage);
    assert_eq!(error.retryability(), retryability);
    let serialized = serde_json::to_value(&error).expect("port error should serialize safely");
    assert_eq!(
        serialized["category"],
        serde_json::to_value(category).unwrap()
    );
    assert_eq!(serialized["stage"], serde_json::to_value(stage).unwrap());
    assert_eq!(
        serialized["retryability"],
        serde_json::to_value(retryability).unwrap()
    );
    assert_eq!(serialized.as_object().unwrap().len(), 3);
    let displayed = error.to_string();
    let debug = format!("{error:?}");
    let serialized_text =
        serde_json::to_string(&error).expect("port error should serialize safely");
    for sentinel in [
        "raw-observation-secret-sentinel",
        "summary-secret-sentinel",
        "memory-title-secret-sentinel",
        "memory-content-secret-sentinel",
        "provider-prompt-secret-sentinel",
        "provider-token-secret-sentinel",
        "provider-body-secret-sentinel",
        "select secret sql",
    ] {
        assert!(
            !displayed.contains(sentinel),
            "Display rendering leaked protected value: {displayed}"
        );
        assert!(
            !debug.contains(sentinel),
            "Debug rendering leaked protected value: {debug}"
        );
        assert!(
            !serialized_text.contains(sentinel),
            "serialized error leaked protected value: {serialized_text}"
        );
    }
}

trait PortFailure {
    fn category(&self) -> FailureCategory;
    fn stage(&self) -> FailureStage;
    fn retryability(&self) -> Retryability;
}

macro_rules! impl_port_failure {
    ($($type:ty),+ $(,)?) => {
        $(
            impl PortFailure for $type {
                fn category(&self) -> FailureCategory {
                    self.category()
                }

                fn stage(&self) -> FailureStage {
                    self.stage()
                }

                fn retryability(&self) -> Retryability {
                    self.retryability()
                }
            }
        )+
    };
}

impl_port_failure!(
    RepositoryError,
    LeaseError,
    ProviderError,
    MemoryPublishError
);

fn assert_safe_rendering<T>(value: &T, sentinels: &[&str])
where
    T: Debug,
{
    let rendered = format!("{value:?}");
    for sentinel in sentinels {
        assert!(
            !rendered.contains(sentinel),
            "Debug rendering leaked protected value: {rendered}"
        );
    }
}

fn assert_staged_candidate_rejected(
    result: Result<StagedMemoryCandidate, ContractError>,
    field: &str,
    code: &str,
) {
    let error = result.expect_err("invalid staged candidate should be rejected");
    assert_contract_error(error, field, code);
}

fn assert_contract_error(error: ContractError, field: &str, code: &str) {
    assert_eq!(error.field(), field);
    assert_eq!(error.code(), code);
    let display = error.to_string();
    let debug = format!("{error:?}");
    let serialized = serde_json::to_string(&error).expect("contract errors should serialize");
    for sentinel in [
        "fingerprint-secret-sentinel",
        "memory-title-secret-sentinel",
        "memory-content-secret-sentinel",
        "file-secret-sentinel",
        "receipt-secret-sentinel",
        "other-session-secret-sentinel",
    ] {
        assert!(
            !display.contains(sentinel),
            "Display error leaked protected value: {display}"
        );
        assert!(
            !debug.contains(sentinel),
            "Debug error leaked protected value: {debug}"
        );
        assert!(
            !serialized.contains(sentinel),
            "serialized error leaked protected value: {serialized}"
        );
    }
}
