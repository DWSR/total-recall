use std::fmt::{Debug, Display};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use session_post_processing::contracts::{
    AttemptState, Claim, ClaimInput, ContractError, CorrelationId, FailureCategory, FailureStage,
    GeneratedRevisionInput, GenerationInput, LoadedSnapshot, MemoryScope, ProcessingError,
    ProviderExchange, ProviderExchangeInput, ProviderOperation, Retryability, SnapshotLoadOutcome,
    SourceSnapshot, SourceSnapshotInput, SweepOutcome, SweepOutcomeInput,
};

const ATTEMPT_ID: &str = "018f5d00-0000-7000-8000-000000000001";
const SOURCE_REVISION: &str = "22222222-2222-5222-8222-222222222222";
const LEASE_TOKEN: &str = "018f5d00-0000-7000-8000-000000000002";
const LIFECYCLE_RECEIPT: &str = "44444444-4444-4444-8444-444444444444";
const OBSERVATION_RECEIPT: &str = "55555555-5555-4555-8555-555555555555";
const OUTSIDE_RECEIPT: &str = "66666666-6666-4666-8666-666666666666";
const CORRELATION_ID: &str = "77777777-7777-4777-8777-777777777777";
const SESSION_ID: &str = "session-1";

#[test]
fn validated_contracts_preserve_source_metadata_and_generation_structure() {
    let claim = Claim::try_from(parse::<ClaimInput>(valid_claim_json()))
        .expect("valid claim should be accepted");
    let snapshot = valid_snapshot();
    let scope = valid_memory_scope();
    let generation_input = GenerationInput::try_new(snapshot.transcript().clone(), scope.clone())
        .expect("observation-backed scope should be accepted");
    let revision = parse::<GeneratedRevisionInput>(valid_generated_revision_json())
        .try_into_generated_revision(&scope)
        .expect("valid generated revision should be accepted");
    let exchange = ProviderExchange::try_from(parse::<ProviderExchangeInput>(
        valid_provider_exchange_json(),
    ))
    .expect("valid provider exchange should be accepted");
    let outcome = SweepOutcome::try_from(parse::<SweepOutcomeInput>(json!({
        "attempted": 4,
        "staged": 1,
        "completed": 1,
        "retryable": 1,
        "skipped": 1,
    })))
    .expect("balanced sweep outcome should be accepted");

    assert_eq!(claim.session_id().as_str(), SESSION_ID);
    assert_eq!(claim.lifecycle_count(), 1);
    assert_eq!(claim.observation_count(), 1);
    assert!(snapshot.matches_claim(&claim));
    assert_eq!(snapshot.source_revision().as_str(), SOURCE_REVISION);
    assert_eq!(snapshot.transcript().entries().len(), 2);
    let observation = &snapshot.transcript().entries()[1];
    assert_eq!(observation.receipt_id().as_str(), OBSERVATION_RECEIPT);
    assert_eq!(observation.project_name(), "project");
    assert_eq!(observation.current_working_directory(), "/work");
    assert_eq!(
        observation.source_timestamp_rfc3339(),
        "2026-09-18T12:00:00.000Z"
    );
    assert_eq!(observation.hook_type(), Some("post_tool_use"));
    let observation_data = json!({
        "opaque": {
            "nested": "raw-observation-secret-sentinel"
        }
    });
    assert_eq!(observation.observation_data(), Some(&observation_data));
    assert_eq!(generation_input.memory_scope(), &scope);
    assert_eq!(revision.source_revision().as_str(), SOURCE_REVISION);
    assert_eq!(
        revision.summary_sentences(),
        ["First sentence.", "Second sentence."]
    );
    assert_eq!(revision.summary(), "First sentence. Second sentence.");
    assert_eq!(revision.concepts().len(), 10);
    assert_eq!(revision.concepts()[0], "Concept 1");
    assert_eq!(revision.memory_candidates().len(), 1);
    assert_eq!(
        revision.memory_candidates()[0].supporting_receipt_ids()[0].as_str(),
        OBSERVATION_RECEIPT
    );
    assert_eq!(exchange.request().operation(), ProviderOperation::Map);
    assert_eq!(exchange.response().summary_sentences(), ["Map sentence."]);
    assert_eq!(outcome.attempted(), 4);
    assert_eq!(outcome.staged(), 1);
    assert_eq!(outcome.completed(), 1);
    assert_eq!(outcome.retryable(), 1);
    assert_eq!(outcome.skipped(), 1);

    let serialized_snapshot = serde_json::to_value(&snapshot).expect("snapshot should serialize");
    assert!(serialized_snapshot.get("entries").is_some());
    assert!(serialized_snapshot.get("transcript").is_none());
    let serialized_revision = serde_json::to_value(&revision).expect("revision should serialize");
    assert!(serialized_revision.get("summary_sentences").is_some());
    assert!(serialized_revision.get("response").is_none());
    assert_eq!(
        serde_json::to_value(outcome).expect("sweep outcome should serialize"),
        json!({
            "attempted": 4,
            "staged": 1,
            "completed": 1,
            "retryable": 1,
            "skipped": 1,
        })
    );
}

#[test]
fn loaded_snapshots_validate_memory_scope_and_redact_source_values() {
    let snapshot = valid_snapshot();
    let scope = valid_memory_scope();
    let loaded = LoadedSnapshot::try_new(snapshot.clone(), scope.clone())
        .expect("an observation-backed scope should load");

    assert_eq!(loaded.snapshot(), &snapshot);
    assert_eq!(loaded.memory_scope(), &scope);
    assert_eq!(
        serde_json::to_value(&loaded).expect("loaded snapshot should serialize safely"),
        json!({
            "source_entry_count": 2,
            "memory_scope_count": 1,
        })
    );

    let outcome = SnapshotLoadOutcome::Current(loaded.clone());
    let renderings = [
        format!("{loaded:?}"),
        serde_json::to_string(&loaded).expect("loaded snapshot should serialize safely"),
        format!("{outcome:?}"),
        serde_json::to_string(&outcome).expect("load outcome should serialize safely"),
    ];
    for rendering in renderings {
        for sentinel in [
            "raw-observation-secret-sentinel",
            "transcript-secret-sentinel",
            OBSERVATION_RECEIPT,
        ] {
            assert!(
                !rendering.contains(sentinel),
                "loaded snapshot diagnostics leaked protected value: {rendering}"
            );
        }
    }

    let lifecycle_scope = MemoryScope::try_from(vec![LIFECYCLE_RECEIPT.to_owned()])
        .expect("lifecycle receipt format should be valid");
    let error = LoadedSnapshot::try_new(snapshot.clone(), lifecycle_scope)
        .expect_err("memory scope cannot contain lifecycle receipts");
    assert_contract_error(error, "memory_scope", "not_observation_receipt");

    let outside_scope = MemoryScope::try_from(vec![OUTSIDE_RECEIPT.to_owned()])
        .expect("outside receipt format should be valid");
    let error = LoadedSnapshot::try_new(snapshot, outside_scope)
        .expect_err("memory scope cannot contain receipts outside the current snapshot");
    assert_contract_error(error, "memory_scope", "not_observation_receipt");
}

#[test]
fn input_boundaries_reject_unknown_fields_and_malformed_source_variants() {
    let mut claim = valid_claim_json();
    claim["unexpected"] = json!(true);
    assert_rejects_deserialization::<ClaimInput>(claim);

    let mut snapshot = valid_snapshot_json();
    snapshot["unexpected"] = json!(true);
    assert_rejects_deserialization::<SourceSnapshotInput>(snapshot);

    let mut snapshot = valid_snapshot_json();
    snapshot["entries"][0]["unexpected"] = json!(true);
    assert_rejects_deserialization::<SourceSnapshotInput>(snapshot);

    let mut exchange = valid_provider_exchange_json();
    exchange["request"]["unexpected"] = json!(true);
    assert_rejects_deserialization::<ProviderExchangeInput>(exchange);

    let mut revision = valid_generated_revision_json();
    revision["unexpected"] = json!(true);
    assert_rejects_deserialization::<GeneratedRevisionInput>(revision);

    let mut outcome = json!({
        "attempted": 0,
        "staged": 0,
        "completed": 0,
        "retryable": 0,
        "skipped": 0,
    });
    outcome["unexpected"] = json!(true);
    assert_rejects_deserialization::<SweepOutcomeInput>(outcome);
    assert_rejects_deserialization::<SweepOutcomeInput>(json!({
        "attempted": 0,
        "completed": 0,
        "retryable": 0,
        "skipped": 0,
    }));

    let mut malformed_kind = valid_snapshot_json();
    malformed_kind["entries"][0]["kind"] = json!("unknown");
    assert_rejects_deserialization::<SourceSnapshotInput>(malformed_kind);

    let mut invalid_lifecycle_type = valid_snapshot_json();
    invalid_lifecycle_type["entries"][0]["event_type"] = json!("observation");
    let error = SourceSnapshot::try_from(parse::<SourceSnapshotInput>(invalid_lifecycle_type))
        .expect_err("observation cannot be a lifecycle event type");
    assert_contract_error(error, "entries", "invalid_event_type");

    let mut malformed_observation = valid_snapshot_json();
    malformed_observation["entries"][1]["data"] = json!("raw-observation-secret-sentinel");
    let error = SourceSnapshot::try_from(parse::<SourceSnapshotInput>(malformed_observation))
        .expect_err("observation data must remain an opaque object");
    assert_contract_error(error, "data", "not_object");

    let mut timestamp_without_milliseconds = valid_snapshot_json();
    timestamp_without_milliseconds["entries"][0]["source_timestamp_rfc3339"] =
        json!("2026-09-18T10:00:00Z");
    let error =
        SourceSnapshot::try_from(parse::<SourceSnapshotInput>(timestamp_without_milliseconds))
            .expect_err("source timestamps must preserve the persisted millisecond format");
    assert_contract_error(error, "source_timestamp_rfc3339", "invalid_rfc3339");

    assert_rejects_deserialization::<AttemptState>(json!("not_a_state"));
}

#[test]
fn source_snapshots_preserve_lowercase_millisecond_timestamp_text() {
    let mut input = valid_snapshot_json();
    input["entries"][1]["source_timestamp_rfc3339"] = json!("2026-09-18t12:00:00.000z");

    let snapshot = SourceSnapshot::try_from(parse::<SourceSnapshotInput>(input))
        .expect("persisted lowercase timestamp text should remain valid");

    assert_eq!(
        snapshot.transcript().entries()[1].source_timestamp_rfc3339(),
        "2026-09-18t12:00:00.000z"
    );
}

#[test]
fn source_snapshots_assemble_complete_transcripts_deterministically() {
    let entries = [
        json!({
            "kind": "lifecycle",
            "receipt_id": "11111111-1111-4111-8111-111111111111",
            "event_type": "session_start",
            "session_id": "deterministic-session",
            "project_name": "project-start",
            "current_working_directory": "/start",
            "source_timestamp_rfc3339": "2026-09-18T10:00:00.000Z",
            "source_timestamp_utc": "2026-09-18T10:00:00Z",
            "ingested_at": "2026-09-18T12:00:00Z",
        }),
        json!({
            "kind": "observation",
            "receipt_id": "22222222-2222-4222-8222-222222222222",
            "event_type": "observation",
            "session_id": "deterministic-session",
            "hook_type": "early_hook",
            "project_name": "project-earlier-source",
            "current_working_directory": "/earlier-source",
            "source_timestamp_rfc3339": "2026-09-18T09:00:00.000Z",
            "source_timestamp_utc": "2026-09-18T09:00:00Z",
            "ingested_at": "2026-09-18T16:00:00Z",
            "data": {"opaque": {"delivery": "earlier-source"}},
        }),
        json!({
            "kind": "observation",
            "receipt_id": "33333333-3333-4333-8333-333333333333",
            "event_type": "observation",
            "session_id": "deterministic-session",
            "hook_type": "recorded_earlier_hook",
            "project_name": "project-recorded-earlier",
            "current_working_directory": "/recorded-earlier",
            "source_timestamp_rfc3339": "2026-09-18T10:00:00.000Z",
            "source_timestamp_utc": "2026-09-18T10:00:00Z",
            "ingested_at": "2026-09-18T11:00:00Z",
            "data": {"opaque": {"delivery": "recorded-earlier"}},
        }),
        json!({
            "kind": "observation",
            "receipt_id": "44444444-4444-4444-8444-444444444444",
            "event_type": "observation",
            "session_id": "deterministic-session",
            "hook_type": "repeated_delivery",
            "project_name": "project-repeated",
            "current_working_directory": "/repeated",
            "source_timestamp_rfc3339": "2026-09-18T10:00:00.000Z",
            "source_timestamp_utc": "2026-09-18T10:00:00Z",
            "ingested_at": "2026-09-18T12:00:00Z",
            "data": {"opaque": {"delivery": "repeated"}},
        }),
        json!({
            "kind": "observation",
            "receipt_id": "55555555-5555-4555-8555-555555555555",
            "event_type": "observation",
            "session_id": "deterministic-session",
            "hook_type": "repeated_delivery",
            "project_name": "project-repeated",
            "current_working_directory": "/repeated",
            "source_timestamp_rfc3339": "2026-09-18T10:00:00.000Z",
            "source_timestamp_utc": "2026-09-18T10:00:00Z",
            "ingested_at": "2026-09-18T12:00:00Z",
            "data": {"opaque": {"delivery": "repeated"}},
        }),
        json!({
            "kind": "lifecycle",
            "receipt_id": "66666666-6666-4666-8666-666666666666",
            "event_type": "session_end",
            "session_id": "deterministic-session",
            "project_name": "project-end",
            "current_working_directory": "/end",
            "source_timestamp_rfc3339": "2026-09-18T10:00:00.000Z",
            "source_timestamp_utc": "2026-09-18T10:00:00Z",
            "ingested_at": "2026-09-18T12:00:00Z",
        }),
    ];
    let snapshot = |entries: Vec<Value>| {
        SourceSnapshot::try_from(parse::<SourceSnapshotInput>(json!({
            "session_id": "deterministic-session",
            "source_revision": SOURCE_REVISION,
            "source_cutoff": "2026-09-18T16:00:00Z",
            "entries": entries,
        })))
        .expect("complete source rows should assemble")
    };

    let first = snapshot(vec![
        entries[5].clone(),
        entries[3].clone(),
        entries[0].clone(),
        entries[4].clone(),
        entries[2].clone(),
        entries[1].clone(),
    ]);
    let second = snapshot(vec![
        entries[1].clone(),
        entries[2].clone(),
        entries[4].clone(),
        entries[5].clone(),
        entries[3].clone(),
        entries[0].clone(),
    ]);
    let expected = json!({
        "entries": [
            entries[1].clone(),
            entries[2].clone(),
            entries[0].clone(),
            entries[3].clone(),
            entries[4].clone(),
            entries[5].clone(),
        ],
    });

    assert_eq!(
        serde_json::to_value(first.transcript()).expect("transcript should serialize"),
        expected
    );
    assert_eq!(
        serde_json::to_value(second.transcript()).expect("transcript should serialize"),
        serde_json::to_value(first.transcript()).expect("transcript should serialize")
    );

    let incomplete_lifecycle = snapshot(vec![entries[4].clone(), entries[3].clone()]);
    assert_eq!(
        serde_json::to_value(incomplete_lifecycle.transcript())
            .expect("incomplete lifecycle transcript should serialize"),
        json!({"entries": [entries[3].clone(), entries[4].clone()]})
    );
}

#[test]
fn source_snapshots_reject_duplicate_receipt_ids_across_lifecycle_and_observation_rows() {
    let mut input = valid_snapshot_json();
    input["entries"][1]["receipt_id"] = input["entries"][0]["receipt_id"].clone();

    let error = SourceSnapshot::try_from(parse::<SourceSnapshotInput>(input))
        .expect_err("combined source rows with duplicate receipt IDs must be rejected");

    assert_contract_error(error, "entries", "duplicate_receipt_id");
}

#[test]
fn contracts_reject_invalid_cardinality_scope_and_state_transitions() {
    let mut empty_claim = valid_claim_json();
    empty_claim["lifecycle_count"] = json!(0);
    empty_claim["observation_count"] = json!(0);
    let error = Claim::try_from(parse::<ClaimInput>(empty_claim))
        .expect_err("a claim without source records must be rejected");
    assert_contract_error(error, "source_counts", "empty");

    let mut non_v5_revision = valid_claim_json();
    non_v5_revision["source_revision"] = json!(ATTEMPT_ID);
    let error = Claim::try_from(parse::<ClaimInput>(non_v5_revision))
        .expect_err("source revisions must be UUIDv5 values");
    assert_contract_error(error, "source_revision", "not_uuid_v5");

    let mut empty_snapshot = valid_snapshot_json();
    empty_snapshot["entries"] = json!([]);
    let error = SourceSnapshot::try_from(parse::<SourceSnapshotInput>(empty_snapshot))
        .expect_err("an empty source snapshot must be rejected");
    assert_contract_error(error, "entries", "empty");

    let claim = Claim::try_from(parse::<ClaimInput>(valid_claim_json()))
        .expect("valid claim should be accepted");
    let mut mismatched_claim = valid_claim_json();
    mismatched_claim["observation_count"] = json!(2);
    let mismatched_claim = Claim::try_from(parse::<ClaimInput>(mismatched_claim))
        .expect("nonempty claimed counts are structurally valid");
    let snapshot = valid_snapshot();
    assert!(snapshot.matches_claim(&claim));
    assert!(!snapshot.matches_claim(&mismatched_claim));

    let lifecycle_scope = MemoryScope::try_from(vec![LIFECYCLE_RECEIPT.to_owned()])
        .expect("valid receipt format should be accepted before transcript validation");
    let error = GenerationInput::try_new(snapshot.transcript().clone(), lifecycle_scope)
        .expect_err("memory scope cannot contain lifecycle receipts");
    assert_contract_error(error, "memory_scope", "not_observation_receipt");

    let scope = valid_memory_scope();
    let mut no_summary = valid_generated_revision_json();
    no_summary["summary_sentences"] = json!([]);
    let error = parse::<GeneratedRevisionInput>(no_summary)
        .try_into_generated_revision(&scope)
        .expect_err("a generated revision requires a summary");
    assert_contract_error(error, "summary_sentences", "empty");

    let mut too_many_sentences = valid_generated_revision_json();
    too_many_sentences["summary_sentences"] = json!(["one", "two", "three", "four", "five", "six"]);
    let error = parse::<GeneratedRevisionInput>(too_many_sentences)
        .try_into_generated_revision(&scope)
        .expect_err("more than five summary sentences must be rejected");
    assert_contract_error(error, "summary_sentences", "too_many");

    let mut short_concepts = valid_generated_revision_json();
    short_concepts["concepts"] = json!([
        "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"
    ]);
    let error = parse::<GeneratedRevisionInput>(short_concepts)
        .try_into_generated_revision(&scope)
        .expect_err("final output requires exactly ten concepts");
    assert_contract_error(error, "concepts", "wrong_count");

    let mut duplicate_concepts = valid_generated_revision_json();
    duplicate_concepts["concepts"] = json!([
        "Concept 1",
        "Concept 2",
        "Concept 3",
        "Concept 4",
        "Concept 5",
        "Concept 6",
        "Concept 7",
        "Concept 8",
        "Concept 9",
        " concept 1 "
    ]);
    let error = parse::<GeneratedRevisionInput>(duplicate_concepts)
        .try_into_generated_revision(&scope)
        .expect_err("concepts must be distinct after trimming and lowercasing");
    assert_contract_error(error, "concepts", "duplicate");

    let mut invalid_evidence = valid_generated_revision_json();
    invalid_evidence["memory_candidates"][0]["supporting_receipt_ids"] = json!([OUTSIDE_RECEIPT]);
    let error = parse::<GeneratedRevisionInput>(invalid_evidence)
        .try_into_generated_revision(&scope)
        .expect_err("candidate evidence must be within the memory scope");
    assert_contract_error(error, "supporting_receipt_ids", "outside_memory_scope");

    let error = MemoryScope::try_from(vec![
        OBSERVATION_RECEIPT.to_owned(),
        OBSERVATION_RECEIPT.to_owned(),
    ])
    .expect_err("memory scope receipt IDs must be unique");
    assert_contract_error(error, "memory_scope", "duplicate_receipt_id");

    let mut duplicate_evidence = valid_generated_revision_json();
    duplicate_evidence["memory_candidates"][0]["supporting_receipt_ids"] =
        json!([OBSERVATION_RECEIPT, OBSERVATION_RECEIPT]);
    let error = parse::<GeneratedRevisionInput>(duplicate_evidence)
        .try_into_generated_revision(&scope)
        .expect_err("candidate evidence receipt IDs must be unique");
    assert_contract_error(error, "supporting_receipt_ids", "duplicate");

    assert!(AttemptState::Claimed.can_transition_to(AttemptState::Staged));
    assert!(AttemptState::Claimed.can_transition_to(AttemptState::Superseded));
    assert!(AttemptState::Claimed.can_transition_to(AttemptState::Retryable));
    assert!(AttemptState::Staged.can_transition_to(AttemptState::Publishing));
    assert!(AttemptState::Staged.can_transition_to(AttemptState::Retryable));
    assert!(AttemptState::Publishing.can_transition_to(AttemptState::Complete));
    assert!(AttemptState::Publishing.can_transition_to(AttemptState::Retryable));
    assert!(AttemptState::Retryable.can_transition_to(AttemptState::Claimed));
    assert!(!AttemptState::Claimed.can_transition_to(AttemptState::Complete));
    assert!(!AttemptState::Complete.can_transition_to(AttemptState::Claimed));
    assert!(AttemptState::Complete.is_terminal());
    assert!(AttemptState::Superseded.is_terminal());

    let error = SweepOutcome::try_from(parse::<SweepOutcomeInput>(json!({
        "attempted": 2,
        "staged": 0,
        "completed": 1,
        "retryable": 1,
        "skipped": 1,
    })))
    .expect_err("outcome counts must balance");
    assert_contract_error(error, "sweep_outcome", "counts_do_not_balance");
}

#[test]
fn sweep_outcomes_count_staged_work_and_keep_diagnostics_safe() {
    let empty = SweepOutcome::try_from(parse::<SweepOutcomeInput>(json!({
        "attempted": 0,
        "staged": 0,
        "completed": 0,
        "retryable": 0,
        "skipped": 0,
    })))
    .expect("an empty sweep should have balanced all-zero counts");
    assert_eq!(empty.attempted(), 0);
    assert_eq!(empty.staged(), 0);
    assert_eq!(empty.completed(), 0);
    assert_eq!(empty.retryable(), 0);
    assert_eq!(empty.skipped(), 0);

    let error = SweepOutcome::try_from(parse::<SweepOutcomeInput>(json!({
        "attempted": u32::MAX,
        "staged": 1,
        "completed": u32::MAX,
        "retryable": 0,
        "skipped": 0,
    })))
    .expect_err("all outcome counts, including staged, must use checked addition");
    assert_contract_error(error, "sweep_outcome", "count_overflow");

    let error = SweepOutcome::try_from(parse::<SweepOutcomeInput>(json!({
        "attempted": 3,
        "staged": 1,
        "completed": 1,
        "retryable": 1,
        "skipped": 1,
    })))
    .expect_err("staged work must contribute to the sweep balance");
    assert_contract_error(error.clone(), "sweep_outcome", "counts_do_not_balance");
    assert_error_is_safe(
        &error,
        &[
            "raw-observation-secret-sentinel",
            "summary-secret-sentinel",
            "memory-content-secret-sentinel",
            "provider-token-secret-sentinel",
            "provider-body-secret-sentinel",
        ],
    );
}

#[test]
fn claims_require_active_uuidv7_fencing_and_u64_whole_second_fields() {
    let mut maximum_count = valid_claim_json();
    maximum_count["lifecycle_count"] = json!(u64::MAX);
    maximum_count["observation_count"] = json!(0);
    let claim = Claim::try_from(parse::<ClaimInput>(maximum_count))
        .expect("u64 lifecycle counts should be accepted");
    assert_eq!(claim.lifecycle_count(), u64::MAX);
    assert_eq!(claim.observation_count(), 0);

    let mut count_overflow = valid_claim_json();
    count_overflow["lifecycle_count"] = json!(u64::MAX);
    count_overflow["observation_count"] = json!(1);
    let error = Claim::try_from(parse::<ClaimInput>(count_overflow))
        .expect_err("combined source counts must not overflow");
    assert_contract_error(error, "source_counts", "overflow");

    let mut negative_count = valid_claim_json();
    negative_count["lifecycle_count"] = json!(-1);
    assert!(
        serde_json::from_value::<ClaimInput>(negative_count).is_err(),
        "negative source counts must not deserialize"
    );

    for state in ["claimed", "staged", "publishing"] {
        let mut input = valid_claim_json();
        input["state"] = json!(state);
        assert!(
            Claim::try_from(parse::<ClaimInput>(input)).is_ok(),
            "{state} is an active claim state"
        );
    }
    for state in ["retryable", "complete", "superseded"] {
        let mut input = valid_claim_json();
        input["state"] = json!(state);
        let error = Claim::try_from(parse::<ClaimInput>(input))
            .expect_err("{state} cannot be returned as an active claim");
        assert_contract_error(error, "state", "not_active");
    }

    let mut v4_attempt = valid_claim_json();
    v4_attempt["attempt_id"] = json!("11111111-1111-4111-8111-111111111111");
    let error =
        Claim::try_from(parse::<ClaimInput>(v4_attempt)).expect_err("attempt IDs must be UUIDv7");
    assert_contract_error(error, "attempt_id", "not_uuid_v7");

    let mut v4_token = valid_claim_json();
    v4_token["lease_token"] = json!("33333333-3333-4333-8333-333333333333");
    let error =
        Claim::try_from(parse::<ClaimInput>(v4_token)).expect_err("lease tokens must be UUIDv7");
    assert_contract_error(error, "lease_token", "not_uuid_v7");

    let mut subsecond_expiry = valid_claim_json();
    subsecond_expiry["lease_expires_at"] = json!("2026-09-18T15:00:00.001Z");
    let error = Claim::try_from(parse::<ClaimInput>(subsecond_expiry))
        .expect_err("lease timestamps must cross the database boundary on whole seconds");
    assert_contract_error(error, "lease_expires_at", "not_whole_second");
}

#[test]
fn failure_projection_has_fixed_stage_category_and_retryability_without_payloads() {
    let correlation = CorrelationId::try_from(CORRELATION_ID.to_owned())
        .expect("test correlation ID should be valid");

    for (category, stage, retryability) in [
        (
            FailureCategory::InvalidConfiguration,
            FailureStage::Startup,
            Retryability::Never,
        ),
        (
            FailureCategory::RegistrationMismatch,
            FailureStage::Startup,
            Retryability::Never,
        ),
        (
            FailureCategory::SourceConflict,
            FailureStage::Source,
            Retryability::Never,
        ),
        (
            FailureCategory::LeaseLost,
            FailureStage::Lease,
            Retryability::AfterLeaseExpiry,
        ),
        (
            FailureCategory::ProviderTransient,
            FailureStage::Provider,
            Retryability::AfterBackoff,
        ),
        (
            FailureCategory::ProviderContract,
            FailureStage::Provider,
            Retryability::AfterBackoff,
        ),
        (
            FailureCategory::ProviderAuthentication,
            FailureStage::Provider,
            Retryability::AfterBackoff,
        ),
        (
            FailureCategory::ProviderUnsupportedContract,
            FailureStage::Provider,
            Retryability::AfterBackoff,
        ),
        (
            FailureCategory::ProviderRequestSize,
            FailureStage::Provider,
            Retryability::AfterBackoff,
        ),
        (
            FailureCategory::MemoryUnknown,
            FailureStage::Memory,
            Retryability::AfterBackoff,
        ),
        (
            FailureCategory::MemoryMismatch,
            FailureStage::Memory,
            Retryability::AfterBackoff,
        ),
        (
            FailureCategory::RepositoryInvocation,
            FailureStage::Repository,
            Retryability::AfterLeaseExpiry,
        ),
        (
            FailureCategory::RepositoryUnconfirmedCommit,
            FailureStage::Repository,
            Retryability::AfterLeaseExpiry,
        ),
        (
            FailureCategory::RepositoryInvalidResponse,
            FailureStage::Repository,
            Retryability::AfterLeaseExpiry,
        ),
    ] {
        let error = ProcessingError::new(category, correlation.clone());
        assert_eq!(error.stage(), stage);
        assert_eq!(error.category(), category);
        assert_eq!(error.retryability(), retryability);

        let serialized = serde_json::to_value(&error).expect("errors should serialize safely");
        assert_eq!(serialized["stage"], serde_json::to_value(stage).unwrap());
        assert_eq!(
            serialized["category"],
            serde_json::to_value(category).unwrap()
        );
        assert_eq!(
            serialized["retryability"],
            serde_json::to_value(retryability).unwrap()
        );
        assert_eq!(serialized["correlation_id"], CORRELATION_ID);
        assert_eq!(
            serialized
                .as_object()
                .expect("error should serialize to an object")
                .len(),
            4
        );
        assert_error_is_safe(
            &error,
            &[
                "raw-observation-secret-sentinel",
                "transcript-secret-sentinel",
                "summary-secret-sentinel",
                "concept-secret-sentinel",
                "memory-title-secret-sentinel",
                "memory-content-secret-sentinel",
                "receipt-secret-sentinel",
                "provider-token-secret-sentinel",
                "provider-body-secret-sentinel",
                "select secret sql",
                "backend-secret-sentinel",
            ],
        );
    }
}

#[test]
fn validation_errors_do_not_render_or_serialize_protected_content() {
    let scope = valid_memory_scope();
    let mut input = valid_generated_revision_json();
    input["summary_sentences"] = json!([]);
    input["concepts"] = json!([
        "concept-secret-sentinel",
        "concept-2",
        "concept-3",
        "concept-4",
        "concept-5",
        "concept-6",
        "concept-7",
        "concept-8",
        "concept-9",
        "concept-10",
    ]);
    input["memory_candidates"][0]["title"] = json!("memory-title-secret-sentinel");
    input["memory_candidates"][0]["content"] = json!("memory-content-secret-sentinel");
    input["memory_candidates"][0]["supporting_receipt_ids"] = json!([OUTSIDE_RECEIPT]);

    let error = parse::<GeneratedRevisionInput>(input)
        .try_into_generated_revision(&scope)
        .expect_err("empty summary should be rejected before protected payloads are reported");
    assert_contract_error(error.clone(), "summary_sentences", "empty");
    assert_error_is_safe(
        &error,
        &[
            "concept-secret-sentinel",
            "memory-title-secret-sentinel",
            "memory-content-secret-sentinel",
            OUTSIDE_RECEIPT,
            "raw-observation-secret-sentinel",
            "provider-token-secret-sentinel",
            "provider-body-secret-sentinel",
            "select secret sql",
            "backend-secret-sentinel",
        ],
    );
}

fn valid_claim_json() -> Value {
    json!({
        "attempt_id": ATTEMPT_ID,
        "session_id": SESSION_ID,
        "source_revision": SOURCE_REVISION,
        "lifecycle_count": 1,
        "observation_count": 1,
        "state": "claimed",
        "lease_token": LEASE_TOKEN,
        "lease_expires_at": "2026-09-18T15:00:00Z",
    })
}

fn valid_snapshot_json() -> Value {
    json!({
        "session_id": SESSION_ID,
        "source_revision": SOURCE_REVISION,
        "source_cutoff": "2026-09-18T14:00:00Z",
        "entries": [
            {
                "kind": "lifecycle",
                "receipt_id": LIFECYCLE_RECEIPT,
                "event_type": "session_start",
                "session_id": SESSION_ID,
                "project_name": "project",
                "current_working_directory": "/work",
                "source_timestamp_rfc3339": "2026-09-18T10:00:00.000Z",
                "source_timestamp_utc": "2026-09-18T10:00:00Z",
                "ingested_at": "2026-09-18T11:00:00Z",
            },
            {
                "kind": "observation",
                "receipt_id": OBSERVATION_RECEIPT,
                "event_type": "observation",
                "session_id": SESSION_ID,
                "hook_type": "post_tool_use",
                "project_name": "project",
                "current_working_directory": "/work",
                "source_timestamp_rfc3339": "2026-09-18T12:00:00.000Z",
                "source_timestamp_utc": "2026-09-18T12:00:00Z",
                "ingested_at": "2026-09-18T13:00:00Z",
                "data": {
                    "opaque": {
                        "nested": "raw-observation-secret-sentinel"
                    }
                },
            }
        ],
    })
}

fn valid_generated_revision_json() -> Value {
    json!({
        "source_revision": SOURCE_REVISION,
        "source_cutoff": "2026-09-18T14:00:00Z",
        "generated_at": "2026-09-18T15:00:00Z",
        "summary_sentences": ["First sentence.", "Second sentence."],
        "concepts": [
            "Concept 1", "Concept 2", "Concept 3", "Concept 4", "Concept 5",
            "Concept 6", "Concept 7", "Concept 8", "Concept 9", "Concept 10"
        ],
        "memory_candidates": [
            {
                "title": "memory title",
                "content": "memory content",
                "concepts": ["Candidate concept"],
                "supporting_receipt_ids": [OBSERVATION_RECEIPT],
            }
        ],
    })
}

fn valid_provider_exchange_json() -> Value {
    json!({
        "request": {
            "operation": "map",
            "prompt": "transcript-secret-sentinel",
        },
        "response": {
            "summary_sentences": ["Map sentence."],
            "concepts": ["Map concept"],
            "memory_candidates": [
                {
                    "title": "provider memory title",
                    "content": "provider memory content",
                    "concepts": ["Map concept"],
                    "supporting_receipt_ids": [OBSERVATION_RECEIPT],
                }
            ],
        },
    })
}

fn valid_snapshot() -> SourceSnapshot {
    SourceSnapshot::try_from(parse::<SourceSnapshotInput>(valid_snapshot_json()))
        .expect("source snapshot fixture should be valid")
}

fn valid_memory_scope() -> MemoryScope {
    MemoryScope::try_from(vec![OBSERVATION_RECEIPT.to_owned()])
        .expect("memory scope fixture should be valid")
}

fn parse<T: DeserializeOwned>(value: Value) -> T {
    serde_json::from_value(value).expect("fixture should deserialize")
}

fn assert_rejects_deserialization<T: DeserializeOwned>(value: Value) {
    assert!(
        serde_json::from_value::<T>(value).is_err(),
        "input should reject an unknown or malformed field"
    );
}

fn assert_contract_error(error: ContractError, field: &str, code: &str) {
    assert_eq!(error.field(), field);
    assert_eq!(error.code(), code);
}

fn assert_error_is_safe<E>(error: &E, sentinels: &[&str])
where
    E: Debug + Display + Serialize,
{
    let display = error.to_string();
    let debug = format!("{error:?}");
    let serialized = serde_json::to_string(error).expect("error should serialize");

    for sentinel in sentinels {
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
