use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use iii_sdk::{Error as IiiError, protocol::TriggerRequest};
use memory_store::contracts::{DatabaseTarget, MemoryVersionInput};
use serde_json::{Value, json};
use session_post_processing::{
    claim_eligible_with,
    contracts::{
        AttemptState, Claim, ClaimInput, FailureCategory, GeneratedRevisionInput, MemoryScope,
        SnapshotLoadOutcome, SourceEventType, SourceSnapshot, SourceSnapshotInput,
        TranscriptEntryInput,
    },
    discover_eligible_with, load_snapshot_with, load_unpublished_candidates_with,
    mark_memory_published_with,
    ports::{RepositoryError, StagedMemoryCandidate, StagedRevision},
    promote_with, renew_with, retry_with, stage_with,
};
use uuid::Uuid;

const DATABASE: &str = "session-post-processing-test";
const RETAINED_ATTEMPT_ID: &str = "018f5d00-0000-7000-8000-000000000010";
const RETAINED_LEASE_TOKEN: &str = "018f5d00-0000-7000-8000-000000000011";
const STAGED_ATTEMPT_ID: &str = "018f5d00-0000-7000-8000-000000000012";
const PUBLISHING_ATTEMPT_ID: &str = "018f5d00-0000-7000-8000-000000000013";
const STAGED_LEASE_TOKEN: &str = "018f5d00-0000-7000-8000-000000000014";
const PUBLISHING_LEASE_TOKEN: &str = "018f5d00-0000-7000-8000-000000000015";
const SOURCE_REVISION_NAMESPACE: &[u8] = b"total-recall/session-post-processing/source-revision/v1";
const STAGING_MEMORY_ID_ONE: &str = "33333333-3333-5333-8333-333333333333";
const STAGING_MEMORY_ID_TWO: &str = "44444444-4444-5444-8444-444444444444";
const STAGING_FINGERPRINT_ONE: &str = "normalized-fingerprint-one";
const STAGING_FINGERPRINT_TWO: &str = "normalized-fingerprint-two";
const STAGING_OBSERVATION_ONE: &str = "55555555-5555-4555-8555-555555555555";
const STAGING_OBSERVATION_TWO: &str = "66666666-6666-4666-8666-666666666666";

#[tokio::test]
async fn load_snapshot_returns_a_current_matching_source_snapshot() {
    let source_identities = [
        ("session_start", "44444444-4444-4444-8444-444444444444"),
        ("observation", "55555555-5555-4555-8555-555555555555"),
    ];
    let claim = source_claim(&source_identities, 1, 1, AttemptState::Claimed);
    let request_claim = claim.clone();
    let response = successful_response(json!([
        lifecycle_source_row(
            "session_start",
            "44444444-4444-4444-8444-444444444444",
            "2026-09-18T10:00:00.123Z",
            "2026-09-18T10:00:00.123+00:00",
            "2026-09-18T11:00:00.123456+00:00",
        ),
        observation_source_row(
            "55555555-5555-4555-8555-555555555555",
            "2026-09-18T12:00:00.456Z",
            "2026-09-18T12:00:00.456+00:00",
            "2026-09-18T13:00:00.654321+00:00",
        ),
    ]));

    let outcome = load_snapshot_with(&database(), &claim, move |request| {
        let response = response.clone();
        let request_claim = request_claim.clone();
        async move {
            assert_source_load_request(&request, &request_claim);
            Ok(response)
        }
    })
    .await
    .expect("matching source rows should load a current snapshot");

    let SnapshotLoadOutcome::Current(loaded) = outcome else {
        panic!("matching source rows must not be superseded")
    };
    let snapshot = loaded.snapshot();
    assert!(snapshot.matches_claim(&claim));
    assert_eq!(
        loaded
            .memory_scope()
            .receipt_ids()
            .map(|receipt_id| receipt_id.as_str())
            .collect::<Vec<_>>(),
        ["55555555-5555-4555-8555-555555555555"],
        "initial processing scopes every current observation and no lifecycle receipt"
    );
    assert_eq!(
        snapshot.source_cutoff(),
        &timestamp("2026-09-18T13:00:00.654321Z")
    );
    assert_eq!(snapshot.transcript().entries().len(), 2);
    let lifecycle = &snapshot.transcript().entries()[0];
    assert_eq!(
        lifecycle.receipt_id().as_str(),
        "44444444-4444-4444-8444-444444444444"
    );
    assert_eq!(lifecycle.event_type(), SourceEventType::SessionStart);
    assert_eq!(lifecycle.session_id().as_str(), "snapshot-session");
    assert_eq!(lifecycle.project_name(), "project");
    assert_eq!(lifecycle.current_working_directory(), "/work");
    assert_eq!(
        lifecycle.source_timestamp_rfc3339(),
        "2026-09-18T10:00:00.123Z"
    );
    assert_eq!(
        lifecycle.source_timestamp_utc(),
        &timestamp("2026-09-18T10:00:00.123Z")
    );
    assert_eq!(
        lifecycle.ingested_at(),
        &timestamp("2026-09-18T11:00:00.123456Z")
    );
    assert_eq!(lifecycle.hook_type(), None);
    assert_eq!(lifecycle.observation_data(), None);
    let observation = &snapshot.transcript().entries()[1];
    assert_eq!(
        observation.receipt_id().as_str(),
        "55555555-5555-4555-8555-555555555555"
    );
    assert_eq!(observation.event_type(), SourceEventType::Observation);
    assert_eq!(observation.session_id().as_str(), "snapshot-session");
    assert_eq!(observation.hook_type(), Some("post_tool_use"));
    assert_eq!(observation.project_name(), "project");
    assert_eq!(observation.current_working_directory(), "/work");
    assert_eq!(
        observation.source_timestamp_rfc3339(),
        "2026-09-18T12:00:00.456Z"
    );
    assert_eq!(
        observation.source_timestamp_utc(),
        &timestamp("2026-09-18T12:00:00.456Z")
    );
    assert_eq!(
        observation.ingested_at(),
        &timestamp("2026-09-18T13:00:00.654321Z")
    );
    assert_eq!(
        observation.observation_data(),
        Some(&json!({"opaque": {"source": "raw-observation-secret-sentinel"}}))
    );
}

#[tokio::test]
async fn load_snapshot_preserves_duplicate_deliveries_without_deduplicating() {
    let source_identities = [
        ("session_start", "44444444-4444-4444-8444-444444444444"),
        ("observation", "55555555-5555-4555-8555-555555555555"),
        ("observation", "66666666-6666-4666-8666-666666666666"),
    ];
    let claim = source_claim(&source_identities, 1, 2, AttemptState::Claimed);
    let request_claim = claim.clone();
    let response = successful_response(json!([
        lifecycle_source_row(
            "session_start",
            "44444444-4444-4444-8444-444444444444",
            "2026-09-18T10:00:00.000Z",
            "2026-09-18T10:00:00+00:00",
            "2026-09-18T11:00:00+00:00",
        ),
        observation_source_row(
            "55555555-5555-4555-8555-555555555555",
            "2026-09-18T12:00:00.000Z",
            "2026-09-18T12:00:00+00:00",
            "2026-09-18T13:00:00+00:00",
        ),
        observation_source_row(
            "66666666-6666-4666-8666-666666666666",
            "2026-09-18T12:00:00.000Z",
            "2026-09-18T12:00:00+00:00",
            "2026-09-18T13:00:00+00:00",
        ),
    ]));

    let outcome = load_snapshot_with(&database(), &claim, move |request| {
        let request_claim = request_claim.clone();
        let response = response.clone();
        async move {
            assert_source_load_request(&request, &request_claim);
            Ok(response)
        }
    })
    .await
    .expect("duplicate deliveries should remain in a current snapshot");

    let SnapshotLoadOutcome::Current(loaded) = outcome else {
        panic!("matching duplicate deliveries must not be superseded")
    };
    let snapshot = loaded.snapshot();
    assert_eq!(snapshot.transcript().entries().len(), 3);
    assert_eq!(snapshot.transcript().lifecycle_count(), 1);
    assert_eq!(snapshot.transcript().observation_count(), 2);
    assert_eq!(
        snapshot
            .transcript()
            .entries()
            .iter()
            .map(|entry| entry.receipt_id().as_str())
            .collect::<Vec<_>>(),
        [
            "44444444-4444-4444-8444-444444444444",
            "55555555-5555-4555-8555-555555555555",
            "66666666-6666-4666-8666-666666666666",
        ]
    );
    assert_eq!(
        loaded
            .memory_scope()
            .receipt_ids()
            .map(|receipt_id| receipt_id.as_str())
            .collect::<Vec<_>>(),
        [
            "55555555-5555-4555-8555-555555555555",
            "66666666-6666-4666-8666-666666666666",
        ]
    );
}

#[tokio::test]
async fn load_snapshot_scopes_refresh_to_observations_absent_from_the_prior_projection() {
    let old_observation = "55555555-5555-4555-8555-555555555555";
    let new_observation = "66666666-6666-4666-8666-666666666666";
    let source_identities = [
        ("session_start", "44444444-4444-4444-8444-444444444444"),
        ("observation", old_observation),
        ("observation", new_observation),
    ];
    let claim = source_claim(&source_identities, 1, 2, AttemptState::Claimed);
    let request_claim = claim.clone();
    let lifecycle = lifecycle_source_row(
        "session_start",
        "44444444-4444-4444-8444-444444444444",
        "2026-09-18T10:00:00.000Z",
        "2026-09-18T10:00:00+00:00",
        "2026-09-18T11:00:00+00:00",
    );
    let old = observation_source_row(
        old_observation,
        "2026-09-18T12:00:00.000Z",
        "2026-09-18T12:00:00+00:00",
        "2026-09-18T13:00:00+00:00",
    );
    let new = observation_source_row(
        new_observation,
        "2026-09-18T14:00:00.000Z",
        "2026-09-18T14:00:00+00:00",
        "2026-09-18T15:00:00+00:00",
    );
    let prior = projection_transcript(&[lifecycle.clone(), old.clone()]);
    let response = successful_response(Value::Array(vec![
        source_row_with_prior_projection(lifecycle, prior.clone()),
        source_row_with_prior_projection(old, prior.clone()),
        source_row_with_prior_projection(new, prior),
    ]));

    let outcome = load_snapshot_with(&database(), &claim, move |request| {
        let request_claim = request_claim.clone();
        let response = response.clone();
        async move {
            assert_source_load_request(&request, &request_claim);
            Ok(response)
        }
    })
    .await
    .expect("a matching refresh snapshot should load");

    let SnapshotLoadOutcome::Current(loaded) = outcome else {
        panic!("a matching refresh snapshot must not be superseded")
    };
    assert!(loaded.snapshot().matches_claim(&claim));
    assert_eq!(
        loaded
            .memory_scope()
            .receipt_ids()
            .map(|receipt_id| receipt_id.as_str())
            .collect::<Vec<_>>(),
        [new_observation],
        "a refresh may support new memories only with observations absent from the prior projection"
    );
}

#[tokio::test]
async fn load_snapshot_rejects_duplicate_prior_observation_receipts() {
    let receipt_sentinel = "77777777-7777-4777-8777-777777777777";
    let claim = source_claim(
        &[("observation", receipt_sentinel)],
        0,
        1,
        AttemptState::Claimed,
    );
    let prior_observation = observation_source_row(
        receipt_sentinel,
        "2026-09-18T12:00:00.000Z",
        "2026-09-18T12:00:00+00:00",
        "2026-09-18T13:00:00+00:00",
    );
    let mut prior_transcript = projection_transcript(std::slice::from_ref(&prior_observation));
    prior_transcript["entries"] = json!([
        prior_observation["entry"].clone(),
        prior_observation["entry"].clone()
    ]);
    let response = successful_response(json!([source_row_with_prior_projection(
        prior_observation,
        prior_transcript,
    )]));
    let request_claim = claim.clone();

    let error = match load_snapshot_with(&database(), &claim, move |request| {
        let request_claim = request_claim.clone();
        let response = response.clone();
        async move {
            assert_source_load_request(&request, &request_claim);
            Ok(response)
        }
    })
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("duplicate prior observation receipts must be rejected"),
    };

    assert_safe_error(
        error,
        RepositoryError::invalid_response(),
        &[receipt_sentinel, "raw-observation-secret-sentinel"],
    );
}

#[tokio::test]
async fn load_snapshot_uses_all_observations_when_the_prior_projection_is_explicitly_absent() {
    let first_observation = "55555555-5555-4555-8555-555555555555";
    let second_observation = "66666666-6666-4666-8666-666666666666";
    let source_identities = [
        ("observation", first_observation),
        ("observation", second_observation),
    ];
    let claim = source_claim(&source_identities, 0, 2, AttemptState::Claimed);
    let request_claim = claim.clone();
    let response = successful_response(json!([
        observation_source_row(
            first_observation,
            "2026-09-18T12:00:00.000Z",
            "2026-09-18T12:00:00+00:00",
            "2026-09-18T13:00:00+00:00",
        ),
        observation_source_row(
            second_observation,
            "2026-09-18T14:00:00.000Z",
            "2026-09-18T14:00:00+00:00",
            "2026-09-18T15:00:00+00:00",
        ),
    ]));

    let outcome = load_snapshot_with(&database(), &claim, move |request| {
        let request_claim = request_claim.clone();
        let response = response.clone();
        async move {
            assert_source_load_request(&request, &request_claim);
            Ok(response)
        }
    })
    .await
    .expect("an initial snapshot with no prior projection should load");

    let SnapshotLoadOutcome::Current(loaded) = outcome else {
        panic!("an initial snapshot must not be superseded")
    };
    assert_eq!(
        loaded
            .memory_scope()
            .receipt_ids()
            .map(|receipt_id| receipt_id.as_str())
            .collect::<Vec<_>>(),
        [first_observation, second_observation]
    );
}

#[tokio::test]
async fn load_snapshot_scopes_a_new_observation_when_recorded_times_are_tied() {
    let old_observation = "55555555-5555-4555-8555-555555555555";
    let new_observation = "66666666-6666-4666-8666-666666666666";
    let source_identities = [
        ("observation", old_observation),
        ("observation", new_observation),
    ];
    let claim = source_claim(&source_identities, 0, 2, AttemptState::Claimed);
    let request_claim = claim.clone();
    let old = observation_source_row(
        old_observation,
        "2026-09-18T12:00:00.000Z",
        "2026-09-18T12:00:00+00:00",
        "2026-09-18T13:00:00+00:00",
    );
    let new = observation_source_row(
        new_observation,
        "2026-09-18T12:00:00.000Z",
        "2026-09-18T12:00:00+00:00",
        "2026-09-18T13:00:00+00:00",
    );
    let prior = projection_transcript(std::slice::from_ref(&old));
    let response = successful_response(Value::Array(vec![
        source_row_with_prior_projection(old, prior.clone()),
        source_row_with_prior_projection(new, prior),
    ]));

    let outcome = load_snapshot_with(&database(), &claim, move |request| {
        let request_claim = request_claim.clone();
        let response = response.clone();
        async move {
            assert_source_load_request(&request, &request_claim);
            Ok(response)
        }
    })
    .await
    .expect("a refresh with tied recorded times should load");

    let SnapshotLoadOutcome::Current(loaded) = outcome else {
        panic!("a tied-time refresh must not be superseded")
    };
    assert_eq!(
        loaded
            .memory_scope()
            .receipt_ids()
            .map(|receipt_id| receipt_id.as_str())
            .collect::<Vec<_>>(),
        [new_observation],
        "receipt-set difference must retain a new observation even when recorded times tie"
    );
}

#[tokio::test]
async fn load_snapshot_rejects_malformed_prior_transcripts_without_superseding() {
    let observation = "55555555-5555-4555-8555-555555555555";
    let claim = source_claim(&[("observation", observation)], 0, 1, AttemptState::Claimed);
    let source_row = observation_source_row(
        observation,
        "2026-09-18T12:00:00.000Z",
        "2026-09-18T12:00:00+00:00",
        "2026-09-18T13:00:00+00:00",
    );
    let mut unknown_field = projection_transcript(std::slice::from_ref(&source_row));
    unknown_field["unexpected"] = json!("prior-transcript-secret-sentinel");
    let mut invalid_entry = source_row.clone();
    invalid_entry["entry"]["data"] = json!("prior-transcript-secret-sentinel");
    let mut wrong_session = source_row.clone();
    wrong_session["entry"]["session_id"] = json!("other-session-secret-sentinel");
    let malformed_transcripts = [
        json!(null),
        json!({"entries": []}),
        unknown_field,
        projection_transcript(&[source_row.clone(), source_row.clone()]),
        projection_transcript(&[invalid_entry]),
        projection_transcript(&[wrong_session]),
    ];

    for transcript in malformed_transcripts {
        let request_claim = claim.clone();
        let response = successful_response(json!([source_row_with_prior_projection(
            source_row.clone(),
            transcript,
        )]));
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocations_for_call = Arc::clone(&invocations);

        let error = load_snapshot_with(&database(), &claim, move |request| {
            let request_claim = request_claim.clone();
            let response = response.clone();
            let invocations = Arc::clone(&invocations_for_call);
            async move {
                assert_eq!(
                    invocations.fetch_add(1, Ordering::SeqCst),
                    0,
                    "malformed prior projections must not issue a supersede update"
                );
                assert_source_load_request(&request, &request_claim);
                Ok(response)
            }
        })
        .await
        .expect_err("malformed prior transcripts must remain opaque repository errors");

        assert_safe_error(
            error,
            RepositoryError::invalid_response(),
            &[
                "raw-observation-secret-sentinel",
                "prior-transcript-secret-sentinel",
                "other-session-secret-sentinel",
            ],
        );
        assert_eq!(invocations.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn load_snapshot_maps_an_empty_fenced_read_to_an_unconfirmed_commit_without_external_work() {
    let claim = source_claim(
        &[("session_start", "44444444-4444-4444-8444-444444444444")],
        1,
        0,
        AttemptState::Claimed,
    );
    let request_claim = claim.clone();
    let invocations = Arc::new(AtomicUsize::new(0));
    let invocations_for_call = Arc::clone(&invocations);

    let error = load_snapshot_with(&database(), &claim, move |request| {
        let request_claim = request_claim.clone();
        let invocations = Arc::clone(&invocations_for_call);
        async move {
            assert_eq!(
                invocations.fetch_add(1, Ordering::SeqCst),
                0,
                "a stale fenced read must not issue a supersede update or external work"
            );
            assert_source_load_request(&request, &request_claim);
            Ok(successful_response(json!([])))
        }
    })
    .await
    .expect_err("a stale fenced read must not be reported as superseded");

    assert_safe_error(error, RepositoryError::unconfirmed_commit(), &[]);
    assert_eq!(invocations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn load_snapshot_supersedes_a_claim_when_a_late_source_row_changes_counts() {
    let claimed_source = [("session_start", "44444444-4444-4444-8444-444444444444")];
    let claim = source_claim(&claimed_source, 1, 0, AttemptState::Claimed);
    let request_claim = claim.clone();
    let source_response = successful_response(json!([
        lifecycle_source_row(
            "session_start",
            "44444444-4444-4444-8444-444444444444",
            "2026-09-18T10:00:00.000Z",
            "2026-09-18T10:00:00+00:00",
            "2026-09-18T11:00:00+00:00",
        ),
        observation_source_row(
            "55555555-5555-4555-8555-555555555555",
            "2026-09-18T12:00:00.000Z",
            "2026-09-18T12:00:00+00:00",
            "2026-09-18T13:00:00+00:00",
        ),
    ]));
    let invocations = Arc::new(AtomicUsize::new(0));
    let invocations_for_call = Arc::clone(&invocations);

    let outcome = load_snapshot_with(&database(), &claim, move |request| {
        let request_claim = request_claim.clone();
        let source_response = source_response.clone();
        let invocations = Arc::clone(&invocations_for_call);
        async move {
            match invocations.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    assert_source_load_request(&request, &request_claim);
                    Ok(source_response)
                }
                1 => {
                    assert_supersede_request(&request, &request_claim);
                    Ok(successful_response(json!([{"superseded": true}])))
                }
                _ => panic!("a source conflict must perform only one read and one fenced write"),
            }
        }
    })
    .await
    .expect("a confirmed fenced supersede should be terminal");

    assert_eq!(outcome, SnapshotLoadOutcome::Superseded);
    assert_eq!(invocations.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn load_snapshot_supersedes_a_claim_when_same_counts_have_different_identities() {
    let claimed_source = [("session_start", "44444444-4444-4444-8444-444444444444")];
    let claim = source_claim(&claimed_source, 1, 0, AttemptState::Claimed);
    let request_claim = claim.clone();
    let source_response = successful_response(json!([lifecycle_source_row(
        "session_end",
        "55555555-5555-4555-8555-555555555555",
        "2026-09-18T12:00:00.000Z",
        "2026-09-18T12:00:00+00:00",
        "2026-09-18T13:00:00+00:00",
    )]));
    let invocations = Arc::new(AtomicUsize::new(0));
    let invocations_for_call = Arc::clone(&invocations);

    let outcome = load_snapshot_with(&database(), &claim, move |request| {
        let request_claim = request_claim.clone();
        let source_response = source_response.clone();
        let invocations = Arc::clone(&invocations_for_call);
        async move {
            match invocations.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    assert_source_load_request(&request, &request_claim);
                    Ok(source_response)
                }
                1 => {
                    assert_supersede_request(&request, &request_claim);
                    Ok(successful_response(json!([{"superseded": true}])))
                }
                _ => panic!("a source conflict must perform only one read and one fenced write"),
            }
        }
    })
    .await
    .expect("different source identities must supersede the stale claim");

    assert_eq!(outcome, SnapshotLoadOutcome::Superseded);
    assert_eq!(invocations.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn load_snapshot_never_reports_superseded_without_a_confirmed_fenced_update() {
    let claimed_source = [("session_start", "44444444-4444-4444-8444-444444444444")];
    let source_response = successful_response(json!([
        lifecycle_source_row(
            "session_start",
            "44444444-4444-4444-8444-444444444444",
            "2026-09-18T10:00:00.000Z",
            "2026-09-18T10:00:00+00:00",
            "2026-09-18T11:00:00+00:00",
        ),
        observation_source_row(
            "55555555-5555-4555-8555-555555555555",
            "2026-09-18T12:00:00.000Z",
            "2026-09-18T12:00:00+00:00",
            "2026-09-18T13:00:00+00:00",
        ),
    ]));
    let mut unknown_envelope = successful_response(json!([{"superseded": true}]));
    unknown_envelope["backend_payload"] = json!("response-secret-sentinel");

    for (supersede_response, expected_error) in [
        (
            successful_response(json!([])),
            RepositoryError::unconfirmed_commit(),
        ),
        (
            successful_response(json!([{"superseded": false}])),
            RepositoryError::invalid_response(),
        ),
        (
            successful_response(json!([{"superseded": true, "unexpected": true}])),
            RepositoryError::invalid_response(),
        ),
        (unknown_envelope, RepositoryError::invalid_response()),
    ] {
        let claim = source_claim(&claimed_source, 1, 0, AttemptState::Claimed);
        let request_claim = claim.clone();
        let source_response = source_response.clone();
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocations_for_call = Arc::clone(&invocations);

        let error = load_snapshot_with(&database(), &claim, move |request| {
            let request_claim = request_claim.clone();
            let source_response = source_response.clone();
            let supersede_response = supersede_response.clone();
            let invocations = Arc::clone(&invocations_for_call);
            async move {
                match invocations.fetch_add(1, Ordering::SeqCst) {
                    0 => {
                        assert_source_load_request(&request, &request_claim);
                        Ok(source_response)
                    }
                    1 => {
                        assert_supersede_request(&request, &request_claim);
                        Ok(supersede_response)
                    }
                    _ => panic!("a source conflict must not perform more than one fenced update"),
                }
            }
        })
        .await
        .expect_err("unconfirmed or malformed supersession must remain an opaque error");

        assert_safe_error(error, expected_error, &["response-secret-sentinel"]);
        assert_eq!(invocations.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn load_snapshot_preserves_reclaimed_staged_and_publishing_progress_on_mismatch() {
    let source_identities = [("session_start", "44444444-4444-4444-8444-444444444444")];
    let source_response = successful_response(json!([
        lifecycle_source_row(
            "session_start",
            "44444444-4444-4444-8444-444444444444",
            "2026-09-18T10:00:00.000Z",
            "2026-09-18T10:00:00+00:00",
            "2026-09-18T11:00:00+00:00",
        ),
        observation_source_row(
            "55555555-5555-4555-8555-555555555555",
            "2026-09-18T12:00:00.000Z",
            "2026-09-18T12:00:00+00:00",
            "2026-09-18T13:00:00+00:00",
        ),
    ]));

    for state in [AttemptState::Staged, AttemptState::Publishing] {
        let claim = source_claim(&source_identities, 1, 0, state);
        let request_claim = claim.clone();
        let source_response = source_response.clone();
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocations_for_call = Arc::clone(&invocations);

        let error = load_snapshot_with(&database(), &claim, move |request| {
            let request_claim = request_claim.clone();
            let source_response = source_response.clone();
            let invocations = Arc::clone(&invocations_for_call);
            async move {
                assert_eq!(
                    invocations.fetch_add(1, Ordering::SeqCst),
                    0,
                    "staged and publishing mismatch must not issue a supersede update"
                );
                assert_source_load_request(&request, &request_claim);
                Ok(source_response)
            }
        })
        .await
        .expect_err("a mismatched retained-progress attempt must not expose a stale snapshot");

        assert_eq!(error, RepositoryError::unconfirmed_commit());
        assert_eq!(invocations.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn load_snapshot_rejects_malformed_source_envelopes_and_entries_opaquely() {
    let source_identities = [("session_start", "44444444-4444-4444-8444-444444444444")];
    let claim = source_claim(&source_identities, 1, 0, AttemptState::Claimed);
    let valid_row = lifecycle_source_row(
        "session_start",
        "44444444-4444-4444-8444-444444444444",
        "2026-09-18T10:00:00.000Z",
        "2026-09-18T10:00:00+00:00",
        "2026-09-18T11:00:00+00:00",
    );
    let mut unknown_envelope = successful_response(json!([valid_row.clone()]));
    unknown_envelope["backend_payload"] = json!("response-secret-sentinel");
    let mut unknown_entry = valid_row.clone();
    unknown_entry["entry"]["unexpected"] = json!(true);
    let mut multiple_fields = valid_row.clone();
    multiple_fields["unexpected"] = json!(true);
    let mut invalid_variant = valid_row.clone();
    invalid_variant["entry"]["event_type"] = json!("observation");
    let mut invalid_timestamp = valid_row.clone();
    invalid_timestamp["entry"]["source_timestamp_rfc3339"] = json!("2026-09-18T10:00:00Z");
    let mut invalid_receipt = valid_row.clone();
    invalid_receipt["entry"]["receipt_id"] = json!("receipt-secret-sentinel");
    let mut wrong_session = valid_row.clone();
    wrong_session["entry"]["session_id"] = json!("other-session");
    let mut invalid_data = observation_source_row(
        "55555555-5555-4555-8555-555555555555",
        "2026-09-18T12:00:00.000Z",
        "2026-09-18T12:00:00+00:00",
        "2026-09-18T13:00:00+00:00",
    );
    invalid_data["entry"]["data"] = json!("raw-observation-secret-sentinel");
    let mut transcript_without_prior_projection = valid_row.clone();
    transcript_without_prior_projection["prior_transcript"] =
        projection_transcript(std::slice::from_ref(&valid_row));
    transcript_without_prior_projection["prior_projection_present"] = json!(false);

    for response in [
        unknown_envelope,
        successful_response(json!([multiple_fields])),
        successful_response(json!([unknown_entry])),
        successful_response(json!([invalid_variant])),
        successful_response(json!([invalid_timestamp])),
        successful_response(json!([invalid_receipt])),
        successful_response(json!([wrong_session])),
        successful_response(json!([invalid_data])),
        successful_response(json!([transcript_without_prior_projection])),
    ] {
        let request_claim = claim.clone();
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocations_for_call = Arc::clone(&invocations);
        let error = load_snapshot_with(&database(), &claim, move |request| {
            let request_claim = request_claim.clone();
            let response = response.clone();
            let invocations = Arc::clone(&invocations_for_call);
            async move {
                assert_eq!(
                    invocations.fetch_add(1, Ordering::SeqCst),
                    0,
                    "invalid source responses must not issue a supersede update"
                );
                assert_source_load_request(&request, &request_claim);
                Ok(response)
            }
        })
        .await
        .expect_err("malformed source data must not produce a snapshot or supersession");

        assert_safe_error(
            error,
            RepositoryError::invalid_response(),
            &[
                "response-secret-sentinel",
                "receipt-secret-sentinel",
                "raw-observation-secret-sentinel",
                "other-session",
            ],
        );
        assert_eq!(invocations.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn claim_eligible_claims_a_fresh_candidate_with_generated_fencing_identifiers() {
    let now = timestamp("2026-09-22T12:00:00.987Z");
    let source_identities = [
        ("session_end", "44444444-4444-4444-8444-444444444444"),
        ("observation", "55555555-5555-4555-8555-555555555555"),
    ];
    let expected_revision = expected_source_revision(&source_identities);
    assert_eq!(
        expected_revision,
        expected_source_revision(&[
            ("observation", "55555555-5555-4555-8555-555555555555"),
            ("session_end", "44444444-4444-4444-8444-444444444444"),
        ]),
        "source revision identity must not depend on source delivery order"
    );
    let expected_revision_for_call = expected_revision.clone();
    let invocations = Arc::new(AtomicUsize::new(0));
    let invocations_for_call = Arc::clone(&invocations);

    let claims = claim_eligible_with(
        &database(),
        now,
        1,
        Duration::from_secs(60),
        move |request| {
            let invocations = Arc::clone(&invocations_for_call);
            let expected_revision = expected_revision_for_call.clone();
            async move {
                match invocations.fetch_add(1, Ordering::SeqCst) {
                    0 => {
                        assert_candidate_request(request, "2026-09-22T12:00:00Z", 1);
                        Ok(successful_response(json!([candidate_row(
                            "fresh-session",
                            "1",
                            "1",
                            &source_identities,
                        )])))
                    }
                    1 => {
                        assert_claim_request(
                            &request,
                            "fresh-session",
                            &expected_revision,
                            "1",
                            "1",
                            "2026-09-22T12:00:00Z",
                            "2026-09-22T12:01:00Z",
                        );
                        let params = request
                            .payload
                            .get("params")
                            .and_then(Value::as_array)
                            .expect("claim request should contain parameters");
                        assert_uuid_v7(params[4].as_str().expect("attempt ID should be text"));
                        assert_uuid_v7(params[5].as_str().expect("lease token should be text"));
                        Ok(successful_response(json!([claim_row(
                            params,
                            params[4].as_str().expect("attempt ID should be text"),
                            "claimed",
                        )])))
                    }
                    _ => panic!("claim should use one read and one atomic write"),
                }
            }
        },
    )
    .await
    .expect("fresh candidate should be claimed");

    assert_eq!(claims.len(), 1, "a prepared candidate should be claimed");
    assert_eq!(claims[0].state(), AttemptState::Claimed);
    assert_eq!(claims[0].source_revision().as_str(), expected_revision);
    assert_uuid_v7(claims[0].attempt_id().as_str());
    assert_uuid_v7(claims[0].lease_token().as_str());
    assert_eq!(
        claims[0].lease_expires_at(),
        &timestamp("2026-09-22T12:01:00Z")
    );
    assert_eq!(invocations.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn claim_eligible_limits_identity_reads_and_skips_claim_writes_for_an_empty_sweep() {
    let invocations = Arc::new(AtomicUsize::new(0));
    let invocations_for_call = Arc::clone(&invocations);

    let claims = claim_eligible_with(
        &database(),
        timestamp("2026-09-22T12:00:00.987Z"),
        2,
        Duration::from_secs(60),
        move |request| {
            let invocations = Arc::clone(&invocations_for_call);
            async move {
                assert_eq!(
                    invocations.fetch_add(1, Ordering::SeqCst),
                    0,
                    "an empty sweep must not issue claim DML"
                );
                assert_candidate_request(request, "2026-09-22T12:00:00Z", 2);
                Ok(successful_response(json!([])))
            }
        },
    )
    .await
    .expect("an empty candidate read should succeed");

    assert!(claims.is_empty());
    assert_eq!(invocations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn claim_eligible_reclaims_due_retryable_work_with_its_attempt_identity_and_a_new_token() {
    let source_identities = [("session_end", "44444444-4444-4444-8444-444444444444")];
    let expected_revision = expected_source_revision(&source_identities);
    let invocations = Arc::new(AtomicUsize::new(0));
    let invocations_for_call = Arc::clone(&invocations);

    let claims = claim_eligible_with(
        &database(),
        timestamp("2026-09-22T12:00:00Z"),
        1,
        Duration::from_secs(90),
        move |request| {
            let invocations = Arc::clone(&invocations_for_call);
            let expected_revision = expected_revision.clone();
            async move {
                match invocations.fetch_add(1, Ordering::SeqCst) {
                    0 => Ok(successful_response(json!([candidate_row(
                        "retryable-session",
                        "1",
                        "0",
                        &source_identities,
                    )]))),
                    1 => {
                        assert_claim_request(
                            &request,
                            "retryable-session",
                            &expected_revision,
                            "1",
                            "0",
                            "2026-09-22T12:00:00Z",
                            "2026-09-22T12:01:30Z",
                        );
                        let params = request.payload["params"]
                            .as_array()
                            .expect("claim request should contain parameters");
                        assert_ne!(params[5], json!(RETAINED_LEASE_TOKEN));
                        Ok(successful_response(json!([claim_row(
                            params,
                            RETAINED_ATTEMPT_ID,
                            "claimed",
                        )])))
                    }
                    _ => panic!("claim should use one read and one atomic write"),
                }
            }
        },
    )
    .await
    .expect("due retryable work should be reclaimed");

    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].attempt_id().as_str(), RETAINED_ATTEMPT_ID);
    assert_eq!(claims[0].state(), AttemptState::Claimed);
    assert_ne!(claims[0].lease_token().as_str(), RETAINED_LEASE_TOKEN);
    assert_uuid_v7(claims[0].lease_token().as_str());
}

#[tokio::test]
async fn claim_eligible_reclaims_a_due_unfinished_attempt_when_candidate_source_is_newer() {
    let retained_source = [("session_end", "44444444-4444-4444-8444-444444444444")];
    let candidate_source = [
        ("session_end", "44444444-4444-4444-8444-444444444444"),
        ("observation", "55555555-5555-4555-8555-555555555555"),
    ];
    let retained_revision = expected_source_revision(&retained_source);
    let candidate_revision = expected_source_revision(&candidate_source);
    assert_ne!(retained_revision, candidate_revision);
    let invocations = Arc::new(AtomicUsize::new(0));
    let invocations_for_call = Arc::clone(&invocations);
    let retained_revision_for_call = retained_revision.clone();

    let claims = claim_eligible_with(
        &database(),
        timestamp("2026-09-22T12:00:00Z"),
        1,
        Duration::from_secs(60),
        move |request| {
            let invocations = Arc::clone(&invocations_for_call);
            let retained_revision = retained_revision_for_call.clone();
            let candidate_revision = candidate_revision.clone();
            async move {
                match invocations.fetch_add(1, Ordering::SeqCst) {
                    0 => Ok(successful_response(json!([candidate_row(
                        "late-source-session",
                        "1",
                        "1",
                        &candidate_source,
                    )]))),
                    1 => {
                        assert_claim_request(
                            &request,
                            "late-source-session",
                            &candidate_revision,
                            "1",
                            "1",
                            "2026-09-22T12:00:00Z",
                            "2026-09-22T12:01:00Z",
                        );
                        let params = request.payload["params"]
                            .as_array()
                            .expect("claim request should contain parameters");
                        assert_eq!(params.len(), 8);
                        Ok(successful_response(json!([reclaimed_claim_row(
                            params,
                            RETAINED_ATTEMPT_ID,
                            &retained_revision,
                            "1",
                            "0",
                            "claimed",
                        )])))
                    }
                    _ => panic!("claim should use one read and one atomic write"),
                }
            }
        },
    )
    .await
    .expect("due unfinished work should be reclaimed before a newer source revision can insert");

    assert_eq!(
        claims.len(),
        1,
        "a reclaim must return the existing attempt so task 2.3 can supersede it and unblock the newer revision"
    );
    assert_eq!(claims[0].attempt_id().as_str(), RETAINED_ATTEMPT_ID);
    assert_eq!(claims[0].source_revision().as_str(), retained_revision);
    assert_eq!(claims[0].lifecycle_count(), 1);
    assert_eq!(claims[0].observation_count(), 0);
    assert_eq!(claims[0].state(), AttemptState::Claimed);
    assert_eq!(invocations.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn claim_eligible_reclaims_expired_staged_and_publishing_work_without_resetting_state() {
    let staged_source = [("session_end", "44444444-4444-4444-8444-444444444444")];
    let publishing_source = [("observation", "55555555-5555-4555-8555-555555555555")];
    let staged_revision = expected_source_revision(&staged_source);
    let publishing_revision = expected_source_revision(&publishing_source);
    let invocations = Arc::new(AtomicUsize::new(0));
    let invocations_for_call = Arc::clone(&invocations);

    let claims = claim_eligible_with(
        &database(),
        timestamp("2026-09-22T12:00:00Z"),
        2,
        Duration::from_secs(60),
        move |request| {
            let invocations = Arc::clone(&invocations_for_call);
            let staged_revision = staged_revision.clone();
            let publishing_revision = publishing_revision.clone();
            async move {
                match invocations.fetch_add(1, Ordering::SeqCst) {
                    0 => Ok(successful_response(json!([
                        candidate_row("staged-session", "1", "0", &staged_source),
                        candidate_row("publishing-session", "0", "1", &publishing_source),
                    ]))),
                    1 => {
                        assert_claim_request(
                            &request,
                            "staged-session",
                            &staged_revision,
                            "1",
                            "0",
                            "2026-09-22T12:00:00Z",
                            "2026-09-22T12:01:00Z",
                        );
                        let params = request.payload["params"]
                            .as_array()
                            .expect("claim request should contain parameters");
                        assert_ne!(params[5], json!(STAGED_LEASE_TOKEN));
                        Ok(successful_response(json!([claim_row(
                            params,
                            STAGED_ATTEMPT_ID,
                            "staged",
                        )])))
                    }
                    2 => {
                        assert_claim_request(
                            &request,
                            "publishing-session",
                            &publishing_revision,
                            "0",
                            "1",
                            "2026-09-22T12:00:00Z",
                            "2026-09-22T12:01:00Z",
                        );
                        let params = request.payload["params"]
                            .as_array()
                            .expect("claim request should contain parameters");
                        assert_ne!(params[5], json!(PUBLISHING_LEASE_TOKEN));
                        Ok(successful_response(json!([claim_row(
                            params,
                            PUBLISHING_ATTEMPT_ID,
                            "publishing",
                        )])))
                    }
                    _ => panic!("each prepared candidate requires exactly one claim DML"),
                }
            }
        },
    )
    .await
    .expect("expired staged and publishing work should be reclaimed");

    assert_eq!(claims.len(), 2);
    assert_eq!(claims[0].attempt_id().as_str(), STAGED_ATTEMPT_ID);
    assert_eq!(claims[0].state(), AttemptState::Staged);
    assert_ne!(claims[0].lease_token().as_str(), STAGED_LEASE_TOKEN);
    assert_eq!(claims[1].attempt_id().as_str(), PUBLISHING_ATTEMPT_ID);
    assert_eq!(claims[1].state(), AttemptState::Publishing);
    assert_ne!(claims[1].lease_token().as_str(), PUBLISHING_LEASE_TOKEN);
}

#[tokio::test]
async fn claim_eligible_rejects_an_empty_limit_before_any_database_call() {
    let error = claim_eligible_with(
        &database(),
        timestamp("2026-09-22T12:00:00Z"),
        0,
        Duration::from_secs(60),
        |_| async { panic!("a nonpositive claim limit must not reach the database") },
    )
    .await
    .expect_err("zero claim limit should be rejected");

    assert_safe_error(error, RepositoryError::invalid_response(), &[]);
}

#[tokio::test]
async fn claim_eligible_rejects_malformed_or_excess_candidate_rows_without_writing() {
    let valid = candidate_row(
        "candidate-session",
        "1",
        "0",
        &[("session_end", "44444444-4444-4444-8444-444444444444")],
    );
    let duplicate_identity = candidate_row(
        "candidate-session",
        "2",
        "0",
        &[
            ("session_end", "44444444-4444-4444-8444-444444444444"),
            ("session_end", "44444444-4444-4444-8444-444444444444"),
        ],
    );
    let count_mismatch = candidate_row(
        "candidate-session",
        "2",
        "0",
        &[("session_end", "44444444-4444-4444-8444-444444444444")],
    );
    let overflow = candidate_row(
        "candidate-session",
        "18446744073709551616",
        "0",
        &[("session_end", "44444444-4444-4444-8444-444444444444")],
    );
    let negative = candidate_row(
        "candidate-session",
        "-1",
        "0",
        &[("session_end", "44444444-4444-4444-8444-444444444444")],
    );
    let mut unknown_envelope = successful_response(json!([]));
    unknown_envelope["backend_payload"] = json!("response-secret-sentinel");

    let responses = [
        json!(null),
        unknown_envelope,
        successful_response(json!([overflow])),
        successful_response(json!([negative])),
        successful_response(json!([count_mismatch])),
        successful_response(json!([duplicate_identity])),
        successful_response(json!([valid.clone(), valid.clone()])),
    ];

    for response in responses {
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocations_for_call = Arc::clone(&invocations);
        let error = claim_eligible_with(
            &database(),
            timestamp("2026-09-22T12:00:00Z"),
            2,
            Duration::from_secs(60),
            move |_| {
                let invocations = Arc::clone(&invocations_for_call);
                let response = response.clone();
                async move {
                    assert_eq!(invocations.fetch_add(1, Ordering::SeqCst), 0);
                    Ok(response)
                }
            },
        )
        .await
        .expect_err("invalid candidate response should be rejected");

        assert_safe_error(
            error,
            RepositoryError::invalid_response(),
            &["response-secret-sentinel"],
        );
        assert_eq!(invocations.load(Ordering::SeqCst), 1);
    }

    let invocations = Arc::new(AtomicUsize::new(0));
    let invocations_for_call = Arc::clone(&invocations);
    let error = claim_eligible_with(
        &database(),
        timestamp("2026-09-22T12:00:00Z"),
        1,
        Duration::from_secs(60),
        move |_| {
            let invocations = Arc::clone(&invocations_for_call);
            let valid = valid.clone();
            async move {
                assert_eq!(invocations.fetch_add(1, Ordering::SeqCst), 0);
                Ok(successful_response(json!([
                    valid,
                    candidate_row(
                        "other-session",
                        "1",
                        "0",
                        &[("session_end", "55555555-5555-4555-8555-555555555555")],
                    ),
                ])))
            }
        },
    )
    .await
    .expect_err("candidate rows above the requested limit should be rejected");
    assert_safe_error(error, RepositoryError::invalid_response(), &[]);
    assert_eq!(invocations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn claim_eligible_rejects_malformed_claim_responses_and_maps_invocation_failures_opaquely() {
    let candidate = candidate_row(
        "claimed-session",
        "1",
        "0",
        &[("session_end", "44444444-4444-4444-8444-444444444444")],
    );
    let mut unknown_envelope = successful_response(json!([]));
    unknown_envelope["backend_payload"] = json!("response-secret-sentinel");
    let malformed_responses = [
        unknown_envelope,
        successful_response(json!([{"attempt_id": "response-secret-sentinel"}])),
        successful_response(json!([
            {"attempt_id": "first-response-secret-sentinel"},
            {"attempt_id": "second-response-secret-sentinel"},
        ])),
        json!({
            "affected_rows": 1,
            "last_insert_id": null,
            "returned_rows": [],
        }),
    ];

    for response in malformed_responses {
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocations_for_call = Arc::clone(&invocations);
        let candidate = candidate.clone();
        let error = claim_eligible_with(
            &database(),
            timestamp("2026-09-22T12:00:00Z"),
            1,
            Duration::from_secs(60),
            move |_| {
                let invocations = Arc::clone(&invocations_for_call);
                let candidate = candidate.clone();
                let response = response.clone();
                async move {
                    if invocations.fetch_add(1, Ordering::SeqCst) == 0 {
                        Ok(successful_response(json!([candidate])))
                    } else {
                        Ok(response)
                    }
                }
            },
        )
        .await
        .expect_err("malformed claim response should be rejected");

        assert_safe_error(
            error,
            RepositoryError::invalid_response(),
            &[
                "response-secret-sentinel",
                "first-response-secret-sentinel",
                "second-response-secret-sentinel",
            ],
        );
        assert_eq!(invocations.load(Ordering::SeqCst), 2);
    }

    let error = claim_eligible_with(
        &database(),
        timestamp("2026-09-22T12:00:00Z"),
        1,
        Duration::from_secs(60),
        |_| async {
            Err(IiiError::Remote {
                code: "DATABASE_UNAVAILABLE".to_owned(),
                message: "database-password-sentinel".to_owned(),
                stacktrace: Some("backend-payload-sentinel".to_owned()),
            })
        },
    )
    .await
    .expect_err("claim invocation failures should be mapped");
    assert_safe_error(
        error,
        RepositoryError::invocation(),
        &["database-password-sentinel", "backend-payload-sentinel"],
    );
}

#[tokio::test]
async fn local_double_allows_one_concurrent_claim_owner_while_real_postgres_race_proof_is_deferred_to_task_5_3()
 {
    let claimed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let source_identities = [("session_end", "44444444-4444-4444-8444-444444444444")];
    let first_claimed = Arc::clone(&claimed);
    let second_claimed = Arc::clone(&claimed);
    let database = database();

    let first = claim_eligible_with(
        &database,
        timestamp("2026-09-22T12:00:00Z"),
        1,
        Duration::from_secs(60),
        move |request| {
            let claimed = Arc::clone(&first_claimed);
            async move {
                let params = request.payload["params"]
                    .as_array()
                    .expect("claim request should contain parameters");
                if params.len() == 2 {
                    return Ok(successful_response(json!([candidate_row(
                        "concurrent-session",
                        "1",
                        "0",
                        &source_identities,
                    )])));
                }

                tokio::task::yield_now().await;
                if claimed
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    Ok(successful_response(json!([claim_row(
                        params,
                        params[4].as_str().expect("attempt ID should be text"),
                        "claimed",
                    )])))
                } else {
                    Ok(successful_response(json!([])))
                }
            }
        },
    );
    let second = claim_eligible_with(
        &database,
        timestamp("2026-09-22T12:00:00Z"),
        1,
        Duration::from_secs(60),
        move |request| {
            let claimed = Arc::clone(&second_claimed);
            async move {
                let params = request.payload["params"]
                    .as_array()
                    .expect("claim request should contain parameters");
                if params.len() == 2 {
                    return Ok(successful_response(json!([candidate_row(
                        "concurrent-session",
                        "1",
                        "0",
                        &source_identities,
                    )])));
                }

                tokio::task::yield_now().await;
                if claimed
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    Ok(successful_response(json!([claim_row(
                        params,
                        params[4].as_str().expect("attempt ID should be text"),
                        "claimed",
                    )])))
                } else {
                    Ok(successful_response(json!([])))
                }
            }
        },
    );

    let (first, second) = tokio::join!(first, second);
    let owner_count = first.expect("first local claim should succeed").len()
        + second.expect("second local claim should succeed").len();

    assert_eq!(
        owner_count, 1,
        "the local atomic-DML double allows one owner"
    );
}

#[tokio::test]
async fn renew_requires_the_active_fencing_token_and_preserves_it_on_success() {
    let claim = retained_claim();
    let request_claim = claim.clone();
    let now = timestamp("2026-09-22T12:00:00.987Z");
    let renewed = renew_with(
        &database(),
        &claim,
        now,
        Duration::from_secs(90),
        move |request| async move {
            assert_renew_request(
                request,
                &request_claim,
                "2026-09-22T12:00:00Z",
                "2026-09-22T12:01:30Z",
            );
            Ok(successful_response(json!([{"renewed": true}])))
        },
    )
    .await
    .expect("active claim should renew");

    assert!(renewed);
    assert_eq!(claim.lease_token().as_str(), RETAINED_LEASE_TOKEN);
}

#[tokio::test]
async fn renew_returns_false_for_stale_owners_and_rejects_malformed_responses() {
    let claim = retained_claim();
    let request_claim = claim.clone();
    let stale = renew_with(
        &database(),
        &claim,
        timestamp("2026-09-22T12:00:00Z"),
        Duration::from_secs(60),
        move |request| async move {
            assert_renew_request(
                request,
                &request_claim,
                "2026-09-22T12:00:00Z",
                "2026-09-22T12:01:00Z",
            );
            Ok(successful_response(json!([])))
        },
    )
    .await
    .expect("a stale token should be a normal false result");
    assert!(!stale);

    for response in [
        json!({
            "affected_rows": 1,
            "last_insert_id": null,
            "returned_rows": [{"renewed": false}],
            "backend_payload": "response-secret-sentinel",
        }),
        successful_response(json!([{"renewed": false}])),
        successful_response(json!([{"renewed": true, "unexpected": true}])),
        successful_response(json!([{"renewed": true}, {"renewed": true}])),
    ] {
        let claim = retained_claim();
        let error = renew_with(
            &database(),
            &claim,
            timestamp("2026-09-22T12:00:00Z"),
            Duration::from_secs(60),
            |_| async move { Ok(response) },
        )
        .await
        .expect_err("malformed renewal response should be rejected");
        assert_safe_error(
            error,
            RepositoryError::invalid_response(),
            &["response-secret-sentinel"],
        );
    }
}

#[tokio::test]
async fn discovery_dispatches_production_eligibility_sql_and_returns_validated_session_identities()
{
    let now = timestamp("2026-09-22T12:00:00Z");
    let invocations = Arc::new(AtomicUsize::new(0));
    let invocations_for_call = Arc::clone(&invocations);

    let session_ids = discover_eligible_with(&database(), now, move |request| {
        invocations_for_call.fetch_add(1, Ordering::SeqCst);
        assert_request(request, now);
        async {
            Ok(successful_response(json!([
                {"session_id": "end-only-session"},
                {"session_id": "inactive-boundary-session"},
            ])))
        }
    })
    .await
    .expect("valid discovery response should succeed");

    assert_eq!(
        session_ids
            .iter()
            .map(|session_id| session_id.as_str())
            .collect::<Vec<_>>(),
        ["end-only-session", "inactive-boundary-session"]
    );
    assert_eq!(invocations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn discovery_and_claim_eligibility_accept_native_session_rows() {
    let now = timestamp("2026-09-22T12:00:00Z");
    let discovered = discover_eligible_with(&database(), now, move |request| {
        assert_request(request, now);
        async { Ok(successful_response(json!([]))) }
    })
    .await
    .expect("native session discovery should succeed");

    assert!(discovered.is_empty());

    let claims = claim_eligible_with(
        &database(),
        now,
        2,
        Duration::from_secs(60),
        |request| async move {
            assert_candidate_request(request, "2026-09-22T12:00:00Z", 2);
            Ok(successful_response(json!([])))
        },
    )
    .await
    .expect("native session claim eligibility should succeed");

    assert!(claims.is_empty());
}

#[tokio::test]
async fn discovery_accepts_an_empty_successful_result() {
    let sessions =
        discover_eligible_with(&database(), timestamp("2026-09-22T12:00:00Z"), |_| async {
            Ok(successful_response(json!([])))
        })
        .await
        .expect("empty discovery response should succeed");

    assert!(sessions.is_empty());
}

#[tokio::test]
async fn discovery_maps_backend_failures_to_an_opaque_repository_error() {
    let error = discover_eligible_with(&database(), timestamp("2026-09-22T12:00:00Z"), |_| async {
        Err(IiiError::Remote {
            code: "DATABASE_UNAVAILABLE".to_owned(),
            message: "database-password-sentinel".to_owned(),
            stacktrace: Some("backend-payload-sentinel".to_owned()),
        })
    })
    .await
    .expect_err("backend failures should be mapped");

    assert_safe_error(
        error,
        RepositoryError::invocation(),
        &["database-password-sentinel", "backend-payload-sentinel"],
    );
}

#[tokio::test]
async fn discovery_rejects_malformed_database_envelopes_without_leaking_them() {
    let malformed_responses = [
        json!(null),
        json!({"affected_rows": 0, "last_insert_id": null}),
        json!({"affected_rows": "1", "last_insert_id": null, "returned_rows": []}),
        json!({"affected_rows": 1, "last_insert_id": "unexpected-id", "returned_rows": []}),
        json!({"affected_rows": 1, "last_insert_id": null, "returned_rows": {}}),
    ];

    for response in malformed_responses {
        let error = discover_eligible_with(
            &database(),
            timestamp("2026-09-22T12:00:00Z"),
            |_| async move { Ok(response) },
        )
        .await
        .expect_err("malformed discovery envelope should fail");
        assert_safe_error(
            error,
            RepositoryError::invalid_response(),
            &["response-secret-sentinel"],
        );
    }
}

#[tokio::test]
async fn discovery_rejects_otherwise_valid_unknown_envelope_fields_without_leaking_them() {
    let error = discover_eligible_with(&database(), timestamp("2026-09-22T12:00:00Z"), |_| async {
        Ok(json!({
            "affected_rows": 1,
            "last_insert_id": null,
            "returned_rows": [{"session_id": "eligible-session"}],
            "backend_payload": "response-secret-sentinel",
        }))
    })
    .await
    .expect_err("otherwise valid discovery envelope with an unknown field should fail");

    assert_safe_error(
        error,
        RepositoryError::invalid_response(),
        &["response-secret-sentinel"],
    );
}

#[tokio::test]
async fn discovery_rejects_malformed_rows_and_invalid_session_ids_without_leaking_them() {
    let malformed_rows = [
        json!(null),
        json!({}),
        json!({"session_id": 42}),
        json!({"session_id": ""}),
        json!({"session_id": "\u{0}"}),
        json!({"session_id": "session-secret-sentinel", "unexpected": true}),
    ];

    for row in malformed_rows {
        let error = discover_eligible_with(
            &database(),
            timestamp("2026-09-22T12:00:00Z"),
            |_| async move { Ok(successful_response(json!([row]))) },
        )
        .await
        .expect_err("malformed discovery row should fail");
        assert_safe_error(
            error,
            RepositoryError::invalid_response(),
            &["session-secret-sentinel"],
        );
    }
}

#[tokio::test]
async fn discovery_sql_remains_read_only_and_does_not_load_out_of_scope_data() {
    let _ = discover_eligible_with(
        &database(),
        timestamp("2026-09-22T12:00:00Z"),
        |request| async move {
            let sql = request
                .payload
                .get("sql")
                .and_then(Value::as_str)
                .expect("discovery request should contain SQL")
                .to_ascii_lowercase();
            assert!(sql.trim_start().starts_with("with "));
            for forbidden in [
                "insert",
                "update",
                "delete",
                "merge",
                "session_processing_attempt",
                "session_memory_candidate",
                "lease",
                "provider",
                "public.memories",
                "project_name",
                "current_working_directory",
                "source_timestamp",
                "hook_type",
                " data",
            ] {
                assert!(
                    !sql.contains(forbidden),
                    "discovery SQL referenced forbidden scope {forbidden}: {sql}"
                );
            }
            Ok(successful_response(json!([])))
        },
    )
    .await
    .expect("scope guard request should be accepted");
}

#[tokio::test]
async fn stage_persists_a_complete_projection_and_ordered_candidates_in_one_fenced_request() {
    let claim = staging_claim(AttemptState::Claimed);
    let output = staged_revision(
        &claim,
        vec![
            staging_candidate(
                &claim,
                STAGING_FINGERPRINT_ONE,
                STAGING_MEMORY_ID_ONE,
                &[STAGING_OBSERVATION_ONE],
            ),
            staging_candidate(
                &claim,
                STAGING_FINGERPRINT_TWO,
                STAGING_MEMORY_ID_TWO,
                &[STAGING_OBSERVATION_TWO],
            ),
        ],
    );
    let request_claim = claim.clone();
    let request_output = output.clone();

    stage_with(&database(), &claim, output, move |request| {
        let request_claim = request_claim.clone();
        let request_output = request_output.clone();
        async move {
            assert_stage_request(&request, &request_claim, &request_output);
            Ok(successful_response(json!([staged_marker(&request_claim)])))
        }
    })
    .await
    .expect("a claimed attempt should stage one complete projection atomically");
}

#[tokio::test]
async fn stage_accepts_zero_candidates_without_skipping_the_fenced_projection() {
    let claim = staging_claim(AttemptState::Claimed);
    let output = staged_revision(&claim, vec![]);
    let request_claim = claim.clone();
    let request_output = output.clone();

    stage_with(&database(), &claim, output, move |request| {
        let request_claim = request_claim.clone();
        let request_output = request_output.clone();
        async move {
            assert_stage_request(&request, &request_claim, &request_output);
            assert_eq!(
                request.payload["params"][10],
                json!([]),
                "a zero-memory revision must still stage an explicit empty candidate array"
            );
            Ok(successful_response(json!([staged_marker(&request_claim)])))
        }
    })
    .await
    .expect("a zero-memory revision should stage its complete projection");
}

#[tokio::test]
async fn stage_merges_same_revision_evidence_before_ordered_conflict_ignoring_insert() {
    let claim = staging_claim(AttemptState::Claimed);
    let output = staged_revision(
        &claim,
        vec![
            staging_candidate(
                &claim,
                STAGING_FINGERPRINT_ONE,
                STAGING_MEMORY_ID_ONE,
                &[STAGING_OBSERVATION_TWO],
            ),
            staging_candidate(
                &claim,
                STAGING_FINGERPRINT_ONE,
                STAGING_MEMORY_ID_ONE,
                &[STAGING_OBSERVATION_ONE],
            ),
        ],
    );

    assert_eq!(output.candidates().len(), 1);
    assert_eq!(
        output.candidates()[0]
            .supporting_receipt_ids()
            .iter()
            .map(|receipt_id| receipt_id.as_str())
            .collect::<Vec<_>>(),
        [STAGING_OBSERVATION_ONE, STAGING_OBSERVATION_TWO]
    );
    assert_eq!(
        output.candidates()[0]
            .canonical_payload()
            .source_observation_ids,
        [STAGING_OBSERVATION_ONE, STAGING_OBSERVATION_TWO]
    );

    let request_claim = claim.clone();
    let request_output = output.clone();
    stage_with(&database(), &claim, output, move |request| {
        let request_claim = request_claim.clone();
        let request_output = request_output.clone();
        async move {
            assert_stage_request(&request, &request_claim, &request_output);
            let candidates = request.payload["params"][10]
                .as_array()
                .expect("staging candidates must remain an ordered JSON array");
            assert_eq!(candidates.len(), 1);
            assert_eq!(
                candidates[0]["supporting_receipt_ids"],
                json!([STAGING_OBSERVATION_ONE, STAGING_OBSERVATION_TWO])
            );
            Ok(successful_response(json!([staged_marker(&request_claim)])))
        }
    })
    .await
    .expect("same-revision evidence should merge before the database conflict guard runs");
}

#[tokio::test]
async fn stage_rejects_stale_or_malformed_outcomes_without_leaking_backend_content() {
    let marker_claim = staging_claim(AttemptState::Claimed);
    let mut unknown_envelope = successful_response(json!([staged_marker(&marker_claim)]));
    unknown_envelope["backend_payload"] = json!("response-secret-sentinel");
    let malformed_marker = successful_response(json!([{
        "attempt_id": RETAINED_ATTEMPT_ID,
        "session_id": "staging-session",
        "source_revision": "22222222-2222-5222-8222-222222222222",
        "state": "claimed",
    }]));

    for (response, expected) in [
        (
            successful_response(json!([])),
            RepositoryError::unconfirmed_commit(),
        ),
        (unknown_envelope, RepositoryError::invalid_response()),
        (malformed_marker, RepositoryError::invalid_response()),
    ] {
        let claim = staging_claim(AttemptState::Claimed);
        let output = staged_revision(&claim, vec![]);
        let error = stage_with(&database(), &claim, output, |_| async move { Ok(response) })
            .await
            .expect_err("stale or malformed staging responses must remain opaque errors");
        assert_safe_error(error, expected, &["response-secret-sentinel"]);
    }

    let claim = staging_claim(AttemptState::Claimed);
    let output = staged_revision(&claim, vec![]);
    let error = stage_with(&database(), &claim, output, |_| async {
        Err(IiiError::Remote {
            code: "DATABASE_UNAVAILABLE".to_owned(),
            message: "database-password-sentinel".to_owned(),
            stacktrace: Some("backend-payload-sentinel".to_owned()),
        })
    })
    .await
    .expect_err("stage invocation failures must remain opaque");
    assert_safe_error(
        error,
        RepositoryError::invocation(),
        &["database-password-sentinel", "backend-payload-sentinel"],
    );
}

#[tokio::test]
async fn stage_rejects_non_claimed_or_mismatched_outputs_before_database_invocation() {
    let staged_claim = staging_claim(AttemptState::Staged);
    let staged_output = staged_revision(&staged_claim, vec![]);
    let error = stage_with(&database(), &staged_claim, staged_output, |_| async {
        panic!("a non-claimed attempt must not stage")
    })
    .await
    .expect_err("only claimed attempts may stage a projection");
    assert_safe_error(error, RepositoryError::invalid_response(), &[]);

    let claim = staging_claim(AttemptState::Claimed);
    let other_claim = staging_claim_for("other-session", AttemptState::Claimed);
    let mismatched_output = staged_revision(&other_claim, vec![]);
    let error = stage_with(&database(), &claim, mismatched_output, |_| async {
        panic!("a mismatched output must not reach the database")
    })
    .await
    .expect_err("the claim session and revision must fence staged output");
    assert_safe_error(error, RepositoryError::invalid_response(), &[]);
}

#[tokio::test]
async fn mark_memory_published_transitions_staged_or_publishing_attempts_idempotently() {
    for state in [AttemptState::Staged, AttemptState::Publishing] {
        let claim = staging_claim(state);
        let request_claim = claim.clone();
        mark_memory_published_with(&database(), &claim, STAGING_MEMORY_ID_ONE, move |request| {
            let request_claim = request_claim.clone();
            async move {
                assert_mark_memory_published_request(
                    &request,
                    &request_claim,
                    STAGING_MEMORY_ID_ONE,
                );
                Ok(successful_response(json!([published_marker(
                    &request_claim,
                    STAGING_MEMORY_ID_ONE,
                )])))
            }
        })
        .await
        .expect("staged and publishing attempts should confirm publication idempotently");
    }
}

#[tokio::test]
async fn mark_memory_published_rejects_missing_stale_terminal_or_malformed_outcomes_safely() {
    let marker_claim = staging_claim(AttemptState::Staged);
    let mut unknown_envelope = successful_response(json!([published_marker(
        &marker_claim,
        STAGING_MEMORY_ID_ONE,
    )]));
    unknown_envelope["backend_payload"] = json!("response-secret-sentinel");
    let malformed_marker = successful_response(json!([{
        "attempt_id": RETAINED_ATTEMPT_ID,
        "memory_id": STAGING_MEMORY_ID_ONE,
        "published_at": "2026-09-22T12:00:00Z",
        "state": "staged",
    }]));

    for (response, expected) in [
        (
            successful_response(json!([])),
            RepositoryError::unconfirmed_commit(),
        ),
        (unknown_envelope, RepositoryError::invalid_response()),
        (malformed_marker, RepositoryError::invalid_response()),
    ] {
        let claim = staging_claim(AttemptState::Staged);
        let error = mark_memory_published_with(
            &database(),
            &claim,
            STAGING_MEMORY_ID_ONE,
            |_| async move { Ok(response) },
        )
        .await
        .expect_err("missing, stale, and malformed publication responses must remain opaque");
        assert_safe_error(error, expected, &["response-secret-sentinel"]);
    }

    let claim = staging_claim(AttemptState::Staged);
    let error = mark_memory_published_with(&database(), &claim, STAGING_MEMORY_ID_ONE, |_| async {
        Err(IiiError::Remote {
            code: "DATABASE_UNAVAILABLE".to_owned(),
            message: "database-password-sentinel".to_owned(),
            stacktrace: Some("backend-payload-sentinel".to_owned()),
        })
    })
    .await
    .expect_err("publication invocation failures must remain opaque");
    assert_safe_error(
        error,
        RepositoryError::invocation(),
        &["database-password-sentinel", "backend-payload-sentinel"],
    );

    for memory_id in [
        "",
        "not-a-memory-id",
        "11111111-1111-4111-8111-111111111111",
    ] {
        let error = mark_memory_published_with(&database(), &claim, memory_id, |_| async {
            panic!("an invalid memory ID must not reach the database")
        })
        .await
        .expect_err("publication requires a UUIDv5 candidate ID");
        assert_safe_error(error, RepositoryError::invalid_response(), &[]);
    }

    let claimed = staging_claim(AttemptState::Claimed);
    let error =
        mark_memory_published_with(&database(), &claimed, STAGING_MEMORY_ID_ONE, |_| async {
            panic!("a claimed attempt has no publication marker")
        })
        .await
        .expect_err("terminal and pre-stage state transitions must not be inferred locally");
    assert_safe_error(error, RepositoryError::invalid_response(), &[]);
}

#[tokio::test]
async fn load_unpublished_candidates_transitions_staged_or_publishing_and_decodes_ordered_candidates()
 {
    for state in [AttemptState::Staged, AttemptState::Publishing] {
        let claim = staging_claim(state);
        let first = staging_candidate(
            &claim,
            STAGING_FINGERPRINT_ONE,
            STAGING_MEMORY_ID_ONE,
            &[STAGING_OBSERVATION_ONE],
        );
        let second = staging_candidate(
            &claim,
            STAGING_FINGERPRINT_TWO,
            STAGING_MEMORY_ID_TWO,
            &[STAGING_OBSERVATION_TWO],
        );
        let expected = vec![first.clone(), second.clone()];
        let request_claim = claim.clone();

        let candidates = load_unpublished_candidates_with(&database(), &claim, move |request| {
            let request_claim = request_claim.clone();
            let first = first.clone();
            let second = second.clone();
            async move {
                assert_load_unpublished_candidates_request(&request, &request_claim);
                Ok(successful_response(json!([unpublished_candidates_row(
                    &request_claim,
                    vec![
                        unpublished_candidate(0, &first),
                        unpublished_candidate(1, &second),
                    ],
                ),])))
            }
        })
        .await
        .expect("a fenced staged or publishing attempt should recover ordered candidates");

        assert_eq!(
            candidates, expected,
            "the recovery operation must expose every unpublished candidate in ordinal order"
        );
    }
}

#[tokio::test]
async fn load_unpublished_candidates_accepts_zero_memory_and_fully_published_markers() {
    for outcome in ["zero-memory", "fully-published"] {
        let claim = staging_claim(AttemptState::Staged);
        let request_claim = claim.clone();
        let candidates = load_unpublished_candidates_with(&database(), &claim, move |request| {
            let request_claim = request_claim.clone();
            async move {
                assert_load_unpublished_candidates_request(&request, &request_claim);
                Ok(successful_response(json!([unpublished_candidates_marker(
                    &request_claim
                )])))
            }
        })
        .await
        .expect("a confirmed empty candidate marker should be a successful recovery outcome");

        assert!(
            candidates.is_empty(),
            "{outcome} recovery must return an empty candidate list without reporting a stale claim"
        );
    }
}

#[tokio::test]
async fn load_unpublished_candidates_rejects_stale_or_malformed_outcomes_without_leaking_content() {
    let claim = staging_claim(AttemptState::Staged);
    let candidate = staging_candidate(
        &claim,
        STAGING_FINGERPRINT_ONE,
        STAGING_MEMORY_ID_ONE,
        &[STAGING_OBSERVATION_ONE],
    );
    let mut unknown_envelope = successful_response(json!([unpublished_candidates_marker(&claim)]));
    unknown_envelope["backend_payload"] = json!("response-secret-sentinel");
    let mut malformed_payload = unpublished_candidate(0, &candidate);
    malformed_payload["canonical_payload"]["unexpected"] =
        json!("candidate-payload-secret-sentinel");
    let mut wrong_identity = unpublished_candidates_marker(&claim);
    wrong_identity["attempt_id"] = json!("attempt-id-secret-sentinel");
    let mut wrong_memory_id = unpublished_candidate(0, &candidate);
    wrong_memory_id["memory_id"] = json!("memory-id-secret-sentinel");
    let mut wrong_marker = unpublished_candidates_marker(&claim);
    wrong_marker["state"] = json!("staged");
    let out_of_order = successful_response(json!([unpublished_candidates_row(
        &claim,
        vec![
            unpublished_candidate(1, &candidate),
            unpublished_candidate(0, &candidate),
        ],
    )]));

    for (response, expected_error) in [
        (
            successful_response(json!([])),
            RepositoryError::unconfirmed_commit(),
        ),
        (unknown_envelope, RepositoryError::invalid_response()),
        (
            successful_response(json!([wrong_marker])),
            RepositoryError::invalid_response(),
        ),
        (
            successful_response(json!([unpublished_candidates_row(
                &claim,
                vec![malformed_payload],
            )])),
            RepositoryError::invalid_response(),
        ),
        (
            successful_response(json!([wrong_identity])),
            RepositoryError::invalid_response(),
        ),
        (
            successful_response(json!([unpublished_candidates_row(
                &claim,
                vec![wrong_memory_id],
            )])),
            RepositoryError::invalid_response(),
        ),
        (out_of_order, RepositoryError::invalid_response()),
    ] {
        let claim = staging_claim(AttemptState::Staged);
        let error =
            load_unpublished_candidates_with(&database(), &claim, |_| async move { Ok(response) })
                .await
                .expect_err(
                    "unconfirmed and malformed candidate recovery outcomes must remain opaque",
                );
        assert_safe_error(
            error,
            expected_error,
            &[
                "response-secret-sentinel",
                "candidate-payload-secret-sentinel",
                "attempt-id-secret-sentinel",
                "memory-id-secret-sentinel",
            ],
        );
    }

    let claim = staging_claim(AttemptState::Staged);
    let error = load_unpublished_candidates_with(&database(), &claim, |_| async {
        Err(IiiError::Remote {
            code: "DATABASE_UNAVAILABLE".to_owned(),
            message: "database-password-sentinel".to_owned(),
            stacktrace: Some("backend-payload-sentinel".to_owned()),
        })
    })
    .await
    .expect_err("candidate recovery invocation failures must remain opaque");
    assert_safe_error(
        error,
        RepositoryError::invocation(),
        &["database-password-sentinel", "backend-payload-sentinel"],
    );

    let claimed = staging_claim(AttemptState::Claimed);
    let error = load_unpublished_candidates_with(&database(), &claimed, |_| async {
        panic!("a pre-stage attempt must not recover candidates")
    })
    .await
    .expect_err("only staged or publishing claims may recover candidates");
    assert_safe_error(error, RepositoryError::invalid_response(), &[]);
}

#[tokio::test]
async fn retry_persists_active_claims_with_floored_times_and_preserves_durable_progress() {
    for state in [
        AttemptState::Claimed,
        AttemptState::Staged,
        AttemptState::Publishing,
    ] {
        let claim = staging_claim(state);
        let request_claim = claim.clone();
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocations_for_call = Arc::clone(&invocations);
        retry_with(
            &database(),
            &claim,
            FailureCategory::ProviderTransient,
            timestamp("2026-09-22T12:05:00.987Z"),
            move |request| {
                let request_claim = request_claim.clone();
                let invocations = Arc::clone(&invocations_for_call);
                async move {
                    assert_eq!(
                        invocations.fetch_add(1, Ordering::SeqCst),
                        0,
                        "retry must issue one exact fenced update"
                    );
                    assert_retry_request(
                        &request,
                        &request_claim,
                        FailureCategory::ProviderTransient,
                        "2026-09-22T12:05:00Z",
                    );
                    Ok(successful_response(json!([retry_marker(
                        &request_claim,
                        FailureCategory::ProviderTransient,
                        "2026-09-22T12:05:00+00:00",
                    )])))
                }
            },
        )
        .await
        .expect("active claims should persist a retry marker without completing the revision");
        assert_eq!(invocations.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn retry_rejects_never_categories_and_unconfirmed_or_malformed_results_safely() {
    let claim = staging_claim(AttemptState::Claimed);
    for failure in [
        FailureCategory::InvalidConfiguration,
        FailureCategory::RegistrationMismatch,
        FailureCategory::SourceConflict,
    ] {
        let error = retry_with(
            &database(),
            &claim,
            failure,
            timestamp("2026-09-22T12:05:00Z"),
            |_| async { panic!("never-retry failure categories must not reach the database") },
        )
        .await
        .expect_err("non-retryable categories must be rejected before state mutation");
        assert_safe_error(error, RepositoryError::invalid_response(), &[]);
    }

    let marker_claim = staging_claim(AttemptState::Publishing);
    let mut unknown_envelope = successful_response(json!([retry_marker(
        &marker_claim,
        FailureCategory::MemoryMismatch,
        "2026-09-22T12:05:00+00:00",
    )]));
    unknown_envelope["backend_payload"] = json!("response-secret-sentinel");
    let mut malformed_marker = retry_marker(
        &marker_claim,
        FailureCategory::MemoryMismatch,
        "2026-09-22T12:05:00+00:00",
    );
    malformed_marker["state"] = json!("complete");
    let mut wrong_category = retry_marker(
        &marker_claim,
        FailureCategory::MemoryMismatch,
        "2026-09-22T12:05:00+00:00",
    );
    wrong_category["failure_category"] = json!("category-secret-sentinel");

    for (response, expected_error) in [
        (
            successful_response(json!([])),
            RepositoryError::unconfirmed_commit(),
        ),
        (unknown_envelope, RepositoryError::invalid_response()),
        (
            successful_response(json!([malformed_marker])),
            RepositoryError::invalid_response(),
        ),
        (
            successful_response(json!([wrong_category])),
            RepositoryError::invalid_response(),
        ),
    ] {
        let claim = staging_claim(AttemptState::Publishing);
        let error = retry_with(
            &database(),
            &claim,
            FailureCategory::MemoryMismatch,
            timestamp("2026-09-22T12:05:00Z"),
            |_| async move { Ok(response) },
        )
        .await
        .expect_err("unconfirmed and malformed retries must remain opaque failures");
        assert_safe_error(
            error,
            expected_error,
            &["response-secret-sentinel", "category-secret-sentinel"],
        );
    }

    let claim = staging_claim(AttemptState::Publishing);
    let error = retry_with(
        &database(),
        &claim,
        FailureCategory::MemoryMismatch,
        timestamp("2026-09-22T12:05:00Z"),
        |_| async {
            Err(IiiError::Remote {
                code: "DATABASE_UNAVAILABLE".to_owned(),
                message: "database-password-sentinel".to_owned(),
                stacktrace: Some("backend-payload-sentinel".to_owned()),
            })
        },
    )
    .await
    .expect_err("retry invocation failures must remain opaque");
    assert_safe_error(
        error,
        RepositoryError::invocation(),
        &["database-password-sentinel", "backend-payload-sentinel"],
    );
}

#[tokio::test]
async fn zero_memory_recovery_transitions_a_staged_attempt_before_promotion() {
    let claim = staging_claim(AttemptState::Staged);
    let recovery_claim = claim.clone();
    let candidates = load_unpublished_candidates_with(&database(), &claim, move |request| {
        let recovery_claim = recovery_claim.clone();
        async move {
            assert_load_unpublished_candidates_request(&request, &recovery_claim);
            Ok(successful_response(json!([unpublished_candidates_marker(
                &recovery_claim
            )])))
        }
    })
    .await
    .expect("a zero-memory attempt should confirm its publishing state");
    assert!(candidates.is_empty());

    let promotion_claim = claim.clone();
    promote_with(&database(), &claim, move |request| {
        let promotion_claim = promotion_claim.clone();
        async move {
            assert_promote_request(&request, &promotion_claim);
            Ok(successful_response(json!([promotion_marker(
                &promotion_claim
            )])))
        }
    })
    .await
    .expect("promotion should accept the original staged claim after database recovery moved it to publishing");
}

#[tokio::test]
async fn promote_accepts_staged_origin_and_publishing_claims_after_all_candidates_are_confirmed() {
    for state in [AttemptState::Staged, AttemptState::Publishing] {
        let claim = staging_claim(state);
        let request_claim = claim.clone();
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocations_for_call = Arc::clone(&invocations);
        promote_with(&database(), &claim, move |request| {
            let request_claim = request_claim.clone();
            let invocations = Arc::clone(&invocations_for_call);
            async move {
                assert_eq!(
                    invocations.fetch_add(1, Ordering::SeqCst),
                    0,
                    "promotion must issue one atomic completion and projection write"
                );
                assert_promote_request(&request, &request_claim);
                Ok(successful_response(json!([promotion_marker(
                    &request_claim
                )])))
            }
        })
        .await
        .expect("staged-origin and publishing claims should promote after the database confirms publishing");
        assert_eq!(invocations.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn promote_preserves_the_prior_current_record_on_unconfirmed_or_malformed_outcomes() {
    let marker_claim = staging_claim(AttemptState::Publishing);
    let prior_current_record = json!({
        "source_revision": "prior-revision-secret-sentinel",
        "summary": "prior-current-summary-secret-sentinel",
    });
    let expected_prior_current_record = prior_current_record.clone();
    let mut unknown_envelope = successful_response(json!([promotion_marker(&marker_claim)]));
    unknown_envelope["backend_payload"] = json!("response-secret-sentinel");
    let mut malformed_marker = promotion_marker(&marker_claim);
    malformed_marker["state"] = json!("publishing");
    let mut stale_marker = promotion_marker(&marker_claim);
    stale_marker["source_revision"] = json!("stale-revision-secret-sentinel");

    for (response, expected_error) in [
        (
            successful_response(json!([])),
            RepositoryError::unconfirmed_commit(),
        ),
        (unknown_envelope, RepositoryError::invalid_response()),
        (
            successful_response(json!([malformed_marker])),
            RepositoryError::invalid_response(),
        ),
        (
            successful_response(json!([stale_marker])),
            RepositoryError::invalid_response(),
        ),
    ] {
        let claim = staging_claim(AttemptState::Publishing);
        let error = promote_with(&database(), &claim, |_| async move { Ok(response) })
            .await
            .expect_err(
                "an absent or malformed promotion marker must leave the old current record current",
            );
        assert_safe_error(
            error,
            expected_error,
            &[
                "response-secret-sentinel",
                "stale-revision-secret-sentinel",
                "prior-current-summary-secret-sentinel",
            ],
        );
        assert_eq!(
            prior_current_record, expected_prior_current_record,
            "an unsuccessful promotion must not replace the prior current record"
        );
    }

    let claim = staging_claim(AttemptState::Publishing);
    let error = promote_with(&database(), &claim, |_| async {
        Err(IiiError::Remote {
            code: "DATABASE_UNAVAILABLE".to_owned(),
            message: "database-password-sentinel".to_owned(),
            stacktrace: Some("backend-payload-sentinel".to_owned()),
        })
    })
    .await
    .expect_err("promotion invocation failures must remain opaque");
    assert_safe_error(
        error,
        RepositoryError::invocation(),
        &["database-password-sentinel", "backend-payload-sentinel"],
    );

    let claimed = staging_claim(AttemptState::Claimed);
    let error = promote_with(&database(), &claimed, |_| async {
        panic!("a claimed attempt must not promote a current record")
    })
    .await
    .expect_err("only staged-origin or publishing claims may promote");
    assert_safe_error(error, RepositoryError::invalid_response(), &[]);
}

fn database() -> DatabaseTarget {
    DatabaseTarget::try_from(DATABASE.to_owned()).expect("fixture database should be valid")
}

fn successful_response(returned_rows: Value) -> Value {
    let affected_rows = returned_rows
        .as_array()
        .expect("fixture rows should be an array")
        .len();
    json!({
        "affected_rows": affected_rows,
        "last_insert_id": null,
        "returned_rows": returned_rows,
    })
}

fn assert_request(request: TriggerRequest, now: DateTime<Utc>) {
    assert_eq!(request.function_id, "database::execute");
    assert_eq!(request.payload.get("db"), Some(&json!(DATABASE)));
    assert_eq!(
        request.payload.get("params"),
        Some(&json!([now.to_rfc3339()]))
    );
    let sql = request
        .payload
        .get("sql")
        .and_then(Value::as_str)
        .expect("discovery request should contain production SQL");
    assert_eligibility_sql(sql);
    assert!(request.action.is_none());
    assert_eq!(request.timeout_ms, None);
}

fn assert_eligibility_sql(sql: &str) {
    for (fragment, behavior) in [
        (
            "BOOL_OR(event_type = 'session_end') AS has_persisted_end",
            "persisted session ends",
        ),
        (
            "MAX(ingested_at) AS latest_observation_ingested_at",
            "the latest recorded observation",
        ),
        (
            "FULL OUTER JOIN observations",
            "end-only and observation-only sessions",
        ),
        (
            "WHERE (source.lifecycle_count > 0 OR source.observation_count > 0)",
            "empty session exclusion",
        ),
        (
            "source.latest_observation_ingested_at <= $1::text::timestamptz - INTERVAL '24 hours'",
            "the inclusive 24-hour inactivity boundary",
        ),
        (
            "current_record.lifecycle_count IS DISTINCT FROM source.lifecycle_count",
            "late lifecycle-event refreshes",
        ),
        (
            "current_record.observation_count IS DISTINCT FROM source.observation_count",
            "late observation refreshes",
        ),
    ] {
        assert!(
            sql.contains(fragment),
            "production discovery SQL must preserve {behavior}: {sql}"
        );
    }
    assert!(
        sql.contains(
            "source.has_persisted_end\n      OR source.latest_observation_ingested_at <= $1::text::timestamptz - INTERVAL '24 hours'"
        ),
        "production discovery SQL must gate discovery on persisted ends or inactivity: {sql}"
    );
}

fn candidate_row(
    session_id: &str,
    lifecycle_count: &str,
    observation_count: &str,
    source_identities: &[(&str, &str)],
) -> Value {
    json!({
        "session_id": session_id,
        "lifecycle_count": lifecycle_count,
        "observation_count": observation_count,
        "source_identities": source_identities
            .iter()
            .map(|(event_type, receipt_id)| json!({
                "event_type": event_type,
                "receipt_id": receipt_id,
            }))
            .collect::<Vec<_>>(),
    })
}

fn claim_row(params: &[Value], attempt_id: &str, state: &str) -> Value {
    json!({
        "attempt_id": attempt_id,
        "session_id": params[0].clone(),
        "source_revision": params[1].clone(),
        "lifecycle_count": params[2].clone(),
        "observation_count": params[3].clone(),
        "state": state,
        "lease_token": params[5].clone(),
        "lease_expires_at": params[7].clone(),
    })
}

fn reclaimed_claim_row(
    params: &[Value],
    attempt_id: &str,
    source_revision: &str,
    lifecycle_count: &str,
    observation_count: &str,
    state: &str,
) -> Value {
    json!({
        "attempt_id": attempt_id,
        "session_id": params[0].clone(),
        "source_revision": source_revision,
        "lifecycle_count": lifecycle_count,
        "observation_count": observation_count,
        "state": state,
        "lease_token": params[5].clone(),
        "lease_expires_at": params[7].clone(),
    })
}

fn retained_claim() -> Claim {
    Claim::try_from(ClaimInput {
        attempt_id: RETAINED_ATTEMPT_ID.to_owned(),
        session_id: "retained-session".to_owned(),
        source_revision: "22222222-2222-5222-8222-222222222222".to_owned(),
        lifecycle_count: 1,
        observation_count: 0,
        state: AttemptState::Claimed,
        lease_token: RETAINED_LEASE_TOKEN.to_owned(),
        lease_expires_at: timestamp("2026-09-22T12:10:00Z"),
    })
    .expect("retained claim fixture should be valid")
}

fn source_claim(
    source_identities: &[(&str, &str)],
    lifecycle_count: u64,
    observation_count: u64,
    state: AttemptState,
) -> Claim {
    Claim::try_from(ClaimInput {
        attempt_id: RETAINED_ATTEMPT_ID.to_owned(),
        session_id: "snapshot-session".to_owned(),
        source_revision: expected_source_revision(source_identities),
        lifecycle_count,
        observation_count,
        state,
        lease_token: RETAINED_LEASE_TOKEN.to_owned(),
        lease_expires_at: timestamp("2026-09-22T12:10:00Z"),
    })
    .expect("source claim fixture should be valid")
}

fn lifecycle_source_row(
    event_type: &str,
    receipt_id: &str,
    source_timestamp_rfc3339: &str,
    source_timestamp_utc: &str,
    ingested_at: &str,
) -> Value {
    json!({
        "entry": {
            "kind": "lifecycle",
            "receipt_id": receipt_id,
            "event_type": event_type,
            "session_id": "snapshot-session",
            "project_name": "project",
            "current_working_directory": "/work",
            "source_timestamp_rfc3339": source_timestamp_rfc3339,
            "source_timestamp_utc": source_timestamp_utc,
            "ingested_at": ingested_at,
        },
        "prior_projection_present": false,
        "prior_transcript": null,
    })
}

fn observation_source_row(
    receipt_id: &str,
    source_timestamp_rfc3339: &str,
    source_timestamp_utc: &str,
    ingested_at: &str,
) -> Value {
    json!({
        "entry": {
            "kind": "observation",
            "receipt_id": receipt_id,
            "event_type": "observation",
            "session_id": "snapshot-session",
            "hook_type": "post_tool_use",
            "project_name": "project",
            "current_working_directory": "/work",
            "source_timestamp_rfc3339": source_timestamp_rfc3339,
            "source_timestamp_utc": source_timestamp_utc,
            "ingested_at": ingested_at,
            "data": {"opaque": {"source": "raw-observation-secret-sentinel"}},
        },
        "prior_projection_present": false,
        "prior_transcript": null,
    })
}

fn source_row_with_prior_projection(mut row: Value, transcript: Value) -> Value {
    row["prior_projection_present"] = json!(true);
    row["prior_transcript"] = transcript;
    row
}

fn projection_transcript(rows: &[Value]) -> Value {
    json!({
        "entries": rows
            .iter()
            .map(|row| row["entry"].clone())
            .collect::<Vec<_>>(),
    })
}

fn assert_source_load_request(request: &TriggerRequest, claim: &Claim) {
    assert_eq!(request.function_id, "database::execute");
    assert_eq!(request.payload.get("db"), Some(&json!(DATABASE)));
    assert_eq!(
        request.payload.get("params"),
        Some(&json!([
            claim.attempt_id().as_str(),
            claim.lease_token().as_str(),
            claim.session_id().as_str(),
            claim.source_revision().as_str(),
        ]))
    );
    let sql = request.payload["sql"]
        .as_str()
        .expect("source-load request should contain production SQL");
    assert_source_load_sql(sql);
    assert!(request.action.is_none());
    assert_eq!(request.timeout_ms, None);
}

fn assert_source_load_sql(sql: &str) {
    assert!(sql.trim_start().starts_with("WITH request AS"));
    for (fragment, behavior) in [
        ("$1::text::uuid AS attempt_id", "attempt ID parameter"),
        ("$2::text::uuid AS lease_token", "lease token parameter"),
        ("$3::text AS session_id", "session ID parameter"),
        (
            "$4::text::uuid AS source_revision",
            "source revision parameter",
        ),
        ("fenced_attempt AS (", "the fenced attempt read"),
        (
            "FROM public.session_processing_attempts AS attempt",
            "the processor-owned attempt fence",
        ),
        (
            "attempt.attempt_id = request.attempt_id",
            "attempt ID fencing",
        ),
        (
            "attempt.lease_token = request.lease_token",
            "lease token fencing",
        ),
        (
            "attempt.session_id = request.session_id",
            "session ID fencing",
        ),
        (
            "attempt.source_revision = request.source_revision",
            "source revision fencing",
        ),
        (
            "attempt.state IN ('claimed', 'staged', 'publishing')",
            "active-attempt fencing",
        ),
        ("source_entries AS (", "one fenced source read"),
        ("'kind', 'lifecycle'", "the lifecycle variant"),
        ("'kind', 'observation'", "the observation variant"),
        (
            "'receipt_id', lifecycle.receipt_id::text",
            "lifecycle receipt IDs",
        ),
        (
            "'receipt_id', observation.receipt_id::text",
            "observation receipt IDs",
        ),
        (
            "'event_type', lifecycle.event_type",
            "lifecycle event types",
        ),
        (
            "'event_type', observation.event_type",
            "observation event types",
        ),
        (
            "'session_id', lifecycle.session_id",
            "lifecycle session IDs",
        ),
        (
            "'session_id', observation.session_id",
            "observation session IDs",
        ),
        (
            "'project_name', lifecycle.project_name",
            "lifecycle project names",
        ),
        (
            "'project_name', observation.project_name",
            "observation project names",
        ),
        (
            "'current_working_directory', lifecycle.current_working_directory",
            "lifecycle working directories",
        ),
        (
            "'current_working_directory', observation.current_working_directory",
            "observation working directories",
        ),
        (
            "'source_timestamp_rfc3339', lifecycle.source_timestamp_rfc3339",
            "lifecycle source timestamp text",
        ),
        (
            "'source_timestamp_rfc3339', observation.source_timestamp_rfc3339",
            "observation source timestamp text",
        ),
        (
            "'source_timestamp_utc', lifecycle.source_timestamp_utc",
            "JSON-wrapped lifecycle source instants",
        ),
        (
            "'source_timestamp_utc', observation.source_timestamp_utc",
            "JSON-wrapped observation source instants",
        ),
        (
            "'ingested_at', lifecycle.ingested_at",
            "JSON-wrapped lifecycle recorded times",
        ),
        (
            "'ingested_at', observation.ingested_at",
            "JSON-wrapped observation recorded times",
        ),
        (
            "'hook_type', observation.hook_type",
            "observation hook types",
        ),
        ("'data', observation.data", "opaque observation JSON"),
        (
            "JOIN session_events AS lifecycle\n        ON lifecycle.session_id = fenced_attempt.session_id",
            "the fenced lifecycle session ID",
        ),
        (
            "JOIN raw_observations AS observation\n        ON observation.session_id = fenced_attempt.session_id",
            "the fenced observation session ID",
        ),
        (
            "LEFT JOIN public.session_records AS current_record",
            "the prior projection read",
        ),
        (
            "current_record.session_id IS NOT NULL AS prior_projection_present",
            "the explicit prior projection marker",
        ),
        (
            "current_record.transcript AS prior_transcript",
            "the stored prior transcript",
        ),
        (
            "SELECT entry, prior_projection_present, prior_transcript",
            "the closed source-load response shape",
        ),
        ("UNION ALL", "append-only duplicate preservation"),
    ] {
        assert!(
            sql.contains(fragment),
            "source-load SQL must preserve {behavior}: {sql}"
        );
    }
    let lowercase = sql.to_ascii_lowercase();
    for forbidden in [
        "distinct",
        "order by",
        "limit",
        "insert",
        "update",
        "delete",
        "merge",
        "session_memory_candidate",
        "provider",
        "memory",
        "source_cutoff",
        "jsonb_agg",
        "transaction",
        "begin",
        "commit",
        ";",
    ] {
        assert!(
            !lowercase.contains(forbidden),
            "source-load SQL must remain one read-only source query without {forbidden}: {sql}"
        );
    }
}

fn assert_supersede_request(request: &TriggerRequest, claim: &Claim) {
    assert_eq!(request.function_id, "database::execute");
    assert_eq!(request.payload.get("db"), Some(&json!(DATABASE)));
    assert_eq!(
        request.payload.get("params"),
        Some(&json!([
            claim.attempt_id().as_str(),
            claim.lease_token().as_str(),
        ]))
    );
    let sql = request.payload["sql"]
        .as_str()
        .expect("supersede request should contain production SQL");
    assert!(sql.trim_start().starts_with("WITH request AS"));
    for (fragment, behavior) in [
        (
            "UPDATE public.session_processing_attempts AS attempt",
            "the processor-owned attempt row",
        ),
        ("SET state = 'superseded'", "the terminal state change"),
        (
            "attempt.attempt_id = request.attempt_id",
            "attempt ID fencing",
        ),
        (
            "attempt.lease_token = request.lease_token",
            "lease-token fencing",
        ),
        ("attempt.state = 'claimed'", "claimed-state fencing"),
        (
            "RETURNING TRUE AS superseded",
            "positive write confirmation",
        ),
    ] {
        assert!(
            sql.contains(fragment),
            "supersede SQL must preserve {behavior}: {sql}"
        );
    }
    let set_clause = sql
        .split_once("SET ")
        .expect("supersede SQL must assign state")
        .1
        .split_once("FROM request")
        .expect("supersede SQL must use fenced request values")
        .0;
    assert_eq!(normalized_sql(set_clause), "state = 'superseded'");
    let lowercase = sql.to_ascii_lowercase();
    for forbidden in [
        "source_revision",
        "lifecycle_count",
        "observation_count",
        "lease_expires_at",
        "next_attempt_at",
        "failure_category",
        "source_cutoff",
        "transcript",
        "summary",
        "concepts",
        "generated_at",
        "session_memory_candidate",
        "transaction",
        "begin",
        "commit",
        ";",
    ] {
        assert!(
            !lowercase.contains(forbidden),
            "supersede SQL must change only state, not {forbidden}: {sql}"
        );
    }
    assert!(request.action.is_none());
    assert_eq!(request.timeout_ms, None);
}

fn expected_source_revision(source_identities: &[(&str, &str)]) -> String {
    let mut source_identities = source_identities.to_vec();
    source_identities.sort_unstable();
    let mut name = Vec::new();
    for (event_type, receipt_id) in source_identities {
        name.extend_from_slice(event_type.as_bytes());
        name.push(b'\0');
        name.extend_from_slice(receipt_id.as_bytes());
        name.push(b'\n');
    }

    let namespace = Uuid::new_v5(&Uuid::NAMESPACE_URL, SOURCE_REVISION_NAMESPACE);
    let source_revision = Uuid::new_v5(&namespace, &name);
    assert_eq!(source_revision.get_version_num(), 5);
    source_revision.to_string()
}

fn assert_candidate_request(request: TriggerRequest, issued_at: &str, limit: u32) {
    assert_eq!(request.function_id, "database::execute");
    assert_eq!(request.payload.get("db"), Some(&json!(DATABASE)));
    assert_eq!(
        request.payload.get("params"),
        Some(&json!([issued_at, limit.to_string()]))
    );
    let sql = request
        .payload
        .get("sql")
        .and_then(Value::as_str)
        .expect("candidate request should contain production SQL");
    assert_claim_candidate_sql(sql);
    assert!(request.action.is_none());
    assert_eq!(request.timeout_ms, None);
}

fn assert_claim_candidate_sql(sql: &str) {
    assert!(sql.trim_start().starts_with("WITH lifecycle AS"));
    for (fragment, behavior) in [
        (
            "MAX(ingested_at) AS latest_observation_ingested_at",
            "the inactivity eligibility boundary",
        ),
        (
            "current_record.lifecycle_count IS DISTINCT FROM source.lifecycle_count",
            "late lifecycle refreshes",
        ),
        (
            "current_record.observation_count IS DISTINCT FROM source.observation_count",
            "late observation refreshes",
        ),
        ("LIMIT $2::text::bigint", "the configured claim limit"),
        (
            "jsonb_build_object('event_type', identities.event_type, 'receipt_id', identities.receipt_id::text)",
            "only source identity pairs",
        ),
        (
            "ORDER BY identities.event_type COLLATE \"C\", identities.receipt_id::text COLLATE \"C\"",
            "canonical source revision ordering",
        ),
    ] {
        assert!(
            sql.contains(fragment),
            "candidate SQL must preserve {behavior}: {sql}"
        );
    }
    let lowercase = sql.to_ascii_lowercase();
    for forbidden in [
        " data",
        "project_name",
        "current_working_directory",
        "source_timestamp",
        "hook_type",
        "transcript",
        "provider",
        "public.memories",
        "insert",
        "update",
        "delete",
        "merge",
    ] {
        assert!(
            !lowercase.contains(forbidden),
            "candidate SQL must not load source payload or write scope {forbidden}: {sql}"
        );
    }
}

fn assert_claim_request(
    request: &TriggerRequest,
    session_id: &str,
    source_revision: &str,
    lifecycle_count: &str,
    observation_count: &str,
    issued_at: &str,
    lease_expires_at: &str,
) {
    assert_eq!(request.function_id, "database::execute");
    assert_eq!(request.payload.get("db"), Some(&json!(DATABASE)));
    let params = request.payload["params"]
        .as_array()
        .expect("claim request should contain parameters");
    assert_eq!(params.len(), 8);
    assert_eq!(params[0], json!(session_id));
    assert_eq!(params[1], json!(source_revision));
    assert_eq!(params[2], json!(lifecycle_count));
    assert_eq!(params[3], json!(observation_count));
    assert_uuid_v7(params[4].as_str().expect("attempt ID should be text"));
    assert_uuid_v7(params[5].as_str().expect("lease token should be text"));
    assert_eq!(params[6], json!(issued_at));
    assert_eq!(params[7], json!(lease_expires_at));
    let sql = request.payload["sql"]
        .as_str()
        .expect("claim request should contain production SQL");
    assert_claim_sql(sql);
    assert!(request.action.is_none());
    assert_eq!(request.timeout_ms, None);
}

fn assert_claim_sql(sql: &str) {
    assert!(sql.trim_start().starts_with("WITH request AS"));
    for (fragment, behavior) in [
        ("reclaimed_retryable AS (", "due retryable reclamation"),
        (
            "reclaimed_expired AS (",
            "expired active-attempt reclamation",
        ),
        (
            "INSERT INTO public.session_processing_attempts",
            "atomic fresh claim",
        ),
        ("ON CONFLICT DO NOTHING", "unique-race protection"),
        ("attempt.state = 'retryable'", "due retry reclamation"),
        (
            "attempt.state IN ('claimed', 'staged', 'publishing')",
            "active-state lease reclamation",
        ),
        (
            "attempt.lease_expires_at <= request.now",
            "expired ownership reclamation",
        ),
        (
            "lease_token = request.lease_token",
            "new fencing-token issue",
        ),
        (
            "next_attempt_at = NULL",
            "retry scheduling removal for retryable work",
        ),
        ("RETURNING", "claim response confirmation"),
    ] {
        assert!(
            sql.contains(fragment),
            "claim SQL must preserve {behavior}: {sql}"
        );
    }
    assert!(!sql.contains("attempt.source_revision = request.source_revision"));
    assert!(!sql.contains("reclaim_any_source_revision"));

    let retryable_set = claim_cte_set_clause(sql, "reclaimed_retryable AS (");
    assert_eq!(
        normalized_sql(retryable_set),
        "state = 'claimed', lease_token = request.lease_token, lease_expires_at = request.lease_expires_at, next_attempt_at = NULL"
    );

    let expired_set = claim_cte_set_clause(sql, "reclaimed_expired AS (");
    assert_eq!(
        normalized_sql(expired_set),
        "lease_token = request.lease_token, lease_expires_at = request.lease_expires_at"
    );
    for forbidden in [
        "state",
        "source_revision",
        "lifecycle_count",
        "observation_count",
        "next_attempt_at",
        "failure_category",
        "source_cutoff",
        "transcript",
        "summary_sentences",
        "summary",
        "concepts",
        "generated_at",
        "session_memory_candidates",
        "published_at",
    ] {
        assert!(
            !expired_set.contains(forbidden),
            "expired staged/publishing reclaim must not update {forbidden}: {expired_set}"
        );
    }
    let lowercase = sql.to_ascii_lowercase();
    for forbidden in ["begin", "commit", "transaction", ";"] {
        assert!(
            !lowercase.contains(forbidden),
            "claim DML must remain one atomic statement, not {forbidden}: {sql}"
        );
    }
}

fn claim_cte_set_clause<'a>(sql: &'a str, cte: &str) -> &'a str {
    let cte = sql
        .split_once(cte)
        .expect("claim SQL should define the expected reclaim CTE")
        .1;
    let set = cte
        .split_once("SET ")
        .expect("reclaim CTE should update lease fields")
        .1;
    set.split_once("FROM request")
        .expect("reclaim CTE should use the request values")
        .0
        .trim()
}

fn normalized_sql(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn assert_renew_request(
    request: TriggerRequest,
    claim: &Claim,
    issued_at: &str,
    lease_expires_at: &str,
) {
    assert_eq!(request.function_id, "database::execute");
    assert_eq!(request.payload.get("db"), Some(&json!(DATABASE)));
    assert_eq!(
        request.payload.get("params"),
        Some(&json!([
            claim.attempt_id().as_str(),
            claim.lease_token().as_str(),
            issued_at,
            lease_expires_at,
        ]))
    );
    let sql = request.payload["sql"]
        .as_str()
        .expect("renew request should contain production SQL");
    for (fragment, behavior) in [
        (
            "UPDATE public.session_processing_attempts AS attempt",
            "renewal write",
        ),
        ("attempt.attempt_id = request.attempt_id", "attempt fencing"),
        ("attempt.lease_token = request.lease_token", "token fencing"),
        (
            "attempt.state IN ('claimed', 'staged', 'publishing')",
            "active-state fencing",
        ),
        (
            "attempt.lease_expires_at > request.now",
            "unexpired-lease fencing",
        ),
        (
            "lease_expires_at = request.lease_expires_at",
            "whole-second lease renewal",
        ),
        ("RETURNING TRUE AS renewed", "renewal confirmation"),
    ] {
        assert!(
            sql.contains(fragment),
            "renew SQL must preserve {behavior}: {sql}"
        );
    }
    let lowercase = sql.to_ascii_lowercase();
    assert!(!lowercase.contains(';'));
    assert!(request.action.is_none());
    assert_eq!(request.timeout_ms, None);
}

fn assert_safe_error(error: RepositoryError, expected: RepositoryError, sentinels: &[&str]) {
    assert_eq!(error, expected);
    let display = error.to_string();
    let debug = format!("{error:?}");
    let serialized = serde_json::to_string(&error).expect("error should serialize safely");
    for sentinel in sentinels {
        assert!(
            !display.contains(sentinel),
            "Display leaked protected value: {display}"
        );
        assert!(
            !debug.contains(sentinel),
            "Debug leaked protected value: {debug}"
        );
        assert!(
            !serialized.contains(sentinel),
            "serialization leaked protected value: {serialized}"
        );
    }
}

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("timestamp fixture should be valid")
        .with_timezone(&Utc)
}

fn assert_uuid_v7(value: &str) {
    assert_eq!(
        Uuid::parse_str(value)
            .expect("identifier should be a UUID")
            .get_version_num(),
        7,
        "identifier should be UUIDv7"
    );
}

fn staging_claim(state: AttemptState) -> Claim {
    staging_claim_for("staging-session", state)
}

fn staging_claim_for(session_id: &str, state: AttemptState) -> Claim {
    Claim::try_from(ClaimInput {
        attempt_id: RETAINED_ATTEMPT_ID.to_owned(),
        session_id: session_id.to_owned(),
        source_revision: "22222222-2222-5222-8222-222222222222".to_owned(),
        lifecycle_count: 1,
        observation_count: 2,
        state,
        lease_token: RETAINED_LEASE_TOKEN.to_owned(),
        lease_expires_at: timestamp("2026-09-22T12:10:00Z"),
    })
    .expect("staging claim fixture should be valid")
}

fn staged_revision(claim: &Claim, candidates: Vec<StagedMemoryCandidate>) -> StagedRevision {
    let snapshot = SourceSnapshot::try_from(SourceSnapshotInput {
        session_id: claim.session_id().as_str().to_owned(),
        source_revision: claim.source_revision().as_str().to_owned(),
        source_cutoff: timestamp("2026-09-18T14:00:00Z"),
        entries: vec![
            TranscriptEntryInput::Lifecycle {
                receipt_id: "44444444-4444-4444-8444-444444444444".to_owned(),
                event_type: SourceEventType::SessionStart,
                session_id: claim.session_id().as_str().to_owned(),
                project_name: "project".to_owned(),
                current_working_directory: "/work".to_owned(),
                source_timestamp_rfc3339: "2026-09-18T10:00:00.000Z".to_owned(),
                source_timestamp_utc: timestamp("2026-09-18T10:00:00Z"),
                ingested_at: timestamp("2026-09-18T11:00:00Z"),
            },
            TranscriptEntryInput::Observation {
                receipt_id: STAGING_OBSERVATION_ONE.to_owned(),
                event_type: SourceEventType::Observation,
                session_id: claim.session_id().as_str().to_owned(),
                hook_type: "post_tool_use".to_owned(),
                project_name: "project".to_owned(),
                current_working_directory: "/work".to_owned(),
                source_timestamp_rfc3339: "2026-09-18T12:00:00.000Z".to_owned(),
                source_timestamp_utc: timestamp("2026-09-18T12:00:00Z"),
                ingested_at: timestamp("2026-09-18T13:00:00Z"),
                data: json!({"opaque": "raw-observation-secret-sentinel"}),
            },
            TranscriptEntryInput::Observation {
                receipt_id: STAGING_OBSERVATION_TWO.to_owned(),
                event_type: SourceEventType::Observation,
                session_id: claim.session_id().as_str().to_owned(),
                hook_type: "post_tool_use".to_owned(),
                project_name: "project".to_owned(),
                current_working_directory: "/work".to_owned(),
                source_timestamp_rfc3339: "2026-09-18T13:00:00.000Z".to_owned(),
                source_timestamp_utc: timestamp("2026-09-18T13:00:00Z"),
                ingested_at: timestamp("2026-09-18T14:00:00Z"),
                data: json!({"opaque": "raw-observation-secret-sentinel"}),
            },
        ],
    })
    .expect("staging snapshot fixture should be valid");
    let scope = MemoryScope::try_from(vec![
        STAGING_OBSERVATION_ONE.to_owned(),
        STAGING_OBSERVATION_TWO.to_owned(),
    ])
    .expect("staging memory scope should be valid");
    let generated_revision = GeneratedRevisionInput {
        source_revision: claim.source_revision().as_str().to_owned(),
        source_cutoff: timestamp("2026-09-18T14:00:00Z"),
        generated_at: timestamp("2026-09-18T15:00:00Z"),
        summary_sentences: vec!["summary-secret-sentinel".to_owned()],
        concepts: (1..=10).map(|number| format!("concept-{number}")).collect(),
        memory_candidates: vec![],
    }
    .try_into_generated_revision(&scope)
    .expect("staging generated revision should be valid");

    StagedRevision::try_new(
        claim,
        snapshot.transcript().clone(),
        generated_revision,
        candidates,
    )
    .expect("staging revision fixture should be valid")
}

fn staging_candidate(
    claim: &Claim,
    content_fingerprint: &str,
    memory_id: &str,
    supporting_receipt_ids: &[&str],
) -> StagedMemoryCandidate {
    let supporting_receipt_ids = supporting_receipt_ids
        .iter()
        .map(|receipt_id| (*receipt_id).to_owned())
        .collect::<Vec<_>>();
    StagedMemoryCandidate::try_new(
        content_fingerprint.to_owned(),
        MemoryVersionInput {
            id: memory_id.to_owned(),
            version: 1,
            memory_type: "session-derived".to_owned(),
            title: "memory-title-secret-sentinel".to_owned(),
            content: "memory-content-secret-sentinel".to_owned(),
            created_at: timestamp("2026-09-18T15:00:00Z"),
            updated_at: timestamp("2026-09-18T15:00:00Z"),
            concepts: vec!["candidate-concept".to_owned()],
            files: vec![],
            session_ids: vec![claim.session_id().as_str().to_owned()],
            source_observation_ids: supporting_receipt_ids.clone(),
        },
        supporting_receipt_ids,
    )
    .expect("staging candidate fixture should be valid")
}

fn staged_marker(claim: &Claim) -> Value {
    json!({
        "attempt_id": claim.attempt_id().as_str(),
        "session_id": claim.session_id().as_str(),
        "source_revision": claim.source_revision().as_str(),
        "state": "staged",
    })
}

fn published_marker(claim: &Claim, memory_id: &str) -> Value {
    json!({
        "attempt_id": claim.attempt_id().as_str(),
        "memory_id": memory_id,
        "published_at": "2026-09-22T12:00:00+00:00",
        "state": "publishing",
    })
}

fn unpublished_candidate(ordinal: i32, candidate: &StagedMemoryCandidate) -> Value {
    json!({
        "ordinal": ordinal,
        "content_fingerprint": candidate.content_fingerprint(),
        "memory_id": candidate.memory_id(),
        "canonical_payload": candidate.canonical_payload(),
        "supporting_receipt_ids": candidate
            .supporting_receipt_ids()
            .iter()
            .map(|receipt_id| receipt_id.as_str())
            .collect::<Vec<_>>(),
    })
}

fn unpublished_candidates_row(claim: &Claim, candidates: Vec<Value>) -> Value {
    json!({
        "attempt_id": claim.attempt_id().as_str(),
        "session_id": claim.session_id().as_str(),
        "source_revision": claim.source_revision().as_str(),
        "state": "publishing",
        "candidates": candidates,
    })
}

fn unpublished_candidates_marker(claim: &Claim) -> Value {
    unpublished_candidates_row(claim, vec![])
}

fn retry_marker(claim: &Claim, failure: FailureCategory, next_attempt_at: &str) -> Value {
    json!({
        "attempt_id": claim.attempt_id().as_str(),
        "session_id": claim.session_id().as_str(),
        "source_revision": claim.source_revision().as_str(),
        "state": "retryable",
        "failure_category": failure.to_string(),
        "next_attempt_at": next_attempt_at,
    })
}

fn promotion_marker(claim: &Claim) -> Value {
    json!({
        "attempt_id": claim.attempt_id().as_str(),
        "session_id": claim.session_id().as_str(),
        "source_revision": claim.source_revision().as_str(),
        "state": "complete",
    })
}

fn assert_load_unpublished_candidates_request(request: &TriggerRequest, claim: &Claim) {
    assert_eq!(request.function_id, "database::execute");
    assert_eq!(request.payload.get("db"), Some(&json!(DATABASE)));
    assert_eq!(
        request.payload.get("params"),
        Some(&json!([
            claim.attempt_id().as_str(),
            claim.lease_token().as_str(),
            claim.session_id().as_str(),
            claim.source_revision().as_str(),
        ]))
    );
    let sql = request.payload["sql"]
        .as_str()
        .expect("candidate recovery request should contain production SQL");
    assert!(sql.trim_start().starts_with("WITH request AS"));
    for (fragment, behavior) in [
        (
            "UPDATE public.session_processing_attempts AS attempt",
            "the fenced publishing transition",
        ),
        ("SET state = 'publishing'", "the publishing state"),
        (
            "attempt.state IN ('staged', 'publishing')",
            "the valid recovery states",
        ),
        (
            "candidate.published_at IS NULL",
            "unpublished candidate selection",
        ),
        ("jsonb_agg(", "one fenced candidate aggregate"),
        ("ORDER BY candidate.ordinal", "candidate ordinal ordering"),
        (
            "FILTER (WHERE candidate.attempt_id IS NOT NULL)",
            "empty candidate-array recovery markers",
        ),
        (
            "attempt.attempt_id = request.attempt_id",
            "attempt ID fencing",
        ),
        (
            "attempt.lease_token = request.lease_token",
            "lease token fencing",
        ),
        (
            "attempt.session_id = request.session_id",
            "session ID fencing",
        ),
        (
            "attempt.source_revision = request.source_revision",
            "source revision fencing",
        ),
    ] {
        assert!(
            sql.contains(fragment),
            "candidate recovery SQL must preserve {behavior}: {sql}"
        );
    }
    let lowercase = sql.to_ascii_lowercase();
    for forbidden in [
        "session_records",
        "session_events",
        "raw_observations",
        "canonical_payload =",
        "supporting_receipt_ids =",
        "published_at =",
        "failure_category",
        "next_attempt_at",
        "begin",
        "commit",
        "transaction",
        ";",
    ] {
        assert!(
            !lowercase.contains(forbidden),
            "candidate recovery SQL must stay within transition and read scope without {forbidden}: {sql}"
        );
    }
    assert!(request.action.is_none());
    assert_eq!(request.timeout_ms, None);
}

fn assert_retry_request(
    request: &TriggerRequest,
    claim: &Claim,
    failure: FailureCategory,
    next_attempt_at: &str,
) {
    assert_eq!(request.function_id, "database::execute");
    assert_eq!(request.payload.get("db"), Some(&json!(DATABASE)));
    assert_eq!(
        request.payload.get("params"),
        Some(&json!([
            claim.attempt_id().as_str(),
            claim.lease_token().as_str(),
            claim.session_id().as_str(),
            claim.source_revision().as_str(),
            failure.to_string(),
            next_attempt_at,
        ]))
    );
    let sql = request.payload["sql"]
        .as_str()
        .expect("retry request should contain production SQL");
    assert!(sql.trim_start().starts_with("WITH request AS"));
    for (fragment, behavior) in [
        (
            "UPDATE public.session_processing_attempts AS attempt",
            "the fenced retry transition",
        ),
        ("SET state = 'retryable'", "the retryable state"),
        (
            "failure_category = request.failure_category",
            "the content-safe failure category",
        ),
        (
            "next_attempt_at = request.next_attempt_at",
            "the whole-second retry schedule",
        ),
        (
            "attempt.state IN ('claimed', 'staged', 'publishing')",
            "active-state fencing",
        ),
        (
            "attempt.attempt_id = request.attempt_id",
            "attempt ID fencing",
        ),
        (
            "attempt.lease_token = request.lease_token",
            "lease token fencing",
        ),
        (
            "attempt.session_id = request.session_id",
            "session ID fencing",
        ),
        (
            "attempt.source_revision = request.source_revision",
            "source revision fencing",
        ),
        ("RETURNING", "the exact retry confirmation"),
    ] {
        assert!(
            sql.contains(fragment),
            "retry SQL must preserve {behavior}: {sql}"
        );
    }
    let set_clause = sql
        .split_once("SET ")
        .expect("retry SQL must set retry fields")
        .1
        .split_once("FROM request")
        .expect("retry SQL must use request values")
        .0;
    assert_eq!(
        normalized_sql(set_clause),
        "state = 'retryable', failure_category = request.failure_category, next_attempt_at = request.next_attempt_at"
    );
    let lowercase = sql.to_ascii_lowercase();
    for forbidden in [
        "session_records",
        "session_memory_candidates",
        "lease_expires_at =",
        "source_cutoff =",
        "transcript =",
        "summary_sentences =",
        "summary =",
        "concepts =",
        "generated_at =",
        "published_at =",
        "begin",
        "commit",
        "transaction",
        ";",
    ] {
        assert!(
            !lowercase.contains(forbidden),
            "retry SQL must preserve staged projection, candidates, and current records without {forbidden}: {sql}"
        );
    }
    assert!(request.action.is_none());
    assert_eq!(request.timeout_ms, None);
}

fn assert_promote_request(request: &TriggerRequest, claim: &Claim) {
    assert_eq!(request.function_id, "database::execute");
    assert_eq!(request.payload.get("db"), Some(&json!(DATABASE)));
    assert_eq!(
        request.payload.get("params"),
        Some(&json!([
            claim.attempt_id().as_str(),
            claim.lease_token().as_str(),
            claim.session_id().as_str(),
            claim.source_revision().as_str(),
        ]))
    );
    let sql = request.payload["sql"]
        .as_str()
        .expect("promotion request should contain production SQL");
    assert!(sql.trim_start().starts_with("WITH request AS"));
    let ready_position = sql
        .find("ready AS MATERIALIZED (")
        .expect("promotion SQL must select and fence a ready attempt");
    let promoted_position = sql
        .find("promoted AS (")
        .expect("promotion SQL must upsert the current projection");
    let completed_position = sql
        .find("completed AS (")
        .expect("promotion SQL must complete the fenced attempt");
    assert!(
        ready_position < promoted_position && promoted_position < completed_position,
        "promotion must select readiness, promote the row, then complete the attempt: {sql}"
    );
    for (fragment, behavior) in [
        (
            "UPDATE public.session_processing_attempts AS attempt",
            "the completion transition",
        ),
        ("SET state = 'complete'", "the terminal completion state"),
        (
            "ready AS MATERIALIZED (",
            "the locked ready-attempt selection",
        ),
        ("FOR UPDATE OF attempt", "the still-owned attempt row lock"),
        (
            "attempt.state = 'publishing'",
            "the database publishing-state fence",
        ),
        (
            "FROM public.session_memory_candidates AS candidate",
            "the unpublished candidate guard",
        ),
        (
            "candidate.published_at IS NULL",
            "the pending-candidate guard",
        ),
        (
            "other.state IN ('claimed', 'staged', 'publishing', 'retryable')",
            "the sole-nonterminal-attempt guard",
        ),
        (
            "other.attempt_id <> attempt.attempt_id",
            "exclusion of the current attempt from the nonterminal guard",
        ),
        (
            "attempt.attempt_id = request.attempt_id",
            "attempt ID fencing",
        ),
        (
            "attempt.lease_token = request.lease_token",
            "lease token fencing",
        ),
        (
            "attempt.session_id = request.session_id",
            "session ID fencing",
        ),
        (
            "attempt.source_revision = request.source_revision",
            "source revision fencing",
        ),
        (
            "INSERT INTO public.session_records AS existing_record",
            "the current-record upsert",
        ),
        ("FROM ready", "promotion only from the fenced ready attempt"),
        (
            "ON CONFLICT (session_id) DO UPDATE",
            "in-place refresh replacement",
        ),
        (
            "RETURNING session_id, source_revision",
            "the promoted row identity returned to completion",
        ),
        (
            "attempt.lease_token = ready.lease_token",
            "completion using the still-owned lease token",
        ),
    ] {
        assert!(
            sql.contains(fragment),
            "promotion SQL must preserve {behavior}: {sql}"
        );
    }
    let promoted_cte = &sql[promoted_position..completed_position];
    assert!(
        promoted_cte.contains("RETURNING session_id, source_revision"),
        "promotion must return the row identity that gates completion: {promoted_cte}"
    );
    let completed_end = sql[completed_position..]
        .find(")\nSELECT")
        .map(|offset| completed_position + offset)
        .expect("completed CTE should terminate before the response SELECT");
    let completed_cte = &sql[completed_position..completed_end];
    for fragment in [
        "JOIN promoted",
        "promoted.session_id = ready.session_id",
        "promoted.source_revision = ready.source_revision",
    ] {
        assert!(
            completed_cte.contains(fragment),
            "the completion CTE must consume a successful promoted row via {fragment}: {completed_cte}"
        );
    }
    let completion_set_clause = completed_cte
        .split_once("SET ")
        .expect("completion must set state")
        .1
        .split_once("FROM ready")
        .expect("completion must consume the ready attempt")
        .0;
    assert_eq!(normalized_sql(completion_set_clause), "state = 'complete'");
    let upsert_set_clause = sql
        .split_once("ON CONFLICT (session_id) DO UPDATE")
        .expect("promotion SQL must upsert in place")
        .1
        .split_once("RETURNING session_id, source_revision")
        .expect("upsert must return the promoted session identity")
        .0;
    assert_eq!(
        normalized_sql(upsert_set_clause),
        "SET source_revision = EXCLUDED.source_revision, lifecycle_count = EXCLUDED.lifecycle_count, observation_count = EXCLUDED.observation_count, source_cutoff = EXCLUDED.source_cutoff, transcript = EXCLUDED.transcript, summary_sentences = EXCLUDED.summary_sentences, summary = EXCLUDED.summary, concepts = EXCLUDED.concepts, generated_at = EXCLUDED.generated_at"
    );
    let lowercase = sql.to_ascii_lowercase();
    for forbidden in [
        "failure_category",
        "next_attempt_at =",
        "lease_expires_at =",
        "update public.session_memory_candidates",
        "canonical_payload =",
        "supporting_receipt_ids =",
        "published_at =",
        "session_events",
        "raw_observations",
        "public.memories",
        "begin",
        "commit",
        "transaction",
        ";",
    ] {
        assert!(
            !lowercase.contains(forbidden),
            "promotion SQL must preserve prior candidate and failure state without {forbidden}: {sql}"
        );
    }
    assert!(request.action.is_none());
    assert_eq!(request.timeout_ms, None);
}

fn assert_stage_request(request: &TriggerRequest, claim: &Claim, output: &StagedRevision) {
    assert_eq!(request.function_id, "database::execute");
    assert_eq!(request.payload.get("db"), Some(&json!(DATABASE)));
    assert_eq!(
        request.payload.get("params"),
        Some(&json!([
            claim.attempt_id().as_str(),
            claim.lease_token().as_str(),
            claim.session_id().as_str(),
            claim.source_revision().as_str(),
            output.generated_revision().source_cutoff().to_rfc3339(),
            serde_json::to_value(output.transcript()).expect("transcript should serialize"),
            output.generated_revision().summary_sentences(),
            output.generated_revision().summary(),
            output.generated_revision().concepts(),
            output.generated_revision().generated_at().to_rfc3339(),
            output
                .candidates()
                .iter()
                .map(|candidate| json!({
                    "content_fingerprint": candidate.content_fingerprint(),
                    "canonical_payload": candidate.canonical_payload(),
                    "supporting_receipt_ids": candidate
                        .supporting_receipt_ids()
                        .iter()
                        .map(|receipt_id| receipt_id.as_str())
                        .collect::<Vec<_>>(),
                }))
                .collect::<Vec<_>>(),
        ]))
    );
    let sql = request.payload["sql"]
        .as_str()
        .expect("stage request should contain production SQL");
    assert_stage_sql(sql);
    assert!(request.action.is_none());
    assert_eq!(request.timeout_ms, None);
}

fn assert_stage_sql(sql: &str) {
    assert!(sql.trim_start().starts_with("WITH request AS"));
    for (fragment, behavior) in [
        (
            "UPDATE public.session_processing_attempts AS attempt",
            "the processor-owned attempt projection",
        ),
        ("SET state = 'staged'", "the staged transition"),
        ("source_cutoff = request.source_cutoff", "the source cutoff"),
        ("transcript = request.transcript", "the complete transcript"),
        (
            "summary_sentences = ARRAY(",
            "the generated summary sentence array",
        ),
        ("summary = request.summary", "the rendered summary"),
        ("concepts = ARRAY(", "the generated concept array"),
        ("generated_at = request.generated_at", "the generation time"),
        (
            "attempt.attempt_id = request.attempt_id",
            "attempt ID fencing",
        ),
        (
            "attempt.lease_token = request.lease_token",
            "lease token fencing",
        ),
        (
            "attempt.session_id = request.session_id",
            "session ID fencing",
        ),
        (
            "attempt.source_revision = request.source_revision",
            "source revision fencing",
        ),
        ("attempt.state = 'claimed'", "claimed-state fencing"),
        (
            "INSERT INTO public.session_memory_candidates",
            "candidate persistence",
        ),
        (
            "jsonb_array_elements(request.candidates) WITH ORDINALITY",
            "ordered candidate decoding",
        ),
        (
            "ORDER BY candidate.ordinality",
            "candidate ordinal preservation",
        ),
        (
            "ON CONFLICT (session_id, content_fingerprint) DO NOTHING",
            "prior candidate preservation",
        ),
        (
            "RETURNING attempt.attempt_id, attempt.session_id, attempt.source_revision, attempt.state",
            "the exact safe staged marker",
        ),
    ] {
        assert!(
            sql.contains(fragment),
            "stage SQL must preserve {behavior}: {sql}"
        );
    }
    let set_clause = sql
        .split_once("SET ")
        .expect("stage SQL should update the attempt")
        .1
        .split_once("FROM request")
        .expect("stage SQL should use request values")
        .0;
    assert_eq!(
        normalized_sql(set_clause),
        "state = 'staged', source_cutoff = request.source_cutoff, transcript = request.transcript, summary_sentences = ARRAY( SELECT sentence.value FROM jsonb_array_elements_text(request.summary_sentences) WITH ORDINALITY AS sentence(value, ordinality) ORDER BY sentence.ordinality ), summary = request.summary, concepts = ARRAY( SELECT concept.value FROM jsonb_array_elements_text(request.concepts) WITH ORDINALITY AS concept(value, ordinality) ORDER BY concept.ordinality ), generated_at = request.generated_at"
    );
    let lowercase = sql.to_ascii_lowercase();
    for forbidden in [
        "session_records",
        "session_events",
        "raw_observations",
        "public.memories",
        "lease_expires_at",
        "next_attempt_at",
        "failure_category",
        "update public.session_memory_candidates",
        "canonical_payload =",
        "supporting_receipt_ids =",
        "published_at =",
        "do update",
        "begin",
        "commit",
        "transaction",
        ";",
    ] {
        assert!(
            !lowercase.contains(forbidden),
            "stage SQL must stay within the attempt and candidate boundary without {forbidden}: {sql}"
        );
    }
}

fn assert_mark_memory_published_request(request: &TriggerRequest, claim: &Claim, memory_id: &str) {
    assert_eq!(request.function_id, "database::execute");
    assert_eq!(request.payload.get("db"), Some(&json!(DATABASE)));
    assert_eq!(
        request.payload.get("params"),
        Some(&json!([
            claim.attempt_id().as_str(),
            claim.lease_token().as_str(),
            claim.session_id().as_str(),
            claim.source_revision().as_str(),
            memory_id,
        ]))
    );
    let sql = request.payload["sql"]
        .as_str()
        .expect("publication request should contain production SQL");
    assert_mark_memory_published_sql(sql);
    assert!(request.action.is_none());
    assert_eq!(request.timeout_ms, None);
}

fn assert_mark_memory_published_sql(sql: &str) {
    assert!(sql.trim_start().starts_with("WITH request AS"));
    for (fragment, behavior) in [
        (
            "FROM public.session_memory_candidates AS candidate",
            "the target candidate check",
        ),
        (
            "UPDATE public.session_processing_attempts AS attempt",
            "the publishing transition",
        ),
        ("SET state = 'publishing'", "the publishing state"),
        (
            "attempt.state IN ('staged', 'publishing')",
            "the active publication states",
        ),
        (
            "EXISTS (SELECT 1 FROM target)",
            "target-gated state transition",
        ),
        (
            "UPDATE public.session_memory_candidates AS candidate",
            "the publication marker update",
        ),
        (
            "published_at = COALESCE(candidate.published_at, CURRENT_TIMESTAMP)",
            "the idempotent timestamp marker",
        ),
        (
            "attempt.attempt_id = request.attempt_id",
            "attempt ID fencing",
        ),
        (
            "attempt.lease_token = request.lease_token",
            "lease token fencing",
        ),
        (
            "attempt.session_id = request.session_id",
            "session ID fencing",
        ),
        (
            "attempt.source_revision = request.source_revision",
            "source revision fencing",
        ),
        (
            "candidate.memory_id = request.memory_id",
            "candidate memory ID fencing",
        ),
        (
            "RETURNING candidate.attempt_id, candidate.memory_id, candidate.published_at",
            "the publication marker",
        ),
    ] {
        assert!(
            sql.contains(fragment),
            "publication SQL must preserve {behavior}: {sql}"
        );
    }
    let lowercase = sql.to_ascii_lowercase();
    for forbidden in [
        "session_records",
        "session_events",
        "raw_observations",
        "public.memories",
        "source_cutoff",
        "transcript",
        "summary",
        "concepts",
        "generated_at",
        "canonical_payload =",
        "supporting_receipt_ids =",
        "content_fingerprint =",
        "begin",
        "commit",
        "transaction",
        ";",
    ] {
        assert!(
            !lowercase.contains(forbidden),
            "publication SQL must update only state and published_at, not {forbidden}: {sql}"
        );
    }
}
