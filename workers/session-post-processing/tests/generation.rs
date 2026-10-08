mod support;

use std::{
    fmt::{Debug, Display},
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use memory_store::contracts::ValidatedMemoryVersion;
use serde::Serialize;
use serde_json::{Value, json};
use session_post_processing::{
    contracts::{
        FailureCategory, GenerationInput, LeaseToken, MemoryCandidate, MemoryCandidateInput,
        MemoryScope, ProviderOperation, Retryability, SourceEventType, StructuredGenerationRequest,
        StructuredGenerationResponse, StructuredGenerationResponseInput, Transcript,
        TranscriptEntryInput,
    },
    generation::{
        GenerationError, GenerationPipeline, chunk_generation_input, normalize_memory_candidates,
        validate_final_response, validate_map_response,
    },
    ports::{LeaseError, LeasePermit, ModelProvider, ProviderError},
};
use support::ports::{DeterministicGate, ManualClock, ScriptedLease, ScriptedModelProvider};

const SESSION_ID: &str = "generation-session";
const LIFECYCLE_RECEIPT: &str = "11111111-1111-4111-8111-111111111111";
const OBSERVATION_RECEIPT: &str = "22222222-2222-4222-8222-222222222222";
const OUTSIDE_RECEIPT: &str = "33333333-3333-4333-8333-333333333333";
const SECOND_OBSERVATION_RECEIPT: &str = "44444444-4444-4444-8444-444444444444";
const SESSION_END_RECEIPT: &str = "55555555-5555-4555-8555-555555555555";
const RAW_TRANSCRIPT_SENTINEL: &str = "raw-transcript-secret-sentinel";
const MAP_CANDIDATE_SENTINEL: &str = "map-candidate-secret-sentinel";
const OVERSIZED_REDUCTION_SENTINEL: &str = "oversized-reduction-secret-sentinel";
const NORMALIZATION_TITLE_SENTINEL: &str = "normalization-title-secret-sentinel";
const NORMALIZATION_CONTENT_SENTINEL: &str = "normalization-content-secret-sentinel";
const NORMALIZATION_CONCEPT_SENTINEL: &str = "normalization-concept-secret-sentinel";

#[tokio::test]
async fn map_pipeline_returns_one_validated_result_per_ordered_chunk_with_bounded_permits() {
    let input = generation_input();
    let chunks = chunk_generation_input(&input, 512)
        .expect("ordered transcript should be chunked within the configured bound");
    assert!(
        chunks.len() > 1,
        "fixture should require multiple map requests"
    );

    let now = timestamp("2026-09-18T14:00:00Z");
    let clock = ManualClock::new(now);
    let lease = ScriptedLease::new((0..chunks.len()).map(|_| {
        Ok::<_, LeaseError>(LeasePermit::new(
            lease_token(),
            now + ChronoDuration::seconds(30),
        ))
    }));
    let provider = ScriptedModelProvider::new((0..chunks.len()).map(|_| {
        Ok::<_, ProviderError>(map_response_with_candidates(vec![
            OBSERVATION_RECEIPT.to_owned(),
        ]))
    }));
    let pipeline = GenerationPipeline::new(&clock, &lease, &provider, Duration::from_secs(60), 512);

    let results = pipeline
        .map(&input)
        .await
        .expect("every ordered chunk should produce one validated map result");

    assert_eq!(results.len(), chunks.len());
    for result in &results {
        assert_eq!(result.summary_sentences(), ["Map sentence."]);
        assert_eq!(result.concepts(), ["map concept"]);
        assert_eq!(result.memory_candidates().len(), 1);
        assert_eq!(
            result.memory_candidates()[0].supporting_receipt_ids()[0].as_str(),
            OBSERVATION_RECEIPT
        );
    }
    assert_eq!(provider.calls().len(), chunks.len());
    for (call, chunk) in provider.calls().iter().zip(&chunks) {
        assert_eq!(call.request().operation(), ProviderOperation::Map);
        assert_eq!(
            call.request().prompt().as_str(),
            chunk.payload(),
            "map requests must retain the task 3.2 chunk serialization"
        );
    }
    assert_eq!(
        lease
            .calls()
            .iter()
            .map(|call| call.maximum)
            .collect::<Vec<_>>(),
        vec![Duration::from_secs(60); chunks.len()]
    );
}

#[tokio::test]
async fn map_pipeline_preserves_provider_failure_category_and_retryability_without_content() {
    let input = generation_input_with_observation_data(json!({
        "opaque": RAW_TRANSCRIPT_SENTINEL,
    }));
    let now = timestamp("2026-09-18T14:00:00Z");

    for (provider_error, category, retryability) in [
        (
            ProviderError::transient(),
            FailureCategory::ProviderTransient,
            Retryability::AfterBackoff,
        ),
        (
            ProviderError::contract(),
            FailureCategory::ProviderContract,
            Retryability::AfterBackoff,
        ),
        (
            ProviderError::authentication(),
            FailureCategory::ProviderAuthentication,
            Retryability::AfterBackoff,
        ),
        (
            ProviderError::unsupported_contract(),
            FailureCategory::ProviderUnsupportedContract,
            Retryability::AfterBackoff,
        ),
        (
            ProviderError::request_size(),
            FailureCategory::ProviderRequestSize,
            Retryability::AfterBackoff,
        ),
    ] {
        let clock = ManualClock::new(now);
        let lease = ScriptedLease::new([Ok(LeasePermit::new(
            lease_token(),
            now + ChronoDuration::seconds(30),
        ))]);
        let provider = ScriptedModelProvider::new([Err(provider_error)]);
        let pipeline =
            GenerationPipeline::new(&clock, &lease, &provider, Duration::from_secs(60), 10_000);

        let error = pipeline
            .map(&input)
            .await
            .expect_err("provider failures must stop the map pipeline");

        assert_failure_projection(
            &error,
            "provider",
            "failed",
            category,
            retryability,
            &[RAW_TRANSCRIPT_SENTINEL, "provider-output-secret-sentinel"],
        );
    }
}

#[tokio::test]
async fn map_pipeline_classifies_model_timeout_as_retryable_provider_transient() {
    let input = generation_input_with_observation_data(json!({
        "opaque": RAW_TRANSCRIPT_SENTINEL,
    }));
    let now = timestamp("2026-09-18T14:00:00Z");
    let clock = ManualClock::new(now);
    let lease = ScriptedLease::new([Ok(LeasePermit::new(
        lease_token(),
        now + ChronoDuration::seconds(2),
    ))]);
    let provider = BlockingModelProvider::new();
    let pipeline = GenerationPipeline::new(
        &clock,
        &lease,
        &provider,
        Duration::from_millis(100),
        10_000,
    );

    let result = tokio::time::timeout(Duration::from_secs(2), async {
        let (result, ()) = tokio::join!(pipeline.map(&input), provider.wait_for_call());
        result
    })
    .await
    .expect("model timeout must complete within the outer test bound");
    let error = result.expect_err("a delayed provider must exceed the model timeout");

    assert_failure_projection(
        &error,
        "provider",
        "timeout",
        FailureCategory::ProviderTransient,
        Retryability::AfterBackoff,
        &[RAW_TRANSCRIPT_SENTINEL, "provider-output-secret-sentinel"],
    );
}

#[tokio::test]
async fn map_pipeline_classifies_permit_timeout_as_retryable_lease_loss() {
    let input = generation_input_with_observation_data(json!({
        "opaque": RAW_TRANSCRIPT_SENTINEL,
    }));
    let now = timestamp("2026-09-18T14:00:00Z");
    let clock = ManualClock::new(now);
    let lease = ScriptedLease::new([Ok(LeasePermit::new(
        lease_token(),
        now + ChronoDuration::milliseconds(100),
    ))]);
    let provider = BlockingModelProvider::new();
    let pipeline =
        GenerationPipeline::new(&clock, &lease, &provider, Duration::from_secs(2), 10_000);

    let result = tokio::time::timeout(Duration::from_secs(2), async {
        let (result, ()) = tokio::join!(pipeline.map(&input), provider.wait_for_call());
        result
    })
    .await
    .expect("permit timeout must complete within the outer test bound");
    let error = result.expect_err("a delayed provider must not outlive its permit");

    assert_failure_projection(
        &error,
        "lease",
        "deadline_elapsed",
        FailureCategory::LeaseLost,
        Retryability::AfterLeaseExpiry,
        &[RAW_TRANSCRIPT_SENTINEL, "provider-output-secret-sentinel"],
    );
}

#[tokio::test]
async fn map_pipeline_rejects_candidates_outside_the_exact_refresh_memory_scope() {
    let input = GenerationInput::refresh(
        generation_input().transcript().clone(),
        vec![OBSERVATION_RECEIPT.to_owned()],
    )
    .expect("refresh fixture should permit its newly recorded observation");
    let now = timestamp("2026-09-18T14:00:00Z");
    let clock = ManualClock::new(now);
    let lease = ScriptedLease::new([Ok(LeasePermit::new(
        lease_token(),
        now + ChronoDuration::seconds(30),
    ))]);
    let provider = ScriptedModelProvider::new([Ok(map_response_with_candidates(vec![
        OUTSIDE_RECEIPT.to_owned(),
    ]))]);
    let pipeline =
        GenerationPipeline::new(&clock, &lease, &provider, Duration::from_secs(60), 10_000);

    let error = pipeline
        .map(&input)
        .await
        .expect_err("map candidate evidence outside the refresh scope must be rejected");

    assert_eq!(error.field(), "supporting_receipt_ids");
    assert_eq!(error.code(), "outside_memory_scope");
    assert_error_is_safe(&error, &[RAW_TRANSCRIPT_SENTINEL, OUTSIDE_RECEIPT]);
    assert_eq!(lease.calls().len(), 1);
    assert_eq!(provider.calls().len(), 1);
}

#[tokio::test]
async fn map_pipeline_stops_before_later_chunks_when_a_lease_is_lost() {
    let input = generation_input();
    let chunks = chunk_generation_input(&input, 512)
        .expect("ordered transcript should be chunked within the configured bound");
    assert!(
        chunks.len() > 1,
        "fixture should require multiple map requests"
    );

    let now = timestamp("2026-09-18T14:00:00Z");
    let clock = ManualClock::new(now);
    let lease = ScriptedLease::new([
        Ok(LeasePermit::new(
            lease_token(),
            now + ChronoDuration::seconds(30),
        )),
        Err(LeaseError::lost()),
    ]);
    let provider = ScriptedModelProvider::new([Ok(map_response_with_candidates(vec![
        OBSERVATION_RECEIPT.to_owned(),
    ]))]);
    let pipeline = GenerationPipeline::new(&clock, &lease, &provider, Duration::from_secs(60), 512);

    let error = pipeline
        .map(&input)
        .await
        .expect_err("lease loss must stop later map calls");

    assert_eq!(error.field(), "lease");
    assert_eq!(error.code(), "lost");
    assert_eq!(lease.calls().len(), 2);
    assert_eq!(provider.calls().len(), 1);
}

#[tokio::test]
async fn map_pipeline_does_not_call_the_provider_with_an_expired_permit() {
    let input = generation_input();
    let now = timestamp("2026-09-18T14:00:00Z");
    let clock = ManualClock::new(now);
    let lease = ScriptedLease::new([Ok(LeasePermit::new(lease_token(), now))]);
    let provider = ScriptedModelProvider::new([Ok(map_response_with_candidates(vec![
        OBSERVATION_RECEIPT.to_owned(),
    ]))]);
    let pipeline =
        GenerationPipeline::new(&clock, &lease, &provider, Duration::from_secs(60), 10_000);

    let error = pipeline
        .map(&input)
        .await
        .expect_err("an expired permit must prevent a provider call");

    assert_eq!(error.field(), "lease");
    assert_eq!(error.code(), "deadline_expired");
    assert_eq!(lease.calls().len(), 1);
    assert!(provider.calls().is_empty());
}

#[tokio::test]
async fn map_pipeline_accepts_a_zero_memory_refresh_result() {
    let input = GenerationInput::refresh(lifecycle_only_transcript(), Vec::new())
        .expect("a refresh with no new observations should be valid");
    let chunks = chunk_generation_input(&input, 512)
        .expect("lifecycle-only transcript should be chunked within the configured bound");
    let now = timestamp("2026-09-18T14:00:00Z");
    let clock = ManualClock::new(now);
    let lease = ScriptedLease::new((0..chunks.len()).map(|_| {
        Ok::<_, LeaseError>(LeasePermit::new(
            lease_token(),
            now + ChronoDuration::seconds(30),
        ))
    }));
    let provider = ScriptedModelProvider::new(
        (0..chunks.len()).map(|_| Ok::<_, ProviderError>(zero_memory_map_response())),
    );
    let pipeline = GenerationPipeline::new(&clock, &lease, &provider, Duration::from_secs(60), 512);

    let results = pipeline
        .map(&input)
        .await
        .expect("a map response with no candidates should remain valid");

    assert_eq!(results.len(), chunks.len());
    assert!(
        results
            .iter()
            .all(|result| result.memory_candidates().is_empty())
    );
}

#[tokio::test]
async fn reduce_pipeline_hierarchically_combines_bounded_summary_and_concept_material() {
    let input = generation_input();
    let map_results = (1..=16)
        .map(|number| {
            map_response_with_candidate_content(
                format!("Map {number} summary."),
                format!("map concept {number}"),
                MAP_CANDIDATE_SENTINEL.to_owned(),
            )
        })
        .collect::<Vec<_>>();
    let now = timestamp("2026-09-18T14:00:00Z");
    let clock = ManualClock::new(now);
    let lease = ScriptedLease::new((0..32).map(|_| {
        Ok::<_, LeaseError>(LeasePermit::new(
            lease_token(),
            now + ChronoDuration::seconds(30),
        ))
    }));
    let provider = ScriptedModelProvider::new(
        (0..32).map(|_| Ok::<_, ProviderError>(final_reduce_response())),
    );
    let pipeline = GenerationPipeline::new(&clock, &lease, &provider, Duration::from_secs(60), 384);

    let result = pipeline
        .reduce(&input, &map_results)
        .await
        .expect("bounded map material should reduce hierarchically to one final result");

    assert_eq!(result.summary_sentences(), ["Final sentence."]);
    assert_eq!(result.concepts().len(), 10);
    assert!(result.memory_candidates().is_empty());
    assert!(
        provider.calls().len() > 1,
        "fixture must require more than one reduction level"
    );
    assert_eq!(provider.calls().len(), lease.calls().len());
    for call in provider.calls() {
        assert_eq!(call.request().operation(), ProviderOperation::Reduce);
        assert!(call.request().prompt().as_str().len() <= 384);
        assert!(
            !call
                .request()
                .prompt()
                .as_str()
                .contains(MAP_CANDIDATE_SENTINEL)
        );
        let payload = serde_json::from_str::<Value>(call.request().prompt().as_str())
            .expect("reduce payload should be valid JSON");
        assert!(payload["items"].is_array());
        assert!(
            payload["items"]
                .as_array()
                .expect("reduce payload items should be an array")
                .iter()
                .all(|item| item.get("memory_candidates").is_none())
        );
    }
}

#[tokio::test]
async fn reduce_pipeline_stops_before_later_requests_when_a_lease_is_lost() {
    let input = generation_input();
    let map_results = (1..=16)
        .map(|number| {
            map_response_with_candidate_content(
                format!("Map {number} summary."),
                format!("map concept {number}"),
                MAP_CANDIDATE_SENTINEL.to_owned(),
            )
        })
        .collect::<Vec<_>>();
    let now = timestamp("2026-09-18T14:00:00Z");
    let clock = ManualClock::new(now);
    let lease = ScriptedLease::new([
        Ok(LeasePermit::new(
            lease_token(),
            now + ChronoDuration::seconds(30),
        )),
        Err(LeaseError::lost()),
    ]);
    let provider = ScriptedModelProvider::new([Ok(final_reduce_response())]);
    let pipeline = GenerationPipeline::new(&clock, &lease, &provider, Duration::from_secs(60), 384);

    let error = pipeline
        .reduce(&input, &map_results)
        .await
        .expect_err("lease loss must stop later reduction calls");

    assert_eq!(error.field(), "lease");
    assert_eq!(error.code(), "lost");
    assert_eq!(lease.calls().len(), 2);
    assert_eq!(provider.calls().len(), 1);
}

#[tokio::test]
async fn reduce_pipeline_stops_after_a_local_final_validation_failure() {
    let input = generation_input();
    let map_results = (1..=16)
        .map(|number| {
            map_response_with_candidate_content(
                format!("Map {number} summary."),
                format!("map concept {number}"),
                MAP_CANDIDATE_SENTINEL.to_owned(),
            )
        })
        .collect::<Vec<_>>();
    let now = timestamp("2026-09-18T14:00:00Z");
    let clock = ManualClock::new(now);
    let lease = ScriptedLease::new([Ok(LeasePermit::new(
        lease_token(),
        now + ChronoDuration::seconds(30),
    ))]);
    let provider = ScriptedModelProvider::new([Ok(reduce_response_with_concepts(9))]);
    let pipeline = GenerationPipeline::new(&clock, &lease, &provider, Duration::from_secs(60), 384);

    let error = pipeline
        .reduce(&input, &map_results)
        .await
        .expect_err("invalid final concepts must stop the reduce pipeline");

    assert_failure_projection(
        &error,
        "concepts",
        "wrong_count",
        FailureCategory::ProviderContract,
        Retryability::AfterBackoff,
        &[MAP_CANDIDATE_SENTINEL],
    );
    assert_eq!(lease.calls().len(), 1);
    assert_eq!(provider.calls().len(), 1);
}

#[tokio::test]
async fn reduce_pipeline_preserves_provider_failure_without_later_requests() {
    let input = generation_input();
    let map_results = vec![map_response_with_candidate_content(
        "Map summary.".to_owned(),
        "map concept".to_owned(),
        MAP_CANDIDATE_SENTINEL.to_owned(),
    )];
    let now = timestamp("2026-09-18T14:00:00Z");
    let clock = ManualClock::new(now);
    let lease = ScriptedLease::new([Ok(LeasePermit::new(
        lease_token(),
        now + ChronoDuration::seconds(30),
    ))]);
    let provider = ScriptedModelProvider::new([Err(ProviderError::transient())]);
    let pipeline = GenerationPipeline::new(&clock, &lease, &provider, Duration::from_secs(60), 384);

    let error = pipeline
        .reduce(&input, &map_results)
        .await
        .expect_err("provider failure must stop the reduce pipeline");

    assert_failure_projection(
        &error,
        "provider",
        "failed",
        FailureCategory::ProviderTransient,
        Retryability::AfterBackoff,
        &[MAP_CANDIDATE_SENTINEL],
    );
    assert_eq!(lease.calls().len(), 1);
    assert_eq!(provider.calls().len(), 1);
}

#[tokio::test]
async fn reduce_pipeline_rejects_a_bound_that_cannot_merge_multiple_items_without_provider_calls() {
    let input = generation_input();
    let map_results = vec![
        map_response_without_candidates("a", "b"),
        map_response_without_candidates("c", "d"),
    ];
    let now = timestamp("2026-09-18T14:00:00Z");
    let clock = ManualClock::new(now);
    let lease = ScriptedLease::new([]);
    let provider = ScriptedModelProvider::new([]);
    let pipeline = GenerationPipeline::new(&clock, &lease, &provider, Duration::from_secs(60), 80);

    let error = pipeline
        .reduce(&input, &map_results)
        .await
        .expect_err("a non-progressing bound must fail before a provider request");

    assert_failure_projection(
        &error,
        "maximum_bytes",
        "too_small",
        FailureCategory::ProviderContract,
        Retryability::AfterBackoff,
        &[MAP_CANDIDATE_SENTINEL],
    );
    assert!(lease.calls().is_empty());
    assert!(provider.calls().is_empty());
}

#[tokio::test]
async fn reduce_pipeline_rejects_an_oversized_middle_item_before_any_external_call() {
    let input = generation_input();
    let oversized_summary = format!("{OVERSIZED_REDUCTION_SENTINEL}{}", "x".repeat(128));
    let map_results = vec![
        map_response_without_candidates("a", "b"),
        map_response_without_candidates("c", "d"),
        map_response_without_candidates(&oversized_summary, "oversized"),
        map_response_without_candidates("e", "f"),
        map_response_without_candidates("g", "h"),
    ];
    let now = timestamp("2026-09-18T14:00:00Z");
    let clock = ManualClock::new(now);
    let lease = ScriptedLease::new((0..3).map(|_| {
        Ok::<_, LeaseError>(LeasePermit::new(
            lease_token(),
            now + ChronoDuration::seconds(30),
        ))
    }));
    let provider =
        ScriptedModelProvider::new((0..3).map(|_| Ok::<_, ProviderError>(final_reduce_response())));
    let pipeline = GenerationPipeline::new(&clock, &lease, &provider, Duration::from_secs(60), 128);

    let error = pipeline
        .reduce(&input, &map_results)
        .await
        .expect_err("an oversized reduction item must be rejected before external calls");

    assert!(lease.calls().is_empty());
    assert!(provider.calls().is_empty());
    assert_failure_projection(
        &error,
        "maximum_bytes",
        "too_small",
        FailureCategory::ProviderContract,
        Retryability::AfterBackoff,
        &[OVERSIZED_REDUCTION_SENTINEL],
    );
}

#[test]
fn revision_specific_memory_scopes_include_initial_observations_and_only_valid_refresh_receipts() {
    let initial_transcript = multi_observation_transcript();
    let expected_initial_receipt_ids = initial_transcript
        .entries()
        .iter()
        .filter(|entry| entry.is_observation())
        .map(|entry| entry.receipt_id().as_str().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        expected_initial_receipt_ids,
        [
            OBSERVATION_RECEIPT.to_owned(),
            SECOND_OBSERVATION_RECEIPT.to_owned()
        ]
    );

    let initial = GenerationInput::initial(initial_transcript)
        .expect("initial processing should permit every observation receipt");
    assert_eq!(
        initial
            .memory_scope()
            .receipt_ids()
            .map(|receipt_id| receipt_id.as_str().to_owned())
            .collect::<Vec<_>>(),
        expected_initial_receipt_ids
    );

    let transcript = generation_input().transcript().clone();
    let empty_refresh = GenerationInput::refresh(transcript.clone(), Vec::new())
        .expect("a refresh may have no newly recorded observations");
    assert!(empty_refresh.memory_scope().receipt_ids().next().is_none());

    let refresh =
        GenerationInput::refresh(transcript.clone(), vec![OBSERVATION_RECEIPT.to_owned()])
            .expect("a newly recorded observation should be permitted for refresh");
    assert_eq!(
        refresh
            .memory_scope()
            .receipt_ids()
            .map(|receipt_id| receipt_id.as_str())
            .collect::<Vec<_>>(),
        [OBSERVATION_RECEIPT]
    );

    let missing = GenerationInput::refresh(transcript.clone(), vec![OUTSIDE_RECEIPT.to_owned()])
        .expect_err("a receipt absent from the transcript must be rejected");
    assert_eq!(missing.field(), "memory_scope");
    assert_eq!(missing.code(), "not_observation_receipt");

    let lifecycle = GenerationInput::refresh(transcript, vec![LIFECYCLE_RECEIPT.to_owned()])
        .expect_err("a lifecycle receipt must not support a refresh memory");
    assert_eq!(lifecycle.field(), "memory_scope");
    assert_eq!(lifecycle.code(), "not_observation_receipt");

    let initial_without_observations = GenerationInput::initial(lifecycle_only_transcript())
        .expect("initial processing should permit a zero-observation transcript");
    assert!(
        initial_without_observations
            .memory_scope()
            .receipt_ids()
            .next()
            .is_none()
    );
}

#[test]
fn generation_input_chunks_are_utf8_bounded_and_preserve_fragment_provenance_and_order() {
    let input = generation_input();

    let chunks = chunk_generation_input(&input, 512)
        .expect("ordered transcript should be chunked within the configured bound");

    assert!(!chunks.is_empty());
    assert!(chunks.iter().all(|chunk| chunk.byte_len() <= 512));
    for chunk in &chunks {
        let payload = serde_json::from_str::<Value>(chunk.payload())
            .expect("chunk payload should be valid JSON");
        let payload_fragments = payload["fragments"]
            .as_array()
            .expect("chunk payload should retain fragments");
        assert_eq!(payload_fragments.len(), chunk.fragments().len());
        for (payload_fragment, fragment) in payload_fragments.iter().zip(chunk.fragments()) {
            assert_eq!(
                payload_fragment["receipt_id"],
                json!(fragment.receipt_id().as_str())
            );
            assert_eq!(
                payload_fragment["fragment_index"],
                json!(fragment.fragment_index())
            );
            assert_eq!(payload_fragment["content"], json!(fragment.content()));
        }
    }

    let fragments = chunks
        .iter()
        .flat_map(|chunk| chunk.fragments())
        .collect::<Vec<_>>();
    let expected_entries = input
        .transcript()
        .entries()
        .iter()
        .map(|entry| {
            (
                entry.receipt_id().as_str().to_owned(),
                serde_json::to_string(entry).expect("transcript entry should serialize"),
            )
        })
        .collect::<Vec<_>>();

    let mut groups = Vec::<(String, Vec<_>)>::new();
    for fragment in fragments {
        if groups
            .last()
            .is_none_or(|(receipt_id, _)| receipt_id != fragment.receipt_id().as_str())
        {
            groups.push((fragment.receipt_id().as_str().to_owned(), Vec::new()));
        }
        groups
            .last_mut()
            .expect("fragment group should exist")
            .1
            .push(fragment);
    }

    assert_eq!(groups.len(), expected_entries.len());
    for ((receipt_id, fragments), (expected_receipt_id, expected_entry)) in
        groups.iter().zip(expected_entries)
    {
        assert_eq!(receipt_id, &expected_receipt_id);
        assert_eq!(
            fragments
                .iter()
                .map(|fragment| fragment.content())
                .collect::<String>(),
            expected_entry
        );
        assert_eq!(
            fragments
                .iter()
                .map(|fragment| fragment.fragment_index())
                .collect::<Vec<_>>(),
            (1..=fragments.len()).collect::<Vec<_>>()
        );
    }

    assert!(
        groups[1].1.len() > 1,
        "the oversized Unicode observation should be fragmented"
    );
}

#[test]
fn generation_input_rejects_a_limit_that_cannot_fit_a_fragment_envelope_without_leaking_content() {
    let input = generation_input_with_observation_data(json!({
        "opaque": RAW_TRANSCRIPT_SENTINEL,
    }));

    let error = chunk_generation_input(&input, 1)
        .expect_err("a fragment envelope cannot fit in a one-byte chunk");

    assert_eq!(error.field(), "maximum_bytes");
    assert_eq!(error.code(), "too_small");
    assert_error_is_safe(&error, &[RAW_TRANSCRIPT_SENTINEL]);
}

#[test]
fn local_response_boundary_enforces_closed_schema_evidence_and_final_summary_contract() {
    let input = generation_input();
    let scope = input.memory_scope();
    let accepted = response_json(
        (1..=10)
            .map(|number| format!("  Concept {number}  "))
            .collect(),
        json!([OBSERVATION_RECEIPT]),
    );
    let accepted_bytes = serde_json::to_vec(&accepted).expect("fixture should serialize");

    let response = validate_final_response(&accepted_bytes, scope)
        .expect("valid final structured response should be accepted");
    assert_eq!(
        response.summary_sentences(),
        ["First sentence.", "Second sentence."]
    );
    assert_eq!(response.concepts().len(), 10);
    assert_eq!(response.concepts()[0], "Concept 1");
    assert_eq!(response.memory_candidates().len(), 1);

    let map_response = response_json(vec!["Map concept".to_owned()], json!([OBSERVATION_RECEIPT]));
    assert!(
        validate_map_response(
            &serde_json::to_vec(&map_response).expect("fixture should serialize"),
            scope,
        )
        .is_ok(),
        "map responses may contain a non-final concept count"
    );

    assert_generation_error(
        validate_final_response(
            br#"{"summary_sentences":["provider-output-secret-sentinel"]"#,
            scope,
        ),
        "response",
        "malformed",
    );

    let mut unknown_field = accepted.clone();
    unknown_field["provider-output-secret-sentinel"] = json!(true);
    assert_generation_error(
        validate_final_response(
            &serde_json::to_vec(&unknown_field).expect("fixture should serialize"),
            scope,
        ),
        "response",
        "malformed",
    );

    let mut malformed = accepted.clone();
    malformed["memory_candidates"][0]["unexpected"] = json!(true);
    assert_generation_error(
        validate_final_response(
            &serde_json::to_vec(&malformed).expect("fixture should serialize"),
            scope,
        ),
        "response",
        "malformed",
    );

    let invalid_evidence = response_json(
        (1..=10).map(|number| format!("Concept {number}")).collect(),
        json!([OUTSIDE_RECEIPT]),
    );
    assert_generation_error(
        validate_final_response(
            &serde_json::to_vec(&invalid_evidence).expect("fixture should serialize"),
            scope,
        ),
        "supporting_receipt_ids",
        "outside_memory_scope",
    );

    let mut empty_sentence = accepted.clone();
    empty_sentence["summary_sentences"] = json!([""]);
    assert_generation_error(
        validate_final_response(
            &serde_json::to_vec(&empty_sentence).expect("fixture should serialize"),
            scope,
        ),
        "summary_sentences",
        "empty_sentence",
    );

    let mut overlong_summary = accepted.clone();
    overlong_summary["summary_sentences"] = json!(["one", "two", "three", "four", "five", "six"]);
    assert_generation_error(
        validate_final_response(
            &serde_json::to_vec(&overlong_summary).expect("fixture should serialize"),
            scope,
        ),
        "summary_sentences",
        "too_many",
    );

    let wrong_count = response_json(
        (1..=9).map(|number| format!("Concept {number}")).collect(),
        json!([OBSERVATION_RECEIPT]),
    );
    assert_generation_error(
        validate_final_response(
            &serde_json::to_vec(&wrong_count).expect("fixture should serialize"),
            scope,
        ),
        "concepts",
        "wrong_count",
    );

    let lowercase_distinct = response_json(
        vec![
            "Straße".to_owned(),
            "STRASSE".to_owned(),
            "Concept 3".to_owned(),
            "Concept 4".to_owned(),
            "Concept 5".to_owned(),
            "Concept 6".to_owned(),
            "Concept 7".to_owned(),
            "Concept 8".to_owned(),
            "Concept 9".to_owned(),
            "Concept 10".to_owned(),
        ],
        json!([OBSERVATION_RECEIPT]),
    );
    assert!(
        validate_final_response(
            &serde_json::to_vec(&lowercase_distinct).expect("fixture should serialize"),
            scope,
        )
        .is_ok(),
        "Unicode lowercase comparison must not apply full case folding"
    );

    let duplicate_lowercase = response_json(
        vec![
            "Concept One".to_owned(),
            "concept one".to_owned(),
            "Concept 3".to_owned(),
            "Concept 4".to_owned(),
            "Concept 5".to_owned(),
            "Concept 6".to_owned(),
            "Concept 7".to_owned(),
            "Concept 8".to_owned(),
            "Concept 9".to_owned(),
            "Concept 10".to_owned(),
        ],
        json!([OBSERVATION_RECEIPT]),
    );
    assert_generation_error(
        validate_final_response(
            &serde_json::to_vec(&duplicate_lowercase).expect("fixture should serialize"),
            scope,
        ),
        "concepts",
        "duplicate",
    );
}

#[test]
fn canonical_memory_candidate_normalization_is_deterministic_and_merges_equivalent_evidence() {
    let input = GenerationInput::initial(multi_observation_transcript())
        .expect("all observations should be eligible memory evidence on initial processing");
    let generated_at = timestamp("2026-09-18T14:00:00.987Z");
    let candidates = vec![
        memory_candidate(
            "  Durable title  ",
            "  Durable content  ",
            vec![
                " ZETA ".to_owned(),
                " STRASSE ".to_owned(),
                " Straße ".to_owned(),
            ],
            vec![
                SECOND_OBSERVATION_RECEIPT.to_owned(),
                OBSERVATION_RECEIPT.to_owned(),
            ],
        ),
        memory_candidate(
            "Durable title",
            "Durable content",
            vec!["strasse".to_owned(), "straße".to_owned(), "zeta".to_owned()],
            vec![OBSERVATION_RECEIPT.to_owned()],
        ),
    ];

    let normalized = normalize_memory_candidates(&input, generated_at, &candidates)
        .expect("equivalent candidates should normalize into one canonical payload");
    let reversed = normalize_memory_candidates(
        &input,
        generated_at,
        &candidates.iter().cloned().rev().collect::<Vec<_>>(),
    )
    .expect("input ordering must not change normalized candidates");

    assert_eq!(normalized, reversed);
    assert_eq!(normalized.len(), 1);
    let candidate = &normalized[0];
    assert_eq!(
        candidate.content_fingerprint(),
        "443593e77ded911e0a8f97a11fe1aa1e1a4889acd89bae4cff6f3e03be242611"
    );
    assert_eq!(
        candidate.memory_id(),
        "bb90e645-847d-554a-876b-a228ca23f239"
    );
    assert_eq!(
        candidate
            .supporting_receipt_ids()
            .iter()
            .map(|receipt_id| receipt_id.as_str())
            .collect::<Vec<_>>(),
        [OBSERVATION_RECEIPT, SECOND_OBSERVATION_RECEIPT]
    );

    let payload = candidate.canonical_payload();
    assert_eq!(payload.version, 1);
    assert_eq!(payload.memory_type, "session-derived");
    assert_eq!(payload.title, "Durable title");
    assert_eq!(payload.content, "Durable content");
    assert_eq!(payload.concepts, ["strasse", "straße", "zeta"]);
    assert!(payload.files.is_empty());
    assert_eq!(payload.session_ids, [SESSION_ID]);
    assert_eq!(
        payload.source_observation_ids,
        [OBSERVATION_RECEIPT, SECOND_OBSERVATION_RECEIPT]
    );
    assert_eq!(payload.created_at, timestamp("2026-09-18T14:00:00Z"));
    assert_eq!(payload.updated_at, timestamp("2026-09-18T14:00:00Z"));
    ValidatedMemoryVersion::try_from(payload.clone())
        .expect("the normalized payload must satisfy the canonical memory contract");
}

#[test]
fn canonical_memory_candidate_normalization_rejects_invalid_values_and_scope_without_leaking_content()
 {
    let input = generation_input_with_observation_data(json!({
        "opaque": RAW_TRANSCRIPT_SENTINEL,
    }));
    let generated_at = timestamp("2026-09-18T14:00:00.987Z");
    let outside_scope = memory_candidate(
        NORMALIZATION_TITLE_SENTINEL,
        NORMALIZATION_CONTENT_SENTINEL,
        vec!["candidate concept".to_owned()],
        vec![OUTSIDE_RECEIPT.to_owned()],
    );

    let error = normalize_memory_candidates(&input, generated_at, &[outside_scope])
        .expect_err("candidate evidence outside the current memory scope must be rejected");
    assert_failure_projection(
        &error,
        "supporting_receipt_ids",
        "outside_memory_scope",
        FailureCategory::ProviderContract,
        Retryability::AfterBackoff,
        &[
            RAW_TRANSCRIPT_SENTINEL,
            NORMALIZATION_TITLE_SENTINEL,
            NORMALIZATION_CONTENT_SENTINEL,
            OUTSIDE_RECEIPT,
        ],
    );

    let invalid_canonical_value = memory_candidate(
        " \t ",
        NORMALIZATION_CONTENT_SENTINEL,
        vec!["candidate concept".to_owned()],
        vec![OBSERVATION_RECEIPT.to_owned()],
    );
    let error = normalize_memory_candidates(&input, generated_at, &[invalid_canonical_value])
        .expect_err("trimmed canonical fields must satisfy the memory-store contract");
    assert_failure_projection(
        &error,
        "canonical_payload",
        "invalid",
        FailureCategory::ProviderContract,
        Retryability::AfterBackoff,
        &[RAW_TRANSCRIPT_SENTINEL, NORMALIZATION_CONTENT_SENTINEL],
    );
}

#[test]
fn canonical_memory_candidate_normalization_rejects_empty_normalized_concepts_without_leaking_content()
 {
    let input = generation_input_with_observation_data(json!({
        "opaque": RAW_TRANSCRIPT_SENTINEL,
    }));
    let generated_at = timestamp("2026-09-18T14:00:00.987Z");
    let candidate = memory_candidate(
        NORMALIZATION_TITLE_SENTINEL,
        NORMALIZATION_CONTENT_SENTINEL,
        vec![
            NORMALIZATION_CONCEPT_SENTINEL.to_owned(),
            " \t\u{2003}".to_owned(),
        ],
        vec![OBSERVATION_RECEIPT.to_owned()],
    );

    let error = normalize_memory_candidates(&input, generated_at, &[candidate])
        .expect_err("whitespace-only normalized concepts must be rejected before staging");
    assert_failure_projection(
        &error,
        "concepts",
        "empty",
        FailureCategory::ProviderContract,
        Retryability::AfterBackoff,
        &[
            RAW_TRANSCRIPT_SENTINEL,
            NORMALIZATION_TITLE_SENTINEL,
            NORMALIZATION_CONTENT_SENTINEL,
            NORMALIZATION_CONCEPT_SENTINEL,
            OBSERVATION_RECEIPT,
        ],
    );
}

fn generation_input() -> GenerationInput {
    generation_input_with_observation_data(json!({"opaque": "☃".repeat(256)}))
}

fn generation_input_with_observation_data(data: Value) -> GenerationInput {
    let transcript = Transcript::try_new(vec![
        TranscriptEntryInput::Lifecycle {
            receipt_id: LIFECYCLE_RECEIPT.to_owned(),
            event_type: SourceEventType::SessionStart,
            session_id: SESSION_ID.to_owned(),
            project_name: "project".to_owned(),
            current_working_directory: "/work".to_owned(),
            source_timestamp_rfc3339: "2026-09-18T10:00:00.000Z".to_owned(),
            source_timestamp_utc: timestamp("2026-09-18T10:00:00Z"),
            ingested_at: timestamp("2026-09-18T11:00:00Z"),
        }
        .try_into()
        .expect("lifecycle fixture should be valid"),
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
            data,
        }
        .try_into()
        .expect("observation fixture should be valid"),
    ])
    .expect("transcript fixture should be valid");

    GenerationInput::try_new(
        transcript,
        MemoryScope::try_from(vec![OBSERVATION_RECEIPT.to_owned()])
            .expect("memory scope fixture should be valid"),
    )
    .expect("generation input fixture should be valid")
}

fn lifecycle_only_transcript() -> Transcript {
    Transcript::try_new(vec![
        TranscriptEntryInput::Lifecycle {
            receipt_id: LIFECYCLE_RECEIPT.to_owned(),
            event_type: SourceEventType::SessionStart,
            session_id: SESSION_ID.to_owned(),
            project_name: "project".to_owned(),
            current_working_directory: "/work".to_owned(),
            source_timestamp_rfc3339: "2026-09-18T10:00:00.000Z".to_owned(),
            source_timestamp_utc: timestamp("2026-09-18T10:00:00Z"),
            ingested_at: timestamp("2026-09-18T11:00:00Z"),
        }
        .try_into()
        .expect("lifecycle fixture should be valid"),
    ])
    .expect("lifecycle-only transcript fixture should be valid")
}

fn multi_observation_transcript() -> Transcript {
    Transcript::try_new(vec![
        TranscriptEntryInput::Lifecycle {
            receipt_id: SESSION_END_RECEIPT.to_owned(),
            event_type: SourceEventType::SessionEnd,
            session_id: SESSION_ID.to_owned(),
            project_name: "project".to_owned(),
            current_working_directory: "/work".to_owned(),
            source_timestamp_rfc3339: "2026-09-18T13:00:00.000Z".to_owned(),
            source_timestamp_utc: timestamp("2026-09-18T13:00:00Z"),
            ingested_at: timestamp("2026-09-18T14:00:00Z"),
        }
        .try_into()
        .expect("session-end fixture should be valid"),
        TranscriptEntryInput::Observation {
            receipt_id: SECOND_OBSERVATION_RECEIPT.to_owned(),
            event_type: SourceEventType::Observation,
            session_id: SESSION_ID.to_owned(),
            hook_type: "post_tool_use".to_owned(),
            project_name: "project".to_owned(),
            current_working_directory: "/work".to_owned(),
            source_timestamp_rfc3339: "2026-09-18T12:00:00.000Z".to_owned(),
            source_timestamp_utc: timestamp("2026-09-18T12:00:00Z"),
            ingested_at: timestamp("2026-09-18T13:00:00Z"),
            data: json!({"opaque": "second observation"}),
        }
        .try_into()
        .expect("second observation fixture should be valid"),
        TranscriptEntryInput::Lifecycle {
            receipt_id: LIFECYCLE_RECEIPT.to_owned(),
            event_type: SourceEventType::SessionStart,
            session_id: SESSION_ID.to_owned(),
            project_name: "project".to_owned(),
            current_working_directory: "/work".to_owned(),
            source_timestamp_rfc3339: "2026-09-18T10:00:00.000Z".to_owned(),
            source_timestamp_utc: timestamp("2026-09-18T10:00:00Z"),
            ingested_at: timestamp("2026-09-18T11:00:00Z"),
        }
        .try_into()
        .expect("lifecycle fixture should be valid"),
        TranscriptEntryInput::Observation {
            receipt_id: OBSERVATION_RECEIPT.to_owned(),
            event_type: SourceEventType::Observation,
            session_id: SESSION_ID.to_owned(),
            hook_type: "post_tool_use".to_owned(),
            project_name: "project".to_owned(),
            current_working_directory: "/work".to_owned(),
            source_timestamp_rfc3339: "2026-09-18T11:00:00.000Z".to_owned(),
            source_timestamp_utc: timestamp("2026-09-18T11:00:00Z"),
            ingested_at: timestamp("2026-09-18T12:00:00Z"),
            data: json!({"opaque": "first observation"}),
        }
        .try_into()
        .expect("first observation fixture should be valid"),
    ])
    .expect("multi-observation transcript fixture should be valid")
}

fn response_json(concepts: Vec<String>, supporting_receipt_ids: Value) -> Value {
    json!({
        "summary_sentences": ["First sentence.", "Second sentence."],
        "concepts": concepts,
        "memory_candidates": [{
            "title": "memory title",
            "content": "memory content",
            "concepts": ["candidate concept"],
            "supporting_receipt_ids": supporting_receipt_ids,
        }],
    })
}

fn memory_candidate(
    title: &str,
    content: &str,
    concepts: Vec<String>,
    supporting_receipt_ids: Vec<String>,
) -> MemoryCandidate {
    MemoryCandidate::try_from(MemoryCandidateInput {
        title: title.to_owned(),
        content: content.to_owned(),
        concepts,
        supporting_receipt_ids,
    })
    .expect("normalization fixture candidate should satisfy the generated candidate contract")
}

fn map_response_with_candidates(
    supporting_receipt_ids: Vec<String>,
) -> StructuredGenerationResponse {
    StructuredGenerationResponse::try_from(StructuredGenerationResponseInput {
        summary_sentences: vec!["Map sentence.".to_owned()],
        concepts: vec!["map concept".to_owned()],
        memory_candidates: vec![MemoryCandidateInput {
            title: "memory title".to_owned(),
            content: "memory content".to_owned(),
            concepts: vec!["candidate concept".to_owned()],
            supporting_receipt_ids,
        }],
    })
    .expect("map response fixture should be valid")
}

fn map_response_with_candidate_content(
    summary: String,
    concept: String,
    candidate_content: String,
) -> StructuredGenerationResponse {
    StructuredGenerationResponse::try_from(StructuredGenerationResponseInput {
        summary_sentences: vec![summary],
        concepts: vec![concept],
        memory_candidates: vec![MemoryCandidateInput {
            title: "memory title".to_owned(),
            content: candidate_content,
            concepts: vec!["candidate concept".to_owned()],
            supporting_receipt_ids: vec![OBSERVATION_RECEIPT.to_owned()],
        }],
    })
    .expect("map response fixture should be valid")
}

fn map_response_without_candidates(summary: &str, concept: &str) -> StructuredGenerationResponse {
    StructuredGenerationResponse::try_from(StructuredGenerationResponseInput {
        summary_sentences: vec![summary.to_owned()],
        concepts: vec![concept.to_owned()],
        memory_candidates: Vec::new(),
    })
    .expect("map response fixture should be valid")
}

fn final_reduce_response() -> StructuredGenerationResponse {
    StructuredGenerationResponse::try_from(StructuredGenerationResponseInput {
        summary_sentences: vec!["Final sentence.".to_owned()],
        concepts: (1..=10).map(|number| format!("concept {number}")).collect(),
        memory_candidates: vec![MemoryCandidateInput {
            title: "memory title".to_owned(),
            content: MAP_CANDIDATE_SENTINEL.to_owned(),
            concepts: vec!["candidate concept".to_owned()],
            supporting_receipt_ids: vec![OBSERVATION_RECEIPT.to_owned()],
        }],
    })
    .expect("reduce response fixture should satisfy the map response contract")
}

fn reduce_response_with_concepts(concept_count: usize) -> StructuredGenerationResponse {
    StructuredGenerationResponse::try_from(StructuredGenerationResponseInput {
        summary_sentences: vec!["Final sentence.".to_owned()],
        concepts: (1..=concept_count)
            .map(|number| format!("concept {number}"))
            .collect(),
        memory_candidates: Vec::new(),
    })
    .expect("reduce response fixture should satisfy the map response contract")
}

fn zero_memory_map_response() -> StructuredGenerationResponse {
    StructuredGenerationResponse::try_from(StructuredGenerationResponseInput {
        summary_sentences: vec!["Map sentence.".to_owned()],
        concepts: vec!["map concept".to_owned()],
        memory_candidates: Vec::new(),
    })
    .expect("zero-memory map response fixture should be valid")
}

fn lease_token() -> LeaseToken {
    LeaseToken::try_from("018f5d00-0000-7000-8000-000000000001".to_owned())
        .expect("lease token fixture should be valid")
}

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("timestamp fixture should be valid")
        .with_timezone(&Utc)
}

fn assert_generation_error(
    result: Result<
        session_post_processing::contracts::StructuredGenerationResponse,
        GenerationError,
    >,
    field: &str,
    code: &str,
) {
    let error = result.expect_err("invalid generated response should be rejected");
    assert_failure_projection(
        &error,
        field,
        code,
        FailureCategory::ProviderContract,
        Retryability::AfterBackoff,
        &["provider-output-secret-sentinel", OUTSIDE_RECEIPT],
    );
}

fn assert_failure_projection(
    error: &GenerationError,
    field: &str,
    code: &str,
    category: FailureCategory,
    retryability: Retryability,
    sentinels: &[&str],
) {
    assert_eq!(error.field(), field);
    assert_eq!(error.code(), code);
    assert_eq!(error.category(), category);
    assert_eq!(error.retryability(), retryability);

    let serialized = serde_json::to_value(error).expect("error should serialize");
    assert_eq!(serialized["category"], json!(category));
    assert_eq!(serialized["retryability"], json!(retryability));
    assert_error_is_safe(error, sentinels);
}

#[derive(Clone)]
struct BlockingModelProvider {
    gate: DeterministicGate,
}

impl BlockingModelProvider {
    fn new() -> Self {
        Self {
            gate: DeterministicGate::new(),
        }
    }

    async fn wait_for_call(&self) {
        self.gate.wait_for_entered(1).await;
    }
}

#[async_trait]
impl ModelProvider for BlockingModelProvider {
    async fn generate(
        &self,
        _request: StructuredGenerationRequest,
    ) -> Result<StructuredGenerationResponse, ProviderError> {
        self.gate.enter().await;
        Err(ProviderError::transient())
    }
}

fn assert_error_is_safe<E>(error: &E, sentinels: &[&str])
where
    E: Debug + Display + Serialize,
{
    let display = error.to_string();
    let debug = format!("{error:?}");
    let serialized = serde_json::to_string(error).expect("error should serialize");

    for sentinel in sentinels {
        assert!(!display.contains(sentinel));
        assert!(!debug.contains(sentinel));
        assert!(!serialized.contains(sentinel));
    }
}
