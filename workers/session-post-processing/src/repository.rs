use std::{collections::BTreeSet, future::Future, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use iii_sdk::{IIIClient, protocol::TriggerRequest};
use memory_store::contracts::{DatabaseTarget, MemoryVersionInput};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    contracts::{
        AttemptState, Claim, ClaimInput, FailureCategory, LoadedSnapshot, MemoryScope, ReceiptId,
        Retryability, SessionId, SnapshotLoadOutcome, SourceEventType, SourceRevision,
        SourceSnapshot, SourceSnapshotInput, TranscriptEntry, TranscriptEntryInput,
    },
    ports::{RepositoryError, SessionRepository, StagedMemoryCandidate, StagedRevision},
};

const DATABASE_EXECUTE_FUNCTION_ID: &str = "database::execute";
const SOURCE_REVISION_NAMESPACE: &[u8] = b"total-recall/session-post-processing/source-revision/v1";
const DISCOVERY_SQL: &str = r#"WITH lifecycle AS (
    SELECT
        session_id,
        COUNT(*) AS lifecycle_count,
        BOOL_OR(event_type = 'session_end') AS has_persisted_end
    FROM session_events
    GROUP BY session_id
),
observations AS (
    SELECT
        session_id,
        COUNT(*) AS observation_count,
        MAX(ingested_at) AS latest_observation_ingested_at
    FROM raw_observations
    GROUP BY session_id
),
source AS (
    SELECT
        COALESCE(lifecycle.session_id, observations.session_id) AS session_id,
        COALESCE(lifecycle.lifecycle_count, 0) AS lifecycle_count,
        COALESCE(observations.observation_count, 0) AS observation_count,
        COALESCE(lifecycle.has_persisted_end, FALSE) AS has_persisted_end,
        observations.latest_observation_ingested_at
    FROM lifecycle
    FULL OUTER JOIN observations
        ON lifecycle.session_id = observations.session_id
)
SELECT source.session_id
FROM source
LEFT JOIN public.session_records AS current_record
    ON current_record.session_id = source.session_id
WHERE (source.lifecycle_count > 0 OR source.observation_count > 0)
  AND (
      source.has_persisted_end
      OR source.latest_observation_ingested_at <= $1::text::timestamptz - INTERVAL '24 hours'
  )
  AND (
        current_record.session_id IS NULL
      OR current_record.lifecycle_count IS DISTINCT FROM source.lifecycle_count
      OR current_record.observation_count IS DISTINCT FROM source.observation_count
  )
ORDER BY source.session_id ASC"#;
const CLAIM_CANDIDATES_SQL: &str = r#"WITH lifecycle AS (
    SELECT
        session_id,
        COUNT(*) AS lifecycle_count,
        BOOL_OR(event_type = 'session_end') AS has_persisted_end
    FROM session_events
    GROUP BY session_id
),
observations AS (
    SELECT
        session_id,
        COUNT(*) AS observation_count,
        MAX(ingested_at) AS latest_observation_ingested_at
    FROM raw_observations
    GROUP BY session_id
),
source AS (
    SELECT
        COALESCE(lifecycle.session_id, observations.session_id) AS session_id,
        COALESCE(lifecycle.lifecycle_count, 0) AS lifecycle_count,
        COALESCE(observations.observation_count, 0) AS observation_count,
        COALESCE(lifecycle.has_persisted_end, FALSE) AS has_persisted_end,
        observations.latest_observation_ingested_at
    FROM lifecycle
    FULL OUTER JOIN observations
        ON lifecycle.session_id = observations.session_id
),
eligible AS MATERIALIZED (
    SELECT
        source.session_id,
        source.lifecycle_count,
        source.observation_count
    FROM source
    LEFT JOIN public.session_records AS current_record
        ON current_record.session_id = source.session_id
    WHERE (source.lifecycle_count > 0 OR source.observation_count > 0)
      AND (
          source.has_persisted_end
          OR source.latest_observation_ingested_at <= $1::text::timestamptz - INTERVAL '24 hours'
      )
      AND (
          current_record.session_id IS NULL
          OR current_record.lifecycle_count IS DISTINCT FROM source.lifecycle_count
          OR current_record.observation_count IS DISTINCT FROM source.observation_count
      )
    ORDER BY source.session_id COLLATE "C" ASC
    LIMIT $2::text::bigint
),
identities AS (
    SELECT event.session_id, event.event_type, event.receipt_id
    FROM session_events AS event
    JOIN eligible ON eligible.session_id = event.session_id
    UNION ALL
    SELECT observation.session_id, observation.event_type, observation.receipt_id
    FROM raw_observations AS observation
    JOIN eligible ON eligible.session_id = observation.session_id
)
SELECT
    eligible.session_id,
    eligible.lifecycle_count::text AS lifecycle_count,
    eligible.observation_count::text AS observation_count,
    jsonb_agg(
        jsonb_build_object('event_type', identities.event_type, 'receipt_id', identities.receipt_id::text)
        ORDER BY identities.event_type COLLATE "C", identities.receipt_id::text COLLATE "C"
    ) AS source_identities
FROM eligible
JOIN identities ON identities.session_id = eligible.session_id
GROUP BY eligible.session_id, eligible.lifecycle_count, eligible.observation_count
ORDER BY eligible.session_id COLLATE "C" ASC"#;
const CLAIM_SQL: &str = r#"WITH request AS (
    SELECT
        $1::text AS session_id,
        $2::text::uuid AS source_revision,
        $3::text::bigint AS lifecycle_count,
        $4::text::bigint AS observation_count,
        $5::text::uuid AS attempt_id,
        $6::text::uuid AS lease_token,
        $7::text::timestamptz AS now,
        $8::text::timestamptz AS lease_expires_at
),
reclaimed_retryable AS (
    UPDATE public.session_processing_attempts AS attempt
    SET state = 'claimed',
        lease_token = request.lease_token,
        lease_expires_at = request.lease_expires_at,
        next_attempt_at = NULL
    FROM request
    WHERE attempt.session_id = request.session_id
      AND attempt.state = 'retryable'
      AND attempt.next_attempt_at <= request.now
    RETURNING
        attempt.attempt_id,
        attempt.session_id,
        attempt.source_revision,
        attempt.lifecycle_count,
        attempt.observation_count,
        attempt.state,
        attempt.lease_token,
        attempt.lease_expires_at
),
reclaimed_expired AS (
    UPDATE public.session_processing_attempts AS attempt
    SET lease_token = request.lease_token,
        lease_expires_at = request.lease_expires_at
    FROM request
    WHERE attempt.session_id = request.session_id
      AND attempt.state IN ('claimed', 'staged', 'publishing')
      AND attempt.lease_expires_at <= request.now
    RETURNING
        attempt.attempt_id,
        attempt.session_id,
        attempt.source_revision,
        attempt.lifecycle_count,
        attempt.observation_count,
        attempt.state,
        attempt.lease_token,
        attempt.lease_expires_at
),
reclaimed AS (
    SELECT * FROM reclaimed_retryable
    UNION ALL
    SELECT * FROM reclaimed_expired
),
inserted AS (
    INSERT INTO public.session_processing_attempts (
        attempt_id,
        session_id,
        source_revision,
        lifecycle_count,
        observation_count,
        state,
        lease_token,
        lease_expires_at,
        next_attempt_at
    )
    SELECT
        request.attempt_id,
        request.session_id,
        request.source_revision,
        request.lifecycle_count,
        request.observation_count,
        'claimed',
        request.lease_token,
        request.lease_expires_at,
        NULL
    FROM request
    WHERE NOT EXISTS (SELECT 1 FROM reclaimed)
    ON CONFLICT DO NOTHING
    RETURNING
        attempt_id,
        session_id,
        source_revision,
        lifecycle_count,
        observation_count,
        state,
        lease_token,
        lease_expires_at
),
claimed AS (
    SELECT * FROM reclaimed
    UNION ALL
    SELECT * FROM inserted
)
SELECT
    attempt_id::text AS attempt_id,
    session_id,
    source_revision::text AS source_revision,
    lifecycle_count::text AS lifecycle_count,
    observation_count::text AS observation_count,
    state,
    lease_token::text AS lease_token,
    lease_expires_at
FROM claimed"#;
const RENEW_SQL: &str = r#"WITH request AS (
    SELECT
        $1::text::uuid AS attempt_id,
        $2::text::uuid AS lease_token,
        $3::text::timestamptz AS now,
        $4::text::timestamptz AS lease_expires_at
)
UPDATE public.session_processing_attempts AS attempt
SET lease_expires_at = request.lease_expires_at
FROM request
WHERE attempt.attempt_id = request.attempt_id
  AND attempt.lease_token = request.lease_token
  AND attempt.state IN ('claimed', 'staged', 'publishing')
  AND attempt.lease_expires_at > request.now
RETURNING TRUE AS renewed"#;
const SOURCE_LOAD_SQL: &str = r#"WITH request AS (
    SELECT
        $1::text::uuid AS attempt_id,
        $2::text::uuid AS lease_token,
        $3::text AS session_id,
        $4::text::uuid AS source_revision
),
fenced_attempt AS (
    SELECT attempt.session_id
    FROM public.session_processing_attempts AS attempt
    JOIN request
        ON attempt.attempt_id = request.attempt_id
    WHERE attempt.lease_token = request.lease_token
      AND attempt.session_id = request.session_id
      AND attempt.source_revision = request.source_revision
      AND attempt.state IN ('claimed', 'staged', 'publishing')
),
source_entries AS (
    SELECT
        jsonb_build_object(
            'kind', 'lifecycle',
            'receipt_id', lifecycle.receipt_id::text,
            'event_type', lifecycle.event_type,
            'session_id', lifecycle.session_id,
            'project_name', lifecycle.project_name,
            'current_working_directory', lifecycle.current_working_directory,
            'source_timestamp_rfc3339', lifecycle.source_timestamp_rfc3339,
            'source_timestamp_utc', lifecycle.source_timestamp_utc,
            'ingested_at', lifecycle.ingested_at
        ) AS entry,
        current_record.session_id IS NOT NULL AS prior_projection_present,
        current_record.transcript AS prior_transcript
    FROM fenced_attempt
    JOIN session_events AS lifecycle
        ON lifecycle.session_id = fenced_attempt.session_id
    LEFT JOIN public.session_records AS current_record
        ON current_record.session_id = fenced_attempt.session_id
    UNION ALL
    SELECT
        jsonb_build_object(
            'kind', 'observation',
            'receipt_id', observation.receipt_id::text,
            'event_type', observation.event_type,
            'session_id', observation.session_id,
            'hook_type', observation.hook_type,
            'project_name', observation.project_name,
            'current_working_directory', observation.current_working_directory,
            'source_timestamp_rfc3339', observation.source_timestamp_rfc3339,
            'source_timestamp_utc', observation.source_timestamp_utc,
            'ingested_at', observation.ingested_at,
            'data', observation.data
        ) AS entry,
        current_record.session_id IS NOT NULL AS prior_projection_present,
        current_record.transcript AS prior_transcript
    FROM fenced_attempt
    JOIN raw_observations AS observation
        ON observation.session_id = fenced_attempt.session_id
    LEFT JOIN public.session_records AS current_record
        ON current_record.session_id = fenced_attempt.session_id
)
SELECT entry, prior_projection_present, prior_transcript
FROM source_entries"#;
const SUPERSEDE_SQL: &str = r#"WITH request AS (
    SELECT
        $1::text::uuid AS attempt_id,
        $2::text::uuid AS lease_token
)
UPDATE public.session_processing_attempts AS attempt
SET state = 'superseded'
FROM request
WHERE attempt.attempt_id = request.attempt_id
  AND attempt.lease_token = request.lease_token
  AND attempt.state = 'claimed'
RETURNING TRUE AS superseded"#;
const STAGE_SQL: &str = r#"WITH request AS (
    SELECT
        $1::text::uuid AS attempt_id,
        $2::text::uuid AS lease_token,
        $3::text AS session_id,
        $4::text::uuid AS source_revision,
        $5::text::timestamptz AS source_cutoff,
        $6::jsonb AS transcript,
        $7::jsonb AS summary_sentences,
        $8::text AS summary,
        $9::jsonb AS concepts,
        $10::text::timestamptz AS generated_at,
        $11::jsonb AS candidates
),
staged AS (
    UPDATE public.session_processing_attempts AS attempt
    SET state = 'staged',
        source_cutoff = request.source_cutoff,
        transcript = request.transcript,
        summary_sentences = ARRAY(
            SELECT sentence.value
            FROM jsonb_array_elements_text(request.summary_sentences) WITH ORDINALITY AS sentence(value, ordinality)
            ORDER BY sentence.ordinality
        ),
        summary = request.summary,
        concepts = ARRAY(
            SELECT concept.value
            FROM jsonb_array_elements_text(request.concepts) WITH ORDINALITY AS concept(value, ordinality)
            ORDER BY concept.ordinality
        ),
        generated_at = request.generated_at
    FROM request
    WHERE attempt.attempt_id = request.attempt_id
      AND attempt.lease_token = request.lease_token
      AND attempt.session_id = request.session_id
      AND attempt.source_revision = request.source_revision
      AND attempt.state = 'claimed'
    RETURNING attempt.attempt_id, attempt.session_id, attempt.source_revision, attempt.state
),
inserted_candidates AS (
    INSERT INTO public.session_memory_candidates (
        attempt_id,
        ordinal,
        session_id,
        content_fingerprint,
        memory_id,
        canonical_payload,
        supporting_receipt_ids
    )
    SELECT
        staged.attempt_id,
        (candidate.ordinality - 1)::integer,
        staged.session_id,
        candidate.value ->> 'content_fingerprint',
        (candidate.value -> 'canonical_payload' ->> 'id')::uuid,
        candidate.value -> 'canonical_payload',
        ARRAY(
            SELECT receipt_id.value::uuid
            FROM jsonb_array_elements_text(candidate.value -> 'supporting_receipt_ids') WITH ORDINALITY AS receipt_id(value, ordinality)
            ORDER BY receipt_id.ordinality
        )
    FROM staged
    CROSS JOIN request
    CROSS JOIN LATERAL jsonb_array_elements(request.candidates) WITH ORDINALITY AS candidate(value, ordinality)
    ORDER BY candidate.ordinality
    ON CONFLICT (session_id, content_fingerprint) DO NOTHING
)
SELECT
    staged.attempt_id::text AS attempt_id,
    staged.session_id,
    staged.source_revision::text AS source_revision,
    staged.state
FROM staged"#;
const MARK_MEMORY_PUBLISHED_SQL: &str = r#"WITH request AS (
    SELECT
        $1::text::uuid AS attempt_id,
        $2::text::uuid AS lease_token,
        $3::text AS session_id,
        $4::text::uuid AS source_revision,
        $5::text::uuid AS memory_id
),
target AS (
    SELECT candidate.attempt_id, candidate.memory_id
    FROM public.session_memory_candidates AS candidate
    JOIN public.session_processing_attempts AS attempt
        ON attempt.attempt_id = candidate.attempt_id
    CROSS JOIN request
    WHERE candidate.attempt_id = request.attempt_id
      AND candidate.memory_id = request.memory_id
      AND attempt.attempt_id = request.attempt_id
      AND attempt.lease_token = request.lease_token
      AND attempt.session_id = request.session_id
      AND attempt.source_revision = request.source_revision
      AND attempt.state IN ('staged', 'publishing')
),
publishing AS (
    UPDATE public.session_processing_attempts AS attempt
    SET state = 'publishing'
    FROM request
    WHERE attempt.attempt_id = request.attempt_id
      AND attempt.lease_token = request.lease_token
      AND attempt.session_id = request.session_id
      AND attempt.source_revision = request.source_revision
      AND attempt.state IN ('staged', 'publishing')
      AND EXISTS (SELECT 1 FROM target)
    RETURNING attempt.attempt_id, attempt.state
),
published AS (
    UPDATE public.session_memory_candidates AS candidate
    SET published_at = COALESCE(candidate.published_at, CURRENT_TIMESTAMP)
    FROM target
    JOIN publishing ON publishing.attempt_id = target.attempt_id
    WHERE candidate.attempt_id = target.attempt_id
      AND candidate.memory_id = target.memory_id
    RETURNING candidate.attempt_id, candidate.memory_id, candidate.published_at
)
SELECT
    published.attempt_id::text AS attempt_id,
    published.memory_id::text AS memory_id,
    published.published_at,
    publishing.state
FROM published
JOIN publishing ON publishing.attempt_id = published.attempt_id"#;
const LOAD_UNPUBLISHED_CANDIDATES_SQL: &str = r#"WITH request AS (
    SELECT
        $1::text::uuid AS attempt_id,
        $2::text::uuid AS lease_token,
        $3::text AS session_id,
        $4::text::uuid AS source_revision
),
publishing AS (
    UPDATE public.session_processing_attempts AS attempt
    SET state = 'publishing'
    FROM request
    WHERE attempt.attempt_id = request.attempt_id
      AND attempt.lease_token = request.lease_token
      AND attempt.session_id = request.session_id
      AND attempt.source_revision = request.source_revision
      AND attempt.state IN ('staged', 'publishing')
    RETURNING attempt.attempt_id, attempt.session_id, attempt.source_revision, attempt.state
)
SELECT
    publishing.attempt_id::text AS attempt_id,
    publishing.session_id,
    publishing.source_revision::text AS source_revision,
    publishing.state,
    COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'ordinal', candidate.ordinal,
                'content_fingerprint', candidate.content_fingerprint,
                'memory_id', candidate.memory_id::text,
                'canonical_payload', candidate.canonical_payload,
                'supporting_receipt_ids', to_jsonb(candidate.supporting_receipt_ids)
            )
            ORDER BY candidate.ordinal
        ) FILTER (WHERE candidate.attempt_id IS NOT NULL),
        '[]'::jsonb
    ) AS candidates
FROM publishing
LEFT JOIN public.session_memory_candidates AS candidate
    ON candidate.attempt_id = publishing.attempt_id
   AND candidate.published_at IS NULL
GROUP BY publishing.attempt_id, publishing.session_id, publishing.source_revision, publishing.state"#;
const RETRY_SQL: &str = r#"WITH request AS (
    SELECT
        $1::text::uuid AS attempt_id,
        $2::text::uuid AS lease_token,
        $3::text AS session_id,
        $4::text::uuid AS source_revision,
        $5::text AS failure_category,
        $6::text::timestamptz AS next_attempt_at
),
retryable AS (
    UPDATE public.session_processing_attempts AS attempt
    SET state = 'retryable',
        failure_category = request.failure_category,
        next_attempt_at = request.next_attempt_at
    FROM request
    WHERE attempt.attempt_id = request.attempt_id
      AND attempt.lease_token = request.lease_token
      AND attempt.session_id = request.session_id
      AND attempt.source_revision = request.source_revision
      AND attempt.state IN ('claimed', 'staged', 'publishing')
    RETURNING
        attempt.attempt_id,
        attempt.session_id,
        attempt.source_revision,
        attempt.state,
        attempt.failure_category,
        attempt.next_attempt_at
)
SELECT
    attempt_id::text AS attempt_id,
    session_id,
    source_revision::text AS source_revision,
    state,
    failure_category,
    next_attempt_at
FROM retryable"#;
const PROMOTE_SQL: &str = r#"WITH request AS (
    SELECT
        $1::text::uuid AS attempt_id,
        $2::text::uuid AS lease_token,
        $3::text AS session_id,
        $4::text::uuid AS source_revision
),
ready AS MATERIALIZED (
    SELECT
        attempt.attempt_id,
        attempt.lease_token,
        attempt.session_id,
        attempt.source_revision,
        attempt.lifecycle_count,
        attempt.observation_count,
        attempt.source_cutoff,
        attempt.transcript,
        attempt.summary_sentences,
        attempt.summary,
        attempt.concepts,
        attempt.generated_at
    FROM public.session_processing_attempts AS attempt
    JOIN request
        ON attempt.attempt_id = request.attempt_id
       AND attempt.lease_token = request.lease_token
       AND attempt.session_id = request.session_id
       AND attempt.source_revision = request.source_revision
    WHERE attempt.state = 'publishing'
      AND NOT EXISTS (
          SELECT 1
          FROM public.session_memory_candidates AS candidate
          WHERE candidate.attempt_id = attempt.attempt_id
            AND candidate.published_at IS NULL
      )
      AND NOT EXISTS (
          SELECT 1
          FROM public.session_processing_attempts AS other
          WHERE other.session_id = attempt.session_id
            AND other.attempt_id <> attempt.attempt_id
            AND other.state IN ('claimed', 'staged', 'publishing', 'retryable')
      )
    FOR UPDATE OF attempt
),
promoted AS (
    INSERT INTO public.session_records AS existing_record (
        session_id,
        source_revision,
        lifecycle_count,
        observation_count,
        source_cutoff,
        transcript,
        summary_sentences,
        summary,
        concepts,
        generated_at
    )
    SELECT
        session_id,
        source_revision,
        lifecycle_count,
        observation_count,
        source_cutoff,
        transcript,
        summary_sentences,
        summary,
        concepts,
        generated_at
    FROM ready
    ON CONFLICT (session_id) DO UPDATE
    SET source_revision = EXCLUDED.source_revision,
        lifecycle_count = EXCLUDED.lifecycle_count,
        observation_count = EXCLUDED.observation_count,
        source_cutoff = EXCLUDED.source_cutoff,
        transcript = EXCLUDED.transcript,
        summary_sentences = EXCLUDED.summary_sentences,
        summary = EXCLUDED.summary,
        concepts = EXCLUDED.concepts,
        generated_at = EXCLUDED.generated_at
    RETURNING session_id, source_revision
),
completed AS (
    UPDATE public.session_processing_attempts AS attempt
    SET state = 'complete'
    FROM ready
    JOIN promoted
        ON promoted.session_id = ready.session_id
       AND promoted.source_revision = ready.source_revision
    WHERE attempt.attempt_id = ready.attempt_id
      AND attempt.lease_token = ready.lease_token
      AND attempt.session_id = ready.session_id
      AND attempt.source_revision = ready.source_revision
      AND attempt.state = 'publishing'
    RETURNING
        attempt.attempt_id,
        attempt.session_id,
        attempt.source_revision,
        attempt.state
)
SELECT
    completed.attempt_id::text AS attempt_id,
    promoted.session_id,
    promoted.source_revision::text AS source_revision,
    completed.state
FROM completed
JOIN promoted
    ON promoted.session_id = completed.session_id
   AND promoted.source_revision = completed.source_revision"#;

pub struct IiiSessionRepository {
    client: IIIClient,
    database: DatabaseTarget,
}

impl IiiSessionRepository {
    pub fn new(client: IIIClient, database: DatabaseTarget) -> Self {
        Self { client, database }
    }

    pub async fn claim_eligible(
        &self,
        now: DateTime<Utc>,
        limit: u32,
        lease: Duration,
    ) -> Result<Vec<Claim>, RepositoryError> {
        claim_eligible_with(&self.database, now, limit, lease, |request| {
            self.client.trigger(request)
        })
        .await
    }

    pub async fn renew(
        &self,
        claim: &Claim,
        now: DateTime<Utc>,
        lease: Duration,
    ) -> Result<bool, RepositoryError> {
        renew_with(&self.database, claim, now, lease, |request| {
            self.client.trigger(request)
        })
        .await
    }

    pub async fn load_snapshot(
        &self,
        claim: &Claim,
    ) -> Result<SnapshotLoadOutcome, RepositoryError> {
        load_snapshot_with(&self.database, claim, |request| {
            self.client.trigger(request)
        })
        .await
    }

    pub async fn stage(
        &self,
        claim: &Claim,
        output: StagedRevision,
    ) -> Result<(), RepositoryError> {
        stage_with(&self.database, claim, output, |request| {
            self.client.trigger(request)
        })
        .await
    }

    pub async fn mark_memory_published(
        &self,
        claim: &Claim,
        memory_id: &str,
    ) -> Result<(), RepositoryError> {
        mark_memory_published_with(&self.database, claim, memory_id, |request| {
            self.client.trigger(request)
        })
        .await
    }

    pub async fn load_unpublished_candidates(
        &self,
        claim: &Claim,
    ) -> Result<Vec<StagedMemoryCandidate>, RepositoryError> {
        load_unpublished_candidates_with(&self.database, claim, |request| {
            self.client.trigger(request)
        })
        .await
    }

    pub async fn promote(&self, claim: &Claim) -> Result<(), RepositoryError> {
        promote_with(&self.database, claim, |request| {
            self.client.trigger(request)
        })
        .await
    }

    pub async fn retry(
        &self,
        claim: &Claim,
        failure: FailureCategory,
        next_at: DateTime<Utc>,
    ) -> Result<(), RepositoryError> {
        retry_with(&self.database, claim, failure, next_at, |request| {
            self.client.trigger(request)
        })
        .await
    }
}

#[async_trait]
impl SessionRepository for IiiSessionRepository {
    async fn claim_eligible(
        &self,
        now: DateTime<Utc>,
        limit: u32,
        lease: Duration,
    ) -> Result<Vec<Claim>, RepositoryError> {
        IiiSessionRepository::claim_eligible(self, now, limit, lease).await
    }

    async fn renew(
        &self,
        claim: &Claim,
        now: DateTime<Utc>,
        lease: Duration,
    ) -> Result<bool, RepositoryError> {
        IiiSessionRepository::renew(self, claim, now, lease).await
    }

    async fn load_snapshot(&self, claim: &Claim) -> Result<SnapshotLoadOutcome, RepositoryError> {
        IiiSessionRepository::load_snapshot(self, claim).await
    }

    async fn stage(&self, claim: &Claim, output: StagedRevision) -> Result<(), RepositoryError> {
        IiiSessionRepository::stage(self, claim, output).await
    }

    async fn load_unpublished_candidates(
        &self,
        claim: &Claim,
    ) -> Result<Vec<StagedMemoryCandidate>, RepositoryError> {
        IiiSessionRepository::load_unpublished_candidates(self, claim).await
    }

    async fn mark_memory_published(
        &self,
        claim: &Claim,
        memory_id: &str,
    ) -> Result<(), RepositoryError> {
        IiiSessionRepository::mark_memory_published(self, claim, memory_id).await
    }

    async fn promote(&self, claim: &Claim) -> Result<(), RepositoryError> {
        IiiSessionRepository::promote(self, claim).await
    }

    async fn retry(
        &self,
        claim: &Claim,
        failure: FailureCategory,
        next_at: DateTime<Utc>,
    ) -> Result<(), RepositoryError> {
        IiiSessionRepository::retry(self, claim, failure, next_at).await
    }
}

pub async fn discover_eligible_with<F, Fut>(
    database: &DatabaseTarget,
    now: DateTime<Utc>,
    invoke: F,
) -> Result<Vec<SessionId>, RepositoryError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    let response = invoke(build_discovery_request(database, now))
        .await
        .map_err(|_| RepositoryError::invocation())?;

    decode_discovery_response(response)
}

pub async fn claim_eligible_with<F, Fut>(
    database: &DatabaseTarget,
    now: DateTime<Utc>,
    limit: u32,
    lease: Duration,
    invoke: F,
) -> Result<Vec<Claim>, RepositoryError>
where
    F: Fn(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    if limit == 0 {
        return Err(RepositoryError::invalid_response());
    }
    let lease_times = lease_times(now, lease)?;
    let response = invoke(build_claim_candidates_request(
        database,
        lease_times.issued_at,
        limit,
    ))
    .await
    .map_err(|_| RepositoryError::invocation())?;
    let candidates = decode_claim_candidates(response, limit)?;
    let mut claims = Vec::with_capacity(candidates.len());

    for candidate in candidates {
        let source_revision = source_revision(&candidate.source_identities)?;
        let attempt_id = Uuid::now_v7().to_string();
        let lease_token = Uuid::now_v7().to_string();
        let response = invoke(build_claim_request(
            database,
            &candidate,
            &source_revision,
            &attempt_id,
            &lease_token,
            &lease_times,
        ))
        .await
        .map_err(|_| RepositoryError::invocation())?;

        let Some(claim) = decode_claim_response(response)? else {
            continue;
        };
        if claim.session_id() != &candidate.session_id
            || claim.lease_token().as_str() != lease_token
            || claim.lease_expires_at() != &lease_times.expires_at
        {
            return Err(RepositoryError::invalid_response());
        }
        claims.push(claim);
    }

    Ok(claims)
}

pub async fn renew_with<F, Fut>(
    database: &DatabaseTarget,
    claim: &Claim,
    now: DateTime<Utc>,
    lease: Duration,
    invoke: F,
) -> Result<bool, RepositoryError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    let lease_times = lease_times(now, lease)?;
    let response = invoke(build_renew_request(database, claim, &lease_times))
        .await
        .map_err(|_| RepositoryError::invocation())?;

    decode_renew_response(response)
}

pub async fn load_snapshot_with<F, Fut>(
    database: &DatabaseTarget,
    claim: &Claim,
    invoke: F,
) -> Result<SnapshotLoadOutcome, RepositoryError>
where
    F: Fn(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    let response = invoke(build_source_load_request(database, claim))
        .await
        .map_err(|_| RepositoryError::invocation())?;
    let Some(loaded) = decode_loaded_source(response, claim)? else {
        return Err(RepositoryError::unconfirmed_commit());
    };
    if loaded.matches_claim(claim) {
        return loaded
            .into_loaded_snapshot()
            .map(SnapshotLoadOutcome::Current);
    }

    match claim.state() {
        AttemptState::Claimed => {
            let response = invoke(build_supersede_request(database, claim))
                .await
                .map_err(|_| RepositoryError::invocation())?;
            decode_supersede_response(response)?;
            Ok(SnapshotLoadOutcome::Superseded)
        }
        AttemptState::Staged | AttemptState::Publishing => {
            Err(RepositoryError::unconfirmed_commit())
        }
        AttemptState::Complete | AttemptState::Superseded | AttemptState::Retryable => {
            Err(RepositoryError::invalid_response())
        }
    }
}

pub async fn stage_with<F, Fut>(
    database: &DatabaseTarget,
    claim: &Claim,
    output: StagedRevision,
    invoke: F,
) -> Result<(), RepositoryError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    if claim.state() != AttemptState::Claimed || !output.matches_claim(claim) {
        return Err(RepositoryError::invalid_response());
    }
    let request = build_stage_request(database, claim, &output)?;
    let response = invoke(request)
        .await
        .map_err(|_| RepositoryError::invocation())?;

    decode_stage_response(response, claim)
}

pub async fn mark_memory_published_with<F, Fut>(
    database: &DatabaseTarget,
    claim: &Claim,
    memory_id: &str,
    invoke: F,
) -> Result<(), RepositoryError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    if !matches!(
        claim.state(),
        AttemptState::Staged | AttemptState::Publishing
    ) || !is_uuid_v5(memory_id)
    {
        return Err(RepositoryError::invalid_response());
    }
    let response = invoke(build_mark_memory_published_request(
        database, claim, memory_id,
    ))
    .await
    .map_err(|_| RepositoryError::invocation())?;

    decode_mark_memory_published_response(response, claim, memory_id)
}

pub async fn load_unpublished_candidates_with<F, Fut>(
    database: &DatabaseTarget,
    claim: &Claim,
    invoke: F,
) -> Result<Vec<StagedMemoryCandidate>, RepositoryError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    if !matches!(
        claim.state(),
        AttemptState::Staged | AttemptState::Publishing
    ) {
        return Err(RepositoryError::invalid_response());
    }
    let response = invoke(build_load_unpublished_candidates_request(database, claim))
        .await
        .map_err(|_| RepositoryError::invocation())?;

    decode_unpublished_candidates_response(response, claim)
}

pub async fn retry_with<F, Fut>(
    database: &DatabaseTarget,
    claim: &Claim,
    failure: FailureCategory,
    next_at: DateTime<Utc>,
    invoke: F,
) -> Result<(), RepositoryError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    if !claim.state().is_active() || failure.retryability() == Retryability::Never {
        return Err(RepositoryError::invalid_response());
    }
    let next_at = floor_to_whole_second(next_at)?;
    let response = invoke(build_retry_request(database, claim, failure, &next_at))
        .await
        .map_err(|_| RepositoryError::invocation())?;

    decode_retry_response(response, claim, failure, &next_at)
}

pub async fn promote_with<F, Fut>(
    database: &DatabaseTarget,
    claim: &Claim,
    invoke: F,
) -> Result<(), RepositoryError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    if !matches!(
        claim.state(),
        AttemptState::Staged | AttemptState::Publishing
    ) {
        return Err(RepositoryError::invalid_response());
    }
    let response = invoke(build_promote_request(database, claim))
        .await
        .map_err(|_| RepositoryError::invocation())?;

    decode_promote_response(response, claim)
}

fn build_discovery_request(database: &DatabaseTarget, now: DateTime<Utc>) -> TriggerRequest {
    TriggerRequest {
        function_id: DATABASE_EXECUTE_FUNCTION_ID.to_owned(),
        payload: json!({
            "db": database.as_str(),
            "sql": DISCOVERY_SQL,
            "params": [now.to_rfc3339()],
        }),
        action: None,
        timeout_ms: None,
    }
}

#[derive(Clone)]
struct SourceIdentity {
    event_type: String,
    receipt_id: ReceiptId,
}

struct ClaimCandidate {
    session_id: SessionId,
    lifecycle_count: u64,
    observation_count: u64,
    source_identities: Vec<SourceIdentity>,
}

struct LeaseTimes {
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

struct LoadedSource {
    session_id: SessionId,
    entries: Vec<TranscriptEntryInput>,
    lifecycle_count: u64,
    observation_count: u64,
    source_revision: SourceRevision,
    source_cutoff: Option<DateTime<Utc>>,
    prior_projection: PriorProjection,
}

#[derive(PartialEq)]
enum PriorProjection {
    Absent,
    Present(Value),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredTranscript {
    entries: Vec<TranscriptEntryInput>,
}

impl LoadedSource {
    fn matches_claim(&self, claim: &Claim) -> bool {
        self.session_id == *claim.session_id()
            && self.lifecycle_count == claim.lifecycle_count()
            && self.observation_count == claim.observation_count()
            && self.source_revision == *claim.source_revision()
    }

    fn into_loaded_snapshot(self) -> Result<LoadedSnapshot, RepositoryError> {
        let LoadedSource {
            session_id,
            entries,
            source_revision,
            source_cutoff,
            prior_projection,
            ..
        } = self;
        let source_cutoff = source_cutoff.ok_or_else(RepositoryError::invalid_response)?;
        let snapshot = SourceSnapshot::try_from(SourceSnapshotInput {
            session_id: session_id.as_str().to_owned(),
            source_revision: source_revision.as_str().to_owned(),
            source_cutoff,
            entries,
        })
        .map_err(|_| RepositoryError::invalid_response())?;
        let prior_observation_receipt_ids = match prior_projection {
            PriorProjection::Absent => BTreeSet::new(),
            PriorProjection::Present(transcript) => {
                prior_observation_receipt_ids(transcript, snapshot.session_id())?
            }
        };
        let mut receipt_ids = snapshot
            .transcript()
            .entries()
            .iter()
            .filter(|entry| entry.is_observation())
            .map(|entry| entry.receipt_id().clone())
            .collect::<BTreeSet<_>>();
        receipt_ids.retain(|receipt_id| !prior_observation_receipt_ids.contains(receipt_id));
        let memory_scope = MemoryScope::try_from(
            receipt_ids
                .into_iter()
                .map(|receipt_id| receipt_id.as_str().to_owned())
                .collect::<Vec<_>>(),
        )
        .map_err(|_| RepositoryError::invalid_response())?;

        LoadedSnapshot::try_new(snapshot, memory_scope)
            .map_err(|_| RepositoryError::invalid_response())
    }
}

fn build_claim_candidates_request(
    database: &DatabaseTarget,
    now: DateTime<Utc>,
    limit: u32,
) -> TriggerRequest {
    build_execute_request(
        database,
        CLAIM_CANDIDATES_SQL,
        json!([whole_second_text(&now), limit.to_string()]),
    )
}

fn build_claim_request(
    database: &DatabaseTarget,
    candidate: &ClaimCandidate,
    source_revision: &SourceRevision,
    attempt_id: &str,
    lease_token: &str,
    lease_times: &LeaseTimes,
) -> TriggerRequest {
    build_execute_request(
        database,
        CLAIM_SQL,
        json!([
            candidate.session_id.as_str(),
            source_revision.as_str(),
            candidate.lifecycle_count.to_string(),
            candidate.observation_count.to_string(),
            attempt_id,
            lease_token,
            whole_second_text(&lease_times.issued_at),
            whole_second_text(&lease_times.expires_at),
        ]),
    )
}

fn build_renew_request(
    database: &DatabaseTarget,
    claim: &Claim,
    lease_times: &LeaseTimes,
) -> TriggerRequest {
    build_execute_request(
        database,
        RENEW_SQL,
        json!([
            claim.attempt_id().as_str(),
            claim.lease_token().as_str(),
            whole_second_text(&lease_times.issued_at),
            whole_second_text(&lease_times.expires_at),
        ]),
    )
}

fn build_source_load_request(database: &DatabaseTarget, claim: &Claim) -> TriggerRequest {
    build_execute_request(
        database,
        SOURCE_LOAD_SQL,
        json!([
            claim.attempt_id().as_str(),
            claim.lease_token().as_str(),
            claim.session_id().as_str(),
            claim.source_revision().as_str(),
        ]),
    )
}

fn build_supersede_request(database: &DatabaseTarget, claim: &Claim) -> TriggerRequest {
    build_execute_request(
        database,
        SUPERSEDE_SQL,
        json!([claim.attempt_id().as_str(), claim.lease_token().as_str()]),
    )
}

fn build_stage_request(
    database: &DatabaseTarget,
    claim: &Claim,
    output: &StagedRevision,
) -> Result<TriggerRequest, RepositoryError> {
    let transcript = serde_json::to_value(output.transcript())
        .map_err(|_| RepositoryError::invalid_response())?;
    let candidates = output
        .candidates()
        .iter()
        .map(|candidate| {
            json!({
                "content_fingerprint": candidate.content_fingerprint(),
                "canonical_payload": candidate.canonical_payload(),
                "supporting_receipt_ids": candidate
                    .supporting_receipt_ids()
                    .iter()
                    .map(|receipt_id| receipt_id.as_str())
                    .collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    let generated = output.generated_revision();

    Ok(build_execute_request(
        database,
        STAGE_SQL,
        json!([
            claim.attempt_id().as_str(),
            claim.lease_token().as_str(),
            claim.session_id().as_str(),
            claim.source_revision().as_str(),
            generated.source_cutoff().to_rfc3339(),
            transcript,
            generated.summary_sentences(),
            generated.summary(),
            generated.concepts(),
            generated.generated_at().to_rfc3339(),
            candidates,
        ]),
    ))
}

fn build_mark_memory_published_request(
    database: &DatabaseTarget,
    claim: &Claim,
    memory_id: &str,
) -> TriggerRequest {
    build_execute_request(
        database,
        MARK_MEMORY_PUBLISHED_SQL,
        json!([
            claim.attempt_id().as_str(),
            claim.lease_token().as_str(),
            claim.session_id().as_str(),
            claim.source_revision().as_str(),
            memory_id,
        ]),
    )
}

fn build_load_unpublished_candidates_request(
    database: &DatabaseTarget,
    claim: &Claim,
) -> TriggerRequest {
    build_execute_request(
        database,
        LOAD_UNPUBLISHED_CANDIDATES_SQL,
        json!([
            claim.attempt_id().as_str(),
            claim.lease_token().as_str(),
            claim.session_id().as_str(),
            claim.source_revision().as_str(),
        ]),
    )
}

fn build_retry_request(
    database: &DatabaseTarget,
    claim: &Claim,
    failure: FailureCategory,
    next_at: &DateTime<Utc>,
) -> TriggerRequest {
    build_execute_request(
        database,
        RETRY_SQL,
        json!([
            claim.attempt_id().as_str(),
            claim.lease_token().as_str(),
            claim.session_id().as_str(),
            claim.source_revision().as_str(),
            failure.to_string(),
            whole_second_text(next_at),
        ]),
    )
}

fn build_promote_request(database: &DatabaseTarget, claim: &Claim) -> TriggerRequest {
    build_execute_request(
        database,
        PROMOTE_SQL,
        json!([
            claim.attempt_id().as_str(),
            claim.lease_token().as_str(),
            claim.session_id().as_str(),
            claim.source_revision().as_str(),
        ]),
    )
}

fn build_execute_request(
    database: &DatabaseTarget,
    sql: &'static str,
    params: Value,
) -> TriggerRequest {
    TriggerRequest {
        function_id: DATABASE_EXECUTE_FUNCTION_ID.to_owned(),
        payload: json!({
            "db": database.as_str(),
            "sql": sql,
            "params": params,
        }),
        action: None,
        timeout_ms: None,
    }
}

fn lease_times(now: DateTime<Utc>, lease: Duration) -> Result<LeaseTimes, RepositoryError> {
    let lease_seconds =
        i64::try_from(lease.as_secs()).map_err(|_| RepositoryError::invalid_response())?;
    if lease_seconds == 0 {
        return Err(RepositoryError::invalid_response());
    }
    let issued_seconds = now.timestamp();
    let issued_at = DateTime::from_timestamp(issued_seconds, 0)
        .ok_or_else(RepositoryError::invalid_response)?;
    let expires_seconds = issued_seconds
        .checked_add(lease_seconds)
        .ok_or_else(RepositoryError::invalid_response)?;
    let expires_at = DateTime::from_timestamp(expires_seconds, 0)
        .ok_or_else(RepositoryError::invalid_response)?;

    Ok(LeaseTimes {
        issued_at,
        expires_at,
    })
}

fn floor_to_whole_second(value: DateTime<Utc>) -> Result<DateTime<Utc>, RepositoryError> {
    DateTime::from_timestamp(value.timestamp(), 0).ok_or_else(RepositoryError::invalid_response)
}

fn whole_second_text(value: &DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn is_uuid_v5(value: &str) -> bool {
    Uuid::parse_str(value)
        .map(|uuid| !uuid.is_nil() && uuid.get_version_num() == 5)
        .unwrap_or(false)
}

fn source_revision(
    source_identities: &[SourceIdentity],
) -> Result<SourceRevision, RepositoryError> {
    let mut source_identities = source_identities.to_vec();
    source_identities.sort_unstable_by(|left, right| {
        left.event_type
            .cmp(&right.event_type)
            .then_with(|| left.receipt_id.cmp(&right.receipt_id))
    });

    let mut name = Vec::new();
    for source_identity in source_identities {
        name.extend_from_slice(source_identity.event_type.as_bytes());
        name.push(b'\0');
        name.extend_from_slice(source_identity.receipt_id.as_str().as_bytes());
        name.push(b'\n');
    }

    let namespace = Uuid::new_v5(&Uuid::NAMESPACE_URL, SOURCE_REVISION_NAMESPACE);
    SourceRevision::try_from(Uuid::new_v5(&namespace, &name).to_string())
        .map_err(|_| RepositoryError::invalid_response())
}

fn decode_loaded_source(
    response: Value,
    claim: &Claim,
) -> Result<Option<LoadedSource>, RepositoryError> {
    let rows = decode_response_rows(response)?;
    if rows.is_empty() {
        return Ok(None);
    }
    let session_id = claim.session_id().clone();
    let mut entries = Vec::with_capacity(rows.len());
    let mut source_identities = Vec::with_capacity(rows.len());
    let mut lifecycle_count = 0_u64;
    let mut observation_count = 0_u64;
    let mut source_cutoff = None;
    let mut prior_projection = None;

    for row in &rows {
        let (input, row_prior_projection) = decode_source_entry(row)?;
        if let Some(prior_projection) = prior_projection.as_ref() {
            if prior_projection != &row_prior_projection {
                return Err(RepositoryError::invalid_response());
            }
        } else {
            prior_projection = Some(row_prior_projection);
        }
        let entry = TranscriptEntry::try_from(input.clone())
            .map_err(|_| RepositoryError::invalid_response())?;
        if entry.session_id() != &session_id {
            return Err(RepositoryError::invalid_response());
        }
        if entry.is_observation() {
            observation_count = observation_count
                .checked_add(1)
                .ok_or_else(RepositoryError::invalid_response)?;
        } else {
            lifecycle_count = lifecycle_count
                .checked_add(1)
                .ok_or_else(RepositoryError::invalid_response)?;
        }
        source_identities.push(SourceIdentity {
            event_type: source_event_type_text(entry.event_type()).to_owned(),
            receipt_id: entry.receipt_id().clone(),
        });
        let ingested_at = entry.ingested_at().to_owned();
        source_cutoff = match source_cutoff {
            Some(current) if current >= ingested_at => Some(current),
            _ => Some(ingested_at),
        };
        entries.push(input);
    }

    Ok(Some(LoadedSource {
        session_id,
        entries,
        lifecycle_count,
        observation_count,
        source_revision: source_revision(&source_identities)?,
        source_cutoff,
        prior_projection: prior_projection.ok_or_else(RepositoryError::invalid_response)?,
    }))
}

fn decode_source_entry(
    row: &Value,
) -> Result<(TranscriptEntryInput, PriorProjection), RepositoryError> {
    let row = object_with_field_count(row, 3)?;
    let prior_projection_present = required_value(row, "prior_projection_present")?
        .as_bool()
        .ok_or_else(RepositoryError::invalid_response)?;
    let prior_projection = match (
        prior_projection_present,
        required_value(row, "prior_transcript")?,
    ) {
        (false, Value::Null) => PriorProjection::Absent,
        (true, transcript) if !transcript.is_null() => PriorProjection::Present(transcript.clone()),
        _ => return Err(RepositoryError::invalid_response()),
    };
    let entry = serde_json::from_value(required_value(row, "entry")?.clone())
        .map_err(|_| RepositoryError::invalid_response())?;

    Ok((entry, prior_projection))
}

fn prior_observation_receipt_ids(
    transcript: Value,
    session_id: &SessionId,
) -> Result<BTreeSet<ReceiptId>, RepositoryError> {
    let transcript = serde_json::from_value::<StoredTranscript>(transcript)
        .map_err(|_| RepositoryError::invalid_response())?;
    if transcript.entries.is_empty() {
        return Err(RepositoryError::invalid_response());
    }

    let mut receipt_ids = BTreeSet::new();
    let mut observation_receipt_ids = BTreeSet::new();
    for input in transcript.entries {
        let entry =
            TranscriptEntry::try_from(input).map_err(|_| RepositoryError::invalid_response())?;
        if entry.session_id() != session_id || !receipt_ids.insert(entry.receipt_id().clone()) {
            return Err(RepositoryError::invalid_response());
        }
        if entry.is_observation() {
            observation_receipt_ids.insert(entry.receipt_id().clone());
        }
    }
    Ok(observation_receipt_ids)
}

fn source_event_type_text(event_type: SourceEventType) -> &'static str {
    match event_type {
        SourceEventType::SessionStart => "session_start",
        SourceEventType::SessionEnd => "session_end",
        SourceEventType::Observation => "observation",
    }
}

fn decode_supersede_response(response: Value) -> Result<(), RepositoryError> {
    match decode_response_rows(response)?.as_slice() {
        [] => Err(RepositoryError::unconfirmed_commit()),
        [row] => {
            let row = object_with_field_count(row, 1)?;
            match row.get("superseded") {
                Some(Value::Bool(true)) => Ok(()),
                _ => Err(RepositoryError::invalid_response()),
            }
        }
        _ => Err(RepositoryError::invalid_response()),
    }
}

fn decode_stage_response(response: Value, claim: &Claim) -> Result<(), RepositoryError> {
    match decode_response_rows(response)?.as_slice() {
        [] => Err(RepositoryError::unconfirmed_commit()),
        [row] => {
            let row = object_with_field_count(row, 4)?;
            if required_text(row, "attempt_id")? != claim.attempt_id().as_str()
                || required_text(row, "session_id")? != claim.session_id().as_str()
                || required_text(row, "source_revision")? != claim.source_revision().as_str()
                || required_text(row, "state")? != "staged"
            {
                return Err(RepositoryError::invalid_response());
            }

            Ok(())
        }
        _ => Err(RepositoryError::invalid_response()),
    }
}

fn decode_mark_memory_published_response(
    response: Value,
    claim: &Claim,
    memory_id: &str,
) -> Result<(), RepositoryError> {
    match decode_response_rows(response)?.as_slice() {
        [] => Err(RepositoryError::unconfirmed_commit()),
        [row] => {
            let row = object_with_field_count(row, 4)?;
            if required_text(row, "attempt_id")? != claim.attempt_id().as_str()
                || required_text(row, "memory_id")? != memory_id
                || required_text(row, "state")? != "publishing"
            {
                return Err(RepositoryError::invalid_response());
            }
            DateTime::parse_from_rfc3339(required_text(row, "published_at")?)
                .map_err(|_| RepositoryError::invalid_response())?;

            Ok(())
        }
        _ => Err(RepositoryError::invalid_response()),
    }
}

fn decode_unpublished_candidates_response(
    response: Value,
    claim: &Claim,
) -> Result<Vec<StagedMemoryCandidate>, RepositoryError> {
    match decode_response_rows(response)?.as_slice() {
        [] => Err(RepositoryError::unconfirmed_commit()),
        [row] => {
            let row = object_with_field_count(row, 5)?;
            if required_text(row, "attempt_id")? != claim.attempt_id().as_str()
                || required_text(row, "session_id")? != claim.session_id().as_str()
                || required_text(row, "source_revision")? != claim.source_revision().as_str()
                || required_text(row, "state")? != "publishing"
            {
                return Err(RepositoryError::invalid_response());
            }
            let candidate_rows = required_value(row, "candidates")?
                .as_array()
                .ok_or_else(RepositoryError::invalid_response)?;
            let mut candidates = Vec::with_capacity(candidate_rows.len());
            let mut prior_ordinal = None;
            let mut fingerprints = BTreeSet::new();
            let mut memory_ids = BTreeSet::new();

            for candidate_row in candidate_rows {
                let candidate_row = object_with_field_count(candidate_row, 5)?;
                let ordinal = required_value(candidate_row, "ordinal")?
                    .as_i64()
                    .filter(|ordinal| *ordinal >= 0)
                    .ok_or_else(RepositoryError::invalid_response)?;
                if prior_ordinal.is_some_and(|prior| ordinal <= prior) {
                    return Err(RepositoryError::invalid_response());
                }
                prior_ordinal = Some(ordinal);

                let content_fingerprint =
                    required_text(candidate_row, "content_fingerprint")?.to_owned();
                let memory_id = required_text(candidate_row, "memory_id")?;
                if !is_uuid_v5(memory_id) {
                    return Err(RepositoryError::invalid_response());
                }
                let canonical_payload =
                    decode_canonical_payload(required_value(candidate_row, "canonical_payload")?)?;
                if canonical_payload.id != memory_id {
                    return Err(RepositoryError::invalid_response());
                }
                let supporting_receipt_ids =
                    decode_text_array(required_value(candidate_row, "supporting_receipt_ids")?)?;
                let candidate = StagedMemoryCandidate::try_new(
                    content_fingerprint.clone(),
                    canonical_payload,
                    supporting_receipt_ids,
                )
                .map_err(|_| RepositoryError::invalid_response())?;
                if candidate.canonical_payload().session_ids.len() != 1
                    || candidate.canonical_payload().session_ids[0] != claim.session_id().as_str()
                    || !fingerprints.insert(content_fingerprint)
                    || !memory_ids.insert(memory_id.to_owned())
                {
                    return Err(RepositoryError::invalid_response());
                }
                candidates.push(candidate);
            }

            Ok(candidates)
        }
        _ => Err(RepositoryError::invalid_response()),
    }
}

fn decode_retry_response(
    response: Value,
    claim: &Claim,
    failure: FailureCategory,
    next_at: &DateTime<Utc>,
) -> Result<(), RepositoryError> {
    match decode_response_rows(response)?.as_slice() {
        [] => Err(RepositoryError::unconfirmed_commit()),
        [row] => {
            let row = object_with_field_count(row, 6)?;
            if required_text(row, "attempt_id")? != claim.attempt_id().as_str()
                || required_text(row, "session_id")? != claim.session_id().as_str()
                || required_text(row, "source_revision")? != claim.source_revision().as_str()
                || required_text(row, "state")? != "retryable"
                || required_text(row, "failure_category")? != failure.to_string()
            {
                return Err(RepositoryError::invalid_response());
            }
            let persisted_next_at =
                DateTime::parse_from_rfc3339(required_text(row, "next_attempt_at")?)
                    .map_err(|_| RepositoryError::invalid_response())?
                    .with_timezone(&Utc);
            if persisted_next_at != *next_at {
                return Err(RepositoryError::invalid_response());
            }

            Ok(())
        }
        _ => Err(RepositoryError::invalid_response()),
    }
}

fn decode_promote_response(response: Value, claim: &Claim) -> Result<(), RepositoryError> {
    match decode_response_rows(response)?.as_slice() {
        [] => Err(RepositoryError::unconfirmed_commit()),
        [row] => {
            let row = object_with_field_count(row, 4)?;
            if required_text(row, "attempt_id")? != claim.attempt_id().as_str()
                || required_text(row, "session_id")? != claim.session_id().as_str()
                || required_text(row, "source_revision")? != claim.source_revision().as_str()
                || required_text(row, "state")? != "complete"
            {
                return Err(RepositoryError::invalid_response());
            }

            Ok(())
        }
        _ => Err(RepositoryError::invalid_response()),
    }
}

fn decode_canonical_payload(value: &Value) -> Result<MemoryVersionInput, RepositoryError> {
    let payload = object_with_field_count(value, 11)?;
    for field in [
        "id",
        "version",
        "memory_type",
        "title",
        "content",
        "created_at",
        "updated_at",
        "concepts",
        "files",
        "session_ids",
        "source_observation_ids",
    ] {
        required_value(payload, field)?;
    }

    serde_json::from_value(value.clone()).map_err(|_| RepositoryError::invalid_response())
}

fn decode_text_array(value: &Value) -> Result<Vec<String>, RepositoryError> {
    value
        .as_array()
        .ok_or_else(RepositoryError::invalid_response)?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(RepositoryError::invalid_response)
        })
        .collect()
}

fn decode_claim_candidates(
    response: Value,
    limit: u32,
) -> Result<Vec<ClaimCandidate>, RepositoryError> {
    let rows = decode_response_rows(response)?;
    if u64::try_from(rows.len()).map_err(|_| RepositoryError::invalid_response())?
        > u64::from(limit)
    {
        return Err(RepositoryError::invalid_response());
    }

    let mut session_ids = BTreeSet::new();
    let mut candidates = Vec::with_capacity(rows.len());
    for row in &rows {
        let candidate = decode_claim_candidate(row)?;
        if !session_ids.insert(candidate.session_id.clone()) {
            return Err(RepositoryError::invalid_response());
        }
        candidates.push(candidate);
    }

    Ok(candidates)
}

fn decode_claim_candidate(row: &Value) -> Result<ClaimCandidate, RepositoryError> {
    let row = object_with_field_count(row, 4)?;
    let session_id = SessionId::try_from(required_text(row, "session_id")?.to_owned())
        .map_err(|_| RepositoryError::invalid_response())?;
    let lifecycle_count = decode_count(required_value(row, "lifecycle_count")?)?;
    let observation_count = decode_count(required_value(row, "observation_count")?)?;
    let source_identities = required_value(row, "source_identities")?
        .as_array()
        .ok_or_else(RepositoryError::invalid_response)?;
    let mut identities = BTreeSet::new();
    let mut decoded_identities = Vec::with_capacity(source_identities.len());
    let mut actual_lifecycle_count = 0_u64;
    let mut actual_observation_count = 0_u64;

    for source_identity in source_identities {
        let source_identity = decode_source_identity(source_identity)?;
        match source_identity.event_type.as_str() {
            "session_start" | "session_end" => {
                actual_lifecycle_count = actual_lifecycle_count
                    .checked_add(1)
                    .ok_or_else(RepositoryError::invalid_response)?;
            }
            "observation" => {
                actual_observation_count = actual_observation_count
                    .checked_add(1)
                    .ok_or_else(RepositoryError::invalid_response)?;
            }
            _ => return Err(RepositoryError::invalid_response()),
        }
        if !identities.insert((
            source_identity.event_type.clone(),
            source_identity.receipt_id.as_str().to_owned(),
        )) {
            return Err(RepositoryError::invalid_response());
        }
        decoded_identities.push(source_identity);
    }

    let source_count = lifecycle_count
        .checked_add(observation_count)
        .ok_or_else(RepositoryError::invalid_response)?;
    if source_count == 0
        || lifecycle_count != actual_lifecycle_count
        || observation_count != actual_observation_count
    {
        return Err(RepositoryError::invalid_response());
    }

    Ok(ClaimCandidate {
        session_id,
        lifecycle_count,
        observation_count,
        source_identities: decoded_identities,
    })
}

fn decode_source_identity(row: &Value) -> Result<SourceIdentity, RepositoryError> {
    let row = object_with_field_count(row, 2)?;
    let event_type = required_text(row, "event_type")?;
    if !matches!(event_type, "session_start" | "session_end" | "observation") {
        return Err(RepositoryError::invalid_response());
    }
    let receipt_id = ReceiptId::try_from(required_text(row, "receipt_id")?.to_owned())
        .map_err(|_| RepositoryError::invalid_response())?;

    Ok(SourceIdentity {
        event_type: event_type.to_owned(),
        receipt_id,
    })
}

fn decode_claim_response(response: Value) -> Result<Option<Claim>, RepositoryError> {
    match decode_response_rows(response)?.as_slice() {
        [] => Ok(None),
        [row] => decode_claim_row(row).map(Some),
        _ => Err(RepositoryError::invalid_response()),
    }
}

fn decode_claim_row(row: &Value) -> Result<Claim, RepositoryError> {
    let row = object_with_field_count(row, 8)?;
    let state = serde_json::from_value::<AttemptState>(required_value(row, "state")?.clone())
        .map_err(|_| RepositoryError::invalid_response())?;
    let lease_expires_at = DateTime::parse_from_rfc3339(required_text(row, "lease_expires_at")?)
        .map_err(|_| RepositoryError::invalid_response())?
        .with_timezone(&Utc);

    Claim::try_from(ClaimInput {
        attempt_id: required_text(row, "attempt_id")?.to_owned(),
        session_id: required_text(row, "session_id")?.to_owned(),
        source_revision: required_text(row, "source_revision")?.to_owned(),
        lifecycle_count: decode_count(required_value(row, "lifecycle_count")?)?,
        observation_count: decode_count(required_value(row, "observation_count")?)?,
        state,
        lease_token: required_text(row, "lease_token")?.to_owned(),
        lease_expires_at,
    })
    .map_err(|_| RepositoryError::invalid_response())
}

fn decode_renew_response(response: Value) -> Result<bool, RepositoryError> {
    match decode_response_rows(response)?.as_slice() {
        [] => Ok(false),
        [row] => {
            let row = object_with_field_count(row, 1)?;
            match row.get("renewed") {
                Some(Value::Bool(true)) => Ok(true),
                _ => Err(RepositoryError::invalid_response()),
            }
        }
        _ => Err(RepositoryError::invalid_response()),
    }
}

fn decode_response_rows(response: Value) -> Result<Vec<Value>, RepositoryError> {
    let response = response
        .as_object()
        .ok_or_else(RepositoryError::invalid_response)?;
    if response.len() != 3 {
        return Err(RepositoryError::invalid_response());
    }
    let affected_rows = response
        .get("affected_rows")
        .and_then(Value::as_u64)
        .ok_or_else(RepositoryError::invalid_response)?;
    match response.get("last_insert_id") {
        Some(Value::Null) => {}
        _ => return Err(RepositoryError::invalid_response()),
    }
    let returned_rows = response
        .get("returned_rows")
        .and_then(Value::as_array)
        .ok_or_else(RepositoryError::invalid_response)?;
    let returned_row_count =
        u64::try_from(returned_rows.len()).map_err(|_| RepositoryError::invalid_response())?;
    if affected_rows != returned_row_count {
        return Err(RepositoryError::invalid_response());
    }

    Ok(returned_rows.clone())
}

fn object_with_field_count(
    value: &Value,
    field_count: usize,
) -> Result<&serde_json::Map<String, Value>, RepositoryError> {
    let value = value
        .as_object()
        .ok_or_else(RepositoryError::invalid_response)?;
    if value.len() != field_count {
        return Err(RepositoryError::invalid_response());
    }

    Ok(value)
}

fn required_value<'a>(
    row: &'a serde_json::Map<String, Value>,
    field: &str,
) -> Result<&'a Value, RepositoryError> {
    row.get(field).ok_or_else(RepositoryError::invalid_response)
}

fn required_text<'a>(
    row: &'a serde_json::Map<String, Value>,
    field: &str,
) -> Result<&'a str, RepositoryError> {
    required_value(row, field)?
        .as_str()
        .ok_or_else(RepositoryError::invalid_response)
}

fn decode_count(value: &Value) -> Result<u64, RepositoryError> {
    value
        .as_str()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(RepositoryError::invalid_response)
}

fn decode_discovery_response(response: Value) -> Result<Vec<SessionId>, RepositoryError> {
    let response = response
        .as_object()
        .ok_or_else(RepositoryError::invalid_response)?;
    if response.len() != 3 {
        return Err(RepositoryError::invalid_response());
    }
    let affected_rows = response
        .get("affected_rows")
        .and_then(Value::as_u64)
        .ok_or_else(RepositoryError::invalid_response)?;
    match response.get("last_insert_id") {
        Some(Value::Null) => {}
        _ => return Err(RepositoryError::invalid_response()),
    }
    let returned_rows = response
        .get("returned_rows")
        .and_then(Value::as_array)
        .ok_or_else(RepositoryError::invalid_response)?;
    let returned_row_count =
        u64::try_from(returned_rows.len()).map_err(|_| RepositoryError::invalid_response())?;
    if affected_rows != returned_row_count {
        return Err(RepositoryError::invalid_response());
    }

    returned_rows.iter().map(decode_discovery_row).collect()
}

fn decode_discovery_row(row: &Value) -> Result<SessionId, RepositoryError> {
    let row = row
        .as_object()
        .ok_or_else(RepositoryError::invalid_response)?;
    if row.len() != 1 {
        return Err(RepositoryError::invalid_response());
    }
    let session_id = row
        .get("session_id")
        .and_then(Value::as_str)
        .ok_or_else(RepositoryError::invalid_response)?;

    SessionId::try_from(session_id.to_owned()).map_err(|_| RepositoryError::invalid_response())
}
