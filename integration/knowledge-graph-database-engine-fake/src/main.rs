use std::{error::Error, fmt, future::Future, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use iii_sdk::{IIIClient, InitOptions, WorkerIdentityMode, register_worker};
use knowledge_graph_store::{
    GraphConfigurationError, IiiKnowledgeGraphStore,
    contracts::{
        Alias, AliasQuery, Assertion, AssertionEvidence, AssertionEvidenceInput, AssertionId,
        AssertionIdInput, BackendFailureReason, Concept, ConceptId, ConceptIdInput, ConceptMention,
        ConceptMentionInput, ConflictField, ConflictReason, CreateAssertion, CreateConcept,
        DatabaseTimeouts, DeleteAssertion, DeleteConcept, DeleteSource, DirectionMode,
        EdgeOrientation, GraphError, GraphPath, GraphResult, LimitReason, LimitResource,
        NeighborQuery, NeighborResult, NotFoundReason, OperationCode, OrientedAssertion, PathQuery,
        RecordIdentity, RecordKind, ReferenceReason, ReferencedRecord, RelatedSourceQuery,
        RelatedSourceResult, RelatedSourceRole, RelationType, ReplaceConceptAliases, Revision,
        SourceReference, SourceReferenceInput, TraversalBound, TraversalReason, UpdateAssertion,
    },
};
use serde_json::{Map, Value, json};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
use uuid::{Uuid, Variant};

const DATABASE_EXECUTE_FUNCTION_ID: &str = "database::execute";
const DATABASE_QUERY_FUNCTION_ID: &str = "database::query";
const WORKER_REGISTER_FUNCTION_ID: &str = "engine::workers::register";
const DATABASE_TARGET: &str = "knowledge-graph-database-fake-private-target";
const QUERY_TIMEOUT: Duration = Duration::from_secs(1);
const INVOCATION_TIMEOUT: Duration = Duration::from_secs(2);
const QUERY_TIMEOUT_MILLIS: u64 = 1_000;
const PEER_TIMEOUT: Duration = Duration::from_secs(6);
const OPERATION_TIMEOUT: Duration = Duration::from_secs(5);
const RESPONSE_HOLD_DELAY: Duration = Duration::from_millis(50);

const ALIAS_SENTINEL: &str = "PRIVATE_ALIAS_SENTINEL";
const OTHER_ALIAS_SENTINEL: &str = "PRIVATE_OTHER_ALIAS_SENTINEL";
const MEMORY_SOURCE_SENTINEL: &str = "PRIVATE_MEMORY_SOURCE_SENTINEL";
const SESSION_SOURCE_SENTINEL: &str = "PRIVATE_SESSION_SOURCE_SENTINEL";
const EVIDENCE_LIMIT_SOURCE_SENTINEL: &str = "PRIVATE_EVIDENCE_LIMIT_SOURCE_SENTINEL";
const PATH_SENTINEL: &str = "PRIVATE_PATH_VALUE_SENTINEL";
const BACKEND_SENTINEL: &str = "PRIVATE_BACKEND_VALUE_SENTINEL";
const BACKEND_CODE_SENTINEL: &str = "PRIVATE_BACKEND_CODE_SENTINEL";
const BACKEND_STACK_SENTINEL: &str = "PRIVATE_BACKEND_STACK_SENTINEL";
const SQL_SENTINEL: &str = "PRIVATE_SQL_VALUE_SENTINEL";

const UUID_V7_BASE: u128 = 0x0000_0000_0000_7000_8000_0000_0000_0000;

const CREATE_CONCEPT_SQL: &str =
    "SELECT knowledge_graph.graph_create_concept($1::text::uuid, $2::jsonb)";
const REPLACE_CONCEPT_ALIASES_SQL: &str = "SELECT knowledge_graph.graph_replace_concept_aliases($1::text::uuid, $2::text::bigint, $3::jsonb)";
const DELETE_CONCEPT_SQL: &str =
    "SELECT knowledge_graph.graph_delete_concept($1::text::uuid, $2::text::bigint)";
const REGISTER_SOURCE_SQL: &str =
    "SELECT knowledge_graph.graph_register_source($1::text, $2::text, $3::text::bigint)";
const DELETE_SOURCE_SQL: &str =
    "SELECT knowledge_graph.graph_delete_source($1::text, $2::text, $3::text::bigint)";
const CREATE_MENTION_SQL: &str = "SELECT knowledge_graph.graph_create_mention($1::text::uuid, $2::text, $3::text, $4::text::bigint)";
const DELETE_MENTION_SQL: &str = "SELECT knowledge_graph.graph_delete_mention($1::text::uuid, $2::text, $3::text, $4::text::bigint)";
const CREATE_ASSERTION_SQL: &str = "SELECT knowledge_graph.graph_create_assertion($1::text::uuid, $2::text::uuid, $3::text, $4::text::uuid, $5::jsonb)";
const UPDATE_ASSERTION_SQL: &str = "SELECT knowledge_graph.graph_update_assertion($1::text::uuid, $2::text::bigint, $3::text::uuid, $4::text, $5::text::uuid)";
const DELETE_ASSERTION_SQL: &str =
    "SELECT knowledge_graph.graph_delete_assertion($1::text::uuid, $2::text::bigint)";
const ADD_ASSERTION_EVIDENCE_SQL: &str = "SELECT knowledge_graph.graph_add_assertion_evidence($1::text::uuid, $2::text, $3::text, $4::text::bigint)";
const REMOVE_ASSERTION_EVIDENCE_SQL: &str = "SELECT knowledge_graph.graph_remove_assertion_evidence($1::text::uuid, $2::text, $3::text, $4::text::bigint)";

const GET_ASSERTION_SQL: &str = r#"
WITH assertion_payload AS (
  SELECT pg_catalog.jsonb_build_object(
    'id', assertions.id,
    'revision', assertions.revision,
    'subject_concept_id', assertions.subject_concept_id,
    'relation_type', assertions.relation_code,
    'object_concept_id', assertions.object_concept_id,
    'evidence', COALESCE(
      (
        SELECT pg_catalog.jsonb_agg(
          CASE source_references.source_kind COLLATE "C"
            WHEN 'memory_version' THEN pg_catalog.jsonb_build_object(
              'kind', 'memory_version',
              'memory_id', source_references.external_id,
              'version', source_references.external_version
            )
            ELSE pg_catalog.jsonb_build_object(
              'kind', 'session_record',
              'session_record_id', source_references.external_id
            )
          END
          ORDER BY source_references.source_kind COLLATE "C" ASC,
            source_references.external_id COLLATE "C" ASC,
            source_references.external_version ASC NULLS FIRST
        )
        FROM knowledge_graph.graph_assertion_evidence AS evidence
        JOIN knowledge_graph.graph_source_references AS source_references
          ON source_references.source_ref_id = evidence.source_ref_id
        WHERE evidence.assertion_id = assertions.id
      ),
      '[]'::jsonb
    )
  ) AS result
  FROM knowledge_graph.graph_assertions AS assertions
  WHERE assertions.id = $1::text::uuid
), complete_payload AS (
  SELECT pg_catalog.jsonb_build_object(
    'outcome', 'complete',
    'result', (SELECT result FROM assertion_payload)
  ) AS payload
)
SELECT CASE
  WHEN pg_catalog.octet_length(complete_payload.payload::text) > 4194304
    THEN pg_catalog.jsonb_build_object('outcome', 'result_too_large')
  ELSE complete_payload.payload
END AS payload
FROM complete_payload
"#;

const GET_CONCEPT_SQL: &str = r#"
WITH concept_payload AS (
  SELECT pg_catalog.jsonb_build_object(
    'id', concepts.id,
    'revision', concepts.revision,
    'aliases', COALESCE(
      (
        SELECT pg_catalog.jsonb_agg(
          pg_catalog.jsonb_build_object(
            'display_text', aliases.display_text,
            'preferred', aliases.is_preferred
          )
          ORDER BY aliases.is_preferred DESC, aliases.alias_key COLLATE "C" ASC
        )
        FROM knowledge_graph.graph_aliases AS aliases
        WHERE aliases.concept_id = concepts.id
      ),
      '[]'::jsonb
    )
  ) AS result
  FROM knowledge_graph.graph_concepts AS concepts
  WHERE concepts.id = $1::text::uuid
), complete_payload AS (
  SELECT pg_catalog.jsonb_build_object(
    'outcome', 'complete',
    'result', (SELECT result FROM concept_payload)
  ) AS payload
)
SELECT CASE
  WHEN pg_catalog.octet_length(complete_payload.payload::text) > 4194304
    THEN pg_catalog.jsonb_build_object('outcome', 'result_too_large')
  ELSE complete_payload.payload
END AS payload
FROM complete_payload
"#;

const RESOLVE_ALIAS_SQL: &str = r#"
WITH matching_concept AS (
  SELECT concepts.id, concepts.revision
  FROM knowledge_graph.graph_concepts AS concepts
  JOIN knowledge_graph.graph_aliases AS matching_alias
    ON matching_alias.concept_id = concepts.id
  WHERE matching_alias.alias_key = $1::text COLLATE "C"
  ORDER BY concepts.id ASC
  LIMIT 1
), concept_payload AS (
  SELECT pg_catalog.jsonb_build_object(
    'id', concepts.id,
    'revision', concepts.revision,
    'aliases', COALESCE(
      (
        SELECT pg_catalog.jsonb_agg(
          pg_catalog.jsonb_build_object(
            'display_text', aliases.display_text,
            'preferred', aliases.is_preferred
          )
          ORDER BY aliases.is_preferred DESC, aliases.alias_key COLLATE "C" ASC
        )
        FROM knowledge_graph.graph_aliases AS aliases
        WHERE aliases.concept_id = concepts.id
      ),
      '[]'::jsonb
    )
  ) AS result
  FROM matching_concept AS concepts
), complete_payload AS (
  SELECT pg_catalog.jsonb_build_object(
    'outcome', 'complete',
    'result', (SELECT result FROM concept_payload)
  ) AS payload
)
SELECT CASE
  WHEN pg_catalog.octet_length(complete_payload.payload::text) > 4194304
    THEN pg_catalog.jsonb_build_object('outcome', 'result_too_large')
  ELSE complete_payload.payload
END AS payload
FROM complete_payload
"#;

const GET_SOURCE_SQL: &str = r#"
WITH source_payload AS (
  SELECT CASE source_references.source_kind
    WHEN 'memory_version' THEN pg_catalog.jsonb_build_object(
      'kind', 'memory_version',
      'memory_id', source_references.external_id,
      'version', source_references.external_version
    )
    WHEN 'session_record' THEN pg_catalog.jsonb_build_object(
      'kind', 'session_record',
      'session_record_id', source_references.external_id
    )
    ELSE pg_catalog.jsonb_build_object('kind', source_references.source_kind)
  END AS result
  FROM knowledge_graph.graph_source_references AS source_references
  WHERE source_references.source_kind = $1::text COLLATE "C"
    AND source_references.external_id = $2::text COLLATE "C"
    AND source_references.external_version IS NOT DISTINCT FROM $3::text::bigint
), complete_payload AS (
  SELECT pg_catalog.jsonb_build_object(
    'outcome', 'complete',
    'result', (SELECT result FROM source_payload)
  ) AS payload
)
SELECT CASE
  WHEN pg_catalog.octet_length(complete_payload.payload::text) > 4194304
    THEN pg_catalog.jsonb_build_object('outcome', 'result_too_large')
  ELSE complete_payload.payload
END AS payload
FROM complete_payload
"#;

const GET_NEIGHBORS_SQL: &str = r#"
WITH adjacent_assertions AS (
  SELECT
    assertions.id AS assertion_id,
    assertions.revision AS assertion_revision,
    assertions.subject_concept_id,
    assertions.relation_code,
    assertions.object_concept_id,
    CASE
      WHEN relation_types.is_symmetric THEN 'symmetric'
      WHEN assertions.subject_concept_id = $1::text::uuid THEN 'outgoing'
      ELSE 'incoming'
    END AS orientation,
    CASE
      WHEN assertions.subject_concept_id = $1::text::uuid
        THEN assertions.object_concept_id
      ELSE assertions.subject_concept_id
    END AS neighbor_id
  FROM knowledge_graph.graph_assertions AS assertions
  JOIN knowledge_graph.graph_relation_types AS relation_types
    ON relation_types.code = assertions.relation_code
  WHERE (
    (
      relation_types.is_symmetric
      AND (
        assertions.subject_concept_id = $1::text::uuid
        OR assertions.object_concept_id = $1::text::uuid
      )
    )
    OR (
      NOT relation_types.is_symmetric
      AND (
        ($2::text = 'outgoing' AND assertions.subject_concept_id = $1::text::uuid)
        OR ($2::text = 'incoming' AND assertions.object_concept_id = $1::text::uuid)
        OR (
          $2::text = 'either'
          AND (
            assertions.subject_concept_id = $1::text::uuid
            OR assertions.object_concept_id = $1::text::uuid
          )
        )
      )
    )
  )
    AND (
      pg_catalog.jsonb_array_length($3::jsonb) = 0
      OR EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements_text($3::jsonb)
          AS requested_relations(relation_code)
        WHERE requested_relations.relation_code COLLATE "C" = assertions.relation_code
      )
    )
), ordered_neighbors AS (
  SELECT
    adjacent_assertions.assertion_id,
    adjacent_assertions.assertion_revision,
    adjacent_assertions.subject_concept_id,
    adjacent_assertions.relation_code,
    adjacent_assertions.object_concept_id,
    adjacent_assertions.orientation,
    adjacent_assertions.neighbor_id
  FROM adjacent_assertions
  ORDER BY relation_code COLLATE "C" ASC,
    orientation COLLATE "C" ASC,
    neighbor_id ASC,
    assertion_id ASC
  LIMIT $4::text::bigint
), neighbor_results AS (
  SELECT
    ordered_neighbors.relation_code,
    ordered_neighbors.orientation,
    concepts.id AS neighbor_id,
    ordered_neighbors.assertion_id,
    pg_catalog.jsonb_build_object(
      'neighbor', pg_catalog.jsonb_build_object(
        'id', concepts.id,
        'revision', concepts.revision,
        'aliases', COALESCE(
          (
            SELECT pg_catalog.jsonb_agg(
              pg_catalog.jsonb_build_object(
                'display_text', aliases.display_text,
                'preferred', aliases.is_preferred
              )
              ORDER BY aliases.is_preferred DESC, aliases.alias_key COLLATE "C" ASC
            )
            FROM knowledge_graph.graph_aliases AS aliases
            WHERE aliases.concept_id = concepts.id
          ),
          '[]'::jsonb
        )
      ),
      'assertion', pg_catalog.jsonb_build_object(
        'id', ordered_neighbors.assertion_id,
        'revision', ordered_neighbors.assertion_revision,
        'subject_concept_id', ordered_neighbors.subject_concept_id,
        'relation_type', ordered_neighbors.relation_code,
        'object_concept_id', ordered_neighbors.object_concept_id,
        'evidence', COALESCE(
          (
            SELECT pg_catalog.jsonb_agg(
              CASE source_references.source_kind COLLATE "C"
                WHEN 'memory_version' THEN pg_catalog.jsonb_build_object(
                  'kind', 'memory_version',
                  'memory_id', source_references.external_id,
                  'version', source_references.external_version
                )
                ELSE pg_catalog.jsonb_build_object(
                  'kind', 'session_record',
                  'session_record_id', source_references.external_id
                )
              END
              ORDER BY source_references.source_kind COLLATE "C" ASC,
                source_references.external_id COLLATE "C" ASC,
                source_references.external_version ASC NULLS FIRST
            )
            FROM knowledge_graph.graph_assertion_evidence AS evidence
            JOIN knowledge_graph.graph_source_references AS source_references
              ON source_references.source_ref_id = evidence.source_ref_id
            WHERE evidence.assertion_id = ordered_neighbors.assertion_id
          ),
          '[]'::jsonb
        )
      ),
      'orientation', ordered_neighbors.orientation
    ) AS result
  FROM ordered_neighbors
  JOIN knowledge_graph.graph_concepts AS concepts
    ON concepts.id = ordered_neighbors.neighbor_id
), neighbor_payload AS (
  SELECT COALESCE(
    pg_catalog.jsonb_agg(
      neighbor_results.result
      ORDER BY neighbor_results.relation_code COLLATE "C" ASC,
        neighbor_results.orientation COLLATE "C" ASC,
        neighbor_results.neighbor_id ASC,
        neighbor_results.assertion_id ASC
    ),
    '[]'::jsonb
  ) AS result
  FROM neighbor_results
), complete_payload AS (
  SELECT pg_catalog.jsonb_build_object(
    'outcome', 'complete',
    'result', (SELECT result FROM neighbor_payload)
  ) AS payload
)
SELECT CASE
  WHEN pg_catalog.octet_length(complete_payload.payload::text) > 4194304
    THEN pg_catalog.jsonb_build_object('outcome', 'result_too_large')
  ELSE complete_payload.payload
END AS payload
FROM complete_payload
"#;

const GET_RELATED_SOURCES_SQL: &str = r#"
WITH related_source_links AS (
  SELECT
    source_references.source_kind AS source_kind,
    source_references.external_id AS external_id,
    source_references.external_version AS external_version,
    'mention'::text AS role
  FROM knowledge_graph.graph_concept_mentions AS mentions
  JOIN knowledge_graph.graph_source_references AS source_references
    ON source_references.source_ref_id = mentions.source_ref_id
  WHERE mentions.concept_id = $1::text::uuid
  UNION
  SELECT
    source_references.source_kind AS source_kind,
    source_references.external_id AS external_id,
    source_references.external_version AS external_version,
    'assertion_evidence'::text AS role
  FROM knowledge_graph.graph_assertions AS assertions
  JOIN knowledge_graph.graph_assertion_evidence AS evidence
    ON evidence.assertion_id = assertions.id
  JOIN knowledge_graph.graph_source_references AS source_references
    ON source_references.source_ref_id = evidence.source_ref_id
  WHERE assertions.subject_concept_id = $1::text::uuid
    OR assertions.object_concept_id = $1::text::uuid
), ordered_sources AS (
  SELECT source_kind, external_id, external_version, role,
    CASE role WHEN 'mention' THEN 0 ELSE 1 END AS role_order
  FROM related_source_links
  ORDER BY role_order ASC,
    source_kind COLLATE "C" ASC,
    external_id COLLATE "C" ASC,
    external_version ASC NULLS FIRST
  LIMIT $2::text::bigint
), related_source_payload AS (
  SELECT COALESCE(
    pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'source', CASE ordered_sources.source_kind COLLATE "C"
          WHEN 'memory_version' THEN pg_catalog.jsonb_build_object(
            'kind', 'memory_version',
            'memory_id', ordered_sources.external_id,
            'version', ordered_sources.external_version
          )
          ELSE pg_catalog.jsonb_build_object(
            'kind', 'session_record',
            'session_record_id', ordered_sources.external_id
          )
        END,
        'role', ordered_sources.role
      )
      ORDER BY ordered_sources.role_order ASC,
        ordered_sources.source_kind COLLATE "C" ASC,
        ordered_sources.external_id COLLATE "C" ASC,
        ordered_sources.external_version ASC NULLS FIRST
    ),
    '[]'::jsonb
  ) AS result
  FROM ordered_sources
), complete_payload AS (
  SELECT pg_catalog.jsonb_build_object(
    'outcome', 'complete',
    'result', (SELECT result FROM related_source_payload)
  ) AS payload
)
SELECT CASE
  WHEN pg_catalog.octet_length(complete_payload.payload::text) > 4194304
    THEN pg_catalog.jsonb_build_object('outcome', 'result_too_large')
  ELSE complete_payload.payload
END AS payload
FROM complete_payload
"#;

const GET_PATHS_SQL: &str = r#"
WITH RECURSIVE path_states (
  current_concept_id,
  concept_ids,
  assertion_ids,
  orientations,
  hops
) AS (
  SELECT
    start_concepts.id,
    ARRAY[start_concepts.id]::uuid[],
    ARRAY[]::uuid[],
    ARRAY[]::text[],
    0::integer
  FROM knowledge_graph.graph_concepts AS start_concepts
  WHERE start_concepts.id = $1::text::uuid

  UNION ALL

  SELECT
    CASE
      WHEN assertions.subject_concept_id = path_states.current_concept_id
        THEN assertions.object_concept_id
      ELSE assertions.subject_concept_id
    END,
    path_states.concept_ids || CASE
      WHEN assertions.subject_concept_id = path_states.current_concept_id
        THEN assertions.object_concept_id
      ELSE assertions.subject_concept_id
    END,
    path_states.assertion_ids || assertions.id,
    path_states.orientations || CASE
      WHEN relation_types.is_symmetric THEN 'symmetric'::text
      WHEN assertions.subject_concept_id = path_states.current_concept_id
        THEN 'outgoing'::text
      ELSE 'incoming'::text
    END,
    path_states.hops + 1
  FROM path_states
  JOIN knowledge_graph.graph_assertions AS assertions
    ON assertions.subject_concept_id = path_states.current_concept_id
    OR assertions.object_concept_id = path_states.current_concept_id
  JOIN knowledge_graph.graph_relation_types AS relation_types
    ON relation_types.code = assertions.relation_code
  WHERE path_states.hops < $4::text::bigint
    AND path_states.current_concept_id <> $2::text::uuid
    AND (
      relation_types.is_symmetric
      OR (
        NOT relation_types.is_symmetric
        AND (
          ($3::text = 'outgoing' AND assertions.subject_concept_id = path_states.current_concept_id)
          OR ($3::text = 'incoming' AND assertions.object_concept_id = path_states.current_concept_id)
          OR (
            $3::text = 'either'
            AND (
              assertions.subject_concept_id = path_states.current_concept_id
              OR assertions.object_concept_id = path_states.current_concept_id
            )
          )
        )
      )
    )
    AND NOT (
      CASE
        WHEN assertions.subject_concept_id = path_states.current_concept_id
          THEN assertions.object_concept_id
        ELSE assertions.subject_concept_id
      END = ANY(path_states.concept_ids)
    )
    AND NOT (assertions.id = ANY(path_states.assertion_ids))
), fenced_path_states AS MATERIALIZED (
  SELECT *
  FROM path_states
  LIMIT $5::text::bigint + 1
), path_state_count AS MATERIALIZED (
  SELECT pg_catalog.count(*) AS state_count
  FROM fenced_path_states
), ordered_path_states AS MATERIALIZED (
  SELECT path_states.*
  FROM fenced_path_states AS path_states
  CROSS JOIN path_state_count
  WHERE path_state_count.state_count <= $5::text::bigint
    AND path_states.current_concept_id = $2::text::uuid
  ORDER BY pg_catalog.cardinality(path_states.assertion_ids) ASC,
    path_states.assertion_ids ASC,
    path_states.concept_ids ASC
  LIMIT $6::text::bigint
), path_payload AS (
  SELECT COALESCE(
    pg_catalog.jsonb_agg(
      path_results.result
      ORDER BY path_results.hops ASC,
        path_results.assertion_ids ASC,
        path_results.concept_ids ASC
    ),
    '[]'::jsonb
  ) AS result
  FROM (
    SELECT
      ordered_path_states.hops,
      ordered_path_states.assertion_ids,
      ordered_path_states.concept_ids,
      pg_catalog.jsonb_build_object(
        'concepts', pg_catalog.to_jsonb(ordered_path_states.concept_ids),
        'assertions', COALESCE(
          (
            SELECT pg_catalog.jsonb_agg(
              pg_catalog.jsonb_build_object(
                'assertion', pg_catalog.jsonb_build_object(
                  'id', assertions.id,
                  'revision', assertions.revision,
                  'subject_concept_id', assertions.subject_concept_id,
                  'relation_type', assertions.relation_code,
                  'object_concept_id', assertions.object_concept_id,
                  'evidence', COALESCE(
                    (
                      SELECT pg_catalog.jsonb_agg(
                        CASE source_references.source_kind COLLATE "C"
                          WHEN 'memory_version' THEN pg_catalog.jsonb_build_object(
                            'kind', 'memory_version',
                            'memory_id', source_references.external_id,
                            'version', source_references.external_version
                          )
                          ELSE pg_catalog.jsonb_build_object(
                            'kind', 'session_record',
                            'session_record_id', source_references.external_id
                          )
                        END
                        ORDER BY source_references.source_kind COLLATE "C" ASC,
                          source_references.external_id COLLATE "C" ASC,
                          source_references.external_version ASC NULLS FIRST
                      )
                      FROM knowledge_graph.graph_assertion_evidence AS evidence
                      JOIN knowledge_graph.graph_source_references AS source_references
                        ON source_references.source_ref_id = evidence.source_ref_id
                      WHERE evidence.assertion_id = assertions.id
                    ),
                    '[]'::jsonb
                  )
                ),
                'orientation', edge_orientations.orientation
              )
              ORDER BY path_edges.ordinality ASC
            )
            FROM pg_catalog.unnest(ordered_path_states.assertion_ids)
              WITH ORDINALITY AS path_edges(assertion_id, ordinality)
            JOIN pg_catalog.unnest(ordered_path_states.orientations)
              WITH ORDINALITY AS edge_orientations(orientation, ordinality)
              ON edge_orientations.ordinality = path_edges.ordinality
            JOIN knowledge_graph.graph_assertions AS assertions
              ON assertions.id = path_edges.assertion_id
          ),
          '[]'::jsonb
        )
      ) AS result
    FROM ordered_path_states
  ) AS path_results
), complete_payload AS (
  SELECT pg_catalog.jsonb_build_object(
    'outcome', 'complete',
    'result', path_payload.result
  ) AS payload
  FROM path_payload
), bounded_payload AS (
  SELECT CASE
    WHEN pg_catalog.octet_length(complete_payload.payload::text) > 4194304
      THEN pg_catalog.jsonb_build_object('outcome', 'result_too_large')
    ELSE complete_payload.payload
  END AS payload
  FROM complete_payload
)
SELECT CASE
  WHEN path_state_count.state_count > $5::text::bigint
    THEN pg_catalog.jsonb_build_object('outcome', 'work_exhausted')
  ELSE (SELECT payload FROM bounded_payload)
END AS payload
FROM path_state_count
"#;

type VerificationResult<T = ()> = Result<T, VerificationError>;

#[derive(Clone, Copy, Debug)]
struct VerificationError;

impl fmt::Display for VerificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("knowledge graph database protocol verification failed")
    }
}

impl Error for VerificationError {}

#[tokio::main]
async fn main() -> VerificationResult {
    run_protocol_verification().await
}

async fn run_protocol_verification() -> VerificationResult {
    verify_mutations_and_direct_reads().await?;
    verify_bounded_queries().await?;
    verify_malformed_responses_are_private().await?;
    verify_bounded_query_failures().await?;
    verify_invocation_timeout_is_applied_without_retry().await?;
    verify_bounded_query_invocation_timeout_without_retry().await?;
    verify_protocol_rejections_are_strict()
}

#[derive(Clone, Copy)]
enum CallKind {
    Execute,
    Query,
}

impl CallKind {
    const fn function_id(self) -> &'static str {
        match self {
            Self::Execute => DATABASE_EXECUTE_FUNCTION_ID,
            Self::Query => DATABASE_QUERY_FUNCTION_ID,
        }
    }
}

enum ParameterExpectation {
    Exact(Value),
    UuidV7,
}

impl ParameterExpectation {
    fn matches(&self, value: &Value) -> bool {
        match self {
            Self::Exact(expected) => value == expected,
            Self::UuidV7 => value
                .as_str()
                .and_then(|text| Uuid::parse_str(text).ok())
                .is_some_and(|uuid| {
                    uuid.get_version_num() == 7 && uuid.get_variant() == Variant::RFC4122
                }),
        }
    }
}

enum PeerReply {
    Result(Value),
    RemoteFailure,
    RemoteError {
        code: String,
        message: String,
        stacktrace: Option<String>,
    },
    NoResponse,
}

type ReplyFactory = Box<dyn Fn(&Value) -> PeerReply + Send + Sync>;

struct ExpectedInvocation {
    kind: CallKind,
    sql: &'static str,
    parameters: Vec<ParameterExpectation>,
    reply: ReplyFactory,
}

impl ExpectedInvocation {
    fn execute_row(sql: &'static str, parameters: Vec<ParameterExpectation>, row: Value) -> Self {
        Self::execute_with(sql, parameters, move |_| row.clone())
    }

    fn execute_with<F>(sql: &'static str, parameters: Vec<ParameterExpectation>, row: F) -> Self
    where
        F: Fn(&Value) -> Value + Send + Sync + 'static,
    {
        Self::execute_reply(sql, parameters, move |request| {
            PeerReply::Result(execute_response(vec![row(request)]))
        })
    }

    fn execute_reply<F>(sql: &'static str, parameters: Vec<ParameterExpectation>, reply: F) -> Self
    where
        F: Fn(&Value) -> PeerReply + Send + Sync + 'static,
    {
        Self {
            kind: CallKind::Execute,
            sql,
            parameters,
            reply: Box::new(reply),
        }
    }

    fn query_result(
        sql: &'static str,
        parameters: Vec<ParameterExpectation>,
        result: Value,
    ) -> Self {
        Self::query_payload(
            sql,
            parameters,
            json!({"outcome": "complete", "result": result}),
        )
    }

    fn query_payload(
        sql: &'static str,
        parameters: Vec<ParameterExpectation>,
        payload: Value,
    ) -> Self {
        Self::query_reply(sql, parameters, move |_| {
            PeerReply::Result(query_response(payload.clone()))
        })
    }

    fn query_reply<F>(sql: &'static str, parameters: Vec<ParameterExpectation>, reply: F) -> Self
    where
        F: Fn(&Value) -> PeerReply + Send + Sync + 'static,
    {
        Self {
            kind: CallKind::Query,
            sql,
            parameters,
            reply: Box::new(reply),
        }
    }
}

enum ExpectedItem {
    Invocation(ExpectedInvocation),
    Finish { expected_calls: usize },
}

struct FakeSession {
    client: IIIClient,
    store: Arc<IiiKnowledgeGraphStore>,
    server: JoinHandle<VerificationResult>,
    expected: mpsc::UnboundedSender<ExpectedItem>,
    observed: mpsc::UnboundedReceiver<usize>,
    release_response: mpsc::UnboundedSender<()>,
    calls_sent: usize,
}

impl FakeSession {
    async fn start(timeouts: DatabaseTimeouts) -> VerificationResult<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .map_err(|_| VerificationError)?;
        let address = listener.local_addr().map_err(|_| VerificationError)?;
        require(address.ip().is_loopback() && address.port() != 0)?;

        let (expected_tx, expected_rx) = mpsc::unbounded_channel();
        let (observed_tx, observed_rx) = mpsc::unbounded_channel();
        let (release_tx, release_rx) = mpsc::unbounded_channel();
        let server = tokio::spawn(run_fake(listener, expected_rx, observed_tx, release_rx));
        let client = register_worker(
            &format!("ws://127.0.0.1:{}", address.port()),
            InitOptions {
                identity: WorkerIdentityMode::Explicit,
                ..InitOptions::default()
            },
        );

        if !matches!(
            timeout(
                OPERATION_TIMEOUT,
                client.wait_until_registered(OPERATION_TIMEOUT)
            )
            .await,
            Ok(Ok(()))
        ) {
            client.shutdown_async().await;
            server.abort();
            let _ = server.await;
            return verification_failure();
        }

        let store =
            match IiiKnowledgeGraphStore::new(client.clone(), DATABASE_TARGET.to_owned(), timeouts)
            {
                Ok(store) => Arc::new(store),
                Err(GraphConfigurationError::InvalidDatabaseTarget) => {
                    client.shutdown_async().await;
                    server.abort();
                    let _ = server.await;
                    return verification_failure();
                }
            };

        Ok(Self {
            client,
            store,
            server,
            expected: expected_tx,
            observed: observed_rx,
            release_response: release_tx,
            calls_sent: 0,
        })
    }

    async fn invoke<T, Fut>(
        &mut self,
        expected: ExpectedInvocation,
        operation: Fut,
    ) -> VerificationResult<GraphResult<T>>
    where
        Fut: Future<Output = GraphResult<T>>,
    {
        let call_index = self.calls_sent;
        self.expected
            .send(ExpectedItem::Invocation(expected))
            .map_err(|_| VerificationError)?;
        self.calls_sent += 1;

        let mut operation = Box::pin(operation);
        let observed = tokio::select! {
            biased;
            _ = &mut operation => return verification_failure(),
            result = timeout(OPERATION_TIMEOUT, self.observed.recv()) => result,
        };
        match observed {
            Ok(Some(observed)) if observed == call_index => {}
            _ => return verification_failure(),
        }
        verify_operation_pending_before_response(&mut operation, RESPONSE_HOLD_DELAY).await?;
        if self.release_response.send(()).is_err() {
            return verification_failure();
        }

        match timeout(OPERATION_TIMEOUT, &mut operation).await {
            Ok(result) => Ok(result),
            _ => verification_failure(),
        }
    }

    async fn invoke_without_response<T, Fut>(
        &mut self,
        expected: ExpectedInvocation,
        operation: Fut,
    ) -> VerificationResult<GraphResult<T>>
    where
        Fut: Future<Output = GraphResult<T>>,
    {
        let call_index = self.calls_sent;
        self.expected
            .send(ExpectedItem::Invocation(expected))
            .map_err(|_| VerificationError)?;
        self.calls_sent += 1;

        let mut operation = Box::pin(operation);
        let observed = tokio::select! {
            biased;
            _ = &mut operation => return verification_failure(),
            result = timeout(OPERATION_TIMEOUT, self.observed.recv()) => result,
        };
        match observed {
            Ok(Some(observed)) if observed == call_index => {}
            _ => return verification_failure(),
        }

        if timeout(Duration::from_millis(1_250), &mut operation)
            .await
            .is_ok()
        {
            return verification_failure();
        }

        match timeout(OPERATION_TIMEOUT, &mut operation).await {
            Ok(result) => Ok(result),
            _ => verification_failure(),
        }
    }

    async fn finish(self) -> VerificationResult {
        let Self {
            client,
            store,
            server,
            expected,
            observed: _,
            release_response: _,
            calls_sent,
        } = self;

        expected
            .send(ExpectedItem::Finish {
                expected_calls: calls_sent,
            })
            .map_err(|_| VerificationError)?;
        drop(expected);
        client.shutdown_async().await;
        drop(store);
        drop(client);

        match timeout(PEER_TIMEOUT, server).await {
            Ok(Ok(result)) => result,
            _ => verification_failure(),
        }
    }
}

async fn run_fake(
    listener: TcpListener,
    mut expected: mpsc::UnboundedReceiver<ExpectedItem>,
    observed: mpsc::UnboundedSender<usize>,
    mut release_response: mpsc::UnboundedReceiver<()>,
) -> VerificationResult {
    let (stream, _) = timeout(OPERATION_TIMEOUT, listener.accept())
        .await
        .map_err(|_| VerificationError)?
        .map_err(|_| VerificationError)?;
    let mut socket = timeout(OPERATION_TIMEOUT, accept_async(stream))
        .await
        .map_err(|_| VerificationError)?
        .map_err(|_| VerificationError)?;

    let registration = next_protocol_message(&mut socket).await?;
    validate_worker_registration(&registration)?;
    send_protocol_message(
        &mut socket,
        json!({"type": "workerregistered", "worker_id": "graph-protocol-fake"}),
    )
    .await?;

    let mut expected_calls = 0;
    let mut received_calls = 0;
    loop {
        let item = timeout(OPERATION_TIMEOUT, expected.recv())
            .await
            .map_err(|_| VerificationError)?
            .ok_or(VerificationError)?;
        match item {
            ExpectedItem::Invocation(expected_call) => {
                expected_calls += 1;
                let message = next_protocol_message(&mut socket).await?;
                let invocation_id = validate_database_invocation(&message, &expected_call)?;
                let call_index = received_calls;
                received_calls += 1;
                observed.send(call_index).map_err(|_| VerificationError)?;

                match (expected_call.reply)(&message) {
                    PeerReply::NoResponse => {}
                    reply => {
                        timeout(OPERATION_TIMEOUT, release_response.recv())
                            .await
                            .map_err(|_| VerificationError)?
                            .ok_or(VerificationError)?;
                        send_reply(
                            &mut socket,
                            &invocation_id,
                            expected_call.kind.function_id(),
                            reply,
                        )
                        .await?;
                    }
                }
            }
            ExpectedItem::Finish {
                expected_calls: declared_count,
            } => {
                validate_call_count(declared_count, expected_calls)?;
                validate_call_count(expected_calls, received_calls)?;
                return wait_for_disconnect(&mut socket, expected_calls, received_calls).await;
            }
        }
    }
}

async fn next_protocol_message(
    socket: &mut WebSocketStream<TcpStream>,
) -> VerificationResult<Value> {
    loop {
        let frame = timeout(OPERATION_TIMEOUT, socket.next())
            .await
            .map_err(|_| VerificationError)?
            .ok_or(VerificationError)?
            .map_err(|_| VerificationError)?;
        match frame {
            Message::Ping(payload) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|_| VerificationError)?;
            }
            Message::Text(text) => {
                return serde_json::from_str(text.as_str()).map_err(|_| VerificationError);
            }
            _ => return verification_failure(),
        }
    }
}

async fn wait_for_disconnect(
    socket: &mut WebSocketStream<TcpStream>,
    expected_calls: usize,
    received_calls: usize,
) -> VerificationResult {
    validate_call_count(expected_calls, received_calls)?;
    loop {
        let next = timeout(OPERATION_TIMEOUT, socket.next())
            .await
            .map_err(|_| VerificationError)?;
        let Some(frame) = next else {
            return Ok(());
        };
        let Ok(frame) = frame else {
            return Ok(());
        };
        match frame {
            Message::Ping(payload) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|_| VerificationError)?;
            }
            Message::Pong(_) => {}
            Message::Close(_) => return Ok(()),
            Message::Text(text) => {
                return match serde_json::from_str(text.as_str()) {
                    Ok(message) => reject_unexpected_database_invocation(&message),
                    Err(_) => verification_failure(),
                };
            }
            _ => return verification_failure(),
        }
    }
}

async fn send_reply(
    socket: &mut WebSocketStream<TcpStream>,
    invocation_id: &str,
    function_id: &str,
    reply: PeerReply,
) -> VerificationResult {
    let response = match reply {
        PeerReply::Result(result) => json!({
            "type": "invocationresult",
            "invocation_id": invocation_id,
            "function_id": function_id,
            "result": result,
        }),
        PeerReply::RemoteFailure => json!({
            "type": "invocationresult",
            "invocation_id": invocation_id,
            "function_id": function_id,
            "error": {
                "code": BACKEND_CODE_SENTINEL,
                "message": format!("{BACKEND_SENTINEL} {PATH_SENTINEL} {SESSION_SOURCE_SENTINEL}"),
                "stacktrace": BACKEND_STACK_SENTINEL,
            },
        }),
        PeerReply::RemoteError {
            code,
            message,
            stacktrace,
        } => json!({
            "type": "invocationresult",
            "invocation_id": invocation_id,
            "function_id": function_id,
            "error": {
                "code": code,
                "message": message,
                "stacktrace": stacktrace,
            },
        }),
        PeerReply::NoResponse => return verification_failure(),
    };
    send_protocol_message(socket, response).await
}

async fn send_protocol_message(
    socket: &mut WebSocketStream<TcpStream>,
    message: Value,
) -> VerificationResult {
    socket
        .send(Message::Text(message.to_string().into()))
        .await
        .map_err(|_| VerificationError)
}

fn validate_worker_registration(message: &Value) -> VerificationResult {
    let Some(object) = message.as_object() else {
        return verification_failure();
    };
    require(has_exact_keys(
        object,
        ["type", "invocation_id", "function_id", "data", "action"].as_slice(),
    ))?;
    require(message.get("type") == Some(&json!("invokefunction")))?;
    require(message.get("invocation_id") == Some(&Value::Null))?;
    require(message.get("function_id") == Some(&json!(WORKER_REGISTER_FUNCTION_ID)))?;
    require(message.get("data").is_some_and(Value::is_object))?;
    require(message.get("action") == Some(&json!({"type": "void"})))
}

fn validate_database_invocation(
    message: &Value,
    expected: &ExpectedInvocation,
) -> VerificationResult<String> {
    let Some(object) = message.as_object() else {
        return verification_failure();
    };
    require(has_exact_keys(
        object,
        ["type", "invocation_id", "function_id", "data"].as_slice(),
    ))?;
    require(message.get("type") == Some(&json!("invokefunction")))?;
    require(message.get("function_id") == Some(&json!(expected.kind.function_id())))?;

    let Some(invocation_id) = message.get("invocation_id").and_then(Value::as_str) else {
        return verification_failure();
    };
    require(Uuid::parse_str(invocation_id).is_ok())?;

    let Some(data) = message.get("data").and_then(Value::as_object) else {
        return verification_failure();
    };
    let expected_fields: &[&str] = match expected.kind {
        CallKind::Execute => &["db", "sql", "params"],
        CallKind::Query => &["db", "sql", "params", "timeout_ms"],
    };
    require(has_exact_keys(data, expected_fields))?;
    require(data.get("db") == Some(&json!(DATABASE_TARGET)))?;
    require(data.get("sql") == Some(&json!(expected.sql)))?;

    let Some(parameters) = data.get("params").and_then(Value::as_array) else {
        return verification_failure();
    };
    require(parameters.len() == expected.parameters.len())?;
    for (parameter, expectation) in parameters.iter().zip(&expected.parameters) {
        require(expectation.matches(parameter))?;
    }

    if matches!(expected.kind, CallKind::Query) {
        require(data.get("timeout_ms") == Some(&json!(QUERY_TIMEOUT_MILLIS)))?;
    }

    Ok(invocation_id.to_owned())
}

fn reject_unexpected_database_invocation(_message: &Value) -> VerificationResult {
    verification_failure()
}

fn validate_call_count(expected: usize, received: usize) -> VerificationResult {
    require(expected == received)
}

fn has_exact_keys(object: &Map<String, Value>, expected: &[&str]) -> bool {
    object.len() == expected.len() && expected.iter().all(|key| object.contains_key(*key))
}

fn execute_response(rows: Vec<Value>) -> Value {
    json!({
        "affected_rows": 1,
        "last_insert_id": null,
        "returned_rows": rows,
    })
}

fn query_response(payload: Value) -> Value {
    json!({
        "rows": [{"payload": payload}],
        "row_count": 1,
        "columns": [{"name": "payload", "type": "jsonb"}],
    })
}

fn exact_parameters(values: Vec<Value>) -> Vec<ParameterExpectation> {
    values
        .into_iter()
        .map(ParameterExpectation::Exact)
        .collect()
}

fn require(condition: bool) -> VerificationResult {
    if condition {
        Ok(())
    } else {
        verification_failure()
    }
}

fn verification_failure<T>() -> VerificationResult<T> {
    Err(VerificationError)
}

async fn verify_operation_pending_before_response<T, Fut>(
    operation: &mut std::pin::Pin<Box<Fut>>,
    hold: Duration,
) -> VerificationResult
where
    Fut: Future<Output = T>,
{
    tokio::select! {
        biased;
        _ = operation.as_mut() => verification_failure(),
        _ = tokio::time::sleep(hold) => Ok(()),
    }
}

#[derive(Clone, Copy)]
struct AliasFixture {
    display_text: &'static str,
    alias_key: &'static str,
    preferred: bool,
}

const CREATED_ALIASES: [AliasFixture; 2] = [
    AliasFixture {
        display_text: ALIAS_SENTINEL,
        alias_key: "private_alias_sentinel",
        preferred: true,
    },
    AliasFixture {
        display_text: "Secondary Alias",
        alias_key: "secondary alias",
        preferred: false,
    },
];

const EXISTING_ALIASES: [AliasFixture; 2] = [
    AliasFixture {
        display_text: "Existing Preferred",
        alias_key: "existing preferred",
        preferred: true,
    },
    AliasFixture {
        display_text: "Existing Secondary",
        alias_key: "existing secondary",
        preferred: false,
    },
];

const STORED_EXISTING_ALIASES: [AliasFixture; 2] = [
    AliasFixture {
        display_text: "EXISTING PREFERRED",
        alias_key: "existing preferred",
        preferred: true,
    },
    AliasFixture {
        display_text: "Existing\tSecondary",
        alias_key: "existing secondary",
        preferred: false,
    },
];

const CONFLICT_ALIASES: [AliasFixture; 2] = [
    AliasFixture {
        display_text: ALIAS_SENTINEL,
        alias_key: "private_alias_sentinel",
        preferred: true,
    },
    AliasFixture {
        display_text: "Conflict Alternative",
        alias_key: "conflict alternative",
        preferred: false,
    },
];

const REPLACEMENT_ALIASES: [AliasFixture; 2] = [
    AliasFixture {
        display_text: "Replacement Preferred",
        alias_key: "replacement preferred",
        preferred: true,
    },
    AliasFixture {
        display_text: "Replacement Secondary",
        alias_key: "replacement secondary",
        preferred: false,
    },
];

fn alias_input(aliases: &[AliasFixture]) -> Vec<Alias> {
    aliases
        .iter()
        .map(|alias| Alias {
            display_text: alias.display_text.to_owned(),
            preferred: alias.preferred,
        })
        .collect()
}

fn alias_json(aliases: &[AliasFixture]) -> Value {
    Value::Array(
        aliases
            .iter()
            .map(|alias| {
                json!({
                    "alias_key": alias.alias_key,
                    "display_text": alias.display_text,
                    "is_preferred": alias.preferred,
                })
            })
            .collect(),
    )
}

fn concept_wire(id: Value, revision: i64, aliases: &[AliasFixture]) -> Value {
    json!({
        "id": id,
        "revision": revision,
        "aliases": aliases
            .iter()
            .map(|alias| json!({"display_text": alias.display_text, "preferred": alias.preferred}))
            .collect::<Vec<_>>(),
    })
}

fn concept_record(id: ConceptId, revision: i64, aliases: &[AliasFixture]) -> Concept {
    Concept {
        id,
        revision: revision_value(revision),
        aliases: alias_input(aliases),
    }
}

fn concept_uuid(id: ConceptId) -> Value {
    json!(id.as_uuid().to_string())
}

fn concept_id(value: u128) -> ConceptId {
    ConceptId::try_from(Uuid::from_u128(UUID_V7_BASE + value))
        .expect("fixture concept IDs must be UUIDv7")
}

fn assertion_id(value: u128) -> AssertionId {
    AssertionId::try_from(Uuid::from_u128(UUID_V7_BASE + value))
        .expect("fixture assertion IDs must be UUIDv7")
}

fn revision_value(value: i64) -> Revision {
    Revision::try_from(value).expect("fixture revisions must be positive")
}

fn memory_source() -> SourceReferenceInput {
    SourceReferenceInput::MemoryVersion {
        memory_id: MEMORY_SOURCE_SENTINEL.to_owned(),
        version: 17,
    }
}

fn session_source() -> SourceReferenceInput {
    SourceReferenceInput::SessionRecord {
        session_record_id: SESSION_SOURCE_SENTINEL.to_owned(),
    }
}

fn source_reference(input: &SourceReferenceInput) -> SourceReference {
    SourceReference::try_from(input.clone()).expect("fixture source identity must be valid")
}

fn source_parameters(input: &SourceReferenceInput) -> Vec<Value> {
    match input {
        SourceReferenceInput::MemoryVersion { memory_id, version } => vec![
            json!("memory_version"),
            json!(memory_id),
            json!(version.to_string()),
        ],
        SourceReferenceInput::SessionRecord { session_record_id } => vec![
            json!("session_record"),
            json!(session_record_id),
            Value::Null,
        ],
    }
}

fn neighbor_parameters(query: &NeighborQuery) -> Vec<ParameterExpectation> {
    let mut relation_filter = query.relation_filter.clone();
    relation_filter.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    relation_filter.dedup();

    vec![
        ParameterExpectation::Exact(concept_uuid(query.concept_id)),
        ParameterExpectation::Exact(json!(query.direction.as_str())),
        ParameterExpectation::Exact(json!(
            relation_filter
                .iter()
                .map(|relation| relation.as_str())
                .collect::<Vec<_>>()
        )),
        ParameterExpectation::Exact(json!(query.limit.to_string())),
    ]
}

fn related_source_parameters(query: &RelatedSourceQuery) -> Vec<ParameterExpectation> {
    vec![
        ParameterExpectation::Exact(concept_uuid(query.concept_id)),
        ParameterExpectation::Exact(json!(query.limit.to_string())),
    ]
}

fn path_parameters(query: &PathQuery) -> Vec<ParameterExpectation> {
    vec![
        ParameterExpectation::Exact(concept_uuid(query.from)),
        ParameterExpectation::Exact(concept_uuid(query.to)),
        ParameterExpectation::Exact(json!(query.direction.as_str())),
        ParameterExpectation::Exact(json!(query.max_depth.to_string())),
        ParameterExpectation::Exact(json!(query.max_work.to_string())),
        ParameterExpectation::Exact(json!(query.limit.to_string())),
    ]
}

fn source_wire(input: &SourceReferenceInput) -> Value {
    match input {
        SourceReferenceInput::MemoryVersion { memory_id, version } => json!({
            "kind": "memory_version",
            "memory_id": memory_id,
            "version": version,
        }),
        SourceReferenceInput::SessionRecord { session_record_id } => json!({
            "kind": "session_record",
            "session_record_id": session_record_id,
        }),
    }
}

fn related_source_wire(input: &SourceReferenceInput, role: RelatedSourceRole) -> Value {
    json!({
        "source": source_wire(input),
        "role": role.as_str(),
    })
}

fn related_source_record(
    input: &SourceReferenceInput,
    role: RelatedSourceRole,
) -> RelatedSourceResult {
    RelatedSourceResult {
        source: source_reference(input),
        role,
    }
}

fn source_identity_wire(input: &SourceReferenceInput) -> Value {
    match input {
        SourceReferenceInput::MemoryVersion { memory_id, version } => json!({
            "source_kind": "memory_version",
            "external_id": memory_id,
            "external_version": version,
        }),
        SourceReferenceInput::SessionRecord { session_record_id } => json!({
            "source_kind": "session_record",
            "external_id": session_record_id,
            "external_version": null,
        }),
    }
}

fn source_mutation_record(source_ref_id: i64, source: &SourceReferenceInput) -> Value {
    match source {
        SourceReferenceInput::MemoryVersion { memory_id, version } => json!({
            "source_ref_id": source_ref_id,
            "source_kind": "memory_version",
            "external_id": memory_id,
            "external_version": version,
        }),
        SourceReferenceInput::SessionRecord { session_record_id } => json!({
            "source_ref_id": source_ref_id,
            "source_kind": "session_record",
            "external_id": session_record_id,
            "external_version": null,
        }),
    }
}

fn mention_mutation_record(
    concept: ConceptId,
    source_ref_id: i64,
    source: &SourceReferenceInput,
) -> Value {
    json!({
        "concept_id": concept.as_uuid().to_string(),
        "source_ref_id": source_ref_id,
        "source": source_identity_wire(source),
    })
}

fn assertion_wire(
    id: Value,
    revision: i64,
    subject: ConceptId,
    relation: RelationType,
    object: ConceptId,
    evidence: &[SourceReferenceInput],
) -> Value {
    json!({
        "id": id,
        "revision": revision,
        "subject_concept_id": subject.as_uuid().to_string(),
        "relation_type": relation.as_str(),
        "object_concept_id": object.as_uuid().to_string(),
        "evidence": evidence.iter().map(source_wire).collect::<Vec<_>>(),
    })
}

fn assertion_record(
    id: AssertionId,
    revision: i64,
    subject: ConceptId,
    relation: RelationType,
    object: ConceptId,
    evidence: &[SourceReferenceInput],
) -> Assertion {
    Assertion {
        id,
        revision: revision_value(revision),
        subject_concept_id: subject,
        relation_type: relation,
        object_concept_id: object,
        evidence: evidence.iter().map(source_reference).collect(),
    }
}

fn evidence_wire(assertion: AssertionId, source: &SourceReferenceInput) -> Value {
    json!({
        "assertion_id": assertion.as_uuid().to_string(),
        "source": source_wire(source),
    })
}

fn protocol_timeouts() -> DatabaseTimeouts {
    DatabaseTimeouts::try_new(QUERY_TIMEOUT, INVOCATION_TIMEOUT)
        .expect("fixture timeouts must be valid")
}

fn expected_conflict(
    operation: OperationCode,
    field: ConflictField,
    reason: ConflictReason,
    identity: RecordIdentity,
    current_revision: i64,
) -> GraphError {
    GraphError::Conflict {
        operation,
        field,
        reason,
        identity,
        current_revision: Some(revision_value(current_revision)),
    }
}

fn expected_not_found(
    operation: OperationCode,
    record_kind: RecordKind,
    identity: Option<RecordIdentity>,
) -> GraphError {
    GraphError::NotFound {
        operation,
        record_kind,
        identity,
        reason: NotFoundReason::Missing,
    }
}

fn expected_database_failure(operation: OperationCode) -> GraphError {
    GraphError::DatabaseFailure {
        operation,
        reason: BackendFailureReason::DatabaseFailure,
    }
}

fn expected_result_bound(operation: OperationCode) -> GraphError {
    GraphError::ResultBoundExceeded { operation }
}

fn expected_traversal_bound(
    operation: OperationCode,
    bound: TraversalBound,
    reason: TraversalReason,
) -> GraphError {
    GraphError::TraversalBoundExceeded {
        operation,
        bound,
        reason,
    }
}

fn expected_query_timeout(operation: OperationCode) -> GraphError {
    expected_traversal_bound(
        operation,
        TraversalBound::QueryTimeout,
        TraversalReason::QueryTimeout,
    )
}

fn worker_error_body(code: &str) -> String {
    let protected_values = protected_sentinels().join(" ");
    format!(r#"{{"code":"{code}","message":"{protected_values} {SQL_SENTINEL}"}}"#)
}

fn exact_query_timeout_reply() -> PeerReply {
    PeerReply::RemoteError {
        code: "invocation_failed".to_owned(),
        message: format!("handler error: {}", worker_error_body("QUERY_TIMEOUT")),
        stacktrace: Some(BACKEND_STACK_SENTINEL.to_owned()),
    }
}

fn expected_invalid_response(operation: OperationCode, reason: BackendFailureReason) -> GraphError {
    GraphError::InvalidResponse { operation, reason }
}

fn protected_sentinels() -> [&'static str; 10] {
    [
        DATABASE_TARGET,
        ALIAS_SENTINEL,
        OTHER_ALIAS_SENTINEL,
        MEMORY_SOURCE_SENTINEL,
        SESSION_SOURCE_SENTINEL,
        EVIDENCE_LIMIT_SOURCE_SENTINEL,
        PATH_SENTINEL,
        BACKEND_SENTINEL,
        BACKEND_CODE_SENTINEL,
        BACKEND_STACK_SENTINEL,
    ]
}

fn assert_error_is_private(error: &GraphError) -> VerificationResult {
    let display = error.to_string();
    let debug = format!("{error:?}");
    for sentinel in protected_sentinels() {
        require(!display.contains(sentinel))?;
        require(!debug.contains(sentinel))?;
    }
    require(!display.contains(SQL_SENTINEL))?;
    require(!debug.contains(SQL_SENTINEL))
}

macro_rules! verify_call {
    ($session:expr, $expected:expr, $method:ident($($argument:expr),* $(,)?), $expected_result:expr) => {{
        let expected_invocation = $expected;
        let expected_result = $expected_result;
        let store = Arc::clone(&$session.store);
        let operation = store.$method($($argument),*);
        let actual = $session.invoke(expected_invocation, operation).await?;
        if let Err(error) = &actual {
            assert_error_is_private(error)?;
        }
        require(actual == expected_result)?;
    }};
}

async fn verify_mutations_and_direct_reads() -> VerificationResult {
    let mut session = FakeSession::start(protocol_timeouts()).await?;
    verify_mutations(&mut session).await?;
    verify_direct_reads(&mut session).await?;
    session.finish().await
}

async fn verify_mutations(session: &mut FakeSession) -> VerificationResult {
    let created_aliases = CREATED_ALIASES;
    let store = Arc::clone(&session.store);
    let created = session
        .invoke(
            ExpectedInvocation::execute_with(
                CREATE_CONCEPT_SQL,
                vec![
                    ParameterExpectation::UuidV7,
                    ParameterExpectation::Exact(alias_json(&created_aliases)),
                ],
                move |request| {
                    json!({
                        "outcome": "created",
                        "record": concept_wire(request_parameter(request, 0), 1, &created_aliases),
                    })
                },
            ),
            store.create_concept(CreateConcept {
                aliases: alias_input(&CREATED_ALIASES),
            }),
        )
        .await?
        .map_err(|_| VerificationError)?;
    require(created.revision == revision_value(1))?;
    require(created.aliases == alias_input(&CREATED_ALIASES))?;
    require(created.id.as_uuid().get_version_num() == 7)?;
    require(created.id.as_uuid().get_variant() == Variant::RFC4122)?;

    let existing_aliases = EXISTING_ALIASES;
    verify_call!(
        session,
        ExpectedInvocation::execute_with(
            CREATE_CONCEPT_SQL,
            vec![
                ParameterExpectation::UuidV7,
                ParameterExpectation::Exact(alias_json(&existing_aliases)),
            ],
            move |_| {
                json!({
                    "outcome": "existing",
                    "record": concept_wire(
                        concept_uuid(concept_id(20)),
                        7,
                        &STORED_EXISTING_ALIASES,
                    ),
                })
            },
        ),
        create_concept(CreateConcept {
            aliases: alias_input(&EXISTING_ALIASES),
        }),
        Ok(concept_record(concept_id(20), 7, &STORED_EXISTING_ALIASES,))
    );

    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            CREATE_CONCEPT_SQL,
            vec![
                ParameterExpectation::UuidV7,
                ParameterExpectation::Exact(alias_json(&CONFLICT_ALIASES)),
            ],
            json!({
                "outcome": "conflict",
                "field": "alias_set",
                "reason": "alias_owned",
                "identity": {"kind": "concept", "id": concept_id(20).as_uuid().to_string()},
                "current_revision": 7,
            }),
        ),
        create_concept(CreateConcept {
            aliases: alias_input(&CONFLICT_ALIASES),
        }),
        Err(expected_conflict(
            OperationCode::CreateConcept,
            ConflictField::AliasSet,
            ConflictReason::AliasOwned,
            RecordIdentity::Concept(concept_id(20)),
            7,
        ))
    );

    let concept = concept_id(10);
    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            REPLACE_CONCEPT_ALIASES_SQL,
            exact_parameters(vec![
                concept_uuid(concept),
                json!("5"),
                alias_json(&REPLACEMENT_ALIASES),
            ]),
            json!({
                "outcome": "updated",
                "record": concept_wire(concept_uuid(concept), 6, &REPLACEMENT_ALIASES),
            }),
        ),
        replace_concept_aliases(ReplaceConceptAliases {
            concept_id: concept,
            expected_revision: revision_value(5),
            aliases: alias_input(&REPLACEMENT_ALIASES),
        }),
        Ok(concept_record(concept, 6, &REPLACEMENT_ALIASES))
    );

    let replacement_input = ReplaceConceptAliases {
        concept_id: concept,
        expected_revision: revision_value(6),
        aliases: alias_input(&CONFLICT_ALIASES),
    };
    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            REPLACE_CONCEPT_ALIASES_SQL,
            exact_parameters(vec![
                concept_uuid(concept),
                json!("6"),
                alias_json(&CONFLICT_ALIASES),
            ]),
            json!({
                "outcome": "conflict",
                "field": "alias_set",
                "reason": "alias_set_mismatch",
                "identity": {"kind": "concept", "id": concept_id(21).as_uuid().to_string()},
                "current_revision": 9,
            }),
        ),
        replace_concept_aliases(replacement_input),
        Err(expected_conflict(
            OperationCode::ReplaceConceptAliases,
            ConflictField::AliasSet,
            ConflictReason::AliasSetMismatch,
            RecordIdentity::Concept(concept_id(21)),
            9,
        ))
    );

    let stale_revision = revision_value(4);
    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            REPLACE_CONCEPT_ALIASES_SQL,
            exact_parameters(vec![
                concept_uuid(concept),
                json!(stale_revision.get().to_string()),
                alias_json(&REPLACEMENT_ALIASES),
            ]),
            json!({
                "outcome": "stale",
                "identity": {"kind": "concept", "id": concept.as_uuid().to_string()},
                "current_revision": 6,
            }),
        ),
        replace_concept_aliases(ReplaceConceptAliases {
            concept_id: concept,
            expected_revision: stale_revision,
            aliases: alias_input(&REPLACEMENT_ALIASES),
        }),
        Err(expected_conflict(
            OperationCode::ReplaceConceptAliases,
            ConflictField::Revision,
            ConflictReason::StaleRevision,
            RecordIdentity::Concept(concept),
            6,
        ))
    );

    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            DELETE_CONCEPT_SQL,
            exact_parameters(vec![concept_uuid(concept), json!("6")]),
            json!({"outcome": "deleted"}),
        ),
        delete_concept(DeleteConcept {
            concept_id: concept,
            expected_revision: revision_value(6),
        }),
        Ok(())
    );

    let referenced_concept = concept_id(11);
    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            DELETE_CONCEPT_SQL,
            exact_parameters(vec![concept_uuid(referenced_concept), json!("1")]),
            json!({
                "outcome": "referenced",
                "reference": {"kind": "concept", "id": referenced_concept.as_uuid().to_string()},
            }),
        ),
        delete_concept(DeleteConcept {
            concept_id: referenced_concept,
            expected_revision: revision_value(1),
        }),
        Err(GraphError::Referenced {
            operation: OperationCode::DeleteConcept,
            record: ReferencedRecord::Concept(referenced_concept),
            reason: ReferenceReason::InUse,
        })
    );

    let memory = memory_source();
    let session_source_input = session_source();
    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            REGISTER_SOURCE_SQL,
            exact_parameters(source_parameters(&memory)),
            json!({
                "outcome": "created",
                "record": source_mutation_record(701, &memory),
            }),
        ),
        register_source(memory.clone()),
        Ok(source_reference(&memory))
    );

    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            REGISTER_SOURCE_SQL,
            exact_parameters(source_parameters(&session_source_input)),
            json!({
                "outcome": "created",
                "record": source_mutation_record(702, &session_source_input),
            }),
        ),
        register_source(session_source_input.clone()),
        Ok(source_reference(&session_source_input))
    );

    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            REGISTER_SOURCE_SQL,
            exact_parameters(source_parameters(&memory)),
            json!({
                "outcome": "existing",
                "record": source_mutation_record(701, &memory),
            }),
        ),
        register_source(memory.clone()),
        Ok(source_reference(&memory))
    );

    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            DELETE_SOURCE_SQL,
            exact_parameters(source_parameters(&session_source_input)),
            json!({"outcome": "deleted"}),
        ),
        delete_source(DeleteSource {
            source: session_source_input.clone(),
        }),
        Ok(())
    );

    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            DELETE_SOURCE_SQL,
            exact_parameters(source_parameters(&memory)),
            json!({"outcome": "missing", "record_kind": "source_reference"}),
        ),
        delete_source(DeleteSource {
            source: memory.clone(),
        }),
        Err(expected_not_found(
            OperationCode::DeleteSource,
            RecordKind::SourceReference,
            None,
        ))
    );

    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            DELETE_SOURCE_SQL,
            exact_parameters(source_parameters(&memory)),
            json!({
                "outcome": "referenced",
                "reference_kind": "source_reference",
            }),
        ),
        delete_source(DeleteSource {
            source: memory.clone(),
        }),
        Err(GraphError::Referenced {
            operation: OperationCode::DeleteSource,
            record: ReferencedRecord::SourceReference,
            reason: ReferenceReason::InUse,
        })
    );

    let mentioned_concept = concept_id(10);
    let mut mention_params = vec![concept_uuid(mentioned_concept)];
    mention_params.extend(source_parameters(&memory));
    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            CREATE_MENTION_SQL,
            exact_parameters(mention_params),
            json!({
                "outcome": "created",
                "record": mention_mutation_record(mentioned_concept, 801, &memory),
            }),
        ),
        create_mention(ConceptMentionInput {
            concept_id: mentioned_concept,
            source: memory.clone(),
        }),
        Ok(ConceptMention {
            concept_id: mentioned_concept,
            source: source_reference(&memory),
        })
    );

    let mut existing_mention_params = vec![concept_uuid(mentioned_concept)];
    existing_mention_params.extend(source_parameters(&session_source_input));
    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            CREATE_MENTION_SQL,
            exact_parameters(existing_mention_params),
            json!({
                "outcome": "existing",
                "record": mention_mutation_record(mentioned_concept, 802, &session_source_input),
            }),
        ),
        create_mention(ConceptMentionInput {
            concept_id: mentioned_concept,
            source: session_source_input.clone(),
        }),
        Ok(ConceptMention {
            concept_id: mentioned_concept,
            source: source_reference(&session_source_input),
        })
    );

    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            CREATE_MENTION_SQL,
            exact_parameters({
                let mut parameters = vec![concept_uuid(mentioned_concept)];
                parameters.extend(source_parameters(&memory));
                parameters
            }),
            json!({
                "outcome": "missing",
                "record_kind": "concept",
                "identity": {"kind": "concept", "id": mentioned_concept.as_uuid().to_string()},
            }),
        ),
        create_mention(ConceptMentionInput {
            concept_id: mentioned_concept,
            source: memory.clone(),
        }),
        Err(expected_not_found(
            OperationCode::CreateMention,
            RecordKind::Concept,
            Some(RecordIdentity::Concept(mentioned_concept)),
        ))
    );

    let mut delete_mention_params = vec![concept_uuid(mentioned_concept)];
    delete_mention_params.extend(source_parameters(&memory));
    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            DELETE_MENTION_SQL,
            exact_parameters(delete_mention_params),
            json!({"outcome": "deleted"}),
        ),
        delete_mention(ConceptMentionInput {
            concept_id: mentioned_concept,
            source: memory.clone(),
        }),
        Ok(())
    );

    let mut missing_mention_params = vec![concept_uuid(mentioned_concept)];
    missing_mention_params.extend(source_parameters(&session_source_input));
    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            DELETE_MENTION_SQL,
            exact_parameters(missing_mention_params),
            json!({"outcome": "missing", "record_kind": "concept_mention"}),
        ),
        delete_mention(ConceptMentionInput {
            concept_id: mentioned_concept,
            source: session_source_input.clone(),
        }),
        Err(expected_not_found(
            OperationCode::DeleteMention,
            RecordKind::ConceptMention,
            None,
        ))
    );

    verify_assertion_mutations(session, &memory, &session_source_input).await
}

async fn verify_assertion_mutations(
    session: &mut FakeSession,
    memory: &SourceReferenceInput,
    session_source_input: &SourceReferenceInput,
) -> VerificationResult {
    let subject = concept_id(10);
    let object = concept_id(11);
    let create_evidence = vec![session_source_input.clone()];
    let create_evidence_wire = create_evidence.clone();
    let store = Arc::clone(&session.store);
    let created = session
        .invoke(
            ExpectedInvocation::execute_with(
                CREATE_ASSERTION_SQL,
                vec![
                    ParameterExpectation::UuidV7,
                    ParameterExpectation::Exact(concept_uuid(subject)),
                    ParameterExpectation::Exact(json!(RelationType::PartOf.as_str())),
                    ParameterExpectation::Exact(concept_uuid(object)),
                    ParameterExpectation::Exact(json!([source_wire(session_source_input)])),
                ],
                move |request| {
                    json!({
                        "outcome": "created",
                        "record": assertion_wire(
                            request_parameter(request, 0),
                            1,
                            subject,
                            RelationType::PartOf,
                            object,
                            &create_evidence_wire,
                        ),
                    })
                },
            ),
            store.create_assertion(CreateAssertion {
                subject_concept_id: subject,
                relation_type: RelationType::PartOf,
                object_concept_id: object,
                supporting_sources: create_evidence,
            }),
        )
        .await?
        .map_err(|_| VerificationError)?;
    require(created.id.as_uuid().get_version_num() == 7)?;
    require(created.id.as_uuid().get_variant() == Variant::RFC4122)?;
    require(
        created
            == assertion_record(
                created.id,
                1,
                subject,
                RelationType::PartOf,
                object,
                std::slice::from_ref(session_source_input),
            ),
    )?;

    let low = concept_id(30);
    let high = concept_id(31);
    let symmetric_evidence = vec![session_source_input.clone()];
    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            CREATE_ASSERTION_SQL,
            vec![
                ParameterExpectation::UuidV7,
                ParameterExpectation::Exact(concept_uuid(low)),
                ParameterExpectation::Exact(json!(RelationType::RelatedTo.as_str())),
                ParameterExpectation::Exact(concept_uuid(high)),
                ParameterExpectation::Exact(json!([source_wire(session_source_input)])),
            ],
            json!({
                "outcome": "existing",
                "record": assertion_wire(
                    json!(assertion_id(40).as_uuid().to_string()),
                    3,
                    low,
                    RelationType::RelatedTo,
                    high,
                    &symmetric_evidence,
                ),
            }),
        ),
        create_assertion(CreateAssertion {
            subject_concept_id: high,
            relation_type: RelationType::RelatedTo,
            object_concept_id: low,
            supporting_sources: symmetric_evidence.clone(),
        }),
        Ok(assertion_record(
            assertion_id(40),
            3,
            low,
            RelationType::RelatedTo,
            high,
            &symmetric_evidence,
        ))
    );

    let update_id = assertion_id(44);
    let retained_evidence = vec![memory.clone(), session_source_input.clone()];
    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            UPDATE_ASSERTION_SQL,
            exact_parameters(vec![
                json!(update_id.as_uuid().to_string()),
                json!("7"),
                concept_uuid(subject),
                json!(RelationType::DependsOn.as_str()),
                concept_uuid(object),
            ]),
            json!({
                "outcome": "updated",
                "record": assertion_wire(
                    json!(update_id.as_uuid().to_string()),
                    8,
                    subject,
                    RelationType::DependsOn,
                    object,
                    &retained_evidence,
                ),
            }),
        ),
        update_assertion(UpdateAssertion {
            assertion_id: update_id,
            expected_revision: revision_value(7),
            subject_concept_id: subject,
            relation_type: RelationType::DependsOn,
            object_concept_id: object,
        }),
        Ok(assertion_record(
            update_id,
            8,
            subject,
            RelationType::DependsOn,
            object,
            &retained_evidence,
        ))
    );

    let duplicate_update_id = assertion_id(45);
    let duplicate_identity = assertion_id(46);
    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            UPDATE_ASSERTION_SQL,
            exact_parameters(vec![
                json!(duplicate_update_id.as_uuid().to_string()),
                json!("4"),
                concept_uuid(subject),
                json!(RelationType::Uses.as_str()),
                concept_uuid(object),
            ]),
            json!({
                "outcome": "conflict",
                "field": "semantic_assertion",
                "reason": "duplicate_semantic_assertion",
                "identity": {"kind": "assertion", "id": duplicate_identity.as_uuid().to_string()},
                "current_revision": 6,
            }),
        ),
        update_assertion(UpdateAssertion {
            assertion_id: duplicate_update_id,
            expected_revision: revision_value(4),
            subject_concept_id: subject,
            relation_type: RelationType::Uses,
            object_concept_id: object,
        }),
        Err(expected_conflict(
            OperationCode::UpdateAssertion,
            ConflictField::SemanticAssertion,
            ConflictReason::DuplicateSemanticAssertion,
            RecordIdentity::Assertion(duplicate_identity),
            6,
        ))
    );

    let stale_update_id = assertion_id(47);
    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            UPDATE_ASSERTION_SQL,
            exact_parameters(vec![
                json!(stale_update_id.as_uuid().to_string()),
                json!("3"),
                concept_uuid(subject),
                json!(RelationType::Causes.as_str()),
                concept_uuid(object),
            ]),
            json!({
                "outcome": "stale",
                "identity": {"kind": "assertion", "id": stale_update_id.as_uuid().to_string()},
                "current_revision": 8,
            }),
        ),
        update_assertion(UpdateAssertion {
            assertion_id: stale_update_id,
            expected_revision: revision_value(3),
            subject_concept_id: subject,
            relation_type: RelationType::Causes,
            object_concept_id: object,
        }),
        Err(expected_conflict(
            OperationCode::UpdateAssertion,
            ConflictField::Revision,
            ConflictReason::StaleRevision,
            RecordIdentity::Assertion(stale_update_id),
            8,
        ))
    );

    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            DELETE_ASSERTION_SQL,
            exact_parameters(vec![json!(update_id.as_uuid().to_string()), json!("8"),]),
            json!({"outcome": "deleted"}),
        ),
        delete_assertion(DeleteAssertion {
            assertion_id: update_id,
            expected_revision: revision_value(8),
        }),
        Ok(())
    );

    let missing_assertion_id = assertion_id(48);
    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            DELETE_ASSERTION_SQL,
            exact_parameters(vec![
                json!(missing_assertion_id.as_uuid().to_string()),
                json!("1"),
            ]),
            json!({
                "outcome": "missing",
                "record_kind": "assertion",
                "identity": {"kind": "assertion", "id": missing_assertion_id.as_uuid().to_string()},
            }),
        ),
        delete_assertion(DeleteAssertion {
            assertion_id: missing_assertion_id,
            expected_revision: revision_value(1),
        }),
        Err(expected_not_found(
            OperationCode::DeleteAssertion,
            RecordKind::Assertion,
            Some(RecordIdentity::Assertion(missing_assertion_id)),
        ))
    );

    let evidence_assertion = assertion_id(50);
    let evidence_limit_source = SourceReferenceInput::MemoryVersion {
        memory_id: EVIDENCE_LIMIT_SOURCE_SENTINEL.to_owned(),
        version: 23,
    };
    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            ADD_ASSERTION_EVIDENCE_SQL,
            exact_parameters({
                let mut parameters = vec![json!(evidence_assertion.as_uuid().to_string())];
                parameters.extend(source_parameters(memory));
                parameters
            }),
            json!({
                "outcome": "created",
                "record": evidence_wire(evidence_assertion, memory),
            }),
        ),
        add_evidence(AssertionEvidenceInput {
            assertion_id: evidence_assertion,
            source: memory.clone(),
        }),
        Ok(AssertionEvidence {
            assertion_id: evidence_assertion,
            source: source_reference(memory),
        })
    );

    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            ADD_ASSERTION_EVIDENCE_SQL,
            exact_parameters({
                let mut parameters = vec![json!(evidence_assertion.as_uuid().to_string())];
                parameters.extend(source_parameters(session_source_input));
                parameters
            }),
            json!({
                "outcome": "existing",
                "record": evidence_wire(evidence_assertion, session_source_input),
            }),
        ),
        add_evidence(AssertionEvidenceInput {
            assertion_id: evidence_assertion,
            source: session_source_input.clone(),
        }),
        Ok(AssertionEvidence {
            assertion_id: evidence_assertion,
            source: source_reference(session_source_input),
        })
    );

    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            ADD_ASSERTION_EVIDENCE_SQL,
            exact_parameters({
                let mut parameters = vec![json!(evidence_assertion.as_uuid().to_string())];
                parameters.extend(source_parameters(&evidence_limit_source));
                parameters
            }),
            json!({"outcome": "evidence_limit"}),
        ),
        add_evidence(AssertionEvidenceInput {
            assertion_id: evidence_assertion,
            source: evidence_limit_source.clone(),
        }),
        Err(GraphError::LimitExceeded {
            operation: OperationCode::AddEvidence,
            resource: LimitResource::AssertionEvidenceCount,
            reason: LimitReason::Exceeded,
        })
    );

    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            REMOVE_ASSERTION_EVIDENCE_SQL,
            exact_parameters({
                let mut parameters = vec![json!(evidence_assertion.as_uuid().to_string())];
                parameters.extend(source_parameters(memory));
                parameters
            }),
            json!({"outcome": "deleted"}),
        ),
        remove_evidence(AssertionEvidenceInput {
            assertion_id: evidence_assertion,
            source: memory.clone(),
        }),
        Ok(())
    );

    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            REMOVE_ASSERTION_EVIDENCE_SQL,
            exact_parameters({
                let mut parameters = vec![json!(evidence_assertion.as_uuid().to_string())];
                parameters.extend(source_parameters(session_source_input));
                parameters
            }),
            json!({
                "outcome": "would_orphan_evidence",
                "assertion_id": evidence_assertion.as_uuid().to_string(),
                "current_revision": 12,
            }),
        ),
        remove_evidence(AssertionEvidenceInput {
            assertion_id: evidence_assertion,
            source: session_source_input.clone(),
        }),
        Err(GraphError::WouldOrphanAssertion {
            operation: OperationCode::RemoveEvidence,
            assertion_id: evidence_assertion,
            revision: revision_value(12),
        })
    );

    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            REMOVE_ASSERTION_EVIDENCE_SQL,
            exact_parameters({
                let mut parameters = vec![json!(evidence_assertion.as_uuid().to_string())];
                parameters.extend(source_parameters(session_source_input));
                parameters
            }),
            json!({
                "outcome": "missing",
                "record_kind": "assertion_evidence",
            }),
        ),
        remove_evidence(AssertionEvidenceInput {
            assertion_id: evidence_assertion,
            source: session_source_input.clone(),
        }),
        Err(expected_not_found(
            OperationCode::RemoveEvidence,
            RecordKind::AssertionEvidence,
            None,
        ))
    );

    Ok(())
}

async fn verify_direct_reads(session: &mut FakeSession) -> VerificationResult {
    let concept = concept_id(10);
    let complete_concept = concept_record(concept, 8, &CREATED_ALIASES);
    verify_call!(
        session,
        ExpectedInvocation::query_result(
            GET_CONCEPT_SQL,
            exact_parameters(vec![concept_uuid(concept)]),
            concept_wire(concept_uuid(concept), 8, &CREATED_ALIASES),
        ),
        get_concept(ConceptIdInput {
            id: concept.as_uuid().to_string(),
        }),
        Ok(Some(complete_concept))
    );

    let absent_concept = concept_id(60);
    verify_call!(
        session,
        ExpectedInvocation::query_result(
            GET_CONCEPT_SQL,
            exact_parameters(vec![concept_uuid(absent_concept)]),
            Value::Null,
        ),
        get_concept(ConceptIdInput {
            id: absent_concept.as_uuid().to_string(),
        }),
        Ok(None)
    );

    verify_call!(
        session,
        ExpectedInvocation::query_result(
            RESOLVE_ALIAS_SQL,
            exact_parameters(vec![json!("private_alias_sentinel")]),
            concept_wire(concept_uuid(concept), 8, &CREATED_ALIASES),
        ),
        resolve_alias(AliasQuery {
            alias: ALIAS_SENTINEL.to_owned(),
        }),
        Ok(Some(concept_record(concept, 8, &CREATED_ALIASES)))
    );

    verify_call!(
        session,
        ExpectedInvocation::query_result(
            RESOLVE_ALIAS_SQL,
            exact_parameters(vec![json!("private_other_alias_sentinel")]),
            Value::Null,
        ),
        resolve_alias(AliasQuery {
            alias: OTHER_ALIAS_SENTINEL.to_owned(),
        }),
        Ok(None)
    );

    let assertion = assertion_id(70);
    let subject = concept_id(10);
    let object = concept_id(11);
    let evidence = vec![memory_source(), session_source()];
    verify_call!(
        session,
        ExpectedInvocation::query_result(
            GET_ASSERTION_SQL,
            exact_parameters(vec![json!(assertion.as_uuid().to_string())]),
            assertion_wire(
                json!(assertion.as_uuid().to_string()),
                9,
                subject,
                RelationType::Uses,
                object,
                &evidence,
            ),
        ),
        get_assertion(AssertionIdInput {
            id: assertion.as_uuid().to_string(),
        }),
        Ok(Some(assertion_record(
            assertion,
            9,
            subject,
            RelationType::Uses,
            object,
            &evidence,
        )))
    );

    let absent_assertion = assertion_id(71);
    verify_call!(
        session,
        ExpectedInvocation::query_result(
            GET_ASSERTION_SQL,
            exact_parameters(vec![json!(absent_assertion.as_uuid().to_string())]),
            Value::Null,
        ),
        get_assertion(AssertionIdInput {
            id: absent_assertion.as_uuid().to_string(),
        }),
        Ok(None)
    );

    let memory = memory_source();
    verify_call!(
        session,
        ExpectedInvocation::query_result(
            GET_SOURCE_SQL,
            exact_parameters(source_parameters(&memory)),
            source_wire(&memory),
        ),
        get_source(memory.clone()),
        Ok(Some(source_reference(&memory)))
    );

    let session_source_input = session_source();
    verify_call!(
        session,
        ExpectedInvocation::query_result(
            GET_SOURCE_SQL,
            exact_parameters(source_parameters(&session_source_input)),
            source_wire(&session_source_input),
        ),
        get_source(session_source_input.clone()),
        Ok(Some(source_reference(&session_source_input)))
    );

    let absent_source = SourceReferenceInput::SessionRecord {
        session_record_id: "ABSENT_SOURCE_KEY_SENTINEL".to_owned(),
    };
    verify_call!(
        session,
        ExpectedInvocation::query_result(
            GET_SOURCE_SQL,
            exact_parameters(source_parameters(&absent_source)),
            Value::Null,
        ),
        get_source(absent_source),
        Ok(None)
    );

    Ok(())
}

async fn verify_bounded_queries() -> VerificationResult {
    let mut session = FakeSession::start(protocol_timeouts()).await?;

    let queried_concept = concept_id(100);
    let outgoing_neighbor = concept_id(101);
    let symmetric_neighbor = concept_id(102);
    let incoming_neighbor = concept_id(103);
    let memory = memory_source();
    let session_source_input = session_source();
    let complete_evidence = vec![memory.clone(), session_source_input.clone()];

    let outgoing_assertion_id = assertion_id(201);
    let outgoing_assertion_wire = assertion_wire(
        json!(outgoing_assertion_id.as_uuid().to_string()),
        4,
        queried_concept,
        RelationType::PartOf,
        outgoing_neighbor,
        &complete_evidence,
    );
    let outgoing_assertion = assertion_record(
        outgoing_assertion_id,
        4,
        queried_concept,
        RelationType::PartOf,
        outgoing_neighbor,
        &complete_evidence,
    );

    let symmetric_assertion_id = assertion_id(202);
    let symmetric_evidence = vec![session_source_input.clone()];
    let symmetric_assertion_wire = assertion_wire(
        json!(symmetric_assertion_id.as_uuid().to_string()),
        5,
        symmetric_neighbor,
        RelationType::RelatedTo,
        queried_concept,
        &symmetric_evidence,
    );
    let symmetric_assertion = assertion_record(
        symmetric_assertion_id,
        5,
        symmetric_neighbor,
        RelationType::RelatedTo,
        queried_concept,
        &symmetric_evidence,
    );

    let incoming_assertion_id = assertion_id(203);
    let incoming_evidence = vec![memory.clone()];
    let incoming_assertion_wire = assertion_wire(
        json!(incoming_assertion_id.as_uuid().to_string()),
        6,
        incoming_neighbor,
        RelationType::Uses,
        queried_concept,
        &incoming_evidence,
    );
    let incoming_assertion = assertion_record(
        incoming_assertion_id,
        6,
        incoming_neighbor,
        RelationType::Uses,
        queried_concept,
        &incoming_evidence,
    );

    let expected_neighbors = vec![
        NeighborResult {
            neighbor: concept_record(outgoing_neighbor, 2, &CREATED_ALIASES),
            assertion: outgoing_assertion,
            orientation: EdgeOrientation::Outgoing,
        },
        NeighborResult {
            neighbor: concept_record(symmetric_neighbor, 3, &EXISTING_ALIASES),
            assertion: symmetric_assertion,
            orientation: EdgeOrientation::Symmetric,
        },
        NeighborResult {
            neighbor: concept_record(incoming_neighbor, 7, &STORED_EXISTING_ALIASES),
            assertion: incoming_assertion,
            orientation: EdgeOrientation::Incoming,
        },
    ];
    verify_call!(
        session,
        ExpectedInvocation::query_result(
            GET_NEIGHBORS_SQL,
            vec![
                ParameterExpectation::Exact(concept_uuid(queried_concept)),
                ParameterExpectation::Exact(json!("either")),
                ParameterExpectation::Exact(json!(["part_of", "related_to", "uses"])),
                ParameterExpectation::Exact(json!("12")),
            ],
            json!([
                {
                    "neighbor": concept_wire(concept_uuid(outgoing_neighbor), 2, &CREATED_ALIASES),
                    "assertion": outgoing_assertion_wire,
                    "orientation": "outgoing",
                },
                {
                    "neighbor": concept_wire(concept_uuid(symmetric_neighbor), 3, &EXISTING_ALIASES),
                    "assertion": symmetric_assertion_wire,
                    "orientation": "symmetric",
                },
                {
                    "neighbor": concept_wire(concept_uuid(incoming_neighbor), 7, &STORED_EXISTING_ALIASES),
                    "assertion": incoming_assertion_wire,
                    "orientation": "incoming",
                },
            ]),
        ),
        neighbors(NeighborQuery {
            concept_id: queried_concept,
            direction: DirectionMode::Either,
            relation_filter: vec![
                RelationType::Uses,
                RelationType::RelatedTo,
                RelationType::PartOf,
            ],
            limit: 12,
        }),
        Ok(expected_neighbors)
    );

    let related_memory = memory_source();
    let related_session = session_source();
    let expected_related_sources = vec![
        related_source_record(&related_memory, RelatedSourceRole::Mention),
        related_source_record(&related_session, RelatedSourceRole::Mention),
        related_source_record(&related_memory, RelatedSourceRole::AssertionEvidence),
        related_source_record(&related_session, RelatedSourceRole::AssertionEvidence),
    ];
    verify_call!(
        session,
        ExpectedInvocation::query_result(
            GET_RELATED_SOURCES_SQL,
            vec![
                ParameterExpectation::Exact(concept_uuid(queried_concept)),
                ParameterExpectation::Exact(json!("12")),
            ],
            json!([
                related_source_wire(&related_memory, RelatedSourceRole::Mention),
                related_source_wire(&related_session, RelatedSourceRole::Mention),
                related_source_wire(&related_memory, RelatedSourceRole::AssertionEvidence),
                related_source_wire(&related_session, RelatedSourceRole::AssertionEvidence),
            ]),
        ),
        related_sources(RelatedSourceQuery {
            concept_id: queried_concept,
            limit: 12,
        }),
        Ok(expected_related_sources)
    );

    let path_from = concept_id(110);
    let path_middle = concept_id(111);
    let path_to = concept_id(112);
    let direct_assertion_id = assertion_id(210);
    let direct_assertion = assertion_record(
        direct_assertion_id,
        3,
        path_to,
        RelationType::Uses,
        path_from,
        &complete_evidence,
    );
    let direct_assertion_wire = assertion_wire(
        json!(direct_assertion_id.as_uuid().to_string()),
        3,
        path_to,
        RelationType::Uses,
        path_from,
        &complete_evidence,
    );
    let first_hop_assertion_id = assertion_id(211);
    let first_hop_assertion = assertion_record(
        first_hop_assertion_id,
        8,
        path_from,
        RelationType::PartOf,
        path_middle,
        &complete_evidence,
    );
    let first_hop_assertion_wire = assertion_wire(
        json!(first_hop_assertion_id.as_uuid().to_string()),
        8,
        path_from,
        RelationType::PartOf,
        path_middle,
        &complete_evidence,
    );
    let second_hop_evidence = vec![session_source_input.clone()];
    let second_hop_assertion_id = assertion_id(212);
    let second_hop_assertion = assertion_record(
        second_hop_assertion_id,
        9,
        path_to,
        RelationType::RelatedTo,
        path_middle,
        &second_hop_evidence,
    );
    let second_hop_assertion_wire = assertion_wire(
        json!(second_hop_assertion_id.as_uuid().to_string()),
        9,
        path_to,
        RelationType::RelatedTo,
        path_middle,
        &second_hop_evidence,
    );
    let expected_paths = vec![
        GraphPath {
            concepts: vec![path_from, path_to],
            assertions: vec![OrientedAssertion {
                assertion: direct_assertion,
                orientation: EdgeOrientation::Incoming,
            }],
        },
        GraphPath {
            concepts: vec![path_from, path_middle, path_to],
            assertions: vec![
                OrientedAssertion {
                    assertion: first_hop_assertion,
                    orientation: EdgeOrientation::Outgoing,
                },
                OrientedAssertion {
                    assertion: second_hop_assertion,
                    orientation: EdgeOrientation::Symmetric,
                },
            ],
        },
    ];
    verify_call!(
        session,
        ExpectedInvocation::query_result(
            GET_PATHS_SQL,
            vec![
                ParameterExpectation::Exact(concept_uuid(path_from)),
                ParameterExpectation::Exact(concept_uuid(path_to)),
                ParameterExpectation::Exact(json!("either")),
                ParameterExpectation::Exact(json!("3")),
                ParameterExpectation::Exact(json!("64")),
                ParameterExpectation::Exact(json!("7")),
            ],
            json!([
                {
                    "concepts": [path_from.as_uuid().to_string(), path_to.as_uuid().to_string()],
                    "assertions": [{
                        "assertion": direct_assertion_wire,
                        "orientation": "incoming",
                    }],
                },
                {
                    "concepts": [
                        path_from.as_uuid().to_string(),
                        path_middle.as_uuid().to_string(),
                        path_to.as_uuid().to_string(),
                    ],
                    "assertions": [
                        {
                            "assertion": first_hop_assertion_wire,
                            "orientation": "outgoing",
                        },
                        {
                            "assertion": second_hop_assertion_wire,
                            "orientation": "symmetric",
                        },
                    ],
                },
            ]),
        ),
        find_paths(PathQuery {
            from: path_from,
            to: path_to,
            direction: DirectionMode::Either,
            max_depth: 3,
            max_work: 64,
            limit: 7,
        }),
        Ok(expected_paths)
    );

    let empty_neighbor = NeighborQuery {
        concept_id: concept_id(120),
        direction: DirectionMode::Outgoing,
        relation_filter: Vec::new(),
        limit: 3,
    };
    verify_call!(
        session,
        ExpectedInvocation::query_result(
            GET_NEIGHBORS_SQL,
            neighbor_parameters(&empty_neighbor),
            json!([]),
        ),
        neighbors(empty_neighbor),
        Ok(Vec::<NeighborResult>::new())
    );

    let empty_related_sources = RelatedSourceQuery {
        concept_id: concept_id(121),
        limit: 4,
    };
    verify_call!(
        session,
        ExpectedInvocation::query_result(
            GET_RELATED_SOURCES_SQL,
            related_source_parameters(&empty_related_sources),
            json!([]),
        ),
        related_sources(empty_related_sources),
        Ok(Vec::<RelatedSourceResult>::new())
    );

    let empty_paths = PathQuery {
        from: concept_id(122),
        to: concept_id(123),
        direction: DirectionMode::Incoming,
        max_depth: 2,
        max_work: 16,
        limit: 4,
    };
    verify_call!(
        session,
        ExpectedInvocation::query_result(GET_PATHS_SQL, path_parameters(&empty_paths), json!([]),),
        find_paths(empty_paths),
        Ok(Vec::<GraphPath>::new())
    );

    let too_large_neighbors = NeighborQuery {
        concept_id: concept_id(130),
        direction: DirectionMode::Either,
        relation_filter: vec![RelationType::PartOf],
        limit: 5,
    };
    verify_call!(
        session,
        ExpectedInvocation::query_payload(
            GET_NEIGHBORS_SQL,
            neighbor_parameters(&too_large_neighbors),
            json!({"outcome": "result_too_large"}),
        ),
        neighbors(too_large_neighbors),
        Err(expected_result_bound(OperationCode::Neighbors))
    );

    let too_large_related_sources = RelatedSourceQuery {
        concept_id: concept_id(131),
        limit: 6,
    };
    verify_call!(
        session,
        ExpectedInvocation::query_payload(
            GET_RELATED_SOURCES_SQL,
            related_source_parameters(&too_large_related_sources),
            json!({"outcome": "result_too_large"}),
        ),
        related_sources(too_large_related_sources),
        Err(expected_result_bound(OperationCode::RelatedSources))
    );

    let too_large_paths = PathQuery {
        from: concept_id(132),
        to: concept_id(133),
        direction: DirectionMode::Either,
        max_depth: 2,
        max_work: 16,
        limit: 4,
    };
    verify_call!(
        session,
        ExpectedInvocation::query_payload(
            GET_PATHS_SQL,
            path_parameters(&too_large_paths),
            json!({"outcome": "result_too_large"}),
        ),
        find_paths(too_large_paths),
        Err(expected_result_bound(OperationCode::FindPaths))
    );

    let exhausted_paths = PathQuery {
        from: concept_id(134),
        to: concept_id(135),
        direction: DirectionMode::Either,
        max_depth: 3,
        max_work: 1,
        limit: 4,
    };
    verify_call!(
        session,
        ExpectedInvocation::query_payload(
            GET_PATHS_SQL,
            path_parameters(&exhausted_paths),
            json!({"outcome": "work_exhausted"}),
        ),
        find_paths(exhausted_paths),
        Err(expected_traversal_bound(
            OperationCode::FindPaths,
            TraversalBound::Work,
            TraversalReason::WorkExhausted,
        ))
    );

    session.finish().await
}

async fn verify_malformed_responses_are_private() -> VerificationResult {
    let mut session = FakeSession::start(protocol_timeouts()).await?;
    let malformed_alias = [AliasFixture {
        display_text: ALIAS_SENTINEL,
        alias_key: "private_alias_sentinel",
        preferred: true,
    }];

    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            CREATE_CONCEPT_SQL,
            vec![
                ParameterExpectation::UuidV7,
                ParameterExpectation::Exact(alias_json(&malformed_alias)),
            ],
            json!({"outcome": BACKEND_SENTINEL}),
        ),
        create_concept(CreateConcept {
            aliases: alias_input(&malformed_alias),
        }),
        Err(expected_invalid_response(
            OperationCode::CreateConcept,
            BackendFailureReason::UnknownOutcome,
        ))
    );

    let bad_execute_envelope = json!({
        "affected_rows": 2,
        "last_insert_id": null,
        "returned_rows": [],
    });
    verify_call!(
        session,
        ExpectedInvocation::execute_reply(
            DELETE_CONCEPT_SQL,
            exact_parameters(vec![concept_uuid(concept_id(80)), json!("1")]),
            move |_| PeerReply::Result(bad_execute_envelope.clone()),
        ),
        delete_concept(DeleteConcept {
            concept_id: concept_id(80),
            expected_revision: revision_value(1),
        }),
        Err(expected_database_failure(OperationCode::DeleteConcept))
    );

    let malformed_concept = json!({
        "id": concept_id(81).as_uuid().to_string(),
        "revision": 3,
        "aliases": [{"display_text": ALIAS_SENTINEL, "preferred": false}],
    });
    verify_call!(
        session,
        ExpectedInvocation::query_result(
            GET_CONCEPT_SQL,
            exact_parameters(vec![concept_uuid(concept_id(81))]),
            malformed_concept,
        ),
        get_concept(ConceptIdInput {
            id: concept_id(81).as_uuid().to_string(),
        }),
        Err(expected_invalid_response(
            OperationCode::GetConcept,
            BackendFailureReason::UnsupportedValue,
        ))
    );

    let malformed_source = json!({
        "kind": "session_record",
        "session_record_id": SESSION_SOURCE_SENTINEL,
        "extra": BACKEND_SENTINEL,
    });
    let malformed_source_input = session_source();
    verify_call!(
        session,
        ExpectedInvocation::query_result(
            GET_SOURCE_SQL,
            exact_parameters(source_parameters(&malformed_source_input)),
            malformed_source,
        ),
        get_source(malformed_source_input),
        Err(expected_invalid_response(
            OperationCode::GetSource,
            BackendFailureReason::UnsupportedValue,
        ))
    );

    let malformed_source_input = memory_source();
    verify_call!(
        session,
        ExpectedInvocation::execute_row(
            DELETE_SOURCE_SQL,
            exact_parameters(source_parameters(&malformed_source_input)),
            json!({"outcome": "deleted", "source_key": MEMORY_SOURCE_SENTINEL}),
        ),
        delete_source(DeleteSource {
            source: malformed_source_input,
        }),
        Err(expected_invalid_response(
            OperationCode::DeleteSource,
            BackendFailureReason::UnsupportedValue,
        ))
    );

    let remote_failure_input = session_source();
    let remote_failure_parameters = exact_parameters(source_parameters(&remote_failure_input));
    let store = Arc::clone(&session.store);
    let remote_error = session
        .invoke(
            ExpectedInvocation::query_reply(GET_SOURCE_SQL, remote_failure_parameters, |_| {
                PeerReply::RemoteFailure
            }),
            store.get_source(remote_failure_input),
        )
        .await?
        .expect_err("a backend failure must not produce a source record");
    assert_error_is_private(&remote_error)?;
    require(remote_error == expected_database_failure(OperationCode::GetSource))?;

    session.finish().await
}

async fn verify_bounded_query_failures() -> VerificationResult {
    let mut session = FakeSession::start(protocol_timeouts()).await?;

    let timeout_neighbors = NeighborQuery {
        concept_id: concept_id(200),
        direction: DirectionMode::Either,
        relation_filter: vec![RelationType::Uses, RelationType::PartOf],
        limit: 6,
    };
    verify_call!(
        session,
        ExpectedInvocation::query_reply(
            GET_NEIGHBORS_SQL,
            neighbor_parameters(&timeout_neighbors),
            |_| exact_query_timeout_reply(),
        ),
        neighbors(timeout_neighbors),
        Err(expected_query_timeout(OperationCode::Neighbors))
    );

    let timeout_related_sources = RelatedSourceQuery {
        concept_id: concept_id(201),
        limit: 8,
    };
    verify_call!(
        session,
        ExpectedInvocation::query_reply(
            GET_RELATED_SOURCES_SQL,
            related_source_parameters(&timeout_related_sources),
            |_| exact_query_timeout_reply(),
        ),
        related_sources(timeout_related_sources),
        Err(expected_query_timeout(OperationCode::RelatedSources))
    );

    let timeout_paths = PathQuery {
        from: concept_id(202),
        to: concept_id(203),
        direction: DirectionMode::Either,
        max_depth: 4,
        max_work: 80,
        limit: 5,
    };
    verify_call!(
        session,
        ExpectedInvocation::query_reply(GET_PATHS_SQL, path_parameters(&timeout_paths), |_| {
            exact_query_timeout_reply()
        },),
        find_paths(timeout_paths),
        Err(expected_query_timeout(OperationCode::FindPaths))
    );

    let timeout_body = worker_error_body("QUERY_TIMEOUT");
    let malformed_json_message = format!(
        "handler error: {{malformed-json {}}}",
        protected_sentinels().join(" ")
    );
    let missing_code_body = format!(r#"{{"message":"{}"}}"#, protected_sentinels().join(" "));
    let near_misses = vec![
        (
            BACKEND_CODE_SENTINEL.to_owned(),
            format!("handler error: {timeout_body}"),
        ),
        ("invocation_failed".to_owned(), timeout_body.clone()),
        (
            "invocation_failed".to_owned(),
            format!("Handler error: {timeout_body}"),
        ),
        (
            "invocation_failed".to_owned(),
            format!("handler error:{timeout_body}"),
        ),
        ("invocation_failed".to_owned(), malformed_json_message),
        (
            "invocation_failed".to_owned(),
            format!("handler error: {}", worker_error_body("query_timeout")),
        ),
        (
            "invocation_failed".to_owned(),
            format!(
                "handler error: {}",
                worker_error_body("QUERY_TIMEOUT_NEAR_MISS")
            ),
        ),
        (
            "invocation_failed".to_owned(),
            format!("handler error: {}", worker_error_body("DATABASE_ERROR")),
        ),
        (
            "invocation_failed".to_owned(),
            format!("handler error: {missing_code_body}"),
        ),
    ];

    for (index, (code, message)) in near_misses.into_iter().enumerate() {
        let base = 300 + (index as u128 * 2);
        let query = PathQuery {
            from: concept_id(base),
            to: concept_id(base + 1),
            direction: DirectionMode::Either,
            max_depth: 3,
            max_work: 40,
            limit: 4,
        };
        let invocation =
            ExpectedInvocation::query_reply(GET_PATHS_SQL, path_parameters(&query), move |_| {
                PeerReply::RemoteError {
                    code: code.clone(),
                    message: message.clone(),
                    stacktrace: Some(BACKEND_STACK_SENTINEL.to_owned()),
                }
            });
        let store = Arc::clone(&session.store);
        let result = session.invoke(invocation, store.find_paths(query)).await?;
        let error = result.expect_err("a timeout near miss must not produce paths");
        assert_error_is_private(&error)?;
        require(error == expected_database_failure(OperationCode::FindPaths))?;
    }

    let generic_query = PathQuery {
        from: concept_id(330),
        to: concept_id(331),
        direction: DirectionMode::Outgoing,
        max_depth: 2,
        max_work: 20,
        limit: 3,
    };
    verify_call!(
        session,
        ExpectedInvocation::query_reply(GET_PATHS_SQL, path_parameters(&generic_query), |_| {
            PeerReply::RemoteFailure
        },),
        find_paths(generic_query),
        Err(expected_database_failure(OperationCode::FindPaths))
    );

    let wrong_row_count = NeighborQuery {
        concept_id: concept_id(340),
        direction: DirectionMode::Outgoing,
        relation_filter: Vec::new(),
        limit: 4,
    };
    verify_call!(
        session,
        ExpectedInvocation::query_reply(
            GET_NEIGHBORS_SQL,
            neighbor_parameters(&wrong_row_count),
            |_| {
                PeerReply::Result(json!({
                    "rows": [{"payload": {"outcome": "complete", "result": []}}],
                    "row_count": 2,
                    "columns": [{"name": "payload", "type": "jsonb"}],
                }))
            },
        ),
        neighbors(wrong_row_count),
        Err(expected_database_failure(OperationCode::Neighbors))
    );

    let wrong_column = RelatedSourceQuery {
        concept_id: concept_id(341),
        limit: 5,
    };
    verify_call!(
        session,
        ExpectedInvocation::query_reply(
            GET_RELATED_SOURCES_SQL,
            related_source_parameters(&wrong_column),
            |_| {
                PeerReply::Result(json!({
                    "rows": [{"payload": {"outcome": "complete", "result": []}}],
                    "row_count": 1,
                    "columns": [{"name": "unexpected", "type": "jsonb"}],
                }))
            },
        ),
        related_sources(wrong_column),
        Err(expected_invalid_response(
            OperationCode::RelatedSources,
            BackendFailureReason::MalformedResponse,
        ))
    );

    let unknown_outcome = PathQuery {
        from: concept_id(342),
        to: concept_id(343),
        direction: DirectionMode::Either,
        max_depth: 2,
        max_work: 20,
        limit: 3,
    };
    verify_call!(
        session,
        ExpectedInvocation::query_payload(
            GET_PATHS_SQL,
            path_parameters(&unknown_outcome),
            json!({"outcome": "partial", "result": []}),
        ),
        find_paths(unknown_outcome),
        Err(expected_invalid_response(
            OperationCode::FindPaths,
            BackendFailureReason::UnknownOutcome,
        ))
    );

    let extra_payload_field = RelatedSourceQuery {
        concept_id: concept_id(344),
        limit: 5,
    };
    verify_call!(
        session,
        ExpectedInvocation::query_payload(
            GET_RELATED_SOURCES_SQL,
            related_source_parameters(&extra_payload_field),
            json!({
                "outcome": "complete",
                "result": [],
                "backend_details": BACKEND_SENTINEL,
            }),
        ),
        related_sources(extra_payload_field),
        Err(expected_invalid_response(
            OperationCode::RelatedSources,
            BackendFailureReason::MalformedResponse,
        ))
    );

    let partial_work_marker = PathQuery {
        from: concept_id(345),
        to: concept_id(346),
        direction: DirectionMode::Either,
        max_depth: 3,
        max_work: 1,
        limit: 4,
    };
    verify_call!(
        session,
        ExpectedInvocation::query_payload(
            GET_PATHS_SQL,
            path_parameters(&partial_work_marker),
            json!({
                "outcome": "work_exhausted",
                "result": [PATH_SENTINEL],
            }),
        ),
        find_paths(partial_work_marker),
        Err(expected_invalid_response(
            OperationCode::FindPaths,
            BackendFailureReason::MalformedResponse,
        ))
    );

    let malformed_neighbor_query = NeighborQuery {
        concept_id: concept_id(350),
        direction: DirectionMode::Either,
        relation_filter: vec![RelationType::Uses],
        limit: 4,
    };
    let malformed_neighbor = concept_id(351);
    let malformed_assertion_id = assertion_id(352);
    let malformed_evidence = vec![memory_source(), session_source()];
    verify_call!(
        session,
        ExpectedInvocation::query_result(
            GET_NEIGHBORS_SQL,
            neighbor_parameters(&malformed_neighbor_query),
            json!([{
                "neighbor": concept_wire(
                    concept_uuid(malformed_neighbor),
                    2,
                    &CREATED_ALIASES,
                ),
                "assertion": assertion_wire(
                    json!(malformed_assertion_id.as_uuid().to_string()),
                    4,
                    malformed_neighbor_query.concept_id,
                    RelationType::Uses,
                    malformed_neighbor,
                    &malformed_evidence,
                ),
                "orientation": "incoming",
            }]),
        ),
        neighbors(malformed_neighbor_query),
        Err(expected_invalid_response(
            OperationCode::Neighbors,
            BackendFailureReason::UnsupportedValue,
        ))
    );

    session.finish().await
}

async fn verify_invocation_timeout_is_applied_without_retry() -> VerificationResult {
    let mut session = FakeSession::start(protocol_timeouts()).await?;
    let source = session_source();
    let store = Arc::clone(&session.store);
    let result = session
        .invoke_without_response(
            ExpectedInvocation::query_reply(
                GET_SOURCE_SQL,
                exact_parameters(source_parameters(&source)),
                |_| PeerReply::NoResponse,
            ),
            store.get_source(source),
        )
        .await?;
    let error = result.expect_err("an invocation without a peer response must time out");
    assert_error_is_private(&error)?;
    require(error == expected_database_failure(OperationCode::GetSource))?;
    session.finish().await?;

    let mut mutation_session = FakeSession::start(protocol_timeouts()).await?;
    let concept = concept_id(82);
    let store = Arc::clone(&mutation_session.store);
    let result = mutation_session
        .invoke_without_response(
            ExpectedInvocation::execute_reply(
                DELETE_CONCEPT_SQL,
                exact_parameters(vec![concept_uuid(concept), json!("1")]),
                |_| PeerReply::NoResponse,
            ),
            store.delete_concept(DeleteConcept {
                concept_id: concept,
                expected_revision: revision_value(1),
            }),
        )
        .await?;
    let error = result.expect_err("a mutation without a peer response must time out");
    assert_error_is_private(&error)?;
    require(error == expected_database_failure(OperationCode::DeleteConcept))?;
    mutation_session.finish().await
}

async fn verify_bounded_query_invocation_timeout_without_retry() -> VerificationResult {
    let mut session = FakeSession::start(protocol_timeouts()).await?;
    let query = PathQuery {
        from: concept_id(360),
        to: concept_id(361),
        direction: DirectionMode::Either,
        max_depth: 4,
        max_work: 64,
        limit: 5,
    };
    let store = Arc::clone(&session.store);
    let result = session
        .invoke_without_response(
            ExpectedInvocation::query_reply(GET_PATHS_SQL, path_parameters(&query), |_| {
                PeerReply::NoResponse
            }),
            store.find_paths(query),
        )
        .await?;
    let error = result.expect_err("an unreturned bounded query must hit invocation timeout");
    assert_error_is_private(&error)?;
    require(error == expected_database_failure(OperationCode::FindPaths))?;
    session.finish().await
}

fn verify_protocol_rejections_are_strict() -> VerificationResult {
    let assertion = assertion_id(90);
    let subject = concept_id(91);
    let object = concept_id(92);
    let valid_parameters = vec![
        json!(assertion.as_uuid().to_string()),
        json!("4"),
        concept_uuid(subject),
        json!(RelationType::Uses.as_str()),
        concept_uuid(object),
    ];
    let expected = ExpectedInvocation::execute_row(
        UPDATE_ASSERTION_SQL,
        exact_parameters(valid_parameters.clone()),
        json!({"outcome": "deleted"}),
    );
    let frame = database_frame(
        DATABASE_EXECUTE_FUNCTION_ID,
        json!({
            "db": DATABASE_TARGET,
            "sql": UPDATE_ASSERTION_SQL,
            "params": valid_parameters,
        }),
    );
    require(validate_database_invocation(&frame, &expected).is_ok())?;

    let mut wrong_type = frame.clone();
    wrong_type["type"] = json!("unexpected");
    let mut missing_invocation_id = frame.clone();
    if let Some(object) = missing_invocation_id.as_object_mut() {
        object.remove("invocation_id");
    }
    let mut wrong_function = frame.clone();
    wrong_function["function_id"] = json!(DATABASE_QUERY_FUNCTION_ID);
    let mut wrong_database = frame.clone();
    wrong_database["data"]["db"] = json!(DATABASE_TARGET.to_owned() + "-drift");
    let mut namespace_drift = frame.clone();
    namespace_drift["data"]["sql"] =
        json!(UPDATE_ASSERTION_SQL.replace("knowledge_graph.", "drifted_graph.",));
    let mut sql_value_drift = frame.clone();
    sql_value_drift["data"]["sql"] = json!(SQL_SENTINEL);
    let mut wrong_order = frame.clone();
    wrong_order["data"]["params"] = json!([
        assertion.as_uuid().to_string(),
        concept_uuid(subject),
        "4",
        RelationType::Uses.as_str(),
        concept_uuid(object),
    ]);
    let mut numeric_bigint = frame.clone();
    numeric_bigint["data"]["params"][1] = json!(4);
    let mut extra_envelope_field = frame.clone();
    extra_envelope_field["namespace"] = json!("unexpected");
    let mut extra_payload_field = frame.clone();
    extra_payload_field["data"]["metadata"] = json!(true);
    let mut wrong_sql_class = frame.clone();
    wrong_sql_class["data"]["sql"] = json!("SELECT 1");

    for candidate in [
        wrong_type,
        missing_invocation_id,
        wrong_function,
        wrong_database,
        namespace_drift,
        sql_value_drift,
        wrong_order,
        numeric_bigint,
        extra_envelope_field,
        extra_payload_field,
        wrong_sql_class,
    ] {
        let Some(error) = validate_database_invocation(&candidate, &expected).err() else {
            return verification_failure();
        };
        assert_verification_error_is_private(&error)?;
    }

    let delete_expectation = ExpectedInvocation::execute_row(
        DELETE_ASSERTION_SQL,
        exact_parameters(vec![json!(assertion.as_uuid().to_string()), json!("4")]),
        json!({"outcome": "deleted"}),
    );
    require(validate_database_invocation(&frame, &delete_expectation).is_err())?;

    let query_expectation = ExpectedInvocation::query_result(
        GET_CONCEPT_SQL,
        exact_parameters(vec![concept_uuid(subject)]),
        Value::Null,
    );
    let query_frame = database_frame(
        DATABASE_QUERY_FUNCTION_ID,
        json!({
            "db": DATABASE_TARGET,
            "sql": GET_CONCEPT_SQL,
            "params": [concept_uuid(subject)],
            "timeout_ms": QUERY_TIMEOUT_MILLIS,
        }),
    );
    require(validate_database_invocation(&query_frame, &query_expectation).is_ok())?;
    let mut wrong_query_timeout = query_frame;
    wrong_query_timeout["data"]["timeout_ms"] = json!(QUERY_TIMEOUT_MILLIS + 1);
    require(validate_database_invocation(&wrong_query_timeout, &query_expectation).is_err())?;

    assert_verification_error_is_private(&VerificationError)?;
    require(reject_unexpected_database_invocation(&frame).is_err())?;
    require(validate_call_count(3, 3).is_ok())?;
    require(validate_call_count(3, 2).is_err())
}

fn database_frame(function_id: &str, data: Value) -> Value {
    json!({
        "type": "invokefunction",
        "invocation_id": "00000000-0000-4000-8000-000000000001",
        "function_id": function_id,
        "data": data,
    })
}

fn request_parameter(request: &Value, index: usize) -> Value {
    request
        .get("data")
        .and_then(|data| data.get("params"))
        .and_then(Value::as_array)
        .and_then(|parameters| parameters.get(index))
        .cloned()
        .unwrap_or(Value::Null)
}

fn assert_verification_error_is_private(error: &VerificationError) -> VerificationResult {
    let display = error.to_string();
    let debug = format!("{error:?}");
    for sentinel in protected_sentinels() {
        require(!display.contains(sentinel))?;
        require(!debug.contains(sentinel))?;
    }
    require(!display.contains(SQL_SENTINEL))?;
    require(!debug.contains(SQL_SENTINEL))
}

#[cfg(test)]
mod tests {
    use super::{
        IiiKnowledgeGraphStore, verify_bounded_queries, verify_bounded_query_failures,
        verify_bounded_query_invocation_timeout_without_retry,
        verify_invocation_timeout_is_applied_without_retry, verify_malformed_responses_are_private,
        verify_mutations_and_direct_reads, verify_operation_pending_before_response,
        verify_protocol_rejections_are_strict,
    };
    use std::time::Duration;

    use tokio::{sync::oneshot, time::sleep};

    #[test]
    fn public_facade_exposes_all_bounded_query_delegates() {
        let _neighbors = IiiKnowledgeGraphStore::neighbors;
        let _related_sources = IiiKnowledgeGraphStore::related_sources;
        let _find_paths = IiiKnowledgeGraphStore::find_paths;
    }

    #[tokio::test]
    async fn early_success_or_failure_during_response_hold_is_rejected() {
        const COMPLETION_DELAY: Duration = Duration::from_millis(5);
        const RESPONSE_HOLD: Duration = Duration::from_millis(40);

        let (success_tx, success_rx) = oneshot::channel();
        tokio::spawn(async move {
            sleep(COMPLETION_DELAY).await;
            let _ = success_tx.send(());
        });
        let mut early_success = Box::pin(async move {
            success_rx.await.map_err(|_| ())?;
            Ok::<(), ()>(())
        });

        let (failure_tx, failure_rx) = oneshot::channel();
        tokio::spawn(async move {
            sleep(COMPLETION_DELAY).await;
            let _ = failure_tx.send(());
        });
        let mut early_failure = Box::pin(async move {
            failure_rx.await.map_err(|_| ())?;
            Err::<(), ()>(())
        });

        let success_was_accepted =
            verify_operation_pending_before_response(&mut early_success, RESPONSE_HOLD)
                .await
                .is_ok();
        let failure_was_accepted =
            verify_operation_pending_before_response(&mut early_failure, RESPONSE_HOLD)
                .await
                .is_ok();

        assert!(
            !success_was_accepted && !failure_was_accepted,
            "held-response check accepted early completion: success={success_was_accepted}, failure={failure_was_accepted}"
        );
    }

    #[tokio::test]
    async fn loopback_protocol_checks_every_mutation_and_direct_read() {
        assert!(verify_mutations_and_direct_reads().await.is_ok());
    }

    #[tokio::test]
    async fn loopback_protocol_checks_bounded_queries_empty_results_and_markers() {
        assert!(verify_bounded_queries().await.is_ok());
    }

    #[tokio::test]
    async fn malformed_rows_and_backend_errors_remain_private() {
        assert!(verify_malformed_responses_are_private().await.is_ok());
    }

    #[tokio::test]
    async fn invocation_deadline_expires_after_the_query_deadline_without_retry() {
        assert!(
            verify_invocation_timeout_is_applied_without_retry()
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn bounded_query_timeouts_near_misses_and_malformed_envelopes_are_private() {
        assert!(verify_bounded_query_failures().await.is_ok());
    }

    #[tokio::test]
    async fn bounded_query_invocation_deadline_expires_without_retry() {
        assert!(
            verify_bounded_query_invocation_timeout_without_retry()
                .await
                .is_ok()
        );
    }

    #[test]
    fn peer_rejects_order_namespace_function_timeout_and_extra_call_drift() {
        assert!(verify_protocol_rejections_are_strict().is_ok());
    }
}
