use std::{cmp::Ordering, fmt, future::Future, time::Duration};

use iii_sdk::{IIIClient, protocol::TriggerRequest};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use thiserror::Error;

use crate::contracts::{
    AliasKey, Assertion, AssertionEvidence, AssertionId, BackendFailureReason, Concept, ConceptId,
    ConceptMention, ConflictField, ConflictReason, DatabaseMutationOutcome, DatabaseReadOutcome,
    DatabaseTimeouts, GraphError, GraphPath, LimitReason, LimitResource, NeighborResult,
    NotFoundReason, OperationCode, RecordIdentity, RecordKind, ReferenceReason, ReferencedRecord,
    RelatedSourceResult, SemanticAssertionIdentity, SourceReference, SourceReferenceInput,
    TraversalBound, TraversalReason, ValidatedAliasSet, ValidatedAssertionEvidence,
    ValidatedConceptMention, ValidatedCreateAssertion, ValidatedCreateConcept,
    ValidatedDeleteAssertion, ValidatedDeleteConcept, ValidatedDeleteSource,
    ValidatedNeighborQuery, ValidatedPathQuery, ValidatedRelatedSourceQuery,
    ValidatedReplaceConceptAliases, ValidatedSourceReference, ValidatedUpdateAssertion,
};

const DATABASE_EXECUTE_FUNCTION_ID: &str = "database::execute";
const DATABASE_QUERY_FUNCTION_ID: &str = "database::query";
const READ_PAYLOAD_COLUMN: &str = "payload";

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

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct DatabaseTarget(String);

impl DatabaseTarget {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    fn is_valid(&self) -> bool {
        !self.0.is_empty() && !self.0.contains('\0')
    }
}

impl fmt::Debug for DatabaseTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DatabaseTarget(<redacted>)")
    }
}

impl TryFrom<String> for DatabaseTarget {
    type Error = GraphConfigurationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let target = Self(value);
        if target.is_valid() {
            Ok(target)
        } else {
            Err(GraphConfigurationError::InvalidDatabaseTarget)
        }
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum GraphConfigurationError {
    #[error("invalid_database_target")]
    InvalidDatabaseTarget,
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct IiiKnowledgeGraphDatabase {
    #[allow(dead_code)]
    client: IIIClient,
    database: DatabaseTarget,
    timeouts: DatabaseTimeouts,
}

impl IiiKnowledgeGraphDatabase {
    #[allow(dead_code)]
    pub(crate) fn new(
        client: IIIClient,
        database: DatabaseTarget,
        timeouts: DatabaseTimeouts,
    ) -> Result<Self, GraphConfigurationError> {
        if !database.is_valid() {
            return Err(GraphConfigurationError::InvalidDatabaseTarget);
        }

        Ok(Self {
            client,
            database,
            timeouts,
        })
    }

    #[allow(dead_code)]
    async fn execute<T: DeserializeOwned>(
        &self,
        operation: OperationCode,
        sql: &'static str,
        params: PositionalParameters,
    ) -> DatabaseResult<DatabaseMutationOutcome<T>> {
        execute_with(
            &self.database,
            self.timeouts,
            operation,
            sql,
            params,
            |request| self.client.trigger(request),
        )
        .await
    }

    #[allow(dead_code)]
    async fn query<T: DeserializeOwned + serde::Serialize>(
        &self,
        operation: OperationCode,
        sql: &'static str,
        params: PositionalParameters,
    ) -> DatabaseResult<DatabaseReadOutcome<T>> {
        query_with(
            &self.database,
            self.timeouts,
            operation,
            sql,
            params,
            |request| self.client.trigger(request),
        )
        .await
    }

    #[allow(dead_code)]
    async fn create_concept(&self, input: &ValidatedCreateConcept) -> DatabaseResult<Concept> {
        self.create_concept_with(input, |request| self.client.trigger(request))
            .await
    }

    #[allow(dead_code)]
    async fn create_concept_with<F, Fut>(
        &self,
        input: &ValidatedCreateConcept,
        invoke: F,
    ) -> DatabaseResult<Concept>
    where
        F: FnOnce(TriggerRequest) -> Fut,
        Fut: Future<Output = Result<Value, iii_sdk::Error>>,
    {
        let outcome = execute_with(
            &self.database,
            self.timeouts,
            OperationCode::CreateConcept,
            CREATE_CONCEPT_SQL,
            PositionalParameters::from(vec![
                json!(input.candidate_id.as_uuid().to_string()),
                normalized_aliases_json(&input.aliases),
            ]),
            invoke,
        )
        .await?;
        decode_create_concept_outcome(input, outcome)
    }

    #[allow(dead_code)]
    async fn replace_concept_aliases(
        &self,
        input: &ValidatedReplaceConceptAliases,
    ) -> DatabaseResult<Concept> {
        self.replace_concept_aliases_with(input, |request| self.client.trigger(request))
            .await
    }

    #[allow(dead_code)]
    async fn replace_concept_aliases_with<F, Fut>(
        &self,
        input: &ValidatedReplaceConceptAliases,
        invoke: F,
    ) -> DatabaseResult<Concept>
    where
        F: FnOnce(TriggerRequest) -> Fut,
        Fut: Future<Output = Result<Value, iii_sdk::Error>>,
    {
        let outcome = execute_with(
            &self.database,
            self.timeouts,
            OperationCode::ReplaceConceptAliases,
            REPLACE_CONCEPT_ALIASES_SQL,
            PositionalParameters::from(vec![
                json!(input.concept_id.as_uuid().to_string()),
                json!(input.expected_revision.get().to_string()),
                normalized_aliases_json(&input.aliases),
            ]),
            invoke,
        )
        .await?;
        decode_replace_concept_aliases_outcome(input, outcome)
    }

    #[allow(dead_code)]
    async fn delete_concept(&self, input: &ValidatedDeleteConcept) -> DatabaseResult<()> {
        self.delete_concept_with(input, |request| self.client.trigger(request))
            .await
    }

    #[allow(dead_code)]
    async fn delete_concept_with<F, Fut>(
        &self,
        input: &ValidatedDeleteConcept,
        invoke: F,
    ) -> DatabaseResult<()>
    where
        F: FnOnce(TriggerRequest) -> Fut,
        Fut: Future<Output = Result<Value, iii_sdk::Error>>,
    {
        let outcome = execute_with(
            &self.database,
            self.timeouts,
            OperationCode::DeleteConcept,
            DELETE_CONCEPT_SQL,
            PositionalParameters::from(vec![
                json!(input.concept_id.as_uuid().to_string()),
                json!(input.expected_revision.get().to_string()),
            ]),
            invoke,
        )
        .await?;
        decode_delete_concept_outcome(input, outcome)
    }

    #[allow(dead_code)]
    async fn get_concept(&self, id: &ConceptId) -> DatabaseResult<Option<Concept>> {
        self.get_concept_with(id, |request| self.client.trigger(request))
            .await
    }

    #[allow(dead_code)]
    async fn get_concept_with<F, Fut>(
        &self,
        id: &ConceptId,
        invoke: F,
    ) -> DatabaseResult<Option<Concept>>
    where
        F: FnOnce(TriggerRequest) -> Fut,
        Fut: Future<Output = Result<Value, iii_sdk::Error>>,
    {
        let outcome = query_with::<Option<Concept>, _, _>(
            &self.database,
            self.timeouts,
            OperationCode::GetConcept,
            GET_CONCEPT_SQL,
            PositionalParameters::from(vec![json!(id.as_uuid().to_string())]),
            invoke,
        )
        .await?
        .into_graph_result(OperationCode::GetConcept)?;
        outcome
            .map(|record| {
                validate_concept_record(OperationCode::GetConcept, record, None, None, None)
            })
            .transpose()
    }

    #[allow(dead_code)]
    async fn resolve_alias(&self, key: &AliasKey) -> DatabaseResult<Option<Concept>> {
        self.resolve_alias_with(key, |request| self.client.trigger(request))
            .await
    }

    #[allow(dead_code)]
    async fn resolve_alias_with<F, Fut>(
        &self,
        key: &AliasKey,
        invoke: F,
    ) -> DatabaseResult<Option<Concept>>
    where
        F: FnOnce(TriggerRequest) -> Fut,
        Fut: Future<Output = Result<Value, iii_sdk::Error>>,
    {
        let outcome = query_with::<Option<Concept>, _, _>(
            &self.database,
            self.timeouts,
            OperationCode::ResolveAlias,
            RESOLVE_ALIAS_SQL,
            PositionalParameters::from(vec![json!(key.as_str())]),
            invoke,
        )
        .await?
        .into_graph_result(OperationCode::ResolveAlias)?;
        outcome
            .map(|record| {
                validate_concept_record(OperationCode::ResolveAlias, record, None, None, None)
            })
            .transpose()
    }

    #[allow(dead_code)]
    async fn register_source(
        &self,
        input: &ValidatedSourceReference,
    ) -> DatabaseResult<SourceReference> {
        self.register_source_with(input, |request| self.client.trigger(request))
            .await
    }

    #[allow(dead_code)]
    async fn register_source_with<F, Fut>(
        &self,
        input: &ValidatedSourceReference,
        invoke: F,
    ) -> DatabaseResult<SourceReference>
    where
        F: FnOnce(TriggerRequest) -> Fut,
        Fut: Future<Output = Result<Value, iii_sdk::Error>>,
    {
        let outcome = execute_source_outcome_with(
            &self.database,
            self.timeouts,
            OperationCode::RegisterSource,
            REGISTER_SOURCE_SQL,
            source_parameters(&input.source),
            invoke,
        )
        .await?;
        decode_register_source_outcome(input, outcome)
    }

    #[allow(dead_code)]
    async fn delete_source(&self, input: &ValidatedDeleteSource) -> DatabaseResult<()> {
        self.delete_source_with(input, |request| self.client.trigger(request))
            .await
    }

    #[allow(dead_code)]
    async fn delete_source_with<F, Fut>(
        &self,
        input: &ValidatedDeleteSource,
        invoke: F,
    ) -> DatabaseResult<()>
    where
        F: FnOnce(TriggerRequest) -> Fut,
        Fut: Future<Output = Result<Value, iii_sdk::Error>>,
    {
        let outcome = execute_source_outcome_with(
            &self.database,
            self.timeouts,
            OperationCode::DeleteSource,
            DELETE_SOURCE_SQL,
            source_parameters(&input.source.source),
            invoke,
        )
        .await?;
        decode_delete_source_outcome(outcome)
    }

    #[allow(dead_code)]
    async fn create_mention(
        &self,
        input: &ValidatedConceptMention,
    ) -> DatabaseResult<ConceptMention> {
        self.create_mention_with(input, |request| self.client.trigger(request))
            .await
    }

    #[allow(dead_code)]
    async fn create_mention_with<F, Fut>(
        &self,
        input: &ValidatedConceptMention,
        invoke: F,
    ) -> DatabaseResult<ConceptMention>
    where
        F: FnOnce(TriggerRequest) -> Fut,
        Fut: Future<Output = Result<Value, iii_sdk::Error>>,
    {
        let outcome = execute_source_outcome_with(
            &self.database,
            self.timeouts,
            OperationCode::CreateMention,
            CREATE_MENTION_SQL,
            mention_parameters(input.concept_id, &input.source.source),
            invoke,
        )
        .await?;
        decode_create_mention_outcome(input, outcome)
    }

    #[allow(dead_code)]
    async fn delete_mention(&self, input: &ValidatedConceptMention) -> DatabaseResult<()> {
        self.delete_mention_with(input, |request| self.client.trigger(request))
            .await
    }

    #[allow(dead_code)]
    async fn delete_mention_with<F, Fut>(
        &self,
        input: &ValidatedConceptMention,
        invoke: F,
    ) -> DatabaseResult<()>
    where
        F: FnOnce(TriggerRequest) -> Fut,
        Fut: Future<Output = Result<Value, iii_sdk::Error>>,
    {
        let outcome = execute_source_outcome_with(
            &self.database,
            self.timeouts,
            OperationCode::DeleteMention,
            DELETE_MENTION_SQL,
            mention_parameters(input.concept_id, &input.source.source),
            invoke,
        )
        .await?;
        decode_delete_mention_outcome(input, outcome)
    }

    #[allow(dead_code)]
    async fn create_assertion(
        &self,
        input: &ValidatedCreateAssertion,
    ) -> DatabaseResult<Assertion> {
        self.create_assertion_with(input, |request| self.client.trigger(request))
            .await
    }

    #[allow(dead_code)]
    async fn create_assertion_with<F, Fut>(
        &self,
        input: &ValidatedCreateAssertion,
        invoke: F,
    ) -> DatabaseResult<Assertion>
    where
        F: FnOnce(TriggerRequest) -> Fut,
        Fut: Future<Output = Result<Value, iii_sdk::Error>>,
    {
        let outcome = execute_with(
            &self.database,
            self.timeouts,
            OperationCode::CreateAssertion,
            CREATE_ASSERTION_SQL,
            create_assertion_parameters(input),
            invoke,
        )
        .await?;
        decode_create_assertion_outcome(input, outcome)
    }

    #[allow(dead_code)]
    async fn update_assertion(
        &self,
        input: &ValidatedUpdateAssertion,
    ) -> DatabaseResult<Assertion> {
        self.update_assertion_with(input, |request| self.client.trigger(request))
            .await
    }

    #[allow(dead_code)]
    async fn update_assertion_with<F, Fut>(
        &self,
        input: &ValidatedUpdateAssertion,
        invoke: F,
    ) -> DatabaseResult<Assertion>
    where
        F: FnOnce(TriggerRequest) -> Fut,
        Fut: Future<Output = Result<Value, iii_sdk::Error>>,
    {
        let outcome = execute_with(
            &self.database,
            self.timeouts,
            OperationCode::UpdateAssertion,
            UPDATE_ASSERTION_SQL,
            update_assertion_parameters(input),
            invoke,
        )
        .await?;
        decode_update_assertion_outcome(input, outcome)
    }

    #[allow(dead_code)]
    async fn delete_assertion(&self, input: &ValidatedDeleteAssertion) -> DatabaseResult<()> {
        self.delete_assertion_with(input, |request| self.client.trigger(request))
            .await
    }

    #[allow(dead_code)]
    async fn delete_assertion_with<F, Fut>(
        &self,
        input: &ValidatedDeleteAssertion,
        invoke: F,
    ) -> DatabaseResult<()>
    where
        F: FnOnce(TriggerRequest) -> Fut,
        Fut: Future<Output = Result<Value, iii_sdk::Error>>,
    {
        let outcome = execute_with(
            &self.database,
            self.timeouts,
            OperationCode::DeleteAssertion,
            DELETE_ASSERTION_SQL,
            delete_assertion_parameters(input),
            invoke,
        )
        .await?;
        decode_delete_assertion_outcome(input, outcome)
    }

    #[allow(dead_code)]
    async fn add_evidence(
        &self,
        input: &ValidatedAssertionEvidence,
    ) -> DatabaseResult<AssertionEvidence> {
        self.add_evidence_with(input, |request| self.client.trigger(request))
            .await
    }

    #[allow(dead_code)]
    async fn add_evidence_with<F, Fut>(
        &self,
        input: &ValidatedAssertionEvidence,
        invoke: F,
    ) -> DatabaseResult<AssertionEvidence>
    where
        F: FnOnce(TriggerRequest) -> Fut,
        Fut: Future<Output = Result<Value, iii_sdk::Error>>,
    {
        let outcome = execute_with(
            &self.database,
            self.timeouts,
            OperationCode::AddEvidence,
            ADD_ASSERTION_EVIDENCE_SQL,
            evidence_parameters(input),
            invoke,
        )
        .await?;
        decode_add_evidence_outcome(input, outcome)
    }

    #[allow(dead_code)]
    async fn remove_evidence(&self, input: &ValidatedAssertionEvidence) -> DatabaseResult<()> {
        self.remove_evidence_with(input, |request| self.client.trigger(request))
            .await
    }

    #[allow(dead_code)]
    async fn remove_evidence_with<F, Fut>(
        &self,
        input: &ValidatedAssertionEvidence,
        invoke: F,
    ) -> DatabaseResult<()>
    where
        F: FnOnce(TriggerRequest) -> Fut,
        Fut: Future<Output = Result<Value, iii_sdk::Error>>,
    {
        let outcome = execute_with(
            &self.database,
            self.timeouts,
            OperationCode::RemoveEvidence,
            REMOVE_ASSERTION_EVIDENCE_SQL,
            evidence_parameters(input),
            invoke,
        )
        .await?;
        decode_remove_evidence_outcome(input, outcome)
    }

    #[allow(dead_code)]
    async fn get_assertion(&self, id: AssertionId) -> DatabaseResult<Option<Assertion>> {
        self.get_assertion_with(id, |request| self.client.trigger(request))
            .await
    }

    #[allow(dead_code)]
    async fn get_assertion_with<F, Fut>(
        &self,
        id: AssertionId,
        invoke: F,
    ) -> DatabaseResult<Option<Assertion>>
    where
        F: FnOnce(TriggerRequest) -> Fut,
        Fut: Future<Output = Result<Value, iii_sdk::Error>>,
    {
        let records = query_with::<Option<Assertion>, _, _>(
            &self.database,
            self.timeouts,
            OperationCode::GetAssertion,
            GET_ASSERTION_SQL,
            PositionalParameters::from(vec![json!(id.as_uuid().to_string())]),
            invoke,
        )
        .await?
        .into_graph_result(OperationCode::GetAssertion)?;

        records
            .map(|record| {
                validate_assertion_record(
                    OperationCode::GetAssertion,
                    record,
                    Some(id),
                    None,
                    None,
                    None,
                )
            })
            .transpose()
    }

    #[allow(dead_code)]
    async fn get_source(
        &self,
        input: &ValidatedSourceReference,
    ) -> DatabaseResult<Option<SourceReference>> {
        self.get_source_with(input, |request| self.client.trigger(request))
            .await
    }

    #[allow(dead_code)]
    async fn get_source_with<F, Fut>(
        &self,
        input: &ValidatedSourceReference,
        invoke: F,
    ) -> DatabaseResult<Option<SourceReference>>
    where
        F: FnOnce(TriggerRequest) -> Fut,
        Fut: Future<Output = Result<Value, iii_sdk::Error>>,
    {
        let outcome = query_with::<Option<SourceReference>, _, _>(
            &self.database,
            self.timeouts,
            OperationCode::GetSource,
            GET_SOURCE_SQL,
            source_parameters(&input.source),
            invoke,
        )
        .await?
        .into_graph_result(OperationCode::GetSource)?;

        outcome
            .map(|record| {
                if record != input.source {
                    return Err(invalid_response(
                        OperationCode::GetSource,
                        BackendFailureReason::UnsupportedValue,
                    ));
                }
                Ok(record)
            })
            .transpose()
    }

    #[allow(dead_code)]
    async fn neighbors(
        &self,
        query: &ValidatedNeighborQuery,
    ) -> DatabaseResult<Vec<NeighborResult>> {
        self.neighbors_with(query, |request| self.client.trigger(request))
            .await
    }

    #[allow(dead_code)]
    async fn neighbors_with<F, Fut>(
        &self,
        query: &ValidatedNeighborQuery,
        invoke: F,
    ) -> DatabaseResult<Vec<NeighborResult>>
    where
        F: FnOnce(TriggerRequest) -> Fut,
        Fut: Future<Output = Result<Value, iii_sdk::Error>>,
    {
        query_with::<Vec<NeighborResult>, _, _>(
            &self.database,
            self.timeouts,
            OperationCode::Neighbors,
            GET_NEIGHBORS_SQL,
            PositionalParameters::from(vec![
                json!(query.concept_id.as_uuid().to_string()),
                json!(query.direction.as_str()),
                json!(
                    query
                        .relation_filter
                        .iter()
                        .map(|relation| relation.as_str())
                        .collect::<Vec<_>>()
                ),
                json!(query.limit.to_string()),
            ]),
            invoke,
        )
        .await?
        .into_graph_result(OperationCode::Neighbors)
    }

    #[allow(dead_code)]
    async fn related_sources(
        &self,
        query: &ValidatedRelatedSourceQuery,
    ) -> DatabaseResult<Vec<RelatedSourceResult>> {
        self.related_sources_with(query, |request| self.client.trigger(request))
            .await
    }

    #[allow(dead_code)]
    async fn related_sources_with<F, Fut>(
        &self,
        query: &ValidatedRelatedSourceQuery,
        invoke: F,
    ) -> DatabaseResult<Vec<RelatedSourceResult>>
    where
        F: FnOnce(TriggerRequest) -> Fut,
        Fut: Future<Output = Result<Value, iii_sdk::Error>>,
    {
        query_with::<Vec<RelatedSourceResult>, _, _>(
            &self.database,
            self.timeouts,
            OperationCode::RelatedSources,
            GET_RELATED_SOURCES_SQL,
            PositionalParameters::from(vec![
                json!(query.concept_id.as_uuid().to_string()),
                json!(query.limit.to_string()),
            ]),
            invoke,
        )
        .await?
        .into_graph_result(OperationCode::RelatedSources)
    }

    #[allow(dead_code)]
    async fn find_paths(&self, query: &ValidatedPathQuery) -> DatabaseResult<Vec<GraphPath>> {
        self.find_paths_with(query, |request| self.client.trigger(request))
            .await
    }

    #[allow(dead_code)]
    async fn find_paths_with<F, Fut>(
        &self,
        query: &ValidatedPathQuery,
        invoke: F,
    ) -> DatabaseResult<Vec<GraphPath>>
    where
        F: FnOnce(TriggerRequest) -> Fut,
        Fut: Future<Output = Result<Value, iii_sdk::Error>>,
    {
        let paths = query_with::<Vec<GraphPath>, _, _>(
            &self.database,
            self.timeouts,
            OperationCode::FindPaths,
            GET_PATHS_SQL,
            PositionalParameters::from(vec![
                json!(query.from.as_uuid().to_string()),
                json!(query.to.as_uuid().to_string()),
                json!(query.direction.as_str()),
                json!(query.max_depth.to_string()),
                json!(query.max_work.to_string()),
                json!(query.limit.to_string()),
            ]),
            invoke,
        )
        .await?
        .into_graph_result(OperationCode::FindPaths)?;
        validate_path_query_response(query, &paths)?;
        Ok(paths)
    }
}

fn validate_path_query_response(
    query: &ValidatedPathQuery,
    paths: &[GraphPath],
) -> DatabaseResult<()> {
    let paths_match_query = paths.len() <= usize::from(query.limit)
        && paths.iter().all(|path| {
            path.concepts.first() == Some(&query.from)
                && path.concepts.last() == Some(&query.to)
                && path.assertions.len() <= usize::from(query.max_depth)
                && path.assertions.iter().all(|edge| {
                    matches!(
                        (query.direction, edge.orientation),
                        (
                            crate::contracts::DirectionMode::Outgoing,
                            crate::contracts::EdgeOrientation::Outgoing
                                | crate::contracts::EdgeOrientation::Symmetric
                        ) | (
                            crate::contracts::DirectionMode::Incoming,
                            crate::contracts::EdgeOrientation::Incoming
                                | crate::contracts::EdgeOrientation::Symmetric
                        ) | (crate::contracts::DirectionMode::Either, _)
                    )
                })
        })
        && paths
            .windows(2)
            .all(|pair| compare_path_order(&pair[0], &pair[1]) == Ordering::Less);

    if paths_match_query {
        Ok(())
    } else {
        Err(invalid_response(
            OperationCode::FindPaths,
            BackendFailureReason::UnsupportedValue,
        ))
    }
}

fn compare_path_order(left: &GraphPath, right: &GraphPath) -> Ordering {
    left.assertions
        .len()
        .cmp(&right.assertions.len())
        .then_with(|| {
            left.assertions
                .iter()
                .map(|edge| edge.assertion.id.as_uuid())
                .cmp(
                    right
                        .assertions
                        .iter()
                        .map(|edge| edge.assertion.id.as_uuid()),
                )
        })
        .then_with(|| {
            left.concepts
                .iter()
                .map(ConceptId::as_uuid)
                .cmp(right.concepts.iter().map(ConceptId::as_uuid))
        })
}

#[async_trait::async_trait]
impl KnowledgeGraphDatabase for IiiKnowledgeGraphDatabase {
    async fn create_concept(&self, input: &ValidatedCreateConcept) -> DatabaseResult<Concept> {
        IiiKnowledgeGraphDatabase::create_concept(self, input).await
    }

    async fn replace_concept_aliases(
        &self,
        input: &ValidatedReplaceConceptAliases,
    ) -> DatabaseResult<Concept> {
        IiiKnowledgeGraphDatabase::replace_concept_aliases(self, input).await
    }

    async fn delete_concept(&self, input: &ValidatedDeleteConcept) -> DatabaseResult<()> {
        IiiKnowledgeGraphDatabase::delete_concept(self, input).await
    }

    async fn register_source(
        &self,
        source: &ValidatedSourceReference,
    ) -> DatabaseResult<SourceReference> {
        IiiKnowledgeGraphDatabase::register_source(self, source).await
    }

    async fn delete_source(&self, input: &ValidatedDeleteSource) -> DatabaseResult<()> {
        IiiKnowledgeGraphDatabase::delete_source(self, input).await
    }

    async fn create_mention(
        &self,
        input: &ValidatedConceptMention,
    ) -> DatabaseResult<ConceptMention> {
        IiiKnowledgeGraphDatabase::create_mention(self, input).await
    }

    async fn delete_mention(&self, input: &ValidatedConceptMention) -> DatabaseResult<()> {
        IiiKnowledgeGraphDatabase::delete_mention(self, input).await
    }

    async fn create_assertion(
        &self,
        input: &ValidatedCreateAssertion,
    ) -> DatabaseResult<Assertion> {
        IiiKnowledgeGraphDatabase::create_assertion(self, input).await
    }

    async fn update_assertion(
        &self,
        input: &ValidatedUpdateAssertion,
    ) -> DatabaseResult<Assertion> {
        IiiKnowledgeGraphDatabase::update_assertion(self, input).await
    }

    async fn delete_assertion(&self, input: &ValidatedDeleteAssertion) -> DatabaseResult<()> {
        IiiKnowledgeGraphDatabase::delete_assertion(self, input).await
    }

    async fn add_evidence(
        &self,
        input: &ValidatedAssertionEvidence,
    ) -> DatabaseResult<AssertionEvidence> {
        IiiKnowledgeGraphDatabase::add_evidence(self, input).await
    }

    async fn remove_evidence(&self, input: &ValidatedAssertionEvidence) -> DatabaseResult<()> {
        IiiKnowledgeGraphDatabase::remove_evidence(self, input).await
    }

    async fn get_concept(&self, id: &ConceptId) -> DatabaseResult<Option<Concept>> {
        IiiKnowledgeGraphDatabase::get_concept(self, id).await
    }

    async fn resolve_alias(&self, key: &AliasKey) -> DatabaseResult<Option<Concept>> {
        IiiKnowledgeGraphDatabase::resolve_alias(self, key).await
    }

    async fn get_assertion(&self, id: AssertionId) -> DatabaseResult<Option<Assertion>> {
        IiiKnowledgeGraphDatabase::get_assertion(self, id).await
    }

    async fn get_source(
        &self,
        source: &ValidatedSourceReference,
    ) -> DatabaseResult<Option<SourceReference>> {
        IiiKnowledgeGraphDatabase::get_source(self, source).await
    }

    async fn neighbors(
        &self,
        query: &ValidatedNeighborQuery,
    ) -> DatabaseResult<Vec<NeighborResult>> {
        IiiKnowledgeGraphDatabase::neighbors(self, query).await
    }

    async fn related_sources(
        &self,
        query: &ValidatedRelatedSourceQuery,
    ) -> DatabaseResult<Vec<RelatedSourceResult>> {
        IiiKnowledgeGraphDatabase::related_sources(self, query).await
    }

    async fn find_paths(&self, query: &ValidatedPathQuery) -> DatabaseResult<Vec<GraphPath>> {
        IiiKnowledgeGraphDatabase::find_paths(self, query).await
    }
}

fn normalized_aliases_json(aliases: &ValidatedAliasSet) -> Value {
    let mut aliases = aliases.aliases.iter().collect::<Vec<_>>();
    aliases.sort_by(|left, right| {
        right
            .preferred
            .cmp(&left.preferred)
            .then_with(|| left.normalized_key.cmp(&right.normalized_key))
    });

    Value::Array(
        aliases
            .into_iter()
            .map(|alias| {
                json!({
                    "alias_key": alias.normalized_key.as_str(),
                    "display_text": alias.display_text,
                    "is_preferred": alias.preferred,
                })
            })
            .collect(),
    )
}

fn concept_aliases_are_canonical(concept: &Concept) -> bool {
    let Some(preferred) = concept.aliases.first() else {
        return false;
    };
    if !preferred.preferred {
        return false;
    }

    let mut previous_key: Option<AliasKey> = None;
    for alias in &concept.aliases[1..] {
        if alias.preferred {
            return false;
        }
        let Ok(key) = crate::normalization::normalize_alias_key(&alias.display_text) else {
            return false;
        };
        if previous_key
            .as_ref()
            .is_some_and(|previous| previous >= &key)
        {
            return false;
        }
        previous_key = Some(key);
    }
    true
}

fn concept_aliases_match_input(concept: &Concept, input: &ValidatedAliasSet) -> bool {
    let mut expected = input.aliases.iter().collect::<Vec<_>>();
    expected.sort_by(|left, right| {
        right
            .preferred
            .cmp(&left.preferred)
            .then_with(|| left.normalized_key.cmp(&right.normalized_key))
    });

    concept.aliases.len() == expected.len()
        && concept
            .aliases
            .iter()
            .zip(expected)
            .all(|(actual, expected)| {
                actual.display_text == expected.display_text
                    && actual.preferred == expected.preferred
            })
}

fn validate_concept_record(
    operation: OperationCode,
    record: Concept,
    expected_id: Option<ConceptId>,
    expected_revision: Option<i64>,
    expected_aliases: Option<&ValidatedAliasSet>,
) -> DatabaseResult<Concept> {
    if !concept_aliases_are_canonical(&record)
        || expected_id.is_some_and(|id| record.id != id)
        || expected_revision.is_some_and(|revision| record.revision.get() != revision)
    {
        return Err(invalid_response(
            operation,
            BackendFailureReason::UnsupportedValue,
        ));
    }

    let normalized_aliases = crate::normalization::normalize_alias_set(&record.aliases)
        .map_err(|_| invalid_response(operation, BackendFailureReason::UnsupportedValue))?;
    if expected_aliases.is_some_and(|expected| normalized_aliases.identity() != expected.identity())
    {
        return Err(invalid_response(
            operation,
            BackendFailureReason::UnsupportedValue,
        ));
    }

    Ok(record)
}

fn invalid_concept_outcome(operation: OperationCode) -> GraphError {
    invalid_response(operation, BackendFailureReason::UnsupportedValue)
}

fn concept_conflict_is_supported(
    operation: OperationCode,
    field: ConflictField,
    reason: ConflictReason,
) -> bool {
    matches!(
        (operation, field, reason),
        (
            OperationCode::CreateConcept | OperationCode::ReplaceConceptAliases,
            ConflictField::AliasSet,
            ConflictReason::AliasOwned | ConflictReason::AliasSetMismatch,
        ) | (
            OperationCode::ReplaceConceptAliases | OperationCode::DeleteConcept,
            ConflictField::Snapshot,
            ConflictReason::SnapshotDrift,
        )
    )
}

fn decode_concept_conflict(
    operation: OperationCode,
    expected_id: ConceptId,
    field: ConflictField,
    reason: ConflictReason,
    identity: RecordIdentity,
    current_revision: crate::contracts::Revision,
) -> GraphError {
    let RecordIdentity::Concept(identity) = identity else {
        return invalid_concept_outcome(operation);
    };
    if !concept_conflict_is_supported(operation, field, reason)
        || (field == ConflictField::Snapshot && identity != expected_id)
    {
        return invalid_concept_outcome(operation);
    }

    GraphError::Conflict {
        operation,
        field,
        reason,
        identity: RecordIdentity::Concept(identity),
        current_revision: Some(current_revision),
    }
}

fn decode_concept_stale(
    operation: OperationCode,
    expected_id: ConceptId,
    identity: RecordIdentity,
    current_revision: crate::contracts::Revision,
) -> GraphError {
    if identity != RecordIdentity::Concept(expected_id) {
        return invalid_concept_outcome(operation);
    }

    GraphError::Conflict {
        operation,
        field: ConflictField::Revision,
        reason: ConflictReason::StaleRevision,
        identity,
        current_revision: Some(current_revision),
    }
}

fn decode_concept_missing(
    operation: OperationCode,
    expected_id: ConceptId,
    record_kind: RecordKind,
    identity: Option<RecordIdentity>,
) -> GraphError {
    if record_kind != RecordKind::Concept || identity != Some(RecordIdentity::Concept(expected_id))
    {
        return invalid_concept_outcome(operation);
    }

    GraphError::NotFound {
        operation,
        record_kind: RecordKind::Concept,
        identity,
        reason: NotFoundReason::Missing,
    }
}

fn decode_create_concept_outcome(
    input: &ValidatedCreateConcept,
    outcome: DatabaseMutationOutcome<Concept>,
) -> DatabaseResult<Concept> {
    let operation = OperationCode::CreateConcept;
    match outcome {
        DatabaseMutationOutcome::Created { record } => {
            let record = validate_concept_record(
                operation,
                record,
                Some(input.candidate_id),
                Some(1),
                Some(&input.aliases),
            )?;
            if !concept_aliases_match_input(&record, &input.aliases) {
                return Err(invalid_concept_outcome(operation));
            }
            Ok(record)
        }
        DatabaseMutationOutcome::Existing { record } => {
            validate_concept_record(operation, record, None, None, Some(&input.aliases))
        }
        DatabaseMutationOutcome::Conflict {
            field,
            reason,
            identity,
            current_revision,
        } => Err(decode_concept_conflict(
            operation,
            input.candidate_id,
            field,
            reason,
            identity,
            current_revision,
        )),
        _ => Err(invalid_concept_outcome(operation)),
    }
}

fn decode_replace_concept_aliases_outcome(
    input: &ValidatedReplaceConceptAliases,
    outcome: DatabaseMutationOutcome<Concept>,
) -> DatabaseResult<Concept> {
    let operation = OperationCode::ReplaceConceptAliases;
    match outcome {
        DatabaseMutationOutcome::Updated { record } => {
            let Some(expected_revision) = input.expected_revision.get().checked_add(1) else {
                return Err(invalid_concept_outcome(operation));
            };
            let record = validate_concept_record(
                operation,
                record,
                Some(input.concept_id),
                Some(expected_revision),
                Some(&input.aliases),
            )?;
            if !concept_aliases_match_input(&record, &input.aliases) {
                return Err(invalid_concept_outcome(operation));
            }
            Ok(record)
        }
        DatabaseMutationOutcome::Conflict {
            field,
            reason,
            identity,
            current_revision,
        } => Err(decode_concept_conflict(
            operation,
            input.concept_id,
            field,
            reason,
            identity,
            current_revision,
        )),
        DatabaseMutationOutcome::Stale {
            identity,
            current_revision,
        } => Err(decode_concept_stale(
            operation,
            input.concept_id,
            identity,
            current_revision,
        )),
        DatabaseMutationOutcome::Missing {
            record_kind,
            identity,
        } => Err(decode_concept_missing(
            operation,
            input.concept_id,
            record_kind,
            identity,
        )),
        _ => Err(invalid_concept_outcome(operation)),
    }
}

fn decode_delete_concept_outcome(
    input: &ValidatedDeleteConcept,
    outcome: DatabaseMutationOutcome<Concept>,
) -> DatabaseResult<()> {
    let operation = OperationCode::DeleteConcept;
    match outcome {
        DatabaseMutationOutcome::Deleted => Ok(()),
        DatabaseMutationOutcome::Conflict {
            field,
            reason,
            identity,
            current_revision,
        } => Err(decode_concept_conflict(
            operation,
            input.concept_id,
            field,
            reason,
            identity,
            current_revision,
        )),
        DatabaseMutationOutcome::Stale {
            identity,
            current_revision,
        } => Err(decode_concept_stale(
            operation,
            input.concept_id,
            identity,
            current_revision,
        )),
        DatabaseMutationOutcome::Referenced { reference }
            if reference == ReferencedRecord::Concept(input.concept_id) =>
        {
            Err(GraphError::Referenced {
                operation,
                record: reference,
                reason: ReferenceReason::InUse,
            })
        }
        DatabaseMutationOutcome::Missing {
            record_kind,
            identity,
        } => Err(decode_concept_missing(
            operation,
            input.concept_id,
            record_kind,
            identity,
        )),
        _ => Err(invalid_concept_outcome(operation)),
    }
}

fn invalid_assertion_outcome(operation: OperationCode) -> GraphError {
    invalid_response(operation, BackendFailureReason::UnsupportedValue)
}

fn assertion_record_matches_identity(
    record: &Assertion,
    expected: SemanticAssertionIdentity,
) -> bool {
    record.subject_concept_id == expected.subject_concept_id
        && record.relation_type == expected.relation_type
        && record.object_concept_id == expected.object_concept_id
}

fn assertion_record_has_canonical_endpoints(record: &Assertion) -> bool {
    !record.relation_type.is_symmetric()
        || record.subject_concept_id.as_uuid() < record.object_concept_id.as_uuid()
}

fn assertion_evidence_contains_sources(
    record: &Assertion,
    expected_sources: &[ValidatedSourceReference],
) -> bool {
    expected_sources
        .iter()
        .all(|source| record.evidence.contains(&source.source))
}

fn assertion_evidence_matches_sources(
    record: &Assertion,
    expected_sources: &[ValidatedSourceReference],
) -> bool {
    record.evidence.len() == expected_sources.len()
        && assertion_evidence_contains_sources(record, expected_sources)
}

fn validate_assertion_record(
    operation: OperationCode,
    record: Assertion,
    expected_id: Option<AssertionId>,
    expected_revision: Option<i64>,
    expected_identity: Option<SemanticAssertionIdentity>,
    expected_sources: Option<&[ValidatedSourceReference]>,
) -> DatabaseResult<Assertion> {
    if !assertion_record_has_canonical_endpoints(&record)
        || expected_id.is_some_and(|id| record.id != id)
        || expected_revision.is_some_and(|revision| record.revision.get() != revision)
        || expected_identity
            .is_some_and(|identity| !assertion_record_matches_identity(&record, identity))
        || expected_sources
            .is_some_and(|sources| !assertion_evidence_contains_sources(&record, sources))
    {
        return Err(invalid_assertion_outcome(operation));
    }

    Ok(record)
}

fn decode_assertion_conflict(
    operation: OperationCode,
    expected_id: AssertionId,
    field: ConflictField,
    reason: ConflictReason,
    identity: RecordIdentity,
    current_revision: crate::contracts::Revision,
) -> GraphError {
    let RecordIdentity::Assertion(identity_id) = identity else {
        return invalid_assertion_outcome(operation);
    };

    let identity_is_valid = match (operation, field, reason) {
        (
            OperationCode::CreateAssertion,
            ConflictField::SemanticAssertion,
            ConflictReason::DuplicateSemanticAssertion,
        ) => identity_id == expected_id,
        (
            OperationCode::UpdateAssertion,
            ConflictField::SemanticAssertion,
            ConflictReason::DuplicateSemanticAssertion,
        ) => identity_id != expected_id,
        (
            OperationCode::UpdateAssertion | OperationCode::DeleteAssertion,
            ConflictField::Snapshot,
            ConflictReason::SnapshotDrift,
        ) => identity_id == expected_id,
        (
            OperationCode::AddEvidence | OperationCode::RemoveEvidence,
            ConflictField::Snapshot,
            ConflictReason::SnapshotDrift,
        ) => identity_id == expected_id,
        _ => false,
    };
    if !identity_is_valid {
        return invalid_assertion_outcome(operation);
    }

    GraphError::Conflict {
        operation,
        field,
        reason,
        identity: RecordIdentity::Assertion(identity_id),
        current_revision: Some(current_revision),
    }
}

fn decode_assertion_stale(
    operation: OperationCode,
    expected_id: AssertionId,
    identity: RecordIdentity,
    current_revision: crate::contracts::Revision,
) -> GraphError {
    if operation != OperationCode::UpdateAssertion && operation != OperationCode::DeleteAssertion {
        return invalid_assertion_outcome(operation);
    }
    if identity != RecordIdentity::Assertion(expected_id) {
        return invalid_assertion_outcome(operation);
    }

    GraphError::Conflict {
        operation,
        field: ConflictField::Revision,
        reason: ConflictReason::StaleRevision,
        identity,
        current_revision: Some(current_revision),
    }
}

fn decode_assertion_missing(
    operation: OperationCode,
    expected_assertion_id: Option<AssertionId>,
    expected_concept_ids: &[ConceptId],
    record_kind: RecordKind,
    identity: Option<RecordIdentity>,
) -> GraphError {
    match (record_kind, identity) {
        (RecordKind::Concept, Some(RecordIdentity::Concept(id)))
            if expected_concept_ids.contains(&id) =>
        {
            GraphError::NotFound {
                operation,
                record_kind: RecordKind::Concept,
                identity: Some(RecordIdentity::Concept(id)),
                reason: NotFoundReason::Missing,
            }
        }
        (RecordKind::Assertion, Some(RecordIdentity::Assertion(id)))
            if expected_assertion_id == Some(id) =>
        {
            GraphError::NotFound {
                operation,
                record_kind: RecordKind::Assertion,
                identity: Some(RecordIdentity::Assertion(id)),
                reason: NotFoundReason::Missing,
            }
        }
        (RecordKind::SourceReference, None) if operation == OperationCode::CreateAssertion => {
            GraphError::NotFound {
                operation,
                record_kind: RecordKind::SourceReference,
                identity: None,
                reason: NotFoundReason::Missing,
            }
        }
        _ => invalid_assertion_outcome(operation),
    }
}

fn decode_create_assertion_outcome(
    input: &ValidatedCreateAssertion,
    outcome: DatabaseMutationOutcome<Assertion>,
) -> DatabaseResult<Assertion> {
    let operation = OperationCode::CreateAssertion;
    let expected_sources = input.supporting_sources.sources.as_slice();
    match outcome {
        DatabaseMutationOutcome::Created { record } => {
            let record = validate_assertion_record(
                operation,
                record,
                Some(input.candidate_id),
                Some(1),
                Some(input.identity),
                Some(expected_sources),
            )?;
            if !assertion_evidence_matches_sources(&record, expected_sources) {
                return Err(invalid_assertion_outcome(operation));
            }
            Ok(record)
        }
        DatabaseMutationOutcome::Existing { record } => validate_assertion_record(
            operation,
            record,
            None,
            None,
            Some(input.identity),
            Some(expected_sources),
        ),
        DatabaseMutationOutcome::Conflict {
            field,
            reason,
            identity,
            current_revision,
        } => Err(decode_assertion_conflict(
            operation,
            input.candidate_id,
            field,
            reason,
            identity,
            current_revision,
        )),
        DatabaseMutationOutcome::Missing {
            record_kind,
            identity,
        } => Err(decode_assertion_missing(
            operation,
            None,
            &[
                input.identity.subject_concept_id,
                input.identity.object_concept_id,
            ],
            record_kind,
            identity,
        )),
        DatabaseMutationOutcome::EvidenceLimit => Err(GraphError::LimitExceeded {
            operation,
            resource: LimitResource::AssertionEvidenceCount,
            reason: LimitReason::Exceeded,
        }),
        _ => Err(invalid_assertion_outcome(operation)),
    }
}

fn decode_update_assertion_outcome(
    input: &ValidatedUpdateAssertion,
    outcome: DatabaseMutationOutcome<Assertion>,
) -> DatabaseResult<Assertion> {
    let operation = OperationCode::UpdateAssertion;
    match outcome {
        DatabaseMutationOutcome::Updated { record } => {
            let Some(expected_revision) = input.expected_revision.get().checked_add(1) else {
                return Err(invalid_assertion_outcome(operation));
            };
            validate_assertion_record(
                operation,
                record,
                Some(input.assertion_id),
                Some(expected_revision),
                Some(input.identity),
                None,
            )
        }
        DatabaseMutationOutcome::Conflict {
            field,
            reason,
            identity,
            current_revision,
        } => Err(decode_assertion_conflict(
            operation,
            input.assertion_id,
            field,
            reason,
            identity,
            current_revision,
        )),
        DatabaseMutationOutcome::Stale {
            identity,
            current_revision,
        } => Err(decode_assertion_stale(
            operation,
            input.assertion_id,
            identity,
            current_revision,
        )),
        DatabaseMutationOutcome::Missing {
            record_kind,
            identity,
        } => Err(decode_assertion_missing(
            operation,
            Some(input.assertion_id),
            &[
                input.identity.subject_concept_id,
                input.identity.object_concept_id,
            ],
            record_kind,
            identity,
        )),
        _ => Err(invalid_assertion_outcome(operation)),
    }
}

fn decode_delete_assertion_outcome(
    input: &ValidatedDeleteAssertion,
    outcome: DatabaseMutationOutcome<Assertion>,
) -> DatabaseResult<()> {
    let operation = OperationCode::DeleteAssertion;
    match outcome {
        DatabaseMutationOutcome::Deleted => Ok(()),
        DatabaseMutationOutcome::Conflict {
            field,
            reason,
            identity,
            current_revision,
        } => Err(decode_assertion_conflict(
            operation,
            input.assertion_id,
            field,
            reason,
            identity,
            current_revision,
        )),
        DatabaseMutationOutcome::Stale {
            identity,
            current_revision,
        } => Err(decode_assertion_stale(
            operation,
            input.assertion_id,
            identity,
            current_revision,
        )),
        DatabaseMutationOutcome::Missing {
            record_kind,
            identity,
        } => Err(decode_assertion_missing(
            operation,
            Some(input.assertion_id),
            &[],
            record_kind,
            identity,
        )),
        _ => Err(invalid_assertion_outcome(operation)),
    }
}

fn decode_evidence_missing(
    operation: OperationCode,
    expected_assertion_id: AssertionId,
    record_kind: RecordKind,
    identity: Option<RecordIdentity>,
) -> GraphError {
    match (record_kind, identity) {
        (RecordKind::Assertion, None) => GraphError::NotFound {
            operation,
            record_kind: RecordKind::Assertion,
            identity: Some(RecordIdentity::Assertion(expected_assertion_id)),
            reason: NotFoundReason::Missing,
        },
        (RecordKind::Assertion, Some(RecordIdentity::Assertion(identity)))
            if identity == expected_assertion_id =>
        {
            GraphError::NotFound {
                operation,
                record_kind: RecordKind::Assertion,
                identity: Some(RecordIdentity::Assertion(identity)),
                reason: NotFoundReason::Missing,
            }
        }
        (RecordKind::SourceReference, None) => GraphError::NotFound {
            operation,
            record_kind: RecordKind::SourceReference,
            identity: None,
            reason: NotFoundReason::Missing,
        },
        (RecordKind::AssertionEvidence, None) if operation == OperationCode::RemoveEvidence => {
            GraphError::NotFound {
                operation,
                record_kind: RecordKind::AssertionEvidence,
                identity: None,
                reason: NotFoundReason::Missing,
            }
        }
        _ => invalid_assertion_outcome(operation),
    }
}

fn decode_add_evidence_outcome(
    input: &ValidatedAssertionEvidence,
    outcome: DatabaseMutationOutcome<AssertionEvidence>,
) -> DatabaseResult<AssertionEvidence> {
    let operation = OperationCode::AddEvidence;
    match outcome {
        DatabaseMutationOutcome::Created { record }
        | DatabaseMutationOutcome::Existing { record } => {
            if record.assertion_id != input.assertion_id || record.source != input.source.source {
                return Err(invalid_assertion_outcome(operation));
            }
            Ok(record)
        }
        DatabaseMutationOutcome::Conflict {
            field,
            reason,
            identity,
            current_revision,
        } => Err(decode_assertion_conflict(
            operation,
            input.assertion_id,
            field,
            reason,
            identity,
            current_revision,
        )),
        DatabaseMutationOutcome::Missing {
            record_kind,
            identity,
        } => Err(decode_evidence_missing(
            operation,
            input.assertion_id,
            record_kind,
            identity,
        )),
        DatabaseMutationOutcome::EvidenceLimit => Err(GraphError::LimitExceeded {
            operation,
            resource: LimitResource::AssertionEvidenceCount,
            reason: LimitReason::Exceeded,
        }),
        _ => Err(invalid_assertion_outcome(operation)),
    }
}

fn decode_remove_evidence_outcome(
    input: &ValidatedAssertionEvidence,
    outcome: DatabaseMutationOutcome<AssertionEvidence>,
) -> DatabaseResult<()> {
    let operation = OperationCode::RemoveEvidence;
    match outcome {
        DatabaseMutationOutcome::Deleted => Ok(()),
        DatabaseMutationOutcome::Conflict {
            field,
            reason,
            identity,
            current_revision,
        } => Err(decode_assertion_conflict(
            operation,
            input.assertion_id,
            field,
            reason,
            identity,
            current_revision,
        )),
        DatabaseMutationOutcome::Missing {
            record_kind,
            identity,
        } => Err(decode_evidence_missing(
            operation,
            input.assertion_id,
            record_kind,
            identity,
        )),
        DatabaseMutationOutcome::WouldOrphanEvidence {
            assertion_id,
            revision,
        } if assertion_id == input.assertion_id => Err(GraphError::WouldOrphanAssertion {
            operation,
            assertion_id,
            revision,
        }),
        _ => Err(invalid_assertion_outcome(operation)),
    }
}

#[derive(Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum SourceMutationOutcome<T> {
    Created {
        record: T,
    },
    Existing {
        record: T,
    },
    Deleted,
    Conflict {
        field: ConflictField,
        reason: ConflictReason,
    },
    Referenced {
        reference_kind: String,
    },
    Missing {
        record_kind: RecordKind,
        #[serde(default)]
        identity: Option<RecordIdentity>,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceIdentityRecord {
    source_kind: String,
    external_id: String,
    external_version: Value,
}

impl SourceIdentityRecord {
    fn into_source_reference(self, operation: OperationCode) -> DatabaseResult<SourceReference> {
        let input = match self.source_kind.as_str() {
            "memory_version" => {
                let Some(version) = self.external_version.as_i64() else {
                    return Err(invalid_response(
                        operation,
                        BackendFailureReason::UnsupportedValue,
                    ));
                };
                SourceReferenceInput::MemoryVersion {
                    memory_id: self.external_id,
                    version,
                }
            }
            "session_record" if self.external_version.is_null() => {
                SourceReferenceInput::SessionRecord {
                    session_record_id: self.external_id,
                }
            }
            _ => {
                return Err(invalid_response(
                    operation,
                    BackendFailureReason::UnsupportedValue,
                ));
            }
        };

        SourceReference::try_from(input)
            .map_err(|_| invalid_response(operation, BackendFailureReason::UnsupportedValue))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceReferenceMutationRecord {
    source_ref_id: i64,
    source_kind: String,
    external_id: String,
    external_version: Value,
}

impl SourceReferenceMutationRecord {
    fn into_source_reference(
        self,
        operation: OperationCode,
        expected: &SourceReference,
    ) -> DatabaseResult<SourceReference> {
        if self.source_ref_id <= 0 {
            return Err(invalid_response(
                operation,
                BackendFailureReason::UnsupportedValue,
            ));
        }

        let source = SourceIdentityRecord {
            source_kind: self.source_kind,
            external_id: self.external_id,
            external_version: self.external_version,
        }
        .into_source_reference(operation)?;
        if &source != expected {
            return Err(invalid_response(
                operation,
                BackendFailureReason::UnsupportedValue,
            ));
        }

        Ok(source)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConceptMentionMutationRecord {
    concept_id: ConceptId,
    source_ref_id: i64,
    source: SourceIdentityRecord,
}

impl ConceptMentionMutationRecord {
    fn into_mention(
        self,
        operation: OperationCode,
        expected_concept_id: ConceptId,
        expected_source: &SourceReference,
    ) -> DatabaseResult<ConceptMention> {
        if self.source_ref_id <= 0 || self.concept_id != expected_concept_id {
            return Err(invalid_response(
                operation,
                BackendFailureReason::UnsupportedValue,
            ));
        }

        let source = self.source.into_source_reference(operation)?;
        if &source != expected_source {
            return Err(invalid_response(
                operation,
                BackendFailureReason::UnsupportedValue,
            ));
        }

        Ok(ConceptMention {
            concept_id: self.concept_id,
            source,
        })
    }
}

fn source_parameter_values(source: &SourceReference) -> Vec<Value> {
    match source {
        SourceReference::MemoryVersion { memory_id, version } => vec![
            json!("memory_version"),
            json!(memory_id.as_str()),
            json!(version.get().to_string()),
        ],
        SourceReference::SessionRecord { session_record_id } => vec![
            json!("session_record"),
            json!(session_record_id.as_str()),
            Value::Null,
        ],
    }
}

fn source_parameters(source: &SourceReference) -> PositionalParameters {
    PositionalParameters::from(source_parameter_values(source))
}

fn mention_parameters(concept_id: ConceptId, source: &SourceReference) -> PositionalParameters {
    let mut parameters = vec![json!(concept_id.as_uuid().to_string())];
    parameters.extend(source_parameter_values(source));
    PositionalParameters::from(parameters)
}

fn evidence_parameters(input: &ValidatedAssertionEvidence) -> PositionalParameters {
    let mut parameters = vec![json!(input.assertion_id.as_uuid().to_string())];
    parameters.extend(source_parameter_values(&input.source.source));
    PositionalParameters::from(parameters)
}

fn source_reference_json(source: &SourceReference) -> Value {
    match source {
        SourceReference::MemoryVersion { memory_id, version } => json!({
            "kind": "memory_version",
            "memory_id": memory_id.as_str(),
            "version": version.get(),
        }),
        SourceReference::SessionRecord { session_record_id } => json!({
            "kind": "session_record",
            "session_record_id": session_record_id.as_str(),
        }),
    }
}

fn create_assertion_parameters(input: &ValidatedCreateAssertion) -> PositionalParameters {
    PositionalParameters::from(vec![
        json!(input.candidate_id.as_uuid().to_string()),
        json!(input.identity.subject_concept_id.as_uuid().to_string()),
        json!(input.identity.relation_type.as_str()),
        json!(input.identity.object_concept_id.as_uuid().to_string()),
        Value::Array(
            input
                .supporting_sources
                .sources
                .iter()
                .map(|source| source_reference_json(&source.source))
                .collect(),
        ),
    ])
}

fn update_assertion_parameters(input: &ValidatedUpdateAssertion) -> PositionalParameters {
    PositionalParameters::from(vec![
        json!(input.assertion_id.as_uuid().to_string()),
        json!(input.expected_revision.get().to_string()),
        json!(input.identity.subject_concept_id.as_uuid().to_string()),
        json!(input.identity.relation_type.as_str()),
        json!(input.identity.object_concept_id.as_uuid().to_string()),
    ])
}

fn delete_assertion_parameters(input: &ValidatedDeleteAssertion) -> PositionalParameters {
    PositionalParameters::from(vec![
        json!(input.assertion_id.as_uuid().to_string()),
        json!(input.expected_revision.get().to_string()),
    ])
}

async fn execute_source_outcome_with<T, F, Fut>(
    database: &DatabaseTarget,
    timeouts: DatabaseTimeouts,
    operation: OperationCode,
    sql: &'static str,
    params: PositionalParameters,
    invoke: F,
) -> DatabaseResult<SourceMutationOutcome<T>>
where
    T: DeserializeOwned,
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    let response = invoke(execute_request(database, timeouts, sql, params))
        .await
        .map_err(|error| invocation_failure(operation, error))?;
    decode_source_mutation_response(operation, response)
}

fn decode_source_mutation_response<T: DeserializeOwned>(
    operation: OperationCode,
    response: Value,
) -> DatabaseResult<SourceMutationOutcome<T>> {
    let row = decode_execute_response_row(operation, response)?;
    let outcome = row
        .get("outcome")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_response(operation, BackendFailureReason::MalformedResponse))?;

    let expected_fields: &[&str] = match outcome {
        "created" | "existing" => &["outcome", "record"],
        "deleted" => &["outcome"],
        "conflict" => &["outcome", "field", "reason"],
        "referenced" => &["outcome", "reference_kind"],
        "missing" => match row.get("record_kind").and_then(Value::as_str) {
            Some("concept") => &["outcome", "record_kind", "identity"],
            Some("source_reference" | "concept_mention") => &["outcome", "record_kind"],
            _ => {
                return Err(invalid_response(
                    operation,
                    BackendFailureReason::UnsupportedValue,
                ));
            }
        },
        "updated" | "stale" | "evidence_limit" | "would_orphan_evidence" => {
            return Err(invalid_response(
                operation,
                BackendFailureReason::UnsupportedValue,
            ));
        }
        _ => {
            return Err(invalid_response(
                operation,
                BackendFailureReason::UnknownOutcome,
            ));
        }
    };
    if !has_exact_fields(&row, expected_fields) {
        return Err(invalid_response(
            operation,
            BackendFailureReason::UnsupportedValue,
        ));
    }

    serde_json::from_value(Value::Object(row))
        .map_err(|_| invalid_response(operation, BackendFailureReason::UnsupportedValue))
}

fn decode_register_source_outcome(
    input: &ValidatedSourceReference,
    outcome: SourceMutationOutcome<SourceReferenceMutationRecord>,
) -> DatabaseResult<SourceReference> {
    let operation = OperationCode::RegisterSource;
    let record = match outcome {
        SourceMutationOutcome::Created { record } | SourceMutationOutcome::Existing { record } => {
            record
        }
        _ => {
            return Err(invalid_response(
                operation,
                BackendFailureReason::UnsupportedValue,
            ));
        }
    };

    record.into_source_reference(operation, &input.source)
}

fn decode_delete_source_outcome(outcome: SourceMutationOutcome<Value>) -> DatabaseResult<()> {
    let operation = OperationCode::DeleteSource;
    match outcome {
        SourceMutationOutcome::Deleted => Ok(()),
        SourceMutationOutcome::Missing {
            record_kind: RecordKind::SourceReference,
            identity: None,
        } => Err(GraphError::NotFound {
            operation,
            record_kind: RecordKind::SourceReference,
            identity: None,
            reason: NotFoundReason::Missing,
        }),
        SourceMutationOutcome::Referenced { reference_kind }
            if reference_kind == "source_reference" =>
        {
            Err(GraphError::Referenced {
                operation,
                record: ReferencedRecord::SourceReference,
                reason: ReferenceReason::InUse,
            })
        }
        SourceMutationOutcome::Conflict { field, reason } => {
            Err(decode_source_conflict(operation, field, reason))
        }
        _ => Err(invalid_response(
            operation,
            BackendFailureReason::UnsupportedValue,
        )),
    }
}

fn decode_create_mention_outcome(
    input: &ValidatedConceptMention,
    outcome: SourceMutationOutcome<ConceptMentionMutationRecord>,
) -> DatabaseResult<ConceptMention> {
    let operation = OperationCode::CreateMention;
    match outcome {
        SourceMutationOutcome::Created { record } | SourceMutationOutcome::Existing { record } => {
            record.into_mention(operation, input.concept_id, &input.source.source)
        }
        SourceMutationOutcome::Missing {
            record_kind: RecordKind::Concept,
            identity: Some(RecordIdentity::Concept(identity)),
        } if identity == input.concept_id => Err(GraphError::NotFound {
            operation,
            record_kind: RecordKind::Concept,
            identity: Some(RecordIdentity::Concept(identity)),
            reason: NotFoundReason::Missing,
        }),
        SourceMutationOutcome::Missing {
            record_kind: RecordKind::SourceReference,
            identity: None,
        } => Err(GraphError::NotFound {
            operation,
            record_kind: RecordKind::SourceReference,
            identity: None,
            reason: NotFoundReason::Missing,
        }),
        SourceMutationOutcome::Conflict { field, reason } => {
            Err(decode_source_conflict(operation, field, reason))
        }
        _ => Err(invalid_response(
            operation,
            BackendFailureReason::UnsupportedValue,
        )),
    }
}

fn decode_delete_mention_outcome(
    input: &ValidatedConceptMention,
    outcome: SourceMutationOutcome<Value>,
) -> DatabaseResult<()> {
    let operation = OperationCode::DeleteMention;
    match outcome {
        SourceMutationOutcome::Deleted => Ok(()),
        SourceMutationOutcome::Missing {
            record_kind: RecordKind::Concept,
            identity: Some(RecordIdentity::Concept(identity)),
        } if identity == input.concept_id => Err(GraphError::NotFound {
            operation,
            record_kind: RecordKind::Concept,
            identity: Some(RecordIdentity::Concept(identity)),
            reason: NotFoundReason::Missing,
        }),
        SourceMutationOutcome::Missing {
            record_kind: RecordKind::SourceReference,
            identity: None,
        } => Err(GraphError::NotFound {
            operation,
            record_kind: RecordKind::SourceReference,
            identity: None,
            reason: NotFoundReason::Missing,
        }),
        SourceMutationOutcome::Missing {
            record_kind: RecordKind::ConceptMention,
            identity: None,
        } => Err(GraphError::NotFound {
            operation,
            record_kind: RecordKind::ConceptMention,
            identity: None,
            reason: NotFoundReason::Missing,
        }),
        SourceMutationOutcome::Conflict { field, reason } => {
            Err(decode_source_conflict(operation, field, reason))
        }
        _ => Err(invalid_response(
            operation,
            BackendFailureReason::UnsupportedValue,
        )),
    }
}

fn decode_source_conflict(
    operation: OperationCode,
    field: ConflictField,
    reason: ConflictReason,
) -> GraphError {
    if field == ConflictField::Snapshot && reason == ConflictReason::SnapshotDrift {
        // The source routines report identity-free conflicts; the public error
        // contract has no source-safe conflict identity to carry.
        database_failure_for_operation(operation)
    } else {
        invalid_response(operation, BackendFailureReason::UnsupportedValue)
    }
}

struct PositionalParameters(Vec<Value>);

impl From<Vec<Value>> for PositionalParameters {
    fn from(values: Vec<Value>) -> Self {
        Self(values)
    }
}

impl PositionalParameters {
    fn into_json(self) -> Value {
        Value::Array(self.0)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecuteResponse {
    affected_rows: u64,
    last_insert_id: Value,
    returned_rows: Vec<Map<String, Value>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryResponse {
    rows: Vec<Map<String, Value>>,
    row_count: u64,
    columns: Vec<QueryColumn>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryColumn {
    name: String,
    #[serde(rename = "type")]
    _column_type: String,
}

#[derive(Deserialize)]
struct WorkerErrorEnvelope {
    code: String,
}

fn timeout_millis(timeout: Duration) -> u64 {
    // DatabaseTimeouts bounds invocation timeouts at 60 seconds.
    let milliseconds = timeout.as_millis();
    let rounded_up = if timeout.subsec_nanos().is_multiple_of(1_000_000) {
        milliseconds
    } else {
        milliseconds + 1
    };
    u64::try_from(rounded_up).expect("validated database timeout fits in u64 millis")
}

fn query_invocation_timeout_millis(timeouts: DatabaseTimeouts) -> u64 {
    let query_timeout = timeout_millis(timeouts.query());
    let invocation_timeout = timeout_millis(timeouts.invocation());
    let strictly_longer = query_timeout
        .checked_add(1)
        .expect("validated query timeout fits in u64 millis");
    invocation_timeout.max(strictly_longer)
}

fn execute_request(
    database: &DatabaseTarget,
    timeouts: DatabaseTimeouts,
    sql: &'static str,
    params: PositionalParameters,
) -> TriggerRequest {
    TriggerRequest {
        function_id: DATABASE_EXECUTE_FUNCTION_ID.to_owned(),
        payload: json!({
            "db": database.as_str(),
            "sql": sql,
            "params": params.into_json(),
        }),
        action: None,
        timeout_ms: Some(timeout_millis(timeouts.invocation())),
    }
}

fn query_request(
    database: &DatabaseTarget,
    timeouts: DatabaseTimeouts,
    sql: &'static str,
    params: PositionalParameters,
) -> TriggerRequest {
    TriggerRequest {
        function_id: DATABASE_QUERY_FUNCTION_ID.to_owned(),
        payload: json!({
            "db": database.as_str(),
            "sql": sql,
            "params": params.into_json(),
            "timeout_ms": timeout_millis(timeouts.query()),
        }),
        action: None,
        timeout_ms: Some(query_invocation_timeout_millis(timeouts)),
    }
}

async fn execute_with<T, F, Fut>(
    database: &DatabaseTarget,
    timeouts: DatabaseTimeouts,
    operation: OperationCode,
    sql: &'static str,
    params: PositionalParameters,
    invoke: F,
) -> DatabaseResult<DatabaseMutationOutcome<T>>
where
    T: DeserializeOwned,
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    let response = invoke(execute_request(database, timeouts, sql, params))
        .await
        .map_err(|error| invocation_failure(operation, error))?;
    decode_mutation_response(operation, response)
}

async fn query_with<T, F, Fut>(
    database: &DatabaseTarget,
    timeouts: DatabaseTimeouts,
    operation: OperationCode,
    sql: &'static str,
    params: PositionalParameters,
    invoke: F,
) -> DatabaseResult<DatabaseReadOutcome<T>>
where
    T: DeserializeOwned + serde::Serialize,
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    let response = invoke(query_request(database, timeouts, sql, params))
        .await
        .map_err(|error| invocation_failure(operation, error))?;
    decode_query_response(operation, response)
}

fn decode_mutation_response<T: DeserializeOwned>(
    operation: OperationCode,
    response: Value,
) -> DatabaseResult<DatabaseMutationOutcome<T>> {
    let row = decode_execute_response_row(operation, response)?;
    let outcome = row
        .get("outcome")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_response(operation, BackendFailureReason::MalformedResponse))?;
    let is_would_orphan_evidence = outcome == "would_orphan_evidence";
    if !matches!(
        outcome,
        "created"
            | "existing"
            | "updated"
            | "deleted"
            | "conflict"
            | "stale"
            | "referenced"
            | "missing"
            | "evidence_limit"
            | "would_orphan_evidence"
    ) {
        return Err(invalid_response(
            operation,
            BackendFailureReason::UnknownOutcome,
        ));
    }

    let expected_fields: &[&str] =
        match outcome {
            "created" | "existing" | "updated" => &["outcome", "record"],
            "deleted" | "evidence_limit" => &["outcome"],
            "conflict" => &["outcome", "field", "reason", "identity", "current_revision"],
            "stale" => &["outcome", "identity", "current_revision"],
            "referenced" => &["outcome", "reference"],
            "missing" => match row.get("record_kind").and_then(Value::as_str) {
                Some("concept") if row.contains_key("identity") => {
                    &["outcome", "record_kind", "identity"]
                }
                Some(
                    "assertion" | "source_reference" | "concept_mention" | "assertion_evidence",
                ) if row.contains_key("identity") => &["outcome", "record_kind", "identity"],
                Some(
                    "assertion" | "source_reference" | "concept_mention" | "assertion_evidence",
                ) => &["outcome", "record_kind"],
                _ => {
                    return Err(invalid_response(
                        operation,
                        BackendFailureReason::UnsupportedValue,
                    ));
                }
            },
            "would_orphan_evidence" => &["outcome", "assertion_id", "current_revision"],
            _ => unreachable!("the supported outcome codes were checked"),
        };
    if !has_exact_fields(&row, expected_fields) {
        return Err(invalid_response(
            operation,
            BackendFailureReason::UnsupportedValue,
        ));
    }

    let mut row = row;
    if is_would_orphan_evidence {
        let current_revision = row
            .remove("current_revision")
            .expect("the exact orphan outcome fields were checked");
        row.insert("revision".to_owned(), current_revision);
    }

    serde_json::from_value(Value::Object(row))
        .map_err(|_| invalid_response(operation, BackendFailureReason::UnsupportedValue))
}

fn decode_execute_response_row(
    operation: OperationCode,
    response: Value,
) -> DatabaseResult<Map<String, Value>> {
    let response = serde_json::from_value::<ExecuteResponse>(response)
        .map_err(|_| invalid_response(operation, BackendFailureReason::MalformedResponse))?;

    if response.affected_rows != 1
        || !response.last_insert_id.is_null()
        || response.returned_rows.len() != 1
    {
        return Err(database_failure_for_operation(operation));
    }

    Ok(response
        .returned_rows
        .into_iter()
        .next()
        .expect("the mutation outcome count was checked"))
}

fn has_exact_fields(row: &Map<String, Value>, expected: &[&str]) -> bool {
    row.len() == expected.len() && expected.iter().all(|field| row.contains_key(*field))
}

fn decode_query_response<T: DeserializeOwned + serde::Serialize>(
    operation: OperationCode,
    response: Value,
) -> DatabaseResult<DatabaseReadOutcome<T>> {
    let response = serde_json::from_value::<QueryResponse>(response)
        .map_err(|_| invalid_response(operation, BackendFailureReason::MalformedResponse))?;

    if response.row_count != 1 || response.rows.len() != 1 {
        return Err(database_failure_for_operation(operation));
    }
    if response.columns.len() != 1 || response.columns[0].name != READ_PAYLOAD_COLUMN {
        return Err(invalid_response(
            operation,
            BackendFailureReason::MalformedResponse,
        ));
    }

    let mut row = response
        .rows
        .into_iter()
        .next()
        .expect("the query row count was checked");
    if row.len() != 1 {
        return Err(invalid_response(
            operation,
            BackendFailureReason::MalformedResponse,
        ));
    }
    let payload = row
        .remove(READ_PAYLOAD_COLUMN)
        .ok_or_else(|| invalid_response(operation, BackendFailureReason::MalformedResponse))?;
    let payload_object = payload
        .as_object()
        .ok_or_else(|| invalid_response(operation, BackendFailureReason::MalformedResponse))?;
    let outcome = payload_object
        .get("outcome")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_response(operation, BackendFailureReason::MalformedResponse))?;

    match outcome {
        "complete" if payload_object.len() == 2 && payload_object.contains_key("result") => {}
        "result_too_large" | "work_exhausted" if payload_object.len() == 1 => {}
        "complete" | "result_too_large" | "work_exhausted" => {
            return Err(invalid_response(
                operation,
                BackendFailureReason::MalformedResponse,
            ));
        }
        _ => {
            return Err(invalid_response(
                operation,
                BackendFailureReason::UnknownOutcome,
            ));
        }
    }

    serde_json::from_value(payload)
        .map_err(|_| invalid_response(operation, BackendFailureReason::UnsupportedValue))
}

fn invalid_response(operation: OperationCode, reason: BackendFailureReason) -> GraphError {
    GraphError::InvalidResponse { operation, reason }
}

fn invocation_failure(operation: OperationCode, error: iii_sdk::Error) -> GraphError {
    let is_query_timeout = match error {
        iii_sdk::Error::Remote {
            code,
            message,
            stacktrace: _,
        } if code == "invocation_failed" => message
            .strip_prefix("handler error: ")
            .and_then(|body| serde_json::from_str::<WorkerErrorEnvelope>(body).ok())
            .is_some_and(|envelope| envelope.code == "QUERY_TIMEOUT"),
        _ => false,
    };

    if is_query_timeout {
        GraphError::TraversalBoundExceeded {
            operation,
            bound: TraversalBound::QueryTimeout,
            reason: TraversalReason::QueryTimeout,
        }
    } else {
        database_failure_for_operation(operation)
    }
}

fn database_failure_for_operation(operation: OperationCode) -> DatabaseError {
    GraphError::DatabaseFailure {
        operation,
        reason: BackendFailureReason::DatabaseFailure,
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) type DatabaseError = GraphError;
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) type DatabaseResult<T> = Result<T, DatabaseError>;

#[async_trait::async_trait]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) trait KnowledgeGraphDatabase: Send + Sync {
    async fn create_concept(&self, input: &ValidatedCreateConcept) -> DatabaseResult<Concept>;
    async fn replace_concept_aliases(
        &self,
        input: &ValidatedReplaceConceptAliases,
    ) -> DatabaseResult<Concept>;
    async fn delete_concept(&self, input: &ValidatedDeleteConcept) -> DatabaseResult<()>;
    async fn register_source(
        &self,
        source: &ValidatedSourceReference,
    ) -> DatabaseResult<SourceReference>;
    async fn delete_source(&self, input: &ValidatedDeleteSource) -> DatabaseResult<()>;
    async fn create_mention(
        &self,
        input: &ValidatedConceptMention,
    ) -> DatabaseResult<ConceptMention>;
    async fn delete_mention(&self, input: &ValidatedConceptMention) -> DatabaseResult<()>;
    async fn create_assertion(&self, input: &ValidatedCreateAssertion)
    -> DatabaseResult<Assertion>;
    async fn update_assertion(&self, input: &ValidatedUpdateAssertion)
    -> DatabaseResult<Assertion>;
    async fn delete_assertion(&self, input: &ValidatedDeleteAssertion) -> DatabaseResult<()>;
    async fn add_evidence(
        &self,
        input: &ValidatedAssertionEvidence,
    ) -> DatabaseResult<AssertionEvidence>;
    async fn remove_evidence(&self, input: &ValidatedAssertionEvidence) -> DatabaseResult<()>;
    async fn get_concept(&self, id: &ConceptId) -> DatabaseResult<Option<Concept>>;
    async fn resolve_alias(&self, key: &AliasKey) -> DatabaseResult<Option<Concept>>;
    async fn get_assertion(&self, id: AssertionId) -> DatabaseResult<Option<Assertion>>;
    async fn get_source(
        &self,
        source: &ValidatedSourceReference,
    ) -> DatabaseResult<Option<SourceReference>>;
    async fn neighbors(
        &self,
        query: &ValidatedNeighborQuery,
    ) -> DatabaseResult<Vec<NeighborResult>>;
    async fn related_sources(
        &self,
        query: &ValidatedRelatedSourceQuery,
    ) -> DatabaseResult<Vec<RelatedSourceResult>>;
    async fn find_paths(&self, query: &ValidatedPathQuery) -> DatabaseResult<Vec<GraphPath>>;
}

#[cfg(test)]
fn database_failure(operation: crate::contracts::OperationCode) -> DatabaseError {
    GraphError::DatabaseFailure {
        operation,
        reason: crate::contracts::BackendFailureReason::DatabaseFailure,
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::{cmp::Ordering, sync::Mutex};

    use crate::contracts::{
        Alias, AliasKey, Assertion, AssertionEvidence, AssertionId, Concept, ConceptId,
        ConceptMention, DirectionMode, GraphPath, NeighborResult, OperationCode,
        RelatedSourceResult, RelationType, Revision, SourceReference, ValidatedAlias,
        ValidatedAssertionEvidence, ValidatedConceptMention, ValidatedCreateAssertion,
        ValidatedCreateConcept, ValidatedDeleteAssertion, ValidatedDeleteConcept,
        ValidatedDeleteSource, ValidatedEvidenceSources, ValidatedNeighborQuery,
        ValidatedPathQuery, ValidatedRelatedSourceQuery, ValidatedReplaceConceptAliases,
        ValidatedSourceReference, ValidatedUpdateAssertion,
    };

    use super::{DatabaseResult, KnowledgeGraphDatabase, database_failure};

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) enum RecordedDatabaseCall {
        CreateConcept {
            candidate_id: ConceptId,
            aliases: Vec<ValidatedAlias>,
        },
        ReplaceConceptAliases {
            concept_id: ConceptId,
            expected_revision: Revision,
            aliases: Vec<ValidatedAlias>,
        },
        DeleteConcept {
            concept_id: ConceptId,
            expected_revision: Revision,
        },
        RegisterSource {
            source: SourceReference,
        },
        DeleteSource {
            source: SourceReference,
        },
        CreateMention {
            concept_id: ConceptId,
            source: SourceReference,
        },
        DeleteMention {
            concept_id: ConceptId,
            source: SourceReference,
        },
        CreateAssertion {
            candidate_id: AssertionId,
            subject_concept_id: ConceptId,
            relation_type: RelationType,
            object_concept_id: ConceptId,
            supporting_sources: Vec<SourceReference>,
        },
        UpdateAssertion {
            assertion_id: AssertionId,
            expected_revision: Revision,
            subject_concept_id: ConceptId,
            relation_type: RelationType,
            object_concept_id: ConceptId,
        },
        DeleteAssertion {
            assertion_id: AssertionId,
            expected_revision: Revision,
        },
        AddEvidence {
            assertion_id: AssertionId,
            source: SourceReference,
        },
        RemoveEvidence {
            assertion_id: AssertionId,
            source: SourceReference,
        },
        GetConcept {
            id: ConceptId,
        },
        ResolveAlias {
            key: AliasKey,
        },
        GetAssertion {
            id: AssertionId,
        },
        GetSource {
            source: SourceReference,
        },
        Neighbors {
            concept_id: ConceptId,
            direction: DirectionMode,
            relation_filter: Vec<RelationType>,
            limit: u16,
        },
        RelatedSources {
            concept_id: ConceptId,
            limit: u16,
        },
        FindPaths {
            from: ConceptId,
            to: ConceptId,
            direction: DirectionMode,
            max_depth: u8,
            max_work: u32,
            limit: u16,
        },
    }

    #[derive(Default)]
    struct CallLog(Mutex<Vec<RecordedDatabaseCall>>);

    impl CallLog {
        fn record(&self, call: RecordedDatabaseCall) {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(call);
        }

        fn calls(&self) -> Vec<RecordedDatabaseCall> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    #[derive(Default)]
    pub(crate) struct RecordingKnowledgeGraphDatabase {
        calls: CallLog,
    }

    impl RecordingKnowledgeGraphDatabase {
        pub(crate) fn calls(&self) -> Vec<RecordedDatabaseCall> {
            self.calls.calls()
        }
    }

    #[derive(Default)]
    pub(crate) struct FailingKnowledgeGraphDatabase {
        calls: CallLog,
    }

    impl FailingKnowledgeGraphDatabase {
        pub(crate) fn calls(&self) -> Vec<RecordedDatabaseCall> {
            self.calls.calls()
        }
    }

    fn recorded_sources(sources: &ValidatedEvidenceSources) -> Vec<SourceReference> {
        sources
            .sources
            .iter()
            .map(|source| source.source.clone())
            .collect()
    }

    fn create_concept_call(input: &ValidatedCreateConcept) -> RecordedDatabaseCall {
        RecordedDatabaseCall::CreateConcept {
            candidate_id: input.candidate_id,
            aliases: input.aliases.aliases.clone(),
        }
    }

    fn replace_concept_aliases_call(
        input: &ValidatedReplaceConceptAliases,
    ) -> RecordedDatabaseCall {
        RecordedDatabaseCall::ReplaceConceptAliases {
            concept_id: input.concept_id,
            expected_revision: input.expected_revision,
            aliases: input.aliases.aliases.clone(),
        }
    }

    fn delete_concept_call(input: &ValidatedDeleteConcept) -> RecordedDatabaseCall {
        RecordedDatabaseCall::DeleteConcept {
            concept_id: input.concept_id,
            expected_revision: input.expected_revision,
        }
    }

    fn register_source_call(input: &ValidatedSourceReference) -> RecordedDatabaseCall {
        RecordedDatabaseCall::RegisterSource {
            source: input.source.clone(),
        }
    }

    fn delete_source_call(input: &ValidatedDeleteSource) -> RecordedDatabaseCall {
        RecordedDatabaseCall::DeleteSource {
            source: input.source.source.clone(),
        }
    }

    fn mention_call(input: &ValidatedConceptMention, create: bool) -> RecordedDatabaseCall {
        let concept_id = input.concept_id;
        let source = input.source.source.clone();
        if create {
            RecordedDatabaseCall::CreateMention { concept_id, source }
        } else {
            RecordedDatabaseCall::DeleteMention { concept_id, source }
        }
    }

    fn create_assertion_call(input: &ValidatedCreateAssertion) -> RecordedDatabaseCall {
        RecordedDatabaseCall::CreateAssertion {
            candidate_id: input.candidate_id,
            subject_concept_id: input.identity.subject_concept_id,
            relation_type: input.identity.relation_type,
            object_concept_id: input.identity.object_concept_id,
            supporting_sources: recorded_sources(&input.supporting_sources),
        }
    }

    fn update_assertion_call(input: &ValidatedUpdateAssertion) -> RecordedDatabaseCall {
        RecordedDatabaseCall::UpdateAssertion {
            assertion_id: input.assertion_id,
            expected_revision: input.expected_revision,
            subject_concept_id: input.identity.subject_concept_id,
            relation_type: input.identity.relation_type,
            object_concept_id: input.identity.object_concept_id,
        }
    }

    fn delete_assertion_call(input: &ValidatedDeleteAssertion) -> RecordedDatabaseCall {
        RecordedDatabaseCall::DeleteAssertion {
            assertion_id: input.assertion_id,
            expected_revision: input.expected_revision,
        }
    }

    fn evidence_call(input: &ValidatedAssertionEvidence, add: bool) -> RecordedDatabaseCall {
        let assertion_id = input.assertion_id;
        let source = input.source.source.clone();
        if add {
            RecordedDatabaseCall::AddEvidence {
                assertion_id,
                source,
            }
        } else {
            RecordedDatabaseCall::RemoveEvidence {
                assertion_id,
                source,
            }
        }
    }

    fn neighbor_call(input: &ValidatedNeighborQuery) -> RecordedDatabaseCall {
        RecordedDatabaseCall::Neighbors {
            concept_id: input.concept_id,
            direction: input.direction,
            relation_filter: input.relation_filter.clone(),
            limit: input.limit,
        }
    }

    fn related_sources_call(input: &ValidatedRelatedSourceQuery) -> RecordedDatabaseCall {
        RecordedDatabaseCall::RelatedSources {
            concept_id: input.concept_id,
            limit: input.limit,
        }
    }

    fn find_paths_call(input: &ValidatedPathQuery) -> RecordedDatabaseCall {
        RecordedDatabaseCall::FindPaths {
            from: input.from,
            to: input.to,
            direction: input.direction,
            max_depth: input.max_depth,
            max_work: input.max_work,
            limit: input.limit,
        }
    }

    fn concept_from_aliases(
        id: ConceptId,
        revision: Revision,
        aliases: &[ValidatedAlias],
    ) -> Concept {
        Concept {
            id,
            revision,
            aliases: aliases
                .iter()
                .map(|alias| Alias {
                    display_text: alias.display_text().to_owned(),
                    preferred: alias.is_preferred(),
                })
                .collect(),
        }
    }

    fn next_revision(revision: Revision) -> Revision {
        revision
            .get()
            .checked_add(1)
            .and_then(|value| Revision::try_from(value).ok())
            .unwrap_or(revision)
    }

    fn source_identity_ordering(left: &SourceReference, right: &SourceReference) -> Ordering {
        match (left, right) {
            (
                SourceReference::MemoryVersion {
                    memory_id: left_id,
                    version: left_version,
                },
                SourceReference::MemoryVersion {
                    memory_id: right_id,
                    version: right_version,
                },
            ) => left_id
                .as_str()
                .cmp(right_id.as_str())
                .then_with(|| left_version.get().cmp(&right_version.get())),
            (
                SourceReference::SessionRecord {
                    session_record_id: left_id,
                },
                SourceReference::SessionRecord {
                    session_record_id: right_id,
                },
            ) => left_id.as_str().cmp(right_id.as_str()),
            (SourceReference::MemoryVersion { .. }, SourceReference::SessionRecord { .. }) => {
                Ordering::Less
            }
            (SourceReference::SessionRecord { .. }, SourceReference::MemoryVersion { .. }) => {
                Ordering::Greater
            }
        }
    }

    fn test_source() -> SourceReference {
        SourceReference::SessionRecord {
            session_record_id: crate::contracts::SessionRecordId::try_from(
                "recording-update-source".to_owned(),
            )
            .expect("test source should be valid"),
        }
    }

    #[async_trait::async_trait]
    impl KnowledgeGraphDatabase for RecordingKnowledgeGraphDatabase {
        async fn create_concept(&self, input: &ValidatedCreateConcept) -> DatabaseResult<Concept> {
            self.calls.record(create_concept_call(input));
            Ok(concept_from_aliases(
                input.candidate_id,
                Revision::try_from(1).expect("first revision is positive"),
                &input.aliases.aliases,
            ))
        }

        async fn replace_concept_aliases(
            &self,
            input: &ValidatedReplaceConceptAliases,
        ) -> DatabaseResult<Concept> {
            self.calls.record(replace_concept_aliases_call(input));
            Ok(concept_from_aliases(
                input.concept_id,
                next_revision(input.expected_revision),
                &input.aliases.aliases,
            ))
        }

        async fn delete_concept(&self, input: &ValidatedDeleteConcept) -> DatabaseResult<()> {
            self.calls.record(delete_concept_call(input));
            Ok(())
        }

        async fn register_source(
            &self,
            source: &ValidatedSourceReference,
        ) -> DatabaseResult<SourceReference> {
            self.calls.record(register_source_call(source));
            Ok(source.source.clone())
        }

        async fn delete_source(&self, input: &ValidatedDeleteSource) -> DatabaseResult<()> {
            self.calls.record(delete_source_call(input));
            Ok(())
        }

        async fn create_mention(
            &self,
            input: &ValidatedConceptMention,
        ) -> DatabaseResult<ConceptMention> {
            self.calls.record(mention_call(input, true));
            Ok(ConceptMention {
                concept_id: input.concept_id,
                source: input.source.source.clone(),
            })
        }

        async fn delete_mention(&self, input: &ValidatedConceptMention) -> DatabaseResult<()> {
            self.calls.record(mention_call(input, false));
            Ok(())
        }

        async fn create_assertion(
            &self,
            input: &ValidatedCreateAssertion,
        ) -> DatabaseResult<Assertion> {
            self.calls.record(create_assertion_call(input));
            let mut evidence = recorded_sources(&input.supporting_sources);
            evidence.sort_by(source_identity_ordering);
            Ok(Assertion {
                id: input.candidate_id,
                revision: Revision::try_from(1).expect("first revision is positive"),
                subject_concept_id: input.identity.subject_concept_id,
                relation_type: input.identity.relation_type,
                object_concept_id: input.identity.object_concept_id,
                evidence,
            })
        }

        async fn update_assertion(
            &self,
            input: &ValidatedUpdateAssertion,
        ) -> DatabaseResult<Assertion> {
            self.calls.record(update_assertion_call(input));
            Ok(Assertion {
                id: input.assertion_id,
                revision: next_revision(input.expected_revision),
                subject_concept_id: input.identity.subject_concept_id,
                relation_type: input.identity.relation_type,
                object_concept_id: input.identity.object_concept_id,
                evidence: vec![test_source()],
            })
        }

        async fn delete_assertion(&self, input: &ValidatedDeleteAssertion) -> DatabaseResult<()> {
            self.calls.record(delete_assertion_call(input));
            Ok(())
        }

        async fn add_evidence(
            &self,
            input: &ValidatedAssertionEvidence,
        ) -> DatabaseResult<AssertionEvidence> {
            self.calls.record(evidence_call(input, true));
            Ok(AssertionEvidence {
                assertion_id: input.assertion_id,
                source: input.source.source.clone(),
            })
        }

        async fn remove_evidence(&self, input: &ValidatedAssertionEvidence) -> DatabaseResult<()> {
            self.calls.record(evidence_call(input, false));
            Ok(())
        }

        async fn get_concept(&self, id: &ConceptId) -> DatabaseResult<Option<Concept>> {
            self.calls
                .record(RecordedDatabaseCall::GetConcept { id: *id });
            Ok(None)
        }

        async fn resolve_alias(&self, key: &AliasKey) -> DatabaseResult<Option<Concept>> {
            self.calls
                .record(RecordedDatabaseCall::ResolveAlias { key: key.clone() });
            Ok(None)
        }

        async fn get_assertion(&self, id: AssertionId) -> DatabaseResult<Option<Assertion>> {
            self.calls.record(RecordedDatabaseCall::GetAssertion { id });
            Ok(None)
        }

        async fn get_source(
            &self,
            source: &ValidatedSourceReference,
        ) -> DatabaseResult<Option<SourceReference>> {
            self.calls.record(RecordedDatabaseCall::GetSource {
                source: source.source.clone(),
            });
            Ok(Some(source.source.clone()))
        }

        async fn neighbors(
            &self,
            query: &ValidatedNeighborQuery,
        ) -> DatabaseResult<Vec<NeighborResult>> {
            self.calls.record(neighbor_call(query));
            Ok(Vec::new())
        }

        async fn related_sources(
            &self,
            query: &ValidatedRelatedSourceQuery,
        ) -> DatabaseResult<Vec<RelatedSourceResult>> {
            self.calls.record(related_sources_call(query));
            Ok(Vec::new())
        }

        async fn find_paths(&self, query: &ValidatedPathQuery) -> DatabaseResult<Vec<GraphPath>> {
            self.calls.record(find_paths_call(query));
            Ok(Vec::new())
        }
    }

    #[async_trait::async_trait]
    impl KnowledgeGraphDatabase for FailingKnowledgeGraphDatabase {
        async fn create_concept(&self, input: &ValidatedCreateConcept) -> DatabaseResult<Concept> {
            self.calls.record(create_concept_call(input));
            Err(database_failure(OperationCode::CreateConcept))
        }

        async fn replace_concept_aliases(
            &self,
            input: &ValidatedReplaceConceptAliases,
        ) -> DatabaseResult<Concept> {
            self.calls.record(replace_concept_aliases_call(input));
            Err(database_failure(OperationCode::ReplaceConceptAliases))
        }

        async fn delete_concept(&self, input: &ValidatedDeleteConcept) -> DatabaseResult<()> {
            self.calls.record(delete_concept_call(input));
            Err(database_failure(OperationCode::DeleteConcept))
        }

        async fn register_source(
            &self,
            source: &ValidatedSourceReference,
        ) -> DatabaseResult<SourceReference> {
            self.calls.record(register_source_call(source));
            Err(database_failure(OperationCode::RegisterSource))
        }

        async fn delete_source(&self, input: &ValidatedDeleteSource) -> DatabaseResult<()> {
            self.calls.record(delete_source_call(input));
            Err(database_failure(OperationCode::DeleteSource))
        }

        async fn create_mention(
            &self,
            input: &ValidatedConceptMention,
        ) -> DatabaseResult<ConceptMention> {
            self.calls.record(mention_call(input, true));
            Err(database_failure(OperationCode::CreateMention))
        }

        async fn delete_mention(&self, input: &ValidatedConceptMention) -> DatabaseResult<()> {
            self.calls.record(mention_call(input, false));
            Err(database_failure(OperationCode::DeleteMention))
        }

        async fn create_assertion(
            &self,
            input: &ValidatedCreateAssertion,
        ) -> DatabaseResult<Assertion> {
            self.calls.record(create_assertion_call(input));
            Err(database_failure(OperationCode::CreateAssertion))
        }

        async fn update_assertion(
            &self,
            input: &ValidatedUpdateAssertion,
        ) -> DatabaseResult<Assertion> {
            self.calls.record(update_assertion_call(input));
            Err(database_failure(OperationCode::UpdateAssertion))
        }

        async fn delete_assertion(&self, input: &ValidatedDeleteAssertion) -> DatabaseResult<()> {
            self.calls.record(delete_assertion_call(input));
            Err(database_failure(OperationCode::DeleteAssertion))
        }

        async fn add_evidence(
            &self,
            input: &ValidatedAssertionEvidence,
        ) -> DatabaseResult<AssertionEvidence> {
            self.calls.record(evidence_call(input, true));
            Err(database_failure(OperationCode::AddEvidence))
        }

        async fn remove_evidence(&self, input: &ValidatedAssertionEvidence) -> DatabaseResult<()> {
            self.calls.record(evidence_call(input, false));
            Err(database_failure(OperationCode::RemoveEvidence))
        }

        async fn get_concept(&self, id: &ConceptId) -> DatabaseResult<Option<Concept>> {
            self.calls
                .record(RecordedDatabaseCall::GetConcept { id: *id });
            Err(database_failure(OperationCode::GetConcept))
        }

        async fn resolve_alias(&self, key: &AliasKey) -> DatabaseResult<Option<Concept>> {
            self.calls
                .record(RecordedDatabaseCall::ResolveAlias { key: key.clone() });
            Err(database_failure(OperationCode::ResolveAlias))
        }

        async fn get_assertion(&self, id: AssertionId) -> DatabaseResult<Option<Assertion>> {
            self.calls.record(RecordedDatabaseCall::GetAssertion { id });
            Err(database_failure(OperationCode::GetAssertion))
        }

        async fn get_source(
            &self,
            source: &ValidatedSourceReference,
        ) -> DatabaseResult<Option<SourceReference>> {
            self.calls.record(RecordedDatabaseCall::GetSource {
                source: source.source.clone(),
            });
            Err(database_failure(OperationCode::GetSource))
        }

        async fn neighbors(
            &self,
            query: &ValidatedNeighborQuery,
        ) -> DatabaseResult<Vec<NeighborResult>> {
            self.calls.record(neighbor_call(query));
            Err(database_failure(OperationCode::Neighbors))
        }

        async fn related_sources(
            &self,
            query: &ValidatedRelatedSourceQuery,
        ) -> DatabaseResult<Vec<RelatedSourceResult>> {
            self.calls.record(related_sources_call(query));
            Err(database_failure(OperationCode::RelatedSources))
        }

        async fn find_paths(&self, query: &ValidatedPathQuery) -> DatabaseResult<Vec<GraphPath>> {
            self.calls.record(find_paths_call(query));
            Err(database_failure(OperationCode::FindPaths))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        contracts::{
            Alias, AliasKey, AssertionEvidenceInput, AssertionId, AssertionIdInput,
            BackendFailureReason, ConceptId, ConceptIdInput, CreateAssertion, DeleteAssertion,
            DeleteConcept, DeleteSource, DirectionMode, NeighborQuery, OperationCode, PathQuery,
            RelatedSourceQuery, RelationType, Revision, SourceReference, SourceReferenceInput,
            UpdateAssertion, ValidatedAssertionEvidence, ValidatedAssertionIdInput,
            ValidatedConceptIdInput, ValidatedConceptMention, ValidatedCreateAssertion,
            ValidatedCreateConcept, ValidatedDeleteAssertion, ValidatedDeleteConcept,
            ValidatedDeleteSource, ValidatedNeighborQuery, ValidatedPathQuery,
            ValidatedRelatedSourceQuery, ValidatedReplaceConceptAliases, ValidatedSourceReference,
            ValidatedUpdateAssertion,
        },
        normalization::{normalize_alias_key, normalize_alias_set},
    };
    use uuid::Uuid;

    use super::{
        DatabaseError, DatabaseResult, KnowledgeGraphDatabase,
        test_support::{
            FailingKnowledgeGraphDatabase, RecordedDatabaseCall, RecordingKnowledgeGraphDatabase,
        },
    };

    macro_rules! database_operation_test {
        (
            $test_name:ident,
            $operation:ident,
            $expected:expr,
            $recording:ident,
            $failing:ident,
            $record_call:block,
            $failure_call:block
        ) => {
            #[tokio::test]
            async fn $test_name() {
                let expected = $expected;
                let $recording = RecordingKnowledgeGraphDatabase::default();
                let recording_result: DatabaseResult<_> = $record_call;
                assert!(
                    recording_result.is_ok(),
                    "the recording database should return a successful test result"
                );
                assert_eq!($recording.calls(), vec![expected.clone()]);

                let $failing = FailingKnowledgeGraphDatabase::default();
                let failure_result: DatabaseResult<_> = $failure_call;
                assert_eq!(
                    failure_result,
                    Err(expected_failure(OperationCode::$operation))
                );
                assert_eq!($failing.calls(), vec![expected]);
            }
        };
    }

    fn expected_failure(operation: OperationCode) -> DatabaseError {
        DatabaseError::DatabaseFailure {
            operation,
            reason: BackendFailureReason::DatabaseFailure,
        }
    }

    fn concept_id(value: u128) -> ConceptId {
        const UUID_V7_BASE: u128 = 0x0000_0000_0000_7000_8000_0000_0000_0000;
        ConceptId::try_from(Uuid::from_u128(UUID_V7_BASE + value))
            .expect("fixture concept id should be UUIDv7")
    }

    fn assertion_id(value: u128) -> AssertionId {
        const UUID_V7_BASE: u128 = 0x0000_0000_0000_7000_8000_0000_0000_0000;
        AssertionId::try_from(Uuid::from_u128(UUID_V7_BASE + value))
            .expect("fixture assertion id should be UUIDv7")
    }

    fn revision(value: i64) -> Revision {
        Revision::try_from(value).expect("fixture revision should be positive")
    }

    fn concept_aliases() -> crate::contracts::ValidatedAliasSet {
        normalize_alias_set(&[
            Alias {
                display_text: "Primary  Alpha".to_owned(),
                preferred: true,
            },
            Alias {
                display_text: "βeta".to_owned(),
                preferred: false,
            },
        ])
        .expect("fixture aliases should be valid")
    }

    fn replacement_aliases() -> crate::contracts::ValidatedAliasSet {
        normalize_alias_set(&[
            Alias {
                display_text: "Revised Concept".to_owned(),
                preferred: true,
            },
            Alias {
                display_text: "Alternate Form".to_owned(),
                preferred: false,
            },
        ])
        .expect("fixture replacement aliases should be valid")
    }

    fn memory_source_input() -> SourceReferenceInput {
        SourceReferenceInput::MemoryVersion {
            memory_id: "memory-key-exact".to_owned(),
            version: 23,
        }
    }

    fn session_source_input() -> SourceReferenceInput {
        SourceReferenceInput::SessionRecord {
            session_record_id: "session-key-exact".to_owned(),
        }
    }

    fn source_reference(input: SourceReferenceInput) -> SourceReference {
        ValidatedSourceReference::try_from(input)
            .expect("fixture source should be valid")
            .source
    }

    fn validated_create_concept() -> ValidatedCreateConcept {
        ValidatedCreateConcept::new(concept_id(1), concept_aliases())
    }

    fn validated_replace_concept_aliases() -> ValidatedReplaceConceptAliases {
        ValidatedReplaceConceptAliases::new(concept_id(2), revision(41), replacement_aliases())
    }

    fn validated_delete_concept() -> ValidatedDeleteConcept {
        ValidatedDeleteConcept::from(DeleteConcept {
            concept_id: concept_id(3),
            expected_revision: revision(42),
        })
    }

    fn validated_source(input: SourceReferenceInput) -> ValidatedSourceReference {
        ValidatedSourceReference::try_from(input).expect("fixture source should be valid")
    }

    fn validated_delete_source() -> ValidatedDeleteSource {
        ValidatedDeleteSource::try_from(DeleteSource {
            source: session_source_input(),
        })
        .expect("fixture source deletion should be valid")
    }

    fn validated_mention(
        concept: ConceptId,
        source: SourceReferenceInput,
    ) -> ValidatedConceptMention {
        ValidatedConceptMention::try_from(crate::contracts::ConceptMentionInput {
            concept_id: concept,
            source,
        })
        .expect("fixture mention should be valid")
    }

    fn validated_create_assertion() -> ValidatedCreateAssertion {
        ValidatedCreateAssertion::try_from_input(
            assertion_id(5),
            CreateAssertion {
                subject_concept_id: concept_id(10),
                relation_type: RelationType::RelatedTo,
                object_concept_id: concept_id(11),
                supporting_sources: vec![memory_source_input(), session_source_input()],
            },
        )
        .expect("fixture assertion should be valid")
    }

    fn validated_update_assertion() -> ValidatedUpdateAssertion {
        ValidatedUpdateAssertion::try_from_input(UpdateAssertion {
            assertion_id: assertion_id(6),
            expected_revision: revision(43),
            subject_concept_id: concept_id(12),
            relation_type: RelationType::DependsOn,
            object_concept_id: concept_id(13),
        })
        .expect("fixture assertion update should be valid")
    }

    fn validated_delete_assertion() -> ValidatedDeleteAssertion {
        ValidatedDeleteAssertion::from(DeleteAssertion {
            assertion_id: assertion_id(7),
            expected_revision: revision(44),
        })
    }

    fn validated_evidence(
        assertion: AssertionId,
        source: SourceReferenceInput,
    ) -> ValidatedAssertionEvidence {
        ValidatedAssertionEvidence::try_from(AssertionEvidenceInput {
            assertion_id: assertion,
            source,
        })
        .expect("fixture evidence should be valid")
    }

    fn validated_concept_id_input(value: u128) -> ValidatedConceptIdInput {
        ValidatedConceptIdInput::try_from(ConceptIdInput {
            id: concept_id(value).as_uuid().to_string(),
        })
        .expect("fixture concept lookup should be valid")
    }

    fn validated_assertion_id_input(value: u128) -> ValidatedAssertionIdInput {
        ValidatedAssertionIdInput::try_from(AssertionIdInput {
            id: assertion_id(value).as_uuid().to_string(),
        })
        .expect("fixture assertion lookup should be valid")
    }

    fn alias_key() -> AliasKey {
        normalize_alias_key("Primary  Alpha").expect("fixture alias key should be valid")
    }

    fn validated_neighbor_query() -> ValidatedNeighborQuery {
        ValidatedNeighborQuery::try_from(NeighborQuery {
            concept_id: concept_id(20),
            direction: DirectionMode::Incoming,
            relation_filter: vec![RelationType::Uses, RelationType::IsA, RelationType::Uses],
            limit: 17,
        })
        .expect("fixture neighbor query should be valid")
    }

    fn validated_related_source_query() -> ValidatedRelatedSourceQuery {
        ValidatedRelatedSourceQuery::try_from(RelatedSourceQuery {
            concept_id: concept_id(21),
            limit: 18,
        })
        .expect("fixture related-source query should be valid")
    }

    fn validated_path_query() -> ValidatedPathQuery {
        ValidatedPathQuery::try_from(PathQuery {
            from: concept_id(22),
            to: concept_id(23),
            direction: DirectionMode::Either,
            max_depth: 6,
            max_work: 701,
            limit: 19,
        })
        .expect("fixture path query should be valid")
    }

    database_operation_test!(
        create_concept_records_and_fails_with_its_operation,
        CreateConcept,
        RecordedDatabaseCall::CreateConcept {
            candidate_id: concept_id(1),
            aliases: concept_aliases().aliases().to_vec(),
        },
        recording,
        failing,
        {
            let input = validated_create_concept();
            recording.create_concept(&input).await
        },
        {
            let input = validated_create_concept();
            failing.create_concept(&input).await
        }
    );

    database_operation_test!(
        replace_concept_aliases_records_and_fails_with_its_operation,
        ReplaceConceptAliases,
        RecordedDatabaseCall::ReplaceConceptAliases {
            concept_id: concept_id(2),
            expected_revision: revision(41),
            aliases: replacement_aliases().aliases().to_vec(),
        },
        recording,
        failing,
        {
            let input = validated_replace_concept_aliases();
            recording.replace_concept_aliases(&input).await
        },
        {
            let input = validated_replace_concept_aliases();
            failing.replace_concept_aliases(&input).await
        }
    );

    database_operation_test!(
        delete_concept_records_and_fails_with_its_operation,
        DeleteConcept,
        RecordedDatabaseCall::DeleteConcept {
            concept_id: concept_id(3),
            expected_revision: revision(42),
        },
        recording,
        failing,
        {
            let input = validated_delete_concept();
            recording.delete_concept(&input).await
        },
        {
            let input = validated_delete_concept();
            failing.delete_concept(&input).await
        }
    );

    database_operation_test!(
        register_source_records_and_fails_with_its_operation,
        RegisterSource,
        RecordedDatabaseCall::RegisterSource {
            source: source_reference(memory_source_input()),
        },
        recording,
        failing,
        {
            let input = validated_source(memory_source_input());
            recording.register_source(&input).await
        },
        {
            let input = validated_source(memory_source_input());
            failing.register_source(&input).await
        }
    );

    database_operation_test!(
        delete_source_records_and_fails_with_its_operation,
        DeleteSource,
        RecordedDatabaseCall::DeleteSource {
            source: source_reference(session_source_input()),
        },
        recording,
        failing,
        {
            let input = validated_delete_source();
            recording.delete_source(&input).await
        },
        {
            let input = validated_delete_source();
            failing.delete_source(&input).await
        }
    );

    database_operation_test!(
        create_mention_records_and_fails_with_its_operation,
        CreateMention,
        RecordedDatabaseCall::CreateMention {
            concept_id: concept_id(30),
            source: source_reference(memory_source_input()),
        },
        recording,
        failing,
        {
            let input = validated_mention(concept_id(30), memory_source_input());
            recording.create_mention(&input).await
        },
        {
            let input = validated_mention(concept_id(30), memory_source_input());
            failing.create_mention(&input).await
        }
    );

    database_operation_test!(
        delete_mention_records_and_fails_with_its_operation,
        DeleteMention,
        RecordedDatabaseCall::DeleteMention {
            concept_id: concept_id(31),
            source: source_reference(session_source_input()),
        },
        recording,
        failing,
        {
            let input = validated_mention(concept_id(31), session_source_input());
            recording.delete_mention(&input).await
        },
        {
            let input = validated_mention(concept_id(31), session_source_input());
            failing.delete_mention(&input).await
        }
    );

    database_operation_test!(
        create_assertion_records_and_fails_with_its_operation,
        CreateAssertion,
        RecordedDatabaseCall::CreateAssertion {
            candidate_id: assertion_id(5),
            subject_concept_id: concept_id(10),
            relation_type: RelationType::RelatedTo,
            object_concept_id: concept_id(11),
            supporting_sources: vec![
                source_reference(memory_source_input()),
                source_reference(session_source_input()),
            ],
        },
        recording,
        failing,
        {
            let input = validated_create_assertion();
            recording.create_assertion(&input).await
        },
        {
            let input = validated_create_assertion();
            failing.create_assertion(&input).await
        }
    );

    database_operation_test!(
        update_assertion_records_and_fails_with_its_operation,
        UpdateAssertion,
        RecordedDatabaseCall::UpdateAssertion {
            assertion_id: assertion_id(6),
            expected_revision: revision(43),
            subject_concept_id: concept_id(12),
            relation_type: RelationType::DependsOn,
            object_concept_id: concept_id(13),
        },
        recording,
        failing,
        {
            let input = validated_update_assertion();
            recording.update_assertion(&input).await
        },
        {
            let input = validated_update_assertion();
            failing.update_assertion(&input).await
        }
    );

    database_operation_test!(
        delete_assertion_records_and_fails_with_its_operation,
        DeleteAssertion,
        RecordedDatabaseCall::DeleteAssertion {
            assertion_id: assertion_id(7),
            expected_revision: revision(44),
        },
        recording,
        failing,
        {
            let input = validated_delete_assertion();
            recording.delete_assertion(&input).await
        },
        {
            let input = validated_delete_assertion();
            failing.delete_assertion(&input).await
        }
    );

    database_operation_test!(
        add_evidence_records_and_fails_with_its_operation,
        AddEvidence,
        RecordedDatabaseCall::AddEvidence {
            assertion_id: assertion_id(8),
            source: source_reference(memory_source_input()),
        },
        recording,
        failing,
        {
            let input = validated_evidence(assertion_id(8), memory_source_input());
            recording.add_evidence(&input).await
        },
        {
            let input = validated_evidence(assertion_id(8), memory_source_input());
            failing.add_evidence(&input).await
        }
    );

    database_operation_test!(
        remove_evidence_records_and_fails_with_its_operation,
        RemoveEvidence,
        RecordedDatabaseCall::RemoveEvidence {
            assertion_id: assertion_id(9),
            source: source_reference(session_source_input()),
        },
        recording,
        failing,
        {
            let input = validated_evidence(assertion_id(9), session_source_input());
            recording.remove_evidence(&input).await
        },
        {
            let input = validated_evidence(assertion_id(9), session_source_input());
            failing.remove_evidence(&input).await
        }
    );

    database_operation_test!(
        get_concept_records_and_fails_with_its_operation,
        GetConcept,
        RecordedDatabaseCall::GetConcept { id: concept_id(60) },
        recording,
        failing,
        {
            let input = validated_concept_id_input(60);
            recording.get_concept(&input.id).await
        },
        {
            let input = validated_concept_id_input(60);
            failing.get_concept(&input.id).await
        }
    );

    database_operation_test!(
        resolve_alias_records_and_fails_with_its_operation,
        ResolveAlias,
        RecordedDatabaseCall::ResolveAlias {
            key: normalize_alias_key("Primary  Alpha").expect("fixture alias key should be valid"),
        },
        recording,
        failing,
        {
            let key = alias_key();
            recording.resolve_alias(&key).await
        },
        {
            let key = alias_key();
            failing.resolve_alias(&key).await
        }
    );

    database_operation_test!(
        get_assertion_records_and_fails_with_its_operation,
        GetAssertion,
        RecordedDatabaseCall::GetAssertion {
            id: assertion_id(61),
        },
        recording,
        failing,
        {
            let input = validated_assertion_id_input(61);
            recording.get_assertion(input.id).await
        },
        {
            let input = validated_assertion_id_input(61);
            failing.get_assertion(input.id).await
        }
    );

    database_operation_test!(
        get_source_records_and_fails_with_its_operation,
        GetSource,
        RecordedDatabaseCall::GetSource {
            source: source_reference(memory_source_input()),
        },
        recording,
        failing,
        {
            let input = validated_source(memory_source_input());
            recording.get_source(&input).await
        },
        {
            let input = validated_source(memory_source_input());
            failing.get_source(&input).await
        }
    );

    database_operation_test!(
        neighbors_records_and_fails_with_its_operation,
        Neighbors,
        RecordedDatabaseCall::Neighbors {
            concept_id: concept_id(20),
            direction: DirectionMode::Incoming,
            relation_filter: vec![RelationType::IsA, RelationType::Uses],
            limit: 17,
        },
        recording,
        failing,
        {
            let query = validated_neighbor_query();
            recording.neighbors(&query).await
        },
        {
            let query = validated_neighbor_query();
            failing.neighbors(&query).await
        }
    );

    database_operation_test!(
        related_sources_records_and_fails_with_its_operation,
        RelatedSources,
        RecordedDatabaseCall::RelatedSources {
            concept_id: concept_id(21),
            limit: 18,
        },
        recording,
        failing,
        {
            let query = validated_related_source_query();
            recording.related_sources(&query).await
        },
        {
            let query = validated_related_source_query();
            failing.related_sources(&query).await
        }
    );

    database_operation_test!(
        find_paths_records_and_fails_with_its_operation,
        FindPaths,
        RecordedDatabaseCall::FindPaths {
            from: concept_id(22),
            to: concept_id(23),
            direction: DirectionMode::Either,
            max_depth: 6,
            max_work: 701,
            limit: 19,
        },
        recording,
        failing,
        {
            let query = validated_path_query();
            recording.find_paths(&query).await
        },
        {
            let query = validated_path_query();
            failing.find_paths(&query).await
        }
    );
}

#[cfg(test)]
mod iii_common_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use iii_sdk::{Error as IiiError, IIIClient, protocol::TriggerRequest};
    use serde_json::{Value, json};
    use uuid::Uuid;

    use crate::contracts::{
        BackendFailureReason, DatabaseMutationOutcome, DatabaseReadOutcome, DatabaseTimeouts,
        GraphError, OperationCode, TraversalBound, TraversalReason,
    };

    use super::{
        DATABASE_EXECUTE_FUNCTION_ID, DATABASE_QUERY_FUNCTION_ID, DatabaseTarget,
        GraphConfigurationError, IiiKnowledgeGraphDatabase, PositionalParameters, execute_with,
        query_with,
    };

    const DATABASE_TARGET_SENTINEL: &str = "graph-database-target-sentinel";
    const SQL_SENTINEL: &str = "SELECT graph-private-sql-sentinel($1::text, $2::bigint)";
    const PARAMETER_SENTINEL: &str = "graph-private-parameter-sentinel";
    const ROW_SENTINEL: &str = "graph-private-row-sentinel";
    const WORKER_MESSAGE_SENTINEL: &str = "graph-private-worker-message-sentinel";
    const SDK_CODE_SENTINEL: &str = "graph-private-sdk-code-sentinel";
    const SDK_STACKTRACE_SENTINEL: &str = "graph-private-sdk-stacktrace-sentinel";

    fn database_target() -> DatabaseTarget {
        DatabaseTarget::try_from(DATABASE_TARGET_SENTINEL.to_owned())
            .expect("test database target should be valid")
    }

    fn timeouts() -> DatabaseTimeouts {
        DatabaseTimeouts::try_new(Duration::from_secs(4), Duration::from_secs(13))
            .expect("test timeouts should be valid")
    }

    fn operation_failure(operation: OperationCode) -> GraphError {
        GraphError::DatabaseFailure {
            operation,
            reason: BackendFailureReason::DatabaseFailure,
        }
    }

    fn uuid_v7(value: u128) -> String {
        const UUID_V7_BASE: u128 = 0x0000_0000_0000_7000_8000_0000_0000_0000;
        Uuid::from_u128(UUID_V7_BASE + value).to_string()
    }

    fn execute_response(rows: Vec<Value>) -> Value {
        execute_response_with(1, Value::Null, rows)
    }

    fn execute_response_with(affected_rows: u64, last_insert_id: Value, rows: Vec<Value>) -> Value {
        json!({
            "affected_rows": affected_rows,
            "last_insert_id": last_insert_id,
            "returned_rows": rows,
        })
    }

    fn query_response(payload: Value) -> Value {
        query_response_with(
            vec![json!({"payload": payload})],
            json!(1),
            vec![json!({"name": "payload", "type": "jsonb"})],
        )
    }

    fn query_response_with(rows: Vec<Value>, row_count: Value, columns: Vec<Value>) -> Value {
        json!({
            "rows": rows,
            "row_count": row_count,
            "columns": columns,
        })
    }

    fn assert_opaque(error: &GraphError, sentinels: &[&str]) {
        let display = error.to_string();
        let debug = format!("{error:?}");
        for sentinel in sentinels {
            assert!(
                !display.contains(sentinel),
                "Display error leaked protected value: {display}"
            );
            assert!(
                !debug.contains(sentinel),
                "Debug error leaked protected value: {debug}"
            );
        }
    }

    async fn execute_row(
        operation: OperationCode,
        row: Value,
    ) -> Result<DatabaseMutationOutcome<Value>, GraphError> {
        let response = execute_response(vec![row]);
        execute_with(
            &database_target(),
            timeouts(),
            operation,
            SQL_SENTINEL,
            PositionalParameters::from(vec![json!(PARAMETER_SENTINEL)]),
            move |_| async move { Ok(response) },
        )
        .await
    }

    async fn query_payload(
        operation: OperationCode,
        payload: Value,
    ) -> Result<DatabaseReadOutcome<Value>, GraphError> {
        let response = query_response(payload);
        query_with(
            &database_target(),
            timeouts(),
            operation,
            SQL_SENTINEL,
            PositionalParameters::from(vec![json!(PARAMETER_SENTINEL)]),
            move |_| async move { Ok(response) },
        )
        .await
    }

    #[test]
    fn database_target_rejects_empty_and_nul_without_echoing_values() {
        assert!(DatabaseTarget::try_from(" \t".to_owned()).is_ok());

        for value in [String::new(), "protected\0database-target".to_owned()] {
            let error = DatabaseTarget::try_from(value).expect_err("invalid target should fail");
            assert_eq!(error, GraphConfigurationError::InvalidDatabaseTarget);
            assert!(!error.to_string().contains("protected"));
            assert!(!format!("{error:?}").contains("protected"));
        }
    }

    #[test]
    fn iii_adapter_constructor_stores_validated_target_and_timeouts_without_connecting() {
        let adapter = IiiKnowledgeGraphDatabase::new(
            IIIClient::new("ws://127.0.0.1:1"),
            database_target(),
            timeouts(),
        )
        .expect("validated database configuration should construct");

        assert_eq!(adapter.database.as_str(), DATABASE_TARGET_SENTINEL);
        assert_eq!(adapter.timeouts.query(), Duration::from_secs(4));
        assert_eq!(adapter.timeouts.invocation(), Duration::from_secs(13));
    }

    #[tokio::test]
    async fn common_iii_execute_request_is_static_positional_and_uses_only_outer_timeout() {
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let callback_count = Arc::clone(&invocation_count);
        let response = execute_response(vec![json!({"outcome": "deleted"})]);

        let result: DatabaseMutationOutcome<Value> = execute_with(
            &database_target(),
            timeouts(),
            OperationCode::DeleteConcept,
            SQL_SENTINEL,
            PositionalParameters::from(vec![json!(PARAMETER_SENTINEL), json!(23)]),
            move |request: TriggerRequest| {
                callback_count.fetch_add(1, Ordering::SeqCst);
                async move {
                    assert_eq!(request.function_id, DATABASE_EXECUTE_FUNCTION_ID);
                    assert_eq!(
                        request.payload,
                        json!({
                            "db": DATABASE_TARGET_SENTINEL,
                            "sql": SQL_SENTINEL,
                            "params": [PARAMETER_SENTINEL, 23],
                        })
                    );
                    assert_eq!(
                        request.payload.as_object().map(serde_json::Map::len),
                        Some(3)
                    );
                    assert!(request.action.is_none());
                    assert_eq!(request.timeout_ms, Some(13_000));
                    Ok(response)
                }
            },
        )
        .await
        .expect("one deleted outcome row should decode");

        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        assert_eq!(result, DatabaseMutationOutcome::Deleted);
    }

    #[tokio::test]
    async fn common_iii_query_request_uses_query_timeout_and_longer_invocation_timeout() {
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let callback_count = Arc::clone(&invocation_count);
        let response = query_response(json!({
            "outcome": "complete",
            "result": {"value": ROW_SENTINEL},
        }));

        let result: DatabaseReadOutcome<Value> = query_with(
            &database_target(),
            timeouts(),
            OperationCode::GetConcept,
            SQL_SENTINEL,
            PositionalParameters::from(vec![json!(PARAMETER_SENTINEL)]),
            move |request: TriggerRequest| {
                callback_count.fetch_add(1, Ordering::SeqCst);
                async move {
                    assert_eq!(request.function_id, DATABASE_QUERY_FUNCTION_ID);
                    assert_eq!(
                        request.payload,
                        json!({
                            "db": DATABASE_TARGET_SENTINEL,
                            "sql": SQL_SENTINEL,
                            "params": [PARAMETER_SENTINEL],
                            "timeout_ms": 4_000,
                        })
                    );
                    assert!(request.action.is_none());
                    assert_eq!(request.timeout_ms, Some(13_000));
                    assert!(request.timeout_ms.expect("invocation timeout should exist") > 4_000);
                    Ok(response)
                }
            },
        )
        .await
        .expect("one complete payload should decode");

        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            result,
            DatabaseReadOutcome::Complete {
                result: json!({"value": ROW_SENTINEL}),
            }
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn common_iii_execute_waits_for_database_response_before_decoding_without_retry() {
        use tokio::sync::oneshot;

        let invocation_count = Arc::new(AtomicUsize::new(0));
        let callback_count = Arc::clone(&invocation_count);
        let (entered_tx, entered_rx) = oneshot::channel();
        let (response_tx, response_rx) = oneshot::channel();
        let mut adapter = tokio::spawn(async move {
            execute_with::<Value, _, _>(
                &database_target(),
                timeouts(),
                OperationCode::DeleteConcept,
                SQL_SENTINEL,
                PositionalParameters::from(vec![json!(PARAMETER_SENTINEL)]),
                move |_| {
                    callback_count.fetch_add(1, Ordering::SeqCst);
                    async move {
                        entered_tx
                            .send(())
                            .expect("the test should receive the invocation entry signal");
                        let response = response_rx
                            .await
                            .expect("the test should release the database response");
                        Ok(response)
                    }
                },
            )
            .await
        });

        tokio::time::timeout(Duration::from_secs(1), entered_rx)
            .await
            .expect("execute invocation should start")
            .expect("execute invocation should signal entry");
        assert!(
            !adapter.is_finished(),
            "execute adapter should wait while the database response is held"
        );

        response_tx
            .send(execute_response(vec![json!({"outcome": "deleted"})]))
            .expect("execute adapter should still be waiting for the response");
        let result = tokio::time::timeout(Duration::from_secs(1), &mut adapter)
            .await
            .expect("execute adapter should finish after the response arrives")
            .expect("execute adapter task should not panic")
            .expect("valid execute response should decode");

        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        assert_eq!(result, DatabaseMutationOutcome::Deleted);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn common_iii_query_waits_for_database_response_before_decoding_without_retry() {
        use tokio::sync::oneshot;

        let invocation_count = Arc::new(AtomicUsize::new(0));
        let callback_count = Arc::clone(&invocation_count);
        let (entered_tx, entered_rx) = oneshot::channel();
        let (response_tx, response_rx) = oneshot::channel();
        let mut adapter = tokio::spawn(async move {
            query_with::<Value, _, _>(
                &database_target(),
                timeouts(),
                OperationCode::GetConcept,
                SQL_SENTINEL,
                PositionalParameters::from(vec![json!(PARAMETER_SENTINEL)]),
                move |_| {
                    callback_count.fetch_add(1, Ordering::SeqCst);
                    async move {
                        entered_tx
                            .send(())
                            .expect("the test should receive the invocation entry signal");
                        let response = response_rx
                            .await
                            .expect("the test should release the database response");
                        Ok(response)
                    }
                },
            )
            .await
        });

        tokio::time::timeout(Duration::from_secs(1), entered_rx)
            .await
            .expect("query invocation should start")
            .expect("query invocation should signal entry");
        assert!(
            !adapter.is_finished(),
            "query adapter should wait while the database response is held"
        );

        response_tx
            .send(query_response(json!({
                "outcome": "complete",
                "result": {"value": ROW_SENTINEL},
            })))
            .expect("query adapter should still be waiting for the response");
        let result = tokio::time::timeout(Duration::from_secs(1), &mut adapter)
            .await
            .expect("query adapter should finish after the response arrives")
            .expect("query adapter task should not panic")
            .expect("valid query response should decode");

        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            result,
            DatabaseReadOutcome::Complete {
                result: json!({"value": ROW_SENTINEL}),
            }
        );
    }

    #[tokio::test]
    async fn common_iii_query_request_keeps_millisecond_deadlines_strictly_ordered() {
        let sub_millisecond_timeouts = DatabaseTimeouts::try_new(
            Duration::from_nanos(1_000_000_001),
            Duration::from_nanos(1_000_000_002),
        )
        .expect("sub-millisecond timeout separation should remain valid");
        let response = query_response(json!({"outcome": "complete", "result": null}));

        let _: DatabaseReadOutcome<Value> = query_with(
            &database_target(),
            sub_millisecond_timeouts,
            OperationCode::GetConcept,
            SQL_SENTINEL,
            PositionalParameters::from(Vec::new()),
            move |request: TriggerRequest| async move {
                assert_eq!(request.payload["timeout_ms"], json!(1_001));
                assert_eq!(request.timeout_ms, Some(1_002));
                Ok(response)
            },
        )
        .await
        .expect("sub-millisecond query and invocation deadlines should remain distinct");
    }

    #[tokio::test]
    async fn common_iii_mutation_decoder_accepts_each_supported_single_outcome() {
        let concept_identity = json!({"kind": "concept", "id": uuid_v7(41)});
        let assertion_identity = json!({"kind": "assertion", "id": uuid_v7(42)});
        let rows = vec![
            json!({"outcome": "created", "record": {"value": ROW_SENTINEL}}),
            json!({"outcome": "existing", "record": {"value": ROW_SENTINEL}}),
            json!({"outcome": "updated", "record": {"value": ROW_SENTINEL}}),
            json!({"outcome": "deleted"}),
            json!({
                "outcome": "conflict",
                "field": "revision",
                "reason": "stale_revision",
                "identity": concept_identity,
                "current_revision": 7,
            }),
            json!({
                "outcome": "stale",
                "identity": assertion_identity,
                "current_revision": 8,
            }),
            json!({
                "outcome": "referenced",
                "reference": {"kind": "source_reference"},
            }),
            json!({"outcome": "missing", "record_kind": "concept", "identity": null}),
            json!({"outcome": "evidence_limit"}),
            json!({
                "outcome": "would_orphan_evidence",
                "assertion_id": uuid_v7(43),
                "current_revision": 9,
            }),
        ];

        for row in rows {
            let expected_outcome = row["outcome"].clone();
            let result = execute_row(OperationCode::CreateConcept, row)
                .await
                .expect("supported mutation outcome should decode");
            let decoded = serde_json::to_value(result).expect("outcome should serialize");
            assert_eq!(decoded["outcome"], expected_outcome);
        }
    }

    #[tokio::test]
    async fn common_iii_mutation_decoder_rejects_malformed_and_unknown_outcomes_without_leaks() {
        let cases = [
            (
                json!({"record": {"value": ROW_SENTINEL}}),
                BackendFailureReason::MalformedResponse,
            ),
            (
                json!({"outcome": "unapproved-outcome-sentinel"}),
                BackendFailureReason::UnknownOutcome,
            ),
            (
                json!({"outcome": "created"}),
                BackendFailureReason::UnsupportedValue,
            ),
            (
                json!({"outcome": "deleted", "secret": ROW_SENTINEL}),
                BackendFailureReason::UnsupportedValue,
            ),
        ];

        for (row, reason) in cases {
            let error = execute_row(OperationCode::DeleteConcept, row)
                .await
                .expect_err("malformed or unknown outcomes should fail");
            assert_eq!(
                error,
                GraphError::InvalidResponse {
                    operation: OperationCode::DeleteConcept,
                    reason,
                }
            );
            assert_opaque(
                &error,
                &[
                    SQL_SENTINEL,
                    PARAMETER_SENTINEL,
                    ROW_SENTINEL,
                    "unapproved-outcome-sentinel",
                ],
            );
        }
    }

    #[tokio::test]
    async fn common_iii_mutation_decoder_requires_one_affected_row_and_one_returned_row() {
        let deleted = json!({"outcome": "deleted"});
        let cases = [
            execute_response_with(0, Value::Null, Vec::new()),
            execute_response_with(1, Value::Null, Vec::new()),
            execute_response_with(2, Value::Null, vec![deleted.clone()]),
            execute_response_with(1, Value::Null, vec![deleted.clone(), deleted.clone()]),
            execute_response_with(1, json!(ROW_SENTINEL), vec![deleted.clone()]),
        ];

        for response in cases {
            let error = execute_with::<Value, _, _>(
                &database_target(),
                timeouts(),
                OperationCode::DeleteConcept,
                SQL_SENTINEL,
                PositionalParameters::from(vec![json!(PARAMETER_SENTINEL)]),
                move |_| async move { Ok(response) },
            )
            .await
            .expect_err("unexpected mutation row counts should fail");
            assert_eq!(error, operation_failure(OperationCode::DeleteConcept));
            assert_opaque(&error, &[SQL_SENTINEL, PARAMETER_SENTINEL, ROW_SENTINEL]);
        }

        for response in [
            json!({
                "affected_rows": 1,
                "returned_rows": [{"outcome": "deleted"}],
            }),
            json!({
                "affected_rows": 1,
                "last_insert_id": null,
                "returned_rows": [{"outcome": "deleted"}],
                "backend_detail": ROW_SENTINEL,
            }),
        ] {
            let error = execute_with::<Value, _, _>(
                &database_target(),
                timeouts(),
                OperationCode::DeleteConcept,
                SQL_SENTINEL,
                PositionalParameters::from(Vec::new()),
                move |_| async move { Ok(response) },
            )
            .await
            .expect_err("malformed execute envelopes should fail");
            assert_eq!(
                error,
                GraphError::InvalidResponse {
                    operation: OperationCode::DeleteConcept,
                    reason: BackendFailureReason::MalformedResponse,
                }
            );
            assert_opaque(&error, &[SQL_SENTINEL, ROW_SENTINEL]);
        }
    }

    #[tokio::test]
    async fn common_iii_read_decoder_accepts_complete_and_payload_free_markers() {
        let cases = [
            (
                json!({"outcome": "complete", "result": {"value": ROW_SENTINEL}}),
                "complete",
            ),
            (json!({"outcome": "result_too_large"}), "result_too_large"),
            (json!({"outcome": "work_exhausted"}), "work_exhausted"),
        ];

        for (payload, expected_outcome) in cases {
            let result = query_payload(OperationCode::FindPaths, payload)
                .await
                .expect("complete payload or exact marker should decode");
            let decoded = serde_json::to_value(result).expect("read outcome should serialize");
            assert_eq!(decoded["outcome"], expected_outcome);
        }
    }

    #[tokio::test]
    async fn common_iii_read_decoder_rejects_partial_shapes_and_unknown_markers_without_leaks() {
        let cases = [
            (
                query_response_with(
                    Vec::new(),
                    json!(0),
                    vec![json!({"name": "payload", "type": "jsonb"})],
                ),
                operation_failure(OperationCode::FindPaths),
            ),
            (
                query_response_with(
                    vec![json!({"payload": {"outcome": "complete", "result": []}})],
                    json!(2),
                    vec![json!({"name": "payload", "type": "jsonb"})],
                ),
                operation_failure(OperationCode::FindPaths),
            ),
            (
                query_response_with(
                    vec![
                        json!({"payload": {"outcome": "complete", "result": []}}),
                        json!({"payload": {"outcome": "result_too_large"}}),
                    ],
                    json!(2),
                    vec![json!({"name": "payload", "type": "jsonb"})],
                ),
                operation_failure(OperationCode::FindPaths),
            ),
            (
                query_response_with(
                    vec![json!({"payload": {"outcome": "complete", "result": []}})],
                    json!(1),
                    vec![json!({"name": "other", "type": "jsonb"})],
                ),
                GraphError::InvalidResponse {
                    operation: OperationCode::FindPaths,
                    reason: BackendFailureReason::MalformedResponse,
                },
            ),
            (
                query_response_with(
                    vec![json!({
                        "payload": {"outcome": "complete", "result": []},
                        "unexpected": ROW_SENTINEL,
                    })],
                    json!(1),
                    vec![json!({"name": "payload", "type": "jsonb"})],
                ),
                GraphError::InvalidResponse {
                    operation: OperationCode::FindPaths,
                    reason: BackendFailureReason::MalformedResponse,
                },
            ),
            (
                query_response_with(
                    vec![json!({"payload": {"outcome": "complete"}})],
                    json!(1),
                    vec![json!({"name": "payload", "type": "jsonb"})],
                ),
                GraphError::InvalidResponse {
                    operation: OperationCode::FindPaths,
                    reason: BackendFailureReason::MalformedResponse,
                },
            ),
            (
                query_response_with(
                    vec![json!({"payload": {"outcome": "mystery-marker-sentinel"}})],
                    json!(1),
                    vec![json!({"name": "payload", "type": "jsonb"})],
                ),
                GraphError::InvalidResponse {
                    operation: OperationCode::FindPaths,
                    reason: BackendFailureReason::UnknownOutcome,
                },
            ),
            (
                query_response_with(
                    vec![json!({"payload": {"outcome": "result_too_large", "result": []}})],
                    json!(1),
                    vec![json!({"name": "payload", "type": "jsonb"})],
                ),
                GraphError::InvalidResponse {
                    operation: OperationCode::FindPaths,
                    reason: BackendFailureReason::MalformedResponse,
                },
            ),
        ];

        for (response, expected) in cases {
            let error = query_with::<Value, _, _>(
                &database_target(),
                timeouts(),
                OperationCode::FindPaths,
                SQL_SENTINEL,
                PositionalParameters::from(vec![json!(PARAMETER_SENTINEL)]),
                move |_| async move { Ok(response) },
            )
            .await
            .expect_err("partial or malformed read outcomes should fail");
            assert_eq!(error, expected);
            assert_opaque(
                &error,
                &[
                    SQL_SENTINEL,
                    PARAMETER_SENTINEL,
                    ROW_SENTINEL,
                    "mystery-marker-sentinel",
                ],
            );
        }

        for response in [
            query_response_with(
                vec![json!({"payload": {"outcome": "complete", "result": []}})],
                json!("1"),
                vec![json!({"name": "payload", "type": "jsonb"})],
            ),
            json!({
                "rows": [{"payload": {"outcome": "complete", "result": []}}],
                "row_count": 1,
                "columns": [{"name": "payload", "type": "jsonb"}],
                "backend_detail": ROW_SENTINEL,
            }),
        ] {
            let error = query_with::<Value, _, _>(
                &database_target(),
                timeouts(),
                OperationCode::FindPaths,
                SQL_SENTINEL,
                PositionalParameters::from(Vec::new()),
                move |_| async move { Ok(response) },
            )
            .await
            .expect_err("malformed query envelopes should fail");
            assert_eq!(
                error,
                GraphError::InvalidResponse {
                    operation: OperationCode::FindPaths,
                    reason: BackendFailureReason::MalformedResponse,
                }
            );
            assert_opaque(&error, &[SQL_SENTINEL, ROW_SENTINEL]);
        }
    }

    #[tokio::test]
    async fn common_iii_timeout_maps_only_the_exact_worker_timeout_envelope() {
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let callback_count = Arc::clone(&invocation_count);
        let sdk_error = IiiError::Remote {
            code: "invocation_failed".to_owned(),
            message: format!(
                "handler error: {{\"code\":\"QUERY_TIMEOUT\",\"message\":\"{WORKER_MESSAGE_SENTINEL}\"}}"
            ),
            stacktrace: Some(SDK_STACKTRACE_SENTINEL.to_owned()),
        };

        let error = query_with::<Value, _, _>(
            &database_target(),
            timeouts(),
            OperationCode::FindPaths,
            SQL_SENTINEL,
            PositionalParameters::from(vec![json!(PARAMETER_SENTINEL)]),
            move |_| {
                callback_count.fetch_add(1, Ordering::SeqCst);
                async move { Err(sdk_error) }
            },
        )
        .await
        .expect_err("the exact query-timeout envelope should be classified");

        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            error,
            GraphError::TraversalBoundExceeded {
                operation: OperationCode::FindPaths,
                bound: TraversalBound::QueryTimeout,
                reason: TraversalReason::QueryTimeout,
            }
        );
        assert_opaque(
            &error,
            &[
                SQL_SENTINEL,
                PARAMETER_SENTINEL,
                WORKER_MESSAGE_SENTINEL,
                SDK_STACKTRACE_SENTINEL,
            ],
        );
    }

    #[tokio::test]
    async fn common_iii_timeout_near_misses_and_generic_errors_are_opaque_and_not_retried() {
        let timeout_body =
            format!("{{\"code\":\"QUERY_TIMEOUT\",\"message\":\"{WORKER_MESSAGE_SENTINEL}\"}}");
        let near_misses = vec![
            IiiError::Remote {
                code: SDK_CODE_SENTINEL.to_owned(),
                message: format!("handler error: {timeout_body}"),
                stacktrace: Some(SDK_STACKTRACE_SENTINEL.to_owned()),
            },
            IiiError::Remote {
                code: "invocation_failed".to_owned(),
                message: timeout_body.clone(),
                stacktrace: Some(SDK_STACKTRACE_SENTINEL.to_owned()),
            },
            IiiError::Remote {
                code: "invocation_failed".to_owned(),
                message: format!("Handler error: {timeout_body}"),
                stacktrace: None,
            },
            IiiError::Remote {
                code: "invocation_failed".to_owned(),
                message: format!("handler error:{timeout_body}"),
                stacktrace: None,
            },
            IiiError::Remote {
                code: "invocation_failed".to_owned(),
                message: "handler error: {malformed-json".to_owned(),
                stacktrace: None,
            },
            IiiError::Remote {
                code: "invocation_failed".to_owned(),
                message: "handler error: {\"code\":\"query_timeout\"}".to_owned(),
                stacktrace: None,
            },
            IiiError::Remote {
                code: "invocation_failed".to_owned(),
                message: "handler error: {\"code\":\"QUERY_TIMEOUT_NEAR_MISS\"}".to_owned(),
                stacktrace: None,
            },
            IiiError::Remote {
                code: "invocation_failed".to_owned(),
                message: "handler error: {\"code\":\"DATABASE_ERROR\"}".to_owned(),
                stacktrace: None,
            },
            IiiError::Timeout,
            IiiError::Handler(WORKER_MESSAGE_SENTINEL.to_owned()),
            IiiError::NotConnected,
        ];

        for sdk_error in near_misses {
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let callback_count = Arc::clone(&invocation_count);
            let error = query_with::<Value, _, _>(
                &database_target(),
                timeouts(),
                OperationCode::RelatedSources,
                SQL_SENTINEL,
                PositionalParameters::from(vec![json!(PARAMETER_SENTINEL)]),
                move |_| {
                    callback_count.fetch_add(1, Ordering::SeqCst);
                    async move { Err(sdk_error) }
                },
            )
            .await
            .expect_err("timeout near misses and generic SDK errors should fail");

            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
            assert_eq!(error, operation_failure(OperationCode::RelatedSources));
            assert_opaque(
                &error,
                &[
                    SQL_SENTINEL,
                    PARAMETER_SENTINEL,
                    SDK_CODE_SENTINEL,
                    WORKER_MESSAGE_SENTINEL,
                    SDK_STACKTRACE_SENTINEL,
                ],
            );
        }
    }
}

#[cfg(test)]
mod iii_concept_operation_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use iii_sdk::{Error as IiiError, IIIClient, protocol::TriggerRequest};
    use serde_json::{Value, json};
    use uuid::Uuid;

    use crate::{
        contracts::{
            Alias, AliasKey, BackendFailureReason, Concept, ConceptId, ConflictField,
            ConflictReason, DatabaseTimeouts, DeleteConcept, GraphError, NotFoundReason,
            OperationCode, RecordIdentity, RecordKind, ReferenceReason, ReferencedRecord, Revision,
            ValidatedAliasSet, ValidatedCreateConcept, ValidatedDeleteConcept,
            ValidatedReplaceConceptAliases,
        },
        normalization::{normalize_alias_key, normalize_alias_set},
    };

    use super::{
        CREATE_CONCEPT_SQL, DATABASE_EXECUTE_FUNCTION_ID, DATABASE_QUERY_FUNCTION_ID,
        DELETE_CONCEPT_SQL, GET_CONCEPT_SQL, IiiKnowledgeGraphDatabase,
        REPLACE_CONCEPT_ALIASES_SQL, RESOLVE_ALIAS_SQL,
    };

    const DATABASE_TARGET: &str = "concept-operation-database-target";
    const ALIAS_SENTINEL: &str = "PRIVATE_CONCEPT_ALIAS_SENTINEL";
    const UUID_V7_BASE: u128 = 0x0000_0000_0000_7000_8000_0000_0000_0000;

    #[derive(Clone)]
    struct ExpectedRequest {
        function_id: &'static str,
        payload: Value,
        timeout_ms: Option<u64>,
    }

    fn database() -> IiiKnowledgeGraphDatabase {
        IiiKnowledgeGraphDatabase::new(
            IIIClient::new("ws://127.0.0.1:1"),
            super::DatabaseTarget::try_from(DATABASE_TARGET.to_owned())
                .expect("test database target should be valid"),
            DatabaseTimeouts::try_new(Duration::from_secs(4), Duration::from_secs(13))
                .expect("test timeouts should be valid"),
        )
        .expect("adapter configuration should be valid")
    }

    fn concept_id(value: u128) -> ConceptId {
        ConceptId::try_from(Uuid::from_u128(UUID_V7_BASE + value))
            .expect("fixture concept ID should be UUIDv7")
    }

    fn revision(value: i64) -> Revision {
        Revision::try_from(value).expect("fixture revision should be positive")
    }

    fn alias(display_text: &str, preferred: bool) -> Alias {
        Alias {
            display_text: display_text.to_owned(),
            preferred,
        }
    }

    fn alias_set(aliases: &[(&str, bool)]) -> ValidatedAliasSet {
        let aliases = aliases
            .iter()
            .map(|(display_text, preferred)| alias(display_text, *preferred))
            .collect::<Vec<_>>();
        normalize_alias_set(&aliases).expect("fixture aliases should be valid")
    }

    fn create_input(candidate: ConceptId) -> ValidatedCreateConcept {
        ValidatedCreateConcept::new(
            candidate,
            alias_set(&[("βeta", false), ("Primary  Alpha", true)]),
        )
    }

    fn replace_input(id: ConceptId, expected_revision: Revision) -> ValidatedReplaceConceptAliases {
        ValidatedReplaceConceptAliases::new(
            id,
            expected_revision,
            alias_set(&[("Zulu Label", false), ("Alpha Label", true)]),
        )
    }

    fn delete_input(id: ConceptId, expected_revision: Revision) -> ValidatedDeleteConcept {
        ValidatedDeleteConcept::from(DeleteConcept {
            concept_id: id,
            expected_revision,
        })
    }

    fn concept(id: ConceptId, revision_value: i64, aliases: &[(&str, bool)]) -> Concept {
        Concept {
            id,
            revision: revision(revision_value),
            aliases: aliases
                .iter()
                .map(|(display_text, preferred)| alias(display_text, *preferred))
                .collect(),
        }
    }

    fn concept_json(id: ConceptId, revision: i64, aliases: &[(&str, bool)]) -> Value {
        json!({
            "id": id.as_uuid().to_string(),
            "revision": revision,
            "aliases": aliases
                .iter()
                .map(|(display_text, preferred)| json!({
                    "display_text": display_text,
                    "preferred": preferred,
                }))
                .collect::<Vec<_>>(),
        })
    }

    fn execute_response(row: Value) -> Value {
        json!({
            "affected_rows": 1,
            "last_insert_id": null,
            "returned_rows": [row],
        })
    }

    fn query_response(payload: Value) -> Value {
        json!({
            "rows": [{"payload": payload}],
            "row_count": 1,
            "columns": [{"name": "payload", "type": "jsonb"}],
        })
    }

    fn complete_payload(result: Value) -> Value {
        json!({"outcome": "complete", "result": result})
    }

    fn execute_request(sql: &'static str, params: Value) -> ExpectedRequest {
        ExpectedRequest {
            function_id: DATABASE_EXECUTE_FUNCTION_ID,
            payload: json!({
                "db": DATABASE_TARGET,
                "sql": sql,
                "params": params,
            }),
            timeout_ms: Some(13_000),
        }
    }

    fn query_request(sql: &'static str, params: Value) -> ExpectedRequest {
        ExpectedRequest {
            function_id: DATABASE_QUERY_FUNCTION_ID,
            payload: json!({
                "db": DATABASE_TARGET,
                "sql": sql,
                "params": params,
                "timeout_ms": 4_000,
            }),
            timeout_ms: Some(13_000),
        }
    }

    async fn respond(
        request: TriggerRequest,
        expected: ExpectedRequest,
        response: Value,
        invocation_count: Arc<AtomicUsize>,
    ) -> Result<Value, IiiError> {
        invocation_count.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.function_id, expected.function_id);
        assert_eq!(request.payload, expected.payload);
        assert!(request.action.is_none());
        assert_eq!(request.timeout_ms, expected.timeout_ms);
        Ok(response)
    }

    fn alias_conflict_row(reason: ConflictReason, id: ConceptId, current_revision: i64) -> Value {
        json!({
            "outcome": "conflict",
            "field": "alias_set",
            "reason": reason.as_str(),
            "identity": {"kind": "concept", "id": id.as_uuid().to_string()},
            "current_revision": current_revision,
        })
    }

    fn expected_conflict(
        operation: OperationCode,
        field: ConflictField,
        reason: ConflictReason,
        id: ConceptId,
        current_revision: i64,
    ) -> GraphError {
        GraphError::Conflict {
            operation,
            field,
            reason,
            identity: RecordIdentity::Concept(id),
            current_revision: Some(revision(current_revision)),
        }
    }

    fn expected_not_found(operation: OperationCode, id: ConceptId) -> GraphError {
        GraphError::NotFound {
            operation,
            record_kind: RecordKind::Concept,
            identity: Some(RecordIdentity::Concept(id)),
            reason: NotFoundReason::Missing,
        }
    }

    fn expected_database_failure(operation: OperationCode) -> GraphError {
        GraphError::DatabaseFailure {
            operation,
            reason: BackendFailureReason::DatabaseFailure,
        }
    }

    fn expected_invalid_response(operation: OperationCode) -> GraphError {
        GraphError::InvalidResponse {
            operation,
            reason: BackendFailureReason::UnsupportedValue,
        }
    }

    fn assert_opaque(error: &GraphError, sentinels: &[&str]) {
        let display = error.to_string();
        let debug = format!("{error:?}");
        for sentinel in sentinels {
            assert!(!display.contains(sentinel), "Display leaked protected text");
            assert!(!debug.contains(sentinel), "Debug leaked protected text");
        }
    }

    #[tokio::test]
    async fn create_concept_invokes_only_the_approved_routine_and_decodes_created_record() {
        let input = create_input(concept_id(1));
        let candidate = input.candidate_id;
        let expected_record = concept(candidate, 1, &[("Primary  Alpha", true), ("βeta", false)]);
        let response = execute_response(json!({
            "outcome": "created",
            "record": concept_json(
                candidate,
                1,
                &[("Primary  Alpha", true), ("βeta", false)],
            ),
        }));
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let result = database()
            .create_concept_with(&input, {
                let invocation_count = Arc::clone(&invocation_count);
                move |request| {
                    respond(
                        request,
                        execute_request(
                            "SELECT knowledge_graph.graph_create_concept($1::text::uuid, $2::jsonb)",
                            json!([
                                candidate.as_uuid().to_string(),
                                [
                                    {
                                        "alias_key": "primary alpha",
                                        "display_text": "Primary  Alpha",
                                        "is_preferred": true,
                                    },
                                    {
                                        "alias_key": "βeta",
                                        "display_text": "βeta",
                                        "is_preferred": false,
                                    },
                                ],
                            ]),
                        ),
                        response,
                        invocation_count,
                    )
                }
            })
            .await;

        assert_eq!(result, Ok(expected_record));
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn idempotent_create_returns_stored_display_text_and_revision() {
        let candidate = concept_id(2);
        let input = ValidatedCreateConcept::new(
            candidate,
            alias_set(&[("Primary   Alpha", true), ("βeta", false)]),
        );
        let stored_id = concept_id(52);
        let expected_record = concept(stored_id, 9, &[("PRIMARY ALPHA", true), ("ΒETA", false)]);
        let response = execute_response(json!({
            "outcome": "existing",
            "record": concept_json(
                stored_id,
                9,
                &[("PRIMARY ALPHA", true), ("ΒETA", false)],
            ),
        }));
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let result = database()
            .create_concept_with(&input, {
                let invocation_count = Arc::clone(&invocation_count);
                move |request| {
                    respond(
                        request,
                        execute_request(
                            CREATE_CONCEPT_SQL,
                            json!([
                                candidate.as_uuid().to_string(),
                                [
                                    {
                                        "alias_key": "primary alpha",
                                        "display_text": "Primary   Alpha",
                                        "is_preferred": true,
                                    },
                                    {
                                        "alias_key": "βeta",
                                        "display_text": "βeta",
                                        "is_preferred": false,
                                    },
                                ],
                            ]),
                        ),
                        response,
                        invocation_count,
                    )
                }
            })
            .await;

        assert_eq!(result, Ok(expected_record));
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn replace_aliases_invokes_routine_with_complete_normalized_set_and_revision() {
        let id = concept_id(3);
        let input = replace_input(id, revision(41));
        let expected_record = concept(id, 42, &[("Alpha Label", true), ("Zulu Label", false)]);
        let response = execute_response(json!({
            "outcome": "updated",
            "record": concept_json(id, 42, &[("Alpha Label", true), ("Zulu Label", false)]),
        }));
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let result = database()
            .replace_concept_aliases_with(&input, {
                let invocation_count = Arc::clone(&invocation_count);
                move |request| {
                    respond(
                        request,
                        execute_request(
                            "SELECT knowledge_graph.graph_replace_concept_aliases($1::text::uuid, $2::text::bigint, $3::jsonb)",
                            json!([
                                id.as_uuid().to_string(),
                                "41",
                                [
                                    {
                                        "alias_key": "alpha label",
                                        "display_text": "Alpha Label",
                                        "is_preferred": true,
                                    },
                                    {
                                        "alias_key": "zulu label",
                                        "display_text": "Zulu Label",
                                        "is_preferred": false,
                                    },
                                ],
                            ]),
                        ),
                        response,
                        invocation_count,
                    )
                }
            })
            .await;

        assert_eq!(result, Ok(expected_record));
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn create_and_replace_decode_alias_and_snapshot_conflicts_without_alias_leaks() {
        let candidate = concept_id(4);
        for (reason, owner, current_revision) in [
            (ConflictReason::AliasOwned, concept_id(40), 6),
            (ConflictReason::AliasSetMismatch, concept_id(41), 7),
        ] {
            let input = create_input(candidate);
            let response = execute_response(alias_conflict_row(reason, owner, current_revision));
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let result = database()
                .create_concept_with(&input, {
                    let invocation_count = Arc::clone(&invocation_count);
                    move |request| {
                        respond(
                            request,
                            execute_request(
                                CREATE_CONCEPT_SQL,
                                json!([
                                    candidate.as_uuid().to_string(),
                                    [
                                        {
                                            "alias_key": "primary alpha",
                                            "display_text": "Primary  Alpha",
                                            "is_preferred": true,
                                        },
                                        {
                                            "alias_key": "βeta",
                                            "display_text": "βeta",
                                            "is_preferred": false,
                                        },
                                    ],
                                ]),
                            ),
                            response,
                            invocation_count,
                        )
                    }
                })
                .await;
            let error = result.expect_err("alias conflicts should be surfaced");
            assert_eq!(
                error,
                expected_conflict(
                    OperationCode::CreateConcept,
                    ConflictField::AliasSet,
                    reason,
                    owner,
                    current_revision,
                )
            );
            assert_opaque(&error, &[ALIAS_SENTINEL, "primary alpha", "βeta"]);
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        }

        let id = concept_id(5);
        for (field, reason, conflict_id, current_revision) in [
            (
                ConflictField::AliasSet,
                ConflictReason::AliasOwned,
                concept_id(50),
                10,
            ),
            (
                ConflictField::AliasSet,
                ConflictReason::AliasSetMismatch,
                concept_id(51),
                11,
            ),
            (
                ConflictField::Snapshot,
                ConflictReason::SnapshotDrift,
                id,
                12,
            ),
        ] {
            let input = replace_input(id, revision(9));
            let response = execute_response(json!({
                "outcome": "conflict",
                "field": field.as_str(),
                "reason": reason.as_str(),
                "identity": {"kind": "concept", "id": conflict_id.as_uuid().to_string()},
                "current_revision": current_revision,
            }));
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let result = database()
                .replace_concept_aliases_with(&input, {
                    let invocation_count = Arc::clone(&invocation_count);
                    move |request| {
                        respond(
                            request,
                            execute_request(
                                REPLACE_CONCEPT_ALIASES_SQL,
                                json!([
                                    id.as_uuid().to_string(),
                                    "9",
                                    [
                                        {
                                            "alias_key": "alpha label",
                                            "display_text": "Alpha Label",
                                            "is_preferred": true,
                                        },
                                        {
                                            "alias_key": "zulu label",
                                            "display_text": "Zulu Label",
                                            "is_preferred": false,
                                        },
                                    ],
                                ]),
                            ),
                            response,
                            invocation_count,
                        )
                    }
                })
                .await;
            let error = result.expect_err("replacement conflicts should be surfaced");
            assert_eq!(
                error,
                expected_conflict(
                    OperationCode::ReplaceConceptAliases,
                    field,
                    reason,
                    conflict_id,
                    current_revision,
                )
            );
            assert_opaque(&error, &["Alpha Label", "Zulu Label"]);
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn replace_aliases_decodes_stale_revision_and_missing_concept() {
        let id = concept_id(53);
        let input = replace_input(id, revision(15));
        let cases = [
            (
                json!({
                    "outcome": "stale",
                    "identity": {"kind": "concept", "id": id.as_uuid().to_string()},
                    "current_revision": 16,
                }),
                expected_conflict(
                    OperationCode::ReplaceConceptAliases,
                    ConflictField::Revision,
                    ConflictReason::StaleRevision,
                    id,
                    16,
                ),
            ),
            (
                json!({
                    "outcome": "missing",
                    "record_kind": "concept",
                    "identity": {"kind": "concept", "id": id.as_uuid().to_string()},
                }),
                expected_not_found(OperationCode::ReplaceConceptAliases, id),
            ),
        ];

        for (row, expected) in cases {
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let result = database()
                .replace_concept_aliases_with(&input, {
                    let invocation_count = Arc::clone(&invocation_count);
                    move |request| {
                        respond(
                            request,
                            execute_request(
                                REPLACE_CONCEPT_ALIASES_SQL,
                                json!([
                                    id.as_uuid().to_string(),
                                    "15",
                                    [
                                        {
                                            "alias_key": "alpha label",
                                            "display_text": "Alpha Label",
                                            "is_preferred": true,
                                        },
                                        {
                                            "alias_key": "zulu label",
                                            "display_text": "Zulu Label",
                                            "is_preferred": false,
                                        },
                                    ],
                                ]),
                            ),
                            execute_response(row),
                            invocation_count,
                        )
                    }
                })
                .await;
            assert_eq!(result, Err(expected));
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn delete_concept_decodes_confirmed_delete_conflicts_references_and_missing() {
        let id = concept_id(6);
        let input = delete_input(id, revision(42));
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let deleted = database()
            .delete_concept_with(&input, {
                let invocation_count = Arc::clone(&invocation_count);
                move |request| {
                    respond(
                        request,
                        execute_request(
                            "SELECT knowledge_graph.graph_delete_concept($1::text::uuid, $2::text::bigint)",
                            json!([id.as_uuid().to_string(), "42"]),
                        ),
                        execute_response(json!({"outcome": "deleted"})),
                        invocation_count,
                    )
                }
            })
            .await;
        assert_eq!(deleted, Ok(()));
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);

        let cases = [
            (
                json!({
                    "outcome": "stale",
                    "identity": {"kind": "concept", "id": id.as_uuid().to_string()},
                    "current_revision": 43,
                }),
                expected_conflict(
                    OperationCode::DeleteConcept,
                    ConflictField::Revision,
                    ConflictReason::StaleRevision,
                    id,
                    43,
                ),
            ),
            (
                json!({
                    "outcome": "missing",
                    "record_kind": "concept",
                    "identity": {"kind": "concept", "id": id.as_uuid().to_string()},
                }),
                expected_not_found(OperationCode::DeleteConcept, id),
            ),
            (
                json!({
                    "outcome": "referenced",
                    "reference": {"kind": "concept", "id": id.as_uuid().to_string()},
                }),
                GraphError::Referenced {
                    operation: OperationCode::DeleteConcept,
                    record: ReferencedRecord::Concept(id),
                    reason: ReferenceReason::InUse,
                },
            ),
            (
                json!({
                    "outcome": "conflict",
                    "field": "snapshot",
                    "reason": "snapshot_drift",
                    "identity": {"kind": "concept", "id": id.as_uuid().to_string()},
                    "current_revision": 44,
                }),
                expected_conflict(
                    OperationCode::DeleteConcept,
                    ConflictField::Snapshot,
                    ConflictReason::SnapshotDrift,
                    id,
                    44,
                ),
            ),
        ];

        for (row, expected) in cases {
            let calls = Arc::new(AtomicUsize::new(0));
            let result = database()
                .delete_concept_with(&input, {
                    let calls = Arc::clone(&calls);
                    move |request| {
                        respond(
                            request,
                            execute_request(
                                DELETE_CONCEPT_SQL,
                                json!([id.as_uuid().to_string(), "42"]),
                            ),
                            execute_response(row),
                            calls,
                        )
                    }
                })
                .await;
            assert_eq!(result, Err(expected));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }

        let other_id = concept_id(60);
        let result = database()
            .delete_concept_with(&input, move |request| {
                respond(
                    request,
                    execute_request(DELETE_CONCEPT_SQL, json!([id.as_uuid().to_string(), "42"])),
                    execute_response(json!({
                        "outcome": "referenced",
                        "reference": {"kind": "concept", "id": other_id.as_uuid().to_string()},
                    })),
                    Arc::new(AtomicUsize::new(0)),
                )
            })
            .await;
        assert_eq!(
            result,
            Err(expected_invalid_response(OperationCode::DeleteConcept))
        );
    }

    #[tokio::test]
    async fn out_of_scope_concept_outcomes_are_operation_only_invalid_responses() {
        let id = concept_id(7);
        let create = create_input(id);
        let create_result = database()
            .create_concept_with(&create, move |request| {
                respond(
                    request,
                    execute_request(
                        CREATE_CONCEPT_SQL,
                        json!([
                            id.as_uuid().to_string(),
                            [
                                {
                                    "alias_key": "primary alpha",
                                    "display_text": "Primary  Alpha",
                                    "is_preferred": true,
                                },
                                {
                                    "alias_key": "βeta",
                                    "display_text": "βeta",
                                    "is_preferred": false,
                                },
                            ],
                        ]),
                    ),
                    execute_response(json!({
                        "outcome": "deleted",
                        "alias": ALIAS_SENTINEL,
                    })),
                    Arc::new(AtomicUsize::new(0)),
                )
            })
            .await;
        let create_error = create_result.expect_err("delete is outside create outcomes");
        assert_eq!(
            create_error,
            GraphError::InvalidResponse {
                operation: OperationCode::CreateConcept,
                reason: BackendFailureReason::UnsupportedValue,
            }
        );
        assert_opaque(&create_error, &[ALIAS_SENTINEL]);

        let delete = delete_input(id, revision(1));
        let delete_result = database()
            .delete_concept_with(&delete, move |request| {
                respond(
                    request,
                    execute_request(DELETE_CONCEPT_SQL, json!([id.as_uuid().to_string(), "1"])),
                    execute_response(json!({
                        "outcome": "created",
                        "record": concept_json(id, 1, &[(ALIAS_SENTINEL, true)]),
                    })),
                    Arc::new(AtomicUsize::new(0)),
                )
            })
            .await;
        let delete_error = delete_result.expect_err("create is outside delete outcomes");
        assert_eq!(
            delete_error,
            expected_invalid_response(OperationCode::DeleteConcept)
        );
        assert_opaque(&delete_error, &[ALIAS_SENTINEL]);
    }

    #[tokio::test]
    async fn direct_and_alias_reads_use_static_queries_and_return_complete_ordered_concepts() {
        let id = concept_id(8);
        assert!(GET_CONCEPT_SQL.contains("WHERE concepts.id = $1::text::uuid"));
        let aliases = &[
            ("Main Label", true),
            ("Straße", false),
            ("Zulu Label", false),
        ];
        let expected_record = concept(id, 13, aliases);
        let result_value = concept_json(id, 13, aliases);

        let invocation_count = Arc::new(AtomicUsize::new(0));
        let direct = database()
            .get_concept_with(&id, {
                let invocation_count = Arc::clone(&invocation_count);
                move |request| {
                    respond(
                        request,
                        query_request(GET_CONCEPT_SQL, json!([id.as_uuid().to_string()])),
                        query_response(complete_payload(result_value)),
                        invocation_count,
                    )
                }
            })
            .await;
        assert_eq!(direct, Ok(Some(expected_record.clone())));
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);

        let key: AliasKey = normalize_alias_key("STRASSE").expect("alias key should normalize");
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let resolved = database()
            .resolve_alias_with(&key, {
                let invocation_count = Arc::clone(&invocation_count);
                move |request| {
                    respond(
                        request,
                        query_request(RESOLVE_ALIAS_SQL, json!(["strasse"])),
                        query_response(complete_payload(concept_json(id, 13, aliases))),
                        invocation_count,
                    )
                }
            })
            .await;
        assert_eq!(resolved, Ok(Some(expected_record)));
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        assert!(!GET_CONCEPT_SQL.contains(ALIAS_SENTINEL));
        assert!(!RESOLVE_ALIAS_SQL.contains(ALIAS_SENTINEL));
    }

    #[tokio::test]
    async fn concept_reads_decode_no_match_result_bound_and_reject_noncanonical_alias_order() {
        let id = concept_id(9);
        let no_match = database()
            .get_concept_with(&id, move |request| {
                respond(
                    request,
                    query_request(GET_CONCEPT_SQL, json!([id.as_uuid().to_string()])),
                    query_response(complete_payload(Value::Null)),
                    Arc::new(AtomicUsize::new(0)),
                )
            })
            .await;
        assert_eq!(no_match, Ok(None));

        let missing_key =
            normalize_alias_key("missing alias").expect("missing alias key should normalize");
        let missing_key_text = missing_key.as_str().to_owned();
        let alias_no_match = database()
            .resolve_alias_with(&missing_key, move |request| {
                respond(
                    request,
                    query_request(RESOLVE_ALIAS_SQL, json!([missing_key_text])),
                    query_response(complete_payload(Value::Null)),
                    Arc::new(AtomicUsize::new(0)),
                )
            })
            .await;
        assert_eq!(alias_no_match, Ok(None));

        let key = normalize_alias_key("missing alias").expect("alias key should normalize");
        let key_text = key.as_str().to_owned();
        let too_large = database()
            .resolve_alias_with(&key, move |request| {
                respond(
                    request,
                    query_request(RESOLVE_ALIAS_SQL, json!([key_text])),
                    query_response(json!({"outcome": "result_too_large"})),
                    Arc::new(AtomicUsize::new(0)),
                )
            })
            .await;
        assert_eq!(
            too_large,
            Err(GraphError::ResultBoundExceeded {
                operation: OperationCode::ResolveAlias,
            })
        );

        let noncanonical = database()
            .get_concept_with(&id, move |request| {
                respond(
                    request,
                    query_request(GET_CONCEPT_SQL, json!([id.as_uuid().to_string()])),
                    query_response(complete_payload(concept_json(
                        id,
                        1,
                        &[
                            ("Preferred Label", true),
                            ("Zulu Label", false),
                            ("Alpha Label", false),
                        ],
                    ))),
                    Arc::new(AtomicUsize::new(0)),
                )
            })
            .await;
        assert_eq!(
            noncanonical,
            Err(expected_invalid_response(OperationCode::GetConcept))
        );
    }

    #[tokio::test]
    async fn malformed_concept_reads_and_backend_failures_do_not_echo_alias_values() {
        let id = concept_id(10);
        let key = normalize_alias_key(ALIAS_SENTINEL).expect("sentinel key should normalize");
        let key_text = key.as_str().to_owned();
        let malformed = database()
            .resolve_alias_with(&key, move |request| {
                respond(
                    request,
                    query_request(RESOLVE_ALIAS_SQL, json!([key_text])),
                    query_response(complete_payload(concept_json(
                        id,
                        1,
                        &[(ALIAS_SENTINEL, false)],
                    ))),
                    Arc::new(AtomicUsize::new(0)),
                )
            })
            .await;
        let error = malformed.expect_err("a concept without a preferred alias must fail");
        assert_eq!(
            error,
            expected_invalid_response(OperationCode::ResolveAlias)
        );
        assert_opaque(&error, &[ALIAS_SENTINEL]);

        let key = normalize_alias_key(ALIAS_SENTINEL).expect("sentinel key should normalize");
        let backend_error = database()
            .resolve_alias_with(&key, move |request| async move {
                assert_eq!(request.function_id, DATABASE_QUERY_FUNCTION_ID);
                Err(IiiError::Handler(ALIAS_SENTINEL.to_owned()))
            })
            .await
            .expect_err("backend failures should remain opaque");
        assert_eq!(
            backend_error,
            expected_database_failure(OperationCode::ResolveAlias)
        );
        assert_opaque(&backend_error, &[ALIAS_SENTINEL]);
    }
}

#[cfg(test)]
mod iii_source_mention_operation_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use iii_sdk::{Error as IiiError, IIIClient, protocol::TriggerRequest};
    use serde_json::{Value, json};
    use uuid::Uuid;

    use crate::contracts::{
        BackendFailureReason, ConceptId, ConceptMention, DatabaseTimeouts, GraphError,
        NotFoundReason, OperationCode, RecordIdentity, RecordKind, ReferenceReason,
        ReferencedRecord, SourceReference, SourceReferenceInput, ValidatedConceptMention,
        ValidatedDeleteSource, ValidatedSourceReference,
    };

    use super::{
        DATABASE_EXECUTE_FUNCTION_ID, DATABASE_QUERY_FUNCTION_ID, DatabaseTarget,
        IiiKnowledgeGraphDatabase,
    };

    const DATABASE_TARGET: &str = "source-mention-operation-database-target";
    const SOURCE_KEY_SENTINEL: &str = "SOURCE_REFERENCE_PRIVATE_KEY_SENTINEL";
    const UUID_V7_BASE: u128 = 0x0000_0000_0000_7000_8000_0000_0000_0000;
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

    #[derive(Clone)]
    struct ExpectedRequest {
        function_id: &'static str,
        payload: Value,
        timeout_ms: Option<u64>,
    }

    fn database() -> IiiKnowledgeGraphDatabase {
        IiiKnowledgeGraphDatabase::new(
            IIIClient::new("ws://127.0.0.1:1"),
            DatabaseTarget::try_from(DATABASE_TARGET.to_owned())
                .expect("test database target should be valid"),
            DatabaseTimeouts::try_new(Duration::from_secs(4), Duration::from_secs(13))
                .expect("test timeouts should be valid"),
        )
        .expect("adapter configuration should be valid")
    }

    fn concept_id(value: u128) -> ConceptId {
        ConceptId::try_from(Uuid::from_u128(UUID_V7_BASE + value))
            .expect("fixture concept ID should be UUIDv7")
    }

    fn max_source_key() -> String {
        format!("雪{}", "x".repeat(2_045))
    }

    fn source_cases() -> Vec<(SourceReferenceInput, SourceReference)> {
        let key = max_source_key();
        let memory = SourceReferenceInput::MemoryVersion {
            memory_id: key.clone(),
            version: 23,
        };
        let session = SourceReferenceInput::SessionRecord {
            session_record_id: key,
        };

        [memory, session]
            .into_iter()
            .map(|input| {
                let source = SourceReference::try_from(input.clone())
                    .expect("fixture source should satisfy its owner contract");
                (input, source)
            })
            .collect()
    }

    fn validated_source(input: SourceReferenceInput) -> ValidatedSourceReference {
        ValidatedSourceReference::try_from(input).expect("fixture source should be valid")
    }

    fn validated_delete_source(source: SourceReferenceInput) -> ValidatedDeleteSource {
        ValidatedDeleteSource::try_from(crate::contracts::DeleteSource { source })
            .expect("fixture source delete should be valid")
    }

    fn validated_mention(id: ConceptId, source: SourceReferenceInput) -> ValidatedConceptMention {
        ValidatedConceptMention::try_from(crate::contracts::ConceptMentionInput {
            concept_id: id,
            source,
        })
        .expect("fixture mention should be valid")
    }

    fn source_key(source: &SourceReference) -> &str {
        match source {
            SourceReference::MemoryVersion { memory_id, .. } => memory_id.as_str(),
            SourceReference::SessionRecord { session_record_id } => session_record_id.as_str(),
        }
    }

    fn source_params(source: &SourceReference) -> Value {
        match source {
            SourceReference::MemoryVersion { memory_id, version } => {
                json!([
                    "memory_version",
                    memory_id.as_str(),
                    version.get().to_string()
                ])
            }
            SourceReference::SessionRecord { session_record_id } => {
                json!(["session_record", session_record_id.as_str(), null])
            }
        }
    }

    fn mention_params(id: ConceptId, source: &SourceReference) -> Value {
        let mut params = vec![json!(id.as_uuid().to_string())];
        params.extend(
            source_params(source)
                .as_array()
                .expect("source parameters should be an array")
                .iter()
                .cloned(),
        );
        Value::Array(params)
    }

    fn source_record(source: &SourceReference, source_ref_id: i64) -> Value {
        match source {
            SourceReference::MemoryVersion { memory_id, version } => json!({
                "source_ref_id": source_ref_id,
                "source_kind": "memory_version",
                "external_id": memory_id.as_str(),
                "external_version": version.get(),
            }),
            SourceReference::SessionRecord { session_record_id } => json!({
                "source_ref_id": source_ref_id,
                "source_kind": "session_record",
                "external_id": session_record_id.as_str(),
                "external_version": null,
            }),
        }
    }

    fn source_identity_record(source: &SourceReference) -> Value {
        match source {
            SourceReference::MemoryVersion { memory_id, version } => json!({
                "source_kind": "memory_version",
                "external_id": memory_id.as_str(),
                "external_version": version.get(),
            }),
            SourceReference::SessionRecord { session_record_id } => json!({
                "source_kind": "session_record",
                "external_id": session_record_id.as_str(),
                "external_version": null,
            }),
        }
    }

    fn source_json(source: &SourceReference) -> Value {
        match source {
            SourceReference::MemoryVersion { memory_id, version } => json!({
                "kind": "memory_version",
                "memory_id": memory_id.as_str(),
                "version": version.get(),
            }),
            SourceReference::SessionRecord { session_record_id } => json!({
                "kind": "session_record",
                "session_record_id": session_record_id.as_str(),
            }),
        }
    }

    fn mention_record(id: ConceptId, source: &SourceReference, source_ref_id: i64) -> Value {
        json!({
            "concept_id": id.as_uuid().to_string(),
            "source_ref_id": source_ref_id,
            "source": source_identity_record(source),
        })
    }

    fn execute_response(row: Value) -> Value {
        json!({
            "affected_rows": 1,
            "last_insert_id": null,
            "returned_rows": [row],
        })
    }

    fn query_response(payload: Value) -> Value {
        json!({
            "rows": [{"payload": payload}],
            "row_count": 1,
            "columns": [{"name": "payload", "type": "jsonb"}],
        })
    }

    fn complete_payload(result: Value) -> Value {
        json!({"outcome": "complete", "result": result})
    }

    fn execute_request(sql: &'static str, params: Value) -> ExpectedRequest {
        ExpectedRequest {
            function_id: DATABASE_EXECUTE_FUNCTION_ID,
            payload: json!({
                "db": DATABASE_TARGET,
                "sql": sql,
                "params": params,
            }),
            timeout_ms: Some(13_000),
        }
    }

    fn query_request(sql: &'static str, params: Value) -> ExpectedRequest {
        ExpectedRequest {
            function_id: DATABASE_QUERY_FUNCTION_ID,
            payload: json!({
                "db": DATABASE_TARGET,
                "sql": sql,
                "params": params,
                "timeout_ms": 4_000,
            }),
            timeout_ms: Some(13_000),
        }
    }

    async fn respond(
        request: TriggerRequest,
        expected: ExpectedRequest,
        response: Value,
        invocation_count: Arc<AtomicUsize>,
    ) -> Result<Value, IiiError> {
        invocation_count.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.function_id, expected.function_id);
        assert_eq!(request.payload, expected.payload);
        assert!(request.action.is_none());
        assert_eq!(request.timeout_ms, expected.timeout_ms);
        Ok(response)
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

    fn expected_invalid_response(
        operation: OperationCode,
        reason: BackendFailureReason,
    ) -> GraphError {
        GraphError::InvalidResponse { operation, reason }
    }

    fn assert_opaque(error: &GraphError, source_key: &str) {
        let display = error.to_string();
        let debug = format!("{error:?}");
        assert!(
            !display.contains(source_key),
            "Display leaked the source key"
        );
        assert!(!debug.contains(source_key), "Debug leaked the source key");
    }

    #[tokio::test]
    async fn register_source_invokes_typed_routine_and_preserves_maximum_unicode_keys() {
        for (input, expected_source) in source_cases() {
            assert_eq!(source_key(&expected_source).len(), 2_048);
            for outcome in ["created", "existing"] {
                let invocation_count = Arc::new(AtomicUsize::new(0));
                let response = execute_response(json!({
                    "outcome": outcome,
                    "record": source_record(&expected_source, 91),
                }));
                let result = database()
                    .register_source_with(&validated_source(input.clone()), {
                        let invocation_count = Arc::clone(&invocation_count);
                        let expected = execute_request(
                            "SELECT knowledge_graph.graph_register_source($1::text, $2::text, $3::text::bigint)",
                            source_params(&expected_source),
                        );
                        move |request| respond(request, expected, response, invocation_count)
                    })
                    .await;

                assert_eq!(result, Ok(expected_source.clone()));
                assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
            }
        }
    }

    #[tokio::test]
    async fn create_mention_invokes_typed_routine_and_decodes_created_or_existing_records() {
        let id = concept_id(1);
        for (source_input, source) in source_cases() {
            for outcome in ["created", "existing"] {
                let invocation_count = Arc::new(AtomicUsize::new(0));
                let response = execute_response(json!({
                    "outcome": outcome,
                    "record": mention_record(id, &source, 92),
                }));
                let result = database()
                    .create_mention_with(&validated_mention(id, source_input.clone()), {
                        let invocation_count = Arc::clone(&invocation_count);
                        let expected = execute_request(
                            "SELECT knowledge_graph.graph_create_mention($1::text::uuid, $2::text, $3::text, $4::text::bigint)",
                            mention_params(id, &source),
                        );
                        move |request| respond(request, expected, response, invocation_count)
                    })
                    .await;

                assert_eq!(
                    result,
                    Ok(ConceptMention {
                        concept_id: id,
                        source: source.clone(),
                    })
                );
                assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
            }
        }
    }

    #[tokio::test]
    async fn delete_source_invokes_typed_routine_for_both_source_variants() {
        for (input, source) in source_cases() {
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let response = execute_response(json!({"outcome": "deleted"}));
            let result = database()
                .delete_source_with(&validated_delete_source(input), {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected = execute_request(
                        "SELECT knowledge_graph.graph_delete_source($1::text, $2::text, $3::text::bigint)",
                        source_params(&source),
                    );
                    move |request| respond(request, expected, response, invocation_count)
                })
                .await;

            assert_eq!(result, Ok(()));
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn delete_source_decodes_delete_missing_reference_and_snapshot_conflict_without_key() {
        let input = SourceReferenceInput::SessionRecord {
            session_record_id: SOURCE_KEY_SENTINEL.to_owned(),
        };
        let cases = [
            (json!({"outcome": "deleted"}), Ok(())),
            (
                json!({"outcome": "missing", "record_kind": "source_reference"}),
                Err(expected_not_found(
                    OperationCode::DeleteSource,
                    RecordKind::SourceReference,
                    None,
                )),
            ),
            (
                json!({
                    "outcome": "missing",
                    "record_kind": "source_reference",
                    "identity": null,
                }),
                Err(expected_invalid_response(
                    OperationCode::DeleteSource,
                    BackendFailureReason::UnsupportedValue,
                )),
            ),
            (
                json!({
                    "outcome": "referenced",
                    "reference_kind": "source_reference",
                }),
                Err(GraphError::Referenced {
                    operation: OperationCode::DeleteSource,
                    record: ReferencedRecord::SourceReference,
                    reason: ReferenceReason::InUse,
                }),
            ),
            (
                json!({
                    "outcome": "conflict",
                    "field": "snapshot",
                    "reason": "snapshot_drift",
                }),
                Err(expected_database_failure(OperationCode::DeleteSource)),
            ),
        ];

        for (row, expected_result) in cases {
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let result = database()
                .delete_source_with(&validated_delete_source(input.clone()), {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected = execute_request(
                        "SELECT knowledge_graph.graph_delete_source($1::text, $2::text, $3::text::bigint)",
                        json!(["session_record", SOURCE_KEY_SENTINEL, null]),
                    );
                    let response = execute_response(row);
                    move |request| respond(request, expected, response, invocation_count)
                })
                .await;

            assert_eq!(result, expected_result);
            if let Err(error) = &result {
                assert_opaque(error, SOURCE_KEY_SENTINEL);
            }
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn create_mention_decodes_missing_endpoints_and_snapshot_conflicts_without_key() {
        let id = concept_id(2);
        let source_input = SourceReferenceInput::SessionRecord {
            session_record_id: SOURCE_KEY_SENTINEL.to_owned(),
        };
        let cases = [
            (
                json!({
                    "outcome": "missing",
                    "record_kind": "concept",
                    "identity": {"kind": "concept", "id": id.as_uuid().to_string()},
                }),
                expected_not_found(
                    OperationCode::CreateMention,
                    RecordKind::Concept,
                    Some(RecordIdentity::Concept(id)),
                ),
            ),
            (
                json!({"outcome": "missing", "record_kind": "source_reference"}),
                expected_not_found(
                    OperationCode::CreateMention,
                    RecordKind::SourceReference,
                    None,
                ),
            ),
            (
                json!({
                    "outcome": "conflict",
                    "field": "snapshot",
                    "reason": "snapshot_drift",
                }),
                expected_database_failure(OperationCode::CreateMention),
            ),
        ];

        for (row, expected_error) in cases {
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let result = database()
                .create_mention_with(&validated_mention(id, source_input.clone()), {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected = execute_request(
                        "SELECT knowledge_graph.graph_create_mention($1::text::uuid, $2::text, $3::text, $4::text::bigint)",
                        json!([
                            id.as_uuid().to_string(),
                            "session_record",
                            SOURCE_KEY_SENTINEL,
                            null,
                        ]),
                    );
                    let response = execute_response(row);
                    move |request| respond(request, expected, response, invocation_count)
                })
                .await;

            let error = result.expect_err("missing endpoint or conflict should fail");
            assert_eq!(error, expected_error);
            assert_opaque(&error, SOURCE_KEY_SENTINEL);
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn delete_mention_invokes_typed_routine_for_both_source_variants() {
        let id = concept_id(5);
        for (source_input, source) in source_cases() {
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let response = execute_response(json!({"outcome": "deleted"}));
            let result = database()
                .delete_mention_with(&validated_mention(id, source_input), {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected = execute_request(
                        "SELECT knowledge_graph.graph_delete_mention($1::text::uuid, $2::text, $3::text, $4::text::bigint)",
                        mention_params(id, &source),
                    );
                    move |request| respond(request, expected, response, invocation_count)
                })
                .await;

            assert_eq!(result, Ok(()));
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn delete_mention_decodes_confirmed_deletion_all_missing_endpoints_and_conflict() {
        let id = concept_id(3);
        let source_input = SourceReferenceInput::SessionRecord {
            session_record_id: SOURCE_KEY_SENTINEL.to_owned(),
        };
        let cases = [
            (json!({"outcome": "deleted"}), Ok(())),
            (
                json!({
                    "outcome": "missing",
                    "record_kind": "concept",
                    "identity": {"kind": "concept", "id": id.as_uuid().to_string()},
                }),
                Err(expected_not_found(
                    OperationCode::DeleteMention,
                    RecordKind::Concept,
                    Some(RecordIdentity::Concept(id)),
                )),
            ),
            (
                json!({"outcome": "missing", "record_kind": "source_reference"}),
                Err(expected_not_found(
                    OperationCode::DeleteMention,
                    RecordKind::SourceReference,
                    None,
                )),
            ),
            (
                json!({"outcome": "missing", "record_kind": "concept_mention"}),
                Err(expected_not_found(
                    OperationCode::DeleteMention,
                    RecordKind::ConceptMention,
                    None,
                )),
            ),
            (
                json!({
                    "outcome": "conflict",
                    "field": "snapshot",
                    "reason": "snapshot_drift",
                }),
                Err(expected_database_failure(OperationCode::DeleteMention)),
            ),
        ];

        for (row, expected_result) in cases {
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let result = database()
                .delete_mention_with(&validated_mention(id, source_input.clone()), {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected = execute_request(
                        "SELECT knowledge_graph.graph_delete_mention($1::text::uuid, $2::text, $3::text, $4::text::bigint)",
                        json!([
                            id.as_uuid().to_string(),
                            "session_record",
                            SOURCE_KEY_SENTINEL,
                            null,
                        ]),
                    );
                    let response = execute_response(row);
                    move |request| respond(request, expected, response, invocation_count)
                })
                .await;

            assert_eq!(result, expected_result);
            if let Err(error) = &result {
                assert_opaque(error, SOURCE_KEY_SENTINEL);
            }
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn get_source_returns_typed_records_or_confirmed_absence_for_both_variants() {
        for (input, expected_source) in source_cases() {
            assert_eq!(source_key(&expected_source).len(), 2_048);
            let params = source_params(&expected_source);
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let result = database()
                .get_source_with(&validated_source(input.clone()), {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected = query_request(GET_SOURCE_SQL, params.clone());
                    let response = query_response(complete_payload(source_json(&expected_source)));
                    move |request| respond(request, expected, response, invocation_count)
                })
                .await;
            assert_eq!(result, Ok(Some(expected_source.clone())));
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);

            let invocation_count = Arc::new(AtomicUsize::new(0));
            let absent = database()
                .get_source_with(&validated_source(input), {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected = query_request(GET_SOURCE_SQL, params);
                    let response = query_response(complete_payload(Value::Null));
                    move |request| respond(request, expected, response, invocation_count)
                })
                .await;
            assert_eq!(absent, Ok(None));
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn source_and_mention_decoders_reject_malformed_extra_and_unexpected_data_without_key() {
        let input = SourceReferenceInput::SessionRecord {
            session_record_id: SOURCE_KEY_SENTINEL.to_owned(),
        };
        let expected_source = SourceReference::try_from(input.clone())
            .expect("fixture source should satisfy its owner contract");
        let invalid_rows = [
            (
                json!({
                    "outcome": "created",
                    "record": source_record(&expected_source, 91),
                    "source_key": SOURCE_KEY_SENTINEL,
                }),
                BackendFailureReason::UnsupportedValue,
            ),
            (
                json!({
                    "outcome": "created",
                    "record": {
                        "source_ref_id": 91,
                        "source_kind": "session_record",
                        "external_id": SOURCE_KEY_SENTINEL,
                        "external_version": 23,
                    },
                }),
                BackendFailureReason::UnsupportedValue,
            ),
            (
                json!({
                    "outcome": "created",
                    "record": {
                        "source_ref_id": 91,
                        "source_kind": "session_record",
                        "external_id": "different-key",
                        "external_version": null,
                    },
                }),
                BackendFailureReason::UnsupportedValue,
            ),
            (
                json!({"outcome": "unapproved-source-outcome"}),
                BackendFailureReason::UnknownOutcome,
            ),
            (
                json!({"outcome": "deleted"}),
                BackendFailureReason::UnsupportedValue,
            ),
        ];

        for (row, reason) in invalid_rows {
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let result = database()
                .register_source_with(&validated_source(input.clone()), {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected = execute_request(
                        "SELECT knowledge_graph.graph_register_source($1::text, $2::text, $3::text::bigint)",
                        json!(["session_record", SOURCE_KEY_SENTINEL, null]),
                    );
                    let response = execute_response(row);
                    move |request| respond(request, expected, response, invocation_count)
                })
                .await;
            let error = result.expect_err("invalid source outcomes should fail");
            assert_eq!(
                error,
                expected_invalid_response(OperationCode::RegisterSource, reason)
            );
            assert_opaque(&error, SOURCE_KEY_SENTINEL);
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        }

        let id = concept_id(4);
        let mention = SourceReference::try_from(input.clone())
            .expect("fixture source should satisfy its owner contract");
        let response = execute_response(json!({
            "outcome": "created",
            "record": {
                "concept_id": id.as_uuid().to_string(),
                "source_ref_id": 92,
                "source": {
                    "source_kind": "session_record",
                    "external_id": SOURCE_KEY_SENTINEL,
                    "external_version": 23,
                },
            },
        }));
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let result = database()
            .create_mention_with(&validated_mention(id, input), {
                let invocation_count = Arc::clone(&invocation_count);
                let expected = execute_request(
                    "SELECT knowledge_graph.graph_create_mention($1::text::uuid, $2::text, $3::text, $4::text::bigint)",
                    json!([
                        id.as_uuid().to_string(),
                        "session_record",
                        SOURCE_KEY_SENTINEL,
                        null,
                    ]),
                );
                move |request| respond(request, expected, response, invocation_count)
            })
            .await;
        let error = result.expect_err("malformed mention source shape should fail");
        assert_eq!(
            error,
            expected_invalid_response(
                OperationCode::CreateMention,
                BackendFailureReason::UnsupportedValue
            )
        );
        assert_opaque(&error, SOURCE_KEY_SENTINEL);
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        assert_eq!(source_key(&mention), SOURCE_KEY_SENTINEL);
    }

    #[tokio::test]
    async fn source_query_rejects_nonmatching_records_without_echoing_key() {
        let input = SourceReferenceInput::SessionRecord {
            session_record_id: SOURCE_KEY_SENTINEL.to_owned(),
        };
        let expected_source = SourceReference::try_from(input.clone())
            .expect("fixture source should satisfy its owner contract");
        let response = query_response(complete_payload(json!({
            "kind": "session_record",
            "session_record_id": "different-key",
        })));
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let result = database()
            .get_source_with(&validated_source(input), {
                let invocation_count = Arc::clone(&invocation_count);
                let expected = query_request(GET_SOURCE_SQL, source_params(&expected_source));
                move |request| respond(request, expected, response, invocation_count)
            })
            .await;

        let error = result.expect_err("a different typed source identity must not be returned");
        assert_eq!(
            error,
            expected_invalid_response(
                OperationCode::GetSource,
                BackendFailureReason::UnsupportedValue
            )
        );
        assert_opaque(&error, SOURCE_KEY_SENTINEL);
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn source_invocation_failures_are_operation_only_and_never_echo_keys() {
        let input = SourceReferenceInput::SessionRecord {
            session_record_id: SOURCE_KEY_SENTINEL.to_owned(),
        };
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let error = database()
            .register_source_with(&validated_source(input), {
                let invocation_count = Arc::clone(&invocation_count);
                move |_| {
                    invocation_count.fetch_add(1, Ordering::SeqCst);
                    async { Err(IiiError::Handler(SOURCE_KEY_SENTINEL.to_owned())) }
                }
            })
            .await
            .expect_err("a backend failure must not return a source record");

        assert_eq!(
            error,
            expected_database_failure(OperationCode::RegisterSource)
        );
        assert_opaque(&error, SOURCE_KEY_SENTINEL);
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
    }
}

#[cfg(test)]
mod iii_assertion_operation_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use iii_sdk::{Error as IiiError, IIIClient, protocol::TriggerRequest};
    use serde_json::{Value, json};
    use uuid::Uuid;

    use crate::contracts::{
        Assertion, AssertionId, BackendFailureReason, ConceptId, ConflictField, ConflictReason,
        CreateAssertion, DeleteAssertion, GraphError, LimitReason, LimitResource, NotFoundReason,
        OperationCode, RecordIdentity, RecordKind, RelationType, Revision, SourceReference,
        SourceReferenceInput, UpdateAssertion, ValidatedCreateAssertion, ValidatedDeleteAssertion,
        ValidatedUpdateAssertion,
    };

    use super::{
        CREATE_ASSERTION_SQL, DATABASE_EXECUTE_FUNCTION_ID, DATABASE_QUERY_FUNCTION_ID,
        DELETE_ASSERTION_SQL, DatabaseTarget, GET_ASSERTION_SQL, IiiKnowledgeGraphDatabase,
        UPDATE_ASSERTION_SQL,
    };

    const DATABASE_TARGET: &str = "assertion-operation-database-target";
    const SOURCE_KEY_SENTINEL: &str = "PRIVATE_ASSERTION_SOURCE_SENTINEL";
    const UUID_V7_BASE: u128 = 0x0000_0000_0000_7000_8000_0000_0000_0000;
    const EXPECTED_CREATE_ASSERTION_SQL: &str = "SELECT knowledge_graph.graph_create_assertion($1::text::uuid, $2::text::uuid, $3::text, $4::text::uuid, $5::jsonb)";
    const EXPECTED_UPDATE_ASSERTION_SQL: &str = "SELECT knowledge_graph.graph_update_assertion($1::text::uuid, $2::text::bigint, $3::text::uuid, $4::text, $5::text::uuid)";
    const EXPECTED_DELETE_ASSERTION_SQL: &str =
        "SELECT knowledge_graph.graph_delete_assertion($1::text::uuid, $2::text::bigint)";

    #[derive(Clone)]
    struct ExpectedRequest {
        function_id: &'static str,
        payload: Value,
        timeout_ms: Option<u64>,
    }

    fn database() -> IiiKnowledgeGraphDatabase {
        IiiKnowledgeGraphDatabase::new(
            IIIClient::new("ws://127.0.0.1:1"),
            DatabaseTarget::try_from(DATABASE_TARGET.to_owned())
                .expect("test database target should be valid"),
            crate::contracts::DatabaseTimeouts::try_new(
                Duration::from_secs(4),
                Duration::from_secs(13),
            )
            .expect("test timeouts should be valid"),
        )
        .expect("adapter configuration should be valid")
    }

    fn concept_id(value: u128) -> ConceptId {
        ConceptId::try_from(Uuid::from_u128(UUID_V7_BASE + value))
            .expect("fixture concept ID should be UUIDv7")
    }

    fn assertion_id(value: u128) -> AssertionId {
        AssertionId::try_from(Uuid::from_u128(UUID_V7_BASE + value))
            .expect("fixture assertion ID should be UUIDv7")
    }

    fn revision(value: i64) -> Revision {
        Revision::try_from(value).expect("fixture revision should be positive")
    }

    fn memory_input(memory_id: &str, version: i64) -> SourceReferenceInput {
        SourceReferenceInput::MemoryVersion {
            memory_id: memory_id.to_owned(),
            version,
        }
    }

    fn session_input(session_record_id: &str) -> SourceReferenceInput {
        SourceReferenceInput::SessionRecord {
            session_record_id: session_record_id.to_owned(),
        }
    }

    fn source_reference(input: SourceReferenceInput) -> SourceReference {
        SourceReference::try_from(input).expect("fixture source should be valid")
    }

    fn create_input(
        candidate_id: AssertionId,
        subject_concept_id: ConceptId,
        relation_type: RelationType,
        object_concept_id: ConceptId,
        supporting_sources: Vec<SourceReferenceInput>,
    ) -> ValidatedCreateAssertion {
        ValidatedCreateAssertion::try_from_input(
            candidate_id,
            CreateAssertion {
                subject_concept_id,
                relation_type,
                object_concept_id,
                supporting_sources,
            },
        )
        .expect("fixture assertion should be valid")
    }

    fn update_input(
        assertion_id: AssertionId,
        expected_revision: i64,
        subject_concept_id: ConceptId,
        relation_type: RelationType,
        object_concept_id: ConceptId,
    ) -> ValidatedUpdateAssertion {
        ValidatedUpdateAssertion::try_from_input(UpdateAssertion {
            assertion_id,
            expected_revision: revision(expected_revision),
            subject_concept_id,
            relation_type,
            object_concept_id,
        })
        .expect("fixture assertion update should be valid")
    }

    fn delete_input(assertion_id: AssertionId, expected_revision: i64) -> ValidatedDeleteAssertion {
        ValidatedDeleteAssertion::from(DeleteAssertion {
            assertion_id,
            expected_revision: revision(expected_revision),
        })
    }

    fn source_json(source: &SourceReference) -> Value {
        match source {
            SourceReference::MemoryVersion { memory_id, version } => json!({
                "kind": "memory_version",
                "memory_id": memory_id.as_str(),
                "version": version.get(),
            }),
            SourceReference::SessionRecord { session_record_id } => json!({
                "kind": "session_record",
                "session_record_id": session_record_id.as_str(),
            }),
        }
    }

    fn assertion_json(
        id: AssertionId,
        revision_value: i64,
        subject: ConceptId,
        relation: RelationType,
        object: ConceptId,
        evidence: &[SourceReference],
    ) -> Value {
        json!({
            "id": id.as_uuid().to_string(),
            "revision": revision_value,
            "subject_concept_id": subject.as_uuid().to_string(),
            "relation_type": relation.as_str(),
            "object_concept_id": object.as_uuid().to_string(),
            "evidence": evidence.iter().map(source_json).collect::<Vec<_>>(),
        })
    }

    fn assertion(
        id: AssertionId,
        revision_value: i64,
        subject: ConceptId,
        relation: RelationType,
        object: ConceptId,
        evidence: Vec<SourceReference>,
    ) -> Assertion {
        Assertion {
            id,
            revision: revision(revision_value),
            subject_concept_id: subject,
            relation_type: relation,
            object_concept_id: object,
            evidence,
        }
    }

    fn create_params(input: &ValidatedCreateAssertion) -> Value {
        json!([
            input.candidate_id.as_uuid().to_string(),
            input.identity.subject_concept_id.as_uuid().to_string(),
            input.identity.relation_type.as_str(),
            input.identity.object_concept_id.as_uuid().to_string(),
            input
                .supporting_sources
                .sources
                .iter()
                .map(|source| source_json(&source.source))
                .collect::<Vec<_>>(),
        ])
    }

    fn update_params(input: &ValidatedUpdateAssertion) -> Value {
        json!([
            input.assertion_id.as_uuid().to_string(),
            input.expected_revision.get().to_string(),
            input.identity.subject_concept_id.as_uuid().to_string(),
            input.identity.relation_type.as_str(),
            input.identity.object_concept_id.as_uuid().to_string(),
        ])
    }

    fn delete_params(input: &ValidatedDeleteAssertion) -> Value {
        json!([
            input.assertion_id.as_uuid().to_string(),
            input.expected_revision.get().to_string(),
        ])
    }

    fn execute_response(row: Value) -> Value {
        json!({
            "affected_rows": 1,
            "last_insert_id": null,
            "returned_rows": [row],
        })
    }

    fn query_response(payload: Value) -> Value {
        json!({
            "rows": [{"payload": payload}],
            "row_count": 1,
            "columns": [{"name": "payload", "type": "jsonb"}],
        })
    }

    fn complete_payload(result: Value) -> Value {
        json!({"outcome": "complete", "result": result})
    }

    fn execute_request(sql: &'static str, params: Value) -> ExpectedRequest {
        ExpectedRequest {
            function_id: DATABASE_EXECUTE_FUNCTION_ID,
            payload: json!({
                "db": DATABASE_TARGET,
                "sql": sql,
                "params": params,
            }),
            timeout_ms: Some(13_000),
        }
    }

    fn query_request(sql: &'static str, params: Value) -> ExpectedRequest {
        ExpectedRequest {
            function_id: DATABASE_QUERY_FUNCTION_ID,
            payload: json!({
                "db": DATABASE_TARGET,
                "sql": sql,
                "params": params,
                "timeout_ms": 4_000,
            }),
            timeout_ms: Some(13_000),
        }
    }

    async fn respond(
        request: TriggerRequest,
        expected: ExpectedRequest,
        response: Value,
        invocation_count: Arc<AtomicUsize>,
    ) -> Result<Value, IiiError> {
        invocation_count.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.function_id, expected.function_id);
        assert_eq!(request.payload, expected.payload);
        assert!(request.action.is_none());
        assert_eq!(request.timeout_ms, expected.timeout_ms);
        Ok(response)
    }

    fn expected_conflict(
        operation: OperationCode,
        field: ConflictField,
        reason: ConflictReason,
        id: AssertionId,
        current_revision: i64,
    ) -> GraphError {
        GraphError::Conflict {
            operation,
            field,
            reason,
            identity: RecordIdentity::Assertion(id),
            current_revision: Some(revision(current_revision)),
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

    fn expected_invalid_response(
        operation: OperationCode,
        reason: BackendFailureReason,
    ) -> GraphError {
        GraphError::InvalidResponse { operation, reason }
    }

    fn assert_opaque(error: &GraphError) {
        let display = error.to_string();
        let debug = format!("{error:?}");
        assert!(
            !display.contains(SOURCE_KEY_SENTINEL),
            "Display leaked the source key"
        );
        assert!(
            !debug.contains(SOURCE_KEY_SENTINEL),
            "Debug leaked the source key"
        );
    }

    #[test]
    fn assertion_sql_uses_worker_compatible_text_first_uuid_and_bigint_casts() {
        assert_eq!(CREATE_ASSERTION_SQL, EXPECTED_CREATE_ASSERTION_SQL);
        assert_eq!(UPDATE_ASSERTION_SQL, EXPECTED_UPDATE_ASSERTION_SQL);
        assert_eq!(DELETE_ASSERTION_SQL, EXPECTED_DELETE_ASSERTION_SQL);
        assert!(GET_ASSERTION_SQL.contains("WHERE assertions.id = $1::text::uuid"));
    }

    #[tokio::test]
    async fn create_uses_uuidv7_canonical_endpoints_all_relations_and_complete_sources() {
        let high = concept_id(2);
        let low = concept_id(1);
        let source_inputs = vec![
            session_input("session-z"),
            memory_input("memory-z", 2),
            memory_input("memory-a", 1),
        ];
        let expected_evidence = vec![
            source_reference(memory_input("memory-a", 1)),
            source_reference(memory_input("memory-z", 2)),
            source_reference(session_input("session-z")),
        ];

        for (index, relation) in RelationType::ALL.iter().copied().enumerate() {
            let candidate = assertion_id(100 + u128::try_from(index).expect("index fits u128"));
            let input = create_input(candidate, high, relation, low, source_inputs.clone());
            let expected_record = assertion(
                candidate,
                1,
                input.identity.subject_concept_id,
                relation,
                input.identity.object_concept_id,
                expected_evidence.clone(),
            );
            let row = json!({
                "outcome": "created",
                "record": assertion_json(
                    candidate,
                    1,
                    input.identity.subject_concept_id,
                    relation,
                    input.identity.object_concept_id,
                    &expected_evidence,
                ),
            });
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let result = database()
                .create_assertion_with(&input, {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected =
                        execute_request(EXPECTED_CREATE_ASSERTION_SQL, create_params(&input));
                    let response = execute_response(row);
                    move |request| respond(request, expected, response, invocation_count)
                })
                .await;

            assert_eq!(
                result,
                Ok(expected_record),
                "relation {}",
                relation.as_str()
            );
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
            if relation.is_symmetric() {
                assert_eq!(input.identity.subject_concept_id, low);
                assert_eq!(input.identity.object_concept_id, high);
            } else {
                assert_eq!(input.identity.subject_concept_id, high);
                assert_eq!(input.identity.object_concept_id, low);
            }
        }
    }

    #[tokio::test]
    async fn semantic_create_replay_returns_database_identity_and_union_evidence() {
        let candidate = assertion_id(120);
        let canonical_id = assertion_id(121);
        let subject = concept_id(10);
        let object = concept_id(11);
        let requested_evidence = vec![
            session_input("requested-session"),
            memory_input("existing-memory", 3),
        ];
        let input = create_input(
            candidate,
            subject,
            RelationType::IsA,
            object,
            requested_evidence,
        );
        let complete_evidence = vec![
            source_reference(memory_input("existing-memory", 3)),
            source_reference(memory_input("retained-memory", 8)),
            source_reference(session_input("requested-session")),
        ];
        let expected_record = assertion(
            canonical_id,
            9,
            subject,
            RelationType::IsA,
            object,
            complete_evidence.clone(),
        );
        let response = execute_response(json!({
            "outcome": "existing",
            "record": assertion_json(
                canonical_id,
                9,
                subject,
                RelationType::IsA,
                object,
                &complete_evidence,
            ),
        }));
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let result = database()
            .create_assertion_with(&input, {
                let invocation_count = Arc::clone(&invocation_count);
                let expected =
                    execute_request(EXPECTED_CREATE_ASSERTION_SQL, create_params(&input));
                move |request| respond(request, expected, response, invocation_count)
            })
            .await;

        assert_eq!(result, Ok(expected_record));
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        assert_ne!(canonical_id, candidate);
    }

    #[tokio::test]
    async fn reversed_symmetric_replays_return_the_existing_canonical_assertion() {
        let low = concept_id(12);
        let high = concept_id(13);
        let evidence = vec![source_reference(session_input("reversal-evidence"))];

        for (offset, relation) in [RelationType::RelatedTo, RelationType::Contradicts]
            .into_iter()
            .enumerate()
        {
            let candidate = assertion_id(122 + u128::try_from(offset).expect("index fits u128"));
            let existing_id = assertion_id(124 + u128::try_from(offset).expect("index fits u128"));
            let input = create_input(
                candidate,
                high,
                relation,
                low,
                vec![session_input("reversal-evidence")],
            );
            let expected_record = assertion(existing_id, 4, low, relation, high, evidence.clone());
            let response = execute_response(json!({
                "outcome": "existing",
                "record": assertion_json(existing_id, 4, low, relation, high, &evidence),
            }));
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let result = database()
                .create_assertion_with(&input, {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected =
                        execute_request(EXPECTED_CREATE_ASSERTION_SQL, create_params(&input));
                    move |request| respond(request, expected, response, invocation_count)
                })
                .await;

            assert_eq!(result, Ok(expected_record));
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
            assert_eq!(input.identity.subject_concept_id, low);
            assert_eq!(input.identity.object_concept_id, high);
        }
    }

    #[tokio::test]
    async fn create_decodes_conflict_missing_endpoint_missing_source_and_evidence_limit() {
        let candidate = assertion_id(130);
        let subject = concept_id(20);
        let object = concept_id(21);
        let input = create_input(
            candidate,
            subject,
            RelationType::Uses,
            object,
            vec![session_input(SOURCE_KEY_SENTINEL)],
        );
        let cases = [
            (
                json!({
                    "outcome": "conflict",
                    "field": "semantic_assertion",
                    "reason": "duplicate_semantic_assertion",
                    "identity": {"kind": "assertion", "id": candidate.as_uuid().to_string()},
                    "current_revision": 6,
                }),
                expected_conflict(
                    OperationCode::CreateAssertion,
                    ConflictField::SemanticAssertion,
                    ConflictReason::DuplicateSemanticAssertion,
                    candidate,
                    6,
                ),
            ),
            (
                json!({
                    "outcome": "missing",
                    "record_kind": "concept",
                    "identity": {"kind": "concept", "id": subject.as_uuid().to_string()},
                }),
                expected_not_found(
                    OperationCode::CreateAssertion,
                    RecordKind::Concept,
                    Some(RecordIdentity::Concept(subject)),
                ),
            ),
            (
                json!({"outcome": "missing", "record_kind": "source_reference"}),
                expected_not_found(
                    OperationCode::CreateAssertion,
                    RecordKind::SourceReference,
                    None,
                ),
            ),
            (
                json!({"outcome": "evidence_limit"}),
                GraphError::LimitExceeded {
                    operation: OperationCode::CreateAssertion,
                    resource: LimitResource::AssertionEvidenceCount,
                    reason: LimitReason::Exceeded,
                },
            ),
        ];

        for (row, expected_error) in cases {
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let result = database()
                .create_assertion_with(&input, {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected =
                        execute_request(EXPECTED_CREATE_ASSERTION_SQL, create_params(&input));
                    let response = execute_response(row);
                    move |request| respond(request, expected, response, invocation_count)
                })
                .await;

            let error = result.expect_err("the mutation outcome should be a typed failure");
            assert_eq!(error, expected_error);
            assert_opaque(&error);
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn update_sends_expected_revision_and_returns_retained_complete_evidence() {
        let id = assertion_id(140);
        let high = concept_id(32);
        let low = concept_id(31);
        let input = update_input(id, 12, high, RelationType::Contradicts, low);
        let retained_evidence = vec![
            source_reference(memory_input("memory-a", 1)),
            source_reference(memory_input("memory-z", 5)),
            source_reference(session_input("session-retained")),
        ];
        let expected_record = assertion(
            id,
            13,
            low,
            RelationType::Contradicts,
            high,
            retained_evidence.clone(),
        );
        let response = execute_response(json!({
            "outcome": "updated",
            "record": assertion_json(
                id,
                13,
                low,
                RelationType::Contradicts,
                high,
                &retained_evidence,
            ),
        }));
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let result = database()
            .update_assertion_with(&input, {
                let invocation_count = Arc::clone(&invocation_count);
                let expected =
                    execute_request(EXPECTED_UPDATE_ASSERTION_SQL, update_params(&input));
                move |request| respond(request, expected, response, invocation_count)
            })
            .await;

        assert_eq!(result, Ok(expected_record));
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        assert_eq!(input.identity.subject_concept_id, low);
        assert_eq!(input.identity.object_concept_id, high);
    }

    #[tokio::test]
    async fn update_decodes_semantic_snapshot_stale_and_missing_outcomes() {
        let id = assertion_id(150);
        let subject = concept_id(40);
        let object = concept_id(41);
        let input = update_input(id, 20, subject, RelationType::DependsOn, object);
        let duplicate_id = assertion_id(151);
        let cases = [
            (
                json!({
                    "outcome": "conflict",
                    "field": "semantic_assertion",
                    "reason": "duplicate_semantic_assertion",
                    "identity": {"kind": "assertion", "id": duplicate_id.as_uuid().to_string()},
                    "current_revision": 11,
                }),
                expected_conflict(
                    OperationCode::UpdateAssertion,
                    ConflictField::SemanticAssertion,
                    ConflictReason::DuplicateSemanticAssertion,
                    duplicate_id,
                    11,
                ),
            ),
            (
                json!({
                    "outcome": "conflict",
                    "field": "snapshot",
                    "reason": "snapshot_drift",
                    "identity": {"kind": "assertion", "id": id.as_uuid().to_string()},
                    "current_revision": 21,
                }),
                expected_conflict(
                    OperationCode::UpdateAssertion,
                    ConflictField::Snapshot,
                    ConflictReason::SnapshotDrift,
                    id,
                    21,
                ),
            ),
            (
                json!({
                    "outcome": "stale",
                    "identity": {"kind": "assertion", "id": id.as_uuid().to_string()},
                    "current_revision": 22,
                }),
                expected_conflict(
                    OperationCode::UpdateAssertion,
                    ConflictField::Revision,
                    ConflictReason::StaleRevision,
                    id,
                    22,
                ),
            ),
            (
                json!({
                    "outcome": "missing",
                    "record_kind": "concept",
                    "identity": {"kind": "concept", "id": object.as_uuid().to_string()},
                }),
                expected_not_found(
                    OperationCode::UpdateAssertion,
                    RecordKind::Concept,
                    Some(RecordIdentity::Concept(object)),
                ),
            ),
            (
                json!({
                    "outcome": "missing",
                    "record_kind": "assertion",
                    "identity": {"kind": "assertion", "id": id.as_uuid().to_string()},
                }),
                expected_not_found(
                    OperationCode::UpdateAssertion,
                    RecordKind::Assertion,
                    Some(RecordIdentity::Assertion(id)),
                ),
            ),
        ];

        for (row, expected_error) in cases {
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let result = database()
                .update_assertion_with(&input, {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected =
                        execute_request(EXPECTED_UPDATE_ASSERTION_SQL, update_params(&input));
                    let response = execute_response(row);
                    move |request| respond(request, expected, response, invocation_count)
                })
                .await;

            let error = result.expect_err("the mutation outcome should be a typed failure");
            assert_eq!(error, expected_error);
            assert_opaque(&error);
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn delete_returns_success_only_for_confirmation_and_decodes_failures() {
        let id = assertion_id(160);
        let input = delete_input(id, 30);
        let outcomes = [
            (json!({"outcome": "deleted"}), Ok(())),
            (
                json!({
                    "outcome": "stale",
                    "identity": {"kind": "assertion", "id": id.as_uuid().to_string()},
                    "current_revision": 31,
                }),
                Err(expected_conflict(
                    OperationCode::DeleteAssertion,
                    ConflictField::Revision,
                    ConflictReason::StaleRevision,
                    id,
                    31,
                )),
            ),
            (
                json!({
                    "outcome": "conflict",
                    "field": "snapshot",
                    "reason": "snapshot_drift",
                    "identity": {"kind": "assertion", "id": id.as_uuid().to_string()},
                    "current_revision": 32,
                }),
                Err(expected_conflict(
                    OperationCode::DeleteAssertion,
                    ConflictField::Snapshot,
                    ConflictReason::SnapshotDrift,
                    id,
                    32,
                )),
            ),
            (
                json!({
                    "outcome": "missing",
                    "record_kind": "assertion",
                    "identity": {"kind": "assertion", "id": id.as_uuid().to_string()},
                }),
                Err(expected_not_found(
                    OperationCode::DeleteAssertion,
                    RecordKind::Assertion,
                    Some(RecordIdentity::Assertion(id)),
                )),
            ),
        ];

        for (row, expected_result) in outcomes {
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let result = database()
                .delete_assertion_with(&input, {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected =
                        execute_request(EXPECTED_DELETE_ASSERTION_SQL, delete_params(&input));
                    let response = execute_response(row);
                    move |request| respond(request, expected, response, invocation_count)
                })
                .await;

            assert_eq!(result, expected_result);
            if let Err(error) = &result {
                assert_opaque(error);
            }
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn direct_read_returns_complete_ordered_assertion_or_confirmed_absence() {
        let id = assertion_id(170);
        assert!(GET_ASSERTION_SQL.contains("WHERE assertions.id = $1::text::uuid"));
        let subject = concept_id(52);
        let object = concept_id(51);
        let evidence = vec![
            source_reference(memory_input("memory-a", 1)),
            source_reference(memory_input("memory-z", 9)),
            source_reference(session_input("session-direct")),
        ];
        let expected_record = assertion(
            id,
            7,
            subject,
            RelationType::PartOf,
            object,
            evidence.clone(),
        );
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let direct = database()
            .get_assertion_with(id, {
                let invocation_count = Arc::clone(&invocation_count);
                let expected = query_request(GET_ASSERTION_SQL, json!([id.as_uuid().to_string()]));
                let response = query_response(complete_payload(assertion_json(
                    id,
                    7,
                    subject,
                    RelationType::PartOf,
                    object,
                    &evidence,
                )));
                move |request| respond(request, expected, response, invocation_count)
            })
            .await;

        assert_eq!(direct, Ok(Some(expected_record)));
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        assert!(GET_ASSERTION_SQL.contains("source_kind COLLATE \"C\" ASC"));
        assert!(GET_ASSERTION_SQL.contains("external_id COLLATE \"C\" ASC"));
        assert!(GET_ASSERTION_SQL.contains("external_version ASC NULLS FIRST"));
        assert!(!GET_ASSERTION_SQL.contains("source_content"));

        let invocation_count = Arc::new(AtomicUsize::new(0));
        let absent = database()
            .get_assertion_with(id, {
                let invocation_count = Arc::clone(&invocation_count);
                let expected = query_request(GET_ASSERTION_SQL, json!([id.as_uuid().to_string()]));
                let response = query_response(complete_payload(Value::Null));
                move |request| respond(request, expected, response, invocation_count)
            })
            .await;
        assert_eq!(absent, Ok(None));
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn direct_read_maps_result_bounds_markers_and_malformed_records_to_stable_errors() {
        let id = assertion_id(180);
        let subject = concept_id(60);
        let object = concept_id(61);
        let valid_evidence = vec![source_reference(memory_input("memory-read", 2))];
        let valid_record =
            assertion_json(id, 2, subject, RelationType::Uses, object, &valid_evidence);
        let reversed_symmetric = assertion_json(
            id,
            2,
            object,
            RelationType::RelatedTo,
            subject,
            &valid_evidence,
        );
        let unordered_evidence = assertion_json(
            id,
            2,
            subject,
            RelationType::Uses,
            object,
            &[
                source_reference(session_input("session-first")),
                source_reference(memory_input("memory-second", 2)),
            ],
        );
        let mut record_with_source_content = valid_record.clone();
        record_with_source_content["source_content"] = json!(SOURCE_KEY_SENTINEL);
        let mut unsupported_relation = valid_record.clone();
        unsupported_relation["relation_type"] = json!("unsupported_relation");
        let other_id = assertion_id(181);
        let other_identity = assertion_json(
            other_id,
            2,
            subject,
            RelationType::Uses,
            object,
            &valid_evidence,
        );
        let cases = [
            (
                json!({"outcome": "result_too_large"}),
                GraphError::ResultBoundExceeded {
                    operation: OperationCode::GetAssertion,
                },
            ),
            (
                json!({"outcome": "unknown_assertion_read_marker"}),
                expected_invalid_response(
                    OperationCode::GetAssertion,
                    BackendFailureReason::UnknownOutcome,
                ),
            ),
            (
                json!({"outcome": "complete"}),
                expected_invalid_response(
                    OperationCode::GetAssertion,
                    BackendFailureReason::MalformedResponse,
                ),
            ),
            (
                complete_payload(reversed_symmetric),
                expected_invalid_response(
                    OperationCode::GetAssertion,
                    BackendFailureReason::UnsupportedValue,
                ),
            ),
            (
                complete_payload(unordered_evidence),
                expected_invalid_response(
                    OperationCode::GetAssertion,
                    BackendFailureReason::UnsupportedValue,
                ),
            ),
            (
                complete_payload(other_identity),
                expected_invalid_response(
                    OperationCode::GetAssertion,
                    BackendFailureReason::UnsupportedValue,
                ),
            ),
            (
                complete_payload(record_with_source_content),
                expected_invalid_response(
                    OperationCode::GetAssertion,
                    BackendFailureReason::UnsupportedValue,
                ),
            ),
            (
                complete_payload(unsupported_relation),
                expected_invalid_response(
                    OperationCode::GetAssertion,
                    BackendFailureReason::UnsupportedValue,
                ),
            ),
        ];

        for (payload, expected_error) in cases {
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let result = database()
                .get_assertion_with(id, {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected =
                        query_request(GET_ASSERTION_SQL, json!([id.as_uuid().to_string()]));
                    let response = query_response(payload);
                    move |request| respond(request, expected, response, invocation_count)
                })
                .await;

            let error =
                result.expect_err("invalid or bounded reads must not return a partial record");
            assert_eq!(error, expected_error);
            assert_opaque(&error);
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn assertion_invocation_failures_are_operation_only_and_not_retried() {
        let input = create_input(
            assertion_id(190),
            concept_id(70),
            RelationType::Causes,
            concept_id(71),
            vec![session_input(SOURCE_KEY_SENTINEL)],
        );
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let error = database()
            .create_assertion_with(&input, {
                let invocation_count = Arc::clone(&invocation_count);
                move |_| {
                    invocation_count.fetch_add(1, Ordering::SeqCst);
                    async { Err(IiiError::Handler(SOURCE_KEY_SENTINEL.to_owned())) }
                }
            })
            .await
            .expect_err("a backend failure must not return an assertion");

        assert_eq!(
            error,
            expected_database_failure(OperationCode::CreateAssertion)
        );
        assert_opaque(&error);
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
    }
}

#[cfg(test)]
mod iii_assertion_evidence_operation_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use iii_sdk::{Error as IiiError, IIIClient, protocol::TriggerRequest};
    use serde_json::{Value, json};
    use uuid::Uuid;

    use crate::contracts::{
        AssertionEvidence, AssertionEvidenceInput, AssertionId, BackendFailureReason,
        ConflictField, ConflictReason, DatabaseTimeouts, DeleteAssertion, GraphError, LimitReason,
        LimitResource, NotFoundReason, OperationCode, RecordIdentity, RecordKind, Revision,
        SourceReference, SourceReferenceInput, ValidatedAssertionEvidence,
        ValidatedDeleteAssertion,
    };

    use super::{
        ADD_ASSERTION_EVIDENCE_SQL, DATABASE_EXECUTE_FUNCTION_ID, DELETE_ASSERTION_SQL,
        DatabaseTarget, IiiKnowledgeGraphDatabase, REMOVE_ASSERTION_EVIDENCE_SQL,
    };

    const DATABASE_TARGET: &str = "assertion-evidence-operation-database-target";
    const SOURCE_KEY_SENTINEL: &str = "PRIVATE_ASSERTION_EVIDENCE_SOURCE_SENTINEL";
    const OTHER_SOURCE_SENTINEL: &str = "OTHER_PRIVATE_ASSERTION_EVIDENCE_SOURCE_SENTINEL";
    const UUID_V7_BASE: u128 = 0x0000_0000_0000_7000_8000_0000_0000_0000;
    const EXPECTED_ADD_ASSERTION_EVIDENCE_SQL: &str = "SELECT knowledge_graph.graph_add_assertion_evidence($1::text::uuid, $2::text, $3::text, $4::text::bigint)";
    const EXPECTED_REMOVE_ASSERTION_EVIDENCE_SQL: &str = "SELECT knowledge_graph.graph_remove_assertion_evidence($1::text::uuid, $2::text, $3::text, $4::text::bigint)";
    const EXPECTED_DELETE_ASSERTION_SQL: &str =
        "SELECT knowledge_graph.graph_delete_assertion($1::text::uuid, $2::text::bigint)";

    #[derive(Clone)]
    struct ExpectedRequest {
        function_id: &'static str,
        payload: Value,
        timeout_ms: Option<u64>,
    }

    fn database() -> IiiKnowledgeGraphDatabase {
        IiiKnowledgeGraphDatabase::new(
            IIIClient::new("ws://127.0.0.1:1"),
            DatabaseTarget::try_from(DATABASE_TARGET.to_owned())
                .expect("test database target should be valid"),
            DatabaseTimeouts::try_new(Duration::from_secs(4), Duration::from_secs(13))
                .expect("test timeouts should be valid"),
        )
        .expect("adapter configuration should be valid")
    }

    fn assertion_id(value: u128) -> AssertionId {
        AssertionId::try_from(Uuid::from_u128(UUID_V7_BASE + value))
            .expect("fixture assertion ID should be UUIDv7")
    }

    fn revision(value: i64) -> Revision {
        Revision::try_from(value).expect("fixture revision should be positive")
    }

    fn memory_input(memory_id: &str, version: i64) -> SourceReferenceInput {
        SourceReferenceInput::MemoryVersion {
            memory_id: memory_id.to_owned(),
            version,
        }
    }

    fn session_input(session_record_id: &str) -> SourceReferenceInput {
        SourceReferenceInput::SessionRecord {
            session_record_id: session_record_id.to_owned(),
        }
    }

    fn source_reference(input: SourceReferenceInput) -> SourceReference {
        SourceReference::try_from(input).expect("fixture source should be valid")
    }

    fn evidence_input(
        assertion_id: AssertionId,
        source: SourceReferenceInput,
    ) -> ValidatedAssertionEvidence {
        ValidatedAssertionEvidence::try_from(AssertionEvidenceInput {
            assertion_id,
            source,
        })
        .expect("fixture evidence should be valid")
    }

    fn delete_input(assertion_id: AssertionId, expected_revision: i64) -> ValidatedDeleteAssertion {
        ValidatedDeleteAssertion::from(DeleteAssertion {
            assertion_id,
            expected_revision: revision(expected_revision),
        })
    }

    fn source_json(source: &SourceReference) -> Value {
        match source {
            SourceReference::MemoryVersion { memory_id, version } => json!({
                "kind": "memory_version",
                "memory_id": memory_id.as_str(),
                "version": version.get(),
            }),
            SourceReference::SessionRecord { session_record_id } => json!({
                "kind": "session_record",
                "session_record_id": session_record_id.as_str(),
            }),
        }
    }

    fn evidence_json(assertion_id: AssertionId, source: &SourceReference) -> Value {
        json!({
            "assertion_id": assertion_id.as_uuid().to_string(),
            "source": source_json(source),
        })
    }

    fn evidence_parameters(assertion_id: AssertionId, source: &SourceReference) -> Value {
        match source {
            SourceReference::MemoryVersion { memory_id, version } => json!([
                assertion_id.as_uuid().to_string(),
                "memory_version",
                memory_id.as_str(),
                version.get().to_string(),
            ]),
            SourceReference::SessionRecord { session_record_id } => json!([
                assertion_id.as_uuid().to_string(),
                "session_record",
                session_record_id.as_str(),
                null,
            ]),
        }
    }

    fn execute_response(row: Value) -> Value {
        json!({
            "affected_rows": 1,
            "last_insert_id": null,
            "returned_rows": [row],
        })
    }

    fn execute_request(sql: &'static str, params: Value) -> ExpectedRequest {
        ExpectedRequest {
            function_id: DATABASE_EXECUTE_FUNCTION_ID,
            payload: json!({
                "db": DATABASE_TARGET,
                "sql": sql,
                "params": params,
            }),
            timeout_ms: Some(13_000),
        }
    }

    async fn respond(
        request: TriggerRequest,
        expected: ExpectedRequest,
        response: Value,
        invocation_count: Arc<AtomicUsize>,
    ) -> Result<Value, IiiError> {
        invocation_count.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.function_id, expected.function_id);
        assert_eq!(request.payload, expected.payload);
        assert!(request.action.is_none());
        assert_eq!(request.timeout_ms, expected.timeout_ms);
        Ok(response)
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

    fn expected_invalid(operation: OperationCode) -> GraphError {
        GraphError::InvalidResponse {
            operation,
            reason: BackendFailureReason::UnsupportedValue,
        }
    }

    fn assert_source_is_opaque(error: &GraphError) {
        let display = error.to_string();
        let debug = format!("{error:?}");
        for sentinel in [SOURCE_KEY_SENTINEL, OTHER_SOURCE_SENTINEL] {
            assert!(
                !display.contains(sentinel),
                "Display leaked a source identity: {display}"
            );
            assert!(
                !debug.contains(sentinel),
                "Debug leaked a source identity: {debug}"
            );
        }
    }

    #[test]
    fn evidence_sql_uses_approved_routines_and_text_first_uuid_bigint_casts() {
        assert_eq!(
            ADD_ASSERTION_EVIDENCE_SQL,
            EXPECTED_ADD_ASSERTION_EVIDENCE_SQL
        );
        assert_eq!(
            REMOVE_ASSERTION_EVIDENCE_SQL,
            EXPECTED_REMOVE_ASSERTION_EVIDENCE_SQL
        );
        assert_eq!(DELETE_ASSERTION_SQL, EXPECTED_DELETE_ASSERTION_SQL);
    }

    #[tokio::test]
    async fn add_decodes_created_and_existing_complete_typed_evidence_records() {
        let cases = [
            (201, memory_input(SOURCE_KEY_SENTINEL, 7), "created"),
            (202, session_input(SOURCE_KEY_SENTINEL), "existing"),
            (203, memory_input(SOURCE_KEY_SENTINEL, 8), "existing"),
            (204, session_input(SOURCE_KEY_SENTINEL), "created"),
        ];

        for (id_value, source_input, outcome) in cases {
            let id = assertion_id(id_value);
            let source = source_reference(source_input.clone());
            let input = evidence_input(id, source_input);
            let record = AssertionEvidence {
                assertion_id: id,
                source: source.clone(),
            };
            let response = execute_response(json!({
                "outcome": outcome,
                "record": evidence_json(id, &source),
            }));
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let result = database()
                .add_evidence_with(&input, {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected = execute_request(
                        EXPECTED_ADD_ASSERTION_EVIDENCE_SQL,
                        evidence_parameters(id, &source),
                    );
                    move |request| respond(request, expected, response, invocation_count)
                })
                .await;

            assert_eq!(result, Ok(record));
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
            if matches!(source, SourceReference::SessionRecord { .. }) {
                assert!(
                    evidence_json(id, &source)["source"]
                        .get("version")
                        .is_none(),
                    "session-record evidence must not acquire a version"
                );
            }
        }
    }

    #[tokio::test]
    async fn add_decodes_cap_missing_endpoints_and_snapshot_drift_without_source_values() {
        let id = assertion_id(210);
        let source = source_reference(session_input(SOURCE_KEY_SENTINEL));
        let input = evidence_input(id, session_input(SOURCE_KEY_SENTINEL));
        let snapshot_revision = revision(8);
        let cases = [
            (
                json!({"outcome": "evidence_limit"}),
                GraphError::LimitExceeded {
                    operation: OperationCode::AddEvidence,
                    resource: LimitResource::AssertionEvidenceCount,
                    reason: LimitReason::Exceeded,
                },
            ),
            (
                json!({"outcome": "missing", "record_kind": "assertion"}),
                expected_not_found(
                    OperationCode::AddEvidence,
                    RecordKind::Assertion,
                    Some(RecordIdentity::Assertion(id)),
                ),
            ),
            (
                json!({"outcome": "missing", "record_kind": "source_reference"}),
                expected_not_found(
                    OperationCode::AddEvidence,
                    RecordKind::SourceReference,
                    None,
                ),
            ),
            (
                json!({
                    "outcome": "conflict",
                    "field": "snapshot",
                    "reason": "snapshot_drift",
                    "identity": {"kind": "assertion", "id": id.as_uuid().to_string()},
                    "current_revision": snapshot_revision.get(),
                }),
                GraphError::Conflict {
                    operation: OperationCode::AddEvidence,
                    field: ConflictField::Snapshot,
                    reason: ConflictReason::SnapshotDrift,
                    identity: RecordIdentity::Assertion(id),
                    current_revision: Some(snapshot_revision),
                },
            ),
        ];

        for (row, expected_error) in cases {
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let result = database()
                .add_evidence_with(&input, {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected = execute_request(
                        EXPECTED_ADD_ASSERTION_EVIDENCE_SQL,
                        evidence_parameters(id, &source),
                    );
                    let response = execute_response(row);
                    move |request| respond(request, expected, response, invocation_count)
                })
                .await;

            let error = result.expect_err("the evidence mutation should decode a typed failure");
            assert_eq!(error, expected_error);
            assert_source_is_opaque(&error);
            if matches!(error, GraphError::LimitExceeded { .. }) {
                assert!(!format!("{error:?}").contains(&id.as_uuid().to_string()));
            }
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn add_rejects_noncanonical_records_and_source_payloads_without_leaks() {
        let id = assertion_id(220);
        let source = source_reference(session_input(SOURCE_KEY_SENTINEL));
        let other_source = source_reference(session_input(OTHER_SOURCE_SENTINEL));
        let input = evidence_input(id, session_input(SOURCE_KEY_SENTINEL));
        let wrong_assertion = evidence_json(assertion_id(221), &source);
        let mut with_excerpt = evidence_json(id, &source);
        with_excerpt["excerpt"] = json!(OTHER_SOURCE_SENTINEL);
        let mut with_payload = evidence_json(id, &source);
        with_payload["source"]["payload"] = json!(OTHER_SOURCE_SENTINEL);
        let mut with_confidence = evidence_json(id, &source);
        with_confidence["confidence"] = json!(0.75);
        let mut with_custom_properties = evidence_json(id, &source);
        with_custom_properties["custom_properties"] = json!({
            "private": OTHER_SOURCE_SENTINEL,
        });
        let records = [
            evidence_json(id, &other_source),
            wrong_assertion,
            with_excerpt,
            with_payload,
            with_confidence,
            with_custom_properties,
        ];

        for record in records {
            let response = execute_response(json!({
                "outcome": "created",
                "record": record,
            }));
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let result = database()
                .add_evidence_with(&input, {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected = execute_request(
                        EXPECTED_ADD_ASSERTION_EVIDENCE_SQL,
                        evidence_parameters(id, &source),
                    );
                    move |request| respond(request, expected, response, invocation_count)
                })
                .await;

            let error = result.expect_err("noncanonical evidence must not be returned");
            assert_eq!(error, expected_invalid(OperationCode::AddEvidence));
            assert_source_is_opaque(&error);
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn remove_decodes_confirmed_selective_deletions_for_both_source_kinds() {
        let cases = [
            (231, memory_input(SOURCE_KEY_SENTINEL, 4)),
            (232, session_input(SOURCE_KEY_SENTINEL)),
        ];

        for (id_value, source_input) in cases {
            let id = assertion_id(id_value);
            let source = source_reference(source_input.clone());
            let input = evidence_input(id, source_input);
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let result = database()
                .remove_evidence_with(&input, {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected = execute_request(
                        EXPECTED_REMOVE_ASSERTION_EVIDENCE_SQL,
                        evidence_parameters(id, &source),
                    );
                    let response = execute_response(json!({"outcome": "deleted"}));
                    move |request| respond(request, expected, response, invocation_count)
                })
                .await;

            assert_eq!(result, Ok(()));
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn remove_decodes_missing_assertion_source_evidence_and_would_orphan() {
        let id = assertion_id(240);
        let source = source_reference(session_input(SOURCE_KEY_SENTINEL));
        let input = evidence_input(id, session_input(SOURCE_KEY_SENTINEL));
        let orphan_revision = revision(12);
        let cases = [
            (
                json!({"outcome": "missing", "record_kind": "assertion"}),
                expected_not_found(
                    OperationCode::RemoveEvidence,
                    RecordKind::Assertion,
                    Some(RecordIdentity::Assertion(id)),
                ),
            ),
            (
                json!({"outcome": "missing", "record_kind": "source_reference"}),
                expected_not_found(
                    OperationCode::RemoveEvidence,
                    RecordKind::SourceReference,
                    None,
                ),
            ),
            (
                json!({"outcome": "missing", "record_kind": "assertion_evidence"}),
                expected_not_found(
                    OperationCode::RemoveEvidence,
                    RecordKind::AssertionEvidence,
                    None,
                ),
            ),
            (
                json!({
                    "outcome": "would_orphan_evidence",
                    "assertion_id": id.as_uuid().to_string(),
                    "current_revision": orphan_revision.get(),
                }),
                GraphError::WouldOrphanAssertion {
                    operation: OperationCode::RemoveEvidence,
                    assertion_id: id,
                    revision: orphan_revision,
                },
            ),
        ];

        for (row, expected_error) in cases {
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let result = database()
                .remove_evidence_with(&input, {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected = execute_request(
                        EXPECTED_REMOVE_ASSERTION_EVIDENCE_SQL,
                        evidence_parameters(id, &source),
                    );
                    let response = execute_response(row);
                    move |request| respond(request, expected, response, invocation_count)
                })
                .await;

            let error = result.expect_err("the evidence mutation should decode a typed failure");
            assert_eq!(error, expected_error);
            assert_source_is_opaque(&error);
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn evidence_invocation_failure_is_operation_only_and_not_retried() {
        let input = evidence_input(assertion_id(250), session_input(SOURCE_KEY_SENTINEL));
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let error = database()
            .add_evidence_with(&input, {
                let invocation_count = Arc::clone(&invocation_count);
                move |_| {
                    invocation_count.fetch_add(1, Ordering::SeqCst);
                    async { Err(IiiError::Handler(SOURCE_KEY_SENTINEL.to_owned())) }
                }
            })
            .await
            .expect_err("a backend failure must not return evidence");

        assert_eq!(
            error,
            GraphError::DatabaseFailure {
                operation: OperationCode::AddEvidence,
                reason: BackendFailureReason::DatabaseFailure,
            }
        );
        assert_source_is_opaque(&error);
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn assertion_delete_confirmation_remains_the_evidence_cascade_result() {
        let id = assertion_id(260);
        let input = delete_input(id, 14);
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let result = database()
            .delete_assertion_with(&input, {
                let invocation_count = Arc::clone(&invocation_count);
                let expected = execute_request(
                    EXPECTED_DELETE_ASSERTION_SQL,
                    json!([id.as_uuid().to_string(), "14"]),
                );
                let response = execute_response(json!({"outcome": "deleted"}));
                move |request| respond(request, expected, response, invocation_count)
            })
            .await;

        assert_eq!(result, Ok(()));
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
    }
}

#[cfg(test)]
mod iii_neighbor_related_source_operation_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use iii_sdk::{Error as IiiError, IIIClient, protocol::TriggerRequest};
    use serde_json::{Value, json};
    use uuid::Uuid;

    use crate::contracts::{
        Alias, Assertion, AssertionId, BackendFailureReason, Concept, ConceptId, DirectionMode,
        EdgeOrientation, GraphError, NeighborQuery, NeighborResult, OperationCode,
        RelatedSourceQuery, RelatedSourceResult, RelatedSourceRole, RelationType, Revision,
        SourceReference, SourceReferenceInput, ValidatedNeighborQuery, ValidatedRelatedSourceQuery,
    };

    use super::{
        DATABASE_QUERY_FUNCTION_ID, DatabaseTarget, GET_NEIGHBORS_SQL, GET_RELATED_SOURCES_SQL,
        IiiKnowledgeGraphDatabase,
    };

    const DATABASE_TARGET: &str = "neighbor-related-source-operation-database-target";
    const SOURCE_KEY_SENTINEL: &str = "PRIVATE_NEIGHBOR_SOURCE_SENTINEL";
    const UUID_V7_BASE: u128 = 0x0000_0000_0000_7000_8000_0000_0000_0000;

    struct ExpectedRequest {
        payload: Value,
        timeout_ms: Option<u64>,
    }

    fn database() -> IiiKnowledgeGraphDatabase {
        IiiKnowledgeGraphDatabase::new(
            IIIClient::new("ws://127.0.0.1:1"),
            DatabaseTarget::try_from(DATABASE_TARGET.to_owned())
                .expect("test database target should be valid"),
            crate::contracts::DatabaseTimeouts::try_new(
                Duration::from_secs(4),
                Duration::from_secs(13),
            )
            .expect("test timeouts should be valid"),
        )
        .expect("adapter configuration should be valid")
    }

    fn concept_id(value: u128) -> ConceptId {
        ConceptId::try_from(Uuid::from_u128(UUID_V7_BASE + value))
            .expect("fixture concept ID should be UUIDv7")
    }

    fn assertion_id(value: u128) -> AssertionId {
        AssertionId::try_from(Uuid::from_u128(UUID_V7_BASE + value))
            .expect("fixture assertion ID should be UUIDv7")
    }

    fn revision(value: i64) -> Revision {
        Revision::try_from(value).expect("fixture revision should be positive")
    }

    fn source_reference(source: SourceReferenceInput) -> SourceReference {
        SourceReference::try_from(source).expect("fixture source should be valid")
    }

    fn memory_source(memory_id: &str, version: i64) -> SourceReference {
        source_reference(SourceReferenceInput::MemoryVersion {
            memory_id: memory_id.to_owned(),
            version,
        })
    }

    fn session_source(session_record_id: &str) -> SourceReference {
        source_reference(SourceReferenceInput::SessionRecord {
            session_record_id: session_record_id.to_owned(),
        })
    }

    fn concept_record(id: ConceptId, alias: &str) -> Concept {
        Concept {
            id,
            revision: revision(3),
            aliases: vec![Alias {
                display_text: alias.to_owned(),
                preferred: true,
            }],
        }
    }

    fn concept_json(id: ConceptId, alias: &str) -> Value {
        json!({
            "id": id.as_uuid().to_string(),
            "revision": 3,
            "aliases": [{"display_text": alias, "preferred": true}],
        })
    }

    fn assertion_record(
        id: AssertionId,
        subject: ConceptId,
        relation: RelationType,
        object: ConceptId,
        evidence: Vec<SourceReference>,
    ) -> Assertion {
        Assertion {
            id,
            revision: revision(7),
            subject_concept_id: subject,
            relation_type: relation,
            object_concept_id: object,
            evidence,
        }
    }

    fn source_json(source: &SourceReference) -> Value {
        match source {
            SourceReference::MemoryVersion { memory_id, version } => json!({
                "kind": "memory_version",
                "memory_id": memory_id.as_str(),
                "version": version.get(),
            }),
            SourceReference::SessionRecord { session_record_id } => json!({
                "kind": "session_record",
                "session_record_id": session_record_id.as_str(),
            }),
        }
    }

    fn assertion_json(
        id: AssertionId,
        subject: ConceptId,
        relation: RelationType,
        object: ConceptId,
        evidence: &[SourceReference],
    ) -> Value {
        json!({
            "id": id.as_uuid().to_string(),
            "revision": 7,
            "subject_concept_id": subject.as_uuid().to_string(),
            "relation_type": relation.as_str(),
            "object_concept_id": object.as_uuid().to_string(),
            "evidence": evidence.iter().map(source_json).collect::<Vec<_>>(),
        })
    }

    fn neighbor_result_record(
        neighbor: ConceptId,
        assertion: Assertion,
        orientation: EdgeOrientation,
    ) -> NeighborResult {
        NeighborResult {
            neighbor: concept_record(neighbor, &format!("concept-{}", neighbor.as_uuid())),
            assertion,
            orientation,
        }
    }

    fn neighbor_json(
        neighbor: ConceptId,
        assertion_id: AssertionId,
        subject: ConceptId,
        relation: RelationType,
        object: ConceptId,
        evidence: &[SourceReference],
        orientation: EdgeOrientation,
    ) -> Value {
        json!({
            "neighbor": concept_json(neighbor, &format!("concept-{}", neighbor.as_uuid())),
            "assertion": assertion_json(assertion_id, subject, relation, object, evidence),
            "orientation": orientation.as_str(),
        })
    }

    fn related_source_record(
        source: SourceReference,
        role: RelatedSourceRole,
    ) -> RelatedSourceResult {
        RelatedSourceResult { source, role }
    }

    fn related_source_json(source: &SourceReference, role: RelatedSourceRole) -> Value {
        json!({"source": source_json(source), "role": role.as_str()})
    }

    fn validated_neighbor_query(
        concept_id: ConceptId,
        direction: DirectionMode,
        relation_filter: Vec<RelationType>,
        limit: u16,
    ) -> ValidatedNeighborQuery {
        ValidatedNeighborQuery::try_from(NeighborQuery {
            concept_id,
            direction,
            relation_filter,
            limit,
        })
        .expect("fixture neighbor query should be valid")
    }

    fn validated_related_source_query(
        concept_id: ConceptId,
        limit: u16,
    ) -> ValidatedRelatedSourceQuery {
        ValidatedRelatedSourceQuery::try_from(RelatedSourceQuery { concept_id, limit })
            .expect("fixture related-source query should be valid")
    }

    fn neighbor_parameters(query: &ValidatedNeighborQuery) -> Value {
        json!([
            query.concept_id.as_uuid().to_string(),
            query.direction.as_str(),
            query
                .relation_filter
                .iter()
                .map(|relation| relation.as_str())
                .collect::<Vec<_>>(),
            query.limit.to_string(),
        ])
    }

    fn related_source_parameters(query: &ValidatedRelatedSourceQuery) -> Value {
        json!([
            query.concept_id.as_uuid().to_string(),
            query.limit.to_string()
        ])
    }

    fn query_request(sql: &'static str, params: Value) -> ExpectedRequest {
        ExpectedRequest {
            payload: json!({
                "db": DATABASE_TARGET,
                "sql": sql,
                "params": params,
                "timeout_ms": 4_000,
            }),
            timeout_ms: Some(13_000),
        }
    }

    fn query_response(payload: Value) -> Value {
        json!({
            "rows": [{"payload": payload}],
            "row_count": 1,
            "columns": [{"name": "payload", "type": "jsonb"}],
        })
    }

    fn complete_payload(result: Value) -> Value {
        json!({"outcome": "complete", "result": result})
    }

    async fn respond(
        request: TriggerRequest,
        expected: ExpectedRequest,
        response: Value,
        invocation_count: Arc<AtomicUsize>,
    ) -> Result<Value, IiiError> {
        invocation_count.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.function_id, DATABASE_QUERY_FUNCTION_ID);
        assert_eq!(request.payload, expected.payload);
        assert!(request.action.is_none());
        assert_eq!(request.timeout_ms, expected.timeout_ms);
        Ok(response)
    }

    fn assert_opaque(error: &GraphError) {
        for message in [error.to_string(), format!("{error:?}")] {
            assert!(
                !message.contains(SOURCE_KEY_SENTINEL),
                "error leaked source identity: {message}"
            );
        }
    }

    fn expected_database_failure(operation: OperationCode) -> GraphError {
        GraphError::DatabaseFailure {
            operation,
            reason: BackendFailureReason::DatabaseFailure,
        }
    }

    #[tokio::test]
    async fn neighbor_query_preserves_full_ordered_records_and_worker_parameters() {
        let concept = concept_id(50);
        let first_neighbor = concept_id(49);
        let second_neighbor = concept_id(51);
        let evidence = vec![
            memory_source("memory-a", 1),
            memory_source("memory-a", 2),
            session_source("session-a"),
        ];
        let query = validated_neighbor_query(
            concept,
            DirectionMode::Either,
            vec![RelationType::Uses, RelationType::IsA, RelationType::Uses],
            2,
        );
        let first_assertion = assertion_record(
            assertion_id(100),
            first_neighbor,
            RelationType::IsA,
            concept,
            evidence.clone(),
        );
        let second_assertion = assertion_record(
            assertion_id(101),
            concept,
            RelationType::Uses,
            second_neighbor,
            evidence.clone(),
        );
        let expected = vec![
            neighbor_result_record(first_neighbor, first_assertion, EdgeOrientation::Incoming),
            neighbor_result_record(second_neighbor, second_assertion, EdgeOrientation::Outgoing),
        ];
        let response = query_response(complete_payload(json!([
            neighbor_json(
                first_neighbor,
                assertion_id(100),
                first_neighbor,
                RelationType::IsA,
                concept,
                &evidence,
                EdgeOrientation::Incoming,
            ),
            neighbor_json(
                second_neighbor,
                assertion_id(101),
                concept,
                RelationType::Uses,
                second_neighbor,
                &evidence,
                EdgeOrientation::Outgoing,
            ),
        ])));
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let actual = database()
            .neighbors_with(&query, {
                let invocation_count = Arc::clone(&invocation_count);
                let expected = query_request(GET_NEIGHBORS_SQL, neighbor_parameters(&query));
                move |request| respond(request, expected, response, invocation_count)
            })
            .await
            .expect("complete neighbor results should decode");

        assert_eq!(actual, expected);
        assert_eq!(actual[0].assertion.evidence, evidence);
        assert_eq!(
            query.relation_filter,
            vec![RelationType::IsA, RelationType::Uses]
        );
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn neighbor_query_binds_all_direction_modes_for_directed_orientation() {
        let center = concept_id(60);
        let incoming_neighbor = concept_id(59);
        let outgoing_neighbor = concept_id(61);
        let evidence = vec![memory_source("memory-direction", 4)];
        let incoming_assertion = assertion_id(110);
        let outgoing_assertion = assertion_id(111);
        let incoming = neighbor_result_record(
            incoming_neighbor,
            assertion_record(
                incoming_assertion,
                incoming_neighbor,
                RelationType::Uses,
                center,
                evidence.clone(),
            ),
            EdgeOrientation::Incoming,
        );
        let outgoing = neighbor_result_record(
            outgoing_neighbor,
            assertion_record(
                outgoing_assertion,
                center,
                RelationType::Uses,
                outgoing_neighbor,
                evidence.clone(),
            ),
            EdgeOrientation::Outgoing,
        );
        let cases = [
            (DirectionMode::Outgoing, vec![outgoing.clone()]),
            (DirectionMode::Incoming, vec![incoming.clone()]),
            (
                DirectionMode::Either,
                vec![incoming.clone(), outgoing.clone()],
            ),
        ];

        for (direction, expected) in cases {
            let query = validated_neighbor_query(center, direction, Vec::new(), 2);
            let result_json = match direction {
                DirectionMode::Outgoing => json!([neighbor_json(
                    outgoing_neighbor,
                    outgoing_assertion,
                    center,
                    RelationType::Uses,
                    outgoing_neighbor,
                    &evidence,
                    EdgeOrientation::Outgoing,
                )]),
                DirectionMode::Incoming => json!([neighbor_json(
                    incoming_neighbor,
                    incoming_assertion,
                    incoming_neighbor,
                    RelationType::Uses,
                    center,
                    &evidence,
                    EdgeOrientation::Incoming,
                )]),
                DirectionMode::Either => json!([
                    neighbor_json(
                        incoming_neighbor,
                        incoming_assertion,
                        incoming_neighbor,
                        RelationType::Uses,
                        center,
                        &evidence,
                        EdgeOrientation::Incoming,
                    ),
                    neighbor_json(
                        outgoing_neighbor,
                        outgoing_assertion,
                        center,
                        RelationType::Uses,
                        outgoing_neighbor,
                        &evidence,
                        EdgeOrientation::Outgoing,
                    ),
                ]),
            };
            let invocation_count = Arc::new(AtomicUsize::new(0));
            let actual = database()
                .neighbors_with(&query, {
                    let invocation_count = Arc::clone(&invocation_count);
                    let expected_request =
                        query_request(GET_NEIGHBORS_SQL, neighbor_parameters(&query));
                    let response = query_response(complete_payload(result_json));
                    move |request| respond(request, expected_request, response, invocation_count)
                })
                .await
                .expect("directed neighbor results should decode");

            assert_eq!(actual, expected);
            assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn symmetric_neighbor_assertions_match_both_endpoints_once_in_every_direction_mode() {
        let low_endpoint = concept_id(70);
        let high_endpoint = concept_id(71);
        let evidence = vec![memory_source("memory-symmetric", 2)];

        for (index, relation) in [RelationType::RelatedTo, RelationType::Contradicts]
            .into_iter()
            .enumerate()
        {
            let id = assertion_id(120 + u128::try_from(index).expect("index fits u128"));
            for direction in [
                DirectionMode::Outgoing,
                DirectionMode::Incoming,
                DirectionMode::Either,
            ] {
                for (query_concept, neighbor) in
                    [(low_endpoint, high_endpoint), (high_endpoint, low_endpoint)]
                {
                    let query =
                        validated_neighbor_query(query_concept, direction, vec![relation], 4);
                    let expected = vec![neighbor_result_record(
                        neighbor,
                        assertion_record(
                            id,
                            low_endpoint,
                            relation,
                            high_endpoint,
                            evidence.clone(),
                        ),
                        EdgeOrientation::Symmetric,
                    )];
                    let response = query_response(complete_payload(json!([neighbor_json(
                        neighbor,
                        id,
                        low_endpoint,
                        relation,
                        high_endpoint,
                        &evidence,
                        EdgeOrientation::Symmetric,
                    )])));
                    let invocation_count = Arc::new(AtomicUsize::new(0));
                    let actual = database()
                        .neighbors_with(&query, {
                            let invocation_count = Arc::clone(&invocation_count);
                            let expected_request =
                                query_request(GET_NEIGHBORS_SQL, neighbor_parameters(&query));
                            move |request| {
                                respond(request, expected_request, response, invocation_count)
                            }
                        })
                        .await
                        .expect("symmetric neighbor results should decode");

                    assert_eq!(actual, expected);
                    assert_eq!(actual.len(), 1);
                    assert_eq!(actual[0].assertion.id, id);
                    assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
                }
            }
        }
    }

    #[test]
    fn neighbor_sql_follows_direction_and_symmetric_edges_without_duplicate_expansion() {
        assert!(GET_NEIGHBORS_SQL.contains("relation_types.is_symmetric"));
        assert!(GET_NEIGHBORS_SQL.contains("$2::text = 'outgoing'"));
        assert!(GET_NEIGHBORS_SQL.contains("$2::text = 'incoming'"));
        assert!(GET_NEIGHBORS_SQL.contains("$2::text = 'either'"));
        assert!(GET_NEIGHBORS_SQL.contains(
            "assertions.subject_concept_id = $1::text::uuid\n        OR assertions.object_concept_id = $1::text::uuid"
        ));
        assert_eq!(
            GET_NEIGHBORS_SQL
                .matches("FROM knowledge_graph.graph_assertions AS assertions")
                .count(),
            1,
            "either-direction expansion should select each base assertion once"
        );
        assert!(!GET_NEIGHBORS_SQL.contains("UNION"));
    }

    #[test]
    fn neighbor_sql_orders_complete_records_before_the_decimal_string_limit() {
        assert!(GET_NEIGHBORS_SQL.contains("jsonb_array_length($3::jsonb) = 0"));
        assert!(GET_NEIGHBORS_SQL.contains(
            "requested_relations.relation_code COLLATE \"C\" = assertions.relation_code"
        ));
        assert!(GET_NEIGHBORS_SQL.contains(
            "ORDER BY relation_code COLLATE \"C\" ASC,\n    orientation COLLATE \"C\" ASC,\n    neighbor_id ASC,\n    assertion_id ASC\n  LIMIT $4::text::bigint"
        ));
        let limit_offset = GET_NEIGHBORS_SQL
            .find("LIMIT $4::text::bigint")
            .expect("neighbor limit should be present");
        let payload_offset = GET_NEIGHBORS_SQL
            .find("'neighbor', pg_catalog.jsonb_build_object(")
            .expect("complete neighbor projection should be present");
        assert!(limit_offset < payload_offset);
        assert!(GET_NEIGHBORS_SQL.contains("aliases.is_preferred DESC"));
        assert!(GET_NEIGHBORS_SQL.contains("source_references.source_kind COLLATE \"C\" ASC"));
        assert!(GET_NEIGHBORS_SQL.contains("source_references.external_id COLLATE \"C\" ASC"));
        assert!(GET_NEIGHBORS_SQL.contains("source_references.external_version ASC NULLS FIRST"));
        assert!(!GET_NEIGHBORS_SQL.contains("source_content"));
    }

    #[tokio::test]
    async fn related_sources_preserve_typed_roles_deduplicate_per_role_and_order_results() {
        let concept = concept_id(80);
        let shared_source = memory_source("memory-a", 2);
        let mention_source = memory_source("memory-z", 1);
        let evidence_source = memory_source("memory-b", 1);
        let session_source = session_source("session-a");
        let query = validated_related_source_query(concept, 6);
        let expected = vec![
            related_source_record(shared_source.clone(), RelatedSourceRole::Mention),
            related_source_record(mention_source.clone(), RelatedSourceRole::Mention),
            related_source_record(session_source.clone(), RelatedSourceRole::Mention),
            related_source_record(shared_source.clone(), RelatedSourceRole::AssertionEvidence),
            related_source_record(
                evidence_source.clone(),
                RelatedSourceRole::AssertionEvidence,
            ),
            related_source_record(session_source.clone(), RelatedSourceRole::AssertionEvidence),
        ];
        let response = query_response(complete_payload(json!([
            related_source_json(&shared_source, RelatedSourceRole::Mention),
            related_source_json(&mention_source, RelatedSourceRole::Mention),
            related_source_json(&session_source, RelatedSourceRole::Mention),
            related_source_json(&shared_source, RelatedSourceRole::AssertionEvidence),
            related_source_json(&evidence_source, RelatedSourceRole::AssertionEvidence),
            related_source_json(&session_source, RelatedSourceRole::AssertionEvidence),
        ])));
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let actual = database()
            .related_sources_with(&query, {
                let invocation_count = Arc::clone(&invocation_count);
                let expected_request =
                    query_request(GET_RELATED_SOURCES_SQL, related_source_parameters(&query));
                move |request| respond(request, expected_request, response, invocation_count)
            })
            .await
            .expect("complete related-source results should decode");

        assert_eq!(actual, expected);
        assert_eq!(actual[0].role, RelatedSourceRole::Mention);
        assert_eq!(actual[3].role, RelatedSourceRole::AssertionEvidence);
        assert_eq!(actual[0].source, actual[3].source);
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn related_source_sql_deduplicates_typed_links_and_orders_before_limit() {
        assert!(GET_RELATED_SOURCES_SQL.contains("\n  UNION\n"));
        assert!(!GET_RELATED_SOURCES_SQL.contains("UNION ALL"));
        assert!(GET_RELATED_SOURCES_SQL.contains("'mention'::text AS role"));
        assert!(GET_RELATED_SOURCES_SQL.contains("'assertion_evidence'::text AS role"));
        assert!(GET_RELATED_SOURCES_SQL.contains("WHERE mentions.concept_id = $1::text::uuid"));
        assert!(GET_RELATED_SOURCES_SQL.contains(
            "WHERE assertions.subject_concept_id = $1::text::uuid\n    OR assertions.object_concept_id = $1::text::uuid"
        ));
        assert!(
            GET_RELATED_SOURCES_SQL.contains("source_kind, external_id, external_version, role")
        );
        assert!(GET_RELATED_SOURCES_SQL.contains(
            "ORDER BY role_order ASC,\n    source_kind COLLATE \"C\" ASC,\n    external_id COLLATE \"C\" ASC,\n    external_version ASC NULLS FIRST\n  LIMIT $2::text::bigint"
        ));
        assert!(GET_RELATED_SOURCES_SQL.contains("CASE role WHEN 'mention' THEN 0 ELSE 1 END"));
        assert!(!GET_RELATED_SOURCES_SQL.contains("source_content"));
    }

    #[tokio::test]
    async fn neighbor_and_related_source_queries_return_complete_empty_arrays() {
        let neighbor_query =
            validated_neighbor_query(concept_id(90), DirectionMode::Either, Vec::new(), 5);
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let neighbors = database()
            .neighbors_with(&neighbor_query, {
                let invocation_count = Arc::clone(&invocation_count);
                let expected = query_request(
                    GET_NEIGHBORS_SQL,
                    json!([concept_id(90).as_uuid().to_string(), "either", [], "5"]),
                );
                let response = query_response(complete_payload(json!([])));
                move |request| respond(request, expected, response, invocation_count)
            })
            .await
            .expect("no neighbor matches should be a complete empty result");
        assert!(neighbors.is_empty());
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);

        let related_query = validated_related_source_query(concept_id(91), 7);
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let related = database()
            .related_sources_with(&related_query, {
                let invocation_count = Arc::clone(&invocation_count);
                let expected = query_request(
                    GET_RELATED_SOURCES_SQL,
                    json!([concept_id(91).as_uuid().to_string(), "7"]),
                );
                let response = query_response(complete_payload(json!([])));
                move |request| respond(request, expected, response, invocation_count)
            })
            .await
            .expect("no related sources should be a complete empty result");
        assert!(related.is_empty());
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn both_discovery_queries_aggregate_one_payload_and_emit_only_the_exact_size_marker() {
        for sql in [GET_NEIGHBORS_SQL, GET_RELATED_SOURCES_SQL] {
            assert!(sql.contains("'outcome', 'complete'"));
            assert!(sql.contains("pg_catalog.jsonb_agg"));
            assert!(
                sql.contains("pg_catalog.octet_length(complete_payload.payload::text) > 4194304")
            );
            assert!(
                sql.contains("THEN pg_catalog.jsonb_build_object('outcome', 'result_too_large')")
            );
        }
    }

    #[tokio::test]
    async fn discovery_queries_map_size_markers_and_reject_malformed_results_without_leaks() {
        let neighbor_query =
            validated_neighbor_query(concept_id(100), DirectionMode::Outgoing, Vec::new(), 1);
        let result_marker = query_response(json!({"outcome": "result_too_large"}));
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let neighbor_error = database()
            .neighbors_with(&neighbor_query, {
                let invocation_count = Arc::clone(&invocation_count);
                let expected =
                    query_request(GET_NEIGHBORS_SQL, neighbor_parameters(&neighbor_query));
                move |request| respond(request, expected, result_marker, invocation_count)
            })
            .await
            .expect_err("an oversized read should not return a partial result");
        assert_eq!(
            neighbor_error,
            GraphError::ResultBoundExceeded {
                operation: OperationCode::Neighbors,
            }
        );
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);

        let related_query = validated_related_source_query(concept_id(101), 1);
        let result_marker = query_response(json!({"outcome": "result_too_large"}));
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let related_error = database()
            .related_sources_with(&related_query, {
                let invocation_count = Arc::clone(&invocation_count);
                let expected = query_request(
                    GET_RELATED_SOURCES_SQL,
                    related_source_parameters(&related_query),
                );
                move |request| respond(request, expected, result_marker, invocation_count)
            })
            .await
            .expect_err("an oversized read should not return partial sources");
        assert_eq!(
            related_error,
            GraphError::ResultBoundExceeded {
                operation: OperationCode::RelatedSources,
            }
        );
        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);

        let mut invalid_neighbor = neighbor_json(
            concept_id(102),
            assertion_id(130),
            concept_id(100),
            RelationType::Uses,
            concept_id(102),
            &[session_source(SOURCE_KEY_SENTINEL)],
            EdgeOrientation::Outgoing,
        );
        invalid_neighbor["unexpected_source_content"] = json!(SOURCE_KEY_SENTINEL);
        let response = query_response(complete_payload(json!([invalid_neighbor])));
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let neighbor_error = database()
            .neighbors_with(&neighbor_query, {
                let invocation_count = Arc::clone(&invocation_count);
                let expected =
                    query_request(GET_NEIGHBORS_SQL, neighbor_parameters(&neighbor_query));
                move |request| respond(request, expected, response, invocation_count)
            })
            .await
            .expect_err("unexpected result fields must be rejected");
        assert_eq!(
            neighbor_error,
            GraphError::InvalidResponse {
                operation: OperationCode::Neighbors,
                reason: BackendFailureReason::UnsupportedValue,
            }
        );
        assert_opaque(&neighbor_error);

        let mut invalid_source = related_source_json(
            &session_source(SOURCE_KEY_SENTINEL),
            RelatedSourceRole::Mention,
        );
        invalid_source["source"]["source_content"] = json!(SOURCE_KEY_SENTINEL);
        let response = query_response(complete_payload(json!([invalid_source])));
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let related_error = database()
            .related_sources_with(&related_query, {
                let invocation_count = Arc::clone(&invocation_count);
                let expected = query_request(
                    GET_RELATED_SOURCES_SQL,
                    related_source_parameters(&related_query),
                );
                move |request| respond(request, expected, response, invocation_count)
            })
            .await
            .expect_err("untyped source payloads must be rejected");
        assert_eq!(
            related_error,
            GraphError::InvalidResponse {
                operation: OperationCode::RelatedSources,
                reason: BackendFailureReason::UnsupportedValue,
            }
        );
        assert_opaque(&related_error);
    }

    #[tokio::test]
    async fn discovery_invocation_failures_are_operation_only_and_not_retried() {
        let neighbor_query =
            validated_neighbor_query(concept_id(110), DirectionMode::Either, Vec::new(), 1);
        let neighbor_invocations = Arc::new(AtomicUsize::new(0));
        let neighbor_error = database()
            .neighbors_with(&neighbor_query, {
                let neighbor_invocations = Arc::clone(&neighbor_invocations);
                move |_| {
                    neighbor_invocations.fetch_add(1, Ordering::SeqCst);
                    async { Err(IiiError::Handler(SOURCE_KEY_SENTINEL.to_owned())) }
                }
            })
            .await
            .expect_err("backend failures should not return neighbors");
        assert_eq!(
            neighbor_error,
            expected_database_failure(OperationCode::Neighbors)
        );
        assert_opaque(&neighbor_error);
        assert_eq!(neighbor_invocations.load(Ordering::SeqCst), 1);

        let related_query = validated_related_source_query(concept_id(111), 1);
        let related_invocations = Arc::new(AtomicUsize::new(0));
        let related_error = database()
            .related_sources_with(&related_query, {
                let related_invocations = Arc::clone(&related_invocations);
                move |_| {
                    related_invocations.fetch_add(1, Ordering::SeqCst);
                    async { Err(IiiError::Handler(SOURCE_KEY_SENTINEL.to_owned())) }
                }
            })
            .await
            .expect_err("backend failures should not return related sources");
        assert_eq!(
            related_error,
            expected_database_failure(OperationCode::RelatedSources)
        );
        assert_opaque(&related_error);
        assert_eq!(related_invocations.load(Ordering::SeqCst), 1);
    }
}

#[cfg(test)]
mod iii_path_query_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use iii_sdk::{Error as IiiError, IIIClient, protocol::TriggerRequest};
    use serde_json::{Value, json};
    use uuid::Uuid;

    use crate::contracts::{
        AssertionId, BackendFailureReason, ConceptId, DatabaseTimeouts, DirectionMode,
        EdgeOrientation, GraphError, GraphPath, OperationCode, PathQuery, RelationType, Revision,
        SourceReference, SourceReferenceInput, TraversalBound, TraversalReason, ValidatedPathQuery,
    };

    use super::{
        DATABASE_QUERY_FUNCTION_ID, DatabaseTarget, GET_PATHS_SQL, IiiKnowledgeGraphDatabase,
        KnowledgeGraphDatabase,
    };

    const DATABASE_TARGET: &str = "path-query-operation-database-target";
    const SOURCE_KEY_SENTINEL: &str = "PRIVATE_PATH_SOURCE_KEY_SENTINEL";
    const WORKER_MESSAGE_SENTINEL: &str = "private-path-worker-message-sentinel";
    const SDK_STACKTRACE_SENTINEL: &str = "private-path-sdk-stacktrace-sentinel";
    const UUID_V7_BASE: u128 = 0x0000_0000_0000_7000_8000_0000_0000_0000;

    struct ExpectedRequest {
        payload: Value,
        timeout_ms: Option<u64>,
    }

    #[derive(Clone)]
    struct PathEdge {
        id: AssertionId,
        subject: ConceptId,
        relation: RelationType,
        object: ConceptId,
        evidence: Vec<SourceReference>,
        orientation: EdgeOrientation,
    }

    fn database() -> IiiKnowledgeGraphDatabase {
        IiiKnowledgeGraphDatabase::new(
            IIIClient::new("ws://127.0.0.1:1"),
            DatabaseTarget::try_from(DATABASE_TARGET.to_owned())
                .expect("test database target should be valid"),
            DatabaseTimeouts::try_new(Duration::from_secs(4), Duration::from_secs(13))
                .expect("test timeouts should be valid"),
        )
        .expect("adapter configuration should be valid")
    }

    fn concept_id(value: u128) -> ConceptId {
        ConceptId::try_from(Uuid::from_u128(UUID_V7_BASE + value))
            .expect("fixture concept ID should be UUIDv7")
    }

    fn assertion_id(value: u128) -> AssertionId {
        AssertionId::try_from(Uuid::from_u128(UUID_V7_BASE + value))
            .expect("fixture assertion ID should be UUIDv7")
    }

    fn revision() -> Revision {
        Revision::try_from(7).expect("fixture revision should be positive")
    }

    fn memory_source(memory_id: &str, version: i64) -> SourceReference {
        SourceReference::try_from(SourceReferenceInput::MemoryVersion {
            memory_id: memory_id.to_owned(),
            version,
        })
        .expect("fixture memory source should be valid")
    }

    fn session_source(session_record_id: &str) -> SourceReference {
        SourceReference::try_from(SourceReferenceInput::SessionRecord {
            session_record_id: session_record_id.to_owned(),
        })
        .expect("fixture session source should be valid")
    }

    fn edge(
        id: AssertionId,
        subject: ConceptId,
        relation: RelationType,
        object: ConceptId,
        evidence: Vec<SourceReference>,
        orientation: EdgeOrientation,
    ) -> PathEdge {
        PathEdge {
            id,
            subject,
            relation,
            object,
            evidence,
            orientation,
        }
    }

    fn source_json(source: &SourceReference) -> Value {
        match source {
            SourceReference::MemoryVersion { memory_id, version } => json!({
                "kind": "memory_version",
                "memory_id": memory_id.as_str(),
                "version": version.get(),
            }),
            SourceReference::SessionRecord { session_record_id } => json!({
                "kind": "session_record",
                "session_record_id": session_record_id.as_str(),
            }),
        }
    }

    fn assertion_json(edge: &PathEdge) -> Value {
        json!({
            "id": edge.id.as_uuid().to_string(),
            "revision": revision().get(),
            "subject_concept_id": edge.subject.as_uuid().to_string(),
            "relation_type": edge.relation.as_str(),
            "object_concept_id": edge.object.as_uuid().to_string(),
            "evidence": edge.evidence.iter().map(source_json).collect::<Vec<_>>(),
        })
    }

    fn path_json(concepts: &[ConceptId], edges: &[PathEdge]) -> Value {
        json!({
            "concepts": concepts
                .iter()
                .map(|concept| concept.as_uuid().to_string())
                .collect::<Vec<_>>(),
            "assertions": edges
                .iter()
                .map(|edge| json!({
                    "assertion": assertion_json(edge),
                    "orientation": edge.orientation.as_str(),
                }))
                .collect::<Vec<_>>(),
        })
    }

    fn graph_path(concepts: &[ConceptId], edges: &[PathEdge]) -> GraphPath {
        serde_json::from_value(path_json(concepts, edges))
            .expect("fixture path should satisfy the path contract")
    }

    fn validated_path_query(
        from: ConceptId,
        to: ConceptId,
        direction: DirectionMode,
        max_depth: u8,
        max_work: u32,
        limit: u16,
    ) -> ValidatedPathQuery {
        ValidatedPathQuery::try_from(PathQuery {
            from,
            to,
            direction,
            max_depth,
            max_work,
            limit,
        })
        .expect("fixture path query should be valid")
    }

    fn query_parameters(query: &ValidatedPathQuery) -> Value {
        json!([
            query.from.as_uuid().to_string(),
            query.to.as_uuid().to_string(),
            query.direction.as_str(),
            query.max_depth.to_string(),
            query.max_work.to_string(),
            query.limit.to_string(),
        ])
    }

    fn query_request(query: &ValidatedPathQuery) -> ExpectedRequest {
        ExpectedRequest {
            payload: json!({
                "db": DATABASE_TARGET,
                "sql": GET_PATHS_SQL,
                "params": query_parameters(query),
                "timeout_ms": 4_000,
            }),
            timeout_ms: Some(13_000),
        }
    }

    fn query_response(payload: Value) -> Value {
        json!({
            "rows": [{"payload": payload}],
            "row_count": 1,
            "columns": [{"name": "payload", "type": "jsonb"}],
        })
    }

    fn complete_payload(result: Value) -> Value {
        json!({"outcome": "complete", "result": result})
    }

    async fn respond(
        request: TriggerRequest,
        expected: ExpectedRequest,
        response: Value,
        invocation_count: Arc<AtomicUsize>,
    ) -> Result<Value, IiiError> {
        invocation_count.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.function_id, DATABASE_QUERY_FUNCTION_ID);
        assert_eq!(request.payload, expected.payload);
        assert!(request.action.is_none());
        assert_eq!(request.timeout_ms, expected.timeout_ms);
        Ok(response)
    }

    async fn path_query_with_payload(
        query: &ValidatedPathQuery,
        payload: Value,
    ) -> (Result<Vec<GraphPath>, GraphError>, usize) {
        let response = query_response(payload);
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let result = database()
            .find_paths_with(query, {
                let invocation_count = Arc::clone(&invocation_count);
                let expected = query_request(query);
                move |request| respond(request, expected, response, invocation_count)
            })
            .await;
        (result, invocation_count.load(Ordering::SeqCst))
    }

    fn assert_opaque(error: &GraphError, sentinels: &[&str]) {
        for message in [error.to_string(), format!("{error:?}")] {
            for sentinel in sentinels {
                assert!(
                    !message.contains(sentinel),
                    "path-query error leaked protected data: {message}"
                );
            }
        }
    }

    #[tokio::test]
    async fn path_query_orders_shortest_paths_by_native_uuid_sequences_and_keeps_complete_evidence()
    {
        let from = concept_id(1);
        let first_branch = concept_id(2);
        let second_branch = concept_id(3);
        let to = concept_id(4);
        let evidence = vec![
            memory_source("path-evidence-alpha", 1),
            memory_source("path-evidence-alpha", 2),
            session_source("path-evidence-omega"),
        ];
        let direct_first = edge(
            assertion_id(10),
            from,
            RelationType::Uses,
            to,
            evidence.clone(),
            EdgeOrientation::Outgoing,
        );
        let direct_second = edge(
            assertion_id(11),
            from,
            RelationType::Implements,
            to,
            evidence.clone(),
            EdgeOrientation::Outgoing,
        );
        let branch_one_first = edge(
            assertion_id(1),
            from,
            RelationType::Uses,
            first_branch,
            evidence.clone(),
            EdgeOrientation::Outgoing,
        );
        let branch_one_second = edge(
            assertion_id(9),
            first_branch,
            RelationType::DependsOn,
            to,
            evidence.clone(),
            EdgeOrientation::Outgoing,
        );
        let branch_two_first = edge(
            assertion_id(2),
            from,
            RelationType::Uses,
            second_branch,
            evidence.clone(),
            EdgeOrientation::Outgoing,
        );
        let branch_two_second = edge(
            assertion_id(3),
            second_branch,
            RelationType::DependsOn,
            to,
            evidence.clone(),
            EdgeOrientation::Outgoing,
        );
        let query = validated_path_query(from, to, DirectionMode::Outgoing, 4, 701, 4);
        let payload = complete_payload(json!([
            path_json(&[from, to], std::slice::from_ref(&direct_first)),
            path_json(&[from, to], std::slice::from_ref(&direct_second)),
            path_json(
                &[from, first_branch, to],
                &[branch_one_first.clone(), branch_one_second.clone()],
            ),
            path_json(
                &[from, second_branch, to],
                &[branch_two_first.clone(), branch_two_second.clone()],
            ),
        ]));

        let (actual, invocation_count) = path_query_with_payload(&query, payload).await;
        let paths = actual.expect("complete path payload should decode");
        assert_eq!(
            paths,
            vec![
                graph_path(&[from, to], &[direct_first]),
                graph_path(&[from, to], &[direct_second]),
                graph_path(
                    &[from, first_branch, to],
                    &[branch_one_first, branch_one_second],
                ),
                graph_path(
                    &[from, second_branch, to],
                    &[branch_two_first, branch_two_second],
                ),
            ]
        );
        assert_eq!(paths[0].assertions[0].assertion.evidence, evidence);
        assert_eq!(invocation_count, 1);
    }

    #[tokio::test]
    async fn path_query_direction_modes_allow_only_requested_directed_edges_and_symmetric_edges() {
        let from = concept_id(20);
        let to = concept_id(21);
        let evidence = vec![memory_source("path-direction-evidence", 4)];
        let outgoing = edge(
            assertion_id(1),
            from,
            RelationType::Uses,
            to,
            evidence.clone(),
            EdgeOrientation::Outgoing,
        );
        let incoming = edge(
            assertion_id(2),
            to,
            RelationType::Uses,
            from,
            evidence.clone(),
            EdgeOrientation::Incoming,
        );
        let symmetric = edge(
            assertion_id(3),
            from,
            RelationType::RelatedTo,
            to,
            evidence,
            EdgeOrientation::Symmetric,
        );
        let cases = [
            (
                DirectionMode::Outgoing,
                vec![outgoing.clone(), symmetric.clone()],
            ),
            (
                DirectionMode::Incoming,
                vec![incoming.clone(), symmetric.clone()],
            ),
            (
                DirectionMode::Either,
                vec![outgoing.clone(), incoming.clone(), symmetric.clone()],
            ),
        ];

        for (direction, edges) in cases {
            let query = validated_path_query(from, to, direction, 1, 10, 3);
            let result = edges
                .iter()
                .map(|edge| path_json(&[from, to], std::slice::from_ref(edge)))
                .collect::<Vec<_>>();
            let (actual, invocation_count) =
                path_query_with_payload(&query, complete_payload(json!(result))).await;

            assert_eq!(
                actual.expect("direction-compatible path payload should decode"),
                edges
                    .iter()
                    .map(|edge| graph_path(&[from, to], std::slice::from_ref(edge)))
                    .collect::<Vec<_>>()
            );
            assert_eq!(invocation_count, 1);
        }
    }

    #[tokio::test]
    async fn empty_paths_and_payload_free_work_and_result_markers_map_to_typed_errors() {
        let query = validated_path_query(
            concept_id(30),
            concept_id(31),
            DirectionMode::Either,
            5,
            81,
            7,
        );
        let (empty, invocation_count) =
            path_query_with_payload(&query, complete_payload(json!([]))).await;
        assert!(
            empty
                .expect("empty path result should be successful")
                .is_empty()
        );
        assert_eq!(invocation_count, 1);

        let (work_error, invocation_count) =
            path_query_with_payload(&query, json!({"outcome": "work_exhausted"})).await;
        assert_eq!(
            work_error,
            Err(GraphError::TraversalBoundExceeded {
                operation: OperationCode::FindPaths,
                bound: TraversalBound::Work,
                reason: TraversalReason::WorkExhausted,
            })
        );
        assert_eq!(invocation_count, 1);

        let (partial_work, invocation_count) = path_query_with_payload(
            &query,
            json!({"outcome": "work_exhausted", "result": [{"partial_path": true}]}),
        )
        .await;
        assert_eq!(
            partial_work,
            Err(GraphError::InvalidResponse {
                operation: OperationCode::FindPaths,
                reason: BackendFailureReason::MalformedResponse,
            })
        );
        assert_eq!(invocation_count, 1);

        let (size_error, invocation_count) =
            path_query_with_payload(&query, json!({"outcome": "result_too_large"})).await;
        assert_eq!(
            size_error,
            Err(GraphError::ResultBoundExceeded {
                operation: OperationCode::FindPaths,
            })
        );
        assert_eq!(invocation_count, 1);
    }

    #[tokio::test]
    async fn path_query_rejects_cycles_malformed_edges_wrong_endpoints_depth_and_excess_results() {
        let from = concept_id(40);
        let middle = concept_id(41);
        let to = concept_id(42);
        let query = validated_path_query(from, to, DirectionMode::Either, 3, 50, 2);
        let evidence = vec![session_source(SOURCE_KEY_SENTINEL)];
        let forward = edge(
            assertion_id(1),
            from,
            RelationType::Uses,
            middle,
            evidence.clone(),
            EdgeOrientation::Outgoing,
        );
        let back = edge(
            assertion_id(2),
            from,
            RelationType::Uses,
            middle,
            evidence.clone(),
            EdgeOrientation::Incoming,
        );
        let finish = edge(
            assertion_id(3),
            from,
            RelationType::Uses,
            to,
            evidence.clone(),
            EdgeOrientation::Outgoing,
        );
        let cyclic_path = path_json(&[from, middle, from, to], &[forward, back, finish]);
        let (cycle_error, _) =
            path_query_with_payload(&query, complete_payload(json!([cyclic_path]))).await;
        let expected_malformed = GraphError::InvalidResponse {
            operation: OperationCode::FindPaths,
            reason: BackendFailureReason::UnsupportedValue,
        };
        assert_eq!(cycle_error, Err(expected_malformed));
        assert_opaque(
            &expected_malformed,
            &[
                SOURCE_KEY_SENTINEL,
                DATABASE_TARGET,
                &from.as_uuid().to_string(),
                &middle.as_uuid().to_string(),
                &to.as_uuid().to_string(),
            ],
        );

        let wrong_orientation = edge(
            assertion_id(4),
            from,
            RelationType::Uses,
            to,
            evidence.clone(),
            EdgeOrientation::Incoming,
        );
        let (orientation_error, _) = path_query_with_payload(
            &query,
            complete_payload(json!([path_json(&[from, to], &[wrong_orientation])])),
        )
        .await;
        assert_eq!(orientation_error, Err(expected_malformed));

        let wrong_direction = edge(
            assertion_id(12),
            to,
            RelationType::Uses,
            from,
            evidence.clone(),
            EdgeOrientation::Incoming,
        );
        let outgoing_query = validated_path_query(from, to, DirectionMode::Outgoing, 3, 50, 2);
        let (direction_error, _) = path_query_with_payload(
            &outgoing_query,
            complete_payload(json!([path_json(&[from, to], &[wrong_direction])])),
        )
        .await;
        assert_eq!(direction_error, Err(expected_malformed));

        let high_id = edge(
            assertion_id(14),
            from,
            RelationType::Uses,
            to,
            evidence.clone(),
            EdgeOrientation::Outgoing,
        );
        let low_id = edge(
            assertion_id(13),
            from,
            RelationType::Uses,
            to,
            evidence.clone(),
            EdgeOrientation::Outgoing,
        );
        let (order_error, _) = path_query_with_payload(
            &query,
            complete_payload(json!([
                path_json(&[from, to], &[high_id]),
                path_json(&[from, to], &[low_id]),
            ])),
        )
        .await;
        assert_eq!(order_error, Err(expected_malformed));

        let outside_query = validated_path_query(from, to, DirectionMode::Either, 1, 50, 2);
        let two_hop = vec![
            edge(
                assertion_id(5),
                from,
                RelationType::Uses,
                middle,
                evidence.clone(),
                EdgeOrientation::Outgoing,
            ),
            edge(
                assertion_id(6),
                middle,
                RelationType::Uses,
                to,
                evidence.clone(),
                EdgeOrientation::Outgoing,
            ),
        ];
        let (depth_error, _) = path_query_with_payload(
            &outside_query,
            complete_payload(json!([path_json(&[from, middle, to], &two_hop)])),
        )
        .await;
        assert_eq!(depth_error, Err(expected_malformed));

        let wrong_endpoint = edge(
            assertion_id(7),
            middle,
            RelationType::Uses,
            to,
            evidence.clone(),
            EdgeOrientation::Outgoing,
        );
        let (endpoint_error, _) = path_query_with_payload(
            &query,
            complete_payload(json!([path_json(&[middle, to], &[wrong_endpoint])])),
        )
        .await;
        assert_eq!(endpoint_error, Err(expected_malformed));

        let limited_query = validated_path_query(from, to, DirectionMode::Either, 3, 50, 1);
        let direct_one = edge(
            assertion_id(8),
            from,
            RelationType::Uses,
            to,
            evidence.clone(),
            EdgeOrientation::Outgoing,
        );
        let direct_two = edge(
            assertion_id(9),
            from,
            RelationType::Implements,
            to,
            evidence,
            EdgeOrientation::Outgoing,
        );
        let (limit_error, _) = path_query_with_payload(
            &limited_query,
            complete_payload(json!([
                path_json(&[from, to], &[direct_one]),
                path_json(&[from, to], &[direct_two]),
            ])),
        )
        .await;
        assert_eq!(limit_error, Err(expected_malformed));
    }

    #[test]
    fn path_sql_tracks_cycle_safe_uuid_sequences_and_fences_work_before_path_ordering() {
        assert!(GET_PATHS_SQL.trim_start().starts_with("WITH RECURSIVE"));
        assert!(GET_PATHS_SQL.contains("UNION ALL"));
        assert!(GET_PATHS_SQL.contains("ARRAY[start_concepts.id]::uuid[]"));
        assert!(GET_PATHS_SQL.contains("ARRAY[]::uuid[]"));
        assert!(GET_PATHS_SQL.contains("path_states.assertion_ids || assertions.id"));
        assert!(GET_PATHS_SQL.contains("path_states.concept_ids || CASE"));
        assert!(GET_PATHS_SQL.contains("= ANY(path_states.concept_ids)"));
        assert!(GET_PATHS_SQL.contains("NOT (assertions.id = ANY(path_states.assertion_ids))"));
        assert!(GET_PATHS_SQL.contains("path_states.hops < $4::text::bigint"));
        assert!(GET_PATHS_SQL.contains("path_states.current_concept_id <> $2::text::uuid"));
        assert!(GET_PATHS_SQL.contains("$3::text = 'outgoing'"));
        assert!(GET_PATHS_SQL.contains("$3::text = 'incoming'"));
        assert!(GET_PATHS_SQL.contains("$3::text = 'either'"));
        assert!(GET_PATHS_SQL.contains("relation_types.is_symmetric"));
        assert!(GET_PATHS_SQL.contains("THEN 'symmetric'::text"));
        assert!(GET_PATHS_SQL.contains("THEN 'outgoing'::text"));
        assert!(GET_PATHS_SQL.contains("ELSE 'incoming'::text"));

        let fence_offset = GET_PATHS_SQL
            .find("fenced_path_states AS MATERIALIZED")
            .expect("recursive work fence should be materialized");
        let ordering_offset = GET_PATHS_SQL
            .find("ORDER BY pg_catalog.cardinality(path_states.assertion_ids) ASC")
            .expect("path ordering should be deterministic");
        assert!(fence_offset < ordering_offset);
        assert!(GET_PATHS_SQL.contains("LIMIT $5::text::bigint + 1"));
        assert!(GET_PATHS_SQL.contains("path_state_count.state_count > $5::text::bigint"));
        assert!(
            GET_PATHS_SQL
                .contains("THEN pg_catalog.jsonb_build_object('outcome', 'work_exhausted')")
        );
        assert!(GET_PATHS_SQL.contains(
            "ORDER BY pg_catalog.cardinality(path_states.assertion_ids) ASC,\n    path_states.assertion_ids ASC,\n    path_states.concept_ids ASC\n  LIMIT $6::text::bigint"
        ));
        assert!(GET_PATHS_SQL.contains("'evidence', COALESCE("));
        assert!(GET_PATHS_SQL.contains("source_references.source_kind COLLATE \"C\" ASC"));
        assert!(GET_PATHS_SQL.contains("source_references.external_id COLLATE \"C\" ASC"));
        assert!(GET_PATHS_SQL.contains("source_references.external_version ASC NULLS FIRST"));
        assert!(
            GET_PATHS_SQL
                .contains("pg_catalog.octet_length(complete_payload.payload::text) > 4194304")
        );
        assert!(
            GET_PATHS_SQL
                .contains("THEN pg_catalog.jsonb_build_object('outcome', 'result_too_large')")
        );
        assert!(!GET_PATHS_SQL.contains("source_content"));
    }

    #[tokio::test]
    async fn path_query_exact_worker_timeout_maps_to_query_timeout_without_retry_or_leak() {
        let query = validated_path_query(
            concept_id(50),
            concept_id(51),
            DirectionMode::Either,
            5,
            81,
            7,
        );
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let callback_count = Arc::clone(&invocation_count);
        let sdk_error = IiiError::Remote {
            code: "invocation_failed".to_owned(),
            message: format!(
                "handler error: {{\"code\":\"QUERY_TIMEOUT\",\"message\":\"{WORKER_MESSAGE_SENTINEL}\"}}"
            ),
            stacktrace: Some(SDK_STACKTRACE_SENTINEL.to_owned()),
        };
        let expected = query_request(&query);

        let error = database()
            .find_paths_with(&query, move |request| {
                callback_count.fetch_add(1, Ordering::SeqCst);
                async move {
                    assert_eq!(request.function_id, DATABASE_QUERY_FUNCTION_ID);
                    assert_eq!(request.payload, expected.payload);
                    assert_eq!(request.timeout_ms, expected.timeout_ms);
                    Err(sdk_error)
                }
            })
            .await
            .expect_err("the exact worker timeout should be classified");

        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            error,
            GraphError::TraversalBoundExceeded {
                operation: OperationCode::FindPaths,
                bound: TraversalBound::QueryTimeout,
                reason: TraversalReason::QueryTimeout,
            }
        );
        assert_opaque(
            &error,
            &[
                DATABASE_TARGET,
                WORKER_MESSAGE_SENTINEL,
                SDK_STACKTRACE_SENTINEL,
                &query.from.as_uuid().to_string(),
                &query.to.as_uuid().to_string(),
            ],
        );
    }

    #[tokio::test]
    async fn path_query_timeout_near_miss_is_an_opaque_operation_failure() {
        let query = validated_path_query(
            concept_id(60),
            concept_id(61),
            DirectionMode::Either,
            5,
            81,
            7,
        );
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let callback_count = Arc::clone(&invocation_count);
        let error_message = format!(
            "handler error: {{\"code\":\"QUERY_TIMEOUT_NEAR_MISS\",\"message\":\"{WORKER_MESSAGE_SENTINEL}\"}}"
        );

        let error = database()
            .find_paths_with(&query, move |_| {
                callback_count.fetch_add(1, Ordering::SeqCst);
                let error_message = error_message.clone();
                async move {
                    Err(IiiError::Remote {
                        code: "invocation_failed".to_owned(),
                        message: error_message,
                        stacktrace: Some(SDK_STACKTRACE_SENTINEL.to_owned()),
                    })
                }
            })
            .await
            .expect_err("a timeout near miss should remain a database failure");

        assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            error,
            GraphError::DatabaseFailure {
                operation: OperationCode::FindPaths,
                reason: BackendFailureReason::DatabaseFailure,
            }
        );
        assert_opaque(
            &error,
            &[
                DATABASE_TARGET,
                WORKER_MESSAGE_SENTINEL,
                SDK_STACKTRACE_SENTINEL,
            ],
        );
    }

    #[test]
    fn concrete_iii_adapter_satisfies_the_knowledge_graph_database_port() {
        fn assert_port_implementation<T: KnowledgeGraphDatabase>() {}

        assert_port_implementation::<IiiKnowledgeGraphDatabase>();
    }
}
