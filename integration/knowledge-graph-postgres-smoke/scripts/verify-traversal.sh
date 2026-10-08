#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 0 ]]; then
	printf 'usage: %s\n' "$0" >&2
	exit 64
fi

unset PGOPTIONS PGSERVICE PGPASSWORD

for variable in \
	KNOWLEDGE_GRAPH_POSTGRES_BIN \
	KNOWLEDGE_GRAPH_POSTGRES_SOCKET_DIR \
	KNOWLEDGE_GRAPH_POSTGRES_DATABASE \
	KNOWLEDGE_GRAPH_POSTGRES_BOOTSTRAP_ROLE \
	KNOWLEDGE_GRAPH_POSTGRES_MIGRATION_ROLE \
	KNOWLEDGE_GRAPH_POSTGRES_APPLICATION_ROLE \
	KNOWLEDGE_GRAPH_POSTGRES_EXPECTED_MAJOR \
	KNOWLEDGE_GRAPH_POSTGRES_MIGRATION \
	KNOWLEDGE_GRAPH_ADAPTER_QUERY_TIMEOUT_SECONDS \
	KNOWLEDGE_GRAPH_POSTGRES_STATEMENT_TIMEOUT_SECONDS \
	KNOWLEDGE_GRAPH_III_INVOCATION_TIMEOUT_SECONDS \
	KNOWLEDGE_GRAPH_ROW_CHANGE_MODE \
	PGHOST \
	PGPORT \
	PGDATABASE \
	PGUSER; do
	if [[ -z "${!variable:-}" ]]; then
		printf 'missing fixture environment variable: %s\n' "$variable" >&2
		exit 64
	fi
done

case "$KNOWLEDGE_GRAPH_POSTGRES_EXPECTED_MAJOR" in
17 | 18) ;;
*)
	printf 'unsupported PostgreSQL major\n' >&2
	exit 64
	;;
esac

case "$KNOWLEDGE_GRAPH_ROW_CHANGE_MODE" in
statement-capture | row-change-publishing-disabled) ;;
*)
	printf 'unsupported database-worker prerequisite mode\n' >&2
	exit 64
	;;
esac

readonly postgres_bin="$KNOWLEDGE_GRAPH_POSTGRES_BIN"
readonly socket_dir="$KNOWLEDGE_GRAPH_POSTGRES_SOCKET_DIR"
readonly database="$KNOWLEDGE_GRAPH_POSTGRES_DATABASE"
readonly bootstrap_role="$KNOWLEDGE_GRAPH_POSTGRES_BOOTSTRAP_ROLE"
readonly migration_role="$KNOWLEDGE_GRAPH_POSTGRES_MIGRATION_ROLE"
readonly application_role="$KNOWLEDGE_GRAPH_POSTGRES_APPLICATION_ROLE"
readonly expected_major="$KNOWLEDGE_GRAPH_POSTGRES_EXPECTED_MAJOR"
readonly schema_migration="$KNOWLEDGE_GRAPH_POSTGRES_MIGRATION"
readonly statement_timeout_seconds=12
readonly script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly psql="$postgres_bin/psql"
readonly rollback_script="$script_dir/rollback.sh"
schema_applied=false

readonly get_neighbors_sql="$(cat <<'SQL'
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
SQL
)"

readonly get_related_sources_sql="$(cat <<'SQL'
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
SQL
)"

readonly get_paths_sql="$(cat <<'SQL'
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
SQL
)"

readonly neighbor_root_id='018f0000-0000-7000-8000-000000000100'
readonly related_to_neighbor_peer_id='018f0000-0000-7000-8000-0000000000ff'
readonly contradicts_neighbor_peer_id='018f0000-0000-7000-8000-000000000108'
readonly neighbor_parameter_types='text, text, jsonb, text'
readonly empty_concept_id='018f0000-0000-7000-8000-000000000109'
readonly path_start_id='018f0000-0000-7000-8000-000000000200'
readonly path_target_id='018f0000-0000-7000-8000-000000000204'
timeout_dir=''
timeout_fifo=''
timeout_fifo_fd=''
timeout_fifo_open=false
timeout_owner_pid=''
timeout_query_pid=''
timeout_owner_stdout=''
timeout_owner_stderr=''
timeout_query_stdout=''
timeout_query_stderr=''
readonly expected_evidence='[
  {"kind":"memory_version","memory_id":"traversal-memory-a","version":7},
  {"kind":"memory_version","memory_id":"traversal-memory-b","version":8},
  {"kind":"session_record","session_record_id":"traversal-session"}
]'

readonly neighbor_identity_projection_sql="(
  SELECT COALESCE(
    pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'relation_type', item.value#>>'{assertion,relation_type}',
        'orientation', item.value->>'orientation',
        'neighbor_id', item.value#>>'{neighbor,id}',
        'assertion_id', item.value#>>'{assertion,id}'
      )
      ORDER BY item.ordinality
    ),
    '[]'::jsonb
  )
  FROM pg_catalog.jsonb_array_elements(payload->'result')
    WITH ORDINALITY AS item(value, ordinality)
)"

readonly neighbor_relation_projection_sql="(
  SELECT COALESCE(
    pg_catalog.jsonb_agg(item.value#>>'{assertion,relation_type}' ORDER BY item.ordinality),
    '[]'::jsonb
  )
  FROM pg_catalog.jsonb_array_elements(payload->'result')
    WITH ORDINALITY AS item(value, ordinality)
)"

readonly neighbor_orientation_projection_sql="(
  SELECT COALESCE(
    pg_catalog.jsonb_agg(item.value->>'orientation' ORDER BY item.ordinality),
    '[]'::jsonb
  )
  FROM pg_catalog.jsonb_array_elements(payload->'result')
    WITH ORDINALITY AS item(value, ordinality)
)"

readonly neighbor_id_projection_sql="(
  SELECT COALESCE(
    pg_catalog.jsonb_agg(item.value#>>'{neighbor,id}' ORDER BY item.ordinality),
    '[]'::jsonb
  )
  FROM pg_catalog.jsonb_array_elements(payload->'result')
    WITH ORDINALITY AS item(value, ordinality)
)"

readonly neighbor_assertion_id_projection_sql="(
  SELECT COALESCE(
    pg_catalog.jsonb_agg(item.value#>>'{assertion,id}' ORDER BY item.ordinality),
    '[]'::jsonb
  )
  FROM pg_catalog.jsonb_array_elements(payload->'result')
    WITH ORDINALITY AS item(value, ordinality)
)"

readonly related_source_projection_sql="(
  SELECT COALESCE(
    pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'source', item.value->'source',
        'role', item.value->>'role'
      )
      ORDER BY item.ordinality
    ),
    '[]'::jsonb
  )
  FROM pg_catalog.jsonb_array_elements(payload->'result')
    WITH ORDINALITY AS item(value, ordinality)
)"

readonly path_identity_projection_sql="(
  SELECT COALESCE(
    pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'concepts', path.value->'concepts',
        'assertion_ids', (
          SELECT COALESCE(
            pg_catalog.jsonb_agg(edge.value#>>'{assertion,id}' ORDER BY edge.ordinality),
            '[]'::jsonb
          )
          FROM pg_catalog.jsonb_array_elements(path.value->'assertions')
            WITH ORDINALITY AS edge(value, ordinality)
        ),
        'orientations', (
          SELECT COALESCE(
            pg_catalog.jsonb_agg(edge.value->>'orientation' ORDER BY edge.ordinality),
            '[]'::jsonb
          )
          FROM pg_catalog.jsonb_array_elements(path.value->'assertions')
            WITH ORDINALITY AS edge(value, ordinality)
        )
      )
      ORDER BY path.ordinality
    ),
    '[]'::jsonb
  )
  FROM pg_catalog.jsonb_array_elements(payload->'result')
    WITH ORDINALITY AS path(value, ordinality)
)"

fail() {
	printf 'traversal verification failed: %s\n' "$1" >&2
	exit 1
}

assert_true() {
	local description=$1
	local sql
	local actual

	sql="$(< /dev/stdin)"
	if ! actual="$(query_as "$application_role" "$sql" 2>/dev/null)"; then
		fail "$description query failed"
	fi
	[[ "$actual" == 't' ]] || fail "$description check failed"
}

assert_prepared_query() {
	local description=$1
	local parameter_types=$2
	local query_sql=$3
	local parameters=$4
	local predicate=$5
	local command_sql
	local actual

	command_sql="PREPARE traversal_check(${parameter_types}) AS
SELECT ${predicate}
FROM (
${query_sql}
) AS query_result(payload);
EXECUTE traversal_check(${parameters});
DEALLOCATE traversal_check;"
	if ! actual="$(query_as "$application_role" "$command_sql" 2>/dev/null)"; then
		fail "$description query failed"
	fi
	[[ "$actual" == 't' ]] || fail "$description check failed"
}

assert_neighbor_query() {
	local description=$1
	local direction=$2
	local relation_filter=$3
	local result_limit=$4
	local expected_projection=$5
	local concept_id=${6:-$neighbor_root_id}
	local parameters
	local expected_count
	local actual_count
	local count_sql
	local expected_relation_projection
	local expected_orientation_projection
	local expected_neighbor_id_projection
	local expected_assertion_id_projection
	local identity_predicate
	local projection_predicate

	parameters="'${concept_id}', '${direction}', '${relation_filter}'::jsonb, '${result_limit}'"
	expected_count="$(read_query_as "$application_role" \
		"SELECT pg_catalog.jsonb_array_length('${expected_projection}'::jsonb)")"
	count_sql="PREPARE traversal_count(${neighbor_parameter_types}) AS
SELECT pg_catalog.jsonb_array_length(payload->'result')
FROM (
${get_neighbors_sql}
) AS query_result(payload);
EXECUTE traversal_count(${parameters});
DEALLOCATE traversal_count;"
	identity_predicate="payload->>'outcome' = 'complete'
  AND (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(payload)) = 2
  AND ${neighbor_identity_projection_sql} = '${expected_projection}'::jsonb"
	projection_predicate="payload->>'outcome' = 'complete'
  AND NOT EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements(payload->'result') AS item(value)
    LEFT JOIN (
      VALUES
        ('018f0000-0000-7000-8000-000000000401'::uuid, 'related_to', '018f0000-0000-7000-8000-0000000000ff'::uuid, '018f0000-0000-7000-8000-000000000100'::uuid),
        ('018f0000-0000-7000-8000-000000000402'::uuid, 'is_a', '018f0000-0000-7000-8000-000000000100'::uuid, '018f0000-0000-7000-8000-000000000101'::uuid),
        ('018f0000-0000-7000-8000-000000000403'::uuid, 'part_of', '018f0000-0000-7000-8000-000000000102'::uuid, '018f0000-0000-7000-8000-000000000100'::uuid),
        ('018f0000-0000-7000-8000-000000000404'::uuid, 'depends_on', '018f0000-0000-7000-8000-000000000100'::uuid, '018f0000-0000-7000-8000-000000000103'::uuid),
        ('018f0000-0000-7000-8000-000000000405'::uuid, 'uses', '018f0000-0000-7000-8000-000000000104'::uuid, '018f0000-0000-7000-8000-000000000100'::uuid),
        ('018f0000-0000-7000-8000-000000000406'::uuid, 'implements', '018f0000-0000-7000-8000-000000000100'::uuid, '018f0000-0000-7000-8000-000000000105'::uuid),
        ('018f0000-0000-7000-8000-000000000407'::uuid, 'causes', '018f0000-0000-7000-8000-000000000106'::uuid, '018f0000-0000-7000-8000-000000000100'::uuid),
        ('018f0000-0000-7000-8000-000000000408'::uuid, 'resolves', '018f0000-0000-7000-8000-000000000100'::uuid, '018f0000-0000-7000-8000-000000000107'::uuid),
        ('018f0000-0000-7000-8000-000000000409'::uuid, 'contradicts', '018f0000-0000-7000-8000-000000000100'::uuid, '018f0000-0000-7000-8000-000000000108'::uuid)
    ) AS expected(assertion_id, relation_code, subject_id, object_id)
      ON expected.assertion_id = (item.value#>>'{assertion,id}')::uuid
    WHERE expected.assertion_id IS NULL
      OR item.value#>>'{assertion,relation_type}' <> expected.relation_code
      OR item.value#>>'{assertion,subject_concept_id}' <> expected.subject_id::text
      OR item.value#>>'{assertion,object_concept_id}' <> expected.object_id::text
      OR item.value#>>'{assertion,revision}' <> '1'
      OR item.value#>>'{neighbor,id}' <> CASE
        WHEN expected.subject_id = '${concept_id}'::uuid THEN expected.object_id::text
        ELSE expected.subject_id::text
      END
      OR item.value#>>'{neighbor,revision}' <> '1'
  )
  AND NOT EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements(payload->'result') AS item(value)
    WHERE (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(item.value)) <> 3
      OR (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(item.value->'neighbor')) <> 3
      OR (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(item.value->'assertion')) <> 6
      OR item.value#>'{assertion,evidence}' <> '${expected_evidence}'::jsonb
      OR pg_catalog.jsonb_array_length(item.value#>'{neighbor,aliases}') = 0
      OR EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements(item.value#>'{neighbor,aliases}') AS alias(value)
        WHERE (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(alias.value)) <> 2
          OR alias.value->'preferred' <> 'true'::jsonb
      )
  )"
	if ! actual_count="$(query_as "$application_role" "$count_sql" 2>/dev/null)"; then
		fail "$description result count query failed"
	fi
	[[ "$actual_count" == "$expected_count" ]] ||
		fail "$description result count mismatch ($actual_count)"
	assert_prepared_query \
		"$description complete outcome" \
		"${neighbor_parameter_types}" \
		"$get_neighbors_sql" \
		"$parameters" \
		"payload->>'outcome' = 'complete'"
	assert_prepared_query \
		"$description complete payload shape" \
		"${neighbor_parameter_types}" \
		"$get_neighbors_sql" \
		"$parameters" \
		"(SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(payload)) = 2"
	expected_relation_projection="(
  SELECT COALESCE(pg_catalog.jsonb_agg(item.value#>>'{relation_type}' ORDER BY item.ordinality), '[]'::jsonb)
  FROM pg_catalog.jsonb_array_elements('${expected_projection}'::jsonb)
    WITH ORDINALITY AS item(value, ordinality)
)"
	expected_orientation_projection="(
  SELECT COALESCE(pg_catalog.jsonb_agg(item.value#>>'{orientation}' ORDER BY item.ordinality), '[]'::jsonb)
  FROM pg_catalog.jsonb_array_elements('${expected_projection}'::jsonb)
    WITH ORDINALITY AS item(value, ordinality)
)"
	expected_neighbor_id_projection="(
  SELECT COALESCE(pg_catalog.jsonb_agg(item.value#>>'{neighbor_id}' ORDER BY item.ordinality), '[]'::jsonb)
  FROM pg_catalog.jsonb_array_elements('${expected_projection}'::jsonb)
    WITH ORDINALITY AS item(value, ordinality)
)"
	expected_assertion_id_projection="(
  SELECT COALESCE(pg_catalog.jsonb_agg(item.value#>>'{assertion_id}' ORDER BY item.ordinality), '[]'::jsonb)
  FROM pg_catalog.jsonb_array_elements('${expected_projection}'::jsonb)
    WITH ORDINALITY AS item(value, ordinality)
)"
	assert_prepared_query \
		"$description relation order" \
		"${neighbor_parameter_types}" \
		"$get_neighbors_sql" \
		"$parameters" \
		"payload->>'outcome' = 'complete' AND ${neighbor_relation_projection_sql} = ${expected_relation_projection}"
	assert_prepared_query \
		"$description orientation order" \
		"${neighbor_parameter_types}" \
		"$get_neighbors_sql" \
		"$parameters" \
		"payload->>'outcome' = 'complete' AND ${neighbor_orientation_projection_sql} = ${expected_orientation_projection}"
	assert_prepared_query \
		"$description neighbor UUID order" \
		"${neighbor_parameter_types}" \
		"$get_neighbors_sql" \
		"$parameters" \
		"payload->>'outcome' = 'complete' AND ${neighbor_id_projection_sql} = ${expected_neighbor_id_projection}"
	assert_prepared_query \
		"$description assertion UUID order" \
		"${neighbor_parameter_types}" \
		"$get_neighbors_sql" \
		"$parameters" \
		"payload->>'outcome' = 'complete' AND ${neighbor_assertion_id_projection_sql} = ${expected_assertion_id_projection}"
	assert_prepared_query \
		"$description identity and deterministic order" \
		"${neighbor_parameter_types}" \
		"$get_neighbors_sql" \
		"$parameters" \
		"$identity_predicate"
	assert_prepared_query \
		"$description assertion and evidence projection" \
		"${neighbor_parameter_types}" \
		"$get_neighbors_sql" \
		"$parameters" \
		"$projection_predicate"
}

assert_related_source_query() {
	local description=$1
	local concept_id=$2
	local result_limit=$3
	local expected_result=$4
	local parameters
	local predicate

	parameters="'${concept_id}', '${result_limit}'"
	predicate="payload->>'outcome' = 'complete'
  AND (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(payload)) = 2
  AND ${related_source_projection_sql} = '${expected_result}'::jsonb
  AND NOT EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements(payload->'result') AS item(value)
    WHERE (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(item.value)) <> 2
      OR (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(item.value->'source')) NOT IN (2, 3)
  )"
	assert_prepared_query \
		"$description" \
		'text, text' \
		"$get_related_sources_sql" \
		"$parameters" \
		"$predicate"
}

assert_path_query() {
	local description=$1
	local from_id=$2
	local to_id=$3
	local direction=$4
	local max_depth=$5
	local max_work=$6
	local result_limit=$7
	local expected_result=$8
	local parameters
	local predicate

	parameters="'${from_id}', '${to_id}', '${direction}', '${max_depth}', '${max_work}', '${result_limit}'"
	predicate="payload->>'outcome' = 'complete'
  AND (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(payload)) = 2
  AND ${path_identity_projection_sql} = '${expected_result}'::jsonb
  AND NOT EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements(payload->'result') AS path(value)
    WHERE (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(path.value)) <> 2
      OR pg_catalog.jsonb_array_length(path.value->'assertions') + 1
        <> pg_catalog.jsonb_array_length(path.value->'concepts')
      OR EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements(path.value->'assertions') AS edge(value)
        WHERE (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(edge.value)) <> 2
          OR (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(edge.value->'assertion')) <> 6
          OR edge.value#>'{assertion,evidence}' <> '${expected_evidence}'::jsonb
      )
  )
  AND NOT EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements(payload->'result') AS path(value)
    CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(path.value->'assertions') AS edge(value)
    LEFT JOIN (
      VALUES
        ('018f0000-0000-7000-8000-000000000321'::uuid, 'uses', '018f0000-0000-7000-8000-000000000200'::uuid, '018f0000-0000-7000-8000-000000000202'::uuid),
        ('018f0000-0000-7000-8000-000000000322'::uuid, 'causes', '018f0000-0000-7000-8000-000000000202'::uuid, '018f0000-0000-7000-8000-000000000204'::uuid),
        ('018f0000-0000-7000-8000-000000000323'::uuid, 'related_to', '018f0000-0000-7000-8000-000000000199'::uuid, '018f0000-0000-7000-8000-000000000200'::uuid),
        ('018f0000-0000-7000-8000-000000000324'::uuid, 'resolves', '018f0000-0000-7000-8000-000000000199'::uuid, '018f0000-0000-7000-8000-000000000204'::uuid),
        ('018f0000-0000-7000-8000-000000000331'::uuid, 'is_a', '018f0000-0000-7000-8000-000000000200'::uuid, '018f0000-0000-7000-8000-000000000201'::uuid),
        ('018f0000-0000-7000-8000-000000000332'::uuid, 'part_of', '018f0000-0000-7000-8000-000000000201'::uuid, '018f0000-0000-7000-8000-000000000204'::uuid),
        ('018f0000-0000-7000-8000-000000000333'::uuid, 'depends_on', '018f0000-0000-7000-8000-000000000202'::uuid, '018f0000-0000-7000-8000-000000000200'::uuid),
        ('018f0000-0000-7000-8000-000000000335'::uuid, 'implements', '018f0000-0000-7000-8000-000000000201'::uuid, '018f0000-0000-7000-8000-000000000202'::uuid)
    ) AS expected(assertion_id, relation_code, subject_id, object_id)
      ON expected.assertion_id = (edge.value#>>'{assertion,id}')::uuid
    WHERE expected.assertion_id IS NULL
      OR edge.value#>>'{assertion,relation_type}' <> expected.relation_code
      OR edge.value#>>'{assertion,subject_concept_id}' <> expected.subject_id::text
      OR edge.value#>>'{assertion,object_concept_id}' <> expected.object_id::text
      OR edge.value#>>'{assertion,revision}' <> '1'
  )
  AND NOT EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements(payload->'result')
      WITH ORDINALITY AS path(value, path_ordinality)
    CROSS JOIN LATERAL pg_catalog.jsonb_array_elements_text(path.value->'concepts') AS concept(id)
    GROUP BY path.path_ordinality
    HAVING pg_catalog.count(*) <> pg_catalog.count(DISTINCT concept.id)
  )"
	assert_prepared_query \
		"$description" \
		'text, text, text, text, text, text' \
		"$get_paths_sql" \
		"$parameters" \
		"$predicate"
}

query_as() {
	local role=$1
	local sql=$2

	PGSERVICEFILE=/dev/null PGPASSFILE=/dev/null \
		"$psql" \
			--no-psqlrc \
			--no-password \
			--host="$socket_dir" \
			--port="$PGPORT" \
			--username="$role" \
			--dbname="$database" \
			--set=ON_ERROR_STOP=1 \
			--tuples-only \
			--no-align \
			--quiet \
			--command="$sql"
}

read_query_as() {
	local output

	if ! output="$(query_as "$1" "$2" 2>/dev/null)"; then
		fail 'fixture database query failed'
	fi
	printf '%s' "$output"
}

apply_migration() {
	{
		printf 'BEGIN;\nSET ROLE %s;\n' "$migration_role"
		printf "DO \$migration_owner\$ BEGIN IF current_user <> '%s' THEN RAISE EXCEPTION 'migration role assertion failed'; END IF; END \$migration_owner\$;\n" "$migration_role"
		cat "$schema_migration"
		printf '\nCOMMIT;\n'
	} | PGSERVICEFILE=/dev/null PGPASSFILE=/dev/null \
		"$psql" \
			--no-psqlrc \
			--no-password \
			--host="$socket_dir" \
			--port="$PGPORT" \
			--username="$bootstrap_role" \
			--dbname="$database" \
			--set=ON_ERROR_STOP=1 \
			--quiet \
			--file=- >/dev/null 2>&1
}

assert_schema_absent() {
	local description=$1
	local actual

	actual="$(read_query_as "$application_role" \
		"SELECT (NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'knowledge_graph'))::text")"
	[[ "$actual" == 'true' ]] || fail "$description check failed"
}

timeout_job_running() {
	local process_id=$1
	local running_jobs

	running_jobs="$(jobs -pr)"
	case $'\n'"$running_jobs"$'\n' in
	*$'\n'"$process_id"$'\n'*) return 0 ;;
	esac
	return 1
}

wait_for_timeout_lock_owner() {
	local attempt
	local observed
	local sql

	sql="
		SELECT pg_catalog.count(*) = 1
		FROM pg_catalog.pg_locks AS locks
		JOIN pg_catalog.pg_class AS relation ON relation.oid = locks.relation
		JOIN pg_catalog.pg_namespace AS schema ON schema.oid = relation.relnamespace
		WHERE schema.nspname = 'knowledge_graph'
		  AND relation.relname = 'graph_assertions'
		  AND locks.locktype = 'relation'
		  AND locks.mode = 'AccessExclusiveLock'
		  AND locks.granted
	"

	for ((attempt = 0; attempt < 200; attempt++)); do
		observed="$(read_query_as "$application_role" "$sql")"
		if [[ "$observed" == 't' ]]; then
			return
		fi
		if ! timeout_job_running "$timeout_owner_pid"; then
			fail 'statement-timeout lock owner exited before acquiring its fixture lock'
		fi
		sleep 0.05
	done
	fail 'bounded wait expired before the statement-timeout fixture lock was acquired'
}

wait_for_timeout_query_waiter() {
	local attempt
	local observed
	local sql

	sql="
		SELECT pg_catalog.count(*) = 1
		FROM pg_catalog.pg_stat_activity AS activity
		WHERE activity.application_name = 'kg-traversal-timeout-query'
		  AND activity.state = 'active'
		  AND activity.wait_event_type = 'Lock'
		  AND activity.wait_event = 'relation'
	"

	for ((attempt = 0; attempt < 200; attempt++)); do
		observed="$(read_query_as "$application_role" "$sql")"
		if [[ "$observed" == 't' ]]; then
			return
		fi
		if ! timeout_job_running "$timeout_query_pid"; then
			fail 'traversal query exited before entering the fixture lock wait'
		fi
		sleep 0.05
	done
	fail 'bounded wait expired before the traversal query entered its relation-lock wait'
}

release_timeout_owner() {
	if [[ "$timeout_fifo_open" == true ]]; then
		if ! printf 'COMMIT;\n' >&"$timeout_fifo_fd"; then
			fail 'statement-timeout fixture lock release failed'
		fi
		if ! exec {timeout_fifo_fd}>&-; then
			fail 'statement-timeout fixture channel close failed'
		fi
		timeout_fifo_open=false
	fi

	if [[ -n "$timeout_owner_pid" ]]; then
		if ! wait "$timeout_owner_pid"; then
			fail 'statement-timeout fixture lock owner failed'
		fi
		timeout_owner_pid=''
		[[ ! -s "$timeout_owner_stdout" ]] || fail 'statement-timeout lock owner emitted unexpected output'
		[[ ! -s "$timeout_owner_stderr" ]] || fail 'statement-timeout lock owner emitted a database diagnostic'
	fi
}

run_timeout_verification() {
	local timeout_sql
	local timeout_error
	local -a psql_args
	local query_status

	timeout_dir="$(mktemp -d /tmp/kgt.XXXXXX)" || fail 'statement-timeout workspace creation failed'
	timeout_fifo="$timeout_dir/owner.fifo"
	timeout_owner_stdout="$timeout_dir/owner.out"
	timeout_owner_stderr="$timeout_dir/owner.err"
	timeout_query_stdout="$timeout_dir/query.out"
	timeout_query_stderr="$timeout_dir/query.err"
	local timeout_passfile="$timeout_dir/pgpass"
	if ! (umask 077; : >"$timeout_passfile"); then
		fail 'statement-timeout password file creation failed'
	fi
	if ! mkfifo "$timeout_fifo"; then
		fail 'statement-timeout lock channel creation failed'
	fi

	psql_args=(
		--no-psqlrc
		--no-password
		--host="$socket_dir"
		--port="$PGPORT"
		--username="$bootstrap_role"
		--dbname="$database"
		--set=ON_ERROR_STOP=1
		--tuples-only
		--no-align
		--quiet
	)
	PGAPPNAME=kg-traversal-timeout-lock PGSERVICEFILE=/dev/null PGPASSFILE="$timeout_passfile" \
		"$psql" "${psql_args[@]}" \
			<"$timeout_fifo" \
			>"$timeout_owner_stdout" \
			2>"$timeout_owner_stderr" &
	timeout_owner_pid=$!
	exec {timeout_fifo_fd}>"$timeout_fifo"
	timeout_fifo_open=true
	if ! printf 'SET ROLE %s;\nBEGIN;\nLOCK TABLE knowledge_graph.graph_assertions IN ACCESS EXCLUSIVE MODE;\n' \
		"$migration_role" >&"$timeout_fifo_fd"; then
		fail 'statement-timeout fixture lock setup failed'
	fi
	wait_for_timeout_lock_owner

	timeout_sql="PREPARE traversal_timeout(text, text, text, text, text, text) AS
SELECT payload->>'outcome' = 'complete'
FROM (
${get_paths_sql}
) AS query_result(payload);
EXECUTE traversal_timeout('${path_start_id}', '${path_target_id}', 'outgoing', '3', '10000', '50');
DEALLOCATE traversal_timeout;"
	psql_args=(
		--no-psqlrc
		--no-password
		--host="$socket_dir"
		--port="$PGPORT"
		--username="$application_role"
		--dbname="$database"
		--set=ON_ERROR_STOP=1
		--tuples-only
		--no-align
		--quiet
	)
	PGAPPNAME=kg-traversal-timeout-query PGSERVICEFILE=/dev/null PGPASSFILE="$timeout_passfile" \
		"$psql" "${psql_args[@]}" \
			--command="$timeout_sql" \
			>"$timeout_query_stdout" \
			2>"$timeout_query_stderr" &
	timeout_query_pid=$!
	wait_for_timeout_query_waiter

	if wait "$timeout_query_pid"; then
		timeout_query_pid=''
		fail 'statement-timeout traversal query unexpectedly completed'
	else
		query_status=$?
		timeout_query_pid=''
	fi
	[[ "$query_status" -ne 0 ]] || fail 'statement-timeout traversal query returned success'
	[[ ! -s "$timeout_query_stdout" ]] || fail 'statement-timeout traversal query returned a partial payload'
	timeout_error="$(<"$timeout_query_stderr")"
	case "$timeout_error" in
	*'canceling statement due to statement timeout'*) ;;
	*) fail 'traversal query did not fail with the configured statement timeout' ;;
	esac

	release_timeout_owner
	if ! rm -rf -- "$timeout_dir"; then
		fail 'statement-timeout workspace cleanup failed'
	fi
	timeout_dir=''
}

cleanup_timeout() {
	local cleanup_failed=false
	local process_id

	if [[ "$timeout_fifo_open" == true ]]; then
		printf 'COMMIT;\n' >&"$timeout_fifo_fd" >/dev/null 2>&1 || cleanup_failed=true
		exec {timeout_fifo_fd}>&- >/dev/null 2>&1 || cleanup_failed=true
		timeout_fifo_open=false
	fi
	for process_id in "$timeout_query_pid" "$timeout_owner_pid"; do
		if [[ -n "$process_id" ]]; then
			kill "$process_id" >/dev/null 2>&1 || true
			wait "$process_id" >/dev/null 2>&1 || true
		fi
	done
	timeout_query_pid=''
	timeout_owner_pid=''
	if [[ -n "$timeout_dir" && -d "$timeout_dir" ]] && ! rm -rf -- "$timeout_dir"; then
		cleanup_failed=true
	fi
	timeout_dir=''
	[[ "$cleanup_failed" == false ]]
}

cleanup() {
	local status=$?
	local cleanup_failed=false

	trap - EXIT
	if ! cleanup_timeout; then
		printf 'traversal verification cleanup failed: statement-timeout sessions could not be cleaned up\n' >&2
		cleanup_failed=true
	fi
	if [[ "$schema_applied" == true ]]; then
		if bash "$rollback_script" >/dev/null 2>&1; then
			schema_applied=false
		else
			printf 'traversal verification cleanup failed: graph schema rollback failed\n' >&2
			cleanup_failed=true
		fi
	fi
	if [[ "$status" -eq 0 && "$cleanup_failed" == true ]]; then
		status=1
	fi
	exit "$status"
}

trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

[[ -x "$psql" ]] || { printf 'missing PostgreSQL executable: psql\n' >&2; exit 66; }
[[ -r "$schema_migration" ]] || { printf 'missing graph migration\n' >&2; exit 66; }
[[ -r "$rollback_script" ]] || { printf 'missing graph rollback fixture\n' >&2; exit 66; }

[[ "$bootstrap_role" == 'knowledge_graph_fixture_bootstrap' ]] || fail 'fixture bootstrap role mismatch'
[[ "$migration_role" == 'knowledge_graph_migration_owner' ]] || fail 'migration owner role mismatch'
[[ "$application_role" == 'knowledge_graph_application' ]] || fail 'application role mismatch'
[[ "$KNOWLEDGE_GRAPH_ADAPTER_QUERY_TIMEOUT_SECONDS" == '10' ]] || fail 'adapter query timeout mismatch'
[[ "$KNOWLEDGE_GRAPH_POSTGRES_STATEMENT_TIMEOUT_SECONDS" == "$statement_timeout_seconds" ]] || fail 'PostgreSQL statement timeout mismatch'
[[ "$KNOWLEDGE_GRAPH_III_INVOCATION_TIMEOUT_SECONDS" == '15' ]] || fail 'iii invocation timeout mismatch'
if [[ "$PGHOST" != "$socket_dir" || "$PGPORT" != '5432' ||
	"$PGDATABASE" != "$database" || "$PGUSER" != "$application_role" ]]; then
	fail 'fixture PostgreSQL environment mismatch'
fi

server_version="$(read_query_as "$application_role" 'SHOW server_version')"
case "$server_version" in
"$expected_major".*) ;;
*) fail 'PostgreSQL server major did not match the requested major' ;;
esac
[[ "$(read_query_as "$application_role" 'SHOW statement_timeout')" == "${statement_timeout_seconds}s" ]] ||
	fail 'application statement timeout mismatch'
[[ "$(read_query_as "$application_role" 'SELECT current_user')" == "$application_role" ]] ||
	fail 'application role connection mismatch'
assert_schema_absent 'graph schema absence before traversal fixture'

if ! apply_migration; then
	assert_schema_absent 'graph schema absence after failed migration'
	fail 'graph migration application failed'
fi
schema_applied=true

readonly expected_neighbors_either='[{"relation_type":"causes","orientation":"incoming","neighbor_id":"018f0000-0000-7000-8000-000000000106","assertion_id":"018f0000-0000-7000-8000-000000000407"},{"relation_type":"contradicts","orientation":"symmetric","neighbor_id":"018f0000-0000-7000-8000-000000000108","assertion_id":"018f0000-0000-7000-8000-000000000409"},{"relation_type":"depends_on","orientation":"outgoing","neighbor_id":"018f0000-0000-7000-8000-000000000103","assertion_id":"018f0000-0000-7000-8000-000000000404"},{"relation_type":"implements","orientation":"outgoing","neighbor_id":"018f0000-0000-7000-8000-000000000105","assertion_id":"018f0000-0000-7000-8000-000000000406"},{"relation_type":"is_a","orientation":"outgoing","neighbor_id":"018f0000-0000-7000-8000-000000000101","assertion_id":"018f0000-0000-7000-8000-000000000402"},{"relation_type":"part_of","orientation":"incoming","neighbor_id":"018f0000-0000-7000-8000-000000000102","assertion_id":"018f0000-0000-7000-8000-000000000403"},{"relation_type":"related_to","orientation":"symmetric","neighbor_id":"018f0000-0000-7000-8000-0000000000ff","assertion_id":"018f0000-0000-7000-8000-000000000401"},{"relation_type":"resolves","orientation":"outgoing","neighbor_id":"018f0000-0000-7000-8000-000000000107","assertion_id":"018f0000-0000-7000-8000-000000000408"},{"relation_type":"uses","orientation":"incoming","neighbor_id":"018f0000-0000-7000-8000-000000000104","assertion_id":"018f0000-0000-7000-8000-000000000405"}]'
readonly expected_neighbors_outgoing='[{"relation_type":"contradicts","orientation":"symmetric","neighbor_id":"018f0000-0000-7000-8000-000000000108","assertion_id":"018f0000-0000-7000-8000-000000000409"},{"relation_type":"depends_on","orientation":"outgoing","neighbor_id":"018f0000-0000-7000-8000-000000000103","assertion_id":"018f0000-0000-7000-8000-000000000404"},{"relation_type":"implements","orientation":"outgoing","neighbor_id":"018f0000-0000-7000-8000-000000000105","assertion_id":"018f0000-0000-7000-8000-000000000406"},{"relation_type":"is_a","orientation":"outgoing","neighbor_id":"018f0000-0000-7000-8000-000000000101","assertion_id":"018f0000-0000-7000-8000-000000000402"},{"relation_type":"related_to","orientation":"symmetric","neighbor_id":"018f0000-0000-7000-8000-0000000000ff","assertion_id":"018f0000-0000-7000-8000-000000000401"},{"relation_type":"resolves","orientation":"outgoing","neighbor_id":"018f0000-0000-7000-8000-000000000107","assertion_id":"018f0000-0000-7000-8000-000000000408"}]'
readonly expected_neighbors_incoming='[{"relation_type":"causes","orientation":"incoming","neighbor_id":"018f0000-0000-7000-8000-000000000106","assertion_id":"018f0000-0000-7000-8000-000000000407"},{"relation_type":"contradicts","orientation":"symmetric","neighbor_id":"018f0000-0000-7000-8000-000000000108","assertion_id":"018f0000-0000-7000-8000-000000000409"},{"relation_type":"part_of","orientation":"incoming","neighbor_id":"018f0000-0000-7000-8000-000000000102","assertion_id":"018f0000-0000-7000-8000-000000000403"},{"relation_type":"related_to","orientation":"symmetric","neighbor_id":"018f0000-0000-7000-8000-0000000000ff","assertion_id":"018f0000-0000-7000-8000-000000000401"},{"relation_type":"uses","orientation":"incoming","neighbor_id":"018f0000-0000-7000-8000-000000000104","assertion_id":"018f0000-0000-7000-8000-000000000405"}]'
readonly expected_neighbors_outgoing_filter='[{"relation_type":"contradicts","orientation":"symmetric","neighbor_id":"018f0000-0000-7000-8000-000000000108","assertion_id":"018f0000-0000-7000-8000-000000000409"},{"relation_type":"is_a","orientation":"outgoing","neighbor_id":"018f0000-0000-7000-8000-000000000101","assertion_id":"018f0000-0000-7000-8000-000000000402"}]'
readonly expected_neighbors_incoming_filter='[{"relation_type":"related_to","orientation":"symmetric","neighbor_id":"018f0000-0000-7000-8000-0000000000ff","assertion_id":"018f0000-0000-7000-8000-000000000401"},{"relation_type":"uses","orientation":"incoming","neighbor_id":"018f0000-0000-7000-8000-000000000104","assertion_id":"018f0000-0000-7000-8000-000000000405"}]'
readonly expected_neighbors_limit='[{"relation_type":"causes","orientation":"incoming","neighbor_id":"018f0000-0000-7000-8000-000000000106","assertion_id":"018f0000-0000-7000-8000-000000000407"},{"relation_type":"contradicts","orientation":"symmetric","neighbor_id":"018f0000-0000-7000-8000-000000000108","assertion_id":"018f0000-0000-7000-8000-000000000409"},{"relation_type":"depends_on","orientation":"outgoing","neighbor_id":"018f0000-0000-7000-8000-000000000103","assertion_id":"018f0000-0000-7000-8000-000000000404"}]'
readonly expected_related_to_peer_neighbor='[{"relation_type":"related_to","orientation":"symmetric","neighbor_id":"018f0000-0000-7000-8000-000000000100","assertion_id":"018f0000-0000-7000-8000-000000000401"}]'
readonly expected_contradicts_peer_neighbor='[{"relation_type":"contradicts","orientation":"symmetric","neighbor_id":"018f0000-0000-7000-8000-000000000100","assertion_id":"018f0000-0000-7000-8000-000000000409"}]'
readonly expected_empty='[]'
readonly expected_related_sources='[{"source":{"kind":"memory_version","memory_id":"traversal-memory-a","version":7},"role":"mention"},{"source":{"kind":"memory_version","memory_id":"traversal-memory-b","version":8},"role":"mention"},{"source":{"kind":"session_record","session_record_id":"traversal-session"},"role":"mention"},{"source":{"kind":"memory_version","memory_id":"traversal-memory-a","version":7},"role":"assertion_evidence"},{"source":{"kind":"memory_version","memory_id":"traversal-memory-b","version":8},"role":"assertion_evidence"},{"source":{"kind":"session_record","session_record_id":"traversal-session"},"role":"assertion_evidence"}]'
readonly expected_related_sources_limit='[{"source":{"kind":"memory_version","memory_id":"traversal-memory-a","version":7},"role":"mention"},{"source":{"kind":"memory_version","memory_id":"traversal-memory-b","version":8},"role":"mention"}]'
readonly expected_paths_outgoing='[{"concepts":["018f0000-0000-7000-8000-000000000200","018f0000-0000-7000-8000-000000000202","018f0000-0000-7000-8000-000000000204"],"assertion_ids":["018f0000-0000-7000-8000-000000000321","018f0000-0000-7000-8000-000000000322"],"orientations":["outgoing","outgoing"]},{"concepts":["018f0000-0000-7000-8000-000000000200","018f0000-0000-7000-8000-000000000199","018f0000-0000-7000-8000-000000000204"],"assertion_ids":["018f0000-0000-7000-8000-000000000323","018f0000-0000-7000-8000-000000000324"],"orientations":["symmetric","outgoing"]},{"concepts":["018f0000-0000-7000-8000-000000000200","018f0000-0000-7000-8000-000000000201","018f0000-0000-7000-8000-000000000204"],"assertion_ids":["018f0000-0000-7000-8000-000000000331","018f0000-0000-7000-8000-000000000332"],"orientations":["outgoing","outgoing"]},{"concepts":["018f0000-0000-7000-8000-000000000200","018f0000-0000-7000-8000-000000000201","018f0000-0000-7000-8000-000000000202","018f0000-0000-7000-8000-000000000204"],"assertion_ids":["018f0000-0000-7000-8000-000000000331","018f0000-0000-7000-8000-000000000335","018f0000-0000-7000-8000-000000000322"],"orientations":["outgoing","outgoing","outgoing"]}]'
readonly expected_paths_outgoing_depth_two='[{"concepts":["018f0000-0000-7000-8000-000000000200","018f0000-0000-7000-8000-000000000202","018f0000-0000-7000-8000-000000000204"],"assertion_ids":["018f0000-0000-7000-8000-000000000321","018f0000-0000-7000-8000-000000000322"],"orientations":["outgoing","outgoing"]},{"concepts":["018f0000-0000-7000-8000-000000000200","018f0000-0000-7000-8000-000000000199","018f0000-0000-7000-8000-000000000204"],"assertion_ids":["018f0000-0000-7000-8000-000000000323","018f0000-0000-7000-8000-000000000324"],"orientations":["symmetric","outgoing"]},{"concepts":["018f0000-0000-7000-8000-000000000200","018f0000-0000-7000-8000-000000000201","018f0000-0000-7000-8000-000000000204"],"assertion_ids":["018f0000-0000-7000-8000-000000000331","018f0000-0000-7000-8000-000000000332"],"orientations":["outgoing","outgoing"]}]'
readonly expected_paths_incoming='[{"concepts":["018f0000-0000-7000-8000-000000000204","018f0000-0000-7000-8000-000000000202","018f0000-0000-7000-8000-000000000200"],"assertion_ids":["018f0000-0000-7000-8000-000000000322","018f0000-0000-7000-8000-000000000321"],"orientations":["incoming","incoming"]},{"concepts":["018f0000-0000-7000-8000-000000000204","018f0000-0000-7000-8000-000000000199","018f0000-0000-7000-8000-000000000200"],"assertion_ids":["018f0000-0000-7000-8000-000000000324","018f0000-0000-7000-8000-000000000323"],"orientations":["incoming","symmetric"]},{"concepts":["018f0000-0000-7000-8000-000000000204","018f0000-0000-7000-8000-000000000201","018f0000-0000-7000-8000-000000000200"],"assertion_ids":["018f0000-0000-7000-8000-000000000332","018f0000-0000-7000-8000-000000000331"],"orientations":["incoming","incoming"]},{"concepts":["018f0000-0000-7000-8000-000000000204","018f0000-0000-7000-8000-000000000202","018f0000-0000-7000-8000-000000000201","018f0000-0000-7000-8000-000000000200"],"assertion_ids":["018f0000-0000-7000-8000-000000000322","018f0000-0000-7000-8000-000000000335","018f0000-0000-7000-8000-000000000331"],"orientations":["incoming","incoming","incoming"]}]'
readonly expected_paths_either='[{"concepts":["018f0000-0000-7000-8000-000000000200","018f0000-0000-7000-8000-000000000202","018f0000-0000-7000-8000-000000000204"],"assertion_ids":["018f0000-0000-7000-8000-000000000321","018f0000-0000-7000-8000-000000000322"],"orientations":["outgoing","outgoing"]},{"concepts":["018f0000-0000-7000-8000-000000000200","018f0000-0000-7000-8000-000000000199","018f0000-0000-7000-8000-000000000204"],"assertion_ids":["018f0000-0000-7000-8000-000000000323","018f0000-0000-7000-8000-000000000324"],"orientations":["symmetric","outgoing"]},{"concepts":["018f0000-0000-7000-8000-000000000200","018f0000-0000-7000-8000-000000000201","018f0000-0000-7000-8000-000000000204"],"assertion_ids":["018f0000-0000-7000-8000-000000000331","018f0000-0000-7000-8000-000000000332"],"orientations":["outgoing","outgoing"]},{"concepts":["018f0000-0000-7000-8000-000000000200","018f0000-0000-7000-8000-000000000202","018f0000-0000-7000-8000-000000000204"],"assertion_ids":["018f0000-0000-7000-8000-000000000333","018f0000-0000-7000-8000-000000000322"],"orientations":["incoming","outgoing"]},{"concepts":["018f0000-0000-7000-8000-000000000200","018f0000-0000-7000-8000-000000000202","018f0000-0000-7000-8000-000000000201","018f0000-0000-7000-8000-000000000204"],"assertion_ids":["018f0000-0000-7000-8000-000000000321","018f0000-0000-7000-8000-000000000335","018f0000-0000-7000-8000-000000000332"],"orientations":["outgoing","incoming","outgoing"]},{"concepts":["018f0000-0000-7000-8000-000000000200","018f0000-0000-7000-8000-000000000201","018f0000-0000-7000-8000-000000000202","018f0000-0000-7000-8000-000000000204"],"assertion_ids":["018f0000-0000-7000-8000-000000000331","018f0000-0000-7000-8000-000000000335","018f0000-0000-7000-8000-000000000322"],"orientations":["outgoing","outgoing","outgoing"]},{"concepts":["018f0000-0000-7000-8000-000000000200","018f0000-0000-7000-8000-000000000202","018f0000-0000-7000-8000-000000000201","018f0000-0000-7000-8000-000000000204"],"assertion_ids":["018f0000-0000-7000-8000-000000000333","018f0000-0000-7000-8000-000000000335","018f0000-0000-7000-8000-000000000332"],"orientations":["incoming","incoming","outgoing"]}]'

assert_true 'traversal source fixture creation' <<'SQL'
WITH requested(source_kind, external_id, external_version) AS (
  VALUES
    ('memory_version', 'traversal-memory-a', 7::bigint),
    ('memory_version', 'traversal-memory-b', 8::bigint),
    ('session_record', 'traversal-session', NULL::bigint)
), outcomes AS MATERIALIZED (
  SELECT knowledge_graph.graph_register_source(
    requested.source_kind,
    requested.external_id,
    requested.external_version
  ) AS result
  FROM requested
)
SELECT pg_catalog.count(*) = 3
  AND pg_catalog.bool_and(result->>'outcome' = 'created')
FROM outcomes
SQL

assert_true 'traversal concept fixture creation' <<'SQL'
WITH requested(id, alias_key, display_text) AS (
  VALUES
    ('018f0000-0000-7000-8000-000000000100'::uuid, 'traversal root', 'Traversal Root'),
    ('018f0000-0000-7000-8000-0000000000ff'::uuid, 'traversal related peer', 'Traversal Related Peer'),
    ('018f0000-0000-7000-8000-000000000101'::uuid, 'traversal is a', 'Traversal Is A'),
    ('018f0000-0000-7000-8000-000000000102'::uuid, 'traversal part of', 'Traversal Part Of'),
    ('018f0000-0000-7000-8000-000000000103'::uuid, 'traversal depends on', 'Traversal Depends On'),
    ('018f0000-0000-7000-8000-000000000104'::uuid, 'traversal uses', 'Traversal Uses'),
    ('018f0000-0000-7000-8000-000000000105'::uuid, 'traversal implements', 'Traversal Implements'),
    ('018f0000-0000-7000-8000-000000000106'::uuid, 'traversal causes', 'Traversal Causes'),
    ('018f0000-0000-7000-8000-000000000107'::uuid, 'traversal resolves', 'Traversal Resolves'),
    ('018f0000-0000-7000-8000-000000000108'::uuid, 'traversal contradicts', 'Traversal Contradicts'),
    ('018f0000-0000-7000-8000-000000000109'::uuid, 'traversal empty', 'Traversal Empty'),
    ('018f0000-0000-7000-8000-000000000199'::uuid, 'path symmetric peer', 'Path Symmetric Peer'),
    ('018f0000-0000-7000-8000-000000000200'::uuid, 'path start', 'Path Start'),
    ('018f0000-0000-7000-8000-000000000201'::uuid, 'path via a', 'Path Via A'),
    ('018f0000-0000-7000-8000-000000000202'::uuid, 'path via b', 'Path Via B'),
    ('018f0000-0000-7000-8000-000000000204'::uuid, 'path target', 'Path Target')
), outcomes AS MATERIALIZED (
  SELECT knowledge_graph.graph_create_concept(
    requested.id,
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'alias_key', requested.alias_key,
      'display_text', requested.display_text,
      'is_preferred', true
    ))
  ) AS result
  FROM requested
)
SELECT pg_catalog.count(*) = 16
  AND pg_catalog.bool_and(result->>'outcome' = 'created')
FROM outcomes
SQL

assert_true 'neighbor assertion fixture creation' <<'SQL'
WITH requested(id, subject_id, relation_code, object_id) AS (
  VALUES
    ('018f0000-0000-7000-8000-000000000401'::uuid, '018f0000-0000-7000-8000-000000000100'::uuid, 'related_to', '018f0000-0000-7000-8000-0000000000ff'::uuid),
    ('018f0000-0000-7000-8000-000000000402'::uuid, '018f0000-0000-7000-8000-000000000100'::uuid, 'is_a', '018f0000-0000-7000-8000-000000000101'::uuid),
    ('018f0000-0000-7000-8000-000000000403'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid, 'part_of', '018f0000-0000-7000-8000-000000000100'::uuid),
    ('018f0000-0000-7000-8000-000000000404'::uuid, '018f0000-0000-7000-8000-000000000100'::uuid, 'depends_on', '018f0000-0000-7000-8000-000000000103'::uuid),
    ('018f0000-0000-7000-8000-000000000405'::uuid, '018f0000-0000-7000-8000-000000000104'::uuid, 'uses', '018f0000-0000-7000-8000-000000000100'::uuid),
    ('018f0000-0000-7000-8000-000000000406'::uuid, '018f0000-0000-7000-8000-000000000100'::uuid, 'implements', '018f0000-0000-7000-8000-000000000105'::uuid),
    ('018f0000-0000-7000-8000-000000000407'::uuid, '018f0000-0000-7000-8000-000000000106'::uuid, 'causes', '018f0000-0000-7000-8000-000000000100'::uuid),
    ('018f0000-0000-7000-8000-000000000408'::uuid, '018f0000-0000-7000-8000-000000000100'::uuid, 'resolves', '018f0000-0000-7000-8000-000000000107'::uuid),
    ('018f0000-0000-7000-8000-000000000409'::uuid, '018f0000-0000-7000-8000-000000000100'::uuid, 'contradicts', '018f0000-0000-7000-8000-000000000108'::uuid)
), source AS MATERIALIZED (
  SELECT external_id, external_version
  FROM knowledge_graph.graph_source_references
  WHERE source_kind = 'memory_version' COLLATE "C"
    AND external_id = 'traversal-memory-a' COLLATE "C"
), outcomes AS MATERIALIZED (
  SELECT knowledge_graph.graph_create_assertion(
    requested.id,
    requested.subject_id,
    requested.relation_code,
    requested.object_id,
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'kind', 'memory_version',
      'memory_id', source.external_id,
      'version', source.external_version
    ))
  ) AS result
  FROM requested CROSS JOIN source
)
SELECT pg_catalog.count(*) = 9
  AND pg_catalog.bool_and(result->>'outcome' = 'created')
FROM outcomes
SQL

assert_true 'neighbor evidence fixture creation' <<'SQL'
WITH assertion_ids(id) AS MATERIALIZED (
  VALUES
    ('018f0000-0000-7000-8000-000000000401'::uuid),
    ('018f0000-0000-7000-8000-000000000402'::uuid),
    ('018f0000-0000-7000-8000-000000000403'::uuid),
    ('018f0000-0000-7000-8000-000000000404'::uuid),
    ('018f0000-0000-7000-8000-000000000405'::uuid),
    ('018f0000-0000-7000-8000-000000000406'::uuid),
    ('018f0000-0000-7000-8000-000000000407'::uuid),
    ('018f0000-0000-7000-8000-000000000408'::uuid),
    ('018f0000-0000-7000-8000-000000000409'::uuid)
), sources(source_kind, external_id, external_version) AS MATERIALIZED (
  VALUES
    ('memory_version', 'traversal-memory-b', 8::bigint),
    ('session_record', 'traversal-session', NULL::bigint)
), outcomes AS MATERIALIZED (
  SELECT knowledge_graph.graph_add_assertion_evidence(
    assertion_ids.id,
    sources.source_kind,
    sources.external_id,
    sources.external_version
  ) AS result
  FROM assertion_ids CROSS JOIN sources
)
SELECT pg_catalog.count(*) = 18
  AND pg_catalog.bool_and(result->>'outcome' = 'created')
FROM outcomes
SQL

assert_true 'related-source mention fixture creation' <<'SQL'
WITH requested(source_kind, external_id, external_version) AS (
  VALUES
    ('memory_version', 'traversal-memory-a', 7::bigint),
    ('memory_version', 'traversal-memory-b', 8::bigint),
    ('session_record', 'traversal-session', NULL::bigint)
), outcomes AS MATERIALIZED (
  SELECT knowledge_graph.graph_create_mention(
    '018f0000-0000-7000-8000-000000000100'::uuid,
    requested.source_kind,
    requested.external_id,
    requested.external_version
  ) AS result
  FROM requested
)
SELECT pg_catalog.count(*) = 3
  AND pg_catalog.bool_and(result->>'outcome' = 'created')
FROM outcomes
SQL

assert_true 'path assertion fixture creation' <<'SQL'
WITH requested(id, subject_id, relation_code, object_id) AS (
  VALUES
    ('018f0000-0000-7000-8000-000000000321'::uuid, '018f0000-0000-7000-8000-000000000200'::uuid, 'uses', '018f0000-0000-7000-8000-000000000202'::uuid),
    ('018f0000-0000-7000-8000-000000000322'::uuid, '018f0000-0000-7000-8000-000000000202'::uuid, 'causes', '018f0000-0000-7000-8000-000000000204'::uuid),
    ('018f0000-0000-7000-8000-000000000323'::uuid, '018f0000-0000-7000-8000-000000000199'::uuid, 'related_to', '018f0000-0000-7000-8000-000000000200'::uuid),
    ('018f0000-0000-7000-8000-000000000324'::uuid, '018f0000-0000-7000-8000-000000000199'::uuid, 'resolves', '018f0000-0000-7000-8000-000000000204'::uuid),
    ('018f0000-0000-7000-8000-000000000331'::uuid, '018f0000-0000-7000-8000-000000000200'::uuid, 'is_a', '018f0000-0000-7000-8000-000000000201'::uuid),
    ('018f0000-0000-7000-8000-000000000332'::uuid, '018f0000-0000-7000-8000-000000000201'::uuid, 'part_of', '018f0000-0000-7000-8000-000000000204'::uuid),
    ('018f0000-0000-7000-8000-000000000333'::uuid, '018f0000-0000-7000-8000-000000000202'::uuid, 'depends_on', '018f0000-0000-7000-8000-000000000200'::uuid),
    ('018f0000-0000-7000-8000-000000000335'::uuid, '018f0000-0000-7000-8000-000000000201'::uuid, 'implements', '018f0000-0000-7000-8000-000000000202'::uuid)
), source AS MATERIALIZED (
  SELECT external_id, external_version
  FROM knowledge_graph.graph_source_references
  WHERE source_kind = 'memory_version' COLLATE "C"
    AND external_id = 'traversal-memory-a' COLLATE "C"
), outcomes AS MATERIALIZED (
  SELECT knowledge_graph.graph_create_assertion(
    requested.id,
    requested.subject_id,
    requested.relation_code,
    requested.object_id,
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'kind', 'memory_version',
      'memory_id', source.external_id,
      'version', source.external_version
    ))
  ) AS result
  FROM requested CROSS JOIN source
)
SELECT pg_catalog.count(*) = 8
  AND pg_catalog.bool_and(result->>'outcome' = 'created')
FROM outcomes
SQL

assert_true 'path evidence fixture creation' <<'SQL'
WITH assertion_ids(id) AS MATERIALIZED (
  VALUES
    ('018f0000-0000-7000-8000-000000000321'::uuid),
    ('018f0000-0000-7000-8000-000000000322'::uuid),
    ('018f0000-0000-7000-8000-000000000323'::uuid),
    ('018f0000-0000-7000-8000-000000000324'::uuid),
    ('018f0000-0000-7000-8000-000000000331'::uuid),
    ('018f0000-0000-7000-8000-000000000332'::uuid),
    ('018f0000-0000-7000-8000-000000000333'::uuid),
    ('018f0000-0000-7000-8000-000000000335'::uuid)
), sources(source_kind, external_id, external_version) AS MATERIALIZED (
  VALUES
    ('memory_version', 'traversal-memory-b', 8::bigint),
    ('session_record', 'traversal-session', NULL::bigint)
), outcomes AS MATERIALIZED (
  SELECT knowledge_graph.graph_add_assertion_evidence(
    assertion_ids.id,
    sources.source_kind,
    sources.external_id,
    sources.external_version
  ) AS result
  FROM assertion_ids CROSS JOIN sources
)
SELECT pg_catalog.count(*) = 16
  AND pg_catalog.bool_and(result->>'outcome' = 'created')
FROM outcomes
SQL

printf 'traversal fixture=all-relations-branching-cycle-both-source-kinds loaded\n'

assert_neighbor_query \
	'neighbor either direction and all relation types' \
	either '[]' 256 "$expected_neighbors_either"
assert_neighbor_query \
	'neighbor outgoing direction with symmetric reversal' \
	outgoing '[]' 256 "$expected_neighbors_outgoing"
assert_neighbor_query \
	'neighbor incoming direction with symmetric reversal' \
	incoming '[]' 256 "$expected_neighbors_incoming"
assert_neighbor_query \
	'neighbor related_to peer reversal in outgoing mode' \
	outgoing '[]' 256 "$expected_related_to_peer_neighbor" "$related_to_neighbor_peer_id"
assert_neighbor_query \
	'neighbor related_to peer reversal in incoming mode' \
	incoming '[]' 256 "$expected_related_to_peer_neighbor" "$related_to_neighbor_peer_id"
assert_neighbor_query \
	'neighbor related_to peer reversal in either mode' \
	either '[]' 256 "$expected_related_to_peer_neighbor" "$related_to_neighbor_peer_id"
assert_neighbor_query \
	'neighbor contradicts peer reversal in outgoing mode' \
	outgoing '[]' 256 "$expected_contradicts_peer_neighbor" "$contradicts_neighbor_peer_id"
assert_neighbor_query \
	'neighbor contradicts peer reversal in incoming mode' \
	incoming '[]' 256 "$expected_contradicts_peer_neighbor" "$contradicts_neighbor_peer_id"
assert_neighbor_query \
	'neighbor contradicts peer reversal in either mode' \
	either '[]' 256 "$expected_contradicts_peer_neighbor" "$contradicts_neighbor_peer_id"
assert_neighbor_query \
	'neighbor outgoing relation filter' \
	outgoing '["is_a","uses","contradicts"]' 256 "$expected_neighbors_outgoing_filter"
assert_neighbor_query \
	'neighbor incoming relation filter' \
	incoming '["uses","related_to"]' 256 "$expected_neighbors_incoming_filter"
assert_neighbor_query \
	'neighbor deterministic result limit' \
	either '[]' 3 "$expected_neighbors_limit"
assert_neighbor_query \
	'neighbor filtered empty result' \
	outgoing '["uses"]' 256 "$expected_empty"

alias_predicate="payload->>'outcome' = 'complete'
  AND (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(payload)) = 2
  AND payload#>'{result,0,neighbor,aliases}'
    = '[{\"display_text\":\"Traversal Is A\",\"preferred\":true}]'::jsonb"
assert_prepared_query \
	'neighbor preferred alias projection' \
	"${neighbor_parameter_types}" \
	"$get_neighbors_sql" \
	"'${neighbor_root_id}', 'outgoing', '[\"is_a\"]'::jsonb, '1'" \
	"$alias_predicate"
assert_neighbor_query \
	'neighbor empty result' \
	either '[]' 256 "$expected_empty" "$empty_concept_id"

assert_related_source_query \
	'related-source mention and assertion-evidence roles' \
	"$neighbor_root_id" 256 "$expected_related_sources"
assert_related_source_query \
	'related-source deterministic result limit' \
	"$neighbor_root_id" 2 "$expected_related_sources_limit"
assert_related_source_query \
	'related-source empty result' \
	"$empty_concept_id" 256 "$expected_empty"

assert_path_query \
	'outgoing cyclic and branching paths with shortest UUID order' \
	"$path_start_id" "$path_target_id" outgoing 3 10000 50 "$expected_paths_outgoing"
assert_path_query \
	'incoming path direction and symmetric reversal' \
	"$path_target_id" "$path_start_id" incoming 3 10000 50 "$expected_paths_incoming"
assert_path_query \
	'either path direction and deterministic native-UUID order' \
	"$path_start_id" "$path_target_id" either 8 10000 50 "$expected_paths_either"
assert_path_query \
	'path depth limit' \
	"$path_start_id" "$path_target_id" outgoing 1 10000 50 "$expected_empty"
assert_path_query \
	'path result limit' \
	"$path_start_id" "$path_target_id" outgoing 3 10000 1 \
	'[{"concepts":["018f0000-0000-7000-8000-000000000200","018f0000-0000-7000-8000-000000000202","018f0000-0000-7000-8000-000000000204"],"assertion_ids":["018f0000-0000-7000-8000-000000000321","018f0000-0000-7000-8000-000000000322"],"orientations":["outgoing","outgoing"]}]'
assert_path_query \
	'path work bound exact completion boundary' \
	"$path_start_id" "$path_target_id" outgoing 2 8 50 \
	"$expected_paths_outgoing_depth_two"
assert_prepared_query \
	'path work sentinel exact exhaustion marker' \
	'text, text, text, text, text, text' \
	"$get_paths_sql" \
	"'${path_start_id}', '${path_target_id}', 'outgoing', '2', '7', '50'" \
	"payload = pg_catalog.jsonb_build_object('outcome', 'work_exhausted')"
assert_path_query \
	'path empty result' \
	"$empty_concept_id" "$path_target_id" either 8 10000 50 "$expected_empty"

printf 'traversal neighbors=directions-filters-modes-order-limits-empty evidence verified\n'
printf 'traversal related-sources=roles-deduplication-order-limit-empty verified\n'
printf 'traversal paths=cycle-branching-simple-shortest-native-uuid-depth-work-limit verified\n'

assert_true 'response-size source fixture creation' <<'SQL'
WITH requested(source_number, external_id) AS (
  SELECT source_number,
    'kg-traversal-budget-' || pg_catalog.lpad(source_number::text, 2, '0')
      || pg_catalog.repeat(
        'x',
        2048 - pg_catalog.octet_length(
          'kg-traversal-budget-' || pg_catalog.lpad(source_number::text, 2, '0')
        )
      )
  FROM pg_catalog.generate_series(1, 32) AS requested(source_number)
), outcomes AS MATERIALIZED (
  SELECT knowledge_graph.graph_register_source(
    'memory_version',
    requested.external_id,
    (30000 + requested.source_number)::bigint
  ) AS result
  FROM requested
)
SELECT pg_catalog.count(*) = 32
  AND pg_catalog.bool_and(result->>'outcome' = 'created')
FROM outcomes
SQL

assert_true 'response-size source maximum-key shape' <<'SQL'
SELECT pg_catalog.count(*) = 32
  AND pg_catalog.bool_and(pg_catalog.octet_length(external_id) = 2048)
FROM knowledge_graph.graph_source_references
WHERE source_kind = 'memory_version' COLLATE "C"
  AND external_version BETWEEN 30001 AND 30032
SQL

assert_true 'response-size concept fixture creation' <<'SQL'
WITH requested(id, alias_key, display_text) AS (
  VALUES
    ('018f0000-0000-7000-8000-000000014998'::uuid, 'traversal budget root', 'Traversal Budget Root'),
    ('018f0000-0000-7000-8000-000000014999'::uuid, 'traversal budget target', 'Traversal Budget Target')
  UNION ALL
  SELECT
    ('018f0000-0000-7000-8000-' || pg_catalog.lpad((14999 + branch_number)::text, 12, '0'))::uuid,
    'traversal budget branch ' || branch_number::text,
    'Traversal Budget Branch ' || branch_number::text
  FROM pg_catalog.generate_series(1, 65) AS requested(branch_number)
), outcomes AS MATERIALIZED (
  SELECT knowledge_graph.graph_create_concept(
    requested.id,
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'alias_key', requested.alias_key,
      'display_text', requested.display_text,
      'is_preferred', true
    ))
  ) AS result
  FROM requested
)
SELECT pg_catalog.count(*) = 67
  AND pg_catalog.bool_and(result->>'outcome' = 'created')
FROM outcomes
SQL

assert_true 'response-size assertion fixture creation' <<'SQL'
WITH marker_source AS MATERIALIZED (
  SELECT external_id, external_version
  FROM knowledge_graph.graph_source_references
  WHERE source_kind = 'memory_version' COLLATE "C"
    AND external_version = 30001
), requested(id, subject_id, object_id) AS MATERIALIZED (
  SELECT
    ('018f0000-0000-7000-8000-' || pg_catalog.lpad((16000 + 2 * (branch_number - 1))::text, 12, '0'))::uuid,
    '018f0000-0000-7000-8000-000000014998'::uuid,
    ('018f0000-0000-7000-8000-' || pg_catalog.lpad((14999 + branch_number)::text, 12, '0'))::uuid
  FROM pg_catalog.generate_series(1, 65) AS requested(branch_number)
  UNION ALL
  SELECT
    ('018f0000-0000-7000-8000-' || pg_catalog.lpad((16001 + 2 * (branch_number - 1))::text, 12, '0'))::uuid,
    ('018f0000-0000-7000-8000-' || pg_catalog.lpad((14999 + branch_number)::text, 12, '0'))::uuid,
    '018f0000-0000-7000-8000-000000014999'::uuid
  FROM pg_catalog.generate_series(1, 65) AS requested(branch_number)
), outcomes AS MATERIALIZED (
  SELECT knowledge_graph.graph_create_assertion(
    requested.id,
    requested.subject_id,
    'is_a',
    requested.object_id,
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'kind', 'memory_version',
      'memory_id', marker_source.external_id,
      'version', marker_source.external_version
    ))
  ) AS result
  FROM requested CROSS JOIN marker_source
)
SELECT pg_catalog.count(*) = 130
  AND pg_catalog.bool_and(result->>'outcome' = 'created')
FROM outcomes
SQL

assert_true 'response-size assertion evidence expansion' <<'SQL'
WITH marker_bounds AS MATERIALIZED (
  SELECT
    '018f0000-0000-7000-8000-000000014998'::uuid AS root_id,
    '018f0000-0000-7000-8000-000000014999'::uuid AS target_id,
    ('018f0000-0000-7000-8000-' || pg_catalog.lpad('15000', 12, '0'))::uuid AS first_branch_id,
    ('018f0000-0000-7000-8000-' || pg_catalog.lpad('15064', 12, '0'))::uuid AS last_branch_id
), marker_assertions AS MATERIALIZED (
  SELECT assertions.id
  FROM knowledge_graph.graph_assertions AS assertions
  CROSS JOIN marker_bounds
  WHERE (
      assertions.subject_concept_id = marker_bounds.root_id
      AND assertions.object_concept_id BETWEEN marker_bounds.first_branch_id AND marker_bounds.last_branch_id
    )
    OR (
      assertions.subject_concept_id BETWEEN marker_bounds.first_branch_id AND marker_bounds.last_branch_id
      AND assertions.object_concept_id = marker_bounds.target_id
    )
), marker_sources AS MATERIALIZED (
  SELECT source_kind, external_id, external_version
  FROM knowledge_graph.graph_source_references
  WHERE source_kind = 'memory_version' COLLATE "C"
    AND external_version BETWEEN 30001 AND 30032
), outcomes AS MATERIALIZED (
  SELECT knowledge_graph.graph_add_assertion_evidence(
    marker_assertions.id,
    marker_sources.source_kind,
    marker_sources.external_id,
    marker_sources.external_version
  ) AS result
  FROM marker_assertions CROSS JOIN marker_sources
)
SELECT pg_catalog.count(*) = 4160
  AND pg_catalog.bool_and(result->>'outcome' IN ('created', 'existing'))
FROM outcomes
SQL

assert_true 'response-size fixture evidence cardinality' <<'SQL'
SELECT pg_catalog.count(*) = 130 * 32
FROM knowledge_graph.graph_assertion_evidence AS evidence
JOIN knowledge_graph.graph_assertions AS assertions
  ON assertions.id = evidence.assertion_id
WHERE assertions.relation_code = 'is_a' COLLATE "C"
  AND (
    (assertions.subject_concept_id = '018f0000-0000-7000-8000-000000014998'::uuid
      AND assertions.object_concept_id BETWEEN
        '018f0000-0000-7000-8000-000000015000'::uuid AND
        '018f0000-0000-7000-8000-000000015064'::uuid)
    OR
    (assertions.subject_concept_id BETWEEN
        '018f0000-0000-7000-8000-000000015000'::uuid AND
        '018f0000-0000-7000-8000-000000015064'::uuid
      AND assertions.object_concept_id = '018f0000-0000-7000-8000-000000014999'::uuid)
  )
SQL

assert_prepared_query \
	'neighbor exact 4 MiB result-size marker' \
	"${neighbor_parameter_types}" \
	"$get_neighbors_sql" \
	"'018f0000-0000-7000-8000-000000014998', 'outgoing', '[]'::jsonb, '256'" \
	"payload = pg_catalog.jsonb_build_object('outcome', 'result_too_large')"
assert_prepared_query \
	'path exact 4 MiB result-size marker' \
	'text, text, text, text, text, text' \
	"$get_paths_sql" \
	"'018f0000-0000-7000-8000-000000014998', '018f0000-0000-7000-8000-000000014999', 'outgoing', '2', '10000', '50'" \
	"payload = pg_catalog.jsonb_build_object('outcome', 'result_too_large')"

printf 'traversal bounds=4-MiB-marker-neighbor-and-path complete-payload-only verified\n'

run_timeout_verification
printf 'traversal timeout=12-second-postgres-statement-cancellation-no-partial-payload verified\n'

if ! bash "$rollback_script" >/dev/null 2>&1; then
	fail 'clean graph schema rollback failed'
fi
schema_applied=false
assert_schema_absent 'graph schema absence after traversal rollback'

printf 'traversal rollback=completed graph-schema=absent\n'
printf 'traversal verification=passed major=%s\n' "$expected_major"
