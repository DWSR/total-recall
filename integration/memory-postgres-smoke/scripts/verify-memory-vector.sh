#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 0 ]]; then
	printf 'usage: verify-memory-vector.sh\n' >&2
	exit 64
fi

for variable in \
	MEMORY_POSTGRES_BIN \
	MEMORY_POSTGRES_SOCKET_DIR \
	MEMORY_POSTGRES_DATABASE \
	MEMORY_POSTGRES_MIGRATION_ROLE \
	MEMORY_POSTGRES_APPLICATION_ROLE \
	MEMORY_POSTGRES_EXPECTED_MAJOR \
	MEMORY_SCHEMA_MIGRATION \
	PGPORT; do
	if [[ -z "${!variable:-}" ]]; then
		printf 'missing fixture environment variable: %s\n' "$variable" >&2
		exit 64
	fi
done

case "$MEMORY_POSTGRES_EXPECTED_MAJOR" in
17 | 18) ;;
*)
	printf 'unsupported PostgreSQL major: %s\n' "$MEMORY_POSTGRES_EXPECTED_MAJOR" >&2
	exit 64
	;;
esac

readonly psql="$MEMORY_POSTGRES_BIN/psql"
if [[ ! -x "$psql" ]]; then
	printf 'missing PostgreSQL executable\n' >&2
	exit 66
fi
if [[ ! -r "$MEMORY_SCHEMA_MIGRATION" ]]; then
	printf 'missing memory schema migration\n' >&2
	exit 66
fi

fail() {
	printf 'memory vector verification failed: %s\n' "$1" >&2
	exit 1
}

expect_equal() {
	local actual=$1
	local expected=$2
	local description=$3

	[[ "$actual" == "$expected" ]] || fail "$description"
}

query_as() {
	local role=$1
	local target_database=$2
	local sql=$3

	"$psql" \
		--no-psqlrc \
		--no-password \
		--host="$MEMORY_POSTGRES_SOCKET_DIR" \
		--port="$PGPORT" \
		--username="$role" \
		--dbname="$target_database" \
		--set=ON_ERROR_STOP=1 \
		--tuples-only \
		--no-align \
		--quiet \
		--command="$sql" 2>/dev/null
}

application_query() {
	query_as "$MEMORY_POSTGRES_APPLICATION_ROLE" "$MEMORY_POSTGRES_DATABASE" "$1"
}

query_result() {
	local sql=$1
	local result

	if ! result="$(application_query "$sql")"; then
		fail 'database query failed'
	fi

	printf '%s' "$result"
}

application_execute() {
	if ! application_query "$1" >/dev/null; then
		fail 'database statement rejected unexpectedly'
	fi
}

expect_application_failure() {
	local description=$1
	local sql=$2

	if application_query "$sql" >/dev/null; then
		fail "$description"
	fi
}

apply_migration() {
	if ! "$psql" \
		--no-psqlrc \
		--no-password \
		--host="$MEMORY_POSTGRES_SOCKET_DIR" \
		--port="$PGPORT" \
		--username="$MEMORY_POSTGRES_MIGRATION_ROLE" \
		--dbname="$MEMORY_POSTGRES_DATABASE" \
		--set=ON_ERROR_STOP=1 \
		--single-transaction \
		--quiet \
		--file="$MEMORY_SCHEMA_MIGRATION" >/dev/null 2>/dev/null; then
		fail 'migration application failed'
	fi
}

insert_memory() {
	local id=$1
	local version=$2
	local title=$3
	local content=$4
	local concepts=$5
	local files=$6
	local session_ids=$7
	local source_observation_ids=$8

	application_execute "
		INSERT INTO public.memories (
			id, version, type, title, content, created_at, updated_at,
			concepts, files, session_ids, source_observation_ids
		) VALUES (
			'$id', $version, 'custom/fact', '$title', '$content',
			TIMESTAMPTZ '2026-07-01 00:00:00+00', TIMESTAMPTZ '2026-07-01 00:00:00+00',
			$concepts, $files, $session_ids, $source_observation_ids
		)"
}

insert_embedding() {
	local id=$1
	local version=$2
	local embedding=$3

	application_execute "
		INSERT INTO public.memory_embeddings (id, version, embedding)
		VALUES ('$id', $version, '$embedding'::vector)"
}

vector_query() {
	local query=$1
	local limit=$2
	local assertion=$3

	query_result "
		WITH query_input AS MATERIALIZED (
			SELECT '$query'::text::vector AS embedding
		),
		query_vector AS MATERIALIZED (
			SELECT
				vector_dims(query_input.embedding) AS dimensions,
				vector_norm(query_input.embedding) AS norm,
				l2_normalize(query_input.embedding) AS normalized_embedding
			FROM query_input
		),
		scored_heads AS MATERIALIZED (
			SELECT
				head.id,
				head.version,
				CASE
					WHEN vector_dims(embedding.embedding) = query_vector.dimensions
					 AND vector_norm(embedding.embedding) > 0::double precision
					 AND query_vector.norm > 0::double precision
					THEN 1::double precision - (
						l2_normalize(embedding.embedding) <=> query_vector.normalized_embedding
					)
				END AS relevance
			FROM public.memory_search_heads AS head
			JOIN public.memory_embeddings AS embedding
				ON embedding.id = head.id AND embedding.version = head.version
			CROSS JOIN query_vector
		),
		canonical_results AS MATERIALIZED (
			SELECT
				memory.id,
				memory.version::text AS version,
				memory.type AS memory_type,
				memory.title,
				memory.content,
				memory.created_at,
				memory.updated_at,
				to_json(memory.concepts) AS concepts,
				to_json(memory.files) AS files,
				to_json(memory.session_ids) AS session_ids,
				to_json(memory.source_observation_ids) AS source_observation_ids,
				scored_heads.relevance
			FROM scored_heads
			JOIN public.memories AS memory
				ON memory.id = scored_heads.id
				AND memory.version = scored_heads.version
			WHERE relevance IS NOT NULL
			ORDER BY scored_heads.relevance DESC,
				scored_heads.id COLLATE \"C\" ASC,
				scored_heads.version ASC
			LIMIT '$limit'::text::bigint
		)
		$assertion"
}

apply_migration

server_version="$(query_result 'SHOW server_version')"
case "$server_version" in
"$MEMORY_POSTGRES_EXPECTED_MAJOR".*) ;;
*) fail 'unexpected PostgreSQL server major' ;;
esac

expect_equal \
	"$(query_result "
		SELECT vector_dims('[1,0]'::text::vector) = 2
			AND vector_norm('[1,0]'::text::vector) > 0::double precision")" \
	't' \
	'query vector preprocessing'
expect_equal \
	"$(query_result "
		SELECT (l2_normalize('[7,0]'::vector) <=> l2_normalize('[1,0]'::text::vector)) IS NOT NULL")" \
	't' \
	'normalized cosine primitive'

insert_memory \
	'vector-rank-aligned' 1 'rank aligned title' 'rank aligned content' \
	"ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]"
insert_embedding 'vector-rank-aligned' 1 '[7,0]'
insert_memory \
	'vector-rank-diagonal' 1 'rank diagonal title' 'rank diagonal content' \
	"ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]"
insert_embedding 'vector-rank-diagonal' 1 '[1,1]'
insert_memory \
	'vector-rank-orthogonal' 1 'rank orthogonal title' 'rank orthogonal content' \
	"ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]"
insert_embedding 'vector-rank-orthogonal' 1 '[0,7]'

expect_equal \
	"$(vector_query '[1,0]' '10' 'SELECT count(*) = 3 FROM canonical_results')" \
	't' \
	'compatible vector candidate count'
expect_equal \
	"$(vector_query '[1,0]' '10' "
		SELECT coalesce(
			count(*) = 3
			AND (SELECT relevance FROM canonical_results WHERE id = 'vector-rank-aligned') >
				(SELECT relevance FROM canonical_results WHERE id = 'vector-rank-diagonal')
			AND (SELECT relevance FROM canonical_results WHERE id = 'vector-rank-diagonal') >
				(SELECT relevance FROM canonical_results WHERE id = 'vector-rank-orthogonal')
			AND string_agg(
				id || ':' || version,
				',' ORDER BY relevance DESC, id COLLATE \"C\" ASC, version::bigint ASC
			) = 'vector-rank-aligned:1,vector-rank-diagonal:1,vector-rank-orthogonal:1',
			false
		)
		FROM canonical_results")" \
	't' \
	'exact cosine ranking'

insert_memory \
	'vector-latest-without-embedding' 1 'older embedded title' 'older embedded content' \
	"ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]"
insert_embedding 'vector-latest-without-embedding' 1 '[1,0]'
insert_memory \
	'vector-latest-without-embedding' 2 'current unembedded title' 'current unembedded content' \
	"ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]"

expect_equal \
	"$(query_result "
		SELECT
			(SELECT count(*) FROM public.memory_embeddings
			 WHERE id = 'vector-latest-without-embedding' AND version = 1) = 1
			AND (SELECT count(*) FROM public.memory_embeddings
				 WHERE id = 'vector-latest-without-embedding' AND version = 2) = 0
			AND (SELECT count(*) FROM public.memory_search_heads
				 WHERE id = 'vector-latest-without-embedding' AND version = 2) = 1")" \
	't' \
	'latest unembedded state'
expect_equal \
	"$(vector_query '[1,0]' '10' "
		SELECT count(*) = 0
		FROM canonical_results
		WHERE id = 'vector-latest-without-embedding'")" \
	't' \
	'latest unembedded suppression'

insert_memory \
	'vector-post-head-embedding' 1 'post-head older title' 'post-head older content' \
	"ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]"
insert_embedding 'vector-post-head-embedding' 1 '[0,1]'
insert_memory \
	'vector-post-head-embedding' 2 'post-head current title' 'post-head current content' \
	"ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]"
post_head_xmin="$(query_result "
	SELECT xmin::text
	FROM public.memory_search_heads
	WHERE id = 'vector-post-head-embedding' AND version = 2")"
[[ -n "$post_head_xmin" ]] || fail 'post-head marker unavailable'
expect_equal \
	"$(vector_query '[1,0]' '10' "
		SELECT count(*) = 0
		FROM canonical_results
		WHERE id = 'vector-post-head-embedding'")" \
	't' \
	'post-head unembedded suppression'
insert_embedding 'vector-post-head-embedding' 2 '[1,0]'
expect_equal \
	"$(query_result "
		SELECT xmin::text = '$post_head_xmin'
			AND search_document = ARRAY['post-head current title', 'post-head current content']::text[]
		FROM public.memory_search_heads
		WHERE id = 'vector-post-head-embedding' AND version = 2")" \
	't' \
	'post-head embedding leaves head unchanged'
expect_equal \
	"$(vector_query '[1,0]' '10' "
		SELECT count(*) = 1 AND bool_and(version = '2')
		FROM canonical_results
		WHERE id = 'vector-post-head-embedding'")" \
	't' \
	'post-head embedding visibility'

insert_memory \
	'vector-incompatible-dimension' 1 'incompatible title' 'incompatible content' \
	"ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]"
insert_embedding 'vector-incompatible-dimension' 1 '[1,0,0]'
expect_equal \
	"$(vector_query '[1,0]' '10' "
		SELECT count(*) = 4
			AND count(*) FILTER (WHERE id = 'vector-incompatible-dimension') = 0
		FROM canonical_results")" \
	't' \
	'incompatible embedding exclusion'
expect_equal \
	"$(vector_query '[1,0,0,0,0]' '10' 'SELECT count(*) = 0 FROM canonical_results')" \
	't' \
	'incompatible-only query empty result'

insert_memory \
	'vector-zero-stored' 1 'zero stored title' 'zero stored content' \
	"ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]"
expect_application_failure 'zero stored embedding rejection' "
	INSERT INTO public.memory_embeddings (id, version, embedding)
	VALUES ('vector-zero-stored', 1, '[0,0]'::vector)"
expect_equal \
	"$(query_result "
		SELECT count(*) = 0
		FROM public.memory_embeddings
		WHERE id = 'vector-zero-stored' AND version = 1")" \
	't' \
	'zero stored embedding absent'
expect_equal \
	"$(vector_query '[0,0]' '10' 'SELECT count(*) = 0 FROM canonical_results')" \
	't' \
	'zero direct query empty result'

for id in vector-tie-A vector-tie-a vector-tie-z; do
	insert_memory \
		"$id" 1 'tie title' 'tie content' \
		"ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]"
	insert_embedding "$id" 1 '[1,0,0,0]'
done

expect_equal \
	"$(vector_query '[1,0,0,0]' '10' "
		SELECT coalesce(
			count(*) = 3
			AND min(relevance) = max(relevance)
			AND string_agg(
				id || ':' || version,
				',' ORDER BY relevance DESC, id COLLATE \"C\" ASC, version::bigint ASC
			) = 'vector-tie-A:1,vector-tie-a:1,vector-tie-z:1',
			false
		)
		FROM canonical_results")" \
	't' \
	'C-collated vector ties'
expect_equal \
	"$(vector_query '[1,0,0,0]' '2' "
		SELECT coalesce(
			string_agg(
				id || ':' || version,
				',' ORDER BY relevance DESC, id COLLATE \"C\" ASC, version::bigint ASC
			) = 'vector-tie-A:1,vector-tie-a:1',
			false
		)
		FROM canonical_results")" \
	't' \
	'vector tie limit'

insert_memory \
	'vector-json-projection' 1 'JSON projection title' 'JSON projection content' \
	"ARRAY['vector-concept-first', 'vector-concept-duplicate', 'vector-concept-duplicate', 'vector-concept-last']::text[]" \
	"ARRAY['vector-file-first', 'vector-file-last']::text[]" \
	"ARRAY['vector-session-first', 'vector-session-last']::text[]" \
	"ARRAY['vector-source-first', 'vector-source-last']::text[]"
insert_embedding 'vector-json-projection' 1 '[3,4,0,0,0,0]'
expect_equal \
	"$(query_result "
		SELECT count(*) = 1
		FROM public.memory_embeddings
		WHERE id = 'vector-json-projection' AND version = 1")" \
	't' \
	'projection fixture embedding present'
expect_equal \
	"$(vector_query '[3,4,0,0,0,0]' '10' "
		, projected_results AS MATERIALIZED (
			SELECT to_jsonb(canonical_results) AS result
			FROM canonical_results
		)
		SELECT
			count(*) = 1
			AND coalesce(bool_and(
				result ?& ARRAY[
					'id', 'version', 'memory_type', 'title', 'content', 'created_at', 'updated_at',
					'concepts', 'files', 'session_ids', 'source_observation_ids', 'relevance'
				]
				AND result ->> 'id' = 'vector-json-projection'
				AND result ->> 'version' = '1'
				AND result -> 'concepts' = '[\"vector-concept-first\", \"vector-concept-duplicate\", \"vector-concept-duplicate\", \"vector-concept-last\"]'::jsonb
				AND result -> 'files' = '[\"vector-file-first\", \"vector-file-last\"]'::jsonb
				AND result -> 'session_ids' = '[\"vector-session-first\", \"vector-session-last\"]'::jsonb
				AND result -> 'source_observation_ids' = '[\"vector-source-first\", \"vector-source-last\"]'::jsonb
				AND NOT (result ? 'embedding')
			), false)
		FROM projected_results")" \
	't' \
	'JSONB projection without embedding'

printf 'PostgreSQL %s memory vector=passed\n' "$MEMORY_POSTGRES_EXPECTED_MAJOR"
printf 'memory vector ranking=cosine:true latest-only:true post-head:true\n'
printf 'memory vector guards=dimensions:true zero-stored:rejected zero-query:empty\n'
printf 'memory vector ties=C-order:true limit-two:true\n'
printf 'memory vector projection=jsonb:true arrays:true duplicate-order:true embedding:false\n'
