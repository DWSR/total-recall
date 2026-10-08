#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 0 ]]; then
	printf 'usage: verify-memory-bm25.sh\n' >&2
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
	printf 'memory BM25 verification failed: %s\n' "$1" >&2
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
			TIMESTAMPTZ '2026-06-01 00:00:00+00', TIMESTAMPTZ '2026-06-01 00:00:00+00',
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

bm25_query() {
	local query=$1
	local limit=$2
	local assertion=$3

	query_result "
		WITH scored_heads AS MATERIALIZED (
			SELECT
				head.id,
				head.version,
				-(head.search_document <@> to_bm25query(
					'$query'::text,
					'public.memory_search_heads_search_document_bm25_idx'
				)) AS relevance
			FROM public.memory_search_heads AS head
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
			WHERE scored_heads.relevance > 0::double precision
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
*) fail "expected PostgreSQL $MEMORY_POSTGRES_EXPECTED_MAJOR" ;;
esac

insert_memory \
	'bm25-title-match' 1 'titlelexeme' 'ordinary body' \
	"ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]"
insert_memory \
	'bm25-content-match' 1 'ordinary title' 'contentlexeme' \
	"ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]"
insert_memory \
	'bm25-concept-match' 1 'ordinary title' 'ordinary body' \
	"ARRAY['conceptlexeme']::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]"

expect_equal \
	"$(bm25_query 'titlelexeme' '10' "
		SELECT coalesce(
			string_agg(id || ':' || version, ',' ORDER BY relevance DESC, id COLLATE \"C\" ASC, version::bigint ASC),
			''
		)
		FROM canonical_results")" \
	'bm25-title-match:1' \
	'title lexical match'
expect_equal \
	"$(bm25_query 'contentlexeme' '10' "
		SELECT coalesce(
			string_agg(id || ':' || version, ',' ORDER BY relevance DESC, id COLLATE \"C\" ASC, version::bigint ASC),
			''
		)
		FROM canonical_results")" \
	'bm25-content-match:1' \
	'content lexical match'
expect_equal \
	"$(bm25_query 'conceptlexeme' '10' "
		SELECT coalesce(
			string_agg(id || ':' || version, ',' ORDER BY relevance DESC, id COLLATE \"C\" ASC, version::bigint ASC),
			''
		)
		FROM canonical_results")" \
	'bm25-concept-match:1' \
	'concept lexical match'
expect_equal \
	"$(bm25_query 'absentlexeme' '10' 'SELECT count(*) FROM canonical_results')" \
	'0' \
	'fixed absent lexical query returns no rows'

insert_memory \
	'bm25-corpus-target' 1 'corpusglyph' 'fixed content' \
	"ARRAY['fixed-concept']::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]"
insert_memory \
	'bm25-corpus-obsolete' 1 'corpusglyph' 'fixed content' \
	"ARRAY['fixed-concept']::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]"

expect_equal \
	"$(bm25_query 'corpusglyph' '10' 'SELECT count(*) FROM canonical_results')" \
	'2' \
	'initial current corpus has target and obsolete match'
expect_equal \
	"$(bm25_query 'corpusglyph' '10' 'SELECT bool_and(relevance > 0::double precision) FROM canonical_results')" \
	't' \
	'initial emitted BM25 scores are positive'
score_before="$(bm25_query 'corpusglyph' '10' "
	SELECT relevance::text
	FROM canonical_results
	WHERE id = 'bm25-corpus-target'")"
[[ -n "$score_before" ]] || fail 'initial target score is present'

insert_memory \
	'bm25-corpus-obsolete' 2 'unrelatedxx' 'fixed content' \
	"ARRAY['fixed-concept']::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]"
expect_equal \
	"$(query_result "
		SELECT (
			SELECT length(title) + length(content) + coalesce((
				SELECT sum(length(concept))
				FROM unnest(concepts) AS concept
			), 0)
			FROM public.memories
			WHERE id = 'bm25-corpus-obsolete' AND version = 1
		) = (
			SELECT length(title) + length(content) + coalesce((
				SELECT sum(length(concept))
				FROM unnest(concepts) AS concept
			), 0)
			FROM public.memories
			WHERE id = 'bm25-corpus-obsolete' AND version = 2
		)")" \
	't' \
	'obsolete versions have equal BM25 document length'
expect_equal \
	"$(query_result "
		SELECT
			(SELECT count(*) FROM public.memories WHERE id = 'bm25-corpus-obsolete') || '|' ||
			(SELECT count(*) FROM public.memory_search_heads WHERE id = 'bm25-corpus-obsolete' AND version = 2)")" \
	'2|1' \
	'obsolete replacement advances the sole current head'
expect_equal \
	"$(bm25_query 'corpusglyph' '10' "
		SELECT coalesce(
			string_agg(id || ':' || version, ',' ORDER BY relevance DESC, id COLLATE \"C\" ASC, version::bigint ASC),
			''
		)
		FROM canonical_results")" \
	'bm25-corpus-target:1' \
	'only the current corpus target matches'
score_after="$(bm25_query 'corpusglyph' '10' "
	SELECT relevance::text
	FROM canonical_results
	WHERE id = 'bm25-corpus-target'")"
[[ -n "$score_after" ]] || fail 'current target score is present'
expect_equal \
	"$(query_result "SELECT '$score_after'::double precision > '$score_before'::double precision")" \
	't' \
	'current-only corpus increases the target score'

for id in bm25-tie-A bm25-tie-a bm25-tie-z; do
	insert_memory \
		"$id" 1 'tiequasar' 'fixed tie content' \
		"ARRAY['fixed-tie-concept']::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]" "ARRAY[]::text[]"
done

expect_equal \
	"$(bm25_query 'tiequasar' '10' 'SELECT count(*) FROM canonical_results')" \
	'3' \
	'identical BM25 tie fixtures all match'
expect_equal \
	"$(bm25_query 'tiequasar' '10' 'SELECT bool_and(relevance > 0::double precision) FROM canonical_results')" \
	't' \
	'all tied BM25 scores are positive'
expect_equal \
	"$(bm25_query 'tiequasar' '10' 'SELECT min(relevance) = max(relevance) FROM canonical_results')" \
	't' \
	'identical BM25 tie fixtures have equal scores'
expect_equal \
	"$(bm25_query 'tiequasar' '10' "
		SELECT string_agg(
			id || ':' || version,
			',' ORDER BY relevance DESC, id COLLATE \"C\" ASC, version::bigint ASC
		)
		FROM canonical_results")" \
	'bm25-tie-A:1,bm25-tie-a:1,bm25-tie-z:1' \
	'C-collated BM25 tie order'
expect_equal \
	"$(bm25_query 'tiequasar' '2' "
		SELECT string_agg(
			id || ':' || version,
			',' ORDER BY relevance DESC, id COLLATE \"C\" ASC, version::bigint ASC
		)
		FROM canonical_results")" \
	'bm25-tie-A:1,bm25-tie-a:1' \
	'BM25 string-cast limit preserves first C-collated ties'

insert_memory \
	'bm25-json-projection' 1 'jsonlexeme' 'projection body' \
	"ARRAY['json-concept-first', 'json-concept-duplicate', 'json-concept-duplicate', 'json-concept-last']::text[]" \
	"ARRAY['json-file-first', 'json-file-last']::text[]" \
	"ARRAY['json-session-first', 'json-session-last']::text[]" \
	"ARRAY['json-source-first', 'json-source-last']::text[]"
insert_embedding 'bm25-json-projection' 1 '[3,4]'
expect_equal \
	"$(query_result "
		SELECT count(*) = 1
		FROM public.memory_embeddings
		WHERE id = 'bm25-json-projection' AND version = 1")" \
	't' \
	'JSON projection fixture stores an embedding'
expect_equal \
	"$(bm25_query 'jsonlexeme' '10' "
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
				AND result ->> 'id' = 'bm25-json-projection'
				AND result ->> 'version' = '1'
				AND result -> 'concepts' = '[\"json-concept-first\", \"json-concept-duplicate\", \"json-concept-duplicate\", \"json-concept-last\"]'::jsonb
				AND result -> 'files' = '[\"json-file-first\", \"json-file-last\"]'::jsonb
				AND result -> 'session_ids' = '[\"json-session-first\", \"json-session-last\"]'::jsonb
				AND result -> 'source_observation_ids' = '[\"json-source-first\", \"json-source-last\"]'::jsonb
				AND (result ->> 'relevance')::double precision > 0::double precision
				AND NOT (result ? 'embedding')
			), false)
		FROM projected_results")" \
	't' \
	'canonical JSONB projection preserves ordered arrays and omits embedding'

printf 'PostgreSQL %s memory BM25=passed\n' "$MEMORY_POSTGRES_EXPECTED_MAJOR"
printf 'memory BM25 fields=title:true content:true concepts:true empty-count:0\n'
printf 'memory BM25 corpus=current-only:true score-increase:true\n'
printf 'memory BM25 ranking=positive-scores:true C-ties:true limit-two:true\n'
printf 'memory BM25 projection=jsonb:true arrays:true duplicate-order:true embedding:false\n'
