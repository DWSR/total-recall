#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 0 ]]; then
	printf 'usage: %s\n' "$0" >&2
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
	printf 'missing PostgreSQL executable: %s\n' "$psql" >&2
	exit 66
fi
if [[ ! -r "$MEMORY_SCHEMA_MIGRATION" ]]; then
	printf 'missing memory schema migration: %s\n' "$MEMORY_SCHEMA_MIGRATION" >&2
	exit 66
fi

fail() {
	printf 'memory search head verification failed: %s\n' "$1" >&2
	exit 1
}

expect_equal() {
	local actual=$1
	local expected=$2
	local description=$3

	[[ "$actual" == "$expected" ]] ||
		fail "$description: expected $expected, found $actual"
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
		--command="$sql"
}

application_query() {
	query_as "$MEMORY_POSTGRES_APPLICATION_ROLE" "$MEMORY_POSTGRES_DATABASE" "$1"
}

migration_query() {
	query_as "$MEMORY_POSTGRES_MIGRATION_ROLE" "$MEMORY_POSTGRES_DATABASE" "$1"
}

application_execute() {
	application_query "$1" >/dev/null
}

# Each rejected statement needs a separate psql session because ON_ERROR_STOP
# would otherwise stop later assertions in a shared session. Suppressing stderr
# avoids emitting protected source-row values from PostgreSQL error context.
expect_failure() {
	local description=$1
	local role=$2
	local sql=$3

	if query_as "$role" "$MEMORY_POSTGRES_DATABASE" "$sql" >/dev/null 2>&1; then
		fail "expected rejection: $description"
	fi

	printf 'rejected=%s\n' "$description"
}

apply_migration() {
	"$psql" \
		--no-psqlrc \
		--no-password \
		--host="$MEMORY_POSTGRES_SOCKET_DIR" \
		--port="$PGPORT" \
		--username="$MEMORY_POSTGRES_MIGRATION_ROLE" \
		--dbname="$MEMORY_POSTGRES_DATABASE" \
		--set=ON_ERROR_STOP=1 \
		--single-transaction \
		--quiet \
		--file="$MEMORY_SCHEMA_MIGRATION" >/dev/null
}

apply_migration

server_version="$(application_query 'SHOW server_version')"
case "$server_version" in
"$MEMORY_POSTGRES_EXPECTED_MAJOR".*) ;;
*) fail "expected PostgreSQL $MEMORY_POSTGRES_EXPECTED_MAJOR, found $server_version" ;;
esac

expect_equal \
	"$(migration_query "SELECT tableowner FROM pg_tables WHERE schemaname = 'public' AND tablename = 'memory_search_heads'")" \
	"$MEMORY_POSTGRES_MIGRATION_ROLE" \
	'migration role owns search heads'
expect_equal \
	"$(migration_query "SELECT string_agg(attname || '|' || format_type(atttypid, atttypmod) || '|' || attnotnull, E'\\n' ORDER BY attnum) FROM pg_attribute WHERE attrelid = 'public.memory_search_heads'::regclass AND attnum > 0 AND NOT attisdropped")" \
	'id|text|true
version|bigint|true
search_document|text[]|true' \
	'current search head columns'
expect_equal \
	"$(migration_query "SELECT condeferrable || '|' || condeferred || '|' || confupdtype::text || '|' || confdeltype::text || '|' || (SELECT string_agg(attribute.attname, ',' ORDER BY key.ordinality) FROM unnest(foreign_key.conkey) WITH ORDINALITY AS key(attnum, ordinality) JOIN pg_attribute AS attribute ON attribute.attrelid = foreign_key.conrelid AND attribute.attnum = key.attnum) || '|' || (SELECT string_agg(attribute.attname, ',' ORDER BY key.ordinality) FROM unnest(foreign_key.confkey) WITH ORDINALITY AS key(attnum, ordinality) JOIN pg_attribute AS attribute ON attribute.attrelid = foreign_key.confrelid AND attribute.attnum = key.attnum) FROM pg_constraint AS foreign_key WHERE foreign_key.conrelid = 'public.memory_search_heads'::regclass AND foreign_key.contype = 'f'")" \
	'false|false|r|r|id,version|id,version' \
	'immediate restrictive search-head foreign key'
expect_equal \
	"$(migration_query "SELECT count(*) FROM pg_index AS table_index JOIN pg_class AS index_relation ON index_relation.oid = table_index.indexrelid JOIN pg_am AS access_method ON access_method.oid = index_relation.relam WHERE table_index.indrelid = 'public.memory_search_heads'::regclass AND index_relation.relname = 'memory_search_heads_search_document_bm25_idx' AND access_method.amname = 'bm25' AND table_index.indisvalid")" \
	'1' \
	'valid english BM25 search-document index'
expect_equal \
	"$(migration_query "SELECT prosecdef || '|' || pg_get_userbyid(proowner) || '|' || coalesce(array_to_string(proconfig, '|'), '') FROM pg_proc WHERE oid = 'public.memory_search_heads_after_insert()'::regprocedure")" \
	"true|$MEMORY_POSTGRES_MIGRATION_ROLE|search_path=pg_catalog, pg_temp" \
	'secured migration-owned trigger function'
expect_equal \
	"$(migration_query "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'public.memories'::regclass AND NOT tgisinternal AND tgname = 'memory_search_heads_after_insert' AND tgfoid = 'public.memory_search_heads_after_insert()'::regprocedure AND tgtype::integer = 5")" \
	'1' \
	'after-insert current-head trigger'
expect_equal \
	"$(migration_query "SELECT has_function_privilege('$MEMORY_POSTGRES_APPLICATION_ROLE', 'public.memory_search_heads_after_insert()'::regprocedure, 'EXECUTE') || '|' || (SELECT count(*) = 0 FROM pg_proc AS routine CROSS JOIN LATERAL aclexplode(routine.proacl) AS privilege WHERE routine.oid = 'public.memory_search_heads_after_insert()'::regprocedure AND privilege.grantee = 0 AND privilege.privilege_type = 'EXECUTE')")" \
	'false|true' \
	'trigger function execution is denied to application and public'
expect_equal \
	"$(application_query "SELECT has_table_privilege(current_user, 'public.memory_search_heads', 'SELECT') || '|' || has_table_privilege(current_user, 'public.memory_search_heads', 'INSERT') || '|' || has_table_privilege(current_user, 'public.memory_search_heads', 'UPDATE') || '|' || has_table_privilege(current_user, 'public.memory_search_heads', 'DELETE') || '|' || has_table_privilege(current_user, 'public.memory_search_heads', 'TRUNCATE')")" \
	'true|false|false|false|false' \
	'application search-head privileges'
expect_equal \
	"$(migration_query "SELECT count(*) FROM pg_class AS relation CROSS JOIN LATERAL aclexplode(relation.relacl) AS privilege WHERE relation.oid = 'public.memory_search_heads'::regclass AND privilege.grantee = 0")" \
	'0' \
	'public search-head privileges'

application_execute "
	INSERT INTO public.memories (
		id, version, type, title, content, created_at, updated_at,
		concepts, files, session_ids, source_observation_ids
	) VALUES (
		'memory-document-order', 1, 'custom/fact', 'document title', 'document content',
		TIMESTAMPTZ '2026-01-01 00:00:00+00', TIMESTAMPTZ '2026-01-01 00:00:00+00',
		ARRAY['concept-one', 'concept-one', 'concept-two']::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[]
	)"
expect_equal \
	"$(application_query "SELECT search_document = ARRAY['document title', 'document content', 'concept-one', 'concept-one', 'concept-two']::text[] FROM public.memory_search_heads WHERE id = 'memory-document-order'")" \
	't' \
	'head search-document preserves title content concept order and duplicates'

application_execute "
	INSERT INTO public.memories (
		id, version, type, title, content, created_at, updated_at,
		concepts, files, session_ids, source_observation_ids
	) VALUES (
		'memory-bm25-title', 1, 'custom/fact', 'Orchards', 'unrelated body',
		TIMESTAMPTZ '2026-01-02 00:00:00+00', TIMESTAMPTZ '2026-01-02 00:00:00+00',
		ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[]
	)"
application_execute "
	INSERT INTO public.memories (
		id, version, type, title, content, created_at, updated_at,
		concepts, files, session_ids, source_observation_ids
	) VALUES (
		'memory-bm25-content', 1, 'custom/fact', 'unrelated title', 'Orchards',
		TIMESTAMPTZ '2026-01-03 00:00:00+00', TIMESTAMPTZ '2026-01-03 00:00:00+00',
		ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[]
	)"
application_execute "
	INSERT INTO public.memories (
		id, version, type, title, content, created_at, updated_at,
		concepts, files, session_ids, source_observation_ids
	) VALUES (
		'memory-bm25-concepts', 1, 'custom/fact', 'unrelated title', 'unrelated body',
		TIMESTAMPTZ '2026-01-04 00:00:00+00', TIMESTAMPTZ '2026-01-04 00:00:00+00',
		ARRAY['Orchards']::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[]
	)"
expect_equal \
	"$(application_query "SELECT string_agg(id, ',' ORDER BY id) FROM (SELECT id FROM public.memory_search_heads ORDER BY search_document <@> to_bm25query('orchard', 'memory_search_heads_search_document_bm25_idx') LIMIT 3) AS matches")" \
	'memory-bm25-concepts,memory-bm25-content,memory-bm25-title' \
	'english BM25 matches title content concepts and stems plurals'
bm25_plan="$(application_query "SET enable_seqscan = off; EXPLAIN (COSTS OFF) SELECT id FROM public.memory_search_heads ORDER BY search_document <@> to_bm25query('orchard', 'memory_search_heads_search_document_bm25_idx') LIMIT 3")"
[[ "$bm25_plan" == *'memory_search_heads_search_document_bm25_idx'* ]] ||
	fail 'BM25 query plan did not use the search-document index'

application_execute "
	INSERT INTO public.memories (
		id, version, type, title, content, created_at, updated_at,
		concepts, files, session_ids, source_observation_ids
	) VALUES (
		'memory-current-head', 1, 'custom/fact', 'version one title', 'version one content',
		TIMESTAMPTZ '2026-01-05 00:00:00+00', TIMESTAMPTZ '2026-01-05 00:00:00+00',
		ARRAY['version-one']::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[]
	)"
application_execute "
	INSERT INTO public.memories (
		id, version, type, title, content, created_at, updated_at,
		concepts, files, session_ids, source_observation_ids
	) VALUES (
		'memory-current-head', 3, 'custom/fact', 'version three title', 'version three content',
		TIMESTAMPTZ '2026-01-06 00:00:00+00', TIMESTAMPTZ '2026-01-06 00:00:00+00',
		ARRAY['version-three']::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[]
	)"
application_execute "
	INSERT INTO public.memories (
		id, version, type, title, content, created_at, updated_at,
		concepts, files, session_ids, source_observation_ids
	) VALUES (
		'memory-current-head', 2, 'custom/fact', 'version two title', 'version two content',
		TIMESTAMPTZ '2026-01-07 00:00:00+00', TIMESTAMPTZ '2026-01-07 00:00:00+00',
		ARRAY['version-two']::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[]
	)"
expect_equal \
	"$(application_query "SELECT version || '|' || (search_document = ARRAY['version three title', 'version three content', 'version-three']::text[]) FROM public.memory_search_heads WHERE id = 'memory-current-head'")" \
	'3|true' \
	'greater version replaces the current head'
expect_equal \
	"$(application_query "SELECT count(*) || '|' || string_agg(version::text, ',' ORDER BY version) FROM public.memories WHERE id = 'memory-current-head'")" \
	'3|1,2,3' \
	'lower out-of-order version remains immutable history'
expect_equal \
	"$(application_query "SELECT count(*) FROM public.memory_search_heads WHERE id = 'memory-current-head'")" \
	'1' \
	'one current head per memory ID'

expect_failure 'application direct head insert' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memory_search_heads (id, version, search_document)
	VALUES ('memory-head-direct-denial', 1, ARRAY['denied']::text[])"
expect_failure 'application direct head update' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	UPDATE public.memory_search_heads SET version = 1 WHERE id = 'memory-document-order'"
expect_failure 'application direct head delete' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	DELETE FROM public.memory_search_heads WHERE id = 'memory-document-order'"
expect_failure 'application direct head truncate' "$MEMORY_POSTGRES_APPLICATION_ROLE" 'TRUNCATE public.memory_search_heads'
expect_failure 'application direct trigger function execution' "$MEMORY_POSTGRES_APPLICATION_ROLE" \
	'SELECT public.memory_search_heads_after_insert()'
expect_equal \
	"$(application_query "SELECT count(*) FROM public.memory_search_heads WHERE id = 'memory-head-direct-denial'")" \
	'0' \
	'direct application head mutation leaves no row'

expect_failure 'head constraint rolls back source memory insert' "$MEMORY_POSTGRES_MIGRATION_ROLE" "
	BEGIN;
	ALTER TABLE public.memory_search_heads
		ADD CONSTRAINT memory_search_heads_ephemeral_failure
		CHECK (id <> 'memory-trigger-failure');
	INSERT INTO public.memories (
		id, version, type, title, content, created_at, updated_at,
		concepts, files, session_ids, source_observation_ids
	) VALUES (
		'memory-trigger-failure', 1, 'custom/fact', 'failure title', 'failure content',
		TIMESTAMPTZ '2026-01-08 00:00:00+00', TIMESTAMPTZ '2026-01-08 00:00:00+00',
		ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[]
	);
	COMMIT"
expect_equal \
	"$(application_query "SELECT (SELECT count(*) FROM public.memories WHERE id = 'memory-trigger-failure') || '|' || (SELECT count(*) FROM public.memory_search_heads WHERE id = 'memory-trigger-failure') || '|' || (SELECT count(*) FROM pg_constraint WHERE conrelid = 'public.memory_search_heads'::regclass AND conname = 'memory_search_heads_ephemeral_failure')")" \
	'0|0|0' \
	'projection failure rolls back source insert and ephemeral constraint'

printf 'PostgreSQL %s memory search heads server=%s\n' "$MEMORY_POSTGRES_EXPECTED_MAJOR" "$server_version"
printf 'memory search head document=title-content-concepts-order-duplicates\n'
printf 'memory search head bm25=english-index-plan title:true content:true concepts:true stemming:true\n'
printf 'memory search head transition=v1-v3-v2 current:v3 history:v1,v2,v3\n'
printf 'memory search head security=select-only direct-dml:false direct-execute:false rollback:true\n'
