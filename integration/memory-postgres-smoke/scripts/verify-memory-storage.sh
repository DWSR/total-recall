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
	printf 'memory storage verification failed: %s\n' "$1" >&2
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
# would otherwise stop later assertions in a shared session.
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
	"$(migration_query "SELECT string_agg(tablename || '|' || tableowner, E'\\n' ORDER BY tablename) FROM pg_tables WHERE schemaname = 'public' AND tablename IN ('memories', 'memory_embeddings', 'memory_search_heads')")" \
	"memories|$MEMORY_POSTGRES_MIGRATION_ROLE
memory_embeddings|$MEMORY_POSTGRES_MIGRATION_ROLE
memory_search_heads|$MEMORY_POSTGRES_MIGRATION_ROLE" \
	'migration role owns storage tables'
expect_equal \
	"$(migration_query "SELECT string_agg(attname || '|' || format_type(atttypid, atttypmod) || '|' || attnotnull, E'\\n' ORDER BY attnum) FROM pg_attribute WHERE attrelid = 'public.memories'::regclass AND attnum > 0 AND NOT attisdropped")" \
	'id|text|true
version|bigint|true
type|text|true
title|text|true
content|text|true
created_at|timestamp with time zone|true
updated_at|timestamp with time zone|true
concepts|text[]|true
files|text[]|true
session_ids|text[]|true
source_observation_ids|text[]|true' \
	'canonical memory columns'
expect_equal \
	"$(migration_query "SELECT string_agg(attname || '|' || format_type(atttypid, atttypmod) || '|' || attnotnull, E'\\n' ORDER BY attnum) FROM pg_attribute WHERE attrelid = 'public.memory_embeddings'::regclass AND attnum > 0 AND NOT attisdropped")" \
	'id|text|true
version|bigint|true
embedding|vector|true' \
	'embedding columns'
expect_equal \
	"$(migration_query "SELECT condeferrable || '|' || condeferred || '|' || confupdtype::text || '|' || confdeltype::text || '|' || (SELECT string_agg(attribute.attname, ',' ORDER BY key.ordinality) FROM unnest(foreign_key.conkey) WITH ORDINALITY AS key(attnum, ordinality) JOIN pg_attribute AS attribute ON attribute.attrelid = foreign_key.conrelid AND attribute.attnum = key.attnum) || '|' || (SELECT string_agg(attribute.attname, ',' ORDER BY key.ordinality) FROM unnest(foreign_key.confkey) WITH ORDINALITY AS key(attnum, ordinality) JOIN pg_attribute AS attribute ON attribute.attrelid = foreign_key.confrelid AND attribute.attnum = key.attnum) FROM pg_constraint AS foreign_key WHERE foreign_key.conrelid = 'public.memory_embeddings'::regclass AND foreign_key.contype = 'f'")" \
	'false|false|r|r|id,version|id,version' \
	'immediate restrictive embedding foreign key'
expect_equal \
	"$(migration_query "SELECT count(*) FROM pg_constraint WHERE conrelid = 'public.memory_embeddings'::regclass AND contype = 'c' AND pg_get_constraintdef(oid) LIKE '%vector_dims(embedding) > 0%'")" \
	'1' \
	'positive embedding dimensions constraint'
expect_equal \
	"$(migration_query "SELECT count(*) FROM pg_constraint WHERE conrelid = 'public.memory_embeddings'::regclass AND contype = 'c' AND conname = 'memory_embeddings_embedding_norm_positive' AND pg_get_constraintdef(oid) LIKE '%vector_norm(embedding)%'")" \
	'1' \
	'positive embedding norm constraint'

for table in memories memory_embeddings; do
	expect_equal \
		"$(application_query "SELECT has_table_privilege(current_user, 'public.$table', 'SELECT') || '|' || has_table_privilege(current_user, 'public.$table', 'INSERT') || '|' || has_table_privilege(current_user, 'public.$table', 'UPDATE') || '|' || has_table_privilege(current_user, 'public.$table', 'DELETE') || '|' || has_table_privilege(current_user, 'public.$table', 'TRUNCATE')")" \
		'true|true|false|false|false' \
		"application $table privileges"
done
expect_equal \
	"$(migration_query "SELECT count(*) FROM pg_class AS relation CROSS JOIN LATERAL aclexplode(relation.relacl) AS privilege WHERE relation.oid IN ('public.memories'::regclass, 'public.memory_embeddings'::regclass) AND privilege.grantee = 0")" \
	'0' \
	'public table privileges'

application_execute "
	INSERT INTO public.memories (
		id, version, type, title, content, created_at, updated_at,
		concepts, files, session_ids, source_observation_ids
	) VALUES (
		'memory-one', 1, 'custom/fact', 'title-one', 'content-one',
		TIMESTAMPTZ '2026-01-01 00:00:00+00', TIMESTAMPTZ '2026-01-01 01:00:00+00',
		ARRAY['concept-one', 'concept-one']::text[], ARRAY['file-one', 'file-two']::text[],
		ARRAY['session-one']::text[], ARRAY['observation-one']::text[]
	)"
expect_equal \
	"$(application_query "SELECT (concepts = ARRAY['concept-one', 'concept-one']::text[]) || '|' || (files = ARRAY['file-one', 'file-two']::text[]) || '|' || (session_ids = ARRAY['session-one']::text[]) || '|' || (source_observation_ids = ARRAY['observation-one']::text[]) FROM public.memories WHERE id = 'memory-one' AND version = 1")" \
	'true|true|true|true' \
	'canonical collection preservation'
application_execute "
	INSERT INTO public.memories (
		id, version, type, title, content, created_at, updated_at,
		concepts, files, session_ids, source_observation_ids
	) VALUES (
		'memory-microsecond-timestamps', 1, 'custom/fact', 'timestamp title', 'timestamp content',
		TIMESTAMPTZ '2026-01-05 06:07:08.123456+00',
		TIMESTAMPTZ '2026-01-05 06:07:08.654321+00',
		ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[]
	)"
expect_equal \
	"$(application_query "SELECT to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US') || '|' || to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US') FROM public.memories WHERE id = 'memory-microsecond-timestamps' AND version = 1")" \
	'2026-01-05T06:07:08.123456|2026-01-05T06:07:08.654321' \
	'canonical microsecond timestamp persistence'
expect_equal \
	"$(application_query "SELECT count(*) FROM public.memory_embeddings WHERE id = 'memory-one' AND version = 1")" \
	'0' \
	'memory persists before its embedding'

application_execute "
	INSERT INTO public.memory_embeddings (id, version, embedding)
	VALUES ('memory-one', 1, '[3,4]'::vector)"
expect_equal \
	"$(application_query "SELECT vector_dims(embedding) || '|' || (vector_norm(embedding) > 0) FROM public.memory_embeddings WHERE id = 'memory-one' AND version = 1")" \
	'2|true' \
	'valid embedding persists independently'

application_execute "
	INSERT INTO public.memories (
		id, version, type, title, content, created_at, updated_at,
		concepts, files, session_ids, source_observation_ids
	) VALUES (
		'memory-unembedded', 1, 'custom/fact', 'title-unembedded', 'content-unembedded',
		TIMESTAMPTZ '2026-01-02 00:00:00+00', TIMESTAMPTZ '2026-01-02 00:00:00+00',
		ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[]
	)"
expect_equal \
	"$(application_query "SELECT cardinality(concepts) || '|' || cardinality(files) || '|' || cardinality(session_ids) || '|' || cardinality(source_observation_ids) FROM public.memories WHERE id = 'memory-unembedded' AND version = 1")" \
	'0|0|0|0' \
	'empty canonical collections'

application_execute "
	INSERT INTO public.memories (
		id, version, type, title, content, created_at, updated_at,
		concepts, files, session_ids, source_observation_ids
	) VALUES
		('memory-history', 2, 'custom/fact', 'title-two', 'content-two', TIMESTAMPTZ '2026-01-03 00:00:00+00', TIMESTAMPTZ '2026-01-03 00:00:00+00', ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[]),
		('memory-history', 1, 'custom/fact', 'title-one', 'content-one', TIMESTAMPTZ '2026-01-03 00:00:00+00', TIMESTAMPTZ '2026-01-03 00:00:00+00', ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[])"
expect_equal \
	"$(application_query "SELECT count(*) || '|' || max(version) FROM public.memories WHERE id = 'memory-history'")" \
	'2|2' \
	'out-of-order memory versions persist'

application_execute "
	INSERT INTO public.memories (
		id, version, type, title, content, created_at, updated_at,
		concepts, files, session_ids, source_observation_ids
	) VALUES (
		'memory-whitespace', 1, ' ', ' ', ' ',
		TIMESTAMPTZ '2026-01-04 00:00:00+00', TIMESTAMPTZ '2026-01-04 00:00:00+00',
		ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[]
	)"
expect_equal \
	"$(application_query "SELECT (type = ' ') || '|' || (title = ' ') || '|' || (content = ' ') FROM public.memories WHERE id = 'memory-whitespace' AND version = 1")" \
	'true|true|true' \
	'nonempty checks do not trim values'

expect_failure 'duplicate memory version' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memories (
		id, version, type, title, content, created_at, updated_at,
		concepts, files, session_ids, source_observation_ids
	) VALUES (
		'memory-one', 1, 'custom/fact', 'replacement', 'replacement',
		TIMESTAMPTZ '2026-01-01 00:00:00+00', TIMESTAMPTZ '2026-01-01 01:00:00+00',
		ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[]
	)"
expect_equal \
	"$(application_query "SELECT title || '|' || content FROM public.memories WHERE id = 'memory-one' AND version = 1")" \
	'title-one|content-one' \
	'duplicate memory leaves canonical row unchanged'

expect_failure 'empty memory id' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memories (id, version, type, title, content, created_at, updated_at, concepts, files, session_ids, source_observation_ids)
	VALUES ('', 1, 'custom/fact', 'title', 'content', TIMESTAMPTZ '2026-01-05 00:00:00+00', TIMESTAMPTZ '2026-01-05 00:00:00+00', ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[])"
expect_failure 'non-positive memory version' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memories (id, version, type, title, content, created_at, updated_at, concepts, files, session_ids, source_observation_ids)
	VALUES ('memory-zero-version', 0, 'custom/fact', 'title', 'content', TIMESTAMPTZ '2026-01-05 00:00:00+00', TIMESTAMPTZ '2026-01-05 00:00:00+00', ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[])"
expect_failure 'empty memory type' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memories (id, version, type, title, content, created_at, updated_at, concepts, files, session_ids, source_observation_ids)
	VALUES ('memory-empty-type', 1, '', 'title', 'content', TIMESTAMPTZ '2026-01-05 00:00:00+00', TIMESTAMPTZ '2026-01-05 00:00:00+00', ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[])"
expect_failure 'empty memory title' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memories (id, version, type, title, content, created_at, updated_at, concepts, files, session_ids, source_observation_ids)
	VALUES ('memory-empty-title', 1, 'custom/fact', '', 'content', TIMESTAMPTZ '2026-01-05 00:00:00+00', TIMESTAMPTZ '2026-01-05 00:00:00+00', ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[])"
expect_failure 'empty memory content' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memories (id, version, type, title, content, created_at, updated_at, concepts, files, session_ids, source_observation_ids)
	VALUES ('memory-empty-content', 1, 'custom/fact', 'title', '', TIMESTAMPTZ '2026-01-05 00:00:00+00', TIMESTAMPTZ '2026-01-05 00:00:00+00', ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[])"
expect_failure 'nonfinite created timestamp' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memories (id, version, type, title, content, created_at, updated_at, concepts, files, session_ids, source_observation_ids)
	VALUES ('memory-infinite-created', 1, 'custom/fact', 'title', 'content', TIMESTAMPTZ 'infinity', TIMESTAMPTZ 'infinity', ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[])"
expect_failure 'nonfinite updated timestamp' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memories (id, version, type, title, content, created_at, updated_at, concepts, files, session_ids, source_observation_ids)
	VALUES ('memory-infinite-updated', 1, 'custom/fact', 'title', 'content', TIMESTAMPTZ '2026-01-05 00:00:00+00', TIMESTAMPTZ 'infinity', ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[])"
expect_failure 'inverted canonical timestamps' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memories (id, version, type, title, content, created_at, updated_at, concepts, files, session_ids, source_observation_ids)
	VALUES ('memory-inverted-times', 1, 'custom/fact', 'title', 'content', TIMESTAMPTZ '2026-01-06 00:00:00+00', TIMESTAMPTZ '2026-01-05 00:00:00+00', ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[])"
expect_failure 'missing concepts collection' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memories (id, version, type, title, content, created_at, updated_at, concepts, files, session_ids, source_observation_ids)
	VALUES ('memory-null-concepts', 1, 'custom/fact', 'title', 'content', TIMESTAMPTZ '2026-01-05 00:00:00+00', TIMESTAMPTZ '2026-01-05 00:00:00+00', NULL, ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[])"
expect_failure 'missing files collection' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memories (id, version, type, title, content, created_at, updated_at, concepts, files, session_ids, source_observation_ids)
	VALUES ('memory-null-files', 1, 'custom/fact', 'title', 'content', TIMESTAMPTZ '2026-01-05 00:00:00+00', TIMESTAMPTZ '2026-01-05 00:00:00+00', ARRAY[]::text[], NULL, ARRAY[]::text[], ARRAY[]::text[])"
expect_failure 'missing session IDs collection' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memories (id, version, type, title, content, created_at, updated_at, concepts, files, session_ids, source_observation_ids)
	VALUES ('memory-null-sessions', 1, 'custom/fact', 'title', 'content', TIMESTAMPTZ '2026-01-05 00:00:00+00', TIMESTAMPTZ '2026-01-05 00:00:00+00', ARRAY[]::text[], ARRAY[]::text[], NULL, ARRAY[]::text[])"
expect_failure 'missing source observation IDs collection' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memories (id, version, type, title, content, created_at, updated_at, concepts, files, session_ids, source_observation_ids)
	VALUES ('memory-null-observations', 1, 'custom/fact', 'title', 'content', TIMESTAMPTZ '2026-01-05 00:00:00+00', TIMESTAMPTZ '2026-01-05 00:00:00+00', ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], NULL)"
expect_equal \
	"$(application_query "SELECT count(*) FROM public.memories WHERE id IN ('memory-zero-version', 'memory-empty-type', 'memory-empty-title', 'memory-empty-content', 'memory-infinite-created', 'memory-infinite-updated', 'memory-inverted-times', 'memory-null-concepts', 'memory-null-files', 'memory-null-sessions', 'memory-null-observations')")" \
	'0' \
	'invalid canonical rows are absent'

expect_failure 'duplicate embedding' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memory_embeddings (id, version, embedding)
	VALUES ('memory-one', 1, '[6,8]'::vector)"
expect_failure 'orphan embedding' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memory_embeddings (id, version, embedding)
	VALUES ('memory-orphan', 1, '[3,4]'::vector)"
expect_failure 'empty embedding' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memory_embeddings (id, version, embedding)
	VALUES ('memory-unembedded', 1, '[]'::vector)"
expect_failure 'zero-norm embedding' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memory_embeddings (id, version, embedding)
	VALUES ('memory-unembedded', 1, '[0,0]'::vector)"
expect_failure 'nonfinite NaN embedding' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memory_embeddings (id, version, embedding)
	VALUES ('memory-unembedded', 1, '[NaN,1]'::vector)"
expect_failure 'nonfinite infinite embedding' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	INSERT INTO public.memory_embeddings (id, version, embedding)
	VALUES ('memory-unembedded', 1, '[Infinity,1]'::vector)"
expect_equal \
	"$(application_query "SELECT count(*) FROM public.memory_embeddings WHERE id IN ('memory-orphan', 'memory-unembedded')")" \
	'0' \
	'invalid or orphan embeddings are absent'

expect_failure 'application memory update' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	UPDATE public.memories SET title = 'updated' WHERE id = 'memory-unembedded' AND version = 1"
expect_failure 'application memory delete' "$MEMORY_POSTGRES_APPLICATION_ROLE" "
	DELETE FROM public.memories WHERE id = 'memory-unembedded' AND version = 1"
expect_failure 'application embedding truncate' "$MEMORY_POSTGRES_APPLICATION_ROLE" 'TRUNCATE public.memory_embeddings'
expect_equal \
	"$(application_query "SELECT title FROM public.memories WHERE id = 'memory-unembedded' AND version = 1")" \
	'title-unembedded' \
	'application write denials preserve canonical rows'

expect_failure 'restrictive parent update' "$MEMORY_POSTGRES_MIGRATION_ROLE" "
	UPDATE public.memories SET id = 'memory-one-renamed' WHERE id = 'memory-one' AND version = 1"
expect_failure 'restrictive parent delete' "$MEMORY_POSTGRES_MIGRATION_ROLE" "
	DELETE FROM public.memories WHERE id = 'memory-one' AND version = 1"
expect_equal \
	"$(migration_query "SELECT (SELECT count(*) FROM public.memories WHERE id = 'memory-one' AND version = 1) || '|' || (SELECT count(*) FROM public.memory_embeddings WHERE id = 'memory-one' AND version = 1)")" \
	'1|1' \
	'restrictive foreign key preserves parent and child'

expect_equal \
	"$(migration_query "SELECT count(*) FROM pg_attribute WHERE attrelid = 'public.memories'::regclass AND attname = 'embedding' AND NOT attisdropped")" \
	'0' \
	'no embedding column on memories'
expect_equal \
	"$(migration_query "SELECT count(*) FROM pg_constraint WHERE conrelid = 'public.memories'::regclass AND contype = 'f'")" \
	'0' \
	'no raw-ledger foreign key'
expect_equal \
	"$(migration_query "SELECT EXISTS (SELECT 1 FROM pg_partitioned_table WHERE partrelid IN ('public.memories'::regclass, 'public.memory_embeddings'::regclass)) || '|' || EXISTS (SELECT 1 FROM pg_class WHERE oid IN ('public.memories'::regclass, 'public.memory_embeddings'::regclass) AND relispartition)")" \
	'false|false' \
	'no memory table partitioning'
expect_equal \
	"$(migration_query "SELECT count(*) FROM pg_index AS table_index JOIN pg_attribute AS attribute ON attribute.attrelid = table_index.indrelid AND attribute.attnum = ANY (table_index.indkey) WHERE table_index.indrelid = 'public.memory_embeddings'::regclass AND attribute.attname = 'embedding'")" \
	'0' \
	'no vector index'
expect_equal \
	"$(migration_query "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'public.memories'::regclass AND NOT tgisinternal AND tgname = 'memory_search_heads_after_insert' AND tgfoid = 'public.memory_search_heads_after_insert()'::regprocedure AND tgtype::integer = 5")" \
	'1' \
	'current-head after-insert trigger'
expect_equal \
	"$(migration_query "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'public.memories'::regclass AND NOT tgisinternal AND tgname <> 'memory_search_heads_after_insert'")" \
	'0' \
	'no other memory triggers'
expect_equal \
	"$(migration_query "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'public.memory_embeddings'::regclass AND NOT tgisinternal")" \
	'0' \
	'no embedding triggers'
expect_equal \
	"$(migration_query "SELECT (SELECT count(*) FROM pg_policy WHERE polrelid IN ('public.memories'::regclass, 'public.memory_embeddings'::regclass, 'public.memory_search_heads'::regclass)) || '|' || (SELECT count(*) FROM pg_rewrite WHERE ev_class IN ('public.memories'::regclass, 'public.memory_embeddings'::regclass, 'public.memory_search_heads'::regclass))")" \
	'0|0' \
	'no storage policies or rules'

printf 'PostgreSQL %s memory storage server=%s\n' "$MEMORY_POSTGRES_EXPECTED_MAJOR" "$server_version"
printf 'memory migration transaction=applied role=%s\n' "$MEMORY_POSTGRES_MIGRATION_ROLE"
printf 'memory application privileges=select:true insert:true update:false delete:false truncate:false\n'
printf 'memory storage exclusions=heads:current vector-index:none raw-ledger-fk:none partitioning:none retention:none\n'
