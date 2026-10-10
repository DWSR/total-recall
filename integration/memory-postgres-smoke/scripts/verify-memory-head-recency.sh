#!/usr/bin/env bash
set -euo pipefail

for variable in MEMORY_POSTGRES_BIN MEMORY_POSTGRES_SOCKET_DIR MEMORY_POSTGRES_DATABASE MEMORY_POSTGRES_MIGRATION_ROLE MEMORY_POSTGRES_APPLICATION_ROLE MEMORY_POSTGRES_EXPECTED_MAJOR MEMORY_SCHEMA_MIGRATION MEMORY_RECENCY_MIGRATION MEMORY_RECENCY_INDEX_MIGRATION MEMORY_CONTEXT_ADAPTER_SOURCE PGPORT; do
	if [[ -z "${!variable:-}" ]]; then
		printf 'missing fixture environment variable: %s\n' "$variable" >&2
		exit 64
	fi
done

readonly psql="$MEMORY_POSTGRES_BIN/psql"
readonly migration_role="$MEMORY_POSTGRES_MIGRATION_ROLE"
readonly application_role="$MEMORY_POSTGRES_APPLICATION_ROLE"
readonly db="$MEMORY_POSTGRES_DATABASE"

fail() { printf 'memory head recency verification failed: %s\n' "$1" >&2; exit 1; }
query() {
	"$psql" --no-psqlrc --no-password --host="$MEMORY_POSTGRES_SOCKET_DIR" --port="$PGPORT" \
		--username="$1" --dbname="$db" --set=ON_ERROR_STOP=1 --tuples-only --no-align --quiet --command="$2"
}
expect() { [[ "$1" == "$2" ]] || fail "$3 (expected '$2', found '$1')"; }
expect_contains() { [[ "$1" == *"$2"* ]] || fail "$3 (expected output to contain '$2')"; }
context_sql() {
	local count="$1" bytes="$2"
	[[ "$count" =~ ^[1-9][0-9]*$ && "$bytes" =~ ^[1-9][0-9]*$ ]] || fail 'invalid fixture context SQL parameters'
	perl -0777 -e '
		local $/; my $s = <>;
		$s =~ /const SEARCH_CONTEXT_SQL: &str = r#"(.*?)"#;/s or die "SEARCH_CONTEXT_SQL source not found\n";
		my $sql = $1;
		$sql =~ s/\$1::text::bigint/'"$count"'/g;
		$sql =~ s/\$2::text::numeric/'"$bytes"'/g;
		print $sql;
	' "$MEMORY_CONTEXT_ADAPTER_SOURCE"
}
context_query() { query "$application_role" "$(context_sql "$1" "$2")"; }

"$psql" --no-psqlrc --no-password --host="$MEMORY_POSTGRES_SOCKET_DIR" --port="$PGPORT" \
	--username="$migration_role" --dbname="$db" --set=ON_ERROR_STOP=1 --single-transaction \
	--file="$MEMORY_SCHEMA_MIGRATION" >/dev/null

# Seed two identities, with a historical version beneath the selected head.
query "$application_role" "INSERT INTO public.memories (id,version,type,title,content,created_at,updated_at,concepts,files,session_ids,source_observation_ids) VALUES
 ('recency-a',1,'fact','a1','old',TIMESTAMPTZ '2026-01-01 00:00:00+00',TIMESTAMPTZ '2026-01-02 00:00:00+00',ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[]),
 ('recency-a',3,'fact','a3','selected',TIMESTAMPTZ '2026-01-03 00:00:00+00',TIMESTAMPTZ '2026-01-04 00:00:00+00',ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[]),
 ('recency-b',1,'fact','b1','selected',TIMESTAMPTZ '2026-01-01 00:00:00+00',TIMESTAMPTZ '2026-01-05 00:00:00+00',ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[])" >/dev/null

baseline="$(query "$migration_role" "SELECT string_agg(id||'|'||version||'|'||search_document[1], E'\\n' ORDER BY id) FROM public.memory_search_heads")"
expect "$(query "$migration_role" "SELECT version FROM public.memory_search_heads WHERE id='recency-a'")" '3' 'seeded greatest-version baseline'

apply_recency() {
	"$psql" --no-psqlrc --no-password --host="$MEMORY_POSTGRES_SOCKET_DIR" --port="$PGPORT" \
		--username="$migration_role" --dbname="$db" --set=ON_ERROR_STOP=1 --single-transaction --file="$1" >/dev/null 2>&1
}

# Lock timeout: hold ACCESS EXCLUSIVE while the actual finite-lock-timeout migration runs.
query "$migration_role" "BEGIN; LOCK TABLE public.memory_search_heads IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(4); COMMIT" >/dev/null &
lock_pid=$!
sleep 0.3
if apply_recency "$MEMORY_RECENCY_MIGRATION"; then wait "$lock_pid"; fail 'migration unexpectedly passed forced lock timeout'; fi
if ! wait "$lock_pid"; then fail 'lock-timeout fixture did not release its table lock cleanly'; fi
expect "$(query "$migration_role" "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='memory_search_heads' AND column_name='updated_at'")" '0' 'lock timeout rolled back schema change'
expect "$(query "$migration_role" "SELECT string_agg(id||'|'||version||'|'||search_document[1], E'\\n' ORDER BY id) FROM public.memory_search_heads")" "$baseline" 'lock timeout preserved baseline rows'

# Statement timeout: use an isolated copy with only the timeout reduced, so the
# exact migration statements and transaction boundary are exercised promptly.
timeout_migration="$(mktemp "${TMPDIR:-/tmp}/memory-recency-statement-timeout.XXXXXX.sql")"
trap 'rm -f -- "$timeout_migration"' EXIT
query "$migration_role" "CREATE FUNCTION public.memory_recency_timeout_test() RETURNS trigger LANGUAGE plpgsql AS \$\$ BEGIN PERFORM pg_catalog.pg_sleep(0.1); RETURN NEW; END; \$\$; CREATE TRIGGER memory_recency_timeout_test BEFORE UPDATE ON public.memory_search_heads FOR EACH ROW EXECUTE FUNCTION public.memory_recency_timeout_test()" >/dev/null
perl -pe "s/SET LOCAL statement_timeout = '30s'/SET LOCAL statement_timeout = '1ms'/" "$MEMORY_RECENCY_MIGRATION" > "$timeout_migration"
if apply_recency "$timeout_migration"; then fail 'migration unexpectedly passed forced statement timeout'; fi
query "$migration_role" 'DROP TRIGGER memory_recency_timeout_test ON public.memory_search_heads; DROP FUNCTION public.memory_recency_timeout_test()' >/dev/null
expect "$(query "$migration_role" "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='memory_search_heads' AND column_name='updated_at'")" '0' 'statement timeout rolled back schema change'
expect "$(query "$migration_role" "SELECT string_agg(id||'|'||version||'|'||search_document[1], E'\\n' ORDER BY id) FROM public.memory_search_heads")" "$baseline" 'statement timeout preserved baseline rows'

apply_recency "$MEMORY_RECENCY_MIGRATION" || fail 'recency migration failed on successful path'
expect "$(query "$migration_role" "SELECT string_agg(id||'|'||version||'|'||(updated_at AT TIME ZONE 'UTC')::text, E'\\n' ORDER BY id) FROM public.memory_search_heads")" \
	$'recency-a|3|2026-01-04 00:00:00\nrecency-b|1|2026-01-05 00:00:00' 'backfill selected exact canonical versions'
expect "$(query "$migration_role" "SELECT count(*) FROM pg_attribute WHERE attrelid='public.memory_search_heads'::regclass AND attname='updated_at' AND attnotnull")" '1' 'recency is non-null'
expect "$(query "$application_role" "SELECT version||'|'||search_document[1] FROM public.memory_search_heads WHERE id='recency-a'")" '3|a3' 'old reader query remains compatible'
expect "$(query "$application_role" "SELECT has_table_privilege(current_user,'public.memory_search_heads','SELECT')||'|'||has_table_privilege(current_user,'public.memory_search_heads','INSERT')||'|'||has_table_privilege(current_user,'public.memory_search_heads','UPDATE')||'|'||has_table_privilege(current_user,'public.memory_search_heads','DELETE')||'|'||has_table_privilege(current_user,'public.memory_search_heads','TRUNCATE')")" 'true|false|false|false|false' 'head table privileges preserved'
expect "$(query "$migration_role" "SELECT count(*) FROM pg_class relation CROSS JOIN LATERAL aclexplode(relation.relacl) privilege WHERE relation.oid='public.memory_search_heads'::regclass AND privilege.grantee=0")" '0' 'public head table privileges remain revoked'
expect "$(query "$migration_role" "SELECT prosecdef||'|'||pg_get_userbyid(proowner)||'|'||coalesce(array_to_string(proconfig,'|'),'') FROM pg_proc WHERE oid='public.memory_search_heads_after_insert()'::regprocedure")" \
	"true|$migration_role|search_path=pg_catalog, pg_temp" 'secured trigger function ownership and search path'
expect "$(query "$migration_role" "SELECT has_function_privilege('$application_role','public.memory_search_heads_after_insert()'::regprocedure,'EXECUTE')||'|'||(SELECT count(*)=0 FROM pg_proc r CROSS JOIN LATERAL aclexplode(r.proacl) a WHERE r.oid='public.memory_search_heads_after_insert()'::regprocedure AND a.grantee=0 AND a.privilege_type='EXECUTE')")" 'false|true' 'trigger function privileges preserved'

# New greater versions advance recency; lower versions cannot replace it.
query "$application_role" "INSERT INTO public.memories (id,version,type,title,content,created_at,updated_at,concepts,files,session_ids,source_observation_ids) VALUES ('recency-a',4,'fact','a4','new',TIMESTAMPTZ '2026-01-07 00:00:00+00',TIMESTAMPTZ '2026-01-08 00:00:00+00',ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[])" >/dev/null
expect "$(query "$application_role" "SELECT version||'|'||(updated_at AT TIME ZONE 'UTC')::text FROM public.memory_search_heads WHERE id='recency-a'")" '4|2026-01-08 00:00:00' 'greater version advances recency'
query "$application_role" "INSERT INTO public.memories (id,version,type,title,content,created_at,updated_at,concepts,files,session_ids,source_observation_ids) VALUES ('recency-a',2,'fact','a2','old',TIMESTAMPTZ '2026-01-09 00:00:00+00',TIMESTAMPTZ '2026-01-10 00:00:00+00',ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[])" >/dev/null
expect "$(query "$application_role" "SELECT version||'|'||(updated_at AT TIME ZONE 'UTC')::text FROM public.memory_search_heads WHERE id='recency-a'")" '4|2026-01-08 00:00:00' 'lower version does not advance recency'

# Writers resume after transactional 0002 and before standalone 0003.
query "$application_role" "INSERT INTO public.memories (id,version,type,title,content,created_at,updated_at,concepts,files,session_ids,source_observation_ids) VALUES ('recency-c',1,'fact','c1','writer resumed',TIMESTAMPTZ '2026-01-11 00:00:00+00',TIMESTAMPTZ '2026-01-12 00:00:00+00',ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[])" >/dev/null
expect "$(query "$application_role" "SELECT version FROM public.memory_search_heads WHERE id='recency-c'")" '1' 'writer remains available after 0002'

# Cause a real concurrent build failure while evaluating an indexed expression;
# PostgreSQL leaves its catalog entry invalid for explicit concurrent cleanup.
query "$migration_role" "CREATE FUNCTION public.memory_recency_index_failure(TIMESTAMPTZ) RETURNS TIMESTAMPTZ LANGUAGE plpgsql IMMUTABLE AS \$\$ BEGIN RAISE EXCEPTION 'forced index build failure'; END; \$\$" >/dev/null
if query "$migration_role" "CREATE INDEX CONCURRENTLY memory_search_heads_context_recency_idx ON public.memory_search_heads (public.memory_recency_index_failure(updated_at))" >/dev/null 2>&1; then
	fail 'forced concurrent index build unexpectedly succeeded'
fi
expect "$(query "$migration_role" "SELECT indisvalid FROM pg_index WHERE indexrelid='public.memory_search_heads_context_recency_idx'::regclass")" 'f' 'failed concurrent build leaves invalid index'
query "$migration_role" 'DROP INDEX CONCURRENTLY public.memory_search_heads_context_recency_idx' >/dev/null
query "$migration_role" 'DROP FUNCTION public.memory_recency_index_failure(TIMESTAMPTZ)' >/dev/null

# The ordered migration is standalone (not wrapped in a transaction) and can be
# retried after invalid-index cleanup while application writers remain active.
"$psql" --no-psqlrc --no-password --host="$MEMORY_POSTGRES_SOCKET_DIR" --port="$PGPORT" \
	--username="$migration_role" --dbname="$db" --set=ON_ERROR_STOP=1 \
	--file="$MEMORY_RECENCY_INDEX_MIGRATION" >/dev/null
query "$application_role" "INSERT INTO public.memories (id,version,type,title,content,created_at,updated_at,concepts,files,session_ids,source_observation_ids) VALUES ('recency-d',1,'fact','d1','writer during index-ready state',TIMESTAMPTZ '2026-01-13 00:00:00+00',TIMESTAMPTZ '2026-01-14 00:00:00+00',ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[])" >/dev/null
expect "$(query "$migration_role" "SELECT indisvalid||'|'||pg_get_indexdef(indexrelid) FROM pg_index WHERE indexrelid='public.memory_search_heads_context_recency_idx'::regclass")" \
	'true|CREATE INDEX memory_search_heads_context_recency_idx ON public.memory_search_heads USING btree (updated_at DESC, id COLLATE "C", version DESC)' 'ordered concurrent index is valid'
expect "$(query "$application_role" "SELECT version FROM public.memory_search_heads WHERE id='recency-d'")" '1' 'writer remains available after concurrent index build'

# Execute the exact SEARCH_CONTEXT_SQL source consumed by the production adapter.
# Only its two static positional parameters are replaced, with fixture-validated
# positive decimal literals; psql executes the resulting production query.
expect "$(query "$application_role" "SELECT has_table_privilege(current_user,'public.memories','SELECT')||'|'||has_table_privilege(current_user,'public.memory_search_heads','SELECT')")" \
	'true|true' 'context reader can read canonical memories and heads'
query "$application_role" "INSERT INTO public.memories (id,version,type,title,content,created_at,updated_at,concepts,files,session_ids,source_observation_ids) VALUES
	('tie-z',1,'fact','z1','tie z',TIMESTAMPTZ '2026-01-14 00:00:00+00',TIMESTAMPTZ '2026-01-15 00:00:00+00',ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[]),
	('tie-a',1,'fact','a1','tie a',TIMESTAMPTZ '2026-01-14 00:00:00+00',TIMESTAMPTZ '2026-01-15 00:00:00+00',ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[])" >/dev/null
expect "$(context_query 2 1000 | wc -l | tr -d ' ')" '2' 'production context query returns exact requested row count'
expect "$(context_query 2 1000 | awk -F'|' 'NR==1 {line1=$0} NR==2 {line2=$0} NR==1 && $1=="f" && $2=="fact" && $3=="a1" && $4=="tie a" && $5=="[]" {first=1} NR==2 && $1=="f" && $2=="fact" && $3=="z1" && $4=="tie z" && $5=="[]" {second=1} END {if (NR==2 && first && second) print "complete ordered records"; else print "invalid records: " line1 " / " line2}')" \
	'complete ordered records' 'production context query returns complete narrow fields in deterministic tie order'
sentinel="$(context_query 2 1)"
[[ "$sentinel" =~ ^t\|\|\|\|\|[1-9][0-9]*$ ]] || fail "production query oversized sentinel did not withhold all record fields (found '$sentinel')"
expect "$(context_query 2 1 | wc -l | tr -d ' ')" '1' 'oversized sentinel terminates the returned candidate prefix'
empty_rows="$(query "$migration_role" "BEGIN; TRUNCATE public.memories CASCADE; $(context_sql 2 1000); ROLLBACK")"
expect "$(printf '%s' "$empty_rows" | wc -l | tr -d ' ')" '0' 'empty production context query returns no rows'

# Force only the scan method choice so the assertion is about index availability,
# not the planner's surrounding join or sort shape.
expect_contains "$(query "$application_role" "SET enable_seqscan=off; EXPLAIN (COSTS OFF) $(context_sql 2 1000)")" \
	'memory_search_heads_context_recency_idx' 'populated production bounded context query has recency index available'

printf 'PostgreSQL %s memory head recency migration=atomic backfill=exact-head privileges=preserved old-reader=compatible lock-timeout=rollback statement-timeout=rollback concurrent-index=recovered writers=available context-order=verified context-index=available\n' "$MEMORY_POSTGRES_EXPECTED_MAJOR"
