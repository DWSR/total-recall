#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 0 ]]; then
	printf 'usage: verify-memory-head-recency.sh\n' >&2
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
	MEMORY_RECENCY_MIGRATION \
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
for migration in "$MEMORY_SCHEMA_MIGRATION" "$MEMORY_RECENCY_MIGRATION"; do
	if [[ ! -r "$migration" ]]; then
		printf 'missing memory migration: %s\n' "$migration" >&2
		exit 66
	fi
done

umask 077

fail() {
	printf 'memory head recency verification failed: %s\n' "$1" >&2
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

migration_query() {
	query_as "$MEMORY_POSTGRES_MIGRATION_ROLE" "$MEMORY_POSTGRES_DATABASE" "$1"
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
		fail "expected rejection: $description"
	fi

	printf 'rejected=%s\n' "$description"
}

expect_migration_failure() {
	local description=$1
	local sql=$2

	if migration_query "$sql" >/dev/null; then
		fail "expected rejection: $description"
	fi

	printf 'rejected=%s\n' "$description"
}

# Apply one migration inside one transaction. The recency migration relies on
# the surrounding transaction so a lock or statement timeout leaves no partial
# change behind.
run_migration() {
	local migration=$1

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
		--file="$migration" >/dev/null 2>&1
}

# An expected failure must abort the whole recency migration. PGOPTIONS carries
# the finite, or deliberately unbounded, lock and statement limits.
expect_recency_failure() {
	local description=$1
	local pgoptions=$2

	if PGOPTIONS="$pgoptions" run_migration "$MEMORY_RECENCY_MIGRATION"; then
		fail "expected recency migration rejection: $description"
	fi

	printf 'rejected=%s\n' "$description"
}

recency_column_count() {
	migration_query "SELECT count(*) FROM pg_attribute WHERE attrelid = 'public.memory_search_heads'::regclass AND attname = 'updated_at' AND NOT attisdropped"
}

insert_memory() {
	local id=$1
	local version=$2
	local title=$3
	local content=$4
	local created_at=$5
	local updated_at=$6

	application_execute "
		INSERT INTO public.memories (
			id, version, type, title, content, created_at, updated_at,
			concepts, files, session_ids, source_observation_ids
		) VALUES (
			'$id', $version, 'custom/fact', '$title', '$content',
			TIMESTAMPTZ '$created_at', TIMESTAMPTZ '$updated_at',
			ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[]
		)"
}

lock_holder_pid=''

cleanup() {
	local status=$?
	local pid=$lock_holder_pid

	trap - EXIT HUP INT TERM
	if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
		kill "$pid" 2>/dev/null || true
	fi
	if [[ -n "$pid" ]]; then
		wait "$pid" >/dev/null 2>&1 || true
	fi
	exit "$status"
}

trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# Hold an ACCESS SHARE lock from a separate session. ACCESS SHARE conflicts
# with the ACCESS EXCLUSIVE lock that ADD COLUMN needs, so the recency migration
# must wait for the finite lock_timeout and then roll back. The holder connects
# as the migration role so the verifier can terminate its own backend.
start_share_lock_holder() {
	PGAPPNAME='memory-head-recency-share-lock' "$psql" \
		--no-psqlrc \
		--no-password \
		--host="$MEMORY_POSTGRES_SOCKET_DIR" \
		--port="$PGPORT" \
		--username="$MEMORY_POSTGRES_MIGRATION_ROLE" \
		--dbname="$MEMORY_POSTGRES_DATABASE" \
		--set=ON_ERROR_STOP=1 \
		--tuples-only \
		--no-align \
		--quiet \
		--command="BEGIN ISOLATION LEVEL READ COMMITTED; SELECT count(*) FROM public.memory_search_heads; SELECT pg_sleep(30); COMMIT" >/dev/null 2>&1 &
	lock_holder_pid=$!
}

wait_for_share_lock() {
	local attempt
	local observed

	# Polling establishes ordering; the bound is only a diagnostic timeout.
	for ((attempt = 0; attempt < 200; attempt++)); do
		observed="$(migration_query "SELECT count(*) FROM pg_locks AS lock JOIN pg_stat_activity AS activity USING (pid) WHERE activity.application_name = 'memory-head-recency-share-lock' AND lock.locktype = 'relation' AND lock.relation = 'public.memory_search_heads'::regclass AND lock.mode = 'AccessShareLock' AND lock.granted")"
		if [[ "$observed" == '1' ]]; then
			return
		fi
		sleep 0.05
	done

	fail 'diagnostic timeout waiting for the blocking share lock'
}

stop_share_lock_holder() {
	local pid=$lock_holder_pid
	local attempt
	local observed

	lock_holder_pid=''
	migration_query "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'memory-head-recency-share-lock'" >/dev/null 2>&1 || true

	# Polling establishes ordering; the bound is only a diagnostic timeout.
	for ((attempt = 0; attempt < 200; attempt++)); do
		observed="$(migration_query "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'memory-head-recency-share-lock'")"
		if [[ "$observed" == '0' ]]; then
			break
		fi
		sleep 0.05
	done
	if [[ "$observed" != '0' ]]; then
		fail 'diagnostic timeout releasing the blocking share lock'
	fi
	if [[ -n "$pid" ]]; then
		wait "$pid" >/dev/null 2>&1 || true
	fi
}

run_migration "$MEMORY_SCHEMA_MIGRATION" || fail 'baseline migration failed'

server_version="$(application_query 'SHOW server_version')"
case "$server_version" in
"$MEMORY_POSTGRES_EXPECTED_MAJOR".*) ;;
*) fail "expected PostgreSQL $MEMORY_POSTGRES_EXPECTED_MAJOR, found $server_version" ;;
esac

# Seed a baseline with two families:
#  - exact:    greatest version (3) has an earlier canonical timestamp than the
#              lower historical version (2), proving backfill copies the exact
#              selected version rather than the newest timestamp.
#  - lower:    a single version that later gains a greater and a lower version.
insert_memory 'memory-recency-exact' 1 'exact v1 title' 'exact v1 content' \
	'2026-01-01 00:00:00+00' '2026-01-01 00:00:00+00'
insert_memory 'memory-recency-exact' 3 'exact v3 title' 'exact v3 content' \
	'2026-01-03 00:00:00+00' '2026-01-03 00:00:00+00'
insert_memory 'memory-recency-exact' 2 'exact v2 title' 'exact v2 content' \
	'2026-01-02 00:00:00+00' '2026-01-09 00:00:00+00'
insert_memory 'memory-recency-lower' 1 'lower v1 title' 'lower v1 content' \
	'2026-02-01 00:00:00+00' '2026-02-01 00:00:00+00'

expect_equal "$(recency_column_count)" '0' 'seeded baseline has no recency column'
expect_equal \
	"$(application_query "SELECT version FROM public.memory_search_heads WHERE id = 'memory-recency-exact'")" \
	'3' \
	'seeded baseline selects the greatest version'

# A missing finite statement limit must be refused before any schema change.
expect_recency_failure 'unbounded-window' '-c lock_timeout=0 -c statement_timeout=0'
expect_equal "$(recency_column_count)" '0' 'unbounded window leaves recency absent'

# A competing lock makes the recency migration wait. PostgreSQL counts that
# wait inside statement_timeout, so a finite statement deadline below the lock
# deadline aborts the uncommitted migration.
start_share_lock_holder
wait_for_share_lock
expect_recency_failure 'statement-timeout' '-c lock_timeout=60s -c statement_timeout=250ms'
expect_equal "$(recency_column_count)" '0' 'statement timeout leaves recency absent'

# A finite lock deadline below the statement deadline aborts the migration the
# same way before it commits.
expect_recency_failure 'lock-timeout' '-c lock_timeout=250ms -c statement_timeout=60s'
stop_share_lock_holder
expect_equal "$(recency_column_count)" '0' 'lock timeout leaves recency absent'
expect_equal \
	"$(application_query "SELECT count(*) FROM public.memory_search_heads WHERE id IN ('memory-recency-exact', 'memory-recency-lower')")" \
	'2' \
	'timeouts leave the seeded baseline heads intact'

# With finite limits the recency migration commits atomically.
if ! PGOPTIONS='-c lock_timeout=5s -c statement_timeout=60s' run_migration "$MEMORY_RECENCY_MIGRATION"; then
	fail 'recency migration failed under finite limits'
fi

expect_equal \
	"$(migration_query "SELECT format_type(atttypid, atttypmod) || '|' || attnotnull FROM pg_attribute WHERE attrelid = 'public.memory_search_heads'::regclass AND attname = 'updated_at' AND NOT attisdropped")" \
	'timestamp with time zone|true' \
	'recency column is validated non-null'
expect_equal \
	"$(migration_query "SELECT position('updated_at' in pg_get_functiondef('public.memory_search_heads_after_insert()'::regprocedure)) > 0")" \
	't' \
	'trigger maintenance now records recency'

expect_equal \
	"$(application_query "SELECT version || '|' || (updated_at = TIMESTAMPTZ '2026-01-03 00:00:00+00') FROM public.memory_search_heads WHERE id = 'memory-recency-exact'")" \
	'3|true' \
	'backfill copies the exact selected version timestamp'
expect_equal \
	"$(application_query "SELECT version || '|' || (updated_at = TIMESTAMPTZ '2026-02-01 00:00:00+00') FROM public.memory_search_heads WHERE id = 'memory-recency-lower'")" \
	'1|true' \
	'backfill copies the sole historical version timestamp'

insert_memory 'memory-recency-lower' 3 'lower v3 title' 'lower v3 content' \
	'2026-02-03 00:00:00+00' '2026-02-03 00:00:00+00'
expect_equal \
	"$(application_query "SELECT version || '|' || (updated_at = TIMESTAMPTZ '2026-02-03 00:00:00+00') FROM public.memory_search_heads WHERE id = 'memory-recency-lower'")" \
	'3|true' \
	'greater version advances recency'

insert_memory 'memory-recency-lower' 2 'lower v2 title' 'lower v2 content' \
	'2026-02-04 00:00:00+00' '2026-02-04 00:00:00+00'
expect_equal \
	"$(application_query "SELECT version || '|' || (updated_at = TIMESTAMPTZ '2026-02-03 00:00:00+00') FROM public.memory_search_heads WHERE id = 'memory-recency-lower'")" \
	'3|true' \
	'lower version keeps the existing head recency'

expect_equal \
	"$(application_query 'SELECT count(*) FROM public.memory_search_heads WHERE updated_at IS NULL')" \
	'0' \
	'every current head records recency'
expect_migration_failure 'null recency rejected' \
	"UPDATE public.memory_search_heads SET updated_at = NULL WHERE id = 'memory-recency-exact'"

expect_equal \
	"$(application_query "SELECT has_table_privilege(current_user, 'public.memory_search_heads', 'SELECT') || '|' || has_table_privilege(current_user, 'public.memory_search_heads', 'INSERT') || '|' || has_table_privilege(current_user, 'public.memory_search_heads', 'UPDATE') || '|' || has_table_privilege(current_user, 'public.memory_search_heads', 'DELETE') || '|' || has_table_privilege(current_user, 'public.memory_search_heads', 'TRUNCATE')")" \
	'true|false|false|false|false' \
	'application retains read-only head privileges'
expect_equal \
	"$(migration_query "SELECT has_function_privilege('$MEMORY_POSTGRES_APPLICATION_ROLE', 'public.memory_search_heads_after_insert()'::regprocedure, 'EXECUTE') || '|' || (SELECT count(*) = 0 FROM pg_proc AS routine CROSS JOIN LATERAL aclexplode(routine.proacl) AS privilege WHERE routine.oid = 'public.memory_search_heads_after_insert()'::regprocedure AND privilege.grantee = 0 AND privilege.privilege_type = 'EXECUTE')")" \
	'false|true' \
	'trigger execution stays denied to application and public'
expect_equal \
	"$(migration_query "SELECT prosecdef || '|' || pg_get_userbyid(proowner) || '|' || coalesce(array_to_string(proconfig, '|'), '') FROM pg_proc WHERE oid = 'public.memory_search_heads_after_insert()'::regprocedure")" \
	"true|$MEMORY_POSTGRES_MIGRATION_ROLE|search_path=pg_catalog, pg_temp" \
	'secured migration-owned trigger function is retained'
expect_application_failure 'application direct head insert' "
	INSERT INTO public.memory_search_heads (id, version, search_document, updated_at)
	VALUES ('memory-recency-direct-denial', 1, ARRAY['denied']::text[], now())"
expect_application_failure 'application direct head update' "
	UPDATE public.memory_search_heads SET updated_at = now() WHERE id = 'memory-recency-exact'"
expect_application_failure 'application direct trigger function execution' \
	'SELECT public.memory_search_heads_after_insert()'

expect_equal \
	"$(application_query 'SELECT count(*) FROM (SELECT id, version, search_document FROM public.memory_search_heads) AS legacy_reader')" \
	'2' \
	'old readers keep selecting the original head shape'

printf 'PostgreSQL %s memory head recency server=%s\n' "$MEMORY_POSTGRES_EXPECTED_MAJOR" "$server_version"
printf 'memory head recency window=finite-limits required unbounded:rejected statement-timeout:rollback lock-timeout:rollback\n'
printf 'memory head recency column=timestamptz not-null:true\n'
printf 'memory head recency backfill=exact-selected-version advance=greater-version lower:unchanged\n'
printf 'memory head recency privileges=application:select-only function:secured old-reader:compatible\n'
