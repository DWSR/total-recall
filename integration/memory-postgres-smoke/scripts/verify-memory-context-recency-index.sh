#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 0 ]]; then
	printf 'usage: verify-memory-context-recency-index.sh\n' >&2
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
	MEMORY_CONTEXT_INDEX_MIGRATION \
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
for migration in \
	"$MEMORY_SCHEMA_MIGRATION" \
	"$MEMORY_RECENCY_MIGRATION" \
	"$MEMORY_CONTEXT_INDEX_MIGRATION"; do
	if [[ ! -r "$migration" ]]; then
		printf 'missing memory migration: %s\n' "$migration" >&2
		exit 66
	fi
done

umask 077

readonly index_name='memory_search_heads_context_recency_idx'
readonly index_relation="public.$index_name"

fail() {
	printf 'memory context recency index verification failed: %s\n' "$1" >&2
	exit 1
}

expect_equal() {
	local actual=$1
	local expected=$2
	local description=$3

	[[ "$actual" == "$expected" ]] || fail "$description: expected $expected, found $actual"
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

# The baseline and the recency change stay strictly transactional. Any failure
# inside the wrapper must leave no partial schema change behind.
run_transactional_migration() {
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

# The standalone index build must run outside every transaction wrapper.
run_standalone_migration() {
	local migration=$1

	"$psql" \
		--no-psqlrc \
		--no-password \
		--host="$MEMORY_POSTGRES_SOCKET_DIR" \
		--port="$PGPORT" \
		--username="$MEMORY_POSTGRES_MIGRATION_ROLE" \
		--dbname="$MEMORY_POSTGRES_DATABASE" \
		--set=ON_ERROR_STOP=1 \
		--quiet \
		--file="$migration" >/dev/null 2>&1
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

index_present() {
	migration_query "
		SELECT count(*)
		FROM pg_class AS relation
		JOIN pg_namespace AS schema ON schema.oid = relation.relnamespace
		WHERE schema.nspname = 'public'
			AND relation.relname = '$index_name'
			AND relation.relkind = 'i'"
}

index_valid() {
	migration_query "
		SELECT index.indisvalid::text
		FROM pg_index AS index
		JOIN pg_class AS relation ON relation.oid = index.indexrelid
		JOIN pg_namespace AS schema ON schema.oid = relation.relnamespace
		WHERE schema.nspname = 'public'
			AND relation.relname = '$index_name'"
}

snapshot_holder_pid=''
build_pid=''
build_log=''

cleanup() {
	local status=$?
	local pid

	trap - EXIT HUP INT TERM
	for pid in "$build_pid" "$snapshot_holder_pid"; do
		if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
			kill "$pid" 2>/dev/null || true
		fi
		if [[ -n "$pid" ]]; then
			wait "$pid" >/dev/null 2>&1 || true
		fi
	done
	if [[ -n "$build_log" ]]; then
		rm -f -- "$build_log"
	fi
	exit "$status"
}

trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# A repeatable-read transaction keeps its snapshot until it commits, so the
# concurrent build must wait for it. ACCESS SHARE does not conflict with the
# concurrent build's lock, so resumed memory writers remain unblocked.
start_snapshot_holder() {
	PGAPPNAME='memory-context-index-snapshot' "$psql" \
		--no-psqlrc \
		--no-password \
		--host="$MEMORY_POSTGRES_SOCKET_DIR" \
		--port="$PGPORT" \
		--username="$MEMORY_POSTGRES_MIGRATION_ROLE" \
		--dbname="$MEMORY_POSTGRES_DATABASE" \
		--set=ON_ERROR_STOP=1 \
		--command="BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM public.memory_search_heads; SELECT pg_sleep(30); COMMIT" >/dev/null 2>&1 &
	snapshot_holder_pid=$!
}

wait_for_snapshot_holder() {
	local attempt
	local observed

	# Polling establishes ordering; the bound is only a diagnostic timeout.
	for ((attempt = 0; attempt < 200; attempt++)); do
		observed="$(migration_query "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'memory-context-index-snapshot' AND query LIKE '%pg_sleep%' AND state = 'active'")"
		if [[ "$observed" == '1' ]]; then
			return
		fi
		sleep 0.05
	done

	fail 'diagnostic timeout waiting for the snapshot holder'
}

stop_snapshot_holder() {
	local pid=$snapshot_holder_pid
	local attempt
	local observed

	snapshot_holder_pid=''
	migration_query "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'memory-context-index-snapshot'" >/dev/null 2>&1 || true

	# Polling establishes ordering; the bound is only a diagnostic timeout.
	for ((attempt = 0; attempt < 200; attempt++)); do
		observed="$(migration_query "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'memory-context-index-snapshot'")"
		if [[ "$observed" == '0' ]]; then
			break
		fi
		sleep 0.05
	done
	if [[ "$observed" != '0' ]]; then
		fail 'diagnostic timeout releasing the snapshot holder'
	fi
	if [[ -n "$pid" ]]; then
		wait "$pid" >/dev/null 2>&1 || true
	fi
}

start_index_build() {
	build_log="$(mktemp "${TMPDIR:-/tmp}/memory-context-index-build.XXXXXX")"
	PGAPPNAME='memory-context-index-build' "$psql" \
		--no-psqlrc \
		--no-password \
		--host="$MEMORY_POSTGRES_SOCKET_DIR" \
		--port="$PGPORT" \
		--username="$MEMORY_POSTGRES_MIGRATION_ROLE" \
		--dbname="$MEMORY_POSTGRES_DATABASE" \
		--set=ON_ERROR_STOP=1 \
		--quiet \
		--file="$MEMORY_CONTEXT_INDEX_MIGRATION" >"$build_log" 2>&1 &
	build_pid=$!
}

wait_for_index_build_lock() {
	local attempt
	local observed

	# Polling establishes ordering; the bound is only a diagnostic timeout.
	for ((attempt = 0; attempt < 400; attempt++)); do
		observed="$(migration_query "SELECT wait_event_type FROM pg_stat_activity WHERE application_name = 'memory-context-index-build' AND query LIKE '%CREATE INDEX CONCURRENTLY%' AND state = 'active'")"
		if [[ "$observed" == 'Lock' ]]; then
			return
		fi
		if [[ -n "$observed" ]]; then
			if ! kill -0 "$build_pid" 2>/dev/null; then
				break
			fi
		fi
		sleep 0.05
	done

	if [[ -s "$build_log" ]]; then
		fail 'index build exited before waiting for resumed writers'
	fi
	fail 'diagnostic timeout waiting for the concurrent index build'
}

wait_for_index_build() {
	local status

	set +e
	wait "$build_pid"
	status=$?
	set -e
	build_pid=''
	if [[ "$status" -ne 0 ]]; then
		if [[ -s "$build_log" ]]; then
			printf 'memory context recency index build output:\n' >&2
			sed -n '1,20p' "$build_log" >&2
		fi
		fail 'concurrent index build failed'
	fi
}

run_transactional_migration "$MEMORY_SCHEMA_MIGRATION" || fail 'baseline migration failed'

server_version="$(application_query 'SHOW server_version')"
case "$server_version" in
"$MEMORY_POSTGRES_EXPECTED_MAJOR".*) ;;
*) fail "expected PostgreSQL $MEMORY_POSTGRES_EXPECTED_MAJOR, found $server_version" ;;
esac

# The transactional recency migration keeps its original mode and finite
# limits, then writers resume normally for the standalone index build.
if ! PGOPTIONS='-c lock_timeout=5s -c statement_timeout=60s' run_transactional_migration "$MEMORY_RECENCY_MIGRATION"; then
	fail 'transactional recency migration failed'
fi

# Seed rows representing resumed writers. The final prefix proves recency DESC,
# bytewise identity ASC, and deterministic ties without any lexical search.
insert_memory 'memory-index-A' 1 'upper title' 'upper content' \
	'2026-01-01 00:00:00+00' '2026-03-02 00:00:00+00'
insert_memory 'memory-index-_' 1 'underscore title' 'underscore content' \
	'2026-01-01 00:00:00+00' '2026-03-02 00:00:00+00'
insert_memory 'memory-index-a' 1 'lower title' 'lower content' \
	'2026-01-01 00:00:00+00' '2026-03-02 00:00:00+00'
insert_memory 'memory-index-b' 1 'second lower title' 'second lower content' \
	'2026-01-01 00:00:00+00' '2026-03-02 00:00:00+00'
insert_memory 'memory-index-z' 1 'oldest title' 'oldest content' \
	'2026-01-01 00:00:00+00' '2026-03-01 00:00:00+00'

expect_equal "$(index_present)" '0' 'index is absent before the standalone migration'

# CREATE INDEX CONCURRENTLY is rejected inside a transaction wrapper. The
# rejection must not leave an index entry behind.
if run_transactional_migration "$MEMORY_CONTEXT_INDEX_MIGRATION"; then
	fail 'expected transaction-wrapped concurrent index rejection'
fi
printf 'rejected=transaction-wrapped-concurrent-build\n'
expect_equal "$(index_present)" '0' 'transaction rejection leaves the index absent'

# Simulate a failed concurrent build with the target name. A unique build over
# duplicate values fails during validation and leaves an invalid index entry.
if PGAPPNAME='memory-context-index-invalid-build' "$psql" \
	--no-psqlrc \
	--no-password \
	--host="$MEMORY_POSTGRES_SOCKET_DIR" \
	--port="$PGPORT" \
	--username="$MEMORY_POSTGRES_MIGRATION_ROLE" \
	--dbname="$MEMORY_POSTGRES_DATABASE" \
	--set=ON_ERROR_STOP=1 \
	--quiet \
	--command="CREATE UNIQUE INDEX CONCURRENTLY $index_name ON public.memory_search_heads ((version > 0))" >/dev/null 2>&1; then
	fail 'expected simulated concurrent index build failure'
fi
printf 'failed-build=invalid-index-left\n'
expect_equal "$(index_present)" '1' 'failed concurrent build leaves an index entry'
expect_equal "$(index_valid)" 'false' 'failed concurrent build leaves the index invalid'

# The migration must not silently replace an invalid index. Recovery is explicit.
if run_standalone_migration "$MEMORY_CONTEXT_INDEX_MIGRATION"; then
	fail 'expected invalid-index collision rejection'
fi
printf 'rejected=invalid-index-present\n'
expect_equal "$(index_valid)" 'false' 'rerunning the migration leaves the invalid index untouched'

# Documented recovery: drop the invalid index concurrently, then rerun 0003.
PGAPPNAME='memory-context-index-recovery' "$psql" \
	--no-psqlrc \
	--no-password \
	--host="$MEMORY_POSTGRES_SOCKET_DIR" \
	--port="$PGPORT" \
	--username="$MEMORY_POSTGRES_MIGRATION_ROLE" \
	--dbname="$MEMORY_POSTGRES_DATABASE" \
	--set=ON_ERROR_STOP=1 \
	--quiet \
	--command="DROP INDEX CONCURRENTLY IF EXISTS $index_relation" >/dev/null
expect_equal "$(index_present)" '0' 'explicit recovery removes the invalid index'
printf 'recovered=drop-invalid-index-concurrently\n'

# Resume writers, block the build on an older snapshot, and prove an
# application insert still commits while the concurrent build waits.
start_snapshot_holder
wait_for_snapshot_holder
start_index_build
wait_for_index_build_lock
PGOPTIONS='-c statement_timeout=5s' insert_memory 'memory-index-writer' 1 \
	'writer title' 'writer content' \
	'2026-01-01 00:00:00+00' '2026-04-01 00:00:00+00'
expect_equal \
	"$(application_query "SELECT count(*) FROM public.memories WHERE id = 'memory-index-writer'")" \
	'1' \
	'resumed writer commits while the concurrent build waits'
printf 'writers=inserted-during-concurrent-build\n'
stop_snapshot_holder
wait_for_index_build

expect_equal "$(index_present)" '1' 'standalone migration creates the index'
expect_equal "$(index_valid)" 'true' 'standalone migration leaves the index valid'
expect_equal \
	"$(migration_query "SELECT index.indisvalid::text || '|' || index.indisready::text || '|' || index.indislive::text || '|' || index.indisunique::text || '|' || index.indisprimary::text FROM pg_index AS index WHERE index.indexrelid = '$index_relation'::regclass")" \
	'true|true|true|false|false' \
	'index is a live, ready, valid, non-unique, non-primary btree'
expect_equal \
	"$(migration_query "SELECT access_method.amname FROM pg_class AS relation JOIN pg_am AS access_method ON access_method.oid = relation.relam WHERE relation.oid = '$index_relation'::regclass")" \
	'btree' \
	'index uses the btree access method'
expect_equal \
	"$(migration_query "SELECT pg_get_indexdef('$index_relation'::regclass)")" \
	'CREATE INDEX memory_search_heads_context_recency_idx ON public.memory_search_heads USING btree (updated_at DESC, id COLLATE "C", version DESC)' \
	'index definition uses recency, bytewise identity, and version order'
expect_equal \
	"$(migration_query "SELECT index.indnkeyatts FROM pg_index AS index WHERE index.indexrelid = '$index_relation'::regclass")" \
	'3' \
	'index has exactly three key columns'

# The ordered prefix is deterministic and demonstrates that bytewise identity
# ordering runs before the older row.
expect_equal \
	"$(application_query "SELECT string_agg(id, '|') FROM (SELECT id FROM public.memory_search_heads ORDER BY updated_at DESC, id COLLATE \"C\" ASC, version DESC LIMIT 4) AS ordered")" \
	'memory-index-writer|memory-index-A|memory-index-_|memory-index-a' \
	'ordered prefix follows recency and bytewise identity'

# The populated bounded read must be able to use the index. Avoid asserting a
# complete planner shape; only require the recency index in the ordered scan.
plan="$(PGOPTIONS='-c enable_seqscan=off -c enable_bitmapscan=off' application_query "EXPLAIN (COSTS OFF) SELECT updated_at, id, version FROM public.memory_search_heads ORDER BY updated_at DESC, id COLLATE \"C\" ASC, version DESC LIMIT 5")"
case "$plan" in
*"Index Scan using $index_name on"* | *"Index Only Scan using $index_name on"*) ;;
*) fail 'ordered bounded read did not use the recency index' ;;
esac
printf 'indexed-plan=ordered-bounded-read\n'

# The transactional path and existing privileges remain intact.
expect_equal \
	"$(migration_query "SELECT format_type(atttypid, atttypmod) || '|' || attnotnull FROM pg_attribute WHERE attrelid = 'public.memory_search_heads'::regclass AND attname = 'updated_at' AND NOT attisdropped")" \
	'timestamp with time zone|true' \
	'recency column stays validated non-null'
expect_equal \
	"$(application_query "SELECT has_table_privilege(current_user, 'public.memory_search_heads', 'SELECT') || '|' || has_table_privilege(current_user, 'public.memory_search_heads', 'INSERT') || '|' || has_table_privilege(current_user, 'public.memory_search_heads', 'UPDATE') || '|' || has_table_privilege(current_user, 'public.memory_search_heads', 'DELETE')")" \
	'true|false|false|false' \
	'application retains read-only head privileges'
expect_application_failure 'application index creation' \
	"CREATE INDEX memory_search_heads_application_denied_idx ON public.memory_search_heads (version)"
expect_application_failure 'application index removal' \
	"DROP INDEX public.$index_name"
expect_equal \
	"$(application_query 'SELECT count(*) FROM (SELECT id, version, search_document FROM public.memory_search_heads) AS legacy_reader')" \
	'6' \
	'old readers keep selecting the original head shape'

printf 'PostgreSQL %s memory context recency index server=%s\n' "$MEMORY_POSTGRES_EXPECTED_MAJOR" "$server_version"
printf 'memory context recency index migration=non-transactional concurrent-build\n'
printf 'memory context recency index recovery=drop-invalid-index-concurrently rerun:valid\n'
printf 'memory context recency index order=updated_at-desc id-c-asc version-desc writers:unblocked\n'
