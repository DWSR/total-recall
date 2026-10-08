#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 0 ]]; then
	printf 'usage: verify-memory-version-heads.sh\n' >&2
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

umask 077

fail() {
	printf 'memory version head verification failed: %s\n' "$1" >&2
	exit 1
}

progress() {
	printf 'memory version heads progress=%s\n' "$1" >&2
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
	local created_at=$5
	local updated_at=$6
	local concepts=$7
	local files=$8
	local session_ids=$9
	local source_observation_ids=${10}

	application_execute "
		INSERT INTO public.memories (
			id, version, type, title, content, created_at, updated_at,
			concepts, files, session_ids, source_observation_ids
		) VALUES (
			'$id', $version, 'custom/fact', '$title', '$content',
			TIMESTAMPTZ '$created_at', TIMESTAMPTZ '$updated_at',
			$concepts, $files, $session_ids, $source_observation_ids
		)"
}

assert_canonical_state() {
	local id=$1
	local expected_snapshots=$2
	local expected_count=$3
	local head_version=$4
	local head_document=$5
	local description=$6

	expect_equal \
		"$(application_query "
			WITH expected (
				id, version, type, title, content, created_at, updated_at,
				concepts, files, session_ids, source_observation_ids
			) AS (
				$expected_snapshots
			)
			SELECT
				(SELECT count(*) FROM public.memories WHERE id = '$id') || '|' ||
				(SELECT count(*)
				 FROM expected
				 JOIN public.memories AS actual
					ON actual.id = expected.id
					AND actual.version = expected.version
					AND actual.type = expected.type
					AND actual.title = expected.title
					AND actual.content = expected.content
					AND actual.created_at = expected.created_at
					AND actual.updated_at = expected.updated_at
					AND actual.concepts = expected.concepts
					AND actual.files = expected.files
					AND actual.session_ids = expected.session_ids
					AND actual.source_observation_ids = expected.source_observation_ids) || '|' ||
				(SELECT count(*) FROM public.memory_search_heads WHERE id = '$id') || '|' ||
				(SELECT count(*)
				 FROM public.memory_search_heads
				 WHERE id = '$id'
					AND version = $head_version
					AND search_document = $head_document)")" \
		"$expected_count|$expected_count|1|1" \
		"$description"
}

readonly sequential_snapshots="
VALUES
	('memory-version-sequential', 1::bigint, 'custom/fact', 'sequential v1 title', 'sequential v1 content', TIMESTAMPTZ '2026-03-01 00:00:00+00', TIMESTAMPTZ '2026-03-01 00:01:00+00', ARRAY['sequential-v1-concept-a', 'sequential-v1-concept-b']::text[], ARRAY['sequential-v1-file-a', 'sequential-v1-file-b']::text[], ARRAY['sequential-v1-session-a']::text[], ARRAY['sequential-v1-observation-a', 'sequential-v1-observation-b']::text[]),
	('memory-version-sequential', 2::bigint, 'custom/fact', 'sequential v2 title', 'sequential v2 content', TIMESTAMPTZ '2026-03-02 00:00:00+00', TIMESTAMPTZ '2026-03-02 00:02:00+00', ARRAY['sequential-v2-concept-a', 'sequential-v2-concept-b']::text[], ARRAY['sequential-v2-file-a', 'sequential-v2-file-b']::text[], ARRAY['sequential-v2-session-a']::text[], ARRAY['sequential-v2-observation-a', 'sequential-v2-observation-b']::text[]),
	('memory-version-sequential', 3::bigint, 'custom/fact', 'sequential v3 title', 'sequential v3 content', TIMESTAMPTZ '2026-03-03 00:00:00+00', TIMESTAMPTZ '2026-03-03 00:03:00+00', ARRAY['sequential-v3-concept-a', 'sequential-v3-concept-b']::text[], ARRAY['sequential-v3-file-a', 'sequential-v3-file-b']::text[], ARRAY['sequential-v3-session-a']::text[], ARRAY['sequential-v3-observation-a', 'sequential-v3-observation-b']::text[])
"

readonly concurrent_snapshots="
VALUES
	('memory-version-concurrent', 1::bigint, 'custom/fact', 'concurrent v1 title', 'concurrent v1 content', TIMESTAMPTZ '2026-04-01 00:00:00+00', TIMESTAMPTZ '2026-04-01 00:01:00+00', ARRAY['concurrent-v1-concept-a', 'concurrent-v1-concept-b']::text[], ARRAY['concurrent-v1-file-a', 'concurrent-v1-file-b']::text[], ARRAY['concurrent-v1-session-a']::text[], ARRAY['concurrent-v1-observation-a', 'concurrent-v1-observation-b']::text[]),
	('memory-version-concurrent', 2::bigint, 'custom/fact', 'concurrent v2 title', 'concurrent v2 content', TIMESTAMPTZ '2026-04-02 00:00:00+00', TIMESTAMPTZ '2026-04-02 00:02:00+00', ARRAY['concurrent-v2-concept-a', 'concurrent-v2-concept-b']::text[], ARRAY['concurrent-v2-file-a', 'concurrent-v2-file-b']::text[], ARRAY['concurrent-v2-session-a']::text[], ARRAY['concurrent-v2-observation-a', 'concurrent-v2-observation-b']::text[]),
	('memory-version-concurrent', 3::bigint, 'custom/fact', 'concurrent v3 title', 'concurrent v3 content', TIMESTAMPTZ '2026-04-03 00:00:00+00', TIMESTAMPTZ '2026-04-03 00:03:00+00', ARRAY['concurrent-v3-concept-a', 'concurrent-v3-concept-b']::text[], ARRAY['concurrent-v3-file-a', 'concurrent-v3-file-b']::text[], ARRAY['concurrent-v3-session-a']::text[], ARRAY['concurrent-v3-observation-a', 'concurrent-v3-observation-b']::text[])
"

readonly rollback_snapshots="
VALUES
	('memory-version-trigger-rollback', 1::bigint, 'custom/fact', 'rollback v1 title', 'rollback v1 content', TIMESTAMPTZ '2026-05-01 00:00:00+00', TIMESTAMPTZ '2026-05-01 00:01:00+00', ARRAY['rollback-v1-concept-a', 'rollback-v1-concept-b']::text[], ARRAY['rollback-v1-file-a', 'rollback-v1-file-b']::text[], ARRAY['rollback-v1-session-a']::text[], ARRAY['rollback-v1-observation-a', 'rollback-v1-observation-b']::text[])
"

race_dir=''
race_fifo=''
race_fifo_fd=''
race_fifo_open=false
gate_pid=''
v3_pid=''
v2_pid=''

cleanup_race() {
	local status=$?
	local pid

	trap - EXIT HUP INT TERM
	for pid in "$gate_pid" "$v3_pid" "$v2_pid"; do
		if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
			kill "$pid" 2>/dev/null || true
		fi
	done
	for pid in "$gate_pid" "$v3_pid" "$v2_pid"; do
		if [[ -n "$pid" ]]; then
			wait "$pid" >/dev/null 2>&1 || true
		fi
	done
	if [[ "$race_fifo_open" == true ]]; then
		exec {race_fifo_fd}>&- || status=1
		race_fifo_open=false
	fi
	if [[ -n "$race_dir" && -d "$race_dir" ]]; then
		rm -rf -- "$race_dir" >/dev/null 2>&1 || status=1
	fi
	exit "$status"
}

trap cleanup_race EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

wait_for_gate_lock() {
	local attempt
	local observed

	# Polling establishes ordering; the bound is only a diagnostic timeout.
	for ((attempt = 0; attempt < 200; attempt++)); do
		if ! observed="$(application_query "SELECT count(*) FROM pg_locks AS lock JOIN pg_stat_activity AS activity USING (pid) WHERE activity.application_name = 'memory-version-heads-race-gate' AND activity.usename = current_user AND lock.locktype = 'advisory' AND lock.classid = 5203 AND lock.objid = 53 AND lock.objsubid = 2 AND lock.granted")"; then
			fail 'gate lock observation query failed'
		fi
		if [[ "$observed" == '1' ]]; then
			return
		fi
		sleep 0.05
	done

	fail 'diagnostic timeout waiting for gate advisory lock'
}

wait_for_lock_state() {
	local application_name=$1
	local wait_event=$2
	local description=$3
	local attempt
	local observed

	# Polling establishes ordering; the bound is only a diagnostic timeout.
	for ((attempt = 0; attempt < 200; attempt++)); do
		if ! observed="$(application_query "SELECT count(*) FROM pg_stat_activity WHERE application_name = '$application_name' AND usename = current_user AND state = 'active' AND wait_event_type = 'Lock' AND wait_event = '$wait_event'")"; then
			fail 'lock state observation query failed'
		fi
		if [[ "$observed" == '1' ]]; then
			return
		fi
		sleep 0.05
	done

	fail "diagnostic timeout waiting for $description"
}

last_background_pid=''
start_background_application() {
	local application_name=$1
	local sql=$2

	PGAPPNAME="$application_name" "$psql" \
		--no-psqlrc \
		--no-password \
		--host="$MEMORY_POSTGRES_SOCKET_DIR" \
		--port="$PGPORT" \
		--username="$MEMORY_POSTGRES_APPLICATION_ROLE" \
		--dbname="$MEMORY_POSTGRES_DATABASE" \
		--set=ON_ERROR_STOP=1 \
		--tuples-only \
		--no-align \
		--quiet \
		--command="$sql" >/dev/null 2>&1 &
	last_background_pid=$!
}

apply_migration

server_version="$(application_query 'SHOW server_version')" || fail 'server version query failed'
case "$server_version" in
"$MEMORY_POSTGRES_EXPECTED_MAJOR".*) ;;
*) fail "expected PostgreSQL $MEMORY_POSTGRES_EXPECTED_MAJOR" ;;
esac

insert_memory \
	'memory-version-sequential' 1 'sequential v1 title' 'sequential v1 content' \
	'2026-03-01 00:00:00+00' '2026-03-01 00:01:00+00' \
	"ARRAY['sequential-v1-concept-a', 'sequential-v1-concept-b']::text[]" \
	"ARRAY['sequential-v1-file-a', 'sequential-v1-file-b']::text[]" \
	"ARRAY['sequential-v1-session-a']::text[]" \
	"ARRAY['sequential-v1-observation-a', 'sequential-v1-observation-b']::text[]"
assert_canonical_state \
	'memory-version-sequential' "$sequential_snapshots" 1 1 \
	"ARRAY['sequential v1 title', 'sequential v1 content', 'sequential-v1-concept-a', 'sequential-v1-concept-b']::text[]" \
	'initial version creates the sole v1 head with a full canonical snapshot'

insert_memory \
	'memory-version-sequential' 3 'sequential v3 title' 'sequential v3 content' \
	'2026-03-03 00:00:00+00' '2026-03-03 00:03:00+00' \
	"ARRAY['sequential-v3-concept-a', 'sequential-v3-concept-b']::text[]" \
	"ARRAY['sequential-v3-file-a', 'sequential-v3-file-b']::text[]" \
	"ARRAY['sequential-v3-session-a']::text[]" \
	"ARRAY['sequential-v3-observation-a', 'sequential-v3-observation-b']::text[]"
assert_canonical_state \
	'memory-version-sequential' "$sequential_snapshots" 2 3 \
	"ARRAY['sequential v3 title', 'sequential v3 content', 'sequential-v3-concept-a', 'sequential-v3-concept-b']::text[]" \
	'newer version creates the sole v3 head with immutable v1 history'

insert_memory \
	'memory-version-sequential' 2 'sequential v2 title' 'sequential v2 content' \
	'2026-03-02 00:00:00+00' '2026-03-02 00:02:00+00' \
	"ARRAY['sequential-v2-concept-a', 'sequential-v2-concept-b']::text[]" \
	"ARRAY['sequential-v2-file-a', 'sequential-v2-file-b']::text[]" \
	"ARRAY['sequential-v2-session-a']::text[]" \
	"ARRAY['sequential-v2-observation-a', 'sequential-v2-observation-b']::text[]"
assert_canonical_state \
	'memory-version-sequential' "$sequential_snapshots" 3 3 \
	"ARRAY['sequential v3 title', 'sequential v3 content', 'sequential-v3-concept-a', 'sequential-v3-concept-b']::text[]" \
	'lower version preserves all immutable snapshots and the sole v3 head'

expect_application_failure 'duplicate-v3-replacement' "
	INSERT INTO public.memories (
		id, version, type, title, content, created_at, updated_at,
		concepts, files, session_ids, source_observation_ids
	) VALUES (
		'memory-version-sequential', 3, 'custom/replacement', 'replacement title', 'replacement content',
		TIMESTAMPTZ '2026-03-30 00:00:00+00', TIMESTAMPTZ '2026-03-30 00:01:00+00',
		ARRAY['replacement-concept']::text[], ARRAY['replacement-file']::text[],
		ARRAY['replacement-session']::text[], ARRAY['replacement-observation']::text[]
	)"
assert_canonical_state \
	'memory-version-sequential' "$sequential_snapshots" 3 3 \
	"ARRAY['sequential v3 title', 'sequential v3 content', 'sequential-v3-concept-a', 'sequential-v3-concept-b']::text[]" \
	'duplicate v3 rejection preserves all full canonical snapshots and the v3 head'

insert_memory \
	'memory-version-concurrent' 1 'concurrent v1 title' 'concurrent v1 content' \
	'2026-04-01 00:00:00+00' '2026-04-01 00:01:00+00' \
	"ARRAY['concurrent-v1-concept-a', 'concurrent-v1-concept-b']::text[]" \
	"ARRAY['concurrent-v1-file-a', 'concurrent-v1-file-b']::text[]" \
	"ARRAY['concurrent-v1-session-a']::text[]" \
	"ARRAY['concurrent-v1-observation-a', 'concurrent-v1-observation-b']::text[]"
assert_canonical_state \
	'memory-version-concurrent' "$concurrent_snapshots" 1 1 \
	"ARRAY['concurrent v1 title', 'concurrent v1 content', 'concurrent-v1-concept-a', 'concurrent-v1-concept-b']::text[]" \
	'concurrency seed retains a full canonical v1 snapshot and sole v1 head'

race_dir="$(mktemp -d "${TMPDIR:-/tmp}/memory-version-heads-race.XXXXXX")" ||
	fail 'private concurrency workspace creation failed'
race_fifo="$race_dir/gate"
if ! mkfifo "$race_fifo"; then
	fail 'concurrency gate creation failed'
fi

PGAPPNAME='memory-version-heads-race-gate' "$psql" \
	--no-psqlrc \
	--no-password \
	--host="$MEMORY_POSTGRES_SOCKET_DIR" \
	--port="$PGPORT" \
	--username="$MEMORY_POSTGRES_APPLICATION_ROLE" \
	--dbname="$MEMORY_POSTGRES_DATABASE" \
	--set=ON_ERROR_STOP=1 \
	--tuples-only \
	--no-align \
	--quiet <"$race_fifo" >/dev/null 2>&1 &
gate_pid=$!
exec {race_fifo_fd}>"$race_fifo"
race_fifo_open=true

if ! printf 'BEGIN ISOLATION LEVEL READ COMMITTED;\nSELECT pg_advisory_xact_lock(5203, 53);\n' >&"$race_fifo_fd"; then
	fail 'concurrency gate setup failed'
fi
wait_for_gate_lock
progress 'concurrency-observed=gate:Lock/advisory'

start_background_application 'memory-version-heads-race-v3' "
	BEGIN ISOLATION LEVEL READ COMMITTED;
	INSERT INTO public.memories (
		id, version, type, title, content, created_at, updated_at,
		concepts, files, session_ids, source_observation_ids
	) VALUES (
		'memory-version-concurrent', 3, 'custom/fact', 'concurrent v3 title', 'concurrent v3 content',
		TIMESTAMPTZ '2026-04-03 00:00:00+00', TIMESTAMPTZ '2026-04-03 00:03:00+00',
		ARRAY['concurrent-v3-concept-a', 'concurrent-v3-concept-b']::text[],
		ARRAY['concurrent-v3-file-a', 'concurrent-v3-file-b']::text[],
		ARRAY['concurrent-v3-session-a']::text[],
		ARRAY['concurrent-v3-observation-a', 'concurrent-v3-observation-b']::text[]
	);
	SELECT pg_advisory_xact_lock(5203, 53);
	COMMIT"
v3_pid=$last_background_pid
wait_for_lock_state \
	'memory-version-heads-race-v3' 'advisory' 'v3 advisory lock'
progress 'concurrency-observed=v3:Lock/advisory'

start_background_application 'memory-version-heads-race-v2' "
	BEGIN ISOLATION LEVEL READ COMMITTED;
	INSERT INTO public.memories (
		id, version, type, title, content, created_at, updated_at,
		concepts, files, session_ids, source_observation_ids
	) VALUES (
		'memory-version-concurrent', 2, 'custom/fact', 'concurrent v2 title', 'concurrent v2 content',
		TIMESTAMPTZ '2026-04-02 00:00:00+00', TIMESTAMPTZ '2026-04-02 00:02:00+00',
		ARRAY['concurrent-v2-concept-a', 'concurrent-v2-concept-b']::text[],
		ARRAY['concurrent-v2-file-a', 'concurrent-v2-file-b']::text[],
		ARRAY['concurrent-v2-session-a']::text[],
		ARRAY['concurrent-v2-observation-a', 'concurrent-v2-observation-b']::text[]
	);
	COMMIT"
v2_pid=$last_background_pid
wait_for_lock_state \
	'memory-version-heads-race-v2' 'transactionid' 'v2 transaction-ID lock'
progress 'concurrency-observed=v2:Lock/transactionid'

if ! printf 'COMMIT;\n' >&"$race_fifo_fd"; then
	fail 'concurrency gate release failed'
fi
if ! exec {race_fifo_fd}>&-; then
	fail 'concurrency gate close failed'
fi
race_fifo_open=false

if ! wait "$gate_pid"; then
	fail 'concurrency gate exited unsuccessfully'
fi
gate_pid=''

if wait "$v3_pid"; then
	v3_status=0
else
	v3_status=$?
fi
v3_pid=''
expect_equal "$v3_status" '0' 'concurrent v3 exit status'

if wait "$v2_pid"; then
	v2_status=0
else
	v2_status=$?
fi
v2_pid=''
expect_equal "$v2_status" '0' 'concurrent v2 exit status'
progress 'concurrency-result=v3-exit:0 v2-exit:0'

assert_canonical_state \
	'memory-version-concurrent' "$concurrent_snapshots" 3 3 \
	"ARRAY['concurrent v3 title', 'concurrent v3 content', 'concurrent-v3-concept-a', 'concurrent-v3-concept-b']::text[]" \
	'concurrent v3 then v2 commits retain all full snapshots and the sole v3 head'

insert_memory \
	'memory-version-trigger-rollback' 1 'rollback v1 title' 'rollback v1 content' \
	'2026-05-01 00:00:00+00' '2026-05-01 00:01:00+00' \
	"ARRAY['rollback-v1-concept-a', 'rollback-v1-concept-b']::text[]" \
	"ARRAY['rollback-v1-file-a', 'rollback-v1-file-b']::text[]" \
	"ARRAY['rollback-v1-session-a']::text[]" \
	"ARRAY['rollback-v1-observation-a', 'rollback-v1-observation-b']::text[]"
expect_migration_failure 'temporary-head-check-rejects-v2' "
	BEGIN;
	ALTER TABLE public.memory_search_heads
		ADD CONSTRAINT memory_version_heads_ephemeral_failure
		CHECK (version <> 2);
	INSERT INTO public.memories (
		id, version, type, title, content, created_at, updated_at,
		concepts, files, session_ids, source_observation_ids
	) VALUES (
		'memory-version-trigger-rollback', 2, 'custom/fact', 'rollback v2 title', 'rollback v2 content',
		TIMESTAMPTZ '2026-05-02 00:00:00+00', TIMESTAMPTZ '2026-05-02 00:02:00+00',
		ARRAY['rollback-v2-concept-a', 'rollback-v2-concept-b']::text[],
		ARRAY['rollback-v2-file-a', 'rollback-v2-file-b']::text[],
		ARRAY['rollback-v2-session-a']::text[],
		ARRAY['rollback-v2-observation-a', 'rollback-v2-observation-b']::text[]
	);
	COMMIT"
assert_canonical_state \
	'memory-version-trigger-rollback' "$rollback_snapshots" 1 1 \
	"ARRAY['rollback v1 title', 'rollback v1 content', 'rollback-v1-concept-a', 'rollback-v1-concept-b']::text[]" \
	'trigger failure rolls back v2 and preserves the full canonical v1 snapshot and head'
expect_equal \
	"$(migration_query "SELECT count(*) FROM pg_constraint WHERE conrelid = 'public.memory_search_heads'::regclass AND conname = 'memory_version_heads_ephemeral_failure'")" \
	'0' \
	'trigger failure rolls back the temporary head constraint'

printf 'PostgreSQL %s memory version heads server=%s\n' "$MEMORY_POSTGRES_EXPECTED_MAJOR" "$server_version"
printf 'memory version heads sequential=initial:v1 newer:v3 lower:v2 history:3 head:v3\n'
printf 'memory version heads duplicate=v3-replacement-rejected snapshots:3 head:v3\n'
printf 'memory version heads concurrency=gate:Lock/advisory v3:Lock/advisory v2:Lock/transactionid exits:v3-0,v2-0 history:3 head:v3\n'
printf 'memory version heads rollback=trigger-rejected history:1 head:v1 constraint:absent\n'
