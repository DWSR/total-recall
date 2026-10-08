#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 0 ]]; then
	printf 'usage: verify-memory-embedding-lifecycle.sh\n' >&2
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
	printf 'memory embedding lifecycle verification failed: %s\n' "$1" >&2
	exit 1
}

progress() {
	printf 'embedding lifecycle progress=%s\n' "$1" >&2
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
		--command="$sql" 2>/dev/null
}

application_query() {
	query_as "$MEMORY_POSTGRES_APPLICATION_ROLE" "$MEMORY_POSTGRES_DATABASE" "$1"
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

	application_execute "
		INSERT INTO public.memories (
			id, version, type, title, content, created_at, updated_at,
			concepts, files, session_ids, source_observation_ids
		) VALUES (
			'$id', $version, 'custom/fact', '$title', '$content',
			TIMESTAMPTZ '2026-02-01 00:00:00+00', TIMESTAMPTZ '2026-02-01 00:00:00+00',
			$concepts, ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[]
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

apply_migration

server_version="$(application_query 'SHOW server_version')" || fail 'server version query failed'
case "$server_version" in
"$MEMORY_POSTGRES_EXPECTED_MAJOR".*) ;;
*) fail "expected PostgreSQL $MEMORY_POSTGRES_EXPECTED_MAJOR" ;;
esac

expect_application_failure 'embedding before parent' "
	INSERT INTO public.memory_embeddings (id, version, embedding)
	VALUES ('embedding-parent-order', 1, '[3,4]'::vector)"
expect_equal \
	"$(application_query "SELECT
		(SELECT count(*) FROM public.memories WHERE id = 'embedding-parent-order') || '|' ||
		(SELECT count(*) FROM public.memory_embeddings WHERE id = 'embedding-parent-order') || '|' ||
		(SELECT count(*) FROM public.memory_search_heads WHERE id = 'embedding-parent-order')")" \
	'0|0|0' \
	'pre-parent rejection leaves all relations absent'

insert_memory \
	'embedding-parent-order' 1 'parent order title' 'parent order content' \
	"ARRAY['parent-order']::text[]"
insert_embedding 'embedding-parent-order' 1 '[3,4]'
expect_equal \
	"$(application_query "SELECT
		(SELECT count(*) FROM public.memories WHERE id = 'embedding-parent-order') || '|' ||
		(SELECT count(*) FROM public.memories WHERE id = 'embedding-parent-order' AND version = 1 AND type = 'custom/fact' AND title = 'parent order title' AND content = 'parent order content' AND created_at = TIMESTAMPTZ '2026-02-01 00:00:00+00' AND updated_at = TIMESTAMPTZ '2026-02-01 00:00:00+00' AND concepts = ARRAY['parent-order']::text[] AND files = ARRAY[]::text[] AND session_ids = ARRAY[]::text[] AND source_observation_ids = ARRAY[]::text[]) || '|' ||
		(SELECT count(*) FROM public.memory_embeddings WHERE id = 'embedding-parent-order') || '|' ||
		(SELECT count(*) FROM public.memory_embeddings WHERE id = 'embedding-parent-order' AND version = 1 AND embedding = '[3,4]'::vector) || '|' ||
		(SELECT count(*) FROM public.memory_search_heads WHERE id = 'embedding-parent-order') || '|' ||
		(SELECT count(*) FROM public.memory_search_heads WHERE id = 'embedding-parent-order' AND version = 1 AND search_document = ARRAY['parent order title', 'parent order content', 'parent-order']::text[])")" \
	'1|1|1|1|1|1' \
	'committed parent accepts an independent child without head mutation'

insert_memory \
	'embedding-history' 1 'history version one title' 'history version one content' \
	"ARRAY['history-one']::text[]"
insert_memory \
	'embedding-history' 2 'history version two title' 'history version two content' \
	"ARRAY['history-two']::text[]"
insert_embedding 'embedding-history' 1 '[5,12]'
expect_equal \
	"$(application_query "SELECT
		(SELECT count(*) FROM public.memories WHERE id = 'embedding-history') || '|' ||
		(SELECT count(*) FROM public.memories WHERE id = 'embedding-history' AND type = 'custom/fact' AND created_at = TIMESTAMPTZ '2026-02-01 00:00:00+00' AND updated_at = TIMESTAMPTZ '2026-02-01 00:00:00+00' AND files = ARRAY[]::text[] AND session_ids = ARRAY[]::text[] AND source_observation_ids = ARRAY[]::text[] AND ((version = 1 AND title = 'history version one title' AND content = 'history version one content' AND concepts = ARRAY['history-one']::text[]) OR (version = 2 AND title = 'history version two title' AND content = 'history version two content' AND concepts = ARRAY['history-two']::text[]))) || '|' ||
		(SELECT count(*) FROM public.memory_embeddings WHERE id = 'embedding-history') || '|' ||
		(SELECT count(*) FROM public.memory_embeddings WHERE id = 'embedding-history' AND version = 1 AND embedding = '[5,12]'::vector) || '|' ||
		(SELECT count(*) FROM public.memory_search_heads WHERE id = 'embedding-history') || '|' ||
		(SELECT count(*) FROM public.memory_search_heads WHERE id = 'embedding-history' AND version = 2 AND search_document = ARRAY['history version two title', 'history version two content', 'history-two']::text[])")" \
	'2|2|1|1|1|1' \
	'historical embedding preserves canonical history and current head'

insert_embedding 'embedding-history' 2 '[8,15]'
expect_equal \
	"$(application_query "SELECT
		(SELECT count(*) FROM public.memories WHERE id = 'embedding-history') || '|' ||
		(SELECT count(*) FROM public.memories WHERE id = 'embedding-history' AND type = 'custom/fact' AND created_at = TIMESTAMPTZ '2026-02-01 00:00:00+00' AND updated_at = TIMESTAMPTZ '2026-02-01 00:00:00+00' AND files = ARRAY[]::text[] AND session_ids = ARRAY[]::text[] AND source_observation_ids = ARRAY[]::text[] AND ((version = 1 AND title = 'history version one title' AND content = 'history version one content' AND concepts = ARRAY['history-one']::text[]) OR (version = 2 AND title = 'history version two title' AND content = 'history version two content' AND concepts = ARRAY['history-two']::text[]))) || '|' ||
		(SELECT count(*) FROM public.memory_embeddings WHERE id = 'embedding-history') || '|' ||
		(SELECT count(*) FROM public.memory_embeddings WHERE id = 'embedding-history' AND version = 1 AND embedding = '[5,12]'::vector) || '|' ||
		(SELECT count(*) FROM public.memory_embeddings WHERE id = 'embedding-history' AND version = 2 AND embedding = '[8,15]'::vector) || '|' ||
		(SELECT count(*) FROM public.memory_search_heads WHERE id = 'embedding-history') || '|' ||
		(SELECT count(*) FROM public.memory_search_heads WHERE id = 'embedding-history' AND version = 2 AND search_document = ARRAY['history version two title', 'history version two content', 'history-two']::text[])")" \
	'2|2|2|1|1|1|1' \
	'post-head embedding preserves canonical row and current head'

race_dir=''
race_fifo=''
race_fifo_fd=''
race_fifo_open=false
gate_pid=''
winner_pid=''
contender_pid=''

cleanup_race() {
	local status=$?
	local pid

	trap - EXIT HUP INT TERM
	for pid in "$gate_pid" "$winner_pid" "$contender_pid"; do
		if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
			kill "$pid" 2>/dev/null || true
		fi
	done
	for pid in "$gate_pid" "$winner_pid" "$contender_pid"; do
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
		if ! observed="$(application_query "SELECT count(*) FROM pg_locks AS lock JOIN pg_stat_activity AS activity USING (pid) WHERE activity.application_name = 'memory-embedding-race-gate' AND activity.usename = current_user AND lock.locktype = 'advisory' AND lock.classid = 5202 AND lock.objid = 52 AND lock.objsubid = 2 AND lock.granted")"; then
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

insert_memory \
	'embedding-race' 1 'race canonical title' 'race canonical content' \
	"ARRAY['race-concept']::text[]"
expect_equal \
	"$(application_query "SELECT
		(SELECT count(*) FROM public.memories WHERE id = 'embedding-race') || '|' ||
		(SELECT count(*) FROM public.memories WHERE id = 'embedding-race' AND version = 1 AND type = 'custom/fact' AND title = 'race canonical title' AND content = 'race canonical content' AND created_at = TIMESTAMPTZ '2026-02-01 00:00:00+00' AND updated_at = TIMESTAMPTZ '2026-02-01 00:00:00+00' AND concepts = ARRAY['race-concept']::text[] AND files = ARRAY[]::text[] AND session_ids = ARRAY[]::text[] AND source_observation_ids = ARRAY[]::text[]) || '|' ||
		(SELECT count(*) FROM public.memory_embeddings WHERE id = 'embedding-race') || '|' ||
		(SELECT count(*) FROM public.memory_search_heads WHERE id = 'embedding-race') || '|' ||
		(SELECT count(*) FROM public.memory_search_heads WHERE id = 'embedding-race' AND version = 1 AND search_document = ARRAY['race canonical title', 'race canonical content', 'race-concept']::text[])")" \
	'1|1|0|1|1' \
	'concurrent embedding parent and head setup'

race_dir="$(mktemp -d "${TMPDIR:-/tmp}/memory-embedding-race.XXXXXX")" ||
	fail 'private concurrency workspace creation failed'
race_fifo="$race_dir/gate"
if ! mkfifo "$race_fifo"; then
	fail 'concurrency gate creation failed'
fi

PGAPPNAME='memory-embedding-race-gate' "$psql" \
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

if ! printf 'BEGIN ISOLATION LEVEL READ COMMITTED;\nSELECT pg_advisory_xact_lock(5202, 52);\n' >&"$race_fifo_fd"; then
	fail 'concurrency gate setup failed'
fi
wait_for_gate_lock
progress 'concurrency-observed=gate:Lock/advisory'

start_background_application 'memory-embedding-race-winner' "
	BEGIN ISOLATION LEVEL READ COMMITTED;
	INSERT INTO public.memory_embeddings (id, version, embedding)
	VALUES ('embedding-race', 1, '[3,4]'::vector);
	SELECT pg_advisory_xact_lock(5202, 52);
	COMMIT"
winner_pid=$last_background_pid
wait_for_lock_state \
	'memory-embedding-race-winner' 'advisory' 'winner advisory lock'
progress 'concurrency-observed=winner:Lock/advisory'

start_background_application 'memory-embedding-race-contender' "
	BEGIN ISOLATION LEVEL READ COMMITTED;
	INSERT INTO public.memory_embeddings (id, version, embedding)
	VALUES ('embedding-race', 1, '[6,8]'::vector);
	COMMIT"
contender_pid=$last_background_pid
wait_for_lock_state \
	'memory-embedding-race-contender' 'transactionid' 'contender transaction-ID lock'
progress 'concurrency-observed=contender:Lock/transactionid'

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

if wait "$winner_pid"; then
	winner_status=0
else
	winner_status=$?
fi
winner_pid=''
expect_equal "$winner_status" '0' 'concurrent winner exit status'

if wait "$contender_pid"; then
	contender_status=0
else
	contender_status=$?
fi
contender_pid=''
if ((contender_status == 0)); then
	fail 'concurrent contender unexpectedly succeeded'
fi
progress 'concurrency-result=winner-exit:0 contender-exit:nonzero'

expect_equal \
	"$(application_query "SELECT
		(SELECT count(*) FROM public.memories WHERE id = 'embedding-race') || '|' ||
		(SELECT count(*) FROM public.memories WHERE id = 'embedding-race' AND version = 1 AND type = 'custom/fact' AND title = 'race canonical title' AND content = 'race canonical content' AND created_at = TIMESTAMPTZ '2026-02-01 00:00:00+00' AND updated_at = TIMESTAMPTZ '2026-02-01 00:00:00+00' AND concepts = ARRAY['race-concept']::text[] AND files = ARRAY[]::text[] AND session_ids = ARRAY[]::text[] AND source_observation_ids = ARRAY[]::text[]) || '|' ||
		(SELECT count(*) FROM public.memory_search_heads WHERE id = 'embedding-race') || '|' ||
		(SELECT count(*) FROM public.memory_search_heads WHERE id = 'embedding-race' AND version = 1 AND search_document = ARRAY['race canonical title', 'race canonical content', 'race-concept']::text[]) || '|' ||
		(SELECT count(*) FROM public.memory_embeddings WHERE id = 'embedding-race') || '|' ||
		(SELECT count(*) FROM public.memory_embeddings WHERE id = 'embedding-race' AND version = 1 AND embedding = '[3,4]'::vector)")" \
	'1|1|1|1|1|1' \
	'concurrent embedding preserves parent and head with one winner'

printf 'PostgreSQL %s memory embedding lifecycle server=%s\n' "$MEMORY_POSTGRES_EXPECTED_MAJOR" "$server_version"
printf 'embedding lifecycle parent-before-child=reject-then-committed-parent-child\n'
printf 'embedding lifecycle historical=v1-after-v2-head memories:2 embeddings:1 head:v2\n'
printf 'embedding lifecycle post-head=embeddings:v1,v2 canonical:unchanged head:v2\n'
printf 'embedding lifecycle concurrency=gate:Lock/advisory winner:Lock/advisory contender:Lock/transactionid winner-exit:0 contender-exit:nonzero embeddings:1 parent:unchanged head:unchanged\n'
