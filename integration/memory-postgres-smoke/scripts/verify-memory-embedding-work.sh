#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 0 ]]; then
	printf 'usage: verify-memory-embedding-work.sh\n' >&2
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
	printf 'memory embedding work verification failed: %s\n' "$1" >&2
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
	local result

	if ! result="$(application_query "$1")"; then
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

	application_execute "
		INSERT INTO public.memories (
			id, version, type, title, content, created_at, updated_at,
			concepts, files, session_ids, source_observation_ids
		) VALUES (
			'$id', $version, 'custom/fact', '$title', '$content',
			TIMESTAMPTZ '2026-09-23 00:00:00+00', TIMESTAMPTZ '2026-09-23 00:00:00+00',
			$concepts, ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[]
		)"
}

IFS= read -r -d '' load_keys_sql <<'SQL' || true
WITH requested AS (
    SELECT
        item.value->>'id' AS id,
        item.value->>'version' AS version,
        item.ordinality
    FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS item(value, ordinality)
)
SELECT
    requested.id,
    requested.version,
    CASE
        WHEN memory.id IS NOT NULL AND embedding.id IS NULL THEN memory.title
    END AS title,
    CASE
        WHEN memory.id IS NOT NULL AND embedding.id IS NULL THEN memory.content
    END AS content,
    CASE
        WHEN memory.id IS NOT NULL AND embedding.id IS NULL THEN to_json(memory.concepts)
    END AS concepts,
    memory.id IS NOT NULL AS memory_present,
    embedding.id IS NOT NULL AS embedding_present
FROM requested
LEFT JOIN public.memories AS memory
    ON memory.id = requested.id
   AND memory.version = requested.version::bigint
LEFT JOIN public.memory_embeddings AS embedding
    ON embedding.id = memory.id
   AND embedding.version = memory.version
ORDER BY requested.ordinality ASC
SQL
readonly load_keys_sql

IFS= read -r -d '' list_missing_sql <<'SQL' || true
SELECT
    memory.id,
    memory.version::text AS version,
    memory.title,
    memory.content,
    to_json(memory.concepts) AS concepts
FROM public.memories AS memory
LEFT JOIN public.memory_embeddings AS embedding
    ON embedding.id = memory.id
   AND embedding.version = memory.version
WHERE embedding.id IS NULL
ORDER BY memory.id COLLATE "C" ASC,
         memory.version ASC
LIMIT $1::text::bigint
SQL
readonly list_missing_sql

IFS= read -r -d '' insert_embedding_sql <<'SQL' || true
WITH parent AS MATERIALIZED (
    SELECT id, version
    FROM public.memories
    WHERE id = $1::text AND version = $2::text::bigint
),
inserted AS (
    INSERT INTO public.memory_embeddings (id, version, embedding)
    SELECT id, version, $3::text::vector
    FROM parent
    ON CONFLICT (id, version) DO NOTHING
    RETURNING id, version
)
SELECT outcome, id, version::text AS version
FROM (
    SELECT 'inserted'::text AS outcome, id, version FROM inserted
    UNION ALL
    SELECT 'conflict'::text AS outcome, id, version FROM parent
    WHERE NOT EXISTS (SELECT 1 FROM inserted)
    UNION ALL
    SELECT 'missing'::text AS outcome, $1::text AS id, $2::text::bigint AS version
    WHERE NOT EXISTS (SELECT 1 FROM parent)
) AS outcome_row
SQL
readonly insert_embedding_sql

load_keys() {
	local requested_keys=$1

	query_result "
		PREPARE memory_embedding_load_keys(jsonb) AS
		$load_keys_sql;
		EXECUTE memory_embedding_load_keys('$requested_keys'::jsonb)"
}

list_missing() {
	local limit=$1

	query_result "
		PREPARE memory_embedding_list_missing(text) AS
		$list_missing_sql;
		EXECUTE memory_embedding_list_missing('$limit'::text)"
}

assert_missing_work_order() {
	expect_equal \
		"$(query_result "
			PREPARE memory_embedding_list_missing(text) AS
			$list_missing_sql;
			SELECT EXISTS (
				SELECT 1
				FROM pg_prepared_statements
				WHERE name = 'memory_embedding_list_missing'
				  AND statement ~ 'ORDER[[:space:]]+BY[[:space:]]+memory[.]id[[:space:]]+COLLATE[[:space:]]+\"C\"[[:space:]]+ASC,[[:space:]]+memory[.]version[[:space:]]+ASC'
			)")" \
		't' \
		'prepared missing-work query retains explicit C-collated ID/version ordering'
}

insert_embedding() {
	local id=$1
	local version=$2
	local vector=$3

	query_result "
		PREPARE memory_embedding_insert(text, text, text) AS
		$insert_embedding_sql;
		EXECUTE memory_embedding_insert('$id'::text, '$version'::text, '$vector'::text)"
}

expect_stored_embedding() {
	local id=$1
	local version=$2
	local vector=$3
	local description=$4

	expect_equal \
		"$(insert_embedding "$id" "$version" "$vector")" \
		"inserted|$id|$version" \
		"$description"
}

progress() {
	printf 'memory embedding work progress=%s\n' "$1" >&2
}

apply_migration

server_version="$(query_result 'SHOW server_version')"
case "$server_version" in
"$MEMORY_POSTGRES_EXPECTED_MAJOR".*) ;;
*) fail 'unexpected PostgreSQL server major' ;;
esac

assert_missing_work_order

expect_equal "$(list_missing '16')" '' 'empty missing-work selection'

insert_memory \
	'work-load-pending' 1 'pending title' 'pending content' \
	"ARRAY['pending-concept']::text[]"
insert_memory \
	'work-load-present' 1 'present title' 'present content' \
	"ARRAY['present-concept']::text[]"
expect_stored_embedding \
	'work-load-present' 1 '[3,4]' 'already-present fixture embedding insertion'

readonly load_keys_parameters='[{"id":"work-load-present","version":"1"},{"id":"work-load-pending","version":"1"},{"id":"work-load-missing","version":"1"}]'
readonly expected_load_keys=$'work-load-present|1||||t|t\nwork-load-pending|1|pending title|pending content|["pending-concept"]|t|f\nwork-load-missing|1||||f|f'
expect_equal \
	"$(load_keys "$load_keys_parameters")" \
	"$expected_load_keys" \
	'exact-key input order and pending/already-present/missing classification'

expect_equal \
	"$(insert_embedding 'work-write-missing' '1' '[3,4]')" \
	'missing|work-write-missing|1' \
	'missing parent insertion outcome'
expect_equal \
	"$(query_result "SELECT count(*) FROM public.memory_embeddings WHERE id = 'work-write-missing' AND version = 1")" \
	'0' \
	'missing parent leaves no embedding'

insert_memory \
	'work-missing-A' 1 'upper title' 'upper content' \
	"ARRAY['upper-concept']::text[]"
insert_memory \
	'work-missing-a' 1 'lower title' 'lower content' \
	"ARRAY['lower-concept']::text[]"
insert_memory \
	'work-missing-history' 1 'history one title' 'history one content' \
	"ARRAY['history-one-concept']::text[]"
insert_memory \
	'work-missing-history' 2 'history two title' 'history two content' \
	"ARRAY['history-two-concept']::text[]"
expect_stored_embedding \
	'work-missing-history' 2 '[5,12]' 'historical current-version fixture embedding insertion'
insert_memory \
	'work-missing-version' 2 'version two title' 'version two content' \
	"ARRAY['version-two-concept']::text[]"
insert_memory \
	'work-missing-version' 1 'version one title' 'version one content' \
	"ARRAY['version-one-concept']::text[]"
insert_memory \
	'work-missing-z' 1 'last title' 'last content' \
	"ARRAY['last-concept']::text[]"

readonly expected_missing_work=$'work-load-pending|1|pending title|pending content|["pending-concept"]\nwork-missing-A|1|upper title|upper content|["upper-concept"]\nwork-missing-a|1|lower title|lower content|["lower-concept"]\nwork-missing-history|1|history one title|history one content|["history-one-concept"]\nwork-missing-version|1|version one title|version one content|["version-one-concept"]\nwork-missing-version|2|version two title|version two content|["version-two-concept"]\nwork-missing-z|1|last title|last content|["last-concept"]'
readonly expected_first_missing_page=$'work-load-pending|1|pending title|pending content|["pending-concept"]\nwork-missing-A|1|upper title|upper content|["upper-concept"]\nwork-missing-a|1|lower title|lower content|["lower-concept"]'
expect_equal \
	"$(list_missing '16')" \
	"$expected_missing_work" \
	'all-version missing-work anti-join and C-collated version ordering'
first_missing_page="$(list_missing '3')"
expect_equal \
	"$first_missing_page" \
	"$expected_first_missing_page" \
	'bounded missing-work first page'
expect_equal \
	"$(list_missing '3')" \
	"$first_missing_page" \
	'stateless missing-work first-page reselection'

expect_stored_embedding \
	'work-load-pending' 1 '[6,8]' 'pending fixture completion'
expect_stored_embedding \
	'work-missing-A' 1 '[8,15]' 'C-order upper fixture completion'
expect_stored_embedding \
	'work-missing-a' 1 '[7,24]' 'C-order lower fixture completion'
expect_stored_embedding \
	'work-missing-history' 1 '[9,40]' 'historical fixture completion'
expect_stored_embedding \
	'work-missing-version' 1 '[20,21]' 'version-one fixture completion'
expect_stored_embedding \
	'work-missing-version' 2 '[12,35]' 'version-two fixture completion'
expect_stored_embedding \
	'work-missing-z' 1 '[11,60]' 'last fixture completion'
expect_equal "$(list_missing '16')" '' 'fully embedded missing-work state'

insert_memory \
	'work-race' 1 'race canonical title' 'race canonical content' \
	"ARRAY['race-concept']::text[]"
race_memory_xmin="$(query_result "SELECT xmin::text FROM public.memories WHERE id = 'work-race' AND version = 1")"
race_head_xmin="$(query_result "SELECT xmin::text FROM public.memory_search_heads WHERE id = 'work-race' AND version = 1")"
[[ -n "$race_memory_xmin" && -n "$race_head_xmin" ]] ||
	fail 'race canonical state markers unavailable'
expect_equal \
	"$(query_result "SELECT
		(SELECT count(*) FROM public.memories WHERE id = 'work-race' AND version = 1) || '|' ||
		(SELECT count(*) FROM public.memory_embeddings WHERE id = 'work-race' AND version = 1) || '|' ||
		(SELECT count(*) FROM public.memory_search_heads WHERE id = 'work-race' AND version = 1)")" \
	'1|0|1' \
	'race fixture canonical state'

race_dir=''
race_fifo=''
race_fifo_fd=''
race_fifo_open=false
gate_pid=''
queue_pid=''
reconciliation_pid=''

cleanup_race() {
	local status=$?
	local pid

	trap - EXIT HUP INT TERM
	for pid in "$gate_pid" "$queue_pid" "$reconciliation_pid"; do
		if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
			kill "$pid" 2>/dev/null || true
		fi
	done
	for pid in "$gate_pid" "$queue_pid" "$reconciliation_pid"; do
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

	for ((attempt = 0; attempt < 200; attempt++)); do
		if ! observed="$(query_result "SELECT count(*) FROM pg_locks AS lock JOIN pg_stat_activity AS activity USING (pid) WHERE activity.application_name = 'memory-embedding-work-race-gate' AND activity.usename = current_user AND lock.locktype = 'advisory' AND lock.classid = 5206 AND lock.objid = 66 AND lock.objsubid = 2 AND lock.granted")"; then
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

	for ((attempt = 0; attempt < 200; attempt++)); do
		if ! observed="$(query_result "SELECT count(*) FROM pg_stat_activity WHERE application_name = '$application_name' AND usename = current_user AND state = 'active' AND wait_event_type = 'Lock' AND wait_event = '$wait_event'")"; then
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
	local output_file=$3
	local error_file=$4

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
		--command="$sql" >"$output_file" 2>"$error_file" &
	last_background_pid=$!
}

read_background_result() {
	local result_file=$1
	local result

	result="$(<"$result_file")"
	[[ -n "$result" ]] || fail 'concurrent result unavailable'
	printf '%s' "$result"
}

race_dir="$(mktemp -d "${TMPDIR:-/tmp}/memory-embedding-work-race.XXXXXX")" ||
	fail 'private concurrency workspace creation failed'
race_fifo="$race_dir/gate"
if ! mkfifo "$race_fifo"; then
	fail 'concurrency gate creation failed'
fi

PGAPPNAME='memory-embedding-work-race-gate' "$psql" \
	--no-psqlrc \
	--no-password \
	--host="$MEMORY_POSTGRES_SOCKET_DIR" \
	--port="$PGPORT" \
	--username="$MEMORY_POSTGRES_APPLICATION_ROLE" \
	--dbname="$MEMORY_POSTGRES_DATABASE" \
	--set=ON_ERROR_STOP=1 \
	--tuples-only \
	--no-align \
	--quiet <"$race_fifo" >"$race_dir/gate.out" 2>"$race_dir/gate.err" &
gate_pid=$!
exec {race_fifo_fd}>"$race_fifo"
race_fifo_open=true

if ! printf 'BEGIN ISOLATION LEVEL READ COMMITTED;\nSELECT pg_advisory_xact_lock(5206, 66);\n' >&"$race_fifo_fd"; then
	fail 'concurrency gate setup failed'
fi
wait_for_gate_lock
progress 'concurrency-observed=gate:Lock/advisory'

queue_sql="
	BEGIN ISOLATION LEVEL READ COMMITTED;
	PREPARE memory_embedding_queue_load(jsonb) AS
	$load_keys_sql;
	EXECUTE memory_embedding_queue_load('[{\"id\":\"work-race\",\"version\":\"1\"}]'::jsonb);
	PREPARE memory_embedding_queue_insert(text, text, text) AS
	$insert_embedding_sql;
	EXECUTE memory_embedding_queue_insert('work-race'::text, '1'::text, '[3,4]'::text);
	SELECT pg_advisory_xact_lock(5206, 66);
	COMMIT"
start_background_application \
	'memory-embedding-work-race-queue' \
	"$queue_sql" \
	"$race_dir/queue.out" \
	"$race_dir/queue.err"
queue_pid=$last_background_pid
wait_for_lock_state \
	'memory-embedding-work-race-queue' 'advisory' 'queue-shaped advisory lock'
progress 'concurrency-observed=queue:Lock/advisory'

reconciliation_sql="
	BEGIN ISOLATION LEVEL READ COMMITTED;
	PREPARE memory_embedding_reconciliation_list(text) AS
	$list_missing_sql;
	EXECUTE memory_embedding_reconciliation_list('1'::text);
	PREPARE memory_embedding_reconciliation_insert(text, text, text) AS
	$insert_embedding_sql;
	EXECUTE memory_embedding_reconciliation_insert('work-race'::text, '1'::text, '[6,8]'::text);
	COMMIT"
start_background_application \
	'memory-embedding-work-race-reconciliation' \
	"$reconciliation_sql" \
	"$race_dir/reconciliation.out" \
	"$race_dir/reconciliation.err"
reconciliation_pid=$last_background_pid
wait_for_lock_state \
	'memory-embedding-work-race-reconciliation' 'transactionid' \
	'reconciliation-shaped transaction-ID lock'
progress 'concurrency-observed=reconciliation:Lock/transactionid'

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

if wait "$queue_pid"; then
	queue_status=0
else
	queue_status=$?
fi
queue_pid=''
expect_equal "$queue_status" '0' 'queue-shaped insertion exit status'

if wait "$reconciliation_pid"; then
	reconciliation_status=0
else
	reconciliation_status=$?
fi
reconciliation_pid=''
expect_equal "$reconciliation_status" '0' 'reconciliation-shaped duplicate completion exit status'
[[ ! -s "$race_dir/queue.err" ]] || fail 'queue-shaped process emitted a diagnostic'
[[ ! -s "$race_dir/reconciliation.err" ]] || fail 'reconciliation-shaped process emitted a diagnostic'
progress 'concurrency-result=queue:inserted reconciliation:conflict'

readonly expected_queue_race=$'work-race|1|race canonical title|race canonical content|["race-concept"]|t|f\ninserted|work-race|1'
readonly expected_reconciliation_race=$'work-race|1|race canonical title|race canonical content|["race-concept"]\nconflict|work-race|1'
expect_equal \
	"$(read_background_result "$race_dir/queue.out")" \
	"$expected_queue_race" \
	'queue-shaped exact load and immutable insertion outcome'
expect_equal \
	"$(read_background_result "$race_dir/reconciliation.out")" \
	"$expected_reconciliation_race" \
	'reconciliation-shaped missing-work and duplicate completion outcome'
expect_equal \
	"$(query_result "SELECT
		(SELECT count(*) FROM public.memory_embeddings WHERE id = 'work-race' AND version = 1) = 1
		AND (SELECT embedding = '[3,4]'::vector FROM public.memory_embeddings WHERE id = 'work-race' AND version = 1)
		AND (SELECT xmin::text FROM public.memories WHERE id = 'work-race' AND version = 1) = '$race_memory_xmin'
		AND (SELECT xmin::text FROM public.memory_search_heads WHERE id = 'work-race' AND version = 1) = '$race_head_xmin'
		AND (SELECT type = 'custom/fact' AND title = 'race canonical title' AND content = 'race canonical content' AND concepts = ARRAY['race-concept']::text[] FROM public.memories WHERE id = 'work-race' AND version = 1)
		AND (SELECT version = 1 AND search_document = ARRAY['race canonical title', 'race canonical content', 'race-concept']::text[] FROM public.memory_search_heads WHERE id = 'work-race')")" \
	't' \
	'concurrent immutable association preserves canonical memory and head'

printf 'PostgreSQL %s memory embedding work=passed\n' "$MEMORY_POSTGRES_EXPECTED_MAJOR"
printf 'memory embedding work load=ordered pending:true already-present:true missing:true\n'
printf 'memory embedding work reconciliation=all-versions:true C-order:true bounded:true empty:true stateless:true\n'
printf 'memory embedding work race=queue-inserted:true reconciliation-conflict:true associations:one canonical:unchanged\n'
