#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 0 ]]; then
	printf 'usage: verify-memory-history-mcp.sh\n' >&2
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
if ! command -v iii >/dev/null 2>&1 || ! command -v cargo >/dev/null 2>&1; then
	printf 'missing iii or cargo; run this verifier inside nix develop\n' >&2
	exit 69
fi

fail() {
	printf 'MCP memory history verification failed: %s\n' "$1" >&2
	exit 1
}

query_as() {
	local role=$1
	local sql=$2

	"$psql" \
		--no-psqlrc \
		--no-password \
		--host="$MEMORY_POSTGRES_SOCKET_DIR" \
		--port="$PGPORT" \
		--username="$role" \
		--dbname="$MEMORY_POSTGRES_DATABASE" \
		--set=ON_ERROR_STOP=1 \
		--tuples-only \
		--no-align \
		--quiet \
		--command="$sql"
}

expect_equal() {
	local actual=$1
	local expected=$2
	local description=$3

	[[ "$actual" == "$expected" ]] || fail "$description"
}

apply_migrations() {
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
		--file="$MEMORY_SCHEMA_MIGRATION" >/dev/null 2>&1; then
		fail 'memory migrations did not apply'
	fi
}

insert_native_memory() {
	local id=$1
	local version=$2
	local title=$3
	local content=$4
	local created_at=$5
	local updated_at=$6

	query_as "$MEMORY_POSTGRES_APPLICATION_ROLE" "
		INSERT INTO public.memories (
			id, version, type, title, content, created_at, updated_at,
			concepts, files, session_ids, source_observation_ids
		) VALUES (
			'$id', $version, 'custom/fact', '$title', '$content',
			TIMESTAMPTZ '$created_at', TIMESTAMPTZ '$updated_at',
			ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::text[]
		)" >/dev/null || fail 'native synthetic memory insert was rejected'
}

apply_migrations

server_version="$(query_as "$MEMORY_POSTGRES_APPLICATION_ROLE" 'SHOW server_version')" ||
	fail 'PostgreSQL version query failed'
case "$server_version" in
"$MEMORY_POSTGRES_EXPECTED_MAJOR".*) ;;
*) fail "expected PostgreSQL $MEMORY_POSTGRES_EXPECTED_MAJOR" ;;
esac

insert_native_memory \
	'native-memory-history' 1 'mcp-native-old-title' 'nativehiddenmarker' \
	'2026-09-28 11:59:59.654321+00' '2026-09-28 12:00:00.654321+00'
insert_native_memory \
	'native-memory-history' 2 'mcp-native-current-title' 'nativecurrentmarker' \
	'2026-09-28 12:00:59.765432+00' '2026-09-28 12:01:00.765432+00'

expect_equal \
	"$(query_as "$MEMORY_POSTGRES_APPLICATION_ROLE" "SELECT string_agg(version::text || ':' || to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US') || '|' || to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US'), ',' ORDER BY version) FROM public.memories WHERE id = 'native-memory-history'")" \
	'1:2026-09-28T11:59:59.654321|2026-09-28T12:00:00.654321,2:2026-09-28T12:00:59.765432|2026-09-28T12:01:00.765432' \
	'PostgreSQL retains native memory microseconds'

umask 077
readonly work_dir="$(mktemp -d "${TMPDIR:-/tmp}/memory-history-mcp.XXXXXX")"
readonly config_dir="$work_dir/config"
readonly compose_state_dir="$work_dir/compose-state"
readonly compose_file="$work_dir/worker-compose.yaml"
readonly compose_log="$work_dir/compose.log"
readonly config_file="$config_dir/memory_history_target.yaml"
readonly compose_bin="$(command -v iii)"
readonly namespace="memory_history_verification_$$"
readonly database_role="$MEMORY_POSTGRES_APPLICATION_ROLE"
readonly socket_host="${MEMORY_POSTGRES_SOCKET_DIR//\//%2F}"
readonly database_url="postgresql://$database_role@$socket_host/$MEMORY_POSTGRES_DATABASE"
compose_pid=''

cleanup() {
	local status=$?

	trap - EXIT HUP INT TERM
	if [[ -n "$compose_pid" ]] && kill -0 "$compose_pid" 2>/dev/null; then
		kill -TERM "$compose_pid" >/dev/null 2>&1 || true
		wait "$compose_pid" >/dev/null 2>&1 || true
	fi
	if ! rm -rf -- "$work_dir"; then
		status=1
	fi
	if [[ -e "$work_dir" ]]; then
		printf 'MCP memory history teardown failed to remove its temporary root\n' >&2
		status=1
	else
		printf 'MCP memory history teardown=removed\n'
	fi
	exit "$status"
}

trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -m 0700 "$config_dir" "$compose_state_dir" "$work_dir/home"

engine_port=''
for ((attempt = 0; attempt < 32; attempt++)); do
	candidate_port=$((49135 + RANDOM % 16384))
	if ! (exec 3<>"/dev/tcp/127.0.0.1/$candidate_port") >/dev/null 2>&1; then
		engine_port=$candidate_port
		break
	fi
done
[[ -n "$engine_port" ]] || fail 'unable to select an unused loopback iii engine port'
readonly engine_port
readonly engine_url="ws://127.0.0.1:$engine_port"

printf '%s\n' \
	'id: memory_history_target' \
	'name: MCP memory history PostgreSQL verification target' \
	'description: Disposable application-role target for synthetic memory retrieval.' \
	'value:' \
	'  history_max_entries: 0' \
	'  history_max_bytes: 0' \
	'  databases:' \
	'    memory:' \
	"      url: $database_url" \
	'      tls:' \
	'        mode: disable' \
	>"$config_file"

printf '%s\n' \
	"namespace: $namespace" \
	'engine:' \
	"  url: $engine_url" \
	'  workers:' \
	'    configuration:' \
	'      adapter:' \
	'        name: fs' \
	'        config:' \
	"          directory: $config_dir" \
	'containers:' \
	'  database:' \
	'    worker: package://database' \
	'    version: "0.5.20"' \
	'    config_name: memory_history_target' \
	'    required: true' \
	>"$compose_file"

(
	cd "$work_dir"
	exec env -u III_URL -u III_NAMESPACE \
		HOME="$work_dir/home" \
		III_COMPOSE_STATE_DIR="$compose_state_dir" \
		"$compose_bin" compose --up --file "$compose_file"
) >"$compose_log" 2>&1 &
compose_pid=$!

probe_target() {
	local result
	local attempt

	for ((attempt = 0; attempt < 120; attempt++)); do
		if ! kill -0 "$compose_pid" 2>/dev/null; then
			fail 'iii Compose exited before the database worker registered'
		fi
		if result="$("$compose_bin" trigger \
			--engine "$engine_url" \
			--namespace "$namespace" \
			--timeout-ms 5000 \
			--json "{\"db\":\"memory\",\"sql\":\"SELECT current_database() || '|' || current_user\",\"params\":[],\"timeout_ms\":3000}" \
			database::query 2>/dev/null)"; then
			if [[ "$result" == *"$MEMORY_POSTGRES_DATABASE"* && "$result" == *"$database_role"* ]]; then
				printf 'MCP database target role=%s server=%s\n' "$database_role" "$MEMORY_POSTGRES_EXPECTED_MAJOR"
				return
			fi
		fi
		sleep 0.25
	done
	fail 'database worker did not expose the configured memory target'
}

probe_target

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/cargo-target/memory-history-mcp}"
III_URL="$engine_url" \
	III_NAMESPACE="$namespace" \
	TOTAL_RECALL_MEMORY_DATABASE=memory \
	cargo run --locked --package memory-postgres-smoke

printf 'PostgreSQL %s MCP memory history native=head-v2 pagination=verified\n' \
	"$MEMORY_POSTGRES_EXPECTED_MAJOR"
