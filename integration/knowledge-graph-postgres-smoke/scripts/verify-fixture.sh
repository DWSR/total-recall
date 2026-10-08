#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 0 ]]; then
	printf 'usage: %s\n' "$0" >&2
	exit 64
fi

unset PGOPTIONS PGSERVICE PGPASSWORD

for variable in \
	KNOWLEDGE_GRAPH_POSTGRES_BIN \
	KNOWLEDGE_GRAPH_POSTGRES_SOCKET_DIR \
	KNOWLEDGE_GRAPH_POSTGRES_DATABASE \
	KNOWLEDGE_GRAPH_POSTGRES_BOOTSTRAP_ROLE \
	KNOWLEDGE_GRAPH_POSTGRES_MIGRATION_ROLE \
	KNOWLEDGE_GRAPH_POSTGRES_APPLICATION_ROLE \
	KNOWLEDGE_GRAPH_POSTGRES_EXPECTED_MAJOR \
	KNOWLEDGE_GRAPH_POSTGRES_MIGRATION \
	KNOWLEDGE_GRAPH_ADAPTER_QUERY_TIMEOUT_SECONDS \
	KNOWLEDGE_GRAPH_POSTGRES_STATEMENT_TIMEOUT_SECONDS \
	KNOWLEDGE_GRAPH_III_INVOCATION_TIMEOUT_SECONDS \
	KNOWLEDGE_GRAPH_ROW_CHANGE_MODE \
	PGHOST \
	PGPORT \
	PGDATABASE \
	PGUSER; do
	if [[ -z "${!variable:-}" ]]; then
		printf 'missing fixture environment variable: %s\n' "$variable" >&2
		exit 64
	fi
done

case "$KNOWLEDGE_GRAPH_POSTGRES_EXPECTED_MAJOR" in
17 | 18) ;;
*)
	printf 'unsupported PostgreSQL major\n' >&2
	exit 64
	;;
esac

case "$KNOWLEDGE_GRAPH_ROW_CHANGE_MODE" in
statement-capture | row-change-publishing-disabled) ;;
*)
	printf 'unsupported row-change prerequisite mode\n' >&2
	exit 64
	;;
esac

readonly postgres_bin="$KNOWLEDGE_GRAPH_POSTGRES_BIN"
readonly socket_dir="$KNOWLEDGE_GRAPH_POSTGRES_SOCKET_DIR"
readonly database="$KNOWLEDGE_GRAPH_POSTGRES_DATABASE"
readonly bootstrap_role="$KNOWLEDGE_GRAPH_POSTGRES_BOOTSTRAP_ROLE"
readonly migration_role="$KNOWLEDGE_GRAPH_POSTGRES_MIGRATION_ROLE"
readonly application_role="$KNOWLEDGE_GRAPH_POSTGRES_APPLICATION_ROLE"
readonly expected_major="$KNOWLEDGE_GRAPH_POSTGRES_EXPECTED_MAJOR"
readonly schema_migration="$KNOWLEDGE_GRAPH_POSTGRES_MIGRATION"
readonly adapter_query_timeout_seconds=10
readonly statement_timeout_seconds=12
readonly invocation_timeout_seconds=15
readonly psql="$postgres_bin/psql"

if [[ ! -x "$psql" ]]; then
	printf 'missing PostgreSQL executable: psql\n' >&2
	exit 66
fi
if [[ ! -r "$schema_migration" ]]; then
	printf 'missing graph migration\n' >&2
	exit 66
fi
if [[ "$KNOWLEDGE_GRAPH_ADAPTER_QUERY_TIMEOUT_SECONDS" != "$adapter_query_timeout_seconds" ||
	"$KNOWLEDGE_GRAPH_POSTGRES_STATEMENT_TIMEOUT_SECONDS" != "$statement_timeout_seconds" ||
	"$KNOWLEDGE_GRAPH_III_INVOCATION_TIMEOUT_SECONDS" != "$invocation_timeout_seconds" ]]; then
	printf 'fixture timeout contract mismatch\n' >&2
	exit 1
fi
if [[ "$PGHOST" != "$socket_dir" || "$PGPORT" != '5432' ||
	"$PGDATABASE" != "$database" || "$PGUSER" != "$application_role" ]]; then
	printf 'fixture PostgreSQL environment mismatch\n' >&2
	exit 1
fi

fail() {
	printf 'fixture verification failed: %s\n' "$1" >&2
	exit 1
}

expect_equal() {
	local actual=$1
	local expected=$2
	local description=$3

	[[ "$actual" == "$expected" ]] || fail "$description check failed"
}

query_as() {
	local role=$1
	local target_database=$2
	local sql=$3

	PGSERVICEFILE=/dev/null PGPASSFILE=/dev/null \
		"$psql" \
			--no-psqlrc \
			--no-password \
			--host="$socket_dir" \
			--port="$PGPORT" \
			--username="$role" \
			--dbname="$target_database" \
			--set=ON_ERROR_STOP=1 \
			--tuples-only \
			--no-align \
			--quiet \
			--command="$sql"
}

read_query_as() {
	local output

	if ! output="$(query_as "$1" "$2" "$3" 2>/dev/null)"; then
		fail 'fixture database query failed'
	fi
	printf '%s' "$output"
}

server_version="$(read_query_as "$application_role" "$database" 'SHOW server_version')"
case "$server_version" in
"$expected_major".*) ;;
*) fail 'PostgreSQL server major did not match the requested major' ;;
esac

expect_equal \
	"$(read_query_as "$bootstrap_role" "$database" "SELECT current_user || '|' || current_database() || '|' || CASE WHEN inet_client_addr() IS NULL THEN 'socket' ELSE 'network' END")" \
	"$bootstrap_role|$database|socket" \
	'fixture bootstrap connection'
expect_equal \
	"$(read_query_as "$application_role" "$database" "SELECT current_user || '|' || current_database() || '|' || CASE WHEN inet_client_addr() IS NULL THEN 'socket' ELSE 'network' END")" \
	"$application_role|$database|socket" \
	'graph application connection'
expect_equal \
	"$(read_query_as "$application_role" "$database" 'SHOW statement_timeout')" \
	"${statement_timeout_seconds}s" \
	'application role statement timeout'
expect_equal \
	"$(read_query_as "$application_role" "$database" "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'knowledge_graph')::text")" \
	'false' \
	'empty fixture before migration'

apply_migration() {
	{
		printf 'BEGIN;\nSET ROLE %s;\n' "$migration_role"
		printf "DO \$migration_owner\$ BEGIN IF current_user <> '%s' THEN RAISE EXCEPTION 'migration role assertion failed'; END IF; END \$migration_owner\$;\n" "$migration_role"
		cat "$schema_migration"
		printf '\nCOMMIT;\n'
	} | PGSERVICEFILE=/dev/null PGPASSFILE=/dev/null \
		"$psql" \
			--no-psqlrc \
			--no-password \
			--host="$socket_dir" \
			--port="$PGPORT" \
			--username="$bootstrap_role" \
			--dbname="$database" \
			--set=ON_ERROR_STOP=1 \
			--quiet \
			--file=- >/dev/null 2>&1
}

if ! apply_migration; then
	fail 'graph migration application failed'
fi

expect_equal \
	"$(read_query_as "$bootstrap_role" "$database" "SELECT pg_catalog.pg_get_userbyid(nspowner) FROM pg_catalog.pg_namespace WHERE nspname = 'knowledge_graph'")" \
	"$migration_role" \
	'graph schema owner'
expect_equal \
	"$(read_query_as "$application_role" "$database" "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'knowledge_graph')::text")" \
	'true' \
	'graph schema creation'

printf 'fixture migration=applied effective-current-user=%s schema-owner=%s\n' "$migration_role" "$migration_role"
printf 'fixture timeout-contract=adapter-query:%ss postgres-statement:%ss iii-invocation:%ss\n' \
	"$adapter_query_timeout_seconds" "$statement_timeout_seconds" "$invocation_timeout_seconds"
printf 'fixture worker-prerequisite=%s declared worker=not-started\n' "$KNOWLEDGE_GRAPH_ROW_CHANGE_MODE"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if ! bash "$script_dir/rollback.sh"; then
	fail 'graph schema rollback failed'
fi

expect_equal \
	"$(read_query_as "$application_role" "$database" "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'knowledge_graph')::text")" \
	'false' \
	'graph schema absence after rollback'

printf 'fixture verification=passed major=%s\n' "$expected_major"
