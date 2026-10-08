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
	KNOWLEDGE_GRAPH_POSTGRES_STATEMENT_TIMEOUT_SECONDS \
	KNOWLEDGE_GRAPH_ROW_CHANGE_MODE \
	PGHOST \
	PGPORT; do
	if [[ -z "${!variable:-}" ]]; then
		printf 'missing fixture environment variable: %s\n' "$variable" >&2
		exit 64
	fi
done

readonly postgres_bin="$KNOWLEDGE_GRAPH_POSTGRES_BIN"
readonly socket_dir="$KNOWLEDGE_GRAPH_POSTGRES_SOCKET_DIR"
readonly database="$KNOWLEDGE_GRAPH_POSTGRES_DATABASE"
readonly bootstrap_role="$KNOWLEDGE_GRAPH_POSTGRES_BOOTSTRAP_ROLE"
readonly migration_role="$KNOWLEDGE_GRAPH_POSTGRES_MIGRATION_ROLE"
readonly application_role="$KNOWLEDGE_GRAPH_POSTGRES_APPLICATION_ROLE"
readonly expected_major="$KNOWLEDGE_GRAPH_POSTGRES_EXPECTED_MAJOR"
readonly psql="$postgres_bin/psql"

if [[ ! -x "$psql" ]]; then
	printf 'missing PostgreSQL executable: psql\n' >&2
	exit 66
fi
if [[ "$KNOWLEDGE_GRAPH_POSTGRES_STATEMENT_TIMEOUT_SECONDS" != '12' ]]; then
	printf 'fixture timeout contract mismatch\n' >&2
	exit 1
fi
case "$expected_major" in
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
if [[ "$PGHOST" != "$socket_dir" || "$PGPORT" != '5432' ]]; then
	printf 'fixture PostgreSQL environment mismatch\n' >&2
	exit 1
fi

fail() {
	printf 'fixture rollback failed: %s\n' "$1" >&2
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
	local sql=$2

	PGSERVICEFILE=/dev/null PGPASSFILE=/dev/null \
		"$psql" \
			--no-psqlrc \
			--no-password \
			--host="$socket_dir" \
			--port="$PGPORT" \
			--username="$role" \
			--dbname="$database" \
			--set=ON_ERROR_STOP=1 \
			--tuples-only \
			--no-align \
			--quiet \
			--command="$sql"
}

read_query_as() {
	local output

	if ! output="$(query_as "$1" "$2" 2>/dev/null)"; then
		fail 'fixture database query failed'
	fi
	printf '%s' "$output"
}

server_version="$(read_query_as "$application_role" 'SHOW server_version')"
case "$server_version" in
"$expected_major".*) ;;
*) fail 'PostgreSQL server major did not match the requested major' ;;
esac
expect_equal \
	"$(read_query_as "$application_role" 'SHOW statement_timeout')" \
	'12s' \
	'application role statement timeout'

rollback_sql="
BEGIN;
SET ROLE $migration_role;
DO \$rollback_owner\$
BEGIN
	IF current_user <> '$migration_role' THEN
		RAISE EXCEPTION 'rollback role assertion failed';
	END IF;
	IF NOT EXISTS (
		SELECT 1
		FROM pg_catalog.pg_namespace AS graph_schema
		JOIN pg_catalog.pg_roles AS schema_owner ON schema_owner.oid = graph_schema.nspowner
		WHERE graph_schema.nspname = 'knowledge_graph'
		  AND schema_owner.rolname = current_user
	) THEN
		RAISE EXCEPTION 'graph schema ownership assertion failed';
	END IF;
END;
\$rollback_owner\$;
DROP SCHEMA knowledge_graph CASCADE;
COMMIT;
"

if ! query_as "$bootstrap_role" "$rollback_sql" >/dev/null 2>&1; then
	fail 'graph schema could not be dropped by its migration owner'
fi

expect_equal \
	"$(read_query_as "$application_role" "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'knowledge_graph')::text")" \
	'false' \
	'graph schema absence'
printf 'fixture rollback=completed graph-schema=absent\n'
