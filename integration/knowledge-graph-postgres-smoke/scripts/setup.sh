#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 5 || $4 != '--' ]]; then
	printf 'usage: %s POSTGRES_BIN_DIR EXPECTED_MAJOR ROW_CHANGE_MODE -- COMMAND [ARG...]\n' "$0" >&2
	exit 64
fi

postgres_bin=$1
expected_major=$2
row_change_mode=$3
shift 4

case "$expected_major" in
17 | 18) ;;
*)
	printf 'unsupported PostgreSQL major\n' >&2
	exit 64
	;;
esac

case "$row_change_mode" in
statement-capture | row-change-publishing-disabled) ;;
*)
	printf 'unsupported row-change prerequisite mode\n' >&2
	exit 64
	;;
esac

unset PGOPTIONS PGSERVICE PGPASSWORD

for command in initdb pg_ctl psql; do
	if [[ ! -x "$postgres_bin/$command" ]]; then
		printf 'missing PostgreSQL executable: %s\n' "$command" >&2
		exit 66
	fi
done

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/../../.." && pwd)"
readonly migration_file="$repo_root/crates/knowledge-graph-store/migrations/0001_knowledge_graph_foundation.sql"
if [[ ! -r "$migration_file" ]]; then
	printf 'missing graph migration\n' >&2
	exit 66
fi

readonly cluster_bootstrap_role='knowledge_graph_cluster_bootstrap'
readonly fixture_bootstrap_role='knowledge_graph_fixture_bootstrap'
readonly migration_role='knowledge_graph_migration_owner'
readonly application_role='knowledge_graph_application'
readonly database='knowledge_graph_postgres_smoke'
readonly adapter_query_timeout_seconds=10
readonly statement_timeout_seconds=12
readonly invocation_timeout_seconds=15

fail() {
	printf 'fixture setup failed: %s\n' "$1" >&2
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
		"$postgres_bin/psql" \
			--no-psqlrc \
			--no-password \
			--host="$socket_dir" \
			--port=5432 \
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

run_sql_as() {
	if ! query_as "$1" "$2" "$3" >/dev/null 2>&1; then
		fail 'fixture database setup statement failed'
	fi
}

umask 077
# Keep the socket pathname below platforms' Unix-domain socket length limits.
work_dir="$(mktemp -d /tmp/kgpg.XXXXXX)"
readonly work_dir
readonly data_dir="$work_dir/data"
readonly socket_dir="$work_dir/socket"
server_may_be_running=false

cleanup() {
	local status=$?
	local cleanup_failed=false
	local remove_work_dir=true
	local server_status
	local server_state='not-started'

	trap - EXIT
	if [[ "$server_may_be_running" == true ]]; then
		"$postgres_bin/pg_ctl" -D "$data_dir" -m fast -w -t 60 stop >/dev/null 2>&1 || true
		if "$postgres_bin/pg_ctl" -D "$data_dir" status >/dev/null 2>&1; then
			printf 'fixture teardown could not confirm PostgreSQL stopped\n' >&2
			cleanup_failed=true
			remove_work_dir=false
		else
			server_status=$?
			if [[ "$server_status" -ne 3 ]]; then
				printf 'fixture teardown could not inspect PostgreSQL status\n' >&2
				cleanup_failed=true
				remove_work_dir=false
			else
				server_state='stopped'
			fi
		fi
	fi
	if [[ "$remove_work_dir" == true ]] && ! rm -rf -- "$work_dir"; then
		printf 'fixture teardown could not remove its private temporary root\n' >&2
		cleanup_failed=true
	fi
	if [[ -e "$work_dir" ]]; then
		printf 'fixture teardown=temp-root-preserved\n' >&2
		cleanup_failed=true
	else
		printf 'fixture teardown=server-%s temp-root-removed\n' "$server_state"
	fi
	if [[ "$status" -eq 0 && "$cleanup_failed" == true ]]; then
		status=1
	fi
	exit "$status"
}

trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

"$postgres_bin/initdb" \
	--pgdata="$data_dir" \
	--username="$cluster_bootstrap_role" \
	--auth=trust \
	--no-locale \
	--encoding=UTF8 \
	--set=shared_memory_type=mmap \
	--set=dynamic_shared_memory_type=mmap \
	--no-instructions >/dev/null 2>&1
mkdir -m 0700 "$socket_dir"

socket_dir_config=${socket_dir//\'/\'\'}
printf "listen_addresses = ''\nport = 5432\nunix_socket_directories = '%s'\nunix_socket_permissions = 0700\nshared_memory_type = mmap\ndynamic_shared_memory_type = mmap\n" "$socket_dir_config" \
	>>"$data_dir/postgresql.conf"

server_may_be_running=true
if ! "$postgres_bin/pg_ctl" -D "$data_dir" -w -t 60 start >/dev/null 2>&1; then
	fail 'isolated PostgreSQL server failed to start'
fi

server_version="$(read_query_as "$cluster_bootstrap_role" postgres 'SHOW server_version')"
case "$server_version" in
"$expected_major".*) ;;
*) fail 'PostgreSQL server major did not match the requested major' ;;
esac

expect_equal "$(read_query_as "$cluster_bootstrap_role" postgres 'SHOW listen_addresses')" '' 'network listener'
expect_equal "$(read_query_as "$cluster_bootstrap_role" postgres 'SHOW port')" '5432' 'fixture port'
expect_equal "$(read_query_as "$cluster_bootstrap_role" postgres 'SHOW unix_socket_directories')" "$socket_dir" 'private socket directory'
expect_equal "$(read_query_as "$cluster_bootstrap_role" postgres 'SHOW unix_socket_permissions')" '0700' 'socket permissions'
expect_equal "$(read_query_as "$cluster_bootstrap_role" postgres 'SHOW shared_memory_type')" 'mmap' 'shared memory type'
expect_equal "$(read_query_as "$cluster_bootstrap_role" postgres 'SHOW dynamic_shared_memory_type')" 'mmap' 'dynamic shared memory type'
expect_equal "$(read_query_as "$cluster_bootstrap_role" postgres 'SHOW block_size')" '8192' 'PostgreSQL page size'
expect_equal "$(read_query_as "$cluster_bootstrap_role" postgres 'SHOW server_encoding')" 'UTF8' 'server encoding'
expect_equal "$(read_query_as "$cluster_bootstrap_role" postgres 'SELECT (inet_client_addr() IS NULL)::text')" 'true' 'bootstrap socket connection'

run_sql_as "$cluster_bootstrap_role" postgres "
	CREATE ROLE $fixture_bootstrap_role LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL;
	CREATE ROLE $migration_role NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL;
	CREATE ROLE $application_role LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL;
	GRANT $migration_role TO $fixture_bootstrap_role;
	ALTER ROLE $application_role SET statement_timeout = '${statement_timeout_seconds}s'"

run_sql_as "$cluster_bootstrap_role" postgres "
	CREATE DATABASE $database
		WITH OWNER $migration_role
		TEMPLATE template0
		ENCODING 'UTF8'
		LC_COLLATE 'C'
		LC_CTYPE 'C'"

run_sql_as "$cluster_bootstrap_role" "$database" "
	REVOKE ALL ON DATABASE $database FROM PUBLIC;
	GRANT CONNECT ON DATABASE $database TO $fixture_bootstrap_role, $migration_role, $application_role;
	REVOKE ALL ON SCHEMA public FROM PUBLIC"

actual_role_attributes="$(read_query_as "$cluster_bootstrap_role" postgres "
	SELECT string_agg(
		rolname || '|' || rolsuper::text || '|' || rolcreatedb::text || '|' ||
		rolcreaterole::text || '|' || rolinherit::text || '|' || rolcanlogin::text || '|' ||
		rolreplication::text || '|' || rolbypassrls::text,
		E'\\n' ORDER BY rolname
	)
	FROM pg_catalog.pg_roles
	WHERE rolname IN ('$fixture_bootstrap_role', '$migration_role', '$application_role')")"
expected_role_attributes="$application_role|false|false|false|false|true|false|false
$fixture_bootstrap_role|false|false|false|false|true|false|false
$migration_role|false|false|false|false|false|false|false"
expect_equal "$actual_role_attributes" "$expected_role_attributes" 'fixture role attributes'
expect_equal \
	"$(read_query_as "$cluster_bootstrap_role" postgres "SELECT bool_and(rolpassword IS NULL)::text FROM pg_catalog.pg_authid WHERE rolname IN ('$fixture_bootstrap_role', '$migration_role', '$application_role')")" \
	'true' \
	'fixture role passwords'
expect_equal \
	"$(read_query_as "$cluster_bootstrap_role" postgres "SELECT count(*) FROM pg_catalog.pg_auth_members AS membership JOIN pg_catalog.pg_roles AS member_role ON member_role.oid = membership.member JOIN pg_catalog.pg_roles AS granted_role ON granted_role.oid = membership.roleid WHERE member_role.rolname IN ('$fixture_bootstrap_role', '$migration_role', '$application_role') OR granted_role.rolname IN ('$fixture_bootstrap_role', '$migration_role', '$application_role')")" \
	'1' \
	'fixture role membership'
expect_equal \
	"$(read_query_as "$cluster_bootstrap_role" "$database" "SELECT pg_catalog.pg_get_userbyid(datdba) FROM pg_catalog.pg_database WHERE datname = current_database()")" \
	"$migration_role" \
	'migration database owner'
expect_equal \
	"$(read_query_as "$cluster_bootstrap_role" "$database" "SELECT pg_catalog.pg_encoding_to_char(encoding) FROM pg_catalog.pg_database WHERE datname = current_database()")" \
	'UTF8' \
	'database encoding'
expect_equal \
	"$(read_query_as "$cluster_bootstrap_role" "$database" "SELECT has_database_privilege('$application_role', current_database(), 'CONNECT')::text || '|' || has_database_privilege('$application_role', current_database(), 'CREATE')::text || '|' || has_database_privilege('$application_role', current_database(), 'TEMPORARY')::text")" \
	'true|false|false' \
	'application database privileges'
expect_equal \
	"$(read_query_as "$application_role" "$database" "SELECT has_schema_privilege(current_user, 'public', 'CREATE')::text")" \
	'false' \
	'application public-schema privileges'
expect_equal \
	"$(read_query_as "$application_role" "$database" "SELECT count(*) FROM pg_catalog.pg_class AS relation JOIN pg_catalog.pg_namespace AS schema ON schema.oid = relation.relnamespace WHERE schema.nspname = 'public' AND relation.relkind IN ('r', 'p')")" \
	'0' \
	'empty public schema'
expect_equal \
	"$(read_query_as "$application_role" "$database" "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'knowledge_graph')::text")" \
	'false' \
	'empty graph schema'
expect_equal \
	"$(read_query_as "$application_role" "$database" 'SHOW statement_timeout')" \
	"${statement_timeout_seconds}s" \
	'application role statement timeout'

run_sql_as "$cluster_bootstrap_role" postgres "ALTER ROLE $cluster_bootstrap_role NOLOGIN"
if query_as "$cluster_bootstrap_role" postgres 'SELECT 1' >/dev/null 2>&1; then
	fail 'cluster bootstrap login remained enabled'
fi

export KNOWLEDGE_GRAPH_POSTGRES_BIN="$postgres_bin"
export KNOWLEDGE_GRAPH_POSTGRES_SOCKET_DIR="$socket_dir"
export KNOWLEDGE_GRAPH_POSTGRES_DATABASE="$database"
export KNOWLEDGE_GRAPH_POSTGRES_BOOTSTRAP_ROLE="$fixture_bootstrap_role"
export KNOWLEDGE_GRAPH_POSTGRES_MIGRATION_ROLE="$migration_role"
export KNOWLEDGE_GRAPH_POSTGRES_APPLICATION_ROLE="$application_role"
export KNOWLEDGE_GRAPH_POSTGRES_EXPECTED_MAJOR="$expected_major"
export KNOWLEDGE_GRAPH_POSTGRES_MIGRATION="$migration_file"
export KNOWLEDGE_GRAPH_ADAPTER_QUERY_TIMEOUT_SECONDS="$adapter_query_timeout_seconds"
export KNOWLEDGE_GRAPH_POSTGRES_STATEMENT_TIMEOUT_SECONDS="$statement_timeout_seconds"
export KNOWLEDGE_GRAPH_III_INVOCATION_TIMEOUT_SECONDS="$invocation_timeout_seconds"
export KNOWLEDGE_GRAPH_ROW_CHANGE_MODE="$row_change_mode"
export PGHOST="$socket_dir"
export PGPORT=5432
export PGDATABASE="$database"
export PGUSER="$application_role"

printf 'PostgreSQL %s isolated graph fixture ready\n' "$expected_major"
printf 'fixture transport=private-unix-socket tcp-listener=disabled socket-permissions=0700\n'
printf 'fixture database-encoding=UTF8 block-size=8192\n'
printf 'fixture roles=bootstrap:limited migration-owner:NOLOGIN graph-application:LOGIN\n'
printf 'fixture timeout-contract=adapter-query:%ss postgres-statement:%ss iii-invocation:%ss\n' \
	"$adapter_query_timeout_seconds" "$statement_timeout_seconds" "$invocation_timeout_seconds"
printf 'fixture worker-prerequisite=%s declared worker=not-started\n' "$row_change_mode"

"$@"
