#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 || $3 != '--' ]]; then
	printf 'usage: %s POSTGRES_BIN_DIR EXPECTED_MAJOR -- COMMAND [ARG...]\n' "$0" >&2
	exit 64
fi

postgres_bin=$1
expected_major=$2
shift 3

case "$expected_major" in
17 | 18) ;;
*)
	printf 'unsupported PostgreSQL major: %s\n' "$expected_major" >&2
	exit 64
	;;
esac

for command in initdb pg_ctl psql; do
	if [[ ! -x "$postgres_bin/$command" ]]; then
		printf 'missing PostgreSQL executable: %s\n' "$postgres_bin/$command" >&2
		exit 66
	fi
done

readonly bootstrap_role='memory_fixture_bootstrap'
readonly database='memory_postgres_smoke'
readonly migration_role='memory_migration'
readonly application_role='memory_application'

fail() {
	printf 'fixture setup failed: %s\n' "$1" >&2
	exit 1
}

expect_equal() {
	local actual=$1
	local expected=$2
	local description=$3

	[[ "$actual" == "$expected" ]] ||
		fail "$description: expected $expected, found $actual"
}

umask 077
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/memory-postgres-fixture.XXXXXX")"
readonly work_dir
readonly data_dir="$work_dir/data"
readonly socket_dir="$work_dir/socket"
server_running=false

cleanup() {
	local status=$?

	trap - EXIT
	if [[ "$server_running" == true ]]; then
		"$postgres_bin/pg_ctl" -D "$data_dir" -m immediate -w stop >/dev/null 2>&1 || status=1
	fi
	if ! rm -rf -- "$work_dir"; then
		status=1
	fi
	if [[ -e "$work_dir" ]]; then
		printf 'fixture teardown failed to remove its temporary root\n' >&2
		status=1
	else
		printf 'fixture teardown=removed\n'
	fi
	exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

query_as() {
	local role=$1
	local target_database=$2
	local sql=$3

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

execute_as() {
	query_as "$1" "$2" "$3" >/dev/null
}

"$postgres_bin/initdb" \
	--pgdata="$data_dir" \
	--username="$bootstrap_role" \
	--auth=trust \
	--no-locale \
	--encoding=UTF8 \
	--set=shared_memory_type=mmap \
	--set=dynamic_shared_memory_type=mmap \
	--no-instructions >/dev/null
mkdir -m 0700 "$socket_dir"

socket_dir_config=${socket_dir//\'/\'\'}
printf "listen_addresses = ''\nport = 5432\nunix_socket_directories = '%s'\nunix_socket_permissions = 0700\nshared_preload_libraries = 'pg_textsearch'\n" "$socket_dir_config" \
	>>"$data_dir/postgresql.conf"

"$postgres_bin/pg_ctl" -D "$data_dir" -w -t 60 start >/dev/null
server_running=true

server_version="$(query_as "$bootstrap_role" postgres 'SHOW server_version')"
case "$server_version" in
"$expected_major".*) ;;
*) fail "expected PostgreSQL $expected_major, found $server_version" ;;
esac
expect_equal "$(query_as "$bootstrap_role" postgres 'SHOW listen_addresses')" '' 'listen addresses'
expect_equal "$(query_as "$bootstrap_role" postgres 'SHOW port')" '5432' 'server port'
expect_equal "$(query_as "$bootstrap_role" postgres 'SHOW unix_socket_directories')" "$socket_dir" 'socket directory'
expect_equal "$(query_as "$bootstrap_role" postgres 'SHOW unix_socket_permissions')" '0700' 'socket permissions'
expect_equal "$(query_as "$bootstrap_role" postgres 'SHOW shared_memory_type')" 'mmap' 'shared memory type'
expect_equal "$(query_as "$bootstrap_role" postgres 'SHOW dynamic_shared_memory_type')" 'mmap' 'dynamic shared memory type'
expect_equal "$(query_as "$bootstrap_role" postgres 'SHOW shared_preload_libraries')" 'pg_textsearch' 'preloaded libraries'

execute_as "$bootstrap_role" postgres "CREATE ROLE $migration_role LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL"
execute_as "$bootstrap_role" postgres "CREATE ROLE $application_role LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL"
execute_as "$bootstrap_role" postgres "CREATE DATABASE $database OWNER $migration_role TEMPLATE template0"

execute_as "$bootstrap_role" "$database" "REVOKE ALL ON DATABASE $database FROM PUBLIC"
execute_as "$bootstrap_role" "$database" "GRANT CONNECT ON DATABASE $database TO $application_role"
execute_as "$bootstrap_role" "$database" 'REVOKE CREATE ON SCHEMA public FROM PUBLIC'
execute_as "$bootstrap_role" "$database" 'CREATE EXTENSION pg_textsearch'
execute_as "$bootstrap_role" "$database" 'CREATE EXTENSION vector'

expect_equal "$(query_as "$bootstrap_role" "$database" "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = '$database'")" "$migration_role" 'migration database owner'
expect_equal "$(query_as "$bootstrap_role" "$database" "SELECT count(*) FROM pg_auth_members membership JOIN pg_roles member_role ON member_role.oid = membership.member JOIN pg_roles granted_role ON granted_role.oid = membership.roleid WHERE member_role.rolname IN ('$migration_role', '$application_role') OR granted_role.rolname IN ('$migration_role', '$application_role')")" '0' 'fixture role memberships'
expect_equal "$(query_as "$bootstrap_role" "$database" "SELECT string_agg(rolname || '|' || rolsuper || '|' || rolcreaterole || '|' || rolcreatedb || '|' || rolinherit || '|' || rolcanlogin || '|' || rolreplication || '|' || rolbypassrls || '|' || (rolpassword IS NULL), E'\\n' ORDER BY rolname) FROM pg_authid WHERE rolname IN ('$migration_role', '$application_role')")" "$application_role|false|false|false|false|true|false|false|true
$migration_role|false|false|false|false|true|false|false|true" 'fixture role attributes'
expect_equal "$(query_as "$bootstrap_role" "$database" "SELECT has_database_privilege('$application_role', '$database', 'CONNECT') || '|' || has_database_privilege('$application_role', '$database', 'CREATE') || '|' || has_database_privilege('$application_role', '$database', 'TEMPORARY') || '|' || has_schema_privilege('$application_role', 'public', 'CREATE')")" 'true|false|false|false' 'application database and schema privileges'
expect_equal "$(query_as "$bootstrap_role" "$database" "SELECT string_agg(extname, ',' ORDER BY extname) FROM pg_extension WHERE extname IN ('pg_textsearch', 'vector')")" 'pg_textsearch,vector' 'fixture extensions'
expect_equal "$(query_as "$bootstrap_role" postgres "SELECT count(*) FROM pg_extension WHERE extname IN ('pg_textsearch', 'vector')")" '0' 'postgres extension scope'
expect_equal "$(query_as "$bootstrap_role" template1 "SELECT count(*) FROM pg_extension WHERE extname IN ('pg_textsearch', 'vector')")" '0' 'template1 extension scope'
expect_equal "$(query_as "$bootstrap_role" "$database" "SELECT count(*) FROM pg_class relation JOIN pg_namespace schema ON schema.oid = relation.relnamespace WHERE schema.nspname = 'public' AND relation.relkind IN ('r', 'p')")" '0' 'fixture application tables'
expect_equal "$(query_as "$bootstrap_role" "$database" 'SELECT count(*) FROM pg_default_acl')" '0' 'fixture default privileges'

execute_as "$bootstrap_role" postgres "ALTER ROLE $bootstrap_role NOLOGIN"
if query_as "$bootstrap_role" "$database" 'SELECT 1' >/dev/null 2>&1; then
	fail 'bootstrap admin remained able to log in'
fi

export MEMORY_POSTGRES_BIN="$postgres_bin"
export MEMORY_POSTGRES_SOCKET_DIR="$socket_dir"
export MEMORY_POSTGRES_DATABASE="$database"
export MEMORY_POSTGRES_MIGRATION_ROLE="$migration_role"
export MEMORY_POSTGRES_APPLICATION_ROLE="$application_role"
export MEMORY_POSTGRES_EXPECTED_MAJOR="$expected_major"
export PGHOST="$socket_dir"
export PGPORT=5432
export PGDATABASE="$database"
export PGUSER="$application_role"

"$@"
