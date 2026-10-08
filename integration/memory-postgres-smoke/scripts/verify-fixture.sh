#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 0 ]]; then
	printf 'usage: %s\n' "$0" >&2
	exit 64
fi

for variable in \
	MEMORY_POSTGRES_BIN \
	MEMORY_POSTGRES_SOCKET_DIR \
	MEMORY_POSTGRES_DATABASE \
	MEMORY_POSTGRES_MIGRATION_ROLE \
	MEMORY_POSTGRES_APPLICATION_ROLE \
	MEMORY_POSTGRES_EXPECTED_MAJOR \
	PGHOST \
	PGPORT \
	PGDATABASE \
	PGUSER; do
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

psql="$MEMORY_POSTGRES_BIN/psql"
if [[ ! -x "$psql" ]]; then
	printf 'missing PostgreSQL executable: %s\n' "$psql" >&2
	exit 66
fi

fail() {
	printf 'fixture verification failed: %s\n' "$1" >&2
	exit 1
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
		--command="$sql"
}

application_query() {
	query_as "$MEMORY_POSTGRES_APPLICATION_ROLE" "$MEMORY_POSTGRES_DATABASE" "$1"
}

migration_query() {
	query_as "$MEMORY_POSTGRES_MIGRATION_ROLE" "$MEMORY_POSTGRES_DATABASE" "$1"
}

expect_equal "$PGHOST" "$MEMORY_POSTGRES_SOCKET_DIR" 'PGHOST'
expect_equal "$PGPORT" '5432' 'PGPORT'
expect_equal "$PGDATABASE" "$MEMORY_POSTGRES_DATABASE" 'PGDATABASE'
expect_equal "$PGUSER" "$MEMORY_POSTGRES_APPLICATION_ROLE" 'PGUSER'

server_version="$(application_query 'SHOW server_version')"
case "$server_version" in
"$MEMORY_POSTGRES_EXPECTED_MAJOR".*) ;;
*) fail "expected PostgreSQL $MEMORY_POSTGRES_EXPECTED_MAJOR, found $server_version" ;;
esac

expect_equal "$(application_query "SELECT current_user || '|' || current_database() || '|' || CASE WHEN inet_client_addr() IS NULL THEN 'socket' ELSE 'network' END")" "$MEMORY_POSTGRES_APPLICATION_ROLE|$MEMORY_POSTGRES_DATABASE|socket" 'application socket identity'
expect_equal "$(migration_query "SELECT current_user || '|' || current_database() || '|' || CASE WHEN inet_client_addr() IS NULL THEN 'socket' ELSE 'network' END")" "$MEMORY_POSTGRES_MIGRATION_ROLE|$MEMORY_POSTGRES_DATABASE|socket" 'migration socket identity'
expect_equal "$(application_query "SELECT string_agg(extname, ',' ORDER BY extname) FROM pg_extension WHERE extname IN ('pg_textsearch', 'vector')")" 'pg_textsearch,vector' 'fixture extension scope'
expect_equal "$(application_query "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = current_database()")" "$MEMORY_POSTGRES_MIGRATION_ROLE" 'migration database owner'
expect_equal \
  "$(application_query "SELECT string_agg(rolname || '|' || rolcanlogin || '|' || rolsuper || '|' || rolcreatedb || '|' || rolcreaterole || '|' || rolinherit || '|' || rolreplication || '|' || rolbypassrls, E'\\n' ORDER BY rolname) FROM pg_roles WHERE rolname IN ('$MEMORY_POSTGRES_APPLICATION_ROLE', '$MEMORY_POSTGRES_MIGRATION_ROLE')")" \
  "$MEMORY_POSTGRES_APPLICATION_ROLE|true|false|false|false|false|false|false
$MEMORY_POSTGRES_MIGRATION_ROLE|true|false|false|false|false|false|false" \
	'fixture role attributes'
expect_equal \
	"$(application_query "SELECT count(*) FROM pg_auth_members membership JOIN pg_roles member_role ON member_role.oid = membership.member JOIN pg_roles granted_role ON granted_role.oid = membership.roleid WHERE member_role.rolname IN ('$MEMORY_POSTGRES_MIGRATION_ROLE', '$MEMORY_POSTGRES_APPLICATION_ROLE') OR granted_role.rolname IN ('$MEMORY_POSTGRES_MIGRATION_ROLE', '$MEMORY_POSTGRES_APPLICATION_ROLE')")" \
	'0' \
	'fixture role memberships'
expect_equal "$(application_query "SELECT has_database_privilege(current_user, current_database(), 'CREATE') || '|' || has_database_privilege(current_user, current_database(), 'TEMPORARY') || '|' || has_schema_privilege(current_user, 'public', 'CREATE')")" 'false|false|false' 'application create privileges'
expect_equal "$(application_query "SELECT count(*) FROM information_schema.table_privileges WHERE grantee IN (current_user, 'PUBLIC') AND table_schema NOT IN ('pg_catalog', 'information_schema')")" '0' 'application table privileges'
expect_equal "$(application_query "SELECT count(*) FROM pg_class relation JOIN pg_namespace schema ON schema.oid = relation.relnamespace WHERE schema.nspname = 'public' AND relation.relkind IN ('r', 'p')")" '0' 'fixture application tables'
expect_equal "$(application_query 'SELECT count(*) FROM pg_default_acl')" '0' 'fixture default privileges'
expect_equal "$(application_query "SELECT rolcanlogin FROM pg_roles WHERE rolname = 'memory_fixture_bootstrap'")" 'f' 'disabled bootstrap admin'

if application_query "SET ROLE $MEMORY_POSTGRES_MIGRATION_ROLE" >/dev/null 2>&1; then
	fail 'application role can set the migration role'
fi
if application_query 'CREATE TABLE public.memory_fixture_application_create_denied (id integer)' >/dev/null 2>&1; then
	fail 'application role created a table'
fi
expect_equal "$(application_query "SELECT to_regclass('public.memory_fixture_application_create_denied') IS NULL")" 't' 'application create table denial'

printf 'PostgreSQL %s fixture server=%s\n' "$MEMORY_POSTGRES_EXPECTED_MAJOR" "$server_version"
printf 'fixture extension scope=%s\n' "$MEMORY_POSTGRES_DATABASE"
printf 'fixture application socket user=%s database=%s\n' "$MEMORY_POSTGRES_APPLICATION_ROLE" "$MEMORY_POSTGRES_DATABASE"
printf 'fixture migration socket user=%s database=%s\n' "$MEMORY_POSTGRES_MIGRATION_ROLE" "$MEMORY_POSTGRES_DATABASE"
printf 'fixture application privileges=create:false temp:false set-role:false tables:0\n'
