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

readonly bootstrap_role='session_post_processing_fixture_bootstrap'
readonly source_role='session_post_processing_source'
readonly migration_role='session_post_processing_migrator'
readonly application_role='session_post_processing_application'
readonly database='session_post_processing_smoke'

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

server_may_be_running=false
work_dir=''
data_dir=''

cleanup() {
	local status=$?
	local cleanup_failed=false
	local remove_work_dir=true
	local server_status

	trap - EXIT
	if [[ "$server_may_be_running" == true ]] &&
		! "$postgres_bin/pg_ctl" -D "$data_dir" -m immediate -w stop >/dev/null 2>&1; then
		if "$postgres_bin/pg_ctl" -D "$data_dir" status >/dev/null 2>&1; then
			printf 'fixture teardown failed to stop PostgreSQL\n' >&2
			cleanup_failed=true
			remove_work_dir=false
		else
			server_status=$?
			# pg_ctl status returns 3 only after confirming no server is running.
			if [[ "$server_status" -ne 3 ]]; then
				printf 'fixture teardown could not confirm PostgreSQL stopped\n' >&2
				cleanup_failed=true
				remove_work_dir=false
			fi
		fi
	fi
	if [[ "$remove_work_dir" == true ]] && ! rm -rf -- "$work_dir"; then
		cleanup_failed=true
	fi
	if [[ -e "$work_dir" ]]; then
		printf 'fixture teardown failed to remove its temporary root\n' >&2
		cleanup_failed=true
	else
		printf 'fixture teardown=removed\n'
	fi
	if [[ "$status" -eq 0 && "$cleanup_failed" == true ]]; then
		status=1
	fi
	exit "$status"
}

umask 077
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/spp-pg.XXXXXX")"
trap cleanup EXIT
readonly work_dir
readonly data_dir="$work_dir/data"
readonly socket_dir="$work_dir/socket"
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
printf "listen_addresses = ''\nport = 5432\nunix_socket_directories = '%s'\nunix_socket_permissions = 0700\n" "$socket_dir_config" \
	>>"$data_dir/postgresql.conf"

server_may_be_running=true
"$postgres_bin/pg_ctl" -D "$data_dir" -w -t 60 start >/dev/null

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

execute_as "$bootstrap_role" postgres "CREATE ROLE $source_role LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL"
execute_as "$bootstrap_role" postgres "CREATE ROLE $migration_role LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL"
execute_as "$bootstrap_role" postgres "CREATE ROLE $application_role LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL"
execute_as "$bootstrap_role" postgres "CREATE DATABASE $database OWNER $migration_role TEMPLATE template0"

execute_as "$bootstrap_role" "$database" "REVOKE ALL ON DATABASE $database FROM PUBLIC"
execute_as "$bootstrap_role" "$database" "GRANT CONNECT ON DATABASE $database TO $source_role, $migration_role, $application_role"
execute_as "$bootstrap_role" "$database" "ALTER SCHEMA public OWNER TO $migration_role"
execute_as "$bootstrap_role" "$database" 'REVOKE ALL ON SCHEMA public FROM PUBLIC'
execute_as "$bootstrap_role" "$database" "GRANT USAGE, CREATE ON SCHEMA public TO $source_role"
execute_as "$bootstrap_role" "$database" "GRANT USAGE ON SCHEMA public TO $application_role"

execute_as "$source_role" "$database" "
	CREATE TABLE public.session_events (
		receipt_id UUID PRIMARY KEY,
		session_id TEXT NOT NULL,
		event_type TEXT NOT NULL CHECK (event_type IN ('session_start', 'session_end')),
		project_name TEXT NOT NULL,
		current_working_directory TEXT NOT NULL,
		source_timestamp_rfc3339 TEXT NOT NULL,
		source_timestamp_utc TIMESTAMPTZ(3) NOT NULL,
		ingested_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp()
	);
	CREATE INDEX session_events_session_source_timestamp_idx
		ON public.session_events USING btree
		(session_id ASC, source_timestamp_utc ASC);
	CREATE TABLE public.raw_observations (
		receipt_id UUID PRIMARY KEY,
		session_id TEXT NOT NULL,
		event_type TEXT NOT NULL CHECK (event_type = 'observation'),
		hook_type TEXT NOT NULL,
		project_name TEXT NOT NULL,
		current_working_directory TEXT NOT NULL,
		source_timestamp_rfc3339 TEXT NOT NULL,
		source_timestamp_utc TIMESTAMPTZ(3) NOT NULL,
		ingested_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
		data JSON NOT NULL
	);
	CREATE INDEX raw_observations_session_source_timestamp_idx
		ON public.raw_observations USING btree
		(session_id ASC, source_timestamp_utc ASC)"
execute_as "$source_role" "$database" "
	REVOKE ALL ON TABLE public.session_events, public.raw_observations FROM PUBLIC;
	REVOKE ALL ON TABLE public.session_events, public.raw_observations FROM $application_role;
	GRANT SELECT ON TABLE public.session_events, public.raw_observations TO $application_role;
	INSERT INTO public.session_events (
		receipt_id, session_id, event_type, project_name, current_working_directory,
		source_timestamp_rfc3339, source_timestamp_utc
	) VALUES
		('00000000-0000-4000-8000-000000000001', 'fixture-session', 'session_start', 'fixture-project', '/fixture', '2026-01-01T00:00:00Z', TIMESTAMPTZ '2026-01-01 00:00:00+00'),
		('00000000-0000-4000-8000-000000000002', 'fixture-session', 'session_end', 'fixture-project', '/fixture', '2026-01-01T00:01:00Z', TIMESTAMPTZ '2026-01-01 00:01:00+00');
	INSERT INTO public.raw_observations (
		receipt_id, session_id, event_type, hook_type, project_name,
		current_working_directory, source_timestamp_rfc3339, source_timestamp_utc, data
	) VALUES (
		'00000000-0000-4000-8000-000000000003', 'fixture-session', 'observation', 'fixture_hook', 'fixture-project',
		'/fixture', '2026-01-01T00:00:30Z', TIMESTAMPTZ '2026-01-01 00:00:30+00', '{\"fixture\": \"observation\"}'::json
	)"
execute_as "$bootstrap_role" "$database" "REVOKE CREATE ON SCHEMA public FROM $source_role"

expect_equal \
	"$(query_as "$bootstrap_role" "$database" "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = '$database'")" \
	"$migration_role" \
	'migration database owner'
	expect_equal \
	"$(query_as "$bootstrap_role" "$database" "SELECT string_agg(rolname || '|' || rolsuper || '|' || rolcreaterole || '|' || rolcreatedb || '|' || rolinherit || '|' || rolcanlogin || '|' || rolreplication || '|' || rolbypassrls || '|' || (rolpassword IS NULL), E'\\n' ORDER BY rolname) FROM pg_authid WHERE rolname IN ('$source_role', '$migration_role', '$application_role')")" \
	"$application_role|false|false|false|false|true|false|false|true
$migration_role|false|false|false|false|true|false|false|true
$source_role|false|false|false|false|true|false|false|true" \
	'fixture login role attributes'
expect_equal \
	"$(query_as "$bootstrap_role" "$database" "SELECT count(*) FROM pg_auth_members AS membership JOIN pg_roles AS member_role ON member_role.oid = membership.member JOIN pg_roles AS granted_role ON granted_role.oid = membership.roleid WHERE member_role.rolname IN ('$source_role', '$migration_role', '$application_role') OR granted_role.rolname IN ('$source_role', '$migration_role', '$application_role')")" \
	'0' \
	'fixture role memberships'
expect_equal \
	"$(query_as "$bootstrap_role" "$database" "SELECT has_database_privilege('$application_role', '$database', 'CONNECT') || '|' || has_database_privilege('$application_role', '$database', 'CREATE') || '|' || has_database_privilege('$application_role', '$database', 'TEMPORARY') || '|' || has_schema_privilege('$application_role', 'public', 'USAGE') || '|' || has_schema_privilege('$application_role', 'public', 'CREATE')")" \
	'true|false|false|true|false' \
	'application database and schema privileges'
expect_equal \
	"$(query_as "$bootstrap_role" "$database" "SELECT count(*) FROM pg_class AS relation JOIN pg_namespace AS schema ON schema.oid = relation.relnamespace WHERE schema.nspname = 'public' AND relation.relkind = 'r' AND relation.relname IN ('session_records', 'session_processing_attempts', 'session_memory_candidates')")" \
	'0' \
	'processor tables before migration'
expect_equal \
	"$(query_as "$bootstrap_role" "$database" "SELECT count(*) FROM pg_extension WHERE extname = 'pg_uuidv7'")" \
	'0' \
	'pg_uuidv7 extension'

# PostgreSQL does not permit its initial bootstrap superuser to lose SUPERUSER.
expect_equal \
	"$(query_as "$bootstrap_role" "$database" "ALTER ROLE $bootstrap_role NOLOGIN NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL; SELECT rolsuper || '|' || rolcreaterole || '|' || rolcreatedb || '|' || rolinherit || '|' || rolcanlogin || '|' || rolreplication || '|' || rolbypassrls || '|' || (rolpassword IS NULL) FROM pg_authid WHERE rolname = '$bootstrap_role'")" \
	'true|false|false|false|false|false|false|true' \
	'disabled bootstrap role attributes'

export SESSION_POST_PROCESSING_POSTGRES_BIN="$postgres_bin"
export SESSION_POST_PROCESSING_POSTGRES_SOCKET_DIR="$socket_dir"
export SESSION_POST_PROCESSING_POSTGRES_DATABASE="$database"
export SESSION_POST_PROCESSING_BOOTSTRAP_ROLE="$bootstrap_role"
export SESSION_POST_PROCESSING_SOURCE_ROLE="$source_role"
export SESSION_POST_PROCESSING_MIGRATION_ROLE="$migration_role"
export SESSION_POST_PROCESSING_APPLICATION_ROLE="$application_role"
export SESSION_POST_PROCESSING_EXPECTED_MAJOR="$expected_major"
export PGHOST="$socket_dir"
export PGPORT=5432
export PGDATABASE="$database"
export PGUSER="$application_role"

"$@"
