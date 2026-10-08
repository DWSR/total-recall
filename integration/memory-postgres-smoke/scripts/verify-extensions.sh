#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
	printf 'usage: %s POSTGRES_BIN_DIR EXPECTED_MAJOR\n' "$0" >&2
	exit 64
fi

postgres_bin=$1
expected_major=$2

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

fail() {
	printf 'extension verification failed: %s\n' "$1" >&2
	exit 1
}

version_at_least() {
	local actual=$1
	local required=$2
	local actual_major actual_minor actual_patch
	local required_major required_minor required_patch

	IFS=. read -r actual_major actual_minor actual_patch <<<"$actual"
	IFS=. read -r required_major required_minor required_patch <<<"$required"

	for component in \
		"$actual_major" "$actual_minor" "$actual_patch" \
		"$required_major" "$required_minor" "$required_patch"; do
		[[ "$component" =~ ^[0-9]+$ ]] || return 1
	done

	if ((10#$actual_major != 10#$required_major)); then
		((10#$actual_major > 10#$required_major))
		return
	fi
	if ((10#$actual_minor != 10#$required_minor)); then
		((10#$actual_minor > 10#$required_minor))
		return
	fi
	((10#$actual_patch >= 10#$required_patch))
}

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/memory-postgres-extensions.XXXXXX")"
data_dir="$work_dir/data"
socket_dir="$work_dir/socket"
smoke_database="memory_extension_smoke"
server_running=false

cleanup() {
	if [[ "$server_running" == true ]]; then
		"$postgres_bin/pg_ctl" -D "$data_dir" -m immediate -w stop >/dev/null 2>&1 || true
	fi
	rm -rf "$work_dir"
}
trap cleanup EXIT

query() {
	"$postgres_bin/psql" \
		--no-psqlrc \
		--host="$socket_dir" \
		--dbname="$1" \
		--set=ON_ERROR_STOP=1 \
		--tuples-only \
		--no-align \
		--quiet \
		--command="$2"
}

execute() {
	query "$1" "$2" >/dev/null
}

"$postgres_bin/initdb" \
	--pgdata="$data_dir" \
	--auth=trust \
	--no-locale \
	--encoding=UTF8 \
	--set=shared_memory_type=mmap \
	--set=dynamic_shared_memory_type=mmap \
	--no-instructions >/dev/null
mkdir "$socket_dir"
printf "listen_addresses = ''\nunix_socket_directories = '%s'\n" "$socket_dir" \
	>>"$data_dir/postgresql.conf"

"$postgres_bin/pg_ctl" -D "$data_dir" -w start >/dev/null
server_running=true

preload_before_reload="$(query postgres 'SHOW shared_preload_libraries')"
[[ -z "$preload_before_reload" ]] || fail 'pg_textsearch was preloaded before configuration'

printf "shared_preload_libraries = 'pg_textsearch'\n" >>"$data_dir/postgresql.conf"
"$postgres_bin/pg_ctl" -D "$data_dir" reload >/dev/null

preload_after_reload="$(query postgres 'SHOW shared_preload_libraries')"
[[ -z "$preload_after_reload" ]] || fail 'pg_textsearch was loaded without a restart'

"$postgres_bin/pg_ctl" -D "$data_dir" -w restart >/dev/null

server_version="$(query postgres 'SHOW server_version')"
case "$server_version" in
"$expected_major".*) ;;
*) fail "expected PostgreSQL $expected_major, found $server_version" ;;
esac

preload_after_restart="$(query postgres 'SHOW shared_preload_libraries')"
[[ "$preload_after_restart" == 'pg_textsearch' ]] ||
	fail "expected pg_textsearch preload after restart, found $preload_after_restart"

execute postgres "CREATE DATABASE $smoke_database"
execute "$smoke_database" 'CREATE EXTENSION pg_textsearch'
execute "$smoke_database" 'CREATE EXTENSION vector'

[[ "$(query postgres "SELECT count(*) FROM pg_extension WHERE extname IN ('pg_textsearch', 'vector')")" == 0 ]] ||
	fail 'search extensions were created outside the smoke database'

pg_textsearch_installed="$(query "$smoke_database" "SELECT extversion FROM pg_extension WHERE extname = 'pg_textsearch'")"
pg_textsearch_default="$(query "$smoke_database" "SELECT default_version FROM pg_available_extensions WHERE name = 'pg_textsearch'")"
vector_installed="$(query "$smoke_database" "SELECT extversion FROM pg_extension WHERE extname = 'vector'")"
vector_default="$(query "$smoke_database" "SELECT default_version FROM pg_available_extensions WHERE name = 'vector'")"

[[ "$pg_textsearch_installed" == '1.4.0' ]] ||
	fail "expected pg_textsearch 1.4.0, found $pg_textsearch_installed"
[[ "$pg_textsearch_default" == '1.4.0' ]] ||
	fail "expected pg_textsearch default 1.4.0, found $pg_textsearch_default"
version_at_least "$vector_installed" '0.8.6' ||
	fail "expected pgvector 0.8.6 or later, found $vector_installed"
version_at_least "$vector_default" '0.8.6' ||
	fail "expected pgvector default 0.8.6 or later, found $vector_default"

printf 'PostgreSQL %s server=%s\n' "$expected_major" "$server_version"
printf 'shared_preload_libraries=%s\n' "$preload_after_restart"
printf 'pg_textsearch installed=%s default=%s\n' "$pg_textsearch_installed" "$pg_textsearch_default"
printf 'pgvector installed=%s default=%s\n' "$vector_installed" "$vector_default"
