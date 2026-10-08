#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 0 ]]; then
	printf 'usage: %s\n' "$0" >&2
	exit 64
fi

readonly script_dir="$(dirname -- "${BASH_SOURCE[0]}")"
readonly setup_script="$script_dir/setup.sh"
readonly bash_path="$BASH"

if [[ ! -r "$setup_script" ]]; then
	printf 'missing fixture setup script: %s\n' "$setup_script" >&2
	exit 66
fi

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/spp-pg-start-failure.XXXXXX")"
readonly work_dir
readonly postgres_bin="$work_dir/postgres-bin"
readonly fixture_tmpdir="$work_dir/tmp"
readonly pg_ctl_log="$work_dir/pg_ctl.log"
readonly initdb_data_dir="$work_dir/initdb-data-dir"
readonly setup_output_file="$work_dir/setup-output"
readonly pre_start_pg_ctl_log="$work_dir/pre-start-pg-ctl.log"
readonly pre_start_initdb_data_dir="$work_dir/pre-start-initdb-data-dir"
readonly pre_start_setup_output_file="$work_dir/pre-start-setup-output"

cleanup() {
	local status=$?

	trap - EXIT
	rm -rf -- "$work_dir"
	exit "$status"
}
trap cleanup EXIT

mkdir -p "$postgres_bin" "$fixture_tmpdir"

printf '%s\n' \
	"#!$bash_path" \
	'set -euo pipefail' \
	'data_dir=' \
	'for argument in "$@"; do' \
	'case "$argument" in' \
	'--pgdata=*) data_dir=${argument#--pgdata=} ;;' \
	'esac' \
	'done' \
	'[[ -n "$data_dir" ]]' \
	'printf '\''%s\n'\'' "$data_dir" >"$INITDB_DATA_DIR"' \
	'if [[ -n "${INITDB_FAILURE_STATUS:-}" ]]; then' \
	'exit "$INITDB_FAILURE_STATUS"' \
	'fi' \
	'mkdir -p "$data_dir"' \
	': >"$data_dir/postgresql.conf"' \
	>"$postgres_bin/initdb"

printf '%s\n' \
	"#!$bash_path" \
	'set -euo pipefail' \
	'data_dir=' \
	'action=' \
	'while (($#)); do' \
	'case "$1" in' \
	'-D)' \
	'data_dir=$2' \
	'shift 2' \
	';;' \
	'start|stop)' \
	'action=$1' \
	'shift' \
	';;' \
	'*) shift ;;' \
	'esac' \
	'done' \
	'case "$action" in' \
	'start)' \
	': >"$data_dir/fake-server-started"' \
	'printf '\''start\n'\'' >>"$PG_CTL_LOG"' \
	'exit 97' \
	';;' \
	'stop)' \
	'[[ -e "$data_dir/fake-server-started" ]]' \
	'rm -f -- "$data_dir/fake-server-started"' \
	'printf '\''stop\n'\'' >>"$PG_CTL_LOG"' \
	';;' \
	'*) exit 64 ;;' \
	'esac' \
	>"$postgres_bin/pg_ctl"

printf '%s\n' \
	"#!$bash_path" \
	'exit 98' \
	>"$postgres_bin/psql"
chmod 0755 "$postgres_bin/initdb" "$postgres_bin/pg_ctl" "$postgres_bin/psql"

set +e
TMPDIR="$fixture_tmpdir" \
PG_CTL_LOG="$pg_ctl_log" \
INITDB_DATA_DIR="$initdb_data_dir" \
"$bash_path" "$setup_script" "$postgres_bin" 17 -- true >"$setup_output_file" 2>&1
setup_status=$?
set -e

setup_output="$(<"$setup_output_file")"
if [[ "$setup_status" -ne 97 ]]; then
	printf 'expected forced start failure status 97, found %s\n%s\n' "$setup_status" "$setup_output" >&2
	exit 1
fi
if [[ ! -s "$initdb_data_dir" ]]; then
	printf 'fake initdb did not record the fixture data directory\n' >&2
	exit 1
fi

fixture_data_dir="$(<"$initdb_data_dir")"
fixture_root="${fixture_data_dir%/data}"
if [[ "$fixture_root" == "$fixture_data_dir" || -e "$fixture_root" ]]; then
	printf 'fixture temporary root was not removed: %s\n' "$fixture_root" >&2
	exit 1
fi

actual_log=''
if [[ -f "$pg_ctl_log" ]]; then
	actual_log="$(<"$pg_ctl_log")"
fi
if [[ "$actual_log" != $'start\nstop' ]]; then
	printf 'expected pg_ctl start and cleanup stop, got:\n%s\nsetup output:\n%s\n' "$actual_log" "$setup_output" >&2
	exit 1
fi
if [[ "$setup_output" != *'fixture teardown=removed'* ]]; then
	printf 'fixture setup did not report temporary-root cleanup:\n%s\n' "$setup_output" >&2
	exit 1
fi

set +e
TMPDIR="$fixture_tmpdir" \
PG_CTL_LOG="$pre_start_pg_ctl_log" \
INITDB_DATA_DIR="$pre_start_initdb_data_dir" \
INITDB_FAILURE_STATUS=96 \
"$bash_path" "$setup_script" "$postgres_bin" 17 -- true >"$pre_start_setup_output_file" 2>&1
pre_start_status=$?
set -e

pre_start_setup_output="$(<"$pre_start_setup_output_file")"
if [[ "$pre_start_status" -ne 96 ]]; then
	printf 'expected forced pre-start failure status 96, found %s\n%s\n' "$pre_start_status" "$pre_start_setup_output" >&2
	exit 1
fi
if [[ ! -s "$pre_start_initdb_data_dir" ]]; then
	printf 'fake initdb did not record the pre-start fixture data directory\n' >&2
	exit 1
fi

pre_start_fixture_data_dir="$(<"$pre_start_initdb_data_dir")"
pre_start_fixture_root="${pre_start_fixture_data_dir%/data}"
if [[ "$pre_start_fixture_root" == "$pre_start_fixture_data_dir" || -e "$pre_start_fixture_root" ]]; then
	printf 'pre-start fixture temporary root was not removed: %s\n' "$pre_start_fixture_root" >&2
	exit 1
fi
if [[ -e "$pre_start_pg_ctl_log" ]]; then
	printf 'pre-start failure unexpectedly invoked pg_ctl:\n%s\n' "$(<"$pre_start_pg_ctl_log")" >&2
	exit 1
fi
if [[ "$pre_start_setup_output" != *'fixture teardown=removed'* ]]; then
	printf 'pre-start fixture setup did not report temporary-root cleanup:\n%s\n' "$pre_start_setup_output" >&2
	exit 1
fi

printf 'forced start failure cleanup=stopped-and-removed\n'
printf 'pre-start failure cleanup=removed-without-stop\n'
