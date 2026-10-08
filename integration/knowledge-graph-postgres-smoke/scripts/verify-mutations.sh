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
	printf 'unsupported database-worker prerequisite mode\n' >&2
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
readonly statement_timeout_seconds=12
readonly script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly psql="$postgres_bin/psql"
readonly rollback_script="$script_dir/rollback.sh"
readonly snapshot_sql="$(cat <<'SQL'
WITH table_states AS (
  SELECT 'graph_aliases'::text AS table_name,
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data)
      ORDER BY pg_catalog.to_jsonb(row_data)::text COLLATE "C"), '[]'::jsonb) AS rows
  FROM knowledge_graph.graph_aliases AS row_data
  UNION ALL
  SELECT 'graph_assertion_evidence',
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data)
      ORDER BY pg_catalog.to_jsonb(row_data)::text COLLATE "C"), '[]'::jsonb)
  FROM knowledge_graph.graph_assertion_evidence AS row_data
  UNION ALL
  SELECT 'graph_assertions',
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data)
      ORDER BY pg_catalog.to_jsonb(row_data)::text COLLATE "C"), '[]'::jsonb)
  FROM knowledge_graph.graph_assertions AS row_data
  UNION ALL
  SELECT 'graph_concept_mentions',
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data)
      ORDER BY pg_catalog.to_jsonb(row_data)::text COLLATE "C"), '[]'::jsonb)
  FROM knowledge_graph.graph_concept_mentions AS row_data
  UNION ALL
  SELECT 'graph_concepts',
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data)
      ORDER BY pg_catalog.to_jsonb(row_data)::text COLLATE "C"), '[]'::jsonb)
  FROM knowledge_graph.graph_concepts AS row_data
  UNION ALL
  SELECT 'graph_relation_types',
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data)
      ORDER BY pg_catalog.to_jsonb(row_data)::text COLLATE "C"), '[]'::jsonb)
  FROM knowledge_graph.graph_relation_types AS row_data
  UNION ALL
  SELECT 'graph_source_references',
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data)
      ORDER BY pg_catalog.to_jsonb(row_data)::text COLLATE "C"), '[]'::jsonb)
  FROM knowledge_graph.graph_source_references AS row_data
), serialized AS (
  SELECT pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object('table', table_name, 'rows', rows)
    ORDER BY table_name COLLATE "C"
  ) AS rows
  FROM table_states
)
SELECT pg_catalog.md5(rows::text)
FROM serialized
SQL
)"
readonly non_mention_snapshot_sql="$(cat <<'SQL'
WITH table_states AS (
  SELECT 'graph_aliases'::text AS table_name,
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data)
      ORDER BY pg_catalog.to_jsonb(row_data)::text COLLATE "C"), '[]'::jsonb) AS rows
  FROM knowledge_graph.graph_aliases AS row_data
  UNION ALL
  SELECT 'graph_assertion_evidence',
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data)
      ORDER BY pg_catalog.to_jsonb(row_data)::text COLLATE "C"), '[]'::jsonb)
  FROM knowledge_graph.graph_assertion_evidence AS row_data
  UNION ALL
  SELECT 'graph_assertions',
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data)
      ORDER BY pg_catalog.to_jsonb(row_data)::text COLLATE "C"), '[]'::jsonb)
  FROM knowledge_graph.graph_assertions AS row_data
  UNION ALL
  SELECT 'graph_concepts',
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data)
      ORDER BY pg_catalog.to_jsonb(row_data)::text COLLATE "C"), '[]'::jsonb)
  FROM knowledge_graph.graph_concepts AS row_data
  UNION ALL
  SELECT 'graph_relation_types',
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data)
      ORDER BY pg_catalog.to_jsonb(row_data)::text COLLATE "C"), '[]'::jsonb)
  FROM knowledge_graph.graph_relation_types AS row_data
  UNION ALL
  SELECT 'graph_source_references',
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data)
      ORDER BY pg_catalog.to_jsonb(row_data)::text COLLATE "C"), '[]'::jsonb)
  FROM knowledge_graph.graph_source_references AS row_data
), serialized AS (
  SELECT pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object('table', table_name, 'rows', rows)
    ORDER BY table_name COLLATE "C"
  ) AS rows
  FROM table_states
)
SELECT pg_catalog.md5(rows::text)
FROM serialized
SQL
)"
readonly non_source_snapshot_sql="$(cat <<'SQL'
WITH table_states AS (
  SELECT 'graph_aliases'::text AS table_name,
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data)
      ORDER BY pg_catalog.to_jsonb(row_data)::text COLLATE "C"), '[]'::jsonb) AS rows
  FROM knowledge_graph.graph_aliases AS row_data
  UNION ALL
  SELECT 'graph_assertion_evidence',
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data)
      ORDER BY pg_catalog.to_jsonb(row_data)::text COLLATE "C"), '[]'::jsonb)
  FROM knowledge_graph.graph_assertion_evidence AS row_data
  UNION ALL
  SELECT 'graph_assertions',
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data)
      ORDER BY pg_catalog.to_jsonb(row_data)::text COLLATE "C"), '[]'::jsonb)
  FROM knowledge_graph.graph_assertions AS row_data
  UNION ALL
  SELECT 'graph_concept_mentions',
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data)
      ORDER BY pg_catalog.to_jsonb(row_data)::text COLLATE "C"), '[]'::jsonb)
  FROM knowledge_graph.graph_concept_mentions AS row_data
  UNION ALL
  SELECT 'graph_concepts',
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data)
      ORDER BY pg_catalog.to_jsonb(row_data)::text COLLATE "C"), '[]'::jsonb)
  FROM knowledge_graph.graph_concepts AS row_data
  UNION ALL
  SELECT 'graph_relation_types',
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data)
      ORDER BY pg_catalog.to_jsonb(row_data)::text COLLATE "C"), '[]'::jsonb)
  FROM knowledge_graph.graph_relation_types AS row_data
), serialized AS (
  SELECT pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object('table', table_name, 'rows', rows)
    ORDER BY table_name COLLATE "C"
  ) AS rows
  FROM table_states
)
SELECT pg_catalog.md5(rows::text)
FROM serialized
SQL
)"
schema_applied=false
race_dir=''
race_fifo=''
race_fifo_fd=''
race_fifo_open=false
race_owner_pid=''
race_contender_pid=''
race_worker_a_pid=''
race_worker_b_pid=''
race_serial=0
race_result_a=''
race_result_b=''
race_winner_id=''
race_winner_slot=''

readonly race_lock_wait_attempts=200
readonly race_completion_wait_attempts=240
readonly race_poll_interval_seconds=0.05

if [[ ! -x "$psql" ]]; then
	printf 'missing PostgreSQL executable: psql\n' >&2
	exit 66
fi
if [[ ! -r "$schema_migration" ]]; then
	printf 'missing graph migration\n' >&2
	exit 66
fi
if [[ ! -r "$rollback_script" ]]; then
	printf 'missing graph rollback fixture\n' >&2
	exit 66
fi

fail() {
	printf 'mutation verification failed: %s\n' "$1" >&2
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

race_min_lock_id_from_keys() {
	local lock_keys_sql=$1
	local sql
	local lock_id

	# The fixture bootstrap may SET ROLE to the owner for the restricted key helpers.
	sql="
		SET ROLE ${migration_role};
		SELECT pg_catalog.min(pg_catalog.hashtextextended(lock_key.value::text, 0))::text
		FROM pg_catalog.jsonb_array_elements(${lock_keys_sql}) AS lock_key(value)
	"
	lock_id="$(read_query_as "$bootstrap_role" "$sql")"
	[[ "$lock_id" =~ ^-?[0-9]+$ ]] || fail 'canonical mutation lock identity query returned an invalid key'
	printf '%s' "$lock_id"
}

assert_true() {
	local description=$1
	local sql
	local actual

	sql="$(< /dev/stdin)"
	if ! actual="$(query_as "$application_role" "$sql" 2>/dev/null)"; then
		fail "$description query failed"
	fi
	[[ "$actual" == 't' ]] || fail "$description check failed"
}

snapshot_state() {
	local output

	if ! output="$(query_as "$application_role" "$snapshot_sql" 2>/dev/null)"; then
		fail 'graph state snapshot query failed'
	fi
	printf '%s' "$output"
}

assert_no_state_change() {
	local description=$1
	local sql
	local before
	local after
	local actual

	sql="$(< /dev/stdin)"
	before="$(snapshot_state)"
	if ! actual="$(query_as "$application_role" "$sql" 2>/dev/null)"; then
		fail "$description query failed"
	fi
	[[ "$actual" == 't' ]] || fail "$description outcome check failed"
	after="$(snapshot_state)"
	[[ "$before" == "$after" ]] || fail "$description changed graph state"
}

assert_schema_absent() {
	local description=$1
	local actual

	actual="$(read_query_as "$application_role" "SELECT (NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'knowledge_graph'))::text")"
	expect_equal "$actual" 'true' "$description"
}

assertion_evidence_snapshot() {
	local assertion_id=$1

	read_query_as "$application_role" "
		SELECT pg_catalog.md5(COALESCE(
			pg_catalog.jsonb_agg(
				pg_catalog.jsonb_build_object(
					'source_kind', source_references.source_kind,
					'external_id', source_references.external_id,
					'external_version', source_references.external_version
				)
				ORDER BY source_references.source_kind COLLATE \"C\",
					source_references.external_id COLLATE \"C\",
					source_references.external_version ASC NULLS FIRST
			),
			'[]'::jsonb
		)::text)
		FROM knowledge_graph.graph_assertion_evidence AS evidence
		JOIN knowledge_graph.graph_source_references AS source_references
			ON source_references.source_ref_id = evidence.source_ref_id
		WHERE evidence.assertion_id = '${assertion_id}'::uuid
	"
}

race_uuid() {
	printf '018f0000-0000-7000-8000-%012x' "$1"
}

race_result_sql() {
	local mutation_sql=$1

	printf 'WITH race_result(value) AS MATERIALIZED (\n%s\n)\n' "$mutation_sql"
	cat <<'SQL'
SELECT pg_catalog.concat_ws(
  '|',
  COALESCE(value->>'outcome', ''),
  COALESCE(value->>'field', ''),
  COALESCE(value->>'reason', ''),
  COALESCE(value->>'record_kind', ''),
  COALESCE(value->>'reference_kind', ''),
  COALESCE(
    value#>>'{identity,id}',
    value#>>'{record,id}',
    value#>>'{record,assertion_id}',
    value#>>'{record,source_ref_id}',
    value->>'assertion_id',
    ''
  ),
  COALESCE(value->>'current_revision', value#>>'{record,revision}', '')
)
FROM race_result;
SQL
}

expect_created_existing_pair() {
	local description=$1
	local id_a=$2
	local id_b=$3

	if [[ "$race_result_a" == "created|||||${id_a}|1" &&
		"$race_result_b" == "existing|||||${id_a}|1" ]]; then
		race_winner_id=$id_a
		race_winner_slot=a
	elif [[ "$race_result_a" == "existing|||||${id_b}|1" &&
		"$race_result_b" == "created|||||${id_b}|1" ]]; then
		race_winner_id=$id_b
		race_winner_slot=b
	else
		fail "$description did not return one canonical created/existing pair"
	fi
}

expect_created_existing_same_identity_pair() {
	local description=$1
	local record_id=$2

	if [[ "$race_result_a" == "created|||||${record_id}|" &&
		"$race_result_b" == "existing|||||${record_id}|" ]]; then
		race_winner_slot=a
	elif [[ "$race_result_a" == "existing|||||${record_id}|" &&
		"$race_result_b" == "created|||||${record_id}|" ]]; then
		race_winner_slot=b
	else
		fail "$description did not return one created and one existing canonical association"
	fi
}

expect_overlapping_concept_create_pair() {
	local description=$1
	local id_a=$2
	local id_b=$3
	local conflict_a="conflict|alias_set|alias_set_mismatch|||${id_a}|1"
	local conflict_b="conflict|alias_set|alias_set_mismatch|||${id_b}|1"

	if [[ "$race_result_a" == "created|||||${id_a}|1" &&
		"$race_result_b" == "$conflict_a" ]]; then
		race_winner_id=$id_a
		race_winner_slot=a
	elif [[ "$race_result_a" == "$conflict_b" &&
		"$race_result_b" == "created|||||${id_b}|1" ]]; then
		race_winner_id=$id_b
		race_winner_slot=b
	else
		fail "$description did not return one canonical create and one typed alias conflict"
	fi
}

expect_revision_update_pair() {
	local description=$1
	local aggregate_id=$2
	local updated="updated|||||${aggregate_id}|2"
	local snapshot_conflict="conflict|snapshot|snapshot_drift|||${aggregate_id}|2"
	local stale="stale|||||${aggregate_id}|2"

	if [[ "$race_result_a" == "$updated" &&
		( "$race_result_b" == "$snapshot_conflict" || "$race_result_b" == "$stale" ) ]]; then
		race_winner_slot=a
	elif [[ "$race_result_b" == "$updated" &&
		( "$race_result_a" == "$snapshot_conflict" || "$race_result_a" == "$stale" ) ]]; then
		race_winner_slot=b
	else
		fail "$description did not return one revision update and one typed rejection"
	fi
}

create_race_concept() {
	local concept_id=$1
	local alias_key=$2

	assert_true 'concurrency fixture concept creation' <<SQL
SELECT knowledge_graph.graph_create_concept(
  '${concept_id}'::uuid,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object(
      'alias_key', '${alias_key}',
      'display_text', '${alias_key}',
      'is_preferred', true
    )
  )
)->>'outcome' = 'created'
SQL
}

create_race_source() {
	local external_id=$1
	local external_version=$2

	assert_true 'concurrency fixture source registration' <<SQL
SELECT knowledge_graph.graph_register_source(
  'memory_version', '${external_id}', ${external_version}
)->>'outcome' = 'created'
SQL
}

create_race_assertion() {
	local assertion_id=$1
	local subject_id=$2
	local relation_code=$3
	local object_id=$4
	local supporting_sources_sql=$5

	assert_true 'concurrency fixture assertion creation' <<SQL
SELECT knowledge_graph.graph_create_assertion(
  '${assertion_id}'::uuid,
  '${subject_id}'::uuid,
  '${relation_code}',
  '${object_id}'::uuid,
  ${supporting_sources_sql}
)->>'outcome' = 'created'
SQL
}

race_job_running() {
	local process_id=$1
	local running_jobs

	running_jobs="$(jobs -pr)"
	case $'\n'"$running_jobs"$'\n' in
	*$'\n'"$process_id"$'\n'*) return 0 ;;
	esac
	return 1
}

wait_for_race_owner() {
	local owner_application=$1
	local attempt
	local observed
	local sql

	sql="
		SELECT pg_catalog.count(*) = 1
		FROM pg_catalog.pg_stat_activity AS activity
		WHERE activity.application_name = '${owner_application}'
		  AND activity.usename = current_user
		  AND activity.state = 'idle in transaction'
		  AND activity.xact_start IS NOT NULL
		  AND EXISTS (
		    SELECT 1
		    FROM pg_catalog.pg_locks AS locks
		    WHERE locks.pid = activity.pid
		      AND locks.locktype = 'advisory'
		      AND locks.objsubid = 1
		      AND locks.granted
		  )
	"

	for ((attempt = 0; attempt < race_lock_wait_attempts; attempt++)); do
		if ! observed="$(read_query_as "$application_role" "$sql")"; then
			fail 'mutation lock-owner observation query failed'
		fi
		if [[ "$observed" == 't' ]]; then
			return
		fi
		if ! race_job_running "$race_owner_pid"; then
			fail 'mutation owner exited before holding a canonical application lock'
		fi
		sleep "$race_poll_interval_seconds"
	done

	fail 'bounded wait expired while the mutation owner acquired application locks'
}

# The owner has returned from its real mutation but holds its transaction locks;
# require the contender to queue on that exact single-key application lock.
wait_for_race_contender() {
	local owner_application=$1
	local contender_application=$2
	local expected_lock_id=${3:-}
	local attempt
	local observed
	local expected_wait_filter=''
	local sql

	if [[ -n "$expected_lock_id" ]]; then
		[[ "$expected_lock_id" =~ ^-?[0-9]+$ ]] || fail 'expected mutation lock identity is invalid'
		expected_wait_filter="
		  AND EXISTS (
		    SELECT 1
		    FROM blocked_on_owner AS expected
		    WHERE expected.lock_id = ${expected_lock_id}::bigint
		  )"
	fi

	sql="
		WITH owner_activity AS MATERIALIZED (
		  SELECT activity.pid
		  FROM pg_catalog.pg_stat_activity AS activity
		  WHERE activity.application_name = '${owner_application}'
		    AND activity.usename = current_user
		    AND activity.state = 'idle in transaction'
		    AND activity.xact_start IS NOT NULL
		), contender_activity AS MATERIALIZED (
		  SELECT activity.pid
		  FROM pg_catalog.pg_stat_activity AS activity
		  WHERE activity.application_name = '${contender_application}'
		    AND activity.usename = current_user
		    AND activity.state = 'active'
		    AND activity.wait_event_type = 'Lock'
		    AND activity.wait_event = 'advisory'
		), owner_locks AS MATERIALIZED (
		  SELECT locks.database, locks.classid, locks.objid
		  FROM owner_activity AS activity
		  JOIN pg_catalog.pg_locks AS locks ON locks.pid = activity.pid
		  WHERE locks.locktype = 'advisory'
		    AND locks.objsubid = 1
		    AND locks.granted
		), contender_waits AS MATERIALIZED (
		  SELECT locks.pid, locks.database, locks.classid, locks.objid,
		    -- Reconstruct the signed BIGINT encoded by the single-key lock tag.
		    CASE
		      WHEN locks.classid::bigint >= 2147483648
		        THEN (locks.classid::bigint - 4294967296) * 4294967296
		          + locks.objid::bigint
		      ELSE locks.classid::bigint * 4294967296 + locks.objid::bigint
		    END AS lock_id
		  FROM contender_activity AS activity
		  JOIN pg_catalog.pg_locks AS locks ON locks.pid = activity.pid
		  WHERE locks.locktype = 'advisory'
		    AND locks.objsubid = 1
		    AND NOT locks.granted
		), blocked_on_owner AS MATERIALIZED (
		  SELECT waits.pid, waits.lock_id
		  FROM contender_waits AS waits
		  JOIN owner_locks AS held
		    ON held.database = waits.database
		   AND held.classid = waits.classid
		   AND held.objid = waits.objid
		)
		SELECT (SELECT pg_catalog.count(*) = 1 FROM owner_activity)
		  AND (SELECT pg_catalog.count(*) = 1 FROM contender_activity)
		  AND (SELECT pg_catalog.count(*) = 1 FROM blocked_on_owner)
		  ${expected_wait_filter}
		  AND NOT EXISTS (
		    SELECT 1
		    FROM blocked_on_owner AS waiting
		    JOIN pg_catalog.pg_locks AS acquired ON acquired.pid = waiting.pid
		    WHERE acquired.locktype = 'advisory'
		      AND acquired.objsubid = 1
		      AND acquired.granted
		      AND CASE
		        WHEN acquired.classid::bigint >= 2147483648
		          THEN (acquired.classid::bigint - 4294967296) * 4294967296
		            + acquired.objid::bigint
		        ELSE acquired.classid::bigint * 4294967296 + acquired.objid::bigint
		      END >= waiting.lock_id
		  )
	"

	for ((attempt = 0; attempt < race_lock_wait_attempts; attempt++)); do
		if ! observed="$(read_query_as "$application_role" "$sql")"; then
			fail 'mutation application-lock waiter observation query failed'
		fi
		if [[ "$observed" == 't' ]]; then
			return
		fi
		if ! race_job_running "$race_owner_pid" || ! race_job_running "$race_contender_pid"; then
			fail 'mutation owner or contender exited before the application-lock wait was observed'
		fi
		sleep "$race_poll_interval_seconds"
	done

	fail 'bounded wait expired before observing the contender on the owner application lock'
}

wait_for_race_jobs() {
	local description=$1
	local attempt
	local running
	local process_id

	for ((attempt = 0; attempt < race_completion_wait_attempts; attempt++)); do
		running=false
		for process_id in "$race_worker_a_pid" "$race_worker_b_pid"; do
			if [[ -n "$process_id" ]] && race_job_running "$process_id"; then
				running=true
				break
			fi
		done
		if [[ "$running" == false ]]; then
			return
		fi
		sleep "$race_poll_interval_seconds"
	done

	for process_id in "$race_worker_a_pid" "$race_worker_b_pid"; do
		if [[ -n "$process_id" ]] && race_job_running "$process_id"; then
			kill "$process_id" >/dev/null 2>&1 || true
		fi
	done
	fail "$description exceeded the bounded completion window"
}

run_two_session_race() {
	local description=$1
	local mutation_a_sql=$2
	local mutation_b_sql=$3
	local expected_wait_lock_id=${4:-}
	local run_id
	local application_a
	local application_b
	local owner_application
	local contender_application
	local owner_mutation_sql
	local contender_mutation_sql
	local contender_command
	local owner_slot
	local contender_slot
	local owner_output
	local owner_error
	local contender_output
	local contender_error
	local worker_a_status
	local worker_b_status
	local race_passfile
	local -a psql_args

	if [[ -z "$race_dir" ]]; then
		race_dir="$(mktemp -d /tmp/kgr.XXXXXX)" ||
			fail 'private concurrency workspace creation failed'
	fi
	race_passfile="$race_dir/pgpass"
	if [[ ! -e "$race_passfile" ]]; then
		(umask 077; : >"$race_passfile") || fail 'private concurrency password-file creation failed'
	fi

	race_serial=$((race_serial + 1))
	printf -v run_id '%04d' "$race_serial"
	application_a="kg-race-${run_id}-a"
	application_b="kg-race-${run_id}-b"
	if ((race_serial % 2 == 1)); then
		owner_slot=a
		contender_slot=b
		owner_application=$application_a
		contender_application=$application_b
		owner_mutation_sql=$mutation_a_sql
		contender_mutation_sql=$mutation_b_sql
	else
		owner_slot=b
		contender_slot=a
		owner_application=$application_b
		contender_application=$application_a
		owner_mutation_sql=$mutation_b_sql
		contender_mutation_sql=$mutation_a_sql
	fi
	owner_output="$race_dir/${run_id}.${owner_slot}.out"
	owner_error="$race_dir/${run_id}.${owner_slot}.err"
	contender_output="$race_dir/${run_id}.${contender_slot}.out"
	contender_error="$race_dir/${run_id}.${contender_slot}.err"
	race_fifo="$race_dir/${run_id}.owner"
	if ! mkfifo "$race_fifo"; then
		fail 'mutation owner channel creation failed'
	fi

	psql_args=(
		--no-psqlrc
		--no-password
		--host="$socket_dir"
		--port="$PGPORT"
		--username="$application_role"
		--dbname="$database"
		--set=ON_ERROR_STOP=1
		--tuples-only
		--no-align
		--quiet
	)

	PGAPPNAME="$owner_application" PGSERVICEFILE=/dev/null PGPASSFILE="$race_passfile" \
		"$psql" "${psql_args[@]}" \
			<"$race_fifo" \
			>"$owner_output" \
			2>"$owner_error" &
	race_owner_pid=$!
	if [[ "$owner_slot" == a ]]; then
		race_worker_a_pid=$race_owner_pid
	else
		race_worker_b_pid=$race_owner_pid
	fi
	exec {race_fifo_fd}>"$race_fifo"
	race_fifo_open=true
	if ! printf 'BEGIN;\n%s\n' "$owner_mutation_sql" >&"$race_fifo_fd"; then
		fail 'mutation application-lock owner setup failed'
	fi
	wait_for_race_owner "$owner_application"

	contender_command=$'BEGIN;\n'"$contender_mutation_sql"$'\nCOMMIT;'
	PGAPPNAME="$contender_application" PGSERVICEFILE=/dev/null PGPASSFILE="$race_passfile" \
		"$psql" "${psql_args[@]}" \
			--command="$contender_command" \
			>"$contender_output" \
			2>"$contender_error" &
	race_contender_pid=$!
	if [[ "$contender_slot" == a ]]; then
		race_worker_a_pid=$race_contender_pid
	else
		race_worker_b_pid=$race_contender_pid
	fi

	wait_for_race_contender "$owner_application" "$contender_application" "$expected_wait_lock_id"
	if ! printf 'COMMIT;\n' >&"$race_fifo_fd"; then
		fail 'mutation application-lock owner release failed'
	fi
	if ! exec {race_fifo_fd}>&-; then
		fail 'mutation application-lock owner close failed'
	fi
	race_fifo_open=false
	race_fifo=''

	wait_for_race_jobs "$description"
	if wait "$race_worker_a_pid"; then
		worker_a_status=0
	else
		worker_a_status=$?
	fi
	race_worker_a_pid=''
	if wait "$race_worker_b_pid"; then
		worker_b_status=0
	else
		worker_b_status=$?
	fi
	race_worker_b_pid=''
	race_owner_pid=''
	race_contender_pid=''

	expect_equal "$worker_a_status" '0' "$description first worker exit status"
	expect_equal "$worker_b_status" '0' "$description second worker exit status"
	[[ ! -s "$race_dir/${run_id}.a.err" ]] || fail "$description first worker emitted a database diagnostic"
	[[ ! -s "$race_dir/${run_id}.b.err" ]] || fail "$description second worker emitted a database diagnostic"
	[[ -s "$race_dir/${run_id}.a.out" ]] || fail "$description first worker returned no outcome"
	[[ -s "$race_dir/${run_id}.b.out" ]] || fail "$description second worker returned no outcome"

	race_result_a="$(<"$race_dir/${run_id}.a.out")"
	race_result_b="$(<"$race_dir/${run_id}.b.out")"
	[[ "$race_result_a" != *$'\n'* && "$race_result_b" != *$'\n'* ]] ||
		fail "$description returned more than one outcome"
}

cleanup_concurrency() {
	local cleanup_failed=false
	local process_id

	if [[ "$race_fifo_open" == true ]]; then
		printf 'COMMIT;\n' >&"$race_fifo_fd" >/dev/null 2>&1 || cleanup_failed=true
		exec {race_fifo_fd}>&- >/dev/null 2>&1 || cleanup_failed=true
		race_fifo_open=false
	fi
	for process_id in "$race_worker_a_pid" "$race_worker_b_pid"; do
		if [[ -n "$process_id" ]]; then
			kill "$process_id" >/dev/null 2>&1 || true
			wait "$process_id" >/dev/null 2>&1 || true
		fi
	done
	race_owner_pid=''
	race_contender_pid=''
	race_worker_a_pid=''
	race_worker_b_pid=''
	if [[ -n "$race_dir" && -d "$race_dir" ]] && ! rm -rf -- "$race_dir"; then
		cleanup_failed=true
	fi
	race_dir=''
	[[ "$cleanup_failed" == false ]]
}

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

cleanup() {
	local status=$?
	local cleanup_failed=false

	trap - EXIT
	if ! cleanup_concurrency; then
		printf 'mutation verification cleanup failed: concurrency workers could not be cleaned up\n' >&2
		cleanup_failed=true
	fi
	if [[ "$schema_applied" == true ]]; then
		if bash "$rollback_script" >/dev/null 2>&1; then
			schema_applied=false
		else
			printf 'mutation verification cleanup failed: graph schema rollback failed\n' >&2
			cleanup_failed=true
		fi
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

expect_equal "$bootstrap_role" 'knowledge_graph_fixture_bootstrap' 'fixture bootstrap role'
expect_equal "$migration_role" 'knowledge_graph_migration_owner' 'migration owner role'
expect_equal "$application_role" 'knowledge_graph_application' 'graph application role'
expect_equal "$KNOWLEDGE_GRAPH_ADAPTER_QUERY_TIMEOUT_SECONDS" '10' 'adapter query timeout'
expect_equal "$KNOWLEDGE_GRAPH_POSTGRES_STATEMENT_TIMEOUT_SECONDS" "$statement_timeout_seconds" 'PostgreSQL statement timeout'
expect_equal "$KNOWLEDGE_GRAPH_III_INVOCATION_TIMEOUT_SECONDS" '15' 'iii invocation timeout'
if [[ "$PGHOST" != "$socket_dir" || "$PGPORT" != '5432' ||
	"$PGDATABASE" != "$database" || "$PGUSER" != "$application_role" ]]; then
	fail 'fixture PostgreSQL environment mismatch'
fi

server_version="$(read_query_as "$application_role" 'SHOW server_version')"
case "$server_version" in
"$expected_major".*) ;;
*) fail 'PostgreSQL server major did not match the requested major' ;;
esac
expect_equal "$(read_query_as "$application_role" 'SHOW statement_timeout')" \
	"${statement_timeout_seconds}s" 'application statement timeout'
expect_equal "$(read_query_as "$application_role" 'SELECT current_user')" \
	"$application_role" 'application role connection'
assert_schema_absent 'graph schema absence before mutation fixture'

if ! apply_migration; then
	assert_schema_absent 'graph schema absence after failed migration'
	fail 'graph migration application failed'
fi
schema_applied=true

assert_true 'concept creation' <<'SQL'
SELECT knowledge_graph.graph_create_concept(
  '018f0000-0000-7000-8000-000000000101'::uuid,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object(
      'alias_key', 'graph alpha',
      'display_text', 'Graph   Alpha',
      'is_preferred', true
    ),
    pg_catalog.jsonb_build_object(
      'alias_key', 'knowledge graph',
      'display_text', 'Knowledge   Graph',
      'is_preferred', false
    )
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'created',
  'record', pg_catalog.jsonb_build_object(
    'id', '018f0000-0000-7000-8000-000000000101'::uuid,
    'revision', 1,
    'aliases', pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object('display_text', 'Graph   Alpha', 'preferred', true),
      pg_catalog.jsonb_build_object('display_text', 'Knowledge   Graph', 'preferred', false)
    )
  )
)
SQL

assert_no_state_change 'concept identity retry' <<'SQL'
SELECT knowledge_graph.graph_create_concept(
  '018f0000-0000-7000-8000-000000000102'::uuid,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object(
      'alias_key', 'graph alpha',
      'display_text', 'Changed display',
      'is_preferred', true
    ),
    pg_catalog.jsonb_build_object(
      'alias_key', 'knowledge graph',
      'display_text', 'Changed additional',
      'is_preferred', false
    )
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'existing',
  'record', pg_catalog.jsonb_build_object(
    'id', '018f0000-0000-7000-8000-000000000101'::uuid,
    'revision', 1,
    'aliases', pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object('display_text', 'Graph   Alpha', 'preferred', true),
      pg_catalog.jsonb_build_object('display_text', 'Knowledge   Graph', 'preferred', false)
    )
  )
)
SQL

assert_no_state_change 'concept overlapping alias set conflict' <<'SQL'
SELECT knowledge_graph.graph_create_concept(
  '018f0000-0000-7000-8000-00000000010f'::uuid,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object(
      'alias_key', 'graph alpha',
      'display_text', 'Graph Alpha',
      'is_preferred', true
    ),
    pg_catalog.jsonb_build_object(
      'alias_key', 'new alias',
      'display_text', 'New Alias',
      'is_preferred', false
    )
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'conflict',
  'field', 'alias_set',
  'reason', 'alias_set_mismatch',
  'identity', pg_catalog.jsonb_build_object(
    'kind', 'concept',
    'id', '018f0000-0000-7000-8000-000000000101'::uuid
  ),
  'current_revision', 1
)
SQL

assert_no_state_change 'concept preferred alias conflict' <<'SQL'
SELECT knowledge_graph.graph_create_concept(
  '018f0000-0000-7000-8000-00000000010e'::uuid,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object(
      'alias_key', 'graph alpha',
      'display_text', 'Graph Alpha',
      'is_preferred', false
    ),
    pg_catalog.jsonb_build_object(
      'alias_key', 'knowledge graph',
      'display_text', 'Knowledge Graph',
      'is_preferred', true
    )
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'conflict',
  'field', 'alias_set',
  'reason', 'alias_set_mismatch',
  'identity', pg_catalog.jsonb_build_object(
    'kind', 'concept',
    'id', '018f0000-0000-7000-8000-000000000101'::uuid
  ),
  'current_revision', 1
)
SQL

assert_true 'concept alias replacement and revision' <<'SQL'
SELECT knowledge_graph.graph_replace_concept_aliases(
  '018f0000-0000-7000-8000-000000000101'::uuid,
  1,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object(
      'alias_key', 'science graph',
      'display_text', 'Science Graph',
      'is_preferred', true
    ),
    pg_catalog.jsonb_build_object(
      'alias_key', 'concept index',
      'display_text', 'Concept Index',
      'is_preferred', false
    )
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'updated',
  'record', pg_catalog.jsonb_build_object(
    'id', '018f0000-0000-7000-8000-000000000101'::uuid,
    'revision', 2,
    'aliases', pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object('display_text', 'Science Graph', 'preferred', true),
      pg_catalog.jsonb_build_object('display_text', 'Concept Index', 'preferred', false)
    )
  )
)
SQL

assert_true 'concept alias replacement is complete' <<'SQL'
SELECT concepts.revision = 2
  AND (SELECT pg_catalog.count(*) = 2
       FROM knowledge_graph.graph_aliases AS aliases
       WHERE aliases.concept_id = concepts.id)
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_aliases AS aliases
    WHERE aliases.concept_id = concepts.id
      AND aliases.alias_key = 'science graph' COLLATE "C"
      AND aliases.display_text = 'Science Graph'
      AND aliases.is_preferred
  )
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_aliases AS aliases
    WHERE aliases.concept_id = concepts.id
      AND aliases.alias_key = 'concept index' COLLATE "C"
      AND aliases.display_text = 'Concept Index'
      AND NOT aliases.is_preferred
  )
  AND NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_aliases AS aliases
    WHERE aliases.concept_id = concepts.id
      AND aliases.alias_key IN ('graph alpha', 'knowledge graph')
  )
FROM knowledge_graph.graph_concepts AS concepts
WHERE concepts.id = '018f0000-0000-7000-8000-000000000101'::uuid
SQL

assert_no_state_change 'invalid empty alias replacement' <<'SQL'
SELECT knowledge_graph.graph_replace_concept_aliases(
  '018f0000-0000-7000-8000-000000000101'::uuid,
  2,
  '[]'::jsonb
) = pg_catalog.jsonb_build_object(
  'outcome', 'conflict',
  'field', 'alias_set',
  'reason', 'alias_set_mismatch',
  'identity', pg_catalog.jsonb_build_object(
    'kind', 'concept',
    'id', '018f0000-0000-7000-8000-000000000101'::uuid
  ),
  'current_revision', 2
)
SQL

assert_no_state_change 'alias replacement without preferred alias' <<'SQL'
SELECT knowledge_graph.graph_replace_concept_aliases(
  '018f0000-0000-7000-8000-000000000101'::uuid,
  2,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object(
      'alias_key', 'unpreferred alpha',
      'display_text', 'Unpreferred Alpha',
      'is_preferred', false
    ),
    pg_catalog.jsonb_build_object(
      'alias_key', 'unpreferred beta',
      'display_text', 'Unpreferred Beta',
      'is_preferred', false
    )
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'conflict',
  'field', 'alias_set',
  'reason', 'alias_set_mismatch',
  'identity', pg_catalog.jsonb_build_object(
    'kind', 'concept',
    'id', '018f0000-0000-7000-8000-000000000101'::uuid
  ),
  'current_revision', 2
)
SQL

assert_no_state_change 'alias replacement with multiple preferred aliases' <<'SQL'
SELECT knowledge_graph.graph_replace_concept_aliases(
  '018f0000-0000-7000-8000-000000000101'::uuid,
  2,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object(
      'alias_key', 'preferred alpha',
      'display_text', 'Preferred Alpha',
      'is_preferred', true
    ),
    pg_catalog.jsonb_build_object(
      'alias_key', 'preferred beta',
      'display_text', 'Preferred Beta',
      'is_preferred', true
    )
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'conflict',
  'field', 'alias_set',
  'reason', 'alias_set_mismatch',
  'identity', pg_catalog.jsonb_build_object(
    'kind', 'concept',
    'id', '018f0000-0000-7000-8000-000000000101'::uuid
  ),
  'current_revision', 2
)
SQL

assert_no_state_change 'stale concept update' <<'SQL'
SELECT knowledge_graph.graph_replace_concept_aliases(
  '018f0000-0000-7000-8000-000000000101'::uuid,
  1,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object(
      'alias_key', 'stale alias',
      'display_text', 'Stale Alias',
      'is_preferred', true
    )
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'stale',
  'identity', pg_catalog.jsonb_build_object(
    'kind', 'concept',
    'id', '018f0000-0000-7000-8000-000000000101'::uuid
  ),
  'current_revision', 2
)
SQL

assert_no_state_change 'stale concept delete' <<'SQL'
SELECT knowledge_graph.graph_delete_concept(
  '018f0000-0000-7000-8000-000000000101'::uuid,
  1
) = pg_catalog.jsonb_build_object(
  'outcome', 'stale',
  'identity', pg_catalog.jsonb_build_object(
    'kind', 'concept',
    'id', '018f0000-0000-7000-8000-000000000101'::uuid
  ),
  'current_revision', 2
)
SQL

assert_true 'related concepts creation' <<'SQL'
SELECT knowledge_graph.graph_create_concept(
  '018f0000-0000-7000-8000-000000000102'::uuid,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object('alias_key', 'graph beta', 'display_text', 'Graph Beta', 'is_preferred', true),
    pg_catalog.jsonb_build_object('alias_key', 'beta alias', 'display_text', 'Beta Alias', 'is_preferred', false)
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'created',
  'record', pg_catalog.jsonb_build_object(
    'id', '018f0000-0000-7000-8000-000000000102'::uuid,
    'revision', 1,
    'aliases', pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object('display_text', 'Graph Beta', 'preferred', true),
      pg_catalog.jsonb_build_object('display_text', 'Beta Alias', 'preferred', false)
    )
  )
)
SQL

assert_true 'third concept creation' <<'SQL'
SELECT knowledge_graph.graph_create_concept(
  '018f0000-0000-7000-8000-000000000103'::uuid,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object('alias_key', 'graph gamma', 'display_text', 'Graph Gamma', 'is_preferred', true)
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'created',
  'record', pg_catalog.jsonb_build_object(
    'id', '018f0000-0000-7000-8000-000000000103'::uuid,
    'revision', 1,
    'aliases', pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object('display_text', 'Graph Gamma', 'preferred', true)
    )
  )
)
SQL

assert_true 'unrelated concept creation' <<'SQL'
SELECT knowledge_graph.graph_create_concept(
  '018f0000-0000-7000-8000-000000000104'::uuid,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object('alias_key', 'graph anchor', 'display_text', 'Graph Anchor', 'is_preferred', true)
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'created',
  'record', pg_catalog.jsonb_build_object(
    'id', '018f0000-0000-7000-8000-000000000104'::uuid,
    'revision', 1,
    'aliases', pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object('display_text', 'Graph Anchor', 'preferred', true)
    )
  )
)
SQL

assert_true 'unreferenced concept creation' <<'SQL'
SELECT knowledge_graph.graph_create_concept(
  '018f0000-0000-7000-8000-000000000105'::uuid,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object('alias_key', 'graph obsolete', 'display_text', 'Graph Obsolete', 'is_preferred', true),
    pg_catalog.jsonb_build_object('alias_key', 'obsolete alias', 'display_text', 'Obsolete Alias', 'is_preferred', false)
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'created',
  'record', pg_catalog.jsonb_build_object(
    'id', '018f0000-0000-7000-8000-000000000105'::uuid,
    'revision', 1,
    'aliases', pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object('display_text', 'Graph Obsolete', 'preferred', true),
      pg_catalog.jsonb_build_object('display_text', 'Obsolete Alias', 'preferred', false)
    )
  )
)
SQL

assert_no_state_change 'missing concept delete' <<'SQL'
SELECT knowledge_graph.graph_delete_concept(
  '018f0000-0000-7000-8000-000000000111'::uuid,
  1
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'concept',
  'identity', pg_catalog.jsonb_build_object(
    'kind', 'concept',
    'id', '018f0000-0000-7000-8000-000000000111'::uuid
  )
)
SQL

assert_true 'unreferenced concept hard delete outcome' <<'SQL'
SELECT knowledge_graph.graph_delete_concept(
  '018f0000-0000-7000-8000-000000000105'::uuid,
  1
) = pg_catalog.jsonb_build_object('outcome', 'deleted')
SQL

assert_true 'unreferenced concept hard delete absence' <<'SQL'
SELECT NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_concepts AS concepts
    WHERE concepts.id = '018f0000-0000-7000-8000-000000000105'::uuid
  )
  AND NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_aliases AS aliases
    WHERE aliases.concept_id = '018f0000-0000-7000-8000-000000000105'::uuid
       OR aliases.alias_key IN ('graph obsolete', 'obsolete alias')
  )
SQL

printf 'mutations concepts=alias-identity-revision-conflict-stale-reference-delete verified\n'

assert_true 'memory source registration shape' <<'SQL'
WITH result AS MATERIALIZED (
  SELECT knowledge_graph.graph_register_source(
    'memory_version', 'opaque-memory-owner-7', 7
  ) AS value
)
SELECT value->>'outcome' = 'created'
  AND pg_catalog.jsonb_typeof(value#>'{record,source_ref_id}') = 'number'
  AND (value->'record') - 'source_ref_id'::text = pg_catalog.jsonb_build_object(
    'source_kind', 'memory_version',
    'external_id', 'opaque-memory-owner-7',
    'external_version', 7
  )
FROM result
SQL

assert_true 'memory source registration persistence' <<'SQL'
SELECT EXISTS (
  SELECT 1
  FROM knowledge_graph.graph_source_references AS source_references
  WHERE source_references.source_kind = 'memory_version' COLLATE "C"
    AND source_references.external_id = 'opaque-memory-owner-7' COLLATE "C"
    AND source_references.external_version = 7
)
SQL

assert_no_state_change 'memory source registration retry' <<'SQL'
WITH result AS MATERIALIZED (
  SELECT knowledge_graph.graph_register_source(
    'memory_version', 'opaque-memory-owner-7', 7
  ) AS value
)
SELECT value->>'outcome' = 'existing'
  AND (value->'record') - 'source_ref_id'::text = pg_catalog.jsonb_build_object(
    'source_kind', 'memory_version',
    'external_id', 'opaque-memory-owner-7',
    'external_version', 7
  )
  AND (value#>>'{record,source_ref_id}')::bigint = (
    SELECT source_ref_id
    FROM knowledge_graph.graph_source_references
    WHERE source_kind = 'memory_version' COLLATE "C"
      AND external_id = 'opaque-memory-owner-7' COLLATE "C"
      AND external_version = 7
  )
  AND (SELECT pg_catalog.count(*) = 1
       FROM knowledge_graph.graph_source_references
       WHERE source_kind = 'memory_version' COLLATE "C"
	         AND external_id = 'opaque-memory-owner-7' COLLATE "C"
         AND external_version = 7)
FROM result
SQL

assert_true 'session source registration shape' <<'SQL'
WITH result AS MATERIALIZED (
  SELECT knowledge_graph.graph_register_source(
    'session_record', 'opaque-session-record-owner', NULL::bigint
  ) AS value
)
SELECT value->>'outcome' = 'created'
  AND pg_catalog.jsonb_typeof(value#>'{record,source_ref_id}') = 'number'
  AND (value->'record') - 'source_ref_id'::text = pg_catalog.jsonb_build_object(
    'source_kind', 'session_record',
    'external_id', 'opaque-session-record-owner',
    'external_version', NULL
  )
FROM result
SQL

assert_true 'session source registration persistence' <<'SQL'
SELECT EXISTS (
  SELECT 1
  FROM knowledge_graph.graph_source_references AS source_references
  WHERE source_references.source_kind = 'session_record' COLLATE "C"
    AND source_references.external_id = 'opaque-session-record-owner' COLLATE "C"
    AND source_references.external_version IS NULL
)
SQL

assert_no_state_change 'session source registration retry' <<'SQL'
WITH result AS MATERIALIZED (
  SELECT knowledge_graph.graph_register_source(
    'session_record', 'opaque-session-record-owner', NULL::bigint
  ) AS value
)
SELECT value->>'outcome' = 'existing'
  AND (value->'record') - 'source_ref_id'::text = pg_catalog.jsonb_build_object(
    'source_kind', 'session_record',
    'external_id', 'opaque-session-record-owner',
    'external_version', NULL
  )
  AND (value#>>'{record,source_ref_id}')::bigint = (
    SELECT source_ref_id
    FROM knowledge_graph.graph_source_references
    WHERE source_kind = 'session_record' COLLATE "C"
      AND external_id = 'opaque-session-record-owner' COLLATE "C"
      AND external_version IS NULL
  )
  AND (SELECT pg_catalog.count(*) = 1
       FROM knowledge_graph.graph_source_references
       WHERE source_kind = 'session_record' COLLATE "C"
         AND external_id = 'opaque-session-record-owner' COLLATE "C"
         AND external_version IS NULL)
FROM result
SQL

assert_true 'unreferenced source registration' <<'SQL'
SELECT knowledge_graph.graph_register_source(
  'memory_version', 'mutation-unreferenced-source', 9000
)->>'outcome' = 'created'
SQL

assert_true 'unrelated source registration' <<'SQL'
SELECT knowledge_graph.graph_register_source(
  'memory_version', 'mutation-anchor-source', 9001
)->>'outcome' = 'created'
SQL

assert_true 'overflow evidence source registration' <<'SQL'
SELECT knowledge_graph.graph_register_source(
  'memory_version', 'mutation-overflow-source', 9002
)->>'outcome' = 'created'
SQL

assert_true 'bounded evidence sources registration' <<'SQL'
WITH results AS MATERIALIZED (
  SELECT knowledge_graph.graph_register_source(
    'memory_version',
    'mutation-evidence-' || pg_catalog.lpad(source_number::text, 2, '0'),
    (1000 + source_number)::bigint
  ) AS value
  FROM pg_catalog.generate_series(1, 30) AS requested(source_number)
)
SELECT pg_catalog.count(*) = 30
  AND pg_catalog.bool_and(value->>'outcome' = 'created')
FROM results
SQL

assert_true 'opaque memory and session source identity shapes' <<'SQL'
SELECT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_source_references
    WHERE source_kind = 'memory_version' COLLATE "C"
      AND external_id = 'opaque-memory-owner-7' COLLATE "C"
      AND external_version = 7
  )
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_source_references
    WHERE source_kind = 'session_record' COLLATE "C"
      AND external_id = 'opaque-session-record-owner' COLLATE "C"
      AND external_version IS NULL
  )
SQL

assert_true 'memory concept mention creation' <<'SQL'
WITH result AS MATERIALIZED (
  SELECT knowledge_graph.graph_create_mention(
    '018f0000-0000-7000-8000-000000000101'::uuid,
    'memory_version', 'opaque-memory-owner-7', 7
  ) AS value
), source_row AS (
  SELECT source_ref_id
  FROM knowledge_graph.graph_source_references
  WHERE source_kind = 'memory_version' COLLATE "C"
    AND external_id = 'opaque-memory-owner-7' COLLATE "C"
    AND external_version = 7
)
SELECT value->>'outcome' = 'created'
  AND (value->'record') - 'source_ref_id'::text = pg_catalog.jsonb_build_object(
    'concept_id', '018f0000-0000-7000-8000-000000000101'::uuid,
    'source', pg_catalog.jsonb_build_object(
      'source_kind', 'memory_version',
      'external_id', 'opaque-memory-owner-7',
      'external_version', 7
    )
  )
  AND (value#>>'{record,source_ref_id}')::bigint = source_row.source_ref_id
FROM result CROSS JOIN source_row
SQL

assert_true 'memory concept mention persistence' <<'SQL'
SELECT EXISTS (
  SELECT 1
  FROM knowledge_graph.graph_concept_mentions AS mentions
  JOIN knowledge_graph.graph_source_references AS source_references
    ON source_references.source_ref_id = mentions.source_ref_id
  WHERE mentions.concept_id = '018f0000-0000-7000-8000-000000000101'::uuid
    AND source_references.source_kind = 'memory_version' COLLATE "C"
    AND source_references.external_id = 'opaque-memory-owner-7' COLLATE "C"
    AND source_references.external_version = 7
)
SQL

assert_true 'session-record concept mention creation' <<'SQL'
WITH result AS MATERIALIZED (
  SELECT knowledge_graph.graph_create_mention(
    '018f0000-0000-7000-8000-000000000102'::uuid,
    'session_record', 'opaque-session-record-owner', NULL::bigint
  ) AS value
), source_row AS (
  SELECT source_ref_id
  FROM knowledge_graph.graph_source_references
  WHERE source_kind = 'session_record' COLLATE "C"
    AND external_id = 'opaque-session-record-owner' COLLATE "C"
    AND external_version IS NULL
)
SELECT value->>'outcome' = 'created'
  AND (value->'record') - 'source_ref_id'::text = pg_catalog.jsonb_build_object(
    'concept_id', '018f0000-0000-7000-8000-000000000102'::uuid,
    'source', pg_catalog.jsonb_build_object(
      'source_kind', 'session_record',
      'external_id', 'opaque-session-record-owner',
      'external_version', NULL
    )
  )
  AND (value#>>'{record,source_ref_id}')::bigint = source_row.source_ref_id
FROM result CROSS JOIN source_row
SQL

assert_true 'session-record concept mention persistence' <<'SQL'
SELECT EXISTS (
  SELECT 1
  FROM knowledge_graph.graph_concept_mentions AS mentions
  JOIN knowledge_graph.graph_source_references AS source_references
    ON source_references.source_ref_id = mentions.source_ref_id
  WHERE mentions.concept_id = '018f0000-0000-7000-8000-000000000102'::uuid
    AND source_references.source_kind = 'session_record' COLLATE "C"
    AND source_references.external_id = 'opaque-session-record-owner' COLLATE "C"
    AND source_references.external_version IS NULL
)
SQL

assert_no_state_change 'memory mention identity retry' <<'SQL'
WITH result AS MATERIALIZED (
  SELECT knowledge_graph.graph_create_mention(
    '018f0000-0000-7000-8000-000000000101'::uuid,
    'memory_version', 'opaque-memory-owner-7', 7
  ) AS value
), source_row AS (
  SELECT source_ref_id
  FROM knowledge_graph.graph_source_references
  WHERE source_kind = 'memory_version' COLLATE "C"
    AND external_id = 'opaque-memory-owner-7' COLLATE "C"
    AND external_version = 7
)
SELECT value->>'outcome' = 'existing'
  AND (value->'record') - 'source_ref_id'::text = pg_catalog.jsonb_build_object(
    'concept_id', '018f0000-0000-7000-8000-000000000101'::uuid,
    'source', pg_catalog.jsonb_build_object(
      'source_kind', 'memory_version',
      'external_id', 'opaque-memory-owner-7',
      'external_version', 7
    )
  )
  AND (value#>>'{record,source_ref_id}')::bigint = source_row.source_ref_id
  AND (SELECT pg_catalog.count(*) = 1
       FROM knowledge_graph.graph_concept_mentions AS mentions
       WHERE mentions.concept_id = '018f0000-0000-7000-8000-000000000101'::uuid
         AND mentions.source_ref_id = source_row.source_ref_id)
FROM result CROSS JOIN source_row
SQL

assert_no_state_change 'session mention identity retry' <<'SQL'
WITH result AS MATERIALIZED (
  SELECT knowledge_graph.graph_create_mention(
    '018f0000-0000-7000-8000-000000000102'::uuid,
    'session_record', 'opaque-session-record-owner', NULL::bigint
  ) AS value
), source_row AS (
  SELECT source_ref_id
  FROM knowledge_graph.graph_source_references
  WHERE source_kind = 'session_record' COLLATE "C"
    AND external_id = 'opaque-session-record-owner' COLLATE "C"
    AND external_version IS NULL
)
SELECT value->>'outcome' = 'existing'
  AND (value->'record') - 'source_ref_id'::text = pg_catalog.jsonb_build_object(
    'concept_id', '018f0000-0000-7000-8000-000000000102'::uuid,
    'source', pg_catalog.jsonb_build_object(
      'source_kind', 'session_record',
      'external_id', 'opaque-session-record-owner',
      'external_version', NULL
    )
  )
  AND (value#>>'{record,source_ref_id}')::bigint = source_row.source_ref_id
  AND (SELECT pg_catalog.count(*) = 1
       FROM knowledge_graph.graph_concept_mentions AS mentions
       WHERE mentions.concept_id = '018f0000-0000-7000-8000-000000000102'::uuid
         AND mentions.source_ref_id = source_row.source_ref_id)
FROM result CROSS JOIN source_row
SQL

assert_no_state_change 'mention create with missing concept' <<'SQL'
SELECT knowledge_graph.graph_create_mention(
  '018f0000-0000-7000-8000-000000000111'::uuid,
  'memory_version', 'opaque-memory-owner-7', 7
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'concept',
  'identity', pg_catalog.jsonb_build_object(
    'kind', 'concept',
    'id', '018f0000-0000-7000-8000-000000000111'::uuid
  )
)
SQL

assert_no_state_change 'mention create with missing source' <<'SQL'
SELECT knowledge_graph.graph_create_mention(
  '018f0000-0000-7000-8000-000000000101'::uuid,
  'memory_version', 'mutation-missing-owner-source', 1
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'source_reference'
)
SQL

assert_no_state_change 'mention delete with missing concept' <<'SQL'
SELECT knowledge_graph.graph_delete_mention(
  '018f0000-0000-7000-8000-000000000111'::uuid,
  'memory_version', 'opaque-memory-owner-7', 7
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'concept',
  'identity', pg_catalog.jsonb_build_object(
    'kind', 'concept',
    'id', '018f0000-0000-7000-8000-000000000111'::uuid
  )
)
SQL

assert_no_state_change 'mention delete with missing source' <<'SQL'
SELECT knowledge_graph.graph_delete_mention(
  '018f0000-0000-7000-8000-000000000101'::uuid,
  'memory_version', 'mutation-missing-owner-source', 1
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'source_reference'
)
SQL

assert_no_state_change 'delete absent mention' <<'SQL'
SELECT knowledge_graph.graph_delete_mention(
  '018f0000-0000-7000-8000-000000000103'::uuid,
  'memory_version', 'opaque-memory-owner-7', 7
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'concept_mention'
)
SQL

assert_no_state_change 'missing source deletion' <<'SQL'
SELECT knowledge_graph.graph_delete_source(
  'memory_version', 'mutation-missing-owner-source', 1
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'source_reference'
)
SQL

assert_no_state_change 'referenced memory source deletion through mention' <<'SQL'
SELECT knowledge_graph.graph_delete_source(
  'memory_version', 'opaque-memory-owner-7', 7
) = pg_catalog.jsonb_build_object(
  'outcome', 'referenced',
  'reference_kind', 'source_reference'
)
SQL

assert_no_state_change 'referenced session source deletion through mention' <<'SQL'
SELECT knowledge_graph.graph_delete_source(
  'session_record', 'opaque-session-record-owner', NULL::bigint
) = pg_catalog.jsonb_build_object(
  'outcome', 'referenced',
  'reference_kind', 'source_reference'
)
SQL

assert_no_state_change 'referenced concept deletion through mention' <<'SQL'
SELECT knowledge_graph.graph_delete_concept(
  '018f0000-0000-7000-8000-000000000101'::uuid,
  2
) = pg_catalog.jsonb_build_object(
  'outcome', 'referenced',
  'reference', pg_catalog.jsonb_build_object(
    'kind', 'concept',
    'id', '018f0000-0000-7000-8000-000000000101'::uuid
  )
)
SQL

assert_true 'all fixed relation assertion creates' <<'SQL'
WITH requests(assertion_id, relation_code, input_subject, input_object, expected_subject, expected_object) AS (
  VALUES
    ('018f0000-0000-7000-8000-000000000201'::uuid, 'related_to',
      '018f0000-0000-7000-8000-000000000102'::uuid, '018f0000-0000-7000-8000-000000000101'::uuid,
      '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid),
    ('018f0000-0000-7000-8000-000000000202'::uuid, 'is_a',
      '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid,
      '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid),
    ('018f0000-0000-7000-8000-000000000203'::uuid, 'part_of',
      '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid,
      '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid),
    ('018f0000-0000-7000-8000-000000000204'::uuid, 'depends_on',
      '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid,
      '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid),
    ('018f0000-0000-7000-8000-000000000205'::uuid, 'uses',
      '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid,
      '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid),
    ('018f0000-0000-7000-8000-000000000206'::uuid, 'implements',
      '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid,
      '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid),
    ('018f0000-0000-7000-8000-000000000207'::uuid, 'causes',
      '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid,
      '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid),
    ('018f0000-0000-7000-8000-000000000208'::uuid, 'resolves',
      '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid,
      '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid),
    ('018f0000-0000-7000-8000-000000000209'::uuid, 'contradicts',
      '018f0000-0000-7000-8000-000000000102'::uuid, '018f0000-0000-7000-8000-000000000101'::uuid,
      '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid)
), results AS MATERIALIZED (
  SELECT requests.*,
    knowledge_graph.graph_create_assertion(
      assertion_id,
      input_subject,
      relation_code,
      input_object,
      pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'kind', 'memory_version',
          'memory_id', 'opaque-memory-owner-7',
          'version', 7
        )
      )
    ) AS value
  FROM requests
)
SELECT pg_catalog.count(*) = 9
  AND pg_catalog.bool_and(
    value->>'outcome' = 'created'
    AND (value->'record') - 'evidence'::text = pg_catalog.jsonb_build_object(
      'id', assertion_id,
      'revision', 1,
      'subject_concept_id', expected_subject,
      'relation_type', relation_code,
      'object_concept_id', expected_object
    )
    AND value#>'{record,evidence}' = pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'kind', 'memory_version',
        'memory_id', 'opaque-memory-owner-7',
        'version', 7
      )
    )
  )
FROM results
SQL

assert_true 'fixed relation direction and symmetric endpoint storage' <<'SQL'
WITH expected(assertion_id, relation_code, subject_id, object_id) AS (
  VALUES
    ('018f0000-0000-7000-8000-000000000201'::uuid, 'related_to', '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid),
    ('018f0000-0000-7000-8000-000000000202'::uuid, 'is_a', '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid),
    ('018f0000-0000-7000-8000-000000000203'::uuid, 'part_of', '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid),
    ('018f0000-0000-7000-8000-000000000204'::uuid, 'depends_on', '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid),
    ('018f0000-0000-7000-8000-000000000205'::uuid, 'uses', '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid),
    ('018f0000-0000-7000-8000-000000000206'::uuid, 'implements', '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid),
    ('018f0000-0000-7000-8000-000000000207'::uuid, 'causes', '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid),
    ('018f0000-0000-7000-8000-000000000208'::uuid, 'resolves', '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid),
    ('018f0000-0000-7000-8000-000000000209'::uuid, 'contradicts', '018f0000-0000-7000-8000-000000000101'::uuid, '018f0000-0000-7000-8000-000000000102'::uuid)
)
SELECT (SELECT pg_catalog.count(*) = 9
        FROM knowledge_graph.graph_assertions AS assertions)
  AND NOT EXISTS (
    SELECT 1
    FROM expected
    LEFT JOIN knowledge_graph.graph_assertions AS assertions
      ON assertions.id = expected.assertion_id
    WHERE assertions.id IS NULL
       OR assertions.relation_code <> expected.relation_code COLLATE "C"
       OR assertions.subject_concept_id <> expected.subject_id
       OR assertions.object_concept_id <> expected.object_id
  )
SQL

assert_no_state_change 'reversed symmetric assertion identity retry' <<'SQL'
SELECT knowledge_graph.graph_create_assertion(
  '018f0000-0000-7000-8000-000000000210'::uuid,
  '018f0000-0000-7000-8000-000000000102'::uuid,
  'related_to',
  '018f0000-0000-7000-8000-000000000101'::uuid,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object(
      'kind', 'memory_version',
      'memory_id', 'opaque-memory-owner-7',
      'version', 7
    )
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'existing',
  'record', pg_catalog.jsonb_build_object(
    'id', '018f0000-0000-7000-8000-000000000201'::uuid,
    'revision', 1,
    'subject_concept_id', '018f0000-0000-7000-8000-000000000101'::uuid,
    'relation_type', 'related_to',
    'object_concept_id', '018f0000-0000-7000-8000-000000000102'::uuid,
    'evidence', pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'kind', 'memory_version',
        'memory_id', 'opaque-memory-owner-7',
        'version', 7
      )
    )
  )
)
SQL

assert_true 'existing symmetric assertion create adds registered evidence' <<'SQL'
SELECT knowledge_graph.graph_create_assertion(
  '018f0000-0000-7000-8000-000000000218'::uuid,
  '018f0000-0000-7000-8000-000000000102'::uuid,
  'related_to',
  '018f0000-0000-7000-8000-000000000101'::uuid,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object(
      'kind', 'session_record',
      'session_record_id', 'opaque-session-record-owner'
    )
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'existing',
  'record', pg_catalog.jsonb_build_object(
    'id', '018f0000-0000-7000-8000-000000000201'::uuid,
    'revision', 1,
    'subject_concept_id', '018f0000-0000-7000-8000-000000000101'::uuid,
    'relation_type', 'related_to',
    'object_concept_id', '018f0000-0000-7000-8000-000000000102'::uuid,
    'evidence', pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'kind', 'memory_version',
        'memory_id', 'opaque-memory-owner-7',
        'version', 7
      ),
      pg_catalog.jsonb_build_object(
        'kind', 'session_record',
        'session_record_id', 'opaque-session-record-owner'
      )
    )
  )
)
SQL

assert_no_state_change 'existing symmetric assertion evidence-enrichment retry' <<'SQL'
SELECT knowledge_graph.graph_create_assertion(
  '018f0000-0000-7000-8000-000000000218'::uuid,
  '018f0000-0000-7000-8000-000000000102'::uuid,
  'related_to',
  '018f0000-0000-7000-8000-000000000101'::uuid,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object(
      'kind', 'session_record',
      'session_record_id', 'opaque-session-record-owner'
    )
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'existing',
  'record', pg_catalog.jsonb_build_object(
    'id', '018f0000-0000-7000-8000-000000000201'::uuid,
    'revision', 1,
    'subject_concept_id', '018f0000-0000-7000-8000-000000000101'::uuid,
    'relation_type', 'related_to',
    'object_concept_id', '018f0000-0000-7000-8000-000000000102'::uuid,
    'evidence', pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'kind', 'memory_version',
        'memory_id', 'opaque-memory-owner-7',
        'version', 7
      ),
      pg_catalog.jsonb_build_object(
        'kind', 'session_record',
        'session_record_id', 'opaque-session-record-owner'
      )
    )
  )
)
SQL

assert_no_state_change 'reversed contradictory assertion identity retry' <<'SQL'
SELECT knowledge_graph.graph_create_assertion(
  '018f0000-0000-7000-8000-000000000217'::uuid,
  '018f0000-0000-7000-8000-000000000101'::uuid,
  'contradicts',
  '018f0000-0000-7000-8000-000000000102'::uuid,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object(
      'kind', 'memory_version',
      'memory_id', 'opaque-memory-owner-7',
      'version', 7
    )
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'existing',
  'record', pg_catalog.jsonb_build_object(
    'id', '018f0000-0000-7000-8000-000000000209'::uuid,
    'revision', 1,
    'subject_concept_id', '018f0000-0000-7000-8000-000000000101'::uuid,
    'relation_type', 'contradicts',
    'object_concept_id', '018f0000-0000-7000-8000-000000000102'::uuid,
    'evidence', pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'kind', 'memory_version',
        'memory_id', 'opaque-memory-owner-7',
        'version', 7
      )
    )
  )
)
SQL

assert_no_state_change 'invalid assertion UUIDv7 create' <<'SQL'
SELECT knowledge_graph.graph_create_assertion(
  '00000000-0000-0000-0000-000000000001'::uuid,
  '018f0000-0000-7000-8000-000000000101'::uuid,
  'related_to',
  '018f0000-0000-7000-8000-000000000102'::uuid,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object('kind', 'memory_version', 'memory_id', 'opaque-memory-owner-7', 'version', 7)
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'invalid_input',
  'operation', 'create_assertion',
  'field', 'assertion_id',
  'reason', 'not_uuid_v7'
)
SQL

assert_no_state_change 'unsupported assertion relation' <<'SQL'
SELECT knowledge_graph.graph_create_assertion(
  '018f0000-0000-7000-8000-000000000211'::uuid,
  '018f0000-0000-7000-8000-000000000101'::uuid,
  'not_supported',
  '018f0000-0000-7000-8000-000000000102'::uuid,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object('kind', 'memory_version', 'memory_id', 'opaque-memory-owner-7', 'version', 7)
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'invalid_input',
  'operation', 'create_assertion',
  'field', 'relation_type',
  'reason', 'unsupported'
)
SQL

assert_no_state_change 'self assertion endpoint' <<'SQL'
SELECT knowledge_graph.graph_create_assertion(
  '018f0000-0000-7000-8000-000000000212'::uuid,
  '018f0000-0000-7000-8000-000000000101'::uuid,
  'related_to',
  '018f0000-0000-7000-8000-000000000101'::uuid,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object('kind', 'memory_version', 'memory_id', 'opaque-memory-owner-7', 'version', 7)
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'invalid_input',
  'operation', 'create_assertion',
  'field', 'subject_concept_id',
  'reason', 'self_relation'
)
SQL

assert_no_state_change 'assertion create without mandatory evidence' <<'SQL'
SELECT knowledge_graph.graph_create_assertion(
  '018f0000-0000-7000-8000-000000000213'::uuid,
  '018f0000-0000-7000-8000-000000000101'::uuid,
  'related_to',
  '018f0000-0000-7000-8000-000000000102'::uuid,
  '[]'::jsonb
) = pg_catalog.jsonb_build_object(
  'outcome', 'invalid_input',
  'operation', 'create_assertion',
  'field', 'supporting_sources',
  'reason', 'empty'
)
SQL

assert_no_state_change 'assertion create with missing subject concept' <<'SQL'
SELECT knowledge_graph.graph_create_assertion(
  '018f0000-0000-7000-8000-000000000214'::uuid,
  '018f0000-0000-7000-8000-000000000111'::uuid,
  'is_a',
  '018f0000-0000-7000-8000-000000000102'::uuid,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object('kind', 'memory_version', 'memory_id', 'opaque-memory-owner-7', 'version', 7)
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'concept',
  'identity', pg_catalog.jsonb_build_object(
    'kind', 'concept',
    'id', '018f0000-0000-7000-8000-000000000111'::uuid
  )
)
SQL

assert_no_state_change 'assertion create with missing object concept' <<'SQL'
SELECT knowledge_graph.graph_create_assertion(
  '018f0000-0000-7000-8000-000000000215'::uuid,
  '018f0000-0000-7000-8000-000000000101'::uuid,
  'is_a',
  '018f0000-0000-7000-8000-000000000111'::uuid,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object('kind', 'memory_version', 'memory_id', 'opaque-memory-owner-7', 'version', 7)
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'concept',
  'identity', pg_catalog.jsonb_build_object(
    'kind', 'concept',
    'id', '018f0000-0000-7000-8000-000000000111'::uuid
  )
)
SQL

assert_no_state_change 'assertion create with missing source' <<'SQL'
SELECT knowledge_graph.graph_create_assertion(
  '018f0000-0000-7000-8000-000000000216'::uuid,
  '018f0000-0000-7000-8000-000000000101'::uuid,
  'uses',
  '018f0000-0000-7000-8000-000000000102'::uuid,
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object('kind', 'memory_version', 'memory_id', 'mutation-missing-owner-source', 'version', 1)
  )
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'source_reference'
)
SQL

unreferenced_source_other_state_before="$(read_query_as "$application_role" "$non_source_snapshot_sql")"
assert_true 'unreferenced source hard delete outcome' <<'SQL'
SELECT knowledge_graph.graph_delete_source(
  'memory_version', 'mutation-unreferenced-source', 9000
) = pg_catalog.jsonb_build_object('outcome', 'deleted')
SQL
unreferenced_source_other_state_after="$(read_query_as "$application_role" "$non_source_snapshot_sql")"
expect_equal "$unreferenced_source_other_state_after" "$unreferenced_source_other_state_before" \
	'unreferenced source deletion preserves other graph state'

assert_true 'unreferenced source hard delete absence' <<'SQL'
SELECT NOT EXISTS (
  SELECT 1 FROM knowledge_graph.graph_source_references
  WHERE source_kind = 'memory_version' COLLATE "C"
    AND external_id = 'mutation-unreferenced-source' COLLATE "C"
    AND external_version = 9000
)
SQL

printf 'mutations sources=memory-session-opaque-idempotent-unreferenced-delete verified\n'
printf 'mutations assertions=all-nine-relations-directions-symmetric-retry-invalid-missing verified\n'

assert_no_state_change 'duplicate initial memory evidence' <<'SQL'
SELECT knowledge_graph.graph_add_assertion_evidence(
  '018f0000-0000-7000-8000-000000000202'::uuid,
  'memory_version', 'opaque-memory-owner-7', 7
) = pg_catalog.jsonb_build_object(
  'outcome', 'existing',
  'record', pg_catalog.jsonb_build_object(
    'assertion_id', '018f0000-0000-7000-8000-000000000202'::uuid,
    'source', pg_catalog.jsonb_build_object(
      'kind', 'memory_version',
      'memory_id', 'opaque-memory-owner-7',
      'version', 7
    )
  )
)
SQL

assert_true 'session evidence addition' <<'SQL'
SELECT knowledge_graph.graph_add_assertion_evidence(
  '018f0000-0000-7000-8000-000000000202'::uuid,
  'session_record', 'opaque-session-record-owner', NULL::bigint
) = pg_catalog.jsonb_build_object(
  'outcome', 'created',
  'record', pg_catalog.jsonb_build_object(
    'assertion_id', '018f0000-0000-7000-8000-000000000202'::uuid,
    'source', pg_catalog.jsonb_build_object(
      'kind', 'session_record',
      'session_record_id', 'opaque-session-record-owner'
    )
  )
)
SQL

assert_no_state_change 'session evidence addition retry' <<'SQL'
SELECT knowledge_graph.graph_add_assertion_evidence(
  '018f0000-0000-7000-8000-000000000202'::uuid,
  'session_record', 'opaque-session-record-owner', NULL::bigint
) = pg_catalog.jsonb_build_object(
  'outcome', 'existing',
  'record', pg_catalog.jsonb_build_object(
    'assertion_id', '018f0000-0000-7000-8000-000000000202'::uuid,
    'source', pg_catalog.jsonb_build_object(
      'kind', 'session_record',
      'session_record_id', 'opaque-session-record-owner'
    )
  )
)
SQL

assert_true 'evidence additions through the declared limit' <<'SQL'
WITH results AS MATERIALIZED (
  SELECT knowledge_graph.graph_add_assertion_evidence(
    '018f0000-0000-7000-8000-000000000202'::uuid,
    'memory_version',
    'mutation-evidence-' || pg_catalog.lpad(source_number::text, 2, '0'),
    (1000 + source_number)::bigint
  ) AS value
  FROM pg_catalog.generate_series(1, 30) AS requested(source_number)
)
SELECT pg_catalog.count(*) = 30
  AND pg_catalog.bool_and(value->>'outcome' = 'created')
FROM results
SQL

assert_true 'evidence count reaches 32 with both source variants' <<'SQL'
SELECT pg_catalog.count(*) = 32
  AND pg_catalog.count(*) FILTER (WHERE source_references.source_kind = 'memory_version') = 31
  AND pg_catalog.count(*) FILTER (WHERE source_references.source_kind = 'session_record') = 1
  AND EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_assertion_evidence AS evidence
    JOIN knowledge_graph.graph_source_references AS source_references
      ON source_references.source_ref_id = evidence.source_ref_id
    WHERE evidence.assertion_id = '018f0000-0000-7000-8000-000000000202'::uuid
      AND source_references.external_id = 'opaque-memory-owner-7' COLLATE "C"
      AND source_references.external_version = 7
  )
  AND EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_assertion_evidence AS evidence
    JOIN knowledge_graph.graph_source_references AS source_references
      ON source_references.source_ref_id = evidence.source_ref_id
    WHERE evidence.assertion_id = '018f0000-0000-7000-8000-000000000202'::uuid
      AND source_references.external_id = 'opaque-session-record-owner' COLLATE "C"
      AND source_references.external_version IS NULL
  )
FROM knowledge_graph.graph_assertion_evidence AS evidence
JOIN knowledge_graph.graph_source_references AS source_references
  ON source_references.source_ref_id = evidence.source_ref_id
WHERE evidence.assertion_id = '018f0000-0000-7000-8000-000000000202'::uuid
SQL

assert_no_state_change 'evidence 33rd source limit' <<'SQL'
SELECT knowledge_graph.graph_add_assertion_evidence(
  '018f0000-0000-7000-8000-000000000202'::uuid,
  'memory_version', 'mutation-overflow-source', 9002
) = pg_catalog.jsonb_build_object('outcome', 'evidence_limit')
SQL

assert_no_state_change 'duplicate evidence retry at the limit' <<'SQL'
SELECT knowledge_graph.graph_add_assertion_evidence(
  '018f0000-0000-7000-8000-000000000202'::uuid,
  'memory_version', 'opaque-memory-owner-7', 7
) = pg_catalog.jsonb_build_object(
  'outcome', 'existing',
  'record', pg_catalog.jsonb_build_object(
    'assertion_id', '018f0000-0000-7000-8000-000000000202'::uuid,
    'source', pg_catalog.jsonb_build_object(
      'kind', 'memory_version',
      'memory_id', 'opaque-memory-owner-7',
      'version', 7
    )
  )
)
SQL

assert_true 'selective evidence removal' <<'SQL'
SELECT knowledge_graph.graph_remove_assertion_evidence(
  '018f0000-0000-7000-8000-000000000202'::uuid,
  'session_record', 'opaque-session-record-owner', NULL::bigint
) = pg_catalog.jsonb_build_object('outcome', 'deleted')
SQL

assert_true 'selective evidence removal preserves other links' <<'SQL'
SELECT pg_catalog.count(*) = 31
  AND pg_catalog.count(*) FILTER (WHERE source_references.external_id = 'opaque-memory-owner-7') = 1
  AND pg_catalog.count(*) FILTER (WHERE source_references.external_id LIKE 'mutation-evidence-%') = 30
  AND NOT pg_catalog.bool_or(source_references.external_id = 'opaque-session-record-owner')
  AND NOT pg_catalog.bool_or(source_references.external_id = 'mutation-overflow-source')
  AND NOT EXISTS (
    SELECT 1
    FROM pg_catalog.generate_series(1, 30) AS requested(source_number)
    WHERE NOT EXISTS (
      SELECT 1
      FROM knowledge_graph.graph_assertion_evidence AS evidence
      JOIN knowledge_graph.graph_source_references AS source_references
        ON source_references.source_ref_id = evidence.source_ref_id
      WHERE evidence.assertion_id = '018f0000-0000-7000-8000-000000000202'::uuid
        AND source_references.external_id =
          'mutation-evidence-' || pg_catalog.lpad(requested.source_number::text, 2, '0')
        AND source_references.external_version = (1000 + requested.source_number)::bigint
    )
  )
FROM knowledge_graph.graph_assertion_evidence AS evidence
JOIN knowledge_graph.graph_source_references AS source_references
  ON source_references.source_ref_id = evidence.source_ref_id
WHERE evidence.assertion_id = '018f0000-0000-7000-8000-000000000202'::uuid
SQL

assert_true 'evidence restoration after selective removal' <<'SQL'
SELECT knowledge_graph.graph_add_assertion_evidence(
  '018f0000-0000-7000-8000-000000000202'::uuid,
  'session_record', 'opaque-session-record-owner', NULL::bigint
) = pg_catalog.jsonb_build_object(
  'outcome', 'created',
  'record', pg_catalog.jsonb_build_object(
    'assertion_id', '018f0000-0000-7000-8000-000000000202'::uuid,
    'source', pg_catalog.jsonb_build_object(
      'kind', 'session_record',
      'session_record_id', 'opaque-session-record-owner'
    )
  )
)
SQL

assert_no_state_change 'restored evidence idempotent retry' <<'SQL'
SELECT knowledge_graph.graph_add_assertion_evidence(
  '018f0000-0000-7000-8000-000000000202'::uuid,
  'session_record', 'opaque-session-record-owner', NULL::bigint
) = pg_catalog.jsonb_build_object(
  'outcome', 'existing',
  'record', pg_catalog.jsonb_build_object(
    'assertion_id', '018f0000-0000-7000-8000-000000000202'::uuid,
    'source', pg_catalog.jsonb_build_object(
      'kind', 'session_record',
      'session_record_id', 'opaque-session-record-owner'
    )
  )
)
SQL

assert_true 'evidence restored to the exact limit' <<'SQL'
SELECT pg_catalog.count(*) = 32
  AND pg_catalog.count(*) FILTER (WHERE source_references.source_kind = 'memory_version') = 31
  AND pg_catalog.count(*) FILTER (WHERE source_references.source_kind = 'session_record') = 1
FROM knowledge_graph.graph_assertion_evidence AS evidence
JOIN knowledge_graph.graph_source_references AS source_references
  ON source_references.source_ref_id = evidence.source_ref_id
WHERE evidence.assertion_id = '018f0000-0000-7000-8000-000000000202'::uuid
SQL

assert_no_state_change 'add evidence to missing assertion' <<'SQL'
SELECT knowledge_graph.graph_add_assertion_evidence(
  '018f0000-0000-7000-8000-0000000002ff'::uuid,
  'memory_version', 'opaque-memory-owner-7', 7
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'assertion'
)
SQL

assert_no_state_change 'add missing source as evidence' <<'SQL'
SELECT knowledge_graph.graph_add_assertion_evidence(
  '018f0000-0000-7000-8000-000000000202'::uuid,
  'memory_version', 'mutation-missing-owner-source', 1
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'source_reference'
)
SQL

assert_no_state_change 'remove evidence from missing assertion' <<'SQL'
SELECT knowledge_graph.graph_remove_assertion_evidence(
  '018f0000-0000-7000-8000-0000000002ff'::uuid,
  'memory_version', 'opaque-memory-owner-7', 7
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'assertion'
)
SQL

assert_no_state_change 'remove evidence with missing source' <<'SQL'
SELECT knowledge_graph.graph_remove_assertion_evidence(
  '018f0000-0000-7000-8000-000000000202'::uuid,
  'memory_version', 'mutation-missing-owner-source', 1
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'source_reference'
)
SQL

assert_no_state_change 'remove absent assertion evidence association' <<'SQL'
SELECT knowledge_graph.graph_remove_assertion_evidence(
  '018f0000-0000-7000-8000-000000000202'::uuid,
  'memory_version', 'mutation-overflow-source', 9002
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'assertion_evidence'
)
SQL

assert_no_state_change 'invalid assertion UUIDv7 evidence addition' <<'SQL'
SELECT knowledge_graph.graph_add_assertion_evidence(
  '00000000-0000-0000-0000-000000000001'::uuid,
  'memory_version', 'opaque-memory-owner-7', 7
) = pg_catalog.jsonb_build_object(
  'outcome', 'invalid_input',
  'operation', 'add_evidence',
  'field', 'assertion_id',
  'reason', 'not_uuid_v7'
)
SQL

assert_no_state_change 'invalid assertion UUIDv7 evidence removal' <<'SQL'
SELECT knowledge_graph.graph_remove_assertion_evidence(
  '00000000-0000-0000-0000-000000000001'::uuid,
  'memory_version', 'opaque-memory-owner-7', 7
) = pg_catalog.jsonb_build_object(
  'outcome', 'invalid_input',
  'operation', 'remove_evidence',
  'field', 'assertion_id',
  'reason', 'not_uuid_v7'
)
SQL

assert_no_state_change 'last evidence removal would orphan assertion' <<'SQL'
SELECT knowledge_graph.graph_remove_assertion_evidence(
  '018f0000-0000-7000-8000-000000000207'::uuid,
  'memory_version', 'opaque-memory-owner-7', 7
) = pg_catalog.jsonb_build_object(
  'outcome', 'would_orphan_evidence',
  'assertion_id', '018f0000-0000-7000-8000-000000000207'::uuid,
  'current_revision', 1
)
SQL

printf 'mutations evidence=mandatory-both-kinds-idempotent-limit-32-selective-remove-orphan verified\n'

assertion_evidence_before_update="$(assertion_evidence_snapshot '018f0000-0000-7000-8000-000000000202')"
assert_true 'assertion update retains complete evidence and increments revision' <<'SQL'
WITH result AS MATERIALIZED (
  SELECT knowledge_graph.graph_update_assertion(
    '018f0000-0000-7000-8000-000000000202'::uuid,
    1,
    '018f0000-0000-7000-8000-000000000101'::uuid,
    'is_a',
    '018f0000-0000-7000-8000-000000000103'::uuid
  ) AS value
), actual_evidence AS (
  SELECT COALESCE(
    pg_catalog.jsonb_agg(
      CASE source_references.source_kind COLLATE "C"
        WHEN 'memory_version' THEN pg_catalog.jsonb_build_object(
          'kind', 'memory_version',
          'memory_id', source_references.external_id,
          'version', source_references.external_version
        )
        ELSE pg_catalog.jsonb_build_object(
          'kind', 'session_record',
          'session_record_id', source_references.external_id
        )
      END
      ORDER BY source_references.source_kind COLLATE "C",
        source_references.external_id COLLATE "C",
        source_references.external_version ASC NULLS FIRST
    ),
    '[]'::jsonb
  ) AS evidence
  FROM knowledge_graph.graph_assertion_evidence AS evidence
  JOIN knowledge_graph.graph_source_references AS source_references
    ON source_references.source_ref_id = evidence.source_ref_id
  WHERE evidence.assertion_id = '018f0000-0000-7000-8000-000000000202'::uuid
)
SELECT value = pg_catalog.jsonb_build_object(
    'outcome', 'updated',
    'record', pg_catalog.jsonb_build_object(
      'id', '018f0000-0000-7000-8000-000000000202'::uuid,
      'revision', 2,
      'subject_concept_id', '018f0000-0000-7000-8000-000000000101'::uuid,
      'relation_type', 'is_a',
      'object_concept_id', '018f0000-0000-7000-8000-000000000103'::uuid,
      'evidence', actual_evidence.evidence
    )
  )
  AND actual_evidence.evidence = value#>'{record,evidence}'
  AND pg_catalog.jsonb_array_length(actual_evidence.evidence) = 32
  AND (SELECT pg_catalog.count(*) = 32
       FROM knowledge_graph.graph_assertion_evidence AS evidence
       WHERE evidence.assertion_id = '018f0000-0000-7000-8000-000000000202'::uuid)
FROM result CROSS JOIN actual_evidence
SQL
assertion_evidence_after_update="$(assertion_evidence_snapshot '018f0000-0000-7000-8000-000000000202')"
expect_equal "$assertion_evidence_after_update" "$assertion_evidence_before_update" \
	'assertion update evidence identity preservation'

assert_no_state_change 'assertion update duplicate semantic identity' <<'SQL'
SELECT knowledge_graph.graph_update_assertion(
  '018f0000-0000-7000-8000-000000000206'::uuid,
  1,
  '018f0000-0000-7000-8000-000000000101'::uuid,
  'uses',
  '018f0000-0000-7000-8000-000000000102'::uuid
) = pg_catalog.jsonb_build_object(
  'outcome', 'conflict',
  'field', 'semantic_assertion',
  'reason', 'duplicate_semantic_assertion',
  'identity', pg_catalog.jsonb_build_object(
    'kind', 'assertion',
    'id', '018f0000-0000-7000-8000-000000000205'::uuid
  ),
  'current_revision', 1
)
SQL

assert_no_state_change 'stale assertion update' <<'SQL'
SELECT knowledge_graph.graph_update_assertion(
  '018f0000-0000-7000-8000-000000000202'::uuid,
  1,
  '018f0000-0000-7000-8000-000000000101'::uuid,
  'is_a',
  '018f0000-0000-7000-8000-000000000102'::uuid
) = pg_catalog.jsonb_build_object(
  'outcome', 'stale',
  'identity', pg_catalog.jsonb_build_object(
    'kind', 'assertion',
    'id', '018f0000-0000-7000-8000-000000000202'::uuid
  ),
  'current_revision', 2
)
SQL

assert_no_state_change 'stale assertion delete' <<'SQL'
SELECT knowledge_graph.graph_delete_assertion(
  '018f0000-0000-7000-8000-000000000202'::uuid,
  1
) = pg_catalog.jsonb_build_object(
  'outcome', 'stale',
  'identity', pg_catalog.jsonb_build_object(
    'kind', 'assertion',
    'id', '018f0000-0000-7000-8000-000000000202'::uuid
  ),
  'current_revision', 2
)
SQL

assert_no_state_change 'invalid assertion update revision' <<'SQL'
SELECT knowledge_graph.graph_update_assertion(
  '018f0000-0000-7000-8000-000000000202'::uuid,
  0,
  '018f0000-0000-7000-8000-000000000101'::uuid,
  'is_a',
  '018f0000-0000-7000-8000-000000000103'::uuid
) = pg_catalog.jsonb_build_object(
  'outcome', 'invalid_input',
  'operation', 'update_assertion',
  'field', 'revision',
  'reason', 'not_positive'
)
SQL

assert_no_state_change 'invalid self assertion update' <<'SQL'
SELECT knowledge_graph.graph_update_assertion(
  '018f0000-0000-7000-8000-000000000202'::uuid,
  2,
  '018f0000-0000-7000-8000-000000000101'::uuid,
  'is_a',
  '018f0000-0000-7000-8000-000000000101'::uuid
) = pg_catalog.jsonb_build_object(
  'outcome', 'invalid_input',
  'operation', 'update_assertion',
  'field', 'subject_concept_id',
  'reason', 'self_relation'
)
SQL

assert_no_state_change 'invalid unsupported assertion update relation' <<'SQL'
SELECT knowledge_graph.graph_update_assertion(
  '018f0000-0000-7000-8000-000000000202'::uuid,
  2,
  '018f0000-0000-7000-8000-000000000101'::uuid,
  'not_supported',
  '018f0000-0000-7000-8000-000000000103'::uuid
) = pg_catalog.jsonb_build_object(
  'outcome', 'invalid_input',
  'operation', 'update_assertion',
  'field', 'relation_type',
  'reason', 'unsupported'
)
SQL

assert_no_state_change 'assertion update with missing concept' <<'SQL'
SELECT knowledge_graph.graph_update_assertion(
  '018f0000-0000-7000-8000-000000000202'::uuid,
  2,
  '018f0000-0000-7000-8000-000000000111'::uuid,
  'is_a',
  '018f0000-0000-7000-8000-000000000103'::uuid
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'concept',
  'identity', pg_catalog.jsonb_build_object(
    'kind', 'concept',
    'id', '018f0000-0000-7000-8000-000000000111'::uuid
  )
)
SQL

assert_no_state_change 'missing assertion update' <<'SQL'
SELECT knowledge_graph.graph_update_assertion(
  '018f0000-0000-7000-8000-0000000002ff'::uuid,
  1,
  '018f0000-0000-7000-8000-000000000101'::uuid,
  'is_a',
  '018f0000-0000-7000-8000-000000000103'::uuid
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'assertion',
  'identity', pg_catalog.jsonb_build_object(
    'kind', 'assertion',
    'id', '018f0000-0000-7000-8000-0000000002ff'::uuid
  )
)
SQL

assert_no_state_change 'invalid assertion delete revision' <<'SQL'
SELECT knowledge_graph.graph_delete_assertion(
  '018f0000-0000-7000-8000-000000000202'::uuid,
  0
) = pg_catalog.jsonb_build_object(
  'outcome', 'invalid_input',
  'operation', 'delete_assertion',
  'field', 'revision',
  'reason', 'not_positive'
)
SQL

assert_no_state_change 'missing assertion delete' <<'SQL'
SELECT knowledge_graph.graph_delete_assertion(
  '018f0000-0000-7000-8000-0000000002ff'::uuid,
  1
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'assertion',
  'identity', pg_catalog.jsonb_build_object(
    'kind', 'assertion',
    'id', '018f0000-0000-7000-8000-0000000002ff'::uuid
  )
)
SQL

assert_no_state_change 'referenced memory source deletion through evidence' <<'SQL'
SELECT knowledge_graph.graph_delete_source(
  'memory_version', 'opaque-memory-owner-7', 7
) = pg_catalog.jsonb_build_object(
  'outcome', 'referenced',
  'reference_kind', 'source_reference'
)
SQL

assert_no_state_change 'referenced session source deletion through evidence and mention' <<'SQL'
SELECT knowledge_graph.graph_delete_source(
  'session_record', 'opaque-session-record-owner', NULL::bigint
) = pg_catalog.jsonb_build_object(
  'outcome', 'referenced',
  'reference_kind', 'source_reference'
)
SQL

assert_no_state_change 'referenced concept deletion through assertions' <<'SQL'
SELECT knowledge_graph.graph_delete_concept(
  '018f0000-0000-7000-8000-000000000101'::uuid,
  2
) = pg_catalog.jsonb_build_object(
  'outcome', 'referenced',
  'reference', pg_catalog.jsonb_build_object(
    'kind', 'concept',
    'id', '018f0000-0000-7000-8000-000000000101'::uuid
  )
)
SQL

assert_true 'unrelated graph records survive rejected mutations' <<'SQL'
SELECT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_concepts
    WHERE id = '018f0000-0000-7000-8000-000000000104'::uuid
      AND revision = 1
  )
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_source_references
    WHERE source_kind = 'memory_version' COLLATE "C"
      AND external_id = 'mutation-anchor-source' COLLATE "C"
      AND external_version = 9001
  )
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_assertions
    WHERE id = '018f0000-0000-7000-8000-000000000205'::uuid
      AND subject_concept_id = '018f0000-0000-7000-8000-000000000101'::uuid
      AND relation_code = 'uses' COLLATE "C"
      AND object_concept_id = '018f0000-0000-7000-8000-000000000102'::uuid
  )
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_concept_mentions AS mentions
    JOIN knowledge_graph.graph_source_references AS source_references
      ON source_references.source_ref_id = mentions.source_ref_id
    WHERE mentions.concept_id = '018f0000-0000-7000-8000-000000000102'::uuid
      AND source_references.source_kind = 'session_record' COLLATE "C"
      AND source_references.external_id = 'opaque-session-record-owner' COLLATE "C"
  )
SQL

assert_true 'assertion deletion cascade outcome' <<'SQL'
SELECT knowledge_graph.graph_delete_assertion(
  '018f0000-0000-7000-8000-000000000207'::uuid,
  1
) = pg_catalog.jsonb_build_object('outcome', 'deleted')
SQL

assert_true 'assertion deletion cascades every evidence row' <<'SQL'
SELECT NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_assertions
    WHERE id = '018f0000-0000-7000-8000-000000000207'::uuid
  )
  AND NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_assertion_evidence
    WHERE assertion_id = '018f0000-0000-7000-8000-000000000207'::uuid
  )
SQL

assert_no_state_change 'deleted assertion retry reports missing' <<'SQL'
SELECT knowledge_graph.graph_delete_assertion(
  '018f0000-0000-7000-8000-000000000207'::uuid,
  1
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'assertion',
  'identity', pg_catalog.jsonb_build_object(
    'kind', 'assertion',
    'id', '018f0000-0000-7000-8000-000000000207'::uuid
  )
)
SQL

non_mention_state_before_delete="$(read_query_as "$application_role" "$non_mention_snapshot_sql")"
assert_true 'mention deletion outcome' <<'SQL'
SELECT knowledge_graph.graph_delete_mention(
  '018f0000-0000-7000-8000-000000000101'::uuid,
  'memory_version', 'opaque-memory-owner-7', 7
) = pg_catalog.jsonb_build_object('outcome', 'deleted')
SQL

assert_true 'mention deletion removes only its association' <<'SQL'
SELECT NOT EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_concept_mentions AS mentions
    JOIN knowledge_graph.graph_source_references AS source_references
      ON source_references.source_ref_id = mentions.source_ref_id
    WHERE mentions.concept_id = '018f0000-0000-7000-8000-000000000101'::uuid
      AND source_references.source_kind = 'memory_version' COLLATE "C"
      AND source_references.external_id = 'opaque-memory-owner-7' COLLATE "C"
      AND source_references.external_version = 7
  )
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_concepts
    WHERE id = '018f0000-0000-7000-8000-000000000101'::uuid
      AND revision = 2
  )
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_source_references
    WHERE source_kind = 'memory_version' COLLATE "C"
      AND external_id = 'opaque-memory-owner-7' COLLATE "C"
      AND external_version = 7
  )
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_assertions
    WHERE id = '018f0000-0000-7000-8000-000000000202'::uuid
      AND revision = 2
  )
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_assertion_evidence
    WHERE assertion_id = '018f0000-0000-7000-8000-000000000202'::uuid
  )
SQL
non_mention_state_after_delete="$(read_query_as "$application_role" "$non_mention_snapshot_sql")"
expect_equal "$non_mention_state_after_delete" "$non_mention_state_before_delete" \
	'mention deletion non-association state preservation'

assert_no_state_change 'deleted mention retry reports missing' <<'SQL'
SELECT knowledge_graph.graph_delete_mention(
  '018f0000-0000-7000-8000-000000000101'::uuid,
  'memory_version', 'opaque-memory-owner-7', 7
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'concept_mention'
)
SQL

assert_no_state_change 'memory source remains referenced by assertions' <<'SQL'
SELECT knowledge_graph.graph_delete_source(
  'memory_version', 'opaque-memory-owner-7', 7
) = pg_catalog.jsonb_build_object(
  'outcome', 'referenced',
  'reference_kind', 'source_reference'
)
SQL

printf 'mutations lifecycle=all-or-none-stale-referenced-revisions-cascades verified\n'

assert_true 'remaining assertion and evidence hard deletes' <<'SQL'
WITH requests(assertion_id, expected_revision) AS (
  VALUES
    ('018f0000-0000-7000-8000-000000000201'::uuid, 1::bigint),
    ('018f0000-0000-7000-8000-000000000202'::uuid, 2::bigint),
    ('018f0000-0000-7000-8000-000000000203'::uuid, 1::bigint),
    ('018f0000-0000-7000-8000-000000000204'::uuid, 1::bigint),
    ('018f0000-0000-7000-8000-000000000205'::uuid, 1::bigint),
    ('018f0000-0000-7000-8000-000000000206'::uuid, 1::bigint),
    ('018f0000-0000-7000-8000-000000000208'::uuid, 1::bigint),
    ('018f0000-0000-7000-8000-000000000209'::uuid, 1::bigint)
), results AS MATERIALIZED (
  SELECT knowledge_graph.graph_delete_assertion(
    assertion_id,
    expected_revision
  ) AS value
  FROM requests
)
SELECT pg_catalog.count(*) = 8
  AND pg_catalog.bool_and(value = pg_catalog.jsonb_build_object('outcome', 'deleted'))
FROM results
SQL

assert_true 'assertion hard delete leaves no assertions or evidence' <<'SQL'
SELECT NOT EXISTS (SELECT 1 FROM knowledge_graph.graph_assertions)
  AND NOT EXISTS (SELECT 1 FROM knowledge_graph.graph_assertion_evidence)
SQL

assert_true 'session mention hard delete outcome' <<'SQL'
SELECT knowledge_graph.graph_delete_mention(
  '018f0000-0000-7000-8000-000000000102'::uuid,
  'session_record', 'opaque-session-record-owner', NULL::bigint
) = pg_catalog.jsonb_build_object('outcome', 'deleted')
SQL

assert_true 'session mention hard delete absence' <<'SQL'
SELECT NOT EXISTS (
  SELECT 1
  FROM knowledge_graph.graph_concept_mentions AS mentions
  JOIN knowledge_graph.graph_source_references AS source_references
    ON source_references.source_ref_id = mentions.source_ref_id
  WHERE mentions.concept_id = '018f0000-0000-7000-8000-000000000102'::uuid
    AND source_references.source_kind = 'session_record' COLLATE "C"
    AND source_references.external_id = 'opaque-session-record-owner' COLLATE "C"
)
SQL

assert_true 'memory source hard delete outcome' <<'SQL'
SELECT knowledge_graph.graph_delete_source(
  'memory_version', 'opaque-memory-owner-7', 7
) = pg_catalog.jsonb_build_object('outcome', 'deleted')
SQL

assert_true 'memory source hard delete absence' <<'SQL'
SELECT NOT EXISTS (
  SELECT 1
  FROM knowledge_graph.graph_source_references AS source_references
  WHERE source_references.source_kind = 'memory_version' COLLATE "C"
    AND source_references.external_id = 'opaque-memory-owner-7' COLLATE "C"
    AND source_references.external_version = 7
)
SQL

assert_no_state_change 'deleted memory source retry reports missing' <<'SQL'
SELECT knowledge_graph.graph_delete_source(
  'memory_version', 'opaque-memory-owner-7', 7
) = pg_catalog.jsonb_build_object(
  'outcome', 'missing',
  'record_kind', 'source_reference'
)
SQL

assert_true 'session source hard delete outcome' <<'SQL'
SELECT knowledge_graph.graph_delete_source(
  'session_record', 'opaque-session-record-owner', NULL::bigint
) = pg_catalog.jsonb_build_object('outcome', 'deleted')
SQL

assert_true 'session source hard delete absence' <<'SQL'
SELECT NOT EXISTS (
  SELECT 1
  FROM knowledge_graph.graph_source_references AS source_references
  WHERE source_references.source_kind = 'session_record' COLLATE "C"
    AND source_references.external_id = 'opaque-session-record-owner' COLLATE "C"
    AND source_references.external_version IS NULL
)
SQL

assert_true 'concept hard deletes after references are removed' <<'SQL'
SELECT knowledge_graph.graph_delete_concept(
    '018f0000-0000-7000-8000-000000000101'::uuid,
    2
  ) = pg_catalog.jsonb_build_object('outcome', 'deleted')
  AND knowledge_graph.graph_delete_concept(
    '018f0000-0000-7000-8000-000000000102'::uuid,
    1
  ) = pg_catalog.jsonb_build_object('outcome', 'deleted')
  AND knowledge_graph.graph_delete_concept(
    '018f0000-0000-7000-8000-000000000103'::uuid,
    1
  ) = pg_catalog.jsonb_build_object('outcome', 'deleted')
SQL

assert_true 'hard-delete absence and unrelated record retention' <<'SQL'
SELECT NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_concepts
    WHERE id IN (
      '018f0000-0000-7000-8000-000000000101'::uuid,
      '018f0000-0000-7000-8000-000000000102'::uuid,
      '018f0000-0000-7000-8000-000000000103'::uuid,
      '018f0000-0000-7000-8000-000000000105'::uuid
    )
  )
  AND NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_aliases
    WHERE concept_id IN (
      '018f0000-0000-7000-8000-000000000101'::uuid,
      '018f0000-0000-7000-8000-000000000102'::uuid,
      '018f0000-0000-7000-8000-000000000103'::uuid,
      '018f0000-0000-7000-8000-000000000105'::uuid
    )
       OR alias_key IN (
         'graph alpha', 'knowledge graph', 'science graph', 'concept index',
         'graph beta', 'beta alias', 'graph gamma', 'graph obsolete', 'obsolete alias'
       )
  )
  AND NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_concept_mentions AS mentions
    WHERE mentions.concept_id IN (
      '018f0000-0000-7000-8000-000000000101'::uuid,
      '018f0000-0000-7000-8000-000000000102'::uuid
    )
  )
  AND NOT EXISTS (SELECT 1 FROM knowledge_graph.graph_assertions)
  AND NOT EXISTS (SELECT 1 FROM knowledge_graph.graph_assertion_evidence)
  AND NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_source_references
    WHERE (source_kind = 'memory_version' COLLATE "C"
           AND external_id IN ('opaque-memory-owner-7', 'mutation-unreferenced-source')
           AND external_version IN (7, 9000))
       OR (source_kind = 'session_record' COLLATE "C"
           AND external_id = 'opaque-session-record-owner' COLLATE "C"
           AND external_version IS NULL)
  )
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_concepts
    WHERE id = '018f0000-0000-7000-8000-000000000104'::uuid
      AND revision = 1
  )
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_aliases
    WHERE concept_id = '018f0000-0000-7000-8000-000000000104'::uuid
      AND alias_key = 'graph anchor' COLLATE "C"
      AND is_preferred
  )
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_source_references
    WHERE source_kind = 'memory_version' COLLATE "C"
      AND external_id = 'mutation-anchor-source' COLLATE "C"
      AND external_version = 9001
  )
SQL

printf 'mutations hard-delete=concept-alias-mention-source-assertion-evidence absence verified\n'
printf 'mutations atomicity=full-graph-snapshots invalid-conflict-stale-reference-limit-orphan verified\n'
printf 'mutations worker-prerequisite=%s declared worker=not-started\n' "$KNOWLEDGE_GRAPH_ROW_CHANGE_MODE"

run_concurrency_verification() {
	local attempt
	local base
	local concept_id_a
	local concept_id_b
	local concept_id_c
	local concept_id_d
	local assertion_id
	local assertion_id_a
	local assertion_id_b
	local alias_key
	local alias_a
	local alias_b
	local shared_alias
	local exclusive_alias_a
	local exclusive_alias_b
	local winner_alias
	local loser_alias
	local loser_id
	local mutation_a_sql
	local mutation_b_sql
	local lock_keys_sql
	local expected_wait_lock_id
	local supporting_sources_sql
	local source_a
	local source_b
	local source_key
	local source_version
	local source_ref_id
	local source_present
	local before_snapshot
	local after_snapshot
	local expected_outcome
	local remaining_source
	local evidence_assertion_id
	local cap_prefix
	local cap_version_base
	local cap_winner_source
	local cap_winner_version
	local cap_loser_source
	local delete_outcome
	local add_outcome
	local final_state

	create_race_source 'kg-race-anchor-source' 7000
	supporting_sources_sql="pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('kind', 'memory_version', 'memory_id', 'kg-race-anchor-source', 'version', 7000))"

	for attempt in 1 2 3 4 5; do
		base=$((10000 + attempt * 10))
		concept_id_a="$(race_uuid "$base")"
		concept_id_b="$(race_uuid "$((base + 1))")"
		alias_key="kg-race-identical-${attempt}"
		mutation_a_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_create_concept(
				'${concept_id_a}'::uuid,
				pg_catalog.jsonb_build_array(
					pg_catalog.jsonb_build_object('alias_key', '${alias_key}', 'display_text', '${alias_key}', 'is_preferred', true)
				)
			)
		")"
		mutation_b_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_create_concept(
				'${concept_id_b}'::uuid,
				pg_catalog.jsonb_build_array(
					pg_catalog.jsonb_build_object('alias_key', '${alias_key}', 'display_text', '${alias_key}', 'is_preferred', true)
				)
			)
		")"
		run_two_session_race 'identical concept create race' "$mutation_a_sql" "$mutation_b_sql"
		expect_created_existing_pair 'identical concept create race' "$concept_id_a" "$concept_id_b"
		if [[ "$race_winner_slot" == a ]]; then
			loser_id=$concept_id_b
		else
			loser_id=$concept_id_a
		fi
		assert_true 'identical concept create leaves one canonical concept' <<SQL
SELECT (SELECT pg_catalog.count(*) = 1
        FROM knowledge_graph.graph_concepts
        WHERE id = '${race_winner_id}'::uuid AND revision = 1)
  AND (SELECT pg_catalog.count(*) = 1
       FROM knowledge_graph.graph_aliases
       WHERE concept_id = '${race_winner_id}'::uuid)
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_aliases
    WHERE concept_id = '${race_winner_id}'::uuid
      AND alias_key = '${alias_key}' COLLATE "C"
      AND is_preferred
  )
  AND NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_concepts
    WHERE id = '${loser_id}'::uuid
  )
SQL
	done
	printf 'concurrency concepts=identical-absent-alias-races:5 one-created-one-existing canonical-id verified\n'

	for attempt in 1 2 3 4 5; do
		base=$((20000 + attempt * 10))
		concept_id_a="$(race_uuid "$base")"
		concept_id_b="$(race_uuid "$((base + 1))")"
		shared_alias="kg-race-overlap-${attempt}-shared"
		exclusive_alias_a="kg-race-overlap-${attempt}-a"
		exclusive_alias_b="kg-race-overlap-${attempt}-b"
		mutation_a_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_create_concept(
				'${concept_id_a}'::uuid,
				pg_catalog.jsonb_build_array(
					pg_catalog.jsonb_build_object('alias_key', '${shared_alias}', 'display_text', '${shared_alias}', 'is_preferred', true),
					pg_catalog.jsonb_build_object('alias_key', '${exclusive_alias_a}', 'display_text', '${exclusive_alias_a}', 'is_preferred', false)
				)
			)
		")"
		mutation_b_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_create_concept(
				'${concept_id_b}'::uuid,
				pg_catalog.jsonb_build_array(
					pg_catalog.jsonb_build_object('alias_key', '${shared_alias}', 'display_text', '${shared_alias}', 'is_preferred', true),
					pg_catalog.jsonb_build_object('alias_key', '${exclusive_alias_b}', 'display_text', '${exclusive_alias_b}', 'is_preferred', false)
				)
			)
		")"
		run_two_session_race 'overlapping concept create race' "$mutation_a_sql" "$mutation_b_sql"
		expect_overlapping_concept_create_pair 'overlapping concept create race' "$concept_id_a" "$concept_id_b"
		if [[ "$race_winner_slot" == a ]]; then
			winner_alias=$exclusive_alias_a
			loser_alias=$exclusive_alias_b
			loser_id=$concept_id_b
		else
			winner_alias=$exclusive_alias_b
			loser_alias=$exclusive_alias_a
			loser_id=$concept_id_a
		fi
		assert_true 'overlapping concept create preserves rejected alias set' <<SQL
SELECT (SELECT pg_catalog.count(*) = 1
        FROM knowledge_graph.graph_concepts
        WHERE id = '${race_winner_id}'::uuid AND revision = 1)
  AND (SELECT pg_catalog.count(*) = 2
       FROM knowledge_graph.graph_aliases
       WHERE concept_id = '${race_winner_id}'::uuid)
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_aliases
    WHERE concept_id = '${race_winner_id}'::uuid
      AND alias_key = '${shared_alias}' COLLATE "C"
      AND is_preferred
  )
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_aliases
    WHERE concept_id = '${race_winner_id}'::uuid
      AND alias_key = '${winner_alias}' COLLATE "C"
      AND NOT is_preferred
  )
  AND NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_aliases
    WHERE alias_key = '${loser_alias}' COLLATE "C"
  )
  AND NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_concepts
    WHERE id = '${loser_id}'::uuid
  )
SQL
	done
	printf 'concurrency concepts=overlapping-alias-create-races:5 one-canonical-set typed-conflict unchanged verified\n'

	for attempt in 1 2 3 4 5; do
		base=$((30000 + attempt * 10))
		concept_id_a="$(race_uuid "$base")"
		concept_id_b="$(race_uuid "$((base + 1))")"
		alias_a="kg-race-swap-${attempt}-a"
		alias_b="kg-race-swap-${attempt}-b"
		create_race_concept "$concept_id_a" "$alias_a"
		create_race_concept "$concept_id_b" "$alias_b"
		before_snapshot="$(snapshot_state)"
		mutation_a_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_replace_concept_aliases(
				'${concept_id_a}'::uuid,
				1,
				pg_catalog.jsonb_build_array(
					pg_catalog.jsonb_build_object('alias_key', '${alias_b}', 'display_text', '${alias_b}', 'is_preferred', true)
				)
			)
		")"
		mutation_b_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_replace_concept_aliases(
				'${concept_id_b}'::uuid,
				1,
				pg_catalog.jsonb_build_array(
					pg_catalog.jsonb_build_object('alias_key', '${alias_a}', 'display_text', '${alias_a}', 'is_preferred', true)
				)
			)
		")"
		lock_keys_sql="pg_catalog.jsonb_build_array(
			knowledge_graph.graph_lock_key_normalized_alias('${alias_a}'),
			knowledge_graph.graph_lock_key_normalized_alias('${alias_b}')
		)"
		expected_wait_lock_id="$(race_min_lock_id_from_keys "$lock_keys_sql")"
		run_two_session_race 'concept alias cross-swap race' "$mutation_a_sql" "$mutation_b_sql" "$expected_wait_lock_id"
		expect_equal "$race_result_a" \
			"conflict|alias_set|alias_owned|||${concept_id_b}|1" \
			'first concept alias cross-swap typed conflict'
		expect_equal "$race_result_b" \
			"conflict|alias_set|alias_owned|||${concept_id_a}|1" \
			'second concept alias cross-swap typed conflict'
		after_snapshot="$(snapshot_state)"
		expect_equal "$after_snapshot" "$before_snapshot" 'concept alias cross-swap unchanged graph snapshot'
	done
	printf 'concurrency aliases=cross-concept-swaps:5 both-conflict full-state-unchanged verified\n'

	for attempt in 1 2 3 4 5; do
		base=$((40000 + attempt * 10))
		concept_id_a="$(race_uuid "$base")"
		alias_key="kg-race-revision-${attempt}-current"
		alias_a="kg-race-revision-${attempt}-a"
		alias_b="kg-race-revision-${attempt}-b"
		create_race_concept "$concept_id_a" "$alias_key"
		mutation_a_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_replace_concept_aliases(
				'${concept_id_a}'::uuid,
				1,
				pg_catalog.jsonb_build_array(
					pg_catalog.jsonb_build_object('alias_key', '${alias_a}', 'display_text', '${alias_a}', 'is_preferred', true)
				)
			)
		")"
		mutation_b_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_replace_concept_aliases(
				'${concept_id_a}'::uuid,
				1,
				pg_catalog.jsonb_build_array(
					pg_catalog.jsonb_build_object('alias_key', '${alias_b}', 'display_text', '${alias_b}', 'is_preferred', true)
				)
			)
		")"
		run_two_session_race 'concept revision update race' "$mutation_a_sql" "$mutation_b_sql"
		expect_revision_update_pair 'concept revision update race' "$concept_id_a"
		if [[ "$race_winner_slot" == a ]]; then
			winner_alias=$alias_a
			loser_alias=$alias_b
		else
			winner_alias=$alias_b
			loser_alias=$alias_a
		fi
		assert_true 'concept revision race applies one complete alias replacement' <<SQL
SELECT concepts.revision = 2
  AND (SELECT pg_catalog.count(*) = 1
       FROM knowledge_graph.graph_aliases AS aliases
       WHERE aliases.concept_id = concepts.id)
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_aliases AS aliases
    WHERE aliases.concept_id = concepts.id
      AND aliases.alias_key = '${winner_alias}' COLLATE "C"
      AND aliases.is_preferred
  )
  AND NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_aliases AS aliases
    WHERE aliases.concept_id = concepts.id
      AND aliases.alias_key IN ('${alias_key}', '${loser_alias}')
  )
FROM knowledge_graph.graph_concepts AS concepts
WHERE concepts.id = '${concept_id_a}'::uuid
SQL
	done
	printf 'concurrency concepts=revision-update-races:5 one-updated one-stale-or-snapshot-conflict final-state verified\n'

	for attempt in 1 2 3 4 5; do
		base=$((50000 + attempt * 10))
		concept_id_a="$(race_uuid "$base")"
		concept_id_b="$(race_uuid "$((base + 1))")"
		assertion_id_a="$(race_uuid "$((base + 2))")"
		assertion_id_b="$(race_uuid "$((base + 3))")"
		source_a="kg-race-create-${attempt}-a"
		source_b="kg-race-create-${attempt}-b"
		source_version=$((50000 + attempt))
		create_race_concept "$concept_id_a" "kg-race-create-${attempt}-subject"
		create_race_concept "$concept_id_b" "kg-race-create-${attempt}-object"
		create_race_source "$source_a" "$source_version"
		create_race_source "$source_b" "$((source_version + 100))"
		mutation_a_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_create_assertion(
				'${assertion_id_a}'::uuid,
				'${concept_id_a}'::uuid,
				'related_to',
				'${concept_id_b}'::uuid,
				pg_catalog.jsonb_build_array(
					pg_catalog.jsonb_build_object('kind', 'memory_version', 'memory_id', '${source_a}', 'version', ${source_version})
				)
			)
		")"
		mutation_b_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_create_assertion(
				'${assertion_id_b}'::uuid,
				'${concept_id_b}'::uuid,
				'related_to',
				'${concept_id_a}'::uuid,
				pg_catalog.jsonb_build_array(
					pg_catalog.jsonb_build_object('kind', 'memory_version', 'memory_id', '${source_b}', 'version', $((source_version + 100)))
				)
			)
		")"
		run_two_session_race 'symmetric assertion create race' "$mutation_a_sql" "$mutation_b_sql"
		expect_created_existing_pair 'symmetric assertion create race' "$assertion_id_a" "$assertion_id_b"
		assert_true 'symmetric assertion create has one identity and both evidence links' <<SQL
SELECT (SELECT pg_catalog.count(*) = 1
        FROM knowledge_graph.graph_assertions
        WHERE id IN ('${assertion_id_a}'::uuid, '${assertion_id_b}'::uuid))
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_assertions AS assertions
    WHERE assertions.id = '${race_winner_id}'::uuid
      AND assertions.revision = 1
      AND assertions.relation_code = 'related_to' COLLATE "C"
      AND ((assertions.subject_concept_id = '${concept_id_a}'::uuid
            AND assertions.object_concept_id = '${concept_id_b}'::uuid)
        OR (assertions.subject_concept_id = '${concept_id_b}'::uuid
            AND assertions.object_concept_id = '${concept_id_a}'::uuid))
  )
  AND (SELECT pg_catalog.count(*) = 2
       FROM knowledge_graph.graph_assertion_evidence AS evidence
       WHERE evidence.assertion_id = '${race_winner_id}'::uuid)
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_assertion_evidence AS evidence
    JOIN knowledge_graph.graph_source_references AS sources
      ON sources.source_ref_id = evidence.source_ref_id
    WHERE evidence.assertion_id = '${race_winner_id}'::uuid
      AND sources.external_id = '${source_a}' COLLATE "C"
      AND sources.external_version = ${source_version}
  )
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_assertion_evidence AS evidence
    JOIN knowledge_graph.graph_source_references AS sources
      ON sources.source_ref_id = evidence.source_ref_id
    WHERE evidence.assertion_id = '${race_winner_id}'::uuid
      AND sources.external_id = '${source_b}' COLLATE "C"
      AND sources.external_version = $((source_version + 100))
  )
SQL
	done
	printf 'concurrency assertions=absent-symmetric-semantic-key-races:5 one-canonical-id both-evidence-links verified\n'

	for attempt in 1 2 3 4 5; do
		base=$((52000 + attempt * 10))
		concept_id_a="$(race_uuid "$base")"
		concept_id_b="$(race_uuid "$((base + 1))")"
		assertion_id_a="$(race_uuid "$((base + 2))")"
		assertion_id_b="$(race_uuid "$((base + 3))")"
		source_key="kg-race-identical-assertion-${attempt}"
		source_version=$((52000 + attempt))
		create_race_concept "$concept_id_a" "kg-race-identical-assertion-${attempt}-subject"
		create_race_concept "$concept_id_b" "kg-race-identical-assertion-${attempt}-object"
		create_race_source "$source_key" "$source_version"
		mutation_a_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_create_assertion(
				'${assertion_id_a}'::uuid,
				'${concept_id_a}'::uuid,
				'uses',
				'${concept_id_b}'::uuid,
				pg_catalog.jsonb_build_array(
					pg_catalog.jsonb_build_object('kind', 'memory_version', 'memory_id', '${source_key}', 'version', ${source_version})
				)
			)
		")"
		mutation_b_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_create_assertion(
				'${assertion_id_b}'::uuid,
				'${concept_id_a}'::uuid,
				'uses',
				'${concept_id_b}'::uuid,
				pg_catalog.jsonb_build_array(
					pg_catalog.jsonb_build_object('kind', 'memory_version', 'memory_id', '${source_key}', 'version', ${source_version})
				)
			)
		")"
		run_two_session_race 'identical assertion create race' "$mutation_a_sql" "$mutation_b_sql"
		expect_created_existing_pair 'identical assertion create race' "$assertion_id_a" "$assertion_id_b"
		assert_true 'identical assertion create leaves one canonical record and evidence' <<SQL
SELECT (SELECT pg_catalog.count(*) = 1
        FROM knowledge_graph.graph_assertions
        WHERE id IN ('${assertion_id_a}'::uuid, '${assertion_id_b}'::uuid))
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_assertions
    WHERE id = '${race_winner_id}'::uuid
      AND revision = 1
      AND subject_concept_id = '${concept_id_a}'::uuid
      AND relation_code = 'uses' COLLATE "C"
      AND object_concept_id = '${concept_id_b}'::uuid
  )
  AND (SELECT pg_catalog.count(*) = 1
       FROM knowledge_graph.graph_assertion_evidence
       WHERE assertion_id = '${race_winner_id}'::uuid)
SQL
	done
	printf 'concurrency assertions=identical-create-races:5 one-created-one-existing canonical-record verified\n'

	for attempt in 1 2 3 4 5; do
		base=$((56000 + attempt * 10))
		concept_id_a="$(race_uuid "$base")"
		concept_id_b="$(race_uuid "$((base + 1))")"
		assertion_id_a="$(race_uuid "$((base + 2))")"
		assertion_id_b="$(race_uuid "$((base + 3))")"
		source_a="kg-race-overlap-assertion-${attempt}-a"
		source_b="kg-race-overlap-assertion-${attempt}-b"
		source_version=$((56000 + attempt))
		create_race_concept "$concept_id_a" "kg-race-overlap-assertion-${attempt}-subject"
		create_race_concept "$concept_id_b" "kg-race-overlap-assertion-${attempt}-object"
		create_race_source "$source_a" "$source_version"
		create_race_source "$source_b" "$((source_version + 100))"
		mutation_a_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_create_assertion(
				'${assertion_id_a}'::uuid,
				'${concept_id_a}'::uuid,
				'uses',
				'${concept_id_b}'::uuid,
				pg_catalog.jsonb_build_array(
					pg_catalog.jsonb_build_object('kind', 'memory_version', 'memory_id', '${source_a}', 'version', ${source_version})
				)
			)
		")"
		mutation_b_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_create_assertion(
				'${assertion_id_b}'::uuid,
				'${concept_id_a}'::uuid,
				'depends_on',
				'${concept_id_b}'::uuid,
				pg_catalog.jsonb_build_array(
					pg_catalog.jsonb_build_object('kind', 'memory_version', 'memory_id', '${source_b}', 'version', $((source_version + 100)))
				)
			)
		")"
		run_two_session_race 'overlapping assertion create race' "$mutation_a_sql" "$mutation_b_sql"
		expect_equal "$race_result_a" "created|||||${assertion_id_a}|1" 'first overlapping assertion create outcome'
		expect_equal "$race_result_b" "created|||||${assertion_id_b}|1" 'second overlapping assertion create outcome'
		assert_true 'overlapping assertion creates preserve both identities and evidence' <<SQL
SELECT (SELECT pg_catalog.count(*) = 2
        FROM knowledge_graph.graph_assertions
        WHERE id IN ('${assertion_id_a}'::uuid, '${assertion_id_b}'::uuid))
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_assertions
    WHERE id = '${assertion_id_a}'::uuid
      AND revision = 1
      AND subject_concept_id = '${concept_id_a}'::uuid
      AND relation_code = 'uses' COLLATE "C"
      AND object_concept_id = '${concept_id_b}'::uuid
  )
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_assertions
    WHERE id = '${assertion_id_b}'::uuid
      AND revision = 1
      AND subject_concept_id = '${concept_id_a}'::uuid
      AND relation_code = 'depends_on' COLLATE "C"
      AND object_concept_id = '${concept_id_b}'::uuid
  )
  AND (SELECT pg_catalog.count(*) = 1
       FROM knowledge_graph.graph_assertion_evidence
       WHERE assertion_id = '${assertion_id_a}'::uuid)
  AND (SELECT pg_catalog.count(*) = 1
       FROM knowledge_graph.graph_assertion_evidence
       WHERE assertion_id = '${assertion_id_b}'::uuid)
SQL
	done
	printf 'concurrency assertions=overlapping-assertion-create-races:5 both-created distinct-semantic-identities verified\n'

	for attempt in 1 2 3 4 5; do
		base=$((60000 + attempt * 10))
		concept_id_a="$(race_uuid "$base")"
		concept_id_b="$(race_uuid "$((base + 1))")"
		concept_id_c="$(race_uuid "$((base + 2))")"
		concept_id_d="$(race_uuid "$((base + 3))")"
		assertion_id_a="$(race_uuid "$((base + 4))")"
		assertion_id_b="$(race_uuid "$((base + 5))")"
		create_race_concept "$concept_id_a" "kg-race-semantic-${attempt}-a"
		create_race_concept "$concept_id_b" "kg-race-semantic-${attempt}-b"
		create_race_concept "$concept_id_c" "kg-race-semantic-${attempt}-c"
		create_race_concept "$concept_id_d" "kg-race-semantic-${attempt}-d"
		create_race_assertion "$assertion_id_a" "$concept_id_a" is_a "$concept_id_b" "$supporting_sources_sql"
		create_race_assertion "$assertion_id_b" "$concept_id_c" is_a "$concept_id_d" "$supporting_sources_sql"
		before_snapshot="$(snapshot_state)"
		mutation_a_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_update_assertion(
				'${assertion_id_a}'::uuid,
				1,
				'${concept_id_c}'::uuid,
				'is_a',
				'${concept_id_d}'::uuid
			)
		")"
		mutation_b_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_update_assertion(
				'${assertion_id_b}'::uuid,
				1,
				'${concept_id_a}'::uuid,
				'is_a',
				'${concept_id_b}'::uuid
			)
		")"
		lock_keys_sql="pg_catalog.jsonb_build_array(
			knowledge_graph.graph_lock_key_concept_id('${concept_id_a}'::uuid),
			knowledge_graph.graph_lock_key_concept_id('${concept_id_b}'::uuid),
			knowledge_graph.graph_lock_key_concept_id('${concept_id_c}'::uuid),
			knowledge_graph.graph_lock_key_concept_id('${concept_id_d}'::uuid),
			knowledge_graph.graph_lock_key_semantic_assertion(
				'is_a', '${concept_id_a}'::uuid, '${concept_id_b}'::uuid
			),
			knowledge_graph.graph_lock_key_semantic_assertion(
				'is_a', '${concept_id_c}'::uuid, '${concept_id_d}'::uuid
			)
		)"
		expected_wait_lock_id="$(race_min_lock_id_from_keys "$lock_keys_sql")"
		run_two_session_race 'semantic assertion cross-swap race' "$mutation_a_sql" "$mutation_b_sql" "$expected_wait_lock_id"
		expect_equal "$race_result_a" \
			"conflict|semantic_assertion|duplicate_semantic_assertion|||${assertion_id_b}|1" \
			'first semantic assertion cross-swap typed conflict'
		expect_equal "$race_result_b" \
			"conflict|semantic_assertion|duplicate_semantic_assertion|||${assertion_id_a}|1" \
			'second semantic assertion cross-swap typed conflict'
		after_snapshot="$(snapshot_state)"
		expect_equal "$after_snapshot" "$before_snapshot" 'semantic assertion cross-swap unchanged graph snapshot'
	done
	printf 'concurrency assertions=semantic-cross-swaps:5 both-duplicate-conflicts full-state-unchanged verified\n'

	for attempt in 1 2 3 4 5; do
		base=$((70000 + attempt * 10))
		concept_id_a="$(race_uuid "$base")"
		concept_id_b="$(race_uuid "$((base + 1))")"
		concept_id_c="$(race_uuid "$((base + 2))")"
		concept_id_d="$(race_uuid "$((base + 3))")"
		assertion_id="$(race_uuid "$((base + 4))")"
		create_race_concept "$concept_id_a" "kg-race-assertion-revision-${attempt}-a"
		create_race_concept "$concept_id_b" "kg-race-assertion-revision-${attempt}-b"
		create_race_concept "$concept_id_c" "kg-race-assertion-revision-${attempt}-c"
		create_race_concept "$concept_id_d" "kg-race-assertion-revision-${attempt}-d"
		create_race_assertion "$assertion_id" "$concept_id_a" related_to "$concept_id_b" "$supporting_sources_sql"
		mutation_a_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_update_assertion(
				'${assertion_id}'::uuid,
				1,
				'${concept_id_a}'::uuid,
				'is_a',
				'${concept_id_c}'::uuid
			)
		")"
		mutation_b_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_update_assertion(
				'${assertion_id}'::uuid,
				1,
				'${concept_id_a}'::uuid,
				'uses',
				'${concept_id_d}'::uuid
			)
		")"
		run_two_session_race 'assertion revision update race' "$mutation_a_sql" "$mutation_b_sql"
		expect_revision_update_pair 'assertion revision update race' "$assertion_id"
		if [[ "$race_winner_slot" == a ]]; then
			expected_outcome="assertions.relation_code = 'is_a' COLLATE \"C\" AND assertions.object_concept_id = '${concept_id_c}'::uuid"
		else
			expected_outcome="assertions.relation_code = 'uses' COLLATE \"C\" AND assertions.object_concept_id = '${concept_id_d}'::uuid"
		fi
		assert_true 'assertion revision race retains evidence and one replacement' <<SQL
SELECT assertions.revision = 2
  AND assertions.subject_concept_id = '${concept_id_a}'::uuid
  AND ${expected_outcome}
  AND (SELECT pg_catalog.count(*) = 1
       FROM knowledge_graph.graph_assertions AS duplicate
       WHERE duplicate.id = assertions.id)
  AND (SELECT pg_catalog.count(*) = 1
       FROM knowledge_graph.graph_assertion_evidence AS evidence
       WHERE evidence.assertion_id = assertions.id)
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_assertion_evidence AS evidence
    JOIN knowledge_graph.graph_source_references AS sources
      ON sources.source_ref_id = evidence.source_ref_id
    WHERE evidence.assertion_id = assertions.id
      AND sources.external_id = 'kg-race-anchor-source' COLLATE "C"
      AND sources.external_version = 7000
  )
FROM knowledge_graph.graph_assertions AS assertions
WHERE assertions.id = '${assertion_id}'::uuid
SQL
	done
	printf 'concurrency assertions=revision-update-races:5 one-updated one-stale-or-snapshot-conflict evidence-retained verified\n'

	for attempt in 1 2 3 4 5; do
		base=$((80000 + attempt * 10))
		source_key="kg-race-register-delete-${attempt}"
		source_version=$((80000 + attempt))
		mutation_a_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_register_source(
				'memory_version', '${source_key}', ${source_version}
			)
		")"
		mutation_b_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_delete_source(
				'memory_version', '${source_key}', ${source_version}
			)
		")"
		run_two_session_race 'source registration/delete race' "$mutation_a_sql" "$mutation_b_sql"
		if [[ "$race_result_a" != created\|\|\|\|\|*\| ]]; then
			fail 'source registration/delete race did not create one canonical source'
		fi
		source_ref_id="${race_result_a#created|||||}"
		source_ref_id="${source_ref_id%|}"
		[[ "$source_ref_id" =~ ^[0-9]+$ ]] || fail 'source registration returned an invalid canonical identity'
		case "$race_result_b" in
		'deleted||||||') source_present=false ;;
		'missing|||source_reference|||') source_present=true ;;
		'conflict|snapshot|snapshot_drift||||') source_present=true ;;
		*) fail 'source registration/delete race returned an untyped outcome' ;;
		esac
		if [[ "$source_present" == true ]]; then
			expect_equal \
				"$(read_query_as "$application_role" "SELECT pg_catalog.count(*) = 1 AND pg_catalog.min(source_ref_id) = ${source_ref_id} FROM knowledge_graph.graph_source_references WHERE source_kind = 'memory_version' COLLATE \"C\" AND external_id = '${source_key}' COLLATE \"C\" AND external_version = ${source_version}")" \
				't' 'source registration/delete race retained the canonical identity'
		else
			expect_equal \
				"$(read_query_as "$application_role" "SELECT NOT EXISTS (SELECT 1 FROM knowledge_graph.graph_source_references WHERE source_kind = 'memory_version' COLLATE \"C\" AND external_id = '${source_key}' COLLATE \"C\" AND external_version = ${source_version})")" \
				't' 'source registration/delete race confirmed deletion'
		fi
	done
	printf 'concurrency sources=absent-register-delete-races:5 created-once typed-delete-outcomes final-state verified\n'

	base=90000
	concept_id_a="$(race_uuid "$base")"
	concept_id_b="$(race_uuid "$((base + 1))")"
	evidence_assertion_id="$(race_uuid "$((base + 2))")"
	create_race_concept "$concept_id_a" kg-race-source-evidence-subject
	create_race_concept "$concept_id_b" kg-race-source-evidence-object
	create_race_assertion "$evidence_assertion_id" "$concept_id_a" related_to "$concept_id_b" "$supporting_sources_sql"
	for attempt in 1 2 3 4 5; do
		source_key="kg-race-source-evidence-${attempt}"
		source_version=$((90000 + attempt))
		create_race_source "$source_key" "$source_version"
		mutation_a_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_delete_source(
				'memory_version', '${source_key}', ${source_version}
			)
		")"
		mutation_b_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_add_assertion_evidence(
				'${evidence_assertion_id}'::uuid,
				'memory_version', '${source_key}', ${source_version}
			)
		")"
		run_two_session_race 'source delete/evidence addition race' "$mutation_a_sql" "$mutation_b_sql"
		if [[ "$race_result_a" == 'deleted||||||' &&
			"$race_result_b" == 'missing|||source_reference|||' ]]; then
			assert_true 'source delete winning race leaves no evidence association' <<SQL
SELECT NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_source_references AS sources
    WHERE sources.source_kind = 'memory_version' COLLATE "C"
      AND sources.external_id = '${source_key}' COLLATE "C"
      AND sources.external_version = ${source_version}
  )
  AND NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_assertion_evidence AS evidence
    JOIN knowledge_graph.graph_source_references AS sources
      ON sources.source_ref_id = evidence.source_ref_id
    WHERE evidence.assertion_id = '${evidence_assertion_id}'::uuid
      AND sources.external_id = '${source_key}' COLLATE "C"
  )
SQL
		elif [[ "$race_result_a" == 'referenced||||source_reference||' &&
			"$race_result_b" == "created|||||${evidence_assertion_id}|" ]]; then
			assert_true 'evidence addition winning race protects its source' <<SQL
SELECT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_source_references AS sources
    WHERE sources.source_kind = 'memory_version' COLLATE "C"
      AND sources.external_id = '${source_key}' COLLATE "C"
      AND sources.external_version = ${source_version}
  )
  AND (SELECT pg_catalog.count(*) = 1
       FROM knowledge_graph.graph_assertion_evidence AS evidence
       JOIN knowledge_graph.graph_source_references AS sources
         ON sources.source_ref_id = evidence.source_ref_id
       WHERE evidence.assertion_id = '${evidence_assertion_id}'::uuid
         AND sources.external_id = '${source_key}' COLLATE "C"
         AND sources.external_version = ${source_version})
SQL
		else
			fail 'source delete/evidence addition race returned a non-canonical outcome pair'
		fi
	done
	printf 'concurrency sources=delete-vs-evidence-add-races:5 deleted/missing-or-referenced/created invariant verified\n'

	for attempt in 1 2 3 4 5; do
		base=$((100000 + attempt * 10))
		concept_id_a="$(race_uuid "$base")"
		source_key="kg-race-concept-mention-${attempt}"
		source_version=$((100000 + attempt))
		create_race_concept "$concept_id_a" "kg-race-concept-mention-${attempt}"
		create_race_source "$source_key" "$source_version"
		mutation_a_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_delete_concept(
				'${concept_id_a}'::uuid,
				1
			)
		")"
		mutation_b_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_create_mention(
				'${concept_id_a}'::uuid,
				'memory_version', '${source_key}', ${source_version}
			)
		")"
		run_two_session_race 'concept delete/mention creation race' "$mutation_a_sql" "$mutation_b_sql"
		if [[ "$race_result_a" == 'deleted||||||' &&
			"$race_result_b" == "missing|||concept||${concept_id_a}|" ]]; then
			assert_true 'concept delete winning mention race leaves no concept or mention' <<SQL
SELECT NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_concepts
    WHERE id = '${concept_id_a}'::uuid
  )
  AND NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_aliases
    WHERE concept_id = '${concept_id_a}'::uuid
       OR alias_key = 'kg-race-concept-mention-${attempt}' COLLATE "C"
  )
  AND NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_concept_mentions AS mentions
    JOIN knowledge_graph.graph_source_references AS sources
      ON sources.source_ref_id = mentions.source_ref_id
    WHERE sources.external_id = '${source_key}' COLLATE "C"
  )
SQL
		elif [[ "$race_result_a" == 'referenced||||||' &&
			"$race_result_b" == created\|\|\|\|\|*\| ]]; then
			source_ref_id="${race_result_b#created|||||}"
			source_ref_id="${source_ref_id%|}"
			[[ "$source_ref_id" =~ ^[0-9]+$ ]] || fail 'mention creation returned an invalid source identity'
			assert_true 'mention creation winning race rejects concept deletion unchanged' <<SQL
SELECT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_concepts
    WHERE id = '${concept_id_a}'::uuid AND revision = 1
  )
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_aliases
    WHERE concept_id = '${concept_id_a}'::uuid
      AND alias_key = 'kg-race-concept-mention-${attempt}' COLLATE "C"
      AND is_preferred
  )
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_concept_mentions
    WHERE concept_id = '${concept_id_a}'::uuid
      AND source_ref_id = ${source_ref_id}
  )
SQL
		else
			fail 'concept delete/mention creation race returned a non-canonical outcome pair'
		fi
	done
	printf 'concurrency concepts=delete-vs-mention-create-races:5 deleted/missing-or-referenced/created invariant verified\n'

	base=120000
	concept_id_a="$(race_uuid "$base")"
	concept_id_b="$(race_uuid "$((base + 1))")"
	assertion_id="$(race_uuid "$((base + 2))")"
	create_race_concept "$concept_id_a" kg-race-evidence-idempotency-subject
	create_race_concept "$concept_id_b" kg-race-evidence-idempotency-object
	create_race_assertion "$assertion_id" "$concept_id_a" related_to "$concept_id_b" "$supporting_sources_sql"
	for attempt in 1 2 3 4 5; do
		source_key="kg-race-evidence-idempotency-${attempt}"
		source_version=$((120000 + attempt))
		create_race_source "$source_key" "$source_version"
		mutation_a_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_add_assertion_evidence(
				'${assertion_id}'::uuid,
				'memory_version', '${source_key}', ${source_version}
			)
		")"
		mutation_b_sql=$mutation_a_sql
		run_two_session_race 'identical evidence addition race' "$mutation_a_sql" "$mutation_b_sql"
		expect_created_existing_same_identity_pair 'identical evidence addition race' "$assertion_id"
		assert_true 'identical evidence addition leaves one canonical association' <<SQL
SELECT (SELECT pg_catalog.count(*) = 1
        FROM knowledge_graph.graph_assertion_evidence AS evidence
        JOIN knowledge_graph.graph_source_references AS sources
          ON sources.source_ref_id = evidence.source_ref_id
        WHERE evidence.assertion_id = '${assertion_id}'::uuid
          AND sources.external_id = '${source_key}' COLLATE "C"
          AND sources.external_version = ${source_version})
  AND (SELECT pg_catalog.count(*) = 1
       FROM knowledge_graph.graph_assertions
       WHERE id = '${assertion_id}'::uuid)
SQL
	done
	printf 'concurrency evidence=identical-add-races:5 one-created-one-existing one-association verified\n'

	for attempt in 1 2 3; do
		base=$((130000 + attempt * 100))
		concept_id_a="$(race_uuid "$base")"
		concept_id_b="$(race_uuid "$((base + 1))")"
		assertion_id="$(race_uuid "$((base + 2))")"
		cap_prefix="kg-race-cap-${attempt}-"
		cap_version_base=$((130000 + attempt * 100))
		create_race_concept "$concept_id_a" "kg-race-cap-${attempt}-subject"
		create_race_concept "$concept_id_b" "kg-race-cap-${attempt}-object"
		assert_true 'concurrency evidence-cap source fixture creation' <<SQL
WITH registrations AS MATERIALIZED (
  SELECT knowledge_graph.graph_register_source(
    'memory_version',
    '${cap_prefix}' || pg_catalog.lpad(source_number::text, 2, '0'),
    (${cap_version_base} + source_number)::bigint
  ) AS value
  FROM pg_catalog.generate_series(1, 33) AS requested(source_number)
)
SELECT pg_catalog.count(*) = 33
  AND pg_catalog.bool_and(value->>'outcome' = 'created')
FROM registrations
SQL
		assert_true 'concurrency evidence-cap assertion fixture creation' <<SQL
SELECT knowledge_graph.graph_create_assertion(
  '${assertion_id}'::uuid,
  '${concept_id_a}'::uuid,
  'related_to',
  '${concept_id_b}'::uuid,
  (
    SELECT pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'kind', 'memory_version',
        'memory_id', '${cap_prefix}' || pg_catalog.lpad(source_number::text, 2, '0'),
        'version', ${cap_version_base} + source_number
      )
      ORDER BY source_number
    )
    FROM pg_catalog.generate_series(1, 31) AS requested(source_number)
  )
)->>'outcome' = 'created'
SQL
		source_a="${cap_prefix}32"
		source_b="${cap_prefix}33"
		mutation_a_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_add_assertion_evidence(
				'${assertion_id}'::uuid,
				'memory_version', '${source_a}', $((cap_version_base + 32))
			)
		")"
		mutation_b_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_add_assertion_evidence(
				'${assertion_id}'::uuid,
				'memory_version', '${source_b}', $((cap_version_base + 33))
			)
		")"
		run_two_session_race 'evidence-cap contention race' "$mutation_a_sql" "$mutation_b_sql"
		if [[ "$race_result_a" == "created|||||${assertion_id}|" &&
			"$race_result_b" == 'evidence_limit||||||' ]]; then
			cap_winner_source=$source_a
			cap_winner_version=$((cap_version_base + 32))
			cap_loser_source=$source_b
		elif [[ "$race_result_b" == "created|||||${assertion_id}|" &&
			"$race_result_a" == 'evidence_limit||||||' ]]; then
			cap_winner_source=$source_b
			cap_winner_version=$((cap_version_base + 33))
			cap_loser_source=$source_a
		else
			fail 'evidence-cap contention race did not return one created and one typed limit outcome'
		fi
		assert_true 'evidence-cap contention preserves the 32-source bound and rejected association' <<SQL
SELECT (SELECT pg_catalog.count(*) = 32
        FROM knowledge_graph.graph_assertion_evidence AS evidence
        WHERE evidence.assertion_id = '${assertion_id}'::uuid)
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_assertion_evidence AS evidence
    JOIN knowledge_graph.graph_source_references AS sources
      ON sources.source_ref_id = evidence.source_ref_id
    WHERE evidence.assertion_id = '${assertion_id}'::uuid
      AND sources.external_id = '${cap_winner_source}' COLLATE "C"
      AND sources.external_version = ${cap_winner_version}
  )
  AND NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_assertion_evidence AS evidence
    JOIN knowledge_graph.graph_source_references AS sources
      ON sources.source_ref_id = evidence.source_ref_id
    WHERE evidence.assertion_id = '${assertion_id}'::uuid
      AND sources.external_id = '${cap_loser_source}' COLLATE "C"
  )
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_source_references AS sources
    WHERE sources.external_id = '${cap_loser_source}' COLLATE "C"
  )
SQL
	done
	printf 'concurrency evidence=32-source-cap-contention-races:3 one-created-one-limit count-and-rejected-link verified\n'

	for attempt in 1 2 3 4 5; do
		base=$((140000 + attempt * 10))
		concept_id_a="$(race_uuid "$base")"
		concept_id_b="$(race_uuid "$((base + 1))")"
		assertion_id="$(race_uuid "$((base + 2))")"
		source_a="kg-race-orphan-${attempt}-a"
		source_b="kg-race-orphan-${attempt}-b"
		source_version=$((140000 + attempt))
		create_race_concept "$concept_id_a" "kg-race-orphan-${attempt}-subject"
		create_race_concept "$concept_id_b" "kg-race-orphan-${attempt}-object"
		create_race_source "$source_a" "$source_version"
		create_race_source "$source_b" "$((source_version + 100))"
		supporting_sources_sql="pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('kind', 'memory_version', 'memory_id', '${source_a}', 'version', ${source_version}), pg_catalog.jsonb_build_object('kind', 'memory_version', 'memory_id', '${source_b}', 'version', $((source_version + 100))))"
		create_race_assertion "$assertion_id" "$concept_id_a" related_to "$concept_id_b" "$supporting_sources_sql"
		mutation_a_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_remove_assertion_evidence(
				'${assertion_id}'::uuid,
				'memory_version', '${source_a}', ${source_version}
			)
		")"
		mutation_b_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_remove_assertion_evidence(
				'${assertion_id}'::uuid,
				'memory_version', '${source_b}', $((source_version + 100))
			)
		")"
		run_two_session_race 'last evidence removal race' "$mutation_a_sql" "$mutation_b_sql"
		if [[ "$race_result_a" == 'deleted||||||' &&
			"$race_result_b" == "would_orphan_evidence|||||${assertion_id}|1" ]]; then
			remaining_source=$source_b
		elif [[ "$race_result_b" == 'deleted||||||' &&
			"$race_result_a" == "would_orphan_evidence|||||${assertion_id}|1" ]]; then
			remaining_source=$source_a
		else
			fail 'last evidence removal race did not return one deleted and one would-orphan outcome'
		fi
		assert_true 'last evidence removal race rejects orphaning and preserves the other row' <<SQL
SELECT (SELECT pg_catalog.count(*) = 1
        FROM knowledge_graph.graph_assertion_evidence AS evidence
        WHERE evidence.assertion_id = '${assertion_id}'::uuid)
  AND (SELECT pg_catalog.bool_and(sources.external_id = '${remaining_source}' COLLATE "C")
       FROM knowledge_graph.graph_assertion_evidence AS evidence
       JOIN knowledge_graph.graph_source_references AS sources
         ON sources.source_ref_id = evidence.source_ref_id
       WHERE evidence.assertion_id = '${assertion_id}'::uuid)
SQL
	done
	printf 'concurrency evidence=last-evidence-orphan-races:5 one-deleted-one-would-orphan surviving-link verified\n'
	supporting_sources_sql="pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('kind', 'memory_version', 'memory_id', 'kg-race-anchor-source', 'version', 7000))"

	for attempt in 1 2 3 4 5; do
		base=$((150000 + attempt * 10))
		concept_id_a="$(race_uuid "$base")"
		concept_id_b="$(race_uuid "$((base + 1))")"
		assertion_id="$(race_uuid "$((base + 2))")"
		source_key="kg-race-assertion-delete-add-${attempt}"
		source_version=$((150000 + attempt))
		create_race_concept "$concept_id_a" "kg-race-assertion-delete-${attempt}-subject"
		create_race_concept "$concept_id_b" "kg-race-assertion-delete-${attempt}-object"
		create_race_source "$source_key" "$source_version"
		create_race_assertion "$assertion_id" "$concept_id_a" related_to "$concept_id_b" "$supporting_sources_sql"
		mutation_a_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_delete_assertion(
				'${assertion_id}'::uuid,
				1
			)
		")"
		mutation_b_sql="$(race_result_sql "
			SELECT knowledge_graph.graph_add_assertion_evidence(
				'${assertion_id}'::uuid,
				'memory_version', '${source_key}', ${source_version}
			)
		")"
		run_two_session_race 'assertion delete/evidence addition race' "$mutation_a_sql" "$mutation_b_sql"
		delete_outcome=$race_result_a
		add_outcome=$race_result_b
		if [[ "$delete_outcome" == 'deleted||||||' &&
			( "$add_outcome" == 'missing|||assertion|||' ||
				"$add_outcome" == "created|||||${assertion_id}|" ) ]]; then
			final_state=deleted
		elif [[ "$delete_outcome" == "conflict|snapshot|snapshot_drift|||${assertion_id}|1" &&
			"$add_outcome" == "created|||||${assertion_id}|" ]]; then
			final_state=present
		else
			fail 'assertion delete/evidence addition race returned a non-canonical outcome pair'
		fi
		if [[ "$final_state" == deleted ]]; then
			assert_true 'assertion deletion race cascades all evidence' <<SQL
SELECT NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_assertions
    WHERE id = '${assertion_id}'::uuid
  )
  AND NOT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_assertion_evidence
    WHERE assertion_id = '${assertion_id}'::uuid
  )
SQL
		else
			assert_true 'assertion snapshot conflict preserves both evidence links' <<SQL
SELECT EXISTS (
    SELECT 1 FROM knowledge_graph.graph_assertions
    WHERE id = '${assertion_id}'::uuid AND revision = 1
  )
  AND (SELECT pg_catalog.count(*) = 2
       FROM knowledge_graph.graph_assertion_evidence AS evidence
       WHERE evidence.assertion_id = '${assertion_id}'::uuid)
  AND EXISTS (
    SELECT 1 FROM knowledge_graph.graph_assertion_evidence AS evidence
    JOIN knowledge_graph.graph_source_references AS sources
      ON sources.source_ref_id = evidence.source_ref_id
    WHERE evidence.assertion_id = '${assertion_id}'::uuid
      AND sources.external_id = '${source_key}' COLLATE "C"
      AND sources.external_version = ${source_version}
  )
SQL
		fi
	done
	printf 'concurrency assertions=delete-vs-evidence-add-races:5 deleted-or-snapshot-conflict typed-outcomes state verified\n'
	printf 'concurrency lock-proof=application-single-key owner-waiter sorted-prefix cross-swap-key-ids verified\n'
}

run_concurrency_verification

if ! bash "$rollback_script" >/dev/null 2>&1; then
	fail 'clean graph schema rollback failed'
fi
schema_applied=false
assert_schema_absent 'graph schema absence after lifecycle rollback'

printf 'mutations rollback=completed graph-schema=absent\n'
printf 'mutations verification=passed major=%s\n' "$expected_major"
