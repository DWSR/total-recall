#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 0 ]]; then
	printf 'usage: %s\n' "$0" >&2
	exit 64
fi

for variable in \
	SESSION_POST_PROCESSING_POSTGRES_BIN \
	SESSION_POST_PROCESSING_POSTGRES_SOCKET_DIR \
	SESSION_POST_PROCESSING_POSTGRES_DATABASE \
	SESSION_POST_PROCESSING_BOOTSTRAP_ROLE \
	SESSION_POST_PROCESSING_SOURCE_ROLE \
	SESSION_POST_PROCESSING_MIGRATION_ROLE \
	SESSION_POST_PROCESSING_APPLICATION_ROLE \
	SESSION_POST_PROCESSING_EXPECTED_MAJOR \
	SESSION_POST_PROCESSING_SCHEMA_MIGRATION \
	PGHOST \
	PGPORT \
	PGDATABASE \
	PGUSER; do
	if [[ -z "${!variable:-}" ]]; then
		printf 'missing fixture environment variable: %s\n' "$variable" >&2
		exit 64
	fi
done

case "$SESSION_POST_PROCESSING_EXPECTED_MAJOR" in
17 | 18) ;;
*)
	printf 'unsupported PostgreSQL major: %s\n' "$SESSION_POST_PROCESSING_EXPECTED_MAJOR" >&2
	exit 64
	;;
esac

readonly postgres_bin="$SESSION_POST_PROCESSING_POSTGRES_BIN"
readonly socket_dir="$SESSION_POST_PROCESSING_POSTGRES_SOCKET_DIR"
readonly database="$SESSION_POST_PROCESSING_POSTGRES_DATABASE"
readonly bootstrap_role="$SESSION_POST_PROCESSING_BOOTSTRAP_ROLE"
readonly source_role="$SESSION_POST_PROCESSING_SOURCE_ROLE"
readonly migration_role="$SESSION_POST_PROCESSING_MIGRATION_ROLE"
readonly application_role="$SESSION_POST_PROCESSING_APPLICATION_ROLE"
readonly expected_major="$SESSION_POST_PROCESSING_EXPECTED_MAJOR"
readonly schema_migration="$SESSION_POST_PROCESSING_SCHEMA_MIGRATION"
readonly psql="$postgres_bin/psql"

if [[ ! -x "$psql" ]]; then
	printf 'missing PostgreSQL executable: %s\n' "$psql" >&2
	exit 66
fi
if [[ ! -r "$schema_migration" ]]; then
	printf 'missing session post-processing schema migration: %s\n' "$schema_migration" >&2
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

expect_fixture_equal() {
	local actual=$1
	local expected=$2
	local description=$3

	[[ "$actual" == "$expected" ]] || fail "$description"
}

expect_fixture_not_equal() {
	local left=$1
	local right=$2
	local description=$3

	[[ "$left" != "$right" ]] || fail "$description"
}

sha1_digest() {
	local output
	local digest

	if command -v sha1sum >/dev/null 2>&1; then
		if ! output="$(sha1sum --binary)"; then
			fail 'SHA-1 digest calculation failed'
		fi
	elif command -v shasum >/dev/null 2>&1; then
		if ! output="$(shasum -a 1)"; then
			fail 'SHA-1 digest calculation failed'
		fi
	else
		fail 'missing SHA-1 utility for source revision derivation'
	fi

	digest=${output%%[[:space:]]*}
	[[ "$digest" =~ ^[0-9a-fA-F]{40}$ ]] || fail 'SHA-1 utility produced an invalid digest'
	digest=${digest//A/a}
	digest=${digest//B/b}
	digest=${digest//C/c}
	digest=${digest//D/d}
	digest=${digest//E/e}
	digest=${digest//F/f}
	printf '%s' "$digest"
}

verify_sha1_digest_helper() {
	local fixture_dir
	local digest
	local missing_error
	local expected_digest='a9993e364706816aba3e25717850c26c9cd0d89d'
	local expected_upper_digest='A9993E364706816ABA3E25717850C26C9CD0D89D'

	if ! fixture_dir="$(mktemp -d "${TMPDIR:-/tmp}/spp-sha1.XXXXXX" 2>/dev/null)"; then
		fail 'SHA-1 utility fixture workspace creation failed'
	fi
	if ! mkdir "$fixture_dir/gnu" "$fixture_dir/bsd" "$fixture_dir/upper" "$fixture_dir/missing"; then
		rm -rf -- "$fixture_dir"
		fail 'SHA-1 utility fixture workspace setup failed'
	fi

	printf '%s\n' \
		"#!$BASH" \
		'[ "$#" -eq 1 ] && [ "$1" = "--binary" ] || exit 64' \
		'while IFS= read -r line || [ -n "$line" ]; do :; done' \
		"printf '%s\\n' '$expected_digest *-'" \
		>"$fixture_dir/gnu/sha1sum"
	printf '%s\n' \
		"#!$BASH" \
		'[ "$#" -eq 2 ] && [ "$1" = "-a" ] && [ "$2" = "1" ] || exit 64' \
		'while IFS= read -r line || [ -n "$line" ]; do :; done' \
		"printf '%s\\n' '$expected_digest  -'" \
		>"$fixture_dir/bsd/shasum"
	printf '%s\n' \
		"#!$BASH" \
		'[ "$#" -eq 1 ] && [ "$1" = "--binary" ] || exit 64' \
		'while IFS= read -r line || [ -n "$line" ]; do :; done' \
		"printf '%s\\n' '$expected_upper_digest *-'" \
		>"$fixture_dir/upper/sha1sum"
	if ! chmod 700 "$fixture_dir/gnu/sha1sum" "$fixture_dir/bsd/shasum" "$fixture_dir/upper/sha1sum"; then
		rm -rf -- "$fixture_dir"
		fail 'SHA-1 utility fixture setup failed'
	fi

	if ! digest="$(printf 'abc' | PATH="$fixture_dir/gnu" sha1_digest)"; then
		rm -rf -- "$fixture_dir"
		fail 'SHA-1 GNU utility fixture failed'
	fi
	expect_fixture_equal "$digest" "$expected_digest" 'SHA-1 GNU digest normalization'
	if ! digest="$(printf 'abc' | PATH="$fixture_dir/bsd" sha1_digest)"; then
		rm -rf -- "$fixture_dir"
		fail 'SHA-1 BSD utility fixture failed'
	fi
	expect_fixture_equal "$digest" "$expected_digest" 'SHA-1 BSD digest normalization'
	if ! digest="$(printf 'abc' | PATH="$fixture_dir/upper" sha1_digest)"; then
		rm -rf -- "$fixture_dir"
		fail 'SHA-1 uppercase utility fixture failed'
	fi
	expect_fixture_equal "$digest" "$expected_digest" 'SHA-1 uppercase digest normalization'
	if missing_error="$(PATH="$fixture_dir/missing" sha1_digest </dev/null 2>&1)"; then
		rm -rf -- "$fixture_dir"
		fail 'SHA-1 missing utility fixture unexpectedly succeeded'
	fi
	expect_fixture_equal \
		"$missing_error" \
		'fixture verification failed: missing SHA-1 utility for source revision derivation' \
		'SHA-1 missing utility diagnostic'
	if ! rm -rf -- "$fixture_dir"; then
		fail 'SHA-1 utility fixture workspace cleanup failed'
	fi
}

uuid_bytes() {
	local uuid="${1//-/}"
	local offset

	[[ "$uuid" =~ ^[0-9a-fA-F]{32}$ ]] || fail 'invalid UUIDv5 namespace'
	for ((offset = 0; offset < 32; offset += 2)); do
		printf '%b' "\\x${uuid:offset:2}"
	done
}

uuid_v5_from_sha1() {
	local sha1=$1
	local variant

	[[ "$sha1" =~ ^[0-9a-f]{40}$ ]] || fail 'UUIDv5 derivation produced an invalid SHA-1 digest'
	sha1="${sha1:0:12}5${sha1:13}"
	printf -v variant '%x' "$(( (16#${sha1:16:1} & 3) | 8 ))"
	sha1="${sha1:0:16}${variant}${sha1:17}"
	printf '%s-%s-%s-%s-%s' \
		"${sha1:0:8}" \
		"${sha1:8:4}" \
		"${sha1:12:4}" \
		"${sha1:16:4}" \
		"${sha1:20:12}"
}

uuid_v5_from_text() {
	local namespace=$1
	local name=$2
	local digest

	if ! digest="$(
		{
			uuid_bytes "$namespace"
			printf '%s' "$name"
		} | sha1_digest
	)"; then
		return 1
	fi
	uuid_v5_from_sha1 "$digest"
}

source_identity_text() {
	local previous=''
	local event_type
	local receipt_id
	local identity

	(( $# >= 2 && $# % 2 == 0 )) || fail 'source revision identity inputs are incomplete'
	while (( $# > 0 )); do
		event_type=$1
		receipt_id=$2
		identity="${event_type}"$'\t'"${receipt_id}"
		if [[ -n "$previous" && "$identity" < "$previous" ]]; then
			fail 'source revision identity inputs are not sorted'
		fi
		printf '%s\t%s\n' "$event_type" "$receipt_id"
		previous=$identity
		shift 2
	done
}

# Mirrors repository.rs: nested URL UUIDv5, then sorted event type, NUL, receipt ID, and newline.
source_revision_v5() {
	local previous=''
	local event_type
	local receipt_id
	local identity
	local digest

	(( $# >= 2 && $# % 2 == 0 )) || fail 'source revision identity inputs are incomplete'
	if ! digest="$(
		{
			uuid_bytes "$source_revision_namespace"
			while (( $# > 0 )); do
				event_type=$1
				receipt_id=$2
				identity="${event_type}"$'\t'"${receipt_id}"
				if [[ -n "$previous" && "$identity" < "$previous" ]]; then
					fail 'source revision identity inputs are not sorted'
				fi
				printf '%s\0%s\n' "$event_type" "$receipt_id"
				previous=$identity
				shift 2
			done
		} | sha1_digest
	)"; then
		return 1
	fi
	uuid_v5_from_sha1 "$digest"
}

readonly uuid_namespace_url='6ba7b811-9dad-11d1-80b4-00c04fd430c8'
readonly source_revision_namespace_name='total-recall/session-post-processing/source-revision/v1'
verify_sha1_digest_helper
source_revision_namespace="$(uuid_v5_from_text "$uuid_namespace_url" "$source_revision_namespace_name")" || exit 1
readonly source_revision_namespace

readonly -a task_5_3_concurrent_claim_identity_pairs=(
	session_end
	'00000000-0000-4000-8000-000000000109'
)
readonly task_5_3_concurrent_claim_source_identities="$(source_identity_text "${task_5_3_concurrent_claim_identity_pairs[@]}")"
readonly task_5_3_concurrent_claim_source_revision="$(source_revision_v5 "${task_5_3_concurrent_claim_identity_pairs[@]}")"

readonly -a task_5_3_expired_reclaim_initial_identity_pairs=(
	session_end
	'00000000-0000-4000-8000-000000000110'
)
readonly -a task_5_3_expired_reclaim_full_identity_pairs=(
	observation
	'00000000-0000-4000-8000-000000000111'
	session_end
	'00000000-0000-4000-8000-000000000110'
)
readonly task_5_3_expired_reclaim_initial_source_identities="$(source_identity_text "${task_5_3_expired_reclaim_initial_identity_pairs[@]}")"
readonly task_5_3_expired_reclaim_full_source_identities="$(source_identity_text "${task_5_3_expired_reclaim_full_identity_pairs[@]}")"
readonly task_5_3_expired_reclaim_initial_source_revision="$(source_revision_v5 "${task_5_3_expired_reclaim_initial_identity_pairs[@]}")"
readonly task_5_3_expired_reclaim_full_source_revision="$(source_revision_v5 "${task_5_3_expired_reclaim_full_identity_pairs[@]}")"

readonly -a task_5_3_snapshot_race_old_identity_pairs=(
	session_end
	'00000000-0000-4000-8000-000000000112'
)
readonly -a task_5_3_snapshot_race_full_identity_pairs=(
	observation
	'00000000-0000-4000-8000-000000000113'
	session_end
	'00000000-0000-4000-8000-000000000112'
)
readonly task_5_3_snapshot_race_old_source_identities="$(source_identity_text "${task_5_3_snapshot_race_old_identity_pairs[@]}")"
readonly task_5_3_snapshot_race_full_source_identities="$(source_identity_text "${task_5_3_snapshot_race_full_identity_pairs[@]}")"
readonly task_5_3_snapshot_race_old_source_revision="$(source_revision_v5 "${task_5_3_snapshot_race_old_identity_pairs[@]}")"
readonly task_5_3_snapshot_race_full_source_revision="$(source_revision_v5 "${task_5_3_snapshot_race_full_identity_pairs[@]}")"
readonly task_5_3_snapshot_race_expected_old_source_revision='8fe55c55-a958-5925-ba77-07300ff2703a'
readonly task_5_3_snapshot_race_expected_full_source_revision='406b944b-041e-58f1-ba49-6a4f1f4e0858'

query_as() {
	local role=$1
	local sql=$2

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

application_query() {
	query_as "$application_role" "$1"
}

migration_query() {
	query_as "$migration_role" "$1"
}

source_query() {
	query_as "$source_role" "$1"
}

safe_application_query() {
	local output

	if ! output="$(application_query "$1" 2>/dev/null)"; then
		fail 'task 5.3 application query failed'
	fi
	printf '%s' "$output"
}

safe_application_execute() {
	if ! application_query "$1" >/dev/null 2>/dev/null; then
		fail 'task 5.3 application statement failed'
	fi
}

safe_source_execute() {
	if ! source_query "$1" >/dev/null 2>/dev/null; then
		fail 'task 5.3 source statement failed'
	fi
}

expect_fixture_contains() {
	local actual=$1
	local expected_fragment=$2
	local description=$3

	[[ "$actual" == *"$expected_fragment"* ]] || fail "$description"
}

expect_fixture_not_contains() {
	local actual=$1
	local unexpected_fragment=$2
	local description=$3

	[[ "$actual" != *"$unexpected_fragment"* ]] || fail "$description"
}

fixture_line_count() {
	local output=$1
	local line
	local count=0

	if [[ -n "$output" ]]; then
		while IFS= read -r line; do
			((count += 1))
		done <<<"$output"
	fi
	printf '%s' "$count"
}

readonly task_5_3_discovery_prepare="
	PREPARE task_5_3_discovery(text) AS
	WITH lifecycle AS (
		SELECT
			session_id,
			COUNT(*) AS lifecycle_count,
			BOOL_OR(event_type = 'session_end') AS has_persisted_end
		FROM session_events
		GROUP BY session_id
	),
	observations AS (
		SELECT
			session_id,
			COUNT(*) AS observation_count,
			MAX(ingested_at) AS latest_observation_ingested_at
		FROM raw_observations
		GROUP BY session_id
	),
	source AS (
		SELECT
			COALESCE(lifecycle.session_id, observations.session_id) AS session_id,
			COALESCE(lifecycle.lifecycle_count, 0) AS lifecycle_count,
			COALESCE(observations.observation_count, 0) AS observation_count,
			COALESCE(lifecycle.has_persisted_end, FALSE) AS has_persisted_end,
			observations.latest_observation_ingested_at
		FROM lifecycle
		FULL OUTER JOIN observations
			ON lifecycle.session_id = observations.session_id
	)
	SELECT source.session_id
	FROM source
	LEFT JOIN public.session_records AS current_record
		ON current_record.session_id = source.session_id
	WHERE (source.lifecycle_count > 0 OR source.observation_count > 0)
	  AND (
		  source.has_persisted_end
		  OR source.latest_observation_ingested_at <= \$1::text::timestamptz - INTERVAL '24 hours'
	  )
	  AND (
		  current_record.session_id IS NULL
		  OR current_record.lifecycle_count IS DISTINCT FROM source.lifecycle_count
		  OR current_record.observation_count IS DISTINCT FROM source.observation_count
	  )
	ORDER BY source.session_id ASC;"

readonly task_5_3_claim_candidates_prepare="
	PREPARE task_5_3_claim_candidates(text, text) AS
	WITH lifecycle AS (
		SELECT
			session_id,
			COUNT(*) AS lifecycle_count,
			BOOL_OR(event_type = 'session_end') AS has_persisted_end
		FROM session_events
		GROUP BY session_id
	),
	observations AS (
		SELECT
			session_id,
			COUNT(*) AS observation_count,
			MAX(ingested_at) AS latest_observation_ingested_at
		FROM raw_observations
		GROUP BY session_id
	),
	source AS (
		SELECT
			COALESCE(lifecycle.session_id, observations.session_id) AS session_id,
			COALESCE(lifecycle.lifecycle_count, 0) AS lifecycle_count,
			COALESCE(observations.observation_count, 0) AS observation_count,
			COALESCE(lifecycle.has_persisted_end, FALSE) AS has_persisted_end,
			observations.latest_observation_ingested_at
		FROM lifecycle
		FULL OUTER JOIN observations
			ON lifecycle.session_id = observations.session_id
	),
	eligible AS MATERIALIZED (
		SELECT
			source.session_id,
			source.lifecycle_count,
			source.observation_count
		FROM source
		LEFT JOIN public.session_records AS current_record
			ON current_record.session_id = source.session_id
		WHERE (source.lifecycle_count > 0 OR source.observation_count > 0)
		  AND (
			  source.has_persisted_end
			  OR source.latest_observation_ingested_at <= \$1::text::timestamptz - INTERVAL '24 hours'
		  )
		  AND (
		  current_record.session_id IS NULL
		  OR current_record.lifecycle_count IS DISTINCT FROM source.lifecycle_count
		  OR current_record.observation_count IS DISTINCT FROM source.observation_count
		  )
		ORDER BY source.session_id COLLATE \"C\" ASC
		LIMIT \$2::text::bigint
	),
	identities AS (
		SELECT event.session_id, event.event_type, event.receipt_id
		FROM session_events AS event
		JOIN eligible ON eligible.session_id = event.session_id
		UNION ALL
		SELECT observation.session_id, observation.event_type, observation.receipt_id
		FROM raw_observations AS observation
		JOIN eligible ON eligible.session_id = observation.session_id
	)
	SELECT
		eligible.session_id,
		eligible.lifecycle_count::text AS lifecycle_count,
		eligible.observation_count::text AS observation_count,
		jsonb_agg(
			jsonb_build_object('event_type', identities.event_type, 'receipt_id', identities.receipt_id::text)
			ORDER BY identities.event_type COLLATE \"C\", identities.receipt_id::text COLLATE \"C\"
		) AS source_identities
	FROM eligible
	JOIN identities ON identities.session_id = eligible.session_id
	GROUP BY eligible.session_id, eligible.lifecycle_count, eligible.observation_count
	ORDER BY eligible.session_id COLLATE \"C\" ASC;"

readonly task_5_3_claim_prepare="
	PREPARE task_5_3_claim(text, text, text, text, text, text, text, text) AS
	WITH request AS (
		SELECT
			\$1::text AS session_id,
			\$2::text::uuid AS source_revision,
			\$3::text::bigint AS lifecycle_count,
			\$4::text::bigint AS observation_count,
			\$5::text::uuid AS attempt_id,
			\$6::text::uuid AS lease_token,
			\$7::text::timestamptz AS now,
			\$8::text::timestamptz AS lease_expires_at
	),
	reclaimed_retryable AS (
		UPDATE public.session_processing_attempts AS attempt
		SET state = 'claimed',
			lease_token = request.lease_token,
			lease_expires_at = request.lease_expires_at,
			next_attempt_at = NULL
		FROM request
		WHERE attempt.session_id = request.session_id
		  AND attempt.state = 'retryable'
		  AND attempt.next_attempt_at <= request.now
		RETURNING
			attempt.attempt_id,
			attempt.session_id,
			attempt.source_revision,
			attempt.lifecycle_count,
			attempt.observation_count,
			attempt.state,
			attempt.lease_token,
			attempt.lease_expires_at
	),
	reclaimed_expired AS (
		UPDATE public.session_processing_attempts AS attempt
		SET lease_token = request.lease_token,
			lease_expires_at = request.lease_expires_at
		FROM request
		WHERE attempt.session_id = request.session_id
		  AND attempt.state IN ('claimed', 'staged', 'publishing')
		  AND attempt.lease_expires_at <= request.now
		RETURNING
			attempt.attempt_id,
			attempt.session_id,
			attempt.source_revision,
			attempt.lifecycle_count,
			attempt.observation_count,
			attempt.state,
			attempt.lease_token,
			attempt.lease_expires_at
	),
	reclaimed AS (
		SELECT * FROM reclaimed_retryable
		UNION ALL
		SELECT * FROM reclaimed_expired
	),
	inserted AS (
		INSERT INTO public.session_processing_attempts (
			attempt_id,
			session_id,
			source_revision,
			lifecycle_count,
			observation_count,
			state,
			lease_token,
			lease_expires_at,
			next_attempt_at
		)
		SELECT
			request.attempt_id,
			request.session_id,
			request.source_revision,
			request.lifecycle_count,
			request.observation_count,
			'claimed',
			request.lease_token,
			request.lease_expires_at,
			NULL
		FROM request
		WHERE NOT EXISTS (SELECT 1 FROM reclaimed)
		ON CONFLICT DO NOTHING
		RETURNING
			attempt_id,
			session_id,
			source_revision,
			lifecycle_count,
			observation_count,
			state,
			lease_token,
			lease_expires_at
	),
	claimed AS (
		SELECT * FROM reclaimed
		UNION ALL
		SELECT * FROM inserted
	)
	SELECT
		attempt_id::text AS attempt_id,
		session_id,
		source_revision::text AS source_revision,
		lifecycle_count::text AS lifecycle_count,
		observation_count::text AS observation_count,
		state,
		lease_token::text AS lease_token,
		lease_expires_at
	FROM claimed;"

readonly task_5_3_renew_prepare="
	PREPARE task_5_3_renew(text, text, text, text) AS
	WITH request AS (
		SELECT
			\$1::text::uuid AS attempt_id,
			\$2::text::uuid AS lease_token,
			\$3::text::timestamptz AS now,
			\$4::text::timestamptz AS lease_expires_at
	)
	UPDATE public.session_processing_attempts AS attempt
	SET lease_expires_at = request.lease_expires_at
	FROM request
	WHERE attempt.attempt_id = request.attempt_id
	  AND attempt.lease_token = request.lease_token
	  AND attempt.state IN ('claimed', 'staged', 'publishing')
	  AND attempt.lease_expires_at > request.now
	RETURNING TRUE AS renewed;"

readonly task_5_3_source_load_prepare="
	PREPARE task_5_3_source_load(text, text, text, text) AS
	WITH request AS (
		SELECT
			\$1::text::uuid AS attempt_id,
			\$2::text::uuid AS lease_token,
			\$3::text AS session_id,
			\$4::text::uuid AS source_revision
	),
	fenced_attempt AS (
		SELECT attempt.session_id
		FROM public.session_processing_attempts AS attempt
		JOIN request
			ON attempt.attempt_id = request.attempt_id
		WHERE attempt.lease_token = request.lease_token
		  AND attempt.session_id = request.session_id
		  AND attempt.source_revision = request.source_revision
		  AND attempt.state IN ('claimed', 'staged', 'publishing')
	),
	source_entries AS (
		SELECT
			jsonb_build_object(
				'kind', 'lifecycle',
				'receipt_id', lifecycle.receipt_id::text,
				'event_type', lifecycle.event_type,
				'session_id', lifecycle.session_id,
				'project_name', lifecycle.project_name,
				'current_working_directory', lifecycle.current_working_directory,
				'source_timestamp_rfc3339', lifecycle.source_timestamp_rfc3339,
				'source_timestamp_utc', lifecycle.source_timestamp_utc,
				'ingested_at', lifecycle.ingested_at
			) AS entry,
			current_record.session_id IS NOT NULL AS prior_projection_present,
			current_record.transcript AS prior_transcript
		FROM fenced_attempt
		JOIN session_events AS lifecycle
			ON lifecycle.session_id = fenced_attempt.session_id
		LEFT JOIN public.session_records AS current_record
			ON current_record.session_id = fenced_attempt.session_id
		UNION ALL
		SELECT
			jsonb_build_object(
				'kind', 'observation',
				'receipt_id', observation.receipt_id::text,
				'event_type', observation.event_type,
				'session_id', observation.session_id,
				'hook_type', observation.hook_type,
				'project_name', observation.project_name,
				'current_working_directory', observation.current_working_directory,
				'source_timestamp_rfc3339', observation.source_timestamp_rfc3339,
				'source_timestamp_utc', observation.source_timestamp_utc,
				'ingested_at', observation.ingested_at,
				'data', observation.data
			) AS entry,
			current_record.session_id IS NOT NULL AS prior_projection_present,
			current_record.transcript AS prior_transcript
		FROM fenced_attempt
		JOIN raw_observations AS observation
			ON observation.session_id = fenced_attempt.session_id
		LEFT JOIN public.session_records AS current_record
			ON current_record.session_id = fenced_attempt.session_id
	)
	SELECT entry, prior_projection_present, prior_transcript
	FROM source_entries;"

readonly task_5_3_supersede_prepare="
	PREPARE task_5_3_supersede(text, text) AS
	WITH request AS (
		SELECT
			\$1::text::uuid AS attempt_id,
			\$2::text::uuid AS lease_token
	)
	UPDATE public.session_processing_attempts AS attempt
	SET state = 'superseded'
	FROM request
	WHERE attempt.attempt_id = request.attempt_id
	  AND attempt.lease_token = request.lease_token
	  AND attempt.state = 'claimed'
	RETURNING TRUE AS superseded;"

# Each rejected statement uses a separate session so ON_ERROR_STOP does not
# suppress later assertions or expose PostgreSQL error context in diagnostics.
expect_failure() {
	local description=$1
	local role=$2
	local sql=$3

	if query_as "$role" "$sql" >/dev/null 2>&1; then
		fail "expected rejection: $description"
	fi

	printf 'rejected=%s\n' "$description"
}

source_security_snapshot() {
	migration_query "
		SELECT string_agg(
			relation.relname || '|' || pg_get_userbyid(relation.relowner) || '|' ||
			has_table_privilege('$application_role', relation.oid, 'SELECT') || '|' ||
			has_table_privilege('$application_role', relation.oid, 'INSERT') || '|' ||
			has_table_privilege('$application_role', relation.oid, 'UPDATE') || '|' ||
			has_table_privilege('$application_role', relation.oid, 'DELETE') || '|' ||
			has_table_privilege('$application_role', relation.oid, 'TRUNCATE') || '|' ||
			(SELECT count(*) = 0
			 FROM aclexplode(relation.relacl) AS privilege
			 WHERE privilege.grantee = 0),
			E'\\n' ORDER BY relation.relname
		)
		FROM pg_class AS relation
		JOIN pg_namespace AS schema ON schema.oid = relation.relnamespace
		WHERE schema.nspname = 'public'
		  AND relation.relkind = 'r'
		  AND relation.relname IN ('session_events', 'raw_observations')"
}

source_grants_snapshot() {
	migration_query "
		SELECT string_agg(
			relation.relname || '|' || coalesce(array_to_string(relation.relacl, ','), ''),
			E'\\n' ORDER BY relation.relname
		)
		FROM pg_class AS relation
		JOIN pg_namespace AS schema ON schema.oid = relation.relnamespace
		WHERE schema.nspname = 'public'
		  AND relation.relkind = 'r'
		  AND relation.relname IN ('session_events', 'raw_observations')"
}

apply_migration() {
	"$psql" \
		--no-psqlrc \
		--no-password \
		--host="$socket_dir" \
		--port="$PGPORT" \
		--username="$migration_role" \
		--dbname="$database" \
		--set=ON_ERROR_STOP=1 \
		--single-transaction \
		--quiet \
		--file="$schema_migration" >/dev/null
}

task_5_3_discovery_at_fixed_now() {
	safe_application_query "$task_5_3_discovery_prepare
		EXECUTE task_5_3_discovery('2026-02-01T00:00:00Z');
		DEALLOCATE task_5_3_discovery"
}

task_5_3_discovery_at_quiet_boundary() {
	safe_application_query "$task_5_3_discovery_prepare
		EXECUTE task_5_3_discovery('2026-02-01T00:00:01Z');
		DEALLOCATE task_5_3_discovery"
}

task_5_3_claim_candidates_at_fixed_now() {
	safe_application_query "$task_5_3_claim_candidates_prepare
		EXECUTE task_5_3_claim_candidates('2026-02-01T00:00:00Z', '100');
		DEALLOCATE task_5_3_claim_candidates"
}

task_5_3_claim_candidates_at_lease_reclaim() {
	safe_application_query "$task_5_3_claim_candidates_prepare
		EXECUTE task_5_3_claim_candidates('2026-02-01T00:06:00Z', '100');
		DEALLOCATE task_5_3_claim_candidates"
}

task_5_3_source_identities() {
	local session_id=$1

	case "$session_id" in
	concurrent-claim | lease-expired | claim-snapshot-race) ;;
	*) fail 'unrecognized task 5.3 source identity fixture' ;;
	esac

	safe_application_query "
		WITH identities AS (
			SELECT event_type, receipt_id
			FROM session_events
			WHERE session_id = '$session_id'
			UNION ALL
			SELECT event_type, receipt_id
			FROM raw_observations
			WHERE session_id = '$session_id'
		)
		SELECT COALESCE(
			string_agg(
				event_type || E'\t' || receipt_id::text,
				E'\n' ORDER BY event_type COLLATE \"C\", receipt_id::text COLLATE \"C\"
			),
			''
		)
		FROM identities"
}

seed_task_5_3_eligibility_sources() {
	safe_source_execute "
		INSERT INTO public.session_events (
			receipt_id, session_id, event_type, project_name, current_working_directory,
			source_timestamp_rfc3339, source_timestamp_utc, ingested_at
		) VALUES
			('00000000-0000-4000-8000-000000000101', 'eligibility-ended-only', 'session_end', 'fixture-project', '/fixture', '2026-01-31T10:00:00Z', TIMESTAMPTZ '2026-01-31 10:00:00+00', TIMESTAMPTZ '2026-01-31 10:00:00+00'),
			('00000000-0000-4000-8000-000000000102', 'eligibility-out-of-order', 'session_end', 'fixture-project', '/fixture', '2026-01-31T10:00:00Z', TIMESTAMPTZ '2026-01-31 10:00:00+00', TIMESTAMPTZ '2026-01-31 10:00:00+00'),
			('00000000-0000-4000-8000-000000000103', 'eligibility-out-of-order', 'session_start', 'fixture-project', '/fixture', '2026-01-31T12:00:00Z', TIMESTAMPTZ '2026-01-31 12:00:00+00', TIMESTAMPTZ '2026-01-31 12:00:00+00'),
			('00000000-0000-4000-8000-000000000104', 'eligibility-out-of-order', 'session_end', 'fixture-project', '/fixture', '2026-01-31T09:00:00Z', TIMESTAMPTZ '2026-01-31 09:00:00+00', TIMESTAMPTZ '2026-01-31 12:00:01+00');
		INSERT INTO public.raw_observations (
			receipt_id, session_id, event_type, hook_type, project_name,
			current_working_directory, source_timestamp_rfc3339, source_timestamp_utc,
			ingested_at, data
		) VALUES
			('00000000-0000-4000-8000-000000000105', 'eligibility-boundary', 'observation', 'fixture_hook', 'fixture-project', '/fixture', '2026-01-31T00:00:00Z', TIMESTAMPTZ '2026-01-31 00:00:00+00', TIMESTAMPTZ '2026-01-31 00:00:00+00', '{}'::json),
			('00000000-0000-4000-8000-000000000106', 'eligibility-active', 'observation', 'fixture_hook', 'fixture-project', '/fixture', '2026-01-31T00:00:01Z', TIMESTAMPTZ '2026-01-31 00:00:01+00', TIMESTAMPTZ '2026-01-31 00:00:01+00', '{}'::json),
			('00000000-0000-4000-8000-000000000107', 'eligibility-resumed', 'observation', 'fixture_hook', 'fixture-project', '/fixture', '2026-01-29T00:00:00Z', TIMESTAMPTZ '2026-01-29 00:00:00+00', TIMESTAMPTZ '2026-01-29 00:00:00+00', '{}'::json)"
}

seed_task_5_3_resumed_record() {
	safe_application_execute "
		INSERT INTO public.session_records (
			session_id, source_revision, lifecycle_count, observation_count, source_cutoff,
			transcript, summary_sentences, summary, concepts, generated_at
		) VALUES (
			'eligibility-resumed', '3d77d36f-c9d9-5b7d-8803-e33fef6cf1ad', 0, 1,
			TIMESTAMPTZ '2026-01-29 00:00:00+00', '[]'::jsonb,
			ARRAY['resumed baseline']::text[], 'resumed baseline',
			ARRAY['resumed-01', 'resumed-02', 'resumed-03', 'resumed-04', 'resumed-05', 'resumed-06', 'resumed-07', 'resumed-08', 'resumed-09', 'resumed-10']::text[],
			TIMESTAMPTZ '2026-01-29 00:01:00+00'
		)"
}

seed_task_5_3_resumed_late_source() {
	safe_source_execute "
		INSERT INTO public.raw_observations (
			receipt_id, session_id, event_type, hook_type, project_name,
			current_working_directory, source_timestamp_rfc3339, source_timestamp_utc,
			ingested_at, data
		) VALUES (
			'00000000-0000-4000-8000-000000000108', 'eligibility-resumed', 'observation', 'fixture_hook', 'fixture-project',
			'/fixture', '2026-01-31T00:00:01Z', TIMESTAMPTZ '2026-01-31 00:00:01+00', TIMESTAMPTZ '2026-01-31 00:00:01+00', '{}'::json
		)"
}

seed_task_5_3_eligibility_fixture() {
	seed_task_5_3_eligibility_sources
	seed_task_5_3_resumed_record
	seed_task_5_3_resumed_late_source
}

resumed_record_is_unchanged() {
	safe_application_query "
		SELECT count(*)
		FROM public.session_records
		WHERE session_id = 'eligibility-resumed'
		  AND source_revision = '3d77d36f-c9d9-5b7d-8803-e33fef6cf1ad'
		  AND lifecycle_count = 0
		  AND observation_count = 1
		  AND source_cutoff = TIMESTAMPTZ '2026-01-29 00:00:00+00'
		  AND summary = 'resumed baseline'
		  AND generated_at = TIMESTAMPTZ '2026-01-29 00:01:00+00'"
}

verify_task_5_3_eligibility() {
	local candidate_rows

	candidate_rows="$(task_5_3_claim_candidates_at_fixed_now)"
	expect_fixture_equal "$(fixture_line_count "$candidate_rows")" '4' 'high-limit candidate discovery returns every eligible source'
	expect_fixture_contains "$candidate_rows" 'eligibility-boundary' 'candidate discovery includes the inactivity boundary'
	expect_fixture_contains "$candidate_rows" 'eligibility-ended-only' 'candidate discovery includes the end-only session'
	expect_fixture_contains "$candidate_rows" 'eligibility-out-of-order' 'candidate discovery includes repeated out-of-order lifecycle data'
	expect_fixture_contains "$candidate_rows" 'fixture-session' 'candidate discovery retains the existing eligible fixture session'
	expect_fixture_not_contains "$candidate_rows" 'eligibility-active' 'candidate discovery excludes active source data'
	expect_fixture_not_contains "$candidate_rows" 'eligibility-resumed' 'candidate discovery excludes the resumed quiet period'
	expect_fixture_equal \
		"$(safe_application_query "SELECT
			(SELECT count(*) FROM public.session_records WHERE session_id = 'eligibility-empty') || '|' ||
			(SELECT count(*) FROM public.session_processing_attempts WHERE session_id = 'eligibility-empty')")" \
		'0|0' \
		'empty sessions create neither records nor attempts'
	expect_fixture_equal \
		"$(task_5_3_discovery_at_fixed_now)" \
		$'eligibility-boundary\neligibility-ended-only\neligibility-out-of-order\nfixture-session' \
		'fixed-now discovery eligibility'
	expect_fixture_equal "$(resumed_record_is_unchanged)" '1' 'resumed current record remains unchanged before quiet boundary'
	expect_fixture_equal \
		"$(safe_application_query "SELECT count(*) FROM public.session_processing_attempts WHERE session_id = 'eligibility-resumed'")" \
		'0' \
		'no premature resumed refresh attempt'
	expect_fixture_equal \
		"$(task_5_3_discovery_at_quiet_boundary)" \
		$'eligibility-active\neligibility-boundary\neligibility-ended-only\neligibility-out-of-order\neligibility-resumed\nfixture-session' \
		'full quiet boundary discovery eligibility'
	expect_fixture_equal "$(resumed_record_is_unchanged)" '1' 'resumed prior record remains unchanged during rediscovery'
}

concurrent_claims_dir=''
concurrent_claims_gate_fifo=''
concurrent_claims_gate_open=false
concurrent_claims_gate_pid=''
concurrent_claims_winner_pid=''
concurrent_claims_contender_pid=''
concurrent_claims_last_pid=''

cleanup_concurrent_claims() {
	local pid

	if [[ "$concurrent_claims_gate_open" == true ]]; then
		exec 3>&- || true
		concurrent_claims_gate_open=false
	fi
	for pid in "$concurrent_claims_gate_pid" "$concurrent_claims_winner_pid" "$concurrent_claims_contender_pid"; do
		if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
			kill "$pid" >/dev/null 2>&1 || true
		fi
	done
	for pid in "$concurrent_claims_gate_pid" "$concurrent_claims_winner_pid" "$concurrent_claims_contender_pid"; do
		if [[ -n "$pid" ]]; then
			wait "$pid" >/dev/null 2>&1 || true
		fi
	done
	concurrent_claims_gate_pid=''
	concurrent_claims_winner_pid=''
	concurrent_claims_contender_pid=''
	if [[ -n "$concurrent_claims_dir" && -d "$concurrent_claims_dir" ]]; then
		rm -rf -- "$concurrent_claims_dir" >/dev/null 2>&1 || true
	fi
	concurrent_claims_dir=''
	concurrent_claims_gate_fifo=''
}

trap cleanup_concurrent_claims EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

wait_for_concurrent_gate_lock() {
	local attempt
	local observed

	for ((attempt = 0; attempt < 100; attempt++)); do
		observed="$(safe_application_query "
			SELECT count(*)
			FROM pg_locks AS lock
			JOIN pg_stat_activity AS activity USING (pid)
			WHERE activity.application_name = 'session-post-processing-claim-gate'
			  AND activity.usename = current_user
			  AND lock.locktype = 'advisory'
			  AND lock.classid = 5303
			  AND lock.objid = 53
			  AND lock.objsubid = 2
			  AND lock.granted")"
		if [[ "$observed" == '1' ]]; then
			return
		fi
		sleep 0.05
	done

	fail 'bounded concurrent claim gate did not acquire its advisory lock'
}

wait_for_concurrent_winner_lock() {
	local attempt
	local observed

	for ((attempt = 0; attempt < 100; attempt++)); do
		observed="$(safe_application_query "
			SELECT count(*)
			FROM pg_stat_activity
			WHERE application_name = 'session-post-processing-claim-winner'
			  AND usename = current_user
			  AND state = 'active'
			  AND wait_event_type = 'Lock'
			  AND wait_event = 'advisory'")"
		if [[ "$observed" == '1' ]]; then
			return
		fi
		sleep 0.05
	done

	fail 'bounded concurrent claim winner did not reach the advisory gate'
}

wait_for_concurrent_contender_lock() {
	local attempt
	local observed

	for ((attempt = 0; attempt < 100; attempt++)); do
		observed="$(safe_application_query "
			SELECT count(*)
			FROM pg_stat_activity
			WHERE application_name = 'session-post-processing-claim-contender'
			  AND usename = current_user
			  AND state = 'active'
			  AND wait_event_type = 'Lock'
			  AND wait_event = 'transactionid'")"
		if [[ "$observed" == '1' ]]; then
			return
		fi
		sleep 0.05
	done

	fail 'bounded concurrent claim contender did not wait on the unique constraint'
}

start_concurrent_claim_application() {
	local application_name=$1
	local sql=$2

	PGAPPNAME="$application_name" "$psql" \
		--no-psqlrc \
		--no-password \
		--host="$socket_dir" \
		--port="$PGPORT" \
		--username="$application_role" \
		--dbname="$database" \
		--set=ON_ERROR_STOP=1 \
		--tuples-only \
		--no-align \
		--quiet \
		--command="$sql" >/dev/null 2>&1 &
	concurrent_claims_last_pid=$!
}

seed_task_5_3_concurrent_claim_source() {
	safe_source_execute "
		INSERT INTO public.session_events (
			receipt_id, session_id, event_type, project_name, current_working_directory,
			source_timestamp_rfc3339, source_timestamp_utc, ingested_at
		) VALUES (
			'00000000-0000-4000-8000-000000000109', 'concurrent-claim', 'session_end', 'fixture-project', '/fixture',
			'2026-01-31T12:00:00Z', TIMESTAMPTZ '2026-01-31 12:00:00+00', TIMESTAMPTZ '2026-01-31 12:00:00+00'
		)"
}

verify_task_5_3_concurrent_claims() {
	local gate_status
	local winner_status
	local contender_status

	seed_task_5_3_concurrent_claim_source
	expect_fixture_equal \
		"$(task_5_3_source_identities 'concurrent-claim')" \
		"$task_5_3_concurrent_claim_source_identities" \
		'concurrent claim source identities match the revision input'
	if ! concurrent_claims_dir="$(mktemp -d "${TMPDIR:-/tmp}/spp-claim-race.XXXXXX" 2>/dev/null)"; then
		fail 'concurrent claim private workspace creation failed'
	fi
	concurrent_claims_gate_fifo="$concurrent_claims_dir/gate"
	if ! mkfifo "$concurrent_claims_gate_fifo" 2>/dev/null; then
		fail 'concurrent claim gate creation failed'
	fi

	PGAPPNAME='session-post-processing-claim-gate' "$psql" \
		--no-psqlrc \
		--no-password \
		--host="$socket_dir" \
		--port="$PGPORT" \
		--username="$application_role" \
		--dbname="$database" \
		--set=ON_ERROR_STOP=1 \
		--tuples-only \
		--no-align \
		--quiet <"$concurrent_claims_gate_fifo" >/dev/null 2>&1 &
	concurrent_claims_gate_pid=$!
	exec 3>"$concurrent_claims_gate_fifo"
	concurrent_claims_gate_open=true
	if ! printf 'BEGIN ISOLATION LEVEL READ COMMITTED;\nSELECT pg_advisory_xact_lock(5303, 53);\n' >&3; then
		fail 'concurrent claim gate setup failed'
	fi
	wait_for_concurrent_gate_lock

	start_concurrent_claim_application 'session-post-processing-claim-winner' "
		BEGIN ISOLATION LEVEL READ COMMITTED;
		$task_5_3_claim_prepare
		EXECUTE task_5_3_claim(
			'concurrent-claim', '$task_5_3_concurrent_claim_source_revision', '1', '0',
			'018f5d00-0000-7000-8000-000000000201', '018f5d00-0000-7000-8000-000000000202',
			'2026-02-01T00:00:00Z', '2026-02-01T00:05:00Z'
		);
		SELECT pg_advisory_xact_lock(5303, 53);
		COMMIT"
	concurrent_claims_winner_pid=$concurrent_claims_last_pid
	wait_for_concurrent_winner_lock

	start_concurrent_claim_application 'session-post-processing-claim-contender' "
		BEGIN ISOLATION LEVEL READ COMMITTED;
		$task_5_3_claim_prepare
		EXECUTE task_5_3_claim(
			'concurrent-claim', '$task_5_3_concurrent_claim_source_revision', '1', '0',
			'018f5d00-0000-7000-8000-000000000203', '018f5d00-0000-7000-8000-000000000204',
			'2026-02-01T00:00:00Z', '2026-02-01T00:05:00Z'
		);
		SELECT pg_advisory_xact_lock(5303, 53);
		COMMIT"
	concurrent_claims_contender_pid=$concurrent_claims_last_pid
	wait_for_concurrent_contender_lock

	if ! printf 'COMMIT;\n' >&3; then
		fail 'concurrent claim gate release failed'
	fi
	if ! exec 3>&-; then
		fail 'concurrent claim gate close failed'
	fi
	concurrent_claims_gate_open=false

	if wait "$concurrent_claims_gate_pid"; then
		gate_status=0
	else
		gate_status=$?
	fi
	concurrent_claims_gate_pid=''
	expect_fixture_equal "$gate_status" '0' 'concurrent claim gate exit status'
	if wait "$concurrent_claims_winner_pid"; then
		winner_status=0
	else
		winner_status=$?
	fi
	concurrent_claims_winner_pid=''
	expect_fixture_equal "$winner_status" '0' 'concurrent claim winner exit status'
	if wait "$concurrent_claims_contender_pid"; then
		contender_status=0
	else
		contender_status=$?
	fi
	concurrent_claims_contender_pid=''
	expect_fixture_equal "$contender_status" '0' 'concurrent claim contender exit status'
	expect_fixture_equal \
		"$(safe_application_query "SELECT
			(SELECT count(*) FROM public.session_processing_attempts WHERE session_id = 'concurrent-claim' AND state = 'claimed' AND source_revision = '$task_5_3_concurrent_claim_source_revision' AND lifecycle_count = 1 AND observation_count = 0) || '|' ||
			(SELECT count(*) FROM public.session_processing_attempts WHERE session_id = 'concurrent-claim' AND state IN ('claimed', 'staged', 'publishing', 'retryable'))")" \
		'1|1' \
		'concurrent claims retain exactly one nonterminal owner'
	cleanup_concurrent_claims
}

seed_task_5_3_expired_reclaim_source() {
	safe_source_execute "
		INSERT INTO public.session_events (
			receipt_id, session_id, event_type, project_name, current_working_directory,
			source_timestamp_rfc3339, source_timestamp_utc, ingested_at
		) VALUES (
			'00000000-0000-4000-8000-000000000110', 'lease-expired', 'session_end', 'fixture-project', '/fixture',
			'2026-01-31T13:00:00Z', TIMESTAMPTZ '2026-01-31 13:00:00+00', TIMESTAMPTZ '2026-01-31 13:00:00+00'
		)"
}

append_task_5_3_expired_reclaim_source() {
	safe_source_execute "
		INSERT INTO public.raw_observations (
			receipt_id, session_id, event_type, hook_type, project_name,
			current_working_directory, source_timestamp_rfc3339, source_timestamp_utc,
			ingested_at, data
		) VALUES (
			'00000000-0000-4000-8000-000000000111', 'lease-expired', 'observation', 'fixture_hook', 'fixture-project',
			'/fixture', '2026-02-01T00:05:00Z', TIMESTAMPTZ '2026-02-01 00:05:00+00', TIMESTAMPTZ '2026-02-01 00:05:00+00', '{}'::json
		)"
}

claim_task_5_3_expired_reclaim_initial() {
	safe_application_execute "$task_5_3_claim_prepare
		EXECUTE task_5_3_claim(
			'lease-expired', '$task_5_3_expired_reclaim_initial_source_revision', '1', '0',
			'018f5d00-0000-7000-8000-000000000205', '018f5d00-0000-7000-8000-000000000206',
			'2026-02-01T00:00:00Z', '2026-02-01T00:05:00Z'
		);
		DEALLOCATE task_5_3_claim"
}

claim_task_5_3_expired_reclaim_active() {
	safe_application_execute "$task_5_3_claim_prepare
		EXECUTE task_5_3_claim(
			'lease-expired', '$task_5_3_expired_reclaim_full_source_revision', '1', '1',
			'018f5d00-0000-7000-8000-000000000207', '018f5d00-0000-7000-8000-000000000208',
			'2026-02-01T00:06:00Z', '2026-02-01T00:11:00Z'
		);
		DEALLOCATE task_5_3_claim"
}

renew_task_5_3_expired_reclaim_stale() {
	safe_application_query "$task_5_3_renew_prepare
		EXECUTE task_5_3_renew(
			'018f5d00-0000-7000-8000-000000000205', '018f5d00-0000-7000-8000-000000000206',
			'2026-02-01T00:07:00Z', '2026-02-01T00:12:00Z'
		);
		DEALLOCATE task_5_3_renew"
}

renew_task_5_3_expired_reclaim_active() {
	safe_application_query "$task_5_3_renew_prepare
		EXECUTE task_5_3_renew(
			'018f5d00-0000-7000-8000-000000000205', '018f5d00-0000-7000-8000-000000000208',
			'2026-02-01T00:07:00Z', '2026-02-01T00:12:00Z'
		);
		DEALLOCATE task_5_3_renew"
}

supersede_task_5_3_expired_reclaim_stale() {
	safe_application_query "$task_5_3_supersede_prepare
		EXECUTE task_5_3_supersede(
			'018f5d00-0000-7000-8000-000000000205', '018f5d00-0000-7000-8000-000000000206'
		);
		DEALLOCATE task_5_3_supersede"
}

supersede_task_5_3_expired_reclaim_active() {
	safe_application_query "$task_5_3_supersede_prepare
		EXECUTE task_5_3_supersede(
			'018f5d00-0000-7000-8000-000000000205', '018f5d00-0000-7000-8000-000000000208'
		);
		DEALLOCATE task_5_3_supersede"
}

verify_task_5_3_expired_reclaim() {
	local candidate_rows

	seed_task_5_3_expired_reclaim_source
	expect_fixture_equal \
		"$(task_5_3_source_identities 'lease-expired')" \
		"$task_5_3_expired_reclaim_initial_source_identities" \
		'expired reclaim initial source identities match the revision input'
	claim_task_5_3_expired_reclaim_initial
	append_task_5_3_expired_reclaim_source
	candidate_rows="$(task_5_3_claim_candidates_at_lease_reclaim)"
	expect_fixture_contains "$candidate_rows" 'lease-expired|1|1|' 'expired reclaim sees the changed source counts with a high candidate limit'
	expect_fixture_equal \
		"$(task_5_3_source_identities 'lease-expired')" \
		"$task_5_3_expired_reclaim_full_source_identities" \
		'expired reclaim changed source identities match the revision input'
	expect_fixture_not_equal \
		"$task_5_3_expired_reclaim_initial_source_revision" \
		"$task_5_3_expired_reclaim_full_source_revision" \
		'expired reclaim changed source revision differs from the retained claim'
	claim_task_5_3_expired_reclaim_active
	expect_fixture_equal \
		"$(safe_application_query "SELECT
			(SELECT count(*) FROM public.session_processing_attempts WHERE attempt_id = '018f5d00-0000-7000-8000-000000000205' AND source_revision = '$task_5_3_expired_reclaim_initial_source_revision' AND lifecycle_count = 1 AND observation_count = 0 AND state = 'claimed' AND lease_token = '018f5d00-0000-7000-8000-000000000208') || '|' ||
			(SELECT count(*) FROM public.session_processing_attempts WHERE source_revision = '$task_5_3_expired_reclaim_full_source_revision')")" \
		'1|0' \
		'expired reclaim retains the original attempt identity revision and counts'
	expect_fixture_equal "$(renew_task_5_3_expired_reclaim_stale)" '' 'stale lease token cannot renew'
	expect_fixture_equal "$(supersede_task_5_3_expired_reclaim_stale)" '' 'stale lease token cannot supersede'
	expect_fixture_equal "$(renew_task_5_3_expired_reclaim_active)" 't' 'active reclaimed lease token renews'
	expect_fixture_equal "$(supersede_task_5_3_expired_reclaim_active)" 't' 'active reclaimed lease token supersedes'
	expect_fixture_equal \
		"$(safe_application_query "SELECT count(*) FROM public.session_processing_attempts WHERE attempt_id = '018f5d00-0000-7000-8000-000000000205' AND state = 'superseded' AND lease_token = '018f5d00-0000-7000-8000-000000000208'")" \
		'1' \
		'active reclaimed token produces the terminal superseded state'
}

task_5_3_claim_candidates_at_snapshot_race() {
	safe_application_query "$task_5_3_claim_candidates_prepare
		EXECUTE task_5_3_claim_candidates('2026-02-01T00:20:30Z', '100');
		DEALLOCATE task_5_3_claim_candidates"
}

seed_task_5_3_snapshot_race_source() {
	safe_source_execute "
		INSERT INTO public.session_events (
			receipt_id, session_id, event_type, project_name, current_working_directory,
			source_timestamp_rfc3339, source_timestamp_utc, ingested_at
		) VALUES (
			'00000000-0000-4000-8000-000000000112', 'claim-snapshot-race', 'session_end', 'fixture-project', '/fixture',
			'2026-01-31T14:00:00Z', TIMESTAMPTZ '2026-01-31 14:00:00+00', TIMESTAMPTZ '2026-01-31 14:00:00+00'
		)"
}

append_task_5_3_snapshot_race_source() {
	safe_source_execute "
		INSERT INTO public.raw_observations (
			receipt_id, session_id, event_type, hook_type, project_name,
			current_working_directory, source_timestamp_rfc3339, source_timestamp_utc,
			ingested_at, data
		) VALUES (
			'00000000-0000-4000-8000-000000000113', 'claim-snapshot-race', 'observation', 'fixture_hook', 'fixture-project',
			'/fixture', '2026-02-01T00:20:30Z', TIMESTAMPTZ '2026-02-01 00:20:30+00', TIMESTAMPTZ '2026-02-01 00:20:30+00', '{}'::json
		)"
}

claim_task_5_3_snapshot_race_initial() {
	safe_application_execute "$task_5_3_claim_prepare
		EXECUTE task_5_3_claim(
			'claim-snapshot-race', '$task_5_3_snapshot_race_old_source_revision', '1', '0',
			'018f5d00-0000-7000-8000-000000000209', '018f5d00-0000-7000-8000-000000000210',
			'2026-02-01T00:20:00Z', '2026-02-01T00:21:00Z'
		);
		DEALLOCATE task_5_3_claim"
}

load_task_5_3_snapshot_race_original() {
	safe_application_query "$task_5_3_source_load_prepare
		EXECUTE task_5_3_source_load(
			'018f5d00-0000-7000-8000-000000000209', '018f5d00-0000-7000-8000-000000000210',
			'claim-snapshot-race', '$task_5_3_snapshot_race_old_source_revision'
		);
		DEALLOCATE task_5_3_source_load"
}

supersede_task_5_3_snapshot_race_original() {
	safe_application_query "$task_5_3_supersede_prepare
		EXECUTE task_5_3_supersede(
			'018f5d00-0000-7000-8000-000000000209', '018f5d00-0000-7000-8000-000000000210'
		);
		DEALLOCATE task_5_3_supersede"
}

claim_task_5_3_snapshot_race_latest() {
	safe_application_execute "$task_5_3_claim_prepare
		EXECUTE task_5_3_claim(
			'claim-snapshot-race', '$task_5_3_snapshot_race_full_source_revision', '1', '1',
			'018f5d00-0000-7000-8000-000000000213', '018f5d00-0000-7000-8000-000000000214',
			'2026-02-01T00:20:30Z', '2026-02-01T00:25:30Z'
		);
		DEALLOCATE task_5_3_claim"
}

verify_task_5_3_snapshot_race() {
	local candidate_rows
	local original_rows

	seed_task_5_3_snapshot_race_source
	expect_fixture_equal \
		"$(task_5_3_source_identities 'claim-snapshot-race')" \
		"$task_5_3_snapshot_race_old_source_identities" \
		'claim-to-snapshot initial source identities match the revision input'
	expect_fixture_equal \
		"$task_5_3_snapshot_race_old_source_revision" \
		"$task_5_3_snapshot_race_expected_old_source_revision" \
		'claim-to-snapshot old source identity derivation matches the production UUIDv5 revision'
	claim_task_5_3_snapshot_race_initial
	expect_fixture_equal \
		"$(safe_application_query "SELECT source_revision::text FROM public.session_processing_attempts WHERE attempt_id = '018f5d00-0000-7000-8000-000000000209'")" \
		"$task_5_3_snapshot_race_old_source_revision" \
		'claim-to-snapshot initial claim uses the production source revision'
	append_task_5_3_snapshot_race_source
	candidate_rows="$(task_5_3_claim_candidates_at_snapshot_race)"
	expect_fixture_contains "$candidate_rows" 'claim-snapshot-race|1|1|' 'claim-to-snapshot race exposes the appended source counts'
	expect_fixture_contains "$candidate_rows" '"event_type": "observation", "receipt_id": "00000000-0000-4000-8000-000000000113"' 'claim-to-snapshot candidate includes the appended source identity'
	expect_fixture_contains "$candidate_rows" '"event_type": "session_end", "receipt_id": "00000000-0000-4000-8000-000000000112"' 'claim-to-snapshot candidate includes the claimed source identity'
	expect_fixture_equal \
		"$(task_5_3_source_identities 'claim-snapshot-race')" \
		"$task_5_3_snapshot_race_full_source_identities" \
		'claim-to-snapshot source identities match the full revision input'
	expect_fixture_equal \
		"$task_5_3_snapshot_race_full_source_revision" \
		"$task_5_3_snapshot_race_expected_full_source_revision" \
		'claim-to-snapshot full source identity derivation matches the production UUIDv5 revision'
	expect_fixture_not_equal \
		"$(safe_application_query "SELECT source_revision::text FROM public.session_processing_attempts WHERE attempt_id = '018f5d00-0000-7000-8000-000000000209'")" \
		"$task_5_3_snapshot_race_full_source_revision" \
		'claim-to-snapshot loaded source revision differs from the active claim'
	original_rows="$(load_task_5_3_snapshot_race_original)"
	expect_fixture_equal "$(fixture_line_count "$original_rows")" '2' 'claim-to-snapshot source load exposes the count mismatch without outputting source JSON'
	expect_fixture_equal "$(supersede_task_5_3_snapshot_race_original)" 't' 'active claim-to-snapshot token immediately supersedes the stale claim'
	expect_fixture_equal "$(supersede_task_5_3_snapshot_race_original)" '' 'terminal supersession rejects the stale claim token'
	claim_task_5_3_snapshot_race_latest
	expect_fixture_equal \
		"$(safe_application_query "SELECT
			(SELECT count(*) FROM public.session_processing_attempts WHERE attempt_id = '018f5d00-0000-7000-8000-000000000209' AND source_revision = '$task_5_3_snapshot_race_old_source_revision' AND state = 'superseded' AND lease_token = '018f5d00-0000-7000-8000-000000000210' AND lease_expires_at > TIMESTAMPTZ '2026-02-01 00:20:30+00') || '|' ||
			(SELECT count(*) FROM public.session_processing_attempts WHERE attempt_id = '018f5d00-0000-7000-8000-000000000213' AND source_revision = '$task_5_3_snapshot_race_full_source_revision' AND lifecycle_count = 1 AND observation_count = 1 AND state = 'claimed' AND lease_token = '018f5d00-0000-7000-8000-000000000214' AND lease_expires_at = TIMESTAMPTZ '2026-02-01 00:25:30+00') || '|' ||
			(SELECT count(*) FROM public.session_processing_attempts WHERE session_id = 'claim-snapshot-race' AND state IN ('claimed', 'staged', 'publishing', 'retryable'))")" \
		'1|1|1' \
		'full revision claims immediately after terminal supersession before the old lease expires'
}

readonly task_5_4_stage_prepare="
	PREPARE task_5_4_stage(text, text, text, text, text, text, text, text, text, text, text) AS
	WITH request AS (
		SELECT
			\$1::text::uuid AS attempt_id,
			\$2::text::uuid AS lease_token,
			\$3::text AS session_id,
			\$4::text::uuid AS source_revision,
			\$5::text::timestamptz AS source_cutoff,
			\$6::jsonb AS transcript,
			\$7::jsonb AS summary_sentences,
			\$8::text AS summary,
			\$9::jsonb AS concepts,
			\$10::text::timestamptz AS generated_at,
			\$11::jsonb AS candidates
	),
	staged AS (
		UPDATE public.session_processing_attempts AS attempt
		SET state = 'staged',
			source_cutoff = request.source_cutoff,
			transcript = request.transcript,
			summary_sentences = ARRAY(
				SELECT sentence.value
				FROM jsonb_array_elements_text(request.summary_sentences) WITH ORDINALITY AS sentence(value, ordinality)
				ORDER BY sentence.ordinality
			),
			summary = request.summary,
			concepts = ARRAY(
				SELECT concept.value
				FROM jsonb_array_elements_text(request.concepts) WITH ORDINALITY AS concept(value, ordinality)
				ORDER BY concept.ordinality
			),
			generated_at = request.generated_at
		FROM request
		WHERE attempt.attempt_id = request.attempt_id
		  AND attempt.lease_token = request.lease_token
		  AND attempt.session_id = request.session_id
		  AND attempt.source_revision = request.source_revision
		  AND attempt.state = 'claimed'
		RETURNING attempt.attempt_id, attempt.session_id, attempt.source_revision, attempt.state
	),
	inserted_candidates AS (
		INSERT INTO public.session_memory_candidates (
			attempt_id,
			ordinal,
			session_id,
			content_fingerprint,
			memory_id,
			canonical_payload,
			supporting_receipt_ids
		)
		SELECT
			staged.attempt_id,
			(candidate.ordinality - 1)::integer,
			staged.session_id,
			candidate.value ->> 'content_fingerprint',
			(candidate.value -> 'canonical_payload' ->> 'id')::uuid,
			candidate.value -> 'canonical_payload',
			ARRAY(
				SELECT receipt_id.value::uuid
				FROM jsonb_array_elements_text(candidate.value -> 'supporting_receipt_ids') WITH ORDINALITY AS receipt_id(value, ordinality)
				ORDER BY receipt_id.ordinality
			)
		FROM staged
		CROSS JOIN request
		CROSS JOIN LATERAL jsonb_array_elements(request.candidates) WITH ORDINALITY AS candidate(value, ordinality)
		ORDER BY candidate.ordinality
		ON CONFLICT (session_id, content_fingerprint) DO NOTHING
	)
	SELECT
		staged.attempt_id::text AS attempt_id,
		staged.session_id,
		staged.source_revision::text AS source_revision,
		staged.state
	FROM staged;"

readonly task_5_4_mark_memory_published_prepare="
	PREPARE task_5_4_mark_memory_published(text, text, text, text, text) AS
	WITH request AS (
		SELECT
			\$1::text::uuid AS attempt_id,
			\$2::text::uuid AS lease_token,
			\$3::text AS session_id,
			\$4::text::uuid AS source_revision,
			\$5::text::uuid AS memory_id
	),
	target AS (
		SELECT candidate.attempt_id, candidate.memory_id
		FROM public.session_memory_candidates AS candidate
		JOIN public.session_processing_attempts AS attempt
			ON attempt.attempt_id = candidate.attempt_id
		CROSS JOIN request
		WHERE candidate.attempt_id = request.attempt_id
		  AND candidate.memory_id = request.memory_id
		  AND attempt.attempt_id = request.attempt_id
		  AND attempt.lease_token = request.lease_token
		  AND attempt.session_id = request.session_id
		  AND attempt.source_revision = request.source_revision
		  AND attempt.state IN ('staged', 'publishing')
	),
	publishing AS (
		UPDATE public.session_processing_attempts AS attempt
		SET state = 'publishing'
		FROM request
		WHERE attempt.attempt_id = request.attempt_id
		  AND attempt.lease_token = request.lease_token
		  AND attempt.session_id = request.session_id
		  AND attempt.source_revision = request.source_revision
		  AND attempt.state IN ('staged', 'publishing')
		  AND EXISTS (SELECT 1 FROM target)
		RETURNING attempt.attempt_id, attempt.state
	),
	published AS (
		UPDATE public.session_memory_candidates AS candidate
		SET published_at = COALESCE(candidate.published_at, CURRENT_TIMESTAMP)
		FROM target
		JOIN publishing ON publishing.attempt_id = target.attempt_id
		WHERE candidate.attempt_id = target.attempt_id
		  AND candidate.memory_id = target.memory_id
		RETURNING candidate.attempt_id, candidate.memory_id, candidate.published_at
	)
	SELECT
		published.attempt_id::text AS attempt_id,
		published.memory_id::text AS memory_id,
		published.published_at,
		publishing.state
	FROM published
	JOIN publishing ON publishing.attempt_id = published.attempt_id;"

readonly task_5_4_load_unpublished_candidates_prepare="
	PREPARE task_5_4_load_unpublished_candidates(text, text, text, text) AS
	WITH request AS (
		SELECT
			\$1::text::uuid AS attempt_id,
			\$2::text::uuid AS lease_token,
			\$3::text AS session_id,
			\$4::text::uuid AS source_revision
	),
	publishing AS (
		UPDATE public.session_processing_attempts AS attempt
		SET state = 'publishing'
		FROM request
		WHERE attempt.attempt_id = request.attempt_id
		  AND attempt.lease_token = request.lease_token
		  AND attempt.session_id = request.session_id
		  AND attempt.source_revision = request.source_revision
		  AND attempt.state IN ('staged', 'publishing')
		RETURNING attempt.attempt_id, attempt.session_id, attempt.source_revision, attempt.state
	)
	SELECT
		publishing.attempt_id::text AS attempt_id,
		publishing.session_id,
		publishing.source_revision::text AS source_revision,
		publishing.state,
		COALESCE(
			jsonb_agg(
				jsonb_build_object(
					'ordinal', candidate.ordinal,
					'content_fingerprint', candidate.content_fingerprint,
					'memory_id', candidate.memory_id::text,
					'canonical_payload', candidate.canonical_payload,
					'supporting_receipt_ids', to_jsonb(candidate.supporting_receipt_ids)
				)
				ORDER BY candidate.ordinal
			) FILTER (WHERE candidate.attempt_id IS NOT NULL),
			'[]'::jsonb
		) AS candidates
	FROM publishing
	LEFT JOIN public.session_memory_candidates AS candidate
		ON candidate.attempt_id = publishing.attempt_id
	   AND candidate.published_at IS NULL
	GROUP BY publishing.attempt_id, publishing.session_id, publishing.source_revision, publishing.state;"

readonly task_5_4_retry_prepare="
	PREPARE task_5_4_retry(text, text, text, text, text, text) AS
	WITH request AS (
		SELECT
			\$1::text::uuid AS attempt_id,
			\$2::text::uuid AS lease_token,
			\$3::text AS session_id,
			\$4::text::uuid AS source_revision,
			\$5::text AS failure_category,
			\$6::text::timestamptz AS next_attempt_at
	),
	retryable AS (
		UPDATE public.session_processing_attempts AS attempt
		SET state = 'retryable',
			failure_category = request.failure_category,
			next_attempt_at = request.next_attempt_at
		FROM request
		WHERE attempt.attempt_id = request.attempt_id
		  AND attempt.lease_token = request.lease_token
		  AND attempt.session_id = request.session_id
		  AND attempt.source_revision = request.source_revision
		  AND attempt.state IN ('claimed', 'staged', 'publishing')
		RETURNING
			attempt.attempt_id,
			attempt.session_id,
			attempt.source_revision,
			attempt.state,
			attempt.failure_category,
			attempt.next_attempt_at
	)
	SELECT
		attempt_id::text AS attempt_id,
		session_id,
		source_revision::text AS source_revision,
		state,
		failure_category,
		next_attempt_at
	FROM retryable;"

readonly task_5_4_promote_prepare="
	PREPARE task_5_4_promote(text, text, text, text) AS
	WITH request AS (
		SELECT
			\$1::text::uuid AS attempt_id,
			\$2::text::uuid AS lease_token,
			\$3::text AS session_id,
			\$4::text::uuid AS source_revision
	),
	ready AS MATERIALIZED (
		SELECT
			attempt.attempt_id,
			attempt.lease_token,
			attempt.session_id,
			attempt.source_revision,
			attempt.lifecycle_count,
			attempt.observation_count,
			attempt.source_cutoff,
			attempt.transcript,
			attempt.summary_sentences,
			attempt.summary,
			attempt.concepts,
			attempt.generated_at
		FROM public.session_processing_attempts AS attempt
		JOIN request
			ON attempt.attempt_id = request.attempt_id
		   AND attempt.lease_token = request.lease_token
		   AND attempt.session_id = request.session_id
		   AND attempt.source_revision = request.source_revision
		WHERE attempt.state = 'publishing'
		  AND NOT EXISTS (
			SELECT 1
			FROM public.session_memory_candidates AS candidate
			WHERE candidate.attempt_id = attempt.attempt_id
			  AND candidate.published_at IS NULL
		)
		  AND NOT EXISTS (
			SELECT 1
			FROM public.session_processing_attempts AS other
			WHERE other.session_id = attempt.session_id
			  AND other.attempt_id <> attempt.attempt_id
			  AND other.state IN ('claimed', 'staged', 'publishing', 'retryable')
		)
		FOR UPDATE OF attempt
	),
	promoted AS (
		INSERT INTO public.session_records AS existing_record (
			session_id,
			source_revision,
			lifecycle_count,
			observation_count,
			source_cutoff,
			transcript,
			summary_sentences,
			summary,
			concepts,
			generated_at
		)
		SELECT
			session_id,
			source_revision,
			lifecycle_count,
			observation_count,
			source_cutoff,
			transcript,
			summary_sentences,
			summary,
			concepts,
			generated_at
		FROM ready
		ON CONFLICT (session_id) DO UPDATE
		SET source_revision = EXCLUDED.source_revision,
			lifecycle_count = EXCLUDED.lifecycle_count,
			observation_count = EXCLUDED.observation_count,
			source_cutoff = EXCLUDED.source_cutoff,
			transcript = EXCLUDED.transcript,
			summary_sentences = EXCLUDED.summary_sentences,
			summary = EXCLUDED.summary,
			concepts = EXCLUDED.concepts,
			generated_at = EXCLUDED.generated_at
		RETURNING session_id, source_revision
	),
	completed AS (
		UPDATE public.session_processing_attempts AS attempt
		SET state = 'complete'
		FROM ready
		JOIN promoted
			ON promoted.session_id = ready.session_id
		   AND promoted.source_revision = ready.source_revision
		WHERE attempt.attempt_id = ready.attempt_id
		  AND attempt.lease_token = ready.lease_token
		  AND attempt.session_id = ready.session_id
		  AND attempt.source_revision = ready.source_revision
		  AND attempt.state = 'publishing'
		RETURNING
			attempt.attempt_id,
			attempt.session_id,
			attempt.source_revision,
			attempt.state
	)
	SELECT
		completed.attempt_id::text AS attempt_id,
		promoted.session_id,
		promoted.source_revision::text AS source_revision,
		completed.state
	FROM completed
	JOIN promoted
		ON promoted.session_id = completed.session_id
	   AND promoted.source_revision = completed.source_revision;"

task_5_4_application_query() {
	local output

	if ! output="$(application_query "$1" 2>/dev/null)"; then
		fail 'task 5.4 application query failed'
	fi
	printf '%s' "$output"
}

task_5_4_application_execute() {
	if ! application_query "$1" >/dev/null 2>/dev/null; then
		fail 'task 5.4 application statement failed'
	fi
}

task_5_4_text_literal() {
	local value=$1

	value=${value//\'/\'\'}
	printf "'%s'" "$value"
}

task_5_4_text_arguments() {
	local argument
	local arguments=''
	local literal

	for argument in "$@"; do
		literal="$(task_5_4_text_literal "$argument")"
		if [[ -n "$arguments" ]]; then
			arguments="$arguments, "
		fi
		arguments="$arguments$literal"
	done
	printf '%s' "$arguments"
}

task_5_4_prepared_query() {
	local prepared=$1
	local statement_name=$2
	shift 2
	local arguments

	arguments="$(task_5_4_text_arguments "$@")"
	task_5_4_application_query "$prepared
		EXECUTE $statement_name($arguments);
		DEALLOCATE $statement_name"
}

task_5_4_prepared_rollback_failure() {
	local description=$1
	local prepared=$2
	local statement_name=$3
	shift 3
	local arguments

	arguments="$(task_5_4_text_arguments "$@")"
	expect_failure "$description" "$application_role" "
		BEGIN;
		$prepared
		EXECUTE $statement_name($arguments);
		DEALLOCATE $statement_name;
		SELECT 1 / 0"
}

task_5_4_prepared_explain() {
	local prepared=$1
	local statement_name=$2
	shift 2
	local arguments

	arguments="$(task_5_4_text_arguments "$@")"
	task_5_4_application_query "$prepared
		EXPLAIN (COSTS OFF) EXECUTE $statement_name($arguments);
		DEALLOCATE $statement_name"
}

task_5_4_stage() {
	task_5_4_prepared_query "$task_5_4_stage_prepare" task_5_4_stage "$@"
}

task_5_4_load_unpublished_candidates() {
	task_5_4_prepared_query \
		"$task_5_4_load_unpublished_candidates_prepare" \
		task_5_4_load_unpublished_candidates \
		"$@"
}

task_5_4_mark_memory_published() {
	task_5_4_prepared_query \
		"$task_5_4_mark_memory_published_prepare" \
		task_5_4_mark_memory_published \
		"$@"
}

task_5_4_retry() {
	task_5_4_prepared_query "$task_5_4_retry_prepare" task_5_4_retry "$@"
}

task_5_4_promote() {
	task_5_4_prepared_query "$task_5_4_promote_prepare" task_5_4_promote "$@"
}

task_5_4_claim() {
	local arguments

	arguments="$(task_5_4_text_arguments "$@")"
	task_5_4_application_query "$task_5_3_claim_prepare
		EXECUTE task_5_3_claim($arguments);
		DEALLOCATE task_5_3_claim"
}

task_5_4_discovery_at() {
	task_5_4_prepared_query "$task_5_3_discovery_prepare" task_5_3_discovery "$1"
}

task_5_4_discovery_plan_at() {
	task_5_4_prepared_explain "$task_5_3_discovery_prepare" task_5_3_discovery "$1"
}

task_5_4_claim_candidates_plan_at() {
	task_5_4_prepared_explain \
		"$task_5_3_claim_candidates_prepare" \
		task_5_3_claim_candidates \
		"$1" \
		'100'
}

task_5_4_memory_id() {
	uuid_v5_from_text \
		"$uuid_namespace_url" \
		"total-recall/session-post-processing/smoke/task-5-4/$1"
}

task_5_4_candidate() {
	local fingerprint=$1
	local memory_id=$2
	local session_id=$3
	local observation_receipt=$4
	local timestamp=$5

	printf '%s' \
		'{"content_fingerprint":"'"$fingerprint"'","canonical_payload":{"id":"'"$memory_id"'","version":1,"memory_type":"session-derived","title":"Task 5.4 '"$fingerprint"'","content":"Task 5.4 durable '"$fingerprint"'","created_at":"'"$timestamp"'","updated_at":"'"$timestamp"'","concepts":["task-5-4-01","task-5-4-02","task-5-4-03","task-5-4-04","task-5-4-05","task-5-4-06","task-5-4-07","task-5-4-08","task-5-4-09","task-5-4-10"],"files":[],"session_ids":["'"$session_id"'"],"source_observation_ids":["'"$observation_receipt"'"]},"supporting_receipt_ids":["'"$observation_receipt"'"]}'
}

task_5_4_end_observation_transcript() {
	local session_id=$1
	local observation_receipt=$2
	local end_receipt=$3
	local timestamp=$4

	printf '%s' \
		'{"entries":[{"kind":"observation","receipt_id":"'"$observation_receipt"'","event_type":"observation","session_id":"'"$session_id"'","hook_type":"fixture_hook","project_name":"fixture-project","current_working_directory":"/fixture","source_timestamp_rfc3339":"'"$timestamp"'","source_timestamp_utc":"'"$timestamp"'","ingested_at":"'"$timestamp"'","data":{}},{"kind":"lifecycle","receipt_id":"'"$end_receipt"'","event_type":"session_end","session_id":"'"$session_id"'","project_name":"fixture-project","current_working_directory":"/fixture","source_timestamp_rfc3339":"'"$timestamp"'","source_timestamp_utc":"'"$timestamp"'","ingested_at":"'"$timestamp"'"}]}'
}

task_5_4_two_observation_end_transcript() {
	local session_id=$1
	local first_observation_receipt=$2
	local second_observation_receipt=$3
	local end_receipt=$4
	local timestamp=$5

	printf '%s' \
		'{"entries":[{"kind":"observation","receipt_id":"'"$first_observation_receipt"'","event_type":"observation","session_id":"'"$session_id"'","hook_type":"fixture_hook","project_name":"fixture-project","current_working_directory":"/fixture","source_timestamp_rfc3339":"'"$timestamp"'","source_timestamp_utc":"'"$timestamp"'","ingested_at":"'"$timestamp"'","data":{}},{"kind":"observation","receipt_id":"'"$second_observation_receipt"'","event_type":"observation","session_id":"'"$session_id"'","hook_type":"fixture_hook","project_name":"fixture-project","current_working_directory":"/fixture","source_timestamp_rfc3339":"'"$timestamp"'","source_timestamp_utc":"'"$timestamp"'","ingested_at":"'"$timestamp"'","data":{}},{"kind":"lifecycle","receipt_id":"'"$end_receipt"'","event_type":"session_end","session_id":"'"$session_id"'","project_name":"fixture-project","current_working_directory":"/fixture","source_timestamp_rfc3339":"'"$timestamp"'","source_timestamp_utc":"'"$timestamp"'","ingested_at":"'"$timestamp"'"}]}'
}

task_5_4_observation_transcript() {
	local session_id=$1
	local observation_receipt=$2
	local timestamp=$3

	printf '%s' \
		'{"entries":[{"kind":"observation","receipt_id":"'"$observation_receipt"'","event_type":"observation","session_id":"'"$session_id"'","hook_type":"fixture_hook","project_name":"fixture-project","current_working_directory":"/fixture","source_timestamp_rfc3339":"'"$timestamp"'","source_timestamp_utc":"'"$timestamp"'","ingested_at":"'"$timestamp"'","data":{}}]}'
}

task_5_4_lifecycle_transcript() {
	local session_id=$1
	local end_receipt=$2
	local timestamp=$3

	printf '%s' \
		'{"entries":[{"kind":"lifecycle","receipt_id":"'"$end_receipt"'","event_type":"session_end","session_id":"'"$session_id"'","project_name":"fixture-project","current_working_directory":"/fixture","source_timestamp_rfc3339":"'"$timestamp"'","source_timestamp_utc":"'"$timestamp"'","ingested_at":"'"$timestamp"'"}]}'
}

task_5_4_two_observation_transcript() {
	local session_id=$1
	local first_observation_receipt=$2
	local second_observation_receipt=$3
	local timestamp=$4

	printf '%s' \
		'{"entries":[{"kind":"observation","receipt_id":"'"$first_observation_receipt"'","event_type":"observation","session_id":"'"$session_id"'","hook_type":"fixture_hook","project_name":"fixture-project","current_working_directory":"/fixture","source_timestamp_rfc3339":"'"$timestamp"'","source_timestamp_utc":"'"$timestamp"'","ingested_at":"'"$timestamp"'","data":{}},{"kind":"observation","receipt_id":"'"$second_observation_receipt"'","event_type":"observation","session_id":"'"$session_id"'","hook_type":"fixture_hook","project_name":"fixture-project","current_working_directory":"/fixture","source_timestamp_rfc3339":"'"$timestamp"'","source_timestamp_utc":"'"$timestamp"'","ingested_at":"'"$timestamp"'","data":{}}]}'
}

task_5_4_seed_ended_source() {
	local session_id=$1
	local observation_receipt=$2
	local end_receipt=$3
	local timestamp=$4

	safe_source_execute "
		INSERT INTO public.session_events (
			receipt_id, session_id, event_type, project_name, current_working_directory,
			source_timestamp_rfc3339, source_timestamp_utc, ingested_at
		) VALUES (
			'$end_receipt', '$session_id', 'session_end', 'fixture-project', '/fixture',
			'$timestamp', TIMESTAMPTZ '$timestamp', TIMESTAMPTZ '$timestamp'
		);
		INSERT INTO public.raw_observations (
			receipt_id, session_id, event_type, hook_type, project_name,
			current_working_directory, source_timestamp_rfc3339, source_timestamp_utc,
			ingested_at, data
		) VALUES (
			'$observation_receipt', '$session_id', 'observation', 'fixture_hook', 'fixture-project',
			'/fixture', '$timestamp', TIMESTAMPTZ '$timestamp', TIMESTAMPTZ '$timestamp', '{}'::json
		)"
}

task_5_4_append_observation() {
	local session_id=$1
	local observation_receipt=$2
	local timestamp=$3

	safe_source_execute "
		INSERT INTO public.raw_observations (
			receipt_id, session_id, event_type, hook_type, project_name,
			current_working_directory, source_timestamp_rfc3339, source_timestamp_utc,
			ingested_at, data
		) VALUES (
			'$observation_receipt', '$session_id', 'observation', 'fixture_hook', 'fixture-project',
			'/fixture', '$timestamp', TIMESTAMPTZ '$timestamp', TIMESTAMPTZ '$timestamp', '{}'::json
		)"
}

task_5_4_insert_prior_record() {
	local session_id=$1
	local source_revision=$2
	local lifecycle_count=$3
	local observation_count=$4
	local source_cutoff=$5
	local transcript=$6
	local summary=$7

	task_5_4_application_execute "
		INSERT INTO public.session_records (
			session_id, source_revision, lifecycle_count, observation_count, source_cutoff,
			transcript, summary_sentences, summary, concepts, generated_at
		) VALUES (
			'$session_id', '$source_revision', $lifecycle_count, $observation_count, TIMESTAMPTZ '$source_cutoff',
			'$transcript'::jsonb, ARRAY['$summary']::text[], '$summary',
			ARRAY['task-5-4-01', 'task-5-4-02', 'task-5-4-03', 'task-5-4-04', 'task-5-4-05', 'task-5-4-06', 'task-5-4-07', 'task-5-4-08', 'task-5-4-09', 'task-5-4-10']::text[],
			TIMESTAMPTZ '$source_cutoff' + INTERVAL '1 second'
		)"
}

task_5_4_migration_execute() {
	if ! migration_query "$1" >/dev/null 2>/dev/null; then
		fail 'task 5.4 migration statement failed'
	fi
}

task_5_4_source_schema_snapshot() {
	migration_query "
		WITH source_columns AS (
			SELECT
				relation.relname || '|' || attribute.attname || '|' ||
				format_type(attribute.atttypid, attribute.atttypmod) || '|' ||
				attribute.attnotnull AS value
			FROM pg_class AS relation
			JOIN pg_namespace AS schema ON schema.oid = relation.relnamespace
			JOIN pg_attribute AS attribute ON attribute.attrelid = relation.oid
			WHERE schema.nspname = 'public'
			  AND relation.relname IN ('session_events', 'raw_observations')
			  AND attribute.attnum > 0
			  AND NOT attribute.attisdropped
		),
		source_constraints AS (
			SELECT
				relation.relname || '|' || table_constraint.conname || '|' ||
				table_constraint.contype::text || '|' || pg_get_constraintdef(table_constraint.oid) AS value
			FROM pg_constraint AS table_constraint
			JOIN pg_class AS relation ON relation.oid = table_constraint.conrelid
			JOIN pg_namespace AS schema ON schema.oid = relation.relnamespace
			WHERE schema.nspname = 'public'
			  AND relation.relname IN ('session_events', 'raw_observations')
		)
		SELECT
			COALESCE((SELECT string_agg(value, E'\\n' ORDER BY value) FROM source_columns), '') ||
			E'\\n--constraints--\\n' ||
			COALESCE((SELECT string_agg(value, E'\\n' ORDER BY value) FROM source_constraints), '')"
}

task_5_4_source_index_snapshot() {
	migration_query "
		SELECT COALESCE(
			string_agg(
				relation.relname || '|' || index_relation.relname || '|' ||
				pg_get_indexdef(table_index.indexrelid),
				E'\\n' ORDER BY relation.relname, index_relation.relname
			),
			''
		)
		FROM pg_index AS table_index
		JOIN pg_class AS relation ON relation.oid = table_index.indrelid
		JOIN pg_class AS index_relation ON index_relation.oid = table_index.indexrelid
		JOIN pg_namespace AS schema ON schema.oid = relation.relnamespace
		WHERE schema.nspname = 'public'
		  AND relation.relname IN ('session_events', 'raw_observations')"
}

task_5_4_stage_attempt() {
	local attempt_id=$1
	local lease_token=$2
	local session_id=$3
	local source_revision=$4
	local source_cutoff=$5
	local transcript=$6
	local generated_at=$7
	local candidates=$8
	local rows
	local summary="Task 5.4 $session_id summary"

	rows="$(task_5_4_stage \
		"$attempt_id" \
		"$lease_token" \
		"$session_id" \
		"$source_revision" \
		"$source_cutoff" \
		"$transcript" \
		"[\"$summary\"]" \
		"$summary" \
		'["task-5-4-01","task-5-4-02","task-5-4-03","task-5-4-04","task-5-4-05","task-5-4-06","task-5-4-07","task-5-4-08","task-5-4-09","task-5-4-10"]' \
		"$generated_at" \
		"$candidates")"
	expect_fixture_equal "$(fixture_line_count "$rows")" '1' 'task 5.4 staged attempt confirmation'
}

task_5_4_staged_progress_snapshot() {
	local attempt_id=$1

	task_5_4_application_query "
		SELECT
			md5(
				source_cutoff::text || E'\\x1f' || transcript::text || E'\\x1f' ||
				summary_sentences::text || E'\\x1f' || summary || E'\\x1f' ||
				concepts::text || E'\\x1f' || generated_at::text
			) || '|' ||
			COALESCE((
				SELECT md5(string_agg(
					ordinal::text || E'\\x1f' || content_fingerprint || E'\\x1f' ||
					memory_id::text || E'\\x1f' || canonical_payload::text || E'\\x1f' ||
					supporting_receipt_ids::text || E'\\x1f' || COALESCE(published_at::text, ''),
					E'\\x1e' ORDER BY ordinal
				))
				FROM public.session_memory_candidates
				WHERE attempt_id = '$attempt_id'
			), '')
		FROM public.session_processing_attempts
		WHERE attempt_id = '$attempt_id'"
}

verify_task_5_4_staging() {
	local source_revision
	local memory_id
	local candidate
	local claim_rows
	local unpublished_rows
	local stale_marker_rows
	local marker_rows
	local first_published_at
	local repeated_marker_rows
	local repeated_published_at

	source_revision="$(source_revision_v5 \
		observation '00000000-0000-4000-8000-000000000401' \
		session_end '00000000-0000-4000-8000-000000000402')"
	memory_id="$(task_5_4_memory_id 'duplicate-fingerprint')"
	candidate="$(task_5_4_candidate \
		'duplicate-fingerprint' \
		"$memory_id" \
		'task-5-4-staging' \
		'00000000-0000-4000-8000-000000000401' \
		'2026-02-02T00:00:00Z')"
	safe_source_execute "
		INSERT INTO public.session_events (
			receipt_id, session_id, event_type, project_name, current_working_directory,
			source_timestamp_rfc3339, source_timestamp_utc, ingested_at
		) VALUES (
			'00000000-0000-4000-8000-000000000402', 'task-5-4-staging', 'session_end', 'fixture-project', '/fixture',
			'2026-02-02T00:00:00Z', TIMESTAMPTZ '2026-02-02 00:00:00+00', TIMESTAMPTZ '2026-02-02 00:00:00+00'
		);
		INSERT INTO public.raw_observations (
			receipt_id, session_id, event_type, hook_type, project_name,
			current_working_directory, source_timestamp_rfc3339, source_timestamp_utc,
			ingested_at, data
		) VALUES (
			'00000000-0000-4000-8000-000000000401', 'task-5-4-staging', 'observation', 'fixture_hook', 'fixture-project',
			'/fixture', '2026-02-02T00:00:00Z', TIMESTAMPTZ '2026-02-02 00:00:00+00', TIMESTAMPTZ '2026-02-02 00:00:00+00', '{}'::json
		)"
	claim_rows="$(task_5_4_claim \
		'task-5-4-staging' \
		"$source_revision" \
		'1' \
		'1' \
		'018f5d00-0000-7000-8000-000000000501' \
		'018f5d00-0000-7000-8000-000000000601' \
		'2026-02-02T00:00:00Z' \
		'2026-02-02T00:05:00Z')"
	expect_fixture_equal "$(fixture_line_count "$claim_rows")" '1' 'task 5.4 staging claim confirmation'
	task_5_4_stage_attempt \
		'018f5d00-0000-7000-8000-000000000501' \
		'018f5d00-0000-7000-8000-000000000601' \
		'task-5-4-staging' \
		"$source_revision" \
		'2026-02-02T00:00:00Z' \
		"$(task_5_4_end_observation_transcript \
			'task-5-4-staging' \
			'00000000-0000-4000-8000-000000000401' \
			'00000000-0000-4000-8000-000000000402' \
			'2026-02-02T00:00:00Z')" \
		'2026-02-02T00:01:00Z' \
		"[$candidate,$candidate]"
	expect_equal \
		"$(task_5_4_application_query "SELECT
			(SELECT state FROM public.session_processing_attempts WHERE attempt_id = '018f5d00-0000-7000-8000-000000000501') || '|' ||
			(SELECT count(*) FROM public.session_memory_candidates WHERE attempt_id = '018f5d00-0000-7000-8000-000000000501') || '|' ||
			(SELECT min(ordinal)::text FROM public.session_memory_candidates WHERE attempt_id = '018f5d00-0000-7000-8000-000000000501') || '|' ||
			(SELECT max(ordinal)::text FROM public.session_memory_candidates WHERE attempt_id = '018f5d00-0000-7000-8000-000000000501') || '|' ||
			(SELECT count(*) FROM public.session_memory_candidates WHERE attempt_id = '018f5d00-0000-7000-8000-000000000501' AND content_fingerprint = 'duplicate-fingerprint' AND memory_id = '$memory_id' AND published_at IS NULL)")" \
		'staged|1|0|0|1' \
		'task 5.4 duplicate fingerprints retain one ordinal-zero unpublished candidate'
	unpublished_rows="$(task_5_4_load_unpublished_candidates \
		'018f5d00-0000-7000-8000-000000000501' \
		'018f5d00-0000-7000-8000-000000000601' \
		'task-5-4-staging' \
		"$source_revision")"
	expect_fixture_equal "$(fixture_line_count "$unpublished_rows")" '1' 'task 5.4 duplicate candidate discovery confirmation'
	expect_equal \
		"$(task_5_4_application_query "SELECT state || '|' || count(*) FILTER (WHERE published_at IS NULL) FROM public.session_processing_attempts AS attempt JOIN public.session_memory_candidates AS candidate USING (attempt_id) WHERE attempt.attempt_id = '018f5d00-0000-7000-8000-000000000501' GROUP BY state")" \
		'publishing|1' \
		'task 5.4 unpublished discovery enters publishing without changing the candidate'
	stale_marker_rows="$(task_5_4_mark_memory_published \
		'018f5d00-0000-7000-8000-000000000501' \
		'018f5d00-0000-7000-8000-000000000602' \
		'task-5-4-staging' \
		"$source_revision" \
		"$memory_id")"
	expect_fixture_equal "$(fixture_line_count "$stale_marker_rows")" '0' 'task 5.4 stale publication token returns no marker'
	expect_equal \
		"$(task_5_4_application_query "SELECT state || '|' || count(*) FILTER (WHERE published_at IS NULL) FROM public.session_processing_attempts AS attempt JOIN public.session_memory_candidates AS candidate USING (attempt_id) WHERE attempt.attempt_id = '018f5d00-0000-7000-8000-000000000501' GROUP BY state")" \
		'publishing|1' \
		'task 5.4 stale publication token leaves the candidate unpublished'
	marker_rows="$(task_5_4_mark_memory_published \
		'018f5d00-0000-7000-8000-000000000501' \
		'018f5d00-0000-7000-8000-000000000601' \
		'task-5-4-staging' \
		"$source_revision" \
		"$memory_id")"
	expect_fixture_equal "$(fixture_line_count "$marker_rows")" '1' 'task 5.4 active publication token confirms one marker'
	first_published_at="$(task_5_4_application_query "SELECT published_at::text FROM public.session_memory_candidates WHERE attempt_id = '018f5d00-0000-7000-8000-000000000501' AND memory_id = '$memory_id'")"
	expect_fixture_not_equal "$first_published_at" '' 'task 5.4 publication marker assigns a timestamp'
	repeated_marker_rows="$(task_5_4_mark_memory_published \
		'018f5d00-0000-7000-8000-000000000501' \
		'018f5d00-0000-7000-8000-000000000601' \
		'task-5-4-staging' \
		"$source_revision" \
		"$memory_id")"
	expect_fixture_equal "$(fixture_line_count "$repeated_marker_rows")" '1' 'task 5.4 repeated publication returns the existing marker'
	repeated_published_at="$(task_5_4_application_query "SELECT published_at::text FROM public.session_memory_candidates WHERE attempt_id = '018f5d00-0000-7000-8000-000000000501' AND memory_id = '$memory_id'")"
	expect_fixture_equal "$repeated_published_at" "$first_published_at" 'task 5.4 repeated publication preserves its timestamp'
}

verify_task_5_4_refresh_suppression() {
	local initial_revision
	local refreshed_revision
	local prior_memory_id
	local new_memory_id
	local prior_candidate
	local new_candidate
	local initial_claim_rows
	local initial_load_rows
	local initial_marker_rows
	local initial_promotion_rows
	local prior_published_at
	local refresh_claim_rows
	local refresh_load_rows
	local refresh_marker_rows
	local refresh_promotion_rows

	initial_revision="$(source_revision_v5 \
		observation '00000000-0000-4000-8000-000000000403' \
		session_end '00000000-0000-4000-8000-000000000404')"
	refreshed_revision="$(source_revision_v5 \
		observation '00000000-0000-4000-8000-000000000403' \
		observation '00000000-0000-4000-8000-000000000405' \
		session_end '00000000-0000-4000-8000-000000000404')"
	prior_memory_id="$(task_5_4_memory_id 'refresh-prior-fingerprint')"
	new_memory_id="$(task_5_4_memory_id 'refresh-new-fingerprint')"
	prior_candidate="$(task_5_4_candidate \
		'refresh-prior-fingerprint' \
		"$prior_memory_id" \
		'task-5-4-refresh' \
		'00000000-0000-4000-8000-000000000403' \
		'2026-02-03T00:00:00Z')"
	new_candidate="$(task_5_4_candidate \
		'refresh-new-fingerprint' \
		"$new_memory_id" \
		'task-5-4-refresh' \
		'00000000-0000-4000-8000-000000000405' \
		'2026-02-03T00:01:00Z')"
	task_5_4_seed_ended_source \
		'task-5-4-refresh' \
		'00000000-0000-4000-8000-000000000403' \
		'00000000-0000-4000-8000-000000000404' \
		'2026-02-03T00:00:00Z'
	initial_claim_rows="$(task_5_4_claim \
		'task-5-4-refresh' \
		"$initial_revision" \
		'1' \
		'1' \
		'018f5d00-0000-7000-8000-000000000502' \
		'018f5d00-0000-7000-8000-000000000603' \
		'2026-02-03T00:00:00Z' \
		'2026-02-03T00:05:00Z')"
	expect_fixture_equal "$(fixture_line_count "$initial_claim_rows")" '1' 'task 5.4 initial refresh claim confirmation'
	task_5_4_stage_attempt \
		'018f5d00-0000-7000-8000-000000000502' \
		'018f5d00-0000-7000-8000-000000000603' \
		'task-5-4-refresh' \
		"$initial_revision" \
		'2026-02-03T00:00:00Z' \
		"$(task_5_4_end_observation_transcript \
			'task-5-4-refresh' \
			'00000000-0000-4000-8000-000000000403' \
			'00000000-0000-4000-8000-000000000404' \
			'2026-02-03T00:00:00Z')" \
		'2026-02-03T00:01:00Z' \
		"[$prior_candidate]"
	initial_load_rows="$(task_5_4_load_unpublished_candidates \
		'018f5d00-0000-7000-8000-000000000502' \
		'018f5d00-0000-7000-8000-000000000603' \
		'task-5-4-refresh' \
		"$initial_revision")"
	expect_fixture_equal "$(fixture_line_count "$initial_load_rows")" '1' 'task 5.4 initial refresh candidate discovery confirmation'
	initial_marker_rows="$(task_5_4_mark_memory_published \
		'018f5d00-0000-7000-8000-000000000502' \
		'018f5d00-0000-7000-8000-000000000603' \
		'task-5-4-refresh' \
		"$initial_revision" \
		"$prior_memory_id")"
	expect_fixture_equal "$(fixture_line_count "$initial_marker_rows")" '1' 'task 5.4 initial refresh publication confirmation'
	initial_promotion_rows="$(task_5_4_promote \
		'018f5d00-0000-7000-8000-000000000502' \
		'018f5d00-0000-7000-8000-000000000603' \
		'task-5-4-refresh' \
		"$initial_revision")"
	expect_fixture_equal "$(fixture_line_count "$initial_promotion_rows")" '1' 'task 5.4 initial refresh promotion confirmation'
	prior_published_at="$(task_5_4_application_query "SELECT published_at::text FROM public.session_memory_candidates WHERE attempt_id = '018f5d00-0000-7000-8000-000000000502' AND memory_id = '$prior_memory_id'")"
	expect_fixture_not_equal "$prior_published_at" '' 'task 5.4 prior refresh memory is published before the late source'
	task_5_4_append_observation \
		'task-5-4-refresh' \
		'00000000-0000-4000-8000-000000000405' \
		'2026-02-03T00:01:00Z'
	refresh_claim_rows="$(task_5_4_claim \
		'task-5-4-refresh' \
		"$refreshed_revision" \
		'1' \
		'2' \
		'018f5d00-0000-7000-8000-000000000503' \
		'018f5d00-0000-7000-8000-000000000604' \
		'2026-02-03T00:02:00Z' \
		'2026-02-03T00:07:00Z')"
	expect_fixture_equal "$(fixture_line_count "$refresh_claim_rows")" '1' 'task 5.4 late-source refresh claim confirmation'
	task_5_4_stage_attempt \
		'018f5d00-0000-7000-8000-000000000503' \
		'018f5d00-0000-7000-8000-000000000604' \
		'task-5-4-refresh' \
		"$refreshed_revision" \
		'2026-02-03T00:01:00Z' \
		"$(task_5_4_two_observation_end_transcript \
			'task-5-4-refresh' \
			'00000000-0000-4000-8000-000000000403' \
			'00000000-0000-4000-8000-000000000405' \
			'00000000-0000-4000-8000-000000000404' \
			'2026-02-03T00:01:00Z')" \
		'2026-02-03T00:02:00Z' \
		"[$prior_candidate,$new_candidate]"
	expect_equal \
		"$(task_5_4_application_query "SELECT
			(SELECT count(*) FROM public.session_memory_candidates WHERE session_id = 'task-5-4-refresh' AND content_fingerprint = 'refresh-prior-fingerprint') || '|' ||
			(SELECT count(*) FROM public.session_memory_candidates WHERE attempt_id = '018f5d00-0000-7000-8000-000000000503' AND content_fingerprint = 'refresh-new-fingerprint' AND ordinal = 1 AND memory_id = '$new_memory_id' AND published_at IS NULL) || '|' ||
			(SELECT published_at::text FROM public.session_memory_candidates WHERE attempt_id = '018f5d00-0000-7000-8000-000000000502' AND memory_id = '$prior_memory_id')")" \
		"1|1|$prior_published_at" \
		'task 5.4 late refresh suppresses the exact prior fingerprint and inserts only the new ordinal'
	refresh_load_rows="$(task_5_4_load_unpublished_candidates \
		'018f5d00-0000-7000-8000-000000000503' \
		'018f5d00-0000-7000-8000-000000000604' \
		'task-5-4-refresh' \
		"$refreshed_revision")"
	expect_fixture_equal "$(fixture_line_count "$refresh_load_rows")" '1' 'task 5.4 late refresh discovers only the new candidate'
	refresh_marker_rows="$(task_5_4_mark_memory_published \
		'018f5d00-0000-7000-8000-000000000503' \
		'018f5d00-0000-7000-8000-000000000604' \
		'task-5-4-refresh' \
		"$refreshed_revision" \
		"$new_memory_id")"
	expect_fixture_equal "$(fixture_line_count "$refresh_marker_rows")" '1' 'task 5.4 late refresh new candidate publication confirmation'
	refresh_promotion_rows="$(task_5_4_promote \
		'018f5d00-0000-7000-8000-000000000503' \
		'018f5d00-0000-7000-8000-000000000604' \
		'task-5-4-refresh' \
		"$refreshed_revision")"
	expect_fixture_equal "$(fixture_line_count "$refresh_promotion_rows")" '1' 'task 5.4 late refresh promotion confirmation'
	expect_equal \
		"$(task_5_4_application_query "SELECT
			(SELECT count(*) FROM public.session_records WHERE session_id = 'task-5-4-refresh') || '|' ||
			(SELECT source_revision::text FROM public.session_records WHERE session_id = 'task-5-4-refresh') || '|' ||
			(SELECT lifecycle_count::text || '|' || observation_count::text FROM public.session_records WHERE session_id = 'task-5-4-refresh') || '|' ||
			(SELECT 'event_derived' FROM public.session_records WHERE session_id = 'task-5-4-refresh') || '|' ||
			(SELECT count(*) FROM public.session_processing_attempts WHERE session_id = 'task-5-4-refresh' AND state = 'complete')")" \
		"1|$refreshed_revision|1|2|event_derived|2" \
		'task 5.4 refresh replaces one current record only after all publication'
}

verify_task_5_4_retry_reclaim() {
	local source_revision
	local memory_id
	local candidate
	local claim_rows
	local stale_retry_rows
	local retry_rows
	local before_due_rows
	local due_rows
	local staged_progress

	source_revision="$(source_revision_v5 \
		observation '00000000-0000-4000-8000-000000000406' \
		session_end '00000000-0000-4000-8000-000000000407')"
	memory_id="$(task_5_4_memory_id 'retry-fingerprint')"
	candidate="$(task_5_4_candidate \
		'retry-fingerprint' \
		"$memory_id" \
		'task-5-4-retry' \
		'00000000-0000-4000-8000-000000000406' \
		'2026-02-04T00:00:00Z')"
	task_5_4_seed_ended_source \
		'task-5-4-retry' \
		'00000000-0000-4000-8000-000000000406' \
		'00000000-0000-4000-8000-000000000407' \
		'2026-02-04T00:00:00Z'
	claim_rows="$(task_5_4_claim \
		'task-5-4-retry' \
		"$source_revision" \
		'1' \
		'1' \
		'018f5d00-0000-7000-8000-000000000504' \
		'018f5d00-0000-7000-8000-000000000605' \
		'2026-02-04T00:00:00Z' \
		'2026-02-04T00:05:00Z')"
	expect_fixture_equal "$(fixture_line_count "$claim_rows")" '1' 'task 5.4 retry claim confirmation'
	task_5_4_stage_attempt \
		'018f5d00-0000-7000-8000-000000000504' \
		'018f5d00-0000-7000-8000-000000000605' \
		'task-5-4-retry' \
		"$source_revision" \
		'2026-02-04T00:00:00Z' \
		"$(task_5_4_end_observation_transcript \
			'task-5-4-retry' \
			'00000000-0000-4000-8000-000000000406' \
			'00000000-0000-4000-8000-000000000407' \
			'2026-02-04T00:00:00Z')" \
		'2026-02-04T00:00:01Z' \
		"[$candidate]"
	staged_progress="$(task_5_4_staged_progress_snapshot '018f5d00-0000-7000-8000-000000000504')"
	stale_retry_rows="$(task_5_4_retry \
		'018f5d00-0000-7000-8000-000000000504' \
		'018f5d00-0000-7000-8000-000000000606' \
		'task-5-4-retry' \
		"$source_revision" \
		'provider_transient' \
		'2026-02-04T00:01:00Z')"
	expect_fixture_equal "$(fixture_line_count "$stale_retry_rows")" '0' 'task 5.4 stale retry token returns no marker'
	retry_rows="$(task_5_4_retry \
		'018f5d00-0000-7000-8000-000000000504' \
		'018f5d00-0000-7000-8000-000000000605' \
		'task-5-4-retry' \
		"$source_revision" \
		'provider_transient' \
		'2026-02-04T00:01:00Z')"
	expect_fixture_equal "$(fixture_line_count "$retry_rows")" '1' 'task 5.4 active retry token persists one marker'
	expect_equal \
		"$(task_5_4_application_query "SELECT
			state || '|' || failure_category || '|' ||
			(next_attempt_at = TIMESTAMPTZ '2026-02-04 00:01:00+00') || '|' ||
			(source_cutoff IS NOT NULL AND transcript IS NOT NULL AND summary_sentences IS NOT NULL AND summary IS NOT NULL AND concepts IS NOT NULL AND generated_at IS NOT NULL) || '|' ||
			(SELECT count(*) FROM public.session_memory_candidates WHERE attempt_id = attempt.attempt_id AND memory_id = '$memory_id' AND published_at IS NULL)
		FROM public.session_processing_attempts AS attempt
		WHERE attempt.attempt_id = '018f5d00-0000-7000-8000-000000000504'")" \
		'retryable|provider_transient|true|true|1' \
		'task 5.4 retry preserves staged output and unpublished candidate progress'
	expect_fixture_equal \
		"$(task_5_4_staged_progress_snapshot '018f5d00-0000-7000-8000-000000000504')" \
		"$staged_progress" \
		'task 5.4 retry preserves the exact staged projection and candidate payloads'
	before_due_rows="$(task_5_4_claim \
		'task-5-4-retry' \
		"$source_revision" \
		'1' \
		'1' \
		'018f5d00-0000-7000-8000-000000000513' \
		'018f5d00-0000-7000-8000-000000000607' \
		'2026-02-04T00:00:59Z' \
		'2026-02-04T00:05:59Z')"
	expect_fixture_equal "$(fixture_line_count "$before_due_rows")" '0' 'task 5.4 retry does not reclaim before the exact due time'
	due_rows="$(task_5_4_claim \
		'task-5-4-retry' \
		"$source_revision" \
		'1' \
		'1' \
		'018f5d00-0000-7000-8000-000000000514' \
		'018f5d00-0000-7000-8000-000000000608' \
		'2026-02-04T00:01:00Z' \
		'2026-02-04T00:06:00Z')"
	expect_fixture_equal "$(fixture_line_count "$due_rows")" '1' 'task 5.4 retry reclaims at the exact due time'
	expect_equal \
		"$(task_5_4_application_query "SELECT
			attempt_id::text || '|' || state || '|' || lease_token::text || '|' ||
			(next_attempt_at IS NULL) || '|' || failure_category || '|' ||
			(source_cutoff IS NOT NULL AND transcript IS NOT NULL AND summary_sentences IS NOT NULL AND summary IS NOT NULL AND concepts IS NOT NULL AND generated_at IS NOT NULL) || '|' ||
			(SELECT count(*) FROM public.session_memory_candidates WHERE attempt_id = attempt.attempt_id AND memory_id = '$memory_id' AND published_at IS NULL)
		FROM public.session_processing_attempts AS attempt
		WHERE attempt.attempt_id = '018f5d00-0000-7000-8000-000000000504'")" \
		'018f5d00-0000-7000-8000-000000000504|claimed|018f5d00-0000-7000-8000-000000000608|true|provider_transient|true|1' \
		'task 5.4 exact-due reclaim retains progress and fences with a new token'
	expect_fixture_equal \
		"$(task_5_4_staged_progress_snapshot '018f5d00-0000-7000-8000-000000000504')" \
		"$staged_progress" \
		'task 5.4 exact-due reclaim preserves the exact staged projection and candidate payloads'
}

verify_task_5_4_fenced_promotion() {
	local prior_revision
	local source_revision
	local memory_id
	local candidate
	local claim_rows
	local prior_record_snapshot
	local pending_promotion_rows
	local load_rows
	local active_pending_promotion_rows
	local stale_promotion_rows
	local marker_rows
	local promotion_rows
	local repeated_promotion_rows

	prior_revision="$(source_revision_v5 \
		session_end '00000000-0000-4000-8000-000000000409')"
	source_revision="$(source_revision_v5 \
		observation '00000000-0000-4000-8000-000000000408' \
		session_end '00000000-0000-4000-8000-000000000409')"
	memory_id="$(task_5_4_memory_id 'promotion-fingerprint')"
	candidate="$(task_5_4_candidate \
		'promotion-fingerprint' \
		"$memory_id" \
		'task-5-4-promotion' \
		'00000000-0000-4000-8000-000000000408' \
		'2026-02-05T00:00:00Z')"
	task_5_4_insert_prior_record \
		'task-5-4-promotion' \
		"$prior_revision" \
		'1' \
		'0' \
		'2026-02-04T00:00:00Z' \
		"$(task_5_4_lifecycle_transcript \
			'task-5-4-promotion' \
			'00000000-0000-4000-8000-000000000409' \
			'2026-02-04T00:00:00Z')" \
		'task 5.4 prior promotion summary'
	prior_record_snapshot="$(task_5_4_application_query "SELECT row_to_json(record)::text FROM public.session_records AS record WHERE session_id = 'task-5-4-promotion'")"
	expect_fixture_not_equal "$prior_record_snapshot" '' 'task 5.4 prior promotion record snapshot exists'
	task_5_4_seed_ended_source \
		'task-5-4-promotion' \
		'00000000-0000-4000-8000-000000000408' \
		'00000000-0000-4000-8000-000000000409' \
		'2026-02-05T00:00:00Z'
	claim_rows="$(task_5_4_claim \
		'task-5-4-promotion' \
		"$source_revision" \
		'1' \
		'1' \
		'018f5d00-0000-7000-8000-000000000505' \
		'018f5d00-0000-7000-8000-000000000609' \
		'2026-02-05T00:00:00Z' \
		'2026-02-05T00:05:00Z')"
	expect_fixture_equal "$(fixture_line_count "$claim_rows")" '1' 'task 5.4 promotion claim confirmation'
	task_5_4_stage_attempt \
		'018f5d00-0000-7000-8000-000000000505' \
		'018f5d00-0000-7000-8000-000000000609' \
		'task-5-4-promotion' \
		"$source_revision" \
		'2026-02-05T00:00:00Z' \
		"$(task_5_4_end_observation_transcript \
			'task-5-4-promotion' \
			'00000000-0000-4000-8000-000000000408' \
			'00000000-0000-4000-8000-000000000409' \
			'2026-02-05T00:00:00Z')" \
		'2026-02-05T00:01:00Z' \
		"[$candidate]"
	pending_promotion_rows="$(task_5_4_promote \
		'018f5d00-0000-7000-8000-000000000505' \
		'018f5d00-0000-7000-8000-000000000609' \
		'task-5-4-promotion' \
		"$source_revision")"
	expect_fixture_equal "$(fixture_line_count "$pending_promotion_rows")" '0' 'task 5.4 staged promotion cannot replace the prior current record'
	expect_equal \
		"$(task_5_4_application_query "SELECT count(*) || '|' || source_revision::text || '|' || summary FROM public.session_records WHERE session_id = 'task-5-4-promotion' GROUP BY source_revision, summary")" \
		"1|$prior_revision|task 5.4 prior promotion summary" \
		'task 5.4 pending promotion leaves the prior current record unchanged'
	load_rows="$(task_5_4_load_unpublished_candidates \
		'018f5d00-0000-7000-8000-000000000505' \
		'018f5d00-0000-7000-8000-000000000609' \
		'task-5-4-promotion' \
		"$source_revision")"
	expect_fixture_equal "$(fixture_line_count "$load_rows")" '1' 'task 5.4 promotion candidate discovery confirmation'
	active_pending_promotion_rows="$(task_5_4_promote \
		'018f5d00-0000-7000-8000-000000000505' \
		'018f5d00-0000-7000-8000-000000000609' \
		'task-5-4-promotion' \
		"$source_revision")"
	expect_fixture_equal "$(fixture_line_count "$active_pending_promotion_rows")" '0' 'task 5.4 active promotion waits for every candidate publication'
	expect_equal \
		"$(task_5_4_application_query "SELECT
			(SELECT state FROM public.session_processing_attempts WHERE attempt_id = '018f5d00-0000-7000-8000-000000000505') || '|' ||
			(SELECT (published_at IS NULL)::text FROM public.session_memory_candidates WHERE attempt_id = '018f5d00-0000-7000-8000-000000000505' AND memory_id = '$memory_id')")" \
		'publishing|true' \
		'task 5.4 active pending promotion leaves publishing state and candidate marker unchanged'
	expect_fixture_equal \
		"$(task_5_4_application_query "SELECT row_to_json(record)::text FROM public.session_records AS record WHERE session_id = 'task-5-4-promotion'")" \
		"$prior_record_snapshot" \
		'task 5.4 active pending promotion leaves the prior current record unchanged'
	stale_promotion_rows="$(task_5_4_promote \
		'018f5d00-0000-7000-8000-000000000505' \
		'018f5d00-0000-7000-8000-000000000610' \
		'task-5-4-promotion' \
		"$source_revision")"
	expect_fixture_equal "$(fixture_line_count "$stale_promotion_rows")" '0' 'task 5.4 stale promotion token returns no row'
	marker_rows="$(task_5_4_mark_memory_published \
		'018f5d00-0000-7000-8000-000000000505' \
		'018f5d00-0000-7000-8000-000000000609' \
		'task-5-4-promotion' \
		"$source_revision" \
		"$memory_id")"
	expect_fixture_equal "$(fixture_line_count "$marker_rows")" '1' 'task 5.4 promotion candidate publication confirmation'
	stale_promotion_rows="$(task_5_4_promote \
		'018f5d00-0000-7000-8000-000000000505' \
		'018f5d00-0000-7000-8000-000000000610' \
		'task-5-4-promotion' \
		"$source_revision")"
	expect_fixture_equal "$(fixture_line_count "$stale_promotion_rows")" '0' 'task 5.4 stale fenced promotion cannot replace the prior record'
	promotion_rows="$(task_5_4_promote \
		'018f5d00-0000-7000-8000-000000000505' \
		'018f5d00-0000-7000-8000-000000000609' \
		'task-5-4-promotion' \
		"$source_revision")"
	expect_fixture_equal "$(fixture_line_count "$promotion_rows")" '1' 'task 5.4 active fenced promotion confirms one replacement'
	repeated_promotion_rows="$(task_5_4_promote \
		'018f5d00-0000-7000-8000-000000000505' \
		'018f5d00-0000-7000-8000-000000000609' \
		'task-5-4-promotion' \
		"$source_revision")"
	expect_fixture_equal "$(fixture_line_count "$repeated_promotion_rows")" '0' 'task 5.4 repeated promotion returns no row'
	expect_equal \
		"$(task_5_4_application_query "SELECT
			(SELECT count(*) FROM public.session_records WHERE session_id = 'task-5-4-promotion') || '|' ||
			(SELECT source_revision::text FROM public.session_records WHERE session_id = 'task-5-4-promotion') || '|' ||
			(SELECT lifecycle_count::text || '|' || observation_count::text FROM public.session_records WHERE session_id = 'task-5-4-promotion') || '|' ||
			(SELECT 'event_derived' FROM public.session_records WHERE session_id = 'task-5-4-promotion') || '|' ||
			(SELECT count(*) FROM public.session_processing_attempts WHERE attempt_id = '018f5d00-0000-7000-8000-000000000505' AND state = 'complete')")" \
		"1|$source_revision|1|1|event_derived|1" \
		'task 5.4 valid fenced promotion replaces exactly one current record'
}

verify_task_5_4_failed_refresh_preservation() {
	local initial_revision
	local refreshed_revision
	local prior_memory_id
	local retry_memory_id
	local prior_candidate
	local retry_candidate
	local initial_claim_rows
	local initial_load_rows
	local initial_marker_rows
	local initial_promotion_rows
	local prior_published_at
	local refresh_claim_rows
	local retry_rows

	initial_revision="$(source_revision_v5 \
		observation '00000000-0000-4000-8000-000000000410' \
		session_end '00000000-0000-4000-8000-000000000411')"
	refreshed_revision="$(source_revision_v5 \
		observation '00000000-0000-4000-8000-000000000410' \
		observation '00000000-0000-4000-8000-000000000412' \
		session_end '00000000-0000-4000-8000-000000000411')"
	prior_memory_id="$(task_5_4_memory_id 'failed-refresh-prior-fingerprint')"
	retry_memory_id="$(task_5_4_memory_id 'failed-refresh-new-fingerprint')"
	prior_candidate="$(task_5_4_candidate \
		'failed-refresh-prior-fingerprint' \
		"$prior_memory_id" \
		'task-5-4-failed-refresh' \
		'00000000-0000-4000-8000-000000000410' \
		'2026-02-06T00:00:00Z')"
	retry_candidate="$(task_5_4_candidate \
		'failed-refresh-new-fingerprint' \
		"$retry_memory_id" \
		'task-5-4-failed-refresh' \
		'00000000-0000-4000-8000-000000000412' \
		'2026-02-06T00:01:00Z')"
	task_5_4_seed_ended_source \
		'task-5-4-failed-refresh' \
		'00000000-0000-4000-8000-000000000410' \
		'00000000-0000-4000-8000-000000000411' \
		'2026-02-06T00:00:00Z'
	initial_claim_rows="$(task_5_4_claim \
		'task-5-4-failed-refresh' \
		"$initial_revision" \
		'1' \
		'1' \
		'018f5d00-0000-7000-8000-000000000506' \
		'018f5d00-0000-7000-8000-000000000611' \
		'2026-02-06T00:00:00Z' \
		'2026-02-06T00:05:00Z')"
	expect_fixture_equal "$(fixture_line_count "$initial_claim_rows")" '1' 'task 5.4 failed-refresh initial claim confirmation'
	task_5_4_stage_attempt \
		'018f5d00-0000-7000-8000-000000000506' \
		'018f5d00-0000-7000-8000-000000000611' \
		'task-5-4-failed-refresh' \
		"$initial_revision" \
		'2026-02-06T00:00:00Z' \
		"$(task_5_4_end_observation_transcript \
			'task-5-4-failed-refresh' \
			'00000000-0000-4000-8000-000000000410' \
			'00000000-0000-4000-8000-000000000411' \
			'2026-02-06T00:00:00Z')" \
		'2026-02-06T00:01:00Z' \
		"[$prior_candidate]"
	initial_load_rows="$(task_5_4_load_unpublished_candidates \
		'018f5d00-0000-7000-8000-000000000506' \
		'018f5d00-0000-7000-8000-000000000611' \
		'task-5-4-failed-refresh' \
		"$initial_revision")"
	expect_fixture_equal "$(fixture_line_count "$initial_load_rows")" '1' 'task 5.4 failed-refresh initial candidate discovery confirmation'
	initial_marker_rows="$(task_5_4_mark_memory_published \
		'018f5d00-0000-7000-8000-000000000506' \
		'018f5d00-0000-7000-8000-000000000611' \
		'task-5-4-failed-refresh' \
		"$initial_revision" \
		"$prior_memory_id")"
	expect_fixture_equal "$(fixture_line_count "$initial_marker_rows")" '1' 'task 5.4 failed-refresh initial publication confirmation'
	initial_promotion_rows="$(task_5_4_promote \
		'018f5d00-0000-7000-8000-000000000506' \
		'018f5d00-0000-7000-8000-000000000611' \
		'task-5-4-failed-refresh' \
		"$initial_revision")"
	expect_fixture_equal "$(fixture_line_count "$initial_promotion_rows")" '1' 'task 5.4 failed-refresh initial promotion confirmation'
	prior_published_at="$(task_5_4_application_query "SELECT published_at::text FROM public.session_memory_candidates WHERE attempt_id = '018f5d00-0000-7000-8000-000000000506' AND memory_id = '$prior_memory_id'")"
	expect_fixture_not_equal "$prior_published_at" '' 'task 5.4 failed-refresh prior memory is published'
	task_5_4_append_observation \
		'task-5-4-failed-refresh' \
		'00000000-0000-4000-8000-000000000412' \
		'2026-02-06T00:01:00Z'
	refresh_claim_rows="$(task_5_4_claim \
		'task-5-4-failed-refresh' \
		"$refreshed_revision" \
		'1' \
		'2' \
		'018f5d00-0000-7000-8000-000000000507' \
		'018f5d00-0000-7000-8000-000000000612' \
		'2026-02-06T00:02:00Z' \
		'2026-02-06T00:07:00Z')"
	expect_fixture_equal "$(fixture_line_count "$refresh_claim_rows")" '1' 'task 5.4 failed-refresh late-source claim confirmation'
	task_5_4_stage_attempt \
		'018f5d00-0000-7000-8000-000000000507' \
		'018f5d00-0000-7000-8000-000000000612' \
		'task-5-4-failed-refresh' \
		"$refreshed_revision" \
		'2026-02-06T00:01:00Z' \
		"$(task_5_4_two_observation_end_transcript \
			'task-5-4-failed-refresh' \
			'00000000-0000-4000-8000-000000000410' \
			'00000000-0000-4000-8000-000000000412' \
			'00000000-0000-4000-8000-000000000411' \
			'2026-02-06T00:01:00Z')" \
		'2026-02-06T00:02:00Z' \
		"[$retry_candidate]"
	retry_rows="$(task_5_4_retry \
		'018f5d00-0000-7000-8000-000000000507' \
		'018f5d00-0000-7000-8000-000000000612' \
		'task-5-4-failed-refresh' \
		"$refreshed_revision" \
		'memory_mismatch' \
		'2026-02-06T00:05:00Z')"
	expect_fixture_equal "$(fixture_line_count "$retry_rows")" '1' 'task 5.4 failed-refresh retry marker confirmation'
	expect_equal \
		"$(task_5_4_application_query "SELECT
			(SELECT count(*) FROM public.session_records WHERE session_id = 'task-5-4-failed-refresh') || '|' ||
			(SELECT source_revision::text FROM public.session_records WHERE session_id = 'task-5-4-failed-refresh') || '|' ||
			(SELECT 'event_derived' FROM public.session_records WHERE session_id = 'task-5-4-failed-refresh') || '|' ||
			(SELECT published_at::text FROM public.session_memory_candidates WHERE attempt_id = '018f5d00-0000-7000-8000-000000000506' AND memory_id = '$prior_memory_id') || '|' ||
			(SELECT state || '|' || failure_category || '|' || (next_attempt_at = TIMESTAMPTZ '2026-02-06 00:05:00+00') FROM public.session_processing_attempts WHERE attempt_id = '018f5d00-0000-7000-8000-000000000507') || '|' ||
			(SELECT count(*) FROM public.session_memory_candidates WHERE attempt_id = '018f5d00-0000-7000-8000-000000000507' AND memory_id = '$retry_memory_id' AND published_at IS NULL)")" \
		"1|$initial_revision|event_derived|$prior_published_at|retryable|memory_mismatch|true|1" \
		'task 5.4 failed refresh retains the prior record and published memory while the new revision is retryable'
}

verify_task_5_4_quiet_period_replacement() {
	local initial_revision
	local refreshed_revision
	local initial_claim_rows
	local initial_load_rows
	local initial_promotion_rows
	local before_boundary_discovery
	local at_boundary_discovery
	local refresh_claim_rows
	local refresh_load_rows
	local refresh_promotion_rows

	initial_revision="$(source_revision_v5 \
		observation '00000000-0000-4000-8000-000000000413')"
	refreshed_revision="$(source_revision_v5 \
		observation '00000000-0000-4000-8000-000000000413' \
		observation '00000000-0000-4000-8000-000000000414')"
	task_5_4_append_observation \
		'task-5-4-quiet' \
		'00000000-0000-4000-8000-000000000413' \
		'2026-02-07T00:00:00Z'
	initial_claim_rows="$(task_5_4_claim \
		'task-5-4-quiet' \
		"$initial_revision" \
		'0' \
		'1' \
		'018f5d00-0000-7000-8000-000000000508' \
		'018f5d00-0000-7000-8000-000000000613' \
		'2026-02-08T00:00:00Z' \
		'2026-02-08T00:05:00Z')"
	expect_fixture_equal "$(fixture_line_count "$initial_claim_rows")" '1' 'task 5.4 quiet initial claim confirmation'
	task_5_4_stage_attempt \
		'018f5d00-0000-7000-8000-000000000508' \
		'018f5d00-0000-7000-8000-000000000613' \
		'task-5-4-quiet' \
		"$initial_revision" \
		'2026-02-07T00:00:00Z' \
		"$(task_5_4_observation_transcript \
			'task-5-4-quiet' \
			'00000000-0000-4000-8000-000000000413' \
			'2026-02-07T00:00:00Z')" \
		'2026-02-08T00:00:00Z' \
		'[]'
	initial_load_rows="$(task_5_4_load_unpublished_candidates \
		'018f5d00-0000-7000-8000-000000000508' \
		'018f5d00-0000-7000-8000-000000000613' \
		'task-5-4-quiet' \
		"$initial_revision")"
	expect_fixture_equal "$(fixture_line_count "$initial_load_rows")" '1' 'task 5.4 quiet zero-candidate discovery confirmation'
	initial_promotion_rows="$(task_5_4_promote \
		'018f5d00-0000-7000-8000-000000000508' \
		'018f5d00-0000-7000-8000-000000000613' \
		'task-5-4-quiet' \
		"$initial_revision")"
	expect_fixture_equal "$(fixture_line_count "$initial_promotion_rows")" '1' 'task 5.4 quiet initial promotion confirmation'
	task_5_4_append_observation \
		'task-5-4-quiet' \
		'00000000-0000-4000-8000-000000000414' \
		'2026-02-09T00:00:00Z'
	before_boundary_discovery="$(task_5_4_discovery_at '2026-02-09T23:59:59Z')"
	expect_fixture_not_contains \
		"$before_boundary_discovery" \
		'task-5-4-quiet' \
		'task 5.4 late unended session remains excluded before the quiet-period boundary'
	at_boundary_discovery="$(task_5_4_discovery_at '2026-02-10T00:00:00Z')"
	expect_fixture_contains \
		"$at_boundary_discovery" \
		'task-5-4-quiet' \
		'task 5.4 late unended session becomes eligible at the inclusive quiet-period boundary'
	refresh_claim_rows="$(task_5_4_claim \
		'task-5-4-quiet' \
		"$refreshed_revision" \
		'0' \
		'2' \
		'018f5d00-0000-7000-8000-000000000509' \
		'018f5d00-0000-7000-8000-000000000614' \
		'2026-02-10T00:00:00Z' \
		'2026-02-10T00:05:00Z')"
	expect_fixture_equal "$(fixture_line_count "$refresh_claim_rows")" '1' 'task 5.4 quiet boundary refresh claim confirmation'
	task_5_4_stage_attempt \
		'018f5d00-0000-7000-8000-000000000509' \
		'018f5d00-0000-7000-8000-000000000614' \
		'task-5-4-quiet' \
		"$refreshed_revision" \
		'2026-02-09T00:00:00Z' \
		"$(task_5_4_two_observation_transcript \
			'task-5-4-quiet' \
			'00000000-0000-4000-8000-000000000413' \
			'00000000-0000-4000-8000-000000000414' \
			'2026-02-09T00:00:00Z')" \
		'2026-02-10T00:00:00Z' \
		'[]'
	refresh_load_rows="$(task_5_4_load_unpublished_candidates \
		'018f5d00-0000-7000-8000-000000000509' \
		'018f5d00-0000-7000-8000-000000000614' \
		'task-5-4-quiet' \
		"$refreshed_revision")"
	expect_fixture_equal "$(fixture_line_count "$refresh_load_rows")" '1' 'task 5.4 quiet refresh zero-candidate discovery confirmation'
	refresh_promotion_rows="$(task_5_4_promote \
		'018f5d00-0000-7000-8000-000000000509' \
		'018f5d00-0000-7000-8000-000000000614' \
		'task-5-4-quiet' \
		"$refreshed_revision")"
	expect_fixture_equal "$(fixture_line_count "$refresh_promotion_rows")" '1' 'task 5.4 quiet boundary promotion confirmation'
	expect_equal \
		"$(task_5_4_application_query "SELECT
			(SELECT count(*) FROM public.session_records WHERE session_id = 'task-5-4-quiet') || '|' ||
			(SELECT source_revision::text FROM public.session_records WHERE session_id = 'task-5-4-quiet') || '|' ||
			(SELECT lifecycle_count::text || '|' || observation_count::text FROM public.session_records WHERE session_id = 'task-5-4-quiet') || '|' ||
			(SELECT 'event_derived' FROM public.session_records WHERE session_id = 'task-5-4-quiet') || '|' ||
			(SELECT count(*) FROM public.session_memory_candidates WHERE session_id = 'task-5-4-quiet')")" \
		"1|$refreshed_revision|0|2|event_derived|0" \
		'task 5.4 quiet boundary replaces exactly one current record with zero candidates'
}

verify_task_5_4_transaction_rollbacks() {
	local stage_revision
	local marker_revision
	local promotion_revision
	local stage_memory_id
	local marker_memory_id
	local stage_candidate
	local marker_candidate
	local stage_claim_rows
	local marker_claim_rows
	local promotion_claim_rows
	local promotion_load_rows

	stage_revision="$(source_revision_v5 \
		observation '00000000-0000-4000-8000-000000000415' \
		session_end '00000000-0000-4000-8000-000000000416')"
	marker_revision="$(source_revision_v5 \
		observation '00000000-0000-4000-8000-000000000417' \
		session_end '00000000-0000-4000-8000-000000000418')"
	promotion_revision="$(source_revision_v5 \
		observation '00000000-0000-4000-8000-000000000419' \
		session_end '00000000-0000-4000-8000-000000000420')"
	stage_memory_id="$(task_5_4_memory_id 'rollback-stage-fingerprint')"
	marker_memory_id="$(task_5_4_memory_id 'rollback-marker-fingerprint')"
	stage_candidate="$(task_5_4_candidate \
		'rollback-stage-fingerprint' \
		"$stage_memory_id" \
		'task-5-4-rollback-stage' \
		'00000000-0000-4000-8000-000000000415' \
		'2026-02-11T00:00:00Z')"
	marker_candidate="$(task_5_4_candidate \
		'rollback-marker-fingerprint' \
		"$marker_memory_id" \
		'task-5-4-rollback-marker' \
		'00000000-0000-4000-8000-000000000417' \
		'2026-02-12T00:00:00Z')"
	task_5_4_seed_ended_source \
		'task-5-4-rollback-stage' \
		'00000000-0000-4000-8000-000000000415' \
		'00000000-0000-4000-8000-000000000416' \
		'2026-02-11T00:00:00Z'
	stage_claim_rows="$(task_5_4_claim \
		'task-5-4-rollback-stage' \
		"$stage_revision" \
		'1' \
		'1' \
		'018f5d00-0000-7000-8000-000000000510' \
		'018f5d00-0000-7000-8000-000000000615' \
		'2026-02-11T00:00:00Z' \
		'2026-02-11T00:05:00Z')"
	expect_fixture_equal "$(fixture_line_count "$stage_claim_rows")" '1' 'task 5.4 rollback-stage claim confirmation'
	task_5_4_prepared_rollback_failure \
		'task 5.4 transactional stage rollback' \
		"$task_5_4_stage_prepare" \
		task_5_4_stage \
		'018f5d00-0000-7000-8000-000000000510' \
		'018f5d00-0000-7000-8000-000000000615' \
		'task-5-4-rollback-stage' \
		"$stage_revision" \
		'2026-02-11T00:00:00Z' \
		"$(task_5_4_end_observation_transcript \
			'task-5-4-rollback-stage' \
			'00000000-0000-4000-8000-000000000415' \
			'00000000-0000-4000-8000-000000000416' \
			'2026-02-11T00:00:00Z')" \
		'["Task 5.4 rollback stage summary"]' \
		'Task 5.4 rollback stage summary' \
		'["task-5-4-01","task-5-4-02","task-5-4-03","task-5-4-04","task-5-4-05","task-5-4-06","task-5-4-07","task-5-4-08","task-5-4-09","task-5-4-10"]' \
		'2026-02-11T00:01:00Z' \
		"[$stage_candidate]"
	expect_equal \
		"$(task_5_4_application_query "SELECT
			(SELECT state || '|' || (source_cutoff IS NULL AND transcript IS NULL AND summary_sentences IS NULL AND summary IS NULL AND concepts IS NULL AND generated_at IS NULL) FROM public.session_processing_attempts WHERE attempt_id = '018f5d00-0000-7000-8000-000000000510') || '|' ||
			(SELECT count(*) FROM public.session_memory_candidates WHERE attempt_id = '018f5d00-0000-7000-8000-000000000510') || '|' ||
			(SELECT count(*) FROM public.session_records WHERE session_id = 'task-5-4-rollback-stage')")" \
		'claimed|true|0|0' \
		'task 5.4 transactional stage failure rolls back attempt output candidate rows and current records'
	task_5_4_seed_ended_source \
		'task-5-4-rollback-marker' \
		'00000000-0000-4000-8000-000000000417' \
		'00000000-0000-4000-8000-000000000418' \
		'2026-02-12T00:00:00Z'
	marker_claim_rows="$(task_5_4_claim \
		'task-5-4-rollback-marker' \
		"$marker_revision" \
		'1' \
		'1' \
		'018f5d00-0000-7000-8000-000000000511' \
		'018f5d00-0000-7000-8000-000000000616' \
		'2026-02-12T00:00:00Z' \
		'2026-02-12T00:05:00Z')"
	expect_fixture_equal "$(fixture_line_count "$marker_claim_rows")" '1' 'task 5.4 rollback-marker claim confirmation'
	task_5_4_stage_attempt \
		'018f5d00-0000-7000-8000-000000000511' \
		'018f5d00-0000-7000-8000-000000000616' \
		'task-5-4-rollback-marker' \
		"$marker_revision" \
		'2026-02-12T00:00:00Z' \
		"$(task_5_4_end_observation_transcript \
			'task-5-4-rollback-marker' \
			'00000000-0000-4000-8000-000000000417' \
			'00000000-0000-4000-8000-000000000418' \
			'2026-02-12T00:00:00Z')" \
		'2026-02-12T00:01:00Z' \
		"[$marker_candidate]"
	task_5_4_prepared_rollback_failure \
		'task 5.4 transactional marker rollback' \
		"$task_5_4_mark_memory_published_prepare" \
		task_5_4_mark_memory_published \
		'018f5d00-0000-7000-8000-000000000511' \
		'018f5d00-0000-7000-8000-000000000616' \
		'task-5-4-rollback-marker' \
		"$marker_revision" \
		"$marker_memory_id"
	expect_equal \
		"$(task_5_4_application_query "SELECT
			(SELECT state FROM public.session_processing_attempts WHERE attempt_id = '018f5d00-0000-7000-8000-000000000511') || '|' ||
			(SELECT count(*) FROM public.session_memory_candidates WHERE attempt_id = '018f5d00-0000-7000-8000-000000000511' AND published_at IS NULL) || '|' ||
			(SELECT count(*) FROM public.session_records WHERE session_id = 'task-5-4-rollback-marker')")" \
		'staged|1|0' \
		'task 5.4 transactional marker failure rolls back publication state and preserves no current record'
	task_5_4_seed_ended_source \
		'task-5-4-rollback-promotion' \
		'00000000-0000-4000-8000-000000000419' \
		'00000000-0000-4000-8000-000000000420' \
		'2026-02-13T00:00:00Z'
	promotion_claim_rows="$(task_5_4_claim \
		'task-5-4-rollback-promotion' \
		"$promotion_revision" \
		'1' \
		'1' \
		'018f5d00-0000-7000-8000-000000000512' \
		'018f5d00-0000-7000-8000-000000000617' \
		'2026-02-13T00:00:00Z' \
		'2026-02-13T00:05:00Z')"
	expect_fixture_equal "$(fixture_line_count "$promotion_claim_rows")" '1' 'task 5.4 rollback-promotion claim confirmation'
	task_5_4_stage_attempt \
		'018f5d00-0000-7000-8000-000000000512' \
		'018f5d00-0000-7000-8000-000000000617' \
		'task-5-4-rollback-promotion' \
		"$promotion_revision" \
		'2026-02-13T00:00:00Z' \
		"$(task_5_4_end_observation_transcript \
			'task-5-4-rollback-promotion' \
			'00000000-0000-4000-8000-000000000419' \
			'00000000-0000-4000-8000-000000000420' \
			'2026-02-13T00:00:00Z')" \
		'2026-02-13T00:01:00Z' \
		'[]'
	promotion_load_rows="$(task_5_4_load_unpublished_candidates \
		'018f5d00-0000-7000-8000-000000000512' \
		'018f5d00-0000-7000-8000-000000000617' \
		'task-5-4-rollback-promotion' \
		"$promotion_revision")"
	expect_fixture_equal "$(fixture_line_count "$promotion_load_rows")" '1' 'task 5.4 rollback-promotion zero-candidate discovery confirmation'
	task_5_4_prepared_rollback_failure \
		'task 5.4 transactional promotion rollback' \
		"$task_5_4_promote_prepare" \
		task_5_4_promote \
		'018f5d00-0000-7000-8000-000000000512' \
		'018f5d00-0000-7000-8000-000000000617' \
		'task-5-4-rollback-promotion' \
		"$promotion_revision"
	expect_equal \
		"$(task_5_4_application_query "SELECT
			(SELECT state FROM public.session_processing_attempts WHERE attempt_id = '018f5d00-0000-7000-8000-000000000512') || '|' ||
			(SELECT count(*) FROM public.session_memory_candidates WHERE attempt_id = '018f5d00-0000-7000-8000-000000000512') || '|' ||
			(SELECT count(*) FROM public.session_records WHERE session_id = 'task-5-4-rollback-promotion')")" \
		'publishing|0|0' \
		'task 5.4 transactional promotion failure rolls back completion and current-record creation'
}

verify_task_5_4_query_plans() {
	local schema_before
	local indexes_before
	local discovery_plan
	local claim_candidates_plan
	local schema_after
	local indexes_after
	local relation

	schema_before="$(task_5_4_source_schema_snapshot)"
	indexes_before="$(task_5_4_source_index_snapshot)"
	safe_source_execute "
		INSERT INTO public.session_events (
			receipt_id, session_id, event_type, project_name, current_working_directory,
			source_timestamp_rfc3339, source_timestamp_utc, ingested_at
		) VALUES (
			'00000000-0000-4000-8000-000000000421', 'task-5-4-plan-ended', 'session_end', 'fixture-project', '/fixture',
			'2026-02-15T00:00:00Z', TIMESTAMPTZ '2026-02-15 00:00:00+00', TIMESTAMPTZ '2026-02-15 00:00:00+00'
		);
		INSERT INTO public.raw_observations (
			receipt_id, session_id, event_type, hook_type, project_name,
			current_working_directory, source_timestamp_rfc3339, source_timestamp_utc,
			ingested_at, data
		) VALUES
			('00000000-0000-4000-8000-000000000422', 'task-5-4-plan-quiet', 'observation', 'fixture_hook', 'fixture-project', '/fixture', '2026-02-14T00:00:00Z', TIMESTAMPTZ '2026-02-14 00:00:00+00', TIMESTAMPTZ '2026-02-14 00:00:00+00', '{}'::json),
			('00000000-0000-4000-8000-000000000423', 'task-5-4-plan-active', 'observation', 'fixture_hook', 'fixture-project', '/fixture', '2026-02-15T23:59:59Z', TIMESTAMPTZ '2026-02-15 23:59:59+00', TIMESTAMPTZ '2026-02-15 23:59:59+00', '{}'::json);
		ANALYZE public.session_events;
		ANALYZE public.raw_observations"
	task_5_4_migration_execute 'ANALYZE public.session_records'
	discovery_plan="$(task_5_4_discovery_plan_at '2026-02-16T00:00:00Z')"
	expect_fixture_not_equal "$discovery_plan" '' 'task 5.4 eligibility discovery plan is nonempty'
	claim_candidates_plan="$(task_5_4_claim_candidates_plan_at '2026-02-16T00:00:00Z')"
	expect_fixture_not_equal "$claim_candidates_plan" '' 'task 5.4 claim candidate plan is nonempty'
	for relation in session_events raw_observations session_records; do
		expect_fixture_contains \
			"$discovery_plan" \
			"$relation" \
			"task 5.4 eligibility discovery plan references $relation"
		expect_fixture_contains \
			"$claim_candidates_plan" \
			"$relation" \
			"task 5.4 claim candidate plan references $relation"
	done
	schema_after="$(task_5_4_source_schema_snapshot)"
	indexes_after="$(task_5_4_source_index_snapshot)"
	expect_fixture_equal "$schema_after" "$schema_before" 'task 5.4 query planning preserves the source schema snapshot'
	expect_fixture_equal "$indexes_after" "$indexes_before" 'task 5.4 query planning preserves the source index snapshot'
}

expect_equal "$bootstrap_role" 'session_post_processing_fixture_bootstrap' 'bootstrap role name'
expect_equal "$source_role" 'session_post_processing_source' 'source role name'
expect_equal "$migration_role" 'session_post_processing_migrator' 'migration role name'
expect_equal "$application_role" 'session_post_processing_application' 'application role name'
expect_equal "$database" 'session_post_processing_smoke' 'fixture database name'
expect_equal "$PGHOST" "$socket_dir" 'PGHOST'
expect_equal "$PGPORT" '5432' 'PGPORT'
expect_equal "$PGDATABASE" "$database" 'PGDATABASE'
expect_equal "$PGUSER" "$application_role" 'PGUSER'

expect_equal \
	"$(migration_query "SELECT count(*) FROM pg_class AS relation JOIN pg_namespace AS schema ON schema.oid = relation.relnamespace WHERE schema.nspname = 'public' AND relation.relkind = 'r' AND relation.relname IN ('session_records', 'session_processing_attempts', 'session_memory_candidates')")" \
	'0' \
	'processor tables before migration'
expect_equal \
	"$(source_security_snapshot)" \
	"raw_observations|$source_role|true|false|false|false|false|true
session_events|$source_role|true|false|false|false|false|true" \
	'source ownership and application grants before migration'
expect_equal \
	"$(migration_query "SELECT string_agg(relation.relname || '|' || attribute.attname || '|' || format_type(attribute.atttypid, attribute.atttypmod) || '|' || attribute.attnotnull, E'\\n' ORDER BY relation.relname, attribute.attnum) FROM pg_class AS relation JOIN pg_namespace AS schema ON schema.oid = relation.relnamespace JOIN pg_attribute AS attribute ON attribute.attrelid = relation.oid WHERE schema.nspname = 'public' AND relation.relname IN ('session_events', 'raw_observations') AND attribute.attnum > 0 AND NOT attribute.attisdropped")" \
	'raw_observations|receipt_id|uuid|true
raw_observations|session_id|text|true
raw_observations|event_type|text|true
raw_observations|hook_type|text|true
raw_observations|project_name|text|true
raw_observations|current_working_directory|text|true
raw_observations|source_timestamp_rfc3339|text|true
raw_observations|source_timestamp_utc|timestamp(3) with time zone|true
raw_observations|ingested_at|timestamp with time zone|true
raw_observations|data|json|true
session_events|receipt_id|uuid|true
session_events|session_id|text|true
session_events|event_type|text|true
session_events|project_name|text|true
session_events|current_working_directory|text|true
session_events|source_timestamp_rfc3339|text|true
session_events|source_timestamp_utc|timestamp(3) with time zone|true
session_events|ingested_at|timestamp with time zone|true' \
	'production-shaped source columns'
expect_equal \
	"$(migration_query "SELECT count(*) FROM pg_attrdef AS attribute_default JOIN pg_attribute AS attribute ON attribute.attrelid = attribute_default.adrelid AND attribute.attnum = attribute_default.adnum WHERE attribute_default.adrelid IN ('public.session_events'::regclass, 'public.raw_observations'::regclass) AND attribute.attname = 'receipt_id'")" \
	'0' \
	'explicit source receipt identifiers'
expect_equal \
	"$(migration_query "SELECT string_agg(relation.relname || '|' || index_relation.relname || '|' || table_index.indisprimary || '|' || COALESCE((SELECT string_agg(attribute.attname, ',' ORDER BY key.ordinality) FROM unnest(table_index.indkey) WITH ORDINALITY AS key(attnum, ordinality) JOIN pg_attribute AS attribute ON attribute.attrelid = relation.oid AND attribute.attnum = key.attnum WHERE key.attnum > 0), ''), E'\\n' ORDER BY relation.relname, index_relation.relname) FROM pg_index AS table_index JOIN pg_class AS relation ON relation.oid = table_index.indrelid JOIN pg_class AS index_relation ON index_relation.oid = table_index.indexrelid WHERE table_index.indrelid IN ('public.session_events'::regclass, 'public.raw_observations'::regclass)")" \
	'raw_observations|raw_observations_pkey|true|receipt_id
raw_observations|raw_observations_session_source_timestamp_idx|false|session_id,source_timestamp_utc
session_events|session_events_pkey|true|receipt_id
session_events|session_events_session_source_timestamp_idx|false|session_id,source_timestamp_utc' \
	'only source primary and production read indexes'
expect_equal \
	"$(migration_query "SELECT count(*) FROM pg_constraint WHERE conrelid IN ('public.session_events'::regclass, 'public.raw_observations'::regclass) AND contype = 'f'")" \
	'0' \
	'source foreign keys'
expect_equal \
	"$(migration_query "SELECT count(*) FROM pg_trigger WHERE tgrelid IN ('public.session_events'::regclass, 'public.raw_observations'::regclass) AND NOT tgisinternal")" \
	'0' \
	'source triggers'
expect_equal \
	"$(migration_query "SELECT string_agg(relname || '|' || relkind::text || '|' || relispartition, E'\\n' ORDER BY relname) FROM pg_class WHERE oid IN ('public.session_events'::regclass, 'public.raw_observations'::regclass)")" \
	'raw_observations|r|false
session_events|r|false' \
	'source non-partitioned tables'
expect_equal \
	"$(source_query "SELECT count(*) || '|' || count(DISTINCT receipt_id) || '|' || count(*) FILTER (WHERE event_type = 'session_start') || '|' || count(*) FILTER (WHERE event_type = 'session_end') FROM public.session_events")" \
	'2|2|1|1' \
	'synthetic lifecycle source data'
expect_equal \
	"$(source_query "SELECT count(*) || '|' || count(DISTINCT receipt_id) || '|' || count(*) FILTER (WHERE event_type = 'observation') FROM public.raw_observations")" \
	'1|1|1' \
	'synthetic observation source data'
expect_failure \
	'session event receipt id is required' \
	"$source_role" \
	"INSERT INTO public.session_events (session_id, event_type, project_name, current_working_directory, source_timestamp_rfc3339, source_timestamp_utc) VALUES ('fixture-missing-receipt', 'session_start', 'fixture-project', '/fixture', '2026-01-01T00:00:00Z', TIMESTAMPTZ '2026-01-01 00:00:00+00')"
expect_failure \
	'session event kind is restricted' \
	"$source_role" \
	"INSERT INTO public.session_events (receipt_id, session_id, event_type, project_name, current_working_directory, source_timestamp_rfc3339, source_timestamp_utc) VALUES ('00000000-0000-4000-8000-000000000010', 'fixture-invalid-session-event', 'invalid', 'fixture-project', '/fixture', '2026-01-01T00:00:00Z', TIMESTAMPTZ '2026-01-01 00:00:00+00')"
expect_failure \
	'observation event kind is restricted' \
	"$source_role" \
	"INSERT INTO public.raw_observations (receipt_id, session_id, event_type, hook_type, project_name, current_working_directory, source_timestamp_rfc3339, source_timestamp_utc, data) VALUES ('00000000-0000-4000-8000-000000000011', 'fixture-invalid-observation', 'invalid', 'fixture', 'fixture-project', '/fixture', '2026-01-01T00:00:00Z', TIMESTAMPTZ '2026-01-01 00:00:00+00', '{}'::json)"
expect_equal \
	"$(source_query "BEGIN; INSERT INTO public.session_events (receipt_id, session_id, event_type, project_name, current_working_directory, source_timestamp_rfc3339, source_timestamp_utc) VALUES ('00000000-0000-4000-8000-000000000012', 'fixture-explicit-receipt', 'session_start', 'fixture-project', '/fixture', '2026-01-01T00:00:00Z', TIMESTAMPTZ '2026-01-01 00:00:00+00'); SELECT count(*) FROM public.session_events WHERE receipt_id = '00000000-0000-4000-8000-000000000012'; ROLLBACK")" \
	'1' \
	'explicit source receipt insert'

source_security_before="$(source_security_snapshot)"
source_grants_before="$(source_grants_snapshot)"
apply_migration

server_version="$(application_query 'SHOW server_version')"
case "$server_version" in
"$expected_major".*) ;;
*) fail "expected PostgreSQL $expected_major, found $server_version" ;;
esac
expect_equal \
	"$(application_query "SELECT current_user || '|' || current_database() || '|' || CASE WHEN inet_client_addr() IS NULL THEN 'socket' ELSE 'network' END")" \
	"$application_role|$database|socket" \
	'application socket identity'
expect_equal \
	"$(migration_query "SELECT current_user || '|' || current_database() || '|' || CASE WHEN inet_client_addr() IS NULL THEN 'socket' ELSE 'network' END")" \
	"$migration_role|$database|socket" \
	'migration socket identity'
expect_equal \
	"$(migration_query "SELECT string_agg(tablename || '|' || tableowner, E'\\n' ORDER BY tablename) FROM pg_tables WHERE schemaname = 'public' AND tablename IN ('session_records', 'session_processing_attempts', 'session_memory_candidates')")" \
	"session_memory_candidates|$migration_role
session_processing_attempts|$migration_role
session_records|$migration_role" \
	'migration role owns processor tables'

for table in session_records session_processing_attempts session_memory_candidates; do
	expect_equal \
		"$(application_query "SELECT has_table_privilege(current_user, 'public.$table', 'SELECT') || '|' || has_table_privilege(current_user, 'public.$table', 'INSERT') || '|' || has_table_privilege(current_user, 'public.$table', 'UPDATE') || '|' || has_table_privilege(current_user, 'public.$table', 'DELETE') || '|' || has_table_privilege(current_user, 'public.$table', 'TRUNCATE')")" \
		'true|true|true|false|false' \
		"application $table privileges"
done
expect_equal \
	"$(application_query "SELECT (SELECT count(*) FROM public.session_events) || '|' || (SELECT count(*) FROM public.raw_observations)")" \
	'2|1' \
	'application source reads'
expect_equal \
	"$(application_query "INSERT INTO public.session_records (session_id, source_revision, lifecycle_count, observation_count, source_cutoff, transcript, summary_sentences, summary, concepts, generated_at) VALUES ('fixture-processor-session', '00000000-0000-4000-8000-000000000021', 1, 1, TIMESTAMPTZ '2026-01-01 00:00:00+00', '[]'::jsonb, ARRAY['fixture sentence']::text[], 'fixture summary', ARRAY['concept-01', 'concept-02', 'concept-03', 'concept-04', 'concept-05', 'concept-06', 'concept-07', 'concept-08', 'concept-09', 'concept-10']::text[], TIMESTAMPTZ '2026-01-01 00:00:00+00'); UPDATE public.session_records SET summary = 'fixture summary updated' WHERE session_id = 'fixture-processor-session'; SELECT summary FROM public.session_records WHERE session_id = 'fixture-processor-session'")" \
	'fixture summary updated' \
	'application processor insert and update'

expect_equal \
	"$(migration_query "SELECT count(*) FROM pg_constraint WHERE conrelid = 'public.session_records'::regclass AND conname IN ('session_records_session_id_not_empty', 'session_records_lifecycle_count_nonnegative', 'session_records_observation_count_nonnegative', 'session_records_source_count_positive', 'session_records_summary_sentence_count', 'session_records_summary_not_empty', 'session_records_concept_count', 'session_records_generated_after_cutoff')")" \
	'8' \
	'session record validation constraints remain present'
expect_equal \
	"$(application_query "SELECT has_table_privilege(current_user, 'public.session_records', 'SELECT') || '|' || has_table_privilege(current_user, 'public.session_records', 'INSERT') || '|' || has_table_privilege(current_user, 'public.session_records', 'UPDATE') || '|' || has_table_privilege(current_user, 'public.session_records', 'DELETE') || '|' || has_table_privilege(current_user, 'public.session_records', 'TRUNCATE')")" \
	'true|true|true|false|false' \
	'application session-record privileges'
expect_equal \
	"$(application_query "SELECT has_any_column_privilege(current_user, 'public.session_records', 'SELECT') || '|' || has_any_column_privilege(current_user, 'public.session_records', 'INSERT')")" \
	'true|true' \
	'application session record column privileges'
expect_equal "$(source_security_snapshot)" "$source_security_before" 'source ownership and grants after migration'
expect_equal "$(source_grants_snapshot)" "$source_grants_before" 'source table ACLs after migration'
expect_equal \
	"$(migration_query "SELECT (SELECT count(*) FROM pg_database AS target_database CROSS JOIN LATERAL aclexplode(target_database.datacl) AS privilege WHERE target_database.datname = '$database' AND privilege.grantee = 0) || '|' || (SELECT count(*) FROM pg_namespace AS schema CROSS JOIN LATERAL aclexplode(schema.nspacl) AS privilege WHERE schema.nspname = 'public' AND privilege.grantee = 0)")" \
	'0|0' \
	'public database and schema privileges'
expect_equal \
	"$(migration_query "SELECT count(*) FROM pg_class AS relation CROSS JOIN LATERAL aclexplode(relation.relacl) AS privilege WHERE relation.oid IN ('public.session_events'::regclass, 'public.raw_observations'::regclass, 'public.session_records'::regclass, 'public.session_processing_attempts'::regclass, 'public.session_memory_candidates'::regclass) AND privilege.grantee = 0")" \
	'0' \
	'public source and processor table privileges'
expect_equal \
	"$(application_query "SELECT count(*) FROM pg_extension WHERE extname = 'pg_uuidv7'")" \
	'0' \
	'pg_uuidv7 extension is not required'
expect_equal \
	"$(application_query "SELECT count(*) FROM pg_proc WHERE proname = 'uuid_generate_v7'")" \
	'0' \
	'uuid generation function is not required'
expect_equal \
	"$(migration_query "SELECT rolcanlogin FROM pg_roles WHERE rolname = '$bootstrap_role'")" \
	'f' \
	'disabled bootstrap role'

expect_failure \
	'application source insert' \
	"$application_role" \
	"INSERT INTO public.session_events (receipt_id, session_id, event_type, project_name, current_working_directory, source_timestamp_rfc3339, source_timestamp_utc) VALUES ('00000000-0000-4000-8000-000000000031', 'fixture-app-write', 'session_start', 'fixture-project', '/fixture', '2026-01-01T00:00:00Z', TIMESTAMPTZ '2026-01-01 00:00:00+00')"
expect_failure \
	'application source update' \
	"$application_role" \
	"UPDATE public.session_events SET session_id = 'fixture-app-write' WHERE false"
expect_failure \
	'application source delete' \
	"$application_role" \
	'DELETE FROM public.session_events WHERE false'
expect_failure \
	'application source truncate' \
	"$application_role" \
	'TRUNCATE public.session_events'
expect_failure \
	'application processor delete' \
	"$application_role" \
	'DELETE FROM public.session_records WHERE false'
expect_failure \
	'application processor truncate' \
	"$application_role" \
	'TRUNCATE public.session_records'
expect_failure \
	'application public schema create' \
	"$application_role" \
	'CREATE TABLE public.session_post_processing_application_create_denied (id integer)'
expect_failure \
	'application database create' \
	"$application_role" \
	'CREATE DATABASE session_post_processing_application_create_denied'
expect_failure \
	'application role create' \
	"$application_role" \
	'CREATE ROLE session_post_processing_application_create_denied'
expect_failure \
	'application migration role escalation' \
	"$application_role" \
	"SET ROLE $migration_role"
expect_failure \
	'disabled bootstrap login' \
	"$bootstrap_role" \
	'SELECT 1'

seed_task_5_3_eligibility_fixture
verify_task_5_3_eligibility
verify_task_5_3_concurrent_claims
verify_task_5_3_expired_reclaim
verify_task_5_3_snapshot_race
verify_task_5_4_staging
verify_task_5_4_refresh_suppression
verify_task_5_4_retry_reclaim
verify_task_5_4_fenced_promotion
verify_task_5_4_failed_refresh_preservation
verify_task_5_4_quiet_period_replacement
verify_task_5_4_transaction_rollbacks
verify_task_5_4_query_plans

printf 'PostgreSQL %s fixture server=%s\n' "$expected_major" "$server_version"
printf 'fixture source tables=production-shaped explicit-receipts no-uuid-extension\n'
printf 'fixture socket database=%s\n' "$database"
printf 'fixture processor privileges=select,insert,update only\n'
printf 'session post-processing eligibility=ended,inactive,boundary,resumed-quiet-period\n'
printf 'session post-processing claims=concurrent-one-owner,expired-reclaim,fenced-lease\n'
printf 'session post-processing supersession=production-uuidv5-source-revision-mismatch-unblocks-latest\n'
printf 'session post-processing staging=duplicate-fingerprint,publication-fencing,retry-reclaim\n'
printf 'session post-processing promotion=refresh-replacement,failed-refresh-preservation,quiet-boundary\n'
printf 'session post-processing rollback=stage,publication,promotion; plans=eligibility,claim-candidates\n'
