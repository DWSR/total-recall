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
readonly adapter_query_timeout_seconds=10
readonly statement_timeout_seconds=12
readonly invocation_timeout_seconds=15
readonly script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly psql="$postgres_bin/psql"
readonly rollback_script="$script_dir/rollback.sh"
schema_applied=false

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
	printf 'schema verification failed: %s\n' "$1" >&2
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

admin_query() {
	local sql=$1

	PGSERVICEFILE=/dev/null PGPASSFILE=/dev/null \
		"$psql" \
			--no-psqlrc \
			--no-password \
			--host="$socket_dir" \
			--port="$PGPORT" \
			--username="$bootstrap_role" \
			--dbname="$database" \
			--set=ON_ERROR_STOP=1 \
			--tuples-only \
			--no-align \
			--quiet \
			--command="SET ROLE $migration_role" \
			--command='SET search_path TO knowledge_graph' \
			--command="$sql"
}

read_query_as() {
	local output

	if ! output="$(query_as "$1" "$2" 2>/dev/null)"; then
		fail 'fixture database query failed'
	fi
	printf '%s' "$output"
}

assert_true() {
	local description=$1
	local sql
	local actual

	sql="$(< /dev/stdin)"
	if ! actual="$(admin_query "$sql" 2>/dev/null)"; then
		fail "$description query failed"
	fi
	[[ "$actual" == 't' ]] || fail "$description check failed"
}

assert_admin_script() {
	local description=$1
	local sql

	sql="$(< /dev/stdin)"
	if ! admin_query "$sql" >/dev/null 2>&1; then
		fail "$description check failed"
	fi
}

run_denied_as() {
	local role=$1
	local description=$2
	local sql=$3

	if query_as "$role" "$sql" >/dev/null 2>&1; then
		fail "$description was allowed"
	fi
}

assert_schema_absent() {
	local description=$1
	local actual

	actual="$(read_query_as "$bootstrap_role" "SELECT (NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'knowledge_graph') AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class AS relation JOIN pg_catalog.pg_namespace AS schema ON schema.oid = relation.relnamespace WHERE schema.nspname = 'knowledge_graph'))::text")"
	expect_equal "$actual" 'true' "$description"
}

apply_migration() {
	local inject_invalid_concept=$1

	{
		printf 'BEGIN;\nSET ROLE %s;\n' "$migration_role"
		printf "DO \$migration_owner\$ BEGIN IF current_user <> '%s' THEN RAISE EXCEPTION 'migration role assertion failed'; END IF; END \$migration_owner\$;\n" "$migration_role"
		cat "$schema_migration"
		if [[ "$inject_invalid_concept" == true ]]; then
			printf "\nINSERT INTO knowledge_graph.graph_concepts (id) VALUES ('018f0000-0000-7000-8000-000000000001');\nSET CONSTRAINTS ALL IMMEDIATE;\n"
		fi
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
	if [[ "$schema_applied" == true ]]; then
		if bash "$rollback_script" >/dev/null 2>&1; then
			schema_applied=false
		else
			printf 'schema verification cleanup failed: graph schema rollback failed\n' >&2
			cleanup_failed=true
		fi
	fi
	if [[ "$status" -eq 0 && "$cleanup_failed" == true ]]; then
		status=1
	fi
	exit "$status"
}

trap cleanup EXIT

expect_equal "$bootstrap_role" 'knowledge_graph_fixture_bootstrap' 'fixture bootstrap role'
expect_equal "$migration_role" 'knowledge_graph_migration_owner' 'migration owner role'
expect_equal "$application_role" 'knowledge_graph_application' 'graph application role'
expect_equal "$KNOWLEDGE_GRAPH_ADAPTER_QUERY_TIMEOUT_SECONDS" "$adapter_query_timeout_seconds" 'adapter query timeout'
expect_equal "$KNOWLEDGE_GRAPH_POSTGRES_STATEMENT_TIMEOUT_SECONDS" "$statement_timeout_seconds" 'PostgreSQL statement timeout'
expect_equal "$KNOWLEDGE_GRAPH_III_INVOCATION_TIMEOUT_SECONDS" "$invocation_timeout_seconds" 'iii invocation timeout'
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
assert_schema_absent 'empty fixture before invariant rollback'

if apply_migration true; then
	fail 'deferred preferred-alias invariant failure was accepted'
fi
assert_schema_absent 'schema absence after failed invariant transaction'
printf 'schema invariant-failure=deferred preferred-alias transaction=rolled-back graph-schema=absent\n'

if ! apply_migration false; then
	fail 'clean graph migration application failed'
fi
schema_applied=true

assert_true 'schema ownership' <<'SQL'
SELECT EXISTS (
  SELECT 1
  FROM pg_catalog.pg_namespace AS schema
  WHERE schema.nspname = current_schema()
    AND schema.nspowner = (
      SELECT role.oid
      FROM pg_catalog.pg_roles AS role
      WHERE role.rolname = current_user
    )
)
SQL

assert_true 'exact graph table set' <<'SQL'
WITH expected(relname) AS (
  VALUES
    ('graph_relation_types'),
    ('graph_concepts'),
    ('graph_aliases'),
    ('graph_source_references'),
    ('graph_concept_mentions'),
    ('graph_assertions'),
    ('graph_assertion_evidence')
),
actual AS (
  SELECT relation.relname::text
  FROM pg_catalog.pg_class AS relation
  WHERE relation.relnamespace = current_schema()::regnamespace
    AND relation.relkind = 'r'
)
SELECT (SELECT count(*) = 7 FROM actual)
  AND NOT EXISTS (SELECT relname FROM expected EXCEPT SELECT relname FROM actual)
  AND NOT EXISTS (SELECT relname FROM actual EXCEPT SELECT relname FROM expected)
SQL

assert_true 'table and index ownership' <<'SQL'
SELECT NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_class AS relation
    WHERE relation.relnamespace = current_schema()::regnamespace
      AND relation.relkind IN ('r', 'i')
      AND relation.relowner <> (
        SELECT role.oid
        FROM pg_catalog.pg_roles AS role
        WHERE role.rolname = current_user
      )
  )
  AND NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_class AS relation
    WHERE relation.relnamespace = current_schema()::regnamespace
      AND relation.relkind NOT IN ('r', 'i', 'S')
  )
SQL

assert_true 'exact graph column definitions' <<'SQL'
WITH expected(relname, attname, data_type, not_null, collation_name, identity_kind, default_expr) AS (
  VALUES
    ('graph_relation_types', 'code', 'text', true, 'C', '', NULL::text),
    ('graph_relation_types', 'is_symmetric', 'boolean', true, NULL, '', NULL),
    ('graph_concepts', 'id', 'uuid', true, NULL, '', NULL),
    ('graph_concepts', 'revision', 'bigint', true, NULL, '', '1'),
    ('graph_aliases', 'concept_id', 'uuid', true, NULL, '', NULL),
    ('graph_aliases', 'alias_key', 'text', true, 'C', '', NULL),
    ('graph_aliases', 'display_text', 'text', true, 'default', '', NULL),
    ('graph_aliases', 'is_preferred', 'boolean', true, NULL, '', NULL),
    ('graph_aliases', 'normalization_version', 'text', true, 'C', '', NULL),
    ('graph_source_references', 'source_ref_id', 'bigint', true, NULL, 'a', NULL),
    ('graph_source_references', 'source_kind', 'text', true, 'C', '', NULL),
    ('graph_source_references', 'external_id', 'text', true, 'C', '', NULL),
    ('graph_source_references', 'external_version', 'bigint', false, NULL, '', NULL),
    ('graph_concept_mentions', 'concept_id', 'uuid', true, NULL, '', NULL),
    ('graph_concept_mentions', 'source_ref_id', 'bigint', true, NULL, '', NULL),
    ('graph_assertions', 'id', 'uuid', true, NULL, '', NULL),
    ('graph_assertions', 'subject_concept_id', 'uuid', true, NULL, '', NULL),
    ('graph_assertions', 'relation_code', 'text', true, 'C', '', NULL),
    ('graph_assertions', 'object_concept_id', 'uuid', true, NULL, '', NULL),
    ('graph_assertions', 'revision', 'bigint', true, NULL, '', '1'),
    ('graph_assertion_evidence', 'assertion_id', 'uuid', true, NULL, '', NULL),
    ('graph_assertion_evidence', 'source_ref_id', 'bigint', true, NULL, '', NULL)
),
actual AS (
  SELECT
    relation.relname::text,
    attribute.attname::text,
    pg_catalog.format_type(attribute.atttypid, attribute.atttypmod),
    attribute.attnotnull,
    CASE
      WHEN attribute.attcollation = 0 THEN NULL
      ELSE collation_row.collname::text
    END,
    COALESCE(attribute.attidentity::text, ''),
    pg_catalog.pg_get_expr(default_value.adbin, default_value.adrelid, true)
  FROM pg_catalog.pg_class AS relation
  JOIN pg_catalog.pg_attribute AS attribute
    ON attribute.attrelid = relation.oid
  LEFT JOIN pg_catalog.pg_collation AS collation_row
    ON collation_row.oid = attribute.attcollation
  LEFT JOIN pg_catalog.pg_attrdef AS default_value
    ON default_value.adrelid = attribute.attrelid
    AND default_value.adnum = attribute.attnum
  WHERE relation.relnamespace = current_schema()::regnamespace
    AND relation.relkind = 'r'
    AND attribute.attnum > 0
    AND NOT attribute.attisdropped
)
SELECT (SELECT count(*) = 22 FROM actual)
  AND NOT EXISTS (SELECT * FROM expected EXCEPT SELECT * FROM actual)
  AND NOT EXISTS (SELECT * FROM actual EXCEPT SELECT * FROM expected)
SQL

assert_true 'identity sequence ownership and dependency' <<'SQL'
SELECT (SELECT count(*) = 1 FROM pg_catalog.pg_class AS sequence
        WHERE sequence.relnamespace = current_schema()::regnamespace
          AND sequence.relkind = 'S'
          AND sequence.relname = 'graph_source_references_source_ref_id_seq'
          AND sequence.relowner = (
            SELECT role.oid
            FROM pg_catalog.pg_roles AS role
            WHERE role.rolname = current_user
          ))
  AND EXISTS (
    SELECT 1
    FROM pg_catalog.pg_class AS sequence
    JOIN pg_catalog.pg_depend AS dependency
      ON dependency.objid = sequence.oid
      AND dependency.classid = 'pg_catalog.pg_class'::regclass
      AND dependency.deptype = 'i'
    JOIN pg_catalog.pg_class AS source_table
      ON source_table.oid = dependency.refobjid
    JOIN pg_catalog.pg_attribute AS source_column
      ON source_column.attrelid = source_table.oid
      AND source_column.attnum = dependency.refobjsubid
    WHERE sequence.relnamespace = current_schema()::regnamespace
      AND sequence.relname = 'graph_source_references_source_ref_id_seq'
      AND source_table.relname = 'graph_source_references'
      AND source_column.attname = 'source_ref_id'
  )
SQL

assert_true 'relation vocabulary seed' <<'SQL'
WITH expected(code, is_symmetric) AS (
  VALUES
    ('related_to', true),
    ('is_a', false),
    ('part_of', false),
    ('depends_on', false),
    ('uses', false),
    ('implements', false),
    ('causes', false),
    ('resolves', false),
    ('contradicts', true)
),
actual AS (
  SELECT code::text, is_symmetric
  FROM knowledge_graph.graph_relation_types
)
SELECT (SELECT count(*) = 9 FROM actual)
  AND NOT EXISTS (SELECT * FROM expected EXCEPT SELECT * FROM actual)
  AND NOT EXISTS (SELECT * FROM actual EXCEPT SELECT * FROM expected)
SQL

assert_true 'named constraint definitions' <<'SQL'
WITH expected(relname, conname, contype, definition) AS (
  VALUES
    ('graph_relation_types', 'graph_relation_types_pkey', 'p', 'PRIMARY KEY (code)'),
    ('graph_relation_types', 'graph_relation_types_code_check', 'c', 'CHECK (code = ANY (ARRAY[''related_to''::text, ''is_a''::text, ''part_of''::text, ''depends_on''::text, ''uses''::text, ''implements''::text, ''causes''::text, ''resolves''::text, ''contradicts''::text]))'),
    ('graph_concepts', 'graph_concepts_pkey', 'p', 'PRIMARY KEY (id)'),
    ('graph_concepts', 'graph_concepts_revision_positive', 'c', 'CHECK (revision > 0)'),
    ('graph_concepts', 'graph_concepts_uuid_v7_check', 'c', 'CHECK ("substring"(id::text, 15, 1) = ''7''::text AND ("substring"(id::text, 20, 1) = ANY (ARRAY[''8''::text, ''9''::text, ''a''::text, ''b''::text])))'),
    ('graph_aliases', 'graph_aliases_pkey', 'p', 'PRIMARY KEY (alias_key)'),
    ('graph_aliases', 'graph_aliases_alias_key_bounds', 'c', 'CHECK (alias_key <> ''''::text AND octet_length(alias_key) >= 1 AND octet_length(alias_key) <= 2048)'),
    ('graph_aliases', 'graph_aliases_display_text_bounds', 'c', 'CHECK (display_text <> ''''::text AND octet_length(display_text) >= 1 AND octet_length(display_text) <= 512)'),
    ('graph_aliases', 'graph_aliases_normalization_version_check', 'c', 'CHECK (normalization_version = ''icu4x-2.3.0-nfkc-fold-v1''::text)'),
    ('graph_aliases', 'graph_aliases_concept_fk', 'f', 'FOREIGN KEY (concept_id) REFERENCES graph_concepts(id) ON UPDATE RESTRICT ON DELETE CASCADE'),
    ('graph_source_references', 'graph_source_references_pkey', 'p', 'PRIMARY KEY (source_ref_id)'),
    ('graph_source_references', 'graph_source_references_kind_check', 'c', 'CHECK (source_kind = ANY (ARRAY[''memory_version''::text, ''session_record''::text]))'),
    ('graph_source_references', 'graph_source_references_shape_check', 'c', 'CHECK (source_kind = ''memory_version''::text AND external_version IS NOT NULL AND external_version > 0 OR source_kind = ''session_record''::text AND external_version IS NULL)'),
    ('graph_source_references', 'graph_source_references_external_id_bounds', 'c', 'CHECK (external_id <> ''''::text AND octet_length(external_id) >= 1 AND octet_length(external_id) <= 2048)'),
    ('graph_source_references', 'graph_source_references_identity_unique', 'u', 'UNIQUE NULLS NOT DISTINCT (source_kind, external_id, external_version)'),
    ('graph_concept_mentions', 'graph_concept_mentions_pkey', 'p', 'PRIMARY KEY (concept_id, source_ref_id)'),
    ('graph_concept_mentions', 'graph_concept_mentions_concept_fk', 'f', 'FOREIGN KEY (concept_id) REFERENCES graph_concepts(id) ON UPDATE RESTRICT ON DELETE RESTRICT'),
    ('graph_concept_mentions', 'graph_concept_mentions_source_fk', 'f', 'FOREIGN KEY (source_ref_id) REFERENCES graph_source_references(source_ref_id) ON UPDATE RESTRICT ON DELETE RESTRICT'),
    ('graph_assertions', 'graph_assertions_pkey', 'p', 'PRIMARY KEY (id)'),
    ('graph_assertions', 'graph_assertions_revision_positive', 'c', 'CHECK (revision > 0)'),
    ('graph_assertions', 'graph_assertions_uuid_v7_check', 'c', 'CHECK ("substring"(id::text, 15, 1) = ''7''::text AND ("substring"(id::text, 20, 1) = ANY (ARRAY[''8''::text, ''9''::text, ''a''::text, ''b''::text])))'),
    ('graph_assertions', 'graph_assertions_distinct_endpoints', 'c', 'CHECK (subject_concept_id <> object_concept_id)'),
    ('graph_assertions', 'graph_assertions_semantic_identity', 'u', 'UNIQUE (subject_concept_id, relation_code, object_concept_id)'),
    ('graph_assertions', 'graph_assertions_subject_concept_fk', 'f', 'FOREIGN KEY (subject_concept_id) REFERENCES graph_concepts(id) ON UPDATE RESTRICT ON DELETE RESTRICT'),
    ('graph_assertions', 'graph_assertions_relation_fk', 'f', 'FOREIGN KEY (relation_code) REFERENCES graph_relation_types(code) ON UPDATE RESTRICT ON DELETE RESTRICT'),
    ('graph_assertions', 'graph_assertions_object_concept_fk', 'f', 'FOREIGN KEY (object_concept_id) REFERENCES graph_concepts(id) ON UPDATE RESTRICT ON DELETE RESTRICT'),
    ('graph_assertion_evidence', 'graph_assertion_evidence_pkey', 'p', 'PRIMARY KEY (assertion_id, source_ref_id)'),
    ('graph_assertion_evidence', 'graph_assertion_evidence_assertion_fk', 'f', 'FOREIGN KEY (assertion_id) REFERENCES graph_assertions(id) ON UPDATE RESTRICT ON DELETE CASCADE'),
    ('graph_assertion_evidence', 'graph_assertion_evidence_source_fk', 'f', 'FOREIGN KEY (source_ref_id) REFERENCES graph_source_references(source_ref_id) ON UPDATE RESTRICT ON DELETE RESTRICT')
),
actual AS (
  SELECT
    relation.relname::text,
    constraint_row.conname::text,
    constraint_row.contype::text,
    pg_catalog.pg_get_constraintdef(constraint_row.oid, true)
  FROM pg_catalog.pg_constraint AS constraint_row
  JOIN pg_catalog.pg_class AS relation
    ON relation.oid = constraint_row.conrelid
  WHERE constraint_row.connamespace = current_schema()::regnamespace
    AND constraint_row.contype NOT IN ('t', 'n')
)
SELECT (SELECT count(*) = 29 FROM actual)
  AND NOT EXISTS (SELECT * FROM expected EXCEPT SELECT * FROM actual)
  AND NOT EXISTS (SELECT * FROM actual EXCEPT SELECT * FROM expected)
  AND NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_constraint AS constraint_row
    WHERE constraint_row.connamespace = current_schema()::regnamespace
      AND constraint_row.contype NOT IN ('t', 'n')
      AND (constraint_row.condeferrable OR constraint_row.condeferred)
  )
SQL

assert_admin_script 'source-key byte-bound and identity-index boundary' <<'SQL'
DO $source_key_boundary$
DECLARE
  memory_key text;
  session_key text;
  oversized_memory_key text;
  oversized_session_key text;
  inserted_rows bigint;
  rejected_state text;
  rejected_constraint text;
  rejected_memory_key boolean := false;
  rejected_session_key boolean := false;
BEGIN
  SELECT pg_catalog.string_agg(
      pg_catalog.md5('kg-schema-memory-' || counter::text),
      '' ORDER BY counter
    )
  INTO memory_key
  FROM pg_catalog.generate_series(1, 64) AS counters(counter);

  SELECT pg_catalog.string_agg(
      pg_catalog.md5('kg-schema-session-' || counter::text),
      '' ORDER BY counter
    )
  INTO session_key
  FROM pg_catalog.generate_series(1, 64) AS counters(counter);

  IF pg_catalog.octet_length(memory_key) <> 2048
     OR pg_catalog.octet_length(session_key) <> 2048 THEN
    RAISE EXCEPTION 'source-key boundary fixture length mismatch';
  END IF;

  oversized_memory_key := memory_key || 'x';
  oversized_session_key := session_key || 'x';
  IF pg_catalog.octet_length(oversized_memory_key) <> 2049
     OR pg_catalog.octet_length(oversized_session_key) <> 2049 THEN
    RAISE EXCEPTION 'oversized source-key fixture length mismatch';
  END IF;

  INSERT INTO knowledge_graph.graph_source_references
    (source_kind, external_id, external_version)
  VALUES ('memory_version', memory_key, 1)
  ON CONFLICT ON CONSTRAINT graph_source_references_identity_unique DO NOTHING;
  GET DIAGNOSTICS inserted_rows = ROW_COUNT;
  IF inserted_rows <> 1 THEN
    RAISE EXCEPTION 'memory source-key fixture was not inserted';
  END IF;

  INSERT INTO knowledge_graph.graph_source_references
    (source_kind, external_id, external_version)
  VALUES ('memory_version', memory_key, 1)
  ON CONFLICT ON CONSTRAINT graph_source_references_identity_unique DO NOTHING;
  GET DIAGNOSTICS inserted_rows = ROW_COUNT;
  IF inserted_rows <> 0 OR NOT EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_source_references
    WHERE source_kind = 'memory_version'
      AND external_id = memory_key
      AND external_version = 1
  ) THEN
    RAISE EXCEPTION 'memory source-key identity index check failed';
  END IF;

  INSERT INTO knowledge_graph.graph_source_references
    (source_kind, external_id, external_version)
  VALUES ('session_record', session_key, NULL)
  ON CONFLICT ON CONSTRAINT graph_source_references_identity_unique DO NOTHING;
  GET DIAGNOSTICS inserted_rows = ROW_COUNT;
  IF inserted_rows <> 1 THEN
    RAISE EXCEPTION 'session source-key fixture was not inserted';
  END IF;

  INSERT INTO knowledge_graph.graph_source_references
    (source_kind, external_id, external_version)
  VALUES ('session_record', session_key, NULL)
  ON CONFLICT ON CONSTRAINT graph_source_references_identity_unique DO NOTHING;
  GET DIAGNOSTICS inserted_rows = ROW_COUNT;
  IF inserted_rows <> 0 OR NOT EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_source_references
    WHERE source_kind = 'session_record'
      AND external_id = session_key
      AND external_version IS NULL
  ) THEN
    RAISE EXCEPTION 'session source-key identity index check failed';
  END IF;

  BEGIN
    INSERT INTO knowledge_graph.graph_source_references
      (source_kind, external_id, external_version)
    VALUES ('memory_version', oversized_memory_key, 1);
  EXCEPTION
    WHEN check_violation THEN
      GET STACKED DIAGNOSTICS
        rejected_state = RETURNED_SQLSTATE,
        rejected_constraint = CONSTRAINT_NAME;
      IF rejected_state = '23514'
         AND rejected_constraint = 'graph_source_references_external_id_bounds' THEN
        rejected_memory_key := true;
      ELSE
        RAISE EXCEPTION 'memory source-key rejection used an unexpected check';
      END IF;
  END;
  IF NOT rejected_memory_key OR EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_source_references
    WHERE source_kind = 'memory_version'
      AND external_id = oversized_memory_key
      AND external_version = 1
  ) THEN
    RAISE EXCEPTION 'memory source-key bound rejection check failed';
  END IF;

  BEGIN
    INSERT INTO knowledge_graph.graph_source_references
      (source_kind, external_id, external_version)
    VALUES ('session_record', oversized_session_key, NULL);
  EXCEPTION
    WHEN check_violation THEN
      GET STACKED DIAGNOSTICS
        rejected_state = RETURNED_SQLSTATE,
        rejected_constraint = CONSTRAINT_NAME;
      IF rejected_state = '23514'
         AND rejected_constraint = 'graph_source_references_external_id_bounds' THEN
        rejected_session_key := true;
      ELSE
        RAISE EXCEPTION 'session source-key rejection used an unexpected check';
      END IF;
  END;
  IF NOT rejected_session_key OR EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_source_references
    WHERE source_kind = 'session_record'
      AND external_id = oversized_session_key
      AND external_version IS NULL
  ) THEN
    RAISE EXCEPTION 'session source-key bound rejection check failed';
  END IF;
END
$source_key_boundary$;
SQL

assert_true 'generated NOT NULL constraint catalog entries' <<'SQL'
WITH expected(relname, conname, definition) AS (
  VALUES
    ('graph_relation_types', 'graph_relation_types_code_not_null', 'NOT NULL code'),
    ('graph_relation_types', 'graph_relation_types_is_symmetric_not_null', 'NOT NULL is_symmetric'),
    ('graph_concepts', 'graph_concepts_id_not_null', 'NOT NULL id'),
    ('graph_concepts', 'graph_concepts_revision_not_null', 'NOT NULL revision'),
    ('graph_aliases', 'graph_aliases_concept_id_not_null', 'NOT NULL concept_id'),
    ('graph_aliases', 'graph_aliases_alias_key_not_null', 'NOT NULL alias_key'),
    ('graph_aliases', 'graph_aliases_display_text_not_null', 'NOT NULL display_text'),
    ('graph_aliases', 'graph_aliases_is_preferred_not_null', 'NOT NULL is_preferred'),
    ('graph_aliases', 'graph_aliases_normalization_version_not_null', 'NOT NULL normalization_version'),
    ('graph_source_references', 'graph_source_references_source_ref_id_not_null', 'NOT NULL source_ref_id'),
    ('graph_source_references', 'graph_source_references_source_kind_not_null', 'NOT NULL source_kind'),
    ('graph_source_references', 'graph_source_references_external_id_not_null', 'NOT NULL external_id'),
    ('graph_concept_mentions', 'graph_concept_mentions_concept_id_not_null', 'NOT NULL concept_id'),
    ('graph_concept_mentions', 'graph_concept_mentions_source_ref_id_not_null', 'NOT NULL source_ref_id'),
    ('graph_assertions', 'graph_assertions_id_not_null', 'NOT NULL id'),
    ('graph_assertions', 'graph_assertions_subject_concept_id_not_null', 'NOT NULL subject_concept_id'),
    ('graph_assertions', 'graph_assertions_relation_code_not_null', 'NOT NULL relation_code'),
    ('graph_assertions', 'graph_assertions_object_concept_id_not_null', 'NOT NULL object_concept_id'),
    ('graph_assertions', 'graph_assertions_revision_not_null', 'NOT NULL revision'),
    ('graph_assertion_evidence', 'graph_assertion_evidence_assertion_id_not_null', 'NOT NULL assertion_id'),
    ('graph_assertion_evidence', 'graph_assertion_evidence_source_ref_id_not_null', 'NOT NULL source_ref_id')
),
actual AS (
  SELECT
    relation.relname::text,
    constraint_row.conname::text,
    pg_catalog.pg_get_constraintdef(constraint_row.oid, true)
  FROM pg_catalog.pg_constraint AS constraint_row
  JOIN pg_catalog.pg_class AS relation
    ON relation.oid = constraint_row.conrelid
  WHERE constraint_row.connamespace = current_schema()::regnamespace
    AND constraint_row.contype = 'n'
)
SELECT (SELECT count(*) = 0 FROM actual)
  OR (
    (SELECT count(*) = 21 FROM actual)
    AND NOT EXISTS (SELECT * FROM expected EXCEPT SELECT * FROM actual)
    AND NOT EXISTS (SELECT * FROM actual EXCEPT SELECT * FROM expected)
  )
SQL

assert_true 'foreign key columns actions and timing' <<'SQL'
WITH expected(conname, source_table, source_columns, target_table, target_columns, update_action, delete_action, is_deferrable, is_initially_deferred, match_type) AS (
  VALUES
    ('graph_aliases_concept_fk', 'graph_aliases', ARRAY['concept_id']::text[], 'graph_concepts', ARRAY['id']::text[], 'r', 'c', false, false, 's'),
    ('graph_concept_mentions_concept_fk', 'graph_concept_mentions', ARRAY['concept_id']::text[], 'graph_concepts', ARRAY['id']::text[], 'r', 'r', false, false, 's'),
    ('graph_concept_mentions_source_fk', 'graph_concept_mentions', ARRAY['source_ref_id']::text[], 'graph_source_references', ARRAY['source_ref_id']::text[], 'r', 'r', false, false, 's'),
    ('graph_assertions_subject_concept_fk', 'graph_assertions', ARRAY['subject_concept_id']::text[], 'graph_concepts', ARRAY['id']::text[], 'r', 'r', false, false, 's'),
    ('graph_assertions_relation_fk', 'graph_assertions', ARRAY['relation_code']::text[], 'graph_relation_types', ARRAY['code']::text[], 'r', 'r', false, false, 's'),
    ('graph_assertions_object_concept_fk', 'graph_assertions', ARRAY['object_concept_id']::text[], 'graph_concepts', ARRAY['id']::text[], 'r', 'r', false, false, 's'),
    ('graph_assertion_evidence_assertion_fk', 'graph_assertion_evidence', ARRAY['assertion_id']::text[], 'graph_assertions', ARRAY['id']::text[], 'r', 'c', false, false, 's'),
    ('graph_assertion_evidence_source_fk', 'graph_assertion_evidence', ARRAY['source_ref_id']::text[], 'graph_source_references', ARRAY['source_ref_id']::text[], 'r', 'r', false, false, 's')
),
actual AS (
  SELECT
    constraint_row.conname::text,
    source_table.relname::text,
    ARRAY(
      SELECT source_column.attname::text
      FROM pg_catalog.unnest(constraint_row.conkey) WITH ORDINALITY AS source_position(attnum, item_position)
      JOIN pg_catalog.pg_attribute AS source_column
        ON source_column.attrelid = constraint_row.conrelid
        AND source_column.attnum = source_position.attnum
      ORDER BY source_position.item_position
    ),
    target_table.relname::text,
    ARRAY(
      SELECT target_column.attname::text
      FROM pg_catalog.unnest(constraint_row.confkey) WITH ORDINALITY AS target_position(attnum, item_position)
      JOIN pg_catalog.pg_attribute AS target_column
        ON target_column.attrelid = constraint_row.confrelid
        AND target_column.attnum = target_position.attnum
      ORDER BY target_position.item_position
    ),
    constraint_row.confupdtype::text,
    constraint_row.confdeltype::text,
    constraint_row.condeferrable,
    constraint_row.condeferred,
    constraint_row.confmatchtype::text
  FROM pg_catalog.pg_constraint AS constraint_row
  JOIN pg_catalog.pg_class AS source_table
    ON source_table.oid = constraint_row.conrelid
  JOIN pg_catalog.pg_class AS target_table
    ON target_table.oid = constraint_row.confrelid
  WHERE constraint_row.connamespace = current_schema()::regnamespace
    AND constraint_row.contype = 'f'
)
SELECT (SELECT count(*) = 8 FROM actual)
  AND NOT EXISTS (SELECT * FROM expected EXCEPT SELECT * FROM actual)
  AND NOT EXISTS (SELECT * FROM actual EXCEPT SELECT * FROM expected)
SQL

assert_true 'index definitions and readiness' <<'SQL'
WITH expected(index_name, definition) AS (
  VALUES
    ('graph_aliases_pkey', 'CREATE UNIQUE INDEX graph_aliases_pkey ON knowledge_graph.graph_aliases USING btree (alias_key)'),
    ('graph_aliases_one_preferred_per_concept_uidx', 'CREATE UNIQUE INDEX graph_aliases_one_preferred_per_concept_uidx ON knowledge_graph.graph_aliases USING btree (concept_id) WHERE is_preferred'),
    ('graph_aliases_concept_order_idx', 'CREATE INDEX graph_aliases_concept_order_idx ON knowledge_graph.graph_aliases USING btree (concept_id, is_preferred DESC, alias_key)'),
    ('graph_source_references_pkey', 'CREATE UNIQUE INDEX graph_source_references_pkey ON knowledge_graph.graph_source_references USING btree (source_ref_id)'),
    ('graph_source_references_identity_unique', 'CREATE UNIQUE INDEX graph_source_references_identity_unique ON knowledge_graph.graph_source_references USING btree (source_kind, external_id, external_version) NULLS NOT DISTINCT'),
    ('graph_concept_mentions_pkey', 'CREATE UNIQUE INDEX graph_concept_mentions_pkey ON knowledge_graph.graph_concept_mentions USING btree (concept_id, source_ref_id)'),
    ('graph_concept_mentions_source_idx', 'CREATE INDEX graph_concept_mentions_source_idx ON knowledge_graph.graph_concept_mentions USING btree (source_ref_id, concept_id)'),
    ('graph_assertions_pkey', 'CREATE UNIQUE INDEX graph_assertions_pkey ON knowledge_graph.graph_assertions USING btree (id)'),
    ('graph_assertions_semantic_identity', 'CREATE UNIQUE INDEX graph_assertions_semantic_identity ON knowledge_graph.graph_assertions USING btree (subject_concept_id, relation_code, object_concept_id)'),
    ('graph_assertions_subject_adjacency_idx', 'CREATE INDEX graph_assertions_subject_adjacency_idx ON knowledge_graph.graph_assertions USING btree (subject_concept_id, relation_code, object_concept_id, id)'),
    ('graph_assertions_object_adjacency_idx', 'CREATE INDEX graph_assertions_object_adjacency_idx ON knowledge_graph.graph_assertions USING btree (object_concept_id, relation_code, subject_concept_id, id)'),
    ('graph_assertion_evidence_pkey', 'CREATE UNIQUE INDEX graph_assertion_evidence_pkey ON knowledge_graph.graph_assertion_evidence USING btree (assertion_id, source_ref_id)'),
    ('graph_assertion_evidence_source_idx', 'CREATE INDEX graph_assertion_evidence_source_idx ON knowledge_graph.graph_assertion_evidence USING btree (source_ref_id, assertion_id)'),
    ('graph_concepts_pkey', 'CREATE UNIQUE INDEX graph_concepts_pkey ON knowledge_graph.graph_concepts USING btree (id)'),
    ('graph_relation_types_pkey', 'CREATE UNIQUE INDEX graph_relation_types_pkey ON knowledge_graph.graph_relation_types USING btree (code)')
),
actual AS (
  SELECT index_relation.relname::text, pg_catalog.pg_get_indexdef(index_row.indexrelid)
  FROM pg_catalog.pg_index AS index_row
  JOIN pg_catalog.pg_class AS index_relation
    ON index_relation.oid = index_row.indexrelid
  WHERE index_relation.relnamespace = current_schema()::regnamespace
)
SELECT (SELECT count(*) = 15 FROM actual)
  AND NOT EXISTS (SELECT * FROM expected EXCEPT SELECT * FROM actual)
  AND NOT EXISTS (SELECT * FROM actual EXCEPT SELECT * FROM expected)
  AND NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_index AS index_row
    JOIN pg_catalog.pg_class AS index_relation
      ON index_relation.oid = index_row.indexrelid
    WHERE index_relation.relnamespace = current_schema()::regnamespace
      AND (NOT index_row.indisvalid OR NOT index_row.indisready)
  )
SQL

assert_true 'deferred constraint trigger definitions' <<'SQL'
WITH expected(trigger_name, table_name, function_name, trigger_type, enabled, is_deferrable, is_initially_deferred, is_constraint, column_names) AS (
  VALUES
    ('graph_assertions_canonical_symmetric_endpoints', 'graph_assertions', 'graph_enforce_assertion_endpoint_order', 23, 'O', false, false, false, ARRAY['subject_concept_id', 'relation_code', 'object_concept_id']::text[]),
    ('graph_assertions_evidence_invariant', 'graph_assertions', 'graph_enforce_assertion_evidence_invariant', 29, 'O', true, true, true, ARRAY[]::text[]),
    ('graph_assertion_evidence_invariant', 'graph_assertion_evidence', 'graph_enforce_assertion_evidence_invariant', 29, 'O', true, true, true, ARRAY[]::text[]),
    ('graph_concepts_preferred_alias_invariant', 'graph_concepts', 'graph_enforce_concept_alias_invariant', 29, 'O', true, true, true, ARRAY[]::text[]),
    ('graph_aliases_preferred_alias_invariant', 'graph_aliases', 'graph_enforce_concept_alias_invariant', 29, 'O', true, true, true, ARRAY[]::text[])
),
actual AS (
  SELECT
    trigger_row.tgname::text,
    table_relation.relname::text,
    trigger_function.proname::text,
    trigger_row.tgtype::integer,
    trigger_row.tgenabled::text,
    trigger_row.tgdeferrable,
    trigger_row.tginitdeferred,
    trigger_row.tgconstraint <> 0,
    ARRAY(
      SELECT attribute.attname::text
      FROM pg_catalog.unnest(trigger_row.tgattr::smallint[]) AS selected(attnum)
      JOIN pg_catalog.pg_attribute AS attribute
        ON attribute.attrelid = trigger_row.tgrelid
        AND attribute.attnum = selected.attnum
      ORDER BY selected.attnum
    )
  FROM pg_catalog.pg_trigger AS trigger_row
  JOIN pg_catalog.pg_class AS table_relation
    ON table_relation.oid = trigger_row.tgrelid
  JOIN pg_catalog.pg_proc AS trigger_function
    ON trigger_function.oid = trigger_row.tgfoid
  WHERE table_relation.relnamespace = current_schema()::regnamespace
    AND NOT trigger_row.tgisinternal
)
SELECT (SELECT count(*) = 5 FROM actual)
  AND NOT EXISTS (SELECT * FROM expected EXCEPT SELECT * FROM actual)
  AND NOT EXISTS (SELECT * FROM actual EXCEPT SELECT * FROM expected)
SQL

assert_true 'routine signatures ownership security and search paths' <<'SQL'
WITH expected(function_name, identity_types, return_type) AS (
  VALUES
    ('graph_lock_key_concept_id', 'uuid', 'jsonb'),
    ('graph_lock_key_normalized_alias', 'text', 'jsonb'),
    ('graph_lock_key_source_identity', 'text, text, bigint', 'jsonb'),
    ('graph_lock_key_concept_mention', 'uuid, text, text, bigint', 'jsonb'),
    ('graph_lock_key_assertion_id', 'uuid', 'jsonb'),
    ('graph_lock_key_semantic_assertion', 'text, uuid, uuid', 'jsonb'),
    ('graph_acquire_mutation_locks', 'jsonb, uuid[], uuid[], bigint[]', 'void'),
    ('graph_create_concept', 'uuid, jsonb', 'jsonb'),
    ('graph_replace_concept_aliases', 'uuid, bigint, jsonb', 'jsonb'),
    ('graph_delete_concept', 'uuid, bigint', 'jsonb'),
    ('graph_register_source', 'text, text, bigint', 'jsonb'),
    ('graph_delete_source', 'text, text, bigint', 'jsonb'),
    ('graph_create_mention', 'uuid, text, text, bigint', 'jsonb'),
    ('graph_delete_mention', 'uuid, text, text, bigint', 'jsonb'),
    ('graph_create_assertion', 'uuid, uuid, text, uuid, jsonb', 'jsonb'),
    ('graph_update_assertion', 'uuid, bigint, uuid, text, uuid', 'jsonb'),
    ('graph_delete_assertion', 'uuid, bigint', 'jsonb'),
    ('graph_add_assertion_evidence', 'uuid, text, text, bigint', 'jsonb'),
    ('graph_remove_assertion_evidence', 'uuid, text, text, bigint', 'jsonb'),
    ('graph_enforce_assertion_endpoint_order', '', 'trigger'),
    ('graph_enforce_assertion_evidence_invariant', '', 'trigger'),
    ('graph_enforce_concept_alias_invariant', '', 'trigger')
),
actual AS (
  SELECT
    routine.proname::text,
    COALESCE(pg_catalog.oidvectortypes(routine.proargtypes), ''),
    pg_catalog.pg_get_function_result(routine.oid)
  FROM pg_catalog.pg_proc AS routine
  WHERE routine.pronamespace = current_schema()::regnamespace
),
owner_executed AS (
  SELECT routine.*
  FROM pg_catalog.pg_proc AS routine
  WHERE routine.pronamespace = current_schema()::regnamespace
)
SELECT (SELECT count(*) = 22 FROM actual)
  AND NOT EXISTS (SELECT * FROM expected EXCEPT SELECT * FROM actual)
  AND NOT EXISTS (SELECT * FROM actual EXCEPT SELECT * FROM expected)
  AND NOT EXISTS (
    SELECT 1
    FROM owner_executed AS routine
    WHERE routine.proowner <> (
        SELECT role.oid
        FROM pg_catalog.pg_roles AS role
        WHERE role.rolname = current_user
      )
      OR NOT routine.prosecdef
      OR routine.proconfig IS DISTINCT FROM ARRAY['search_path=pg_catalog, pg_temp']::text[]
  )
SQL

assert_true 'PUBLIC privileges revoked from graph objects' <<'SQL'
WITH object_acl(acl_items, acl_kind, owner_oid) AS (
  SELECT schema.nspacl, 'n'::"char", schema.nspowner
  FROM pg_catalog.pg_namespace AS schema
  WHERE schema.nspname = current_schema()
  UNION ALL
  SELECT relation.relacl, 'r'::"char", relation.relowner
  FROM pg_catalog.pg_class AS relation
  WHERE relation.relnamespace = current_schema()::regnamespace
    AND relation.relkind = 'r'
  UNION ALL
  SELECT sequence.relacl, 'S'::"char", sequence.relowner
  FROM pg_catalog.pg_class AS sequence
  WHERE sequence.relnamespace = current_schema()::regnamespace
    AND sequence.relkind = 'S'
  UNION ALL
  SELECT routine.proacl, 'f'::"char", routine.proowner
  FROM pg_catalog.pg_proc AS routine
  WHERE routine.pronamespace = current_schema()::regnamespace
)
SELECT NOT EXISTS (
  SELECT 1
  FROM object_acl
  CROSS JOIN LATERAL pg_catalog.aclexplode(
    COALESCE(acl_items, pg_catalog.acldefault(acl_kind, owner_oid))
  ) AS privilege
  WHERE privilege.grantee = 0
)
SQL

assert_true 'application schema privilege set' <<'SQL'
WITH application_role AS (
  SELECT oid
  FROM pg_catalog.pg_roles
  WHERE rolname = 'knowledge_graph_application'
),
actual AS (
  SELECT privilege.privilege_type::text, privilege.is_grantable
  FROM pg_catalog.pg_namespace AS schema
  CROSS JOIN LATERAL pg_catalog.aclexplode(
    COALESCE(schema.nspacl, pg_catalog.acldefault('n', schema.nspowner))
  ) AS privilege
  WHERE schema.nspname = current_schema()
    AND privilege.grantee = (SELECT oid FROM application_role)
)
SELECT (SELECT count(*) = 1 FROM actual)
  AND EXISTS (SELECT 1 FROM actual WHERE privilege_type = 'USAGE' AND NOT is_grantable)
  AND pg_catalog.has_schema_privilege(
    (SELECT oid FROM application_role),
    current_schema(),
    'USAGE'
  )
  AND NOT pg_catalog.has_schema_privilege(
    (SELECT oid FROM application_role),
    current_schema(),
    'CREATE'
  )
SQL

assert_true 'application table SELECT grants' <<'SQL'
WITH expected(relname, privilege_type, is_grantable) AS (
  VALUES
    ('graph_relation_types', 'SELECT', false),
    ('graph_concepts', 'SELECT', false),
    ('graph_aliases', 'SELECT', false),
    ('graph_source_references', 'SELECT', false),
    ('graph_concept_mentions', 'SELECT', false),
    ('graph_assertions', 'SELECT', false),
    ('graph_assertion_evidence', 'SELECT', false)
),
application_role AS (
  SELECT oid
  FROM pg_catalog.pg_roles
  WHERE rolname = 'knowledge_graph_application'
),
actual AS (
  SELECT relation.relname::text, privilege.privilege_type::text, privilege.is_grantable
  FROM pg_catalog.pg_class AS relation
  CROSS JOIN LATERAL pg_catalog.aclexplode(
    COALESCE(relation.relacl, pg_catalog.acldefault('r', relation.relowner))
  ) AS privilege
  WHERE relation.relnamespace = current_schema()::regnamespace
    AND relation.relkind = 'r'
    AND privilege.grantee = (SELECT oid FROM application_role)
)
SELECT (SELECT count(*) = 7 FROM actual)
  AND NOT EXISTS (SELECT * FROM expected EXCEPT SELECT * FROM actual)
  AND NOT EXISTS (SELECT * FROM actual EXCEPT SELECT * FROM expected)
  AND NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_class AS relation
    WHERE relation.relnamespace = current_schema()::regnamespace
      AND relation.relkind = 'r'
      AND NOT pg_catalog.has_table_privilege(
        (SELECT oid FROM application_role), relation.oid, 'SELECT'
      )
  )
SQL

assert_true 'application direct DML and truncate privileges denied' <<'SQL'
SELECT NOT EXISTS (
  SELECT 1
  FROM pg_catalog.pg_class AS relation
  WHERE relation.relnamespace = current_schema()::regnamespace
    AND relation.relkind = 'r'
    AND (
      pg_catalog.has_table_privilege('knowledge_graph_application', relation.oid, 'INSERT')
      OR pg_catalog.has_table_privilege('knowledge_graph_application', relation.oid, 'UPDATE')
      OR pg_catalog.has_table_privilege('knowledge_graph_application', relation.oid, 'DELETE')
      OR pg_catalog.has_table_privilege('knowledge_graph_application', relation.oid, 'TRUNCATE')
      OR pg_catalog.has_table_privilege('knowledge_graph_application', relation.oid, 'REFERENCES')
      OR pg_catalog.has_table_privilege('knowledge_graph_application', relation.oid, 'TRIGGER')
    )
)
SQL

assert_true 'application has no sequence privileges' <<'SQL'
SELECT (SELECT count(*) = 1
        FROM pg_catalog.pg_class AS sequence
        WHERE sequence.relnamespace = current_schema()::regnamespace
          AND sequence.relkind = 'S')
  AND NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_class AS sequence
    WHERE sequence.relnamespace = current_schema()::regnamespace
      AND sequence.relkind = 'S'
      AND (
        pg_catalog.has_sequence_privilege('knowledge_graph_application', sequence.oid, 'USAGE')
        OR pg_catalog.has_sequence_privilege('knowledge_graph_application', sequence.oid, 'SELECT')
        OR pg_catalog.has_sequence_privilege('knowledge_graph_application', sequence.oid, 'UPDATE')
      )
  )
SQL

assert_true 'application mutation routine EXECUTE grant set' <<'SQL'
WITH expected(function_name, identity_types) AS (
  VALUES
    ('graph_create_concept', 'uuid, jsonb'),
    ('graph_replace_concept_aliases', 'uuid, bigint, jsonb'),
    ('graph_delete_concept', 'uuid, bigint'),
    ('graph_register_source', 'text, text, bigint'),
    ('graph_delete_source', 'text, text, bigint'),
    ('graph_create_mention', 'uuid, text, text, bigint'),
    ('graph_delete_mention', 'uuid, text, text, bigint'),
    ('graph_create_assertion', 'uuid, uuid, text, uuid, jsonb'),
    ('graph_update_assertion', 'uuid, bigint, uuid, text, uuid'),
    ('graph_delete_assertion', 'uuid, bigint'),
    ('graph_add_assertion_evidence', 'uuid, text, text, bigint'),
    ('graph_remove_assertion_evidence', 'uuid, text, text, bigint')
),
application_role AS (
  SELECT oid
  FROM pg_catalog.pg_roles
  WHERE rolname = 'knowledge_graph_application'
),
effective AS (
  SELECT routine.proname::text, COALESCE(pg_catalog.oidvectortypes(routine.proargtypes), '')
  FROM pg_catalog.pg_proc AS routine
  WHERE routine.pronamespace = current_schema()::regnamespace
    AND pg_catalog.has_function_privilege(
      (SELECT oid FROM application_role), routine.oid, 'EXECUTE'
    )
),
direct_grants AS (
  SELECT routine.proname::text, COALESCE(pg_catalog.oidvectortypes(routine.proargtypes), '')
  FROM pg_catalog.pg_proc AS routine
  CROSS JOIN LATERAL pg_catalog.aclexplode(
    COALESCE(routine.proacl, pg_catalog.acldefault('f', routine.proowner))
  ) AS privilege
  WHERE routine.pronamespace = current_schema()::regnamespace
    AND privilege.grantee = (SELECT oid FROM application_role)
    AND privilege.privilege_type = 'EXECUTE'
    AND NOT privilege.is_grantable
)
SELECT (SELECT count(*) = 12 FROM effective)
  AND (SELECT count(*) = 12 FROM direct_grants)
  AND NOT EXISTS (SELECT * FROM expected EXCEPT SELECT * FROM effective)
  AND NOT EXISTS (SELECT * FROM effective EXCEPT SELECT * FROM expected)
  AND NOT EXISTS (SELECT * FROM expected EXCEPT SELECT * FROM direct_grants)
  AND NOT EXISTS (SELECT * FROM direct_grants EXCEPT SELECT * FROM expected)
SQL

assert_true 'migration owner role and SET ROLE denial' <<'SQL'
SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_roles AS migration_role
    WHERE migration_role.rolname = 'knowledge_graph_migration_owner'
      AND NOT migration_role.rolcanlogin
      AND NOT migration_role.rolsuper
  )
  AND NOT pg_catalog.pg_has_role(
    'knowledge_graph_application',
    'knowledge_graph_migration_owner',
    'SET'
  )
SQL

expect_equal \
	"$(read_query_as "$application_role" 'SELECT count(*) FROM knowledge_graph.graph_relation_types')" \
	'9' \
	'application relation-vocabulary read'
run_denied_as "$application_role" 'application migration-role escalation' \
	"SET ROLE $migration_role"
run_denied_as "$application_role" 'application direct INSERT' \
	"BEGIN; INSERT INTO knowledge_graph.graph_source_references (source_ref_id, source_kind, external_id, external_version) OVERRIDING SYSTEM VALUE VALUES (9223372036854770000, 'memory_version', 'schema-verifier-direct-insert', 1); ROLLBACK;"
run_denied_as "$application_role" 'application direct UPDATE' \
	'BEGIN; UPDATE knowledge_graph.graph_aliases SET display_text = display_text WHERE false; ROLLBACK;'
run_denied_as "$application_role" 'application direct DELETE' \
	'BEGIN; DELETE FROM knowledge_graph.graph_aliases WHERE false; ROLLBACK;'
run_denied_as "$application_role" 'application direct TRUNCATE' \
	'BEGIN; TRUNCATE knowledge_graph.graph_aliases; ROLLBACK;'
run_denied_as "$application_role" 'application lock-helper execution' \
	"SELECT knowledge_graph.graph_lock_key_concept_id('018f0000-0000-7000-8000-000000000001'::uuid)"

printf 'schema catalog=tables columns relations constraints foreign-keys indexes sequences routines triggers verified\n'
printf 'schema security=owners fixed-search-path public-revokes application-grants direct-dml-and-helper-denial verified\n'
printf 'schema timeout-contract=adapter-query:%ss postgres-statement:%ss iii-invocation:%ss\n' \
	"$adapter_query_timeout_seconds" "$statement_timeout_seconds" "$invocation_timeout_seconds"
printf 'schema worker-prerequisite=%s declared worker=not-started\n' "$KNOWLEDGE_GRAPH_ROW_CHANGE_MODE"

if ! bash "$rollback_script" >/dev/null 2>&1; then
	fail 'clean graph schema rollback failed'
fi
schema_applied=false
assert_schema_absent 'graph schema absence after clean rollback'

printf 'schema rollback=completed graph-schema=absent\n'
printf 'schema verification=passed major=%s\n' "$expected_major"
