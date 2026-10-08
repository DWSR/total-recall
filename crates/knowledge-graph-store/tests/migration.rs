const MIGRATION: &str = include_str!("../migrations/0001_knowledge_graph_foundation.sql");

const GRAPH_FUNCTION_SIGNATURES: &[&str] = &[
    "knowledge_graph.graph_lock_key_concept_id(uuid)",
    "knowledge_graph.graph_lock_key_normalized_alias(text)",
    "knowledge_graph.graph_lock_key_source_identity(text, text, bigint)",
    "knowledge_graph.graph_lock_key_concept_mention(uuid, text, text, bigint)",
    "knowledge_graph.graph_lock_key_assertion_id(uuid)",
    "knowledge_graph.graph_lock_key_semantic_assertion(text, uuid, uuid)",
    "knowledge_graph.graph_acquire_mutation_locks(jsonb, uuid[], uuid[], bigint[])",
    "knowledge_graph.graph_create_concept(uuid, jsonb)",
    "knowledge_graph.graph_replace_concept_aliases(uuid, bigint, jsonb)",
    "knowledge_graph.graph_delete_concept(uuid, bigint)",
    "knowledge_graph.graph_register_source(text, text, bigint)",
    "knowledge_graph.graph_delete_source(text, text, bigint)",
    "knowledge_graph.graph_create_mention(uuid, text, text, bigint)",
    "knowledge_graph.graph_delete_mention(uuid, text, text, bigint)",
    "knowledge_graph.graph_create_assertion(uuid, uuid, text, uuid, jsonb)",
    "knowledge_graph.graph_update_assertion(uuid, bigint, uuid, text, uuid)",
    "knowledge_graph.graph_delete_assertion(uuid, bigint)",
    "knowledge_graph.graph_add_assertion_evidence(uuid, text, text, bigint)",
    "knowledge_graph.graph_remove_assertion_evidence(uuid, text, text, bigint)",
    "knowledge_graph.graph_enforce_assertion_endpoint_order()",
    "knowledge_graph.graph_enforce_assertion_evidence_invariant()",
    "knowledge_graph.graph_enforce_concept_alias_invariant()",
];

const APPLICATION_MUTATION_FUNCTIONS: &[&str] = &[
    "knowledge_graph.graph_create_concept(uuid, jsonb)",
    "knowledge_graph.graph_replace_concept_aliases(uuid, bigint, jsonb)",
    "knowledge_graph.graph_delete_concept(uuid, bigint)",
    "knowledge_graph.graph_register_source(text, text, bigint)",
    "knowledge_graph.graph_delete_source(text, text, bigint)",
    "knowledge_graph.graph_create_mention(uuid, text, text, bigint)",
    "knowledge_graph.graph_delete_mention(uuid, text, text, bigint)",
    "knowledge_graph.graph_create_assertion(uuid, uuid, text, uuid, jsonb)",
    "knowledge_graph.graph_update_assertion(uuid, bigint, uuid, text, uuid)",
    "knowledge_graph.graph_delete_assertion(uuid, bigint)",
    "knowledge_graph.graph_add_assertion_evidence(uuid, text, text, bigint)",
    "knowledge_graph.graph_remove_assertion_evidence(uuid, text, text, bigint)",
];

const GRAPH_TABLE_NAMES: &[&str] = &[
    "GRAPH_RELATION_TYPES",
    "GRAPH_CONCEPTS",
    "GRAPH_ALIASES",
    "GRAPH_SOURCE_REFERENCES",
    "GRAPH_CONCEPT_MENTIONS",
    "GRAPH_ASSERTIONS",
    "GRAPH_ASSERTION_EVIDENCE",
];

fn executable_sql() -> String {
    MIGRATION
        .lines()
        .map(|line| {
            line.split_once("--")
                .map_or(line, |(statement, _)| statement)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn compact_sql(sql: &str) -> String {
    let sql = sql
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_uppercase();
    sql.replace("( ", "(").replace(" )", ")")
}

fn assert_contains_sql(sql: &str, expected_fragment: &str) {
    let sql = compact_sql(sql);
    let expected_fragment = compact_sql(expected_fragment);
    assert!(
        sql.contains(&expected_fragment),
        "expected SQL fragment:\n{expected_fragment}\n\nSQL was:\n{sql}"
    );
}

fn table_definition(table: &str) -> String {
    let sql = compact_sql(&executable_sql());
    let marker = format!("CREATE TABLE KNOWLEDGE_GRAPH.{table} (");
    let start = sql
        .find(&marker)
        .unwrap_or_else(|| panic!("missing table definition for {table}"));
    let definition = &sql[start..];
    let end = definition
        .find(");")
        .unwrap_or_else(|| panic!("unterminated table definition for {table}"));

    definition[..end + 2].to_owned()
}

fn function_definition(signature: &str) -> String {
    let sql = compact_sql(&executable_sql());
    let marker = format!("CREATE FUNCTION KNOWLEDGE_GRAPH.{signature}");
    let start = sql
        .find(&marker)
        .unwrap_or_else(|| panic!("missing function definition for {signature}"));
    let definition = &sql[start..];
    let end = definition
        .find("$FUNCTION$;")
        .unwrap_or_else(|| panic!("unterminated function definition for {signature}"));

    definition[..end + "$FUNCTION$;".len()].to_owned()
}

fn function_definitions() -> Vec<String> {
    compact_sql(&executable_sql())
        .split("CREATE FUNCTION ")
        .skip(1)
        .map(|definition| {
            let end = definition
                .find("$FUNCTION$;")
                .unwrap_or_else(|| panic!("unterminated function definition: {definition}"));
            format!(
                "CREATE FUNCTION {}",
                &definition[..end + "$FUNCTION$;".len()]
            )
        })
        .collect()
}

fn function_identity(definition: &str) -> String {
    let definition = compact_sql(definition);
    let signature_start = "CREATE FUNCTION ".len();
    let signature_end = definition
        .find(" RETURNS ")
        .expect("function definition should declare its return type");
    let signature = &definition[signature_start..signature_end];
    let open_paren = signature
        .find('(')
        .expect("function signature should include its argument list");
    let function_name = &signature[..open_paren];
    let arguments = &signature[open_paren + 1..signature.len() - 1];
    let argument_types = if arguments.is_empty() {
        String::new()
    } else {
        arguments
            .split(", ")
            .map(|argument| {
                argument
                    .split_whitespace()
                    .last()
                    .expect("function arguments should include a type")
                    .to_ascii_lowercase()
            })
            .collect::<Vec<_>>()
            .join(", ")
    };

    format!("{}({argument_types})", function_name.to_ascii_lowercase())
}

fn sql_statements_with_prefix(sql: &str, prefix: &str) -> Vec<String> {
    let sql = compact_sql(sql);
    let mut statements = Vec::new();
    let mut search_from = 0;

    while let Some(relative_start) = sql[search_from..].find(prefix) {
        let start = search_from + relative_start;
        let statement = &sql[start..];
        let end = statement
            .find(';')
            .unwrap_or_else(|| panic!("unterminated SQL statement starting with {prefix}"));
        statements.push(statement[..=end].to_owned());
        search_from = start + end + 1;
    }

    statements
}

fn outcome_returns(function: &str, outcome: &str) -> Vec<String> {
    let function = compact_sql(function);
    let marker = compact_sql(&format!("'outcome', '{outcome}'"));
    let mut returns = Vec::new();
    let mut search_from = 0;

    while let Some(relative_marker_start) = function[search_from..].find(&marker) {
        let marker_start = search_from + relative_marker_start;
        let return_start = function[..marker_start]
            .rfind("RETURN PG_CATALOG.JSONB_BUILD_OBJECT(")
            .unwrap_or_else(|| panic!("missing return object for outcome {outcome}"));
        let open_paren = function[return_start..]
            .find('(')
            .map(|relative| return_start + relative)
            .expect("return object should open with a parenthesis");
        let bytes = function.as_bytes();
        let mut depth = 0;
        let mut in_string = false;
        let mut closed = false;
        let mut cursor = open_paren;

        while cursor < bytes.len() {
            match bytes[cursor] {
                b'\'' if in_string && bytes.get(cursor + 1) == Some(&b'\'') => cursor += 1,
                b'\'' => in_string = !in_string,
                b'(' if !in_string => depth += 1,
                b')' if !in_string => {
                    depth -= 1;
                    if depth == 0 {
                        returns.push(function[return_start..=cursor].to_owned());
                        search_from = cursor + 1;
                        closed = true;
                        break;
                    }
                }
                _ => {}
            }
            cursor += 1;
        }

        assert!(closed, "return object for outcome {outcome} should close");
    }

    returns
}

fn assert_source_identity_omitted(function: &str, outcome: &str) {
    let returns = outcome_returns(function, outcome);
    assert!(!returns.is_empty(), "{outcome} outcome should be returned");

    for returned in returns {
        for source_identity_field in [
            "SOURCE_KIND",
            "EXTERNAL_ID",
            "EXTERNAL_VERSION",
            "SOURCE_REF_ID",
        ] {
            assert!(
                !returned.contains(source_identity_field),
                "{outcome} outcome must not return {source_identity_field}: {returned}"
            );
        }
    }
}

fn assert_typed_mutation_function(function: &str, outcomes: &[&str]) {
    let function = compact_sql(function);
    assert_contains_sql(&function, "RETURNS JSONB");
    assert_contains_sql(&function, "SECURITY DEFINER");
    assert_contains_sql(&function, "SET SEARCH_PATH = PG_CATALOG, PG_TEMP");

    let return_count = function.matches("RETURN ").count();
    assert!(return_count > 0, "mutation should return an outcome");
    assert_eq!(
        function
            .matches("RETURN PG_CATALOG.JSONB_BUILD_OBJECT(")
            .count(),
        return_count,
        "every return path should build one outcome object"
    );
    assert_eq!(
        function.matches("'OUTCOME', '").count(),
        return_count,
        "every return path should have exactly one outcome code"
    );
    for outcome in outcomes {
        assert_contains_sql(&function, &format!("'outcome', '{outcome}'"));
    }
    assert!(
        !function.contains("RETURN NEXT ")
            && !function.contains("RETURN QUERY ")
            && !function.contains("SQLERRM")
            && !function.contains("GET STACKED DIAGNOSTICS")
            && !function.contains("RAISE EXCEPTION")
            && !function.contains("EXECUTE "),
        "mutation outcomes should be singular, static, and independent of backend diagnostics"
    );
}

#[test]
fn migration_creates_dedicated_schema_before_schema_qualified_objects() {
    let migration = compact_sql(&executable_sql());
    let schema_creation = migration
        .find("CREATE SCHEMA KNOWLEDGE_GRAPH AUTHORIZATION CURRENT_USER;")
        .expect("migration should create its dedicated schema for the current role");
    let first_schema_qualified_object = migration
        .find("KNOWLEDGE_GRAPH.")
        .expect("migration should define schema-qualified graph objects");

    assert!(
        schema_creation < first_schema_qualified_object,
        "migration should create its schema before referencing schema-qualified objects"
    );
}

#[test]
fn migration_seeds_exact_relation_vocabulary_and_symmetry() {
    let sql = executable_sql();
    let seed_start = sql
        .find("INSERT INTO knowledge_graph.graph_relation_types")
        .expect("relation vocabulary seed insert should exist");
    let seed = &sql[seed_start..];
    let seed_end = seed.find(';').expect("relation seed insert should end");
    let seed = &seed[..=seed_end];

    assert_eq!(
        compact_sql(seed),
        compact_sql(
            "INSERT INTO knowledge_graph.graph_relation_types (code, is_symmetric) VALUES \
             ('related_to', TRUE), ('is_a', FALSE), ('part_of', FALSE), \
             ('depends_on', FALSE), ('uses', FALSE), ('implements', FALSE), \
             ('causes', FALSE), ('resolves', FALSE), ('contradicts', TRUE);"
        )
    );

    let relation_types = table_definition("GRAPH_RELATION_TYPES");
    assert_contains_sql(&relation_types, "code text COLLATE \"C\" NOT NULL");
    assert_contains_sql(&relation_types, "is_symmetric boolean NOT NULL");
    assert_contains_sql(&relation_types, "PRIMARY KEY (code)");
    assert_contains_sql(
        &relation_types,
        "CONSTRAINT graph_relation_types_code_check CHECK (code IN ( \
         'related_to', 'is_a', 'part_of', 'depends_on', 'uses', 'implements', \
         'causes', 'resolves', 'contradicts' ))",
    );
}

#[test]
fn migration_enforces_concept_and_alias_identity_with_deferred_preference() {
    let concepts = table_definition("GRAPH_CONCEPTS");
    assert_contains_sql(&concepts, "id uuid NOT NULL");
    assert_contains_sql(&concepts, "revision bigint NOT NULL DEFAULT 1");
    assert_contains_sql(
        &concepts,
        "CONSTRAINT graph_concepts_revision_positive CHECK (revision > 0)",
    );
    assert_contains_sql(&concepts, "pg_catalog.substring(id::text, 15, 1) = '7'");
    assert_contains_sql(
        &concepts,
        "pg_catalog.substring(id::text, 20, 1) IN ('8', '9', 'a', 'b')",
    );
    assert_contains_sql(&concepts, "PRIMARY KEY (id)");

    let aliases = table_definition("GRAPH_ALIASES");
    assert_contains_sql(&aliases, "alias_key text COLLATE \"C\" NOT NULL");
    assert_contains_sql(&aliases, "display_text text NOT NULL");
    assert_contains_sql(&aliases, "is_preferred boolean NOT NULL");
    assert_contains_sql(
        &aliases,
        "normalization_version text COLLATE \"C\" NOT NULL",
    );
    assert_contains_sql(&aliases, "PRIMARY KEY (alias_key)");
    assert_contains_sql(
        &aliases,
        "CHECK (alias_key <> '' AND pg_catalog.octet_length(alias_key) BETWEEN 1 AND 2048)",
    );
    assert_contains_sql(
        &aliases,
        "CHECK (display_text <> '' AND pg_catalog.octet_length(display_text) BETWEEN 1 AND 512)",
    );
    assert_contains_sql(
        &aliases,
        "CHECK (normalization_version = 'icu4x-2.3.0-nfkc-fold-v1')",
    );
    assert_contains_sql(
        &aliases,
        "FOREIGN KEY (concept_id) REFERENCES knowledge_graph.graph_concepts (id) \
         ON UPDATE RESTRICT ON DELETE CASCADE NOT DEFERRABLE",
    );

    let migration = executable_sql();
    assert_contains_sql(
        &migration,
        "CREATE UNIQUE INDEX graph_aliases_one_preferred_per_concept_uidx \
         ON knowledge_graph.graph_aliases (concept_id) WHERE is_preferred",
    );

    let function = compact_sql(&migration);
    let function_start = function
        .find("CREATE FUNCTION KNOWLEDGE_GRAPH.GRAPH_ENFORCE_CONCEPT_ALIAS_INVARIANT()")
        .expect("deferred alias-invariant helper should exist");
    let function_end = function[function_start..]
        .find("$FUNCTION$;")
        .expect("deferred alias-invariant helper should terminate");
    let function = &function[function_start..function_start + function_end + "$FUNCTION$;".len()];
    assert_contains_sql(function, "SECURITY DEFINER");
    assert_contains_sql(function, "SET SEARCH_PATH = PG_CATALOG, PG_TEMP");
    assert_contains_sql(function, "FROM KNOWLEDGE_GRAPH.GRAPH_CONCEPTS");
    assert_contains_sql(function, "FROM KNOWLEDGE_GRAPH.GRAPH_ALIASES");
    assert_contains_sql(
        function,
        "IF EXISTS (SELECT 1 FROM knowledge_graph.graph_concepts AS concepts \
         WHERE concepts.id = concept_id_to_check) THEN",
    );
    assert_contains_sql(
        function,
        "affected_concept_ids := ARRAY[OLD.concept_id, NEW.concept_id]",
    );
    assert_contains_sql(function, "IF PREFERRED_ALIAS_COUNT <> 1 THEN");

    assert_contains_sql(
        &migration,
        "CREATE CONSTRAINT TRIGGER graph_concepts_preferred_alias_invariant \
         AFTER INSERT OR UPDATE OR DELETE ON knowledge_graph.graph_concepts \
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW \
         EXECUTE FUNCTION knowledge_graph.graph_enforce_concept_alias_invariant()",
    );
    assert_contains_sql(
        &migration,
        "CREATE CONSTRAINT TRIGGER graph_aliases_preferred_alias_invariant \
         AFTER INSERT OR UPDATE OR DELETE ON knowledge_graph.graph_aliases \
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW \
         EXECUTE FUNCTION knowledge_graph.graph_enforce_concept_alias_invariant()",
    );
}

#[test]
fn migration_stores_typed_opaque_sources_and_restrictive_mentions() {
    let sources = table_definition("GRAPH_SOURCE_REFERENCES");
    assert_contains_sql(
        &sources,
        "source_ref_id bigint GENERATED ALWAYS AS IDENTITY",
    );
    assert_contains_sql(&sources, "source_kind text COLLATE \"C\" NOT NULL");
    assert_contains_sql(&sources, "external_id text COLLATE \"C\" NOT NULL");
    assert_contains_sql(&sources, "external_version bigint");
    assert_contains_sql(
        &sources,
        "CHECK (source_kind IN ('memory_version', 'session_record'))",
    );
    assert_contains_sql(
        &sources,
        "CHECK ((source_kind = 'memory_version' AND external_version IS NOT NULL \
         AND external_version > 0) OR (source_kind = 'session_record' \
         AND external_version IS NULL))",
    );
    assert_contains_sql(
        &sources,
        "CHECK (external_id <> '' AND pg_catalog.octet_length(external_id) BETWEEN 1 AND 2048)",
    );
    assert_contains_sql(
        &sources,
        "UNIQUE NULLS NOT DISTINCT (source_kind, external_id, external_version)",
    );
    assert!(!sources.contains("FOREIGN KEY"));
    assert!(!sources.contains("REFERENCES KNOWLEDGE_GRAPH."));
    assert!(!sources.contains("CONTENT"));
    assert!(!sources.contains("PAYLOAD"));

    let mentions = table_definition("GRAPH_CONCEPT_MENTIONS");
    assert_contains_sql(&mentions, "PRIMARY KEY (concept_id, source_ref_id)");
    assert_contains_sql(
        &mentions,
        "FOREIGN KEY (concept_id) REFERENCES knowledge_graph.graph_concepts (id) \
         ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE",
    );
    assert_contains_sql(
        &mentions,
        "FOREIGN KEY (source_ref_id) \
         REFERENCES knowledge_graph.graph_source_references (source_ref_id) \
         ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE",
    );

    let migration = compact_sql(&executable_sql());
    assert!(migration.contains("PG_CATALOG.CURRENT_SETTING('SERVER_ENCODING') <> 'UTF8'"));
    assert!(!migration.contains("PUBLIC.MEMORIES"));
    assert!(!migration.contains("PUBLIC.SESSIONS"));
}

#[test]
fn migration_persists_semantic_assertions_with_direction_and_canonical_identity() {
    let assertions = table_definition("GRAPH_ASSERTIONS");
    assert_contains_sql(&assertions, "id uuid NOT NULL");
    assert_contains_sql(&assertions, "subject_concept_id uuid NOT NULL");
    assert_contains_sql(&assertions, "relation_code text COLLATE \"C\" NOT NULL");
    assert_contains_sql(&assertions, "object_concept_id uuid NOT NULL");
    assert_contains_sql(&assertions, "revision bigint NOT NULL DEFAULT 1");
    assert_contains_sql(&assertions, "PRIMARY KEY (id)");
    assert_contains_sql(
        &assertions,
        "CONSTRAINT graph_assertions_revision_positive CHECK (revision > 0)",
    );
    assert_contains_sql(&assertions, "pg_catalog.substring(id::text, 15, 1) = '7'");
    assert_contains_sql(
        &assertions,
        "pg_catalog.substring(id::text, 20, 1) IN ('8', '9', 'a', 'b')",
    );
    assert_contains_sql(
        &assertions,
        "CONSTRAINT graph_assertions_distinct_endpoints CHECK (subject_concept_id <> object_concept_id)",
    );
    assert_contains_sql(
        &assertions,
        "CONSTRAINT graph_assertions_semantic_identity UNIQUE (subject_concept_id, relation_code, object_concept_id)",
    );
    assert_contains_sql(
        &assertions,
        "FOREIGN KEY (subject_concept_id) REFERENCES knowledge_graph.graph_concepts (id) \
         ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE",
    );
    assert_contains_sql(
        &assertions,
        "FOREIGN KEY (object_concept_id) REFERENCES knowledge_graph.graph_concepts (id) \
         ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE",
    );
    assert_contains_sql(
        &assertions,
        "FOREIGN KEY (relation_code) REFERENCES knowledge_graph.graph_relation_types (code) \
         ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE",
    );

    let migration = executable_sql();
    assert_contains_sql(
        &migration,
        "CREATE INDEX graph_assertions_subject_adjacency_idx \
         ON knowledge_graph.graph_assertions \
         (subject_concept_id ASC, relation_code ASC, object_concept_id ASC, id ASC)",
    );
    assert_contains_sql(
        &migration,
        "CREATE INDEX graph_assertions_object_adjacency_idx \
         ON knowledge_graph.graph_assertions \
         (object_concept_id ASC, relation_code ASC, subject_concept_id ASC, id ASC)",
    );

    let function = function_definition("GRAPH_ENFORCE_ASSERTION_ENDPOINT_ORDER()");
    assert_contains_sql(&function, "SECURITY DEFINER");
    assert_contains_sql(&function, "SET SEARCH_PATH = PG_CATALOG, PG_TEMP");
    assert_contains_sql(
        &function,
        "SELECT relation_types.is_symmetric INTO relation_is_symmetric \
         FROM knowledge_graph.graph_relation_types AS relation_types \
         WHERE relation_types.code = NEW.relation_code",
    );
    assert_contains_sql(
        &function,
        "IF relation_is_symmetric AND NEW.subject_concept_id > NEW.object_concept_id THEN",
    );
    assert_contains_sql(
        &function,
        "RAISE EXCEPTION 'symmetric assertion endpoints must be canonical' USING ERRCODE = '23514'",
    );
    assert!(
        !function.contains("NEW.SUBJECT_CONCEPT_ID :=")
            && !function.contains("NEW.OBJECT_CONCEPT_ID :="),
        "the database must reject noncanonical symmetric endpoints rather than rewrite them"
    );
    assert_contains_sql(
        &migration,
        "CREATE TRIGGER graph_assertions_canonical_symmetric_endpoints \
         BEFORE INSERT OR UPDATE OF subject_concept_id, relation_code, object_concept_id \
         ON knowledge_graph.graph_assertions FOR EACH ROW \
         EXECUTE FUNCTION knowledge_graph.graph_enforce_assertion_endpoint_order()",
    );
}

#[test]
fn migration_persists_assertion_evidence_as_restrictive_source_associations() {
    let evidence = table_definition("GRAPH_ASSERTION_EVIDENCE");
    assert_contains_sql(&evidence, "assertion_id uuid NOT NULL");
    assert_contains_sql(&evidence, "source_ref_id bigint NOT NULL");
    assert_contains_sql(&evidence, "PRIMARY KEY (assertion_id, source_ref_id)");
    assert_contains_sql(
        &evidence,
        "FOREIGN KEY (assertion_id) REFERENCES knowledge_graph.graph_assertions (id) \
         ON UPDATE RESTRICT ON DELETE CASCADE NOT DEFERRABLE",
    );
    assert_contains_sql(
        &evidence,
        "FOREIGN KEY (source_ref_id) \
         REFERENCES knowledge_graph.graph_source_references (source_ref_id) \
         ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE",
    );
    for forbidden_column in [
        "CONTENT",
        "EXCERPT",
        "CONFIDENCE",
        "TIMESTAMP",
        "PROPERTIES",
        "PAYLOAD",
    ] {
        assert!(
            !evidence.contains(forbidden_column),
            "assertion evidence must not contain {forbidden_column}"
        );
    }

    assert_contains_sql(
        &executable_sql(),
        "CREATE INDEX graph_assertion_evidence_source_idx \
         ON knowledge_graph.graph_assertion_evidence (source_ref_id ASC, assertion_id ASC)",
    );
}

#[test]
fn migration_defers_the_required_evidence_invariant_to_transaction_completion() {
    let migration = executable_sql();
    let function = function_definition("GRAPH_ENFORCE_ASSERTION_EVIDENCE_INVARIANT()");
    assert_contains_sql(&function, "SECURITY DEFINER");
    assert_contains_sql(&function, "SET SEARCH_PATH = PG_CATALOG, PG_TEMP");
    assert_contains_sql(
        &function,
        "FROM pg_catalog.unnest(affected_assertion_ids) AS requested(assertion_id)",
    );
    assert_contains_sql(
        &function,
        "IF EXISTS (SELECT 1 FROM knowledge_graph.graph_assertions AS assertions \
         WHERE assertions.id = assertion_id_to_check) \
         AND NOT EXISTS (SELECT 1 FROM knowledge_graph.graph_assertion_evidence AS evidence \
         WHERE evidence.assertion_id = assertion_id_to_check) THEN",
    );
    assert_contains_sql(
        &function,
        "RAISE EXCEPTION 'graph assertion evidence invariant violated' USING ERRCODE = '23514'",
    );
    assert_contains_sql(
        &function,
        "ELSIF TG_TABLE_NAME = 'GRAPH_ASSERTIONS' THEN \
         IF TG_OP = 'INSERT' THEN affected_assertion_ids := ARRAY[NEW.ID]; \
         ELSIF TG_OP = 'UPDATE' THEN affected_assertion_ids := ARRAY[OLD.ID, NEW.ID]",
    );
    assert_contains_sql(
        &function,
        "ELSIF TG_TABLE_NAME = 'GRAPH_ASSERTION_EVIDENCE' THEN \
         IF TG_OP = 'INSERT' THEN affected_assertion_ids := ARRAY[NEW.ASSERTION_ID]; \
         ELSIF TG_OP = 'UPDATE' THEN \
         affected_assertion_ids := ARRAY[OLD.ASSERTION_ID, NEW.ASSERTION_ID]",
    );
    assert_contains_sql(
        &migration,
        "CREATE CONSTRAINT TRIGGER graph_assertions_evidence_invariant \
         AFTER INSERT OR UPDATE OR DELETE ON knowledge_graph.graph_assertions \
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW \
         EXECUTE FUNCTION knowledge_graph.graph_enforce_assertion_evidence_invariant()",
    );
    assert_contains_sql(
        &migration,
        "CREATE CONSTRAINT TRIGGER graph_assertion_evidence_invariant \
         AFTER INSERT OR UPDATE OR DELETE ON knowledge_graph.graph_assertion_evidence \
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW \
         EXECUTE FUNCTION knowledge_graph.graph_enforce_assertion_evidence_invariant()",
    );
}

#[test]
fn migration_adds_deterministic_alias_lookup_and_source_reverse_indexes() {
    let migration = executable_sql();
    assert_contains_sql(
        &migration,
        "CREATE INDEX graph_aliases_concept_order_idx \
         ON knowledge_graph.graph_aliases \
         (concept_id ASC, is_preferred DESC, alias_key ASC)",
    );
    assert_contains_sql(
        &migration,
        "CREATE INDEX graph_concept_mentions_source_idx \
         ON knowledge_graph.graph_concept_mentions (source_ref_id ASC, concept_id ASC)",
    );
}

#[test]
fn migration_revokes_public_and_application_access_before_granting_reads_and_mutations() {
    let migration = executable_sql();
    assert_contains_sql(
        &migration,
        "REVOKE ALL ON SCHEMA knowledge_graph \
         FROM PUBLIC, knowledge_graph_application",
    );
    assert_contains_sql(
        &migration,
        "REVOKE ALL ON TABLE knowledge_graph.graph_relation_types, \
         knowledge_graph.graph_concepts, knowledge_graph.graph_aliases, \
         knowledge_graph.graph_source_references, knowledge_graph.graph_concept_mentions, \
         knowledge_graph.graph_assertions, knowledge_graph.graph_assertion_evidence \
         FROM PUBLIC, knowledge_graph_application",
    );
    assert_contains_sql(
        &migration,
        "REVOKE ALL ON ALL SEQUENCES IN SCHEMA knowledge_graph \
         FROM PUBLIC, knowledge_graph_application",
    );
    assert_contains_sql(
        &migration,
        "REVOKE ALL ON FUNCTION \
         knowledge_graph.graph_enforce_concept_alias_invariant() \
         FROM PUBLIC, knowledge_graph_application",
    );
    assert_contains_sql(
        &migration,
        "REVOKE ALL ON FUNCTION \
         knowledge_graph.graph_enforce_assertion_endpoint_order() \
         FROM PUBLIC, knowledge_graph_application",
    );
    assert_contains_sql(
        &migration,
        "REVOKE ALL ON FUNCTION \
         knowledge_graph.graph_enforce_assertion_evidence_invariant() \
         FROM PUBLIC, knowledge_graph_application",
    );

    let normalized = compact_sql(&migration);
    let first_grant = normalized
        .find("GRANT ")
        .expect("application access should be granted after the privilege revokes");
    let last_revoke = normalized
        .rfind("REVOKE ")
        .expect("graph privileges should be revoked before any grant");
    assert!(
        last_revoke < first_grant,
        "all graph privilege revokes should precede application grants"
    );
}

#[test]
fn migration_runs_every_graph_routine_with_owner_security_and_a_fixed_search_path() {
    let migration = compact_sql(&executable_sql());
    assert_contains_sql(
        &migration,
        "CREATE SCHEMA knowledge_graph AUTHORIZATION CURRENT_USER",
    );
    assert!(
        !migration.contains("SET ROLE ") && !migration.contains("OWNER TO "),
        "graph objects should retain the migration executor as owner"
    );

    let functions = function_definitions();
    assert_eq!(
        functions.len(),
        GRAPH_FUNCTION_SIGNATURES.len(),
        "the security contract should cover every graph routine"
    );
    let mut actual_function_signatures = functions
        .iter()
        .map(|function| function_identity(function))
        .collect::<Vec<_>>();
    let mut expected_function_signatures = GRAPH_FUNCTION_SIGNATURES
        .iter()
        .map(|signature| (*signature).to_owned())
        .collect::<Vec<_>>();
    actual_function_signatures.sort();
    expected_function_signatures.sort();
    assert_eq!(
        actual_function_signatures, expected_function_signatures,
        "the security contract should name every graph routine signature exactly"
    );

    for function in functions {
        assert!(
            function.starts_with("CREATE FUNCTION KNOWLEDGE_GRAPH."),
            "routine creation must use the graph schema explicitly: {function}"
        );
        assert_contains_sql(&function, "SECURITY DEFINER");
        assert_contains_sql(&function, "SET SEARCH_PATH = PG_CATALOG, PG_TEMP");
        assert_eq!(
            function.matches("SET SEARCH_PATH =").count(),
            1,
            "each routine must have exactly one fixed search path"
        );
        assert!(
            !function.contains("EXECUTE "),
            "graph routines must not use dynamic SQL or caller-controlled identifiers: {function}"
        );

        for table_name in GRAPH_TABLE_NAMES {
            for clause in ["FROM", "JOIN", "INSERT INTO", "UPDATE", "DELETE FROM"] {
                assert!(
                    !function.contains(&format!("{clause} {table_name}")),
                    "graph table references must be schema-qualified: {clause} {table_name}"
                );
            }
        }

        for signature in GRAPH_FUNCTION_SIGNATURES {
            let function_name = signature
                .strip_prefix("knowledge_graph.")
                .expect("graph routine signature should be schema-qualified")
                .split('(')
                .next()
                .expect("graph routine signature should contain a name")
                .to_ascii_uppercase();
            let call = format!("{function_name}(");
            let mut search_from = 0;
            while let Some(relative_call_start) = function[search_from..].find(&call) {
                let call_start = search_from + relative_call_start;
                assert!(
                    function[..call_start]
                        .trim_end()
                        .ends_with("KNOWLEDGE_GRAPH."),
                    "graph routine calls must be schema-qualified: {function_name}"
                );
                search_from = call_start + call.len();
            }
        }
    }
}

#[test]
fn migration_revokes_every_graph_routine_and_grants_only_the_mutation_api() {
    let migration = executable_sql();
    let mut expected_function_revokes = GRAPH_FUNCTION_SIGNATURES
        .iter()
        .map(|signature| {
            format!("REVOKE ALL ON FUNCTION {signature} FROM PUBLIC, knowledge_graph_application;")
        })
        .map(|statement| compact_sql(&statement))
        .collect::<Vec<_>>();
    let mut actual_function_revokes =
        sql_statements_with_prefix(&migration, "REVOKE ALL ON FUNCTION");
    expected_function_revokes.sort();
    actual_function_revokes.sort();
    assert_eq!(
        actual_function_revokes, expected_function_revokes,
        "every graph routine, including lock and trigger helpers, must be revoked from PUBLIC and the application role"
    );

    let mut expected_grants = vec![
        compact_sql(
            "GRANT USAGE ON SCHEMA knowledge_graph \
             TO knowledge_graph_application;",
        ),
        compact_sql(
            "GRANT SELECT ON TABLE knowledge_graph.graph_relation_types, \
             knowledge_graph.graph_concepts, knowledge_graph.graph_aliases, \
             knowledge_graph.graph_source_references, knowledge_graph.graph_concept_mentions, \
             knowledge_graph.graph_assertions, knowledge_graph.graph_assertion_evidence \
             TO knowledge_graph_application;",
        ),
    ];
    expected_grants.extend(APPLICATION_MUTATION_FUNCTIONS.iter().map(|signature| {
        compact_sql(&format!(
            "GRANT EXECUTE ON FUNCTION {signature} TO knowledge_graph_application;"
        ))
    }));

    let mut actual_grants = sql_statements_with_prefix(&migration, "GRANT ");
    expected_grants.sort();
    actual_grants.sort();
    assert_eq!(
        actual_grants, expected_grants,
        "the application role may receive only schema usage, table reads, and the 12 completed mutation routines"
    );
    assert!(
        !actual_grants.iter().any(|grant| {
            [
                "INSERT",
                "UPDATE",
                "DELETE",
                "TRUNCATE",
                "REFERENCES",
                "TRIGGER",
            ]
            .iter()
            .any(|privilege| grant.contains(&format!("GRANT {privilege}")))
        }),
        "the graph application role must not receive direct table mutation or trigger privileges"
    );
}

#[test]
fn migration_static_lock_keys_are_domain_tagged_jsonb_identities() {
    let key_functions = [
        (
            "GRAPH_LOCK_KEY_CONCEPT_ID(P_CONCEPT_ID UUID)",
            "'domain', 'knowledge_graph.concept_id.v1'",
            "'concept_id', p_concept_id",
        ),
        (
            "GRAPH_LOCK_KEY_NORMALIZED_ALIAS(P_NORMALIZED_ALIAS TEXT)",
            "'domain', 'knowledge_graph.normalized_alias.v1'",
            "'normalized_alias', p_normalized_alias",
        ),
        (
            "GRAPH_LOCK_KEY_ASSERTION_ID(P_ASSERTION_ID UUID)",
            "'domain', 'knowledge_graph.assertion_id.v1'",
            "'assertion_id', p_assertion_id",
        ),
    ];

    for (signature, domain, identity_field) in key_functions {
        let function = function_definition(signature);
        assert_contains_sql(&function, "RETURNS JSONB");
        assert_contains_sql(&function, "pg_catalog.jsonb_build_object");
        assert_contains_sql(&function, domain);
        assert_contains_sql(&function, identity_field);
        assert_contains_sql(&function, "SECURITY DEFINER");
        assert_contains_sql(&function, "SET SEARCH_PATH = PG_CATALOG, PG_TEMP");
        assert!(
            !function.contains("||") && !function.contains("PG_CATALOG.CONCAT("),
            "lock identities must use JSONB fields rather than delimiter concatenation"
        );
    }

    let source_identity = function_definition(
        "GRAPH_LOCK_KEY_SOURCE_IDENTITY(P_SOURCE_KIND TEXT, P_EXTERNAL_ID TEXT, P_EXTERNAL_VERSION BIGINT)",
    );
    assert_contains_sql(&source_identity, "RETURNS JSONB");
    assert_contains_sql(&source_identity, "pg_catalog.jsonb_build_object");
    assert_contains_sql(
        &source_identity,
        "'domain', 'knowledge_graph.source_identity.v1'",
    );
    assert_contains_sql(
        &source_identity,
        "'source_kind', p_source_kind, 'external_id', p_external_id, \
         'external_version', p_external_version",
    );
    assert_contains_sql(&source_identity, "SECURITY DEFINER");
    assert_contains_sql(&source_identity, "SET SEARCH_PATH = PG_CATALOG, PG_TEMP");

    let semantic_identity = function_definition(
        "GRAPH_LOCK_KEY_SEMANTIC_ASSERTION(P_RELATION_CODE TEXT, P_SUBJECT_CONCEPT_ID UUID, P_OBJECT_CONCEPT_ID UUID)",
    );
    assert_contains_sql(&semantic_identity, "RETURNS JSONB");
    assert_contains_sql(&semantic_identity, "SECURITY DEFINER");
    assert_contains_sql(&semantic_identity, "SET SEARCH_PATH = PG_CATALOG, PG_TEMP");
    assert_contains_sql(
        &semantic_identity,
        "'domain', 'knowledge_graph.semantic_assertion.v1'",
    );
    assert_contains_sql(
        &semantic_identity,
        "'relation_code', p_relation_code, 'subject_concept_id', \
         canonical_subject_concept_id, 'object_concept_id', canonical_object_concept_id",
    );
    assert_contains_sql(
        &semantic_identity,
        "FROM knowledge_graph.graph_relation_types AS relation_types \
         WHERE relation_types.code = p_relation_code COLLATE \"C\"",
    );
    assert_contains_sql(
        &semantic_identity,
        "IF relation_is_symmetric AND canonical_subject_concept_id > canonical_object_concept_id THEN",
    );
    assert_contains_sql(
        &semantic_identity,
        "canonical_subject_concept_id := p_object_concept_id",
    );
    assert_contains_sql(
        &semantic_identity,
        "canonical_object_concept_id := p_subject_concept_id",
    );
}

#[test]
fn migration_static_lock_acquisition_sorts_deduplicates_before_ordered_row_locks() {
    let function = function_definition(
        "GRAPH_ACQUIRE_MUTATION_LOCKS(P_LOCK_KEYS JSONB, P_CONCEPT_IDS UUID[], \
         P_ASSERTION_IDS UUID[], P_SOURCE_REF_IDS BIGINT[])",
    );
    assert_contains_sql(&function, "RETURNS VOID");
    assert_contains_sql(&function, "SECURITY DEFINER");
    assert_contains_sql(&function, "SET SEARCH_PATH = PG_CATALOG, PG_TEMP");
    assert_contains_sql(&function, "p_lock_keys jsonb");
    assert_contains_sql(
        &function,
        "SELECT DISTINCT pg_catalog.hashtextextended(key_item.value::text, 0) AS lock_id \
         FROM pg_catalog.jsonb_array_elements(p_lock_keys) AS key_item(value) \
         ORDER BY lock_id ASC",
    );
    assert_contains_sql(
        &function,
        "pg_catalog.pg_advisory_xact_lock(advisory_lock_id)",
    );
    assert_contains_sql(
        &function,
        "RAISE EXCEPTION 'invalid graph mutation lock inputs' USING ERRCODE = '22023'",
    );

    let advisory_lock = function
        .find("PERFORM PG_CATALOG.PG_ADVISORY_XACT_LOCK(ADVISORY_LOCK_ID)")
        .expect("advisory locks should use the precomputed lock-id loop");
    let lock_input_check = function
        .find("IF P_LOCK_KEYS IS NULL OR PG_CATALOG.JSONB_TYPEOF(P_LOCK_KEYS) <> 'ARRAY' THEN")
        .expect("the complete JSONB lock-key set should be validated before acquisition");
    let hash_projection = function
        .find("SELECT DISTINCT PG_CATALOG.HASHTEXTEXTENDED(KEY_ITEM.VALUE::TEXT, 0) AS LOCK_ID")
        .expect("each caller-supplied JSONB key should be hashed");
    let first_row_phase = function
        .find("END LOOP; FOR LOCKED_CONCEPT_ID IN")
        .expect("row locking should begin only after advisory acquisition completes");
    let concept_locks = function
        .find("FOR LOCKED_CONCEPT_ID IN")
        .expect("existing concepts should be row-locked");
    let assertion_locks = function
        .find("FOR LOCKED_ASSERTION_ID IN")
        .expect("existing assertions should be row-locked");
    let source_locks = function
        .find("FOR LOCKED_SOURCE_REF_ID IN")
        .expect("existing source references should be row-locked");

    assert!(
        lock_input_check < hash_projection
            && hash_projection < advisory_lock
            && advisory_lock < first_row_phase
            && first_row_phase < concept_locks
            && concept_locks < assertion_locks
            && assertion_locks < source_locks,
        "all advisory locks must precede concepts, assertions, and source-reference row locks"
    );
    assert_contains_sql(
        &function,
        "WHERE concepts.id = ANY(COALESCE(p_concept_ids, ARRAY[]::UUID[])) \
         ORDER BY concepts.id ASC FOR UPDATE OF concepts",
    );
    assert_contains_sql(
        &function,
        "WHERE assertions.id = ANY(COALESCE(p_assertion_ids, ARRAY[]::UUID[])) \
         OR EXISTS (SELECT 1 \
         FROM pg_catalog.jsonb_array_elements(p_lock_keys) AS supplied(lock_key) \
         WHERE supplied.lock_key ->> 'domain' = 'knowledge_graph.semantic_assertion.v1' \
         AND supplied.lock_key = pg_catalog.jsonb_build_object( \
         'domain', 'knowledge_graph.semantic_assertion.v1', \
         'relation_code', assertions.relation_code, \
         'subject_concept_id', CASE WHEN relation_types.is_symmetric \
         AND assertions.subject_concept_id > assertions.object_concept_id \
         THEN assertions.object_concept_id ELSE assertions.subject_concept_id END, \
         'object_concept_id', CASE WHEN relation_types.is_symmetric \
         AND assertions.subject_concept_id > assertions.object_concept_id \
         THEN assertions.subject_concept_id ELSE assertions.object_concept_id END)) \
         ORDER BY assertions.id ASC FOR UPDATE OF assertions",
    );
    assert_contains_sql(
        &function,
        "JOIN knowledge_graph.graph_relation_types AS relation_types \
         ON relation_types.code = assertions.relation_code",
    );
    assert_contains_sql(
        &function,
        "WHERE source_references.source_ref_id = ANY( \
         COALESCE(p_source_ref_ids, ARRAY[]::BIGINT[])) \
         ORDER BY source_references.source_ref_id ASC \
         FOR UPDATE OF source_references",
    );

    let post_advisory = &function[first_row_phase..];
    assert!(
        !post_advisory.contains("HASHTEXTEXTENDED")
            && !post_advisory.contains("GRAPH_LOCK_KEY_")
            && !post_advisory.contains("PG_ADVISORY_XACT_LOCK("),
        "row-lock phase must not discover or acquire additional advisory locks"
    );
    assert_contains_sql(
        &function,
        "supplied.lock_key ->> 'domain' = 'knowledge_graph.semantic_assertion.v1'",
    );
    assert!(
        !function.contains("WITH ORDINALITY"),
        "advisory lock order must not depend on caller input order"
    );
    assert!(
        !function.contains("EXECUTE "),
        "row locking must use fixed schema-qualified queries, not dynamic SQL"
    );
}

#[test]
fn migration_revokes_public_execution_of_mutation_lock_helpers() {
    let migration = executable_sql();
    for signature in [
        "knowledge_graph.graph_lock_key_concept_id(uuid)",
        "knowledge_graph.graph_lock_key_normalized_alias(text)",
        "knowledge_graph.graph_lock_key_source_identity(text, text, bigint)",
        "knowledge_graph.graph_lock_key_assertion_id(uuid)",
        "knowledge_graph.graph_lock_key_semantic_assertion(text, uuid, uuid)",
        "knowledge_graph.graph_acquire_mutation_locks(jsonb, uuid[], uuid[], bigint[])",
    ] {
        assert_contains_sql(
            &migration,
            &format!("REVOKE ALL ON FUNCTION {signature} FROM PUBLIC"),
        );
    }
}

#[test]
fn migration_concept_mutations_return_one_typed_jsonb_outcome_and_use_fixed_security() {
    let functions = [
        (
            "GRAPH_CREATE_CONCEPT(P_CANDIDATE_ID UUID, P_ALIASES JSONB)",
            &["created", "existing", "conflict"][..],
        ),
        (
            "GRAPH_REPLACE_CONCEPT_ALIASES(P_CONCEPT_ID UUID, P_EXPECTED_REVISION BIGINT, P_ALIASES JSONB)",
            &["updated", "conflict", "stale", "missing"][..],
        ),
        (
            "GRAPH_DELETE_CONCEPT(P_CONCEPT_ID UUID, P_EXPECTED_REVISION BIGINT)",
            &["deleted", "conflict", "stale", "referenced", "missing"][..],
        ),
    ];

    for (signature, outcomes) in functions {
        let function = function_definition(signature);
        assert_contains_sql(&function, "RETURNS JSONB");
        assert_contains_sql(&function, "SECURITY DEFINER");
        assert_contains_sql(&function, "SET SEARCH_PATH = PG_CATALOG, PG_TEMP");

        let return_count = function.matches("RETURN ").count();
        assert!(return_count > 0, "{signature} should return an outcome");
        assert_eq!(
            function
                .matches("RETURN PG_CATALOG.JSONB_BUILD_OBJECT(")
                .count(),
            return_count,
            "every return path in {signature} should build one outcome object"
        );
        assert_eq!(
            function.matches("'OUTCOME', '").count(),
            return_count,
            "every return path in {signature} should have exactly one outcome code"
        );
        for outcome in outcomes {
            assert_contains_sql(&function, &format!("'outcome', '{outcome}'"));
        }
        assert!(
            !function.contains("RETURN NEXT ") && !function.contains("RETURN QUERY "),
            "{signature} must not return multiple rows"
        );
        assert!(
            !function.contains("SQLERRM") && !function.contains("GET STACKED DIAGNOSTICS"),
            "{signature} must not return backend exception diagnostics"
        );
        assert!(
            !function.contains("RAISE EXCEPTION"),
            "{signature} must classify mutation failures with typed outcomes"
        );
        assert!(
            !function.contains("EXECUTE "),
            "{signature} must use static statements"
        );
    }

    let migration = executable_sql();
    for signature in [
        "knowledge_graph.graph_create_concept(uuid, jsonb)",
        "knowledge_graph.graph_replace_concept_aliases(uuid, bigint, jsonb)",
        "knowledge_graph.graph_delete_concept(uuid, bigint)",
    ] {
        assert_contains_sql(
            &migration,
            &format!("REVOKE ALL ON FUNCTION {signature} FROM PUBLIC"),
        );
    }
}

#[test]
fn migration_concept_create_locks_candidate_and_aliases_before_idempotency_checks() {
    let function =
        function_definition("GRAPH_CREATE_CONCEPT(P_CANDIDATE_ID UUID, P_ALIASES JSONB)");
    let candidate_lock_key = function
        .find("GRAPH_LOCK_KEY_CONCEPT_ID(P_CANDIDATE_ID)")
        .expect("create should lock the candidate concept identity");
    let alias_lock_key = function
        .find("GRAPH_LOCK_KEY_NORMALIZED_ALIAS(")
        .expect("create should lock every requested normalized alias");
    let acquire_locks = function
        .find("PERFORM KNOWLEDGE_GRAPH.GRAPH_ACQUIRE_MUTATION_LOCKS(")
        .expect("create should use the canonical lock helper");
    let first_alias_lookup = function
        .find("FROM KNOWLEDGE_GRAPH.GRAPH_ALIASES AS ALIASES")
        .expect("create should inspect aliases only after locking their keys");
    let overlap_conflict_check = function
        .find("IF OWNER_COUNT > 0 THEN")
        .expect("create should classify overlapping non-matching alias sets as conflicts");
    let concept_insert = function
        .find("INSERT INTO KNOWLEDGE_GRAPH.GRAPH_CONCEPTS")
        .expect("create should insert a concept when no alias overlaps");
    let alias_insert = function
        .find("INSERT INTO KNOWLEDGE_GRAPH.GRAPH_ALIASES")
        .expect("create should insert the complete alias set");

    assert!(
        candidate_lock_key < acquire_locks
            && alias_lock_key < acquire_locks
            && acquire_locks < first_alias_lookup
            && first_alias_lookup < overlap_conflict_check
            && overlap_conflict_check < concept_insert
            && concept_insert < alias_insert,
        "create must establish the full lock set before classifying or mutating aliases"
    );
    assert_contains_sql(&function, "'ICU4X-2.3.0-NFKC-FOLD-V1'");
    assert_contains_sql(&function, "VALUES (P_CANDIDATE_ID, 1)");
    assert_contains_sql(&function, ") = ALIAS_COUNT");
    assert_contains_sql(
        &function,
        "ALIASES.IS_PREFERRED = (REQUESTED.VALUE ->> 'IS_PREFERRED')::BOOLEAN",
    );
    assert_contains_sql(
        &function,
        "ALIASES.ALIAS_KEY = (REQUESTED.VALUE ->> 'ALIAS_KEY') COLLATE \"C\"",
    );
    assert_contains_sql(
        &function,
        "'FIELD', 'ALIAS_SET', 'REASON', 'ALIAS_SET_MISMATCH'",
    );
    assert_contains_sql(&function, "'REASON', 'ALIAS_OWNED'");
    assert_contains_sql(
        &function,
        "'RECORD', PG_CATALOG.JSONB_BUILD_OBJECT('ID', EXISTING_CONCEPT_ID, 'REVISION', EXISTING_REVISION",
    );
    assert_contains_sql(&function, "WHERE ALIASES.CONCEPT_ID = EXISTING_CONCEPT_ID");
    assert_contains_sql(&function, "'DISPLAY_TEXT', ALIASES.DISPLAY_TEXT");
    assert_contains_sql(&function, "'PREFERRED', ALIASES.IS_PREFERRED");
    assert!(
        !function.contains("UPDATE KNOWLEDGE_GRAPH.GRAPH_ALIASES"),
        "idempotent create must preserve the stored alias display text"
    );
}

#[test]
fn migration_concept_replacement_checks_snapshot_revision_and_final_alias_set_before_mutation() {
    let function = function_definition(
        "GRAPH_REPLACE_CONCEPT_ALIASES(P_CONCEPT_ID UUID, P_EXPECTED_REVISION BIGINT, P_ALIASES JSONB)",
    );
    let old_alias_snapshot = function
        .find("INTO SNAPSHOT_REVISION, SNAPSHOT_ALIAS_SNAPSHOT")
        .expect("replacement should capture old aliases before acquiring locks");
    let old_alias_lock = function
        .find("GRAPH_LOCK_KEY_NORMALIZED_ALIAS(")
        .expect("replacement should lock old and new alias keys");
    let acquire_locks = function
        .find("PERFORM KNOWLEDGE_GRAPH.GRAPH_ACQUIRE_MUTATION_LOCKS(")
        .expect("replacement should use the canonical lock helper");
    let snapshot_check = function
        .find("CURRENT_ALIAS_SNAPSHOT IS DISTINCT FROM SNAPSHOT_ALIAS_SNAPSHOT")
        .expect("replacement should reject snapshot drift after acquiring locks");
    let stale_check = function
        .find("P_EXPECTED_REVISION IS DISTINCT FROM CURRENT_REVISION")
        .expect("replacement should compare the caller revision after snapshot verification");
    let invalid_alias_set_check = function
        .find("PREFERRED_ALIAS_COUNT <> 1")
        .expect("replacement should reject a final set without exactly one preferred alias");
    let owner_check = function
        .find("CONCEPTS.ID <> P_CONCEPT_ID")
        .expect("replacement should reject aliases owned by another concept");
    let alias_delete = function
        .find("DELETE FROM KNOWLEDGE_GRAPH.GRAPH_ALIASES")
        .expect("replacement should remove the previous complete alias set");
    let alias_insert = function
        .find("INSERT INTO KNOWLEDGE_GRAPH.GRAPH_ALIASES")
        .expect("replacement should insert the complete validated alias set");
    let revision_update = function
        .find("UPDATE KNOWLEDGE_GRAPH.GRAPH_CONCEPTS AS CONCEPTS")
        .expect("replacement should increment the concept revision");

    assert!(
        old_alias_snapshot < old_alias_lock
            && old_alias_lock < acquire_locks
            && acquire_locks < snapshot_check
            && snapshot_check < stale_check
            && stale_check < invalid_alias_set_check
            && invalid_alias_set_check < owner_check
            && owner_check < alias_delete
            && alias_delete < alias_insert
            && alias_insert < revision_update,
        "replacement must lock and classify the full snapshot before any all-or-none mutation"
    );
    assert_contains_sql(&function, "ALIAS_COUNT BETWEEN 1 AND 64");
    assert_contains_sql(&function, "DISTINCT_ALIAS_COUNT <> ALIAS_COUNT");
    assert_contains_sql(&function, "'FIELD', 'SNAPSHOT', 'REASON', 'SNAPSHOT_DRIFT'");
    assert_contains_sql(&function, "'FIELD', 'ALIAS_SET', 'REASON', 'ALIAS_OWNED'");
    assert_contains_sql(&function, "SET REVISION = CONCEPTS.REVISION + 1");
    assert_contains_sql(
        &function,
        "WHERE (SELECT pg_catalog.count(*) \
         FROM pg_catalog.jsonb_object_keys(item.value)) <> 3",
    );

    let acquire_locks_end = acquire_locks
        + function[acquire_locks..]
            .find(");")
            .expect("replacement lock-helper call should terminate")
        + 2;
    let post_lock_phase = &function[acquire_locks_end..alias_delete];
    assert!(
        !post_lock_phase.contains("GRAPH_LOCK_KEY_")
            && !post_lock_phase.contains("GRAPH_ACQUIRE_MUTATION_LOCKS"),
        "replacement must not discover or acquire lock keys after ordered acquisition"
    );
}

#[test]
fn migration_concept_delete_checks_snapshot_revision_and_references_before_hard_delete() {
    let function =
        function_definition("GRAPH_DELETE_CONCEPT(P_CONCEPT_ID UUID, P_EXPECTED_REVISION BIGINT)");
    let old_alias_snapshot = function
        .find("INTO SNAPSHOT_REVISION, SNAPSHOT_ALIAS_SNAPSHOT")
        .expect("delete should capture old aliases before acquiring locks");
    let alias_lock = function
        .find("GRAPH_LOCK_KEY_NORMALIZED_ALIAS(")
        .expect("delete should lock each old alias uniqueness key");
    let acquire_locks = function
        .find("PERFORM KNOWLEDGE_GRAPH.GRAPH_ACQUIRE_MUTATION_LOCKS(")
        .expect("delete should use the canonical lock helper");
    let snapshot_check = function
        .find("CURRENT_ALIAS_SNAPSHOT IS DISTINCT FROM SNAPSHOT_ALIAS_SNAPSHOT")
        .expect("delete should reject snapshot drift after acquiring locks");
    let stale_check = function
        .find("P_EXPECTED_REVISION IS DISTINCT FROM CURRENT_REVISION")
        .expect("delete should compare the expected revision after snapshot verification");
    let mention_check = function
        .find("FROM KNOWLEDGE_GRAPH.GRAPH_CONCEPT_MENTIONS AS MENTIONS")
        .expect("delete should reject concepts referenced by mentions");
    let assertion_check = function
        .find("FROM KNOWLEDGE_GRAPH.GRAPH_ASSERTIONS AS ASSERTIONS")
        .expect("delete should reject concepts used as assertion endpoints");
    let alias_delete = function
        .find("DELETE FROM KNOWLEDGE_GRAPH.GRAPH_ALIASES")
        .expect("delete should hard-delete aliases");
    let concept_delete = function
        .find("DELETE FROM KNOWLEDGE_GRAPH.GRAPH_CONCEPTS")
        .expect("delete should hard-delete the concept");

    assert!(
        old_alias_snapshot < alias_lock
            && alias_lock < acquire_locks
            && acquire_locks < snapshot_check
            && snapshot_check < stale_check
            && stale_check < mention_check
            && mention_check < assertion_check
            && assertion_check < alias_delete
            && alias_delete < concept_delete,
        "delete must acquire all locks and reject stale, drifted, or referenced state before DML"
    );
    assert_contains_sql(&function, "ASSERTIONS.SUBJECT_CONCEPT_ID = P_CONCEPT_ID");
    assert_contains_sql(&function, "ASSERTIONS.OBJECT_CONCEPT_ID = P_CONCEPT_ID");
    assert_contains_sql(
        &function,
        "'REFERENCE', PG_CATALOG.JSONB_BUILD_OBJECT('KIND', 'CONCEPT', 'ID', P_CONCEPT_ID)",
    );
    assert_contains_sql(&function, "'OUTCOME', 'DELETED'");

    let acquire_locks_end = acquire_locks
        + function[acquire_locks..]
            .find(");")
            .expect("delete lock-helper call should terminate")
        + 2;
    let post_lock_phase = &function[acquire_locks_end..alias_delete];
    assert!(
        !post_lock_phase.contains("GRAPH_LOCK_KEY_")
            && !post_lock_phase.contains("GRAPH_ACQUIRE_MUTATION_LOCKS"),
        "delete must not discover or acquire lock keys after ordered acquisition"
    );
}

#[test]
fn migration_source_routines_register_replay_and_restrict_unreferenced_delete() {
    let source_identity_key = function_definition(
        "GRAPH_LOCK_KEY_SOURCE_IDENTITY(P_SOURCE_KIND TEXT, P_EXTERNAL_ID TEXT, P_EXTERNAL_VERSION BIGINT)",
    );
    assert_contains_sql(
        &source_identity_key,
        "p_external_id IS NULL OR p_external_id = '' OR \
         pg_catalog.octet_length(p_external_id) NOT BETWEEN 1 AND 2048",
    );
    assert_contains_sql(
        &source_identity_key,
        "p_source_kind COLLATE \"C\" = 'memory_version' AND \
         (p_external_version IS NULL OR p_external_version <= 0)",
    );
    assert_contains_sql(
        &source_identity_key,
        "p_source_kind COLLATE \"C\" = 'session_record' \
         AND p_external_version IS NOT NULL",
    );
    assert_contains_sql(
        &source_identity_key,
        "RAISE EXCEPTION 'invalid graph source lock identity'",
    );
    assert!(
        !source_identity_key.contains("P_EXTERNAL_ID ||")
            && !source_identity_key.contains("PG_CATALOG.CONCAT(P_EXTERNAL_ID"),
        "source validation errors must not include the supplied identity"
    );

    let register_source = function_definition(
        "GRAPH_REGISTER_SOURCE(P_SOURCE_KIND TEXT, P_EXTERNAL_ID TEXT, P_EXTERNAL_VERSION BIGINT)",
    );
    assert_typed_mutation_function(&register_source, &["created", "existing"]);
    assert_contains_sql(
        &register_source,
        "'source_kind', current_source_kind, 'external_id', current_external_id, \
         'external_version', current_external_version",
    );
    for outcome in ["created", "existing"] {
        let returns = outcome_returns(&register_source, outcome);
        assert_eq!(returns.len(), 1, "register should have one {outcome} path");
        assert!(returns[0].contains("CURRENT_EXTERNAL_ID"));
        assert!(returns[0].contains("CURRENT_SOURCE_KIND"));
        assert!(returns[0].contains("CURRENT_EXTERNAL_VERSION"));
    }
    for forbidden_owner_lookup in ["PUBLIC.MEMORIES", "PUBLIC.SESSIONS", "TO_REGCLASS("] {
        assert!(
            !register_source.contains(forbidden_owner_lookup),
            "source registration must not check external owner tables"
        );
    }

    let register_key = register_source
        .find("GRAPH_LOCK_KEY_SOURCE_IDENTITY(")
        .expect("register should derive the typed source identity lock key");
    let register_snapshot = register_source
        .find("INTO SNAPSHOT_SOURCE_REF_ID")
        .expect("register should resolve a source row only as an optimistic snapshot");
    let register_locks = register_source
        .find("PERFORM KNOWLEDGE_GRAPH.GRAPH_ACQUIRE_MUTATION_LOCKS(")
        .expect("register should use the canonical mutation lock helper");
    let register_revalidation = register_source
        .find("INTO CURRENT_SOURCE_REF_ID")
        .expect("register should re-read the typed source identity after locking");
    let register_insert = register_source
        .find("INSERT INTO KNOWLEDGE_GRAPH.GRAPH_SOURCE_REFERENCES")
        .expect("register should insert a missing source identity");
    assert!(
        register_key < register_snapshot
            && register_snapshot < register_locks
            && register_locks < register_revalidation
            && register_revalidation < register_insert,
        "register must serialize the typed identity before revalidation or insertion"
    );
    assert_contains_sql(
        &register_source,
        "source_references.external_version IS NOT DISTINCT FROM p_external_version",
    );
    assert!(
        register_source.find("'OUTCOME', 'EXISTING'").unwrap() < register_insert,
        "replayed registration must return the canonical row without inserting"
    );

    let delete_source = function_definition(
        "GRAPH_DELETE_SOURCE(P_SOURCE_KIND TEXT, P_EXTERNAL_ID TEXT, P_EXTERNAL_VERSION BIGINT)",
    );
    assert_typed_mutation_function(
        &delete_source,
        &["deleted", "missing", "referenced", "conflict"],
    );
    for outcome in ["missing", "referenced", "conflict"] {
        assert_source_identity_omitted(&delete_source, outcome);
    }
    let delete_snapshot = delete_source
        .find("INTO SNAPSHOT_SOURCE_REF_ID")
        .expect("delete should capture the source identity before acquiring locks");
    let delete_key = delete_source
        .find("GRAPH_LOCK_KEY_SOURCE_IDENTITY(")
        .expect("delete should lock the complete typed source identity");
    let delete_locks = delete_source
        .find("PERFORM KNOWLEDGE_GRAPH.GRAPH_ACQUIRE_MUTATION_LOCKS(")
        .expect("delete should use the canonical mutation lock helper");
    let delete_revalidation = delete_source
        .find("INTO CURRENT_SOURCE_REF_ID")
        .expect("delete should revalidate the typed identity after acquiring locks");
    let mention_reference_check = delete_source
        .find("FROM KNOWLEDGE_GRAPH.GRAPH_CONCEPT_MENTIONS AS MENTIONS")
        .expect("delete should reject source rows referenced by mentions");
    let evidence_reference_check = delete_source
        .find("FROM KNOWLEDGE_GRAPH.GRAPH_ASSERTION_EVIDENCE AS EVIDENCE")
        .expect("delete should reject source rows referenced by evidence");
    let source_delete = delete_source
        .find("DELETE FROM KNOWLEDGE_GRAPH.GRAPH_SOURCE_REFERENCES")
        .expect("delete should remove the unreferenced source row");
    assert!(
        delete_key < delete_snapshot
            && delete_snapshot < delete_locks
            && delete_locks < delete_revalidation
            && delete_revalidation < mention_reference_check
            && mention_reference_check < evidence_reference_check
            && evidence_reference_check < source_delete,
        "source delete must lock and revalidate before checking references or mutating"
    );
    assert_contains_sql(
        &delete_source,
        "current_source_ref_id IS DISTINCT FROM snapshot_source_ref_id",
    );
    assert_contains_sql(
        &delete_source,
        "'field', 'snapshot', 'reason', 'snapshot_drift'",
    );
    for unrelated_delete in [
        "DELETE FROM KNOWLEDGE_GRAPH.GRAPH_CONCEPTS",
        "DELETE FROM KNOWLEDGE_GRAPH.GRAPH_ASSERTIONS",
        "DELETE FROM KNOWLEDGE_GRAPH.GRAPH_CONCEPT_MENTIONS",
        "DELETE FROM KNOWLEDGE_GRAPH.GRAPH_ASSERTION_EVIDENCE",
    ] {
        assert!(
            !delete_source.contains(unrelated_delete),
            "source deletion must preserve unrelated graph state"
        );
    }
    for forbidden_owner_lookup in ["PUBLIC.MEMORIES", "PUBLIC.SESSIONS", "TO_REGCLASS("] {
        assert!(
            !delete_source.contains(forbidden_owner_lookup),
            "source deletion must not check external owner tables"
        );
    }
}

#[test]
fn migration_mention_routines_lock_full_identity_and_change_only_the_association() {
    let mention_key = function_definition(
        "GRAPH_LOCK_KEY_CONCEPT_MENTION(P_CONCEPT_ID UUID, P_SOURCE_KIND TEXT, \
         P_EXTERNAL_ID TEXT, P_EXTERNAL_VERSION BIGINT)",
    );
    assert_contains_sql(&mention_key, "RETURNS JSONB");
    assert_contains_sql(&mention_key, "SECURITY DEFINER");
    assert_contains_sql(&mention_key, "SET SEARCH_PATH = PG_CATALOG, PG_TEMP");
    assert_contains_sql(
        &mention_key,
        "'domain', 'knowledge_graph.concept_mention.v1'",
    );
    assert_contains_sql(&mention_key, "'concept_id', p_concept_id");
    assert_contains_sql(
        &mention_key,
        "knowledge_graph.graph_lock_key_source_identity( \
         p_source_kind, p_external_id, p_external_version )",
    );

    let create_mention = function_definition(
        "GRAPH_CREATE_MENTION(P_CONCEPT_ID UUID, P_SOURCE_KIND TEXT, \
         P_EXTERNAL_ID TEXT, P_EXTERNAL_VERSION BIGINT)",
    );
    assert_typed_mutation_function(
        &create_mention,
        &["created", "existing", "missing", "conflict"],
    );
    for outcome in ["missing", "conflict"] {
        assert_source_identity_omitted(&create_mention, outcome);
    }
    for outcome in ["created", "existing"] {
        let returns = outcome_returns(&create_mention, outcome);
        assert_eq!(
            returns.len(),
            1,
            "mention create should have one {outcome} path"
        );
        assert!(returns[0].contains("CURRENT_EXTERNAL_ID"));
        assert!(returns[0].contains("CURRENT_SOURCE_KIND"));
        assert!(returns[0].contains("CURRENT_EXTERNAL_VERSION"));
    }

    let delete_mention = function_definition(
        "GRAPH_DELETE_MENTION(P_CONCEPT_ID UUID, P_SOURCE_KIND TEXT, \
         P_EXTERNAL_ID TEXT, P_EXTERNAL_VERSION BIGINT)",
    );
    assert_typed_mutation_function(&delete_mention, &["deleted", "missing", "conflict"]);
    for outcome in ["missing", "conflict"] {
        assert_source_identity_omitted(&delete_mention, outcome);
    }

    for (function, first_mutation) in [
        (
            &create_mention,
            "INSERT INTO KNOWLEDGE_GRAPH.GRAPH_CONCEPT_MENTIONS",
        ),
        (
            &delete_mention,
            "DELETE FROM KNOWLEDGE_GRAPH.GRAPH_CONCEPT_MENTIONS",
        ),
    ] {
        let concept_key = function
            .find("GRAPH_LOCK_KEY_CONCEPT_ID(P_CONCEPT_ID)")
            .expect("mention mutation should lock the concept aggregate");
        let source_key = function
            .find("GRAPH_LOCK_KEY_SOURCE_IDENTITY(")
            .expect("mention mutation should lock the typed source identity");
        let relation_key = function
            .find("GRAPH_LOCK_KEY_CONCEPT_MENTION(")
            .expect("mention mutation should lock the complete association identity");
        let source_snapshot = function
            .find("INTO SNAPSHOT_SOURCE_REF_ID")
            .expect("mention mutation should resolve the internal source ID optimistically");
        let acquire_locks = function
            .find("PERFORM KNOWLEDGE_GRAPH.GRAPH_ACQUIRE_MUTATION_LOCKS(")
            .expect("mention mutation should use the canonical lock helper");
        let current_identity = function
            .find("INTO CURRENT_SOURCE_REF_ID")
            .expect("mention mutation should revalidate source identity after locking");
        let mutation = function
            .find(first_mutation)
            .expect("mention mutation should change only the association row");
        assert!(
            concept_key < acquire_locks
                && source_key < acquire_locks
                && relation_key < acquire_locks
                && source_snapshot < acquire_locks
                && acquire_locks < current_identity
                && current_identity < mutation,
            "mention mutation must precompute all keys, lock, revalidate, then mutate"
        );
        assert_contains_sql(function, "ARRAY[P_CONCEPT_ID]");
        assert_contains_sql(function, "ARRAY[SNAPSHOT_SOURCE_REF_ID]");
        assert_contains_sql(
            function,
            "CURRENT_SOURCE_REF_ID IS DISTINCT FROM SNAPSHOT_SOURCE_REF_ID",
        );

        let acquire_locks_end = acquire_locks
            + function[acquire_locks..]
                .find(");")
                .expect("lock-helper call should terminate")
            + 2;
        let post_lock_pre_mutation = &function[acquire_locks_end..mutation];
        assert!(
            !post_lock_pre_mutation.contains("GRAPH_LOCK_KEY_")
                && !post_lock_pre_mutation.contains("GRAPH_ACQUIRE_MUTATION_LOCKS"),
            "mention mutation must not discover or acquire locks after ordered locking begins"
        );
        for unrelated_mutation in [
            "INSERT INTO KNOWLEDGE_GRAPH.GRAPH_CONCEPTS",
            "UPDATE KNOWLEDGE_GRAPH.GRAPH_CONCEPTS",
            "DELETE FROM KNOWLEDGE_GRAPH.GRAPH_CONCEPTS",
            "INSERT INTO KNOWLEDGE_GRAPH.GRAPH_SOURCE_REFERENCES",
            "DELETE FROM KNOWLEDGE_GRAPH.GRAPH_SOURCE_REFERENCES",
            "INSERT INTO KNOWLEDGE_GRAPH.GRAPH_ASSERTIONS",
            "DELETE FROM KNOWLEDGE_GRAPH.GRAPH_ASSERTIONS",
            "INSERT INTO KNOWLEDGE_GRAPH.GRAPH_ASSERTION_EVIDENCE",
            "DELETE FROM KNOWLEDGE_GRAPH.GRAPH_ASSERTION_EVIDENCE",
        ] {
            assert!(
                !function.contains(unrelated_mutation),
                "mention mutation must not change related or unrelated graph state"
            );
        }
    }

    assert!(
        create_mention.find("'OUTCOME', 'EXISTING'").unwrap()
            < create_mention
                .find("INSERT INTO KNOWLEDGE_GRAPH.GRAPH_CONCEPT_MENTIONS")
                .unwrap(),
        "idempotent mention replay must return the existing association without inserting"
    );
    assert_contains_sql(&create_mention, "'record_kind', 'concept'");
    assert_contains_sql(&create_mention, "'record_kind', 'source_reference'");
    assert_contains_sql(&delete_mention, "'record_kind', 'concept_mention'");
    assert_contains_sql(&delete_mention, "'outcome', 'missing'");
}

#[test]
fn migration_source_and_mention_routines_revoke_public_execution_without_early_grants() {
    let migration = executable_sql();
    for signature in [
        "knowledge_graph.graph_lock_key_concept_mention(uuid, text, text, bigint)",
        "knowledge_graph.graph_register_source(text, text, bigint)",
        "knowledge_graph.graph_delete_source(text, text, bigint)",
        "knowledge_graph.graph_create_mention(uuid, text, text, bigint)",
        "knowledge_graph.graph_delete_mention(uuid, text, text, bigint)",
    ] {
        assert_contains_sql(
            &migration,
            &format!("REVOKE ALL ON FUNCTION {signature} FROM PUBLIC"),
        );
    }
}

#[test]
fn migration_assertion_mutations_return_typed_outcomes_and_revoke_public_execution() {
    let functions = [
        (
            "GRAPH_CREATE_ASSERTION(P_CANDIDATE_ID UUID, P_SUBJECT_CONCEPT_ID UUID, \
             P_RELATION_CODE TEXT, P_OBJECT_CONCEPT_ID UUID, P_SUPPORTING_SOURCES JSONB)",
            &[
                "created",
                "existing",
                "conflict",
                "missing",
                "evidence_limit",
                "invalid_input",
            ][..],
        ),
        (
            "GRAPH_UPDATE_ASSERTION(P_ASSERTION_ID UUID, P_EXPECTED_REVISION BIGINT, \
             P_SUBJECT_CONCEPT_ID UUID, P_RELATION_CODE TEXT, P_OBJECT_CONCEPT_ID UUID)",
            &["updated", "conflict", "stale", "missing", "invalid_input"][..],
        ),
        (
            "GRAPH_DELETE_ASSERTION(P_ASSERTION_ID UUID, P_EXPECTED_REVISION BIGINT)",
            &["deleted", "conflict", "stale", "missing", "invalid_input"][..],
        ),
    ];

    for (signature, outcomes) in functions {
        let function = function_definition(signature);
        assert_typed_mutation_function(&function, outcomes);
    }

    let create = function_definition(
        "GRAPH_CREATE_ASSERTION(P_CANDIDATE_ID UUID, P_SUBJECT_CONCEPT_ID UUID, \
         P_RELATION_CODE TEXT, P_OBJECT_CONCEPT_ID UUID, P_SUPPORTING_SOURCES JSONB)",
    );
    let update = function_definition(
        "GRAPH_UPDATE_ASSERTION(P_ASSERTION_ID UUID, P_EXPECTED_REVISION BIGINT, \
         P_SUBJECT_CONCEPT_ID UUID, P_RELATION_CODE TEXT, P_OBJECT_CONCEPT_ID UUID)",
    );
    let delete = function_definition(
        "GRAPH_DELETE_ASSERTION(P_ASSERTION_ID UUID, P_EXPECTED_REVISION BIGINT)",
    );
    for function in [&create, &update, &delete] {
        for outcome in ["missing", "conflict", "invalid_input"] {
            for returned in outcome_returns(function, outcome) {
                for source_identity_field in ["EXTERNAL_ID", "EXTERNAL_VERSION", "SOURCE_REF_ID"] {
                    assert!(
                        !returned.contains(source_identity_field),
                        "{outcome} assertion outcome must omit {source_identity_field}: {returned}"
                    );
                }
            }
        }
    }
    assert_source_identity_omitted(&create, "missing");
    assert_source_identity_omitted(&delete, "missing");

    let migration = executable_sql();
    for signature in [
        "knowledge_graph.graph_create_assertion(uuid, uuid, text, uuid, jsonb)",
        "knowledge_graph.graph_update_assertion(uuid, bigint, uuid, text, uuid)",
        "knowledge_graph.graph_delete_assertion(uuid, bigint)",
    ] {
        assert_contains_sql(
            &migration,
            &format!("REVOKE ALL ON FUNCTION {signature} FROM PUBLIC"),
        );
    }
}

#[test]
fn migration_assertion_create_counts_keys_for_each_source_shape() {
    let function = function_definition(
        "GRAPH_CREATE_ASSERTION(P_CANDIDATE_ID UUID, P_SUBJECT_CONCEPT_ID UUID, \
         P_RELATION_CODE TEXT, P_OBJECT_CONCEPT_ID UUID, P_SUPPORTING_SOURCES JSONB)",
    );

    assert!(
        !function.contains("PG_CATALOG.JSONB_OBJECT_LENGTH("),
        "assertion source validation must use PostgreSQL-supported JSONB functions"
    );
    assert_contains_sql(
        &function,
        "WHERE (requested.value ->> 'kind') COLLATE \"C\" = 'memory_version' \
         AND (
           (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(requested.value)) <> 3
           OR pg_catalog.jsonb_typeof(requested.value -> 'memory_id') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(requested.value -> 'version') IS DISTINCT FROM 'number'
         )",
    );
    assert_contains_sql(
        &function,
        "OR (requested.value ->> 'kind') COLLATE \"C\" = 'session_record' \
         AND (
           (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(requested.value)) <> 2
           OR pg_catalog.jsonb_typeof(requested.value -> 'session_record_id') \
             IS DISTINCT FROM 'string'
         )",
    );
}

#[test]
fn migration_assertion_create_orders_distinct_source_identities_by_c_collation() {
    let function = function_definition(
        "GRAPH_CREATE_ASSERTION(P_CANDIDATE_ID UUID, P_SUBJECT_CONCEPT_ID UUID, \
         P_RELATION_CODE TEXT, P_OBJECT_CONCEPT_ID UUID, P_SUPPORTING_SOURCES JSONB)",
    );

    assert_contains_sql(
        &function,
        "FOR requested_source IN \
         SELECT requested_sources.source_kind, requested_sources.external_id, \
           requested_sources.external_version \
         FROM ( \
           SELECT DISTINCT \
             (requested.value ->> 'kind') COLLATE \"C\" AS source_kind, \
             (CASE (requested.value ->> 'kind') COLLATE \"C\" \
               WHEN 'memory_version' THEN requested.value ->> 'memory_id' \
               ELSE requested.value ->> 'session_record_id' \
             END) COLLATE \"C\" AS external_id, \
             CASE (requested.value ->> 'kind') COLLATE \"C\" \
               WHEN 'memory_version' THEN (requested.value ->> 'version')::bigint \
               ELSE NULL::bigint \
             END AS external_version \
           FROM pg_catalog.jsonb_array_elements(p_supporting_sources) AS requested(value) \
         ) AS requested_sources \
         ORDER BY requested_sources.source_kind COLLATE \"C\" ASC, \
           requested_sources.external_id COLLATE \"C\" ASC, \
           requested_sources.external_version ASC NULLS FIRST",
    );
}

#[test]
fn migration_assertion_create_canonicalizes_and_serializes_semantics_and_evidence() {
    let function = function_definition(
        "GRAPH_CREATE_ASSERTION(P_CANDIDATE_ID UUID, P_SUBJECT_CONCEPT_ID UUID, \
         P_RELATION_CODE TEXT, P_OBJECT_CONCEPT_ID UUID, P_SUPPORTING_SOURCES JSONB)",
    );
    assert_contains_sql(
        &function,
        "pg_catalog.substring(p_candidate_id::text, 15, 1) IS DISTINCT FROM '7'",
    );
    assert_contains_sql(
        &function,
        "pg_catalog.substring(p_candidate_id::text, 20, 1) NOT IN ('8', '9', 'a', 'b')",
    );
    assert_contains_sql(
        &function,
        "FROM knowledge_graph.graph_relation_types AS relation_types \
         WHERE relation_types.code = p_relation_code COLLATE \"C\"",
    );
    assert_contains_sql(
        &function,
        "IF relation_is_symmetric AND canonical_subject_concept_id > canonical_object_concept_id THEN",
    );
    assert_contains_sql(
        &function,
        "canonical_subject_concept_id := p_object_concept_id",
    );
    assert_contains_sql(
        &function,
        "canonical_object_concept_id := p_subject_concept_id",
    );
    assert_contains_sql(&function, "p_subject_concept_id = p_object_concept_id");
    assert_contains_sql(
        &function,
        "pg_catalog.jsonb_array_length(p_supporting_sources) > 32",
    );

    let source_snapshot = function
        .find("INTO SNAPSHOT_SOURCE_REF_IDS")
        .expect("create should snapshot existing requested source rows");
    let semantic_snapshot = function
        .find("INTO SNAPSHOT_ASSERTION_ID")
        .expect("create should inspect the semantic row only as a lock snapshot");
    let source_identity_key = function
        .find("GRAPH_LOCK_KEY_SOURCE_IDENTITY(")
        .expect("create should lock every requested source identity");
    let acquire_locks = function
        .find("PERFORM KNOWLEDGE_GRAPH.GRAPH_ACQUIRE_MUTATION_LOCKS(")
        .expect("create should use the canonical ordered-lock helper");
    let endpoint_check = function
        .find("FROM KNOWLEDGE_GRAPH.GRAPH_CONCEPTS AS CONCEPTS")
        .expect("create should validate graph endpoints after row locks");
    let existing_semantic = function
        .find("INTO EXISTING_ASSERTION_ID, EXISTING_ASSERTION_REVISION")
        .expect("create should re-read its canonical semantic assertion after locks");
    let evidence_limit = function
        .find("CURRENT_EVIDENCE_COUNT + MISSING_EVIDENCE_COUNT > 32")
        .expect("create should check the combined evidence count before mutation");
    let evidence_insert = function
        .find("INSERT INTO KNOWLEDGE_GRAPH.GRAPH_ASSERTION_EVIDENCE")
        .expect("create should attach only missing evidence");
    let assertion_insert = function
        .find("INSERT INTO KNOWLEDGE_GRAPH.GRAPH_ASSERTIONS")
        .expect("create should insert a new semantic assertion after validation");

    assert!(
        source_snapshot < acquire_locks
            && semantic_snapshot < acquire_locks
            && source_identity_key < acquire_locks
            && acquire_locks < endpoint_check
            && endpoint_check < existing_semantic
            && evidence_limit < evidence_insert
            && evidence_limit < assertion_insert,
        "create must precompute locks, revalidate state, and check the total cap before writes"
    );
    assert_contains_sql(&function, "ARRAY[p_candidate_id, snapshot_assertion_id]");
    assert_contains_sql(&function, "snapshot_source_ref_ids");
    assert_contains_sql(
        &function,
        "WHERE NOT EXISTS (SELECT 1 FROM knowledge_graph.graph_assertion_evidence",
    );
    assert_contains_sql(
        &function,
        "ON CONFLICT (assertion_id, source_ref_id) DO NOTHING",
    );
    assert_contains_sql(&function, "'outcome', 'evidence_limit'");
    assert_contains_sql(&function, "'operation', 'create_assertion'");
    assert!(
        !function.contains("UPDATE KNOWLEDGE_GRAPH.GRAPH_ASSERTIONS"),
        "idempotent create may attach evidence without changing aggregate revision"
    );

    let acquire_locks_end = acquire_locks
        + function[acquire_locks..]
            .find(");")
            .expect("create lock-helper call should terminate")
        + 2;
    let post_lock_phase = &function[acquire_locks_end..];
    assert!(
        !post_lock_phase.contains("GRAPH_LOCK_KEY_")
            && !post_lock_phase.contains("GRAPH_ACQUIRE_MUTATION_LOCKS"),
        "create must not discover or acquire new keys after ordered locking"
    );
}

#[test]
fn migration_assertion_update_checks_snapshot_revision_and_semantic_conflict_before_write() {
    let function = function_definition(
        "GRAPH_UPDATE_ASSERTION(P_ASSERTION_ID UUID, P_EXPECTED_REVISION BIGINT, \
         P_SUBJECT_CONCEPT_ID UUID, P_RELATION_CODE TEXT, P_OBJECT_CONCEPT_ID UUID)",
    );
    let snapshot = function
        .find("INTO SNAPSHOT_REVISION, SNAPSHOT_SUBJECT_CONCEPT_ID")
        .expect("update should snapshot all old semantic fields before locking");
    let old_semantic_key = function
        .find("GRAPH_LOCK_KEY_SEMANTIC_ASSERTION(")
        .expect("update should lock the old semantic key");
    let new_semantic_key = function[old_semantic_key + 1..]
        .find("GRAPH_LOCK_KEY_SEMANTIC_ASSERTION(")
        .map(|position| position + old_semantic_key + 1)
        .expect("update should lock the complete new semantic key");
    let acquire_locks = function
        .find("PERFORM KNOWLEDGE_GRAPH.GRAPH_ACQUIRE_MUTATION_LOCKS(")
        .expect("update should use the canonical ordered-lock helper");
    let snapshot_check = function
        .find("CURRENT_SUBJECT_CONCEPT_ID IS DISTINCT FROM SNAPSHOT_SUBJECT_CONCEPT_ID")
        .expect("update should revalidate the old row after locking");
    let stale_check = function
        .find("P_EXPECTED_REVISION IS DISTINCT FROM CURRENT_REVISION")
        .expect("update should compare the caller's expected revision");
    let endpoint_check = function
        .find("FROM KNOWLEDGE_GRAPH.GRAPH_CONCEPTS AS CONCEPTS")
        .expect("update should verify new endpoint concepts");
    let duplicate_check = function
        .find("ASSERTIONS.ID <> P_ASSERTION_ID")
        .expect("update should exclude itself when checking duplicate semantics");
    let update = function
        .find("UPDATE KNOWLEDGE_GRAPH.GRAPH_ASSERTIONS AS ASSERTIONS")
        .expect("update should mutate the assertion only after every check");

    assert!(
        snapshot < old_semantic_key
            && old_semantic_key < new_semantic_key
            && new_semantic_key < acquire_locks
            && acquire_locks < snapshot_check
            && snapshot_check < stale_check
            && stale_check < endpoint_check
            && endpoint_check < duplicate_check
            && duplicate_check < update,
        "update must snapshot, lock both keys, revalidate and classify before changing state"
    );
    assert_contains_sql(
        &function,
        "ARRAY[ snapshot_subject_concept_id, p_subject_concept_id, \
         p_object_concept_id, snapshot_object_concept_id ]",
    );
    assert_contains_sql(&function, "REVISION = ASSERTIONS.REVISION + 1");
    assert_contains_sql(&function, "'field', 'snapshot', 'reason', 'snapshot_drift'");
    assert_contains_sql(
        &function,
        "'field', 'semantic_assertion', 'reason', 'duplicate_semantic_assertion'",
    );
    assert_contains_sql(&function, "'operation', 'update_assertion'");
    assert!(
        !function.contains("INSERT INTO KNOWLEDGE_GRAPH.GRAPH_ASSERTION_EVIDENCE")
            && !function.contains("DELETE FROM KNOWLEDGE_GRAPH.GRAPH_ASSERTION_EVIDENCE"),
        "update must retain every evidence association"
    );

    let acquire_locks_end = acquire_locks
        + function[acquire_locks..]
            .find(");")
            .expect("update lock-helper call should terminate")
        + 2;
    let post_lock_phase = &function[acquire_locks_end..update];
    assert!(
        !post_lock_phase.contains("GRAPH_LOCK_KEY_")
            && !post_lock_phase.contains("GRAPH_ACQUIRE_MUTATION_LOCKS"),
        "update must not discover or acquire new keys after ordered locking"
    );
}

#[test]
fn migration_assertion_delete_locks_snapshotted_evidence_before_current_revision_cascade() {
    let function = function_definition(
        "GRAPH_DELETE_ASSERTION(P_ASSERTION_ID UUID, P_EXPECTED_REVISION BIGINT)",
    );
    let assertion_snapshot = function
        .find("INTO SNAPSHOT_REVISION, SNAPSHOT_SUBJECT_CONCEPT_ID")
        .expect("delete should snapshot the assertion before acquiring locks");
    let evidence_snapshot = function
        .find("INTO SNAPSHOT_EVIDENCE, SNAPSHOT_SOURCE_REF_IDS")
        .expect("delete should snapshot evidence identities and source row IDs");
    let source_identity_key = function
        .find("GRAPH_LOCK_KEY_SOURCE_IDENTITY(")
        .expect("delete should lock every snapshotted evidence source identity");
    let acquire_locks = function
        .find("PERFORM KNOWLEDGE_GRAPH.GRAPH_ACQUIRE_MUTATION_LOCKS(")
        .expect("delete should use the canonical ordered-lock helper");
    let snapshot_check = function
        .find("CURRENT_SUBJECT_CONCEPT_ID IS DISTINCT FROM SNAPSHOT_SUBJECT_CONCEPT_ID")
        .expect("delete should revalidate assertion semantic fields");
    let evidence_check = function
        .find("CURRENT_EVIDENCE IS DISTINCT FROM SNAPSHOT_EVIDENCE")
        .expect("delete should revalidate the complete evidence snapshot");
    let stale_check = function
        .find("P_EXPECTED_REVISION IS DISTINCT FROM CURRENT_REVISION")
        .expect("delete should require the expected current revision");
    let assertion_delete = function
        .find("DELETE FROM KNOWLEDGE_GRAPH.GRAPH_ASSERTIONS")
        .expect("delete should rely on the assertion evidence cascade");

    assert!(
        assertion_snapshot < evidence_snapshot
            && evidence_snapshot < source_identity_key
            && source_identity_key < acquire_locks
            && acquire_locks < snapshot_check
            && snapshot_check < evidence_check
            && evidence_check < stale_check
            && stale_check < assertion_delete,
        "delete must lock and revalidate the complete assertion/evidence snapshot before DML"
    );
    assert_contains_sql(&function, "ARRAY[p_assertion_id]");
    assert_contains_sql(
        &function,
        "ARRAY[snapshot_subject_concept_id, snapshot_object_concept_id]",
    );
    assert_contains_sql(
        &function,
        "COALESCE(snapshot_source_ref_ids, ARRAY[]::BIGINT[])",
    );
    assert_contains_sql(&function, "'source_kind', source_references.source_kind");
    assert_contains_sql(&function, "'external_id', source_references.external_id");
    assert_contains_sql(
        &function,
        "'external_version', source_references.external_version",
    );
    assert_contains_sql(&function, "'operation', 'delete_assertion'");
    assert!(
        !function.contains("DELETE FROM KNOWLEDGE_GRAPH.GRAPH_ASSERTION_EVIDENCE"),
        "assertion deletion should cascade its evidence associations"
    );

    let acquire_locks_end = acquire_locks
        + function[acquire_locks..]
            .find(");")
            .expect("delete lock-helper call should terminate")
        + 2;
    let post_lock_phase = &function[acquire_locks_end..assertion_delete];
    assert!(
        !post_lock_phase.contains("GRAPH_LOCK_KEY_")
            && !post_lock_phase.contains("GRAPH_ACQUIRE_MUTATION_LOCKS"),
        "delete must not discover or acquire new keys after ordered locking"
    );
}

#[test]
fn migration_evidence_mutations_return_typed_outcomes_and_revoke_public_execution() {
    let add = function_definition(
        "GRAPH_ADD_ASSERTION_EVIDENCE(P_ASSERTION_ID UUID, P_SOURCE_KIND TEXT, \
         P_EXTERNAL_ID TEXT, P_EXTERNAL_VERSION BIGINT)",
    );
    let remove = function_definition(
        "GRAPH_REMOVE_ASSERTION_EVIDENCE(P_ASSERTION_ID UUID, P_SOURCE_KIND TEXT, \
         P_EXTERNAL_ID TEXT, P_EXTERNAL_VERSION BIGINT)",
    );

    assert_typed_mutation_function(
        &add,
        &[
            "created",
            "existing",
            "evidence_limit",
            "missing",
            "conflict",
            "invalid_input",
        ],
    );
    assert_typed_mutation_function(
        &remove,
        &[
            "deleted",
            "missing",
            "would_orphan_evidence",
            "conflict",
            "invalid_input",
        ],
    );

    for outcome in ["missing", "evidence_limit", "conflict", "invalid_input"] {
        assert_source_identity_omitted(&add, outcome);
    }
    for outcome in [
        "missing",
        "would_orphan_evidence",
        "conflict",
        "invalid_input",
    ] {
        assert_source_identity_omitted(&remove, outcome);
    }

    for outcome in ["created", "existing"] {
        let returns = outcome_returns(&add, outcome);
        assert_eq!(returns.len(), 1, "add should have one {outcome} path");
        assert_contains_sql(&returns[0], "'assertion_id', p_assertion_id");
        assert_contains_sql(&returns[0], "'source', current_source_json");
        assert!(
            !returns[0].contains("SOURCE_REF_ID"),
            "evidence success should return the typed source, not its internal row ID"
        );
    }

    let would_orphan = outcome_returns(&remove, "would_orphan_evidence");
    assert_eq!(would_orphan.len(), 1);
    assert_contains_sql(&would_orphan[0], "'assertion_id', p_assertion_id");
    assert_contains_sql(&would_orphan[0], "'current_revision', current_revision");

    let migration = executable_sql();
    for signature in [
        "knowledge_graph.graph_add_assertion_evidence(uuid, text, text, bigint)",
        "knowledge_graph.graph_remove_assertion_evidence(uuid, text, text, bigint)",
    ] {
        assert_contains_sql(
            &migration,
            &format!("REVOKE ALL ON FUNCTION {signature} FROM PUBLIC"),
        );
    }
}

#[test]
fn migration_evidence_add_locks_both_identities_and_checks_duplicates_before_the_cap() {
    let function = function_definition(
        "GRAPH_ADD_ASSERTION_EVIDENCE(P_ASSERTION_ID UUID, P_SOURCE_KIND TEXT, \
         P_EXTERNAL_ID TEXT, P_EXTERNAL_VERSION BIGINT)",
    );
    let assertion_key = function
        .find("GRAPH_LOCK_KEY_ASSERTION_ID(P_ASSERTION_ID)")
        .expect("add should use the same assertion identity lock as assertion deletion");
    let source_key = function
        .find("GRAPH_LOCK_KEY_SOURCE_IDENTITY(")
        .expect("add should use the canonical typed source identity lock");
    let source_snapshot = function
        .find("INTO SNAPSHOT_SOURCE_REF_ID")
        .expect("add should snapshot the existing source row before locking");
    let acquire_locks = function
        .find("PERFORM KNOWLEDGE_GRAPH.GRAPH_ACQUIRE_MUTATION_LOCKS(")
        .expect("add should use the canonical advisory and row-lock helper");
    let assertion_revalidation = function
        .find("INTO CURRENT_REVISION")
        .expect("add should recheck the assertion after acquiring its lock");
    let source_revalidation = function
        .find("INTO CURRENT_SOURCE_REF_ID")
        .expect("add should revalidate and read the locked typed source identity");
    let duplicate_check = function
        .find(&compact_sql(
            "IF EXISTS ( SELECT 1 FROM KNOWLEDGE_GRAPH.GRAPH_ASSERTION_EVIDENCE AS EVIDENCE \
             WHERE EVIDENCE.ASSERTION_ID = P_ASSERTION_ID \
             AND EVIDENCE.SOURCE_REF_ID = CURRENT_SOURCE_REF_ID ) THEN",
        ))
        .expect("add should classify an existing association before counting evidence");
    let existing_return = function
        .find("'OUTCOME', 'EXISTING'")
        .expect("duplicate add should return existing evidence");
    let evidence_count = function
        .find("INTO CURRENT_EVIDENCE_COUNT")
        .expect("add should count evidence under the assertion row lock");
    let evidence_limit = function
        .find("CURRENT_EVIDENCE_COUNT >= 32")
        .expect("new evidence at the total cap should be rejected");
    let evidence_insert = function
        .find("INSERT INTO KNOWLEDGE_GRAPH.GRAPH_ASSERTION_EVIDENCE")
        .expect("add should insert the requested evidence association");

    assert!(
        assertion_key < source_key
            && source_key < source_snapshot
            && source_snapshot < acquire_locks
            && acquire_locks < assertion_revalidation
            && assertion_revalidation < source_revalidation
            && source_revalidation < duplicate_check
            && duplicate_check < existing_return
            && existing_return < evidence_count
            && evidence_count < evidence_limit
            && evidence_limit < evidence_insert,
        "add must lock and revalidate both identities, return duplicates, then enforce the cap before insertion"
    );
    assert_contains_sql(&function, "ARRAY[P_ASSERTION_ID]");
    assert_contains_sql(&function, "ARRAY[SNAPSHOT_SOURCE_REF_ID]");
    assert_contains_sql(
        &function,
        "CURRENT_SOURCE_REF_ID IS DISTINCT FROM SNAPSHOT_SOURCE_REF_ID",
    );
    assert_contains_sql(
        &function,
        "SELECT PG_CATALOG.COUNT(*) INTO CURRENT_EVIDENCE_COUNT \
         FROM KNOWLEDGE_GRAPH.GRAPH_ASSERTION_EVIDENCE AS EVIDENCE \
         WHERE EVIDENCE.ASSERTION_ID = P_ASSERTION_ID",
    );
    assert_contains_sql(
        &function,
        "ON CONFLICT (ASSERTION_ID, SOURCE_REF_ID) DO NOTHING",
    );
    assert!(
        !function.contains("UPDATE KNOWLEDGE_GRAPH.GRAPH_ASSERTIONS"),
        "evidence addition must not revise the assertion aggregate"
    );

    let acquire_locks_end = acquire_locks
        + function[acquire_locks..]
            .find(");")
            .expect("add lock-helper call should terminate")
        + 2;
    let post_lock_phase = &function[acquire_locks_end..evidence_insert];
    assert!(
        !post_lock_phase.contains("GRAPH_LOCK_KEY_")
            && !post_lock_phase.contains("GRAPH_ACQUIRE_MUTATION_LOCKS"),
        "add must not discover or acquire additional locks after ordered locking"
    );
}

#[test]
fn migration_evidence_remove_preserves_the_last_association_and_deletes_only_the_selected_row() {
    let function = function_definition(
        "GRAPH_REMOVE_ASSERTION_EVIDENCE(P_ASSERTION_ID UUID, P_SOURCE_KIND TEXT, \
         P_EXTERNAL_ID TEXT, P_EXTERNAL_VERSION BIGINT)",
    );
    let assertion_key = function
        .find("GRAPH_LOCK_KEY_ASSERTION_ID(P_ASSERTION_ID)")
        .expect("remove should use the same assertion identity lock as assertion deletion");
    let source_key = function
        .find("GRAPH_LOCK_KEY_SOURCE_IDENTITY(")
        .expect("remove should use the canonical typed source identity lock");
    let source_snapshot = function
        .find("INTO SNAPSHOT_SOURCE_REF_ID")
        .expect("remove should snapshot the source row before locking");
    let acquire_locks = function
        .find("PERFORM KNOWLEDGE_GRAPH.GRAPH_ACQUIRE_MUTATION_LOCKS(")
        .expect("remove should use the canonical advisory and row-lock helper");
    let assertion_revalidation = function
        .find("INTO CURRENT_REVISION")
        .expect("remove should read the locked assertion revision");
    let source_revalidation = function
        .find("INTO CURRENT_SOURCE_REF_ID")
        .expect("remove should revalidate and read the locked source identity");
    let evidence_check = function
        .find(&compact_sql(
            "IF NOT EXISTS ( SELECT 1 FROM KNOWLEDGE_GRAPH.GRAPH_ASSERTION_EVIDENCE AS EVIDENCE \
             WHERE EVIDENCE.ASSERTION_ID = P_ASSERTION_ID \
             AND EVIDENCE.SOURCE_REF_ID = CURRENT_SOURCE_REF_ID ) THEN",
        ))
        .expect("remove should return missing when the selected association is absent");
    let remaining_evidence_count = function
        .find("INTO OTHER_EVIDENCE_COUNT")
        .expect("remove should count the other evidence associations");
    let would_orphan = function
        .find("'OUTCOME', 'WOULD_ORPHAN_EVIDENCE'")
        .expect("remove should reject deleting the final evidence association");
    let evidence_delete = function
        .find("DELETE FROM KNOWLEDGE_GRAPH.GRAPH_ASSERTION_EVIDENCE")
        .expect("remove should delete only the selected association");

    assert!(
        assertion_key < source_key
            && source_key < source_snapshot
            && source_snapshot < acquire_locks
            && acquire_locks < assertion_revalidation
            && assertion_revalidation < source_revalidation
            && source_revalidation < evidence_check
            && evidence_check < remaining_evidence_count
            && remaining_evidence_count < would_orphan
            && would_orphan < evidence_delete,
        "remove must lock and revalidate both identities, classify absence and orphaning, then delete"
    );
    assert_contains_sql(&function, "ARRAY[P_ASSERTION_ID]");
    assert_contains_sql(&function, "ARRAY[SNAPSHOT_SOURCE_REF_ID]");
    assert_contains_sql(
        &function,
        "CURRENT_SOURCE_REF_ID IS DISTINCT FROM SNAPSHOT_SOURCE_REF_ID",
    );
    assert_contains_sql(
        &function,
        "WHERE EVIDENCE.ASSERTION_ID = P_ASSERTION_ID \
         AND EVIDENCE.SOURCE_REF_ID <> CURRENT_SOURCE_REF_ID",
    );
    assert_contains_sql(
        &function,
        "WHERE EVIDENCE.ASSERTION_ID = P_ASSERTION_ID \
         AND EVIDENCE.SOURCE_REF_ID = CURRENT_SOURCE_REF_ID",
    );
    assert_eq!(
        function
            .matches("DELETE FROM KNOWLEDGE_GRAPH.GRAPH_ASSERTION_EVIDENCE")
            .count(),
        1,
        "remove should delete one association and preserve all other evidence"
    );
    for unrelated_mutation in [
        "DELETE FROM KNOWLEDGE_GRAPH.GRAPH_ASSERTIONS",
        "DELETE FROM KNOWLEDGE_GRAPH.GRAPH_SOURCE_REFERENCES",
        "UPDATE KNOWLEDGE_GRAPH.GRAPH_ASSERTIONS",
    ] {
        assert!(
            !function.contains(unrelated_mutation),
            "remove must preserve unrelated or revisioned graph state: {unrelated_mutation}"
        );
    }

    let acquire_locks_end = acquire_locks
        + function[acquire_locks..]
            .find(");")
            .expect("remove lock-helper call should terminate")
        + 2;
    let post_lock_phase = &function[acquire_locks_end..evidence_delete];
    assert!(
        !post_lock_phase.contains("GRAPH_LOCK_KEY_")
            && !post_lock_phase.contains("GRAPH_ACQUIRE_MUTATION_LOCKS"),
        "remove must not discover or acquire additional locks after ordered locking"
    );
}
