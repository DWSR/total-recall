-- The migration execution role creates and owns the dedicated knowledge_graph schema.
-- This migration intentionally grants no application-role privileges.

DO $encoding$
BEGIN
  IF pg_catalog.current_setting('server_encoding') <> 'UTF8' THEN
    RAISE EXCEPTION 'knowledge graph requires UTF8 database encoding'
      USING ERRCODE = '22023';
  END IF;
END;
$encoding$;

CREATE SCHEMA knowledge_graph AUTHORIZATION CURRENT_USER;

CREATE TABLE knowledge_graph.graph_relation_types (
  code text COLLATE "C" NOT NULL,
  is_symmetric boolean NOT NULL,
  CONSTRAINT graph_relation_types_pkey PRIMARY KEY (code),
  CONSTRAINT graph_relation_types_code_check CHECK (
    code IN (
      'related_to',
      'is_a',
      'part_of',
      'depends_on',
      'uses',
      'implements',
      'causes',
      'resolves',
      'contradicts'
    )
  )
);

INSERT INTO knowledge_graph.graph_relation_types (code, is_symmetric)
VALUES
  ('related_to', TRUE),
  ('is_a', FALSE),
  ('part_of', FALSE),
  ('depends_on', FALSE),
  ('uses', FALSE),
  ('implements', FALSE),
  ('causes', FALSE),
  ('resolves', FALSE),
  ('contradicts', TRUE);

CREATE TABLE knowledge_graph.graph_concepts (
  id uuid NOT NULL,
  revision bigint NOT NULL DEFAULT 1,
  CONSTRAINT graph_concepts_pkey PRIMARY KEY (id),
  CONSTRAINT graph_concepts_revision_positive CHECK (revision > 0),
  CONSTRAINT graph_concepts_uuid_v7_check CHECK (
    pg_catalog.substring(id::text, 15, 1) = '7'
    AND pg_catalog.substring(id::text, 20, 1) IN ('8', '9', 'a', 'b')
  )
);

CREATE TABLE knowledge_graph.graph_aliases (
  concept_id uuid NOT NULL,
  alias_key text COLLATE "C" NOT NULL,
  display_text text NOT NULL,
  is_preferred boolean NOT NULL,
  normalization_version text COLLATE "C" NOT NULL,
  CONSTRAINT graph_aliases_pkey PRIMARY KEY (alias_key),
  CONSTRAINT graph_aliases_alias_key_bounds CHECK (
    alias_key <> '' AND pg_catalog.octet_length(alias_key) BETWEEN 1 AND 2048
  ),
  CONSTRAINT graph_aliases_display_text_bounds CHECK (
    display_text <> '' AND pg_catalog.octet_length(display_text) BETWEEN 1 AND 512
  ),
  CONSTRAINT graph_aliases_normalization_version_check CHECK (
    normalization_version = 'icu4x-2.3.0-nfkc-fold-v1'
  ),
  CONSTRAINT graph_aliases_concept_fk
    FOREIGN KEY (concept_id)
    REFERENCES knowledge_graph.graph_concepts (id)
    ON UPDATE RESTRICT
    ON DELETE CASCADE
    NOT DEFERRABLE
);

CREATE UNIQUE INDEX graph_aliases_one_preferred_per_concept_uidx
ON knowledge_graph.graph_aliases (concept_id)
WHERE is_preferred;

CREATE INDEX graph_aliases_concept_order_idx
ON knowledge_graph.graph_aliases (concept_id ASC, is_preferred DESC, alias_key ASC);

CREATE TABLE knowledge_graph.graph_source_references (
  source_ref_id bigint GENERATED ALWAYS AS IDENTITY,
  source_kind text COLLATE "C" NOT NULL,
  external_id text COLLATE "C" NOT NULL,
  external_version bigint,
  CONSTRAINT graph_source_references_pkey PRIMARY KEY (source_ref_id),
  CONSTRAINT graph_source_references_kind_check CHECK (
    source_kind IN ('memory_version', 'session_record')
  ),
  CONSTRAINT graph_source_references_shape_check CHECK (
    (source_kind = 'memory_version' AND external_version IS NOT NULL AND external_version > 0)
    OR (source_kind = 'session_record' AND external_version IS NULL)
  ),
  CONSTRAINT graph_source_references_external_id_bounds CHECK (
    external_id <> '' AND pg_catalog.octet_length(external_id) BETWEEN 1 AND 2048
  ),
  CONSTRAINT graph_source_references_identity_unique
    UNIQUE NULLS NOT DISTINCT (source_kind, external_id, external_version)
);

CREATE TABLE knowledge_graph.graph_concept_mentions (
  concept_id uuid NOT NULL,
  source_ref_id bigint NOT NULL,
  CONSTRAINT graph_concept_mentions_pkey PRIMARY KEY (concept_id, source_ref_id),
  CONSTRAINT graph_concept_mentions_concept_fk
    FOREIGN KEY (concept_id)
    REFERENCES knowledge_graph.graph_concepts (id)
    ON UPDATE RESTRICT
    ON DELETE RESTRICT
    NOT DEFERRABLE,
  CONSTRAINT graph_concept_mentions_source_fk
    FOREIGN KEY (source_ref_id)
    REFERENCES knowledge_graph.graph_source_references (source_ref_id)
    ON UPDATE RESTRICT
    ON DELETE RESTRICT
    NOT DEFERRABLE
);

CREATE INDEX graph_concept_mentions_source_idx
ON knowledge_graph.graph_concept_mentions (source_ref_id ASC, concept_id ASC);

CREATE TABLE knowledge_graph.graph_assertions (
  id uuid NOT NULL,
  subject_concept_id uuid NOT NULL,
  relation_code text COLLATE "C" NOT NULL,
  object_concept_id uuid NOT NULL,
  revision bigint NOT NULL DEFAULT 1,
  CONSTRAINT graph_assertions_pkey PRIMARY KEY (id),
  CONSTRAINT graph_assertions_revision_positive CHECK (revision > 0),
  CONSTRAINT graph_assertions_uuid_v7_check CHECK (
    pg_catalog.substring(id::text, 15, 1) = '7'
    AND pg_catalog.substring(id::text, 20, 1) IN ('8', '9', 'a', 'b')
  ),
  CONSTRAINT graph_assertions_distinct_endpoints CHECK (
    subject_concept_id <> object_concept_id
  ),
  CONSTRAINT graph_assertions_semantic_identity
    UNIQUE (subject_concept_id, relation_code, object_concept_id),
  CONSTRAINT graph_assertions_subject_concept_fk
    FOREIGN KEY (subject_concept_id)
    REFERENCES knowledge_graph.graph_concepts (id)
    ON UPDATE RESTRICT
    ON DELETE RESTRICT
    NOT DEFERRABLE,
  CONSTRAINT graph_assertions_relation_fk
    FOREIGN KEY (relation_code)
    REFERENCES knowledge_graph.graph_relation_types (code)
    ON UPDATE RESTRICT
    ON DELETE RESTRICT
    NOT DEFERRABLE,
  CONSTRAINT graph_assertions_object_concept_fk
    FOREIGN KEY (object_concept_id)
    REFERENCES knowledge_graph.graph_concepts (id)
    ON UPDATE RESTRICT
    ON DELETE RESTRICT
    NOT DEFERRABLE
);

CREATE INDEX graph_assertions_subject_adjacency_idx
ON knowledge_graph.graph_assertions
  (subject_concept_id ASC, relation_code ASC, object_concept_id ASC, id ASC);

CREATE INDEX graph_assertions_object_adjacency_idx
ON knowledge_graph.graph_assertions
  (object_concept_id ASC, relation_code ASC, subject_concept_id ASC, id ASC);

CREATE TABLE knowledge_graph.graph_assertion_evidence (
  assertion_id uuid NOT NULL,
  source_ref_id bigint NOT NULL,
  CONSTRAINT graph_assertion_evidence_pkey PRIMARY KEY (assertion_id, source_ref_id),
  CONSTRAINT graph_assertion_evidence_assertion_fk
    FOREIGN KEY (assertion_id)
    REFERENCES knowledge_graph.graph_assertions (id)
    ON UPDATE RESTRICT
    ON DELETE CASCADE
    NOT DEFERRABLE,
  CONSTRAINT graph_assertion_evidence_source_fk
    FOREIGN KEY (source_ref_id)
    REFERENCES knowledge_graph.graph_source_references (source_ref_id)
    ON UPDATE RESTRICT
    ON DELETE RESTRICT
    NOT DEFERRABLE
);

CREATE INDEX graph_assertion_evidence_source_idx
ON knowledge_graph.graph_assertion_evidence (source_ref_id ASC, assertion_id ASC);

-- JSONB supplies canonical member ordering and escaping for every lock identity.
-- Mutation callers pass the complete aggregate and uniqueness key array before
-- this helper acquires any lock.
CREATE FUNCTION knowledge_graph.graph_lock_key_concept_id(p_concept_id uuid)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
STRICT
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
  SELECT pg_catalog.jsonb_build_object(
    'domain', 'knowledge_graph.concept_id.v1',
    'concept_id', p_concept_id
  )
$function$;

CREATE FUNCTION knowledge_graph.graph_lock_key_normalized_alias(p_normalized_alias text)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
STRICT
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
  SELECT pg_catalog.jsonb_build_object(
    'domain', 'knowledge_graph.normalized_alias.v1',
    'normalized_alias', p_normalized_alias
  )
$function$;

CREATE FUNCTION knowledge_graph.graph_lock_key_source_identity(
  p_source_kind text,
  p_external_id text,
  p_external_version bigint
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
  IF p_source_kind IS NULL
    OR p_source_kind COLLATE "C" NOT IN ('memory_version', 'session_record')
    OR p_external_id IS NULL
    OR p_external_id = ''
    OR pg_catalog.octet_length(p_external_id) NOT BETWEEN 1 AND 2048
    OR (p_source_kind COLLATE "C" = 'memory_version'
      AND (p_external_version IS NULL OR p_external_version <= 0))
    OR (p_source_kind COLLATE "C" = 'session_record' AND p_external_version IS NOT NULL)
  THEN
    RAISE EXCEPTION 'invalid graph source lock identity' USING ERRCODE = '22023';
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'domain', 'knowledge_graph.source_identity.v1',
    'source_kind', p_source_kind,
    'external_id', p_external_id,
    'external_version', p_external_version
  );
END;
$function$;

CREATE FUNCTION knowledge_graph.graph_lock_key_concept_mention(
  p_concept_id uuid,
  p_source_kind text,
  p_external_id text,
  p_external_version bigint
)
RETURNS jsonb
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
  SELECT pg_catalog.jsonb_build_object(
    'domain', 'knowledge_graph.concept_mention.v1',
    'concept_id', p_concept_id,
    'source_identity', knowledge_graph.graph_lock_key_source_identity(
      p_source_kind,
      p_external_id,
      p_external_version
    )
  )
$function$;

CREATE FUNCTION knowledge_graph.graph_lock_key_assertion_id(p_assertion_id uuid)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
STRICT
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
  SELECT pg_catalog.jsonb_build_object(
    'domain', 'knowledge_graph.assertion_id.v1',
    'assertion_id', p_assertion_id
  )
$function$;

CREATE FUNCTION knowledge_graph.graph_lock_key_semantic_assertion(
  p_relation_code text,
  p_subject_concept_id uuid,
  p_object_concept_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
STRICT
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  relation_is_symmetric boolean;
  canonical_subject_concept_id uuid := p_subject_concept_id;
  canonical_object_concept_id uuid := p_object_concept_id;
BEGIN
  SELECT relation_types.is_symmetric
  INTO relation_is_symmetric
  FROM knowledge_graph.graph_relation_types AS relation_types
  WHERE relation_types.code = p_relation_code COLLATE "C";

  IF NOT FOUND THEN
    RAISE EXCEPTION 'invalid graph semantic lock key' USING ERRCODE = '22023';
  END IF;

  IF relation_is_symmetric AND canonical_subject_concept_id > canonical_object_concept_id THEN
    canonical_subject_concept_id := p_object_concept_id;
    canonical_object_concept_id := p_subject_concept_id;
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'domain', 'knowledge_graph.semantic_assertion.v1',
    'relation_code', p_relation_code,
    'subject_concept_id', canonical_subject_concept_id,
    'object_concept_id', canonical_object_concept_id
  );
END;
$function$;

CREATE FUNCTION knowledge_graph.graph_acquire_mutation_locks(
  p_lock_keys jsonb,
  p_concept_ids uuid[],
  p_assertion_ids uuid[],
  p_source_ref_ids bigint[]
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  advisory_lock_id bigint;
  locked_concept_id uuid;
  locked_assertion_id uuid;
  locked_source_ref_id bigint;
BEGIN
  IF p_lock_keys IS NULL OR pg_catalog.jsonb_typeof(p_lock_keys) <> 'array' THEN
    RAISE EXCEPTION 'invalid graph mutation lock inputs' USING ERRCODE = '22023';
  END IF;

  IF pg_catalog.jsonb_array_length(p_lock_keys) = 0 OR EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements(p_lock_keys) AS supplied(lock_key)
    WHERE pg_catalog.jsonb_typeof(supplied.lock_key) <> 'object'
      OR pg_catalog.jsonb_typeof(supplied.lock_key -> 'domain') IS DISTINCT FROM 'string'
  ) THEN
    RAISE EXCEPTION 'invalid graph mutation lock inputs' USING ERRCODE = '22023';
  END IF;

  FOR advisory_lock_id IN
    SELECT DISTINCT pg_catalog.hashtextextended(key_item.value::text, 0) AS lock_id
    FROM pg_catalog.jsonb_array_elements(p_lock_keys) AS key_item(value)
    ORDER BY lock_id ASC
  LOOP
    PERFORM pg_catalog.pg_advisory_xact_lock(advisory_lock_id);
  END LOOP;

  FOR locked_concept_id IN
    SELECT concepts.id
    FROM knowledge_graph.graph_concepts AS concepts
    WHERE concepts.id = ANY(COALESCE(p_concept_ids, ARRAY[]::uuid[]))
    ORDER BY concepts.id ASC
    FOR UPDATE OF concepts
  LOOP
    NULL;
  END LOOP;

  FOR locked_assertion_id IN
    SELECT assertions.id
    FROM knowledge_graph.graph_assertions AS assertions
    JOIN knowledge_graph.graph_relation_types AS relation_types
      ON relation_types.code = assertions.relation_code
    WHERE assertions.id = ANY(COALESCE(p_assertion_ids, ARRAY[]::uuid[]))
      OR EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements(p_lock_keys) AS supplied(lock_key)
        WHERE supplied.lock_key ->> 'domain' = 'knowledge_graph.semantic_assertion.v1'
          AND supplied.lock_key = pg_catalog.jsonb_build_object(
            'domain', 'knowledge_graph.semantic_assertion.v1',
            'relation_code', assertions.relation_code,
            'subject_concept_id', CASE
              WHEN relation_types.is_symmetric
                AND assertions.subject_concept_id > assertions.object_concept_id
              THEN assertions.object_concept_id
              ELSE assertions.subject_concept_id
            END,
            'object_concept_id', CASE
              WHEN relation_types.is_symmetric
                AND assertions.subject_concept_id > assertions.object_concept_id
              THEN assertions.subject_concept_id
              ELSE assertions.object_concept_id
            END
          )
      )
    ORDER BY assertions.id ASC
    FOR UPDATE OF assertions
  LOOP
    NULL;
  END LOOP;

  FOR locked_source_ref_id IN
    SELECT source_references.source_ref_id
    FROM knowledge_graph.graph_source_references AS source_references
    WHERE source_references.source_ref_id = ANY(
      COALESCE(p_source_ref_ids, ARRAY[]::bigint[])
    )
    ORDER BY source_references.source_ref_id ASC
    FOR UPDATE OF source_references
  LOOP
    NULL;
  END LOOP;
END;
$function$;

CREATE FUNCTION knowledge_graph.graph_create_concept(
  p_candidate_id uuid,
  p_aliases jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  lock_keys jsonb := '[]'::jsonb;
  requested_alias record;
  alias_count bigint;
  existing_concept_id uuid;
  existing_revision bigint;
  conflict_concept_id uuid;
  conflict_revision bigint;
  owner_count bigint;
  candidate_revision bigint;
BEGIN
  lock_keys := pg_catalog.jsonb_build_array(
    knowledge_graph.graph_lock_key_concept_id(p_candidate_id)
  );

  IF p_aliases IS NOT NULL AND pg_catalog.jsonb_typeof(p_aliases) = 'array' THEN
    FOR requested_alias IN
      SELECT item.value ->> 'alias_key' AS alias_key
      FROM pg_catalog.jsonb_array_elements(p_aliases) AS item(value)
      WHERE pg_catalog.jsonb_typeof(item.value) = 'object'
        AND pg_catalog.jsonb_typeof(item.value -> 'alias_key') = 'string'
        AND item.value ->> 'alias_key' <> ''
    LOOP
      lock_keys := lock_keys || pg_catalog.jsonb_build_array(
        knowledge_graph.graph_lock_key_normalized_alias(requested_alias.alias_key)
      );
    END LOOP;
  END IF;

  PERFORM knowledge_graph.graph_acquire_mutation_locks(
    lock_keys,
    ARRAY[p_candidate_id],
    ARRAY[]::uuid[],
    ARRAY[]::bigint[]
  );

  SELECT pg_catalog.count(*)
  INTO alias_count
  FROM pg_catalog.jsonb_array_elements(p_aliases) AS requested(value);

  SELECT concepts.id, concepts.revision
  INTO existing_concept_id, existing_revision
  FROM knowledge_graph.graph_concepts AS concepts
  WHERE (
    SELECT pg_catalog.count(*)
    FROM knowledge_graph.graph_aliases AS aliases
    WHERE aliases.concept_id = concepts.id
  ) = alias_count
    AND NOT EXISTS (
      SELECT 1
      FROM pg_catalog.jsonb_array_elements(p_aliases) AS requested(value)
      WHERE NOT EXISTS (
        SELECT 1
        FROM knowledge_graph.graph_aliases AS aliases
        WHERE aliases.concept_id = concepts.id
          AND aliases.alias_key = (requested.value ->> 'alias_key') COLLATE "C"
          AND aliases.is_preferred = (requested.value ->> 'is_preferred')::boolean
      )
    )
  ORDER BY concepts.id ASC
  LIMIT 1;

  IF FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'existing',
      'record', pg_catalog.jsonb_build_object(
        'id', existing_concept_id,
        'revision', existing_revision,
        'aliases', (
          SELECT COALESCE(
            pg_catalog.jsonb_agg(
              pg_catalog.jsonb_build_object(
                'display_text', aliases.display_text,
                'preferred', aliases.is_preferred
              )
              ORDER BY aliases.is_preferred DESC, aliases.alias_key COLLATE "C" ASC
            ),
            '[]'::jsonb
          )
          FROM knowledge_graph.graph_aliases AS aliases
          WHERE aliases.concept_id = existing_concept_id
        )
      )
    );
  END IF;

  SELECT pg_catalog.count(DISTINCT aliases.concept_id)
  INTO owner_count
  FROM knowledge_graph.graph_aliases AS aliases
  WHERE EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements(p_aliases) AS requested(value)
    WHERE aliases.alias_key = (requested.value ->> 'alias_key') COLLATE "C"
  );

  IF owner_count > 0 THEN
    SELECT concepts.id, concepts.revision
    INTO conflict_concept_id, conflict_revision
    FROM knowledge_graph.graph_concepts AS concepts
    WHERE EXISTS (
      SELECT 1
      FROM knowledge_graph.graph_aliases AS aliases
      WHERE aliases.concept_id = concepts.id
        AND EXISTS (
          SELECT 1
          FROM pg_catalog.jsonb_array_elements(p_aliases) AS requested(value)
          WHERE aliases.alias_key = (requested.value ->> 'alias_key') COLLATE "C"
        )
    )
    ORDER BY concepts.id ASC
    LIMIT 1;

    IF owner_count = 1 THEN
      RETURN pg_catalog.jsonb_build_object(
        'outcome', 'conflict',
        'field', 'alias_set',
        'reason', 'alias_set_mismatch',
        'identity', pg_catalog.jsonb_build_object(
          'kind', 'concept',
          'id', conflict_concept_id
        ),
        'current_revision', conflict_revision
      );
    END IF;

    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'conflict',
      'field', 'alias_set',
      'reason', 'alias_owned',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'concept',
        'id', conflict_concept_id
      ),
      'current_revision', conflict_revision
    );
  END IF;

  SELECT concepts.revision
  INTO candidate_revision
  FROM knowledge_graph.graph_concepts AS concepts
  WHERE concepts.id = p_candidate_id;

  IF FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'conflict',
      'field', 'alias_set',
      'reason', 'alias_set_mismatch',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'concept',
        'id', p_candidate_id
      ),
      'current_revision', candidate_revision
    );
  END IF;

  INSERT INTO knowledge_graph.graph_concepts (id, revision)
  VALUES (p_candidate_id, 1);

  INSERT INTO knowledge_graph.graph_aliases (
    concept_id,
    alias_key,
    display_text,
    is_preferred,
    normalization_version
  )
  SELECT
    p_candidate_id,
    requested.value ->> 'alias_key',
    requested.value ->> 'display_text',
    (requested.value ->> 'is_preferred')::boolean,
    'icu4x-2.3.0-nfkc-fold-v1'
  FROM pg_catalog.jsonb_array_elements(p_aliases) AS requested(value)
  ORDER BY (requested.value ->> 'alias_key') COLLATE "C" ASC;

  RETURN pg_catalog.jsonb_build_object(
    'outcome', 'created',
    'record', pg_catalog.jsonb_build_object(
      'id', p_candidate_id,
      'revision', 1,
      'aliases', (
        SELECT COALESCE(
          pg_catalog.jsonb_agg(
            pg_catalog.jsonb_build_object(
              'display_text', aliases.display_text,
              'preferred', aliases.is_preferred
            )
            ORDER BY aliases.is_preferred DESC, aliases.alias_key COLLATE "C" ASC
          ),
          '[]'::jsonb
        )
        FROM knowledge_graph.graph_aliases AS aliases
        WHERE aliases.concept_id = p_candidate_id
      )
    )
  );
END;
$function$;

CREATE FUNCTION knowledge_graph.graph_replace_concept_aliases(
  p_concept_id uuid,
  p_expected_revision bigint,
  p_aliases jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  lock_keys jsonb := '[]'::jsonb;
  snapshot_exists boolean := FALSE;
  snapshot_revision bigint;
  current_revision bigint;
  new_revision bigint;
  snapshot_alias_snapshot jsonb := '[]'::jsonb;
  current_alias_snapshot jsonb := '[]'::jsonb;
  snapshot_alias record;
  requested_alias record;
  alias_set_valid boolean := FALSE;
  invalid_alias_item boolean := FALSE;
  alias_count bigint := 0;
  preferred_alias_count bigint := 0;
  distinct_alias_count bigint := 0;
  conflict_concept_id uuid;
  conflict_revision bigint;
BEGIN
  SELECT
    concepts.revision,
    COALESCE(
      (
        SELECT pg_catalog.jsonb_agg(
          pg_catalog.jsonb_build_object(
            'alias_key', aliases.alias_key,
            'display_text', aliases.display_text,
            'is_preferred', aliases.is_preferred,
            'normalization_version', aliases.normalization_version
          )
          ORDER BY aliases.alias_key COLLATE "C" ASC
        )
        FROM knowledge_graph.graph_aliases AS aliases
        WHERE aliases.concept_id = concepts.id
      ),
      '[]'::jsonb
    )
  INTO snapshot_revision, snapshot_alias_snapshot
  FROM knowledge_graph.graph_concepts AS concepts
  WHERE concepts.id = p_concept_id;
  snapshot_exists := FOUND;

  lock_keys := pg_catalog.jsonb_build_array(
    knowledge_graph.graph_lock_key_concept_id(p_concept_id)
  );

  FOR snapshot_alias IN
    SELECT item.value ->> 'alias_key' AS alias_key
    FROM pg_catalog.jsonb_array_elements(snapshot_alias_snapshot) AS item(value)
  LOOP
    lock_keys := lock_keys || pg_catalog.jsonb_build_array(
      knowledge_graph.graph_lock_key_normalized_alias(snapshot_alias.alias_key)
    );
  END LOOP;

  IF p_aliases IS NOT NULL AND pg_catalog.jsonb_typeof(p_aliases) = 'array' THEN
    FOR requested_alias IN
      SELECT item.value ->> 'alias_key' AS alias_key
      FROM pg_catalog.jsonb_array_elements(p_aliases) AS item(value)
      WHERE pg_catalog.jsonb_typeof(item.value) = 'object'
        AND pg_catalog.jsonb_typeof(item.value -> 'alias_key') = 'string'
        AND item.value ->> 'alias_key' <> ''
    LOOP
      lock_keys := lock_keys || pg_catalog.jsonb_build_array(
        knowledge_graph.graph_lock_key_normalized_alias(requested_alias.alias_key)
      );
    END LOOP;
  END IF;

  PERFORM knowledge_graph.graph_acquire_mutation_locks(
    lock_keys,
    ARRAY[p_concept_id],
    ARRAY[]::uuid[],
    ARRAY[]::bigint[]
  );

  SELECT
    concepts.revision,
    COALESCE(
      (
        SELECT pg_catalog.jsonb_agg(
          pg_catalog.jsonb_build_object(
            'alias_key', aliases.alias_key,
            'display_text', aliases.display_text,
            'is_preferred', aliases.is_preferred,
            'normalization_version', aliases.normalization_version
          )
          ORDER BY aliases.alias_key COLLATE "C" ASC
        )
        FROM knowledge_graph.graph_aliases AS aliases
        WHERE aliases.concept_id = concepts.id
      ),
      '[]'::jsonb
    )
  INTO current_revision, current_alias_snapshot
  FROM knowledge_graph.graph_concepts AS concepts
  WHERE concepts.id = p_concept_id;

  IF NOT FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'concept',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'concept',
        'id', p_concept_id
      )
    );
  END IF;

  IF NOT snapshot_exists
    OR current_revision IS DISTINCT FROM snapshot_revision
    OR current_alias_snapshot IS DISTINCT FROM snapshot_alias_snapshot
  THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'conflict',
      'field', 'snapshot',
      'reason', 'snapshot_drift',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'concept',
        'id', p_concept_id
      ),
      'current_revision', current_revision
    );
  END IF;

  IF p_expected_revision IS DISTINCT FROM current_revision THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'stale',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'concept',
        'id', p_concept_id
      ),
      'current_revision', current_revision
    );
  END IF;

  IF p_aliases IS NULL OR pg_catalog.jsonb_typeof(p_aliases) IS DISTINCT FROM 'array' THEN
    alias_set_valid := FALSE;
  ELSE
    alias_count := pg_catalog.jsonb_array_length(p_aliases);
    IF NOT (alias_count BETWEEN 1 AND 64) THEN
      alias_set_valid := FALSE;
    ELSE
      SELECT EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements(p_aliases) AS item(value)
        WHERE pg_catalog.jsonb_typeof(item.value) IS DISTINCT FROM 'object'
      )
      INTO invalid_alias_item;

      IF invalid_alias_item THEN
        alias_set_valid := FALSE;
      ELSE
        SELECT EXISTS (
          SELECT 1
          FROM pg_catalog.jsonb_array_elements(p_aliases) AS item(value)
          WHERE (SELECT pg_catalog.count(*)
                 FROM pg_catalog.jsonb_object_keys(item.value)) <> 3
        )
        INTO invalid_alias_item;

        IF invalid_alias_item THEN
          alias_set_valid := FALSE;
        ELSE
          SELECT EXISTS (
            SELECT 1
            FROM pg_catalog.jsonb_array_elements(p_aliases) AS item(value)
            WHERE pg_catalog.jsonb_typeof(item.value -> 'alias_key') IS DISTINCT FROM 'string'
              OR pg_catalog.jsonb_typeof(item.value -> 'display_text') IS DISTINCT FROM 'string'
              OR pg_catalog.jsonb_typeof(item.value -> 'is_preferred') IS DISTINCT FROM 'boolean'
              OR pg_catalog.octet_length(item.value ->> 'alias_key') NOT BETWEEN 1 AND 2048
              OR pg_catalog.octet_length(item.value ->> 'display_text') NOT BETWEEN 1 AND 512
          )
          INTO invalid_alias_item;

          IF invalid_alias_item THEN
            alias_set_valid := FALSE;
          ELSE
            SELECT
              pg_catalog.count(*),
              pg_catalog.count(*) FILTER (
                WHERE (item.value ->> 'is_preferred')::boolean
              ),
              pg_catalog.count(DISTINCT (item.value ->> 'alias_key') COLLATE "C")
            INTO alias_count, preferred_alias_count, distinct_alias_count
            FROM pg_catalog.jsonb_array_elements(p_aliases) AS item(value);

            alias_set_valid := preferred_alias_count = 1
              AND distinct_alias_count = alias_count;
          END IF;
        END IF;
      END IF;
    END IF;
  END IF;

  IF NOT alias_set_valid
    OR preferred_alias_count <> 1
    OR distinct_alias_count <> alias_count
  THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'conflict',
      'field', 'alias_set',
      'reason', 'alias_set_mismatch',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'concept',
        'id', p_concept_id
      ),
      'current_revision', current_revision
    );
  END IF;

  SELECT concepts.id, concepts.revision
  INTO conflict_concept_id, conflict_revision
  FROM knowledge_graph.graph_concepts AS concepts
  WHERE concepts.id <> p_concept_id
    AND EXISTS (
      SELECT 1
      FROM knowledge_graph.graph_aliases AS aliases
      WHERE aliases.concept_id = concepts.id
        AND EXISTS (
          SELECT 1
          FROM pg_catalog.jsonb_array_elements(p_aliases) AS requested(value)
          WHERE aliases.alias_key = (requested.value ->> 'alias_key') COLLATE "C"
        )
    )
  ORDER BY concepts.id ASC
  LIMIT 1;

  IF FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'conflict',
      'field', 'alias_set',
      'reason', 'alias_owned',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'concept',
        'id', conflict_concept_id
      ),
      'current_revision', conflict_revision
    );
  END IF;

  DELETE FROM knowledge_graph.graph_aliases AS aliases
  WHERE aliases.concept_id = p_concept_id;

  INSERT INTO knowledge_graph.graph_aliases (
    concept_id,
    alias_key,
    display_text,
    is_preferred,
    normalization_version
  )
  SELECT
    p_concept_id,
    requested.value ->> 'alias_key',
    requested.value ->> 'display_text',
    (requested.value ->> 'is_preferred')::boolean,
    'icu4x-2.3.0-nfkc-fold-v1'
  FROM pg_catalog.jsonb_array_elements(p_aliases) AS requested(value)
  ORDER BY (requested.value ->> 'alias_key') COLLATE "C" ASC;

  UPDATE knowledge_graph.graph_concepts AS concepts
  SET revision = concepts.revision + 1
  WHERE concepts.id = p_concept_id
  RETURNING concepts.revision INTO new_revision;

  RETURN pg_catalog.jsonb_build_object(
    'outcome', 'updated',
    'record', pg_catalog.jsonb_build_object(
      'id', p_concept_id,
      'revision', new_revision,
      'aliases', (
        SELECT COALESCE(
          pg_catalog.jsonb_agg(
            pg_catalog.jsonb_build_object(
              'display_text', aliases.display_text,
              'preferred', aliases.is_preferred
            )
            ORDER BY aliases.is_preferred DESC, aliases.alias_key COLLATE "C" ASC
          ),
          '[]'::jsonb
        )
        FROM knowledge_graph.graph_aliases AS aliases
        WHERE aliases.concept_id = p_concept_id
      )
    )
  );
END;
$function$;

CREATE FUNCTION knowledge_graph.graph_delete_concept(
  p_concept_id uuid,
  p_expected_revision bigint
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  lock_keys jsonb := '[]'::jsonb;
  snapshot_exists boolean := FALSE;
  snapshot_revision bigint;
  current_revision bigint;
  snapshot_alias_snapshot jsonb := '[]'::jsonb;
  current_alias_snapshot jsonb := '[]'::jsonb;
  snapshot_alias record;
BEGIN
  SELECT
    concepts.revision,
    COALESCE(
      (
        SELECT pg_catalog.jsonb_agg(
          pg_catalog.jsonb_build_object(
            'alias_key', aliases.alias_key,
            'display_text', aliases.display_text,
            'is_preferred', aliases.is_preferred,
            'normalization_version', aliases.normalization_version
          )
          ORDER BY aliases.alias_key COLLATE "C" ASC
        )
        FROM knowledge_graph.graph_aliases AS aliases
        WHERE aliases.concept_id = concepts.id
      ),
      '[]'::jsonb
    )
  INTO snapshot_revision, snapshot_alias_snapshot
  FROM knowledge_graph.graph_concepts AS concepts
  WHERE concepts.id = p_concept_id;
  snapshot_exists := FOUND;

  lock_keys := pg_catalog.jsonb_build_array(
    knowledge_graph.graph_lock_key_concept_id(p_concept_id)
  );

  FOR snapshot_alias IN
    SELECT item.value ->> 'alias_key' AS alias_key
    FROM pg_catalog.jsonb_array_elements(snapshot_alias_snapshot) AS item(value)
  LOOP
    lock_keys := lock_keys || pg_catalog.jsonb_build_array(
      knowledge_graph.graph_lock_key_normalized_alias(snapshot_alias.alias_key)
    );
  END LOOP;

  PERFORM knowledge_graph.graph_acquire_mutation_locks(
    lock_keys,
    ARRAY[p_concept_id],
    ARRAY[]::uuid[],
    ARRAY[]::bigint[]
  );

  SELECT
    concepts.revision,
    COALESCE(
      (
        SELECT pg_catalog.jsonb_agg(
          pg_catalog.jsonb_build_object(
            'alias_key', aliases.alias_key,
            'display_text', aliases.display_text,
            'is_preferred', aliases.is_preferred,
            'normalization_version', aliases.normalization_version
          )
          ORDER BY aliases.alias_key COLLATE "C" ASC
        )
        FROM knowledge_graph.graph_aliases AS aliases
        WHERE aliases.concept_id = concepts.id
      ),
      '[]'::jsonb
    )
  INTO current_revision, current_alias_snapshot
  FROM knowledge_graph.graph_concepts AS concepts
  WHERE concepts.id = p_concept_id;

  IF NOT FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'concept',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'concept',
        'id', p_concept_id
      )
    );
  END IF;

  IF NOT snapshot_exists
    OR current_revision IS DISTINCT FROM snapshot_revision
    OR current_alias_snapshot IS DISTINCT FROM snapshot_alias_snapshot
  THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'conflict',
      'field', 'snapshot',
      'reason', 'snapshot_drift',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'concept',
        'id', p_concept_id
      ),
      'current_revision', current_revision
    );
  END IF;

  IF p_expected_revision IS DISTINCT FROM current_revision THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'stale',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'concept',
        'id', p_concept_id
      ),
      'current_revision', current_revision
    );
  END IF;

  IF EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_concept_mentions AS mentions
    WHERE mentions.concept_id = p_concept_id
  ) OR EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_assertions AS assertions
    WHERE assertions.subject_concept_id = p_concept_id
      OR assertions.object_concept_id = p_concept_id
  ) THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'referenced',
      'reference', pg_catalog.jsonb_build_object(
        'kind', 'concept',
        'id', p_concept_id
      )
    );
  END IF;

  DELETE FROM knowledge_graph.graph_aliases AS aliases
  WHERE aliases.concept_id = p_concept_id;

  DELETE FROM knowledge_graph.graph_concepts AS concepts
  WHERE concepts.id = p_concept_id;

  RETURN pg_catalog.jsonb_build_object(
    'outcome', 'deleted'
  );
END;
$function$;

CREATE FUNCTION knowledge_graph.graph_register_source(
  p_source_kind text,
  p_external_id text,
  p_external_version bigint
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  lock_keys jsonb;
  snapshot_source_ref_id bigint;
  current_source_ref_id bigint;
  current_source_kind text;
  current_external_id text;
  current_external_version bigint;
BEGIN
  lock_keys := pg_catalog.jsonb_build_array(
    knowledge_graph.graph_lock_key_source_identity(
      p_source_kind,
      p_external_id,
      p_external_version
    )
  );

  SELECT source_references.source_ref_id
  INTO snapshot_source_ref_id
  FROM knowledge_graph.graph_source_references AS source_references
  WHERE source_references.source_kind = p_source_kind COLLATE "C"
    AND source_references.external_id = p_external_id COLLATE "C"
    AND source_references.external_version IS NOT DISTINCT FROM p_external_version;

  PERFORM knowledge_graph.graph_acquire_mutation_locks(
    lock_keys,
    ARRAY[]::uuid[],
    ARRAY[]::uuid[],
    ARRAY[snapshot_source_ref_id]
  );

  SELECT
    source_references.source_ref_id,
    source_references.source_kind,
    source_references.external_id,
    source_references.external_version
  INTO
    current_source_ref_id,
    current_source_kind,
    current_external_id,
    current_external_version
  FROM knowledge_graph.graph_source_references AS source_references
  WHERE source_references.source_kind = p_source_kind COLLATE "C"
    AND source_references.external_id = p_external_id COLLATE "C"
    AND source_references.external_version IS NOT DISTINCT FROM p_external_version;

  IF FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'existing',
      'record', pg_catalog.jsonb_build_object(
        'source_ref_id', current_source_ref_id,
        'source_kind', current_source_kind,
        'external_id', current_external_id,
        'external_version', current_external_version
      )
    );
  END IF;

  INSERT INTO knowledge_graph.graph_source_references AS source_references (
    source_kind,
    external_id,
    external_version
  )
  VALUES (
    p_source_kind,
    p_external_id,
    p_external_version
  )
  RETURNING
    source_references.source_ref_id,
    source_references.source_kind,
    source_references.external_id,
    source_references.external_version
  INTO
    current_source_ref_id,
    current_source_kind,
    current_external_id,
    current_external_version;

  RETURN pg_catalog.jsonb_build_object(
    'outcome', 'created',
    'record', pg_catalog.jsonb_build_object(
      'source_ref_id', current_source_ref_id,
      'source_kind', current_source_kind,
      'external_id', current_external_id,
      'external_version', current_external_version
    )
  );
END;
$function$;

CREATE FUNCTION knowledge_graph.graph_delete_source(
  p_source_kind text,
  p_external_id text,
  p_external_version bigint
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  lock_keys jsonb;
  snapshot_exists boolean := FALSE;
  snapshot_source_ref_id bigint;
  current_source_ref_id bigint;
BEGIN
  lock_keys := pg_catalog.jsonb_build_array(
    knowledge_graph.graph_lock_key_source_identity(
      p_source_kind,
      p_external_id,
      p_external_version
    )
  );

  SELECT source_references.source_ref_id
  INTO snapshot_source_ref_id
  FROM knowledge_graph.graph_source_references AS source_references
  WHERE source_references.source_kind = p_source_kind COLLATE "C"
    AND source_references.external_id = p_external_id COLLATE "C"
    AND source_references.external_version IS NOT DISTINCT FROM p_external_version;
  snapshot_exists := FOUND;

  PERFORM knowledge_graph.graph_acquire_mutation_locks(
    lock_keys,
    ARRAY[]::uuid[],
    ARRAY[]::uuid[],
    ARRAY[snapshot_source_ref_id]
  );

  SELECT source_references.source_ref_id
  INTO current_source_ref_id
  FROM knowledge_graph.graph_source_references AS source_references
  WHERE source_references.source_kind = p_source_kind COLLATE "C"
    AND source_references.external_id = p_external_id COLLATE "C"
    AND source_references.external_version IS NOT DISTINCT FROM p_external_version;

  IF NOT FOUND THEN
    IF snapshot_exists AND EXISTS (
      SELECT 1
      FROM knowledge_graph.graph_source_references AS source_references
      WHERE source_references.source_ref_id = snapshot_source_ref_id
    ) THEN
      RETURN pg_catalog.jsonb_build_object(
        'outcome', 'conflict',
        'field', 'snapshot',
        'reason', 'snapshot_drift'
      );
    END IF;

    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'source_reference'
    );
  END IF;

  IF NOT snapshot_exists
    OR current_source_ref_id IS DISTINCT FROM snapshot_source_ref_id
  THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'conflict',
      'field', 'snapshot',
      'reason', 'snapshot_drift'
    );
  END IF;

  IF EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_concept_mentions AS mentions
    WHERE mentions.source_ref_id = current_source_ref_id
  ) OR EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_assertion_evidence AS evidence
    WHERE evidence.source_ref_id = current_source_ref_id
  ) THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'referenced',
      'reference_kind', 'source_reference'
    );
  END IF;

  DELETE FROM knowledge_graph.graph_source_references AS source_references
  WHERE source_references.source_ref_id = current_source_ref_id;

  RETURN pg_catalog.jsonb_build_object(
    'outcome', 'deleted'
  );
END;
$function$;

CREATE FUNCTION knowledge_graph.graph_create_mention(
  p_concept_id uuid,
  p_source_kind text,
  p_external_id text,
  p_external_version bigint
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  lock_keys jsonb;
  concept_exists boolean := FALSE;
  snapshot_source_exists boolean := FALSE;
  snapshot_source_ref_id bigint;
  current_source_ref_id bigint;
  current_source_kind text;
  current_external_id text;
  current_external_version bigint;
BEGIN
  lock_keys := pg_catalog.jsonb_build_array(
    knowledge_graph.graph_lock_key_concept_id(p_concept_id),
    knowledge_graph.graph_lock_key_source_identity(
      p_source_kind,
      p_external_id,
      p_external_version
    ),
    knowledge_graph.graph_lock_key_concept_mention(
      p_concept_id,
      p_source_kind,
      p_external_id,
      p_external_version
    )
  );

  SELECT source_references.source_ref_id
  INTO snapshot_source_ref_id
  FROM knowledge_graph.graph_source_references AS source_references
  WHERE source_references.source_kind = p_source_kind COLLATE "C"
    AND source_references.external_id = p_external_id COLLATE "C"
    AND source_references.external_version IS NOT DISTINCT FROM p_external_version;
  snapshot_source_exists := FOUND;

  PERFORM knowledge_graph.graph_acquire_mutation_locks(
    lock_keys,
    ARRAY[p_concept_id],
    ARRAY[]::uuid[],
    ARRAY[snapshot_source_ref_id]
  );

  SELECT EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_concepts AS concepts
    WHERE concepts.id = p_concept_id
  )
  INTO concept_exists;

  IF NOT concept_exists THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'concept',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'concept',
        'id', p_concept_id
      )
    );
  END IF;

  SELECT
    source_references.source_ref_id,
    source_references.source_kind,
    source_references.external_id,
    source_references.external_version
  INTO
    current_source_ref_id,
    current_source_kind,
    current_external_id,
    current_external_version
  FROM knowledge_graph.graph_source_references AS source_references
  WHERE source_references.source_kind = p_source_kind COLLATE "C"
    AND source_references.external_id = p_external_id COLLATE "C"
    AND source_references.external_version IS NOT DISTINCT FROM p_external_version;

  IF NOT FOUND THEN
    IF snapshot_source_exists AND EXISTS (
      SELECT 1
      FROM knowledge_graph.graph_source_references AS source_references
      WHERE source_references.source_ref_id = snapshot_source_ref_id
    ) THEN
      RETURN pg_catalog.jsonb_build_object(
        'outcome', 'conflict',
        'field', 'snapshot',
        'reason', 'snapshot_drift'
      );
    END IF;

    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'source_reference'
    );
  END IF;

  IF NOT snapshot_source_exists
    OR current_source_ref_id IS DISTINCT FROM snapshot_source_ref_id
  THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'conflict',
      'field', 'snapshot',
      'reason', 'snapshot_drift'
    );
  END IF;

  IF EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_concept_mentions AS mentions
    WHERE mentions.concept_id = p_concept_id
      AND mentions.source_ref_id = current_source_ref_id
  ) THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'existing',
      'record', pg_catalog.jsonb_build_object(
        'concept_id', p_concept_id,
        'source_ref_id', current_source_ref_id,
        'source', pg_catalog.jsonb_build_object(
          'source_kind', current_source_kind,
          'external_id', current_external_id,
          'external_version', current_external_version
        )
      )
    );
  END IF;

  INSERT INTO knowledge_graph.graph_concept_mentions (
    concept_id,
    source_ref_id
  )
  VALUES (
    p_concept_id,
    current_source_ref_id
  );

  RETURN pg_catalog.jsonb_build_object(
    'outcome', 'created',
    'record', pg_catalog.jsonb_build_object(
      'concept_id', p_concept_id,
      'source_ref_id', current_source_ref_id,
      'source', pg_catalog.jsonb_build_object(
        'source_kind', current_source_kind,
        'external_id', current_external_id,
        'external_version', current_external_version
      )
    )
  );
END;
$function$;

CREATE FUNCTION knowledge_graph.graph_delete_mention(
  p_concept_id uuid,
  p_source_kind text,
  p_external_id text,
  p_external_version bigint
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  lock_keys jsonb;
  concept_exists boolean := FALSE;
  snapshot_source_exists boolean := FALSE;
  snapshot_source_ref_id bigint;
  current_source_ref_id bigint;
BEGIN
  lock_keys := pg_catalog.jsonb_build_array(
    knowledge_graph.graph_lock_key_concept_id(p_concept_id),
    knowledge_graph.graph_lock_key_source_identity(
      p_source_kind,
      p_external_id,
      p_external_version
    ),
    knowledge_graph.graph_lock_key_concept_mention(
      p_concept_id,
      p_source_kind,
      p_external_id,
      p_external_version
    )
  );

  SELECT source_references.source_ref_id
  INTO snapshot_source_ref_id
  FROM knowledge_graph.graph_source_references AS source_references
  WHERE source_references.source_kind = p_source_kind COLLATE "C"
    AND source_references.external_id = p_external_id COLLATE "C"
    AND source_references.external_version IS NOT DISTINCT FROM p_external_version;
  snapshot_source_exists := FOUND;

  PERFORM knowledge_graph.graph_acquire_mutation_locks(
    lock_keys,
    ARRAY[p_concept_id],
    ARRAY[]::uuid[],
    ARRAY[snapshot_source_ref_id]
  );

  SELECT EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_concepts AS concepts
    WHERE concepts.id = p_concept_id
  )
  INTO concept_exists;

  IF NOT concept_exists THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'concept',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'concept',
        'id', p_concept_id
      )
    );
  END IF;

  SELECT source_references.source_ref_id
  INTO current_source_ref_id
  FROM knowledge_graph.graph_source_references AS source_references
  WHERE source_references.source_kind = p_source_kind COLLATE "C"
    AND source_references.external_id = p_external_id COLLATE "C"
    AND source_references.external_version IS NOT DISTINCT FROM p_external_version;

  IF NOT FOUND THEN
    IF snapshot_source_exists AND EXISTS (
      SELECT 1
      FROM knowledge_graph.graph_source_references AS source_references
      WHERE source_references.source_ref_id = snapshot_source_ref_id
    ) THEN
      RETURN pg_catalog.jsonb_build_object(
        'outcome', 'conflict',
        'field', 'snapshot',
        'reason', 'snapshot_drift'
      );
    END IF;

    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'source_reference'
    );
  END IF;

  IF NOT snapshot_source_exists
    OR current_source_ref_id IS DISTINCT FROM snapshot_source_ref_id
  THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'conflict',
      'field', 'snapshot',
      'reason', 'snapshot_drift'
    );
  END IF;

  DELETE FROM knowledge_graph.graph_concept_mentions AS mentions
  WHERE mentions.concept_id = p_concept_id
    AND mentions.source_ref_id = current_source_ref_id;

  IF NOT FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'concept_mention'
    );
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'outcome', 'deleted'
  );
END;
$function$;

CREATE FUNCTION knowledge_graph.graph_create_assertion(
  p_candidate_id uuid,
  p_subject_concept_id uuid,
  p_relation_code text,
  p_object_concept_id uuid,
  p_supporting_sources jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  lock_keys jsonb := '[]'::jsonb;
  relation_is_symmetric boolean;
  canonical_subject_concept_id uuid;
  canonical_object_concept_id uuid;
  requested_source record;
  snapshot_source_ref_ids bigint[] := ARRAY[]::bigint[];
  requested_source_ref_ids bigint[] := ARRAY[]::bigint[];
  snapshot_assertion_id uuid;
  existing_assertion_id uuid;
  existing_assertion_revision bigint;
  candidate_assertion_revision bigint;
  requested_source_count bigint := 0;
  current_evidence_count bigint := 0;
  missing_evidence_count bigint := 0;
  invalid_source_shape boolean := FALSE;
BEGIN
  IF p_candidate_id IS NULL
    OR pg_catalog.substring(p_candidate_id::text, 15, 1) IS DISTINCT FROM '7'
    OR pg_catalog.substring(p_candidate_id::text, 20, 1) NOT IN ('8', '9', 'a', 'b')
  THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'create_assertion',
      'field', 'assertion_id',
      'reason', 'not_uuid_v7'
    );
  END IF;

  IF p_subject_concept_id IS NULL THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'create_assertion',
      'field', 'subject_concept_id',
      'reason', 'invalid_uuid'
    );
  END IF;

  IF p_object_concept_id IS NULL THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'create_assertion',
      'field', 'object_concept_id',
      'reason', 'invalid_uuid'
    );
  END IF;

  IF p_subject_concept_id = p_object_concept_id THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'create_assertion',
      'field', 'subject_concept_id',
      'reason', 'self_relation'
    );
  END IF;

  SELECT relation_types.is_symmetric
  INTO relation_is_symmetric
  FROM knowledge_graph.graph_relation_types AS relation_types
  WHERE relation_types.code = p_relation_code COLLATE "C";

  IF NOT FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'create_assertion',
      'field', 'relation_type',
      'reason', 'unsupported'
    );
  END IF;

  canonical_subject_concept_id := p_subject_concept_id;
  canonical_object_concept_id := p_object_concept_id;
  IF relation_is_symmetric AND canonical_subject_concept_id > canonical_object_concept_id THEN
    canonical_subject_concept_id := p_object_concept_id;
    canonical_object_concept_id := p_subject_concept_id;
  END IF;

  IF p_supporting_sources IS NULL
    OR pg_catalog.jsonb_typeof(p_supporting_sources) IS DISTINCT FROM 'array'
  THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'create_assertion',
      'field', 'supporting_sources',
      'reason', 'invalid_shape'
    );
  END IF;

  IF pg_catalog.jsonb_array_length(p_supporting_sources) = 0 THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'create_assertion',
      'field', 'supporting_sources',
      'reason', 'empty'
    );
  END IF;

  IF pg_catalog.jsonb_array_length(p_supporting_sources) > 32 THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'evidence_limit'
    );
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements(p_supporting_sources) AS requested(value)
    WHERE pg_catalog.jsonb_typeof(requested.value) IS DISTINCT FROM 'object'
  )
  INTO invalid_source_shape;

  IF invalid_source_shape THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'create_assertion',
      'field', 'supporting_sources',
      'reason', 'invalid_shape'
    );
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements(p_supporting_sources) AS requested(value)
    WHERE pg_catalog.jsonb_typeof(requested.value -> 'kind') IS DISTINCT FROM 'string'
  )
  INTO invalid_source_shape;

  IF invalid_source_shape THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'create_assertion',
      'field', 'supporting_sources',
      'reason', 'invalid_shape'
    );
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements(p_supporting_sources) AS requested(value)
    WHERE (requested.value ->> 'kind') COLLATE "C"
      NOT IN ('memory_version', 'session_record')
  ) THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'create_assertion',
      'field', 'source_kind',
      'reason', 'unsupported'
    );
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements(p_supporting_sources) AS requested(value)
    WHERE (requested.value ->> 'kind') COLLATE "C" = 'memory_version'
      AND (
        (SELECT pg_catalog.count(*)
         FROM pg_catalog.jsonb_object_keys(requested.value)) <> 3
        OR pg_catalog.jsonb_typeof(requested.value -> 'memory_id') IS DISTINCT FROM 'string'
        OR pg_catalog.jsonb_typeof(requested.value -> 'version') IS DISTINCT FROM 'number'
      )
      OR (requested.value ->> 'kind') COLLATE "C" = 'session_record'
      AND (
        (SELECT pg_catalog.count(*)
         FROM pg_catalog.jsonb_object_keys(requested.value)) <> 2
        OR pg_catalog.jsonb_typeof(requested.value -> 'session_record_id')
          IS DISTINCT FROM 'string'
      )
  )
  INTO invalid_source_shape;

  IF invalid_source_shape THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'create_assertion',
      'field', 'supporting_sources',
      'reason', 'invalid_shape'
    );
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements(p_supporting_sources) AS requested(value)
    WHERE (requested.value ->> 'kind') COLLATE "C" = 'memory_version'
      AND (requested.value ->> 'memory_id') = ''
  ) THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'create_assertion',
      'field', 'memory_id',
      'reason', 'empty'
    );
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements(p_supporting_sources) AS requested(value)
    WHERE (requested.value ->> 'kind') COLLATE "C" = 'session_record'
      AND (requested.value ->> 'session_record_id') = ''
  ) THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'create_assertion',
      'field', 'session_record_id',
      'reason', 'empty'
    );
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements(p_supporting_sources) AS requested(value)
    WHERE ((requested.value ->> 'kind') COLLATE "C" = 'memory_version'
        AND pg_catalog.octet_length(requested.value ->> 'memory_id') NOT BETWEEN 1 AND 2048)
      OR ((requested.value ->> 'kind') COLLATE "C" = 'session_record'
        AND pg_catalog.octet_length(requested.value ->> 'session_record_id') NOT BETWEEN 1 AND 2048)
  ) THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'create_assertion',
      'field', 'supporting_sources',
      'reason', 'too_long'
    );
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements(p_supporting_sources) AS requested(value)
    WHERE (requested.value ->> 'kind') COLLATE "C" = 'memory_version'
      AND (requested.value ->> 'version') !~ '^-?[0-9]+$'
  ) THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'create_assertion',
      'field', 'memory_version',
      'reason', 'invalid_shape'
    );
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements(p_supporting_sources) AS requested(value)
    WHERE (requested.value ->> 'kind') COLLATE "C" = 'memory_version'
      AND ((requested.value ->> 'version') LIKE '-%'
        OR pg_catalog.ltrim(requested.value ->> 'version', '0') = '')
  ) THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'create_assertion',
      'field', 'memory_version',
      'reason', 'not_positive'
    );
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements(p_supporting_sources) AS requested(value)
    WHERE (requested.value ->> 'kind') COLLATE "C" = 'memory_version'
      AND (
        pg_catalog.octet_length(requested.value ->> 'version') > 19
        OR (pg_catalog.octet_length(requested.value ->> 'version') = 19
          AND (requested.value ->> 'version') COLLATE "C" > '9223372036854775807')
      )
  ) THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'create_assertion',
      'field', 'memory_version',
      'reason', 'too_long'
    );
  END IF;

  lock_keys := pg_catalog.jsonb_build_array(
    knowledge_graph.graph_lock_key_assertion_id(p_candidate_id),
    knowledge_graph.graph_lock_key_concept_id(canonical_subject_concept_id),
    knowledge_graph.graph_lock_key_concept_id(canonical_object_concept_id),
    knowledge_graph.graph_lock_key_semantic_assertion(
      p_relation_code,
      canonical_subject_concept_id,
      canonical_object_concept_id
    )
  );

  FOR requested_source IN
    SELECT requested_sources.source_kind,
      requested_sources.external_id,
      requested_sources.external_version
    FROM (
      SELECT DISTINCT
        (requested.value ->> 'kind') COLLATE "C" AS source_kind,
        (CASE (requested.value ->> 'kind') COLLATE "C"
          WHEN 'memory_version' THEN requested.value ->> 'memory_id'
          ELSE requested.value ->> 'session_record_id'
        END) COLLATE "C" AS external_id,
        CASE (requested.value ->> 'kind') COLLATE "C"
          WHEN 'memory_version' THEN (requested.value ->> 'version')::bigint
          ELSE NULL::bigint
        END AS external_version
      FROM pg_catalog.jsonb_array_elements(p_supporting_sources) AS requested(value)
    ) AS requested_sources
    ORDER BY requested_sources.source_kind COLLATE "C" ASC,
      requested_sources.external_id COLLATE "C" ASC,
      requested_sources.external_version ASC NULLS FIRST
  LOOP
    lock_keys := lock_keys || pg_catalog.jsonb_build_array(
      knowledge_graph.graph_lock_key_source_identity(
        requested_source.source_kind,
        requested_source.external_id,
        requested_source.external_version
      )
    );
  END LOOP;

  SELECT COALESCE(
    pg_catalog.array_agg(DISTINCT source_references.source_ref_id
      ORDER BY source_references.source_ref_id),
    ARRAY[]::bigint[]
  )
  INTO snapshot_source_ref_ids
  FROM (
    SELECT DISTINCT
      requested.value ->> 'kind' AS source_kind,
      CASE (requested.value ->> 'kind') COLLATE "C"
        WHEN 'memory_version' THEN requested.value ->> 'memory_id'
        ELSE requested.value ->> 'session_record_id'
      END AS external_id,
      CASE (requested.value ->> 'kind') COLLATE "C"
        WHEN 'memory_version' THEN (requested.value ->> 'version')::bigint
        ELSE NULL::bigint
      END AS external_version
    FROM pg_catalog.jsonb_array_elements(p_supporting_sources) AS requested(value)
  ) AS requested_sources
  JOIN knowledge_graph.graph_source_references AS source_references
    ON source_references.source_kind = requested_sources.source_kind COLLATE "C"
    AND source_references.external_id = requested_sources.external_id COLLATE "C"
    AND source_references.external_version IS NOT DISTINCT FROM requested_sources.external_version;

  SELECT assertions.id
  INTO snapshot_assertion_id
  FROM knowledge_graph.graph_assertions AS assertions
  WHERE assertions.subject_concept_id = canonical_subject_concept_id
    AND assertions.relation_code = p_relation_code COLLATE "C"
    AND assertions.object_concept_id = canonical_object_concept_id
  ORDER BY assertions.id ASC
  LIMIT 1;

  PERFORM knowledge_graph.graph_acquire_mutation_locks(
    lock_keys,
    ARRAY[canonical_subject_concept_id, canonical_object_concept_id],
    ARRAY[p_candidate_id, snapshot_assertion_id],
    snapshot_source_ref_ids
  );

  IF NOT EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_concepts AS concepts
    WHERE concepts.id = canonical_subject_concept_id
  ) THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'concept',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'concept',
        'id', canonical_subject_concept_id
      )
    );
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_concepts AS concepts
    WHERE concepts.id = canonical_object_concept_id
  ) THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'concept',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'concept',
        'id', canonical_object_concept_id
      )
    );
  END IF;

  SELECT
    pg_catalog.count(DISTINCT (
      requested_sources.source_kind,
      requested_sources.external_id,
      requested_sources.external_version
    )),
    COALESCE(
      pg_catalog.array_agg(DISTINCT source_references.source_ref_id
        ORDER BY source_references.source_ref_id)
        FILTER (WHERE source_references.source_ref_id IS NOT NULL),
      ARRAY[]::bigint[]
    )
  INTO requested_source_count, requested_source_ref_ids
  FROM (
    SELECT DISTINCT
      requested.value ->> 'kind' AS source_kind,
      CASE (requested.value ->> 'kind') COLLATE "C"
        WHEN 'memory_version' THEN requested.value ->> 'memory_id'
        ELSE requested.value ->> 'session_record_id'
      END AS external_id,
      CASE (requested.value ->> 'kind') COLLATE "C"
        WHEN 'memory_version' THEN (requested.value ->> 'version')::bigint
        ELSE NULL::bigint
      END AS external_version
    FROM pg_catalog.jsonb_array_elements(p_supporting_sources) AS requested(value)
  ) AS requested_sources
  LEFT JOIN knowledge_graph.graph_source_references AS source_references
    ON source_references.source_kind = requested_sources.source_kind COLLATE "C"
    AND source_references.external_id = requested_sources.external_id COLLATE "C"
    AND source_references.external_version IS NOT DISTINCT FROM requested_sources.external_version;

  IF pg_catalog.cardinality(requested_source_ref_ids) <> requested_source_count THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'source_reference'
    );
  END IF;

  SELECT assertions.id, assertions.revision
  INTO existing_assertion_id, existing_assertion_revision
  FROM knowledge_graph.graph_assertions AS assertions
  WHERE assertions.subject_concept_id = canonical_subject_concept_id
    AND assertions.relation_code = p_relation_code COLLATE "C"
    AND assertions.object_concept_id = canonical_object_concept_id;

  SELECT pg_catalog.count(*)
  INTO current_evidence_count
  FROM knowledge_graph.graph_assertion_evidence AS evidence
  WHERE evidence.assertion_id = existing_assertion_id;

  SELECT pg_catalog.count(*)
  INTO missing_evidence_count
  FROM pg_catalog.unnest(requested_source_ref_ids) AS requested(source_ref_id)
  WHERE NOT EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_assertion_evidence AS evidence
    WHERE evidence.assertion_id = existing_assertion_id
      AND evidence.source_ref_id = requested.source_ref_id
  );

  IF current_evidence_count + missing_evidence_count > 32 THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'evidence_limit'
    );
  END IF;

  IF existing_assertion_id IS NOT NULL THEN
    INSERT INTO knowledge_graph.graph_assertion_evidence (
      assertion_id,
      source_ref_id
    )
    SELECT existing_assertion_id, requested.source_ref_id
    FROM pg_catalog.unnest(requested_source_ref_ids) AS requested(source_ref_id)
    WHERE NOT EXISTS (
      SELECT 1
      FROM knowledge_graph.graph_assertion_evidence AS evidence
      WHERE evidence.assertion_id = existing_assertion_id
        AND evidence.source_ref_id = requested.source_ref_id
    )
    ORDER BY requested.source_ref_id ASC
    ON CONFLICT (assertion_id, source_ref_id) DO NOTHING;

    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'existing',
      'record', pg_catalog.jsonb_build_object(
        'id', existing_assertion_id,
        'revision', existing_assertion_revision,
        'subject_concept_id', canonical_subject_concept_id,
        'relation_type', p_relation_code,
        'object_concept_id', canonical_object_concept_id,
        'evidence', (
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
              ORDER BY source_references.source_kind COLLATE "C" ASC,
                source_references.external_id COLLATE "C" ASC,
                source_references.external_version ASC NULLS FIRST
            ),
            '[]'::jsonb
          )
          FROM knowledge_graph.graph_assertion_evidence AS evidence
          JOIN knowledge_graph.graph_source_references AS source_references
            ON source_references.source_ref_id = evidence.source_ref_id
          WHERE evidence.assertion_id = existing_assertion_id
        )
      )
    );
  END IF;

  SELECT assertions.revision
  INTO candidate_assertion_revision
  FROM knowledge_graph.graph_assertions AS assertions
  WHERE assertions.id = p_candidate_id;

  IF FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'conflict',
      'field', 'semantic_assertion',
      'reason', 'duplicate_semantic_assertion',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'assertion',
        'id', p_candidate_id
      ),
      'current_revision', candidate_assertion_revision
    );
  END IF;

  INSERT INTO knowledge_graph.graph_assertions (
    id,
    subject_concept_id,
    relation_code,
    object_concept_id,
    revision
  )
  VALUES (
    p_candidate_id,
    canonical_subject_concept_id,
    p_relation_code,
    canonical_object_concept_id,
    1
  );

  INSERT INTO knowledge_graph.graph_assertion_evidence (
    assertion_id,
    source_ref_id
  )
  SELECT p_candidate_id, requested.source_ref_id
  FROM pg_catalog.unnest(requested_source_ref_ids) AS requested(source_ref_id)
  ORDER BY requested.source_ref_id ASC
  ON CONFLICT (assertion_id, source_ref_id) DO NOTHING;

  RETURN pg_catalog.jsonb_build_object(
    'outcome', 'created',
    'record', pg_catalog.jsonb_build_object(
      'id', p_candidate_id,
      'revision', 1,
      'subject_concept_id', canonical_subject_concept_id,
      'relation_type', p_relation_code,
      'object_concept_id', canonical_object_concept_id,
      'evidence', (
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
            ORDER BY source_references.source_kind COLLATE "C" ASC,
              source_references.external_id COLLATE "C" ASC,
              source_references.external_version ASC NULLS FIRST
          ),
          '[]'::jsonb
        )
        FROM knowledge_graph.graph_assertion_evidence AS evidence
        JOIN knowledge_graph.graph_source_references AS source_references
          ON source_references.source_ref_id = evidence.source_ref_id
        WHERE evidence.assertion_id = p_candidate_id
      )
    )
  );
END;
$function$;

CREATE FUNCTION knowledge_graph.graph_update_assertion(
  p_assertion_id uuid,
  p_expected_revision bigint,
  p_subject_concept_id uuid,
  p_relation_code text,
  p_object_concept_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  lock_keys jsonb := '[]'::jsonb;
  snapshot_exists boolean := FALSE;
  relation_is_symmetric boolean;
  canonical_subject_concept_id uuid;
  canonical_object_concept_id uuid;
  snapshot_revision bigint;
  snapshot_subject_concept_id uuid;
  snapshot_relation_code text;
  snapshot_object_concept_id uuid;
  current_revision bigint;
  current_subject_concept_id uuid;
  current_relation_code text;
  current_object_concept_id uuid;
  new_revision bigint;
  conflict_assertion_id uuid;
  conflict_revision bigint;
BEGIN
  IF p_assertion_id IS NULL
    OR pg_catalog.substring(p_assertion_id::text, 15, 1) IS DISTINCT FROM '7'
    OR pg_catalog.substring(p_assertion_id::text, 20, 1) NOT IN ('8', '9', 'a', 'b')
  THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'update_assertion',
      'field', 'assertion_id',
      'reason', 'not_uuid_v7'
    );
  END IF;

  IF p_expected_revision IS NULL OR p_expected_revision <= 0 THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'update_assertion',
      'field', 'revision',
      'reason', 'not_positive'
    );
  END IF;

  IF p_subject_concept_id IS NULL THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'update_assertion',
      'field', 'subject_concept_id',
      'reason', 'invalid_uuid'
    );
  END IF;

  IF p_object_concept_id IS NULL THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'update_assertion',
      'field', 'object_concept_id',
      'reason', 'invalid_uuid'
    );
  END IF;

  IF p_subject_concept_id = p_object_concept_id THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'update_assertion',
      'field', 'subject_concept_id',
      'reason', 'self_relation'
    );
  END IF;

  SELECT relation_types.is_symmetric
  INTO relation_is_symmetric
  FROM knowledge_graph.graph_relation_types AS relation_types
  WHERE relation_types.code = p_relation_code COLLATE "C";

  IF NOT FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'update_assertion',
      'field', 'relation_type',
      'reason', 'unsupported'
    );
  END IF;

  canonical_subject_concept_id := p_subject_concept_id;
  canonical_object_concept_id := p_object_concept_id;
  IF relation_is_symmetric AND canonical_subject_concept_id > canonical_object_concept_id THEN
    canonical_subject_concept_id := p_object_concept_id;
    canonical_object_concept_id := p_subject_concept_id;
  END IF;

  SELECT
    assertions.revision,
    assertions.subject_concept_id,
    assertions.relation_code,
    assertions.object_concept_id
  INTO
    snapshot_revision,
    snapshot_subject_concept_id,
    snapshot_relation_code,
    snapshot_object_concept_id
  FROM knowledge_graph.graph_assertions AS assertions
  WHERE assertions.id = p_assertion_id;
  snapshot_exists := FOUND;

  lock_keys := pg_catalog.jsonb_build_array(
    knowledge_graph.graph_lock_key_assertion_id(p_assertion_id)
  );

  IF snapshot_exists THEN
    lock_keys := lock_keys || pg_catalog.jsonb_build_array(
      knowledge_graph.graph_lock_key_concept_id(snapshot_subject_concept_id),
      knowledge_graph.graph_lock_key_concept_id(snapshot_object_concept_id),
      knowledge_graph.graph_lock_key_semantic_assertion(
        snapshot_relation_code,
        snapshot_subject_concept_id,
        snapshot_object_concept_id
      )
    );
  END IF;

  lock_keys := lock_keys || pg_catalog.jsonb_build_array(
    knowledge_graph.graph_lock_key_concept_id(canonical_subject_concept_id),
    knowledge_graph.graph_lock_key_concept_id(canonical_object_concept_id),
    knowledge_graph.graph_lock_key_semantic_assertion(
      p_relation_code,
      canonical_subject_concept_id,
      canonical_object_concept_id
    )
  );

  PERFORM knowledge_graph.graph_acquire_mutation_locks(
    lock_keys,
    ARRAY[
      snapshot_subject_concept_id,
      p_subject_concept_id,
      p_object_concept_id,
      snapshot_object_concept_id
    ],
    ARRAY[p_assertion_id],
    ARRAY[]::bigint[]
  );

  SELECT
    assertions.revision,
    assertions.subject_concept_id,
    assertions.relation_code,
    assertions.object_concept_id
  INTO
    current_revision,
    current_subject_concept_id,
    current_relation_code,
    current_object_concept_id
  FROM knowledge_graph.graph_assertions AS assertions
  WHERE assertions.id = p_assertion_id;

  IF NOT FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'assertion',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'assertion',
        'id', p_assertion_id
      )
    );
  END IF;

  IF NOT snapshot_exists
    OR current_revision IS DISTINCT FROM snapshot_revision
    OR current_subject_concept_id IS DISTINCT FROM snapshot_subject_concept_id
    OR current_relation_code IS DISTINCT FROM snapshot_relation_code
    OR current_object_concept_id IS DISTINCT FROM snapshot_object_concept_id
  THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'conflict',
      'field', 'snapshot',
      'reason', 'snapshot_drift',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'assertion',
        'id', p_assertion_id
      ),
      'current_revision', current_revision
    );
  END IF;

  IF p_expected_revision IS DISTINCT FROM current_revision THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'stale',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'assertion',
        'id', p_assertion_id
      ),
      'current_revision', current_revision
    );
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_concepts AS concepts
    WHERE concepts.id = canonical_subject_concept_id
  ) THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'concept',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'concept',
        'id', canonical_subject_concept_id
      )
    );
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_concepts AS concepts
    WHERE concepts.id = canonical_object_concept_id
  ) THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'concept',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'concept',
        'id', canonical_object_concept_id
      )
    );
  END IF;

  SELECT assertions.id, assertions.revision
  INTO conflict_assertion_id, conflict_revision
  FROM knowledge_graph.graph_assertions AS assertions
  WHERE assertions.id <> p_assertion_id
    AND assertions.subject_concept_id = canonical_subject_concept_id
    AND assertions.relation_code = p_relation_code COLLATE "C"
    AND assertions.object_concept_id = canonical_object_concept_id
  ORDER BY assertions.id ASC
  LIMIT 1;

  IF FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'conflict',
      'field', 'semantic_assertion',
      'reason', 'duplicate_semantic_assertion',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'assertion',
        'id', conflict_assertion_id
      ),
      'current_revision', conflict_revision
    );
  END IF;

  UPDATE knowledge_graph.graph_assertions AS assertions
  SET
    subject_concept_id = canonical_subject_concept_id,
    relation_code = p_relation_code,
    object_concept_id = canonical_object_concept_id,
    revision = assertions.revision + 1
  WHERE assertions.id = p_assertion_id
  RETURNING assertions.revision INTO new_revision;

  RETURN pg_catalog.jsonb_build_object(
    'outcome', 'updated',
    'record', pg_catalog.jsonb_build_object(
      'id', p_assertion_id,
      'revision', new_revision,
      'subject_concept_id', canonical_subject_concept_id,
      'relation_type', p_relation_code,
      'object_concept_id', canonical_object_concept_id,
      'evidence', (
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
            ORDER BY source_references.source_kind COLLATE "C" ASC,
              source_references.external_id COLLATE "C" ASC,
              source_references.external_version ASC NULLS FIRST
          ),
          '[]'::jsonb
        )
        FROM knowledge_graph.graph_assertion_evidence AS evidence
        JOIN knowledge_graph.graph_source_references AS source_references
          ON source_references.source_ref_id = evidence.source_ref_id
        WHERE evidence.assertion_id = p_assertion_id
      )
    )
  );
END;
$function$;

CREATE FUNCTION knowledge_graph.graph_delete_assertion(
  p_assertion_id uuid,
  p_expected_revision bigint
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  lock_keys jsonb := '[]'::jsonb;
  snapshot_exists boolean := FALSE;
  snapshot_revision bigint;
  snapshot_subject_concept_id uuid;
  snapshot_relation_code text;
  snapshot_object_concept_id uuid;
  current_revision bigint;
  current_subject_concept_id uuid;
  current_relation_code text;
  current_object_concept_id uuid;
  snapshot_evidence jsonb := '[]'::jsonb;
  current_evidence jsonb := '[]'::jsonb;
  snapshot_source_ref_ids bigint[] := ARRAY[]::bigint[];
  snapshot_source record;
BEGIN
  IF p_assertion_id IS NULL
    OR pg_catalog.substring(p_assertion_id::text, 15, 1) IS DISTINCT FROM '7'
    OR pg_catalog.substring(p_assertion_id::text, 20, 1) NOT IN ('8', '9', 'a', 'b')
  THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'delete_assertion',
      'field', 'assertion_id',
      'reason', 'not_uuid_v7'
    );
  END IF;

  IF p_expected_revision IS NULL OR p_expected_revision <= 0 THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'delete_assertion',
      'field', 'revision',
      'reason', 'not_positive'
    );
  END IF;

  SELECT
    assertions.revision,
    assertions.subject_concept_id,
    assertions.relation_code,
    assertions.object_concept_id
  INTO
    snapshot_revision,
    snapshot_subject_concept_id,
    snapshot_relation_code,
    snapshot_object_concept_id
  FROM knowledge_graph.graph_assertions AS assertions
  WHERE assertions.id = p_assertion_id;
  snapshot_exists := FOUND;

  IF snapshot_exists THEN
    SELECT
      COALESCE(
        pg_catalog.jsonb_agg(
          pg_catalog.jsonb_build_object(
            'source_ref_id', source_references.source_ref_id,
            'source_kind', source_references.source_kind,
            'external_id', source_references.external_id,
            'external_version', source_references.external_version
          )
          ORDER BY source_references.source_ref_id ASC
        ),
        '[]'::jsonb
      ),
      COALESCE(
        pg_catalog.array_agg(source_references.source_ref_id
          ORDER BY source_references.source_ref_id ASC),
        ARRAY[]::bigint[]
      )
    INTO snapshot_evidence, snapshot_source_ref_ids
    FROM knowledge_graph.graph_assertion_evidence AS evidence
    JOIN knowledge_graph.graph_source_references AS source_references
      ON source_references.source_ref_id = evidence.source_ref_id
    WHERE evidence.assertion_id = p_assertion_id;
  END IF;

  lock_keys := pg_catalog.jsonb_build_array(
    knowledge_graph.graph_lock_key_assertion_id(p_assertion_id)
  );

  IF snapshot_exists THEN
    lock_keys := lock_keys || pg_catalog.jsonb_build_array(
      knowledge_graph.graph_lock_key_concept_id(snapshot_subject_concept_id),
      knowledge_graph.graph_lock_key_concept_id(snapshot_object_concept_id),
      knowledge_graph.graph_lock_key_semantic_assertion(
        snapshot_relation_code,
        snapshot_subject_concept_id,
        snapshot_object_concept_id
      )
    );
  END IF;

  FOR snapshot_source IN
    SELECT
      requested.value ->> 'source_kind' AS source_kind,
      requested.value ->> 'external_id' AS external_id,
      (requested.value ->> 'external_version')::bigint AS external_version
    FROM pg_catalog.jsonb_array_elements(snapshot_evidence) AS requested(value)
  LOOP
    lock_keys := lock_keys || pg_catalog.jsonb_build_array(
      knowledge_graph.graph_lock_key_source_identity(
        snapshot_source.source_kind,
        snapshot_source.external_id,
        snapshot_source.external_version
      )
    );
  END LOOP;

  PERFORM knowledge_graph.graph_acquire_mutation_locks(
    lock_keys,
    ARRAY[snapshot_subject_concept_id, snapshot_object_concept_id],
    ARRAY[p_assertion_id],
    COALESCE(snapshot_source_ref_ids, ARRAY[]::bigint[])
  );

  SELECT
    assertions.revision,
    assertions.subject_concept_id,
    assertions.relation_code,
    assertions.object_concept_id
  INTO
    current_revision,
    current_subject_concept_id,
    current_relation_code,
    current_object_concept_id
  FROM knowledge_graph.graph_assertions AS assertions
  WHERE assertions.id = p_assertion_id;

  IF NOT FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'assertion',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'assertion',
        'id', p_assertion_id
      )
    );
  END IF;

  IF NOT snapshot_exists
    OR current_revision IS DISTINCT FROM snapshot_revision
    OR current_subject_concept_id IS DISTINCT FROM snapshot_subject_concept_id
    OR current_relation_code IS DISTINCT FROM snapshot_relation_code
    OR current_object_concept_id IS DISTINCT FROM snapshot_object_concept_id
  THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'conflict',
      'field', 'snapshot',
      'reason', 'snapshot_drift',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'assertion',
        'id', p_assertion_id
      ),
      'current_revision', current_revision
    );
  END IF;

  SELECT COALESCE(
    pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'source_ref_id', source_references.source_ref_id,
        'source_kind', source_references.source_kind,
        'external_id', source_references.external_id,
        'external_version', source_references.external_version
      )
      ORDER BY source_references.source_ref_id ASC
    ),
    '[]'::jsonb
  )
  INTO current_evidence
  FROM knowledge_graph.graph_assertion_evidence AS evidence
  JOIN knowledge_graph.graph_source_references AS source_references
    ON source_references.source_ref_id = evidence.source_ref_id
  WHERE evidence.assertion_id = p_assertion_id;

  IF current_evidence IS DISTINCT FROM snapshot_evidence THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'conflict',
      'field', 'snapshot',
      'reason', 'snapshot_drift',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'assertion',
        'id', p_assertion_id
      ),
      'current_revision', current_revision
    );
  END IF;

  IF p_expected_revision IS DISTINCT FROM current_revision THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'stale',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'assertion',
        'id', p_assertion_id
      ),
      'current_revision', current_revision
    );
  END IF;

  DELETE FROM knowledge_graph.graph_assertions AS assertions
  WHERE assertions.id = p_assertion_id;

  RETURN pg_catalog.jsonb_build_object(
    'outcome', 'deleted'
  );
END;
$function$;

CREATE FUNCTION knowledge_graph.graph_add_assertion_evidence(
  p_assertion_id uuid,
  p_source_kind text,
  p_external_id text,
  p_external_version bigint
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  lock_keys jsonb;
  snapshot_source_exists boolean := FALSE;
  snapshot_source_ref_id bigint;
  current_revision bigint;
  current_source_ref_id bigint;
  current_source_kind text;
  current_external_id text;
  current_external_version bigint;
  current_source_json jsonb;
  current_evidence_count bigint := 0;
BEGIN
  IF p_assertion_id IS NULL
    OR pg_catalog.substring(p_assertion_id::text, 15, 1) IS DISTINCT FROM '7'
    OR pg_catalog.substring(p_assertion_id::text, 20, 1) NOT IN ('8', '9', 'a', 'b')
  THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'add_evidence',
      'field', 'assertion_id',
      'reason', 'not_uuid_v7'
    );
  END IF;

  lock_keys := pg_catalog.jsonb_build_array(
    knowledge_graph.graph_lock_key_assertion_id(p_assertion_id),
    knowledge_graph.graph_lock_key_source_identity(
      p_source_kind,
      p_external_id,
      p_external_version
    )
  );

  SELECT source_references.source_ref_id
  INTO snapshot_source_ref_id
  FROM knowledge_graph.graph_source_references AS source_references
  WHERE source_references.source_kind = p_source_kind COLLATE "C"
    AND source_references.external_id = p_external_id COLLATE "C"
    AND source_references.external_version IS NOT DISTINCT FROM p_external_version;
  snapshot_source_exists := FOUND;

  PERFORM knowledge_graph.graph_acquire_mutation_locks(
    lock_keys,
    ARRAY[]::uuid[],
    ARRAY[p_assertion_id],
    ARRAY[snapshot_source_ref_id]
  );

  SELECT assertions.revision
  INTO current_revision
  FROM knowledge_graph.graph_assertions AS assertions
  WHERE assertions.id = p_assertion_id;

  IF NOT FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'assertion'
    );
  END IF;

  SELECT
    source_references.source_ref_id,
    source_references.source_kind,
    source_references.external_id,
    source_references.external_version
  INTO
    current_source_ref_id,
    current_source_kind,
    current_external_id,
    current_external_version
  FROM knowledge_graph.graph_source_references AS source_references
  WHERE source_references.source_kind = p_source_kind COLLATE "C"
    AND source_references.external_id = p_external_id COLLATE "C"
    AND source_references.external_version IS NOT DISTINCT FROM p_external_version;

  IF NOT FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'source_reference'
    );
  END IF;

  IF NOT snapshot_source_exists
    OR current_source_ref_id IS DISTINCT FROM snapshot_source_ref_id
  THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'conflict',
      'field', 'snapshot',
      'reason', 'snapshot_drift',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'assertion',
        'id', p_assertion_id
      ),
      'current_revision', current_revision
    );
  END IF;

  current_source_json := CASE current_source_kind COLLATE "C"
    WHEN 'memory_version' THEN pg_catalog.jsonb_build_object(
      'kind', 'memory_version',
      'memory_id', current_external_id,
      'version', current_external_version
    )
    ELSE pg_catalog.jsonb_build_object(
      'kind', 'session_record',
      'session_record_id', current_external_id
    )
  END;

  IF EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_assertion_evidence AS evidence
    WHERE evidence.assertion_id = p_assertion_id
      AND evidence.source_ref_id = current_source_ref_id
  ) THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'existing',
      'record', pg_catalog.jsonb_build_object(
        'assertion_id', p_assertion_id,
        'source', current_source_json
      )
    );
  END IF;

  SELECT pg_catalog.count(*)
  INTO current_evidence_count
  FROM knowledge_graph.graph_assertion_evidence AS evidence
  WHERE evidence.assertion_id = p_assertion_id;

  IF current_evidence_count >= 32 THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'evidence_limit'
    );
  END IF;

  INSERT INTO knowledge_graph.graph_assertion_evidence (
    assertion_id,
    source_ref_id
  )
  VALUES (
    p_assertion_id,
    current_source_ref_id
  )
  ON CONFLICT (assertion_id, source_ref_id) DO NOTHING;

  RETURN pg_catalog.jsonb_build_object(
    'outcome', 'created',
    'record', pg_catalog.jsonb_build_object(
      'assertion_id', p_assertion_id,
      'source', current_source_json
    )
  );
END;
$function$;

CREATE FUNCTION knowledge_graph.graph_remove_assertion_evidence(
  p_assertion_id uuid,
  p_source_kind text,
  p_external_id text,
  p_external_version bigint
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  lock_keys jsonb;
  snapshot_source_exists boolean := FALSE;
  snapshot_source_ref_id bigint;
  current_revision bigint;
  current_source_ref_id bigint;
  other_evidence_count bigint := 0;
BEGIN
  IF p_assertion_id IS NULL
    OR pg_catalog.substring(p_assertion_id::text, 15, 1) IS DISTINCT FROM '7'
    OR pg_catalog.substring(p_assertion_id::text, 20, 1) NOT IN ('8', '9', 'a', 'b')
  THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'invalid_input',
      'operation', 'remove_evidence',
      'field', 'assertion_id',
      'reason', 'not_uuid_v7'
    );
  END IF;

  lock_keys := pg_catalog.jsonb_build_array(
    knowledge_graph.graph_lock_key_assertion_id(p_assertion_id),
    knowledge_graph.graph_lock_key_source_identity(
      p_source_kind,
      p_external_id,
      p_external_version
    )
  );

  SELECT source_references.source_ref_id
  INTO snapshot_source_ref_id
  FROM knowledge_graph.graph_source_references AS source_references
  WHERE source_references.source_kind = p_source_kind COLLATE "C"
    AND source_references.external_id = p_external_id COLLATE "C"
    AND source_references.external_version IS NOT DISTINCT FROM p_external_version;
  snapshot_source_exists := FOUND;

  PERFORM knowledge_graph.graph_acquire_mutation_locks(
    lock_keys,
    ARRAY[]::uuid[],
    ARRAY[p_assertion_id],
    ARRAY[snapshot_source_ref_id]
  );

  SELECT assertions.revision
  INTO current_revision
  FROM knowledge_graph.graph_assertions AS assertions
  WHERE assertions.id = p_assertion_id;

  IF NOT FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'assertion'
    );
  END IF;

  SELECT source_references.source_ref_id
  INTO current_source_ref_id
  FROM knowledge_graph.graph_source_references AS source_references
  WHERE source_references.source_kind = p_source_kind COLLATE "C"
    AND source_references.external_id = p_external_id COLLATE "C"
    AND source_references.external_version IS NOT DISTINCT FROM p_external_version;

  IF NOT FOUND THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'source_reference'
    );
  END IF;

  IF NOT snapshot_source_exists
    OR current_source_ref_id IS DISTINCT FROM snapshot_source_ref_id
  THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'conflict',
      'field', 'snapshot',
      'reason', 'snapshot_drift',
      'identity', pg_catalog.jsonb_build_object(
        'kind', 'assertion',
        'id', p_assertion_id
      ),
      'current_revision', current_revision
    );
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM knowledge_graph.graph_assertion_evidence AS evidence
    WHERE evidence.assertion_id = p_assertion_id
      AND evidence.source_ref_id = current_source_ref_id
  ) THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'missing',
      'record_kind', 'assertion_evidence'
    );
  END IF;

  SELECT pg_catalog.count(*)
  INTO other_evidence_count
  FROM knowledge_graph.graph_assertion_evidence AS evidence
  WHERE evidence.assertion_id = p_assertion_id
    AND evidence.source_ref_id <> current_source_ref_id;

  IF other_evidence_count = 0 THEN
    RETURN pg_catalog.jsonb_build_object(
      'outcome', 'would_orphan_evidence',
      'assertion_id', p_assertion_id,
      'current_revision', current_revision
    );
  END IF;

  DELETE FROM knowledge_graph.graph_assertion_evidence AS evidence
  WHERE evidence.assertion_id = p_assertion_id
    AND evidence.source_ref_id = current_source_ref_id;

  RETURN pg_catalog.jsonb_build_object(
    'outcome', 'deleted'
  );
END;
$function$;

CREATE FUNCTION knowledge_graph.graph_enforce_assertion_endpoint_order()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  relation_is_symmetric boolean;
BEGIN
  IF TG_TABLE_SCHEMA <> 'knowledge_graph' OR TG_TABLE_NAME <> 'graph_assertions' THEN
    RAISE EXCEPTION 'invalid graph assertion trigger target' USING ERRCODE = '55000';
  END IF;

  SELECT relation_types.is_symmetric
  INTO relation_is_symmetric
  FROM knowledge_graph.graph_relation_types AS relation_types
  WHERE relation_types.code = NEW.relation_code;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'invalid graph assertion relation' USING ERRCODE = '23503';
  END IF;

  IF relation_is_symmetric AND NEW.subject_concept_id > NEW.object_concept_id THEN
    RAISE EXCEPTION 'symmetric assertion endpoints must be canonical'
      USING ERRCODE = '23514';
  END IF;

  RETURN NEW;
END;
$function$;

CREATE TRIGGER graph_assertions_canonical_symmetric_endpoints
BEFORE INSERT OR UPDATE OF subject_concept_id, relation_code, object_concept_id
ON knowledge_graph.graph_assertions
FOR EACH ROW
EXECUTE FUNCTION knowledge_graph.graph_enforce_assertion_endpoint_order();

CREATE FUNCTION knowledge_graph.graph_enforce_assertion_evidence_invariant()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  affected_assertion_ids uuid[];
  assertion_id_to_check uuid;
BEGIN
  IF TG_TABLE_SCHEMA <> 'knowledge_graph' THEN
    RAISE EXCEPTION 'invalid graph trigger target' USING ERRCODE = '55000';
  ELSIF TG_TABLE_NAME = 'graph_assertions' THEN
    IF TG_OP = 'INSERT' THEN
      affected_assertion_ids := ARRAY[NEW.id];
    ELSIF TG_OP = 'UPDATE' THEN
      affected_assertion_ids := ARRAY[OLD.id, NEW.id];
    ELSE
      affected_assertion_ids := ARRAY[OLD.id];
    END IF;
  ELSIF TG_TABLE_NAME = 'graph_assertion_evidence' THEN
    IF TG_OP = 'INSERT' THEN
      affected_assertion_ids := ARRAY[NEW.assertion_id];
    ELSIF TG_OP = 'UPDATE' THEN
      affected_assertion_ids := ARRAY[OLD.assertion_id, NEW.assertion_id];
    ELSE
      affected_assertion_ids := ARRAY[OLD.assertion_id];
    END IF;
  ELSE
    RAISE EXCEPTION 'invalid graph trigger table' USING ERRCODE = '55000';
  END IF;

  FOR assertion_id_to_check IN
    SELECT DISTINCT requested.assertion_id
    FROM pg_catalog.unnest(affected_assertion_ids) AS requested(assertion_id)
  LOOP
    IF EXISTS (
      SELECT 1
      FROM knowledge_graph.graph_assertions AS assertions
      WHERE assertions.id = assertion_id_to_check
    ) AND NOT EXISTS (
      SELECT 1
      FROM knowledge_graph.graph_assertion_evidence AS evidence
      WHERE evidence.assertion_id = assertion_id_to_check
    ) THEN
      RAISE EXCEPTION 'graph assertion evidence invariant violated'
        USING ERRCODE = '23514';
    END IF;
  END LOOP;

  RETURN NULL;
END;
$function$;

CREATE CONSTRAINT TRIGGER graph_assertions_evidence_invariant
AFTER INSERT OR UPDATE OR DELETE ON knowledge_graph.graph_assertions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION knowledge_graph.graph_enforce_assertion_evidence_invariant();

CREATE CONSTRAINT TRIGGER graph_assertion_evidence_invariant
AFTER INSERT OR UPDATE OR DELETE ON knowledge_graph.graph_assertion_evidence
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION knowledge_graph.graph_enforce_assertion_evidence_invariant();

CREATE FUNCTION knowledge_graph.graph_enforce_concept_alias_invariant()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  affected_concept_ids uuid[];
  concept_id_to_check uuid;
  preferred_alias_count bigint;
BEGIN
  IF TG_TABLE_SCHEMA <> 'knowledge_graph' THEN
    RAISE EXCEPTION 'invalid graph trigger target' USING ERRCODE = '55000';
  ELSIF TG_TABLE_NAME = 'graph_concepts' THEN
    IF TG_OP = 'INSERT' THEN
      affected_concept_ids := ARRAY[NEW.id];
    ELSIF TG_OP = 'UPDATE' THEN
      affected_concept_ids := ARRAY[OLD.id, NEW.id];
    ELSE
      affected_concept_ids := ARRAY[OLD.id];
    END IF;
  ELSIF TG_TABLE_NAME = 'graph_aliases' THEN
    IF TG_OP = 'INSERT' THEN
      affected_concept_ids := ARRAY[NEW.concept_id];
    ELSIF TG_OP = 'UPDATE' THEN
      affected_concept_ids := ARRAY[OLD.concept_id, NEW.concept_id];
    ELSE
      affected_concept_ids := ARRAY[OLD.concept_id];
    END IF;
  ELSE
    RAISE EXCEPTION 'invalid graph trigger table' USING ERRCODE = '55000';
  END IF;

  FOR concept_id_to_check IN
    SELECT DISTINCT requested.concept_id
    FROM pg_catalog.unnest(affected_concept_ids) AS requested(concept_id)
  LOOP
    IF EXISTS (
      SELECT 1
      FROM knowledge_graph.graph_concepts AS concepts
      WHERE concepts.id = concept_id_to_check
    ) THEN
      SELECT pg_catalog.count(*)
      INTO preferred_alias_count
      FROM knowledge_graph.graph_aliases AS aliases
      WHERE aliases.concept_id = concept_id_to_check
        AND aliases.is_preferred;

      IF preferred_alias_count <> 1 THEN
        RAISE EXCEPTION 'graph concept preferred-alias invariant violated'
          USING ERRCODE = '23514';
      END IF;
    END IF;
  END LOOP;

  RETURN NULL;
END;
$function$;

CREATE CONSTRAINT TRIGGER graph_concepts_preferred_alias_invariant
AFTER INSERT OR UPDATE OR DELETE ON knowledge_graph.graph_concepts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION knowledge_graph.graph_enforce_concept_alias_invariant();

CREATE CONSTRAINT TRIGGER graph_aliases_preferred_alias_invariant
AFTER INSERT OR UPDATE OR DELETE ON knowledge_graph.graph_aliases
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION knowledge_graph.graph_enforce_concept_alias_invariant();

REVOKE ALL ON SCHEMA knowledge_graph
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON TABLE
  knowledge_graph.graph_relation_types,
  knowledge_graph.graph_concepts,
  knowledge_graph.graph_aliases,
  knowledge_graph.graph_source_references,
  knowledge_graph.graph_concept_mentions,
  knowledge_graph.graph_assertions,
  knowledge_graph.graph_assertion_evidence
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON ALL SEQUENCES IN SCHEMA knowledge_graph
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_enforce_concept_alias_invariant()
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_enforce_assertion_endpoint_order()
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_enforce_assertion_evidence_invariant()
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_lock_key_concept_id(uuid)
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_lock_key_normalized_alias(text)
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_lock_key_source_identity(text, text, bigint)
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_lock_key_concept_mention(uuid, text, text, bigint)
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_lock_key_assertion_id(uuid)
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_lock_key_semantic_assertion(text, uuid, uuid)
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_acquire_mutation_locks(jsonb, uuid[], uuid[], bigint[])
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_create_concept(uuid, jsonb)
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_replace_concept_aliases(uuid, bigint, jsonb)
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_delete_concept(uuid, bigint)
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_register_source(text, text, bigint)
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_delete_source(text, text, bigint)
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_create_mention(uuid, text, text, bigint)
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_delete_mention(uuid, text, text, bigint)
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_create_assertion(uuid, uuid, text, uuid, jsonb)
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_update_assertion(uuid, bigint, uuid, text, uuid)
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_delete_assertion(uuid, bigint)
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_add_assertion_evidence(uuid, text, text, bigint)
FROM PUBLIC, knowledge_graph_application;

REVOKE ALL ON FUNCTION
  knowledge_graph.graph_remove_assertion_evidence(uuid, text, text, bigint)
FROM PUBLIC, knowledge_graph_application;

GRANT USAGE ON SCHEMA knowledge_graph
TO knowledge_graph_application;

GRANT SELECT ON TABLE
  knowledge_graph.graph_relation_types,
  knowledge_graph.graph_concepts,
  knowledge_graph.graph_aliases,
  knowledge_graph.graph_source_references,
  knowledge_graph.graph_concept_mentions,
  knowledge_graph.graph_assertions,
  knowledge_graph.graph_assertion_evidence
TO knowledge_graph_application;

GRANT EXECUTE ON FUNCTION knowledge_graph.graph_create_concept(uuid, jsonb)
TO knowledge_graph_application;

GRANT EXECUTE ON FUNCTION
  knowledge_graph.graph_replace_concept_aliases(uuid, bigint, jsonb)
TO knowledge_graph_application;

GRANT EXECUTE ON FUNCTION knowledge_graph.graph_delete_concept(uuid, bigint)
TO knowledge_graph_application;

GRANT EXECUTE ON FUNCTION knowledge_graph.graph_register_source(text, text, bigint)
TO knowledge_graph_application;

GRANT EXECUTE ON FUNCTION knowledge_graph.graph_delete_source(text, text, bigint)
TO knowledge_graph_application;

GRANT EXECUTE ON FUNCTION
  knowledge_graph.graph_create_mention(uuid, text, text, bigint)
TO knowledge_graph_application;

GRANT EXECUTE ON FUNCTION
  knowledge_graph.graph_delete_mention(uuid, text, text, bigint)
TO knowledge_graph_application;

GRANT EXECUTE ON FUNCTION
  knowledge_graph.graph_create_assertion(uuid, uuid, text, uuid, jsonb)
TO knowledge_graph_application;

GRANT EXECUTE ON FUNCTION
  knowledge_graph.graph_update_assertion(uuid, bigint, uuid, text, uuid)
TO knowledge_graph_application;

GRANT EXECUTE ON FUNCTION knowledge_graph.graph_delete_assertion(uuid, bigint)
TO knowledge_graph_application;

GRANT EXECUTE ON FUNCTION
  knowledge_graph.graph_add_assertion_evidence(uuid, text, text, bigint)
TO knowledge_graph_application;

GRANT EXECUTE ON FUNCTION
  knowledge_graph.graph_remove_assertion_evidence(uuid, text, text, bigint)
TO knowledge_graph_application;
