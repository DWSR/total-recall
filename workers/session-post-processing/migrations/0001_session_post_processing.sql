CREATE TABLE public.session_records (
    session_id TEXT PRIMARY KEY,
    source_revision UUID NOT NULL,
    lifecycle_count BIGINT NOT NULL,
    observation_count BIGINT NOT NULL,
    source_cutoff TIMESTAMPTZ NOT NULL,
    transcript JSONB NOT NULL,
    summary_sentences TEXT[] NOT NULL,
    summary TEXT NOT NULL,
    concepts TEXT[] NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT session_records_session_id_not_empty CHECK (session_id <> ''),
    CONSTRAINT session_records_lifecycle_count_nonnegative CHECK (lifecycle_count >= 0),
    CONSTRAINT session_records_observation_count_nonnegative CHECK (observation_count >= 0),
    CONSTRAINT session_records_source_count_positive CHECK (
        lifecycle_count > 0 OR observation_count > 0
    ),
    CONSTRAINT session_records_summary_sentence_count CHECK (
        cardinality(summary_sentences) BETWEEN 1 AND 5
    ),
    CONSTRAINT session_records_summary_not_empty CHECK (summary <> ''),
    CONSTRAINT session_records_concept_count CHECK (cardinality(concepts) = 10),
    CONSTRAINT session_records_generated_after_cutoff CHECK (generated_at >= source_cutoff)
);

CREATE TABLE public.session_processing_attempts (
    attempt_id UUID PRIMARY KEY,
    session_id TEXT NOT NULL,
    source_revision UUID NOT NULL,
    lifecycle_count BIGINT NOT NULL,
    observation_count BIGINT NOT NULL,
    state TEXT NOT NULL,
    lease_token UUID NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    next_attempt_at TIMESTAMPTZ,
    failure_category TEXT,
    source_cutoff TIMESTAMPTZ,
    transcript JSONB,
    summary_sentences TEXT[],
    summary TEXT,
    concepts TEXT[],
    generated_at TIMESTAMPTZ,
    CONSTRAINT session_processing_attempts_session_id_not_empty CHECK (session_id <> ''),
    CONSTRAINT session_processing_attempts_source_revision_key UNIQUE (source_revision),
    CONSTRAINT session_processing_attempts_lifecycle_count_nonnegative CHECK (
        lifecycle_count >= 0
    ),
    CONSTRAINT session_processing_attempts_observation_count_nonnegative CHECK (
        observation_count >= 0
    ),
    CONSTRAINT session_processing_attempts_source_count_positive CHECK (
        lifecycle_count > 0 OR observation_count > 0
    ),
    CONSTRAINT session_processing_attempts_state_check CHECK (
        state IN ('claimed', 'staged', 'publishing', 'complete', 'superseded', 'retryable')
    ),
    CONSTRAINT session_processing_attempts_retry_time_check CHECK (
        (state = 'retryable' AND next_attempt_at IS NOT NULL)
        OR (state <> 'retryable' AND next_attempt_at IS NULL)
    ),
    CONSTRAINT session_processing_attempts_failure_category_not_empty CHECK (
        failure_category IS NULL OR failure_category <> ''
    ),
    CONSTRAINT session_processing_attempts_staged_columns_complete CHECK (
        (
            source_cutoff IS NULL
            AND transcript IS NULL
            AND summary_sentences IS NULL
            AND summary IS NULL
            AND concepts IS NULL
            AND generated_at IS NULL
        )
        OR (
            source_cutoff IS NOT NULL
            AND transcript IS NOT NULL
            AND summary_sentences IS NOT NULL
            AND summary IS NOT NULL
            AND concepts IS NOT NULL
            AND generated_at IS NOT NULL
        )
    ),
    CONSTRAINT session_processing_attempts_summary_sentence_count CHECK (
        summary_sentences IS NULL OR cardinality(summary_sentences) BETWEEN 1 AND 5
    ),
    CONSTRAINT session_processing_attempts_summary_not_empty CHECK (
        summary IS NULL OR summary <> ''
    ),
    CONSTRAINT session_processing_attempts_concept_count CHECK (
        concepts IS NULL OR cardinality(concepts) = 10
    ),
    CONSTRAINT session_processing_attempts_generated_after_cutoff CHECK (
        generated_at IS NULL OR generated_at >= source_cutoff
    ),
    CONSTRAINT session_processing_attempts_staged_state_requires_output CHECK (
        state NOT IN ('staged', 'publishing', 'complete')
        OR source_cutoff IS NOT NULL
    )
);

CREATE TABLE public.session_memory_candidates (
    attempt_id UUID NOT NULL,
    ordinal INTEGER NOT NULL,
    session_id TEXT NOT NULL,
    content_fingerprint TEXT NOT NULL,
    memory_id UUID NOT NULL,
    canonical_payload JSONB NOT NULL,
    supporting_receipt_ids UUID[] NOT NULL,
    published_at TIMESTAMPTZ,
    CONSTRAINT session_memory_candidates_pkey PRIMARY KEY (attempt_id, ordinal),
    CONSTRAINT session_memory_candidates_attempt_fk
        FOREIGN KEY (attempt_id)
        REFERENCES public.session_processing_attempts (attempt_id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT
        NOT DEFERRABLE,
    CONSTRAINT session_memory_candidates_ordinal_nonnegative CHECK (ordinal >= 0),
    CONSTRAINT session_memory_candidates_session_id_not_empty CHECK (session_id <> ''),
    CONSTRAINT session_memory_candidates_content_fingerprint_not_empty CHECK (
        content_fingerprint <> ''
    ),
    CONSTRAINT session_memory_candidates_session_content_fingerprint_key UNIQUE (
        session_id,
        content_fingerprint
    ),
    CONSTRAINT session_memory_candidates_supporting_receipts_not_empty CHECK (
        cardinality(supporting_receipt_ids) > 0
    )
);

CREATE INDEX session_records_source_revision_idx
    ON public.session_records USING btree
    (source_revision ASC);

CREATE UNIQUE INDEX session_processing_attempts_one_nonterminal_per_session_idx
    ON public.session_processing_attempts USING btree
    (session_id ASC)
    WHERE state IN ('claimed', 'staged', 'publishing', 'retryable');

CREATE INDEX session_processing_attempts_retry_idx
    ON public.session_processing_attempts USING btree
    (next_attempt_at ASC)
    WHERE state = 'retryable';

CREATE INDEX session_processing_attempts_lease_expiry_idx
    ON public.session_processing_attempts USING btree
    (lease_expires_at ASC)
    WHERE state IN ('claimed', 'staged', 'publishing');

CREATE INDEX session_memory_candidates_unpublished_idx
    ON public.session_memory_candidates USING btree
    (attempt_id ASC, ordinal ASC)
    WHERE published_at IS NULL;

ALTER TABLE public.session_records OWNER TO session_post_processing_migrator;
ALTER TABLE public.session_processing_attempts OWNER TO session_post_processing_migrator;
ALTER TABLE public.session_memory_candidates OWNER TO session_post_processing_migrator;

REVOKE ALL ON TABLE public.session_records, public.session_processing_attempts, public.session_memory_candidates FROM PUBLIC;
REVOKE ALL ON TABLE public.session_records, public.session_processing_attempts, public.session_memory_candidates FROM session_post_processing_application;
GRANT SELECT, INSERT, UPDATE ON TABLE public.session_records, public.session_processing_attempts, public.session_memory_candidates TO session_post_processing_application;
