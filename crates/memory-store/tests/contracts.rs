use chrono::{DateTime, TimeZone, Timelike, Utc};
use memory_store::contracts::{
    Bm25Search, DatabaseError, DatabaseOperation, DatabaseTarget, EmbeddingInput, MemoryId,
    MemorySearchResult, MemoryStoreError, MemoryVersion, MemoryVersionInput, MemoryVersionKey,
    ValidatedBm25Search, ValidatedEmbedding, ValidatedMemoryVersion, ValidatedVectorSearch,
    VectorSearch,
};

fn timestamp(day: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, day, 12, 34, 56)
        .single()
        .expect("test timestamp must be valid")
}

fn timestamp_with_nanoseconds(day: u32, nanoseconds: u32) -> DateTime<Utc> {
    timestamp(day)
        .with_nanosecond(nanoseconds)
        .expect("test timestamp nanoseconds must be valid")
}

fn timestamp_in_year(year: i32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0)
        .single()
        .expect("test timestamp year must be valid in Chrono")
}

fn valid_memory() -> MemoryVersionInput {
    MemoryVersionInput {
        id: "memory-1".to_owned(),
        version: 7,
        memory_type: "unrestricted/type".to_owned(),
        title: "canonical title".to_owned(),
        content: "canonical content".to_owned(),
        created_at: timestamp(18),
        updated_at: timestamp(19),
        concepts: vec!["concept-a".to_owned()],
        files: vec!["file-a".to_owned()],
        session_ids: vec!["session-a".to_owned()],
        source_observation_ids: vec!["observation-a".to_owned()],
    }
}

fn valid_embedding() -> EmbeddingInput {
    EmbeddingInput {
        id: "memory-1".to_owned(),
        version: 7,
        embedding: vec![1.25, -2.5],
    }
}

fn valid_bm25_search() -> Bm25Search {
    Bm25Search {
        query: "canonical lexical query".to_owned(),
        limit: 5,
    }
}

fn valid_vector_search() -> VectorSearch {
    VectorSearch {
        vector: vec![1.25, -2.5],
        limit: 5,
    }
}

fn assert_invalid_memory(input: MemoryVersionInput, field: &str, code: &str) {
    let error = ValidatedMemoryVersion::try_from(input).expect_err("memory should be rejected");
    assert_eq!(error.field(), field);
    assert_eq!(error.code(), code);
}

fn assert_invalid_embedding(input: EmbeddingInput, code: &str) {
    let error = ValidatedEmbedding::try_from(input).expect_err("embedding should be rejected");
    assert_eq!(error.field(), "embedding");
    assert_eq!(error.code(), code);
}

fn assert_invalid_bm25(input: Bm25Search, field: &str, code: &str) {
    let error = ValidatedBm25Search::try_from(input).expect_err("BM25 search should be rejected");
    assert_eq!(error.field(), field);
    assert_eq!(error.code(), code);
}

fn assert_invalid_vector(input: VectorSearch, field: &str, code: &str) {
    let error =
        ValidatedVectorSearch::try_from(input).expect_err("vector search should be rejected");
    assert_eq!(error.field(), field);
    assert_eq!(error.code(), code);
}

#[test]
fn valid_memory_preserves_all_canonical_fields_and_open_taxonomy() {
    let input = valid_memory();

    let memory = ValidatedMemoryVersion::try_from(input).expect("memory should be valid");

    assert_eq!(memory.id().as_str(), "memory-1");
    assert_eq!(memory.version().get(), 7);
    assert_eq!(memory.memory_type(), "unrestricted/type");
    assert_eq!(memory.title(), "canonical title");
    assert_eq!(memory.content(), "canonical content");
    assert_eq!(memory.created_at(), &timestamp(18));
    assert_eq!(memory.updated_at(), &timestamp(19));
    assert_eq!(memory.concepts(), ["concept-a"]);
    assert_eq!(memory.files(), ["file-a"]);
    assert_eq!(memory.session_ids(), ["session-a"]);
    assert_eq!(memory.source_observation_ids(), ["observation-a"]);
}

#[test]
fn empty_collections_are_valid() {
    let mut input = valid_memory();
    input.concepts.clear();
    input.files.clear();
    input.session_ids.clear();
    input.source_observation_ids.clear();

    let memory =
        ValidatedMemoryVersion::try_from(input).expect("empty collections should be valid");

    assert!(memory.concepts().is_empty());
    assert!(memory.files().is_empty());
    assert!(memory.session_ids().is_empty());
    assert!(memory.source_observation_ids().is_empty());
}

#[test]
fn collections_preserve_order_duplicates_and_blank_values() {
    let mut input = valid_memory();
    input.concepts = vec![
        "".to_owned(),
        "novel".to_owned(),
        "".to_owned(),
        "novel".to_owned(),
    ];
    input.files = vec![
        "file-b".to_owned(),
        "".to_owned(),
        "file-a".to_owned(),
        "file-b".to_owned(),
    ];
    input.session_ids = vec![
        "session-b".to_owned(),
        "".to_owned(),
        "session-b".to_owned(),
    ];
    input.source_observation_ids = vec![
        "source-b".to_owned(),
        "".to_owned(),
        "source-a".to_owned(),
        "source-b".to_owned(),
    ];

    let memory =
        ValidatedMemoryVersion::try_from(input).expect("opaque collections should be valid");

    assert_eq!(memory.concepts(), ["", "novel", "", "novel"]);
    assert_eq!(memory.files(), ["file-b", "", "file-a", "file-b"]);
    assert_eq!(memory.session_ids(), ["session-b", "", "session-b"]);
    assert_eq!(
        memory.source_observation_ids(),
        ["source-b", "", "source-a", "source-b"]
    );
}

#[test]
fn persistence_bound_text_rejects_nul_bytes_without_leaking_values() {
    const NUL_SENTINEL: &str = "persistence-nul-secret\0sentinel";

    let mut nul_id = valid_memory();
    nul_id.id = NUL_SENTINEL.to_owned();
    let mut nul_type = valid_memory();
    nul_type.memory_type = NUL_SENTINEL.to_owned();
    let mut nul_title = valid_memory();
    nul_title.title = NUL_SENTINEL.to_owned();
    let mut nul_content = valid_memory();
    nul_content.content = NUL_SENTINEL.to_owned();
    let mut nul_concepts = valid_memory();
    nul_concepts.concepts = vec![NUL_SENTINEL.to_owned()];
    let mut nul_files = valid_memory();
    nul_files.files = vec![NUL_SENTINEL.to_owned()];
    let mut nul_session_ids = valid_memory();
    nul_session_ids.session_ids = vec![NUL_SENTINEL.to_owned()];
    let mut nul_source_observation_ids = valid_memory();
    nul_source_observation_ids.source_observation_ids = vec![NUL_SENTINEL.to_owned()];

    for (input, field) in [
        (nul_id, "id"),
        (nul_type, "memory_type"),
        (nul_title, "title"),
        (nul_content, "content"),
        (nul_concepts, "concepts"),
        (nul_files, "files"),
        (nul_session_ids, "session_ids"),
        (nul_source_observation_ids, "source_observation_ids"),
    ] {
        let error = ValidatedMemoryVersion::try_from(input)
            .expect_err("NUL-bearing memory text should fail");
        assert_eq!(error.field(), field);
        assert_eq!(error.code(), "contains_nul");
        assert_error_is_opaque(&error, &[NUL_SENTINEL]);
    }

    let target_error = DatabaseTarget::try_from(NUL_SENTINEL.to_owned())
        .expect_err("NUL-bearing database targets should fail");
    assert_eq!(target_error.field(), "database_target");
    assert_eq!(target_error.code(), "contains_nul");
    assert_error_is_opaque(&target_error, &[NUL_SENTINEL]);

    let query_error = ValidatedBm25Search::try_from(Bm25Search {
        query: NUL_SENTINEL.to_owned(),
        ..valid_bm25_search()
    })
    .expect_err("NUL-bearing lexical queries should fail");
    assert_eq!(query_error.field(), "query");
    assert_eq!(query_error.code(), "contains_nul");
    assert_error_is_opaque(&query_error, &[NUL_SENTINEL]);
}

#[test]
fn canonical_empty_fields_are_rejected() {
    for field in ["id", "memory_type", "title", "content"] {
        let mut input = valid_memory();
        match field {
            "id" => input.id.clear(),
            "memory_type" => input.memory_type.clear(),
            "title" => input.title.clear(),
            "content" => input.content.clear(),
            _ => unreachable!("test field is known"),
        }

        assert_invalid_memory(input, field, "empty");
    }
}

#[test]
fn non_empty_whitespace_canonical_strings_are_preserved() {
    let mut input = valid_memory();
    input.id = " ".to_owned();
    input.memory_type = "  unrestricted/type  ".to_owned();
    input.title = " ".to_owned();
    input.content = "\t".to_owned();

    let memory = ValidatedMemoryVersion::try_from(input)
        .expect("canonical strings should be checked with is_empty only");

    assert_eq!(memory.id().as_str(), " ");
    assert_eq!(memory.memory_type(), "  unrestricted/type  ");
    assert_eq!(memory.title(), " ");
    assert_eq!(memory.content(), "\t");
}

#[test]
fn versions_must_be_positive_for_memory_and_embeddings() {
    for version in [0, -1] {
        let mut memory = valid_memory();
        memory.version = version;
        assert_invalid_memory(memory, "version", "not_positive");

        let mut embedding = valid_embedding();
        embedding.version = version;
        let error =
            ValidatedEmbedding::try_from(embedding).expect_err("embedding should be rejected");
        assert_eq!(error.field(), "version");
        assert_eq!(error.code(), "not_positive");
    }
}

#[test]
fn embedding_keys_require_a_non_empty_id() {
    let mut input = valid_embedding();
    input.id.clear();

    let error = ValidatedEmbedding::try_from(input).expect_err("embedding should be rejected");

    assert_eq!(error.field(), "id");
    assert_eq!(error.code(), "empty");
}

#[test]
fn updated_at_may_equal_created_at_but_must_not_precede_it() {
    let mut equal_times = valid_memory();
    equal_times.created_at = timestamp_with_nanoseconds(18, 123_456_000);
    equal_times.updated_at = equal_times.created_at;
    ValidatedMemoryVersion::try_from(equal_times).expect("equal timestamps should be valid");

    let mut inverted_times = valid_memory();
    inverted_times.updated_at = timestamp(17);
    assert_invalid_memory(inverted_times, "updated_at", "before_created_at");

    let mut inverted_fractional_times = valid_memory();
    inverted_fractional_times.created_at = timestamp_with_nanoseconds(18, 123_456_000);
    inverted_fractional_times.updated_at = timestamp_with_nanoseconds(18, 123_455_000);
    assert_invalid_memory(inverted_fractional_times, "updated_at", "before_created_at");
}

#[test]
fn canonical_timestamps_accept_milliseconds_and_microseconds_without_rounding() {
    let created_at = timestamp_with_nanoseconds(18, 123_000_000);
    let updated_at = timestamp_with_nanoseconds(19, 654_321_000);
    let mut input = valid_memory();
    input.created_at = created_at;
    input.updated_at = updated_at;

    let memory = ValidatedMemoryVersion::try_from(input)
        .expect("millisecond and microsecond timestamps should be valid");

    assert_eq!(memory.created_at(), &created_at);
    assert_eq!(memory.updated_at(), &updated_at);
}

#[test]
fn canonical_timestamps_reject_submicrosecond_precision_without_leaking_values() {
    let mut submicrosecond_created_at = valid_memory();
    submicrosecond_created_at.created_at = timestamp_with_nanoseconds(18, 123_456_001);
    let created_at_sentinel = submicrosecond_created_at.created_at.to_rfc3339();
    let created_at_error = ValidatedMemoryVersion::try_from(submicrosecond_created_at)
        .expect_err("submicrosecond created_at values should fail");
    assert_eq!(created_at_error.field(), "created_at");
    assert_eq!(created_at_error.code(), "not_microsecond_aligned");
    assert_error_is_opaque(&created_at_error, &[&created_at_sentinel]);

    let mut submicrosecond_updated_at = valid_memory();
    submicrosecond_updated_at.updated_at = timestamp_with_nanoseconds(19, 654_321_001);
    let updated_at_sentinel = submicrosecond_updated_at.updated_at.to_rfc3339();
    let updated_at_error = ValidatedMemoryVersion::try_from(submicrosecond_updated_at)
        .expect_err("submicrosecond updated_at values should fail");
    assert_eq!(updated_at_error.field(), "updated_at");
    assert_eq!(updated_at_error.code(), "not_microsecond_aligned");
    assert_error_is_opaque(&updated_at_error, &[&updated_at_sentinel]);
}

#[test]
fn canonical_timestamps_require_ad_years_one_through_9999() {
    for year in [1, 9999] {
        for nanoseconds in [0, 123_456_000] {
            let boundary = timestamp_in_year(year)
                .with_nanosecond(nanoseconds)
                .expect("boundary timestamp precision should be valid");
            let mut input = valid_memory();
            input.created_at = boundary;
            input.updated_at = boundary;

            let memory = ValidatedMemoryVersion::try_from(input)
                .expect("supported years and microsecond precision should be valid");
            assert_eq!(memory.created_at(), &boundary);
            assert_eq!(memory.updated_at(), &boundary);
        }
    }

    for year in [0, -1, 10_000] {
        let unsupported = timestamp_in_year(year);
        let mut created_at = valid_memory();
        created_at.created_at = unsupported;
        created_at.updated_at = unsupported;
        let created_at_sentinel = unsupported.to_rfc3339();
        let created_at_error = ValidatedMemoryVersion::try_from(created_at)
            .expect_err("unsupported created_at years should fail");
        assert_eq!(created_at_error.field(), "created_at");
        assert_eq!(created_at_error.code(), "unsupported_year");
        assert_error_is_opaque(&created_at_error, &[&created_at_sentinel]);

        let mut updated_at = valid_memory();
        updated_at.created_at = timestamp(18);
        updated_at.updated_at = unsupported;
        let updated_at_error = ValidatedMemoryVersion::try_from(updated_at)
            .expect_err("unsupported updated_at years should fail");
        assert_eq!(updated_at_error.field(), "updated_at");
        assert_eq!(updated_at_error.code(), "unsupported_year");
        assert_error_is_opaque(&updated_at_error, &[&created_at_sentinel]);
    }
}

#[test]
fn valid_embedding_preserves_its_independent_key_and_values() {
    let embedding =
        ValidatedEmbedding::try_from(valid_embedding()).expect("embedding should be valid");

    assert_eq!(embedding.id().as_str(), "memory-1");
    assert_eq!(embedding.version().get(), 7);
    assert_eq!(embedding.embedding().as_slice(), [1.25, -2.5]);
}

#[test]
fn embeddings_reject_empty_zero_and_non_finite_vectors() {
    assert_invalid_embedding(
        EmbeddingInput {
            embedding: Vec::new(),
            ..valid_embedding()
        },
        "empty",
    );
    assert_invalid_embedding(
        EmbeddingInput {
            embedding: vec![0.0],
            ..valid_embedding()
        },
        "zero_norm",
    );
    assert_invalid_embedding(
        EmbeddingInput {
            embedding: vec![-0.0],
            ..valid_embedding()
        },
        "zero_norm",
    );

    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_invalid_embedding(
            EmbeddingInput {
                embedding: vec![value],
                ..valid_embedding()
            },
            "non_finite",
        );
    }
}

#[test]
fn embeddings_reject_nonrepresentable_vectors_and_accept_the_physical_limit() {
    for vector in [
        vec![f64::MAX],
        vec![f64::MIN_POSITIVE],
        vec![1.000_000_000_000_000_2],
    ] {
        assert_invalid_embedding(
            EmbeddingInput {
                embedding: vector,
                ..valid_embedding()
            },
            "not_float4_exact",
        );
    }
    assert_invalid_embedding(
        EmbeddingInput {
            embedding: vec![1.0; 16_001],
            ..valid_embedding()
        },
        "too_many_components",
    );

    let embedding = ValidatedEmbedding::try_from(EmbeddingInput {
        embedding: vec![f64::from(f32::MAX); 16_000],
        ..valid_embedding()
    })
    .expect("16,000 float4-exact components should be valid");
    assert_eq!(embedding.embedding().as_slice().len(), 16_000);
    assert!(
        embedding
            .embedding()
            .as_slice()
            .iter()
            .all(|component| *component == f64::from(f32::MAX))
    );
}

#[test]
fn validation_errors_do_not_leak_protected_values() {
    const TITLE_SENTINEL: &str = "title-secret-sentinel";
    const CONTENT_SENTINEL: &str = "content-secret-sentinel";
    const COLLECTION_SENTINEL: &str = "collection-secret-sentinel";
    const VECTOR_SENTINEL: f64 = 987_654.25;

    let mut memory = valid_memory();
    memory.id.clear();
    memory.title = TITLE_SENTINEL.to_owned();
    memory.content = CONTENT_SENTINEL.to_owned();
    memory.created_at = timestamp(20);
    memory.updated_at = timestamp(21);
    memory.concepts = vec![COLLECTION_SENTINEL.to_owned()];
    memory.files = vec![COLLECTION_SENTINEL.to_owned()];
    memory.session_ids = vec![COLLECTION_SENTINEL.to_owned()];
    memory.source_observation_ids = vec![COLLECTION_SENTINEL.to_owned()];

    let memory_error =
        ValidatedMemoryVersion::try_from(memory).expect_err("memory should be rejected");
    assert_error_is_opaque(
        &memory_error,
        &[
            TITLE_SENTINEL,
            CONTENT_SENTINEL,
            COLLECTION_SENTINEL,
            &timestamp(20).to_rfc3339(),
            &timestamp(21).to_rfc3339(),
        ],
    );

    let embedding_error = ValidatedEmbedding::try_from(EmbeddingInput {
        id: String::new(),
        version: 7,
        embedding: vec![VECTOR_SENTINEL],
    })
    .expect_err("embedding should be rejected");
    let vector_sentinel = VECTOR_SENTINEL.to_string();
    assert_error_is_opaque(&embedding_error, &[&vector_sentinel]);
}

#[test]
fn embedding_vector_validation_errors_do_not_leak_finite_components() {
    const VECTOR_SENTINEL: f64 = 987_654.25;

    let error = ValidatedEmbedding::try_from(EmbeddingInput {
        id: "memory-1".to_owned(),
        version: 7,
        embedding: vec![VECTOR_SENTINEL, f64::NAN],
    })
    .expect_err("non-finite embedding should be rejected");

    assert_eq!(error.field(), "embedding");
    assert_eq!(error.code(), "non_finite");
    let vector_sentinel = VECTOR_SENTINEL.to_string();
    assert_error_is_opaque(&error, &[&vector_sentinel]);
}

fn assert_error_is_opaque<E: std::fmt::Debug + std::fmt::Display>(error: &E, sentinels: &[&str]) {
    let display = error.to_string();
    let debug = format!("{error:?}");

    for sentinel in sentinels {
        assert!(
            !display.contains(sentinel),
            "Display error leaked protected value: {display}"
        );
        assert!(
            !debug.contains(sentinel),
            "Debug error leaked protected value: {debug}"
        );
    }
}

#[test]
fn database_targets_require_a_name_and_preserve_logical_whitespace() {
    let target = DatabaseTarget::try_from("memory/search target?".to_owned())
        .expect("logical target names are opaque");
    assert_eq!(target.as_str(), "memory/search target?");

    let whitespace =
        DatabaseTarget::try_from(" \t".to_owned()).expect("whitespace is a nonempty target");
    assert_eq!(whitespace.as_str(), " \t");

    let error = DatabaseTarget::try_from(String::new()).expect_err("empty target should fail");
    assert_eq!(error.field(), "database_target");
    assert_eq!(error.code(), "empty");
}

#[test]
fn bm25_searches_validate_query_and_limit_without_query_interpretation() {
    let search = ValidatedBm25Search::try_from(Bm25Search {
        query: " title:quoted \"terms\" ".to_owned(),
        limit: 9,
    })
    .expect("nonempty lexical query with positive limit should be valid");
    assert_eq!(search.query(), " title:quoted \"terms\" ");
    assert_eq!(search.limit(), 9);

    let whitespace = ValidatedBm25Search::try_from(Bm25Search {
        query: "\t".to_owned(),
        ..valid_bm25_search()
    })
    .expect("query validation uses is_empty only");
    assert_eq!(whitespace.query(), "\t");

    assert_invalid_bm25(
        Bm25Search {
            query: String::new(),
            ..valid_bm25_search()
        },
        "query",
        "empty",
    );
    assert_invalid_bm25(
        Bm25Search {
            limit: 0,
            ..valid_bm25_search()
        },
        "limit",
        "not_positive",
    );
    assert_invalid_bm25(
        Bm25Search {
            query: String::new(),
            limit: 0,
        },
        "query",
        "empty",
    );

    const LEXICAL_SENTINEL: &str = "lexical-query-secret-sentinel";
    let error = ValidatedBm25Search::try_from(Bm25Search {
        query: LEXICAL_SENTINEL.to_owned(),
        limit: 0,
    })
    .expect_err("zero limit should fail without exposing the query");
    assert_error_is_opaque(&error, &[LEXICAL_SENTINEL]);
}

#[test]
fn vector_searches_validate_values_limits_and_physical_representability() {
    for vector in [vec![1.0], vec![3.0, 4.0], vec![1.0, 2.0, 3.0]] {
        let search = ValidatedVectorSearch::try_from(VectorSearch { vector, limit: 3 })
            .expect("finite nonzero vectors of any dimension should be valid");
        assert_eq!(search.limit(), 3);
        assert!(search.vector().iter().any(|value| *value != 0.0));
    }

    let unnormalized = ValidatedVectorSearch::try_from(VectorSearch {
        vector: vec![3.0, 4.0],
        ..valid_vector_search()
    })
    .expect("vector queries do not require normalization");
    assert_eq!(unnormalized.vector(), [3.0, 4.0]);

    assert_invalid_vector(
        VectorSearch {
            vector: Vec::new(),
            ..valid_vector_search()
        },
        "vector",
        "empty",
    );
    assert_invalid_vector(
        VectorSearch {
            vector: vec![0.0],
            ..valid_vector_search()
        },
        "vector",
        "zero_norm",
    );
    assert_invalid_vector(
        VectorSearch {
            vector: vec![-0.0],
            ..valid_vector_search()
        },
        "vector",
        "zero_norm",
    );
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_invalid_vector(
            VectorSearch {
                vector: vec![value],
                ..valid_vector_search()
            },
            "vector",
            "non_finite",
        );
    }
    for vector in [
        vec![f64::MAX],
        vec![f64::MIN_POSITIVE],
        vec![1.000_000_000_000_000_2],
    ] {
        assert_invalid_vector(
            VectorSearch {
                vector,
                ..valid_vector_search()
            },
            "vector",
            "not_float4_exact",
        );
    }
    assert_invalid_vector(
        VectorSearch {
            vector: vec![1.0; 16_001],
            ..valid_vector_search()
        },
        "vector",
        "too_many_components",
    );

    let maximum = ValidatedVectorSearch::try_from(VectorSearch {
        vector: vec![f64::from(f32::MAX); 16_000],
        ..valid_vector_search()
    })
    .expect("16,000 float4-exact vector components should be valid");
    assert_eq!(maximum.vector().len(), 16_000);
    assert!(
        maximum
            .vector()
            .iter()
            .all(|component| *component == f64::from(f32::MAX))
    );
    assert_invalid_vector(
        VectorSearch {
            limit: 0,
            ..valid_vector_search()
        },
        "limit",
        "not_positive",
    );
    assert_invalid_vector(
        VectorSearch {
            vector: Vec::new(),
            limit: 0,
        },
        "vector",
        "empty",
    );

    const VECTOR_SENTINEL: f64 = 987_654.25;
    let error = ValidatedVectorSearch::try_from(VectorSearch {
        vector: vec![VECTOR_SENTINEL, f64::NAN],
        limit: 1,
    })
    .expect_err("non-finite vectors should fail without exposing components");
    let vector_sentinel = VECTOR_SENTINEL.to_string();
    assert_error_is_opaque(&error, &[&vector_sentinel]);
}

#[test]
fn search_results_preserve_complete_memory_without_embedding_or_nonfinite_relevance() {
    let mut input = valid_memory();
    input.concepts.clear();
    input.files.clear();
    input.session_ids.clear();
    input.source_observation_ids.clear();
    let memory = ValidatedMemoryVersion::try_from(input).expect("memory should be valid");

    for relevance in [-1.5, 0.0, 2.75] {
        let result = MemorySearchResult::try_new(memory.clone(), relevance)
            .expect("finite relevance should be valid");
        assert_eq!(result.id().as_str(), "memory-1");
        assert_eq!(result.version().get(), 7);
        assert_eq!(result.memory_type(), "unrestricted/type");
        assert_eq!(result.title(), "canonical title");
        assert_eq!(result.content(), "canonical content");
        assert_eq!(result.created_at(), &timestamp(18));
        assert_eq!(result.updated_at(), &timestamp(19));
        assert!(result.concepts().is_empty());
        assert!(result.files().is_empty());
        assert!(result.session_ids().is_empty());
        assert!(result.source_observation_ids().is_empty());
        assert_eq!(result.relevance(), relevance);
    }

    let result = MemorySearchResult::try_new(memory.clone(), 2.75)
        .expect("finite relevance should be valid");
    assert_eq!(result.memory(), &memory);
    let serialized = serde_json::to_value(&result).expect("result should serialize");
    let object = serialized
        .as_object()
        .expect("serialized result should be an object");
    for field in [
        "id",
        "version",
        "memory_type",
        "title",
        "content",
        "created_at",
        "updated_at",
        "concepts",
        "files",
        "session_ids",
        "source_observation_ids",
        "relevance",
    ] {
        assert!(object.contains_key(field), "missing result field: {field}");
    }
    assert!(
        !object.contains_key("embedding"),
        "search result serialization must omit embeddings"
    );

    for relevance in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let error = MemorySearchResult::try_new(memory.clone(), relevance)
            .expect_err("non-finite relevance should fail");
        assert_eq!(error.field(), "relevance");
        assert_eq!(error.code(), "non_finite");
    }
}

#[test]
fn database_errors_are_typed_distinct_and_protected_value_safe() {
    const KEY_SENTINEL: &str = "missing-memory-key-secret-sentinel";
    let key = MemoryVersionKey::new(
        MemoryId::try_from(KEY_SENTINEL.to_owned()).expect("key ID should be valid"),
        MemoryVersion::try_from(47).expect("key version should be valid"),
    );
    let missing = DatabaseError::missing_memory_version(key);
    assert!(matches!(
        &missing,
        DatabaseError::MissingMemoryVersion { key }
            if key.id().as_str() == KEY_SENTINEL && key.version().get() == 47
    ));

    let memory_conflict = DatabaseError::conflict(DatabaseOperation::InsertMemory);
    let embedding_conflict = DatabaseError::conflict(DatabaseOperation::InsertEmbedding);
    assert_ne!(memory_conflict, embedding_conflict);
    assert!(matches!(
        &memory_conflict,
        DatabaseError::Conflict {
            operation: DatabaseOperation::InsertMemory
        }
    ));
    assert!(matches!(
        &embedding_conflict,
        DatabaseError::Conflict {
            operation: DatabaseOperation::InsertEmbedding
        }
    ));

    let database_failure = DatabaseError::database_failure(DatabaseOperation::SearchBm25);
    assert!(matches!(
        &database_failure,
        DatabaseError::DatabaseFailure {
            operation: DatabaseOperation::SearchBm25
        }
    ));
    let invalid_response =
        DatabaseError::invalid_response(DatabaseOperation::SearchVector, "relevance");
    assert!(matches!(
        &invalid_response,
        DatabaseError::InvalidResponse {
            operation: DatabaseOperation::SearchVector,
            column: "relevance"
        }
    ));
    assert!(invalid_response.to_string().contains("search_vector"));
    assert!(invalid_response.to_string().contains("relevance"));

    let validation_error = ValidatedBm25Search::try_from(Bm25Search {
        query: String::new(),
        limit: 1,
    })
    .expect_err("empty query should be invalid");
    let validation = MemoryStoreError::from(validation_error);
    assert!(matches!(validation, MemoryStoreError::InvalidInput(_)));
    let database = MemoryStoreError::from(missing.clone());
    assert!(matches!(database, MemoryStoreError::Database(_)));

    for error in [
        &missing,
        &memory_conflict,
        &embedding_conflict,
        &database_failure,
        &invalid_response,
    ] {
        assert_error_is_opaque(error, &[KEY_SENTINEL]);
    }
    assert_error_is_opaque(&database, &[KEY_SENTINEL]);
}
