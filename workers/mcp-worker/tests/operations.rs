use chrono::{DateTime, TimeZone, Utc};
use mcp_worker::operations::{
    BackendResponseField, BackendSource, FailingOperations, MemoryOperationFailures,
    MemoryOperationResponses, MemoryOperations, MemoryVersionSummary, Operation, OperationCall,
    OperationError, OperationInputField, RecordingOperations, VersionPageQuery,
};
use memory_store::contracts::{
    Bm25Search, MemoryId, MemorySearchResult, MemoryVersion, MemoryVersionInput, VectorSearch,
};

const DIAGNOSTIC_SENTINELS: &[&str] = &[
    "title-secret-sentinel",
    "content-secret-sentinel",
    "query-secret-sentinel",
    "id-secret-sentinel",
    "backend-secret-sentinel",
];

#[tokio::test]
async fn recording_operations_return_configured_results_and_share_typed_calls() {
    let inserted = memory("inserted", "inserted title", "inserted content", 1);
    let lexical_query = Bm25Search {
        query: "lexical query".to_owned(),
        limit: 2,
    };
    let vector_query = VectorSearch {
        vector: vec![1.0, -2.0],
        limit: 2,
    };
    let latest_id = memory_id("latest-id");
    let exact_id = memory_id("exact-id");
    let exact_version = memory_version(3);
    let page_query = VersionPageQuery {
        id: memory_id("versions-id"),
        offset: 4,
        limit: 2,
    };
    let lexical_results = vec![search_result("lexical-result", 2)];
    let vector_results = vec![search_result("vector-result", 3)];
    let latest = memory("latest-id", "latest title", "latest content", 4);
    let exact = memory("exact-id", "exact title", "exact content", 3);
    let versions = vec![
        MemoryVersionSummary {
            version: memory_version(5),
            updated_at: timestamp(5),
        },
        MemoryVersionSummary {
            version: memory_version(4),
            updated_at: timestamp(4),
        },
    ];
    let operations = RecordingOperations::new(MemoryOperationResponses {
        insert_memory: Ok(()),
        search_lexical: Ok(lexical_results.clone()),
        search_vector: Ok(vector_results.clone()),
        get_latest: Ok(latest.clone()),
        get_exact: Ok(exact.clone()),
        list_versions: Ok(versions.clone()),
    });
    let observer = operations.clone();

    operations
        .insert_memory(inserted.clone())
        .await
        .expect("configured insert result should succeed");
    assert_eq!(
        operations
            .search_lexical(lexical_query.clone())
            .await
            .expect("configured lexical result should succeed"),
        lexical_results
    );
    assert_eq!(
        operations
            .search_vector(vector_query.clone())
            .await
            .expect("configured vector result should succeed"),
        vector_results
    );
    assert_eq!(
        operations
            .get_latest(latest_id.clone())
            .await
            .expect("configured latest result should succeed"),
        latest
    );
    assert_eq!(
        operations
            .get_exact(exact_id.clone(), exact_version)
            .await
            .expect("configured exact result should succeed"),
        exact
    );
    assert_eq!(
        operations
            .list_versions(page_query.clone())
            .await
            .expect("configured version results should succeed"),
        versions
    );

    assert_eq!(
        observer.calls(),
        vec![
            OperationCall::InsertMemory(inserted),
            OperationCall::SearchLexical(lexical_query),
            OperationCall::SearchVector(vector_query),
            OperationCall::GetLatest(latest_id),
            OperationCall::GetExact {
                id: exact_id,
                version: exact_version,
            },
            OperationCall::ListVersions(page_query),
        ]
    );
}

#[tokio::test]
async fn failing_operations_classify_errors_without_leaking_protected_values() {
    const TITLE_SENTINEL: &str = "title-secret-sentinel";
    const CONTENT_SENTINEL: &str = "content-secret-sentinel";
    const QUERY_SENTINEL: &str = "query-secret-sentinel";
    const VECTOR_SENTINEL: f64 = 987_654.25;
    const ID_SENTINEL: &str = "id-secret-sentinel";

    let insert_error = OperationError::conflict(Operation::InsertMemory);
    let lexical_error =
        OperationError::backend_failure(Operation::SearchLexical, BackendSource::MemoryStore);
    let vector_error = OperationError::invalid_backend_response(
        Operation::SearchVector,
        BackendResponseField::SearchResult,
    );
    let latest_error = OperationError::not_found(Operation::GetLatest);
    let exact_error =
        OperationError::invalid_input(Operation::GetExact, OperationInputField::Version);
    let versions_error =
        OperationError::backend_failure(Operation::ListVersions, BackendSource::Database);
    let operations = FailingOperations::new(MemoryOperationFailures {
        insert_memory: insert_error.clone(),
        search_lexical: lexical_error.clone(),
        search_vector: vector_error.clone(),
        get_latest: latest_error.clone(),
        get_exact: exact_error.clone(),
        list_versions: versions_error.clone(),
    });
    let observer = operations.clone();
    let inserted = memory("insert-id", TITLE_SENTINEL, CONTENT_SENTINEL, 1);
    let lexical_query = Bm25Search {
        query: QUERY_SENTINEL.to_owned(),
        limit: 1,
    };
    let vector_query = VectorSearch {
        vector: vec![VECTOR_SENTINEL],
        limit: 1,
    };
    let latest_id = memory_id(ID_SENTINEL);
    let exact_id = memory_id("exact-secret-sentinel");
    let exact_version = memory_version(7);
    let page_query = VersionPageQuery {
        id: memory_id("versions-secret-sentinel"),
        offset: 8,
        limit: 3,
    };

    assert_eq!(
        operations
            .insert_memory(inserted.clone())
            .await
            .expect_err("configured insert failure should be returned"),
        OperationError::Conflict {
            operation: Operation::InsertMemory,
        }
    );
    assert_eq!(
        operations
            .search_lexical(lexical_query.clone())
            .await
            .expect_err("configured lexical failure should be returned"),
        OperationError::BackendFailure {
            operation: Operation::SearchLexical,
            source: BackendSource::MemoryStore,
        }
    );
    assert_eq!(
        operations
            .search_vector(vector_query.clone())
            .await
            .expect_err("configured vector failure should be returned"),
        OperationError::InvalidBackendResponse {
            operation: Operation::SearchVector,
            field: BackendResponseField::SearchResult,
        }
    );
    assert_eq!(
        operations
            .get_latest(latest_id.clone())
            .await
            .expect_err("configured latest failure should be returned"),
        OperationError::NotFound {
            operation: Operation::GetLatest,
        }
    );
    assert_eq!(
        operations
            .get_exact(exact_id.clone(), exact_version)
            .await
            .expect_err("configured exact failure should be returned"),
        OperationError::InvalidInput {
            operation: Operation::GetExact,
            field: OperationInputField::Version,
        }
    );
    assert_eq!(
        operations
            .list_versions(page_query.clone())
            .await
            .expect_err("configured version failure should be returned"),
        OperationError::BackendFailure {
            operation: Operation::ListVersions,
            source: BackendSource::Database,
        }
    );

    for error in [
        insert_error,
        lexical_error,
        vector_error,
        latest_error,
        exact_error,
        versions_error,
    ] {
        assert_error_is_opaque(
            &error,
            &[
                TITLE_SENTINEL,
                CONTENT_SENTINEL,
                QUERY_SENTINEL,
                "987654.25",
                ID_SENTINEL,
                "exact-secret-sentinel",
                "versions-secret-sentinel",
            ],
        );
    }

    assert_eq!(
        observer.calls(),
        vec![
            OperationCall::InsertMemory(inserted),
            OperationCall::SearchLexical(lexical_query),
            OperationCall::SearchVector(vector_query),
            OperationCall::GetLatest(latest_id),
            OperationCall::GetExact {
                id: exact_id,
                version: exact_version,
            },
            OperationCall::ListVersions(page_query),
        ]
    );
}

#[test]
fn operation_labels_are_fixed_and_version_summaries_are_narrow() {
    let summary = MemoryVersionSummary {
        version: memory_version(9),
        updated_at: timestamp(9),
    };
    let MemoryVersionSummary {
        version,
        updated_at,
    } = summary;

    assert_eq!(version, memory_version(9));
    assert_eq!(updated_at, timestamp(9));
    for (operation, label, debug_label) in [
        (Operation::InsertMemory, "insert_memory", "InsertMemory"),
        (Operation::SearchLexical, "search_lexical", "SearchLexical"),
        (Operation::SearchVector, "search_vector", "SearchVector"),
        (Operation::GetLatest, "get_latest", "GetLatest"),
        (Operation::GetExact, "get_exact", "GetExact"),
        (Operation::ListVersions, "list_versions", "ListVersions"),
    ] {
        assert_fixed_label(operation, label, debug_label);
    }
}

#[test]
fn operation_error_categories_have_fixed_content_safe_rendering() {
    for (field, label, debug_label) in [
        (OperationInputField::Memory, "memory", "Memory"),
        (OperationInputField::Title, "title", "Title"),
        (OperationInputField::Content, "content", "Content"),
        (OperationInputField::SessionId, "session_id", "SessionId"),
        (OperationInputField::Query, "query", "Query"),
        (OperationInputField::Vector, "vector", "Vector"),
        (OperationInputField::Id, "id", "Id"),
        (OperationInputField::Version, "version", "Version"),
        (OperationInputField::Offset, "offset", "Offset"),
        (OperationInputField::Limit, "limit", "Limit"),
    ] {
        assert_fixed_label(field, label, debug_label);
        assert_error_rendering(
            &OperationError::invalid_input(Operation::InsertMemory, field),
            &format!("invalid input: insert_memory ({label})"),
            &format!("InvalidInput {{ operation: InsertMemory, field: {debug_label} }}"),
        );
    }

    for (field, label, debug_label) in [
        (
            BackendResponseField::InsertConfirmation,
            "insert_confirmation",
            "InsertConfirmation",
        ),
        (
            BackendResponseField::SearchResult,
            "search_result",
            "SearchResult",
        ),
        (BackendResponseField::Memory, "memory", "Memory"),
        (
            BackendResponseField::VersionSummary,
            "version_summary",
            "VersionSummary",
        ),
    ] {
        assert_fixed_label(field, label, debug_label);
        assert_error_rendering(
            &OperationError::invalid_backend_response(Operation::GetLatest, field),
            &format!("invalid backend response: get_latest ({label})"),
            &format!("InvalidBackendResponse {{ operation: GetLatest, field: {debug_label} }}"),
        );
    }

    for (source, label, debug_label) in [
        (BackendSource::MemoryStore, "memory_store", "MemoryStore"),
        (BackendSource::Database, "database", "Database"),
    ] {
        assert_fixed_label(source, label, debug_label);
        assert_error_rendering(
            &OperationError::backend_failure(Operation::SearchLexical, source),
            &format!("backend failure: search_lexical ({label})"),
            &format!("BackendFailure {{ operation: SearchLexical, source: {debug_label} }}"),
        );
    }

    for (error, display, debug) in [
        (
            OperationError::not_found(Operation::GetExact),
            "not found: get_exact",
            "NotFound { operation: GetExact }",
        ),
        (
            OperationError::conflict(Operation::InsertMemory),
            "conflict: insert_memory",
            "Conflict { operation: InsertMemory }",
        ),
    ] {
        assert_error_rendering(&error, display, debug);
    }
}

fn memory(id: &str, title: &str, content: &str, version: i64) -> MemoryVersionInput {
    MemoryVersionInput {
        id: id.to_owned(),
        version,
        memory_type: "unclassified".to_owned(),
        title: title.to_owned(),
        content: content.to_owned(),
        created_at: timestamp(1),
        updated_at: timestamp(2),
        concepts: vec!["concept".to_owned()],
        files: vec!["file".to_owned()],
        session_ids: vec!["session".to_owned()],
        source_observation_ids: vec!["observation".to_owned()],
    }
}

fn memory_id(value: &str) -> MemoryId {
    MemoryId::try_from(value.to_owned()).expect("test memory ID should be valid")
}

fn memory_version(value: i64) -> MemoryVersion {
    MemoryVersion::try_from(value).expect("test memory version should be positive")
}

fn search_result(id: &str, version: i64) -> MemorySearchResult {
    MemorySearchResult::try_new(
        memory(id, "result title", "result content", version)
            .try_into()
            .expect("test memory should be valid"),
        0.75,
    )
    .expect("test relevance should be valid")
}

fn timestamp(day: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, day, 12, 0, 0)
        .single()
        .expect("test timestamp should be valid")
}

fn assert_fixed_label(
    value: impl std::fmt::Display + std::fmt::Debug,
    display_label: &str,
    debug_label: &str,
) {
    let display = value.to_string();
    let debug = format!("{value:?}");

    assert_eq!(display, display_label);
    assert_eq!(debug, debug_label);
    for sentinel in DIAGNOSTIC_SENTINELS {
        assert!(
            !display.contains(sentinel),
            "Display label leaked protected value: {display}"
        );
        assert!(
            !debug.contains(sentinel),
            "Debug label leaked protected value: {debug}"
        );
    }
}

fn assert_error_rendering(error: &OperationError, display: &str, debug: &str) {
    assert_eq!(error.to_string(), display);
    assert_eq!(format!("{error:?}"), debug);
    assert_error_is_opaque(error, DIAGNOSTIC_SENTINELS);
}

fn assert_error_is_opaque(error: &OperationError, sentinels: &[&str]) {
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
