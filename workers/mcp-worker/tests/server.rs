use chrono::{DateTime, Utc};
use mcp_worker::{
    contracts::{MemoryDto, tool_registry},
    embedding::{QueryEmbeddingError, RecordingQueryEmbeddingGenerator},
    operations::{
        BackendResponseField, BackendSource, MemoryOperationResponses, MemoryVersionSummary,
        Operation, OperationCall, OperationError, RecordingOperations, VersionPageQuery,
    },
    server::{DispatchResponse, McpServer},
    service::{CreateContext, MemoryToolService},
};
use memory_store::contracts::{
    Bm25Search, EmbeddingVector, MemoryId, MemorySearchResult, MemoryVersion, MemoryVersionInput,
    VectorSearch,
};
use rust_mcp_schema::schema_utils::RpcErrorCodes;
use rust_mcp_schema::{
    CallToolRequest, CallToolRequestParams, ClientCapabilities, DiscoverRequest,
    DiscoverResultCacheScope, JsonrpcErrorResponse, JsonrpcRequest, ListToolsRequest,
    ListToolsResultCacheScope, PaginatedRequestParams, RequestId, RequestMetaObject, RequestParams,
};
use serde_json::{Value, json};

const SERVER_NAME: &str = "total-recall-mcp";
const PROTOCOL_VERSION: &str = "2026-07-28";
const SAVE_TITLE: &str = "save-title-private-sentinel";
const SAVE_CONTENT: &str = "save-content-private-sentinel";
const SAVE_SESSION: &str = "save-session-private-sentinel";
const SEARCH_QUERY: &str = "search-query-private-sentinel";
const SEARCH_LIMIT: u32 = 3;
const LEGACY_CALLER_VECTOR: &[f64] = &[456_789.75, -654_321.5];
const LEGACY_CALLER_VECTOR_TEXT: &[&str] = &["456789.75", "654321.5"];
const LATEST_ID: &str = "latest-id-private-sentinel";
const EXACT_ID: &str = "exact-id-private-sentinel";
const EXACT_VERSION: i64 = 7;
const VERSION_LIST_ID: &str = "version-list-id-private-sentinel";
const GENERATED_VECTOR: &[f64] = &[987_654.25, -123_456.5];
const GENERATED_VECTOR_TEXT: &[&str] = &["987654.25", "123456.5"];

#[test]
fn discover_advertises_only_tools_with_stateless_identity() {
    let operations = RecordingOperations::new(unused_responses());
    let observer = operations.clone();
    let server = server(operations, None);
    let request_id = RequestId::String("discover-request-id".to_owned());

    let response = server.discover(DiscoverRequest::new(
        request_id.clone(),
        RequestParams::default(),
    ));

    assert_eq!(response.id, request_id);
    assert_eq!(
        response.result.cache_scope,
        DiscoverResultCacheScope::Public
    );
    assert_eq!(response.result.result_type, "complete");
    assert_eq!(
        response.result.supported_versions,
        vec![PROTOCOL_VERSION.to_owned()]
    );
    assert_only_tools_capability(&response.result.capabilities);

    let wire = serde_json::to_value(response).expect("discovery response should serialize");
    assert_eq!(wire["jsonrpc"], "2.0");
    assert_eq!(wire["id"], "discover-request-id");
    assert_eq!(wire["result"]["cacheScope"], "public");
    assert_eq!(wire["result"]["capabilities"], json!({ "tools": {} }));
    assert_eq!(wire["result"]["resultType"], "complete");
    assert_eq!(
        wire["result"]["supportedVersions"],
        json!([PROTOCOL_VERSION])
    );
    assert_eq!(wire["result"]["ttlMs"], 0);
    assert!(
        !wire["result"]
            .as_object()
            .expect("result should be an object")
            .contains_key("instructions")
    );
    assert_identity(&wire["result"]);
    assert!(observer.calls().is_empty());
}

#[test]
fn list_tools_returns_the_complete_strict_contract_registry_without_a_cursor() {
    let operations = RecordingOperations::new(unused_responses());
    let observer = operations.clone();
    let server = server(operations, None);
    let request_id = RequestId::Integer(42);

    let response = server.list_tools(ListToolsRequest::new(
        request_id.clone(),
        PaginatedRequestParams::default(),
    ));

    assert_eq!(response.id, request_id);
    assert_eq!(
        response.result.cache_scope,
        ListToolsResultCacheScope::Public
    );
    assert_eq!(response.result.result_type, "complete");
    assert!(response.result.next_cursor.is_none());

    let wire = serde_json::to_value(response).expect("tools response should serialize");
    let result = wire
        .get("result")
        .expect("response should contain a result");
    assert_eq!(wire["jsonrpc"], "2.0");
    assert_eq!(wire["id"], 42);
    assert_eq!(result["cacheScope"], "public");
    assert_eq!(result["resultType"], "complete");
    assert_eq!(result["ttlMs"], 0);
    assert!(
        !result
            .as_object()
            .expect("result should be an object")
            .contains_key("nextCursor")
    );
    assert_identity(result);
    assert_eq!(
        result["tools"],
        serde_json::to_value(tool_registry()).unwrap()
    );
    assert_eq!(
        result["tools"]
            .as_array()
            .expect("tools should be an array")
            .iter()
            .map(|tool| tool["name"].as_str().expect("tool names should be strings"))
            .collect::<Vec<_>>(),
        [
            "memory_save",
            "memory_search",
            "memory_get_latest",
            "memory_get_exact",
            "memory_list_versions",
        ]
    );
    assert_eq!(
        result["tools"][1]["inputSchema"],
        json!({
            "type": "object",
            "properties": {
                "query": { "type": "string" },
                "limit": { "type": "integer", "minimum": 1, "maximum": 50 },
            },
            "required": ["query", "limit"],
            "additionalProperties": false,
        })
    );
    assert!(observer.calls().is_empty());
}

#[tokio::test]
async fn memory_save_calls_the_service_once_and_serializes_the_advertised_result() {
    for generator in generator_compositions() {
        let generator_observer = generator.clone();
        let mut responses = unused_responses();
        responses.insert_memory = Ok(());
        let operations = RecordingOperations::new(responses);
        let observer = operations.clone();
        let server = server(operations, generator);
        let expected = saved_memory();

        let response = server
            .call_tool(call_tool_request(
                "save-request",
                "memory_save",
                Some(json!({
                    "title": SAVE_TITLE,
                    "content": SAVE_CONTENT,
                    "session_id": SAVE_SESSION,
                })),
            ))
            .await;

        assert_success_response(
            &response,
            "save-request",
            "memory_save",
            json!({ "memory": memory_json(&expected) }),
        );
        assert_eq!(
            observer.calls(),
            vec![OperationCall::InsertMemory(expected)]
        );
        assert_generator_not_invoked(&generator_observer);
    }
}

#[tokio::test]
async fn memory_search_returns_bm25_results_alone_when_query_embedding_is_disabled() {
    let lexical_b = canonical_memory("search-memory-b", 2, "lexical-b");
    let lexical_a = canonical_memory("search-memory-a", 1, "lexical-a");
    let unrequested_vector = canonical_memory("search-memory-0", 9, "unrequested-vector");
    let mut responses = unused_responses();
    responses.search_lexical = Ok(vec![search_result(&lexical_b), search_result(&lexical_a)]);
    responses.search_vector = Ok(vec![search_result(&unrequested_vector)]);
    let operations = RecordingOperations::new(responses);
    let observer = operations.clone();
    let server = server(operations, None);

    let response = server
        .call_tool(call_tool_request(
            "search-request",
            "memory_search",
            Some(search_arguments()),
        ))
        .await;

    assert_success_response(
        &response,
        "search-request",
        "memory_search",
        json!({ "results": [memory_json(&lexical_a), memory_json(&lexical_b)] }),
    );
    assert_eq!(observer.calls(), vec![lexical_search_call()]);
    assert_response_excludes(&response, &[SEARCH_QUERY]);
}

#[tokio::test]
async fn memory_search_merges_bm25_and_generated_vector_results_when_query_embedding_is_enabled() {
    let generator = enabled_generator();
    let generator_observer = generator.clone();
    let lexical_b = canonical_memory("search-memory-b", 1, "lexical-b");
    let lexical_c = canonical_memory("search-memory-c", 2, "lexical-c");
    let vector_a = canonical_memory("search-memory-a", 1, "vector-a");
    let vector_b = canonical_memory("search-memory-b", 3, "vector-b");
    let mut responses = unused_responses();
    responses.search_lexical = Ok(vec![search_result(&lexical_c), search_result(&lexical_b)]);
    responses.search_vector = Ok(vec![search_result(&vector_b), search_result(&vector_a)]);
    let operations = RecordingOperations::new(responses);
    let observer = operations.clone();
    let server = server(operations, Some(generator));

    let response = server
        .call_tool(call_tool_request(
            "search-request",
            "memory_search",
            Some(search_arguments()),
        ))
        .await;

    assert_success_response(
        &response,
        "search-request",
        "memory_search",
        json!({
            "results": [
                memory_json(&vector_a),
                memory_json(&vector_b),
                memory_json(&lexical_c),
            ],
        }),
    );
    assert_eq!(
        observer.calls(),
        vec![lexical_search_call(), generated_vector_search_call()]
    );
    assert_eq!(generator_observer.calls(), vec![SEARCH_QUERY.to_owned()]);
    assert_response_excludes(&response, &[SEARCH_QUERY]);
    assert_response_excludes(&response, GENERATED_VECTOR_TEXT);
}

#[tokio::test]
async fn memory_search_falls_back_to_bm25_results_after_each_generation_failure() {
    for failure in [
        QueryEmbeddingError::Unavailable,
        QueryEmbeddingError::InvalidResponse,
    ] {
        let generator = RecordingQueryEmbeddingGenerator::new(Err(failure));
        let generator_observer = generator.clone();
        let lexical_b = canonical_memory("search-memory-b", 2, "lexical-b");
        let lexical_a = canonical_memory("search-memory-a", 1, "lexical-a");
        let unrequested_vector = canonical_memory("search-memory-0", 9, "unrequested-vector");
        let mut responses = unused_responses();
        responses.search_lexical = Ok(vec![search_result(&lexical_b), search_result(&lexical_a)]);
        responses.search_vector = Ok(vec![search_result(&unrequested_vector)]);
        let operations = RecordingOperations::new(responses);
        let observer = operations.clone();
        let server = server(operations, Some(generator));

        let response = server
            .call_tool(call_tool_request(
                "search-request",
                "memory_search",
                Some(search_arguments()),
            ))
            .await;

        assert_success_response(
            &response,
            "search-request",
            "memory_search",
            json!({ "results": [memory_json(&lexical_a), memory_json(&lexical_b)] }),
        );
        assert_eq!(observer.calls(), vec![lexical_search_call()]);
        assert_eq!(generator_observer.calls(), vec![SEARCH_QUERY.to_owned()]);
        assert_response_excludes(&response, &[SEARCH_QUERY, &failure.to_string()]);
    }
}

#[tokio::test]
async fn memory_search_rejects_invalid_queries_and_limits_before_generation_or_search() {
    for generator in generator_compositions() {
        for arguments in [
            json!({ "query": "", "limit": SEARCH_LIMIT }),
            json!({ "query": format!("\0{SEARCH_QUERY}"), "limit": SEARCH_LIMIT }),
            json!({ "query": "search-query\0-private-sentinel", "limit": SEARCH_LIMIT }),
            json!({ "query": format!("{SEARCH_QUERY}\0"), "limit": SEARCH_LIMIT }),
            json!({ "query": SEARCH_QUERY, "limit": 0 }),
            json!({ "query": SEARCH_QUERY, "limit": 51 }),
        ] {
            let operations = RecordingOperations::new(unused_responses());
            let observer = operations.clone();
            let server = server(operations, generator.clone());

            let response = server
                .call_tool(call_tool_request(
                    "invalid-search",
                    "memory_search",
                    Some(arguments),
                ))
                .await;

            assert_tool_error(
                &response,
                "invalid_input",
                &[SEARCH_QUERY, "search-query", "private-sentinel"],
            );
            assert!(observer.calls().is_empty());
        }
        assert_generator_not_invoked(&generator);
    }
}

#[tokio::test]
async fn memory_search_failures_return_distinct_content_safe_errors_without_partial_results() {
    let lexical = canonical_memory("search-lexical-partial", 1, "lexical-partial-sentinel");
    let vector = canonical_memory("search-vector-partial", 1, "vector-partial-sentinel");
    for (case, generator, lexical_response, vector_response, expected_code, expected_calls) in [
        (
            "BM25 backend failure without embedding",
            None,
            Err(OperationError::backend_failure(
                Operation::SearchLexical,
                BackendSource::MemoryStore,
            )),
            Ok(vec![search_result(&vector)]),
            "backend_failure",
            vec![lexical_search_call()],
        ),
        (
            "BM25 backend failure after generation",
            Some(enabled_generator()),
            Err(OperationError::backend_failure(
                Operation::SearchLexical,
                BackendSource::MemoryStore,
            )),
            Ok(vec![search_result(&vector)]),
            "backend_failure",
            vec![lexical_search_call(), generated_vector_search_call()],
        ),
        (
            "BM25 backend failure after generation failure",
            Some(RecordingQueryEmbeddingGenerator::new(Err(
                QueryEmbeddingError::Unavailable,
            ))),
            Err(OperationError::backend_failure(
                Operation::SearchLexical,
                BackendSource::MemoryStore,
            )),
            Ok(vec![search_result(&vector)]),
            "backend_failure",
            vec![lexical_search_call()],
        ),
        (
            "BM25 invalid response",
            Some(enabled_generator()),
            Err(OperationError::invalid_backend_response(
                Operation::SearchLexical,
                BackendResponseField::SearchResult,
            )),
            Ok(vec![search_result(&vector)]),
            "internal_failure",
            vec![lexical_search_call(), generated_vector_search_call()],
        ),
        (
            "vector invalid response after generation",
            Some(enabled_generator()),
            Ok(vec![search_result(&lexical)]),
            Err(OperationError::invalid_backend_response(
                Operation::SearchVector,
                BackendResponseField::SearchResult,
            )),
            "internal_failure",
            vec![lexical_search_call(), generated_vector_search_call()],
        ),
        (
            "vector backend failure after generation",
            Some(enabled_generator()),
            Ok(vec![search_result(&lexical)]),
            Err(OperationError::backend_failure(
                Operation::SearchVector,
                BackendSource::MemoryStore,
            )),
            "backend_failure",
            vec![lexical_search_call(), generated_vector_search_call()],
        ),
    ] {
        let generator_observer = generator.clone();
        let mut responses = unused_responses();
        responses.search_lexical = lexical_response;
        responses.search_vector = vector_response;
        let operations = RecordingOperations::new(responses);
        let observer = operations.clone();
        let server = server(operations, generator);

        let response = server
            .call_tool(call_tool_request(
                "search-failure",
                "memory_search",
                Some(search_arguments()),
            ))
            .await;

        assert_tool_error(
            &response,
            expected_code,
            &[
                SEARCH_QUERY,
                "search-lexical-partial",
                "lexical-partial-sentinel",
                "search-vector-partial",
                "vector-partial-sentinel",
            ],
        );
        assert_response_excludes(&response, GENERATED_VECTOR_TEXT);
        assert_eq!(observer.calls(), expected_calls, "{case}");
        if let Some(generator) = generator_observer {
            assert_eq!(generator.calls(), vec![SEARCH_QUERY.to_owned()], "{case}");
        }
    }
}

#[tokio::test]
async fn memory_get_latest_calls_the_service_once_and_serializes_the_advertised_result() {
    for generator in generator_compositions() {
        let generator_observer = generator.clone();
        let expected = canonical_memory(LATEST_ID, 3, "latest");
        let mut responses = unused_responses();
        responses.get_latest = Ok(expected.clone());
        let operations = RecordingOperations::new(responses);
        let observer = operations.clone();
        let server = server(operations, generator);

        let response = server
            .call_tool(call_tool_request(
                "latest-request",
                "memory_get_latest",
                Some(json!({ "id": LATEST_ID })),
            ))
            .await;

        assert_success_response(
            &response,
            "latest-request",
            "memory_get_latest",
            json!({ "memory": memory_json(&expected) }),
        );
        assert_eq!(
            observer.calls(),
            vec![OperationCall::GetLatest(
                MemoryId::try_from(LATEST_ID.to_owned()).expect("test latest ID should be valid"),
            )]
        );
        assert_generator_not_invoked(&generator_observer);
    }
}

#[tokio::test]
async fn memory_get_exact_calls_the_service_once_and_serializes_the_advertised_result() {
    for generator in generator_compositions() {
        let generator_observer = generator.clone();
        let expected = canonical_memory(EXACT_ID, EXACT_VERSION, "exact");
        let mut responses = unused_responses();
        responses.get_exact = Ok(expected.clone());
        let operations = RecordingOperations::new(responses);
        let observer = operations.clone();
        let server = server(operations, generator);

        let response = server
            .call_tool(call_tool_request(
                "exact-request",
                "memory_get_exact",
                Some(json!({ "id": EXACT_ID, "version": EXACT_VERSION })),
            ))
            .await;

        assert_success_response(
            &response,
            "exact-request",
            "memory_get_exact",
            json!({ "memory": memory_json(&expected) }),
        );
        assert_eq!(
            observer.calls(),
            vec![OperationCall::GetExact {
                id: MemoryId::try_from(EXACT_ID.to_owned()).expect("test exact ID should be valid"),
                version: MemoryVersion::try_from(EXACT_VERSION)
                    .expect("test exact version should be valid"),
            }]
        );
        assert_generator_not_invoked(&generator_observer);
    }
}

#[tokio::test]
async fn memory_list_versions_calls_the_service_once_and_serializes_the_advertised_result() {
    for generator in generator_compositions() {
        let generator_observer = generator.clone();
        let mut responses = unused_responses();
        responses.list_versions = Ok(vec![
            version_summary_with_microseconds(4),
            version_summary(3),
        ]);
        let operations = RecordingOperations::new(responses);
        let observer = operations.clone();
        let server = server(operations, generator);

        let response = server
            .call_tool(call_tool_request(
                "versions-request",
                "memory_list_versions",
                Some(json!({ "id": VERSION_LIST_ID, "offset": 3, "limit": 2 })),
            ))
            .await;

        assert_success_response(
            &response,
            "versions-request",
            "memory_list_versions",
            json!({
                "versions": [
                    { "version": 4, "updated_at": "2026-09-20T00:00:00.123456Z" },
                    { "version": 3, "updated_at": timestamp() },
                ],
                "next_offset": null,
            }),
        );
        assert_eq!(
            observer.calls(),
            vec![OperationCall::ListVersions(VersionPageQuery {
                id: MemoryId::try_from(VERSION_LIST_ID.to_owned())
                    .expect("test version-list ID should be valid"),
                offset: 3,
                limit: 3,
            })]
        );
        assert_generator_not_invoked(&generator_observer);
    }
}

#[tokio::test]
async fn strict_arguments_and_unknown_tools_return_invalid_input_without_operations() {
    for generator in generator_compositions() {
        assert_strict_arguments_are_rejected_without_operations(generator).await;
    }
}

async fn assert_strict_arguments_are_rejected_without_operations(
    generator: Option<RecordingQueryEmbeddingGenerator>,
) {
    for (tool, arguments, protected_value) in [
        (
            "memory_save",
            Some(json!({ "title": SAVE_TITLE, "session_id": SAVE_SESSION })),
            SAVE_TITLE,
        ),
        (
            "memory_save",
            Some(json!({
                "title": 17,
                "content": SAVE_CONTENT,
                "session_id": SAVE_SESSION,
            })),
            "17",
        ),
        (
            "memory_save",
            Some(json!({
                "title": SAVE_TITLE,
                "content": SAVE_CONTENT,
                "session_id": SAVE_SESSION,
                "extra": "unknown-save-private-sentinel",
            })),
            "unknown-save-private-sentinel",
        ),
        (
            "memory_search",
            Some(json!({
                "query": SEARCH_QUERY,
                "vector": LEGACY_CALLER_VECTOR,
                "limit": SEARCH_LIMIT,
            })),
            SEARCH_QUERY,
        ),
        (
            "memory_search",
            Some(json!({
                "query": SEARCH_QUERY,
                "vector": [],
                "limit": SEARCH_LIMIT,
            })),
            SEARCH_QUERY,
        ),
        (
            "memory_search",
            Some(json!({
                "query": SEARCH_QUERY,
                "vector": "private-vector",
                "limit": SEARCH_LIMIT,
            })),
            "private-vector",
        ),
        (
            "memory_search",
            Some(json!({
                "query": SEARCH_QUERY,
                "limit": SEARCH_LIMIT,
                "extra": "unknown-search-private-sentinel",
            })),
            "unknown-search-private-sentinel",
        ),
        (
            "memory_search",
            Some(json!({ "limit": SEARCH_LIMIT })),
            SEARCH_QUERY,
        ),
        (
            "memory_search",
            Some(json!({ "query": SEARCH_QUERY })),
            SEARCH_QUERY,
        ),
        (
            "memory_search",
            Some(json!({ "query": 17, "limit": SEARCH_LIMIT })),
            "17",
        ),
        (
            "memory_search",
            Some(json!({ "query": SEARCH_QUERY, "limit": "private-limit" })),
            "private-limit",
        ),
        (
            "memory_search",
            Some(json!({ "query": SEARCH_QUERY, "limit": -3 })),
            "-3",
        ),
        ("memory_get_latest", Some(json!({})), LATEST_ID),
        ("memory_get_latest", Some(json!({ "id": 17 })), "17"),
        (
            "memory_get_latest",
            Some(json!({ "id": LATEST_ID, "extra": "private-extra" })),
            "private-extra",
        ),
        (
            "memory_get_exact",
            Some(json!({ "id": EXACT_ID })),
            EXACT_ID,
        ),
        (
            "memory_get_exact",
            Some(json!({ "id": EXACT_ID, "version": "private-version" })),
            "private-version",
        ),
        (
            "memory_get_exact",
            Some(json!({
                "id": EXACT_ID,
                "version": EXACT_VERSION,
                "extra": "unknown-exact-private-sentinel",
            })),
            "unknown-exact-private-sentinel",
        ),
        (
            "memory_list_versions",
            Some(json!({ "offset": 3, "limit": 2 })),
            VERSION_LIST_ID,
        ),
        ("memory_list_versions", Some(json!({ "id": 17 })), "17"),
        (
            "memory_list_versions",
            Some(json!({
                "id": VERSION_LIST_ID,
                "extra": "unknown-versions-private-sentinel",
            })),
            "unknown-versions-private-sentinel",
        ),
        ("memory_save", None, SAVE_CONTENT),
    ] {
        let operations = RecordingOperations::new(unused_responses());
        let observer = operations.clone();
        let server = server(operations, generator.clone());

        let response = server
            .call_tool(call_tool_request("invalid-input", tool, arguments))
            .await;

        assert_tool_error(
            &response,
            "invalid_input",
            &[
                protected_value,
                SEARCH_QUERY,
                LEGACY_CALLER_VECTOR_TEXT[0],
                LEGACY_CALLER_VECTOR_TEXT[1],
            ],
        );
        assert!(observer.calls().is_empty());
    }

    let operations = RecordingOperations::new(unused_responses());
    let observer = operations.clone();
    let server = server(operations, generator.clone());

    let response = server
        .call_tool(call_tool_request(
            "unknown-tool",
            "unknown-tool-private-sentinel",
            Some(json!({ "title": SAVE_TITLE })),
        ))
        .await;

    assert_tool_error(
        &response,
        "invalid_input",
        &["unknown-tool-private-sentinel", SAVE_TITLE],
    );
    assert!(observer.calls().is_empty());
    assert_generator_not_invoked(&generator);
}

#[tokio::test]
async fn typed_service_failures_are_text_only_and_content_safe() {
    let mut save_responses = unused_responses();
    save_responses.insert_memory = Err(OperationError::conflict(Operation::InsertMemory));
    let save_operations = RecordingOperations::new(save_responses);
    let save_observer = save_operations.clone();
    let save_server = server(save_operations, None);
    let save_response = save_server
        .call_tool(call_tool_request(
            "save-conflict",
            "memory_save",
            Some(json!({
                "title": SAVE_TITLE,
                "content": SAVE_CONTENT,
                "session_id": SAVE_SESSION,
            })),
        ))
        .await;
    assert_tool_error(
        &save_response,
        "conflict",
        &[SAVE_TITLE, SAVE_CONTENT, SAVE_SESSION],
    );
    assert_eq!(save_observer.calls().len(), 1);

    let mut search_responses = unused_responses();
    search_responses.search_lexical = Err(OperationError::backend_failure(
        Operation::SearchLexical,
        BackendSource::MemoryStore,
    ));
    search_responses.search_vector = Ok(Vec::new());
    let search_operations = RecordingOperations::new(search_responses);
    let search_observer = search_operations.clone();
    let search_server = server(search_operations, None);
    let search_response = search_server
        .call_tool(call_tool_request(
            "search-backend",
            "memory_search",
            Some(search_arguments()),
        ))
        .await;
    assert_tool_error(&search_response, "backend_failure", &[SEARCH_QUERY]);
    assert_eq!(search_observer.calls(), vec![lexical_search_call()]);

    let mut latest_responses = unused_responses();
    latest_responses.get_latest = Err(OperationError::not_found(Operation::GetLatest));
    let latest_operations = RecordingOperations::new(latest_responses);
    let latest_observer = latest_operations.clone();
    let latest_server = server(latest_operations, None);
    let latest_response = latest_server
        .call_tool(call_tool_request(
            "latest-not-found",
            "memory_get_latest",
            Some(json!({ "id": LATEST_ID })),
        ))
        .await;
    assert_tool_error(&latest_response, "not_found", &[LATEST_ID]);
    assert_eq!(latest_observer.calls().len(), 1);

    let mut exact_responses = unused_responses();
    exact_responses.get_exact = Err(OperationError::invalid_backend_response(
        Operation::GetExact,
        BackendResponseField::Memory,
    ));
    let exact_operations = RecordingOperations::new(exact_responses);
    let exact_observer = exact_operations.clone();
    let exact_server = server(exact_operations, None);
    let exact_response = exact_server
        .call_tool(call_tool_request(
            "exact-internal",
            "memory_get_exact",
            Some(json!({ "id": EXACT_ID, "version": EXACT_VERSION })),
        ))
        .await;
    assert_tool_error(&exact_response, "internal_failure", &[EXACT_ID, "7"]);
    assert_eq!(exact_observer.calls().len(), 1);

    let mut versions_responses = unused_responses();
    versions_responses.list_versions = Err(OperationError::backend_failure(
        Operation::ListVersions,
        BackendSource::MemoryStore,
    ));
    let versions_operations = RecordingOperations::new(versions_responses);
    let versions_observer = versions_operations.clone();
    let versions_server = server(versions_operations, None);
    let versions_response = versions_server
        .call_tool(call_tool_request(
            "versions-backend",
            "memory_list_versions",
            Some(json!({ "id": VERSION_LIST_ID })),
        ))
        .await;
    assert_tool_error(&versions_response, "backend_failure", &[VERSION_LIST_ID]);
    assert_eq!(versions_observer.calls().len(), 1);
}

#[tokio::test]
async fn dispatch_rejects_invalid_metadata_features_and_methods_without_operations() {
    let operations = RecordingOperations::new(unused_responses());
    let observer = operations.clone();
    let server = server(operations, None);

    for (request, expected_code, expected_message, protected_value) in [
        (
            raw_request("missing-params", "tools/call", None),
            i64::from(RpcErrorCodes::INVALID_PARAMS),
            "invalid_protocol_metadata",
            "missing-params-secret",
        ),
        (
            raw_request(
                "missing-capabilities",
                "tools/call",
                Some(json!({
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION,
                    },
                    "name": "memory_save",
                    "arguments": { "title": SAVE_TITLE },
                })),
            ),
            i64::from(RpcErrorCodes::INVALID_PARAMS),
            "invalid_protocol_metadata",
            SAVE_TITLE,
        ),
        (
            raw_request(
                "unsupported-version",
                "tools/call",
                Some(raw_tool_params(
                    json!({
                        "io.modelcontextprotocol/protocolVersion": "unsupported-version-private-sentinel",
                        "io.modelcontextprotocol/clientCapabilities": {},
                    }),
                    "memory_save",
                    json!({ "title": SAVE_TITLE }),
                )),
            ),
            i64::from(RpcErrorCodes::UNSUPPORTED_PROTOCOL_VERSION),
            "unsupported_protocol_version",
            "unsupported-version-private-sentinel",
        ),
        (
            raw_request(
                "malformed-capabilities",
                "tools/call",
                Some(raw_tool_params(
                    json!({
                        "io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION,
                        "io.modelcontextprotocol/clientCapabilities": {
                            "sampling": "unsupported-capability-private-sentinel",
                        },
                    }),
                    "memory_save",
                    json!({ "title": SAVE_TITLE }),
                )),
            ),
            i64::from(RpcErrorCodes::INVALID_PARAMS),
            "invalid_protocol_metadata",
            "unsupported-capability-private-sentinel",
        ),
        (
            raw_request(
                "unsupported-feature",
                "tools/call",
                Some(json!({
                    "_meta": request_meta_value(),
                    "name": "memory_save",
                    "arguments": { "title": SAVE_TITLE },
                    "requestState": "unsupported-feature-private-sentinel",
                })),
            ),
            i64::from(RpcErrorCodes::INVALID_PARAMS),
            "unsupported_tool_feature",
            "unsupported-feature-private-sentinel",
        ),
        (
            raw_request(
                "unsupported-method",
                "unsupported-method-private-sentinel",
                Some(json!({ "_meta": request_meta_value() })),
            ),
            i64::from(RpcErrorCodes::METHOD_NOT_FOUND),
            "method_not_found",
            "unsupported-method-private-sentinel",
        ),
    ] {
        let response = server.dispatch(request).await;
        let DispatchResponse::Error(response) = response else {
            panic!("invalid dispatch should return a protocol error");
        };

        assert_protocol_error(
            &response,
            expected_code,
            expected_message,
            &[protected_value, SAVE_TITLE],
        );
    }

    assert!(observer.calls().is_empty());
}

#[tokio::test]
async fn dispatch_accepts_stateless_empty_capabilities_for_supported_methods() {
    let operations = RecordingOperations::new(unused_responses());
    let observer = operations.clone();
    let discovery_server = server(operations, None);

    let discover = discovery_server
        .dispatch(raw_request(
            "dispatch-discover",
            "server/discover",
            Some(json!({ "_meta": request_meta_value() })),
        ))
        .await;
    let DispatchResponse::Discover(discover) = discover else {
        panic!("server/discover should dispatch to discovery");
    };
    assert_eq!(
        discover.id,
        RequestId::String("dispatch-discover".to_owned())
    );

    let list_tools = discovery_server
        .dispatch(raw_request(
            "dispatch-tools-list",
            "tools/list",
            Some(json!({ "_meta": request_meta_value() })),
        ))
        .await;
    let DispatchResponse::ListTools(list_tools) = list_tools else {
        panic!("tools/list should dispatch to the tool registry");
    };
    assert_eq!(
        list_tools.id,
        RequestId::String("dispatch-tools-list".to_owned())
    );
    assert!(observer.calls().is_empty());

    let mut responses = unused_responses();
    responses.insert_memory = Ok(());
    let operations = RecordingOperations::new(responses);
    let observer = operations.clone();
    let server = server(operations, None);
    let expected = saved_memory();

    let call = server
        .dispatch(raw_request(
            "dispatch-save",
            "tools/call",
            Some(raw_tool_params(
                request_meta_value(),
                "memory_save",
                json!({
                    "title": SAVE_TITLE,
                    "content": SAVE_CONTENT,
                    "session_id": SAVE_SESSION,
                }),
            )),
        ))
        .await;
    let DispatchResponse::CallTool(call) = call else {
        panic!("tools/call should dispatch to the memory service");
    };
    assert_success_response(
        &call,
        "dispatch-save",
        "memory_save",
        json!({ "memory": memory_json(&expected) }),
    );
    assert_eq!(
        observer.calls(),
        vec![OperationCall::InsertMemory(expected)]
    );
}

fn server(
    operations: RecordingOperations,
    generator: Option<RecordingQueryEmbeddingGenerator>,
) -> McpServer<RecordingOperations, DeterministicCreateContext, RecordingQueryEmbeddingGenerator> {
    McpServer::new(MemoryToolService::new(
        operations,
        DeterministicCreateContext,
        generator,
    ))
}

fn generator_compositions() -> [Option<RecordingQueryEmbeddingGenerator>; 2] {
    [None, Some(enabled_generator())]
}

fn enabled_generator() -> RecordingQueryEmbeddingGenerator {
    RecordingQueryEmbeddingGenerator::new(Ok(EmbeddingVector::try_from(GENERATED_VECTOR.to_vec())
        .expect("the generated test vector should be a canonical embedding")))
}

fn search_arguments() -> Value {
    json!({ "query": SEARCH_QUERY, "limit": SEARCH_LIMIT })
}

fn lexical_search_call() -> OperationCall {
    OperationCall::SearchLexical(Bm25Search {
        query: SEARCH_QUERY.to_owned(),
        limit: SEARCH_LIMIT,
    })
}

fn generated_vector_search_call() -> OperationCall {
    OperationCall::SearchVector(VectorSearch {
        vector: GENERATED_VECTOR.to_vec(),
        limit: SEARCH_LIMIT,
    })
}

fn assert_response_excludes(
    response: &rust_mcp_schema::CallToolResultResponse,
    protected_values: &[&str],
) {
    let serialized = serde_json::to_string(response).expect("tool response should serialize");
    for value in protected_values {
        assert!(
            !serialized.contains(value),
            "tool response leaked protected value {value}: {serialized}"
        );
    }
}

fn assert_generator_not_invoked(generator: &Option<RecordingQueryEmbeddingGenerator>) {
    if let Some(generator) = generator {
        assert!(
            generator.calls().is_empty(),
            "the query embedding generator should not be invoked"
        );
    }
}

fn unused_responses() -> MemoryOperationResponses {
    MemoryOperationResponses {
        insert_memory: Err(OperationError::backend_failure(
            Operation::InsertMemory,
            BackendSource::MemoryStore,
        )),
        search_lexical: Err(OperationError::backend_failure(
            Operation::SearchLexical,
            BackendSource::MemoryStore,
        )),
        search_vector: Err(OperationError::backend_failure(
            Operation::SearchVector,
            BackendSource::MemoryStore,
        )),
        get_latest: Err(OperationError::backend_failure(
            Operation::GetLatest,
            BackendSource::MemoryStore,
        )),
        get_exact: Err(OperationError::backend_failure(
            Operation::GetExact,
            BackendSource::MemoryStore,
        )),
        list_versions: Err(OperationError::backend_failure(
            Operation::ListVersions,
            BackendSource::MemoryStore,
        )),
    }
}

fn assert_only_tools_capability(capabilities: &rust_mcp_schema::ServerCapabilities) {
    assert!(capabilities.tools.is_some());
    assert!(capabilities.completions.is_none());
    assert!(capabilities.experimental.is_none());
    assert!(capabilities.extensions.is_none());
    assert!(capabilities.logging.is_none());
    assert!(capabilities.prompts.is_none());
    assert!(capabilities.resources.is_none());
}

fn assert_identity(result: &Value) {
    assert_eq!(
        result["_meta"]["io.modelcontextprotocol/serverInfo"],
        json!({
            "name": SERVER_NAME,
            "version": env!("CARGO_PKG_VERSION"),
        })
    );
}

#[derive(Clone)]
struct DeterministicCreateContext;

impl CreateContext for DeterministicCreateContext {
    fn new_id(&self) -> String {
        "assigned-memory-id".to_owned()
    }

    fn now(&self) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-09-20T00:00:00Z")
            .expect("fixed timestamp should parse")
            .with_timezone(&Utc)
    }
}

fn call_tool_request(request_id: &str, tool: &str, arguments: Option<Value>) -> CallToolRequest {
    let params = CallToolRequestParams::new(tool, request_meta());
    let params = match arguments {
        Some(Value::Object(arguments)) => params.with_arguments(arguments),
        Some(_) => panic!("test tool arguments should be JSON objects"),
        None => params,
    };

    CallToolRequest::new(RequestId::String(request_id.to_owned()), params)
}

fn raw_request(request_id: &str, method: &str, params: Option<Value>) -> JsonrpcRequest {
    let params = params.map(|params| {
        params
            .as_object()
            .expect("test request params should be JSON objects")
            .clone()
    });

    JsonrpcRequest::new(
        RequestId::String(request_id.to_owned()),
        method.to_owned(),
        params,
    )
}

fn raw_tool_params(meta: Value, name: &str, arguments: Value) -> Value {
    json!({
        "_meta": meta,
        "name": name,
        "arguments": arguments,
    })
}

fn request_meta() -> RequestMetaObject {
    RequestMetaObject::new(PROTOCOL_VERSION, ClientCapabilities::default())
}

fn request_meta_value() -> Value {
    serde_json::to_value(request_meta()).expect("request metadata should serialize")
}

fn saved_memory() -> MemoryVersionInput {
    MemoryVersionInput {
        id: "assigned-memory-id".to_owned(),
        version: 1,
        memory_type: "unclassified".to_owned(),
        title: SAVE_TITLE.to_owned(),
        content: SAVE_CONTENT.to_owned(),
        created_at: timestamp(),
        updated_at: timestamp(),
        concepts: Vec::new(),
        files: Vec::new(),
        session_ids: vec![SAVE_SESSION.to_owned()],
        source_observation_ids: Vec::new(),
    }
}

fn canonical_memory(id: &str, version: i64, marker: &str) -> MemoryVersionInput {
    MemoryVersionInput {
        id: id.to_owned(),
        version,
        memory_type: "unclassified".to_owned(),
        title: format!("{marker} title"),
        content: format!("{marker} content"),
        created_at: timestamp(),
        updated_at: timestamp(),
        concepts: vec![format!("{marker} concept")],
        files: vec![format!("{marker} file")],
        session_ids: vec![format!("{marker} session")],
        source_observation_ids: vec![format!("{marker} observation")],
    }
}

fn search_result(memory: &MemoryVersionInput) -> MemorySearchResult {
    MemorySearchResult::try_new(
        memory
            .clone()
            .try_into()
            .expect("test memory should be a valid search result"),
        0.5,
    )
    .expect("test relevance should be valid")
}

fn version_summary(version: i64) -> MemoryVersionSummary {
    MemoryVersionSummary {
        version: MemoryVersion::try_from(version).expect("test versions should be positive"),
        updated_at: timestamp(),
    }
}

fn version_summary_with_microseconds(version: i64) -> MemoryVersionSummary {
    MemoryVersionSummary {
        version: MemoryVersion::try_from(version).expect("test versions should be positive"),
        updated_at: DateTime::parse_from_rfc3339("2026-09-20T00:00:00.123456Z")
            .expect("fixed fractional test timestamp should parse")
            .with_timezone(&Utc),
    }
}

fn timestamp() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-09-20T00:00:00Z")
        .expect("fixed timestamp should parse")
        .with_timezone(&Utc)
}

fn memory_json(memory: &MemoryVersionInput) -> Value {
    serde_json::to_value(MemoryDto::from(memory.clone()))
        .expect("memory DTO should serialize successfully")
}

fn assert_success_response(
    response: &rust_mcp_schema::CallToolResultResponse,
    request_id: &str,
    tool: &str,
    expected_structured_content: Value,
) {
    let wire = serde_json::to_value(response).expect("tool response should serialize");
    let result = wire
        .get("result")
        .expect("tool response should include a result");

    assert_eq!(wire["jsonrpc"], "2.0");
    assert_eq!(wire["id"], request_id);
    assert_eq!(result["resultType"], "complete");
    assert!(
        !result
            .as_object()
            .expect("tool result should be an object")
            .contains_key("isError")
    );
    assert_eq!(
        result["content"],
        json!([{ "type": "text", "text": format!("{tool} complete") }])
    );
    assert_identity(result);
    assert_eq!(result["structuredContent"], expected_structured_content);
    assert_matches_schema(
        &result["structuredContent"],
        &advertised_output_schema(tool),
    );
}

fn assert_tool_error(
    response: &rust_mcp_schema::CallToolResultResponse,
    expected_code: &str,
    protected_values: &[&str],
) {
    let wire = serde_json::to_value(response).expect("tool error should serialize");
    let result = wire
        .get("result")
        .expect("tool error should include a result");
    let result_object = result
        .as_object()
        .expect("tool error result should be an object");

    assert_eq!(result["resultType"], "complete");
    assert_eq!(result["isError"], true);
    assert_eq!(
        result["content"],
        json!([{ "type": "text", "text": expected_code }])
    );
    assert!(!result_object.contains_key("structuredContent"));
    assert!(!result_object.contains_key("_meta"));

    let serialized = wire.to_string();
    for value in protected_values {
        assert!(
            !serialized.contains(value),
            "tool error leaked protected value {value}: {serialized}"
        );
    }
}

fn assert_protocol_error(
    response: &JsonrpcErrorResponse,
    expected_code: i64,
    expected_message: &str,
    protected_values: &[&str],
) {
    let wire = serde_json::to_value(response).expect("protocol error should serialize");
    let error = wire
        .get("error")
        .expect("protocol error should include an error");
    let error_object = error
        .as_object()
        .expect("protocol error should be an object");

    assert_eq!(wire["jsonrpc"], "2.0");
    assert_eq!(error["code"], expected_code);
    assert_eq!(error["message"], expected_message);
    assert!(!error_object.contains_key("data"));

    let serialized = wire.to_string();
    for value in protected_values {
        assert!(
            !serialized.contains(value),
            "protocol error leaked protected value {value}: {serialized}"
        );
    }
}

fn advertised_output_schema(tool_name: &str) -> Value {
    serde_json::to_value(tool_registry())
        .expect("tool registry should serialize")
        .as_array()
        .expect("tool registry should be an array")
        .iter()
        .find(|tool| tool["name"] == tool_name)
        .unwrap_or_else(|| panic!("tool registry should contain {tool_name}"))["outputSchema"]
        .clone()
}

fn assert_matches_schema(value: &Value, schema: &Value) {
    let schema_type = schema
        .get("type")
        .expect("advertised schema should declare a type");
    let matches_type = match schema_type {
        Value::String(kind) => value_matches_type(value, kind),
        Value::Array(kinds) => kinds
            .iter()
            .filter_map(Value::as_str)
            .any(|kind| value_matches_type(value, kind)),
        _ => false,
    };
    assert!(matches_type, "value {value} should match schema {schema}");

    if let (Some(object), Some(properties)) = (value.as_object(), schema["properties"].as_object())
    {
        for required in schema["required"]
            .as_array()
            .expect("object schema should list required properties")
        {
            assert!(
                object.contains_key(
                    required
                        .as_str()
                        .expect("required property names should be strings")
                ),
                "structured result should contain every required property"
            );
        }

        if schema["additionalProperties"] == false {
            assert!(
                object.keys().all(|key| properties.contains_key(key)),
                "structured result should not contain properties outside its advertised schema"
            );
        }

        for (name, property_schema) in properties {
            if let Some(property) = object.get(name) {
                assert_matches_schema(property, property_schema);
            }
        }
    }

    if let (Some(items), Some(item_schema)) = (value.as_array(), schema.get("items")) {
        for item in items {
            assert_matches_schema(item, item_schema);
        }
    }
}

fn value_matches_type(value: &Value, schema_type: &str) -> bool {
    match schema_type {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "number" => value.is_number(),
        "null" => value.is_null(),
        _ => false,
    }
}
