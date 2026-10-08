use std::{
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use iii_sdk::{IIIClient, InitOptions, WorkerIdentityMode, runtime::WorkerMetadata};
use mcp_worker::{
    config::{
        Config, III_NAMESPACE_ENV, III_URL_ENV, III_WORKER_NAME_ENV, QueryEmbeddingConfig,
        TOTAL_RECALL_EMBEDDING_MODEL_ENV, TOTAL_RECALL_EMBEDDING_PROVIDER_ENV,
        TOTAL_RECALL_MCP_CHANNEL_CAPACITY_ENV, TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS_ENV,
        TOTAL_RECALL_MCP_MAX_IN_FLIGHT_ENV, TOTAL_RECALL_MCP_MAX_LINE_BYTES_ENV,
        TOTAL_RECALL_MEMORY_DATABASE_ENV,
    },
    contracts::{MemoryDto, tool_registry},
    embedding::{RecordingQueryEmbeddingGenerator, RouterQueryEmbeddingGenerator},
    operations::{
        BackendSource, MemoryOperationResponses, Operation, OperationCall, OperationError,
        RecordingOperations,
    },
    runtime::{RuntimeInput, RuntimeShutdown, run_mcp_server},
    server::McpServer,
    service::{CreateContext, MemoryToolService},
};
use memory_store::contracts::{
    Bm25Search, EmbeddingVector, MemorySearchResult, MemoryVersionInput, VectorSearch,
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    time::timeout,
};
use tokio_tungstenite::{
    WebSocketStream, accept_async, accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{Callback, ErrorResponse, Request, Response},
    },
};

const PROTOCOL_VERSION: &str = "2026-07-28";
const PRIVATE_SENTINEL: &str = "runtime-server-private-sentinel";
const GENERATED_VECTOR: &[f64] = &[987_654.25, -123_456.5];
const LEGACY_CALLER_VECTOR: &[f64] = &[456_789.75, -654_321.5];
const LATE_VECTOR: &[f64] = &[234_567.75, -345_678.5];
const VECTOR_TEXT: &[&str] = &[
    "987654.25",
    "123456.5",
    "456789.75",
    "654321.5",
    "234567.75",
    "345678.5",
];
const SEARCH_LIMIT: u32 = 3;
const VALID_QUERY: &str = "valid-query-private-sentinel";
const REJECTED_QUERY: &str = "rejected-query-private-sentinel";
const UNKNOWN_ARGUMENT_SENTINEL: &str = "unknown-argument-private-sentinel";
const WRONG_TYPE_SENTINEL: &str = "wrong-type-private-sentinel";
const ACTIVE_QUERY: &str = "active-query-private-sentinel";
const QUEUED_QUERY: &str = "queued-query-private-sentinel";
const TIMED_OUT_QUERY: &str = "timed-out-query-private-sentinel";
const RECOVERED_QUERY: &str = "recovered-query-private-sentinel";
const PROVIDER_SENTINEL: &str = "provider-private-sentinel";
const MODEL_SENTINEL: &str = "model-private-sentinel";
const NAMESPACE: &str = "mcp-worker-runtime-server";
const WORKER_NAME: &str = "mcp-worker-runtime-server";
const EMBEDDING_TIMEOUT_MS: u64 = 200;
const TEST_BOUND: Duration = Duration::from_secs(5);

#[tokio::test]
async fn runtime_serializes_direct_schema_responses_for_valid_raw_frames_in_order() {
    for generator in generator_compositions() {
        let generator_observer = generator.clone();
        let mut responses = unused_responses();
        responses.insert_memory = Ok(());
        let operations = RecordingOperations::new(responses);
        let observer = operations.clone();
        let server = server(operations, generator);
        let frames = frames([
            request(
                "discover-request",
                "server/discover",
                json!({ "_meta": request_meta() }),
            ),
            request(
                "tools-request",
                "tools/list",
                json!({ "_meta": request_meta() }),
            ),
            request(
                "save-request",
                "tools/call",
                tool_params(
                    "memory_save",
                    json!({
                        "title": "saved title",
                        "content": "saved content",
                        "session_id": "saved session",
                    }),
                ),
            ),
        ]);
        let config = config(4_096, 3, 1);
        let (input, queue) = RuntimeInput::new(ReadyReader::new(frames), &config);
        let writer = CaptureWriter::open();

        run_mcp_server(
            input,
            queue,
            config.max_in_flight,
            RuntimeShutdown::new(),
            server,
            writer.clone(),
        )
        .await
        .expect("EOF should drain every schema response");

        assert_eq!(
            response_lines(&writer.bytes()),
            vec![
                discover_response("discover-request"),
                tools_response("tools-request"),
                save_response("save-request"),
            ]
        );
        assert_eq!(writer.flushes(), 4);
        assert_eq!(
            observer.calls(),
            vec![OperationCall::InsertMemory(saved_memory())]
        );
        assert_generator_not_invoked(&generator_observer);
    }
}

#[tokio::test]
async fn runtime_serializes_protocol_and_tool_errors_without_operations_or_notification_output() {
    for generator in generator_compositions() {
        let generator_observer = generator.clone();
        let operations = RecordingOperations::new(unused_responses());
        let observer = operations.clone();
        let server = server(operations, generator);
        let frames = frames([
            request(
                "missing-metadata",
                "tools/call",
                json!({
                    "name": "memory_save",
                    "arguments": { "title": PRIVATE_SENTINEL },
                }),
            ),
            request(
                "unsupported-version",
                "server/discover",
                json!({
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": PRIVATE_SENTINEL,
                        "io.modelcontextprotocol/clientCapabilities": {},
                    },
                }),
            ),
            request(
                "unsupported-method",
                PRIVATE_SENTINEL,
                json!({ "_meta": request_meta() }),
            ),
            request(
                "invalid-arguments",
                "tools/call",
                tool_params(
                    "memory_save",
                    json!({
                        "title": "title",
                        "content": "content",
                        "session_id": "session",
                        "unexpected": PRIVATE_SENTINEL,
                    }),
                ),
            ),
            json!({
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": tool_params(
                    "memory_save",
                    json!({ "title": PRIVATE_SENTINEL }),
                ),
            }),
        ]);
        let config = config(4_096, 5, 1);
        let (input, queue) = RuntimeInput::new(ReadyReader::new(frames), &config);
        let writer = CaptureWriter::open();

        run_mcp_server(
            input,
            queue,
            config.max_in_flight,
            RuntimeShutdown::new(),
            server,
            writer.clone(),
        )
        .await
        .expect("invalid protocol frames should receive stable responses");

        assert_eq!(
            response_lines(&writer.bytes()),
            vec![
                protocol_error("missing-metadata", -32602, "invalid_protocol_metadata"),
                protocol_error(
                    "unsupported-version",
                    -32022,
                    "unsupported_protocol_version",
                ),
                protocol_error("unsupported-method", -32601, "method_not_found"),
                tool_error("invalid-arguments", "invalid_input"),
            ]
        );
        assert_eq!(writer.flushes(), 5);
        assert!(observer.calls().is_empty());
        assert!(
            !String::from_utf8(writer.bytes())
                .expect("responses should be UTF-8 JSON")
                .contains(PRIVATE_SENTINEL)
        );
        assert_generator_not_invoked(&generator_observer);
    }
}

#[tokio::test]
async fn runtime_keeps_malformed_and_oversize_frames_response_free_and_recovers() {
    for generator in generator_compositions() {
        let generator_observer = generator.clone();
        let operations = RecordingOperations::new(unused_responses());
        let observer = operations.clone();
        let server = server(operations, generator);
        let valid = request(
            "recovered-request",
            "server/discover",
            json!({ "_meta": request_meta() }),
        );
        let valid_wire = serde_json::to_vec(&valid).expect("valid request should serialize");
        let max_line_bytes = valid_wire.len();
        let mut raw_frames = format!("{{\"secret\":\"{PRIVATE_SENTINEL}\"").into_bytes();
        raw_frames.push(b'\n');
        raw_frames.extend(std::iter::repeat_n(b'x', max_line_bytes + 1));
        raw_frames.push(b'\n');
        raw_frames.extend(valid_wire);
        raw_frames.push(b'\n');
        let config = config(max_line_bytes, 1, 1);
        let (input, queue) = RuntimeInput::new(ReadyReader::new(raw_frames), &config);
        let writer = CaptureWriter::open();

        run_mcp_server(
            input,
            queue,
            config.max_in_flight,
            RuntimeShutdown::new(),
            server,
            writer.clone(),
        )
        .await
        .expect("response-free admission failures should not prevent recovery");

        assert_eq!(
            response_lines(&writer.bytes()),
            vec![discover_response("recovered-request")]
        );
        assert!(observer.calls().is_empty());
        assert!(
            !String::from_utf8(writer.bytes())
                .expect("responses should be UTF-8 JSON")
                .contains(PRIVATE_SENTINEL)
        );
        assert_generator_not_invoked(&generator_observer);
    }
}

#[tokio::test]
async fn runtime_holds_server_permits_through_flush_and_drains_eof_in_order() {
    for generator in generator_compositions() {
        let generator_observer = generator.clone();
        let mut responses = unused_responses();
        responses.insert_memory = Ok(());
        let operations = RecordingOperations::new(responses);
        let observer = operations.clone();
        let server = server(operations, generator);
        let frames = frames([
            request(
                "first-request",
                "tools/call",
                tool_params(
                    "memory_save",
                    json!({
                        "title": "saved title",
                        "content": "saved content",
                        "session_id": "saved session",
                    }),
                ),
            ),
            request(
                "second-request",
                "tools/call",
                tool_params(
                    "memory_save",
                    json!({
                        "title": "saved title",
                        "content": "saved content",
                        "session_id": "saved session",
                    }),
                ),
            ),
        ]);
        let config = config(4_096, 1, 1);
        let (input, queue) = RuntimeInput::new(ReadyReader::new(frames), &config);
        let writer = CaptureWriter::blocked();
        let mut runner = tokio::spawn(run_mcp_server(
            input,
            queue,
            config.max_in_flight,
            RuntimeShutdown::new(),
            server,
            writer.clone(),
        ));

        tokio::select! {
            _ = writer.wait_until_flush_blocked() => {}
            result = &mut runner => panic!("runtime completed before flushing its first response: {result:?}"),
        }
        assert_eq!(
            response_lines(&writer.bytes()),
            vec![save_response("first-request")]
        );
        assert_eq!(
            observer.calls(),
            vec![OperationCall::InsertMemory(saved_memory())]
        );
        writer.release();

        runner
            .await
            .expect("runtime task should not panic")
            .expect("EOF should drain the queued server response");
        assert_eq!(
            response_lines(&writer.bytes()),
            vec![
                save_response("first-request"),
                save_response("second-request"),
            ]
        );
        assert_eq!(writer.flushes(), 4);
        assert_eq!(
            observer.calls(),
            vec![
                OperationCall::InsertMemory(saved_memory()),
                OperationCall::InsertMemory(saved_memory()),
            ]
        );
        assert_generator_not_invoked(&generator_observer);
    }
}

#[tokio::test]
async fn runtime_rejects_caller_vectors_and_invalid_search_frames_before_generation_or_operations()
{
    for generator in generator_compositions() {
        let generator_observer = generator.clone();
        let enabled = generator.is_some();
        let operations = RecordingOperations::new(search_responses());
        let observer = operations.clone();
        let server = server(operations, generator);
        let frames = frames([
            search_request(
                "legacy-vector",
                json!({
                    "query": REJECTED_QUERY,
                    "vector": LEGACY_CALLER_VECTOR,
                    "limit": SEARCH_LIMIT,
                }),
            ),
            search_request(
                "valid-search",
                json!({ "query": VALID_QUERY, "limit": SEARCH_LIMIT }),
            ),
            search_request(
                "null-vector",
                json!({ "query": REJECTED_QUERY, "vector": null, "limit": SEARCH_LIMIT }),
            ),
            search_request(
                "unknown-argument",
                json!({
                    "query": REJECTED_QUERY,
                    "limit": SEARCH_LIMIT,
                    "extra": UNKNOWN_ARGUMENT_SENTINEL,
                }),
            ),
            search_request(
                "wrong-type",
                json!({ "query": REJECTED_QUERY, "limit": WRONG_TYPE_SENTINEL }),
            ),
            search_request("empty-query", json!({ "query": "", "limit": SEARCH_LIMIT })),
            search_request(
                "nul-query",
                json!({ "query": format!("{REJECTED_QUERY}\0"), "limit": SEARCH_LIMIT }),
            ),
            search_request(
                "over-cap-limit",
                json!({ "query": REJECTED_QUERY, "limit": 51 }),
            ),
        ]);
        let config = config(4_096, 2, 1);
        let (input, queue) = RuntimeInput::new(ReadyReader::new(frames), &config);
        let writer = CaptureWriter::open();

        run_mcp_server(
            input,
            queue,
            config.max_in_flight,
            RuntimeShutdown::new(),
            server,
            writer.clone(),
        )
        .await
        .expect("EOF should drain every search response");

        assert_eq!(
            response_lines(&writer.bytes()),
            vec![
                tool_error("legacy-vector", "invalid_input"),
                search_response("valid-search", &expected_search_results(enabled)),
                tool_error("null-vector", "invalid_input"),
                tool_error("unknown-argument", "invalid_input"),
                tool_error("wrong-type", "invalid_input"),
                tool_error("empty-query", "invalid_input"),
                tool_error("nul-query", "invalid_input"),
                tool_error("over-cap-limit", "invalid_input"),
            ]
        );
        assert_eq!(writer.flushes(), 9);
        assert_eq!(observer.calls(), search_calls(VALID_QUERY, enabled));
        if let Some(generator) = generator_observer {
            assert_eq!(generator.calls(), vec![VALID_QUERY.to_owned()]);
        }
        assert_output_excludes(
            &writer,
            &[
                REJECTED_QUERY,
                VALID_QUERY,
                UNKNOWN_ARGUMENT_SENTINEL,
                WRONG_TYPE_SENTINEL,
            ],
        );
    }
}

#[tokio::test]
async fn runtime_shutdown_drains_the_active_search_without_starting_queued_requests() {
    for generator in generator_compositions() {
        let generator_observer = generator.clone();
        let enabled = generator.is_some();
        let operations = RecordingOperations::new(search_responses());
        let observer = operations.clone();
        let server = server(operations, generator);
        let frames = frames([
            search_request(
                "active-search",
                json!({ "query": ACTIVE_QUERY, "limit": SEARCH_LIMIT }),
            ),
            search_request(
                "queued-search",
                json!({ "query": QUEUED_QUERY, "limit": SEARCH_LIMIT }),
            ),
        ]);
        let config = config(4_096, 2, 1);
        let (input, queue) = RuntimeInput::new(ReadyReader::held_open(frames), &config);
        let writer = CaptureWriter::blocked();
        let shutdown = RuntimeShutdown::new();
        let mut runner = tokio::spawn(run_mcp_server(
            input,
            queue,
            config.max_in_flight,
            shutdown.clone(),
            server,
            writer.clone(),
        ));

        tokio::select! {
            _ = writer.wait_until_flush_blocked() => {}
            result = &mut runner => panic!("runtime completed before flushing the active search: {result:?}"),
        }
        let active_response = search_response("active-search", &expected_search_results(enabled));
        assert_eq!(
            response_lines(&writer.bytes()),
            vec![active_response.clone()]
        );
        shutdown.request();
        assert!(!runner.is_finished());
        writer.release();

        timeout(TEST_BOUND, runner)
            .await
            .expect("shutdown should finish once the active response flushes")
            .expect("runtime task should not panic")
            .expect("shutdown should drain the active search");
        assert_eq!(response_lines(&writer.bytes()), vec![active_response]);
        assert_eq!(writer.flushes(), 3);
        assert_eq!(observer.calls(), search_calls(ACTIVE_QUERY, enabled));
        if let Some(generator) = generator_observer {
            assert_eq!(generator.calls(), vec![ACTIVE_QUERY.to_owned()]);
        }
        assert_output_excludes(&writer, &[ACTIVE_QUERY, QUEUED_QUERY]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_returns_lexical_results_after_the_sdk_embedding_timeout_and_releases_the_permit() {
    let mut engine = FakeRouterEngine::start().await;
    let config = router_config(&engine.url);
    assert_eq!(config.max_in_flight, 1);
    let client = connected_managed_client(&config).await;
    let operations = RecordingOperations::new(search_responses());
    let observer = operations.clone();
    let server = McpServer::new(MemoryToolService::new(
        operations,
        DeterministicContext,
        Some(router_generator(&client, &config)),
    ));
    let frames = frames([
        search_request(
            "timed-out-search",
            json!({ "query": TIMED_OUT_QUERY, "limit": SEARCH_LIMIT }),
        ),
        search_request(
            "recovered-search",
            json!({ "query": RECOVERED_QUERY, "limit": SEARCH_LIMIT }),
        ),
    ]);
    let (input, queue) = RuntimeInput::new(ReadyReader::new(frames), &config);
    let writer = CaptureWriter::open();
    let started = Instant::now();
    let runner = tokio::spawn(run_mcp_server(
        input,
        queue,
        config.max_in_flight,
        RuntimeShutdown::new(),
        server,
        writer.clone(),
    ));

    let unanswered = engine.next_invocation().await;
    assert_router_embed_invocation(&unanswered, TIMED_OUT_QUERY);
    timeout(TEST_BOUND, async {
        tokio::select! {
            biased;
            () = writer.wait_for_lines(1) => {}
            invocation = engine.invocations.recv() => panic!(
                "iii received another invocation before the timed-out search released its permit: {:?}",
                invocation.map(|invocation| invocation["function_id"].clone()),
            ),
        }
    })
    .await
    .expect("the timed-out search should return lexical results within the test bound");
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(EMBEDDING_TIMEOUT_MS),
        "the search returned before the configured SDK timeout: {elapsed:?}"
    );
    let timed_out_response = search_response("timed-out-search", &lexical_results());
    assert_eq!(
        response_lines(&writer.bytes()),
        vec![timed_out_response.clone()]
    );

    let recovered = timeout(TEST_BOUND, engine.invocations.recv())
        .await
        .expect("the queued search should reach iii after the timed-out search releases its permit")
        .expect("the fake engine should keep the worker connection open");
    assert_router_embed_invocation(&recovered, RECOVERED_QUERY);
    assert_ne!(recovered["invocation_id"], unanswered["invocation_id"]);
    let calls_before_recovery = observer.calls();
    assert_eq!(
        calls_before_recovery.first(),
        Some(&lexical_search_call(TIMED_OUT_QUERY))
    );
    assert!(
        !calls_before_recovery
            .iter()
            .any(|call| matches!(call, OperationCall::SearchVector(_))),
        "the timed-out search should not submit a vector search"
    );
    engine.reply(router_result(&unanswered, LATE_VECTOR));
    engine.reply(router_result(&recovered, GENERATED_VECTOR));

    timeout(TEST_BOUND, runner)
        .await
        .expect("the recovered search should finish within the test bound")
        .expect("runtime task should not panic")
        .expect("EOF should drain both search responses");
    assert_eq!(
        response_lines(&writer.bytes()),
        vec![
            timed_out_response,
            search_response("recovered-search", &combined_results()),
        ]
    );
    assert_eq!(writer.flushes(), 3);
    assert_eq!(
        observer.calls(),
        vec![
            lexical_search_call(TIMED_OUT_QUERY),
            lexical_search_call(RECOVERED_QUERY),
            generated_vector_search_call(),
        ]
    );
    assert_output_excludes(
        &writer,
        &[
            TIMED_OUT_QUERY,
            RECOVERED_QUERY,
            PROVIDER_SENTINEL,
            MODEL_SENTINEL,
        ],
    );

    client.shutdown_async().await;
    engine
        .assert_disconnected_without_further_invocations()
        .await;
}

fn server(
    operations: RecordingOperations,
    generator: Option<RecordingQueryEmbeddingGenerator>,
) -> McpServer<RecordingOperations, DeterministicContext, RecordingQueryEmbeddingGenerator> {
    McpServer::new(MemoryToolService::new(
        operations,
        DeterministicContext,
        generator,
    ))
}

fn generator_compositions() -> [Option<RecordingQueryEmbeddingGenerator>; 2] {
    [
        None,
        Some(RecordingQueryEmbeddingGenerator::new(Ok(
            EmbeddingVector::try_from(GENERATED_VECTOR.to_vec())
                .expect("the generated test vector should be a canonical embedding"),
        ))),
    ]
}

fn assert_generator_not_invoked(generator: &Option<RecordingQueryEmbeddingGenerator>) {
    if let Some(generator) = generator {
        assert!(
            generator.calls().is_empty(),
            "the query embedding generator should not be invoked"
        );
    }
}

fn search_responses() -> MemoryOperationResponses {
    let mut responses = unused_responses();
    responses.search_lexical = Ok(vec![
        search_result(&lexical_c()),
        search_result(&lexical_b()),
    ]);
    responses.search_vector = Ok(vec![search_result(&vector_b()), search_result(&vector_a())]);
    responses
}

fn lexical_b() -> MemoryVersionInput {
    canonical_memory("search-memory-b", 1, "lexical-b")
}

fn lexical_c() -> MemoryVersionInput {
    canonical_memory("search-memory-c", 2, "lexical-c")
}

fn vector_a() -> MemoryVersionInput {
    canonical_memory("search-memory-a", 1, "vector-a")
}

fn vector_b() -> MemoryVersionInput {
    canonical_memory("search-memory-b", 3, "vector-b")
}

fn lexical_results() -> Vec<MemoryVersionInput> {
    vec![lexical_b(), lexical_c()]
}

fn combined_results() -> Vec<MemoryVersionInput> {
    vec![vector_a(), vector_b(), lexical_c()]
}

fn expected_search_results(enabled: bool) -> Vec<MemoryVersionInput> {
    if enabled {
        combined_results()
    } else {
        lexical_results()
    }
}

fn search_calls(query: &str, enabled: bool) -> Vec<OperationCall> {
    let mut calls = vec![lexical_search_call(query)];
    if enabled {
        calls.push(generated_vector_search_call());
    }
    calls
}

fn lexical_search_call(query: &str) -> OperationCall {
    OperationCall::SearchLexical(Bm25Search {
        query: query.to_owned(),
        limit: SEARCH_LIMIT,
    })
}

fn generated_vector_search_call() -> OperationCall {
    OperationCall::SearchVector(VectorSearch {
        vector: GENERATED_VECTOR.to_vec(),
        limit: SEARCH_LIMIT,
    })
}

fn assert_output_excludes(writer: &CaptureWriter, protected_values: &[&str]) {
    let output = String::from_utf8(writer.bytes()).expect("responses should be UTF-8 JSON");
    for value in protected_values.iter().chain(VECTOR_TEXT) {
        assert!(
            !output.contains(value),
            "runtime output leaked protected value {value}"
        );
    }
}

fn router_config(engine_url: &str) -> Config {
    Config::from_values([
        (
            TOTAL_RECALL_MEMORY_DATABASE_ENV.to_owned(),
            "total-recall-memory".to_owned(),
        ),
        (
            TOTAL_RECALL_MCP_MAX_LINE_BYTES_ENV.to_owned(),
            "4096".to_owned(),
        ),
        (
            TOTAL_RECALL_MCP_CHANNEL_CAPACITY_ENV.to_owned(),
            "2".to_owned(),
        ),
        (
            TOTAL_RECALL_MCP_MAX_IN_FLIGHT_ENV.to_owned(),
            "1".to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_PROVIDER_ENV.to_owned(),
            PROVIDER_SENTINEL.to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_MODEL_ENV.to_owned(),
            MODEL_SENTINEL.to_owned(),
        ),
        (
            TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS_ENV.to_owned(),
            EMBEDDING_TIMEOUT_MS.to_string(),
        ),
        (III_URL_ENV.to_owned(), engine_url.to_owned()),
        (III_WORKER_NAME_ENV.to_owned(), WORKER_NAME.to_owned()),
        (III_NAMESPACE_ENV.to_owned(), NAMESPACE.to_owned()),
    ])
    .expect("the router test configuration should be valid")
}

async fn connected_managed_client(config: &Config) -> IIIClient {
    let mut metadata = WorkerMetadata::default();
    if let Some(worker_name) = &config.iii.worker_name {
        metadata.name.clone_from(worker_name);
    }
    metadata.namespace = config.iii.namespace.clone();

    let client = iii_sdk::register_worker(
        &config.iii.engine_url,
        InitOptions {
            metadata: Some(metadata),
            headers: None,
            otel: None,
            namespace: config.iii.namespace.clone(),
            identity: WorkerIdentityMode::Managed,
        },
    );
    client
        .wait_until_registered(TEST_BOUND)
        .await
        .expect("the fake engine should accept the worker registration");
    client
}

fn router_generator(client: &IIIClient, config: &Config) -> RouterQueryEmbeddingGenerator {
    let QueryEmbeddingConfig::Enabled(settings) = &config.query_embedding else {
        panic!("the router test configuration should enable query embedding");
    };
    RouterQueryEmbeddingGenerator::new(client.clone(), settings.clone())
}

fn assert_router_embed_invocation(invocation: &Value, query: &str) {
    assert_eq!(invocation["type"], "invokefunction");
    assert_eq!(invocation["function_id"], "router::embed");
    assert_eq!(invocation["namespace"], NAMESPACE);
    assert_eq!(
        invocation["data"],
        json!({
            "input": [query],
            "provider": PROVIDER_SENTINEL,
            "model": MODEL_SENTINEL,
        })
    );
    assert!(invocation["invocation_id"].is_string());
}

fn router_result(invocation: &Value, embedding: &[f64]) -> Value {
    json!({
        "type": "invocationresult",
        "invocation_id": invocation["invocation_id"],
        "function_id": "router::embed",
        "result": {
            "provider": PROVIDER_SENTINEL,
            "model": MODEL_SENTINEL,
            "embeddings": [embedding],
        },
    })
}

fn config(max_line_bytes: usize, channel_capacity: usize, max_in_flight: usize) -> Config {
    Config::from_values([
        (
            TOTAL_RECALL_MEMORY_DATABASE_ENV.to_owned(),
            "total-recall-memory".to_owned(),
        ),
        (
            TOTAL_RECALL_MCP_MAX_LINE_BYTES_ENV.to_owned(),
            max_line_bytes.to_string(),
        ),
        (
            TOTAL_RECALL_MCP_CHANNEL_CAPACITY_ENV.to_owned(),
            channel_capacity.to_string(),
        ),
        (
            TOTAL_RECALL_MCP_MAX_IN_FLIGHT_ENV.to_owned(),
            max_in_flight.to_string(),
        ),
    ])
    .expect("test configuration should be valid")
}

fn frames<const N: usize>(requests: [Value; N]) -> Vec<u8> {
    let mut frames = Vec::new();
    for request in requests {
        frames.extend(serde_json::to_vec(&request).expect("request should serialize"));
        frames.push(b'\n');
    }
    frames
}

fn request(id: &str, method: &str, params: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    })
}

fn request_meta() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION,
        "io.modelcontextprotocol/clientCapabilities": {},
    })
}

fn tool_params(tool: &str, arguments: Value) -> Value {
    json!({
        "_meta": request_meta(),
        "name": tool,
        "arguments": arguments,
    })
}

fn search_request(id: &str, arguments: Value) -> Value {
    request(id, "tools/call", tool_params("memory_search", arguments))
}

fn response_lines(bytes: &[u8]) -> Vec<Value> {
    let text = std::str::from_utf8(bytes).expect("response bytes should be UTF-8");
    assert!(
        text.ends_with('\n'),
        "every response should end with a newline"
    );
    text.lines()
        .map(|line| serde_json::from_str(line).expect("every response line should be JSON"))
        .collect()
}

fn server_identity() -> Value {
    json!({
        "io.modelcontextprotocol/serverInfo": {
            "name": "total-recall-mcp",
            "version": env!("CARGO_PKG_VERSION"),
        },
    })
}

fn discover_response(id: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "cacheScope": "public",
            "capabilities": { "tools": {} },
            "_meta": server_identity(),
            "resultType": "complete",
            "supportedVersions": [PROTOCOL_VERSION],
            "ttlMs": 0,
        },
    })
}

fn tools_response(id: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "cacheScope": "public",
            "_meta": server_identity(),
            "resultType": "complete",
            "tools": tool_registry(),
            "ttlMs": 0,
        },
    })
}

fn save_response(id: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "content": [{ "type": "text", "text": "memory_save complete" }],
            "_meta": server_identity(),
            "resultType": "complete",
            "structuredContent": {
                "memory": {
                    "id": "assigned-memory-id",
                    "version": 1,
                    "type": "unclassified",
                    "title": "saved title",
                    "content": "saved content",
                    "created_at": "2026-09-20T00:00:00Z",
                    "updated_at": "2026-09-20T00:00:00Z",
                    "concepts": [],
                    "files": [],
                    "session_ids": ["saved session"],
                    "source_observation_ids": [],
                },
            },
        },
    })
}

fn search_response(id: &str, results: &[MemoryVersionInput]) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "content": [{ "type": "text", "text": "memory_search complete" }],
            "_meta": server_identity(),
            "resultType": "complete",
            "structuredContent": {
                "results": results.iter().map(memory_json).collect::<Vec<_>>(),
            },
        },
    })
}

fn memory_json(memory: &MemoryVersionInput) -> Value {
    serde_json::to_value(MemoryDto::from(memory.clone())).expect("memory DTO should serialize")
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

fn protocol_error(id: &str, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message },
    })
}

fn tool_error(id: &str, code: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "content": [{ "type": "text", "text": code }],
            "isError": true,
            "resultType": "complete",
        },
    })
}

fn saved_memory() -> MemoryVersionInput {
    MemoryVersionInput {
        id: "assigned-memory-id".to_owned(),
        version: 1,
        memory_type: "unclassified".to_owned(),
        title: "saved title".to_owned(),
        content: "saved content".to_owned(),
        created_at: timestamp(),
        updated_at: timestamp(),
        concepts: Vec::new(),
        files: Vec::new(),
        session_ids: vec!["saved session".to_owned()],
        source_observation_ids: Vec::new(),
    }
}

fn timestamp() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-09-20T00:00:00Z")
        .expect("fixed timestamp should parse")
        .with_timezone(&Utc)
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

#[derive(Clone)]
struct DeterministicContext;

impl CreateContext for DeterministicContext {
    fn new_id(&self) -> String {
        "assigned-memory-id".to_owned()
    }

    fn now(&self) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-09-20T00:00:00Z")
            .expect("fixed timestamp should parse")
            .with_timezone(&Utc)
    }
}

struct ReadyReader {
    bytes: Vec<u8>,
    position: usize,
    hold_open: bool,
}

impl ReadyReader {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            position: 0,
            hold_open: false,
        }
    }

    fn held_open(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            position: 0,
            hold_open: true,
        }
    }
}

impl AsyncRead for ReadyReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let reader = self.as_mut().get_mut();
        let remaining = &reader.bytes[reader.position..];
        if remaining.is_empty() && reader.hold_open {
            // An open input without data never becomes ready; only shutdown can end it.
            return Poll::Pending;
        }
        let count = remaining.len().min(buffer.remaining());
        buffer.put_slice(&remaining[..count]);
        reader.position += count;
        Poll::Ready(Ok(()))
    }
}

struct Signal {
    set: AtomicBool,
    changed: watch::Sender<()>,
}

impl Default for Signal {
    fn default() -> Self {
        let (changed, _) = watch::channel(());
        Self {
            set: AtomicBool::new(false),
            changed,
        }
    }
}

impl Signal {
    fn trigger(&self) {
        if !self.set.swap(true, Ordering::SeqCst) {
            self.changed.send_replace(());
        }
    }

    async fn wait(&self) {
        if self.set.load(Ordering::SeqCst) {
            return;
        }
        let mut changed = self.changed.subscribe();
        if !self.set.load(Ordering::SeqCst) {
            let _ = changed.changed().await;
        }
    }
}

#[derive(Clone)]
struct CaptureWriter {
    state: Arc<Mutex<WriterState>>,
    flush_blocked: Arc<Signal>,
    lines: Arc<watch::Sender<usize>>,
}

struct WriterState {
    bytes: Vec<u8>,
    block_flush: bool,
    flushes: AtomicUsize,
    waker: Option<Waker>,
}

impl CaptureWriter {
    fn open() -> Self {
        Self::new(false)
    }

    fn blocked() -> Self {
        Self::new(true)
    }

    fn new(block_flush: bool) -> Self {
        Self {
            state: Arc::new(Mutex::new(WriterState {
                bytes: Vec::new(),
                block_flush,
                flushes: AtomicUsize::new(0),
                waker: None,
            })),
            flush_blocked: Arc::new(Signal::default()),
            lines: Arc::new(watch::channel(0).0),
        }
    }

    async fn wait_for_lines(&self, count: usize) {
        let mut lines = self.lines.subscribe();
        lines
            .wait_for(|lines| *lines >= count)
            .await
            .expect("the capture writer should keep its line counter open");
    }

    fn bytes(&self) -> Vec<u8> {
        self.state
            .lock()
            .expect("writer state should not be poisoned")
            .bytes
            .clone()
    }

    fn flushes(&self) -> usize {
        self.state
            .lock()
            .expect("writer state should not be poisoned")
            .flushes
            .load(Ordering::SeqCst)
    }

    fn release(&self) {
        let waker = {
            let mut state = self
                .state
                .lock()
                .expect("writer state should not be poisoned");
            state.block_flush = false;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    async fn wait_until_flush_blocked(&self) {
        self.flush_blocked.wait().await;
    }
}

impl AsyncWrite for CaptureWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let writer = self.get_mut();
        writer
            .state
            .lock()
            .expect("writer state should not be poisoned")
            .bytes
            .extend_from_slice(buffer);
        let completed_lines = buffer.iter().filter(|byte| **byte == b'\n').count();
        if completed_lines > 0 {
            writer.lines.send_modify(|lines| *lines += completed_lines);
        }
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let writer = self.get_mut();
        let blocked = {
            let mut state = writer
                .state
                .lock()
                .expect("writer state should not be poisoned");
            state.flushes.fetch_add(1, Ordering::SeqCst);
            if state.block_flush {
                state.waker = Some(context.waker().clone());
                true
            } else {
                false
            }
        };
        if blocked {
            writer.flush_blocked.trigger();
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(context)
    }
}

struct FakeRouterEngine {
    url: String,
    invocations: mpsc::UnboundedReceiver<Value>,
    replies: mpsc::UnboundedSender<Value>,
}

impl FakeRouterEngine {
    async fn start() -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("the fake engine should bind a loopback port");
        let url = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("the fake engine should expose a local address")
        );
        let (invocation_sender, invocations) = mpsc::unbounded_channel();
        let (replies, reply_receiver) = mpsc::unbounded_channel();
        tokio::spawn(serve_fake_engine(
            listener,
            invocation_sender,
            reply_receiver,
        ));

        Self {
            url,
            invocations,
            replies,
        }
    }

    async fn next_invocation(&mut self) -> Value {
        timeout(TEST_BOUND, self.invocations.recv())
            .await
            .expect("the router invocation should reach the fake engine")
            .expect("the fake engine should keep the worker connection open")
    }

    fn reply(&self, message: Value) {
        self.replies
            .send(message)
            .expect("the fake engine should accept a reply");
    }

    async fn assert_disconnected_without_further_invocations(&mut self) {
        if let Some(invocation) = timeout(TEST_BOUND, self.invocations.recv())
            .await
            .expect("the fake engine should observe the worker disconnect")
        {
            panic!(
                "the worker sent another iii invocation to {}",
                invocation["function_id"]
            );
        }
    }
}

async fn serve_fake_engine(
    listener: TcpListener,
    invocations: mpsc::UnboundedSender<Value>,
    mut replies: mpsc::UnboundedReceiver<Value>,
) {
    let mut socket = accept_worker_connection(&listener).await;
    tokio::spawn(discard_later_connections(listener));
    loop {
        tokio::select! {
            frame = socket.next() => {
                let text = match frame {
                    Some(Ok(Message::Text(text))) => text,
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            return;
                        }
                        continue;
                    }
                    Some(Ok(Message::Close(_)) | Err(_)) | None => return,
                    Some(Ok(_)) => continue,
                };
                let frame: Value = serde_json::from_str(text.as_str())
                    .expect("worker iii frames should be JSON");
                if frame["function_id"] == "engine::workers::register" {
                    let registered = json!({
                        "type": "workerregistered",
                        "worker_id": "mcp-worker-runtime-server-engine",
                    });
                    if socket
                        .send(Message::Text(registered.to_string().into()))
                        .await
                        .is_err()
                    {
                        return;
                    }
                } else if invocations.send(frame).is_err() {
                    return;
                }
            }
            reply = replies.recv() => {
                let Some(reply) = reply else {
                    return;
                };
                if socket.send(Message::Text(reply.to_string().into())).await.is_err() {
                    return;
                }
            }
        }
    }
}

async fn accept_worker_connection(listener: &TcpListener) -> WebSocketStream<TcpStream> {
    loop {
        let (stream, _) = timeout(TEST_BOUND, listener.accept())
            .await
            .expect("the worker should connect to the fake engine")
            .expect("the fake engine should accept a connection");
        let mut path = String::new();
        let Ok(socket) = accept_hdr_async(stream, RequestPath(&mut path)).await else {
            continue;
        };

        // The SDK's default telemetry exporter opens a second connection on `/otel`.
        if path == "/otel" {
            tokio::spawn(discard_frames(socket));
        } else {
            return socket;
        }
    }
}

async fn discard_later_connections(listener: TcpListener) {
    // The telemetry connection can also arrive after the worker connection.
    while let Ok((stream, _)) = listener.accept().await {
        if let Ok(socket) = accept_async(stream).await {
            tokio::spawn(discard_frames(socket));
        }
    }
}

struct RequestPath<'path>(&'path mut String);

impl Callback for RequestPath<'_> {
    fn on_request(self, request: &Request, response: Response) -> Result<Response, ErrorResponse> {
        request.uri().path().clone_into(self.0);
        Ok(response)
    }
}

async fn discard_frames(mut socket: WebSocketStream<TcpStream>) {
    while let Some(Ok(_)) = socket.next().await {}
}
