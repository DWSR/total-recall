use std::{
    error::Error,
    time::{Duration, Instant},
};

use futures_util::{SinkExt, StreamExt};
use iii_sdk::{IIIClient, InitOptions, WorkerIdentityMode, runtime::WorkerMetadata};
use mcp_worker::{
    config::{
        Config, III_NAMESPACE_ENV, III_URL_ENV, III_WORKER_NAME_ENV, QueryEmbeddingConfig,
        QueryEmbeddingSettings, TOTAL_RECALL_EMBEDDING_MODEL_ENV,
        TOTAL_RECALL_EMBEDDING_PROVIDER_ENV, TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS_ENV,
        TOTAL_RECALL_MEMORY_DATABASE_ENV,
    },
    embedding::{
        FailingQueryEmbeddingGenerator, QueryEmbeddingError, QueryEmbeddingGenerator,
        RecordingQueryEmbeddingGenerator, RouterQueryEmbeddingGenerator,
    },
};
use memory_store::contracts::EmbeddingVector;
use serde_json::{Value, json};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::timeout,
};
use tokio_tungstenite::{
    WebSocketStream, accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{Callback, ErrorResponse, Request, Response},
    },
};

const QUERY_SENTINEL: &str = " \tquery-secret-sentinel\n ";
const WHITESPACE_QUERY: &str = " \t\n ";
const UNICODE_QUERY: &str = "naïve café ☕ query";
const ROUTED_QUERY_SENTINEL: &str =
    "  \tquery-secret-sentinel na\u{ef}ve cafe\u{301} \u{2615}\r\n ";
const VECTOR_SENTINEL: &[f64] = &[987_654.25, -123_456.5];
const LATE_VECTOR_SENTINEL: &[f64] = &[456_789.75, -654_321.5];
const PROVIDER_SENTINEL: &str = "provider-secret-sentinel";
const MODEL_SENTINEL: &str = "model-secret-sentinel";
const SDK_CODE_SENTINEL: &str = "sdk-code-secret-sentinel";
const SDK_MESSAGE_SENTINEL: &str = "sdk-message-secret-sentinel";
const SDK_STACKTRACE_SENTINEL: &str = "sdk-stacktrace-secret-sentinel";
const PROTECTED_SENTINELS: &[&str] = &[
    "query-secret-sentinel",
    "987654.25",
    "123456.5",
    "456789.75",
    "654321.5",
    PROVIDER_SENTINEL,
    MODEL_SENTINEL,
    SDK_CODE_SENTINEL,
    SDK_MESSAGE_SENTINEL,
    SDK_STACKTRACE_SENTINEL,
];
const FAILURE_CLASSES: [QueryEmbeddingError; 2] = [
    QueryEmbeddingError::Unavailable,
    QueryEmbeddingError::InvalidResponse,
];
const NAMESPACE: &str = "mcp-worker-embedding";
const WORKER_NAME: &str = "mcp-worker-embedding";
const ROUTER_TIMEOUT_MS: u64 = 200;
const ENGINE_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_WINDOW: Duration = Duration::from_millis(300);

#[tokio::test]
async fn recording_generator_returns_the_configured_vector_and_records_the_exact_query() {
    let generator = RecordingQueryEmbeddingGenerator::new(Ok(sentinel_vector()));
    let observer = generator.clone();

    let vector = generator
        .generate(QUERY_SENTINEL)
        .await
        .expect("the configured embedding should be returned");

    assert_eq!(vector.as_slice(), VECTOR_SENTINEL);
    assert_eq!(observer.calls(), vec![QUERY_SENTINEL.to_owned()]);
}

#[tokio::test]
async fn recording_generator_shares_every_exact_query_in_call_order_across_clones() {
    let generator = RecordingQueryEmbeddingGenerator::new(Ok(sentinel_vector()));
    let second_handle = generator.clone();
    let observer = generator.clone();

    assert!(observer.calls().is_empty());
    for (handle, query) in [
        (&generator, QUERY_SENTINEL),
        (&second_handle, WHITESPACE_QUERY),
        (&generator, UNICODE_QUERY),
    ] {
        let vector = handle
            .generate(query)
            .await
            .expect("every call should return the configured embedding");

        assert_eq!(vector.as_slice(), VECTOR_SENTINEL);
    }

    assert_eq!(
        observer.calls(),
        vec![
            QUERY_SENTINEL.to_owned(),
            WHITESPACE_QUERY.to_owned(),
            UNICODE_QUERY.to_owned(),
        ]
    );
}

#[tokio::test]
async fn recording_generator_returns_configured_failures_and_records_the_call() {
    for error in FAILURE_CLASSES {
        let generator = RecordingQueryEmbeddingGenerator::new(Err(error));
        let observer = generator.clone();

        let returned = generator
            .generate(QUERY_SENTINEL)
            .await
            .expect_err("the configured failure should be returned");

        assert_eq!(returned, error);
        assert_error_is_opaque(&returned);
        assert_eq!(observer.calls(), vec![QUERY_SENTINEL.to_owned()]);
    }
}

#[tokio::test]
async fn failing_generator_returns_each_failure_class_and_records_every_call() {
    for error in FAILURE_CLASSES {
        let generator = FailingQueryEmbeddingGenerator::new(error);
        let observer = generator.clone();

        assert!(observer.calls().is_empty());
        for query in [QUERY_SENTINEL, WHITESPACE_QUERY] {
            let returned = generator
                .generate(query)
                .await
                .expect_err("the failing generator should never return an embedding");

            assert_eq!(returned, error);
            assert_error_is_opaque(&returned);
        }

        assert_eq!(
            observer.calls(),
            vec![QUERY_SENTINEL.to_owned(), WHITESPACE_QUERY.to_owned()]
        );
    }
}

#[test]
fn query_embedding_failures_are_distinct_payload_free_and_render_fixed_text() {
    assert_ne!(
        QueryEmbeddingError::Unavailable,
        QueryEmbeddingError::InvalidResponse
    );

    for (error, class, display, debug) in [
        (
            QueryEmbeddingError::Unavailable,
            "unavailable",
            "query_embedding_unavailable",
            "Unavailable",
        ),
        (
            QueryEmbeddingError::InvalidResponse,
            "invalid_response",
            "query_embedding_invalid_response",
            "InvalidResponse",
        ),
    ] {
        let boxed: Box<dyn Error + Send + Sync> = Box::new(error);

        assert_eq!(failure_class(error), class);
        assert_eq!(error.to_string(), display);
        assert_eq!(boxed.to_string(), display);
        assert_eq!(format!("{error:?}"), debug);
        assert_eq!(format!("{error:#?}"), debug);
        assert!(boxed.source().is_none());
        assert_error_is_opaque(&error);
    }
}

#[tokio::test]
async fn moved_generators_keep_call_history_visible_to_retained_observers() {
    let recording = RecordingQueryEmbeddingGenerator::new(Ok(sentinel_vector()));
    let recording_observer = recording.clone();
    let failing = FailingQueryEmbeddingGenerator::new(QueryEmbeddingError::Unavailable);
    let failing_observer = failing.clone();

    let vector = generate_in_spawned_task(recording, QUERY_SENTINEL)
        .await
        .expect("the moved recording generator should return its embedding");
    let error = generate_in_spawned_task(failing, QUERY_SENTINEL)
        .await
        .expect_err("the moved failing generator should return its failure");

    assert_eq!(vector.as_slice(), VECTOR_SENTINEL);
    assert_eq!(error, QueryEmbeddingError::Unavailable);
    assert_eq!(recording_observer.calls(), vec![QUERY_SENTINEL.to_owned()]);
    assert_eq!(failing_observer.calls(), vec![QUERY_SENTINEL.to_owned()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_generator_invokes_router_embed_once_in_the_managed_namespace() {
    let mut engine = FakeRouterEngine::start().await;
    let config = router_config(&engine.url, Some(NAMESPACE), "5000");
    let client = connected_managed_client(&config).await;
    assert_eq!(client.namespace().as_deref(), Some(NAMESPACE));
    let generator = RouterQueryEmbeddingGenerator::new(client.clone(), enabled_settings(&config));

    let response = router_response(
        PROVIDER_SENTINEL,
        MODEL_SENTINEL,
        json!([[987_654.25, -123_456.5, 0.1, 7]]),
    );
    let (result, invocation) = tokio::join!(
        bounded(generator.generate(ROUTED_QUERY_SENTINEL)),
        engine.answer_next(|invocation| invocation_result(invocation, response))
    );

    assert_eq!(invocation["type"], "invokefunction");
    assert_eq!(invocation["function_id"], "router::embed");
    assert_eq!(invocation["namespace"], NAMESPACE);
    assert_eq!(
        invocation["data"],
        json!({
            "input": [ROUTED_QUERY_SENTINEL],
            "provider": PROVIDER_SENTINEL,
            "model": MODEL_SENTINEL,
        })
    );
    assert_eq!(
        invocation["data"]["input"][0].as_str().map(str::as_bytes),
        Some(ROUTED_QUERY_SENTINEL.as_bytes())
    );
    assert!(invocation["invocation_id"].is_string());
    for field in ["action", "metadata", "timeout_ms"] {
        assert!(
            invocation.get(field).is_none(),
            "the router invocation frame should not carry {field}"
        );
    }
    assert_eq!(
        result
            .expect("a valid router response should produce an embedding")
            .as_slice(),
        [987_654.25, -123_456.5, f64::from(0.1_f32), 7.0]
    );
    engine.assert_no_further_invocation().await;
    client.shutdown_async().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_generator_adds_no_namespace_for_a_client_without_one() {
    let mut engine = FakeRouterEngine::start().await;
    let config = router_config(&engine.url, None, "5000");
    let client = iii_sdk::register_worker(
        &engine.url,
        InitOptions {
            metadata: None,
            headers: None,
            otel: None,
            namespace: None,
            identity: WorkerIdentityMode::Explicit,
        },
    );
    await_registration(&client).await;
    assert_eq!(client.namespace(), None);
    let generator = RouterQueryEmbeddingGenerator::new(client.clone(), enabled_settings(&config));

    let response = router_response(PROVIDER_SENTINEL, MODEL_SENTINEL, json!([VECTOR_SENTINEL]));
    let (result, invocation) = tokio::join!(
        bounded(generator.generate(ROUTED_QUERY_SENTINEL)),
        engine.answer_next(|invocation| invocation_result(invocation, response))
    );

    assert_eq!(invocation["function_id"], "router::embed");
    assert!(
        invocation.get("namespace").is_none(),
        "the router invocation should inherit the absent client namespace"
    );
    assert_eq!(
        result
            .expect("a valid router response should produce an embedding")
            .as_slice(),
        VECTOR_SENTINEL
    );
    engine.assert_no_further_invocation().await;
    client.shutdown_async().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_generator_times_out_once_through_the_sdk_and_recovers_for_the_next_query() {
    let mut engine = FakeRouterEngine::start().await;
    let config = router_config(&engine.url, Some(NAMESPACE), &ROUTER_TIMEOUT_MS.to_string());
    let client = connected_managed_client(&config).await;
    let generator = RouterQueryEmbeddingGenerator::new(client.clone(), enabled_settings(&config));

    let started = Instant::now();
    let timed_generation = async {
        let result = bounded(generator.generate(ROUTED_QUERY_SENTINEL)).await;
        (result, started.elapsed())
    };
    let ((result, elapsed), unanswered) = tokio::join!(timed_generation, engine.next_invocation());

    assert_eq!(unanswered["function_id"], "router::embed");
    let error = result.expect_err("an unanswered router call must not produce an embedding");
    assert_eq!(error, QueryEmbeddingError::Unavailable);
    assert_error_is_opaque(&error);
    assert!(
        elapsed >= Duration::from_millis(ROUTER_TIMEOUT_MS),
        "generation returned before the configured SDK timeout: {elapsed:?}"
    );
    engine.assert_no_further_invocation().await;

    engine.reply(invocation_result(
        &unanswered,
        router_response(
            PROVIDER_SENTINEL,
            MODEL_SENTINEL,
            json!([LATE_VECTOR_SENTINEL]),
        ),
    ));
    let response = router_response(PROVIDER_SENTINEL, MODEL_SENTINEL, json!([VECTOR_SENTINEL]));
    let (result, answered) = tokio::join!(
        bounded(generator.generate(ROUTED_QUERY_SENTINEL)),
        engine.answer_next(|invocation| invocation_result(invocation, response))
    );

    assert_eq!(answered["function_id"], "router::embed");
    assert_ne!(answered["invocation_id"], unanswered["invocation_id"]);
    assert_eq!(
        result
            .expect("the next query should use its own router result")
            .as_slice(),
        VECTOR_SENTINEL
    );
    engine.assert_no_further_invocation().await;
    client.shutdown_async().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_generator_maps_a_remote_router_failure_to_unavailable_after_one_invocation() {
    let mut engine = FakeRouterEngine::start().await;
    let config = router_config(&engine.url, Some(NAMESPACE), "5000");
    let client = connected_managed_client(&config).await;
    let generator = RouterQueryEmbeddingGenerator::new(client.clone(), enabled_settings(&config));

    let (result, invocation) = tokio::join!(
        bounded(generator.generate(ROUTED_QUERY_SENTINEL)),
        engine.answer_next(invocation_failure)
    );

    assert_eq!(invocation["function_id"], "router::embed");
    let error = result.expect_err("a remote router failure must not produce an embedding");
    assert_eq!(error, QueryEmbeddingError::Unavailable);
    assert_error_is_opaque(&error);
    engine.assert_no_further_invocation().await;
    client.shutdown_async().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_generator_rejects_invalid_router_results_after_one_invocation_each() {
    let mut engine = FakeRouterEngine::start().await;
    let config = router_config(&engine.url, Some(NAMESPACE), "5000");
    let client = connected_managed_client(&config).await;
    let generator = RouterQueryEmbeddingGenerator::new(client.clone(), enabled_settings(&config));

    for response in [
        router_response("other-provider", MODEL_SENTINEL, json!([VECTOR_SENTINEL])),
        router_response(PROVIDER_SENTINEL, "other-model", json!([VECTOR_SENTINEL])),
        router_response(PROVIDER_SENTINEL, MODEL_SENTINEL, json!([])),
        router_response(
            PROVIDER_SENTINEL,
            MODEL_SENTINEL,
            json!([VECTOR_SENTINEL, VECTOR_SENTINEL]),
        ),
        router_response(
            PROVIDER_SENTINEL,
            MODEL_SENTINEL,
            json!([[987_654.25, 3.5e38]]),
        ),
        router_response(PROVIDER_SENTINEL, MODEL_SENTINEL, json!([[0.0, 1e-50]])),
    ] {
        let (result, invocation) = tokio::join!(
            bounded(generator.generate(ROUTED_QUERY_SENTINEL)),
            engine.answer_next(|invocation| invocation_result(invocation, response))
        );

        assert_eq!(invocation["function_id"], "router::embed");
        let error = result.expect_err("an invalid router result must not produce an embedding");
        assert_eq!(error, QueryEmbeddingError::InvalidResponse);
        assert_error_is_opaque(&error);
        engine.assert_no_further_invocation().await;
    }
    client.shutdown_async().await;
}

fn sentinel_vector() -> EmbeddingVector {
    EmbeddingVector::try_from(VECTOR_SENTINEL.to_vec())
        .expect("the sentinel vector should be a canonical embedding")
}

fn failure_class(error: QueryEmbeddingError) -> &'static str {
    match error {
        QueryEmbeddingError::Unavailable => "unavailable",
        QueryEmbeddingError::InvalidResponse => "invalid_response",
    }
}

async fn generate_in_spawned_task<G>(
    generator: G,
    query: &'static str,
) -> Result<EmbeddingVector, QueryEmbeddingError>
where
    G: QueryEmbeddingGenerator + 'static,
{
    tokio::spawn(async move { generator.generate(query).await })
        .await
        .expect("the generator task should not panic")
}

fn assert_error_is_opaque(error: &QueryEmbeddingError) {
    let renderings = [
        error.to_string(),
        format!("{error:?}"),
        format!("{error:#?}"),
    ];

    for rendering in &renderings {
        for sentinel in PROTECTED_SENTINELS {
            assert!(
                !rendering.contains(sentinel),
                "a query embedding error rendering exposed a protected value"
            );
        }
    }
}

fn router_config(engine_url: &str, namespace: Option<&str>, timeout_ms: &str) -> Config {
    let mut values = vec![
        (
            TOTAL_RECALL_MEMORY_DATABASE_ENV.to_owned(),
            "mcp-worker-embedding".to_owned(),
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
            timeout_ms.to_owned(),
        ),
        (III_URL_ENV.to_owned(), engine_url.to_owned()),
        (III_WORKER_NAME_ENV.to_owned(), WORKER_NAME.to_owned()),
    ];
    if let Some(namespace) = namespace {
        values.push((III_NAMESPACE_ENV.to_owned(), namespace.to_owned()));
    }

    Config::from_values(values).expect("the router test configuration should be valid")
}

fn enabled_settings(config: &Config) -> QueryEmbeddingSettings {
    match &config.query_embedding {
        QueryEmbeddingConfig::Enabled(settings) => settings.clone(),
        QueryEmbeddingConfig::Disabled => {
            panic!("the router test configuration should enable query embedding")
        }
    }
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
    await_registration(&client).await;
    client
}

async fn await_registration(client: &IIIClient) {
    client
        .wait_until_registered(ENGINE_TIMEOUT)
        .await
        .expect("the fake engine should accept the worker registration");
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    timeout(ENGINE_TIMEOUT, future)
        .await
        .expect("query embedding generation should finish within the test bound")
}

fn router_response(provider: &str, model: &str, embeddings: Value) -> Value {
    json!({
        "provider": provider,
        "model": model,
        "embeddings": embeddings,
    })
}

fn invocation_result(invocation: &Value, result: Value) -> Value {
    json!({
        "type": "invocationresult",
        "invocation_id": invocation["invocation_id"],
        "function_id": "router::embed",
        "result": result,
    })
}

fn invocation_failure(invocation: &Value) -> Value {
    json!({
        "type": "invocationresult",
        "invocation_id": invocation["invocation_id"],
        "function_id": "router::embed",
        "error": {
            "code": SDK_CODE_SENTINEL,
            "message": SDK_MESSAGE_SENTINEL,
            "stacktrace": SDK_STACKTRACE_SENTINEL,
        },
    })
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
        timeout(ENGINE_TIMEOUT, self.invocations.recv())
            .await
            .expect("the router invocation should reach the fake engine")
            .expect("the fake engine should keep the worker connection open")
    }

    async fn answer_next(&mut self, answer: impl FnOnce(&Value) -> Value) -> Value {
        let invocation = self.next_invocation().await;
        self.reply(answer(&invocation));
        invocation
    }

    async fn assert_no_further_invocation(&mut self) {
        if let Ok(Some(invocation)) = timeout(RETRY_WINDOW, self.invocations.recv()).await {
            panic!(
                "the generator sent another iii invocation to {}",
                invocation["function_id"]
            );
        }
    }

    fn reply(&self, message: Value) {
        self.replies
            .send(message)
            .expect("the fake engine should accept a reply");
    }
}

async fn serve_fake_engine(
    listener: TcpListener,
    invocations: mpsc::UnboundedSender<Value>,
    mut replies: mpsc::UnboundedReceiver<Value>,
) {
    let mut socket = accept_worker_connection(&listener).await;
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
                        "worker_id": "mcp-worker-embedding-engine",
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
        let (stream, _) = timeout(ENGINE_TIMEOUT, listener.accept())
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
