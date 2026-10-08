use std::{
    future::pending,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use iii_sdk::{Error as IiiError, protocol::TriggerRequest};
use memory_embedding::{
    IiiEmbeddingRouter, RouterExecutor,
    contracts::{
        CanonicalEmbeddingInput, Deadline, EmbeddingError, EmbeddingRouter, MemoryKey,
        RouterFailure,
    },
};
use serde_json::{Value, json};

const FIRST_KEY_SENTINEL: &str = "first-key-sentinel";
const SECOND_KEY_SENTINEL: &str = "second-key-sentinel";
const FIRST_INPUT_SENTINEL: &str = "first-canonical-input-sentinel";
const SECOND_INPUT_SENTINEL: &str = "second-canonical-input-sentinel";
const PROVIDER_SENTINEL: &str = "provider-sentinel";
const MODEL_SENTINEL: &str = "model-sentinel";
const VECTOR_SENTINEL: &str = "vector-sentinel";
const RESPONSE_SENTINEL: &str = "response-sentinel";
const SDK_CODE_SENTINEL: &str = "sdk-code-sentinel";
const SDK_MESSAGE_SENTINEL: &str = "sdk-message-sentinel";
const SDK_STACKTRACE_SENTINEL: &str = "sdk-stacktrace-sentinel";
const EARLIER_DEADLINE: Duration = Duration::from_millis(50);
const OUTER_TIMEOUT: Duration = Duration::from_millis(500);
const LATER_DEADLINE: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct FakeRouterExecutor {
    state: Arc<Mutex<FakeRouterState>>,
}

struct FakeRouterState {
    response: FakeRouterResponse,
    requests: Vec<CapturedRequest>,
}

#[derive(Clone)]
enum FakeRouterResponse {
    Response(Value),
    RemoteFailure,
    Pending,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CapturedRequest {
    function_id: String,
    payload: Value,
    action_is_none: bool,
    timeout_ms: Option<u64>,
}

impl FakeRouterExecutor {
    fn new(response: FakeRouterResponse) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeRouterState {
                response,
                requests: Vec::new(),
            })),
        }
    }

    fn requests(&self) -> Vec<CapturedRequest> {
        self.state
            .lock()
            .expect("test router state lock should not be poisoned")
            .requests
            .clone()
    }
}

#[async_trait]
impl RouterExecutor for FakeRouterExecutor {
    async fn execute(&self, request: TriggerRequest) -> Result<Value, IiiError> {
        let response = {
            let mut state = self
                .state
                .lock()
                .expect("test router state lock should not be poisoned");
            state.requests.push(CapturedRequest {
                function_id: request.function_id,
                payload: request.payload,
                action_is_none: request.action.is_none(),
                timeout_ms: request.timeout_ms,
            });
            state.response.clone()
        };

        match response {
            FakeRouterResponse::Response(response) => Ok(response),
            FakeRouterResponse::RemoteFailure => Err(IiiError::Remote {
                code: SDK_CODE_SENTINEL.to_owned(),
                message: SDK_MESSAGE_SENTINEL.to_owned(),
                stacktrace: Some(SDK_STACKTRACE_SENTINEL.to_owned()),
            }),
            FakeRouterResponse::Pending => pending().await,
        }
    }
}

fn inputs() -> Vec<CanonicalEmbeddingInput> {
    vec![
        CanonicalEmbeddingInput::new(
            MemoryKey::try_new(FIRST_KEY_SENTINEL, "7").expect("test key should be valid"),
            FIRST_INPUT_SENTINEL,
        ),
        CanonicalEmbeddingInput::new(
            MemoryKey::try_new(SECOND_KEY_SENTINEL, "9").expect("test key should be valid"),
            SECOND_INPUT_SENTINEL,
        ),
    ]
}

fn router(
    executor: FakeRouterExecutor,
    timeout: Duration,
) -> IiiEmbeddingRouter<FakeRouterExecutor> {
    IiiEmbeddingRouter::with_executor(executor, PROVIDER_SENTINEL, MODEL_SENTINEL, timeout)
}

fn response(provider: &str, model: &str, embeddings: Value) -> Value {
    json!({
        "provider": provider,
        "model": model,
        "embeddings": embeddings,
    })
}

fn assert_opaque(error: &EmbeddingError) {
    let display = error.to_string();
    let debug = format!("{error:?}");

    for sentinel in [
        FIRST_KEY_SENTINEL,
        SECOND_KEY_SENTINEL,
        FIRST_INPUT_SENTINEL,
        SECOND_INPUT_SENTINEL,
        PROVIDER_SENTINEL,
        MODEL_SENTINEL,
        VECTOR_SENTINEL,
        RESPONSE_SENTINEL,
        SDK_CODE_SENTINEL,
        SDK_MESSAGE_SENTINEL,
        SDK_STACKTRACE_SENTINEL,
    ] {
        assert!(
            !display.contains(sentinel),
            "Display error leaked protected sentinel {sentinel}: {display}"
        );
        assert!(
            !debug.contains(sentinel),
            "Debug error leaked protected sentinel {sentinel}: {debug}"
        );
    }
}

#[tokio::test]
async fn embed_uses_one_exact_router_request_and_positionally_returns_float32_vectors() {
    let executor = FakeRouterExecutor::new(FakeRouterResponse::Response(response(
        PROVIDER_SENTINEL,
        MODEL_SENTINEL,
        json!([[16_777_217.0, -2.5], [3.25]]),
    )));
    let router = router(executor.clone(), Duration::from_secs(1));
    let inputs = inputs();

    let embeddings =
        EmbeddingRouter::embed(&router, &inputs, Deadline::after(Duration::from_secs(1)))
            .await
            .expect("a valid router response should decode");

    assert_eq!(embeddings.len(), 2);
    assert_eq!(embeddings[0].key(), inputs[0].key());
    assert_eq!(embeddings[1].key(), inputs[1].key());
    assert_eq!(embeddings[0].vector(), [16_777_216.0, -2.5]);
    assert_eq!(embeddings[1].vector(), [3.25]);

    assert_eq!(
        executor.requests(),
        vec![CapturedRequest {
            function_id: "router::embed".to_owned(),
            payload: json!({
                "input": [FIRST_INPUT_SENTINEL, SECOND_INPUT_SENTINEL],
                "provider": PROVIDER_SENTINEL,
                "model": MODEL_SENTINEL,
            }),
            action_is_none: true,
            timeout_ms: None,
        }],
        "the adapter must make one exact router request"
    );
}

#[tokio::test]
async fn embed_maps_remote_errors_and_local_timeouts_without_retries_or_leaks() {
    let remote_executor = FakeRouterExecutor::new(FakeRouterResponse::RemoteFailure);
    let remote_router = router(remote_executor.clone(), Duration::from_secs(1));

    let remote_error = EmbeddingRouter::embed(
        &remote_router,
        &inputs(),
        Deadline::after(Duration::from_secs(1)),
    )
    .await
    .expect_err("a router SDK failure must become an opaque error");

    assert_eq!(remote_error, EmbeddingError::router(RouterFailure::Remote));
    assert_eq!(
        remote_executor.requests().len(),
        1,
        "the adapter must not retry"
    );
    assert_opaque(&remote_error);
}

#[tokio::test]
async fn embed_uses_local_timeout_when_it_precedes_absolute_deadline() {
    let executor = FakeRouterExecutor::new(FakeRouterResponse::Pending);
    let adapter = router(executor.clone(), EARLIER_DEADLINE);

    let inner = tokio::time::timeout(
        OUTER_TIMEOUT,
        EmbeddingRouter::embed(&adapter, &inputs(), Deadline::after(LATER_DEADLINE)),
    )
    .await
    .expect("the earlier local timeout must complete before the outer timeout");

    let error = inner.expect_err("the earlier local timeout must stop waiting");

    assert_eq!(error, EmbeddingError::router(RouterFailure::Timeout));
    assert_eq!(executor.requests().len(), 1, "the adapter must not retry");
    assert_opaque(&error);
}

#[tokio::test]
async fn embed_uses_absolute_deadline_when_it_precedes_local_timeout() {
    let executor = FakeRouterExecutor::new(FakeRouterResponse::Pending);
    let adapter = router(executor.clone(), LATER_DEADLINE);

    let inner = tokio::time::timeout(
        OUTER_TIMEOUT,
        EmbeddingRouter::embed(&adapter, &inputs(), Deadline::after(EARLIER_DEADLINE)),
    )
    .await
    .expect("the earlier absolute deadline must complete before the outer timeout");

    let error = inner.expect_err("the earlier absolute deadline must stop waiting");

    assert_eq!(error, EmbeddingError::router(RouterFailure::Timeout));
    assert_eq!(executor.requests().len(), 1, "the adapter must not retry");
    assert_opaque(&error);
}

#[tokio::test]
async fn embed_rejects_expired_or_empty_batches_without_admitting_a_router_call() {
    let expired_executor = FakeRouterExecutor::new(FakeRouterResponse::Response(response(
        PROVIDER_SENTINEL,
        MODEL_SENTINEL,
        json!([[1.0]]),
    )));
    let expired_router = router(expired_executor.clone(), Duration::from_secs(1));

    let expired_error =
        EmbeddingRouter::embed(&expired_router, &inputs(), Deadline::at(Instant::now()))
            .await
            .expect_err("an expired deadline must not admit a router call");

    assert_eq!(
        expired_error,
        EmbeddingError::router(RouterFailure::Timeout)
    );
    assert!(expired_executor.requests().is_empty());
    assert_opaque(&expired_error);

    let empty_executor = FakeRouterExecutor::new(FakeRouterResponse::Response(response(
        PROVIDER_SENTINEL,
        MODEL_SENTINEL,
        json!([]),
    )));
    let empty_router = router(empty_executor.clone(), Duration::from_secs(1));

    let empty_error =
        EmbeddingRouter::embed(&empty_router, &[], Deadline::after(Duration::from_secs(1)))
            .await
            .expect_err("an empty batch must not reach the router");

    assert_eq!(
        empty_error,
        EmbeddingError::router(RouterFailure::CountMismatch)
    );
    assert!(empty_executor.requests().is_empty());
    assert_opaque(&empty_error);
}

#[tokio::test]
async fn embed_rejects_mismatched_counts_and_malformed_response_shapes_without_partial_output() {
    let malformed = vec![
        Value::Null,
        json!({
            "provider": PROVIDER_SENTINEL,
            "model": MODEL_SENTINEL,
            "embeddings": [[1.0], [2.0]],
            "extra": RESPONSE_SENTINEL,
        }),
        json!({
            "provider": PROVIDER_SENTINEL,
            "model": MODEL_SENTINEL,
            "embeddings": RESPONSE_SENTINEL,
        }),
        json!({
            "provider": 7,
            "model": MODEL_SENTINEL,
            "embeddings": [[1.0], [2.0]],
        }),
    ];

    for response in malformed {
        let executor = FakeRouterExecutor::new(FakeRouterResponse::Response(response));
        let adapter = router(executor.clone(), Duration::from_secs(1));

        let error =
            EmbeddingRouter::embed(&adapter, &inputs(), Deadline::after(Duration::from_secs(1)))
                .await
                .expect_err("malformed response data must not produce partial output");

        assert_eq!(error, EmbeddingError::router(RouterFailure::Remote));
        assert_eq!(executor.requests().len(), 1, "the adapter must not retry");
        assert_opaque(&error);
    }

    let count_executor = FakeRouterExecutor::new(FakeRouterResponse::Response(response(
        PROVIDER_SENTINEL,
        MODEL_SENTINEL,
        json!([[1.0]]),
    )));
    let count_router = router(count_executor.clone(), Duration::from_secs(1));

    let count_error = EmbeddingRouter::embed(
        &count_router,
        &inputs(),
        Deadline::after(Duration::from_secs(1)),
    )
    .await
    .expect_err("a wrong vector count must fail as a whole batch");

    assert_eq!(
        count_error,
        EmbeddingError::router(RouterFailure::CountMismatch)
    );
    assert_eq!(
        count_executor.requests().len(),
        1,
        "the adapter must not retry"
    );
    assert_opaque(&count_error);
}

#[tokio::test]
async fn embed_rejects_provider_model_mismatches_and_invalid_vectors_without_retries() {
    for (provider, model, expected) in [
        (
            RESPONSE_SENTINEL,
            MODEL_SENTINEL,
            RouterFailure::ProviderMismatch,
        ),
        (
            PROVIDER_SENTINEL,
            RESPONSE_SENTINEL,
            RouterFailure::ModelMismatch,
        ),
    ] {
        let executor = FakeRouterExecutor::new(FakeRouterResponse::Response(response(
            provider,
            model,
            json!([[1.0], [2.0]]),
        )));
        let adapter = router(executor.clone(), Duration::from_secs(1));

        let error =
            EmbeddingRouter::embed(&adapter, &inputs(), Deadline::after(Duration::from_secs(1)))
                .await
                .expect_err("a mismatched router identity must fail");

        assert_eq!(error, EmbeddingError::router(expected));
        assert_eq!(executor.requests().len(), 1, "the adapter must not retry");
        assert_opaque(&error);
    }

    let oversized_vector = Value::Array(vec![json!(1.0); 16_001]);
    let invalid_vectors = vec![
        json!([[], [2.0]]),
        Value::Array(vec![oversized_vector, json!([2.0])]),
        json!([[VECTOR_SENTINEL], [2.0]]),
        json!([[f64::MAX], [2.0]]),
        json!([[0.0], [-0.0]]),
    ];

    for vectors in invalid_vectors {
        let executor = FakeRouterExecutor::new(FakeRouterResponse::Response(response(
            PROVIDER_SENTINEL,
            MODEL_SENTINEL,
            vectors,
        )));
        let adapter = router(executor.clone(), Duration::from_secs(1));

        let error =
            EmbeddingRouter::embed(&adapter, &inputs(), Deadline::after(Duration::from_secs(1)))
                .await
                .expect_err("invalid vectors must fail as a whole batch");

        assert_eq!(error, EmbeddingError::router(RouterFailure::InvalidVector));
        assert_eq!(executor.requests().len(), 1, "the adapter must not retry");
        assert_opaque(&error);
    }
}
