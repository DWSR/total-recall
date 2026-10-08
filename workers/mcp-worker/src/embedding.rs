use std::{
    fmt,
    future::Future,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use iii_sdk::{IIIClient, protocol::TriggerRequest};
use memory_store::contracts::EmbeddingVector;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::config::QueryEmbeddingSettings;

const ROUTER_EMBED_FUNCTION_ID: &str = "router::embed";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryEmbeddingError {
    Unavailable,
    InvalidResponse,
}

impl fmt::Display for QueryEmbeddingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unavailable => "query_embedding_unavailable",
            Self::InvalidResponse => "query_embedding_invalid_response",
        })
    }
}

impl std::error::Error for QueryEmbeddingError {}

#[async_trait]
pub trait QueryEmbeddingGenerator: Send + Sync {
    async fn generate(&self, query: &str) -> Result<EmbeddingVector, QueryEmbeddingError>;
}

#[derive(Clone)]
pub struct RouterQueryEmbeddingGenerator {
    client: IIIClient,
    settings: QueryEmbeddingSettings,
}

impl RouterQueryEmbeddingGenerator {
    pub fn new(client: IIIClient, settings: QueryEmbeddingSettings) -> Self {
        Self { client, settings }
    }
}

#[async_trait]
impl QueryEmbeddingGenerator for RouterQueryEmbeddingGenerator {
    async fn generate(&self, query: &str) -> Result<EmbeddingVector, QueryEmbeddingError> {
        generate_with(&self.settings, query, |request| {
            self.client.trigger(request)
        })
        .await
    }
}

async fn generate_with<F, Fut>(
    settings: &QueryEmbeddingSettings,
    query: &str,
    invoke: F,
) -> Result<EmbeddingVector, QueryEmbeddingError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    let response = invoke(build_embed_request(settings, query))
        .await
        .map_err(|_| QueryEmbeddingError::Unavailable)?;

    decode_embed_response(settings, response)
}

fn build_embed_request(settings: &QueryEmbeddingSettings, query: &str) -> TriggerRequest {
    TriggerRequest {
        function_id: ROUTER_EMBED_FUNCTION_ID.to_owned(),
        payload: json!({
            "input": [query],
            "provider": settings.provider().as_str(),
            "model": settings.model().as_str(),
        }),
        action: None,
        timeout_ms: Some(settings.timeout_ms()),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RouterEmbedResponse {
    provider: String,
    model: String,
    embeddings: Vec<Vec<f32>>,
}

fn decode_embed_response(
    settings: &QueryEmbeddingSettings,
    response: Value,
) -> Result<EmbeddingVector, QueryEmbeddingError> {
    // Serde also accepts a positional array for a struct; the envelope must be an object.
    if !response.is_object() {
        return Err(QueryEmbeddingError::InvalidResponse);
    }

    let response = serde_json::from_value::<RouterEmbedResponse>(response)
        .map_err(|_| QueryEmbeddingError::InvalidResponse)?;
    if response.provider != settings.provider().as_str()
        || response.model != settings.model().as_str()
    {
        return Err(QueryEmbeddingError::InvalidResponse);
    }

    let Ok([components]) = <[Vec<f32>; 1]>::try_from(response.embeddings) else {
        return Err(QueryEmbeddingError::InvalidResponse);
    };

    EmbeddingVector::try_from(components.into_iter().map(f64::from).collect::<Vec<_>>())
        .map_err(|_| QueryEmbeddingError::InvalidResponse)
}

#[derive(Clone, Default)]
struct QueryHistory {
    queries: Arc<Mutex<Vec<String>>>,
}

impl QueryHistory {
    fn record(&self, query: &str) {
        self.queries
            .lock()
            .expect("query embedding call history lock should not be poisoned")
            .push(query.to_owned());
    }

    fn calls(&self) -> Vec<String> {
        self.queries
            .lock()
            .expect("query embedding call history lock should not be poisoned")
            .clone()
    }
}

#[derive(Clone)]
pub struct RecordingQueryEmbeddingGenerator {
    history: QueryHistory,
    response: Result<EmbeddingVector, QueryEmbeddingError>,
}

impl RecordingQueryEmbeddingGenerator {
    pub fn new(response: Result<EmbeddingVector, QueryEmbeddingError>) -> Self {
        Self {
            history: QueryHistory::default(),
            response,
        }
    }

    pub fn calls(&self) -> Vec<String> {
        self.history.calls()
    }
}

#[async_trait]
impl QueryEmbeddingGenerator for RecordingQueryEmbeddingGenerator {
    async fn generate(&self, query: &str) -> Result<EmbeddingVector, QueryEmbeddingError> {
        self.history.record(query);
        self.response.clone()
    }
}

#[derive(Clone)]
pub struct FailingQueryEmbeddingGenerator {
    history: QueryHistory,
    failure: QueryEmbeddingError,
}

impl FailingQueryEmbeddingGenerator {
    pub fn new(failure: QueryEmbeddingError) -> Self {
        Self {
            history: QueryHistory::default(),
            failure,
        }
    }

    pub fn calls(&self) -> Vec<String> {
        self.history.calls()
    }
}

#[async_trait]
impl QueryEmbeddingGenerator for FailingQueryEmbeddingGenerator {
    async fn generate(&self, query: &str) -> Result<EmbeddingVector, QueryEmbeddingError> {
        self.history.record(query);
        Err(self.failure)
    }
}

#[cfg(test)]
mod tests {
    use iii_sdk::Error as IiiError;
    use serde_json::json;

    use super::*;
    use crate::config::{
        Config, QueryEmbeddingConfig, TOTAL_RECALL_EMBEDDING_MODEL_ENV,
        TOTAL_RECALL_EMBEDDING_PROVIDER_ENV, TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS_ENV,
        TOTAL_RECALL_MEMORY_DATABASE_ENV,
    };

    const PROVIDER_SENTINEL: &str = "provider-secret-sentinel";
    const MODEL_SENTINEL: &str = "model-secret-sentinel";
    const QUERY_SENTINEL: &str = "  \tquery-secret-sentinel na\u{ef}ve cafe\u{301} \u{2615}\r\n ";
    const SDK_CODE_SENTINEL: &str = "sdk-code-secret-sentinel";
    const SDK_MESSAGE_SENTINEL: &str = "sdk-message-secret-sentinel";
    const SDK_STACKTRACE_SENTINEL: &str = "sdk-stacktrace-secret-sentinel";
    const VECTOR_SENTINEL: [f64; 2] = [987_654.25, -123_456.5];
    const PROTECTED_SENTINELS: &[&str] = &[
        PROVIDER_SENTINEL,
        MODEL_SENTINEL,
        "query-secret-sentinel",
        "987654.25",
        "123456.5",
        SDK_CODE_SENTINEL,
        SDK_MESSAGE_SENTINEL,
        SDK_STACKTRACE_SENTINEL,
    ];

    fn settings_with(
        provider: &str,
        model: &str,
        timeout_ms: Option<&str>,
    ) -> QueryEmbeddingSettings {
        let mut values = vec![
            (
                TOTAL_RECALL_MEMORY_DATABASE_ENV.to_owned(),
                "memory-database".to_owned(),
            ),
            (
                TOTAL_RECALL_EMBEDDING_PROVIDER_ENV.to_owned(),
                provider.to_owned(),
            ),
            (
                TOTAL_RECALL_EMBEDDING_MODEL_ENV.to_owned(),
                model.to_owned(),
            ),
        ];
        if let Some(timeout_ms) = timeout_ms {
            values.push((
                TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS_ENV.to_owned(),
                timeout_ms.to_owned(),
            ));
        }

        match Config::from_values(values)
            .expect("test embedding configuration should be valid")
            .query_embedding
        {
            QueryEmbeddingConfig::Enabled(settings) => settings,
            QueryEmbeddingConfig::Disabled => {
                panic!("test embedding configuration should enable query embedding")
            }
        }
    }

    fn settings() -> QueryEmbeddingSettings {
        settings_with(PROVIDER_SENTINEL, MODEL_SENTINEL, Some("1234"))
    }

    fn router_response(embeddings: Value) -> Value {
        router_response_with(PROVIDER_SENTINEL, MODEL_SENTINEL, embeddings)
    }

    fn router_response_with(provider: &str, model: &str, embeddings: Value) -> Value {
        json!({
            "provider": provider,
            "model": model,
            "embeddings": embeddings,
        })
    }

    async fn generate_once(
        settings: &QueryEmbeddingSettings,
        query: &str,
        response: Result<Value, IiiError>,
    ) -> (
        Result<EmbeddingVector, QueryEmbeddingError>,
        Vec<TriggerRequest>,
    ) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&requests);
        let result = generate_with(settings, query, move |request| {
            recorder
                .lock()
                .expect("router request recorder lock should not be poisoned")
                .push(request);
            async move { response }
        })
        .await;
        let requests = requests
            .lock()
            .expect("router request recorder lock should not be poisoned")
            .clone();

        (result, requests)
    }

    fn only_request(requests: &[TriggerRequest]) -> &TriggerRequest {
        match requests {
            [request] => request,
            _ => panic!(
                "generation should make exactly one router invocation, made {}",
                requests.len()
            ),
        }
    }

    async fn assert_rejected_after_one_attempt(case: &str, response: Value) {
        let (result, requests) = generate_once(&settings(), QUERY_SENTINEL, Ok(response)).await;

        assert_eq!(
            requests.len(),
            1,
            "{case}: generation should make exactly one router invocation"
        );
        let error = match result {
            Ok(_) => panic!("{case}: an invalid router response must not produce an embedding"),
            Err(error) => error,
        };
        assert_eq!(error, QueryEmbeddingError::InvalidResponse, "{case}");
        assert_opaque(&error);
    }

    fn assert_opaque(error: &QueryEmbeddingError) {
        for rendering in [
            error.to_string(),
            format!("{error:?}"),
            format!("{error:#?}"),
        ] {
            for sentinel in PROTECTED_SENTINELS {
                assert!(
                    !rendering.contains(sentinel),
                    "a query embedding error rendering exposed a protected value"
                );
            }
        }
    }

    #[tokio::test]
    async fn routed_generation_sends_one_exact_router_embed_request_and_returns_its_vector() {
        let (result, requests) = generate_once(
            &settings(),
            QUERY_SENTINEL,
            Ok(router_response(json!([VECTOR_SENTINEL]))),
        )
        .await;

        let request = only_request(&requests);
        assert_eq!(request.function_id, "router::embed");
        assert_eq!(
            request.payload,
            json!({
                "input": [QUERY_SENTINEL],
                "provider": PROVIDER_SENTINEL,
                "model": MODEL_SENTINEL,
            })
        );
        assert_eq!(
            request.payload["input"][0].as_str().map(str::as_bytes),
            Some(QUERY_SENTINEL.as_bytes())
        );
        assert!(request.action.is_none());
        assert_eq!(request.timeout_ms, Some(1234));
        assert_eq!(
            result
                .expect("a valid router response should produce an embedding")
                .as_slice(),
            VECTOR_SENTINEL
        );
    }

    #[tokio::test]
    async fn routed_generation_sends_every_query_byte_for_byte_as_the_only_input() {
        for query in [
            QUERY_SENTINEL,
            " ",
            " \t\r\n ",
            "na\u{ef}ve cafe\u{301} caf\u{e9} \u{1f600}",
            "\u{7}control\u{1b}[0m\u{7f}",
            "\"quoted\" \\backslash\\ trailing ",
        ] {
            let (result, requests) = generate_once(
                &settings(),
                query,
                Ok(router_response(json!([VECTOR_SENTINEL]))),
            )
            .await;

            let request = only_request(&requests);
            assert_eq!(request.payload["input"], json!([query]));
            assert_eq!(
                request.payload["input"][0].as_str().map(str::as_bytes),
                Some(query.as_bytes())
            );
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn routed_generation_gives_the_configured_timeout_to_the_sdk_request() {
        for (timeout_ms, expected) in [(None, 21_000), (Some("1"), 1), (Some("30000"), 30_000)] {
            let settings = settings_with(PROVIDER_SENTINEL, MODEL_SENTINEL, timeout_ms);

            let (_, requests) = generate_once(
                &settings,
                QUERY_SENTINEL,
                Ok(router_response(json!([VECTOR_SENTINEL]))),
            )
            .await;

            assert_eq!(only_request(&requests).timeout_ms, Some(expected));
        }
    }

    #[tokio::test]
    async fn routed_generation_sends_and_requires_configured_identifiers_verbatim() {
        let provider = "  Provider Sentinel\t";
        let model = " model/\u{fc}n\u{ef}code-sentinel ";
        let settings = settings_with(provider, model, None);

        let (result, requests) = generate_once(
            &settings,
            QUERY_SENTINEL,
            Ok(router_response_with(
                provider,
                model,
                json!([VECTOR_SENTINEL]),
            )),
        )
        .await;

        let request = only_request(&requests);
        assert_eq!(request.payload["provider"], provider);
        assert_eq!(request.payload["model"], model);
        assert_eq!(
            result
                .expect("a response echoing the verbatim identity should be accepted")
                .as_slice(),
            VECTOR_SENTINEL
        );

        for (response_provider, response_model) in
            [(provider.trim(), model), (provider, model.trim())]
        {
            let (result, requests) = generate_once(
                &settings,
                QUERY_SENTINEL,
                Ok(router_response_with(
                    response_provider,
                    response_model,
                    json!([VECTOR_SENTINEL]),
                )),
            )
            .await;

            assert_eq!(requests.len(), 1);
            assert_eq!(result, Err(QueryEmbeddingError::InvalidResponse));
        }
    }

    #[tokio::test]
    async fn routed_generation_decodes_components_as_float32_before_exact_widening() {
        let smallest_subnormal = f64::from(f32::from_bits(1));
        let (result, requests) = generate_once(
            &settings(),
            QUERY_SENTINEL,
            Ok(router_response(json!([[
                0.1,
                -2.5,
                0.100_000_000_1,
                1,
                -7,
                16_777_217,
                f64::from(f32::MAX),
                -f64::from(f32::MAX),
                smallest_subnormal,
            ]]))),
        )
        .await;

        assert_eq!(requests.len(), 1);
        assert_ne!(f64::from(0.1_f32), 0.1);
        assert_eq!(
            result
                .expect("float32-representable components should be accepted")
                .as_slice(),
            [
                f64::from(0.1_f32),
                -2.5,
                f64::from(0.1_f32),
                1.0,
                -7.0,
                16_777_216.0,
                f64::from(f32::MAX),
                -f64::from(f32::MAX),
                smallest_subnormal,
            ]
        );
    }

    #[tokio::test]
    async fn routed_generation_accepts_16000_components_and_rejects_16001_after_one_attempt() {
        let (result, requests) = generate_once(
            &settings(),
            QUERY_SENTINEL,
            Ok(router_response(json!([vec![0.5; 16_000]]))),
        )
        .await;

        assert_eq!(requests.len(), 1);
        let vector = result.expect("a 16000-component vector should be accepted");
        assert_eq!(vector.as_slice().len(), 16_000);
        assert!(vector.as_slice().iter().all(|component| *component == 0.5));

        assert_rejected_after_one_attempt(
            "oversized vector",
            router_response(json!([vec![0.5; 16_001]])),
        )
        .await;
    }

    #[tokio::test]
    async fn routed_generation_rejects_every_identity_mismatch_after_one_attempt() {
        let embeddings = json!([VECTOR_SENTINEL]);
        for (case, provider, model) in [
            ("other provider", "other-provider", MODEL_SENTINEL),
            (
                "provider case variant",
                "PROVIDER-SECRET-SENTINEL",
                MODEL_SENTINEL,
            ),
            (
                "provider leading whitespace",
                " provider-secret-sentinel",
                MODEL_SENTINEL,
            ),
            (
                "provider trailing whitespace",
                "provider-secret-sentinel\n",
                MODEL_SENTINEL,
            ),
            ("empty provider", "", MODEL_SENTINEL),
            ("other model", PROVIDER_SENTINEL, "other-model"),
            (
                "model case variant",
                PROVIDER_SENTINEL,
                "Model-Secret-Sentinel",
            ),
            (
                "model leading whitespace",
                PROVIDER_SENTINEL,
                "\tmodel-secret-sentinel",
            ),
            (
                "model trailing whitespace",
                PROVIDER_SENTINEL,
                "model-secret-sentinel ",
            ),
            ("empty model", PROVIDER_SENTINEL, ""),
            ("swapped identity", MODEL_SENTINEL, PROVIDER_SENTINEL),
        ] {
            assert_rejected_after_one_attempt(
                case,
                router_response_with(provider, model, embeddings.clone()),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn routed_generation_rejects_every_cardinality_mismatch_after_one_attempt() {
        for (case, embeddings) in [
            ("zero vectors", json!([])),
            (
                "two identical vectors",
                json!([VECTOR_SENTINEL, VECTOR_SENTINEL]),
            ),
            (
                "three vectors",
                json!([VECTOR_SENTINEL, [1.0, 2.0], [3.0, 4.0]]),
            ),
        ] {
            assert_rejected_after_one_attempt(case, router_response(embeddings)).await;
        }
    }

    #[tokio::test]
    async fn routed_generation_rejects_every_invalid_vector_class_after_one_attempt() {
        for (case, embeddings) in [
            ("empty vector", json!([[]])),
            ("positive float32 overflow", json!([[3.5e38]])),
            ("negative float32 overflow", json!([[1.0, -3.5e38]])),
            ("float64 maximum", json!([[0.5, f64::MAX]])),
            ("all zero components", json!([[0, 0.0, -0.0]])),
            (
                "components underflowing to zero",
                json!([[1e-50, -1e-50, 1e-46]]),
            ),
            ("string component", json!([[1.0, "2.0"]])),
            ("null component", json!([[1.0, null]])),
            ("boolean component", json!([[true]])),
            ("nested array component", json!([[[0.5]]])),
            ("object component", json!([[{ "value": 0.5 }]])),
            ("numeric entry", json!([0.5])),
            ("string entry", json!(["[0.5]"])),
            ("null entry", json!([null])),
            ("object entry", json!([{ "0": 0.5 }])),
            ("object embeddings", json!({ "0": [0.5] })),
            ("string embeddings", json!("[[0.5]]")),
            ("numeric embeddings", json!(0.5)),
            ("null embeddings", Value::Null),
            ("boolean embeddings", json!(true)),
        ] {
            assert_rejected_after_one_attempt(case, router_response(embeddings)).await;
        }
    }

    #[tokio::test]
    async fn routed_generation_rejects_every_malformed_router_envelope_after_one_attempt() {
        let embeddings = json!([VECTOR_SENTINEL]);
        for (case, response) in [
            ("null envelope", Value::Null),
            (
                "positional array envelope",
                json!([PROVIDER_SENTINEL, MODEL_SENTINEL, embeddings.clone()]),
            ),
            ("string envelope", json!("router-response")),
            ("numeric envelope", json!(7)),
            ("boolean envelope", json!(true)),
            ("empty envelope", json!({})),
            (
                "missing provider",
                json!({ "model": MODEL_SENTINEL, "embeddings": embeddings.clone() }),
            ),
            (
                "missing model",
                json!({ "provider": PROVIDER_SENTINEL, "embeddings": embeddings.clone() }),
            ),
            (
                "missing embeddings",
                json!({ "provider": PROVIDER_SENTINEL, "model": MODEL_SENTINEL }),
            ),
            (
                "extra field",
                json!({
                    "provider": PROVIDER_SENTINEL,
                    "model": MODEL_SENTINEL,
                    "embeddings": embeddings.clone(),
                    "usage": { "tokens": 3 },
                }),
            ),
            (
                "misspelled field",
                json!({
                    "Provider": PROVIDER_SENTINEL,
                    "model": MODEL_SENTINEL,
                    "embeddings": embeddings.clone(),
                }),
            ),
            (
                "numeric provider",
                json!({ "provider": 7, "model": MODEL_SENTINEL, "embeddings": embeddings.clone() }),
            ),
            (
                "null provider",
                json!({ "provider": null, "model": MODEL_SENTINEL, "embeddings": embeddings.clone() }),
            ),
            (
                "array provider",
                json!({
                    "provider": [PROVIDER_SENTINEL],
                    "model": MODEL_SENTINEL,
                    "embeddings": embeddings.clone(),
                }),
            ),
            (
                "object model",
                json!({
                    "provider": PROVIDER_SENTINEL,
                    "model": { "id": MODEL_SENTINEL },
                    "embeddings": embeddings.clone(),
                }),
            ),
            (
                "boolean model",
                json!({ "provider": PROVIDER_SENTINEL, "model": false, "embeddings": embeddings.clone() }),
            ),
        ] {
            assert_rejected_after_one_attempt(case, response).await;
        }
    }

    #[tokio::test]
    async fn routed_generation_maps_every_sdk_failure_to_unavailable_after_one_attempt() {
        for failure in [
            IiiError::NotConnected,
            IiiError::Timeout,
            IiiError::Runtime(SDK_MESSAGE_SENTINEL.to_owned()),
            IiiError::Remote {
                code: SDK_CODE_SENTINEL.to_owned(),
                message: SDK_MESSAGE_SENTINEL.to_owned(),
                stacktrace: Some(SDK_STACKTRACE_SENTINEL.to_owned()),
            },
            IiiError::Handler(SDK_MESSAGE_SENTINEL.to_owned()),
            IiiError::Serde(SDK_MESSAGE_SENTINEL.to_owned()),
            IiiError::WebSocket(SDK_MESSAGE_SENTINEL.to_owned()),
            IiiError::RegistrationRejected {
                code: SDK_CODE_SENTINEL.to_owned(),
                namespace: SDK_MESSAGE_SENTINEL.to_owned(),
                worker_name: Some(SDK_MESSAGE_SENTINEL.to_owned()),
                function_id: Some("router::embed".to_owned()),
                owner_worker_id: SDK_STACKTRACE_SENTINEL.to_owned(),
            },
        ] {
            let (result, requests) = generate_once(&settings(), QUERY_SENTINEL, Err(failure)).await;

            assert_eq!(
                requests.len(),
                1,
                "generation should make exactly one router invocation"
            );
            let error = match result {
                Ok(_) => panic!("an SDK failure must not produce an embedding"),
                Err(error) => error,
            };
            assert_eq!(error, QueryEmbeddingError::Unavailable);
            assert_opaque(&error);
        }
    }
}
