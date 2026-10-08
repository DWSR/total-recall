use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    io::{self, Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use serde::Serialize;
use serde_json::{Value, json};
use session_post_processing::{
    config::{
        AnthropicConfig, Config, ProviderConfig,
        TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_API_TOKEN_ENV,
        TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_BASE_URL_ENV,
        TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_MODEL_ENV,
        TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV,
        TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV,
        TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV,
    },
    contracts::{
        FailureCategory, ProviderOperation, Retryability, StructuredGenerationRequest,
        StructuredGenerationRequestInput,
    },
    ports::{ModelProvider, ProviderError},
    provider::anthropic::AnthropicProvider,
};

const API_TOKEN: &str = "anthropic-token-secret-sentinel";
const PROMPT_SENTINEL: &str = "untrusted-prompt-secret-sentinel";
const RESPONSE_BODY_SENTINEL: &str = "provider-response-secret-sentinel";
const CANDIDATE_CONTENT_SENTINEL: &str = "candidate-content-secret-sentinel";
const RECEIPT_ID_SENTINEL: &str = "receipt-id-secret-sentinel";

#[tokio::test]
async fn anthropic_provider_sends_closed_messages_requests_with_json_schema() {
    let peer = LoopbackPeer::start(vec![
        successful_message(ProviderOperation::Map).requiring_request(validate_map_request),
        successful_message(ProviderOperation::Reduce).requiring_request(validate_reduce_request),
    ]);
    let provider = provider(&peer, 1, Duration::from_secs(1));
    assert_provider_debug_is_safe(&provider);

    let map = provider
        .generate(request(ProviderOperation::Map, "schema-map"))
        .await
        .expect("JSON Schema map response should be accepted");
    assert_eq!(map.concepts(), ["map concept"]);

    let reduce = provider
        .generate(request(ProviderOperation::Reduce, "schema-reduce"))
        .await
        .expect("JSON Schema reduce response should be accepted");
    assert_eq!(reduce.concepts().len(), 10);

    assert_eq!(peer.finish(), 2);
}

#[tokio::test]
async fn anthropic_provider_classifies_refusal_filtering_truncation_and_malformed_responses_as_contract_failures()
 {
    let responses = vec![
        ScriptedResponse::json(
            200,
            json!({"type": "message", "content": [], "stop_reason": "end_turn"}),
        ),
        ScriptedResponse::json(
            200,
            message(Some("end_turn"), json!([{"type": "text", "text": ""}])),
        ),
        ScriptedResponse::json(
            200,
            message(
                Some("refusal"),
                json!([{"type": "text", "text": RESPONSE_BODY_SENTINEL}]),
            ),
        ),
        ScriptedResponse::json(
            200,
            message(
                Some("content_filter"),
                json!([{"type": "text", "text": RESPONSE_BODY_SENTINEL}]),
            ),
        ),
        ScriptedResponse::json(
            200,
            message(
                Some("max_tokens"),
                json!([{"type": "text", "text": RESPONSE_BODY_SENTINEL}]),
            ),
        ),
        ScriptedResponse::raw(
            200,
            format!("not-json-{RESPONSE_BODY_SENTINEL}").into_bytes(),
        ),
        malformed_message_envelope("not_message", "assistant"),
        malformed_message_envelope("message", "user"),
        ScriptedResponse::json(
            200,
            message(
                Some("end_turn"),
                json!([{"type": "thinking", "thinking": RESPONSE_BODY_SENTINEL}]),
            ),
        ),
        ScriptedResponse::json(
            200,
            message(
                Some("end_turn"),
                json!([{"type": "text", "text": format!("not-json-{RESPONSE_BODY_SENTINEL}")}]),
            ),
        ),
        ScriptedResponse::json(
            200,
            message(
                Some("end_turn"),
                json!([{
                    "type": "text",
                    "text": serde_json::to_string(&json!({
                        "summary_sentences": ["summary"],
                        "concepts": ["concept"],
                        "memory_candidates": [],
                        "unexpected": CANDIDATE_CONTENT_SENTINEL,
                    }))
                    .expect("schema mismatch fixture should serialize"),
                }]),
            ),
        ),
        successful_message_with_concept_count(9),
    ];

    for response in responses {
        let peer = LoopbackPeer::start(vec![response]);
        let error = provider(&peer, 1, Duration::from_secs(1))
            .generate(request(ProviderOperation::Reduce, "contract-failure"))
            .await
            .expect_err("invalid provider output must fail the closed response contract");

        assert_provider_error(&error, FailureCategory::ProviderContract);
        assert_eq!(peer.finish(), 1);
    }
}

#[tokio::test]
async fn anthropic_provider_classifies_permanent_statuses_without_retrying() {
    let cases = vec![
        (
            ScriptedResponse::json(401, provider_error_body("authentication_error")),
            FailureCategory::ProviderAuthentication,
        ),
        (
            ScriptedResponse::json(403, provider_error_body("permission_error")),
            FailureCategory::ProviderAuthentication,
        ),
        (
            ScriptedResponse::json(413, provider_error_body("request_too_large")),
            FailureCategory::ProviderRequestSize,
        ),
        (
            ScriptedResponse::json(400, provider_error_body("context_length_exceeded")),
            FailureCategory::ProviderRequestSize,
        ),
        (
            ScriptedResponse::json(400, provider_error_body("output_config_not_supported")),
            FailureCategory::ProviderUnsupportedContract,
        ),
    ];

    for (response, category) in cases {
        let peer = LoopbackPeer::start(vec![response]);
        let error = provider(&peer, 3, Duration::from_secs(1))
            .generate(request(ProviderOperation::Map, "permanent-status"))
            .await
            .expect_err("permanent provider status must fail without retrying");

        assert_provider_error(&error, category);
        assert_eq!(peer.finish(), 1);
    }
}

#[tokio::test]
async fn anthropic_provider_retries_429_and_5xx_with_retry_after_before_returning_a_complete_response()
 {
    let peer = LoopbackPeer::start(vec![
        ScriptedResponse::json(429, provider_error_body("context_length_exceeded"))
            .with_header("Retry-After", "2")
            .requiring_request(validate_map_request),
        ScriptedResponse::json(503, provider_error_body("overloaded_error"))
            .requiring_request(validate_map_request)
            .not_before(Duration::from_millis(1_900)),
        successful_message(ProviderOperation::Map).requiring_request(validate_map_request),
    ]);

    let started = Instant::now();
    let response = provider(&peer, 3, Duration::from_secs(1))
        .generate(request(ProviderOperation::Map, "retryable-status"))
        .await
        .expect("a valid response after bounded retries should be returned atomically");

    assert_eq!(response.summary_sentences(), ["summary"]);
    assert!(started.elapsed() >= Duration::from_millis(1_900));
    assert_eq!(peer.finish(), 3);
}

#[tokio::test]
async fn anthropic_provider_retries_transport_failures_and_classifies_timeouts_without_leaking_payloads()
 {
    let transport_peer = LoopbackPeer::start(vec![
        ScriptedResponse::close_connection().requiring_request(validate_map_request),
        ScriptedResponse::close_connection().requiring_request(validate_map_request),
    ]);
    let transport_error = provider(&transport_peer, 2, Duration::from_secs(1))
        .generate(request(ProviderOperation::Map, "transport-failure"))
        .await
        .expect_err("dropped HTTP connections must become a transient provider failure");
    assert_provider_error(&transport_error, FailureCategory::ProviderTransient);
    assert_eq!(transport_peer.finish(), 2);

    let timeout_peer = LoopbackPeer::start(vec![
        successful_message(ProviderOperation::Map)
            .delayed(Duration::from_millis(100))
            .requiring_request(validate_map_request),
    ]);
    let timeout_error = tokio::time::timeout(
        Duration::from_secs(2),
        provider(&timeout_peer, 1, Duration::from_millis(25))
            .generate(request(ProviderOperation::Map, "timeout-failure")),
    )
    .await
    .expect("bounded provider timeout retries must not stall the test")
    .expect_err("timed-out HTTP calls must become a transient provider failure");
    assert_provider_error(&timeout_error, FailureCategory::ProviderTransient);
    assert_eq!(timeout_peer.finish(), 1);
}

fn provider(peer: &LoopbackPeer, attempts: u32, timeout: Duration) -> AnthropicProvider {
    AnthropicProvider::new(
        anthropic_config(peer.base_url().trim_end_matches('/').to_owned()),
        timeout,
        attempts,
        64,
    )
    .expect("loopback Anthropic provider should initialize")
}

fn anthropic_config(base_url: String) -> AnthropicConfig {
    let config = Config::from_values(vec![
        (
            TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV.to_owned(),
            "source-session".to_owned(),
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV.to_owned(),
            "memory".to_owned(),
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV.to_owned(),
            "anthropic".to_owned(),
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_BASE_URL_ENV.to_owned(),
            base_url,
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_API_TOKEN_ENV.to_owned(),
            API_TOKEN.to_owned(),
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_MODEL_ENV.to_owned(),
            "loopback-model".to_owned(),
        ),
    ])
    .expect("loopback Anthropic configuration should be valid");

    let ProviderConfig::Anthropic(config) = config.provider else {
        panic!("Anthropic must be selected for the provider fixture");
    };
    config
}

fn request(operation: ProviderOperation, suffix: &str) -> StructuredGenerationRequest {
    StructuredGenerationRequest::try_from(StructuredGenerationRequestInput {
        operation,
        prompt: format!("{PROMPT_SENTINEL}-{suffix}"),
    })
    .expect("provider request fixture should be valid")
}

fn successful_message(operation: ProviderOperation) -> ScriptedResponse {
    let concept_count = match operation {
        ProviderOperation::Map => 1,
        ProviderOperation::Reduce => 10,
    };
    successful_message_with_concept_count(concept_count)
}

fn successful_message_with_concept_count(concept_count: usize) -> ScriptedResponse {
    ScriptedResponse::json(
        200,
        successful_message_body_with_concept_count(concept_count),
    )
}

fn malformed_message_envelope(envelope_type: &str, role: &str) -> ScriptedResponse {
    let mut envelope = successful_message_body_with_concept_count(10);
    envelope["type"] = json!(envelope_type);
    envelope["role"] = json!(role);
    ScriptedResponse::json(200, envelope)
}

fn successful_message_body_with_concept_count(concept_count: usize) -> Value {
    let content = json!({
        "summary_sentences": ["summary"],
        "concepts": (1..=concept_count)
            .map(|index| if index == 1 { "map concept".to_owned() } else { format!("concept {index}") })
            .collect::<Vec<_>>(),
        "memory_candidates": [],
    });
    message(
        Some("end_turn"),
        json!([{
            "type": "text",
            "text": serde_json::to_string(&content)
                .expect("successful response fixture should serialize"),
        }]),
    )
}

fn message(stop_reason: Option<&str>, content: Value) -> Value {
    json!({
        "type": "message",
        "role": "assistant",
        "model": "loopback-model",
        "content": content,
        "stop_reason": stop_reason,
        "stop_sequence": null,
        "usage": {"input_tokens": 1, "output_tokens": 1},
    })
}

fn provider_error_body(error_type: &str) -> Value {
    json!({
        "type": "error",
        "error": {
            "type": error_type,
            "message": format!(
                "{RESPONSE_BODY_SENTINEL}-{CANDIDATE_CONTENT_SENTINEL}-{RECEIPT_ID_SENTINEL}"
            ),
        },
    })
}

fn validate_map_request(request: &RecordedRequest) -> Result<(), String> {
    validate_common_request(request)?;
    validate_json_schema(request)
}

fn validate_reduce_request(request: &RecordedRequest) -> Result<(), String> {
    validate_common_request(request)?;
    validate_json_schema(request)
}

fn validate_common_request(request: &RecordedRequest) -> Result<(), String> {
    if request.method != "POST" || request.path != "/v1/messages" {
        return Err("Anthropic request must be POST /v1/messages".to_owned());
    }
    if request.headers.get("x-api-key") != Some(&API_TOKEN.to_owned()) {
        return Err("Anthropic request must use x-api-key authentication".to_owned());
    }
    if request.headers.get("anthropic-version") != Some(&"2023-06-01".to_owned()) {
        return Err("Anthropic request must use the configured API version".to_owned());
    }
    if request.headers.contains_key("authorization") {
        return Err("Anthropic request must not use bearer authentication".to_owned());
    }
    if !request
        .headers
        .get("content-type")
        .is_some_and(|value| value.starts_with("application/json"))
    {
        return Err("Anthropic request must be JSON".to_owned());
    }
    if request.body.as_object().is_none_or(|body| body.len() != 5) {
        return Err("Anthropic request must contain only the expected fields".to_owned());
    }
    if request.body["model"] != json!("loopback-model") || request.body["max_tokens"] != json!(64) {
        return Err(
            "Anthropic request must contain the configured model and output limit".to_owned(),
        );
    }

    let system = request.body["system"]
        .as_str()
        .ok_or_else(|| "Anthropic request must contain system instructions".to_owned())?;
    if !system.contains("untrusted data") || system.contains(PROMPT_SENTINEL) {
        return Err("Anthropic system instructions must delimit untrusted prompt data".to_owned());
    }
    if request.body["messages"]
        .as_array()
        .is_none_or(|messages| messages.len() != 1)
        || request.body["messages"][0]["role"] != json!("user")
        || !request.body["messages"][0]["content"]
            .as_str()
            .is_some_and(|content| content.starts_with(PROMPT_SENTINEL))
    {
        return Err("Anthropic request must send one user prompt message".to_owned());
    }

    Ok(())
}

fn validate_json_schema(request: &RecordedRequest) -> Result<(), String> {
    let output_config = request.body["output_config"]
        .as_object()
        .ok_or_else(|| "Anthropic request must contain output_config".to_owned())?;
    if output_config.len() != 1 {
        return Err("Anthropic output_config must contain only format".to_owned());
    }
    let format = output_config
        .get("format")
        .and_then(Value::as_object)
        .ok_or_else(|| "Anthropic output_config must contain JSON Schema format".to_owned())?;
    if format.len() != 2 || format.get("type") != Some(&json!("json_schema")) {
        return Err("Anthropic output format must be JSON Schema".to_owned());
    }
    let schema = format
        .get("schema")
        .and_then(Value::as_object)
        .ok_or_else(|| "Anthropic JSON Schema output format must include a schema".to_owned())?;
    reject_unsupported_schema_keywords(&Value::Object(schema.clone()))?;
    if schema.get("type") != Some(&json!("object"))
        || schema.get("additionalProperties") != Some(&json!(false))
        || schema.get("required")
            != Some(&json!([
                "summary_sentences",
                "concepts",
                "memory_candidates"
            ]))
    {
        return Err("Anthropic JSON Schema must close the generated response object".to_owned());
    }
    let properties = schema
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| "Anthropic JSON Schema must declare response properties".to_owned())?;
    let candidates = properties
        .get("memory_candidates")
        .and_then(Value::as_object)
        .and_then(|candidates| candidates.get("items"))
        .and_then(Value::as_object)
        .ok_or_else(|| "Anthropic JSON Schema must declare memory candidates".to_owned())?;
    if candidates.get("additionalProperties") != Some(&json!(false))
        || candidates.get("required")
            != Some(&json!([
                "title",
                "content",
                "concepts",
                "supporting_receipt_ids"
            ]))
    {
        return Err("Anthropic JSON Schema must close memory candidates".to_owned());
    }
    let concepts = properties
        .get("concepts")
        .and_then(Value::as_object)
        .ok_or_else(|| "Anthropic JSON Schema must declare concepts".to_owned())?;
    if concepts.get("type") != Some(&json!("array"))
        || concepts
            .get("items")
            .and_then(Value::as_object)
            .and_then(|items| items.get("type"))
            != Some(&json!("string"))
    {
        return Err("Anthropic JSON Schema must constrain concepts to strings".to_owned());
    }
    let summary_sentences = properties
        .get("summary_sentences")
        .and_then(Value::as_object)
        .ok_or_else(|| "Anthropic JSON Schema must declare summary sentences".to_owned())?;
    if summary_sentences.get("type") != Some(&json!("array"))
        || summary_sentences
            .get("items")
            .and_then(Value::as_object)
            .and_then(|items| items.get("type"))
            != Some(&json!("string"))
    {
        return Err("Anthropic JSON Schema must constrain summary sentences to strings".to_owned());
    }

    Ok(())
}

fn reject_unsupported_schema_keywords(schema: &Value) -> Result<(), String> {
    match schema {
        Value::Object(object) => {
            for (keyword, value) in object {
                if matches!(
                    keyword.as_str(),
                    "minLength" | "uniqueItems" | "maxItems" | "minItems"
                ) {
                    return Err(format!("Anthropic JSON Schema must not use {keyword}"));
                }
                reject_unsupported_schema_keywords(value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                reject_unsupported_schema_keywords(value)?;
            }
        }
        _ => {}
    }

    Ok(())
}

fn assert_provider_error(error: &ProviderError, category: FailureCategory) {
    assert_eq!(error.category(), category);
    assert_eq!(error.retryability(), Retryability::AfterBackoff);
    assert_error_is_safe(
        error,
        &[
            API_TOKEN,
            PROMPT_SENTINEL,
            RESPONSE_BODY_SENTINEL,
            CANDIDATE_CONTENT_SENTINEL,
            RECEIPT_ID_SENTINEL,
        ],
    );
}

fn assert_provider_debug_is_safe(provider: &AnthropicProvider) {
    let debug = format!("{provider:?}");
    for sentinel in [API_TOKEN, PROMPT_SENTINEL, RESPONSE_BODY_SENTINEL] {
        assert!(!debug.contains(sentinel));
    }
}

fn assert_error_is_safe<E>(error: &E, sentinels: &[&str])
where
    E: Debug + Display + Serialize,
{
    let display = error.to_string();
    let debug = format!("{error:?}");
    let serialized = serde_json::to_string(error).expect("provider errors should serialize");

    for sentinel in sentinels {
        assert!(!display.contains(sentinel));
        assert!(!debug.contains(sentinel));
        assert!(!serialized.contains(sentinel));
    }
}

struct LoopbackPeer {
    base_url: String,
    worker: JoinHandle<Result<usize, String>>,
}

impl LoopbackPeer {
    fn start(responses: Vec<ScriptedResponse>) -> Self {
        let listener =
            TcpListener::bind(("127.0.0.1", 0)).expect("loopback peer should reserve a local port");
        let address = listener
            .local_addr()
            .expect("loopback peer should have a local address");
        listener
            .set_nonblocking(true)
            .expect("loopback peer should accept connections asynchronously");
        let worker = thread::spawn(move || run_peer(listener, responses));

        Self {
            base_url: format!("http://{address}/v1/"),
            worker,
        }
    }

    fn base_url(&self) -> String {
        self.base_url.clone()
    }

    fn finish(self) -> usize {
        self.worker
            .join()
            .expect("loopback peer thread should not panic")
            .expect("loopback peer should satisfy its scripted exchange")
    }
}

struct RecordedRequest {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Value,
}

type RequestValidator = fn(&RecordedRequest) -> Result<(), String>;

struct ScriptedResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    delay: Duration,
    not_before: Option<Duration>,
    close_connection: bool,
    request_validator: Option<RequestValidator>,
}

impl ScriptedResponse {
    fn json(status: u16, body: Value) -> Self {
        Self::raw(
            status,
            serde_json::to_vec(&body).expect("scripted JSON response should serialize"),
        )
    }

    fn raw(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body,
            delay: Duration::ZERO,
            not_before: None,
            close_connection: false,
            request_validator: None,
        }
    }

    fn requiring_request(mut self, request_validator: RequestValidator) -> Self {
        self.request_validator = Some(request_validator);
        self
    }

    fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }

    fn delayed(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    fn not_before(mut self, delay: Duration) -> Self {
        self.not_before = Some(delay);
        self
    }

    fn close_connection() -> Self {
        Self {
            status: 0,
            headers: Vec::new(),
            body: Vec::new(),
            delay: Duration::ZERO,
            not_before: None,
            close_connection: true,
            request_validator: None,
        }
    }
}

fn run_peer(listener: TcpListener, responses: Vec<ScriptedResponse>) -> Result<usize, String> {
    let mut request_count = 0;
    let mut previous_response_at: Option<Instant> = None;
    for response in responses {
        let mut stream = accept_connection(&listener)?;
        let request = read_request(&mut stream)?;
        if let (Some(not_before), Some(previous_response_at)) =
            (response.not_before, previous_response_at)
            && previous_response_at.elapsed() < not_before
        {
            return Err("loopback provider retried before Retry-After elapsed".to_owned());
        }
        if let Some(request_validator) = response.request_validator {
            request_validator(&request)?;
        }
        request_count += 1;

        if response.close_connection {
            let _ = stream.shutdown(Shutdown::Both);
            continue;
        }
        if !response.delay.is_zero() {
            thread::sleep(response.delay);
        }
        write_response(&mut stream, &response);
        previous_response_at = Some(Instant::now());
    }

    Ok(request_count)
}

fn accept_connection(listener: &TcpListener) -> Result<TcpStream, String> {
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false).map_err(|_| {
                    "loopback provider could not make a connection blocking".to_owned()
                })?;
                return Ok(stream);
            }
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock && Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                return Err("timed out waiting for a loopback provider request".to_owned());
            }
            Err(_) => return Err("loopback provider accept failed".to_owned()),
        }
    }
}

fn read_request(stream: &mut TcpStream) -> Result<RecordedRequest, String> {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|_| "loopback provider could not set a read deadline".to_owned())?;
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut chunk = [0; 1024];
        let count = stream
            .read(&mut chunk)
            .map_err(|_| "loopback provider could not read a request".to_owned())?;
        if count == 0 {
            return Err("loopback provider request ended before headers".to_owned());
        }
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
    };

    let headers_text = std::str::from_utf8(&bytes[..header_end - 4])
        .map_err(|_| "loopback provider request headers were not UTF-8".to_owned())?;
    let mut lines = headers_text.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| "loopback provider request had no request line".to_owned())?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| "loopback provider request had no method".to_owned())?
        .to_owned();
    let path = request_parts
        .next()
        .ok_or_else(|| "loopback provider request had no path".to_owned())?
        .to_owned();
    let mut headers = BTreeMap::new();
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| "loopback provider request had a malformed header".to_owned())?;
        headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
    }
    let content_length = headers
        .get("content-length")
        .ok_or_else(|| "loopback provider request had no content length".to_owned())?
        .parse::<usize>()
        .map_err(|_| "loopback provider request had an invalid content length".to_owned())?;
    while bytes.len() < header_end + content_length {
        let mut chunk = [0; 1024];
        let count = stream
            .read(&mut chunk)
            .map_err(|_| "loopback provider could not finish reading a request".to_owned())?;
        if count == 0 {
            return Err("loopback provider request ended before its body".to_owned());
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    let body = serde_json::from_slice(&bytes[header_end..header_end + content_length])
        .map_err(|_| "loopback provider request body was not JSON".to_owned())?;

    Ok(RecordedRequest {
        method,
        path,
        headers,
        body,
    })
}

fn write_response(stream: &mut TcpStream, response: &ScriptedResponse) {
    let mut headers = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        response.status,
        status_text(response.status),
        response.body.len(),
    );
    for (name, value) in &response.headers {
        headers.push_str(&format!("{name}: {value}\r\n"));
    }
    headers.push_str("\r\n");

    let _ = stream.write_all(headers.as_bytes());
    let _ = stream.write_all(&response.body);
    let _ = stream.flush();
}

fn status_text(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        413 => "Payload Too Large",
        429 => "Too Many Requests",
        503 => "Service Unavailable",
        _ => "Response",
    }
}
