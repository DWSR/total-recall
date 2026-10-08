use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    io::{self, Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use serde::Serialize;
use serde_json::{Value, json};
use session_post_processing::{
    config::{
        Config, OpenAiConfig, OpenAiOutputMode, ProviderConfig,
        TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV,
        TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV,
        TOTAL_RECALL_POST_PROCESSING_OPENAI_API_TOKEN_ENV,
        TOTAL_RECALL_POST_PROCESSING_OPENAI_BASE_URL_ENV,
        TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL_ENV,
        TOTAL_RECALL_POST_PROCESSING_OPENAI_OUTPUT_MODE_ENV,
        TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV,
    },
    contracts::{
        FailureCategory, ProviderOperation, Retryability, StructuredGenerationRequest,
        StructuredGenerationRequestInput,
    },
    ports::{ModelProvider, ProviderError},
    provider::openai_compatible::OpenAiCompatibleProvider,
};

const API_TOKEN: &str = "openai-token-secret-sentinel";
const PROMPT_SENTINEL: &str = "untrusted-prompt-secret-sentinel";
const RESPONSE_BODY_SENTINEL: &str = "provider-response-secret-sentinel";
const CANDIDATE_CONTENT_SENTINEL: &str = "candidate-content-secret-sentinel";
const RECEIPT_ID_SENTINEL: &str = "receipt-id-secret-sentinel";

#[tokio::test]
async fn openai_provider_sends_closed_chat_completion_requests_for_every_output_mode() {
    let peer = LoopbackPeer::start(vec![
        successful_completion(ProviderOperation::Map),
        successful_completion(ProviderOperation::Reduce),
        successful_completion(ProviderOperation::Reduce)
            .requiring_request(validate_json_object_reduce_request),
        successful_completion(ProviderOperation::Reduce),
    ]);

    let schema_provider = provider(
        &peer,
        OpenAiOutputMode::JsonSchema,
        1,
        Duration::from_secs(1),
    );
    assert_provider_debug_is_safe(&schema_provider);
    let map = schema_provider
        .generate(request(ProviderOperation::Map, "schema-map"))
        .await
        .expect("JSON Schema map response should be accepted");
    assert_eq!(map.concepts(), ["map concept"]);
    let reduce = schema_provider
        .generate(request(ProviderOperation::Reduce, "schema-reduce"))
        .await
        .expect("JSON Schema reduce response should be accepted");
    assert_eq!(reduce.concepts().len(), 10);

    let object_provider = provider(
        &peer,
        OpenAiOutputMode::JsonObject,
        1,
        Duration::from_secs(1),
    );
    let object_reduce = object_provider
        .generate(request(ProviderOperation::Reduce, "object-reduce"))
        .await
        .expect("JSON object reduce response should be accepted");
    assert_eq!(object_reduce.concepts().len(), 10);

    let prompt_provider = provider(&peer, OpenAiOutputMode::Prompt, 1, Duration::from_secs(1));
    prompt_provider
        .generate(request(ProviderOperation::Reduce, "prompt-reduce"))
        .await
        .expect("prompt-only reduce response should be accepted");

    let requests = peer.finish();
    assert_eq!(requests.len(), 4);
    assert_common_request(&requests[0], "schema-map");
    assert_json_schema_request(&requests[0], ProviderOperation::Map);
    assert_common_request(&requests[1], "schema-reduce");
    assert_json_schema_request(&requests[1], ProviderOperation::Reduce);
    assert_common_request(&requests[2], "object-reduce");
    assert_common_request(&requests[3], "prompt-reduce");
    assert!(requests[3].body.get("response_format").is_none());
    let prompt_instruction = requests[3].body["messages"][0]["content"]
        .as_str()
        .expect("prompt-only request should include system instructions");
    assert!(prompt_instruction.contains("summary_sentences"));
    assert!(prompt_instruction.contains("exactly 10"));
}

#[tokio::test]
async fn openai_provider_classifies_refusal_filtering_truncation_and_malformed_responses_as_contract_failures()
 {
    let responses = vec![
        ScriptedResponse::json(
            200,
            json!({
                "choices": [],
            }),
        ),
        ScriptedResponse::json(
            200,
            json!({
                "choices": [{
                    "message": {"content": ""},
                    "finish_reason": "stop",
                }],
            }),
        ),
        ScriptedResponse::json(
            200,
            json!({
                "choices": [{
                    "message": {"content": null, "refusal": RESPONSE_BODY_SENTINEL},
                    "finish_reason": "stop",
                }],
            }),
        ),
        ScriptedResponse::json(
            200,
            json!({
                "choices": [{
                    "message": {"content": null},
                    "finish_reason": "content_filter",
                }],
            }),
        ),
        ScriptedResponse::json(
            200,
            json!({
                "choices": [{
                    "message": {"content": RESPONSE_BODY_SENTINEL},
                    "finish_reason": "length",
                }],
            }),
        ),
        ScriptedResponse::raw(
            200,
            format!("not-json-{RESPONSE_BODY_SENTINEL}").into_bytes(),
        ),
        ScriptedResponse::json(
            200,
            json!({
                "choices": [{
                    "message": {"content": format!("not-json-{RESPONSE_BODY_SENTINEL}")},
                    "finish_reason": "stop",
                }],
            }),
        ),
        ScriptedResponse::json(
            200,
            json!({
                "choices": [{
                    "message": {
                        "content": serde_json::to_string(&json!({
                            "summary_sentences": ["summary"],
                            "concepts": ["concept"],
                            "memory_candidates": [],
                            "unexpected": CANDIDATE_CONTENT_SENTINEL,
                        }))
                        .expect("schema mismatch fixture should serialize"),
                    },
                    "finish_reason": "stop",
                }],
            }),
        ),
        successful_completion_with_concept_count(9),
    ];

    for response in responses {
        let peer = LoopbackPeer::start(vec![response]);
        let error = provider(
            &peer,
            OpenAiOutputMode::JsonSchema,
            1,
            Duration::from_secs(1),
        )
        .generate(request(ProviderOperation::Reduce, "contract-failure"))
        .await
        .expect_err("invalid provider output must fail the closed response contract");

        assert_provider_error(&error, FailureCategory::ProviderContract);
        assert_eq!(peer.finish().len(), 1);
    }
}

#[tokio::test]
async fn openai_provider_classifies_permanent_statuses_without_retrying() {
    let cases = vec![
        (
            ScriptedResponse::json(401, provider_error_body("invalid_api_key")),
            FailureCategory::ProviderAuthentication,
        ),
        (
            ScriptedResponse::json(403, provider_error_body("forbidden")),
            FailureCategory::ProviderAuthentication,
        ),
        (
            ScriptedResponse::json(413, provider_error_body("payload_too_large")),
            FailureCategory::ProviderRequestSize,
        ),
        (
            ScriptedResponse::json(400, provider_error_body("context_length_exceeded")),
            FailureCategory::ProviderRequestSize,
        ),
        (
            ScriptedResponse::json(400, provider_error_body("response_format_not_supported")),
            FailureCategory::ProviderUnsupportedContract,
        ),
    ];

    for (response, category) in cases {
        let peer = LoopbackPeer::start(vec![response]);
        let error = provider(
            &peer,
            OpenAiOutputMode::JsonSchema,
            3,
            Duration::from_secs(1),
        )
        .generate(request(ProviderOperation::Map, "permanent-status"))
        .await
        .expect_err("permanent provider status must fail without retrying");

        assert_provider_error(&error, category);
        assert_eq!(peer.finish().len(), 1);
    }
}

#[tokio::test]
async fn openai_provider_retries_429_and_5xx_with_retry_after_before_returning_a_complete_response()
{
    let peer = LoopbackPeer::start(vec![
        ScriptedResponse::json(429, provider_error_body("context_length_exceeded"))
            .with_header("Retry-After", "1"),
        ScriptedResponse::json(503, provider_error_body("context_length_exceeded")),
        successful_completion(ProviderOperation::Map),
    ]);

    let started = Instant::now();
    let response = provider(
        &peer,
        OpenAiOutputMode::JsonSchema,
        3,
        Duration::from_secs(1),
    )
    .generate(request(ProviderOperation::Map, "retryable-status"))
    .await
    .expect("a valid response after bounded retries should be returned atomically");

    assert_eq!(response.summary_sentences(), ["summary"]);
    assert!(started.elapsed() >= Duration::from_millis(900));
    let requests = peer.finish();
    assert_eq!(requests.len(), 3);
    for request in &requests {
        assert_common_request(request, "retryable-status");
    }
}

#[tokio::test]
async fn openai_provider_retries_transport_failures_and_classifies_timeouts_without_leaking_payloads()
 {
    let transport_peer = LoopbackPeer::start(vec![
        ScriptedResponse::close_connection(),
        ScriptedResponse::close_connection(),
    ]);
    let transport_error = provider(
        &transport_peer,
        OpenAiOutputMode::JsonSchema,
        2,
        Duration::from_secs(1),
    )
    .generate(request(ProviderOperation::Map, "transport-failure"))
    .await
    .expect_err("dropped HTTP connections must become a transient provider failure");
    assert_provider_error(&transport_error, FailureCategory::ProviderTransient);
    assert_eq!(transport_peer.finish().len(), 2);

    let timeout_peer = LoopbackPeer::start(vec![
        successful_completion(ProviderOperation::Map).delayed(Duration::from_millis(100)),
    ]);
    let timeout_error = tokio::time::timeout(
        Duration::from_secs(2),
        provider(
            &timeout_peer,
            OpenAiOutputMode::JsonSchema,
            1,
            Duration::from_millis(25),
        )
        .generate(request(ProviderOperation::Map, "timeout-failure")),
    )
    .await
    .expect("bounded provider timeout retries must not stall the test")
    .expect_err("timed-out HTTP calls must become a transient provider failure");
    assert_provider_error(&timeout_error, FailureCategory::ProviderTransient);
    assert_eq!(timeout_peer.finish().len(), 1);
}

fn provider(
    peer: &LoopbackPeer,
    output_mode: OpenAiOutputMode,
    attempts: u32,
    timeout: Duration,
) -> OpenAiCompatibleProvider {
    OpenAiCompatibleProvider::new(
        openai_config(
            peer.base_url().trim_end_matches('/').to_owned(),
            output_mode,
        ),
        timeout,
        attempts,
        64,
    )
    .expect("loopback OpenAI provider should initialize")
}

fn openai_config(base_url: String, output_mode: OpenAiOutputMode) -> OpenAiConfig {
    let output_mode = match output_mode {
        OpenAiOutputMode::JsonSchema => "json_schema",
        OpenAiOutputMode::JsonObject => "json_object",
        OpenAiOutputMode::Prompt => "prompt",
    };
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
            "openai".to_owned(),
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_OPENAI_BASE_URL_ENV.to_owned(),
            base_url,
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_OPENAI_API_TOKEN_ENV.to_owned(),
            API_TOKEN.to_owned(),
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL_ENV.to_owned(),
            "loopback-model".to_owned(),
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_OPENAI_OUTPUT_MODE_ENV.to_owned(),
            output_mode.to_owned(),
        ),
    ])
    .expect("loopback OpenAI configuration should be valid");

    let ProviderConfig::OpenAi(config) = config.provider else {
        panic!("OpenAI must be selected for the provider fixture");
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

fn successful_completion(operation: ProviderOperation) -> ScriptedResponse {
    let concept_count = match operation {
        ProviderOperation::Map => 1,
        ProviderOperation::Reduce => 10,
    };
    successful_completion_with_concept_count(concept_count)
}

fn successful_completion_with_concept_count(concept_count: usize) -> ScriptedResponse {
    let content = json!({
        "summary_sentences": ["summary"],
        "concepts": (1..=concept_count)
            .map(|index| if index == 1 { "map concept".to_owned() } else { format!("concept {index}") })
            .collect::<Vec<_>>(),
        "memory_candidates": [],
    });
    ScriptedResponse::json(
        200,
        json!({
            "choices": [{
                "message": {
                    "content": serde_json::to_string(&content)
                        .expect("successful response fixture should serialize"),
                },
                "finish_reason": "stop",
            }],
        }),
    )
}

fn provider_error_body(code: &str) -> Value {
    json!({
        "error": {
            "code": code,
            "message": format!(
                "{RESPONSE_BODY_SENTINEL}-{CANDIDATE_CONTENT_SENTINEL}-{RECEIPT_ID_SENTINEL}"
            ),
        },
    })
}

fn assert_common_request(request: &RecordedRequest, suffix: &str) {
    assert!(request.method == "POST");
    assert!(request.path == "/v1/chat/completions");
    assert!(
        request
            .headers
            .get("authorization")
            .is_some_and(|value| value == &format!("Bearer {API_TOKEN}"))
    );
    assert!(
        request
            .headers
            .get("content-type")
            .is_some_and(|value| value.starts_with("application/json"))
    );
    assert!(request.body["model"] == json!("loopback-model"));
    assert!(request.body["max_tokens"] == json!(64));

    let messages = request.body["messages"]
        .as_array()
        .expect("chat completion request should contain messages");
    assert_eq!(messages.len(), 2);
    assert!(messages[0]["role"] == json!("system"));
    assert!(messages[1]["role"] == json!("user"));
    let system = messages[0]["content"]
        .as_str()
        .expect("system message should contain instructions");
    assert!(system.contains("untrusted data"));
    assert!(!system.contains(PROMPT_SENTINEL));
    assert!(messages[1]["content"] == json!(format!("{PROMPT_SENTINEL}-{suffix}")));
}

fn assert_json_schema_request(request: &RecordedRequest, operation: ProviderOperation) {
    let response_format = &request.body["response_format"];
    assert!(response_format["type"] == json!("json_schema"));
    assert!(response_format["json_schema"]["strict"] == json!(true));
    let expected_name = match operation {
        ProviderOperation::Map => "session_post_processing_map",
        ProviderOperation::Reduce => "session_post_processing_reduce",
    };
    assert!(response_format["json_schema"]["name"] == json!(expected_name));
    let schema = &response_format["json_schema"]["schema"];
    assert!(schema["type"] == json!("object"));
    assert!(schema["additionalProperties"] == json!(false));
    assert!(schema["required"] == json!(["summary_sentences", "concepts", "memory_candidates"]));
    assert!(
        schema["properties"]["memory_candidates"]["items"]["additionalProperties"] == json!(false)
    );
    assert!(
        schema["properties"]["memory_candidates"]["items"]["required"]
            == json!(["title", "content", "concepts", "supporting_receipt_ids"])
    );

    let concepts = &schema["properties"]["concepts"];
    match operation {
        ProviderOperation::Map => {
            assert!(concepts.get("minItems").is_none());
            assert!(concepts.get("maxItems").is_none());
        }
        ProviderOperation::Reduce => {
            assert!(concepts["minItems"] == json!(10));
            assert!(concepts["maxItems"] == json!(10));
        }
    }
}

fn validate_json_object_reduce_request(request: &RecordedRequest) -> Result<(), String> {
    if request.body["response_format"] != json!({"type": "json_object"}) {
        return Err(
            "JSON-object request must include the JSON-object response format hint".to_owned(),
        );
    }

    let system_instruction = request.body["messages"]
        .as_array()
        .and_then(|messages| messages.first())
        .and_then(|message| message["content"].as_str())
        .ok_or_else(|| "JSON-object request must include a system instruction".to_owned())?;
    if !system_instruction.contains(
        "Return only one JSON object with exactly summary_sentences, concepts, and memory_candidates fields. Do not add fields or markdown.",
    ) {
        return Err(
            "JSON-object request must require one closed JSON object without markdown".to_owned(),
        );
    }
    if !system_instruction.contains("The concepts field must contain exactly 10 items.") {
        return Err("JSON-object reduce request must require exactly 10 concepts".to_owned());
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

fn assert_provider_debug_is_safe(provider: &OpenAiCompatibleProvider) {
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
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    worker: JoinHandle<Result<(), String>>,
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
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded_requests = Arc::clone(&requests);
        let worker = thread::spawn(move || run_peer(listener, responses, recorded_requests));

        Self {
            base_url: format!("http://{address}/v1/"),
            requests,
            worker,
        }
    }

    fn base_url(&self) -> String {
        self.base_url.clone()
    }

    fn finish(self) -> Vec<RecordedRequest> {
        self.worker
            .join()
            .expect("loopback peer thread should not panic")
            .expect("loopback peer should satisfy its scripted exchange");
        self.requests
            .lock()
            .expect("loopback peer requests should not be poisoned")
            .clone()
    }
}

#[derive(Clone)]
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

    fn close_connection() -> Self {
        Self {
            status: 0,
            headers: Vec::new(),
            body: Vec::new(),
            delay: Duration::ZERO,
            close_connection: true,
            request_validator: None,
        }
    }
}

fn run_peer(
    listener: TcpListener,
    responses: Vec<ScriptedResponse>,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
) -> Result<(), String> {
    for response in responses {
        let mut stream = accept_connection(&listener)?;
        let request = read_request(&mut stream)?;
        if let Some(request_validator) = response.request_validator {
            request_validator(&request)?;
        }
        requests
            .lock()
            .map_err(|_| "loopback request lock poisoned".to_owned())?
            .push(request);

        if response.close_connection {
            let _ = stream.shutdown(Shutdown::Both);
            continue;
        }
        if !response.delay.is_zero() {
            thread::sleep(response.delay);
        }
        write_response(&mut stream, response);
    }

    Ok(())
}

fn accept_connection(listener: &TcpListener) -> Result<TcpStream, String> {
    let deadline = Instant::now() + Duration::from_secs(2);
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

fn write_response(stream: &mut TcpStream, response: ScriptedResponse) {
    let mut headers = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        response.status,
        status_text(response.status),
        response.body.len(),
    );
    for (name, value) in response.headers {
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
