//! Offline model request recorder and pinned host completion controls.
//!
//! The recorder is the offline provider for both pinned OpenCode generations:
//! one authenticated loopback endpoint serves a deterministic streaming
//! completion and rejects unknown route, bearer, model, stream flag, or
//! request count. The completion controls drive exactly one assistant
//! completion per generation through the verified host routes.

use std::{
    collections::HashSet,
    io,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::StreamExt;
use reqwest::{Client, Method, Request, StatusCode, header};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::{Instant, timeout_at},
};

use crate::Generation;

/// Bundled OpenCode provider adapter that both pinned generations already
/// ship; recorder binding values feed it through `opencode.json`.
pub const OPENCODE_BUNDLED_COMPATIBLE_NPM: &str = "@ai-sdk/openai-compatible";
pub const OPENCODE_PROVIDER_ID: &str = "harness-recorder";
pub const OPENCODE_MODEL_ID: &str = "harness-recorder-model";
pub const OPENCODE_PROVIDER_NAME: &str = "harness-recorder";
pub const OPENCODE_MESSAGE_ROUTE: &str = "/v1/chat/completions";
pub const OPENCODE_REPLY_TEXT: &str = "startup-context-recorder-reply";

const RECORDER_BASE_PATH: &str = "/v1";
const RECORDER_METHOD: &[u8] = b"POST";
const MAX_PROVIDER_REQUEST_HEADERS: usize = 16 * 1024;
const MAX_PROVIDER_REQUEST_BODY: usize = 1024 * 1024;
const MAX_REJECTION_DRAIN_BYTES: usize = 4 * 1024 * 1024;
const MAX_API_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_ASSISTANT_TEXT_BYTES: usize = 16 * 1024;
const PROVIDER_IO_DEADLINE: Duration = Duration::from_secs(10);
const COMPLETION_EXECUTION_DEADLINE: Duration = Duration::from_secs(60);
const COMPLETION_POLL_INTERVAL: Duration = Duration::from_millis(50);
const REQUEST_SEPARATOR: &[u8] = b"\r\n\r\n";
const SSE_REPLY_PART_SIZE: usize = 16;
const SSE_CHUNK_ID: &str = "chatcmpl-harness-recorder";
const SSE_CREATED_AT: u64 = 1_759_296_000;
const V1_SESSION_PREFIX: &str = "/session";
const V2_SESSION_PREFIX: &str = "/api/session";
const SYSTEM_ROLE: &str = "system";
const TEXT_PART_TYPE: &str = "text";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfflineModelRecorderRejection {
    Route,
    Bearer,
    Model,
    Stream,
    RequestCount,
    RequestBodyTooLarge,
    RequestHeadersTooLarge,
    MalformedBody,
}

impl OfflineModelRecorderRejection {
    const fn status(self) -> StatusCode {
        match self {
            Self::Route => StatusCode::NOT_FOUND,
            Self::Bearer => StatusCode::UNAUTHORIZED,
            Self::Model | Self::Stream | Self::MalformedBody => StatusCode::BAD_REQUEST,
            Self::RequestCount => StatusCode::TOO_MANY_REQUESTS,
            Self::RequestBodyTooLarge | Self::RequestHeadersTooLarge => {
                StatusCode::PAYLOAD_TOO_LARGE
            }
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Route => "route",
            Self::Bearer => "bearer",
            Self::Model => "model",
            Self::Stream => "stream",
            Self::RequestCount => "request count",
            Self::RequestBodyTooLarge => "request body too large",
            Self::RequestHeadersTooLarge => "request headers too large",
            Self::MalformedBody => "malformed provider request body",
        }
    }
}

impl std::fmt::Display for OfflineModelRecorderRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.label())
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum OfflineModelRecorderError {
    #[error("recorder listener failed ({kind:?})")]
    Listener { kind: io::ErrorKind },
    #[error("recorder endpoint address is unavailable")]
    MissingAddress,
    #[error("recorder connection failed ({kind:?})")]
    Connection { kind: io::ErrorKind },
    #[error("recorder did not serve {expected} completions before the bounded deadline")]
    ServeDeadline { expected: usize },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelRequestRecord {
    body: Vec<u8>,
}

impl ModelRequestRecord {
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn body_len(&self) -> usize {
        self.body.len()
    }

    pub fn contains(&self, needle: &[u8]) -> bool {
        needle.len() <= self.body.len()
            && self
                .body
                .windows(needle.len())
                .any(|window| window == needle)
    }

    pub fn has_non_empty_system_message(&self) -> bool {
        let Ok(body) = serde_json::from_slice::<Value>(&self.body) else {
            return false;
        };
        let Some(messages) = body.get("messages").and_then(Value::as_array) else {
            return false;
        };
        messages.iter().any(|message| {
            message.get("role").and_then(Value::as_str) == Some(SYSTEM_ROLE)
                && system_content_is_non_empty(message.get("content"))
        })
    }
}

fn system_content_is_non_empty(content: Option<&Value>) -> bool {
    match content {
        Some(Value::String(text)) => !text.is_empty(),
        Some(Value::Array(parts)) => !parts.is_empty(),
        _ => false,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfflineProviderBinding {
    base_url: String,
    bearer: String,
}

impl OfflineProviderBinding {
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn model_reference(&self) -> &'static str {
        OPENCODE_MODEL_ID
    }

    pub fn provider_id(&self) -> &'static str {
        OPENCODE_PROVIDER_ID
    }

    pub fn config_entry_value(&self) -> Value {
        json!({
            "name": OPENCODE_PROVIDER_NAME,
            "npm": OPENCODE_BUNDLED_COMPATIBLE_NPM,
            "options": {
                "baseURL": self.base_url,
                "apiKey": self.bearer,
            },
            "models": {OPENCODE_MODEL_ID: {}},
        })
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum OfflineAssistantCompletionError {
    #[error("OpenCode {generation:?} {operation:?} request failed ({kind:?})")]
    Api {
        generation: Generation,
        operation: AssistantOperation,
        kind: AssistantApiFailure,
    },
    #[error("OpenCode {generation:?} {operation:?} request returned HTTP status {status}")]
    HttpStatus {
        generation: Generation,
        operation: AssistantOperation,
        status: u16,
    },
    #[error("OpenCode {generation:?} {operation:?} response was malformed")]
    MalformedResponse {
        generation: Generation,
        operation: AssistantOperation,
    },
    #[error("OpenCode {generation:?} {operation:?} response exceeded the bounded body size")]
    ResponseTooLarge {
        generation: Generation,
        operation: AssistantOperation,
    },
    #[error(
        "OpenCode {generation:?} assistant completion did not finish before the bounded deadline"
    )]
    CompletionTimeout { generation: Generation },
    #[error("OpenCode {generation:?} assistant completion did not finish successfully")]
    CompletionFailed { generation: Generation },
    #[error("OpenCode {generation:?} assistant text exceeded the bounded size")]
    AssistantTextTooLarge { generation: Generation },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssistantOperation {
    Message,
    SetModel,
    Prompt,
    PollMessages,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssistantApiFailure {
    ClientBuild,
    RequestBuild,
    Connection,
    Timeout,
    Transport,
    Body,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssistantCompletion {
    text: String,
}

impl AssistantCompletion {
    pub fn text(&self) -> &str {
        &self.text
    }
}

struct RecorderShared {
    bearer: String,
    allowance: usize,
    served: AtomicUsize,
    records: Mutex<Vec<ModelRequestRecord>>,
    rejections: Mutex<Vec<OfflineModelRecorderRejection>>,
}

impl RecorderShared {
    fn reject(&self, rejection: OfflineModelRecorderRejection) {
        self.rejections
            .lock()
            .expect("recorder rejection list stays reachable")
            .push(rejection);
    }

    fn accept(&self, body: Vec<u8>) {
        self.records
            .lock()
            .expect("recorder record list stays reachable")
            .push(ModelRequestRecord { body });
        self.served.fetch_add(1, Ordering::SeqCst);
    }
}

pub struct OfflineModelRecorder {
    base_url: String,
    binding: OfflineProviderBinding,
    shared: Arc<RecorderShared>,
    listener: Option<JoinHandle<TcpListener>>,
}

impl std::fmt::Debug for OfflineModelRecorder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OfflineModelRecorder")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl OfflineModelRecorder {
    pub async fn start(
        bearer: &str,
        expected_requests: NonZeroUsize,
    ) -> Result<Self, OfflineModelRecorderError> {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .map_err(|error| OfflineModelRecorderError::Listener { kind: error.kind() })?;
        let address = listener
            .local_addr()
            .map_err(|_| OfflineModelRecorderError::MissingAddress)?;
        let base_url = format!("http://{address}{RECORDER_BASE_PATH}");
        let shared = Arc::new(RecorderShared {
            bearer: bearer.to_owned(),
            allowance: expected_requests.get(),
            served: AtomicUsize::new(0),
            records: Mutex::new(Vec::new()),
            rejections: Mutex::new(Vec::new()),
        });
        let serve = tokio::spawn(serve_loop(listener, shared.clone()));
        let binding = OfflineProviderBinding {
            base_url: base_url.clone(),
            bearer: bearer.to_owned(),
        };
        Ok(Self {
            base_url,
            binding,
            shared,
            listener: Some(serve),
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn binding(&self) -> OfflineProviderBinding {
        self.binding.clone()
    }

    pub fn records(&self) -> Vec<ModelRequestRecord> {
        self.shared
            .records
            .lock()
            .expect("recorder record list stays reachable")
            .clone()
    }

    pub fn served_count(&self) -> usize {
        self.shared.served.load(Ordering::SeqCst)
    }

    pub fn rejection_kinds(&self) -> Vec<OfflineModelRecorderRejection> {
        self.shared
            .rejections
            .lock()
            .expect("recorder rejection list stays reachable")
            .clone()
    }

    pub async fn wait_until_served(
        &self,
        expected: usize,
        deadline: Instant,
    ) -> Result<(), OfflineModelRecorderError> {
        loop {
            if self.served_count() >= expected {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(OfflineModelRecorderError::ServeDeadline { expected });
            }
            tokio::time::sleep(remaining.min(COMPLETION_POLL_INTERVAL)).await;
        }
    }

    pub async fn stop(mut self) {
        if let Some(handle) = self.listener.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

impl Drop for OfflineModelRecorder {
    fn drop(&mut self) {
        if let Some(handle) = self.listener.as_ref() {
            handle.abort();
        }
    }
}

async fn serve_loop(listener: TcpListener, shared: Arc<RecorderShared>) -> TcpListener {
    while let Ok((stream, _source)) = listener.accept().await {
        let _ = serve_connection(stream, shared.clone()).await;
    }
    listener
}

enum RequestFail {
    Connection(io::ErrorKind),
    Rejection(OfflineModelRecorderRejection),
}

async fn serve_connection(
    mut stream: TcpStream,
    shared: Arc<RecorderShared>,
) -> Result<(), OfflineModelRecorderError> {
    let deadline = Instant::now() + PROVIDER_IO_DEADLINE;
    let head = match read_request(&mut stream, deadline, &shared.bearer).await {
        Ok(head) => head,
        Err(RequestFail::Rejection(rejection)) => {
            return reject(shared, stream, rejection).await;
        }
        Err(RequestFail::Connection(kind)) => {
            return Err(OfflineModelRecorderError::Connection { kind });
        }
    };
    if head.method != RECORDER_METHOD || head.path != OPENCODE_MESSAGE_ROUTE.as_bytes() {
        return reject(shared, stream, OfflineModelRecorderRejection::Route).await;
    }
    if !head.bearer_ok {
        return reject(shared, stream, OfflineModelRecorderRejection::Bearer).await;
    }
    if head.content_length > MAX_PROVIDER_REQUEST_BODY {
        return reject(
            shared,
            stream,
            OfflineModelRecorderRejection::RequestBodyTooLarge,
        )
        .await;
    }
    let mut body = head.consumed_body;
    while body.len() < head.content_length {
        let read = read_bounded(&mut stream, deadline).await?;
        if read.is_empty() {
            return Err(OfflineModelRecorderError::Connection {
                kind: io::ErrorKind::UnexpectedEof,
            });
        }
        if body.len() + read.len() > head.content_length
            || body.len() + read.len() > MAX_PROVIDER_REQUEST_BODY
        {
            return reject(
                shared,
                stream,
                OfflineModelRecorderRejection::RequestBodyTooLarge,
            )
            .await;
        }
        body.extend_from_slice(&read);
    }
    let Some(parsed) = serde_json::from_slice::<Value>(&body).ok() else {
        return reject(shared, stream, OfflineModelRecorderRejection::MalformedBody).await;
    };
    if parsed.get("model").and_then(Value::as_str) != Some(OPENCODE_MODEL_ID) {
        return reject(shared, stream, OfflineModelRecorderRejection::Model).await;
    }
    if parsed.get("stream") != Some(&Value::Bool(true)) {
        return reject(shared, stream, OfflineModelRecorderRejection::Stream).await;
    }
    if shared.served.load(Ordering::SeqCst) >= shared.allowance {
        return reject(shared, stream, OfflineModelRecorderRejection::RequestCount).await;
    }
    shared.accept(body);
    stream_completion(&mut stream, deadline).await?;
    Ok(())
}

struct RequestHead {
    method: Vec<u8>,
    path: Vec<u8>,
    content_length: usize,
    bearer_ok: bool,
    consumed_body: Vec<u8>,
}

async fn read_request(
    stream: &mut TcpStream,
    deadline: Instant,
    bearer: &str,
) -> Result<RequestHead, RequestFail> {
    let mut buffered: Vec<u8> = Vec::new();
    let mut scratch = [0_u8; 2048];
    let separator = loop {
        if let Some(separator) = find_subsequence(&buffered, REQUEST_SEPARATOR) {
            break separator;
        }
        if buffered.len() >= MAX_PROVIDER_REQUEST_HEADERS {
            return Err(RequestFail::Rejection(
                OfflineModelRecorderRejection::RequestHeadersTooLarge,
            ));
        }
        let read = timeout_at(deadline, stream.read(&mut scratch))
            .await
            .map_err(|_| RequestFail::Connection(io::ErrorKind::TimedOut))?
            .map_err(|error| RequestFail::Connection(error.kind()))?;
        if read == 0 {
            return Err(RequestFail::Connection(io::ErrorKind::UnexpectedEof));
        }
        buffered.extend_from_slice(&scratch[..read]);
    };
    let head_text = String::from_utf8_lossy(&buffered[..separator]).into_owned();
    let mut method = Vec::new();
    let mut path = Vec::new();
    let mut content_length = 0_usize;
    let mut bearer_ok = false;
    let mut lines = head_text.split("\r\n");
    if let Some(request_line) = lines.next() {
        let mut pieces = request_line.split(' ');
        method = pieces.next().unwrap_or_default().as_bytes().to_vec();
        path = pieces.next().unwrap_or_default().as_bytes().to_vec();
    }
    let expected = format!("Bearer {bearer}");
    for line in lines {
        let Some((field, value)) = line.split_once(':') else {
            continue;
        };
        if field.trim().eq_ignore_ascii_case("content-length")
            && let Ok(parsed) = value.trim().parse::<usize>()
        {
            content_length = parsed;
        }
        if field.trim().eq_ignore_ascii_case("authorization") {
            bearer_ok = value.trim() == expected;
        }
    }
    let consumed_body = buffered[separator + REQUEST_SEPARATOR.len()..].to_vec();
    Ok(RequestHead {
        method,
        path,
        content_length,
        bearer_ok,
        consumed_body,
    })
}

async fn read_bounded(
    stream: &mut TcpStream,
    deadline: Instant,
) -> Result<Vec<u8>, OfflineModelRecorderError> {
    let mut scratch = [0_u8; 4096];
    let read = timeout_at(deadline, stream.read(&mut scratch))
        .await
        .map_err(|_| OfflineModelRecorderError::Connection {
            kind: io::ErrorKind::TimedOut,
        })?
        .map_err(|error| OfflineModelRecorderError::Connection { kind: error.kind() })?;
    Ok(scratch[..read].to_vec())
}

fn rejection_response(rejection: OfflineModelRecorderRejection) -> String {
    let status = rejection.status();
    format!(
        "HTTP/1.1 {} {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        status.as_u16(),
        status.canonical_reason().unwrap_or("Rejected"),
    )
}

async fn reject(
    shared: Arc<RecorderShared>,
    mut stream: TcpStream,
    rejection: OfflineModelRecorderRejection,
) -> Result<(), OfflineModelRecorderError> {
    shared.reject(rejection);
    let response = rejection_response(rejection);
    let deadline = Instant::now() + PROVIDER_IO_DEADLINE;
    let _ = timeout_at(deadline, stream.write_all(response.as_bytes())).await;
    let mut drained = 0_usize;
    while drained <= MAX_REJECTION_DRAIN_BYTES {
        let chunk = read_bounded(&mut stream, deadline)
            .await
            .unwrap_or_default();
        if chunk.is_empty() {
            break;
        }
        drained += chunk.len();
    }
    let _ = timeout_at(deadline, stream.shutdown()).await;
    Ok(())
}

async fn stream_completion(
    stream: &mut TcpStream,
    deadline: Instant,
) -> Result<(), OfflineModelRecorderError> {
    let head = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";
    write_bounded(stream, head, deadline).await?;
    for chunk in completion_chunks() {
        let framed = format!("{:x}\r\n{}\r\n", chunk.len(), chunk);
        write_bounded(stream, framed.as_bytes(), deadline).await?;
    }
    write_bounded(stream, b"0\r\n\r\n", deadline).await?;
    let _ = timeout_at(deadline, stream.shutdown()).await;
    Ok(())
}

async fn write_bounded(
    stream: &mut TcpStream,
    bytes: &[u8],
    deadline: Instant,
) -> Result<(), OfflineModelRecorderError> {
    timeout_at(deadline, stream.write_all(bytes))
        .await
        .map_err(|_| OfflineModelRecorderError::Connection {
            kind: io::ErrorKind::TimedOut,
        })?
        .map_err(|error| OfflineModelRecorderError::Connection { kind: error.kind() })?;
    timeout_at(deadline, stream.flush())
        .await
        .map_err(|_| OfflineModelRecorderError::Connection {
            kind: io::ErrorKind::TimedOut,
        })?
        .map_err(|error| OfflineModelRecorderError::Connection { kind: error.kind() })?;
    Ok(())
}

fn completion_chunks() -> Vec<String> {
    let mut parts = Vec::new();
    parts.push(data_chunk(
        json!({"role": "assistant", "content": ""}),
        Value::Null,
    ));
    let mut offset = 0;
    while offset < OPENCODE_REPLY_TEXT.len() {
        let end = (offset + SSE_REPLY_PART_SIZE).min(OPENCODE_REPLY_TEXT.len());
        parts.push(data_chunk(
            json!({"content": &OPENCODE_REPLY_TEXT[offset..end]}),
            Value::Null,
        ));
        offset = end;
    }
    parts.push(data_chunk(json!({}), json!("stop")));
    parts.push(usage_chunk());
    parts.push("data: [DONE]\n\n".to_owned());
    parts
}

fn data_chunk(delta: Value, finish_reason: Value) -> String {
    format!(
        "data: {}\n\n",
        json!({
            "id": SSE_CHUNK_ID,
            "object": "chat.completion.chunk",
            "created": SSE_CREATED_AT,
            "model": OPENCODE_MODEL_ID,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
        })
    )
}

fn usage_chunk() -> String {
    format!(
        "data: {}\n\n",
        json!({
            "id": SSE_CHUNK_ID,
            "object": "chat.completion.chunk",
            "created": SSE_CREATED_AT,
            "model": OPENCODE_MODEL_ID,
            "choices": [],
            "usage": {"prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10},
        })
    )
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RequestCredentials<'a> {
    username: &'a str,
    password: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RequestScope {
    generation: Generation,
    operation: AssistantOperation,
}

fn open_code_request(
    client: &Client,
    method: Method,
    url: String,
    project: &str,
    credentials: RequestCredentials<'_>,
    body: Option<String>,
    scope: RequestScope,
) -> Result<Request, OfflineAssistantCompletionError> {
    let generation = scope.generation;
    let operation = scope.operation;
    let mut builder = client
        .request(method, url)
        .basic_auth(credentials.username, Some(credentials.password))
        .header("x-opencode-directory", project);
    if let Some(body) = body {
        builder = builder
            .header(header::CONTENT_TYPE, "application/json")
            .body(body);
    }
    builder
        .build()
        .map_err(|_| OfflineAssistantCompletionError::Api {
            generation,
            operation,
            kind: AssistantApiFailure::RequestBuild,
        })
}

async fn bounded_operation(
    client: &Client,
    request: Request,
    operation: AssistantOperation,
    generation: Generation,
    deadline: Instant,
) -> Result<(StatusCode, Vec<u8>), OfflineAssistantCompletionError> {
    let response = match timeout_at(deadline, client.execute(request)).await {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            let kind = if error.is_connect() {
                AssistantApiFailure::Connection
            } else if error.is_timeout() {
                AssistantApiFailure::Timeout
            } else {
                AssistantApiFailure::Transport
            };
            return Err(OfflineAssistantCompletionError::Api {
                generation,
                operation,
                kind,
            });
        }
        Err(_) => {
            return Err(OfflineAssistantCompletionError::Api {
                generation,
                operation,
                kind: AssistantApiFailure::Timeout,
            });
        }
    };
    let status = response.status();
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    let drain = async {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| OfflineAssistantCompletionError::Api {
                generation,
                operation,
                kind: AssistantApiFailure::Body,
            })?;
            if bytes.len().saturating_add(chunk.len()) > MAX_API_RESPONSE_BYTES {
                return Err(OfflineAssistantCompletionError::ResponseTooLarge {
                    generation,
                    operation,
                });
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok::<(), OfflineAssistantCompletionError>(())
    };
    match timeout_at(deadline, drain).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return Err(error),
        Err(_) => {
            return Err(OfflineAssistantCompletionError::Api {
                generation,
                operation,
                kind: AssistantApiFailure::Timeout,
            });
        }
    }
    Ok((status, bytes))
}

async fn bounded_json(
    client: &Client,
    request: Request,
    operation: AssistantOperation,
    generation: Generation,
    deadline: Instant,
) -> Result<(StatusCode, Value), OfflineAssistantCompletionError> {
    let (status, bytes) =
        bounded_operation(client, request, operation, generation, deadline).await?;
    let parsed = serde_json::from_slice::<Value>(&bytes).map_err(|_| {
        OfflineAssistantCompletionError::MalformedResponse {
            generation,
            operation,
        }
    })?;
    Ok((status, parsed))
}

fn bounded_text(
    generation: Generation,
    text: String,
) -> Result<String, OfflineAssistantCompletionError> {
    if text.len() > MAX_ASSISTANT_TEXT_BYTES {
        return Err(OfflineAssistantCompletionError::AssistantTextTooLarge { generation });
    }
    Ok(text)
}

fn concat_text_parts(parts: &[Value], field: &str) -> String {
    parts
        .iter()
        .filter(|part| part.get("type").and_then(Value::as_str) == Some(TEXT_PART_TYPE))
        .filter_map(|part| part.get(field).and_then(Value::as_str))
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub async fn drive_v1_message_completion(
    client: &Client,
    base_url: &str,
    project: &str,
    username: &str,
    password: &str,
    session_id: &str,
    binding: &OfflineProviderBinding,
    prompt: &str,
) -> Result<AssistantCompletion, OfflineAssistantCompletionError> {
    let generation = Generation::V1;
    let operation = AssistantOperation::Message;
    let deadline = Instant::now() + COMPLETION_EXECUTION_DEADLINE;
    let url = format!("{base_url}{V1_SESSION_PREFIX}/{session_id}/message");
    let request = open_code_request(
        client,
        Method::POST,
        url,
        project,
        RequestCredentials { username, password },
        Some(
            json!({
                "parts": [{"type": TEXT_PART_TYPE, "text": prompt}],
                "model": {
                    "providerID": binding.provider_id(),
                    "modelID": binding.model_reference(),
                },
            })
            .to_string(),
        ),
        RequestScope {
            generation,
            operation,
        },
    )?;
    let (status, body) = bounded_json(client, request, operation, generation, deadline).await?;
    if status != StatusCode::OK {
        return Err(OfflineAssistantCompletionError::HttpStatus {
            generation,
            operation,
            status: status.as_u16(),
        });
    }
    let role_ok = body
        .get("info")
        .and_then(|info| info.get("role"))
        .and_then(Value::as_str)
        == Some("assistant");
    let completed = body
        .get("info")
        .and_then(|info| info.get("time"))
        .and_then(|time| time.get("completed"))
        .is_some_and(Value::is_number);
    let parts = body.get("parts").and_then(Value::as_array);
    let Some(parts) = parts else {
        return Err(OfflineAssistantCompletionError::MalformedResponse {
            generation,
            operation,
        });
    };
    if !(role_ok && completed) {
        return Err(OfflineAssistantCompletionError::CompletionFailed { generation });
    }
    let text = bounded_text(generation, concat_text_parts(parts, TEXT_PART_TYPE))?;
    if text.is_empty() {
        return Err(OfflineAssistantCompletionError::CompletionFailed { generation });
    }
    Ok(AssistantCompletion { text })
}

#[allow(clippy::too_many_arguments)]
pub async fn drive_v2_prompt_completion(
    client: &Client,
    base_url: &str,
    project: &str,
    username: &str,
    password: &str,
    session_id: &str,
    binding: &OfflineProviderBinding,
    prompt: &str,
) -> Result<AssistantCompletion, OfflineAssistantCompletionError> {
    let generation = Generation::V2;
    let deadline = Instant::now() + COMPLETION_EXECUTION_DEADLINE;
    let credentials = RequestCredentials { username, password };

    let known =
        known_assistant_ids(client, base_url, project, credentials, session_id, deadline).await?;

    let model_url = format!("{base_url}{V2_SESSION_PREFIX}/{session_id}/model");
    let status_check = AssistantOperation::SetModel;
    let model_request = open_code_request(
        client,
        Method::POST,
        model_url,
        project,
        credentials,
        Some(
            json!({
                "model": {
                    "id": binding.model_reference(),
                    "providerID": binding.provider_id(),
                }
            })
            .to_string(),
        ),
        RequestScope {
            generation,
            operation: status_check,
        },
    )?;
    let status = fetch_status(client, model_request, status_check, generation, deadline).await?;
    if status != StatusCode::NO_CONTENT {
        return Err(OfflineAssistantCompletionError::HttpStatus {
            generation,
            operation: status_check,
            status: status.as_u16(),
        });
    }

    let prompt_url = format!("{base_url}{V2_SESSION_PREFIX}/{session_id}/prompt");
    let request = open_code_request(
        client,
        Method::POST,
        prompt_url,
        project,
        credentials,
        Some(
            json!({
                "id": Value::Null,
                "text": prompt,
                "delivery": "queue",
            })
            .to_string(),
        ),
        RequestScope {
            generation,
            operation: AssistantOperation::Prompt,
        },
    )?;
    let (status, _) = bounded_json(
        client,
        request,
        AssistantOperation::Prompt,
        generation,
        deadline,
    )
    .await?;
    if status != StatusCode::OK {
        return Err(OfflineAssistantCompletionError::HttpStatus {
            generation,
            operation: AssistantOperation::Prompt,
            status: status.as_u16(),
        });
    }
    wait_for_v2_completion(
        client,
        base_url,
        project,
        credentials,
        session_id,
        known,
        deadline,
    )
    .await
}

async fn fetch_status(
    client: &Client,
    request: Request,
    operation: AssistantOperation,
    generation: Generation,
    deadline: Instant,
) -> Result<StatusCode, OfflineAssistantCompletionError> {
    match timeout_at(deadline, client.execute(request)).await {
        Ok(Ok(response)) => Ok(response.status()),
        Ok(Err(error)) => {
            let kind = if error.is_connect() {
                AssistantApiFailure::Connection
            } else if error.is_timeout() {
                AssistantApiFailure::Timeout
            } else {
                AssistantApiFailure::Transport
            };
            Err(OfflineAssistantCompletionError::Api {
                generation,
                operation,
                kind,
            })
        }
        Err(_) => Err(OfflineAssistantCompletionError::Api {
            generation,
            operation,
            kind: AssistantApiFailure::Timeout,
        }),
    }
}

fn known_assistant_ids_request(base_url: &str, session_id: &str) -> String {
    format!("{base_url}{V2_SESSION_PREFIX}/{session_id}/message")
}

fn collected_assistant_ids(body: &Value) -> HashSet<String> {
    body.get("data")
        .and_then(Value::as_array)
        .map(|messages| {
            messages
                .iter()
                .filter(|message| message.get("type").and_then(Value::as_str) == Some("assistant"))
                .filter_map(|message| message.get("id").and_then(Value::as_str))
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

async fn known_assistant_ids(
    client: &Client,
    base_url: &str,
    project: &str,
    credentials: RequestCredentials<'_>,
    session_id: &str,
    deadline: Instant,
) -> Result<HashSet<String>, OfflineAssistantCompletionError> {
    let generation = Generation::V2;
    let operation = AssistantOperation::PollMessages;
    let request = open_code_request(
        client,
        Method::GET,
        known_assistant_ids_request(base_url, session_id),
        project,
        credentials,
        None,
        RequestScope {
            generation,
            operation,
        },
    )?;
    let (_, body) = bounded_json(client, request, operation, generation, deadline).await?;
    Ok(collected_assistant_ids(&body))
}

async fn wait_for_v2_completion(
    client: &Client,
    base_url: &str,
    project: &str,
    credentials: RequestCredentials<'_>,
    session_id: &str,
    known: HashSet<String>,
    deadline: Instant,
) -> Result<AssistantCompletion, OfflineAssistantCompletionError> {
    let generation = Generation::V2;
    let operation = AssistantOperation::PollMessages;
    loop {
        let request = open_code_request(
            client,
            Method::GET,
            known_assistant_ids_request(base_url, session_id),
            project,
            credentials,
            None,
            RequestScope {
                generation,
                operation,
            },
        )?;
        let (_, body) = bounded_json(client, request, operation, generation, deadline).await?;
        if let Some(messages) = body.get("data").and_then(Value::as_array) {
            for message in messages {
                if message.get("type").and_then(Value::as_str) != Some("assistant")
                    || known.contains(
                        message
                            .get("id")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                    )
                {
                    continue;
                }
                let completed = message
                    .get("time")
                    .and_then(|time| time.get("completed"))
                    .is_some_and(Value::is_number);
                if !completed {
                    continue;
                }
                let Some(parts) = message.get("content").and_then(Value::as_array) else {
                    return Err(OfflineAssistantCompletionError::MalformedResponse {
                        generation,
                        operation,
                    });
                };
                let text = bounded_text(generation, concat_text_parts(parts, TEXT_PART_TYPE))?;
                if text.is_empty() {
                    return Err(OfflineAssistantCompletionError::CompletionFailed { generation });
                }
                return Ok(AssistantCompletion { text });
            }
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(OfflineAssistantCompletionError::CompletionTimeout { generation });
        }
        tokio::time::sleep(remaining.min(COMPLETION_POLL_INTERVAL)).await;
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use reqwest::{Client, StatusCode, header};
    use serde_json::{Value, json};

    use super::{
        AssistantApiFailure, AssistantOperation, Generation, OPENCODE_BUNDLED_COMPATIBLE_NPM,
        OPENCODE_MODEL_ID, OPENCODE_PROVIDER_ID, OPENCODE_PROVIDER_NAME, OPENCODE_REPLY_TEXT,
        OfflineAssistantCompletionError, OfflineModelRecorder, OfflineModelRecorderError,
        OfflineModelRecorderRejection,
    };

    const UNIT_BEARER: &str = "unit-recorder-bearer";
    const UNIT_SYSTEM: &str = "unit-system-sentinel";
    const UNIT_USER: &str = "unit-user-sentinel";
    const UNIT_WRONG_BEARER: &str = "unit-wrong-bearer";
    const UNIT_MODEL: &str = "someone-elses-model";

    async fn recorder(expected: usize) -> OfflineModelRecorder {
        let allowance = NonZeroUsize::new(expected).expect("positive fixture allowance");
        OfflineModelRecorder::start(UNIT_BEARER, allowance)
            .await
            .expect("recorder starts on loopback")
    }

    fn client() -> Client {
        Client::builder().no_proxy().build().expect("client builds")
    }

    fn completion_body() -> String {
        json!({
            "model": OPENCODE_MODEL_ID,
            "stream": true,
            "messages": [
                {"role": "system", "content": UNIT_SYSTEM},
                {"role": "user", "content": UNIT_USER},
            ],
        })
        .to_string()
    }

    async fn post_completion(
        recorder: &OfflineModelRecorder,
        path: &str,
        bearer: Option<&str>,
        body: String,
    ) -> reqwest::Response {
        let mut request = client()
            .post(format!("{}{}", recorder.base_url(), path))
            .header(header::CONTENT_TYPE, "application/json")
            .body(body);
        if let Some(bearer) = bearer {
            request = request.bearer_auth(bearer);
        }
        request
            .send()
            .await
            .expect("recorder accepts loopback connections")
    }

    async fn empty_body(response: reqwest::Response) -> Vec<u8> {
        response.bytes().await.expect("fixed body drains").to_vec()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recorder_serves_deterministic_streaming_completions_and_records_provider_bytes() {
        let recorder = recorder(2).await;
        let first = post_completion(
            &recorder,
            "/chat/completions",
            Some(UNIT_BEARER),
            completion_body(),
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);
        let content_type = first
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
            .expect("streaming completion advertises its content type");
        assert!(content_type.contains("text/event-stream"));
        let first_body = first.bytes().await.expect("first stream drains").to_vec();

        let second = post_completion(
            &recorder,
            "/chat/completions",
            Some(UNIT_BEARER),
            completion_body(),
        )
        .await;
        assert_eq!(second.status(), StatusCode::OK);
        let second_body = second.bytes().await.expect("second stream drains").to_vec();

        assert_eq!(
            first_body, second_body,
            "streaming replies are deterministic"
        );
        let stream = String::from_utf8(first_body).expect("stream is UTF-8");
        let mut reply = String::new();
        for line in stream.lines().filter(|line| line.starts_with("data: ")) {
            if line == "data: [DONE]" {
                break;
            }
            let parsed: Value =
                serde_json::from_str(&line["data: ".len()..]).expect("stream chunk parses");
            let delta = parsed["choices"][0]["delta"]
                .get("content")
                .and_then(Value::as_str);
            if let Some(delta) = delta {
                reply.push_str(delta);
            }
        }
        assert_eq!(reply, OPENCODE_REPLY_TEXT);
        assert!(stream.trim_end().ends_with("data: [DONE]"));

        let records = recorder.records();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].body(), records[1].body());
        for record in &records {
            let parsed: Value = serde_json::from_slice(record.body()).expect("body parses");
            assert_eq!(parsed["model"], OPENCODE_MODEL_ID);
            assert_eq!(parsed["stream"], true);
            let system = parsed["messages"]
                .as_array()
                .and_then(|messages| messages.first())
                .and_then(|message| message.get("content"))
                .and_then(Value::as_str)
                .expect("system message bytes are recorded");
            assert_eq!(system, UNIT_SYSTEM);
        }
        assert_eq!(recorder.served_count(), 2);
        assert_eq!(
            recorder
                .records()
                .first()
                .map(|record| record.has_non_empty_system_message()),
            Some(true)
        );
        assert!(recorder.rejection_kinds().is_empty());
        assert_eq!(
            recorder.binding().config_entry_value(),
            json!({
                "name": OPENCODE_PROVIDER_NAME,
                "npm": OPENCODE_BUNDLED_COMPATIBLE_NPM,
                "options": {
                    "baseURL": recorder.base_url(),
                    "apiKey": UNIT_BEARER,
                },
                "models": {OPENCODE_MODEL_ID: {}},
            })
        );
        assert_eq!(recorder.binding().provider_id(), OPENCODE_PROVIDER_ID);
        assert_eq!(recorder.binding().base_url(), recorder.base_url());
        recorder.stop().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recorder_rejects_unknown_routes_without_payload_echo() {
        let recorder = recorder(2).await;
        let response = post_completion(&recorder, "/wrong/route", None, completion_body()).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(empty_body(response).await.is_empty());
        assert_eq!(recorder.served_count(), 0);
        assert_eq!(
            recorder.rejection_kinds(),
            vec![OfflineModelRecorderRejection::Route]
        );
        assert!(recorder.records().is_empty());
        recorder.stop().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recorder_rejects_bearer_model_and_stream_mismatches_without_payload_echo() {
        let recorder = recorder(2).await;
        let verifier =
            post_completion(&recorder, "/chat/completions", None, completion_body()).await;
        assert_eq!(verifier.status(), StatusCode::UNAUTHORIZED);
        assert!(empty_body(verifier).await.is_empty());

        let wrong = post_completion(
            &recorder,
            "/chat/completions",
            Some(UNIT_WRONG_BEARER),
            completion_body(),
        )
        .await;
        assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);
        assert!(empty_body(wrong).await.is_empty());

        let mut body_value =
            serde_json::from_str::<Value>(&completion_body()).expect("fixture body parses");
        body_value["model"] = json!(UNIT_MODEL);
        let wrong_model = post_completion(
            &recorder,
            "/chat/completions",
            Some(UNIT_BEARER),
            body_value.to_string(),
        )
        .await;
        assert_eq!(wrong_model.status(), StatusCode::BAD_REQUEST);
        assert!(empty_body(wrong_model).await.is_empty());

        let mut body_value =
            serde_json::from_str::<Value>(&completion_body()).expect("fixture body parses");
        body_value["stream"] = json!(false);
        let non_streaming = post_completion(
            &recorder,
            "/chat/completions",
            Some(UNIT_BEARER),
            body_value.to_string(),
        )
        .await;
        assert_eq!(non_streaming.status(), StatusCode::BAD_REQUEST);
        assert!(empty_body(non_streaming).await.is_empty());

        let parsed = post_completion(
            &recorder,
            "/chat/completions",
            Some(UNIT_BEARER),
            "not-json".to_owned(),
        )
        .await;
        assert_eq!(parsed.status(), StatusCode::BAD_REQUEST);
        assert!(empty_body(parsed).await.is_empty());

        assert_eq!(recorder.served_count(), 0);
        assert_eq!(
            recorder.rejection_kinds(),
            vec![
                OfflineModelRecorderRejection::Bearer,
                OfflineModelRecorderRejection::Bearer,
                OfflineModelRecorderRejection::Model,
                OfflineModelRecorderRejection::Stream,
                OfflineModelRecorderRejection::MalformedBody,
            ]
        );
        assert!(recorder.records().is_empty());
        recorder.stop().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recorder_rejects_requests_that_exceed_the_configured_count() {
        let recorder = recorder(1).await;
        let first = post_completion(
            &recorder,
            "/chat/completions",
            Some(UNIT_BEARER),
            completion_body(),
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);
        let _ = first.bytes().await.expect("first stream drains");

        let extra = post_completion(
            &recorder,
            "/chat/completions",
            Some(UNIT_BEARER),
            completion_body(),
        )
        .await;
        assert_eq!(extra.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(empty_body(extra).await.is_empty());

        assert_eq!(recorder.served_count(), 1);
        assert_eq!(
            recorder.rejection_kinds(),
            vec![OfflineModelRecorderRejection::RequestCount]
        );
        assert_eq!(recorder.records().len(), 1);
        recorder.stop().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn oversized_provider_requests_are_rejected_by_declared_bounds() {
        let recorder = recorder(1).await;
        let body = json!({
            "model": OPENCODE_MODEL_ID,
            "stream": true,
            "messages": [{"role": "system", "content": "a".repeat(2 * 1024 * 1024)}],
        })
        .to_string();
        let oversized =
            post_completion(&recorder, "/chat/completions", Some(UNIT_BEARER), body).await;
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(empty_body(oversized).await.is_empty());
        assert_eq!(recorder.served_count(), 0);
        assert_eq!(
            recorder.rejection_kinds(),
            vec![OfflineModelRecorderRejection::RequestBodyTooLarge]
        );
        recorder.stop().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recorder_outcomes_exclude_bearer_token_and_request_bytes() {
        let recorder = recorder(1).await;
        let _ = post_completion(
            &recorder,
            "/chat/completions",
            Some(UNIT_BEARER),
            completion_body(),
        )
        .await;
        let _ = post_completion(
            &recorder,
            "/chat/completions",
            Some(UNIT_WRONG_BEARER),
            completion_body(),
        )
        .await;

        let error = OfflineAssistantCompletionError::Api {
            generation: Generation::V1,
            operation: AssistantOperation::Message,
            kind: AssistantApiFailure::Transport,
        };
        let deadline_error = OfflineModelRecorderError::ServeDeadline { expected: 1 };
        for diagnostic in [
            error.to_string(),
            format!("{error:?}"),
            deadline_error.to_string(),
            format!("{deadline_error:?}"),
        ] {
            assert!(!diagnostic.contains(UNIT_BEARER));
            assert!(!diagnostic.contains(UNIT_WRONG_BEARER));
            assert!(!diagnostic.contains(UNIT_SYSTEM));
            assert!(!diagnostic.contains(UNIT_USER));
        }
        let status_error = OfflineAssistantCompletionError::HttpStatus {
            generation: Generation::V2,
            operation: AssistantOperation::PollMessages,
            status: 503,
        };
        assert!(status_error.to_string().contains("503"));
        recorder.stop().await;
    }
}
