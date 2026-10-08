use std::{
    env,
    error::Error,
    fs, io,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use harness_event_persistence::{
    contracts::QueuedHarnessEventInput, persistence::PersistenceResponse,
};
use schemars::{JsonSchema, r#gen::SchemaSettings};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, BufReader},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    sync::mpsc,
    time::{Instant, timeout},
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
use uuid::Uuid;

const PERSIST_EVENT_FUNCTION_ID: &str = "harness::persist_event";
const DURABLE_SUBSCRIBER_TRIGGER_TYPE: &str = "durable:subscriber";
const WORKER_REGISTER_FUNCTION: &str = "engine::workers::register";
const FUNCTIONS_INFO_FUNCTION: &str = "engine::functions::info";
const REGISTERED_TRIGGERS_LIST_FUNCTION: &str = "engine::registered-triggers::list";
const REGISTERED_TRIGGERS_INFO_FUNCTION: &str = "engine::registered-triggers::info";
const DATABASE_EXECUTE_FUNCTION: &str = "database::execute";
const ENGINE_NAMESPACE: &str = "default";
const WORKER_NAME: &str = "harness-event-persistence-ci";
const NAMESPACE: &str = "harness-persistence-smoke";
const QUEUE_TOPIC: &str = "harness-persistence-smoke";
const DATABASE: &str = "harness-persistence-ledger";
const OBSERVATION_TIMESTAMP: &str = "2026-09-18t12:35:00.123+00:00";
const OBSERVATION_EPOCH_MILLIS: i64 = 1_789_734_900_123;
const OPAQUE_SENTINEL: &str = "persistence-opaque-observation-sentinel";
const DATABASE_ACK_GUARD: Duration = Duration::from_millis(50);
const STDERR_CAPTURE_LIMIT: usize = 64 * 1024;
const READINESS_PENDING: u8 = 0;
const READINESS_CATALOG_REPLY_SENT: u8 = 1;

type BoxError = Box<dyn Error + Send + Sync>;
type SmokeResult<T> = Result<T, BoxError>;

fn persist_event_function_schemas() -> SmokeResult<(Value, Value)> {
    Ok((
        sdk_function_schema::<QueuedHarnessEventInput>()?,
        sdk_function_schema::<PersistenceResponse>()?,
    ))
}

fn sdk_function_schema<T: JsonSchema>() -> SmokeResult<Value> {
    // Matches iii-sdk 0.24's RegisterFunction::new_async schema serialization.
    serde_json::to_value(
        SchemaSettings::draft07()
            .into_generator()
            .into_root_schema_for::<T>(),
    )
    .map_err(|_| failure("failed to derive typed function schema"))
}

fn validate_persist_event_function_schemas(message: &Value) -> SmokeResult<()> {
    let (request_format, response_format) = persist_event_function_schemas()?;
    validate_function_schema(message, "request_format", &request_format)?;
    validate_function_schema(message, "response_format", &response_format)
}

fn validate_function_schema(
    message: &Value,
    field: &str,
    expected_schema: &Value,
) -> SmokeResult<()> {
    let schema = message
        .get(field)
        .ok_or_else(|| failure(format!("function registration did not include {field}")))?;
    if !schema.is_object() {
        return Err(failure(format!(
            "function registration {field} was not a JSON object"
        )));
    }
    if schema != expected_schema {
        return Err(failure(format!(
            "function registration {field} did not match the persistence contract"
        )));
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct Arguments {
    manifest: PathBuf,
    timeout: Duration,
}

#[derive(Debug, PartialEq, Eq)]
struct WorkerLaunch {
    working_directory: PathBuf,
    start: String,
}

#[derive(Deserialize)]
struct WorkerManifest {
    scripts: Option<WorkerScripts>,
}

#[derive(Deserialize)]
struct WorkerScripts {
    start: Option<String>,
}

#[derive(Default)]
struct RegistrationState {
    function_nonce: Option<String>,
    trigger: Option<TriggerRegistration>,
}

struct TriggerRegistration {
    id: String,
    nonce: String,
}

impl RegistrationState {
    fn record_function(&mut self, message: &Value) -> SmokeResult<()> {
        if self.function_nonce.is_some() {
            return Err(failure(
                "worker registered harness::persist_event more than once",
            ));
        }
        if message.get("id").and_then(Value::as_str) != Some(PERSIST_EVENT_FUNCTION_ID) {
            return Err(failure("worker registered an unexpected function"));
        }
        validate_persist_event_function_schemas(message)?;

        self.function_nonce = Some(registration_nonce(message.get("metadata"))?);
        Ok(())
    }

    fn record_trigger(&mut self, message: &Value) -> SmokeResult<()> {
        if self.trigger.is_some() {
            return Err(failure(
                "worker registered a durable subscriber more than once",
            ));
        }
        if message.get("trigger_type").and_then(Value::as_str)
            != Some(DURABLE_SUBSCRIBER_TRIGGER_TYPE)
        {
            return Err(failure("worker registered an unexpected trigger type"));
        }
        if message.get("function_id").and_then(Value::as_str) != Some(PERSIST_EVENT_FUNCTION_ID) {
            return Err(failure(
                "worker registered a trigger for an unexpected function",
            ));
        }
        if message.get("config") != Some(&json!({"queue": QUEUE_TOPIC})) {
            return Err(failure(
                "worker registered a subscriber with the wrong queue configuration",
            ));
        }
        if message.get("namespace").and_then(Value::as_str) != Some(NAMESPACE) {
            return Err(failure(
                "worker registered a subscriber with the wrong target namespace",
            ));
        }
        if message.get("trigger_namespace").and_then(Value::as_str) != Some(NAMESPACE) {
            return Err(failure(
                "worker registered a subscriber with the wrong provider namespace",
            ));
        }

        let id = required_uuid(message, "id", "subscriber registration")?;
        let nonce = registration_nonce(message.get("metadata"))?;
        self.trigger = Some(TriggerRegistration { id, nonce });
        Ok(())
    }

    fn nonce(&self) -> SmokeResult<&str> {
        let function_nonce = self
            .function_nonce
            .as_deref()
            .ok_or_else(|| failure("worker did not register harness::persist_event"))?;
        let trigger = self
            .trigger
            .as_ref()
            .ok_or_else(|| failure("worker did not register a durable subscriber"))?;
        if function_nonce != trigger.nonce {
            return Err(failure(
                "function and durable subscriber registrations use different nonces",
            ));
        }
        Ok(function_nonce)
    }

    fn trigger_id(&self) -> SmokeResult<&str> {
        self.nonce()?;
        self.trigger
            .as_ref()
            .map(|trigger| trigger.id.as_str())
            .ok_or_else(|| failure("worker did not register a durable subscriber"))
    }
}

#[derive(Default)]
struct StderrCapture {
    bytes: Vec<u8>,
    overflowed: bool,
}

impl StderrCapture {
    fn append(&mut self, line: &[u8]) {
        if self.overflowed {
            return;
        }
        let Some(remaining) = STDERR_CAPTURE_LIMIT.checked_sub(self.bytes.len()) else {
            self.overflowed = true;
            return;
        };
        if line.len() > remaining {
            self.overflowed = true;
            return;
        }
        self.bytes.extend_from_slice(line);
    }

    fn validate(&self) -> SmokeResult<()> {
        if self.overflowed {
            return Err(failure("worker stderr exceeded the bounded safety capture"));
        }
        if self
            .bytes
            .windows(OPAQUE_SENTINEL.len())
            .any(|window| window == OPAQUE_SENTINEL.as_bytes())
        {
            return Err(failure("worker stderr exposed opaque observation data"));
        }
        Ok(())
    }
}

struct ReadinessReport {
    phase: u8,
}

#[tokio::main]
async fn main() -> SmokeResult<()> {
    let arguments = parse_arguments(env::args())?;
    run(arguments).await
}

async fn run(arguments: Arguments) -> SmokeResult<()> {
    let launch = load_worker_launch(&arguments.manifest)?;
    run_worker(arguments, launch).await
}

async fn run_worker(arguments: Arguments, launch: WorkerLaunch) -> SmokeResult<()> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let address = listener.local_addr()?;
    let mut worker = Command::new("sh")
        .arg("-c")
        .arg(format!("exec {}", launch.start))
        .current_dir(&launch.working_directory)
        .env("TOTAL_RECALL_QUEUE_TOPIC", QUEUE_TOPIC)
        .env("TOTAL_RECALL_DATABASE", DATABASE)
        .env("III_URL", format!("ws://127.0.0.1:{}", address.port()))
        .env("III_WORKER_NAME", WORKER_NAME)
        .env("III_NAMESPACE", NAMESPACE)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;

    let stderr = worker
        .stderr
        .take()
        .ok_or_else(|| failure("worker stderr was not piped"))?;
    let capture = Arc::new(Mutex::new(StderrCapture::default()));
    let readiness_phase = Arc::new(AtomicU8::new(READINESS_PENDING));
    let (readiness_sender, readiness_receiver) = mpsc::unbounded_channel();
    let log_task = tokio::spawn(read_worker_stderr(
        stderr,
        Arc::clone(&capture),
        Arc::clone(&readiness_phase),
        readiness_sender,
    ));

    let protocol_result = async {
        let (stream, _) = timeout(arguments.timeout, listener.accept())
            .await
            .map_err(|_| failure("worker did not connect to the fake engine in time"))??;
        let mut socket = timeout(arguments.timeout, accept_async(stream))
            .await
            .map_err(|_| failure("worker WebSocket handshake timed out"))??;
        run_protocol(
            &mut socket,
            readiness_receiver,
            readiness_phase,
            arguments.timeout,
        )
        .await
    }
    .await;

    let cleanup_result = terminate_worker(&mut worker).await;
    let log_result = log_task
        .await
        .map_err(|error| failure(format!("worker stderr task failed: {error}")))?;
    let stderr_result = capture
        .lock()
        .map_err(|_| failure("worker stderr capture lock was poisoned"))?
        .validate();

    match (protocol_result, cleanup_result, log_result, stderr_result) {
        (Ok(()), Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(protocol_error), _, _, _) => Err(protocol_error),
        (Ok(()), Err(cleanup_error), _, _) => Err(cleanup_error),
        (Ok(()), Ok(()), Err(log_error), _) => Err(log_error),
        (Ok(()), Ok(()), Ok(()), Err(stderr_error)) => Err(stderr_error),
    }
}

fn load_worker_launch(manifest_path: &Path) -> SmokeResult<WorkerLaunch> {
    let contents = fs::read_to_string(manifest_path).map_err(|error| {
        failure(format!(
            "failed to read worker manifest {}: {error}",
            manifest_path.display()
        ))
    })?;
    parse_worker_launch(&contents, manifest_path)
}

fn parse_worker_launch(contents: &str, manifest_path: &Path) -> SmokeResult<WorkerLaunch> {
    let manifest = serde_yaml::from_str::<WorkerManifest>(contents)
        .map_err(|error| failure(format!("worker manifest is invalid YAML: {error}")))?;
    let start = manifest
        .scripts
        .and_then(|scripts| scripts.start)
        .filter(|start| !start.trim().is_empty())
        .ok_or_else(|| failure("worker manifest must define non-blank scripts.start"))?;
    let working_directory = manifest_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();

    Ok(WorkerLaunch {
        working_directory,
        start,
    })
}

async fn read_worker_stderr(
    stderr: impl AsyncRead + Unpin,
    capture: Arc<Mutex<StderrCapture>>,
    readiness_phase: Arc<AtomicU8>,
    readiness_sender: mpsc::UnboundedSender<ReadinessReport>,
) -> SmokeResult<()> {
    let mut reader = BufReader::new(stderr);
    let mut line = Vec::new();

    loop {
        let bytes_read = reader.read_until(b'\n', &mut line).await?;
        if bytes_read == 0 {
            return Ok(());
        }

        capture
            .lock()
            .map_err(|_| failure("worker stderr capture lock was poisoned"))?
            .append(&line);
        if std::str::from_utf8(&line).is_ok_and(|line| line.trim() == "worker ready") {
            let _ = readiness_sender.send(ReadinessReport {
                phase: readiness_phase.load(Ordering::SeqCst),
            });
        }
        line.clear();
    }
}

async fn terminate_worker(worker: &mut Child) -> SmokeResult<()> {
    if let Some(status) = worker.try_wait()? {
        return require_worker_success(status);
    }

    if let Some(pid) = worker.id() {
        #[cfg(unix)]
        {
            let status = Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .status()
                .await?;
            if !status.success() && worker.try_wait()?.is_none() {
                return Err(failure("failed to terminate the worker process"));
            }
        }
        #[cfg(not(unix))]
        {
            worker.kill().await?;
        }
    }

    match timeout(Duration::from_secs(5), worker.wait()).await {
        Ok(status) => require_worker_success(status?),
        Err(_) => {
            worker.kill().await?;
            worker.wait().await?;
            Err(failure(
                "worker did not exit within five seconds of the termination signal",
            ))
        }
    }
}

fn require_worker_success(status: ExitStatus) -> SmokeResult<()> {
    if status.success() {
        Ok(())
    } else {
        Err(failure(format!("worker exited unsuccessfully: {status}")))
    }
}

async fn run_protocol(
    socket: &mut WebSocketStream<TcpStream>,
    mut readiness_receiver: mpsc::UnboundedReceiver<ReadinessReport>,
    readiness_phase: Arc<AtomicU8>,
    operation_timeout: Duration,
) -> SmokeResult<()> {
    let deadline = Instant::now() + operation_timeout;
    let mut worker_registered = false;
    let mut registrations = RegistrationState::default();
    let mut catalog_step = 0;
    let mut delivery_sent = false;
    let mut database_request_seen = false;
    let mut database_ack_sent = false;

    loop {
        let frame = next_frame(socket, deadline).await?;
        match frame {
            Message::Ping(payload) => {
                socket.send(Message::Pong(payload)).await?;
            }
            Message::Close(_) => return Err(failure("worker closed the fake engine connection")),
            Message::Text(text) => {
                let message: Value = serde_json::from_str(text.as_str())?;
                let message_type = message
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| failure("worker sent a frame without a type"))?;

                match message_type {
                    "registerfunction" => {
                        registrations.record_function(&message)?;
                    }
                    "registertrigger" => {
                        registrations.record_trigger(&message)?;
                    }
                    "invocationresult" => {
                        if !delivery_sent {
                            return Err(failure(
                                "worker returned a result before the queued delivery",
                            ));
                        }
                        if !database_ack_sent {
                            return Err(failure(
                                "worker acknowledged persistence before the database confirmation",
                            ));
                        }
                        validate_persistence_result(&message)?;
                        if !database_request_seen {
                            return Err(failure(
                                "worker returned persistence without database execution",
                            ));
                        }
                        return Ok(());
                    }
                    "invokefunction" => {
                        let function_id = message
                            .get("function_id")
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                failure("invokefunction did not include a function id")
                            })?;
                        match function_id {
                            WORKER_REGISTER_FUNCTION => {
                                if worker_registered {
                                    return Err(failure(
                                        "worker registered with the engine more than once",
                                    ));
                                }
                                validate_worker_registration_request(&message)?;
                                worker_registered = true;
                                socket
                                    .send(Message::Text(
                                        json!({
                                            "type": "workerregistered",
                                            "worker_id": "harness-persistence-fake-engine"
                                        })
                                        .to_string()
                                        .into(),
                                    ))
                                    .await?;
                            }
                            FUNCTIONS_INFO_FUNCTION => {
                                if !worker_registered {
                                    return Err(failure(
                                        "function catalog request arrived before worker registration",
                                    ));
                                }
                                if catalog_step != 0 {
                                    return Err(failure(
                                        "worker requested the function catalog out of order",
                                    ));
                                }
                                let invocation_id = validate_catalog_invocation(
                                    &message,
                                    FUNCTIONS_INFO_FUNCTION,
                                    json!({
                                        "function_ids": [PERSIST_EVENT_FUNCTION_ID],
                                        "namespace": NAMESPACE,
                                    }),
                                )?;
                                let nonce = registrations.nonce()?;
                                send_invocation_result(
                                    socket,
                                    &invocation_id,
                                    FUNCTIONS_INFO_FUNCTION,
                                    json!({
                                        "functions": [{
                                            "function_id": PERSIST_EVENT_FUNCTION_ID,
                                            "namespace": NAMESPACE,
                                            "worker_name": WORKER_NAME,
                                            "metadata": {"registration_nonce": nonce},
                                        }],
                                    }),
                                )
                                .await?;
                                catalog_step = 1;
                            }
                            REGISTERED_TRIGGERS_LIST_FUNCTION => {
                                if catalog_step != 1 {
                                    return Err(failure(
                                        "worker requested registered-trigger list before function ownership verification",
                                    ));
                                }
                                let invocation_id = validate_catalog_invocation(
                                    &message,
                                    REGISTERED_TRIGGERS_LIST_FUNCTION,
                                    json!({
                                        "function_id": PERSIST_EVENT_FUNCTION_ID,
                                        "trigger_type": DURABLE_SUBSCRIBER_TRIGGER_TYPE,
                                        "include_pending": true,
                                    }),
                                )?;
                                let trigger_id = registrations.trigger_id()?;
                                send_invocation_result(
                                    socket,
                                    &invocation_id,
                                    REGISTERED_TRIGGERS_LIST_FUNCTION,
                                    json!({
                                        "registered_triggers": [{
                                            "id": trigger_id,
                                            "trigger_type": DURABLE_SUBSCRIBER_TRIGGER_TYPE,
                                            "function_id": PERSIST_EVENT_FUNCTION_ID,
                                        }],
                                    }),
                                )
                                .await?;
                                catalog_step = 2;
                            }
                            REGISTERED_TRIGGERS_INFO_FUNCTION => {
                                if catalog_step != 2 {
                                    return Err(failure(
                                        "worker requested registered-trigger info before list verification",
                                    ));
                                }
                                let trigger_id = registrations.trigger_id()?.to_owned();
                                let invocation_id = validate_catalog_invocation(
                                    &message,
                                    REGISTERED_TRIGGERS_INFO_FUNCTION,
                                    json!({"id": trigger_id}),
                                )?;
                                let nonce = registrations.nonce()?;
                                readiness_phase
                                    .store(READINESS_CATALOG_REPLY_SENT, Ordering::SeqCst);
                                send_invocation_result(
                                    socket,
                                    &invocation_id,
                                    REGISTERED_TRIGGERS_INFO_FUNCTION,
                                    json!({
                                        "id": trigger_id,
                                        "trigger_type": DURABLE_SUBSCRIBER_TRIGGER_TYPE,
                                        "function_id": PERSIST_EVENT_FUNCTION_ID,
                                        "worker_name": WORKER_NAME,
                                        "status": "active",
                                        "config": {"queue": QUEUE_TOPIC},
                                        "metadata": {"registration_nonce": nonce},
                                        "trigger": {
                                            "id": DURABLE_SUBSCRIBER_TRIGGER_TYPE,
                                            "namespace": NAMESPACE,
                                        },
                                        "function": {
                                            "function_id": PERSIST_EVENT_FUNCTION_ID,
                                            "namespace": NAMESPACE,
                                            "worker_name": WORKER_NAME,
                                        },
                                    }),
                                )
                                .await?;
                                wait_for_worker_ready(&mut readiness_receiver, deadline).await?;
                                send_persistence_invocation(socket).await?;
                                delivery_sent = true;
                                catalog_step = 3;
                            }
                            DATABASE_EXECUTE_FUNCTION => {
                                if catalog_step != 3 || !delivery_sent {
                                    return Err(failure(
                                        "database execution arrived before readiness delivery",
                                    ));
                                }
                                if database_request_seen {
                                    return Err(failure(
                                        "worker invoked database::execute more than once",
                                    ));
                                }
                                let invocation_id = validate_database_invocation(&message)?;
                                database_request_seen = true;
                                send_database_result_after_guard(socket, &invocation_id, deadline)
                                    .await?;
                                database_ack_sent = true;
                            }
                            _ => return Err(failure("worker invoked an unexpected function")),
                        }
                    }
                    _ => return Err(failure("worker sent an unsupported frame type")),
                }
            }
            Message::Binary(_) => return Err(failure("worker sent a binary frame")),
            _ => {}
        }
    }
}

async fn next_frame(
    socket: &mut WebSocketStream<TcpStream>,
    deadline: Instant,
) -> SmokeResult<Message> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(failure("fake engine smoke operation timed out"));
    }
    let frame = timeout(remaining, socket.next())
        .await
        .map_err(|_| failure("fake engine smoke operation timed out"))?
        .ok_or_else(|| failure("worker disconnected from the fake engine"))??;
    Ok(frame)
}

async fn send_invocation_result(
    socket: &mut WebSocketStream<TcpStream>,
    invocation_id: &str,
    function_id: &str,
    result: Value,
) -> SmokeResult<()> {
    socket
        .send(Message::Text(
            json!({
                "type": "invocationresult",
                "invocation_id": invocation_id,
                "function_id": function_id,
                "result": result,
            })
            .to_string()
            .into(),
        ))
        .await?;
    Ok(())
}

async fn wait_for_worker_ready(
    readiness_receiver: &mut mpsc::UnboundedReceiver<ReadinessReport>,
    deadline: Instant,
) -> SmokeResult<()> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let readiness = timeout(remaining, readiness_receiver.recv())
        .await
        .map_err(|_| failure("worker did not report ready after ownership verification"))?
        .ok_or_else(|| failure("worker stderr stream ended before readiness"))?;
    if readiness.phase != READINESS_CATALOG_REPLY_SENT {
        return Err(failure(
            "worker reported ready before registered-trigger ownership verification",
        ));
    }
    Ok(())
}

async fn send_persistence_invocation(socket: &mut WebSocketStream<TcpStream>) -> SmokeResult<()> {
    socket
        .send(Message::Text(
            json!({
                "type": "invokefunction",
                "invocation_id": "00000000-0000-4000-8000-000000000042",
                "function_id": PERSIST_EVENT_FUNCTION_ID,
                "namespace": NAMESPACE,
                "data": observation_event(),
            })
            .to_string()
            .into(),
        ))
        .await?;
    Ok(())
}

async fn send_database_result_after_guard(
    socket: &mut WebSocketStream<TcpStream>,
    invocation_id: &str,
    deadline: Instant,
) -> SmokeResult<()> {
    let guard_deadline = (Instant::now() + DATABASE_ACK_GUARD).min(deadline);
    loop {
        let remaining = guard_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining, socket.next()).await {
            Err(_) => break,
            Ok(None) => return Err(failure("worker disconnected before database confirmation")),
            Ok(Some(Err(error))) => return Err(error.into()),
            Ok(Some(Ok(Message::Ping(payload)))) => {
                socket.send(Message::Pong(payload)).await?;
            }
            Ok(Some(Ok(Message::Pong(_)))) => {}
            Ok(Some(Ok(Message::Text(text)))) => {
                let message: Value = serde_json::from_str(text.as_str())?;
                if message.get("type").and_then(Value::as_str) == Some("invocationresult") {
                    return Err(failure(
                        "worker acknowledged persistence before the delayed database confirmation",
                    ));
                }
                return Err(failure("worker sent a frame before database confirmation"));
            }
            Ok(Some(Ok(_))) => {
                return Err(failure(
                    "worker sent an unsupported frame before database confirmation",
                ));
            }
        }
    }

    send_invocation_result(
        socket,
        invocation_id,
        DATABASE_EXECUTE_FUNCTION,
        json!({
            "affected_rows": 1,
            "last_insert_id": null,
            "returned_rows": [],
        }),
    )
    .await
}

fn validate_worker_registration_request(message: &Value) -> SmokeResult<()> {
    if message
        .get("invocation_id")
        .is_some_and(|value| !value.is_null())
    {
        return Err(failure(
            "worker registration must be a fire-and-forget invocation",
        ));
    }
    if message.get("action") != Some(&json!({"type": "void"})) {
        return Err(failure("worker registration did not use the void action"));
    }
    if message
        .get("namespace")
        .is_some_and(|value| !value.is_null())
    {
        return Err(failure("worker registration named an unexpected namespace"));
    }
    let metadata = message
        .get("data")
        .and_then(Value::as_object)
        .ok_or_else(|| failure("worker registration metadata was not an object"))?;
    if metadata.get("name").and_then(Value::as_str) != Some(WORKER_NAME) {
        return Err(failure("worker registered with the wrong managed name"));
    }
    if metadata.get("namespace").and_then(Value::as_str) != Some(NAMESPACE) {
        return Err(failure(
            "worker registered with the wrong managed namespace",
        ));
    }
    Ok(())
}

fn validate_catalog_invocation(
    message: &Value,
    function_id: &str,
    expected_payload: Value,
) -> SmokeResult<String> {
    if message.get("action").is_some_and(|value| !value.is_null()) {
        return Err(failure("catalog invocation used an unexpected action"));
    }
    if message.get("namespace").and_then(Value::as_str) != Some(ENGINE_NAMESPACE) {
        return Err(failure(
            "catalog invocation did not use default engine namespace routing",
        ));
    }
    if message.get("data") != Some(&expected_payload) {
        return Err(failure(
            "catalog invocation payload did not match the readiness contract",
        ));
    }
    required_uuid(message, "invocation_id", function_id)
}

fn validate_database_invocation(message: &Value) -> SmokeResult<String> {
    if message.get("action").is_some_and(|value| !value.is_null()) {
        return Err(failure("database invocation used an unexpected action"));
    }
    if message.get("namespace").and_then(Value::as_str) != Some(NAMESPACE) {
        return Err(failure(
            "database invocation did not inherit the worker namespace",
        ));
    }
    if message.get("data") != Some(&expected_database_request()) {
        return Err(failure(
            "database invocation did not match the required observation insert",
        ));
    }
    required_uuid(message, "invocation_id", "database invocation")
}

fn validate_persistence_result(message: &Value) -> SmokeResult<()> {
    if message.get("function_id").and_then(Value::as_str) != Some(PERSIST_EVENT_FUNCTION_ID) {
        return Err(failure("persistence response used the wrong function id"));
    }
    if message.get("invocation_id").and_then(Value::as_str)
        != Some("00000000-0000-4000-8000-000000000042")
    {
        return Err(failure("persistence response used the wrong invocation id"));
    }
    if message.get("result") != Some(&json!({"persisted": true})) {
        return Err(failure(
            "persistence response did not confirm one stored event",
        ));
    }
    if message.get("error").is_some_and(|value| !value.is_null()) {
        return Err(failure("persistence response returned an unexpected error"));
    }
    Ok(())
}

fn registration_nonce(metadata: Option<&Value>) -> SmokeResult<String> {
    let metadata = metadata
        .and_then(Value::as_object)
        .ok_or_else(|| failure("registration metadata was not an object"))?;
    if metadata.len() != 3
        || metadata.get("worker_name").and_then(Value::as_str) != Some(WORKER_NAME)
        || metadata.get("namespace").and_then(Value::as_str) != Some(NAMESPACE)
    {
        return Err(failure(
            "registration metadata did not identify this managed worker",
        ));
    }
    let nonce = metadata
        .get("registration_nonce")
        .and_then(Value::as_str)
        .ok_or_else(|| failure("registration metadata did not include a nonce"))?;
    let nonce_uuid = Uuid::parse_str(nonce)
        .map_err(|_| failure("registration metadata nonce was not a UUID"))?;
    if nonce_uuid.get_version_num() != 4 {
        return Err(failure("registration metadata nonce was not version 4"));
    }
    Ok(nonce.to_owned())
}

fn required_uuid(message: &Value, field: &str, context: &str) -> SmokeResult<String> {
    let value = message
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| failure(format!("{context} did not include {field}")))?;
    Uuid::parse_str(value)
        .map_err(|_| failure(format!("{context} included an invalid {field}")))?;
    Ok(value.to_owned())
}

fn observation_event() -> Value {
    json!({
        "event_type": "observation",
        "hook_type": "post_tool_use",
        "project_name": "harness-persistence-smoke",
        "current_working_directory": "/workspace/harness",
        "timestamp": OBSERVATION_TIMESTAMP,
        "session_id": "persistence-smoke-session",
        "data": {
            "nested": {
                "items": [true, null, {"rounded": 9_007_199_254_740_993u64}],
            },
            "opaque": OPAQUE_SENTINEL,
        },
    })
}

fn expected_database_request() -> Value {
    json!({
        "db": DATABASE,
        "sql": "INSERT INTO raw_observations (session_id, event_type, hook_type, project_name, current_working_directory, source_timestamp_rfc3339, source_timestamp_utc, data) VALUES ($1, $2, $3, $4, $5, $6, TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond'), $8::json)",
        "params": [
            "persistence-smoke-session",
            "observation",
            "post_tool_use",
            "harness-persistence-smoke",
            "/workspace/harness",
            OBSERVATION_TIMESTAMP,
            OBSERVATION_EPOCH_MILLIS,
            {
                "nested": {
                    "items": [true, null, {"rounded": 9_007_199_254_740_992.0}],
                },
                "opaque": OPAQUE_SENTINEL,
            },
        ],
    })
}

fn parse_arguments<I>(arguments: I) -> Result<Arguments, String>
where
    I: IntoIterator<Item = String>,
{
    let mut arguments = arguments.into_iter();
    let _program = arguments.next();
    let mut manifest = None;
    let mut operation_timeout = Duration::from_secs(30);

    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--manifest" => {
                manifest = Some(
                    arguments
                        .next()
                        .ok_or_else(|| "--manifest requires a path".to_owned())?
                        .into(),
                );
            }
            "--timeout-seconds" => {
                let seconds = arguments
                    .next()
                    .ok_or_else(|| "--timeout-seconds requires a value".to_owned())?
                    .parse::<u64>()
                    .map_err(|_| "--timeout-seconds must be an integer".to_owned())?;
                if seconds == 0 {
                    return Err("--timeout-seconds must be greater than zero".to_owned());
                }
                operation_timeout = Duration::from_secs(seconds);
            }
            "--help" | "-h" => return Err(usage().to_owned()),
            unknown => return Err(format!("unknown argument {unknown:?}\n\n{}", usage())),
        }
    }

    Ok(Arguments {
        manifest: manifest.ok_or_else(|| format!("--manifest is required\n\n{}", usage()))?,
        timeout: operation_timeout,
    })
}

fn usage() -> &'static str {
    "Usage: harness-persistence-engine-fake --manifest <path> [--timeout-seconds <seconds>]"
}

fn failure(message: impl Into<String>) -> BoxError {
    Box::new(io::Error::other(message.into()))
}

#[cfg(test)]
mod tests {
    use std::{
        path::{Path, PathBuf},
        time::Duration,
    };

    use serde_json::json;

    use super::{
        Arguments, DATABASE_EXECUTE_FUNCTION, ENGINE_NAMESPACE, NAMESPACE,
        PERSIST_EVENT_FUNCTION_ID, QUEUE_TOPIC, RegistrationState, WORKER_NAME, WorkerLaunch,
        expected_database_request, parse_arguments, parse_worker_launch,
        persist_event_function_schemas, validate_catalog_invocation, validate_database_invocation,
        validate_persistence_result,
    };

    fn persist_event_function_registration() -> serde_json::Value {
        let (request_format, response_format) = persist_event_function_schemas().unwrap();
        json!({
            "id": PERSIST_EVENT_FUNCTION_ID,
            "request_format": request_format,
            "response_format": response_format,
            "metadata": {
                "registration_nonce": "63ec9af3-668e-4ea6-9bfe-a17b5f0c3c75",
                "worker_name": WORKER_NAME,
                "namespace": NAMESPACE,
            },
        })
    }

    #[test]
    fn arguments_require_a_manifest_path() {
        let error = parse_arguments(["harness-persistence-engine-fake".to_owned()]).unwrap_err();

        assert!(error.contains("--manifest is required"));
    }

    #[test]
    fn arguments_accept_a_custom_timeout() {
        let arguments = parse_arguments([
            "harness-persistence-engine-fake".to_owned(),
            "--manifest".to_owned(),
            "/tmp/harness-event-persistence/iii.worker.yaml".to_owned(),
            "--timeout-seconds".to_owned(),
            "12".to_owned(),
        ])
        .unwrap();

        assert_eq!(
            arguments,
            Arguments {
                manifest: PathBuf::from("/tmp/harness-event-persistence/iii.worker.yaml"),
                timeout: Duration::from_secs(12),
            }
        );
    }

    #[test]
    fn manifest_start_command_is_resolved_from_scripts() {
        let launch = parse_worker_launch(
            "name: harness-event-persistence\nscripts:\n  start: ../../target/release/harness-event-persistence\n",
            Path::new("/repo/workers/harness-event-persistence/iii.worker.yaml"),
        )
        .unwrap();

        assert_eq!(
            launch,
            WorkerLaunch {
                working_directory: PathBuf::from("/repo/workers/harness-event-persistence"),
                start: "../../target/release/harness-event-persistence".to_owned(),
            }
        );
    }

    #[test]
    fn manifest_rejects_a_blank_or_unsupported_start_command() {
        for manifest in [
            "name: harness-event-persistence\nstart: ../../target/release/harness-event-persistence\n",
            "scripts:\n  start: '   '\n",
        ] {
            let error = parse_worker_launch(manifest, Path::new("iii.worker.yaml")).unwrap_err();

            assert!(error.to_string().contains("scripts.start"));
        }
    }

    #[test]
    fn registration_requires_one_function_and_one_owned_subscriber_with_a_shared_nonce() {
        let mut registrations = RegistrationState::default();
        let nonce = "63ec9af3-668e-4ea6-9bfe-a17b5f0c3c75";
        let mut function = persist_event_function_registration();
        function["metadata"]["registration_nonce"] = json!(nonce);
        registrations.record_function(&function).unwrap();
        registrations
            .record_trigger(&json!({
                "id": "8d4d6f3a-2a42-4e9a-9d37-4d0d4a6e0a11",
                "trigger_type": "durable:subscriber",
                "function_id": PERSIST_EVENT_FUNCTION_ID,
                "config": {"queue": QUEUE_TOPIC},
                "metadata": {
                    "registration_nonce": nonce,
                    "worker_name": WORKER_NAME,
                    "namespace": NAMESPACE,
                },
                "namespace": NAMESPACE,
                "trigger_namespace": NAMESPACE,
            }))
            .unwrap();

        assert_eq!(registrations.nonce().unwrap(), nonce);
        assert_eq!(
            registrations.trigger_id().unwrap(),
            "8d4d6f3a-2a42-4e9a-9d37-4d0d4a6e0a11"
        );
    }

    #[test]
    fn registration_rejects_mismatched_target_namespace_or_nonce() {
        let mut registrations = RegistrationState::default();
        let function = persist_event_function_registration();
        registrations.record_function(&function).unwrap();
        let error = registrations
            .record_trigger(&json!({
                "id": "8d4d6f3a-2a42-4e9a-9d37-4d0d4a6e0a11",
                "trigger_type": "durable:subscriber",
                "function_id": PERSIST_EVENT_FUNCTION_ID,
                "config": {"queue": QUEUE_TOPIC},
                "metadata": {
                    "registration_nonce": "63ec9af3-668e-4ea6-9bfe-a17b5f0c3c75",
                    "worker_name": WORKER_NAME,
                    "namespace": NAMESPACE,
                },
                "namespace": "foreign",
                "trigger_namespace": NAMESPACE,
            }))
            .unwrap_err();

        assert!(error.to_string().contains("target namespace"));

        let mut registrations = RegistrationState::default();
        let function = persist_event_function_registration();
        registrations.record_function(&function).unwrap();
        registrations
            .record_trigger(&json!({
                "id": "8d4d6f3a-2a42-4e9a-9d37-4d0d4a6e0a11",
                "trigger_type": "durable:subscriber",
                "function_id": PERSIST_EVENT_FUNCTION_ID,
                "config": {"queue": QUEUE_TOPIC},
                "metadata": {
                    "registration_nonce": "8d4d6f3a-2a42-4e9a-9d37-4d0d4a6e0a11",
                    "worker_name": WORKER_NAME,
                    "namespace": NAMESPACE,
                },
                "namespace": NAMESPACE,
                "trigger_namespace": NAMESPACE,
            }))
            .unwrap();

        assert!(registrations.nonce().is_err());
    }

    #[test]
    fn registration_accepts_the_persistence_function_contract_schemas() {
        let mut registrations = RegistrationState::default();

        registrations
            .record_function(&persist_event_function_registration())
            .unwrap();

        assert_eq!(
            registrations.function_nonce.as_deref(),
            Some("63ec9af3-668e-4ea6-9bfe-a17b5f0c3c75")
        );
    }

    #[test]
    fn registration_rejects_absent_request_or_response_schema() {
        for schema_field in ["request_format", "response_format"] {
            let mut registration = persist_event_function_registration();
            registration.as_object_mut().unwrap().remove(schema_field);

            assert!(
                RegistrationState::default()
                    .record_function(&registration)
                    .is_err()
            );
        }
    }

    #[test]
    fn registration_rejects_malformed_request_or_response_schema() {
        for schema_field in ["request_format", "response_format"] {
            let mut registration = persist_event_function_registration();
            registration[schema_field] = json!(false);

            assert!(
                RegistrationState::default()
                    .record_function(&registration)
                    .is_err()
            );
        }
    }

    #[test]
    fn registration_rejects_wrong_request_or_response_schema() {
        for schema_field in ["request_format", "response_format"] {
            let mut registration = persist_event_function_registration();
            registration[schema_field] = json!({"title": "Wrong schema"});

            assert!(
                RegistrationState::default()
                    .record_function(&registration)
                    .is_err()
            );
        }
    }

    #[test]
    fn catalog_validation_requires_default_engine_routing_and_exact_payload() {
        let message = json!({
            "invocation_id": "00000000-0000-4000-8000-000000000001",
            "namespace": ENGINE_NAMESPACE,
            "data": {
                "function_ids": [PERSIST_EVENT_FUNCTION_ID],
                "namespace": NAMESPACE,
            },
        });

        assert!(
            validate_catalog_invocation(
                &message,
                "engine::functions::info",
                json!({
                    "function_ids": [PERSIST_EVENT_FUNCTION_ID],
                    "namespace": NAMESPACE,
                }),
            )
            .is_ok()
        );

        let mut wrong_namespace = message;
        wrong_namespace["namespace"] = json!(NAMESPACE);
        assert!(
            validate_catalog_invocation(
                &wrong_namespace,
                "engine::functions::info",
                json!({
                    "function_ids": [PERSIST_EVENT_FUNCTION_ID],
                    "namespace": NAMESPACE,
                }),
            )
            .is_err()
        );
    }

    #[test]
    fn database_validation_requires_the_exact_static_observation_insert() {
        let message = json!({
            "invocation_id": "00000000-0000-4000-8000-000000000002",
            "function_id": DATABASE_EXECUTE_FUNCTION,
            "namespace": NAMESPACE,
            "data": expected_database_request(),
        });

        assert!(validate_database_invocation(&message).is_ok());

        let mut wrong_request = message;
        wrong_request["data"]["params"]
            .as_array_mut()
            .unwrap()
            .pop();
        assert!(validate_database_invocation(&wrong_request).is_err());
    }

    #[test]
    fn persistence_result_requires_the_delivered_invocation_success() {
        let result = json!({
            "function_id": PERSIST_EVENT_FUNCTION_ID,
            "invocation_id": "00000000-0000-4000-8000-000000000042",
            "result": {"persisted": true},
        });

        assert!(validate_persistence_result(&result).is_ok());

        let mut early_failure = result;
        early_failure["result"] = json!({"persisted": false});
        assert!(validate_persistence_result(&early_failure).is_err());
    }
}
