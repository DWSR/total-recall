use std::{
    collections::HashMap,
    env,
    error::Error,
    fs, io,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    sync::oneshot,
    time::{Instant, timeout},
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

const FUNCTION_IDS: [&str; 3] = [
    "harness::session_start",
    "harness::observation",
    "harness::session_end",
];
const WORKER_REGISTER_FUNCTION: &str = "engine::workers::register";
const FUNCTIONS_INFO_FUNCTION: &str = "engine::functions::info";
const PUBLISH_FUNCTION: &str = "iii::durable::publish";
const WORKER_NAME: &str = "harness-ingestion-ci";
const NAMESPACE: &str = "harness-smoke";
const QUEUE_TOPIC: &str = "harness-smoke";
const PUBLISH_ACK_GUARD: Duration = Duration::from_millis(50);

type BoxError = Box<dyn Error + Send + Sync>;
type SmokeResult<T> = Result<T, BoxError>;

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

#[derive(Debug)]
struct HarnessCase {
    function_id: &'static str,
    invocation_id: &'static str,
    input: Value,
    event: Value,
}

fn harness_cases() -> [HarnessCase; 3] {
    [
        HarnessCase {
            function_id: FUNCTION_IDS[0],
            invocation_id: "00000000-0000-4000-8000-000000000001",
            input: json!({
                "session_id": "ci-session",
                "project_name": "harness-smoke",
                "timestamp": "2026-09-18T12:34:56.789Z",
                "current_working_directory": "/github/workspace",
            }),
            event: json!({
                "event_type": "session_start",
                "session_id": "ci-session",
                "project_name": "harness-smoke",
                "timestamp": "2026-09-18T12:34:56.789Z",
                "current_working_directory": "/github/workspace",
            }),
        },
        HarnessCase {
            function_id: FUNCTION_IDS[1],
            invocation_id: "00000000-0000-4000-8000-000000000002",
            input: json!({
                "hook_type": "PostToolUse",
                "project_name": "harness-smoke",
                "current_working_directory": "/github/workspace",
                "timestamp": "2026-09-18T12:35:00.123+00:00",
                "session_id": "ci-session",
                "data": {
                    "nested": {"enabled": true},
                    "items": ["value", null, 1.5],
                },
            }),
            event: json!({
                "event_type": "observation",
                "hook_type": "PostToolUse",
                "project_name": "harness-smoke",
                "current_working_directory": "/github/workspace",
                "timestamp": "2026-09-18T12:35:00.123+00:00",
                "session_id": "ci-session",
                "data": {
                    "nested": {"enabled": true},
                    "items": ["value", null, 1.5],
                },
            }),
        },
        HarnessCase {
            function_id: FUNCTION_IDS[2],
            invocation_id: "00000000-0000-4000-8000-000000000003",
            input: json!({
                "session_id": "ci-session",
                "project_name": "harness-smoke",
                "timestamp": "2026-09-18T12:35:05.456Z",
                "current_working_directory": "/github/workspace",
            }),
            event: json!({
                "event_type": "session_end",
                "session_id": "ci-session",
                "project_name": "harness-smoke",
                "timestamp": "2026-09-18T12:35:05.456Z",
                "current_working_directory": "/github/workspace",
            }),
        },
    ]
}

#[derive(Default)]
struct RegistrationState {
    metadata: HashMap<String, Value>,
}

impl RegistrationState {
    fn add(&mut self, function_id: String, metadata: Value) -> SmokeResult<()> {
        if !FUNCTION_IDS.contains(&function_id.as_str()) {
            return Err(failure(format!(
                "worker registered unexpected function {function_id:?}"
            )));
        }
        if self
            .metadata
            .insert(function_id.clone(), metadata)
            .is_some()
        {
            return Err(failure(format!(
                "worker registered function {function_id:?} more than once"
            )));
        }
        Ok(())
    }

    fn is_complete(&self) -> bool {
        self.metadata.len() == FUNCTION_IDS.len()
            && FUNCTION_IDS
                .iter()
                .all(|function_id| self.metadata.contains_key(*function_id))
    }

    fn nonce(&self) -> SmokeResult<String> {
        let mut nonce = None;
        for function_id in FUNCTION_IDS {
            let metadata = self
                .metadata
                .get(function_id)
                .and_then(Value::as_object)
                .ok_or_else(|| failure(format!("{function_id} registration has no metadata")))?;
            let worker_name = metadata
                .get("worker_name")
                .and_then(Value::as_str)
                .ok_or_else(|| failure(format!("{function_id} registration has no worker name")))?;
            if worker_name != WORKER_NAME {
                return Err(failure(format!(
                    "{function_id} registered as {worker_name:?}, expected {WORKER_NAME:?}"
                )));
            }
            let namespace = metadata
                .get("namespace")
                .and_then(Value::as_str)
                .ok_or_else(|| failure(format!("{function_id} registration has no namespace")))?;
            if namespace != NAMESPACE {
                return Err(failure(format!(
                    "{function_id} registered in {namespace:?}, expected {NAMESPACE:?}"
                )));
            }
            let current = metadata
                .get("registration_nonce")
                .and_then(Value::as_str)
                .ok_or_else(|| failure(format!("{function_id} registration has no nonce")))?;
            if let Some(previous) = nonce {
                if previous != current {
                    return Err(failure("function registrations use different nonces"));
                }
            } else {
                nonce = Some(current);
            }
        }

        nonce
            .map(str::to_owned)
            .ok_or_else(|| failure("function registrations did not contain a nonce"))
    }
}

#[tokio::main]
async fn main() -> SmokeResult<()> {
    let arguments = parse_arguments(env::args())?;
    run(arguments).await
}

async fn run(arguments: Arguments) -> SmokeResult<()> {
    let launch = load_worker_launch(&arguments.manifest)?;

    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let address = listener.local_addr()?;
    let mut worker = Command::new("sh")
        .arg("-c")
        .arg(format!("exec {}", launch.start))
        .current_dir(&launch.working_directory)
        .env("TOTAL_RECALL_QUEUE_TOPIC", QUEUE_TOPIC)
        .env("III_URL", format!("ws://127.0.0.1:{}", address.port()))
        .env("III_WORKER_NAME", WORKER_NAME)
        .env("III_NAMESPACE", NAMESPACE)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::piped())
        .spawn()?;

    let stderr = worker
        .stderr
        .take()
        .ok_or_else(|| failure("worker stderr was not piped"))?;
    let (ready_sender, ready_receiver) = oneshot::channel();
    let log_task = tokio::spawn(read_worker_stderr(stderr, ready_sender));

    let result = async {
        let (stream, _) = timeout(arguments.timeout, listener.accept())
            .await
            .map_err(|_| failure("worker did not connect to the fake engine in time"))??;
        let mut socket = timeout(arguments.timeout, accept_async(stream))
            .await
            .map_err(|_| failure("worker WebSocket handshake timed out"))??;
        run_protocol(&mut socket, ready_receiver, arguments.timeout).await
    }
    .await;

    let cleanup = terminate_worker(&mut worker).await;
    log_task.abort();
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(protocol_error), Ok(())) => Err(protocol_error),
        (Ok(()), Err(cleanup_error)) => Err(cleanup_error),
        (Err(protocol_error), Err(cleanup_error)) => {
            eprintln!("smoke cleanup failed after protocol error: {cleanup_error}");
            Err(protocol_error)
        }
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
    stderr: impl tokio::io::AsyncRead + Unpin,
    ready_sender: oneshot::Sender<()>,
) {
    let mut ready_sender = Some(ready_sender);
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        eprintln!("worker: {line}");
        if line.trim() == "worker ready"
            && let Some(sender) = ready_sender.take()
        {
            let _ = sender.send(());
        }
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
    ready_receiver: oneshot::Receiver<()>,
    operation_timeout: Duration,
) -> SmokeResult<()> {
    let deadline = Instant::now() + operation_timeout;
    let mut registrations = RegistrationState::default();
    let mut ready_receiver = Some(ready_receiver);
    let cases = harness_cases();
    let mut case_index = 0;
    let mut publish_invocation_seen = false;
    let mut publish_ack_sent = false;

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
                if message_type == "invocationresult" {
                    let case = &cases[case_index];
                    let function_id = message
                        .get("function_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| failure("invocationresult did not include a function id"))?;
                    if function_id != case.function_id {
                        return Err(failure(format!(
                            "worker returned an unexpected invocation result for {function_id}"
                        )));
                    }
                    if !publish_ack_sent {
                        return Err(failure(
                            "worker reported harness dispatch before the publish acknowledgement",
                        ));
                    }
                    validate_harness_result(&message, case)?;
                    case_index += 1;
                    if case_index == cases.len() {
                        return Ok(());
                    }
                    publish_invocation_seen = false;
                    publish_ack_sent = false;
                    send_harness_invocation(socket, &cases[case_index]).await?;
                    continue;
                }
                if message_type == "registerfunction" {
                    let function_id = message
                        .get("id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| failure("registerfunction did not include an id"))?;
                    registrations.add(
                        function_id.to_owned(),
                        message.get("metadata").cloned().unwrap_or(Value::Null),
                    )?;
                    continue;
                }
                if message_type != "invokefunction" {
                    return Err(failure(format!(
                        "worker sent unsupported frame type {message_type}"
                    )));
                }

                let function_id = message
                    .get("function_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| failure("invokefunction did not include a function id"))?;
                let invocation_id = message.get("invocation_id").cloned();
                match function_id {
                    WORKER_REGISTER_FUNCTION => {
                        validate_worker_registration_request(&message)?;
                        socket
                            .send(Message::Text(
                                json!({
                                    "type": "workerregistered",
                                    "worker_id": "harness-smoke-engine"
                                })
                                .to_string()
                                .into(),
                            ))
                            .await?;
                    }
                    FUNCTIONS_INFO_FUNCTION => {
                        validate_functions_info_request(&message)?;
                        let invocation_id = invocation_id
                            .and_then(|value| value.as_str().map(str::to_owned))
                            .ok_or_else(|| failure("functions info call had no invocation id"))?;
                        let result = if registrations.is_complete() {
                            let nonce = registrations.nonce()?;
                            json!({
                                "functions": FUNCTION_IDS.iter().map(|function_id| json!({
                                    "function_id": function_id,
                                    "namespace": NAMESPACE,
                                    "worker_name": WORKER_NAME,
                                    "metadata": {"registration_nonce": nonce},
                                    "registered_triggers": [],
                                })).collect::<Vec<_>>()
                            })
                        } else {
                            json!({
                                "functions": FUNCTION_IDS.iter().map(|function_id| json!({
                                    "function_id": function_id,
                                    "error": "pending",
                                })).collect::<Vec<_>>()
                            })
                        };
                        send_invocation_result(socket, &invocation_id, function_id, result).await?;

                        if registrations.is_complete() && ready_receiver.is_some() {
                            let receiver = ready_receiver.take().ok_or_else(|| {
                                failure("readiness receiver was already consumed")
                            })?;
                            let remaining = deadline.saturating_duration_since(Instant::now());
                            timeout(remaining, receiver)
                                .await
                                .map_err(|_| failure("worker did not report ready in time"))?
                                .map_err(|_| failure("worker readiness log stream ended"))?;
                            send_harness_invocation(socket, &cases[case_index]).await?;
                        }
                    }
                    PUBLISH_FUNCTION => {
                        if publish_invocation_seen {
                            return Err(failure("worker published more than once"));
                        }
                        let invocation_id = invocation_id
                            .and_then(|value| value.as_str().map(str::to_owned))
                            .ok_or_else(|| failure("publish call had no invocation id"))?;
                        validate_publish_payload(&message, &cases[case_index])?;
                        publish_invocation_seen = true;
                        send_publish_ack_after_guard(socket, &invocation_id, function_id, deadline)
                            .await?;
                        publish_ack_sent = true;
                    }
                    _ => {
                        return Err(failure(format!(
                            "unexpected worker invocation: {function_id}"
                        )));
                    }
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

async fn send_publish_ack_after_guard(
    socket: &mut WebSocketStream<TcpStream>,
    invocation_id: &str,
    function_id: &str,
    deadline: Instant,
) -> SmokeResult<()> {
    let guard_deadline = (Instant::now() + PUBLISH_ACK_GUARD).min(deadline);
    loop {
        let remaining = guard_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }

        match timeout(remaining, socket.next()).await {
            Err(_) => break,
            Ok(None) => {
                return Err(failure(
                    "worker disconnected before publish acknowledgement",
                ));
            }
            Ok(Some(Err(error))) => return Err(error.into()),
            Ok(Some(Ok(Message::Ping(payload)))) => {
                socket.send(Message::Pong(payload)).await?;
            }
            Ok(Some(Ok(Message::Pong(_)))) => {}
            Ok(Some(Ok(Message::Text(text)))) => {
                let message: Value = serde_json::from_str(text.as_str())?;
                let message_type = message
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                return Err(failure(format!(
                    "worker sent {message_type} before publish acknowledgement"
                )));
            }
            Ok(Some(Ok(_))) => {
                return Err(failure(
                    "worker sent an unsupported frame before publish acknowledgement",
                ));
            }
        }
    }

    send_invocation_result(socket, invocation_id, function_id, Value::Null).await
}

async fn send_harness_invocation(
    socket: &mut WebSocketStream<TcpStream>,
    case: &HarnessCase,
) -> SmokeResult<()> {
    socket
        .send(Message::Text(
            json!({
                "type": "invokefunction",
                "invocation_id": case.invocation_id,
                "function_id": case.function_id,
                "data": case.input,
            })
            .to_string()
            .into(),
        ))
        .await?;
    Ok(())
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
    if !message.get("data").is_some_and(Value::is_object) {
        return Err(failure("worker registration metadata was not an object"));
    }
    Ok(())
}

fn validate_functions_info_request(message: &Value) -> SmokeResult<()> {
    if message.get("action").is_some_and(|value| !value.is_null()) {
        return Err(failure("function catalog lookup used an unexpected action"));
    }
    if message.get("namespace").and_then(Value::as_str) != Some("default") {
        return Err(failure(
            "function catalog lookup did not target the engine namespace",
        ));
    }
    let data = message
        .get("data")
        .and_then(Value::as_object)
        .ok_or_else(|| failure("function catalog lookup data was not an object"))?;
    let requested_ids = data
        .get("function_ids")
        .and_then(Value::as_array)
        .ok_or_else(|| failure("function catalog lookup did not list function ids"))?
        .iter()
        .map(|value| value.as_str().map(str::to_owned))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| failure("function catalog lookup contained a non-string function id"))?;
    let expected_ids = FUNCTION_IDS
        .iter()
        .map(|function_id| (*function_id).to_owned())
        .collect::<Vec<_>>();
    if requested_ids != expected_ids {
        return Err(failure(
            "function catalog lookup requested the wrong function ids",
        ));
    }
    if data.get("namespace").and_then(Value::as_str) != Some(NAMESPACE) {
        return Err(failure(
            "function catalog lookup requested the wrong catalog namespace",
        ));
    }
    Ok(())
}

fn validate_publish_payload(message: &Value, case: &HarnessCase) -> SmokeResult<()> {
    if message.get("action").is_some_and(|value| !value.is_null()) {
        return Err(failure("publish invocation used an unexpected action"));
    }
    if message.get("namespace").and_then(Value::as_str) != Some(NAMESPACE) {
        return Err(failure("publish invocation used the wrong namespace"));
    }
    let expected = json!({
        "topic": QUEUE_TOPIC,
        "data": case.event,
    });
    if message.get("data") != Some(&expected) {
        return Err(failure(format!(
            "publish payload for {} did not match the expected event",
            case.function_id
        )));
    }
    Ok(())
}

fn validate_harness_result(message: &Value, case: &HarnessCase) -> SmokeResult<()> {
    if message.get("function_id").and_then(Value::as_str) != Some(case.function_id) {
        return Err(failure("harness result used the wrong function id"));
    }
    if message.get("invocation_id").and_then(Value::as_str) != Some(case.invocation_id) {
        return Err(failure("harness response used the wrong invocation id"));
    }
    if message.get("result") != Some(&json!({"dispatched": true})) {
        return Err(failure(
            "harness function did not report dispatched success",
        ));
    }
    Ok(())
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
            "--help" | "-h" => {
                return Err(usage().to_owned());
            }
            unknown => return Err(format!("unknown argument {unknown:?}\n\n{}", usage())),
        }
    }

    Ok(Arguments {
        manifest: manifest.ok_or_else(|| format!("--manifest is required\n\n{}", usage()))?,
        timeout: operation_timeout,
    })
}

fn usage() -> &'static str {
    "Usage: harness-engine-fake --manifest <path> [--timeout-seconds <seconds>]"
}

fn failure(message: impl Into<String>) -> BoxError {
    Box::new(io::Error::other(message.into()))
}

#[cfg(test)]
mod tests {
    use super::{
        Arguments, FUNCTION_IDS, NAMESPACE, QUEUE_TOPIC, RegistrationState, WORKER_NAME,
        WorkerLaunch, harness_cases, parse_arguments, parse_worker_launch, terminate_worker,
        validate_publish_payload,
    };
    use serde_json::json;
    use std::{
        path::{Path, PathBuf},
        time::Duration,
    };

    #[test]
    fn arguments_require_a_manifest_path() {
        let error = parse_arguments(["harness-engine-fake".to_owned()]).unwrap_err();
        assert!(error.contains("--manifest is required"));
    }

    #[test]
    fn arguments_accept_a_custom_timeout() {
        let arguments = parse_arguments([
            "harness-engine-fake".to_owned(),
            "--manifest".to_owned(),
            "/tmp/harness-ingestion/iii.worker.yaml".to_owned(),
            "--timeout-seconds".to_owned(),
            "12".to_owned(),
        ])
        .unwrap();

        assert_eq!(
            arguments,
            Arguments {
                manifest: PathBuf::from("/tmp/harness-ingestion/iii.worker.yaml"),
                timeout: Duration::from_secs(12),
            }
        );
    }

    #[test]
    fn manifest_start_command_is_resolved_from_scripts() {
        let launch = parse_worker_launch(
            "name: harness-ingestion\nscripts:\n  start: ../../target/release/harness-ingestion\n",
            Path::new("/repo/workers/harness-ingestion/iii.worker.yaml"),
        )
        .unwrap();

        assert_eq!(
            launch,
            WorkerLaunch {
                working_directory: PathBuf::from("/repo/workers/harness-ingestion"),
                start: "../../target/release/harness-ingestion".to_owned(),
            }
        );
    }

    #[test]
    fn manifest_rejects_the_unsupported_top_level_start_field() {
        let error = parse_worker_launch(
            "name: harness-ingestion\nstart: ../../target/release/harness-ingestion\n",
            Path::new("/repo/workers/harness-ingestion/iii.worker.yaml"),
        )
        .unwrap_err();

        assert!(error.to_string().contains("scripts.start"));
    }

    #[test]
    fn harness_cases_cover_each_registered_function() {
        let cases = harness_cases();

        assert_eq!(
            cases
                .iter()
                .map(|case| case.function_id)
                .collect::<Vec<_>>(),
            FUNCTION_IDS.to_vec(),
        );
        assert_eq!(cases[1].event["event_type"], "observation");
        assert_eq!(cases[1].event["data"]["nested"]["enabled"], true);
        assert_eq!(cases[2].event["event_type"], "session_end");
    }

    #[test]
    fn publish_payload_rejects_extra_event_fields() {
        let case = harness_cases().into_iter().next().unwrap();
        let mut event = case.event.clone();
        event
            .as_object_mut()
            .unwrap()
            .insert("unexpected".to_owned(), json!(true));
        let message = json!({
            "action": null,
            "namespace": NAMESPACE,
            "data": {
                "topic": QUEUE_TOPIC,
                "data": event,
            },
        });

        assert!(validate_publish_payload(&message, &case).is_err());
    }

    #[tokio::test]
    async fn cleanup_rejects_a_nonzero_worker_exit() {
        let mut worker = tokio::process::Command::new("sh")
            .args(["-c", "exit 7"])
            .spawn()
            .unwrap();
        worker.wait().await.unwrap();

        let error = terminate_worker(&mut worker).await.unwrap_err();

        assert!(error.to_string().contains("worker exited unsuccessfully"));
    }

    #[test]
    fn registrations_must_share_worker_namespace_and_nonce() {
        let mut state = RegistrationState::default();
        for function_id in [
            "harness::session_start",
            "harness::observation",
            "harness::session_end",
        ] {
            state
                .add(
                    function_id.to_owned(),
                    json!({
                        "worker_name": WORKER_NAME,
                        "namespace": NAMESPACE,
                        "registration_nonce": "nonce",
                    }),
                )
                .unwrap();
        }

        assert!(state.is_complete());
        assert_eq!(state.nonce().unwrap(), "nonce");
    }
}
