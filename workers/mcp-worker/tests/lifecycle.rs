use std::{
    io::{BufRead, BufReader, Read, Write},
    process::{Child, ChildStderr, ChildStdout, Command, Stdio},
    time::{Duration, Instant},
};

use futures_util::{SinkExt, StreamExt};
use mcp_worker::config::{
    III_NAMESPACE_ENV, III_URL_ENV, III_WORKER_NAME_ENV, TOTAL_RECALL_EMBEDDING_MODEL_ENV,
    TOTAL_RECALL_EMBEDDING_PROVIDER_ENV, TOTAL_RECALL_MCP_CHANNEL_CAPACITY_ENV,
    TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS_ENV, TOTAL_RECALL_MCP_MAX_IN_FLIGHT_ENV,
    TOTAL_RECALL_MCP_MAX_LINE_BYTES_ENV, TOTAL_RECALL_MEMORY_DATABASE_ENV,
};
use serde_json::{Value, json};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::timeout,
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

const DATABASE: &str = "mcp-worker-lifecycle";
const WORKER_NAME: &str = "mcp-worker-lifecycle";
const NAMESPACE: &str = "mcp-worker-lifecycle";
const EMBEDDING_PROVIDER: &str = "lifecycle-provider-secret-sentinel";
const EMBEDDING_MODEL: &str = "lifecycle-model-secret-sentinel";
const EMBEDDING_TIMEOUT_MS: &str = "1234";
const PROCESS_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone, Copy)]
enum QueryEmbedding {
    Disabled,
    Enabled,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tool_call_reaches_database_over_the_connected_iii_client() {
    assert_tool_call_lifecycle(QueryEmbedding::Disabled).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enabled_query_embedding_serves_a_tool_call_without_router_invocations() {
    assert_tool_call_lifecycle(QueryEmbedding::Enabled).await;
}

async fn assert_tool_call_lifecycle(query_embedding: QueryEmbedding) {
    let (engine_url, registered, engine) = start_fake_engine(true).await;
    let mut child = worker_command(&engine_url, query_embedding)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("worker binary should start");
    let stderr = child.stderr.take().expect("worker stderr should be piped");

    await_registration(registered, &mut child, &engine).await;

    let mut stdin = child.stdin.take().expect("worker stdin should be piped");
    let request = json!({
        "jsonrpc": "2.0",
        "id": "list-versions",
        "method": "tools/call",
        "params": {
            "_meta": request_meta(),
            "name": "memory_list_versions",
            "arguments": { "id": "lifecycle-memory", "limit": 1 },
        },
    });
    stdin
        .write_all(format!("{request}\n").as_bytes())
        .expect("tool request should be written");

    let stdout = child.stdout.take().expect("worker stdout should be piped");
    let response = match timeout(PROCESS_TIMEOUT, read_response(stdout)).await {
        Ok(response) => response,
        Err(_) => {
            terminate_child(&mut child);
            engine.abort();
            panic!("tool response exceeded the lifecycle timeout");
        }
    };
    if response.trim().is_empty() {
        drop(stdin);
        let status = child
            .wait()
            .expect("worker should be reaped after stdout closes");
        let stderr = read_stderr(stderr).await;
        engine.abort();
        panic!("worker closed stdout before its tool response: {status}; {stderr}");
    }
    let response: Value = serde_json::from_str(response.trim())
        .expect("tool response should be one JSON-RPC message");
    assert_eq!(response["id"], "list-versions");
    assert_eq!(
        response["result"]["structuredContent"]["versions"],
        json!([])
    );

    drop(stdin);
    assert_worker_exits_successfully(&mut child).await;
    assert!(
        read_stderr(stderr).await.is_empty(),
        "clean tool lifecycle should not write diagnostics"
    );
    timeout(PROCESS_TIMEOUT, engine)
        .await
        .expect("fake engine should observe client shutdown")
        .expect("fake engine task should not panic");
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigint_terminates_with_stdin_held_open() {
    assert_signal_shutdown("INT", QueryEmbedding::Disabled).await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_terminates_with_stdin_held_open() {
    assert_signal_shutdown("TERM", QueryEmbedding::Disabled).await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signals_terminate_enabled_query_embedding_with_stdin_held_open() {
    for signal in ["INT", "TERM"] {
        assert_signal_shutdown(signal, QueryEmbedding::Enabled).await;
    }
}

#[cfg(unix)]
async fn assert_signal_shutdown(signal: &str, query_embedding: QueryEmbedding) {
    let (engine_url, registered, engine) = start_fake_engine(false).await;
    let mut child = worker_command(&engine_url, query_embedding)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("worker binary should start");
    let stderr = child.stderr.take().expect("worker stderr should be piped");
    let mut stdin = child.stdin.take().expect("worker stdin should be piped");

    await_registration(registered, &mut child, &engine).await;
    stdin
        .write_all(b"{")
        .expect("partial frame should be written");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let pid = child.id();
    let status = Command::new("kill")
        .args([format!("-{signal}"), pid.to_string()])
        .status()
        .expect("signal command should run");
    assert!(status.success(), "signal command should target the worker");

    assert_worker_exits_successfully(&mut child).await;
    drop(stdin);
    assert!(
        read_stderr(stderr).await.is_empty(),
        "signal shutdown should not write diagnostics"
    );
    timeout(PROCESS_TIMEOUT, engine)
        .await
        .expect("fake engine should observe client shutdown")
        .expect("fake engine task should not panic");
}

async fn start_fake_engine(
    expect_database_call: bool,
) -> (String, oneshot::Receiver<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("fake engine should bind a loopback port");
    let engine_url = format!(
        "ws://{}",
        listener
            .local_addr()
            .expect("fake engine should expose a local address")
    );
    let (registered_sender, registered_receiver) = oneshot::channel();
    let engine = tokio::spawn(run_fake_engine(
        listener,
        registered_sender,
        expect_database_call,
    ));
    (engine_url, registered_receiver, engine)
}

async fn run_fake_engine(
    listener: TcpListener,
    registered_sender: oneshot::Sender<()>,
    expect_database_call: bool,
) {
    let (stream, _) = timeout(PROCESS_TIMEOUT, listener.accept())
        .await
        .expect("worker should connect to the fake iii listener")
        .expect("fake iii listener should accept a connection");
    let mut socket = accept_async(stream)
        .await
        .expect("worker should complete the WebSocket handshake");

    let registration = next_json(&mut socket).await;
    assert_eq!(registration["type"], "invokefunction");
    assert_eq!(registration["function_id"], "engine::workers::register");
    assert_eq!(registration["invocation_id"], Value::Null);
    assert_eq!(registration["action"], json!({ "type": "void" }));
    assert_eq!(registration["data"]["name"], WORKER_NAME);
    assert_eq!(registration["data"]["namespace"], NAMESPACE);
    send_json(
        &mut socket,
        json!({
            "type": "workerregistered",
            "worker_id": "mcp-worker-lifecycle-engine",
        }),
    )
    .await;
    registered_sender
        .send(())
        .expect("registration observation should be delivered once");

    if expect_database_call {
        let invocation = next_json(&mut socket).await;
        assert_eq!(invocation["type"], "invokefunction");
        assert_eq!(invocation["function_id"], "database::execute");
        assert_eq!(invocation["data"]["db"], DATABASE);
        assert_eq!(
            invocation["data"]["params"],
            json!(["lifecycle-memory", "0", "2"])
        );
        let invocation_id = invocation["invocation_id"]
            .as_str()
            .expect("database invocation should have an ID");
        send_json(
            &mut socket,
            json!({
                "type": "invocationresult",
                "invocation_id": invocation_id,
                "function_id": "database::execute",
                "result": {
                    "affected_rows": 0,
                    "last_insert_id": null,
                    "returned_rows": [],
                },
            }),
        )
        .await;
    }

    wait_for_disconnect(&mut socket).await;
}

async fn next_json(socket: &mut WebSocketStream<TcpStream>) -> Value {
    loop {
        let frame = timeout(PROCESS_TIMEOUT, socket.next())
            .await
            .expect("fake iii listener should receive a frame")
            .expect("worker should keep the connection open")
            .expect("worker WebSocket frame should be valid");
        match frame {
            Message::Text(text) => {
                return serde_json::from_str(text.as_str())
                    .expect("worker WebSocket text frame should be JSON");
            }
            Message::Ping(payload) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .expect("fake iii listener should answer pings");
            }
            Message::Pong(_) => {}
            Message::Close(_) => panic!("worker closed before the expected iii call"),
            Message::Binary(_) | Message::Frame(_) => {
                panic!("worker should use JSON text frames")
            }
        }
    }
}

async fn send_json(socket: &mut WebSocketStream<TcpStream>, message: Value) {
    socket
        .send(Message::Text(message.to_string().into()))
        .await
        .expect("fake iii listener should send a response");
}

async fn wait_for_disconnect(socket: &mut WebSocketStream<TcpStream>) {
    loop {
        let frame = timeout(PROCESS_TIMEOUT, socket.next())
            .await
            .expect("worker should close the iii connection during shutdown");
        match frame {
            None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return,
            Some(Ok(Message::Ping(payload))) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .expect("fake iii listener should answer pings");
            }
            Some(Ok(Message::Pong(_))) => {}
            Some(Ok(Message::Text(_)))
            | Some(Ok(Message::Binary(_)))
            | Some(Ok(Message::Frame(_))) => {
                panic!("worker sent an unexpected iii frame while shutting down")
            }
        }
    }
}

async fn await_registration(
    registered: oneshot::Receiver<()>,
    child: &mut Child,
    engine: &tokio::task::JoinHandle<()>,
) {
    match timeout(PROCESS_TIMEOUT, registered).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => {
            terminate_child(child);
            engine.abort();
            panic!("fake iii listener closed before worker registration");
        }
        Err(_) => {
            terminate_child(child);
            engine.abort();
            panic!("worker registration exceeded the lifecycle timeout");
        }
    }
}

async fn read_response(stdout: ChildStdout) -> String {
    tokio::task::spawn_blocking(move || {
        let mut line = String::new();
        BufReader::new(stdout)
            .read_line(&mut line)
            .expect("worker stdout should be readable");
        line
    })
    .await
    .expect("stdout reader should not panic")
}

async fn read_stderr(stderr: ChildStderr) -> String {
    tokio::task::spawn_blocking(move || {
        let mut stderr = BufReader::new(stderr);
        let mut output = String::new();
        stderr
            .read_to_string(&mut output)
            .expect("worker stderr should be readable");
        output
    })
    .await
    .expect("stderr reader should not panic")
}

async fn assert_worker_exits_successfully(child: &mut Child) {
    let deadline = Instant::now() + PROCESS_TIMEOUT;
    loop {
        match child.try_wait().expect("worker status should be readable") {
            Some(status) => {
                assert!(
                    status.success(),
                    "worker should exit successfully: {status}"
                );
                return;
            }
            None if Instant::now() >= deadline => {
                terminate_child(child);
                panic!("worker did not exit within the lifecycle timeout");
            }
            None => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
}

fn terminate_child(child: &mut Child) {
    if child
        .try_wait()
        .expect("worker status should be readable during cleanup")
        .is_none()
    {
        let _ = child.kill();
    }
    let _ = child.wait();
}

fn worker_command(engine_url: &str, query_embedding: QueryEmbedding) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mcp-worker"));
    command
        .env(TOTAL_RECALL_MEMORY_DATABASE_ENV, DATABASE)
        .env(III_URL_ENV, engine_url)
        .env(III_WORKER_NAME_ENV, WORKER_NAME)
        .env(III_NAMESPACE_ENV, NAMESPACE)
        .env_remove(TOTAL_RECALL_MCP_MAX_LINE_BYTES_ENV)
        .env_remove(TOTAL_RECALL_MCP_CHANNEL_CAPACITY_ENV)
        .env_remove(TOTAL_RECALL_MCP_MAX_IN_FLIGHT_ENV);
    match query_embedding {
        QueryEmbedding::Disabled => {
            command
                .env_remove(TOTAL_RECALL_EMBEDDING_PROVIDER_ENV)
                .env_remove(TOTAL_RECALL_EMBEDDING_MODEL_ENV)
                .env_remove(TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS_ENV);
        }
        QueryEmbedding::Enabled => {
            command
                .env(TOTAL_RECALL_EMBEDDING_PROVIDER_ENV, EMBEDDING_PROVIDER)
                .env(TOTAL_RECALL_EMBEDDING_MODEL_ENV, EMBEDDING_MODEL)
                .env(
                    TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS_ENV,
                    EMBEDDING_TIMEOUT_MS,
                );
        }
    }
    command
}

fn request_meta() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {},
    })
}
