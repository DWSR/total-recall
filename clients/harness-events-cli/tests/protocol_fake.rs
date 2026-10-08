mod support;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::{Duration, timeout},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use support::engine::{
    CapturedInvocation, FakeEngine, FakeEngineConfig, InvocationOutcome, RegistrationOutcome,
};

const IDENTITY: &str = "harness-events-cli-test";
const REGISTRATION_NAMESPACE: &str = "registration-space";
const INVOCATION_NAMESPACE: &str = "invocation-space";
const FUNCTION_ID: &str = "harness::session_start";
const INVOCATION_ID: &str = "00000000-0000-4000-8000-000000000001";
const FAKE_WORKER_ID: &str = "harness-events-cli-fake-worker";
const FAKE_OWNER_WORKER_ID: &str = "harness-events-cli-fake-engine";

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn connect(engine: &FakeEngine) -> Socket {
    connect_async(engine.url())
        .await
        .expect("fake engine accepts a WebSocket connection")
        .0
}

async fn send_json<S>(socket: &mut WebSocketStream<S>, value: Value)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .expect("send protocol frame");
}

async fn receive_json<S>(socket: &mut WebSocketStream<S>) -> Value
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let frame = socket
        .next()
        .await
        .expect("fake engine sends a response")
        .expect("fake engine response is valid");
    let Message::Text(text) = frame else {
        panic!("fake engine response must be text, got {frame:?}");
    };

    serde_json::from_str(text.as_str()).expect("fake engine response is JSON")
}

async fn send_registration(socket: &mut Socket) {
    send_json(
        socket,
        json!({
            "type": "invokefunction",
            "invocation_id": null,
            "function_id": "engine::workers::register",
            "data": {
                "name": IDENTITY,
                "namespace": REGISTRATION_NAMESPACE,
            },
            "action": {"type": "void"},
        }),
    )
    .await;
}

async fn send_default_namespace_registration(socket: &mut Socket) {
    send_json(
        socket,
        json!({
            "type": "invokefunction",
            "invocation_id": null,
            "function_id": "engine::workers::register",
            "data": {"name": IDENTITY},
            "action": {"type": "void"},
        }),
    )
    .await;
}

async fn send_invocation(socket: &mut Socket, payload: Value) {
    send_json(
        socket,
        json!({
            "type": "invokefunction",
            "invocation_id": INVOCATION_ID,
            "function_id": FUNCTION_ID,
            "namespace": INVOCATION_NAMESPACE,
            "data": payload,
        }),
    )
    .await;
}

async fn close<S>(socket: &mut WebSocketStream<S>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    socket
        .close(None)
        .await
        .expect("close WebSocket connection");
}

#[tokio::test]
async fn rejects_wrong_frame_type_without_a_response_or_state_change() {
    let mut engine = FakeEngine::start(FakeEngineConfig::default()).await;
    let mut socket = connect(&engine).await;

    send_json(
        &mut socket,
        json!({
            "type": "workerregistered",
            "function_id": "engine::workers::register",
            "data": {
                "name": IDENTITY,
                "namespace": REGISTRATION_NAMESPACE,
            },
        }),
    )
    .await;

    let outcome = socket.next().await;
    assert!(
        matches!(outcome, None | Some(Err(_))),
        "wrong frame type must not receive a successful response, got {outcome:?}"
    );
    assert_eq!(
        engine
            .finish()
            .await
            .expect_err("fake engine reports the wrong frame type"),
        "protocol fake received unsupported frame type: workerregistered"
    );
    assert_eq!(engine.captured().await, CapturedInvocation::default());
}

#[tokio::test]
async fn accepts_registration_captures_wire_values_and_returns_success() {
    let mut engine = FakeEngine::start(FakeEngineConfig::default()).await;
    let mut socket = connect(&engine).await;

    socket
        .send(Message::Ping(vec![1, 2, 3].into()))
        .await
        .expect("send ping");
    let pong = socket
        .next()
        .await
        .expect("fake engine sends pong")
        .expect("pong is valid");
    assert_eq!(pong, Message::Pong(vec![1, 2, 3].into()));

    send_registration(&mut socket).await;
    assert_eq!(
        receive_json(&mut socket).await,
        json!({
            "type": "workerregistered",
            "worker_id": FAKE_WORKER_ID,
        })
    );

    let payload = json!({"session_id": "session-1", "nested": {"count": 2}});
    send_invocation(&mut socket, payload.clone()).await;
    assert_eq!(
        receive_json(&mut socket).await,
        json!({
            "type": "invocationresult",
            "invocation_id": INVOCATION_ID,
            "function_id": FUNCTION_ID,
            "result": {"dispatched": true},
        })
    );

    close(&mut socket).await;
    engine.finish().await.expect("fake engine completes");
    let captured = engine.captured().await;
    assert_eq!(captured.identity.as_deref(), Some(IDENTITY));
    assert_eq!(
        captured.registration_namespace.as_deref(),
        Some(REGISTRATION_NAMESPACE)
    );
    assert_eq!(
        captured.invocation_namespace.as_deref(),
        Some(INVOCATION_NAMESPACE)
    );
    assert_eq!(captured.function_id.as_deref(), Some(FUNCTION_ID));
    assert_eq!(captured.payload, Some(payload));
    assert_eq!(captured.invocation_count, 1);
    assert!(captured.shutdown_observed);
}

#[tokio::test]
async fn rejects_registration_with_a_fatal_worker_namespace_conflict() {
    let mut engine = FakeEngine::start(FakeEngineConfig {
        registration: RegistrationOutcome::Rejected,
        ..FakeEngineConfig::default()
    })
    .await;
    let mut socket = connect(&engine).await;

    send_registration(&mut socket).await;
    assert_eq!(
        receive_json(&mut socket).await,
        json!({
            "type": "registrationrejected",
            "code": "WORKER_NAMESPACE_CONFLICT",
            "namespace": REGISTRATION_NAMESPACE,
            "worker_name": IDENTITY,
            "owner_worker_id": FAKE_OWNER_WORKER_ID,
        })
    );

    close(&mut socket).await;
    engine.finish().await.expect("fake engine completes");
    let captured = engine.captured().await;
    assert_eq!(captured.identity.as_deref(), Some(IDENTITY));
    assert_eq!(
        captured.registration_namespace.as_deref(),
        Some(REGISTRATION_NAMESPACE)
    );
    assert_eq!(captured.invocation_count, 0);
    assert!(captured.shutdown_observed);
}

#[tokio::test]
async fn rejects_default_namespace_registration_with_a_protocol_valid_namespace() {
    let mut engine = FakeEngine::start(FakeEngineConfig {
        registration: RegistrationOutcome::Rejected,
        ..FakeEngineConfig::default()
    })
    .await;
    let mut socket = connect(&engine).await;

    send_default_namespace_registration(&mut socket).await;
    assert_eq!(
        receive_json(&mut socket).await,
        json!({
            "type": "registrationrejected",
            "code": "WORKER_NAMESPACE_CONFLICT",
            "namespace": "default",
            "worker_name": IDENTITY,
            "owner_worker_id": FAKE_OWNER_WORKER_ID,
        })
    );

    close(&mut socket).await;
    engine.finish().await.expect("fake engine completes");
    let captured = engine.captured().await;
    assert!(captured.registration_namespace.is_none());
    assert!(captured.shutdown_observed);
}

#[tokio::test]
async fn silently_holds_registration_open_until_client_shutdown() {
    let mut engine = FakeEngine::start(FakeEngineConfig {
        registration: RegistrationOutcome::Silent,
        ..FakeEngineConfig::default()
    })
    .await;
    let mut socket = connect(&engine).await;

    send_registration(&mut socket).await;
    assert!(
        timeout(Duration::from_millis(50), socket.next())
            .await
            .is_err(),
        "silent registration must not send a response"
    );

    close(&mut socket).await;
    engine.finish().await.expect("fake engine completes");
    let captured = engine.captured().await;
    assert_eq!(captured.identity.as_deref(), Some(IDENTITY));
    assert_eq!(captured.invocation_count, 0);
    assert!(captured.shutdown_observed);
}

#[tokio::test]
async fn returns_a_remote_rejection() {
    let mut engine = FakeEngine::start(FakeEngineConfig {
        invocation: InvocationOutcome::RemoteRejection,
        ..FakeEngineConfig::default()
    })
    .await;
    let mut socket = connect(&engine).await;

    send_registration(&mut socket).await;
    receive_json(&mut socket).await;
    send_invocation(&mut socket, json!({"session_id": "session-1"})).await;
    assert_eq!(
        receive_json(&mut socket).await,
        json!({
            "type": "invocationresult",
            "invocation_id": INVOCATION_ID,
            "function_id": FUNCTION_ID,
            "error": {
                "code": "FAKE_REMOTE_REJECTION",
                "message": "fake engine rejected the invocation",
            },
        })
    );

    close(&mut socket).await;
    engine.finish().await.expect("fake engine completes");
}

#[tokio::test]
async fn silently_holds_an_invocation_open_until_client_shutdown() {
    let mut engine = FakeEngine::start(FakeEngineConfig {
        invocation: InvocationOutcome::Silent,
        ..FakeEngineConfig::default()
    })
    .await;
    let mut socket = connect(&engine).await;

    send_registration(&mut socket).await;
    receive_json(&mut socket).await;
    send_invocation(&mut socket, json!({"session_id": "session-1"})).await;
    assert!(
        timeout(Duration::from_millis(50), socket.next())
            .await
            .is_err(),
        "silent invocation must not send a response"
    );

    close(&mut socket).await;
    engine.finish().await.expect("fake engine completes");
    let captured = engine.captured().await;
    assert_eq!(captured.invocation_count, 1);
    assert!(captured.shutdown_observed);
}

#[tokio::test]
async fn returns_a_protocol_valid_application_invalid_result() {
    let mut engine = FakeEngine::start(FakeEngineConfig {
        invocation: InvocationOutcome::ApplicationInvalid,
        ..FakeEngineConfig::default()
    })
    .await;
    let mut socket = connect(&engine).await;

    send_registration(&mut socket).await;
    receive_json(&mut socket).await;
    send_invocation(&mut socket, json!({"session_id": "session-1"})).await;
    assert_eq!(
        receive_json(&mut socket).await,
        json!({
            "type": "invocationresult",
            "invocation_id": INVOCATION_ID,
            "function_id": FUNCTION_ID,
            "result": {"dispatched": false},
        })
    );

    close(&mut socket).await;
    engine.finish().await.expect("fake engine completes");
}

#[tokio::test]
async fn drops_the_connection_after_an_invocation() {
    let mut engine = FakeEngine::start(FakeEngineConfig {
        invocation: InvocationOutcome::DroppedConnection,
        ..FakeEngineConfig::default()
    })
    .await;
    let mut socket = connect(&engine).await;

    send_registration(&mut socket).await;
    receive_json(&mut socket).await;
    send_invocation(&mut socket, json!({"session_id": "session-1"})).await;

    let outcome = socket.next().await;
    assert!(
        matches!(outcome, None | Some(Err(_))),
        "dropped connection must end with EOF or a transport error, got {outcome:?}"
    );

    engine.finish().await.expect("fake engine completes");
}
