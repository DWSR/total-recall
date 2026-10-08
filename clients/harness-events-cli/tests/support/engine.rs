use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use iii_sdk::protocol::WORKER_NAMESPACE_CONFLICT;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::{Mutex, oneshot},
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

const WORKER_REGISTRATION_FUNCTION: &str = "engine::workers::register";
const WORKER_ID: &str = "harness-events-cli-fake-worker";
const OWNER_WORKER_ID: &str = "harness-events-cli-fake-engine";
const DEFAULT_NAMESPACE: &str = "default";
const SERVER_FRAME_DEADLINE: Duration = Duration::from_secs(5);

type EngineResult<T> = Result<T, String>;

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RegistrationOutcome {
    Accepted,
    Rejected,
    ConnectionFailure,
    Silent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InvocationOutcome {
    Success,
    RemoteRejection,
    ApplicationInvalid,
    DroppedConnection,
    Silent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FakeEngineConfig {
    pub(crate) registration: RegistrationOutcome,
    pub(crate) invocation: InvocationOutcome,
}

impl Default for FakeEngineConfig {
    fn default() -> Self {
        Self {
            registration: RegistrationOutcome::Accepted,
            invocation: InvocationOutcome::Success,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct CapturedInvocation {
    pub(crate) identity: Option<String>,
    pub(crate) registration_namespace: Option<String>,
    pub(crate) invocation_namespace: Option<String>,
    pub(crate) function_id: Option<String>,
    pub(crate) payload: Option<Value>,
    pub(crate) invocation_count: usize,
    pub(crate) shutdown_observed: bool,
}

pub(crate) struct FakeEngine {
    url: String,
    captured: Arc<Mutex<CapturedInvocation>>,
    connection_count: Arc<AtomicUsize>,
    cancellation: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<EngineResult<()>>>,
}

impl FakeEngine {
    pub(crate) async fn start(config: FakeEngineConfig) -> Self {
        Self::start_with_listener_deadline(config, SERVER_FRAME_DEADLINE).await
    }

    pub(crate) async fn start_with_listener_deadline(
        config: FakeEngineConfig,
        listener_deadline: Duration,
    ) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind protocol fake listener");
        let url = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("read protocol fake listener address")
        );
        let captured = Arc::new(Mutex::new(CapturedInvocation::default()));
        let task_captured = Arc::clone(&captured);
        let connection_count = Arc::new(AtomicUsize::new(0));
        let task_connection_count = Arc::clone(&connection_count);
        let (cancellation, cancellation_receiver) = oneshot::channel();
        let task = tokio::spawn(async move {
            serve(
                listener,
                task_captured,
                task_connection_count,
                config,
                cancellation_receiver,
                listener_deadline,
            )
            .await
        });
        tokio::task::yield_now().await;

        Self {
            url,
            captured,
            connection_count,
            cancellation: Some(cancellation),
            task: Some(task),
        }
    }

    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    pub(crate) async fn captured(&self) -> CapturedInvocation {
        self.captured.lock().await.clone()
    }

    pub(crate) fn connection_count(&self) -> usize {
        self.connection_count.load(Ordering::SeqCst)
    }

    pub(crate) async fn finish(&mut self) -> Result<(), String> {
        let result = self
            .task
            .as_mut()
            .ok_or_else(|| "protocol fake was already finished".to_owned())?
            .await;
        self.task.take();
        result.map_err(|error| format!("protocol fake task failed: {error}"))?
    }

    pub(crate) async fn cancel(&mut self) -> Result<(), String> {
        self.cancellation
            .take()
            .ok_or_else(|| "protocol fake cancellation was already requested".to_owned())?
            .send(())
            .map_err(|_| "protocol fake cancellation receiver was unavailable".to_owned())?;
        self.finish().await
    }

    pub(crate) async fn finish_without_connection(&mut self) -> Result<(), String> {
        self.cancel().await?;
        if self.connection_count() != 0 {
            return Err("protocol fake unexpectedly received a TCP connection".to_owned());
        }

        Ok(())
    }
}

impl Drop for FakeEngine {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn serve(
    listener: TcpListener,
    captured: Arc<Mutex<CapturedInvocation>>,
    connection_count: Arc<AtomicUsize>,
    config: FakeEngineConfig,
    mut cancellation: oneshot::Receiver<()>,
    listener_deadline: Duration,
) -> EngineResult<()> {
    let mut dropped_invocation = None;

    loop {
        let (stream, _) = tokio::select! {
            biased;
            connection = timeout(listener_deadline, listener.accept()) => match connection {
                Ok(Ok(connection)) => connection,
                Ok(Err(error)) => {
                    return Err(format!("protocol fake TCP accept failed: {error}"));
                }
                Err(_) if dropped_invocation.is_some() => return Ok(()),
                Err(_) => {
                    return Err("protocol fake did not receive a TCP connection".to_owned());
                }
            },
            cancelled = &mut cancellation => {
                cancelled.map_err(|_| "protocol fake cancellation channel closed".to_owned())?;
                match accept_pending_connection(listener) {
                    Ok(true) => {
                        connection_count.fetch_add(1, Ordering::SeqCst);
                        return Ok(());
                    }
                    Ok(false) => return Ok(()),
                    Err(error) => {
                        return Err(format!("protocol fake TCP accept failed: {error}"));
                    }
                }
            }
        };
        connection_count.fetch_add(1, Ordering::SeqCst);
        let mut socket = tokio::select! {
            handshake = timeout(SERVER_FRAME_DEADLINE, accept_async(stream)) => handshake
                .map_err(|_| "protocol fake WebSocket handshake did not complete".to_owned())?
                .map_err(|error| format!("protocol fake WebSocket handshake failed: {error}"))?,
            cancelled = &mut cancellation => {
                cancelled.map_err(|_| "protocol fake cancellation channel closed".to_owned())?;
                return Ok(());
            }
        };

        let continue_serving = serve_connection(
            &mut socket,
            Arc::clone(&captured),
            config,
            &mut cancellation,
            &mut dropped_invocation,
        )
        .await?;
        if !continue_serving {
            return Ok(());
        }
    }
}

fn accept_pending_connection(listener: TcpListener) -> io::Result<bool> {
    let listener = listener.into_std()?;
    match listener.accept() {
        Ok((stream, _)) => {
            drop(stream);
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(false),
        Err(error) => Err(error),
    }
}

async fn serve_connection(
    socket: &mut WebSocketStream<TcpStream>,
    captured: Arc<Mutex<CapturedInvocation>>,
    config: FakeEngineConfig,
    cancellation: &mut oneshot::Receiver<()>,
    dropped_invocation: &mut Option<String>,
) -> EngineResult<bool> {
    let deadline = Instant::now() + SERVER_FRAME_DEADLINE;
    let mut registered = false;
    let mut invocation_completed = false;

    loop {
        let next_frame = tokio::select! {
            frame = next_frame(socket, deadline) => frame?,
            cancelled = &mut *cancellation => {
                cancelled.map_err(|_| "protocol fake cancellation channel closed".to_owned())?;
                return Ok(false);
            }
        };
        let Some(frame) = next_frame else {
            record_shutdown(&captured).await;
            return Ok(false);
        };

        match frame {
            Message::Ping(payload) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|error| format!("protocol fake could not send pong: {error}"))?;
            }
            Message::Pong(_) => {}
            Message::Close(_) => {
                record_shutdown(&captured).await;
                return Ok(false);
            }
            Message::Text(text) => {
                let message: Value = serde_json::from_str(text.as_str())
                    .map_err(|error| format!("protocol fake received invalid JSON: {error}"))?;
                let message_type = message
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "protocol fake frame did not include a type".to_owned())?;
                if message_type == "reattach" {
                    continue;
                }
                if message_type != "invokefunction" {
                    return Err(format!(
                        "protocol fake received unsupported frame type: {message_type}"
                    ));
                }
                let function_id = message
                    .get("function_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        "protocol fake invocation did not include a function id".to_owned()
                    })?;

                if function_id == WORKER_REGISTRATION_FUNCTION {
                    if registered {
                        return Err(
                            "protocol fake received duplicate worker registration".to_owned()
                        );
                    }

                    record_registration(&captured, &message).await?;
                    match config.registration {
                        RegistrationOutcome::Accepted => {
                            send_json(
                                socket,
                                json!({
                                    "type": "workerregistered",
                                    "worker_id": WORKER_ID,
                                }),
                            )
                            .await?;
                            registered = true;
                        }
                        RegistrationOutcome::Rejected => {
                            send_registration_rejection(
                                socket,
                                &captured,
                                WORKER_NAMESPACE_CONFLICT,
                            )
                            .await?;
                        }
                        RegistrationOutcome::ConnectionFailure => {
                            send_registration_rejection(
                                socket,
                                &captured,
                                "FAKE_CONNECTION_FAILURE",
                            )
                            .await?;
                        }
                        RegistrationOutcome::Silent => {
                            registered = true;
                        }
                    }
                    if matches!(config.registration, RegistrationOutcome::Accepted)
                        && let Some(invocation_id) = dropped_invocation.take()
                    {
                        let function_id = captured
                            .lock()
                            .await
                            .function_id
                            .clone()
                            .unwrap_or_default();
                        send_invocation_result(
                            socket,
                            &invocation_id,
                            &function_id,
                            None,
                            Some(json!({
                                "code": "FAKE_DROPPED_CONNECTION",
                                "message": "fake engine completed the dropped invocation",
                            })),
                        )
                        .await?;
                    }
                    continue;
                }

                if !registered {
                    return Err(
                        "protocol fake received an invocation before registration".to_owned()
                    );
                }
                if invocation_completed {
                    return Err(
                        "protocol fake received more than one function invocation".to_owned()
                    );
                }

                let (invocation_id, invocation_count) =
                    record_invocation(&captured, &message).await?;
                match config.invocation {
                    InvocationOutcome::Success => {
                        send_invocation_result(
                            socket,
                            &invocation_id,
                            function_id,
                            Some(json!({"dispatched": true})),
                            None,
                        )
                        .await?;
                    }
                    InvocationOutcome::RemoteRejection => {
                        send_invocation_result(
                            socket,
                            &invocation_id,
                            function_id,
                            None,
                            Some(json!({
                                "code": "FAKE_REMOTE_REJECTION",
                                "message": "fake engine rejected the invocation",
                            })),
                        )
                        .await?;
                    }
                    InvocationOutcome::ApplicationInvalid => {
                        send_invocation_result(
                            socket,
                            &invocation_id,
                            function_id,
                            Some(json!({"dispatched": false})),
                            None,
                        )
                        .await?;
                    }
                    InvocationOutcome::DroppedConnection => {
                        if invocation_count == 1 {
                            *dropped_invocation = Some(invocation_id);
                            return Ok(true);
                        }
                        send_invocation_result(
                            socket,
                            &invocation_id,
                            function_id,
                            None,
                            Some(json!({
                                "code": "FAKE_UNEXPECTED_APPLICATION_RETRY",
                                "message": "fake engine observed a second application invocation",
                            })),
                        )
                        .await?;
                    }
                    InvocationOutcome::Silent => {}
                }
                invocation_completed = true;
            }
            Message::Binary(_) => {
                return Err("protocol fake received an unsupported binary frame".to_owned());
            }
            _ => {}
        }
    }
}

async fn next_frame<S>(
    socket: &mut WebSocketStream<S>,
    deadline: Instant,
) -> EngineResult<Option<Message>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err("protocol fake server frame deadline expired".to_owned());
    }

    match timeout(remaining, socket.next())
        .await
        .map_err(|_| "protocol fake server frame deadline expired".to_owned())?
    {
        Some(Ok(frame)) => Ok(Some(frame)),
        Some(Err(error)) if is_client_shutdown_error(&error) => Ok(None),
        Some(Err(error)) => Err(format!("protocol fake WebSocket receive failed: {error}")),
        None => Ok(None),
    }
}

fn is_client_shutdown_error(error: &tokio_tungstenite::tungstenite::Error) -> bool {
    match error {
        tokio_tungstenite::tungstenite::Error::Protocol(
            tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
        ) => true,
        tokio_tungstenite::tungstenite::Error::Io(error)
            if error.kind() == io::ErrorKind::ConnectionReset =>
        {
            true
        }
        _ => false,
    }
}

async fn record_shutdown(captured: &Mutex<CapturedInvocation>) {
    captured.lock().await.shutdown_observed = true;
}

async fn record_registration(
    captured: &Mutex<CapturedInvocation>,
    message: &Value,
) -> EngineResult<()> {
    let data = message
        .get("data")
        .and_then(Value::as_object)
        .ok_or_else(|| "protocol fake registration did not include data".to_owned())?;
    let identity = data
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| "protocol fake registration did not include a worker name".to_owned())?;
    let registration_namespace = data
        .get("namespace")
        .and_then(Value::as_str)
        .map(str::to_owned);

    let mut captured = captured.lock().await;
    captured.identity = Some(identity.to_owned());
    captured.registration_namespace = registration_namespace;
    Ok(())
}

async fn send_registration_rejection(
    socket: &mut WebSocketStream<TcpStream>,
    captured: &Mutex<CapturedInvocation>,
    code: &str,
) -> EngineResult<()> {
    let captured = captured.lock().await.clone();
    let rejection_namespace = captured
        .registration_namespace
        .as_deref()
        .unwrap_or(DEFAULT_NAMESPACE);
    send_json(
        socket,
        json!({
            "type": "registrationrejected",
            "code": code,
            "namespace": rejection_namespace,
            "worker_name": captured.identity,
            "owner_worker_id": OWNER_WORKER_ID,
        }),
    )
    .await
}

async fn record_invocation(
    captured: &Mutex<CapturedInvocation>,
    message: &Value,
) -> EngineResult<(String, usize)> {
    let invocation_id = message
        .get("invocation_id")
        .and_then(Value::as_str)
        .ok_or_else(|| "protocol fake invocation did not include an invocation id".to_owned())?;
    let function_id = message
        .get("function_id")
        .and_then(Value::as_str)
        .ok_or_else(|| "protocol fake invocation did not include a function id".to_owned())?;
    let payload = message
        .get("data")
        .cloned()
        .ok_or_else(|| "protocol fake invocation did not include data".to_owned())?;
    let invocation_namespace = message
        .get("namespace")
        .and_then(Value::as_str)
        .map(str::to_owned);

    let mut captured = captured.lock().await;
    captured.invocation_count += 1;
    let invocation_count = captured.invocation_count;
    if invocation_count == 1 {
        captured.invocation_namespace = invocation_namespace;
        captured.function_id = Some(function_id.to_owned());
        captured.payload = Some(payload);
    }
    Ok((invocation_id.to_owned(), invocation_count))
}

async fn send_invocation_result(
    socket: &mut WebSocketStream<TcpStream>,
    invocation_id: &str,
    function_id: &str,
    result: Option<Value>,
    error: Option<Value>,
) -> EngineResult<()> {
    let mut response = json!({
        "type": "invocationresult",
        "invocation_id": invocation_id,
        "function_id": function_id,
    });
    if let Some(result) = result {
        response["result"] = result;
    }
    if let Some(error) = error {
        response["error"] = error;
    }
    send_json(socket, response).await
}

async fn send_json(socket: &mut WebSocketStream<TcpStream>, message: Value) -> EngineResult<()> {
    socket
        .send(Message::Text(message.to_string().into()))
        .await
        .map_err(|error| format!("protocol fake could not send response: {error}"))
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
        time::Duration,
    };

    #[cfg(unix)]
    use std::os::fd::AsRawFd;

    use futures_util::task::noop_waker;
    use tokio::{
        io::{AsyncRead, AsyncWrite, ReadBuf},
        net::TcpListener,
        sync::{Mutex, oneshot},
        time::{Instant, timeout},
    };
    use tokio_tungstenite::{
        WebSocketStream,
        tungstenite::{Message, protocol::Role},
    };

    use super::{
        CapturedInvocation, FakeEngine, FakeEngineConfig, SERVER_FRAME_DEADLINE, next_frame, serve,
    };

    struct IoErrorStream {
        kind: io::ErrorKind,
    }

    impl AsyncRead for IoErrorStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::new(
                self.get_mut().kind,
                "synthetic client shutdown",
            )))
        }
    }

    impl AsyncWrite for IoErrorStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            let _ = self;
            Poll::Ready(Ok(buffer.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            let _ = self;
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            let _ = self;
            Poll::Ready(Ok(()))
        }
    }

    async fn next_frame_with_io_error(kind: io::ErrorKind) -> Result<Option<Message>, String> {
        let mut socket =
            WebSocketStream::from_raw_socket(IoErrorStream { kind }, Role::Server, None).await;

        next_frame(&mut socket, Instant::now() + Duration::from_secs(1)).await
    }

    #[cfg(unix)]
    fn wait_for_listener_readiness(listener: &TcpListener) {
        #[repr(C)]
        struct PollFd {
            fd: i32,
            events: i16,
            revents: i16,
        }

        unsafe extern "C" {
            fn poll(fds: *mut PollFd, count: usize, timeout: i32) -> i32;
        }

        let mut poll_fd = PollFd {
            fd: listener.as_raw_fd(),
            events: 0x0001,
            revents: 0,
        };
        let result = unsafe { poll(&mut poll_fd, 1, -1) };
        assert_eq!(result, 1, "listener readiness poll must complete");
        assert_ne!(poll_fd.revents & 0x0001, 0, "listener must be readable");
    }

    async fn serve_with_queued_tcp_connection() -> (
        std::thread::JoinHandle<Result<(), String>>,
        Arc<AtomicUsize>,
        std::net::TcpStream,
    ) {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind protocol fake listener");
        let address = listener
            .local_addr()
            .expect("read protocol fake listener address");
        let client = std::net::TcpStream::connect(address)
            .expect("establish a TCP connection before starting the listener task");
        #[cfg(unix)]
        wait_for_listener_readiness(&listener);
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(listener.poll_accept(&mut context), Poll::Pending));
        let captured = Arc::new(Mutex::new(CapturedInvocation::default()));
        let connection_count = Arc::new(AtomicUsize::new(0));
        let task_connection_count = Arc::clone(&connection_count);
        let (cancellation, cancellation_receiver) = oneshot::channel();
        cancellation
            .send(())
            .expect("cancellation receiver remains available");
        let task = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("build time-only Tokio runtime")
                .block_on(serve(
                    listener,
                    captured,
                    task_connection_count,
                    FakeEngineConfig::default(),
                    cancellation_receiver,
                    SERVER_FRAME_DEADLINE,
                ))
        });

        (task, connection_count, client)
    }

    #[tokio::test]
    async fn treats_client_connection_reset_as_clean_shutdown() {
        let outcome = next_frame_with_io_error(io::ErrorKind::ConnectionReset).await;

        assert!(
            matches!(outcome, Ok(None)),
            "client connection reset must be a clean shutdown, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn preserves_unexpected_io_errors() {
        let error = next_frame_with_io_error(io::ErrorKind::BrokenPipe)
            .await
            .expect_err("unexpected I/O errors must fail the fake");

        assert!(
            error.starts_with("protocol fake WebSocket receive failed:"),
            "unexpected I/O errors must retain their failure path, got {error}"
        );
    }

    #[tokio::test]
    async fn finishes_without_waiting_for_a_connection() {
        let mut engine = FakeEngine::start(FakeEngineConfig::default()).await;

        timeout(
            Duration::from_millis(50),
            engine.finish_without_connection(),
        )
        .await
        .expect("no-connection finalization must not wait for a grace period")
        .expect("no-connection finalization succeeds");
        assert_eq!(engine.connection_count(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancellation_rejects_a_kernel_queued_connection_without_an_io_reactor_tick() {
        for _ in 0..32 {
            let (task, connection_count, _client) = serve_with_queued_tcp_connection().await;
            task.join()
                .expect("protocol fake thread must complete")
                .expect("protocol fake cancellation succeeds");

            assert_eq!(
                connection_count.load(Ordering::SeqCst),
                1,
                "a kernel-queued connection must be observed before cancellation completes"
            );
        }
    }
}
