use std::{
    fmt,
    future::Future,
    sync::{
        Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard,
        atomic::{AtomicBool, Ordering},
    },
};

use rust_mcp_schema::{JsonrpcNotification, JsonrpcRequest};
use serde_json::Value;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{Mutex, Semaphore, mpsc, watch},
    task::{JoinError, JoinSet},
};

use crate::{
    config::Config, embedding::QueryEmbeddingGenerator, operations::MemoryOperations,
    server::McpServer, service::CreateContext,
};

const READ_BUFFER_BYTES: usize = 8_192;

pub struct DispatchItem {
    request: JsonrpcRequest,
}

impl DispatchItem {
    pub fn request(&self) -> &JsonrpcRequest {
        &self.request
    }

    pub fn into_request(self) -> JsonrpcRequest {
        self.request
    }
}

impl fmt::Debug for DispatchItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DispatchItem")
    }
}

pub struct NotificationItem {
    notification: JsonrpcNotification,
}

impl NotificationItem {
    pub fn notification(&self) -> &JsonrpcNotification {
        &self.notification
    }
}

impl fmt::Debug for NotificationItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NotificationItem")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionRejection {
    Malformed,
    Oversize,
}

impl fmt::Display for AdmissionRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Malformed => "malformed_input",
            Self::Oversize => "oversize_input",
        })
    }
}

pub enum AdmissionOutcome {
    Queued,
    Notification(NotificationItem),
    Rejected(AdmissionRejection),
    Eof,
}

impl fmt::Debug for AdmissionOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Queued => "AdmissionOutcome::Queued",
            Self::Notification(_) => "AdmissionOutcome::Notification",
            Self::Rejected(AdmissionRejection::Malformed) => {
                "AdmissionOutcome::Rejected(Malformed)"
            }
            Self::Rejected(AdmissionRejection::Oversize) => "AdmissionOutcome::Rejected(Oversize)",
            Self::Eof => "AdmissionOutcome::Eof",
        })
    }
}

impl fmt::Display for AdmissionOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Queued => "queued",
            Self::Notification(_) => "notification",
            Self::Rejected(rejection) => return rejection.fmt(formatter),
            Self::Eof => "eof",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeError {
    Read,
    QueueClosed,
    InvalidMaxInFlight,
    Handler,
    Output,
    ResponseFrame,
    ResponseSerialization,
    Task,
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Read => "runtime_input_read_failed",
            Self::QueueClosed => "runtime_dispatch_queue_closed",
            Self::InvalidMaxInFlight => "runtime_max_in_flight_invalid",
            Self::Handler => "runtime_handler_failed",
            Self::Output => "runtime_output_write_failed",
            Self::ResponseFrame => "runtime_response_frame_invalid",
            Self::ResponseSerialization => "runtime_response_serialization_failed",
            Self::Task => "runtime_task_failed",
        })
    }
}

impl std::error::Error for RuntimeError {}

pub struct DispatchQueue {
    receiver: mpsc::Receiver<DispatchItem>,
    capacity: usize,
}

impl DispatchQueue {
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    pub async fn recv(&mut self) -> Option<DispatchItem> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Result<DispatchItem, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

pub enum DispatchOutcome {
    Response(Vec<u8>),
    NoResponse,
}

impl fmt::Debug for DispatchOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Response(_) => "DispatchOutcome::Response",
            Self::NoResponse => "DispatchOutcome::NoResponse",
        })
    }
}

#[derive(Clone)]
pub struct RuntimeShutdown {
    state: Arc<ShutdownState>,
}

struct ShutdownState {
    requested: AtomicBool,
    changed: watch::Sender<()>,
    start_gate: StdMutex<()>,
}

impl Default for RuntimeShutdown {
    fn default() -> Self {
        let (changed, _) = watch::channel(());
        Self {
            state: Arc::new(ShutdownState {
                requested: AtomicBool::new(false),
                changed,
                start_gate: StdMutex::new(()),
            }),
        }
    }
}

impl fmt::Debug for RuntimeShutdown {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RuntimeShutdown")
    }
}

impl RuntimeShutdown {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn request(&self) {
        let _start_gate = self
            .state
            .start_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !self.state.requested.swap(true, Ordering::SeqCst) {
            self.state.changed.send_replace(());
        }
    }

    fn is_requested(&self) -> bool {
        self.state.requested.load(Ordering::SeqCst)
    }

    fn admit_handler(&self) -> Option<StdMutexGuard<'_, ()>> {
        let start_gate = self
            .state
            .start_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.is_requested() {
            None
        } else {
            Some(start_gate)
        }
    }

    async fn cancelled(&self) {
        if self.is_requested() {
            return;
        }
        let mut changed = self.state.changed.subscribe();
        if !self.is_requested() {
            let _ = changed.changed().await;
        }
    }
}

pub struct RuntimeExecutor {
    queue: DispatchQueue,
    max_in_flight: usize,
    shutdown: RuntimeShutdown,
}

impl RuntimeExecutor {
    pub fn new(queue: DispatchQueue, max_in_flight: usize, shutdown: RuntimeShutdown) -> Self {
        Self {
            queue,
            max_in_flight,
            shutdown,
        }
    }

    pub async fn run<H, HandlerFuture, W>(self, handler: H, writer: W) -> Result<(), RuntimeError>
    where
        H: Fn(DispatchItem) -> HandlerFuture + Send + Sync + 'static,
        HandlerFuture: Future<Output = Result<DispatchOutcome, RuntimeError>> + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let Self {
            mut queue,
            max_in_flight,
            shutdown,
        } = self;
        if max_in_flight == 0 {
            return Err(RuntimeError::InvalidMaxInFlight);
        }

        let permits = Arc::new(Semaphore::new(max_in_flight));
        let handler = Arc::new(handler);
        let writer = Arc::new(Mutex::new(writer));
        let mut tasks = JoinSet::new();
        let mut failure = None;
        let mut accepting = true;

        while accepting {
            while let Some(completed) = tasks.try_join_next() {
                capture_task_result(&mut failure, completed);
            }
            if failure.is_some() {
                break;
            }

            let permit = Arc::clone(&permits).acquire_owned();
            tokio::pin!(permit);
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    accepting = false;
                }
                completed = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(completed) = completed {
                        capture_task_result(&mut failure, completed);
                    }
                }
                permit = &mut permit => {
                    let permit = permit.map_err(|_| RuntimeError::Task)?;
                    if shutdown.is_requested() {
                        drop(permit);
                        accepting = false;
                        continue;
                    }

                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => {
                            drop(permit);
                            accepting = false;
                        }
                        completed = tasks.join_next(), if !tasks.is_empty() => {
                            drop(permit);
                            if let Some(completed) = completed {
                                capture_task_result(&mut failure, completed);
                            }
                        }
                        item = queue.recv() => match item {
                            Some(item) => {
                                match shutdown.admit_handler() {
                                    Some(_admission) => {
                                        let handler = Arc::clone(&handler);
                                        let writer = Arc::clone(&writer);
                                        tasks.spawn(async move {
                                            let result = async {
                                                match (handler.as_ref())(item).await? {
                                                    DispatchOutcome::Response(response) => {
                                                        write_response(&writer, &response).await
                                                    }
                                                    DispatchOutcome::NoResponse => Ok(()),
                                                }
                                            }
                                            .await;
                                            drop(permit);
                                            result
                                        });
                                    }
                                    None => {
                                        drop(permit);
                                        accepting = false;
                                    }
                                }
                            }
                            None => {
                                drop(permit);
                                accepting = false;
                            }
                        },
                    }
                }
            }
        }

        while let Some(completed) = tasks.join_next().await {
            capture_task_result(&mut failure, completed);
        }

        let flush_result = {
            let mut writer = writer.lock().await;
            writer.flush().await.map_err(|_| RuntimeError::Output)
        };
        failure.map_or(flush_result, Err)
    }
}

pub async fn run_mcp_server<R, W, O, C, G>(
    mut input: RuntimeInput<R>,
    queue: DispatchQueue,
    max_in_flight: usize,
    shutdown: RuntimeShutdown,
    server: McpServer<O, C, G>,
    writer: W,
) -> Result<(), RuntimeError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
    O: MemoryOperations + 'static,
    C: CreateContext + 'static,
    G: QueryEmbeddingGenerator + 'static,
{
    let input_shutdown = shutdown.clone();
    let server = Arc::new(server);
    let executor = RuntimeExecutor::new(queue, max_in_flight, shutdown).run(
        move |item| {
            let server = Arc::clone(&server);
            async move {
                let response = server.dispatch(item.into_request()).await;
                let response = response
                    .into_wire_bytes()
                    .map_err(|_| RuntimeError::ResponseSerialization)?;
                Ok(DispatchOutcome::Response(response))
            }
        },
        writer,
    );
    tokio::pin!(executor);

    loop {
        tokio::select! {
            biased;
            _ = input_shutdown.cancelled() => {
                drop(input);
                return executor.await;
            }
            result = &mut executor => return result,
            admission = input.admit_next() => match admission {
                Ok(AdmissionOutcome::Eof) => {
                    drop(input);
                    return executor.await;
                }
                Ok(
                    AdmissionOutcome::Queued
                    | AdmissionOutcome::Notification(_)
                    | AdmissionOutcome::Rejected(_),
                ) => {}
                Err(error) => {
                    drop(input);
                    executor.await?;
                    return Err(error);
                }
            },
        }
    }
}

fn capture_task_result(
    failure: &mut Option<RuntimeError>,
    completed: Result<Result<(), RuntimeError>, JoinError>,
) {
    let result = completed
        .map_err(|_| RuntimeError::Task)
        .and_then(|result| result);
    if let Err(error) = result {
        failure.get_or_insert(error);
    }
}

async fn write_response<W>(writer: &Mutex<W>, response: &[u8]) -> Result<(), RuntimeError>
where
    W: AsyncWrite + Unpin,
{
    if response.iter().any(|byte| matches!(byte, b'\n' | b'\r')) {
        return Err(RuntimeError::ResponseFrame);
    }

    let mut writer = writer.lock().await;
    writer
        .write_all(response)
        .await
        .map_err(|_| RuntimeError::Output)?;
    writer
        .write_all(b"\n")
        .await
        .map_err(|_| RuntimeError::Output)?;
    writer.flush().await.map_err(|_| RuntimeError::Output)
}

pub struct RuntimeInput<R> {
    reader: R,
    max_line_bytes: usize,
    sender: mpsc::Sender<DispatchItem>,
    read_buffer: [u8; READ_BUFFER_BYTES],
    read_start: usize,
    read_end: usize,
    frame: Vec<u8>,
    discarding_oversize: bool,
}

impl<R> RuntimeInput<R>
where
    R: AsyncRead + Unpin,
{
    pub fn new(reader: R, config: &Config) -> (Self, DispatchQueue) {
        let (sender, receiver) = mpsc::channel(config.channel_capacity);
        (
            Self {
                reader,
                max_line_bytes: config.max_line_bytes,
                sender,
                read_buffer: [0; READ_BUFFER_BYTES],
                read_start: 0,
                read_end: 0,
                frame: Vec::with_capacity(config.max_line_bytes.min(READ_BUFFER_BYTES)),
                discarding_oversize: false,
            },
            DispatchQueue {
                receiver,
                capacity: config.channel_capacity,
            },
        )
    }

    pub async fn admit_next(&mut self) -> Result<AdmissionOutcome, RuntimeError> {
        match self.next_frame().await? {
            Frame::Eof => Ok(AdmissionOutcome::Eof),
            Frame::Oversize => Ok(AdmissionOutcome::Rejected(AdmissionRejection::Oversize)),
            Frame::Bounded(frame) => match parse_frame(&frame) {
                ParsedFrame::Dispatch(item) => {
                    self.sender
                        .send(item)
                        .await
                        .map_err(|_| RuntimeError::QueueClosed)?;
                    Ok(AdmissionOutcome::Queued)
                }
                ParsedFrame::Notification(item) => Ok(AdmissionOutcome::Notification(item)),
                ParsedFrame::Malformed => {
                    Ok(AdmissionOutcome::Rejected(AdmissionRejection::Malformed))
                }
            },
        }
    }

    async fn next_frame(&mut self) -> Result<Frame, RuntimeError> {
        loop {
            while self.read_start < self.read_end {
                let byte = self.read_buffer[self.read_start];
                self.read_start += 1;

                if self.discarding_oversize {
                    if byte == b'\n' {
                        self.discarding_oversize = false;
                        return Ok(Frame::Oversize);
                    }
                    continue;
                }

                if byte == b'\n' {
                    return Ok(Frame::Bounded(std::mem::take(&mut self.frame)));
                }

                if self.frame.len() == self.max_line_bytes {
                    self.frame.clear();
                    self.discarding_oversize = true;
                } else {
                    self.frame.push(byte);
                }
            }

            self.read_start = 0;
            self.read_end = self
                .reader
                .read(&mut self.read_buffer)
                .await
                .map_err(|_| RuntimeError::Read)?;
            if self.read_end == 0 {
                if self.discarding_oversize {
                    self.discarding_oversize = false;
                    return Ok(Frame::Oversize);
                }
                if self.frame.is_empty() {
                    return Ok(Frame::Eof);
                }
                return Ok(Frame::Bounded(std::mem::take(&mut self.frame)));
            }
        }
    }
}

enum Frame {
    Bounded(Vec<u8>),
    Oversize,
    Eof,
}

enum ParsedFrame {
    Dispatch(DispatchItem),
    Notification(NotificationItem),
    Malformed,
}

fn parse_frame(frame: &[u8]) -> ParsedFrame {
    let Ok(value) = serde_json::from_slice::<Value>(frame) else {
        return ParsedFrame::Malformed;
    };
    let Some(envelope) = value.as_object() else {
        return ParsedFrame::Malformed;
    };

    if envelope.contains_key("id") {
        return serde_json::from_value::<JsonrpcRequest>(value)
            .map(|request| ParsedFrame::Dispatch(DispatchItem { request }))
            .unwrap_or(ParsedFrame::Malformed);
    }

    serde_json::from_value::<JsonrpcNotification>(value)
        .map(|notification| ParsedFrame::Notification(NotificationItem { notification }))
        .unwrap_or(ParsedFrame::Malformed)
}
