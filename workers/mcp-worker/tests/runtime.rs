use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

use mcp_worker::{
    config::{
        Config, TOTAL_RECALL_MCP_CHANNEL_CAPACITY_ENV, TOTAL_RECALL_MCP_MAX_IN_FLIGHT_ENV,
        TOTAL_RECALL_MCP_MAX_LINE_BYTES_ENV, TOTAL_RECALL_MEMORY_DATABASE_ENV,
    },
    runtime::{
        AdmissionOutcome, AdmissionRejection, DispatchOutcome, RuntimeError, RuntimeExecutor,
        RuntimeInput, RuntimeShutdown,
    },
};
use serde_json::json;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::{mpsc::error::TryRecvError, watch},
};

const PRIVATE_SENTINEL: &str = "runtime-private-frame-sentinel";

#[tokio::test]
async fn valid_bounded_request_is_queued_and_eof_is_normal() {
    let frame = request_frame("valid-request", json!({ "secret": PRIVATE_SENTINEL }));
    let config = config(frame.len(), 1);
    let (mut input, mut queue) = RuntimeInput::new(reader(frame_with_newline(&frame)), &config);

    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Queued
    ));
    let item = queue.recv().await.expect("request should be queued");
    assert_eq!(item.request().method, "server/discover");
    assert!(!format!("{item:?}").contains(PRIVATE_SENTINEL));
    assert_eq!(
        serde_json::to_value(item.request()).unwrap()["id"],
        "valid-request"
    );
    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Eof
    ));
}

#[tokio::test]
async fn malformed_frame_recovers_to_a_following_valid_request() {
    let valid = request_frame("recovered-request", json!({}));
    let mut frames = format!("{{\"{PRIVATE_SENTINEL}\":\n").into_bytes();
    frames.extend_from_slice(
        &serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": "malformed-envelope",
            "method": 7,
        }))
        .unwrap(),
    );
    frames.push(b'\n');
    frames.extend_from_slice(&valid);
    frames.push(b'\n');
    let config = config(1_024, 1);
    let (mut input, mut queue) = RuntimeInput::new(reader(frames), &config);

    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Rejected(AdmissionRejection::Malformed)
    ));
    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Rejected(AdmissionRejection::Malformed)
    ));
    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Queued
    ));
    let item = queue
        .recv()
        .await
        .expect("recovered request should be queued");
    assert_eq!(
        serde_json::to_value(item.request()).unwrap()["id"],
        "recovered-request"
    );
}

#[tokio::test]
async fn oversize_frame_is_discarded_through_newline_and_recovers() {
    let valid = request_frame("after-oversize", json!({}));
    let max_line_bytes = valid.len();
    let mut frames = vec![b'x'; max_line_bytes + 1];
    frames.push(b'\n');
    frames.extend_from_slice(&valid);
    frames.push(b'\n');
    let config = config(max_line_bytes, 1);
    let (mut input, mut queue) = RuntimeInput::new(reader(frames), &config);

    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Rejected(AdmissionRejection::Oversize)
    ));
    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Queued
    ));
    let item = queue
        .recv()
        .await
        .expect("post-oversize request should be queued");
    assert_eq!(
        serde_json::to_value(item.request()).unwrap()["id"],
        "after-oversize"
    );
}

#[tokio::test]
async fn configured_line_bound_accepts_the_maximum_and_rejects_maximum_plus_one() {
    let valid = request_frame("at-boundary", json!({}));
    let config = config(valid.len(), 1);
    let (mut accepted_input, mut accepted_queue) =
        RuntimeInput::new(reader(frame_with_newline(&valid)), &config);

    assert!(matches!(
        accepted_input.admit_next().await.unwrap(),
        AdmissionOutcome::Queued
    ));
    assert!(accepted_queue.recv().await.is_some());

    let mut oversize = vec![b'x'; valid.len() + 1];
    oversize.push(b'\n');
    let (mut rejected_input, rejected_queue) = RuntimeInput::new(reader(oversize), &config);
    assert_eq!(rejected_queue.capacity(), 1);
    assert!(matches!(
        rejected_input.admit_next().await.unwrap(),
        AdmissionOutcome::Rejected(AdmissionRejection::Oversize)
    ));
}

#[tokio::test]
async fn eof_terminated_maximum_frame_is_queued_then_eof() {
    let frame = request_frame("eof-at-boundary", json!({}));
    let config = config(frame.len(), 1);
    let (mut input, mut queue) = RuntimeInput::new(reader(frame), &config);

    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Queued
    ));
    let item = queue
        .recv()
        .await
        .expect("eof-terminated request should queue");
    assert_eq!(
        serde_json::to_value(item.request()).unwrap()["id"],
        "eof-at-boundary"
    );
    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Eof
    ));
}

#[tokio::test]
async fn eof_terminated_maximum_plus_one_frame_is_rejected_without_parsing_or_leaking() {
    let oversize = request_frame("oversize-eof", json!({ "secret": PRIVATE_SENTINEL }));
    let config = config(oversize.len() - 1, 1);
    assert_eq!(oversize.len(), config.max_line_bytes + 1);
    let (mut input, mut queue) = RuntimeInput::new(reader(oversize), &config);

    let outcome = input.admit_next().await.unwrap();
    assert!(matches!(
        outcome,
        AdmissionOutcome::Rejected(AdmissionRejection::Oversize)
    ));
    assert!(matches!(queue.try_recv(), Err(TryRecvError::Empty)));
    assert!(!format!("{outcome:?}").contains(PRIVATE_SENTINEL));
    assert!(!outcome.to_string().contains(PRIVATE_SENTINEL));
    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Eof
    ));
}

#[tokio::test]
async fn notifications_are_classified_without_a_queued_response_item() {
    let frame = serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "method": "notifications/private",
        "params": { "secret": PRIVATE_SENTINEL },
    }))
    .unwrap();
    let config = config(frame.len(), 1);
    let (mut input, mut queue) = RuntimeInput::new(reader(frame_with_newline(&frame)), &config);

    let outcome = input.admit_next().await.unwrap();
    let AdmissionOutcome::Notification(notification) = outcome else {
        panic!("input without an id should be classified as a notification");
    };
    assert_eq!(notification.notification().method, "notifications/private");
    assert!(!format!("{notification:?}").contains(PRIVATE_SENTINEL));
    assert!(matches!(queue.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn queue_backpressure_waits_for_receiver_capacity() {
    let first = request_frame("first", json!({}));
    let second = request_frame("second", json!({}));
    let mut frames = frame_with_newline(&first);
    frames.extend_from_slice(&frame_with_newline(&second));
    let config = config(first.len().max(second.len()), 1);
    let (mut input, mut queue) = RuntimeInput::new(reader(frames), &config);

    assert_eq!(queue.capacity(), 1);
    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Queued
    ));

    let mut second_admission = std::pin::pin!(input.admit_next());
    let mut context = Context::from_waker(Waker::noop());
    // ReadyReader cannot yield, so Pending can only be the full queue send.
    assert!(matches!(
        Future::poll(second_admission.as_mut(), &mut context),
        Poll::Pending
    ));

    let item = queue.recv().await.expect("first request should be queued");
    assert_eq!(serde_json::to_value(item.request()).unwrap()["id"], "first");
    assert!(matches!(
        Future::poll(second_admission.as_mut(), &mut context),
        Poll::Ready(Ok(AdmissionOutcome::Queued))
    ));
    let item = queue.recv().await.expect("second request should be queued");
    assert_eq!(
        serde_json::to_value(item.request()).unwrap()["id"],
        "second"
    );
}

#[tokio::test]
async fn admission_diagnostics_and_debug_output_redact_raw_frames() {
    let malformed = format!("{{\"secret\":\"{PRIVATE_SENTINEL}\"").into_bytes();
    let malformed_config = config(1_024, 1);
    let (mut input, _) =
        RuntimeInput::new(reader(frame_with_newline(&malformed)), &malformed_config);

    let outcome = input.admit_next().await.unwrap();
    assert!(matches!(
        outcome,
        AdmissionOutcome::Rejected(AdmissionRejection::Malformed)
    ));
    let debug = format!("{outcome:?}");
    let display = outcome.to_string();
    assert!(
        !debug.contains(PRIVATE_SENTINEL),
        "debug leaked raw frame: {debug}"
    );
    assert!(
        !display.contains(PRIVATE_SENTINEL),
        "display leaked raw frame: {display}"
    );
    assert_eq!(display, "malformed_input");

    let oversize = format!("{PRIVATE_SENTINEL}x").into_bytes();
    let config = config(PRIVATE_SENTINEL.len(), 1);
    let (mut input, _) = RuntimeInput::new(reader(frame_with_newline(&oversize)), &config);
    let outcome = input.admit_next().await.unwrap();
    assert!(matches!(
        outcome,
        AdmissionOutcome::Rejected(AdmissionRejection::Oversize)
    ));
    assert!(!format!("{outcome:?}").contains(PRIVATE_SENTINEL));
    assert!(!outcome.to_string().contains(PRIVATE_SENTINEL));
}

#[tokio::test]
async fn executor_eof_drains_queued_responses_before_completion() {
    let config = config_with_max_in_flight(1_024, 2, 1);
    let (mut input, queue) =
        RuntimeInput::new(reader(request_frames(["first", "second"])), &config);
    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Queued
    ));
    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Queued
    ));
    drop(input);

    let writer = ControlledWriter::open();
    RuntimeExecutor::new(queue, config.max_in_flight, RuntimeShutdown::new())
        .run(
            |item| async move { Ok(DispatchOutcome::Response(request_id(item).into_bytes())) },
            writer.clone(),
        )
        .await
        .expect("EOF should drain queued responses");

    assert_eq!(writer.bytes(), b"first\nsecond\n");
}

#[tokio::test]
async fn executor_acquires_permit_before_dequeue_and_recovers_after_writer_unblocks() {
    let config = config_with_max_in_flight(1_024, 1, 1);
    let (mut input, queue) = RuntimeInput::new(
        reader(request_frames(["first", "second", "third"])),
        &config,
    );
    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Queued
    ));

    let writer = ControlledWriter::blocked();
    let call_count = Arc::new(AtomicUsize::new(0));
    let handler_calls = Arc::clone(&call_count);
    let runner = tokio::spawn(
        RuntimeExecutor::new(queue, config.max_in_flight, RuntimeShutdown::new()).run(
            move |item| {
                let handler_calls = Arc::clone(&handler_calls);
                async move {
                    handler_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(DispatchOutcome::Response(request_id(item).into_bytes()))
                }
            },
            writer.clone(),
        ),
    );

    writer.wait_until_flush_blocked().await;
    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Queued
    ));

    let third_admission = {
        let mut third_admission = std::pin::pin!(input.admit_next());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            Future::poll(third_admission.as_mut(), &mut context),
            Poll::Pending
        ));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        writer.release();
        third_admission.await.unwrap()
    };
    assert!(matches!(third_admission, AdmissionOutcome::Queued));

    drop(input);
    runner
        .await
        .expect("executor task should not panic")
        .expect("writer recovery should drain every response");
    assert_eq!(writer.bytes(), b"first\nsecond\nthird\n");
}

#[tokio::test]
async fn executor_caps_active_handlers_and_preserves_a_full_queue() {
    let config = config_with_max_in_flight(1_024, 1, 2);
    let (mut input, queue) = RuntimeInput::new(
        reader(request_frames(["first", "second", "third", "fourth"])),
        &config,
    );
    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Queued
    ));

    let tracker = Arc::new(HandlerTracker::default());
    let handler_tracker = Arc::clone(&tracker);
    let writer = ControlledWriter::open();
    let runner = tokio::spawn(
        RuntimeExecutor::new(queue, config.max_in_flight, RuntimeShutdown::new()).run(
            move |item| {
                let handler_tracker = Arc::clone(&handler_tracker);
                async move {
                    handler_tracker.enter().await;
                    Ok(DispatchOutcome::Response(request_id(item).into_bytes()))
                }
            },
            writer.clone(),
        ),
    );

    tracker.wait_for_active(1).await;
    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Queued
    ));
    tracker.wait_for_active(2).await;
    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Queued
    ));

    let fourth_admission = {
        let mut fourth_admission = std::pin::pin!(input.admit_next());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            Future::poll(fourth_admission.as_mut(), &mut context),
            Poll::Pending
        ));
        assert_eq!(tracker.max_active(), config.max_in_flight);

        tracker.release();
        fourth_admission.await.unwrap()
    };
    assert!(matches!(fourth_admission, AdmissionOutcome::Queued));

    drop(input);
    runner
        .await
        .expect("executor task should not panic")
        .expect("all handlers should complete");

    let mut frames = String::from_utf8(writer.bytes())
        .expect("responses should be UTF-8 test values")
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    frames.sort();
    assert_eq!(frames, ["first", "fourth", "second", "third"]);
}

#[tokio::test]
async fn executor_shutdown_drains_active_response_without_stdout_diagnostics() {
    let config = config_with_max_in_flight(1_024, 1, 1);
    let frame = request_frame("shutdown", json!({ "secret": PRIVATE_SENTINEL }));
    let (mut input, queue) = RuntimeInput::new(reader(frame_with_newline(&frame)), &config);
    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Queued
    ));

    let shutdown = RuntimeShutdown::new();
    let writer = ControlledWriter::blocked();
    let runner = tokio::spawn(
        RuntimeExecutor::new(queue, config.max_in_flight, shutdown.clone()).run(
            |_| async { Ok(DispatchOutcome::Response(b"active-response".to_vec())) },
            writer.clone(),
        ),
    );

    writer.wait_until_flush_blocked().await;
    shutdown.request();
    assert!(!runner.is_finished());

    writer.release();
    runner
        .await
        .expect("executor task should not panic")
        .expect("shutdown should drain the active response");
    assert_eq!(writer.bytes(), b"active-response\n");
    assert!(
        !String::from_utf8(writer.bytes())
            .unwrap()
            .contains(PRIVATE_SENTINEL)
    );

    drop(input);
}

#[tokio::test]
async fn executor_shutdown_does_not_start_queued_handler_after_active_response() {
    let config = config_with_max_in_flight(1_024, 2, 1);
    let (mut input, queue) =
        RuntimeInput::new(reader(request_frames(["first", "second"])), &config);
    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Queued
    ));
    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Queued
    ));
    drop(input);

    let shutdown = RuntimeShutdown::new();
    let writer = ControlledWriter::blocked();
    let call_count = Arc::new(AtomicUsize::new(0));
    let handler_calls = Arc::clone(&call_count);
    let runner = tokio::spawn(
        RuntimeExecutor::new(queue, config.max_in_flight, shutdown.clone()).run(
            move |item| {
                let handler_calls = Arc::clone(&handler_calls);
                async move {
                    handler_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(DispatchOutcome::Response(request_id(item).into_bytes()))
                }
            },
            writer.clone(),
        ),
    );

    writer.wait_until_flush_blocked().await;
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    shutdown.request();
    assert!(!runner.is_finished());
    writer.release();
    runner
        .await
        .expect("executor task should not panic")
        .expect("shutdown should not start queued work");

    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    assert_eq!(writer.bytes(), b"first\n");
}

#[tokio::test]
async fn executor_errors_and_debug_output_redact_request_and_response_payloads() {
    let config = config_with_max_in_flight(1_024, 1, 1);
    let frame = request_frame("failing", json!({ "secret": PRIVATE_SENTINEL }));
    let (mut input, queue) = RuntimeInput::new(reader(frame_with_newline(&frame)), &config);
    assert!(matches!(
        input.admit_next().await.unwrap(),
        AdmissionOutcome::Queued
    ));
    drop(input);

    let writer = ControlledWriter::open();
    let error = RuntimeExecutor::new(queue, config.max_in_flight, RuntimeShutdown::new())
        .run(
            |item| async move {
                assert!(!format!("{item:?}").contains(PRIVATE_SENTINEL));
                Err(RuntimeError::Handler)
            },
            writer.clone(),
        )
        .await
        .expect_err("handler failure should be returned without a stdout diagnostic");

    assert_eq!(error, RuntimeError::Handler);
    assert!(!format!("{error:?}").contains(PRIVATE_SENTINEL));
    assert!(!error.to_string().contains(PRIVATE_SENTINEL));
    assert!(
        !format!(
            "{:?}",
            DispatchOutcome::Response(PRIVATE_SENTINEL.as_bytes().to_vec())
        )
        .contains(PRIVATE_SENTINEL)
    );
    assert!(writer.bytes().is_empty());
}

fn config(max_line_bytes: usize, channel_capacity: usize) -> Config {
    config_with_max_in_flight(max_line_bytes, channel_capacity, 16)
}

fn config_with_max_in_flight(
    max_line_bytes: usize,
    channel_capacity: usize,
    max_in_flight: usize,
) -> Config {
    Config::from_values([
        (
            TOTAL_RECALL_MEMORY_DATABASE_ENV.to_owned(),
            "total-recall-memory".to_owned(),
        ),
        (
            TOTAL_RECALL_MCP_MAX_LINE_BYTES_ENV.to_owned(),
            max_line_bytes.to_string(),
        ),
        (
            TOTAL_RECALL_MCP_CHANNEL_CAPACITY_ENV.to_owned(),
            channel_capacity.to_string(),
        ),
        (
            TOTAL_RECALL_MCP_MAX_IN_FLIGHT_ENV.to_owned(),
            max_in_flight.to_string(),
        ),
    ])
    .unwrap()
}

fn request_frame(id: &str, params: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "server/discover",
        "params": params,
    }))
    .unwrap()
}

fn frame_with_newline(frame: &[u8]) -> Vec<u8> {
    let mut framed = frame.to_vec();
    framed.push(b'\n');
    framed
}

fn request_frames<const N: usize>(ids: [&str; N]) -> Vec<u8> {
    let mut frames = Vec::new();
    for id in ids {
        frames.extend_from_slice(&frame_with_newline(&request_frame(id, json!({}))));
    }
    frames
}

fn request_id(item: mcp_worker::runtime::DispatchItem) -> String {
    serde_json::to_value(item.into_request())
        .expect("request should serialize for test response")
        ["id"]
        .as_str()
        .expect("test request ID should be a string")
        .to_owned()
}

struct Signal {
    set: AtomicBool,
    changed: watch::Sender<()>,
}

impl Default for Signal {
    fn default() -> Self {
        let (changed, _) = watch::channel(());
        Self {
            set: AtomicBool::new(false),
            changed,
        }
    }
}

impl Signal {
    fn trigger(&self) {
        if !self.set.swap(true, Ordering::SeqCst) {
            self.changed.send_replace(());
        }
    }

    async fn wait(&self) {
        if self.set.load(Ordering::SeqCst) {
            return;
        }
        let mut changed = self.changed.subscribe();
        if !self.set.load(Ordering::SeqCst) {
            let _ = changed.changed().await;
        }
    }
}

struct HandlerTracker {
    active: AtomicUsize,
    max_active: AtomicUsize,
    changed: watch::Sender<()>,
    released: Signal,
}

impl Default for HandlerTracker {
    fn default() -> Self {
        let (changed, _) = watch::channel(());
        Self {
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            changed,
            released: Signal::default(),
        }
    }
}

impl HandlerTracker {
    async fn enter(&self) {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        self.changed.send_replace(());
        self.released.wait().await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        self.changed.send_replace(());
    }

    async fn wait_for_active(&self, expected: usize) {
        let mut changed = self.changed.subscribe();
        loop {
            if self.active.load(Ordering::SeqCst) >= expected {
                return;
            }
            let _ = changed.changed().await;
        }
    }

    fn max_active(&self) -> usize {
        self.max_active.load(Ordering::SeqCst)
    }

    fn release(&self) {
        self.released.trigger();
    }
}

#[derive(Clone)]
struct ControlledWriter {
    state: Arc<Mutex<WriterState>>,
    flush_blocked: Arc<Signal>,
}

struct WriterState {
    bytes: Vec<u8>,
    block_flush: bool,
    waker: Option<Waker>,
}

impl ControlledWriter {
    fn open() -> Self {
        Self::new(false)
    }

    fn blocked() -> Self {
        Self::new(true)
    }

    fn new(block_flush: bool) -> Self {
        Self {
            state: Arc::new(Mutex::new(WriterState {
                bytes: Vec::new(),
                block_flush,
                waker: None,
            })),
            flush_blocked: Arc::new(Signal::default()),
        }
    }

    fn bytes(&self) -> Vec<u8> {
        self.state
            .lock()
            .expect("controlled writer state should not be poisoned")
            .bytes
            .clone()
    }

    fn release(&self) {
        let waker = {
            let mut state = self
                .state
                .lock()
                .expect("controlled writer state should not be poisoned");
            state.block_flush = false;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    async fn wait_until_flush_blocked(&self) {
        self.flush_blocked.wait().await;
    }
}

impl AsyncWrite for ControlledWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let writer = self.get_mut();
        writer
            .state
            .lock()
            .expect("controlled writer state should not be poisoned")
            .bytes
            .extend_from_slice(buffer);
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let writer = self.get_mut();
        let blocked = {
            let mut state = writer
                .state
                .lock()
                .expect("controlled writer state should not be poisoned");
            if state.block_flush {
                state.waker = Some(context.waker().clone());
                true
            } else {
                false
            }
        };
        if blocked {
            writer.flush_blocked.trigger();
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(context)
    }
}

struct ReadyReader {
    bytes: Vec<u8>,
    position: usize,
}

impl AsyncRead for ReadyReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let reader = self.as_mut().get_mut();
        let remaining = &reader.bytes[reader.position..];
        let bytes_to_copy = remaining.len().min(buffer.remaining());
        buffer.put_slice(&remaining[..bytes_to_copy]);
        reader.position += bytes_to_copy;
        Poll::Ready(Ok(()))
    }
}

fn reader(bytes: Vec<u8>) -> ReadyReader {
    ReadyReader { bytes, position: 0 }
}
