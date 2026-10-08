use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use async_trait::async_trait;
use harness_event_persistence::{
    contracts::{PersistableEvent, QueuedHarnessEventInput},
    persistence::{AppendReceipt, EventStore, PersistenceResponse, StoreError, persist_event},
    runtime::handle_persist_event,
};
use serde_json::{Value, json};

const TIMESTAMP: &str = "2026-09-18T12:34:56.789Z";

#[derive(Clone, Default)]
struct RecordingStore {
    events: Arc<Mutex<Vec<PersistableEvent>>>,
}

impl RecordingStore {
    fn events(&self) -> Vec<PersistableEvent> {
        self.events.lock().unwrap().clone()
    }
}

#[async_trait]
impl EventStore for RecordingStore {
    async fn append(&self, event: &PersistableEvent) -> Result<AppendReceipt, StoreError> {
        self.events.lock().unwrap().push(event.clone());
        Ok(AppendReceipt { affected_rows: 1 })
    }
}

struct FailingStore {
    attempts: Arc<AtomicUsize>,
}

#[async_trait]
impl EventStore for FailingStore {
    async fn append(&self, _event: &PersistableEvent) -> Result<AppendReceipt, StoreError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(StoreError)
    }
}

struct ReceiptStore {
    affected_rows: u64,
    attempts: AtomicUsize,
}

struct ConfirmingStore {
    append_started: AtomicBool,
    committed: AtomicBool,
}

#[async_trait]
impl EventStore for ConfirmingStore {
    async fn append(&self, _event: &PersistableEvent) -> Result<AppendReceipt, StoreError> {
        self.append_started.store(true, Ordering::SeqCst);
        while !self.committed.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        Ok(AppendReceipt { affected_rows: 1 })
    }
}

#[async_trait]
impl EventStore for ReceiptStore {
    async fn append(&self, _event: &PersistableEvent) -> Result<AppendReceipt, StoreError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Ok(AppendReceipt {
            affected_rows: self.affected_rows,
        })
    }
}

fn queued(value: Value) -> QueuedHarnessEventInput {
    serde_json::from_value(value).expect("fixture should be a valid queued event")
}

fn session_start() -> QueuedHarnessEventInput {
    queued(json!({
        "event_type": "session_start",
        "session_id": "session-1",
        "project_name": "project",
        "timestamp": TIMESTAMP,
        "current_working_directory": "/work",
    }))
}

fn observation(data: Value) -> QueuedHarnessEventInput {
    queued(json!({
        "event_type": "observation",
        "hook_type": "post_tool_use",
        "project_name": "project",
        "current_working_directory": "/work",
        "timestamp": TIMESTAMP,
        "session_id": "session-1",
        "data": data,
    }))
}

fn session_end() -> QueuedHarnessEventInput {
    queued(json!({
        "event_type": "session_end",
        "session_id": "session-1",
        "project_name": "project",
        "timestamp": TIMESTAMP,
        "current_working_directory": "/work",
    }))
}

#[test]
fn persistence_response_serializes_as_a_success_acknowledgment() {
    assert_eq!(
        serde_json::to_value(PersistenceResponse { persisted: true }).unwrap(),
        json!({"persisted": true})
    );
}

#[tokio::test]
async fn every_event_variant_maps_to_one_append() {
    let store = RecordingStore::default();

    for input in [
        session_start(),
        observation(json!({"key": "value"})),
        session_end(),
    ] {
        assert_eq!(
            persist_event(input, &store)
                .await
                .expect("persistence service should succeed"),
            PersistenceResponse { persisted: true }
        );
    }

    let events = store.events();
    assert_eq!(events.len(), 3);
    assert!(matches!(&events[0], PersistableEvent::SessionStart { .. }));
    assert!(matches!(&events[1], PersistableEvent::Observation { .. }));
    assert!(matches!(&events[2], PersistableEvent::SessionEnd { .. }));
}

#[tokio::test]
async fn repeated_deliveries_are_appended_independently() {
    let store = RecordingStore::default();
    let input = observation(json!({"duplicate": true}));

    persist_event(input.clone(), &store)
        .await
        .expect("first persistence service call should succeed");
    persist_event(input, &store)
        .await
        .expect("second persistence service call should succeed");

    let events = store.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0], events[1]);
}

#[tokio::test]
async fn session_end_before_start_is_appended_without_ordering_checks() {
    let store = RecordingStore::default();

    persist_event(session_end(), &store)
        .await
        .expect("session end should be accepted before session start");
    persist_event(session_start(), &store)
        .await
        .expect("session start should be accepted after session end");

    let events = store.events();
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], PersistableEvent::SessionEnd { .. }));
    assert!(matches!(&events[1], PersistableEvent::SessionStart { .. }));
}

#[tokio::test]
async fn store_failures_are_returned_after_one_attempt_without_observation_data() {
    const OPAQUE_DATA: &str = "opaque-observation-data";

    let attempts = Arc::new(AtomicUsize::new(0));
    let error = persist_event(
        observation(json!({"secret": OPAQUE_DATA})),
        &FailingStore {
            attempts: Arc::clone(&attempts),
        },
    )
    .await
    .expect_err("store failure should reach the caller");

    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(error.to_string(), "observation event persistence failed");
    assert!(!error.to_string().contains(OPAQUE_DATA));
}

#[tokio::test]
async fn receipts_other_than_one_affected_row_are_rejected() {
    for affected_rows in [0, 2, u64::MAX] {
        let store = ReceiptStore {
            affected_rows,
            attempts: AtomicUsize::new(0),
        };

        let error = persist_event(session_start(), &store)
            .await
            .expect_err("an unconfirmed write should be rejected");

        assert_eq!(store.attempts.load(Ordering::SeqCst), 1);
        assert_eq!(
            error.to_string(),
            "session_start event persistence was not confirmed"
        );
    }
}

#[tokio::test]
async fn handler_acknowledges_only_after_the_store_confirms_one_row() {
    let store = Arc::new(ConfirmingStore {
        append_started: AtomicBool::new(false),
        committed: AtomicBool::new(false),
    });
    let store_for_handler = Arc::clone(&store);
    let task = tokio::spawn(async move {
        handle_persist_event(session_start(), store_for_handler.as_ref()).await
    });

    for _ in 0..100 {
        if store.append_started.load(Ordering::SeqCst) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        store.append_started.load(Ordering::SeqCst),
        "the handler should invoke the store"
    );
    assert!(
        !task.is_finished(),
        "the handler must not acknowledge before a confirmed write"
    );

    store.committed.store(true, Ordering::SeqCst);
    assert_eq!(
        task.await
            .expect("handler task should finish")
            .expect("confirmed write should acknowledge the delivery"),
        PersistenceResponse { persisted: true }
    );
}

#[tokio::test]
async fn handler_returns_opaque_errors_without_retries_or_observation_data() {
    const OPAQUE_DATA: &str = "observation-secret-sentinel";
    let attempts = Arc::new(AtomicUsize::new(0));
    let error = handle_persist_event(
        observation(json!({"secret": OPAQUE_DATA})),
        &FailingStore {
            attempts: Arc::clone(&attempts),
        },
    )
    .await
    .expect_err("a failed write must reject the delivery");

    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        error.to_string(),
        "handler error: observation event persistence failed"
    );
    assert!(!error.to_string().contains(OPAQUE_DATA));
}

#[tokio::test]
async fn handler_rejects_an_unconfirmed_store_result() {
    let store = ReceiptStore {
        affected_rows: 0,
        attempts: AtomicUsize::new(0),
    };

    let error = handle_persist_event(session_start(), &store)
        .await
        .expect_err("an unconfirmed write must reject the delivery");

    assert_eq!(store.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        error.to_string(),
        "handler error: session_start event persistence was not confirmed"
    );
}

#[tokio::test]
async fn handler_rejects_invalid_typed_input_before_writing_or_exposing_data() {
    const OPAQUE_DATA: &str = "observation-secret-sentinel";
    let attempts = Arc::new(AtomicUsize::new(0));
    let error = handle_persist_event(
        QueuedHarnessEventInput::Observation {
            hook_type: "post_tool_use".to_owned(),
            project_name: "project".to_owned(),
            current_working_directory: "/work".to_owned(),
            timestamp: "not-a-timestamp".to_owned(),
            session_id: "session-1".to_owned(),
            data: serde_json::from_value(json!({"secret": OPAQUE_DATA}))
                .expect("fixture observation data should deserialize"),
        },
        &FailingStore {
            attempts: Arc::clone(&attempts),
        },
    )
    .await
    .expect_err("invalid input must reject the delivery");

    assert_eq!(attempts.load(Ordering::SeqCst), 0);
    assert_eq!(
        error.to_string(),
        "handler error: observation event validation failed"
    );
    assert!(!error.to_string().contains(OPAQUE_DATA));
}
