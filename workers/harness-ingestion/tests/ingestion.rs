use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use harness_ingestion::{
    DispatchResponse, IngestionService,
    contracts::{ObservationInput, SessionEndInput, SessionStartInput},
    publisher::{PublishError, QueuePublisher},
};
use iii_sdk::Error as IiiError;
use schemars::schema_for;
use serde_json::{Value, json};

const VALID_TIMESTAMP: &str = "2026-09-18T12:34:56.789Z";

#[test]
fn dispatch_response_exposes_its_json_and_schema_contract() {
    let response = DispatchResponse { dispatched: true };
    assert_eq!(
        serde_json::to_value(response).unwrap(),
        json!({"dispatched": true})
    );

    let schema = serde_json::to_value(schema_for!(DispatchResponse)).unwrap();
    assert_eq!(schema["properties"]["dispatched"]["type"], "boolean");
}

#[derive(Clone, Default)]
struct RecordingPublisher {
    events: Arc<Mutex<Vec<Value>>>,
}

impl RecordingPublisher {
    fn events(&self) -> Vec<Value> {
        self.events.lock().unwrap().clone()
    }
}

#[async_trait]
impl QueuePublisher for RecordingPublisher {
    async fn publish(&self, event: Value) -> Result<(), PublishError> {
        self.events.lock().unwrap().push(event);
        Ok(())
    }
}

struct FailingPublisher {
    attempts: Arc<AtomicUsize>,
}

#[async_trait]
impl QueuePublisher for FailingPublisher {
    async fn publish(&self, _event: Value) -> Result<(), PublishError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(PublishError::Invocation(IiiError::Remote {
            code: "QUEUE_UNAVAILABLE".to_owned(),
            message: "queue unavailable".to_owned(),
            stacktrace: None,
        }))
    }
}

fn session_start_input() -> SessionStartInput {
    serde_json::from_value(json!({
        "session_id": "session-start",
        "project_name": "project",
        "timestamp": VALID_TIMESTAMP,
        "current_working_directory": "/work",
    }))
    .unwrap()
}

fn session_end_input() -> SessionEndInput {
    serde_json::from_value(json!({
        "session_id": "session-end",
        "project_name": "project",
        "timestamp": VALID_TIMESTAMP,
        "current_working_directory": "/work",
    }))
    .unwrap()
}

fn observation_input() -> ObservationInput {
    serde_json::from_value(json!({
        "hook_type": "tool.finished",
        "project_name": "project",
        "current_working_directory": "/work",
        "timestamp": VALID_TIMESTAMP,
        "session_id": "session-observation",
        "data": {
            "arbitrary.key/with spaces": {
                "nested": [null, true, "opaque-value", 42.5]
            },
            "another-key": "preserved",
        },
    }))
    .unwrap()
}

#[tokio::test]
async fn session_start_publishes_the_exact_protojson_event() {
    let publisher = RecordingPublisher::default();
    let service = IngestionService::new(publisher.clone());

    let response = service.session_start(session_start_input()).await.unwrap();

    assert_eq!(response, DispatchResponse { dispatched: true });
    assert_eq!(
        publisher.events(),
        vec![json!({
            "event_type": "session_start",
            "session_id": "session-start",
            "project_name": "project",
            "timestamp": VALID_TIMESTAMP,
            "current_working_directory": "/work",
        })]
    );
}

#[tokio::test]
async fn observation_publishes_the_exact_protojson_event_and_preserves_opaque_data() {
    let publisher = RecordingPublisher::default();
    let service = IngestionService::new(publisher.clone());

    let response = service.observation(observation_input()).await.unwrap();

    assert_eq!(response, DispatchResponse { dispatched: true });
    assert_eq!(
        publisher.events(),
        vec![json!({
            "event_type": "observation",
            "hook_type": "tool.finished",
            "project_name": "project",
            "current_working_directory": "/work",
            "timestamp": VALID_TIMESTAMP,
            "session_id": "session-observation",
            "data": {
                "arbitrary.key/with spaces": {
                    "nested": [null, true, "opaque-value", 42.5]
                },
                "another-key": "preserved",
            },
        })]
    );
}

#[tokio::test]
async fn session_end_publishes_the_exact_protojson_event() {
    let publisher = RecordingPublisher::default();
    let service = IngestionService::new(publisher.clone());

    let response = service.session_end(session_end_input()).await.unwrap();

    assert_eq!(response, DispatchResponse { dispatched: true });
    assert_eq!(
        publisher.events(),
        vec![json!({
            "event_type": "session_end",
            "session_id": "session-end",
            "project_name": "project",
            "timestamp": VALID_TIMESTAMP,
            "current_working_directory": "/work",
        })]
    );
}

#[tokio::test]
async fn each_accepted_call_is_published_once_without_session_state() {
    let publisher = RecordingPublisher::default();
    let service = IngestionService::new(publisher.clone());

    service.session_end(session_end_input()).await.unwrap();
    service.observation(observation_input()).await.unwrap();

    let events = publisher.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["event_type"], "session_end");
    assert_eq!(events[1]["event_type"], "observation");
}

#[tokio::test]
async fn publisher_errors_are_visible_without_retries_or_opaque_data() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let service = IngestionService::new(FailingPublisher {
        attempts: Arc::clone(&attempts),
    });

    let error = service
        .observation(observation_input())
        .await
        .expect_err("publisher failure should reach the caller");

    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(error.to_string(), "event publication failed");
    assert!(!error.to_string().contains("opaque-value"));
}
