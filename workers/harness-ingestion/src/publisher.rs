use std::future::Future;

use async_trait::async_trait;
use iii_sdk::{IIIClient, protocol::TriggerRequest};
use serde_json::{Value, json};
use thiserror::Error;

pub const DURABLE_PUBLISH_FUNCTION_ID: &str = "iii::durable::publish";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueueTopic(String);

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("queue topic must not be blank")]
pub struct QueueTopicError;

impl QueueTopic {
    pub fn new(topic: impl Into<String>) -> Result<Self, QueueTopicError> {
        let topic = topic.into();
        let topic = topic.trim();
        if topic.is_empty() {
            return Err(QueueTopicError);
        }

        Ok(Self(topic.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for QueueTopic {
    type Error = QueueTopicError;

    fn try_from(topic: String) -> Result<Self, Self::Error> {
        Self::new(topic)
    }
}

impl TryFrom<&str> for QueueTopic {
    type Error = QueueTopicError;

    fn try_from(topic: &str) -> Result<Self, Self::Error> {
        Self::new(topic)
    }
}

#[derive(Debug, Error)]
pub enum PublishError {
    #[error("queue invocation failed")]
    Invocation(#[source] iii_sdk::Error),
    #[error("queue invocation returned an unexpected result")]
    UnexpectedResult,
}

#[async_trait]
pub trait QueuePublisher: Send + Sync {
    async fn publish(&self, event: Value) -> Result<(), PublishError>;
}

pub struct IiiQueuePublisher {
    client: IIIClient,
    topic: QueueTopic,
}

impl IiiQueuePublisher {
    pub fn new(client: IIIClient, topic: QueueTopic) -> Self {
        Self { client, topic }
    }
}

#[async_trait]
impl QueuePublisher for IiiQueuePublisher {
    async fn publish(&self, event: Value) -> Result<(), PublishError> {
        let request = build_publish_request(&self.topic, event);
        invoke_once(request, |request| self.client.trigger(request)).await
    }
}

pub fn build_publish_request(topic: &QueueTopic, event: Value) -> TriggerRequest {
    TriggerRequest {
        function_id: DURABLE_PUBLISH_FUNCTION_ID.to_owned(),
        payload: json!({
            "topic": topic.as_str(),
            "data": event,
        }),
        action: None,
        timeout_ms: None,
    }
}

async fn invoke_once<F, Fut>(request: TriggerRequest, invoke: F) -> Result<(), PublishError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    match invoke(request).await.map_err(PublishError::Invocation)? {
        Value::Null => Ok(()),
        _ => Err(PublishError::UnexpectedResult),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use iii_sdk::Error as IiiError;
    use serde_json::json;

    use super::*;

    #[test]
    fn publish_request_contains_only_the_validated_topic_and_event_data() {
        let topic = QueueTopic::new("harness-events").unwrap();
        let event = json!({"event_type": "session_start", "session_id": "session-1"});

        let request = build_publish_request(&topic, event.clone());

        assert_eq!(request.function_id, DURABLE_PUBLISH_FUNCTION_ID);
        assert_eq!(
            request.payload,
            json!({"topic": "harness-events", "data": event})
        );
        assert!(request.action.is_none());
        assert_eq!(request.timeout_ms, None);
    }

    #[test]
    fn publish_request_leaves_namespace_unset_for_client_inheritance() {
        let topic = QueueTopic::new("harness-events").unwrap();
        let request = build_publish_request(&topic, json!({"event_type": "observation"}));
        let request_with_metadata: iii_sdk::protocol::TriggerRequestWithMetadata = request.into();

        assert!(format!("{request_with_metadata:?}").contains("namespace: None"));
    }

    #[test]
    fn queue_topic_is_trimmed_and_rejects_blank_values() {
        assert_eq!(
            QueueTopic::new("  harness-events  ").unwrap().as_str(),
            "harness-events"
        );
        assert!(QueueTopic::new(" \t\n ").is_err());
    }

    #[tokio::test]
    async fn null_invocation_result_maps_to_success() {
        let topic = QueueTopic::new("harness-events").unwrap();
        let request = build_publish_request(&topic, json!({"event_type": "observation"}));

        let result = invoke_once(request, |_| async { Ok(json!(null)) }).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn non_null_invocation_result_is_an_error() {
        let topic = QueueTopic::new("harness-events").unwrap();
        let request = build_publish_request(&topic, json!({"event_type": "session_end"}));

        let result = invoke_once(request, |_| async { Ok(json!({"receipt": "unexpected"})) }).await;

        assert!(matches!(result, Err(PublishError::UnexpectedResult)));
    }

    #[tokio::test]
    async fn invocation_errors_are_propagated_without_event_data() {
        let topic = QueueTopic::new("harness-events").unwrap();
        let request = build_publish_request(
            &topic,
            json!({"event_type": "observation", "opaque": "event-secret"}),
        );

        let result = invoke_once(request, |_| async {
            Err(IiiError::Remote {
                code: "QUEUE_UNAVAILABLE".to_owned(),
                message: "queue unavailable".to_owned(),
                stacktrace: None,
            })
        })
        .await;

        let error = result.expect_err("the invocation error should be returned");
        assert!(matches!(error, PublishError::Invocation(_)));
        assert!(!error.to_string().contains("event-secret"));
    }

    #[tokio::test]
    async fn invocation_is_attempted_once_without_retry() {
        let topic = QueueTopic::new("harness-events").unwrap();
        let request = build_publish_request(&topic, json!({"event_type": "observation"}));
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_call = Arc::clone(&attempts);

        let result = invoke_once(request, move |_| {
            attempts_for_call.fetch_add(1, Ordering::SeqCst);
            async { Err(IiiError::Timeout) }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
