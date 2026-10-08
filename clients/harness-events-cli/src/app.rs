use std::future::Future;

use crate::contracts::Submission;

#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvokeError {
    #[error("engine connection failed")]
    Connection,
    #[error("function invocation failed")]
    Invocation,
}

#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum SubmitError {
    #[error("submission failed")]
    Invoke(InvokeError),
    #[error("unexpected submission response")]
    UnexpectedResponse,
}

pub async fn submit_once<F, Fut>(submission: Submission, invoke: F) -> Result<(), SubmitError>
where
    F: FnOnce(Submission) -> Fut,
    Fut: Future<Output = Result<serde_json::Value, InvokeError>>,
{
    let response = invoke(submission).await.map_err(SubmitError::Invoke)?;

    if response != serde_json::json!({"dispatched": true}) {
        return Err(SubmitError::UnexpectedResponse);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use serde_json::{Value, json};

    use crate::contracts::{IngestionFunction, Submission};

    use super::{InvokeError, SubmitError, submit_once};

    fn submission() -> Submission {
        Submission {
            function_id: IngestionFunction::SessionStart,
            payload: json!({"session_id": "payload-sentinel"}),
        }
    }

    #[tokio::test]
    async fn accepts_only_the_exact_dispatched_response() {
        let result = submit_once(submission(), |_| async { Ok(json!({"dispatched": true})) }).await;

        assert_eq!(result, Ok(()));
    }

    #[tokio::test]
    async fn rejects_every_non_exact_response() {
        let responses = [
            ("null", Value::Null),
            ("boolean scalar", json!(true)),
            ("string scalar", json!("dispatched")),
            ("number scalar", json!(1)),
            ("array", json!([{"dispatched": true}])),
            ("empty object", json!({})),
            ("false dispatched value", json!({"dispatched": false})),
            ("string dispatched value", json!({"dispatched": "true"})),
            ("number dispatched value", json!({"dispatched": 1})),
            ("unrelated object", json!({"accepted": true})),
            (
                "success with an extra field",
                json!({"dispatched": true, "extra": "unexpected"}),
            ),
        ];

        for (description, response) in responses {
            let result = submit_once(submission(), move |_| async move { Ok(response) }).await;

            assert_eq!(
                result,
                Err(SubmitError::UnexpectedResponse),
                "{description} must be rejected"
            );
        }
    }

    #[tokio::test]
    async fn propagates_connection_and_invocation_failures() {
        let connection = submit_once(submission(), |_| async {
            Err::<Value, _>(InvokeError::Connection)
        })
        .await;
        let remote_rejection = submit_once(submission(), |_| async {
            Err::<Value, _>(InvokeError::Invocation)
        })
        .await;
        let timeout = submit_once(submission(), |_| async {
            Err::<Value, _>(InvokeError::Invocation)
        })
        .await;

        assert_eq!(
            connection,
            Err(SubmitError::Invoke(InvokeError::Connection))
        );
        assert_eq!(
            remote_rejection,
            Err(SubmitError::Invoke(InvokeError::Invocation))
        );
        assert_eq!(timeout, Err(SubmitError::Invoke(InvokeError::Invocation)));
    }

    #[tokio::test]
    async fn invokes_once_for_success_injected_failure_and_unexpected_response() {
        let success_calls = Arc::new(AtomicUsize::new(0));
        let success_result = submit_once(submission(), {
            let calls = Arc::clone(&success_calls);
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(json!({"dispatched": true})) }
            }
        })
        .await;

        assert_eq!(success_result, Ok(()));
        assert_eq!(success_calls.load(Ordering::SeqCst), 1);

        let failure_calls = Arc::new(AtomicUsize::new(0));
        let failure_result = submit_once(submission(), {
            let calls = Arc::clone(&failure_calls);
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err::<Value, _>(InvokeError::Invocation) }
            }
        })
        .await;

        assert_eq!(
            failure_result,
            Err(SubmitError::Invoke(InvokeError::Invocation))
        );
        assert_eq!(failure_calls.load(Ordering::SeqCst), 1);

        let unexpected_calls = Arc::new(AtomicUsize::new(0));
        let unexpected_result = submit_once(submission(), {
            let calls = Arc::clone(&unexpected_calls);
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(json!({"dispatched": false})) }
            }
        })
        .await;

        assert_eq!(unexpected_result, Err(SubmitError::UnexpectedResponse));
        assert_eq!(unexpected_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn error_display_is_opaque_to_response_and_payload_content() {
        let payload_sentinel = "payload-sentinel";
        let response_sentinel = "response-sentinel";
        let submission = Submission {
            function_id: IngestionFunction::Observation,
            payload: json!({"opaque": payload_sentinel}),
        };
        let error = submit_once(submission, move |_| async move {
            Ok(json!({"dispatched": response_sentinel}))
        })
        .await
        .expect_err("unexpected responses must fail");
        let display = error.to_string();

        assert!(!display.contains(payload_sentinel));
        assert!(!display.contains(response_sentinel));
    }
}
