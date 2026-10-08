use std::future::Future;

use async_trait::async_trait;
use iii_sdk::{IIIClient, protocol::TriggerRequest};
use serde_json::{Value, json};
use thiserror::Error;

use crate::{
    contracts::PersistableEvent,
    persistence::{AppendReceipt, EventStore, StoreError},
};

const DATABASE_EXECUTE_FUNCTION_ID: &str = "database::execute";
const SESSION_EVENTS_INSERT: &str = "INSERT INTO session_events (session_id, event_type, project_name, current_working_directory, source_timestamp_rfc3339, source_timestamp_utc) VALUES ($1, $2, $3, $4, $5, TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond'))";
const RAW_OBSERVATIONS_INSERT: &str = "INSERT INTO raw_observations (session_id, event_type, hook_type, project_name, current_working_directory, source_timestamp_rfc3339, source_timestamp_utc, data) VALUES ($1, $2, $3, $4, $5, $6, TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond'), $8::json)";

pub struct IiiDatabaseStore {
    client: IIIClient,
    database: String,
}

impl IiiDatabaseStore {
    pub fn new(client: IIIClient, database: impl Into<String>) -> Self {
        Self {
            client,
            database: database.into(),
        }
    }
}

#[async_trait]
impl EventStore for IiiDatabaseStore {
    async fn append(&self, event: &PersistableEvent) -> Result<AppendReceipt, StoreError> {
        let request = build_execute_request(&self.database, event).map_err(|_| StoreError)?;
        invoke_once(request, |request| self.client.trigger(request))
            .await
            .map_err(|_| StoreError)
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
enum DatabaseError {
    #[error("database invocation failed")]
    Invocation,
    #[error("database response was malformed")]
    MalformedResponse,
    #[error("database write was not confirmed")]
    UnconfirmedWrite,
    #[error("database event mapping failed")]
    EventMapping,
}

fn build_execute_request(
    database: &str,
    event: &PersistableEvent,
) -> Result<TriggerRequest, DatabaseError> {
    let (sql, params) = match event {
        PersistableEvent::SessionStart {
            event,
            source_timestamp_epoch_millis,
        } => (
            SESSION_EVENTS_INSERT,
            json!([
                event.session_id,
                event.event_type,
                event.project_name,
                event.current_working_directory,
                event.timestamp,
                source_timestamp_epoch_millis,
            ]),
        ),
        PersistableEvent::SessionEnd {
            event,
            source_timestamp_epoch_millis,
        } => (
            SESSION_EVENTS_INSERT,
            json!([
                event.session_id,
                event.event_type,
                event.project_name,
                event.current_working_directory,
                event.timestamp,
                source_timestamp_epoch_millis,
            ]),
        ),
        PersistableEvent::Observation {
            event,
            source_timestamp_epoch_millis,
        } => {
            let data =
                serde_json::to_value(event.data.as_ref().ok_or(DatabaseError::EventMapping)?)
                    .map_err(|_| DatabaseError::EventMapping)?;
            (
                RAW_OBSERVATIONS_INSERT,
                json!([
                    event.session_id,
                    event.event_type,
                    event.hook_type,
                    event.project_name,
                    event.current_working_directory,
                    event.timestamp,
                    source_timestamp_epoch_millis,
                    data,
                ]),
            )
        }
    };

    Ok(TriggerRequest {
        function_id: DATABASE_EXECUTE_FUNCTION_ID.to_owned(),
        payload: json!({
            "db": database,
            "sql": sql,
            "params": params,
        }),
        action: None,
        timeout_ms: None,
    })
}

async fn invoke_once<F, Fut>(
    request: TriggerRequest,
    invoke: F,
) -> Result<AppendReceipt, DatabaseError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    let response = invoke(request)
        .await
        .map_err(|_| DatabaseError::Invocation)?;
    parse_execute_response(response)
}

fn parse_execute_response(response: Value) -> Result<AppendReceipt, DatabaseError> {
    let response = response
        .as_object()
        .ok_or(DatabaseError::MalformedResponse)?;
    let affected_rows = response
        .get("affected_rows")
        .and_then(Value::as_u64)
        .ok_or(DatabaseError::MalformedResponse)?;

    match response.get("last_insert_id") {
        Some(Value::Null | Value::String(_)) => {}
        _ => return Err(DatabaseError::MalformedResponse),
    }

    if !response
        .get("returned_rows")
        .and_then(Value::as_array)
        .is_some_and(|rows| rows.iter().all(Value::is_object))
    {
        return Err(DatabaseError::MalformedResponse);
    }

    if affected_rows != 1 {
        return Err(DatabaseError::UnconfirmedWrite);
    }

    Ok(AppendReceipt { affected_rows })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use iii_sdk::{Error as IiiError, protocol::TriggerRequestWithMetadata};
    use serde_json::json;

    use super::*;
    use crate::contracts::adapt_queued_event;

    const DATABASE: &str = "harness-ledger";
    const TIMESTAMP: &str = "2026-09-18T12:34:56.789Z";
    const EPOCH_MILLIS: i64 = 1_789_734_896_789;
    const SESSION_EVENTS_SQL: &str = "INSERT INTO session_events (session_id, event_type, project_name, current_working_directory, source_timestamp_rfc3339, source_timestamp_utc) VALUES ($1, $2, $3, $4, $5, TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond'))";
    const RAW_OBSERVATIONS_SQL: &str = "INSERT INTO raw_observations (session_id, event_type, hook_type, project_name, current_working_directory, source_timestamp_rfc3339, source_timestamp_utc, data) VALUES ($1, $2, $3, $4, $5, $6, TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond'), $8::json)";

    fn session_start() -> PersistableEvent {
        adapt_queued_event(json!({
            "event_type": "session_start",
            "session_id": "session-1",
            "project_name": "project",
            "timestamp": TIMESTAMP,
            "current_working_directory": "/work",
        }))
        .expect("fixture should adapt")
    }

    fn session_end() -> PersistableEvent {
        adapt_queued_event(json!({
            "event_type": "session_end",
            "session_id": "session-1",
            "project_name": "project",
            "timestamp": TIMESTAMP,
            "current_working_directory": "/work",
        }))
        .expect("fixture should adapt")
    }

    fn observation() -> PersistableEvent {
        adapt_queued_event(json!({
            "event_type": "observation",
            "hook_type": "post_tool_use",
            "project_name": "project",
            "current_working_directory": "/work",
            "timestamp": TIMESTAMP,
            "session_id": "session-1",
            "data": {
                "arbitrary.key/with spaces": [null, true, {"nested": "value"}],
                "rounded": 9_007_199_254_740_993u64,
                "opaque": "opaque-observation-data",
            },
        }))
        .expect("fixture should adapt")
    }

    fn request_for(event: &PersistableEvent) -> TriggerRequest {
        build_execute_request(DATABASE, event).expect("adapter should build an execute request")
    }

    fn successful_response(affected_rows: Value) -> Value {
        json!({
            "affected_rows": affected_rows,
            "last_insert_id": null,
            "returned_rows": [],
        })
    }

    fn assert_request(request: TriggerRequest, sql: &str, params: Value) {
        assert_eq!(request.function_id, DATABASE_EXECUTE_FUNCTION_ID);
        assert_eq!(
            request.payload,
            json!({
                "db": DATABASE,
                "sql": sql,
                "params": params,
            })
        );
        assert!(request.action.is_none());
        assert_eq!(request.timeout_ms, None);

        let metadata: TriggerRequestWithMetadata = request.into();
        assert!(format!("{metadata:?}").contains("namespace: None"));
    }

    #[test]
    fn lifecycle_requests_use_the_static_insert_and_preserve_start_and_end_values() {
        assert_request(
            request_for(&session_start()),
            SESSION_EVENTS_SQL,
            json!([
                "session-1",
                "session_start",
                "project",
                "/work",
                TIMESTAMP,
                EPOCH_MILLIS,
            ]),
        );
        assert_request(
            request_for(&session_end()),
            SESSION_EVENTS_SQL,
            json!([
                "session-1",
                "session_end",
                "project",
                "/work",
                TIMESTAMP,
                EPOCH_MILLIS,
            ]),
        );
    }

    #[test]
    fn observation_request_uses_json_values_and_preserves_post_tool_use_data() {
        assert_request(
            request_for(&observation()),
            RAW_OBSERVATIONS_SQL,
            json!([
                "session-1",
                "observation",
                "post_tool_use",
                "project",
                "/work",
                TIMESTAMP,
                EPOCH_MILLIS,
                {
                    "arbitrary.key/with spaces": [null, true, {"nested": "value"}],
                    "rounded": 9_007_199_254_740_992.0,
                    "opaque": "opaque-observation-data",
                },
            ]),
        );
    }

    #[test]
    fn static_inserts_do_not_return_receipts_or_access_embeddings() {
        for sql in [SESSION_EVENTS_SQL, RAW_OBSERVATIONS_SQL] {
            assert!(!sql.contains("RETURNING"));
            assert!(!sql.contains("receipt_id"));
            assert!(!sql.contains("uuid"));
            assert!(!sql.contains("session_embeddings"));
        }
    }

    #[tokio::test]
    async fn exactly_one_affected_row_confirms_the_write_after_one_invocation() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_call = Arc::clone(&attempts);

        let receipt = invoke_once(request_for(&session_start()), move |_| {
            attempts_for_call.fetch_add(1, Ordering::SeqCst);
            async { Ok(successful_response(json!(1))) }
        })
        .await
        .expect("one affected row should confirm the write");

        assert_eq!(receipt, AppendReceipt { affected_rows: 1 });
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn nullable_last_insert_id_and_object_array_returned_rows_are_required_and_accepted() {
        let response = json!({
            "affected_rows": 1,
            "last_insert_id": "generated-receipt",
            "returned_rows": [{"commit": "confirmed"}],
        });

        assert_eq!(
            invoke_once(request_for(&session_start()), |_| async { Ok(response) })
                .await
                .expect("valid response should be accepted"),
            AppendReceipt { affected_rows: 1 }
        );
    }

    #[tokio::test]
    async fn invocation_failures_and_timeouts_are_opaque_and_not_retried() {
        const OPAQUE_DATA: &str = "opaque-observation-data";
        const CREDENTIAL: &str = "database-password";
        const SQL_PARAM: &str = "session-1";
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_call = Arc::clone(&attempts);
        let error = invoke_once(request_for(&observation()), move |_| {
            attempts_for_call.fetch_add(1, Ordering::SeqCst);
            async move {
                Err(IiiError::Remote {
                    code: "DATABASE_UNAVAILABLE".to_owned(),
                    message: CREDENTIAL.to_owned(),
                    stacktrace: Some("full remote payload".to_owned()),
                })
            }
        })
        .await
        .expect_err("invocation failure should be rejected");
        assert_eq!(error, DatabaseError::Invocation);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        for sensitive in [OPAQUE_DATA, CREDENTIAL, SQL_PARAM, "full remote payload"] {
            assert!(!error.to_string().contains(sensitive));
        }

        let timeout = invoke_once(request_for(&observation()), |_| async {
            Err(IiiError::Timeout)
        })
        .await
        .expect_err("timeout should be rejected");
        assert_eq!(timeout, DatabaseError::Invocation);
        assert!(!timeout.to_string().contains(OPAQUE_DATA));
    }

    #[tokio::test]
    async fn malformed_responses_are_rejected_without_exposing_their_contents() {
        const OPAQUE_DATA: &str = "opaque-observation-data";
        const CREDENTIAL: &str = "database-password";
        const FULL_PAYLOAD: &str = "full-response-payload";
        let malformed = [
            json!(null),
            json!({"last_insert_id": null, "returned_rows": []}),
            json!({"affected_rows": 1, "returned_rows": []}),
            json!({"affected_rows": 1, "last_insert_id": null}),
            json!({"affected_rows": -1, "last_insert_id": null, "returned_rows": []}),
            json!({"affected_rows": 1.0, "last_insert_id": null, "returned_rows": []}),
            json!({"affected_rows": "1", "last_insert_id": null, "returned_rows": []}),
            json!({"affected_rows": 1, "last_insert_id": 1, "returned_rows": []}),
            json!({"affected_rows": 1, "last_insert_id": false, "returned_rows": []}),
            json!({"affected_rows": 1, "last_insert_id": null, "returned_rows": {}}),
            json!({"affected_rows": 1, "last_insert_id": null, "returned_rows": [null]}),
            json!({
                "affected_rows": 1,
                "last_insert_id": CREDENTIAL,
                "returned_rows": {"opaque": OPAQUE_DATA, "payload": FULL_PAYLOAD},
            }),
        ];

        for response in malformed {
            let error = invoke_once(request_for(&observation()), |_| async { Ok(response) })
                .await
                .expect_err("malformed response should be rejected");
            assert_eq!(error, DatabaseError::MalformedResponse);
            for sensitive in [OPAQUE_DATA, CREDENTIAL, FULL_PAYLOAD, "session-1"] {
                assert!(!error.to_string().contains(sensitive));
            }
        }
    }

    #[tokio::test]
    async fn zero_and_multiple_affected_rows_are_not_confirmed_writes() {
        for affected_rows in [json!(0), json!(2), json!(u64::MAX)] {
            let error = invoke_once(request_for(&session_end()), |_| async {
                Ok(successful_response(affected_rows))
            })
            .await
            .expect_err("only one affected row should be confirmed");
            assert_eq!(error, DatabaseError::UnconfirmedWrite);
            assert!(!error.to_string().contains("session-1"));
        }
    }
}
