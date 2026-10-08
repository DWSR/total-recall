use std::io;

use harness_events_cli::{
    app,
    cli::{EventCommand, LifecycleArgs, ObservationArgs},
    contracts::{self, IngestionFunction, Submission},
};
use harness_ingestion::{
    DispatchResponse,
    contracts::{
        ObservationInput, SessionEndInput, SessionEndRequest, SessionStartInput,
        SessionStartRequest,
    },
    runtime::HARNESS_FUNCTION_IDS,
};
use serde_json::{Value, json};

const TIMESTAMP: &str = "2026-09-19T12:34:56.000Z";
const OBSERVATION_STDIN: &[u8] = br#"{
    "arbitrary.key/with spaces": {
        "nested key": [null, true, "value", {"inner/key": false}],
        "nested object": {"deeper key": ["nested", 1.5]}
    },
    "rounded": 9007199254740993
}"#;

fn lifecycle_args(session_id: &str) -> LifecycleArgs {
    LifecycleArgs {
        session_id: session_id.to_owned(),
        project_name: "contract-project".to_owned(),
        current_working_directory: "/workspace/contract".to_owned(),
        timestamp: TIMESTAMP.to_owned(),
    }
}

fn client_submissions() -> [Submission; 3] {
    [
        contracts::prepare(
            EventCommand::SessionStart(lifecycle_args("contract-session-start")),
            io::empty(),
        )
        .expect("session-start command should prepare"),
        contracts::prepare(
            EventCommand::Observation(ObservationArgs {
                hook_type: "PostToolUse".to_owned(),
                project_name: "contract-project".to_owned(),
                current_working_directory: "/workspace/contract".to_owned(),
                timestamp: TIMESTAMP.to_owned(),
                session_id: "contract-observation".to_owned(),
            }),
            OBSERVATION_STDIN,
        )
        .expect("observation command should prepare"),
        contracts::prepare(
            EventCommand::SessionEnd(lifecycle_args("contract-session-end")),
            io::empty(),
        )
        .expect("session-end command should prepare"),
    ]
}

fn assert_observation_struct_semantics(payload: &Value) {
    let data = &payload["data"];

    assert_eq!(
        data["arbitrary.key/with spaces"]["nested key"],
        json!([null, true, "value", {"inner/key": false}])
    );
    assert_eq!(
        data["arbitrary.key/with spaces"]["nested object"],
        json!({"deeper key": ["nested", 1.5]})
    );
    assert_eq!(
        data["rounded"].as_f64(),
        Some(9_007_199_254_740_992.0),
        "protobuf Struct numbers must use f64 semantics"
    );
}

#[test]
fn client_closed_function_set_matches_authoritative_ingestion_ids() {
    assert_eq!(
        IngestionFunction::ALL.map(IngestionFunction::as_str),
        HARNESS_FUNCTION_IDS
    );
}

#[test]
fn client_protojson_payloads_round_trip_through_authoritative_ingestion_inputs() {
    let [session_start, observation, session_end] = client_submissions();

    assert_eq!(
        [
            session_start.function_id,
            observation.function_id,
            session_end.function_id,
        ],
        [
            IngestionFunction::SessionStart,
            IngestionFunction::Observation,
            IngestionFunction::SessionEnd,
        ]
    );

    let session_start_input: SessionStartInput =
        serde_json::from_value(session_start.payload.clone())
            .expect("client session-start payload should deserialize through the worker input");
    let session_start_request: SessionStartRequest = session_start_input
        .try_into()
        .expect("worker session-start input should convert to its request");
    assert_eq!(
        serde_json::to_value(session_start_request)
            .expect("worker session-start request should serialize"),
        session_start.payload
    );

    assert_observation_struct_semantics(&observation.payload);
    let observation_input: ObservationInput = serde_json::from_value(observation.payload.clone())
        .expect("client observation payload should deserialize through the worker input");
    let observation_request = observation_input
        .into_request()
        .expect("worker observation input should convert to its request");
    let authoritative_observation_payload = serde_json::to_value(observation_request)
        .expect("worker observation request should serialize");
    assert_observation_struct_semantics(&authoritative_observation_payload);
    assert_eq!(authoritative_observation_payload, observation.payload);

    let session_end_input: SessionEndInput = serde_json::from_value(session_end.payload.clone())
        .expect("client session-end payload should deserialize through the worker input");
    let session_end_request: SessionEndRequest = session_end_input
        .try_into()
        .expect("worker session-end input should convert to its request");
    assert_eq!(
        serde_json::to_value(session_end_request)
            .expect("worker session-end request should serialize"),
        session_end.payload
    );
}

#[tokio::test]
async fn client_accepts_the_authoritative_dispatch_response() {
    let dispatch_success = serde_json::to_value(DispatchResponse { dispatched: true })
        .expect("worker dispatch response should serialize");

    for submission in client_submissions() {
        app::submit_once(submission, {
            let dispatch_success = dispatch_success.clone();
            move |_| async move { Ok::<_, app::InvokeError>(dispatch_success) }
        })
        .await
        .expect("client should accept the worker-derived dispatch success response");
    }
}
