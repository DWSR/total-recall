use harness_ingestion::contracts::{
    HarnessTimestamp, ObservationInput, SessionEndInput, SessionEndRequest, SessionStartInput,
    SessionStartRequest,
};
use serde_json::{Value, json};

const VALID_TIMESTAMP: &str = "2026-09-18T12:34:56.789Z";

fn session_start_value(timestamp: &str) -> Value {
    json!({
        "session_id": "session-1",
        "project_name": "project",
        "timestamp": timestamp,
        "current_working_directory": "/work",
    })
}

fn observation_value(timestamp: &str) -> Value {
    json!({
        "hook_type": "hook",
        "project_name": "project",
        "current_working_directory": "/work",
        "timestamp": timestamp,
        "session_id": "session-1",
        "data": {},
    })
}

fn observation_request_with_data(data: Value) -> harness_ingestion::contracts::ObservationRequest {
    let mut input = observation_value(VALID_TIMESTAMP);
    input["data"] = data;
    serde_json::from_value::<ObservationInput>(input)
        .unwrap()
        .into_request()
        .unwrap()
}

#[test]
fn session_start_requires_every_schema_field() {
    let input = json!({
        "session_id": "session-1",
        "project_name": "project",
        "timestamp": VALID_TIMESTAMP,
    });

    assert!(serde_json::from_value::<SessionStartInput>(input).is_err());
}

#[test]
fn session_end_requires_every_schema_field() {
    let input = json!({
        "session_id": "session-1",
        "project_name": "project",
        "timestamp": VALID_TIMESTAMP,
    });

    assert!(serde_json::from_value::<SessionEndInput>(input).is_err());
}

#[test]
fn observation_requires_every_schema_field() {
    let input = json!({
        "hook_type": "hook",
        "project_name": "project",
        "current_working_directory": "/work",
        "timestamp": VALID_TIMESTAMP,
        "session_id": "session-1",
    });

    assert!(serde_json::from_value::<ObservationInput>(input).is_err());
}

#[test]
fn adapters_reject_unknown_top_level_fields() {
    let mut session_start = session_start_value(VALID_TIMESTAMP);
    session_start["unexpected"] = json!(true);
    assert!(serde_json::from_value::<SessionStartInput>(session_start).is_err());

    let mut session_end = session_start_value(VALID_TIMESTAMP);
    session_end["unexpected"] = json!(true);
    assert!(serde_json::from_value::<SessionEndInput>(session_end).is_err());

    let mut observation = observation_value(VALID_TIMESTAMP);
    observation["unexpected"] = json!(true);
    assert!(serde_json::from_value::<ObservationInput>(observation).is_err());
}

#[test]
fn observation_data_must_be_a_json_object() {
    for data in [json!(null), json!([]), json!("not-an-object"), json!(true)] {
        let mut input = observation_value(VALID_TIMESTAMP);
        input["data"] = data;
        assert!(serde_json::from_value::<ObservationInput>(input).is_err());
    }
}

#[test]
fn observation_data_errors_do_not_include_opaque_values() {
    const SENTINEL: &str = "opaque-secret-value";

    let mut input = observation_value(VALID_TIMESTAMP);
    input["data"] = json!(SENTINEL);
    let error = serde_json::from_value::<ObservationInput>(input)
        .expect_err("non-object data should be rejected")
        .to_string();

    assert!(
        error.contains("data"),
        "error should identify data: {error}"
    );
    assert!(
        !error.contains(SENTINEL),
        "error must not include opaque data: {error}"
    );
}

#[test]
fn empty_session_ids_are_rejected() {
    let mut session_start = session_start_value(VALID_TIMESTAMP);
    session_start["session_id"] = json!("");
    assert!(serde_json::from_value::<SessionStartInput>(session_start).is_err());

    let mut session_end = session_start_value(VALID_TIMESTAMP);
    session_end["session_id"] = json!("");
    assert!(serde_json::from_value::<SessionEndInput>(session_end).is_err());

    let mut observation = observation_value(VALID_TIMESTAMP);
    observation["session_id"] = json!("");
    assert!(serde_json::from_value::<ObservationInput>(observation).is_err());
}

#[test]
fn timestamps_require_exactly_three_fractional_digits() {
    for timestamp in [
        "2026-09-18T12:34:56Z",
        "2026-09-18T12:34:56.78Z",
        "2026-09-18T12:34:56.7890Z",
        "2026-09-18T12:34:56.789",
    ] {
        assert!(
            serde_json::from_value::<HarnessTimestamp>(json!(timestamp)).is_err(),
            "timestamp should be rejected: {timestamp}"
        );
    }
}

#[test]
fn timestamps_require_valid_calendar_values() {
    for timestamp in [
        "2026-02-29T12:34:56.789Z",
        "2026-13-18T12:34:56.789Z",
        "2026-09-31T12:34:56.789Z",
    ] {
        assert!(
            serde_json::from_value::<HarnessTimestamp>(json!(timestamp)).is_err(),
            "timestamp should be rejected: {timestamp}"
        );
    }
}

#[test]
fn timestamps_require_valid_offsets() {
    for timestamp in [
        "2026-09-18T12:34:56.789+24:00",
        "2026-09-18T12:34:56.789-24:00",
        "2026-09-18T12:34:56.789+00:60",
        "2026-09-18T12:34:56.789+0100",
    ] {
        assert!(
            serde_json::from_value::<HarnessTimestamp>(json!(timestamp)).is_err(),
            "timestamp should be rejected: {timestamp}"
        );
    }
}

#[test]
fn valid_timestamps_preserve_source_text_and_case() {
    for timestamp in [
        "2026-09-18T12:34:56.789Z",
        "2026-09-18t12:34:56.789z",
        "2026-09-18T12:34:56.789+05:30",
    ] {
        let parsed: HarnessTimestamp = serde_json::from_value(json!(timestamp)).unwrap();
        assert_eq!(parsed.as_str(), timestamp);
    }
}

#[test]
fn other_required_strings_are_not_content_validated() {
    let input = json!({
        "session_id": "session-1",
        "project_name": "",
        "timestamp": VALID_TIMESTAMP,
        "current_working_directory": "",
    });

    let parsed: SessionStartInput = serde_json::from_value(input).unwrap();
    assert_eq!(parsed.project_name, "");
    assert_eq!(parsed.current_working_directory, "");
}

#[test]
fn lifecycle_inputs_convert_through_generated_request_messages() {
    let start: SessionStartInput =
        serde_json::from_value(session_start_value(VALID_TIMESTAMP)).unwrap();
    let start_request: SessionStartRequest = start.try_into().unwrap();
    assert_eq!(start_request.timestamp, VALID_TIMESTAMP);
    assert_eq!(start_request.session_id, "session-1");

    let end: SessionEndInput =
        serde_json::from_value(session_start_value(VALID_TIMESTAMP)).unwrap();
    let end_request: SessionEndRequest = end.try_into().unwrap();
    assert_eq!(end_request.timestamp, VALID_TIMESTAMP);
    assert_eq!(end_request.session_id, "session-1");
}

#[test]
fn observation_input_converts_scalar_fields_through_generated_request_message() {
    let input: ObservationInput =
        serde_json::from_value(observation_value(VALID_TIMESTAMP)).unwrap();
    let request = input.into_request().unwrap();

    assert_eq!(request.hook_type, "hook");
    assert_eq!(request.timestamp, VALID_TIMESTAMP);
    assert_eq!(request.session_id, "session-1");
    assert_eq!(
        serde_json::to_value(request.data.unwrap()).unwrap(),
        json!({})
    );
}

#[test]
fn observation_data_round_trips_nested_protojson_values_and_arbitrary_keys() {
    let request = observation_request_with_data(json!({
        "arbitrary.key/with spaces": {
            "nested key": [null, true, false, "text", 42.5, {"empty": {}}],
        },
        "session_id": "opaque-value",
    }));

    assert_eq!(
        serde_json::to_value(request).unwrap(),
        json!({
            "hook_type": "hook",
            "project_name": "project",
            "current_working_directory": "/work",
            "timestamp": VALID_TIMESTAMP,
            "session_id": "session-1",
            "data": {
                "arbitrary.key/with spaces": {
                    "nested key": [null, true, false, "text", 42.5, {"empty": {}}],
                },
                "session_id": "opaque-value",
            },
        })
    );
}

#[test]
fn observation_data_round_trips_empty_object() {
    let request = observation_request_with_data(json!({}));

    assert_eq!(
        serde_json::to_value(request.data.unwrap()).unwrap(),
        json!({})
    );
}

#[test]
fn observation_data_round_trips_exact_integer_boundary_using_f64_semantics() {
    let request = observation_request_with_data(json!({
        "exact": 9_007_199_254_740_991u64,
        "rounded": 9_007_199_254_740_993u64,
    }));

    assert_eq!(
        serde_json::to_value(request.data.unwrap()).unwrap(),
        json!({
            "exact": 9_007_199_254_740_991.0,
            "rounded": 9_007_199_254_740_992.0,
        })
    );
}
