use harness_event_persistence::contracts::{
    ObservationEvent, PersistableEvent, SessionEndEvent, SessionStartEvent, adapt_queued_event,
};
use serde_json::{Value, json};

const UTC_TIMESTAMP: &str = "2026-09-18T12:34:56.789Z";
const LOWERCASE_UTC_TIMESTAMP: &str = "2026-09-18t12:34:56.789z";
const OFFSET_TIMESTAMP: &str = "2026-09-18T12:34:56.789+05:30";
const UTC_EPOCH_MILLIS: i64 = 1_789_734_896_789;
const OFFSET_EPOCH_MILLIS: i64 = 1_789_715_096_789;

fn session_start(timestamp: &str) -> Value {
    json!({
        "event_type": "session_start",
        "session_id": "session-1",
        "project_name": "project",
        "timestamp": timestamp,
        "current_working_directory": "/work",
    })
}

fn observation(timestamp: &str, hook_type: &str, data: Value) -> Value {
    json!({
        "event_type": "observation",
        "hook_type": hook_type,
        "project_name": "project",
        "current_working_directory": "/work",
        "timestamp": timestamp,
        "session_id": "session-1",
        "data": data,
    })
}

fn session_end(timestamp: &str) -> Value {
    json!({
        "event_type": "session_end",
        "session_id": "session-1",
        "project_name": "project",
        "timestamp": timestamp,
        "current_working_directory": "/work",
    })
}

fn adapt(value: Value) -> PersistableEvent {
    adapt_queued_event(value).expect("valid queued event should adapt")
}

fn reject(value: Value) -> String {
    adapt_queued_event(value)
        .expect_err("invalid queued event should be rejected")
        .to_string()
}

fn without_field(mut value: Value, field: &str) -> Value {
    value
        .as_object_mut()
        .expect("fixture should be an object")
        .remove(field);
    value
}

fn with_unknown_field(mut value: Value) -> Value {
    value["unexpected"] = json!(true);
    value
}

#[test]
fn accepts_all_tagged_generated_protojson_variants() {
    let start = adapt(
        serde_json::to_value(SessionStartEvent {
            event_type: "session_start".to_owned(),
            session_id: "session-1".to_owned(),
            project_name: "project".to_owned(),
            timestamp: LOWERCASE_UTC_TIMESTAMP.to_owned(),
            current_working_directory: "/work".to_owned(),
        })
        .unwrap(),
    );
    match start {
        PersistableEvent::SessionStart {
            event,
            source_timestamp_epoch_millis,
        } => {
            assert_eq!(event.event_type, "session_start");
            assert_eq!(event.session_id, "session-1");
            assert_eq!(event.project_name, "project");
            assert_eq!(event.timestamp, LOWERCASE_UTC_TIMESTAMP);
            assert_eq!(event.current_working_directory, "/work");
            assert_eq!(source_timestamp_epoch_millis, UTC_EPOCH_MILLIS);
        }
        _ => panic!("expected a session-start event"),
    }

    let observation = adapt(
        serde_json::to_value(ObservationEvent {
            event_type: "observation".to_owned(),
            hook_type: "post_tool_use".to_owned(),
            project_name: "project".to_owned(),
            current_working_directory: "/work".to_owned(),
            timestamp: OFFSET_TIMESTAMP.to_owned(),
            session_id: "session-1".to_owned(),
            data: Some(serde_json::from_value(json!({})).unwrap()),
        })
        .unwrap(),
    );
    match observation {
        PersistableEvent::Observation {
            event,
            source_timestamp_epoch_millis,
        } => {
            assert_eq!(event.event_type, "observation");
            assert_eq!(event.hook_type, "post_tool_use");
            assert_eq!(event.project_name, "project");
            assert_eq!(event.current_working_directory, "/work");
            assert_eq!(event.timestamp, OFFSET_TIMESTAMP);
            assert_eq!(event.session_id, "session-1");
            assert_eq!(serde_json::to_value(event.data).unwrap(), json!({}));
            assert_eq!(source_timestamp_epoch_millis, OFFSET_EPOCH_MILLIS);
        }
        _ => panic!("expected an observation event"),
    }

    let end = adapt(
        serde_json::to_value(SessionEndEvent {
            event_type: "session_end".to_owned(),
            session_id: "session-1".to_owned(),
            project_name: "project".to_owned(),
            timestamp: UTC_TIMESTAMP.to_owned(),
            current_working_directory: "/work".to_owned(),
        })
        .unwrap(),
    );
    match end {
        PersistableEvent::SessionEnd {
            event,
            source_timestamp_epoch_millis,
        } => {
            assert_eq!(event.event_type, "session_end");
            assert_eq!(event.session_id, "session-1");
            assert_eq!(event.project_name, "project");
            assert_eq!(event.timestamp, UTC_TIMESTAMP);
            assert_eq!(event.current_working_directory, "/work");
            assert_eq!(source_timestamp_epoch_millis, UTC_EPOCH_MILLIS);
        }
        _ => panic!("expected a session-end event"),
    }
}

#[test]
fn allows_whitespace_session_ids_and_empty_non_session_strings() {
    let mut start = session_start(UTC_TIMESTAMP);
    start["session_id"] = json!(" \t ");
    start["project_name"] = json!("");
    start["current_working_directory"] = json!("");
    assert!(matches!(
        adapt(start),
        PersistableEvent::SessionStart { event, .. }
            if event.session_id == " \t "
                && event.project_name.is_empty()
                && event.current_working_directory.is_empty()
    ));

    let mut observation = observation(UTC_TIMESTAMP, "", json!({}));
    observation["session_id"] = json!(" \t ");
    observation["project_name"] = json!("");
    observation["current_working_directory"] = json!("");
    assert!(matches!(
        adapt(observation),
        PersistableEvent::Observation { event, .. }
            if event.session_id == " \t "
                && event.hook_type.is_empty()
                && event.project_name.is_empty()
                && event.current_working_directory.is_empty()
    ));

    let mut end = session_end(UTC_TIMESTAMP);
    end["session_id"] = json!(" \t ");
    end["project_name"] = json!("");
    end["current_working_directory"] = json!("");
    assert!(matches!(
        adapt(end),
        PersistableEvent::SessionEnd { event, .. }
            if event.session_id == " \t "
                && event.project_name.is_empty()
                && event.current_working_directory.is_empty()
    ));
}

#[test]
fn preserves_arbitrary_observation_data_with_protobuf_double_number_semantics() {
    let event = adapt(observation(
        UTC_TIMESTAMP,
        "post_tool_use",
        json!({
            "arbitrary.key/with spaces": {
                "nested key": [null, true, false, "text", 42.5, {"empty": {}}],
            },
            "exact": 9_007_199_254_740_991u64,
            "rounded": 9_007_199_254_740_993u64,
            "session_id": "opaque-value",
        }),
    ));

    let PersistableEvent::Observation { event, .. } = event else {
        panic!("expected an observation event");
    };
    assert_eq!(
        serde_json::to_value(event.data).unwrap(),
        json!({
            "arbitrary.key/with spaces": {
                "nested key": [null, true, false, "text", 42.5, {"empty": {}}],
            },
            "exact": 9_007_199_254_740_991.0,
            "rounded": 9_007_199_254_740_992.0,
            "session_id": "opaque-value",
        })
    );
}

#[test]
fn rejects_exactly_empty_session_ids_for_every_variant() {
    for mut value in [
        session_start(UTC_TIMESTAMP),
        observation(UTC_TIMESTAMP, "hook", json!({})),
        session_end(UTC_TIMESTAMP),
    ] {
        value["session_id"] = json!("");
        assert!(!reject(value).is_empty());
    }
}

#[test]
fn rejects_missing_required_fields_for_every_variant() {
    for field in [
        "event_type",
        "session_id",
        "project_name",
        "timestamp",
        "current_working_directory",
    ] {
        assert!(!reject(without_field(session_start(UTC_TIMESTAMP), field)).is_empty());
        assert!(!reject(without_field(session_end(UTC_TIMESTAMP), field)).is_empty());
    }

    for field in [
        "event_type",
        "hook_type",
        "project_name",
        "current_working_directory",
        "timestamp",
        "session_id",
        "data",
    ] {
        assert!(
            !reject(without_field(
                observation(UTC_TIMESTAMP, "hook", json!({})),
                field
            ))
            .is_empty()
        );
    }
}

#[test]
fn rejects_unknown_fields_for_every_variant() {
    for value in [
        session_start(UTC_TIMESTAMP),
        observation(UTC_TIMESTAMP, "hook", json!({})),
        session_end(UTC_TIMESTAMP),
    ] {
        assert!(!reject(with_unknown_field(value)).is_empty());
    }

    const SENTINEL: &str = "opaque-secret-value";
    let mut observation = observation(UTC_TIMESTAMP, "hook", json!({"opaque": SENTINEL}));
    observation["unexpected"] = json!(true);
    assert!(!reject(observation).contains(SENTINEL));
}

#[test]
fn rejects_wrong_discriminators_and_non_object_payloads() {
    for discriminator in ["sessionStart", "post_tool_use", "unknown"] {
        let mut value = session_start(UTC_TIMESTAMP);
        value["event_type"] = json!(discriminator);
        assert!(!reject(value).is_empty());
    }

    for value in [json!(null), json!([]), json!(true), json!(42)] {
        assert!(!reject(value).is_empty());
    }

    const SENTINEL: &str = "opaque payload";
    assert!(!reject(json!(SENTINEL)).contains(SENTINEL));
}

#[test]
fn rejects_malformed_timestamps_without_exposing_payload_data() {
    const SENTINEL: &str = "opaque-secret-value";

    for timestamp in [
        "2026-09-18T12:34:56Z",
        "2026-09-18T12:34:56.78Z",
        "2026-09-18T12:34:56.7890Z",
        "2026-09-18T12:34:56.789",
        "2026-02-29T12:34:56.789Z",
        "2026-13-18T12:34:56.789Z",
        "2026-09-31T12:34:56.789Z",
        "2026-09-18T12:34:56.789+24:00",
        "2026-09-18T12:34:56.789-24:00",
        "2026-09-18T12:34:56.789+00:60",
        "2026-09-18T12:34:56.789+0100",
    ] {
        let error = reject(observation(timestamp, "hook", json!({"opaque": SENTINEL})));
        assert!(!error.contains(SENTINEL));
    }
}

#[test]
fn rejects_non_object_observation_data_without_exposing_it() {
    const SENTINEL: &str = "opaque-secret-value";

    for data in [json!(null), json!([]), json!(true), json!(SENTINEL)] {
        let error = reject(observation(UTC_TIMESTAMP, "hook", data));
        assert!(error.contains("data"));
        assert!(!error.contains(SENTINEL));
    }
}
