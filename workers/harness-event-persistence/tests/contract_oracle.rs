use harness_event_persistence::contracts::{ContractError, PersistableEvent, adapt_queued_event};
use harness_ingestion::contracts::{
    HarnessTimestamp as IngestionHarnessTimestamp, ObservationEvent as IngestionObservationEvent,
    ObservationInput as IngestionObservationInput, SessionEndEvent as IngestionSessionEndEvent,
    SessionStartEvent as IngestionSessionStartEvent,
};
use serde::Serialize;
use serde_json::{Value, json};

const UTC_TIMESTAMP: &str = "2026-09-18T12:34:56.789Z";
const LOWERCASE_UTC_TIMESTAMP: &str = "2026-09-18t12:34:56.789z";
const OFFSET_TIMESTAMP: &str = "2026-09-18T12:34:56.789+05:30";
const UTC_EPOCH_MILLIS: i64 = 1_789_734_896_789;
const OFFSET_EPOCH_MILLIS: i64 = 1_789_715_096_789;

fn protojson<T: Serialize>(event: T) -> Value {
    serde_json::to_value(event).expect("public ingestion event should serialize as ProtoJSON")
}

fn oracle_session_start(session_id: &str) -> Value {
    protojson(IngestionSessionStartEvent {
        event_type: "session_start".to_owned(),
        session_id: session_id.to_owned(),
        project_name: "project".to_owned(),
        timestamp: LOWERCASE_UTC_TIMESTAMP.to_owned(),
        current_working_directory: "/work".to_owned(),
    })
}

fn oracle_observation(session_id: &str) -> Value {
    let request = IngestionObservationInput {
        hook_type: "post_tool_use".to_owned(),
        project_name: "project".to_owned(),
        current_working_directory: "/work".to_owned(),
        timestamp: IngestionHarnessTimestamp::try_from(OFFSET_TIMESTAMP.to_owned())
            .expect("fixture timestamp should be valid"),
        session_id: session_id.to_owned(),
        data: serde_json::from_value(json!({
            "arbitrary.key/with spaces": {
                "nested key": [null, true, false, "text", 42.5, {"empty": {}}],
            },
            "exact": 9_007_199_254_740_991u64,
            "rounded": 9_007_199_254_740_993u64,
            "session_id": "opaque-value",
        }))
        .expect("observation fixture should be a JSON object"),
    }
    .into_request()
    .expect("observation fixture should convert through the ingestion contract");

    protojson(IngestionObservationEvent {
        event_type: "observation".to_owned(),
        hook_type: request.hook_type,
        project_name: request.project_name,
        current_working_directory: request.current_working_directory,
        timestamp: request.timestamp,
        session_id: request.session_id,
        data: request.data,
    })
}

fn oracle_session_end(session_id: &str) -> Value {
    protojson(IngestionSessionEndEvent {
        event_type: "session_end".to_owned(),
        session_id: session_id.to_owned(),
        project_name: "project".to_owned(),
        timestamp: UTC_TIMESTAMP.to_owned(),
        current_working_directory: "/work".to_owned(),
    })
}

fn rejected(value: Value) -> ContractError {
    adapt_queued_event(value).expect_err("invalid oracle ProtoJSON should be rejected")
}

#[test]
fn public_ingestion_protojson_adapts_all_variants_without_translation() {
    let session_start_protojson = oracle_session_start("session-1");
    let PersistableEvent::SessionStart {
        event: session_start,
        source_timestamp_epoch_millis,
    } = adapt_queued_event(session_start_protojson.clone())
        .expect("ingestion session-start ProtoJSON should adapt")
    else {
        panic!("expected a session-start event");
    };
    assert_eq!(
        serde_json::to_value(&session_start).expect("persistence event should serialize"),
        session_start_protojson
    );
    assert_eq!(source_timestamp_epoch_millis, UTC_EPOCH_MILLIS);

    let observation_protojson = oracle_observation("session-1");
    assert_eq!(
        observation_protojson["data"]["rounded"].as_f64(),
        Some(9_007_199_254_740_992.0)
    );
    let PersistableEvent::Observation {
        event: observation,
        source_timestamp_epoch_millis,
    } = adapt_queued_event(observation_protojson.clone())
        .expect("ingestion observation ProtoJSON should adapt")
    else {
        panic!("expected an observation event");
    };
    assert_eq!(observation.event_type, "observation");
    assert_eq!(observation.hook_type, "post_tool_use");
    assert_eq!(
        serde_json::to_value(&observation).expect("persistence event should serialize"),
        observation_protojson
    );
    assert_eq!(source_timestamp_epoch_millis, OFFSET_EPOCH_MILLIS);

    let session_end_protojson = oracle_session_end("session-1");
    let PersistableEvent::SessionEnd {
        event: session_end,
        source_timestamp_epoch_millis,
    } = adapt_queued_event(session_end_protojson.clone())
        .expect("ingestion session-end ProtoJSON should adapt")
    else {
        panic!("expected a session-end event");
    };
    assert_eq!(
        serde_json::to_value(&session_end).expect("persistence event should serialize"),
        session_end_protojson
    );
    assert_eq!(source_timestamp_epoch_millis, UTC_EPOCH_MILLIS);
}

#[test]
fn public_ingestion_protojson_keeps_strict_validation_edges() {
    for protojson in [
        oracle_session_start(" \t "),
        oracle_observation(" \t "),
        oracle_session_end(" \t "),
    ] {
        assert!(
            adapt_queued_event(protojson).is_ok(),
            "whitespace-only session IDs should retain ingestion parity"
        );
    }

    for mut protojson in [
        oracle_session_start("session-1"),
        oracle_observation("session-1"),
        oracle_session_end("session-1"),
    ] {
        protojson["session_id"] = json!("");
        assert!(matches!(
            rejected(protojson),
            ContractError::EmptySessionId { .. }
        ));
    }

    for mut protojson in [
        oracle_session_start("session-1"),
        oracle_observation("session-1"),
        oracle_session_end("session-1"),
    ] {
        protojson["unexpected"] = json!(true);
        assert!(matches!(
            rejected(protojson),
            ContractError::InvalidFields { .. }
        ));
    }

    let mut missing_start_field = oracle_session_start("session-1");
    missing_start_field
        .as_object_mut()
        .expect("oracle ProtoJSON should be an object")
        .remove("current_working_directory");
    let mut missing_observation_field = oracle_observation("session-1");
    missing_observation_field
        .as_object_mut()
        .expect("oracle ProtoJSON should be an object")
        .remove("data");
    let mut missing_end_field = oracle_session_end("session-1");
    missing_end_field
        .as_object_mut()
        .expect("oracle ProtoJSON should be an object")
        .remove("project_name");
    for protojson in [
        missing_start_field,
        missing_observation_field,
        missing_end_field,
    ] {
        assert!(matches!(
            rejected(protojson),
            ContractError::InvalidFields { .. }
        ));
    }

    for mut protojson in [
        oracle_session_start("session-1"),
        oracle_observation("session-1"),
        oracle_session_end("session-1"),
    ] {
        protojson["event_type"] = json!("not_an_event");
        assert!(matches!(
            rejected(protojson),
            ContractError::InvalidDiscriminator
        ));
    }

    for mut protojson in [
        oracle_session_start("session-1"),
        oracle_observation("session-1"),
        oracle_session_end("session-1"),
    ] {
        protojson["timestamp"] = json!("2026-09-18T12:34:56Z");
        assert!(matches!(
            rejected(protojson),
            ContractError::InvalidTimestamp { .. }
        ));
    }

    let mut non_object_observation_data = oracle_observation("session-1");
    non_object_observation_data["data"] = json!(["not", "an", "object"]);
    assert!(matches!(
        rejected(non_object_observation_data),
        ContractError::InvalidObservationData
    ));
}
