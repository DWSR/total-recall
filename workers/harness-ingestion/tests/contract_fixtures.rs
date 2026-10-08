use harness_ingestion::contracts::{ObservationEvent, SessionEndEvent, SessionStartEvent};
use pbjson_types::Struct;
use serde_json::json;

#[test]
fn session_start_event_serializes_the_complete_snake_case_shape() {
    let event = SessionStartEvent {
        event_type: "session_start".to_owned(),
        session_id: String::new(),
        project_name: String::new(),
        timestamp: String::new(),
        current_working_directory: String::new(),
    };

    assert_eq!(
        serde_json::to_value(event).unwrap(),
        json!({
            "event_type": "session_start",
            "session_id": "",
            "project_name": "",
            "timestamp": "",
            "current_working_directory": "",
        })
    );
}

#[test]
fn session_end_event_serializes_the_complete_snake_case_shape() {
    let event = SessionEndEvent {
        event_type: "session_end".to_owned(),
        session_id: String::new(),
        project_name: String::new(),
        timestamp: String::new(),
        current_working_directory: String::new(),
    };

    assert_eq!(
        serde_json::to_value(event).unwrap(),
        json!({
            "event_type": "session_end",
            "session_id": "",
            "project_name": "",
            "timestamp": "",
            "current_working_directory": "",
        })
    );
}

#[test]
fn observation_event_serializes_defaults_and_an_empty_data_object() {
    let event = ObservationEvent {
        event_type: "observation".to_owned(),
        hook_type: String::new(),
        project_name: String::new(),
        current_working_directory: String::new(),
        timestamp: String::new(),
        session_id: String::new(),
        data: Some(Struct::default()),
    };

    assert_eq!(
        serde_json::to_value(event).unwrap(),
        json!({
            "event_type": "observation",
            "hook_type": "",
            "project_name": "",
            "current_working_directory": "",
            "timestamp": "",
            "session_id": "",
            "data": {},
        })
    );
}
