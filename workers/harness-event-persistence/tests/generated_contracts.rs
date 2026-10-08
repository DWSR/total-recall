use harness_event_persistence::contracts::{ObservationEvent, SessionEndEvent, SessionStartEvent};
use pbjson_types::Struct;
use serde_json::json;

#[test]
fn generated_queued_event_messages_serialize_as_protojson() {
    assert_eq!(
        serde_json::to_value(SessionStartEvent {
            event_type: "session_start".to_owned(),
            session_id: String::new(),
            project_name: String::new(),
            timestamp: String::new(),
            current_working_directory: String::new(),
        })
        .unwrap(),
        json!({
            "event_type": "session_start",
            "session_id": "",
            "project_name": "",
            "timestamp": "",
            "current_working_directory": "",
        })
    );

    assert_eq!(
        serde_json::to_value(ObservationEvent {
            event_type: "observation".to_owned(),
            hook_type: String::new(),
            project_name: String::new(),
            current_working_directory: String::new(),
            timestamp: String::new(),
            session_id: String::new(),
            data: Some(Struct::default()),
        })
        .unwrap(),
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

    assert_eq!(
        serde_json::to_value(SessionEndEvent {
            event_type: "session_end".to_owned(),
            session_id: String::new(),
            project_name: String::new(),
            timestamp: String::new(),
            current_working_directory: String::new(),
        })
        .unwrap(),
        json!({
            "event_type": "session_end",
            "session_id": "",
            "project_name": "",
            "timestamp": "",
            "current_working_directory": "",
        })
    );
}
