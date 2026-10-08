use harness_events_cli::contracts::{ObservationRequest, SessionEndRequest, SessionStartRequest};
use pbjson_types::Struct;
use serde_json::json;

#[test]
fn generated_requests_compile_and_serialize_with_proto_field_names() {
    assert_eq!(
        serde_json::to_value(SessionStartRequest {
            session_id: "session-1".to_owned(),
            project_name: "project".to_owned(),
            timestamp: "2026-09-19T00:00:00.000Z".to_owned(),
            current_working_directory: "/workspace".to_owned(),
        })
        .unwrap(),
        json!({
            "session_id": "session-1",
            "project_name": "project",
            "timestamp": "2026-09-19T00:00:00.000Z",
            "current_working_directory": "/workspace",
        })
    );

    assert_eq!(
        serde_json::to_value(ObservationRequest {
            hook_type: "after_tool".to_owned(),
            project_name: "project".to_owned(),
            current_working_directory: "/workspace".to_owned(),
            timestamp: "2026-09-19T00:00:00.000Z".to_owned(),
            session_id: "session-1".to_owned(),
            data: Some(Struct::default()),
        })
        .unwrap(),
        json!({
            "hook_type": "after_tool",
            "project_name": "project",
            "current_working_directory": "/workspace",
            "timestamp": "2026-09-19T00:00:00.000Z",
            "session_id": "session-1",
            "data": {},
        })
    );

    assert_eq!(
        serde_json::to_value(SessionEndRequest {
            session_id: "session-1".to_owned(),
            project_name: "project".to_owned(),
            timestamp: "2026-09-19T00:00:00.000Z".to_owned(),
            current_working_directory: "/workspace".to_owned(),
        })
        .unwrap(),
        json!({
            "session_id": "session-1",
            "project_name": "project",
            "timestamp": "2026-09-19T00:00:00.000Z",
            "current_working_directory": "/workspace",
        })
    );
}
