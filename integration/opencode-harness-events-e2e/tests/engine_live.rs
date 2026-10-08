use iii_sdk::protocol::TriggerRequest;
use opencode_harness_events_e2e::{ArtifactPaths, CAPTURE_FUNCTION_IDS, EngineHarness};
use serde_json::json;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires all explicit iii, OpenCode v1 and v2, CLI, and plugin artifacts"]
async fn real_iii_capture_boundary_registers_routes_and_cleans_up() {
    let paths = ArtifactPaths::from_env().expect("live artifact paths must be explicit");
    let session = EngineHarness::new(&paths)
        .start()
        .await
        .expect("real iii capture boundary starts");
    assert!(
        session.ready(),
        "SDK registration and catalog readiness completed"
    );

    let payloads = [
        json!({"session_id": "live-session", "project_name": "live-project"}),
        json!({"session_id": "live-session", "hook_type": "PostToolUse", "data": {"value": 7}}),
        json!({"session_id": "live-session", "project_name": "live-project"}),
    ];
    for (function_id, payload) in CAPTURE_FUNCTION_IDS.iter().zip(payloads.iter()) {
        let response = session
            .client()
            .trigger(TriggerRequest {
                function_id: (*function_id).to_owned(),
                payload: payload.clone(),
                action: None,
                timeout_ms: Some(5_000),
            })
            .await
            .expect("capture function invocation succeeds");
        assert_eq!(response, json!({"dispatched": true}));
    }

    let capture = session.capture();
    capture
        .assert_ordered_function_ids(&CAPTURE_FUNCTION_IDS)
        .expect("capture function IDs and receive order match");
    capture
        .assert_namespace(session.identity().namespace.as_str())
        .expect("all captures use the registered namespace");
    for (function_id, payload) in CAPTURE_FUNCTION_IDS.iter().zip(payloads.iter()) {
        capture
            .assert_payload_contains(function_id, payload)
            .expect("capture payload matches the invocation payload");
    }

    session
        .shutdown()
        .await
        .expect("iii client and owned process group shut down");
}
