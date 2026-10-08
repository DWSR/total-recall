#[allow(dead_code)]
mod support;

use std::{
    io::{self, Cursor, Read},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use harness_events_cli::{
    CommandRunError,
    app::{InvokeError, SubmitError},
    cli::{EventCommand, LifecycleArgs, ObservationArgs},
    config::ClientConfig,
    contracts::{IngestionFunction, InputError, Submission},
    run_command, sdk,
};
use serde_json::{Value, json};

use support::engine::{FakeEngine, FakeEngineConfig, InvocationOutcome};

const NAMESPACE: &str = "orchestration-namespace";

struct PanicReader;

impl Read for PanicReader {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        panic!("lifecycle commands must not read standard input")
    }
}

struct FailingReader;

impl Read for FailingReader {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other("observation data must remain local"))
    }
}

fn session_start() -> EventCommand {
    EventCommand::SessionStart(LifecycleArgs {
        session_id: "start-session".to_owned(),
        project_name: "start-project".to_owned(),
        current_working_directory: "/workspace/start".to_owned(),
        timestamp: "start-time".to_owned(),
    })
}

fn observation() -> EventCommand {
    EventCommand::Observation(ObservationArgs {
        hook_type: "PostToolUse".to_owned(),
        project_name: "observation-project".to_owned(),
        current_working_directory: "/workspace/observation".to_owned(),
        timestamp: "observation-time".to_owned(),
        session_id: "observation-session".to_owned(),
    })
}

fn session_end() -> EventCommand {
    EventCommand::SessionEnd(LifecycleArgs {
        session_id: "end-session".to_owned(),
        project_name: "end-project".to_owned(),
        current_working_directory: "/workspace/end".to_owned(),
        timestamp: "end-time".to_owned(),
    })
}

#[tokio::test]
async fn prepares_before_creating_the_single_injected_session() {
    let config = ClientConfig {
        engine_url: "ws://recording-engine.test".to_owned(),
        namespace: Some(NAMESPACE.to_owned()),
    };
    let expected_submission = Submission {
        function_id: IngestionFunction::SessionStart,
        payload: json!({
            "session_id": "start-session",
            "project_name": "start-project",
            "timestamp": "start-time",
            "current_working_directory": "/workspace/start",
        }),
    };
    let invocation_count = Arc::new(AtomicUsize::new(0));
    let recorded = Arc::new(Mutex::new(None));

    let result = run_command(session_start(), PanicReader, config.clone(), {
        let invocation_count = Arc::clone(&invocation_count);
        let recorded = Arc::clone(&recorded);
        move |received_config, submission| {
            invocation_count.fetch_add(1, Ordering::SeqCst);
            *recorded.lock().expect("recording lock is available") =
                Some((received_config, submission));
            async { Ok::<_, InvokeError>(json!({"dispatched": true})) }
        }
    })
    .await;

    assert!(result.is_ok(), "the exact success response must succeed");
    assert_eq!(invocation_count.load(Ordering::SeqCst), 1);
    let (received_config, received_submission) = recorded
        .lock()
        .expect("recording lock is available")
        .take()
        .expect("the invocation must receive config and submission");
    assert_eq!(received_config, config);
    assert_eq!(received_submission, expected_submission);

    let session_creations = Arc::new(AtomicUsize::new(0));
    let input_failure = run_command(
        observation(),
        FailingReader,
        ClientConfig {
            engine_url: "ws://must-not-connect.test".to_owned(),
            namespace: Some(NAMESPACE.to_owned()),
        },
        {
            let session_creations = Arc::clone(&session_creations);
            move |_, _| {
                session_creations.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, InvokeError>(json!({"dispatched": true})) }
            }
        },
    )
    .await;

    assert!(matches!(
        input_failure,
        Err(CommandRunError::Input(InputError::ObservationInputRead))
    ));
    assert_eq!(
        session_creations.load(Ordering::SeqCst),
        0,
        "preparation failures must not create an SDK session"
    );
}

async fn assert_real_sdk_success(
    command: EventCommand,
    stdin: impl Read,
    expected_function_id: &str,
    expected_payload: Value,
) {
    let mut engine = FakeEngine::start(FakeEngineConfig::default()).await;
    let result = run_command(
        command,
        stdin,
        ClientConfig {
            engine_url: engine.url().to_owned(),
            namespace: Some(NAMESPACE.to_owned()),
        },
        sdk::invoke,
    )
    .await;

    assert!(result.is_ok(), "the exact success response must succeed");
    engine.finish().await.expect("fake engine completes");
    let captured = engine.captured().await;
    assert!(
        captured.identity.is_some(),
        "the SDK session must register before invoking"
    );
    assert_eq!(
        captured.registration_namespace.as_deref(),
        Some(NAMESPACE),
        "the configured namespace must reach registration"
    );
    assert_eq!(
        captured.invocation_namespace.as_deref(),
        Some(NAMESPACE),
        "the configured namespace must reach invocation"
    );
    assert_eq!(captured.function_id.as_deref(), Some(expected_function_id));
    assert_eq!(captured.payload, Some(expected_payload));
    assert_eq!(captured.invocation_count, 1);
    assert!(
        captured.shutdown_observed,
        "the SDK must finish its joining shutdown before the runner returns"
    );
}

#[tokio::test]
async fn runs_all_commands_through_real_sdk_invocation_and_shutdown() {
    assert_real_sdk_success(
        session_start(),
        PanicReader,
        "harness::session_start",
        json!({
            "session_id": "start-session",
            "project_name": "start-project",
            "timestamp": "start-time",
            "current_working_directory": "/workspace/start",
        }),
    )
    .await;
    assert_real_sdk_success(
        observation(),
        Cursor::new(
            br#"{
                "opaque outer": {
                    "opaque/nested": "opaque-value",
                    "states": [null, true, "still-opaque"]
                }
            }"#
            .to_vec(),
        ),
        "harness::observation",
        json!({
            "hook_type": "PostToolUse",
            "project_name": "observation-project",
            "current_working_directory": "/workspace/observation",
            "timestamp": "observation-time",
            "session_id": "observation-session",
            "data": {
                "opaque outer": {
                    "opaque/nested": "opaque-value",
                    "states": [null, true, "still-opaque"]
                }
            }
        }),
    )
    .await;
    assert_real_sdk_success(
        session_end(),
        PanicReader,
        "harness::session_end",
        json!({
            "session_id": "end-session",
            "project_name": "end-project",
            "timestamp": "end-time",
            "current_working_directory": "/workspace/end",
        }),
    )
    .await;
}

#[tokio::test]
async fn rejects_an_application_invalid_sdk_response_through_the_runner() {
    let mut engine = FakeEngine::start(FakeEngineConfig {
        invocation: InvocationOutcome::ApplicationInvalid,
        ..FakeEngineConfig::default()
    })
    .await;
    let result = run_command(
        session_start(),
        Cursor::new(Vec::new()),
        ClientConfig {
            engine_url: engine.url().to_owned(),
            namespace: Some(NAMESPACE.to_owned()),
        },
        sdk::invoke,
    )
    .await;

    assert!(matches!(
        result,
        Err(CommandRunError::Submit(SubmitError::UnexpectedResponse))
    ));
    engine.finish().await.expect("fake engine completes");
    let captured = engine.captured().await;
    assert_eq!(captured.invocation_count, 1);
    assert!(captured.shutdown_observed);
}
