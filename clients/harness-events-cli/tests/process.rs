#[allow(dead_code)]
mod support;

use std::time::Duration;

use serde_json::{Value, json};
use support::{
    engine::{
        CapturedInvocation, FakeEngine, FakeEngineConfig, InvocationOutcome, RegistrationOutcome,
    },
    process::{ProcessOutput, ProcessScenario, StdinMode, run},
};
use tokio::time::timeout;

const NAMESPACE: &str = "process-smoke-namespace";
const INVALID_OBSERVATION_DIAGNOSTIC: &[u8] = b"harness-events: invalid observation data\n";
const ENGINE_CONNECTION_DIAGNOSTIC: &[u8] = b"harness-events: engine connection failed\n";
const INVOCATION_DIAGNOSTIC: &[u8] = b"harness-events: function invocation failed\n";
const FAILURE_OBSERVATION: &[u8] = br#"{
    "opaque.key/with spaces": {
        "nested key": [null, true, "task-4-5-opaque-observation-sentinel", {"inner/key": false}],
        "payload marker": "task-4-5-serialized-payload-sentinel",
        "source marker": "task-4-5-source-chain-sentinel",
        "stack marker": "task-4-5-stack-sentinel"
    }
}"#;
const SERIALIZED_PAYLOAD_FRAGMENT: &str = "opaque.key/with spaces";
const SOURCE_CHAIN_FRAGMENT: &str = "Caused by:";
const REMOTE_MESSAGE: &str = "fake engine rejected the invocation";
const STACK_FRAGMENT: &str = "stack backtrace";
const SDK_FAILURE_PROCESS_DEADLINE: Duration = Duration::from_secs(5);

fn scenario(engine_url: &str, args: Vec<String>, stdin: StdinMode) -> ProcessScenario {
    ProcessScenario {
        args,
        stdin,
        environment: vec![
            ("III_URL".to_owned(), engine_url.to_owned()),
            ("III_NAMESPACE".to_owned(), NAMESPACE.to_owned()),
        ],
        ..ProcessScenario::default()
    }
}

fn observation_args() -> Vec<String> {
    vec![
        "observation".to_owned(),
        "--hook-type".to_owned(),
        "process-local-observation".to_owned(),
        "--project-name".to_owned(),
        "process-local-project".to_owned(),
        "--current-working-directory".to_owned(),
        "/workspace/process-local".to_owned(),
        "--timestamp".to_owned(),
        "2026-09-19T20:17:33.123Z".to_owned(),
        "--session-id".to_owned(),
        "process-local-session".to_owned(),
    ]
}

fn failure_scenario(engine_url: &str) -> ProcessScenario {
    ProcessScenario {
        timeout: SDK_FAILURE_PROCESS_DEADLINE,
        ..scenario(
            engine_url,
            observation_args(),
            StdinMode::Closed(FAILURE_OBSERVATION.to_vec()),
        )
    }
}

fn failure_observation_payload() -> Value {
    json!({
        "hook_type": "process-local-observation",
        "project_name": "process-local-project",
        "current_working_directory": "/workspace/process-local",
        "timestamp": "2026-09-19T20:17:33.123Z",
        "session_id": "process-local-session",
        "data": {
            "opaque.key/with spaces": {
                "nested key": [
                    null,
                    true,
                    "task-4-5-opaque-observation-sentinel",
                    {"inner/key": false}
                ],
                "payload marker": "task-4-5-serialized-payload-sentinel",
                "source marker": "task-4-5-source-chain-sentinel",
                "stack marker": "task-4-5-stack-sentinel"
            }
        }
    })
}

fn lifecycle_args(command: &str) -> Vec<String> {
    vec![
        command.to_owned(),
        "--session-id".to_owned(),
        "process-open-stdin-session".to_owned(),
        "--project-name".to_owned(),
        "process-open-stdin-project".to_owned(),
        "--current-working-directory".to_owned(),
        "/workspace/process-open-stdin".to_owned(),
        "--timestamp".to_owned(),
        "2026-09-19T20:18:33.123Z".to_owned(),
    ]
}

fn assert_no_fake_connection(output: &ProcessOutput) {
    assert_eq!(output.connection_count, 0);
    assert_eq!(&output.captured, &CapturedInvocation::default());
}

fn assert_help(output: &ProcessOutput, expected_fragments: &[&str]) {
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(output.stderr, b"");
    assert_no_fake_connection(output);

    let stdout = String::from_utf8_lossy(&output.stdout);
    for fragment in expected_fragments {
        assert!(
            stdout.contains(fragment),
            "help output must contain {fragment:?}, got {stdout}"
        );
    }
}

fn assert_usage_failure(output: &ProcessOutput, description: &str) {
    assert_eq!(output.status.code(), Some(2), "{description}");
    assert_eq!(output.stdout, b"", "{description} must not write stdout");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Usage: harness-events"),
        "{description} must render Clap usage text, got {stderr}"
    );
    assert_no_fake_connection(output);
}

fn assert_local_observation_failure(output: &ProcessOutput, description: &str) {
    assert_eq!(output.status.code(), Some(2), "{description}");
    assert_eq!(output.stdout, b"", "{description} must not write stdout");
    assert_eq!(
        output.stderr, INVALID_OBSERVATION_DIAGNOSTIC,
        "{description}"
    );
    assert_no_fake_connection(output);
}

fn assert_process_failure(
    output: &ProcessOutput,
    status: i32,
    diagnostic: &[u8],
    description: &str,
) {
    assert_eq!(output.status.code(), Some(status), "{description}");
    assert_eq!(output.stdout, b"", "{description} must not write stdout");
    assert_eq!(output.stderr, diagnostic, "{description} diagnostic");
}

fn assert_private_failure_output(output: &ProcessOutput, description: &str) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    for forbidden in [
        "task-4-5-opaque-observation-sentinel",
        "task-4-5-serialized-payload-sentinel",
        SERIALIZED_PAYLOAD_FRAGMENT,
        SOURCE_CHAIN_FRAGMENT,
        "task-4-5-source-chain-sentinel",
        REMOTE_MESSAGE,
        STACK_FRAGMENT,
        "task-4-5-stack-sentinel",
    ] {
        assert!(
            !stdout.contains(forbidden),
            "{description} stdout must not contain {forbidden:?}: {stdout}"
        );
        assert!(
            !stderr.contains(forbidden),
            "{description} stderr must not contain {forbidden:?}: {stderr}"
        );
    }
}

fn assert_success(output: support::process::ProcessOutput, function_id: &str, payload: Value) {
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(output.stdout, b"");
    assert_eq!(output.stderr, b"");
    assert!(output.captured.identity.is_some());
    assert_eq!(
        output.captured.registration_namespace.as_deref(),
        Some(NAMESPACE)
    );
    assert_eq!(
        output.captured.invocation_namespace.as_deref(),
        Some(NAMESPACE)
    );
    assert_eq!(output.captured.function_id.as_deref(), Some(function_id));
    assert_eq!(output.captured.payload, Some(payload));
    assert_eq!(output.captured.invocation_count, 1);
    assert!(output.captured.shutdown_observed);
}

#[tokio::test(flavor = "current_thread")]
async fn session_start_completes_against_the_fake_without_deadlocking() {
    let mut engine = FakeEngine::start(FakeEngineConfig::default()).await;
    let engine_url = engine.url().to_owned();
    let output = run(
        &mut engine,
        scenario(
            &engine_url,
            vec![
                "session-start".to_owned(),
                "--session-id".to_owned(),
                "process-smoke-session".to_owned(),
                "--project-name".to_owned(),
                "process-smoke-project".to_owned(),
                "--current-working-directory".to_owned(),
                "/workspace/process-smoke".to_owned(),
                "--timestamp".to_owned(),
                "2026-09-19T20:14:33.123Z".to_owned(),
            ],
            StdinMode::Closed(Vec::new()),
        ),
    )
    .await
    .expect("session-start child completes against the in-process fake");

    assert_success(
        output,
        "harness::session_start",
        json!({
            "session_id": "process-smoke-session",
            "project_name": "process-smoke-project",
            "timestamp": "2026-09-19T20:14:33.123Z",
            "current_working_directory": "/workspace/process-smoke",
        }),
    );
}

#[tokio::test(flavor = "current_thread")]
async fn root_help_returns_zero_without_connecting_to_the_fake() {
    let mut engine = FakeEngine::start(FakeEngineConfig::default()).await;
    let engine_url = engine.url().to_owned();
    let output = run(
        &mut engine,
        scenario(
            &engine_url,
            vec!["--help".to_owned()],
            StdinMode::Open(Vec::new()),
        ),
    )
    .await
    .expect("root help child completes without an engine connection");

    assert_help(
        &output,
        &[
            "Usage: harness-events <COMMAND>",
            "session-start",
            "observation",
            "session-end",
        ],
    );
}

#[tokio::test(flavor = "current_thread")]
async fn subcommand_help_returns_zero_without_connecting_to_the_fake() {
    let lifecycle_fragments = [
        "--session-id <SESSION_ID>",
        "--project-name <PROJECT_NAME>",
        "--current-working-directory <CURRENT_WORKING_DIRECTORY>",
        "--timestamp <TIMESTAMP>",
    ];
    let cases = vec![
        (
            vec!["session-start".to_owned(), "--help".to_owned()],
            vec![
                "Usage: harness-events session-start",
                lifecycle_fragments[0],
                lifecycle_fragments[1],
                lifecycle_fragments[2],
                lifecycle_fragments[3],
            ],
        ),
        (
            vec!["observation".to_owned(), "--help".to_owned()],
            vec![
                "Usage: harness-events observation",
                "--hook-type <HOOK_TYPE>",
                "--project-name <PROJECT_NAME>",
                "--current-working-directory <CURRENT_WORKING_DIRECTORY>",
                "--timestamp <TIMESTAMP>",
                "--session-id <SESSION_ID>",
            ],
        ),
        (
            vec!["session-end".to_owned(), "--help".to_owned()],
            vec![
                "Usage: harness-events session-end",
                lifecycle_fragments[0],
                lifecycle_fragments[1],
                lifecycle_fragments[2],
                lifecycle_fragments[3],
            ],
        ),
    ];

    for (args, expected_fragments) in cases {
        let mut engine = FakeEngine::start(FakeEngineConfig::default()).await;
        let engine_url = engine.url().to_owned();
        let output = run(
            &mut engine,
            scenario(&engine_url, args, StdinMode::Open(Vec::new())),
        )
        .await
        .expect("subcommand help child completes without an engine connection");

        assert_help(&output, &expected_fragments);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn clap_usage_failures_return_two_without_connecting_to_the_fake() {
    let cases = [
        ("missing command", Vec::new()),
        ("unknown command", vec!["not-a-command".to_owned()]),
        (
            "missing required option",
            vec![
                "session-start".to_owned(),
                "--session-id".to_owned(),
                "process-missing-option-session".to_owned(),
            ],
        ),
        (
            "unknown option",
            vec![
                "session-start".to_owned(),
                "--session-id".to_owned(),
                "process-unknown-option-session".to_owned(),
                "--project-name".to_owned(),
                "process-unknown-option-project".to_owned(),
                "--current-working-directory".to_owned(),
                "/workspace/process-unknown-option".to_owned(),
                "--timestamp".to_owned(),
                "2026-09-19T20:19:33.123Z".to_owned(),
                "--not-a-real-option".to_owned(),
            ],
        ),
    ];

    for (description, args) in cases {
        let mut engine = FakeEngine::start(FakeEngineConfig::default()).await;
        let engine_url = engine.url().to_owned();
        let output = run(
            &mut engine,
            scenario(&engine_url, args, StdinMode::Open(Vec::new())),
        )
        .await
        .unwrap_or_else(|error| panic!("{description} child completes: {error}"));

        assert_usage_failure(&output, description);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_observation_input_returns_two_before_connecting_to_the_fake() {
    let cases = [
        ("empty observation input", Vec::new()),
        ("malformed observation input", b"{\"value\":".to_vec()),
        ("non-object observation input", b"null".to_vec()),
        (
            "trailing observation input",
            br#"{} trailing content"#.to_vec(),
        ),
    ];

    for (description, input) in cases {
        let mut engine = FakeEngine::start(FakeEngineConfig::default()).await;
        let engine_url = engine.url().to_owned();
        let output = run(
            &mut engine,
            scenario(&engine_url, observation_args(), StdinMode::Closed(input)),
        )
        .await
        .unwrap_or_else(|error| panic!("{description} child completes: {error}"));

        assert_local_observation_failure(&output, description);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn session_start_never_reads_held_open_stdin() {
    let mut engine = FakeEngine::start(FakeEngineConfig::default()).await;
    let engine_url = engine.url().to_owned();
    let output = run(
        &mut engine,
        ProcessScenario {
            stdin: StdinMode::Open(Vec::new()),
            ..scenario(
                &engine_url,
                lifecycle_args("session-start"),
                StdinMode::Closed(Vec::new()),
            )
        },
    )
    .await
    .expect("session-start must complete while stdin remains open");

    assert_success(
        output,
        "harness::session_start",
        json!({
            "session_id": "process-open-stdin-session",
            "project_name": "process-open-stdin-project",
            "timestamp": "2026-09-19T20:18:33.123Z",
            "current_working_directory": "/workspace/process-open-stdin",
        }),
    );
}

#[tokio::test(flavor = "current_thread")]
async fn session_end_never_reads_held_open_stdin() {
    let mut engine = FakeEngine::start(FakeEngineConfig::default()).await;
    let engine_url = engine.url().to_owned();
    let output = run(
        &mut engine,
        ProcessScenario {
            stdin: StdinMode::Open(Vec::new()),
            ..scenario(
                &engine_url,
                lifecycle_args("session-end"),
                StdinMode::Closed(Vec::new()),
            )
        },
    )
    .await
    .expect("session-end must complete while stdin remains open");

    assert_success(
        output,
        "harness::session_end",
        json!({
            "session_id": "process-open-stdin-session",
            "project_name": "process-open-stdin-project",
            "timestamp": "2026-09-19T20:18:33.123Z",
            "current_working_directory": "/workspace/process-open-stdin",
        }),
    );
}

#[tokio::test(flavor = "current_thread")]
async fn observation_completes_with_opaque_nested_data_and_proto_double_rounding() {
    let mut engine = FakeEngine::start(FakeEngineConfig::default()).await;
    let engine_url = engine.url().to_owned();
    let output = run(
        &mut engine,
        scenario(
            &engine_url,
            vec![
                "observation".to_owned(),
                "--hook-type".to_owned(),
                "process-observation-hook".to_owned(),
                "--project-name".to_owned(),
                "process-observation-project".to_owned(),
                "--current-working-directory".to_owned(),
                "/workspace/process-observation".to_owned(),
                "--timestamp".to_owned(),
                "2026-09-19T20:15:33.123Z".to_owned(),
                "--session-id".to_owned(),
                "process-observation-session".to_owned(),
            ],
            StdinMode::Closed(
                br#"{
                    "opaque.key/with spaces": {
                        "nested key": [null, true, "opaque-value", {"inner/key": false}],
                        "large integer": 9007199254740993
                    }
                }"#
                .to_vec(),
            ),
        ),
    )
    .await
    .expect("observation child completes against the in-process fake");

    assert_success(
        output,
        "harness::observation",
        json!({
            "hook_type": "process-observation-hook",
            "project_name": "process-observation-project",
            "current_working_directory": "/workspace/process-observation",
            "timestamp": "2026-09-19T20:15:33.123Z",
            "session_id": "process-observation-session",
            "data": {
                "opaque.key/with spaces": {
                    "nested key": [null, true, "opaque-value", {"inner/key": false}],
                    "large integer": 9_007_199_254_740_992.0,
                }
            }
        }),
    );
}

#[tokio::test(flavor = "current_thread")]
async fn session_end_completes_against_the_fake_without_live_services() {
    let mut engine = FakeEngine::start(FakeEngineConfig::default()).await;
    let engine_url = engine.url().to_owned();
    let output = run(
        &mut engine,
        scenario(
            &engine_url,
            vec![
                "session-end".to_owned(),
                "--session-id".to_owned(),
                "process-end-session".to_owned(),
                "--project-name".to_owned(),
                "process-end-project".to_owned(),
                "--current-working-directory".to_owned(),
                "/workspace/process-end".to_owned(),
                "--timestamp".to_owned(),
                "2026-09-19T20:16:33.123Z".to_owned(),
            ],
            StdinMode::Closed(Vec::new()),
        ),
    )
    .await
    .expect("session-end child completes against the in-process fake");

    assert_success(
        output,
        "harness::session_end",
        json!({
            "session_id": "process-end-session",
            "project_name": "process-end-project",
            "timestamp": "2026-09-19T20:16:33.123Z",
            "current_working_directory": "/workspace/process-end",
        }),
    );
}

#[tokio::test(flavor = "current_thread")]
async fn connection_failure_returns_three_without_triggering() {
    let mut engine = FakeEngine::start(FakeEngineConfig {
        registration: RegistrationOutcome::ConnectionFailure,
        ..FakeEngineConfig::default()
    })
    .await;
    let engine_url = engine.url().to_owned();
    let output = run(&mut engine, failure_scenario(&engine_url))
        .await
        .expect("connection failure child completes");

    assert_process_failure(
        &output,
        3,
        ENGINE_CONNECTION_DIAGNOSTIC,
        "connection failure",
    );
    assert_eq!(output.connection_count, 1);
    assert!(output.captured.identity.is_some());
    assert_eq!(
        output.captured.registration_namespace.as_deref(),
        Some(NAMESPACE)
    );
    assert_eq!(output.captured.invocation_count, 0);
    assert!(output.captured.function_id.is_none());
    assert!(output.captured.payload.is_none());
    assert!(output.captured.shutdown_observed);
    assert_private_failure_output(&output, "connection failure");
}

#[tokio::test(flavor = "current_thread")]
async fn registration_rejection_returns_three_without_triggering() {
    let mut engine = FakeEngine::start(FakeEngineConfig {
        registration: RegistrationOutcome::Rejected,
        ..FakeEngineConfig::default()
    })
    .await;
    let engine_url = engine.url().to_owned();
    let output = run(&mut engine, failure_scenario(&engine_url))
        .await
        .expect("registration rejection child completes");

    assert_process_failure(
        &output,
        3,
        ENGINE_CONNECTION_DIAGNOSTIC,
        "registration rejection",
    );
    assert_eq!(output.connection_count, 1);
    assert!(output.captured.identity.is_some());
    assert_eq!(
        output.captured.registration_namespace.as_deref(),
        Some(NAMESPACE)
    );
    assert_eq!(output.captured.invocation_count, 0);
    assert!(output.captured.function_id.is_none());
    assert!(output.captured.payload.is_none());
    assert!(output.captured.shutdown_observed);
    assert_private_failure_output(&output, "registration rejection");
}

#[tokio::test(flavor = "current_thread")]
async fn invocation_failures_return_four_once_without_retry_or_private_output() {
    let cases = [
        ("remote rejection", InvocationOutcome::RemoteRejection),
        ("dropped connection", InvocationOutcome::DroppedConnection),
        ("malformed success", InvocationOutcome::ApplicationInvalid),
    ];

    for (description, invocation) in cases {
        let mut engine = FakeEngine::start(FakeEngineConfig {
            invocation,
            ..FakeEngineConfig::default()
        })
        .await;
        let engine_url = engine.url().to_owned();
        let output = run(&mut engine, failure_scenario(&engine_url))
            .await
            .unwrap_or_else(|error| panic!("{description} child completes: {error}"));

        assert_process_failure(&output, 4, INVOCATION_DIAGNOSTIC, description);
        if invocation == InvocationOutcome::DroppedConnection {
            assert!(
                output.connection_count >= 2,
                "{description} must observe the SDK reconnect separately from application attempts"
            );
        } else {
            assert_eq!(output.connection_count, 1, "{description}");
        }
        assert_eq!(output.captured.invocation_count, 1, "{description}");
        assert_eq!(
            output.captured.function_id.as_deref(),
            Some("harness::observation"),
            "{description}"
        );
        assert_eq!(
            output.captured.payload,
            Some(failure_observation_payload()),
            "{description} payload"
        );
        assert!(output.captured.shutdown_observed, "{description}");
        assert_private_failure_output(&output, description);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn open_stdin_forces_timeout_cleanup_before_run_returns() {
    let mut engine = FakeEngine::start(FakeEngineConfig::default()).await;
    let result = timeout(
        Duration::from_secs(3),
        run(
            &mut engine,
            ProcessScenario {
                args: vec![
                    "observation".to_owned(),
                    "--hook-type".to_owned(),
                    "process-harness-open-stdin".to_owned(),
                    "--project-name".to_owned(),
                    "process-harness-project".to_owned(),
                    "--current-working-directory".to_owned(),
                    "/workspace/process-harness".to_owned(),
                    "--timestamp".to_owned(),
                    "2026-09-19T20:14:33.123Z".to_owned(),
                    "--session-id".to_owned(),
                    "process-harness-session".to_owned(),
                ],
                stdin: StdinMode::Open(Vec::new()),
                timeout: Duration::from_millis(100),
                ..ProcessScenario::default()
            },
        ),
    )
    .await
    .expect("the runner must return after timing out and cleaning up the child");
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("a child with held-open stdin must not complete from EOF"),
    };

    assert_eq!(error, "process test child did not complete");
    assert_eq!(engine.connection_count(), 0);
}
