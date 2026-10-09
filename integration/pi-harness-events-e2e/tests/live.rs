#![cfg(unix)]

use std::{io, time::Duration};

use pi_harness_events_e2e::{
    artifacts::ArtifactPaths,
    engine::{CapturedInvocation, EngineHarness},
    pi::{PiScenarioOutcome, run_headless_pi, run_headless_pi_with_engine},
};
use serde_json::Value;
use tokio::time::{Instant, sleep};

const ENGINE_STARTUP_TIMEOUT: Duration = Duration::from_secs(15);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);
const ENGINE_CLEANUP_TIMEOUT: Duration = Duration::from_secs(3);
const SCRIPTED_PROVIDER_TIMESTAMP_MILLIS: i64 = 1_726_922_096_789;
const SCRIPTED_PROVIDER_TIMESTAMP_RFC3339: &str = "2024-09-21T12:34:56.789Z";
const EXPECTED_OBSERVATION_EVENTS: [&str; 11] = [
    "agent_start",
    "turn_start",
    "message_end",
    "message_end",
    "tool_execution_start",
    "tool_execution_end",
    "turn_end",
    "turn_start",
    "message_end",
    "turn_end",
    "agent_settled",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeliveryWaitError {
    ScenarioFailed,
    TimedOut,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TeardownWaitError {
    ProcessGroupTimedOut,
    ScenarioFailed,
    TemporaryStateRemained,
}

#[tokio::test(flavor = "current_thread")]
async fn delivery_timeout_is_reported_without_panicking() {
    let delivery = wait_for_delivery(Vec::<CapturedInvocation>::new, 1, Instant::now()).await;

    assert!(matches!(delivery, Err(DeliveryWaitError::TimedOut)));
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires explicitly staged Node, Pi, iii, harness-events, extension, and fixture artifacts"]
async fn staged_package_routes_real_pi_activity_through_iii() {
    let artifacts =
        ArtifactPaths::from_environment().expect("validate the explicit artifact contract first");
    let mut engine = EngineHarness::start(&artifacts.iii, Instant::now() + ENGINE_STARTUP_TIMEOUT)
        .await
        .expect("real iii capture functions must become routable before Pi starts");
    let engine_process_id = engine.process_id();
    let engine_namespace = engine.namespace().to_owned();

    let scenario = run_headless_pi_with_engine(&artifacts, engine.url(), engine.namespace()).await;
    let delivery = if scenario.is_ok() {
        wait_for_shutdown_delivery(&engine).await
    } else {
        Err(DeliveryWaitError::ScenarioFailed)
    };
    let engine_shutdown = engine
        .shutdown_until(Instant::now() + ENGINE_CLEANUP_TIMEOUT)
        .await;
    let engine_reaped = wait_for_process_absence(engine_process_id).await;

    let forced_delivery_failure = run_headless_pi(&artifacts).await;
    let forced_teardown = match forced_delivery_failure.as_ref() {
        Ok(outcome) => wait_for_clean_pi_teardown(outcome).await,
        Err(_) => Err(TeardownWaitError::ScenarioFailed),
    };

    assert!(
        engine_shutdown.is_ok(),
        "real iii cleanup must complete within its deadline"
    );
    assert!(
        engine_reaped.is_ok(),
        "the real iii process group must be gone before assertions run"
    );
    assert!(
        scenario.is_ok(),
        "the staged package must complete the offline scripted Pi scenario through real iii"
    );
    assert!(
        delivery.is_ok(),
        "Pi shutdown must deliver every expected capture before its deadline"
    );
    assert!(
        forced_delivery_failure.is_ok(),
        "closed-loopback harness delivery failures must remain isolated from Pi"
    );
    assert!(
        forced_teardown.is_ok(),
        "forced delivery failure must still clean every owned Pi process and temporary root"
    );

    match (&scenario, &delivery) {
        (Ok(outcome), Ok(captures)) => {
            assert_capture_contract(captures, &engine_namespace, outcome)
        }
        _ => unreachable!("outcomes are asserted after all owned processes are cleaned up"),
    }
}

async fn wait_for_shutdown_delivery(
    engine: &EngineHarness,
) -> Result<Vec<CapturedInvocation>, DeliveryWaitError> {
    wait_for_delivery(
        || engine.captures(),
        EXPECTED_OBSERVATION_EVENTS.len() + 2,
        Instant::now() + DELIVERY_TIMEOUT,
    )
    .await
}

async fn wait_for_delivery(
    captures: impl Fn() -> Vec<CapturedInvocation>,
    expected_count: usize,
    deadline: Instant,
) -> Result<Vec<CapturedInvocation>, DeliveryWaitError> {
    loop {
        let captures = captures();
        if captures.len() >= expected_count {
            return Ok(captures);
        }

        if Instant::now() >= deadline {
            return Err(DeliveryWaitError::TimedOut);
        }

        sleep(Duration::from_millis(10)).await;
    }
}

fn assert_capture_contract(
    captures: &[CapturedInvocation],
    namespace: &str,
    outcome: &PiScenarioOutcome,
) {
    assert!(
        captures.len() == EXPECTED_OBSERVATION_EVENTS.len() + 2,
        "the real route must receive one lifecycle start, every coarse observation, and one lifecycle end"
    );
    assert!(
        captures
            .iter()
            .enumerate()
            .all(|(index, capture)| capture.order == index as u64 + 1),
        "capture orders must preserve FIFO receipt order"
    );
    assert!(
        captures
            .iter()
            .all(|capture| capture.namespace == namespace),
        "every capture must retain the EngineHarness namespace"
    );
    assert!(
        captures
            .first()
            .is_some_and(|capture| capture.function_id == "harness::session_start"),
        "the first capture must be session-start"
    );
    assert!(
        captures
            .last()
            .is_some_and(|capture| capture.function_id == "harness::session_end"),
        "the final capture must be session-end after shutdown delivery"
    );

    for capture in captures {
        assert_metadata(capture, outcome);
    }
    assert_fallback_timestamps(captures);

    let observations = &captures[1..captures.len() - 1];
    assert!(
        observations
            .iter()
            .zip(EXPECTED_OBSERVATION_EVENTS)
            .all(|(capture, expected_event)| {
                capture.function_id == "harness::observation"
                    && value_is_string(
                        &capture.payload,
                        "hook_type",
                        &format!("pi.{expected_event}"),
                    )
                    && value_is_string_at(&capture.payload, "/data/source", "pi")
                    && value_is_string_at(&capture.payload, "/data/version", "1.0.0")
                    && value_is_string_at(&capture.payload, "/data/event", expected_event)
            }),
        "observation captures must retain the expected Pi native event identities"
    );

    assert_expected_observation_data(observations);
}

fn assert_metadata(capture: &CapturedInvocation, outcome: &PiScenarioOutcome) {
    let working_directory = outcome
        .working_directory
        .to_str()
        .expect("isolated fixture path must be UTF-8");
    let project_name = outcome
        .working_directory
        .file_name()
        .and_then(|name| name.to_str())
        .expect("isolated fixture working directory has a basename");

    assert!(
        value_is_string(&capture.payload, "session_id", &outcome.session_id),
        "every capture must use Pi's native session identity"
    );
    assert!(
        value_is_string(&capture.payload, "project_name", project_name),
        "every capture must derive the staged fixture project basename"
    );
    assert!(
        value_is_string(
            &capture.payload,
            "current_working_directory",
            working_directory
        ),
        "every capture must retain the staged fixture working directory"
    );
}

fn assert_fallback_timestamps(captures: &[CapturedInvocation]) {
    const FALLBACK_CAPTURE_INDEXES: [usize; 8] = [0, 1, 5, 6, 7, 10, 11, 12];

    assert!(
        FALLBACK_CAPTURE_INDEXES.into_iter().all(|index| {
            captures
                .get(index)
                .is_some_and(capture_has_millisecond_timestamp)
        }),
        "captures without native timestamps must use handler-entry timestamps with millisecond precision"
    );
}

fn assert_expected_observation_data(observations: &[CapturedInvocation]) {
    let [
        agent_start,
        first_turn_start,
        user_message,
        tool_call_message,
        tool_start,
        tool_end,
        first_turn_end,
        second_turn_start,
        final_message,
        second_turn_end,
        agent_settled,
    ] = observations
    else {
        unreachable!("the capture count is asserted before observation data checks");
    };

    assert!(
        value_is_empty_object_at(&agent_start.payload, "/data/payload")
            && value_is_empty_object_at(&agent_settled.payload, "/data/payload"),
        "agent lifecycle captures must preserve their empty native payloads"
    );
    assert!(
        capture_has_millisecond_timestamp(first_turn_start)
            && capture_has_millisecond_timestamp(user_message)
            && capture_has_millisecond_timestamp(second_turn_start)
            && value_is_number_at(&first_turn_start.payload, "/data/payload/timestamp")
            && value_is_number_at(&first_turn_start.payload, "/data/payload/turnIndex")
            && value_is_number_at(&first_turn_end.payload, "/data/payload/turnIndex")
            && value_is_number_at(&second_turn_start.payload, "/data/payload/timestamp")
            && value_is_number_at(&second_turn_start.payload, "/data/payload/turnIndex")
            && value_is_number_at(&second_turn_end.payload, "/data/payload/turnIndex"),
        "turn captures must retain native timestamps and indexes"
    );
    assert!(
        value_is_string_at(&user_message.payload, "/data/payload/role", "user")
            && value_is_string_at(
                &user_message.payload,
                "/data/payload/text",
                "run the fixture"
            )
            && value_is_number_at(&user_message.payload, "/data/payload/timestamp"),
        "the user message capture must retain the scripted fixture text fields"
    );
    assert!(
        value_is_string(
            &tool_call_message.payload,
            "timestamp",
            SCRIPTED_PROVIDER_TIMESTAMP_RFC3339,
        ),
        "the scripted tool-call metadata must preserve the fixture-native timestamp"
    );
    assert!(
        value_is_exact_number_at(
            &tool_call_message.payload,
            "/data/payload/timestamp",
            SCRIPTED_PROVIDER_TIMESTAMP_MILLIS,
        ),
        "the scripted tool-call payload must preserve the fixture-native timestamp"
    );
    assert!(
        value_is_string_at(
            &tool_call_message.payload,
            "/data/payload/api",
            "pi-harness-events-scripted"
        ) && value_is_string_at(
            &tool_call_message.payload,
            "/data/payload/model",
            "scripted-fixture-v1"
        ) && value_is_string_at(
            &tool_call_message.payload,
            "/data/payload/provider",
            "pi-harness-events-scripted"
        ) && value_is_string_at(
            &tool_call_message.payload,
            "/data/payload/role",
            "assistant"
        ) && value_is_string_at(
            &tool_call_message.payload,
            "/data/payload/stopReason",
            "toolUse"
        ) && value_is_string_at(&tool_call_message.payload, "/data/payload/text", "")
            && value_is_number_at(&tool_call_message.payload, "/data/payload/timestamp"),
        "the scripted tool-call message must retain its versioned fixture data"
    );
    assert!(
        value_is_empty_object_at(&tool_start.payload, "/data/payload/args")
            && value_is_string_at(
                &tool_start.payload,
                "/data/payload/toolCallId",
                "scripted-fixture-tool-call",
            )
            && value_is_string_at(
                &tool_start.payload,
                "/data/payload/toolName",
                "fixture_tool"
            ),
        "the fixture tool start capture must retain its native fields"
    );
    assert!(
        value_is_bool_at(&tool_end.payload, "/data/payload/isError", false)
            && value_is_string_at(
                &tool_end.payload,
                "/data/payload/toolCallId",
                "scripted-fixture-tool-call",
            )
            && value_is_string_at(&tool_end.payload, "/data/payload/toolName", "fixture_tool")
            && value_is_string_at(
                &tool_end.payload,
                "/data/payload/result/content/0/text",
                "fixture tool result",
            )
            && value_is_string_at(
                &tool_end.payload,
                "/data/payload/result/details/fixture",
                "scripted",
            )
            && value_is_string_at(
                &tool_end.payload,
                "/data/payload/result/details/status",
                "success",
            ),
        "the fixture tool result capture must retain its deterministic data fields"
    );
    assert!(
        value_is_string(
            &final_message.payload,
            "timestamp",
            SCRIPTED_PROVIDER_TIMESTAMP_RFC3339,
        ),
        "the final assistant metadata must preserve the fixture-native timestamp"
    );
    assert!(
        value_is_exact_number_at(
            &final_message.payload,
            "/data/payload/timestamp",
            SCRIPTED_PROVIDER_TIMESTAMP_MILLIS,
        ),
        "the final assistant payload must preserve the fixture-native timestamp"
    );
    assert!(
        value_is_string_at(&final_message.payload, "/data/payload/role", "assistant")
            && value_is_string_at(&final_message.payload, "/data/payload/stopReason", "stop")
            && value_is_string_at(
                &final_message.payload,
                "/data/payload/text",
                "fixture complete"
            )
            && value_is_number_at(&final_message.payload, "/data/payload/timestamp"),
        "the final assistant capture must retain the scripted fixture data"
    );
}

async fn wait_for_clean_pi_teardown(outcome: &PiScenarioOutcome) -> Result<(), TeardownWaitError> {
    wait_for_process_absence(outcome.process_id).await?;
    if outcome.isolated_root.exists() {
        Err(TeardownWaitError::TemporaryStateRemained)
    } else {
        Ok(())
    }
}

async fn wait_for_process_absence(process_id: u32) -> Result<(), TeardownWaitError> {
    let deadline = Instant::now() + ENGINE_CLEANUP_TIMEOUT;

    loop {
        if process_is_absent(process_id) && process_group_is_absent(process_id) {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(TeardownWaitError::ProcessGroupTimedOut);
        }

        sleep(Duration::from_millis(10)).await;
    }
}

fn process_is_absent(process_id: u32) -> bool {
    (unsafe { libc::kill(process_id as libc::pid_t, 0) == -1 })
        && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
}

fn process_group_is_absent(process_id: u32) -> bool {
    (unsafe { libc::kill(-(process_id as libc::pid_t), 0) == -1 })
        && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
}

fn value_is_string(value: &Value, key: &str, expected: &str) -> bool {
    value.get(key).and_then(Value::as_str) == Some(expected)
}

fn value_is_string_at(value: &Value, pointer: &str, expected: &str) -> bool {
    value.pointer(pointer).and_then(Value::as_str) == Some(expected)
}

fn value_is_number_at(value: &Value, pointer: &str) -> bool {
    value.pointer(pointer).is_some_and(Value::is_number)
}

fn value_is_exact_number_at(value: &Value, pointer: &str, expected: i64) -> bool {
    value.pointer(pointer).and_then(Value::as_f64) == Some(expected as f64)
}

fn value_is_bool_at(value: &Value, pointer: &str, expected: bool) -> bool {
    value.pointer(pointer).and_then(Value::as_bool) == Some(expected)
}

fn value_is_empty_object_at(value: &Value, pointer: &str) -> bool {
    value
        .pointer(pointer)
        .and_then(Value::as_object)
        .is_some_and(serde_json::Map::is_empty)
}

fn capture_has_millisecond_timestamp(capture: &CapturedInvocation) -> bool {
    capture
        .payload
        .get("timestamp")
        .and_then(Value::as_str)
        .is_some_and(is_millisecond_timestamp)
}

fn is_millisecond_timestamp(timestamp: &str) -> bool {
    let bytes = timestamp.as_bytes();
    bytes.len() == 24
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[10] == b'T'
        && bytes[13] == b':'
        && bytes[16] == b':'
        && bytes[19] == b'.'
        && bytes[23] == b'Z'
        && [0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18, 20, 21, 22]
            .into_iter()
            .all(|index| bytes[index].is_ascii_digit())
}
