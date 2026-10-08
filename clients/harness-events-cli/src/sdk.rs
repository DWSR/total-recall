use std::time::Duration;

use iii_sdk::{
    IIIClient, InitOptions, WorkerIdentityMode, protocol::TriggerRequest, register_worker,
    runtime::WorkerMetadata,
};
use uuid::Uuid;

use crate::{app::InvokeError, config::ClientConfig, contracts::Submission};

#[derive(Clone, Copy)]
struct TimeoutBounds {
    registration: Duration,
    trigger: Duration,
}

const PRODUCTION_TIMEOUT_BOUNDS: TimeoutBounds = TimeoutBounds {
    registration: Duration::from_secs(30),
    trigger: Duration::from_secs(30),
};

pub async fn invoke(
    config: ClientConfig,
    submission: Submission,
) -> Result<serde_json::Value, InvokeError> {
    invoke_with_timeout_bounds(config, submission, PRODUCTION_TIMEOUT_BOUNDS).await
}

async fn invoke_with_timeout_bounds(
    config: ClientConfig,
    submission: Submission,
    bounds: TimeoutBounds,
) -> Result<serde_json::Value, InvokeError> {
    invoke_with_timeout_bounds_and_shutdown(config, submission, bounds, |client, _| {
        client.shutdown()
    })
    .await
}

async fn invoke_with_timeout_bounds_and_shutdown(
    config: ClientConfig,
    submission: Submission,
    bounds: TimeoutBounds,
    shutdown: impl FnOnce(&IIIClient, &Result<serde_json::Value, InvokeError>),
) -> Result<serde_json::Value, InvokeError> {
    let ClientConfig {
        engine_url,
        namespace,
    } = config;
    let client = register_worker(&engine_url, init_options(namespace));
    let result = async {
        client
            .wait_until_registered(bounds.registration)
            .await
            .map_err(|_| InvokeError::Connection)?;

        client
            .trigger(TriggerRequest {
                action: None,
                function_id: submission.function_id.as_str().to_owned(),
                payload: submission.payload,
                timeout_ms: Some(timeout_millis(bounds.trigger)),
            })
            .await
            .map_err(|_| InvokeError::Invocation)
    }
    .await;

    shutdown(&client, &result);
    result
}

fn init_options(namespace: Option<String>) -> InitOptions {
    let metadata = WorkerMetadata {
        name: format!("harness-events-cli-{}", Uuid::new_v4()),
        ..Default::default()
    };

    let mut options = InitOptions {
        metadata: Some(metadata),
        headers: None,
        otel: Some(Default::default()),
        namespace,
        identity: WorkerIdentityMode::Explicit,
    };
    if let Some(otel) = options.otel.as_mut() {
        otel.enabled = Some(false);
    }
    options
}

fn timeout_millis(timeout: Duration) -> u64 {
    u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "../tests/support/engine.rs"]
mod engine;

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use serde_json::json;
    use uuid::Uuid;

    use crate::{
        app::{SubmitError, submit_once},
        config::ClientConfig,
        contracts::{IngestionFunction, Submission},
    };

    use super::engine::{
        CapturedInvocation, FakeEngine, FakeEngineConfig, InvocationOutcome, RegistrationOutcome,
    };
    use super::{
        InvokeError, PRODUCTION_TIMEOUT_BOUNDS, TimeoutBounds, WorkerIdentityMode, init_options,
        invoke_with_timeout_bounds, invoke_with_timeout_bounds_and_shutdown,
    };

    fn submission() -> Submission {
        Submission {
            function_id: IngestionFunction::SessionStart,
            payload: json!({"session_id": "sdk-payload-sentinel"}),
        }
    }

    fn config(engine: &FakeEngine, namespace: Option<&str>) -> ClientConfig {
        ClientConfig {
            engine_url: engine.url().to_owned(),
            namespace: namespace.map(str::to_owned),
        }
    }

    fn short_timeout_bounds() -> TimeoutBounds {
        TimeoutBounds {
            registration: Duration::from_millis(50),
            trigger: Duration::from_millis(50),
        }
    }

    async fn finish(engine: &mut FakeEngine) -> CapturedInvocation {
        engine.finish().await.expect("fake engine completes");
        let captured = engine.captured().await;
        assert!(
            captured.shutdown_observed,
            "the adapter must join SDK shutdown before returning"
        );
        captured
    }

    fn assert_explicit_identity(identity: &str) {
        let uuid = identity
            .strip_prefix("harness-events-cli-")
            .expect("identity has the transient client prefix");
        let parsed = Uuid::parse_str(uuid).expect("identity suffix is a UUID v4");

        assert_eq!(parsed.get_version_num(), 4);
        assert_eq!(parsed.to_string(), uuid);
    }

    #[test]
    fn uses_thirty_second_production_timeout_bounds() {
        assert_eq!(
            PRODUCTION_TIMEOUT_BOUNDS.registration,
            Duration::from_secs(30)
        );
        assert_eq!(PRODUCTION_TIMEOUT_BOUNDS.trigger, Duration::from_secs(30));
    }

    #[test]
    fn configures_explicit_identity_namespace_and_disabled_telemetry() {
        let options = init_options(Some("client-namespace".to_owned()));
        let metadata = options
            .metadata
            .as_ref()
            .expect("SDK registration must include client metadata");

        assert_explicit_identity(&metadata.name);
        assert_eq!(options.identity, WorkerIdentityMode::Explicit);
        assert_eq!(options.namespace.as_deref(), Some("client-namespace"));
        assert_eq!(
            options
                .otel
                .as_ref()
                .expect("SDK registration must explicitly configure telemetry")
                .enabled,
            Some(false)
        );
    }

    #[tokio::test]
    async fn registers_unique_explicit_client_identities() {
        let mut first_engine = FakeEngine::start(FakeEngineConfig::default()).await;
        let first_result = invoke_with_timeout_bounds(
            config(&first_engine, Some("client-namespace")),
            submission(),
            short_timeout_bounds(),
        )
        .await;
        assert_eq!(first_result, Ok(json!({"dispatched": true})));
        let first_identity = finish(&mut first_engine)
            .await
            .identity
            .expect("first registration has an identity");

        let mut second_engine = FakeEngine::start(FakeEngineConfig::default()).await;
        let second_result = invoke_with_timeout_bounds(
            config(&second_engine, Some("client-namespace")),
            submission(),
            short_timeout_bounds(),
        )
        .await;
        assert_eq!(second_result, Ok(json!({"dispatched": true})));
        let second_identity = finish(&mut second_engine)
            .await
            .identity
            .expect("second registration has an identity");

        assert_explicit_identity(&first_identity);
        assert_explicit_identity(&second_identity);
        assert_ne!(first_identity, second_identity);
    }

    #[tokio::test]
    async fn applies_the_configured_namespace_to_registration_and_invocation() {
        let mut engine = FakeEngine::start(FakeEngineConfig::default()).await;
        let result = invoke_with_timeout_bounds(
            config(&engine, Some("client-namespace")),
            submission(),
            short_timeout_bounds(),
        )
        .await;

        assert_eq!(result, Ok(json!({"dispatched": true})));
        let captured = finish(&mut engine).await;
        assert_eq!(
            captured.registration_namespace.as_deref(),
            Some("client-namespace")
        );
        assert_eq!(
            captured.invocation_namespace.as_deref(),
            Some("client-namespace")
        );
    }

    #[tokio::test]
    async fn leaves_default_namespace_routing_unset() {
        let mut engine = FakeEngine::start(FakeEngineConfig::default()).await;
        let result =
            invoke_with_timeout_bounds(config(&engine, None), submission(), short_timeout_bounds())
                .await;

        assert_eq!(result, Ok(json!({"dispatched": true})));
        let captured = finish(&mut engine).await;
        assert_eq!(captured.registration_namespace, None);
        assert_eq!(captured.invocation_namespace, None);
    }

    #[tokio::test]
    async fn triggers_the_submission_once_with_its_function_and_payload() {
        let mut engine = FakeEngine::start(FakeEngineConfig::default()).await;
        let result = invoke_with_timeout_bounds(
            config(&engine, Some("client-namespace")),
            submission(),
            short_timeout_bounds(),
        )
        .await;

        assert_eq!(result, Ok(json!({"dispatched": true})));
        let captured = finish(&mut engine).await;
        assert_eq!(
            captured.function_id.as_deref(),
            Some("harness::session_start")
        );
        assert_eq!(
            captured.payload,
            Some(json!({"session_id": "sdk-payload-sentinel"}))
        );
        assert_eq!(captured.invocation_count, 1);
    }

    #[tokio::test]
    async fn maps_registration_rejection_to_connection_without_triggering() {
        let mut engine = FakeEngine::start(FakeEngineConfig {
            registration: RegistrationOutcome::Rejected,
            ..FakeEngineConfig::default()
        })
        .await;
        let result = invoke_with_timeout_bounds(
            config(&engine, Some("client-namespace")),
            submission(),
            short_timeout_bounds(),
        )
        .await;

        assert_eq!(result, Err(InvokeError::Connection));
        assert_eq!(finish(&mut engine).await.invocation_count, 0);
    }

    #[tokio::test]
    async fn maps_a_short_silent_registration_bound_to_connection_without_triggering() {
        let mut engine = FakeEngine::start(FakeEngineConfig {
            registration: RegistrationOutcome::Silent,
            ..FakeEngineConfig::default()
        })
        .await;
        let result = invoke_with_timeout_bounds(
            config(&engine, Some("client-namespace")),
            submission(),
            short_timeout_bounds(),
        )
        .await;

        assert_eq!(result, Err(InvokeError::Connection));
        assert_eq!(finish(&mut engine).await.invocation_count, 0);
    }

    #[tokio::test]
    async fn maps_remote_rejection_to_invocation_after_one_trigger() {
        let mut engine = FakeEngine::start(FakeEngineConfig {
            invocation: InvocationOutcome::RemoteRejection,
            ..FakeEngineConfig::default()
        })
        .await;
        let result = invoke_with_timeout_bounds(
            config(&engine, Some("client-namespace")),
            submission(),
            short_timeout_bounds(),
        )
        .await;

        assert_eq!(result, Err(InvokeError::Invocation));
        assert_eq!(finish(&mut engine).await.invocation_count, 1);
    }

    #[tokio::test]
    async fn maps_a_dropped_connection_to_invocation_after_one_trigger() {
        let mut engine = FakeEngine::start(FakeEngineConfig {
            invocation: InvocationOutcome::DroppedConnection,
            ..FakeEngineConfig::default()
        })
        .await;
        let shutdown_returned = Arc::new(AtomicBool::new(false));
        let result = invoke_with_timeout_bounds_and_shutdown(
            config(&engine, Some("client-namespace")),
            submission(),
            short_timeout_bounds(),
            {
                let shutdown_returned = Arc::clone(&shutdown_returned);
                move |client, outcome| {
                    assert_eq!(
                        outcome,
                        &Err(InvokeError::Invocation),
                        "shutdown must run after the dropped connection is classified"
                    );
                    client.shutdown();
                    shutdown_returned.store(true, Ordering::SeqCst);
                }
            },
        )
        .await;

        assert_eq!(result, Err(InvokeError::Invocation));
        assert!(
            shutdown_returned.load(Ordering::SeqCst),
            "synchronous SDK shutdown must return after the dropped connection"
        );
        engine.finish().await.expect("fake engine completes");
        let captured = engine.captured().await;
        assert_eq!(captured.invocation_count, 1);
    }

    #[tokio::test]
    async fn maps_a_short_silent_invocation_bound_to_invocation_after_one_trigger() {
        let mut engine = FakeEngine::start(FakeEngineConfig {
            invocation: InvocationOutcome::Silent,
            ..FakeEngineConfig::default()
        })
        .await;
        let result = invoke_with_timeout_bounds(
            config(&engine, Some("client-namespace")),
            submission(),
            short_timeout_bounds(),
        )
        .await;

        assert_eq!(result, Err(InvokeError::Invocation));
        assert_eq!(finish(&mut engine).await.invocation_count, 1);
    }

    #[tokio::test]
    async fn leaves_application_invalid_responses_for_the_app_boundary() {
        let mut engine = FakeEngine::start(FakeEngineConfig {
            invocation: InvocationOutcome::ApplicationInvalid,
            ..FakeEngineConfig::default()
        })
        .await;
        let client_config = config(&engine, Some("client-namespace"));
        let result = submit_once(submission(), move |submission| {
            invoke_with_timeout_bounds(client_config, submission, short_timeout_bounds())
        })
        .await;

        assert_eq!(result, Err(SubmitError::UnexpectedResponse));
        assert_eq!(finish(&mut engine).await.invocation_count, 1);
    }
}
