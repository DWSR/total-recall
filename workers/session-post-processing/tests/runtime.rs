use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use iii_sdk::{
    DEFAULT_ENGINE_URL, Error as IiiError, WorkerIdentityMode, builtin_triggers::CronCallRequest,
    engine::EngineFunctions,
};
use serde_json::{Value, json};
use session_post_processing::{
    config::{
        Config, DEFAULT_CRON_EXPRESSION, III_NAMESPACE_ENV, III_URL_ENV, III_WORKER_NAME_ENV,
        TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV,
        TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV,
        TOTAL_RECALL_POST_PROCESSING_OPENAI_API_TOKEN_ENV,
        TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL_ENV, TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV,
    },
    contracts::{CorrelationId, FailureCategory, ProcessingError, SweepOutcome, SweepOutcomeInput},
    ports::{Clock, SessionProcessingService},
    runtime::{
        CRON_TRIGGER_TYPE, CatalogRequest, DEFAULT_NAMESPACE, FunctionRegistration,
        REGISTRATION_NAMESPACE_METADATA_KEY, REGISTRATION_NONCE_METADATA_KEY,
        REGISTRATION_WORKER_NAME_METADATA_KEY, ReadinessConnectionState, ReadinessEvaluator,
        ReadinessPortError, RegistrationPlan, RegistrationReadinessPort, SWEEP_FUNCTION_ID,
        WORKER_READINESS_TIMEOUT, WorkerStartupError, handle_sweep,
    },
};
use uuid::Uuid;

const WORKER_NAME: &str = "session-post-processing-runtime";
const NAMESPACE: &str = "total-recall";

#[derive(Clone)]
struct FixedClock {
    now: DateTime<Utc>,
}

impl FixedClock {
    fn new(now: DateTime<Utc>) -> Self {
        Self { now }
    }
}

impl Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.now
    }
}

#[derive(Clone)]
struct RecordingService {
    state: Arc<Mutex<RecordingServiceState>>,
}

struct RecordingServiceState {
    calls: Vec<DateTime<Utc>>,
    result: Result<SweepOutcome, ProcessingError>,
}

impl RecordingService {
    fn succeeding(result: SweepOutcome) -> Self {
        Self {
            state: Arc::new(Mutex::new(RecordingServiceState {
                calls: Vec::new(),
                result: Ok(result),
            })),
        }
    }

    fn failing(result: ProcessingError) -> Self {
        Self {
            state: Arc::new(Mutex::new(RecordingServiceState {
                calls: Vec::new(),
                result: Err(result),
            })),
        }
    }

    fn calls(&self) -> Vec<DateTime<Utc>> {
        self.state
            .lock()
            .expect("recording service lock should not be poisoned")
            .calls
            .clone()
    }
}

#[async_trait]
impl SessionProcessingService for RecordingService {
    async fn run_sweep(&self, now: DateTime<Utc>) -> Result<SweepOutcome, ProcessingError> {
        let mut state = self
            .state
            .lock()
            .expect("recording service lock should not be poisoned");
        state.calls.push(now);
        state.result.clone()
    }
}

struct Timed<T> {
    advance: Duration,
    result: T,
}

impl<T> Timed<T> {
    fn immediate(result: T) -> Self {
        Self {
            advance: Duration::ZERO,
            result,
        }
    }

    fn after(advance: Duration, result: T) -> Self {
        Self { advance, result }
    }
}

struct ReadinessPortState {
    elapsed: Duration,
    connection: ReadinessConnectionState,
    connection_after_registration: Option<ReadinessConnectionState>,
    fatal: bool,
    registration: Option<Timed<Result<(), ReadinessPortError>>>,
    calls: VecDeque<Timed<Result<Value, ReadinessPortError>>>,
    registration_timeouts: Vec<Duration>,
    requests: Vec<CatalogRequest>,
    sleeps: Vec<Duration>,
}

struct ScriptedReadinessPort {
    state: Mutex<ReadinessPortState>,
}

impl ScriptedReadinessPort {
    fn new(
        registration: Timed<Result<(), ReadinessPortError>>,
        calls: impl IntoIterator<Item = Timed<Result<Value, ReadinessPortError>>>,
    ) -> Self {
        Self {
            state: Mutex::new(ReadinessPortState {
                elapsed: Duration::ZERO,
                connection: ReadinessConnectionState::Connected,
                connection_after_registration: None,
                fatal: false,
                registration: Some(registration),
                calls: calls.into_iter().collect(),
                registration_timeouts: Vec::new(),
                requests: Vec::new(),
                sleeps: Vec::new(),
            }),
        }
    }

    fn set_connection(&self, connection: ReadinessConnectionState) {
        self.state
            .lock()
            .expect("scripted readiness port lock should not be poisoned")
            .connection = connection;
    }

    fn set_connection_after_registration(&self, connection: ReadinessConnectionState) {
        self.state
            .lock()
            .expect("scripted readiness port lock should not be poisoned")
            .connection_after_registration = Some(connection);
    }

    fn set_fatal(&self) {
        self.state
            .lock()
            .expect("scripted readiness port lock should not be poisoned")
            .fatal = true;
    }

    fn requests(&self) -> Vec<CatalogRequest> {
        self.state
            .lock()
            .expect("scripted readiness port lock should not be poisoned")
            .requests
            .clone()
    }

    fn registration_timeouts(&self) -> Vec<Duration> {
        self.state
            .lock()
            .expect("scripted readiness port lock should not be poisoned")
            .registration_timeouts
            .clone()
    }

    fn sleeps(&self) -> Vec<Duration> {
        self.state
            .lock()
            .expect("scripted readiness port lock should not be poisoned")
            .sleeps
            .clone()
    }
}

#[async_trait]
impl RegistrationReadinessPort for ScriptedReadinessPort {
    fn elapsed(&self) -> Duration {
        self.state
            .lock()
            .expect("scripted readiness port lock should not be poisoned")
            .elapsed
    }

    fn connection_state(&self) -> ReadinessConnectionState {
        self.state
            .lock()
            .expect("scripted readiness port lock should not be poisoned")
            .connection
    }

    fn has_fatal_error(&self) -> bool {
        self.state
            .lock()
            .expect("scripted readiness port lock should not be poisoned")
            .fatal
    }

    async fn wait_until_registered(&self, timeout: Duration) -> Result<(), ReadinessPortError> {
        let mut state = self
            .state
            .lock()
            .expect("scripted readiness port lock should not be poisoned");
        state.registration_timeouts.push(timeout);
        let step = state
            .registration
            .take()
            .expect("readiness made an unexpected registration wait");
        state.elapsed += step.advance;
        if step.result.is_ok()
            && let Some(connection) = state.connection_after_registration.take()
        {
            state.connection = connection;
        }
        step.result
    }

    async fn invoke_catalog(&self, request: CatalogRequest) -> Result<Value, ReadinessPortError> {
        let mut state = self
            .state
            .lock()
            .expect("scripted readiness port lock should not be poisoned");
        state.requests.push(request);
        let step = state
            .calls
            .pop_front()
            .expect("readiness made an unexpected catalog request");
        state.elapsed += step.advance;
        step.result
    }

    async fn sleep(&self, duration: Duration) {
        let mut state = self
            .state
            .lock()
            .expect("scripted readiness port lock should not be poisoned");
        state.sleeps.push(duration);
        state.elapsed += duration;
    }
}

#[tokio::test]
async fn sweep_handler_accepts_full_typed_cron_calls_and_uses_only_the_injected_clock() {
    let now = timestamp("2026-09-23T20:30:00Z");
    let outcome = sweep_outcome(4, 1, 1, 1, 1);
    let service = RecordingService::succeeding(outcome);
    let clock = FixedClock::new(now);
    let expected_response = json!({
        "attempted": 4,
        "staged": 1,
        "completed": 1,
        "retryable": 1,
        "skipped": 1,
    });

    for request in [
        cron_call(
            "cron-a",
            "job-a",
            "1999-01-01T00:00:00Z",
            "2000-01-01T00:00:00Z",
        ),
        cron_call(
            "cron-payload-secret-sentinel",
            "job-payload-secret-sentinel",
            "2035-12-31T23:59:59Z",
            "2042-01-01T00:00:00Z",
        ),
    ] {
        let response = handle_sweep(request, &service, &clock)
            .await
            .expect("a complete typed cron request should invoke the sweep");
        assert_eq!(response, outcome);
        let serialized = serde_json::to_value(response).expect("sweep response should serialize");
        assert_eq!(serialized, expected_response);
        assert!(
            !serialized
                .to_string()
                .contains("cron-payload-secret-sentinel"),
            "cron input must not appear in a typed sweep response: {serialized}"
        );
    }

    assert_eq!(service.calls(), vec![now, now]);
}

#[test]
fn typed_sweep_response_schema_requires_unsigned_staged_count() {
    let schema = serde_json::to_value(schemars::schema_for!(SweepOutcome))
        .expect("sweep response schema should serialize");
    let required = schema["required"]
        .as_array()
        .expect("sweep response schema should require outcome counts");
    assert!(
        required.iter().any(|field| field == "staged"),
        "sweep response schema must require staged"
    );

    let staged = &schema["properties"]["staged"];
    assert_eq!(staged["type"], json!("integer"));
    assert_eq!(staged["format"], json!("uint32"));
    assert_eq!(staged["minimum"], json!(0.0));
}

#[tokio::test]
async fn sweep_handler_projects_only_content_safe_failure_diagnostics() {
    const SENTINEL: &str = "cron-payload-secret-sentinel";
    const CORRELATION_ID: &str = "c476843f-4bb4-4a05-9f1c-4cf0bbd8d13c";
    let service = RecordingService::failing(ProcessingError::new(
        FailureCategory::RepositoryInvocation,
        CorrelationId::try_from(CORRELATION_ID.to_owned())
            .expect("fixture correlation ID should be valid"),
    ));
    let clock = FixedClock::new(timestamp("2026-09-23T20:30:00Z"));

    let error = handle_sweep(
        cron_call(SENTINEL, SENTINEL, SENTINEL, SENTINEL),
        &service,
        &clock,
    )
    .await
    .expect_err("service failures must become opaque iii remote errors");

    assert!(matches!(
        &error,
        IiiError::Remote {
            code,
            message,
            stacktrace: None,
        } if code == "SESSION_POST_PROCESSING_FAILED"
            && *message == format!(
                "stage=repository category=repository_invocation retryability=after_lease_expiry correlation_id={CORRELATION_ID}"
            )
    ));
    assert_eq!(service.calls(), vec![timestamp("2026-09-23T20:30:00Z")]);

    for rendered in [
        error.to_string(),
        format!("{error:?}"),
        serde_json::to_string(&error).expect("iii errors should serialize"),
    ] {
        for permitted in [
            "repository",
            "repository_invocation",
            "after_lease_expiry",
            CORRELATION_ID,
        ] {
            assert!(
                rendered.contains(permitted),
                "safe failure diagnostic was omitted: {rendered}"
            );
        }
        assert!(
            !rendered.contains(SENTINEL),
            "cron payload escaped in a sweep error: {rendered}"
        );
        for protected in [
            "raw-observation-secret-sentinel",
            "provider-token-secret-sentinel",
            "provider-body-secret-sentinel",
            "select secret sql",
        ] {
            assert!(
                !rendered.contains(protected),
                "protected content escaped in a sweep error: {rendered}"
            );
        }
    }
}

#[test]
fn registration_plan_describes_one_typed_sweep_and_one_utc_cron_binding() {
    let nonce = Uuid::parse_str("8d4d6f3a-2a42-4e9a-9d37-4d0d4a6e0a11")
        .expect("fixture nonce should be valid");
    let plan =
        RegistrationPlan::with_nonce(&config(None, Some(WORKER_NAME), Some(NAMESPACE)), nonce);
    let expected_metadata = json!({
        REGISTRATION_NONCE_METADATA_KEY: nonce.to_string(),
        REGISTRATION_WORKER_NAME_METADATA_KEY: WORKER_NAME,
        REGISTRATION_NAMESPACE_METADATA_KEY: NAMESPACE,
    });

    assert_cron_call_request(&plan.function);
    assert_eq!([&plan.function].len(), 1);
    assert_eq!([&plan.cron].len(), 1);
    assert_eq!(plan.function.function_id, SWEEP_FUNCTION_ID);
    assert_eq!(plan.function.metadata, expected_metadata);
    assert_eq!(plan.cron.trigger_type, CRON_TRIGGER_TYPE);
    assert_eq!(plan.cron.function_id, SWEEP_FUNCTION_ID);
    assert_eq!(
        plan.cron.config,
        json!({"expression": DEFAULT_CRON_EXPRESSION})
    );
    assert_eq!(plan.cron.metadata.as_ref(), Some(&plan.function.metadata));
    assert_eq!(plan.cron.namespace.as_deref(), Some(NAMESPACE));
    assert_eq!(plan.cron.trigger_namespace.as_deref(), Some(NAMESPACE));
    assert_eq!(plan.expected_namespace, NAMESPACE);
    assert_eq!(plan.expected_worker_name, WORKER_NAME);
    assert_eq!(plan.nonce, nonce);
}

#[test]
fn registration_plan_preserves_default_and_overridden_cron_expressions() {
    for (expression, expected) in [
        (None, DEFAULT_CRON_EXPRESSION),
        (Some("0 */5 * * * *"), "0 */5 * * * *"),
    ] {
        let plan = RegistrationPlan::with_nonce(
            &config(expression, Some(WORKER_NAME), Some(NAMESPACE)),
            Uuid::parse_str("9ce70a1b-a4fc-4bb2-8e6f-dcdf71dddbad")
                .expect("fixture nonce should be valid"),
        );

        assert_eq!(plan.cron.config, json!({"expression": expected}));
    }
}

#[test]
fn registration_plan_uses_managed_identity_and_explicit_cron_namespaces() {
    let plan = readiness_plan();
    let init_options = plan.managed_worker.init_options();
    let metadata = init_options
        .metadata
        .expect("managed initialization should include worker metadata");

    assert_eq!(plan.managed_worker.identity, WorkerIdentityMode::Managed);
    assert_eq!(plan.managed_worker.metadata.name, WORKER_NAME);
    assert_eq!(
        plan.managed_worker.metadata.namespace.as_deref(),
        Some(NAMESPACE)
    );
    assert_eq!(plan.managed_worker.init_namespace, None);
    assert_eq!(init_options.identity, WorkerIdentityMode::Managed);
    assert_eq!(init_options.namespace, None);
    assert!(init_options.headers.is_none());
    assert_eq!(
        init_options
            .otel
            .as_ref()
            .expect("worker initialization must configure telemetry")
            .enabled,
        Some(false)
    );
    assert_eq!(metadata.name, WORKER_NAME);
    assert_eq!(metadata.namespace.as_deref(), Some(NAMESPACE));
    assert_eq!(plan.cron.namespace.as_deref(), Some(NAMESPACE));
    assert_eq!(plan.cron.trigger_namespace.as_deref(), Some(NAMESPACE));
}

#[test]
fn registration_plan_defaults_cron_namespaces_and_shares_one_non_nil_v4_nonce() {
    let plan = RegistrationPlan::new(&config(None, Some(WORKER_NAME), None));
    let nonce = json!(plan.nonce.to_string());

    assert_eq!(plan.expected_namespace, DEFAULT_NAMESPACE);
    assert_eq!(plan.cron.namespace.as_deref(), Some(DEFAULT_NAMESPACE));
    assert_eq!(
        plan.cron.trigger_namespace.as_deref(),
        Some(DEFAULT_NAMESPACE)
    );
    assert!(!plan.nonce.is_nil());
    assert_eq!(plan.nonce.get_version_num(), 4);
    assert_eq!(
        plan.function.metadata[REGISTRATION_NONCE_METADATA_KEY],
        nonce
    );
    assert_eq!(
        plan.cron
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get(REGISTRATION_NONCE_METADATA_KEY)),
        Some(&plan.function.metadata[REGISTRATION_NONCE_METADATA_KEY])
    );
    assert_eq!(
        plan.function.metadata[REGISTRATION_WORKER_NAME_METADATA_KEY],
        json!(WORKER_NAME)
    );
    assert_eq!(
        plan.function.metadata[REGISTRATION_NAMESPACE_METADATA_KEY],
        json!(DEFAULT_NAMESPACE)
    );
}

#[test]
fn managed_iii_configuration_uses_pure_defaults_and_safe_overrides() {
    let defaults = config(None, None, None);
    assert_eq!(defaults.iii.engine_url, DEFAULT_ENGINE_URL);
    assert_eq!(defaults.iii.worker_name, None);
    assert_eq!(defaults.iii.namespace, None);

    let configured = Config::from_values([
        (
            TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV.to_owned(),
            "source-session".to_owned(),
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV.to_owned(),
            "memory".to_owned(),
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV.to_owned(),
            "openai".to_owned(),
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_OPENAI_API_TOKEN_ENV.to_owned(),
            "runtime-token-secret-sentinel".to_owned(),
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL_ENV.to_owned(),
            "runtime-model".to_owned(),
        ),
        (
            III_URL_ENV.to_owned(),
            "ws://runtime-url-secret-sentinel@127.0.0.1:49134".to_owned(),
        ),
        (III_WORKER_NAME_ENV.to_owned(), WORKER_NAME.to_owned()),
        (III_NAMESPACE_ENV.to_owned(), NAMESPACE.to_owned()),
    ])
    .expect("managed iii overrides should be accepted");

    assert_eq!(
        configured.iii.engine_url,
        "ws://runtime-url-secret-sentinel@127.0.0.1:49134"
    );
    assert_eq!(configured.iii.worker_name.as_deref(), Some(WORKER_NAME));
    assert_eq!(configured.iii.namespace.as_deref(), Some(NAMESPACE));
    assert!(!format!("{configured:?}").contains("runtime-url-secret-sentinel"));
    assert!(!format!("{:?}", configured.iii).contains("runtime-url-secret-sentinel"));
}

#[tokio::test]
async fn readiness_accepts_exactly_one_active_current_process_cron_binding() {
    let plan = readiness_plan();
    let port = ScriptedReadinessPort::new(Timed::immediate(Ok(())), ready_calls(&plan));

    ReadinessEvaluator::new(&plan, &port)
        .wait_until_ready()
        .await
        .expect("one active owned cron binding should make the worker ready");

    assert_eq!(port.registration_timeouts(), vec![WORKER_READINESS_TIMEOUT]);
    assert_eq!(
        port.requests(),
        vec![
            CatalogRequest {
                function_id: EngineFunctions::INFO_FUNCTIONS.to_owned(),
                payload: json!({
                    "function_ids": [SWEEP_FUNCTION_ID],
                    "namespace": NAMESPACE,
                }),
                timeout: WORKER_READINESS_TIMEOUT,
                namespace: None,
            },
            CatalogRequest {
                function_id: EngineFunctions::LIST_REGISTERED_TRIGGERS.to_owned(),
                payload: json!({
                    "function_id": SWEEP_FUNCTION_ID,
                    "trigger_type": CRON_TRIGGER_TYPE,
                    "include_pending": true,
                }),
                timeout: WORKER_READINESS_TIMEOUT,
                namespace: None,
            },
            CatalogRequest {
                function_id: EngineFunctions::INFO_REGISTERED_TRIGGERS.to_owned(),
                payload: json!({"id": "active"}),
                timeout: WORKER_READINESS_TIMEOUT,
                namespace: None,
            },
        ]
    );
}

#[tokio::test]
async fn readiness_waits_for_registration_before_validating_the_catalog() {
    let plan = readiness_plan();
    let port = ScriptedReadinessPort::new(Timed::immediate(Ok(())), ready_calls(&plan));
    port.set_connection(ReadinessConnectionState::Connecting);
    port.set_connection_after_registration(ReadinessConnectionState::Connected);

    ReadinessEvaluator::new(&plan, &port)
        .wait_until_ready()
        .await
        .expect("a connecting worker should validate its catalog after registration");

    assert_eq!(port.connection_state(), ReadinessConnectionState::Connected);
}

#[tokio::test]
async fn readiness_rejects_registration_disconnect_and_catalog_failures_without_payloads() {
    let plan = readiness_plan();

    let rejected = ScriptedReadinessPort::new(
        Timed::immediate(Err(ReadinessPortError::RegistrationRejected)),
        [],
    );
    assert_eq!(
        ReadinessEvaluator::new(&plan, &rejected)
            .wait_until_ready()
            .await,
        Err(WorkerStartupError::RegistrationRejected)
    );

    let disconnected =
        ScriptedReadinessPort::new(Timed::immediate(Err(ReadinessPortError::Disconnected)), []);
    assert_eq!(
        ReadinessEvaluator::new(&plan, &disconnected)
            .wait_until_ready()
            .await,
        Err(WorkerStartupError::Disconnected)
    );

    let catalog_failure = ScriptedReadinessPort::new(
        Timed::immediate(Ok(())),
        [Timed::immediate(Err(ReadinessPortError::RequestFailed))],
    );
    assert_eq!(
        ReadinessEvaluator::new(&plan, &catalog_failure)
            .wait_until_ready()
            .await,
        Err(WorkerStartupError::CatalogRequestFailed)
    );

    let fatal = ScriptedReadinessPort::new(Timed::immediate(Ok(())), []);
    fatal.set_fatal();
    assert_eq!(
        ReadinessEvaluator::new(&plan, &fatal)
            .wait_until_ready()
            .await,
        Err(WorkerStartupError::RegistrationRejected)
    );
    assert!(fatal.registration_timeouts().is_empty());

    for error in [
        WorkerStartupError::RegistrationRejected,
        WorkerStartupError::Disconnected,
        WorkerStartupError::CatalogRequestFailed,
    ] {
        let rendered = format!("{error:?} {error}");
        assert!(
            !rendered.contains("catalog-payload-secret-sentinel"),
            "startup error leaked catalog content: {rendered}"
        );
    }
}

#[tokio::test]
async fn readiness_rejects_stale_pending_inactive_foreign_and_duplicate_catalog_states() {
    let plan = readiness_plan();
    let stale_nonce = Uuid::parse_str("af7c58fa-f6bc-4d38-b6a5-7504952b496e")
        .expect("fixture nonce should be valid");

    assert_catalog_mismatch(
        &plan,
        vec![Timed::immediate(Ok(function_catalog(&plan, stale_nonce)))],
    )
    .await;

    for status in ["pending", "inactive"] {
        assert_catalog_mismatch(
            &plan,
            vec![
                Timed::immediate(Ok(function_catalog(&plan, plan.nonce))),
                Timed::immediate(Ok(registered_trigger_list(&["status"]))),
                Timed::immediate(Ok(registered_trigger_detail(
                    &plan, "status", status, plan.nonce,
                ))),
            ],
        )
        .await;
    }

    let mut stale_trigger = registered_trigger_detail(&plan, "stale", "active", stale_nonce);
    stale_trigger["metadata"][REGISTRATION_NONCE_METADATA_KEY] = json!(stale_nonce.to_string());
    assert_catalog_mismatch(
        &plan,
        vec![
            Timed::immediate(Ok(function_catalog(&plan, plan.nonce))),
            Timed::immediate(Ok(registered_trigger_list(&["stale"]))),
            Timed::immediate(Ok(stale_trigger)),
        ],
    )
    .await;

    let mut wrong_expression = registered_trigger_detail(&plan, "expression", "active", plan.nonce);
    wrong_expression["config"] = json!({"expression": "0 0 1 * * *"});
    let mut foreign_provider = registered_trigger_detail(&plan, "provider", "active", plan.nonce);
    foreign_provider["trigger"]["namespace"] = json!("foreign-namespace");
    let mut foreign_target = registered_trigger_detail(&plan, "target", "active", plan.nonce);
    foreign_target["function"]["worker_name"] = json!("foreign-worker");
    let mut foreign_target_namespace =
        registered_trigger_detail(&plan, "target-namespace", "active", plan.nonce);
    foreign_target_namespace["function"]["namespace"] = json!("foreign-namespace");
    let mut foreign_target_id = registered_trigger_detail(&plan, "target-id", "active", plan.nonce);
    foreign_target_id["function"]["function_id"] = json!("foreign::sweep");
    for (id, detail) in [
        ("expression", wrong_expression),
        ("provider", foreign_provider),
        ("target", foreign_target),
        ("target-namespace", foreign_target_namespace),
        ("target-id", foreign_target_id),
    ] {
        assert_catalog_mismatch(
            &plan,
            vec![
                Timed::immediate(Ok(function_catalog(&plan, plan.nonce))),
                Timed::immediate(Ok(registered_trigger_list(&[id]))),
                Timed::immediate(Ok(detail)),
            ],
        )
        .await;
    }

    assert_catalog_mismatch(
        &plan,
        vec![
            Timed::immediate(Ok(function_catalog(&plan, plan.nonce))),
            Timed::immediate(Ok(registered_trigger_list(&["one", "two"]))),
            Timed::immediate(Ok(registered_trigger_detail(
                &plan, "one", "active", plan.nonce,
            ))),
            Timed::immediate(Ok(registered_trigger_detail(
                &plan, "two", "active", plan.nonce,
            ))),
        ],
    )
    .await;
}

#[tokio::test]
async fn readiness_rejects_malformed_and_wrong_function_catalogs() {
    let plan = readiness_plan();

    assert_catalog_mismatch(
        &plan,
        vec![Timed::immediate(Ok(json!({
            "functions": "catalog-payload-secret-sentinel",
        })))],
    )
    .await;

    let mut wrong_function = function_catalog(&plan, plan.nonce);
    wrong_function["functions"][0]["function_id"] = json!("foreign::sweep");
    assert_catalog_mismatch(&plan, vec![Timed::immediate(Ok(wrong_function))]).await;

    let wrong_list = json!({
        "registered_triggers": [{
            "id": "foreign",
            "trigger_type": CRON_TRIGGER_TYPE,
            "function_id": "foreign::sweep",
        }],
    });
    assert_catalog_mismatch(
        &plan,
        vec![
            Timed::immediate(Ok(function_catalog(&plan, plan.nonce))),
            Timed::immediate(Ok(wrong_list)),
        ],
    )
    .await;
}

#[tokio::test]
async fn readiness_rejects_function_catalog_namespace_and_owner_mismatches() {
    let plan = readiness_plan();

    for (field, value) in [
        ("namespace", json!("foreign-namespace")),
        ("worker_name", json!("foreign-worker")),
    ] {
        let mut calls = ready_calls(&plan);
        ready_response_mut(&mut calls, 0)["functions"][0][field] = value;

        assert_catalog_mismatch(&plan, calls).await;
    }
}

#[tokio::test]
async fn readiness_rejects_a_detail_with_a_mismatched_provider_id() {
    let plan = readiness_plan();
    let mut calls = ready_calls(&plan);
    ready_response_mut(&mut calls, 2)["trigger"]["id"] = json!("foreign-provider");

    assert_catalog_mismatch(&plan, calls).await;
}

#[tokio::test]
async fn readiness_rejects_detail_trigger_type_and_top_level_target_mismatches() {
    let plan = readiness_plan();

    for (field, value) in [
        ("trigger_type", json!("foreign-trigger")),
        ("function_id", json!("foreign::sweep")),
    ] {
        let mut calls = ready_calls(&plan);
        ready_response_mut(&mut calls, 2)[field] = value;

        assert_catalog_mismatch(&plan, calls).await;
    }
}

#[tokio::test]
async fn readiness_maps_a_direct_catalog_timeout_to_startup_timeout() {
    let plan = readiness_plan();
    let port = ScriptedReadinessPort::new(
        Timed::immediate(Ok(())),
        [Timed::immediate(Err(ReadinessPortError::Timeout))],
    );

    assert_eq!(
        ReadinessEvaluator::new(&plan, &port)
            .wait_until_ready()
            .await,
        Err(WorkerStartupError::Timeout)
    );
}

#[tokio::test]
async fn readiness_times_out_only_while_catalog_registration_is_pending() {
    let plan = readiness_plan();
    let timeout = Duration::from_millis(7);
    let port = ScriptedReadinessPort::new(
        Timed::immediate(Ok(())),
        [
            Timed::immediate(Ok(function_catalog(&plan, plan.nonce))),
            Timed::immediate(Ok(registered_trigger_list(&[]))),
        ],
    );

    assert_eq!(
        ReadinessEvaluator::new(&plan, &port)
            .wait_until_ready_with_timeout(timeout)
            .await,
        Err(WorkerStartupError::Timeout)
    );
    assert_eq!(port.sleeps(), vec![timeout]);
}

#[tokio::test]
async fn readiness_preserves_one_absolute_deadline_for_registration_and_catalog_calls() {
    let plan = readiness_plan();
    let timeout = Duration::from_millis(10);
    let port = ScriptedReadinessPort::new(
        Timed::after(Duration::from_millis(3), Ok(())),
        [Timed::after(
            Duration::from_millis(4),
            Ok(json!({
                "functions": [{
                    "function_id": SWEEP_FUNCTION_ID,
                    "error": "not_found",
                }],
            })),
        )],
    );

    assert_eq!(
        ReadinessEvaluator::new(&plan, &port)
            .wait_until_ready_with_timeout(timeout)
            .await,
        Err(WorkerStartupError::Timeout)
    );
    assert_eq!(port.registration_timeouts(), vec![timeout]);
    assert_eq!(port.requests()[0].timeout, Duration::from_millis(7));
    assert_eq!(port.sleeps(), vec![Duration::from_millis(3)]);
}

fn assert_cron_call_request(_: &FunctionRegistration<CronCallRequest>) {}

async fn assert_catalog_mismatch(
    plan: &RegistrationPlan,
    calls: Vec<Timed<Result<Value, ReadinessPortError>>>,
) {
    let port = ScriptedReadinessPort::new(Timed::immediate(Ok(())), calls);

    assert_eq!(
        ReadinessEvaluator::new(plan, &port)
            .wait_until_ready()
            .await,
        Err(WorkerStartupError::CatalogMismatch)
    );
}

fn config(expression: Option<&str>, worker_name: Option<&str>, namespace: Option<&str>) -> Config {
    let mut values = vec![
        (
            TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV.to_owned(),
            "source-session".to_owned(),
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV.to_owned(),
            "memory".to_owned(),
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV.to_owned(),
            "openai".to_owned(),
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_OPENAI_API_TOKEN_ENV.to_owned(),
            "runtime-token-secret-sentinel".to_owned(),
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL_ENV.to_owned(),
            "runtime-model".to_owned(),
        ),
    ];
    if let Some(expression) = expression {
        values.push((
            "TOTAL_RECALL_POST_PROCESSING_CRON_EXPRESSION".to_owned(),
            expression.to_owned(),
        ));
    }
    if let Some(worker_name) = worker_name {
        values.push((III_WORKER_NAME_ENV.to_owned(), worker_name.to_owned()));
    }
    if let Some(namespace) = namespace {
        values.push((III_NAMESPACE_ENV.to_owned(), namespace.to_owned()));
    }
    Config::from_values(values).expect("runtime test configuration should be valid")
}

fn cron_call(
    trigger: &str,
    job_id: &str,
    scheduled_time: &str,
    actual_time: &str,
) -> CronCallRequest {
    CronCallRequest {
        trigger: trigger.to_owned(),
        job_id: job_id.to_owned(),
        scheduled_time: scheduled_time.to_owned(),
        actual_time: actual_time.to_owned(),
    }
}

fn function_catalog(plan: &RegistrationPlan, nonce: Uuid) -> Value {
    json!({
        "functions": [{
            "function_id": SWEEP_FUNCTION_ID,
            "namespace": plan.expected_namespace,
            "worker_name": plan.expected_worker_name,
            "metadata": {REGISTRATION_NONCE_METADATA_KEY: nonce.to_string()},
        }],
    })
}

fn registered_trigger_list(ids: &[&str]) -> Value {
    json!({
        "registered_triggers": ids.iter().map(|id| json!({
            "id": id,
            "trigger_type": CRON_TRIGGER_TYPE,
            "function_id": SWEEP_FUNCTION_ID,
        })).collect::<Vec<_>>(),
    })
}

fn registered_trigger_detail(
    plan: &RegistrationPlan,
    id: &str,
    status: &str,
    nonce: Uuid,
) -> Value {
    json!({
        "id": id,
        "trigger_type": CRON_TRIGGER_TYPE,
        "function_id": SWEEP_FUNCTION_ID,
        "worker_name": plan.expected_worker_name,
        "status": status,
        "config": plan.cron.config.clone(),
        "metadata": {REGISTRATION_NONCE_METADATA_KEY: nonce.to_string()},
        "trigger": {
            "id": CRON_TRIGGER_TYPE,
            "namespace": plan.expected_namespace,
        },
        "function": {
            "function_id": SWEEP_FUNCTION_ID,
            "namespace": plan.expected_namespace,
            "worker_name": plan.expected_worker_name,
        },
    })
}

fn ready_calls(plan: &RegistrationPlan) -> Vec<Timed<Result<Value, ReadinessPortError>>> {
    vec![
        Timed::immediate(Ok(function_catalog(plan, plan.nonce))),
        Timed::immediate(Ok(registered_trigger_list(&["active"]))),
        Timed::immediate(Ok(registered_trigger_detail(
            plan, "active", "active", plan.nonce,
        ))),
    ]
}

fn ready_response_mut(
    calls: &mut [Timed<Result<Value, ReadinessPortError>>],
    index: usize,
) -> &mut Value {
    calls[index]
        .result
        .as_mut()
        .expect("ready sequence should contain a successful catalog response")
}

fn readiness_plan() -> RegistrationPlan {
    RegistrationPlan::with_nonce(
        &config(None, Some(WORKER_NAME), Some(NAMESPACE)),
        Uuid::parse_str("63ec9af3-668e-4ea6-9bfe-a17b5f0c3c75")
            .expect("fixture nonce should be valid"),
    )
}

fn sweep_outcome(
    attempted: u32,
    staged: u32,
    completed: u32,
    retryable: u32,
    skipped: u32,
) -> SweepOutcome {
    SweepOutcome::try_from(SweepOutcomeInput {
        attempted,
        staged,
        completed,
        retryable,
        skipped,
    })
    .expect("fixture sweep counts should balance")
}

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("timestamp fixture should be valid")
        .with_timezone(&Utc)
}
