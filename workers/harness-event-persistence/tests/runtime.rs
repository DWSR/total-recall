use std::{collections::VecDeque, sync::Mutex, time::Duration};

use async_trait::async_trait;
use harness_event_persistence::{
    config::{
        Config, III_NAMESPACE_ENV, III_WORKER_NAME_ENV, TOTAL_RECALL_DATABASE_ENV,
        TOTAL_RECALL_QUEUE_TOPIC_ENV,
    },
    contracts::QueuedHarnessEventInput,
    runtime::{
        CatalogRequest, DEFAULT_NAMESPACE, DURABLE_SUBSCRIBER_TRIGGER_TYPE, FunctionRegistration,
        IiiRegistrationReadinessPort, InheritedCallNamespaces, PERSIST_EVENT_FUNCTION_ID,
        REGISTRATION_NAMESPACE_METADATA_KEY, REGISTRATION_NONCE_METADATA_KEY,
        REGISTRATION_WORKER_NAME_METADATA_KEY, ReadinessConnectionState, ReadinessEvaluator,
        ReadinessPortError, RegistrationPlan, RegistrationReadinessPort, WORKER_READINESS_TIMEOUT,
        WorkerStartupError,
    },
};
use iii_sdk::{IIIClient, WorkerIdentityMode, engine::EngineFunctions};
use serde_json::json;
use uuid::Uuid;

const WORKER_NAME: &str = "harness-event-persistence";
const NAMESPACE: &str = "total-recall";
const TOPIC: &str = "harness-events";

fn config(worker_name: Option<&str>, namespace: Option<&str>) -> Config {
    let mut values = vec![
        (TOTAL_RECALL_QUEUE_TOPIC_ENV.to_owned(), TOPIC.to_owned()),
        (
            TOTAL_RECALL_DATABASE_ENV.to_owned(),
            "harness-ledger".to_owned(),
        ),
    ];
    if let Some(worker_name) = worker_name {
        values.push((III_WORKER_NAME_ENV.to_owned(), worker_name.to_owned()));
    }
    if let Some(namespace) = namespace {
        values.push((III_NAMESPACE_ENV.to_owned(), namespace.to_owned()));
    }

    Config::from_values(values).expect("runtime test configuration should be valid")
}

fn assert_queued_harness_event_input(_: &FunctionRegistration<QueuedHarnessEventInput>) {}

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
    calls: VecDeque<Timed<Result<serde_json::Value, ReadinessPortError>>>,
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
        calls: impl IntoIterator<Item = Timed<Result<serde_json::Value, ReadinessPortError>>>,
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
        self.state.lock().unwrap().connection = connection;
    }

    fn set_connection_after_registration(&self, connection: ReadinessConnectionState) {
        self.state.lock().unwrap().connection_after_registration = Some(connection);
    }

    fn set_fatal(&self) {
        self.state.lock().unwrap().fatal = true;
    }

    fn requests(&self) -> Vec<CatalogRequest> {
        self.state.lock().unwrap().requests.clone()
    }

    fn registration_timeouts(&self) -> Vec<Duration> {
        self.state.lock().unwrap().registration_timeouts.clone()
    }

    fn sleeps(&self) -> Vec<Duration> {
        self.state.lock().unwrap().sleeps.clone()
    }
}

#[async_trait]
impl RegistrationReadinessPort for ScriptedReadinessPort {
    fn elapsed(&self) -> Duration {
        self.state.lock().unwrap().elapsed
    }

    fn connection_state(&self) -> ReadinessConnectionState {
        self.state.lock().unwrap().connection
    }

    fn has_fatal_error(&self) -> bool {
        self.state.lock().unwrap().fatal
    }

    async fn wait_until_registered(&self, timeout: Duration) -> Result<(), ReadinessPortError> {
        let mut state = self.state.lock().unwrap();
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

    async fn invoke_catalog(
        &self,
        request: CatalogRequest,
    ) -> Result<serde_json::Value, ReadinessPortError> {
        let mut state = self.state.lock().unwrap();
        state.requests.push(request);
        let step = state
            .calls
            .pop_front()
            .expect("readiness made an unexpected catalog request");
        state.elapsed += step.advance;
        step.result
    }

    async fn sleep(&self, duration: Duration) {
        let mut state = self.state.lock().unwrap();
        state.sleeps.push(duration);
        state.elapsed += duration;
    }
}

fn function_catalog(plan: &RegistrationPlan, nonce: Uuid) -> serde_json::Value {
    json!({
        "functions": [{
            "function_id": PERSIST_EVENT_FUNCTION_ID,
            "namespace": plan.expected_namespace,
            "worker_name": plan.expected_worker_name,
            "metadata": {REGISTRATION_NONCE_METADATA_KEY: nonce.to_string()},
        }],
    })
}

fn registered_trigger_list(ids: &[&str]) -> serde_json::Value {
    json!({
        "registered_triggers": ids.iter().map(|id| json!({
            "id": id,
            "trigger_type": DURABLE_SUBSCRIBER_TRIGGER_TYPE,
            "function_id": PERSIST_EVENT_FUNCTION_ID,
        })).collect::<Vec<_>>(),
    })
}

fn registered_trigger_detail(
    plan: &RegistrationPlan,
    id: &str,
    status: &str,
    nonce: Uuid,
) -> serde_json::Value {
    json!({
        "id": id,
        "trigger_type": DURABLE_SUBSCRIBER_TRIGGER_TYPE,
        "function_id": PERSIST_EVENT_FUNCTION_ID,
        "worker_name": plan.expected_worker_name,
        "status": status,
        "config": plan.subscriber.config,
        "metadata": {REGISTRATION_NONCE_METADATA_KEY: nonce.to_string()},
        "trigger": {
            "id": DURABLE_SUBSCRIBER_TRIGGER_TYPE,
            "namespace": plan.expected_namespace,
        },
        "function": {
            "function_id": PERSIST_EVENT_FUNCTION_ID,
            "namespace": plan.expected_namespace,
            "worker_name": plan.expected_worker_name,
        },
    })
}

fn ready_calls(
    plan: &RegistrationPlan,
) -> Vec<Timed<Result<serde_json::Value, ReadinessPortError>>> {
    vec![
        Timed::immediate(Ok(function_catalog(plan, plan.nonce))),
        Timed::immediate(Ok(registered_trigger_list(&["active"]))),
        Timed::immediate(Ok(registered_trigger_detail(
            plan, "active", "active", plan.nonce,
        ))),
    ]
}

fn ready_response_mut(
    calls: &mut [Timed<Result<serde_json::Value, ReadinessPortError>>],
    index: usize,
) -> &mut serde_json::Value {
    calls[index]
        .result
        .as_mut()
        .expect("ready sequence should contain a successful catalog response")
}

async fn assert_catalog_mismatch(
    plan: &RegistrationPlan,
    calls: Vec<Timed<Result<serde_json::Value, ReadinessPortError>>>,
    expected_request_functions: &[&str],
) {
    let port = ScriptedReadinessPort::new(Timed::immediate(Ok(())), calls);

    assert_eq!(
        ReadinessEvaluator::new(plan, &port)
            .wait_until_ready()
            .await,
        Err(WorkerStartupError::CatalogMismatch)
    );

    let request_functions = port
        .requests()
        .into_iter()
        .map(|request| request.function_id)
        .collect::<Vec<_>>();
    assert_eq!(
        request_functions,
        expected_request_functions
            .iter()
            .map(|function_id| (*function_id).to_owned())
            .collect::<Vec<_>>()
    );
}

fn readiness_plan() -> RegistrationPlan {
    RegistrationPlan::with_nonce(
        &config(Some(WORKER_NAME), Some(NAMESPACE)),
        Uuid::parse_str("63ec9af3-668e-4ea6-9bfe-a17b5f0c3c75")
            .expect("fixture nonce should be valid"),
    )
}

#[test]
fn registration_plan_describes_one_typed_function_and_one_subscriber() {
    let nonce = Uuid::parse_str("8d4d6f3a-2a42-4e9a-9d37-4d0d4a6e0a11")
        .expect("fixture nonce should be valid");
    let plan = RegistrationPlan::with_nonce(&config(Some(WORKER_NAME), Some(NAMESPACE)), nonce);
    let expected_metadata = json!({
        REGISTRATION_NONCE_METADATA_KEY: nonce.to_string(),
        REGISTRATION_WORKER_NAME_METADATA_KEY: WORKER_NAME,
        REGISTRATION_NAMESPACE_METADATA_KEY: NAMESPACE,
    });

    assert_queued_harness_event_input(&plan.function);
    assert_eq!([&plan.function].len(), 1);
    assert_eq!([&plan.subscriber].len(), 1);
    assert_eq!(plan.function.function_id, PERSIST_EVENT_FUNCTION_ID);
    assert_eq!(plan.function.metadata, expected_metadata);
    assert_eq!(
        plan.subscriber.trigger_type,
        DURABLE_SUBSCRIBER_TRIGGER_TYPE
    );
    assert_eq!(plan.subscriber.function_id, PERSIST_EVENT_FUNCTION_ID);
    assert_eq!(plan.subscriber.config, json!({"queue": TOPIC}));
    assert_eq!(
        plan.subscriber.metadata.as_ref(),
        Some(&plan.function.metadata)
    );
    assert_eq!(plan.subscriber.namespace.as_deref(), Some(NAMESPACE));
    assert_eq!(
        plan.subscriber.trigger_namespace.as_deref(),
        Some(NAMESPACE)
    );
    assert_eq!(plan.expected_namespace, NAMESPACE);
    assert_eq!(plan.expected_worker_name, WORKER_NAME);
    assert_eq!(plan.nonce, nonce);
}

#[test]
fn registration_plan_uses_managed_identity_and_inherited_calls() {
    let nonce = Uuid::parse_str("3bb4fe42-c2f5-48e6-9df9-2b2427bfa7cd")
        .expect("fixture nonce should be valid");
    let plan = RegistrationPlan::with_nonce(&config(Some(WORKER_NAME), Some(NAMESPACE)), nonce);
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
            .expect("worker initialization must explicitly configure telemetry")
            .enabled,
        Some(false)
    );
    assert_eq!(metadata.name, WORKER_NAME);
    assert_eq!(metadata.namespace.as_deref(), Some(NAMESPACE));
    assert_eq!(
        plan.calls,
        InheritedCallNamespaces {
            function: None,
            database: None,
        }
    );
}

#[test]
fn runtime_init_options_disable_telemetry_before_opaque_handler_payloads_can_be_dispatched() {
    const OPAQUE_OBSERVATION_SENTINEL: &str = "opaque-observation-sentinel";
    let plan = readiness_plan();
    let init_options = plan.managed_worker.init_options();

    assert_eq!(
        init_options
            .otel
            .as_ref()
            .expect("worker initialization must configure telemetry before connecting")
            .enabled,
        Some(false),
        "telemetry must be disabled before an invocation containing {OPAQUE_OBSERVATION_SENTINEL} can reach the SDK handler"
    );
}

#[test]
fn registration_plan_explicitly_uses_default_for_subscriber_namespaces() {
    let nonce = Uuid::parse_str("5ebbd7e3-878e-44ef-ae12-cf1e6b7d8055")
        .expect("fixture nonce should be valid");
    let plan = RegistrationPlan::with_nonce(&config(Some(WORKER_NAME), None), nonce);

    assert_eq!(plan.managed_worker.metadata.namespace, None);
    assert_eq!(plan.expected_namespace, DEFAULT_NAMESPACE);
    assert_eq!(
        plan.function.metadata[REGISTRATION_NAMESPACE_METADATA_KEY],
        json!(DEFAULT_NAMESPACE)
    );
    assert_eq!(
        plan.subscriber.namespace.as_deref(),
        Some(DEFAULT_NAMESPACE)
    );
    assert_eq!(
        plan.subscriber.trigger_namespace.as_deref(),
        Some(DEFAULT_NAMESPACE)
    );
}

#[test]
fn registration_plan_generates_one_v4_nonce_shared_by_both_registrations() {
    let plan = RegistrationPlan::new(&config(Some(WORKER_NAME), Some(NAMESPACE)));
    let nonce = json!(plan.nonce.to_string());

    assert_eq!(plan.nonce.get_version_num(), 4);
    assert_eq!(
        plan.function.metadata[REGISTRATION_NONCE_METADATA_KEY],
        nonce
    );
    assert_eq!(
        plan.subscriber
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get(REGISTRATION_NONCE_METADATA_KEY)),
        Some(&plan.function.metadata[REGISTRATION_NONCE_METADATA_KEY])
    );
}

#[tokio::test]
async fn iii_readiness_port_maps_an_unstarted_sdk_client_to_disconnected() {
    let client = IIIClient::new("ws://127.0.0.1:0");
    let port = IiiRegistrationReadinessPort::new(&client);

    assert_eq!(
        port.connection_state(),
        ReadinessConnectionState::Disconnected
    );
    assert_eq!(
        port.wait_until_registered(Duration::from_millis(1)).await,
        Err(ReadinessPortError::Disconnected)
    );
}

#[tokio::test]
async fn readiness_accepts_one_active_owned_trigger_and_uses_catalog_contracts() {
    let plan = readiness_plan();
    let port = ScriptedReadinessPort::new(Timed::immediate(Ok(())), ready_calls(&plan));

    ReadinessEvaluator::new(&plan, &port)
        .wait_until_ready()
        .await
        .expect("one active owned subscriber should make the worker ready");

    assert_eq!(port.registration_timeouts(), vec![WORKER_READINESS_TIMEOUT]);
    assert_eq!(
        port.requests(),
        vec![
            CatalogRequest {
                function_id: EngineFunctions::INFO_FUNCTIONS.to_owned(),
                payload: json!({
                    "function_ids": [PERSIST_EVENT_FUNCTION_ID],
                    "namespace": NAMESPACE,
                }),
                timeout: WORKER_READINESS_TIMEOUT,
                namespace: None,
            },
            CatalogRequest {
                function_id: EngineFunctions::LIST_REGISTERED_TRIGGERS.to_owned(),
                payload: json!({
                    "function_id": PERSIST_EVENT_FUNCTION_ID,
                    "trigger_type": DURABLE_SUBSCRIBER_TRIGGER_TYPE,
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
async fn readiness_waits_for_a_connecting_client_to_register_before_validating_catalogs() {
    let plan = readiness_plan();
    let port = ScriptedReadinessPort::new(Timed::immediate(Ok(())), ready_calls(&plan));
    port.set_connection(ReadinessConnectionState::Connecting);
    port.set_connection_after_registration(ReadinessConnectionState::Connected);

    ReadinessEvaluator::new(&plan, &port)
        .wait_until_ready()
        .await
        .expect("a connecting client should become ready after registration and valid catalogs");

    assert_eq!(port.connection_state(), ReadinessConnectionState::Connected);
    assert_eq!(port.registration_timeouts(), vec![WORKER_READINESS_TIMEOUT]);
    assert_eq!(
        port.requests()
            .into_iter()
            .map(|request| request.function_id)
            .collect::<Vec<_>>(),
        vec![
            EngineFunctions::INFO_FUNCTIONS.to_owned(),
            EngineFunctions::LIST_REGISTERED_TRIGGERS.to_owned(),
            EngineFunctions::INFO_REGISTERED_TRIGGERS.to_owned(),
        ]
    );
}

#[tokio::test]
async fn readiness_returns_distinct_registration_disconnect_and_catalog_errors() {
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

    let cannot_register =
        ScriptedReadinessPort::new(Timed::immediate(Err(ReadinessPortError::Disconnected)), []);
    cannot_register.set_connection(ReadinessConnectionState::Connecting);
    assert_eq!(
        ReadinessEvaluator::new(&plan, &cannot_register)
            .wait_until_ready()
            .await,
        Err(WorkerStartupError::Disconnected)
    );
    assert_eq!(
        cannot_register.registration_timeouts(),
        vec![WORKER_READINESS_TIMEOUT]
    );

    let disconnected = ScriptedReadinessPort::new(Timed::immediate(Ok(())), []);
    disconnected.set_connection(ReadinessConnectionState::Disconnected);
    assert_eq!(
        ReadinessEvaluator::new(&plan, &disconnected)
            .wait_until_ready()
            .await,
        Err(WorkerStartupError::Disconnected)
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

    let failed = ScriptedReadinessPort::new(
        Timed::immediate(Ok(())),
        [Timed::immediate(Err(ReadinessPortError::RequestFailed))],
    );
    assert_eq!(
        ReadinessEvaluator::new(&plan, &failed)
            .wait_until_ready()
            .await,
        Err(WorkerStartupError::CatalogRequestFailed)
    );

    let catalog_disconnect = ScriptedReadinessPort::new(
        Timed::immediate(Ok(())),
        [Timed::immediate(Err(ReadinessPortError::Disconnected))],
    );
    assert_eq!(
        ReadinessEvaluator::new(&plan, &catalog_disconnect)
            .wait_until_ready()
            .await,
        Err(WorkerStartupError::Disconnected)
    );
}

#[tokio::test]
async fn readiness_times_out_when_a_registered_trigger_is_missing() {
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
async fn readiness_rejects_non_not_found_function_catalog_errors() {
    let plan = readiness_plan();
    let port = ScriptedReadinessPort::new(
        Timed::immediate(Ok(())),
        [Timed::immediate(Ok(json!({
            "functions": [{
                "function_id": PERSIST_EVENT_FUNCTION_ID,
                "error": "forbidden",
            }],
        })))],
    );

    assert_eq!(
        ReadinessEvaluator::new(&plan, &port)
            .wait_until_ready()
            .await,
        Err(WorkerStartupError::CatalogMismatch)
    );
}

#[tokio::test]
async fn readiness_rejects_stale_function_or_trigger_nonces() {
    let plan = readiness_plan();
    let stale_nonce = Uuid::parse_str("af7c58fa-f6bc-4d38-b6a5-7504952b496e")
        .expect("fixture nonce should be valid");

    for calls in [
        vec![Timed::immediate(Ok(function_catalog(&plan, stale_nonce)))],
        vec![
            Timed::immediate(Ok(function_catalog(&plan, plan.nonce))),
            Timed::immediate(Ok(registered_trigger_list(&["stale"]))),
            Timed::immediate(Ok(registered_trigger_detail(
                &plan,
                "stale",
                "active",
                stale_nonce,
            ))),
        ],
    ] {
        let port = ScriptedReadinessPort::new(Timed::immediate(Ok(())), calls);

        assert_eq!(
            ReadinessEvaluator::new(&plan, &port)
                .wait_until_ready()
                .await,
            Err(WorkerStartupError::CatalogMismatch)
        );
    }
}

#[tokio::test]
async fn readiness_rejects_pending_and_ambiguous_trigger_details() {
    let plan = readiness_plan();
    let pending = ScriptedReadinessPort::new(
        Timed::immediate(Ok(())),
        [
            Timed::immediate(Ok(function_catalog(&plan, plan.nonce))),
            Timed::immediate(Ok(registered_trigger_list(&["pending"]))),
            Timed::immediate(Ok(registered_trigger_detail(
                &plan, "pending", "pending", plan.nonce,
            ))),
        ],
    );
    assert_eq!(
        ReadinessEvaluator::new(&plan, &pending)
            .wait_until_ready()
            .await,
        Err(WorkerStartupError::CatalogMismatch)
    );

    let ambiguous = ScriptedReadinessPort::new(
        Timed::immediate(Ok(())),
        [
            Timed::immediate(Ok(function_catalog(&plan, plan.nonce))),
            Timed::immediate(Ok(registered_trigger_list(&["one", "two"]))),
            Timed::immediate(Ok(registered_trigger_detail(
                &plan, "one", "active", plan.nonce,
            ))),
            Timed::immediate(Ok(registered_trigger_detail(
                &plan, "two", "active", plan.nonce,
            ))),
        ],
    );
    assert_eq!(
        ReadinessEvaluator::new(&plan, &ambiguous)
            .wait_until_ready()
            .await,
        Err(WorkerStartupError::CatalogMismatch)
    );
}

#[tokio::test]
async fn readiness_rejects_foreign_target_owners_and_mismatched_details() {
    let plan = readiness_plan();
    let mut foreign_owner = registered_trigger_detail(&plan, "foreign", "active", plan.nonce);
    foreign_owner["function"]["worker_name"] = json!("foreign-worker");

    let mut wrong_config = registered_trigger_detail(&plan, "config", "active", plan.nonce);
    wrong_config["config"] = json!({"queue": "wrong-topic"});

    let mut wrong_provider = registered_trigger_detail(&plan, "provider", "active", plan.nonce);
    wrong_provider["trigger"]["namespace"] = json!("foreign-namespace");

    let mut wrong_target_namespace =
        registered_trigger_detail(&plan, "target-namespace", "active", plan.nonce);
    wrong_target_namespace["function"]["namespace"] = json!("foreign-namespace");

    let mut wrong_function = registered_trigger_detail(&plan, "function", "active", plan.nonce);
    wrong_function["function_id"] = json!("foreign::persist_event");

    for (id, detail) in [
        ("foreign", foreign_owner),
        ("config", wrong_config),
        ("provider", wrong_provider),
        ("target-namespace", wrong_target_namespace),
        ("function", wrong_function),
    ] {
        let port = ScriptedReadinessPort::new(
            Timed::immediate(Ok(())),
            [
                Timed::immediate(Ok(function_catalog(&plan, plan.nonce))),
                Timed::immediate(Ok(registered_trigger_list(&[id]))),
                Timed::immediate(Ok(detail)),
            ],
        );

        assert_eq!(
            ReadinessEvaluator::new(&plan, &port)
                .wait_until_ready()
                .await,
            Err(WorkerStartupError::CatalogMismatch)
        );
    }
}

#[tokio::test]
async fn readiness_rejects_a_missing_target_function() {
    let plan = readiness_plan();
    let mut calls = ready_calls(&plan);
    ready_response_mut(&mut calls, 2)
        .as_object_mut()
        .expect("ready detail should be an object")
        .remove("function");

    assert_catalog_mismatch(
        &plan,
        calls,
        &[
            EngineFunctions::INFO_FUNCTIONS,
            EngineFunctions::LIST_REGISTERED_TRIGGERS,
            EngineFunctions::INFO_REGISTERED_TRIGGERS,
        ],
    )
    .await;
}

#[tokio::test]
async fn readiness_rejects_a_null_target_function() {
    let plan = readiness_plan();
    let mut calls = ready_calls(&plan);
    ready_response_mut(&mut calls, 2)["function"] = serde_json::Value::Null;

    assert_catalog_mismatch(
        &plan,
        calls,
        &[
            EngineFunctions::INFO_FUNCTIONS,
            EngineFunctions::LIST_REGISTERED_TRIGGERS,
            EngineFunctions::INFO_REGISTERED_TRIGGERS,
        ],
    )
    .await;
}

#[tokio::test]
async fn readiness_rejects_a_target_function_with_a_mismatched_id() {
    let plan = readiness_plan();
    let mut calls = ready_calls(&plan);
    ready_response_mut(&mut calls, 2)["function"]["function_id"] = json!("foreign::persist_event");

    assert_catalog_mismatch(
        &plan,
        calls,
        &[
            EngineFunctions::INFO_FUNCTIONS,
            EngineFunctions::LIST_REGISTERED_TRIGGERS,
            EngineFunctions::INFO_REGISTERED_TRIGGERS,
        ],
    )
    .await;
}

#[tokio::test]
async fn readiness_rejects_a_detail_with_a_mismatched_trigger_type() {
    let plan = readiness_plan();
    let mut calls = ready_calls(&plan);
    ready_response_mut(&mut calls, 2)["trigger_type"] = json!("durable:foreign");

    assert_catalog_mismatch(
        &plan,
        calls,
        &[
            EngineFunctions::INFO_FUNCTIONS,
            EngineFunctions::LIST_REGISTERED_TRIGGERS,
            EngineFunctions::INFO_REGISTERED_TRIGGERS,
        ],
    )
    .await;
}

#[tokio::test]
async fn readiness_rejects_a_detail_with_a_mismatched_provider_id() {
    let plan = readiness_plan();
    let mut calls = ready_calls(&plan);
    ready_response_mut(&mut calls, 2)["trigger"]["id"] = json!("durable:foreign");

    assert_catalog_mismatch(
        &plan,
        calls,
        &[
            EngineFunctions::INFO_FUNCTIONS,
            EngineFunctions::LIST_REGISTERED_TRIGGERS,
            EngineFunctions::INFO_REGISTERED_TRIGGERS,
        ],
    )
    .await;
}

#[tokio::test]
async fn readiness_rejects_a_function_catalog_entry_with_a_mismatched_id() {
    let plan = readiness_plan();
    let mut calls = ready_calls(&plan);
    ready_response_mut(&mut calls, 0)["functions"][0]["function_id"] =
        json!("foreign::persist_event");

    assert_catalog_mismatch(&plan, calls, &[EngineFunctions::INFO_FUNCTIONS]).await;
}

#[tokio::test]
async fn readiness_rejects_a_function_catalog_entry_with_a_mismatched_namespace() {
    let plan = readiness_plan();
    let mut calls = ready_calls(&plan);
    ready_response_mut(&mut calls, 0)["functions"][0]["namespace"] = json!("foreign-namespace");

    assert_catalog_mismatch(&plan, calls, &[EngineFunctions::INFO_FUNCTIONS]).await;
}

#[tokio::test]
async fn readiness_rejects_a_function_catalog_entry_with_a_mismatched_owner() {
    let plan = readiness_plan();
    let mut calls = ready_calls(&plan);
    ready_response_mut(&mut calls, 0)["functions"][0]["worker_name"] = json!("foreign-worker");

    assert_catalog_mismatch(&plan, calls, &[EngineFunctions::INFO_FUNCTIONS]).await;
}

#[tokio::test]
async fn readiness_preserves_one_absolute_deadline_for_catalog_calls_and_polls() {
    let plan = readiness_plan();
    let timeout = Duration::from_millis(10);
    let port = ScriptedReadinessPort::new(
        Timed::after(Duration::from_millis(3), Ok(())),
        [Timed::after(
            Duration::from_millis(4),
            Ok(json!({
                "functions": [{
                    "function_id": PERSIST_EVENT_FUNCTION_ID,
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

#[tokio::test]
async fn readiness_propagates_catalog_timeouts() {
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
