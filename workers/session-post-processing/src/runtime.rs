use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use iii_sdk::{
    Error as IiiError, IIIClient, InitOptions, RegisterFunction, WorkerIdentityMode,
    builtin_triggers::{CronCallRequest, CronTriggerConfig, IIITrigger},
    engine::EngineFunctions,
    protocol::{RegisterTriggerInput, TriggerRequest},
    runtime::{FunctionRef, IIIConnectionState, WorkerMetadata},
    trigger::Trigger,
};
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use uuid::Uuid;

use crate::{
    config::Config,
    contracts::{ProcessingError, SweepOutcome},
    ports::{Clock, SessionProcessingService},
};

pub const SWEEP_FUNCTION_ID: &str = "session_post_processing::sweep";
pub const CRON_TRIGGER_TYPE: &str = "cron";
pub const DEFAULT_NAMESPACE: &str = "default";
pub const REGISTRATION_NONCE_METADATA_KEY: &str = "registration_nonce";
pub const REGISTRATION_WORKER_NAME_METADATA_KEY: &str = "worker_name";
pub const REGISTRATION_NAMESPACE_METADATA_KEY: &str = "namespace";
pub const WORKER_READINESS_TIMEOUT: Duration = Duration::from_secs(30);
const CATALOG_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub(crate) struct SweepAdmissionController {
    state: Arc<SweepAdmissionState>,
}

pub(crate) struct SweepAdmission {
    _permit: OwnedSemaphorePermit,
}

struct SweepAdmissionState {
    accepting: Mutex<bool>,
    permits: Arc<Semaphore>,
    limit: u32,
}

impl SweepAdmissionController {
    pub(crate) fn new(limit: u32) -> Self {
        assert!(limit > 0);
        Self {
            state: Arc::new(SweepAdmissionState {
                accepting: Mutex::new(true),
                permits: Arc::new(Semaphore::new(limit as usize)),
                limit,
            }),
        }
    }

    pub(crate) fn try_admit(&self) -> Option<SweepAdmission> {
        let accepting = self
            .state
            .accepting
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !*accepting {
            return None;
        }

        self.state
            .permits
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| SweepAdmission { _permit: permit })
    }

    pub(crate) fn close(&self) {
        *self
            .state
            .accepting
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = false;
    }

    pub(crate) async fn close_and_drain(&self, deadline: Duration) -> bool {
        self.close();
        matches!(
            tokio::time::timeout(
                deadline,
                Arc::clone(&self.state.permits).acquire_many_owned(self.state.limit),
            )
            .await,
            Ok(Ok(_))
        )
    }
}

#[derive(Clone, Debug)]
pub struct ManagedWorkerConfiguration {
    pub metadata: WorkerMetadata,
    pub identity: WorkerIdentityMode,
    pub init_namespace: Option<String>,
}

impl ManagedWorkerConfiguration {
    fn from_config(config: &Config) -> Self {
        let mut metadata = WorkerMetadata::default();
        if let Some(worker_name) = &config.iii.worker_name {
            metadata.name.clone_from(worker_name);
        }
        metadata.namespace = config.iii.namespace.clone();

        Self {
            metadata,
            identity: WorkerIdentityMode::Managed,
            init_namespace: None,
        }
    }

    pub fn init_options(&self) -> InitOptions {
        let mut options = InitOptions {
            metadata: Some(self.metadata.clone()),
            headers: None,
            otel: Some(Default::default()),
            namespace: self.init_namespace.clone(),
            identity: self.identity,
        };
        options
            .otel
            .as_mut()
            .expect("worker telemetry configuration is always present")
            .enabled = Some(false);
        options
    }
}

#[derive(Clone, Debug)]
pub struct FunctionRegistration<Input> {
    pub function_id: &'static str,
    pub metadata: Value,
    _input: PhantomData<fn(Input)>,
}

#[derive(Clone, Debug)]
pub struct RegistrationPlan {
    pub managed_worker: ManagedWorkerConfiguration,
    pub expected_worker_name: String,
    pub expected_namespace: String,
    pub nonce: Uuid,
    pub function: FunctionRegistration<CronCallRequest>,
    pub cron: RegisterTriggerInput,
}

impl RegistrationPlan {
    pub fn new(config: &Config) -> Self {
        Self::with_nonce(config, Uuid::new_v4())
    }

    pub fn with_nonce(config: &Config, nonce: Uuid) -> Self {
        let managed_worker = ManagedWorkerConfiguration::from_config(config);
        let expected_worker_name = managed_worker.metadata.name.clone();
        let expected_namespace = managed_worker
            .metadata
            .namespace
            .as_deref()
            .unwrap_or(DEFAULT_NAMESPACE)
            .to_owned();
        let metadata = registration_metadata(nonce, &expected_worker_name, &expected_namespace);
        let cron = IIITrigger::Cron(CronTriggerConfig::new(config.cron_expression.clone()))
            .for_function(SWEEP_FUNCTION_ID)
            .with_metadata(metadata.clone())
            .in_namespace(expected_namespace.clone())
            .in_trigger_namespace(expected_namespace.clone());

        Self {
            managed_worker,
            expected_worker_name,
            expected_namespace,
            nonce,
            function: FunctionRegistration {
                function_id: SWEEP_FUNCTION_ID,
                metadata,
                _input: PhantomData,
            },
            cron,
        }
    }
}

pub struct WorkerRuntime {
    client: Option<IIIClient>,
    plan: RegistrationPlan,
    admissions: SweepAdmissionController,
    shutdown_drain: Duration,
    _function_ref: FunctionRef,
    _cron: Trigger,
}

impl WorkerRuntime {
    pub fn start(
        config: &Config,
        service: Arc<dyn SessionProcessingService>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, WorkerStartupError> {
        Self::start_with_service_factory(config, clock, move |_| service)
    }

    #[doc(hidden)]
    pub fn start_with_service_factory<F>(
        config: &Config,
        clock: Arc<dyn Clock>,
        service_factory: F,
    ) -> Result<Self, WorkerStartupError>
    where
        F: FnOnce(IIIClient) -> Arc<dyn SessionProcessingService>,
    {
        let plan = RegistrationPlan::new(config);
        let client =
            iii_sdk::register_worker(&config.iii.engine_url, plan.managed_worker.init_options());
        let service = service_factory(client.clone());

        Self::compose(
            client,
            plan,
            service,
            clock,
            config.concurrency,
            config.shutdown_drain,
        )
    }

    fn compose(
        client: IIIClient,
        plan: RegistrationPlan,
        service: Arc<dyn SessionProcessingService>,
        clock: Arc<dyn Clock>,
        concurrency: u32,
        shutdown_drain: Duration,
    ) -> Result<Self, WorkerStartupError> {
        let admissions = SweepAdmissionController::new(concurrency);
        let function_ref =
            register_sweep_function(&client, &plan, service, clock, admissions.clone());
        let cron = match client.register_trigger(plan.cron.clone()) {
            Ok(cron) => cron,
            Err(_) => {
                client.shutdown();
                return Err(WorkerStartupError::CronRegistrationFailed);
            }
        };

        Ok(Self {
            client: Some(client),
            plan,
            admissions,
            shutdown_drain,
            _function_ref: function_ref,
            _cron: cron,
        })
    }

    pub fn client(&self) -> &IIIClient {
        self.client
            .as_ref()
            .expect("worker client is retained until shutdown")
    }

    pub fn registration_plan(&self) -> &RegistrationPlan {
        &self.plan
    }

    pub async fn wait_until_ready(&self) -> Result<(), WorkerStartupError> {
        self.wait_until_ready_with_timeout(WORKER_READINESS_TIMEOUT)
            .await
    }

    pub async fn wait_until_ready_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<(), WorkerStartupError> {
        let port = IiiRegistrationReadinessPort::new(self.client());
        ReadinessEvaluator::new(&self.plan, &port)
            .wait_until_ready_with_timeout(timeout)
            .await
    }

    pub async fn shutdown(mut self) {
        let deadline = Instant::now() + self.shutdown_drain;
        let _ = self.admissions.close_and_drain(self.shutdown_drain).await;
        if let Some(client) = self.client.take() {
            shutdown_client_before(client, deadline).await;
        }
    }
}

impl Drop for WorkerRuntime {
    fn drop(&mut self) {
        self.admissions.close();
        if let Some(client) = self.client.take() {
            client.shutdown();
        }
    }
}

async fn shutdown_client_before(client: IIIClient, deadline: Instant) {
    client.shutdown_async().await;

    let client_for_join = client.clone();
    let connection_shutdown = std::thread::spawn(move || client_for_join.shutdown());
    loop {
        if connection_shutdown.is_finished() {
            let _ = connection_shutdown.join();
            return;
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            // A healthy client joins above; a stalled SDK handshake cannot extend the drain budget.
            return;
        }
        tokio::time::sleep(remaining.min(CATALOG_POLL_INTERVAL)).await;
    }
}

pub async fn handle_sweep(
    input: CronCallRequest,
    service: &dyn SessionProcessingService,
    clock: &dyn Clock,
) -> Result<SweepOutcome, IiiError> {
    let CronCallRequest {
        trigger: _,
        job_id: _,
        scheduled_time: _,
        actual_time: _,
    } = input;

    service.run_sweep(clock.now()).await.map_err(sweep_failure)
}

pub(crate) async fn handle_admitted_sweep(
    input: CronCallRequest,
    service: &dyn SessionProcessingService,
    clock: &dyn Clock,
    admissions: &SweepAdmissionController,
) -> Result<SweepOutcome, IiiError> {
    let Some(_admission) = admissions.try_admit() else {
        return Ok(empty_sweep_outcome());
    };

    handle_sweep(input, service, clock).await
}

fn empty_sweep_outcome() -> SweepOutcome {
    SweepOutcome::try_from(crate::contracts::SweepOutcomeInput {
        attempted: 0,
        staged: 0,
        completed: 0,
        retryable: 0,
        skipped: 0,
    })
    .expect("zero sweep counts always satisfy the sweep outcome contract")
}

fn register_sweep_function(
    client: &IIIClient,
    plan: &RegistrationPlan,
    service: Arc<dyn SessionProcessingService>,
    clock: Arc<dyn Clock>,
    admissions: SweepAdmissionController,
) -> FunctionRef {
    let function_service = Arc::clone(&service);
    let function_clock = Arc::clone(&clock);
    let function_admissions = admissions.clone();
    client.register_function(
        plan.function.function_id,
        RegisterFunction::new_async(move |input: CronCallRequest| {
            let service = Arc::clone(&function_service);
            let clock = Arc::clone(&function_clock);
            let admissions = function_admissions.clone();
            async move {
                handle_admitted_sweep(input, service.as_ref(), clock.as_ref(), &admissions).await
            }
        })
        .metadata(plan.function.metadata.clone()),
    )
}

fn sweep_failure(error: ProcessingError) -> IiiError {
    IiiError::Remote {
        code: "SESSION_POST_PROCESSING_FAILED".to_owned(),
        message: format!(
            "stage={} category={} retryability={} correlation_id={}",
            error.stage(),
            error.category(),
            error.retryability(),
            error.correlation_id(),
        ),
        stacktrace: None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogRequest {
    pub function_id: String,
    pub payload: Value,
    pub timeout: Duration,
    /// `None` retains the SDK's default engine namespace routing for `engine::*` calls.
    pub namespace: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadinessConnectionState {
    Connected,
    Connecting,
    Disconnected,
    Reconnecting,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadinessPortError {
    RegistrationRejected,
    Disconnected,
    RequestFailed,
    Timeout,
}

#[async_trait]
pub trait RegistrationReadinessPort: Send + Sync {
    fn elapsed(&self) -> Duration;

    fn connection_state(&self) -> ReadinessConnectionState;

    fn has_fatal_error(&self) -> bool;

    async fn wait_until_registered(&self, timeout: Duration) -> Result<(), ReadinessPortError>;

    async fn invoke_catalog(&self, request: CatalogRequest) -> Result<Value, ReadinessPortError>;

    async fn sleep(&self, duration: Duration);
}

pub struct IiiRegistrationReadinessPort<'client> {
    client: &'client IIIClient,
    started_at: Instant,
}

impl<'client> IiiRegistrationReadinessPort<'client> {
    pub fn new(client: &'client IIIClient) -> Self {
        Self {
            client,
            started_at: Instant::now(),
        }
    }

    fn map_error(error: IiiError) -> ReadinessPortError {
        match error {
            IiiError::RegistrationRejected { .. } => ReadinessPortError::RegistrationRejected,
            IiiError::Timeout => ReadinessPortError::Timeout,
            IiiError::NotConnected => ReadinessPortError::Disconnected,
            IiiError::Runtime(_)
            | IiiError::Remote { .. }
            | IiiError::Handler(_)
            | IiiError::Serde(_)
            | IiiError::WebSocket(_) => ReadinessPortError::RequestFailed,
        }
    }
}

#[async_trait]
impl RegistrationReadinessPort for IiiRegistrationReadinessPort<'_> {
    fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    fn connection_state(&self) -> ReadinessConnectionState {
        match self.client.get_connection_state() {
            IIIConnectionState::Connected => ReadinessConnectionState::Connected,
            IIIConnectionState::Connecting => ReadinessConnectionState::Connecting,
            IIIConnectionState::Disconnected => ReadinessConnectionState::Disconnected,
            IIIConnectionState::Reconnecting => ReadinessConnectionState::Reconnecting,
            IIIConnectionState::Failed => ReadinessConnectionState::Failed,
        }
    }

    fn has_fatal_error(&self) -> bool {
        self.client.fatal_error().is_some()
    }

    async fn wait_until_registered(&self, timeout: Duration) -> Result<(), ReadinessPortError> {
        self.client
            .wait_until_registered(timeout)
            .await
            .map_err(Self::map_error)
    }

    async fn invoke_catalog(&self, request: CatalogRequest) -> Result<Value, ReadinessPortError> {
        let CatalogRequest {
            function_id,
            payload,
            timeout,
            namespace,
        } = request;
        let request = TriggerRequest {
            function_id,
            payload,
            action: None,
            timeout_ms: Some(timeout_millis(timeout)),
        };
        let response = match namespace {
            Some(namespace) => self.client.trigger(request.namespace(namespace)).await,
            None => self.client.trigger(request).await,
        };

        response.map_err(Self::map_error)
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum WorkerStartupError {
    #[error("iii worker registration was rejected")]
    RegistrationRejected,
    #[error("worker cron registration failed")]
    CronRegistrationFailed,
    #[error("iii client disconnected during worker startup")]
    Disconnected,
    #[error("registered catalogs do not match the current worker")]
    CatalogMismatch,
    #[error("iii catalog request failed")]
    CatalogRequestFailed,
    #[error("worker startup exceeded its readiness deadline")]
    Timeout,
}

pub struct ReadinessEvaluator<'a, Port: ?Sized> {
    plan: &'a RegistrationPlan,
    port: &'a Port,
}

impl<'a, Port> ReadinessEvaluator<'a, Port>
where
    Port: RegistrationReadinessPort + ?Sized,
{
    pub fn new(plan: &'a RegistrationPlan, port: &'a Port) -> Self {
        Self { plan, port }
    }

    pub async fn wait_until_ready(&self) -> Result<(), WorkerStartupError> {
        self.wait_until_ready_with_timeout(WORKER_READINESS_TIMEOUT)
            .await
    }

    pub async fn wait_until_ready_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<(), WorkerStartupError> {
        let deadline = self.port.elapsed().saturating_add(timeout);
        let remaining = self.remaining(deadline)?;
        if self.port.has_fatal_error() {
            return Err(WorkerStartupError::RegistrationRejected);
        }
        match self.port.wait_until_registered(remaining).await {
            Ok(()) => {}
            Err(error) => {
                self.remaining(deadline)?;
                return Err(self.registration_error(error));
            }
        }
        self.remaining(deadline)?;
        self.ensure_connected()?;

        loop {
            let function_catalog = self
                .invoke_catalog(
                    deadline,
                    EngineFunctions::INFO_FUNCTIONS,
                    json!({
                        "function_ids": [self.plan.function.function_id],
                        "namespace": self.plan.expected_namespace.clone(),
                    }),
                )
                .await?;
            let Some(function) = parse_function_catalog(function_catalog)? else {
                self.wait_for_next_catalog_poll(deadline).await?;
                continue;
            };
            if !function_matches(self.plan, &function) {
                return Err(WorkerStartupError::CatalogMismatch);
            }

            let registered_triggers = self
                .invoke_catalog(
                    deadline,
                    EngineFunctions::LIST_REGISTERED_TRIGGERS,
                    json!({
                        "function_id": self.plan.function.function_id,
                        "trigger_type": CRON_TRIGGER_TYPE,
                        "include_pending": true,
                    }),
                )
                .await?;
            let candidates = parse_registered_trigger_list(registered_triggers)?;
            if candidates.is_empty() {
                self.wait_for_next_catalog_poll(deadline).await?;
                continue;
            }

            for candidate in &candidates {
                let detail = self
                    .invoke_catalog(
                        deadline,
                        EngineFunctions::INFO_REGISTERED_TRIGGERS,
                        json!({"id": candidate.id}),
                    )
                    .await?;
                let detail = parse_registered_trigger_detail(detail)?;
                if !registered_trigger_matches(self.plan, candidate, &detail) {
                    return Err(WorkerStartupError::CatalogMismatch);
                }
            }

            if candidates.len() != 1 {
                return Err(WorkerStartupError::CatalogMismatch);
            }

            self.remaining(deadline)?;
            self.ensure_connected()?;
            return Ok(());
        }
    }

    fn ensure_connected(&self) -> Result<(), WorkerStartupError> {
        if self.port.has_fatal_error() {
            return Err(WorkerStartupError::RegistrationRejected);
        }

        match self.port.connection_state() {
            ReadinessConnectionState::Connected => Ok(()),
            ReadinessConnectionState::Connecting
            | ReadinessConnectionState::Disconnected
            | ReadinessConnectionState::Reconnecting
            | ReadinessConnectionState::Failed => Err(WorkerStartupError::Disconnected),
        }
    }

    fn remaining(&self, deadline: Duration) -> Result<Duration, WorkerStartupError> {
        let remaining = deadline.saturating_sub(self.port.elapsed());
        if remaining.is_zero() {
            Err(WorkerStartupError::Timeout)
        } else {
            Ok(remaining)
        }
    }

    async fn invoke_catalog(
        &self,
        deadline: Duration,
        function_id: &'static str,
        payload: Value,
    ) -> Result<Value, WorkerStartupError> {
        let timeout = self.remaining(deadline)?;
        self.ensure_connected()?;
        let request = CatalogRequest {
            function_id: function_id.to_owned(),
            payload,
            timeout,
            namespace: None,
        };
        match self.port.invoke_catalog(request).await {
            Ok(response) => {
                self.remaining(deadline)?;
                self.ensure_connected()?;
                Ok(response)
            }
            Err(error) => {
                self.remaining(deadline)?;
                Err(self.catalog_error(error))
            }
        }
    }

    async fn wait_for_next_catalog_poll(
        &self,
        deadline: Duration,
    ) -> Result<(), WorkerStartupError> {
        let delay = CATALOG_POLL_INTERVAL.min(self.remaining(deadline)?);
        self.port.sleep(delay).await;
        self.remaining(deadline)?;
        Ok(())
    }

    fn registration_error(&self, error: ReadinessPortError) -> WorkerStartupError {
        if self.port.has_fatal_error() || error == ReadinessPortError::RegistrationRejected {
            WorkerStartupError::RegistrationRejected
        } else if error == ReadinessPortError::Timeout {
            WorkerStartupError::Timeout
        } else {
            WorkerStartupError::Disconnected
        }
    }

    fn catalog_error(&self, error: ReadinessPortError) -> WorkerStartupError {
        if self.port.has_fatal_error() || error == ReadinessPortError::RegistrationRejected {
            WorkerStartupError::RegistrationRejected
        } else if error == ReadinessPortError::Disconnected
            || self.port.connection_state() != ReadinessConnectionState::Connected
        {
            WorkerStartupError::Disconnected
        } else if error == ReadinessPortError::Timeout {
            WorkerStartupError::Timeout
        } else {
            WorkerStartupError::CatalogRequestFailed
        }
    }
}

#[derive(Debug, Deserialize)]
struct FunctionCatalogResponse {
    functions: Vec<Value>,
}

#[derive(Debug, Deserialize)]
struct FunctionCatalogEntry {
    function_id: String,
    namespace: String,
    worker_name: String,
    #[serde(default)]
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct RegisteredTriggerListResponse {
    registered_triggers: Vec<RegisteredTriggerSummary>,
}

#[derive(Debug, Deserialize)]
struct RegisteredTriggerSummary {
    id: String,
    trigger_type: String,
    function_id: String,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum RegisteredTriggerStatus {
    Active,
    Pending,
    Inactive,
}

#[derive(Debug, Deserialize)]
struct TriggerProviderDetail {
    id: String,
    namespace: String,
}

#[derive(Debug, Deserialize)]
struct RegisteredTriggerDetail {
    id: String,
    trigger_type: String,
    function_id: String,
    worker_name: String,
    status: RegisteredTriggerStatus,
    config: Value,
    #[serde(default)]
    metadata: Option<Value>,
    #[serde(default)]
    trigger: Option<TriggerProviderDetail>,
    #[serde(default)]
    function: Option<FunctionCatalogEntry>,
}

fn parse_function_catalog(
    value: Value,
) -> Result<Option<FunctionCatalogEntry>, WorkerStartupError> {
    let response = serde_json::from_value::<FunctionCatalogResponse>(value)
        .map_err(|_| WorkerStartupError::CatalogMismatch)?;
    let mut functions = response.functions.into_iter();
    let Some(function) = functions.next() else {
        return Ok(None);
    };
    if functions.next().is_some() {
        return Err(WorkerStartupError::CatalogMismatch);
    }
    if let Some(error) = function.get("error") {
        return match error.as_str() {
            Some("not_found") => Ok(None),
            _ => Err(WorkerStartupError::CatalogMismatch),
        };
    }

    serde_json::from_value(function)
        .map(Some)
        .map_err(|_| WorkerStartupError::CatalogMismatch)
}

fn parse_registered_trigger_list(
    value: Value,
) -> Result<Vec<RegisteredTriggerSummary>, WorkerStartupError> {
    let response = serde_json::from_value::<RegisteredTriggerListResponse>(value)
        .map_err(|_| WorkerStartupError::CatalogMismatch)?;
    if response.registered_triggers.iter().any(|trigger| {
        trigger.function_id != SWEEP_FUNCTION_ID || trigger.trigger_type != CRON_TRIGGER_TYPE
    }) {
        return Err(WorkerStartupError::CatalogMismatch);
    }
    Ok(response.registered_triggers)
}

fn parse_registered_trigger_detail(
    value: Value,
) -> Result<RegisteredTriggerDetail, WorkerStartupError> {
    serde_json::from_value(value).map_err(|_| WorkerStartupError::CatalogMismatch)
}

fn function_matches(plan: &RegistrationPlan, function: &FunctionCatalogEntry) -> bool {
    function.function_id == plan.function.function_id
        && function.namespace == plan.expected_namespace
        && function.worker_name == plan.expected_worker_name
        && metadata_matches_nonce(function.metadata.as_ref(), plan.nonce)
}

fn registered_trigger_matches(
    plan: &RegistrationPlan,
    candidate: &RegisteredTriggerSummary,
    detail: &RegisteredTriggerDetail,
) -> bool {
    detail.id == candidate.id
        && detail.status == RegisteredTriggerStatus::Active
        && detail.trigger_type == CRON_TRIGGER_TYPE
        && detail.function_id == plan.function.function_id
        && detail.worker_name == plan.expected_worker_name
        && detail.config == plan.cron.config
        && metadata_matches_nonce(detail.metadata.as_ref(), plan.nonce)
        && detail.trigger.as_ref().is_some_and(|trigger| {
            trigger.id == CRON_TRIGGER_TYPE && trigger.namespace == plan.expected_namespace
        })
        && detail.function.as_ref().is_some_and(|function| {
            function.function_id == plan.function.function_id
                && function.namespace == plan.expected_namespace
                && function.worker_name == plan.expected_worker_name
        })
}

fn metadata_matches_nonce(metadata: Option<&Value>, nonce: Uuid) -> bool {
    let nonce = nonce.to_string();
    metadata
        .and_then(Value::as_object)
        .and_then(|metadata| metadata.get(REGISTRATION_NONCE_METADATA_KEY))
        .and_then(Value::as_str)
        == Some(nonce.as_str())
}

fn timeout_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128).max(1) as u64
}

fn registration_metadata(nonce: Uuid, worker_name: &str, namespace: &str) -> Value {
    json!({
        REGISTRATION_NONCE_METADATA_KEY: nonce.to_string(),
        REGISTRATION_WORKER_NAME_METADATA_KEY: worker_name,
        REGISTRATION_NAMESPACE_METADATA_KEY: namespace,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use tokio::sync::Notify;

    use super::*;
    use crate::contracts::ProcessingError;

    struct TestClock;

    impl Clock for TestClock {
        fn now(&self) -> DateTime<Utc> {
            DateTime::UNIX_EPOCH
        }
    }

    struct TestService;

    #[async_trait]
    impl SessionProcessingService for TestService {
        async fn run_sweep(&self, _now: DateTime<Utc>) -> Result<SweepOutcome, ProcessingError> {
            unreachable!("the registration retention test never invokes the handler")
        }
    }

    struct BlockingService {
        calls: AtomicUsize,
        entered: Notify,
        release: Notify,
    }

    impl BlockingService {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                entered: Notify::new(),
                release: Notify::new(),
            }
        }

        async fn wait_for_call(&self) {
            loop {
                if self.calls.load(Ordering::SeqCst) > 0 {
                    return;
                }
                self.entered.notified().await;
            }
        }
    }

    #[async_trait]
    impl SessionProcessingService for BlockingService {
        async fn run_sweep(&self, _now: DateTime<Utc>) -> Result<SweepOutcome, ProcessingError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.release.notified().await;
            SweepOutcome::try_from(crate::contracts::SweepOutcomeInput {
                attempted: 0,
                staged: 0,
                completed: 0,
                retryable: 0,
                skipped: 0,
            })
            .map_err(|_| {
                ProcessingError::new(
                    crate::contracts::FailureCategory::RepositoryInvalidResponse,
                    crate::contracts::CorrelationId::try_from(Uuid::new_v4().to_string())
                        .expect("a UUIDv4 is a valid correlation ID"),
                )
            })
        }
    }

    #[tokio::test]
    async fn admission_bounds_overlapping_sweeps_and_stops_new_work_before_drain() {
        let admissions = SweepAdmissionController::new(1);
        let service = Arc::new(BlockingService::new());
        let clock = Arc::new(TestClock);
        let first_admissions = admissions.clone();
        let first_service = Arc::clone(&service);
        let first_clock = Arc::clone(&clock);
        let first = tokio::spawn(async move {
            handle_admitted_sweep(
                CronCallRequest {
                    trigger: "cron".to_owned(),
                    job_id: "first".to_owned(),
                    scheduled_time: "2026-09-24T00:00:00Z".to_owned(),
                    actual_time: "2026-09-24T00:00:00Z".to_owned(),
                },
                first_service.as_ref(),
                first_clock.as_ref(),
                &first_admissions,
            )
            .await
        });

        tokio::time::timeout(Duration::from_millis(50), service.wait_for_call())
            .await
            .expect("enabled worker composition should admit the first sweep");

        let saturated = handle_admitted_sweep(
            CronCallRequest {
                trigger: "cron".to_owned(),
                job_id: "saturated".to_owned(),
                scheduled_time: "2026-09-24T00:00:00Z".to_owned(),
                actual_time: "2026-09-24T00:00:00Z".to_owned(),
            },
            service.as_ref(),
            clock.as_ref(),
            &admissions,
        )
        .await
        .expect("saturated admission should return a safe empty outcome");
        assert_eq!(saturated.attempted(), 0);
        assert_eq!(service.calls.load(Ordering::SeqCst), 1);

        assert!(
            !admissions.close_and_drain(Duration::from_millis(10)).await,
            "shutdown must wait only through its configured drain deadline"
        );
        let closed = handle_admitted_sweep(
            CronCallRequest {
                trigger: "cron".to_owned(),
                job_id: "closed".to_owned(),
                scheduled_time: "2026-09-24T00:00:00Z".to_owned(),
                actual_time: "2026-09-24T00:00:00Z".to_owned(),
            },
            service.as_ref(),
            clock.as_ref(),
            &admissions,
        )
        .await
        .expect("closed admission should return a safe empty outcome");
        assert_eq!(closed.attempted(), 0);
        assert_eq!(service.calls.load(Ordering::SeqCst), 1);

        service.release.notify_one();
        first
            .await
            .expect("admitted sweep task should not panic")
            .expect("admitted sweep should complete after release");
        assert!(
            admissions.close_and_drain(Duration::from_millis(10)).await,
            "shutdown should drain once admitted work completes"
        );
    }

    #[test]
    fn runtime_composition_retains_one_function_and_cron_handle() {
        let config = Config::from_values([
            (
                "TOTAL_RECALL_POST_PROCESSING_DATABASE".to_owned(),
                "source-session".to_owned(),
            ),
            (
                "TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE".to_owned(),
                "memory".to_owned(),
            ),
            (
                "TOTAL_RECALL_POST_PROCESSING_PROVIDER".to_owned(),
                "openai".to_owned(),
            ),
            (
                "TOTAL_RECALL_POST_PROCESSING_OPENAI_API_TOKEN".to_owned(),
                "test-token".to_owned(),
            ),
            (
                "TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL".to_owned(),
                "test-model".to_owned(),
            ),
        ])
        .expect("test configuration should be valid");
        let client = IIIClient::new("ws://127.0.0.1:0");
        let plan = RegistrationPlan::new(&config);

        let runtime = WorkerRuntime::compose(
            client,
            plan,
            Arc::new(TestService),
            Arc::new(TestClock),
            1,
            Duration::from_secs(1),
        )
        .expect("runtime composition should retain both registrations");

        assert_eq!(runtime._function_ref.id, SWEEP_FUNCTION_ID);
        let _: &Trigger = &runtime._cron;
    }

    #[test]
    fn cron_registration_failure_returns_an_opaque_startup_error() {
        let config = Config::from_values([
            (
                "TOTAL_RECALL_POST_PROCESSING_DATABASE".to_owned(),
                "source-session".to_owned(),
            ),
            (
                "TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE".to_owned(),
                "memory".to_owned(),
            ),
            (
                "TOTAL_RECALL_POST_PROCESSING_PROVIDER".to_owned(),
                "openai".to_owned(),
            ),
            (
                "TOTAL_RECALL_POST_PROCESSING_OPENAI_API_TOKEN".to_owned(),
                "test-token".to_owned(),
            ),
            (
                "TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL".to_owned(),
                "test-model".to_owned(),
            ),
        ])
        .expect("test configuration should be valid");
        let client = IIIClient::new("ws://127.0.0.1:0");
        let mut plan = RegistrationPlan::new(&config);
        plan.cron.namespace = Some(" ".to_owned());

        assert!(matches!(
            WorkerRuntime::compose(
                client,
                plan,
                Arc::new(TestService),
                Arc::new(TestClock),
                1,
                Duration::from_secs(1),
            ),
            Err(WorkerStartupError::CronRegistrationFailed)
        ));
    }
}
