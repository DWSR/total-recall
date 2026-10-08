//! iii registration, readiness, and lifecycle.

use std::{
    borrow::Cow,
    collections::HashSet,
    future::Future,
    io::{self, Read},
    marker::PhantomData,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant as StdInstant},
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
use schemars::{JsonSchema, r#gen::SchemaGenerator, schema::Schema};
use serde::{Deserialize, de};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{Notify, OnceCell, Semaphore, oneshot};
use tokio::time::Instant;
use uuid::Uuid;

use crate::{
    config::{Config, DEFAULT_NAMESPACE, ManagedIiiEnvironment},
    contracts::Deadline,
    event::EventAdapter,
    repository::IiiEmbeddingWorkRepository,
    router::IiiEmbeddingRouter,
    service::{EmbeddingCoordinator, QueueEventProcessor, ReconciliationProcessor},
    writer::IiiEmbeddingWriter,
};

pub const EMBED_VERSIONS_FUNCTION_ID: &str = "memory::embed_versions";
pub const RECONCILE_EMBEDDINGS_FUNCTION_ID: &str = "memory::reconcile_embeddings";
pub const DURABLE_SUBSCRIBER_TRIGGER_TYPE: &str = "durable:subscriber";
pub const REGISTRATION_NONCE_METADATA_KEY: &str = "registration_nonce";
pub const WORKER_READINESS_TIMEOUT: Duration = Duration::from_secs(30);

const SUBSCRIBER_MAX_RETRIES: u32 = 3;
const SUBSCRIBER_BACKOFF_MS: u64 = 1_000;
const CATALOG_POLL_INTERVAL: Duration = Duration::from_millis(100);
const QUEUE_HANDLER_FAILURE: &str = "runtime_queue_handler_failed";
const RECONCILIATION_HANDLER_FAILURE: &str = "runtime_reconciliation_handler_failed";
const HANDLER_ADMISSION_CLOSED: &str = "runtime_handler_admission_closed";
const RECONCILIATION_CALL_DECODE_FAILURE: &str = "runtime_reconciliation_call_malformed";

#[doc(hidden)]
pub fn spawn_detached_eof_watcher<Wait>(
    wait_for_eof: Wait,
) -> io::Result<oneshot::Receiver<io::Result<()>>>
where
    Wait: FnOnce() -> io::Result<()> + Send + 'static,
{
    let (sender, receiver) = oneshot::channel();
    let _ = std::thread::Builder::new()
        .name("memory-embedding-stdin-eof".to_owned())
        .spawn(move || {
            let _ = sender.send(wait_for_eof());
        })?;
    Ok(receiver)
}

#[doc(hidden)]
pub fn spawn_stdin_eof_watcher() -> io::Result<oneshot::Receiver<io::Result<()>>> {
    spawn_detached_eof_watcher(|| {
        let stdin = io::stdin();
        let mut stdin = stdin.lock();
        read_until_eof(&mut stdin)
    })
}

fn read_until_eof<Reader>(reader: &mut Reader) -> io::Result<()>
where
    Reader: Read,
{
    let mut buffer = [0_u8; 1024];
    loop {
        if reader.read(&mut buffer)? == 0 {
            return Ok(());
        }
    }
}

#[doc(hidden)]
pub async fn wait_for_detached_eof(receiver: oneshot::Receiver<io::Result<()>>) -> io::Result<()> {
    receiver
        .await
        .map_err(|_| io::Error::other("stdin EOF watcher stopped"))?
}

#[doc(hidden)]
pub async fn wait_for_termination<Signal, Eof>(signal: Signal, eof: Eof) -> io::Result<()>
where
    Signal: Future<Output = io::Result<()>>,
    Eof: Future<Output = io::Result<()>>,
{
    tokio::select! {
        result = signal => result,
        result = eof => result,
    }
}

pub struct ContentSafeCronCall(CronCallRequest);

impl ContentSafeCronCall {
    fn into_inner(self) -> CronCallRequest {
        self.0
    }
}

impl<'de> Deserialize<'de> for ContentSafeCronCall {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)
            .map_err(|_| de::Error::custom(RECONCILIATION_CALL_DECODE_FAILURE))?;
        serde_json::from_value(value)
            .map(Self)
            .map_err(|_| de::Error::custom(RECONCILIATION_CALL_DECODE_FAILURE))
    }
}

impl JsonSchema for ContentSafeCronCall {
    fn is_referenceable() -> bool {
        CronCallRequest::is_referenceable()
    }

    fn schema_name() -> String {
        CronCallRequest::schema_name()
    }

    fn schema_id() -> Cow<'static, str> {
        CronCallRequest::schema_id()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        CronCallRequest::json_schema(generator)
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
        metadata.namespace = Some(config.iii.namespace.clone());

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

#[derive(Clone)]
pub struct FunctionRegistration<Input> {
    pub function_id: &'static str,
    pub metadata: Value,
    _input: PhantomData<fn(Input)>,
}

pub struct RegistrationPlan {
    pub iii: ManagedIiiEnvironment,
    pub managed_worker: ManagedWorkerConfiguration,
    pub expected_worker_name: String,
    pub expected_namespace: String,
    pub nonce: Uuid,
    pub queue_function: FunctionRegistration<Value>,
    pub reconciliation_function: FunctionRegistration<ContentSafeCronCall>,
    pub subscriber: RegisterTriggerInput,
    pub cron: RegisterTriggerInput,
}

pub struct RuntimeRegistrations {
    queue_function: FunctionRef,
    reconciliation_function: FunctionRef,
    subscriber: Trigger,
    cron: Trigger,
}

impl RuntimeRegistrations {
    pub fn queue_function(&self) -> &FunctionRef {
        &self.queue_function
    }

    pub fn reconciliation_function(&self) -> &FunctionRef {
        &self.reconciliation_function
    }

    pub fn subscriber(&self) -> &Trigger {
        &self.subscriber
    }

    pub fn cron(&self) -> &Trigger {
        &self.cron
    }

    fn release(self) {
        self.subscriber.unregister();
        self.cron.unregister();
        self.queue_function.unregister();
        self.reconciliation_function.unregister();
    }
}

pub struct AdmissionGate {
    accepting: Mutex<bool>,
}

impl AdmissionGate {
    pub fn new() -> Self {
        Self {
            accepting: Mutex::new(true),
        }
    }

    pub fn close(&self) {
        *self
            .accepting
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = false;
    }

    pub fn is_open(&self) -> bool {
        *self
            .accepting
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn admit<'a>(&self, tracker: &'a TaskTracker) -> Option<TaskGuard<'a>> {
        let accepting = self
            .accepting
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (*accepting).then(|| tracker.track())
    }
}

impl Default for AdmissionGate {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TaskTracker {
    active_tasks: AtomicUsize,
    drained: Notify,
}

impl TaskTracker {
    pub fn new() -> Self {
        Self {
            active_tasks: AtomicUsize::new(0),
            drained: Notify::new(),
        }
    }

    pub fn active_tasks(&self) -> usize {
        self.active_tasks.load(Ordering::Acquire)
    }

    fn track(&self) -> TaskGuard<'_> {
        self.active_tasks.fetch_add(1, Ordering::AcqRel);
        TaskGuard { tracker: self }
    }

    #[doc(hidden)]
    pub async fn wait_for_drain(&self) {
        loop {
            if self.active_tasks() == 0 {
                return;
            }

            let notified = self.drained.notified();
            if self.active_tasks() == 0 {
                return;
            }
            notified.await;
        }
    }
}

impl Default for TaskTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[must_use]
struct TaskGuard<'a> {
    tracker: &'a TaskTracker,
}

impl Drop for TaskGuard<'_> {
    fn drop(&mut self) {
        if self.tracker.active_tasks.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.tracker.drained.notify_waiters();
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RuntimeShutdownError {
    #[error("runtime_shutdown_drain_timeout")]
    DrainTimeout,
    #[error("runtime_client_shutdown_failed")]
    ClientShutdown,
}

#[async_trait]
#[doc(hidden)]
pub trait RuntimeLifecyclePort: Send + Sync {
    fn release_registrations(&self);

    async fn drain(&self, task_tracker: &TaskTracker, timeout: Duration) -> bool;

    async fn shutdown_client(&self) -> Result<(), RuntimeShutdownError>;
}

#[async_trait]
#[doc(hidden)]
pub trait RuntimeClientShutdown: Send + Sync {
    async fn shutdown_client(&self) -> Result<(), RuntimeShutdownError>;
}

#[doc(hidden)]
pub async fn shutdown_with_port<Port>(
    admission_gate: &AdmissionGate,
    task_tracker: &TaskTracker,
    shutdown: &OnceCell<Result<(), RuntimeShutdownError>>,
    timeout: Duration,
    port: &Port,
) -> Result<(), RuntimeShutdownError>
where
    Port: RuntimeLifecyclePort + ?Sized,
{
    *shutdown
        .get_or_init(|| async {
            admission_gate.close();
            port.release_registrations();
            let drained = port.drain(task_tracker, timeout).await;
            let cleanup = port.shutdown_client().await;

            match cleanup {
                Err(error) => Err(error),
                Ok(()) if drained => Ok(()),
                Ok(()) => Err(RuntimeShutdownError::DrainTimeout),
            }
        })
        .await
}

pub struct RuntimeHandlers<Repository, Router, Writer> {
    queue: QueueEventProcessor<Repository, Router, Writer>,
    reconciliation: ReconciliationProcessor<Repository, Router, Writer>,
    invocation_timeout: Duration,
    admission_gate: Arc<AdmissionGate>,
    task_tracker: Arc<TaskTracker>,
}

impl<Repository, Router, Writer> RuntimeHandlers<Repository, Router, Writer> {
    pub fn new(config: &Config, repository: Repository, router: Router, writer: Writer) -> Self {
        let repository = Arc::new(repository);
        let router_permits = Arc::new(Semaphore::new(config.max_in_flight));
        let coordinator = Arc::new(EmbeddingCoordinator::new(
            router,
            writer,
            config.batch_limit,
            config.max_input_bytes,
            router_permits,
        ));

        Self {
            queue: QueueEventProcessor::with_shared(
                EventAdapter::new(config),
                Arc::clone(&repository),
                Arc::clone(&coordinator),
            ),
            reconciliation: ReconciliationProcessor::with_shared(
                repository,
                coordinator,
                config.reconciliation_limit,
            ),
            invocation_timeout: config.invocation_timeout,
            admission_gate: Arc::new(AdmissionGate::new()),
            task_tracker: Arc::new(TaskTracker::new()),
        }
    }

    pub fn admission_gate(&self) -> Arc<AdmissionGate> {
        Arc::clone(&self.admission_gate)
    }

    pub fn task_tracker(&self) -> Arc<TaskTracker> {
        Arc::clone(&self.task_tracker)
    }

    fn deadline(&self, error: &'static str) -> Result<Deadline, IiiError> {
        StdInstant::now()
            .checked_add(self.invocation_timeout)
            .map(Deadline::at)
            .ok_or_else(|| handler_error(error))
    }
}

impl<Repository, Router, Writer> RuntimeHandlers<Repository, Router, Writer>
where
    Repository: crate::contracts::EmbeddingWorkRepository,
    Router: crate::contracts::EmbeddingRouter,
    Writer: crate::contracts::EmbeddingWriter,
{
    pub async fn handle_queue(&self, payload: Value) -> Result<Value, IiiError> {
        let Some(_task) = self.admission_gate.admit(&self.task_tracker) else {
            return Err(handler_error(HANDLER_ADMISSION_CLOSED));
        };
        let deadline = self.deadline(QUEUE_HANDLER_FAILURE)?;
        let outcome = self
            .queue
            .process_event(payload, deadline)
            .await
            .map_err(|_| handler_error(QUEUE_HANDLER_FAILURE))?;

        serde_json::to_value(outcome).map_err(|_| handler_error(QUEUE_HANDLER_FAILURE))
    }

    pub async fn handle_reconciliation(&self, call: CronCallRequest) -> Result<Value, IiiError> {
        let Some(_task) = self.admission_gate.admit(&self.task_tracker) else {
            return Err(handler_error(HANDLER_ADMISSION_CLOSED));
        };
        let deadline = self.deadline(RECONCILIATION_HANDLER_FAILURE)?;
        let outcome = self
            .reconciliation
            .reconcile(call, deadline)
            .await
            .map_err(|_| handler_error(RECONCILIATION_HANDLER_FAILURE))?;

        serde_json::to_value(outcome).map_err(|_| handler_error(RECONCILIATION_HANDLER_FAILURE))
    }
}

fn handler_error(message: &'static str) -> IiiError {
    IiiError::Handler(message.to_owned())
}

#[doc(hidden)]
pub type RegisteredHandlerFuture = Pin<Box<dyn Future<Output = Result<Value, IiiError>> + Send>>;

#[doc(hidden)]
pub fn registered_reconciliation_handler<Repository, Router, Writer>(
    handlers: Arc<RuntimeHandlers<Repository, Router, Writer>>,
) -> impl Fn(ContentSafeCronCall) -> RegisteredHandlerFuture + Send + Sync + 'static
where
    Repository: crate::contracts::EmbeddingWorkRepository + 'static,
    Router: crate::contracts::EmbeddingRouter + 'static,
    Writer: crate::contracts::EmbeddingWriter + 'static,
{
    move |call| {
        let handlers = Arc::clone(&handlers);
        Box::pin(async move { handlers.handle_reconciliation(call.into_inner()).await })
    }
}

struct IiiRuntimeClient<'a> {
    client: &'a IIIClient,
}

#[async_trait]
impl RuntimeClientShutdown for IiiRuntimeClient<'_> {
    async fn shutdown_client(&self) -> Result<(), RuntimeShutdownError> {
        let client = self.client.clone();
        tokio::task::spawn_blocking(move || client.shutdown())
            .await
            .map_err(|_| RuntimeShutdownError::ClientShutdown)
    }
}

#[doc(hidden)]
pub struct IiiRuntimeLifecycle<'a, Client: ?Sized> {
    registrations: &'a Mutex<Option<RuntimeRegistrations>>,
    client: &'a Client,
}

impl<'a, Client: ?Sized> IiiRuntimeLifecycle<'a, Client> {
    pub fn new(registrations: &'a Mutex<Option<RuntimeRegistrations>>, client: &'a Client) -> Self {
        Self {
            registrations,
            client,
        }
    }
}

#[async_trait]
impl<Client> RuntimeLifecyclePort for IiiRuntimeLifecycle<'_, Client>
where
    Client: RuntimeClientShutdown + ?Sized,
{
    fn release_registrations(&self) {
        let registrations = self
            .registrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(registrations) = registrations {
            registrations.release();
        }
    }

    async fn drain(&self, task_tracker: &TaskTracker, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, task_tracker.wait_for_drain())
            .await
            .is_ok()
    }

    async fn shutdown_client(&self) -> Result<(), RuntimeShutdownError> {
        self.client.shutdown_client().await
    }
}

pub struct WorkerRuntime {
    client: IIIClient,
    plan: RegistrationPlan,
    registrations: Mutex<Option<RuntimeRegistrations>>,
    handlers:
        Arc<RuntimeHandlers<IiiEmbeddingWorkRepository, IiiEmbeddingRouter, IiiEmbeddingWriter>>,
    shutdown: OnceCell<Result<(), RuntimeShutdownError>>,
}

impl WorkerRuntime {
    pub fn start(config: &Config) -> Result<Self, RuntimeRegistrationError> {
        let plan = RegistrationPlan::new(config);
        let client =
            iii_sdk::register_worker(&plan.iii.engine_url, plan.managed_worker.init_options());
        let handlers = Arc::new(RuntimeHandlers::new(
            config,
            IiiEmbeddingWorkRepository::new(
                client.clone(),
                config.database.clone(),
                config.database_timeout,
            ),
            IiiEmbeddingRouter::new(
                client.clone(),
                config.provider.clone(),
                config.model.clone(),
                config.router_timeout,
            ),
            IiiEmbeddingWriter::new(
                client.clone(),
                config.database.clone(),
                config.database_timeout,
            ),
        ));
        Self::compose_with_cleanup(client, plan, handlers, IIIClient::shutdown)
    }

    #[doc(hidden)]
    pub fn compose_with_cleanup<Cleanup>(
        client: IIIClient,
        plan: RegistrationPlan,
        handlers: Arc<
            RuntimeHandlers<IiiEmbeddingWorkRepository, IiiEmbeddingRouter, IiiEmbeddingWriter>,
        >,
        cleanup: Cleanup,
    ) -> Result<Self, RuntimeRegistrationError>
    where
        Cleanup: FnOnce(&IIIClient),
    {
        let queue_handlers = Arc::clone(&handlers);
        let registrations = match plan.register(
            &client,
            move |payload| {
                let handlers = Arc::clone(&queue_handlers);
                async move { handlers.handle_queue(payload).await }
            },
            registered_reconciliation_handler(Arc::clone(&handlers)),
        ) {
            Ok(registrations) => registrations,
            Err(error) => {
                cleanup(&client);
                return Err(error);
            }
        };

        Ok(Self {
            client,
            plan,
            registrations: Mutex::new(Some(registrations)),
            handlers,
            shutdown: OnceCell::new(),
        })
    }

    pub fn client(&self) -> &IIIClient {
        &self.client
    }

    pub fn registration_plan(&self) -> &RegistrationPlan {
        &self.plan
    }

    pub fn admission_gate(&self) -> Arc<AdmissionGate> {
        self.handlers.admission_gate()
    }

    pub fn task_tracker(&self) -> Arc<TaskTracker> {
        self.handlers.task_tracker()
    }

    pub async fn shutdown(&self, timeout: Duration) -> Result<(), RuntimeShutdownError> {
        let client = IiiRuntimeClient {
            client: &self.client,
        };
        let lifecycle = IiiRuntimeLifecycle::new(&self.registrations, &client);
        shutdown_with_port(
            &self.handlers.admission_gate,
            &self.handlers.task_tracker,
            &self.shutdown,
            timeout,
            &lifecycle,
        )
        .await
    }

    pub async fn wait_until_ready(&self) -> Result<(), RuntimeReadinessError> {
        self.wait_until_ready_with_timeout(WORKER_READINESS_TIMEOUT)
            .await
    }

    pub async fn wait_until_ready_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<(), RuntimeReadinessError> {
        let port = IiiRegistrationReadinessPort::new(&self.client);
        Self::wait_until_ready_with_port(&self.plan, &port, timeout).await
    }

    #[doc(hidden)]
    pub async fn wait_until_ready_with_port<Port>(
        plan: &RegistrationPlan,
        port: &Port,
        timeout: Duration,
    ) -> Result<(), RuntimeReadinessError>
    where
        Port: RegistrationReadinessPort + ?Sized,
    {
        ReadinessEvaluator::new(plan, port)
            .wait_until_ready_with_timeout(timeout)
            .await
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RuntimeRegistrationError {
    #[error("runtime_trigger_registration_failed")]
    TriggerRegistration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogRequest {
    pub function_id: String,
    pub payload: Value,
    pub timeout: Duration,
    /// `None` uses the SDK's default engine namespace routing for `engine::*` calls.
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

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RuntimeReadinessError {
    #[error("runtime_registration_rejected")]
    RegistrationRejected,
    #[error("runtime_startup_disconnected")]
    Disconnected,
    #[error("runtime_catalog_mismatch")]
    CatalogMismatch,
    #[error("runtime_catalog_request_failed")]
    CatalogRequestFailed,
    #[error("runtime_readiness_timeout")]
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

    pub async fn wait_until_ready(&self) -> Result<(), RuntimeReadinessError> {
        self.wait_until_ready_with_timeout(WORKER_READINESS_TIMEOUT)
            .await
    }

    pub async fn wait_until_ready_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<(), RuntimeReadinessError> {
        let deadline = self.port.elapsed().saturating_add(timeout);
        if self.port.has_fatal_error() {
            return Err(RuntimeReadinessError::RegistrationRejected);
        }

        let remaining = self.remaining(deadline)?;
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
            let functions = self
                .invoke_catalog(
                    deadline,
                    EngineFunctions::INFO_FUNCTIONS,
                    json!({
                        "function_ids": [
                            self.plan.queue_function.function_id,
                            self.plan.reconciliation_function.function_id,
                        ],
                        "namespace": self.plan.expected_namespace.clone(),
                    }),
                )
                .await?;
            if function_catalog_state(self.plan, functions)? == CatalogState::Pending {
                self.wait_for_next_catalog_poll(deadline).await?;
                continue;
            }

            let mut trigger_pending = false;
            let mut trigger_ids = HashSet::new();
            for trigger in [&self.plan.subscriber, &self.plan.cron] {
                let registered_triggers = self
                    .invoke_catalog(
                        deadline,
                        EngineFunctions::LIST_REGISTERED_TRIGGERS,
                        json!({
                            "function_id": trigger.function_id.clone(),
                            "trigger_type": trigger.trigger_type.clone(),
                            "include_pending": true,
                        }),
                    )
                    .await?;
                let Some(candidate) = registered_trigger_candidate(registered_triggers, trigger)?
                else {
                    trigger_pending = true;
                    break;
                };
                if !trigger_ids.insert(candidate.id.clone()) {
                    return Err(RuntimeReadinessError::CatalogMismatch);
                }

                let detail = self
                    .invoke_catalog(
                        deadline,
                        EngineFunctions::INFO_REGISTERED_TRIGGERS,
                        json!({"id": candidate.id}),
                    )
                    .await?;
                let detail = parse_registered_trigger_detail(detail)?;
                if !registered_trigger_matches(self.plan, trigger, &candidate, &detail) {
                    return Err(RuntimeReadinessError::CatalogMismatch);
                }
            }

            if trigger_pending {
                self.wait_for_next_catalog_poll(deadline).await?;
                continue;
            }

            self.remaining(deadline)?;
            self.ensure_connected()?;
            return Ok(());
        }
    }

    fn ensure_connected(&self) -> Result<(), RuntimeReadinessError> {
        if self.port.has_fatal_error() {
            return Err(RuntimeReadinessError::RegistrationRejected);
        }

        match self.port.connection_state() {
            ReadinessConnectionState::Connected => Ok(()),
            ReadinessConnectionState::Connecting
            | ReadinessConnectionState::Disconnected
            | ReadinessConnectionState::Reconnecting
            | ReadinessConnectionState::Failed => Err(RuntimeReadinessError::Disconnected),
        }
    }

    fn remaining(&self, deadline: Duration) -> Result<Duration, RuntimeReadinessError> {
        let remaining = deadline.saturating_sub(self.port.elapsed());
        if remaining.is_zero() {
            Err(RuntimeReadinessError::Timeout)
        } else {
            Ok(remaining)
        }
    }

    async fn invoke_catalog(
        &self,
        deadline: Duration,
        function_id: &'static str,
        payload: Value,
    ) -> Result<Value, RuntimeReadinessError> {
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
    ) -> Result<(), RuntimeReadinessError> {
        let delay = CATALOG_POLL_INTERVAL.min(self.remaining(deadline)?);
        self.port.sleep(delay).await;
        self.remaining(deadline)?;
        self.ensure_connected()
    }

    fn registration_error(&self, error: ReadinessPortError) -> RuntimeReadinessError {
        if self.port.has_fatal_error() || error == ReadinessPortError::RegistrationRejected {
            RuntimeReadinessError::RegistrationRejected
        } else if error == ReadinessPortError::Timeout {
            RuntimeReadinessError::Timeout
        } else {
            RuntimeReadinessError::Disconnected
        }
    }

    fn catalog_error(&self, error: ReadinessPortError) -> RuntimeReadinessError {
        if self.port.has_fatal_error() || error == ReadinessPortError::RegistrationRejected {
            RuntimeReadinessError::RegistrationRejected
        } else if error == ReadinessPortError::Disconnected
            || self.port.connection_state() != ReadinessConnectionState::Connected
        {
            RuntimeReadinessError::Disconnected
        } else if error == ReadinessPortError::Timeout {
            RuntimeReadinessError::Timeout
        } else {
            RuntimeReadinessError::CatalogRequestFailed
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct FunctionCatalogResponse {
    functions: Vec<Value>,
}

#[derive(Debug, serde::Deserialize)]
struct FunctionCatalogEntry {
    function_id: String,
    namespace: String,
    worker_name: String,
    #[serde(default)]
    metadata: Option<Value>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct UnavailableFunctionCatalogEntry {
    function_id: String,
    error: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CatalogState {
    Pending,
    Ready,
}

#[derive(Debug, serde::Deserialize)]
struct RegisteredTriggerListResponse {
    registered_triggers: Vec<RegisteredTriggerSummary>,
}

#[derive(Debug, serde::Deserialize)]
struct RegisteredTriggerSummary {
    id: String,
    trigger_type: String,
    function_id: String,
}

#[derive(Debug, serde::Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum RegisteredTriggerStatus {
    Active,
    Pending,
}

#[derive(Debug, serde::Deserialize)]
struct TriggerProviderDetail {
    id: String,
    namespace: String,
}

#[derive(Debug, serde::Deserialize)]
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

fn function_catalog_state(
    plan: &RegistrationPlan,
    value: Value,
) -> Result<CatalogState, RuntimeReadinessError> {
    let response = serde_json::from_value::<FunctionCatalogResponse>(value)
        .map_err(|_| RuntimeReadinessError::CatalogMismatch)?;
    let expected_ids = [
        plan.queue_function.function_id,
        plan.reconciliation_function.function_id,
    ];
    let mut seen = HashSet::new();
    let mut owned_functions = 0;
    let mut pending = false;

    for row in response.functions {
        if row.get("error").is_some() {
            let unavailable = serde_json::from_value::<UnavailableFunctionCatalogEntry>(row)
                .map_err(|_| RuntimeReadinessError::CatalogMismatch)?;
            if !matches!(unavailable.error.as_str(), "pending" | "not_found")
                || !expected_ids.contains(&unavailable.function_id.as_str())
                || !seen.insert(unavailable.function_id)
            {
                return Err(RuntimeReadinessError::CatalogMismatch);
            }
            pending = true;
            continue;
        }

        let function = serde_json::from_value::<FunctionCatalogEntry>(row)
            .map_err(|_| RuntimeReadinessError::CatalogMismatch)?;
        if !expected_ids.contains(&function.function_id.as_str())
            || !seen.insert(function.function_id.clone())
        {
            return Err(RuntimeReadinessError::CatalogMismatch);
        }
        if !function_matches(plan, &function) {
            return Err(RuntimeReadinessError::CatalogMismatch);
        }
        owned_functions += 1;
    }

    if pending || owned_functions != expected_ids.len() {
        Ok(CatalogState::Pending)
    } else {
        Ok(CatalogState::Ready)
    }
}

fn registered_trigger_candidate(
    value: Value,
    expected: &RegisterTriggerInput,
) -> Result<Option<RegisteredTriggerSummary>, RuntimeReadinessError> {
    let response = serde_json::from_value::<RegisteredTriggerListResponse>(value)
        .map_err(|_| RuntimeReadinessError::CatalogMismatch)?;
    if response.registered_triggers.is_empty() {
        return Ok(None);
    }
    if response.registered_triggers.len() != 1 {
        return Err(RuntimeReadinessError::CatalogMismatch);
    }

    let candidate = response
        .registered_triggers
        .into_iter()
        .next()
        .expect("a non-empty trigger list has one candidate");
    if candidate.id.is_empty()
        || candidate.trigger_type != expected.trigger_type
        || candidate.function_id != expected.function_id
    {
        return Err(RuntimeReadinessError::CatalogMismatch);
    }

    Ok(Some(candidate))
}

fn parse_registered_trigger_detail(
    value: Value,
) -> Result<RegisteredTriggerDetail, RuntimeReadinessError> {
    serde_json::from_value(value).map_err(|_| RuntimeReadinessError::CatalogMismatch)
}

fn function_matches(plan: &RegistrationPlan, function: &FunctionCatalogEntry) -> bool {
    matches!(
        function.function_id.as_str(),
        EMBED_VERSIONS_FUNCTION_ID | RECONCILE_EMBEDDINGS_FUNCTION_ID
    ) && function.namespace == plan.expected_namespace
        && function.worker_name == plan.expected_worker_name
        && metadata_matches_nonce(function.metadata.as_ref(), plan.nonce)
}

fn registered_trigger_matches(
    plan: &RegistrationPlan,
    expected: &RegisterTriggerInput,
    candidate: &RegisteredTriggerSummary,
    detail: &RegisteredTriggerDetail,
) -> bool {
    detail.id == candidate.id
        && detail.status == RegisteredTriggerStatus::Active
        && detail.trigger_type == expected.trigger_type
        && detail.function_id == expected.function_id
        && detail.worker_name == plan.expected_worker_name
        && detail.config == expected.config
        && metadata_matches_nonce(detail.metadata.as_ref(), plan.nonce)
        && detail.trigger.as_ref().is_some_and(|trigger| {
            trigger.id == expected.trigger_type && trigger.namespace == plan.expected_namespace
        })
        && detail.function.as_ref().is_some_and(|function| {
            function.function_id == expected.function_id
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
        let metadata = json!({REGISTRATION_NONCE_METADATA_KEY: nonce.to_string()});
        let subscriber = RegisterTriggerInput::new(
            DURABLE_SUBSCRIBER_TRIGGER_TYPE,
            EMBED_VERSIONS_FUNCTION_ID,
            json!({
                "queue": config.queue_topic,
                "max_retries": SUBSCRIBER_MAX_RETRIES,
                "backoff_ms": SUBSCRIBER_BACKOFF_MS,
                "queue_config": {
                    "type": "concurrent",
                    "concurrency": config.max_in_flight,
                },
            }),
        )
        .with_metadata(metadata.clone())
        .in_namespace(DEFAULT_NAMESPACE)
        .in_trigger_namespace(DEFAULT_NAMESPACE);
        let cron = IIITrigger::Cron(CronTriggerConfig::new(&config.cron_expression))
            .for_function(RECONCILE_EMBEDDINGS_FUNCTION_ID)
            .with_metadata(metadata.clone())
            .in_namespace(DEFAULT_NAMESPACE)
            .in_trigger_namespace(DEFAULT_NAMESPACE);

        Self {
            iii: config.iii.clone(),
            managed_worker,
            expected_worker_name,
            expected_namespace,
            nonce,
            queue_function: FunctionRegistration {
                function_id: EMBED_VERSIONS_FUNCTION_ID,
                metadata: metadata.clone(),
                _input: PhantomData,
            },
            reconciliation_function: FunctionRegistration {
                function_id: RECONCILE_EMBEDDINGS_FUNCTION_ID,
                metadata,
                _input: PhantomData,
            },
            subscriber,
            cron,
        }
    }

    pub fn register<QueueHandler, QueueFuture, ReconciliationHandler, ReconciliationFuture>(
        &self,
        client: &IIIClient,
        queue_handler: QueueHandler,
        reconciliation_handler: ReconciliationHandler,
    ) -> Result<RuntimeRegistrations, RuntimeRegistrationError>
    where
        QueueHandler: Fn(Value) -> QueueFuture + Send + Sync + 'static,
        QueueFuture: Future<Output = Result<Value, IiiError>> + Send + 'static,
        ReconciliationHandler:
            Fn(ContentSafeCronCall) -> ReconciliationFuture + Send + Sync + 'static,
        ReconciliationFuture: Future<Output = Result<Value, IiiError>> + Send + 'static,
    {
        let queue_function = client.register_function(
            self.queue_function.function_id,
            RegisterFunction::new_async(queue_handler)
                .metadata(self.queue_function.metadata.clone()),
        );
        let reconciliation_function = client.register_function(
            self.reconciliation_function.function_id,
            RegisterFunction::new_async(reconciliation_handler)
                .metadata(self.reconciliation_function.metadata.clone()),
        );
        let subscriber = client
            .register_trigger(self.subscriber.clone())
            .map_err(|_| RuntimeRegistrationError::TriggerRegistration)?;
        let cron = client
            .register_trigger(self.cron.clone())
            .map_err(|_| RuntimeRegistrationError::TriggerRegistration)?;

        Ok(RuntimeRegistrations {
            queue_function,
            reconciliation_function,
            subscriber,
            cron,
        })
    }
}
