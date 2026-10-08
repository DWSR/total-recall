use std::{
    collections::VecDeque,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use iii_sdk::{
    Error as IiiError, IIIClient, WorkerIdentityMode,
    builtin_triggers::CronCallRequest,
    engine::EngineFunctions,
    iii::IntoAsyncHandler,
    protocol::RegisterTriggerInput,
    runtime::{FunctionRef, WorkerMetadata},
    trigger::Trigger,
};
use memory_embedding::{
    IiiEmbeddingRouter, IiiEmbeddingWorkRepository, IiiEmbeddingWriter,
    config::{
        Config, DEFAULT_NAMESPACE, III_NAMESPACE_ENV, III_URL_ENV, III_WORKER_NAME_ENV,
        TOTAL_RECALL_EMBEDDING_CRON_EXPRESSION_ENV, TOTAL_RECALL_EMBEDDING_DATABASE_ENV,
        TOTAL_RECALL_EMBEDDING_MAX_IN_FLIGHT_ENV, TOTAL_RECALL_EMBEDDING_MODEL_ENV,
        TOTAL_RECALL_EMBEDDING_PROVIDER_ENV, TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC_ENV,
    },
    contracts::{
        EmbeddingError, EmbeddingRouter, EmbeddingWorkItem, FailingEmbeddingWorkRepository,
        GeneratedEmbedding, LoadedEmbeddingWork, MemoryKey, RecordingEmbeddingRouter,
        RecordingEmbeddingWorkRepository, RecordingEmbeddingWriter, RepositoryCall,
        RepositoryFailure, RepositoryFailures, RepositoryResponses, RouterError, RouterResponses,
        WriteOutcome, WriterResponses,
    },
    runtime::{
        CatalogRequest, ContentSafeCronCall, EMBED_VERSIONS_FUNCTION_ID, FunctionRegistration,
        IiiRegistrationReadinessPort, IiiRuntimeLifecycle, RECONCILE_EMBEDDINGS_FUNCTION_ID,
        REGISTRATION_NONCE_METADATA_KEY, ReadinessConnectionState, ReadinessEvaluator,
        ReadinessPortError, RegisteredHandlerFuture, RegistrationPlan, RegistrationReadinessPort,
        RuntimeClientShutdown, RuntimeHandlers, RuntimeLifecyclePort, RuntimeReadinessError,
        RuntimeRegistrationError, RuntimeRegistrations, RuntimeShutdownError,
        WORKER_READINESS_TIMEOUT, WorkerRuntime, registered_reconciliation_handler,
        shutdown_with_port, spawn_detached_eof_watcher, wait_for_detached_eof,
        wait_for_termination,
    },
};
use schemars::schema_for;
use serde_json::{Value, json};
use tokio::sync::{Notify, OnceCell, Semaphore, oneshot};
use uuid::Uuid;

const WORKER_NAME: &str = "memory-embedding";
const ENGINE_URL: &str = "ws://127.0.0.1:49134";
const TOPIC: &str = "memory-embedding";
const CRON_EXPRESSION: &str = "0 */5 * * * *";

fn config(max_in_flight: usize) -> Config {
    config_with_worker_name(max_in_flight, Some(WORKER_NAME))
}

fn config_with_worker_name(max_in_flight: usize, worker_name: Option<&str>) -> Config {
    let mut values = vec![
        (
            TOTAL_RECALL_EMBEDDING_DATABASE_ENV.to_owned(),
            "memory".to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC_ENV.to_owned(),
            TOPIC.to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_CRON_EXPRESSION_ENV.to_owned(),
            CRON_EXPRESSION.to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_PROVIDER_ENV.to_owned(),
            "openai".to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_MODEL_ENV.to_owned(),
            "text-embedding-3-small".to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_MAX_IN_FLIGHT_ENV.to_owned(),
            max_in_flight.to_string(),
        ),
        (III_URL_ENV.to_owned(), ENGINE_URL.to_owned()),
        (III_NAMESPACE_ENV.to_owned(), DEFAULT_NAMESPACE.to_owned()),
    ];
    if let Some(worker_name) = worker_name {
        values.push((III_WORKER_NAME_ENV.to_owned(), worker_name.to_owned()));
    }

    Config::from_values(values).expect("runtime test configuration should be valid")
}

fn assert_queue_payload(_: &FunctionRegistration<Value>) {}

fn assert_reconciliation_payload(_: &FunctionRegistration<ContentSafeCronCall>) {}

fn assert_function_ref(_: &FunctionRef) {}

fn assert_trigger(_: &Trigger) {}

async fn queue_handler(_: Value) -> Result<Value, IiiError> {
    Ok(Value::Null)
}

async fn reconciliation_handler(_: ContentSafeCronCall) -> Result<Value, IiiError> {
    Ok(Value::Null)
}

const RUNTIME_DATABASE: &str = "runtime-database-sentinel";
const RUNTIME_PROVIDER: &str = "runtime-provider-sentinel";
const RUNTIME_MODEL: &str = "runtime-model-sentinel";
const RUNTIME_MEMORY_ID: &str = "runtime-memory-id-sentinel";
const RUNTIME_MEMORY_TITLE: &str = "runtime-memory-title-sentinel";
const RUNTIME_MEMORY_CONTENT: &str = "runtime-memory-content-sentinel";
const RUNTIME_MEMORY_CONCEPT: &str = "runtime-memory-concept-sentinel";
const RUNTIME_CRON_JOB: &str = "runtime-cron-job-sentinel";
const RUNTIME_CRON_PAYLOAD: &str = "runtime-cron-payload-sentinel";

fn runtime_config(max_in_flight: usize) -> Config {
    Config::from_values([
        (
            TOTAL_RECALL_EMBEDDING_DATABASE_ENV.to_owned(),
            RUNTIME_DATABASE.to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC_ENV.to_owned(),
            "runtime-memory-inserts".to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_PROVIDER_ENV.to_owned(),
            RUNTIME_PROVIDER.to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_MODEL_ENV.to_owned(),
            RUNTIME_MODEL.to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_MAX_IN_FLIGHT_ENV.to_owned(),
            max_in_flight.to_string(),
        ),
    ])
    .expect("runtime composition configuration should be valid")
}

fn runtime_work_item() -> EmbeddingWorkItem {
    EmbeddingWorkItem::new(
        MemoryKey::try_new(RUNTIME_MEMORY_ID, "7").expect("runtime key should be valid"),
        RUNTIME_MEMORY_TITLE,
        RUNTIME_MEMORY_CONTENT,
        vec![RUNTIME_MEMORY_CONCEPT.to_owned()],
    )
}

fn runtime_generated(item: &EmbeddingWorkItem) -> GeneratedEmbedding {
    GeneratedEmbedding::try_new(item.key().clone(), vec![1.0])
        .expect("runtime generated vector should be valid")
}

fn runtime_queue_payload() -> Value {
    json!({
        "db": RUNTIME_DATABASE,
        "table": "public.memories",
        "op": "insert",
        "affected_rows": 1,
        "returning": [{"id": RUNTIME_MEMORY_ID, "version": "7"}],
        "at": 0,
    })
}

fn runtime_cron_call() -> CronCallRequest {
    CronCallRequest {
        trigger: "cron".to_owned(),
        job_id: RUNTIME_CRON_JOB.to_owned(),
        scheduled_time: "2026-09-23T00:00:00Z".to_owned(),
        actual_time: "2026-09-23T00:00:01Z".to_owned(),
    }
}

fn runtime_repository(item: &EmbeddingWorkItem) -> RecordingEmbeddingWorkRepository {
    RecordingEmbeddingWorkRepository::new(RepositoryResponses {
        load_keys: Ok(vec![LoadedEmbeddingWork::Pending(item.clone())]),
        list_missing: Ok(vec![item.clone()]),
    })
}

fn runtime_writer() -> RecordingEmbeddingWriter {
    RecordingEmbeddingWriter::new(WriterResponses {
        insert: Ok(WriteOutcome::Stored),
    })
}

fn assert_runtime_content_safe(output: &str) {
    for protected in [
        RUNTIME_DATABASE,
        RUNTIME_PROVIDER,
        RUNTIME_MODEL,
        RUNTIME_MEMORY_ID,
        RUNTIME_MEMORY_TITLE,
        RUNTIME_MEMORY_CONTENT,
        RUNTIME_MEMORY_CONCEPT,
        RUNTIME_CRON_JOB,
        RUNTIME_CRON_PAYLOAD,
    ] {
        assert!(
            !output.contains(protected),
            "runtime output leaked protected sentinel {protected}: {output}"
        );
    }
}

fn handler_message(error: IiiError) -> String {
    match error {
        IiiError::Handler(message) => message,
        other => panic!("handler should return iii handler errors: {other:?}"),
    }
}

fn serde_message(error: IiiError) -> String {
    match error {
        IiiError::Serde(message) => message,
        other => panic!("registered handler should return iii serde errors: {other:?}"),
    }
}

fn production_runtime_handlers(
    config: &Config,
    client: &IIIClient,
) -> Arc<RuntimeHandlers<IiiEmbeddingWorkRepository, IiiEmbeddingRouter, IiiEmbeddingWriter>> {
    Arc::new(RuntimeHandlers::new(
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
    ))
}

#[derive(Clone)]
struct BlockingRuntimeRouter {
    state: Arc<Mutex<BlockingRuntimeRouterState>>,
    release_first: Arc<Semaphore>,
}

struct BlockingRuntimeRouterState {
    calls: usize,
    first_started: Option<oneshot::Sender<()>>,
}

impl BlockingRuntimeRouter {
    fn new() -> (Self, oneshot::Receiver<()>) {
        let (first_started, receiver) = oneshot::channel();

        (
            Self {
                state: Arc::new(Mutex::new(BlockingRuntimeRouterState {
                    calls: 0,
                    first_started: Some(first_started),
                })),
                release_first: Arc::new(Semaphore::new(0)),
            },
            receiver,
        )
    }

    fn calls(&self) -> usize {
        self.state
            .lock()
            .expect("runtime router state lock should not be poisoned")
            .calls
    }

    fn release_first(&self) {
        self.release_first.add_permits(1);
    }
}

#[async_trait]
impl EmbeddingRouter for BlockingRuntimeRouter {
    async fn embed(
        &self,
        inputs: &[memory_embedding::contracts::CanonicalEmbeddingInput],
        _: memory_embedding::contracts::Deadline,
    ) -> Result<Vec<GeneratedEmbedding>, RouterError> {
        let first_started = {
            let mut state = self
                .state
                .lock()
                .expect("runtime router state lock should not be poisoned");
            state.calls += 1;
            (state.calls == 1).then(|| {
                state
                    .first_started
                    .take()
                    .expect("first router call should have a reporter")
            })
        };

        if let Some(first_started) = first_started {
            let _ = first_started.send(());
            let _permit = self
                .release_first
                .acquire()
                .await
                .expect("runtime router release semaphore should remain open");
        }

        inputs
            .iter()
            .map(|input| GeneratedEmbedding::try_new(input.key().clone(), vec![1.0]))
            .collect()
    }
}

#[derive(Clone, Copy)]
enum FakeDrainBehavior {
    WaitForTracker,
    TimedOut,
    WaitForRelease,
}

#[derive(Clone)]
struct FakeLifecyclePort {
    admission_gate: Arc<memory_embedding::runtime::AdmissionGate>,
    state: Arc<Mutex<FakeLifecycleState>>,
    drain_started: Arc<Notify>,
    release_drain: Arc<Semaphore>,
}

struct FakeLifecycleState {
    drain_behavior: FakeDrainBehavior,
    cleanup_result: Result<(), RuntimeShutdownError>,
    events: Vec<&'static str>,
    drain_timeouts: Vec<Duration>,
    release_calls: usize,
    cleanup_calls: usize,
    release_observed_closed_gate: bool,
}

impl FakeLifecyclePort {
    fn new(
        admission_gate: Arc<memory_embedding::runtime::AdmissionGate>,
        drain_behavior: FakeDrainBehavior,
        cleanup_result: Result<(), RuntimeShutdownError>,
    ) -> Self {
        Self {
            admission_gate,
            state: Arc::new(Mutex::new(FakeLifecycleState {
                drain_behavior,
                cleanup_result,
                events: Vec::new(),
                drain_timeouts: Vec::new(),
                release_calls: 0,
                cleanup_calls: 0,
                release_observed_closed_gate: false,
            })),
            drain_started: Arc::new(Notify::new()),
            release_drain: Arc::new(Semaphore::new(0)),
        }
    }

    async fn wait_for_drain(&self) {
        self.drain_started.notified().await;
    }

    fn release_drain(&self) {
        self.release_drain.add_permits(1);
    }

    fn events(&self) -> Vec<&'static str> {
        self.state
            .lock()
            .expect("fake lifecycle state lock should not be poisoned")
            .events
            .clone()
    }

    fn drain_timeouts(&self) -> Vec<Duration> {
        self.state
            .lock()
            .expect("fake lifecycle state lock should not be poisoned")
            .drain_timeouts
            .clone()
    }

    fn release_calls(&self) -> usize {
        self.state
            .lock()
            .expect("fake lifecycle state lock should not be poisoned")
            .release_calls
    }

    fn cleanup_calls(&self) -> usize {
        self.state
            .lock()
            .expect("fake lifecycle state lock should not be poisoned")
            .cleanup_calls
    }

    fn release_observed_closed_gate(&self) -> bool {
        self.state
            .lock()
            .expect("fake lifecycle state lock should not be poisoned")
            .release_observed_closed_gate
    }
}

#[async_trait]
impl RuntimeLifecyclePort for FakeLifecyclePort {
    fn release_registrations(&self) {
        let mut state = self
            .state
            .lock()
            .expect("fake lifecycle state lock should not be poisoned");
        state.events.push("release_registrations");
        state.release_calls += 1;
        state.release_observed_closed_gate = !self.admission_gate.is_open();
    }

    async fn drain(
        &self,
        task_tracker: &memory_embedding::runtime::TaskTracker,
        timeout: Duration,
    ) -> bool {
        let drain_behavior = {
            let mut state = self
                .state
                .lock()
                .expect("fake lifecycle state lock should not be poisoned");
            state.events.push("drain");
            state.drain_timeouts.push(timeout);
            state.drain_behavior
        };
        self.drain_started.notify_one();

        match drain_behavior {
            FakeDrainBehavior::WaitForTracker => {
                task_tracker.wait_for_drain().await;
                true
            }
            FakeDrainBehavior::TimedOut => false,
            FakeDrainBehavior::WaitForRelease => {
                let _permit = self
                    .release_drain
                    .acquire()
                    .await
                    .expect("fake lifecycle release semaphore should remain open");
                true
            }
        }
    }

    async fn shutdown_client(&self) -> Result<(), RuntimeShutdownError> {
        let mut state = self
            .state
            .lock()
            .expect("fake lifecycle state lock should not be poisoned");
        state.events.push("shutdown_client");
        state.cleanup_calls += 1;
        state.cleanup_result
    }
}

#[derive(Default)]
struct SpyRuntimeClient {
    shutdown_calls: AtomicUsize,
}

impl SpyRuntimeClient {
    fn shutdown_calls(&self) -> usize {
        self.shutdown_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl RuntimeClientShutdown for SpyRuntimeClient {
    async fn shutdown_client(&self) -> Result<(), RuntimeShutdownError> {
        self.shutdown_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn runtime_registrations(
    config: &Config,
    client: &IIIClient,
) -> Mutex<Option<RuntimeRegistrations>> {
    let plan = RegistrationPlan::new(config);
    Mutex::new(Some(
        plan.register(client, queue_handler, reconciliation_handler)
            .expect("concrete lifecycle test registration should succeed"),
    ))
}

#[tokio::test]
async fn runtime_handlers_dispatch_both_paths_with_content_free_outcomes() {
    let config = runtime_config(1);
    let item = runtime_work_item();
    let repository = runtime_repository(&item);
    let router = RecordingEmbeddingRouter::new(RouterResponses {
        embed: Ok(vec![runtime_generated(&item)]),
    });
    let writer = runtime_writer();
    let handlers =
        RuntimeHandlers::new(&config, repository.clone(), router.clone(), writer.clone());

    let queue = handlers
        .handle_queue(runtime_queue_payload())
        .await
        .expect("queue handler should dispatch the shared embedding pipeline");
    tokio::time::sleep(Duration::from_millis(1)).await;
    let reconciliation = handlers
        .handle_reconciliation(runtime_cron_call())
        .await
        .expect("reconciliation handler should dispatch the shared embedding pipeline");

    let expected = json!({
        "selected": 1,
        "generated": 1,
        "already_present": 0,
        "stored": 1,
    });
    assert_eq!(queue, expected);
    assert_eq!(reconciliation, expected);
    assert_runtime_content_safe(&queue.to_string());
    assert_runtime_content_safe(&reconciliation.to_string());
    assert_eq!(router.calls().len(), 2);
    assert_eq!(writer.calls().len(), 2);

    let calls = repository.calls();
    let [
        RepositoryCall::LoadKeys {
            deadline: queue_deadline,
            ..
        },
        RepositoryCall::ListMissing {
            deadline: reconciliation_deadline,
            ..
        },
    ] = calls.as_slice()
    else {
        panic!("both handlers should use one shared repository: {calls:?}");
    };
    assert!(
        reconciliation_deadline > queue_deadline,
        "each admitted handler should compute its own invocation deadline"
    );
}

#[tokio::test]
async fn registered_cron_boundary_is_content_safe_and_preserves_the_cron_contract() {
    let config = runtime_config(1);
    let item = runtime_work_item();
    let repository = runtime_repository(&item);
    let router = RecordingEmbeddingRouter::new(RouterResponses {
        embed: Ok(vec![runtime_generated(&item)]),
    });
    let writer = runtime_writer();
    let handlers = Arc::new(RuntimeHandlers::new(
        &config,
        repository.clone(),
        router.clone(),
        writer.clone(),
    ));
    let plan = RegistrationPlan::new(&config);
    let registration_client = IIIClient::new("ws://127.0.0.1:0");

    plan.register(
        &registration_client,
        queue_handler,
        registered_reconciliation_handler(Arc::clone(&handlers)),
    )
    .expect("the production registration accepts the content-safe cron handler");

    let registered = registered_reconciliation_handler(handlers);
    let registered = <_ as IntoAsyncHandler<(
        ContentSafeCronCall,
        RegisteredHandlerFuture,
        Value,
    )>>::into_handler(registered);
    let expected = json!({
        "selected": 1,
        "generated": 1,
        "already_present": 0,
        "stored": 1,
    });
    assert_eq!(
        registered(
            serde_json::to_value(runtime_cron_call())
                .expect("cron fixture should serialize for the SDK boundary"),
            None,
        )
        .await
        .expect("registered cron handler should dispatch its validated call"),
        expected
    );
    assert_eq!(repository.calls().len(), 1);
    assert_eq!(router.calls().len(), 1);
    assert_eq!(writer.calls().len(), 1);

    let error = registered(json!(RUNTIME_CRON_PAYLOAD), None)
        .await
        .expect_err("a non-object cron payload must fail in the registered SDK handler");
    let wire_diagnostic = error.to_string();
    let diagnostic = serde_message(error);
    assert_eq!(diagnostic, "runtime_reconciliation_call_malformed");
    assert_runtime_content_safe(&wire_diagnostic);
    assert_runtime_content_safe(&diagnostic);
    assert_eq!(repository.calls().len(), 1);
    assert_eq!(router.calls().len(), 1);
    assert_eq!(writer.calls().len(), 1);
    assert_eq!(
        serde_json::to_value(schema_for!(ContentSafeCronCall))
            .expect("content-safe cron schema should serialize"),
        serde_json::to_value(schema_for!(CronCallRequest))
            .expect("SDK cron schema should serialize")
    );
}

#[test]
fn startup_registration_failure_runs_the_composition_cleanup_seam() {
    let config = runtime_config(1);
    let client = IIIClient::new("ws://127.0.0.1:0");
    let handlers = production_runtime_handlers(&config, &client);
    let mut plan = RegistrationPlan::new(&config);
    plan.cron.namespace = Some(" ".to_owned());
    let cleanup_called = Arc::new(AtomicBool::new(false));
    let cleanup_flag = Arc::clone(&cleanup_called);

    let Err(error) = WorkerRuntime::compose_with_cleanup(client, plan, handlers, move |_| {
        cleanup_flag.store(true, Ordering::SeqCst);
    }) else {
        panic!("invalid cron trigger registration must fail composition");
    };

    assert_eq!(error, RuntimeRegistrationError::TriggerRegistration);
    assert_runtime_content_safe(&error.to_string());
    assert!(
        cleanup_called.load(Ordering::SeqCst),
        "a plan.register trigger failure must shut down the started client"
    );
}

#[tokio::test]
async fn runtime_handlers_map_failures_to_fixed_content_safe_iii_errors() {
    let config = runtime_config(1);
    let item = runtime_work_item();
    let repository = FailingEmbeddingWorkRepository::new(RepositoryFailures {
        load_keys: EmbeddingError::repository(RepositoryFailure::Backend),
        list_missing: EmbeddingError::repository(RepositoryFailure::Backend),
    });
    let router = RecordingEmbeddingRouter::new(RouterResponses {
        embed: Ok(vec![runtime_generated(&item)]),
    });
    let handlers = RuntimeHandlers::new(&config, repository, router, runtime_writer());

    let queue_error = handlers
        .handle_queue(runtime_queue_payload())
        .await
        .expect_err("queue failure should become an iii handler error");
    let queue_diagnostic = format!("{queue_error:?}");
    assert_eq!(handler_message(queue_error), "runtime_queue_handler_failed");
    assert_runtime_content_safe(&queue_diagnostic);

    let reconciliation_error = handlers
        .handle_reconciliation(runtime_cron_call())
        .await
        .expect_err("reconciliation failure should become an iii handler error");
    let reconciliation_diagnostic = format!("{reconciliation_error:?}");
    assert_eq!(
        handler_message(reconciliation_error),
        "runtime_reconciliation_handler_failed"
    );
    assert_runtime_content_safe(&reconciliation_diagnostic);
}

#[tokio::test]
async fn runtime_handlers_share_router_permits_and_track_admitted_work() {
    let config = runtime_config(1);
    let item = runtime_work_item();
    let repository = runtime_repository(&item);
    let (router, first_router_started) = BlockingRuntimeRouter::new();
    let handlers = Arc::new(RuntimeHandlers::new(
        &config,
        repository,
        router.clone(),
        runtime_writer(),
    ));

    let first_handlers = Arc::clone(&handlers);
    let queue =
        tokio::spawn(async move { first_handlers.handle_queue(runtime_queue_payload()).await });
    tokio::time::timeout(Duration::from_secs(1), first_router_started)
        .await
        .expect("queue handler should reach the router")
        .expect("queue router start notifier should remain open");
    assert_eq!(handlers.task_tracker().active_tasks(), 1);

    let second_handlers = Arc::clone(&handlers);
    let reconciliation = tokio::spawn(async move {
        second_handlers
            .handle_reconciliation(runtime_cron_call())
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if handlers.task_tracker().active_tasks() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both handlers should be tracked while one waits for the shared permit");
    assert_eq!(router.calls(), 1);

    handlers.admission_gate().close();
    assert!(!handlers.admission_gate().is_open());
    let closed_error = handlers
        .handle_queue(runtime_queue_payload())
        .await
        .expect_err("a closed gate must reject later handler admission");
    assert_eq!(
        handler_message(closed_error),
        "runtime_handler_admission_closed"
    );
    assert_eq!(router.calls(), 1);

    router.release_first();
    queue
        .await
        .expect("queue handler task should not panic")
        .expect("queue handler should complete after router release");
    reconciliation
        .await
        .expect("reconciliation handler task should not panic")
        .expect("reconciliation handler should complete after router release");
    assert_eq!(router.calls(), 2);
    assert_eq!(handlers.task_tracker().active_tasks(), 0);
}

#[tokio::test]
async fn lifecycle_closes_admission_releases_registrations_and_drains_admitted_work() {
    let config = runtime_config(1);
    let item = runtime_work_item();
    let (router, first_router_started) = BlockingRuntimeRouter::new();
    let handlers = Arc::new(RuntimeHandlers::new(
        &config,
        runtime_repository(&item),
        router.clone(),
        runtime_writer(),
    ));
    let queue_handlers = Arc::clone(&handlers);
    let queue =
        tokio::spawn(async move { queue_handlers.handle_queue(runtime_queue_payload()).await });
    tokio::time::timeout(Duration::from_secs(1), first_router_started)
        .await
        .expect("the admitted queue handler should reach the router")
        .expect("the router start notifier should remain open");

    let admission_gate = handlers.admission_gate();
    let task_tracker = handlers.task_tracker();
    let shutdown = Arc::new(OnceCell::new());
    let port = FakeLifecyclePort::new(
        Arc::clone(&admission_gate),
        FakeDrainBehavior::WaitForTracker,
        Ok(()),
    );
    let shutdown_task = {
        let admission_gate = Arc::clone(&admission_gate);
        let task_tracker = Arc::clone(&task_tracker);
        let shutdown = Arc::clone(&shutdown);
        let port = port.clone();
        tokio::spawn(async move {
            shutdown_with_port(
                &admission_gate,
                &task_tracker,
                &shutdown,
                config.shutdown_timeout,
                &port,
            )
            .await
        })
    };

    tokio::time::timeout(Duration::from_secs(1), port.wait_for_drain())
        .await
        .expect("shutdown should release registrations before it starts draining");
    assert!(!admission_gate.is_open());
    assert!(port.release_observed_closed_gate());
    assert_eq!(port.release_calls(), 1);
    assert!(
        !shutdown_task.is_finished(),
        "shutdown must wait for work admitted before closure"
    );

    let closed_error = handlers
        .handle_queue(runtime_queue_payload())
        .await
        .expect_err("admission closure must reject later queue work");
    assert_eq!(
        handler_message(closed_error),
        "runtime_handler_admission_closed"
    );

    router.release_first();
    queue
        .await
        .expect("queue task should not panic")
        .expect("admitted queue work should finish after the router releases");
    assert_eq!(
        shutdown_task.await.expect("shutdown task should not panic"),
        Ok(())
    );
    assert_eq!(
        port.events(),
        vec!["release_registrations", "drain", "shutdown_client"]
    );
    assert_eq!(port.cleanup_calls(), 1);
}

#[tokio::test]
async fn lifecycle_times_out_the_drain_but_always_cleans_up_the_client() {
    let admission_gate = Arc::new(memory_embedding::runtime::AdmissionGate::new());
    let task_tracker = Arc::new(memory_embedding::runtime::TaskTracker::new());
    let shutdown = OnceCell::new();
    let timeout = Duration::from_millis(17);
    let port = FakeLifecyclePort::new(
        Arc::clone(&admission_gate),
        FakeDrainBehavior::TimedOut,
        Ok(()),
    );

    assert_eq!(
        shutdown_with_port(&admission_gate, &task_tracker, &shutdown, timeout, &port,).await,
        Err(RuntimeShutdownError::DrainTimeout)
    );
    assert!(!admission_gate.is_open());
    assert!(port.release_observed_closed_gate());
    assert_eq!(port.drain_timeouts(), vec![timeout]);
    assert_eq!(
        port.events(),
        vec!["release_registrations", "drain", "shutdown_client"]
    );
    assert_eq!(port.cleanup_calls(), 1);
}

#[tokio::test]
async fn lifecycle_is_idempotent_for_concurrent_shutdown_requests() {
    let admission_gate = Arc::new(memory_embedding::runtime::AdmissionGate::new());
    let task_tracker = Arc::new(memory_embedding::runtime::TaskTracker::new());
    let shutdown = Arc::new(OnceCell::new());
    let port = FakeLifecyclePort::new(
        Arc::clone(&admission_gate),
        FakeDrainBehavior::WaitForRelease,
        Ok(()),
    );

    let first = {
        let admission_gate = Arc::clone(&admission_gate);
        let task_tracker = Arc::clone(&task_tracker);
        let shutdown = Arc::clone(&shutdown);
        let port = port.clone();
        tokio::spawn(async move {
            shutdown_with_port(
                &admission_gate,
                &task_tracker,
                &shutdown,
                Duration::from_secs(1),
                &port,
            )
            .await
        })
    };
    let second = {
        let admission_gate = Arc::clone(&admission_gate);
        let task_tracker = Arc::clone(&task_tracker);
        let shutdown = Arc::clone(&shutdown);
        let port = port.clone();
        tokio::spawn(async move {
            shutdown_with_port(
                &admission_gate,
                &task_tracker,
                &shutdown,
                Duration::from_secs(1),
                &port,
            )
            .await
        })
    };

    tokio::time::timeout(Duration::from_secs(1), port.wait_for_drain())
        .await
        .expect("one shutdown request should own the drain");
    assert!(!admission_gate.is_open());
    assert_eq!(port.release_calls(), 1);
    assert_eq!(port.cleanup_calls(), 0);

    port.release_drain();
    assert_eq!(
        first.await.expect("first shutdown task should not panic"),
        Ok(())
    );
    assert_eq!(
        second.await.expect("second shutdown task should not panic"),
        Ok(())
    );
    assert_eq!(port.release_calls(), 1);
    assert_eq!(port.cleanup_calls(), 1);
}

#[tokio::test(start_paused = true)]
async fn concrete_lifecycle_times_out_real_tracker_and_shuts_down_client() {
    let config = runtime_config(1);
    let item = runtime_work_item();
    let (router, first_router_started) = BlockingRuntimeRouter::new();
    let handlers = Arc::new(RuntimeHandlers::new(
        &config,
        runtime_repository(&item),
        router.clone(),
        runtime_writer(),
    ));
    let queue_handlers = Arc::clone(&handlers);
    let queue =
        tokio::spawn(async move { queue_handlers.handle_queue(runtime_queue_payload()).await });
    first_router_started
        .await
        .expect("the admitted queue handler should reach the router");

    let registration_client = IIIClient::new("ws://127.0.0.1:0");
    let registrations = runtime_registrations(&config, &registration_client);
    let client = SpyRuntimeClient::default();
    let lifecycle = IiiRuntimeLifecycle::new(&registrations, &client);
    let shutdown = OnceCell::new();
    let timeout = Duration::from_secs(1);
    let admission_gate = handlers.admission_gate();
    let task_tracker = handlers.task_tracker();
    let shutdown = shutdown_with_port(
        &admission_gate,
        &task_tracker,
        &shutdown,
        timeout,
        &lifecycle,
    );
    tokio::pin!(shutdown);

    tokio::select! {
        biased;
        result = &mut shutdown => panic!("shutdown must wait for admitted work: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    assert!(!handlers.admission_gate().is_open());
    assert!(
        registrations
            .lock()
            .expect("registration lock should not be poisoned")
            .is_none(),
        "concrete lifecycle should release real registration handles before draining"
    );

    tokio::time::advance(timeout).await;
    assert_eq!(
        shutdown.await,
        Err(RuntimeShutdownError::DrainTimeout),
        "the concrete lifecycle must use the real task tracker drain deadline"
    );
    assert_eq!(
        client.shutdown_calls(),
        1,
        "client cleanup must run after a concrete drain timeout"
    );

    router.release_first();
    queue
        .await
        .expect("queue handler task should not panic")
        .expect("admitted queue work should finish after cleanup");
}

#[tokio::test]
async fn concrete_lifecycle_client_cleanup_is_idempotent() {
    let config = runtime_config(1);
    let item = runtime_work_item();
    let handlers = RuntimeHandlers::new(
        &config,
        runtime_repository(&item),
        RecordingEmbeddingRouter::new(RouterResponses {
            embed: Ok(vec![runtime_generated(&item)]),
        }),
        runtime_writer(),
    );
    let registration_client = IIIClient::new("ws://127.0.0.1:0");
    let registrations = runtime_registrations(&config, &registration_client);
    let client = SpyRuntimeClient::default();
    let lifecycle = IiiRuntimeLifecycle::new(&registrations, &client);
    let shutdown = OnceCell::new();
    let admission_gate = handlers.admission_gate();
    let task_tracker = handlers.task_tracker();

    let (first, second) = tokio::join!(
        shutdown_with_port(
            &admission_gate,
            &task_tracker,
            &shutdown,
            Duration::from_secs(1),
            &lifecycle,
        ),
        shutdown_with_port(
            &admission_gate,
            &task_tracker,
            &shutdown,
            Duration::from_secs(1),
            &lifecycle,
        ),
    );

    assert_eq!(first, Ok(()));
    assert_eq!(second, Ok(()));
    assert!(!handlers.admission_gate().is_open());
    assert!(
        registrations
            .lock()
            .expect("registration lock should not be poisoned")
            .is_none()
    );
    assert_eq!(client.shutdown_calls(), 1);
}

#[tokio::test]
async fn signal_wins_while_detached_eof_watcher_remains_open() {
    let (started_sender, started) = oneshot::channel();
    let (finished_sender, finished) = oneshot::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let watcher_release = Arc::clone(&release);
    let watcher = spawn_detached_eof_watcher(move || {
        let _ = started_sender.send(());
        let (released, wake) = &*watcher_release;
        let mut released = released
            .lock()
            .expect("detached EOF watcher release lock should not be poisoned");
        while !*released {
            released = wake
                .wait(released)
                .expect("detached EOF watcher release lock should not be poisoned");
        }
        let _ = finished_sender.send(());
        Ok(())
    })
    .expect("detached EOF watcher should start");

    let terminated = wait_for_termination(
        async move {
            started
                .await
                .expect("detached EOF watcher should report that it is pending");
            Ok(())
        },
        wait_for_detached_eof(watcher),
    );
    assert!(terminated.await.is_ok());
    assert!(
        !*release
            .0
            .lock()
            .expect("detached EOF watcher release lock should not be poisoned"),
        "the signal path must return while EOF remains pending"
    );

    *release
        .0
        .lock()
        .expect("detached EOF watcher release lock should not be poisoned") = true;
    release.1.notify_one();
    finished
        .await
        .expect("detached EOF watcher should finish after its reader is released");
}

#[test]
fn lifecycle_errors_are_fixed_content_safe_runtime_categories() {
    assert_eq!(
        RuntimeShutdownError::DrainTimeout.to_string(),
        "runtime_shutdown_drain_timeout"
    );
    assert_eq!(
        RuntimeShutdownError::ClientShutdown.to_string(),
        "runtime_client_shutdown_failed"
    );
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
        self.state.lock().unwrap().connection = connection;
    }

    fn set_connection_after_registration(&self, connection: ReadinessConnectionState) {
        self.state.lock().unwrap().connection_after_registration = Some(connection);
    }

    fn set_fatal(&self) {
        self.state.lock().unwrap().fatal = true;
    }

    fn registration_timeouts(&self) -> Vec<Duration> {
        self.state.lock().unwrap().registration_timeouts.clone()
    }

    fn requests(&self) -> Vec<CatalogRequest> {
        self.state.lock().unwrap().requests.clone()
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

    async fn invoke_catalog(&self, request: CatalogRequest) -> Result<Value, ReadinessPortError> {
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

fn function_catalog_entry(plan: &RegistrationPlan, function_id: &str, nonce: Uuid) -> Value {
    json!({
        "function_id": function_id,
        "namespace": plan.expected_namespace,
        "worker_name": plan.expected_worker_name,
        "metadata": {REGISTRATION_NONCE_METADATA_KEY: nonce.to_string()},
    })
}

fn function_catalog(plan: &RegistrationPlan, nonce: Uuid) -> Value {
    json!({
        "functions": [
            function_catalog_entry(plan, EMBED_VERSIONS_FUNCTION_ID, nonce),
            function_catalog_entry(plan, RECONCILE_EMBEDDINGS_FUNCTION_ID, nonce),
        ],
    })
}

fn registered_trigger_list(trigger: &RegisterTriggerInput, ids: &[&str]) -> Value {
    json!({
        "registered_triggers": ids.iter().map(|id| json!({
            "id": id,
            "trigger_type": trigger.trigger_type,
            "function_id": trigger.function_id,
        })).collect::<Vec<_>>(),
    })
}

fn registered_trigger_detail(
    plan: &RegistrationPlan,
    trigger: &RegisterTriggerInput,
    id: &str,
    status: &str,
    nonce: Uuid,
) -> Value {
    json!({
        "id": id,
        "trigger_type": trigger.trigger_type,
        "function_id": trigger.function_id,
        "worker_name": plan.expected_worker_name,
        "status": status,
        "config": trigger.config,
        "metadata": {REGISTRATION_NONCE_METADATA_KEY: nonce.to_string()},
        "trigger": {
            "id": trigger.trigger_type,
            "namespace": DEFAULT_NAMESPACE,
        },
        "function": {
            "function_id": trigger.function_id,
            "namespace": DEFAULT_NAMESPACE,
            "worker_name": plan.expected_worker_name,
        },
    })
}

fn ready_calls(plan: &RegistrationPlan) -> Vec<Timed<Result<Value, ReadinessPortError>>> {
    vec![
        Timed::immediate(Ok(function_catalog(plan, plan.nonce))),
        Timed::immediate(Ok(registered_trigger_list(
            &plan.subscriber,
            &["subscriber"],
        ))),
        Timed::immediate(Ok(registered_trigger_detail(
            plan,
            &plan.subscriber,
            "subscriber",
            "active",
            plan.nonce,
        ))),
        Timed::immediate(Ok(registered_trigger_list(&plan.cron, &["cron"]))),
        Timed::immediate(Ok(registered_trigger_detail(
            plan, &plan.cron, "cron", "active", plan.nonce,
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
        &config(4),
        Uuid::parse_str("63ec9af3-668e-4ea6-9bfe-a17b5f0c3c75")
            .expect("fixture nonce should be valid"),
    )
}

async fn assert_catalog_mismatch(
    plan: &RegistrationPlan,
    calls: Vec<Timed<Result<Value, ReadinessPortError>>>,
    expected_request_functions: &[&str],
) {
    let port = ScriptedReadinessPort::new(Timed::immediate(Ok(())), calls);

    assert_eq!(
        ReadinessEvaluator::new(plan, &port)
            .wait_until_ready()
            .await,
        Err(RuntimeReadinessError::CatalogMismatch)
    );
    assert_eq!(
        port.requests()
            .into_iter()
            .map(|request| request.function_id)
            .collect::<Vec<_>>(),
        expected_request_functions
            .iter()
            .map(|function_id| (*function_id).to_owned())
            .collect::<Vec<_>>()
    );
}

#[test]
fn registration_plan_describes_two_typed_functions_and_exact_trigger_bindings() {
    let nonce = Uuid::parse_str("c1f7195d-5339-4f4b-9b13-8efca53cfbdc")
        .expect("fixture nonce should be valid");
    let plan = RegistrationPlan::with_nonce(&config(7), nonce);
    let expected_metadata = json!({REGISTRATION_NONCE_METADATA_KEY: nonce.to_string()});

    assert_queue_payload(&plan.queue_function);
    assert_reconciliation_payload(&plan.reconciliation_function);
    assert_eq!(plan.queue_function.function_id, EMBED_VERSIONS_FUNCTION_ID);
    assert_eq!(
        plan.reconciliation_function.function_id,
        RECONCILE_EMBEDDINGS_FUNCTION_ID
    );
    assert_eq!(plan.queue_function.metadata, expected_metadata);
    assert_eq!(plan.reconciliation_function.metadata, expected_metadata);

    assert_eq!(plan.subscriber.trigger_type, "durable:subscriber");
    assert_eq!(plan.subscriber.function_id, EMBED_VERSIONS_FUNCTION_ID);
    assert_eq!(
        plan.subscriber.config,
        json!({
            "queue": TOPIC,
            "max_retries": 3,
            "backoff_ms": 1_000,
            "queue_config": {
                "type": "concurrent",
                "concurrency": 7,
            },
        })
    );
    assert_eq!(plan.subscriber.metadata.as_ref(), Some(&expected_metadata));
    assert_eq!(
        plan.subscriber.namespace.as_deref(),
        Some(DEFAULT_NAMESPACE)
    );
    assert_eq!(
        plan.subscriber.trigger_namespace.as_deref(),
        Some(DEFAULT_NAMESPACE)
    );

    assert_eq!(plan.cron.trigger_type, "cron");
    assert_eq!(plan.cron.function_id, RECONCILE_EMBEDDINGS_FUNCTION_ID);
    assert_eq!(plan.cron.config, json!({"expression": CRON_EXPRESSION}));
    assert_eq!(plan.cron.metadata.as_ref(), Some(&expected_metadata));
    assert_eq!(plan.cron.namespace.as_deref(), Some(DEFAULT_NAMESPACE));
    assert_eq!(
        plan.cron.trigger_namespace.as_deref(),
        Some(DEFAULT_NAMESPACE)
    );

    assert_eq!(plan.iii.engine_url, ENGINE_URL);
    assert_eq!(plan.iii.worker_name.as_deref(), Some(WORKER_NAME));
    assert_eq!(plan.iii.namespace, DEFAULT_NAMESPACE);
    assert_eq!(plan.nonce, nonce);
}

#[test]
fn registration_plan_generates_one_v4_nonce_for_all_registrations() {
    let plan = RegistrationPlan::new(&config(4));
    let nonce = json!(plan.nonce.to_string());

    assert_eq!(plan.nonce.get_version_num(), 4);
    assert_eq!(
        plan.queue_function.metadata[REGISTRATION_NONCE_METADATA_KEY],
        nonce
    );
    assert_eq!(
        plan.reconciliation_function.metadata[REGISTRATION_NONCE_METADATA_KEY],
        nonce
    );
    assert_eq!(
        plan.subscriber
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get(REGISTRATION_NONCE_METADATA_KEY)),
        Some(&plan.queue_function.metadata[REGISTRATION_NONCE_METADATA_KEY])
    );
    assert_eq!(
        plan.cron
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get(REGISTRATION_NONCE_METADATA_KEY)),
        Some(&plan.queue_function.metadata[REGISTRATION_NONCE_METADATA_KEY])
    );
}

#[test]
fn registration_plan_registers_two_functions_two_triggers_and_retains_handles() {
    let plan = RegistrationPlan::with_nonce(
        &config(4),
        Uuid::parse_str("a9ff52c3-45db-40a7-bc47-9c963b063b64")
            .expect("fixture nonce should be valid"),
    );
    let client = IIIClient::new("ws://127.0.0.1:0");

    let registrations = plan
        .register(&client, queue_handler, reconciliation_handler)
        .expect("registration should retain both functions and triggers");

    assert_eq!(
        registrations.queue_function().id,
        EMBED_VERSIONS_FUNCTION_ID
    );
    assert_eq!(
        registrations.reconciliation_function().id,
        RECONCILE_EMBEDDINGS_FUNCTION_ID
    );
    assert_function_ref(registrations.queue_function());
    assert_function_ref(registrations.reconciliation_function());
    assert_trigger(registrations.subscriber());
    assert_trigger(registrations.cron());
}

#[test]
fn registration_plan_maps_trigger_failure_to_a_fixed_content_safe_runtime_error() {
    let mut plan = RegistrationPlan::new(&config(4));
    plan.cron.namespace = Some(" ".to_owned());
    let client = IIIClient::new("ws://127.0.0.1:0");

    let Err(error) = plan.register(&client, queue_handler, reconciliation_handler) else {
        panic!("invalid trigger namespace should fail registration");
    };

    assert_eq!(error, RuntimeRegistrationError::TriggerRegistration);
    assert_eq!(error.to_string(), "runtime_trigger_registration_failed");
}

#[test]
fn registration_plan_derives_managed_worker_identity_from_the_pinned_sdk_contract() {
    let configured = RegistrationPlan::with_nonce(
        &config_with_worker_name(4, Some(WORKER_NAME)),
        Uuid::parse_str("6f8e5571-8c8c-47a3-81df-e3d5756a0eaa")
            .expect("fixture nonce should be valid"),
    );
    let configured_options = configured.managed_worker.init_options();

    assert_eq!(configured.expected_worker_name, WORKER_NAME);
    assert_eq!(configured.expected_namespace, DEFAULT_NAMESPACE);
    assert_eq!(
        configured.managed_worker.identity,
        WorkerIdentityMode::Managed
    );
    assert_eq!(configured.managed_worker.metadata.name, WORKER_NAME);
    assert_eq!(
        configured.managed_worker.metadata.namespace.as_deref(),
        Some(DEFAULT_NAMESPACE)
    );
    assert_eq!(configured_options.identity, WorkerIdentityMode::Managed);
    assert_eq!(configured_options.namespace, None);
    assert_eq!(
        configured_options
            .metadata
            .as_ref()
            .expect("managed initialization should include worker metadata")
            .name,
        WORKER_NAME
    );
    assert_eq!(
        configured_options
            .otel
            .as_ref()
            .expect("managed initialization should explicitly configure telemetry")
            .enabled,
        Some(false)
    );

    let default_metadata = WorkerMetadata::default();
    let defaulted = RegistrationPlan::with_nonce(
        &config_with_worker_name(4, None),
        Uuid::parse_str("9a2eec91-c5a7-4cab-bdb4-df69a0d9f701")
            .expect("fixture nonce should be valid"),
    );
    assert_eq!(defaulted.expected_worker_name, default_metadata.name);
}

#[tokio::test]
async fn readiness_accepts_both_owned_functions_and_active_owned_triggers() {
    let plan = readiness_plan();
    let port = ScriptedReadinessPort::new(Timed::immediate(Ok(())), ready_calls(&plan));

    ReadinessEvaluator::new(&plan, &port)
        .wait_until_ready()
        .await
        .expect("matching current registrations should make the worker ready");

    assert_eq!(port.registration_timeouts(), vec![WORKER_READINESS_TIMEOUT]);
    assert_eq!(
        port.requests(),
        vec![
            CatalogRequest {
                function_id: EngineFunctions::INFO_FUNCTIONS.to_owned(),
                payload: json!({
                    "function_ids": [
                        EMBED_VERSIONS_FUNCTION_ID,
                        RECONCILE_EMBEDDINGS_FUNCTION_ID,
                    ],
                    "namespace": DEFAULT_NAMESPACE,
                }),
                timeout: WORKER_READINESS_TIMEOUT,
                namespace: None,
            },
            CatalogRequest {
                function_id: EngineFunctions::LIST_REGISTERED_TRIGGERS.to_owned(),
                payload: json!({
                    "function_id": EMBED_VERSIONS_FUNCTION_ID,
                    "trigger_type": "durable:subscriber",
                    "include_pending": true,
                }),
                timeout: WORKER_READINESS_TIMEOUT,
                namespace: None,
            },
            CatalogRequest {
                function_id: EngineFunctions::INFO_REGISTERED_TRIGGERS.to_owned(),
                payload: json!({"id": "subscriber"}),
                timeout: WORKER_READINESS_TIMEOUT,
                namespace: None,
            },
            CatalogRequest {
                function_id: EngineFunctions::LIST_REGISTERED_TRIGGERS.to_owned(),
                payload: json!({
                    "function_id": RECONCILE_EMBEDDINGS_FUNCTION_ID,
                    "trigger_type": "cron",
                    "include_pending": true,
                }),
                timeout: WORKER_READINESS_TIMEOUT,
                namespace: None,
            },
            CatalogRequest {
                function_id: EngineFunctions::INFO_REGISTERED_TRIGGERS.to_owned(),
                payload: json!({"id": "cron"}),
                timeout: WORKER_READINESS_TIMEOUT,
                namespace: None,
            },
        ]
    );
}

#[tokio::test]
async fn worker_runtime_readiness_wrapper_seam_composes_the_plan_and_port() {
    let plan = readiness_plan();
    let port = ScriptedReadinessPort::new(Timed::immediate(Ok(())), ready_calls(&plan));

    WorkerRuntime::wait_until_ready_with_port(&plan, &port, Duration::from_secs(1))
        .await
        .expect("the runtime readiness seam should delegate to the owned-plan evaluator");

    assert_eq!(port.registration_timeouts(), vec![Duration::from_secs(1)]);
}

#[tokio::test]
async fn readiness_waits_for_registration_before_validating_catalogs() {
    let plan = readiness_plan();
    let port = ScriptedReadinessPort::new(Timed::immediate(Ok(())), ready_calls(&plan));
    port.set_connection(ReadinessConnectionState::Connecting);
    port.set_connection_after_registration(ReadinessConnectionState::Connected);

    ReadinessEvaluator::new(&plan, &port)
        .wait_until_ready()
        .await
        .expect("a connecting worker should validate catalogs only after registration");
    assert_eq!(port.registration_timeouts(), vec![WORKER_READINESS_TIMEOUT]);
}

#[tokio::test]
async fn readiness_rejects_registration_disconnect_fatal_and_catalog_request_failures() {
    let plan = readiness_plan();

    let rejected = ScriptedReadinessPort::new(
        Timed::immediate(Err(ReadinessPortError::RegistrationRejected)),
        [],
    );
    assert_eq!(
        ReadinessEvaluator::new(&plan, &rejected)
            .wait_until_ready()
            .await,
        Err(RuntimeReadinessError::RegistrationRejected)
    );

    let disconnected =
        ScriptedReadinessPort::new(Timed::immediate(Err(ReadinessPortError::Disconnected)), []);
    disconnected.set_connection(ReadinessConnectionState::Connecting);
    assert_eq!(
        ReadinessEvaluator::new(&plan, &disconnected)
            .wait_until_ready()
            .await,
        Err(RuntimeReadinessError::Disconnected)
    );

    let disconnected_after_registration = ScriptedReadinessPort::new(Timed::immediate(Ok(())), []);
    disconnected_after_registration.set_connection(ReadinessConnectionState::Disconnected);
    assert_eq!(
        ReadinessEvaluator::new(&plan, &disconnected_after_registration)
            .wait_until_ready()
            .await,
        Err(RuntimeReadinessError::Disconnected)
    );

    let fatal = ScriptedReadinessPort::new(Timed::immediate(Ok(())), []);
    fatal.set_fatal();
    assert_eq!(
        ReadinessEvaluator::new(&plan, &fatal)
            .wait_until_ready()
            .await,
        Err(RuntimeReadinessError::RegistrationRejected)
    );
    assert!(fatal.registration_timeouts().is_empty());

    let request_failed = ScriptedReadinessPort::new(
        Timed::immediate(Ok(())),
        [Timed::immediate(Err(ReadinessPortError::RequestFailed))],
    );
    assert_eq!(
        ReadinessEvaluator::new(&plan, &request_failed)
            .wait_until_ready()
            .await,
        Err(RuntimeReadinessError::CatalogRequestFailed)
    );

    let timed_out = ScriptedReadinessPort::new(
        Timed::immediate(Ok(())),
        [Timed::immediate(Err(ReadinessPortError::Timeout))],
    );
    assert_eq!(
        ReadinessEvaluator::new(&plan, &timed_out)
            .wait_until_ready()
            .await,
        Err(RuntimeReadinessError::Timeout)
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
async fn readiness_times_out_for_pending_functions_and_missing_trigger_candidates() {
    let plan = readiness_plan();
    let timeout = Duration::from_millis(7);
    let pending_function = ScriptedReadinessPort::new(
        Timed::immediate(Ok(())),
        [Timed::immediate(Ok(json!({
            "functions": [{
                "function_id": EMBED_VERSIONS_FUNCTION_ID,
                "error": "pending",
            }],
        })))],
    );
    assert_eq!(
        ReadinessEvaluator::new(&plan, &pending_function)
            .wait_until_ready_with_timeout(timeout)
            .await,
        Err(RuntimeReadinessError::Timeout)
    );
    assert_eq!(pending_function.sleeps(), vec![timeout]);

    let missing_trigger = ScriptedReadinessPort::new(
        Timed::immediate(Ok(())),
        [
            Timed::immediate(Ok(function_catalog(&plan, plan.nonce))),
            Timed::immediate(Ok(registered_trigger_list(&plan.subscriber, &[]))),
        ],
    );
    assert_eq!(
        ReadinessEvaluator::new(&plan, &missing_trigger)
            .wait_until_ready_with_timeout(timeout)
            .await,
        Err(RuntimeReadinessError::Timeout)
    );
    assert_eq!(missing_trigger.sleeps(), vec![timeout]);

    let missing_cron_trigger = ScriptedReadinessPort::new(
        Timed::immediate(Ok(())),
        [
            Timed::immediate(Ok(function_catalog(&plan, plan.nonce))),
            Timed::immediate(Ok(registered_trigger_list(
                &plan.subscriber,
                &["subscriber"],
            ))),
            Timed::immediate(Ok(registered_trigger_detail(
                &plan,
                &plan.subscriber,
                "subscriber",
                "active",
                plan.nonce,
            ))),
            Timed::immediate(Ok(registered_trigger_list(&plan.cron, &[]))),
        ],
    );
    assert_eq!(
        ReadinessEvaluator::new(&plan, &missing_cron_trigger)
            .wait_until_ready_with_timeout(timeout)
            .await,
        Err(RuntimeReadinessError::Timeout)
    );
    assert_eq!(missing_cron_trigger.sleeps(), vec![timeout]);
}

#[tokio::test]
async fn readiness_polls_until_functions_and_trigger_candidates_are_active() {
    let plan = readiness_plan();
    let port = ScriptedReadinessPort::new(
        Timed::immediate(Ok(())),
        [
            Timed::immediate(Ok(json!({
                "functions": [{
                    "function_id": EMBED_VERSIONS_FUNCTION_ID,
                    "error": "not_found",
                }],
            }))),
            Timed::immediate(Ok(function_catalog(&plan, plan.nonce))),
            Timed::immediate(Ok(registered_trigger_list(&plan.subscriber, &[]))),
            Timed::immediate(Ok(function_catalog(&plan, plan.nonce))),
            Timed::immediate(Ok(registered_trigger_list(
                &plan.subscriber,
                &["subscriber"],
            ))),
            Timed::immediate(Ok(registered_trigger_detail(
                &plan,
                &plan.subscriber,
                "subscriber",
                "active",
                plan.nonce,
            ))),
            Timed::immediate(Ok(registered_trigger_list(&plan.cron, &["cron"]))),
            Timed::immediate(Ok(registered_trigger_detail(
                &plan, &plan.cron, "cron", "active", plan.nonce,
            ))),
        ],
    );

    ReadinessEvaluator::new(&plan, &port)
        .wait_until_ready()
        .await
        .expect("catalogs that become active before the deadline should make readiness succeed");
    assert_eq!(
        port.sleeps(),
        vec![Duration::from_millis(100), Duration::from_millis(100)]
    );
}

#[tokio::test]
async fn readiness_rejects_malformed_stale_foreign_duplicate_and_mismatched_functions() {
    let plan = readiness_plan();
    let stale_nonce = Uuid::parse_str("af7c58fa-f6bc-4d38-b6a5-7504952b496e")
        .expect("fixture nonce should be valid");

    let mut stale = function_catalog(&plan, plan.nonce);
    stale["functions"][0]["metadata"] =
        json!({REGISTRATION_NONCE_METADATA_KEY: stale_nonce.to_string()});

    let mut foreign_owner = function_catalog(&plan, plan.nonce);
    foreign_owner["functions"][0]["worker_name"] = json!("foreign-worker");

    let mut wrong_namespace = function_catalog(&plan, plan.nonce);
    wrong_namespace["functions"][0]["namespace"] = json!("foreign-namespace");

    let mut wrong_id = function_catalog(&plan, plan.nonce);
    wrong_id["functions"][0]["function_id"] = json!("foreign::embed_versions");

    let mut duplicate = function_catalog(&plan, plan.nonce);
    duplicate["functions"][1]["function_id"] = json!(EMBED_VERSIONS_FUNCTION_ID);

    let malformed = json!({"functions": "not-an-array"});
    let unknown_pending_error = json!({
        "functions": [{
            "function_id": EMBED_VERSIONS_FUNCTION_ID,
            "error": "forbidden",
        }],
    });

    for response in [
        stale,
        foreign_owner,
        wrong_namespace,
        wrong_id,
        duplicate,
        malformed,
        unknown_pending_error,
    ] {
        assert_catalog_mismatch(
            &plan,
            vec![Timed::immediate(Ok(response))],
            &[EngineFunctions::INFO_FUNCTIONS],
        )
        .await;
    }
}

#[tokio::test]
async fn readiness_rejects_error_rows_with_ownership_or_extra_fields() {
    let plan = readiness_plan();
    let timeout = Duration::from_millis(1);

    for response in [
        json!({
            "functions": [{
                "function_id": EMBED_VERSIONS_FUNCTION_ID,
                "error": "pending",
                "namespace": plan.expected_namespace,
            }],
        }),
        json!({
            "functions": [{
                "function_id": EMBED_VERSIONS_FUNCTION_ID,
                "error": "not_found",
                "worker_name": plan.expected_worker_name,
            }],
        }),
        json!({
            "functions": [{
                "function_id": EMBED_VERSIONS_FUNCTION_ID,
                "error": "pending",
                "metadata": {REGISTRATION_NONCE_METADATA_KEY: plan.nonce.to_string()},
            }],
        }),
        json!({
            "functions": [{
                "function_id": EMBED_VERSIONS_FUNCTION_ID,
                "error": "not_found",
                "unexpected": true,
            }],
        }),
    ] {
        let port =
            ScriptedReadinessPort::new(Timed::immediate(Ok(())), [Timed::immediate(Ok(response))]);

        assert_eq!(
            ReadinessEvaluator::new(&plan, &port)
                .wait_until_ready_with_timeout(timeout)
                .await,
            Err(RuntimeReadinessError::CatalogMismatch)
        );
        assert!(port.sleeps().is_empty());
    }
}

#[tokio::test]
async fn readiness_rejects_wrong_or_duplicate_trigger_candidates() {
    let plan = readiness_plan();
    let mut wrong_type = registered_trigger_list(&plan.subscriber, &["subscriber"]);
    wrong_type["registered_triggers"][0]["trigger_type"] = json!("foreign:subscriber");

    let mut wrong_target = registered_trigger_list(&plan.subscriber, &["subscriber"]);
    wrong_target["registered_triggers"][0]["function_id"] = json!("foreign::embed_versions");

    let mut blank_id = registered_trigger_list(&plan.subscriber, &[""]);
    blank_id["registered_triggers"][0]["id"] = json!("");

    let duplicate = registered_trigger_list(&plan.subscriber, &["one", "two"]);

    for response in [wrong_type, wrong_target, blank_id, duplicate] {
        assert_catalog_mismatch(
            &plan,
            vec![
                Timed::immediate(Ok(function_catalog(&plan, plan.nonce))),
                Timed::immediate(Ok(response)),
            ],
            &[
                EngineFunctions::INFO_FUNCTIONS,
                EngineFunctions::LIST_REGISTERED_TRIGGERS,
            ],
        )
        .await;
    }
}

#[tokio::test]
async fn readiness_rejects_one_registered_trigger_id_for_both_bindings() {
    let plan = readiness_plan();
    let shared_id = "shared";

    assert_catalog_mismatch(
        &plan,
        vec![
            Timed::immediate(Ok(function_catalog(&plan, plan.nonce))),
            Timed::immediate(Ok(registered_trigger_list(&plan.subscriber, &[shared_id]))),
            Timed::immediate(Ok(registered_trigger_detail(
                &plan,
                &plan.subscriber,
                shared_id,
                "active",
                plan.nonce,
            ))),
            Timed::immediate(Ok(registered_trigger_list(&plan.cron, &[shared_id]))),
            Timed::immediate(Ok(registered_trigger_detail(
                &plan, &plan.cron, shared_id, "active", plan.nonce,
            ))),
        ],
        &[
            EngineFunctions::INFO_FUNCTIONS,
            EngineFunctions::LIST_REGISTERED_TRIGGERS,
            EngineFunctions::INFO_REGISTERED_TRIGGERS,
            EngineFunctions::LIST_REGISTERED_TRIGGERS,
        ],
    )
    .await;
}

#[tokio::test]
async fn readiness_rejects_every_queue_trigger_ownership_mismatch() {
    let plan = readiness_plan();
    let stale_nonce = Uuid::parse_str("198ad5d4-ab1e-4328-89fb-bb26563e80a9")
        .expect("fixture nonce should be valid");

    let mut wrong_id =
        registered_trigger_detail(&plan, &plan.subscriber, "subscriber", "active", plan.nonce);
    wrong_id["id"] = json!("foreign");

    let pending =
        registered_trigger_detail(&plan, &plan.subscriber, "subscriber", "pending", plan.nonce);

    let stale =
        registered_trigger_detail(&plan, &plan.subscriber, "subscriber", "active", stale_nonce);

    let mut foreign_owner =
        registered_trigger_detail(&plan, &plan.subscriber, "subscriber", "active", plan.nonce);
    foreign_owner["worker_name"] = json!("foreign-worker");

    let mut wrong_config =
        registered_trigger_detail(&plan, &plan.subscriber, "subscriber", "active", plan.nonce);
    wrong_config["config"] = json!({"queue": "wrong-topic"});

    let mut wrong_type =
        registered_trigger_detail(&plan, &plan.subscriber, "subscriber", "active", plan.nonce);
    wrong_type["trigger_type"] = json!("foreign:subscriber");

    let mut wrong_provider_id =
        registered_trigger_detail(&plan, &plan.subscriber, "subscriber", "active", plan.nonce);
    wrong_provider_id["trigger"]["id"] = json!("foreign:subscriber");

    let mut wrong_provider_namespace =
        registered_trigger_detail(&plan, &plan.subscriber, "subscriber", "active", plan.nonce);
    wrong_provider_namespace["trigger"]["namespace"] = json!("foreign-namespace");

    let mut missing_target =
        registered_trigger_detail(&plan, &plan.subscriber, "subscriber", "active", plan.nonce);
    missing_target
        .as_object_mut()
        .expect("trigger detail should be an object")
        .remove("function");

    let mut wrong_target_id =
        registered_trigger_detail(&plan, &plan.subscriber, "subscriber", "active", plan.nonce);
    wrong_target_id["function"]["function_id"] = json!("foreign::embed_versions");

    let mut wrong_target_namespace =
        registered_trigger_detail(&plan, &plan.subscriber, "subscriber", "active", plan.nonce);
    wrong_target_namespace["function"]["namespace"] = json!("foreign-namespace");

    let mut wrong_target_owner =
        registered_trigger_detail(&plan, &plan.subscriber, "subscriber", "active", plan.nonce);
    wrong_target_owner["function"]["worker_name"] = json!("foreign-worker");

    for detail in [
        wrong_id,
        pending,
        stale,
        foreign_owner,
        wrong_config,
        wrong_type,
        wrong_provider_id,
        wrong_provider_namespace,
        missing_target,
        wrong_target_id,
        wrong_target_namespace,
        wrong_target_owner,
    ] {
        assert_catalog_mismatch(
            &plan,
            vec![
                Timed::immediate(Ok(function_catalog(&plan, plan.nonce))),
                Timed::immediate(Ok(registered_trigger_list(
                    &plan.subscriber,
                    &["subscriber"],
                ))),
                Timed::immediate(Ok(detail)),
            ],
            &[
                EngineFunctions::INFO_FUNCTIONS,
                EngineFunctions::LIST_REGISTERED_TRIGGERS,
                EngineFunctions::INFO_REGISTERED_TRIGGERS,
            ],
        )
        .await;
    }
}

#[tokio::test]
async fn readiness_rejects_cron_trigger_mismatches_after_queue_ownership_is_validated() {
    let plan = readiness_plan();
    let mut wrong_config =
        registered_trigger_detail(&plan, &plan.cron, "cron", "active", plan.nonce);
    wrong_config["config"] = json!({"expression": "0 * * * * *"});

    let mut wrong_type = registered_trigger_detail(&plan, &plan.cron, "cron", "active", plan.nonce);
    wrong_type["trigger_type"] = json!("foreign:cron");

    let mut wrong_provider =
        registered_trigger_detail(&plan, &plan.cron, "cron", "active", plan.nonce);
    wrong_provider["trigger"]["id"] = json!("foreign:cron");

    let mut wrong_target_owner =
        registered_trigger_detail(&plan, &plan.cron, "cron", "active", plan.nonce);
    wrong_target_owner["function"]["worker_name"] = json!("foreign-worker");

    let pending = registered_trigger_detail(&plan, &plan.cron, "cron", "pending", plan.nonce);

    let mut wrong_provider_namespace =
        registered_trigger_detail(&plan, &plan.cron, "cron", "active", plan.nonce);
    wrong_provider_namespace["trigger"]["namespace"] = json!("foreign-namespace");

    for detail in [
        wrong_config,
        wrong_type,
        wrong_provider,
        wrong_target_owner,
        pending,
        wrong_provider_namespace,
    ] {
        let mut calls = ready_calls(&plan);
        *ready_response_mut(&mut calls, 4) = detail;
        assert_catalog_mismatch(
            &plan,
            calls,
            &[
                EngineFunctions::INFO_FUNCTIONS,
                EngineFunctions::LIST_REGISTERED_TRIGGERS,
                EngineFunctions::INFO_REGISTERED_TRIGGERS,
                EngineFunctions::LIST_REGISTERED_TRIGGERS,
                EngineFunctions::INFO_REGISTERED_TRIGGERS,
            ],
        )
        .await;
    }
}

#[tokio::test]
async fn readiness_uses_one_absolute_deadline_for_registration_catalog_calls_and_polls() {
    let plan = readiness_plan();
    let timeout = Duration::from_millis(20);
    let port = ScriptedReadinessPort::new(
        Timed::after(Duration::from_millis(3), Ok(())),
        [
            Timed::after(
                Duration::from_millis(2),
                Ok(function_catalog(&plan, plan.nonce)),
            ),
            Timed::after(
                Duration::from_millis(3),
                Ok(registered_trigger_list(&plan.subscriber, &["subscriber"])),
            ),
            Timed::after(
                Duration::from_millis(2),
                Ok(registered_trigger_detail(
                    &plan,
                    &plan.subscriber,
                    "subscriber",
                    "active",
                    plan.nonce,
                )),
            ),
            Timed::after(
                Duration::from_millis(4),
                Ok(registered_trigger_list(&plan.cron, &["cron"])),
            ),
            Timed::after(
                Duration::from_millis(1),
                Ok(registered_trigger_detail(
                    &plan, &plan.cron, "cron", "active", plan.nonce,
                )),
            ),
        ],
    );

    ReadinessEvaluator::new(&plan, &port)
        .wait_until_ready_with_timeout(timeout)
        .await
        .expect("all ownership calls should complete before the one deadline");
    assert_eq!(port.registration_timeouts(), vec![timeout]);
    assert_eq!(
        port.requests()
            .into_iter()
            .map(|request| request.timeout)
            .collect::<Vec<_>>(),
        vec![
            Duration::from_millis(17),
            Duration::from_millis(15),
            Duration::from_millis(12),
            Duration::from_millis(10),
            Duration::from_millis(6),
        ]
    );

    let pending = ScriptedReadinessPort::new(
        Timed::after(Duration::from_millis(3), Ok(())),
        [Timed::after(
            Duration::from_millis(4),
            Ok(json!({
                "functions": [{
                    "function_id": EMBED_VERSIONS_FUNCTION_ID,
                    "error": "not_found",
                }],
            })),
        )],
    );
    assert_eq!(
        ReadinessEvaluator::new(&plan, &pending)
            .wait_until_ready_with_timeout(Duration::from_millis(10))
            .await,
        Err(RuntimeReadinessError::Timeout)
    );
    assert_eq!(pending.requests()[0].timeout, Duration::from_millis(7));
    assert_eq!(pending.sleeps(), vec![Duration::from_millis(3)]);
}

#[test]
fn readiness_errors_are_fixed_content_safe_runtime_categories() {
    const SENTINEL: &str = "catalog-config-secret";
    for (error, expected) in [
        (
            RuntimeReadinessError::RegistrationRejected,
            "runtime_registration_rejected",
        ),
        (
            RuntimeReadinessError::Disconnected,
            "runtime_startup_disconnected",
        ),
        (
            RuntimeReadinessError::CatalogMismatch,
            "runtime_catalog_mismatch",
        ),
        (
            RuntimeReadinessError::CatalogRequestFailed,
            "runtime_catalog_request_failed",
        ),
        (RuntimeReadinessError::Timeout, "runtime_readiness_timeout"),
    ] {
        assert_eq!(error.to_string(), expected);
        assert!(!error.to_string().contains(SENTINEL));
    }
}
