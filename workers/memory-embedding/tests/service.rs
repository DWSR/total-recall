use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use iii_sdk::builtin_triggers::CronCallRequest;
use memory_embedding::{
    EmbeddingCoordinator, EventAdapter, QueueEventProcessor, ReconciliationProcessor,
    config::{
        Config, TOTAL_RECALL_EMBEDDING_DATABASE_ENV, TOTAL_RECALL_EMBEDDING_MODEL_ENV,
        TOTAL_RECALL_EMBEDDING_PROVIDER_ENV, TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC_ENV,
    },
    contracts::{
        CanonicalEmbeddingInput, Deadline, EmbeddingError, EmbeddingOutcome, EmbeddingRouter,
        EmbeddingWorkItem, EmbeddingWriter, EventFailure, GeneratedEmbedding, LoadedEmbeddingWork,
        MemoryKey, RecordingEmbeddingWorkRepository, RepositoryCall, RepositoryFailure,
        RepositoryResponses, RouterError, RouterFailure, WriteOutcome, WriterError, WriterFailure,
    },
    renderer::render,
};
use serde_json::{Value, json};
use tokio::sync::{Barrier, Semaphore, oneshot, watch};

const DATABASE: &str = "service-queue-database-sentinel";
const FIRST_ID: &str = "service-first-key-sentinel";
const SECOND_ID: &str = "service-second-key-sentinel";
const FIRST_TITLE: &str = "service-first-title-sentinel";
const SECOND_TITLE: &str = "service-second-title-sentinel";
const FIRST_CONTENT: &str = "service-first-content-sentinel";
const SECOND_CONTENT: &str = "service-second-content-sentinel";
const FIRST_CONCEPT: &str = "service-first-concept-sentinel";
const SECOND_CONCEPT: &str = "service-second-concept-sentinel";
const OUTER_TIMEOUT: Duration = Duration::from_secs(1);
const BATCH_LIMIT: u32 = 2;

#[derive(Clone, Debug, Eq, PartialEq)]
struct RouterCall {
    inputs: Vec<CanonicalEmbeddingInput>,
    deadline: Deadline,
}

#[derive(Clone)]
struct ScriptedRouter {
    state: Arc<Mutex<ScriptedRouterState>>,
}

struct ScriptedRouterState {
    response: Result<Vec<GeneratedEmbedding>, RouterError>,
    calls: Vec<RouterCall>,
}

impl ScriptedRouter {
    fn new(response: Result<Vec<GeneratedEmbedding>, RouterError>) -> Self {
        Self {
            state: Arc::new(Mutex::new(ScriptedRouterState {
                response,
                calls: Vec::new(),
            })),
        }
    }

    fn calls(&self) -> Vec<RouterCall> {
        self.state
            .lock()
            .expect("test router state lock should not be poisoned")
            .calls
            .clone()
    }
}

#[async_trait]
impl EmbeddingRouter for ScriptedRouter {
    async fn embed(
        &self,
        inputs: &[CanonicalEmbeddingInput],
        deadline: Deadline,
    ) -> Result<Vec<GeneratedEmbedding>, RouterError> {
        let mut state = self
            .state
            .lock()
            .expect("test router state lock should not be poisoned");
        state.calls.push(RouterCall {
            inputs: inputs.to_vec(),
            deadline,
        });
        state.response.clone()
    }
}

#[derive(Clone, PartialEq)]
struct WriterCall {
    key: MemoryKey,
    vector: Vec<f64>,
    deadline: Deadline,
}

#[derive(Clone)]
struct ScriptedWriter {
    state: Arc<Mutex<ScriptedWriterState>>,
}

struct ScriptedWriterState {
    responses: Vec<(String, Result<WriteOutcome, WriterError>)>,
    calls: Vec<WriterCall>,
    barrier: Option<Arc<Barrier>>,
}

impl ScriptedWriter {
    fn new(
        responses: Vec<(String, Result<WriteOutcome, WriterError>)>,
        barrier: Option<Arc<Barrier>>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(ScriptedWriterState {
                responses,
                calls: Vec::new(),
                barrier,
            })),
        }
    }

    fn calls(&self) -> Vec<WriterCall> {
        self.state
            .lock()
            .expect("test writer state lock should not be poisoned")
            .calls
            .clone()
    }
}

#[async_trait]
impl EmbeddingWriter for ScriptedWriter {
    async fn insert(
        &self,
        key: MemoryKey,
        vector: Vec<f64>,
        deadline: Deadline,
    ) -> Result<WriteOutcome, WriterError> {
        let (response, barrier) = {
            let mut state = self
                .state
                .lock()
                .expect("test writer state lock should not be poisoned");
            let response = state
                .responses
                .iter()
                .find(|(id, _)| id == key.id())
                .map(|(_, response)| *response)
                .expect("test writer response should exist for every key");
            state.calls.push(WriterCall {
                key,
                vector,
                deadline,
            });
            (response, state.barrier.clone())
        };

        if let Some(barrier) = barrier {
            barrier.wait().await;
        }

        response
    }
}

#[derive(Clone)]
struct DelayedPartialWriter {
    state: Arc<Mutex<DelayedPartialWriterState>>,
    admitted: Arc<Barrier>,
    release_second: Arc<Semaphore>,
    first_settled: watch::Sender<bool>,
}

struct DelayedPartialWriterState {
    calls: Vec<WriterCall>,
    settled: Vec<String>,
}

impl DelayedPartialWriter {
    fn new() -> (Self, watch::Receiver<bool>) {
        let (first_settled, receiver) = watch::channel(false);

        (
            Self {
                state: Arc::new(Mutex::new(DelayedPartialWriterState {
                    calls: Vec::new(),
                    settled: Vec::new(),
                })),
                admitted: Arc::new(Barrier::new(2)),
                release_second: Arc::new(Semaphore::new(0)),
                first_settled,
            },
            receiver,
        )
    }

    fn calls(&self) -> Vec<WriterCall> {
        self.state
            .lock()
            .expect("test delayed-writer state lock should not be poisoned")
            .calls
            .clone()
    }

    fn settled(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("test delayed-writer state lock should not be poisoned")
            .settled
            .clone()
    }

    fn release_second(&self) {
        self.release_second.add_permits(1);
    }

    fn record_settled(&self, id: String) {
        self.state
            .lock()
            .expect("test delayed-writer state lock should not be poisoned")
            .settled
            .push(id);
    }
}

#[async_trait]
impl EmbeddingWriter for DelayedPartialWriter {
    async fn insert(
        &self,
        key: MemoryKey,
        vector: Vec<f64>,
        deadline: Deadline,
    ) -> Result<WriteOutcome, WriterError> {
        let id = key.id().to_owned();
        self.state
            .lock()
            .expect("test delayed-writer state lock should not be poisoned")
            .calls
            .push(WriterCall {
                key,
                vector,
                deadline,
            });
        self.admitted.wait().await;

        if id == FIRST_ID {
            self.record_settled(id);
            self.first_settled.send_replace(true);
            Err(EmbeddingError::writer(WriterFailure::Backend))
        } else {
            let _release = self
                .release_second
                .acquire()
                .await
                .expect("test delayed writer release semaphore should remain open");
            self.record_settled(id);
            Ok(WriteOutcome::Stored)
        }
    }
}

#[derive(Clone)]
struct LateResponseRouter {
    state: Arc<Mutex<LateResponseRouterState>>,
}

struct LateResponseRouterState {
    calls: usize,
    late_response_at: Option<Instant>,
    response_reporter: Option<oneshot::Sender<Instant>>,
}

impl LateResponseRouter {
    fn new() -> (Self, oneshot::Receiver<Instant>) {
        let (response_reporter, response_received) = oneshot::channel();

        (
            Self {
                state: Arc::new(Mutex::new(LateResponseRouterState {
                    calls: 0,
                    late_response_at: None,
                    response_reporter: Some(response_reporter),
                })),
            },
            response_received,
        )
    }

    fn calls(&self) -> usize {
        self.state
            .lock()
            .expect("test late-response router state lock should not be poisoned")
            .calls
    }

    fn late_response_at(&self) -> Option<Instant> {
        self.state
            .lock()
            .expect("test late-response router state lock should not be poisoned")
            .late_response_at
    }
}

#[async_trait]
impl EmbeddingRouter for LateResponseRouter {
    async fn embed(
        &self,
        inputs: &[CanonicalEmbeddingInput],
        deadline: Deadline,
    ) -> Result<Vec<GeneratedEmbedding>, RouterError> {
        {
            let mut state = self
                .state
                .lock()
                .expect("test late-response router state lock should not be poisoned");
            state.calls += 1;
        }

        let response = inputs
            .iter()
            .map(|input| {
                GeneratedEmbedding::try_new(input.key().clone(), vec![1.0])
                    .expect("test late router response vector should be valid")
            })
            .collect();
        let (sender, receiver) = oneshot::channel();
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            tokio::time::sleep_until(
                tokio::time::Instant::from_std(deadline.instant()) + Duration::from_millis(50),
            )
            .await;
            let response_at = Instant::now();
            let mut state = state
                .lock()
                .expect("test late-response router state lock should not be poisoned");
            state.late_response_at = Some(response_at);
            let response_reporter = state.response_reporter.take();
            let _ = sender.send(Ok(response));
            if let Some(response_reporter) = response_reporter {
                let _ = response_reporter.send(response_at);
            }
        });

        receiver
            .await
            .map_err(|_| EmbeddingError::router(RouterFailure::Remote))?
    }
}

fn work_item(
    id: &str,
    version: &str,
    title: &str,
    content: impl Into<String>,
    concept: &str,
) -> EmbeddingWorkItem {
    EmbeddingWorkItem::new(
        MemoryKey::try_new(id, version).expect("test key should be valid"),
        title,
        content,
        vec![concept.to_owned()],
    )
}

fn work_items() -> Vec<EmbeddingWorkItem> {
    vec![
        work_item(FIRST_ID, "7", FIRST_TITLE, FIRST_CONTENT, FIRST_CONCEPT),
        work_item(SECOND_ID, "9", SECOND_TITLE, SECOND_CONTENT, SECOND_CONCEPT),
    ]
}

fn generated(key: &MemoryKey, vector: Vec<f32>) -> GeneratedEmbedding {
    GeneratedEmbedding::try_new(key.clone(), vector).expect("test vector should be valid")
}

fn coordinator<R, W>(
    router: R,
    writer: W,
    max_input_bytes: usize,
    permits: Arc<Semaphore>,
) -> EmbeddingCoordinator<R, W> {
    EmbeddingCoordinator::new(router, writer, BATCH_LIMIT, max_input_bytes, permits)
}

fn event_adapter() -> EventAdapter {
    let config = Config::from_values([
        (
            TOTAL_RECALL_EMBEDDING_DATABASE_ENV.to_owned(),
            DATABASE.to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC_ENV.to_owned(),
            "memory-inserts".to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_PROVIDER_ENV.to_owned(),
            "embedding-router".to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_MODEL_ENV.to_owned(),
            "embedding-model".to_owned(),
        ),
    ])
    .expect("queue event test configuration should be valid");

    EventAdapter::new(&config)
}

fn queue_payload() -> Value {
    json!({
        "db": DATABASE,
        "table": "public.memories",
        "op": "insert",
        "affected_rows": 2,
        "returning": [
            { "id": FIRST_ID, "version": "7" },
            { "id": SECOND_ID, "version": "9" },
        ],
        "at": 0,
    })
}

fn cron_call() -> CronCallRequest {
    CronCallRequest {
        trigger: "cron".to_owned(),
        job_id: "service-reconciliation-job".to_owned(),
        scheduled_time: "2026-09-23T00:00:00Z".to_owned(),
        actual_time: "2026-09-23T00:00:01Z".to_owned(),
    }
}

fn queue_processor(
    repository: RecordingEmbeddingWorkRepository,
    router: ScriptedRouter,
    writer: ScriptedWriter,
) -> QueueEventProcessor<RecordingEmbeddingWorkRepository, ScriptedRouter, ScriptedWriter> {
    QueueEventProcessor::new(
        event_adapter(),
        repository,
        coordinator(router, writer, usize::MAX, Arc::new(Semaphore::new(1))),
    )
}

fn reconciliation_processor(
    repository: RecordingEmbeddingWorkRepository,
    router: ScriptedRouter,
    writer: ScriptedWriter,
    reconciliation_limit: u32,
) -> ReconciliationProcessor<RecordingEmbeddingWorkRepository, ScriptedRouter, ScriptedWriter> {
    ReconciliationProcessor::new(
        repository,
        coordinator(router, writer, usize::MAX, Arc::new(Semaphore::new(1))),
        reconciliation_limit,
    )
}

fn assert_opaque(error: &EmbeddingError) {
    assert_content_safe(&error.to_string());
    assert_content_safe(&format!("{error:?}"));
}

fn assert_content_safe(output: &str) {
    for protected in [
        DATABASE,
        FIRST_ID,
        SECOND_ID,
        FIRST_TITLE,
        SECOND_TITLE,
        FIRST_CONTENT,
        SECOND_CONTENT,
        FIRST_CONCEPT,
        SECOND_CONCEPT,
    ] {
        assert!(
            !output.contains(protected),
            "output leaked protected sentinel {protected}: {output}"
        );
    }
}

#[tokio::test]
async fn queue_event_processes_valid_multi_key_delivery_with_content_free_counts() {
    let first = work_item(FIRST_ID, "7", FIRST_TITLE, FIRST_CONTENT, FIRST_CONCEPT);
    let second = work_item(SECOND_ID, "9", SECOND_TITLE, SECOND_CONTENT, SECOND_CONCEPT);
    let deadline = Deadline::after(Duration::from_secs(1));
    let repository = RecordingEmbeddingWorkRepository::new(RepositoryResponses {
        load_keys: Ok(vec![
            LoadedEmbeddingWork::Pending(first.clone()),
            LoadedEmbeddingWork::AlreadyPresent(second.key().clone()),
        ]),
        list_missing: Ok(Vec::new()),
    });
    let router = ScriptedRouter::new(Ok(vec![generated(first.key(), vec![1.0])]));
    let writer = ScriptedWriter::new(vec![(FIRST_ID.to_owned(), Ok(WriteOutcome::Stored))], None);
    let service = queue_processor(repository.clone(), router.clone(), writer.clone());

    let outcome = service
        .process_event(queue_payload(), deadline)
        .await
        .expect("valid queued keys should complete");

    assert_eq!(outcome, EmbeddingOutcome::new(2, 1, 1, 1));
    assert_eq!(
        repository.calls(),
        vec![RepositoryCall::LoadKeys {
            keys: vec![first.key().clone(), second.key().clone()],
            deadline,
        }],
        "the queue flow must load all adapted keys exactly once"
    );
    assert_eq!(router.calls().len(), 1);
    assert_eq!(writer.calls().len(), 1);
    assert_eq!(router.calls()[0].deadline, deadline);
    assert_eq!(writer.calls()[0].deadline, deadline);
    assert_content_safe(&serde_json::to_string(&outcome).expect("outcome counts should serialize"));
    assert_content_safe(&format!("{outcome:?}"));
}

#[tokio::test]
async fn queue_event_rejects_malformed_payload_before_any_external_call() {
    let repository = RecordingEmbeddingWorkRepository::new(RepositoryResponses {
        load_keys: Ok(Vec::new()),
        list_missing: Ok(Vec::new()),
    });
    let router = ScriptedRouter::new(Err(EmbeddingError::router(RouterFailure::Remote)));
    let writer = ScriptedWriter::new(Vec::new(), None);
    let service = queue_processor(repository.clone(), router.clone(), writer.clone());
    let mut payload = queue_payload();
    payload["title"] = json!(FIRST_TITLE);

    let error = service
        .process_event(payload, Deadline::after(Duration::from_secs(1)))
        .await
        .expect_err("malformed queued payloads must be rejected");

    assert_eq!(error, EmbeddingError::event(EventFailure::Malformed));
    assert_opaque(&error);
    assert!(repository.calls().is_empty());
    assert!(router.calls().is_empty());
    assert!(writer.calls().is_empty());
}

#[tokio::test]
async fn queue_event_processes_pending_work_before_returning_a_missing_delivery_error() {
    let first = work_item(FIRST_ID, "7", FIRST_TITLE, FIRST_CONTENT, FIRST_CONCEPT);
    let second = work_item(SECOND_ID, "9", SECOND_TITLE, SECOND_CONTENT, SECOND_CONCEPT);
    let repository = RecordingEmbeddingWorkRepository::new(RepositoryResponses {
        load_keys: Ok(vec![
            LoadedEmbeddingWork::Pending(first.clone()),
            LoadedEmbeddingWork::Missing(second.key().clone()),
        ]),
        list_missing: Ok(Vec::new()),
    });
    let router = ScriptedRouter::new(Ok(vec![generated(first.key(), vec![1.0])]));
    let writer = ScriptedWriter::new(vec![(FIRST_ID.to_owned(), Ok(WriteOutcome::Stored))], None);
    let service = queue_processor(repository.clone(), router.clone(), writer.clone());

    let error = service
        .process_event(queue_payload(), Deadline::after(Duration::from_secs(1)))
        .await
        .expect_err("a missing key must fail delivery after pending work settles");

    assert_eq!(
        error,
        EmbeddingError::repository(RepositoryFailure::Missing)
    );
    assert_opaque(&error);
    assert_eq!(repository.calls().len(), 1);
    assert_eq!(router.calls().len(), 1);
    assert_eq!(writer.calls().len(), 1);
}

#[tokio::test]
async fn queue_event_retry_skips_committed_work_after_a_partial_write_failure() {
    let first = work_item(FIRST_ID, "7", FIRST_TITLE, FIRST_CONTENT, FIRST_CONCEPT);
    let second = work_item(SECOND_ID, "9", SECOND_TITLE, SECOND_CONTENT, SECOND_CONCEPT);
    let first_repository = RecordingEmbeddingWorkRepository::new(RepositoryResponses {
        load_keys: Ok(vec![
            LoadedEmbeddingWork::Pending(first.clone()),
            LoadedEmbeddingWork::Pending(second.clone()),
        ]),
        list_missing: Ok(Vec::new()),
    });
    let first_router = ScriptedRouter::new(Ok(vec![
        generated(first.key(), vec![1.0]),
        generated(second.key(), vec![2.0]),
    ]));
    let first_writer = ScriptedWriter::new(
        vec![
            (FIRST_ID.to_owned(), Ok(WriteOutcome::Stored)),
            (
                SECOND_ID.to_owned(),
                Err(EmbeddingError::writer(WriterFailure::Backend)),
            ),
        ],
        None,
    );
    let first_attempt = queue_processor(
        first_repository.clone(),
        first_router.clone(),
        first_writer.clone(),
    );

    let error = first_attempt
        .process_event(queue_payload(), Deadline::after(Duration::from_secs(1)))
        .await
        .expect_err("a partial write must fail the delivery");

    assert_eq!(error, EmbeddingError::writer(WriterFailure::Backend));
    assert_opaque(&error);
    assert_eq!(first_repository.calls().len(), 1);
    assert_eq!(first_router.calls().len(), 1);
    assert_eq!(first_writer.calls().len(), 2);

    let retry_repository = RecordingEmbeddingWorkRepository::new(RepositoryResponses {
        load_keys: Ok(vec![
            LoadedEmbeddingWork::AlreadyPresent(first.key().clone()),
            LoadedEmbeddingWork::Pending(second.clone()),
        ]),
        list_missing: Ok(Vec::new()),
    });
    let retry_router = ScriptedRouter::new(Ok(vec![generated(second.key(), vec![2.0])]));
    let retry_writer =
        ScriptedWriter::new(vec![(SECOND_ID.to_owned(), Ok(WriteOutcome::Stored))], None);
    let retry = queue_processor(
        retry_repository.clone(),
        retry_router.clone(),
        retry_writer.clone(),
    );

    let outcome = retry
        .process_event(queue_payload(), Deadline::after(Duration::from_secs(1)))
        .await
        .expect("a retry should process only the remaining pending key");

    assert_eq!(outcome, EmbeddingOutcome::new(2, 1, 1, 1));
    assert_eq!(retry_repository.calls().len(), 1);
    assert_eq!(retry_router.calls().len(), 1);
    assert_eq!(retry_router.calls()[0].inputs.len(), 1);
    assert_eq!(retry_writer.calls().len(), 1);
    assert_eq!(retry_writer.calls()[0].key, *second.key());
}

#[test]
fn reconciliation_uses_the_sdk_typed_cron_request_contract() {
    let malformed = serde_json::from_value::<CronCallRequest>(json!({
        "trigger": "cron",
        "job_id": "service-reconciliation-job",
        "scheduled_time": "2026-09-23T00:00:00Z",
    }));

    assert!(
        malformed.is_err(),
        "the SDK cron request requires every typed field before reconciliation is called"
    );
}

#[tokio::test]
async fn reconciliation_rejects_malformed_cron_calls_before_any_external_call() {
    let repository = RecordingEmbeddingWorkRepository::new(RepositoryResponses {
        load_keys: Ok(Vec::new()),
        list_missing: Ok(Vec::new()),
    });
    let router = ScriptedRouter::new(Err(EmbeddingError::router(RouterFailure::Remote)));
    let writer = ScriptedWriter::new(Vec::new(), None);
    let service = reconciliation_processor(repository.clone(), router.clone(), writer.clone(), 1);

    for call in [
        CronCallRequest {
            trigger: "queue".to_owned(),
            ..cron_call()
        },
        CronCallRequest {
            scheduled_time: "invalid-scheduled-time-sentinel".to_owned(),
            ..cron_call()
        },
        CronCallRequest {
            actual_time: "invalid-actual-time-sentinel".to_owned(),
            ..cron_call()
        },
    ] {
        let error = service
            .reconcile(call, Deadline::after(Duration::from_secs(1)))
            .await
            .expect_err("a malformed cron call must be rejected before reconciliation work");

        assert_eq!(error, EmbeddingError::event(EventFailure::Malformed));
        assert_opaque(&error);
        assert!(
            !error
                .to_string()
                .contains("invalid-scheduled-time-sentinel")
        );
        assert!(!error.to_string().contains("invalid-actual-time-sentinel"));
    }

    assert!(repository.calls().is_empty());
    assert!(router.calls().is_empty());
    assert!(writer.calls().is_empty());
}

#[tokio::test]
async fn reconciliation_returns_zero_counts_for_one_empty_page_without_routing() {
    let deadline = Deadline::after(Duration::from_secs(1));
    let repository = RecordingEmbeddingWorkRepository::new(RepositoryResponses {
        load_keys: Ok(Vec::new()),
        list_missing: Ok(Vec::new()),
    });
    let router = ScriptedRouter::new(Err(EmbeddingError::router(RouterFailure::Remote)));
    let writer = ScriptedWriter::new(Vec::new(), None);
    let service = reconciliation_processor(repository.clone(), router.clone(), writer.clone(), 2);

    let outcome = service
        .reconcile(cron_call(), deadline)
        .await
        .expect("an empty reconciliation page should succeed");

    assert_eq!(outcome, EmbeddingOutcome::new(0, 0, 0, 0));
    assert_eq!(
        repository.calls(),
        vec![RepositoryCall::ListMissing { limit: 2, deadline }],
        "reconciliation must select one configured missing-work page"
    );
    assert!(router.calls().is_empty());
    assert!(writer.calls().is_empty());
}

#[tokio::test]
async fn reconciliation_processes_one_configured_historical_page() {
    let historical = work_item(FIRST_ID, "2", FIRST_TITLE, FIRST_CONTENT, FIRST_CONCEPT);
    let current = work_item(FIRST_ID, "7", FIRST_TITLE, FIRST_CONTENT, FIRST_CONCEPT);
    let deadline = Deadline::after(Duration::from_secs(1));
    let repository = RecordingEmbeddingWorkRepository::new(RepositoryResponses {
        load_keys: Ok(Vec::new()),
        list_missing: Ok(vec![historical.clone(), current.clone()]),
    });
    let router = ScriptedRouter::new(Ok(vec![
        generated(historical.key(), vec![1.0]),
        generated(current.key(), vec![2.0]),
    ]));
    let writer = ScriptedWriter::new(vec![(FIRST_ID.to_owned(), Ok(WriteOutcome::Stored))], None);
    let service = reconciliation_processor(repository.clone(), router.clone(), writer.clone(), 2);

    let outcome = service
        .reconcile(cron_call(), deadline)
        .await
        .expect("the configured historical page should complete");

    assert_eq!(outcome, EmbeddingOutcome::new(2, 2, 0, 2));
    assert_eq!(
        repository.calls(),
        vec![RepositoryCall::ListMissing { limit: 2, deadline }]
    );
    assert_eq!(router.calls().len(), 1, "only one router batch is admitted");
    assert_eq!(router.calls()[0].inputs.len(), 2);
    assert_eq!(router.calls()[0].deadline, deadline);
    assert_eq!(writer.calls().len(), 2);
    assert!(writer.calls().iter().all(|call| call.deadline == deadline));
}

#[tokio::test]
async fn reconciliation_counts_an_already_present_write_race_as_complete() {
    let item = work_item(FIRST_ID, "7", FIRST_TITLE, FIRST_CONTENT, FIRST_CONCEPT);
    let repository = RecordingEmbeddingWorkRepository::new(RepositoryResponses {
        load_keys: Ok(Vec::new()),
        list_missing: Ok(vec![item.clone()]),
    });
    let router = ScriptedRouter::new(Ok(vec![generated(item.key(), vec![1.0])]));
    let writer = ScriptedWriter::new(
        vec![(FIRST_ID.to_owned(), Ok(WriteOutcome::AlreadyPresent))],
        None,
    );
    let service = reconciliation_processor(repository, router.clone(), writer.clone(), 1);

    let outcome = service
        .reconcile(cron_call(), Deadline::after(Duration::from_secs(1)))
        .await
        .expect("a duplicate immutable insert should complete the selected item");

    assert_eq!(outcome, EmbeddingOutcome::new(1, 1, 1, 0));
    assert_eq!(router.calls().len(), 1);
    assert_eq!(
        writer.calls().len(),
        1,
        "a race must not trigger replacement work"
    );
}

#[tokio::test]
async fn reconciliation_retains_partial_writes_and_allows_a_later_page_to_repair() {
    let first = work_item(FIRST_ID, "7", FIRST_TITLE, FIRST_CONTENT, FIRST_CONCEPT);
    let second = work_item(SECOND_ID, "9", SECOND_TITLE, SECOND_CONTENT, SECOND_CONCEPT);
    let first_repository = RecordingEmbeddingWorkRepository::new(RepositoryResponses {
        load_keys: Ok(Vec::new()),
        list_missing: Ok(vec![first.clone(), second.clone()]),
    });
    let first_router = ScriptedRouter::new(Ok(vec![
        generated(first.key(), vec![1.0]),
        generated(second.key(), vec![2.0]),
    ]));
    let first_writer = ScriptedWriter::new(
        vec![
            (FIRST_ID.to_owned(), Ok(WriteOutcome::Stored)),
            (
                SECOND_ID.to_owned(),
                Err(EmbeddingError::writer(WriterFailure::Backend)),
            ),
        ],
        None,
    );
    let first_attempt = reconciliation_processor(
        first_repository.clone(),
        first_router.clone(),
        first_writer.clone(),
        2,
    );

    let error = first_attempt
        .reconcile(cron_call(), Deadline::after(Duration::from_secs(1)))
        .await
        .expect_err("a partial immutable write must return an opaque reconciliation error");

    assert_eq!(error, EmbeddingError::writer(WriterFailure::Backend));
    assert_opaque(&error);
    assert_eq!(first_repository.calls().len(), 1);
    assert_eq!(first_router.calls().len(), 1);
    assert_eq!(
        first_writer.calls().len(),
        2,
        "admitted writes are retained"
    );

    let later_repository = RecordingEmbeddingWorkRepository::new(RepositoryResponses {
        load_keys: Ok(Vec::new()),
        list_missing: Ok(vec![second.clone()]),
    });
    let later_router = ScriptedRouter::new(Ok(vec![generated(second.key(), vec![2.0])]));
    let later_writer =
        ScriptedWriter::new(vec![(SECOND_ID.to_owned(), Ok(WriteOutcome::Stored))], None);
    let later_attempt = reconciliation_processor(
        later_repository.clone(),
        later_router.clone(),
        later_writer.clone(),
        2,
    );

    let outcome = later_attempt
        .reconcile(cron_call(), Deadline::after(Duration::from_secs(1)))
        .await
        .expect("a later selected page should repair the remaining work");

    assert_eq!(outcome, EmbeddingOutcome::new(1, 1, 0, 1));
    assert_eq!(later_repository.calls().len(), 1);
    assert_eq!(later_router.calls().len(), 1);
    assert_eq!(later_writer.calls().len(), 1);
}

#[tokio::test]
async fn reconciliation_reselects_a_poison_first_page_without_internal_retry_state() {
    let poison = work_item(FIRST_ID, "7", FIRST_TITLE, FIRST_CONTENT, FIRST_CONCEPT);
    let deadline = Deadline::after(Duration::from_secs(1));
    let repository = RecordingEmbeddingWorkRepository::new(RepositoryResponses {
        load_keys: Ok(Vec::new()),
        list_missing: Ok(vec![poison.clone()]),
    });
    let router = ScriptedRouter::new(Err(EmbeddingError::router(RouterFailure::Remote)));
    let writer = ScriptedWriter::new(Vec::new(), None);
    let service = reconciliation_processor(repository.clone(), router.clone(), writer.clone(), 1);

    for _ in 0..2 {
        let error = service
            .reconcile(cron_call(), deadline)
            .await
            .expect_err("a poison first page must remain retryable by a later invocation");
        assert_eq!(error, EmbeddingError::router(RouterFailure::Remote));
        assert_opaque(&error);
    }

    assert_eq!(
        repository.calls(),
        vec![
            RepositoryCall::ListMissing { limit: 1, deadline },
            RepositoryCall::ListMissing { limit: 1, deadline },
        ],
        "each invocation must select its page once without cursor or attempt state"
    );
    assert_eq!(
        router.calls().len(),
        2,
        "the processor must not retry internally"
    );
    assert!(writer.calls().is_empty());
}

#[tokio::test]
async fn reconciliation_propagates_the_caller_deadline_to_selection_and_processing() {
    let item = work_item(FIRST_ID, "7", FIRST_TITLE, FIRST_CONTENT, FIRST_CONCEPT);
    let (router, late_response) = LateResponseRouter::new();
    let repository = RecordingEmbeddingWorkRepository::new(RepositoryResponses {
        load_keys: Ok(Vec::new()),
        list_missing: Ok(vec![item]),
    });
    let writer = ScriptedWriter::new(Vec::new(), None);
    let service = ReconciliationProcessor::new(
        repository.clone(),
        EmbeddingCoordinator::new(
            router.clone(),
            writer.clone(),
            BATCH_LIMIT,
            usize::MAX,
            Arc::new(Semaphore::new(1)),
        ),
        1,
    );
    let deadline = Deadline::after(Duration::from_millis(50));

    let error = tokio::time::timeout(OUTER_TIMEOUT, service.reconcile(cron_call(), deadline))
        .await
        .expect("the caller deadline must bound reconciliation")
        .expect_err("a late router response must fail locally");
    let local_timeout_returned_at = Instant::now();

    assert_eq!(error, EmbeddingError::router(RouterFailure::Timeout));
    assert_opaque(&error);
    assert_eq!(
        repository.calls(),
        vec![RepositoryCall::ListMissing { limit: 1, deadline }]
    );
    assert_eq!(router.calls(), 1);
    assert!(writer.calls().is_empty());

    let late_response_at = tokio::time::timeout(OUTER_TIMEOUT, late_response)
        .await
        .expect("the simulated upstream should eventually respond")
        .expect("the simulated upstream should report its response time");
    assert!(late_response_at > deadline.instant());
    assert!(late_response_at > local_timeout_returned_at);
    assert!(writer.calls().is_empty());
}

#[tokio::test]
async fn process_routes_one_batch_and_returns_content_free_complete_counts() {
    let items = work_items();
    let router = ScriptedRouter::new(Ok(vec![
        generated(items[0].key(), vec![16_777_217.0, -2.5]),
        generated(items[1].key(), vec![3.25]),
    ]));
    let writer = ScriptedWriter::new(
        vec![
            (FIRST_ID.to_owned(), Ok(WriteOutcome::Stored)),
            (SECOND_ID.to_owned(), Ok(WriteOutcome::AlreadyPresent)),
        ],
        None,
    );
    let permits = Arc::new(Semaphore::new(1));
    let service = coordinator(
        router.clone(),
        writer.clone(),
        usize::MAX,
        Arc::clone(&permits),
    );
    let deadline = Deadline::after(Duration::from_secs(1));

    let outcome = service
        .process(&items, 1, deadline)
        .await
        .expect("complete work should succeed");

    assert_eq!(outcome, EmbeddingOutcome::new(3, 2, 2, 1));
    assert_eq!(
        router.calls(),
        vec![RouterCall {
            inputs: vec![
                render(&items[0], usize::MAX).expect("first item should render"),
                render(&items[1], usize::MAX).expect("second item should render"),
            ],
            deadline,
        }],
        "the coordinator must make one batch request with canonical inputs"
    );

    let calls = writer.calls();
    assert_eq!(
        calls.len(),
        2,
        "each generated vector should be written once"
    );
    let first = calls
        .iter()
        .find(|call| call.key == *items[0].key())
        .expect("first vector should retain its positional key");
    let second = calls
        .iter()
        .find(|call| call.key == *items[1].key())
        .expect("second vector should retain its positional key");
    assert_eq!(first.vector, vec![f64::from(16_777_217.0_f32), -2.5]);
    assert_eq!(second.vector, vec![3.25]);
    assert_eq!(first.deadline, deadline);
    assert_eq!(second.deadline, deadline);

    let permit = permits
        .try_acquire()
        .expect("a completed router call must release its shared permit");
    drop(permit);
}

#[tokio::test]
async fn process_returns_already_present_counts_without_routing_empty_pending_work() {
    let router = ScriptedRouter::new(Err(EmbeddingError::router(RouterFailure::Remote)));
    let writer = ScriptedWriter::new(Vec::new(), None);
    let service = coordinator(
        router.clone(),
        writer.clone(),
        usize::MAX,
        Arc::new(Semaphore::new(1)),
    );

    let outcome = service
        .process(&[], 2, Deadline::after(Duration::from_secs(1)))
        .await
        .expect("already-present work must complete without a router request");

    assert_eq!(outcome, EmbeddingOutcome::new(2, 0, 2, 0));
    assert!(router.calls().is_empty());
    assert!(writer.calls().is_empty());
}

#[tokio::test]
async fn process_rejects_work_above_its_batch_limit_before_rendering_or_backend_calls() {
    let items = work_items();
    let router = ScriptedRouter::new(Err(EmbeddingError::router(RouterFailure::Remote)));
    let writer = ScriptedWriter::new(Vec::new(), None);
    let service = EmbeddingCoordinator::new(
        router.clone(),
        writer.clone(),
        1,
        1,
        Arc::new(Semaphore::new(1)),
    );

    let error = service
        .process(&items, 0, Deadline::after(Duration::from_secs(1)))
        .await
        .expect_err("work above the coordinator batch limit must be rejected");

    assert_eq!(error, EmbeddingError::router(RouterFailure::CountMismatch));
    assert_opaque(&error);
    assert!(
        router.calls().is_empty(),
        "over-limit work must not reach the router"
    );
    assert!(
        writer.calls().is_empty(),
        "over-limit work must not reach the writer"
    );
}

#[tokio::test]
async fn process_continues_after_render_failure_and_returns_an_opaque_error_after_writes_settle() {
    let accepted = work_item(FIRST_ID, "7", FIRST_TITLE, FIRST_CONTENT, FIRST_CONCEPT);
    let rejected = work_item(
        SECOND_ID,
        "9",
        SECOND_TITLE,
        "service-oversized-content-sentinel".repeat(128),
        SECOND_CONCEPT,
    );
    let max_input_bytes = render(&accepted, usize::MAX)
        .expect("accepted item should render")
        .text()
        .len();
    let router = ScriptedRouter::new(Ok(vec![generated(accepted.key(), vec![1.0])]));
    let writer = ScriptedWriter::new(vec![(FIRST_ID.to_owned(), Ok(WriteOutcome::Stored))], None);
    let service = coordinator(
        router.clone(),
        writer.clone(),
        max_input_bytes,
        Arc::new(Semaphore::new(1)),
    );

    let error = service
        .process(
            &[accepted.clone(), rejected],
            0,
            Deadline::after(Duration::from_secs(1)),
        )
        .await
        .expect_err("a local render failure must make the batch fail after valid work settles");

    assert_eq!(
        error,
        EmbeddingError::render(memory_embedding::contracts::RenderFailure::InputTooLarge)
    );
    assert_opaque(&error);
    assert_eq!(
        router.calls().len(),
        1,
        "the valid item should still be routed"
    );
    assert_eq!(router.calls()[0].inputs.len(), 1);
    assert_eq!(
        writer.calls().len(),
        1,
        "the valid item should still be stored"
    );
}

#[tokio::test]
async fn process_settles_all_writes_before_returning_an_opaque_partial_write_error() {
    let items = work_items();
    let router = ScriptedRouter::new(Ok(vec![
        generated(items[0].key(), vec![1.0]),
        generated(items[1].key(), vec![2.0]),
    ]));
    let (writer, mut first_settled) = DelayedPartialWriter::new();
    let service = coordinator(
        router,
        writer.clone(),
        usize::MAX,
        Arc::new(Semaphore::new(1)),
    );
    let mut process = tokio::spawn(async move {
        service
            .process(&items, 0, Deadline::after(Duration::from_secs(1)))
            .await
    });

    tokio::time::timeout(OUTER_TIMEOUT, first_settled.changed())
        .await
        .expect("both delayed writes should be admitted before the first settles")
        .expect("first-settled notification sender should remain available");
    assert!(*first_settled.borrow());
    assert_eq!(
        writer.calls().len(),
        2,
        "both writes must be admitted together"
    );
    assert_eq!(writer.settled(), vec![FIRST_ID.to_owned()]);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut process)
            .await
            .is_err(),
        "the coordinator must wait for the delayed admitted write after the first failure"
    );

    writer.release_second();
    let error = tokio::time::timeout(OUTER_TIMEOUT, process)
        .await
        .expect("the delayed write should settle after release")
        .expect("coordinator task should not panic")
        .expect_err("a writer failure must reject a partial commit");

    assert_eq!(error, EmbeddingError::writer(WriterFailure::Backend));
    assert_opaque(&error);
    assert_eq!(writer.calls().len(), 2, "all admitted writes must settle");
    assert_eq!(
        writer.settled(),
        vec![FIRST_ID.to_owned(), SECOND_ID.to_owned()],
        "the coordinator must not return before every admitted write settles"
    );
}

#[tokio::test]
async fn process_rejects_invalid_router_results_before_any_write() {
    let items = work_items();

    for reason in [
        RouterFailure::ProviderMismatch,
        RouterFailure::ModelMismatch,
        RouterFailure::CountMismatch,
        RouterFailure::InvalidVector,
    ] {
        let router = ScriptedRouter::new(Err(EmbeddingError::router(reason)));
        let writer = ScriptedWriter::new(Vec::new(), None);
        let permits = Arc::new(Semaphore::new(1));
        let service = coordinator(
            router.clone(),
            writer.clone(),
            usize::MAX,
            Arc::clone(&permits),
        );

        let error = service
            .process(&items, 0, Deadline::after(Duration::from_secs(1)))
            .await
            .expect_err("an invalid router result must fail the whole response");

        assert_eq!(error, EmbeddingError::router(reason));
        assert_opaque(&error);
        assert_eq!(router.calls().len(), 1, "the router is attempted once");
        assert!(
            writer.calls().is_empty(),
            "no invalid vector may reach the writer"
        );
        let permit = permits
            .try_acquire()
            .expect("a failed router call must release its shared permit");
        drop(permit);
    }
}

#[tokio::test]
async fn process_writes_an_admitted_batch_concurrently() {
    let items = work_items();
    let router = ScriptedRouter::new(Ok(vec![
        generated(items[0].key(), vec![1.0]),
        generated(items[1].key(), vec![2.0]),
    ]));
    let writer = ScriptedWriter::new(
        vec![
            (FIRST_ID.to_owned(), Ok(WriteOutcome::Stored)),
            (SECOND_ID.to_owned(), Ok(WriteOutcome::Stored)),
        ],
        Some(Arc::new(Barrier::new(2))),
    );
    let service = coordinator(
        router,
        writer.clone(),
        usize::MAX,
        Arc::new(Semaphore::new(1)),
    );

    let outcome = tokio::time::timeout(
        OUTER_TIMEOUT,
        service.process(&items, 0, Deadline::after(Duration::from_secs(1))),
    )
    .await
    .expect("concurrent writes should reach the shared barrier")
    .expect("all writes should succeed");

    assert_eq!(outcome, EmbeddingOutcome::new(2, 2, 0, 2));
    assert_eq!(writer.calls().len(), 2);
}

#[tokio::test]
async fn process_returns_local_timeout_before_a_late_upstream_response_and_never_writes() {
    let items = work_items();
    let (router, late_response) = LateResponseRouter::new();
    let writer = ScriptedWriter::new(Vec::new(), None);
    let service = coordinator(
        router.clone(),
        writer.clone(),
        usize::MAX,
        Arc::new(Semaphore::new(1)),
    );
    let deadline = Deadline::after(Duration::from_millis(50));

    let error = tokio::time::timeout(OUTER_TIMEOUT, service.process(&items, 0, deadline))
        .await
        .expect("the absolute deadline must stop the local wait")
        .expect_err("a locally timed-out router call must fail");
    let local_timeout_returned_at = Instant::now();

    assert_eq!(error, EmbeddingError::router(RouterFailure::Timeout));
    assert_opaque(&error);
    assert_eq!(router.calls(), 1);
    assert!(writer.calls().is_empty());

    let late_response_at = tokio::time::timeout(OUTER_TIMEOUT, late_response)
        .await
        .expect("the independent simulated upstream should eventually respond")
        .expect("the simulated upstream should report its late response");
    assert!(
        late_response_at > deadline.instant(),
        "the simulated upstream response must be produced after the local deadline"
    );
    assert!(
        late_response_at > local_timeout_returned_at,
        "the local timeout must return before the simulated upstream response"
    );
    assert_eq!(router.late_response_at(), Some(late_response_at));
    assert!(
        writer.calls().is_empty(),
        "a late upstream response must not re-enter the writer"
    );
}

#[tokio::test]
async fn process_times_out_waiting_for_the_shared_router_permit_without_admitting_a_call() {
    let items = work_items();
    let router = ScriptedRouter::new(Ok(vec![
        generated(items[0].key(), vec![1.0]),
        generated(items[1].key(), vec![2.0]),
    ]));
    let writer = ScriptedWriter::new(Vec::new(), None);
    let permits = Arc::new(Semaphore::new(1));
    let held_permit = Arc::clone(&permits)
        .acquire_owned()
        .await
        .expect("the test should hold the only router permit");
    let service = coordinator(router.clone(), writer.clone(), usize::MAX, permits);

    let error = tokio::time::timeout(
        OUTER_TIMEOUT,
        service.process(&items, 0, Deadline::after(Duration::from_millis(50))),
    )
    .await
    .expect("permit waiting must honor the absolute deadline")
    .expect_err("a locally unavailable permit must fail the batch");

    assert_eq!(error, EmbeddingError::router(RouterFailure::Timeout));
    assert_opaque(&error);
    assert!(
        router.calls().is_empty(),
        "no router call may start without a permit"
    );
    assert!(writer.calls().is_empty());
    drop(held_permit);
}
