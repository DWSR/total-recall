use std::{
    ffi::OsString,
    fs, io,
    net::TcpListener,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use iii_sdk::{
    IIIClient, InitOptions, RegisterFunction, WorkerIdentityMode,
    engine::EngineFunctions,
    protocol::TriggerRequest,
    register_worker,
    runtime::{FunctionRef, IIIConnectionState, WorkerMetadata},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::{
    sync::oneshot,
    time::{Instant, sleep, sleep_until, timeout_at},
};
use uuid::Uuid;

use crate::{
    ArtifactPaths, E2eError, ProcessError, ProcessGuard, ProcessIdentity, ProcessPhase, ProcessSpec,
};

pub const CAPTURE_FUNCTION_IDS: [&str; 3] = [
    "harness::session_start",
    "harness::observation",
    "harness::session_end",
];

pub const DATABASE_FUNCTION_ID: &str = "database::execute";
pub const SEARCH_CONTEXT_SQL: &str = r#"WITH bounded_heads AS MATERIALIZED (
    SELECT head.id, head.version, head.updated_at
    FROM public.memory_search_heads AS head
    ORDER BY head.updated_at DESC, head.id COLLATE "C" ASC, head.version DESC
    LIMIT $1::text::bigint
), sized_candidates AS MATERIALIZED (
    SELECT bounded_heads.updated_at, bounded_heads.id, bounded_heads.version,
           memory.type AS memory_type, memory.title, memory.content, memory.concepts,
           octet_length(json_build_object('type', memory.type, 'title', memory.title,
               'content', memory.content, 'concepts', memory.concepts)::text) AS candidate_bytes,
           row_number() OVER (ORDER BY bounded_heads.updated_at DESC,
               bounded_heads.id COLLATE "C" ASC, bounded_heads.version DESC) AS candidate_position
    FROM bounded_heads
    JOIN public.memories AS memory ON memory.id = bounded_heads.id AND memory.version = bounded_heads.version
), candidates AS MATERIALIZED (
    SELECT sized_candidates.*,
           min(candidate_position) FILTER (WHERE candidate_bytes > $2::text::numeric) OVER ()
               AS first_oversized_position
    FROM sized_candidates
)
SELECT CASE WHEN candidate_bytes > $2::text::numeric THEN true ELSE false END AS oversized,
       CASE WHEN candidate_bytes > $2::text::numeric THEN NULL ELSE memory_type END AS memory_type,
       CASE WHEN candidate_bytes > $2::text::numeric THEN NULL ELSE title END AS title,
       CASE WHEN candidate_bytes > $2::text::numeric THEN NULL ELSE content END AS content,
       CASE WHEN candidate_bytes > $2::text::numeric THEN NULL ELSE to_json(concepts) END AS concepts,
       candidate_bytes::text AS candidate_bytes
FROM candidates
WHERE first_oversized_position IS NULL OR candidate_position <= first_oversized_position
ORDER BY updated_at DESC, id COLLATE "C" ASC, version DESC"#;
pub const CONTEXT_DATABASE_TARGET: &str = "total-recall-harness";
pub const CONTEXT_SCENARIO_REPLIES: [ContextScriptReply; 5] = [
    ContextScriptReply::Success,
    ContextScriptReply::Empty,
    ContextScriptReply::BackendFailure,
    ContextScriptReply::Malformed,
    ContextScriptReply::Delayed,
];

const CONTEXT_ROW_TYPE: &str = "context-type-private-sentinel";
const CONTEXT_ROW_TITLE: &str = "context-title-private-sentinel";
const CONTEXT_ROW_CONTENT: &str = "context-content-private-sentinel";
const CONTEXT_ROW_CONCEPT: &str = "context-concept-private-sentinel";
const CONTEXT_ROW_BYTES: &str = "128";
const CONTEXT_MALFORMED_REFERENCE: &str = "context-script-malformed";
const CONTEXT_DATABASE_INVALID_REQUEST: &str = "context_database_invalid_request";
const CONTEXT_DATABASE_BACKEND_FAILURE: &str = "context_database_backend_failure";
const CONTEXT_DATABASE_SCRIPT_EXHAUSTED: &str = "context_database_script_exhausted";
const CONTEXT_DELAY_PARK_BOUND: Duration = Duration::from_secs(5);

const ENGINE_NAMESPACE: &str = "opencode-harness-events-e2e";
const CATALOG_POLL_INTERVAL: Duration = Duration::from_millis(100);
const REGISTRATION_NONCE_METADATA_KEY: &str = "registration_nonce";
const REGISTRATION_WORKER_NAME_METADATA_KEY: &str = "worker_name";
const REGISTRATION_NAMESPACE_METADATA_KEY: &str = "namespace";

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CaptureRecord {
    pub function_id: String,
    pub namespace: String,
    pub payload: Value,
    pub order: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct CaptureSet {
    pub records: Vec<CaptureRecord>,
}

impl CaptureSet {
    pub fn since(&self, receive_index: usize) -> Self {
        Self {
            records: self
                .records
                .iter()
                .skip(receive_index)
                .enumerate()
                .map(|(order, record)| CaptureRecord {
                    function_id: record.function_id.clone(),
                    namespace: record.namespace.clone(),
                    payload: record.payload.clone(),
                    order,
                })
                .collect(),
        }
    }

    pub fn assert_ordered_function_ids(
        &self,
        expected: &[&str],
    ) -> Result<(), CaptureAssertionError> {
        if self.records.len() != expected.len() {
            return Err(CaptureAssertionError::Count {
                expected: expected.len(),
                actual: self.records.len(),
            });
        }

        for (index, (record, expected_function_id)) in
            self.records.iter().zip(expected.iter()).enumerate()
        {
            if record.order != index {
                return Err(CaptureAssertionError::Order { index });
            }
            if record.function_id != *expected_function_id {
                return Err(CaptureAssertionError::FunctionId {
                    index,
                    expected: (*expected_function_id).to_owned(),
                    actual: record.function_id.clone(),
                });
            }
        }

        Ok(())
    }

    pub fn assert_namespace(&self, expected: &str) -> Result<(), CaptureAssertionError> {
        for (index, record) in self.records.iter().enumerate() {
            if record.namespace != expected {
                return Err(CaptureAssertionError::Namespace { index });
            }
        }

        Ok(())
    }

    pub fn assert_payload(
        &self,
        function_id: &str,
        expected: &Value,
    ) -> Result<(), CaptureAssertionError> {
        let record = self
            .records
            .iter()
            .find(|record| record.function_id == function_id)
            .ok_or_else(|| CaptureAssertionError::MissingFunction {
                function_id: function_id.to_owned(),
            })?;
        if &record.payload != expected {
            return Err(CaptureAssertionError::Payload {
                function_id: function_id.to_owned(),
            });
        }

        Ok(())
    }

    pub fn assert_payload_contains(
        &self,
        function_id: &str,
        expected: &Value,
    ) -> Result<(), CaptureAssertionError> {
        let record = self
            .records
            .iter()
            .find(|record| record.function_id == function_id)
            .ok_or_else(|| CaptureAssertionError::MissingFunction {
                function_id: function_id.to_owned(),
            })?;
        if !value_contains(&record.payload, expected) {
            return Err(CaptureAssertionError::Payload {
                function_id: function_id.to_owned(),
            });
        }

        Ok(())
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum CaptureAssertionError {
    #[error("capture count did not match the expected count")]
    Count { expected: usize, actual: usize },
    #[error("capture order was not monotonic at index {index}")]
    Order { index: usize },
    #[error(
        "capture function id did not match at index {index}: expected {expected:?}, got {actual:?}"
    )]
    FunctionId {
        index: usize,
        expected: String,
        actual: String,
    },
    #[error("capture namespace did not match at index {index}")]
    Namespace { index: usize },
    #[error("capture did not contain function {function_id:?}")]
    MissingFunction { function_id: String },
    #[error("capture payload did not match for function {function_id:?}")]
    Payload { function_id: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineIdentity {
    pub worker_name: String,
    pub namespace: String,
    pub nonce: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineFileOperation {
    CreateWorkspace,
    ReserveLoopbackPort,
    WriteConfig,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineReadinessError {
    RegistrationRejected,
    Disconnected,
    CatalogRequestFailed,
    CatalogResponseMalformed,
    CatalogMismatch,
    Timeout,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum EngineError {
    #[error("engine file operation {operation:?} failed ({kind:?})")]
    File {
        operation: EngineFileOperation,
        kind: io::ErrorKind,
    },
    #[error(transparent)]
    Process(#[from] ProcessError),
    #[error("iii readiness failed ({kind:?})")]
    Readiness { kind: EngineReadinessError },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextScriptReply {
    Success,
    Empty,
    BackendFailure,
    Malformed,
    Delayed,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ContextDatabaseFailure {
    #[error("context database request did not match the worker request shape")]
    UnexpectedShape,
    #[error("context database request database target did not match the scripted target")]
    UnexpectedTarget,
    #[error("context database request did not carry the static worker statement")]
    UnexpectedSql,
    #[error("context database request did not carry the expected positional parameters")]
    UnexpectedParameters,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextLookupOutcome {
    Success,
    Empty,
    BackendFailure,
    Malformed,
    Delayed,
    Exhausted,
    Rejected(ContextDatabaseFailure),
}

#[derive(Clone, Debug)]
pub struct ContextDatabaseLookup {
    pub order: usize,
    pub payload: Value,
    pub outcome: ContextLookupOutcome,
}

#[derive(Clone, Debug)]
pub struct ContextDatabaseLookups {
    pub records: Vec<ContextDatabaseLookup>,
}

impl ContextDatabaseLookups {
    pub fn count(&self) -> usize {
        self.records.len()
    }

    pub fn assert_count(&self, expected: usize) -> Result<(), ContextLookupAssertionError> {
        if self.records.len() != expected {
            return Err(ContextLookupAssertionError::Count {
                expected,
                actual: self.records.len(),
            });
        }

        Ok(())
    }

    pub fn assert_outcome(
        &self,
        index: usize,
        expected: ContextLookupOutcome,
    ) -> Result<(), ContextLookupAssertionError> {
        if self.record(index)?.outcome != expected {
            return Err(ContextLookupAssertionError::Outcome { index });
        }

        Ok(())
    }

    pub fn assert_exact_request(
        &self,
        index: usize,
        target: &str,
        params: &[&str],
    ) -> Result<(), ContextLookupAssertionError> {
        let record = self.record(index)?;
        let Some(object) = record.payload.as_object() else {
            return Err(ContextLookupAssertionError::Shape { index });
        };
        if !object
            .keys()
            .all(|key| matches!(key.as_str(), "db" | "sql" | "params" | "_caller_worker_id"))
        {
            return Err(ContextLookupAssertionError::Shape { index });
        }
        if record
            .payload
            .get("_caller_worker_id")
            .is_some_and(|caller| caller.as_str().is_none())
        {
            return Err(ContextLookupAssertionError::Shape { index });
        }
        if record.payload.get("db").and_then(Value::as_str) != Some(target) {
            return Err(ContextLookupAssertionError::Target { index });
        }
        if record.payload.get("sql").and_then(Value::as_str) != Some(SEARCH_CONTEXT_SQL) {
            return Err(ContextLookupAssertionError::Sql { index });
        }
        let expected_params: Vec<Value> = params
            .iter()
            .map(|param| Value::String((*param).to_owned()))
            .collect();
        if record.payload.get("params") != Some(&Value::Array(expected_params)) {
            return Err(ContextLookupAssertionError::Parameters { index });
        }

        Ok(())
    }

    pub fn assert_payload_contains(
        &self,
        index: usize,
        expected: &Value,
    ) -> Result<(), ContextLookupAssertionError> {
        let record = self.record(index)?;
        if !value_contains(&record.payload, expected) {
            return Err(ContextLookupAssertionError::Payload { index });
        }

        Ok(())
    }

    fn record(&self, index: usize) -> Result<&ContextDatabaseLookup, ContextLookupAssertionError> {
        self.records
            .get(index)
            .ok_or(ContextLookupAssertionError::Missing { index })
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ContextLookupAssertionError {
    #[error("context lookup count did not match the expected count")]
    Count { expected: usize, actual: usize },
    #[error("context lookup was not recorded at index {index}")]
    Missing { index: usize },
    #[error("context lookup {index} payload did not match the worker request shape")]
    Shape { index: usize },
    #[error("context lookup {index} database target did not match the scripted target")]
    Target { index: usize },
    #[error("context lookup {index} sql did not match the static worker statement")]
    Sql { index: usize },
    #[error("context lookup {index} did not carry the expected parameters")]
    Parameters { index: usize },
    #[error("context lookup {index} served a different outcome than expected")]
    Outcome { index: usize },
    #[error("context lookup {index} payload did not contain the expected value")]
    Payload { index: usize },
}

#[derive(Clone)]
pub struct ContextDatabaseScript {
    config: Arc<ScriptConfig>,
    state: Arc<ScriptProgressState>,
}

#[derive(Clone)]
struct ScriptConfig {
    target: String,
    replies: ScriptReplies,
    delay_park_bound: Duration,
}

#[derive(Clone)]
enum ScriptReplies {
    Ordered(Vec<ContextScriptReply>),
    Deterministic,
}

struct ScriptProgressState {
    progress: Mutex<ScriptProgress>,
    settles: AtomicUsize,
}

enum ScriptOutcome {
    Immediate(Value),
    Parked(oneshot::Receiver<()>),
}

struct ScriptProgress {
    next: usize,
    parked: Vec<oneshot::Sender<()>>,
    lookups: Vec<ContextDatabaseLookup>,
}

impl ContextDatabaseScript {
    pub fn deterministic(target: impl Into<String>) -> Self {
        Self::build(ScriptConfig {
            target: target.into(),
            replies: ScriptReplies::Deterministic,
            delay_park_bound: CONTEXT_DELAY_PARK_BOUND,
        })
    }

    pub fn ordered(
        target: impl Into<String>,
        replies: impl IntoIterator<Item = ContextScriptReply>,
    ) -> Self {
        Self::build(ScriptConfig {
            target: target.into(),
            replies: ScriptReplies::Ordered(replies.into_iter().collect()),
            delay_park_bound: CONTEXT_DELAY_PARK_BOUND,
        })
    }

    pub fn with_delay_park_bound(mut self, delay_park_bound: Duration) -> Self {
        Arc::make_mut(&mut self.config).delay_park_bound = delay_park_bound;
        self
    }

    fn build(config: ScriptConfig) -> Self {
        Self {
            config: Arc::new(config),
            state: Arc::new(ScriptProgressState {
                progress: Mutex::new(ScriptProgress {
                    next: 0,
                    parked: Vec::new(),
                    lookups: Vec::new(),
                }),
                settles: AtomicUsize::new(0),
            }),
        }
    }

    pub async fn serve(&self, payload: Value) -> Result<Value, iii_sdk::Error> {
        let scripted = {
            let mut progress = self
                .state
                .progress
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Err(failure) = context_request_valid(&self.config.target, &payload) {
                let outcome = ContextLookupOutcome::Rejected(failure);
                let order = progress.lookups.len();
                progress.lookups.push(ContextDatabaseLookup {
                    order,
                    payload,
                    outcome,
                });
                return Err(remote_category_error(CONTEXT_DATABASE_INVALID_REQUEST));
            }

            let reply = match &self.config.replies {
                ScriptReplies::Deterministic => Some(ContextScriptReply::Success),
                ScriptReplies::Ordered(replies) => {
                    let next = progress.next;
                    progress.next = progress.next.saturating_add(1);
                    replies.get(next).copied()
                }
            };
            match reply {
                None => {
                    record_lookup(&mut progress, payload, ContextLookupOutcome::Exhausted);
                    return Err(remote_category_error(CONTEXT_DATABASE_SCRIPT_EXHAUSTED));
                }
                Some(ContextScriptReply::Success) => {
                    record_lookup(&mut progress, payload, ContextLookupOutcome::Success);
                    ScriptOutcome::Immediate(context_success_envelope())
                }
                Some(ContextScriptReply::Empty) => {
                    record_lookup(&mut progress, payload, ContextLookupOutcome::Empty);
                    ScriptOutcome::Immediate(context_empty_envelope())
                }
                Some(ContextScriptReply::BackendFailure) => {
                    record_lookup(&mut progress, payload, ContextLookupOutcome::BackendFailure);
                    return Err(remote_category_error(CONTEXT_DATABASE_BACKEND_FAILURE));
                }
                Some(ContextScriptReply::Malformed) => {
                    record_lookup(&mut progress, payload, ContextLookupOutcome::Malformed);
                    ScriptOutcome::Immediate(context_malformed_envelope())
                }
                Some(ContextScriptReply::Delayed) => {
                    let (sender, receiver) = oneshot::channel();
                    progress.parked.push(sender);
                    let order = progress.lookups.len();
                    progress.lookups.push(ContextDatabaseLookup {
                        order,
                        payload,
                        outcome: ContextLookupOutcome::Delayed,
                    });
                    ScriptOutcome::Parked(receiver)
                }
            }
        };

        match scripted {
            ScriptOutcome::Immediate(value) => Ok(value),
            ScriptOutcome::Parked(receiver) => self.settle_park(receiver).await,
        }
    }

    async fn settle_park(&self, receiver: oneshot::Receiver<()>) -> Result<Value, iii_sdk::Error> {
        let park_deadline = Instant::now() + self.config.delay_park_bound;
        tokio::select! {
            _ = receiver => {}
            _ = sleep_until(park_deadline) => {}
        }
        self.state.settles.fetch_add(1, Ordering::SeqCst);

        Ok(context_success_envelope())
    }

    pub fn release_parked_delay(&self) {
        let released = {
            let mut progress = self
                .state
                .progress
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut progress.parked)
        };
        for sender in released {
            let _ = sender.send(());
        }
    }

    pub fn parked_delay_settles(&self) -> usize {
        self.state.settles.load(Ordering::SeqCst)
    }

    pub fn lookups(&self) -> ContextDatabaseLookups {
        let progress = self
            .state
            .progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        ContextDatabaseLookups {
            records: progress.lookups.clone(),
        }
    }
}

fn record_lookup(progress: &mut ScriptProgress, payload: Value, outcome: ContextLookupOutcome) {
    let order = progress.lookups.len();
    progress.lookups.push(ContextDatabaseLookup {
        order,
        payload,
        outcome,
    });
}

fn context_request_valid(target: &str, payload: &Value) -> Result<(), ContextDatabaseFailure> {
    let Some(fields) = payload.as_object() else {
        return Err(ContextDatabaseFailure::UnexpectedShape);
    };
    if !fields
        .keys()
        .all(|key| matches!(key.as_str(), "db" | "sql" | "params" | "_caller_worker_id"))
    {
        return Err(ContextDatabaseFailure::UnexpectedShape);
    }
    if fields
        .get("_caller_worker_id")
        .map(|caller| caller.as_str().is_none())
        == Some(true)
    {
        return Err(ContextDatabaseFailure::UnexpectedShape);
    }
    let Some(db) = fields.get("db").and_then(Value::as_str) else {
        return Err(ContextDatabaseFailure::UnexpectedShape);
    };
    if db != target {
        return Err(ContextDatabaseFailure::UnexpectedTarget);
    }
    let Some(sql) = fields.get("sql").and_then(Value::as_str) else {
        return Err(ContextDatabaseFailure::UnexpectedShape);
    };
    if sql != SEARCH_CONTEXT_SQL {
        return Err(ContextDatabaseFailure::UnexpectedSql);
    }
    let Some(params) = fields.get("params").and_then(Value::as_array) else {
        return Err(ContextDatabaseFailure::UnexpectedShape);
    };
    if params.len() != 2 {
        return Err(ContextDatabaseFailure::UnexpectedParameters);
    }
    let limit = params[0]
        .as_str()
        .and_then(|value| value.parse::<u32>().ok());
    let bytes = params[1]
        .as_str()
        .and_then(|value| value.parse::<u64>().ok());
    if !limit.is_some_and(|value| value > 0) || !bytes.is_some_and(|value| value > 0) {
        return Err(ContextDatabaseFailure::UnexpectedParameters);
    }

    Ok(())
}

fn remote_category_error(category: &str) -> iii_sdk::Error {
    iii_sdk::Error::Remote {
        code: category.to_owned(),
        message: category.to_owned(),
        stacktrace: None,
    }
}

fn context_success_envelope() -> Value {
    json!({
        "affected_rows": 1,
        "last_insert_id": Value::Null,
        "returned_rows": [{
            "oversized": false,
            "memory_type": CONTEXT_ROW_TYPE,
            "title": CONTEXT_ROW_TITLE,
            "content": CONTEXT_ROW_CONTENT,
            "concepts": [CONTEXT_ROW_CONCEPT],
            "candidate_bytes": CONTEXT_ROW_BYTES,
        }],
    })
}

fn context_empty_envelope() -> Value {
    json!({
        "affected_rows": 0,
        "last_insert_id": Value::Null,
        "returned_rows": [],
    })
}

fn context_malformed_envelope() -> Value {
    json!({
        "affected_rows": 1,
        "last_insert_id": CONTEXT_MALFORMED_REFERENCE,
        "returned_rows": [],
    })
}

#[derive(Clone)]
pub struct EngineHarness {
    launch: ProcessSpec,
    context_script: ContextDatabaseScript,
}

impl EngineHarness {
    pub fn new(artifacts: &ArtifactPaths) -> Self {
        Self {
            launch: ProcessSpec::new(artifacts.iii.clone()),
            context_script: ContextDatabaseScript::deterministic(CONTEXT_DATABASE_TARGET),
        }
    }

    pub fn with_context_script(mut self, context_script: ContextDatabaseScript) -> Self {
        self.context_script = context_script;
        self
    }

    pub fn launch_spec(&self) -> &ProcessSpec {
        &self.launch
    }

    pub async fn start(self) -> Result<EngineSession, E2eError> {
        let identity = EngineIdentity {
            worker_name: format!(
                "harness-events-e2e-{}-{}",
                std::process::id(),
                Uuid::new_v4().simple()
            ),
            namespace: ENGINE_NAMESPACE.to_owned(),
            nonce: Uuid::new_v4(),
        };
        let workspace = TempWorkspace::create().map_err(E2eError::from)?;
        let (port_reservation, engine_port) = reserve_loopback_port().map_err(E2eError::from)?;
        let config_path = workspace.path.join("config.yaml");
        fs::write(&config_path, engine_config(engine_port)).map_err(|error| {
            E2eError::from(EngineError::File {
                operation: EngineFileOperation::WriteConfig,
                kind: error.kind(),
            })
        })?;

        let launch = configured_launch(self.launch, &config_path, &workspace.path);
        let process_result = ProcessGuard::spawn(launch.clone()).await;
        drop(port_reservation);
        let process = process_result
            .map_err(EngineError::from)
            .map_err(E2eError::from)?;

        let engine_url = format!("ws://127.0.0.1:{engine_port}");
        let client = register_capture_worker(&engine_url, &identity);
        let capture = Arc::new(Mutex::new(CaptureSet::default()));
        let mut function_refs = register_capture_functions(&client, &identity, &capture).to_vec();
        function_refs.push(register_database_function(
            &client,
            &identity,
            &self.context_script,
        ));
        let readiness = process
            .within_deadline(
                ProcessPhase::Readiness,
                wait_until_ready(
                    &client,
                    &identity,
                    process.deadline(ProcessPhase::Readiness),
                ),
            )
            .await;

        match readiness {
            Ok(Ok(())) => Ok(EngineSession {
                launch,
                identity,
                engine_url,
                process: Some(process),
                client: Some(client),
                capture,
                context_script: self.context_script,
                _function_refs: function_refs,
                _workspace: workspace,
            }),
            Ok(Err(error)) => {
                client.shutdown();
                let _ = process.terminate().await;
                Err(E2eError::from(error))
            }
            Err(error) => {
                client.shutdown();
                let _ = process.terminate().await;
                Err(E2eError::from(EngineError::Process(error)))
            }
        }
    }
}

pub struct EngineSession {
    launch: ProcessSpec,
    identity: EngineIdentity,
    engine_url: String,
    process: Option<ProcessGuard>,
    client: Option<IIIClient>,
    capture: Arc<Mutex<CaptureSet>>,
    context_script: ContextDatabaseScript,
    _function_refs: Vec<FunctionRef>,
    _workspace: TempWorkspace,
}

impl EngineSession {
    pub fn launch_spec(&self) -> &ProcessSpec {
        &self.launch
    }

    pub fn iii_path(&self) -> &Path {
        &self.launch.program
    }

    pub fn engine_url(&self) -> &str {
        &self.engine_url
    }

    pub fn client(&self) -> &IIIClient {
        self.client
            .as_ref()
            .expect("engine session client exists while the session is live")
    }

    pub fn identity(&self) -> &EngineIdentity {
        &self.identity
    }

    pub fn process_identity(&self) -> ProcessIdentity {
        self.process
            .as_ref()
            .expect("engine session process exists while the session is live")
            .identity()
    }

    pub fn ready(&self) -> bool {
        self.client
            .as_ref()
            .is_some_and(|client| client.get_connection_state() == IIIConnectionState::Connected)
    }

    pub fn capture(&self) -> CaptureSet {
        self.capture
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn capture_since(&self, receive_index: usize) -> CaptureSet {
        self.capture().since(receive_index)
    }

    pub fn context_lookups(&self) -> ContextDatabaseLookups {
        self.context_script.lookups()
    }

    pub fn release_parked_delay(&self) {
        self.context_script.release_parked_delay();
    }

    pub fn parked_delay_settles(&self) -> usize {
        self.context_script.parked_delay_settles()
    }

    pub async fn shutdown(mut self) -> Result<(), E2eError> {
        if let Some(client) = self.client.take() {
            client.shutdown();
        }
        if let Some(process) = self.process.take() {
            process
                .terminate()
                .await
                .map_err(EngineError::from)
                .map_err(E2eError::from)?;
        }

        Ok(())
    }
}

impl Drop for EngineSession {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            client.shutdown();
        }
        if let Some(process) = self.process.take() {
            drop(process);
        }
    }
}

#[derive(Debug)]
struct TempWorkspace {
    path: PathBuf,
}

impl TempWorkspace {
    fn create() -> Result<Self, EngineError> {
        let base = std::env::temp_dir();
        for _ in 0..8 {
            let path = base.join(format!(
                "opencode-harness-events-iii-{}-{}",
                std::process::id(),
                Uuid::new_v4().simple()
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(EngineError::File {
                        operation: EngineFileOperation::CreateWorkspace,
                        kind: error.kind(),
                    });
                }
            }
        }

        Err(EngineError::File {
            operation: EngineFileOperation::CreateWorkspace,
            kind: io::ErrorKind::AlreadyExists,
        })
    }
}

impl Drop for TempWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn configured_launch(
    mut launch: ProcessSpec,
    config_path: &Path,
    working_directory: &Path,
) -> ProcessSpec {
    launch.arguments = vec![
        OsString::from("--config"),
        config_path.as_os_str().to_os_string(),
        OsString::from("--no-update-check"),
    ];
    launch.environment = vec![(
        OsString::from("III_TELEMETRY_ENABLED"),
        OsString::from("false"),
    )];
    launch.current_directory = Some(working_directory.to_path_buf());
    launch
}

fn engine_config(port: u16) -> String {
    format!(
        "workers:\n  - name: iii-worker-manager\n    config:\n      host: 127.0.0.1\n      port: {port}\n"
    )
}

fn reserve_loopback_port() -> Result<(TcpListener, u16), EngineError> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).map_err(|error| EngineError::File {
        operation: EngineFileOperation::ReserveLoopbackPort,
        kind: error.kind(),
    })?;
    let port = listener
        .local_addr()
        .map_err(|error| EngineError::File {
            operation: EngineFileOperation::ReserveLoopbackPort,
            kind: error.kind(),
        })?
        .port();
    Ok((listener, port))
}

fn register_capture_worker(engine_url: &str, identity: &EngineIdentity) -> IIIClient {
    let mut metadata = WorkerMetadata::default();
    metadata.name.clone_from(&identity.worker_name);
    metadata.namespace = Some(identity.namespace.clone());

    let mut options = InitOptions {
        metadata: Some(metadata),
        headers: None,
        otel: Some(Default::default()),
        namespace: Some(identity.namespace.clone()),
        identity: WorkerIdentityMode::Explicit,
    };
    if let Some(otel) = options.otel.as_mut() {
        otel.enabled = Some(false);
    }

    register_worker(engine_url, options)
}

fn register_capture_functions(
    client: &IIIClient,
    identity: &EngineIdentity,
    capture: &Arc<Mutex<CaptureSet>>,
) -> [FunctionRef; 3] {
    std::array::from_fn(|index| {
        let function_id = CAPTURE_FUNCTION_IDS[index];
        let namespace = identity.namespace.clone();
        let capture = Arc::clone(capture);
        client.register_function(
            function_id,
            RegisterFunction::new_async(move |payload: Value| {
                let namespace = namespace.clone();
                let capture = Arc::clone(&capture);
                async move {
                    let mut capture = capture
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let order = capture.records.len();
                    capture.records.push(CaptureRecord {
                        function_id: function_id.to_owned(),
                        namespace,
                        payload,
                        order,
                    });
                    Ok(json!({"dispatched": true}))
                }
            })
            .metadata(registration_metadata(identity)),
        )
    })
}

fn register_database_function(
    client: &IIIClient,
    identity: &EngineIdentity,
    script: &ContextDatabaseScript,
) -> FunctionRef {
    let script = script.clone();
    client.register_function(
        DATABASE_FUNCTION_ID,
        RegisterFunction::new_async(move |payload: Value| {
            let script = script.clone();
            async move { script.serve(payload).await }
        })
        .metadata(registration_metadata(identity)),
    )
}

async fn wait_until_ready(
    client: &IIIClient,
    identity: &EngineIdentity,
    timeout: Duration,
) -> Result<(), EngineError> {
    let deadline = Instant::now() + timeout;
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(readiness_error(EngineReadinessError::Timeout));
    }

    client
        .wait_until_registered(remaining)
        .await
        .map_err(|error| map_registration_error(client, error))?;
    ensure_connected(client)?;

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(readiness_error(EngineReadinessError::Timeout));
        }

        let response = timeout_at(
            deadline,
            client.trigger(TriggerRequest {
                function_id: EngineFunctions::INFO_FUNCTIONS.to_owned(),
                payload: json!({
                    "function_ids": CAPTURE_FUNCTION_IDS,
                    "namespace": identity.namespace,
                }),
                action: None,
                timeout_ms: Some(timeout_millis(remaining)),
            }),
        )
        .await
        .map_err(|_| readiness_error(EngineReadinessError::Timeout))?
        .map_err(|error| map_catalog_error(client, error))?;
        match parse_catalog_response(response)? {
            CatalogProbe::Pending => wait_for_catalog_poll(deadline).await?,
            CatalogProbe::Entries(entries) if catalog_matches(identity, &entries) => {
                ensure_connected(client)?;
                await_database_catalog(client, identity, deadline).await?;
                ensure_connected(client)?;
                return Ok(());
            }
            CatalogProbe::Entries(entries) if entries.len() < CAPTURE_FUNCTION_IDS.len() => {
                wait_for_catalog_poll(deadline).await?;
            }
            CatalogProbe::Entries(_) => {
                return Err(readiness_error(EngineReadinessError::CatalogMismatch));
            }
        }
    }
}

async fn await_database_catalog(
    client: &IIIClient,
    identity: &EngineIdentity,
    deadline: Instant,
) -> Result<(), EngineError> {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(readiness_error(EngineReadinessError::Timeout));
        }

        let response = timeout_at(
            deadline,
            client.trigger(TriggerRequest {
                function_id: EngineFunctions::INFO_FUNCTIONS.to_owned(),
                payload: json!({
                    "function_ids": [DATABASE_FUNCTION_ID],
                    "namespace": identity.namespace,
                }),
                action: None,
                timeout_ms: Some(timeout_millis(remaining)),
            }),
        )
        .await
        .map_err(|_| readiness_error(EngineReadinessError::Timeout))?
        .map_err(|error| map_catalog_error(client, error))?;
        match parse_catalog_response(response)? {
            CatalogProbe::Pending => wait_for_catalog_poll(deadline).await?,
            CatalogProbe::Entries(entries) if database_catalog_owned(identity, &entries) => {
                return Ok(());
            }
            CatalogProbe::Entries(_) => {
                return Err(readiness_error(EngineReadinessError::CatalogMismatch));
            }
        }
    }
}

fn ensure_connected(client: &IIIClient) -> Result<(), EngineError> {
    if client.fatal_error().is_some() {
        return Err(readiness_error(EngineReadinessError::RegistrationRejected));
    }
    if client.get_connection_state() != IIIConnectionState::Connected {
        return Err(readiness_error(EngineReadinessError::Disconnected));
    }

    Ok(())
}

async fn wait_for_catalog_poll(deadline: Instant) -> Result<(), EngineError> {
    let next_poll = std::cmp::min(deadline, Instant::now() + CATALOG_POLL_INTERVAL);
    sleep(next_poll.saturating_duration_since(Instant::now())).await;
    if Instant::now() >= deadline {
        return Err(readiness_error(EngineReadinessError::Timeout));
    }

    Ok(())
}

fn map_registration_error(client: &IIIClient, error: iii_sdk::Error) -> EngineError {
    if client.fatal_error().is_some()
        || matches!(error, iii_sdk::Error::RegistrationRejected { .. })
    {
        readiness_error(EngineReadinessError::RegistrationRejected)
    } else if matches!(error, iii_sdk::Error::Timeout) {
        readiness_error(EngineReadinessError::Timeout)
    } else {
        readiness_error(EngineReadinessError::Disconnected)
    }
}

fn map_catalog_error(client: &IIIClient, error: iii_sdk::Error) -> EngineError {
    if client.fatal_error().is_some()
        || matches!(error, iii_sdk::Error::RegistrationRejected { .. })
    {
        readiness_error(EngineReadinessError::RegistrationRejected)
    } else if client.get_connection_state() != IIIConnectionState::Connected {
        readiness_error(EngineReadinessError::Disconnected)
    } else if matches!(error, iii_sdk::Error::Timeout) {
        readiness_error(EngineReadinessError::Timeout)
    } else {
        readiness_error(EngineReadinessError::CatalogRequestFailed)
    }
}

fn readiness_error(kind: EngineReadinessError) -> EngineError {
    EngineError::Readiness { kind }
}

fn timeout_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128).max(1) as u64
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct FunctionCatalogEntry {
    function_id: String,
    namespace: String,
    worker_name: String,
    #[serde(default)]
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct FunctionCatalogResponse {
    functions: Vec<Value>,
}

#[derive(Debug, Eq, PartialEq)]
enum CatalogProbe {
    Pending,
    Entries(Vec<FunctionCatalogEntry>),
}

fn parse_catalog_response(value: Value) -> Result<CatalogProbe, EngineError> {
    let response = serde_json::from_value::<FunctionCatalogResponse>(value)
        .map_err(|_| readiness_error(EngineReadinessError::CatalogResponseMalformed))?;
    if response
        .functions
        .iter()
        .any(|function| function.get("error").is_some())
    {
        return Ok(CatalogProbe::Pending);
    }

    let entries = response
        .functions
        .into_iter()
        .map(|function| {
            serde_json::from_value(function)
                .map_err(|_| readiness_error(EngineReadinessError::CatalogResponseMalformed))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CatalogProbe::Entries(entries))
}

fn catalog_matches(identity: &EngineIdentity, catalog: &[FunctionCatalogEntry]) -> bool {
    if catalog.len() != CAPTURE_FUNCTION_IDS.len() {
        return false;
    }

    let mut ids = catalog
        .iter()
        .map(|entry| entry.function_id.as_str())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    let mut expected_ids = CAPTURE_FUNCTION_IDS.to_vec();
    expected_ids.sort_unstable();
    if ids != expected_ids {
        return false;
    }

    let expected_nonce = identity.nonce.to_string();
    catalog.iter().all(|entry| {
        entry.namespace == identity.namespace
            && entry.worker_name == identity.worker_name
            && entry
                .metadata
                .as_ref()
                .and_then(Value::as_object)
                .and_then(|metadata| metadata.get(REGISTRATION_NONCE_METADATA_KEY))
                .and_then(Value::as_str)
                == Some(expected_nonce.as_str())
    })
}

fn database_catalog_owned(identity: &EngineIdentity, catalog: &[FunctionCatalogEntry]) -> bool {
    if catalog.len() != 1 {
        return false;
    }

    let expected_nonce = identity.nonce.to_string();
    catalog.iter().all(|entry| {
        entry.function_id == DATABASE_FUNCTION_ID
            && entry.namespace == identity.namespace
            && entry.worker_name == identity.worker_name
            && entry
                .metadata
                .as_ref()
                .and_then(Value::as_object)
                .and_then(|metadata| metadata.get(REGISTRATION_NONCE_METADATA_KEY))
                .and_then(Value::as_str)
                == Some(expected_nonce.as_str())
    })
}

fn registration_metadata(identity: &EngineIdentity) -> Value {
    json!({
        REGISTRATION_NONCE_METADATA_KEY: identity.nonce.to_string(),
        REGISTRATION_WORKER_NAME_METADATA_KEY: identity.worker_name,
        REGISTRATION_NAMESPACE_METADATA_KEY: identity.namespace,
    })
}

fn value_contains(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (Value::Object(actual), Value::Object(expected)) => expected.iter().all(|(key, value)| {
            actual
                .get(key)
                .is_some_and(|actual| value_contains(actual, value))
        }),
        _ => actual == expected,
    }
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsString, path::PathBuf, time::Duration};

    use iii_sdk::{
        IIIClient, InitOptions, WorkerIdentityMode, protocol::TriggerRequest, register_worker,
        runtime::WorkerMetadata,
    };
    use serde_json::{Value, json};
    use tokio::time::{Instant, sleep};
    use uuid::Uuid;

    use super::{
        ArtifactPaths, CAPTURE_FUNCTION_IDS, CONTEXT_DATABASE_BACKEND_FAILURE,
        CONTEXT_DATABASE_INVALID_REQUEST, CONTEXT_DATABASE_SCRIPT_EXHAUSTED,
        CONTEXT_DATABASE_TARGET, CONTEXT_ROW_CONCEPT, CONTEXT_ROW_CONTENT, CONTEXT_ROW_TITLE,
        CONTEXT_ROW_TYPE, CONTEXT_SCENARIO_REPLIES, CaptureSet, ContextDatabaseFailure,
        ContextDatabaseLookup, ContextDatabaseLookups, ContextDatabaseScript, ContextLookupOutcome,
        ContextScriptReply, DATABASE_FUNCTION_ID, EngineHarness, EngineIdentity, EngineSession,
        FunctionCatalogEntry, ProcessSpec, SEARCH_CONTEXT_SQL, catalog_matches, configured_launch,
        context_empty_envelope, context_malformed_envelope, context_success_envelope,
        database_catalog_owned, engine_config, parse_catalog_response, registration_metadata,
    };

    fn assert_served_envelope(
        reply: Result<Value, iii_sdk::Error>,
        expected: Value,
        expectation: &str,
    ) {
        match reply {
            Ok(value) => assert_eq!(value, expected, "{expectation}"),
            Err(error) => panic!("unexpected database script failure while {expectation}: {error}"),
        }
    }

    #[test]
    fn launch_uses_real_iii_config_and_update_check_arguments() {
        let launch = configured_launch(
            ProcessSpec::new(PathBuf::from("iii")),
            PathBuf::from("/tmp/iii/config.yaml").as_path(),
            PathBuf::from("/tmp/iii").as_path(),
        );

        assert_eq!(
            launch.arguments,
            vec![
                OsString::from("--config"),
                OsString::from("/tmp/iii/config.yaml"),
                OsString::from("--no-update-check"),
            ]
        );
        assert_eq!(launch.current_directory, Some(PathBuf::from("/tmp/iii")));
        assert_eq!(
            launch.environment,
            vec![(
                OsString::from("III_TELEMETRY_ENABLED"),
                OsString::from("false")
            )]
        );
        assert!(engine_config(49134).contains("name: iii-worker-manager"));
        assert!(engine_config(49134).contains("host: 127.0.0.1"));
        assert!(engine_config(49134).contains("port: 49134"));
    }

    #[test]
    fn registration_metadata_contains_the_worker_namespace_and_nonce() {
        let identity = identity();
        assert_eq!(
            registration_metadata(&identity),
            json!({
                "registration_nonce": identity.nonce.to_string(),
                "worker_name": "worker",
                "namespace": "namespace",
            })
        );
    }

    #[test]
    fn catalog_parser_distinguishes_pending_and_owned_entries() {
        let identity = identity();
        let pending = parse_catalog_response(json!({
            "functions": [{"function_id": CAPTURE_FUNCTION_IDS[0], "error": "pending"}],
        }))
        .expect("pending catalog parses");
        assert_eq!(pending, super::CatalogProbe::Pending);

        let entries = CAPTURE_FUNCTION_IDS
            .iter()
            .map(|function_id| {
                json!({
                    "function_id": function_id,
                    "namespace": identity.namespace,
                    "worker_name": identity.worker_name,
                    "metadata": {"registration_nonce": identity.nonce.to_string()},
                })
            })
            .collect::<Vec<_>>();
        let parsed =
            parse_catalog_response(json!({"functions": entries})).expect("owned catalog parses");
        assert!(matches!(parsed, super::CatalogProbe::Entries(_)));
        let super::CatalogProbe::Entries(entries) = parsed else {
            unreachable!("catalog entries were checked above");
        };
        assert!(catalog_matches(&identity, &entries));
    }

    #[test]
    fn catalog_matching_rejects_stale_or_foreign_ownership() {
        let identity = identity();
        let stale = CAPTURE_FUNCTION_IDS
            .iter()
            .map(|function_id| FunctionCatalogEntry {
                function_id: (*function_id).to_owned(),
                namespace: identity.namespace.clone(),
                worker_name: identity.worker_name.clone(),
                metadata: Some(json!({"registration_nonce": Uuid::new_v4().to_string()})),
            })
            .collect::<Vec<_>>();
        assert!(!catalog_matches(&identity, &stale));

        let mut foreign = stale;
        for entry in &mut foreign {
            entry.metadata = Some(json!({"registration_nonce": identity.nonce.to_string()}));
        }
        foreign[0].worker_name = "foreign".to_owned();
        assert!(!catalog_matches(&identity, &foreign));
    }

    #[test]
    fn capture_set_assertions_cover_order_namespace_and_payload_without_payload_errors() {
        let capture = CaptureSet {
            records: vec![super::CaptureRecord {
                function_id: CAPTURE_FUNCTION_IDS[0].to_owned(),
                namespace: "namespace".to_owned(),
                payload: json!({"sentinel": true}),
                order: 0,
            }],
        };
        capture
            .assert_ordered_function_ids(&[CAPTURE_FUNCTION_IDS[0]])
            .expect("function order matches");
        capture
            .assert_namespace("namespace")
            .expect("namespace matches");
        capture
            .assert_payload(CAPTURE_FUNCTION_IDS[0], &json!({"sentinel": true}))
            .expect("payload matches");
        let error = capture
            .assert_payload(CAPTURE_FUNCTION_IDS[0], &json!({"sentinel": false}))
            .expect_err("payload mismatch is typed");
        assert_eq!(
            error.to_string(),
            "capture payload did not match for function \"harness::session_start\""
        );
    }

    #[test]
    fn capture_set_since_filters_receive_records_and_rebases_local_order() {
        let capture = CaptureSet {
            records: CAPTURE_FUNCTION_IDS
                .iter()
                .enumerate()
                .map(|(order, function_id)| super::CaptureRecord {
                    function_id: (*function_id).to_owned(),
                    namespace: "namespace".to_owned(),
                    payload: json!({"order": order}),
                    order,
                })
                .collect(),
        };

        let filtered = capture.since(1);

        assert_eq!(filtered.records.len(), 2);
        assert_eq!(filtered.records[0].order, 0);
        assert_eq!(filtered.records[1].order, 1);
        assert_eq!(filtered.records[0].function_id, CAPTURE_FUNCTION_IDS[1]);
        assert_eq!(filtered.records[1].function_id, CAPTURE_FUNCTION_IDS[2]);
        filtered
            .assert_ordered_function_ids(&[CAPTURE_FUNCTION_IDS[1], CAPTURE_FUNCTION_IDS[2]])
            .expect("filtered records preserve local receive order");
    }

    fn identity() -> EngineIdentity {
        EngineIdentity {
            worker_name: "worker".to_owned(),
            namespace: "namespace".to_owned(),
            nonce: Uuid::parse_str("00000000-0000-4000-8000-000000000001")
                .expect("fixture nonce is valid"),
        }
    }

    const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);
    const INVOCATION_TIMEOUT_MS: u64 = 5_000;
    const DELAY_INVOCATION_TIMEOUT_MS: u64 = 1_500;
    const RELEASE_SETTLE_TIMEOUT: Duration = Duration::from_secs(2);
    const RELEASE_SETTLE_POLL_INTERVAL: Duration = Duration::from_millis(10);
    const SCRIPT_PARK_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
    const SCRIPT_PARK_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(5);

    #[tokio::test(flavor = "current_thread")]
    async fn deterministic_script_serves_the_exact_context_envelope_repeatedly() {
        let script = ContextDatabaseScript::deterministic(CONTEXT_DATABASE_TARGET);
        let request = json!({
            "db": CONTEXT_DATABASE_TARGET,
            "sql": SEARCH_CONTEXT_SQL,
            "params": ["3", "4096"],
        });

        for _ in 0..2 {
            let reply = script.serve(request.clone()).await;
            assert_served_envelope(
                reply,
                context_success_envelope(),
                "the deterministic success",
            )
        }

        let lookups = script.lookups();
        lookups
            .assert_count(2)
            .expect("both deterministic lookups are recorded");
        for index in 0..2 {
            lookups
                .assert_exact_request(index, CONTEXT_DATABASE_TARGET, &["3", "4096"])
                .expect("every lookup matches the exact worker request");
            lookups
                .assert_outcome(index, ContextLookupOutcome::Success)
                .expect("every lookup serves the deterministic success");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn database_script_rejections_are_categorized_without_payload_details() {
        let script = ContextDatabaseScript::deterministic(CONTEXT_DATABASE_TARGET);
        let rejects = [
            (
                json!({"repo-one": true, "db": CONTEXT_DATABASE_TARGET, "sql": SEARCH_CONTEXT_SQL, "params": ["3", "4096"]}),
                ContextLookupOutcome::Rejected(ContextDatabaseFailure::UnexpectedShape),
                "repo-one",
            ),
            (
                json!({"db": "not-the-scripted-target", "sql": SEARCH_CONTEXT_SQL, "params": ["3", "4096"]}),
                ContextLookupOutcome::Rejected(ContextDatabaseFailure::UnexpectedTarget),
                "not-the-scripted-target",
            ),
            (
                json!({"db": CONTEXT_DATABASE_TARGET, "sql": "SELECT NOT-THE-STATIC-STATEMENT", "params": ["3", "4096"]}),
                ContextLookupOutcome::Rejected(ContextDatabaseFailure::UnexpectedSql),
                "NOT-THE-STATIC-STATEMENT",
            ),
            (
                json!({"db": CONTEXT_DATABASE_TARGET, "sql": SEARCH_CONTEXT_SQL, "params": ["0", "4096"]}),
                ContextLookupOutcome::Rejected(ContextDatabaseFailure::UnexpectedParameters),
                "zzz-params-below-zero",
            ),
            (
                json!({"db": CONTEXT_DATABASE_TARGET, "sql": SEARCH_CONTEXT_SQL, "params": ["3", "abc"]}),
                ContextLookupOutcome::Rejected(ContextDatabaseFailure::UnexpectedParameters),
                "zzz-params-alphabet",
            ),
            (
                json!({"db": CONTEXT_DATABASE_TARGET, "sql": SEARCH_CONTEXT_SQL, "params": ["3", "4096", "3"]}),
                ContextLookupOutcome::Rejected(ContextDatabaseFailure::UnexpectedParameters),
                "zzz-params-extra",
            ),
            (
                json!({"db": CONTEXT_DATABASE_TARGET, "sql": SEARCH_CONTEXT_SQL, "params": ["3"]}),
                ContextLookupOutcome::Rejected(ContextDatabaseFailure::UnexpectedParameters),
                "zzz-params-short",
            ),
        ];

        for (request, _outcome, sentinel) in &rejects {
            let reply = script.serve(request.clone()).await;
            let Err(iii_sdk::Error::Remote {
                code,
                message,
                stacktrace,
            }) = reply
            else {
                panic!("a rejected request must serve a typed remote rejection");
            };
            assert_eq!(code, CONTEXT_DATABASE_INVALID_REQUEST);
            assert_eq!(message, CONTEXT_DATABASE_INVALID_REQUEST);
            assert_eq!(stacktrace, None);
            assert!(
                !code.contains(sentinel) && !message.contains(sentinel),
                "rejection categories must not carry request details ({sentinel})"
            );
        }

        let lookups = script.lookups();
        lookups
            .assert_count(7)
            .expect("every rejected request is recorded");
        for (index, (_, expected, _)) in rejects.iter().enumerate() {
            lookups
                .assert_outcome(index, *expected)
                .expect("each rejection is categorized");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scripted_lookups_serve_the_ordered_scenario_once_each_and_then_exhaust() {
        let script =
            ContextDatabaseScript::ordered(CONTEXT_DATABASE_TARGET, CONTEXT_SCENARIO_REPLIES);
        let request = json!({
            "db": CONTEXT_DATABASE_TARGET,
            "sql": SEARCH_CONTEXT_SQL,
            "params": ["3", "4096"],
        });

        let success = script.serve(request.clone()).await;
        assert_served_envelope(
            success,
            context_success_envelope(),
            "the scripted success reply",
        );
        let empty = script.serve(request.clone()).await;
        assert_served_envelope(empty, context_empty_envelope(), "the scripted empty reply");
        let backend = script.serve(request.clone()).await;
        let Err(iii_sdk::Error::Remote {
            code,
            message,
            stacktrace,
        }) = backend
        else {
            panic!("the backend-failure script must surface a remote error");
        };
        assert_eq!(code, CONTEXT_DATABASE_BACKEND_FAILURE);
        assert_eq!(message, CONTEXT_DATABASE_BACKEND_FAILURE);
        assert_eq!(stacktrace, None);
        for row_sentinel in [
            CONTEXT_ROW_TYPE,
            CONTEXT_ROW_TITLE,
            CONTEXT_ROW_CONTENT,
            CONTEXT_ROW_CONCEPT,
        ] {
            assert!(!code.contains(row_sentinel) && !message.contains(row_sentinel));
        }
        let malformed = script.serve(request.clone()).await;
        assert_served_envelope(
            malformed,
            context_malformed_envelope(),
            "the scripted malformed reply",
        );

        let parked_task = tokio::spawn({
            let script = script.clone();
            let request = request.clone();
            async move { script.serve(request).await }
        });
        wait_for_script_park(&script).await;
        script.release_parked_delay();
        let parked = parked_task
            .await
            .expect("the parked scripted lookup resolves");
        assert_served_envelope(
            parked,
            context_success_envelope(),
            "the parked scripted reply",
        );
        assert_eq!(script.parked_delay_settles(), 1);

        let exhausted = script.serve(request).await;
        let Err(iii_sdk::Error::Remote {
            code,
            message,
            stacktrace,
        }) = exhausted
        else {
            panic!("an exhausted script must surface a fixed remote failure");
        };
        assert_eq!(code, CONTEXT_DATABASE_SCRIPT_EXHAUSTED);
        assert_eq!(message, CONTEXT_DATABASE_SCRIPT_EXHAUSTED);
        assert_eq!(stacktrace, None);

        let lookups = script.lookups();
        lookups
            .assert_count(6)
            .expect("the full ordered scenario is recorded");
        let outcomes = [
            ContextLookupOutcome::Success,
            ContextLookupOutcome::Empty,
            ContextLookupOutcome::BackendFailure,
            ContextLookupOutcome::Malformed,
            ContextLookupOutcome::Delayed,
            ContextLookupOutcome::Exhausted,
        ];
        for (index, expected) in outcomes.iter().enumerate() {
            lookups
                .assert_outcome(index, *expected)
                .expect("ordered script outcomes run in sequence");
            lookups
                .assert_exact_request(index, CONTEXT_DATABASE_TARGET, &["3", "4096"])
                .expect("every scripted lookup matches the exact worker request");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn parked_delay_serves_after_the_bounded_expiry_settles_without_a_release() {
        let script =
            ContextDatabaseScript::ordered(CONTEXT_DATABASE_TARGET, [ContextScriptReply::Delayed])
                .with_delay_park_bound(Duration::from_millis(80));
        let request = json!({
            "db": CONTEXT_DATABASE_TARGET,
            "sql": SEARCH_CONTEXT_SQL,
            "params": ["3", "4096"],
        });

        let reply = script.serve(request.clone()).await;
        assert_served_envelope(
            reply,
            context_success_envelope(),
            "the bounded parked reply",
        );
        assert_eq!(script.parked_delay_settles(), 1);

        let lookups = script.lookups();
        lookups
            .assert_outcome(0, ContextLookupOutcome::Delayed)
            .expect("the bounded parked lookup is recorded");
        lookups
            .assert_exact_request(0, CONTEXT_DATABASE_TARGET, &["3", "4096"])
            .expect("the parked lookup request is captured exactly");

        let exhausted = script.serve(request).await;
        assert!(matches!(exhausted, Err(iii_sdk::Error::Remote { .. })));
    }

    #[test]
    fn database_catalog_ownership_rejects_foreign_stale_and_extra_entries() {
        let identity = identity();
        let owned = FunctionCatalogEntry {
            function_id: DATABASE_FUNCTION_ID.to_owned(),
            namespace: identity.namespace.clone(),
            worker_name: identity.worker_name.clone(),
            metadata: Some(json!({"registration_nonce": identity.nonce.to_string()})),
        };
        assert!(database_catalog_owned(
            &identity,
            std::slice::from_ref(&owned)
        ));

        let mut stale = owned.clone();
        stale.metadata = Some(json!({"registration_nonce": Uuid::new_v4().to_string()}));
        assert!(!database_catalog_owned(&identity, &[stale]));

        let foreign = FunctionCatalogEntry {
            worker_name: "foreign".to_owned(),
            ..owned.clone()
        };
        assert!(!database_catalog_owned(&identity, &[foreign]));

        let namespace_foreign = FunctionCatalogEntry {
            namespace: "foreign-namespace".to_owned(),
            ..owned.clone()
        };
        assert!(!database_catalog_owned(&identity, &[namespace_foreign]));

        let wrong_function = FunctionCatalogEntry {
            function_id: CAPTURE_FUNCTION_IDS[0].to_owned(),
            ..owned.clone()
        };
        assert!(!database_catalog_owned(&identity, &[wrong_function]));

        let additional = FunctionCatalogEntry {
            function_id: CAPTURE_FUNCTION_IDS[0].to_owned(),
            ..owned.clone()
        };
        let missing_metadata = FunctionCatalogEntry {
            metadata: None,
            function_id: DATABASE_FUNCTION_ID.to_owned(),
            namespace: identity.namespace.clone(),
            worker_name: identity.worker_name.clone(),
        };
        assert!(
            !database_catalog_owned(&identity, &[owned, additional, missing_metadata]),
            "ownership requires exactly one fully owned database entry"
        );
    }

    #[test]
    fn context_lookup_assertions_are_surface_only_and_do_not_leak_payloads() {
        let lookups = ContextDatabaseLookups {
            records: vec![ContextDatabaseLookup {
                order: 0,
                payload: json!({"db": "secret-target", "sql": SEARCH_CONTEXT_SQL, "params": ["1", "2"]}),
                outcome: ContextLookupOutcome::Success,
            }],
        };

        lookups.assert_count(1).expect("count matches");
        lookups
            .assert_exact_request(0, "secret-target", &["1", "2"])
            .expect("exact request matches");
        lookups
            .assert_outcome(0, ContextLookupOutcome::Success)
            .expect("outcome matches");
        lookups
            .assert_payload_contains(0, &json!({"db": "secret-target"}))
            .expect("payload sanity check works");

        let error = lookups
            .assert_outcome(0, ContextLookupOutcome::Empty)
            .expect_err("outcome mismatch is typed");
        assert_eq!(
            error.to_string(),
            "context lookup 0 served a different outcome than expected"
        );

        let error = lookups
            .assert_exact_request(0, "other-target", &["1", "2"])
            .expect_err("target mismatch is typed");
        assert_eq!(
            error.to_string(),
            "context lookup 0 database target did not match the scripted target"
        );

        let error = lookups
            .assert_count(2)
            .expect_err("count mismatch is typed");
        assert_eq!(
            error.to_string(),
            "context lookup count did not match the expected count"
        );

        let report = format!("{error:?}");
        assert!(
            !report.contains("secret-target") && !report.contains("secret-sql"),
            "assertion errors must not carry database request details"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn real_iii_engine_reaches_six_tool_database_readiness_and_serves_one_exact_context_call()
    {
        let iii = nix_iii_artifact();
        assert_harness_iii_version(&iii);
        let paths = ArtifactPaths {
            iii: iii.clone(),
            opencode_v1: iii.clone(),
            opencode_v2: iii.clone(),
            harness_events: iii.clone(),
            plugin_tarball: iii,
        };
        let session = EngineHarness::new(&paths)
            .start()
            .await
            .expect("real iii capture and context harness starts");
        assert!(
            session.ready(),
            "catalog readiness includes the registered context database path"
        );

        let invoker = trigger_client(session.engine_url(), session.identity().namespace.as_str());
        invoker
            .wait_until_registered(STARTUP_TIMEOUT)
            .await
            .expect("context invoker registers with real iii");

        let response = invoker
            .trigger(TriggerRequest {
                function_id: DATABASE_FUNCTION_ID.to_owned(),
                payload: json!({
                    "db": CONTEXT_DATABASE_TARGET,
                    "sql": SEARCH_CONTEXT_SQL,
                    "params": ["3", "4096"],
                }),
                action: None,
                timeout_ms: Some(INVOCATION_TIMEOUT_MS),
            })
            .await
            .expect("the deterministic context database reply is served end to end");
        assert_eq!(
            response,
            context_success_envelope(),
            "the served context data must carry the exact narrow row envelope"
        );

        let lookups = session.context_lookups();
        lookups
            .assert_count(1)
            .expect("the engine serves exactly one context call here");
        lookups
            .assert_exact_request(0, CONTEXT_DATABASE_TARGET, &["3", "4096"])
            .expect("the served request is byte-exact to the six-tool worker contract");
        lookups
            .assert_outcome(0, ContextLookupOutcome::Success)
            .expect("the context call serves the deterministic success");

        let payloads = [
            json!({"session_id": "engine-context-session", "project_name": "engine-project"}),
            json!({"session_id": "engine-context-session", "hook_type": "PostToolUse", "data": {"value": 7}}),
            json!({"session_id": "engine-context-session", "project_name": "engine-project"}),
        ];
        for (function_id, payload) in CAPTURE_FUNCTION_IDS.iter().zip(payloads.iter()) {
            let response = invoker
                .trigger(TriggerRequest {
                    function_id: (*function_id).to_owned(),
                    payload: payload.clone(),
                    action: None,
                    timeout_ms: Some(INVOCATION_TIMEOUT_MS),
                })
                .await
                .expect("every capture function remains routable after the context call");
            assert_eq!(response, json!({"dispatched": true}));
        }

        let capture = session.capture();
        capture
            .assert_ordered_function_ids(&CAPTURE_FUNCTION_IDS)
            .expect("every existing capture function keeps recording in order");
        capture
            .assert_namespace(session.identity().namespace.as_str())
            .expect("capture namespace is preserved");
        for (function_id, payload) in CAPTURE_FUNCTION_IDS.iter().zip(payloads.iter()) {
            capture
                .assert_payload_contains(function_id, payload)
                .expect("the capture payloads are preserved beside the context call");
        }

        invoker.shutdown();
        session
            .shutdown()
            .await
            .expect("engine harness cleanup completes");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn real_iii_engine_scripts_ordered_context_replies_and_settles_the_delayed_reply() {
        let iii = nix_iii_artifact();
        assert_harness_iii_version(&iii);
        let paths = ArtifactPaths {
            iii: iii.clone(),
            opencode_v1: iii.clone(),
            opencode_v2: iii.clone(),
            harness_events: iii.clone(),
            plugin_tarball: iii,
        };
        let harness = EngineHarness::new(&paths).with_context_script(
            ContextDatabaseScript::ordered(CONTEXT_DATABASE_TARGET, CONTEXT_SCENARIO_REPLIES),
        );
        let session = harness
            .start()
            .await
            .expect("real iii capture and scripted context harness starts");
        let invoker = trigger_client(session.engine_url(), session.identity().namespace.as_str());
        invoker
            .wait_until_registered(STARTUP_TIMEOUT)
            .await
            .expect("context invoker registers with real iii");

        let request = json!({
            "db": CONTEXT_DATABASE_TARGET,
            "sql": SEARCH_CONTEXT_SQL,
            "params": ["3", "4096"],
        });
        let trigger = |timeout_ms: u64| TriggerRequest {
            function_id: DATABASE_FUNCTION_ID.to_owned(),
            payload: request.clone(),
            action: None,
            timeout_ms: Some(timeout_ms),
        };

        let success = invoker
            .trigger(trigger(INVOCATION_TIMEOUT_MS))
            .await
            .expect("the success script reply is served");
        assert_eq!(success, context_success_envelope());

        let empty = invoker
            .trigger(trigger(INVOCATION_TIMEOUT_MS))
            .await
            .expect("the empty script reply is served");
        assert_eq!(empty, context_empty_envelope());

        let backend = invoker
            .trigger(trigger(INVOCATION_TIMEOUT_MS))
            .await
            .expect_err("the backend-failure script reply surfaces as a remote error");
        let iii_sdk::Error::Remote {
            code,
            message,
            stacktrace,
        } = backend
        else {
            panic!("the backend-failure reply must be a remote error");
        };
        assert_eq!(code, CONTEXT_DATABASE_BACKEND_FAILURE);
        assert_eq!(message, CONTEXT_DATABASE_BACKEND_FAILURE);
        assert_eq!(stacktrace, None);
        for row_sentinel in [
            CONTEXT_ROW_TYPE,
            CONTEXT_ROW_TITLE,
            CONTEXT_ROW_CONTENT,
            CONTEXT_ROW_CONCEPT,
        ] {
            assert!(
                !code.contains(row_sentinel) && !message.contains(row_sentinel),
                "harness script errors must not expose context row fields"
            );
        }

        let malformed = invoker
            .trigger(trigger(INVOCATION_TIMEOUT_MS))
            .await
            .expect("the malformed script reply is served on the success channel");
        assert_eq!(malformed, context_malformed_envelope());

        let parked = invoker
            .trigger(trigger(DELAY_INVOCATION_TIMEOUT_MS))
            .await
            .expect_err("the parked delayed reply outlasts the bounded invocation");
        assert!(matches!(parked, iii_sdk::Error::Timeout));
        session.release_parked_delay();
        let settled = wait_for_released_delay(&session).await;
        assert!(
            settled,
            "the released parked reply must settle its invocation deterministically"
        );

        let exhausted = invoker
            .trigger(trigger(INVOCATION_TIMEOUT_MS))
            .await
            .expect_err("an exhausted script settles later lookups with a fixed failure");
        let iii_sdk::Error::Remote {
            code,
            message,
            stacktrace,
        } = exhausted
        else {
            panic!("the exhausted reply must be a remote error");
        };
        assert_eq!(code, CONTEXT_DATABASE_SCRIPT_EXHAUSTED);
        assert_eq!(message, CONTEXT_DATABASE_SCRIPT_EXHAUSTED);
        assert_eq!(stacktrace, None);

        let lookups = session.context_lookups();
        lookups
            .assert_count(6)
            .expect("every scripted lookup is recorded");
        let outcomes = [
            ContextLookupOutcome::Success,
            ContextLookupOutcome::Empty,
            ContextLookupOutcome::BackendFailure,
            ContextLookupOutcome::Malformed,
            ContextLookupOutcome::Delayed,
            ContextLookupOutcome::Exhausted,
        ];
        for (index, expected) in outcomes.iter().enumerate() {
            lookups
                .assert_outcome(index, *expected)
                .expect("ordered script outcomes run in sequence");
        }
        lookups
            .assert_exact_request(2, CONTEXT_DATABASE_TARGET, &["3", "4096"])
            .expect("the backend-failure lookup request is captured exactly");
        assert!(
            session.capture().records.is_empty(),
            "context lookups must not enter the harness capture records"
        );

        invoker.shutdown();
        session
            .shutdown()
            .await
            .expect("engine harness cleanup completes");
    }

    async fn wait_for_released_delay(session: &EngineSession) -> bool {
        let deadline = Instant::now() + RELEASE_SETTLE_TIMEOUT;
        loop {
            if session.parked_delay_settles() >= 1 {
                return true;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            sleep(RELEASE_SETTLE_POLL_INTERVAL).await;
        }
    }

    async fn wait_for_script_park(script: &ContextDatabaseScript) {
        let deadline = Instant::now() + SCRIPT_PARK_WAIT_TIMEOUT;
        loop {
            if script.lookups().count() == 5 {
                return;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                panic!("the parked delayed lookup did not register its release receiver");
            }
            sleep(SCRIPT_PARK_WAIT_POLL_INTERVAL).await;
        }
    }

    fn nix_iii_artifact() -> PathBuf {
        let path =
            std::env::var_os("PATH").expect("Nix-provided iii artifact is unavailable from PATH");
        std::env::split_paths(&path)
            .map(|directory| directory.join("iii"))
            .find(|path| path.is_file())
            .expect("Nix-provided iii artifact is unavailable from PATH")
    }

    fn assert_harness_iii_version(iii: &std::path::Path) {
        let output = std::process::Command::new(iii)
            .arg("--version")
            .output()
            .expect("read Nix-provided iii version");
        assert!(
            output.status.success(),
            "Nix-provided iii must report a version"
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).trim() == "0.24.0",
            "the harness must use iii 0.24.0"
        );
    }

    fn trigger_client(engine_url: &str, namespace: &str) -> IIIClient {
        let metadata = WorkerMetadata {
            name: format!(
                "harness-events-e2e-invoker-{}-{}",
                std::process::id(),
                Uuid::new_v4().simple()
            ),
            namespace: Some(namespace.to_owned()),
            ..WorkerMetadata::default()
        };

        let mut options = InitOptions {
            metadata: Some(metadata),
            headers: None,
            otel: Some(Default::default()),
            namespace: Some(namespace.to_owned()),
            identity: WorkerIdentityMode::Explicit,
        };
        if let Some(otel) = options.otel.as_mut() {
            otel.enabled = Some(false);
        }

        register_worker(engine_url, options)
    }
}
