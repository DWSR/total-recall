use std::{
    env,
    error::Error,
    fmt, fs,
    path::{Component, Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use memory_embedding::runtime::ContentSafeCronCall;
use schemars::{JsonSchema, r#gen::SchemaSettings};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    net::{TcpListener, TcpStream},
    process::{Child, ChildStdin, Command},
    sync::mpsc,
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_tungstenite::{
    WebSocketStream, accept_async,
    tungstenite::{Error as TungsteniteError, Message, error::ProtocolError},
};
use uuid::Uuid;

const EMBED_VERSIONS_FUNCTION_ID: &str = "memory::embed_versions";
const RECONCILE_EMBEDDINGS_FUNCTION_ID: &str = "memory::reconcile_embeddings";
const WORKER_REGISTER_FUNCTION_ID: &str = "engine::workers::register";
const FUNCTIONS_INFO_FUNCTION_ID: &str = "engine::functions::info";
const REGISTERED_TRIGGERS_LIST_FUNCTION_ID: &str = "engine::registered-triggers::list";
const REGISTERED_TRIGGERS_INFO_FUNCTION_ID: &str = "engine::registered-triggers::info";
const DATABASE_EXECUTE_FUNCTION_ID: &str = "database::execute";
const ROUTER_EMBED_FUNCTION_ID: &str = "router::embed";
const DURABLE_SUBSCRIBER_TRIGGER_TYPE: &str = "durable:subscriber";
const CRON_TRIGGER_TYPE: &str = "cron";
const DEFAULT_NAMESPACE: &str = "default";
const WORKER_NAME: &str = "memory-embedding-protocol-fake";
const DATABASE: &str = "memory-embedding-fake-database";
const QUEUE_TOPIC: &str = "memory-embedding-fake-topic";
const CRON_EXPRESSION: &str = "0 */5 * * * *";
const RECONCILIATION_JOB_ID: &str = "memory-embedding-reconciliation-job";
const PROVIDER: &str = "memory-embedding-fake-provider";
const MODEL: &str = "memory-embedding-fake-model";
const MAX_IN_FLIGHT: usize = 1;
const MEMORY_ID: &str = "memory-embedding-private-id";
const MEMORY_TITLE: &str = "memory-embedding-private-title";
const MEMORY_CONTENT: &str = "memory-embedding-private-content";
const MEMORY_CONCEPT: &str = "memory-embedding-private-concept";
const REMOTE_CODE: &str = "memory-embedding-private-remote-code";
const REMOTE_MESSAGE: &str = "memory-embedding-private-remote-message";
const REMOTE_STACKTRACE: &str = "memory-embedding-private-remote-stacktrace";
const SECOND_MEMORY_ID: &str = "memory-embedding-private-id-two";
const SECOND_MEMORY_TITLE: &str = "memory-embedding-private-title-two";
const SECOND_MEMORY_CONTENT: &str = "memory-embedding-private-content-two";
const SECOND_MEMORY_CONCEPT_FIRST: &str = "memory-embedding-private-concept-two-first";
const SECOND_MEMORY_CONCEPT_SECOND: &str = "memory-embedding-private-concept-two-second";
const OUTPUT_CAPTURE_LIMIT: usize = 64 * 1024;
const READINESS_LINE_LIMIT: usize = 256;
const CHILD_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const DATABASE_TIMEOUT: Duration = Duration::from_millis(500);
const ROUTER_TIMEOUT: Duration = Duration::from_secs(1);
const INVOCATION_TIMEOUT: Duration = Duration::from_secs(2);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(40);
const STARTUP_READINESS_GRACE: Duration = Duration::from_secs(5);
const SERIAL_STRESS_ITERATIONS: usize = 3;
const NORMAL_FLOW_RESPONSE_DELAY: Duration = Duration::from_millis(25);
const READINESS_PENDING: u8 = 0;
const READINESS_CATALOG_COMPLETE: u8 = 1;
const READY_STDERR: &[u8] = b"worker ready\n";
const STARTUP_CONFIGURATION_FAILURE_STDERR: &[u8] = b"worker startup configuration failed\n";
const STARTUP_READINESS_FAILURE_STDERR: &[u8] = b"worker startup readiness failed\n";

const LOAD_KEYS_SQL: &str = r#"WITH requested AS (
    SELECT
        item.value->>'id' AS id,
        item.value->>'version' AS version,
        item.ordinality
    FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS item(value, ordinality)
)
SELECT
    requested.id,
    requested.version,
    CASE
        WHEN memory.id IS NOT NULL AND embedding.id IS NULL THEN memory.title
    END AS title,
    CASE
        WHEN memory.id IS NOT NULL AND embedding.id IS NULL THEN memory.content
    END AS content,
    CASE
        WHEN memory.id IS NOT NULL AND embedding.id IS NULL THEN to_json(memory.concepts)
    END AS concepts,
    memory.id IS NOT NULL AS memory_present,
    embedding.id IS NOT NULL AS embedding_present
FROM requested
LEFT JOIN public.memories AS memory
    ON memory.id = requested.id
   AND memory.version = requested.version::bigint
LEFT JOIN public.memory_embeddings AS embedding
    ON embedding.id = memory.id
   AND embedding.version = memory.version
ORDER BY requested.ordinality ASC"#;

const LIST_MISSING_SQL: &str = r#"SELECT
    memory.id,
    memory.version::text AS version,
    memory.title,
    memory.content,
    to_json(memory.concepts) AS concepts
FROM public.memories AS memory
LEFT JOIN public.memory_embeddings AS embedding
    ON embedding.id = memory.id
   AND embedding.version = memory.version
WHERE embedding.id IS NULL
ORDER BY memory.id COLLATE "C" ASC,
         memory.version ASC
LIMIT $1::text::bigint"#;

const INSERT_EMBEDDING_SQL: &str = r#"WITH parent AS MATERIALIZED (
    SELECT id, version
    FROM public.memories
    WHERE id = $1::text AND version = $2::text::bigint
),
inserted AS (
    INSERT INTO public.memory_embeddings (id, version, embedding)
    SELECT id, version, $3::text::vector
    FROM parent
    ON CONFLICT (id, version) DO NOTHING
    RETURNING id, version
)
SELECT outcome, id, version::text AS version
FROM (
    SELECT 'inserted'::text AS outcome, id, version FROM inserted
    UNION ALL
    SELECT 'conflict'::text AS outcome, id, version FROM parent
    WHERE NOT EXISTS (SELECT 1 FROM inserted)
    UNION ALL
    SELECT 'missing'::text AS outcome, $1::text AS id, $2::text::bigint AS version
    WHERE NOT EXISTS (SELECT 1 FROM parent)
) AS outcome_row"#;

type SmokeResult<T = ()> = Result<T, FakeError>;

#[derive(Clone, Copy, Debug)]
struct FakeError(&'static str);

impl fmt::Display for FakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for FakeError {}

#[derive(Debug, PartialEq, Eq)]
struct Arguments {
    manifest: PathBuf,
    timeout: Duration,
    scenario: ScenarioSelection,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScenarioSelection {
    All,
    Normal,
    QueueRejections,
    QueueRetry,
    RouterFailures,
    Reconciliation,
    Lifecycle,
    LifecycleStress,
}

#[derive(Debug, PartialEq, Eq)]
struct WorkerLaunch {
    working_directory: PathBuf,
    executable: PathBuf,
}

#[derive(Clone, Copy)]
struct WorkerSettings {
    max_event_keys: u32,
    batch_limit: u32,
    reconciliation_limit: u32,
    max_input_bytes: usize,
    startup: StartupConfiguration,
}

impl WorkerSettings {
    const SINGLE: Self = Self {
        max_event_keys: 1,
        batch_limit: 1,
        reconciliation_limit: 1,
        max_input_bytes: 4096,
        startup: StartupConfiguration::Valid,
    };

    const MULTI: Self = Self {
        max_event_keys: 2,
        batch_limit: 2,
        reconciliation_limit: 2,
        max_input_bytes: 4096,
        startup: StartupConfiguration::Valid,
    };

    const INPUT_TOO_LARGE: Self = Self {
        max_event_keys: 1,
        batch_limit: 1,
        reconciliation_limit: 1,
        max_input_bytes: 1,
        startup: StartupConfiguration::Valid,
    };

    const fn with_startup(mut self, startup: StartupConfiguration) -> Self {
        self.startup = startup;
        self
    }
}

#[derive(Clone, Copy)]
enum StartupConfiguration {
    Valid,
    MissingProvider,
    ForeignNamespace,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CatalogProfile {
    Ready,
    Pending,
    StaleNonce,
    ForeignOwner,
    DuplicateCandidates,
    Mismatched(CatalogMismatch),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CatalogMismatch {
    Config,
    TriggerType,
    Provider,
    Target,
    Namespace,
}

#[derive(Clone, Copy)]
struct CatalogScript {
    profile: CatalogProfile,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CatalogProgress {
    Continue,
    Ready,
    Failed,
}

struct CatalogSession {
    script: CatalogScript,
    worker_registered: bool,
    registrations: RegistrationState,
    readiness_step: u8,
    pending_polls: usize,
}

#[derive(Deserialize)]
struct WorkerManifest {
    scripts: Option<WorkerScripts>,
}

#[derive(Deserialize)]
struct WorkerScripts {
    start: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DirectFunction {
    EmbedVersions,
    ReconcileEmbeddings,
}

impl DirectFunction {
    const fn function_id(self) -> &'static str {
        match self {
            Self::EmbedVersions => EMBED_VERSIONS_FUNCTION_ID,
            Self::ReconcileEmbeddings => RECONCILE_EMBEDDINGS_FUNCTION_ID,
        }
    }
}

#[derive(Clone)]
struct DirectInvocation {
    function: DirectFunction,
    invocation_id: Uuid,
    data: Value,
    expected: DirectExpectation,
    response_timing: Option<ResponseTiming>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResponseTiming {
    minimum: Duration,
    maximum: Duration,
}

impl ResponseTiming {
    fn validate(self, elapsed: Duration) -> SmokeResult {
        if elapsed < self.minimum {
            return failure("release worker returned before the scripted response delay");
        }
        if elapsed > self.maximum {
            return failure("release worker response exceeded the scenario deadline");
        }
        Ok(())
    }
}

#[derive(Clone)]
#[allow(dead_code)]
enum DirectExpectation {
    Success(Value),
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UpstreamFunction {
    Database,
    Router,
}

impl UpstreamFunction {
    const fn function_id(self) -> &'static str {
        match self {
            Self::Database => DATABASE_EXECUTE_FUNCTION_ID,
            Self::Router => ROUTER_EMBED_FUNCTION_ID,
        }
    }
}

#[derive(Clone)]
struct UpstreamStep {
    function: UpstreamFunction,
    expected_payload: Value,
    response: ScriptedResponse,
}

#[derive(Clone)]
enum UpstreamExpectation {
    Ordered(UpstreamStep),
    Unordered(Vec<UpstreamStep>),
}

#[derive(Clone)]
#[allow(dead_code)]
enum ScriptedResponse {
    Success(Value),
    RemoteError,
    Malformed(Value),
    Delayed {
        delay: Duration,
        response: Box<ScriptedResponse>,
    },
    AwaitLocalError {
        late_response: Box<ScriptedResponse>,
    },
    LoadCommittedKeys {
        memories: Vec<&'static QueueMemory>,
    },
    ListUncommittedMemories {
        memories: Vec<&'static QueueMemory>,
    },
}

#[derive(Clone)]
struct ProtocolScript {
    worker_settings: WorkerSettings,
    invocation: DirectInvocation,
    expectations: Vec<UpstreamExpectation>,
    follow_up_deliveries: Vec<FollowUpDelivery>,
    post_late: Option<DirectInvocation>,
}

#[derive(Clone)]
struct FollowUpDelivery {
    invocation: DirectInvocation,
    expectations: Vec<UpstreamExpectation>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct InvocationCounts {
    direct_deliveries: usize,
    database_calls: usize,
    router_calls: usize,
}

impl InvocationCounts {
    fn record_upstream(&mut self, function: UpstreamFunction) {
        match function {
            UpstreamFunction::Database => self.database_calls += 1,
            UpstreamFunction::Router => self.router_calls += 1,
        }
    }
}

struct ProtocolRun {
    registrations: RegistrationState,
    invocation_counts: InvocationCounts,
}

struct PendingLocalError {
    invocation_id: String,
    function: UpstreamFunction,
    late_response: ScriptedResponse,
}

impl ProtocolScript {
    fn direct(
        function: DirectFunction,
        invocation_id: Uuid,
        data: Value,
        expected: DirectExpectation,
        worker_settings: WorkerSettings,
    ) -> Self {
        Self {
            worker_settings,
            invocation: DirectInvocation {
                function,
                invocation_id,
                data,
                expected,
                response_timing: None,
            },
            expectations: Vec::new(),
            follow_up_deliveries: Vec::new(),
            post_late: None,
        }
    }

    fn queue(
        invocation_id: Uuid,
        event: Value,
        expected: DirectExpectation,
        worker_settings: WorkerSettings,
    ) -> Self {
        Self::direct(
            DirectFunction::EmbedVersions,
            invocation_id,
            event,
            expected,
            worker_settings,
        )
    }

    fn reconciliation(
        invocation_id: Uuid,
        call: Value,
        expected: DirectExpectation,
        worker_settings: WorkerSettings,
    ) -> Self {
        let mut script = Self::direct(
            DirectFunction::ReconcileEmbeddings,
            invocation_id,
            call,
            expected,
            worker_settings,
        );
        script.invocation.response_timing = Some(ResponseTiming {
            minimum: Duration::ZERO,
            maximum: INVOCATION_TIMEOUT,
        });
        script
    }

    fn with_response_timing(mut self, response_timing: ResponseTiming) -> Self {
        self.invocation.response_timing = Some(response_timing);
        self
    }

    fn expect_ordered(mut self, step: UpstreamStep) -> Self {
        self.expectations.push(UpstreamExpectation::Ordered(step));
        self
    }

    fn expect_unordered(mut self, steps: Vec<UpstreamStep>) -> Self {
        assert!(
            !steps.is_empty(),
            "unordered script expectations must contain at least one step"
        );
        self.expectations
            .push(UpstreamExpectation::Unordered(steps));
        self
    }

    fn then_direct(
        mut self,
        function: DirectFunction,
        invocation_id: Uuid,
        data: Value,
        expected: DirectExpectation,
        expectations: Vec<UpstreamExpectation>,
    ) -> Self {
        self.follow_up_deliveries.push(FollowUpDelivery {
            invocation: DirectInvocation {
                function,
                invocation_id,
                data,
                expected,
                response_timing: None,
            },
            expectations,
        });
        self
    }

    fn then_queue(
        self,
        invocation_id: Uuid,
        event: Value,
        expected: DirectExpectation,
        expectations: Vec<UpstreamExpectation>,
    ) -> Self {
        self.then_direct(
            DirectFunction::EmbedVersions,
            invocation_id,
            event,
            expected,
            expectations,
        )
    }

    fn then_reconciliation(
        mut self,
        invocation_id: Uuid,
        expected: DirectExpectation,
        expectations: Vec<UpstreamExpectation>,
    ) -> Self {
        self.follow_up_deliveries.push(FollowUpDelivery {
            invocation: DirectInvocation {
                function: DirectFunction::ReconcileEmbeddings,
                invocation_id,
                data: cron_call(),
                expected,
                response_timing: Some(ResponseTiming {
                    minimum: Duration::ZERO,
                    maximum: INVOCATION_TIMEOUT,
                }),
            },
            expectations,
        });
        self
    }

    fn with_post_late_barrier(
        mut self,
        function: DirectFunction,
        invocation_id: Uuid,
        data: Value,
    ) -> Self {
        assert!(
            self.post_late.is_none(),
            "protocol scripts may define only one post-late barrier"
        );
        self.post_late = Some(DirectInvocation {
            function,
            invocation_id,
            data,
            expected: DirectExpectation::Error,
            response_timing: match function {
                DirectFunction::EmbedVersions => None,
                DirectFunction::ReconcileEmbeddings => Some(ResponseTiming {
                    minimum: Duration::ZERO,
                    maximum: INVOCATION_TIMEOUT,
                }),
            },
        });
        self
    }

    fn delivery_count(&self) -> usize {
        1 + self.follow_up_deliveries.len()
    }

    fn delivery(&self, index: usize) -> Option<(&DirectInvocation, &[UpstreamExpectation])> {
        if index == 0 {
            Some((&self.invocation, &self.expectations))
        } else {
            self.follow_up_deliveries
                .get(index - 1)
                .map(|delivery| (&delivery.invocation, delivery.expectations.as_slice()))
        }
    }

    fn expected_invocation_counts(&self) -> InvocationCounts {
        let mut counts = InvocationCounts {
            direct_deliveries: self.delivery_count() + usize::from(self.post_late.is_some()),
            ..Default::default()
        };
        for index in 0..self.delivery_count() {
            let (_, expectations) = self
                .delivery(index)
                .expect("protocol script delivery must be present");
            for expectation in expectations {
                match expectation {
                    UpstreamExpectation::Ordered(step) => counts.record_upstream(step.function),
                    UpstreamExpectation::Unordered(steps) => {
                        for step in steps {
                            counts.record_upstream(step.function);
                        }
                    }
                }
            }
        }
        counts
    }

    fn verify_invocation_counts(&self, actual: InvocationCounts) -> SmokeResult {
        if actual != self.expected_invocation_counts() {
            return failure("release worker invocation counts did not match the script");
        }
        Ok(())
    }
}

#[derive(Clone)]
struct FunctionRegistration {
    nonce: String,
}

#[derive(Clone)]
struct TriggerRegistration {
    id: String,
    nonce: String,
}

#[derive(Clone, Default)]
struct RegistrationState {
    queue_function: Option<FunctionRegistration>,
    reconciliation_function: Option<FunctionRegistration>,
    subscriber: Option<TriggerRegistration>,
    cron: Option<TriggerRegistration>,
}

impl RegistrationState {
    fn record_function(&mut self, message: &Value) -> SmokeResult {
        let function_id = required_string(message, "id", "function registration")?;
        let nonce = validate_function_registration(message, function_id)?;
        let registration = FunctionRegistration { nonce };

        match function_id {
            EMBED_VERSIONS_FUNCTION_ID => {
                if self.queue_function.replace(registration).is_some() {
                    return failure("worker registered the queue function more than once");
                }
            }
            RECONCILE_EMBEDDINGS_FUNCTION_ID => {
                if self.reconciliation_function.replace(registration).is_some() {
                    return failure("worker registered the reconciliation function more than once");
                }
            }
            _ => return failure("worker registered an unexpected function"),
        }

        Ok(())
    }

    fn record_trigger(&mut self, message: &Value) -> SmokeResult {
        validate_trigger_registration(message)?;
        let trigger_type = required_string(message, "trigger_type", "trigger registration")?;
        let registration = TriggerRegistration {
            id: required_uuid(message, "id", "trigger registration")?,
            nonce: registration_nonce(message)?,
        };

        match trigger_type {
            DURABLE_SUBSCRIBER_TRIGGER_TYPE => {
                if self.subscriber.replace(registration).is_some() {
                    return failure("worker registered the durable subscriber more than once");
                }
            }
            CRON_TRIGGER_TYPE => {
                if self.cron.replace(registration).is_some() {
                    return failure("worker registered the cron trigger more than once");
                }
            }
            _ => return failure("worker registered an unexpected trigger"),
        }

        Ok(())
    }

    fn verify_complete(&self) -> SmokeResult {
        let queue = self
            .queue_function
            .as_ref()
            .ok_or(FakeError("worker did not register the queue function"))?;
        let reconciliation = self.reconciliation_function.as_ref().ok_or(FakeError(
            "worker did not register the reconciliation function",
        ))?;
        let subscriber = self
            .subscriber
            .as_ref()
            .ok_or(FakeError("worker did not register the durable subscriber"))?;
        let cron = self
            .cron
            .as_ref()
            .ok_or(FakeError("worker did not register the cron trigger"))?;
        if queue.nonce != reconciliation.nonce
            || queue.nonce != subscriber.nonce
            || queue.nonce != cron.nonce
        {
            return failure("worker registrations did not use one shared nonce");
        }

        Ok(())
    }

    fn nonce(&self) -> SmokeResult<String> {
        self.verify_complete()?;
        Ok(self
            .queue_function
            .as_ref()
            .expect("verified queue registration is present")
            .nonce
            .clone())
    }

    fn trigger_id(&self, function: DirectFunction) -> SmokeResult<String> {
        self.verify_complete()?;
        let trigger = match function {
            DirectFunction::EmbedVersions => &self.subscriber,
            DirectFunction::ReconcileEmbeddings => &self.cron,
        };
        Ok(trigger
            .as_ref()
            .expect("verified trigger registration is present")
            .id
            .clone())
    }
}

impl CatalogScript {
    const READY: Self = Self {
        profile: CatalogProfile::Ready,
    };

    const fn new(profile: CatalogProfile) -> Self {
        Self { profile }
    }

    const fn is_pending(self) -> bool {
        matches!(self.profile, CatalogProfile::Pending)
    }

    fn function_response(
        self,
        registrations: &RegistrationState,
    ) -> SmokeResult<(Value, CatalogProgress)> {
        let mut response = function_catalog_response(registrations)?;
        let progress = match self.profile {
            CatalogProfile::Ready
            | CatalogProfile::DuplicateCandidates
            | CatalogProfile::Mismatched(CatalogMismatch::Config)
            | CatalogProfile::Mismatched(CatalogMismatch::TriggerType)
            | CatalogProfile::Mismatched(CatalogMismatch::Provider)
            | CatalogProfile::Mismatched(CatalogMismatch::Target) => CatalogProgress::Continue,
            CatalogProfile::Pending => {
                response = json!({
                    "functions": [
                        {
                            "function_id": EMBED_VERSIONS_FUNCTION_ID,
                            "error": "pending",
                        },
                        {
                            "function_id": RECONCILE_EMBEDDINGS_FUNCTION_ID,
                            "error": "pending",
                        },
                    ],
                });
                CatalogProgress::Continue
            }
            CatalogProfile::StaleNonce => {
                response["functions"][0]["metadata"] =
                    json!({"registration_nonce": "00000000-0000-4000-8000-000000000001"});
                CatalogProgress::Failed
            }
            CatalogProfile::ForeignOwner => {
                response["functions"][0]["worker_name"] = json!("foreign-worker");
                CatalogProgress::Failed
            }
            CatalogProfile::Mismatched(CatalogMismatch::Namespace) => {
                response["functions"][0]["namespace"] = json!("foreign-namespace");
                CatalogProgress::Failed
            }
        };
        Ok((response, progress))
    }

    fn trigger_list_response(
        self,
        registrations: &RegistrationState,
        function: DirectFunction,
        trigger_type: &str,
    ) -> SmokeResult<(Value, CatalogProgress)> {
        let mut response = registered_trigger_list_response(registrations, function, trigger_type)?;
        let progress = match self.profile {
            CatalogProfile::DuplicateCandidates if function == DirectFunction::EmbedVersions => {
                let candidates = response["registered_triggers"]
                    .as_array_mut()
                    .ok_or(FakeError("fake trigger list response was malformed"))?;
                let candidate = candidates
                    .first()
                    .cloned()
                    .ok_or(FakeError("fake trigger list response was empty"))?;
                candidates.push(candidate);
                CatalogProgress::Failed
            }
            CatalogProfile::Mismatched(CatalogMismatch::TriggerType)
                if function == DirectFunction::EmbedVersions =>
            {
                response["registered_triggers"][0]["trigger_type"] = json!("foreign:trigger");
                CatalogProgress::Failed
            }
            _ => CatalogProgress::Continue,
        };
        Ok((response, progress))
    }

    fn trigger_detail_response(
        self,
        registrations: &RegistrationState,
        function: DirectFunction,
    ) -> SmokeResult<(Value, CatalogProgress)> {
        let mut response = registered_trigger_detail_response(registrations, function)?;
        let progress = match self.profile {
            CatalogProfile::Mismatched(CatalogMismatch::Config)
                if function == DirectFunction::EmbedVersions =>
            {
                response["config"] = json!({"queue": "foreign-topic"});
                CatalogProgress::Failed
            }
            CatalogProfile::Mismatched(CatalogMismatch::Provider)
                if function == DirectFunction::EmbedVersions =>
            {
                response["trigger"]["id"] = json!("foreign:provider");
                CatalogProgress::Failed
            }
            CatalogProfile::Mismatched(CatalogMismatch::Target)
                if function == DirectFunction::EmbedVersions =>
            {
                response["function"]["function_id"] = json!("foreign::function");
                CatalogProgress::Failed
            }
            _ => CatalogProgress::Continue,
        };
        Ok((response, progress))
    }
}

impl CatalogSession {
    fn new(script: CatalogScript) -> Self {
        Self {
            script,
            worker_registered: false,
            registrations: RegistrationState::default(),
            readiness_step: 0,
            pending_polls: 0,
        }
    }

    async fn handle_message(
        &mut self,
        socket: &mut WebSocketStream<TcpStream>,
        message: &Value,
        readiness_phase: &Arc<AtomicU8>,
    ) -> SmokeResult<CatalogProgress> {
        match message_type(message)? {
            "registerfunction" => {
                self.registrations.record_function(message)?;
                Ok(CatalogProgress::Continue)
            }
            "registertrigger" => {
                self.registrations.record_trigger(message)?;
                Ok(CatalogProgress::Continue)
            }
            "invokefunction" => {
                let function_id = required_string(message, "function_id", "invocation")?;
                if function_id == WORKER_REGISTER_FUNCTION_ID {
                    if self.worker_registered {
                        return failure("release worker registered with the engine more than once");
                    }
                    validate_worker_registration(message)?;
                    self.worker_registered = true;
                    send_worker_registered(socket).await?;
                    return Ok(CatalogProgress::Continue);
                }
                if !matches!(
                    function_id,
                    FUNCTIONS_INFO_FUNCTION_ID
                        | REGISTERED_TRIGGERS_LIST_FUNCTION_ID
                        | REGISTERED_TRIGGERS_INFO_FUNCTION_ID
                ) {
                    return failure(
                        "release worker made an unexpected invocation before readiness",
                    );
                }
                if !self.worker_registered {
                    return failure("catalog request arrived before worker registration");
                }
                if self.script.is_pending() && function_id == FUNCTIONS_INFO_FUNCTION_ID {
                    self.pending_polls += 1;
                }
                handle_catalog_request(
                    socket,
                    message,
                    &self.registrations,
                    &mut self.readiness_step,
                    readiness_phase,
                    self.script,
                )
                .await
            }
            _ => failure("release worker sent an unsupported protocol frame before readiness"),
        }
    }
}

#[derive(Default)]
struct StreamCapture {
    bytes: Vec<u8>,
    overflowed: bool,
}

impl StreamCapture {
    fn append(&mut self, bytes: &[u8], stream: &'static str) -> SmokeResult {
        if self.overflowed {
            return failure(output_capture_limit_error(stream));
        }
        let Some(remaining) = OUTPUT_CAPTURE_LIMIT.checked_sub(self.bytes.len()) else {
            self.overflowed = true;
            return failure(output_capture_limit_error(stream));
        };
        if bytes.len() > remaining {
            self.overflowed = true;
            return failure(output_capture_limit_error(stream));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn validate(&self, stream: &'static str) -> SmokeResult {
        if self.overflowed {
            return failure(output_capture_limit_error(stream));
        }
        if protected_sentinel_present(&self.bytes) {
            return failure(match stream {
                "stdout" => "worker stdout exposed protected data",
                "stderr" => "worker stderr exposed protected data",
                _ => "worker output exposed protected data",
            });
        }
        Ok(())
    }
}

fn output_capture_limit_error(stream: &'static str) -> &'static str {
    match stream {
        "stdout" => "worker stdout exceeded the bounded capture",
        "stderr" => "worker stderr exceeded the bounded capture",
        _ => "worker output exceeded the bounded capture",
    }
}

#[derive(Default)]
struct ReadinessDetector {
    line: Vec<u8>,
    discarded_line: bool,
}

impl ReadinessDetector {
    fn observe(&mut self, bytes: &[u8]) -> bool {
        let mut ready = false;
        for byte in bytes {
            if *byte == b'\n' {
                ready |= self.finish_line();
            } else if !self.discarded_line {
                if self.line.len() == READINESS_LINE_LIMIT {
                    self.line.clear();
                    self.discarded_line = true;
                } else {
                    self.line.push(*byte);
                }
            }
        }
        ready
    }

    fn finish(&mut self) -> bool {
        self.finish_line()
    }

    fn finish_line(&mut self) -> bool {
        let line = self.line.strip_suffix(b"\r").unwrap_or(&self.line);
        let ready = !self.discarded_line && line == b"worker ready";
        self.line.clear();
        self.discarded_line = false;
        ready
    }
}

struct ReadinessReport {
    phase: u8,
}

#[derive(Clone, Copy)]
enum ExitExpectation {
    Success,
    Failure,
    ForcedTermination,
}

struct WorkerOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

struct WorkerProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout_task: JoinHandle<SmokeResult>,
    stderr_task: JoinHandle<SmokeResult>,
    stdout_capture: Arc<Mutex<StreamCapture>>,
    stderr_capture: Arc<Mutex<StreamCapture>>,
}

impl WorkerProcess {
    async fn start(
        launch: &WorkerLaunch,
        engine_url: String,
        readiness_phase: Arc<AtomicU8>,
        worker_settings: WorkerSettings,
    ) -> SmokeResult<(Self, mpsc::UnboundedReceiver<ReadinessReport>)> {
        let mut child = worker_command(launch, engine_url, worker_settings)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| FakeError("failed to start release worker"))?;

        let stdin = child.stdin.take();
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let missing_stream = if stdin.is_none() {
            Some("release worker stdin was not piped")
        } else if stdout.is_none() {
            Some("release worker stdout was not piped")
        } else if stderr.is_none() {
            Some("release worker stderr was not piped")
        } else {
            None
        };
        if let Some(message) = missing_stream {
            drop(stdin);
            drop(stdout);
            drop(stderr);
            reap_after_start_failure(&mut child).await?;
            return failure(message);
        }

        let stdin = stdin.expect("verified release worker stdin is present");
        let stdout = stdout.expect("verified release worker stdout is present");
        let stderr = stderr.expect("verified release worker stderr is present");

        let stdout_capture = Arc::new(Mutex::new(StreamCapture::default()));
        let stderr_capture = Arc::new(Mutex::new(StreamCapture::default()));
        let (readiness_sender, readiness_receiver) = mpsc::unbounded_channel();
        let stdout_task = tokio::spawn(capture_stream(stdout, Arc::clone(&stdout_capture)));
        let stderr_task = tokio::spawn(capture_stderr(
            stderr,
            Arc::clone(&stderr_capture),
            readiness_phase,
            readiness_sender,
        ));

        Ok((
            Self {
                child,
                stdin: Some(stdin),
                stdout_task,
                stderr_task,
                stdout_capture,
                stderr_capture,
            },
            readiness_receiver,
        ))
    }

    fn close_stdin(&mut self) {
        self.stdin.take();
    }

    fn start_forced_termination(&mut self) -> SmokeResult {
        self.child
            .start_kill()
            .map_err(|_| FakeError("failed to force terminate release worker"))
    }

    async fn wait_for_exit(&mut self) -> SmokeResult<ExitStatus> {
        if let Some(status) = self
            .child
            .try_wait()
            .map_err(|_| FakeError("failed to inspect release worker"))?
        {
            return Ok(status);
        }

        match timeout(CHILD_CLEANUP_TIMEOUT, self.child.wait()).await {
            Ok(Ok(status)) => Ok(status),
            Ok(Err(_)) => failure("failed to reap release worker"),
            Err(_) => {
                force_reap_worker(&mut self.child).await?;
                failure("release worker did not exit before cleanup")
            }
        }
    }

    async fn finish(mut self, expected_exit: ExitExpectation) -> SmokeResult<WorkerOutput> {
        let child_result = self.wait_for_exit().await;
        let stdout_result = match self.stdout_task.await {
            Ok(result) => result,
            Err(_) => failure("release worker stdout capture task failed"),
        };
        let stderr_result = match self.stderr_task.await {
            Ok(result) => result,
            Err(_) => failure("release worker stderr capture task failed"),
        };
        let stdout_capture = match self.stdout_capture.lock() {
            Ok(capture) => {
                capture.validate("stdout")?;
                Ok(capture.bytes.clone())
            }
            Err(_) => failure("release worker stdout capture lock failed"),
        };
        let stderr_capture = match self.stderr_capture.lock() {
            Ok(capture) => {
                capture.validate("stderr")?;
                Ok(capture.bytes.clone())
            }
            Err(_) => failure("release worker stderr capture lock failed"),
        };

        let status = child_result?;
        stdout_result?;
        stderr_result?;
        let stdout = stdout_capture?;
        let stderr = stderr_capture?;
        match expected_exit {
            ExitExpectation::Success => require_worker_success(status)?,
            ExitExpectation::Failure => require_worker_failure(status)?,
            ExitExpectation::ForcedTermination => require_forced_worker_termination(status)?,
        }

        Ok(WorkerOutput {
            status,
            stdout,
            stderr,
        })
    }
}

fn worker_command(
    launch: &WorkerLaunch,
    engine_url: String,
    worker_settings: WorkerSettings,
) -> Command {
    let mut command = Command::new(&launch.executable);
    command
        .current_dir(&launch.working_directory)
        .env_clear()
        .env("TOTAL_RECALL_EMBEDDING_DATABASE", DATABASE)
        .env("TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC", QUEUE_TOPIC)
        .env("TOTAL_RECALL_EMBEDDING_CRON_EXPRESSION", CRON_EXPRESSION)
        .env("TOTAL_RECALL_EMBEDDING_MODEL", MODEL)
        .env(
            "TOTAL_RECALL_EMBEDDING_MAX_EVENT_KEYS",
            worker_settings.max_event_keys.to_string(),
        )
        .env(
            "TOTAL_RECALL_EMBEDDING_BATCH_LIMIT",
            worker_settings.batch_limit.to_string(),
        )
        .env(
            "TOTAL_RECALL_EMBEDDING_RECONCILIATION_LIMIT",
            worker_settings.reconciliation_limit.to_string(),
        )
        .env(
            "TOTAL_RECALL_EMBEDDING_MAX_INPUT_BYTES",
            worker_settings.max_input_bytes.to_string(),
        )
        .env("TOTAL_RECALL_EMBEDDING_MAX_IN_FLIGHT", "1")
        .env(
            "TOTAL_RECALL_EMBEDDING_DATABASE_TIMEOUT_MS",
            DATABASE_TIMEOUT.as_millis().to_string(),
        )
        .env(
            "TOTAL_RECALL_EMBEDDING_ROUTER_TIMEOUT_MS",
            ROUTER_TIMEOUT.as_millis().to_string(),
        )
        .env(
            "TOTAL_RECALL_EMBEDDING_INVOCATION_TIMEOUT_MS",
            INVOCATION_TIMEOUT.as_millis().to_string(),
        )
        .env(
            "TOTAL_RECALL_EMBEDDING_SHUTDOWN_TIMEOUT_MS",
            SHUTDOWN_TIMEOUT.as_millis().to_string(),
        )
        .env("III_URL", engine_url)
        .env("III_WORKER_NAME", WORKER_NAME);
    match worker_settings.startup {
        StartupConfiguration::Valid => {
            command
                .env("TOTAL_RECALL_EMBEDDING_PROVIDER", PROVIDER)
                .env("III_NAMESPACE", DEFAULT_NAMESPACE);
        }
        StartupConfiguration::MissingProvider => {
            command.env("III_NAMESPACE", DEFAULT_NAMESPACE);
        }
        StartupConfiguration::ForeignNamespace => {
            command
                .env("TOTAL_RECALL_EMBEDDING_PROVIDER", PROVIDER)
                .env("III_NAMESPACE", "foreign");
        }
    }
    command
}

#[tokio::main]
async fn main() -> SmokeResult {
    run(parse_arguments(env::args())?).await
}

async fn run(arguments: Arguments) -> SmokeResult {
    let launch = load_worker_launch(&arguments.manifest)?;
    for script in queue_scripts(arguments.scenario) {
        run_script(arguments.timeout, &launch, script).await?;
    }
    match arguments.scenario {
        ScenarioSelection::All => {
            run_lifecycle_scenarios(arguments.timeout, &launch).await?;
            run_lifecycle_serial_stress(arguments.timeout, &launch).await
        }
        ScenarioSelection::Lifecycle => run_lifecycle_scenarios(arguments.timeout, &launch).await,
        ScenarioSelection::LifecycleStress => {
            run_lifecycle_serial_stress(arguments.timeout, &launch).await
        }
        ScenarioSelection::Normal
        | ScenarioSelection::QueueRejections
        | ScenarioSelection::QueueRetry
        | ScenarioSelection::RouterFailures
        | ScenarioSelection::Reconciliation => Ok(()),
    }
}

async fn run_script(
    operation_timeout: Duration,
    launch: &WorkerLaunch,
    script: ProtocolScript,
) -> SmokeResult {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .map_err(|_| FakeError("failed to bind loopback fake engine"))?;
    let address = listener
        .local_addr()
        .map_err(|_| FakeError("failed to inspect loopback fake engine"))?;
    let readiness_phase = Arc::new(AtomicU8::new(READINESS_PENDING));
    let (mut worker, readiness_receiver) = WorkerProcess::start(
        launch,
        format!("ws://127.0.0.1:{}", address.port()),
        Arc::clone(&readiness_phase),
        script.worker_settings,
    )
    .await?;
    let deadline = Instant::now() + operation_timeout;
    let protocol_result = async {
        let (stream, _) = timeout_remaining(deadline, listener.accept())
            .await?
            .map_err(|_| FakeError("release worker did not connect to the fake engine"))?;
        let mut socket = timeout_remaining(deadline, accept_async(stream))
            .await?
            .map_err(|_| FakeError("release worker websocket handshake failed"))?;
        let protocol_run = run_protocol(
            &mut socket,
            readiness_receiver,
            readiness_phase,
            &script,
            deadline,
        )
        .await?;
        script.verify_invocation_counts(protocol_run.invocation_counts)?;
        drain_shutdown(&mut socket, &protocol_run.registrations, deadline).await?;
        worker.close_stdin();
        Ok(())
    }
    .await;

    worker.close_stdin();
    let cleanup_result = worker.finish(ExitExpectation::Success).await;
    match (protocol_result, cleanup_result) {
        (Ok(()), Ok(output)) => assert_worker_output(&output, READY_STDERR),
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
    }
}

#[derive(Clone, Copy)]
enum ShutdownStimulus {
    Eof,
    #[cfg(unix)]
    Signal,
}

async fn run_lifecycle_scenarios(
    operation_timeout: Duration,
    launch: &WorkerLaunch,
) -> SmokeResult {
    validate_lifecycle_timeout(operation_timeout)?;
    for profile in lifecycle_catalog_profiles() {
        run_catalog_failure(operation_timeout, launch, profile)
            .await
            .map_err(|_| FakeError("catalog lifecycle scenario failed"))?;
    }
    for startup in [
        StartupConfiguration::MissingProvider,
        StartupConfiguration::ForeignNamespace,
    ] {
        run_invalid_startup_failure(operation_timeout, launch, startup)
            .await
            .map_err(|_| FakeError("invalid startup lifecycle scenario failed"))?;
    }
    run_empty_lifecycle_shutdown(operation_timeout, launch, false)
        .await
        .map_err(|_| FakeError("normal cleanup lifecycle scenario failed"))?;
    run_shutdown_during_admitted_work(operation_timeout, launch, ShutdownStimulus::Eof)
        .await
        .map_err(|_| FakeError("EOF shutdown lifecycle scenario failed"))?;
    #[cfg(unix)]
    run_shutdown_during_admitted_work(operation_timeout, launch, ShutdownStimulus::Signal)
        .await
        .map_err(|_| FakeError("signal shutdown lifecycle scenario failed"))?;
    run_lifecycle_deadline(operation_timeout, launch)
        .await
        .map_err(|_| FakeError("deadline lifecycle scenario failed"))?;
    run_empty_lifecycle_shutdown(operation_timeout, launch, true)
        .await
        .map_err(|_| FakeError("forced termination lifecycle scenario failed"))
}

async fn run_lifecycle_serial_stress(
    operation_timeout: Duration,
    launch: &WorkerLaunch,
) -> SmokeResult {
    for _ in 0..SERIAL_STRESS_ITERATIONS {
        run_shutdown_during_admitted_work(operation_timeout, launch, ShutdownStimulus::Eof).await?;
    }
    Ok(())
}

fn validate_lifecycle_timeout(operation_timeout: Duration) -> SmokeResult {
    let minimum = memory_embedding::runtime::WORKER_READINESS_TIMEOUT + STARTUP_READINESS_GRACE;
    if operation_timeout < minimum {
        return failure("lifecycle scenarios require a timeout beyond the readiness deadline");
    }
    Ok(())
}

async fn run_catalog_failure(
    operation_timeout: Duration,
    launch: &WorkerLaunch,
    profile: CatalogProfile,
) -> SmokeResult {
    validate_lifecycle_timeout(operation_timeout)?;
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .map_err(|_| FakeError("failed to bind loopback fake engine"))?;
    let address = listener
        .local_addr()
        .map_err(|_| FakeError("failed to inspect loopback fake engine"))?;
    let readiness_phase = Arc::new(AtomicU8::new(READINESS_PENDING));
    let child_started_at = Instant::now();
    let (mut worker, mut readiness_receiver) = WorkerProcess::start(
        launch,
        format!("ws://127.0.0.1:{}", address.port()),
        Arc::clone(&readiness_phase),
        WorkerSettings::SINGLE,
    )
    .await?;
    let deadline = Instant::now() + operation_timeout;
    let protocol_result = async {
        let (stream, _) = timeout_remaining(deadline, listener.accept())
            .await?
            .map_err(|_| FakeError("release worker did not connect to the fake engine"))?;
        let mut socket = timeout_remaining(deadline, accept_async(stream))
            .await?
            .map_err(|_| FakeError("release worker websocket handshake failed"))?;
        drive_catalog_failure(
            &mut socket,
            &mut readiness_receiver,
            readiness_phase,
            CatalogScript::new(profile),
            child_started_at,
            deadline,
        )
        .await
    }
    .await;

    if protocol_result.is_err() {
        worker.close_stdin();
    }
    let cleanup_result = worker.finish(ExitExpectation::Failure).await;
    match (protocol_result, cleanup_result) {
        (Ok(()), Ok(output)) => {
            assert_no_readiness(&mut readiness_receiver)?;
            assert_worker_output(&output, STARTUP_READINESS_FAILURE_STDERR)
        }
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
    }
}

async fn run_invalid_startup_failure(
    operation_timeout: Duration,
    launch: &WorkerLaunch,
    startup: StartupConfiguration,
) -> SmokeResult {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .map_err(|_| FakeError("failed to bind loopback fake engine"))?;
    let address = listener
        .local_addr()
        .map_err(|_| FakeError("failed to inspect loopback fake engine"))?;
    let readiness_phase = Arc::new(AtomicU8::new(READINESS_PENDING));
    let (mut worker, mut readiness_receiver) = WorkerProcess::start(
        launch,
        format!("ws://127.0.0.1:{}", address.port()),
        readiness_phase,
        WorkerSettings::SINGLE.with_startup(startup),
    )
    .await?;
    let deadline = Instant::now() + operation_timeout;
    let startup_result = async {
        let _ = timeout_remaining(deadline, worker.wait_for_exit()).await??;
        match timeout(Duration::from_millis(100), listener.accept()).await {
            Err(_) => Ok(()),
            Ok(Ok(_)) => failure("invalid startup connected to the fake engine"),
            Ok(Err(_)) => failure("invalid startup connection check failed"),
        }
    }
    .await;

    if startup_result.is_err() {
        worker.close_stdin();
    }
    let cleanup_result = worker.finish(ExitExpectation::Failure).await;
    match (startup_result, cleanup_result) {
        (Ok(()), Ok(output)) => {
            assert_no_readiness(&mut readiness_receiver)?;
            assert_worker_output(&output, STARTUP_CONFIGURATION_FAILURE_STDERR)
        }
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
    }
}

async fn run_empty_lifecycle_shutdown(
    operation_timeout: Duration,
    launch: &WorkerLaunch,
    forced: bool,
) -> SmokeResult {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .map_err(|_| FakeError("failed to bind loopback fake engine"))?;
    let address = listener
        .local_addr()
        .map_err(|_| FakeError("failed to inspect loopback fake engine"))?;
    let readiness_phase = Arc::new(AtomicU8::new(READINESS_PENDING));
    let (mut worker, mut readiness_receiver) = WorkerProcess::start(
        launch,
        format!("ws://127.0.0.1:{}", address.port()),
        Arc::clone(&readiness_phase),
        WorkerSettings::SINGLE,
    )
    .await?;
    let deadline = Instant::now() + operation_timeout;
    let protocol_result = async {
        let (stream, _) = timeout_remaining(deadline, listener.accept())
            .await?
            .map_err(|_| FakeError("release worker did not connect to the fake engine"))?;
        let mut socket = timeout_remaining(deadline, accept_async(stream))
            .await?
            .map_err(|_| FakeError("release worker websocket handshake failed"))?;
        let registrations = complete_readiness(
            &mut socket,
            &mut readiness_receiver,
            &readiness_phase,
            CatalogScript::READY,
            deadline,
        )
        .await?;
        if forced {
            worker.start_forced_termination()?;
        } else {
            worker.close_stdin();
        }
        if forced {
            drain_forced_worker_shutdown(&mut socket, &registrations, deadline).await
        } else {
            let mut releases = ShutdownState::default();
            drain_worker_shutdown(&mut socket, &registrations, &mut releases, deadline).await
        }
    }
    .await;

    if protocol_result.is_err() && !forced {
        worker.close_stdin();
    }
    let expected_exit = if forced {
        ExitExpectation::ForcedTermination
    } else {
        ExitExpectation::Success
    };
    let cleanup_result = worker.finish(expected_exit).await;
    match (protocol_result, cleanup_result) {
        (Ok(()), Ok(output)) => {
            assert_worker_output(&output, READY_STDERR)?;
            #[cfg(unix)]
            if forced {
                assert_forced_termination(&output)?;
            }
            Ok(())
        }
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
    }
}

async fn run_lifecycle_deadline(operation_timeout: Duration, launch: &WorkerLaunch) -> SmokeResult {
    run_script(operation_timeout, launch, lifecycle_deadline_script()).await
}

fn assert_worker_output(output: &WorkerOutput, expected_stderr: &[u8]) -> SmokeResult {
    if !output.stdout.is_empty() {
        return failure("release worker wrote unexpected stdout output");
    }
    if output.stderr != expected_stderr {
        return failure("release worker stderr did not match the expected diagnostic");
    }
    Ok(())
}

fn assert_no_readiness(
    readiness_receiver: &mut mpsc::UnboundedReceiver<ReadinessReport>,
) -> SmokeResult {
    match readiness_receiver.try_recv() {
        Ok(_) => failure("release worker reported readiness for a failed startup"),
        Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => Ok(()),
    }
}

#[cfg(unix)]
fn assert_forced_termination(output: &WorkerOutput) -> SmokeResult {
    use std::os::unix::process::ExitStatusExt;

    if output.status.signal() == Some(9) {
        Ok(())
    } else {
        failure("forced release worker termination did not use SIGKILL")
    }
}

async fn run_shutdown_during_admitted_work(
    operation_timeout: Duration,
    launch: &WorkerLaunch,
    stimulus: ShutdownStimulus,
) -> SmokeResult {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .map_err(|_| FakeError("failed to bind loopback fake engine"))?;
    let address = listener
        .local_addr()
        .map_err(|_| FakeError("failed to inspect loopback fake engine"))?;
    let readiness_phase = Arc::new(AtomicU8::new(READINESS_PENDING));
    let (mut worker, mut readiness_receiver) = WorkerProcess::start(
        launch,
        format!("ws://127.0.0.1:{}", address.port()),
        Arc::clone(&readiness_phase),
        WorkerSettings::SINGLE,
    )
    .await?;
    let deadline = Instant::now() + operation_timeout;
    let protocol_result = async {
        let (stream, _) = timeout_remaining(deadline, listener.accept())
            .await?
            .map_err(|_| FakeError("release worker did not connect to the fake engine"))?;
        let mut socket = timeout_remaining(deadline, accept_async(stream))
            .await?
            .map_err(|_| FakeError("release worker websocket handshake failed"))?;
        let registrations = complete_readiness(
            &mut socket,
            &mut readiness_receiver,
            &readiness_phase,
            CatalogScript::READY,
            deadline,
        )
        .await?;
        drive_admitted_shutdown(&mut socket, &registrations, &mut worker, stimulus, deadline).await
    }
    .await;

    if protocol_result.is_err() {
        worker.close_stdin();
    }
    let cleanup_result = worker.finish(ExitExpectation::Success).await;
    match (protocol_result, cleanup_result) {
        (Ok(()), Ok(output)) => assert_worker_output(&output, READY_STDERR),
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
    }
}

async fn drive_admitted_shutdown(
    socket: &mut WebSocketStream<TcpStream>,
    registrations: &RegistrationState,
    worker: &mut WorkerProcess,
    stimulus: ShutdownStimulus,
    deadline: Instant,
) -> SmokeResult {
    let first = lifecycle_queue_invocation(
        "00000000-0000-4000-8000-000000000111",
        &FIRST_QUEUE_MEMORY,
        DirectExpectation::Success(queue_outcome(1, 1, 0, 1)),
    );
    let second = lifecycle_queue_invocation(
        "00000000-0000-4000-8000-000000000112",
        &SECOND_QUEUE_MEMORY,
        DirectExpectation::Success(queue_outcome(1, 1, 0, 1)),
    );
    let rejected = DirectInvocation {
        function: DirectFunction::EmbedVersions,
        invocation_id: scripted_invocation_id("00000000-0000-4000-8000-000000000113"),
        data: queue_event(&[&FIRST_QUEUE_MEMORY]),
        expected: DirectExpectation::Error,
        response_timing: None,
    };
    let first_read = lifecycle_load_step(&FIRST_QUEUE_MEMORY);
    let first_router = lifecycle_router_step(&FIRST_QUEUE_MEMORY);
    let first_write = insert_embedding_step(&FIRST_QUEUE_MEMORY);
    let second_read = lifecycle_load_step(&SECOND_QUEUE_MEMORY);
    let second_router = lifecycle_router_step(&SECOND_QUEUE_MEMORY);
    let second_write = insert_embedding_step(&SECOND_QUEUE_MEMORY);
    let committed_keys = CommittedKeyState::default();

    let first_started = Instant::now();
    send_direct_invocation(socket, &first).await?;
    let first_read_message = next_protocol_message(socket, deadline).await?;
    reply_to_expected_upstream(
        socket,
        &first_read_message,
        &first_read,
        &committed_keys,
        deadline,
    )
    .await?;

    let first_router_message = next_protocol_message(socket, deadline).await?;
    validate_upstream_invocation(&first_router_message, &first_router)?;
    let first_router_id = required_uuid(
        &first_router_message,
        "invocation_id",
        "upstream invocation",
    )?;

    let second_started = Instant::now();
    send_direct_invocation(socket, &second).await?;
    let second_read_message = next_protocol_message(socket, deadline).await?;
    if upstream_invocation_matches(&second_read_message, &second_router) {
        return failure("release worker exceeded the local router concurrency limit");
    }
    reply_to_expected_upstream(
        socket,
        &second_read_message,
        &second_read,
        &committed_keys,
        deadline,
    )
    .await?;

    let shutdown_started = Instant::now();
    request_shutdown(worker, stimulus).await?;
    let mut releases = ShutdownState::default();
    await_shutdown_release(socket, registrations, &mut releases, deadline).await?;

    let rejected_started = Instant::now();
    send_direct_invocation(socket, &rejected).await?;
    await_rejected_delivery(
        socket,
        registrations,
        &mut releases,
        &rejected,
        rejected_started,
        deadline,
    )
    .await?;

    send_scripted_reply(
        socket,
        &first_router_id,
        &first_router,
        &committed_keys,
        deadline,
    )
    .await?;

    let mut first_write_seen = false;
    let mut second_router_seen = false;
    let mut second_write_seen = false;
    let mut first_result_seen = false;
    let mut second_result_seen = false;
    while !first_result_seen || !second_result_seen {
        let message = next_protocol_message(socket, deadline).await?;
        match message_type(&message)? {
            "unregisterfunction" | "unregistertrigger" => {
                releases.record(&message, registrations)?;
            }
            "invokefunction"
                if !first_write_seen && upstream_invocation_matches(&message, &first_write) =>
            {
                reply_to_expected_upstream(
                    socket,
                    &message,
                    &first_write,
                    &committed_keys,
                    deadline,
                )
                .await?;
                first_write_seen = true;
            }
            "invokefunction"
                if !second_router_seen && upstream_invocation_matches(&message, &second_router) =>
            {
                reply_to_expected_upstream(
                    socket,
                    &message,
                    &second_router,
                    &committed_keys,
                    deadline,
                )
                .await?;
                second_router_seen = true;
            }
            "invokefunction"
                if !second_write_seen && upstream_invocation_matches(&message, &second_write) =>
            {
                reply_to_expected_upstream(
                    socket,
                    &message,
                    &second_write,
                    &committed_keys,
                    deadline,
                )
                .await?;
                second_write_seen = true;
            }
            "invocationresult"
                if message.get("invocation_id") == Some(&json!(first.invocation_id)) =>
            {
                if !first_write_seen {
                    return failure(
                        "release worker completed work before the first immutable write",
                    );
                }
                validate_direct_result(&message, &first, first_started.elapsed())?;
                first_result_seen = true;
            }
            "invocationresult"
                if message.get("invocation_id") == Some(&json!(second.invocation_id)) =>
            {
                if !second_write_seen {
                    return failure(
                        "release worker completed work before the second immutable write",
                    );
                }
                validate_direct_result(&message, &second, second_started.elapsed())?;
                second_result_seen = true;
            }
            "invokefunction" => {
                return failure("release worker made an unexpected admitted-work invocation");
            }
            "invocationresult" => {
                return failure("release worker returned an unexpected admitted-work result");
            }
            _ => return failure("release worker sent an unsupported admitted-work frame"),
        }
    }

    drain_worker_shutdown(socket, registrations, &mut releases, deadline).await?;
    if shutdown_started.elapsed() > SHUTDOWN_TIMEOUT {
        return failure("release worker exceeded the configured shutdown drain bound");
    }
    Ok(())
}

fn lifecycle_queue_invocation(
    invocation_id: &str,
    memory: &'static QueueMemory,
    expected: DirectExpectation,
) -> DirectInvocation {
    DirectInvocation {
        function: DirectFunction::EmbedVersions,
        invocation_id: scripted_invocation_id(invocation_id),
        data: queue_event(&[memory]),
        expected,
        response_timing: None,
    }
}

fn lifecycle_load_step(memory: &'static QueueMemory) -> UpstreamStep {
    UpstreamStep {
        function: UpstreamFunction::Database,
        expected_payload: load_keys_request(&[memory]),
        response: ScriptedResponse::Success(load_keys_response(&[LoadedQueueWork::Pending(
            memory,
        )])),
    }
}

fn lifecycle_router_step(memory: &'static QueueMemory) -> UpstreamStep {
    UpstreamStep {
        function: UpstreamFunction::Router,
        expected_payload: router_request(&[memory]),
        response: ScriptedResponse::Success(router_response(&[memory])),
    }
}

async fn request_shutdown(worker: &mut WorkerProcess, stimulus: ShutdownStimulus) -> SmokeResult {
    match stimulus {
        ShutdownStimulus::Eof => {
            worker.close_stdin();
            Ok(())
        }
        #[cfg(unix)]
        ShutdownStimulus::Signal => {
            let pid = worker
                .child
                .id()
                .ok_or(FakeError("release worker did not expose a process id"))?;
            let status = Command::new("kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await
                .map_err(|_| FakeError("failed to send release worker termination signal"))?;
            if status.success() {
                Ok(())
            } else {
                failure("release worker termination signal was rejected")
            }
        }
    }
}

async fn next_protocol_message(
    socket: &mut WebSocketStream<TcpStream>,
    deadline: Instant,
) -> SmokeResult<Value> {
    loop {
        match next_frame(socket, deadline).await? {
            Message::Ping(payload) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|_| FakeError("failed to answer worker ping"))?;
            }
            Message::Pong(_) => {}
            Message::Text(text) => return parse_protocol_message(text.as_str()),
            Message::Close(_) => {
                return failure("release worker closed before protocol completion");
            }
            Message::Binary(_) => return failure("release worker sent a binary protocol frame"),
            _ => return failure("release worker sent an unsupported websocket frame"),
        }
    }
}

async fn reply_to_expected_upstream(
    socket: &mut WebSocketStream<TcpStream>,
    message: &Value,
    step: &UpstreamStep,
    committed_keys: &CommittedKeyState,
    deadline: Instant,
) -> SmokeResult {
    validate_upstream_invocation(message, step)?;
    let invocation_id = required_uuid(message, "invocation_id", "upstream invocation")?;
    send_scripted_reply(socket, &invocation_id, step, committed_keys, deadline).await
}

async fn await_shutdown_release(
    socket: &mut WebSocketStream<TcpStream>,
    registrations: &RegistrationState,
    releases: &mut ShutdownState,
    deadline: Instant,
) -> SmokeResult {
    let message = next_protocol_message(socket, deadline).await?;
    match message_type(&message)? {
        "unregisterfunction" | "unregistertrigger" => {
            releases.record(&message, registrations)?;
            Ok(())
        }
        _ => failure("release worker sent a frame before shutdown release"),
    }
}

async fn await_rejected_delivery(
    socket: &mut WebSocketStream<TcpStream>,
    registrations: &RegistrationState,
    releases: &mut ShutdownState,
    rejected: &DirectInvocation,
    started_at: Instant,
    deadline: Instant,
) -> SmokeResult {
    loop {
        let message = next_protocol_message(socket, deadline).await?;
        match message_type(&message)? {
            "unregisterfunction" | "unregistertrigger" => {
                releases.record(&message, registrations)?;
            }
            "invocationresult" => {
                validate_direct_result(&message, rejected, started_at.elapsed())?;
                return Ok(());
            }
            "invokefunction" => {
                return failure("release worker admitted work after shutdown began");
            }
            _ => return failure("release worker sent an unsupported shutdown frame"),
        }
    }
}

fn load_worker_launch(manifest_path: &Path) -> SmokeResult<WorkerLaunch> {
    let contents = fs::read_to_string(manifest_path)
        .map_err(|_| FakeError("failed to read release worker manifest"))?;
    parse_worker_launch(&contents, manifest_path)
}

fn parse_worker_launch(contents: &str, manifest_path: &Path) -> SmokeResult<WorkerLaunch> {
    let manifest = serde_yaml::from_str::<WorkerManifest>(contents)
        .map_err(|_| FakeError("worker manifest is invalid"))?;
    let start = manifest
        .scripts
        .and_then(|scripts| scripts.start)
        .filter(|start| !start.trim().is_empty())
        .ok_or(FakeError("worker manifest must define scripts.start"))?;
    let working_directory = manifest_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let executable = parse_executable_path(&start, &working_directory)?;

    Ok(WorkerLaunch {
        working_directory,
        executable,
    })
}

fn parse_executable_path(start: &str, working_directory: &Path) -> SmokeResult<PathBuf> {
    if !start
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-'))
    {
        return failure("worker manifest scripts.start must be a safe executable path");
    }

    let path = Path::new(start);
    if !matches!(path.components().next_back(), Some(Component::Normal(_)))
        || path
            .components()
            .any(|component| matches!(component, Component::Prefix(_)))
    {
        return failure("worker manifest scripts.start must be a safe executable path");
    }

    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(working_directory.join(path))
    }
}

async fn capture_stream(
    mut stream: impl AsyncRead + Unpin,
    capture: Arc<Mutex<StreamCapture>>,
) -> SmokeResult {
    let mut buffer = [0_u8; 4096];
    loop {
        let bytes_read = stream
            .read(&mut buffer)
            .await
            .map_err(|_| FakeError("release worker output capture failed"))?;
        if bytes_read == 0 {
            return Ok(());
        }
        capture
            .lock()
            .map_err(|_| FakeError("release worker output capture lock failed"))?
            .append(&buffer[..bytes_read], "stdout")?;
    }
}

async fn capture_stderr(
    mut stderr: impl AsyncRead + Unpin,
    capture: Arc<Mutex<StreamCapture>>,
    readiness_phase: Arc<AtomicU8>,
    readiness_sender: mpsc::UnboundedSender<ReadinessReport>,
) -> SmokeResult {
    let mut buffer = [0_u8; 4096];
    let mut readiness = ReadinessDetector::default();

    loop {
        let bytes_read = stderr
            .read(&mut buffer)
            .await
            .map_err(|_| FakeError("release worker stderr capture failed"))?;
        if bytes_read == 0 {
            if readiness.finish() {
                let _ = readiness_sender.send(ReadinessReport {
                    phase: readiness_phase.load(Ordering::SeqCst),
                });
            }
            return Ok(());
        }
        capture
            .lock()
            .map_err(|_| FakeError("release worker stderr capture lock failed"))?
            .append(&buffer[..bytes_read], "stderr")?;
        if readiness.observe(&buffer[..bytes_read]) {
            let _ = readiness_sender.send(ReadinessReport {
                phase: readiness_phase.load(Ordering::SeqCst),
            });
        }
    }
}

async fn reap_after_start_failure(worker: &mut Child) -> SmokeResult {
    worker.stdin.take();
    match timeout(CHILD_CLEANUP_TIMEOUT, worker.wait()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(_)) => failure("failed to reap release worker"),
        Err(_) => {
            force_reap_worker(worker).await?;
            Ok(())
        }
    }
}

async fn force_reap_worker(worker: &mut Child) -> SmokeResult<ExitStatus> {
    let kill_failed = worker.start_kill().is_err();
    match timeout(CHILD_CLEANUP_TIMEOUT, worker.wait()).await {
        Ok(Ok(status)) => Ok(status),
        Ok(Err(_)) => failure("failed to reap release worker"),
        Err(_) if kill_failed => failure("failed to terminate release worker"),
        Err(_) => failure("release worker did not terminate after forced cleanup"),
    }
}

fn require_worker_success(status: ExitStatus) -> SmokeResult {
    if status.success() {
        Ok(())
    } else {
        failure("release worker exited unsuccessfully")
    }
}

fn require_worker_failure(status: ExitStatus) -> SmokeResult {
    if status.code() == Some(1) {
        Ok(())
    } else {
        failure("release worker did not exit with ExitCode::FAILURE")
    }
}

fn require_forced_worker_termination(status: ExitStatus) -> SmokeResult {
    if status.success() {
        failure("forced release worker termination exited successfully")
    } else {
        Ok(())
    }
}

async fn run_protocol(
    socket: &mut WebSocketStream<TcpStream>,
    mut readiness_receiver: mpsc::UnboundedReceiver<ReadinessReport>,
    readiness_phase: Arc<AtomicU8>,
    script: &ProtocolScript,
    deadline: Instant,
) -> SmokeResult<ProtocolRun> {
    let registrations = complete_readiness(
        socket,
        &mut readiness_receiver,
        &readiness_phase,
        CatalogScript::READY,
        deadline,
    )
    .await?;
    let mut expectations = script.expectations.clone();
    let mut upstream_index = 0;
    let mut delivery_index = 0;
    let mut invocation_counts = InvocationCounts::default();
    let mut committed_keys = CommittedKeyState::default();
    let mut pending_local_error: Option<PendingLocalError> = None;
    let mut post_late_barrier: Option<(DirectInvocation, Instant)> = None;

    let (invocation, _) = script
        .delivery(delivery_index)
        .ok_or(FakeError("protocol script delivery was missing"))?;
    let mut delivery_started = Instant::now();
    send_direct_invocation(socket, invocation).await?;
    invocation_counts.direct_deliveries += 1;

    loop {
        match next_frame(socket, deadline).await? {
            Message::Ping(payload) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|_| FakeError("failed to answer worker ping"))?;
            }
            Message::Pong(_) => {}
            Message::Close(_) => return failure("release worker closed before completion"),
            Message::Text(text) => {
                let message = parse_protocol_message(text.as_str())?;
                match message_type(&message)? {
                    "invocationresult" => {
                        if let Some((barrier, started_at)) = post_late_barrier.take() {
                            validate_direct_result(&message, &barrier, started_at.elapsed())?;
                            return Ok(ProtocolRun {
                                registrations,
                                invocation_counts,
                            });
                        }
                        if upstream_index != expectations.len() {
                            return failure(
                                "release worker returned an invocation result out of order",
                            );
                        }
                        let (invocation, _) = script
                            .delivery(delivery_index)
                            .ok_or(FakeError("protocol script delivery was missing"))?;
                        validate_direct_result(&message, invocation, delivery_started.elapsed())?;
                        if let Some(pending) = pending_local_error.take() {
                            if !matches!(&invocation.expected, DirectExpectation::Error) {
                                return failure(
                                    "await-local-error scripts must expect a direct error",
                                );
                            }
                            let barrier = script.post_late.clone().ok_or(FakeError(
                                "await-local-error scripts must define a post-late barrier",
                            ))?;
                            send_scripted_response(
                                socket,
                                &pending.invocation_id,
                                pending.function,
                                &pending.late_response,
                                &committed_keys,
                                deadline,
                            )
                            .await?;
                            send_direct_invocation(socket, &barrier).await?;
                            invocation_counts.direct_deliveries += 1;
                            post_late_barrier = Some((barrier, Instant::now()));
                            continue;
                        }
                        if delivery_index + 1 == script.delivery_count() {
                            return Ok(ProtocolRun {
                                registrations,
                                invocation_counts,
                            });
                        }

                        delivery_index += 1;
                        let (next_invocation, next_expectations) = script
                            .delivery(delivery_index)
                            .ok_or(FakeError("protocol script delivery was missing"))?;
                        expectations = next_expectations.to_vec();
                        upstream_index = 0;
                        delivery_started = Instant::now();
                        send_direct_invocation(socket, next_invocation).await?;
                        invocation_counts.direct_deliveries += 1;
                    }
                    "invokefunction" => {
                        if post_late_barrier.is_some() {
                            return failure(
                                "release worker made an invocation before completing the post-late barrier",
                            );
                        }
                        if upstream_index == expectations.len() {
                            return failure("release worker made an unexpected invocation");
                        }
                        let step =
                            next_upstream_step(&message, &mut expectations, &mut upstream_index)?;
                        invocation_counts.record_upstream(step.function);
                        let invocation_id =
                            required_uuid(&message, "invocation_id", "upstream invocation")?;
                        if let ScriptedResponse::AwaitLocalError { late_response } = &step.response
                        {
                            if step.function != UpstreamFunction::Router {
                                return failure(
                                    "await-local-error is only valid for router replies",
                                );
                            }
                            pending_local_error = Some(PendingLocalError {
                                invocation_id,
                                function: step.function,
                                late_response: (**late_response).clone(),
                            });
                        } else {
                            send_scripted_reply(
                                socket,
                                &invocation_id,
                                &step,
                                &committed_keys,
                                deadline,
                            )
                            .await?;
                            committed_keys.record_successful_insert(&step);
                        }
                    }
                    _ => return failure("release worker sent an unsupported protocol frame"),
                }
            }
            Message::Binary(_) => return failure("release worker sent a binary protocol frame"),
            _ => return failure("release worker sent an unsupported websocket frame"),
        }
    }
}

async fn complete_readiness(
    socket: &mut WebSocketStream<TcpStream>,
    readiness_receiver: &mut mpsc::UnboundedReceiver<ReadinessReport>,
    readiness_phase: &Arc<AtomicU8>,
    catalog: CatalogScript,
    deadline: Instant,
) -> SmokeResult<RegistrationState> {
    let mut session = CatalogSession::new(catalog);
    loop {
        match next_frame(socket, deadline).await? {
            Message::Ping(payload) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|_| FakeError("failed to answer worker ping"))?;
            }
            Message::Pong(_) => {}
            Message::Close(_) => return failure("release worker closed before readiness"),
            Message::Text(text) => match session
                .handle_message(
                    socket,
                    &parse_protocol_message(text.as_str())?,
                    readiness_phase,
                )
                .await?
            {
                CatalogProgress::Continue => {}
                CatalogProgress::Ready => {
                    wait_for_worker_ready(readiness_receiver, deadline).await?;
                    return Ok(session.registrations);
                }
                CatalogProgress::Failed => {
                    return failure("release worker catalog profile rejected readiness");
                }
            },
            Message::Binary(_) => return failure("release worker sent a binary protocol frame"),
            _ => return failure("release worker sent an unsupported websocket frame"),
        }
    }
}

async fn drive_catalog_failure(
    socket: &mut WebSocketStream<TcpStream>,
    readiness_receiver: &mut mpsc::UnboundedReceiver<ReadinessReport>,
    readiness_phase: Arc<AtomicU8>,
    catalog: CatalogScript,
    child_started_at: Instant,
    deadline: Instant,
) -> SmokeResult {
    let mut session = CatalogSession::new(catalog);
    let mut releases = ShutdownState::default();
    let mut catalog_rejected = false;

    loop {
        let Some(frame) =
            next_shutdown_frame(socket, deadline, ShutdownTerminal::FailedStartup).await?
        else {
            return finish_catalog_failure(
                &session,
                catalog_rejected,
                readiness_receiver,
                child_started_at.elapsed(),
            );
        };
        match frame {
            Message::Ping(payload) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|_| FakeError("failed to answer worker ping"))?;
            }
            Message::Pong(_) => {}
            Message::Close(_) => {
                return finish_catalog_failure(
                    &session,
                    catalog_rejected,
                    readiness_receiver,
                    child_started_at.elapsed(),
                );
            }
            Message::Text(text) => {
                let message = parse_protocol_message(text.as_str())?;
                match message_type(&message)? {
                    "unregisterfunction" | "unregistertrigger" => {
                        releases.record(&message, &session.registrations)?;
                    }
                    _ if catalog_rejected => {
                        return failure("release worker sent a frame after readiness rejection");
                    }
                    _ => match session
                        .handle_message(socket, &message, &readiness_phase)
                        .await?
                    {
                        CatalogProgress::Continue => {}
                        CatalogProgress::Ready => {
                            return failure("release worker reported ready for a rejected catalog");
                        }
                        CatalogProgress::Failed => catalog_rejected = true,
                    },
                }
            }
            Message::Binary(_) => return failure("release worker sent a binary protocol frame"),
            _ => return failure("release worker sent an unsupported websocket frame"),
        }
    }
}

fn finish_catalog_failure(
    session: &CatalogSession,
    catalog_rejected: bool,
    readiness_receiver: &mut mpsc::UnboundedReceiver<ReadinessReport>,
    elapsed: Duration,
) -> SmokeResult {
    if session.script.is_pending() {
        if session.pending_polls == 0 {
            return failure("release worker did not poll the pending readiness catalog");
        }
        assert_readiness_timeout(elapsed)?;
    } else if !catalog_rejected {
        return failure("release worker exited before the catalog rejection was delivered");
    }
    assert_no_readiness(readiness_receiver)
}

fn assert_readiness_timeout(elapsed: Duration) -> SmokeResult {
    let readiness_timeout = memory_embedding::runtime::WORKER_READINESS_TIMEOUT;
    if elapsed < readiness_timeout {
        return failure("release worker exited before the readiness timeout");
    }
    if elapsed > readiness_timeout + STARTUP_READINESS_GRACE {
        return failure("release worker exceeded the readiness timeout bound");
    }
    Ok(())
}

async fn handle_catalog_request(
    socket: &mut WebSocketStream<TcpStream>,
    message: &Value,
    registrations: &RegistrationState,
    readiness_step: &mut u8,
    readiness_phase: &Arc<AtomicU8>,
    catalog: CatalogScript,
) -> SmokeResult<CatalogProgress> {
    registrations.verify_complete()?;
    let function_id = required_string(message, "function_id", "catalog invocation")?;
    match (*readiness_step, function_id) {
        (0, FUNCTIONS_INFO_FUNCTION_ID) => {
            let invocation_id = validate_catalog_invocation(
                message,
                FUNCTIONS_INFO_FUNCTION_ID,
                json!({
                    "function_ids": [
                        EMBED_VERSIONS_FUNCTION_ID,
                        RECONCILE_EMBEDDINGS_FUNCTION_ID,
                    ],
                    "namespace": DEFAULT_NAMESPACE,
                }),
            )?;
            let (response, progress) = catalog.function_response(registrations)?;
            send_invocation_result(socket, &invocation_id, FUNCTIONS_INFO_FUNCTION_ID, response)
                .await?;
            if !catalog.is_pending() && progress == CatalogProgress::Continue {
                *readiness_step = 1;
            }
            Ok(progress)
        }
        (1, REGISTERED_TRIGGERS_LIST_FUNCTION_ID) => {
            let invocation_id = validate_catalog_invocation(
                message,
                REGISTERED_TRIGGERS_LIST_FUNCTION_ID,
                json!({
                    "function_id": EMBED_VERSIONS_FUNCTION_ID,
                    "trigger_type": DURABLE_SUBSCRIBER_TRIGGER_TYPE,
                    "include_pending": true,
                }),
            )?;
            let (response, progress) = catalog.trigger_list_response(
                registrations,
                DirectFunction::EmbedVersions,
                DURABLE_SUBSCRIBER_TRIGGER_TYPE,
            )?;
            send_invocation_result(
                socket,
                &invocation_id,
                REGISTERED_TRIGGERS_LIST_FUNCTION_ID,
                response,
            )
            .await?;
            if progress == CatalogProgress::Continue {
                *readiness_step = 2;
            }
            Ok(progress)
        }
        (2, REGISTERED_TRIGGERS_INFO_FUNCTION_ID) => {
            let trigger_id = registrations.trigger_id(DirectFunction::EmbedVersions)?;
            let invocation_id = validate_catalog_invocation(
                message,
                REGISTERED_TRIGGERS_INFO_FUNCTION_ID,
                json!({"id": trigger_id}),
            )?;
            let (response, progress) =
                catalog.trigger_detail_response(registrations, DirectFunction::EmbedVersions)?;
            send_invocation_result(
                socket,
                &invocation_id,
                REGISTERED_TRIGGERS_INFO_FUNCTION_ID,
                response,
            )
            .await?;
            if progress == CatalogProgress::Continue {
                *readiness_step = 3;
            }
            Ok(progress)
        }
        (3, REGISTERED_TRIGGERS_LIST_FUNCTION_ID) => {
            let invocation_id = validate_catalog_invocation(
                message,
                REGISTERED_TRIGGERS_LIST_FUNCTION_ID,
                json!({
                    "function_id": RECONCILE_EMBEDDINGS_FUNCTION_ID,
                    "trigger_type": CRON_TRIGGER_TYPE,
                    "include_pending": true,
                }),
            )?;
            let (response, progress) = catalog.trigger_list_response(
                registrations,
                DirectFunction::ReconcileEmbeddings,
                CRON_TRIGGER_TYPE,
            )?;
            send_invocation_result(
                socket,
                &invocation_id,
                REGISTERED_TRIGGERS_LIST_FUNCTION_ID,
                response,
            )
            .await?;
            if progress == CatalogProgress::Continue {
                *readiness_step = 4;
            }
            Ok(progress)
        }
        (4, REGISTERED_TRIGGERS_INFO_FUNCTION_ID) => {
            let trigger_id = registrations.trigger_id(DirectFunction::ReconcileEmbeddings)?;
            let invocation_id = validate_catalog_invocation(
                message,
                REGISTERED_TRIGGERS_INFO_FUNCTION_ID,
                json!({"id": trigger_id}),
            )?;
            let (response, progress) = catalog
                .trigger_detail_response(registrations, DirectFunction::ReconcileEmbeddings)?;
            if progress == CatalogProgress::Continue {
                readiness_phase.store(READINESS_CATALOG_COMPLETE, Ordering::SeqCst);
            }
            send_invocation_result(
                socket,
                &invocation_id,
                REGISTERED_TRIGGERS_INFO_FUNCTION_ID,
                response,
            )
            .await?;
            if progress == CatalogProgress::Continue {
                *readiness_step = 5;
                Ok(CatalogProgress::Ready)
            } else {
                Ok(progress)
            }
        }
        _ => failure("release worker requested catalogs out of readiness order"),
    }
}

async fn drain_shutdown(
    socket: &mut WebSocketStream<TcpStream>,
    registrations: &RegistrationState,
    deadline: Instant,
) -> SmokeResult {
    socket
        .close(None)
        .await
        .map_err(|_| FakeError("failed to initiate fake server websocket close"))?;
    let mut releases = ShutdownState::default();
    drain_worker_shutdown(socket, registrations, &mut releases, deadline).await
}

async fn drain_worker_shutdown(
    socket: &mut WebSocketStream<TcpStream>,
    registrations: &RegistrationState,
    releases: &mut ShutdownState,
    deadline: Instant,
) -> SmokeResult {
    drain_worker_shutdown_with_terminal(
        socket,
        registrations,
        releases,
        deadline,
        ShutdownTerminal::Clean,
    )
    .await
}

async fn drain_forced_worker_shutdown(
    socket: &mut WebSocketStream<TcpStream>,
    registrations: &RegistrationState,
    deadline: Instant,
) -> SmokeResult {
    let mut releases = ShutdownState::default();
    drain_worker_shutdown_with_terminal(
        socket,
        registrations,
        &mut releases,
        deadline,
        ShutdownTerminal::Forced,
    )
    .await
}

#[derive(Clone, Copy)]
enum ShutdownTerminal {
    Clean,
    FailedStartup,
    Forced,
}

async fn drain_worker_shutdown_with_terminal(
    socket: &mut WebSocketStream<TcpStream>,
    registrations: &RegistrationState,
    releases: &mut ShutdownState,
    deadline: Instant,
    terminal: ShutdownTerminal,
) -> SmokeResult {
    loop {
        let Some(frame) = next_shutdown_frame(socket, deadline, terminal).await? else {
            return Ok(());
        };
        match frame {
            Message::Ping(_) => {}
            Message::Pong(_) => {}
            Message::Close(_) => return Ok(()),
            Message::Text(text) => {
                let message = parse_protocol_message(text.as_str())?;
                releases.record(&message, registrations)?
            }
            _ => return failure("release worker sent an unsupported shutdown frame"),
        }
    }
}

async fn next_shutdown_frame(
    socket: &mut WebSocketStream<TcpStream>,
    deadline: Instant,
    terminal: ShutdownTerminal,
) -> SmokeResult<Option<Message>> {
    match timeout(remaining(deadline)?, socket.next()).await {
        Err(_) => failure("release worker did not close the fake engine connection"),
        Ok(None) => Ok(None),
        Ok(Some(Err(error))) if terminal_error_is_accepted(terminal, &error) => Ok(None),
        Ok(Some(Err(error))) => Err(shutdown_transport_error(&error)),
        Ok(Some(Ok(frame))) => Ok(Some(frame)),
    }
}

fn shutdown_transport_error(error: &TungsteniteError) -> FakeError {
    match error {
        TungsteniteError::Io(error) => match error.kind() {
            std::io::ErrorKind::BrokenPipe => {
                FakeError("release worker websocket broke during shutdown")
            }
            std::io::ErrorKind::ConnectionAborted => {
                FakeError("release worker websocket aborted during shutdown")
            }
            std::io::ErrorKind::ConnectionReset => {
                FakeError("release worker websocket reset during shutdown")
            }
            std::io::ErrorKind::UnexpectedEof => {
                FakeError("release worker websocket ended unexpectedly during shutdown")
            }
            _ => FakeError("release worker websocket I/O failed during shutdown"),
        },
        TungsteniteError::AlreadyClosed => {
            FakeError("release worker websocket closed unexpectedly")
        }
        TungsteniteError::Protocol(_) => {
            FakeError("release worker websocket protocol failed during shutdown")
        }
        _ => FakeError("release worker websocket failed during shutdown"),
    }
}

fn terminal_error_is_accepted(terminal: ShutdownTerminal, error: &TungsteniteError) -> bool {
    is_clean_terminal_close_error(error)
        || matches!(
            (terminal, error),
            (
                ShutdownTerminal::FailedStartup | ShutdownTerminal::Forced,
                TungsteniteError::Io(error)
            )
                if error.kind() == std::io::ErrorKind::ConnectionReset
        )
        || matches!(
            (terminal, error),
            (ShutdownTerminal::Clean, TungsteniteError::Io(error))
                if error.kind() == std::io::ErrorKind::ConnectionReset
        )
}

fn is_clean_terminal_close_error(error: &TungsteniteError) -> bool {
    matches!(
        error,
        TungsteniteError::ConnectionClosed
            | TungsteniteError::Protocol(ProtocolError::ResetWithoutClosingHandshake)
    )
}

async fn next_frame(
    socket: &mut WebSocketStream<TcpStream>,
    deadline: Instant,
) -> SmokeResult<Message> {
    timeout_remaining(deadline, socket.next())
        .await?
        .ok_or(FakeError("release worker disconnected before completion"))?
        .map_err(|_| FakeError("release worker websocket failed"))
}

async fn timeout_remaining<F>(deadline: Instant, future: F) -> SmokeResult<F::Output>
where
    F: std::future::Future,
{
    timeout(remaining(deadline)?, future)
        .await
        .map_err(|_| FakeError("protocol fake operation timed out"))
}

fn remaining(deadline: Instant) -> SmokeResult<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return failure("protocol fake operation timed out");
    }
    Ok(remaining)
}

async fn send_worker_registered(socket: &mut WebSocketStream<TcpStream>) -> SmokeResult {
    send_protocol_message(
        socket,
        json!({
            "type": "workerregistered",
            "worker_id": "memory-embedding-protocol-fake-engine",
        }),
    )
    .await
}

async fn send_invocation_result(
    socket: &mut WebSocketStream<TcpStream>,
    invocation_id: &str,
    function_id: &str,
    result: Value,
) -> SmokeResult {
    send_protocol_message(
        socket,
        json!({
            "type": "invocationresult",
            "invocation_id": invocation_id,
            "function_id": function_id,
            "result": result,
        }),
    )
    .await
}

async fn send_direct_invocation(
    socket: &mut WebSocketStream<TcpStream>,
    invocation: &DirectInvocation,
) -> SmokeResult {
    send_protocol_message(
        socket,
        json!({
            "type": "invokefunction",
            "invocation_id": invocation.invocation_id,
            "function_id": invocation.function.function_id(),
            "data": invocation.data,
        }),
    )
    .await
}

async fn send_scripted_reply(
    socket: &mut WebSocketStream<TcpStream>,
    invocation_id: &str,
    step: &UpstreamStep,
    committed_keys: &CommittedKeyState,
    deadline: Instant,
) -> SmokeResult {
    send_scripted_response(
        socket,
        invocation_id,
        step.function,
        &step.response,
        committed_keys,
        deadline,
    )
    .await
}

async fn send_scripted_response(
    socket: &mut WebSocketStream<TcpStream>,
    invocation_id: &str,
    function: UpstreamFunction,
    response: &ScriptedResponse,
    committed_keys: &CommittedKeyState,
    deadline: Instant,
) -> SmokeResult {
    let mut response = response;
    loop {
        match response {
            ScriptedResponse::Delayed {
                delay,
                response: next,
            } => {
                if *delay >= remaining(deadline)? {
                    return failure("scripted response delay exceeded the operation deadline");
                }
                tokio::time::sleep(*delay).await;
                response = next;
            }
            ScriptedResponse::Success(result) | ScriptedResponse::Malformed(result) => {
                return send_invocation_result(
                    socket,
                    invocation_id,
                    function.function_id(),
                    result.clone(),
                )
                .await;
            }
            ScriptedResponse::RemoteError => {
                return send_protocol_message(
                    socket,
                    json!({
                        "type": "invocationresult",
                        "invocation_id": invocation_id,
                        "function_id": function.function_id(),
                        "error": {
                            "code": REMOTE_CODE,
                            "message": REMOTE_MESSAGE,
                            "stacktrace": REMOTE_STACKTRACE,
                        },
                    }),
                )
                .await;
            }
            ScriptedResponse::AwaitLocalError { .. } => {
                return failure("await-local-error response was sent before the local error");
            }
            ScriptedResponse::LoadCommittedKeys { memories } => {
                return send_invocation_result(
                    socket,
                    invocation_id,
                    function.function_id(),
                    committed_keys.load_response(memories),
                )
                .await;
            }
            ScriptedResponse::ListUncommittedMemories { memories } => {
                return send_invocation_result(
                    socket,
                    invocation_id,
                    function.function_id(),
                    committed_keys.list_missing_response(memories),
                )
                .await;
            }
        }
    }
}

async fn send_protocol_message(
    socket: &mut WebSocketStream<TcpStream>,
    message: Value,
) -> SmokeResult {
    socket
        .send(Message::Text(message.to_string().into()))
        .await
        .map_err(|_| FakeError("failed to send fake engine protocol reply"))
}

async fn wait_for_worker_ready(
    readiness_receiver: &mut mpsc::UnboundedReceiver<ReadinessReport>,
    deadline: Instant,
) -> SmokeResult {
    let readiness = timeout_remaining(deadline, readiness_receiver.recv())
        .await?
        .ok_or(FakeError("release worker stderr ended before readiness"))?;
    if readiness.phase != READINESS_CATALOG_COMPLETE {
        return failure("release worker reported ready before catalog ownership completed");
    }
    Ok(())
}

fn validate_worker_registration(message: &Value) -> SmokeResult {
    let object = exact_object(
        message,
        &["type", "invocation_id", "function_id", "data", "action"],
        "worker registration",
    )?;
    if object.get("type") != Some(&json!("invokefunction"))
        || object.get("invocation_id") != Some(&Value::Null)
        || object.get("function_id") != Some(&json!(WORKER_REGISTER_FUNCTION_ID))
        || object.get("action") != Some(&json!({"type": "void"}))
    {
        return failure("release worker registration did not match the managed contract");
    }
    let data = object
        .get("data")
        .and_then(Value::as_object)
        .ok_or(FakeError(
            "release worker registration metadata was malformed",
        ))?;
    if data.get("name").and_then(Value::as_str) != Some(WORKER_NAME)
        || data.get("namespace").and_then(Value::as_str) != Some(DEFAULT_NAMESPACE)
    {
        return failure("release worker registration used the wrong managed identity");
    }
    Ok(())
}

fn validate_function_registration(message: &Value, function_id: &str) -> SmokeResult<String> {
    let object = exact_object(
        message,
        &[
            "type",
            "id",
            "request_format",
            "response_format",
            "metadata",
        ],
        "function registration",
    )?;
    if object.get("type") != Some(&json!("registerfunction")) {
        return failure("release worker function registration had the wrong type");
    }
    let (expected_request, expected_response) = match function_id {
        EMBED_VERSIONS_FUNCTION_ID => (
            sdk_function_schema::<Value>()?,
            sdk_function_schema::<Value>()?,
        ),
        RECONCILE_EMBEDDINGS_FUNCTION_ID => (
            sdk_function_schema::<ContentSafeCronCall>()?,
            sdk_function_schema::<Value>()?,
        ),
        _ => return failure("release worker registered an unexpected function"),
    };
    if object.get("request_format") != Some(&expected_request)
        || object.get("response_format") != Some(&expected_response)
    {
        return failure("release worker function schema did not match the runtime contract");
    }
    registration_nonce(message)
}

fn validate_trigger_registration(message: &Value) -> SmokeResult {
    let object = exact_object(
        message,
        &[
            "type",
            "id",
            "trigger_type",
            "function_id",
            "config",
            "metadata",
            "namespace",
            "trigger_namespace",
        ],
        "trigger registration",
    )?;
    if object.get("type") != Some(&json!("registertrigger"))
        || object.get("namespace") != Some(&json!(DEFAULT_NAMESPACE))
        || object.get("trigger_namespace") != Some(&json!(DEFAULT_NAMESPACE))
    {
        return failure("release worker trigger namespaces did not match the runtime contract");
    }
    match object.get("trigger_type").and_then(Value::as_str) {
        Some(DURABLE_SUBSCRIBER_TRIGGER_TYPE) => {
            if object.get("function_id") != Some(&json!(EMBED_VERSIONS_FUNCTION_ID))
                || object.get("config") != Some(&subscriber_config())
            {
                return failure(
                    "release worker durable subscriber did not match the runtime contract",
                );
            }
        }
        Some(CRON_TRIGGER_TYPE) => {
            if object.get("function_id") != Some(&json!(RECONCILE_EMBEDDINGS_FUNCTION_ID))
                || object.get("config") != Some(&cron_config())
            {
                return failure("release worker cron trigger did not match the runtime contract");
            }
        }
        _ => return failure("release worker registered an unexpected trigger"),
    }
    let _ = required_uuid(message, "id", "trigger registration")?;
    let _ = registration_nonce(message)?;
    Ok(())
}

fn validate_catalog_invocation(
    message: &Value,
    function_id: &str,
    expected_payload: Value,
) -> SmokeResult<String> {
    let object = invoke_object(message, "catalog invocation")?;
    if object.get("function_id") != Some(&json!(function_id))
        || object.get("namespace") != Some(&json!(DEFAULT_NAMESPACE))
        || object.get("data") != Some(&expected_payload)
        || object.get("action").is_some_and(|value| !value.is_null())
        || object.get("metadata").is_some_and(|value| !value.is_null())
    {
        return failure("release worker catalog request did not match the readiness contract");
    }
    required_uuid(message, "invocation_id", "catalog invocation")
}

fn validate_upstream_invocation(message: &Value, step: &UpstreamStep) -> SmokeResult {
    let object = invoke_object(message, "upstream invocation")?;
    if object.get("function_id") != Some(&json!(step.function.function_id()))
        || object.get("namespace") != Some(&json!(DEFAULT_NAMESPACE))
        || object.get("data") != Some(&step.expected_payload)
        || object.get("action").is_some_and(|value| !value.is_null())
        || object.get("metadata").is_some_and(|value| !value.is_null())
    {
        return failure("release worker upstream request did not match the script");
    }
    let _ = required_uuid(message, "invocation_id", "upstream invocation")?;
    Ok(())
}

fn upstream_invocation_matches(message: &Value, step: &UpstreamStep) -> bool {
    let Ok(object) = invoke_object(message, "upstream invocation") else {
        return false;
    };
    object.get("function_id") == Some(&json!(step.function.function_id()))
        && object.get("namespace") == Some(&json!(DEFAULT_NAMESPACE))
        && object.get("data") == Some(&step.expected_payload)
        && object.get("action").is_none_or(Value::is_null)
        && object.get("metadata").is_none_or(Value::is_null)
        && required_uuid(message, "invocation_id", "upstream invocation").is_ok()
}

fn next_upstream_step(
    message: &Value,
    expectations: &mut [UpstreamExpectation],
    upstream_index: &mut usize,
) -> SmokeResult<UpstreamStep> {
    let Some(expectation) = expectations.get_mut(*upstream_index) else {
        return failure("release worker made an unexpected invocation");
    };
    match expectation {
        UpstreamExpectation::Ordered(step) => {
            validate_upstream_invocation(message, step)?;
            *upstream_index += 1;
            Ok(step.clone())
        }
        UpstreamExpectation::Unordered(steps) => {
            let Some(step_index) = steps
                .iter()
                .position(|step| upstream_invocation_matches(message, step))
            else {
                return failure(
                    "release worker upstream request did not match the unordered script",
                );
            };
            let step = steps.remove(step_index);
            if steps.is_empty() {
                *upstream_index += 1;
            }
            Ok(step)
        }
    }
}

fn validate_direct_result(
    message: &Value,
    invocation: &DirectInvocation,
    elapsed: Duration,
) -> SmokeResult {
    let object = allowed_object(
        message,
        &[
            "type",
            "invocation_id",
            "function_id",
            "result",
            "error",
            "traceparent",
            "baggage",
        ],
        "direct invocation result",
    )?;
    if object.get("type") != Some(&json!("invocationresult"))
        || object.get("function_id") != Some(&json!(invocation.function.function_id()))
        || object.get("invocation_id") != Some(&json!(invocation.invocation_id.to_string()))
        || protected_sentinel_present(message.to_string().as_bytes())
    {
        return failure(
            "release worker direct invocation result was invalid or exposed protected data",
        );
    }
    match &invocation.expected {
        DirectExpectation::Success(expected) => {
            if object.get("result") != Some(expected)
                || object.get("error").is_some_and(|value| !value.is_null())
            {
                return failure(
                    "release worker direct invocation did not return the scripted success",
                );
            }
        }
        DirectExpectation::Error => {
            if object.get("result").is_some_and(|value| !value.is_null())
                || !object.get("error").is_some_and(Value::is_object)
            {
                return failure(
                    "release worker direct invocation did not return the scripted error",
                );
            }
        }
    }
    if let Some(response_timing) = invocation.response_timing {
        response_timing.validate(elapsed)?;
    }
    Ok(())
}

#[derive(Default)]
struct ShutdownState {
    queue_function: bool,
    reconciliation_function: bool,
    subscriber: bool,
    cron: bool,
}

impl ShutdownState {
    fn record(&mut self, message: &Value, registrations: &RegistrationState) -> SmokeResult {
        match message_type(message)? {
            "unregisterfunction" => {
                let object = exact_object(message, &["type", "id"], "function release")?;
                match object.get("id").and_then(Value::as_str) {
                    Some(EMBED_VERSIONS_FUNCTION_ID) => {
                        Self::record_release(&mut self.queue_function, "function")
                    }
                    Some(RECONCILE_EMBEDDINGS_FUNCTION_ID) => {
                        Self::record_release(&mut self.reconciliation_function, "function")
                    }
                    _ => failure("release worker released an unexpected function"),
                }
            }
            "unregistertrigger" => {
                let object =
                    exact_object(message, &["type", "id", "trigger_type"], "trigger release")?;
                let subscriber_id = registrations.trigger_id(DirectFunction::EmbedVersions)?;
                let cron_id = registrations.trigger_id(DirectFunction::ReconcileEmbeddings)?;
                match (
                    object.get("id").and_then(Value::as_str),
                    object.get("trigger_type").and_then(Value::as_str),
                ) {
                    (Some(id), Some(DURABLE_SUBSCRIBER_TRIGGER_TYPE)) if id == subscriber_id => {
                        Self::record_release(&mut self.subscriber, "trigger")
                    }
                    (Some(id), Some(CRON_TRIGGER_TYPE)) if id == cron_id => {
                        Self::record_release(&mut self.cron, "trigger")
                    }
                    _ => failure("release worker released an unexpected trigger"),
                }
            }
            _ => failure("release worker sent an unexpected shutdown message"),
        }
    }

    fn record_release(released: &mut bool, kind: &'static str) -> SmokeResult {
        if *released {
            return failure(match kind {
                "function" => "release worker released a function more than once",
                _ => "release worker released a trigger more than once",
            });
        }
        *released = true;
        Ok(())
    }
}

fn function_catalog_response(registrations: &RegistrationState) -> SmokeResult<Value> {
    let nonce = registrations.nonce()?;
    Ok(json!({
        "functions": [
            {
                "function_id": EMBED_VERSIONS_FUNCTION_ID,
                "namespace": DEFAULT_NAMESPACE,
                "worker_name": WORKER_NAME,
                "metadata": {"registration_nonce": nonce},
            },
            {
                "function_id": RECONCILE_EMBEDDINGS_FUNCTION_ID,
                "namespace": DEFAULT_NAMESPACE,
                "worker_name": WORKER_NAME,
                "metadata": {"registration_nonce": nonce},
            },
        ],
    }))
}

fn registered_trigger_list_response(
    registrations: &RegistrationState,
    function: DirectFunction,
    trigger_type: &str,
) -> SmokeResult<Value> {
    Ok(json!({
        "registered_triggers": [{
            "id": registrations.trigger_id(function)?,
            "trigger_type": trigger_type,
            "function_id": function.function_id(),
        }],
    }))
}

fn registered_trigger_detail_response(
    registrations: &RegistrationState,
    function: DirectFunction,
) -> SmokeResult<Value> {
    let (trigger_type, config) = match function {
        DirectFunction::EmbedVersions => (DURABLE_SUBSCRIBER_TRIGGER_TYPE, subscriber_config()),
        DirectFunction::ReconcileEmbeddings => (CRON_TRIGGER_TYPE, cron_config()),
    };
    Ok(json!({
        "id": registrations.trigger_id(function)?,
        "trigger_type": trigger_type,
        "function_id": function.function_id(),
        "worker_name": WORKER_NAME,
        "status": "active",
        "config": config,
        "metadata": {"registration_nonce": registrations.nonce()?},
        "trigger": {
            "id": trigger_type,
            "namespace": DEFAULT_NAMESPACE,
        },
        "function": {
            "function_id": function.function_id(),
            "namespace": DEFAULT_NAMESPACE,
            "worker_name": WORKER_NAME,
        },
    }))
}

fn subscriber_config() -> Value {
    json!({
        "queue": QUEUE_TOPIC,
        "max_retries": 3,
        "backoff_ms": 1_000,
        "queue_config": {
            "type": "concurrent",
            "concurrency": MAX_IN_FLIGHT,
        },
    })
}

fn cron_config() -> Value {
    json!({"expression": CRON_EXPRESSION})
}

#[derive(Clone, Copy)]
struct QueueMemory {
    id: &'static str,
    version: &'static str,
    title: &'static str,
    content: &'static str,
    concepts: &'static [&'static str],
    vector: &'static [f32],
    vector_text: &'static str,
}

static FIRST_QUEUE_MEMORY: QueueMemory = QueueMemory {
    id: MEMORY_ID,
    version: "1",
    title: MEMORY_TITLE,
    content: MEMORY_CONTENT,
    concepts: &[MEMORY_CONCEPT],
    vector: &[1.0, -0.5],
    vector_text: "[1.0,-0.5]",
};

static SECOND_QUEUE_MEMORY: QueueMemory = QueueMemory {
    id: SECOND_MEMORY_ID,
    version: "2",
    title: SECOND_MEMORY_TITLE,
    content: SECOND_MEMORY_CONTENT,
    concepts: &[SECOND_MEMORY_CONCEPT_FIRST, SECOND_MEMORY_CONCEPT_SECOND],
    vector: &[3.25, 4.5],
    vector_text: "[3.25,4.5]",
};

static CURRENT_MEMORY: QueueMemory = QueueMemory {
    id: MEMORY_ID,
    version: "3",
    title: "memory-embedding-private-current-title",
    content: "memory-embedding-private-current-content",
    concepts: &["memory-embedding-private-current-concept"],
    vector: &[7.0, -1.25],
    vector_text: "[7.0,-1.25]",
};

#[derive(Clone, Copy)]
enum LoadedQueueWork {
    Pending(&'static QueueMemory),
    AlreadyPresent(&'static QueueMemory),
    Missing(&'static QueueMemory),
}

impl LoadedQueueWork {
    const fn memory(self) -> &'static QueueMemory {
        match self {
            Self::Pending(memory) | Self::AlreadyPresent(memory) | Self::Missing(memory) => memory,
        }
    }

    const fn is_pending(self) -> bool {
        matches!(self, Self::Pending(_))
    }
}

#[derive(Default)]
struct CommittedKeyState {
    keys: Vec<(&'static str, &'static str)>,
}

impl CommittedKeyState {
    fn record_successful_insert(&mut self, step: &UpstreamStep) {
        let Some(memory) = exact_successful_insert_memory(step) else {
            return;
        };
        if !self
            .keys
            .iter()
            .any(|(id, version)| *id == memory.id && *version == memory.version)
        {
            self.keys.push((memory.id, memory.version));
        }
    }

    fn load_response(&self, memories: &[&'static QueueMemory]) -> Value {
        let work = memories
            .iter()
            .map(|memory| {
                if self
                    .keys
                    .iter()
                    .any(|(id, version)| *id == memory.id && *version == memory.version)
                {
                    LoadedQueueWork::AlreadyPresent(memory)
                } else {
                    LoadedQueueWork::Pending(memory)
                }
            })
            .collect::<Vec<_>>();
        load_keys_response(&work)
    }

    fn list_missing_response(&self, memories: &[&'static QueueMemory]) -> Value {
        let uncommitted = memories
            .iter()
            .copied()
            .filter(|memory| {
                !self
                    .keys
                    .iter()
                    .any(|(id, version)| *id == memory.id && *version == memory.version)
            })
            .collect::<Vec<_>>();
        list_missing_response(&uncommitted)
    }
}

fn normal_queue_scripts() -> Vec<ProtocolScript> {
    vec![
        single_pending_queue_script(),
        multi_pending_queue_script(),
        already_present_queue_script(),
    ]
}

fn reconciliation_scripts() -> Vec<ProtocolScript> {
    vec![
        empty_reconciliation_script(),
        historical_bounded_reconciliation_script(),
        repeated_poison_reconciliation_script(),
        concurrent_conflict_reconciliation_script(),
        partial_repair_reconciliation_script(),
        provider_mismatch_reconciliation_script(),
        model_mismatch_reconciliation_script(),
        malformed_vector_reconciliation_script(),
        remote_error_reconciliation_script(),
        timeout_reconciliation_script(),
    ]
}

fn lifecycle_catalog_profiles() -> Vec<CatalogProfile> {
    vec![
        CatalogProfile::Pending,
        CatalogProfile::StaleNonce,
        CatalogProfile::ForeignOwner,
        CatalogProfile::DuplicateCandidates,
        CatalogProfile::Mismatched(CatalogMismatch::Config),
        CatalogProfile::Mismatched(CatalogMismatch::TriggerType),
        CatalogProfile::Mismatched(CatalogMismatch::Provider),
        CatalogProfile::Mismatched(CatalogMismatch::Target),
        CatalogProfile::Mismatched(CatalogMismatch::Namespace),
    ]
}

fn queue_scripts(selection: ScenarioSelection) -> Vec<ProtocolScript> {
    match selection {
        ScenarioSelection::Normal => normal_queue_scripts(),
        ScenarioSelection::QueueRejections => queue_rejection_scripts(),
        ScenarioSelection::QueueRetry => queue_retry_scripts(),
        ScenarioSelection::RouterFailures => router_failure_scripts(),
        ScenarioSelection::Reconciliation => reconciliation_scripts(),
        ScenarioSelection::Lifecycle | ScenarioSelection::LifecycleStress => Vec::new(),
        ScenarioSelection::All => {
            let mut scripts = normal_queue_scripts();
            scripts.extend(queue_failure_scripts());
            scripts.extend(reconciliation_scripts());
            scripts
        }
    }
}

fn queue_failure_scripts() -> Vec<ProtocolScript> {
    let mut scripts = queue_rejection_scripts();
    scripts.extend(queue_retry_scripts());
    scripts.extend(router_failure_scripts());
    scripts
}

fn empty_reconciliation_script() -> ProtocolScript {
    ProtocolScript::reconciliation(
        scripted_invocation_id("00000000-0000-4000-8000-000000000091"),
        cron_call(),
        DirectExpectation::Success(reconciliation_outcome(0, 0, 0, 0)),
        WorkerSettings::SINGLE,
    )
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Database,
        expected_payload: list_missing_request(1),
        response: ScriptedResponse::Success(list_missing_response(&[])),
    })
}

fn historical_bounded_reconciliation_script() -> ProtocolScript {
    let first_page = [&FIRST_QUEUE_MEMORY, &CURRENT_MEMORY];
    ProtocolScript::reconciliation(
        scripted_invocation_id("00000000-0000-4000-8000-000000000092"),
        cron_call(),
        DirectExpectation::Success(reconciliation_outcome(2, 2, 0, 2)),
        WorkerSettings::MULTI,
    )
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Database,
        expected_payload: list_missing_request(2),
        response: ScriptedResponse::Success(list_missing_response(&first_page)),
    })
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Router,
        expected_payload: router_request(&first_page),
        response: ScriptedResponse::Success(router_response(&first_page)),
    })
    .expect_unordered(vec![
        insert_embedding_step(&FIRST_QUEUE_MEMORY),
        insert_embedding_step(&CURRENT_MEMORY),
    ])
}

fn repeated_poison_reconciliation_script() -> ProtocolScript {
    let poison = [&FIRST_QUEUE_MEMORY];
    ProtocolScript::reconciliation(
        scripted_invocation_id("00000000-0000-4000-8000-000000000093"),
        cron_call(),
        DirectExpectation::Error,
        WorkerSettings::SINGLE,
    )
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Database,
        expected_payload: list_missing_request(1),
        response: ScriptedResponse::Success(list_missing_response(&poison)),
    })
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Router,
        expected_payload: router_request(&poison),
        response: ScriptedResponse::RemoteError,
    })
    .then_reconciliation(
        scripted_invocation_id("00000000-0000-4000-8000-000000000094"),
        DirectExpectation::Error,
        vec![
            UpstreamExpectation::Ordered(UpstreamStep {
                function: UpstreamFunction::Database,
                expected_payload: list_missing_request(1),
                response: ScriptedResponse::Success(list_missing_response(&poison)),
            }),
            UpstreamExpectation::Ordered(UpstreamStep {
                function: UpstreamFunction::Router,
                expected_payload: router_request(&poison),
                response: ScriptedResponse::RemoteError,
            }),
        ],
    )
}

fn concurrent_conflict_reconciliation_script() -> ProtocolScript {
    let pending = [&FIRST_QUEUE_MEMORY];
    ProtocolScript::reconciliation(
        scripted_invocation_id("00000000-0000-4000-8000-000000000095"),
        cron_call(),
        DirectExpectation::Success(reconciliation_outcome(1, 1, 1, 0)),
        WorkerSettings::SINGLE,
    )
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Database,
        expected_payload: list_missing_request(1),
        response: ScriptedResponse::Success(list_missing_response(&pending)),
    })
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Router,
        expected_payload: router_request(&pending),
        response: ScriptedResponse::Success(router_response(&pending)),
    })
    .expect_ordered(insert_embedding_conflict_step(&FIRST_QUEUE_MEMORY))
}

fn partial_repair_reconciliation_script() -> ProtocolScript {
    let selected = [&FIRST_QUEUE_MEMORY, &SECOND_QUEUE_MEMORY];
    let remaining = [&SECOND_QUEUE_MEMORY];
    ProtocolScript::reconciliation(
        scripted_invocation_id("00000000-0000-4000-8000-000000000096"),
        cron_call(),
        DirectExpectation::Error,
        WorkerSettings::MULTI,
    )
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Database,
        expected_payload: list_missing_request(2),
        response: ScriptedResponse::Success(list_missing_response(&selected)),
    })
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Router,
        expected_payload: router_request(&selected),
        response: ScriptedResponse::Success(router_response(&selected)),
    })
    .expect_unordered(vec![
        insert_embedding_step(&FIRST_QUEUE_MEMORY),
        insert_embedding_error_step(&SECOND_QUEUE_MEMORY),
    ])
    .then_reconciliation(
        scripted_invocation_id("00000000-0000-4000-8000-000000000097"),
        DirectExpectation::Success(reconciliation_outcome(1, 1, 0, 1)),
        vec![
            UpstreamExpectation::Ordered(UpstreamStep {
                function: UpstreamFunction::Database,
                expected_payload: list_missing_request(2),
                response: ScriptedResponse::ListUncommittedMemories {
                    memories: selected.to_vec(),
                },
            }),
            UpstreamExpectation::Ordered(UpstreamStep {
                function: UpstreamFunction::Router,
                expected_payload: router_request(&remaining),
                response: ScriptedResponse::Success(router_response(&remaining)),
            }),
            UpstreamExpectation::Ordered(insert_embedding_step(&SECOND_QUEUE_MEMORY)),
        ],
    )
}

fn provider_mismatch_reconciliation_script() -> ProtocolScript {
    reconciliation_router_failure_script(
        "00000000-0000-4000-8000-000000000098",
        ScriptedResponse::Success(router_response_with_identity(
            &[&FIRST_QUEUE_MEMORY],
            "memory-embedding-unexpected-provider",
            MODEL,
        )),
    )
}

fn model_mismatch_reconciliation_script() -> ProtocolScript {
    reconciliation_router_failure_script(
        "00000000-0000-4000-8000-000000000099",
        ScriptedResponse::Success(router_response_with_identity(
            &[&FIRST_QUEUE_MEMORY],
            PROVIDER,
            "memory-embedding-unexpected-model",
        )),
    )
}

fn malformed_vector_reconciliation_script() -> ProtocolScript {
    reconciliation_router_failure_script(
        "00000000-0000-4000-8000-000000000100",
        ScriptedResponse::Malformed(json!({
            "provider": PROVIDER,
            "model": MODEL,
            "embeddings": [["not-a-vector-component"]],
        })),
    )
}

fn remote_error_reconciliation_script() -> ProtocolScript {
    reconciliation_router_failure_script(
        "00000000-0000-4000-8000-000000000101",
        ScriptedResponse::RemoteError,
    )
}

fn timeout_reconciliation_script() -> ProtocolScript {
    let pending = [&FIRST_QUEUE_MEMORY];
    ProtocolScript::reconciliation(
        scripted_invocation_id("00000000-0000-4000-8000-000000000102"),
        cron_call(),
        DirectExpectation::Error,
        WorkerSettings::SINGLE,
    )
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Database,
        expected_payload: list_missing_request(1),
        response: ScriptedResponse::Success(list_missing_response(&pending)),
    })
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Router,
        expected_payload: router_request(&pending),
        response: ScriptedResponse::AwaitLocalError {
            late_response: Box::new(ScriptedResponse::Success(router_response(&pending))),
        },
    })
    .with_post_late_barrier(
        DirectFunction::ReconcileEmbeddings,
        scripted_invocation_id("00000000-0000-4000-8000-000000000103"),
        malformed_cron_call(),
    )
}

fn reconciliation_router_failure_script(
    invocation_id: &str,
    response: ScriptedResponse,
) -> ProtocolScript {
    let pending = [&FIRST_QUEUE_MEMORY];
    ProtocolScript::reconciliation(
        scripted_invocation_id(invocation_id),
        cron_call(),
        DirectExpectation::Error,
        WorkerSettings::SINGLE,
    )
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Database,
        expected_payload: list_missing_request(1),
        response: ScriptedResponse::Success(list_missing_response(&pending)),
    })
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Router,
        expected_payload: router_request(&pending),
        response,
    })
}

fn queue_rejection_scripts() -> Vec<ProtocolScript> {
    vec![
        malformed_queue_script(),
        unrelated_queue_script(),
        truncated_queue_script(),
        oversized_queue_script(),
        input_too_large_queue_script(),
    ]
}

fn queue_retry_scripts() -> Vec<ProtocolScript> {
    vec![missing_queue_script(), partial_write_retry_queue_script()]
}

fn router_failure_scripts() -> Vec<ProtocolScript> {
    vec![
        provider_mismatch_queue_script(),
        model_mismatch_queue_script(),
        invalid_vector_queue_script(),
        remote_error_queue_script(),
        timeout_queue_script(),
    ]
}

fn malformed_queue_script() -> ProtocolScript {
    let mut event = queue_event(&[&FIRST_QUEUE_MEMORY]);
    event["unexpected"] = json!(true);
    ProtocolScript::queue(
        scripted_invocation_id("00000000-0000-4000-8000-000000000071"),
        event,
        DirectExpectation::Error,
        WorkerSettings::SINGLE,
    )
}

fn unrelated_queue_script() -> ProtocolScript {
    let mut event = queue_event(&[&FIRST_QUEUE_MEMORY]);
    event["db"] = json!("memory-embedding-foreign-database");
    ProtocolScript::queue(
        scripted_invocation_id("00000000-0000-4000-8000-000000000072"),
        event,
        DirectExpectation::Error,
        WorkerSettings::SINGLE,
    )
}

fn truncated_queue_script() -> ProtocolScript {
    let mut event = queue_event(&[&FIRST_QUEUE_MEMORY]);
    event["truncated"] = json!(true);
    ProtocolScript::queue(
        scripted_invocation_id("00000000-0000-4000-8000-000000000073"),
        event,
        DirectExpectation::Error,
        WorkerSettings::SINGLE,
    )
}

fn oversized_queue_script() -> ProtocolScript {
    let memories = [&FIRST_QUEUE_MEMORY, &SECOND_QUEUE_MEMORY];
    ProtocolScript::queue(
        scripted_invocation_id("00000000-0000-4000-8000-000000000074"),
        queue_event(&memories),
        DirectExpectation::Error,
        WorkerSettings::SINGLE,
    )
}

fn input_too_large_queue_script() -> ProtocolScript {
    let pending = [&FIRST_QUEUE_MEMORY];
    ProtocolScript::queue(
        scripted_invocation_id("00000000-0000-4000-8000-000000000075"),
        queue_event(&pending),
        DirectExpectation::Error,
        WorkerSettings::INPUT_TOO_LARGE,
    )
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Database,
        expected_payload: load_keys_request(&pending),
        response: ScriptedResponse::Success(load_keys_response(&[LoadedQueueWork::Pending(
            &FIRST_QUEUE_MEMORY,
        )])),
    })
}

fn missing_queue_script() -> ProtocolScript {
    let keys = [&FIRST_QUEUE_MEMORY, &SECOND_QUEUE_MEMORY];
    let pending = [&FIRST_QUEUE_MEMORY];
    ProtocolScript::queue(
        scripted_invocation_id("00000000-0000-4000-8000-000000000076"),
        queue_event(&keys),
        DirectExpectation::Error,
        WorkerSettings::MULTI,
    )
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Database,
        expected_payload: load_keys_request(&keys),
        response: ScriptedResponse::Success(load_keys_response(&[
            LoadedQueueWork::Pending(&FIRST_QUEUE_MEMORY),
            LoadedQueueWork::Missing(&SECOND_QUEUE_MEMORY),
        ])),
    })
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Router,
        expected_payload: router_request(&pending),
        response: ScriptedResponse::Success(router_response(&pending)),
    })
    .expect_ordered(insert_embedding_step(&FIRST_QUEUE_MEMORY))
}

fn partial_write_retry_queue_script() -> ProtocolScript {
    let keys = [&FIRST_QUEUE_MEMORY, &SECOND_QUEUE_MEMORY];
    let retry_pending = [&SECOND_QUEUE_MEMORY];
    ProtocolScript::queue(
        scripted_invocation_id("00000000-0000-4000-8000-000000000077"),
        queue_event(&keys),
        DirectExpectation::Error,
        WorkerSettings::MULTI,
    )
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Database,
        expected_payload: load_keys_request(&keys),
        response: ScriptedResponse::Success(load_keys_response(&[
            LoadedQueueWork::Pending(&FIRST_QUEUE_MEMORY),
            LoadedQueueWork::Pending(&SECOND_QUEUE_MEMORY),
        ])),
    })
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Router,
        expected_payload: router_request(&keys),
        response: ScriptedResponse::Success(router_response(&keys)),
    })
    .expect_unordered(vec![
        insert_embedding_step(&FIRST_QUEUE_MEMORY),
        insert_embedding_error_step(&SECOND_QUEUE_MEMORY),
    ])
    .then_queue(
        scripted_invocation_id("00000000-0000-4000-8000-000000000078"),
        queue_event(&keys),
        DirectExpectation::Success(queue_outcome(2, 1, 1, 1)),
        vec![
            UpstreamExpectation::Ordered(UpstreamStep {
                function: UpstreamFunction::Database,
                expected_payload: load_keys_request(&keys),
                response: ScriptedResponse::LoadCommittedKeys {
                    memories: keys.to_vec(),
                },
            }),
            UpstreamExpectation::Ordered(UpstreamStep {
                function: UpstreamFunction::Router,
                expected_payload: router_request(&retry_pending),
                response: ScriptedResponse::Success(router_response(&retry_pending)),
            }),
            UpstreamExpectation::Ordered(insert_embedding_step(&SECOND_QUEUE_MEMORY)),
        ],
    )
}

fn provider_mismatch_queue_script() -> ProtocolScript {
    router_failure_queue_script(
        "00000000-0000-4000-8000-000000000079",
        ScriptedResponse::Success(router_response_with_identity(
            &[&FIRST_QUEUE_MEMORY],
            "memory-embedding-unexpected-provider",
            MODEL,
        )),
    )
}

fn model_mismatch_queue_script() -> ProtocolScript {
    router_failure_queue_script(
        "00000000-0000-4000-8000-000000000080",
        ScriptedResponse::Success(router_response_with_identity(
            &[&FIRST_QUEUE_MEMORY],
            PROVIDER,
            "memory-embedding-unexpected-model",
        )),
    )
}

fn invalid_vector_queue_script() -> ProtocolScript {
    router_failure_queue_script(
        "00000000-0000-4000-8000-000000000081",
        ScriptedResponse::Success(json!({
            "provider": PROVIDER,
            "model": MODEL,
            "embeddings": [[0.0, 0.0]],
        })),
    )
}

fn remote_error_queue_script() -> ProtocolScript {
    router_failure_queue_script(
        "00000000-0000-4000-8000-000000000082",
        ScriptedResponse::RemoteError,
    )
}

fn timeout_queue_script() -> ProtocolScript {
    router_failure_queue_script(
        "00000000-0000-4000-8000-000000000083",
        ScriptedResponse::AwaitLocalError {
            late_response: Box::new(ScriptedResponse::Success(router_response(&[
                &FIRST_QUEUE_MEMORY,
            ]))),
        },
    )
    .with_post_late_barrier(
        DirectFunction::EmbedVersions,
        scripted_invocation_id("00000000-0000-4000-8000-000000000086"),
        json!({"unexpected": true}),
    )
}

fn lifecycle_deadline_script() -> ProtocolScript {
    timeout_queue_script().with_response_timing(ResponseTiming {
        minimum: Duration::from_millis(500),
        maximum: INVOCATION_TIMEOUT,
    })
}

fn router_failure_queue_script(invocation_id: &str, response: ScriptedResponse) -> ProtocolScript {
    let pending = [&FIRST_QUEUE_MEMORY];
    ProtocolScript::queue(
        scripted_invocation_id(invocation_id),
        queue_event(&pending),
        DirectExpectation::Error,
        WorkerSettings::SINGLE,
    )
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Database,
        expected_payload: load_keys_request(&pending),
        response: ScriptedResponse::Success(load_keys_response(&[LoadedQueueWork::Pending(
            &FIRST_QUEUE_MEMORY,
        )])),
    })
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Router,
        expected_payload: router_request(&pending),
        response,
    })
}

fn single_pending_queue_script() -> ProtocolScript {
    let pending = [&FIRST_QUEUE_MEMORY];
    ProtocolScript::queue(
        scripted_invocation_id("00000000-0000-4000-8000-000000000061"),
        queue_event(&pending),
        DirectExpectation::Success(queue_outcome(1, 1, 0, 1)),
        WorkerSettings::SINGLE,
    )
    .with_response_timing(ResponseTiming {
        minimum: NORMAL_FLOW_RESPONSE_DELAY,
        maximum: INVOCATION_TIMEOUT,
    })
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Database,
        expected_payload: load_keys_request(&pending),
        response: ScriptedResponse::Success(load_keys_response(&[LoadedQueueWork::Pending(
            &FIRST_QUEUE_MEMORY,
        )])),
    })
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Router,
        expected_payload: router_request(&pending),
        response: ScriptedResponse::Delayed {
            delay: NORMAL_FLOW_RESPONSE_DELAY,
            response: Box::new(ScriptedResponse::Success(router_response(&pending))),
        },
    })
    .expect_ordered(insert_embedding_step(&FIRST_QUEUE_MEMORY))
}

fn multi_pending_queue_script() -> ProtocolScript {
    let pending = [&FIRST_QUEUE_MEMORY, &SECOND_QUEUE_MEMORY];
    ProtocolScript::queue(
        scripted_invocation_id("00000000-0000-4000-8000-000000000062"),
        queue_event(&pending),
        DirectExpectation::Success(queue_outcome(2, 2, 0, 2)),
        WorkerSettings::MULTI,
    )
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Database,
        expected_payload: load_keys_request(&pending),
        response: ScriptedResponse::Success(load_keys_response(&[
            LoadedQueueWork::Pending(&FIRST_QUEUE_MEMORY),
            LoadedQueueWork::Pending(&SECOND_QUEUE_MEMORY),
        ])),
    })
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Router,
        expected_payload: router_request(&pending),
        response: ScriptedResponse::Success(router_response(&pending)),
    })
    .expect_unordered(vec![
        insert_embedding_step(&FIRST_QUEUE_MEMORY),
        insert_embedding_step(&SECOND_QUEUE_MEMORY),
    ])
}

fn already_present_queue_script() -> ProtocolScript {
    let keys = [&FIRST_QUEUE_MEMORY];
    ProtocolScript::queue(
        scripted_invocation_id("00000000-0000-4000-8000-000000000063"),
        queue_event(&keys),
        DirectExpectation::Success(queue_outcome(1, 0, 1, 0)),
        WorkerSettings::SINGLE,
    )
    .expect_ordered(UpstreamStep {
        function: UpstreamFunction::Database,
        expected_payload: load_keys_request(&keys),
        response: ScriptedResponse::Success(load_keys_response(&[
            LoadedQueueWork::AlreadyPresent(&FIRST_QUEUE_MEMORY),
        ])),
    })
}

fn scripted_invocation_id(value: &str) -> Uuid {
    Uuid::parse_str(value).expect("static direct invocation UUID is valid")
}

fn queue_outcome(selected: u32, generated: u32, already_present: u32, stored: u32) -> Value {
    embedding_outcome(selected, generated, already_present, stored)
}

fn reconciliation_outcome(
    selected: u32,
    generated: u32,
    already_present: u32,
    stored: u32,
) -> Value {
    embedding_outcome(selected, generated, already_present, stored)
}

fn embedding_outcome(selected: u32, generated: u32, already_present: u32, stored: u32) -> Value {
    json!({
        "selected": selected,
        "generated": generated,
        "already_present": already_present,
        "stored": stored,
    })
}

fn cron_call() -> Value {
    json!({
        "trigger": "cron",
        "job_id": RECONCILIATION_JOB_ID,
        "scheduled_time": "2026-09-23T00:00:00Z",
        "actual_time": "2026-09-23T00:00:01Z",
    })
}

fn malformed_cron_call() -> Value {
    json!({
        "trigger": "queue",
        "job_id": RECONCILIATION_JOB_ID,
        "scheduled_time": "2026-09-23T00:00:00Z",
        "actual_time": "2026-09-23T00:00:01Z",
    })
}

fn list_missing_request(limit: u32) -> Value {
    json!({
        "db": DATABASE,
        "sql": LIST_MISSING_SQL,
        "params": [limit.to_string()],
    })
}

fn list_missing_response(memories: &[&QueueMemory]) -> Value {
    json!({
        "affected_rows": memories.len(),
        "last_insert_id": null,
        "returned_rows": memories.iter().map(|memory| json!({
            "id": memory.id,
            "version": memory.version,
            "title": memory.title,
            "content": memory.content,
            "concepts": memory.concepts,
        })).collect::<Vec<_>>(),
    })
}

fn queue_event(memories: &[&QueueMemory]) -> Value {
    json!({
        "db": DATABASE,
        "table": "public.memories",
        "op": "insert",
        "affected_rows": memories.len(),
        "returning": memories.iter().map(|memory| json!({
            "id": memory.id,
            "version": memory.version,
        })).collect::<Vec<_>>(),
        "at": 1_758_547_200_000_i64,
    })
}

fn load_keys_request(memories: &[&QueueMemory]) -> Value {
    json!({
        "db": DATABASE,
        "sql": LOAD_KEYS_SQL,
        "params": [memories.iter().map(|memory| json!({
            "id": memory.id,
            "version": memory.version,
        })).collect::<Vec<_>>()],
    })
}

fn load_keys_response(work: &[LoadedQueueWork]) -> Value {
    json!({
        "affected_rows": work.len(),
        "last_insert_id": null,
        "returned_rows": work.iter().map(|work| {
            let memory = work.memory();
            let (title, content, concepts) = if work.is_pending() {
                (json!(memory.title), json!(memory.content), json!(memory.concepts))
            } else {
                (Value::Null, Value::Null, Value::Null)
            };
            json!({
                "id": memory.id,
                "version": memory.version,
                "title": title,
                "content": content,
                "concepts": concepts,
                "memory_present": !matches!(work, LoadedQueueWork::Missing(_)),
                "embedding_present": matches!(work, LoadedQueueWork::AlreadyPresent(_)),
            })
        }).collect::<Vec<_>>(),
    })
}

fn router_request(memories: &[&QueueMemory]) -> Value {
    json!({
        "input": memories.iter().map(|memory| canonical_embedding_input(memory)).collect::<Vec<_>>(),
        "provider": PROVIDER,
        "model": MODEL,
    })
}

fn router_response(memories: &[&QueueMemory]) -> Value {
    router_response_with_identity(memories, PROVIDER, MODEL)
}

fn router_response_with_identity(memories: &[&QueueMemory], provider: &str, model: &str) -> Value {
    json!({
        "provider": provider,
        "model": model,
        "embeddings": memories.iter().map(|memory| json!(memory.vector)).collect::<Vec<_>>(),
    })
}

fn insert_embedding_request(memory: &QueueMemory) -> Value {
    json!({
        "db": DATABASE,
        "sql": INSERT_EMBEDDING_SQL,
        "params": [memory.id, memory.version, memory.vector_text],
    })
}

fn inserted_embedding_response(memory: &QueueMemory) -> Value {
    json!({
        "affected_rows": 1,
        "last_insert_id": null,
        "returned_rows": [{
            "outcome": "inserted",
            "id": memory.id,
            "version": memory.version,
        }],
    })
}

fn conflict_embedding_response(memory: &QueueMemory) -> Value {
    json!({
        "affected_rows": 1,
        "last_insert_id": null,
        "returned_rows": [{
            "outcome": "conflict",
            "id": memory.id,
            "version": memory.version,
        }],
    })
}

fn insert_embedding_step(memory: &QueueMemory) -> UpstreamStep {
    UpstreamStep {
        function: UpstreamFunction::Database,
        expected_payload: insert_embedding_request(memory),
        response: ScriptedResponse::Success(inserted_embedding_response(memory)),
    }
}

fn insert_embedding_conflict_step(memory: &QueueMemory) -> UpstreamStep {
    UpstreamStep {
        function: UpstreamFunction::Database,
        expected_payload: insert_embedding_request(memory),
        response: ScriptedResponse::Success(conflict_embedding_response(memory)),
    }
}

fn insert_embedding_error_step(memory: &QueueMemory) -> UpstreamStep {
    let mut step = insert_embedding_step(memory);
    step.response = ScriptedResponse::RemoteError;
    step
}

fn exact_successful_insert_memory(step: &UpstreamStep) -> Option<&'static QueueMemory> {
    let ScriptedResponse::Success(response) = &step.response else {
        return None;
    };
    [&FIRST_QUEUE_MEMORY, &SECOND_QUEUE_MEMORY, &CURRENT_MEMORY]
        .into_iter()
        .find(|&memory| {
            step.function == UpstreamFunction::Database
                && step.expected_payload == insert_embedding_request(memory)
                && response == &inserted_embedding_response(memory)
        })
}

fn canonical_embedding_input(memory: &QueueMemory) -> String {
    let title = serde_json::to_string(memory.title)
        .expect("static queue fixture title must serialize as JSON");
    let content = serde_json::to_string(memory.content)
        .expect("static queue fixture content must serialize as JSON");
    let concepts = serde_json::to_string(memory.concepts)
        .expect("static queue fixture concepts must serialize as JSON");
    format!(
        "{{\"format\":\"total-recall.memory-embedding.v1\",\"title\":{title},\"content\":{content},\"concepts\":{concepts}}}",
    )
}

fn sdk_function_schema<T: JsonSchema>() -> SmokeResult<Value> {
    serde_json::to_value(
        SchemaSettings::draft07()
            .into_generator()
            .into_root_schema_for::<T>(),
    )
    .map_err(|_| FakeError("failed to derive function schema"))
}

fn registration_nonce(message: &Value) -> SmokeResult<String> {
    let metadata = message
        .get("metadata")
        .and_then(Value::as_object)
        .ok_or(FakeError("registration metadata was malformed"))?;
    if metadata.len() != 1 {
        return failure("registration metadata did not contain one nonce");
    }
    let nonce = metadata
        .get("registration_nonce")
        .and_then(Value::as_str)
        .ok_or(FakeError("registration metadata did not contain a nonce"))?;
    let parsed = Uuid::parse_str(nonce).map_err(|_| FakeError("registration nonce was invalid"))?;
    if parsed.get_version_num() != 4 {
        return failure("registration nonce was not version four");
    }
    Ok(nonce.to_owned())
}

fn invoke_object<'a>(
    message: &'a Value,
    context: &'static str,
) -> SmokeResult<&'a Map<String, Value>> {
    let object = allowed_object(
        message,
        &[
            "type",
            "invocation_id",
            "function_id",
            "data",
            "action",
            "metadata",
            "namespace",
            "traceparent",
            "baggage",
        ],
        context,
    )?;
    for field in ["type", "invocation_id", "function_id", "data", "namespace"] {
        if !object.contains_key(field) {
            return failure("protocol invocation omitted a required field");
        }
    }
    if object.get("type") != Some(&json!("invokefunction")) {
        return failure("protocol invocation had the wrong type");
    }
    Ok(object)
}

fn exact_object<'a>(
    value: &'a Value,
    expected_fields: &[&str],
    _context: &'static str,
) -> SmokeResult<&'a Map<String, Value>> {
    let object = value
        .as_object()
        .ok_or(FakeError("protocol message was not an object"))?;
    if object.len() != expected_fields.len()
        || expected_fields
            .iter()
            .any(|field| !object.contains_key(*field))
    {
        return failure("protocol message had an unexpected field shape");
    }
    Ok(object)
}

fn allowed_object<'a>(
    value: &'a Value,
    allowed_fields: &[&str],
    _context: &'static str,
) -> SmokeResult<&'a Map<String, Value>> {
    let object = value
        .as_object()
        .ok_or(FakeError("protocol message was not an object"))?;
    if object
        .keys()
        .any(|field| !allowed_fields.contains(&field.as_str()))
    {
        return failure("protocol message included an unsupported field");
    }
    Ok(object)
}

fn message_type(message: &Value) -> SmokeResult<&str> {
    message
        .get("type")
        .and_then(Value::as_str)
        .ok_or(FakeError("protocol message did not include a type"))
}

fn required_string<'a>(
    message: &'a Value,
    field: &'static str,
    _context: &'static str,
) -> SmokeResult<&'a str> {
    message
        .get(field)
        .and_then(Value::as_str)
        .ok_or(FakeError("protocol message omitted a required string"))
}

fn required_uuid(
    message: &Value,
    field: &'static str,
    context: &'static str,
) -> SmokeResult<String> {
    let value = required_string(message, field, context)?;
    Uuid::parse_str(value).map_err(|_| FakeError("protocol message contained an invalid UUID"))?;
    Ok(value.to_owned())
}

fn parse_protocol_message(text: &str) -> SmokeResult<Value> {
    serde_json::from_str(text).map_err(|_| FakeError("release worker sent malformed JSON"))
}

fn protected_sentinel_present(bytes: &[u8]) -> bool {
    [
        DATABASE,
        QUEUE_TOPIC,
        RECONCILIATION_JOB_ID,
        PROVIDER,
        MODEL,
        MEMORY_ID,
        MEMORY_TITLE,
        MEMORY_CONTENT,
        MEMORY_CONCEPT,
        SECOND_MEMORY_ID,
        SECOND_MEMORY_TITLE,
        SECOND_MEMORY_CONTENT,
        SECOND_MEMORY_CONCEPT_FIRST,
        SECOND_MEMORY_CONCEPT_SECOND,
        CURRENT_MEMORY.title,
        CURRENT_MEMORY.content,
        CURRENT_MEMORY.concepts[0],
        REMOTE_CODE,
        REMOTE_MESSAGE,
        REMOTE_STACKTRACE,
        "[1.0,-0.5]",
        "[3.25,4.5]",
        "[7.0,-1.25]",
    ]
    .iter()
    .any(|sentinel| {
        bytes
            .windows(sentinel.len())
            .any(|window| window == sentinel.as_bytes())
    })
}

fn parse_arguments<I>(arguments: I) -> SmokeResult<Arguments>
where
    I: IntoIterator<Item = String>,
{
    let mut arguments = arguments.into_iter();
    let _program = arguments.next();
    let mut manifest = None;
    let mut operation_timeout = DEFAULT_OPERATION_TIMEOUT;
    let mut scenario = ScenarioSelection::All;

    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--manifest" => {
                manifest = Some(PathBuf::from(
                    arguments
                        .next()
                        .ok_or(FakeError("--manifest requires a path"))?,
                ));
            }
            "--timeout-seconds" => {
                let seconds = arguments
                    .next()
                    .ok_or(FakeError("--timeout-seconds requires a value"))?
                    .parse::<u64>()
                    .map_err(|_| FakeError("--timeout-seconds must be an integer"))?;
                if seconds == 0 {
                    return failure("--timeout-seconds must be greater than zero");
                }
                operation_timeout = Duration::from_secs(seconds);
            }
            "--scenario" => {
                scenario = match arguments
                    .next()
                    .ok_or(FakeError("--scenario requires a value"))?
                    .as_str()
                {
                    "all" => ScenarioSelection::All,
                    "normal" => ScenarioSelection::Normal,
                    "queue-rejections" => ScenarioSelection::QueueRejections,
                    "queue-retry" => ScenarioSelection::QueueRetry,
                    "router-failures" => ScenarioSelection::RouterFailures,
                    "reconciliation" => ScenarioSelection::Reconciliation,
                    "lifecycle" => ScenarioSelection::Lifecycle,
                    "lifecycle-stress" => ScenarioSelection::LifecycleStress,
                    _ => {
                        return failure(
                            "--scenario must be all, normal, queue-rejections, queue-retry, router-failures, reconciliation, lifecycle, or lifecycle-stress",
                        );
                    }
                };
            }
            "--help" | "-h" => return failure(usage()),
            _ => return failure("unknown command line argument"),
        }
    }

    Ok(Arguments {
        manifest: manifest.ok_or(FakeError("--manifest is required"))?,
        timeout: operation_timeout,
        scenario,
    })
}

const fn usage() -> &'static str {
    "Usage: memory-embedding-engine-fake --manifest <path> [--timeout-seconds <seconds>] [--scenario <all|normal|queue-rejections|queue-retry|router-failures|reconciliation|lifecycle|lifecycle-stress>]"
}

fn failure<T>(message: &'static str) -> SmokeResult<T> {
    Err(FakeError(message))
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        future::pending,
        net::SocketAddr,
        path::{Path, PathBuf},
        sync::{Arc, Mutex, atomic::AtomicU8},
        time::Duration,
    };

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use futures_util::{SinkExt, StreamExt};
    use serde_json::{Value, json};
    use tokio::{
        io::{AsyncRead, AsyncWrite, AsyncWriteExt, duplex},
        net::TcpListener,
        sync::mpsc,
        time::{Instant, timeout},
    };
    use tokio_tungstenite::{WebSocketStream, accept_async, connect_async, tungstenite::Message};
    use uuid::Uuid;

    use super::{
        Arguments, CRON_TRIGGER_TYPE, CatalogMismatch, CatalogProfile, CatalogScript,
        DEFAULT_NAMESPACE, DURABLE_SUBSCRIBER_TRIGGER_TYPE, DirectExpectation, DirectFunction,
        DirectInvocation, EMBED_VERSIONS_FUNCTION_ID, FIRST_QUEUE_MEMORY,
        FUNCTIONS_INFO_FUNCTION_ID, InvocationCounts, MEMORY_CONCEPT, MEMORY_CONTENT, MEMORY_ID,
        MEMORY_TITLE, NORMAL_FLOW_RESPONSE_DELAY, OUTPUT_CAPTURE_LIMIT, ProtocolScript,
        READINESS_CATALOG_COMPLETE, READINESS_LINE_LIMIT, READINESS_PENDING,
        RECONCILE_EMBEDDINGS_FUNCTION_ID, REGISTERED_TRIGGERS_INFO_FUNCTION_ID,
        REGISTERED_TRIGGERS_LIST_FUNCTION_ID, ReadinessDetector, RegistrationState,
        SECOND_MEMORY_ID, ScenarioSelection, ScriptedResponse, ShutdownState, SmokeResult,
        StreamCapture, UpstreamExpectation, UpstreamFunction, UpstreamStep, WorkerLaunch,
        WorkerSettings, await_rejected_delivery, canonical_embedding_input, capture_stderr,
        cron_config, drain_shutdown, drain_worker_shutdown, drive_catalog_failure, failure,
        lifecycle_catalog_profiles, multi_pending_queue_script, next_upstream_step,
        normal_queue_scripts, parse_arguments, parse_protocol_message, parse_worker_launch,
        protected_sentinel_present, queue_failure_scripts, run_protocol, scripted_invocation_id,
        sdk_function_schema, single_pending_queue_script, subscriber_config,
        validate_direct_result, worker_command,
    };

    const TEST_NONCE: &str = "63ec9af3-668e-4ea6-9bfe-a17b5f0c3c75";
    const TEST_SUBSCRIBER_TRIGGER_ID: &str = "8d4d6f3a-2a42-4e9a-9d37-4d0d4a6e0a11";
    const TEST_CRON_TRIGGER_ID: &str = "6fb41c68-e818-4f1c-9c87-967a9d32fd6a";
    const TEST_PROTOCOL_TIMEOUT: Duration = Duration::from_secs(3);

    #[derive(Clone, Copy)]
    enum ShutdownClose {
        Transport,
        AwaitServer,
        PingThenAwaitServer,
    }

    fn function_registration(function_id: &str, nonce: &str) -> Value {
        let request_format = match function_id {
            EMBED_VERSIONS_FUNCTION_ID => sdk_function_schema::<Value>().unwrap(),
            RECONCILE_EMBEDDINGS_FUNCTION_ID => {
                sdk_function_schema::<memory_embedding::runtime::ContentSafeCronCall>().unwrap()
            }
            _ => unreachable!(),
        };
        json!({
            "type": "registerfunction",
            "id": function_id,
            "request_format": request_format,
            "response_format": sdk_function_schema::<Value>().unwrap(),
            "metadata": {"registration_nonce": nonce},
        })
    }

    fn trigger_registration(
        trigger_id: &str,
        trigger_type: &str,
        function_id: &str,
        config: Value,
    ) -> Value {
        json!({
            "type": "registertrigger",
            "id": trigger_id,
            "trigger_type": trigger_type,
            "function_id": function_id,
            "config": config,
            "metadata": {"registration_nonce": TEST_NONCE},
            "namespace": DEFAULT_NAMESPACE,
            "trigger_namespace": DEFAULT_NAMESPACE,
        })
    }

    async fn send_test_message<Stream>(
        socket: &mut WebSocketStream<Stream>,
        message: Value,
    ) -> SmokeResult
    where
        Stream: AsyncRead + AsyncWrite + Unpin,
    {
        socket
            .send(Message::Text(message.to_string().into()))
            .await
            .map_err(|_| super::FakeError("test worker failed to send a protocol message"))
    }

    async fn receive_test_message<Stream>(
        socket: &mut WebSocketStream<Stream>,
    ) -> SmokeResult<Value>
    where
        Stream: AsyncRead + AsyncWrite + Unpin,
    {
        let frame = timeout(TEST_PROTOCOL_TIMEOUT, socket.next())
            .await
            .map_err(|_| super::FakeError("test worker did not receive a protocol message"))?
            .ok_or(super::FakeError(
                "test worker connection closed unexpectedly",
            ))?
            .map_err(|_| super::FakeError("test worker websocket failed"))?;
        let Message::Text(text) = frame else {
            return failure("test worker received an unsupported protocol frame");
        };
        parse_protocol_message(text.as_str())
    }

    async fn await_server_close<Stream>(socket: &mut WebSocketStream<Stream>) -> SmokeResult
    where
        Stream: AsyncRead + AsyncWrite + Unpin,
    {
        let frame = timeout(TEST_PROTOCOL_TIMEOUT, socket.next())
            .await
            .map_err(|_| super::FakeError("test worker did not receive the server close frame"))?
            .ok_or(super::FakeError(
                "test worker connection closed before the server close frame",
            ))?
            .map_err(|_| super::FakeError("test worker websocket failed during server close"))?;
        if !matches!(frame, Message::Close(_)) {
            return failure("test worker expected the fake server close frame");
        }
        socket
            .flush()
            .await
            .map_err(|_| super::FakeError("test worker failed to flush the server close reply"))
    }

    fn validate_test_result(
        message: &Value,
        invocation_id: &str,
        function_id: &str,
    ) -> SmokeResult {
        if message.get("type") != Some(&json!("invocationresult"))
            || message.get("invocation_id") != Some(&json!(invocation_id))
            || message.get("function_id") != Some(&json!(function_id))
        {
            return failure("test worker received an unexpected invocation result");
        }
        Ok(())
    }

    async fn request_catalog<Stream>(
        socket: &mut WebSocketStream<Stream>,
        function_id: &str,
        data: Value,
    ) -> SmokeResult
    where
        Stream: AsyncRead + AsyncWrite + Unpin,
    {
        let invocation_id = Uuid::new_v4().to_string();
        send_test_message(
            socket,
            json!({
                "type": "invokefunction",
                "invocation_id": invocation_id,
                "function_id": function_id,
                "data": data,
                "namespace": DEFAULT_NAMESPACE,
            }),
        )
        .await?;
        let response = receive_test_message(socket).await?;
        validate_test_result(&response, &invocation_id, function_id)
    }

    fn scripted_reply_matches(
        message: &Value,
        invocation_id: &str,
        step: &super::UpstreamStep,
        committed_keys: &super::CommittedKeyState,
        elapsed: Duration,
    ) -> SmokeResult {
        validate_test_result(message, invocation_id, step.function.function_id())?;

        let mut response = &step.response;
        let mut minimum_delay = Duration::ZERO;
        while let ScriptedResponse::Delayed {
            delay,
            response: next,
        } = response
        {
            minimum_delay += *delay;
            response = next;
        }
        if elapsed < minimum_delay {
            return failure("scripted response completed before its configured delay");
        }

        match response {
            ScriptedResponse::Success(expected) | ScriptedResponse::Malformed(expected) => {
                if message.get("result") != Some(expected)
                    || message.get("error").is_some_and(|value| !value.is_null())
                {
                    return failure("scripted response did not preserve its result shape");
                }
            }
            ScriptedResponse::RemoteError => {
                if !message.get("error").is_some_and(Value::is_object)
                    || message.get("result").is_some_and(|value| !value.is_null())
                {
                    return failure("scripted remote error did not preserve its error shape");
                }
            }
            ScriptedResponse::LoadCommittedKeys { memories } => {
                if message.get("result") != Some(&committed_keys.load_response(memories))
                    || message.get("error").is_some_and(|value| !value.is_null())
                {
                    return failure("scripted retry load did not reflect committed keys");
                }
            }
            ScriptedResponse::ListUncommittedMemories { memories } => {
                if message.get("result") != Some(&committed_keys.list_missing_response(memories))
                    || message.get("error").is_some_and(|value| !value.is_null())
                {
                    return failure("scripted reconciliation page did not reflect committed keys");
                }
            }
            ScriptedResponse::Delayed { .. } => unreachable!("delayed responses are unwrapped"),
            ScriptedResponse::AwaitLocalError { .. } => {
                return failure("await-local-error response arrived before the direct error");
            }
        }
        Ok(())
    }

    fn scripted_direct_result(invocation: &super::DirectInvocation) -> Value {
        match &invocation.expected {
            DirectExpectation::Success(result) => json!({
                "type": "invocationresult",
                "invocation_id": invocation.invocation_id,
                "function_id": invocation.function.function_id(),
                "result": result,
            }),
            DirectExpectation::Error => json!({
                "type": "invocationresult",
                "invocation_id": invocation.invocation_id,
                "function_id": invocation.function.function_id(),
                "error": {"code": "worker_error", "message": "worker_error"},
            }),
        }
    }

    fn scripted_steps(expectations: &[UpstreamExpectation]) -> Vec<super::UpstreamStep> {
        let mut steps = Vec::new();
        for expectation in expectations {
            match expectation {
                UpstreamExpectation::Ordered(step) => steps.push(step.clone()),
                UpstreamExpectation::Unordered(unordered) => {
                    steps.extend(unordered.iter().rev().cloned());
                }
            }
        }
        steps
    }

    fn upstream_invocation(step: &super::UpstreamStep) -> Value {
        json!({
            "type": "invokefunction",
            "invocation_id": "00000000-0000-4000-8000-000000000064",
            "function_id": step.function.function_id(),
            "data": &step.expected_payload,
            "namespace": DEFAULT_NAMESPACE,
        })
    }

    fn standard_shutdown_messages() -> Vec<Value> {
        vec![
            json!({"type": "unregisterfunction", "id": EMBED_VERSIONS_FUNCTION_ID}),
            json!({"type": "unregisterfunction", "id": RECONCILE_EMBEDDINGS_FUNCTION_ID}),
            json!({
                "type": "unregistertrigger",
                "id": TEST_SUBSCRIBER_TRIGGER_ID,
                "trigger_type": DURABLE_SUBSCRIBER_TRIGGER_TYPE,
            }),
            json!({
                "type": "unregistertrigger",
                "id": TEST_CRON_TRIGGER_ID,
                "trigger_type": CRON_TRIGGER_TYPE,
            }),
        ]
    }

    fn complete_test_registrations() -> RegistrationState {
        let mut registrations = RegistrationState::default();
        registrations
            .record_function(&function_registration(
                EMBED_VERSIONS_FUNCTION_ID,
                TEST_NONCE,
            ))
            .unwrap();
        registrations
            .record_function(&function_registration(
                RECONCILE_EMBEDDINGS_FUNCTION_ID,
                TEST_NONCE,
            ))
            .unwrap();
        registrations
            .record_trigger(&trigger_registration(
                TEST_SUBSCRIBER_TRIGGER_ID,
                DURABLE_SUBSCRIBER_TRIGGER_TYPE,
                EMBED_VERSIONS_FUNCTION_ID,
                subscriber_config(),
            ))
            .unwrap();
        registrations
            .record_trigger(&trigger_registration(
                TEST_CRON_TRIGGER_ID,
                CRON_TRIGGER_TYPE,
                RECONCILE_EMBEDDINGS_FUNCTION_ID,
                cron_config(),
            ))
            .unwrap();
        registrations
    }

    async fn begin_test_catalog_session<Stream>(socket: &mut WebSocketStream<Stream>) -> SmokeResult
    where
        Stream: AsyncRead + AsyncWrite + Unpin,
    {
        send_test_message(
            socket,
            json!({
                "type": "invokefunction",
                "invocation_id": null,
                "function_id": "engine::workers::register",
                "data": {"name": "memory-embedding-protocol-fake", "namespace": DEFAULT_NAMESPACE},
                "action": {"type": "void"},
            }),
        )
        .await?;
        let worker_registered = receive_test_message(socket).await?;
        if worker_registered.get("type") != Some(&json!("workerregistered")) {
            return failure("test worker did not receive worker registration confirmation");
        }

        for registration in [
            function_registration(EMBED_VERSIONS_FUNCTION_ID, TEST_NONCE),
            function_registration(RECONCILE_EMBEDDINGS_FUNCTION_ID, TEST_NONCE),
            trigger_registration(
                TEST_SUBSCRIBER_TRIGGER_ID,
                DURABLE_SUBSCRIBER_TRIGGER_TYPE,
                EMBED_VERSIONS_FUNCTION_ID,
                subscriber_config(),
            ),
            trigger_registration(
                TEST_CRON_TRIGGER_ID,
                CRON_TRIGGER_TYPE,
                RECONCILE_EMBEDDINGS_FUNCTION_ID,
                cron_config(),
            ),
        ] {
            send_test_message(socket, registration).await?;
        }
        Ok(())
    }

    async fn drive_loopback_worker(
        address: SocketAddr,
        script: &ProtocolScript,
        shutdown_messages: &[Value],
        shutdown_close: ShutdownClose,
        invoke_writer_after_late_reply: bool,
    ) -> SmokeResult {
        let (mut socket, _) = timeout(
            TEST_PROTOCOL_TIMEOUT,
            connect_async(format!("ws://{address}")),
        )
        .await
        .map_err(|_| super::FakeError("test worker did not connect to the loopback fake"))?
        .map_err(|_| super::FakeError("test worker websocket connection failed"))?;

        begin_test_catalog_session(&mut socket).await?;

        request_catalog(
            &mut socket,
            FUNCTIONS_INFO_FUNCTION_ID,
            json!({
                "function_ids": [
                    EMBED_VERSIONS_FUNCTION_ID,
                    RECONCILE_EMBEDDINGS_FUNCTION_ID,
                ],
                "namespace": DEFAULT_NAMESPACE,
            }),
        )
        .await?;
        request_catalog(
            &mut socket,
            REGISTERED_TRIGGERS_LIST_FUNCTION_ID,
            json!({
                "function_id": EMBED_VERSIONS_FUNCTION_ID,
                "trigger_type": DURABLE_SUBSCRIBER_TRIGGER_TYPE,
                "include_pending": true,
            }),
        )
        .await?;
        request_catalog(
            &mut socket,
            REGISTERED_TRIGGERS_INFO_FUNCTION_ID,
            json!({"id": TEST_SUBSCRIBER_TRIGGER_ID}),
        )
        .await?;
        request_catalog(
            &mut socket,
            REGISTERED_TRIGGERS_LIST_FUNCTION_ID,
            json!({
                "function_id": RECONCILE_EMBEDDINGS_FUNCTION_ID,
                "trigger_type": CRON_TRIGGER_TYPE,
                "include_pending": true,
            }),
        )
        .await?;
        request_catalog(
            &mut socket,
            REGISTERED_TRIGGERS_INFO_FUNCTION_ID,
            json!({"id": TEST_CRON_TRIGGER_ID}),
        )
        .await?;

        let mut committed_keys = super::CommittedKeyState::default();
        for delivery_index in 0..script.delivery_count() {
            let (invocation, expectations) = script
                .delivery(delivery_index)
                .ok_or(super::FakeError("test script delivery was missing"))?;
            let direct = receive_test_message(&mut socket).await?;
            if direct.get("type") != Some(&json!("invokefunction"))
                || direct.get("invocation_id") != Some(&json!(invocation.invocation_id))
                || direct.get("function_id") != Some(&json!(invocation.function.function_id()))
                || direct.get("data") != Some(&invocation.data)
            {
                return failure("test worker received an unexpected direct invocation");
            }

            let mut direct_result_sent = false;
            for step in scripted_steps(expectations) {
                let invocation_id = Uuid::new_v4().to_string();
                let started = Instant::now();
                send_test_message(
                    &mut socket,
                    json!({
                        "type": "invokefunction",
                        "invocation_id": invocation_id,
                        "function_id": step.function.function_id(),
                        "data": &step.expected_payload,
                        "namespace": DEFAULT_NAMESPACE,
                    }),
                )
                .await?;

                if let ScriptedResponse::AwaitLocalError { late_response } = &step.response {
                    if !matches!(&invocation.expected, DirectExpectation::Error) {
                        return failure("await-local-error scripts must expect a direct error");
                    }
                    send_test_message(&mut socket, scripted_direct_result(invocation)).await?;
                    direct_result_sent = true;
                    let response = receive_test_message(&mut socket).await?;
                    let late_step = UpstreamStep {
                        function: step.function,
                        expected_payload: step.expected_payload.clone(),
                        response: (**late_response).clone(),
                    };
                    scripted_reply_matches(
                        &response,
                        &invocation_id,
                        &late_step,
                        &committed_keys,
                        started.elapsed(),
                    )?;
                    committed_keys.record_successful_insert(&late_step);

                    let barrier = script.post_late.as_ref().ok_or(super::FakeError(
                        "await-local-error test scripts must define a post-late barrier",
                    ))?;
                    if invoke_writer_after_late_reply {
                        let writer = super::insert_embedding_step(&FIRST_QUEUE_MEMORY);
                        send_test_message(&mut socket, upstream_invocation(&writer)).await?;
                        return Ok(());
                    }
                    let barrier_delivery = receive_test_message(&mut socket).await?;
                    if barrier_delivery.get("type") != Some(&json!("invokefunction"))
                        || barrier_delivery.get("invocation_id")
                            != Some(&json!(barrier.invocation_id))
                        || barrier_delivery.get("function_id")
                            != Some(&json!(barrier.function.function_id()))
                        || barrier_delivery.get("data") != Some(&barrier.data)
                    {
                        return failure("test worker received an unexpected post-late barrier");
                    }
                    send_test_message(&mut socket, scripted_direct_result(barrier)).await?;
                } else {
                    let response = receive_test_message(&mut socket).await?;
                    scripted_reply_matches(
                        &response,
                        &invocation_id,
                        &step,
                        &committed_keys,
                        started.elapsed(),
                    )?;
                    committed_keys.record_successful_insert(&step);
                }
            }

            if !direct_result_sent {
                send_test_message(&mut socket, scripted_direct_result(invocation)).await?;
            }
        }
        for message in shutdown_messages {
            send_test_message(&mut socket, message.clone()).await?;
        }
        match shutdown_close {
            ShutdownClose::Transport => Ok(()),
            ShutdownClose::AwaitServer => await_server_close(&mut socket).await,
            ShutdownClose::PingThenAwaitServer => {
                socket
                    .send(Message::Ping(Vec::new().into()))
                    .await
                    .map_err(|_| {
                        super::FakeError("test worker failed to send an in-flight ping")
                    })?;
                await_server_close(&mut socket).await
            }
        }
    }

    async fn run_loopback_script(
        script: ProtocolScript,
        shutdown_messages: Vec<Value>,
    ) -> SmokeResult {
        run_loopback_script_with_close(script, shutdown_messages, ShutdownClose::AwaitServer).await
    }

    async fn run_loopback_script_with_close(
        script: ProtocolScript,
        shutdown_messages: Vec<Value>,
        shutdown_close: ShutdownClose,
    ) -> SmokeResult {
        run_loopback_script_with_options(script, shutdown_messages, shutdown_close, false).await
    }

    async fn run_loopback_script_with_late_writer(script: ProtocolScript) -> SmokeResult {
        run_loopback_script_with_options(script, Vec::new(), ShutdownClose::AwaitServer, true).await
    }

    async fn run_loopback_script_with_options(
        script: ProtocolScript,
        shutdown_messages: Vec<Value>,
        shutdown_close: ShutdownClose,
        invoke_writer_after_late_reply: bool,
    ) -> SmokeResult {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .map_err(|_| super::FakeError("test loopback listener did not bind"))?;
        let address = listener
            .local_addr()
            .map_err(|_| super::FakeError("test loopback listener did not expose an address"))?;
        let deadline = Instant::now() + TEST_PROTOCOL_TIMEOUT;
        let readiness_phase = Arc::new(AtomicU8::new(READINESS_PENDING));
        let (readiness_sender, readiness_receiver) = mpsc::unbounded_channel();
        readiness_sender
            .send(super::ReadinessReport {
                phase: READINESS_CATALOG_COMPLETE,
            })
            .map_err(|_| super::FakeError("test loopback readiness channel closed"))?;
        drop(readiness_sender);
        let server_script = script.clone();
        let server_phase = Arc::clone(&readiness_phase);
        let server = tokio::spawn(async move {
            let (stream, _) = timeout(TEST_PROTOCOL_TIMEOUT, listener.accept())
                .await
                .map_err(|_| super::FakeError("test loopback server did not accept a client"))?
                .map_err(|_| super::FakeError("test loopback server accept failed"))?;
            let mut socket = timeout(TEST_PROTOCOL_TIMEOUT, accept_async(stream))
                .await
                .map_err(|_| super::FakeError("test loopback handshake timed out"))?
                .map_err(|_| super::FakeError("test loopback handshake failed"))?;
            let protocol_run = run_protocol(
                &mut socket,
                readiness_receiver,
                server_phase,
                &server_script,
                deadline,
            )
            .await?;
            server_script.verify_invocation_counts(protocol_run.invocation_counts)?;
            drain_shutdown(&mut socket, &protocol_run.registrations, deadline).await
        });

        let client_result = drive_loopback_worker(
            address,
            &script,
            &shutdown_messages,
            shutdown_close,
            invoke_writer_after_late_reply,
        )
        .await;
        if let Err(error) = client_result {
            server.abort();
            let _ = server.await;
            return Err(error);
        }
        server
            .await
            .map_err(|_| super::FakeError("test loopback protocol task failed"))?
    }

    async fn run_scripted_response(script: ProtocolScript) -> SmokeResult {
        run_loopback_script(script, standard_shutdown_messages()).await
    }

    fn queue_script_with_response(
        step_index: usize,
        response: ScriptedResponse,
        expected: DirectExpectation,
    ) -> ProtocolScript {
        let mut script = single_pending_queue_script();
        script.expectations.truncate(step_index + 1);
        let UpstreamExpectation::Ordered(step) = &mut script.expectations[step_index] else {
            unreachable!("single pending queue scripts use ordered expectations")
        };
        step.response = response;
        script.invocation.expected = expected;
        script.invocation.response_timing = None;
        script
    }

    fn delayed_queue_script(step_index: usize) -> ProtocolScript {
        let mut script = single_pending_queue_script();
        script.expectations.truncate(step_index + 1);
        let UpstreamExpectation::Ordered(step) = &mut script.expectations[step_index] else {
            unreachable!("single pending queue scripts use ordered expectations")
        };
        let response = step.response.clone();
        step.response = ScriptedResponse::Delayed {
            delay: Duration::from_millis(5),
            response: Box::new(response),
        };
        script.invocation.response_timing = Some(super::ResponseTiming {
            minimum: Duration::from_millis(5),
            maximum: super::INVOCATION_TIMEOUT,
        });
        script
    }

    #[test]
    fn arguments_require_a_manifest_path() {
        assert!(parse_arguments(["memory-embedding-engine-fake".to_owned()]).is_err());
    }

    #[test]
    fn lifecycle_catalog_profiles_cover_every_readiness_rejection() {
        assert_eq!(
            lifecycle_catalog_profiles(),
            vec![
                CatalogProfile::Pending,
                CatalogProfile::StaleNonce,
                CatalogProfile::ForeignOwner,
                CatalogProfile::DuplicateCandidates,
                CatalogProfile::Mismatched(CatalogMismatch::Config),
                CatalogProfile::Mismatched(CatalogMismatch::TriggerType),
                CatalogProfile::Mismatched(CatalogMismatch::Provider),
                CatalogProfile::Mismatched(CatalogMismatch::Target),
                CatalogProfile::Mismatched(CatalogMismatch::Namespace),
            ]
        );
    }

    #[test]
    fn pending_catalog_profile_returns_every_expected_function_as_pending() {
        let registrations = complete_test_registrations();
        let (response, _) = CatalogScript::new(CatalogProfile::Pending)
            .function_response(&registrations)
            .unwrap();

        assert_eq!(
            response,
            json!({
                "functions": [
                    {
                        "function_id": EMBED_VERSIONS_FUNCTION_ID,
                        "error": "pending",
                    },
                    {
                        "function_id": RECONCILE_EMBEDDINGS_FUNCTION_ID,
                        "error": "pending",
                    },
                ],
            })
        );
    }

    #[test]
    fn arguments_accept_a_bounded_timeout() {
        let arguments = parse_arguments([
            "memory-embedding-engine-fake".to_owned(),
            "--manifest".to_owned(),
            "/tmp/memory-embedding/iii.worker.yaml".to_owned(),
            "--timeout-seconds".to_owned(),
            "12".to_owned(),
        ])
        .unwrap();

        assert_eq!(
            arguments,
            Arguments {
                manifest: PathBuf::from("/tmp/memory-embedding/iii.worker.yaml"),
                timeout: Duration::from_secs(12),
                scenario: ScenarioSelection::All,
            }
        );
    }

    #[test]
    fn arguments_select_bounded_scenario_groups() {
        for (value, expected) in [
            ("all", ScenarioSelection::All),
            ("normal", ScenarioSelection::Normal),
            ("queue-rejections", ScenarioSelection::QueueRejections),
            ("queue-retry", ScenarioSelection::QueueRetry),
            ("router-failures", ScenarioSelection::RouterFailures),
            ("reconciliation", ScenarioSelection::Reconciliation),
            ("lifecycle", ScenarioSelection::Lifecycle),
            ("lifecycle-stress", ScenarioSelection::LifecycleStress),
        ] {
            let arguments = parse_arguments([
                "memory-embedding-engine-fake".to_owned(),
                "--manifest".to_owned(),
                "/tmp/memory-embedding/iii.worker.yaml".to_owned(),
                "--scenario".to_owned(),
                value.to_owned(),
            ])
            .unwrap();
            assert_eq!(arguments.scenario, expected);
        }
        assert!(
            parse_arguments([
                "memory-embedding-engine-fake".to_owned(),
                "--manifest".to_owned(),
                "/tmp/memory-embedding/iii.worker.yaml".to_owned(),
                "--scenario".to_owned(),
                "unknown".to_owned(),
            ])
            .is_err()
        );
    }

    #[test]
    fn manifest_start_command_is_resolved_from_its_directory() {
        let launch = parse_worker_launch(
            "name: memory-embedding\nscripts:\n  start: ../../target/release/memory-embedding\n",
            Path::new("/repo/workers/memory-embedding/iii.worker.yaml"),
        )
        .unwrap();

        assert_eq!(
            launch,
            WorkerLaunch {
                working_directory: PathBuf::from("/repo/workers/memory-embedding"),
                executable: PathBuf::from(
                    "/repo/workers/memory-embedding/../../target/release/memory-embedding",
                ),
            }
        );
    }

    #[test]
    fn manifest_rejects_shell_syntax_and_arguments() {
        for start in [
            "../../target/release/memory-embedding --flag",
            "../../target/release/memory-embedding; other",
            "$(../../target/release/memory-embedding)",
            "sh -c memory-embedding",
        ] {
            assert!(
                parse_worker_launch(
                    &format!("scripts:\n  start: {start}\n"),
                    Path::new("/repo/workers/memory-embedding/iii.worker.yaml"),
                )
                .is_err()
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn worker_launch_clears_inherited_environment() {
        let directory =
            env::temp_dir().join(format!("memory-embedding-engine-fake-{}", Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        let executable = directory.join("check-environment");
        fs::write(
            &executable,
            "#!/bin/sh\nset -e\ntest -z \"${MEMORY_EMBEDDING_ENGINE_FAKE_HOSTILE+x}\"\ntest \"$TOTAL_RECALL_EMBEDDING_DATABASE\" = \"memory-embedding-fake-database\"\ntest \"$III_URL\" = \"ws://127.0.0.1:1\"\ntest \"$III_WORKER_NAME\" = \"memory-embedding-protocol-fake\"\ntest \"$III_NAMESPACE\" = \"default\"\n",
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        unsafe {
            env::set_var("MEMORY_EMBEDDING_ENGINE_FAKE_HOSTILE", "inherited");
        }

        let status = worker_command(
            &WorkerLaunch {
                working_directory: directory.clone(),
                executable: executable.clone(),
            },
            "ws://127.0.0.1:1".to_owned(),
            WorkerSettings::SINGLE,
        )
        .status()
        .await
        .unwrap();

        unsafe {
            env::remove_var("MEMORY_EMBEDDING_ENGINE_FAKE_HOSTILE");
        }
        fs::remove_file(executable).unwrap();
        fs::remove_dir(directory).unwrap();
        assert!(status.success());
    }

    #[test]
    fn normal_queue_scenarios_define_exact_requests_and_content_free_successes() {
        let scripts = normal_queue_scripts();
        assert_eq!(
            scripts.len(),
            3,
            "the release fake must run every normal queue scenario"
        );
        let [single, multi, already_present] = scripts.as_slice() else {
            panic!("normal queue scenarios must include single, multi, and already-present flows")
        };

        assert_eq!(single.invocation.function, DirectFunction::EmbedVersions);
        let timing = single
            .invocation
            .response_timing
            .expect("single pending flow must assert its delayed response timing");
        assert_eq!(timing.minimum, NORMAL_FLOW_RESPONSE_DELAY);
        assert_eq!(timing.maximum, super::INVOCATION_TIMEOUT);
        assert!(matches!(
            single.expectations.as_slice(),
            [
                UpstreamExpectation::Ordered(_),
                UpstreamExpectation::Ordered(_),
                UpstreamExpectation::Ordered(_),
            ]
        ));

        let UpstreamExpectation::Ordered(read) = &multi.expectations[0] else {
            panic!("multi-key flow must load all keys before routing")
        };
        assert_eq!(
            read.expected_payload["params"],
            json!([[
                {"id": MEMORY_ID, "version": "1"},
                {"id": SECOND_MEMORY_ID, "version": "2"},
            ]])
        );
        let UpstreamExpectation::Ordered(router) = &multi.expectations[1] else {
            panic!("multi-key flow must make one ordered router request")
        };
        assert_eq!(
            router.expected_payload["input"],
            json!([
                canonical_embedding_input(&FIRST_QUEUE_MEMORY),
                canonical_embedding_input(&super::SECOND_QUEUE_MEMORY),
            ])
        );
        let UpstreamExpectation::Unordered(writes) = &multi.expectations[2] else {
            panic!("multi-key writes must be modeled as an unordered exact set")
        };
        assert_eq!(writes.len(), 2);
        assert!(writes.iter().any(|step| {
            step.function == UpstreamFunction::Database
                && step.expected_payload["params"] == json!([MEMORY_ID, "1", "[1.0,-0.5]"])
        }));
        assert!(writes.iter().any(|step| {
            step.function == UpstreamFunction::Database
                && step.expected_payload["params"] == json!([SECOND_MEMORY_ID, "2", "[3.25,4.5]"])
        }));

        assert_eq!(already_present.expectations.len(), 1);
        assert!(matches!(
            already_present.expectations.as_slice(),
            [UpstreamExpectation::Ordered(step)] if step.function == UpstreamFunction::Database
        ));
        for script in scripts {
            let DirectExpectation::Success(result) = &script.invocation.expected else {
                panic!("normal queue scenarios must return a success result")
            };
            assert_eq!(result.as_object().map(|result| result.len()), Some(4));
            assert!(
                !protected_sentinel_present(result.to_string().as_bytes()),
                "normal queue outcomes must remain content-free"
            );
        }
    }

    #[test]
    fn reconciliation_scenarios_cover_the_bounded_stateless_repair_contract() {
        let scripts = super::reconciliation_scripts();

        assert_eq!(
            scripts.len(),
            10,
            "the release fake must run every reconciliation scenario"
        );
        assert_eq!(
            super::queue_scripts(ScenarioSelection::Reconciliation).len(),
            scripts.len(),
            "the reconciliation selector must run only its typed cron scenarios"
        );
        assert_eq!(
            super::queue_scripts(ScenarioSelection::All).len(),
            25,
            "the default selector must include queue and reconciliation scenarios"
        );
        let [
            empty,
            historical,
            poison,
            conflict,
            partial_repair,
            provider_mismatch,
            model_mismatch,
            malformed_vector,
            remote_error,
            timeout,
        ] = scripts.as_slice()
        else {
            panic!("reconciliation scenarios must retain their declared coverage order")
        };

        for script in &scripts {
            assert_eq!(
                script.invocation.function,
                DirectFunction::ReconcileEmbeddings
            );
            assert_eq!(
                script.invocation.response_timing,
                Some(super::ResponseTiming {
                    minimum: Duration::ZERO,
                    maximum: super::INVOCATION_TIMEOUT,
                }),
                "every cron result or error must return before the configured caller bound"
            );
            if let DirectExpectation::Success(result) = &script.invocation.expected {
                assert_eq!(result.as_object().map(|result| result.len()), Some(4));
                assert!(
                    !protected_sentinel_present(result.to_string().as_bytes()),
                    "reconciliation outcomes must remain content-free"
                );
            }
        }

        let DirectExpectation::Success(empty_outcome) = &empty.invocation.expected else {
            panic!("empty reconciliation must complete successfully")
        };
        assert_eq!(
            empty_outcome,
            &json!({
                "selected": 0,
                "generated": 0,
                "already_present": 0,
                "stored": 0,
            })
        );
        let [UpstreamExpectation::Ordered(empty_select)] = empty.expectations.as_slice() else {
            panic!("empty reconciliation must select once without routing or writing")
        };
        assert_eq!(
            empty_select.expected_payload,
            super::list_missing_request(1)
        );
        assert_eq!(
            empty.expected_invocation_counts(),
            InvocationCounts {
                direct_deliveries: 1,
                database_calls: 1,
                router_calls: 0,
            }
        );

        let [
            UpstreamExpectation::Ordered(historical_select),
            UpstreamExpectation::Ordered(historical_router),
            UpstreamExpectation::Unordered(historical_writes),
        ] = historical.expectations.as_slice()
        else {
            panic!("historical reconciliation must select, route once, and write an exact set")
        };
        assert_eq!(
            historical_select.expected_payload,
            super::list_missing_request(2),
            "the configured reconciliation bound must reach the missing-work request"
        );
        assert_eq!(
            historical_select.expected_payload["sql"],
            json!(super::LIST_MISSING_SQL),
            "the release fake must require the exact C-collated all-version query"
        );
        let ScriptedResponse::Success(historical_page) = &historical_select.response else {
            panic!("historical reconciliation must receive a bounded missing-work page")
        };
        assert_eq!(
            historical_page["returned_rows"].as_array().map(Vec::len),
            Some(2)
        );
        assert_eq!(historical_page["returned_rows"][0]["id"], json!(MEMORY_ID));
        assert_eq!(historical_page["returned_rows"][0]["version"], json!("1"));
        assert_eq!(historical_page["returned_rows"][1]["id"], json!(MEMORY_ID));
        assert_eq!(historical_page["returned_rows"][1]["version"], json!("3"));
        assert_eq!(
            historical_router.expected_payload["input"],
            json!([
                canonical_embedding_input(&FIRST_QUEUE_MEMORY),
                canonical_embedding_input(&super::CURRENT_MEMORY),
            ])
        );
        assert_eq!(historical_writes.len(), 2);
        assert!(historical_writes.iter().any(|step| {
            step.expected_payload == super::insert_embedding_request(&FIRST_QUEUE_MEMORY)
        }));
        assert!(historical_writes.iter().any(|step| {
            step.expected_payload == super::insert_embedding_request(&super::CURRENT_MEMORY)
        }));
        assert_eq!(
            historical.expected_invocation_counts(),
            InvocationCounts {
                direct_deliveries: 1,
                database_calls: 3,
                router_calls: 1,
            }
        );

        assert_eq!(poison.delivery_count(), 2);
        let poison_retry = poison
            .follow_up_deliveries
            .first()
            .expect("poison reconciliation must be retried only by an explicit cron delivery");
        assert_eq!(
            poison_retry.invocation.function,
            DirectFunction::ReconcileEmbeddings
        );
        assert_eq!(poison_retry.invocation.data, super::cron_call());
        assert_eq!(
            poison_retry.invocation.response_timing,
            Some(super::ResponseTiming {
                minimum: Duration::ZERO,
                maximum: super::INVOCATION_TIMEOUT,
            })
        );
        assert!(matches!(
            poison.invocation.expected,
            DirectExpectation::Error
        ));
        assert!(matches!(
            poison_retry.invocation.expected,
            DirectExpectation::Error
        ));
        assert_eq!(
            poison.expected_invocation_counts(),
            InvocationCounts {
                direct_deliveries: 2,
                database_calls: 2,
                router_calls: 2,
            },
            "a repeated poison page must not advance a cursor or create worker retries"
        );
        let UpstreamExpectation::Ordered(poison_select) = &poison.expectations[0] else {
            panic!("poison reconciliation must select its first page")
        };
        let UpstreamExpectation::Ordered(poison_retry_select) = &poison_retry.expectations[0]
        else {
            panic!("explicit poison retry must select its first page again")
        };
        assert_eq!(
            poison_select.expected_payload, poison_retry_select.expected_payload,
            "each explicit cron call must reselect the same deterministic page"
        );

        let DirectExpectation::Success(conflict_outcome) = &conflict.invocation.expected else {
            panic!("an immutable writer conflict must complete reconciliation")
        };
        assert_eq!(
            conflict_outcome,
            &json!({
                "selected": 1,
                "generated": 1,
                "already_present": 1,
                "stored": 0,
            })
        );
        let UpstreamExpectation::Ordered(conflict_write) = &conflict.expectations[2] else {
            panic!("concurrent presence must be represented by one immutable write")
        };
        assert_eq!(
            conflict_write.expected_payload,
            super::insert_embedding_request(&FIRST_QUEUE_MEMORY)
        );
        assert!(matches!(
            conflict_write.response,
            ScriptedResponse::Success(ref response)
                if response["returned_rows"][0]["outcome"] == json!("conflict")
        ));
        assert!(
            conflict_write.expected_payload["sql"]
                .as_str()
                .is_some_and(|sql| sql.contains("ON CONFLICT (id, version) DO NOTHING")),
            "the race must preserve the existing immutable embedding"
        );
        assert_eq!(
            conflict.expected_invocation_counts(),
            InvocationCounts {
                direct_deliveries: 1,
                database_calls: 2,
                router_calls: 1,
            }
        );

        assert_eq!(partial_repair.delivery_count(), 2);
        let partial_retry = partial_repair
            .follow_up_deliveries
            .first()
            .expect("partial reconciliation must use a later explicit cron invocation");
        assert_eq!(
            partial_retry.invocation.response_timing,
            Some(super::ResponseTiming {
                minimum: Duration::ZERO,
                maximum: super::INVOCATION_TIMEOUT,
            })
        );
        assert_eq!(
            partial_repair.expected_invocation_counts(),
            InvocationCounts {
                direct_deliveries: 2,
                database_calls: 5,
                router_calls: 2,
            }
        );
        let UpstreamExpectation::Ordered(repair_select) = &partial_retry.expectations[0] else {
            panic!("later reconciliation must select the remaining page")
        };
        let ScriptedResponse::ListUncommittedMemories { memories } = &repair_select.response else {
            panic!("later reconciliation must derive missing state from committed inserts")
        };
        let mut committed = super::CommittedKeyState::default();
        committed.record_successful_insert(&super::insert_embedding_step(&FIRST_QUEUE_MEMORY));
        committed.record_successful_insert(&super::insert_embedding_error_step(
            &super::SECOND_QUEUE_MEMORY,
        ));
        let repaired_page = committed.list_missing_response(memories);
        assert_eq!(
            repaired_page["returned_rows"].as_array().map(Vec::len),
            Some(1)
        );
        assert_eq!(
            repaired_page["returned_rows"][0]["id"],
            json!(SECOND_MEMORY_ID),
            "only the failed sibling may remain eligible for repair"
        );
        let UpstreamExpectation::Ordered(repair_router) = &partial_retry.expectations[1] else {
            panic!("later reconciliation must route only the remaining work")
        };
        assert_eq!(
            repair_router.expected_payload["input"],
            json!([canonical_embedding_input(&super::SECOND_QUEUE_MEMORY)])
        );

        for failed_router in [
            provider_mismatch,
            model_mismatch,
            malformed_vector,
            remote_error,
        ] {
            assert!(matches!(
                failed_router.invocation.expected,
                DirectExpectation::Error
            ));
            assert_eq!(failed_router.expectations.len(), 2);
            assert_eq!(
                failed_router.expected_invocation_counts(),
                InvocationCounts {
                    direct_deliveries: 1,
                    database_calls: 1,
                    router_calls: 1,
                },
                "router rejection must return an opaque error without a writer or retry"
            );
        }
        let UpstreamExpectation::Ordered(provider_router) = &provider_mismatch.expectations[1]
        else {
            panic!("provider mismatch must be a router response")
        };
        let ScriptedResponse::Success(provider_response) = &provider_router.response else {
            panic!("provider mismatch must return a response with the wrong provider")
        };
        assert_ne!(provider_response["provider"], json!(super::PROVIDER));
        let UpstreamExpectation::Ordered(model_router) = &model_mismatch.expectations[1] else {
            panic!("model mismatch must be a router response")
        };
        let ScriptedResponse::Success(model_response) = &model_router.response else {
            panic!("model mismatch must return a response with the wrong model")
        };
        assert_ne!(model_response["model"], json!(super::MODEL));
        assert!(matches!(
            malformed_vector.expectations[1],
            UpstreamExpectation::Ordered(UpstreamStep {
                response: ScriptedResponse::Malformed(_),
                ..
            })
        ));
        assert!(matches!(
            remote_error.expectations[1],
            UpstreamExpectation::Ordered(UpstreamStep {
                response: ScriptedResponse::RemoteError,
                ..
            })
        ));

        let timing = timeout
            .invocation
            .response_timing
            .expect("deadline reconciliation must remain below the caller bound");
        assert_eq!(timing.maximum, super::INVOCATION_TIMEOUT);
        let timeout_barrier = timeout
            .post_late
            .as_ref()
            .expect("deadline reconciliation must include a post-late barrier");
        assert_eq!(
            timeout_barrier.function,
            DirectFunction::ReconcileEmbeddings
        );
        assert_eq!(timeout_barrier.data, super::malformed_cron_call());
        assert!(matches!(timeout_barrier.expected, DirectExpectation::Error));
        assert_eq!(
            timeout_barrier.response_timing,
            Some(super::ResponseTiming {
                minimum: Duration::ZERO,
                maximum: super::INVOCATION_TIMEOUT,
            })
        );
        assert_eq!(
            timeout.expected_invocation_counts(),
            InvocationCounts {
                direct_deliveries: 2,
                database_calls: 1,
                router_calls: 1,
            },
            "a late router response must not re-enter the writer before the typed barrier"
        );
    }

    #[test]
    fn queue_failure_scenarios_define_rejections_retries_and_discarded_responses() {
        let scripts = queue_failure_scripts();
        assert_eq!(
            scripts.len(),
            12,
            "the release fake must cover every queue rejection and failure scenario"
        );
        let [
            malformed,
            unrelated,
            truncated,
            oversized,
            input_too_large,
            missing,
            partial_retry,
            provider_mismatch,
            model_mismatch,
            invalid_vector,
            remote_error,
            timeout,
        ] = scripts.as_slice()
        else {
            panic!("queue failure scenarios must retain their declared coverage order")
        };

        for rejected in [malformed, unrelated, truncated, oversized] {
            assert!(matches!(
                rejected.invocation.expected,
                DirectExpectation::Error
            ));
            assert!(
                rejected.expectations.is_empty(),
                "rejected row events must not call a database, router, or writer"
            );
            assert_eq!(rejected.delivery_count(), 1);
            assert_eq!(
                rejected.expected_invocation_counts(),
                InvocationCounts {
                    direct_deliveries: 1,
                    database_calls: 0,
                    router_calls: 0,
                }
            );
        }

        assert!(matches!(
            input_too_large.invocation.expected,
            DirectExpectation::Error
        ));
        assert_eq!(input_too_large.expectations.len(), 1);
        assert_eq!(
            input_too_large.expected_invocation_counts(),
            InvocationCounts {
                direct_deliveries: 1,
                database_calls: 1,
                router_calls: 0,
            },
            "an oversized canonical input must not reach the router"
        );

        assert!(matches!(
            missing.invocation.expected,
            DirectExpectation::Error
        ));
        assert_eq!(missing.expectations.len(), 3);
        assert_eq!(
            missing.expected_invocation_counts(),
            InvocationCounts {
                direct_deliveries: 1,
                database_calls: 2,
                router_calls: 1,
            }
        );

        assert!(matches!(
            partial_retry.invocation.expected,
            DirectExpectation::Error
        ));
        assert_eq!(partial_retry.delivery_count(), 2);
        assert_eq!(
            partial_retry.expected_invocation_counts(),
            InvocationCounts {
                direct_deliveries: 2,
                database_calls: 5,
                router_calls: 2,
            }
        );
        let retry = partial_retry
            .follow_up_deliveries
            .first()
            .expect("partial write flow must send one explicit retry delivery");
        assert!(matches!(
            retry.invocation.expected,
            DirectExpectation::Success(_)
        ));
        let [
            UpstreamExpectation::Ordered(retry_load),
            UpstreamExpectation::Ordered(retry_router),
            UpstreamExpectation::Ordered(retry_write),
        ] = retry.expectations.as_slice()
        else {
            panic!("retry must load both keys, route only the pending key, then write it")
        };
        let ScriptedResponse::LoadCommittedKeys { memories } = &retry_load.response else {
            panic!("retry exact load must derive state from committed writes")
        };
        let mut committed = super::CommittedKeyState::default();
        committed.record_successful_insert(&super::insert_embedding_step(&FIRST_QUEUE_MEMORY));
        let retry_load_response = committed.load_response(memories);
        assert_eq!(
            retry_load_response["returned_rows"][0]["embedding_present"],
            json!(true),
            "the committed sibling must be observable as already present"
        );
        assert_eq!(
            retry_load_response["returned_rows"][1]["embedding_present"],
            json!(false),
            "the failed sibling must remain pending for the explicit retry"
        );
        assert_eq!(
            retry_router.expected_payload["input"],
            json!([canonical_embedding_input(&super::SECOND_QUEUE_MEMORY)]),
            "only the remaining pending key may reach the router"
        );
        assert_eq!(
            retry_write.expected_payload["params"],
            json!([SECOND_MEMORY_ID, "2", "[3.25,4.5]"]),
            "only the remaining pending key may reach the writer"
        );

        for rejected_router_response in [
            provider_mismatch,
            model_mismatch,
            invalid_vector,
            remote_error,
        ] {
            assert!(matches!(
                rejected_router_response.invocation.expected,
                DirectExpectation::Error
            ));
            assert_eq!(
                rejected_router_response.expectations.len(),
                2,
                "discarded router responses must not reach the writer"
            );
            assert_eq!(
                rejected_router_response.expected_invocation_counts(),
                InvocationCounts {
                    direct_deliveries: 1,
                    database_calls: 1,
                    router_calls: 1,
                },
                "each failed router delivery must make one router call and no retry"
            );
        }
        assert!(matches!(
            remote_error.expectations[1],
            UpstreamExpectation::Ordered(UpstreamStep {
                response: ScriptedResponse::RemoteError,
                ..
            })
        ));
        assert!(matches!(
            timeout.expectations[1],
            UpstreamExpectation::Ordered(UpstreamStep {
                response: ScriptedResponse::AwaitLocalError { .. },
                ..
            })
        ));
        let barrier = timeout
            .post_late
            .as_ref()
            .expect("timeout delivery must include a post-late barrier");
        assert_eq!(barrier.function, DirectFunction::EmbedVersions);
        assert_eq!(barrier.data, json!({"unexpected": true}));
        assert!(matches!(barrier.expected, DirectExpectation::Error));
        assert_eq!(
            timeout.expected_invocation_counts(),
            InvocationCounts {
                direct_deliveries: 2,
                database_calls: 1,
                router_calls: 1,
            },
            "the post-late barrier must be a second direct delivery with no upstream calls"
        );
    }

    #[test]
    fn failure_scenario_privacy_checks_reject_sentinels_without_echoing_them() {
        let invocation = DirectInvocation {
            function: DirectFunction::EmbedVersions,
            invocation_id: scripted_invocation_id("00000000-0000-4000-8000-000000000084"),
            data: Value::Null,
            expected: DirectExpectation::Error,
            response_timing: None,
        };
        let direct_error = validate_direct_result(
            &json!({
                "type": "invocationresult",
                "invocation_id": invocation.invocation_id,
                "function_id": invocation.function.function_id(),
                "error": {"message": MEMORY_CONTENT},
            }),
            &invocation,
            Duration::ZERO,
        )
        .unwrap_err();
        assert!(!protected_sentinel_present(
            direct_error.to_string().as_bytes()
        ));

        let mut expectations = vec![UpstreamExpectation::Ordered(UpstreamStep {
            function: UpstreamFunction::Database,
            expected_payload: json!({"safe": true}),
            response: ScriptedResponse::Success(Value::Null),
        })];
        let mut upstream_index = 0;
        let Err(diagnostic) = next_upstream_step(
            &json!({
                "type": "invokefunction",
                "invocation_id": "00000000-0000-4000-8000-000000000085",
                "function_id": UpstreamFunction::Database.function_id(),
                "data": {"private": MEMORY_CONTENT},
                "namespace": DEFAULT_NAMESPACE,
            }),
            &mut expectations,
            &mut upstream_index,
        ) else {
            panic!("mismatched fake request diagnostic must fail")
        };
        assert!(!protected_sentinel_present(
            diagnostic.to_string().as_bytes()
        ));
    }

    #[test]
    fn unordered_write_expectations_reject_wrong_duplicate_and_missing_associations() {
        let script = multi_pending_queue_script();
        let UpstreamExpectation::Unordered(steps) = script.expectations[2].clone() else {
            panic!("multi-key flow must use an unordered write expectation")
        };
        let first = steps[0].clone();
        let second = steps[1].clone();
        let mut expectations = vec![UpstreamExpectation::Unordered(steps)];
        let mut upstream_index = 0;

        let mut wrong = upstream_invocation(&first);
        wrong["data"] = Value::Null;
        let Err(error) = next_upstream_step(&wrong, &mut expectations, &mut upstream_index) else {
            panic!("wrong immutable association must be rejected")
        };
        assert_eq!(
            error.to_string(),
            "release worker upstream request did not match the unordered script"
        );
        assert_eq!(upstream_index, 0);

        assert_eq!(
            next_upstream_step(
                &upstream_invocation(&first),
                &mut expectations,
                &mut upstream_index,
            )
            .unwrap()
            .expected_payload,
            first.expected_payload
        );
        assert_eq!(
            upstream_index, 0,
            "a missing immutable association must leave the unordered set incomplete"
        );
        let Err(error) = next_upstream_step(
            &upstream_invocation(&first),
            &mut expectations,
            &mut upstream_index,
        ) else {
            panic!("duplicate immutable association must be rejected")
        };
        assert_eq!(
            error.to_string(),
            "release worker upstream request did not match the unordered script"
        );

        assert_eq!(
            next_upstream_step(
                &upstream_invocation(&second),
                &mut expectations,
                &mut upstream_index,
            )
            .unwrap()
            .expected_payload,
            second.expected_payload
        );
        assert_eq!(upstream_index, 1);
    }

    #[tokio::test]
    async fn loopback_scripts_execute_normal_queue_responses() {
        for script in normal_queue_scripts() {
            run_scripted_response(script).await.unwrap();
        }
    }

    #[tokio::test]
    async fn loopback_scripts_execute_queue_failure_responses() {
        let scripts = queue_failure_scripts();
        assert_eq!(scripts.len(), 12);
        for script in scripts {
            run_scripted_response(script).await.unwrap();
        }
    }

    #[tokio::test]
    async fn loopback_scripts_execute_reconciliation_responses() {
        let scripts = super::reconciliation_scripts();
        assert_eq!(scripts.len(), 10);
        for script in scripts {
            run_scripted_response(script).await.unwrap();
        }
    }

    #[test]
    fn partial_retry_load_uses_only_exact_successful_insert_replies() {
        let script = super::partial_write_retry_queue_script();
        let retry = script
            .follow_up_deliveries
            .first()
            .expect("partial write script must include a retry");
        let UpstreamExpectation::Ordered(retry_load) = &retry.expectations[0] else {
            panic!("retry must begin with an exact load")
        };
        let ScriptedResponse::LoadCommittedKeys { memories } = &retry_load.response else {
            panic!("retry load must derive present keys from this script's committed state")
        };

        let mut committed = super::CommittedKeyState::default();
        committed
            .record_successful_insert(&super::insert_embedding_error_step(&FIRST_QUEUE_MEMORY));
        let after_failed_insert = committed.load_response(memories);
        assert_eq!(
            after_failed_insert["returned_rows"][0]["embedding_present"],
            json!(false),
            "a failed write must leave its key pending"
        );

        committed.record_successful_insert(&super::insert_embedding_step(&FIRST_QUEUE_MEMORY));
        let after_first_success = committed.load_response(memories);
        assert_eq!(
            after_first_success["returned_rows"][0]["embedding_present"],
            json!(true),
            "the first exact successful insert must become already present on retry"
        );
        assert_eq!(
            after_first_success["returned_rows"][1]["embedding_present"],
            json!(false),
            "the failed sibling must remain pending"
        );

        let mut non_insert_reply = super::insert_embedding_step(&super::SECOND_QUEUE_MEMORY);
        non_insert_reply.response = ScriptedResponse::Success(json!({
            "affected_rows": 1,
            "last_insert_id": null,
            "returned_rows": [{
                "outcome": "conflict",
                "id": SECOND_MEMORY_ID,
                "version": "2",
            }],
        }));
        committed.record_successful_insert(&non_insert_reply);
        assert_eq!(
            committed.load_response(memories)["returned_rows"][1]["embedding_present"],
            json!(false),
            "only the exact inserted reply may commit retry state"
        );
    }

    #[tokio::test]
    async fn loopback_timeout_waits_for_local_error_and_rejects_a_late_writer() {
        let error = run_loopback_script_with_late_writer(super::timeout_queue_script())
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "release worker made an invocation before completing the post-late barrier"
        );
    }

    #[tokio::test]
    async fn reconciliation_timeout_waits_for_local_error_and_rejects_a_late_writer() {
        let error = run_loopback_script_with_late_writer(super::timeout_reconciliation_script())
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "release worker made an invocation before completing the post-late barrier"
        );
    }

    #[tokio::test]
    async fn loopback_script_executes_remote_error_responses() {
        for step_index in [0, 1] {
            run_scripted_response(queue_script_with_response(
                step_index,
                ScriptedResponse::RemoteError,
                DirectExpectation::Error,
            ))
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn loopback_script_executes_malformed_responses() {
        for step_index in [0, 1] {
            run_scripted_response(queue_script_with_response(
                step_index,
                ScriptedResponse::Malformed(Value::Null),
                DirectExpectation::Error,
            ))
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn loopback_script_executes_delayed_responses() {
        for step_index in [0, 1] {
            run_scripted_response(delayed_queue_script(step_index))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn stderr_capture_rejects_oversized_unterminated_output() {
        let (mut writer, reader) = duplex(1024);
        let capture = Arc::new(Mutex::new(StreamCapture::default()));
        let readiness_phase = Arc::new(AtomicU8::new(READINESS_PENDING));
        let (readiness_sender, _readiness_receiver) = mpsc::unbounded_channel();
        let writer = tokio::spawn(async move {
            let _ = writer
                .write_all(&vec![b'x'; OUTPUT_CAPTURE_LIMIT + 1])
                .await;
            pending::<()>().await;
        });

        let result = timeout(
            TEST_PROTOCOL_TIMEOUT,
            capture_stderr(
                reader,
                Arc::clone(&capture),
                readiness_phase,
                readiness_sender,
            ),
        )
        .await
        .expect("oversized unterminated stderr should be rejected before EOF");
        writer.abort();
        let _ = writer.await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "worker stderr exceeded the bounded capture"
        );
        assert!(capture.lock().unwrap().bytes.len() <= OUTPUT_CAPTURE_LIMIT);
    }

    #[test]
    fn readiness_detection_is_chunked_and_line_bounded() {
        let mut readiness = ReadinessDetector::default();
        assert!(!readiness.observe(b"worker "));
        assert!(readiness.observe(b"ready\n"));
        assert!(!readiness.observe(&vec![b'x'; READINESS_LINE_LIMIT + 1]));
        assert!(!readiness.observe(b"\n"));
        assert!(readiness.observe(b"worker ready\n"));
    }

    #[tokio::test]
    async fn loopback_shutdown_rejects_duplicate_release() {
        let error = run_loopback_script(
            single_pending_queue_script(),
            vec![
                json!({"type": "unregisterfunction", "id": EMBED_VERSIONS_FUNCTION_ID}),
                json!({"type": "unregisterfunction", "id": EMBED_VERSIONS_FUNCTION_ID}),
            ],
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "release worker released a function more than once"
        );
    }

    #[tokio::test]
    async fn admitted_shutdown_rejects_duplicate_release_across_direct_result_and_final_drain() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let deadline = Instant::now() + TEST_PROTOCOL_TIMEOUT;
        let rejected = DirectInvocation {
            function: DirectFunction::EmbedVersions,
            invocation_id: scripted_invocation_id("00000000-0000-4000-8000-000000000114"),
            data: Value::Null,
            expected: DirectExpectation::Error,
            response_timing: None,
        };
        let client_rejected = rejected.clone();
        let client = tokio::spawn(async move {
            let (mut socket, _) = connect_async(format!("ws://{address}"))
                .await
                .map_err(|_| super::FakeError("test worker websocket connection failed"))?;
            let release = json!({
                "type": "unregisterfunction",
                "id": EMBED_VERSIONS_FUNCTION_ID,
            });
            send_test_message(&mut socket, release.clone()).await?;
            send_test_message(&mut socket, scripted_direct_result(&client_rejected)).await?;
            send_test_message(&mut socket, release).await
        });
        let (stream, _) = timeout(TEST_PROTOCOL_TIMEOUT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let registrations = complete_test_registrations();
        let mut releases = ShutdownState::default();

        await_rejected_delivery(
            &mut socket,
            &registrations,
            &mut releases,
            &rejected,
            Instant::now(),
            deadline,
        )
        .await
        .unwrap();
        let final_drain =
            drain_worker_shutdown(&mut socket, &registrations, &mut releases, deadline).await;
        client.await.unwrap().unwrap();

        assert_eq!(
            final_drain.unwrap_err().to_string(),
            "release worker released a function more than once"
        );
    }

    #[tokio::test]
    async fn pending_catalog_rejects_early_child_exit_after_multiple_polls() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let deadline = Instant::now() + TEST_PROTOCOL_TIMEOUT;
        let readiness_phase = Arc::new(AtomicU8::new(READINESS_PENDING));
        let (readiness_sender, mut readiness_receiver) = mpsc::unbounded_channel();
        drop(readiness_sender);
        let client = tokio::spawn(async move {
            let (mut socket, _) = connect_async(format!("ws://{address}"))
                .await
                .map_err(|_| super::FakeError("test worker websocket connection failed"))?;
            begin_test_catalog_session(&mut socket).await?;
            for _ in 0..2 {
                request_catalog(
                    &mut socket,
                    FUNCTIONS_INFO_FUNCTION_ID,
                    json!({
                        "function_ids": [
                            EMBED_VERSIONS_FUNCTION_ID,
                            RECONCILE_EMBEDDINGS_FUNCTION_ID,
                        ],
                        "namespace": DEFAULT_NAMESPACE,
                    }),
                )
                .await?;
            }
            Ok::<(), super::FakeError>(())
        });
        let (stream, _) = timeout(TEST_PROTOCOL_TIMEOUT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let result = drive_catalog_failure(
            &mut socket,
            &mut readiness_receiver,
            readiness_phase,
            CatalogScript::new(CatalogProfile::Pending),
            Instant::now(),
            deadline,
        )
        .await;
        client.await.unwrap().unwrap();

        assert_eq!(
            result.unwrap_err().to_string(),
            "release worker exited before the readiness timeout"
        );
    }

    #[tokio::test]
    async fn loopback_shutdown_accepts_clean_close_before_all_releases() {
        run_loopback_script(single_pending_queue_script(), Vec::new())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn loopback_shutdown_accepts_clean_transport_close_before_all_releases() {
        run_loopback_script_with_close(
            single_pending_queue_script(),
            Vec::new(),
            ShutdownClose::Transport,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn loopback_shutdown_is_driven_by_the_server_after_final_delivery() {
        run_loopback_script_with_close(
            single_pending_queue_script(),
            Vec::new(),
            ShutdownClose::AwaitServer,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn loopback_shutdown_accepts_an_in_flight_ping_after_server_close() {
        run_loopback_script_with_close(
            single_pending_queue_script(),
            Vec::new(),
            ShutdownClose::PingThenAwaitServer,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn loopback_shutdown_rejects_unexpected_release() {
        let error = run_loopback_script(
            single_pending_queue_script(),
            vec![json!({"type": "unregisterfunction", "id": "unexpected::function"})],
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "release worker released an unexpected function"
        );
    }

    #[tokio::test]
    async fn loopback_shutdown_rejects_malformed_release() {
        let error = run_loopback_script(
            single_pending_queue_script(),
            vec![json!({
                "type": "unregisterfunction",
                "id": EMBED_VERSIONS_FUNCTION_ID,
                "unexpected": true,
            })],
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "protocol message had an unexpected field shape"
        );
    }

    #[test]
    fn clean_terminal_accepts_only_approved_shutdown_errors() {
        use std::io;

        use tokio_tungstenite::tungstenite::{Error as TungsteniteError, error::ProtocolError};

        for error in [
            TungsteniteError::ConnectionClosed,
            TungsteniteError::Protocol(ProtocolError::ResetWithoutClosingHandshake),
        ] {
            assert!(super::terminal_error_is_accepted(
                super::ShutdownTerminal::Clean,
                &error
            ));
        }
        assert!(super::terminal_error_is_accepted(
            super::ShutdownTerminal::Clean,
            &TungsteniteError::Io(io::Error::from(io::ErrorKind::ConnectionReset))
        ));

        for error in [
            TungsteniteError::Io(io::Error::from(io::ErrorKind::BrokenPipe)),
            TungsteniteError::Io(io::Error::from(io::ErrorKind::ConnectionAborted)),
            TungsteniteError::Io(io::Error::from(io::ErrorKind::UnexpectedEof)),
            TungsteniteError::Io(io::Error::other("unapproved shutdown I/O error")),
            TungsteniteError::AlreadyClosed,
            TungsteniteError::Protocol(ProtocolError::ReceivedAfterClosing),
        ] {
            assert!(!super::terminal_error_is_accepted(
                super::ShutdownTerminal::Clean,
                &error
            ));
        }
    }

    #[test]
    fn failed_startup_and_forced_termination_preserve_connection_reset_acceptance() {
        use std::io;

        use tokio_tungstenite::tungstenite::Error as TungsteniteError;

        let reset = TungsteniteError::Io(io::Error::from(io::ErrorKind::ConnectionReset));
        assert!(super::terminal_error_is_accepted(
            super::ShutdownTerminal::Forced,
            &reset
        ));
        assert!(super::terminal_error_is_accepted(
            super::ShutdownTerminal::FailedStartup,
            &reset
        ));
        assert!(!super::terminal_error_is_accepted(
            super::ShutdownTerminal::Forced,
            &TungsteniteError::Io(io::Error::from(io::ErrorKind::BrokenPipe))
        ));
    }

    #[test]
    fn registrations_require_both_functions_both_triggers_and_one_nonce() {
        let nonce = "63ec9af3-668e-4ea6-9bfe-a17b5f0c3c75";
        let mut registrations = RegistrationState::default();
        registrations
            .record_function(&function_registration(EMBED_VERSIONS_FUNCTION_ID, nonce))
            .unwrap();
        registrations
            .record_function(&function_registration(
                RECONCILE_EMBEDDINGS_FUNCTION_ID,
                nonce,
            ))
            .unwrap();
        registrations
            .record_trigger(&json!({
                "type": "registertrigger",
                "id": "8d4d6f3a-2a42-4e9a-9d37-4d0d4a6e0a11",
                "trigger_type": DURABLE_SUBSCRIBER_TRIGGER_TYPE,
                "function_id": EMBED_VERSIONS_FUNCTION_ID,
                "config": super::subscriber_config(),
                "metadata": {"registration_nonce": nonce},
                "namespace": "default",
                "trigger_namespace": "default",
            }))
            .unwrap();
        registrations
            .record_trigger(&json!({
                "type": "registertrigger",
                "id": "6fb41c68-e818-4f1c-9c87-967a9d32fd6a",
                "trigger_type": CRON_TRIGGER_TYPE,
                "function_id": RECONCILE_EMBEDDINGS_FUNCTION_ID,
                "config": super::cron_config(),
                "metadata": {"registration_nonce": nonce},
                "namespace": "default",
                "trigger_namespace": "default",
            }))
            .unwrap();

        assert!(registrations.verify_complete().is_ok());
        assert_eq!(registrations.nonce().unwrap(), nonce);
    }

    #[test]
    fn registration_rejects_a_non_default_trigger_namespace() {
        let mut registrations = RegistrationState::default();
        let error = registrations
            .record_trigger(&json!({
                "type": "registertrigger",
                "id": "8d4d6f3a-2a42-4e9a-9d37-4d0d4a6e0a11",
                "trigger_type": DURABLE_SUBSCRIBER_TRIGGER_TYPE,
                "function_id": EMBED_VERSIONS_FUNCTION_ID,
                "config": super::subscriber_config(),
                "metadata": {"registration_nonce": "63ec9af3-668e-4ea6-9bfe-a17b5f0c3c75"},
                "namespace": "foreign",
                "trigger_namespace": "default",
            }))
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "release worker trigger namespaces did not match the runtime contract"
        );
    }

    #[test]
    fn protected_sentinels_are_detected_without_rendering_them() {
        for sentinel in [MEMORY_ID, MEMORY_TITLE, MEMORY_CONTENT, MEMORY_CONCEPT] {
            assert!(protected_sentinel_present(sentinel.as_bytes()));
        }
        assert!(!protected_sentinel_present(b"worker ready"));
    }

    #[test]
    fn stream_capture_rejects_oversized_and_protected_output() {
        let mut oversized = StreamCapture::default();
        assert_eq!(
            oversized
                .append(&vec![0; OUTPUT_CAPTURE_LIMIT + 1], "stderr")
                .unwrap_err()
                .to_string(),
            "worker stderr exceeded the bounded capture"
        );
        assert_eq!(
            oversized.validate("stderr").unwrap_err().to_string(),
            "worker stderr exceeded the bounded capture"
        );

        let mut protected = StreamCapture::default();
        protected
            .append(MEMORY_CONTENT.as_bytes(), "stdout")
            .unwrap();
        assert_eq!(
            protected.validate("stdout").unwrap_err().to_string(),
            "worker stdout exposed protected data"
        );
    }
}
