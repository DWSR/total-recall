use std::{
    ffi::OsString,
    fs, io,
    net::TcpListener,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
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
use tokio::time::{Instant, sleep, timeout_at};
use uuid::Uuid;

use crate::{
    ArtifactPaths, E2eError, ProcessError, ProcessGuard, ProcessIdentity, ProcessPhase, ProcessSpec,
};

pub const CAPTURE_FUNCTION_IDS: [&str; 3] = [
    "harness::session_start",
    "harness::observation",
    "harness::session_end",
];

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineHarness {
    launch: ProcessSpec,
}

impl EngineHarness {
    pub fn new(artifacts: &ArtifactPaths) -> Self {
        Self {
            launch: ProcessSpec::new(artifacts.iii.clone()),
        }
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
        let function_refs = register_capture_functions(&client, &identity, &capture);
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
    _function_refs: [FunctionRef; 3],
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
    use std::{ffi::OsString, path::PathBuf};

    use serde_json::json;
    use uuid::Uuid;

    use super::{
        CAPTURE_FUNCTION_IDS, CaptureSet, EngineIdentity, FunctionCatalogEntry, ProcessSpec,
        catalog_matches, configured_launch, engine_config, parse_catalog_response,
        registration_metadata,
    };

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
}
