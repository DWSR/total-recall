use std::{collections::HashSet, sync::Arc, time::Duration};

use iii_sdk::engine::EngineFunctions;
use iii_sdk::protocol::TriggerRequest;
use iii_sdk::runtime::{FunctionRef, IIIConnectionState};
use iii_sdk::{
    IIIClient, InitOptions, RegisterFunction, WorkerIdentityMode, runtime::WorkerMetadata,
};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::time::Instant;
use uuid::Uuid;

use crate::{
    config::Config,
    contracts::{ObservationInput, SessionEndInput, SessionStartInput},
    ingestion::IngestionService,
    publisher::IiiQueuePublisher,
};

pub const HARNESS_FUNCTION_IDS: [&str; 3] = [
    "harness::session_start",
    "harness::observation",
    "harness::session_end",
];

pub const REGISTRATION_NONCE_METADATA_KEY: &str = "registration_nonce";
pub const REGISTRATION_WORKER_NAME_METADATA_KEY: &str = "worker_name";
pub const REGISTRATION_NAMESPACE_METADATA_KEY: &str = "namespace";
pub const DEFAULT_NAMESPACE: &str = "default";
pub const WORKER_READINESS_TIMEOUT: Duration = Duration::from_secs(30);
const CATALOG_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FunctionRegistration {
    pub function_id: &'static str,
    pub metadata: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistrationPlan {
    pub expected_worker_name: String,
    pub expected_namespace: Option<String>,
    pub nonce: Uuid,
    pub registrations: [FunctionRegistration; 3],
}

impl RegistrationPlan {
    pub fn new(
        expected_worker_name: impl Into<String>,
        expected_namespace: Option<String>,
    ) -> Self {
        Self::with_nonce(expected_worker_name, expected_namespace, Uuid::new_v4())
    }

    pub fn with_nonce(
        expected_worker_name: impl Into<String>,
        expected_namespace: Option<String>,
        nonce: Uuid,
    ) -> Self {
        let expected_worker_name = expected_worker_name.into();
        let registrations = std::array::from_fn(|index| FunctionRegistration {
            function_id: HARNESS_FUNCTION_IDS[index],
            metadata: json!({
                REGISTRATION_NONCE_METADATA_KEY: nonce.to_string(),
                REGISTRATION_WORKER_NAME_METADATA_KEY: expected_worker_name,
                REGISTRATION_NAMESPACE_METADATA_KEY: expected_namespace,
            }),
        });

        Self {
            expected_worker_name,
            expected_namespace,
            nonce,
            registrations,
        }
    }

    pub fn expected_catalog_namespace(&self) -> &str {
        self.expected_namespace
            .as_deref()
            .unwrap_or(DEFAULT_NAMESPACE)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct FunctionCatalogEntry {
    pub function_id: String,
    pub namespace: String,
    pub worker_name: String,
    #[serde(default)]
    pub metadata: Option<Value>,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum WorkerStartupError {
    #[error("iii worker registration was rejected")]
    RegistrationRejected,
    #[error("iii client disconnected during worker startup")]
    Disconnected,
    #[error("registered function catalog does not match the current worker")]
    CatalogMismatch,
    #[error("iii function catalog request failed")]
    CatalogRequestFailed,
    #[error("worker startup exceeded its readiness deadline")]
    Timeout,
}

pub fn catalog_matches(plan: &RegistrationPlan, catalog: &[FunctionCatalogEntry]) -> bool {
    if catalog.len() != HARNESS_FUNCTION_IDS.len() {
        return false;
    }

    let ids = catalog
        .iter()
        .map(|entry| entry.function_id.as_str())
        .collect::<HashSet<_>>();
    if ids.len() != HARNESS_FUNCTION_IDS.len()
        || HARNESS_FUNCTION_IDS
            .iter()
            .any(|function_id| !ids.contains(function_id))
    {
        return false;
    }

    let expected_nonce = plan.nonce.to_string();
    catalog.iter().all(|entry| {
        entry.namespace == plan.expected_catalog_namespace()
            && entry.worker_name == plan.expected_worker_name
            && entry
                .metadata
                .as_ref()
                .and_then(Value::as_object)
                .and_then(|metadata| metadata.get(REGISTRATION_NONCE_METADATA_KEY))
                .and_then(Value::as_str)
                == Some(expected_nonce.as_str())
    })
}

pub struct WorkerRuntime {
    client: IIIClient,
    plan: RegistrationPlan,
    _function_refs: [FunctionRef; 3],
}

impl WorkerRuntime {
    pub fn start(config: &Config) -> Self {
        let worker_metadata = managed_worker_metadata(config);
        let expected_worker_name = worker_metadata.name.clone();
        let client = iii_sdk::register_worker(
            &config.iii.engine_url,
            InitOptions {
                metadata: Some(worker_metadata),
                headers: None,
                otel: None,
                namespace: None,
                identity: WorkerIdentityMode::Managed,
            },
        );
        let plan = RegistrationPlan::new(expected_worker_name, client.namespace());
        let publisher = IiiQueuePublisher::new(client.clone(), config.queue_topic.clone());
        let service = Arc::new(IngestionService::new(publisher));
        let function_refs = register_harness_functions(&client, &plan, service);

        Self {
            client,
            plan,
            _function_refs: function_refs,
        }
    }

    pub fn client(&self) -> &IIIClient {
        &self.client
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
        let deadline = Instant::now() + timeout;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(WorkerStartupError::Timeout);
        }

        tokio::time::timeout_at(deadline, self.client.wait_until_registered(remaining))
            .await
            .map_err(|_| WorkerStartupError::Timeout)?
            .map_err(|error| self.registration_error(error))?;

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(WorkerStartupError::Timeout);
            }
            self.ensure_connected()?;

            let query = self.client.trigger(TriggerRequest {
                function_id: EngineFunctions::INFO_FUNCTIONS.to_owned(),
                payload: json!({
                    "function_ids": HARNESS_FUNCTION_IDS,
                    "namespace": self.plan.expected_catalog_namespace(),
                }),
                action: None,
                timeout_ms: Some(timeout_millis(remaining)),
            });
            let response = tokio::time::timeout_at(deadline, query)
                .await
                .map_err(|_| WorkerStartupError::Timeout)?
                .map_err(|error| self.catalog_error(error))?;

            match parse_catalog_response(response)? {
                None => self.wait_for_next_catalog_poll(deadline).await?,
                Some(catalog) if catalog_matches(&self.plan, &catalog) => {
                    self.ensure_connected()?;
                    return Ok(());
                }
                Some(catalog) => {
                    if catalog.len() < HARNESS_FUNCTION_IDS.len() {
                        self.wait_for_next_catalog_poll(deadline).await?;
                    } else {
                        return Err(WorkerStartupError::CatalogMismatch);
                    }
                }
            }
        }
    }

    pub fn shutdown(&self) {
        self.client.shutdown();
    }

    fn ensure_connected(&self) -> Result<(), WorkerStartupError> {
        if self.client.fatal_error().is_some() {
            return Err(WorkerStartupError::RegistrationRejected);
        }

        match self.client.get_connection_state() {
            IIIConnectionState::Connected => Ok(()),
            IIIConnectionState::Connecting
            | IIIConnectionState::Disconnected
            | IIIConnectionState::Reconnecting
            | IIIConnectionState::Failed => Err(WorkerStartupError::Disconnected),
        }
    }

    async fn wait_for_next_catalog_poll(
        &self,
        deadline: Instant,
    ) -> Result<(), WorkerStartupError> {
        tokio::time::timeout_at(deadline, tokio::time::sleep(CATALOG_POLL_INTERVAL))
            .await
            .map(|_| ())
            .map_err(|_| WorkerStartupError::Timeout)
    }

    fn registration_error(&self, error: iii_sdk::Error) -> WorkerStartupError {
        if self.client.fatal_error().is_some()
            || matches!(error, iii_sdk::Error::RegistrationRejected { .. })
        {
            WorkerStartupError::RegistrationRejected
        } else if matches!(error, iii_sdk::Error::Timeout) {
            WorkerStartupError::Timeout
        } else {
            WorkerStartupError::Disconnected
        }
    }

    fn catalog_error(&self, error: iii_sdk::Error) -> WorkerStartupError {
        if self.client.fatal_error().is_some()
            || matches!(error, iii_sdk::Error::RegistrationRejected { .. })
        {
            WorkerStartupError::RegistrationRejected
        } else if !matches!(
            self.client.get_connection_state(),
            IIIConnectionState::Connected
        ) {
            WorkerStartupError::Disconnected
        } else if matches!(error, iii_sdk::Error::Timeout) {
            WorkerStartupError::Timeout
        } else {
            WorkerStartupError::CatalogRequestFailed
        }
    }
}

impl Drop for WorkerRuntime {
    fn drop(&mut self) {
        self.client.shutdown();
    }
}

#[derive(Debug, Deserialize)]
struct FunctionCatalogResponse {
    #[serde(deserialize_with = "deserialize_catalog_rows")]
    functions: CatalogRows,
}

#[derive(Debug)]
enum CatalogRows {
    Pending,
    Entries(Vec<FunctionCatalogEntry>),
}

fn deserialize_catalog_rows<'de, D>(deserializer: D) -> Result<CatalogRows, D::Error>
where
    D: Deserializer<'de>,
{
    let rows = Vec::<Value>::deserialize(deserializer)?;
    let mut entries = Vec::with_capacity(rows.len());
    for row in rows {
        if row.get("error").is_some() {
            return Ok(CatalogRows::Pending);
        }
        entries.push(
            serde_json::from_value(row)
                .map_err(|error| serde::de::Error::custom(error.to_string()))?,
        );
    }
    Ok(CatalogRows::Entries(entries))
}

fn parse_catalog_response(
    value: Value,
) -> Result<Option<Vec<FunctionCatalogEntry>>, WorkerStartupError> {
    let response = serde_json::from_value::<FunctionCatalogResponse>(value)
        .map_err(|_| WorkerStartupError::CatalogMismatch)?;
    Ok(match response.functions {
        CatalogRows::Pending => None,
        CatalogRows::Entries(entries) => Some(entries),
    })
}

fn timeout_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128).max(1) as u64
}

fn managed_worker_metadata(config: &Config) -> WorkerMetadata {
    let mut metadata = WorkerMetadata::default();
    if let Some(worker_name) = &config.iii.worker_name {
        metadata.name.clone_from(worker_name);
    }
    metadata.namespace = config.iii.namespace.clone();
    metadata
}

pub fn register_harness_functions(
    client: &IIIClient,
    plan: &RegistrationPlan,
    service: Arc<IngestionService<IiiQueuePublisher>>,
) -> [FunctionRef; 3] {
    let [session_start, observation, session_end] = &plan.registrations;

    let session_start_service = Arc::clone(&service);
    let session_start = client.register_function(
        session_start.function_id,
        RegisterFunction::new_async(move |input: SessionStartInput| {
            let service = Arc::clone(&session_start_service);
            async move {
                service
                    .session_start(input)
                    .await
                    .map_err(|error| iii_sdk::Error::Handler(error.to_string()))
            }
        })
        .metadata(session_start.metadata.clone()),
    );

    let observation_service = Arc::clone(&service);
    let observation = client.register_function(
        observation.function_id,
        RegisterFunction::new_async(move |input: ObservationInput| {
            let service = Arc::clone(&observation_service);
            async move {
                service
                    .observation(input)
                    .await
                    .map_err(|error| iii_sdk::Error::Handler(error.to_string()))
            }
        })
        .metadata(observation.metadata.clone()),
    );

    let session_end_service = Arc::clone(&service);
    let session_end = client.register_function(
        session_end.function_id,
        RegisterFunction::new_async(move |input: SessionEndInput| {
            let service = Arc::clone(&session_end_service);
            async move {
                service
                    .session_end(input)
                    .await
                    .map_err(|error| iii_sdk::Error::Handler(error.to_string()))
            }
        })
        .metadata(session_end.metadata.clone()),
    );

    [session_start, observation, session_end]
}
