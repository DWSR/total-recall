#![deny(unsafe_code)]

use std::{
    env,
    ffi::OsString,
    fmt, fs,
    path::{Path, PathBuf},
    time::Duration,
};

use chrono::DateTime;
use serde_json::Value;
use thiserror::Error;
use tokio::time::{Instant, sleep};

pub mod engine;
pub mod opencode;
pub mod process;

pub use engine::{CAPTURE_FUNCTION_IDS, CaptureRecord, CaptureSet, EngineHarness, EngineSession};
pub use engine::{CaptureAssertionError, EngineError, EngineIdentity};
pub use opencode::{
    OPENCODE_V1_VERSION, OPENCODE_V2_VERSION, OpenCodeChildLogs, OpenCodeError, OpenCodeResult,
    OpenCodeScenario, OpenCodeScenarioResult,
};
pub use process::{
    CaptureStream, ProcessDeadlines, ProcessError, ProcessGuard, ProcessIdentity, ProcessOutput,
    ProcessPhase, ProcessSpec, TerminationSignal,
};

const III_BIN: &str = "III_BIN";
const OPENCODE_V1_BIN: &str = "OPENCODE_V1_BIN";
const OPENCODE_V2_BIN: &str = "OPENCODE_V2_BIN";
const HARNESS_EVENTS_BIN: &str = "HARNESS_EVENTS_BIN";
const PLUGIN_TARBALL: &str = "PLUGIN_TARBALL";
const MATRIX_CAPTURE_TIMEOUT: Duration = Duration::from_secs(30);
const MATRIX_CAPTURE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const III_CALLER_WORKER_ID_FIELD: &str = "_caller_worker_id";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactKind {
    Iii,
    OpenCodeV1,
    OpenCodeV2,
    HarnessEvents,
    PluginTarball,
}

impl ArtifactKind {
    pub const ALL: [Self; 5] = [
        Self::Iii,
        Self::OpenCodeV1,
        Self::OpenCodeV2,
        Self::HarnessEvents,
        Self::PluginTarball,
    ];

    pub const fn environment_variable(self) -> &'static str {
        match self {
            Self::Iii => III_BIN,
            Self::OpenCodeV1 => OPENCODE_V1_BIN,
            Self::OpenCodeV2 => OPENCODE_V2_BIN,
            Self::HarnessEvents => HARNESS_EVENTS_BIN,
            Self::PluginTarball => PLUGIN_TARBALL,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Iii => "iii",
            Self::OpenCodeV1 => "opencode-v1",
            Self::OpenCodeV2 => "opencode-v2",
            Self::HarnessEvents => "harness-events",
            Self::PluginTarball => "plugin tarball",
        }
    }
}

impl fmt::Display for ArtifactKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.label())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactPaths {
    pub iii: PathBuf,
    pub opencode_v1: PathBuf,
    pub opencode_v2: PathBuf,
    pub harness_events: PathBuf,
    pub plugin_tarball: PathBuf,
}

impl ArtifactPaths {
    pub fn from_env() -> Result<Self, ArtifactPathError> {
        Self::from_lookup(|variable| env::var_os(variable))
    }

    fn from_lookup<F>(mut lookup: F) -> Result<Self, ArtifactPathError>
    where
        F: FnMut(&str) -> Option<OsString>,
    {
        let paths = Self {
            iii: path_from_lookup(ArtifactKind::Iii, &mut lookup)?,
            opencode_v1: path_from_lookup(ArtifactKind::OpenCodeV1, &mut lookup)?,
            opencode_v2: path_from_lookup(ArtifactKind::OpenCodeV2, &mut lookup)?,
            harness_events: path_from_lookup(ArtifactKind::HarnessEvents, &mut lookup)?,
            plugin_tarball: path_from_lookup(ArtifactKind::PluginTarball, &mut lookup)?,
        };
        paths.validate()?;
        Ok(paths)
    }

    pub fn validate(&self) -> Result<(), ArtifactPathError> {
        for (kind, path) in self.entries() {
            validate_path(kind, path)?;
        }
        Ok(())
    }

    fn entries(&self) -> [(ArtifactKind, &Path); 5] {
        [
            (ArtifactKind::Iii, self.iii.as_path()),
            (ArtifactKind::OpenCodeV1, self.opencode_v1.as_path()),
            (ArtifactKind::OpenCodeV2, self.opencode_v2.as_path()),
            (ArtifactKind::HarnessEvents, self.harness_events.as_path()),
            (ArtifactKind::PluginTarball, self.plugin_tarball.as_path()),
        ]
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ArtifactPathError {
    #[error("artifact environment variable {variable} is missing")]
    MissingEnvironment { variable: &'static str },
    #[error("artifact environment variable {variable} is blank")]
    BlankEnvironment { variable: &'static str },
    #[error("{artifact} artifact path is blank")]
    Blank { artifact: ArtifactKind },
    #[error("{artifact} artifact path {path:?} is unusable: {reason}")]
    Unusable {
        artifact: ArtifactKind,
        path: PathBuf,
        reason: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Generation {
    V1,
    V2,
}

impl Generation {
    pub const fn label(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MatrixAssertionKind {
    ScenarioVersion,
    ScenarioSessionId,
    ScenarioFixtureDirectory,
    ScenarioProjectDirectory,
    CaptureCount,
    StartCount,
    EndCount,
    MissingCreationObservation,
    MissingDeletionObservation,
    RecordOrder(usize),
    RecordFunctionId(usize),
    RecordNamespace(usize),
    RecordSessionId(usize),
    RecordProjectName(usize),
    RecordDirectory(usize),
    RecordTimestamp(usize),
    RecordHookType(usize),
    RecordEnvelope(usize),
    RecordNativePayload(usize),
    LifecycleOrder,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum E2eError {
    #[error(transparent)]
    ArtifactPath(#[from] ArtifactPathError),
    #[error(transparent)]
    Engine(#[from] engine::EngineError),
    #[error(transparent)]
    OpenCode(#[from] opencode::OpenCodeError),
    #[error("OpenCode {generation:?} compatibility matrix assertion failed ({kind:?})")]
    MatrixAssertion {
        generation: Generation,
        kind: MatrixAssertionKind,
    },
    #[error("OpenCode {generation:?} capture did not drain before the bounded deadline")]
    MatrixCaptureTimeout { generation: Generation },
}

pub async fn run_generation(
    generation: Generation,
    artifacts: &ArtifactPaths,
) -> Result<CaptureSet, E2eError> {
    artifacts.validate()?;
    let engine = EngineHarness::new(artifacts).start().await?;
    let receive_index = engine.capture().records.len();
    let result = run_generation_on_engine(generation, artifacts, &engine, receive_index).await;
    let shutdown = engine.shutdown().await;

    match (result, shutdown) {
        (Ok(capture), Ok(())) => Ok(capture),
        (Err(error), Ok(())) => Err(error),
        (_, Err(error)) => Err(error),
    }
}

pub async fn run_compatibility_matrix(artifacts: &ArtifactPaths) -> Result<(), E2eError> {
    artifacts.validate()?;
    let engine = EngineHarness::new(artifacts).start().await?;
    let result = run_matrix_on_engine(artifacts, &engine).await;
    let shutdown = engine.shutdown().await;

    match (result, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (_, Err(error)) => Err(error),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MatrixExpectation {
    generation: Generation,
    native_session_id: String,
    project_name: String,
    project_directory: String,
}

async fn run_matrix_on_engine(
    artifacts: &ArtifactPaths,
    engine: &EngineSession,
) -> Result<(), E2eError> {
    for generation in [Generation::V1, Generation::V2] {
        let receive_index = engine.capture().records.len();
        let _ = run_generation_on_engine(generation, artifacts, engine, receive_index).await?;
    }

    Ok(())
}

async fn run_generation_on_engine(
    generation: Generation,
    artifacts: &ArtifactPaths,
    engine: &EngineSession,
    receive_index: usize,
) -> Result<CaptureSet, E2eError> {
    let scenario = OpenCodeScenario::new(generation, artifacts);
    let result = scenario.run(engine).await?;
    let expectation = matrix_expectation(generation, &result)?;
    let capture = wait_for_capture(engine, receive_index, &expectation).await?;

    assert_capture_contract(&capture, &expectation, engine.identity().namespace.as_str())
        .map_err(|kind| E2eError::MatrixAssertion { generation, kind })?;
    Ok(capture)
}

fn matrix_expectation(
    generation: Generation,
    result: &OpenCodeScenarioResult,
) -> Result<MatrixExpectation, E2eError> {
    let expected_version = match generation {
        Generation::V1 => OPENCODE_V1_VERSION,
        Generation::V2 => OPENCODE_V2_VERSION,
    };
    if result.version != expected_version {
        return Err(E2eError::MatrixAssertion {
            generation,
            kind: MatrixAssertionKind::ScenarioVersion,
        });
    }
    if result.native_session_id.trim().is_empty() {
        return Err(E2eError::MatrixAssertion {
            generation,
            kind: MatrixAssertionKind::ScenarioSessionId,
        });
    }

    let fixture_directory =
        fs::canonicalize(&result.fixture_directory).map_err(|_| E2eError::MatrixAssertion {
            generation,
            kind: MatrixAssertionKind::ScenarioFixtureDirectory,
        })?;
    let project_directory =
        fs::canonicalize(&result.project_directory).map_err(|_| E2eError::MatrixAssertion {
            generation,
            kind: MatrixAssertionKind::ScenarioProjectDirectory,
        })?;
    if !fixture_directory.is_dir() {
        return Err(E2eError::MatrixAssertion {
            generation,
            kind: MatrixAssertionKind::ScenarioFixtureDirectory,
        });
    }
    if !project_directory.is_dir()
        || project_directory.parent() != Some(fixture_directory.as_path())
    {
        return Err(E2eError::MatrixAssertion {
            generation,
            kind: MatrixAssertionKind::ScenarioProjectDirectory,
        });
    }

    let Some(project_name) = project_directory.file_name().and_then(|name| name.to_str()) else {
        return Err(E2eError::MatrixAssertion {
            generation,
            kind: MatrixAssertionKind::ScenarioProjectDirectory,
        });
    };

    Ok(MatrixExpectation {
        generation,
        native_session_id: result.native_session_id.clone(),
        project_name: project_name.to_owned(),
        project_directory: project_directory.to_string_lossy().into_owned(),
    })
}

async fn wait_for_capture(
    engine: &EngineSession,
    receive_index: usize,
    expectation: &MatrixExpectation,
) -> Result<CaptureSet, E2eError> {
    let deadline = Instant::now() + MATRIX_CAPTURE_TIMEOUT;
    loop {
        let capture = engine.capture_since(receive_index);
        if capture_has_required_lifecycle(&capture, expectation) {
            return Ok(capture);
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(E2eError::MatrixCaptureTimeout {
                generation: expectation.generation,
            });
        }
        sleep(remaining.min(MATRIX_CAPTURE_POLL_INTERVAL)).await;
    }
}

fn capture_has_required_lifecycle(capture: &CaptureSet, expectation: &MatrixExpectation) -> bool {
    if capture.records.len() < 4 {
        return false;
    }

    let creation_hook = format!(
        "opencode.{}.session.created",
        expectation.generation.label()
    );
    let deletion_hook = format!(
        "opencode.{}.session.deleted",
        expectation.generation.label()
    );
    let mut has_start = false;
    let mut has_creation = false;
    let mut has_deletion = false;
    let mut has_end = false;

    for record in &capture.records {
        match record.function_id.as_str() {
            "harness::session_start" => {
                has_start |= payload_string(&record.payload, "session_id")
                    == Some(expectation.native_session_id.as_str());
            }
            "harness::observation" => {
                let hook_type = payload_string(&record.payload, "hook_type");
                let session_id = payload_string(&record.payload, "session_id");
                if session_id == Some(expectation.native_session_id.as_str()) {
                    has_creation |= hook_type == Some(creation_hook.as_str());
                    has_deletion |= hook_type == Some(deletion_hook.as_str());
                }
            }
            "harness::session_end" => {
                has_end |= payload_string(&record.payload, "session_id")
                    == Some(expectation.native_session_id.as_str());
            }
            _ => {}
        }
    }

    has_start && has_creation && has_deletion && has_end
}

fn assert_capture_contract(
    capture: &CaptureSet,
    expectation: &MatrixExpectation,
    namespace: &str,
) -> Result<(), MatrixAssertionKind> {
    if capture.records.len() < 4 {
        return Err(MatrixAssertionKind::CaptureCount);
    }

    let mut starts = Vec::new();
    let mut ends = Vec::new();
    let mut creations = Vec::new();
    let mut deletions = Vec::new();
    let mut start_timestamp = None;
    let mut creation_timestamps = Vec::new();
    let mut deletion_timestamps = Vec::new();
    let mut end_timestamp = None;

    for (index, record) in capture.records.iter().enumerate() {
        if record.order != index {
            return Err(MatrixAssertionKind::RecordOrder(index));
        }
        if record.namespace != namespace {
            return Err(MatrixAssertionKind::RecordNamespace(index));
        }

        match record.function_id.as_str() {
            "harness::session_start" => {
                starts.push(index);
                start_timestamp = Some(assert_lifecycle_payload(record, index, expectation)?);
            }
            "harness::observation" => {
                let (native_kind, timestamp) =
                    assert_observation_payload(record, index, expectation)?;
                match native_kind.as_str() {
                    "session.created" => {
                        creations.push(index);
                        creation_timestamps.push(timestamp);
                    }
                    "session.deleted" => {
                        deletions.push(index);
                        deletion_timestamps.push(timestamp);
                    }
                    _ => {}
                }
            }
            "harness::session_end" => {
                ends.push(index);
                end_timestamp = Some(assert_lifecycle_payload(record, index, expectation)?);
            }
            _ => return Err(MatrixAssertionKind::RecordFunctionId(index)),
        }
    }

    if starts.len() != 1 {
        return Err(MatrixAssertionKind::StartCount);
    }
    if ends.len() != 1 {
        return Err(MatrixAssertionKind::EndCount);
    }
    if creations.is_empty() {
        return Err(MatrixAssertionKind::MissingCreationObservation);
    }
    if deletions.is_empty() {
        return Err(MatrixAssertionKind::MissingDeletionObservation);
    }

    let start = starts[0];
    let end = ends[0];
    let last_creation = *creations.last().expect("creation observation exists");
    let first_deletion = deletions[0];
    let last_deletion = *deletions.last().expect("deletion observation exists");
    if start >= creations[0]
        || last_creation >= first_deletion
        || last_deletion >= end
        || capture.records[start + 1..end]
            .iter()
            .any(|record| record.function_id == "harness::session_start")
    {
        return Err(MatrixAssertionKind::LifecycleOrder);
    }
    if capture.records[..start]
        .iter()
        .chain(capture.records[end + 1..].iter())
        .any(|record| record.function_id == "harness::observation")
    {
        return Err(MatrixAssertionKind::LifecycleOrder);
    }

    if start_timestamp
        .as_ref()
        .is_none_or(|timestamp| creation_timestamps.iter().any(|value| value != timestamp))
        || end_timestamp
            .as_ref()
            .is_none_or(|timestamp| deletion_timestamps.iter().any(|value| value != timestamp))
    {
        return Err(MatrixAssertionKind::RecordTimestamp(start));
    }

    Ok(())
}

fn assert_lifecycle_payload(
    record: &CaptureRecord,
    index: usize,
    expectation: &MatrixExpectation,
) -> Result<String, MatrixAssertionKind> {
    if !has_exact_capture_fields(
        &record.payload,
        [
            "session_id",
            "project_name",
            "current_working_directory",
            "timestamp",
        ],
    ) {
        return Err(MatrixAssertionKind::RecordEnvelope(index));
    }
    if payload_string(&record.payload, "session_id") != Some(expectation.native_session_id.as_str())
    {
        return Err(MatrixAssertionKind::RecordSessionId(index));
    }
    if payload_string(&record.payload, "project_name") != Some(expectation.project_name.as_str()) {
        return Err(MatrixAssertionKind::RecordProjectName(index));
    }
    if payload_string(&record.payload, "current_working_directory")
        != Some(expectation.project_directory.as_str())
    {
        return Err(MatrixAssertionKind::RecordDirectory(index));
    }

    let Some(timestamp) = payload_string(&record.payload, "timestamp") else {
        return Err(MatrixAssertionKind::RecordTimestamp(index));
    };
    if !is_millisecond_rfc3339(timestamp) {
        return Err(MatrixAssertionKind::RecordTimestamp(index));
    }
    Ok(timestamp.to_owned())
}

fn assert_observation_payload(
    record: &CaptureRecord,
    index: usize,
    expectation: &MatrixExpectation,
) -> Result<(String, String), MatrixAssertionKind> {
    if !has_exact_capture_fields(
        &record.payload,
        [
            "hook_type",
            "project_name",
            "current_working_directory",
            "timestamp",
            "session_id",
            "data",
        ],
    ) {
        return Err(MatrixAssertionKind::RecordEnvelope(index));
    }
    if payload_string(&record.payload, "session_id") != Some(expectation.native_session_id.as_str())
    {
        return Err(MatrixAssertionKind::RecordSessionId(index));
    }
    if payload_string(&record.payload, "project_name") != Some(expectation.project_name.as_str()) {
        return Err(MatrixAssertionKind::RecordProjectName(index));
    }
    if payload_string(&record.payload, "current_working_directory")
        != Some(expectation.project_directory.as_str())
    {
        return Err(MatrixAssertionKind::RecordDirectory(index));
    }

    let Some(timestamp) = payload_string(&record.payload, "timestamp") else {
        return Err(MatrixAssertionKind::RecordTimestamp(index));
    };
    if !is_millisecond_rfc3339(timestamp) {
        return Err(MatrixAssertionKind::RecordTimestamp(index));
    }

    let Some(hook_type) = payload_string(&record.payload, "hook_type") else {
        return Err(MatrixAssertionKind::RecordHookType(index));
    };
    let prefix = format!("opencode.{}.", expectation.generation.label());
    let Some(native_kind) = hook_type.strip_prefix(&prefix) else {
        return Err(MatrixAssertionKind::RecordHookType(index));
    };
    if native_kind.is_empty() {
        return Err(MatrixAssertionKind::RecordHookType(index));
    }

    let Some(data) = record.payload.get("data") else {
        return Err(MatrixAssertionKind::RecordEnvelope(index));
    };
    if !has_exact_fields(data, ["source", "generation", "kind", "payload"])
        || payload_string(data, "source") != Some("opencode")
        || payload_string(data, "generation") != Some(expectation.generation.label())
        || payload_string(data, "kind") != Some(native_kind)
        || !data.get("payload").is_some_and(Value::is_object)
    {
        return Err(MatrixAssertionKind::RecordEnvelope(index));
    }

    if matches!(native_kind, "session.created" | "session.deleted")
        && !data
            .get("payload")
            .is_some_and(|payload| contains_string(payload, &expectation.native_session_id))
    {
        return Err(MatrixAssertionKind::RecordNativePayload(index));
    }

    Ok((native_kind.to_owned(), timestamp.to_owned()))
}

fn payload_string<'a>(payload: &'a Value, field: &str) -> Option<&'a str> {
    payload.get(field).and_then(Value::as_str)
}

fn has_exact_capture_fields<const N: usize>(payload: &Value, fields: [&str; N]) -> bool {
    let Some(object) = payload.as_object() else {
        return false;
    };
    fields.into_iter().all(|field| object.contains_key(field))
        && object
            .keys()
            .all(|field| fields.contains(&field.as_str()) || field == III_CALLER_WORKER_ID_FIELD)
}

fn has_exact_fields<const N: usize>(payload: &Value, fields: [&str; N]) -> bool {
    let Some(object) = payload.as_object() else {
        return false;
    };
    object.len() == N && fields.into_iter().all(|field| object.contains_key(field))
}

fn contains_string(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(value) => value == expected,
        Value::Array(values) => values.iter().any(|value| contains_string(value, expected)),
        Value::Object(fields) => fields
            .values()
            .any(|value| contains_string(value, expected)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn is_millisecond_rfc3339(value: &str) -> bool {
    let bytes = value.as_bytes();
    value.len() == 24
        && bytes[19] == b'.'
        && bytes[20..23].iter().all(u8::is_ascii_digit)
        && bytes[23] == b'Z'
        && DateTime::parse_from_rfc3339(value).is_ok()
}

fn path_from_lookup<F>(kind: ArtifactKind, lookup: &mut F) -> Result<PathBuf, ArtifactPathError>
where
    F: FnMut(&str) -> Option<OsString>,
{
    let variable = kind.environment_variable();
    let value = lookup(variable).ok_or(ArtifactPathError::MissingEnvironment { variable })?;
    let path = PathBuf::from(value);
    if is_blank(path.as_path()) {
        return Err(ArtifactPathError::BlankEnvironment { variable });
    }
    Ok(path)
}

fn validate_path(kind: ArtifactKind, path: &Path) -> Result<(), ArtifactPathError> {
    if is_blank(path) {
        return Err(ArtifactPathError::Blank { artifact: kind });
    }

    let metadata = fs::metadata(path).map_err(|error| ArtifactPathError::Unusable {
        artifact: kind,
        path: path.to_path_buf(),
        reason: format!("metadata lookup failed with {:?}", error.kind()),
    })?;
    if !metadata.is_file() {
        return Err(ArtifactPathError::Unusable {
            artifact: kind,
            path: path.to_path_buf(),
            reason: "path is not a regular file".to_owned(),
        });
    }

    Ok(())
}

fn is_blank(path: &Path) -> bool {
    path.as_os_str().is_empty() || path.to_string_lossy().trim().is_empty()
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, ffi::OsString, path::PathBuf};

    use serde_json::{Value, json};

    use super::{
        ArtifactKind, ArtifactPathError, ArtifactPaths, CaptureRecord, CaptureSet, Generation,
        MatrixAssertionKind, MatrixExpectation, assert_capture_contract,
    };

    fn regular_file() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml")
    }

    fn artifact_environment() -> HashMap<&'static str, OsString> {
        let path = regular_file().into_os_string();
        ArtifactKind::ALL
            .map(|kind| (kind.environment_variable(), path.clone()))
            .into_iter()
            .collect()
    }

    fn expectation(generation: Generation) -> MatrixExpectation {
        MatrixExpectation {
            generation,
            native_session_id: "ses_fixture".to_owned(),
            project_name: "project".to_owned(),
            project_directory: "/fixture/project".to_owned(),
        }
    }

    fn record(function_id: &str, payload: Value, order: usize) -> CaptureRecord {
        CaptureRecord {
            function_id: function_id.to_owned(),
            namespace: "namespace".to_owned(),
            payload,
            order,
        }
    }

    fn valid_capture(generation: Generation) -> CaptureSet {
        let expectation = expectation(generation);
        let timestamp = "2026-09-19T20:14:33.123Z";
        let lifecycle = |function_id: &str| {
            record(
                function_id,
                json!({
                    "session_id": expectation.native_session_id.clone(),
                    "project_name": expectation.project_name.clone(),
                    "current_working_directory": expectation.project_directory.clone(),
                    "timestamp": timestamp,
                }),
                if function_id == "harness::session_start" {
                    0
                } else {
                    3
                },
            )
        };
        let observation = |kind: &str, order: usize| {
            record(
                "harness::observation",
                json!({
                    "hook_type": format!("opencode.{}.{kind}", generation.label()),
                    "project_name": "project",
                    "current_working_directory": "/fixture/project",
                    "timestamp": timestamp,
                    "session_id": "ses_fixture",
                    "data": {
                        "source": "opencode",
                        "generation": generation.label(),
                        "kind": kind,
                        "payload": {"session_id": "ses_fixture"},
                    },
                }),
                order,
            )
        };

        CaptureSet {
            records: vec![
                lifecycle("harness::session_start"),
                observation("session.created", 1),
                observation("session.deleted", 2),
                lifecycle("harness::session_end"),
            ],
        }
    }

    #[test]
    fn artifact_environment_parser_requires_every_explicit_path() {
        let values = artifact_environment();
        let parsed = ArtifactPaths::from_lookup(|variable| values.get(variable).cloned())
            .expect("all explicit artifact paths parse");
        assert_eq!(parsed.iii, regular_file());
        assert_eq!(parsed.opencode_v1, regular_file());
        assert_eq!(parsed.opencode_v2, regular_file());
        assert_eq!(parsed.harness_events, regular_file());
        assert_eq!(parsed.plugin_tarball, regular_file());

        let mut missing = artifact_environment();
        missing.remove(ArtifactKind::OpenCodeV2.environment_variable());
        assert_eq!(
            ArtifactPaths::from_lookup(|variable| missing.get(variable).cloned()),
            Err(ArtifactPathError::MissingEnvironment {
                variable: "OPENCODE_V2_BIN"
            })
        );

        let mut blank = artifact_environment();
        blank.insert(
            ArtifactKind::HarnessEvents.environment_variable(),
            "  ".into(),
        );
        assert_eq!(
            ArtifactPaths::from_lookup(|variable| blank.get(variable).cloned()),
            Err(ArtifactPathError::BlankEnvironment {
                variable: "HARNESS_EVENTS_BIN"
            })
        );
    }

    #[test]
    fn compatibility_matrix_assertions_cover_exact_fields_and_native_envelopes() {
        for generation in [Generation::V1, Generation::V2] {
            let capture = valid_capture(generation);

            assert_capture_contract(&capture, &expectation(generation), "namespace")
                .expect("the provider-free lifecycle capture satisfies the matrix contract");
        }
    }

    #[test]
    fn compatibility_matrix_allows_iii_caller_metadata_but_no_other_extra_fields() {
        let mut capture = valid_capture(Generation::V1);
        for record in &mut capture.records {
            record.payload["_caller_worker_id"] = json!("iii-worker");
        }

        assert_capture_contract(&capture, &expectation(Generation::V1), "namespace")
            .expect("iii transport metadata does not alter the plugin payload contract");

        capture.records[0].payload["unexpected"] = json!(true);
        assert_eq!(
            assert_capture_contract(&capture, &expectation(Generation::V1), "namespace"),
            Err(MatrixAssertionKind::RecordEnvelope(0))
        );

        let mut nested_metadata = valid_capture(Generation::V1);
        nested_metadata.records[1].payload["data"]["_caller_worker_id"] = json!("spoofed");
        assert_eq!(
            assert_capture_contract(&nested_metadata, &expectation(Generation::V1), "namespace"),
            Err(MatrixAssertionKind::RecordEnvelope(1))
        );
    }

    #[test]
    fn compatibility_matrix_assertion_errors_do_not_include_native_payloads() {
        let mut capture = valid_capture(Generation::V1);
        capture.records[2].payload["session_id"] = json!("payload-secret");

        let error = assert_capture_contract(&capture, &expectation(Generation::V1), "namespace")
            .expect_err("unrelated session identity must fail the matrix contract");

        assert_eq!(error, MatrixAssertionKind::RecordSessionId(2));
        assert!(!format!("{error:?}").contains("payload-secret"));
    }
}
