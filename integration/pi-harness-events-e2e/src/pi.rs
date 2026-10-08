use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use serde_json::Value;
use thiserror::Error;
use tokio::{process::Command, time::Instant};

use crate::{
    artifacts::ArtifactPaths,
    process::{CaptureLimits, ProcessGuard, STDERR_CAPTURE_LIMIT},
};

const NODE_MAJOR: &str = "24";
const PI_VERSION: &str = "0.86.1";
const III_VERSION: &str = "0.24.0";
const PI_PACKAGE_NAME: &str = "@earendil-works/pi-coding-agent";
const SCRIPTED_PROVIDER_NAME: &str = "scripted-provider.ts";
const FIXTURE_TOOL_NAME: &str = "fixture_tool";
const FIXTURE_FINAL_TEXT: &str = "fixture complete";
const FIXTURE_TOOL_RESULT: &str = "fixture tool result";
const HARNESS_EVENTS_TIMEOUT_MS: &str = "2000";
const CLOSED_LOOPBACK_III_URL: &str = "ws://127.0.0.1:1";
const CLOSED_LOOPBACK_III_NAMESPACE: &str = "pi-harness-events-e2e-closed-loopback";
const VERSION_CAPTURE_LIMIT: usize = 1024;
const JSONL_CAPTURE_LIMIT: usize = 128 * 1024;
const VERSION_DEADLINE: Duration = Duration::from_secs(5);
const PI_DEADLINE: Duration = Duration::from_secs(35);
const HARNESS_EVENTS_ROOT_HELP: &[&str] = &[
    "Usage: harness-events <COMMAND>",
    "Commands:",
    "session-start",
    "observation",
    "session-end",
    "help Print this message or the help of the given subcommand(s)",
    "Options:",
    "-h, --help Print help",
];
const HARNESS_EVENTS_SESSION_START_HELP: &[&str] = &[
    "Usage: harness-events session-start --session-id <SESSION_ID> --project-name <PROJECT_NAME> --current-working-directory <CURRENT_WORKING_DIRECTORY> --timestamp <TIMESTAMP>",
    "Options:",
    "--session-id <SESSION_ID>",
    "--project-name <PROJECT_NAME>",
    "--current-working-directory <CURRENT_WORKING_DIRECTORY>",
    "--timestamp <TIMESTAMP>",
    "-h, --help Print help",
];
const HARNESS_EVENTS_OBSERVATION_HELP: &[&str] = &[
    "Usage: harness-events observation --hook-type <HOOK_TYPE> --project-name <PROJECT_NAME> --current-working-directory <CURRENT_WORKING_DIRECTORY> --timestamp <TIMESTAMP> --session-id <SESSION_ID>",
    "Options:",
    "--hook-type <HOOK_TYPE>",
    "--project-name <PROJECT_NAME>",
    "--current-working-directory <CURRENT_WORKING_DIRECTORY>",
    "--timestamp <TIMESTAMP>",
    "--session-id <SESSION_ID>",
    "-h, --help Print help",
];
const HARNESS_EVENTS_SESSION_END_HELP: &[&str] = &[
    "Usage: harness-events session-end --session-id <SESSION_ID> --project-name <PROJECT_NAME> --current-working-directory <CURRENT_WORKING_DIRECTORY> --timestamp <TIMESTAMP>",
    "Options:",
    "--session-id <SESSION_ID>",
    "--project-name <PROJECT_NAME>",
    "--current-working-directory <CURRENT_WORKING_DIRECTORY>",
    "--timestamp <TIMESTAMP>",
    "-h, --help Print help",
];

static NEXT_TEMP_DIRECTORY: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PiScenarioError {
    #[error("Pi scenario artifact is invalid")]
    InvalidArtifact,
    #[error("Pi scenario engine route is invalid")]
    InvalidEngineRoute,
    #[error("Pi scenario setup failed")]
    SetupFailed,
    #[error("Pi scenario process failed")]
    ProcessFailed,
    #[error("Pi scenario exited unsuccessfully")]
    UnexpectedExit,
    #[error("Pi scenario output was truncated")]
    OutputTruncated,
    #[error("Pi scenario emitted invalid JSON")]
    InvalidJsonOutput,
    #[error("Pi scenario did not emit a session identity")]
    MissingSessionIdentity,
    #[error("Pi scenario activity was incomplete")]
    MissingActivity,
    #[error("Pi scenario used an unsupported Node version")]
    UnsupportedNodeVersion,
    #[error("Pi scenario used an unsupported Pi version")]
    UnsupportedPiVersion,
}

pub struct PiScenarioOutcome {
    pub process_id: u32,
    pub isolated_root: PathBuf,
    pub session_id: String,
    pub working_directory: PathBuf,
}

pub async fn run_headless_pi(
    artifacts: &ArtifactPaths,
) -> Result<PiScenarioOutcome, PiScenarioError> {
    run_headless_pi_with_engine(
        artifacts,
        CLOSED_LOOPBACK_III_URL,
        CLOSED_LOOPBACK_III_NAMESPACE,
    )
    .await
}

pub async fn run_headless_pi_with_engine(
    artifacts: &ArtifactPaths,
    engine_url: &str,
    namespace: &str,
) -> Result<PiScenarioOutcome, PiScenarioError> {
    if engine_url.trim().is_empty() || namespace.trim().is_empty() {
        return Err(PiScenarioError::InvalidEngineRoute);
    }

    let artifacts = resolve_artifacts(artifacts)?;
    let roots = IsolatedRoots::new()?;
    let working_directory =
        fs::canonicalize(roots.path("work")).map_err(|_| PiScenarioError::SetupFailed)?;

    verify_node_version(&artifacts).await?;
    verify_pi_version(&artifacts).await?;
    verify_iii_version(&artifacts).await?;
    verify_harness_events_identity(&artifacts).await?;

    let mut command = Command::new(&artifacts.node);
    command
        .arg(&artifacts.pi_cli)
        .args(headless_arguments(&artifacts, &roots))
        .current_dir(&working_directory)
        .env_clear()
        .envs(scenario_environment_with_engine(
            &artifacts, &roots, engine_url, namespace,
        )?)
        // ProcessGuard pipes only stdout and stderr. Explicitly close stdin so
        // Pi does not wait on the test runner's non-TTY input stream.
        .stdin(Stdio::null());
    let mut process = ProcessGuard::spawn(
        &mut command,
        CaptureLimits::new(JSONL_CAPTURE_LIMIT, STDERR_CAPTURE_LIMIT),
    )
    .map_err(|_| PiScenarioError::ProcessFailed)?;
    let process_id = process.process_id();
    let status = process
        .wait_for_exit_until(Instant::now() + PI_DEADLINE)
        .await
        .map_err(|_| PiScenarioError::ProcessFailed)?;
    if !status.success() {
        return Err(PiScenarioError::UnexpectedExit);
    }

    let output = process.output().ok_or(PiScenarioError::ProcessFailed)?;
    if output.stdout().truncated() || output.stderr().truncated() {
        return Err(PiScenarioError::OutputTruncated);
    }
    let events = parse_jsonl(output.stdout().bytes())?;
    let session_id = session_identity(&events)?;
    assert_expected_activity(&events)?;
    let isolated_root = roots.root.clone();
    drop(roots);

    Ok(PiScenarioOutcome {
        process_id,
        isolated_root,
        session_id,
        working_directory,
    })
}

fn resolve_artifacts(artifacts: &ArtifactPaths) -> Result<ArtifactPaths, PiScenarioError> {
    let node = canonical_executable(&artifacts.node)?;
    let pi_cli = canonical_file(&artifacts.pi_cli)?;
    let iii = canonical_executable(&artifacts.iii)?;
    let harness_events = canonical_executable(&artifacts.harness_events)?;
    let extension_root = canonical_directory(&artifacts.extension_root)?;
    let scripted_provider = canonical_file(&artifacts.scripted_provider)?;

    validate_pi_cli(&pi_cli)?;
    if !scripted_provider.ends_with(
        Path::new("test")
            .join("fixtures")
            .join(SCRIPTED_PROVIDER_NAME),
    ) {
        return Err(PiScenarioError::InvalidArtifact);
    }

    Ok(ArtifactPaths {
        node,
        pi_cli,
        iii,
        harness_events,
        extension_root,
        scripted_provider,
    })
}

fn canonical_file(path: &Path) -> Result<PathBuf, PiScenarioError> {
    let path = fs::canonicalize(path).map_err(|_| PiScenarioError::InvalidArtifact)?;
    if path.is_file() {
        Ok(path)
    } else {
        Err(PiScenarioError::InvalidArtifact)
    }
}

fn canonical_directory(path: &Path) -> Result<PathBuf, PiScenarioError> {
    let path = fs::canonicalize(path).map_err(|_| PiScenarioError::InvalidArtifact)?;
    if path.is_dir() {
        Ok(path)
    } else {
        Err(PiScenarioError::InvalidArtifact)
    }
}

fn canonical_executable(path: &Path) -> Result<PathBuf, PiScenarioError> {
    let path = canonical_file(path)?;
    if fs::metadata(&path)
        .map_err(|_| PiScenarioError::InvalidArtifact)?
        .permissions()
        .mode()
        & 0o111
        != 0
    {
        Ok(path)
    } else {
        Err(PiScenarioError::InvalidArtifact)
    }
}

fn validate_pi_cli(pi_cli: &Path) -> Result<(), PiScenarioError> {
    if !pi_cli.ends_with(Path::new("dist").join("bundle").join("cli.js")) {
        return Err(PiScenarioError::InvalidArtifact);
    }
    let package_root = pi_cli
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .ok_or(PiScenarioError::InvalidArtifact)?;
    let manifest = fs::read_to_string(package_root.join("package.json"))
        .ok()
        .and_then(|contents| serde_json::from_str::<Value>(&contents).ok())
        .ok_or(PiScenarioError::InvalidArtifact)?;
    if manifest.get("name").and_then(Value::as_str) == Some(PI_PACKAGE_NAME)
        && manifest.get("version").and_then(Value::as_str) == Some(PI_VERSION)
    {
        Ok(())
    } else {
        Err(PiScenarioError::InvalidArtifact)
    }
}

async fn verify_node_version(artifacts: &ArtifactPaths) -> Result<(), PiScenarioError> {
    let mut command = Command::new(&artifacts.node);
    command.arg("--version").env_clear().stdin(Stdio::null());
    if is_node_24(&run_probe_command(&mut command).await?) {
        Ok(())
    } else {
        Err(PiScenarioError::UnsupportedNodeVersion)
    }
}

async fn verify_pi_version(artifacts: &ArtifactPaths) -> Result<(), PiScenarioError> {
    let mut command = Command::new(&artifacts.node);
    command
        .arg(&artifacts.pi_cli)
        .arg("--version")
        .env_clear()
        .stdin(Stdio::null());
    if is_pi_086_1(&run_probe_command(&mut command).await?) {
        Ok(())
    } else {
        Err(PiScenarioError::UnsupportedPiVersion)
    }
}

async fn verify_iii_version(artifacts: &ArtifactPaths) -> Result<(), PiScenarioError> {
    let mut command = Command::new(&artifacts.iii);
    command.arg("--version").env_clear().stdin(Stdio::null());
    if is_iii_024(&run_probe_command(&mut command).await?) {
        Ok(())
    } else {
        Err(PiScenarioError::InvalidArtifact)
    }
}

async fn verify_harness_events_identity(artifacts: &ArtifactPaths) -> Result<(), PiScenarioError> {
    let mut command = Command::new(&artifacts.harness_events);
    command.arg("--help").env_clear().stdin(Stdio::null());
    if !is_harness_events_help(&run_probe_command(&mut command).await?) {
        return Err(PiScenarioError::InvalidArtifact);
    }

    for (arguments, expected) in [
        (
            &["session-start", "--help"],
            HARNESS_EVENTS_SESSION_START_HELP,
        ),
        (&["observation", "--help"], HARNESS_EVENTS_OBSERVATION_HELP),
        (&["session-end", "--help"], HARNESS_EVENTS_SESSION_END_HELP),
    ] {
        let mut command = Command::new(&artifacts.harness_events);
        command.args(arguments).env_clear().stdin(Stdio::null());
        if !has_harness_events_help_grammar(&run_probe_command(&mut command).await?, expected) {
            return Err(PiScenarioError::InvalidArtifact);
        }
    }

    Ok(())
}

async fn run_probe_command(command: &mut Command) -> Result<OsString, PiScenarioError> {
    let mut process = ProcessGuard::spawn(
        command,
        CaptureLimits::new(VERSION_CAPTURE_LIMIT, STDERR_CAPTURE_LIMIT),
    )
    .map_err(|_| PiScenarioError::ProcessFailed)?;
    let status = process
        .wait_for_exit_until(Instant::now() + VERSION_DEADLINE)
        .await
        .map_err(|_| PiScenarioError::ProcessFailed)?;
    if !status.success() {
        return Err(PiScenarioError::UnexpectedExit);
    }

    let output = process.output().ok_or(PiScenarioError::ProcessFailed)?;
    if output.stdout().truncated() || output.stderr().truncated() {
        return Err(PiScenarioError::OutputTruncated);
    }
    let version = std::str::from_utf8(output.stdout().bytes())
        .map_err(|_| PiScenarioError::InvalidArtifact)?
        .trim();
    Ok(OsString::from(version))
}

fn is_node_24(version: &OsStr) -> bool {
    version
        .to_str()
        .and_then(|version| version.trim().strip_prefix('v'))
        .and_then(|version| version.split('.').next())
        == Some(NODE_MAJOR)
}

fn is_pi_086_1(version: &OsStr) -> bool {
    version
        .to_str()
        .is_some_and(|version| version.trim() == PI_VERSION)
}

fn is_iii_024(version: &OsStr) -> bool {
    version
        .to_str()
        .is_some_and(|version| version.trim() == III_VERSION)
}

fn is_harness_events_help(output: &OsStr) -> bool {
    has_harness_events_help_grammar(output, HARNESS_EVENTS_ROOT_HELP)
}

fn has_harness_events_help_grammar(output: &OsStr, expected: &[&str]) -> bool {
    let Some(output) = output.to_str() else {
        return false;
    };
    let actual = output
        .lines()
        .filter_map(|line| {
            let line = line.split_whitespace().collect::<Vec<_>>().join(" ");
            (!line.is_empty()).then_some(line)
        })
        .collect::<Vec<_>>();

    actual
        .iter()
        .map(String::as_str)
        .eq(expected.iter().copied())
}

fn headless_arguments(artifacts: &ArtifactPaths, roots: &IsolatedRoots) -> Vec<OsString> {
    vec![
        "--mode".into(),
        "json".into(),
        "--no-session".into(),
        "--session-dir".into(),
        roots.path("session").into(),
        "--offline".into(),
        "--no-approve".into(),
        "--no-extensions".into(),
        "-e".into(),
        artifacts.extension_root.clone().into(),
        "-e".into(),
        artifacts.scripted_provider.clone().into(),
        "--no-skills".into(),
        "--no-prompt-templates".into(),
        "--no-themes".into(),
        "--no-context-files".into(),
        "--no-builtin-tools".into(),
        "--provider".into(),
        "pi-harness-events-scripted".into(),
        "--model".into(),
        "scripted-fixture-v1".into(),
        "--api-key".into(),
        "fixture-key".into(),
        "--thinking".into(),
        "off".into(),
        "--".into(),
        "run the fixture".into(),
    ]
}

#[cfg(test)]
fn scenario_environment(
    artifacts: &ArtifactPaths,
    roots: &IsolatedRoots,
) -> Result<BTreeMap<String, OsString>, PiScenarioError> {
    scenario_environment_with_engine(
        artifacts,
        roots,
        CLOSED_LOOPBACK_III_URL,
        CLOSED_LOOPBACK_III_NAMESPACE,
    )
}

fn scenario_environment_with_engine(
    artifacts: &ArtifactPaths,
    roots: &IsolatedRoots,
    engine_url: &str,
    namespace: &str,
) -> Result<BTreeMap<String, OsString>, PiScenarioError> {
    if !artifacts.harness_events.is_absolute() {
        return Err(PiScenarioError::InvalidArtifact);
    }
    if engine_url.trim().is_empty() || namespace.trim().is_empty() {
        return Err(PiScenarioError::InvalidEngineRoute);
    }

    Ok(BTreeMap::from([
        (
            "HARNESS_EVENTS_BIN".into(),
            artifacts.harness_events.clone().into_os_string(),
        ),
        (
            "HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS".into(),
            HARNESS_EVENTS_TIMEOUT_MS.into(),
        ),
        (
            "HARNESS_EVENTS_TIMEOUT_MS".into(),
            HARNESS_EVENTS_TIMEOUT_MS.into(),
        ),
        ("HOME".into(), roots.path("home").into_os_string()),
        ("III_NAMESPACE".into(), namespace.into()),
        ("III_URL".into(), engine_url.into()),
        (
            "NODE_COMPILE_CACHE".into(),
            roots.path("node-compile-cache").into_os_string(),
        ),
        (
            "PI_CODING_AGENT_DIR".into(),
            roots.path("pi-state").into_os_string(),
        ),
        (
            "PI_CODING_AGENT_SESSION_DIR".into(),
            roots.path("session").into_os_string(),
        ),
        ("PI_OFFLINE".into(), "1".into()),
        ("PI_SKIP_VERSION_CHECK".into(), "1".into()),
        ("PI_TELEMETRY".into(), "0".into()),
        ("TEMP".into(), roots.path("tmp").into_os_string()),
        ("TMP".into(), roots.path("tmp").into_os_string()),
        ("TMPDIR".into(), roots.path("tmp").into_os_string()),
        (
            "XDG_CACHE_HOME".into(),
            roots.path("xdg-cache").into_os_string(),
        ),
        (
            "XDG_CONFIG_HOME".into(),
            roots.path("xdg-config").into_os_string(),
        ),
        (
            "XDG_DATA_HOME".into(),
            roots.path("xdg-data").into_os_string(),
        ),
        (
            "XDG_STATE_HOME".into(),
            roots.path("xdg-state").into_os_string(),
        ),
        (
            "npm_config_cache".into(),
            roots.path("npm-cache").into_os_string(),
        ),
        ("npm_config_offline".into(), "true".into()),
    ]))
}

fn parse_jsonl(output: &[u8]) -> Result<Vec<Value>, PiScenarioError> {
    let output = std::str::from_utf8(output).map_err(|_| PiScenarioError::InvalidJsonOutput)?;
    output
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let value = serde_json::from_str::<Value>(line)
                .map_err(|_| PiScenarioError::InvalidJsonOutput)?;
            if value.is_object() {
                Ok(value)
            } else {
                Err(PiScenarioError::InvalidJsonOutput)
            }
        })
        .collect()
}

fn session_identity(events: &[Value]) -> Result<String, PiScenarioError> {
    events
        .iter()
        .find(|event| event_type(event, "session"))
        .and_then(|event| event.get("id"))
        .and_then(Value::as_str)
        .filter(|identity| !identity.is_empty())
        .map(ToOwned::to_owned)
        .ok_or(PiScenarioError::MissingSessionIdentity)
}

fn assert_expected_activity(events: &[Value]) -> Result<(), PiScenarioError> {
    let mut stage = 0;
    for event in events {
        let matched = match stage {
            0 => event_type(event, "agent_start"),
            1 => event_type(event, "turn_start"),
            2 => message_has(event, "user", "text", "text", "run the fixture"),
            3 => message_has(event, "assistant", "toolCall", "name", FIXTURE_TOOL_NAME),
            4 => {
                event_type(event, "tool_execution_start")
                    && event.get("toolName").and_then(Value::as_str) == Some(FIXTURE_TOOL_NAME)
            }
            5 => {
                event_type(event, "tool_execution_end")
                    && event.get("toolName").and_then(Value::as_str) == Some(FIXTURE_TOOL_NAME)
                    && event.get("isError").and_then(Value::as_bool) == Some(false)
            }
            6 => {
                message_has(event, "toolResult", "text", "text", FIXTURE_TOOL_RESULT)
                    && event.pointer("/message/toolName").and_then(Value::as_str)
                        == Some(FIXTURE_TOOL_NAME)
                    && event.pointer("/message/isError").and_then(Value::as_bool) == Some(false)
            }
            7 | 10 => event_type(event, "turn_end"),
            8 => event_type(event, "turn_start"),
            9 => message_has(event, "assistant", "text", "text", FIXTURE_FINAL_TEXT),
            11 => event_type(event, "agent_end"),
            12 => event_type(event, "agent_settled"),
            _ => false,
        };
        if matched {
            stage += 1;
        }
    }

    if stage == 13 {
        Ok(())
    } else {
        Err(PiScenarioError::MissingActivity)
    }
}

fn event_type(event: &Value, expected: &str) -> bool {
    event.get("type").and_then(Value::as_str) == Some(expected)
}

fn message_has(event: &Value, role: &str, content_type: &str, field: &str, value: &str) -> bool {
    event_type(event, "message_end")
        && event
            .get("message")
            .filter(|message| message.get("role").and_then(Value::as_str) == Some(role))
            .and_then(|message| message.get("content").and_then(Value::as_array))
            .is_some_and(|content| {
                content.iter().any(|entry| {
                    entry.get("type").and_then(Value::as_str) == Some(content_type)
                        && entry.get(field).and_then(Value::as_str) == Some(value)
                })
            })
}

struct IsolatedRoots {
    root: PathBuf,
}

impl IsolatedRoots {
    fn new() -> Result<Self, PiScenarioError> {
        let sequence = NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "pi-harness-events-e2e-pi-{}-{sequence}",
            std::process::id()
        ));
        let roots = Self { root };
        for directory in "home tmp xdg-cache xdg-config xdg-data xdg-state pi-state session npm-cache node-compile-cache work".split_whitespace() {
            fs::create_dir_all(roots.path(directory)).map_err(|_| PiScenarioError::SetupFailed)?;
        }
        Ok(roots)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }
}

impl Drop for IsolatedRoots {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        ffi::{OsStr, OsString},
        fs,
        os::unix::fs::{PermissionsExt, symlink},
        path::{Path, PathBuf},
    };

    use serde_json::{Value, json};

    use crate::artifacts::ArtifactPaths;

    use super::{
        IsolatedRoots, PiScenarioError, assert_expected_activity, headless_arguments,
        is_harness_events_help, is_node_24, is_pi_086_1, parse_jsonl, resolve_artifacts,
        scenario_environment,
    };

    #[test]
    fn headless_invocation_uses_only_the_explicit_fixture_roots() {
        let roots = IsolatedRoots::new().expect("create isolated roots");
        let artifacts = artifact_paths();

        let expected: Vec<OsString> = vec![
            "--mode".into(),
            "json".into(),
            "--no-session".into(),
            "--session-dir".into(),
            roots.path("session").into(),
            "--offline".into(),
            "--no-approve".into(),
            "--no-extensions".into(),
            "-e".into(),
            artifacts.extension_root.clone().into(),
            "-e".into(),
            artifacts.scripted_provider.clone().into(),
            "--no-skills".into(),
            "--no-prompt-templates".into(),
            "--no-themes".into(),
            "--no-context-files".into(),
            "--no-builtin-tools".into(),
            "--provider".into(),
            "pi-harness-events-scripted".into(),
            "--model".into(),
            "scripted-fixture-v1".into(),
            "--api-key".into(),
            "fixture-key".into(),
            "--thinking".into(),
            "off".into(),
            "--".into(),
            "run the fixture".into(),
        ];

        assert_eq!(headless_arguments(&artifacts, &roots), expected);
    }

    #[test]
    fn scenario_environment_is_closed_and_contains_only_isolated_values() {
        let roots = IsolatedRoots::new().expect("create isolated roots");
        let artifacts = artifact_paths();
        let environment =
            scenario_environment(&artifacts, &roots).expect("create isolated scenario environment");

        let expected = "HARNESS_EVENTS_BIN HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS HARNESS_EVENTS_TIMEOUT_MS HOME III_NAMESPACE III_URL NODE_COMPILE_CACHE PI_CODING_AGENT_DIR PI_CODING_AGENT_SESSION_DIR PI_OFFLINE PI_SKIP_VERSION_CHECK PI_TELEMETRY TEMP TMP TMPDIR XDG_CACHE_HOME XDG_CONFIG_HOME XDG_DATA_HOME XDG_STATE_HOME npm_config_cache npm_config_offline".split_whitespace().collect::<BTreeSet<_>>();
        assert_eq!(
            environment
                .keys()
                .map(|key| key.as_str())
                .collect::<BTreeSet<_>>(),
            expected
        );
        assert_eq!(
            environment.get("HARNESS_EVENTS_BIN"),
            Some(&artifacts.harness_events.into_os_string())
        );
        assert_eq!(
            environment.get("HARNESS_EVENTS_TIMEOUT_MS"),
            Some(&"2000".into())
        );
        assert_eq!(
            environment.get("HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS"),
            Some(&"2000".into())
        );
        assert_eq!(environment.get("III_URL"), Some(&"ws://127.0.0.1:1".into()));
        assert_eq!(
            environment.get("III_NAMESPACE"),
            Some(&"pi-harness-events-e2e-closed-loopback".into())
        );

        for key in [
            "HOME",
            "TMPDIR",
            "TMP",
            "TEMP",
            "XDG_CACHE_HOME",
            "XDG_CONFIG_HOME",
            "XDG_DATA_HOME",
            "XDG_STATE_HOME",
            "PI_CODING_AGENT_DIR",
            "PI_CODING_AGENT_SESSION_DIR",
            "NODE_COMPILE_CACHE",
            "npm_config_cache",
        ] {
            let value = environment.get(key).expect("isolated path is configured");
            assert!(
                PathBuf::from(value).starts_with(&roots.root),
                "{key} must stay under the isolated root"
            );
        }
        assert_eq!(environment.get("npm_config_offline"), Some(&"true".into()));
        assert_eq!(environment.get("PI_OFFLINE"), Some(&"1".into()));
        assert_eq!(environment.get("PI_SKIP_VERSION_CHECK"), Some(&"1".into()));
        assert_eq!(environment.get("PI_TELEMETRY"), Some(&"0".into()));
    }

    #[test]
    fn recognizes_the_pinned_runtime_versions() {
        assert!(is_node_24(OsStr::new("v24.12.0")));
        assert!(!is_node_24(OsStr::new("v26.0.0")));
        assert!(is_pi_086_1(OsStr::new("0.86.1")));
        assert!(!is_pi_086_1(OsStr::new("0.86.2")));
    }

    #[test]
    fn rejects_counterfeit_harness_events_help() {
        let counterfeit = "Usage: harness-events <COMMAND>\nsession-start observation session-end";

        assert!(!is_harness_events_help(OsStr::new(counterfeit)));
    }

    #[test]
    fn canonicalizes_every_artifact_path() {
        let (roots, mut artifacts) = artifact_fixture();
        let linked_iii = roots.path("iii-link");
        symlink(&artifacts.iii, &linked_iii).expect("link iii artifact");
        artifacts.iii = linked_iii;

        let resolved = resolve_artifacts(&artifacts).expect("resolve artifact paths");

        for (actual, source) in [
            (&resolved.node, &artifacts.node),
            (&resolved.pi_cli, &artifacts.pi_cli),
            (&resolved.iii, &artifacts.iii),
            (&resolved.harness_events, &artifacts.harness_events),
            (&resolved.extension_root, &artifacts.extension_root),
            (&resolved.scripted_provider, &artifacts.scripted_provider),
        ] {
            assert_eq!(
                actual,
                &fs::canonicalize(source).expect("canonical artifact path")
            );
        }
    }

    #[test]
    fn rejects_nonexecutable_runtime_artifacts() {
        for role in ["node", "iii", "harness-events"] {
            let (_roots, artifacts) = artifact_fixture();
            let path = match role {
                "node" => &artifacts.node,
                "iii" => &artifacts.iii,
                "harness-events" => &artifacts.harness_events,
                _ => unreachable!(),
            };
            set_mode(path, 0o600);

            assert_eq!(
                resolve_artifacts(&artifacts),
                Err(PiScenarioError::InvalidArtifact),
                "{role} must be executable"
            );
        }
    }

    #[test]
    fn accepts_the_scripted_fixture_activity_subsequence() {
        let jsonl = fixture_activity_jsonl();
        let events = parse_jsonl(jsonl.as_bytes()).expect("parse Pi JSONL output");

        assert_expected_activity(&events).expect("fixture activity must be complete");
    }

    #[test]
    fn rejects_activity_without_the_fixture_tool_result() {
        let jsonl = fixture_activity_jsonl();
        let mut events = parse_jsonl(jsonl.as_bytes()).expect("parse Pi JSONL output");
        events.retain(|event| {
            event
                .get("message")
                .and_then(|message| message.get("role"))
                .and_then(Value::as_str)
                != Some("toolResult")
        });

        assert_eq!(
            assert_expected_activity(&events),
            Err(PiScenarioError::MissingActivity)
        );
    }

    fn fixture_activity_jsonl() -> String {
        [
            json!({"type": "session", "version": 3}),
            json!({"type": "agent_start"}),
            json!({"type": "turn_start"}),
            json!({"type": "message_end", "message": {"role": "user", "content": [{"type": "text", "text": "run the fixture"}]}}),
            json!({"type": "message_end", "message": {"role": "assistant", "content": [{"type": "toolCall", "name": "fixture_tool"}]}}),
            json!({"type": "tool_execution_start", "toolName": "fixture_tool"}),
            json!({"type": "tool_execution_end", "toolName": "fixture_tool", "isError": false}),
            json!({"type": "message_end", "message": {"role": "toolResult", "toolName": "fixture_tool", "isError": false, "content": [{"type": "text", "text": "fixture tool result"}]}}),
            json!({"type": "turn_end"}),
            json!({"type": "turn_start"}),
            json!({"type": "message_end", "message": {"role": "assistant", "content": [{"type": "text", "text": "fixture complete"}]}}),
            json!({"type": "turn_end"}),
            json!({"type": "agent_end"}),
            json!({"type": "agent_settled"}),
        ]
        .into_iter()
        .map(|event| event.to_string())
        .collect::<Vec<_>>()
        .join("\n")
    }

    fn artifact_paths() -> ArtifactPaths {
        ArtifactPaths {
            node: PathBuf::from("/artifacts/node"),
            pi_cli: PathBuf::from(
                "/artifacts/node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js",
            ),
            iii: PathBuf::from("/artifacts/iii"),
            harness_events: PathBuf::from("/artifacts/harness-events"),
            extension_root: PathBuf::from("/staged/node_modules/@dwsr/pi-harness-events"),
            scripted_provider: PathBuf::from("/fixtures/test/fixtures/scripted-provider.ts"),
        }
    }

    fn artifact_fixture() -> (IsolatedRoots, ArtifactPaths) {
        let roots = IsolatedRoots::new().expect("create artifact fixture root");
        let pi_root = roots.path("pi");
        let pi_cli = write_file(
            &pi_root.join("dist/bundle/cli.js"),
            "console.log('fixture');\n",
        );
        fs::write(
            pi_root.join("package.json"),
            r#"{"name":"@earendil-works/pi-coding-agent","version":"0.86.1"}"#,
        )
        .expect("write Pi manifest");
        let extension_root = roots.path("staged/node_modules/@dwsr/pi-harness-events");
        fs::create_dir_all(&extension_root).expect("create staged extension root");

        let artifacts = ArtifactPaths {
            node: write_executable(&roots.path("node")),
            pi_cli,
            iii: write_executable(&roots.path("iii")),
            harness_events: write_executable(&roots.path("harness-events")),
            extension_root,
            scripted_provider: write_file(
                &roots.path("test/fixtures/scripted-provider.ts"),
                "export default () => {};\n",
            ),
        };
        (roots, artifacts)
    }

    fn write_executable(path: &Path) -> PathBuf {
        let path = write_file(path, "fixture\n");
        set_mode(&path, 0o700);
        path
    }

    fn write_file(path: &Path, contents: &str) -> PathBuf {
        fs::create_dir_all(path.parent().expect("fixture file parent"))
            .expect("create fixture file parent");
        fs::write(path, contents).expect("write fixture file");
        path.to_owned()
    }

    fn set_mode(path: &Path, mode: u32) {
        let mut permissions = fs::metadata(path)
            .expect("read fixture permissions")
            .permissions();
        permissions.set_mode(mode);
        fs::set_permissions(path, permissions).expect("set fixture permissions");
    }
}
