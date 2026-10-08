use std::{
    env, fs, io,
    net::TcpListener,
    path::{Path, PathBuf},
    time::Duration,
};

use futures_util::StreamExt;
use reqwest::{Client, Method, Request, StatusCode, header};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::json;
use thiserror::Error;
use tokio::time::{Instant, sleep, timeout_at};
use uuid::Uuid;

use crate::{
    ArtifactPaths, CaptureSet, EngineSession, Generation, ProcessError, ProcessGuard,
    ProcessOutput, ProcessPhase, ProcessSpec,
};

pub const OPENCODE_V1_VERSION: &str = "1.18.29";
pub const OPENCODE_V2_VERSION: &str = "2.0.10";

const BUN_PROGRAM: &str = "bun";
const BUN_INSTALL_TIMEOUT: Duration = Duration::from_secs(30);
const READINESS_POLL_INTERVAL: Duration = Duration::from_millis(25);
const READINESS_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);
const CAPTURE_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const CAPTURE_DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(50);
const MAX_API_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_LOG_BYTES: usize = 16 * 1024;
const SERVER_USERNAME: &str = "opencode-e2e";
const V2_SERVER_USERNAME: &str = "opencode";
const V2_PLUGIN_ID: &str = "opencode-harness-events";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenCodeScenario {
    generation: Generation,
    executable: PathBuf,
    harness_events: PathBuf,
    plugin_tarball: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenCodeFileOperation {
    CreateFixture,
    CreateDirectory,
    WriteConsumerManifest,
    WriteConfig,
    ReadInstalledPlugin,
    ReserveLoopbackPort,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenCodeApiOperation {
    Health,
    PluginState,
    CreateSession,
    DeleteSession,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenCodeApiFailure {
    ClientBuild,
    RequestBuild,
    Connection,
    Timeout,
    Transport,
    Body,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum OpenCodeError {
    #[error("OpenCode {generation:?} file operation {operation:?} failed ({kind:?})")]
    File {
        generation: Generation,
        operation: OpenCodeFileOperation,
        kind: io::ErrorKind,
    },
    #[error("OpenCode {generation:?} process failed: {source}")]
    Process {
        generation: Generation,
        source: ProcessError,
    },
    #[error(
        "OpenCode {generation:?} child exited during {phase:?} (status {status:?}, signal {signal:?})"
    )]
    ChildExited {
        generation: Generation,
        phase: ProcessPhase,
        status: Option<i32>,
        signal: Option<i32>,
    },
    #[error("{source}; child {child:?}; output {output:?}; cleanup error {cleanup_error:?}")]
    ScenarioFailure {
        source: Box<OpenCodeError>,
        child: OpenCodeChildState,
        output: Option<OpenCodeOutputDiagnostics>,
        cleanup_error: Option<ProcessError>,
    },
    #[error("OpenCode {generation:?} API {operation:?} failed ({kind:?})")]
    Api {
        generation: Generation,
        operation: OpenCodeApiOperation,
        kind: OpenCodeApiFailure,
    },
    #[error("OpenCode {generation:?} API {operation:?} returned HTTP status {status}")]
    HttpStatus {
        generation: Generation,
        operation: OpenCodeApiOperation,
        status: u16,
    },
    #[error("OpenCode {generation:?} API {operation:?} response was malformed")]
    MalformedResponse {
        generation: Generation,
        operation: OpenCodeApiOperation,
    },
    #[error("OpenCode {generation:?} API {operation:?} response exceeded the bounded body size")]
    ResponseTooLarge {
        generation: Generation,
        operation: OpenCodeApiOperation,
    },
    #[error("OpenCode {generation:?} health response was not healthy")]
    Unhealthy { generation: Generation },
    #[error("OpenCode {generation:?} version did not match {expected:?}; received {actual:?}")]
    VersionMismatch {
        generation: Generation,
        expected: &'static str,
        actual: String,
    },
    #[error("OpenCode {generation:?} create response contained no session identity")]
    EmptySessionIdentity { generation: Generation },
    #[error("OpenCode {generation:?} delete response was not acknowledged")]
    DeleteNotAcknowledged { generation: Generation },
    #[error("OpenCode {generation:?} packed plugin was not installed in the consumer")]
    PluginNotInstalled { generation: Generation },
    #[error("OpenCode {generation:?} Bun installation exited with status {status:?}")]
    InstallExit {
        generation: Generation,
        status: Option<i32>,
    },
    #[error("OpenCode {generation:?} config serialization failed")]
    ConfigSerialization { generation: Generation },
    #[error("OpenCode {generation:?} capture did not drain before the bounded deadline")]
    CaptureDrainTimeout { generation: Generation },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenCodeChildLogs {
    pub stdout: String,
    pub stderr: String,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenCodeChildState {
    Running,
    Exited {
        status: Option<i32>,
        signal: Option<i32>,
    },
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OpenCodeOutputDiagnostics {
    pub status: Option<i32>,
    pub stdout_bytes: usize,
    pub stderr_bytes: usize,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

#[derive(Debug)]
pub struct OpenCodeScenarioResult {
    pub version: String,
    pub native_session_id: String,
    pub fixture_directory: PathBuf,
    pub project_directory: PathBuf,
    pub logs: OpenCodeChildLogs,
    _workspace: ScenarioWorkspace,
}

pub type OpenCodeResult = OpenCodeScenarioResult;

#[derive(Clone, Debug, Eq, PartialEq)]
struct ScenarioRoots {
    fixture_directory: PathBuf,
    home: PathBuf,
    xdg_data: PathBuf,
    xdg_config: PathBuf,
    xdg_cache: PathBuf,
    xdg_state: PathBuf,
    config: PathBuf,
    project: PathBuf,
    consumer: PathBuf,
    bun_cache: PathBuf,
    tmp: PathBuf,
    database: PathBuf,
}

impl ScenarioRoots {
    fn from_fixture(fixture_directory: PathBuf) -> Self {
        Self {
            home: fixture_directory.join("home"),
            xdg_data: fixture_directory.join("xdg-data"),
            xdg_config: fixture_directory.join("xdg-config"),
            xdg_cache: fixture_directory.join("xdg-cache"),
            xdg_state: fixture_directory.join("xdg-state"),
            config: fixture_directory.join("config"),
            project: fixture_directory.join("project"),
            consumer: fixture_directory.join("consumer"),
            bun_cache: fixture_directory.join("bun-cache"),
            tmp: fixture_directory.join("tmp"),
            database: fixture_directory.join("xdg-state").join("opencode.db"),
            fixture_directory,
        }
    }
}

#[derive(Debug)]
struct ScenarioWorkspace {
    roots: ScenarioRoots,
}

impl ScenarioWorkspace {
    fn create(generation: Generation) -> Result<Self, OpenCodeError> {
        let base = env::temp_dir();
        let mut fixture_directory = None;
        for _ in 0..8 {
            let candidate = base.join(format!(
                "opencode-harness-events-opencode-{}-{}",
                std::process::id(),
                Uuid::new_v4().simple()
            ));
            match fs::create_dir(&candidate) {
                Ok(()) => {
                    fixture_directory = Some(candidate);
                    break;
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => {
                    return Err(OpenCodeError::File {
                        generation,
                        operation: OpenCodeFileOperation::CreateFixture,
                        kind: error.kind(),
                    });
                }
            }
        }

        let fixture_directory = fixture_directory.ok_or(OpenCodeError::File {
            generation,
            operation: OpenCodeFileOperation::CreateFixture,
            kind: io::ErrorKind::AlreadyExists,
        })?;
        let roots = ScenarioRoots::from_fixture(fixture_directory);
        let config_node_modules = roots.config.join("node_modules");
        let xdg_config_node_modules = roots.xdg_config.join("opencode/node_modules");
        let directories = [
            roots.home.as_path(),
            roots.xdg_data.as_path(),
            roots.xdg_config.as_path(),
            roots.xdg_cache.as_path(),
            roots.xdg_state.as_path(),
            roots.config.as_path(),
            roots.project.as_path(),
            roots.consumer.as_path(),
            roots.bun_cache.as_path(),
            roots.tmp.as_path(),
            config_node_modules.as_path(),
            xdg_config_node_modules.as_path(),
        ];
        for directory in directories {
            if let Err(error) = fs::create_dir_all(directory) {
                let _ = fs::remove_dir_all(&roots.fixture_directory);
                return Err(OpenCodeError::File {
                    generation,
                    operation: OpenCodeFileOperation::CreateDirectory,
                    kind: error.kind(),
                });
            }
        }

        Ok(Self { roots })
    }
}

impl Drop for ScenarioWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.roots.fixture_directory);
    }
}

impl OpenCodeScenario {
    pub fn new(generation: Generation, artifacts: &ArtifactPaths) -> Self {
        let executable = match generation {
            Generation::V1 => artifacts.opencode_v1.clone(),
            Generation::V2 => artifacts.opencode_v2.clone(),
        };

        Self {
            generation,
            executable,
            harness_events: artifacts.harness_events.clone(),
            plugin_tarball: artifacts.plugin_tarball.clone(),
        }
    }

    pub fn generation(&self) -> Generation {
        self.generation
    }

    pub fn executable(&self) -> &Path {
        &self.executable
    }

    pub fn harness_events(&self) -> &Path {
        &self.harness_events
    }

    pub fn plugin_tarball(&self) -> &Path {
        &self.plugin_tarball
    }

    pub async fn run(
        &self,
        engine: &EngineSession,
    ) -> Result<OpenCodeScenarioResult, OpenCodeError> {
        let workspace = ScenarioWorkspace::create(self.generation)?;
        let credentials =
            credentials_for_generation(self.generation, Uuid::new_v4().simple().to_string());
        let installed_plugin = self.install_plugin(&workspace, engine).await?;
        let _config_path =
            write_plugin_config(&workspace.roots, &installed_plugin, self.generation)?;
        let (port_reservation, port) = reserve_loopback_port(self.generation)?;
        let environment = runtime_environment(
            &workspace.roots,
            engine,
            self.harness_events.as_path(),
            &credentials,
            self.generation,
        );
        let launch = self.launch_spec(&workspace.roots, port, environment);
        let client = build_http_client(self.generation)?;
        let process = ProcessGuard::spawn(launch)
            .await
            .map_err(|source| self.process_error(source))?;
        drop(port_reservation);

        let base_url = format!("http://127.0.0.1:{port}");
        let receive_index = engine.capture().records.len();
        let mut process = Some(process);
        let outcome = async {
            let process_ref = process
                .as_mut()
                .expect("OpenCode process remains owned during the scenario");
            let version = wait_for_readiness(
                &client,
                &base_url,
                &workspace.roots.project,
                &credentials,
                process_ref,
                self.generation,
            )
            .await?;
            if self.generation == Generation::V2 {
                wait_for_v2_plugin_activation(
                    &client,
                    &base_url,
                    &workspace.roots.project,
                    &credentials,
                    process_ref,
                )
                .await?;
            }
            let session_id = create_session(
                &client,
                &base_url,
                &workspace.roots.project,
                &credentials,
                process_ref,
                self.generation,
            )
            .await?;
            delete_session(
                &client,
                &base_url,
                &workspace.roots.project,
                &credentials,
                &session_id,
                process_ref,
                self.generation,
            )
            .await?;
            wait_for_capture_lifecycle(engine, receive_index, self.generation, &session_id).await?;
            Ok::<(String, String), OpenCodeError>((version, session_id))
        }
        .await;

        let child_state = process
            .as_mut()
            .map(observe_child)
            .transpose()
            .ok()
            .flatten()
            .unwrap_or(OpenCodeChildState::Unknown);
        let process_output = match process.take() {
            Some(process) => process.terminate().await,
            None => Ok(ProcessOutput::default()),
        };

        match (outcome, process_output) {
            (Err(error), output) => Err(scenario_failure(error, child_state, output)),
            (Ok(_), Err(source)) => Err(self.process_error(source)),
            (Ok((version, native_session_id)), Ok(output)) => Ok(OpenCodeScenarioResult {
                version,
                native_session_id,
                fixture_directory: workspace.roots.fixture_directory.clone(),
                project_directory: workspace.roots.project.clone(),
                logs: OpenCodeChildLogs::from(output),
                _workspace: workspace,
            }),
        }
    }

    async fn install_plugin(
        &self,
        workspace: &ScenarioWorkspace,
        engine: &EngineSession,
    ) -> Result<PathBuf, OpenCodeError> {
        let manifest_path = workspace.roots.consumer.join("package.json");
        fs::write(
            &manifest_path,
            br#"{"name":"opencode-harness-events-e2e-consumer","private":true,"type":"module"}"#,
        )
        .map_err(|error| OpenCodeError::File {
            generation: self.generation,
            operation: OpenCodeFileOperation::WriteConsumerManifest,
            kind: error.kind(),
        })?;

        let install =
            self.install_spec(&workspace.roots, bun_environment(&workspace.roots, engine));
        let install = ProcessGuard::spawn(install)
            .await
            .map_err(|source| self.process_error(source))?;
        let output = install
            .wait()
            .await
            .map_err(|source| self.process_error(source))?;
        if output.status_code != Some(0) {
            return Err(OpenCodeError::InstallExit {
                generation: self.generation,
                status: output.status_code,
            });
        }

        let installed = workspace
            .roots
            .consumer
            .join("node_modules")
            .join("opencode-harness-events");
        let metadata = fs::metadata(&installed).map_err(|error| OpenCodeError::File {
            generation: self.generation,
            operation: OpenCodeFileOperation::ReadInstalledPlugin,
            kind: error.kind(),
        })?;
        if !metadata.is_dir() {
            return Err(OpenCodeError::PluginNotInstalled {
                generation: self.generation,
            });
        }

        fs::canonicalize(installed).map_err(|error| OpenCodeError::File {
            generation: self.generation,
            operation: OpenCodeFileOperation::ReadInstalledPlugin,
            kind: error.kind(),
        })
    }

    fn install_spec(
        &self,
        roots: &ScenarioRoots,
        environment: Vec<(std::ffi::OsString, std::ffi::OsString)>,
    ) -> ProcessSpec {
        let mut install = ProcessSpec::new(PathBuf::from(BUN_PROGRAM));
        install.arguments = vec![
            "add".into(),
            "--offline".into(),
            "--no-save".into(),
            "--no-progress".into(),
            "--ignore-scripts".into(),
            "--linker".into(),
            "hoisted".into(),
            "--cache-dir".into(),
            roots.bun_cache.as_os_str().to_os_string(),
            self.plugin_tarball.as_os_str().to_os_string(),
        ];
        install.environment = environment;
        install.clear_environment = true;
        install.current_directory = Some(roots.consumer.clone());
        install.timeout = BUN_INSTALL_TIMEOUT;
        install
    }

    fn launch_spec(
        &self,
        roots: &ScenarioRoots,
        port: u16,
        environment: Vec<(std::ffi::OsString, std::ffi::OsString)>,
    ) -> ProcessSpec {
        let mut launch = ProcessSpec::new(self.executable.clone());
        launch.arguments = vec![
            "serve".into(),
            "--hostname".into(),
            "127.0.0.1".into(),
            "--port".into(),
            port.to_string().into(),
        ];
        if self.generation == Generation::V1 {
            launch
                .arguments
                .extend(["--log-level".into(), "ERROR".into()]);
        }
        launch.environment = environment;
        launch.clear_environment = true;
        launch.current_directory = Some(roots.project.clone());
        launch
    }

    fn process_error(&self, source: ProcessError) -> OpenCodeError {
        OpenCodeError::Process {
            generation: self.generation,
            source,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Credentials {
    username: &'static str,
    password: String,
}

fn credentials_for_generation(generation: Generation, password: String) -> Credentials {
    Credentials {
        username: match generation {
            Generation::V1 => SERVER_USERNAME,
            Generation::V2 => V2_SERVER_USERNAME,
        },
        password,
    }
}

fn bun_environment(
    roots: &ScenarioRoots,
    engine: &EngineSession,
) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    vec![
        ("HOME".into(), roots.home.as_os_str().to_os_string()),
        ("TMPDIR".into(), roots.tmp.as_os_str().to_os_string()),
        (
            "XDG_DATA_HOME".into(),
            roots.xdg_data.as_os_str().to_os_string(),
        ),
        (
            "XDG_CONFIG_HOME".into(),
            roots.xdg_config.as_os_str().to_os_string(),
        ),
        (
            "XDG_CACHE_HOME".into(),
            roots.xdg_cache.as_os_str().to_os_string(),
        ),
        (
            "XDG_STATE_HOME".into(),
            roots.xdg_state.as_os_str().to_os_string(),
        ),
        ("III_URL".into(), engine.engine_url().into()),
        (
            "III_NAMESPACE".into(),
            engine.identity().namespace.clone().into(),
        ),
    ]
}

fn runtime_environment(
    roots: &ScenarioRoots,
    engine: &EngineSession,
    harness_events: &Path,
    credentials: &Credentials,
    generation: Generation,
) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    let mut environment = vec![
        ("HOME".into(), roots.home.as_os_str().to_os_string()),
        (
            "OPENCODE_TEST_HOME".into(),
            roots.home.as_os_str().to_os_string(),
        ),
        ("TMPDIR".into(), roots.tmp.as_os_str().to_os_string()),
        (
            "XDG_DATA_HOME".into(),
            roots.xdg_data.as_os_str().to_os_string(),
        ),
        (
            "XDG_CONFIG_HOME".into(),
            roots.xdg_config.as_os_str().to_os_string(),
        ),
        (
            "XDG_CACHE_HOME".into(),
            roots.xdg_cache.as_os_str().to_os_string(),
        ),
        (
            "XDG_STATE_HOME".into(),
            roots.xdg_state.as_os_str().to_os_string(),
        ),
        (
            "OPENCODE_CONFIG_DIR".into(),
            roots.config.as_os_str().to_os_string(),
        ),
        (
            "OPENCODE_DB".into(),
            roots.database.as_os_str().to_os_string(),
        ),
    ];
    match generation {
        Generation::V1 => environment.extend([
            ("OPENCODE_DISABLE_MODELS_FETCH".into(), "1".into()),
            ("OPENCODE_DISABLE_AUTOUPDATE".into(), "1".into()),
            ("OPENCODE_DISABLE_LSP_DOWNLOAD".into(), "1".into()),
            (
                "OPENCODE_EXPERIMENTAL_DISABLE_FILEWATCHER".into(),
                "1".into(),
            ),
            ("OPENCODE_DISABLE_PROJECT_CONFIG".into(), "1".into()),
            ("OPENCODE_DISABLE_DEFAULT_PLUGINS".into(), "1".into()),
            (
                "OPENCODE_SERVER_USERNAME".into(),
                credentials.username.into(),
            ),
        ]),
        Generation::V2 => environment.extend([
            ("OPENCODE_DISABLE_MODELS_FETCH".into(), "1".into()),
            ("OPENCODE_DISABLE_AUTOUPDATE".into(), "1".into()),
            ("OPENCODE_DISABLE_FILEWATCHER".into(), "1".into()),
            ("OPENCODE_DISABLE_PROJECT_CONFIG".into(), "1".into()),
        ]),
    }
    environment.extend([
        (
            "OPENCODE_SERVER_PASSWORD".into(),
            credentials.password.clone().into(),
        ),
        ("III_URL".into(), engine.engine_url().into()),
        (
            "III_NAMESPACE".into(),
            engine.identity().namespace.clone().into(),
        ),
        (
            "HARNESS_EVENTS_BIN".into(),
            harness_events.as_os_str().to_os_string(),
        ),
    ]);
    environment
}

fn write_plugin_config(
    roots: &ScenarioRoots,
    installed_plugin: &Path,
    generation: Generation,
) -> Result<PathBuf, OpenCodeError> {
    let config_path = roots.config.join("opencode.json");
    let value = match generation {
        Generation::V1 => json!({
            "plugin": [installed_plugin.to_string_lossy()],
            "lsp": false,
        }),
        Generation::V2 => json!({
            "plugins": [installed_plugin.join("dist").to_string_lossy()],
            "lsp": false,
        }),
    };
    let content = serde_json::to_vec_pretty(&value)
        .map_err(|_| OpenCodeError::ConfigSerialization { generation })?;
    fs::write(&config_path, content).map_err(|error| OpenCodeError::File {
        generation,
        operation: OpenCodeFileOperation::WriteConfig,
        kind: error.kind(),
    })?;
    Ok(config_path)
}

fn reserve_loopback_port(generation: Generation) -> Result<(TcpListener, u16), OpenCodeError> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).map_err(|error| OpenCodeError::File {
        generation,
        operation: OpenCodeFileOperation::ReserveLoopbackPort,
        kind: error.kind(),
    })?;
    let port = listener
        .local_addr()
        .map_err(|error| OpenCodeError::File {
            generation,
            operation: OpenCodeFileOperation::ReserveLoopbackPort,
            kind: error.kind(),
        })?
        .port();
    Ok((listener, port))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ApiRequestKind {
    Health,
    PluginState,
    CreateSession,
    DeleteSession,
}

impl ApiRequestKind {
    const fn operation(self) -> OpenCodeApiOperation {
        match self {
            Self::Health => OpenCodeApiOperation::Health,
            Self::PluginState => OpenCodeApiOperation::PluginState,
            Self::CreateSession => OpenCodeApiOperation::CreateSession,
            Self::DeleteSession => OpenCodeApiOperation::DeleteSession,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ApiRequestSpec {
    kind: ApiRequestKind,
    url: String,
    directory: Option<PathBuf>,
    body: Option<&'static str>,
}

fn api_request(
    kind: ApiRequestKind,
    base_url: &str,
    project: &Path,
    session_id: Option<&str>,
    generation: Generation,
) -> ApiRequestSpec {
    let v2 = generation == Generation::V2;
    let (path, body, directory) = match kind {
        ApiRequestKind::Health => (
            if v2 { "/api/info" } else { "/global/health" }.to_owned(),
            None,
            None,
        ),
        ApiRequestKind::PluginState => ("/api/plugin".to_owned(), None, Some(project)),
        ApiRequestKind::CreateSession => (
            if v2 { "/api/session" } else { "/session" }.to_owned(),
            Some("{}"),
            Some(project),
        ),
        ApiRequestKind::DeleteSession => (
            format!(
                "{}/{}",
                if v2 { "/api/session" } else { "/session" },
                session_id.unwrap_or_default()
            ),
            None,
            Some(project),
        ),
    };
    ApiRequestSpec {
        kind,
        url: format!("{base_url}{path}"),
        directory: directory.map(Path::to_path_buf),
        body,
    }
}

fn build_request(
    client: &Client,
    spec: &ApiRequestSpec,
    credentials: &Credentials,
    generation: Generation,
) -> Result<Request, OpenCodeError> {
    let method = match spec.kind {
        ApiRequestKind::Health | ApiRequestKind::PluginState => Method::GET,
        ApiRequestKind::CreateSession => Method::POST,
        ApiRequestKind::DeleteSession => Method::DELETE,
    };
    let mut request = client
        .request(method, &spec.url)
        .basic_auth(credentials.username, Some(&credentials.password));
    if let Some(directory) = &spec.directory {
        request = request.header(
            "x-opencode-directory",
            directory.to_string_lossy().into_owned(),
        );
    }
    if let Some(body) = spec.body {
        request = request
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.to_owned());
    }
    request.build().map_err(|_| OpenCodeError::Api {
        generation,
        operation: spec.kind.operation(),
        kind: OpenCodeApiFailure::RequestBuild,
    })
}

fn build_http_client(generation: Generation) -> Result<Client, OpenCodeError> {
    Client::builder()
        .no_proxy()
        .build()
        .map_err(|_| OpenCodeError::Api {
            generation,
            operation: OpenCodeApiOperation::Health,
            kind: OpenCodeApiFailure::ClientBuild,
        })
}

async fn execute_json<T>(
    client: &Client,
    request: Request,
    deadline: Instant,
    generation: Generation,
    operation: OpenCodeApiOperation,
) -> Result<T, OpenCodeError>
where
    T: DeserializeOwned,
{
    let response = timeout_at(deadline, async {
        let response = client
            .execute(request)
            .await
            .map_err(|error| OpenCodeError::Api {
                generation,
                operation,
                kind: if error.is_connect() {
                    OpenCodeApiFailure::Connection
                } else {
                    OpenCodeApiFailure::Transport
                },
            })?;
        let status = response.status();
        let mut stream = response.bytes_stream();
        let mut body = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| OpenCodeError::Api {
                generation,
                operation,
                kind: OpenCodeApiFailure::Body,
            })?;
            if body.len().saturating_add(chunk.len()) > MAX_API_RESPONSE_BYTES {
                return Err(OpenCodeError::ResponseTooLarge {
                    generation,
                    operation,
                });
            }
            body.extend_from_slice(&chunk);
        }
        Ok::<(StatusCode, Vec<u8>), OpenCodeError>((status, body))
    })
    .await
    .map_err(|_| OpenCodeError::Api {
        generation,
        operation,
        kind: OpenCodeApiFailure::Timeout,
    })??;

    let (status, body) = response;
    if status != StatusCode::OK {
        return Err(OpenCodeError::HttpStatus {
            generation,
            operation,
            status: status.as_u16(),
        });
    }
    serde_json::from_slice(&body).map_err(|_| OpenCodeError::MalformedResponse {
        generation,
        operation,
    })
}

async fn execute_status(
    client: &Client,
    request: Request,
    deadline: Instant,
    generation: Generation,
    operation: OpenCodeApiOperation,
) -> Result<StatusCode, OpenCodeError> {
    timeout_at(deadline, async {
        client
            .execute(request)
            .await
            .map(|response| response.status())
            .map_err(|error| OpenCodeError::Api {
                generation,
                operation,
                kind: if error.is_connect() {
                    OpenCodeApiFailure::Connection
                } else {
                    OpenCodeApiFailure::Transport
                },
            })
    })
    .await
    .map_err(|_| OpenCodeError::Api {
        generation,
        operation,
        kind: OpenCodeApiFailure::Timeout,
    })?
}

#[derive(Debug, Deserialize)]
struct HealthResponse {
    healthy: bool,
    version: String,
}

#[derive(Debug, Deserialize)]
struct SessionResponse {
    id: String,
}

#[derive(Debug, Deserialize)]
struct V2InfoResponse {
    version: String,
    #[serde(rename = "pid")]
    _pid: u32,
    #[serde(rename = "urls")]
    _urls: Vec<String>,
    #[serde(rename = "paths")]
    _paths: V2InfoPaths,
}

#[derive(Debug, Deserialize)]
struct V2InfoPaths {
    #[serde(rename = "tmp")]
    _tmp: String,
}

#[derive(Debug, Deserialize)]
struct V2PluginListResponse {
    data: Vec<V2PluginInfo>,
}

#[derive(Debug, Deserialize)]
struct V2PluginInfo {
    id: Option<String>,
    state: V2PluginState,
}

#[derive(Debug, Deserialize)]
struct V2PluginState {
    status: String,
}

#[derive(Debug, Deserialize)]
struct V2SessionResponse {
    data: V2SessionInfo,
}

#[derive(Debug, Deserialize)]
struct V2SessionInfo {
    id: String,
}

async fn read_version(
    client: &Client,
    request: Request,
    deadline: Instant,
    generation: Generation,
) -> Result<String, OpenCodeError> {
    match generation {
        Generation::V1 => {
            let response = execute_json::<HealthResponse>(
                client,
                request,
                deadline,
                generation,
                OpenCodeApiOperation::Health,
            )
            .await?;
            if !response.healthy {
                return Err(OpenCodeError::Unhealthy { generation });
            }
            Ok(response.version)
        }
        Generation::V2 => Ok(execute_json::<V2InfoResponse>(
            client,
            request,
            deadline,
            generation,
            OpenCodeApiOperation::Health,
        )
        .await?
        .version),
    }
}

fn readiness_attempt_deadline(now: Instant, deadline: Instant) -> Instant {
    deadline.min(now + READINESS_ATTEMPT_TIMEOUT)
}

fn observe_child(process: &mut ProcessGuard) -> Result<OpenCodeChildState, ProcessError> {
    let Some(status) = process.try_wait()? else {
        return Ok(OpenCodeChildState::Running);
    };
    #[cfg(unix)]
    let signal = {
        use std::os::unix::process::ExitStatusExt;
        status.signal()
    };
    #[cfg(not(unix))]
    let signal = None;
    Ok(OpenCodeChildState::Exited {
        status: status.code(),
        signal,
    })
}

fn ensure_child_running(
    process: &mut ProcessGuard,
    generation: Generation,
) -> Result<(), OpenCodeError> {
    let state =
        observe_child(process).map_err(|source| OpenCodeError::Process { generation, source })?;
    if let OpenCodeChildState::Exited { status, signal } = state {
        return Err(OpenCodeError::ChildExited {
            generation,
            phase: ProcessPhase::Readiness,
            status,
            signal,
        });
    }
    Ok(())
}

fn scenario_failure(
    source: OpenCodeError,
    child: OpenCodeChildState,
    cleanup: Result<ProcessOutput, ProcessError>,
) -> OpenCodeError {
    let (output, cleanup_error) = match cleanup {
        Ok(output) => (
            Some(OpenCodeOutputDiagnostics {
                status: output.status_code,
                stdout_bytes: output.stdout.len(),
                stderr_bytes: output.stderr.len(),
                stdout_truncated: output.stdout_truncated,
                stderr_truncated: output.stderr_truncated,
            }),
            None,
        ),
        Err(error) => (None, Some(error)),
    };
    OpenCodeError::ScenarioFailure {
        source: Box::new(source),
        child,
        output,
        cleanup_error,
    }
}

async fn wait_for_readiness(
    client: &Client,
    base_url: &str,
    project: &Path,
    credentials: &Credentials,
    process: &mut ProcessGuard,
    generation: Generation,
) -> Result<String, OpenCodeError> {
    let deadline = Instant::now() + process.deadline(ProcessPhase::Readiness);
    loop {
        ensure_child_running(process, generation)?;
        let request = build_request(
            client,
            &api_request(ApiRequestKind::Health, base_url, project, None, generation),
            credentials,
            generation,
        )?;
        match read_version(
            client,
            request,
            readiness_attempt_deadline(Instant::now(), deadline),
            generation,
        )
        .await
        {
            Ok(version) => {
                let expected = match generation {
                    Generation::V1 => OPENCODE_V1_VERSION,
                    Generation::V2 => OPENCODE_V2_VERSION,
                };
                if version != expected {
                    return Err(OpenCodeError::VersionMismatch {
                        generation,
                        expected,
                        actual: bounded_metadata(&version),
                    });
                }
                return Ok(version);
            }
            Err(error) if retryable_readiness_error(&error) && Instant::now() < deadline => {
                sleep(READINESS_POLL_INTERVAL).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn retryable_readiness_error(error: &OpenCodeError) -> bool {
    matches!(
        error,
        OpenCodeError::Api {
            operation: OpenCodeApiOperation::Health,
            kind: OpenCodeApiFailure::Connection
                | OpenCodeApiFailure::Timeout
                | OpenCodeApiFailure::Transport,
            ..
        } | OpenCodeError::HttpStatus {
            operation: OpenCodeApiOperation::Health,
            status: 503,
            ..
        }
    )
}

async fn wait_for_v2_plugin_activation(
    client: &Client,
    base_url: &str,
    project: &Path,
    credentials: &Credentials,
    process: &mut ProcessGuard,
) -> Result<(), OpenCodeError> {
    let generation = Generation::V2;
    let deadline = Instant::now() + process.deadline(ProcessPhase::Readiness);
    loop {
        ensure_child_running(process, generation)?;
        let request = build_request(
            client,
            &api_request(
                ApiRequestKind::PluginState,
                base_url,
                project,
                None,
                generation,
            ),
            credentials,
            generation,
        )?;
        match execute_json::<V2PluginListResponse>(
            client,
            request,
            deadline,
            generation,
            OpenCodeApiOperation::PluginState,
        )
        .await
        {
            Ok(response) if v2_plugin_is_active(&response) => return Ok(()),
            Ok(_) if Instant::now() < deadline => sleep(READINESS_POLL_INTERVAL).await,
            Ok(_) => return Err(plugin_activation_timeout_error()),
            Err(error)
                if retryable_plugin_activation_error(&error) && Instant::now() < deadline =>
            {
                sleep(READINESS_POLL_INTERVAL).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn v2_plugin_is_active(response: &V2PluginListResponse) -> bool {
    response
        .data
        .iter()
        .any(|plugin| plugin.id.as_deref() == Some(V2_PLUGIN_ID) && plugin.state.status == "active")
}

fn plugin_activation_timeout_error() -> OpenCodeError {
    OpenCodeError::Api {
        generation: Generation::V2,
        operation: OpenCodeApiOperation::PluginState,
        kind: OpenCodeApiFailure::Timeout,
    }
}

fn retryable_plugin_activation_error(error: &OpenCodeError) -> bool {
    matches!(
        error,
        OpenCodeError::Api {
            operation: OpenCodeApiOperation::PluginState,
            kind: OpenCodeApiFailure::Connection,
            ..
        } | OpenCodeError::HttpStatus {
            operation: OpenCodeApiOperation::PluginState,
            status: 503,
            ..
        }
    )
}

async fn create_session(
    client: &Client,
    base_url: &str,
    project: &Path,
    credentials: &Credentials,
    process: &ProcessGuard,
    generation: Generation,
) -> Result<String, OpenCodeError> {
    let spec = api_request(
        ApiRequestKind::CreateSession,
        base_url,
        project,
        None,
        generation,
    );
    let request = build_request(client, &spec, credentials, generation)?;
    let session_id = match generation {
        Generation::V1 => {
            execute_json::<SessionResponse>(
                client,
                request,
                Instant::now() + process.deadline(ProcessPhase::Execution),
                generation,
                OpenCodeApiOperation::CreateSession,
            )
            .await?
            .id
        }
        Generation::V2 => {
            execute_json::<V2SessionResponse>(
                client,
                request,
                Instant::now() + process.deadline(ProcessPhase::Execution),
                generation,
                OpenCodeApiOperation::CreateSession,
            )
            .await?
            .data
            .id
        }
    };
    let session_id = session_id.trim();
    if session_id.is_empty() {
        return Err(OpenCodeError::EmptySessionIdentity { generation });
    }
    Ok(session_id.to_owned())
}

async fn delete_session(
    client: &Client,
    base_url: &str,
    project: &Path,
    credentials: &Credentials,
    session_id: &str,
    process: &ProcessGuard,
    generation: Generation,
) -> Result<(), OpenCodeError> {
    let spec = api_request(
        ApiRequestKind::DeleteSession,
        base_url,
        project,
        Some(session_id),
        generation,
    );
    let request = build_request(client, &spec, credentials, generation)?;
    match generation {
        Generation::V1 => {
            let deleted = execute_json::<bool>(
                client,
                request,
                Instant::now() + process.deadline(ProcessPhase::Execution),
                generation,
                OpenCodeApiOperation::DeleteSession,
            )
            .await?;
            if !deleted {
                return Err(OpenCodeError::DeleteNotAcknowledged { generation });
            }
        }
        Generation::V2 => {
            let status = execute_status(
                client,
                request,
                Instant::now() + process.deadline(ProcessPhase::Execution),
                generation,
                OpenCodeApiOperation::DeleteSession,
            )
            .await?;
            if status != StatusCode::NO_CONTENT {
                return Err(OpenCodeError::HttpStatus {
                    generation,
                    operation: OpenCodeApiOperation::DeleteSession,
                    status: status.as_u16(),
                });
            }
        }
    }
    Ok(())
}

async fn wait_for_capture_lifecycle(
    engine: &EngineSession,
    receive_index: usize,
    generation: Generation,
    session_id: &str,
) -> Result<(), OpenCodeError> {
    let deadline = Instant::now() + CAPTURE_DRAIN_TIMEOUT;
    loop {
        let capture = engine.capture_since(receive_index);
        if capture_has_required_lifecycle(&capture, generation, session_id) {
            return Ok(());
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(OpenCodeError::CaptureDrainTimeout { generation });
        }
        sleep(remaining.min(CAPTURE_DRAIN_POLL_INTERVAL)).await;
    }
}

fn capture_has_required_lifecycle(
    capture: &CaptureSet,
    generation: Generation,
    session_id: &str,
) -> bool {
    if capture.records.len() < 4 {
        return false;
    }

    let creation_hook = format!("opencode.{}.session.created", generation.label());
    let deletion_hook = format!("opencode.{}.session.deleted", generation.label());
    let mut has_start = false;
    let mut has_creation = false;
    let mut has_deletion = false;
    let mut has_end = false;

    for record in &capture.records {
        match record.function_id.as_str() {
            "harness::session_start" => {
                has_start |= payload_string(&record.payload, "session_id") == Some(session_id);
            }
            "harness::observation" => {
                if payload_string(&record.payload, "session_id") == Some(session_id) {
                    let hook_type = payload_string(&record.payload, "hook_type");
                    has_creation |= hook_type == Some(creation_hook.as_str());
                    has_deletion |= hook_type == Some(deletion_hook.as_str());
                }
            }
            "harness::session_end" => {
                has_end |= payload_string(&record.payload, "session_id") == Some(session_id);
            }
            _ => {}
        }
    }

    has_start && has_creation && has_deletion && has_end
}

fn payload_string<'a>(payload: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    payload.get(field).and_then(serde_json::Value::as_str)
}

fn bounded_metadata(value: &str) -> String {
    value.chars().take(64).collect()
}

impl From<ProcessOutput> for OpenCodeChildLogs {
    fn from(output: ProcessOutput) -> Self {
        let (stdout, stdout_truncated) = bounded_log(&output.stdout);
        let (stderr, stderr_truncated) = bounded_log(&output.stderr);
        Self {
            stdout,
            stderr,
            stdout_truncated: output.stdout_truncated || stdout_truncated,
            stderr_truncated: output.stderr_truncated || stderr_truncated,
        }
    }
}

fn bounded_log(bytes: &[u8]) -> (String, bool) {
    let truncated = bytes.len() > MAX_LOG_BYTES;
    let bytes = &bytes[..bytes.len().min(MAX_LOG_BYTES)];
    (String::from_utf8_lossy(bytes).into_owned(), truncated)
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsString,
        fs,
        path::{Path, PathBuf},
        time::Duration,
    };

    use reqwest::Client;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::time::Instant;

    use super::{
        ApiRequestKind, Credentials, OPENCODE_V2_VERSION, OpenCodeApiFailure, OpenCodeApiOperation,
        OpenCodeChildLogs, OpenCodeError, OpenCodeFileOperation, OpenCodeScenario,
        ScenarioWorkspace, V2PluginListResponse, api_request, bounded_log, build_request,
        credentials_for_generation, retryable_plugin_activation_error, v2_plugin_is_active,
        write_plugin_config,
    };
    use crate::{
        ArtifactPaths, CaptureRecord, CaptureSet, Generation, ProcessGuard, ProcessOutput,
        ProcessPhase, ProcessSpec,
    };

    fn regular_file() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml")
    }

    fn artifacts() -> ArtifactPaths {
        let path = regular_file();
        ArtifactPaths {
            iii: path.clone(),
            opencode_v1: path.clone(),
            opencode_v2: path.clone(),
            harness_events: path.clone(),
            plugin_tarball: path,
        }
    }

    #[cfg(unix)]
    async fn readiness_child(deadline: Duration) -> ProcessGuard {
        let mut spec = ProcessSpec::new(PathBuf::from("/bin/sh"));
        spec.arguments = vec!["-c".into(), "sleep 30".into()];
        spec.deadlines.readiness = deadline;
        spec.deadlines.termination_grace = Duration::from_millis(20);
        ProcessGuard::spawn(spec).await.expect("start owned child")
    }

    #[cfg(unix)]
    async fn read_health_request(connection: &mut tokio::net::TcpStream) {
        let mut request = [0_u8; 2048];
        let mut length = 0;
        while !request[..length].ends_with(b"\r\n\r\n") {
            assert!(
                length < request.len(),
                "health request exceeded fixture bound"
            );
            let read = connection
                .read(&mut request[length..])
                .await
                .expect("read health request");
            assert!(
                read > 0,
                "health request closed before its headers completed"
            );
            length += read;
        }
        assert!(request[..length].starts_with(b"GET /global/health HTTP/1.1\r\n"));
    }

    #[test]
    fn v1_selects_the_pinned_host_artifact_and_keeps_the_other_generation_separate() {
        let scenario = OpenCodeScenario::new(Generation::V1, &artifacts());
        assert_eq!(scenario.generation(), Generation::V1);
        assert_eq!(scenario.executable(), regular_file().as_path());
        assert_eq!(scenario.harness_events(), regular_file().as_path());
        assert_eq!(scenario.plugin_tarball(), regular_file().as_path());
    }

    #[test]
    fn scenario_workspace_creates_and_removes_distinct_isolated_roots() {
        let workspace = ScenarioWorkspace::create(Generation::V1).expect("workspace creates");
        let roots = &workspace.roots;
        assert!(roots.fixture_directory.is_dir());
        assert!(roots.home.is_dir());
        assert!(roots.xdg_config.is_dir());
        assert!(roots.config.join("node_modules").is_dir());
        assert!(roots.xdg_config.join("opencode/node_modules").is_dir());
        assert_ne!(roots.home, roots.xdg_config);
        assert_ne!(roots.xdg_cache, roots.xdg_state);
        let fixture = roots.fixture_directory.clone();
        drop(workspace);
        assert!(!fixture.exists());
    }

    #[test]
    fn v1_launch_spec_uses_exact_serve_arguments_and_isolated_environment() {
        let workspace = ScenarioWorkspace::create(Generation::V1).expect("workspace creates");
        let scenario = OpenCodeScenario::new(Generation::V1, &artifacts());
        let environment = vec![
            (
                OsString::from("HOME"),
                workspace.roots.home.clone().into_os_string(),
            ),
            (
                OsString::from("OPENCODE_DISABLE_MODELS_FETCH"),
                OsString::from("1"),
            ),
            (
                OsString::from("III_URL"),
                OsString::from("ws://127.0.0.1:7"),
            ),
        ];
        let spec = scenario.launch_spec(&workspace.roots, 43123, environment.clone());
        assert_eq!(spec.program, regular_file());
        assert_eq!(
            spec.arguments,
            vec![
                OsString::from("serve"),
                OsString::from("--hostname"),
                OsString::from("127.0.0.1"),
                OsString::from("--port"),
                OsString::from("43123"),
                OsString::from("--log-level"),
                OsString::from("ERROR"),
            ]
        );
        assert_eq!(spec.environment, environment);
        assert!(spec.clear_environment);
        assert_eq!(
            spec.current_directory,
            Some(workspace.roots.project.clone())
        );
    }

    #[test]
    fn bun_install_spec_uses_the_offline_packed_tarball_and_hoisted_consumer() {
        let workspace = ScenarioWorkspace::create(Generation::V1).expect("workspace creates");
        let scenario = OpenCodeScenario::new(Generation::V1, &artifacts());
        let spec = scenario.install_spec(&workspace.roots, Vec::new());
        assert_eq!(spec.program, PathBuf::from("bun"));
        assert_eq!(
            spec.arguments,
            vec![
                OsString::from("add"),
                OsString::from("--offline"),
                OsString::from("--no-save"),
                OsString::from("--no-progress"),
                OsString::from("--ignore-scripts"),
                OsString::from("--linker"),
                OsString::from("hoisted"),
                OsString::from("--cache-dir"),
                workspace.roots.bun_cache.as_os_str().to_os_string(),
                regular_file().as_os_str().to_os_string(),
            ]
        );
        assert!(spec.clear_environment);
        assert_eq!(
            spec.current_directory,
            Some(workspace.roots.consumer.clone())
        );
    }

    #[test]
    fn plugin_config_uses_the_v1_plugin_key_and_installed_path() {
        let workspace = ScenarioWorkspace::create(Generation::V1).expect("workspace creates");
        let installed = workspace.roots.consumer.join("node_modules/plugin");
        let config = write_plugin_config(&workspace.roots, &installed, Generation::V1)
            .expect("config writes");
        let value: serde_json::Value =
            serde_json::from_slice(&fs::read(config).expect("config reads"))
                .expect("config parses");
        assert_eq!(
            value,
            json!({"plugin": [installed.to_string_lossy()], "lsp": false})
        );
    }

    #[test]
    fn api_requests_use_authenticated_v1_paths_and_project_routing() {
        let project = Path::new("/fixture/project");
        let credentials = Credentials {
            username: "opencode-e2e",
            password: "unit-password".to_owned(),
        };
        let client = Client::builder().no_proxy().build().expect("client builds");
        let create = api_request(
            ApiRequestKind::CreateSession,
            "http://127.0.0.1:43123",
            project,
            None,
            Generation::V1,
        );
        let request = build_request(&client, &create, &credentials, Generation::V1)
            .expect("create request builds");
        assert_eq!(request.method(), reqwest::Method::POST);
        assert_eq!(request.url().as_str(), "http://127.0.0.1:43123/session");
        assert_eq!(
            request.headers()["x-opencode-directory"],
            "/fixture/project"
        );
        assert_eq!(request.headers()["content-type"], "application/json");
        assert_eq!(
            request.headers()["authorization"],
            "Basic b3BlbmNvZGUtZTJlOnVuaXQtcGFzc3dvcmQ="
        );
        assert_eq!(
            request.body().and_then(|body| body.as_bytes()),
            Some(b"{}".as_slice())
        );

        let delete = api_request(
            ApiRequestKind::DeleteSession,
            "http://127.0.0.1:43123",
            project,
            Some("ses_native"),
            Generation::V1,
        );
        assert_eq!(delete.url, "http://127.0.0.1:43123/session/ses_native");
        assert_eq!(delete.kind.operation(), OpenCodeApiOperation::DeleteSession);
    }

    #[test]
    fn v2_launch_spec_uses_exact_serve_arguments_and_isolated_environment() {
        let workspace = ScenarioWorkspace::create(Generation::V2).expect("workspace creates");
        let scenario = OpenCodeScenario::new(Generation::V2, &artifacts());
        let environment = vec![
            (
                OsString::from("HOME"),
                workspace.roots.home.clone().into_os_string(),
            ),
            (
                OsString::from("OPENCODE_DISABLE_MODELS_FETCH"),
                OsString::from("1"),
            ),
        ];
        let spec = scenario.launch_spec(&workspace.roots, 43124, environment.clone());
        assert_eq!(
            spec.arguments,
            vec![
                OsString::from("serve"),
                OsString::from("--hostname"),
                OsString::from("127.0.0.1"),
                OsString::from("--port"),
                OsString::from("43124"),
            ]
        );
        assert_eq!(spec.environment, environment);
        assert!(spec.clear_environment);
        assert_eq!(
            spec.current_directory,
            Some(workspace.roots.project.clone())
        );
    }

    #[test]
    fn v2_plugin_activation_request_is_project_scoped() {
        let project = Path::new("/fixture/project");
        let credentials = credentials_for_generation(Generation::V2, "unit-password".to_owned());
        let client = Client::builder().no_proxy().build().expect("client builds");
        let spec = api_request(
            ApiRequestKind::PluginState,
            "http://127.0.0.1:43124",
            project,
            None,
            Generation::V2,
        );
        let request = build_request(&client, &spec, &credentials, Generation::V2)
            .expect("plugin state request builds");

        assert_eq!(request.method(), reqwest::Method::GET);
        assert_eq!(request.url().as_str(), "http://127.0.0.1:43124/api/plugin");
        assert_eq!(
            request.headers()["x-opencode-directory"],
            "/fixture/project"
        );
        assert_eq!(
            request.headers()["authorization"],
            "Basic b3BlbmNvZGU6dW5pdC1wYXNzd29yZA=="
        );
        assert!(request.body().is_none());
    }

    #[test]
    fn v2_plugin_activation_requires_the_expected_active_plugin() {
        let active: V2PluginListResponse = serde_json::from_value(json!({
            "location": {"directory": "/fixture/project"},
            "data": [
                {"id": "other-plugin", "state": {"status": "active"}},
                {"id": "opencode-harness-events", "state": {"status": "active"}},
            ],
        }))
        .expect("plugin list response parses");
        assert!(v2_plugin_is_active(&active));

        let inactive: V2PluginListResponse = serde_json::from_value(json!({
            "location": {"directory": "/fixture/project"},
            "data": [
                {
                    "id": "opencode-harness-events",
                    "state": {"status": "failed", "error": "not ready"},
                },
            ],
        }))
        .expect("inactive plugin list response parses");
        assert!(!v2_plugin_is_active(&inactive));

        assert!(
            serde_json::from_value::<V2PluginListResponse>(json!({
                "location": {"directory": "/fixture/project"},
                "data": {},
            }))
            .is_err()
        );
    }

    #[test]
    fn v2_plugin_activation_retries_only_expected_transient_errors() {
        assert!(retryable_plugin_activation_error(&OpenCodeError::Api {
            generation: Generation::V2,
            operation: OpenCodeApiOperation::PluginState,
            kind: OpenCodeApiFailure::Connection,
        }));
        assert!(retryable_plugin_activation_error(
            &OpenCodeError::HttpStatus {
                generation: Generation::V2,
                operation: OpenCodeApiOperation::PluginState,
                status: 503,
            }
        ));
        assert!(!retryable_plugin_activation_error(&OpenCodeError::Api {
            generation: Generation::V2,
            operation: OpenCodeApiOperation::PluginState,
            kind: OpenCodeApiFailure::Timeout,
        }));
        assert!(!retryable_plugin_activation_error(
            &OpenCodeError::HttpStatus {
                generation: Generation::V2,
                operation: OpenCodeApiOperation::PluginState,
                status: 500,
            }
        ));
        assert!(!retryable_plugin_activation_error(
            &OpenCodeError::HttpStatus {
                generation: Generation::V2,
                operation: OpenCodeApiOperation::Health,
                status: 503,
            }
        ));
    }

    #[test]
    fn health_readiness_retries_expected_transient_failures() {
        assert!(super::retryable_readiness_error(&OpenCodeError::Api {
            generation: Generation::V2,
            operation: OpenCodeApiOperation::Health,
            kind: OpenCodeApiFailure::Connection,
        }));
        assert!(super::retryable_readiness_error(&OpenCodeError::Api {
            generation: Generation::V2,
            operation: OpenCodeApiOperation::Health,
            kind: OpenCodeApiFailure::Timeout,
        }));
        assert!(super::retryable_readiness_error(&OpenCodeError::Api {
            generation: Generation::V1,
            operation: OpenCodeApiOperation::Health,
            kind: OpenCodeApiFailure::Transport,
        }));
        for kind in [
            OpenCodeApiFailure::ClientBuild,
            OpenCodeApiFailure::RequestBuild,
            OpenCodeApiFailure::Body,
        ] {
            assert!(!super::retryable_readiness_error(&OpenCodeError::Api {
                generation: Generation::V1,
                operation: OpenCodeApiOperation::Health,
                kind,
            }));
        }
        assert!(!super::retryable_readiness_error(&OpenCodeError::Api {
            generation: Generation::V1,
            operation: OpenCodeApiOperation::CreateSession,
            kind: OpenCodeApiFailure::Transport,
        }));
        assert!(super::retryable_readiness_error(
            &OpenCodeError::HttpStatus {
                generation: Generation::V2,
                operation: OpenCodeApiOperation::Health,
                status: 503,
            }
        ));
        assert!(!super::retryable_readiness_error(
            &OpenCodeError::HttpStatus {
                generation: Generation::V2,
                operation: OpenCodeApiOperation::Health,
                status: 500,
            }
        ));
        assert!(!super::retryable_readiness_error(
            &OpenCodeError::HttpStatus {
                generation: Generation::V2,
                operation: OpenCodeApiOperation::CreateSession,
                status: 503,
            }
        ));
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn health_readiness_recovers_after_a_dropped_connection() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind health server");
        let base_url = format!("http://{}", listener.local_addr().expect("health address"));
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.expect("first health request");
            read_health_request(&mut first).await;
            drop(first);

            let (mut second, _) = listener.accept().await.expect("retried health request");
            read_health_request(&mut second).await;
            let body = json!({"healthy": true, "version": super::OPENCODE_V1_VERSION}).to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            second
                .write_all(response.as_bytes())
                .await
                .expect("write healthy response");
        });
        let mut process = readiness_child(Duration::from_secs(2)).await;
        let client = super::build_http_client(Generation::V1).expect("build health client");
        let credentials = credentials_for_generation(Generation::V1, "unit-password".to_owned());

        let result = super::wait_for_readiness(
            &client,
            &base_url,
            Path::new("/fixture/project"),
            &credentials,
            &mut process,
            Generation::V1,
        )
        .await;

        process.terminate().await.expect("reap owned child");
        server.abort();
        let _ = server.await;
        assert_eq!(result, Ok(super::OPENCODE_V1_VERSION.to_owned()));
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn repeated_health_transport_failures_keep_the_original_deadline() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind health server");
        let base_url = format!("http://{}", listener.local_addr().expect("health address"));
        let server = tokio::spawn(async move {
            loop {
                let (mut connection, _) = listener.accept().await.expect("health request");
                read_health_request(&mut connection).await;
            }
        });
        let readiness = Duration::from_millis(200);
        let mut process = readiness_child(readiness).await;
        let client = super::build_http_client(Generation::V1).expect("build health client");
        let credentials = credentials_for_generation(Generation::V1, "unit-password".to_owned());
        let started = Instant::now();

        let result = super::wait_for_readiness(
            &client,
            &base_url,
            Path::new("/fixture/project"),
            &credentials,
            &mut process,
            Generation::V1,
        )
        .await;
        let elapsed = started.elapsed();

        process.terminate().await.expect("reap owned child");
        server.abort();
        let _ = server.await;
        assert!(matches!(
            result,
            Err(OpenCodeError::Api {
                operation: OpenCodeApiOperation::Health,
                kind: OpenCodeApiFailure::Transport | OpenCodeApiFailure::Timeout,
                ..
            })
        ));
        assert!(elapsed >= readiness);
        assert!(elapsed < Duration::from_secs(2));
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn health_readiness_reports_child_exit_without_exposing_output() {
        let mut spec = ProcessSpec::new(PathBuf::from("/bin/sh"));
        spec.arguments = vec![
            "-c".into(),
            "printf 'private-child-output-sentinel' >&2; exit 17".into(),
        ];
        spec.deadlines.readiness = Duration::from_secs(2);
        spec.deadlines.termination_grace = Duration::from_millis(20);
        let mut process = ProcessGuard::spawn(spec)
            .await
            .expect("start exiting child");
        let client = super::build_http_client(Generation::V1).expect("build health client");
        let credentials = credentials_for_generation(Generation::V1, "unit-password".to_owned());

        let error = super::wait_for_readiness(
            &client,
            "http://127.0.0.1:0",
            Path::new("/fixture/project"),
            &credentials,
            &mut process,
            Generation::V1,
        )
        .await
        .expect_err("the exited child cannot become healthy");
        let child = super::observe_child(&mut process).expect("observe cached child exit");
        let output = process.terminate().await.expect("collect child output");
        assert_eq!(
            error,
            OpenCodeError::ChildExited {
                generation: Generation::V1,
                phase: ProcessPhase::Readiness,
                status: Some(17),
                signal: None,
            }
        );
        assert_eq!(output.status_code, Some(17));
        assert_eq!(output.stderr.len(), "private-child-output-sentinel".len());

        let failure = super::scenario_failure(error, child, Ok(output));
        for diagnostic in [failure.to_string(), format!("{failure:?}")] {
            assert!(diagnostic.contains("17"));
            assert!(diagnostic.contains("stderr_bytes"));
            assert!(!diagnostic.contains("private-child-output-sentinel"));
        }
    }

    #[test]
    fn malformed_health_requests_are_classified_as_non_retryable_construction_errors() {
        let client = super::build_http_client(Generation::V1).expect("build health client");
        let credentials = credentials_for_generation(Generation::V1, "unit-password".to_owned());
        let spec = api_request(
            ApiRequestKind::Health,
            "http://[private-url-sentinel",
            Path::new("/fixture/project"),
            None,
            Generation::V1,
        );
        let error = build_request(&client, &spec, &credentials, Generation::V1)
            .expect_err("invalid URL cannot build a request");

        assert_eq!(
            error,
            OpenCodeError::Api {
                generation: Generation::V1,
                operation: OpenCodeApiOperation::Health,
                kind: OpenCodeApiFailure::RequestBuild,
            }
        );
        assert!(!super::retryable_readiness_error(&error));
        assert!(!format!("{error:?}").contains("private-url-sentinel"));
    }

    #[test]
    fn scenario_diagnostics_preserve_the_failure_and_cleanup_error() {
        let failure = super::scenario_failure(
            OpenCodeError::Api {
                generation: Generation::V1,
                operation: OpenCodeApiOperation::Health,
                kind: OpenCodeApiFailure::Transport,
            },
            super::OpenCodeChildState::Running,
            Err(crate::ProcessError::DeadlineExceeded {
                phase: ProcessPhase::Shutdown,
            }),
        );

        assert!(matches!(
            failure,
            OpenCodeError::ScenarioFailure {
                source,
                child: super::OpenCodeChildState::Running,
                output: None,
                cleanup_error: Some(crate::ProcessError::DeadlineExceeded {
                    phase: ProcessPhase::Shutdown,
                }),
            } if *source == OpenCodeError::Api {
                generation: Generation::V1,
                operation: OpenCodeApiOperation::Health,
                kind: OpenCodeApiFailure::Transport,
            }
        ));
    }

    #[test]
    fn readiness_attempt_deadline_is_capped_by_the_total_readiness_deadline() {
        let now = Instant::now();
        let long_deadline = now + Duration::from_secs(30);
        let long_attempt = super::readiness_attempt_deadline(now, long_deadline);
        assert!(long_attempt <= long_deadline);
        assert_eq!(
            long_attempt,
            now + super::READINESS_ATTEMPT_TIMEOUT,
            "long readiness windows use the fixed per-attempt cap"
        );

        let short_deadline = now + Duration::from_millis(1);
        let short_attempt = super::readiness_attempt_deadline(now, short_deadline);
        assert!(short_attempt <= short_deadline);
        assert_eq!(
            short_attempt, short_deadline,
            "an attempt deadline never exceeds the overall readiness deadline"
        );
    }

    #[test]
    fn capture_drain_requires_deletion_and_end_after_an_extra_observation() {
        let session_id = "ses_native";
        let lifecycle = |function_id: &str, order| CaptureRecord {
            function_id: function_id.to_owned(),
            namespace: "namespace".to_owned(),
            payload: json!({"session_id": session_id}),
            order,
        };
        let observation = |kind: &str, order| CaptureRecord {
            function_id: "harness::observation".to_owned(),
            namespace: "namespace".to_owned(),
            payload: json!({
                "session_id": session_id,
                "hook_type": format!("opencode.v1.{kind}"),
            }),
            order,
        };
        let capture = CaptureSet {
            records: vec![
                lifecycle("harness::session_start", 0),
                observation("session.created", 1),
                observation("session.updated", 2),
                observation("session.deleted", 3),
                lifecycle("harness::session_end", 4),
            ],
        };
        let prefix = CaptureSet {
            records: capture.records[..4].to_vec(),
        };

        assert!(!super::capture_has_required_lifecycle(
            &prefix,
            Generation::V1,
            session_id,
        ));
        assert!(super::capture_has_required_lifecycle(
            &capture,
            Generation::V1,
            session_id,
        ));
    }

    #[test]
    fn plugin_config_uses_the_v2_plugins_key_and_dist_entrypoint() {
        let workspace = ScenarioWorkspace::create(Generation::V2).expect("workspace creates");
        let installed = workspace.roots.consumer.join("node_modules/plugin");
        let config = write_plugin_config(&workspace.roots, &installed, Generation::V2)
            .expect("config writes");
        let value: serde_json::Value =
            serde_json::from_slice(&fs::read(config).expect("config reads"))
                .expect("config parses");
        assert_eq!(
            value,
            json!({"plugins": [installed.join("dist").to_string_lossy()], "lsp": false})
        );
    }

    #[test]
    fn v2_api_requests_use_authenticated_info_and_session_routes() {
        let project = Path::new("/fixture/project");
        let credentials = credentials_for_generation(Generation::V2, "unit-password".to_owned());
        assert_eq!(credentials.username, "opencode");
        let client = Client::builder().no_proxy().build().expect("client builds");

        let health = api_request(
            ApiRequestKind::Health,
            "http://127.0.0.1:43124",
            project,
            None,
            Generation::V2,
        );
        let health_request = build_request(&client, &health, &credentials, Generation::V2)
            .expect("health request builds");
        assert_eq!(health_request.method(), reqwest::Method::GET);
        assert_eq!(
            health_request.url().as_str(),
            "http://127.0.0.1:43124/api/info"
        );
        assert_eq!(
            health_request.headers()["authorization"],
            "Basic b3BlbmNvZGU6dW5pdC1wYXNzd29yZA=="
        );

        let create = api_request(
            ApiRequestKind::CreateSession,
            "http://127.0.0.1:43124",
            project,
            None,
            Generation::V2,
        );
        let create_request = build_request(&client, &create, &credentials, Generation::V2)
            .expect("create request builds");
        assert_eq!(create_request.method(), reqwest::Method::POST);
        assert_eq!(
            create_request.url().as_str(),
            "http://127.0.0.1:43124/api/session"
        );
        assert_eq!(
            create_request.headers()["x-opencode-directory"],
            "/fixture/project"
        );
        assert_eq!(
            create_request.body().and_then(|body| body.as_bytes()),
            Some(b"{}".as_slice())
        );

        let delete = api_request(
            ApiRequestKind::DeleteSession,
            "http://127.0.0.1:43124",
            project,
            Some("ses_native"),
            Generation::V2,
        );
        assert_eq!(delete.url, "http://127.0.0.1:43124/api/session/ses_native");
        assert_eq!(delete.kind.operation(), OpenCodeApiOperation::DeleteSession);
    }

    #[test]
    fn v2_api_response_shapes_validate_info_and_session_identity() {
        let info: super::V2InfoResponse = serde_json::from_value(json!({
            "version": "2.0.10",
            "pid": 43124,
            "urls": ["http://127.0.0.1:43124"],
            "paths": {"tmp": "/fixture/tmp"},
        }))
        .expect("v2 info response parses");
        assert_eq!(info.version, OPENCODE_V2_VERSION);

        let session: super::V2SessionResponse =
            serde_json::from_value(json!({"data": {"id": "ses_native"}}))
                .expect("v2 session response parses");
        assert_eq!(session.data.id, "ses_native");
    }

    #[test]
    fn child_logs_are_bounded_and_preserve_process_capture_flags() {
        let output = ProcessOutput {
            status_code: Some(0),
            stdout: vec![b'a'; 20_000],
            stderr: vec![b'b'; 2],
            stdout_truncated: true,
            stderr_truncated: false,
        };
        let logs = OpenCodeChildLogs::from(output);
        assert_eq!(logs.stdout.len(), 16 * 1024);
        assert!(logs.stdout_truncated);
        assert_eq!(logs.stderr, "bb");
        assert_eq!(bounded_log(b"safe"), ("safe".to_owned(), false));
    }

    #[test]
    fn file_operation_errors_keep_metadata_without_paths() {
        let error = super::OpenCodeError::File {
            generation: Generation::V1,
            operation: OpenCodeFileOperation::WriteConfig,
            kind: std::io::ErrorKind::PermissionDenied,
        };
        assert!(!error.to_string().contains("/fixture"));
        assert!(error.to_string().contains("WriteConfig"));
    }
}
