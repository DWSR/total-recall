#![deny(unsafe_code)]

use std::{
    env, fs,
    net::TcpListener,
    path::{Path, PathBuf},
    time::Duration,
};

use serde_json::json;

use futures_util::future::BoxFuture;
use opencode_harness_events_e2e::opencode::{OpenCodeApiFailure, OpenCodeApiOperation};
use opencode_harness_events_e2e::{
    Generation, OPENCODE_MODEL_ID, OPENCODE_REPLY_TEXT, OfflineModelRecorder, OpenCodeError,
    ProcessGuard, ProcessSpec, drive_v1_message_completion, drive_v2_prompt_completion,
};

const LIVE_WAIT: Duration = Duration::from_secs(120);
const LIVE_POLL: Duration = Duration::from_millis(50);
const V1_HEALTH: &str = "/global/health";
const V2_HEALTH: &str = "/api/info";

fn host_binary(generation: Generation) -> PathBuf {
    let variable = match generation {
        Generation::V1 => "OPENCODE_V1_BIN",
        Generation::V2 => "OPENCODE_V2_BIN",
    };
    let binary = env::var_os(variable).unwrap_or_default();
    assert!(
        !binary.is_empty(),
        "live artifacts must be explicit: set {variable}"
    );
    PathBuf::from(binary)
}

struct Workspace {
    roots: PathBuf,
}

impl Workspace {
    fn create(generation: Generation) -> Self {
        let label = match generation {
            Generation::V1 => "v1",
            Generation::V2 => "v2",
        };
        let roots = std::env::temp_dir().join(format!(
            "opencode-harness-events-provider-live-{label}-{}",
            std::process::id()
        ));
        match fs::remove_dir_all(&roots) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("live workspace fixture is replaceable: {error:?}"),
        }
        for directory in [
            "home",
            "xdg-data",
            "xdg-config/opencode/node_modules",
            "xdg-cache",
            "xdg-state",
            "config/node_modules",
            "project",
            "tmp",
        ] {
            fs::create_dir_all(roots.join(directory)).expect("live workspace directories create");
        }
        Self { roots }
    }

    fn project(&self) -> PathBuf {
        self.roots.join("project")
    }

    fn write_provider_config(&self, recorder: &OfflineModelRecorder) {
        let binding = recorder.binding();
        let mut config = json!({
            "lsp": false,
            "provider": {},
        });
        config["provider"][binding.provider_id()] = binding.config_entry_value();
        let content = serde_json::to_vec_pretty(&config).expect("provider config serializes");
        fs::write(self.roots.join("config").join("opencode.json"), content)
            .expect("provider config writes");
    }

    fn host_environment(
        &self,
        generation: Generation,
        password: String,
    ) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
        let string = |value: &str| value.to_string().into();
        let mut environment = vec![
            ("HOME".into(), self.roots.join("home").into_os_string()),
            ("TMPDIR".into(), self.roots.join("tmp").into_os_string()),
            (
                "XDG_DATA_HOME".into(),
                self.roots.join("xdg-data").into_os_string(),
            ),
            (
                "XDG_CONFIG_HOME".into(),
                self.roots.join("xdg-config").into_os_string(),
            ),
            (
                "XDG_CACHE_HOME".into(),
                self.roots.join("xdg-cache").into_os_string(),
            ),
            (
                "XDG_STATE_HOME".into(),
                self.roots.join("xdg-state").into_os_string(),
            ),
            (
                "OPENCODE_CONFIG_DIR".into(),
                self.roots.join("config").into_os_string(),
            ),
            (
                "OPENCODE_DB".into(),
                self.roots
                    .join("xdg-state")
                    .join("opencode.db")
                    .into_os_string(),
            ),
            ("OPENCODE_DISABLE_MODELS_FETCH".into(), string("1")),
            ("OPENCODE_DISABLE_AUTOUPDATE".into(), string("1")),
            ("OPENCODE_DISABLE_PROJECT_CONFIG".into(), string("1")),
        ];
        match generation {
            Generation::V1 => {
                environment.extend([
                    ("OPENCODE_DISABLE_LSP_DOWNLOAD".into(), string("1")),
                    (
                        "OPENCODE_EXPERIMENTAL_DISABLE_FILEWATCHER".into(),
                        string("1"),
                    ),
                    ("OPENCODE_DISABLE_DEFAULT_PLUGINS".into(), string("1")),
                    ("OPENCODE_SERVER_USERNAME".into(), "opencode-e2e".into()),
                ]);
            }
            Generation::V2 => {
                environment.extend([("OPENCODE_DISABLE_FILEWATCHER".into(), string("1"))]);
            }
        }
        environment.extend([("OPENCODE_SERVER_PASSWORD".into(), password.into())]);
        environment
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.roots);
    }
}

fn launch_spec(
    generation: Generation,
    binary: &Path,
    workspace: &Workspace,
    port: u16,
    environment: Vec<(std::ffi::OsString, std::ffi::OsString)>,
) -> ProcessSpec {
    let mut spec = ProcessSpec::new(binary.to_path_buf());
    spec.arguments = vec![
        "serve".into(),
        "--hostname".into(),
        "127.0.0.1".into(),
        "--port".into(),
        port.to_string().into(),
    ];
    if generation == Generation::V1 {
        spec.arguments
            .extend(["--log-level".into(), "ERROR".into()]);
    }
    spec.environment = environment;
    spec.clear_environment = true;
    spec.current_directory = Some(workspace.project());
    spec
}

fn credentials_username(generation: Generation) -> &'static str {
    match generation {
        Generation::V1 => "opencode-e2e",
        Generation::V2 => "opencode",
    }
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("live client builds")
}

async fn wait_for_readiness(
    generation: Generation,
    port: u16,
    username: &str,
    password: &str,
) -> Result<(), OpenCodeError> {
    let path = match generation {
        Generation::V1 => V1_HEALTH,
        Generation::V2 => V2_HEALTH,
    };
    let url = format!("http://127.0.0.1:{port}{path}");
    let deadline = tokio::time::Instant::now() + LIVE_WAIT;
    loop {
        let attempt = client()
            .get(&url)
            .basic_auth(username, Some(password))
            .timeout(Duration::from_secs(1))
            .send()
            .await;
        if matches!(attempt, Ok(response) if response.status() == reqwest::StatusCode::OK) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(OpenCodeError::Api {
                generation,
                operation: OpenCodeApiOperation::Health,
                kind: OpenCodeApiFailure::Timeout,
            });
        }
        tokio::time::sleep(LIVE_POLL).await;
    }
}

async fn create_session(
    generation: Generation,
    port: u16,
    username: &str,
    password: &str,
    project: &Path,
) -> Result<String, OpenCodeError> {
    let prefix = match generation {
        Generation::V1 => "/session",
        Generation::V2 => "/api/session",
    };
    let response = client()
        .post(format!("http://127.0.0.1:{port}{prefix}"))
        .basic_auth(username, Some(password))
        .header(
            "x-opencode-directory",
            project.to_string_lossy().into_owned(),
        )
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body("{}")
        .timeout(LIVE_WAIT)
        .send()
        .await
        .map_err(|_| OpenCodeError::Api {
            generation,
            operation: OpenCodeApiOperation::CreateSession,
            kind: OpenCodeApiFailure::Transport,
        })?;
    if response.status() != reqwest::StatusCode::OK {
        return Err(OpenCodeError::HttpStatus {
            generation,
            operation: OpenCodeApiOperation::CreateSession,
            status: response.status().as_u16(),
        });
    }
    let parsed: serde_json::Value =
        response
            .json()
            .await
            .map_err(|_| OpenCodeError::MalformedResponse {
                generation,
                operation: OpenCodeApiOperation::CreateSession,
            })?;
    let session_id = parsed
        .pointer("/data/id")
        .or_else(|| parsed.get("id"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .filter(|value| !value.trim().is_empty())
        .ok_or(OpenCodeError::EmptySessionIdentity { generation })?;
    Ok(session_id)
}

#[allow(clippy::too_many_lines)]
async fn drive_one_completion_per_generation(generation: Generation) {
    let binary = host_binary(generation);
    let workspace = Workspace::create(generation);
    let password = format!("live-{}-{generation:?}-pw", std::process::id());
    let bearer = format!("live-bearer-{generation:?}-{}", std::process::id());
    let recorder = OfflineModelRecorder::start(
        &bearer,
        std::num::NonZeroUsize::new(2).expect("deterministic two-request drive plan"),
    )
    .await
    .expect("recorder starts on loopback");
    workspace.write_provider_config(&recorder);

    let (ended_listener, port) = {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("loopback port binds");
        let port = listener.local_addr().expect("loopback address").port();
        (listener, port)
    };
    let spec = launch_spec(
        generation,
        &binary,
        &workspace,
        port,
        workspace.host_environment(generation, password.clone()),
    );
    let host = ProcessGuard::spawn(spec)
        .await
        .expect("pinned host spawns offline");
    drop(ended_listener);

    let username = credentials_username(generation);
    let base_url = format!("http://127.0.0.1:{port}");
    let project_path = workspace.project();
    let project = project_path.to_string_lossy().into_owned();
    wait_for_readiness(generation, port, username, &password)
        .await
        .expect("pinned host becomes healthy with only the offline provider");
    let session_id = create_session(generation, port, username, &password, &project_path)
        .await
        .expect("live session identity is available");

    let drive_prompt = format!(
        "recorder-live-prompt-{generation:?}-{}",
        recorder.base_url()
    );
    let http = client();
    let binding = recorder.binding();
    let binding = &binding;
    let drive: BoxFuture<
        '_,
        Result<
            opencode_harness_events_e2e::AssistantCompletion,
            opencode_harness_events_e2e::OfflineAssistantCompletionError,
        >,
    > = match generation {
        Generation::V1 => Box::pin(drive_v1_message_completion(
            &http,
            &base_url,
            &project,
            username,
            &password,
            &session_id,
            binding,
            &drive_prompt,
        )),
        Generation::V2 => Box::pin(drive_v2_prompt_completion(
            &http,
            &base_url,
            &project,
            username,
            &password,
            &session_id,
            binding,
            &drive_prompt,
        )),
    };
    let completion = tokio::time::timeout_at(tokio::time::Instant::now() + LIVE_WAIT, drive)
        .await
        .expect("assistant completion resolves within the live deadline")
        .expect("pinned host completes ONE assistant turn against the offline recorder");
    assert_eq!(
        completion.text(),
        OPENCODE_REPLY_TEXT,
        "the recorder reply text reaches the assistant completion"
    );

    recorder
        .wait_until_served(2, tokio::time::Instant::now() + LIVE_WAIT)
        .await
        .expect("the deterministic two-request drive plan arrives without external services");
    let records = recorder.records();
    assert_eq!(
        records.len(),
        2,
        "exactly the planned provider requests arrive"
    );
    for record in &records {
        let parsed: serde_json::Value =
            serde_json::from_slice(record.body()).expect("provider-visible body parses");
        assert_eq!(parsed["model"], OPENCODE_MODEL_ID);
        assert_eq!(parsed["stream"], true);
        assert!(
            record.has_non_empty_system_message(),
            "provider-visible system bytes are recorded"
        );
    }
    assert!(
        records
            .iter()
            .any(|record| record.contains(drive_prompt.as_bytes())),
        "the driven prompt reaches the provider request bodies"
    );
    assert_eq!(
        recorder.rejection_kinds(),
        Vec::<opencode_harness_events_e2e::OfflineModelRecorderRejection>::new(),
        "no rejections precede the counted completion"
    );

    let route_probe = client()
        .post(format!("{}/unknown", recorder.base_url()))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body("{}")
        .timeout(LIVE_WAIT)
        .send()
        .await
        .expect("route probe posts");
    assert_eq!(route_probe.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(
        route_probe.bytes().await.expect("body drains").to_vec(),
        Vec::<u8>::new()
    );

    let count_probe = client()
        .post(format!("{}/chat/completions", recorder.base_url()))
        .bearer_auth(&bearer)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(
            json!({
                "model": OPENCODE_MODEL_ID,
                "stream": true,
                "messages": [{"role": "user", "content": "beyond the plan"}],
            })
            .to_string(),
        )
        .timeout(LIVE_WAIT)
        .send()
        .await
        .expect("post-allowance count probe posts");
    assert_eq!(count_probe.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        count_probe.bytes().await.expect("body drains").to_vec(),
        Vec::<u8>::new()
    );

    let termination = host.terminate().await.expect("owned live host terminates");
    let _ = termination.status_code;
    recorder.stop().await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires the pinned v1 host to be set in OPENCODE_V1_BIN"]
async fn recorder_drives_one_v1_message_completion_offline() {
    drive_one_completion_per_generation(Generation::V1).await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires the pinned v2 host to be set in OPENCODE_V2_BIN"]
async fn recorder_drives_one_v2_prompt_completion_offline() {
    drive_one_completion_per_generation(Generation::V2).await;
}
