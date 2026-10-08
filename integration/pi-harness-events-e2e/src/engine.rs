use std::{
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use iii_sdk::protocol::TriggerRequest;
use iii_sdk::{
    IIIClient, InitOptions, RegisterFunction, WorkerIdentityMode,
    engine::EngineFunctions,
    register_worker,
    runtime::{FunctionRef, WorkerMetadata},
};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::{
    process::Command,
    time::{Instant, sleep, timeout_at},
};

use crate::process::{CaptureLimits, ProcessGuard};

pub const CAPTURE_FUNCTION_IDS: [&str; 3] = [
    "harness::session_start",
    "harness::observation",
    "harness::session_end",
];

static NEXT_HARNESS_ID: AtomicU64 = AtomicU64::new(0);
static NEXT_TEMP_DIRECTORY: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct CapturedInvocation {
    pub function_id: String,
    pub namespace: String,
    pub payload: Value,
    pub order: u64,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum EngineHarnessError {
    #[error("engine artifact is invalid")]
    InvalidArtifact,
    #[error("engine setup failed")]
    SetupFailed,
    #[error("engine startup failed")]
    StartupFailed,
    #[error("engine cleanup failed")]
    CleanupFailed,
}

pub struct EngineHarness {
    // Fields drop in declaration order. Kill the owned engine group before
    // joining the SDK connection thread during panic unwinding.
    process: ProcessGuard,
    capture: CaptureWorker,
    _directory: TemporaryDirectory,
    url: String,
}

impl EngineHarness {
    pub async fn start(iii: &Path, deadline: Instant) -> Result<Self, EngineHarnessError> {
        if !iii.is_file() {
            return Err(EngineHarnessError::InvalidArtifact);
        }

        let directory = TemporaryDirectory::new()?;
        let port = available_loopback_port()?;
        let config_path = directory.path.join("config.yaml");
        fs::write(&config_path, engine_config(port))
            .map_err(|_| EngineHarnessError::SetupFailed)?;

        let mut command = Command::new(iii);
        command
            .arg("--config")
            .arg(config_path)
            .arg("--no-update-check")
            .current_dir(&directory.path)
            .env("III_TELEMETRY_ENABLED", "false");
        let mut process = ProcessGuard::spawn(&mut command, CaptureLimits::default())
            .map_err(|_| EngineHarnessError::StartupFailed)?;
        let url = format!("ws://127.0.0.1:{port}");
        let capture = CaptureWorker::start(&url);

        let readiness = process
            .wait_for_readiness(deadline, capture.wait_until_routable(deadline))
            .await;
        if readiness.is_err() {
            capture.begin_shutdown().await;
            let _ = process.shutdown_until(deadline).await;
            capture.shutdown();
            return Err(EngineHarnessError::StartupFailed);
        }

        Ok(Self {
            process,
            capture,
            _directory: directory,
            url,
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn namespace(&self) -> &str {
        &self.capture.namespace
    }

    pub const fn process_id(&self) -> u32 {
        self.process.process_id()
    }

    pub fn captures(&self) -> Vec<CapturedInvocation> {
        self.capture.captures()
    }

    pub async fn shutdown_until(&mut self, deadline: Instant) -> Result<(), EngineHarnessError> {
        self.capture.begin_shutdown().await;
        let result = self.process.shutdown_until(deadline).await;
        // IIIClient::shutdown joins its connection thread. Joining after the
        // process guard preserves the engine process deadline and avoids a
        // detached SDK worker thread.
        self.capture.shutdown();
        result.map_err(|_| EngineHarnessError::CleanupFailed)
    }

    #[cfg(test)]
    fn block_capture_shutdown(
        &self,
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) {
        self.capture.block_shutdown(entered, release);
    }
}

struct CaptureWorker {
    client: IIIClient,
    identity: String,
    namespace: String,
    captures: Arc<Mutex<Vec<CapturedInvocation>>>,
    _registrations: [FunctionRef; 3],
    #[cfg(test)]
    shutdown_blocker: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
}

impl CaptureWorker {
    fn start(engine_url: &str) -> Self {
        let sequence = NEXT_HARNESS_ID.fetch_add(1, Ordering::Relaxed);
        let identity = format!(
            "pi-harness-events-e2e-worker-{}-{sequence}",
            std::process::id()
        );
        let namespace = format!(
            "pi-harness-events-e2e-namespace-{}-{sequence}",
            std::process::id()
        );
        let client = register_worker(
            engine_url,
            worker_options(identity.clone(), namespace.clone()),
        );
        let captures = Arc::new(Mutex::new(Vec::new()));
        let next_order = Arc::new(AtomicU64::new(0));
        let registrations = CAPTURE_FUNCTION_IDS.map(|function_id| {
            client.register_function(
                function_id,
                capture_registration(
                    function_id,
                    namespace.clone(),
                    Arc::clone(&captures),
                    Arc::clone(&next_order),
                ),
            )
        });

        Self {
            client,
            identity,
            namespace,
            captures,
            _registrations: registrations,
            #[cfg(test)]
            shutdown_blocker: Mutex::new(None),
        }
    }

    async fn wait_until_routable(&self, deadline: Instant) -> Result<(), EngineHarnessError> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(EngineHarnessError::StartupFailed);
        }
        self.client
            .wait_until_registered(remaining)
            .await
            .map_err(|_| EngineHarnessError::StartupFailed)?;

        loop {
            if self.client.fatal_error().is_some() {
                return Err(EngineHarnessError::StartupFailed);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(EngineHarnessError::StartupFailed);
            }

            let catalog = timeout_at(
                deadline,
                self.client.trigger(TriggerRequest {
                    function_id: EngineFunctions::INFO_FUNCTIONS.to_owned(),
                    payload: json!({
                        "function_ids": CAPTURE_FUNCTION_IDS,
                        "namespace": self.namespace,
                    }),
                    action: None,
                    timeout_ms: Some(remaining.as_millis().min(u64::MAX as u128).max(1) as u64),
                }),
            )
            .await;
            match catalog {
                Ok(Ok(catalog))
                    if catalog_is_routable(&catalog, &self.identity, &self.namespace) =>
                {
                    return Ok(());
                }
                Ok(Ok(_)) | Ok(Err(_)) => {}
                Err(_) => return Err(EngineHarnessError::StartupFailed),
            }

            sleep(Duration::from_millis(20).min(remaining)).await;
        }
    }

    fn captures(&self) -> Vec<CapturedInvocation> {
        let mut captures = self
            .captures
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        captures.sort_by_key(|capture| capture.order);
        captures
    }

    fn shutdown(&self) {
        #[cfg(test)]
        if let Some((entered, release)) = self
            .shutdown_blocker
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            let _ = entered.send(());
            let _ = release.recv();
        }
        self.client.shutdown();
    }

    async fn begin_shutdown(&self) {
        self.client.shutdown_async().await;
    }

    #[cfg(test)]
    fn block_shutdown(
        &self,
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) {
        *self
            .shutdown_blocker
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some((entered, release));
    }
}

impl Drop for CaptureWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn worker_options(identity: String, namespace: String) -> InitOptions {
    let metadata = WorkerMetadata {
        name: identity,
        ..Default::default()
    };
    let mut options = InitOptions {
        metadata: Some(metadata),
        headers: None,
        otel: Some(Default::default()),
        namespace: Some(namespace),
        identity: WorkerIdentityMode::Explicit,
    };
    if let Some(otel) = options.otel.as_mut() {
        otel.enabled = Some(false);
    }
    options
}

fn capture_registration(
    function_id: &'static str,
    namespace: String,
    captures: Arc<Mutex<Vec<CapturedInvocation>>>,
    next_order: Arc<AtomicU64>,
) -> RegisterFunction {
    RegisterFunction::new_async(move |payload: Value| {
        let captures = Arc::clone(&captures);
        let namespace = namespace.clone();
        let next_order = Arc::clone(&next_order);
        async move {
            let order = next_order.fetch_add(1, Ordering::SeqCst) + 1;
            captures
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(CapturedInvocation {
                    function_id: function_id.to_owned(),
                    namespace,
                    payload,
                    order,
                });
            Ok::<Value, iii_sdk::Error>(json!({"dispatched": true}))
        }
    })
}

fn catalog_is_routable(catalog: &Value, identity: &str, namespace: &str) -> bool {
    let Some(functions) = catalog.get("functions").and_then(Value::as_array) else {
        return false;
    };

    CAPTURE_FUNCTION_IDS.iter().all(|function_id| {
        functions.iter().any(|function| {
            function.get("function_id").and_then(Value::as_str) == Some(*function_id)
                && function.get("namespace").and_then(Value::as_str) == Some(namespace)
                && function.get("worker_name").and_then(Value::as_str) == Some(identity)
        })
    })
}

fn available_loopback_port() -> Result<u16, EngineHarnessError> {
    TcpListener::bind(("127.0.0.1", 0))
        .and_then(|listener| listener.local_addr())
        .map(|address| address.port())
        .map_err(|_| EngineHarnessError::SetupFailed)
}

fn engine_config(port: u16) -> String {
    format!(
        "workers:\n  - name: iii-worker-manager\n    config:\n      host: 127.0.0.1\n      port: {port}\n"
    )
}

struct TemporaryDirectory {
    path: PathBuf,
}

impl TemporaryDirectory {
    fn new() -> Result<Self, EngineHarnessError> {
        let sequence = NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "pi-harness-events-e2e-engine-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&path).map_err(|_| EngineHarnessError::SetupFailed)?;
        Ok(Self { path })
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use std::{env, io, path::PathBuf, process::Command, sync::mpsc, thread, time::Duration};

    use iii_sdk::protocol::TriggerRequest;
    use iii_sdk::register_worker;
    use serde_json::json;
    use tokio::time::{Instant, sleep, timeout};

    use super::{CAPTURE_FUNCTION_IDS, EngineHarness, worker_options};

    const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);
    const INVOCATION_TIMEOUT: Duration = Duration::from_secs(5);
    const CLEANUP_TIMEOUT: Duration = Duration::from_secs(3);

    #[tokio::test(flavor = "current_thread")]
    async fn real_iii_routes_all_capture_functions_and_reaps_the_engine() {
        let mut engine = start_engine().await;
        let invoker = register_worker(
            engine.url(),
            worker_options(
                format!("pi-harness-events-e2e-invoker-{}", std::process::id()),
                engine.namespace().to_owned(),
            ),
        );
        invoker
            .wait_until_registered(STARTUP_TIMEOUT)
            .await
            .expect("test invoker must register with real iii");

        let payloads = [
            json!(["session-start", {"nested": true}]),
            json!(["observation", {"nested": true}]),
            json!(["session-end", {"nested": true}]),
        ];
        for (function_id, payload) in CAPTURE_FUNCTION_IDS.into_iter().zip(payloads.iter()) {
            let response = timeout(
                INVOCATION_TIMEOUT,
                invoker.trigger(TriggerRequest {
                    function_id: function_id.to_owned(),
                    payload: payload.clone(),
                    action: None,
                    timeout_ms: Some(INVOCATION_TIMEOUT.as_millis() as u64),
                }),
            )
            .await
            .expect("real iii capture invocation must complete before its deadline")
            .expect("real iii capture invocation must succeed");
            assert!(
                response == json!({"dispatched": true}),
                "capture function must return the authoritative success object"
            );
        }

        let captures = engine.captures();
        assert_eq!(
            captures.len(),
            CAPTURE_FUNCTION_IDS.len(),
            "every registered capture function must receive one invocation"
        );
        for (index, ((function_id, payload), capture)) in CAPTURE_FUNCTION_IDS
            .into_iter()
            .zip(payloads.iter())
            .zip(captures.iter())
            .enumerate()
        {
            assert!(
                capture.function_id == function_id,
                "capture function identity must be retained"
            );
            assert!(
                capture.namespace == engine.namespace(),
                "capture namespace must be retained"
            );
            assert!(
                capture.payload == *payload,
                "capture payload must be retained"
            );
            assert_eq!(
                capture.order,
                index as u64 + 1,
                "capture receive order must be monotonic"
            );
        }

        invoker.shutdown();
        let engine_pid = engine.process_id();
        engine
            .shutdown_until(Instant::now() + STARTUP_TIMEOUT)
            .await
            .expect("engine harness cleanup must complete");
        wait_for_process_absence(engine_pid).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocked_capture_shutdown_does_not_delay_engine_cleanup() {
        let mut engine = start_engine().await;
        let engine_pid = engine.process_id();
        let (entered, release) = block_capture_shutdown(&engine);
        let deadline = Instant::now() + CLEANUP_TIMEOUT;
        let (reaped_sender, reaped_receiver) = mpsc::channel();
        let monitor = thread::spawn(move || {
            let reaped = process_is_absent_by_thread(engine_pid, deadline);
            let _ = reaped_sender.send(reaped);
            let _ = release.send(());
        });

        let cleanup_result = engine.shutdown_until(deadline).await;
        let blocked = entered.recv_timeout(CLEANUP_TIMEOUT).is_ok();
        let reaped_by_deadline = reaped_receiver.recv().unwrap_or(false);
        monitor.join().expect("monitor thread must not panic");
        wait_for_process_absence(engine_pid).await;

        assert!(
            blocked,
            "capture shutdown must reach the blocking join seam"
        );
        assert!(
            reaped_by_deadline,
            "engine cleanup must not wait for the capture client shutdown"
        );
        assert!(
            cleanup_result.is_ok(),
            "cleanup must complete after capture release"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn panic_drop_kills_the_engine_before_blocking_capture_shutdown() {
        let engine = start_engine().await;
        let engine_pid = engine.process_id();
        let (entered, release) = block_capture_shutdown(&engine);
        let deadline = Instant::now() + CLEANUP_TIMEOUT;
        let dropper = thread::spawn(move || {
            let _engine = engine;
            panic!("exercise engine harness panic cleanup");
        });

        let blocked = entered.recv_timeout(CLEANUP_TIMEOUT).is_ok();
        let reaped_by_deadline = if blocked {
            process_is_absent_by(engine_pid, deadline).await
        } else {
            false
        };
        let _ = release.send(());
        let panicked = dropper.join().is_err();
        wait_for_process_absence(engine_pid).await;

        assert!(
            blocked,
            "capture shutdown must reach the blocking join seam"
        );
        assert!(
            reaped_by_deadline,
            "panic cleanup must kill the engine before capture shutdown blocks"
        );
        assert!(panicked, "drop test thread must panic");
    }

    async fn start_engine() -> EngineHarness {
        let iii = nix_iii_artifact();
        assert_iii_version(&iii);
        EngineHarness::start(&iii, Instant::now() + STARTUP_TIMEOUT)
            .await
            .expect("real iii capture harness must become routable")
    }

    fn block_capture_shutdown(engine: &EngineHarness) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (entered_sender, entered_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        engine.block_capture_shutdown(entered_sender, release_receiver);
        (entered_receiver, release_sender)
    }

    fn nix_iii_artifact() -> PathBuf {
        let path = env::var_os("PATH").expect("Nix-provided iii artifact is unavailable from PATH");
        env::split_paths(&path)
            .map(|directory| directory.join("iii"))
            .find(|path| path.is_file())
            .expect("Nix-provided iii artifact is unavailable from PATH")
    }

    fn assert_iii_version(iii: &std::path::Path) {
        let output = Command::new(iii)
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

    async fn wait_for_process_absence(process_id: u32) {
        let deadline = Instant::now() + Duration::from_secs(1);
        assert!(
            process_is_absent_by(process_id, deadline).await,
            "real iii process survived harness cleanup"
        );
    }

    async fn process_is_absent_by(process_id: u32, deadline: Instant) -> bool {
        loop {
            if process_is_absent(process_id) {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    fn process_is_absent_by_thread(process_id: u32, deadline: Instant) -> bool {
        loop {
            if process_is_absent(process_id) {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn process_is_absent(process_id: u32) -> bool {
        (unsafe { libc::kill(process_id as libc::pid_t, 0) == -1 })
            && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
    }
}
