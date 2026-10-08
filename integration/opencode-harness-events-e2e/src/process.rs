use std::{
    ffi::OsString,
    future::Future,
    io,
    path::PathBuf,
    process::{ExitStatus, Stdio},
    time::{Duration, Instant},
};

use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
    task::JoinHandle,
    time::{sleep, timeout},
};

pub const DEFAULT_PROCESS_TIMEOUT: Duration = Duration::from_secs(30);
pub const MAX_CAPTURE_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessSpec {
    pub program: PathBuf,
    pub arguments: Vec<OsString>,
    pub environment: Vec<(OsString, OsString)>,
    pub clear_environment: bool,
    pub current_directory: Option<PathBuf>,
    pub timeout: Duration,
    pub deadlines: ProcessDeadlines,
}

impl ProcessSpec {
    pub fn new(program: PathBuf) -> Self {
        Self {
            program,
            arguments: Vec::new(),
            environment: Vec::new(),
            clear_environment: false,
            current_directory: None,
            timeout: DEFAULT_PROCESS_TIMEOUT,
            deadlines: ProcessDeadlines::default(),
        }
    }

    pub fn command(&self) -> Command {
        let mut command = Command::new(&self.program);
        command.args(&self.arguments);
        if self.clear_environment {
            command.env_clear();
            if let Some(path) = std::env::var_os("PATH") {
                command.env("PATH", path);
            }
        }
        for (name, value) in &self.environment {
            command.env(name, value);
        }
        if let Some(current_directory) = &self.current_directory {
            command.current_dir(current_directory);
        }
        command
    }

    pub fn deadline(&self, phase: ProcessPhase) -> Duration {
        self.deadlines.for_phase(phase, self.timeout)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessDeadlines {
    pub startup: Duration,
    pub readiness: Duration,
    pub shutdown: Duration,
    pub termination_grace: Duration,
}

impl Default for ProcessDeadlines {
    fn default() -> Self {
        Self {
            startup: Duration::from_secs(10),
            readiness: Duration::from_secs(30),
            shutdown: Duration::from_secs(5),
            termination_grace: Duration::from_millis(250),
        }
    }
}

impl ProcessDeadlines {
    pub const fn for_phase(self, phase: ProcessPhase, execution: Duration) -> Duration {
        match phase {
            ProcessPhase::Startup => self.startup,
            ProcessPhase::Readiness => self.readiness,
            ProcessPhase::Execution => execution,
            ProcessPhase::Shutdown => self.shutdown,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessPhase {
    Startup,
    Readiness,
    Execution,
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub process_group: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminationSignal {
    Term,
    Kill,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProcessOutput {
    pub status_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ProcessError {
    #[error("owned process groups are unsupported on this platform")]
    UnsupportedPlatform,
    #[error("child process failed to start ({kind:?})")]
    Spawn { kind: io::ErrorKind },
    #[error("child process did not expose a {stream:?} pipe")]
    MissingPipe { stream: CaptureStream },
    #[error("child process id was unavailable after start")]
    MissingProcessId,
    #[error("{phase:?} deadline expired")]
    DeadlineExceeded { phase: ProcessPhase },
    #[error("child process wait failed ({kind:?})")]
    Wait { kind: io::ErrorKind },
    #[error("{stream:?} capture failed ({kind:?})")]
    Capture {
        stream: CaptureStream,
        kind: io::ErrorKind,
    },
    #[error("owned process-group {signal:?} failed ({kind:?})")]
    Signal {
        signal: TerminationSignal,
        kind: io::ErrorKind,
    },
}

#[derive(Debug)]
pub struct ProcessGuard {
    identity: ProcessIdentity,
    child: Option<Child>,
    exit_status: Option<ExitStatus>,
    process_group: Option<i32>,
    deadlines: ProcessDeadlines,
    execution_timeout: Duration,
    stdout: Option<DrainTask>,
    stderr: Option<DrainTask>,
}

impl ProcessGuard {
    pub async fn spawn(spec: ProcessSpec) -> Result<Self, ProcessError> {
        #[cfg(unix)]
        {
            let startup = spec.deadline(ProcessPhase::Startup);
            match timeout(startup, async move { Self::spawn_unix(spec) }).await {
                Ok(result) => result,
                Err(_) => Err(ProcessError::DeadlineExceeded {
                    phase: ProcessPhase::Startup,
                }),
            }
        }

        #[cfg(not(unix))]
        {
            let _ = spec;
            Err(ProcessError::UnsupportedPlatform)
        }
    }

    pub fn identity(&self) -> ProcessIdentity {
        self.identity
    }

    pub fn deadline(&self, phase: ProcessPhase) -> Duration {
        self.deadlines.for_phase(phase, self.execution_timeout)
    }

    pub fn try_wait(&mut self) -> Result<Option<ExitStatus>, ProcessError> {
        let Some(child) = self.child.as_mut() else {
            return Ok(self.exit_status);
        };
        let status = child
            .try_wait()
            .map_err(|error| ProcessError::Wait { kind: error.kind() })?;
        if status.is_some() {
            self.child = None;
            self.exit_status = status;
        }
        Ok(status)
    }

    pub async fn within_deadline<T, Operation>(
        &self,
        phase: ProcessPhase,
        operation: Operation,
    ) -> Result<T, ProcessError>
    where
        Operation: Future<Output = T>,
    {
        timeout(self.deadline(phase), operation)
            .await
            .map_err(|_| ProcessError::DeadlineExceeded { phase })
    }

    pub async fn wait(mut self) -> Result<ProcessOutput, ProcessError> {
        let status = match self.reap_until(self.execution_timeout).await {
            Ok(Some(status)) => status,
            Ok(None) => {
                return self
                    .fail(ProcessError::DeadlineExceeded {
                        phase: ProcessPhase::Execution,
                    })
                    .await;
            }
            Err(error) => return self.fail(error).await,
        };

        let cleanup = self.cleanup_group_and_reap().await;
        let captured = self.collect_output().await;

        cleanup?;

        let (stdout, stderr) = captured?;
        Ok(output_from(Some(status), stdout, stderr))
    }

    pub async fn terminate(mut self) -> Result<ProcessOutput, ProcessError> {
        let cleanup = self.cleanup_group_and_reap().await;
        let captured = self.collect_output().await;

        let status = match cleanup {
            Ok(status) => status,
            Err(error) => return Err(error),
        };
        let (stdout, stderr) = captured?;
        Ok(output_from(status, stdout, stderr))
    }

    #[cfg(unix)]
    fn spawn_unix(spec: ProcessSpec) -> Result<Self, ProcessError> {
        let mut command = spec.command();
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .process_group(0);

        let mut child = command
            .spawn()
            .map_err(|error| ProcessError::Spawn { kind: error.kind() })?;
        let pid = child.id().ok_or_else(|| {
            let _ = child.start_kill();
            ProcessError::MissingProcessId
        })?;
        let process_group = i32::try_from(pid).map_err(|_| {
            let _ = child.start_kill();
            ProcessError::MissingProcessId
        })?;

        let stdout = child.stdout.take().ok_or_else(|| {
            let _ = signal_process_group(process_group, libc::SIGKILL);
            let _ = child.start_kill();
            ProcessError::MissingPipe {
                stream: CaptureStream::Stdout,
            }
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            let _ = signal_process_group(process_group, libc::SIGKILL);
            let _ = child.start_kill();
            ProcessError::MissingPipe {
                stream: CaptureStream::Stderr,
            }
        })?;

        Ok(Self {
            identity: ProcessIdentity { pid, process_group },
            child: Some(child),
            exit_status: None,
            process_group: Some(process_group),
            deadlines: spec.deadlines,
            execution_timeout: spec.timeout,
            stdout: Some(spawn_drain(stdout)),
            stderr: Some(spawn_drain(stderr)),
        })
    }

    async fn reap_until(&mut self, deadline: Duration) -> Result<Option<ExitStatus>, ProcessError> {
        let Some(child) = self.child.as_mut() else {
            return Ok(self.exit_status);
        };

        let result = timeout(deadline, child.wait()).await;
        match result {
            Ok(Ok(status)) => {
                self.child = None;
                self.exit_status = Some(status);
                Ok(Some(status))
            }
            Ok(Err(error)) => Err(ProcessError::Wait { kind: error.kind() }),
            Err(_) => Ok(None),
        }
    }

    async fn cleanup_group_and_reap(&mut self) -> Result<Option<ExitStatus>, ProcessError> {
        let shutdown_deadline = Instant::now() + self.deadlines.shutdown;
        let mut status = self.exit_status;
        let mut first_error = None;
        let mut term_sent = false;

        if let Some(process_group) = self.process_group {
            match send_termination_signal(process_group, TerminationSignal::Term) {
                Ok(SignalResult::Sent) => term_sent = true,
                Ok(SignalResult::Gone) => self.process_group = None,
                Err(kind) => {
                    first_error = Some(ProcessError::Signal {
                        signal: TerminationSignal::Term,
                        kind,
                    })
                }
            }
        }

        if term_sent {
            let grace_deadline = std::cmp::min(
                shutdown_deadline,
                Instant::now() + self.deadlines.termination_grace,
            );
            if self.child.is_some() {
                match self.reap_until(remaining(grace_deadline)).await {
                    Ok(Some(child_status)) => status = Some(child_status),
                    Ok(None) => {}
                    Err(error) => {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                }
            }
            if self.child.is_none() {
                let wait = remaining(grace_deadline);
                if !wait.is_zero() {
                    sleep(wait).await;
                }
            }
        }

        if let Some(process_group) = self.process_group {
            match send_termination_signal(process_group, TerminationSignal::Kill) {
                Ok(SignalResult::Sent | SignalResult::Gone) => self.process_group = None,
                Err(kind) => {
                    if first_error.is_none() {
                        first_error = Some(ProcessError::Signal {
                            signal: TerminationSignal::Kill,
                            kind,
                        });
                    }
                }
            }
        }

        if self.child.is_some() {
            match self.reap_until(remaining(shutdown_deadline)).await {
                Ok(Some(child_status)) => status = Some(child_status),
                Ok(None) => {
                    if first_error.is_none() {
                        first_error = Some(ProcessError::DeadlineExceeded {
                            phase: ProcessPhase::Shutdown,
                        });
                    }
                }
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }

        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(status)
        }
    }

    async fn collect_output(&mut self) -> Result<(CapturedStream, CapturedStream), ProcessError> {
        let deadline = Instant::now() + self.deadlines.shutdown;
        let stdout = match self.stdout.take() {
            Some(task) => join_drain(task, CaptureStream::Stdout, remaining(deadline)).await,
            None => Err(ProcessError::MissingPipe {
                stream: CaptureStream::Stdout,
            }),
        };
        let stdout = match stdout {
            Ok(stdout) => stdout,
            Err(error) => {
                self.discard_capture_tasks().await;
                return Err(error);
            }
        };

        let stderr = match self.stderr.take() {
            Some(task) => join_drain(task, CaptureStream::Stderr, remaining(deadline)).await,
            None => Err(ProcessError::MissingPipe {
                stream: CaptureStream::Stderr,
            }),
        };
        match stderr {
            Ok(stderr) => Ok((stdout, stderr)),
            Err(error) => {
                self.discard_capture_tasks().await;
                Err(error)
            }
        }
    }

    async fn discard_capture_tasks(&mut self) {
        if let Some(task) = self.stdout.take() {
            abort_drain(task).await;
        }
        if let Some(task) = self.stderr.take() {
            abort_drain(task).await;
        }
    }

    async fn fail(&mut self, error: ProcessError) -> Result<ProcessOutput, ProcessError> {
        let _ = self.cleanup_group_and_reap().await;
        self.discard_capture_tasks().await;
        Err(error)
    }

    fn abort_capture_tasks(&mut self) {
        if let Some(task) = self.stdout.take() {
            task.abort();
        }
        if let Some(task) = self.stderr.take() {
            task.abort();
        }
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        if let Some(process_group) = self.process_group.take() {
            let _ = send_termination_signal(process_group, TerminationSignal::Kill);
        }
        self.abort_capture_tasks();
        let _ = self.child.take();
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CapturedStream {
    bytes: Vec<u8>,
    truncated: bool,
}

type DrainTask = JoinHandle<io::Result<CapturedStream>>;

fn spawn_drain<R>(reader: R) -> DrainTask
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(capture(reader))
}

async fn capture<R>(mut reader: R) -> io::Result<CapturedStream>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::with_capacity(MAX_CAPTURE_BYTES);
    let mut truncated = false;
    let mut buffer = [0_u8; 8192];

    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }

        let available = MAX_CAPTURE_BYTES.saturating_sub(bytes.len());
        let retained = available.min(read);
        bytes.extend_from_slice(&buffer[..retained]);
        truncated |= retained < read;
    }

    Ok(CapturedStream { bytes, truncated })
}

async fn join_drain(
    task: DrainTask,
    stream: CaptureStream,
    deadline: Duration,
) -> Result<CapturedStream, ProcessError> {
    let mut task = task;
    match timeout(deadline, &mut task).await {
        Ok(Ok(Ok(output))) => Ok(output),
        Ok(Ok(Err(error))) => Err(ProcessError::Capture {
            stream,
            kind: error.kind(),
        }),
        Ok(Err(_)) => Err(ProcessError::Capture {
            stream,
            kind: io::ErrorKind::Other,
        }),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err(ProcessError::DeadlineExceeded {
                phase: ProcessPhase::Shutdown,
            })
        }
    }
}

async fn abort_drain(task: DrainTask) {
    task.abort();
    let _ = task.await;
}

fn output_from(
    status: Option<ExitStatus>,
    stdout: CapturedStream,
    stderr: CapturedStream,
) -> ProcessOutput {
    ProcessOutput {
        status_code: status.and_then(|status| status.code()),
        stdout: stdout.bytes,
        stderr: stderr.bytes,
        stdout_truncated: stdout.truncated,
        stderr_truncated: stderr.truncated,
    }
}

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SignalResult {
    Sent,
    Gone,
}

fn send_termination_signal(
    process_group: i32,
    signal: TerminationSignal,
) -> Result<SignalResult, io::ErrorKind> {
    let signal_number = match signal {
        TerminationSignal::Term => termination_signal_number::TERM,
        TerminationSignal::Kill => termination_signal_number::KILL,
    };
    signal_process_group(process_group, signal_number)
}

#[cfg(unix)]
mod termination_signal_number {
    pub const KILL: libc::c_int = libc::SIGKILL;
    pub const TERM: libc::c_int = libc::SIGTERM;
}

#[cfg(not(unix))]
mod termination_signal_number {
    pub const KILL: i32 = 9;
    pub const TERM: i32 = 15;
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn signal_process_group(
    process_group: i32,
    signal: libc::c_int,
) -> Result<SignalResult, io::ErrorKind> {
    if process_group <= 0 {
        return Err(io::ErrorKind::InvalidInput);
    }

    let result = unsafe { libc::kill(-(process_group as libc::pid_t), signal) };
    if result == 0 {
        return Ok(SignalResult::Sent);
    }

    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(SignalResult::Gone)
    } else {
        Err(error.kind())
    }
}

#[cfg(not(unix))]
fn signal_process_group(_process_group: i32, _signal: i32) -> Result<SignalResult, io::ErrorKind> {
    Err(io::ErrorKind::Unsupported)
}

#[cfg(all(unix, test))]
fn process_group_exists(process_group: i32) -> bool {
    matches!(
        signal_process_group(process_group, 0),
        Ok(SignalResult::Sent)
    )
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsString,
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use tokio::time::{Instant, sleep, timeout};

    use super::{
        CaptureStream, MAX_CAPTURE_BYTES, ProcessDeadlines, ProcessError, ProcessGuard,
        ProcessPhase, ProcessSpec,
    };

    static MARKER_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

    #[cfg(unix)]
    fn shell_spec(script: &str, marker: &Path) -> ProcessSpec {
        let mut spec = ProcessSpec::new(PathBuf::from("/bin/sh"));
        spec.arguments = vec![
            OsString::from("-c"),
            OsString::from(script),
            OsString::from("process-guard-test"),
            marker.as_os_str().to_os_string(),
        ];
        spec.deadlines = ProcessDeadlines {
            startup: Duration::from_secs(2),
            readiness: Duration::from_secs(2),
            shutdown: Duration::from_secs(2),
            termination_grace: Duration::from_millis(20),
        };
        spec
    }

    #[cfg(unix)]
    fn marker_path(name: &str) -> PathBuf {
        let sequence = MARKER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "opencode-harness-events-process-{name}-{}-{sequence}",
            std::process::id()
        ))
    }

    #[cfg(unix)]
    async fn wait_for_marker(path: &Path) -> (i32, i32) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Ok(contents) = fs::read_to_string(path)
                && let Some((parent, descendant)) =
                    contents.split_once(' ').and_then(|(parent, descendant)| {
                        Some((parent.parse().ok()?, descendant.trim().parse().ok()?))
                    })
            {
                return (parent, descendant);
            }

            assert!(
                Instant::now() < deadline,
                "process did not publish its readiness marker"
            );
            sleep(Duration::from_millis(5)).await;
        }
    }

    #[cfg(unix)]
    async fn assert_group_gone(process_group: i32) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if !super::process_group_exists(process_group) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "owned process group {process_group} remained alive"
            );
            sleep(Duration::from_millis(5)).await;
        }
    }

    #[cfg(unix)]
    #[allow(unsafe_code)]
    fn process_exists(pid: i32) -> bool {
        unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
    }

    #[cfg(unix)]
    async fn assert_process_gone(pid: i32) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if !process_exists(pid) {
                return;
            }
            assert!(Instant::now() < deadline, "process {pid} remained alive");
            sleep(Duration::from_millis(5)).await;
        }
    }

    #[cfg(unix)]
    const CHILD_WITH_DESCENDANT: &str =
        "sleep 30 & descendant=$!; printf '%s %s' \"$$\" \"$descendant\" > \"$1\"; exit 0";

    #[cfg(unix)]
    const TERM_HANDLED_CHILD_WITH_DESCENDANT: &str = "trap 'exit 0' TERM; sleep 30 & descendant=$!; printf '%s %s' \"$$\" \"$descendant\" > \"$1\"; wait";

    #[cfg(unix)]
    const WAITING_CHILD_WITH_DESCENDANT: &str =
        "sleep 30 & descendant=$!; printf '%s %s' \"$$\" \"$descendant\" > \"$1\"; wait";

    #[cfg(unix)]
    const TERM_RESISTANT_CHILD_WITH_DESCENDANT: &str = "trap '' TERM; sleep 30 & descendant=$!; printf '%s %s' \"$$\" \"$descendant\" > \"$1\"; wait";

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn success_reaps_the_child_and_descendant_process_group() {
        let marker = marker_path("success");
        let guard = ProcessGuard::spawn(shell_spec(CHILD_WITH_DESCENDANT, &marker))
            .await
            .expect("start process group");
        let identity = guard.identity();
        let (parent, descendant) = wait_for_marker(&marker).await;

        let output = guard.wait().await.expect("successful process completion");

        assert_eq!(output.status_code, Some(0));
        assert_ne!(parent, descendant);
        assert_group_gone(identity.process_group).await;
        let _ = fs::remove_file(marker);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn polling_exit_preserves_status_and_descendant_cleanup() {
        let marker = marker_path("polled-exit");
        let mut guard = ProcessGuard::spawn(shell_spec(CHILD_WITH_DESCENDANT, &marker))
            .await
            .expect("start process group");
        let identity = guard.identity();
        let _ = wait_for_marker(&marker).await;
        let deadline = Instant::now() + Duration::from_secs(2);
        let status = loop {
            if let Some(status) = guard.try_wait().expect("poll child exit") {
                break status;
            }
            assert!(Instant::now() < deadline, "child did not exit");
            sleep(Duration::from_millis(5)).await;
        };
        assert_eq!(status.code(), Some(0));
        assert_eq!(guard.try_wait().expect("poll cached exit"), Some(status));

        let output = guard.terminate().await.expect("clean process group");

        assert_eq!(output.status_code, Some(0));
        assert_group_gone(identity.process_group).await;
        let _ = fs::remove_file(marker);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn returned_error_path_terminates_the_owned_process_group() {
        let marker = marker_path("error");
        let guard = ProcessGuard::spawn(shell_spec(TERM_HANDLED_CHILD_WITH_DESCENDANT, &marker))
            .await
            .expect("start process group");
        let identity = guard.identity();
        let (parent, descendant) = wait_for_marker(&marker).await;
        assert_ne!(identity.pid, descendant as u32);

        #[derive(Debug, Eq, PartialEq)]
        enum ScenarioError {
            Returned,
        }

        let result: Result<(), ScenarioError> = async {
            let output = timeout(Duration::from_secs(2), guard.terminate())
                .await
                .expect("termination must be bounded")
                .expect("owned process termination");
            assert_eq!(output.status_code, Some(0));
            Err(ScenarioError::Returned)
        }
        .await;

        assert_eq!(result, Err(ScenarioError::Returned));
        assert_group_gone(identity.process_group).await;
        assert_process_gone(parent).await;
        assert_process_gone(descendant).await;
        let _ = fs::remove_file(marker);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn timeout_escalates_from_term_to_kill_and_reaps_the_group() {
        let marker = marker_path("timeout");
        let mut spec = shell_spec(TERM_RESISTANT_CHILD_WITH_DESCENDANT, &marker);
        spec.timeout = Duration::from_millis(50);
        let guard = ProcessGuard::spawn(spec)
            .await
            .expect("start process group");
        let identity = guard.identity();
        let (parent, descendant) = wait_for_marker(&marker).await;
        assert_eq!(identity.pid, parent as u32);

        let result = guard.wait().await;

        assert_eq!(
            result,
            Err(ProcessError::DeadlineExceeded {
                phase: ProcessPhase::Execution,
            })
        );
        assert_group_gone(identity.process_group).await;
        assert_process_gone(parent).await;
        assert_process_gone(descendant).await;
        let _ = fs::remove_file(marker);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn panic_drop_kills_the_owned_process_group_without_unwinding_again() {
        let marker = marker_path("panic");
        let spec = shell_spec(WAITING_CHILD_WITH_DESCENDANT, &marker);
        let task_marker = marker.clone();
        let task = tokio::spawn(async move {
            let guard = ProcessGuard::spawn(spec)
                .await
                .expect("start process group");
            let identity = guard.identity();
            let _ = wait_for_marker(&task_marker).await;
            panic!("scenario panic after child startup: {identity:?}");
        });
        let join_error = task.await.expect_err("the scenario task must panic");

        assert!(join_error.is_panic());
        let _ = wait_for_marker(&marker).await;
        let contents = fs::read_to_string(&marker).expect("panic test marker");
        let (parent, descendant) = contents.split_once(' ').expect("panic test marker fields");
        let _ = descendant.trim().parse::<i32>().expect("descendant pid");
        let process_group = parent.parse::<i32>().expect("parent pid");
        assert_group_gone(process_group).await;
        let _ = fs::remove_file(marker);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn output_capture_drains_continuously_and_retains_a_fixed_bound() {
        let marker = marker_path("output");
        let script = "printf '1 2' > \"$1\"; yes | head -c 100000; yes | head -c 100000 >&2";
        let guard = ProcessGuard::spawn(shell_spec(script, &marker))
            .await
            .expect("start output process group");
        let identity = guard.identity();
        let _ = wait_for_marker(&marker).await;

        let output = guard.wait().await.expect("complete output process");

        assert_eq!(output.stdout.len(), MAX_CAPTURE_BYTES);
        assert!(output.stdout_truncated);
        assert_eq!(output.stderr.len(), MAX_CAPTURE_BYTES);
        assert!(output.stderr_truncated);
        assert_group_gone(identity.process_group).await;
        let _ = fs::remove_file(marker);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn readiness_deadline_is_typed_and_bounded() {
        #[cfg(unix)]
        {
            let marker = marker_path("readiness");
            let guard = ProcessGuard::spawn(shell_spec("sleep 30", &marker))
                .await
                .expect("start readiness process group");
            let result = guard
                .within_deadline(ProcessPhase::Readiness, sleep(Duration::from_secs(3)))
                .await;

            assert_eq!(
                result,
                Err(ProcessError::DeadlineExceeded {
                    phase: ProcessPhase::Readiness,
                })
            );
            let _ = guard.terminate().await;
            let _ = fs::remove_file(marker);
        }
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn process_spec_executes_exact_invocation_fields() {
        let current_directory = marker_path("invocation-cwd");
        fs::create_dir(&current_directory).expect("create invocation cwd");
        let canonical_directory = fs::canonicalize(&current_directory).expect("canonical cwd");

        let mut spec = ProcessSpec::new(PathBuf::from("/bin/sh"));
        spec.arguments = vec![
            OsString::from("-c"),
            OsString::from(
                "printf '%s\\n' \"$0\" \"$1\" \"$2\" \"$TEST_NAME\" \"$TEST_LITERAL\"; pwd -P",
            ),
            OsString::from("probe-name"),
            OsString::from("literal argument"),
            OsString::from("$HOME"),
        ];
        spec.environment = vec![
            (OsString::from("TEST_NAME"), OsString::from("value")),
            (
                OsString::from("TEST_LITERAL"),
                OsString::from("$NOT_EXPANDED"),
            ),
        ];
        spec.current_directory = Some(current_directory);

        let guard = ProcessGuard::spawn(spec)
            .await
            .expect("start probe process");
        let output = guard.wait().await.expect("complete probe process");

        assert_eq!(output.status_code, Some(0));
        assert!(output.stderr.is_empty());
        assert_eq!(
            String::from_utf8(output.stdout).expect("probe output is utf8"),
            format!(
                "probe-name\nliteral argument\n$HOME\nvalue\n$NOT_EXPANDED\n{}\n",
                canonical_directory.display()
            )
        );
        assert_eq!(
            ProcessSpec::new(PathBuf::from("program")).deadline(ProcessPhase::Execution),
            ProcessSpec::new(PathBuf::from("program")).timeout
        );
        let _ = fs::remove_dir(canonical_directory);
    }

    #[test]
    fn capture_stream_types_are_distinct() {
        assert_ne!(CaptureStream::Stdout, CaptureStream::Stderr);
    }

    #[cfg(not(unix))]
    #[tokio::test(flavor = "current_thread")]
    async fn unsupported_platform_does_not_claim_process_success() {
        let result = ProcessGuard::spawn(ProcessSpec::new(PathBuf::from("program"))).await;
        assert_eq!(result, Err(ProcessError::UnsupportedPlatform));
    }
}
