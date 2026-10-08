use std::{
    future::Future,
    io,
    os::unix::process::CommandExt,
    process::{ExitStatus, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
    task::JoinHandle,
    time::{Instant, sleep, timeout_at},
};

pub const STDERR_CAPTURE_LIMIT: usize = 8 * 1024;

const TERMINATION_GRACE: Duration = Duration::from_millis(500);
const CLEANUP_RESERVE: Duration = Duration::from_secs(1);
const CLEANUP_POLL_INTERVAL: Duration = Duration::from_millis(10);
const DRAIN_BUFFER_SIZE: usize = 4 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CaptureLimits {
    stdout: usize,
    stderr: usize,
}

impl CaptureLimits {
    pub const fn new(stdout: usize, stderr: usize) -> Self {
        Self {
            stdout,
            stderr: if stderr > STDERR_CAPTURE_LIMIT {
                STDERR_CAPTURE_LIMIT
            } else {
                stderr
            },
        }
    }
}

impl Default for CaptureLimits {
    fn default() -> Self {
        Self::new(STDERR_CAPTURE_LIMIT, STDERR_CAPTURE_LIMIT)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedOutput {
    bytes: Vec<u8>,
    truncated: bool,
}

impl CapturedOutput {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ProcessOutput {
    stdout: CapturedOutput,
    stderr: CapturedOutput,
}

struct CaptureState {
    output: CapturedOutput,
    complete: bool,
}

struct Drain {
    task: DrainTask,
    capture: Arc<Mutex<CaptureState>>,
}

#[cfg(test)]
#[derive(Clone)]
struct CaptureProbe {
    stdout: Arc<Mutex<CaptureState>>,
    stderr: Arc<Mutex<CaptureState>>,
}

#[cfg(test)]
impl CaptureProbe {
    fn output(&self) -> Option<ProcessOutput> {
        Some(ProcessOutput {
            stdout: completed_output(&self.stdout).ok()?,
            stderr: completed_output(&self.stderr).ok()?,
        })
    }
}

impl ProcessOutput {
    pub const fn stdout(&self) -> &CapturedOutput {
        &self.stdout
    }

    pub const fn stderr(&self) -> &CapturedOutput {
        &self.stderr
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProcessError {
    #[error("process spawn failed")]
    SpawnFailed,
    #[error("process output pipe was unavailable")]
    OutputPipeUnavailable,
    #[error("process readiness failed")]
    ReadinessFailed,
    #[error("process exited before readiness")]
    ExitedBeforeReadiness,
    #[error("process readiness timed out")]
    ReadinessTimedOut,
    #[error("process execution timed out")]
    ExecutionTimedOut,
    #[error("process wait failed")]
    WaitFailed,
    #[error("process group signal failed")]
    SignalFailed,
    #[error("process cleanup timed out")]
    CleanupTimedOut,
    #[error("process output drain failed")]
    OutputDrainFailed,
}

pub struct ProcessGuard {
    child: Child,
    process_id: u32,
    stdout: Option<Drain>,
    stderr: Option<Drain>,
    output: Option<ProcessOutput>,
    armed: bool,
}

type DrainTask = JoinHandle<io::Result<()>>;

impl ProcessGuard {
    pub fn spawn(command: &mut Command, limits: CaptureLimits) -> Result<Self, ProcessError> {
        unsafe {
            command.as_std_mut().pre_exec(|| {
                if libc::setpgid(0, 0) == -1 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
        command.stdout(Stdio::piped()).stderr(Stdio::piped());

        let child = command.spawn().map_err(|_| ProcessError::SpawnFailed)?;
        let process_id = child.id().ok_or(ProcessError::SpawnFailed)?;
        let mut guard = Self {
            child,
            process_id,
            stdout: None,
            stderr: None,
            output: None,
            armed: true,
        };
        let stdout = guard
            .child
            .stdout
            .take()
            .ok_or(ProcessError::OutputPipeUnavailable)?;
        guard.stdout = Some(drain(stdout, limits.stdout));
        let stderr = guard
            .child
            .stderr
            .take()
            .ok_or(ProcessError::OutputPipeUnavailable)?;
        guard.stderr = Some(drain(stderr, limits.stderr));
        Ok(guard)
    }

    pub const fn process_id(&self) -> u32 {
        self.process_id
    }

    pub fn output(&self) -> Option<&ProcessOutput> {
        self.output.as_ref()
    }

    #[cfg(test)]
    fn capture_probe(&self) -> Option<CaptureProbe> {
        Some(CaptureProbe {
            stdout: Arc::clone(&self.stdout.as_ref()?.capture),
            stderr: Arc::clone(&self.stderr.as_ref()?.capture),
        })
    }

    pub async fn wait_for_readiness<T, E>(
        &mut self,
        deadline: Instant,
        readiness: impl Future<Output = Result<T, E>>,
    ) -> Result<T, ProcessError> {
        let cleanup_start = cleanup_start(deadline);
        tokio::pin!(readiness);

        loop {
            let reaped = match self.child.try_wait() {
                Ok(status) => status.is_some(),
                Err(_) => {
                    return self
                        .return_after_cleanup(deadline, ProcessError::WaitFailed)
                        .await;
                }
            };
            if reaped {
                return self
                    .return_after_cleanup(deadline, ProcessError::ExitedBeforeReadiness)
                    .await;
            }
            let now = Instant::now();
            if now >= cleanup_start {
                return self
                    .return_after_cleanup(deadline, ProcessError::ReadinessTimedOut)
                    .await;
            }
            let next_check = (now + CLEANUP_POLL_INTERVAL).min(cleanup_start);

            tokio::select! {
                result = &mut readiness => match result {
                    Ok(value) => return Ok(value),
                    Err(_) => return self.return_after_cleanup(deadline, ProcessError::ReadinessFailed).await,
                },
                _ = sleep(next_check.saturating_duration_since(now)) => {}
            }
        }
    }

    pub async fn wait_for_exit_until(
        &mut self,
        deadline: Instant,
    ) -> Result<ExitStatus, ProcessError> {
        match timeout_at(cleanup_start(deadline), self.child.wait()).await {
            Ok(Ok(status)) => {
                self.cleanup_until(deadline).await?;
                Ok(status)
            }
            Ok(Err(_)) => {
                self.return_after_cleanup(deadline, ProcessError::WaitFailed)
                    .await
            }
            Err(_) => {
                self.return_after_cleanup(deadline, ProcessError::ExecutionTimedOut)
                    .await
            }
        }
    }

    pub async fn shutdown_until(&mut self, deadline: Instant) -> Result<(), ProcessError> {
        self.cleanup_until(deadline).await
    }

    async fn return_after_cleanup<T>(
        &mut self,
        deadline: Instant,
        error: ProcessError,
    ) -> Result<T, ProcessError> {
        self.cleanup_until(deadline).await?;
        Err(error)
    }

    async fn cleanup_until(&mut self, deadline: Instant) -> Result<(), ProcessError> {
        if !self.armed {
            return Ok(());
        }

        if let Err(error) = self.signal_group(libc::SIGTERM) {
            let _ = self.signal_group(libc::SIGKILL);
            return Err(error);
        }
        let termination_deadline = (Instant::now() + TERMINATION_GRACE).min(deadline);
        let terminated = match self.wait_for_group_exit(termination_deadline).await {
            Ok(terminated) => terminated,
            Err(error) => {
                let _ = self.signal_group(libc::SIGKILL);
                return Err(error);
            }
        };
        if !terminated {
            self.signal_group(libc::SIGKILL)?;
            let killed = match self.wait_for_group_exit(deadline).await {
                Ok(killed) => killed,
                Err(error) => {
                    let _ = self.signal_group(libc::SIGKILL);
                    return Err(error);
                }
            };
            if !killed {
                return Err(ProcessError::CleanupTimedOut);
            }
        }

        self.armed = false;
        self.collect_output_until(deadline).await
    }

    async fn wait_for_group_exit(&mut self, deadline: Instant) -> Result<bool, ProcessError> {
        loop {
            let reaped = self
                .child
                .try_wait()
                .map_err(|_| ProcessError::WaitFailed)?
                .is_some();
            if reaped && !self.group_exists_after_reap()? {
                return Ok(true);
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(false);
            }
            sleep(remaining.min(CLEANUP_POLL_INTERVAL)).await;
        }
    }

    async fn collect_output_until(&mut self, deadline: Instant) -> Result<(), ProcessError> {
        let stdout = self.stdout.take().ok_or(ProcessError::OutputDrainFailed)?;
        let stderr = self.stderr.take().ok_or(ProcessError::OutputDrainFailed)?;
        let (stdout, stderr) = tokio::join!(
            collect_drain(stdout, deadline),
            collect_drain(stderr, deadline),
        );
        self.output = Some(ProcessOutput {
            stdout: stdout?,
            stderr: stderr?,
        });
        Ok(())
    }

    fn signal_group(&mut self, signal: libc::c_int) -> Result<(), ProcessError> {
        let reaped = self
            .child
            .try_wait()
            .map(|status| status.is_some())
            .map_err(|_| ProcessError::WaitFailed);
        signal_group_if_owned(
            reaped,
            || self.group_exists_after_reap(),
            || self.signal_group_raw(signal),
        )
    }

    fn signal_group_raw(&self, signal: libc::c_int) -> Result<(), ProcessError> {
        let result = unsafe { libc::kill(-(self.process_id as libc::pid_t), signal) };
        let delivery = if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        };
        classify_group_signal_delivery(delivery)
    }

    fn group_exists_after_reap(&self) -> Result<bool, ProcessError> {
        let result = unsafe { libc::kill(-(self.process_id as libc::pid_t), 0) };
        let probe = if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        };
        classify_post_reap_group_probe(probe)
    }
}

fn signal_group_if_owned(
    reaped: Result<bool, ProcessError>,
    group_exists_after_reap: impl FnOnce() -> Result<bool, ProcessError>,
    signal_group: impl FnOnce() -> Result<(), ProcessError>,
) -> Result<(), ProcessError> {
    let reaped = reaped?;
    if reaped && !group_exists_after_reap()? {
        return Ok(());
    }

    signal_group()
}

fn classify_group_signal_delivery(delivery: io::Result<()>) -> Result<(), ProcessError> {
    match delivery {
        Ok(()) => Ok(()),
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => Ok(()),
        Err(_) => Err(ProcessError::SignalFailed),
    }
}

fn classify_post_reap_group_probe(probe: io::Result<()>) -> Result<bool, ProcessError> {
    match probe {
        Ok(()) => Ok(true),
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => Ok(false),
        Err(error) if error.raw_os_error() == Some(libc::EPERM) => Ok(false),
        Err(_) => Err(ProcessError::SignalFailed),
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.signal_group(libc::SIGKILL);
        }
    }
}

fn cleanup_start(deadline: Instant) -> Instant {
    deadline
        .checked_sub(CLEANUP_RESERVE)
        .unwrap_or_else(Instant::now)
}

fn drain<R>(mut reader: R, limit: usize) -> Drain
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let capture = Arc::new(Mutex::new(CaptureState {
        output: CapturedOutput {
            bytes: Vec::with_capacity(limit),
            truncated: false,
        },
        complete: false,
    }));
    let task_capture = Arc::clone(&capture);
    let task = tokio::spawn(async move {
        let mut buffer = [0; DRAIN_BUFFER_SIZE];

        loop {
            let read = reader.read(&mut buffer).await?;
            let mut capture = task_capture
                .lock()
                .map_err(|_| io::Error::other("process capture lock poisoned"))?;
            if read == 0 {
                capture.complete = true;
                return Ok(());
            }
            let retained = limit.saturating_sub(capture.output.bytes.len()).min(read);
            capture.output.bytes.extend_from_slice(&buffer[..retained]);
            capture.output.truncated |= retained < read;
        }
    });
    Drain { task, capture }
}

async fn collect_drain(
    mut drain: Drain,
    deadline: Instant,
) -> Result<CapturedOutput, ProcessError> {
    match timeout_at(deadline, &mut drain.task).await {
        Ok(Ok(Ok(()))) => completed_output(&drain.capture),
        Ok(Ok(Err(_)) | Err(_)) => Err(ProcessError::OutputDrainFailed),
        Err(_) => {
            drain.task.abort();
            let _ = drain.task.await;
            Err(ProcessError::OutputDrainFailed)
        }
    }
}

fn completed_output(capture: &Arc<Mutex<CaptureState>>) -> Result<CapturedOutput, ProcessError> {
    let capture = capture
        .lock()
        .map_err(|_| ProcessError::OutputDrainFailed)?;
    if capture.complete {
        Ok(capture.output.clone())
    } else {
        Err(ProcessError::OutputDrainFailed)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        fs, io,
        panic::{AssertUnwindSafe, catch_unwind},
        path::{Path, PathBuf},
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use tokio::{
        process::Command,
        time::{Instant, sleep, timeout},
    };

    use super::{
        CaptureLimits, CaptureProbe, ProcessError, ProcessGuard, ProcessOutput,
        STDERR_CAPTURE_LIMIT,
    };

    const RETAINED_STDOUT_BYTES: usize = 37;
    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    #[derive(Clone, Copy)]
    enum FixtureExit {
        Status(i32),
        Wait,
    }

    #[derive(Clone, Copy)]
    enum FixturePidPublication {
        Immediate,
        Delayed,
    }

    struct TemporaryDirectory {
        path: PathBuf,
    }

    impl TemporaryDirectory {
        fn new() -> Self {
            let sequence = NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "pi-harness-events-e2e-process-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("create process fixture directory");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TemporaryDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    struct Fixture {
        process: ProcessGuard,
        directory: TemporaryDirectory,
        pid_path: PathBuf,
        ready_path: PathBuf,
        release_path: Option<PathBuf>,
        direct_pid: u32,
    }

    impl Fixture {
        async fn wait_for_setup(&self) -> io::Result<u32> {
            wait_for_file(&self.ready_path).await;
            let descendant_pid = read_pid(&self.pid_path).await?;

            let group = unsafe { libc::getpgid(descendant_pid as libc::pid_t) };
            assert_eq!(group, self.direct_pid as libc::pid_t);
            Ok(descendant_pid)
        }

        fn release_delayed_pid(&self) {
            fs::write(
                self.release_path
                    .as_ref()
                    .expect("fixture must delay PID publication"),
                "",
            )
            .expect("release delayed PID publication");
        }

        fn into_parts(self, descendant_pid: u32) -> (ProcessGuard, TemporaryDirectory, u32, u32) {
            (
                self.process,
                self.directory,
                self.direct_pid,
                descendant_pid,
            )
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn normal_exit_reaps_the_direct_child_and_descendant_with_bounded_output() {
        let (mut process, _directory, direct_pid, descendant_pid) =
            start_fixture(FixtureExit::Status(0), false).await;

        let status = process
            .wait_for_exit_until(Instant::now() + Duration::from_secs(3))
            .await
            .expect("normal exit must complete cleanup");

        assert!(status.success());
        assert_reaped(&mut process, descendant_pid).await;
        assert_bounded_output(&process);
        assert_eq!(process.process_id(), direct_pid);
    }

    #[test]
    fn post_reap_probe_treats_absent_or_unowned_groups_as_gone() {
        assert_eq!(
            super::classify_post_reap_group_probe(Err(io::Error::from_raw_os_error(libc::ESRCH))),
            Ok(false)
        );
        assert_eq!(
            super::classify_post_reap_group_probe(Err(io::Error::from_raw_os_error(libc::EPERM))),
            Ok(false)
        );
        assert_eq!(
            super::classify_post_reap_group_probe(Err(io::Error::from_raw_os_error(libc::EINVAL))),
            Err(ProcessError::SignalFailed)
        );
    }

    #[test]
    fn failed_child_wait_skips_probe_and_signal_delivery() {
        let probes = Cell::new(0);
        let deliveries = Cell::new(0);

        assert_eq!(
            super::signal_group_if_owned(
                Err(ProcessError::WaitFailed),
                || {
                    probes.set(probes.get() + 1);
                    Ok(true)
                },
                || {
                    deliveries.set(deliveries.get() + 1);
                    Ok(())
                },
            ),
            Err(ProcessError::WaitFailed)
        );
        assert_eq!(probes.get(), 0);
        assert_eq!(deliveries.get(), 0);
    }

    #[test]
    fn reaped_unowned_group_skips_term_and_kill_delivery() {
        let probes = Cell::new(0);
        let delivered = RefCell::new(Vec::new());

        for signal in [libc::SIGTERM, libc::SIGKILL] {
            assert_eq!(
                super::signal_group_if_owned(
                    Ok(true),
                    || {
                        probes.set(probes.get() + 1);
                        super::classify_post_reap_group_probe(Err(io::Error::from_raw_os_error(
                            libc::EPERM,
                        )))
                    },
                    || {
                        delivered.borrow_mut().push(signal);
                        Ok(())
                    },
                ),
                Ok(())
            );
        }

        {
            let delivered = delivered.borrow();
            assert!(
                delivered.is_empty(),
                "post-reap EPERM must skip raw signal delivery, got {delivered:?}"
            );
        }
        assert_eq!(probes.get(), 2);
    }

    #[test]
    fn reaped_group_that_still_exists_receives_termination_signal() {
        let probes = Cell::new(0);
        let delivered = RefCell::new(Vec::new());

        assert_eq!(
            super::signal_group_if_owned(
                Ok(true),
                || {
                    probes.set(probes.get() + 1);
                    Ok(true)
                },
                || {
                    delivered.borrow_mut().push(libc::SIGTERM);
                    Ok(())
                },
            ),
            Ok(())
        );

        assert_eq!(probes.get(), 1);
        assert_eq!(delivered.borrow().as_slice(), &[libc::SIGTERM]);
    }

    #[test]
    fn unreaped_child_signals_without_a_post_reap_probe() {
        let delivered = RefCell::new(Vec::new());

        assert_eq!(
            super::signal_group_if_owned(
                Ok(false),
                || -> Result<bool, ProcessError> {
                    panic!("unreaped children must not use the post-reap probe")
                },
                || {
                    delivered.borrow_mut().push(libc::SIGTERM);
                    Ok(())
                },
            ),
            Ok(())
        );

        assert_eq!(delivered.borrow().as_slice(), &[libc::SIGTERM]);
    }

    #[test]
    fn raw_delivery_permission_error_remains_signal_failed() {
        let deliveries = Cell::new(0);

        assert_eq!(
            super::signal_group_if_owned(
                Ok(true),
                || Ok(true),
                || {
                    deliveries.set(deliveries.get() + 1);
                    super::classify_group_signal_delivery(Err(io::Error::from_raw_os_error(
                        libc::EPERM,
                    )))
                },
            ),
            Err(ProcessError::SignalFailed)
        );
        assert_eq!(deliveries.get(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn explicit_shutdown_reaps_the_direct_child_and_descendant_with_bounded_output() {
        let (mut process, _directory, _direct_pid, descendant_pid) =
            start_fixture(FixtureExit::Wait, false).await;

        process
            .shutdown_until(Instant::now() + Duration::from_secs(3))
            .await
            .expect("explicit shutdown must complete cleanup");

        assert_reaped(&mut process, descendant_pid).await;
        assert_bounded_output(&process);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn nonzero_leader_exit_reaps_the_direct_child_and_descendant_with_bounded_output() {
        let (mut process, _directory, _direct_pid, descendant_pid) =
            start_fixture(FixtureExit::Status(7), false).await;

        let status = process
            .wait_for_exit_until(Instant::now() + Duration::from_secs(3))
            .await
            .expect("nonzero exit must complete cleanup");

        assert_eq!(status.code(), Some(7));
        assert_reaped(&mut process, descendant_pid).await;
        assert_bounded_output(&process);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn readiness_failure_reaps_the_direct_child_and_descendant_with_bounded_output() {
        let (mut process, _directory, _direct_pid, descendant_pid) =
            start_fixture(FixtureExit::Wait, false).await;

        let error = process
            .wait_for_readiness(Instant::now() + Duration::from_secs(3), async {
                Err::<(), ()>(())
            })
            .await
            .expect_err("readiness failure must fail after cleanup");

        assert_eq!(error, ProcessError::ReadinessFailed);
        assert_reaped(&mut process, descendant_pid).await;
        assert_bounded_output(&process);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn readiness_timeout_reaps_the_direct_child_and_descendant_with_bounded_output() {
        let (mut process, _directory, _direct_pid, descendant_pid) =
            start_fixture(FixtureExit::Wait, true).await;

        let error = process
            .wait_for_readiness(
                Instant::now() + Duration::from_secs(2),
                std::future::pending::<Result<(), ()>>(),
            )
            .await
            .expect_err("readiness must time out after cleanup");

        assert_eq!(error, ProcessError::ReadinessTimedOut);
        assert_reaped(&mut process, descendant_pid).await;
        assert_bounded_output(&process);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execution_timeout_reaps_the_direct_child_and_descendant_with_bounded_output() {
        let (mut process, _directory, _direct_pid, descendant_pid) =
            start_fixture(FixtureExit::Wait, true).await;

        let error = process
            .wait_for_exit_until(Instant::now() + Duration::from_secs(2))
            .await
            .expect_err("running child must time out after cleanup");

        assert_eq!(error, ProcessError::ExecutionTimedOut);
        assert_reaped(&mut process, descendant_pid).await;
        assert_bounded_output(&process);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn panic_unwind_kills_the_owned_process_group() {
        let (process, _directory, direct_pid, descendant_pid) =
            start_fixture(FixtureExit::Wait, false).await;
        let probe = process
            .capture_probe()
            .expect("capture probe must be available before cleanup");

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _process = process;
            panic!("exercise process guard unwind cleanup");
        }));
        assert!(panic.is_err());

        wait_for_process_absence(direct_pid).await;
        wait_for_process_absence(descendant_pid).await;
        assert_bounded_capture(&wait_for_captured_output(&probe).await);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn delayed_pid_publication_waits_for_ready_before_reading_pid() {
        let fixture = spawn_fixture(FixtureExit::Wait, false, FixturePidPublication::Delayed).await;

        assert!(
            timeout(Duration::from_millis(100), fixture.wait_for_setup())
                .await
                .is_err(),
            "fixture setup must stay pending until delayed PID publication is released"
        );

        fixture.release_delayed_pid();
        let descendant_pid = fixture
            .wait_for_setup()
            .await
            .expect("setup must complete after delayed PID publication is released");
        let (mut process, _directory, _direct_pid, descendant_pid) =
            fixture.into_parts(descendant_pid);

        let error = process
            .wait_for_readiness(Instant::now() + Duration::from_secs(3), async {
                Err::<(), ()>(())
            })
            .await
            .expect_err("readiness failure must clean up the delayed fixture");

        assert_eq!(error, ProcessError::ReadinessFailed);
        assert_reaped(&mut process, descendant_pid).await;
        assert_bounded_output(&process);
    }

    async fn start_fixture(
        exit: FixtureExit,
        ignore_termination: bool,
    ) -> (ProcessGuard, TemporaryDirectory, u32, u32) {
        let fixture =
            spawn_fixture(exit, ignore_termination, FixturePidPublication::Immediate).await;
        let descendant_pid = fixture.wait_for_setup().await.expect("read descendant pid");
        fixture.into_parts(descendant_pid)
    }

    async fn spawn_fixture(
        exit: FixtureExit,
        ignore_termination: bool,
        pid_publication: FixturePidPublication,
    ) -> Fixture {
        let directory = TemporaryDirectory::new();
        let pid_path = directory.path().join("descendant.pid");
        let ready_path = directory.path().join("output-ready");
        let release_path = directory.path().join("release-pid");
        let delayed_pid_publication = matches!(pid_publication, FixturePidPublication::Delayed);
        let script = format!(
            r#"
                {parent_trap}
                {child}
                {pid_publication}
                printf '%s' "$!" > "$1"
                printf '%9000s' '' | tr ' ' x
                printf '%9000s' '' | tr ' ' y >&2
                printf ready > "$2"
                {finish}
            "#,
            parent_trap = if ignore_termination {
                "trap '' TERM"
            } else {
                ""
            },
            child = if ignore_termination {
                "(trap '' TERM; sleep 30) &"
            } else {
                "sleep 30 &"
            },
            pid_publication = if delayed_pid_publication {
                r#"
                    : > "$1"
                    while [ ! -f "$3" ]; do
                        sleep 0.01
                    done
                "#
            } else {
                ""
            },
            finish = match exit {
                FixtureExit::Status(status) => format!("exit {status}"),
                FixtureExit::Wait => "wait".to_owned(),
            },
        );
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg(script)
            .arg("--")
            .arg(&pid_path)
            .arg(&ready_path)
            .arg(&release_path);

        let process = ProcessGuard::spawn(
            &mut command,
            CaptureLimits::new(RETAINED_STDOUT_BYTES, STDERR_CAPTURE_LIMIT + 1),
        )
        .expect("spawn process fixture");
        let direct_pid = process.process_id();
        Fixture {
            process,
            directory,
            pid_path,
            ready_path,
            release_path: if delayed_pid_publication {
                Some(release_path)
            } else {
                None
            },
            direct_pid,
        }
    }

    async fn assert_reaped(process: &mut ProcessGuard, descendant_pid: u32) {
        assert!(
            process
                .child
                .try_wait()
                .expect("inspect direct child after cleanup")
                .is_some(),
            "cleanup must reap the direct child"
        );
        wait_for_process_absence(descendant_pid).await;
    }

    fn assert_bounded_output(process: &ProcessGuard) {
        assert_bounded_capture(process.output().expect("cleanup must finish both drains"));
    }

    fn assert_bounded_capture(output: &ProcessOutput) {
        assert_eq!(output.stdout().bytes().len(), RETAINED_STDOUT_BYTES);
        assert!(output.stdout().truncated());
        assert!(output.stdout().bytes().iter().all(|byte| *byte == b'x'));
        assert_eq!(output.stderr().bytes().len(), STDERR_CAPTURE_LIMIT);
        assert!(output.stderr().truncated());
        assert!(output.stderr().bytes().iter().all(|byte| *byte == b'y'));
    }

    async fn wait_for_captured_output(probe: &CaptureProbe) -> ProcessOutput {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if let Some(output) = probe.output() {
                return output;
            }
            assert!(Instant::now() < deadline, "capture drain did not reach EOF");
            sleep(Duration::from_millis(10)).await;
        }
    }

    async fn read_pid(path: &Path) -> io::Result<u32> {
        wait_for_file(path).await;
        fs::read_to_string(path)?
            .trim()
            .parse()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    async fn wait_for_file(path: &Path) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if path.is_file() {
                return;
            }
            assert!(Instant::now() < deadline, "fixture did not create {path:?}");
            sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_process_absence(pid: u32) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if process_is_absent(pid) {
                return;
            }
            assert!(Instant::now() < deadline, "process {pid} survived cleanup");
            sleep(Duration::from_millis(10)).await;
        }
    }

    fn process_is_absent(pid: u32) -> bool {
        (unsafe { libc::kill(pid as libc::pid_t, 0) == -1 })
            && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
    }
}
