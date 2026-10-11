use std::time::Duration;

use thiserror::Error;
use tokio::time::{Instant, sleep};

use crate::{
    ArtifactKind, ArtifactPathError, ProcessError, ProcessGuard, ProcessIdentity, ProcessPhase,
    ProcessSpec, validate_path,
};

const EXIT_POLL_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ContextWorkerError {
    #[error(transparent)]
    ArtifactPath(#[from] ArtifactPathError),
    #[error("owned context-worker process failed: {0}")]
    Process(#[from] ProcessError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextWorkerExit {
    pub child: ProcessIdentity,
    pub status_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stdout_truncated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContextWorkerOutcome {
    Completed(ContextWorkerExit),
    TimedOut(ContextWorkerExit),
}

pub struct ContextWorkerProcess {
    child: ProcessIdentity,
    guard: ProcessGuard,
}

impl ContextWorkerProcess {
    pub async fn spawn(spec: ProcessSpec) -> Result<Self, ContextWorkerError> {
        validate_path(ArtifactKind::McpWorker, &spec.program)?;
        let guard = ProcessGuard::spawn(spec).await?;
        let child = guard.identity();
        Ok(Self { child, guard })
    }

    pub fn child_identity(&self) -> ProcessIdentity {
        self.child
    }

    pub async fn wait_for_exit(mut self) -> Result<ContextWorkerOutcome, ContextWorkerError> {
        let deadline = Instant::now() + self.guard.deadline(ProcessPhase::Execution);
        loop {
            let pending = self.guard.try_wait()?;
            if pending.is_some() {
                return Ok(ContextWorkerOutcome::Completed(self.finish().await?));
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(ContextWorkerOutcome::TimedOut(self.finish().await?));
            }
            sleep(remaining.min(EXIT_POLL_INTERVAL)).await;
        }
    }

    pub async fn terminate(self) -> Result<ContextWorkerExit, ContextWorkerError> {
        self.finish().await
    }

    async fn finish(self) -> Result<ContextWorkerExit, ContextWorkerError> {
        let output = self.guard.terminate().await?;
        Ok(ContextWorkerExit {
            child: self.child,
            status_code: output.status_code,
            stdout: output.stdout,
            stdout_truncated: output.stdout_truncated,
        })
    }
}

#[cfg(all(unix, test))]
mod tests {
    use std::{
        ffi::OsString,
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use tokio::time::{Instant, sleep};

    use crate::{
        ArtifactKind, ArtifactPathError, ProcessSpec,
        context_worker::{ContextWorkerError, ContextWorkerOutcome, ContextWorkerProcess},
        process::MAX_CAPTURE_BYTES,
    };

    static MARKER_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

    fn worker_spec(script: &str, marker: &Path) -> ProcessSpec {
        let mut spec = ProcessSpec::new(PathBuf::from("/bin/sh"));
        spec.arguments = vec![
            OsString::from("-c"),
            OsString::from(script),
            OsString::from("context-worker-test"),
            marker.as_os_str().to_os_string(),
        ];
        spec
    }

    fn marker_path(name: &str) -> PathBuf {
        let sequence = MARKER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "opencode-harness-events-context-worker-{name}-{}-{sequence}",
            std::process::id()
        ))
    }

    async fn wait_for_pid_marker(path: &Path) -> (i32, i32) {
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
                "worker did not publish its readiness marker"
            );
            sleep(Duration::from_millis(5)).await;
        }
    }

    #[allow(unsafe_code)]
    fn process_exists(pid: i32) -> bool {
        unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
    }

    #[allow(unsafe_code)]
    fn process_group_exists(process_group: i32) -> bool {
        unsafe { libc::kill(-(process_group as libc::pid_t), 0) == 0 }
    }

    async fn assert_process_gone(published_pid: u32) {
        let Ok(pid) = i32::try_from(published_pid) else {
            panic!("published worker pid must fit in the platform pid range")
        };
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if !process_exists(pid) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "published retrieval child {pid} remained alive"
            );
            sleep(Duration::from_millis(5)).await;
        }
    }

    async fn assert_group_gone(process_group: i32) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if !process_group_exists(process_group) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "owned process group {process_group} remained alive"
            );
            sleep(Duration::from_millis(5)).await;
        }
    }

    const EXITING_WORKER: &str = "printf 'context-forwarded\\n'; exit 0";

    const FAILING_WORKER: &str = "echo worker-probe-protected-stderr >&2; exit 3";

    const TERM_HANDLING_WORKER: &str = "trap 'exit 0' TERM; sleep 30 & descendant=$!; printf '%s %s' \"$$\" \"$descendant\" > \"$1\"; wait";

    const TERM_RESISTANT_WORKER: &str = "trap '' TERM; sleep 30 & descendant=$!; printf '%s %s' \"$$\" \"$descendant\" > \"$1\"; wait";

    #[tokio::test(flavor = "current_thread")]
    async fn success_exit_publishes_identity_and_observes_direct_child_exit() {
        let marker = marker_path("success");
        let process = ContextWorkerProcess::spawn(worker_spec(EXITING_WORKER, &marker))
            .await
            .expect("launch owned context worker");
        let published = process.child_identity();

        let outcome = process.wait_for_exit().await.expect("worker completion");

        let ContextWorkerOutcome::Completed(evidence) = outcome else {
            panic!("a spontaneously exiting worker completes before its deadline");
        };
        assert_eq!(evidence.child, published);
        assert_eq!(evidence.status_code, Some(0));
        assert_eq!(evidence.stdout, b"context-forwarded\n".to_vec());
        assert!(!evidence.stdout_truncated);
        assert_process_gone(published.pid).await;
        assert_group_gone(published.process_group).await;
        let _ = fs::remove_file(marker);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn error_exit_status_is_observed_and_stderr_is_discarded() {
        let marker = marker_path("error");
        let process = ContextWorkerProcess::spawn(worker_spec(FAILING_WORKER, &marker))
            .await
            .expect("launch owned context worker");
        let published = process.child_identity();

        let outcome = process.wait_for_exit().await.expect("worker completion");

        let ContextWorkerOutcome::Completed(evidence) = outcome else {
            panic!("a spontaneously exiting worker completes before its deadline");
        };
        assert_eq!(evidence.child, published);
        assert_eq!(evidence.status_code, Some(3));
        assert!(evidence.stdout.is_empty());

        let evidence_report = format!("{evidence:?}");
        assert!(
            !evidence_report.contains("worker-probe-protected-stderr"),
            "worker stderr must be drained and discarded"
        );
        assert_process_gone(published.pid).await;
        assert_group_gone(published.process_group).await;
        let _ = fs::remove_file(marker);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execution_deadline_reports_timed_out_with_direct_child_exit() {
        let marker = marker_path("timeout");
        let mut spec = worker_spec(TERM_RESISTANT_WORKER, &marker);
        spec.timeout = Duration::from_millis(60);
        let process = ContextWorkerProcess::spawn(spec)
            .await
            .expect("launch owned context worker");
        let published = process.child_identity();
        let (parent, descendant) = wait_for_pid_marker(&marker).await;

        let outcome = process.wait_for_exit().await;

        let ContextWorkerOutcome::TimedOut(evidence) = outcome.expect("bounded timeout resolution")
        else {
            panic!("a deadline bound must resolve the still-running worker as timed out");
        };
        assert_eq!(evidence.child, published);
        assert_eq!(evidence.status_code, None);
        assert!(evidence.stdout.is_empty());
        assert_process_gone(published.pid).await;
        assert_process_gone(parent as u32).await;
        assert_process_gone(descendant as u32).await;
        assert_group_gone(published.process_group).await;
        let _ = fs::remove_file(marker);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn error_phase_terminate_exits_directly_within_the_shutdown_bound() {
        let marker = marker_path("terminate");
        let process = ContextWorkerProcess::spawn(worker_spec(TERM_HANDLING_WORKER, &marker))
            .await
            .expect("launch owned context worker");
        let published = process.child_identity();
        let (parent, descendant) = wait_for_pid_marker(&marker).await;
        assert_eq!(parent as u32, published.pid);

        let evidence = process
            .terminate()
            .await
            .expect("bounded context worker termination");

        assert_eq!(evidence.child, published);
        assert_eq!(evidence.status_code, Some(0));
        assert!(evidence.stdout.is_empty());
        assert_process_gone(published.pid).await;
        assert_process_gone(descendant as u32).await;
        assert_group_gone(published.process_group).await;
        let _ = fs::remove_file(marker);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn spawn_rejects_blank_nonexistent_and_wrong_worker_paths() {
        assert!(matches!(
            ContextWorkerProcess::spawn(ProcessSpec::new(PathBuf::from("   "))).await,
            Err(ContextWorkerError::ArtifactPath(ArtifactPathError::Blank {
                artifact: ArtifactKind::McpWorker
            }))
        ));
        assert!(matches!(
            ContextWorkerProcess::spawn(ProcessSpec::new(marker_path("nonexistent-worker"))).await,
            Err(ContextWorkerError::ArtifactPath(
                ArtifactPathError::Unusable {
                    artifact: ArtifactKind::McpWorker,
                    ..
                }
            ))
        ));

        let directory = marker_path("wrong-worker-directory");
        fs::create_dir(&directory).expect("create wrong worker fixture");
        let wrong_worker = ContextWorkerProcess::spawn(ProcessSpec::new(directory.clone())).await;
        let _ = fs::remove_dir(directory);
        assert!(matches!(
            wrong_worker,
            Err(ContextWorkerError::ArtifactPath(
                ArtifactPathError::Unusable {
                    artifact: ArtifactKind::McpWorker,
                    ..
                }
            ))
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn forwarded_stdout_capture_retains_a_fixed_bound() {
        let marker = marker_path("bounded-stdout");
        let process =
            ContextWorkerProcess::spawn(worker_spec("yes | head -c 100000 >&1; exit 0", &marker))
                .await
                .expect("launch owned context worker");
        let published = process.child_identity();

        let outcome = process.wait_for_exit().await.expect("worker completion");

        let ContextWorkerOutcome::Completed(evidence) = outcome else {
            panic!("a spontaneously exiting worker completes before its deadline");
        };
        assert_eq!(evidence.child, published);
        assert_eq!(evidence.stdout.len(), MAX_CAPTURE_BYTES);
        assert!(evidence.stdout_truncated);
        assert_process_gone(published.pid).await;
        assert_group_gone(published.process_group).await;
        let _ = fs::remove_file(marker);
    }
}
