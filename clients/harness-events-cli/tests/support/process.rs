use std::{
    io,
    process::{ExitStatus, Stdio},
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStdin, Command},
    task::JoinHandle,
    time::timeout,
};

use super::engine::{CapturedInvocation, FakeEngine};

const PROCESS_DEADLINE: Duration = Duration::from_secs(5);
const CLEANUP_DEADLINE: Duration = Duration::from_secs(2);
const ISOLATED_ENVIRONMENT: [&str; 3] = ["III_URL", "III_NAMESPACE", "III_WORKER_NAME"];

pub(crate) enum StdinMode {
    Closed(Vec<u8>),
    Open(Vec<u8>),
}

impl Default for StdinMode {
    fn default() -> Self {
        Self::Closed(Vec::new())
    }
}

pub(crate) struct ProcessScenario {
    pub(crate) args: Vec<String>,
    pub(crate) stdin: StdinMode,
    pub(crate) environment: Vec<(String, String)>,
    pub(crate) removed_environment: Vec<String>,
    pub(crate) timeout: Duration,
}

impl Default for ProcessScenario {
    fn default() -> Self {
        Self {
            args: Vec::new(),
            stdin: StdinMode::default(),
            environment: Vec::new(),
            removed_environment: Vec::new(),
            timeout: PROCESS_DEADLINE,
        }
    }
}

pub(crate) struct ProcessOutput {
    pub(crate) status: ExitStatus,
    pub(crate) stdout: Vec<u8>,
    pub(crate) stderr: Vec<u8>,
    pub(crate) connection_count: usize,
    pub(crate) captured: CapturedInvocation,
}

struct ChildOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

#[derive(Clone, Copy)]
enum ChildFailure {
    Incomplete,
    Cleanup,
}

impl ChildFailure {
    fn message(self) -> &'static str {
        match self {
            Self::Incomplete => "process test child did not complete",
            Self::Cleanup => "process test child cleanup failed",
        }
    }
}

type DrainTask = JoinHandle<io::Result<Vec<u8>>>;

pub(crate) async fn run(
    engine: &mut FakeEngine,
    scenario: ProcessScenario,
) -> Result<ProcessOutput, String> {
    let child = match run_child(scenario).await {
        Ok(child) => child,
        Err(error) => {
            let _ = finish_engine(engine, true).await;
            return Err(error.message().to_owned());
        }
    };
    let captured = match finish_engine(engine, false).await {
        Ok(captured) => captured,
        Err(()) => return Err("process test fake did not finish".to_owned()),
    };
    let connection_count = engine.connection_count();

    Ok(ProcessOutput {
        status: child.status,
        stdout: child.stdout,
        stderr: child.stderr,
        connection_count,
        captured,
    })
}

async fn run_child(scenario: ProcessScenario) -> Result<ChildOutput, ChildFailure> {
    let ProcessScenario {
        args,
        stdin,
        environment,
        removed_environment,
        timeout: deadline,
    } = scenario;
    let mut command = Command::new(env!("CARGO_BIN_EXE_harness-events"));
    command
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for variable in ISOLATED_ENVIRONMENT {
        command.env_remove(variable);
    }
    for variable in removed_environment {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }

    let mut child = command.spawn().map_err(|_| ChildFailure::Incomplete)?;
    let Some(stdin_handle) = child.stdin.take() else {
        return Err(failure_after_cleanup(&mut child, None, None, None).await);
    };
    let Some(stdout) = child.stdout.take() else {
        return Err(failure_after_cleanup(&mut child, Some(stdin_handle), None, None).await);
    };
    let stdout_task = drain(stdout);
    let Some(stderr) = child.stderr.take() else {
        return Err(
            failure_after_cleanup(&mut child, Some(stdin_handle), Some(stdout_task), None).await,
        );
    };
    let stderr_task = drain(stderr);

    let held_stdin = match timeout(deadline, write_stdin(stdin_handle, stdin)).await {
        Ok(Ok(held_stdin)) => held_stdin,
        Ok(Err(_)) | Err(_) => {
            return Err(failure_after_cleanup(
                &mut child,
                None,
                Some(stdout_task),
                Some(stderr_task),
            )
            .await);
        }
    };

    let status = match timeout(deadline, child.wait()).await {
        Ok(Ok(status)) => status,
        Ok(Err(_)) | Err(_) => {
            return Err(failure_after_cleanup(
                &mut child,
                held_stdin,
                Some(stdout_task),
                Some(stderr_task),
            )
            .await);
        }
    };
    drop(held_stdin);
    let stdout = collect_drain(stdout_task).await;
    let stderr = collect_drain(stderr_task).await;
    let (Ok(stdout), Ok(stderr)) = (stdout, stderr) else {
        return Err(ChildFailure::Incomplete);
    };

    Ok(ChildOutput {
        status,
        stdout,
        stderr,
    })
}

async fn write_stdin(mut stdin: ChildStdin, mode: StdinMode) -> io::Result<Option<ChildStdin>> {
    let (bytes, keep_open) = match mode {
        StdinMode::Closed(bytes) => (bytes, false),
        StdinMode::Open(bytes) => (bytes, true),
    };
    stdin.write_all(&bytes).await?;
    if keep_open {
        Ok(Some(stdin))
    } else {
        stdin.shutdown().await?;
        Ok(None)
    }
}

fn drain<R>(mut reader: R) -> DrainTask
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut output = Vec::new();
        reader.read_to_end(&mut output).await?;
        Ok(output)
    })
}

async fn collect_drain(mut task: DrainTask) -> Result<Vec<u8>, ()> {
    match timeout(CLEANUP_DEADLINE, &mut task).await {
        Ok(Ok(Ok(output))) => Ok(output),
        Ok(Ok(Err(_)) | Err(_)) => Err(()),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err(())
        }
    }
}

async fn failure_after_cleanup(
    child: &mut Child,
    stdin: Option<ChildStdin>,
    stdout: Option<DrainTask>,
    stderr: Option<DrainTask>,
) -> ChildFailure {
    match cleanup_child(child, stdin, stdout, stderr).await {
        Ok(()) => ChildFailure::Incomplete,
        Err(()) => ChildFailure::Cleanup,
    }
}

async fn cleanup_child(
    child: &mut Child,
    stdin: Option<ChildStdin>,
    stdout: Option<DrainTask>,
    stderr: Option<DrainTask>,
) -> Result<(), ()> {
    drop(stdin);
    let _ = child.start_kill();
    let reap = timeout(CLEANUP_DEADLINE, child.wait()).await;
    tokio::join!(
        async {
            if let Some(stdout) = stdout {
                discard_drain(stdout).await;
            }
        },
        async {
            if let Some(stderr) = stderr {
                discard_drain(stderr).await;
            }
        },
    );
    if matches!(reap, Ok(Ok(_))) {
        Ok(())
    } else {
        Err(())
    }
}

async fn discard_drain(task: DrainTask) {
    task.abort();
    let _ = task.await;
}

async fn finish_engine(engine: &mut FakeEngine, cancel: bool) -> Result<CapturedInvocation, ()> {
    let outcome = if cancel {
        timeout(CLEANUP_DEADLINE, engine.cancel()).await
    } else if engine.connection_count() == 0 {
        timeout(CLEANUP_DEADLINE, engine.finish_without_connection()).await
    } else {
        timeout(CLEANUP_DEADLINE, engine.finish()).await
    };

    match outcome {
        Ok(Ok(())) => Ok(engine.captured().await),
        Ok(Err(_)) | Err(_) => {
            let _ = timeout(CLEANUP_DEADLINE, engine.cancel()).await;
            let _ = timeout(CLEANUP_DEADLINE, engine.finish()).await;
            Err(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{process::Stdio, time::Duration};

    use tokio::{process::Command, time::timeout};

    use super::{ISOLATED_ENVIRONMENT, StdinMode, cleanup_child, drain, write_stdin};

    #[tokio::test(flavor = "current_thread")]
    async fn cleanup_closes_held_stdin_and_reaps_the_child() {
        for _ in 0..32 {
            let mut command = Command::new(env!("CARGO_BIN_EXE_harness-events"));
            command
                .args([
                    "observation",
                    "--hook-type",
                    "process-harness-cleanup",
                    "--project-name",
                    "process-harness-project",
                    "--current-working-directory",
                    "/workspace/process-harness",
                    "--timestamp",
                    "2026-09-19T20:14:33.123Z",
                    "--session-id",
                    "process-harness-session",
                ])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            for variable in ISOLATED_ENVIRONMENT {
                command.env_remove(variable);
            }

            let mut child = command.spawn().expect("spawn child with held stdin");
            let stdin = child.stdin.take().expect("child stdin is piped");
            let stdout = child.stdout.take().expect("child stdout is piped");
            let stderr = child.stderr.take().expect("child stderr is piped");
            let held_stdin = write_stdin(stdin, StdinMode::Open(Vec::new()))
                .await
                .expect("write the held-open stdin mode");

            timeout(
                Duration::from_secs(3),
                cleanup_child(
                    &mut child,
                    held_stdin,
                    Some(drain(stdout)),
                    Some(drain(stderr)),
                ),
            )
            .await
            .expect("cleanup must not return before the child is reaped")
            .expect("cleanup must report successful reap");

            assert!(
                child
                    .try_wait()
                    .expect("inspect child after cleanup")
                    .is_some(),
                "cleanup must reap the child before returning"
            );
        }
    }
}
