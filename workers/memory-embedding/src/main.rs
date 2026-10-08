use std::{future::Future, io, process::ExitCode};

use memory_embedding::{
    config::Config,
    runtime::{
        RuntimeReadinessError, RuntimeShutdownError, WorkerRuntime, spawn_stdin_eof_watcher,
        wait_for_detached_eof, wait_for_termination,
    },
};

#[tokio::main]
async fn main() -> ExitCode {
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(_) => {
            eprintln!("worker startup configuration failed");
            return ExitCode::FAILURE;
        }
    };
    let mut termination = match TerminationHandlers::install() {
        Ok(termination) => termination,
        Err(_) => {
            eprintln!("worker termination handler setup failed");
            return ExitCode::FAILURE;
        }
    };
    let stdin_eof = match spawn_stdin_eof_watcher() {
        Ok(watcher) => wait_for_detached_eof(watcher),
        Err(_) => {
            eprintln!("worker stdin eof watcher setup failed");
            return ExitCode::FAILURE;
        }
    };
    tokio::pin!(stdin_eof);
    let runtime = match WorkerRuntime::start(&config) {
        Ok(runtime) => runtime,
        Err(_) => {
            eprintln!("worker startup registration failed");
            return ExitCode::FAILURE;
        }
    };

    let (startup, startup_shutdown) = wait_for_startup(
        runtime.wait_until_ready(),
        wait_for_termination(termination.wait(), stdin_eof.as_mut()),
        || runtime.shutdown(config.shutdown_timeout),
    )
    .await;
    match startup {
        StartupOutcome::Ready(Ok(())) => {}
        StartupOutcome::Ready(Err(_)) => {
            eprintln!("worker startup readiness failed");
            if startup_shutdown.is_err() {
                eprintln!("worker shutdown failed");
            }
            return ExitCode::FAILURE;
        }
        StartupOutcome::Terminated(Ok(())) => {
            eprintln!("worker termination received during startup readiness");
            if startup_shutdown.is_err() {
                eprintln!("worker shutdown failed");
            }
            return ExitCode::FAILURE;
        }
        StartupOutcome::Terminated(Err(_)) => {
            eprintln!("worker termination wait failed");
            if startup_shutdown.is_err() {
                eprintln!("worker shutdown failed");
            }
            return ExitCode::FAILURE;
        }
    }

    eprintln!("worker ready");
    let termination = wait_for_termination(termination.wait(), stdin_eof.as_mut()).await;
    let shutdown = runtime.shutdown(config.shutdown_timeout).await;
    match (termination, shutdown) {
        (Ok(()), Ok(())) => ExitCode::SUCCESS,
        (Ok(()), Err(_)) => {
            eprintln!("worker shutdown failed");
            ExitCode::FAILURE
        }
        (Err(_), Ok(())) => {
            eprintln!("worker termination wait failed");
            ExitCode::FAILURE
        }
        (Err(_), Err(_)) => {
            eprintln!("worker termination wait failed");
            eprintln!("worker shutdown failed");
            ExitCode::FAILURE
        }
    }
}

#[derive(Debug)]
enum StartupOutcome {
    Ready(Result<(), RuntimeReadinessError>),
    Terminated(io::Result<()>),
}

impl StartupOutcome {
    fn is_ready(&self) -> bool {
        matches!(self, Self::Ready(Ok(())))
    }
}

async fn wait_for_startup<Readiness, Termination, Shutdown, ShutdownFuture>(
    readiness: Readiness,
    termination: Termination,
    shutdown: Shutdown,
) -> (StartupOutcome, Result<(), RuntimeShutdownError>)
where
    Readiness: Future<Output = Result<(), RuntimeReadinessError>>,
    Termination: Future<Output = io::Result<()>>,
    Shutdown: FnOnce() -> ShutdownFuture,
    ShutdownFuture: Future<Output = Result<(), RuntimeShutdownError>>,
{
    let outcome = tokio::select! {
        result = readiness => StartupOutcome::Ready(result),
        result = termination => StartupOutcome::Terminated(result),
    };
    let shutdown = if outcome.is_ready() {
        Ok(())
    } else {
        shutdown().await
    };

    (outcome, shutdown)
}

#[cfg(unix)]
struct TerminationHandlers {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl TerminationHandlers {
    fn install() -> io::Result<Self> {
        Ok(Self {
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?,
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
        })
    }

    async fn wait(&mut self) -> io::Result<()> {
        tokio::select! {
            _ = self.interrupt.recv() => Ok(()),
            _ = self.terminate.recv() => Ok(()),
        }
    }
}

#[cfg(windows)]
struct TerminationHandlers {
    ctrl_c: tokio::signal::windows::CtrlC,
    ctrl_close: tokio::signal::windows::CtrlClose,
}

#[cfg(windows)]
impl TerminationHandlers {
    fn install() -> io::Result<Self> {
        Ok(Self {
            ctrl_c: tokio::signal::windows::ctrl_c()?,
            ctrl_close: tokio::signal::windows::ctrl_close()?,
        })
    }

    async fn wait(&mut self) -> io::Result<()> {
        tokio::select! {
            _ = self.ctrl_c.recv() => Ok(()),
            _ = self.ctrl_close.recv() => Ok(()),
        }
    }
}

#[cfg(all(not(unix), not(windows)))]
struct TerminationHandlers;

#[cfg(all(not(unix), not(windows)))]
impl TerminationHandlers {
    fn install() -> io::Result<Self> {
        Ok(Self)
    }

    async fn wait(&mut self) -> io::Result<()> {
        tokio::signal::ctrl_c().await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        sync::{Arc, Mutex},
    };

    use super::{StartupOutcome, wait_for_startup};
    use memory_embedding::runtime::{RuntimeShutdownError, wait_for_termination};

    #[tokio::test]
    async fn startup_termination_runs_shutdown_before_returning() {
        let sequence = Arc::new(Mutex::new(Vec::new()));
        let shutdown_sequence = Arc::clone(&sequence);
        let (outcome, shutdown) = wait_for_startup(
            pending(),
            async {
                sequence
                    .lock()
                    .expect("startup sequence lock should not be poisoned")
                    .push("termination");
                Ok(())
            },
            move || async move {
                shutdown_sequence
                    .lock()
                    .expect("startup sequence lock should not be poisoned")
                    .push("shutdown");
                Ok(())
            },
        )
        .await;

        assert!(matches!(outcome, StartupOutcome::Terminated(Ok(()))));
        assert_eq!(shutdown, Ok(()));
        assert_eq!(
            *sequence
                .lock()
                .expect("startup sequence lock should not be poisoned"),
            vec!["termination", "shutdown"]
        );
    }

    #[tokio::test]
    async fn startup_readiness_does_not_shutdown_the_runtime() {
        let shutdown_called = Arc::new(Mutex::new(false));
        let shutdown_flag = Arc::clone(&shutdown_called);
        let (outcome, shutdown) =
            wait_for_startup(async { Ok(()) }, pending(), move || async move {
                *shutdown_flag
                    .lock()
                    .expect("shutdown flag lock should not be poisoned") = true;
                Ok(())
            })
            .await;

        assert!(matches!(outcome, StartupOutcome::Ready(Ok(()))));
        assert_eq!(shutdown, Ok(()));
        assert!(
            !*shutdown_called
                .lock()
                .expect("shutdown flag lock should not be poisoned")
        );
    }

    #[tokio::test]
    async fn stdin_eof_terminates_without_waiting_for_a_signal() {
        assert!(matches!(
            wait_for_termination(pending(), async { Ok(()) }).await,
            Ok(())
        ));
    }

    #[test]
    fn startup_shutdown_errors_remain_content_safe() {
        assert_eq!(
            RuntimeShutdownError::DrainTimeout.to_string(),
            "runtime_shutdown_drain_timeout"
        );
    }
}
