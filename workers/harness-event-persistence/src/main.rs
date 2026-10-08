use std::{future::Future, io, process::ExitCode};

use harness_event_persistence::{
    config::Config,
    runtime::{WorkerRuntime, WorkerStartupError},
};

#[tokio::main]
async fn main() -> ExitCode {
    match Config::from_env() {
        Ok(config) => {
            let mut termination = match TerminationHandlers::install() {
                Ok(termination) => termination,
                Err(error) => {
                    eprintln!("worker termination handler setup failed: {error}");
                    return ExitCode::FAILURE;
                }
            };
            let runtime = match WorkerRuntime::start(&config) {
                Ok(runtime) => runtime,
                Err(error) => {
                    eprintln!("worker startup registration failed: {error}");
                    return ExitCode::FAILURE;
                }
            };

            let termination_wait = termination.wait();
            let readiness_wait = runtime.wait_until_ready();
            match wait_for_startup(readiness_wait, termination_wait, || runtime.shutdown()).await {
                StartupOutcome::Ready(Ok(())) => {}
                StartupOutcome::Ready(Err(error)) => {
                    eprintln!("worker startup readiness failed: {error}");
                    return ExitCode::FAILURE;
                }
                StartupOutcome::Terminated(Ok(())) => {
                    eprintln!("worker termination signal received during startup readiness");
                    return ExitCode::FAILURE;
                }
                StartupOutcome::Terminated(Err(error)) => {
                    eprintln!("worker termination signal failed: {error}");
                    return ExitCode::FAILURE;
                }
            }

            eprintln!("worker ready");
            if let Err(error) = termination.wait().await {
                eprintln!("worker termination signal failed: {error}");
                runtime.shutdown();
                return ExitCode::FAILURE;
            }

            runtime.shutdown();
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("worker startup configuration failed: {error}");
            ExitCode::FAILURE
        }
    }
}

#[derive(Debug)]
enum StartupOutcome {
    Ready(Result<(), WorkerStartupError>),
    Terminated(io::Result<()>),
}

impl StartupOutcome {
    fn is_ready(&self) -> bool {
        matches!(self, Self::Ready(Ok(())))
    }
}

async fn wait_for_startup<Readiness, Termination, Shutdown>(
    readiness: Readiness,
    termination: Termination,
    shutdown: Shutdown,
) -> StartupOutcome
where
    Readiness: Future<Output = Result<(), WorkerStartupError>>,
    Termination: Future<Output = io::Result<()>>,
    Shutdown: FnOnce(),
{
    let outcome = tokio::select! {
        result = readiness => StartupOutcome::Ready(result),
        result = termination => StartupOutcome::Terminated(result),
    };

    if !outcome.is_ready() {
        shutdown();
    }

    outcome
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
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use super::{StartupOutcome, wait_for_startup};
    use harness_event_persistence::runtime::WorkerStartupError;

    #[tokio::test]
    async fn startup_termination_wins_over_pending_readiness_and_shuts_down() {
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let shutdown_flag = Arc::clone(&shutdown_called);
        let readiness = std::future::pending::<Result<(), WorkerStartupError>>();
        let outcome = wait_for_startup(readiness, async { Ok(()) }, move || {
            shutdown_flag.store(true, Ordering::SeqCst);
        })
        .await;

        assert!(matches!(outcome, StartupOutcome::Terminated(Ok(()))));
        assert!(shutdown_called.load(Ordering::SeqCst));
    }
}
