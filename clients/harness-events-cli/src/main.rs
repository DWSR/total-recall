use std::{io, process::ExitCode};

use clap::Parser as _;
use harness_events_cli::{
    CommandRunError,
    app::{InvokeError, SubmitError},
    cli::Cli,
    config::ClientConfig,
    run_command, sdk,
};

#[tokio::main]
async fn main() -> ExitCode {
    let Cli { command } = Cli::parse();
    let config = ClientConfig::from_env();
    let stdin = io::stdin();
    let outcome = run_command(command, stdin.lock(), config, sdk::invoke).await;
    let mut stderr = io::stderr().lock();

    finish_command(outcome, &mut stderr)
}

fn finish_command<W: io::Write>(outcome: Result<(), CommandRunError>, stderr: &mut W) -> ExitCode {
    let (status, diagnostic) = match outcome {
        Ok(()) => (ExitCode::SUCCESS, None),
        Err(CommandRunError::Input(_)) => (
            ExitCode::from(2),
            Some("harness-events: invalid observation data"),
        ),
        Err(CommandRunError::Submit(SubmitError::Invoke(InvokeError::Connection))) => (
            ExitCode::from(3),
            Some("harness-events: engine connection failed"),
        ),
        Err(CommandRunError::Submit(SubmitError::Invoke(InvokeError::Invocation)))
        | Err(CommandRunError::Submit(SubmitError::UnexpectedResponse)) => (
            ExitCode::from(4),
            Some("harness-events: function invocation failed"),
        ),
    };

    if let Some(diagnostic) = diagnostic {
        let _ = writeln!(stderr, "{diagnostic}");
    }

    status
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Read, Write},
        process::ExitCode,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use harness_events_cli::{
        CommandRunError,
        app::{InvokeError, SubmitError},
        cli::{EventCommand, ObservationArgs},
        config::ClientConfig,
        contracts::InputError,
        run_command,
    };

    use super::finish_command;

    const OBSERVATION_SENTINEL: &str = "task-3-2-observation-sentinel";

    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("stderr is unavailable"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct FailingReader;

    impl Read for FailingReader {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other(OBSERVATION_SENTINEL))
        }
    }

    fn observation_command() -> EventCommand {
        EventCommand::Observation(ObservationArgs {
            hook_type: "PostToolUse".to_owned(),
            project_name: "example-project".to_owned(),
            current_working_directory: "/workspace/example".to_owned(),
            timestamp: "2026-09-19T20:14:33.123Z".to_owned(),
            session_id: "example-session".to_owned(),
        })
    }

    #[test]
    fn maps_success_to_a_silent_zero_exit() {
        let mut stderr = Vec::new();

        assert_eq!(finish_command(Ok(()), &mut stderr), ExitCode::SUCCESS);
        assert_eq!(stderr, b"".to_vec());
    }

    #[test]
    fn maps_local_observation_failures_to_status_two_with_a_fixed_diagnostic() {
        let mut stderr = Vec::new();

        assert_eq!(
            finish_command(
                Err(CommandRunError::Input(InputError::InvalidObservationJson)),
                &mut stderr,
            ),
            ExitCode::from(2)
        );
        assert_eq!(
            stderr,
            b"harness-events: invalid observation data\n".to_vec()
        );
    }

    #[test]
    fn maps_connection_failures_to_status_three_with_a_fixed_diagnostic() {
        let mut stderr = Vec::new();

        assert_eq!(
            finish_command(
                Err(CommandRunError::Submit(SubmitError::Invoke(
                    InvokeError::Connection,
                ))),
                &mut stderr,
            ),
            ExitCode::from(3)
        );
        assert_eq!(
            stderr,
            b"harness-events: engine connection failed\n".to_vec()
        );
    }

    #[test]
    fn maps_invocation_failures_to_status_four_with_a_fixed_diagnostic() {
        for outcome in [
            CommandRunError::Submit(SubmitError::Invoke(InvokeError::Invocation)),
            CommandRunError::Submit(SubmitError::UnexpectedResponse),
        ] {
            let mut stderr = Vec::new();

            assert_eq!(finish_command(Err(outcome), &mut stderr), ExitCode::from(4));
            assert_eq!(
                stderr,
                b"harness-events: function invocation failed\n".to_vec()
            );
        }
    }

    #[test]
    fn returns_the_determined_status_when_stderr_cannot_be_written() {
        assert_eq!(
            finish_command(
                Err(CommandRunError::Submit(SubmitError::Invoke(
                    InvokeError::Invocation,
                ))),
                &mut FailingWriter,
            ),
            ExitCode::from(4)
        );
    }

    #[tokio::test]
    async fn suppresses_observation_reader_details_from_the_diagnostic() {
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let outcome = run_command(
            observation_command(),
            FailingReader,
            ClientConfig {
                engine_url: "ws://must-not-connect.test".to_owned(),
                namespace: None,
            },
            {
                let invocation_count = Arc::clone(&invocation_count);
                move |_, _| {
                    invocation_count.fetch_add(1, Ordering::SeqCst);
                    async { Ok::<_, InvokeError>(serde_json::json!({"dispatched": true})) }
                }
            },
        )
        .await;
        let mut stderr = Vec::new();

        assert!(matches!(
            &outcome,
            Err(CommandRunError::Input(InputError::ObservationInputRead))
        ));
        assert_eq!(finish_command(outcome, &mut stderr), ExitCode::from(2));
        assert_eq!(
            stderr,
            b"harness-events: invalid observation data\n".to_vec()
        );
        assert!(
            !String::from_utf8(stderr)
                .expect("fixed diagnostics are UTF-8")
                .contains(OBSERVATION_SENTINEL)
        );
        assert_eq!(invocation_count.load(Ordering::SeqCst), 0);
    }
}
