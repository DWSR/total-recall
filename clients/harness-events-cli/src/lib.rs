//! Shared types for the harness-events client.

pub mod app;
pub mod cli;
pub mod config;
pub mod contracts;
pub mod sdk;

#[derive(Debug, thiserror::Error)]
pub enum CommandRunError {
    #[error(transparent)]
    Input(#[from] contracts::InputError),
    #[error(transparent)]
    Submit(#[from] app::SubmitError),
}

pub async fn run_command<F, Fut>(
    command: cli::EventCommand,
    stdin: impl std::io::Read,
    config: config::ClientConfig,
    invoke: F,
) -> Result<(), CommandRunError>
where
    F: FnOnce(config::ClientConfig, contracts::Submission) -> Fut,
    Fut: std::future::Future<Output = Result<serde_json::Value, app::InvokeError>>,
{
    let submission = contracts::prepare(command, stdin)?;
    app::submit_once(submission, move |submission| invoke(config, submission)).await?;

    Ok(())
}
