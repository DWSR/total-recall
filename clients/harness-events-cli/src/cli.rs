use clap::{Args, Parser, Subcommand};

#[derive(Clone, Debug, Eq, Parser, PartialEq)]
#[command(name = "harness-events")]
pub struct Cli {
    #[command(subcommand)]
    pub command: EventCommand,
}

#[derive(Clone, Debug, Eq, PartialEq, Subcommand)]
pub enum EventCommand {
    #[command(name = "session-start")]
    SessionStart(LifecycleArgs),
    #[command(name = "observation")]
    Observation(ObservationArgs),
    #[command(name = "session-end")]
    SessionEnd(LifecycleArgs),
}

#[derive(Args, Clone, Debug, Eq, PartialEq)]
pub struct LifecycleArgs {
    #[arg(long = "session-id")]
    pub session_id: String,
    #[arg(long = "project-name")]
    pub project_name: String,
    #[arg(long = "current-working-directory")]
    pub current_working_directory: String,
    #[arg(long = "timestamp")]
    pub timestamp: String,
}

#[derive(Args, Clone, Debug, Eq, PartialEq)]
pub struct ObservationArgs {
    #[arg(long = "hook-type")]
    pub hook_type: String,
    #[arg(long = "project-name")]
    pub project_name: String,
    #[arg(long = "current-working-directory")]
    pub current_working_directory: String,
    #[arg(long = "timestamp")]
    pub timestamp: String,
    #[arg(long = "session-id")]
    pub session_id: String,
}
