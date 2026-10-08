mod memory_history;

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    match memory_history::run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("memory PostgreSQL history verification failed: {error}");
            ExitCode::FAILURE
        }
    }
}
