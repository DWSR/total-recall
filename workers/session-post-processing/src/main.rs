use std::{fmt, future::Future, io, process::ExitCode, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use session_post_processing::{
    IiiCanonicalMemoryPublisher, IiiSessionRepository,
    config::{Config, ProviderConfig},
    contracts::{StructuredGenerationRequest, StructuredGenerationResponse},
    coordinator::{SweepCoordinator, SweepCoordinatorLimits},
    ports::{Clock, ModelProvider, ProviderError, SessionProcessingService},
    provider::{anthropic::AnthropicProvider, openai_compatible::OpenAiCompatibleProvider},
    runtime::{WorkerRuntime, WorkerStartupError},
};

enum SelectedProvider {
    OpenAi(OpenAiCompatibleProvider),
    Anthropic(AnthropicProvider),
}

#[derive(Debug)]
enum WorkerCompositionError {
    Provider,
    Runtime(WorkerStartupError),
}

impl fmt::Display for WorkerCompositionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Provider => "worker provider setup failed",
            Self::Runtime(error) => return error.fmt(formatter),
        })
    }
}

impl std::error::Error for WorkerCompositionError {}

fn selected_provider(config: &Config) -> Result<SelectedProvider, WorkerCompositionError> {
    let provider = match &config.provider {
        ProviderConfig::OpenAi(provider) => OpenAiCompatibleProvider::new(
            provider.clone(),
            config.model_timeout,
            config.model_attempts,
            config.model_output_tokens,
        )
        .map(SelectedProvider::OpenAi),
        ProviderConfig::Anthropic(provider) => AnthropicProvider::new(
            provider.clone(),
            config.model_timeout,
            config.model_attempts,
            config.model_output_tokens,
        )
        .map(SelectedProvider::Anthropic),
    };

    provider.map_err(|_| WorkerCompositionError::Provider)
}

#[async_trait]
impl ModelProvider for SelectedProvider {
    async fn generate(
        &self,
        request: StructuredGenerationRequest,
    ) -> Result<StructuredGenerationResponse, ProviderError> {
        match self {
            Self::OpenAi(provider) => provider.generate(request).await,
            Self::Anthropic(provider) => provider.generate(request).await,
        }
    }
}

fn compose_runtime(config: &Config) -> Result<WorkerRuntime, WorkerCompositionError> {
    let provider: Arc<dyn ModelProvider> = Arc::new(selected_provider(config)?);
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let limits = SweepCoordinatorLimits {
        batch_limit: config.batch_limit,
        concurrency: config.concurrency,
        lease_duration: config.lease_duration,
        model_timeout: config.model_timeout,
        model_chunk_bytes: config.model_chunk_bytes,
    };
    let database = config.database.clone();
    let memory_database = config.memory_database.clone();

    WorkerRuntime::start_with_service_factory(config, Arc::clone(&clock), move |client| {
        let repository = Arc::new(IiiSessionRepository::new(client.clone(), database));
        let memory_sink = Arc::new(IiiCanonicalMemoryPublisher::new(client, memory_database));
        let service: Arc<dyn SessionProcessingService> = Arc::new(SweepCoordinator::new(
            clock,
            repository,
            provider,
            memory_sink,
            limits,
        ));
        service
    })
    .map_err(WorkerCompositionError::Runtime)
}

struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("worker startup configuration failed: {error}");
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
    let runtime = match compose_runtime(&config) {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("worker startup composition failed: {error}");
            return ExitCode::FAILURE;
        }
    };

    match wait_for_startup(runtime.wait_until_ready(), termination.wait()).await {
        StartupOutcome::Ready(Ok(())) => {}
        StartupOutcome::Ready(Err(error)) => {
            runtime.shutdown().await;
            eprintln!("worker startup readiness failed: {error}");
            return ExitCode::FAILURE;
        }
        StartupOutcome::Terminated(Ok(())) => {
            runtime.shutdown().await;
            eprintln!("worker termination signal received during startup readiness");
            return ExitCode::FAILURE;
        }
        StartupOutcome::Terminated(Err(_)) => {
            runtime.shutdown().await;
            eprintln!("worker termination signal failed");
            return ExitCode::FAILURE;
        }
    }

    eprintln!("worker ready");
    match termination.wait().await {
        Ok(()) => {
            runtime.shutdown().await;
            ExitCode::SUCCESS
        }
        Err(_) => {
            runtime.shutdown().await;
            eprintln!("worker termination signal failed");
            ExitCode::FAILURE
        }
    }
}

#[derive(Debug)]
enum StartupOutcome {
    Ready(Result<(), WorkerStartupError>),
    Terminated(io::Result<()>),
}

async fn wait_for_startup<Readiness, Termination>(
    readiness: Readiness,
    termination: Termination,
) -> StartupOutcome
where
    Readiness: Future<Output = Result<(), WorkerStartupError>>,
    Termination: Future<Output = io::Result<()>>,
{
    tokio::select! {
        result = readiness => StartupOutcome::Ready(result),
        result = termination => StartupOutcome::Terminated(result),
    }
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
    use super::{SelectedProvider, StartupOutcome, selected_provider, wait_for_startup};
    use session_post_processing::{
        config::{
            Config, TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_API_TOKEN_ENV,
            TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_BASE_URL_ENV,
            TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_MODEL_ENV,
            TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV,
            TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV,
            TOTAL_RECALL_POST_PROCESSING_OPENAI_API_TOKEN_ENV,
            TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL_ENV,
            TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV,
        },
        runtime::WorkerStartupError,
    };

    #[test]
    fn composition_constructs_only_the_selected_provider() {
        let openai = Config::from_values([
            (
                TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV.to_owned(),
                "source-session".to_owned(),
            ),
            (
                TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV.to_owned(),
                "memory".to_owned(),
            ),
            (
                TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV.to_owned(),
                "openai".to_owned(),
            ),
            (
                TOTAL_RECALL_POST_PROCESSING_OPENAI_API_TOKEN_ENV.to_owned(),
                "openai-token-secret-sentinel".to_owned(),
            ),
            (
                TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL_ENV.to_owned(),
                "openai-model".to_owned(),
            ),
            (
                TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_BASE_URL_ENV.to_owned(),
                "http://example.invalid/v1/".to_owned(),
            ),
            (
                TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_API_TOKEN_ENV.to_owned(),
                " ".to_owned(),
            ),
            (
                TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_MODEL_ENV.to_owned(),
                " ".to_owned(),
            ),
        ])
        .expect("inactive Anthropic settings should not invalidate OpenAI composition");
        let anthropic = Config::from_values([
            (
                TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV.to_owned(),
                "source-session".to_owned(),
            ),
            (
                TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV.to_owned(),
                "memory".to_owned(),
            ),
            (
                TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV.to_owned(),
                "anthropic".to_owned(),
            ),
            (
                TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_API_TOKEN_ENV.to_owned(),
                "anthropic-token-secret-sentinel".to_owned(),
            ),
            (
                TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_MODEL_ENV.to_owned(),
                "anthropic-model".to_owned(),
            ),
        ])
        .expect("Anthropic composition should be valid");

        assert!(matches!(
            selected_provider(&openai),
            Ok(SelectedProvider::OpenAi(_))
        ));
        assert!(matches!(
            selected_provider(&anthropic),
            Ok(SelectedProvider::Anthropic(_))
        ));
    }

    #[tokio::test]
    async fn readiness_failure_never_reports_ready() {
        let outcome = wait_for_startup(
            async { Err(WorkerStartupError::CatalogMismatch) },
            std::future::pending(),
        )
        .await;

        assert!(matches!(
            outcome,
            StartupOutcome::Ready(Err(WorkerStartupError::CatalogMismatch))
        ));
    }
}
