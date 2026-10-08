use std::{
    future::Future,
    io::{self, Read},
    pin::Pin,
    process::ExitCode,
    task::{Context, Poll},
};

use iii_sdk::{IIIClient, InitOptions, WorkerIdentityMode, runtime::WorkerMetadata};
use mcp_worker::{
    backend::{CanonicalStoreDelegate, ProductionMemoryOperations, ReadOnlyRetrievalAdapter},
    config::{Config, QueryEmbeddingConfig},
    embedding::RouterQueryEmbeddingGenerator,
    runtime::{RuntimeError, RuntimeInput, RuntimeShutdown, run_mcp_server},
    server::McpServer,
    service::{MemoryToolService, ProductionCreateContext},
};
use memory_store::{MemoryStore, database::IiiMemoryDatabase};
use tokio::{
    io::{AsyncRead, ReadBuf},
    sync::mpsc,
};

fn main() -> ExitCode {
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("worker startup configuration failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            eprintln!("worker runtime setup failed");
            return ExitCode::FAILURE;
        }
    };

    runtime.block_on(async move {
        let mut termination = match TerminationHandlers::install() {
            Ok(termination) => termination,
            Err(_) => {
                eprintln!("worker termination handler setup failed");
                return ExitCode::FAILURE;
            }
        };
        match run_production(config, &mut termination).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("worker runtime failed: {error}");
                ExitCode::FAILURE
            }
        }
    })
}

async fn run_production(
    config: Config,
    termination: &mut TerminationHandlers,
) -> Result<(), ProcessError> {
    let stdin = ProcessStdin::new().map_err(|_| ProcessError::Stdin)?;
    let client = production_client(&config);
    let database = config.database.clone();
    let store = MemoryStore::new(IiiMemoryDatabase::new(client.clone(), database.clone()));
    let canonical_store = CanonicalStoreDelegate::new(store);
    let retrieval = ReadOnlyRetrievalAdapter::new(client.clone(), database);
    let operations = ProductionMemoryOperations::new(canonical_store, retrieval);
    let generator = query_embedding_generator(&config.query_embedding, &client);
    let service = MemoryToolService::new(operations, ProductionCreateContext, generator);
    let server = McpServer::new(service);
    let (input, queue) = RuntimeInput::new(stdin, &config);
    let shutdown = RuntimeShutdown::new();
    let runtime = run_mcp_server(
        input,
        queue,
        config.max_in_flight,
        shutdown.clone(),
        server,
        tokio::io::stdout(),
    );

    let result =
        wait_for_runtime_or_termination(runtime, termination.wait(), move || shutdown.request())
            .await;
    client.shutdown_async().await;
    result
}

fn production_client(config: &Config) -> IIIClient {
    let mut metadata = WorkerMetadata::default();
    if let Some(worker_name) = &config.iii.worker_name {
        metadata.name.clone_from(worker_name);
    }
    metadata.namespace = config.iii.namespace.clone();

    iii_sdk::register_worker(
        &config.iii.engine_url,
        InitOptions {
            metadata: Some(metadata),
            headers: None,
            otel: None,
            namespace: config.iii.namespace.clone(),
            identity: WorkerIdentityMode::Managed,
        },
    )
}

fn query_embedding_generator(
    query_embedding: &QueryEmbeddingConfig,
    client: &IIIClient,
) -> Option<RouterQueryEmbeddingGenerator> {
    match query_embedding {
        QueryEmbeddingConfig::Disabled => None,
        QueryEmbeddingConfig::Enabled(settings) => Some(RouterQueryEmbeddingGenerator::new(
            client.clone(),
            settings.clone(),
        )),
    }
}

async fn wait_for_runtime_or_termination<Runtime, Termination, Shutdown>(
    runtime: Runtime,
    termination: Termination,
    shutdown: Shutdown,
) -> Result<(), ProcessError>
where
    Runtime: Future<Output = Result<(), RuntimeError>>,
    Termination: Future<Output = io::Result<()>>,
    Shutdown: FnOnce(),
{
    tokio::pin!(runtime);
    tokio::select! {
        result = &mut runtime => result.map_err(ProcessError::Runtime),
        termination_result = termination => {
            shutdown();
            let runtime_result = runtime.await;
            termination_result.map_err(|_| ProcessError::Termination)?;
            runtime_result.map_err(ProcessError::Runtime)
        }
    }
}

#[derive(Debug)]
enum ProcessError {
    Stdin,
    Runtime(RuntimeError),
    Termination,
}

impl std::fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stdin => formatter.write_str("worker_stdin_reader_setup_failed"),
            Self::Runtime(error) => error.fmt(formatter),
            Self::Termination => formatter.write_str("worker_termination_wait_failed"),
        }
    }
}

struct ProcessStdin {
    receiver: mpsc::Receiver<io::Result<Vec<u8>>>,
    bytes: Vec<u8>,
    position: usize,
}

impl ProcessStdin {
    fn new() -> io::Result<Self> {
        let (sender, receiver) = mpsc::channel(1);
        std::thread::Builder::new()
            .name("mcp-worker-stdin".to_owned())
            .spawn(move || {
                let stdin = io::stdin();
                let mut reader = stdin.lock();
                loop {
                    let mut bytes = [0; 8_192];
                    match reader.read(&mut bytes) {
                        Ok(0) => return,
                        Ok(count) => {
                            if sender.blocking_send(Ok(bytes[..count].to_vec())).is_err() {
                                return;
                            }
                        }
                        Err(error) => {
                            let _ = sender.blocking_send(Err(error));
                            return;
                        }
                    }
                }
            })?;

        Ok(Self {
            receiver,
            bytes: Vec::new(),
            position: 0,
        })
    }
}

impl AsyncRead for ProcessStdin {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let input = self.as_mut().get_mut();
        loop {
            if input.position < input.bytes.len() {
                let count = (input.bytes.len() - input.position).min(buffer.remaining());
                buffer.put_slice(&input.bytes[input.position..input.position + count]);
                input.position += count;
                if input.position == input.bytes.len() {
                    input.bytes.clear();
                    input.position = 0;
                }
                return Poll::Ready(Ok(()));
            }

            match Pin::new(&mut input.receiver).poll_recv(context) {
                Poll::Ready(Some(Ok(bytes))) => input.bytes = bytes,
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(error)),
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
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
    use iii_sdk::IIIClient;
    use mcp_worker::config::{
        Config, QueryEmbeddingConfig, TOTAL_RECALL_EMBEDDING_MODEL_ENV,
        TOTAL_RECALL_EMBEDDING_PROVIDER_ENV, TOTAL_RECALL_MEMORY_DATABASE_ENV,
    };

    use super::{query_embedding_generator, wait_for_runtime_or_termination};

    #[test]
    fn query_embedding_generator_is_composed_only_for_enabled_configuration() {
        let client = IIIClient::new("ws://127.0.0.1:9");
        let enabled = Config::from_values([
            (
                TOTAL_RECALL_MEMORY_DATABASE_ENV.to_owned(),
                "memory-database".to_owned(),
            ),
            (
                TOTAL_RECALL_EMBEDDING_PROVIDER_ENV.to_owned(),
                "provider".to_owned(),
            ),
            (
                TOTAL_RECALL_EMBEDDING_MODEL_ENV.to_owned(),
                "model".to_owned(),
            ),
        ])
        .expect("enabled embedding configuration should be valid")
        .query_embedding;
        assert!(matches!(enabled, QueryEmbeddingConfig::Enabled(_)));

        assert!(query_embedding_generator(&QueryEmbeddingConfig::Disabled, &client).is_none());
        assert!(query_embedding_generator(&enabled, &client).is_some());
    }

    #[tokio::test]
    async fn termination_requests_shutdown_and_waits_for_runtime_drain() {
        let (shutdown_requested, shutdown_observed) = tokio::sync::oneshot::channel();
        let (release_runtime, runtime_released) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(wait_for_runtime_or_termination(
            async move {
                runtime_released
                    .await
                    .expect("test runtime release should be sent");
                Ok(())
            },
            async { Ok(()) },
            move || {
                shutdown_requested
                    .send(())
                    .expect("shutdown should be requested once");
            },
        ));

        shutdown_observed
            .await
            .expect("termination should request runtime shutdown");
        assert!(!task.is_finished());
        release_runtime
            .send(())
            .expect("test runtime should still be waiting to drain");
        task.await
            .expect("lifecycle task should not panic")
            .expect("clean termination should complete after runtime drain");
    }
}
