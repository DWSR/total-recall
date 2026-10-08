//! Shared embedding coordination.

use std::sync::Arc;

use futures_util::{StreamExt, stream::FuturesUnordered};
use iii_sdk::builtin_triggers::CronCallRequest;
use serde_json::Value;
use tokio::sync::Semaphore;

use crate::EventAdapter;
use crate::contracts::{
    Deadline, EmbeddingError, EmbeddingOutcome, EmbeddingRouter, EmbeddingWorkItem,
    EmbeddingWorkRepository, EmbeddingWriter, EventFailure, LoadedEmbeddingWork,
    ReconciliationOutcome, RepositoryFailure, RouterFailure, WriteOutcome, WriterFailure,
};

pub struct QueueEventProcessor<Repository, Router, Writer> {
    event_adapter: EventAdapter,
    repository: Arc<Repository>,
    coordinator: Arc<EmbeddingCoordinator<Router, Writer>>,
}

impl<Repository, Router, Writer> QueueEventProcessor<Repository, Router, Writer> {
    pub fn new(
        event_adapter: EventAdapter,
        repository: Repository,
        coordinator: EmbeddingCoordinator<Router, Writer>,
    ) -> Self {
        Self::with_shared(event_adapter, Arc::new(repository), Arc::new(coordinator))
    }

    pub fn with_shared(
        event_adapter: EventAdapter,
        repository: Arc<Repository>,
        coordinator: Arc<EmbeddingCoordinator<Router, Writer>>,
    ) -> Self {
        Self {
            event_adapter,
            repository,
            coordinator,
        }
    }
}

impl<Repository, Router, Writer> QueueEventProcessor<Repository, Router, Writer>
where
    Repository: EmbeddingWorkRepository,
    Router: EmbeddingRouter,
    Writer: EmbeddingWriter,
{
    pub async fn process_event(
        &self,
        payload: Value,
        deadline: Deadline,
    ) -> Result<EmbeddingOutcome, EmbeddingError> {
        let keys = self.event_adapter.adapt(payload)?;
        let loaded = self.repository.load_keys(&keys, deadline).await?;
        let mut pending = Vec::new();
        let mut already_present = 0_u32;
        let mut missing = false;

        for work in loaded {
            match work {
                LoadedEmbeddingWork::Pending(item) => pending.push(item),
                LoadedEmbeddingWork::AlreadyPresent(_) => {
                    already_present = already_present.checked_add(1).ok_or_else(|| {
                        EmbeddingError::repository(RepositoryFailure::MalformedResponse)
                    })?;
                }
                LoadedEmbeddingWork::Missing(_) => missing = true,
            }
        }

        match self
            .coordinator
            .process(&pending, already_present, deadline)
            .await
        {
            Ok(outcome) if !missing => Ok(outcome),
            Ok(_) => Err(EmbeddingError::repository(RepositoryFailure::Missing)),
            Err(error) => Err(error),
        }
    }
}

pub struct ReconciliationProcessor<Repository, Router, Writer> {
    repository: Arc<Repository>,
    coordinator: Arc<EmbeddingCoordinator<Router, Writer>>,
    reconciliation_limit: u32,
}

impl<Repository, Router, Writer> ReconciliationProcessor<Repository, Router, Writer> {
    pub fn new(
        repository: Repository,
        coordinator: EmbeddingCoordinator<Router, Writer>,
        reconciliation_limit: u32,
    ) -> Self {
        Self::with_shared(
            Arc::new(repository),
            Arc::new(coordinator),
            reconciliation_limit,
        )
    }

    pub fn with_shared(
        repository: Arc<Repository>,
        coordinator: Arc<EmbeddingCoordinator<Router, Writer>>,
        reconciliation_limit: u32,
    ) -> Self {
        Self {
            repository,
            coordinator,
            reconciliation_limit,
        }
    }
}

impl<Repository, Router, Writer> ReconciliationProcessor<Repository, Router, Writer>
where
    Repository: EmbeddingWorkRepository,
    Router: EmbeddingRouter,
    Writer: EmbeddingWriter,
{
    pub async fn reconcile(
        &self,
        call: CronCallRequest,
        deadline: Deadline,
    ) -> Result<ReconciliationOutcome, EmbeddingError> {
        validate_cron_call(&call)?;
        let pending = self
            .repository
            .list_missing(self.reconciliation_limit, deadline)
            .await?;
        self.coordinator.process(&pending, 0, deadline).await
    }
}

fn validate_cron_call(call: &CronCallRequest) -> Result<(), EmbeddingError> {
    if call.trigger != "cron"
        || chrono::DateTime::parse_from_rfc3339(&call.scheduled_time).is_err()
        || chrono::DateTime::parse_from_rfc3339(&call.actual_time).is_err()
    {
        return Err(EmbeddingError::event(EventFailure::Malformed));
    }

    Ok(())
}

pub struct EmbeddingCoordinator<R, W> {
    router: R,
    writer: W,
    batch_limit: u32,
    max_input_bytes: usize,
    router_permits: Arc<Semaphore>,
}

impl<R, W> EmbeddingCoordinator<R, W> {
    pub fn new(
        router: R,
        writer: W,
        batch_limit: u32,
        max_input_bytes: usize,
        router_permits: Arc<Semaphore>,
    ) -> Self {
        Self {
            router,
            writer,
            batch_limit,
            max_input_bytes,
            router_permits,
        }
    }
}

impl<R, W> EmbeddingCoordinator<R, W>
where
    R: EmbeddingRouter,
    W: EmbeddingWriter,
{
    pub async fn process(
        &self,
        pending: &[EmbeddingWorkItem],
        already_present: u32,
        deadline: Deadline,
    ) -> Result<EmbeddingOutcome, EmbeddingError> {
        let pending_count = u32::try_from(pending.len())
            .map_err(|_| EmbeddingError::router(RouterFailure::CountMismatch))?;
        if pending_count > self.batch_limit {
            return Err(EmbeddingError::router(RouterFailure::CountMismatch));
        }
        let selected = pending_count
            .checked_add(already_present)
            .ok_or(EmbeddingError::router(RouterFailure::CountMismatch))?;
        let mut rendered = Vec::with_capacity(pending.len());
        let mut first_failure = None;

        for (index, item) in pending.iter().enumerate() {
            match crate::renderer::render(item, self.max_input_bytes) {
                Ok(input) => rendered.push((index, input)),
                Err(error) => record_first_failure(&mut first_failure, index, error),
            }
        }

        if rendered.is_empty() {
            return match first_failure {
                Some((_, error)) => Err(error),
                None => Ok(EmbeddingOutcome::new(selected, 0, already_present, 0)),
            };
        }

        let inputs = rendered
            .iter()
            .map(|(_, input)| input.clone())
            .collect::<Vec<_>>();
        let generated = {
            let _permit = tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline.instant()),
                Arc::clone(&self.router_permits).acquire_owned(),
            )
            .await
            .map_err(|_| EmbeddingError::router(RouterFailure::Timeout))?
            .map_err(|_| EmbeddingError::router(RouterFailure::Timeout))?;

            tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline.instant()),
                self.router.embed(&inputs, deadline),
            )
            .await
            .map_err(|_| EmbeddingError::router(RouterFailure::Timeout))??
        };

        if generated.len() != inputs.len() {
            return Err(EmbeddingError::router(RouterFailure::CountMismatch));
        }

        let mut writes = FuturesUnordered::new();
        for ((index, input), embedding) in rendered.into_iter().zip(generated) {
            let key = input.key().clone();
            let vector = embedding.into_vector().into_iter().map(f64::from).collect();
            let writer = &self.writer;
            writes.push(async move {
                let result = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline.instant()),
                    writer.insert(key, vector, deadline),
                )
                .await
                .map_err(|_| EmbeddingError::writer(WriterFailure::Timeout))
                .and_then(|result| result);

                (index, result)
            });
        }

        let mut stored = 0;
        let mut written_already_present = 0;
        while let Some((index, result)) = writes.next().await {
            match result {
                Ok(WriteOutcome::Stored) => stored += 1,
                Ok(WriteOutcome::AlreadyPresent) => written_already_present += 1,
                Err(error) => record_first_failure(&mut first_failure, index, error),
            }
        }

        if let Some((_, error)) = first_failure {
            return Err(error);
        }

        Ok(EmbeddingOutcome::new(
            selected,
            pending_count,
            already_present + written_already_present,
            stored,
        ))
    }
}

fn record_first_failure(
    first_failure: &mut Option<(usize, EmbeddingError)>,
    index: usize,
    error: EmbeddingError,
) {
    if first_failure
        .as_ref()
        .is_none_or(|(first_index, _)| index < *first_index)
    {
        *first_failure = Some((index, error));
    }
}
