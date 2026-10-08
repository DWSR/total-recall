use async_trait::async_trait;
use schemars::JsonSchema;
use serde::Serialize;
use thiserror::Error;

use crate::contracts::{PersistableEvent, QueuedHarnessEventInput};

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize)]
pub struct PersistenceResponse {
    pub persisted: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendReceipt {
    pub affected_rows: u64,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("store append failed")]
pub struct StoreError;

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PersistenceError {
    #[error("{event_type} event validation failed")]
    Validation { event_type: &'static str },
    #[error("{event_type} event persistence failed")]
    Store { event_type: &'static str },
    #[error("{event_type} event persistence was not confirmed")]
    UnconfirmedWrite { event_type: &'static str },
}

#[async_trait]
pub trait EventStore: Send + Sync {
    async fn append(&self, event: &PersistableEvent) -> Result<AppendReceipt, StoreError>;
}

pub async fn persist_event(
    input: QueuedHarnessEventInput,
    store: &dyn EventStore,
) -> Result<PersistenceResponse, PersistenceError> {
    let event_type = event_type(&input);
    let event = input
        .into_persistable_event()
        .map_err(|_| PersistenceError::Validation { event_type })?;
    let receipt = store
        .append(&event)
        .await
        .map_err(|_| PersistenceError::Store { event_type })?;

    if receipt.affected_rows != 1 {
        return Err(PersistenceError::UnconfirmedWrite { event_type });
    }

    Ok(PersistenceResponse { persisted: true })
}

fn event_type(input: &QueuedHarnessEventInput) -> &'static str {
    match input {
        QueuedHarnessEventInput::SessionStart { .. } => "session_start",
        QueuedHarnessEventInput::Observation { .. } => "observation",
        QueuedHarnessEventInput::SessionEnd { .. } => "session_end",
    }
}
