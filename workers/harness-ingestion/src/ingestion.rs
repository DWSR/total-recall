use schemars::JsonSchema;
use serde::Serialize;
use thiserror::Error;

use crate::{
    contracts::{
        ObservationEvent, ObservationInput, SessionEndEvent, SessionEndInput, SessionEndRequest,
        SessionStartEvent, SessionStartInput, SessionStartRequest,
    },
    publisher::{PublishError, QueuePublisher},
};

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize)]
pub struct DispatchResponse {
    pub dispatched: bool,
}

#[derive(Debug, Error)]
pub enum IngestionError {
    #[error("contract validation failed")]
    Contract(#[from] crate::contracts::ContractError),
    #[error("event serialization failed")]
    Serialization(#[source] serde_json::Error),
    #[error("event publication failed")]
    Publication(#[source] PublishError),
}

pub struct IngestionService<P> {
    publisher: P,
}

impl<P> IngestionService<P> {
    pub fn new(publisher: P) -> Self {
        Self { publisher }
    }
}

impl<P> IngestionService<P>
where
    P: QueuePublisher,
{
    pub async fn session_start(
        &self,
        input: SessionStartInput,
    ) -> Result<DispatchResponse, IngestionError> {
        let request: SessionStartRequest = input.try_into()?;
        self.publish_event(SessionStartEvent {
            event_type: "session_start".to_owned(),
            session_id: request.session_id,
            project_name: request.project_name,
            timestamp: request.timestamp,
            current_working_directory: request.current_working_directory,
        })
        .await
    }

    pub async fn observation(
        &self,
        input: ObservationInput,
    ) -> Result<DispatchResponse, IngestionError> {
        let request = input.into_request()?;
        self.publish_event(ObservationEvent {
            event_type: "observation".to_owned(),
            hook_type: request.hook_type,
            project_name: request.project_name,
            current_working_directory: request.current_working_directory,
            timestamp: request.timestamp,
            session_id: request.session_id,
            data: request.data,
        })
        .await
    }

    pub async fn session_end(
        &self,
        input: SessionEndInput,
    ) -> Result<DispatchResponse, IngestionError> {
        let request: SessionEndRequest = input.try_into()?;
        self.publish_event(SessionEndEvent {
            event_type: "session_end".to_owned(),
            session_id: request.session_id,
            project_name: request.project_name,
            timestamp: request.timestamp,
            current_working_directory: request.current_working_directory,
        })
        .await
    }

    async fn publish_event<E>(&self, event: E) -> Result<DispatchResponse, IngestionError>
    where
        E: Serialize,
    {
        let event = serde_json::to_value(event).map_err(IngestionError::Serialization)?;
        self.publisher
            .publish(event)
            .await
            .map_err(IngestionError::Publication)?;

        Ok(DispatchResponse { dispatched: true })
    }
}
