//! Routed embedding generation.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use iii_sdk::{IIIClient, protocol::TriggerRequest};
use serde_json::{Map, Value, json};

use crate::contracts::{
    CanonicalEmbeddingInput, Deadline, EmbeddingRouter, GeneratedEmbedding, RouterError,
    RouterFailure,
};

const ROUTER_EMBED_FUNCTION_ID: &str = "router::embed";

#[async_trait]
#[doc(hidden)]
pub trait RouterExecutor: Send + Sync {
    async fn execute(&self, request: TriggerRequest) -> Result<Value, iii_sdk::Error>;
}

#[async_trait]
impl RouterExecutor for IIIClient {
    async fn execute(&self, request: TriggerRequest) -> Result<Value, iii_sdk::Error> {
        self.trigger(request).await
    }
}

pub struct IiiEmbeddingRouter<E = IIIClient> {
    executor: E,
    provider: String,
    model: String,
    timeout: Duration,
}

impl IiiEmbeddingRouter<IIIClient> {
    pub fn new(
        client: IIIClient,
        provider: impl Into<String>,
        model: impl Into<String>,
        timeout: Duration,
    ) -> Self {
        Self::with_executor(client, provider, model, timeout)
    }
}

impl<E> IiiEmbeddingRouter<E>
where
    E: RouterExecutor,
{
    #[doc(hidden)]
    pub fn with_executor(
        executor: E,
        provider: impl Into<String>,
        model: impl Into<String>,
        timeout: Duration,
    ) -> Self {
        Self {
            executor,
            provider: provider.into(),
            model: model.into(),
            timeout,
        }
    }

    pub async fn embed(
        &self,
        inputs: &[CanonicalEmbeddingInput],
        deadline: Deadline,
    ) -> Result<Vec<GeneratedEmbedding>, RouterError> {
        if inputs.is_empty() {
            return Err(router_error(RouterFailure::CountMismatch));
        }

        let request_deadline = request_deadline(self.timeout, deadline);
        if request_deadline
            .saturating_duration_since(Instant::now())
            .is_zero()
        {
            return Err(router_error(RouterFailure::Timeout));
        }

        let response = tokio::time::timeout_at(
            tokio::time::Instant::from_std(request_deadline),
            self.executor
                .execute(build_embed_request(inputs, &self.provider, &self.model)),
        )
        .await
        .map_err(|_| router_error(RouterFailure::Timeout))?
        .map_err(|_| router_error(RouterFailure::Remote))?;

        decode_embed_response(inputs, &self.provider, &self.model, response)
    }
}

#[async_trait]
impl<E> EmbeddingRouter for IiiEmbeddingRouter<E>
where
    E: RouterExecutor,
{
    async fn embed(
        &self,
        inputs: &[CanonicalEmbeddingInput],
        deadline: Deadline,
    ) -> Result<Vec<GeneratedEmbedding>, RouterError> {
        IiiEmbeddingRouter::embed(self, inputs, deadline).await
    }
}

const fn router_error(reason: RouterFailure) -> RouterError {
    crate::contracts::EmbeddingError::router(reason)
}

fn request_deadline(timeout: Duration, deadline: Deadline) -> Instant {
    let local_deadline = Instant::now()
        .checked_add(timeout)
        .unwrap_or_else(|| deadline.instant());

    local_deadline.min(deadline.instant())
}

fn build_embed_request(
    inputs: &[CanonicalEmbeddingInput],
    provider: &str,
    model: &str,
) -> TriggerRequest {
    TriggerRequest {
        function_id: ROUTER_EMBED_FUNCTION_ID.to_owned(),
        payload: json!({
            "input": inputs.iter().map(CanonicalEmbeddingInput::text).collect::<Vec<_>>(),
            "provider": provider,
            "model": model,
        }),
        action: None,
        timeout_ms: None,
    }
}

fn decode_embed_response(
    inputs: &[CanonicalEmbeddingInput],
    provider: &str,
    model: &str,
    response: Value,
) -> Result<Vec<GeneratedEmbedding>, RouterError> {
    let response = exact_object(&response, &["provider", "model", "embeddings"])?;
    let response_provider = string_value(response, "provider")?;
    if response_provider != provider {
        return Err(router_error(RouterFailure::ProviderMismatch));
    }

    let response_model = string_value(response, "model")?;
    if response_model != model {
        return Err(router_error(RouterFailure::ModelMismatch));
    }

    let vectors = value(response, "embeddings")?
        .as_array()
        .ok_or_else(remote_error)?;
    if vectors.len() != inputs.len() {
        return Err(router_error(RouterFailure::CountMismatch));
    }

    inputs
        .iter()
        .zip(vectors)
        .map(|(input, vector)| {
            let vector = decode_vector(vector)?;
            GeneratedEmbedding::try_new(input.key().clone(), vector)
                .map_err(|_| router_error(RouterFailure::InvalidVector))
        })
        .collect()
}

fn exact_object<'a>(
    value: &'a Value,
    expected_fields: &[&str],
) -> Result<&'a Map<String, Value>, RouterError> {
    let object = value.as_object().ok_or_else(remote_error)?;
    if object.len() != expected_fields.len()
        || expected_fields
            .iter()
            .any(|field| !object.contains_key(*field))
    {
        return Err(remote_error());
    }

    Ok(object)
}

fn value<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a Value, RouterError> {
    object.get(field).ok_or_else(remote_error)
}

fn string_value<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a str, RouterError> {
    value(object, field)?.as_str().ok_or_else(remote_error)
}

fn decode_vector(vector: &Value) -> Result<Vec<f32>, RouterError> {
    let vector = vector
        .as_array()
        .ok_or_else(|| router_error(RouterFailure::InvalidVector))?;
    let vector = vector
        .iter()
        .map(|component| {
            let component = component
                .as_f64()
                .ok_or_else(|| router_error(RouterFailure::InvalidVector))?
                as f32;
            if !component.is_finite() {
                return Err(router_error(RouterFailure::InvalidVector));
            }

            Ok(component)
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(vector)
}

const fn remote_error() -> RouterError {
    router_error(RouterFailure::Remote)
}
