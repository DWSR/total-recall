//! Canonical embedding input rendering.

use crate::contracts::{CanonicalEmbeddingInput, EmbeddingError, EmbeddingWorkItem, RenderFailure};
use serde::Serialize;

const FORMAT: &str = "total-recall.memory-embedding.v1";

#[derive(Serialize)]
struct RenderedInput<'a> {
    format: &'static str,
    title: &'a str,
    content: &'a str,
    concepts: &'a [String],
}

pub fn render(
    item: &EmbeddingWorkItem,
    max_input_bytes: usize,
) -> Result<CanonicalEmbeddingInput, EmbeddingError> {
    let text = serde_json::to_string(&RenderedInput {
        format: FORMAT,
        title: item.title(),
        content: item.content(),
        concepts: item.concepts(),
    })
    .map_err(|_| EmbeddingError::render(RenderFailure::Serialization))?;

    if text.len() > max_input_bytes {
        return Err(EmbeddingError::render(RenderFailure::InputTooLarge));
    }

    Ok(CanonicalEmbeddingInput::new(item.key().clone(), text))
}
