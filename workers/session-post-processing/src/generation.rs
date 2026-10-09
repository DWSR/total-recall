use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    time::Duration,
};

use chrono::{DateTime, Timelike, Utc};
use memory_store::contracts::{MemoryVersionInput, ValidatedMemoryVersion};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::contracts::{
    ContractError, FailureCategory, GenerationInput, MemoryCandidate, MemoryScope,
    ProviderOperation, ReceiptId, Retryability, StructuredGenerationRequest,
    StructuredGenerationRequestInput, StructuredGenerationResponse,
    StructuredGenerationResponseInput,
};
use crate::ports::{
    Clock, ExternalCallLease, LeasePermit, ModelProvider, ProviderError, StagedMemoryCandidate,
};

const CANONICAL_MEMORY_TYPE: &str = "session-derived";
const CANONICAL_MEMORY_VERSION: i64 = 1;
const CANONICAL_MEMORY_VERSION_TEXT: &str = "1";
const MEMORY_ID_NAMESPACE: &[u8] = b"total-recall/session-post-processing/memory-id/v1";

pub fn normalize_memory_candidates(
    input: &GenerationInput,
    generated_at: DateTime<Utc>,
    candidates: &[MemoryCandidate],
) -> Result<Vec<StagedMemoryCandidate>, GenerationError> {
    let generated_at = generated_at
        .with_nanosecond(0)
        .ok_or(GenerationError::invalid("generated_at", "invalid"))?;
    let session_id = input
        .transcript()
        .entries()
        .first()
        .expect("GenerationInput always contains a non-empty transcript")
        .session_id()
        .as_str()
        .to_owned();
    let mut normalized = BTreeMap::<String, NormalizedMemoryCandidate>::new();

    for candidate in candidates {
        let supporting_receipt_ids = candidate
            .supporting_receipt_ids()
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if supporting_receipt_ids.is_empty() {
            return Err(GenerationError::invalid("supporting_receipt_ids", "empty"));
        }
        if supporting_receipt_ids
            .iter()
            .any(|receipt_id| !input.memory_scope().contains(receipt_id))
        {
            return Err(GenerationError::invalid(
                "supporting_receipt_ids",
                "outside_memory_scope",
            ));
        }

        let title = candidate.title().trim().to_owned();
        let content = candidate.content().trim().to_owned();
        let mut concepts = candidate
            .concepts()
            .iter()
            .map(|concept| concept.trim().to_lowercase())
            .collect::<Vec<_>>();
        if concepts.iter().any(|concept| concept.is_empty()) {
            return Err(GenerationError::invalid("concepts", "empty"));
        }
        concepts.sort_unstable();
        let fingerprint = content_fingerprint(&title, &content, &concepts)?;
        let normalized_candidate = NormalizedMemoryCandidate {
            digest: fingerprint.digest,
            title,
            content,
            concepts,
            supporting_receipt_ids,
        };

        if let Some(existing) = normalized.get_mut(&fingerprint.hexadecimal) {
            if existing.title != normalized_candidate.title
                || existing.content != normalized_candidate.content
                || existing.concepts != normalized_candidate.concepts
            {
                return Err(GenerationError::invalid(
                    "content_fingerprint",
                    "conflicting_payload",
                ));
            }
            existing
                .supporting_receipt_ids
                .extend(normalized_candidate.supporting_receipt_ids);
        } else {
            normalized.insert(fingerprint.hexadecimal, normalized_candidate);
        }
    }

    normalized
        .into_iter()
        .map(|(content_fingerprint, candidate)| {
            let supporting_receipt_ids = candidate
                .supporting_receipt_ids
                .into_iter()
                .map(|receipt_id| receipt_id.as_str().to_owned())
                .collect::<Vec<_>>();
            let canonical_payload = MemoryVersionInput {
                id: memory_id(&session_id, &candidate.digest)?,
                version: CANONICAL_MEMORY_VERSION,
                memory_type: CANONICAL_MEMORY_TYPE.to_owned(),
                title: candidate.title,
                content: candidate.content,
                created_at: generated_at,
                updated_at: generated_at,
                concepts: candidate.concepts,
                files: Vec::new(),
                session_ids: vec![session_id.clone()],
                source_observation_ids: supporting_receipt_ids.clone(),
            };
            ValidatedMemoryVersion::try_from(canonical_payload.clone())
                .map_err(|_| GenerationError::invalid("canonical_payload", "invalid"))?;

            StagedMemoryCandidate::try_new(
                content_fingerprint,
                canonical_payload,
                supporting_receipt_ids,
            )
            .map_err(GenerationError::from_contract)
        })
        .collect()
}

struct NormalizedMemoryCandidate {
    digest: [u8; 32],
    title: String,
    content: String,
    concepts: Vec<String>,
    supporting_receipt_ids: BTreeSet<ReceiptId>,
}

struct ContentFingerprint {
    hexadecimal: String,
    digest: [u8; 32],
}

fn content_fingerprint(
    title: &str,
    content: &str,
    concepts: &[String],
) -> Result<ContentFingerprint, GenerationError> {
    let mut bytes = Vec::new();
    for field in [
        CANONICAL_MEMORY_VERSION_TEXT.as_bytes(),
        CANONICAL_MEMORY_TYPE.as_bytes(),
        title.as_bytes(),
        content.as_bytes(),
    ] {
        append_length_framed(&mut bytes, field)?;
    }
    for concept in concepts {
        append_length_framed(&mut bytes, concept.as_bytes())?;
    }

    let digest = Sha256::digest(bytes);
    let mut raw_digest = [0; 32];
    raw_digest.copy_from_slice(&digest);
    Ok(ContentFingerprint {
        hexadecimal: raw_digest.iter().map(|b| format!("{b:02x}")).collect(),
        digest: raw_digest,
    })
}

fn memory_id(session_id: &str, digest: &[u8; 32]) -> Result<String, GenerationError> {
    let mut name = Vec::new();
    append_length_framed(&mut name, session_id.as_bytes())?;
    append_length_framed(&mut name, digest)?;

    let namespace = Uuid::new_v5(&Uuid::NAMESPACE_URL, MEMORY_ID_NAMESPACE);
    Ok(Uuid::new_v5(&namespace, &name).to_string())
}

fn append_length_framed(bytes: &mut Vec<u8>, field: &[u8]) -> Result<(), GenerationError> {
    let length = u64::try_from(field.len())
        .map_err(|_| GenerationError::invalid("canonical_payload", "field_too_long"))?;
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(field);
    Ok(())
}

pub struct GenerationPipeline<'a> {
    clock: &'a dyn Clock,
    lease: &'a dyn ExternalCallLease,
    provider: &'a dyn ModelProvider,
    model_timeout: Duration,
    model_chunk_bytes: usize,
}

impl<'a> GenerationPipeline<'a> {
    pub fn new(
        clock: &'a dyn Clock,
        lease: &'a dyn ExternalCallLease,
        provider: &'a dyn ModelProvider,
        model_timeout: Duration,
        model_chunk_bytes: usize,
    ) -> Self {
        Self {
            clock,
            lease,
            provider,
            model_timeout,
            model_chunk_bytes,
        }
    }

    pub async fn map(
        &self,
        input: &GenerationInput,
    ) -> Result<Vec<StructuredGenerationResponse>, GenerationError> {
        let chunks = chunk_generation_input(input, self.model_chunk_bytes)?;
        let mut results = Vec::with_capacity(chunks.len());

        for chunk in chunks {
            let request = StructuredGenerationRequest::try_from(StructuredGenerationRequestInput {
                operation: ProviderOperation::Map,
                prompt: chunk.payload().to_owned(),
            })
            .map_err(GenerationError::from_contract)?;
            let response = self.call_provider(request).await?;
            let response = serialized_response(&response)?;

            results.push(validate_map_response(&response, input.memory_scope())?);
        }

        Ok(results)
    }

    pub async fn reduce(
        &self,
        input: &GenerationInput,
        map_results: &[StructuredGenerationResponse],
    ) -> Result<StructuredGenerationResponse, GenerationError> {
        let mut level = map_results.to_vec();
        loop {
            let chunks = chunk_reduction_results(&level, self.model_chunk_bytes)?;
            if level.len() > 1 && chunks.len() >= level.len() {
                return Err(GenerationError::invalid("maximum_bytes", "too_small"));
            }

            let mut reduced = Vec::with_capacity(chunks.len());
            for payload in chunks {
                let request =
                    StructuredGenerationRequest::try_from(StructuredGenerationRequestInput {
                        operation: ProviderOperation::Reduce,
                        prompt: payload,
                    })
                    .map_err(GenerationError::from_contract)?;
                let response = self.call_provider(request).await?;
                let response = serialized_response(&response)?;

                let response = validate_final_response(&response, input.memory_scope())?;
                reduced.push(summary_and_concepts_only(&response)?);
            }

            if reduced.len() == 1 {
                return Ok(reduced
                    .pop()
                    .expect("one reduced response was checked before returning"));
            }
            level = reduced;
        }
    }

    async fn call_provider(
        &self,
        request: StructuredGenerationRequest,
    ) -> Result<StructuredGenerationResponse, GenerationError> {
        let permit = self
            .lease
            .permit(self.model_timeout)
            .await
            .map_err(|_| GenerationError::lease("lost"))?;
        let timeout = self.timeout_for(&permit)?;

        tokio::time::timeout(timeout.duration(), self.provider.generate(request))
            .await
            .map_err(|_| timeout.elapsed_error())?
            .map_err(GenerationError::from_provider)
    }

    fn timeout_for(&self, permit: &LeasePermit) -> Result<ProviderCallTimeout, GenerationError> {
        let remaining = permit
            .deadline()
            .signed_duration_since(self.clock.now())
            .to_std()
            .map_err(|_| GenerationError::lease("deadline_expired"))?;
        if remaining.is_zero() {
            return Err(GenerationError::lease("deadline_expired"));
        }

        Ok(if self.model_timeout <= remaining {
            ProviderCallTimeout::Model(self.model_timeout)
        } else {
            ProviderCallTimeout::Permit(remaining)
        })
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq, Serialize)]
#[error("generation pipeline failure: {field} ({code})")]
pub struct GenerationError {
    field: &'static str,
    code: &'static str,
    category: FailureCategory,
    retryability: Retryability,
}

impl GenerationError {
    pub const fn field(&self) -> &'static str {
        self.field
    }

    pub const fn code(&self) -> &'static str {
        self.code
    }

    pub const fn category(&self) -> FailureCategory {
        self.category
    }

    pub const fn retryability(&self) -> Retryability {
        self.retryability
    }

    const fn invalid(field: &'static str, code: &'static str) -> Self {
        Self::new(field, code, FailureCategory::ProviderContract)
    }

    const fn lease(code: &'static str) -> Self {
        Self::new("lease", code, FailureCategory::LeaseLost)
    }

    const fn from_provider(error: ProviderError) -> Self {
        Self::with_projection("provider", "failed", error.category(), error.retryability())
    }

    const fn from_contract(error: ContractError) -> Self {
        Self::invalid(error.field(), error.code())
    }

    const fn new(field: &'static str, code: &'static str, category: FailureCategory) -> Self {
        Self::with_projection(field, code, category, category.retryability())
    }

    const fn with_projection(
        field: &'static str,
        code: &'static str,
        category: FailureCategory,
        retryability: Retryability,
    ) -> Self {
        Self {
            field,
            code,
            category,
            retryability,
        }
    }
}

#[derive(Clone, Copy)]
enum ProviderCallTimeout {
    Model(Duration),
    Permit(Duration),
}

impl ProviderCallTimeout {
    const fn duration(self) -> Duration {
        match self {
            Self::Model(duration) | Self::Permit(duration) => duration,
        }
    }

    const fn elapsed_error(self) -> GenerationError {
        match self {
            Self::Model(_) => {
                GenerationError::new("provider", "timeout", FailureCategory::ProviderTransient)
            }
            Self::Permit(_) => GenerationError::lease("deadline_elapsed"),
        }
    }
}

#[derive(Clone, PartialEq, Serialize)]
pub struct TranscriptFragment {
    receipt_id: ReceiptId,
    fragment_index: usize,
    content: String,
}

impl TranscriptFragment {
    pub fn receipt_id(&self) -> &ReceiptId {
        &self.receipt_id
    }

    pub const fn fragment_index(&self) -> usize {
        self.fragment_index
    }

    pub fn content(&self) -> &str {
        &self.content
    }
}

impl fmt::Debug for TranscriptFragment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TranscriptFragment")
            .field("receipt_id", &self.receipt_id)
            .field("fragment_index", &self.fragment_index)
            .finish()
    }
}

#[derive(Clone, PartialEq)]
pub struct GenerationChunk {
    fragments: Vec<TranscriptFragment>,
    payload: String,
}

impl GenerationChunk {
    pub fn fragments(&self) -> &[TranscriptFragment] {
        &self.fragments
    }

    pub fn payload(&self) -> &str {
        &self.payload
    }

    pub fn byte_len(&self) -> usize {
        self.payload.len()
    }
}

impl fmt::Debug for GenerationChunk {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GenerationChunk")
            .field("fragment_count", &self.fragments.len())
            .field("byte_len", &self.payload.len())
            .finish()
    }
}

pub fn chunk_generation_input(
    input: &GenerationInput,
    maximum_bytes: usize,
) -> Result<Vec<GenerationChunk>, GenerationError> {
    if maximum_bytes == 0 {
        return Err(GenerationError::invalid("maximum_bytes", "zero"));
    }

    let mut fragments = Vec::new();
    for entry in input.transcript().entries() {
        let serialized_entry = serde_json::to_string(entry)
            .map_err(|_| GenerationError::invalid("transcript", "serialization_failed"))?;
        fragments.extend(fragment_entry(
            entry.receipt_id(),
            &serialized_entry,
            maximum_bytes,
        )?);
    }

    chunk_fragments(fragments, maximum_bytes)
}

pub fn validate_map_response(
    response: &[u8],
    memory_scope: &MemoryScope,
) -> Result<StructuredGenerationResponse, GenerationError> {
    let response = serde_json::from_slice::<StructuredGenerationResponseInput>(response)
        .map_err(|_| GenerationError::invalid("response", "malformed"))?;
    let response =
        StructuredGenerationResponse::try_from(response).map_err(GenerationError::from_contract)?;

    if response.memory_candidates().iter().any(|candidate| {
        candidate
            .supporting_receipt_ids()
            .iter()
            .any(|receipt_id| !memory_scope.contains(receipt_id))
    }) {
        return Err(GenerationError::invalid(
            "supporting_receipt_ids",
            "outside_memory_scope",
        ));
    }

    Ok(response)
}

pub fn validate_final_response(
    response: &[u8],
    memory_scope: &MemoryScope,
) -> Result<StructuredGenerationResponse, GenerationError> {
    let response = validate_map_response(response, memory_scope)?;
    if response.concepts().len() != 10 {
        return Err(GenerationError::invalid("concepts", "wrong_count"));
    }

    Ok(response)
}

fn serialized_response(
    response: &StructuredGenerationResponse,
) -> Result<Vec<u8>, GenerationError> {
    serde_json::to_vec(response)
        .map_err(|_| GenerationError::invalid("response", "serialization_failed"))
}

fn summary_and_concepts_only(
    response: &StructuredGenerationResponse,
) -> Result<StructuredGenerationResponse, GenerationError> {
    StructuredGenerationResponse::try_from(StructuredGenerationResponseInput {
        summary_sentences: response.summary_sentences().to_vec(),
        concepts: response.concepts().to_vec(),
        memory_candidates: Vec::new(),
    })
    .map_err(GenerationError::from_contract)
}

fn fragment_entry(
    receipt_id: &ReceiptId,
    serialized_entry: &str,
    maximum_bytes: usize,
) -> Result<Vec<TranscriptFragment>, GenerationError> {
    let mut fragments = Vec::new();
    let mut start = 0;
    let mut fragment_index = 1;

    while start < serialized_entry.len() {
        let end = largest_fitting_fragment_end(
            receipt_id,
            fragment_index,
            serialized_entry,
            start,
            maximum_bytes,
        )?;
        fragments.push(TranscriptFragment {
            receipt_id: receipt_id.clone(),
            fragment_index,
            content: serialized_entry[start..end].to_owned(),
        });
        start = end;
        fragment_index = fragment_index
            .checked_add(1)
            .ok_or(GenerationError::invalid("fragment_index", "overflow"))?;
    }

    Ok(fragments)
}

fn largest_fitting_fragment_end(
    receipt_id: &ReceiptId,
    fragment_index: usize,
    serialized_entry: &str,
    start: usize,
    maximum_bytes: usize,
) -> Result<usize, GenerationError> {
    let mut boundaries = serialized_entry[start..]
        .char_indices()
        .map(|(offset, _)| start + offset)
        .collect::<Vec<_>>();
    boundaries.push(serialized_entry.len());

    let mut lower = 1;
    let mut upper = boundaries.len() - 1;
    let mut result = None;
    while lower <= upper {
        let middle = lower + (upper - lower) / 2;
        let end = boundaries[middle];
        let fragment = TranscriptFragment {
            receipt_id: receipt_id.clone(),
            fragment_index,
            content: serialized_entry[start..end].to_owned(),
        };
        if serialized_payload(&[fragment])?.len() <= maximum_bytes {
            result = Some(end);
            lower = middle + 1;
        } else {
            upper = middle - 1;
        }
    }

    result.ok_or(GenerationError::invalid("maximum_bytes", "too_small"))
}

fn chunk_fragments(
    fragments: Vec<TranscriptFragment>,
    maximum_bytes: usize,
) -> Result<Vec<GenerationChunk>, GenerationError> {
    let mut chunks = Vec::new();
    let mut current = Vec::new();

    for fragment in fragments {
        current.push(fragment);
        if serialized_payload(&current)?.len() <= maximum_bytes {
            continue;
        }

        let fragment = current
            .pop()
            .expect("a fragment was pushed before checking the chunk size");
        if current.is_empty() {
            return Err(GenerationError::invalid("maximum_bytes", "too_small"));
        }
        chunks.push(GenerationChunk::try_new(std::mem::take(&mut current))?);
        current.push(fragment);
    }

    if !current.is_empty() {
        chunks.push(GenerationChunk::try_new(current)?);
    }

    Ok(chunks)
}

fn chunk_reduction_results(
    results: &[StructuredGenerationResponse],
    maximum_bytes: usize,
) -> Result<Vec<String>, GenerationError> {
    if maximum_bytes == 0 {
        return Err(GenerationError::invalid("maximum_bytes", "zero"));
    }
    if results.is_empty() {
        return Err(GenerationError::invalid("map_results", "empty"));
    }

    let mut chunks = Vec::new();
    let mut current = Vec::new();
    for result in results {
        let item = ReductionItem {
            summary_sentences: result.summary_sentences(),
            concepts: result.concepts(),
        };
        if serialized_reduction_payload(std::slice::from_ref(&item))?.len() > maximum_bytes {
            return Err(GenerationError::invalid("maximum_bytes", "too_small"));
        }

        current.push(item);
        if serialized_reduction_payload(&current)?.len() <= maximum_bytes {
            continue;
        }

        let item = current
            .pop()
            .expect("a reduction item was pushed before checking the chunk size");
        if current.is_empty() {
            return Err(GenerationError::invalid("maximum_bytes", "too_small"));
        }
        chunks.push(serialized_reduction_payload(&std::mem::take(&mut current))?);
        current.push(item);
    }

    if !current.is_empty() {
        chunks.push(serialized_reduction_payload(&current)?);
    }

    Ok(chunks)
}

impl GenerationChunk {
    fn try_new(fragments: Vec<TranscriptFragment>) -> Result<Self, GenerationError> {
        let payload = serialized_payload(&fragments)?;
        Ok(Self { fragments, payload })
    }
}

#[derive(Serialize)]
struct GenerationChunkPayload<'a> {
    fragments: &'a [TranscriptFragment],
}

fn serialized_payload(fragments: &[TranscriptFragment]) -> Result<String, GenerationError> {
    serde_json::to_string(&GenerationChunkPayload { fragments })
        .map_err(|_| GenerationError::invalid("transcript", "serialization_failed"))
}

#[derive(Serialize)]
struct ReductionItem<'a> {
    summary_sentences: &'a [String],
    concepts: &'a [String],
}

#[derive(Serialize)]
struct ReductionPayload<'a> {
    items: &'a [ReductionItem<'a>],
}

fn serialized_reduction_payload(items: &[ReductionItem<'_>]) -> Result<String, GenerationError> {
    serde_json::to_string(&ReductionPayload { items })
        .map_err(|_| GenerationError::invalid("reduce", "serialization_failed"))
}
