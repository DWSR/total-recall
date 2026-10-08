use std::{collections::BTreeMap, fmt};

use chrono::{DateTime, Timelike, Utc};
use memory_store::contracts::{
    Bm25Search, MemoryId, MemorySearchResult, MemoryVersion, MemoryVersionInput, VectorSearch,
};
use uuid::Uuid;

use crate::{
    contracts::{
        GetExactInput, GetLatestInput, ListVersionsInput, MemoryDto, MemoryResults,
        MemorySearchInput, MemoryVersionDto, MemoryVersions, SEARCH_LIMIT_MAX, SaveInput,
        VERSION_LIST_DEFAULT_LIMIT, VERSION_LIST_MAX_LIMIT, VERSION_LIST_MAX_OFFSET,
    },
    embedding::{QueryEmbeddingError, QueryEmbeddingGenerator},
    operations::{MemoryOperations, OperationError, OperationInputField, VersionPageQuery},
};

pub trait CreateContext: Send + Sync {
    fn new_id(&self) -> String;
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Clone, Copy, Default)]
pub struct ProductionCreateContext;

impl CreateContext for ProductionCreateContext {
    fn new_id(&self) -> String {
        Uuid::new_v4().to_string()
    }

    fn now(&self) -> DateTime<Utc> {
        Utc::now()
            .with_nanosecond(0)
            .expect("zero nanoseconds should be a valid UTC timestamp")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolError {
    InvalidInput { field: OperationInputField },
    NotFound,
    Conflict,
    BackendFailure,
    InternalFailure,
}

impl ToolError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidInput { .. } => "invalid_input",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::BackendFailure => "backend_failure",
            Self::InternalFailure => "internal_failure",
        }
    }
}

impl fmt::Display for ToolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for ToolError {}

#[derive(Debug, PartialEq)]
struct SearchCandidates {
    lexical: Vec<MemorySearchResult>,
    vector: Vec<MemorySearchResult>,
}

impl From<OperationError> for ToolError {
    fn from(error: OperationError) -> Self {
        match error {
            OperationError::InvalidInput { field, .. } => Self::InvalidInput { field },
            OperationError::NotFound { .. } => Self::NotFound,
            OperationError::Conflict { .. } => Self::Conflict,
            OperationError::BackendFailure { .. } => Self::BackendFailure,
            OperationError::InvalidBackendResponse { .. } => Self::InternalFailure,
        }
    }
}

pub struct MemoryToolService<O, C, G>
where
    O: MemoryOperations,
    C: CreateContext,
    G: QueryEmbeddingGenerator,
{
    operations: O,
    context: C,
    generator: Option<G>,
}

impl<O, C, G> MemoryToolService<O, C, G>
where
    O: MemoryOperations,
    C: CreateContext,
    G: QueryEmbeddingGenerator,
{
    pub fn new(operations: O, context: C, generator: Option<G>) -> Self {
        Self {
            operations,
            context,
            generator,
        }
    }

    pub async fn save(&self, input: SaveInput) -> Result<MemoryDto, ToolError> {
        validate_save_input(&input)?;

        let timestamp = self.context.now();
        let memory = MemoryVersionInput {
            id: self.context.new_id(),
            version: 1,
            memory_type: "unclassified".to_owned(),
            title: input.title,
            content: input.content,
            created_at: timestamp,
            updated_at: timestamp,
            concepts: Vec::new(),
            files: Vec::new(),
            session_ids: vec![input.session_id],
            source_observation_ids: Vec::new(),
        };

        self.operations
            .insert_memory(memory.clone())
            .await
            .map_err(ToolError::from)?;

        Ok(memory.into())
    }

    pub async fn search(&self, input: MemorySearchInput) -> Result<MemoryResults, ToolError> {
        let limit = input.limit as usize;
        let SearchCandidates { lexical, vector } = self.search_candidates(input).await?;

        let mut candidates_by_id: BTreeMap<String, MemorySearchResult> = BTreeMap::new();
        for candidate in lexical.into_iter().chain(vector) {
            let id = candidate.id().as_str().to_owned();
            match candidates_by_id.get(&id) {
                Some(current) if current.version().get() >= candidate.version().get() => {}
                _ => {
                    candidates_by_id.insert(id, candidate);
                }
            }
        }

        Ok(MemoryResults {
            results: candidates_by_id
                .into_values()
                .take(limit)
                .map(|candidate| MemoryDto::from(&candidate))
                .collect(),
        })
    }

    pub async fn get_latest(&self, input: GetLatestInput) -> Result<MemoryDto, ToolError> {
        let id = MemoryId::try_from(input.id).map_err(|_| ToolError::InvalidInput {
            field: OperationInputField::Id,
        })?;
        self.operations
            .get_latest(id)
            .await
            .map(MemoryDto::from)
            .map_err(ToolError::from)
    }

    pub async fn get_exact(&self, input: GetExactInput) -> Result<MemoryDto, ToolError> {
        let id = MemoryId::try_from(input.id).map_err(|_| ToolError::InvalidInput {
            field: OperationInputField::Id,
        })?;
        let version =
            MemoryVersion::try_from(input.version).map_err(|_| ToolError::InvalidInput {
                field: OperationInputField::Version,
            })?;
        self.operations
            .get_exact(id, version)
            .await
            .map(MemoryDto::from)
            .map_err(ToolError::from)
    }

    pub async fn list_versions(
        &self,
        input: ListVersionsInput,
    ) -> Result<MemoryVersions, ToolError> {
        let id = MemoryId::try_from(input.id).map_err(|_| ToolError::InvalidInput {
            field: OperationInputField::Id,
        })?;
        let offset = input.offset.unwrap_or(0);
        if offset > VERSION_LIST_MAX_OFFSET {
            return Err(ToolError::InvalidInput {
                field: OperationInputField::Offset,
            });
        }

        let limit = input.limit.unwrap_or(VERSION_LIST_DEFAULT_LIMIT);
        if !(1..=VERSION_LIST_MAX_LIMIT).contains(&limit) {
            return Err(ToolError::InvalidInput {
                field: OperationInputField::Limit,
            });
        }

        let probe_limit = limit.checked_add(1).ok_or(ToolError::InternalFailure)?;
        let mut summaries = self
            .operations
            .list_versions(VersionPageQuery {
                id,
                offset,
                limit: probe_limit,
            })
            .await
            .map_err(ToolError::from)?;

        summaries.sort_by_key(|summary| std::cmp::Reverse(summary.version.get()));
        let has_extra_row = summaries.len() > limit as usize;
        let next_offset = if has_extra_row {
            Some(
                offset
                    .checked_add(u64::from(limit))
                    .ok_or(ToolError::InternalFailure)?,
            )
        } else {
            None
        };

        Ok(MemoryVersions {
            versions: summaries
                .into_iter()
                .take(limit as usize)
                .map(|summary| MemoryVersionDto {
                    version: summary.version.get(),
                    updated_at: crate::contracts::mcp_timestamp(summary.updated_at),
                })
                .collect(),
            next_offset,
        })
    }

    async fn search_candidates(
        &self,
        input: MemorySearchInput,
    ) -> Result<SearchCandidates, ToolError> {
        validate_search_input(&input)?;

        let MemorySearchInput { query, limit } = input;
        // `join!` lets in-flight generation finish after a BM25 failure; dropping the SDK
        // future early would skip its pending-invocation cleanup.
        let (lexical, vector) = tokio::join!(
            self.operations.search_lexical(Bm25Search {
                query: query.clone(),
                limit,
            }),
            self.semantic_candidates(&query, limit),
        );

        Ok(SearchCandidates {
            lexical: lexical.map_err(ToolError::from)?,
            vector: vector.map_err(ToolError::from)?,
        })
    }

    async fn semantic_candidates(
        &self,
        query: &str,
        limit: u32,
    ) -> Result<Vec<MemorySearchResult>, OperationError> {
        let Some(generator) = &self.generator else {
            return Ok(Vec::new());
        };

        match generator.generate(query).await {
            Ok(embedding) => {
                self.operations
                    .search_vector(VectorSearch {
                        vector: embedding.as_slice().to_vec(),
                        limit,
                    })
                    .await
            }
            Err(QueryEmbeddingError::Unavailable | QueryEmbeddingError::InvalidResponse) => {
                Ok(Vec::new())
            }
        }
    }
}

fn validate_save_input(input: &SaveInput) -> Result<(), ToolError> {
    if input.title.is_empty() {
        return Err(ToolError::InvalidInput {
            field: OperationInputField::Title,
        });
    }

    if input.content.is_empty() {
        return Err(ToolError::InvalidInput {
            field: OperationInputField::Content,
        });
    }

    if input.session_id.is_empty() {
        return Err(ToolError::InvalidInput {
            field: OperationInputField::SessionId,
        });
    }

    Ok(())
}

fn validate_search_input(input: &MemorySearchInput) -> Result<(), ToolError> {
    if input.query.is_empty() || input.query.contains('\0') {
        return Err(ToolError::InvalidInput {
            field: OperationInputField::Query,
        });
    }

    if !(1..=SEARCH_LIMIT_MAX).contains(&input.limit) {
        return Err(ToolError::InvalidInput {
            field: OperationInputField::Limit,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use tokio::{
        sync::{Barrier, Notify},
        time::timeout,
    };

    use crate::{
        contracts::{
            ListVersionsInput, MemoryVersions, SEARCH_LIMIT_MAX, VERSION_LIST_DEFAULT_LIMIT,
            VERSION_LIST_MAX_LIMIT, VERSION_LIST_MAX_NEXT_OFFSET, VERSION_LIST_MAX_OFFSET,
        },
        embedding::{
            QueryEmbeddingError, QueryEmbeddingGenerator, RecordingQueryEmbeddingGenerator,
        },
        operations::{
            BackendResponseField, BackendSource, FailingOperations, MemoryOperationFailures,
            MemoryOperationResponses, MemoryOperations, MemoryVersionSummary, Operation,
            OperationCall, OperationError, OperationInputField, RecordingOperations,
            VersionPageQuery,
        },
    };
    use memory_store::contracts::{
        Bm25Search, EmbeddingVector, MemoryId, MemoryVersion, VectorSearch,
    };

    const QUERY: &str = " query-secret-sentinel ";
    const GENERATED_VECTOR: &[f64] = &[987_654.25, -123_456.5];
    const GENERATED_VECTOR_TEXT: [&str; 2] = ["987654.25", "123456.5"];
    const GENERATION_FAILURES: [QueryEmbeddingError; 2] = [
        QueryEmbeddingError::Unavailable,
        QueryEmbeddingError::InvalidResponse,
    ];
    const CONCURRENCY_BOUND: Duration = Duration::from_secs(5);
    const LIMIT: u32 = 7;
    const VERSION_LIST_ID: &str = "version-list-id-secret-sentinel";
    const LATEST_ID: &str = " latest-id-secret-sentinel ";
    const EXACT_ID: &str = " exact-id-secret-sentinel ";
    const EXACT_VERSION: i64 = 37;

    #[tokio::test]
    async fn search_candidates_run_only_bm25_when_query_embedding_is_disabled() {
        let lexical_results = vec![search_result("lexical-result", 0.75)];
        let operations = RecordingOperations::new(search_responses(
            Ok(lexical_results.clone()),
            Ok(vec![search_result("unrequested-vector-result", 0.5)]),
        ));
        let observer = operations.clone();
        let service = service(operations, None);

        let candidates = service
            .search_candidates(valid_search_input())
            .await
            .expect("disabled query embedding should still run BM25");

        assert_eq!(
            candidates,
            SearchCandidates {
                lexical: lexical_results,
                vector: Vec::new(),
            }
        );
        assert_eq!(observer.calls(), lexical_only_calls(LIMIT));
    }

    #[tokio::test]
    async fn search_candidates_search_with_the_generated_vector_when_query_embedding_is_enabled() {
        let generator = enabled_generator();
        let generator_observer = generator.clone();
        let lexical_results = vec![search_result("lexical-result", 0.75)];
        let vector_results = vec![search_result("vector-result", 0.5)];
        let operations = RecordingOperations::new(search_responses(
            Ok(lexical_results.clone()),
            Ok(vector_results.clone()),
        ));
        let observer = operations.clone();
        let service = service(operations, Some(generator));

        let candidates = service
            .search_candidates(valid_search_input())
            .await
            .expect("enabled query embedding should run BM25 and vector search");

        assert_eq!(
            candidates,
            SearchCandidates {
                lexical: lexical_results,
                vector: vector_results,
            }
        );
        assert_eq!(observer.calls(), combined_calls(LIMIT));
        assert_eq!(generator_observer.calls(), vec![QUERY.to_owned()]);
    }

    #[tokio::test]
    async fn search_candidates_use_empty_vector_candidates_after_each_generation_failure() {
        for failure in GENERATION_FAILURES {
            let generator = failing_generator(failure);
            let generator_observer = generator.clone();
            let lexical_results = vec![search_result("lexical-result", 0.75)];
            let operations = RecordingOperations::new(search_responses(
                Ok(lexical_results.clone()),
                Ok(vec![search_result("unrequested-vector-result", 0.5)]),
            ));
            let observer = operations.clone();
            let service = service(operations, Some(generator));

            let candidates = service
                .search_candidates(valid_search_input())
                .await
                .expect("a generation failure should fall back to BM25 candidates");

            assert_eq!(
                candidates,
                SearchCandidates {
                    lexical: lexical_results,
                    vector: Vec::new(),
                }
            );
            assert_eq!(observer.calls(), lexical_only_calls(LIMIT));
            assert_eq!(generator_observer.calls(), vec![QUERY.to_owned()]);
        }
    }

    #[tokio::test]
    async fn search_candidates_forward_every_query_byte_to_bm25_and_generation() {
        for query in [
            QUERY,
            " ",
            " \t\r\n ",
            "\u{feff}na\u{ef}ve cafe\u{301} \u{2615}",
            "\u{7}control\u{1b}[0m\u{7f}",
            "\"quoted\" \\backslash\\ trailing ",
        ] {
            for generator in [None, Some(enabled_generator())] {
                let generator_observer = generator.clone();
                let operations =
                    RecordingOperations::new(search_responses(Ok(Vec::new()), Ok(Vec::new())));
                let observer = operations.clone();
                let service = service(operations, generator);

                service
                    .search_candidates(search_input(query, LIMIT))
                    .await
                    .expect("every non-empty NUL-free query should be searchable");

                let mut expected_calls = vec![lexical_call(query, LIMIT)];
                if let Some(generator) = generator_observer {
                    assert_eq!(generator.calls(), vec![query.to_owned()]);
                    expected_calls.push(generated_vector_call(LIMIT));
                }
                assert_eq!(observer.calls(), expected_calls);
            }
        }
    }

    #[tokio::test]
    async fn search_candidates_accept_both_inclusive_limit_boundaries() {
        for limit in [1, SEARCH_LIMIT_MAX] {
            for generator in [None, Some(enabled_generator())] {
                let generator_observer = generator.clone();
                let operations =
                    RecordingOperations::new(search_responses(Ok(Vec::new()), Ok(Vec::new())));
                let observer = operations.clone();
                let service = service(operations, generator);

                service
                    .search_candidates(search_input(QUERY, limit))
                    .await
                    .expect("inclusive search limits should be accepted");

                match generator_observer {
                    Some(generator) => {
                        assert_eq!(observer.calls(), combined_calls(limit));
                        assert_eq!(generator.calls(), vec![QUERY.to_owned()]);
                    }
                    None => assert_eq!(observer.calls(), lexical_only_calls(limit)),
                }
            }
        }
    }

    #[tokio::test]
    async fn search_candidates_reject_invalid_input_without_generation_or_search() {
        for generator in [
            None,
            Some(enabled_generator()),
            Some(failing_generator(QueryEmbeddingError::Unavailable)),
        ] {
            for (field, input) in [
                (OperationInputField::Query, search_input("", LIMIT)),
                (OperationInputField::Query, search_input("\0", LIMIT)),
                (
                    OperationInputField::Query,
                    search_input(format!("\0{QUERY}"), LIMIT),
                ),
                (
                    OperationInputField::Query,
                    search_input(" query-secret\0-sentinel ", LIMIT),
                ),
                (
                    OperationInputField::Query,
                    search_input(format!("{QUERY}\0"), LIMIT),
                ),
                (OperationInputField::Query, search_input("", 0)),
                (OperationInputField::Limit, search_input(QUERY, 0)),
                (
                    OperationInputField::Limit,
                    search_input(QUERY, SEARCH_LIMIT_MAX + 1),
                ),
                (OperationInputField::Limit, search_input(QUERY, u32::MAX)),
            ] {
                let operations =
                    RecordingOperations::new(search_responses(Ok(Vec::new()), Ok(Vec::new())));
                let observer = operations.clone();
                let service = service(operations, generator.clone());

                let error = service
                    .search_candidates(input)
                    .await
                    .expect_err("invalid search input should be rejected");

                assert_eq!(error, ToolError::InvalidInput { field });
                assert_error_is_opaque(&error);
                assert!(observer.calls().is_empty());
            }
            assert_generator_not_invoked(&generator);
        }
    }

    #[tokio::test]
    async fn search_candidates_return_the_classified_bm25_error_without_vector_candidates() {
        for (case, generator, lexical_error, expected_error, expected_calls) in [
            (
                "disabled embedding",
                None,
                OperationError::backend_failure(
                    Operation::SearchLexical,
                    BackendSource::MemoryStore,
                ),
                ToolError::BackendFailure,
                lexical_only_calls(LIMIT),
            ),
            (
                "generated vector",
                Some(enabled_generator()),
                OperationError::backend_failure(
                    Operation::SearchLexical,
                    BackendSource::MemoryStore,
                ),
                ToolError::BackendFailure,
                combined_calls(LIMIT),
            ),
            (
                "generation failure",
                Some(failing_generator(QueryEmbeddingError::Unavailable)),
                OperationError::backend_failure(
                    Operation::SearchLexical,
                    BackendSource::MemoryStore,
                ),
                ToolError::BackendFailure,
                lexical_only_calls(LIMIT),
            ),
            (
                "invalid BM25 response",
                Some(enabled_generator()),
                OperationError::invalid_backend_response(
                    Operation::SearchLexical,
                    BackendResponseField::SearchResult,
                ),
                ToolError::InternalFailure,
                combined_calls(LIMIT),
            ),
        ] {
            let operations = RecordingOperations::new(search_responses(
                Err(lexical_error),
                Ok(vec![search_result("vector-result", 0.5)]),
            ));
            let observer = operations.clone();
            let service = service(operations, generator);

            let error = service
                .search_candidates(valid_search_input())
                .await
                .expect_err("a failed BM25 search must not return vector candidates");

            assert_eq!(error, expected_error, "{case}");
            assert_eq!(error.to_string(), error.code(), "{case}");
            assert_error_is_opaque(&error);
            assert_eq!(observer.calls(), expected_calls, "{case}");
        }
    }

    #[tokio::test]
    async fn search_candidates_return_the_classified_vector_error_without_bm25_candidates() {
        for (vector_error, expected_error) in [
            (
                OperationError::backend_failure(
                    Operation::SearchVector,
                    BackendSource::MemoryStore,
                ),
                ToolError::BackendFailure,
            ),
            (
                OperationError::invalid_backend_response(
                    Operation::SearchVector,
                    BackendResponseField::SearchResult,
                ),
                ToolError::InternalFailure,
            ),
        ] {
            let generator = enabled_generator();
            let generator_observer = generator.clone();
            let operations = RecordingOperations::new(search_responses(
                Ok(vec![search_result("lexical-result", 0.75)]),
                Err(vector_error),
            ));
            let observer = operations.clone();
            let service = service(operations, Some(generator));

            let error = service
                .search_candidates(valid_search_input())
                .await
                .expect_err("a failed vector search must not return BM25 candidates");

            assert_eq!(error, expected_error);
            assert_eq!(error.to_string(), error.code());
            assert_error_is_opaque(&error);
            assert_eq!(observer.calls(), combined_calls(LIMIT));
            assert_eq!(generator_observer.calls(), vec![QUERY.to_owned()]);
        }
    }

    #[tokio::test]
    async fn search_candidates_use_lexical_error_precedence_after_both_searches_fail() {
        let operations = FailingOperations::new(search_failures(
            OperationError::backend_failure(Operation::SearchLexical, BackendSource::MemoryStore),
            OperationError::invalid_backend_response(
                Operation::SearchVector,
                BackendResponseField::SearchResult,
            ),
        ));
        let observer = operations.clone();
        let service = service(operations, Some(enabled_generator()));

        let error = service
            .search_candidates(valid_search_input())
            .await
            .expect_err("two failed searches must return one typed error");

        assert_eq!(error, ToolError::BackendFailure);
        assert_error_is_opaque(&error);
        assert_eq!(observer.calls(), combined_calls(LIMIT));
    }

    #[tokio::test]
    async fn search_starts_bm25_and_query_embedding_generation_concurrently() {
        let rendezvous = Arc::new(Barrier::new(2));
        let lexical = canonical_memory("memory-b", 1, "lexical-b");
        let vector = canonical_memory("memory-a", 1, "vector-a");
        let recorder = RecordingOperations::new(search_responses(
            Ok(vec![search_result_from_memory(lexical.clone(), 0.75)]),
            Ok(vec![search_result_from_memory(vector.clone(), 0.5)]),
        ));
        let observer = recorder.clone();
        let generator_recorder = enabled_generator();
        let generator_observer = generator_recorder.clone();
        let service = MemoryToolService::new(
            RendezvousOperations {
                recorder,
                rendezvous: Arc::clone(&rendezvous),
            },
            ProductionCreateContext,
            Some(RendezvousQueryEmbeddingGenerator {
                recorder: generator_recorder,
                rendezvous,
            }),
        );

        let results = timeout(CONCURRENCY_BOUND, service.search(valid_search_input()))
            .await
            .expect("BM25 and query embedding generation should be in flight together")
            .expect("concurrent searches should succeed");

        assert_eq!(
            results,
            MemoryResults {
                results: vec![MemoryDto::from(vector), MemoryDto::from(lexical)],
            }
        );
        assert_eq!(observer.calls(), combined_calls(LIMIT));
        assert_eq!(generator_observer.calls(), vec![QUERY.to_owned()]);
    }

    #[tokio::test]
    async fn search_issues_vector_search_only_after_generation_completes() {
        let generator = GatedQueryEmbeddingGenerator::new(Ok(generated_embedding()));
        let gate = generator.clone();
        let lexical = canonical_memory("memory-b", 1, "lexical-b");
        let vector = canonical_memory("memory-a", 1, "vector-a");
        let operations = RecordingOperations::new(search_responses(
            Ok(vec![search_result_from_memory(lexical.clone(), 0.75)]),
            Ok(vec![search_result_from_memory(vector.clone(), 0.5)]),
        ));
        let observer = operations.clone();
        let service = MemoryToolService::new(operations, ProductionCreateContext, Some(generator));
        let search = tokio::spawn(async move { service.search(valid_search_input()).await });

        timeout(CONCURRENCY_BOUND, gate.wait_until_started())
            .await
            .expect("query embedding generation should start");
        settle().await;

        assert_eq!(observer.calls(), lexical_only_calls(LIMIT));
        assert!(
            !search.is_finished(),
            "search should wait for in-flight query embedding generation"
        );

        gate.release();
        let results = timeout(CONCURRENCY_BOUND, search)
            .await
            .expect("search should finish after generation completes")
            .expect("search task should not panic")
            .expect("search should merge BM25 and generated-vector candidates");

        assert!(gate.completed());
        assert_eq!(
            results,
            MemoryResults {
                results: vec![MemoryDto::from(vector), MemoryDto::from(lexical)],
            }
        );
        assert_eq!(observer.calls(), combined_calls(LIMIT));
        assert_eq!(gate.calls(), vec![QUERY.to_owned()]);
    }

    #[tokio::test]
    async fn search_waits_for_in_flight_generation_after_bm25_fails() {
        let generator = GatedQueryEmbeddingGenerator::new(Ok(generated_embedding()));
        let gate = generator.clone();
        let operations = FailingOperations::new(search_failures(
            OperationError::backend_failure(Operation::SearchLexical, BackendSource::MemoryStore),
            OperationError::invalid_backend_response(
                Operation::SearchVector,
                BackendResponseField::SearchResult,
            ),
        ));
        let observer = operations.clone();
        let service = MemoryToolService::new(operations, ProductionCreateContext, Some(generator));
        let search = tokio::spawn(async move { service.search(valid_search_input()).await });

        timeout(CONCURRENCY_BOUND, gate.wait_until_started())
            .await
            .expect("query embedding generation should start while BM25 is in flight");
        settle().await;

        assert_eq!(observer.calls(), lexical_only_calls(LIMIT));
        assert!(
            !search.is_finished(),
            "a BM25 failure must not cancel in-flight query embedding generation"
        );
        assert!(!gate.completed());

        gate.release();
        let error = timeout(CONCURRENCY_BOUND, search)
            .await
            .expect("search should finish after generation completes")
            .expect("search task should not panic")
            .expect_err("a BM25 failure should fail the search");

        assert_eq!(error, ToolError::BackendFailure);
        assert!(gate.completed());
        assert_eq!(observer.calls(), combined_calls(LIMIT));
        assert_eq!(gate.calls(), vec![QUERY.to_owned()]);
    }

    #[tokio::test]
    async fn search_deduplicates_candidates_and_keeps_greater_versions_with_lexical_ties() {
        let generator = enabled_generator();
        let generator_observer = generator.clone();
        let lexical_z = canonical_memory("memory-z", 1, "lexical-z");
        let lexical_b_first = canonical_memory("memory-b", 1, "lexical-b-first");
        let lexical_b_duplicate = canonical_memory("memory-b", 1, "lexical-b-duplicate");
        let lexical_c = canonical_memory("memory-c", 2, "lexical-c");
        let vector_a_first = canonical_memory("memory-a", 1, "vector-a-first");
        let vector_a_duplicate = canonical_memory("memory-a", 1, "vector-a-duplicate");
        let vector_b_newer = canonical_memory("memory-b", 3, "vector-b-newer");
        let vector_c_equal = canonical_memory("memory-c", 2, "vector-c-equal");
        let operations = RecordingOperations::new(search_responses(
            Ok(vec![
                search_result_from_memory(lexical_z.clone(), 0.99),
                search_result_from_memory(lexical_b_first.clone(), 0.75),
                search_result_from_memory(lexical_b_duplicate, 0.01),
                search_result_from_memory(lexical_c.clone(), 0.01),
            ]),
            Ok(vec![
                search_result_from_memory(vector_a_first.clone(), 0.25),
                search_result_from_memory(vector_a_duplicate, 0.99),
                search_result_from_memory(vector_b_newer.clone(), 0.01),
                search_result_from_memory(vector_c_equal, 0.99),
            ]),
        ));
        let observer = operations.clone();
        let service = service(operations, Some(generator));

        let results = service
            .search(valid_search_input())
            .await
            .expect("valid searches should return a merged result");

        assert_eq!(
            results,
            MemoryResults {
                results: vec![
                    MemoryDto::from(vector_a_first),
                    MemoryDto::from(vector_b_newer),
                    MemoryDto::from(lexical_c),
                    MemoryDto::from(lexical_z),
                ],
            }
        );
        assert_eq!(observer.calls(), combined_calls(LIMIT));
        assert_eq!(generator_observer.calls(), vec![QUERY.to_owned()]);
    }

    #[tokio::test]
    async fn search_projects_lexical_only_results_through_the_same_merge() {
        for generator in [
            None,
            Some(failing_generator(QueryEmbeddingError::Unavailable)),
            Some(failing_generator(QueryEmbeddingError::InvalidResponse)),
        ] {
            let lexical_z = canonical_memory("memory-z", 1, "lexical-z");
            let lexical_b_older = canonical_memory("memory-b", 1, "lexical-b-older");
            let lexical_b_newer = canonical_memory("memory-b", 2, "lexical-b-newer");
            let lexical_b_duplicate = canonical_memory("memory-b", 2, "lexical-b-duplicate");
            let lexical_a = canonical_memory("memory-a", 1, "lexical-a");
            let lexical_c = canonical_memory("memory-c", 1, "lexical-c");
            let operations = RecordingOperations::new(search_responses(
                Ok(vec![
                    search_result_from_memory(lexical_z, 0.99),
                    search_result_from_memory(lexical_b_older, 0.75),
                    search_result_from_memory(lexical_b_newer.clone(), 0.5),
                    search_result_from_memory(lexical_a.clone(), 0.25),
                    search_result_from_memory(lexical_b_duplicate, 0.2),
                    search_result_from_memory(lexical_c.clone(), 0.1),
                ]),
                Ok(vec![search_result_from_memory(
                    canonical_memory("memory-0", 9, "unrequested-vector"),
                    0.99,
                )]),
            ));
            let observer = operations.clone();
            let service = service(operations, generator);

            let results = service
                .search(search_input(QUERY, 3))
                .await
                .expect("lexical-only searches should return a merged result");

            assert_eq!(
                results,
                MemoryResults {
                    results: vec![
                        MemoryDto::from(lexical_a),
                        MemoryDto::from(lexical_b_newer),
                        MemoryDto::from(lexical_c),
                    ],
                }
            );
            assert_eq!(observer.calls(), lexical_only_calls(3));
        }
    }

    #[tokio::test]
    async fn search_omits_scores_and_embeddings_from_serialized_results() {
        for generator in [None, Some(enabled_generator())] {
            let candidate = canonical_memory("memory-score-free", 2, "score-free");
            let vector_candidate = canonical_memory("memory-vector-score-free", 1, "vector");
            let operations = RecordingOperations::new(search_responses(
                Ok(vec![search_result_from_memory(candidate, 0.99)]),
                Ok(vec![search_result_from_memory(vector_candidate, 0.875)]),
            ));
            let service = service(operations, generator);

            let results = service
                .search(valid_search_input())
                .await
                .expect("valid searches should return a score-free projection");
            let serialized = serde_json::to_value(&results)
                .expect("search results should serialize successfully");
            let memory = serialized["results"][0]
                .as_object()
                .expect("one canonical memory projection should be returned");

            assert_eq!(memory.len(), 11);
            assert_eq!(memory["id"], "memory-score-free");
            assert_eq!(memory["version"], 2);
            assert_eq!(memory["title"], "score-free title");
            assert_eq!(memory["content"], "score-free content");
            assert_eq!(
                memory["concepts"],
                serde_json::json!(["score-free concept"])
            );
            assert_eq!(memory["files"], serde_json::json!(["score-free file"]));
            assert_eq!(
                memory["session_ids"],
                serde_json::json!(["score-free session"])
            );
            assert_eq!(
                memory["source_observation_ids"],
                serde_json::json!(["score-free observation"])
            );
            assert!(!memory.contains_key("relevance"));
            assert!(!memory.contains_key("embedding"));

            let rendered = serialized.to_string();
            for protected_value in ["0.99", "0.875", "relevance", "embedding"]
                .into_iter()
                .chain(GENERATED_VECTOR_TEXT)
            {
                assert!(
                    !rendered.contains(protected_value),
                    "search results exposed a score or embedding: {rendered}"
                );
            }
        }
    }

    #[tokio::test]
    async fn search_sorts_before_applying_the_requested_limit() {
        let operations = RecordingOperations::new(search_responses(
            Ok(vec![
                search_result_from_memory(canonical_memory("memory-z", 1, "lexical-z"), 0.99),
                search_result_from_memory(canonical_memory("memory-y", 1, "lexical-y"), 0.75),
            ]),
            Ok(vec![
                search_result_from_memory(canonical_memory("memory-x", 1, "vector-x"), 0.5),
                search_result_from_memory(canonical_memory("memory-a", 1, "vector-a"), 0.25),
            ]),
        ));
        let observer = operations.clone();
        let service = service(operations, Some(enabled_generator()));

        let results = service
            .search(search_input(QUERY, 2))
            .await
            .expect("valid searches should sort then cap the merged union");

        assert_eq!(
            results
                .results
                .iter()
                .map(|memory| memory.id.as_str())
                .collect::<Vec<_>>(),
            vec!["memory-a", "memory-x"]
        );
        assert_eq!(observer.calls(), combined_calls(2));
    }

    #[tokio::test]
    async fn search_returns_an_empty_collection_when_every_selected_search_is_empty() {
        for (generator, vector_response, expected_calls) in [
            (
                None,
                Ok(vec![search_result("unrequested-vector-result", 0.5)]),
                lexical_only_calls(LIMIT),
            ),
            (
                Some(failing_generator(QueryEmbeddingError::Unavailable)),
                Ok(vec![search_result("unrequested-vector-result", 0.5)]),
                lexical_only_calls(LIMIT),
            ),
            (
                Some(enabled_generator()),
                Ok(Vec::new()),
                combined_calls(LIMIT),
            ),
        ] {
            let operations =
                RecordingOperations::new(search_responses(Ok(Vec::new()), vector_response));
            let observer = operations.clone();
            let service = service(operations, generator);

            let results = service
                .search(valid_search_input())
                .await
                .expect("empty candidate sets should be a successful search");

            assert_eq!(
                results,
                MemoryResults {
                    results: Vec::new()
                }
            );
            assert_eq!(observer.calls(), expected_calls);
        }
    }

    #[tokio::test]
    async fn list_versions_defaults_to_first_page_and_projects_narrow_newest_first_results() {
        for generator in generator_compositions() {
            let generator_observer = generator.clone();
            let summaries = (1..=i64::from(VERSION_LIST_DEFAULT_LIMIT) + 1)
                .map(version_summary)
                .collect();
            let operations = RecordingOperations::new(version_responses(Ok(summaries)));
            let observer = operations.clone();
            let service = service(operations, generator);

            let result = service
                .list_versions(version_list_input(VERSION_LIST_ID, None, None))
                .await
                .expect("default version page should succeed");

            assert_eq!(
                result.next_offset,
                Some(u64::from(VERSION_LIST_DEFAULT_LIMIT))
            );
            assert_eq!(
                result
                    .versions
                    .iter()
                    .map(|version| version.version)
                    .collect::<Vec<_>>(),
                (2..=i64::from(VERSION_LIST_DEFAULT_LIMIT) + 1)
                    .rev()
                    .collect::<Vec<_>>(),
            );
            assert_version_call(observer.calls(), 0, VERSION_LIST_DEFAULT_LIMIT + 1);

            let serialized =
                serde_json::to_value(&result).expect("version pages should serialize successfully");
            let page = serialized
                .as_object()
                .expect("version pages should serialize as objects");
            assert_eq!(page.len(), 2);
            assert!(page.contains_key("versions"));
            assert!(page.contains_key("next_offset"));
            let version = serialized["versions"][0]
                .as_object()
                .expect("version summaries should serialize as objects");
            assert_eq!(version.len(), 2);
            assert_eq!(
                version["version"],
                serde_json::json!(i64::from(VERSION_LIST_DEFAULT_LIMIT) + 1)
            );
            assert_eq!(version["updated_at"], serde_json::json!(timestamp()));
            assert_generator_not_invoked(&generator_observer);
        }
    }

    #[tokio::test]
    async fn list_versions_accepts_inclusive_pagination_boundaries_and_maximum_next_offset() {
        for (offset, limit, expected_next_offset) in [
            (0_u64, 1_u32, 1_u64),
            (
                VERSION_LIST_MAX_OFFSET,
                VERSION_LIST_MAX_LIMIT,
                VERSION_LIST_MAX_NEXT_OFFSET,
            ),
        ] {
            let summaries = (1..=i64::from(limit) + 1).map(version_summary).collect();
            let operations = RecordingOperations::new(version_responses(Ok(summaries)));
            let observer = operations.clone();
            let service = service(operations, None);

            let result = service
                .list_versions(version_list_input(
                    VERSION_LIST_ID,
                    Some(offset),
                    Some(limit),
                ))
                .await
                .expect("inclusive pagination boundaries should succeed");

            assert_eq!(result.versions.len(), limit as usize);
            assert_eq!(result.next_offset, Some(expected_next_offset));
            assert_eq!(result.versions[0].version, i64::from(limit) + 1);
            assert_version_call(observer.calls(), offset, limit + 1);
        }
    }

    #[tokio::test]
    async fn list_versions_returns_continuation_only_when_an_extra_row_exists() {
        for (summaries, expected_versions, expected_next_offset) in [
            (
                vec![version_summary(2), version_summary(4), version_summary(3)],
                vec![4, 3],
                Some(19),
            ),
            (
                vec![version_summary(2), version_summary(3)],
                vec![3, 2],
                None,
            ),
        ] {
            let operations = RecordingOperations::new(version_responses(Ok(summaries)));
            let observer = operations.clone();
            let service = service(operations, None);

            let result = service
                .list_versions(version_list_input(VERSION_LIST_ID, Some(17), Some(2)))
                .await
                .expect("configured version page should succeed");

            assert_eq!(
                result
                    .versions
                    .iter()
                    .map(|version| version.version)
                    .collect::<Vec<_>>(),
                expected_versions
            );
            assert_eq!(result.next_offset, expected_next_offset);
            assert_version_call(observer.calls(), 17, 3);
        }
    }

    #[tokio::test]
    async fn list_versions_returns_an_empty_terminal_page() {
        let operations = RecordingOperations::new(version_responses(Ok(Vec::new())));
        let observer = operations.clone();
        let service = service(operations, None);

        let result = service
            .list_versions(version_list_input(VERSION_LIST_ID, Some(9), Some(7)))
            .await
            .expect("empty version page should succeed");

        assert_eq!(
            result,
            MemoryVersions {
                versions: Vec::new(),
                next_offset: None,
            }
        );
        assert_version_call(observer.calls(), 9, 8);
    }

    #[tokio::test]
    async fn list_versions_rejects_invalid_input_without_submitting_a_port_call() {
        for (field, input) in [
            (OperationInputField::Id, version_list_input("", None, None)),
            (
                OperationInputField::Id,
                version_list_input(format!("{VERSION_LIST_ID}\0"), None, None),
            ),
            (
                OperationInputField::Offset,
                version_list_input(VERSION_LIST_ID, Some(VERSION_LIST_MAX_OFFSET + 1), None),
            ),
            (
                OperationInputField::Limit,
                version_list_input(VERSION_LIST_ID, None, Some(0)),
            ),
            (
                OperationInputField::Limit,
                version_list_input(VERSION_LIST_ID, None, Some(VERSION_LIST_MAX_LIMIT + 1)),
            ),
        ] {
            let operations = RecordingOperations::new(version_responses(Ok(Vec::new())));
            let observer = operations.clone();
            let service = service(operations, None);

            let error = service
                .list_versions(input)
                .await
                .expect_err("invalid version-list input should be rejected");

            assert_eq!(error, ToolError::InvalidInput { field });
            assert!(observer.calls().is_empty());
        }
    }

    #[tokio::test]
    async fn list_versions_maps_closed_port_failures_without_a_partial_page() {
        for (operation_error, expected_error) in [
            (
                OperationError::invalid_input(Operation::ListVersions, OperationInputField::Id),
                ToolError::InvalidInput {
                    field: OperationInputField::Id,
                },
            ),
            (
                OperationError::backend_failure(
                    Operation::ListVersions,
                    BackendSource::MemoryStore,
                ),
                ToolError::BackendFailure,
            ),
            (
                OperationError::invalid_backend_response(
                    Operation::ListVersions,
                    BackendResponseField::VersionSummary,
                ),
                ToolError::InternalFailure,
            ),
        ] {
            let operations = FailingOperations::new(version_failures(operation_error));
            let observer = operations.clone();
            let service = service(operations, None);

            let error = service
                .list_versions(version_list_input(VERSION_LIST_ID, None, None))
                .await
                .expect_err("a port failure must not return a partial version page");

            assert_eq!(error, expected_error);
            assert_eq!(error.to_string(), error.code());
            assert_error_is_opaque(&error);
            assert_version_call(observer.calls(), 0, VERSION_LIST_DEFAULT_LIMIT + 1);
        }
    }

    #[tokio::test]
    async fn get_latest_projects_a_complete_memory_and_only_invokes_latest() {
        for generator in generator_compositions() {
            let generator_observer = generator.clone();
            let expected = canonical_memory(LATEST_ID, 11, "latest-retrieval");
            let operations = RecordingOperations::new(retrieval_responses(
                Ok(expected.clone()),
                Err(OperationError::not_found(Operation::GetExact)),
            ));
            let observer = operations.clone();
            let service = service(operations, generator);

            let memory = service
                .get_latest(GetLatestInput {
                    id: LATEST_ID.to_owned(),
                })
                .await
                .expect("a latest retrieval should return the complete canonical memory");

            assert_eq!(memory, MemoryDto::from(expected.clone()));
            assert_retrieval_call(observer.calls(), OperationCall::GetLatest(latest_id()));
            assert_serialized_memory_is_complete_and_private(&memory, &expected);
            assert_generator_not_invoked(&generator_observer);
        }
    }

    #[tokio::test]
    async fn get_exact_projects_a_complete_memory_and_only_invokes_exact() {
        for generator in generator_compositions() {
            let generator_observer = generator.clone();
            let expected = canonical_memory(EXACT_ID, EXACT_VERSION, "exact-retrieval");
            let operations = RecordingOperations::new(retrieval_responses(
                Err(OperationError::not_found(Operation::GetLatest)),
                Ok(expected.clone()),
            ));
            let observer = operations.clone();
            let service = service(operations, generator);

            let memory = service
                .get_exact(GetExactInput {
                    id: EXACT_ID.to_owned(),
                    version: EXACT_VERSION,
                })
                .await
                .expect("an exact retrieval should return the complete canonical memory");

            assert_eq!(memory, MemoryDto::from(expected.clone()));
            assert_retrieval_call(
                observer.calls(),
                OperationCall::GetExact {
                    id: exact_id(),
                    version: exact_version(),
                },
            );
            assert_serialized_memory_is_complete_and_private(&memory, &expected);
            assert_generator_not_invoked(&generator_observer);
        }
    }

    #[tokio::test]
    async fn get_latest_rejects_empty_and_nul_ids_without_submitting_a_port_call() {
        for id in [String::new(), format!("{LATEST_ID}\0")] {
            let operations = RecordingOperations::new(retrieval_responses(
                Ok(canonical_memory(LATEST_ID, 1, "unused-latest")),
                Ok(canonical_memory(EXACT_ID, 1, "unused-exact")),
            ));
            let observer = operations.clone();
            let service = service(operations, None);

            let error = service
                .get_latest(GetLatestInput { id })
                .await
                .expect_err("an invalid latest ID should be rejected");

            assert_eq!(
                error,
                ToolError::InvalidInput {
                    field: OperationInputField::Id,
                }
            );
            assert!(observer.calls().is_empty());
        }
    }

    #[tokio::test]
    async fn get_exact_rejects_invalid_ids_and_versions_without_submitting_a_port_call() {
        for input in [
            GetExactInput {
                id: String::new(),
                version: EXACT_VERSION,
            },
            GetExactInput {
                id: format!("{EXACT_ID}\0"),
                version: EXACT_VERSION,
            },
            GetExactInput {
                id: EXACT_ID.to_owned(),
                version: 0,
            },
            GetExactInput {
                id: EXACT_ID.to_owned(),
                version: -1,
            },
        ] {
            let operations = RecordingOperations::new(retrieval_responses(
                Ok(canonical_memory(LATEST_ID, 1, "unused-latest")),
                Ok(canonical_memory(EXACT_ID, 1, "unused-exact")),
            ));
            let observer = operations.clone();
            let service = service(operations, None);

            let error = service
                .get_exact(input.clone())
                .await
                .expect_err("an invalid exact request should be rejected");
            let field = if input.id.is_empty() || input.id.contains('\0') {
                OperationInputField::Id
            } else {
                OperationInputField::Version
            };

            assert_eq!(error, ToolError::InvalidInput { field });
            assert!(observer.calls().is_empty());
        }
    }

    #[tokio::test]
    async fn get_latest_maps_closed_port_failures_without_a_partial_memory() {
        for (operation_error, expected_error) in [
            (
                OperationError::not_found(Operation::GetLatest),
                ToolError::NotFound,
            ),
            (
                OperationError::backend_failure(Operation::GetLatest, BackendSource::MemoryStore),
                ToolError::BackendFailure,
            ),
            (
                OperationError::invalid_backend_response(
                    Operation::GetLatest,
                    BackendResponseField::Memory,
                ),
                ToolError::InternalFailure,
            ),
        ] {
            let operations = FailingOperations::new(retrieval_failures(
                operation_error,
                OperationError::not_found(Operation::GetExact),
            ));
            let observer = operations.clone();
            let service = service(operations, None);

            let error = service
                .get_latest(GetLatestInput {
                    id: LATEST_ID.to_owned(),
                })
                .await
                .expect_err("a failed latest retrieval must not return a partial memory");

            assert_eq!(error, expected_error);
            assert_eq!(error.to_string(), error.code());
            assert_error_is_opaque(&error);
            assert_retrieval_call(observer.calls(), OperationCall::GetLatest(latest_id()));
        }
    }

    #[tokio::test]
    async fn get_exact_maps_closed_port_failures_without_a_partial_memory() {
        for (operation_error, expected_error) in [
            (
                OperationError::not_found(Operation::GetExact),
                ToolError::NotFound,
            ),
            (
                OperationError::backend_failure(Operation::GetExact, BackendSource::MemoryStore),
                ToolError::BackendFailure,
            ),
            (
                OperationError::invalid_backend_response(
                    Operation::GetExact,
                    BackendResponseField::Memory,
                ),
                ToolError::InternalFailure,
            ),
        ] {
            let operations = FailingOperations::new(retrieval_failures(
                OperationError::not_found(Operation::GetLatest),
                operation_error,
            ));
            let observer = operations.clone();
            let service = service(operations, None);

            let error = service
                .get_exact(GetExactInput {
                    id: EXACT_ID.to_owned(),
                    version: EXACT_VERSION,
                })
                .await
                .expect_err("a failed exact retrieval must not return a partial memory");

            assert_eq!(error, expected_error);
            assert_eq!(error.to_string(), error.code());
            assert_error_is_opaque(&error);
            assert_retrieval_call(
                observer.calls(),
                OperationCall::GetExact {
                    id: exact_id(),
                    version: exact_version(),
                },
            );
        }
    }

    fn service<O: MemoryOperations>(
        operations: O,
        generator: Option<RecordingQueryEmbeddingGenerator>,
    ) -> MemoryToolService<O, ProductionCreateContext, RecordingQueryEmbeddingGenerator> {
        MemoryToolService::new(operations, ProductionCreateContext, generator)
    }

    fn generator_compositions() -> [Option<RecordingQueryEmbeddingGenerator>; 2] {
        [None, Some(enabled_generator())]
    }

    fn enabled_generator() -> RecordingQueryEmbeddingGenerator {
        RecordingQueryEmbeddingGenerator::new(Ok(generated_embedding()))
    }

    fn failing_generator(failure: QueryEmbeddingError) -> RecordingQueryEmbeddingGenerator {
        RecordingQueryEmbeddingGenerator::new(Err(failure))
    }

    fn generated_embedding() -> EmbeddingVector {
        EmbeddingVector::try_from(GENERATED_VECTOR.to_vec())
            .expect("the generated test vector should be a canonical embedding")
    }

    fn assert_generator_not_invoked(generator: &Option<RecordingQueryEmbeddingGenerator>) {
        if let Some(generator) = generator {
            assert!(
                generator.calls().is_empty(),
                "the query embedding generator should not be invoked"
            );
        }
    }

    fn valid_search_input() -> MemorySearchInput {
        search_input(QUERY, LIMIT)
    }

    fn search_input(query: impl Into<String>, limit: u32) -> MemorySearchInput {
        MemorySearchInput {
            query: query.into(),
            limit,
        }
    }

    fn lexical_call(query: &str, limit: u32) -> OperationCall {
        OperationCall::SearchLexical(Bm25Search {
            query: query.to_owned(),
            limit,
        })
    }

    fn generated_vector_call(limit: u32) -> OperationCall {
        OperationCall::SearchVector(VectorSearch {
            vector: GENERATED_VECTOR.to_vec(),
            limit,
        })
    }

    fn lexical_only_calls(limit: u32) -> Vec<OperationCall> {
        vec![lexical_call(QUERY, limit)]
    }

    fn combined_calls(limit: u32) -> Vec<OperationCall> {
        vec![lexical_call(QUERY, limit), generated_vector_call(limit)]
    }

    /// Lets every runnable task make progress without advancing any gate.
    async fn settle() {
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
    }

    /// Records calls and holds BM25 until query embedding generation has also started.
    struct RendezvousOperations {
        recorder: RecordingOperations,
        rendezvous: Arc<Barrier>,
    }

    #[async_trait]
    impl MemoryOperations for RendezvousOperations {
        async fn insert_memory(&self, memory: MemoryVersionInput) -> Result<(), OperationError> {
            self.recorder.insert_memory(memory).await
        }

        async fn search_lexical(
            &self,
            query: Bm25Search,
        ) -> Result<Vec<MemorySearchResult>, OperationError> {
            let result = self.recorder.search_lexical(query).await;
            self.rendezvous.wait().await;
            result
        }

        async fn search_vector(
            &self,
            query: VectorSearch,
        ) -> Result<Vec<MemorySearchResult>, OperationError> {
            self.recorder.search_vector(query).await
        }

        async fn get_latest(&self, id: MemoryId) -> Result<MemoryVersionInput, OperationError> {
            self.recorder.get_latest(id).await
        }

        async fn get_exact(
            &self,
            id: MemoryId,
            version: MemoryVersion,
        ) -> Result<MemoryVersionInput, OperationError> {
            self.recorder.get_exact(id, version).await
        }

        async fn list_versions(
            &self,
            query: VersionPageQuery,
        ) -> Result<Vec<MemoryVersionSummary>, OperationError> {
            self.recorder.list_versions(query).await
        }
    }

    /// Records the query and holds generation until BM25 has also started.
    struct RendezvousQueryEmbeddingGenerator {
        recorder: RecordingQueryEmbeddingGenerator,
        rendezvous: Arc<Barrier>,
    }

    #[async_trait]
    impl QueryEmbeddingGenerator for RendezvousQueryEmbeddingGenerator {
        async fn generate(&self, query: &str) -> Result<EmbeddingVector, QueryEmbeddingError> {
            let result = self.recorder.generate(query).await;
            self.rendezvous.wait().await;
            result
        }
    }

    /// Records the query, then stays in flight until the test releases it.
    #[derive(Clone)]
    struct GatedQueryEmbeddingGenerator {
        recorder: RecordingQueryEmbeddingGenerator,
        started: Arc<Notify>,
        release: Arc<Notify>,
        completed: Arc<AtomicBool>,
    }

    impl GatedQueryEmbeddingGenerator {
        fn new(response: Result<EmbeddingVector, QueryEmbeddingError>) -> Self {
            Self {
                recorder: RecordingQueryEmbeddingGenerator::new(response),
                started: Arc::default(),
                release: Arc::default(),
                completed: Arc::default(),
            }
        }

        async fn wait_until_started(&self) {
            self.started.notified().await;
        }

        fn release(&self) {
            self.release.notify_one();
        }

        fn completed(&self) -> bool {
            self.completed.load(Ordering::SeqCst)
        }

        fn calls(&self) -> Vec<String> {
            self.recorder.calls()
        }
    }

    #[async_trait]
    impl QueryEmbeddingGenerator for GatedQueryEmbeddingGenerator {
        async fn generate(&self, query: &str) -> Result<EmbeddingVector, QueryEmbeddingError> {
            let result = self.recorder.generate(query).await;
            self.started.notify_one();
            self.release.notified().await;
            self.completed.store(true, Ordering::SeqCst);
            result
        }
    }

    fn search_responses(
        search_lexical: Result<Vec<MemorySearchResult>, OperationError>,
        search_vector: Result<Vec<MemorySearchResult>, OperationError>,
    ) -> MemoryOperationResponses {
        MemoryOperationResponses {
            insert_memory: Err(OperationError::backend_failure(
                Operation::InsertMemory,
                BackendSource::MemoryStore,
            )),
            search_lexical,
            search_vector,
            get_latest: Err(OperationError::not_found(Operation::GetLatest)),
            get_exact: Err(OperationError::not_found(Operation::GetExact)),
            list_versions: Err(OperationError::backend_failure(
                Operation::ListVersions,
                BackendSource::MemoryStore,
            )),
        }
    }

    fn search_failures(
        search_lexical: OperationError,
        search_vector: OperationError,
    ) -> MemoryOperationFailures {
        MemoryOperationFailures {
            insert_memory: OperationError::backend_failure(
                Operation::InsertMemory,
                BackendSource::MemoryStore,
            ),
            search_lexical,
            search_vector,
            get_latest: OperationError::not_found(Operation::GetLatest),
            get_exact: OperationError::not_found(Operation::GetExact),
            list_versions: OperationError::backend_failure(
                Operation::ListVersions,
                BackendSource::MemoryStore,
            ),
        }
    }

    fn version_responses(
        list_versions: Result<Vec<MemoryVersionSummary>, OperationError>,
    ) -> MemoryOperationResponses {
        MemoryOperationResponses {
            insert_memory: Err(OperationError::backend_failure(
                Operation::InsertMemory,
                BackendSource::MemoryStore,
            )),
            search_lexical: Err(OperationError::backend_failure(
                Operation::SearchLexical,
                BackendSource::MemoryStore,
            )),
            search_vector: Err(OperationError::backend_failure(
                Operation::SearchVector,
                BackendSource::MemoryStore,
            )),
            get_latest: Err(OperationError::not_found(Operation::GetLatest)),
            get_exact: Err(OperationError::not_found(Operation::GetExact)),
            list_versions,
        }
    }

    fn retrieval_responses(
        get_latest: Result<MemoryVersionInput, OperationError>,
        get_exact: Result<MemoryVersionInput, OperationError>,
    ) -> MemoryOperationResponses {
        MemoryOperationResponses {
            insert_memory: Err(OperationError::backend_failure(
                Operation::InsertMemory,
                BackendSource::MemoryStore,
            )),
            search_lexical: Err(OperationError::backend_failure(
                Operation::SearchLexical,
                BackendSource::MemoryStore,
            )),
            search_vector: Err(OperationError::backend_failure(
                Operation::SearchVector,
                BackendSource::MemoryStore,
            )),
            get_latest,
            get_exact,
            list_versions: Err(OperationError::backend_failure(
                Operation::ListVersions,
                BackendSource::MemoryStore,
            )),
        }
    }

    fn version_failures(list_versions: OperationError) -> MemoryOperationFailures {
        MemoryOperationFailures {
            insert_memory: OperationError::backend_failure(
                Operation::InsertMemory,
                BackendSource::MemoryStore,
            ),
            search_lexical: OperationError::backend_failure(
                Operation::SearchLexical,
                BackendSource::MemoryStore,
            ),
            search_vector: OperationError::backend_failure(
                Operation::SearchVector,
                BackendSource::MemoryStore,
            ),
            get_latest: OperationError::not_found(Operation::GetLatest),
            get_exact: OperationError::not_found(Operation::GetExact),
            list_versions,
        }
    }

    fn retrieval_failures(
        get_latest: OperationError,
        get_exact: OperationError,
    ) -> MemoryOperationFailures {
        MemoryOperationFailures {
            insert_memory: OperationError::backend_failure(
                Operation::InsertMemory,
                BackendSource::MemoryStore,
            ),
            search_lexical: OperationError::backend_failure(
                Operation::SearchLexical,
                BackendSource::MemoryStore,
            ),
            search_vector: OperationError::backend_failure(
                Operation::SearchVector,
                BackendSource::MemoryStore,
            ),
            get_latest,
            get_exact,
            list_versions: OperationError::backend_failure(
                Operation::ListVersions,
                BackendSource::MemoryStore,
            ),
        }
    }

    fn search_result(id: &str, relevance: f64) -> MemorySearchResult {
        search_result_from_memory(canonical_memory(id, 1, "candidate"), relevance)
    }

    fn search_result_from_memory(memory: MemoryVersionInput, relevance: f64) -> MemorySearchResult {
        MemorySearchResult::try_new(
            memory.try_into().expect("test candidates should be valid"),
            relevance,
        )
        .expect("test relevance should be valid")
    }

    fn canonical_memory(id: &str, version: i64, marker: &str) -> MemoryVersionInput {
        MemoryVersionInput {
            id: id.to_owned(),
            version,
            memory_type: "unclassified".to_owned(),
            title: format!("{marker} title"),
            content: format!("{marker} content"),
            created_at: timestamp(),
            updated_at: timestamp(),
            concepts: vec![format!("{marker} concept")],
            files: vec![format!("{marker} file")],
            session_ids: vec![format!("{marker} session")],
            source_observation_ids: vec![format!("{marker} observation")],
        }
    }

    fn version_summary(version: i64) -> MemoryVersionSummary {
        MemoryVersionSummary {
            version: MemoryVersion::try_from(version)
                .expect("test version summaries should use positive versions"),
            updated_at: timestamp(),
        }
    }

    fn version_list_input(
        id: impl Into<String>,
        offset: Option<u64>,
        limit: Option<u32>,
    ) -> ListVersionsInput {
        ListVersionsInput {
            id: id.into(),
            offset,
            limit,
        }
    }

    fn version_list_id() -> MemoryId {
        MemoryId::try_from(VERSION_LIST_ID.to_owned())
            .expect("test version-list IDs should be valid")
    }

    fn latest_id() -> MemoryId {
        MemoryId::try_from(LATEST_ID.to_owned()).expect("test latest IDs should be valid")
    }

    fn exact_id() -> MemoryId {
        MemoryId::try_from(EXACT_ID.to_owned()).expect("test exact IDs should be valid")
    }

    fn exact_version() -> MemoryVersion {
        MemoryVersion::try_from(EXACT_VERSION)
            .expect("test exact versions should be positive canonical versions")
    }

    fn timestamp() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-09-20T12:34:56Z")
            .expect("test timestamp should parse")
            .with_timezone(&Utc)
    }

    fn assert_version_call(calls: Vec<OperationCall>, offset: u64, limit: u32) {
        assert_eq!(
            calls,
            vec![OperationCall::ListVersions(VersionPageQuery {
                id: version_list_id(),
                offset,
                limit,
            })]
        );
    }

    fn assert_retrieval_call(calls: Vec<OperationCall>, expected_call: OperationCall) {
        assert_eq!(calls, vec![expected_call]);
    }

    fn assert_serialized_memory_is_complete_and_private(
        memory: &MemoryDto,
        expected: &MemoryVersionInput,
    ) {
        let serialized =
            serde_json::to_value(memory).expect("memory projections should serialize successfully");
        let object = serialized
            .as_object()
            .expect("memory projections should serialize as objects");

        assert_eq!(object.len(), 11);
        assert_eq!(serialized["id"], expected.id);
        assert_eq!(serialized["version"], expected.version);
        assert_eq!(serialized["type"], expected.memory_type);
        assert_eq!(serialized["title"], expected.title);
        assert_eq!(serialized["content"], expected.content);
        assert_eq!(
            serialized["created_at"],
            serde_json::to_value(expected.created_at)
                .expect("canonical timestamps should serialize successfully")
        );
        assert_eq!(
            serialized["updated_at"],
            serde_json::to_value(expected.updated_at)
                .expect("canonical timestamps should serialize successfully")
        );
        assert_eq!(
            serialized["concepts"],
            serde_json::to_value(&expected.concepts)
                .expect("canonical concepts should serialize successfully")
        );
        assert_eq!(
            serialized["files"],
            serde_json::to_value(&expected.files)
                .expect("canonical files should serialize successfully")
        );
        assert_eq!(
            serialized["session_ids"],
            serde_json::to_value(&expected.session_ids)
                .expect("canonical session IDs should serialize successfully")
        );
        assert_eq!(
            serialized["source_observation_ids"],
            serde_json::to_value(&expected.source_observation_ids)
                .expect("canonical source observation IDs should serialize successfully")
        );
        assert!(!object.contains_key("embedding"));
        assert!(!object.contains_key("relevance"));
    }

    fn assert_error_is_opaque(error: &ToolError) {
        for protected_value in [
            QUERY.trim(),
            VERSION_LIST_ID,
            LATEST_ID.trim(),
            EXACT_ID.trim(),
            GENERATED_VECTOR_TEXT[0],
            GENERATED_VECTOR_TEXT[1],
            "37",
            "memory_store",
        ] {
            assert!(
                !error.to_string().contains(protected_value),
                "Display error leaked protected value: {error}"
            );
            assert!(
                !format!("{error:?}").contains(protected_value),
                "Debug error leaked protected value: {error:?}"
            );
        }
    }
}
