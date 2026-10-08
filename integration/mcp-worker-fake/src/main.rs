use std::{
    collections::BTreeSet,
    env,
    error::Error,
    fmt, fs, io,
    path::{Path, PathBuf},
    pin::Pin,
    process::{ExitStatus, Stdio},
    task::{Context, Poll},
    time::Duration,
};

use chrono::{DateTime, Timelike, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    sync::oneshot,
    task::JoinHandle,
    time::{Instant, timeout, timeout_at},
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
use uuid::Uuid;

const DEFAULT_TIMEOUT_SECONDS: u64 = 5;
const DEFAULT_MAX_LINE_BYTES: usize = 65_536;
const DEFAULT_STDERR_CAP_BYTES: usize = 65_536;
const SMOKE_DATABASE_TARGET: &str = "mcp-worker-fake-smoke";
const UNREACHABLE_III_URL: &str = "ws://127.0.0.1:1";
const PROTOCOL_VERSION: &str = "2026-07-28";
const SERVER_NAME: &str = "total-recall-mcp";
const WORKER_VERSION: &str = "0.1.0";
const INVALID_STARTUP_STDERR: &str = "worker startup configuration failed: TOTAL_RECALL_MCP_MAX_LINE_BYTES must be a positive usize\n";
const PARTIAL_EMBEDDING_STARTUP_STDERR: &str = "worker startup configuration failed: TOTAL_RECALL_EMBEDDING_PROVIDER and TOTAL_RECALL_EMBEDDING_MODEL must both be non-blank or both be blank\n";
const STARTUP_EMBEDDING_PROVIDER_SENTINEL: &str = "startup-embedding-provider-private-sentinel";
const STARTUP_EMBEDDING_MODEL_SENTINEL: &str = "startup-embedding-model-private-sentinel";
const BLANK_EMBEDDING_IDENTIFIER: &str = " \t ";
const SMALL_WORKER_MAX_LINE_BYTES: usize = 512;
const MAX_RAW_TEST_LINE_BYTES: usize = DEFAULT_MAX_LINE_BYTES;
const FLOOD_REQUEST_COUNT: usize = 512;
const FLOOD_REQUEST_ID_PADDING_BYTES: usize = 2_048;
const FLOOD_WORKER_MAX_LINE_BYTES: usize = 4_096;
const MAX_FLOOD_REQUESTS: usize = FLOOD_REQUEST_COUNT;
const FLOOD_PRESSURE_NOT_OBSERVED: &str =
    "bounded flood did not observe stdin write pressure while stdout was undrained";
const UNEXPECTED_SHUTDOWN_MESSAGE: &str =
    "worker sent an unexpected fake database engine message during shutdown";
const PRIVATE_OVERSIZE_SENTINEL: &str = "oversize-private-frame-sentinel";
const SAVE_TITLE_SENTINEL: &str = "save-title-private-sentinel";
const SAVE_CONTENT_SENTINEL: &str = "save-content-private-sentinel";
const SAVE_SESSION_SENTINEL: &str = "save-session-private-sentinel";
const ENGINE_ERROR_CODE_SENTINEL: &str = "engine-error-code-private-sentinel";
const ENGINE_ERROR_MESSAGE_SENTINEL: &str = "engine-error-message-private-sentinel";
const ENGINE_ERROR_STACKTRACE_SENTINEL: &str = "engine-error-stacktrace-private-sentinel";
const SEARCH_QUERY_SENTINEL: &str = "search-query-private-sentinel";
const SEARCH_CALLER_VECTOR: [f64; 2] = [0.3046875, -0.6953125];
const SEARCH_UNKNOWN_ARGUMENT_SENTINEL: &str = "search-unknown-argument-private-sentinel";
const SEARCH_LIMIT: u32 = 5;
const SEARCH_RESULT_SENTINEL: &str = "search-result-private-sentinel";
const SEARCH_TIE_ID: &str = "search-id-tie";
const SEARCH_TIE_VERSION: i64 = 2;
const SEARCH_LEXICAL_TIE_MARKER: &str = "lexical-tie-v2-first";
const SEARCH_LEXICAL_TIE_DUPLICATE_MARKER: &str = "lexical-tie-v2-duplicate";
const SEARCH_VECTOR_TIE_MARKER: &str = "vector-tie-v2";
const SEARCH_ENGINE_ERROR_CODE_SENTINEL: &str = "search-engine-error-code-private-sentinel";
const SEARCH_ENGINE_ERROR_MESSAGE_SENTINEL: &str = "search-engine-error-message-private-sentinel";
const SEARCH_ENGINE_ERROR_STACKTRACE_SENTINEL: &str =
    "search-engine-error-stacktrace-private-sentinel";
const SEARCH_COMBINED_QUERY: &str =
    "  \tsearch-query-private-sentinel na\u{ef}ve cafe\u{301} \u{2615}\r\n ";
const SEARCH_EMBEDDING_PROVIDER_SENTINEL: &str = "search-embedding-provider-private-sentinel";
const SEARCH_EMBEDDING_MODEL_SENTINEL: &str = "search-embedding-model-private-sentinel";
const SEARCH_NAMESPACE: &str = "mcp-worker-fake-search-namespace";
const SEARCH_GENERATED_VECTOR: [f64; 3] = [0.828125, -0.4140625, 1.2109375];
const SEARCH_GENERATED_VECTOR_PARAMETER: &str = "[0.828125,-0.4140625,1.2109375]";
const SEARCH_LEXICAL_FALLBACK_LIMIT: u32 = 6;
const SEARCH_FALLBACK_TIE_ID: &str = "search-id-k";
const SEARCH_FALLBACK_TIE_VERSION: i64 = 3;
const SEARCH_FALLBACK_TIE_MARKER: &str = "fallback-lexical-k-v3-first";
const SEARCH_FALLBACK_TIE_DUPLICATE_MARKER: &str = "fallback-lexical-k-v3-duplicate";
const SEARCH_ROUTER_ERROR_CODE_SENTINEL: &str = "search-router-error-code-private-sentinel";
const SEARCH_ROUTER_ERROR_MESSAGE_SENTINEL: &str = "search-router-error-message-private-sentinel";
const SEARCH_ROUTER_ERROR_STACKTRACE_SENTINEL: &str =
    "search-router-error-stacktrace-private-sentinel";
const SEARCH_ROUTER_CREDENTIAL_SENTINEL: &str = "search-router-credential-private-sentinel";
const SEARCH_ROUTER_RESOLVED_MODEL_SENTINEL: &str = "search-router-resolved-model-private-sentinel";
const SEARCH_ROUTER_INVALID_VECTOR: [f64; 3] = [0.890625, -0.4453125, 1.3359375];
const EMBEDDING_PROVIDER_ENV: &str = "TOTAL_RECALL_EMBEDDING_PROVIDER";
const EMBEDDING_MODEL_ENV: &str = "TOTAL_RECALL_EMBEDDING_MODEL";
const EMBEDDING_TIMEOUT_ENV: &str = "TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS";
const VERSION_LIST_ID_SENTINEL: &str = "version-list-id-private-sentinel";
const VERSION_LIST_UPDATED_AT: &str = "2026-09-21T00:00:00Z";
const VERSION_LIST_DEFAULT_LIMIT: u32 = 50;
const VERSION_LIST_CONTINUED_OFFSET: u64 = 17;
const VERSION_LIST_CONTINUED_LIMIT: u32 = 2;
const VERSION_LIST_EMPTY_OFFSET: u64 = 9;
const VERSION_LIST_EMPTY_LIMIT: u32 = 7;
const VERSION_LIST_ENGINE_ERROR_CODE_SENTINEL: &str =
    "version-list-engine-error-code-private-sentinel";
const VERSION_LIST_ENGINE_ERROR_MESSAGE_SENTINEL: &str =
    "version-list-engine-error-message-private-sentinel";
const VERSION_LIST_ENGINE_ERROR_STACKTRACE_SENTINEL: &str =
    "version-list-engine-error-stacktrace-private-sentinel";
const RETRIEVAL_ID_SENTINEL: &str = "retrieval-id-private-sentinel";
const RETRIEVAL_EXACT_VERSION: i64 = 7;
const RETRIEVAL_LATEST_VERSION: i64 = 19;
const RETRIEVAL_RESULT_SENTINEL: &str = "retrieval-result-private-sentinel";
const RETRIEVAL_EXACT_CREATED_AT: &str = "2026-09-21T01:02:03Z";
const RETRIEVAL_EXACT_UPDATED_AT: &str = "2026-09-21T01:02:04Z";
const RETRIEVAL_LATEST_CREATED_AT: &str = "2026-09-21T03:04:05Z";
const RETRIEVAL_LATEST_UPDATED_AT: &str = "2026-09-21T03:04:06Z";
const RETRIEVAL_ENGINE_ERROR_CODE_SENTINEL: &str = "retrieval-engine-error-code-private-sentinel";
const RETRIEVAL_ENGINE_ERROR_MESSAGE_SENTINEL: &str =
    "retrieval-engine-error-message-private-sentinel";
const RETRIEVAL_ENGINE_ERROR_STACKTRACE_SENTINEL: &str =
    "retrieval-engine-error-stacktrace-private-sentinel";
const DATABASE_EXECUTE_FUNCTION_ID: &str = "database::execute";
const ROUTER_EMBED_FUNCTION_ID: &str = "router::embed";
const WORKER_REGISTER_FUNCTION_ID: &str = "engine::workers::register";
const INSERT_MEMORY_SQL: &str = r#"INSERT INTO public.memories (
    id,
    version,
    type,
    title,
    content,
    created_at,
    updated_at,
    concepts,
    files,
    session_ids,
    source_observation_ids
)
VALUES (
    $1::text,
    $2::text::bigint,
    $3::text,
    $4::text,
    $5::text,
    $6::text::timestamptz,
    $7::text::timestamptz,
    ARRAY(
        SELECT value
        FROM jsonb_array_elements_text($8::jsonb) WITH ORDINALITY AS concepts(value, ordinality)
        ORDER BY ordinality
    ),
    ARRAY(
        SELECT value
        FROM jsonb_array_elements_text($9::jsonb) WITH ORDINALITY AS files(value, ordinality)
        ORDER BY ordinality
    ),
    ARRAY(
        SELECT value
        FROM jsonb_array_elements_text($10::jsonb) WITH ORDINALITY AS session_ids(value, ordinality)
        ORDER BY ordinality
    ),
    ARRAY(
        SELECT value
        FROM jsonb_array_elements_text($11::jsonb) WITH ORDINALITY AS source_observation_ids(value, ordinality)
        ORDER BY ordinality
    )
)
ON CONFLICT (id, version) DO NOTHING
RETURNING id, version"#;
const SEARCH_BM25_SQL: &str = r#"WITH scored_heads AS MATERIALIZED (
    SELECT
        head.id,
        head.version,
        -(head.search_document <@> to_bm25query(
            $1::text,
            'public.memory_search_heads_search_document_bm25_idx'
        )) AS relevance
    FROM public.memory_search_heads AS head
)
SELECT
    memory.id,
    memory.version::text AS version,
    memory.type AS memory_type,
    memory.title,
    memory.content,
    memory.created_at,
    memory.updated_at,
    to_json(memory.concepts) AS concepts,
    to_json(memory.files) AS files,
    to_json(memory.session_ids) AS session_ids,
    to_json(memory.source_observation_ids) AS source_observation_ids,
    scored_heads.relevance
FROM scored_heads
JOIN public.memories AS memory
    ON memory.id = scored_heads.id
   AND memory.version = scored_heads.version
WHERE scored_heads.relevance > 0::double precision
ORDER BY scored_heads.relevance DESC,
         scored_heads.id COLLATE "C" ASC,
         scored_heads.version ASC
LIMIT $2::text::bigint"#;
const SEARCH_VECTOR_SQL: &str = r#"WITH query_input AS MATERIALIZED (
    SELECT $1::text::vector AS embedding
),
query_vector AS MATERIALIZED (
    SELECT
        vector_dims(query_input.embedding) AS dimensions,
        vector_norm(query_input.embedding) AS norm,
        l2_normalize(query_input.embedding) AS normalized_embedding
    FROM query_input
),
scored_heads AS MATERIALIZED (
    SELECT
        head.id,
        head.version,
        CASE
            WHEN vector_dims(embedding.embedding) = query_vector.dimensions
             AND vector_norm(embedding.embedding) > 0::double precision
             AND query_vector.norm > 0::double precision
            THEN 1::double precision - (
                l2_normalize(embedding.embedding) <=> query_vector.normalized_embedding
            )
        END AS relevance
    FROM public.memory_search_heads AS head
    JOIN public.memory_embeddings AS embedding
      ON embedding.id = head.id AND embedding.version = head.version
    CROSS JOIN query_vector
)
SELECT
    memory.id,
    memory.version::text AS version,
    memory.type AS memory_type,
    memory.title,
    memory.content,
    memory.created_at,
    memory.updated_at,
    to_json(memory.concepts) AS concepts,
    to_json(memory.files) AS files,
    to_json(memory.session_ids) AS session_ids,
    to_json(memory.source_observation_ids) AS source_observation_ids,
    scored_heads.relevance
FROM scored_heads
JOIN public.memories AS memory
    ON memory.id = scored_heads.id
   AND memory.version = scored_heads.version
WHERE relevance IS NOT NULL
ORDER BY scored_heads.relevance DESC,
         scored_heads.id COLLATE "C" ASC,
         scored_heads.version ASC
LIMIT $2::text::bigint"#;
const VERSION_LIST_SQL: &str = r#"SELECT
    memory.version::text AS version,
    memory.updated_at
FROM public.memories AS memory
WHERE memory.id = $1::text
ORDER BY memory.version DESC
OFFSET $2::text::bigint
LIMIT $3::text::bigint"#;
const EXACT_MEMORY_SQL: &str = r#"SELECT
    memory.id,
    memory.version::text AS version,
    memory.type AS memory_type,
    memory.title,
    memory.content,
    memory.created_at,
    memory.updated_at,
    to_json(memory.concepts) AS concepts,
    to_json(memory.files) AS files,
    to_json(memory.session_ids) AS session_ids,
    to_json(memory.source_observation_ids) AS source_observation_ids
FROM public.memories AS memory
WHERE memory.id = $1::text
  AND memory.version = $2::text::bigint"#;
const LATEST_MEMORY_SQL: &str = r#"SELECT
    memory.id,
    memory.version::text AS version,
    memory.type AS memory_type,
    memory.title,
    memory.content,
    memory.created_at,
    memory.updated_at,
    to_json(memory.concepts) AS concepts,
    to_json(memory.files) AS files,
    to_json(memory.session_ids) AS session_ids,
    to_json(memory.source_observation_ids) AS source_observation_ids
FROM public.memory_search_heads AS head
JOIN public.memories AS memory
    ON memory.id = head.id
   AND memory.version = head.version
WHERE head.id = $1::text"#;

type FakeResult<T = ()> = Result<T, ProcessClientError>;

#[derive(Clone, Copy, Debug)]
struct ProcessLimits {
    request_timeout: Duration,
    max_line_bytes: usize,
    stderr_cap_bytes: usize,
}

impl Default for ProcessLimits {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(DEFAULT_TIMEOUT_SECONDS),
            max_line_bytes: DEFAULT_MAX_LINE_BYTES,
            stderr_cap_bytes: DEFAULT_STDERR_CAP_BYTES,
        }
    }
}

impl ProcessLimits {
    fn validate(self) -> FakeResult {
        if self.request_timeout.is_zero() || self.max_line_bytes == 0 || self.stderr_cap_bytes == 0
        {
            return Err(ProcessClientError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Arguments {
    worker: PathBuf,
    limits: ProcessLimits,
}

struct WorkerInvocation {
    executable: PathBuf,
}

#[derive(Clone, Copy)]
struct WorkerBounds {
    max_line_bytes: usize,
    channel_capacity: usize,
    max_in_flight: usize,
}

const DEFAULT_WORKER_BOUNDS: WorkerBounds = WorkerBounds {
    max_line_bytes: DEFAULT_MAX_LINE_BYTES,
    channel_capacity: 1,
    max_in_flight: 1,
};

impl WorkerInvocation {
    fn command(&self, bounds: WorkerBounds) -> Command {
        self.command_with_iii_url(bounds, UNREACHABLE_III_URL)
    }

    fn command_with_iii_url(&self, bounds: WorkerBounds, iii_url: &str) -> Command {
        let mut command = Command::new(&self.executable);
        command
            .env("TOTAL_RECALL_MEMORY_DATABASE", SMOKE_DATABASE_TARGET)
            .env(
                "TOTAL_RECALL_MCP_MAX_LINE_BYTES",
                bounds.max_line_bytes.to_string(),
            )
            .env(
                "TOTAL_RECALL_MCP_CHANNEL_CAPACITY",
                bounds.channel_capacity.to_string(),
            )
            .env(
                "TOTAL_RECALL_MCP_MAX_IN_FLIGHT",
                bounds.max_in_flight.to_string(),
            )
            .env("III_URL", iii_url)
            .env_remove("III_WORKER_NAME")
            .env_remove("III_NAMESPACE")
            .env_remove(EMBEDDING_PROVIDER_ENV)
            .env_remove(EMBEDDING_MODEL_ENV)
            .env_remove(EMBEDDING_TIMEOUT_ENV);
        command
    }

    /// Leaves query embedding disabled while matching the enabled launch in every other value.
    fn disabled_embedding_command(&self, iii_url: &str) -> Command {
        let mut command = self.command_with_iii_url(DEFAULT_WORKER_BOUNDS, iii_url);
        command.env("III_NAMESPACE", SEARCH_NAMESPACE);
        command
    }

    fn enabled_embedding_command(&self, iii_url: &str) -> Command {
        let mut command = self.disabled_embedding_command(iii_url);
        command
            .env(EMBEDDING_PROVIDER_ENV, SEARCH_EMBEDDING_PROVIDER_SENTINEL)
            .env(EMBEDDING_MODEL_ENV, SEARCH_EMBEDDING_MODEL_SENTINEL);
        command
    }

    fn invalid_configuration_command(&self) -> Command {
        let mut command = self.command(DEFAULT_WORKER_BOUNDS);
        command.env("TOTAL_RECALL_MCP_MAX_LINE_BYTES", "not-a-number");
        command
    }

    /// Sets each present provider or model value; an absent value stays removed.
    fn embedding_identity_command(&self, provider: Option<&str>, model: Option<&str>) -> Command {
        let mut command = self.command(DEFAULT_WORKER_BOUNDS);
        if let Some(provider) = provider {
            command.env(EMBEDDING_PROVIDER_ENV, provider);
        }
        if let Some(model) = model {
            command.env(EMBEDDING_MODEL_ENV, model);
        }
        command
    }
}

#[derive(Clone, Copy)]
enum SaveEngineMode {
    InsertSuccess,
    InsertConflict,
    BackendFailure,
    NoDatabaseInvocation,
}

#[derive(Clone, Copy)]
enum SearchEngineMode {
    CombinedSuccess,
    CombinedEmpty,
    DisabledEmbeddingFallback,
    DisabledEmbeddingEmptyFallback,
    RouterFailureFallback,
    InvalidRouterResponseFallback,
    DisabledEmbeddingLexicalFailure,
    LexicalBackendFailure,
    InvalidVectorSearchResponse,
    NoDatabaseInvocation,
}

impl SearchEngineMode {
    const fn query_embedding_enabled(self) -> bool {
        matches!(
            self,
            Self::CombinedSuccess
                | Self::CombinedEmpty
                | Self::RouterFailureFallback
                | Self::InvalidRouterResponseFallback
                | Self::LexicalBackendFailure
                | Self::InvalidVectorSearchResponse
                | Self::NoDatabaseInvocation
        )
    }

    const fn unexpected_router_invocation(self) -> &'static str {
        match self {
            Self::DisabledEmbeddingFallback
            | Self::DisabledEmbeddingEmptyFallback
            | Self::DisabledEmbeddingLexicalFailure => {
                "disabled query embedding invoked router::embed"
            }
            Self::RouterFailureFallback => {
                "lexical fallback retried router::embed after a remote router failure"
            }
            Self::InvalidRouterResponseFallback => {
                "lexical fallback retried router::embed after an invalid router response"
            }
            Self::LexicalBackendFailure | Self::InvalidVectorSearchResponse => {
                "failed memory search retried router::embed"
            }
            Self::CombinedSuccess | Self::CombinedEmpty | Self::NoDatabaseInvocation => {
                "memory search sent an unexpected router::embed call"
            }
        }
    }

    const fn unexpected_vector_search(self) -> &'static str {
        match self {
            Self::DisabledEmbeddingFallback
            | Self::DisabledEmbeddingEmptyFallback
            | Self::DisabledEmbeddingLexicalFailure => {
                "disabled query embedding issued vector search"
            }
            Self::RouterFailureFallback => {
                "lexical fallback issued vector search after a remote router failure"
            }
            Self::InvalidRouterResponseFallback => {
                "lexical fallback issued vector search after an invalid router response"
            }
            Self::LexicalBackendFailure | Self::InvalidVectorSearchResponse => {
                "failed memory search retried its vector search"
            }
            Self::CombinedSuccess | Self::CombinedEmpty | Self::NoDatabaseInvocation => {
                "memory search sent an unexpected vector search"
            }
        }
    }

    /// Names the partial-result regression for a search whose BM25 or vector search failed.
    const fn failed_search_returned_results(self) -> &'static str {
        match self {
            Self::DisabledEmbeddingLexicalFailure => {
                "disabled query embedding returned results after BM25 search failed"
            }
            Self::LexicalBackendFailure => {
                "memory search returned vector-search results after BM25 search failed"
            }
            Self::InvalidVectorSearchResponse => {
                "memory search returned BM25 results after vector search failed"
            }
            Self::CombinedSuccess
            | Self::CombinedEmpty
            | Self::DisabledEmbeddingFallback
            | Self::DisabledEmbeddingEmptyFallback
            | Self::RouterFailureFallback
            | Self::InvalidRouterResponseFallback
            | Self::NoDatabaseInvocation => {
                "memory search returned results instead of a tool error"
            }
        }
    }
}

#[derive(Clone, Copy)]
enum VersionListEngineMode {
    DefaultPage,
    ContinuedPage,
    EmptyPage,
    BackendFailure,
    NoDatabaseInvocation,
}

#[derive(Clone, Copy)]
enum RetrievalEngineMode {
    ExactSuccess,
    LatestSuccess,
    ExactNotFound,
    LatestNotFound,
    ExactMalformed,
    LatestMalformed,
    ExactBackendFailure,
    LatestBackendFailure,
    NoDatabaseInvocation,
}

#[derive(Clone, Copy)]
enum RetrievalOperation {
    Exact,
    Latest,
}

impl RetrievalEngineMode {
    const fn operation(self) -> Option<RetrievalOperation> {
        match self {
            Self::ExactSuccess
            | Self::ExactNotFound
            | Self::ExactMalformed
            | Self::ExactBackendFailure => Some(RetrievalOperation::Exact),
            Self::LatestSuccess
            | Self::LatestNotFound
            | Self::LatestMalformed
            | Self::LatestBackendFailure => Some(RetrievalOperation::Latest),
            Self::NoDatabaseInvocation => None,
        }
    }
}

impl RetrievalOperation {
    const fn tool_name(self) -> &'static str {
        match self {
            Self::Exact => "memory_get_exact",
            Self::Latest => "memory_get_latest",
        }
    }

    const fn sql(self) -> &'static str {
        match self {
            Self::Exact => EXACT_MEMORY_SQL,
            Self::Latest => LATEST_MEMORY_SQL,
        }
    }
}

#[derive(Clone, Copy)]
enum FakeDatabaseEngineMode {
    Save(SaveEngineMode),
    Search(SearchEngineMode),
    VersionList(VersionListEngineMode),
    Retrieval(RetrievalEngineMode),
}

#[derive(Clone, Copy)]
enum SearchOperation {
    Lexical,
    Vector,
}

struct CapturedDatabaseInvocation {
    database: String,
    sql: String,
    params: Vec<Value>,
}

struct CapturedRouterInvocation {
    data: Value,
}

struct CapturedCombinedSearchInvocations {
    router: CapturedRouterInvocation,
    lexical: CapturedDatabaseInvocation,
    vector: CapturedDatabaseInvocation,
}

struct CapturedLexicalFallbackInvocations {
    router: Option<CapturedRouterInvocation>,
    lexical: CapturedDatabaseInvocation,
}

struct CapturedFailedSearchInvocations {
    lexical: CapturedDatabaseInvocation,
    /// The router call and its chained vector search; absent when query embedding is disabled.
    semantic: Option<(CapturedRouterInvocation, CapturedDatabaseInvocation)>,
}

enum ConcurrentSearchInvocation {
    Router(String, CapturedRouterInvocation),
    Lexical(String, CapturedDatabaseInvocation),
}

struct ConcurrentSearchStart {
    router_invocation_id: String,
    router: CapturedRouterInvocation,
    lexical_invocation_id: String,
    lexical: CapturedDatabaseInvocation,
}

enum EngineCompletion {
    DatabaseInvocation(CapturedDatabaseInvocation),
    CombinedSearchInvocations(CapturedCombinedSearchInvocations),
    LexicalFallbackInvocations(CapturedLexicalFallbackInvocations),
    FailedSearchInvocations(CapturedFailedSearchInvocations),
    VersionListInvocation(CapturedDatabaseInvocation),
    RetrievalInvocation(CapturedDatabaseInvocation),
    NoDatabaseInvocation,
}

struct FakeDatabaseEngine {
    url: String,
    registered: Option<oneshot::Receiver<()>>,
    no_database_invocation: Option<oneshot::Receiver<()>>,
    completion: JoinHandle<FakeResult<EngineCompletion>>,
    timeout: Duration,
}

impl FakeDatabaseEngine {
    async fn start(mode: FakeDatabaseEngineMode, timeout_duration: Duration) -> FakeResult<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.map_err(|_| {
            ProcessClientError::ProtocolScenario("fake database engine could not bind")
        })?;
        let address = listener.local_addr().map_err(|_| {
            ProcessClientError::ProtocolScenario("fake database engine could not get its address")
        })?;
        let (registered_sender, registered) = oneshot::channel();
        let (no_database_sender, no_database_invocation): (
            Option<oneshot::Sender<()>>,
            Option<oneshot::Receiver<()>>,
        ) = if matches!(
            mode,
            FakeDatabaseEngineMode::Save(SaveEngineMode::NoDatabaseInvocation)
                | FakeDatabaseEngineMode::Search(SearchEngineMode::NoDatabaseInvocation)
                | FakeDatabaseEngineMode::VersionList(VersionListEngineMode::NoDatabaseInvocation)
                | FakeDatabaseEngineMode::Retrieval(RetrievalEngineMode::NoDatabaseInvocation,)
        ) {
            let (sender, receiver) = oneshot::channel();
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };

        Ok(Self {
            url: format!("ws://{address}"),
            registered: Some(registered),
            no_database_invocation,
            completion: tokio::spawn(run_fake_database_engine(
                listener,
                mode,
                registered_sender,
                no_database_sender,
                timeout_duration,
            )),
            timeout: timeout_duration,
        })
    }

    fn url(&self) -> &str {
        &self.url
    }

    async fn wait_until_registered(&mut self) -> FakeResult {
        let registered = self
            .registered
            .take()
            .ok_or(ProcessClientError::ProtocolScenario(
                "fake database engine registration was observed more than once",
            ))?;
        match timeout(self.timeout, registered).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(ProcessClientError::ProtocolScenario(
                "fake database engine closed before worker registration",
            )),
            Err(_) => Err(ProcessClientError::ProtocolScenario(
                "worker registration exceeded the fake database engine deadline",
            )),
        }
    }

    async fn wait_until_no_database_invocation(&mut self) -> FakeResult {
        let observed =
            self.no_database_invocation
                .take()
                .ok_or(ProcessClientError::ProtocolScenario(
                    "fake database engine does not expect an absent database invocation",
                ))?;
        match timeout(self.timeout + Duration::from_secs(1), observed).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(ProcessClientError::ProtocolScenario(
                "fake database engine closed before observing no database invocation",
            )),
            Err(_) => Err(ProcessClientError::ProtocolScenario(
                "fake database engine did not observe no database invocation before its deadline",
            )),
        }
    }

    async fn finish(mut self) -> FakeResult<EngineCompletion> {
        match timeout(self.timeout, &mut self.completion).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ProcessClientError::ProtocolScenario(
                "fake database engine task failed",
            )),
            Err(_) => {
                self.completion.abort();
                let _ = (&mut self.completion).await;
                Err(ProcessClientError::ProtocolScenario(
                    "fake database engine cleanup exceeded its deadline",
                ))
            }
        }
    }

    async fn abort_and_wait(mut self) {
        self.completion.abort();
        let _ = (&mut self.completion).await;
    }
}

impl Drop for FakeDatabaseEngine {
    fn drop(&mut self) {
        self.completion.abort();
    }
}

struct CapturedStderr {
    bytes: Vec<u8>,
    truncated: bool,
}

impl CapturedStderr {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            truncated: false,
        }
    }

    fn append(&mut self, bytes: &[u8], capacity: usize) {
        let remaining = capacity.saturating_sub(self.bytes.len());
        let accepted = remaining.min(bytes.len());
        self.bytes.extend_from_slice(&bytes[..accepted]);
        self.truncated |= accepted < bytes.len();
    }

    fn contains(&self, needle: &[u8]) -> bool {
        !needle.is_empty()
            && self
                .bytes
                .windows(needle.len())
                .any(|chunk| chunk == needle)
    }

    fn is_truncated(&self) -> bool {
        self.truncated
    }
}

impl fmt::Debug for CapturedStderr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapturedStderr")
            .field("captured_bytes", &self.bytes.len())
            .field("truncated", &self.truncated)
            .finish()
    }
}

#[allow(dead_code)]
struct ChildProcessClient {
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    stderr_task: Option<JoinHandle<io::Result<CapturedStderr>>>,
    #[cfg(test)]
    stderr_capture_complete: Option<oneshot::Receiver<()>>,
    limits: ProcessLimits,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
enum ProcessClientError {
    InvalidArguments(&'static str),
    InvalidLimits,
    InvalidWorkerPath,
    ChildSpawn,
    ChildStdin,
    ChildStdout,
    ChildStderr,
    ChildIo,
    RequestEncoding,
    RequestLineTooLong,
    RawTestLineTooLong,
    RawTestLineContainsNewline,
    ResponseLineTooLong,
    ResponseMissingNewline,
    ResponseEnded,
    ResponseInvalidJson,
    RequestDeadline,
    ChildExitDeadline,
    ChildTermination,
    ChildExitedUnsuccessfully,
    StderrCapture,
    StderrLimitExceeded,
    ChildExitedSuccessfully,
    ProtocolScenario(&'static str),
}

impl fmt::Display for ProcessClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArguments(message) => formatter.write_str(message),
            Self::InvalidLimits => formatter.write_str("process limits must be positive"),
            Self::InvalidWorkerPath => formatter.write_str("worker executable path is invalid"),
            Self::ChildSpawn => formatter.write_str("worker process could not start"),
            Self::ChildStdin => formatter.write_str("worker stdin was not piped"),
            Self::ChildStdout => formatter.write_str("worker stdout was not piped"),
            Self::ChildStderr => formatter.write_str("worker stderr was not piped"),
            Self::ChildIo => formatter.write_str("worker process I/O failed"),
            Self::RequestEncoding => formatter.write_str("request could not be encoded"),
            Self::RequestLineTooLong => {
                formatter.write_str("request exceeds the configured line limit")
            }
            Self::RawTestLineTooLong => {
                formatter.write_str("raw test frame exceeds the harness line limit")
            }
            Self::RawTestLineContainsNewline => {
                formatter.write_str("raw test frame must contain one harness-added newline")
            }
            Self::ResponseLineTooLong => {
                formatter.write_str("response exceeds the configured line limit")
            }
            Self::ResponseMissingNewline => {
                formatter.write_str("worker response ended without a newline")
            }
            Self::ResponseEnded => formatter.write_str("worker response stream ended"),
            Self::ResponseInvalidJson => formatter.write_str("worker response was not valid JSON"),
            Self::RequestDeadline => formatter.write_str("worker request deadline elapsed"),
            Self::ChildExitDeadline => {
                formatter.write_str("worker did not exit before the deadline")
            }
            Self::ChildTermination => formatter.write_str("worker process could not be terminated"),
            Self::ChildExitedUnsuccessfully => formatter.write_str("worker exited unsuccessfully"),
            Self::StderrCapture => formatter.write_str("worker stderr capture failed"),
            Self::StderrLimitExceeded => {
                formatter.write_str("worker stderr exceeded the configured capture limit")
            }
            Self::ChildExitedSuccessfully => {
                formatter.write_str("worker exited successfully when failure was expected")
            }
            Self::ProtocolScenario(message) => formatter.write_str(message),
        }
    }
}

impl Error for ProcessClientError {}

struct PressureObservedWriter<W> {
    inner: W,
    pressure_observed: Option<oneshot::Sender<()>>,
    wrote_request_bytes: bool,
}

impl<W> PressureObservedWriter<W> {
    fn new(inner: W, pressure_observed: oneshot::Sender<()>) -> Self {
        Self {
            inner,
            pressure_observed: Some(pressure_observed),
            wrote_request_bytes: false,
        }
    }

    fn record_pending_write(&mut self) {
        if let Some(observer) = self.pressure_observed.take() {
            let _ = observer.send(());
        }
    }
}

impl<W> AsyncWrite for PressureObservedWriter<W>
where
    W: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(context, buffer) {
            Poll::Pending => {
                if this.wrote_request_bytes && !buffer.is_empty() {
                    this.record_pending_write();
                }
                Poll::Pending
            }
            Poll::Ready(Ok(written)) => {
                this.wrote_request_bytes |= written > 0;
                Poll::Ready(Ok(written))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(context)
    }
}

struct RequestFlood {
    pressure_observed: oneshot::Receiver<()>,
    completion: JoinHandle<FakeResult>,
    timeout: Duration,
}

impl RequestFlood {
    async fn wait_until_pressure_observed(&mut self) -> FakeResult {
        match timeout(self.timeout, &mut self.pressure_observed).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) | Err(_) => Err(ProcessClientError::ProtocolScenario(
                FLOOD_PRESSURE_NOT_OBSERVED,
            )),
        }
    }

    async fn finish(mut self) -> FakeResult {
        match timeout(self.timeout, &mut self.completion).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ProcessClientError::ChildIo),
            Err(_) => {
                self.completion.abort();
                Err(ProcessClientError::RequestDeadline)
            }
        }
    }
}

impl Drop for RequestFlood {
    fn drop(&mut self) {
        self.completion.abort();
    }
}

#[tokio::main]
async fn main() -> FakeResult {
    run(parse_arguments(env::args())?).await
}

async fn run(arguments: Arguments) -> FakeResult {
    let executable = resolve_worker_executable(&arguments.worker)?;
    run_protocol_scenarios(executable, arguments.limits).await
}

async fn run_protocol_scenarios(executable: PathBuf, limits: ProcessLimits) -> FakeResult {
    assert_invalid_startup(&executable, limits).await?;
    assert_partial_embedding_startup(&executable, limits).await?;
    assert_disabled_embedding_startup(&executable, limits).await?;
    assert_discovery_and_protocol_errors(&executable, limits).await?;
    assert_oversized_input_recovers(&executable, limits).await?;
    assert_bounded_output_flood_recovers(&executable, limits).await?;
    assert_eof_drains_response(&executable, limits).await?;
    #[cfg(unix)]
    {
        assert_signal_shutdown(&executable, limits, "INT").await?;
        assert_signal_shutdown(&executable, limits, "TERM").await?;
    }
    assert_memory_save_flows(&executable, limits).await?;
    assert_memory_version_list_flows(&executable, limits).await?;
    assert_memory_retrieval_flows(&executable, limits).await?;
    assert_memory_search_flows(&executable, limits).await?;
    Ok(())
}

async fn start_worker(executable: &Path, limits: ProcessLimits) -> FakeResult<ChildProcessClient> {
    start_worker_with_bounds(executable, limits, DEFAULT_WORKER_BOUNDS).await
}

async fn start_worker_with_bounds(
    executable: &Path,
    limits: ProcessLimits,
    bounds: WorkerBounds,
) -> FakeResult<ChildProcessClient> {
    ChildProcessClient::start(
        WorkerInvocation {
            executable: executable.to_owned(),
        }
        .command(bounds),
        limits,
    )
    .await
}

async fn start_worker_with_engine(
    executable: &Path,
    limits: ProcessLimits,
    engine_url: &str,
) -> FakeResult<ChildProcessClient> {
    ChildProcessClient::start(
        WorkerInvocation {
            executable: executable.to_owned(),
        }
        .command_with_iii_url(DEFAULT_WORKER_BOUNDS, engine_url),
        limits,
    )
    .await
}

async fn assert_memory_save_flows(executable: &Path, limits: ProcessLimits) -> FakeResult {
    assert_successful_memory_save(executable, limits).await?;
    assert_invalid_memory_save_requests(executable, limits).await?;
    assert_failed_memory_save(
        executable,
        limits,
        SaveEngineMode::InsertConflict,
        "save-conflict",
        "conflict",
    )
    .await?;
    assert_failed_memory_save(
        executable,
        limits,
        SaveEngineMode::BackendFailure,
        "save-backend-failure",
        "backend_failure",
    )
    .await
}

async fn assert_successful_memory_save(executable: &Path, limits: ProcessLimits) -> FakeResult {
    let (mut client, engine) =
        start_memory_save_scenario(executable, limits, SaveEngineMode::InsertSuccess).await?;
    let response = match client
        .request(&tool_request(
            "save-success",
            "memory_save",
            valid_memory_save_arguments(),
        ))
        .await
    {
        Ok(response) => response,
        Err(error) => return terminate_memory_save_scenario(client, engine, error).await,
    };
    let (stderr, completion) = stop_memory_save_scenario(client, engine).await?;
    let EngineCompletion::DatabaseInvocation(invocation) = completion else {
        return Err(ProcessClientError::ProtocolScenario(
            "successful memory save did not reach the database engine",
        ));
    };

    assert_empty_stderr(&stderr)?;
    assert_successful_memory_save_response(&response, &invocation)
}

async fn assert_invalid_memory_save_requests(
    executable: &Path,
    limits: ProcessLimits,
) -> FakeResult {
    let (mut client, mut engine) =
        start_memory_save_scenario(executable, limits, SaveEngineMode::NoDatabaseInvocation)
            .await?;
    for (id, arguments) in [
        (
            "save-invalid-title",
            json!({
                "title": "",
                "content": SAVE_CONTENT_SENTINEL,
                "session_id": SAVE_SESSION_SENTINEL,
            }),
        ),
        (
            "save-invalid-content",
            json!({
                "title": SAVE_TITLE_SENTINEL,
                "content": "",
                "session_id": SAVE_SESSION_SENTINEL,
            }),
        ),
        (
            "save-invalid-session",
            json!({
                "title": SAVE_TITLE_SENTINEL,
                "content": SAVE_CONTENT_SENTINEL,
                "session_id": "",
            }),
        ),
    ] {
        let response = match client
            .request(&tool_request(id, "memory_save", arguments))
            .await
        {
            Ok(response) => response,
            Err(error) => return terminate_memory_save_scenario(client, engine, error).await,
        };
        if let Err(error) = assert_tool_error(&response, id, "invalid_input") {
            return terminate_memory_save_scenario(client, engine, error).await;
        }
        if let Err(error) = assert_protected_values_absent(&response) {
            return terminate_memory_save_scenario(client, engine, error).await;
        }
    }
    if let Err(error) = engine.wait_until_no_database_invocation().await {
        return terminate_memory_save_scenario(client, engine, error).await;
    }

    let (stderr, completion) = stop_memory_save_scenario(client, engine).await?;
    require(
        matches!(completion, EngineCompletion::NoDatabaseInvocation),
        "invalid memory save unexpectedly reached the database engine",
    )?;
    assert_empty_stderr(&stderr)?;
    assert_protected_stderr_values_absent(&stderr)
}

async fn assert_failed_memory_save(
    executable: &Path,
    limits: ProcessLimits,
    mode: SaveEngineMode,
    request_id: &str,
    expected_error: &str,
) -> FakeResult {
    let (mut client, engine) = start_memory_save_scenario(executable, limits, mode).await?;
    let response = match client
        .request(&tool_request(
            request_id,
            "memory_save",
            valid_memory_save_arguments(),
        ))
        .await
    {
        Ok(response) => response,
        Err(error) => return terminate_memory_save_scenario(client, engine, error).await,
    };
    let (stderr, completion) = stop_memory_save_scenario(client, engine).await?;
    let EngineCompletion::DatabaseInvocation(invocation) = completion else {
        return Err(ProcessClientError::ProtocolScenario(
            "failed memory save did not reach the database engine",
        ));
    };

    assert_memory_insert_request(&invocation)?;
    assert_tool_error(&response, request_id, expected_error)?;
    assert_protected_values_absent(&response)?;
    assert_empty_stderr(&stderr)?;
    assert_protected_stderr_values_absent(&stderr)
}

async fn start_memory_save_scenario(
    executable: &Path,
    limits: ProcessLimits,
    mode: SaveEngineMode,
) -> FakeResult<(ChildProcessClient, FakeDatabaseEngine)> {
    let mut engine =
        FakeDatabaseEngine::start(FakeDatabaseEngineMode::Save(mode), limits.request_timeout)
            .await?;
    let client = match start_worker_with_engine(executable, limits, engine.url()).await {
        Ok(client) => client,
        Err(error) => {
            engine.abort_and_wait().await;
            return Err(error);
        }
    };
    if let Err(error) = engine.wait_until_registered().await {
        return terminate_memory_save_scenario(client, engine, error).await;
    }

    Ok((client, engine))
}

async fn stop_memory_save_scenario(
    client: ChildProcessClient,
    engine: FakeDatabaseEngine,
) -> FakeResult<(CapturedStderr, EngineCompletion)> {
    let stderr = match client.stop_successfully().await {
        Ok(stderr) => stderr,
        Err(error) => {
            engine.abort_and_wait().await;
            return Err(error);
        }
    };
    let completion = engine.finish().await?;
    Ok((stderr, completion))
}

async fn terminate_memory_save_scenario<T>(
    client: ChildProcessClient,
    engine: FakeDatabaseEngine,
    error: ProcessClientError,
) -> FakeResult<T> {
    let child_cleanup = client.terminate().await;
    let engine_cleanup = engine.finish().await;
    match (child_cleanup, engine_cleanup) {
        (Err(cleanup_error), _) => Err(cleanup_error),
        (Ok(_), _) => Err(error),
    }
}

fn valid_memory_save_arguments() -> Value {
    json!({
        "title": SAVE_TITLE_SENTINEL,
        "content": SAVE_CONTENT_SENTINEL,
        "session_id": SAVE_SESSION_SENTINEL,
    })
}

async fn assert_memory_search_flows(executable: &Path, limits: ProcessLimits) -> FakeResult {
    assert_enabled_combined_memory_search(executable, limits, SearchEngineMode::CombinedSuccess)
        .await?;
    assert_enabled_combined_memory_search(executable, limits, SearchEngineMode::CombinedEmpty)
        .await?;
    for mode in [
        SearchEngineMode::DisabledEmbeddingFallback,
        SearchEngineMode::DisabledEmbeddingEmptyFallback,
        SearchEngineMode::RouterFailureFallback,
        SearchEngineMode::InvalidRouterResponseFallback,
    ] {
        assert_lexical_fallback_memory_search(executable, limits, mode).await?;
    }
    assert_invalid_memory_search_requests(executable, limits).await?;
    assert_failed_memory_search(
        executable,
        limits,
        SearchEngineMode::DisabledEmbeddingLexicalFailure,
        "search-disabled-lexical-backend-failure",
        "backend_failure",
    )
    .await?;
    assert_failed_memory_search(
        executable,
        limits,
        SearchEngineMode::LexicalBackendFailure,
        "search-lexical-backend-failure",
        "backend_failure",
    )
    .await?;
    assert_failed_memory_search(
        executable,
        limits,
        SearchEngineMode::InvalidVectorSearchResponse,
        "search-vector-invalid-response",
        "internal_failure",
    )
    .await
}

async fn assert_memory_version_list_flows(executable: &Path, limits: ProcessLimits) -> FakeResult {
    assert_default_memory_version_list(executable, limits).await?;
    assert_continued_memory_version_list(executable, limits).await?;
    assert_empty_memory_version_list(executable, limits).await?;
    assert_invalid_memory_version_list_requests(executable, limits).await?;
    assert_failed_memory_version_list(executable, limits).await
}

async fn assert_memory_retrieval_flows(executable: &Path, limits: ProcessLimits) -> FakeResult {
    assert_successful_memory_retrieval(
        executable,
        limits,
        RetrievalEngineMode::ExactSuccess,
        "retrieval-exact-success",
        RetrievalOperation::Exact,
    )
    .await?;
    assert_successful_memory_retrieval(
        executable,
        limits,
        RetrievalEngineMode::LatestSuccess,
        "retrieval-latest-success",
        RetrievalOperation::Latest,
    )
    .await?;
    assert_not_found_memory_retrieval(
        executable,
        limits,
        RetrievalEngineMode::ExactNotFound,
        "retrieval-exact-not-found",
        RetrievalOperation::Exact,
    )
    .await?;
    assert_not_found_memory_retrieval(
        executable,
        limits,
        RetrievalEngineMode::LatestNotFound,
        "retrieval-latest-not-found",
        RetrievalOperation::Latest,
    )
    .await?;
    assert_invalid_memory_retrieval_requests(executable, limits).await?;
    assert_failed_memory_retrieval(
        executable,
        limits,
        RetrievalEngineMode::ExactMalformed,
        "retrieval-exact-malformed",
        RetrievalOperation::Exact,
        "internal_failure",
    )
    .await?;
    assert_failed_memory_retrieval(
        executable,
        limits,
        RetrievalEngineMode::LatestMalformed,
        "retrieval-latest-malformed",
        RetrievalOperation::Latest,
        "internal_failure",
    )
    .await?;
    assert_failed_memory_retrieval(
        executable,
        limits,
        RetrievalEngineMode::ExactBackendFailure,
        "retrieval-exact-backend-failure",
        RetrievalOperation::Exact,
        "backend_failure",
    )
    .await?;
    assert_failed_memory_retrieval(
        executable,
        limits,
        RetrievalEngineMode::LatestBackendFailure,
        "retrieval-latest-backend-failure",
        RetrievalOperation::Latest,
        "backend_failure",
    )
    .await
}

async fn assert_successful_memory_retrieval(
    executable: &Path,
    limits: ProcessLimits,
    mode: RetrievalEngineMode,
    request_id: &str,
    operation: RetrievalOperation,
) -> FakeResult {
    let (mut client, engine) = start_memory_retrieval_scenario(executable, limits, mode).await?;
    let response = match client
        .request(&tool_request(
            request_id,
            operation.tool_name(),
            memory_retrieval_arguments(operation),
        ))
        .await
    {
        Ok(response) => response,
        Err(error) => return terminate_memory_save_scenario(client, engine, error).await,
    };
    let (stderr, completion) = stop_memory_save_scenario(client, engine).await?;
    let EngineCompletion::RetrievalInvocation(invocation) = completion else {
        return Err(ProcessClientError::ProtocolScenario(
            "successful memory retrieval did not reach the database engine",
        ));
    };

    assert_empty_stderr(&stderr)?;
    assert_memory_retrieval_request(&invocation, operation)?;
    assert_successful_memory_retrieval_response(&response, request_id, operation)
}

async fn assert_not_found_memory_retrieval(
    executable: &Path,
    limits: ProcessLimits,
    mode: RetrievalEngineMode,
    request_id: &str,
    operation: RetrievalOperation,
) -> FakeResult {
    assert_failed_memory_retrieval(executable, limits, mode, request_id, operation, "not_found")
        .await
}

async fn assert_failed_memory_retrieval(
    executable: &Path,
    limits: ProcessLimits,
    mode: RetrievalEngineMode,
    request_id: &str,
    operation: RetrievalOperation,
    expected_error: &str,
) -> FakeResult {
    let (mut client, engine) = start_memory_retrieval_scenario(executable, limits, mode).await?;
    let response = match client
        .request(&tool_request(
            request_id,
            operation.tool_name(),
            memory_retrieval_arguments(operation),
        ))
        .await
    {
        Ok(response) => response,
        Err(error) => return terminate_memory_save_scenario(client, engine, error).await,
    };
    let (stderr, completion) = stop_memory_save_scenario(client, engine).await?;
    let EngineCompletion::RetrievalInvocation(invocation) = completion else {
        return Err(ProcessClientError::ProtocolScenario(
            "failed memory retrieval did not reach the database engine",
        ));
    };

    assert_memory_retrieval_request(&invocation, operation)?;
    assert_tool_error(&response, request_id, expected_error)?;
    assert_protected_retrieval_values_absent(&response)?;
    assert_empty_stderr(&stderr)?;
    assert_protected_retrieval_stderr_values_absent(&stderr)
}

async fn assert_invalid_memory_retrieval_requests(
    executable: &Path,
    limits: ProcessLimits,
) -> FakeResult {
    let (mut client, mut engine) = start_memory_retrieval_scenario(
        executable,
        limits,
        RetrievalEngineMode::NoDatabaseInvocation,
    )
    .await?;
    for (request_id, tool_name, arguments) in [
        (
            "retrieval-latest-invalid-id",
            RetrievalOperation::Latest.tool_name(),
            json!({ "id": "" }),
        ),
        (
            "retrieval-exact-invalid-id",
            RetrievalOperation::Exact.tool_name(),
            json!({ "id": "", "version": RETRIEVAL_EXACT_VERSION }),
        ),
        (
            "retrieval-exact-invalid-zero-version",
            RetrievalOperation::Exact.tool_name(),
            json!({ "id": RETRIEVAL_ID_SENTINEL, "version": 0 }),
        ),
        (
            "retrieval-exact-invalid-negative-version",
            RetrievalOperation::Exact.tool_name(),
            json!({ "id": RETRIEVAL_ID_SENTINEL, "version": -1 }),
        ),
    ] {
        let response = match client
            .request(&tool_request(request_id, tool_name, arguments))
            .await
        {
            Ok(response) => response,
            Err(error) => return terminate_memory_save_scenario(client, engine, error).await,
        };
        if let Err(error) = assert_tool_error(&response, request_id, "invalid_input") {
            return terminate_memory_save_scenario(client, engine, error).await;
        }
        if let Err(error) = assert_protected_retrieval_values_absent(&response) {
            return terminate_memory_save_scenario(client, engine, error).await;
        }
    }
    if let Err(error) = engine.wait_until_no_database_invocation().await {
        return terminate_memory_save_scenario(client, engine, error).await;
    }

    let (stderr, completion) = stop_memory_save_scenario(client, engine).await?;
    require(
        matches!(completion, EngineCompletion::NoDatabaseInvocation),
        "invalid memory retrieval unexpectedly reached the database engine",
    )?;
    assert_empty_stderr(&stderr)?;
    assert_protected_retrieval_stderr_values_absent(&stderr)
}

async fn start_memory_retrieval_scenario(
    executable: &Path,
    limits: ProcessLimits,
    mode: RetrievalEngineMode,
) -> FakeResult<(ChildProcessClient, FakeDatabaseEngine)> {
    let mut engine = FakeDatabaseEngine::start(
        FakeDatabaseEngineMode::Retrieval(mode),
        limits.request_timeout,
    )
    .await?;
    let client = match start_worker_with_engine(executable, limits, engine.url()).await {
        Ok(client) => client,
        Err(error) => {
            engine.abort_and_wait().await;
            return Err(error);
        }
    };
    if let Err(error) = engine.wait_until_registered().await {
        return terminate_memory_save_scenario(client, engine, error).await;
    }

    Ok((client, engine))
}

fn memory_retrieval_arguments(operation: RetrievalOperation) -> Value {
    match operation {
        RetrievalOperation::Exact => {
            json!({ "id": RETRIEVAL_ID_SENTINEL, "version": RETRIEVAL_EXACT_VERSION })
        }
        RetrievalOperation::Latest => json!({ "id": RETRIEVAL_ID_SENTINEL }),
    }
}

async fn assert_default_memory_version_list(
    executable: &Path,
    limits: ProcessLimits,
) -> FakeResult {
    let (mut client, engine) =
        start_memory_version_list_scenario(executable, limits, VersionListEngineMode::DefaultPage)
            .await?;
    let response = match client
        .request(&tool_request(
            "version-list-default",
            "memory_list_versions",
            memory_version_list_arguments(None, None),
        ))
        .await
    {
        Ok(response) => response,
        Err(error) => return terminate_memory_save_scenario(client, engine, error).await,
    };
    let (stderr, completion) = stop_memory_save_scenario(client, engine).await?;
    let EngineCompletion::VersionListInvocation(invocation) = completion else {
        return Err(ProcessClientError::ProtocolScenario(
            "default version listing did not reach the database engine",
        ));
    };

    assert_empty_stderr(&stderr)?;
    assert_memory_version_list_request(&invocation, 0, VERSION_LIST_DEFAULT_LIMIT + 1)?;
    assert_successful_memory_version_list_response(
        &response,
        "version-list-default",
        &(2_i64..=i64::from(VERSION_LIST_DEFAULT_LIMIT) + 1)
            .rev()
            .collect::<Vec<_>>(),
        Some(u64::from(VERSION_LIST_DEFAULT_LIMIT)),
    )
}

async fn assert_continued_memory_version_list(
    executable: &Path,
    limits: ProcessLimits,
) -> FakeResult {
    let (mut client, engine) = start_memory_version_list_scenario(
        executable,
        limits,
        VersionListEngineMode::ContinuedPage,
    )
    .await?;
    let response = match client
        .request(&tool_request(
            "version-list-continued",
            "memory_list_versions",
            memory_version_list_arguments(
                Some(VERSION_LIST_CONTINUED_OFFSET),
                Some(VERSION_LIST_CONTINUED_LIMIT),
            ),
        ))
        .await
    {
        Ok(response) => response,
        Err(error) => return terminate_memory_save_scenario(client, engine, error).await,
    };
    let (stderr, completion) = stop_memory_save_scenario(client, engine).await?;
    let EngineCompletion::VersionListInvocation(invocation) = completion else {
        return Err(ProcessClientError::ProtocolScenario(
            "continued version listing did not reach the database engine",
        ));
    };

    assert_empty_stderr(&stderr)?;
    assert_memory_version_list_request(
        &invocation,
        VERSION_LIST_CONTINUED_OFFSET,
        VERSION_LIST_CONTINUED_LIMIT + 1,
    )?;
    assert_successful_memory_version_list_response(
        &response,
        "version-list-continued",
        &[9, 8],
        Some(VERSION_LIST_CONTINUED_OFFSET + u64::from(VERSION_LIST_CONTINUED_LIMIT)),
    )
}

async fn assert_empty_memory_version_list(executable: &Path, limits: ProcessLimits) -> FakeResult {
    let (mut client, engine) =
        start_memory_version_list_scenario(executable, limits, VersionListEngineMode::EmptyPage)
            .await?;
    let response = match client
        .request(&tool_request(
            "version-list-empty",
            "memory_list_versions",
            memory_version_list_arguments(
                Some(VERSION_LIST_EMPTY_OFFSET),
                Some(VERSION_LIST_EMPTY_LIMIT),
            ),
        ))
        .await
    {
        Ok(response) => response,
        Err(error) => return terminate_memory_save_scenario(client, engine, error).await,
    };
    let (stderr, completion) = stop_memory_save_scenario(client, engine).await?;
    let EngineCompletion::VersionListInvocation(invocation) = completion else {
        return Err(ProcessClientError::ProtocolScenario(
            "empty version listing did not reach the database engine",
        ));
    };

    assert_empty_stderr(&stderr)?;
    assert_memory_version_list_request(
        &invocation,
        VERSION_LIST_EMPTY_OFFSET,
        VERSION_LIST_EMPTY_LIMIT + 1,
    )?;
    assert_successful_memory_version_list_response(&response, "version-list-empty", &[], None)
}

async fn assert_invalid_memory_version_list_requests(
    executable: &Path,
    limits: ProcessLimits,
) -> FakeResult {
    let (mut client, mut engine) = start_memory_version_list_scenario(
        executable,
        limits,
        VersionListEngineMode::NoDatabaseInvocation,
    )
    .await?;
    for (id, arguments) in [
        ("version-list-invalid-id", json!({ "id": "" })),
        (
            "version-list-invalid-offset",
            json!({
                "id": VERSION_LIST_ID_SENTINEL,
                "offset": 9_007_199_254_740_892u64,
            }),
        ),
        (
            "version-list-invalid-limit",
            json!({ "id": VERSION_LIST_ID_SENTINEL, "limit": 0 }),
        ),
    ] {
        let response = match client
            .request(&tool_request(id, "memory_list_versions", arguments))
            .await
        {
            Ok(response) => response,
            Err(error) => return terminate_memory_save_scenario(client, engine, error).await,
        };
        if let Err(error) = assert_tool_error(&response, id, "invalid_input") {
            return terminate_memory_save_scenario(client, engine, error).await;
        }
        if let Err(error) = assert_protected_version_list_values_absent(&response) {
            return terminate_memory_save_scenario(client, engine, error).await;
        }
    }
    if let Err(error) = engine.wait_until_no_database_invocation().await {
        return terminate_memory_save_scenario(client, engine, error).await;
    }

    let (stderr, completion) = stop_memory_save_scenario(client, engine).await?;
    require(
        matches!(completion, EngineCompletion::NoDatabaseInvocation),
        "invalid version listing unexpectedly reached the database engine",
    )?;
    assert_empty_stderr(&stderr)?;
    assert_protected_version_list_stderr_values_absent(&stderr)
}

async fn assert_failed_memory_version_list(executable: &Path, limits: ProcessLimits) -> FakeResult {
    let (mut client, engine) = start_memory_version_list_scenario(
        executable,
        limits,
        VersionListEngineMode::BackendFailure,
    )
    .await?;
    let response = match client
        .request(&tool_request(
            "version-list-backend-failure",
            "memory_list_versions",
            memory_version_list_arguments(None, None),
        ))
        .await
    {
        Ok(response) => response,
        Err(error) => return terminate_memory_save_scenario(client, engine, error).await,
    };
    let (stderr, completion) = stop_memory_save_scenario(client, engine).await?;
    let EngineCompletion::VersionListInvocation(invocation) = completion else {
        return Err(ProcessClientError::ProtocolScenario(
            "failed version listing did not reach the database engine",
        ));
    };

    assert_memory_version_list_request(&invocation, 0, VERSION_LIST_DEFAULT_LIMIT + 1)?;
    assert_tool_error(&response, "version-list-backend-failure", "backend_failure")?;
    assert_protected_version_list_values_absent(&response)?;
    assert_empty_stderr(&stderr)?;
    assert_protected_version_list_stderr_values_absent(&stderr)
}

async fn start_memory_version_list_scenario(
    executable: &Path,
    limits: ProcessLimits,
    mode: VersionListEngineMode,
) -> FakeResult<(ChildProcessClient, FakeDatabaseEngine)> {
    let mut engine = FakeDatabaseEngine::start(
        FakeDatabaseEngineMode::VersionList(mode),
        limits.request_timeout,
    )
    .await?;
    let client = match start_worker_with_engine(executable, limits, engine.url()).await {
        Ok(client) => client,
        Err(error) => {
            engine.abort_and_wait().await;
            return Err(error);
        }
    };
    if let Err(error) = engine.wait_until_registered().await {
        return terminate_memory_save_scenario(client, engine, error).await;
    }

    Ok((client, engine))
}

fn memory_version_list_arguments(offset: Option<u64>, limit: Option<u32>) -> Value {
    let mut arguments = Map::new();
    arguments.insert("id".to_owned(), json!(VERSION_LIST_ID_SENTINEL));
    if let Some(offset) = offset {
        arguments.insert("offset".to_owned(), json!(offset));
    }
    if let Some(limit) = limit {
        arguments.insert("limit".to_owned(), json!(limit));
    }
    Value::Object(arguments)
}

async fn assert_enabled_combined_memory_search(
    executable: &Path,
    limits: ProcessLimits,
    mode: SearchEngineMode,
) -> FakeResult {
    let (mut client, engine) = start_memory_search_scenario(executable, limits, mode).await?;
    let response = match client
        .request(&tool_request(
            "search-success",
            "memory_search",
            json!({ "query": SEARCH_COMBINED_QUERY, "limit": SEARCH_LIMIT }),
        ))
        .await
    {
        Ok(response) => response,
        Err(error) => return finish_failed_combined_memory_search(client, engine, error).await,
    };
    let (stderr, completion) = stop_memory_save_scenario(client, engine).await?;
    let EngineCompletion::CombinedSearchInvocations(invocations) = completion else {
        return Err(ProcessClientError::ProtocolScenario(
            "enabled memory search did not capture router::embed and both database searches",
        ));
    };

    assert_empty_stderr(&stderr)?;
    assert_combined_memory_search_requests(&invocations)?;
    if matches!(mode, SearchEngineMode::CombinedEmpty) {
        require(
            response == memory_search_response(Vec::new()),
            "enabled memory search did not return an empty collection after both searches were empty",
        )?;
    } else {
        assert_successful_memory_search_response(&response)?;
    }
    assert_query_embedding_values_absent(&response)
}

async fn finish_failed_combined_memory_search<T>(
    client: ChildProcessClient,
    engine: FakeDatabaseEngine,
    error: ProcessClientError,
) -> FakeResult<T> {
    // A failed request has already terminated and reaped the child, which disconnects the
    // engine. A stalled request is better explained by the invocation the engine never got.
    drop(client);
    match (error, engine.finish().await) {
        (ProcessClientError::RequestDeadline, Err(engine_error)) => Err(engine_error),
        (error, _) => Err(error),
    }
}

async fn assert_lexical_fallback_memory_search(
    executable: &Path,
    limits: ProcessLimits,
    mode: SearchEngineMode,
) -> FakeResult {
    let (mut client, engine) = start_memory_search_scenario(executable, limits, mode).await?;
    let response = match client
        .request(&tool_request(
            "search-success",
            "memory_search",
            json!({ "query": SEARCH_COMBINED_QUERY, "limit": SEARCH_LEXICAL_FALLBACK_LIMIT }),
        ))
        .await
    {
        Ok(response) => response,
        Err(error) => return finish_failed_combined_memory_search(client, engine, error).await,
    };
    let (stderr, completion) = stop_memory_save_scenario(client, engine).await?;
    let EngineCompletion::LexicalFallbackInvocations(invocations) = completion else {
        return Err(ProcessClientError::ProtocolScenario(
            "lexical fallback did not capture its only BM25 search",
        ));
    };

    assert_lexical_fallback_stderr(&stderr)?;
    assert_lexical_fallback_requests(&invocations, mode)?;
    assert_lexical_fallback_response(&response, mode)
}

/// Runs with query embedding enabled, so a request that reached generation or search would send
/// `router::embed` or `database::execute` inside the fake engine's quiet window.
async fn assert_invalid_memory_search_requests(
    executable: &Path,
    limits: ProcessLimits,
) -> FakeResult {
    let (mut client, mut engine) =
        start_memory_search_scenario(executable, limits, SearchEngineMode::NoDatabaseInvocation)
            .await?;
    let nul_query = format!("{SEARCH_QUERY_SENTINEL}\0");
    for (id, arguments) in [
        (
            "search-caller-vector",
            json!({
                "query": SEARCH_COMBINED_QUERY,
                "vector": SEARCH_CALLER_VECTOR,
                "limit": SEARCH_LIMIT,
            }),
        ),
        (
            "search-null-vector",
            json!({ "query": SEARCH_COMBINED_QUERY, "vector": null, "limit": SEARCH_LIMIT }),
        ),
        (
            "search-unknown-argument",
            json!({
                "query": SEARCH_COMBINED_QUERY,
                "limit": SEARCH_LIMIT,
                "filter": SEARCH_UNKNOWN_ARGUMENT_SENTINEL,
            }),
        ),
        (
            "search-wrong-type-limit",
            json!({ "query": SEARCH_COMBINED_QUERY, "limit": SEARCH_LIMIT.to_string() }),
        ),
        (
            "search-empty-query",
            json!({ "query": "", "limit": SEARCH_LIMIT }),
        ),
        (
            "search-nul-query",
            json!({ "query": nul_query, "limit": SEARCH_LIMIT }),
        ),
        (
            "search-zero-limit",
            json!({ "query": SEARCH_COMBINED_QUERY, "limit": 0 }),
        ),
        (
            "search-oversized-limit",
            json!({ "query": SEARCH_COMBINED_QUERY, "limit": 51 }),
        ),
    ] {
        let response = match client
            .request(&tool_request(id, "memory_search", arguments))
            .await
        {
            Ok(response) => response,
            Err(error) => return finish_failed_combined_memory_search(client, engine, error).await,
        };
        if let Err(error) = assert_tool_error(&response, id, "invalid_input") {
            return terminate_memory_save_scenario(client, engine, error).await;
        }
        if let Err(error) = assert_protected_search_values_absent(&response) {
            return terminate_memory_save_scenario(client, engine, error).await;
        }
    }
    if let Err(error) = engine.wait_until_no_database_invocation().await {
        return terminate_memory_save_scenario(client, engine, error).await;
    }

    let (stderr, completion) = stop_memory_save_scenario(client, engine).await?;
    require(
        matches!(completion, EngineCompletion::NoDatabaseInvocation),
        "invalid memory search unexpectedly reached router::embed or the database engine",
    )?;
    assert_protected_search_stderr_values_absent(&stderr)?;
    assert_empty_stderr(&stderr)
}

async fn assert_failed_memory_search(
    executable: &Path,
    limits: ProcessLimits,
    mode: SearchEngineMode,
    request_id: &str,
    expected_error: &str,
) -> FakeResult {
    let (mut client, engine) = start_memory_search_scenario(executable, limits, mode).await?;
    let response = match client
        .request(&tool_request(
            request_id,
            "memory_search",
            json!({ "query": SEARCH_COMBINED_QUERY, "limit": SEARCH_LIMIT }),
        ))
        .await
    {
        Ok(response) => response,
        Err(error) => return finish_failed_combined_memory_search(client, engine, error).await,
    };
    let (stderr, completion) = stop_memory_save_scenario(client, engine).await?;
    let EngineCompletion::FailedSearchInvocations(invocations) = completion else {
        return Err(ProcessClientError::ProtocolScenario(
            "failed memory search did not capture every search it started",
        ));
    };

    assert_failed_memory_search_requests(&invocations, mode)?;
    require(
        response["result"]["isError"] == true
            && response["result"].get("structuredContent").is_none(),
        mode.failed_search_returned_results(),
    )?;
    require(
        response["result"]["content"] == json!([{ "type": "text", "text": expected_error }]),
        "failed memory search did not return its stable error category",
    )?;
    assert_tool_error(&response, request_id, expected_error)?;
    assert_protected_search_values_absent(&response)?;
    assert_protected_search_stderr_values_absent(&stderr)?;
    assert_empty_stderr(&stderr)
}

async fn start_memory_search_scenario(
    executable: &Path,
    limits: ProcessLimits,
    mode: SearchEngineMode,
) -> FakeResult<(ChildProcessClient, FakeDatabaseEngine)> {
    let mut engine =
        FakeDatabaseEngine::start(FakeDatabaseEngineMode::Search(mode), limits.request_timeout)
            .await?;
    let invocation = WorkerInvocation {
        executable: executable.to_owned(),
    };
    let command = if mode.query_embedding_enabled() {
        invocation.enabled_embedding_command(engine.url())
    } else {
        invocation.disabled_embedding_command(engine.url())
    };
    let client = match ChildProcessClient::start(command, limits).await {
        Ok(client) => client,
        Err(error) => {
            engine.abort_and_wait().await;
            return Err(error);
        }
    };
    if let Err(error) = engine.wait_until_registered().await {
        return terminate_memory_save_scenario(client, engine, error).await;
    }

    Ok((client, engine))
}

async fn run_fake_database_engine(
    listener: TcpListener,
    mode: FakeDatabaseEngineMode,
    registered: oneshot::Sender<()>,
    no_database_invocation: Option<oneshot::Sender<()>>,
    timeout_duration: Duration,
) -> FakeResult<EngineCompletion> {
    let (stream, _) = timeout(timeout_duration, listener.accept())
        .await
        .map_err(|_| {
            ProcessClientError::ProtocolScenario("worker did not connect to fake database engine")
        })?
        .map_err(|_| {
            ProcessClientError::ProtocolScenario("fake database engine could not accept worker")
        })?;
    let mut socket = timeout(timeout_duration, accept_async(stream))
        .await
        .map_err(|_| ProcessClientError::ProtocolScenario("worker WebSocket handshake timed out"))?
        .map_err(|_| ProcessClientError::ProtocolScenario("worker WebSocket handshake failed"))?;

    let registration = next_engine_message(&mut socket, timeout_duration).await?;
    validate_managed_worker_registration(&registration)?;
    send_engine_message(
        &mut socket,
        json!({
            "type": "workerregistered",
            "worker_id": "mcp-worker-fake-database-engine",
        }),
    )
    .await?;
    registered.send(()).map_err(|_| {
        ProcessClientError::ProtocolScenario("fake database engine registration observer dropped")
    })?;

    match mode {
        FakeDatabaseEngineMode::Save(SaveEngineMode::NoDatabaseInvocation)
        | FakeDatabaseEngineMode::Search(SearchEngineMode::NoDatabaseInvocation)
        | FakeDatabaseEngineMode::VersionList(VersionListEngineMode::NoDatabaseInvocation)
        | FakeDatabaseEngineMode::Retrieval(RetrievalEngineMode::NoDatabaseInvocation) => {
            complete_no_database_invocation(&mut socket, no_database_invocation, timeout_duration)
                .await
        }
        FakeDatabaseEngineMode::Save(mode) => {
            let (invocation_id, invocation) =
                capture_database_invocation(&mut socket, timeout_duration).await?;
            send_database_reply(&mut socket, mode, &invocation_id, &invocation).await?;
            wait_for_engine_disconnect(&mut socket, timeout_duration).await?;
            Ok(EngineCompletion::DatabaseInvocation(invocation))
        }
        FakeDatabaseEngineMode::Search(
            mode @ (SearchEngineMode::CombinedSuccess | SearchEngineMode::CombinedEmpty),
        ) => {
            let invocations =
                capture_combined_search_invocations(&mut socket, mode, timeout_duration).await?;
            wait_for_engine_disconnect(&mut socket, timeout_duration).await?;
            Ok(EngineCompletion::CombinedSearchInvocations(invocations))
        }
        FakeDatabaseEngineMode::Search(
            mode @ (SearchEngineMode::DisabledEmbeddingFallback
            | SearchEngineMode::DisabledEmbeddingEmptyFallback
            | SearchEngineMode::RouterFailureFallback
            | SearchEngineMode::InvalidRouterResponseFallback),
        ) => {
            let invocations =
                capture_lexical_fallback_invocations(&mut socket, mode, timeout_duration).await?;
            Ok(EngineCompletion::LexicalFallbackInvocations(invocations))
        }
        FakeDatabaseEngineMode::Search(
            mode @ (SearchEngineMode::DisabledEmbeddingLexicalFailure
            | SearchEngineMode::LexicalBackendFailure
            | SearchEngineMode::InvalidVectorSearchResponse),
        ) => {
            let invocations =
                capture_failed_search_invocations(&mut socket, mode, timeout_duration).await?;
            Ok(EngineCompletion::FailedSearchInvocations(invocations))
        }
        FakeDatabaseEngineMode::VersionList(mode) => {
            let (invocation_id, invocation) =
                capture_version_list_invocation(&mut socket, timeout_duration).await?;
            send_version_list_database_reply(&mut socket, mode, &invocation_id).await?;
            wait_for_engine_disconnect(&mut socket, timeout_duration).await?;
            Ok(EngineCompletion::VersionListInvocation(invocation))
        }
        FakeDatabaseEngineMode::Retrieval(mode) => {
            let operation = mode
                .operation()
                .ok_or(ProcessClientError::ProtocolScenario(
                    "fake database engine received an invalid retrieval mode",
                ))?;
            let (invocation_id, invocation) =
                capture_retrieval_invocation(&mut socket, timeout_duration, operation).await?;
            send_retrieval_database_reply(&mut socket, mode, &invocation_id).await?;
            wait_for_engine_disconnect(&mut socket, timeout_duration).await?;
            Ok(EngineCompletion::RetrievalInvocation(invocation))
        }
    }
}

async fn complete_no_database_invocation(
    socket: &mut WebSocketStream<TcpStream>,
    no_database_invocation: Option<oneshot::Sender<()>>,
    timeout_duration: Duration,
) -> FakeResult<EngineCompletion> {
    wait_for_no_database_invocation(socket, timeout_duration).await?;
    no_database_invocation
        .ok_or(ProcessClientError::ProtocolScenario(
            "fake database engine missing no-invocation observer",
        ))?
        .send(())
        .map_err(|_| {
            ProcessClientError::ProtocolScenario(
                "fake database engine no-invocation observer dropped",
            )
        })?;
    wait_for_engine_disconnect(socket, timeout_duration).await?;
    Ok(EngineCompletion::NoDatabaseInvocation)
}

async fn next_engine_message(
    socket: &mut WebSocketStream<TcpStream>,
    timeout_duration: Duration,
) -> FakeResult<Value> {
    receive_engine_message(socket, timeout_duration, None).await
}

/// Reports a timeout, disconnect, or close before the next message as `absent`.
async fn next_expected_engine_message(
    socket: &mut WebSocketStream<TcpStream>,
    timeout_duration: Duration,
    absent: &'static str,
) -> FakeResult<Value> {
    receive_engine_message(socket, timeout_duration, Some(absent)).await
}

async fn receive_engine_message(
    socket: &mut WebSocketStream<TcpStream>,
    timeout_duration: Duration,
    absent: Option<&'static str>,
) -> FakeResult<Value> {
    let absence = |message| ProcessClientError::ProtocolScenario(absent.unwrap_or(message));
    loop {
        let frame = timeout(timeout_duration, socket.next())
            .await
            .map_err(|_| absence("fake database engine receive timed out"))?
            .ok_or_else(|| {
                absence("worker disconnected before expected fake database engine message")
            })?
            .map_err(|_| absence("fake database engine receive failed"))?;
        match frame {
            Message::Text(text) => {
                return serde_json::from_str(text.as_str()).map_err(|_| {
                    ProcessClientError::ProtocolScenario(
                        "worker sent invalid fake database engine JSON",
                    )
                });
            }
            Message::Ping(payload) => {
                socket.send(Message::Pong(payload)).await.map_err(|_| {
                    ProcessClientError::ProtocolScenario(
                        "fake database engine could not answer ping",
                    )
                })?;
            }
            Message::Pong(_) => {}
            Message::Close(_) => {
                return Err(absence(
                    "worker closed before expected fake database engine message",
                ));
            }
            Message::Binary(_) | Message::Frame(_) => {
                return Err(ProcessClientError::ProtocolScenario(
                    "worker sent a non-text fake database engine message",
                ));
            }
        }
    }
}

async fn wait_for_no_database_invocation(
    socket: &mut WebSocketStream<TcpStream>,
    timeout_duration: Duration,
) -> FakeResult {
    let deadline = tokio::time::sleep(timeout_duration);
    tokio::pin!(deadline);
    loop {
        let frame = tokio::select! {
            biased;
            _ = &mut deadline => return Ok(()),
            frame = socket.next() => match frame {
                None => {
                return Err(ProcessClientError::ProtocolScenario(
                    "worker disconnected before no database invocation deadline",
                ));
                }
                Some(Err(_)) => {
                return Err(ProcessClientError::ProtocolScenario(
                    "fake database engine receive failed before no database invocation deadline",
                ));
                }
                Some(Ok(frame)) => frame,
            },
        };
        match frame {
            Message::Ping(payload) => {
                socket.send(Message::Pong(payload)).await.map_err(|_| {
                    ProcessClientError::ProtocolScenario(
                        "fake database engine could not answer ping",
                    )
                })?;
            }
            Message::Pong(_) => {}
            Message::Text(text) => {
                let message = serde_json::from_str::<Value>(text.as_str()).unwrap_or(Value::Null);
                return Err(ProcessClientError::ProtocolScenario(
                    if message["function_id"] == ROUTER_EMBED_FUNCTION_ID {
                        "invalid memory request invoked router::embed"
                    } else {
                        "invalid memory request invoked the database engine"
                    },
                ));
            }
            Message::Close(_) | Message::Binary(_) | Message::Frame(_) => {
                return Err(ProcessClientError::ProtocolScenario(
                    "worker closed before no database invocation deadline",
                ));
            }
        }
    }
}

async fn wait_for_engine_disconnect(
    socket: &mut WebSocketStream<TcpStream>,
    timeout_duration: Duration,
) -> FakeResult {
    wait_for_engine_disconnect_rejecting(socket, timeout_duration, |_| UNEXPECTED_SHUTDOWN_MESSAGE)
        .await
}

/// Waits for the worker to disconnect; `unexpected_text` names the failure for a text frame.
async fn wait_for_engine_disconnect_rejecting(
    socket: &mut WebSocketStream<TcpStream>,
    timeout_duration: Duration,
    unexpected_text: impl Fn(&str) -> &'static str,
) -> FakeResult {
    loop {
        let frame = timeout(timeout_duration, socket.next())
            .await
            .map_err(|_| {
                ProcessClientError::ProtocolScenario(
                    "worker did not close fake database engine connection",
                )
            })?;
        let Some(frame) = frame else {
            return Ok(());
        };
        let frame = match frame {
            Ok(frame) => frame,
            Err(_) => return Ok(()),
        };
        match frame {
            Message::Ping(payload) => {
                socket.send(Message::Pong(payload)).await.map_err(|_| {
                    ProcessClientError::ProtocolScenario(
                        "fake database engine could not answer ping",
                    )
                })?;
            }
            Message::Pong(_) => {}
            Message::Close(_) => return Ok(()),
            Message::Text(text) => {
                return Err(ProcessClientError::ProtocolScenario(unexpected_text(
                    text.as_str(),
                )));
            }
            Message::Binary(_) | Message::Frame(_) => {
                return Err(ProcessClientError::ProtocolScenario(
                    UNEXPECTED_SHUTDOWN_MESSAGE,
                ));
            }
        }
    }
}

fn validate_managed_worker_registration(message: &Value) -> FakeResult {
    let registration = message
        .as_object()
        .ok_or(ProcessClientError::ProtocolScenario(
            "worker registration was not an object",
        ))?;
    require(
        has_exact_keys(
            registration,
            &["type", "invocation_id", "function_id", "data", "action"],
        ),
        "worker registration had an unexpected shape",
    )?;
    require(
        message["type"] == "invokefunction"
            && message["function_id"] == WORKER_REGISTER_FUNCTION_ID
            && message["invocation_id"].is_null()
            && message["data"].is_object()
            && message["action"] == json!({ "type": "void" }),
        "worker registration did not use the managed worker protocol",
    )
}

async fn capture_database_invocation(
    socket: &mut WebSocketStream<TcpStream>,
    timeout_duration: Duration,
) -> FakeResult<(String, CapturedDatabaseInvocation)> {
    let message = next_engine_message(socket, timeout_duration).await?;
    parse_database_invocation(&message, None)
}

/// Parses one `database::execute` frame; a namespaced worker must send its namespace.
fn parse_database_invocation(
    message: &Value,
    namespace: Option<&str>,
) -> FakeResult<(String, CapturedDatabaseInvocation)> {
    let invocation = message
        .as_object()
        .ok_or(ProcessClientError::ProtocolScenario(
            "database invocation was not an object",
        ))?;
    let expected_keys: &[&str] = match namespace {
        Some(_) => &["type", "invocation_id", "function_id", "data", "namespace"],
        None => &["type", "invocation_id", "function_id", "data"],
    };
    require(
        has_exact_keys(invocation, expected_keys),
        "database invocation had an unexpected shape",
    )?;
    require(
        message["type"] == "invokefunction"
            && message["function_id"] == DATABASE_EXECUTE_FUNCTION_ID,
        "worker did not invoke database::execute",
    )?;
    require(
        namespace.is_none_or(|namespace| message["namespace"] == namespace),
        "database invocation did not resolve in the managed worker namespace",
    )?;
    let invocation_id = uuid_invocation_id(
        message,
        "database invocation did not use a UUID invocation ID",
    )?;
    let data = message["data"]
        .as_object()
        .ok_or(ProcessClientError::ProtocolScenario(
            "database invocation data was not an object",
        ))?;
    require(
        has_exact_keys(data, &["db", "sql", "params"]),
        "database invocation data had an unexpected shape",
    )?;
    let database = data["db"]
        .as_str()
        .ok_or(ProcessClientError::ProtocolScenario(
            "database invocation did not name a database target",
        ))?
        .to_owned();
    let sql = data["sql"]
        .as_str()
        .ok_or(ProcessClientError::ProtocolScenario(
            "database invocation did not include SQL",
        ))?
        .to_owned();
    let params = data["params"]
        .as_array()
        .ok_or(ProcessClientError::ProtocolScenario(
            "database invocation did not include positional parameters",
        ))?
        .to_vec();

    Ok((
        invocation_id,
        CapturedDatabaseInvocation {
            database,
            sql,
            params,
        },
    ))
}

fn uuid_invocation_id(message: &Value, error: &'static str) -> FakeResult<String> {
    message["invocation_id"]
        .as_str()
        .filter(|value| Uuid::parse_str(value).is_ok())
        .map(str::to_owned)
        .ok_or(ProcessClientError::ProtocolScenario(error))
}

async fn capture_combined_search_invocations(
    socket: &mut WebSocketStream<TcpStream>,
    mode: SearchEngineMode,
    timeout_duration: Duration,
) -> FakeResult<CapturedCombinedSearchInvocations> {
    let ConcurrentSearchStart {
        router_invocation_id,
        router,
        lexical_invocation_id,
        lexical,
    } = capture_concurrent_search_start(socket, timeout_duration).await?;

    send_router_embed_reply(socket, mode, &router_invocation_id).await?;
    send_search_database_reply(
        socket,
        mode,
        SearchOperation::Lexical,
        &lexical_invocation_id,
    )
    .await?;
    let (vector_invocation_id, vector) = parse_database_invocation(
        &next_expected_engine_message(
            socket,
            timeout_duration,
            "enabled memory search did not issue vector search after valid query embedding generation",
        )
        .await?,
        Some(SEARCH_NAMESPACE),
    )?;
    require(
        matches!(search_operation(&vector)?, SearchOperation::Vector),
        "enabled memory search did not follow query embedding generation with the static vector query",
    )?;
    send_search_database_reply(socket, mode, SearchOperation::Vector, &vector_invocation_id)
        .await?;

    Ok(CapturedCombinedSearchInvocations {
        router,
        lexical,
        vector,
    })
}

async fn capture_lexical_fallback_invocations(
    socket: &mut WebSocketStream<TcpStream>,
    mode: SearchEngineMode,
    timeout_duration: Duration,
) -> FakeResult<CapturedLexicalFallbackInvocations> {
    let invocations = if mode.query_embedding_enabled() {
        let start = capture_concurrent_search_start(socket, timeout_duration).await?;
        send_router_embed_reply(socket, mode, &start.router_invocation_id).await?;
        send_search_database_reply(
            socket,
            mode,
            SearchOperation::Lexical,
            &start.lexical_invocation_id,
        )
        .await?;
        CapturedLexicalFallbackInvocations {
            router: Some(start.router),
            lexical: start.lexical,
        }
    } else {
        let (lexical_invocation_id, lexical) = lexical_fallback_invocation(
            &next_expected_engine_message(
                socket,
                timeout_duration,
                "disabled query embedding did not start the BM25 search",
            )
            .await?,
            mode,
        )?;
        send_search_database_reply(
            socket,
            mode,
            SearchOperation::Lexical,
            &lexical_invocation_id,
        )
        .await?;
        CapturedLexicalFallbackInvocations {
            router: None,
            lexical,
        }
    };

    // Every expected request has its reply, so a worker cannot finish the search while it awaits
    // a later router or vector request; the fake sees that request before any clean disconnect.
    wait_for_engine_disconnect_rejecting(socket, timeout_duration, |text| {
        unexpected_lexical_fallback_request(text, mode)
    })
    .await?;
    Ok(invocations)
}

/// Accepts only the BM25 search on the lexical-only path.
fn lexical_fallback_invocation(
    message: &Value,
    mode: SearchEngineMode,
) -> FakeResult<(String, CapturedDatabaseInvocation)> {
    if message["function_id"] == ROUTER_EMBED_FUNCTION_ID {
        return Err(ProcessClientError::ProtocolScenario(
            mode.unexpected_router_invocation(),
        ));
    }
    let (invocation_id, invocation) = parse_database_invocation(message, Some(SEARCH_NAMESPACE))?;
    match search_operation(&invocation)? {
        SearchOperation::Lexical => Ok((invocation_id, invocation)),
        SearchOperation::Vector => Err(ProcessClientError::ProtocolScenario(
            mode.unexpected_vector_search(),
        )),
    }
}

fn unexpected_lexical_fallback_request(text: &str, mode: SearchEngineMode) -> &'static str {
    let message = serde_json::from_str::<Value>(text).unwrap_or(Value::Null);
    if message["function_id"] == ROUTER_EMBED_FUNCTION_ID {
        mode.unexpected_router_invocation()
    } else if message["function_id"] != DATABASE_EXECUTE_FUNCTION_ID {
        "worker sent an unexpected fake database engine message during lexical fallback"
    } else {
        match message["data"]["sql"].as_str() {
            Some(SEARCH_VECTOR_SQL) => mode.unexpected_vector_search(),
            Some(SEARCH_BM25_SQL) => "lexical fallback repeated its BM25 search",
            _ => "lexical fallback invoked an unexpected database operation",
        }
    }
}

/// Withholds every reply until BM25 and generation have both started, so a worker that awaits
/// either one before sending the other never sends the second request.
async fn capture_concurrent_search_start(
    socket: &mut WebSocketStream<TcpStream>,
    timeout_duration: Duration,
) -> FakeResult<ConcurrentSearchStart> {
    let first = concurrent_search_invocation(
        &next_expected_engine_message(
            socket,
            timeout_duration,
            "enabled memory search did not start BM25 or query embedding generation",
        )
        .await?,
    )?;
    let second = concurrent_search_invocation(
        &next_expected_engine_message(
            socket,
            timeout_duration,
            "enabled memory search did not start BM25 and query embedding generation concurrently",
        )
        .await?,
    )?;
    match (first, second) {
        (
            ConcurrentSearchInvocation::Router(router_invocation_id, router),
            ConcurrentSearchInvocation::Lexical(lexical_invocation_id, lexical),
        )
        | (
            ConcurrentSearchInvocation::Lexical(lexical_invocation_id, lexical),
            ConcurrentSearchInvocation::Router(router_invocation_id, router),
        ) => Ok(ConcurrentSearchStart {
            router_invocation_id,
            router,
            lexical_invocation_id,
            lexical,
        }),
        _ => Err(ProcessClientError::ProtocolScenario(
            "enabled memory search did not send exactly one BM25 search and one router::embed call",
        )),
    }
}

fn concurrent_search_invocation(message: &Value) -> FakeResult<ConcurrentSearchInvocation> {
    if message["function_id"] == ROUTER_EMBED_FUNCTION_ID {
        let (invocation_id, invocation) = parse_router_invocation(message)?;
        return Ok(ConcurrentSearchInvocation::Router(
            invocation_id,
            invocation,
        ));
    }
    let (invocation_id, invocation) = parse_database_invocation(message, Some(SEARCH_NAMESPACE))?;
    match search_operation(&invocation)? {
        SearchOperation::Lexical => Ok(ConcurrentSearchInvocation::Lexical(
            invocation_id,
            invocation,
        )),
        SearchOperation::Vector => Err(ProcessClientError::ProtocolScenario(
            "enabled memory search issued vector search before query embedding generation completed",
        )),
    }
}

fn parse_router_invocation(message: &Value) -> FakeResult<(String, CapturedRouterInvocation)> {
    let invocation = message
        .as_object()
        .ok_or(ProcessClientError::ProtocolScenario(
            "router invocation was not an object",
        ))?;
    require(
        has_exact_keys(
            invocation,
            &["type", "invocation_id", "function_id", "data", "namespace"],
        ),
        "router invocation had an unexpected shape",
    )?;
    require(
        message["type"] == "invokefunction" && message["function_id"] == ROUTER_EMBED_FUNCTION_ID,
        "worker did not invoke router::embed",
    )?;
    require(
        message["namespace"] == SEARCH_NAMESPACE,
        "router::embed did not resolve in the managed worker namespace",
    )?;
    let invocation_id = uuid_invocation_id(
        message,
        "router invocation did not use a UUID invocation ID",
    )?;

    Ok((
        invocation_id,
        CapturedRouterInvocation {
            data: message["data"].clone(),
        },
    ))
}

/// Answers every request a failed search starts, so the worker holds each candidate set before
/// it returns its error. BM25 settles before generation, so a worker that abandons generation
/// after a BM25 failure never sends the chained vector search this path requires.
async fn capture_failed_search_invocations(
    socket: &mut WebSocketStream<TcpStream>,
    mode: SearchEngineMode,
    timeout_duration: Duration,
) -> FakeResult<CapturedFailedSearchInvocations> {
    let invocations = if mode.query_embedding_enabled() {
        let start = capture_concurrent_search_start(socket, timeout_duration).await?;
        send_search_database_reply(
            socket,
            mode,
            SearchOperation::Lexical,
            &start.lexical_invocation_id,
        )
        .await?;
        send_router_embed_reply(socket, mode, &start.router_invocation_id).await?;
        let (vector_invocation_id, vector) = parse_database_invocation(
            &next_expected_engine_message(
                socket,
                timeout_duration,
                "memory search did not issue vector search after valid query embedding generation",
            )
            .await?,
            Some(SEARCH_NAMESPACE),
        )?;
        require(
            matches!(search_operation(&vector)?, SearchOperation::Vector),
            "memory search did not follow valid query embedding generation with the static vector query",
        )?;
        send_search_database_reply(socket, mode, SearchOperation::Vector, &vector_invocation_id)
            .await?;
        CapturedFailedSearchInvocations {
            lexical: start.lexical,
            semantic: Some((start.router, vector)),
        }
    } else {
        let (lexical_invocation_id, lexical) = lexical_fallback_invocation(
            &next_expected_engine_message(
                socket,
                timeout_duration,
                "disabled query embedding did not start the BM25 search",
            )
            .await?,
            mode,
        )?;
        send_search_database_reply(
            socket,
            mode,
            SearchOperation::Lexical,
            &lexical_invocation_id,
        )
        .await?;
        CapturedFailedSearchInvocations {
            lexical,
            semantic: None,
        }
    };

    wait_for_engine_disconnect_rejecting(socket, timeout_duration, |text| {
        unexpected_failed_search_request(text, mode)
    })
    .await?;
    Ok(invocations)
}

fn unexpected_failed_search_request(text: &str, mode: SearchEngineMode) -> &'static str {
    let message = serde_json::from_str::<Value>(text).unwrap_or(Value::Null);
    if message["function_id"] == ROUTER_EMBED_FUNCTION_ID {
        mode.unexpected_router_invocation()
    } else if message["function_id"] != DATABASE_EXECUTE_FUNCTION_ID {
        "worker sent an unexpected fake database engine message after a failed search"
    } else {
        match message["data"]["sql"].as_str() {
            Some(SEARCH_VECTOR_SQL) => mode.unexpected_vector_search(),
            Some(SEARCH_BM25_SQL) => "failed memory search retried its BM25 search",
            _ => "failed memory search invoked an unexpected database operation",
        }
    }
}

fn search_operation(invocation: &CapturedDatabaseInvocation) -> FakeResult<SearchOperation> {
    match invocation.sql.as_str() {
        SEARCH_BM25_SQL => Ok(SearchOperation::Lexical),
        SEARCH_VECTOR_SQL => Ok(SearchOperation::Vector),
        _ => Err(ProcessClientError::ProtocolScenario(
            "memory search did not use a static BM25 or vector query",
        )),
    }
}

async fn capture_version_list_invocation(
    socket: &mut WebSocketStream<TcpStream>,
    timeout_duration: Duration,
) -> FakeResult<(String, CapturedDatabaseInvocation)> {
    let (invocation_id, invocation) = capture_database_invocation(socket, timeout_duration).await?;
    require(
        invocation.sql == VERSION_LIST_SQL,
        "memory version listing did not use the static version-list query",
    )?;
    Ok((invocation_id, invocation))
}

async fn capture_retrieval_invocation(
    socket: &mut WebSocketStream<TcpStream>,
    timeout_duration: Duration,
    operation: RetrievalOperation,
) -> FakeResult<(String, CapturedDatabaseInvocation)> {
    let (invocation_id, invocation) = capture_database_invocation(socket, timeout_duration).await?;
    require(
        invocation.sql == operation.sql(),
        "memory retrieval did not use its static canonical read query",
    )?;
    Ok((invocation_id, invocation))
}

async fn send_database_reply(
    socket: &mut WebSocketStream<TcpStream>,
    mode: SaveEngineMode,
    invocation_id: &str,
    invocation: &CapturedDatabaseInvocation,
) -> FakeResult {
    let message = match mode {
        SaveEngineMode::InsertSuccess => json!({
            "type": "invocationresult",
            "invocation_id": invocation_id,
            "function_id": DATABASE_EXECUTE_FUNCTION_ID,
            "result": successful_memory_insert_result(invocation)?,
        }),
        SaveEngineMode::InsertConflict => json!({
            "type": "invocationresult",
            "invocation_id": invocation_id,
            "function_id": DATABASE_EXECUTE_FUNCTION_ID,
            "result": {
                "affected_rows": 0,
                "last_insert_id": null,
                "returned_rows": [],
            },
        }),
        SaveEngineMode::BackendFailure => json!({
            "type": "invocationresult",
            "invocation_id": invocation_id,
            "function_id": DATABASE_EXECUTE_FUNCTION_ID,
            "error": {
                "code": ENGINE_ERROR_CODE_SENTINEL,
                "message": ENGINE_ERROR_MESSAGE_SENTINEL,
                "stacktrace": ENGINE_ERROR_STACKTRACE_SENTINEL,
            },
        }),
        SaveEngineMode::NoDatabaseInvocation => {
            return Err(ProcessClientError::ProtocolScenario(
                "fake database engine tried to reply to an absent invocation",
            ));
        }
    };
    send_engine_message(socket, message).await
}

async fn send_router_embed_reply(
    socket: &mut WebSocketStream<TcpStream>,
    mode: SearchEngineMode,
    invocation_id: &str,
) -> FakeResult {
    let message = match mode {
        SearchEngineMode::CombinedSuccess
        | SearchEngineMode::CombinedEmpty
        | SearchEngineMode::LexicalBackendFailure
        | SearchEngineMode::InvalidVectorSearchResponse => json!({
            "type": "invocationresult",
            "invocation_id": invocation_id,
            "function_id": ROUTER_EMBED_FUNCTION_ID,
            "result": {
                "provider": SEARCH_EMBEDDING_PROVIDER_SENTINEL,
                "model": SEARCH_EMBEDDING_MODEL_SENTINEL,
                "embeddings": [SEARCH_GENERATED_VECTOR],
            },
        }),
        SearchEngineMode::RouterFailureFallback => json!({
            "type": "invocationresult",
            "invocation_id": invocation_id,
            "function_id": ROUTER_EMBED_FUNCTION_ID,
            "error": {
                "code": SEARCH_ROUTER_ERROR_CODE_SENTINEL,
                "message": format!(
                    "{SEARCH_ROUTER_ERROR_MESSAGE_SENTINEL} authorization: Bearer {SEARCH_ROUTER_CREDENTIAL_SENTINEL}"
                ),
                "stacktrace": SEARCH_ROUTER_ERROR_STACKTRACE_SENTINEL,
            },
        }),
        // The vector is canonical, so only the resolved-model check keeps it from vector search.
        SearchEngineMode::InvalidRouterResponseFallback => json!({
            "type": "invocationresult",
            "invocation_id": invocation_id,
            "function_id": ROUTER_EMBED_FUNCTION_ID,
            "result": {
                "provider": SEARCH_EMBEDDING_PROVIDER_SENTINEL,
                "model": SEARCH_ROUTER_RESOLVED_MODEL_SENTINEL,
                "embeddings": [SEARCH_ROUTER_INVALID_VECTOR],
            },
        }),
        SearchEngineMode::DisabledEmbeddingFallback
        | SearchEngineMode::DisabledEmbeddingEmptyFallback
        | SearchEngineMode::DisabledEmbeddingLexicalFailure
        | SearchEngineMode::NoDatabaseInvocation => {
            return Err(ProcessClientError::ProtocolScenario(
                "fake database engine tried to answer router::embed in a scenario that forbids it",
            ));
        }
    };
    send_engine_message(socket, message).await
}

async fn send_search_database_reply(
    socket: &mut WebSocketStream<TcpStream>,
    mode: SearchEngineMode,
    operation: SearchOperation,
    invocation_id: &str,
) -> FakeResult {
    let failure = matches!(
        (mode, operation),
        (
            SearchEngineMode::DisabledEmbeddingLexicalFailure
                | SearchEngineMode::LexicalBackendFailure,
            SearchOperation::Lexical
        )
    );
    let message = if failure {
        json!({
            "type": "invocationresult",
            "invocation_id": invocation_id,
            "function_id": DATABASE_EXECUTE_FUNCTION_ID,
            "error": {
                "code": SEARCH_ENGINE_ERROR_CODE_SENTINEL,
                "message": SEARCH_ENGINE_ERROR_MESSAGE_SENTINEL,
                "stacktrace": SEARCH_ENGINE_ERROR_STACKTRACE_SENTINEL,
            },
        })
    } else {
        json!({
            "type": "invocationresult",
            "invocation_id": invocation_id,
            "function_id": DATABASE_EXECUTE_FUNCTION_ID,
            "result": search_database_result(mode, operation)?,
        })
    };
    send_engine_message(socket, message).await
}

async fn send_version_list_database_reply(
    socket: &mut WebSocketStream<TcpStream>,
    mode: VersionListEngineMode,
    invocation_id: &str,
) -> FakeResult {
    let message = match mode {
        VersionListEngineMode::DefaultPage
        | VersionListEngineMode::ContinuedPage
        | VersionListEngineMode::EmptyPage => json!({
            "type": "invocationresult",
            "invocation_id": invocation_id,
            "function_id": DATABASE_EXECUTE_FUNCTION_ID,
            "result": version_list_database_result(mode)?,
        }),
        VersionListEngineMode::BackendFailure => json!({
            "type": "invocationresult",
            "invocation_id": invocation_id,
            "function_id": DATABASE_EXECUTE_FUNCTION_ID,
            "error": {
                "code": VERSION_LIST_ENGINE_ERROR_CODE_SENTINEL,
                "message": VERSION_LIST_ENGINE_ERROR_MESSAGE_SENTINEL,
                "stacktrace": VERSION_LIST_ENGINE_ERROR_STACKTRACE_SENTINEL,
            },
        }),
        VersionListEngineMode::NoDatabaseInvocation => {
            return Err(ProcessClientError::ProtocolScenario(
                "fake database engine tried to reply to an absent version-list invocation",
            ));
        }
    };
    send_engine_message(socket, message).await
}

async fn send_retrieval_database_reply(
    socket: &mut WebSocketStream<TcpStream>,
    mode: RetrievalEngineMode,
    invocation_id: &str,
) -> FakeResult {
    let message = match mode {
        RetrievalEngineMode::ExactSuccess | RetrievalEngineMode::LatestSuccess => json!({
            "type": "invocationresult",
            "invocation_id": invocation_id,
            "function_id": DATABASE_EXECUTE_FUNCTION_ID,
            "result": retrieval_database_result(mode)?,
        }),
        RetrievalEngineMode::ExactNotFound | RetrievalEngineMode::LatestNotFound => json!({
            "type": "invocationresult",
            "invocation_id": invocation_id,
            "function_id": DATABASE_EXECUTE_FUNCTION_ID,
            "result": {
                "affected_rows": 0,
                "last_insert_id": null,
                "returned_rows": [],
            },
        }),
        RetrievalEngineMode::ExactMalformed | RetrievalEngineMode::LatestMalformed => json!({
            "type": "invocationresult",
            "invocation_id": invocation_id,
            "function_id": DATABASE_EXECUTE_FUNCTION_ID,
            "result": malformed_retrieval_database_result(mode)?,
        }),
        RetrievalEngineMode::ExactBackendFailure | RetrievalEngineMode::LatestBackendFailure => {
            json!({
                "type": "invocationresult",
                "invocation_id": invocation_id,
                "function_id": DATABASE_EXECUTE_FUNCTION_ID,
                "error": {
                    "code": RETRIEVAL_ENGINE_ERROR_CODE_SENTINEL,
                    "message": RETRIEVAL_ENGINE_ERROR_MESSAGE_SENTINEL,
                    "stacktrace": RETRIEVAL_ENGINE_ERROR_STACKTRACE_SENTINEL,
                },
            })
        }
        RetrievalEngineMode::NoDatabaseInvocation => {
            return Err(ProcessClientError::ProtocolScenario(
                "fake database engine tried to reply to an absent retrieval invocation",
            ));
        }
    };
    send_engine_message(socket, message).await
}

fn search_database_result(mode: SearchEngineMode, operation: SearchOperation) -> FakeResult<Value> {
    let rows = match (mode, operation) {
        (SearchEngineMode::CombinedEmpty, _)
        | (SearchEngineMode::DisabledEmbeddingEmptyFallback, SearchOperation::Lexical) => {
            Vec::new()
        }
        (
            SearchEngineMode::DisabledEmbeddingFallback
            | SearchEngineMode::RouterFailureFallback
            | SearchEngineMode::InvalidRouterResponseFallback,
            SearchOperation::Lexical,
        ) => lexical_fallback_rows(),
        (
            SearchEngineMode::DisabledEmbeddingFallback
            | SearchEngineMode::DisabledEmbeddingEmptyFallback
            | SearchEngineMode::RouterFailureFallback
            | SearchEngineMode::InvalidRouterResponseFallback
            | SearchEngineMode::DisabledEmbeddingLexicalFailure,
            SearchOperation::Vector,
        ) => {
            return Err(ProcessClientError::ProtocolScenario(
                "fake database engine tried to answer vector search on the lexical-only path",
            ));
        }
        (SearchEngineMode::InvalidVectorSearchResponse, SearchOperation::Vector) => {
            malformed_vector_search_rows()
        }
        (_, SearchOperation::Lexical) => lexical_search_rows(),
        (_, SearchOperation::Vector) => vector_search_rows(),
    };
    Ok(json!({
        "affected_rows": rows.len(),
        "last_insert_id": null,
        "returned_rows": rows,
    }))
}

fn version_list_database_result(mode: VersionListEngineMode) -> FakeResult<Value> {
    let rows = match mode {
        VersionListEngineMode::DefaultPage => {
            version_list_rows(i64::from(VERSION_LIST_DEFAULT_LIMIT) + 1, 51)
        }
        VersionListEngineMode::ContinuedPage => version_list_rows(9, 3),
        VersionListEngineMode::EmptyPage => Vec::new(),
        VersionListEngineMode::BackendFailure | VersionListEngineMode::NoDatabaseInvocation => {
            return Err(ProcessClientError::ProtocolScenario(
                "fake database engine built an invalid version-list result",
            ));
        }
    };
    Ok(json!({
        "affected_rows": rows.len(),
        "last_insert_id": null,
        "returned_rows": rows,
    }))
}

fn retrieval_database_result(mode: RetrievalEngineMode) -> FakeResult<Value> {
    let operation = mode
        .operation()
        .ok_or(ProcessClientError::ProtocolScenario(
            "fake database engine built an invalid retrieval result",
        ))?;
    let rows = vec![retrieval_memory_row(operation)];
    Ok(json!({
        "affected_rows": rows.len(),
        "last_insert_id": null,
        "returned_rows": rows,
    }))
}

fn malformed_retrieval_database_result(mode: RetrievalEngineMode) -> FakeResult<Value> {
    let operation = mode
        .operation()
        .ok_or(ProcessClientError::ProtocolScenario(
            "fake database engine built an invalid malformed retrieval result",
        ))?;
    let mut row = retrieval_memory_row(operation);
    row["version"] = json!("not-a-version");
    Ok(json!({
        "affected_rows": 1,
        "last_insert_id": null,
        "returned_rows": [row],
    }))
}

fn retrieval_memory_row(operation: RetrievalOperation) -> Value {
    let (version, marker, created_at, updated_at) = retrieval_memory_fixture(operation);
    json!({
        "id": RETRIEVAL_ID_SENTINEL,
        "version": version.to_string(),
        "memory_type": format!("{RETRIEVAL_RESULT_SENTINEL} {marker} type"),
        "title": format!("{RETRIEVAL_RESULT_SENTINEL} {marker} title"),
        "content": format!("{RETRIEVAL_RESULT_SENTINEL} {marker} content"),
        "created_at": created_at,
        "updated_at": updated_at,
        "concepts": [format!("{RETRIEVAL_RESULT_SENTINEL} {marker} concept")],
        "files": [format!("{RETRIEVAL_RESULT_SENTINEL} {marker} file")],
        "session_ids": [format!("{RETRIEVAL_RESULT_SENTINEL} {marker} session")],
        "source_observation_ids": [format!("{RETRIEVAL_RESULT_SENTINEL} {marker} observation")],
    })
}

fn retrieval_memory_dto(operation: RetrievalOperation) -> Value {
    let (version, marker, created_at, updated_at) = retrieval_memory_fixture(operation);
    json!({
        "id": RETRIEVAL_ID_SENTINEL,
        "version": version,
        "type": format!("{RETRIEVAL_RESULT_SENTINEL} {marker} type"),
        "title": format!("{RETRIEVAL_RESULT_SENTINEL} {marker} title"),
        "content": format!("{RETRIEVAL_RESULT_SENTINEL} {marker} content"),
        "created_at": created_at,
        "updated_at": updated_at,
        "concepts": [format!("{RETRIEVAL_RESULT_SENTINEL} {marker} concept")],
        "files": [format!("{RETRIEVAL_RESULT_SENTINEL} {marker} file")],
        "session_ids": [format!("{RETRIEVAL_RESULT_SENTINEL} {marker} session")],
        "source_observation_ids": [format!("{RETRIEVAL_RESULT_SENTINEL} {marker} observation")],
    })
}

fn retrieval_memory_fixture(
    operation: RetrievalOperation,
) -> (i64, &'static str, &'static str, &'static str) {
    match operation {
        RetrievalOperation::Exact => (
            RETRIEVAL_EXACT_VERSION,
            "exact",
            RETRIEVAL_EXACT_CREATED_AT,
            RETRIEVAL_EXACT_UPDATED_AT,
        ),
        RetrievalOperation::Latest => (
            RETRIEVAL_LATEST_VERSION,
            "latest",
            RETRIEVAL_LATEST_CREATED_AT,
            RETRIEVAL_LATEST_UPDATED_AT,
        ),
    }
}

fn version_list_rows(first_version: i64, count: usize) -> Vec<Value> {
    (0..count)
        .map(|index| version_list_row(first_version - index as i64))
        .collect()
}

fn version_list_row(version: i64) -> Value {
    json!({
        "version": version.to_string(),
        "updated_at": VERSION_LIST_UPDATED_AT,
    })
}

fn lexical_search_rows() -> Vec<Value> {
    vec![
        search_row("search-id-z", 1, "lexical-z-v1", 0.99),
        search_row("search-id-b", 1, "lexical-b-v1", 0.88),
        search_row(
            SEARCH_TIE_ID,
            SEARCH_TIE_VERSION,
            SEARCH_LEXICAL_TIE_MARKER,
            0.10,
        ),
        search_row("search-id-c", 2, "lexical-c-v2", 0.01),
        search_row(
            SEARCH_TIE_ID,
            SEARCH_TIE_VERSION,
            SEARCH_LEXICAL_TIE_DUPLICATE_MARKER,
            0.005,
        ),
    ]
}

fn vector_search_rows() -> Vec<Value> {
    vec![
        search_row(
            SEARCH_TIE_ID,
            SEARCH_TIE_VERSION,
            SEARCH_VECTOR_TIE_MARKER,
            0.99,
        ),
        search_row("search-id-c", 1, "vector-c-v1", 0.95),
        search_row("search-id-a", 1, "vector-a-v1", 0.75),
        search_row("search-id-y", 1, "vector-y-v1", 0.50),
        search_row("search-id-b", 3, "vector-b-v3", 0.02),
    ]
}

/// Fills the vector page but gives its last row a non-numeric version, so the memory store
/// rejects the whole page after decoding the earlier rows.
fn malformed_vector_search_rows() -> Vec<Value> {
    let mut rows = vector_search_rows();
    if let Some(row) = rows.last_mut() {
        row["version"] = json!("not-a-version");
    }
    rows
}

/// Fills the requested limit, which is the largest BM25 page the memory store accepts.
fn lexical_fallback_rows() -> Vec<Value> {
    vec![
        search_row(
            SEARCH_FALLBACK_TIE_ID,
            SEARCH_FALLBACK_TIE_VERSION,
            SEARCH_FALLBACK_TIE_MARKER,
            0.9,
        ),
        search_row("search-id-r", 1, "fallback-lexical-r-v1", 0.8),
        search_row("search-id-e", 1, "fallback-lexical-e-v1", 0.7),
        search_row(
            SEARCH_FALLBACK_TIE_ID,
            SEARCH_FALLBACK_TIE_VERSION,
            SEARCH_FALLBACK_TIE_DUPLICATE_MARKER,
            0.6,
        ),
        search_row("search-id-r", 4, "fallback-lexical-r-v4", 0.5),
        search_row(SEARCH_FALLBACK_TIE_ID, 2, "fallback-lexical-k-v2", 0.4),
    ]
}

fn search_row(id: &str, version: i64, marker: &str, relevance: f64) -> Value {
    let (created_at, updated_at) = search_timestamps(marker);
    json!({
        "id": id,
        "version": version.to_string(),
        "memory_type": format!("{SEARCH_RESULT_SENTINEL} {marker} type"),
        "title": format!("{SEARCH_RESULT_SENTINEL} {marker} title"),
        "content": format!("{SEARCH_RESULT_SENTINEL} {marker} content"),
        "created_at": created_at,
        "updated_at": updated_at,
        "concepts": [format!("{SEARCH_RESULT_SENTINEL} {marker} concept")],
        "files": [format!("{SEARCH_RESULT_SENTINEL} {marker} file")],
        "session_ids": [format!("{SEARCH_RESULT_SENTINEL} {marker} session")],
        "source_observation_ids": [format!("{SEARCH_RESULT_SENTINEL} {marker} observation")],
        "relevance": relevance,
    })
}

fn search_timestamps(marker: &str) -> (&'static str, &'static str) {
    match marker {
        SEARCH_LEXICAL_TIE_MARKER => ("2026-09-21T01:02:03Z", "2026-09-21T01:02:04Z"),
        SEARCH_LEXICAL_TIE_DUPLICATE_MARKER => ("2026-09-21T02:03:04Z", "2026-09-21T02:03:05Z"),
        SEARCH_VECTOR_TIE_MARKER => ("2026-09-21T03:04:05Z", "2026-09-21T03:04:06Z"),
        SEARCH_FALLBACK_TIE_MARKER => ("2026-09-21T04:05:06Z", "2026-09-21T04:05:07Z"),
        SEARCH_FALLBACK_TIE_DUPLICATE_MARKER => ("2026-09-21T05:06:07Z", "2026-09-21T05:06:08Z"),
        _ => ("2026-09-21T00:00:00Z", "2026-09-21T00:00:00Z"),
    }
}

fn successful_memory_insert_result(invocation: &CapturedDatabaseInvocation) -> FakeResult<Value> {
    let id = database_parameter(invocation, 0)?;
    let version = database_parameter(invocation, 1)?;
    Ok(json!({
        "affected_rows": 1,
        "last_insert_id": id,
        "returned_rows": [{
            "id": id,
            "version": version,
        }],
    }))
}

async fn send_engine_message(
    socket: &mut WebSocketStream<TcpStream>,
    message: Value,
) -> FakeResult {
    socket
        .send(Message::Text(message.to_string().into()))
        .await
        .map_err(|_| {
            ProcessClientError::ProtocolScenario("fake database engine could not send a response")
        })
}

fn has_exact_keys(object: &Map<String, Value>, expected: &[&str]) -> bool {
    object.len() == expected.len() && expected.iter().all(|key| object.contains_key(*key))
}

async fn assert_invalid_startup(executable: &Path, limits: ProcessLimits) -> FakeResult {
    let client = ChildProcessClient::start(
        WorkerInvocation {
            executable: executable.to_owned(),
        }
        .invalid_configuration_command(),
        limits,
    )
    .await?;
    let stderr = client.stop_unsuccessfully().await?;
    require(
        stderr.bytes == INVALID_STARTUP_STDERR.as_bytes(),
        "invalid startup did not write the stable stderr diagnostic",
    )
}

/// Each launch leaves exactly one of provider or model non-blank; the other is absent, empty,
/// or whitespace-only. The request written at launch must go unanswered.
async fn assert_partial_embedding_startup(executable: &Path, limits: ProcessLimits) -> FakeResult {
    let invocation = WorkerInvocation {
        executable: executable.to_owned(),
    };
    for (provider, model) in [
        (Some(STARTUP_EMBEDDING_PROVIDER_SENTINEL), None),
        (None, Some(STARTUP_EMBEDDING_MODEL_SENTINEL)),
        (
            Some(STARTUP_EMBEDDING_PROVIDER_SENTINEL),
            Some(BLANK_EMBEDDING_IDENTIFIER),
        ),
        (Some(""), Some(STARTUP_EMBEDDING_MODEL_SENTINEL)),
    ] {
        let mut client = ChildProcessClient::start(
            invocation.embedding_identity_command(provider, model),
            limits,
        )
        .await?;
        client
            .offer_request_before_exit(&discover_request("partial-embedding-startup"))
            .await?;
        let stderr = client.stop_unsuccessfully().await?;
        require(
            [
                STARTUP_EMBEDDING_PROVIDER_SENTINEL,
                STARTUP_EMBEDDING_MODEL_SENTINEL,
            ]
            .iter()
            .all(|value| !stderr.contains(value.as_bytes())),
            "partial embedding startup diagnostics exposed a configured provider or model value",
        )?;
        require(
            stderr.bytes == PARTIAL_EMBEDDING_STARTUP_STDERR.as_bytes(),
            "partial embedding startup did not write the stable stderr diagnostic",
        )?;
    }
    Ok(())
}

/// Absent and blank provider and model settings both select lexical-only startup.
async fn assert_disabled_embedding_startup(executable: &Path, limits: ProcessLimits) -> FakeResult {
    let invocation = WorkerInvocation {
        executable: executable.to_owned(),
    };
    for (provider, model) in [(None, None), (Some(""), Some(BLANK_EMBEDDING_IDENTIFIER))] {
        let mut client = ChildProcessClient::start(
            invocation.embedding_identity_command(provider, model),
            limits,
        )
        .await?;
        let response = client
            .request(&discover_request("disabled-embedding-startup"))
            .await?;
        if let Err(error) = assert_discovery_response(&response, "disabled-embedding-startup") {
            return client.finish_failed_request(error).await;
        }
        let stderr = client.stop_successfully().await?;
        assert_empty_stderr(&stderr)?;
    }
    Ok(())
}

async fn assert_discovery_and_protocol_errors(
    executable: &Path,
    limits: ProcessLimits,
) -> FakeResult {
    let mut client = start_worker(executable, limits).await?;

    let response = client.request(&discover_request("discover")).await?;
    assert_discovery_response(&response, "discover")?;

    let response = client.request(&tools_list_request("tools")).await?;
    assert_tool_registry_response(&response, "tools")?;

    let response = client
        .request(&json!({
            "jsonrpc": "2.0",
            "id": "missing-metadata",
            "method": "tools/call",
            "params": {
                "name": "memory_save",
                "arguments": {
                    "title": "private-title-sentinel",
                    "content": "private-content-sentinel",
                    "session_id": "private-session-sentinel",
                },
            },
        }))
        .await?;
    assert_protocol_error(
        &response,
        "missing-metadata",
        -32602,
        "invalid_protocol_metadata",
    )?;

    let response = client
        .request(&json!({
            "jsonrpc": "2.0",
            "id": "invalid-metadata",
            "method": "server/discover",
            "params": {
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION,
                    "io.modelcontextprotocol/clientCapabilities": [],
                },
            },
        }))
        .await?;
    assert_protocol_error(
        &response,
        "invalid-metadata",
        -32602,
        "invalid_protocol_metadata",
    )?;

    let response = client
        .request(&json!({
            "jsonrpc": "2.0",
            "id": "unsupported-version",
            "method": "server/discover",
            "params": {
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": "unsupported-version-private-sentinel",
                    "io.modelcontextprotocol/clientCapabilities": {},
                },
            },
        }))
        .await?;
    assert_protocol_error(
        &response,
        "unsupported-version",
        -32022,
        "unsupported_protocol_version",
    )?;

    let response = client
        .request(&json!({
            "jsonrpc": "2.0",
            "id": "unknown-method",
            "method": "unknown-method-private-sentinel",
            "params": { "_meta": request_meta() },
        }))
        .await?;
    assert_protocol_error(&response, "unknown-method", -32601, "method_not_found")?;

    let response = client
        .request(&tool_request(
            "unknown-argument",
            "memory_save",
            json!({
                "title": "private-title-sentinel",
                "content": "private-content-sentinel",
                "session_id": "private-session-sentinel",
                "unexpected": "private-argument-sentinel",
            }),
        ))
        .await?;
    assert_tool_error(&response, "unknown-argument", "invalid_input")?;

    let stderr = client.stop_successfully().await?;
    assert_empty_stderr(&stderr)
}

async fn assert_oversized_input_recovers(executable: &Path, limits: ProcessLimits) -> FakeResult {
    let mut client = start_worker_with_bounds(
        executable,
        limits,
        WorkerBounds {
            max_line_bytes: SMALL_WORKER_MAX_LINE_BYTES,
            ..DEFAULT_WORKER_BOUNDS
        },
    )
    .await?;
    let oversized_frame = oversized_raw_test_frame(SMALL_WORKER_MAX_LINE_BYTES);
    client.send_raw_test_line(&oversized_frame).await?;

    let response = client.request(&discover_request("after-oversize")).await?;
    assert_discovery_response(&response, "after-oversize")?;
    require(
        !response.to_string().contains(PRIVATE_OVERSIZE_SENTINEL),
        "worker stdout leaked the oversized raw frame",
    )?;

    let stderr = client.stop_successfully().await?;
    require(
        !stderr.contains(PRIVATE_OVERSIZE_SENTINEL.as_bytes()),
        "worker stderr leaked the oversized raw frame",
    )?;
    assert_empty_stderr(&stderr)
}

async fn assert_bounded_output_flood_recovers(
    executable: &Path,
    limits: ProcessLimits,
) -> FakeResult {
    let mut client = start_worker_with_bounds(
        executable,
        limits,
        WorkerBounds {
            max_line_bytes: FLOOD_WORKER_MAX_LINE_BYTES,
            ..DEFAULT_WORKER_BOUNDS
        },
    )
    .await?;
    let probe = client.request(&discover_request("flood-probe")).await?;
    if let Err(error) = assert_discovery_response(&probe, "flood-probe") {
        return client.finish_failed_request(error).await;
    }

    let flood_id_padding = "x".repeat(FLOOD_REQUEST_ID_PADDING_BYTES);
    let requests = (0..FLOOD_REQUEST_COUNT)
        .map(|index| discover_request(&format!("flood-{index}-{flood_id_padding}")))
        .collect::<Vec<_>>();
    let mut expected_ids = requests
        .iter()
        .map(|request| {
            request["id"]
                .as_str()
                .expect("generated flood request must have a string ID")
                .to_owned()
        })
        .collect::<BTreeSet<_>>();

    let mut flood = client.start_bounded_request_flood(&requests).await?;
    if let Err(error) = flood.wait_until_pressure_observed().await {
        return terminate_after_flood_failure(client, flood, error).await;
    }
    if let Err(error) = client.assert_running() {
        return terminate_after_flood_failure(client, flood, error).await;
    }

    for _ in &requests {
        let response = match client.read_response().await {
            Ok(response) => response,
            Err(error) => {
                drop(flood);
                return Err(error);
            }
        };
        let response_id = match response["id"].as_str() {
            Some(response_id) => response_id,
            None => {
                return terminate_after_flood_failure(
                    client,
                    flood,
                    ProcessClientError::ProtocolScenario(
                        "flood response did not contain a string ID",
                    ),
                )
                .await;
            }
        };
        if let Err(error) = require(
            expected_ids.remove(response_id),
            "flood response ID was duplicated or unexpected",
        ) {
            return terminate_after_flood_failure(client, flood, error).await;
        }
        if let Err(error) = assert_discovery_response(&response, response_id) {
            return terminate_after_flood_failure(client, flood, error).await;
        }
    }
    if let Err(error) = require(
        expected_ids.is_empty(),
        "bounded flood did not return every request ID",
    ) {
        return terminate_after_flood_failure(client, flood, error).await;
    }
    if let Err(error) = flood.finish().await {
        return client.finish_failed_request(error).await;
    }

    let stderr = client.stop_successfully().await?;
    assert_empty_stderr(&stderr)
}

fn oversized_raw_test_frame(max_line_bytes: usize) -> Vec<u8> {
    let mut frame = PRIVATE_OVERSIZE_SENTINEL.as_bytes().to_vec();
    frame.resize(max_line_bytes + 1, b'x');
    frame
}

async fn terminate_after_flood_failure<T>(
    client: ChildProcessClient,
    flood: RequestFlood,
    error: ProcessClientError,
) -> FakeResult<T> {
    drop(flood);
    client.terminate().await?;
    Err(error)
}

async fn assert_eof_drains_response(executable: &Path, limits: ProcessLimits) -> FakeResult {
    let mut client = start_worker(executable, limits).await?;
    client
        .send_request(&discover_request("eof-discover"))
        .await?;
    client.close_stdin();
    let response = client.read_response().await?;
    assert_discovery_response(&response, "eof-discover")?;
    let stderr = client.stop_successfully().await?;
    assert_empty_stderr(&stderr)
}

#[cfg(unix)]
async fn assert_signal_shutdown(
    executable: &Path,
    limits: ProcessLimits,
    signal: &'static str,
) -> FakeResult {
    let mut client = start_worker(executable, limits).await?;
    let response = client
        .request(&discover_request(&format!("signal-{signal}")))
        .await?;
    assert_discovery_response(&response, &format!("signal-{signal}"))?;
    let stderr = client.stop_after_signal(signal).await?;
    assert_empty_stderr(&stderr)
}

fn discover_request(id: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "server/discover",
        "params": { "_meta": request_meta() },
    })
}

fn tools_list_request(id: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/list",
        "params": { "_meta": request_meta() },
    })
}

fn tool_request(id: &str, name: &str, arguments: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/call",
        "params": {
            "_meta": request_meta(),
            "name": name,
            "arguments": arguments,
        },
    })
}

fn request_meta() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION,
        "io.modelcontextprotocol/clientCapabilities": {},
    })
}

fn assert_discovery_response(response: &Value, id: &str) -> FakeResult {
    require(
        *response
            == json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "cacheScope": "public",
                    "capabilities": { "tools": {} },
                    "_meta": server_identity(),
                    "resultType": "complete",
                    "supportedVersions": [PROTOCOL_VERSION],
                    "ttlMs": 0,
                },
            }),
        "server discovery response did not advertise the expected tools-only identity",
    )
}

fn assert_tool_registry_response(response: &Value, id: &str) -> FakeResult {
    require(
        *response
            == json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "cacheScope": "public",
                    "_meta": server_identity(),
                    "resultType": "complete",
                    "tools": expected_tool_registry(),
                    "ttlMs": 0,
                },
            }),
        "tools/list response did not advertise the exact five-tool schema registry",
    )
}

fn assert_protocol_error(response: &Value, id: &str, code: i64, message: &str) -> FakeResult {
    require(
        *response
            == json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": code, "message": message },
            }),
        "protocol request did not return the expected content-safe error",
    )
}

fn assert_tool_error(response: &Value, id: &str, code: &str) -> FakeResult {
    require(
        *response
            == json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "content": [{ "type": "text", "text": code }],
                    "isError": true,
                    "resultType": "complete",
                },
            }),
        "invalid tool arguments did not return the expected content-safe error",
    )
}

fn assert_failed_memory_search_requests(
    invocations: &CapturedFailedSearchInvocations,
    mode: SearchEngineMode,
) -> FakeResult {
    let lexical = &invocations.lexical;
    match (&invocations.semantic, mode.query_embedding_enabled()) {
        (Some((router, vector)), true) => {
            assert_router_embed_request(router)?;
            assert_search_database_requests(
                lexical,
                vector,
                SEARCH_COMBINED_QUERY,
                SEARCH_GENERATED_VECTOR_PARAMETER,
            )
        }
        (None, false) => require(
            lexical.database == SMOKE_DATABASE_TARGET
                && lexical.sql == SEARCH_BM25_SQL
                && lexical.params.len() == 2
                && database_parameter(lexical, 0)? == SEARCH_COMBINED_QUERY
                && database_parameter(lexical, 1)? == SEARCH_LIMIT.to_string(),
            "failed memory search did not send the exact query and requested limit to BM25",
        ),
        (Some(_), false) | (None, true) => Err(ProcessClientError::ProtocolScenario(
            "failed memory search captured an unexpected query embedding state",
        )),
    }
}

fn assert_combined_memory_search_requests(
    invocations: &CapturedCombinedSearchInvocations,
) -> FakeResult {
    assert_router_embed_request(&invocations.router)?;
    assert_search_database_requests(
        &invocations.lexical,
        &invocations.vector,
        SEARCH_COMBINED_QUERY,
        SEARCH_GENERATED_VECTOR_PARAMETER,
    )
}

fn assert_router_embed_request(invocation: &CapturedRouterInvocation) -> FakeResult {
    let data = invocation
        .data
        .as_object()
        .ok_or(ProcessClientError::ProtocolScenario(
            "router::embed data was not an object",
        ))?;
    require(
        has_exact_keys(data, &["input", "provider", "model"]),
        "router::embed data had an unexpected shape",
    )?;
    require(
        invocation.data["input"] == json!([SEARCH_COMBINED_QUERY]),
        "router::embed did not receive the exact query as its only input",
    )?;
    require(
        invocation.data["provider"] == SEARCH_EMBEDDING_PROVIDER_SENTINEL
            && invocation.data["model"] == SEARCH_EMBEDDING_MODEL_SENTINEL,
        "router::embed did not request the configured provider and model",
    )
}

fn assert_lexical_fallback_requests(
    invocations: &CapturedLexicalFallbackInvocations,
    mode: SearchEngineMode,
) -> FakeResult {
    match (&invocations.router, mode.query_embedding_enabled()) {
        (Some(router), true) => assert_router_embed_request(router)?,
        (None, false) => {}
        (Some(_), false) | (None, true) => {
            return Err(ProcessClientError::ProtocolScenario(
                "lexical fallback captured an unexpected router::embed state",
            ));
        }
    }
    let lexical = &invocations.lexical;
    require(
        lexical.database == SMOKE_DATABASE_TARGET && lexical.sql == SEARCH_BM25_SQL,
        "lexical fallback did not use the static current-memory BM25 SQL on the smoke database",
    )?;
    require(
        lexical.params.len() == 2
            && database_parameter(lexical, 0)? == SEARCH_COMBINED_QUERY
            && database_parameter(lexical, 1)? == SEARCH_LEXICAL_FALLBACK_LIMIT.to_string(),
        "lexical fallback did not send the exact query and requested limit to BM25",
    )
}

fn assert_search_database_requests(
    lexical: &CapturedDatabaseInvocation,
    vector: &CapturedDatabaseInvocation,
    expected_query: &str,
    expected_vector_parameter: &str,
) -> FakeResult {
    require(
        lexical.database == SMOKE_DATABASE_TARGET && vector.database == SMOKE_DATABASE_TARGET,
        "memory search used an unexpected database target",
    )?;
    require(
        lexical.sql == SEARCH_BM25_SQL && vector.sql == SEARCH_VECTOR_SQL,
        "memory search did not use the static current-memory BM25 and vector SQL",
    )?;
    let lexical_sql = lexical.sql.to_ascii_lowercase();
    let vector_sql = vector.sql.to_ascii_lowercase();
    require(
        !lexical_sql.contains("embedding")
            && !lexical_sql.contains("hybrid")
            && !lexical_sql.contains("fallback")
            && !lexical_sql.contains("union")
            && !vector_sql.contains("hybrid")
            && !vector_sql.contains("fallback")
            && !vector_sql.contains("union"),
        "memory search SQL introduced an unsupported fallback or hybrid claim",
    )?;
    require(
        lexical.params.len() == 2 && vector.params.len() == 2,
        "memory search did not use two positional parameters per operation",
    )?;
    let lexical_query = database_parameter(lexical, 0)?;
    let lexical_limit = database_parameter(lexical, 1)?;
    let vector_parameter = database_parameter(vector, 0)?;
    let vector_limit = database_parameter(vector, 1)?;
    let expected_limit = SEARCH_LIMIT.to_string();
    require(
        lexical_query == expected_query,
        "memory search did not send the exact query to BM25",
    )?;
    require(
        vector_parameter == expected_vector_parameter,
        "memory search did not forward the expected query vector to vector search",
    )?;
    require(
        lexical_limit == expected_limit && vector_limit == expected_limit,
        "memory search did not share the requested limit across BM25 and vector search",
    )
}

fn assert_memory_version_list_request(
    invocation: &CapturedDatabaseInvocation,
    expected_offset: u64,
    expected_probe_limit: u32,
) -> FakeResult {
    require(
        invocation.database == SMOKE_DATABASE_TARGET,
        "memory version listing used an unexpected database target",
    )?;
    require(
        invocation.sql == VERSION_LIST_SQL,
        "memory version listing did not use the static newest-first query",
    )?;
    let sql = invocation.sql.to_ascii_lowercase();
    require(
        sql.contains(
            "select\n    memory.version::text as version,\n    memory.updated_at\nfrom public.memories as memory",
        ) && sql.contains("where memory.id = $1::text")
            && sql.contains("order by memory.version desc")
            && sql.contains("offset $2::text::bigint")
            && sql.contains("limit $3::text::bigint"),
        "memory version listing did not project version metadata with the expected pagination order",
    )?;
    require(
        [
            "title",
            "content",
            "embedding",
            "concept",
            "file",
            "session",
            "source_observation",
            "search",
            "relevance",
            "score",
            "vector",
        ]
        .iter()
        .all(|forbidden| !sql.contains(forbidden)),
        "memory version listing selected protected or search fields",
    )?;
    require(
        invocation.params.len() == 3
            && database_parameter(invocation, 0)? == VERSION_LIST_ID_SENTINEL
            && database_parameter(invocation, 1)? == expected_offset.to_string()
            && database_parameter(invocation, 2)? == expected_probe_limit.to_string(),
        "memory version listing did not use the expected ID, offset, and limit-plus-one parameters",
    )
}

fn assert_memory_retrieval_request(
    invocation: &CapturedDatabaseInvocation,
    operation: RetrievalOperation,
) -> FakeResult {
    require(
        invocation.database == SMOKE_DATABASE_TARGET,
        "memory retrieval used an unexpected database target",
    )?;
    require(
        invocation.sql == operation.sql(),
        "memory retrieval did not use the expected static canonical read query",
    )?;
    let sql = invocation.sql.to_ascii_lowercase();
    require(
        [
            "embedding",
            "search_document",
            "relevance",
            "score",
            "vector",
        ]
        .iter()
        .all(|forbidden| !sql.contains(forbidden)),
        "memory retrieval selected an embedding or search-only field",
    )?;
    match operation {
        RetrievalOperation::Exact => require(
            sql.contains("from public.memories as memory")
                && !sql.contains("memory_search_heads")
                && sql.contains("where memory.id = $1::text")
                && sql.contains("and memory.version = $2::text::bigint")
                && invocation.params.len() == 2
                && database_parameter(invocation, 0)? == RETRIEVAL_ID_SENTINEL
                && database_parameter(invocation, 1)? == RETRIEVAL_EXACT_VERSION.to_string(),
            "memory exact retrieval did not use its canonical ID and version query",
        ),
        RetrievalOperation::Latest => require(
            sql.contains("from public.memory_search_heads as head")
                && sql.contains("join public.memories as memory")
                && sql.contains("on memory.id = head.id")
                && sql.contains("and memory.version = head.version")
                && sql.contains("where head.id = $1::text")
                && !sql.contains("order by")
                && invocation.params.len() == 1
                && database_parameter(invocation, 0)? == RETRIEVAL_ID_SENTINEL,
            "memory latest retrieval did not use the deterministic head query",
        ),
    }
}

fn assert_successful_memory_retrieval_response(
    response: &Value,
    request_id: &str,
    operation: RetrievalOperation,
) -> FakeResult {
    require(
        *response == expected_memory_retrieval_response(request_id, operation),
        "successful memory retrieval did not return the exact canonical record",
    )?;
    let structured_content = response["result"]["structuredContent"].as_object().ok_or(
        ProcessClientError::ProtocolScenario(
            "successful memory retrieval did not return structured content",
        ),
    )?;
    require(
        has_exact_keys(structured_content, &["memory"]),
        "successful memory retrieval returned an unexpected structured result shape",
    )?;
    let memory =
        structured_content["memory"]
            .as_object()
            .ok_or(ProcessClientError::ProtocolScenario(
                "successful memory retrieval did not return a memory object",
            ))?;
    require(
        has_exact_keys(
            memory,
            &[
                "id",
                "version",
                "type",
                "title",
                "content",
                "created_at",
                "updated_at",
                "concepts",
                "files",
                "session_ids",
                "source_observation_ids",
            ],
        ) && !memory.contains_key("embedding"),
        "successful memory retrieval did not return exactly the canonical memory fields",
    )?;
    let output = Value::Object(memory.clone())
        .to_string()
        .to_ascii_lowercase();
    require(
        !output.contains("embedding"),
        "successful memory retrieval exposed an embedding",
    )
}

fn assert_successful_memory_search_response(response: &Value) -> FakeResult {
    require(
        *response == expected_memory_search_response(),
        "successful memory search did not return the exact score-free canonical result",
    )?;
    assert_same_version_cross_backend_tie(response)?;
    let output = response.to_string().to_ascii_lowercase();
    require(
        ["relevance", "score", "embedding", "rank"]
            .iter()
            .all(|forbidden| !output.contains(forbidden)),
        "successful memory search made a score, embedding, or rank claim",
    )
}

fn assert_query_embedding_values_absent(response: &Value) -> FakeResult {
    let output = response.to_string();
    require(
        !output.contains(SEARCH_GENERATED_VECTOR_PARAMETER)
            && SEARCH_GENERATED_VECTOR
                .iter()
                .all(|component| !output.contains(&component.abs().to_string()))
            && !output.contains(SEARCH_EMBEDDING_PROVIDER_SENTINEL)
            && !output.contains(SEARCH_EMBEDDING_MODEL_SENTINEL),
        "successful memory search exposed the generated query vector or embedding identity",
    )
}

fn assert_lexical_fallback_response(response: &Value, mode: SearchEngineMode) -> FakeResult {
    require(
        response["result"]["isError"] != true,
        "lexical fallback returned a tool error instead of lexical-only results",
    )?;
    let expected = if matches!(mode, SearchEngineMode::DisabledEmbeddingEmptyFallback) {
        memory_search_response(Vec::new())
    } else {
        expected_lexical_fallback_response()
    };
    require(
        *response == expected,
        "lexical fallback did not return the exact score-free lexical-only result",
    )?;
    let output = response.to_string();
    let lowercase_output = output.to_ascii_lowercase();
    require(
        ["relevance", "score", "embedding", "rank", "vector"]
            .iter()
            .all(|forbidden| !lowercase_output.contains(forbidden)),
        "lexical fallback made a score, embedding, rank, or vector claim",
    )?;
    require(
        protected_lexical_fallback_values()
            .iter()
            .all(|value| !output.contains(value.as_str())),
        "lexical fallback response exposed a query, embedding identity, router response, or vector value",
    )
}

fn assert_lexical_fallback_stderr(stderr: &CapturedStderr) -> FakeResult {
    require(
        protected_lexical_fallback_values()
            .iter()
            .all(|value| !stderr.contains(value.as_bytes()))
            && !stderr.contains(SEARCH_RESULT_SENTINEL.as_bytes()),
        "lexical fallback diagnostics leaked a query, embedding identity, router response, vector, or memory value",
    )?;
    assert_empty_stderr(stderr)
}

fn assert_successful_memory_version_list_response(
    response: &Value,
    id: &str,
    expected_versions: &[i64],
    expected_next_offset: Option<u64>,
) -> FakeResult {
    require(
        *response
            == expected_memory_version_list_response(id, expected_versions, expected_next_offset),
        "successful memory version listing did not return the exact narrow newest-first page",
    )?;
    let structured_content = response["result"]["structuredContent"].as_object().ok_or(
        ProcessClientError::ProtocolScenario(
            "successful memory version listing did not return structured content",
        ),
    )?;
    require(
        has_exact_keys(structured_content, &["versions", "next_offset"]),
        "successful memory version listing returned an unexpected structured result shape",
    )?;
    let versions =
        structured_content["versions"]
            .as_array()
            .ok_or(ProcessClientError::ProtocolScenario(
                "successful memory version listing did not return a versions array",
            ))?;
    require(
        versions.len() == expected_versions.len()
            && versions
                .iter()
                .zip(expected_versions)
                .all(|(summary, version)| {
                    summary.as_object().is_some_and(|summary| {
                        has_exact_keys(summary, &["version", "updated_at"])
                            && summary["version"].as_i64() == Some(*version)
                            && summary["updated_at"] == VERSION_LIST_UPDATED_AT
                    })
                })
            && expected_versions
                .windows(2)
                .all(|versions| versions[0] > versions[1]),
        "successful memory version listing did not expose descending narrow metadata",
    )?;
    let output = structured_content["versions"]
        .to_string()
        .to_ascii_lowercase();
    require(
        [
            "title",
            "content",
            "embedding",
            "concept",
            "file",
            "session",
            "source_observation",
            "relevance",
            "score",
            "rank",
        ]
        .iter()
        .all(|forbidden| !output.contains(forbidden)),
        "successful memory version listing exposed protected memory fields",
    )?;
    assert_protected_version_list_values_absent(response)
}

fn assert_same_version_cross_backend_tie(response: &Value) -> FakeResult {
    let results = response["result"]["structuredContent"]["results"]
        .as_array()
        .ok_or(ProcessClientError::ProtocolScenario(
            "successful memory search did not return a results array",
        ))?;
    let matching = results
        .iter()
        .filter(|memory| memory["id"] == SEARCH_TIE_ID)
        .collect::<Vec<_>>();
    let expected = search_memory_dto(SEARCH_TIE_ID, SEARCH_TIE_VERSION, SEARCH_LEXICAL_TIE_MARKER);

    require(
        matching.len() == 1 && *matching[0] == expected,
        "same-version cross-backend candidates did not retain exactly one lexical-first canonical result",
    )
}

fn expected_memory_search_response() -> Value {
    memory_search_response(vec![
        search_memory_dto("search-id-a", 1, "vector-a-v1"),
        search_memory_dto("search-id-b", 3, "vector-b-v3"),
        search_memory_dto("search-id-c", 2, "lexical-c-v2"),
        search_memory_dto(SEARCH_TIE_ID, SEARCH_TIE_VERSION, SEARCH_LEXICAL_TIE_MARKER),
        search_memory_dto("search-id-y", 1, "vector-y-v1"),
    ])
}

fn expected_lexical_fallback_response() -> Value {
    memory_search_response(vec![
        search_memory_dto("search-id-e", 1, "fallback-lexical-e-v1"),
        search_memory_dto(
            SEARCH_FALLBACK_TIE_ID,
            SEARCH_FALLBACK_TIE_VERSION,
            SEARCH_FALLBACK_TIE_MARKER,
        ),
        search_memory_dto("search-id-r", 4, "fallback-lexical-r-v4"),
    ])
}

fn memory_search_response(results: Vec<Value>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": "search-success",
        "result": {
            "content": [{ "type": "text", "text": "memory_search complete" }],
            "_meta": server_identity(),
            "resultType": "complete",
            "structuredContent": { "results": results },
        },
    })
}

fn expected_memory_version_list_response(
    id: &str,
    versions: &[i64],
    next_offset: Option<u64>,
) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "content": [{ "type": "text", "text": "memory_list_versions complete" }],
            "_meta": server_identity(),
            "resultType": "complete",
            "structuredContent": {
                "versions": versions
                    .iter()
                    .map(|version| json!({
                        "version": version,
                        "updated_at": VERSION_LIST_UPDATED_AT,
                    }))
                    .collect::<Vec<_>>(),
                "next_offset": next_offset,
            },
        },
    })
}

fn expected_memory_retrieval_response(request_id: &str, operation: RetrievalOperation) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {
            "content": [{ "type": "text", "text": format!("{} complete", operation.tool_name()) }],
            "_meta": server_identity(),
            "resultType": "complete",
            "structuredContent": {
                "memory": retrieval_memory_dto(operation),
            },
        },
    })
}

fn search_memory_dto(id: &str, version: i64, marker: &str) -> Value {
    let (created_at, updated_at) = search_timestamps(marker);
    json!({
        "id": id,
        "version": version,
        "type": format!("{SEARCH_RESULT_SENTINEL} {marker} type"),
        "title": format!("{SEARCH_RESULT_SENTINEL} {marker} title"),
        "content": format!("{SEARCH_RESULT_SENTINEL} {marker} content"),
        "created_at": created_at,
        "updated_at": updated_at,
        "concepts": [format!("{SEARCH_RESULT_SENTINEL} {marker} concept")],
        "files": [format!("{SEARCH_RESULT_SENTINEL} {marker} file")],
        "session_ids": [format!("{SEARCH_RESULT_SENTINEL} {marker} session")],
        "source_observation_ids": [format!("{SEARCH_RESULT_SENTINEL} {marker} observation")],
    })
}

fn assert_successful_memory_save_response(
    response: &Value,
    invocation: &CapturedDatabaseInvocation,
) -> FakeResult {
    assert_memory_insert_request(invocation)?;
    let response_object = response
        .as_object()
        .ok_or(ProcessClientError::ProtocolScenario(
            "successful memory save response was not an object",
        ))?;
    require(
        has_exact_keys(response_object, &["jsonrpc", "id", "result"]),
        "successful memory save response had an unexpected shape",
    )?;
    require(
        response["jsonrpc"] == "2.0" && response["id"] == "save-success",
        "successful memory save response did not preserve its request ID",
    )?;
    let result = response["result"]
        .as_object()
        .ok_or(ProcessClientError::ProtocolScenario(
            "successful memory save result was not an object",
        ))?;
    require(
        has_exact_keys(
            result,
            &["content", "_meta", "resultType", "structuredContent"],
        ),
        "successful memory save result had an unexpected shape",
    )?;
    require(
        response["result"]["content"]
            == json!([{ "type": "text", "text": "memory_save complete" }])
            && response["result"]["_meta"] == server_identity()
            && response["result"]["resultType"] == "complete",
        "successful memory save result did not use the expected MCP success envelope",
    )?;
    let structured_content = response["result"]["structuredContent"].as_object().ok_or(
        ProcessClientError::ProtocolScenario(
            "successful memory save structured content was not an object",
        ),
    )?;
    require(
        has_exact_keys(structured_content, &["memory"]),
        "successful memory save structured content had an unexpected shape",
    )?;
    let memory =
        structured_content["memory"]
            .as_object()
            .ok_or(ProcessClientError::ProtocolScenario(
                "successful memory save did not return a memory object",
            ))?;
    require(
        has_exact_keys(
            memory,
            &[
                "id",
                "version",
                "type",
                "title",
                "content",
                "created_at",
                "updated_at",
                "concepts",
                "files",
                "session_ids",
                "source_observation_ids",
            ],
        ),
        "successful memory save did not return every canonical field",
    )?;

    let request_id = database_parameter(invocation, 0)?;
    let request_created_at = canonical_whole_second_timestamp(database_parameter(invocation, 5)?)?;
    let request_updated_at = canonical_whole_second_timestamp(database_parameter(invocation, 6)?)?;
    let response_created_at =
        canonical_whole_second_timestamp(memory["created_at"].as_str().ok_or(
            ProcessClientError::ProtocolScenario(
                "successful memory save created-at timestamp was not a string",
            ),
        )?)?;
    let response_updated_at =
        canonical_whole_second_timestamp(memory["updated_at"].as_str().ok_or(
            ProcessClientError::ProtocolScenario(
                "successful memory save updated-at timestamp was not a string",
            ),
        )?)?;

    require(
        memory["id"].as_str() == Some(request_id) && is_uuid_v4(request_id),
        "successful memory save did not return its stable UUIDv4 ID",
    )?;
    require(
        memory["version"].as_i64() == Some(1)
            && memory["type"].as_str() == Some("unclassified")
            && memory["title"].as_str() == Some(SAVE_TITLE_SENTINEL)
            && memory["content"].as_str() == Some(SAVE_CONTENT_SENTINEL),
        "successful memory save did not return assigned canonical fields",
    )?;
    require(
        memory["concepts"] == json!([])
            && memory["files"] == json!([])
            && memory["session_ids"] == json!([SAVE_SESSION_SENTINEL])
            && memory["source_observation_ids"] == json!([])
            && !memory.contains_key("embedding"),
        "successful memory save did not return the expected session and enrichment collections",
    )?;
    require(
        request_created_at == request_updated_at
            && request_created_at == response_created_at
            && response_created_at == response_updated_at,
        "successful memory save timestamps were not one canonical whole-second value",
    )
}

fn assert_memory_insert_request(invocation: &CapturedDatabaseInvocation) -> FakeResult {
    require(
        invocation.database == SMOKE_DATABASE_TARGET,
        "memory save used an unexpected database target",
    )?;
    require(
        invocation.sql == INSERT_MEMORY_SQL,
        "memory save did not use the static immutable insert SQL",
    )?;
    let sql = invocation.sql.to_ascii_lowercase();
    require(
        !sql.contains("embedding") && !sql.contains("vector"),
        "memory save insert request included an embedding",
    )?;
    require(
        invocation.params.len() == 11,
        "memory save insert request did not use eleven positional parameters",
    )?;
    let id = database_parameter(invocation, 0)?;
    let created_at = database_parameter(invocation, 5)?;
    let updated_at = database_parameter(invocation, 6)?;
    require(
        is_uuid_v4(id)
            && invocation.params[1].as_str() == Some("1")
            && invocation.params[2].as_str() == Some("unclassified")
            && invocation.params[3].as_str() == Some(SAVE_TITLE_SENTINEL)
            && invocation.params[4].as_str() == Some(SAVE_CONTENT_SENTINEL)
            && created_at == updated_at
            && invocation.params[7] == json!([])
            && invocation.params[8] == json!([])
            && invocation.params[9] == json!([SAVE_SESSION_SENTINEL])
            && invocation.params[10] == json!([]),
        "memory save insert request did not contain the canonical immutable memory",
    )?;
    let _ = canonical_whole_second_timestamp(created_at)?;
    Ok(())
}

fn database_parameter(invocation: &CapturedDatabaseInvocation, index: usize) -> FakeResult<&str> {
    invocation.params.get(index).and_then(Value::as_str).ok_or(
        ProcessClientError::ProtocolScenario(
            "database invocation had a non-string positional parameter",
        ),
    )
}

fn canonical_whole_second_timestamp(value: &str) -> FakeResult<DateTime<Utc>> {
    let timestamp = DateTime::parse_from_rfc3339(value).map_err(|_| {
        ProcessClientError::ProtocolScenario("memory save timestamp was not RFC3339")
    })?;
    require(
        timestamp.offset().local_minus_utc() == 0
            && timestamp.nanosecond() == 0
            && !value.contains('.'),
        "memory save timestamp was not canonical UTC whole-second RFC3339",
    )?;
    Ok(timestamp.with_timezone(&Utc))
}

fn is_uuid_v4(value: &str) -> bool {
    let bytes = value.as_bytes();
    value.len() == 36
        && [8, 13, 18, 23]
            .into_iter()
            .all(|index| bytes.get(index) == Some(&b'-'))
        && bytes.get(14) == Some(&b'4')
        && matches!(bytes.get(19), Some(b'8' | b'9' | b'a' | b'b'))
        && Uuid::parse_str(value).is_ok_and(|uuid| uuid.hyphenated().to_string() == value)
}

fn assert_protected_values_absent(response: &Value) -> FakeResult {
    let output = response.to_string();
    require(
        protected_save_values()
            .iter()
            .all(|value| !output.contains(value)),
        "memory save failure response leaked a protected value",
    )
}

fn assert_protected_search_values_absent(response: &Value) -> FakeResult {
    let output = response.to_string();
    require(
        protected_search_values()
            .iter()
            .all(|value| !output.contains(value.as_str())),
        "memory search failure response leaked a protected value",
    )
}

fn assert_protected_version_list_values_absent(response: &Value) -> FakeResult {
    let output = response.to_string();
    require(
        protected_version_list_values()
            .iter()
            .all(|value| !output.contains(value)),
        "memory version-list response leaked a protected value",
    )
}

fn assert_protected_retrieval_values_absent(response: &Value) -> FakeResult {
    let output = response.to_string();
    require(
        protected_retrieval_values()
            .iter()
            .all(|value| !output.contains(value)),
        "memory retrieval failure response leaked a protected value",
    )
}

fn assert_protected_stderr_values_absent(stderr: &CapturedStderr) -> FakeResult {
    require(
        protected_save_values()
            .iter()
            .all(|value| !stderr.contains(value.as_bytes())),
        "memory save failure diagnostics leaked a protected value",
    )
}

fn assert_protected_search_stderr_values_absent(stderr: &CapturedStderr) -> FakeResult {
    require(
        protected_search_values()
            .iter()
            .all(|value| !stderr.contains(value.as_bytes())),
        "memory search failure diagnostics leaked a protected value",
    )
}

fn assert_protected_version_list_stderr_values_absent(stderr: &CapturedStderr) -> FakeResult {
    require(
        protected_version_list_values()
            .iter()
            .all(|value| !stderr.contains(value.as_bytes())),
        "memory version-list diagnostics leaked a protected value",
    )
}

fn assert_protected_retrieval_stderr_values_absent(stderr: &CapturedStderr) -> FakeResult {
    require(
        protected_retrieval_values()
            .iter()
            .all(|value| !stderr.contains(value.as_bytes())),
        "memory retrieval diagnostics leaked a protected value",
    )
}

fn protected_save_values() -> [&'static str; 6] {
    [
        SAVE_TITLE_SENTINEL,
        SAVE_CONTENT_SENTINEL,
        SAVE_SESSION_SENTINEL,
        ENGINE_ERROR_CODE_SENTINEL,
        ENGINE_ERROR_MESSAGE_SENTINEL,
        ENGINE_ERROR_STACKTRACE_SENTINEL,
    ]
}

/// Request, candidate, engine-error, embedding-identity, and vector values that failed or
/// rejected searches must keep out of responses and diagnostics.
fn protected_search_values() -> Vec<String> {
    let mut values = [
        SEARCH_QUERY_SENTINEL,
        SEARCH_UNKNOWN_ARGUMENT_SENTINEL,
        SEARCH_RESULT_SENTINEL,
        SEARCH_ENGINE_ERROR_CODE_SENTINEL,
        SEARCH_ENGINE_ERROR_MESSAGE_SENTINEL,
        SEARCH_ENGINE_ERROR_STACKTRACE_SENTINEL,
        SEARCH_EMBEDDING_PROVIDER_SENTINEL,
        SEARCH_EMBEDDING_MODEL_SENTINEL,
    ]
    .map(str::to_owned)
    .to_vec();
    values.extend(
        SEARCH_GENERATED_VECTOR
            .iter()
            .chain(&SEARCH_CALLER_VECTOR)
            .map(|component| component.abs().to_string()),
    );
    values
}

fn protected_lexical_fallback_values() -> Vec<String> {
    let mut values = [
        SEARCH_QUERY_SENTINEL,
        SEARCH_EMBEDDING_PROVIDER_SENTINEL,
        SEARCH_EMBEDDING_MODEL_SENTINEL,
        SEARCH_ROUTER_ERROR_CODE_SENTINEL,
        SEARCH_ROUTER_ERROR_MESSAGE_SENTINEL,
        SEARCH_ROUTER_ERROR_STACKTRACE_SENTINEL,
        SEARCH_ROUTER_CREDENTIAL_SENTINEL,
        SEARCH_ROUTER_RESOLVED_MODEL_SENTINEL,
    ]
    .map(str::to_owned)
    .to_vec();
    values.extend(
        SEARCH_ROUTER_INVALID_VECTOR
            .iter()
            .map(|component| component.abs().to_string()),
    );
    values
}

fn protected_version_list_values() -> [&'static str; 4] {
    [
        VERSION_LIST_ID_SENTINEL,
        VERSION_LIST_ENGINE_ERROR_CODE_SENTINEL,
        VERSION_LIST_ENGINE_ERROR_MESSAGE_SENTINEL,
        VERSION_LIST_ENGINE_ERROR_STACKTRACE_SENTINEL,
    ]
}

fn protected_retrieval_values() -> [&'static str; 5] {
    [
        RETRIEVAL_ID_SENTINEL,
        RETRIEVAL_RESULT_SENTINEL,
        RETRIEVAL_ENGINE_ERROR_CODE_SENTINEL,
        RETRIEVAL_ENGINE_ERROR_MESSAGE_SENTINEL,
        RETRIEVAL_ENGINE_ERROR_STACKTRACE_SENTINEL,
    ]
}

fn assert_empty_stderr(stderr: &CapturedStderr) -> FakeResult {
    require(
        stderr.bytes.is_empty() && !stderr.is_truncated(),
        "worker wrote unexpected diagnostics to stderr",
    )
}

fn require(condition: bool, message: &'static str) -> FakeResult {
    condition
        .then_some(())
        .ok_or(ProcessClientError::ProtocolScenario(message))
}

fn server_identity() -> Value {
    json!({
        "io.modelcontextprotocol/serverInfo": {
            "name": SERVER_NAME,
            "version": WORKER_VERSION,
        },
    })
}

fn expected_tool_registry() -> Value {
    json!([
        tool(
            "memory_save",
            object_schema(
                json!({
                    "title": { "type": "string" },
                    "content": { "type": "string" },
                    "session_id": { "type": "string" },
                }),
                &["title", "content", "session_id"],
            ),
            object_schema(json!({ "memory": memory_schema() }), &["memory"]),
        ),
        tool(
            "memory_search",
            object_schema(
                json!({
                    "query": { "type": "string" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 50 },
                }),
                &["query", "limit"],
            ),
            object_schema(
                json!({ "results": { "type": "array", "items": memory_schema() } }),
                &["results"],
            ),
        ),
        tool(
            "memory_get_latest",
            object_schema(json!({ "id": { "type": "string" } }), &["id"]),
            object_schema(json!({ "memory": memory_schema() }), &["memory"]),
        ),
        tool(
            "memory_get_exact",
            object_schema(
                json!({
                    "id": { "type": "string" },
                    "version": { "type": "integer", "minimum": 1 },
                }),
                &["id", "version"],
            ),
            object_schema(json!({ "memory": memory_schema() }), &["memory"]),
        ),
        tool(
            "memory_list_versions",
            object_schema(
                json!({
                    "id": { "type": "string" },
                    "offset": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 9_007_199_254_740_891u64,
                        "default": 0,
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100,
                        "default": 50,
                    },
                }),
                &["id"],
            ),
            object_schema(
                json!({
                    "versions": { "type": "array", "items": memory_version_schema() },
                    "next_offset": {
                        "type": ["integer", "null"],
                        "minimum": 0,
                        "maximum": 9_007_199_254_740_991u64,
                    },
                }),
                &["versions", "next_offset"],
            ),
        ),
    ])
}

fn tool(name: &str, input_schema: Value, output_schema: Value) -> Value {
    json!({
        "name": name,
        "inputSchema": input_schema,
        "outputSchema": output_schema,
    })
}

fn object_schema(properties: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}

fn memory_schema() -> Value {
    object_schema(
        json!({
            "id": { "type": "string" },
            "version": { "type": "integer", "minimum": 1 },
            "type": { "type": "string" },
            "title": { "type": "string" },
            "content": { "type": "string" },
            "created_at": { "type": "string", "format": "date-time" },
            "updated_at": { "type": "string", "format": "date-time" },
            "concepts": { "type": "array", "items": { "type": "string" } },
            "files": { "type": "array", "items": { "type": "string" } },
            "session_ids": { "type": "array", "items": { "type": "string" } },
            "source_observation_ids": { "type": "array", "items": { "type": "string" } },
        }),
        &[
            "id",
            "version",
            "type",
            "title",
            "content",
            "created_at",
            "updated_at",
            "concepts",
            "files",
            "session_ids",
            "source_observation_ids",
        ],
    )
}

fn memory_version_schema() -> Value {
    object_schema(
        json!({
            "version": { "type": "integer", "minimum": 1 },
            "updated_at": { "type": "string", "format": "date-time" },
        }),
        &["version", "updated_at"],
    )
}

fn parse_arguments<I>(arguments: I) -> FakeResult<Arguments>
where
    I: IntoIterator<Item = String>,
{
    let mut arguments = arguments.into_iter();
    let _program = arguments.next();
    let mut worker = None;
    let mut limits = ProcessLimits::default();

    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--worker" => {
                worker = Some(PathBuf::from(arguments.next().ok_or(
                    ProcessClientError::InvalidArguments("--worker requires a path"),
                )?));
            }
            "--timeout-seconds" => {
                limits.request_timeout = Duration::from_secs(parse_positive_u64(
                    arguments.next(),
                    "--timeout-seconds must be a positive integer",
                )?);
            }
            "--max-line-bytes" => {
                limits.max_line_bytes = parse_positive_usize(
                    arguments.next(),
                    "--max-line-bytes must be a positive integer",
                )?;
            }
            "--stderr-cap-bytes" => {
                limits.stderr_cap_bytes = parse_positive_usize(
                    arguments.next(),
                    "--stderr-cap-bytes must be a positive integer",
                )?;
            }
            "--help" | "-h" => {
                return Err(ProcessClientError::InvalidArguments(
                    "Usage: mcp-worker-fake --worker <release-worker-path> [--timeout-seconds <seconds>] [--max-line-bytes <bytes>] [--stderr-cap-bytes <bytes>]",
                ));
            }
            _ => return Err(ProcessClientError::InvalidArguments("unknown argument")),
        }
    }

    let arguments = Arguments {
        worker: worker.ok_or(ProcessClientError::InvalidArguments("--worker is required"))?,
        limits,
    };
    arguments.limits.validate()?;
    Ok(arguments)
}

fn parse_positive_u64(value: Option<String>, message: &'static str) -> FakeResult<u64> {
    value
        .and_then(|value| value.parse().ok())
        .filter(|value: &u64| *value > 0)
        .ok_or(ProcessClientError::InvalidArguments(message))
}

fn parse_positive_usize(value: Option<String>, message: &'static str) -> FakeResult<usize> {
    value
        .and_then(|value| value.parse().ok())
        .filter(|value: &usize| *value > 0)
        .ok_or(ProcessClientError::InvalidArguments(message))
}

fn resolve_worker_executable(path: &Path) -> FakeResult<PathBuf> {
    let path = fs::canonicalize(path).map_err(|_| ProcessClientError::InvalidWorkerPath)?;
    if path.is_file() {
        Ok(path)
    } else {
        Err(ProcessClientError::InvalidWorkerPath)
    }
}

#[allow(dead_code)]
impl ChildProcessClient {
    async fn start(mut command: Command, limits: ProcessLimits) -> FakeResult<Self> {
        limits.validate()?;
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command
            .spawn()
            .map_err(|_| ProcessClientError::ChildSpawn)?;
        let stdin = match child.stdin.take() {
            Some(stdin) => stdin,
            None => {
                reap_unmanaged_child(&mut child, limits.request_timeout).await;
                return Err(ProcessClientError::ChildStdin);
            }
        };
        let stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => {
                reap_unmanaged_child(&mut child, limits.request_timeout).await;
                return Err(ProcessClientError::ChildStdout);
            }
        };
        let stderr = match child.stderr.take() {
            Some(stderr) => stderr,
            None => {
                reap_unmanaged_child(&mut child, limits.request_timeout).await;
                return Err(ProcessClientError::ChildStderr);
            }
        };
        #[cfg(test)]
        let (stderr_capture_complete, stderr_capture_observer) = oneshot::channel();

        Ok(Self {
            child: Some(child),
            stdin: Some(stdin),
            stdout: BufReader::new(stdout),
            stderr_task: Some(tokio::spawn(capture_stderr(
                stderr,
                limits.stderr_cap_bytes,
                #[cfg(test)]
                stderr_capture_complete,
            ))),
            #[cfg(test)]
            stderr_capture_complete: Some(stderr_capture_observer),
            limits,
        })
    }

    async fn send_request(&mut self, request: &Value) -> FakeResult {
        let result = self
            .send_request_before(request, Instant::now() + self.limits.request_timeout)
            .await;
        match result {
            Ok(()) => Ok(()),
            Err(error) => self.finish_failed_request(error).await,
        }
    }

    /// Writes one request to a worker expected to exit before reading stdin. A broken pipe means
    /// the worker already exited without reading it, so it is accepted like a completed write.
    async fn offer_request_before_exit(&mut self, request: &Value) -> FakeResult {
        let line = match encode_request(request, self.limits.max_line_bytes) {
            Ok(line) => line,
            Err(error) => return self.finish_failed_request(error).await,
        };
        let Some(stdin) = self.stdin.as_mut() else {
            return self
                .finish_failed_request(ProcessClientError::ChildStdin)
                .await;
        };
        let written = timeout(self.limits.request_timeout, async {
            stdin.write_all(&line).await?;
            stdin.write_all(b"\n").await?;
            stdin.flush().await
        })
        .await;
        match written {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) if error.kind() == io::ErrorKind::BrokenPipe => Ok(()),
            Ok(Err(_)) => {
                self.finish_failed_request(ProcessClientError::ChildIo)
                    .await
            }
            Err(_) => {
                self.finish_failed_request(ProcessClientError::RequestDeadline)
                    .await
            }
        }
    }

    async fn send_raw_test_line(&mut self, line: &[u8]) -> FakeResult {
        let result = self
            .send_raw_test_line_before(line, Instant::now() + self.limits.request_timeout)
            .await;
        match result {
            Ok(()) => Ok(()),
            Err(error) => self.finish_failed_request(error).await,
        }
    }

    async fn start_bounded_request_flood(
        &mut self,
        requests: &[Value],
    ) -> FakeResult<RequestFlood> {
        if requests.is_empty() || requests.len() > MAX_FLOOD_REQUESTS {
            return self
                .finish_failed_request(ProcessClientError::ProtocolScenario(
                    "bounded flood request limits are invalid",
                ))
                .await;
        }
        let frames = match requests
            .iter()
            .map(|request| encode_request(request, self.limits.max_line_bytes))
            .collect::<FakeResult<Vec<_>>>()
        {
            Ok(frames) => frames,
            Err(error) => return self.finish_failed_request(error).await,
        };
        let stdin = match self.stdin.take() {
            Some(stdin) => stdin,
            None => {
                return self
                    .finish_failed_request(ProcessClientError::ChildStdin)
                    .await;
            }
        };
        let deadline = Instant::now() + self.limits.request_timeout;
        let (pressure_signal, pressure_observed) = oneshot::channel();

        Ok(RequestFlood {
            pressure_observed,
            completion: tokio::spawn(write_bounded_request_flood(
                stdin,
                frames,
                pressure_signal,
                deadline,
            )),
            timeout: self.limits.request_timeout,
        })
    }

    async fn read_response(&mut self) -> FakeResult<Value> {
        let result = self
            .read_response_before(Instant::now() + self.limits.request_timeout)
            .await;
        match result {
            Ok(response) => Ok(response),
            Err(error) => self.finish_failed_request(error).await,
        }
    }

    async fn request(&mut self, request: &Value) -> FakeResult<Value> {
        let deadline = Instant::now() + self.limits.request_timeout;
        if let Err(error) = self.send_request_before(request, deadline).await {
            return self.finish_failed_request(error).await;
        }
        match self.read_response_before(deadline).await {
            Ok(response) => Ok(response),
            Err(error) => self.finish_failed_request(error).await,
        }
    }

    fn close_stdin(&mut self) {
        self.stdin.take();
    }

    fn assert_running(&mut self) -> FakeResult {
        let child = self
            .child
            .as_mut()
            .ok_or(ProcessClientError::ChildTermination)?;
        match child.try_wait().map_err(|_| ProcessClientError::ChildIo)? {
            None => Ok(()),
            Some(_) => Err(ProcessClientError::ProtocolScenario(
                "worker exited during the bounded flood",
            )),
        }
    }

    #[cfg(test)]
    fn take_stderr_capture_complete(&mut self) -> oneshot::Receiver<()> {
        self.stderr_capture_complete
            .take()
            .expect("stderr capture observer should be available")
    }

    async fn stop_successfully(mut self) -> FakeResult<CapturedStderr> {
        self.close_stdin();
        let status = match self.wait_for_exit().await {
            Ok(status) => status,
            Err(error) => {
                let _ = self.finish_stderr().await;
                return Err(error);
            }
        };
        self.finish_after_exit(status, true).await
    }

    async fn stop_unsuccessfully(mut self) -> FakeResult<CapturedStderr> {
        self.close_stdin();
        let status = match self.wait_for_exit().await {
            Ok(status) => status,
            Err(error) => {
                let _ = self.finish_stderr().await;
                return Err(error);
            }
        };
        self.finish_after_exit(status, false).await
    }

    #[cfg(unix)]
    async fn stop_after_signal(mut self, signal: &'static str) -> FakeResult<CapturedStderr> {
        if let Err(error) = self.send_signal(signal).await {
            let _ = self.terminate_and_finish_stderr().await;
            return Err(error);
        }
        let status = match self.wait_for_exit().await {
            Ok(status) => status,
            Err(error) => {
                let _ = self.finish_stderr().await;
                return Err(error);
            }
        };
        self.finish_after_exit(status, true).await
    }

    async fn terminate(mut self) -> FakeResult<CapturedStderr> {
        self.terminate_and_finish_stderr().await
    }

    async fn finish_failed_request<T>(&mut self, error: ProcessClientError) -> FakeResult<T> {
        self.terminate_and_finish_stderr().await?;
        Err(error)
    }

    async fn terminate_and_finish_stderr(&mut self) -> FakeResult<CapturedStderr> {
        self.close_stdin();
        let termination = self.terminate_and_wait().await;
        let stderr = self.finish_stderr().await;
        match (termination, stderr) {
            (Err(error), _) => Err(error),
            (_, Err(error)) => Err(error),
            (Ok(_), Ok(stderr)) if stderr.is_truncated() => {
                Err(ProcessClientError::StderrLimitExceeded)
            }
            (Ok(_), Ok(stderr)) => Ok(stderr),
        }
    }

    async fn finish_after_exit(
        &mut self,
        status: ExitStatus,
        expect_success: bool,
    ) -> FakeResult<CapturedStderr> {
        let stdout = self.assert_stdout_end().await;
        let stderr = self.finish_stderr().await;
        let stderr = match (stdout, stderr) {
            (Err(error), _) => return Err(error),
            (_, Err(error)) => return Err(error),
            (_, Ok(stderr)) if stderr.is_truncated() => {
                return Err(ProcessClientError::StderrLimitExceeded);
            }
            (_, Ok(stderr)) => stderr,
        };
        if status.success() == expect_success {
            Ok(stderr)
        } else if expect_success {
            Err(ProcessClientError::ChildExitedUnsuccessfully)
        } else {
            Err(ProcessClientError::ChildExitedSuccessfully)
        }
    }

    async fn assert_stdout_end(&mut self) -> FakeResult {
        match timeout(
            self.limits.request_timeout,
            read_bounded_line(&mut self.stdout, self.limits.max_line_bytes),
        )
        .await
        {
            Ok(Ok(BoundedLine::End)) => Ok(()),
            Ok(Ok(_)) => Err(ProcessClientError::ProtocolScenario(
                "worker wrote unexpected data to stdout",
            )),
            Ok(Err(_)) => Err(ProcessClientError::ChildIo),
            Err(_) => Err(ProcessClientError::RequestDeadline),
        }
    }

    #[cfg(unix)]
    async fn send_signal(&mut self, signal: &'static str) -> FakeResult {
        let process_id = self
            .child
            .as_ref()
            .and_then(Child::id)
            .ok_or(ProcessClientError::ChildTermination)?;
        let mut command = Command::new("kill");
        command
            .arg(format!("-{signal}"))
            .arg(process_id.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        match timeout(self.limits.request_timeout, command.status()).await {
            Ok(Ok(status)) if status.success() => Ok(()),
            Ok(Ok(_)) | Ok(Err(_)) => Err(ProcessClientError::ProtocolScenario(
                "worker signal delivery failed",
            )),
            Err(_) => Err(ProcessClientError::RequestDeadline),
        }
    }

    async fn send_request_before(&mut self, request: &Value, deadline: Instant) -> FakeResult {
        let line = encode_request(request, self.limits.max_line_bytes)?;
        self.write_line_before(&line, deadline).await
    }

    async fn send_raw_test_line_before(&mut self, line: &[u8], deadline: Instant) -> FakeResult {
        if line.len() > MAX_RAW_TEST_LINE_BYTES {
            return Err(ProcessClientError::RawTestLineTooLong);
        }
        if line.contains(&b'\n') {
            return Err(ProcessClientError::RawTestLineContainsNewline);
        }
        self.write_line_before(line, deadline).await
    }

    async fn write_line_before(&mut self, line: &[u8], deadline: Instant) -> FakeResult {
        let stdin = self.stdin.as_mut().ok_or(ProcessClientError::ChildStdin)?;
        match timeout_at(deadline, async {
            stdin.write_all(line).await?;
            stdin.write_all(b"\n").await?;
            stdin.flush().await
        })
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(ProcessClientError::ChildIo),
            Err(_) => Err(ProcessClientError::RequestDeadline),
        }
    }

    async fn read_response_before(&mut self, deadline: Instant) -> FakeResult<Value> {
        let line = match timeout_at(
            deadline,
            read_bounded_line(&mut self.stdout, self.limits.max_line_bytes),
        )
        .await
        {
            Ok(Ok(BoundedLine::Line(line))) => line,
            Ok(Ok(BoundedLine::TooLong)) => return Err(ProcessClientError::ResponseLineTooLong),
            Ok(Ok(BoundedLine::Unterminated)) => {
                return Err(ProcessClientError::ResponseMissingNewline);
            }
            Ok(Ok(BoundedLine::End)) => return Err(ProcessClientError::ResponseEnded),
            Ok(Err(_)) => return Err(ProcessClientError::ChildIo),
            Err(_) => return Err(ProcessClientError::RequestDeadline),
        };
        serde_json::from_slice(&line).map_err(|_| ProcessClientError::ResponseInvalidJson)
    }

    async fn wait_for_exit(&mut self) -> FakeResult<ExitStatus> {
        let wait_result = {
            let child = self
                .child
                .as_mut()
                .ok_or(ProcessClientError::ChildTermination)?;
            timeout(self.limits.request_timeout, child.wait()).await
        };
        match wait_result {
            Ok(Ok(status)) => Ok(status),
            Ok(Err(_)) => {
                self.terminate_and_wait().await?;
                Err(ProcessClientError::ChildIo)
            }
            Err(_) => {
                self.terminate_and_wait().await?;
                Err(ProcessClientError::ChildExitDeadline)
            }
        }
    }

    async fn terminate_and_wait(&mut self) -> FakeResult<ExitStatus> {
        let child = self
            .child
            .as_mut()
            .ok_or(ProcessClientError::ChildTermination)?;
        if let Some(status) = child.try_wait().map_err(|_| ProcessClientError::ChildIo)? {
            return Ok(status);
        }
        if child.start_kill().is_err() {
            return child
                .try_wait()
                .map_err(|_| ProcessClientError::ChildIo)?
                .ok_or(ProcessClientError::ChildTermination);
        }
        child.wait().await.map_err(|_| ProcessClientError::ChildIo)
    }

    async fn finish_stderr(&mut self) -> FakeResult<CapturedStderr> {
        let mut capture = self
            .stderr_task
            .take()
            .ok_or(ProcessClientError::StderrCapture)?;
        match timeout(self.limits.request_timeout, &mut capture).await {
            Ok(Ok(Ok(stderr))) => Ok(stderr),
            _ => {
                capture.abort();
                let _ = capture.await;
                Err(ProcessClientError::StderrCapture)
            }
        }
    }
}

fn encode_request(request: &Value, max_line_bytes: usize) -> FakeResult<Vec<u8>> {
    let line = serde_json::to_vec(request).map_err(|_| ProcessClientError::RequestEncoding)?;
    if line.len() > max_line_bytes {
        return Err(ProcessClientError::RequestLineTooLong);
    }
    Ok(line)
}

async fn write_bounded_request_flood(
    stdin: ChildStdin,
    frames: Vec<Vec<u8>>,
    pressure_signal: oneshot::Sender<()>,
    deadline: Instant,
) -> FakeResult {
    let mut stdin = PressureObservedWriter::new(stdin, pressure_signal);
    match timeout_at(deadline, async {
        for frame in &frames {
            stdin
                .write_all(frame)
                .await
                .map_err(|_| ProcessClientError::ChildIo)?;
            stdin
                .write_all(b"\n")
                .await
                .map_err(|_| ProcessClientError::ChildIo)?;
        }
        stdin.flush().await.map_err(|_| ProcessClientError::ChildIo)
    })
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(ProcessClientError::RequestDeadline),
    }
}

impl Drop for ChildProcessClient {
    fn drop(&mut self) {
        self.close_stdin();
        let Some(mut child) = self.child.take() else {
            return;
        };
        let stderr_task = self.stderr_task.take();
        if stderr_task.is_none() && child.try_wait().ok().flatten().is_some() {
            return;
        }
        schedule_dropped_child_cleanup(child, stderr_task);
    }
}

fn schedule_dropped_child_cleanup(
    mut child: Child,
    stderr_task: Option<JoinHandle<io::Result<CapturedStderr>>>,
) {
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => {
            runtime.spawn(cleanup_dropped_child(child, stderr_task));
        }
        Err(_) => {
            let _ = child.start_kill();
            let _ = std::thread::Builder::new()
                .name("mcp-worker-fake-child-cleanup".to_owned())
                .spawn(move || {
                    if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                        .enable_io()
                        .build()
                    {
                        runtime.block_on(cleanup_dropped_child(child, stderr_task));
                    }
                });
        }
    }
}

async fn cleanup_dropped_child(
    mut child: Child,
    stderr_task: Option<JoinHandle<io::Result<CapturedStderr>>>,
) {
    if child.try_wait().ok().flatten().is_none() {
        let _ = child.start_kill();
    }
    let _ = child.wait().await;
    if let Some(stderr_task) = stderr_task {
        let _ = stderr_task.await;
    }
}

async fn capture_stderr(
    mut stderr: ChildStderr,
    capacity: usize,
    #[cfg(test)] stderr_capture_complete: oneshot::Sender<()>,
) -> io::Result<CapturedStderr> {
    let mut captured = CapturedStderr::new();
    let mut buffer = [0; 8_192];
    loop {
        let count = stderr.read(&mut buffer).await?;
        if count == 0 {
            #[cfg(test)]
            let _ = stderr_capture_complete.send(());
            return Ok(captured);
        }
        captured.append(&buffer[..count], capacity);
    }
}

async fn reap_unmanaged_child(child: &mut Child, timeout_duration: Duration) {
    if child.try_wait().ok().flatten().is_none() {
        let _ = child.start_kill();
    }
    let _ = timeout(timeout_duration, child.wait()).await;
}

#[allow(dead_code)]
enum BoundedLine {
    Line(Vec<u8>),
    TooLong,
    Unterminated,
    End,
}

#[allow(dead_code)]
async fn read_bounded_line<R>(reader: &mut R, max_line_bytes: usize) -> io::Result<BoundedLine>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    loop {
        let buffer = reader.fill_buf().await?;
        if buffer.is_empty() {
            return Ok(if line.is_empty() {
                BoundedLine::End
            } else {
                BoundedLine::Unterminated
            });
        }
        if let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
            if newline > max_line_bytes.saturating_sub(line.len()) {
                reader.consume(newline + 1);
                return Ok(BoundedLine::TooLong);
            }
            line.extend_from_slice(&buffer[..newline]);
            reader.consume(newline + 1);
            return Ok(BoundedLine::Line(line));
        }
        if buffer.len() > max_line_bytes.saturating_sub(line.len()) {
            return Ok(BoundedLine::TooLong);
        }
        let consumed = buffer.len();
        line.extend_from_slice(buffer);
        reader.consume(consumed);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BoundedLine, CapturedStderr, ChildProcessClient, FLOOD_PRESSURE_NOT_OBSERVED,
        MAX_RAW_TEST_LINE_BYTES, PressureObservedWriter, ProcessClientError, ProcessLimits,
        RequestFlood, parse_arguments, read_bounded_line,
    };
    use serde_json::json;
    use std::{io, time::Duration};
    use tokio::{
        io::{AsyncWriteExt, BufReader},
        process::Command,
        sync::oneshot,
        time::timeout,
    };

    fn limits(max_line_bytes: usize) -> ProcessLimits {
        ProcessLimits {
            request_timeout: Duration::from_secs(2),
            max_line_bytes,
            stderr_cap_bytes: 64,
        }
    }

    #[test]
    fn arguments_require_an_explicit_worker_path() {
        let error = parse_arguments(["mcp-worker-fake".to_owned()]).unwrap_err();

        assert!(matches!(
            error,
            ProcessClientError::InvalidArguments("--worker is required")
        ));
    }

    #[test]
    fn arguments_reject_non_positive_limits() {
        for option in [
            "--timeout-seconds",
            "--max-line-bytes",
            "--stderr-cap-bytes",
        ] {
            let error = parse_arguments([
                "mcp-worker-fake".to_owned(),
                "--worker".to_owned(),
                "/tmp/mcp-worker".to_owned(),
                option.to_owned(),
                "0".to_owned(),
            ])
            .unwrap_err();

            assert!(matches!(error, ProcessClientError::InvalidArguments(_)));
        }
    }

    #[tokio::test]
    async fn bounded_line_reader_rejects_an_oversized_response() {
        let (mut writer, reader) = tokio::io::duplex(16);
        writer.write_all(b"12345\n").await.unwrap();
        drop(writer);
        let mut reader = BufReader::new(reader);

        assert!(matches!(
            read_bounded_line(&mut reader, 4).await.unwrap(),
            BoundedLine::TooLong
        ));
    }

    #[test]
    fn stderr_capture_is_bounded_and_debug_redacted() {
        let mut stderr = CapturedStderr::new();
        stderr.append(b"secret", 6);
        stderr.append(b"-more", 6);

        assert!(stderr.contains(b"secret"));
        assert!(stderr.is_truncated());
        assert!(!format!("{stderr:?}").contains("secret"));
    }

    #[cfg(unix)]
    fn echo_child() -> Command {
        let mut command = Command::new("sh");
        command.args([
            "-c",
            "while IFS= read -r line; do printf '%s\\n' \"$line\"; done",
        ]);
        command
    }

    #[cfg(unix)]
    fn acknowledging_child() -> Command {
        let mut command = Command::new("sh");
        command.args(["-c", "while IFS= read -r _; do printf '{}\\n'; done"]);
        command
    }

    #[cfg(unix)]
    fn silent_child() -> Command {
        let mut command = Command::new("sh");
        command.args(["-c", "while IFS= read -r _; do :; done"]);
        command
    }

    #[cfg(unix)]
    fn live_stderr_child() -> Command {
        let mut command = Command::new("sh");
        command.args([
            "-c",
            "printf 'stderr-drain-sentinel' >&2; printf '{\"ready\":true}\\n'; exec sleep 1000",
        ]);
        command
    }

    #[cfg(unix)]
    unsafe extern "C" {
        fn waitpid(process_id: i32, status: *mut i32, options: i32) -> i32;
    }

    #[cfg(unix)]
    struct ReapGuard {
        process_id: Option<i32>,
    }

    #[cfg(unix)]
    impl ReapGuard {
        fn new(process_id: u32) -> Self {
            Self {
                process_id: Some(process_id.try_into().expect("child PID should fit in i32")),
            }
        }

        fn disarm(&mut self) {
            self.process_id = None;
        }
    }

    #[cfg(unix)]
    impl Drop for ReapGuard {
        fn drop(&mut self) {
            let Some(process_id) = self.process_id else {
                return;
            };
            let _ = std::process::Command::new("kill")
                .args(["-KILL", &process_id.to_string()])
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
            let mut status = 0;
            loop {
                // SAFETY: this test spawned the direct child and owns its PID.
                let result = unsafe { waitpid(process_id, &mut status, 0) };
                if result != -1 || io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                    return;
                }
            }
        }
    }

    #[cfg(unix)]
    async fn wait_for_process_to_be_reaped(process_id: u32) {
        timeout(Duration::from_secs(2), async {
            loop {
                let status = std::process::Command::new("kill")
                    .args(["-0", &process_id.to_string()])
                    .stdin(std::process::Stdio::null())
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status()
                    .expect("kill utility should be available");
                if !status.success() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropped client should reap its child");
    }

    #[cfg(unix)]
    fn assert_request_failure_reaped(client: &mut ChildProcessClient) {
        assert!(
            client
                .child
                .as_mut()
                .expect("client should retain its child handle")
                .try_wait()
                .expect("controlled child status should be readable")
                .is_some(),
            "request failure must reap the child before returning"
        );
        assert!(
            client.stderr_task.is_none(),
            "request failure must finish stderr capture before returning"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn process_client_round_trips_a_controlled_child() {
        let mut client = ChildProcessClient::start(echo_child(), limits(128))
            .await
            .expect("controlled child should start");

        client
            .send_request(&json!({"jsonrpc": "2.0", "id": 1, "method": "test"}))
            .await
            .expect("controlled child should receive one request");
        let response = client
            .read_response()
            .await
            .expect("controlled child should return one response");

        assert_eq!(response["id"], 1);
        client
            .stop_successfully()
            .await
            .expect("controlled child should exit after EOF");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn raw_test_line_bypasses_the_request_limit_without_affecting_response_bounds() {
        let mut client = ChildProcessClient::start(acknowledging_child(), limits(8))
            .await
            .expect("controlled child should start");

        client
            .send_raw_test_line(b"123456789")
            .await
            .expect("raw test line should bypass the normal request limit");
        assert_eq!(
            client
                .read_response()
                .await
                .expect("acknowledging child should return a bounded response"),
            json!({})
        );
        client
            .stop_successfully()
            .await
            .expect("controlled child should exit after EOF");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn raw_test_line_rejection_is_bounded_and_reaps_the_child() {
        let mut client = ChildProcessClient::start(silent_child(), limits(8))
            .await
            .expect("controlled child should start");
        let oversized = vec![b'x'; MAX_RAW_TEST_LINE_BYTES + 1];

        let error = client.send_raw_test_line(&oversized).await.unwrap_err();

        assert!(matches!(error, ProcessClientError::RawTestLineTooLong));
        assert_request_failure_reaped(&mut client);
    }

    #[tokio::test]
    async fn pending_write_observer_reports_async_write_pressure() {
        let (writer, _reader) = tokio::io::duplex(1);
        let (pressure_observed, pressure_observer) = oneshot::channel();
        let mut writer = PressureObservedWriter::new(writer, pressure_observed);
        let write = tokio::spawn(async move { writer.write_all(b"ab").await });

        let observed = timeout(Duration::from_secs(2), pressure_observer).await;
        write.abort();
        assert!(
            write
                .await
                .expect_err("blocked write should be cancelled")
                .is_cancelled()
        );
        assert!(
            observed.is_ok(),
            "observer should signal after an AsyncWrite pending result"
        );
        assert!(
            observed
                .expect("observer timeout was checked above")
                .is_ok(),
            "writer should retain the pressure observer until it signals"
        );
    }

    #[tokio::test]
    async fn bounded_flood_reports_stable_failure_without_a_pressure_signal() {
        let (pressure_signal, pressure_observed) = oneshot::channel();
        drop(pressure_signal);
        let mut flood = RequestFlood {
            pressure_observed,
            completion: tokio::spawn(async { Ok::<(), ProcessClientError>(()) }),
            timeout: Duration::from_secs(2),
        };

        let error = flood.wait_until_pressure_observed().await.unwrap_err();
        assert!(matches!(
            error,
            ProcessClientError::ProtocolScenario(FLOOD_PRESSURE_NOT_OBSERVED)
        ));
        flood
            .finish()
            .await
            .expect("the completed flood writer should remain joinable");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn process_client_rejects_an_oversized_request_without_writing_it() {
        let mut client = ChildProcessClient::start(echo_child(), limits(16))
            .await
            .expect("controlled child should start");

        let error = client
            .send_request(&json!({"payload": "this request is too large"}))
            .await
            .unwrap_err();

        assert!(matches!(error, ProcessClientError::RequestLineTooLong));
        assert_request_failure_reaped(&mut client);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn process_client_request_deadline_reaps_the_child_automatically() {
        let mut client = ChildProcessClient::start(
            silent_child(),
            ProcessLimits {
                request_timeout: Duration::from_millis(50),
                max_line_bytes: 128,
                stderr_cap_bytes: 64,
            },
        )
        .await
        .expect("controlled child should start");

        let error = client.request(&json!({"id": 1})).await.unwrap_err();

        assert!(matches!(error, ProcessClientError::RequestDeadline));
        assert_request_failure_reaped(&mut client);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancelling_a_live_client_reaps_its_child_and_drains_stderr() {
        let mut client = ChildProcessClient::start(live_stderr_child(), limits(128))
            .await
            .expect("controlled child should start");
        let ready = client
            .read_response()
            .await
            .expect("controlled child should report readiness");
        assert_eq!(ready["ready"], true);
        let process_id = client
            .child
            .as_ref()
            .expect("client should retain its child handle")
            .id()
            .expect("controlled child should still be running");
        let mut reap_guard = ReapGuard::new(process_id);
        let stderr_capture_complete = client.take_stderr_capture_complete();
        let (started, entered) = oneshot::channel();
        let task = tokio::spawn(async move {
            started
                .send(())
                .expect("cancellation task should have one observer");
            std::future::pending::<()>().await;
            drop(client);
        });

        entered
            .await
            .expect("cancellation task should retain the live client");
        task.abort();
        assert!(
            task.await
                .expect_err("cancellation task should not complete")
                .is_cancelled()
        );
        timeout(Duration::from_secs(2), stderr_capture_complete)
            .await
            .expect("dropped client should drain stderr")
            .expect("stderr capture should finish instead of being aborted");
        wait_for_process_to_be_reaped(process_id).await;
        reap_guard.disarm();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn process_client_rejects_an_oversized_child_response() {
        let mut command = Command::new("sh");
        command.args([
            "-c",
            "printf '123456789\\n'; while IFS= read -r _; do :; done",
        ]);
        let mut client = ChildProcessClient::start(command, limits(8))
            .await
            .expect("controlled child should start");

        let error = client.read_response().await.unwrap_err();

        assert!(matches!(error, ProcessClientError::ResponseLineTooLong));
        assert_request_failure_reaped(&mut client);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn process_client_rejects_truncated_stderr_without_exposing_it() {
        let mut command = Command::new("sh");
        command.args(["-c", "printf 'stderr-private-sentinel' >&2"]);
        let client = ChildProcessClient::start(
            command,
            ProcessLimits {
                request_timeout: Duration::from_secs(2),
                max_line_bytes: 128,
                stderr_cap_bytes: 4,
            },
        )
        .await
        .expect("controlled child should start");

        let error = client.stop_successfully().await.unwrap_err();

        assert!(matches!(error, ProcessClientError::StderrLimitExceeded));
        assert!(!error.to_string().contains("stderr-private-sentinel"));
    }
}
