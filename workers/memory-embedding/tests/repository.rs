use std::{
    future::pending,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use iii_sdk::{Error as IiiError, protocol::TriggerRequest};
use memory_embedding::{
    DatabaseExecutor, IiiEmbeddingWorkRepository,
    contracts::{
        Deadline, EmbeddingError, EmbeddingWorkItem, EmbeddingWorkRepository, LoadedEmbeddingWork,
        MemoryKey, RepositoryFailure,
    },
};
use memory_store::contracts::DatabaseTarget;
use serde_json::{Value, json};

const DATABASE: &str = "database-target-sentinel";
const PENDING_ID: &str = "pending-key-sentinel";
const PRESENT_ID: &str = "present-key-sentinel";
const MISSING_ID: &str = "missing-key-sentinel";
const HISTORICAL_ID: &str = "historical-key-sentinel";
const LATER_ID: &str = "later-key-sentinel";
const TITLE_SENTINEL: &str = "title-sentinel";
const CONTENT_SENTINEL: &str = "content-sentinel";
const CONCEPT_SENTINEL: &str = "concept-sentinel";
const VECTOR_SENTINEL: &str = "vector-sentinel";
const LAST_INSERT_ID_SENTINEL: &str = "last-insert-id-sentinel";
const SDK_CODE_SENTINEL: &str = "sdk-code-sentinel";
const SDK_MESSAGE_SENTINEL: &str = "sdk-message-sentinel";
const SDK_STACKTRACE_SENTINEL: &str = "sdk-stacktrace-sentinel";
const DATABASE_TIMEOUT: Duration = Duration::from_secs(1);
const INVOCATION_TIMEOUT: Duration = Duration::from_secs(2);
const EARLIER_DATABASE_TIMEOUT: Duration = Duration::from_millis(50);
const OUTER_TIMEOUT: Duration = Duration::from_millis(500);

const EXPECTED_LOAD_KEYS_SQL: &str = r#"WITH requested AS (
    SELECT
        item.value->>'id' AS id,
        item.value->>'version' AS version,
        item.ordinality
    FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS item(value, ordinality)
)
SELECT
    requested.id,
    requested.version,
    CASE
        WHEN memory.id IS NOT NULL AND embedding.id IS NULL THEN memory.title
    END AS title,
    CASE
        WHEN memory.id IS NOT NULL AND embedding.id IS NULL THEN memory.content
    END AS content,
    CASE
        WHEN memory.id IS NOT NULL AND embedding.id IS NULL THEN to_json(memory.concepts)
    END AS concepts,
    memory.id IS NOT NULL AS memory_present,
    embedding.id IS NOT NULL AS embedding_present
FROM requested
LEFT JOIN public.memories AS memory
    ON memory.id = requested.id
   AND memory.version = requested.version::bigint
LEFT JOIN public.memory_embeddings AS embedding
    ON embedding.id = memory.id
   AND embedding.version = memory.version
ORDER BY requested.ordinality ASC"#;

const EXPECTED_LIST_MISSING_SQL: &str = r#"SELECT
    memory.id,
    memory.version::text AS version,
    memory.title,
    memory.content,
    to_json(memory.concepts) AS concepts
FROM public.memories AS memory
LEFT JOIN public.memory_embeddings AS embedding
    ON embedding.id = memory.id
   AND embedding.version = memory.version
WHERE embedding.id IS NULL
ORDER BY memory.id COLLATE "C" ASC,
         memory.version ASC
LIMIT $1::text::bigint"#;

#[derive(Clone)]
struct FakeDatabaseExecutor {
    state: Arc<Mutex<FakeDatabaseState>>,
}

struct FakeDatabaseState {
    response: FakeDatabaseResponse,
    requests: Vec<CapturedRequest>,
}

#[derive(Clone)]
enum FakeDatabaseResponse {
    Response(Value),
    BackendFailure,
    Pending,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CapturedRequest {
    function_id: String,
    payload: Value,
    action_is_none: bool,
    timeout_ms: Option<u64>,
}

impl FakeDatabaseExecutor {
    fn new(response: FakeDatabaseResponse) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeDatabaseState {
                response,
                requests: Vec::new(),
            })),
        }
    }

    fn requests(&self) -> Vec<CapturedRequest> {
        self.state
            .lock()
            .expect("test database state lock should not be poisoned")
            .requests
            .clone()
    }
}

#[async_trait]
impl DatabaseExecutor for FakeDatabaseExecutor {
    async fn execute(&self, request: TriggerRequest) -> Result<Value, IiiError> {
        let response = {
            let mut state = self
                .state
                .lock()
                .expect("test database state lock should not be poisoned");
            state.requests.push(CapturedRequest {
                function_id: request.function_id,
                payload: request.payload,
                action_is_none: request.action.is_none(),
                timeout_ms: request.timeout_ms,
            });
            state.response.clone()
        };

        match response {
            FakeDatabaseResponse::Response(response) => Ok(response),
            FakeDatabaseResponse::BackendFailure => Err(IiiError::Remote {
                code: SDK_CODE_SENTINEL.to_owned(),
                message: SDK_MESSAGE_SENTINEL.to_owned(),
                stacktrace: Some(SDK_STACKTRACE_SENTINEL.to_owned()),
            }),
            FakeDatabaseResponse::Pending => pending().await,
        }
    }
}

fn database() -> DatabaseTarget {
    DatabaseTarget::try_from(DATABASE.to_owned()).expect("test database target should be valid")
}

fn repository(
    executor: FakeDatabaseExecutor,
    database_timeout: Duration,
) -> IiiEmbeddingWorkRepository<FakeDatabaseExecutor> {
    IiiEmbeddingWorkRepository::with_executor(executor, database(), database_timeout)
}

fn keys() -> Vec<MemoryKey> {
    vec![
        MemoryKey::try_new(PENDING_ID, "2").expect("test key should be valid"),
        MemoryKey::try_new(PRESENT_ID, "7").expect("test key should be valid"),
        MemoryKey::try_new(MISSING_ID, "11").expect("test key should be valid"),
    ]
}

fn load_response(rows: Vec<Value>) -> Value {
    json!({
        "affected_rows": rows.len(),
        "last_insert_id": null,
        "returned_rows": rows,
    })
}

fn missing_work_row(id: &str, version: &str) -> Value {
    json!({
        "id": id,
        "version": version,
        "title": TITLE_SENTINEL,
        "content": CONTENT_SENTINEL,
        "concepts": [CONCEPT_SENTINEL, "", CONCEPT_SENTINEL],
    })
}

fn pending_row(id: &str, version: &str) -> Value {
    json!({
        "id": id,
        "version": version,
        "title": TITLE_SENTINEL,
        "content": CONTENT_SENTINEL,
        "concepts": [CONCEPT_SENTINEL, "", CONCEPT_SENTINEL],
        "memory_present": true,
        "embedding_present": false,
    })
}

fn already_present_row(id: &str, version: &str) -> Value {
    json!({
        "id": id,
        "version": version,
        "title": null,
        "content": null,
        "concepts": null,
        "memory_present": true,
        "embedding_present": true,
    })
}

fn missing_row(id: &str, version: &str) -> Value {
    json!({
        "id": id,
        "version": version,
        "title": null,
        "content": null,
        "concepts": null,
        "memory_present": false,
        "embedding_present": false,
    })
}

fn assert_opaque(error: &EmbeddingError) {
    let display = error.to_string();
    let debug = format!("{error:?}");

    for sentinel in [
        DATABASE,
        PENDING_ID,
        PRESENT_ID,
        MISSING_ID,
        HISTORICAL_ID,
        LATER_ID,
        TITLE_SENTINEL,
        CONTENT_SENTINEL,
        CONCEPT_SENTINEL,
        VECTOR_SENTINEL,
        LAST_INSERT_ID_SENTINEL,
        SDK_CODE_SENTINEL,
        SDK_MESSAGE_SENTINEL,
        SDK_STACKTRACE_SENTINEL,
    ] {
        assert!(
            !display.contains(sentinel),
            "Display error leaked protected sentinel {sentinel}: {display}"
        );
        assert!(
            !debug.contains(sentinel),
            "Debug error leaked protected sentinel {sentinel}: {debug}"
        );
    }
}

#[tokio::test]
async fn list_missing_uses_one_static_bounded_anti_join_for_historical_versions() {
    let executor = FakeDatabaseExecutor::new(FakeDatabaseResponse::Response(load_response(vec![
        missing_work_row(HISTORICAL_ID, "2"),
        missing_work_row(HISTORICAL_ID, "9"),
        missing_work_row(LATER_ID, "1"),
    ])));
    let repository = repository(executor.clone(), DATABASE_TIMEOUT);

    let selected = EmbeddingWorkRepository::list_missing(
        &repository,
        3,
        Deadline::after(Duration::from_secs(1)),
    )
    .await
    .expect("a complete ordered database result should decode");

    assert_eq!(
        selected,
        vec![
            EmbeddingWorkItem::new(
                MemoryKey::try_new(HISTORICAL_ID, "2").expect("test key should be valid"),
                TITLE_SENTINEL,
                CONTENT_SENTINEL,
                vec![
                    CONCEPT_SENTINEL.to_owned(),
                    String::new(),
                    CONCEPT_SENTINEL.to_owned(),
                ],
            ),
            EmbeddingWorkItem::new(
                MemoryKey::try_new(HISTORICAL_ID, "9").expect("test key should be valid"),
                TITLE_SENTINEL,
                CONTENT_SENTINEL,
                vec![
                    CONCEPT_SENTINEL.to_owned(),
                    String::new(),
                    CONCEPT_SENTINEL.to_owned(),
                ],
            ),
            EmbeddingWorkItem::new(
                MemoryKey::try_new(LATER_ID, "1").expect("test key should be valid"),
                TITLE_SENTINEL,
                CONTENT_SENTINEL,
                vec![
                    CONCEPT_SENTINEL.to_owned(),
                    String::new(),
                    CONCEPT_SENTINEL.to_owned(),
                ],
            ),
        ]
    );

    let requests = executor.requests();
    assert_eq!(
        requests.len(),
        1,
        "the adapter must not retry missing-work selection"
    );
    assert_eq!(
        requests[0],
        CapturedRequest {
            function_id: "database::execute".to_owned(),
            payload: json!({
                "db": DATABASE,
                "sql": EXPECTED_LIST_MISSING_SQL,
                "params": ["3"],
            }),
            action_is_none: true,
            timeout_ms: None,
        }
    );

    let sql = EXPECTED_LIST_MISSING_SQL.to_ascii_lowercase();
    for forbidden in [
        "memory_search_heads",
        "current",
        "latest",
        "attempt",
        "claim",
        "cursor",
        "skip",
        "vector",
        "files",
        "session_ids",
        "source_observation_ids",
        "memory.type",
        "created_at",
        "updated_at",
    ] {
        assert!(
            !sql.contains(forbidden),
            "the work query must not read or persist {forbidden}"
        );
    }
}

#[tokio::test]
async fn list_missing_accepts_an_empty_page_and_reselects_the_same_first_page() {
    let empty_executor =
        FakeDatabaseExecutor::new(FakeDatabaseResponse::Response(load_response(vec![])));
    let empty_repository = repository(empty_executor.clone(), DATABASE_TIMEOUT);

    let empty = EmbeddingWorkRepository::list_missing(
        &empty_repository,
        7,
        Deadline::after(Duration::from_secs(1)),
    )
    .await
    .expect("an empty missing-work page should be successful");

    assert!(empty.is_empty());
    assert_eq!(
        empty_executor.requests().len(),
        1,
        "an empty page must use one database request"
    );

    let response = load_response(vec![
        missing_work_row(HISTORICAL_ID, "2"),
        missing_work_row(HISTORICAL_ID, "9"),
    ]);
    let executor = FakeDatabaseExecutor::new(FakeDatabaseResponse::Response(response));
    let repository = repository(executor.clone(), DATABASE_TIMEOUT);

    let first = EmbeddingWorkRepository::list_missing(
        &repository,
        2,
        Deadline::after(Duration::from_secs(1)),
    )
    .await
    .expect("the first page should decode");
    let second = EmbeddingWorkRepository::list_missing(
        &repository,
        2,
        Deadline::after(Duration::from_secs(1)),
    )
    .await
    .expect("the same database page should decode again");

    assert_eq!(first, second, "selection must not persist pagination state");
    let requests = executor.requests();
    assert_eq!(
        requests.len(),
        2,
        "each selection must be one database request"
    );
    assert_eq!(requests[0].payload, requests[1].payload);
}

#[tokio::test]
async fn list_missing_rejects_malformed_unordered_or_over_limit_responses_all_or_error() {
    let mut unexpected_row = missing_work_row(HISTORICAL_ID, "2");
    unexpected_row
        .as_object_mut()
        .expect("row is an object")
        .insert("vector".to_owned(), json!(VECTOR_SENTINEL));
    let mut missing_concepts = missing_work_row(HISTORICAL_ID, "2");
    missing_concepts
        .as_object_mut()
        .expect("row is an object")
        .remove("concepts");
    let mut incomplete_title = missing_work_row(HISTORICAL_ID, "2");
    incomplete_title["title"] = Value::Null;

    let malformed_responses = vec![
        (Value::Null, 1),
        (
            json!({
                "affected_rows": 0,
                "last_insert_id": null,
                "returned_rows": [],
                "extra": "unexpected",
            }),
            1,
        ),
        (
            json!({
                "affected_rows": 1,
                "last_insert_id": LAST_INSERT_ID_SENTINEL,
                "returned_rows": [missing_work_row(HISTORICAL_ID, "2")],
            }),
            1,
        ),
        (
            json!({
                "affected_rows": 1,
                "last_insert_id": null,
                "returned_rows": [],
            }),
            1,
        ),
        (
            json!({
                "affected_rows": 1,
                "last_insert_id": null,
                "returned_rows": {},
            }),
            1,
        ),
        (load_response(vec![unexpected_row]), 1),
        (load_response(vec![missing_concepts]), 1),
        (load_response(vec![incomplete_title]), 1),
        (
            load_response(vec![missing_work_row(HISTORICAL_ID, "02")]),
            1,
        ),
        (load_response(vec![missing_work_row(HISTORICAL_ID, "0")]), 1),
        (
            load_response(vec![
                missing_work_row(LATER_ID, "1"),
                missing_work_row(HISTORICAL_ID, "2"),
            ]),
            2,
        ),
        (
            load_response(vec![
                missing_work_row(HISTORICAL_ID, "9"),
                missing_work_row(HISTORICAL_ID, "2"),
            ]),
            2,
        ),
        (
            load_response(vec![
                missing_work_row(HISTORICAL_ID, "2"),
                missing_work_row(HISTORICAL_ID, "9"),
            ]),
            1,
        ),
    ];

    for (response, limit) in malformed_responses {
        let executor = FakeDatabaseExecutor::new(FakeDatabaseResponse::Response(response));
        let repository = repository(executor.clone(), DATABASE_TIMEOUT);

        let error = EmbeddingWorkRepository::list_missing(
            &repository,
            limit,
            Deadline::after(Duration::from_secs(1)),
        )
        .await
        .expect_err("malformed pages must not return partial work");

        assert_eq!(
            error,
            EmbeddingError::repository(RepositoryFailure::MalformedResponse)
        );
        assert_eq!(
            executor.requests().len(),
            1,
            "the adapter must not retry malformed responses"
        );
        assert_opaque(&error);
    }
}

#[tokio::test]
async fn list_missing_maps_backend_and_local_timeout_to_opaque_failures_without_retries() {
    let backend = FakeDatabaseExecutor::new(FakeDatabaseResponse::BackendFailure);
    let backend_repository = repository(backend.clone(), DATABASE_TIMEOUT);

    let backend_error = EmbeddingWorkRepository::list_missing(
        &backend_repository,
        1,
        Deadline::after(Duration::from_secs(1)),
    )
    .await
    .expect_err("iii failures must become opaque repository failures");

    assert_eq!(
        backend_error,
        EmbeddingError::repository(RepositoryFailure::Backend)
    );
    assert_eq!(backend.requests().len(), 1, "the adapter must not retry");
    assert_opaque(&backend_error);

    let timeout = FakeDatabaseExecutor::new(FakeDatabaseResponse::Pending);
    let timeout_repository = repository(timeout.clone(), DATABASE_TIMEOUT);

    let timeout_error = EmbeddingWorkRepository::list_missing(
        &timeout_repository,
        1,
        Deadline::after(Duration::from_millis(25)),
    )
    .await
    .expect_err("a local deadline must stop waiting for the database response");

    assert_eq!(
        timeout_error,
        EmbeddingError::repository(RepositoryFailure::Timeout)
    );
    assert_eq!(timeout.requests().len(), 1, "the adapter must not retry");
    assert_opaque(&timeout_error);
}

#[tokio::test]
async fn load_keys_uses_one_static_positional_request_and_preserves_all_work_states() {
    let keys = keys();
    let executor = FakeDatabaseExecutor::new(FakeDatabaseResponse::Response(load_response(vec![
        pending_row(PENDING_ID, "2"),
        already_present_row(PRESENT_ID, "7"),
        missing_row(MISSING_ID, "11"),
    ])));
    let repository = repository(executor.clone(), DATABASE_TIMEOUT);

    let loaded = repository
        .load_keys(&keys, Deadline::after(Duration::from_secs(1)))
        .await
        .expect("a complete ordered database result should decode");

    assert_eq!(
        loaded,
        vec![
            LoadedEmbeddingWork::Pending(EmbeddingWorkItem::new(
                keys[0].clone(),
                TITLE_SENTINEL,
                CONTENT_SENTINEL,
                vec![
                    CONCEPT_SENTINEL.to_owned(),
                    String::new(),
                    CONCEPT_SENTINEL.to_owned(),
                ],
            )),
            LoadedEmbeddingWork::AlreadyPresent(keys[1].clone()),
            LoadedEmbeddingWork::Missing(keys[2].clone()),
        ]
    );

    let requests = executor.requests();
    assert_eq!(
        requests.len(),
        1,
        "the adapter must not retry database reads"
    );
    assert_eq!(
        requests[0],
        CapturedRequest {
            function_id: "database::execute".to_owned(),
            payload: json!({
                "db": DATABASE,
                "sql": EXPECTED_LOAD_KEYS_SQL,
                "params": [[
                    { "id": PENDING_ID, "version": "2" },
                    { "id": PRESENT_ID, "version": "7" },
                    { "id": MISSING_ID, "version": "11" },
                ]],
            }),
            action_is_none: true,
            timeout_ms: None,
        }
    );

    let sql = EXPECTED_LOAD_KEYS_SQL.to_ascii_lowercase();
    for forbidden in [
        "vector",
        "files",
        "session_ids",
        "source_observation_ids",
        "memory.type",
        "created_at",
        "updated_at",
        "memory_search_heads",
    ] {
        assert!(
            !sql.contains(forbidden),
            "the work query must not read protected field {forbidden}"
        );
    }
}

#[tokio::test]
async fn load_keys_rejects_malformed_or_unexpected_responses_all_or_error() {
    let keys = keys();
    let mut unexpected_field = pending_row(PENDING_ID, "2");
    unexpected_field
        .as_object_mut()
        .expect("row is an object")
        .insert("vector".to_owned(), json!(VECTOR_SENTINEL));
    let mut missing_pending_content = pending_row(PENDING_ID, "2");
    missing_pending_content["content"] = Value::Null;
    let mut unexpected_present_content = already_present_row(PRESENT_ID, "7");
    unexpected_present_content["title"] = json!(TITLE_SENTINEL);
    let invalid_embedding_state = json!({
        "id": MISSING_ID,
        "version": "11",
        "title": null,
        "content": null,
        "concepts": null,
        "memory_present": false,
        "embedding_present": true,
    });
    let reversed = load_response(vec![
        already_present_row(PRESENT_ID, "7"),
        pending_row(PENDING_ID, "2"),
        missing_row(MISSING_ID, "11"),
    ]);

    let malformed_responses = vec![
        Value::Null,
        json!({
            "affected_rows": 3,
            "last_insert_id": null,
            "returned_rows": [pending_row(PENDING_ID, "2")],
        }),
        json!({
            "affected_rows": 3,
            "last_insert_id": LAST_INSERT_ID_SENTINEL,
            "returned_rows": [
                pending_row(PENDING_ID, "2"),
                already_present_row(PRESENT_ID, "7"),
                missing_row(MISSING_ID, "11"),
            ],
        }),
        load_response(vec![
            pending_row(PENDING_ID, "2"),
            already_present_row(PRESENT_ID, "7"),
            invalid_embedding_state,
        ]),
        load_response(vec![
            missing_pending_content,
            already_present_row(PRESENT_ID, "7"),
            missing_row(MISSING_ID, "11"),
        ]),
        load_response(vec![
            unexpected_field,
            already_present_row(PRESENT_ID, "7"),
            missing_row(MISSING_ID, "11"),
        ]),
        load_response(vec![
            pending_row(PENDING_ID, "2"),
            unexpected_present_content,
            missing_row(MISSING_ID, "11"),
        ]),
        reversed,
    ];

    for response in malformed_responses {
        let executor = FakeDatabaseExecutor::new(FakeDatabaseResponse::Response(response));
        let repository = repository(executor.clone(), DATABASE_TIMEOUT);

        let error = repository
            .load_keys(&keys, Deadline::after(Duration::from_secs(1)))
            .await
            .expect_err("malformed responses must not return partial work");

        assert_eq!(
            error,
            EmbeddingError::repository(RepositoryFailure::MalformedResponse)
        );
        assert_eq!(executor.requests().len(), 1, "the adapter must not retry");
        assert_opaque(&error);
    }
}

#[tokio::test]
async fn load_keys_maps_backend_and_local_timeout_to_opaque_failures_without_retries() {
    let keys = keys();
    let backend = FakeDatabaseExecutor::new(FakeDatabaseResponse::BackendFailure);
    let backend_repository = repository(backend.clone(), DATABASE_TIMEOUT);

    let backend_error = backend_repository
        .load_keys(&keys, Deadline::after(Duration::from_secs(1)))
        .await
        .expect_err("iii failures must become opaque repository failures");

    assert_eq!(
        backend_error,
        EmbeddingError::repository(RepositoryFailure::Backend)
    );
    assert_eq!(backend.requests().len(), 1, "the adapter must not retry");
    assert_opaque(&backend_error);

    let timeout = FakeDatabaseExecutor::new(FakeDatabaseResponse::Pending);
    let timeout_repository = repository(timeout.clone(), DATABASE_TIMEOUT);

    let timeout_error = timeout_repository
        .load_keys(&keys, Deadline::after(Duration::from_millis(25)))
        .await
        .expect_err("a local deadline must stop waiting for the database response");

    assert_eq!(
        timeout_error,
        EmbeddingError::repository(RepositoryFailure::Timeout)
    );
    assert_eq!(timeout.requests().len(), 1, "the adapter must not retry");
    assert_opaque(&timeout_error);
}

#[tokio::test]
async fn repository_uses_the_configured_database_timeout_before_the_invocation_deadline() {
    let load_executor = FakeDatabaseExecutor::new(FakeDatabaseResponse::Pending);
    let load_repository = repository(load_executor.clone(), EARLIER_DATABASE_TIMEOUT);
    let load_keys = keys();

    let load_error = tokio::time::timeout(
        OUTER_TIMEOUT,
        load_repository.load_keys(&load_keys, Deadline::after(INVOCATION_TIMEOUT)),
    )
    .await
    .expect("the configured database timeout must precede the invocation deadline")
    .expect_err("the local database timeout should stop loading");
    assert_eq!(
        load_error,
        EmbeddingError::repository(RepositoryFailure::Timeout)
    );
    assert_eq!(
        load_executor.requests().len(),
        1,
        "the adapter must not retry"
    );
    assert_opaque(&load_error);

    let missing_executor = FakeDatabaseExecutor::new(FakeDatabaseResponse::Pending);
    let missing_repository = repository(missing_executor.clone(), EARLIER_DATABASE_TIMEOUT);
    let missing_error = tokio::time::timeout(OUTER_TIMEOUT, async {
        EmbeddingWorkRepository::list_missing(
            &missing_repository,
            1,
            Deadline::after(INVOCATION_TIMEOUT),
        )
        .await
    })
    .await
    .expect("the configured database timeout must precede the invocation deadline")
    .expect_err("the local database timeout should stop missing-work selection");
    assert_eq!(
        missing_error,
        EmbeddingError::repository(RepositoryFailure::Timeout)
    );
    assert_eq!(
        missing_executor.requests().len(),
        1,
        "the adapter must not retry"
    );
    assert_opaque(&missing_error);
}
