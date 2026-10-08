use std::{error::Error, fmt, time::Duration};

use chrono::{TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use iii_sdk::{IIIClient, InitOptions, WorkerIdentityMode, register_worker};
use memory_store::{
    contracts::{
        Bm25Search, DatabaseError, DatabaseOperation, DatabaseTarget, EmbeddingInput,
        MemorySearchResult, MemoryVersionInput, ValidatedBm25Search, ValidatedEmbedding,
        ValidatedMemoryVersion, ValidatedVectorSearch, VectorSearch,
    },
    database::{IiiMemoryDatabase, MemoryDatabase},
};
use serde_json::{Map, Value, json};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
use uuid::Uuid;

const DATABASE_EXECUTE_FUNCTION_ID: &str = "database::execute";
const WORKER_REGISTER_FUNCTION_ID: &str = "engine::workers::register";
const DATABASE_TARGET: &str = "memory-store-protocol-fake";
const OPERATION_TIMEOUT: Duration = Duration::from_secs(5);

const MEMORY_ID: &str = "memory-id-private-sentinel";
const MEMORY_TYPE: &str = "memory-type-private-sentinel";
const MEMORY_TITLE: &str = "memory-title-private-sentinel";
const MEMORY_CONTENT: &str = "memory-content-private-sentinel";
const CONCEPT: &str = "memory-concept-private-sentinel";
const FILE: &str = "memory-file-private-sentinel";
const SESSION: &str = "memory-session-private-sentinel";
const SOURCE: &str = "memory-source-private-sentinel";
const BM25_QUERY: &str = "bm25-query-private-sentinel";
const VECTOR_COMPONENT: &str = "987654.25";
const SEARCH_ID: &str = "search-id-private-sentinel";
const SEARCH_TYPE: &str = "search-type-private-sentinel";
const SEARCH_TITLE: &str = "search-title-private-sentinel";
const SEARCH_CONTENT: &str = "search-content-private-sentinel";
const SEARCH_CONCEPT: &str = "search-concept-private-sentinel";
const SEARCH_FILE: &str = "search-file-private-sentinel";
const SEARCH_SESSION: &str = "search-session-private-sentinel";
const SEARCH_SOURCE: &str = "search-source-private-sentinel";
const REMOTE_CODE: &str = "remote-code-private-sentinel";
const REMOTE_MESSAGE: &str = "remote-message-private-sentinel";
const REMOTE_STACKTRACE: &str = "remote-stacktrace-private-sentinel";
const SQL_SENTINEL: &str = "sql-private-sentinel";

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

const INSERT_EMBEDDING_SQL: &str = r#"WITH parent AS MATERIALIZED (
    SELECT id, version
    FROM public.memories
    WHERE id = $1::text AND version = $2::text::bigint
),
inserted AS (
    INSERT INTO public.memory_embeddings (id, version, embedding)
    SELECT id, version, $3::text::vector
    FROM parent
    ON CONFLICT (id, version) DO NOTHING
    RETURNING id, version
)
SELECT outcome, id, version::text AS version
FROM (
    SELECT 'inserted'::text AS outcome, id, version FROM inserted
    UNION ALL
    SELECT 'conflict'::text AS outcome, id, version FROM parent
    WHERE NOT EXISTS (SELECT 1 FROM inserted)
    UNION ALL
    SELECT 'missing'::text AS outcome, $1::text AS id, $2::text::bigint AS version
    WHERE NOT EXISTS (SELECT 1 FROM parent)
) AS outcome_row"#;

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

type VerificationResult<T = ()> = Result<T, VerificationError>;

#[derive(Clone, Copy, Debug)]
struct VerificationError;

impl fmt::Display for VerificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("database protocol verification failed")
    }
}

impl Error for VerificationError {}

#[derive(Clone, Copy)]
enum AdapterOperation {
    InsertMemory,
    InsertEmbedding,
    SearchBm25,
    SearchVector,
}

impl AdapterOperation {
    const ALL: [Self; 4] = [
        Self::InsertMemory,
        Self::InsertEmbedding,
        Self::SearchBm25,
        Self::SearchVector,
    ];

    const fn database_operation(self) -> DatabaseOperation {
        match self {
            Self::InsertMemory => DatabaseOperation::InsertMemory,
            Self::InsertEmbedding => DatabaseOperation::InsertEmbedding,
            Self::SearchBm25 => DatabaseOperation::SearchBm25,
            Self::SearchVector => DatabaseOperation::SearchVector,
        }
    }
}

#[derive(Clone)]
struct ExpectedInvocation {
    payload: Value,
    reply: Reply,
}

#[derive(Clone)]
enum Reply {
    Result(Value),
    RemoteFailure,
}

struct Fixture {
    memory: ValidatedMemoryVersion,
    embedding: ValidatedEmbedding,
    bm25: ValidatedBm25Search,
    vector: ValidatedVectorSearch,
}

struct FakeSession {
    client: IIIClient,
    server: JoinHandle<VerificationResult>,
}

impl FakeSession {
    async fn start(expected: Vec<ExpectedInvocation>) -> VerificationResult<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .map_err(|_| VerificationError)?;
        let address = listener.local_addr().map_err(|_| VerificationError)?;
        let server = tokio::spawn(run_fake(listener, expected));
        let client = register_worker(
            &format!("ws://127.0.0.1:{}", address.port()),
            InitOptions {
                identity: WorkerIdentityMode::Explicit,
                ..InitOptions::default()
            },
        );

        if !matches!(
            timeout(
                OPERATION_TIMEOUT,
                client.wait_until_registered(OPERATION_TIMEOUT)
            )
            .await,
            Ok(Ok(()))
        ) {
            client.shutdown_async().await;
            server.abort();
            let _ = server.await;
            return verification_failure();
        }

        Ok(Self { client, server })
    }

    async fn finish(self) -> VerificationResult {
        let Self { client, server } = self;
        client.shutdown_async().await;
        drop(client);

        match timeout(OPERATION_TIMEOUT, server).await {
            Ok(Ok(Ok(()))) => Ok(()),
            _ => verification_failure(),
        }
    }
}

#[tokio::main]
async fn main() -> VerificationResult {
    run_protocol_verification().await
}

async fn run_protocol_verification() -> VerificationResult {
    verify_compliant_operations().await?;
    verify_write_outcomes_are_typed_and_opaque().await?;
    verify_remote_failures_are_typed_and_opaque().await?;
    verify_malformed_responses_are_typed_and_opaque().await?;
    verify_protocol_rejections_are_strict()?;
    Ok(())
}

async fn verify_compliant_operations() -> VerificationResult {
    let fixture = fixture()?;
    let session = FakeSession::start(success_cases(&fixture)).await?;
    let database = IiiMemoryDatabase::new(session.client.clone(), database_target()?);

    let result = async {
        database
            .insert_memory(&fixture.memory)
            .await
            .map_err(|_| VerificationError)?;
        database
            .insert_embedding(&fixture.embedding)
            .await
            .map_err(|_| VerificationError)?;
        let bm25_results = database
            .search_bm25(&fixture.bm25)
            .await
            .map_err(|_| VerificationError)?;
        verify_search_results(&bm25_results, 1.75)?;
        let vector_results = database
            .search_vector(&fixture.vector)
            .await
            .map_err(|_| VerificationError)?;
        verify_search_results(&vector_results, -0.5)?;
        Ok(())
    }
    .await;

    drop(database);
    session.finish().await?;
    result
}

async fn verify_write_outcomes_are_typed_and_opaque() -> VerificationResult {
    let fixture = fixture()?;

    let error = invoke_for_error(
        &fixture,
        AdapterOperation::InsertMemory,
        Reply::Result(memory_conflict_response()),
    )
    .await?;
    require(error == DatabaseError::conflict(DatabaseOperation::InsertMemory))?;
    assert_database_error_is_opaque(&error)?;

    for outcome in ["conflict", "missing"] {
        let error = invoke_for_error(
            &fixture,
            AdapterOperation::InsertEmbedding,
            Reply::Result(embedding_response(&fixture.embedding, outcome)),
        )
        .await?;
        assert_database_error_is_opaque(&error)?;

        if outcome == "conflict" {
            require(error == DatabaseError::conflict(DatabaseOperation::InsertEmbedding))?;
        } else {
            let DatabaseError::MissingMemoryVersion { key } = error else {
                return verification_failure();
            };
            require(key.id().as_str() == MEMORY_ID)?;
            require(key.version().get() == 9)?;
        }
    }

    Ok(())
}

async fn verify_remote_failures_are_typed_and_opaque() -> VerificationResult {
    let fixture = fixture()?;

    for operation in AdapterOperation::ALL {
        let error = invoke_for_error(&fixture, operation, Reply::RemoteFailure).await?;
        require(error == DatabaseError::database_failure(operation.database_operation()))?;
        assert_database_error_is_opaque(&error)?;
    }

    Ok(())
}

async fn verify_malformed_responses_are_typed_and_opaque() -> VerificationResult {
    let fixture = fixture()?;
    let cases = [
        (
            AdapterOperation::InsertMemory,
            malformed_memory_response(&fixture.memory),
            "last_insert_id",
        ),
        (
            AdapterOperation::InsertEmbedding,
            malformed_embedding_response(&fixture.embedding),
            "outcome",
        ),
        (
            AdapterOperation::SearchBm25,
            malformed_bm25_response(),
            "last_insert_id",
        ),
        (
            AdapterOperation::SearchVector,
            malformed_vector_response(),
            "content",
        ),
    ];

    for (operation, response, column) in cases {
        let error = invoke_for_error(&fixture, operation, Reply::Result(response)).await?;
        require(error == DatabaseError::invalid_response(operation.database_operation(), column))?;
        assert_database_error_is_opaque(&error)?;
    }

    Ok(())
}

async fn invoke_for_error(
    fixture: &Fixture,
    operation: AdapterOperation,
    reply: Reply,
) -> VerificationResult<DatabaseError> {
    let session = FakeSession::start(vec![expected_invocation(operation, fixture, reply)]).await?;
    let database = IiiMemoryDatabase::new(session.client.clone(), database_target()?);
    let result = invoke_operation(&database, fixture, operation).await;

    drop(database);
    session.finish().await?;

    match result {
        Ok(()) => verification_failure(),
        Err(error) => Ok(error),
    }
}

async fn invoke_operation(
    database: &IiiMemoryDatabase,
    fixture: &Fixture,
    operation: AdapterOperation,
) -> Result<(), DatabaseError> {
    match operation {
        AdapterOperation::InsertMemory => database.insert_memory(&fixture.memory).await,
        AdapterOperation::InsertEmbedding => database.insert_embedding(&fixture.embedding).await,
        AdapterOperation::SearchBm25 => database.search_bm25(&fixture.bm25).await.map(|_| ()),
        AdapterOperation::SearchVector => database.search_vector(&fixture.vector).await.map(|_| ()),
    }
}

async fn run_fake(listener: TcpListener, expected: Vec<ExpectedInvocation>) -> VerificationResult {
    let (stream, _) = timeout(OPERATION_TIMEOUT, listener.accept())
        .await
        .map_err(|_| VerificationError)?
        .map_err(|_| VerificationError)?;
    let mut socket = timeout(OPERATION_TIMEOUT, accept_async(stream))
        .await
        .map_err(|_| VerificationError)?
        .map_err(|_| VerificationError)?;

    let registration = next_protocol_message(&mut socket).await?;
    validate_worker_registration(&registration)?;
    send_protocol_message(
        &mut socket,
        json!({
            "type": "workerregistered",
            "worker_id": "loopback-protocol-verifier",
        }),
    )
    .await?;

    for expected in expected {
        let message = next_protocol_message(&mut socket).await?;
        let invocation_id = validate_database_invocation(&message, &expected)?;
        send_reply(&mut socket, &invocation_id, expected.reply).await?;
    }

    wait_for_disconnect(&mut socket).await
}

async fn next_protocol_message(
    socket: &mut WebSocketStream<TcpStream>,
) -> VerificationResult<Value> {
    loop {
        let frame = timeout(OPERATION_TIMEOUT, socket.next())
            .await
            .map_err(|_| VerificationError)?
            .ok_or(VerificationError)?
            .map_err(|_| VerificationError)?;
        match frame {
            Message::Ping(payload) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|_| VerificationError)?;
            }
            Message::Text(text) => {
                return serde_json::from_str(text.as_str()).map_err(|_| VerificationError);
            }
            _ => return verification_failure(),
        }
    }
}

async fn wait_for_disconnect(socket: &mut WebSocketStream<TcpStream>) -> VerificationResult {
    loop {
        let next = timeout(OPERATION_TIMEOUT, socket.next())
            .await
            .map_err(|_| VerificationError)?;
        let Some(frame) = next else {
            return Ok(());
        };
        let Ok(frame) = frame else {
            return Ok(());
        };
        match frame {
            Message::Ping(payload) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|_| VerificationError)?;
            }
            Message::Pong(_) => {}
            Message::Close(_) => return Ok(()),
            _ => return verification_failure(),
        }
    }
}

async fn send_reply(
    socket: &mut WebSocketStream<TcpStream>,
    invocation_id: &str,
    reply: Reply,
) -> VerificationResult {
    let response = match reply {
        Reply::Result(result) => json!({
            "type": "invocationresult",
            "invocation_id": invocation_id,
            "function_id": DATABASE_EXECUTE_FUNCTION_ID,
            "result": result,
        }),
        Reply::RemoteFailure => json!({
            "type": "invocationresult",
            "invocation_id": invocation_id,
            "function_id": DATABASE_EXECUTE_FUNCTION_ID,
            "error": {
                "code": REMOTE_CODE,
                "message": REMOTE_MESSAGE,
                "stacktrace": REMOTE_STACKTRACE,
            },
        }),
    };
    send_protocol_message(socket, response).await
}

async fn send_protocol_message(
    socket: &mut WebSocketStream<TcpStream>,
    message: Value,
) -> VerificationResult {
    socket
        .send(Message::Text(message.to_string().into()))
        .await
        .map_err(|_| VerificationError)
}

fn validate_worker_registration(message: &Value) -> VerificationResult {
    let Some(object) = message.as_object() else {
        return verification_failure();
    };
    require(has_exact_keys(
        object,
        ["type", "invocation_id", "function_id", "data", "action"].as_slice(),
    ))?;
    require(message.get("type") == Some(&json!("invokefunction")))?;
    require(message.get("invocation_id") == Some(&Value::Null))?;
    require(message.get("function_id") == Some(&json!(WORKER_REGISTER_FUNCTION_ID)))?;
    require(message.get("data").is_some_and(Value::is_object))?;
    require(message.get("action") == Some(&json!({"type": "void"})))
}

fn validate_database_invocation(
    message: &Value,
    expected: &ExpectedInvocation,
) -> VerificationResult<String> {
    let Some(object) = message.as_object() else {
        return verification_failure();
    };
    require(has_exact_keys(
        object,
        ["type", "invocation_id", "function_id", "data"].as_slice(),
    ))?;
    require(message.get("type") == Some(&json!("invokefunction")))?;
    require(message.get("function_id") == Some(&json!(DATABASE_EXECUTE_FUNCTION_ID)))?;
    require(message.get("data") == Some(&expected.payload))?;
    let Some(invocation_id) = message.get("invocation_id").and_then(Value::as_str) else {
        return verification_failure();
    };
    require(Uuid::parse_str(invocation_id).is_ok())?;
    Ok(invocation_id.to_owned())
}

fn has_exact_keys(object: &Map<String, Value>, expected: &[&str]) -> bool {
    object.len() == expected.len() && expected.iter().all(|key| object.contains_key(*key))
}

fn success_cases(fixture: &Fixture) -> Vec<ExpectedInvocation> {
    vec![
        expected_invocation(
            AdapterOperation::InsertMemory,
            fixture,
            Reply::Result(successful_memory_response(&fixture.memory)),
        ),
        expected_invocation(
            AdapterOperation::InsertEmbedding,
            fixture,
            Reply::Result(embedding_response(&fixture.embedding, "inserted")),
        ),
        expected_invocation(
            AdapterOperation::SearchBm25,
            fixture,
            Reply::Result(search_response(1.75)),
        ),
        expected_invocation(
            AdapterOperation::SearchVector,
            fixture,
            Reply::Result(search_response(-0.5)),
        ),
    ]
}

fn expected_invocation(
    operation: AdapterOperation,
    fixture: &Fixture,
    reply: Reply,
) -> ExpectedInvocation {
    let payload = match operation {
        AdapterOperation::InsertMemory => json!({
            "db": DATABASE_TARGET,
            "sql": INSERT_MEMORY_SQL,
            "params": [
                fixture.memory.id().as_str(),
                fixture.memory.version().get().to_string(),
                fixture.memory.memory_type(),
                fixture.memory.title(),
                fixture.memory.content(),
                fixture.memory.created_at().to_rfc3339(),
                fixture.memory.updated_at().to_rfc3339(),
                fixture.memory.concepts(),
                fixture.memory.files(),
                fixture.memory.session_ids(),
                fixture.memory.source_observation_ids(),
            ],
        }),
        AdapterOperation::InsertEmbedding => json!({
            "db": DATABASE_TARGET,
            "sql": INSERT_EMBEDDING_SQL,
            "params": [MEMORY_ID, "9", "[987654.25,-3.5]"],
        }),
        AdapterOperation::SearchBm25 => json!({
            "db": DATABASE_TARGET,
            "sql": SEARCH_BM25_SQL,
            "params": [BM25_QUERY, "2"],
        }),
        AdapterOperation::SearchVector => json!({
            "db": DATABASE_TARGET,
            "sql": SEARCH_VECTOR_SQL,
            "params": ["[-3.5,987654.25]", "2"],
        }),
    };

    ExpectedInvocation { payload, reply }
}

fn successful_memory_response(memory: &ValidatedMemoryVersion) -> Value {
    json!({
        "affected_rows": 1,
        "last_insert_id": memory.id().as_str(),
        "returned_rows": [{
            "id": memory.id().as_str(),
            "version": memory.version().get().to_string(),
        }],
    })
}

fn memory_conflict_response() -> Value {
    json!({
        "affected_rows": 0,
        "last_insert_id": null,
        "returned_rows": [],
    })
}

fn embedding_response(embedding: &ValidatedEmbedding, outcome: &str) -> Value {
    json!({
        "affected_rows": 1,
        "last_insert_id": null,
        "returned_rows": [{
            "outcome": outcome,
            "id": embedding.id().as_str(),
            "version": embedding.version().get().to_string(),
        }],
    })
}

fn search_response(relevance: f64) -> Value {
    json!({
        "affected_rows": 1,
        "last_insert_id": null,
        "returned_rows": [search_row(relevance)],
    })
}

fn search_row(relevance: f64) -> Value {
    json!({
        "id": SEARCH_ID,
        "version": "12",
        "memory_type": SEARCH_TYPE,
        "title": SEARCH_TITLE,
        "content": SEARCH_CONTENT,
        "created_at": "2026-09-19T12:34:56+00:00",
        "updated_at": "2026-09-19T12:35:56+00:00",
        "concepts": [SEARCH_CONCEPT, "", SEARCH_CONCEPT],
        "files": [SEARCH_FILE, "", SEARCH_FILE],
        "session_ids": [SEARCH_SESSION, "", SEARCH_SESSION],
        "source_observation_ids": [SEARCH_SOURCE, "", SEARCH_SOURCE],
        "relevance": relevance,
    })
}

fn malformed_memory_response(memory: &ValidatedMemoryVersion) -> Value {
    json!({
        "affected_rows": 1,
        "returned_rows": [{
            "id": memory.id().as_str(),
            "version": memory.version().get().to_string(),
        }],
    })
}

fn malformed_embedding_response(embedding: &ValidatedEmbedding) -> Value {
    json!({
        "affected_rows": 1,
        "last_insert_id": null,
        "returned_rows": [{
            "id": embedding.id().as_str(),
            "version": embedding.version().get().to_string(),
        }],
    })
}

fn malformed_bm25_response() -> Value {
    json!({
        "affected_rows": 1,
        "returned_rows": [search_row(1.75)],
    })
}

fn malformed_vector_response() -> Value {
    let mut row = search_row(-0.5);
    if let Some(object) = row.as_object_mut() {
        object.remove("content");
    }
    json!({
        "affected_rows": 1,
        "last_insert_id": null,
        "returned_rows": [row],
    })
}

fn fixture() -> VerificationResult<Fixture> {
    let timestamp = Utc
        .with_ymd_and_hms(2026, 9, 19, 12, 34, 56)
        .single()
        .ok_or(VerificationError)?;
    let memory = ValidatedMemoryVersion::try_from(MemoryVersionInput {
        id: MEMORY_ID.to_owned(),
        version: 9,
        memory_type: MEMORY_TYPE.to_owned(),
        title: MEMORY_TITLE.to_owned(),
        content: MEMORY_CONTENT.to_owned(),
        created_at: timestamp,
        updated_at: timestamp,
        concepts: vec![CONCEPT.to_owned(), String::new(), CONCEPT.to_owned()],
        files: vec![FILE.to_owned(), String::new(), FILE.to_owned()],
        session_ids: vec![SESSION.to_owned(), String::new(), SESSION.to_owned()],
        source_observation_ids: vec![SOURCE.to_owned(), String::new(), SOURCE.to_owned()],
    })
    .map_err(|_| VerificationError)?;
    let embedding = ValidatedEmbedding::try_from(EmbeddingInput {
        id: MEMORY_ID.to_owned(),
        version: 9,
        embedding: vec![987_654.25, -3.5],
    })
    .map_err(|_| VerificationError)?;
    let bm25 = ValidatedBm25Search::try_from(Bm25Search {
        query: BM25_QUERY.to_owned(),
        limit: 2,
    })
    .map_err(|_| VerificationError)?;
    let vector = ValidatedVectorSearch::try_from(VectorSearch {
        vector: vec![-3.5, 987_654.25],
        limit: 2,
    })
    .map_err(|_| VerificationError)?;

    Ok(Fixture {
        memory,
        embedding,
        bm25,
        vector,
    })
}

fn database_target() -> VerificationResult<DatabaseTarget> {
    DatabaseTarget::try_from(DATABASE_TARGET.to_owned()).map_err(|_| VerificationError)
}

fn verify_search_results(
    results: &[MemorySearchResult],
    expected_relevance: f64,
) -> VerificationResult {
    let [result] = results else {
        return verification_failure();
    };
    require(result.id().as_str() == SEARCH_ID)?;
    require(result.version().get() == 12)?;
    require(result.memory_type() == SEARCH_TYPE)?;
    require(result.title() == SEARCH_TITLE)?;
    require(result.content() == SEARCH_CONTENT)?;
    require(result.created_at().to_rfc3339() == "2026-09-19T12:34:56+00:00")?;
    require(result.updated_at().to_rfc3339() == "2026-09-19T12:35:56+00:00")?;
    require(result.concepts() == [SEARCH_CONCEPT, "", SEARCH_CONCEPT])?;
    require(result.files() == [SEARCH_FILE, "", SEARCH_FILE])?;
    require(result.session_ids() == [SEARCH_SESSION, "", SEARCH_SESSION])?;
    require(result.source_observation_ids() == [SEARCH_SOURCE, "", SEARCH_SOURCE])?;
    require(result.relevance() == expected_relevance)?;

    let serialized = serde_json::to_value(results).map_err(|_| VerificationError)?;
    let Some(rows) = serialized.as_array() else {
        return verification_failure();
    };
    require(rows.len() == 1)?;
    require(rows[0].get("embedding").is_none())
}

fn assert_database_error_is_opaque(error: &DatabaseError) -> VerificationResult {
    let display = error.to_string();
    let debug = format!("{error:?}");

    for sentinel in protected_sentinels() {
        require(!display.contains(sentinel))?;
        require(!debug.contains(sentinel))?;
    }

    Ok(())
}

fn verify_protocol_rejections_are_strict() -> VerificationResult {
    let fixture = fixture()?;
    let expected_memory = expected_invocation(
        AdapterOperation::InsertMemory,
        &fixture,
        Reply::Result(Value::Null),
    );
    let expected_vector = expected_invocation(
        AdapterOperation::SearchVector,
        &fixture,
        Reply::Result(Value::Null),
    );
    let frame = database_frame(&expected_memory);
    require(validate_database_invocation(&frame, &expected_memory).is_ok())?;

    let mut cases = Vec::new();

    let mut wrong_type = frame.clone();
    wrong_type["type"] = json!("unexpected");
    cases.push((wrong_type, &expected_memory));

    let mut missing_invocation_id = frame.clone();
    missing_invocation_id["invocation_id"] = Value::Null;
    cases.push((missing_invocation_id, &expected_memory));

    let mut wrong_function = frame.clone();
    wrong_function["function_id"] = json!("unexpected::function");
    cases.push((wrong_function, &expected_memory));

    let mut wrong_database = frame.clone();
    wrong_database["data"]["db"] = json!("unexpected-database");
    cases.push((wrong_database, &expected_memory));

    let mut wrong_sql = frame.clone();
    wrong_sql["data"]["sql"] = json!(SQL_SENTINEL);
    cases.push((wrong_sql, &expected_memory));

    let mut wrong_bigint = frame.clone();
    wrong_bigint["data"]["params"][1] = json!(9);
    cases.push((wrong_bigint, &expected_memory));

    let mut reordered_collection = frame.clone();
    reordered_collection["data"]["params"][7] = json!(["", CONCEPT, CONCEPT]);
    cases.push((reordered_collection, &expected_memory));

    let mut metadata = frame.clone();
    metadata["metadata"] = json!({"unexpected": true});
    cases.push((metadata, &expected_memory));

    let mut action = frame.clone();
    action["action"] = Value::Null;
    cases.push((action, &expected_memory));

    let mut namespace = frame.clone();
    namespace["namespace"] = json!("unexpected");
    cases.push((namespace, &expected_memory));

    let mut extra_data = frame.clone();
    extra_data["data"]["unexpected"] = json!(true);
    cases.push((extra_data, &expected_memory));

    let vector_frame = database_frame(&expected_vector);
    let mut missing_vector_cast = vector_frame.clone();
    let Some(sql) = missing_vector_cast["data"]["sql"].as_str() else {
        return verification_failure();
    };
    missing_vector_cast["data"]["sql"] = json!(sql.replacen("$1::text::vector", "$1::vector", 1));
    cases.push((missing_vector_cast, &expected_vector));

    let mut non_text_vector = vector_frame;
    non_text_vector["data"]["params"][0] = json!([-3.5, 987_654.25]);
    cases.push((non_text_vector, &expected_vector));

    for (candidate, expected) in cases {
        assert_protocol_rejected(&candidate, expected)?;
    }

    Ok(())
}

fn database_frame(expected: &ExpectedInvocation) -> Value {
    json!({
        "type": "invokefunction",
        "invocation_id": "00000000-0000-4000-8000-000000000001",
        "function_id": DATABASE_EXECUTE_FUNCTION_ID,
        "data": expected.payload,
    })
}

fn assert_protocol_rejected(message: &Value, expected: &ExpectedInvocation) -> VerificationResult {
    let Some(error) = validate_database_invocation(message, expected).err() else {
        return verification_failure();
    };
    let display = error.to_string();
    let debug = format!("{error:?}");
    for sentinel in protected_sentinels() {
        require(!display.contains(sentinel))?;
        require(!debug.contains(sentinel))?;
    }
    Ok(())
}

fn protected_sentinels() -> [&'static str; 23] {
    [
        DATABASE_TARGET,
        MEMORY_ID,
        MEMORY_TYPE,
        MEMORY_TITLE,
        MEMORY_CONTENT,
        CONCEPT,
        FILE,
        SESSION,
        SOURCE,
        BM25_QUERY,
        VECTOR_COMPONENT,
        SEARCH_ID,
        SEARCH_TYPE,
        SEARCH_TITLE,
        SEARCH_CONTENT,
        SEARCH_CONCEPT,
        SEARCH_FILE,
        SEARCH_SESSION,
        SEARCH_SOURCE,
        REMOTE_CODE,
        REMOTE_MESSAGE,
        REMOTE_STACKTRACE,
        SQL_SENTINEL,
    ]
}

fn require(condition: bool) -> VerificationResult {
    if condition {
        Ok(())
    } else {
        verification_failure()
    }
}

fn verification_failure<T>() -> VerificationResult<T> {
    Err(VerificationError)
}

#[cfg(test)]
mod tests {
    use super::{
        verify_compliant_operations, verify_malformed_responses_are_typed_and_opaque,
        verify_protocol_rejections_are_strict, verify_remote_failures_are_typed_and_opaque,
        verify_write_outcomes_are_typed_and_opaque,
    };

    #[tokio::test]
    async fn loopback_protocol_verifies_all_four_adapter_operations() {
        assert!(verify_compliant_operations().await.is_ok());
    }

    #[tokio::test]
    async fn loopback_protocol_maps_write_outcomes_without_leaks() {
        assert!(verify_write_outcomes_are_typed_and_opaque().await.is_ok());
    }

    #[tokio::test]
    async fn loopback_protocol_maps_remote_failures_without_leaks() {
        assert!(verify_remote_failures_are_typed_and_opaque().await.is_ok());
    }

    #[tokio::test]
    async fn loopback_protocol_rejects_malformed_responses_without_leaks() {
        assert!(
            verify_malformed_responses_are_typed_and_opaque()
                .await
                .is_ok()
        );
    }

    #[test]
    fn fake_rejects_protocol_and_privacy_mismatches() {
        assert!(verify_protocol_rejections_are_strict().is_ok());
    }
}
