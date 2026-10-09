use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use iii_sdk::{
    IIIClient, InitOptions, WorkerIdentityMode, register_worker, runtime::WorkerMetadata,
};
use mcp_worker::{
    backend::{CanonicalStoreDelegate, ProductionMemoryOperations, ReadOnlyRetrievalAdapter},
    operations::{
        BackendResponseField, BackendSource, MemoryOperations, Operation, OperationError,
        OperationInputField, VersionPageQuery,
    },
};
use memory_store::{
    MemoryStore,
    contracts::{
        Bm25Search, DatabaseError, DatabaseOperation, DatabaseTarget, MemoryId, MemorySearchResult,
        MemoryVersion, MemoryVersionInput, MemoryVersionKey, ValidatedBm25Search,
        ValidatedEmbedding, ValidatedMemoryVersion, ValidatedVectorSearch, VectorSearch,
    },
    database::MemoryDatabase,
};
use serde_json::{Value, json};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_tungstenite::{
    WebSocketStream, accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{Callback, ErrorResponse, Request, Response},
    },
};

const TITLE_SENTINEL: &str = "title-secret-sentinel";
const CONTENT_SENTINEL: &str = "content-secret-sentinel";
const QUERY_SENTINEL: &str = "query-secret-sentinel";
const VECTOR_SENTINEL: f64 = 42.5;
const BACKEND_PAYLOAD_SENTINEL: &str = "backend-payload-secret-sentinel";

#[tokio::test]
async fn construction_does_not_invoke_the_database() {
    let database = TestDatabase::new(Ok(()), Ok(Vec::new()), Ok(Vec::new()));
    let store = MemoryStore::new(database.clone());
    let _delegate = CanonicalStoreDelegate::new(store);

    assert!(database.calls().is_empty());
}

#[test]
fn production_port_construction_is_inert_and_substitutes_for_all_six_operations() {
    let database = TestDatabase::new(Ok(()), Ok(Vec::new()), Ok(Vec::new()));
    let delegate = CanonicalStoreDelegate::new(MemoryStore::new(database.clone()));
    let retrieval = ReadOnlyRetrievalAdapter::new(
        IIIClient::new("ws://127.0.0.1:0"),
        DatabaseTarget::try_from("database-target-sentinel".to_owned())
            .expect("test database target should be valid"),
    );
    let operations = ProductionMemoryOperations::new(delegate, retrieval);

    assert_memory_operations(&operations);
    assert!(database.calls().is_empty());
}

#[tokio::test]
async fn delegates_each_operation_once_with_exact_store_arguments_and_unmodified_results() {
    let memory = memory("memory-id", "memory title", "memory content", 3);
    let lexical_query = Bm25Search {
        query: "lexical query".to_owned(),
        limit: 2,
    };
    let vector_query = VectorSearch {
        vector: vec![1.0, -2.0],
        limit: 2,
    };
    let lexical_results = vec![
        search_result("lexical-first", 2, 0.9),
        search_result("lexical-second", 1, 0.4),
    ];
    let vector_results = vec![
        search_result("vector-first", 4, 0.8),
        search_result("vector-second", 3, 0.1),
    ];
    let database = TestDatabase::new(
        Ok(()),
        Ok(lexical_results.clone()),
        Ok(vector_results.clone()),
    );
    let delegate = CanonicalStoreDelegate::new(MemoryStore::new(database.clone()));

    delegate
        .insert_memory(memory.clone())
        .await
        .expect("insert should delegate to the store");
    let actual_lexical = delegate
        .search_lexical(lexical_query.clone())
        .await
        .expect("lexical search should delegate to the store");
    let actual_vector = delegate
        .search_vector(vector_query.clone())
        .await
        .expect("vector search should delegate to the store");

    assert_eq!(actual_lexical, lexical_results);
    assert_eq!(actual_vector, vector_results);
    assert_eq!(actual_lexical[0].relevance(), 0.9);
    assert_eq!(actual_lexical[1].relevance(), 0.4);
    assert_eq!(actual_vector[0].relevance(), 0.8);
    assert_eq!(actual_vector[1].relevance(), 0.1);
    assert_eq!(
        database.calls(),
        vec![
            DatabaseCall::InsertMemory(memory.try_into().expect("test memory should be valid"),),
            DatabaseCall::SearchBm25(
                lexical_query
                    .try_into()
                    .expect("test lexical query should be valid"),
            ),
            DatabaseCall::SearchVector(
                vector_query
                    .try_into()
                    .expect("test vector query should be valid"),
            ),
        ]
    );
}

#[tokio::test]
async fn store_validation_rejects_invalid_input_before_the_database_is_called() {
    let database = TestDatabase::new(Ok(()), Ok(Vec::new()), Ok(Vec::new()));
    let delegate = CanonicalStoreDelegate::new(MemoryStore::new(database.clone()));

    let invalid_memory_id = delegate
        .insert_memory(MemoryVersionInput {
            id: String::new(),
            ..memory("memory-id", "memory title", "memory content", 1)
        })
        .await
        .expect_err("an empty memory ID should be rejected by the store");
    let invalid_memory_version = delegate
        .insert_memory(MemoryVersionInput {
            version: 0,
            ..memory("memory-id", "memory title", "memory content", 1)
        })
        .await
        .expect_err("a non-positive memory version should be rejected by the store");
    let invalid_memory = delegate
        .insert_memory(MemoryVersionInput {
            title: String::new(),
            ..memory("memory-id", "memory title", "memory content", 1)
        })
        .await
        .expect_err("other canonical memory validation should remain closed");
    let invalid_query = delegate
        .search_lexical(Bm25Search {
            query: String::new(),
            limit: 1,
        })
        .await
        .expect_err("an empty lexical query should be rejected by the store");
    let invalid_limit = delegate
        .search_lexical(Bm25Search {
            query: "query".to_owned(),
            limit: 0,
        })
        .await
        .expect_err("a zero lexical limit should be rejected by the store");
    let invalid_vector = delegate
        .search_vector(VectorSearch {
            vector: Vec::new(),
            limit: 1,
        })
        .await
        .expect_err("an empty vector should be rejected by the store");

    assert_eq!(
        invalid_memory_id,
        OperationError::invalid_input(Operation::InsertMemory, OperationInputField::Id)
    );
    assert_eq!(
        invalid_memory_version,
        OperationError::invalid_input(Operation::InsertMemory, OperationInputField::Version)
    );
    assert_eq!(
        invalid_memory,
        OperationError::invalid_input(Operation::InsertMemory, OperationInputField::Memory)
    );
    assert_eq!(
        invalid_query,
        OperationError::invalid_input(Operation::SearchLexical, OperationInputField::Query)
    );
    assert_eq!(
        invalid_limit,
        OperationError::invalid_input(Operation::SearchLexical, OperationInputField::Limit)
    );
    assert_eq!(
        invalid_vector,
        OperationError::invalid_input(Operation::SearchVector, OperationInputField::Vector)
    );
    assert!(database.calls().is_empty());
}

#[tokio::test]
async fn maps_store_outcomes_to_closed_operation_errors() {
    let conflict_database = TestDatabase::new(
        Err(DatabaseError::conflict(DatabaseOperation::InsertMemory)),
        Ok(Vec::new()),
        Ok(Vec::new()),
    );
    let conflict_delegate =
        CanonicalStoreDelegate::new(MemoryStore::new(conflict_database.clone()));
    let conflict = conflict_delegate
        .insert_memory(memory("memory-id", TITLE_SENTINEL, CONTENT_SENTINEL, 1))
        .await
        .expect_err("a store conflict should remain a conflict");

    let backend_database = TestDatabase::new(
        Ok(()),
        Err(DatabaseError::database_failure(
            DatabaseOperation::SearchBm25,
        )),
        Ok(Vec::new()),
    );
    let backend_delegate = CanonicalStoreDelegate::new(MemoryStore::new(backend_database.clone()));
    let backend = backend_delegate
        .search_lexical(Bm25Search {
            query: QUERY_SENTINEL.to_owned(),
            limit: 1,
        })
        .await
        .expect_err("a database failure should become a memory-store backend failure");

    let malformed_database = TestDatabase::new(
        Ok(()),
        Ok(Vec::new()),
        Err(DatabaseError::invalid_response(
            DatabaseOperation::SearchVector,
            BACKEND_PAYLOAD_SENTINEL,
        )),
    );
    let malformed_delegate =
        CanonicalStoreDelegate::new(MemoryStore::new(malformed_database.clone()));
    let malformed = malformed_delegate
        .search_vector(VectorSearch {
            vector: vec![VECTOR_SENTINEL],
            limit: 1,
        })
        .await
        .expect_err("a malformed store response should remain closed");

    let missing_database = TestDatabase::new(
        Ok(()),
        Err(DatabaseError::missing_memory_version(
            MemoryVersionKey::new(
                MemoryId::try_from("missing-id".to_owned()).expect("test ID should be valid"),
                MemoryVersion::try_from(1).expect("test version should be valid"),
            ),
        )),
        Ok(Vec::new()),
    );
    let missing_delegate = CanonicalStoreDelegate::new(MemoryStore::new(missing_database.clone()));
    let missing = missing_delegate
        .search_lexical(Bm25Search {
            query: "missing query".to_owned(),
            limit: 1,
        })
        .await
        .expect_err("a missing store record should remain closed");

    assert_eq!(conflict, OperationError::conflict(Operation::InsertMemory));
    assert_eq!(
        backend,
        OperationError::backend_failure(Operation::SearchLexical, BackendSource::MemoryStore)
    );
    assert_eq!(
        malformed,
        OperationError::invalid_backend_response(
            Operation::SearchVector,
            BackendResponseField::SearchResult,
        )
    );
    assert_eq!(missing, OperationError::not_found(Operation::SearchLexical));
    assert_eq!(conflict_database.calls().len(), 1);
    assert_eq!(backend_database.calls().len(), 1);
    assert_eq!(malformed_database.calls().len(), 1);
    assert_eq!(missing_database.calls().len(), 1);
    for error in [&conflict, &backend, &malformed, &missing] {
        assert_error_is_opaque(error);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_only_version_list_adapter_preserves_fractional_times_and_page_contract() {
    const ID: &str = "fractional-memory";
    const DATABASE: &str = "version-list-database";
    const TIMEOUT: Duration = Duration::from_secs(5);
    const SQL: &str = "SELECT\n    memory.version::text AS version,\n    to_char(memory.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US+00:00') AS updated_at\nFROM public.memories AS memory\nWHERE memory.id = $1::text\nORDER BY memory.version DESC\nOFFSET $2::text::bigint\nLIMIT $3::text::bigint";

    let pages = vec![
        (
            17,
            2,
            json!({
                "affected_rows": 2,
                "last_insert_id": null,
                "returned_rows": [
                    { "version": "8", "updated_at": "2026-09-20T12:35:56.123+00:00" },
                    { "version": "7", "updated_at": "2026-09-20T12:34:56.123456+00:00" },
                ],
            }),
        ),
        (
            19,
            1,
            json!({
                "affected_rows": 1,
                "last_insert_id": null,
                "returned_rows": [
                    { "version": "6", "updated_at": "2026-09-20T12:33:56.123456001+00:00" },
                ],
            }),
        ),
        (
            20,
            1,
            json!({
                "affected_rows": 1,
                "last_insert_id": null,
                "returned_rows": [
                    { "version": "5", "updated_at": "0000-09-20T12:32:56.123456+00:00" },
                ],
            }),
        ),
    ];
    let mut engine = FakeVersionListEngine::start(ID, DATABASE, SQL, pages).await;
    let client = register_worker(
        &engine.url,
        InitOptions {
            metadata: Some(WorkerMetadata::default()),
            identity: WorkerIdentityMode::Explicit,
            ..InitOptions::default()
        },
    );
    let adapter = ReadOnlyRetrievalAdapter::new(
        client.clone(),
        DatabaseTarget::try_from(DATABASE.to_owned()).expect("database target should be valid"),
    );

    let registered = timeout(TIMEOUT, client.wait_until_registered(TIMEOUT)).await;
    let results = if matches!(registered, Ok(Ok(()))) {
        let valid_page = adapter
            .list_versions(VersionPageQuery {
                id: MemoryId::try_from(ID.to_owned()).expect("memory ID should be valid"),
                offset: 17,
                limit: 2,
            })
            .await;
        let submicrosecond_page = adapter
            .list_versions(VersionPageQuery {
                id: MemoryId::try_from(ID.to_owned()).expect("memory ID should be valid"),
                offset: 19,
                limit: 1,
            })
            .await;
        let unsupported_year_page = adapter
            .list_versions(VersionPageQuery {
                id: MemoryId::try_from(ID.to_owned()).expect("memory ID should be valid"),
                offset: 20,
                limit: 1,
            })
            .await;
        Some((valid_page, submicrosecond_page, unsupported_year_page))
    } else {
        None
    };

    client.shutdown();
    timeout(TIMEOUT, &mut engine.task)
        .await
        .expect("fake engine should finish after client shutdown")
        .expect("fake engine should not panic");

    registered
        .expect("worker registration should complete within the test timeout")
        .expect("fake engine should accept the client");
    let (valid_page, submicrosecond_page, unsupported_year_page) =
        results.expect("version pages should run after worker registration");
    let valid_page = valid_page.expect("microsecond version timestamps should decode");
    assert_eq!(
        valid_page,
        vec![
            mcp_worker::operations::MemoryVersionSummary {
                version: MemoryVersion::try_from(8).expect("version should be positive"),
                updated_at: DateTime::parse_from_rfc3339("2026-09-20T12:35:56.123+00:00")
                    .expect("millisecond timestamp should parse")
                    .with_timezone(&Utc),
            },
            mcp_worker::operations::MemoryVersionSummary {
                version: MemoryVersion::try_from(7).expect("version should be positive"),
                updated_at: DateTime::parse_from_rfc3339("2026-09-20T12:34:56.123456+00:00")
                    .expect("microsecond timestamp should parse")
                    .with_timezone(&Utc),
            },
        ]
    );

    for (error, protected_timestamp) in [
        (submicrosecond_page, "2026-09-20T12:33:56.123456001+00:00"),
        (unsupported_year_page, "0000-09-20T12:32:56.123456+00:00"),
    ] {
        let error = error.expect_err("invalid version timestamps should fail closed");
        assert_eq!(
            error,
            OperationError::invalid_backend_response(
                Operation::ListVersions,
                BackendResponseField::VersionSummary,
            )
        );
        assert_error_is_opaque(&error);
        assert!(!error.to_string().contains(protected_timestamp));
        assert!(!format!("{error:?}").contains(protected_timestamp));
    }
}

struct FakeVersionListEngine {
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl FakeVersionListEngine {
    async fn start(
        id: &'static str,
        database: &'static str,
        sql: &'static str,
        pages: Vec<(u64, u32, Value)>,
    ) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("fake engine should bind a loopback port");
        let url = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("fake engine should expose a loopback address")
        );
        let task = tokio::spawn(serve_version_list_pages(listener, id, database, sql, pages));

        Self { url, task }
    }
}

async fn serve_version_list_pages(
    listener: TcpListener,
    id: &str,
    database: &str,
    sql: &str,
    pages: Vec<(u64, u32, Value)>,
) {
    let mut socket = accept_engine_connection(&listener).await;
    let registration = next_engine_message(&mut socket).await;
    assert_eq!(registration["type"], "invokefunction");
    assert_eq!(registration["function_id"], "engine::workers::register");
    assert_eq!(registration["invocation_id"], Value::Null);
    send_engine_message(
        &mut socket,
        json!({
            "type": "workerregistered",
            "worker_id": "mcp-worker-version-list-test",
        }),
    )
    .await;

    for (offset, limit, result) in pages {
        let invocation = next_engine_message(&mut socket).await;
        assert_eq!(invocation["type"], "invokefunction");
        assert_eq!(invocation["function_id"], "database::execute");
        assert_eq!(invocation["data"]["db"], database);
        assert_eq!(invocation["data"]["sql"], sql);
        assert_eq!(
            invocation["data"]["params"],
            json!([id, offset.to_string(), limit.to_string()])
        );
        send_engine_message(
            &mut socket,
            json!({
                "type": "invocationresult",
                "invocation_id": invocation["invocation_id"],
                "function_id": "database::execute",
                "result": result,
            }),
        )
        .await;
    }

    wait_for_engine_disconnect(&mut socket).await;
}

async fn accept_engine_connection(listener: &TcpListener) -> WebSocketStream<TcpStream> {
    loop {
        let (stream, _) = timeout(Duration::from_secs(5), listener.accept())
            .await
            .expect("client should connect to fake engine")
            .expect("fake engine should accept a connection");
        let mut path = String::new();
        let Ok(socket) = accept_hdr_async(stream, EngineRequestPath(&mut path)).await else {
            continue;
        };

        if path == "/otel" {
            tokio::spawn(discard_engine_frames(socket));
        } else {
            return socket;
        }
    }
}

struct EngineRequestPath<'path>(&'path mut String);

impl Callback for EngineRequestPath<'_> {
    fn on_request(self, request: &Request, response: Response) -> Result<Response, ErrorResponse> {
        request.uri().path().clone_into(self.0);
        Ok(response)
    }
}

async fn next_engine_message(socket: &mut WebSocketStream<TcpStream>) -> Value {
    loop {
        let frame = timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("fake engine should receive a client frame")
            .expect("client should keep the connection open")
            .expect("client WebSocket frame should be valid");
        match frame {
            Message::Text(text) => {
                return serde_json::from_str(text.as_str())
                    .expect("client WebSocket message should be JSON");
            }
            Message::Ping(payload) => socket
                .send(Message::Pong(payload))
                .await
                .expect("fake engine should answer WebSocket pings"),
            Message::Pong(_) => {}
            Message::Close(_) => panic!("client closed before all version pages were read"),
            Message::Binary(_) | Message::Frame(_) => {
                panic!("client should send JSON text frames")
            }
        }
    }
}

async fn send_engine_message(socket: &mut WebSocketStream<TcpStream>, message: Value) {
    socket
        .send(Message::Text(message.to_string().into()))
        .await
        .expect("fake engine should send a response");
}

async fn wait_for_engine_disconnect(socket: &mut WebSocketStream<TcpStream>) {
    loop {
        let frame = timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("client should disconnect after the test");
        match frame {
            None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return,
            Some(Ok(Message::Ping(payload))) => socket
                .send(Message::Pong(payload))
                .await
                .expect("fake engine should answer WebSocket pings"),
            Some(Ok(Message::Pong(_))) => {}
            Some(Ok(Message::Text(_) | Message::Binary(_) | Message::Frame(_))) => {
                panic!("client sent an unexpected frame after all responses")
            }
        }
    }
}

async fn discard_engine_frames(mut socket: WebSocketStream<TcpStream>) {
    while let Some(Ok(_)) = socket.next().await {}
}

#[derive(Clone, Debug, PartialEq)]
enum DatabaseCall {
    InsertMemory(ValidatedMemoryVersion),
    SearchBm25(ValidatedBm25Search),
    SearchVector(ValidatedVectorSearch),
}

struct TestDatabaseState {
    calls: Vec<DatabaseCall>,
    insert_memory: Result<(), DatabaseError>,
    search_bm25: Result<Vec<MemorySearchResult>, DatabaseError>,
    search_vector: Result<Vec<MemorySearchResult>, DatabaseError>,
}

#[derive(Clone)]
struct TestDatabase {
    state: Arc<Mutex<TestDatabaseState>>,
}

impl TestDatabase {
    fn new(
        insert_memory: Result<(), DatabaseError>,
        search_bm25: Result<Vec<MemorySearchResult>, DatabaseError>,
        search_vector: Result<Vec<MemorySearchResult>, DatabaseError>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(TestDatabaseState {
                calls: Vec::new(),
                insert_memory,
                search_bm25,
                search_vector,
            })),
        }
    }

    fn calls(&self) -> Vec<DatabaseCall> {
        self.state
            .lock()
            .expect("test database lock should not be poisoned")
            .calls
            .clone()
    }
}

#[async_trait]
impl MemoryDatabase for TestDatabase {
    async fn insert_memory(&self, memory: &ValidatedMemoryVersion) -> Result<(), DatabaseError> {
        let mut state = self
            .state
            .lock()
            .expect("test database lock should not be poisoned");
        state.calls.push(DatabaseCall::InsertMemory(memory.clone()));
        state.insert_memory.clone()
    }

    async fn insert_embedding(&self, _: &ValidatedEmbedding) -> Result<(), DatabaseError> {
        panic!("the canonical store delegate must not insert embeddings")
    }

    async fn search_bm25(
        &self,
        query: &ValidatedBm25Search,
    ) -> Result<Vec<MemorySearchResult>, DatabaseError> {
        let mut state = self
            .state
            .lock()
            .expect("test database lock should not be poisoned");
        state.calls.push(DatabaseCall::SearchBm25(query.clone()));
        state.search_bm25.clone()
    }

    async fn search_vector(
        &self,
        query: &ValidatedVectorSearch,
    ) -> Result<Vec<MemorySearchResult>, DatabaseError> {
        let mut state = self
            .state
            .lock()
            .expect("test database lock should not be poisoned");
        state.calls.push(DatabaseCall::SearchVector(query.clone()));
        state.search_vector.clone()
    }
}

fn memory(id: &str, title: &str, content: &str, version: i64) -> MemoryVersionInput {
    MemoryVersionInput {
        id: id.to_owned(),
        version,
        memory_type: "unclassified".to_owned(),
        title: title.to_owned(),
        content: content.to_owned(),
        created_at: timestamp(),
        updated_at: timestamp(),
        concepts: vec!["concept".to_owned()],
        files: vec!["file".to_owned()],
        session_ids: vec!["session".to_owned()],
        source_observation_ids: vec!["source".to_owned()],
    }
}

fn search_result(id: &str, version: i64, relevance: f64) -> MemorySearchResult {
    MemorySearchResult::try_new(
        memory(id, "result title", "result content", version)
            .try_into()
            .expect("test result memory should be valid"),
        relevance,
    )
    .expect("test relevance should be valid")
}

fn timestamp() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 20, 12, 34, 56)
        .single()
        .expect("test timestamp should be valid")
}

fn assert_error_is_opaque(error: &OperationError) {
    let display = error.to_string();
    let debug = format!("{error:?}");

    for protected_value in [
        TITLE_SENTINEL,
        CONTENT_SENTINEL,
        QUERY_SENTINEL,
        "42.5",
        BACKEND_PAYLOAD_SENTINEL,
    ] {
        assert!(
            !display.contains(protected_value),
            "Display error leaked protected value: {display}"
        );
        assert!(
            !debug.contains(protected_value),
            "Debug error leaked protected value: {debug}"
        );
    }
}

fn assert_memory_operations<T: MemoryOperations>(_: &T) {}
