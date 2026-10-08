use std::future::Future;

use chrono::{DateTime, Datelike, Utc};
use iii_sdk::{IIIClient, protocol::TriggerRequest};
use memory_store::{
    MemoryStore,
    contracts::{
        Bm25Search, DatabaseError, DatabaseTarget, MemoryId, MemorySearchResult, MemoryStoreError,
        MemoryVersion, MemoryVersionInput, ValidatedMemoryVersion, VectorSearch,
    },
    database::MemoryDatabase,
};
use serde_json::{Map, Value, json};

use crate::operations::{
    BackendResponseField, BackendSource, MemoryOperations, MemoryVersionSummary, Operation,
    OperationError, OperationInputField, VersionPageQuery,
};

const DATABASE_EXECUTE_FUNCTION_ID: &str = "database::execute";
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
const VERSION_LIST_SQL: &str = r#"SELECT
    memory.version::text AS version,
    memory.updated_at
FROM public.memories AS memory
WHERE memory.id = $1::text
ORDER BY memory.version DESC
OFFSET $2::text::bigint
LIMIT $3::text::bigint"#;

pub struct ReadOnlyRetrievalAdapter {
    client: IIIClient,
    database: DatabaseTarget,
}

impl ReadOnlyRetrievalAdapter {
    pub fn new(client: IIIClient, database: DatabaseTarget) -> Self {
        Self { client, database }
    }

    pub async fn get_latest(&self, id: MemoryId) -> Result<MemoryVersionInput, OperationError> {
        get_latest_with(&self.database, id, |request| self.client.trigger(request)).await
    }

    pub async fn get_exact(
        &self,
        id: MemoryId,
        version: MemoryVersion,
    ) -> Result<MemoryVersionInput, OperationError> {
        get_exact_with(&self.database, id, version, |request| {
            self.client.trigger(request)
        })
        .await
    }

    pub async fn list_versions(
        &self,
        query: VersionPageQuery,
    ) -> Result<Vec<MemoryVersionSummary>, OperationError> {
        list_versions_with(&self.database, query, |request| {
            self.client.trigger(request)
        })
        .await
    }
}

async fn get_latest_with<F, Fut>(
    database: &DatabaseTarget,
    id: MemoryId,
    invoke: F,
) -> Result<MemoryVersionInput, OperationError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    retrieve_once(
        Operation::GetLatest,
        build_latest_memory_request(database, &id),
        invoke,
    )
    .await
}

async fn get_exact_with<F, Fut>(
    database: &DatabaseTarget,
    id: MemoryId,
    version: MemoryVersion,
    invoke: F,
) -> Result<MemoryVersionInput, OperationError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    retrieve_once(
        Operation::GetExact,
        build_exact_memory_request(database, &id, version),
        invoke,
    )
    .await
}

async fn list_versions_with<F, Fut>(
    database: &DatabaseTarget,
    query: VersionPageQuery,
    invoke: F,
) -> Result<Vec<MemoryVersionSummary>, OperationError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    let response = invoke(build_version_list_request(database, &query))
        .await
        .map_err(|_| {
            OperationError::backend_failure(Operation::ListVersions, BackendSource::Database)
        })?;

    decode_version_list_response(query.limit, response)
}

fn build_latest_memory_request(database: &DatabaseTarget, id: &MemoryId) -> TriggerRequest {
    build_execute_request(database, LATEST_MEMORY_SQL, json!([id.as_str()]))
}

fn build_exact_memory_request(
    database: &DatabaseTarget,
    id: &MemoryId,
    version: MemoryVersion,
) -> TriggerRequest {
    build_execute_request(
        database,
        EXACT_MEMORY_SQL,
        json!([id.as_str(), version.get().to_string()]),
    )
}

fn build_version_list_request(
    database: &DatabaseTarget,
    query: &VersionPageQuery,
) -> TriggerRequest {
    build_execute_request(
        database,
        VERSION_LIST_SQL,
        json!([
            query.id.as_str(),
            query.offset.to_string(),
            query.limit.to_string(),
        ]),
    )
}

fn build_execute_request(
    database: &DatabaseTarget,
    sql: &'static str,
    params: Value,
) -> TriggerRequest {
    TriggerRequest {
        function_id: DATABASE_EXECUTE_FUNCTION_ID.to_owned(),
        payload: json!({
            "db": database.as_str(),
            "sql": sql,
            "params": params,
        }),
        action: None,
        timeout_ms: None,
    }
}

async fn retrieve_once<F, Fut>(
    operation: Operation,
    request: TriggerRequest,
    invoke: F,
) -> Result<MemoryVersionInput, OperationError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    let response = invoke(request)
        .await
        .map_err(|_| OperationError::backend_failure(operation, BackendSource::Database))?;

    decode_retrieval_response(operation, response)
}

fn decode_retrieval_response(
    operation: Operation,
    response: Value,
) -> Result<MemoryVersionInput, OperationError> {
    let response = response
        .as_object()
        .ok_or_else(|| invalid_memory_response(operation))?;
    let affected_rows = response
        .get("affected_rows")
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid_memory_response(operation))?;
    match response.get("last_insert_id") {
        Some(Value::Null) => {}
        _ => return Err(invalid_memory_response(operation)),
    }
    let returned_rows = response
        .get("returned_rows")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid_memory_response(operation))?;
    let returned_row_count =
        u64::try_from(returned_rows.len()).map_err(|_| invalid_memory_response(operation))?;

    if affected_rows != returned_row_count {
        return Err(invalid_memory_response(operation));
    }

    match returned_rows.as_slice() {
        [] => Err(OperationError::not_found(operation)),
        [row] => decode_canonical_memory(operation, row),
        _ => Err(invalid_memory_response(operation)),
    }
}

fn decode_version_list_response(
    limit: u32,
    response: Value,
) -> Result<Vec<MemoryVersionSummary>, OperationError> {
    let response = response
        .as_object()
        .ok_or_else(invalid_version_summary_response)?;
    let affected_rows = response
        .get("affected_rows")
        .and_then(Value::as_u64)
        .ok_or_else(invalid_version_summary_response)?;
    match response.get("last_insert_id") {
        Some(Value::Null) => {}
        _ => return Err(invalid_version_summary_response()),
    }
    let returned_rows = response
        .get("returned_rows")
        .and_then(Value::as_array)
        .ok_or_else(invalid_version_summary_response)?;
    let returned_row_count =
        u64::try_from(returned_rows.len()).map_err(|_| invalid_version_summary_response())?;

    if affected_rows != returned_row_count || returned_row_count > u64::from(limit) {
        return Err(invalid_version_summary_response());
    }

    returned_rows.iter().map(decode_version_summary).collect()
}

fn decode_version_summary(row: &Value) -> Result<MemoryVersionSummary, OperationError> {
    let row = row
        .as_object()
        .ok_or_else(invalid_version_summary_response)?;
    let version = row
        .get("version")
        .and_then(Value::as_str)
        .ok_or_else(invalid_version_summary_response)?
        .parse::<i64>()
        .map_err(|_| invalid_version_summary_response())?;
    let updated_at = row
        .get("updated_at")
        .and_then(Value::as_str)
        .ok_or_else(invalid_version_summary_response)?;

    let updated_at = DateTime::parse_from_rfc3339(updated_at)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|_| invalid_version_summary_response())?;

    if !updated_at.timestamp_subsec_nanos().is_multiple_of(1_000)
        || !(1..=9999).contains(&updated_at.year())
    {
        return Err(invalid_version_summary_response());
    }

    Ok(MemoryVersionSummary {
        version: MemoryVersion::try_from(version)
            .map_err(|_| invalid_version_summary_response())?,
        updated_at,
    })
}

fn decode_canonical_memory(
    operation: Operation,
    row: &Value,
) -> Result<MemoryVersionInput, OperationError> {
    let row = row
        .as_object()
        .ok_or_else(|| invalid_memory_response(operation))?;
    let memory = MemoryVersionInput {
        id: canonical_string(operation, row, "id")?,
        version: canonical_string(operation, row, "version")?
            .parse()
            .map_err(|_| invalid_memory_response(operation))?,
        memory_type: canonical_string(operation, row, "memory_type")?,
        title: canonical_string(operation, row, "title")?,
        content: canonical_string(operation, row, "content")?,
        created_at: canonical_timestamp(operation, row, "created_at")?,
        updated_at: canonical_timestamp(operation, row, "updated_at")?,
        concepts: canonical_string_array(operation, row, "concepts")?,
        files: canonical_string_array(operation, row, "files")?,
        session_ids: canonical_string_array(operation, row, "session_ids")?,
        source_observation_ids: canonical_string_array(operation, row, "source_observation_ids")?,
    };

    ValidatedMemoryVersion::try_from(memory.clone())
        .map_err(|_| invalid_memory_response(operation))?;
    Ok(memory)
}

fn canonical_string(
    operation: Operation,
    row: &Map<String, Value>,
    column: &str,
) -> Result<String, OperationError> {
    row.get(column)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| invalid_memory_response(operation))
}

fn canonical_timestamp(
    operation: Operation,
    row: &Map<String, Value>,
    column: &str,
) -> Result<DateTime<Utc>, OperationError> {
    DateTime::parse_from_rfc3339(&canonical_string(operation, row, column)?)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|_| invalid_memory_response(operation))
}

fn canonical_string_array(
    operation: Operation,
    row: &Map<String, Value>,
    column: &str,
) -> Result<Vec<String>, OperationError> {
    row.get(column)
        .and_then(Value::as_array)
        .ok_or_else(|| invalid_memory_response(operation))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| invalid_memory_response(operation))
        })
        .collect()
}

fn invalid_memory_response(operation: Operation) -> OperationError {
    OperationError::invalid_backend_response(operation, BackendResponseField::Memory)
}

fn invalid_version_summary_response() -> OperationError {
    OperationError::invalid_backend_response(
        Operation::ListVersions,
        BackendResponseField::VersionSummary,
    )
}

pub struct CanonicalStoreDelegate<D: MemoryDatabase> {
    store: MemoryStore<D>,
}

impl<D: MemoryDatabase> CanonicalStoreDelegate<D> {
    pub fn new(store: MemoryStore<D>) -> Self {
        Self { store }
    }

    pub async fn insert_memory(&self, memory: MemoryVersionInput) -> Result<(), OperationError> {
        self.store
            .insert_memory(memory)
            .await
            .map_err(|error| map_store_error(Operation::InsertMemory, error))
    }

    pub async fn search_lexical(
        &self,
        query: Bm25Search,
    ) -> Result<Vec<MemorySearchResult>, OperationError> {
        self.store
            .search_bm25(query)
            .await
            .map_err(|error| map_store_error(Operation::SearchLexical, error))
    }

    pub async fn search_vector(
        &self,
        query: VectorSearch,
    ) -> Result<Vec<MemorySearchResult>, OperationError> {
        self.store
            .search_vector(query)
            .await
            .map_err(|error| map_store_error(Operation::SearchVector, error))
    }
}

pub struct ProductionMemoryOperations<D: MemoryDatabase> {
    canonical_store: CanonicalStoreDelegate<D>,
    retrieval: ReadOnlyRetrievalAdapter,
}

impl<D: MemoryDatabase> ProductionMemoryOperations<D> {
    pub fn new(
        canonical_store: CanonicalStoreDelegate<D>,
        retrieval: ReadOnlyRetrievalAdapter,
    ) -> Self {
        Self {
            canonical_store,
            retrieval,
        }
    }
}

#[async_trait::async_trait]
impl<D: MemoryDatabase> MemoryOperations for ProductionMemoryOperations<D> {
    async fn insert_memory(&self, memory: MemoryVersionInput) -> Result<(), OperationError> {
        self.canonical_store.insert_memory(memory).await
    }

    async fn search_lexical(
        &self,
        query: Bm25Search,
    ) -> Result<Vec<MemorySearchResult>, OperationError> {
        self.canonical_store.search_lexical(query).await
    }

    async fn search_vector(
        &self,
        query: VectorSearch,
    ) -> Result<Vec<MemorySearchResult>, OperationError> {
        self.canonical_store.search_vector(query).await
    }

    async fn get_latest(&self, id: MemoryId) -> Result<MemoryVersionInput, OperationError> {
        self.retrieval.get_latest(id).await
    }

    async fn get_exact(
        &self,
        id: MemoryId,
        version: MemoryVersion,
    ) -> Result<MemoryVersionInput, OperationError> {
        self.retrieval.get_exact(id, version).await
    }

    async fn list_versions(
        &self,
        query: VersionPageQuery,
    ) -> Result<Vec<MemoryVersionSummary>, OperationError> {
        self.retrieval.list_versions(query).await
    }
}

fn map_store_error(operation: Operation, error: MemoryStoreError) -> OperationError {
    match error {
        MemoryStoreError::InvalidInput(error) => {
            OperationError::invalid_input(operation, input_field(error.field()))
        }
        MemoryStoreError::Database(error) => map_database_error(operation, error),
    }
}

fn input_field(field: &'static str) -> OperationInputField {
    match field {
        "id" => OperationInputField::Id,
        "version" => OperationInputField::Version,
        "query" => OperationInputField::Query,
        "vector" => OperationInputField::Vector,
        "limit" => OperationInputField::Limit,
        _ => OperationInputField::Memory,
    }
}

fn map_database_error(operation: Operation, error: DatabaseError) -> OperationError {
    match error {
        DatabaseError::MissingMemoryVersion { .. } => OperationError::not_found(operation),
        DatabaseError::Conflict { .. } => OperationError::conflict(operation),
        DatabaseError::DatabaseFailure { .. } => {
            OperationError::backend_failure(operation, BackendSource::MemoryStore)
        }
        DatabaseError::InvalidResponse { .. } => {
            OperationError::invalid_backend_response(operation, response_field(operation))
        }
    }
}

fn response_field(operation: Operation) -> BackendResponseField {
    match operation {
        Operation::InsertMemory => BackendResponseField::InsertConfirmation,
        Operation::SearchLexical | Operation::SearchVector => BackendResponseField::SearchResult,
        Operation::GetLatest | Operation::GetExact => BackendResponseField::Memory,
        Operation::ListVersions => BackendResponseField::VersionSummary,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use chrono::{TimeZone, Utc};
    use iii_sdk::{Error as IiiError, IIIClient};

    use super::*;

    const DATABASE: &str = "database-target-sentinel";
    const ID_SENTINEL: &str = "memory-id-secret-sentinel";
    const TITLE_SENTINEL: &str = "memory-title-secret-sentinel";
    const CONTENT_SENTINEL: &str = "memory-content-secret-sentinel";
    const SDK_CODE_SENTINEL: &str = "sdk-code-secret-sentinel";
    const SDK_MESSAGE_SENTINEL: &str = "sdk-message-secret-sentinel";
    const SDK_STACKTRACE_SENTINEL: &str = "sdk-stacktrace-secret-sentinel";
    const EXPECTED_VERSION_LIST_SQL: &str = r#"SELECT
    memory.version::text AS version,
    memory.updated_at
FROM public.memories AS memory
WHERE memory.id = $1::text
ORDER BY memory.version DESC
OFFSET $2::text::bigint
LIMIT $3::text::bigint"#;

    fn database() -> DatabaseTarget {
        DatabaseTarget::try_from(DATABASE.to_owned()).expect("test database target should be valid")
    }

    fn memory_id() -> MemoryId {
        MemoryId::try_from(ID_SENTINEL.to_owned()).expect("test memory ID should be valid")
    }

    fn memory_version() -> MemoryVersion {
        MemoryVersion::try_from(7).expect("test memory version should be valid")
    }

    fn expected_memory() -> MemoryVersionInput {
        MemoryVersionInput {
            id: ID_SENTINEL.to_owned(),
            version: 7,
            memory_type: "unclassified".to_owned(),
            title: TITLE_SENTINEL.to_owned(),
            content: CONTENT_SENTINEL.to_owned(),
            created_at: Utc
                .with_ymd_and_hms(2026, 9, 20, 12, 34, 56)
                .single()
                .expect("test timestamp should be valid"),
            updated_at: Utc
                .with_ymd_and_hms(2026, 9, 20, 12, 35, 56)
                .single()
                .expect("test timestamp should be valid"),
            concepts: vec!["concept-sentinel".to_owned(), "concept-next".to_owned()],
            files: vec!["file-sentinel".to_owned()],
            session_ids: vec!["session-sentinel".to_owned()],
            source_observation_ids: vec!["source-sentinel".to_owned()],
        }
    }

    fn canonical_row(memory: &MemoryVersionInput) -> Value {
        json!({
            "id": memory.id,
            "version": memory.version.to_string(),
            "memory_type": memory.memory_type,
            "title": memory.title,
            "content": memory.content,
            "created_at": memory.created_at.to_rfc3339(),
            "updated_at": memory.updated_at.to_rfc3339(),
            "concepts": memory.concepts,
            "files": memory.files,
            "session_ids": memory.session_ids,
            "source_observation_ids": memory.source_observation_ids,
        })
    }

    fn retrieval_response(rows: Vec<Value>) -> Value {
        json!({
            "affected_rows": rows.len(),
            "last_insert_id": null,
            "returned_rows": rows,
        })
    }

    fn version_page_query(offset: u64, limit: u32) -> VersionPageQuery {
        VersionPageQuery {
            id: memory_id(),
            offset,
            limit,
        }
    }

    fn version_summary_row(version: &str, updated_at: &str) -> Value {
        json!({
            "version": version,
            "updated_at": updated_at,
        })
    }

    fn version_summary(version: i64, updated_at: &str) -> MemoryVersionSummary {
        MemoryVersionSummary {
            version: MemoryVersion::try_from(version).expect("test version should be positive"),
            updated_at: DateTime::parse_from_rfc3339(updated_at)
                .expect("test timestamp should be RFC3339")
                .with_timezone(&Utc),
        }
    }

    fn version_list_response(rows: Vec<Value>) -> Value {
        json!({
            "affected_rows": rows.len(),
            "last_insert_id": null,
            "returned_rows": rows,
        })
    }

    fn without_version_summary_field(mut row: Value, field: &str) -> Value {
        row.as_object_mut()
            .expect("test version summary row should be an object")
            .remove(field);
        row
    }

    fn assert_request(request: TriggerRequest, sql: &str, params: Value) {
        assert_eq!(request.function_id, DATABASE_EXECUTE_FUNCTION_ID);
        assert_eq!(
            request.payload,
            json!({
                "db": DATABASE,
                "sql": sql,
                "params": params,
            })
        );
        assert!(request.action.is_none());
        assert_eq!(request.timeout_ms, None);

        let sql = sql.to_ascii_lowercase();
        for forbidden in [
            "memory_embeddings",
            "embedding",
            "search_document",
            "relevance",
        ] {
            assert!(
                !sql.contains(forbidden),
                "retrieval SQL must not select or derive {forbidden}"
            );
        }
    }

    async fn latest_with_response(
        response: Value,
    ) -> (Result<MemoryVersionInput, OperationError>, usize) {
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocation_counter = Arc::clone(&invocations);
        let result = get_latest_with(&database(), memory_id(), move |_| {
            invocation_counter.fetch_add(1, Ordering::SeqCst);
            async move { Ok(response) }
        })
        .await;

        (result, invocations.load(Ordering::SeqCst))
    }

    async fn exact_with_response(
        response: Value,
    ) -> (Result<MemoryVersionInput, OperationError>, usize) {
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocation_counter = Arc::clone(&invocations);
        let result = get_exact_with(&database(), memory_id(), memory_version(), move |_| {
            invocation_counter.fetch_add(1, Ordering::SeqCst);
            async move { Ok(response) }
        })
        .await;

        (result, invocations.load(Ordering::SeqCst))
    }

    async fn list_versions_with_response(
        query: VersionPageQuery,
        response: Value,
    ) -> (Result<Vec<MemoryVersionSummary>, OperationError>, usize) {
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocation_counter = Arc::clone(&invocations);
        let result = list_versions_with(&database(), query, move |_| {
            invocation_counter.fetch_add(1, Ordering::SeqCst);
            async move { Ok(response) }
        })
        .await;

        (result, invocations.load(Ordering::SeqCst))
    }

    fn assert_opaque(error: &OperationError, values: &[&str]) {
        let display = error.to_string();
        let debug = format!("{error:?}");

        for value in values {
            assert!(
                !display.contains(value),
                "Display error leaked protected value: {display}"
            );
            assert!(
                !debug.contains(value),
                "Debug error leaked protected value: {debug}"
            );
        }
    }

    #[test]
    fn retrieval_adapter_construction_does_not_invoke_iii() {
        let client = IIIClient::new("ws://127.0.0.1:0");

        let _adapter = ReadOnlyRetrievalAdapter::new(client, database());
    }

    #[tokio::test]
    async fn exact_retrieval_uses_static_parameterized_sql_and_decodes_a_complete_memory() {
        let memory = expected_memory();
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocation_counter = Arc::clone(&invocations);
        let actual = get_exact_with(&database(), memory_id(), memory_version(), move |request| {
            assert_request(request, EXACT_MEMORY_SQL, json!([ID_SENTINEL, "7"]));
            invocation_counter.fetch_add(1, Ordering::SeqCst);
            async move { Ok(retrieval_response(vec![canonical_row(&memory)])) }
        })
        .await
        .expect("one complete exact row should decode");

        assert_eq!(actual, expected_memory());
        assert_eq!(invocations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn latest_retrieval_uses_current_head_sql_and_decodes_a_complete_memory() {
        let memory = expected_memory();
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocation_counter = Arc::clone(&invocations);
        let actual = get_latest_with(&database(), memory_id(), move |request| {
            assert_request(request, LATEST_MEMORY_SQL, json!([ID_SENTINEL]));
            invocation_counter.fetch_add(1, Ordering::SeqCst);
            async move { Ok(retrieval_response(vec![canonical_row(&memory)])) }
        })
        .await
        .expect("one complete current-head row should decode");

        assert_eq!(actual, expected_memory());
        assert_eq!(invocations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn version_listing_uses_static_parameterized_sql_and_preserves_database_order() {
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocation_counter = Arc::clone(&invocations);
        let actual = list_versions_with(&database(), version_page_query(17, 101), move |request| {
            assert_request(
                request,
                EXPECTED_VERSION_LIST_SQL,
                json!([ID_SENTINEL, "17", "101"]),
            );
            invocation_counter.fetch_add(1, Ordering::SeqCst);
            async move {
                Ok(version_list_response(vec![
                    version_summary_row("3", "2026-09-20T12:35:56+00:00"),
                    version_summary_row("7", "2026-09-20T12:34:56+00:00"),
                ]))
            }
        })
        .await
        .expect("complete ordered version rows should decode");

        assert_eq!(
            actual,
            vec![
                version_summary(3, "2026-09-20T12:35:56+00:00"),
                version_summary(7, "2026-09-20T12:34:56+00:00"),
            ]
        );
        assert_eq!(invocations.load(Ordering::SeqCst), 1);

        let sql = EXPECTED_VERSION_LIST_SQL.to_ascii_lowercase();
        for forbidden in [
            "title",
            "content",
            "created_at",
            "concepts",
            "files",
            "session_ids",
            "source_observation_ids",
            "memory_embeddings",
            "embedding",
            "search_document",
            "relevance",
        ] {
            assert!(
                !sql.contains(forbidden),
                "version-list SQL must not select or derive {forbidden}"
            );
        }
    }

    #[tokio::test]
    async fn version_listing_accepts_an_exact_101_row_probe_and_preserves_database_order() {
        let updated_at = "2026-09-20T12:35:56+00:00";
        let database_order = (1_i64..=101)
            .map(|version| if version == 101 { 1 } else { version + 1 })
            .collect::<Vec<_>>();
        let rows = database_order
            .iter()
            .map(|version| version_summary_row(&version.to_string(), updated_at))
            .collect();
        let expected = database_order
            .iter()
            .map(|version| version_summary(*version, updated_at))
            .collect::<Vec<_>>();

        let (result, invocations) =
            list_versions_with_response(version_page_query(0, 101), version_list_response(rows))
                .await;

        assert_eq!(invocations, 1);
        assert_eq!(
            result.expect("an exact requested probe size should decode"),
            expected
        );
    }

    #[tokio::test]
    async fn empty_version_pages_are_successful_after_one_invocation() {
        let (result, invocations) = list_versions_with_response(
            version_page_query(9, 8),
            version_list_response(Vec::new()),
        )
        .await;

        assert_eq!(invocations, 1);
        assert_eq!(
            result.expect("empty version pages should decode"),
            Vec::<MemoryVersionSummary>::new()
        );
    }

    #[tokio::test]
    async fn malformed_version_pages_are_rejected_all_or_error_without_content_leakage() {
        let row = version_summary_row("7", "2026-09-20T12:35:56+00:00");
        let mut numeric_version = row.clone();
        numeric_version["version"] = json!(7);
        let mut zero_version = row.clone();
        zero_version["version"] = json!("0");
        let mut negative_version = row.clone();
        negative_version["version"] = json!("-7");
        let mut malformed_timestamp = row.clone();
        malformed_timestamp["updated_at"] = json!("not-rfc3339");
        let mut numeric_timestamp = row.clone();
        numeric_timestamp["updated_at"] = json!(7);
        let mut malformed_second_row = row.clone();
        malformed_second_row["updated_at"] = json!("not-rfc3339");
        let malformed = [
            Value::Null,
            json!({
                "last_insert_id": null,
                "returned_rows": [row.clone()],
            }),
            json!({
                "affected_rows": 1,
                "last_insert_id": "database-password-sentinel",
                "returned_rows": [row.clone()],
            }),
            json!({
                "affected_rows": 0,
                "last_insert_id": null,
                "returned_rows": [row.clone()],
            }),
            json!({
                "affected_rows": 1,
                "last_insert_id": null,
                "returned_rows": {},
            }),
            version_list_response(vec![Value::Null]),
            version_list_response(vec![without_version_summary_field(row.clone(), "version")]),
            version_list_response(vec![without_version_summary_field(
                row.clone(),
                "updated_at",
            )]),
            version_list_response(vec![numeric_version]),
            version_list_response(vec![zero_version]),
            version_list_response(vec![negative_version]),
            version_list_response(vec![malformed_timestamp]),
            version_list_response(vec![numeric_timestamp]),
            version_list_response(vec![row, malformed_second_row]),
        ];

        for response in malformed {
            let (result, invocations) =
                list_versions_with_response(version_page_query(0, 101), response).await;

            assert_eq!(invocations, 1);
            let error = result.expect_err("malformed version pages must fail as a unit");
            assert_eq!(error, invalid_version_summary_response());
            assert_opaque(
                &error,
                &[
                    ID_SENTINEL,
                    TITLE_SENTINEL,
                    CONTENT_SENTINEL,
                    "database-password-sentinel",
                ],
            );
        }
    }

    #[tokio::test]
    async fn version_pages_reject_submicrosecond_updated_at_without_content_leakage() {
        let fractional_timestamp = "2026-09-20T12:35:56.123456001+00:00";
        let (result, invocations) = list_versions_with_response(
            version_page_query(0, 101),
            version_list_response(vec![version_summary_row("7", fractional_timestamp)]),
        )
        .await;

        assert_eq!(invocations, 1);
        let error = result.expect_err("submicrosecond updated_at values must be rejected");
        assert_eq!(error, invalid_version_summary_response());
        assert_opaque(&error, &[ID_SENTINEL, fractional_timestamp]);
    }

    #[tokio::test]
    async fn version_pages_reject_parseable_unsupported_updated_at_years_without_content_leakage() {
        let unsupported_year = "0000-09-20T12:35:56+00:00";
        assert_eq!(
            DateTime::parse_from_rfc3339(unsupported_year)
                .expect("year zero should be parseable")
                .with_timezone(&Utc)
                .year(),
            0
        );

        let (result, invocations) = list_versions_with_response(
            version_page_query(0, 101),
            version_list_response(vec![version_summary_row("7", unsupported_year)]),
        )
        .await;

        assert_eq!(invocations, 1);
        let error = result.expect_err("unsupported updated_at years must be rejected");
        assert_eq!(error, invalid_version_summary_response());
        assert_opaque(&error, &[ID_SENTINEL, unsupported_year]);
    }

    #[tokio::test]
    async fn version_pages_exceeding_the_requested_101_row_probe_are_rejected() {
        let rows = (0..102)
            .map(|_| version_summary_row("7", "2026-09-20T12:35:56+00:00"))
            .collect();
        let (result, invocations) =
            list_versions_with_response(version_page_query(0, 101), version_list_response(rows))
                .await;

        assert_eq!(invocations, 1);
        assert_eq!(
            result.expect_err("pages beyond the requested probe limit must fail"),
            invalid_version_summary_response()
        );
    }

    #[tokio::test]
    async fn version_listing_maps_iii_errors_to_opaque_database_failures_without_retries() {
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocation_counter = Arc::clone(&invocations);
        let error = list_versions_with(&database(), version_page_query(0, 101), move |_| {
            invocation_counter.fetch_add(1, Ordering::SeqCst);
            async {
                Err(IiiError::Remote {
                    code: SDK_CODE_SENTINEL.to_owned(),
                    message: SDK_MESSAGE_SENTINEL.to_owned(),
                    stacktrace: Some(SDK_STACKTRACE_SENTINEL.to_owned()),
                })
            }
        })
        .await
        .expect_err("iii failures should map to an opaque database failure");

        assert_eq!(invocations.load(Ordering::SeqCst), 1);
        assert_eq!(
            error,
            OperationError::backend_failure(Operation::ListVersions, BackendSource::Database)
        );
        assert_opaque(
            &error,
            &[
                ID_SENTINEL,
                SDK_CODE_SENTINEL,
                SDK_MESSAGE_SENTINEL,
                SDK_STACKTRACE_SENTINEL,
            ],
        );
    }

    #[tokio::test]
    async fn empty_canonical_results_are_not_found_after_one_invocation() {
        let (latest, latest_invocations) =
            latest_with_response(retrieval_response(Vec::new())).await;
        let (exact, exact_invocations) = exact_with_response(retrieval_response(Vec::new())).await;

        assert_eq!(latest_invocations, 1);
        assert_eq!(exact_invocations, 1);
        assert_eq!(latest, Err(OperationError::not_found(Operation::GetLatest)));
        assert_eq!(exact, Err(OperationError::not_found(Operation::GetExact)));
    }

    #[tokio::test]
    async fn excess_canonical_rows_are_rejected_after_one_invocation() {
        let row = canonical_row(&expected_memory());
        let response = retrieval_response(vec![row.clone(), row]);
        let (latest, latest_invocations) = latest_with_response(response.clone()).await;
        let (exact, exact_invocations) = exact_with_response(response).await;

        assert_eq!(latest_invocations, 1);
        assert_eq!(exact_invocations, 1);
        assert_eq!(
            latest,
            Err(OperationError::invalid_backend_response(
                Operation::GetLatest,
                BackendResponseField::Memory,
            ))
        );
        assert_eq!(
            exact,
            Err(OperationError::invalid_backend_response(
                Operation::GetExact,
                BackendResponseField::Memory,
            ))
        );
    }

    #[tokio::test]
    async fn malformed_envelopes_and_rows_are_content_safe_invalid_backend_responses() {
        let row = canonical_row(&expected_memory());
        let mut malformed_version = row.clone();
        malformed_version["version"] = json!(7);
        let mut malformed_concepts = row.clone();
        malformed_concepts["concepts"] = json!(["concept-sentinel", 7]);
        let mut invalid_title = row.clone();
        invalid_title["title"] = json!("");
        let malformed = [
            Value::Null,
            json!({
                "last_insert_id": null,
                "returned_rows": [row.clone()],
            }),
            json!({
                "affected_rows": 1,
                "last_insert_id": "database-password-sentinel",
                "returned_rows": [row.clone()],
            }),
            json!({
                "affected_rows": 0,
                "last_insert_id": null,
                "returned_rows": [row.clone()],
            }),
            retrieval_response(vec![Value::Null]),
            retrieval_response(vec![malformed_version]),
            retrieval_response(vec![malformed_concepts]),
            retrieval_response(vec![invalid_title]),
        ];

        for response in malformed {
            let (latest, latest_invocations) = latest_with_response(response.clone()).await;
            let (exact, exact_invocations) = exact_with_response(response).await;

            assert_eq!(latest_invocations, 1);
            assert_eq!(exact_invocations, 1);
            let latest_error = latest.expect_err("malformed canonical data must fail as a unit");
            let exact_error = exact.expect_err("malformed canonical data must fail as a unit");
            assert_eq!(
                latest_error,
                OperationError::invalid_backend_response(
                    Operation::GetLatest,
                    BackendResponseField::Memory,
                )
            );
            assert_eq!(
                exact_error,
                OperationError::invalid_backend_response(
                    Operation::GetExact,
                    BackendResponseField::Memory,
                )
            );
            for error in [&latest_error, &exact_error] {
                assert_opaque(
                    error,
                    &[
                        ID_SENTINEL,
                        TITLE_SENTINEL,
                        CONTENT_SENTINEL,
                        "concept-sentinel",
                        "database-password-sentinel",
                    ],
                );
            }
        }
    }

    #[tokio::test]
    async fn iii_errors_are_database_failures_without_raw_sdk_data_or_retries() {
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocation_counter = Arc::clone(&invocations);
        let error = get_exact_with(&database(), memory_id(), memory_version(), move |_| {
            invocation_counter.fetch_add(1, Ordering::SeqCst);
            async {
                Err(IiiError::Remote {
                    code: SDK_CODE_SENTINEL.to_owned(),
                    message: SDK_MESSAGE_SENTINEL.to_owned(),
                    stacktrace: Some(SDK_STACKTRACE_SENTINEL.to_owned()),
                })
            }
        })
        .await
        .expect_err("iii failures should map to an opaque database failure");

        assert_eq!(invocations.load(Ordering::SeqCst), 1);
        assert_eq!(
            error,
            OperationError::backend_failure(Operation::GetExact, BackendSource::Database)
        );
        assert_opaque(
            &error,
            &[
                ID_SENTINEL,
                SDK_CODE_SENTINEL,
                SDK_MESSAGE_SENTINEL,
                SDK_STACKTRACE_SENTINEL,
            ],
        );
    }
}
