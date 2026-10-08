//! Memory work discovery.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use iii_sdk::{IIIClient, protocol::TriggerRequest};
use memory_store::contracts::DatabaseTarget;
use serde_json::{Map, Value, json};

use crate::contracts::{
    Deadline, EmbeddingError, EmbeddingWorkItem, EmbeddingWorkRepository, LoadedEmbeddingWork,
    MemoryKey, RepositoryError, RepositoryFailure,
};

const DATABASE_EXECUTE_FUNCTION_ID: &str = "database::execute";
const LOAD_KEYS_SQL: &str = r#"WITH requested AS (
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
const LIST_MISSING_SQL: &str = r#"SELECT
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

#[async_trait]
#[doc(hidden)]
pub trait DatabaseExecutor: Send + Sync {
    async fn execute(&self, request: TriggerRequest) -> Result<Value, iii_sdk::Error>;
}

#[async_trait]
impl DatabaseExecutor for IIIClient {
    async fn execute(&self, request: TriggerRequest) -> Result<Value, iii_sdk::Error> {
        self.trigger(request).await
    }
}

pub struct IiiEmbeddingWorkRepository<E = IIIClient> {
    executor: E,
    database: DatabaseTarget,
    database_timeout: Duration,
}

impl IiiEmbeddingWorkRepository<IIIClient> {
    pub fn new(client: IIIClient, database: DatabaseTarget, database_timeout: Duration) -> Self {
        Self::with_executor(client, database, database_timeout)
    }
}

impl<E> IiiEmbeddingWorkRepository<E>
where
    E: DatabaseExecutor,
{
    #[doc(hidden)]
    pub fn with_executor(
        executor: E,
        database: DatabaseTarget,
        database_timeout: Duration,
    ) -> Self {
        Self {
            executor,
            database,
            database_timeout,
        }
    }

    pub async fn load_keys(
        &self,
        keys: &[MemoryKey],
        deadline: Deadline,
    ) -> Result<Vec<LoadedEmbeddingWork>, RepositoryError> {
        let request_deadline = request_deadline(self.database_timeout, deadline);
        if request_deadline
            .saturating_duration_since(Instant::now())
            .is_zero()
        {
            return Err(repository_error(RepositoryFailure::Timeout));
        }

        let response = tokio::time::timeout_at(
            tokio::time::Instant::from_std(request_deadline),
            self.executor
                .execute(build_load_keys_request(&self.database, keys)),
        )
        .await
        .map_err(|_| repository_error(RepositoryFailure::Timeout))?
        .map_err(|_| repository_error(RepositoryFailure::Backend))?;

        decode_load_keys_response(keys, response)
    }

    pub async fn list_missing(
        &self,
        limit: u32,
        deadline: Deadline,
    ) -> Result<Vec<EmbeddingWorkItem>, RepositoryError> {
        let request_deadline = request_deadline(self.database_timeout, deadline);
        if request_deadline
            .saturating_duration_since(Instant::now())
            .is_zero()
        {
            return Err(repository_error(RepositoryFailure::Timeout));
        }

        let response = tokio::time::timeout_at(
            tokio::time::Instant::from_std(request_deadline),
            self.executor
                .execute(build_list_missing_request(&self.database, limit)),
        )
        .await
        .map_err(|_| repository_error(RepositoryFailure::Timeout))?
        .map_err(|_| repository_error(RepositoryFailure::Backend))?;

        decode_list_missing_response(limit, response)
    }
}

fn request_deadline(timeout: Duration, deadline: Deadline) -> Instant {
    let local_deadline = Instant::now()
        .checked_add(timeout)
        .unwrap_or_else(|| deadline.instant());

    local_deadline.min(deadline.instant())
}

#[async_trait]
impl<E> EmbeddingWorkRepository for IiiEmbeddingWorkRepository<E>
where
    E: DatabaseExecutor,
{
    async fn load_keys(
        &self,
        keys: &[MemoryKey],
        deadline: Deadline,
    ) -> Result<Vec<LoadedEmbeddingWork>, RepositoryError> {
        IiiEmbeddingWorkRepository::load_keys(self, keys, deadline).await
    }

    async fn list_missing(
        &self,
        limit: u32,
        deadline: Deadline,
    ) -> Result<Vec<EmbeddingWorkItem>, RepositoryError> {
        IiiEmbeddingWorkRepository::list_missing(self, limit, deadline).await
    }
}

fn build_load_keys_request(database: &DatabaseTarget, keys: &[MemoryKey]) -> TriggerRequest {
    TriggerRequest {
        function_id: DATABASE_EXECUTE_FUNCTION_ID.to_owned(),
        payload: json!({
            "db": database.as_str(),
            "sql": LOAD_KEYS_SQL,
            "params": [keys.iter().map(|key| json!({
                "id": key.id(),
                "version": key.version().to_string(),
            })).collect::<Vec<_>>()],
        }),
        action: None,
        timeout_ms: None,
    }
}

fn build_list_missing_request(database: &DatabaseTarget, limit: u32) -> TriggerRequest {
    TriggerRequest {
        function_id: DATABASE_EXECUTE_FUNCTION_ID.to_owned(),
        payload: json!({
            "db": database.as_str(),
            "sql": LIST_MISSING_SQL,
            "params": [limit.to_string()],
        }),
        action: None,
        timeout_ms: None,
    }
}

fn decode_load_keys_response(
    keys: &[MemoryKey],
    response: Value,
) -> Result<Vec<LoadedEmbeddingWork>, RepositoryError> {
    let response = exact_object(
        &response,
        &["affected_rows", "last_insert_id", "returned_rows"],
    )?;
    let affected_rows = value(response, "affected_rows")
        .and_then(|value| value.as_u64().ok_or_else(malformed_response))?;
    let expected_rows = u64::try_from(keys.len()).map_err(|_| malformed_response())?;
    if affected_rows != expected_rows || value(response, "last_insert_id")? != &Value::Null {
        return Err(malformed_response());
    }
    let rows = value(response, "returned_rows")
        .and_then(|value| value.as_array().ok_or_else(malformed_response))?;

    if rows.len() != keys.len() {
        return Err(malformed_response());
    }

    rows.iter()
        .zip(keys)
        .map(|(row, key)| decode_load_keys_row(key, row))
        .collect()
}

fn decode_load_keys_row(
    key: &MemoryKey,
    row: &Value,
) -> Result<LoadedEmbeddingWork, RepositoryError> {
    let row = exact_object(
        row,
        &[
            "id",
            "version",
            "title",
            "content",
            "concepts",
            "memory_present",
            "embedding_present",
        ],
    )?;
    let id = string_value(row, "id")?;
    let version = string_value(row, "version")?;
    if id != key.id() || version != key.version().to_string() {
        return Err(malformed_response());
    }

    let memory_present = bool_value(row, "memory_present")?;
    let embedding_present = bool_value(row, "embedding_present")?;
    match (memory_present, embedding_present) {
        (false, false) => {
            nullable_work_fields_must_be_null(row)?;
            Ok(LoadedEmbeddingWork::Missing(key.clone()))
        }
        (true, true) => {
            nullable_work_fields_must_be_null(row)?;
            Ok(LoadedEmbeddingWork::AlreadyPresent(key.clone()))
        }
        (true, false) => Ok(LoadedEmbeddingWork::Pending(EmbeddingWorkItem::new(
            key.clone(),
            pending_text(row, "title")?,
            pending_text(row, "content")?,
            pending_concepts(row)?,
        ))),
        (false, true) => Err(malformed_response()),
    }
}

fn decode_list_missing_response(
    limit: u32,
    response: Value,
) -> Result<Vec<EmbeddingWorkItem>, RepositoryError> {
    let response = exact_object(
        &response,
        &["affected_rows", "last_insert_id", "returned_rows"],
    )?;
    let affected_rows = value(response, "affected_rows")
        .and_then(|value| value.as_u64().ok_or_else(malformed_response))?;
    let rows = value(response, "returned_rows")
        .and_then(|value| value.as_array().ok_or_else(malformed_response))?;
    let returned_rows = u64::try_from(rows.len()).map_err(|_| malformed_response())?;
    if value(response, "last_insert_id")? != &Value::Null
        || affected_rows != returned_rows
        || affected_rows > u64::from(limit)
    {
        return Err(malformed_response());
    }

    let work = rows
        .iter()
        .map(decode_list_missing_row)
        .collect::<Result<Vec<_>, _>>()?;
    if work
        .windows(2)
        .any(|items| !is_strictly_before(&items[0], &items[1]))
    {
        return Err(malformed_response());
    }

    Ok(work)
}

fn decode_list_missing_row(row: &Value) -> Result<EmbeddingWorkItem, RepositoryError> {
    let row = exact_object(row, &["id", "version", "title", "content", "concepts"])?;
    let key = MemoryKey::try_new(
        string_value(row, "id")?.to_owned(),
        string_value(row, "version")?,
    )
    .map_err(|_| malformed_response())?;

    Ok(EmbeddingWorkItem::new(
        key,
        pending_text(row, "title")?,
        pending_text(row, "content")?,
        pending_concepts(row)?,
    ))
}

fn is_strictly_before(left: &EmbeddingWorkItem, right: &EmbeddingWorkItem) -> bool {
    match left.key().id().as_bytes().cmp(right.key().id().as_bytes()) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Equal => left.key().version() < right.key().version(),
        std::cmp::Ordering::Greater => false,
    }
}

fn exact_object<'a>(
    value: &'a Value,
    expected_fields: &[&str],
) -> Result<&'a Map<String, Value>, RepositoryError> {
    let object = value.as_object().ok_or_else(malformed_response)?;
    if object.len() != expected_fields.len()
        || expected_fields
            .iter()
            .any(|field| !object.contains_key(*field))
    {
        return Err(malformed_response());
    }

    Ok(object)
}

fn value<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a Value, RepositoryError> {
    object.get(field).ok_or_else(malformed_response)
}

fn string_value<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a str, RepositoryError> {
    value(object, field)?
        .as_str()
        .ok_or_else(malformed_response)
}

fn bool_value(object: &Map<String, Value>, field: &str) -> Result<bool, RepositoryError> {
    value(object, field)?
        .as_bool()
        .ok_or_else(malformed_response)
}

fn nullable_work_fields_must_be_null(row: &Map<String, Value>) -> Result<(), RepositoryError> {
    if ["title", "content", "concepts"]
        .iter()
        .any(|field| !matches!(value(row, field), Ok(Value::Null)))
    {
        return Err(malformed_response());
    }

    Ok(())
}

fn pending_text(row: &Map<String, Value>, field: &str) -> Result<String, RepositoryError> {
    let text = string_value(row, field)?;
    if text.is_empty() || text.contains('\0') {
        return Err(malformed_response());
    }

    Ok(text.to_owned())
}

fn pending_concepts(row: &Map<String, Value>) -> Result<Vec<String>, RepositoryError> {
    let concepts =
        value(row, "concepts").and_then(|value| value.as_array().ok_or_else(malformed_response))?;
    concepts
        .iter()
        .map(|concept| {
            let concept = concept.as_str().ok_or_else(malformed_response)?;
            if concept.contains('\0') {
                return Err(malformed_response());
            }

            Ok(concept.to_owned())
        })
        .collect()
}

const fn repository_error(reason: RepositoryFailure) -> RepositoryError {
    EmbeddingError::repository(reason)
}

fn malformed_response() -> RepositoryError {
    repository_error(RepositoryFailure::MalformedResponse)
}
