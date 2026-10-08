use std::future::Future;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use iii_sdk::{IIIClient, protocol::TriggerRequest};
use memory_store::{
    MemoryStore,
    contracts::{
        DatabaseError, DatabaseTarget, MemoryStoreError, MemoryVersionInput, ValidatedMemoryVersion,
    },
    database::{IiiMemoryDatabase, MemoryDatabase},
};
use serde_json::{Map, Value, json};

use crate::ports::{CanonicalMemorySink, EnsureMemoryOutcome, MemoryPublishError};

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

pub struct IiiCanonicalMemoryPublisher {
    memory_store: MemoryStore<IiiMemoryDatabase>,
    client: IIIClient,
    database: DatabaseTarget,
}

impl IiiCanonicalMemoryPublisher {
    pub fn new(client: IIIClient, database: DatabaseTarget) -> Self {
        let memory_store =
            MemoryStore::new(IiiMemoryDatabase::new(client.clone(), database.clone()));

        Self {
            memory_store,
            client,
            database,
        }
    }

    pub async fn ensure_memory(
        &self,
        memory: MemoryVersionInput,
    ) -> Result<EnsureMemoryOutcome, MemoryPublishError> {
        ensure_memory_with(&self.memory_store, memory, |id, version| async move {
            lookup_exact_with(&self.database, &id, version, |request| {
                self.client.trigger(request)
            })
            .await
        })
        .await
    }
}

#[async_trait]
impl CanonicalMemorySink for IiiCanonicalMemoryPublisher {
    async fn ensure_memory(
        &self,
        memory: MemoryVersionInput,
    ) -> Result<EnsureMemoryOutcome, MemoryPublishError> {
        Self::ensure_memory(self, memory).await
    }
}

pub async fn ensure_memory_with<D, F, Fut>(
    memory_store: &MemoryStore<D>,
    memory: MemoryVersionInput,
    lookup: F,
) -> Result<EnsureMemoryOutcome, MemoryPublishError>
where
    D: MemoryDatabase,
    F: FnOnce(String, i64) -> Fut,
    Fut: Future<Output = Result<Option<MemoryVersionInput>, MemoryPublishError>>,
{
    match memory_store.insert_memory(memory.clone()).await {
        Ok(()) => Ok(EnsureMemoryOutcome::Inserted),
        Err(MemoryStoreError::InvalidInput(_)) => Err(MemoryPublishError::unknown()),
        Err(MemoryStoreError::Database(DatabaseError::Conflict { .. })) => {
            recover_exact_memory(memory, lookup, MemoryPublishError::mismatch()).await
        }
        Err(MemoryStoreError::Database(_)) => {
            recover_exact_memory(memory, lookup, MemoryPublishError::unknown()).await
        }
    }
}

pub async fn lookup_exact_with<F, Fut>(
    database: &DatabaseTarget,
    id: &str,
    version: i64,
    invoke: F,
) -> Result<Option<MemoryVersionInput>, MemoryPublishError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    let response = invoke(build_exact_lookup_request(database, id, version))
        .await
        .map_err(|_| MemoryPublishError::unknown())?;

    decode_exact_lookup_response(response)
}

async fn recover_exact_memory<F, Fut>(
    memory: MemoryVersionInput,
    lookup: F,
    missing_error: MemoryPublishError,
) -> Result<EnsureMemoryOutcome, MemoryPublishError>
where
    F: FnOnce(String, i64) -> Fut,
    Fut: Future<Output = Result<Option<MemoryVersionInput>, MemoryPublishError>>,
{
    let stored = lookup(memory.id.clone(), memory.version)
        .await
        .map_err(|_| MemoryPublishError::unknown())?;

    match stored {
        Some(stored) if stored == memory => Ok(EnsureMemoryOutcome::ExistingEquivalent),
        Some(_) => Err(MemoryPublishError::mismatch()),
        None => Err(missing_error),
    }
}

fn build_exact_lookup_request(database: &DatabaseTarget, id: &str, version: i64) -> TriggerRequest {
    TriggerRequest {
        function_id: DATABASE_EXECUTE_FUNCTION_ID.to_owned(),
        payload: json!({
            "db": database.as_str(),
            "sql": EXACT_MEMORY_SQL,
            "params": [id, version.to_string()],
        }),
        action: None,
        timeout_ms: None,
    }
}

fn decode_exact_lookup_response(
    response: Value,
) -> Result<Option<MemoryVersionInput>, MemoryPublishError> {
    let response = response
        .as_object()
        .filter(|response| response.len() == 3)
        .ok_or_else(MemoryPublishError::unknown)?;
    let affected_rows = response
        .get("affected_rows")
        .and_then(Value::as_u64)
        .ok_or_else(MemoryPublishError::unknown)?;
    if !matches!(response.get("last_insert_id"), Some(Value::Null)) {
        return Err(MemoryPublishError::unknown());
    }
    let rows = response
        .get("returned_rows")
        .and_then(Value::as_array)
        .ok_or_else(MemoryPublishError::unknown)?;
    let row_count = u64::try_from(rows.len()).map_err(|_| MemoryPublishError::unknown())?;
    if affected_rows != row_count {
        return Err(MemoryPublishError::unknown());
    }

    match rows.as_slice() {
        [] => Ok(None),
        [row] => decode_canonical_memory(row).map(Some),
        _ => Err(MemoryPublishError::unknown()),
    }
}

fn decode_canonical_memory(row: &Value) -> Result<MemoryVersionInput, MemoryPublishError> {
    let row = row
        .as_object()
        .filter(|row| row.len() == 11)
        .ok_or_else(MemoryPublishError::unknown)?;
    let memory = MemoryVersionInput {
        id: required_string(row, "id")?,
        version: required_string(row, "version")?
            .parse()
            .map_err(|_| MemoryPublishError::unknown())?,
        memory_type: required_string(row, "memory_type")?,
        title: required_string(row, "title")?,
        content: required_string(row, "content")?,
        created_at: required_timestamp(row, "created_at")?,
        updated_at: required_timestamp(row, "updated_at")?,
        concepts: required_string_array(row, "concepts")?,
        files: required_string_array(row, "files")?,
        session_ids: required_string_array(row, "session_ids")?,
        source_observation_ids: required_string_array(row, "source_observation_ids")?,
    };

    ValidatedMemoryVersion::try_from(memory.clone()).map_err(|_| MemoryPublishError::unknown())?;
    Ok(memory)
}

fn required_string(
    row: &Map<String, Value>,
    column: &'static str,
) -> Result<String, MemoryPublishError> {
    row.get(column)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(MemoryPublishError::unknown)
}

fn required_timestamp(
    row: &Map<String, Value>,
    column: &'static str,
) -> Result<DateTime<Utc>, MemoryPublishError> {
    DateTime::parse_from_rfc3339(&required_string(row, column)?)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|_| MemoryPublishError::unknown())
}

fn required_string_array(
    row: &Map<String, Value>,
    column: &'static str,
) -> Result<Vec<String>, MemoryPublishError> {
    row.get(column)
        .and_then(Value::as_array)
        .ok_or_else(MemoryPublishError::unknown)?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(MemoryPublishError::unknown)
        })
        .collect()
}
