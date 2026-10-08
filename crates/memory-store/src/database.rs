use std::future::Future;

use chrono::{DateTime, Utc};
use iii_sdk::{IIIClient, protocol::TriggerRequest};
use serde_json::{Map, Value, json};

use crate::contracts::{
    DatabaseError, DatabaseOperation, DatabaseTarget, MemorySearchResult, MemoryVersionInput,
    MemoryVersionKey, ValidatedBm25Search, ValidatedEmbedding, ValidatedMemoryVersion,
    ValidatedVectorSearch,
};

const DATABASE_EXECUTE_FUNCTION_ID: &str = "database::execute";
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

struct PositionalParameters(Vec<Value>);

impl From<Vec<Value>> for PositionalParameters {
    fn from(values: Vec<Value>) -> Self {
        Self(values)
    }
}

impl PositionalParameters {
    fn into_json(self) -> Value {
        Value::Array(self.0)
    }
}

#[async_trait::async_trait]
pub trait MemoryDatabase: Send + Sync {
    async fn insert_memory(&self, memory: &ValidatedMemoryVersion) -> Result<(), DatabaseError>;
    async fn insert_embedding(&self, embedding: &ValidatedEmbedding) -> Result<(), DatabaseError>;
    async fn search_bm25(
        &self,
        query: &ValidatedBm25Search,
    ) -> Result<Vec<MemorySearchResult>, DatabaseError>;
    async fn search_vector(
        &self,
        query: &ValidatedVectorSearch,
    ) -> Result<Vec<MemorySearchResult>, DatabaseError>;
}

pub struct IiiMemoryDatabase {
    client: IIIClient,
    database: DatabaseTarget,
}

impl IiiMemoryDatabase {
    pub fn new(client: IIIClient, database: DatabaseTarget) -> Self {
        Self { client, database }
    }

    #[allow(dead_code)]
    async fn invoke(
        &self,
        operation: DatabaseOperation,
        sql: &'static str,
        params: PositionalParameters,
    ) -> Result<Value, DatabaseError> {
        execute(&self.database, operation, sql, params, |request| {
            self.client.trigger(request)
        })
        .await
    }

    async fn insert_memory_impl(
        &self,
        memory: &ValidatedMemoryVersion,
    ) -> Result<(), DatabaseError> {
        insert_memory_with(&self.database, memory, |request| {
            self.client.trigger(request)
        })
        .await
    }

    async fn insert_embedding_impl(
        &self,
        embedding: &ValidatedEmbedding,
    ) -> Result<(), DatabaseError> {
        insert_embedding_with(&self.database, embedding, |request| {
            self.client.trigger(request)
        })
        .await
    }

    async fn search_bm25_impl(
        &self,
        query: &ValidatedBm25Search,
    ) -> Result<Vec<MemorySearchResult>, DatabaseError> {
        search_bm25_with(&self.database, query, |request| {
            self.client.trigger(request)
        })
        .await
    }

    async fn search_vector_impl(
        &self,
        query: &ValidatedVectorSearch,
    ) -> Result<Vec<MemorySearchResult>, DatabaseError> {
        search_vector_with(&self.database, query, |request| {
            self.client.trigger(request)
        })
        .await
    }
}

#[async_trait::async_trait]
impl MemoryDatabase for IiiMemoryDatabase {
    async fn insert_memory(&self, memory: &ValidatedMemoryVersion) -> Result<(), DatabaseError> {
        self.insert_memory_impl(memory).await
    }

    async fn insert_embedding(&self, embedding: &ValidatedEmbedding) -> Result<(), DatabaseError> {
        self.insert_embedding_impl(embedding).await
    }

    async fn search_bm25(
        &self,
        query: &ValidatedBm25Search,
    ) -> Result<Vec<MemorySearchResult>, DatabaseError> {
        self.search_bm25_impl(query).await
    }

    async fn search_vector(
        &self,
        query: &ValidatedVectorSearch,
    ) -> Result<Vec<MemorySearchResult>, DatabaseError> {
        self.search_vector_impl(query).await
    }
}

async fn execute<F, Fut>(
    database: &DatabaseTarget,
    operation: DatabaseOperation,
    sql: &'static str,
    params: PositionalParameters,
    invoke: F,
) -> Result<Value, DatabaseError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    let request = TriggerRequest {
        function_id: DATABASE_EXECUTE_FUNCTION_ID.to_owned(),
        payload: json!({
            "db": database.as_str(),
            "sql": sql,
            "params": params.into_json(),
        }),
        action: None,
        timeout_ms: None,
    };

    invoke(request)
        .await
        .map_err(|error| database_failure(operation, error))
}

fn database_failure(operation: DatabaseOperation, _: iii_sdk::Error) -> DatabaseError {
    DatabaseError::database_failure(operation)
}

fn memory_insert_parameters(memory: &ValidatedMemoryVersion) -> PositionalParameters {
    PositionalParameters::from(vec![
        json!(memory.id().as_str()),
        json!(memory.version().get().to_string()),
        json!(memory.memory_type()),
        json!(memory.title()),
        json!(memory.content()),
        json!(memory.created_at().to_rfc3339()),
        json!(memory.updated_at().to_rfc3339()),
        json!(memory.concepts()),
        json!(memory.files()),
        json!(memory.session_ids()),
        json!(memory.source_observation_ids()),
    ])
}

fn embedding_insert_parameters(
    embedding: &ValidatedEmbedding,
) -> Result<PositionalParameters, DatabaseError> {
    let operation = DatabaseOperation::InsertEmbedding;
    let vector = pgvector_text(embedding.embedding().as_slice(), operation)?;

    Ok(PositionalParameters::from(vec![
        json!(embedding.id().as_str()),
        json!(embedding.version().get().to_string()),
        json!(vector),
    ]))
}

fn pgvector_text(
    components: &[f64],
    operation: DatabaseOperation,
) -> Result<String, DatabaseError> {
    if components.len() > 16_000 {
        return Err(DatabaseError::database_failure(operation));
    }

    let vector = components
        .iter()
        .copied()
        .map(|component| {
            let narrowed = component as f32;
            if !narrowed.is_finite() || f64::from(narrowed) != component {
                return Err(DatabaseError::database_failure(operation));
            }

            Ok(narrowed)
        })
        .collect::<Result<Vec<_>, _>>()?;

    serde_json::to_string(&vector).map_err(|_| DatabaseError::database_failure(operation))
}

fn bm25_search_parameters(query: &ValidatedBm25Search) -> PositionalParameters {
    PositionalParameters::from(vec![json!(query.query()), json!(query.limit().to_string())])
}

fn vector_search_parameters(
    query: &ValidatedVectorSearch,
) -> Result<PositionalParameters, DatabaseError> {
    let operation = DatabaseOperation::SearchVector;
    let vector = pgvector_text(query.vector(), operation)?;

    Ok(PositionalParameters::from(vec![
        json!(vector),
        json!(query.limit().to_string()),
    ]))
}

async fn search_bm25_with<F, Fut>(
    database: &DatabaseTarget,
    query: &ValidatedBm25Search,
    invoke: F,
) -> Result<Vec<MemorySearchResult>, DatabaseError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    let response = execute(
        database,
        DatabaseOperation::SearchBm25,
        SEARCH_BM25_SQL,
        bm25_search_parameters(query),
        invoke,
    )
    .await?;

    decode_bm25_search_response(query, response)
}

async fn search_vector_with<F, Fut>(
    database: &DatabaseTarget,
    query: &ValidatedVectorSearch,
    invoke: F,
) -> Result<Vec<MemorySearchResult>, DatabaseError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    let response = execute(
        database,
        DatabaseOperation::SearchVector,
        SEARCH_VECTOR_SQL,
        vector_search_parameters(query)?,
        invoke,
    )
    .await?;

    decode_vector_search_response(query, response)
}

fn decode_bm25_search_response(
    query: &ValidatedBm25Search,
    response: Value,
) -> Result<Vec<MemorySearchResult>, DatabaseError> {
    decode_search_response(
        DatabaseOperation::SearchBm25,
        query.limit(),
        response,
        decode_bm25_search_row,
    )
}

fn decode_vector_search_response(
    query: &ValidatedVectorSearch,
    response: Value,
) -> Result<Vec<MemorySearchResult>, DatabaseError> {
    decode_search_response(
        DatabaseOperation::SearchVector,
        query.limit(),
        response,
        decode_vector_search_row,
    )
}

fn decode_search_response(
    operation: DatabaseOperation,
    limit: u32,
    response: Value,
    decode_row: fn(&Value) -> Result<MemorySearchResult, DatabaseError>,
) -> Result<Vec<MemorySearchResult>, DatabaseError> {
    let response = response
        .as_object()
        .ok_or_else(|| DatabaseError::invalid_response(operation, "response"))?;
    let affected_rows = response
        .get("affected_rows")
        .and_then(Value::as_u64)
        .ok_or_else(|| DatabaseError::invalid_response(operation, "affected_rows"))?;
    match response.get("last_insert_id") {
        None => return Err(DatabaseError::invalid_response(operation, "last_insert_id")),
        Some(Value::Null) => {}
        Some(value) if value.is_string() => {
            return Err(DatabaseError::database_failure(operation));
        }
        Some(_) => return Err(DatabaseError::invalid_response(operation, "last_insert_id")),
    }
    let returned_rows = response
        .get("returned_rows")
        .and_then(Value::as_array)
        .ok_or_else(|| DatabaseError::invalid_response(operation, "returned_rows"))?;
    let returned_row_count = u64::try_from(returned_rows.len())
        .map_err(|_| DatabaseError::database_failure(operation))?;

    if affected_rows != returned_row_count || affected_rows > u64::from(limit) {
        return Err(DatabaseError::database_failure(operation));
    }

    returned_rows.iter().map(decode_row).collect()
}

fn decode_bm25_search_row(row: &Value) -> Result<MemorySearchResult, DatabaseError> {
    decode_search_row(DatabaseOperation::SearchBm25, row, true)
}

fn decode_vector_search_row(row: &Value) -> Result<MemorySearchResult, DatabaseError> {
    decode_search_row(DatabaseOperation::SearchVector, row, false)
}

fn decode_search_row(
    operation: DatabaseOperation,
    row: &Value,
    relevance_must_be_positive: bool,
) -> Result<MemorySearchResult, DatabaseError> {
    let row = row
        .as_object()
        .ok_or_else(|| DatabaseError::invalid_response(operation, "returned_rows"))?;
    let id = search_row_string(operation, row, "id")?.to_owned();
    let version = search_row_string(operation, row, "version")?
        .parse::<i64>()
        .map_err(|_| DatabaseError::invalid_response(operation, "version"))?;
    let memory_type = search_row_string(operation, row, "memory_type")?.to_owned();
    let title = search_row_string(operation, row, "title")?.to_owned();
    let content = search_row_string(operation, row, "content")?.to_owned();
    let created_at = search_row_timestamp(operation, row, "created_at")?;
    let updated_at = search_row_timestamp(operation, row, "updated_at")?;
    let concepts = search_row_string_array(operation, row, "concepts")?;
    let files = search_row_string_array(operation, row, "files")?;
    let session_ids = search_row_string_array(operation, row, "session_ids")?;
    let source_observation_ids = search_row_string_array(operation, row, "source_observation_ids")?;
    let relevance = row
        .get("relevance")
        .and_then(Value::as_f64)
        .ok_or_else(|| DatabaseError::invalid_response(operation, "relevance"))?;

    if !relevance.is_finite() || (relevance_must_be_positive && relevance <= 0.0) {
        return Err(DatabaseError::invalid_response(operation, "relevance"));
    }

    let memory = ValidatedMemoryVersion::try_from(MemoryVersionInput {
        id,
        version,
        memory_type,
        title,
        content,
        created_at,
        updated_at,
        concepts,
        files,
        session_ids,
        source_observation_ids,
    })
    .map_err(|error| DatabaseError::invalid_response(operation, error.field()))?;

    MemorySearchResult::try_new(memory, relevance)
        .map_err(|error| DatabaseError::invalid_response(operation, error.field()))
}

fn search_row_string<'a>(
    operation: DatabaseOperation,
    row: &'a Map<String, Value>,
    column: &'static str,
) -> Result<&'a str, DatabaseError> {
    row.get(column)
        .and_then(Value::as_str)
        .ok_or_else(|| DatabaseError::invalid_response(operation, column))
}

fn search_row_timestamp(
    operation: DatabaseOperation,
    row: &Map<String, Value>,
    column: &'static str,
) -> Result<DateTime<Utc>, DatabaseError> {
    DateTime::parse_from_rfc3339(search_row_string(operation, row, column)?)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|_| DatabaseError::invalid_response(operation, column))
}

fn search_row_string_array(
    operation: DatabaseOperation,
    row: &Map<String, Value>,
    column: &'static str,
) -> Result<Vec<String>, DatabaseError> {
    row.get(column)
        .and_then(Value::as_array)
        .ok_or_else(|| DatabaseError::invalid_response(operation, column))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| DatabaseError::invalid_response(operation, column))
        })
        .collect()
}

async fn insert_memory_with<F, Fut>(
    database: &DatabaseTarget,
    memory: &ValidatedMemoryVersion,
    invoke: F,
) -> Result<(), DatabaseError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    let response = execute(
        database,
        DatabaseOperation::InsertMemory,
        INSERT_MEMORY_SQL,
        memory_insert_parameters(memory),
        invoke,
    )
    .await?;

    decode_memory_insert_response(memory, response)
}

fn decode_memory_insert_response(
    memory: &ValidatedMemoryVersion,
    response: Value,
) -> Result<(), DatabaseError> {
    let operation = DatabaseOperation::InsertMemory;
    let response = response
        .as_object()
        .ok_or_else(|| DatabaseError::invalid_response(operation, "response"))?;
    let affected_rows = response
        .get("affected_rows")
        .and_then(Value::as_u64)
        .ok_or_else(|| DatabaseError::invalid_response(operation, "affected_rows"))?;
    let last_insert_id = match response.get("last_insert_id") {
        None => return Err(DatabaseError::invalid_response(operation, "last_insert_id")),
        Some(Value::Null) => None,
        Some(value) => Some(
            value
                .as_str()
                .ok_or_else(|| DatabaseError::invalid_response(operation, "last_insert_id"))?,
        ),
    };
    let returned_rows = response
        .get("returned_rows")
        .and_then(Value::as_array)
        .ok_or_else(|| DatabaseError::invalid_response(operation, "returned_rows"))?;

    if affected_rows == 0 && last_insert_id.is_none() && returned_rows.is_empty() {
        return Err(DatabaseError::conflict(operation));
    }

    if affected_rows != 1 || returned_rows.len() != 1 {
        return Err(DatabaseError::database_failure(operation));
    }

    let returned_row = returned_rows[0]
        .as_object()
        .ok_or_else(|| DatabaseError::invalid_response(operation, "returned_rows"))?;
    let returned_id = returned_row
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| DatabaseError::invalid_response(operation, "id"))?;
    let returned_version = returned_row
        .get("version")
        .and_then(Value::as_str)
        .ok_or_else(|| DatabaseError::invalid_response(operation, "version"))?;
    let last_insert_id =
        last_insert_id.ok_or_else(|| DatabaseError::database_failure(operation))?;

    if returned_id != memory.id().as_str()
        || returned_version != memory.version().get().to_string()
        || last_insert_id != memory.id().as_str()
    {
        return Err(DatabaseError::database_failure(operation));
    }

    Ok(())
}

async fn insert_embedding_with<F, Fut>(
    database: &DatabaseTarget,
    embedding: &ValidatedEmbedding,
    invoke: F,
) -> Result<(), DatabaseError>
where
    F: FnOnce(TriggerRequest) -> Fut,
    Fut: Future<Output = Result<Value, iii_sdk::Error>>,
{
    let response = execute(
        database,
        DatabaseOperation::InsertEmbedding,
        INSERT_EMBEDDING_SQL,
        embedding_insert_parameters(embedding)?,
        invoke,
    )
    .await?;

    decode_embedding_insert_response(embedding, response)
}

fn decode_embedding_insert_response(
    embedding: &ValidatedEmbedding,
    response: Value,
) -> Result<(), DatabaseError> {
    let operation = DatabaseOperation::InsertEmbedding;
    let response = response
        .as_object()
        .ok_or_else(|| DatabaseError::invalid_response(operation, "response"))?;
    let affected_rows = response
        .get("affected_rows")
        .and_then(Value::as_u64)
        .ok_or_else(|| DatabaseError::invalid_response(operation, "affected_rows"))?;
    let has_last_insert_id = match response.get("last_insert_id") {
        None => return Err(DatabaseError::invalid_response(operation, "last_insert_id")),
        Some(Value::Null) => false,
        Some(value) => {
            value
                .as_str()
                .ok_or_else(|| DatabaseError::invalid_response(operation, "last_insert_id"))?;
            true
        }
    };
    let returned_rows = response
        .get("returned_rows")
        .and_then(Value::as_array)
        .ok_or_else(|| DatabaseError::invalid_response(operation, "returned_rows"))?;

    if affected_rows != 1 || has_last_insert_id || returned_rows.len() != 1 {
        return Err(DatabaseError::database_failure(operation));
    }

    let returned_row = returned_rows[0]
        .as_object()
        .ok_or_else(|| DatabaseError::invalid_response(operation, "returned_rows"))?;
    let outcome = returned_row
        .get("outcome")
        .and_then(Value::as_str)
        .ok_or_else(|| DatabaseError::invalid_response(operation, "outcome"))?;
    let id = returned_row
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| DatabaseError::invalid_response(operation, "id"))?;
    let version = returned_row
        .get("version")
        .and_then(Value::as_str)
        .ok_or_else(|| DatabaseError::invalid_response(operation, "version"))?;

    if id != embedding.id().as_str() || version != embedding.version().get().to_string() {
        return Err(DatabaseError::database_failure(operation));
    }

    match outcome {
        "inserted" => Ok(()),
        "conflict" => Err(DatabaseError::conflict(operation)),
        "missing" => Err(DatabaseError::missing_memory_version(
            MemoryVersionKey::new(embedding.id().clone(), embedding.version()),
        )),
        _ => Err(DatabaseError::database_failure(operation)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use chrono::{TimeZone, Timelike, Utc};
    use iii_sdk::Error as IiiError;
    use serde_json::{Value, json};

    use super::*;

    const EXPECTED_INSERT_MEMORY_SQL: &str = r#"INSERT INTO public.memories (
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
    const EXPECTED_INSERT_EMBEDDING_SQL: &str = r#"WITH parent AS MATERIALIZED (
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
    const EXPECTED_SEARCH_BM25_SQL: &str = r#"WITH scored_heads AS MATERIALIZED (
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
    const EXPECTED_SEARCH_VECTOR_SQL: &str = r#"WITH query_input AS MATERIALIZED (
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

    fn database_target() -> DatabaseTarget {
        DatabaseTarget::try_from("memory-store-test".to_owned())
            .expect("database target should be valid")
    }

    fn memory() -> ValidatedMemoryVersion {
        let timestamp = Utc
            .with_ymd_and_hms(2026, 9, 19, 12, 34, 56)
            .single()
            .expect("test timestamp should be valid");

        ValidatedMemoryVersion::try_from(crate::contracts::MemoryVersionInput {
            id: "memory-id-sentinel".to_owned(),
            version: 7,
            memory_type: "type-sentinel".to_owned(),
            title: "title-sentinel".to_owned(),
            content: "content-sentinel".to_owned(),
            created_at: timestamp,
            updated_at: timestamp,
            concepts: vec![
                "concept-first".to_owned(),
                String::new(),
                "concept-first".to_owned(),
            ],
            files: vec![
                "file-last".to_owned(),
                "file-first".to_owned(),
                "file-last".to_owned(),
            ],
            session_ids: vec![
                "session-first".to_owned(),
                String::new(),
                "session-first".to_owned(),
            ],
            source_observation_ids: vec![
                "source-last".to_owned(),
                "source-first".to_owned(),
                "source-last".to_owned(),
            ],
        })
        .expect("test memory should be valid")
    }

    fn embedding(vector: Vec<f64>) -> ValidatedEmbedding {
        ValidatedEmbedding::try_from(crate::contracts::EmbeddingInput {
            id: "memory-id-sentinel".to_owned(),
            version: 7,
            embedding: vector,
        })
        .expect("test embedding should be valid")
    }

    fn bm25_search(query: &str, limit: u32) -> ValidatedBm25Search {
        ValidatedBm25Search::try_from(crate::contracts::Bm25Search {
            query: query.to_owned(),
            limit,
        })
        .expect("test BM25 search should be valid")
    }

    fn vector_search(vector: Vec<f64>, limit: u32) -> ValidatedVectorSearch {
        ValidatedVectorSearch::try_from(crate::contracts::VectorSearch { vector, limit })
            .expect("test vector search should be valid")
    }

    fn successful_insert_response(memory: &ValidatedMemoryVersion) -> Value {
        json!({
            "affected_rows": 1,
            "last_insert_id": memory.id().as_str(),
            "returned_rows": [{
                "id": memory.id().as_str(),
                "version": memory.version().get().to_string(),
            }],
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

    fn bm25_row(id: &str, version: &str, relevance: Value) -> Value {
        json!({
            "id": id,
            "version": version,
            "memory_type": "type-sentinel",
            "title": "title-sentinel",
            "content": "content-sentinel",
            "created_at": "2026-09-19T12:34:56+00:00",
            "updated_at": "2026-09-19T12:34:56+00:00",
            "concepts": ["concept-last", "", "concept-last"],
            "files": ["file-last", "file-first", "file-last"],
            "session_ids": ["session-last", "", "session-last"],
            "source_observation_ids": ["source-last", "source-first", "source-last"],
            "relevance": relevance,
        })
    }

    fn bm25_response(rows: Vec<Value>) -> Value {
        json!({
            "affected_rows": rows.len(),
            "last_insert_id": null,
            "returned_rows": rows,
        })
    }

    fn without_column(mut row: Value, column: &str) -> Value {
        row.as_object_mut()
            .expect("test row should be an object")
            .remove(column);
        row
    }

    async fn insert_with_response(response: Value) -> (Result<(), DatabaseError>, usize) {
        let database = database_target();
        let memory = memory();
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocation_counter = Arc::clone(&invocations);
        let result = insert_memory_with(&database, &memory, move |_| {
            let invocation_counter = Arc::clone(&invocation_counter);
            async move {
                invocation_counter.fetch_add(1, Ordering::SeqCst);
                Ok(response)
            }
        })
        .await;

        (result, invocations.load(Ordering::SeqCst))
    }

    async fn insert_embedding_with_response(
        embedding: &ValidatedEmbedding,
        response: Value,
    ) -> (Result<(), DatabaseError>, usize) {
        let database = database_target();
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocation_counter = Arc::clone(&invocations);
        let result = insert_embedding_with(&database, embedding, move |_| {
            let invocation_counter = Arc::clone(&invocation_counter);
            async move {
                invocation_counter.fetch_add(1, Ordering::SeqCst);
                Ok(response)
            }
        })
        .await;

        (result, invocations.load(Ordering::SeqCst))
    }

    async fn search_bm25_with_response(
        query: &ValidatedBm25Search,
        response: Value,
    ) -> (Result<Vec<MemorySearchResult>, DatabaseError>, usize) {
        let database = database_target();
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocation_counter = Arc::clone(&invocations);
        let result = search_bm25_with(&database, query, move |_| {
            let invocation_counter = Arc::clone(&invocation_counter);
            async move {
                invocation_counter.fetch_add(1, Ordering::SeqCst);
                Ok(response)
            }
        })
        .await;

        (result, invocations.load(Ordering::SeqCst))
    }

    async fn search_vector_with_response(
        query: &ValidatedVectorSearch,
        response: Value,
    ) -> (Result<Vec<MemorySearchResult>, DatabaseError>, usize) {
        let database = database_target();
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocation_counter = Arc::clone(&invocations);
        let result = search_vector_with(&database, query, move |_| {
            let invocation_counter = Arc::clone(&invocation_counter);
            async move {
                invocation_counter.fetch_add(1, Ordering::SeqCst);
                Ok(response)
            }
        })
        .await;

        (result, invocations.load(Ordering::SeqCst))
    }

    fn assert_opaque(error: &DatabaseError, sentinels: &[&str]) {
        let display = error.to_string();
        let debug = format!("{error:?}");

        for sentinel in sentinels {
            assert!(
                !display.contains(sentinel),
                "Display error leaked protected value: {display}"
            );
            assert!(
                !debug.contains(sentinel),
                "Debug error leaked protected value: {debug}"
            );
        }
    }

    #[test]
    fn positional_parameters_are_constructed_from_values_vectors() {
        let construct: fn(Vec<Value>) -> PositionalParameters = PositionalParameters::from;
        let parameters = construct(vec![
            json!("parameter-sentinel"),
            json!({"field": "value-sentinel"}),
            Value::Null,
        ]);
        let serialized = parameters.into_json();

        assert!(serialized.is_array());
        assert_eq!(
            serialized,
            json!([
                "parameter-sentinel",
                {"field": "value-sentinel"},
                null,
            ])
        );
    }

    #[tokio::test]
    async fn execute_builds_the_database_worker_envelope() {
        let database = DatabaseTarget::try_from("memory-store-test".to_owned())
            .expect("database target should be valid");
        let params = PositionalParameters::from(vec![json!("parameter-sentinel")]);

        let response = execute(
            &database,
            DatabaseOperation::SearchBm25,
            "SELECT $1",
            params,
            move |request| async move {
                assert_eq!(request.function_id, DATABASE_EXECUTE_FUNCTION_ID);
                assert_eq!(
                    request.payload,
                    json!({
                        "db": "memory-store-test",
                        "sql": "SELECT $1",
                        "params": ["parameter-sentinel"],
                    })
                );
                assert!(request.payload["params"].is_array());
                assert!(request.action.is_none());
                assert_eq!(request.timeout_ms, None);
                Ok(json!({"rows": []}))
            },
        )
        .await
        .expect("successful database invocation should return its full response");

        assert_eq!(response, json!({"rows": []}));
    }

    #[tokio::test]
    async fn execute_maps_sdk_errors_to_opaque_database_failures() {
        const SQL_SENTINEL: &str = "sql-secret-sentinel";
        const PARAMETER_SENTINEL: &str = "parameter-secret-sentinel";
        const SDK_CODE_SENTINEL: &str = "sdk-code-secret-sentinel";
        const SDK_MESSAGE_SENTINEL: &str = "sdk-message-secret-sentinel";

        let database = DatabaseTarget::try_from("memory-store-test".to_owned())
            .expect("database target should be valid");
        let error = execute(
            &database,
            DatabaseOperation::SearchVector,
            SQL_SENTINEL,
            PositionalParameters::from(vec![json!(PARAMETER_SENTINEL)]),
            |_| async {
                Err(IiiError::Remote {
                    code: SDK_CODE_SENTINEL.to_owned(),
                    message: SDK_MESSAGE_SENTINEL.to_owned(),
                    stacktrace: None,
                })
            },
        )
        .await
        .expect_err("SDK errors should become database failures");

        assert!(matches!(
            error,
            DatabaseError::DatabaseFailure {
                operation: DatabaseOperation::SearchVector
            }
        ));
        let display = error.to_string();
        let debug = format!("{error:?}");
        for sentinel in [
            SQL_SENTINEL,
            PARAMETER_SENTINEL,
            SDK_CODE_SENTINEL,
            SDK_MESSAGE_SENTINEL,
        ] {
            assert!(
                !display.contains(sentinel),
                "Display error leaked protected value: {display}"
            );
            assert!(
                !debug.contains(sentinel),
                "Debug error leaked protected value: {debug}"
            );
        }
    }

    #[tokio::test]
    async fn search_bm25_uses_exact_current_head_request_and_decodes_complete_ordered_rows() {
        let database = database_target();
        let query = bm25_search("query-sentinel", 2);
        let mut first = bm25_row("id-first", "7", json!(2.5));
        first["memory_type"] = json!("type-first");
        first["title"] = json!("title-first");
        first["content"] = json!("content-first");
        first["created_at"] = json!("2026-09-19T12:34:56+00:00");
        first["updated_at"] = json!("2026-09-19T12:34:57+00:00");
        let mut second = bm25_row("id-second", "8", json!(1.25));
        second["memory_type"] = json!("type-second");
        second["title"] = json!("title-second");
        second["content"] = json!("content-second");
        let expected_response = bm25_response(vec![first, second]);

        let results = search_bm25_with(&database, &query, move |request| async move {
            assert_eq!(request.function_id, DATABASE_EXECUTE_FUNCTION_ID);
            assert_eq!(
                request.payload,
                json!({
                    "db": "memory-store-test",
                    "sql": EXPECTED_SEARCH_BM25_SQL,
                    "params": ["query-sentinel", "2"],
                })
            );
            assert!(request.action.is_none());
            assert_eq!(request.timeout_ms, None);
            let request_text = request.payload.to_string().to_ascii_lowercase();
            assert!(!request_text.contains("embedding"));
            assert!(!request_text.contains("vector"));
            assert!(!request_text.contains("phrase"));
            assert!(!request_text.contains("weight"));
            Ok(expected_response)
        })
        .await
        .expect("complete ordered BM25 rows should decode");

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id().as_str(), "id-first");
        assert_eq!(results[0].version().get(), 7);
        assert_eq!(results[0].memory_type(), "type-first");
        assert_eq!(results[0].title(), "title-first");
        assert_eq!(results[0].content(), "content-first");
        assert_eq!(
            results[0].created_at().to_rfc3339(),
            "2026-09-19T12:34:56+00:00"
        );
        assert_eq!(
            results[0].updated_at().to_rfc3339(),
            "2026-09-19T12:34:57+00:00"
        );
        assert_eq!(results[0].concepts(), ["concept-last", "", "concept-last"]);
        assert_eq!(results[0].files(), ["file-last", "file-first", "file-last"]);
        assert_eq!(
            results[0].session_ids(),
            ["session-last", "", "session-last"]
        );
        assert_eq!(
            results[0].source_observation_ids(),
            ["source-last", "source-first", "source-last"]
        );
        assert_eq!(results[0].relevance(), 2.5);
        assert_eq!(results[1].id().as_str(), "id-second");
        assert_eq!(results[1].version().get(), 8);
        assert_eq!(results[1].memory_type(), "type-second");
        assert_eq!(results[1].title(), "title-second");
        assert_eq!(results[1].content(), "content-second");
        assert_eq!(results[1].relevance(), 1.25);

        let serialized = serde_json::to_value(&results).expect("results should serialize");
        let rows = serialized
            .as_array()
            .expect("serialized results should be an array");
        assert!(rows.iter().all(|row| row.get("embedding").is_none()));
    }

    #[tokio::test]
    async fn search_bm25_accepts_an_empty_result_set() {
        let query = bm25_search("query-sentinel", 1);
        let (result, invocations) =
            search_bm25_with_response(&query, bm25_response(Vec::new())).await;

        assert_eq!(invocations, 1);
        assert_eq!(
            result.expect("empty BM25 result sets should decode"),
            Vec::<MemorySearchResult>::new()
        );
    }

    #[tokio::test]
    async fn search_bm25_maps_remote_errors_to_opaque_database_failures() {
        const QUERY_SENTINEL: &str = "query-secret-sentinel";
        const SDK_CODE_SENTINEL: &str = "sdk-code-secret-sentinel";
        const SDK_MESSAGE_SENTINEL: &str = "sdk-message-secret-sentinel";

        let database = database_target();
        let query = bm25_search(QUERY_SENTINEL, 1);
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocation_counter = Arc::clone(&invocations);
        let error = search_bm25_with(&database, &query, move |_| {
            let invocation_counter = Arc::clone(&invocation_counter);
            async move {
                invocation_counter.fetch_add(1, Ordering::SeqCst);
                Err(IiiError::Remote {
                    code: SDK_CODE_SENTINEL.to_owned(),
                    message: SDK_MESSAGE_SENTINEL.to_owned(),
                    stacktrace: None,
                })
            }
        })
        .await
        .expect_err("remote BM25 failures should be opaque");

        assert_eq!(invocations.load(Ordering::SeqCst), 1);
        assert_eq!(
            error,
            DatabaseError::database_failure(DatabaseOperation::SearchBm25)
        );
        assert_opaque(
            &error,
            &[QUERY_SENTINEL, SDK_CODE_SENTINEL, SDK_MESSAGE_SENTINEL],
        );
    }

    #[tokio::test]
    async fn search_bm25_rejects_malformed_envelopes_with_static_columns() {
        let query = bm25_search("query-sentinel", 1);
        let row = bm25_row("memory-id-sentinel", "7", json!(1.0));
        for (response, column) in [
            (Value::Null, "response"),
            (
                json!({
                    "last_insert_id": null,
                    "returned_rows": [row.clone()],
                }),
                "affected_rows",
            ),
            (
                json!({
                    "affected_rows": "1",
                    "last_insert_id": null,
                    "returned_rows": [row.clone()],
                }),
                "affected_rows",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "returned_rows": [row.clone()],
                }),
                "last_insert_id",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": 7,
                    "returned_rows": [row.clone()],
                }),
                "last_insert_id",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": false,
                    "returned_rows": [row.clone()],
                }),
                "last_insert_id",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": [],
                    "returned_rows": [row.clone()],
                }),
                "last_insert_id",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": {},
                    "returned_rows": [row.clone()],
                }),
                "last_insert_id",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": null,
                }),
                "returned_rows",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": null,
                    "returned_rows": {},
                }),
                "returned_rows",
            ),
        ] {
            let (result, invocations) = search_bm25_with_response(&query, response).await;

            assert_eq!(invocations, 1);
            let error = result.expect_err("malformed BM25 envelopes should fail");
            assert_eq!(
                error,
                DatabaseError::invalid_response(DatabaseOperation::SearchBm25, column)
            );
            assert_opaque(&error, &["memory-id-sentinel", "content-sentinel"]);
        }
    }

    #[tokio::test]
    async fn search_bm25_rejects_malformed_rows_with_static_columns() {
        let query = bm25_search("query-sentinel", 1);
        let row = bm25_row("memory-id-sentinel", "7", json!(1.0));
        let mut numeric_version = row.clone();
        numeric_version["version"] = json!(7);
        let mut numeric_created_at = row.clone();
        numeric_created_at["created_at"] = json!(7);
        let mut malformed_updated_at = row.clone();
        malformed_updated_at["updated_at"] = json!("not-rfc3339");
        let mut object_concepts = row.clone();
        object_concepts["concepts"] = json!({"not": "an array"});
        let mut numeric_files = row.clone();
        numeric_files["files"] = json!(["file-sentinel", 7]);
        let mut null_session_ids = row.clone();
        null_session_ids["session_ids"] = Value::Null;
        let mut object_source_observation_ids = row.clone();
        object_source_observation_ids["source_observation_ids"] = json!({});
        let mut text_relevance = row.clone();
        text_relevance["relevance"] = json!("1.0");
        let mut null_relevance = row.clone();
        null_relevance["relevance"] = Value::Null;
        for (row, column) in [
            (Value::Null, "returned_rows"),
            (without_column(row.clone(), "id"), "id"),
            (numeric_version, "version"),
            (without_column(row.clone(), "memory_type"), "memory_type"),
            (without_column(row.clone(), "title"), "title"),
            (without_column(row.clone(), "content"), "content"),
            (numeric_created_at, "created_at"),
            (malformed_updated_at, "updated_at"),
            (object_concepts, "concepts"),
            (numeric_files, "files"),
            (null_session_ids, "session_ids"),
            (object_source_observation_ids, "source_observation_ids"),
            (text_relevance, "relevance"),
            (null_relevance, "relevance"),
        ] {
            let (result, invocations) =
                search_bm25_with_response(&query, bm25_response(vec![row])).await;

            assert_eq!(invocations, 1);
            let error = result.expect_err("malformed BM25 rows should fail");
            assert_eq!(
                error,
                DatabaseError::invalid_response(DatabaseOperation::SearchBm25, column)
            );
            assert_opaque(&error, &["memory-id-sentinel", "content-sentinel"]);
        }
    }

    #[tokio::test]
    async fn search_bm25_maps_contract_invalidity_to_static_columns() {
        let query = bm25_search("query-sentinel", 1);
        let row = bm25_row("memory-id-sentinel", "7", json!(1.0));
        let mut zero_version = row.clone();
        zero_version["version"] = json!("0");
        let mut empty_memory_type = row.clone();
        empty_memory_type["memory_type"] = json!("");
        let mut empty_title = row.clone();
        empty_title["title"] = json!("");
        let mut empty_content = row.clone();
        empty_content["content"] = json!("");
        let mut inverted_timestamps = row.clone();
        inverted_timestamps["updated_at"] = json!("2026-09-19T12:34:55+00:00");
        for (row, column) in [
            (
                {
                    let mut empty_id = row.clone();
                    empty_id["id"] = json!("");
                    empty_id
                },
                "id",
            ),
            (zero_version, "version"),
            (empty_memory_type, "memory_type"),
            (empty_title, "title"),
            (empty_content, "content"),
            (inverted_timestamps, "updated_at"),
        ] {
            let (result, invocations) =
                search_bm25_with_response(&query, bm25_response(vec![row])).await;

            assert_eq!(invocations, 1);
            let error = result.expect_err("contract-invalid BM25 rows should fail");
            assert_eq!(
                error,
                DatabaseError::invalid_response(DatabaseOperation::SearchBm25, column)
            );
            assert_opaque(&error, &["memory-id-sentinel", "content-sentinel"]);
        }
    }

    #[tokio::test]
    async fn search_bm25_rejects_semantic_envelope_and_relevance_failures() {
        let query = bm25_search("query-sentinel", 1);
        let row = bm25_row("memory-id-sentinel", "7", json!(1.0));
        for response in [
            json!({
                "affected_rows": 1,
                "last_insert_id": "unexpected-last-id",
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
                "returned_rows": [],
            }),
            json!({
                "affected_rows": 2,
                "last_insert_id": null,
                "returned_rows": [row.clone(), row.clone()],
            }),
        ] {
            let (result, invocations) = search_bm25_with_response(&query, response).await;

            assert_eq!(invocations, 1);
            let error = result.expect_err("semantic BM25 envelope failures should fail");
            assert_eq!(
                error,
                DatabaseError::database_failure(DatabaseOperation::SearchBm25)
            );
            assert_opaque(&error, &["memory-id-sentinel", "content-sentinel"]);
        }

        for relevance in [json!(0.0), json!(-0.25)] {
            let (result, invocations) = search_bm25_with_response(
                &query,
                bm25_response(vec![bm25_row("memory-id-sentinel", "7", relevance)]),
            )
            .await;

            assert_eq!(invocations, 1);
            let error = result.expect_err("nonpositive BM25 relevance should fail");
            assert_eq!(
                error,
                DatabaseError::invalid_response(DatabaseOperation::SearchBm25, "relevance")
            );
            assert_opaque(&error, &["memory-id-sentinel", "content-sentinel"]);
        }
    }

    #[tokio::test]
    async fn search_bm25_returns_no_partial_results_when_a_later_row_is_malformed() {
        let query = bm25_search("query-sentinel", 2);
        let mut malformed_second_row = bm25_row("memory-id-second", "8", json!(1.0));
        malformed_second_row["content"] = Value::Null;
        let response = bm25_response(vec![
            bm25_row("memory-id-first", "7", json!(2.0)),
            malformed_second_row,
        ]);

        let (result, invocations) = search_bm25_with_response(&query, response).await;

        assert_eq!(invocations, 1);
        let error =
            result.expect_err("one malformed later row should reject the entire result set");
        assert_eq!(
            error,
            DatabaseError::invalid_response(DatabaseOperation::SearchBm25, "content")
        );
        assert_opaque(
            &error,
            &["memory-id-first", "memory-id-second", "content-sentinel"],
        );
    }

    #[tokio::test]
    async fn search_vector_uses_the_exact_guarded_current_head_request_and_embedding_free_results()
    {
        let database = database_target();
        let query = vector_search(vec![1.25, -2.5], 2);
        let mut first = bm25_row("id-first", "7", json!(1.0));
        first["memory_type"] = json!("type-first");
        first["title"] = json!("title-first");
        first["content"] = json!("content-first");
        let mut second = bm25_row("id-second", "8", json!(-1.0));
        second["memory_type"] = json!("type-second");
        second["title"] = json!("title-second");
        second["content"] = json!("content-second");
        let expected_response = bm25_response(vec![first, second]);

        let results = search_vector_with(&database, &query, move |request| async move {
            assert_eq!(request.function_id, DATABASE_EXECUTE_FUNCTION_ID);
            assert_eq!(
                request.payload,
                json!({
                    "db": "memory-store-test",
                    "sql": EXPECTED_SEARCH_VECTOR_SQL,
                    "params": ["[1.25,-2.5]", "2"],
                })
            );
            assert!(request.action.is_none());
            assert_eq!(request.timeout_ms, None);

            let sql = request.payload["sql"]
                .as_str()
                .expect("vector SQL should be a string");
            let case_start = sql.find("CASE").expect("vector relevance should use CASE");
            let case_end = sql
                .find("END AS relevance")
                .expect("vector relevance CASE should end");
            let cosine_operator = sql
                .find("<=>")
                .expect("vector SQL should use cosine distance");
            assert!(cosine_operator > case_start && cosine_operator < case_end);
            let where_clause = sql
                .split_once("WHERE relevance IS NOT NULL")
                .expect("vector SQL should exclude unscored rows")
                .1;
            assert!(!where_clause.contains("<=>"));
            assert!(sql.contains("FROM public.memory_search_heads AS head"));
            assert!(sql.contains("JOIN public.memory_embeddings AS embedding"));
            assert!(sql.contains("ON embedding.id = head.id AND embedding.version = head.version"));
            assert!(!sql.contains("LEFT JOIN public.memory_embeddings"));
            assert!(!sql.contains("ivfflat"));
            assert!(!sql.contains("hnsw"));
            assert!(sql.contains("id COLLATE \"C\" ASC"));
            assert!(sql.contains("scored_heads.version ASC"));

            let projection = sql
                .rsplit_once("\nSELECT\n")
                .expect("vector SQL should have a final SELECT")
                .1
                .split_once("\nFROM scored_heads")
                .expect("vector SQL should select from scored heads")
                .0;
            assert!(!projection.contains("embedding"));
            assert!(!projection.contains("vector"));
            Ok(expected_response)
        })
        .await
        .expect("complete ordered vector rows should decode");

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id().as_str(), "id-first");
        assert_eq!(results[0].version().get(), 7);
        assert_eq!(results[0].memory_type(), "type-first");
        assert_eq!(results[0].title(), "title-first");
        assert_eq!(results[0].content(), "content-first");
        assert_eq!(results[0].relevance(), 1.0);
        assert_eq!(results[1].id().as_str(), "id-second");
        assert_eq!(results[1].version().get(), 8);
        assert_eq!(results[1].memory_type(), "type-second");
        assert_eq!(results[1].title(), "title-second");
        assert_eq!(results[1].content(), "content-second");
        assert_eq!(results[1].relevance(), -1.0);

        let serialized = serde_json::to_value(&results).expect("results should serialize");
        let rows = serialized
            .as_array()
            .expect("serialized results should be an array");
        assert!(rows.iter().all(|row| row.get("embedding").is_none()));
    }

    #[tokio::test]
    async fn search_vector_reuses_embedding_physical_encoding() {
        let database = database_target();
        let embedding = embedding(vec![1.25, -2.5]);
        let query = vector_search(vec![1.25, -2.5], 1);
        let insert_parameters = embedding_insert_parameters(&embedding)
            .expect("the embedding should be pgvector compatible")
            .into_json();
        let expected_vector = insert_parameters[2].clone();
        let expected_response = bm25_response(Vec::new());

        search_vector_with(&database, &query, move |request| async move {
            assert_eq!(request.payload["params"][0], expected_vector);
            assert_eq!(request.payload["params"][1], json!("1"));
            assert!(request.payload["params"][0].is_string());
            Ok(expected_response)
        })
        .await
        .expect("the query should reuse the embedding's physical vector encoding");
    }

    #[tokio::test]
    async fn search_vector_accepts_positive_zero_and_negative_finite_relevance() {
        let query = vector_search(vec![1.0], 1);

        for (relevance, expected_relevance) in
            [(json!(1.0), 1.0), (json!(0.0), 0.0), (json!(-1.0), -1.0)]
        {
            let (result, invocations) = search_vector_with_response(
                &query,
                bm25_response(vec![bm25_row("memory-id-sentinel", "7", relevance)]),
            )
            .await;

            assert_eq!(invocations, 1);
            let result = result.expect("finite vector relevance should decode");
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].relevance(), expected_relevance);
        }
    }

    #[tokio::test]
    async fn search_vector_accepts_empty_results_and_rejects_malformed_envelopes() {
        let query = vector_search(vec![1.0], 1);
        let (empty_result, empty_invocations) =
            search_vector_with_response(&query, bm25_response(Vec::new())).await;
        assert_eq!(empty_invocations, 1);
        assert_eq!(
            empty_result.expect("empty vector result sets should decode"),
            Vec::<MemorySearchResult>::new()
        );

        let row = bm25_row("memory-id-sentinel", "7", json!(1.0));
        let mut text_relevance = row.clone();
        text_relevance["relevance"] = json!("1.0");
        for (response, column) in [
            (Value::Null, "response"),
            (
                json!({
                    "last_insert_id": null,
                    "returned_rows": [row.clone()],
                }),
                "affected_rows",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "returned_rows": [row.clone()],
                }),
                "last_insert_id",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": null,
                    "returned_rows": {},
                }),
                "returned_rows",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": null,
                    "returned_rows": [without_column(row.clone(), "content")],
                }),
                "content",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": null,
                    "returned_rows": [text_relevance],
                }),
                "relevance",
            ),
        ] {
            let (result, invocations) = search_vector_with_response(&query, response).await;

            assert_eq!(invocations, 1);
            let error = result.expect_err("malformed vector responses should fail");
            assert_eq!(
                error,
                DatabaseError::invalid_response(DatabaseOperation::SearchVector, column)
            );
            assert_opaque(&error, &["memory-id-sentinel", "content-sentinel"]);
        }
    }

    #[tokio::test]
    async fn search_vector_returns_no_partial_results_when_a_later_row_is_malformed() {
        let query = vector_search(vec![1.0], 2);
        let mut malformed_second_row = bm25_row("memory-id-second", "8", json!(0.0));
        malformed_second_row["content"] = Value::Null;
        let response = bm25_response(vec![
            bm25_row("memory-id-first", "7", json!(1.0)),
            malformed_second_row,
        ]);

        let (result, invocations) = search_vector_with_response(&query, response).await;

        assert_eq!(invocations, 1);
        let error = result.expect_err("one malformed later row should reject all vector results");
        assert_eq!(
            error,
            DatabaseError::invalid_response(DatabaseOperation::SearchVector, "content")
        );
        assert_opaque(
            &error,
            &["memory-id-first", "memory-id-second", "content-sentinel"],
        );
    }

    #[tokio::test]
    async fn search_vector_maps_remote_errors_to_opaque_database_failures() {
        const VECTOR_SENTINEL: &str = "987654.25";
        const SDK_CODE_SENTINEL: &str = "sdk-code-secret-sentinel";
        const SDK_MESSAGE_SENTINEL: &str = "sdk-message-secret-sentinel";

        let database = database_target();
        let query = vector_search(vec![987_654.25], 1);
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocation_counter = Arc::clone(&invocations);
        let error = search_vector_with(&database, &query, move |_| {
            let invocation_counter = Arc::clone(&invocation_counter);
            async move {
                invocation_counter.fetch_add(1, Ordering::SeqCst);
                Err(IiiError::Remote {
                    code: SDK_CODE_SENTINEL.to_owned(),
                    message: SDK_MESSAGE_SENTINEL.to_owned(),
                    stacktrace: None,
                })
            }
        })
        .await
        .expect_err("remote vector failures should be opaque");

        assert_eq!(invocations.load(Ordering::SeqCst), 1);
        assert_eq!(
            error,
            DatabaseError::database_failure(DatabaseOperation::SearchVector)
        );
        assert_opaque(
            &error,
            &[VECTOR_SENTINEL, SDK_CODE_SENTINEL, SDK_MESSAGE_SENTINEL],
        );
    }

    #[test]
    fn pgvector_text_defensively_rejects_incompatible_vectors() {
        for vector in [
            vec![f64::MAX],
            vec![1.0, f64::MIN_POSITIVE],
            vec![1.000_000_000_000_000_2],
            vec![1.0; 16_001],
        ] {
            let error = pgvector_text(&vector, DatabaseOperation::SearchVector)
                .expect_err("pgvector-incompatible vectors should fail");

            assert_eq!(
                error,
                DatabaseError::database_failure(DatabaseOperation::SearchVector)
            );
            assert_opaque(&error, &["1.0", "340282", "2.225", "16001"]);
        }
    }

    #[tokio::test]
    async fn insert_memory_uses_exact_embedding_free_envelope_and_confirms_response() {
        let database = database_target();
        let memory = memory();
        let expected_response = successful_insert_response(&memory);
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocation_counter = Arc::clone(&invocations);

        insert_memory_with(&database, &memory, move |request| {
            let invocation_counter = Arc::clone(&invocation_counter);
            async move {
                invocation_counter.fetch_add(1, Ordering::SeqCst);
                assert_eq!(request.function_id, DATABASE_EXECUTE_FUNCTION_ID);
                assert_eq!(request.payload["db"], "memory-store-test");
                assert_eq!(
                    request.payload["sql"].as_str(),
                    Some(EXPECTED_INSERT_MEMORY_SQL)
                );
                assert_eq!(
                    request.payload["params"],
                    json!([
                        "memory-id-sentinel",
                        "7",
                        "type-sentinel",
                        "title-sentinel",
                        "content-sentinel",
                        "2026-09-19T12:34:56+00:00",
                        "2026-09-19T12:34:56+00:00",
                        ["concept-first", "", "concept-first"],
                        ["file-last", "file-first", "file-last"],
                        ["session-first", "", "session-first"],
                        ["source-last", "source-first", "source-last"],
                    ])
                );
                let parameters = request.payload["params"]
                    .as_array()
                    .expect("memory parameters should be positional");
                assert_eq!(parameters.len(), 11);
                let request_text = request.payload.to_string().to_ascii_lowercase();
                assert!(!request_text.contains("vector"));
                assert!(!request_text.contains("embedding"));
                assert!(request.action.is_none());
                assert_eq!(request.timeout_ms, None);
                Ok(expected_response)
            }
        })
        .await
        .expect("one confirmed memory insert should succeed");

        assert_eq!(invocations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn insert_memory_preserves_microsecond_timestamps_in_database_parameters() {
        let created_at = Utc
            .with_ymd_and_hms(2026, 9, 19, 12, 34, 56)
            .single()
            .expect("test timestamp should be valid")
            .with_nanosecond(123_000_000)
            .expect("test timestamp precision should be valid");
        let updated_at = Utc
            .with_ymd_and_hms(2026, 9, 19, 12, 34, 57)
            .single()
            .expect("test timestamp should be valid")
            .with_nanosecond(654_321_000)
            .expect("test timestamp precision should be valid");
        let memory = ValidatedMemoryVersion::try_from(crate::contracts::MemoryVersionInput {
            id: "memory-id-sentinel".to_owned(),
            version: 7,
            memory_type: "type-sentinel".to_owned(),
            title: "title-sentinel".to_owned(),
            content: "content-sentinel".to_owned(),
            created_at,
            updated_at,
            concepts: vec!["concept".to_owned()],
            files: vec!["file".to_owned()],
            session_ids: vec!["session".to_owned()],
            source_observation_ids: vec!["source".to_owned()],
        })
        .expect("microsecond memory timestamps should validate");
        let database = database_target();
        let expected_response = successful_insert_response(&memory);

        insert_memory_with(&database, &memory, move |request| async move {
            assert_eq!(request.payload["params"][5], json!(created_at.to_rfc3339()));
            assert_eq!(request.payload["params"][6], json!(updated_at.to_rfc3339()));

            for (index, expected) in [(5, created_at), (6, updated_at)] {
                let serialized = request.payload["params"][index]
                    .as_str()
                    .expect("timestamp parameter should be RFC3339 text");
                let parsed = chrono::DateTime::parse_from_rfc3339(serialized)
                    .expect("timestamp parameter should parse as RFC3339")
                    .with_timezone(&Utc);
                assert_eq!(parsed, expected, "timestamp parameter {index} changed");
            }

            Ok(expected_response)
        })
        .await
        .expect("one confirmed microsecond memory insert should succeed");
    }

    #[tokio::test]
    async fn insert_memory_maps_only_the_exact_duplicate_response_to_conflict() {
        let (result, invocations) = insert_with_response(json!({
            "affected_rows": 0,
            "last_insert_id": null,
            "returned_rows": [],
        }))
        .await;

        assert_eq!(invocations, 1);
        assert_eq!(
            result,
            Err(DatabaseError::conflict(DatabaseOperation::InsertMemory))
        );
    }

    #[tokio::test]
    async fn insert_memory_rejects_a_missing_last_insert_id_as_an_opaque_invalid_response() {
        const MEMORY_ID_SENTINEL: &str = "memory-id-sentinel";
        let (result, invocations) = insert_with_response(json!({
            "affected_rows": 1,
            "returned_rows": [{
                "id": MEMORY_ID_SENTINEL,
                "version": "7",
            }],
        }))
        .await;

        assert_eq!(invocations, 1);
        let error = result.expect_err("a missing last insert ID should be invalid");
        assert_eq!(
            error,
            DatabaseError::invalid_response(DatabaseOperation::InsertMemory, "last_insert_id")
        );
        assert_opaque(&error, &[MEMORY_ID_SENTINEL]);
    }

    #[tokio::test]
    async fn insert_memory_rejects_a_null_successful_last_insert_id_as_an_opaque_failure() {
        const MEMORY_ID_SENTINEL: &str = "memory-id-sentinel";
        let (result, invocations) = insert_with_response(json!({
            "affected_rows": 1,
            "last_insert_id": null,
            "returned_rows": [{
                "id": MEMORY_ID_SENTINEL,
                "version": "7",
            }],
        }))
        .await;

        assert_eq!(invocations, 1);
        let error = result.expect_err("a null successful last insert ID should fail");
        assert_eq!(
            error,
            DatabaseError::database_failure(DatabaseOperation::InsertMemory)
        );
        assert_opaque(&error, &[MEMORY_ID_SENTINEL]);
    }

    #[tokio::test]
    async fn insert_memory_rejects_malformed_responses_with_static_columns() {
        for (response, column) in [
            (
                json!({
                    "last_insert_id": "memory-id-sentinel",
                    "returned_rows": [{"id": "memory-id-sentinel", "version": "7"}],
                }),
                "affected_rows",
            ),
            (
                json!({
                    "affected_rows": "1",
                    "last_insert_id": "memory-id-sentinel",
                    "returned_rows": [{"id": "memory-id-sentinel", "version": "7"}],
                }),
                "affected_rows",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": "memory-id-sentinel",
                }),
                "returned_rows",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": 7,
                    "returned_rows": [{"id": "memory-id-sentinel", "version": "7"}],
                }),
                "last_insert_id",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": "memory-id-sentinel",
                    "returned_rows": [{"version": "7"}],
                }),
                "id",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": "memory-id-sentinel",
                    "returned_rows": [{"id": "memory-id-sentinel", "version": 7}],
                }),
                "version",
            ),
        ] {
            let (result, invocations) = insert_with_response(response).await;

            assert_eq!(invocations, 1);
            assert_eq!(
                result,
                Err(DatabaseError::invalid_response(
                    DatabaseOperation::InsertMemory,
                    column,
                ))
            );
        }
    }

    #[tokio::test]
    async fn insert_memory_rejects_inconsistent_cardinality_and_keys() {
        for response in [
            json!({
                "affected_rows": 1,
                "last_insert_id": "memory-id-sentinel",
                "returned_rows": [],
            }),
            json!({
                "affected_rows": 1,
                "last_insert_id": "memory-id-sentinel",
                "returned_rows": [
                    {"id": "memory-id-sentinel", "version": "7"},
                    {"id": "memory-id-sentinel", "version": "7"},
                ],
            }),
            json!({
                "affected_rows": 0,
                "last_insert_id": "memory-id-sentinel",
                "returned_rows": [],
            }),
            json!({
                "affected_rows": 0,
                "last_insert_id": null,
                "returned_rows": [{"id": "memory-id-sentinel", "version": "7"}],
            }),
            json!({
                "affected_rows": 1,
                "last_insert_id": "memory-id-sentinel",
                "returned_rows": [{"id": "other-memory-id", "version": "7"}],
            }),
            json!({
                "affected_rows": 1,
                "last_insert_id": "memory-id-sentinel",
                "returned_rows": [{"id": "memory-id-sentinel", "version": "8"}],
            }),
            json!({
                "affected_rows": 1,
                "last_insert_id": "other-memory-id",
                "returned_rows": [{"id": "memory-id-sentinel", "version": "7"}],
            }),
        ] {
            let (result, invocations) = insert_with_response(response).await;

            assert_eq!(invocations, 1);
            assert_eq!(
                result,
                Err(DatabaseError::database_failure(
                    DatabaseOperation::InsertMemory,
                ))
            );
        }
    }

    #[tokio::test]
    async fn insert_memory_maps_sdk_errors_to_opaque_database_failures() {
        const SDK_CODE_SENTINEL: &str = "23505-sqlstate-sentinel";
        const SDK_MESSAGE_SENTINEL: &str = "sdk-message-secret-sentinel";

        let database = database_target();
        let memory = memory();
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocation_counter = Arc::clone(&invocations);
        let error = insert_memory_with(&database, &memory, move |_| {
            let invocation_counter = Arc::clone(&invocation_counter);
            async move {
                invocation_counter.fetch_add(1, Ordering::SeqCst);
                Err(IiiError::Remote {
                    code: SDK_CODE_SENTINEL.to_owned(),
                    message: SDK_MESSAGE_SENTINEL.to_owned(),
                    stacktrace: None,
                })
            }
        })
        .await
        .expect_err("SDK failure should reject the memory insert");

        assert_eq!(invocations.load(Ordering::SeqCst), 1);
        assert_eq!(
            error,
            DatabaseError::database_failure(DatabaseOperation::InsertMemory)
        );
        assert_opaque(&error, &[SDK_CODE_SENTINEL, SDK_MESSAGE_SENTINEL]);
    }

    #[tokio::test]
    async fn insert_embedding_uses_exact_cte_request_and_never_mutates_canonical_memory() {
        let database = database_target();
        let embedding = embedding(vec![1.25, -2.5]);
        let expected_response = embedding_response(&embedding, "inserted");
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocation_counter = Arc::clone(&invocations);

        insert_embedding_with(&database, &embedding, move |request| {
            let invocation_counter = Arc::clone(&invocation_counter);
            async move {
                invocation_counter.fetch_add(1, Ordering::SeqCst);
                assert_eq!(request.function_id, DATABASE_EXECUTE_FUNCTION_ID);
                assert_eq!(request.payload["db"], "memory-store-test");
                assert_eq!(
                    request.payload["sql"].as_str(),
                    Some(EXPECTED_INSERT_EMBEDDING_SQL)
                );
                assert_eq!(
                    request.payload["params"],
                    json!(["memory-id-sentinel", "7", "[1.25,-2.5]"])
                );
                let parameters = request.payload["params"]
                    .as_array()
                    .expect("embedding parameters should be positional");
                assert_eq!(parameters.len(), 3);
                assert!(parameters[2].is_string());
                assert_ne!(parameters[2], json!([1.25, -2.5]));
                let sql = request.payload["sql"]
                    .as_str()
                    .expect("embedding SQL should be a string");
                assert!(sql.contains("INSERT INTO public.memory_embeddings"));
                assert!(!sql.contains("INSERT INTO public.memories"));
                assert!(!sql.contains("UPDATE public.memories"));
                assert!(!sql.contains("DELETE FROM public.memories"));
                assert!(!sql.contains("memory_search_heads"));
                assert!(request.action.is_none());
                assert_eq!(request.timeout_ms, None);
                Ok(expected_response)
            }
        })
        .await
        .expect("one confirmed embedding insert should succeed");

        assert_eq!(invocations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn insert_embedding_maps_matching_conflict_and_missing_outcomes() {
        let embedding = embedding(vec![987_654.25]);

        let (conflict_result, conflict_invocations) =
            insert_embedding_with_response(&embedding, embedding_response(&embedding, "conflict"))
                .await;
        assert_eq!(conflict_invocations, 1);
        let conflict_error = conflict_result.expect_err("a matching conflict row should fail");
        assert_eq!(
            conflict_error,
            DatabaseError::conflict(DatabaseOperation::InsertEmbedding)
        );
        assert_opaque(&conflict_error, &["987654.25"]);

        let (missing_result, missing_invocations) =
            insert_embedding_with_response(&embedding, embedding_response(&embedding, "missing"))
                .await;
        assert_eq!(missing_invocations, 1);
        let missing_error = missing_result.expect_err("a matching missing row should fail");
        match missing_error {
            DatabaseError::MissingMemoryVersion { key } => {
                assert_eq!(key.id(), embedding.id());
                assert_eq!(key.version(), embedding.version());
            }
            error => panic!("expected a missing-parent error, got {error:?}"),
        }
    }

    #[tokio::test]
    async fn insert_embedding_rejects_malformed_envelopes_with_static_columns() {
        let embedding = embedding(vec![1.0]);
        for (response, column) in [
            (
                json!({
                    "last_insert_id": null,
                    "returned_rows": [{"outcome": "inserted", "id": "memory-id-sentinel", "version": "7"}],
                }),
                "affected_rows",
            ),
            (
                json!({
                    "affected_rows": "1",
                    "last_insert_id": null,
                    "returned_rows": [{"outcome": "inserted", "id": "memory-id-sentinel", "version": "7"}],
                }),
                "affected_rows",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "returned_rows": [{"outcome": "inserted", "id": "memory-id-sentinel", "version": "7"}],
                }),
                "last_insert_id",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": 7,
                    "returned_rows": [{"outcome": "inserted", "id": "memory-id-sentinel", "version": "7"}],
                }),
                "last_insert_id",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": true,
                    "returned_rows": [{"outcome": "inserted", "id": "memory-id-sentinel", "version": "7"}],
                }),
                "last_insert_id",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": [],
                    "returned_rows": [{"outcome": "inserted", "id": "memory-id-sentinel", "version": "7"}],
                }),
                "last_insert_id",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": {},
                    "returned_rows": [{"outcome": "inserted", "id": "memory-id-sentinel", "version": "7"}],
                }),
                "last_insert_id",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": null,
                    "returned_rows": {"outcome": "inserted"},
                }),
                "returned_rows",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": null,
                    "returned_rows": [{"id": "memory-id-sentinel", "version": "7"}],
                }),
                "outcome",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": null,
                    "returned_rows": [{"outcome": "inserted", "version": "7"}],
                }),
                "id",
            ),
            (
                json!({
                    "affected_rows": 1,
                    "last_insert_id": null,
                    "returned_rows": [{"outcome": "inserted", "id": "memory-id-sentinel", "version": 7}],
                }),
                "version",
            ),
        ] {
            let (result, invocations) = insert_embedding_with_response(&embedding, response).await;

            assert_eq!(invocations, 1);
            assert_eq!(
                result,
                Err(DatabaseError::invalid_response(
                    DatabaseOperation::InsertEmbedding,
                    column,
                ))
            );
        }
    }

    #[tokio::test]
    async fn insert_embedding_rejects_semantically_corrupt_outcomes() {
        let embedding = embedding(vec![1.0]);
        for response in [
            json!({
                "affected_rows": 1,
                "last_insert_id": "unexpected-last-id",
                "returned_rows": [{"outcome": "inserted", "id": "memory-id-sentinel", "version": "7"}],
            }),
            json!({
                "affected_rows": 0,
                "last_insert_id": null,
                "returned_rows": [{"outcome": "inserted", "id": "memory-id-sentinel", "version": "7"}],
            }),
            json!({
                "affected_rows": 1,
                "last_insert_id": null,
                "returned_rows": [],
            }),
            json!({
                "affected_rows": 1,
                "last_insert_id": null,
                "returned_rows": [
                    {"outcome": "inserted", "id": "memory-id-sentinel", "version": "7"},
                    {"outcome": "inserted", "id": "memory-id-sentinel", "version": "7"},
                ],
            }),
            json!({
                "affected_rows": 1,
                "last_insert_id": null,
                "returned_rows": [{"outcome": "unexpected", "id": "memory-id-sentinel", "version": "7"}],
            }),
            json!({
                "affected_rows": 1,
                "last_insert_id": null,
                "returned_rows": [{"outcome": "inserted", "id": "other-memory-id", "version": "7"}],
            }),
            json!({
                "affected_rows": 1,
                "last_insert_id": null,
                "returned_rows": [{"outcome": "conflict", "id": "memory-id-sentinel", "version": "8"}],
            }),
            json!({
                "affected_rows": 1,
                "last_insert_id": null,
                "returned_rows": [{"outcome": "missing", "id": "other-memory-id", "version": "8"}],
            }),
        ] {
            let (result, invocations) = insert_embedding_with_response(&embedding, response).await;

            assert_eq!(invocations, 1);
            assert_eq!(
                result,
                Err(DatabaseError::database_failure(
                    DatabaseOperation::InsertEmbedding,
                ))
            );
        }
    }

    #[tokio::test]
    async fn insert_embedding_maps_remote_errors_to_opaque_database_failures() {
        const SQLSTATE_SENTINEL: &str = "23503-sqlstate-sentinel";
        const REMOTE_MESSAGE_SENTINEL: &str = "remote-message-secret-sentinel";
        const VECTOR_SENTINEL: &str = "987654.25";

        let database = database_target();
        let embedding = embedding(vec![987_654.25]);
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocation_counter = Arc::clone(&invocations);
        let error = insert_embedding_with(&database, &embedding, move |_| {
            let invocation_counter = Arc::clone(&invocation_counter);
            async move {
                invocation_counter.fetch_add(1, Ordering::SeqCst);
                Err(IiiError::Remote {
                    code: SQLSTATE_SENTINEL.to_owned(),
                    message: REMOTE_MESSAGE_SENTINEL.to_owned(),
                    stacktrace: None,
                })
            }
        })
        .await
        .expect_err("remote errors should reject an embedding insert");

        assert_eq!(invocations.load(Ordering::SeqCst), 1);
        assert_eq!(
            error,
            DatabaseError::database_failure(DatabaseOperation::InsertEmbedding)
        );
        assert_opaque(
            &error,
            &[SQLSTATE_SENTINEL, REMOTE_MESSAGE_SENTINEL, VECTOR_SENTINEL],
        );
    }

    #[tokio::test]
    async fn insert_embedding_accepts_one_and_sixteen_thousand_dimensions() {
        for vector in [vec![1.0], vec![1.0; 16_000]] {
            let database = database_target();
            let embedding = embedding(vector);
            let expected_response = embedding_response(&embedding, "inserted");
            let dimensions = embedding.embedding().as_slice().len();
            let invocations = Arc::new(AtomicUsize::new(0));
            let invocation_counter = Arc::clone(&invocations);

            insert_embedding_with(&database, &embedding, move |request| {
                let invocation_counter = Arc::clone(&invocation_counter);
                async move {
                    invocation_counter.fetch_add(1, Ordering::SeqCst);
                    let vector_literal = request.payload["params"][2]
                        .as_str()
                        .expect("the vector parameter should be a literal string");
                    let narrowed: Vec<f32> = serde_json::from_str(vector_literal)
                        .expect("the vector literal should be JSON-compatible");
                    assert_eq!(narrowed.len(), dimensions);
                    assert!(narrowed.iter().all(|component| *component == 1.0));
                    Ok(expected_response)
                }
            })
            .await
            .expect("pgvector-supported dimensions should insert");

            assert_eq!(invocations.load(Ordering::SeqCst), 1);
        }
    }
}
