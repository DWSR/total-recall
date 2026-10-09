use chrono::{DateTime, Timelike, Utc};
use memory_store::contracts::{MemorySearchResult, MemoryVersionInput};
use rust_mcp_schema::{Tool, ToolInputSchema, ToolOutputSchema};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

pub const SEARCH_LIMIT_MAX: u32 = 50;
pub const VERSION_MIN: i64 = 1;
pub const VERSION_LIST_DEFAULT_LIMIT: u32 = 50;
pub const VERSION_LIST_MAX_LIMIT: u32 = 100;
pub const VERSION_LIST_MAX_OFFSET: u64 = 9_007_199_254_740_891;
pub const VERSION_LIST_MAX_NEXT_OFFSET: u64 = 9_007_199_254_740_991;

pub(crate) fn mcp_timestamp(timestamp: DateTime<Utc>) -> DateTime<Utc> {
    let microsecond_nanos = timestamp.timestamp_subsec_nanos() / 1_000 * 1_000;
    timestamp
        .with_nanosecond(microsecond_nanos)
        .expect("microsecond-aligned nanoseconds should be a valid UTC timestamp")
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SaveInput {
    pub title: String,
    pub content: String,
    pub session_id: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MemorySearchInput {
    pub query: String,
    pub limit: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GetLatestInput {
    pub id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GetExactInput {
    pub id: String,
    pub version: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ListVersionsInput {
    pub id: String,
    pub offset: Option<u64>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MemoryDto {
    pub id: String,
    pub version: i64,
    #[serde(rename = "type")]
    pub memory_type: String,
    pub title: String,
    pub content: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub concepts: Vec<String>,
    pub files: Vec<String>,
    pub session_ids: Vec<String>,
    pub source_observation_ids: Vec<String>,
}

impl From<MemoryVersionInput> for MemoryDto {
    fn from(memory: MemoryVersionInput) -> Self {
        let MemoryVersionInput {
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
        } = memory;

        Self {
            id,
            version,
            memory_type,
            title,
            content,
            created_at: mcp_timestamp(created_at),
            updated_at: mcp_timestamp(updated_at),
            concepts,
            files,
            session_ids,
            source_observation_ids,
        }
    }
}

impl From<&MemorySearchResult> for MemoryDto {
    fn from(memory: &MemorySearchResult) -> Self {
        Self {
            id: memory.id().as_str().to_owned(),
            version: memory.version().get(),
            memory_type: memory.memory_type().to_owned(),
            title: memory.title().to_owned(),
            content: memory.content().to_owned(),
            created_at: mcp_timestamp(memory.created_at().to_owned()),
            updated_at: mcp_timestamp(memory.updated_at().to_owned()),
            concepts: memory.concepts().to_vec(),
            files: memory.files().to_vec(),
            session_ids: memory.session_ids().to_vec(),
            source_observation_ids: memory.source_observation_ids().to_vec(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MemoryResults {
    pub results: Vec<MemoryDto>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MemoryVersionDto {
    pub version: i64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MemoryVersions {
    pub versions: Vec<MemoryVersionDto>,
    pub next_offset: Option<u64>,
}

pub fn tool_registry() -> Vec<Tool> {
    vec![
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
            memory_output_schema(),
        ),
        tool(
            "memory_search",
            object_schema(
                json!({
                    "query": { "type": "string" },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": SEARCH_LIMIT_MAX,
                    },
                }),
                &["query", "limit"],
            ),
            object_schema(
                json!({
                    "results": {
                        "type": "array",
                        "items": memory_schema(),
                    },
                }),
                &["results"],
            ),
        ),
        tool(
            "memory_get_latest",
            object_schema(json!({ "id": { "type": "string" } }), &["id"]),
            memory_output_schema(),
        ),
        tool(
            "memory_get_exact",
            object_schema(
                json!({
                    "id": { "type": "string" },
                    "version": { "type": "integer", "minimum": VERSION_MIN },
                }),
                &["id", "version"],
            ),
            memory_output_schema(),
        ),
        tool(
            "memory_list_versions",
            object_schema(
                json!({
                    "id": { "type": "string" },
                    "offset": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": VERSION_LIST_MAX_OFFSET,
                        "default": 0,
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": VERSION_LIST_MAX_LIMIT,
                        "default": VERSION_LIST_DEFAULT_LIMIT,
                    },
                }),
                &["id"],
            ),
            object_schema(
                json!({
                    "versions": {
                        "type": "array",
                        "items": memory_version_schema(),
                    },
                    "next_offset": {
                        "type": ["integer", "null"],
                        "minimum": 0,
                        "maximum": VERSION_LIST_MAX_NEXT_OFFSET,
                    },
                }),
                &["versions", "next_offset"],
            ),
        ),
    ]
}

fn tool(name: &str, input: Value, output: Value) -> Tool {
    Tool {
        annotations: None,
        description: None,
        icons: Vec::new(),
        input_schema: ToolInputSchema::new(None, Some(schema_map(input))),
        meta: None,
        name: name.to_owned(),
        output_schema: Some(ToolOutputSchema {
            schema: None,
            extra: Some(schema_map(output)),
        }),
        title: None,
    }
}

fn memory_output_schema() -> Value {
    object_schema(json!({ "memory": memory_schema() }), &["memory"])
}

fn memory_schema() -> Value {
    object_schema(
        json!({
            "id": { "type": "string" },
            "version": { "type": "integer", "minimum": VERSION_MIN },
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
            "version": { "type": "integer", "minimum": VERSION_MIN },
            "updated_at": { "type": "string", "format": "date-time" },
        }),
        &["version", "updated_at"],
    )
}

fn object_schema(properties: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}

fn schema_map(value: Value) -> Map<String, Value> {
    let Value::Object(map) = value else {
        unreachable!("tool schemas are JSON objects");
    };
    map
}
