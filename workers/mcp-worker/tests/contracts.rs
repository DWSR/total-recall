use chrono::{DateTime, Utc};
use mcp_worker::contracts::{
    GetExactInput, GetLatestInput, ListVersionsInput, MemoryDto, MemoryResults, MemorySearchInput,
    MemoryVersionDto, MemoryVersions, SaveInput, tool_registry,
};
use memory_store::contracts::{MemorySearchResult, MemoryVersionInput};
use serde_json::{Value, json};

const MAX_LIST_OFFSET: u64 = 9_007_199_254_740_891;
const MAX_JSON_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

#[test]
fn tool_registry_snapshots_all_strict_contracts() {
    assert_eq!(
        serde_json::to_value(tool_registry()).unwrap(),
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
                        "limit": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": 50,
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
                            "maximum": MAX_LIST_OFFSET,
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
                        "versions": {
                            "type": "array",
                            "items": memory_version_schema(),
                        },
                        "next_offset": {
                            "type": ["integer", "null"],
                            "minimum": 0,
                            "maximum": MAX_JSON_SAFE_INTEGER,
                        },
                    }),
                    &["versions", "next_offset"],
                ),
            ),
        ]),
    );
}

#[test]
fn inputs_reject_unknown_fields_and_wrong_json_types() {
    assert_eq!(
        serde_json::from_value::<SaveInput>(json!({
            "title": "title",
            "content": "content",
            "session_id": "session",
        }))
        .unwrap(),
        SaveInput {
            title: "title".to_owned(),
            content: "content".to_owned(),
            session_id: "session".to_owned(),
        }
    );
    assert_eq!(
        serde_json::from_value::<GetLatestInput>(json!({ "id": "memory" })).unwrap(),
        GetLatestInput {
            id: "memory".to_owned(),
        }
    );
    assert_eq!(
        serde_json::from_value::<GetExactInput>(json!({ "id": "memory", "version": 1 })).unwrap(),
        GetExactInput {
            id: "memory".to_owned(),
            version: 1,
        }
    );
    assert_eq!(
        serde_json::from_value::<ListVersionsInput>(json!({ "id": "memory" })).unwrap(),
        ListVersionsInput {
            id: "memory".to_owned(),
            offset: None,
            limit: None,
        }
    );

    assert!(
        serde_json::from_value::<SaveInput>(json!({
            "title": "title",
            "content": "content",
            "session_id": "session",
            "unknown": true,
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<SaveInput>(json!({
            "title": 7,
            "content": "content",
            "session_id": "session",
        }))
        .is_err()
    );

    assert!(
        serde_json::from_value::<GetLatestInput>(json!({ "id": "memory", "unknown": true }))
            .is_err()
    );
    assert!(serde_json::from_value::<GetLatestInput>(json!({ "id": [] })).is_err());

    assert!(
        serde_json::from_value::<GetExactInput>(json!({
            "id": "memory",
            "version": 1,
            "unknown": true,
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<GetExactInput>(json!({ "id": "memory", "version": "1" })).is_err()
    );

    assert!(
        serde_json::from_value::<ListVersionsInput>(json!({
            "id": "memory",
            "unknown": true,
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<ListVersionsInput>(json!({ "id": "memory", "offset": "0" }))
            .is_err()
    );
}

#[test]
fn memory_search_input_accepts_a_query_and_limit_as_the_complete_input() {
    for (query, limit) in [
        ("query", 1),
        (" query-secret-sentinel ", 50),
        (" \t\r\n ", 7),
        ("na\u{ef}ve cafe\u{301} \u{2615}", 3),
        ("nul\0is-rejected-by-the-service", 2),
    ] {
        let input =
            serde_json::from_value::<MemorySearchInput>(json!({ "query": query, "limit": limit }))
                .expect("a query and limit should be the complete search input");

        assert_eq!(input.query.as_bytes(), query.as_bytes());
        assert_eq!(
            input,
            MemorySearchInput {
                query: query.to_owned(),
                limit,
            }
        );
    }
}

#[test]
fn memory_search_input_rejects_caller_vectors_unknown_fields_and_wrong_types() {
    for arguments in [
        json!({ "query": "query", "vector": [1.25, -2.5], "limit": 1 }),
        json!({ "query": "query", "vector": [], "limit": 1 }),
        json!({ "query": "query", "vector": null, "limit": 1 }),
        json!({ "query": "query", "limit": 1, "embedding": [1.25, -2.5] }),
        json!({ "query": "query", "limit": 1, "unknown": true }),
        json!({ "Query": "query", "limit": 1 }),
        json!({ "limit": 1 }),
        json!({ "query": "query" }),
        json!({}),
        json!({ "query": false, "limit": 1 }),
        json!({ "query": 7, "limit": 1 }),
        json!({ "query": null, "limit": 1 }),
        json!({ "query": ["query"], "limit": 1 }),
        json!({ "query": "query", "limit": "1" }),
        json!({ "query": "query", "limit": 1.5 }),
        json!({ "query": "query", "limit": -1 }),
        json!({ "query": "query", "limit": 4_294_967_296_u64 }),
        json!({ "query": "query", "limit": null }),
    ] {
        assert!(
            serde_json::from_value::<MemorySearchInput>(arguments.clone()).is_err(),
            "search input should reject {arguments}"
        );
    }
}

#[test]
fn success_dtos_project_complete_score_free_memories_and_narrow_versions() {
    let canonical = canonical_memory();
    let expected_memory_value = expected_memory();

    assert_eq!(
        serde_json::to_value(MemoryDto::from(canonical.clone())).unwrap(),
        expected_memory_value
    );

    let search_result = MemorySearchResult::try_new(canonical.try_into().unwrap(), 0.875).unwrap();
    let results = serde_json::to_value(MemoryResults {
        results: vec![MemoryDto::from(&search_result)],
    })
    .unwrap();
    assert_eq!(results, json!({ "results": [expected_memory()] }));
    assert!(
        !results["results"][0]
            .as_object()
            .unwrap()
            .contains_key("embedding")
    );
    assert!(
        !results["results"][0]
            .as_object()
            .unwrap()
            .contains_key("relevance")
    );

    let versions = serde_json::to_value(MemoryVersions {
        versions: vec![MemoryVersionDto {
            version: 7,
            updated_at: timestamp("2026-09-20T10:05:00Z"),
        }],
        next_offset: None,
    })
    .unwrap();
    assert_eq!(
        versions,
        json!({
            "versions": [{
                "version": 7,
                "updated_at": "2026-09-20T10:05:00Z",
            }],
            "next_offset": null,
        })
    );
    assert_eq!(versions["versions"][0].as_object().unwrap().len(), 2);
}

#[test]
fn memory_dtos_normalize_created_and_updated_timestamps_to_whole_seconds() {
    let mut canonical = canonical_memory();
    canonical.created_at = timestamp("2026-09-20T10:00:00.123456Z");
    canonical.updated_at = timestamp("2026-09-20T10:05:00.654321Z");

    let exact = serde_json::to_value(MemoryDto::from(canonical.clone())).unwrap();
    assert_eq!(exact["created_at"], "2026-09-20T10:00:00Z");
    assert_eq!(exact["updated_at"], "2026-09-20T10:05:00Z");

    let search_result = MemorySearchResult::try_new(canonical.try_into().unwrap(), 0.875).unwrap();
    let search = serde_json::to_value(MemoryResults {
        results: vec![MemoryDto::from(&search_result)],
    })
    .unwrap();
    assert_eq!(search["results"][0]["created_at"], "2026-09-20T10:00:00Z");
    assert_eq!(search["results"][0]["updated_at"], "2026-09-20T10:05:00Z");
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

fn canonical_memory() -> MemoryVersionInput {
    MemoryVersionInput {
        id: "memory-7".to_owned(),
        version: 7,
        memory_type: "unclassified".to_owned(),
        title: "Strict contract".to_owned(),
        content: "A complete memory projection.".to_owned(),
        created_at: timestamp("2026-09-20T10:00:00Z"),
        updated_at: timestamp("2026-09-20T10:05:00Z"),
        concepts: vec!["contracts".to_owned()],
        files: vec!["workers/mcp-worker/src/contracts.rs".to_owned()],
        session_ids: vec!["session-7".to_owned()],
        source_observation_ids: vec!["observation-7".to_owned()],
    }
}

fn expected_memory() -> Value {
    json!({
        "id": "memory-7",
        "version": 7,
        "type": "unclassified",
        "title": "Strict contract",
        "content": "A complete memory projection.",
        "created_at": "2026-09-20T10:00:00Z",
        "updated_at": "2026-09-20T10:05:00Z",
        "concepts": ["contracts"],
        "files": ["workers/mcp-worker/src/contracts.rs"],
        "session_ids": ["session-7"],
        "source_observation_ids": ["observation-7"],
    })
}

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .unwrap()
        .with_timezone(&Utc)
}
