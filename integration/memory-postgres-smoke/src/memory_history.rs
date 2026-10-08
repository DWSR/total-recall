use std::{env, time::Duration};

use iii_sdk::{InitOptions, WorkerIdentityMode, register_worker, runtime::WorkerMetadata};
use mcp_worker::{
    backend::{CanonicalStoreDelegate, ProductionMemoryOperations, ReadOnlyRetrievalAdapter},
    embedding::RouterQueryEmbeddingGenerator,
    server::McpServer,
    service::{MemoryToolService, ProductionCreateContext},
};
use memory_store::{MemoryStore, contracts::DatabaseTarget, database::IiiMemoryDatabase};
use rust_mcp_schema::{ClientCapabilities, JsonrpcRequest, RequestId, RequestMetaObject};
use serde_json::{Map, Value, json};
use tokio::time::timeout;

const III_URL_ENV: &str = "III_URL";
const III_NAMESPACE_ENV: &str = "III_NAMESPACE";
const MEMORY_DATABASE_ENV: &str = "TOTAL_RECALL_MEMORY_DATABASE";
const PROTOCOL_VERSION: &str = "2026-07-28";
const REGISTRATION_TIMEOUT: Duration = Duration::from_secs(10);
const TOOL_TIMEOUT: Duration = Duration::from_secs(10);
const VERIFICATION_TIMEOUT: Duration = Duration::from_secs(60);

const NATIVE_ID: &str = "native-memory-history";

type ProductionServer = McpServer<
    ProductionMemoryOperations<IiiMemoryDatabase>,
    ProductionCreateContext,
    RouterQueryEmbeddingGenerator,
>;

pub async fn run() -> Result<(), String> {
    let engine_url = required_env(III_URL_ENV)?;
    let namespace = required_env(III_NAMESPACE_ENV)?;
    let database_name = required_env(MEMORY_DATABASE_ENV)?;
    let database = DatabaseTarget::try_from(database_name)
        .map_err(|_| "memory database target is invalid".to_owned())?;

    let metadata = WorkerMetadata {
        name: format!("memory-history-verifier-{}", std::process::id()),
        namespace: Some(namespace.clone()),
        ..WorkerMetadata::default()
    };
    let client = register_worker(
        &engine_url,
        InitOptions {
            metadata: Some(metadata),
            namespace: Some(namespace),
            identity: WorkerIdentityMode::Managed,
            ..InitOptions::default()
        },
    );

    match timeout(
        REGISTRATION_TIMEOUT,
        client.wait_until_registered(REGISTRATION_TIMEOUT),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(_)) => {
            client.shutdown_async().await;
            return Err("MCP verifier could not register with the iii engine".to_owned());
        }
        Err(_) => {
            client.shutdown_async().await;
            return Err("MCP verifier registration exceeded its deadline".to_owned());
        }
    }

    let memory_database = IiiMemoryDatabase::new(client.clone(), database.clone());
    let store = MemoryStore::new(memory_database);
    let operations = ProductionMemoryOperations::new(
        CanonicalStoreDelegate::new(store),
        ReadOnlyRetrievalAdapter::new(client.clone(), database),
    );
    let service = MemoryToolService::new(
        operations,
        ProductionCreateContext,
        None::<RouterQueryEmbeddingGenerator>,
    );
    let server = McpServer::new(service);

    let result = match timeout(VERIFICATION_TIMEOUT, verify(&server)).await {
        Ok(result) => result,
        Err(_) => Err("MCP history verification exceeded its deadline".to_owned()),
    };
    client.shutdown_async().await;
    result
}

async fn verify(server: &ProductionServer) -> Result<(), String> {
    let native_latest = call_tool(server, "memory_get_latest", json!({ "id": NATIVE_ID })).await?;
    let native_latest = memory_result(&native_latest, "memory_get_latest")?;
    expect_memory(native_latest, NATIVE_ID, 2, "mcp-native-current-title")?;

    let native_exact = call_tool(
        server,
        "memory_get_exact",
        json!({ "id": NATIVE_ID, "version": 1 }),
    )
    .await?;
    let native_exact = memory_result(&native_exact, "memory_get_exact")?;
    expect_memory(native_exact, NATIVE_ID, 1, "mcp-native-old-title")?;

    let first_page = call_tool(
        server,
        "memory_list_versions",
        json!({ "id": NATIVE_ID, "limit": 1 }),
    )
    .await?;
    let first_page = success_content(&first_page, "memory_list_versions")?;
    expect_version_page(
        first_page,
        &[(2, "2026-09-28T12:01:00.654321+00:00")],
        Some(1),
    )?;

    let second_page = call_tool(
        server,
        "memory_list_versions",
        json!({ "id": NATIVE_ID, "limit": 1, "offset": 1 }),
    )
    .await?;
    let second_page = success_content(&second_page, "memory_list_versions")?;
    expect_version_page(
        second_page,
        &[(1, "2026-09-28T12:00:00.654321+00:00")],
        None,
    )?;

    let historical_search = call_tool(
        server,
        "memory_search",
        json!({ "query": "historyhiddenmarker", "limit": 10 }),
    )
    .await?;
    expect_search_excludes(&historical_search, NATIVE_ID)?;

    let native_current_search = call_tool(
        server,
        "memory_search",
        json!({ "query": "nativecurrentmarker", "limit": 10 }),
    )
    .await?;
    expect_search_version(&native_current_search, NATIVE_ID, 2)?;

    let native_old_search = call_tool(
        server,
        "memory_search",
        json!({ "query": "nativehiddenmarker", "limit": 10 }),
    )
    .await?;
    expect_search_excludes(&native_old_search, NATIVE_ID)?;

    Ok(())
}

async fn call_tool(
    server: &ProductionServer,
    tool: &str,
    arguments: Value,
) -> Result<Value, String> {
    let arguments = arguments
        .as_object()
        .ok_or_else(|| "MCP verifier arguments should be an object".to_owned())?;
    let mut params = Map::new();
    let metadata = RequestMetaObject::new(PROTOCOL_VERSION, ClientCapabilities::default());
    params.insert(
        "_meta".to_owned(),
        serde_json::to_value(metadata)
            .map_err(|_| "MCP verifier metadata could not be serialized".to_owned())?,
    );
    params.insert("name".to_owned(), Value::String(tool.to_owned()));
    params.insert("arguments".to_owned(), Value::Object(arguments.clone()));

    let request_id = format!("memory-history-{tool}");
    let request = JsonrpcRequest::new(
        RequestId::String(request_id.clone()),
        "tools/call".to_owned(),
        Some(params),
    );
    let response = timeout(TOOL_TIMEOUT, server.dispatch(request))
        .await
        .map_err(|_| format!("MCP tool {tool} exceeded its deadline"))?;
    let response = response
        .into_wire_bytes()
        .map_err(|_| format!("MCP tool {tool} response could not be serialized"))?;
    let response: Value = serde_json::from_slice(&response)
        .map_err(|_| format!("MCP tool {tool} response was not valid JSON"))?;

    if response.get("id").and_then(Value::as_str) != Some(request_id.as_str()) {
        return Err(format!("MCP tool {tool} response ID did not match"));
    }
    Ok(response)
}

fn success_content<'a>(response: &'a Value, tool: &str) -> Result<&'a Value, String> {
    let result = response
        .get("result")
        .ok_or_else(|| format!("MCP tool {tool} omitted its result"))?;
    if result.get("isError").and_then(Value::as_bool) == Some(true) {
        let code = result
            .get("content")
            .and_then(Value::as_array)
            .and_then(|content| content.first())
            .and_then(|content| content.get("text"))
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        return Err(format!("MCP tool {tool} returned {code}"));
    }
    result
        .get("structuredContent")
        .ok_or_else(|| format!("MCP tool {tool} omitted structured content"))
}

fn memory_result<'a>(response: &'a Value, tool: &str) -> Result<&'a Value, String> {
    success_content(response, tool)?
        .get("memory")
        .ok_or_else(|| format!("MCP tool {tool} omitted its memory"))
}

fn expect_memory(
    memory: &Value,
    expected_id: &str,
    expected_version: i64,
    expected_title: &str,
) -> Result<(), String> {
    if memory.get("id").and_then(Value::as_str) != Some(expected_id)
        || memory.get("version").and_then(Value::as_i64) != Some(expected_version)
        || memory.get("title").and_then(Value::as_str) != Some(expected_title)
    {
        return Err("MCP memory lookup returned the wrong version".to_owned());
    }
    Ok(())
}

fn expect_version_page(
    content: &Value,
    expected: &[(i64, &str)],
    expected_next_offset: Option<u64>,
) -> Result<(), String> {
    let versions = content
        .get("versions")
        .and_then(Value::as_array)
        .ok_or_else(|| "MCP version listing omitted versions".to_owned())?;
    if versions.len() != expected.len() {
        return Err("MCP version listing returned the wrong page size".to_owned());
    }
    for (version, (expected_version, expected_timestamp)) in versions.iter().zip(expected) {
        if version.get("version").and_then(Value::as_i64) != Some(*expected_version) {
            return Err("MCP version listing returned the wrong order".to_owned());
        }
        expect_timestamp(version.get("updated_at"), expected_timestamp)?;
    }
    if content.get("next_offset").and_then(Value::as_u64) != expected_next_offset {
        return Err("MCP version listing returned the wrong continuation offset".to_owned());
    }
    Ok(())
}

fn expect_timestamp(value: Option<&Value>, expected: &str) -> Result<(), String> {
    let actual = value
        .and_then(Value::as_str)
        .ok_or_else(|| "MCP timestamp was not a string".to_owned())?;
    let actual = chrono::DateTime::parse_from_rfc3339(actual)
        .map_err(|_| "MCP timestamp was not RFC 3339".to_owned())?;
    let expected = chrono::DateTime::parse_from_rfc3339(expected)
        .map_err(|_| "MCP verifier timestamp fixture was invalid".to_owned())?;
    if actual.timestamp_micros() != expected.timestamp_micros()
        || actual.timestamp_subsec_nanos() != expected.timestamp_subsec_nanos()
    {
        return Err(format!(
            "MCP timestamp differs from the expected value: expected {expected}, received {actual}"
        ));
    }
    Ok(())
}

fn expect_search_excludes(response: &Value, excluded_id: &str) -> Result<(), String> {
    let results = success_content(response, "memory_search")?
        .get("results")
        .and_then(Value::as_array)
        .ok_or_else(|| "MCP search omitted results".to_owned())?;
    if results
        .iter()
        .any(|memory| memory.get("id").and_then(Value::as_str) == Some(excluded_id))
    {
        return Err("MCP current search exposed a historical memory version".to_owned());
    }
    Ok(())
}

fn expect_search_version(
    response: &Value,
    expected_id: &str,
    expected_version: i64,
) -> Result<(), String> {
    let results = success_content(response, "memory_search")?
        .get("results")
        .and_then(Value::as_array)
        .ok_or_else(|| "MCP search omitted results".to_owned())?;
    if !results.iter().any(|memory| {
        memory.get("id").and_then(Value::as_str) == Some(expected_id)
            && memory.get("version").and_then(Value::as_i64) == Some(expected_version)
    }) {
        return Err("MCP current search omitted the expected current head".to_owned());
    }
    Ok(())
}

fn required_env(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("{name} is required"))
}
