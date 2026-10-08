use rust_mcp_schema::schema_utils::RpcErrorCodes;
use rust_mcp_schema::{
    CallToolRequest, CallToolRequestParams, CallToolResult, CallToolResultResponse,
    CallToolResultResponseResult, ClientCapabilities, ContentBlock, DiscoverRequest,
    DiscoverResult, DiscoverResultCacheScope, DiscoverResultResponse, Implementation,
    JsonrpcErrorResponse, JsonrpcRequest, ListToolsRequest, ListToolsResult,
    ListToolsResultCacheScope, ListToolsResultResponse, PaginatedRequestParams, RequestId,
    RequestMetaObject, RequestParams, ResultMetaObject, ServerCapabilities,
    ServerCapabilitiesTools, TextContent,
};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};

use crate::{
    contracts::{
        GetExactInput, GetLatestInput, ListVersionsInput, MemorySearchInput, SaveInput,
        tool_registry,
    },
    embedding::QueryEmbeddingGenerator,
    operations::MemoryOperations,
    service::{CreateContext, MemoryToolService},
};

const SERVER_NAME: &str = "total-recall-mcp";
const PROTOCOL_VERSION: &str = "2026-07-28";
const RESPONSE_TTL_MS: u64 = 0;
const PROTOCOL_VERSION_META_KEY: &str = "io.modelcontextprotocol/protocolVersion";
const CLIENT_CAPABILITIES_META_KEY: &str = "io.modelcontextprotocol/clientCapabilities";

pub enum DispatchResponse {
    Discover(DiscoverResultResponse),
    ListTools(ListToolsResultResponse),
    CallTool(CallToolResultResponse),
    Error(JsonrpcErrorResponse),
}

impl DispatchResponse {
    pub fn into_wire_bytes(self) -> Result<Vec<u8>, serde_json::Error> {
        match self {
            Self::Discover(response) => serde_json::to_vec(&response),
            Self::ListTools(response) => serde_json::to_vec(&response),
            Self::CallTool(response) => serde_json::to_vec(&response),
            Self::Error(response) => serde_json::to_vec(&response),
        }
    }
}

pub struct McpServer<O, C, G>
where
    O: MemoryOperations,
    C: CreateContext,
    G: QueryEmbeddingGenerator,
{
    service: MemoryToolService<O, C, G>,
}

impl<O, C, G> McpServer<O, C, G>
where
    O: MemoryOperations,
    C: CreateContext,
    G: QueryEmbeddingGenerator,
{
    pub fn new(service: MemoryToolService<O, C, G>) -> Self {
        Self { service }
    }

    pub fn discover(&self, request: DiscoverRequest) -> DiscoverResultResponse {
        DiscoverResultResponse::new(
            request.id,
            DiscoverResult {
                cache_scope: DiscoverResultCacheScope::Public,
                capabilities: ServerCapabilities {
                    completions: None,
                    experimental: None,
                    extensions: None,
                    logging: None,
                    prompts: None,
                    resources: None,
                    tools: Some(ServerCapabilitiesTools { list_changed: None }),
                },
                instructions: None,
                meta: Some(server_identity()),
                result_type: "complete".to_owned(),
                supported_versions: vec![PROTOCOL_VERSION.to_owned()],
                ttl_ms: RESPONSE_TTL_MS,
            },
        )
    }

    pub fn list_tools(&self, request: ListToolsRequest) -> ListToolsResultResponse {
        ListToolsResultResponse::new(
            request.id,
            ListToolsResult {
                cache_scope: ListToolsResultCacheScope::Public,
                meta: Some(server_identity()),
                next_cursor: None,
                result_type: "complete".to_owned(),
                tools: tool_registry(),
                ttl_ms: RESPONSE_TTL_MS,
            },
        )
    }

    pub async fn call_tool(&self, request: CallToolRequest) -> CallToolResultResponse {
        let CallToolRequest { id, params, .. } = request;
        if !has_supported_metadata(&params.meta)
            || params.input_responses.is_some()
            || params.request_state.is_some()
        {
            return tool_error_response(id, "invalid_input");
        }

        let tool_name = params.name;
        let arguments = params.arguments;
        match tool_name.as_str() {
            "memory_save" => match decode_arguments::<SaveInput>(arguments) {
                Ok(input) => match self.service.save(input).await {
                    Ok(memory) => {
                        tool_success_response(id, &tool_name, json!({ "memory": memory }))
                    }
                    Err(error) => tool_error_response(id, error.code()),
                },
                Err(()) => tool_error_response(id, "invalid_input"),
            },
            "memory_search" => match decode_arguments::<MemorySearchInput>(arguments) {
                Ok(input) => match self.service.search(input).await {
                    Ok(results) => {
                        tool_success_response(id, &tool_name, json!({ "results": results.results }))
                    }
                    Err(error) => tool_error_response(id, error.code()),
                },
                Err(()) => tool_error_response(id, "invalid_input"),
            },
            "memory_get_latest" => match decode_arguments::<GetLatestInput>(arguments) {
                Ok(input) => match self.service.get_latest(input).await {
                    Ok(memory) => {
                        tool_success_response(id, &tool_name, json!({ "memory": memory }))
                    }
                    Err(error) => tool_error_response(id, error.code()),
                },
                Err(()) => tool_error_response(id, "invalid_input"),
            },
            "memory_get_exact" => match decode_arguments::<GetExactInput>(arguments) {
                Ok(input) => match self.service.get_exact(input).await {
                    Ok(memory) => {
                        tool_success_response(id, &tool_name, json!({ "memory": memory }))
                    }
                    Err(error) => tool_error_response(id, error.code()),
                },
                Err(()) => tool_error_response(id, "invalid_input"),
            },
            "memory_list_versions" => match decode_arguments::<ListVersionsInput>(arguments) {
                Ok(input) => match self.service.list_versions(input).await {
                    Ok(versions) => tool_success_response(
                        id,
                        &tool_name,
                        json!({
                            "versions": versions.versions,
                            "next_offset": versions.next_offset,
                        }),
                    ),
                    Err(error) => tool_error_response(id, error.code()),
                },
                Err(()) => tool_error_response(id, "invalid_input"),
            },
            _ => tool_error_response(id, "invalid_input"),
        }
    }

    pub async fn dispatch(&self, request: JsonrpcRequest) -> DispatchResponse {
        let JsonrpcRequest {
            id, method, params, ..
        } = request;
        let Some(params) = params else {
            return DispatchResponse::Error(protocol_error(
                Some(id),
                RpcErrorCodes::INVALID_PARAMS,
                "invalid_protocol_metadata",
            ));
        };

        if let Err(error) = validate_metadata(&params) {
            return DispatchResponse::Error(protocol_error(
                Some(id),
                error.code(),
                error.message(),
            ));
        }

        match method.as_str() {
            "server/discover" => match deserialize_request_params::<RequestParams>(params) {
                Ok(params) => {
                    DispatchResponse::Discover(self.discover(DiscoverRequest::new(id, params)))
                }
                Err(()) => DispatchResponse::Error(protocol_error(
                    Some(id),
                    RpcErrorCodes::INVALID_PARAMS,
                    "invalid_protocol_metadata",
                )),
            },
            "tools/list" => match deserialize_request_params::<PaginatedRequestParams>(params) {
                Ok(params) => {
                    DispatchResponse::ListTools(self.list_tools(ListToolsRequest::new(id, params)))
                }
                Err(()) => DispatchResponse::Error(protocol_error(
                    Some(id),
                    RpcErrorCodes::INVALID_PARAMS,
                    "invalid_protocol_metadata",
                )),
            },
            "tools/call" => match deserialize_request_params::<CallToolRequestParams>(params) {
                Ok(params)
                    if params.input_responses.is_none() && params.request_state.is_none() =>
                {
                    DispatchResponse::CallTool(
                        self.call_tool(CallToolRequest::new(id, params)).await,
                    )
                }
                Ok(_) => DispatchResponse::Error(protocol_error(
                    Some(id),
                    RpcErrorCodes::INVALID_PARAMS,
                    "unsupported_tool_feature",
                )),
                Err(()) => DispatchResponse::Error(protocol_error(
                    Some(id),
                    RpcErrorCodes::INVALID_PARAMS,
                    "invalid_protocol_metadata",
                )),
            },
            _ => DispatchResponse::Error(protocol_error(
                Some(id),
                RpcErrorCodes::METHOD_NOT_FOUND,
                "method_not_found",
            )),
        }
    }
}

enum MetadataError {
    Invalid,
    UnsupportedVersion,
}

impl MetadataError {
    const fn code(&self) -> RpcErrorCodes {
        match self {
            Self::Invalid => RpcErrorCodes::INVALID_PARAMS,
            Self::UnsupportedVersion => RpcErrorCodes::UNSUPPORTED_PROTOCOL_VERSION,
        }
    }

    const fn message(&self) -> &'static str {
        match self {
            Self::Invalid => "invalid_protocol_metadata",
            Self::UnsupportedVersion => "unsupported_protocol_version",
        }
    }
}

fn validate_metadata(params: &Map<String, Value>) -> Result<(), MetadataError> {
    let Some(Value::Object(meta)) = params.get("_meta") else {
        return Err(MetadataError::Invalid);
    };
    let Some(Value::String(protocol_version)) = meta.get(PROTOCOL_VERSION_META_KEY) else {
        return Err(MetadataError::Invalid);
    };
    if protocol_version != PROTOCOL_VERSION {
        return Err(MetadataError::UnsupportedVersion);
    }
    let Some(Value::Object(capabilities)) = meta.get(CLIENT_CAPABILITIES_META_KEY) else {
        return Err(MetadataError::Invalid);
    };

    serde_json::from_value::<ClientCapabilities>(Value::Object(capabilities.clone()))
        .map(|_| ())
        .map_err(|_| MetadataError::Invalid)
}

fn has_supported_metadata(meta: &RequestMetaObject) -> bool {
    meta.protocol_version == PROTOCOL_VERSION
}

fn deserialize_request_params<T: DeserializeOwned>(params: Map<String, Value>) -> Result<T, ()> {
    serde_json::from_value(Value::Object(params)).map_err(|_| ())
}

fn decode_arguments<T: DeserializeOwned>(arguments: Option<Map<String, Value>>) -> Result<T, ()> {
    serde_json::from_value(Value::Object(arguments.unwrap_or_default())).map_err(|_| ())
}

fn tool_success_response(
    id: RequestId,
    tool_name: &str,
    structured_content: Value,
) -> CallToolResultResponse {
    CallToolResultResponse::new(
        id,
        CallToolResultResponseResult::CallToolResult(CallToolResult {
            content: vec![ContentBlock::TextContent(TextContent::from(format!(
                "{tool_name} complete"
            )))],
            is_error: None,
            meta: Some(server_identity()),
            result_type: "complete".to_owned(),
            structured_content: Some(structured_content),
        }),
    )
}

fn tool_error_response(id: RequestId, code: &str) -> CallToolResultResponse {
    CallToolResultResponse::new(
        id,
        CallToolResultResponseResult::CallToolResult(CallToolResult {
            content: vec![ContentBlock::TextContent(TextContent::from(code))],
            is_error: Some(true),
            meta: None,
            result_type: "complete".to_owned(),
            structured_content: None,
        }),
    )
}

fn protocol_error(
    id: Option<RequestId>,
    code: RpcErrorCodes,
    message: &str,
) -> JsonrpcErrorResponse {
    JsonrpcErrorResponse::create(id, code, message.to_owned(), None)
}

fn server_identity() -> ResultMetaObject {
    ResultMetaObject {
        io_modelcontextprotocol_server_info: Some(Implementation {
            description: None,
            icons: Vec::new(),
            name: SERVER_NAME.to_owned(),
            title: None,
            version: env!("CARGO_PKG_VERSION").to_owned(),
            website_url: None,
        }),
        extra: None,
    }
}
