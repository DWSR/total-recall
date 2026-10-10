use async_trait::async_trait;
use mcp_worker::{
    config::{Config, TOTAL_RECALL_MEMORY_DATABASE_ENV},
    contracts::{MemoryContextInput, MemoryContextOutput},
    operations::{OperationError, OperationInputField},
    service::{StartupContextOperations, StartupContextService, ToolError},
};
use memory_store::contracts::{ContextCandidate, ContextMemory, ContextQuery};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct FakeOperations {
    result: Result<Vec<ContextCandidate>, OperationError>,
    calls: Arc<Mutex<Vec<ContextQuery>>>,
}

#[async_trait]
impl StartupContextOperations for FakeOperations {
    async fn list_recent(
        &self,
        query: ContextQuery,
    ) -> Result<Vec<ContextCandidate>, OperationError> {
        self.calls.lock().unwrap().push(query);
        self.result.clone()
    }
}

fn memory(title: &str, content: &str) -> ContextCandidate {
    ContextCandidate::Memory(ContextMemory {
        memory_type: "fact".into(),
        title: title.into(),
        content: content.into(),
        concepts: vec!["café".into()],
    })
}

fn service_for(
    result: Result<Vec<ContextCandidate>, OperationError>,
    max_bytes: usize,
) -> (
    StartupContextService<FakeOperations>,
    Arc<Mutex<Vec<ContextQuery>>>,
) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let operations = FakeOperations {
        result,
        calls: calls.clone(),
    };
    let config = Config::from_values([
        (
            TOTAL_RECALL_MEMORY_DATABASE_ENV.to_owned(),
            "test-db".to_owned(),
        ),
        (
            "TOTAL_RECALL_MCP_CONTEXT_MAX_RESULT_BYTES".to_owned(),
            max_bytes.to_string(),
        ),
    ])
    .unwrap();
    (
        StartupContextService::new(operations, config.context),
        calls,
    )
}

fn input(limit: Option<u32>) -> MemoryContextInput {
    MemoryContextInput { limit }
}

#[tokio::test]
async fn omitted_default_and_explicit_limits_issue_one_read_with_expected_count() {
    for (limit, expected) in [(None, 10), (Some(3), 3)] {
        let (service, calls) = service_for(Ok(Vec::new()), 128);
        let output = service.context(input(limit)).await.unwrap();
        assert_eq!(output, MemoryContextOutput { memories: vec![] });
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            &[ContextQuery {
                limit: expected,
                max_candidate_bytes: 128
            }]
        );
    }
}

#[tokio::test]
async fn configured_maximum_is_checked_before_the_single_operation() {
    let (service, calls) = service_for(Ok(Vec::new()), 128);
    let error = service
        .context(input(Some(21)))
        .await
        .expect_err("request above the configured maximum must fail");
    assert_eq!(
        error,
        ToolError::InvalidInput {
            field: OperationInputField::Limit
        }
    );
    assert!(calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn empty_output_has_exact_compact_bytes_and_successful_result() {
    let (service, _) = service_for(Ok(Vec::new()), br#"{"memories":[]}"#.len());
    let output = service.context(input(None)).await.unwrap();
    assert_eq!(serde_json::to_vec(&output).unwrap(), br#"{"memories":[]}"#);
}

#[tokio::test]
async fn exact_compact_prefix_and_oversized_or_non_fitting_candidate_terminate_selection() {
    let candidates = vec![
        memory("first", "one"),
        memory("second", "two"),
        memory("third", "three"),
    ];
    let complete = json!({"memories":[
        {"type":"fact","title":"first","content":"one","concepts":["café"]},
        {"type":"fact","title":"second","content":"two","concepts":["café"]}
    ]});
    let exact_two_bytes = serde_json::to_vec(&complete).unwrap().len();
    let (service, _) = service_for(Ok(candidates), exact_two_bytes);
    assert_eq!(
        serde_json::to_value(service.context(input(Some(3))).await.unwrap()).unwrap(),
        complete
    );

    let (service, _) = service_for(
        Ok(vec![
            memory("too-large", &"x".repeat(100)),
            memory("later", "ignored"),
        ]),
        64,
    );
    assert_eq!(
        service.context(input(None)).await.unwrap().memories.len(),
        0
    );

    let (service, _) = service_for(
        Ok(vec![
            memory("first", "one"),
            ContextCandidate::Oversized,
            memory("later", "ignored"),
        ]),
        512,
    );
    assert_eq!(
        service.context(input(None)).await.unwrap().memories.len(),
        1
    );
}

#[tokio::test]
async fn ordinary_non_fitting_candidate_stops_after_the_exact_fitting_prefix() {
    let candidates = vec![
        memory("first", "one"),
        memory("second", "two"),
        memory("third", "three"),
    ];
    let first = json!({"memories":[
        {"type":"fact","title":"first","content":"one","concepts":["café"]}
    ]});
    let max_bytes = serde_json::to_vec(&first).unwrap().len();
    let first_two = json!({"memories":[
        {"type":"fact","title":"first","content":"one","concepts":["café"]},
        {"type":"fact","title":"second","content":"two","concepts":["café"]}
    ]});
    assert!(serde_json::to_vec(&first_two).unwrap().len() > max_bytes);
    let (service, calls) = service_for(Ok(candidates), max_bytes);

    let output = service
        .context(input(Some(3)))
        .await
        .expect("the first candidate fits the configured compact byte bound");
    let encoded = serde_json::to_vec(&output).unwrap();

    assert_eq!(serde_json::to_value(&output).unwrap(), first);
    assert_eq!(encoded.len(), max_bytes);
    assert!(encoded.len() <= max_bytes);
    assert!(
        !encoded
            .windows(b"second".len())
            .any(|bytes| bytes == b"second")
    );
    assert!(
        !encoded
            .windows(b"third".len())
            .any(|bytes| bytes == b"third")
    );
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        &[ContextQuery {
            limit: 3,
            max_candidate_bytes: max_bytes,
        }]
    );
}

#[tokio::test]
async fn operation_errors_are_fixed_and_never_return_partial_content_or_retry() {
    for (error, expected) in [
        (
            OperationError::invalid_input(
                mcp_worker::operations::Operation::ListRecentContext,
                OperationInputField::Limit,
            ),
            ToolError::InvalidInput {
                field: OperationInputField::Limit,
            },
        ),
        (
            OperationError::backend_failure(
                mcp_worker::operations::Operation::ListRecentContext,
                mcp_worker::operations::BackendSource::MemoryStore,
            ),
            ToolError::BackendFailure,
        ),
        (
            OperationError::invalid_backend_response(
                mcp_worker::operations::Operation::ListRecentContext,
                mcp_worker::operations::BackendResponseField::Memory,
            ),
            ToolError::InternalFailure,
        ),
    ] {
        let (service, calls) = service_for(Err(error), 128);
        assert_eq!(service.context(input(None)).await.unwrap_err(), expected);
        assert_eq!(calls.lock().unwrap().len(), 1);
    }
}

#[test]
fn context_result_serializes_only_the_four_allowed_fields() {
    let value: Value = serde_json::to_value(MemoryContextOutput {
        memories: vec![match memory("t", "c") {
            ContextCandidate::Memory(memory) => mcp_worker::contracts::ContextMemoryDto {
                memory_type: memory.memory_type,
                title: memory.title,
                content: memory.content,
                concepts: memory.concepts,
            },
            _ => unreachable!(),
        }],
    })
    .unwrap();
    assert_eq!(
        value,
        json!({"memories":[{"type":"fact","title":"t","content":"c","concepts":["café"]}]})
    );
}
