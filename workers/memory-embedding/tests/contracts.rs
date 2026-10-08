use std::time::{Duration, Instant};

use memory_embedding::contracts::{
    CanonicalEmbeddingInput, ConfigFailure, Deadline, EmbeddingError, EmbeddingOutcome,
    EmbeddingRouter, EmbeddingWorkItem, EmbeddingWorkRepository, EmbeddingWriter, ErrorStage,
    EventFailure, FailingEmbeddingRouter, FailingEmbeddingWorkRepository, FailingEmbeddingWriter,
    FailureReason, GeneratedEmbedding, LoadedEmbeddingWork, MemoryKey, ReconciliationOutcome,
    RecordingEmbeddingRouter, RecordingEmbeddingWorkRepository, RecordingEmbeddingWriter,
    RenderFailure, RepositoryCall, RepositoryFailure, RepositoryFailures, RepositoryResponses,
    RouterCall, RouterFailure, RouterFailures, RouterResponses, RowChangedEvent, RuntimeFailure,
    WriteOutcome, WriterCall, WriterFailure, WriterFailures, WriterResponses,
    parse_row_changed_event,
};
use serde_json::{Value, json};

const KEY_SENTINEL: &str = "key-sentinel";
const TITLE_SENTINEL: &str = "title-sentinel";
const CONTENT_SENTINEL: &str = "content-sentinel";
const CONCEPT_SENTINEL: &str = "concept-sentinel";
const INPUT_SENTINEL: &str = "canonical-input-sentinel";
const VECTOR_SENTINEL: &str = "31415.25";
const VECTOR_VALUE: f32 = 31_415.25;
const PROVIDER_SENTINEL: &str = "provider-sentinel";
const MODEL_SENTINEL: &str = "model-sentinel";
const BACKEND_SENTINEL: &str = "backend-sentinel";
const EVENT_SENTINEL: &str = "event-sentinel";

fn valid_event_payload() -> Value {
    json!({
        "db": "memory-database",
        "table": "public.memories",
        "op": "insert",
        "affected_rows": 1,
        "returning": [{ "id": KEY_SENTINEL, "version": "42" }],
        "at": 1_726_765_600_000_i64,
        "truncated": false,
    })
}

fn memory_key() -> MemoryKey {
    MemoryKey::try_new(KEY_SENTINEL, "42").expect("a canonical key should be accepted")
}

fn work_item(key: MemoryKey) -> EmbeddingWorkItem {
    EmbeddingWorkItem::new(
        key,
        TITLE_SENTINEL,
        CONTENT_SENTINEL,
        vec![CONCEPT_SENTINEL.to_owned(), "second-concept".to_owned()],
    )
}

fn deadline() -> Deadline {
    Deadline::at(Instant::now() + Duration::from_secs(1))
}

fn assert_content_safe(output: &str) {
    for sentinel in [
        KEY_SENTINEL,
        TITLE_SENTINEL,
        CONTENT_SENTINEL,
        CONCEPT_SENTINEL,
        INPUT_SENTINEL,
        VECTOR_SENTINEL,
        PROVIDER_SENTINEL,
        MODEL_SENTINEL,
        BACKEND_SENTINEL,
        EVENT_SENTINEL,
    ] {
        assert!(
            !output.contains(sentinel),
            "output leaked protected sentinel {sentinel}: {output}"
        );
    }
}

#[test]
fn direct_row_change_deserialization_failures_are_opaque() {
    for (field, value) in [
        ("affected_rows", json!(EVENT_SENTINEL)),
        ("truncated", Value::Null),
        ("truncated", json!(EVENT_SENTINEL)),
    ] {
        let mut payload = valid_event_payload();
        payload
            .as_object_mut()
            .expect("payload is an object")
            .insert(field.to_owned(), value);

        let error = serde_json::from_value::<RowChangedEvent>(payload)
            .expect_err("an invalid row-change field must fail typed deserialization");

        assert_eq!(error.to_string(), "event:malformed");
        assert_content_safe(&error.to_string());
        assert_content_safe(&format!("{error:?}"));
    }
}

#[test]
fn direct_row_change_conversion_failures_are_opaque() {
    let mut payload = valid_event_payload();
    payload
        .get_mut("returning")
        .and_then(Value::as_array_mut)
        .expect("returning is an array")[0]
        .as_object_mut()
        .expect("returning entry is an object")
        .insert("version".to_owned(), json!(EVENT_SENTINEL));

    let error = serde_json::from_value::<RowChangedEvent>(payload)
        .expect_err("a noncanonical returned version must fail typed deserialization");
    let display = error.to_string();
    let debug = format!("{error:?}");

    assert_eq!(display, "event:malformed");
    assert!(
        !display.contains(EVENT_SENTINEL),
        "display leaked event sentinel: {display}"
    );
    assert!(
        !debug.contains(EVENT_SENTINEL),
        "debug leaked event sentinel: {debug}"
    );
}

#[test]
fn row_change_contract_accepts_only_the_durable_key_payload_shape() {
    let direct_event: RowChangedEvent = serde_json::from_value(valid_event_payload())
        .expect("the typed serde boundary should accept the durable payload");
    let event = parse_row_changed_event(valid_event_payload())
        .expect("the durable row-change payload should be accepted");

    assert_eq!(direct_event, event);
    assert_eq!(event.db(), "memory-database");
    assert_eq!(event.table(), "public.memories");
    assert_eq!(event.op(), "insert");
    assert_eq!(event.affected_rows(), 1);
    assert_eq!(event.returning().len(), 1);
    assert_eq!(event.returning()[0].id(), KEY_SENTINEL);
    assert_eq!(event.returning()[0].version(), 42);
    assert_eq!(event.at(), 1_726_765_600_000_i64);
    assert_eq!(event.truncated(), Some(false));

    let mut optional_truncation = valid_event_payload();
    optional_truncation
        .as_object_mut()
        .expect("payload is an object")
        .remove("truncated");
    assert_eq!(
        parse_row_changed_event(optional_truncation)
            .expect("absent optional truncation marker should be accepted")
            .truncated(),
        None
    );

    for protected_field in ["title", "content", "concepts", "vector"] {
        let mut payload = valid_event_payload();
        payload
            .as_object_mut()
            .expect("payload is an object")
            .insert(protected_field.to_owned(), json!(TITLE_SENTINEL));
        assert!(
            serde_json::from_value::<RowChangedEvent>(payload.clone()).is_err(),
            "outer protected field {protected_field} must fail typed deserialization"
        );

        assert_eq!(
            parse_row_changed_event(payload),
            Err(EmbeddingError::event(EventFailure::Malformed)),
            "outer protected field {protected_field} must be rejected"
        );

        let mut payload = valid_event_payload();
        payload
            .get_mut("returning")
            .and_then(Value::as_array_mut)
            .expect("returning is an array")[0]
            .as_object_mut()
            .expect("returning entry is an object")
            .insert(protected_field.to_owned(), json!(CONTENT_SENTINEL));
        assert!(
            serde_json::from_value::<RowChangedEvent>(payload.clone()).is_err(),
            "returned key field {protected_field} must fail typed deserialization"
        );

        assert_eq!(
            parse_row_changed_event(payload),
            Err(EmbeddingError::event(EventFailure::Malformed)),
            "returned key field {protected_field} must be rejected"
        );
    }

    let mut payload = valid_event_payload();
    payload
        .as_object_mut()
        .expect("payload is an object")
        .insert("unexpected".to_owned(), json!(TITLE_SENTINEL));
    assert!(
        serde_json::from_value::<RowChangedEvent>(payload.clone()).is_err(),
        "unknown fields must fail typed deserialization"
    );
    assert_eq!(
        parse_row_changed_event(payload),
        Err(EmbeddingError::event(EventFailure::Malformed)),
        "unknown fields must be rejected"
    );

    let mut payload = valid_event_payload();
    payload
        .get_mut("returning")
        .and_then(Value::as_array_mut)
        .expect("returning is an array")[0]
        .as_object_mut()
        .expect("returning entry is an object")
        .insert("version".to_owned(), json!(42));
    assert!(
        serde_json::from_value::<RowChangedEvent>(payload.clone()).is_err(),
        "numeric versions must fail typed deserialization"
    );
    assert_eq!(
        parse_row_changed_event(payload),
        Err(EmbeddingError::event(EventFailure::Malformed)),
        "numeric versions must be rejected"
    );
}

#[test]
fn memory_keys_require_nonempty_ids_and_positive_canonical_decimal_versions() {
    let key = memory_key();
    assert_eq!(key.id(), KEY_SENTINEL);
    assert_eq!(key.version(), 42);

    for (id, version) in [
        ("", "1"),
        ("key\0sentinel", "1"),
        (KEY_SENTINEL, ""),
        (KEY_SENTINEL, "0"),
        (KEY_SENTINEL, "00"),
        (KEY_SENTINEL, "01"),
        (KEY_SENTINEL, "+1"),
        (KEY_SENTINEL, "-1"),
        (KEY_SENTINEL, "1.0"),
        (KEY_SENTINEL, " 1"),
        (KEY_SENTINEL, "1 "),
        (KEY_SENTINEL, "999999999999999999999999999999999999999"),
    ] {
        assert_eq!(
            MemoryKey::try_new(id, version),
            Err(EmbeddingError::event(EventFailure::Malformed)),
            "{id:?}/{version:?} must be rejected"
        );
    }
}

#[test]
fn work_and_router_contracts_preserve_only_validated_associations() {
    let key = memory_key();
    let item = work_item(key.clone());
    let input = CanonicalEmbeddingInput::new(key.clone(), INPUT_SENTINEL);
    let generated = GeneratedEmbedding::try_new(key.clone(), vec![VECTOR_VALUE])
        .expect("finite non-zero float32 vectors should be accepted");
    let maximum = GeneratedEmbedding::try_new(key.clone(), vec![1.0; 16_000])
        .expect("the maximum vector dimension should be accepted");

    assert_eq!(item.key(), &key);
    assert_eq!(item.title(), TITLE_SENTINEL);
    assert_eq!(item.content(), CONTENT_SENTINEL);
    assert_eq!(item.concepts(), [CONCEPT_SENTINEL, "second-concept"]);
    assert_eq!(input.key(), &key);
    assert_eq!(input.text(), INPUT_SENTINEL);
    assert_eq!(generated.key(), &key);
    assert_eq!(generated.vector(), [31_415.25]);
    assert_eq!(maximum.vector().len(), 16_000);

    for vector in [
        Vec::new(),
        vec![0.0],
        vec![f32::NAN],
        vec![f32::INFINITY],
        vec![1.0; 16_001],
    ] {
        assert_eq!(
            GeneratedEmbedding::try_new(key.clone(), vector),
            Err(EmbeddingError::router(RouterFailure::InvalidVector)),
            "invalid vectors must be rejected"
        );
    }

    for output in [
        format!("{key:?}"),
        format!("{item:?}"),
        format!("{input:?}"),
        format!("{generated:?}"),
    ] {
        assert_content_safe(&output);
    }
}

#[test]
fn outcomes_serialize_only_content_free_counts() {
    let outcome = EmbeddingOutcome::new(4, 3, 1, 2);
    let reconciliation: ReconciliationOutcome = EmbeddingOutcome::new(0, 0, 0, 0);

    assert_eq!(
        serde_json::to_value(outcome).expect("outcome should serialize"),
        json!({
            "selected": 4,
            "generated": 3,
            "already_present": 1,
            "stored": 2,
        })
    );
    assert_eq!(
        serde_json::to_value(reconciliation).expect("outcome should serialize"),
        json!({
            "selected": 0,
            "generated": 0,
            "already_present": 0,
            "stored": 0,
        })
    );
}

#[test]
fn closed_errors_expose_only_stable_valid_stage_reason_pairs() {
    let errors = [
        (
            EmbeddingError::config(ConfigFailure::Missing),
            ErrorStage::Config,
            FailureReason::Missing,
        ),
        (
            EmbeddingError::config(ConfigFailure::Blank),
            ErrorStage::Config,
            FailureReason::Blank,
        ),
        (
            EmbeddingError::config(ConfigFailure::Malformed),
            ErrorStage::Config,
            FailureReason::Malformed,
        ),
        (
            EmbeddingError::config(ConfigFailure::Inconsistent),
            ErrorStage::Config,
            FailureReason::Inconsistent,
        ),
        (
            EmbeddingError::event(EventFailure::Malformed),
            ErrorStage::Event,
            FailureReason::Malformed,
        ),
        (
            EmbeddingError::event(EventFailure::Unrelated),
            ErrorStage::Event,
            FailureReason::Unrelated,
        ),
        (
            EmbeddingError::event(EventFailure::Truncated),
            ErrorStage::Event,
            FailureReason::Truncated,
        ),
        (
            EmbeddingError::event(EventFailure::Oversized),
            ErrorStage::Event,
            FailureReason::Oversized,
        ),
        (
            EmbeddingError::event(EventFailure::Duplicate),
            ErrorStage::Event,
            FailureReason::Duplicate,
        ),
        (
            EmbeddingError::repository(RepositoryFailure::Missing),
            ErrorStage::Repository,
            FailureReason::Missing,
        ),
        (
            EmbeddingError::repository(RepositoryFailure::Timeout),
            ErrorStage::Repository,
            FailureReason::Timeout,
        ),
        (
            EmbeddingError::repository(RepositoryFailure::Backend),
            ErrorStage::Repository,
            FailureReason::Backend,
        ),
        (
            EmbeddingError::repository(RepositoryFailure::MalformedResponse),
            ErrorStage::Repository,
            FailureReason::MalformedResponse,
        ),
        (
            EmbeddingError::render(RenderFailure::InputTooLarge),
            ErrorStage::Render,
            FailureReason::InputTooLarge,
        ),
        (
            EmbeddingError::render(RenderFailure::Serialization),
            ErrorStage::Render,
            FailureReason::Serialization,
        ),
        (
            EmbeddingError::router(RouterFailure::Timeout),
            ErrorStage::Router,
            FailureReason::Timeout,
        ),
        (
            EmbeddingError::router(RouterFailure::Remote),
            ErrorStage::Router,
            FailureReason::Remote,
        ),
        (
            EmbeddingError::router(RouterFailure::ProviderMismatch),
            ErrorStage::Router,
            FailureReason::ProviderMismatch,
        ),
        (
            EmbeddingError::router(RouterFailure::ModelMismatch),
            ErrorStage::Router,
            FailureReason::ModelMismatch,
        ),
        (
            EmbeddingError::router(RouterFailure::CountMismatch),
            ErrorStage::Router,
            FailureReason::CountMismatch,
        ),
        (
            EmbeddingError::router(RouterFailure::InvalidVector),
            ErrorStage::Router,
            FailureReason::InvalidVector,
        ),
        (
            EmbeddingError::writer(WriterFailure::MissingParent),
            ErrorStage::Writer,
            FailureReason::MissingParent,
        ),
        (
            EmbeddingError::writer(WriterFailure::Timeout),
            ErrorStage::Writer,
            FailureReason::Timeout,
        ),
        (
            EmbeddingError::writer(WriterFailure::Backend),
            ErrorStage::Writer,
            FailureReason::Backend,
        ),
        (
            EmbeddingError::writer(WriterFailure::MalformedResponse),
            ErrorStage::Writer,
            FailureReason::MalformedResponse,
        ),
        (
            EmbeddingError::runtime(RuntimeFailure::Registration),
            ErrorStage::Runtime,
            FailureReason::Registration,
        ),
        (
            EmbeddingError::runtime(RuntimeFailure::Ownership),
            ErrorStage::Runtime,
            FailureReason::Ownership,
        ),
        (
            EmbeddingError::runtime(RuntimeFailure::Trigger),
            ErrorStage::Runtime,
            FailureReason::Trigger,
        ),
        (
            EmbeddingError::runtime(RuntimeFailure::Shutdown),
            ErrorStage::Runtime,
            FailureReason::Shutdown,
        ),
    ];

    assert_eq!(errors.len(), 29);

    for (error, stage, reason) in errors {
        assert_eq!(error.stage(), stage);
        assert_eq!(error.reason(), reason);
        assert_eq!(
            error.to_string(),
            format!("{}:{}", stage.as_str(), reason.as_str())
        );
        assert_eq!(
            format!("{error:?}"),
            format!(
                "EmbeddingError {{ stage: {}, reason: {} }}",
                stage.as_str(),
                reason.as_str()
            )
        );
        assert_content_safe(&error.to_string());
        assert_content_safe(&format!("{error:?}"));
    }
}

#[tokio::test]
async fn recording_and_failing_ports_are_deterministic_and_content_safe() {
    let key = memory_key();
    let item = work_item(key.clone());
    let input = CanonicalEmbeddingInput::new(key.clone(), INPUT_SENTINEL);
    let generated = GeneratedEmbedding::try_new(key.clone(), vec![1.0])
        .expect("valid generated embedding should construct");
    let deadline = deadline();

    let loaded = vec![
        LoadedEmbeddingWork::Pending(item.clone()),
        LoadedEmbeddingWork::AlreadyPresent(key.clone()),
        LoadedEmbeddingWork::Missing(key.clone()),
    ];
    let repository = RecordingEmbeddingWorkRepository::new(RepositoryResponses {
        load_keys: Ok(loaded.clone()),
        list_missing: Ok(vec![item.clone()]),
    });
    assert_eq!(
        repository
            .load_keys(std::slice::from_ref(&key), deadline)
            .await,
        Ok(loaded)
    );
    assert_eq!(
        repository.list_missing(7, deadline).await,
        Ok(vec![item.clone()])
    );
    assert_eq!(
        repository.calls(),
        vec![
            RepositoryCall::LoadKeys {
                keys: vec![key.clone()],
                deadline,
            },
            RepositoryCall::ListMissing { limit: 7, deadline },
        ]
    );

    let router = RecordingEmbeddingRouter::new(RouterResponses {
        embed: Ok(vec![generated.clone()]),
    });
    assert_eq!(
        router.embed(std::slice::from_ref(&input), deadline).await,
        Ok(vec![generated])
    );
    assert_eq!(
        router.calls(),
        vec![RouterCall::Embed {
            inputs: vec![input.clone()],
            deadline,
        }]
    );

    let writer = RecordingEmbeddingWriter::new(WriterResponses {
        insert: Ok(WriteOutcome::Stored),
    });
    assert_eq!(
        writer.insert(key.clone(), vec![1.0, -2.0], deadline).await,
        Ok(WriteOutcome::Stored)
    );
    assert_eq!(
        writer.calls(),
        vec![WriterCall::Insert {
            key: key.clone(),
            vector: vec![1.0, -2.0],
            deadline,
        }]
    );

    let failing_repository = FailingEmbeddingWorkRepository::new(RepositoryFailures {
        load_keys: EmbeddingError::repository(RepositoryFailure::Backend),
        list_missing: EmbeddingError::repository(RepositoryFailure::Timeout),
    });
    assert_eq!(
        failing_repository
            .load_keys(std::slice::from_ref(&key), deadline)
            .await,
        Err(EmbeddingError::repository(RepositoryFailure::Backend))
    );
    assert_eq!(
        failing_repository.list_missing(1, deadline).await,
        Err(EmbeddingError::repository(RepositoryFailure::Timeout))
    );
    assert_eq!(failing_repository.calls().len(), 2);

    let failing_router = FailingEmbeddingRouter::new(RouterFailures {
        embed: EmbeddingError::router(RouterFailure::Remote),
    });
    assert_eq!(
        failing_router
            .embed(std::slice::from_ref(&input), deadline)
            .await,
        Err(EmbeddingError::router(RouterFailure::Remote))
    );
    assert_eq!(failing_router.calls().len(), 1);

    let failing_writer = FailingEmbeddingWriter::new(WriterFailures {
        insert: EmbeddingError::writer(WriterFailure::Backend),
    });
    assert_eq!(
        failing_writer.insert(key, vec![1.0], deadline).await,
        Err(EmbeddingError::writer(WriterFailure::Backend))
    );
    assert_eq!(failing_writer.calls().len(), 1);

    for output in [
        format!("{:?}", repository.calls()),
        format!("{:?}", router.calls()),
        format!("{:?}", writer.calls()),
        format!("{:?}", failing_repository.calls()),
        format!("{:?}", failing_router.calls()),
        format!("{:?}", failing_writer.calls()),
    ] {
        assert_content_safe(&output);
    }
}
