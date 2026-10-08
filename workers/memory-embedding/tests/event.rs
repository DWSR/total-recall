use memory_embedding::{
    EventAdapter,
    config::{
        Config, TOTAL_RECALL_EMBEDDING_DATABASE_ENV, TOTAL_RECALL_EMBEDDING_MAX_EVENT_KEYS_ENV,
        TOTAL_RECALL_EMBEDDING_MODEL_ENV, TOTAL_RECALL_EMBEDDING_PROVIDER_ENV,
        TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC_ENV,
    },
    contracts::{EmbeddingError, EventFailure, MemoryKey},
};
use serde_json::{Value, json};

const DATABASE: &str = "memory-database";
const KEY_SENTINEL: &str = "event-key-sentinel";
const UNRELATED_DATABASE_SENTINEL: &str = "unrelated-database-sentinel";
const UNKNOWN_FIELD_SENTINEL: &str = "unknown-field-sentinel";
const TRUNCATED_SENTINEL: &str = "truncated-sentinel";

fn adapter(max_event_keys: u32) -> EventAdapter {
    let config = Config::from_values([
        (
            TOTAL_RECALL_EMBEDDING_DATABASE_ENV.to_owned(),
            DATABASE.to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC_ENV.to_owned(),
            "memory-inserts".to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_PROVIDER_ENV.to_owned(),
            "embedding-router".to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_MODEL_ENV.to_owned(),
            "embedding-model".to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_MAX_EVENT_KEYS_ENV.to_owned(),
            max_event_keys.to_string(),
        ),
    ])
    .expect("event adapter test configuration should be valid");

    EventAdapter::new(&config)
}

fn valid_payload() -> Value {
    json!({
        "db": DATABASE,
        "table": "PUBLIC.MEMORIES",
        "op": "insert",
        "affected_rows": 2,
        "returning": [
            { "id": KEY_SENTINEL, "version": "1" },
            { "id": "second-event-key", "version": "2" },
        ],
        "at": 0,
        "truncated": false,
    })
}

fn assert_event_error(result: Result<Vec<MemoryKey>, EmbeddingError>, expected: EventFailure) {
    let error = result.expect_err("invalid event must be rejected");
    assert_eq!(error, EmbeddingError::event(expected));

    for output in [error.to_string(), format!("{error:?}")] {
        for sentinel in [
            KEY_SENTINEL,
            UNRELATED_DATABASE_SENTINEL,
            UNKNOWN_FIELD_SENTINEL,
            TRUNCATED_SENTINEL,
        ] {
            assert!(
                !output.contains(sentinel),
                "event diagnostic leaked protected input: {output}"
            );
        }
    }
}

#[test]
fn event_adapter_accepts_direct_canonical_insert_payloads_with_absent_or_false_truncation() {
    let adapter = adapter(2);

    for (table, truncated) in [("memories", None), ("PUBLIC.MEMORIES", Some(json!(false)))] {
        let mut payload = valid_payload();
        payload["table"] = json!(table);
        match truncated {
            Some(truncated) => payload["truncated"] = truncated,
            None => {
                payload
                    .as_object_mut()
                    .expect("payload is an object")
                    .remove("truncated");
            }
        }

        assert_eq!(
            adapter
                .adapt(payload)
                .expect("direct canonical insert payload should adapt"),
            vec![
                MemoryKey::try_new(KEY_SENTINEL, "1").expect("first key is canonical"),
                MemoryKey::try_new("second-event-key", "2").expect("second key is canonical"),
            ]
        );
    }
}

#[test]
fn event_adapter_rejects_true_null_and_nonboolean_truncation_markers() {
    let adapter = adapter(2);

    for (truncated, expected) in [
        (json!(true), EventFailure::Truncated),
        (Value::Null, EventFailure::Malformed),
        (json!(TRUNCATED_SENTINEL), EventFailure::Malformed),
    ] {
        let mut payload = valid_payload();
        payload["truncated"] = truncated;

        assert_event_error(adapter.adapt(payload), expected);
    }
}

#[test]
fn event_adapter_rejects_malformed_payloads_and_inconsistent_counts() {
    let adapter = adapter(2);
    let mut unknown_field = valid_payload();
    unknown_field
        .as_object_mut()
        .expect("payload is an object")
        .insert("unknown".to_owned(), json!(UNKNOWN_FIELD_SENTINEL));

    let mut zero_affected_rows = valid_payload();
    zero_affected_rows["affected_rows"] = json!(0);

    let mut mismatched_affected_rows = valid_payload();
    mismatched_affected_rows["affected_rows"] = json!(1);

    let mut empty_returning = valid_payload();
    empty_returning["returning"] = json!([]);

    let mut fractional_timestamp = valid_payload();
    fractional_timestamp["at"] = json!(0.5);

    for payload in [
        unknown_field,
        zero_affected_rows,
        mismatched_affected_rows,
        empty_returning,
        fractional_timestamp,
    ] {
        assert_event_error(adapter.adapt(payload), EventFailure::Malformed);
    }
}

#[test]
fn event_adapter_rejects_unrelated_database_table_and_operation() {
    let adapter = adapter(2);

    for (field, value) in [
        ("db", json!(UNRELATED_DATABASE_SENTINEL)),
        ("table", json!("other.memories")),
        ("op", json!("INSERT")),
    ] {
        let mut payload = valid_payload();
        payload[field] = value;

        assert_event_error(adapter.adapt(payload), EventFailure::Unrelated);
    }
}

#[test]
fn event_adapter_rejects_duplicate_returning_keys() {
    let adapter = adapter(2);
    let mut payload = valid_payload();
    let returning = payload["returning"]
        .as_array_mut()
        .expect("returning is an array");
    returning[1] = returning[0].clone();

    assert_event_error(adapter.adapt(payload), EventFailure::Duplicate);
}

#[test]
fn event_adapter_rejects_truncated_and_oversized_payloads() {
    let mut truncated = valid_payload();
    truncated["truncated"] = json!(true);
    assert_event_error(adapter(2).adapt(truncated), EventFailure::Truncated);

    assert_event_error(adapter(1).adapt(valid_payload()), EventFailure::Oversized);
}

#[test]
fn event_adapter_rejects_noncanonical_returning_keys() {
    let adapter = adapter(2);

    for (id, version) in [("", "1"), (KEY_SENTINEL, "0"), (KEY_SENTINEL, "01")] {
        let mut payload = valid_payload();
        let key = payload["returning"][0]
            .as_object_mut()
            .expect("returned key is an object");
        key.insert("id".to_owned(), json!(id));
        key.insert("version".to_owned(), json!(version));

        assert_event_error(adapter.adapt(payload), EventFailure::Malformed);
    }

    let mut numeric_version = valid_payload();
    numeric_version["returning"][0]["version"] = json!(1);
    assert_event_error(adapter.adapt(numeric_version), EventFailure::Malformed);
}

#[test]
fn event_adapter_validates_epoch_milliseconds_through_year_9999() {
    let adapter = adapter(2);

    for timestamp in [json!(-1), json!(253_402_300_800_000_i64), json!(i64::MAX)] {
        let mut payload = valid_payload();
        payload["at"] = timestamp;
        assert_event_error(adapter.adapt(payload), EventFailure::Malformed);
    }

    for timestamp in [json!(0), json!(253_402_300_799_999_i64)] {
        let mut payload = valid_payload();
        payload["at"] = timestamp;
        assert!(
            adapter.adapt(payload).is_ok(),
            "valid epoch milliseconds should adapt"
        );
    }
}
