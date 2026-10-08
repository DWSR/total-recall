use std::time::Duration;

use iii_sdk::DEFAULT_ENGINE_URL;
use memory_embedding::config::{
    Config, ConfigError, DEFAULT_CRON_EXPRESSION, DEFAULT_NAMESPACE, III_NAMESPACE_ENV,
    III_URL_ENV, III_WORKER_NAME_ENV, TOTAL_RECALL_EMBEDDING_CRON_EXPRESSION_ENV,
    TOTAL_RECALL_EMBEDDING_DATABASE_ENV, TOTAL_RECALL_EMBEDDING_MAX_IN_FLIGHT_ENV,
    TOTAL_RECALL_EMBEDDING_MODEL_ENV, TOTAL_RECALL_EMBEDDING_PROVIDER_ENV,
    TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC_ENV,
};
use tokio::sync::Semaphore;

fn valid_values() -> Vec<(String, String)> {
    vec![
        (
            TOTAL_RECALL_EMBEDDING_DATABASE_ENV.to_owned(),
            "total-recall-memory".to_owned(),
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
    ]
}

fn with_value(
    mut values: Vec<(String, String)>,
    variable: &str,
    value: impl Into<String>,
) -> Vec<(String, String)> {
    if let Some((_, existing)) = values.iter_mut().find(|(name, _)| name == variable) {
        *existing = value.into();
    } else {
        values.push((variable.to_owned(), value.into()));
    }

    values
}

fn with_values(
    mut values: Vec<(String, String)>,
    overrides: &[(&str, &str)],
) -> Vec<(String, String)> {
    for &(variable, value) in overrides {
        values = with_value(values, variable, value);
    }

    values
}

#[test]
fn configuration_rejects_zero_resource_and_deadline_settings() {
    for variable in [
        "TOTAL_RECALL_EMBEDDING_MAX_EVENT_KEYS",
        "TOTAL_RECALL_EMBEDDING_BATCH_LIMIT",
        "TOTAL_RECALL_EMBEDDING_RECONCILIATION_LIMIT",
        "TOTAL_RECALL_EMBEDDING_MAX_INPUT_BYTES",
        "TOTAL_RECALL_EMBEDDING_MAX_IN_FLIGHT",
        "TOTAL_RECALL_EMBEDDING_DATABASE_TIMEOUT_MS",
        "TOTAL_RECALL_EMBEDDING_ROUTER_TIMEOUT_MS",
        "TOTAL_RECALL_EMBEDDING_INVOCATION_TIMEOUT_MS",
        "TOTAL_RECALL_EMBEDDING_SHUTDOWN_TIMEOUT_MS",
    ] {
        assert!(
            Config::from_values(with_value(valid_values(), variable, "0")).is_err(),
            "{variable} must reject zero"
        );
    }
}

#[test]
fn configuration_rejects_max_in_flight_above_tokio_semaphore_capacity() {
    let over_maximum = Semaphore::MAX_PERMITS
        .checked_add(1)
        .expect("Tokio semaphore capacity must leave room for an invalid setting");

    assert_eq!(
        Config::from_values(with_value(
            valid_values(),
            TOTAL_RECALL_EMBEDDING_MAX_IN_FLIGHT_ENV,
            over_maximum.to_string(),
        )),
        Err(ConfigError::InvalidMaxInFlight)
    );
}

#[test]
fn configuration_defaults_resource_and_deadline_policy() {
    let config =
        Config::from_values(valid_values()).expect("default configuration should be valid");

    assert_eq!(config.max_event_keys, 16);
    assert_eq!(config.batch_limit, 16);
    assert_eq!(config.reconciliation_limit, 16);
    assert_eq!(config.max_input_bytes, 32_768);
    assert_eq!(config.max_in_flight, 4);
    assert_eq!(config.database_timeout, Duration::from_millis(3_000));
    assert_eq!(config.router_timeout, Duration::from_millis(21_000));
    assert_eq!(config.invocation_timeout, Duration::from_millis(28_000));
    assert_eq!(config.shutdown_timeout, Duration::from_millis(30_000));
}

#[test]
fn configuration_accepts_resource_and_deadline_boundaries() {
    let minimum = Config::from_values(with_values(
        valid_values(),
        &[
            ("TOTAL_RECALL_EMBEDDING_MAX_EVENT_KEYS", "1"),
            ("TOTAL_RECALL_EMBEDDING_BATCH_LIMIT", "1"),
            ("TOTAL_RECALL_EMBEDDING_RECONCILIATION_LIMIT", "1"),
            ("TOTAL_RECALL_EMBEDDING_MAX_INPUT_BYTES", "1"),
            ("TOTAL_RECALL_EMBEDDING_MAX_IN_FLIGHT", "1"),
            ("TOTAL_RECALL_EMBEDDING_DATABASE_TIMEOUT_MS", "1"),
            ("TOTAL_RECALL_EMBEDDING_ROUTER_TIMEOUT_MS", "1"),
            ("TOTAL_RECALL_EMBEDDING_INVOCATION_TIMEOUT_MS", "3"),
            ("TOTAL_RECALL_EMBEDDING_SHUTDOWN_TIMEOUT_MS", "1"),
        ],
    ))
    .expect("minimum positive resource and deadline settings should be valid");
    let maximum = Config::from_values(with_values(
        valid_values(),
        &[
            ("TOTAL_RECALL_EMBEDDING_MAX_EVENT_KEYS", "100"),
            ("TOTAL_RECALL_EMBEDDING_BATCH_LIMIT", "100"),
            ("TOTAL_RECALL_EMBEDDING_RECONCILIATION_LIMIT", "100"),
            ("TOTAL_RECALL_EMBEDDING_DATABASE_TIMEOUT_MS", "3000"),
            ("TOTAL_RECALL_EMBEDDING_ROUTER_TIMEOUT_MS", "23999"),
            ("TOTAL_RECALL_EMBEDDING_INVOCATION_TIMEOUT_MS", "29999"),
        ],
    ))
    .expect("designed maximum settings should be valid");

    assert_eq!(minimum.max_event_keys, 1);
    assert_eq!(minimum.batch_limit, 1);
    assert_eq!(minimum.reconciliation_limit, 1);
    assert_eq!(minimum.max_input_bytes, 1);
    assert_eq!(minimum.max_in_flight, 1);
    assert_eq!(minimum.database_timeout, Duration::from_millis(1));
    assert_eq!(minimum.router_timeout, Duration::from_millis(1));
    assert_eq!(minimum.invocation_timeout, Duration::from_millis(3));
    assert_eq!(minimum.shutdown_timeout, Duration::from_millis(1));
    assert_eq!(maximum.max_event_keys, 100);
    assert_eq!(maximum.batch_limit, 100);
    assert_eq!(maximum.reconciliation_limit, 100);
    assert_eq!(maximum.router_timeout, Duration::from_millis(23_999));
    assert_eq!(maximum.invocation_timeout, Duration::from_millis(29_999));
}

#[test]
fn configuration_rejects_out_of_range_and_overflow_resource_and_deadline_settings() {
    for (variable, value) in [
        ("TOTAL_RECALL_EMBEDDING_MAX_EVENT_KEYS", "101"),
        ("TOTAL_RECALL_EMBEDDING_BATCH_LIMIT", "101"),
        ("TOTAL_RECALL_EMBEDDING_RECONCILIATION_LIMIT", "17"),
    ] {
        assert!(
            Config::from_values(with_value(valid_values(), variable, value)).is_err(),
            "{variable}={value} must be rejected"
        );
    }

    for variable in [
        "TOTAL_RECALL_EMBEDDING_MAX_EVENT_KEYS",
        "TOTAL_RECALL_EMBEDDING_BATCH_LIMIT",
        "TOTAL_RECALL_EMBEDDING_RECONCILIATION_LIMIT",
        "TOTAL_RECALL_EMBEDDING_MAX_INPUT_BYTES",
        "TOTAL_RECALL_EMBEDDING_MAX_IN_FLIGHT",
        "TOTAL_RECALL_EMBEDDING_DATABASE_TIMEOUT_MS",
        "TOTAL_RECALL_EMBEDDING_ROUTER_TIMEOUT_MS",
        "TOTAL_RECALL_EMBEDDING_INVOCATION_TIMEOUT_MS",
        "TOTAL_RECALL_EMBEDDING_SHUTDOWN_TIMEOUT_MS",
    ] {
        assert!(
            Config::from_values(with_value(valid_values(), variable, "18446744073709551616",))
                .is_err(),
            "{variable} must reject numeric overflow"
        );
    }
}

#[test]
fn configuration_rejects_inconsistent_resource_and_deadline_policy() {
    for (variable, value) in [
        ("TOTAL_RECALL_EMBEDDING_MAX_EVENT_KEYS", "17"),
        ("TOTAL_RECALL_EMBEDDING_RECONCILIATION_LIMIT", "17"),
        ("TOTAL_RECALL_EMBEDDING_ROUTER_TIMEOUT_MS", "28000"),
        ("TOTAL_RECALL_EMBEDDING_INVOCATION_TIMEOUT_MS", "30000"),
        ("TOTAL_RECALL_EMBEDDING_INVOCATION_TIMEOUT_MS", "26999"),
    ] {
        assert!(
            Config::from_values(with_value(valid_values(), variable, value)).is_err(),
            "{variable}={value} must be rejected"
        );
    }
}

#[test]
fn configuration_redacts_resource_and_deadline_values() {
    let config = Config::from_values(with_values(
        valid_values(),
        &[
            ("TOTAL_RECALL_EMBEDDING_MAX_EVENT_KEYS", "17"),
            ("TOTAL_RECALL_EMBEDDING_BATCH_LIMIT", "17"),
            ("TOTAL_RECALL_EMBEDDING_RECONCILIATION_LIMIT", "17"),
            ("TOTAL_RECALL_EMBEDDING_MAX_INPUT_BYTES", "314159"),
            ("TOTAL_RECALL_EMBEDDING_MAX_IN_FLIGHT", "19"),
            ("TOTAL_RECALL_EMBEDDING_DATABASE_TIMEOUT_MS", "2718"),
            ("TOTAL_RECALL_EMBEDDING_ROUTER_TIMEOUT_MS", "12000"),
            ("TOTAL_RECALL_EMBEDDING_INVOCATION_TIMEOUT_MS", "20000"),
            ("TOTAL_RECALL_EMBEDDING_SHUTDOWN_TIMEOUT_MS", "23456"),
        ],
    ))
    .expect("valid resource and deadline values should be accepted");
    let error = Config::from_values(with_value(
        valid_values(),
        "TOTAL_RECALL_EMBEDDING_MAX_EVENT_KEYS",
        "resource-sentinel",
    ))
    .expect_err("invalid resource value should fail");

    let debug = format!("{config:?}");
    for value in ["17", "314159", "19", "2718", "12000", "20000", "23456"] {
        assert!(!debug.contains(value), "debug output leaked {value}");
    }
    assert!(!error.to_string().contains("resource-sentinel"));
    assert!(!format!("{error:?}").contains("resource-sentinel"));
}

#[test]
fn configuration_accepts_trimmed_destinations_and_six_or_seven_field_cron_expressions() {
    let six_field = Config::from_values(with_value(
        with_value(
            with_value(
                valid_values(),
                TOTAL_RECALL_EMBEDDING_DATABASE_ENV,
                "  total-recall-memory  ",
            ),
            TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC_ENV,
            "  memory-inserts  ",
        ),
        TOTAL_RECALL_EMBEDDING_CRON_EXPRESSION_ENV,
        "  0 30 9 1,15 May-Aug Mon,Wed,Fri  ",
    ))
    .expect("six-field expression should be valid");
    let seven_field = Config::from_values(with_value(
        valid_values(),
        TOTAL_RECALL_EMBEDDING_CRON_EXPRESSION_ENV,
        "0 30 9 1,15 May-Aug Mon,Wed,Fri 2030/2",
    ))
    .expect("seven-field expression should be valid");
    let question_mark = Config::from_values(with_value(
        valid_values(),
        TOTAL_RECALL_EMBEDDING_CRON_EXPRESSION_ENV,
        "*/15 0-59/5 8,12 ? Jan-Mar Mon-Fri",
    ))
    .expect("question-mark day-of-month expression should be valid");

    assert_eq!(six_field.database.as_str(), "total-recall-memory");
    assert_eq!(six_field.queue_topic, "memory-inserts");
    assert_eq!(six_field.cron_expression, "0 30 9 1,15 May-Aug Mon,Wed,Fri");
    assert_eq!(six_field.provider, "embedding-router");
    assert_eq!(six_field.model, "embedding-model");
    assert_eq!(
        seven_field.cron_expression,
        "0 30 9 1,15 May-Aug Mon,Wed,Fri 2030/2"
    );
    assert_eq!(
        question_mark.cron_expression,
        "*/15 0-59/5 8,12 ? Jan-Mar Mon-Fri"
    );
}

#[test]
fn configuration_uses_the_default_cron_expression() {
    let config =
        Config::from_values(valid_values()).expect("default configuration should be valid");

    assert_eq!(config.cron_expression, DEFAULT_CRON_EXPRESSION);
}

#[test]
fn configuration_accepts_cron_worker_steps_and_delimiter_whitespace() {
    let arbitrary_positive_step = Config::from_values(with_value(
        valid_values(),
        TOTAL_RECALL_EMBEDDING_CRON_EXPRESSION_ENV,
        "*/60 * * * * *",
    ))
    .expect("the cron worker accepts positive steps above a field maximum");
    let delimiter_whitespace = Config::from_values(with_value(
        valid_values(),
        TOTAL_RECALL_EMBEDDING_CRON_EXPRESSION_ENV,
        "0, 30 * * * * *",
    ))
    .expect("the cron worker accepts whitespace after a list delimiter");

    assert_eq!(arbitrary_positive_step.cron_expression, "*/60 * * * * *");
    assert_eq!(delimiter_whitespace.cron_expression, "0, 30 * * * * *");
}

#[test]
fn configuration_rejects_missing_and_blank_required_values() {
    for (variable, expected) in [
        (
            TOTAL_RECALL_EMBEDDING_DATABASE_ENV,
            ConfigError::MissingDatabase,
        ),
        (
            TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC_ENV,
            ConfigError::MissingQueueTopic,
        ),
        (
            TOTAL_RECALL_EMBEDDING_PROVIDER_ENV,
            ConfigError::MissingProvider,
        ),
        (TOTAL_RECALL_EMBEDDING_MODEL_ENV, ConfigError::MissingModel),
    ] {
        let mut values = valid_values();
        values.retain(|(name, _)| name != variable);

        assert_eq!(Config::from_values(values), Err(expected));
    }

    for (variable, expected) in [
        (
            TOTAL_RECALL_EMBEDDING_DATABASE_ENV,
            ConfigError::BlankDatabase,
        ),
        (
            TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC_ENV,
            ConfigError::BlankQueueTopic,
        ),
        (
            TOTAL_RECALL_EMBEDDING_CRON_EXPRESSION_ENV,
            ConfigError::BlankCronExpression,
        ),
        (
            TOTAL_RECALL_EMBEDDING_PROVIDER_ENV,
            ConfigError::BlankProvider,
        ),
        (TOTAL_RECALL_EMBEDDING_MODEL_ENV, ConfigError::BlankModel),
    ] {
        assert_eq!(
            Config::from_values(with_value(valid_values(), variable, " \t\n ")),
            Err(expected)
        );
    }
}

#[test]
fn configuration_rejects_malformed_destinations_and_cron_expressions() {
    assert_eq!(
        Config::from_values(with_value(
            valid_values(),
            TOTAL_RECALL_EMBEDDING_DATABASE_ENV,
            "memory\0target",
        )),
        Err(ConfigError::InvalidDatabase)
    );
    assert_eq!(
        Config::from_values(with_value(
            valid_values(),
            TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC_ENV,
            "memory\0inserts",
        )),
        Err(ConfigError::InvalidQueueTopic)
    );
    assert_eq!(
        Config::from_values(with_value(
            valid_values(),
            TOTAL_RECALL_EMBEDDING_PROVIDER_ENV,
            "provider\0name",
        )),
        Err(ConfigError::InvalidProvider)
    );
    assert_eq!(
        Config::from_values(with_value(
            valid_values(),
            TOTAL_RECALL_EMBEDDING_MODEL_ENV,
            "model\0name",
        )),
        Err(ConfigError::InvalidModel)
    );

    for expression in [
        "* * * * *",
        "60 * * * * *",
        "*/0 * * * * *",
        "*, * * * * * *",
        "+1 * * * * *",
        "0 * * * * Funday",
        "? * * * * *",
        "0 * * * * * * *",
    ] {
        assert_eq!(
            Config::from_values(with_value(
                valid_values(),
                TOTAL_RECALL_EMBEDDING_CRON_EXPRESSION_ENV,
                expression,
            )),
            Err(ConfigError::InvalidCronExpression)
        );
    }
}

#[test]
fn configuration_defaults_managed_iii_values_and_preserves_worker_name_whitespace() {
    let absent = Config::from_values(valid_values()).expect("defaults should be valid");
    let empty_worker_name = Config::from_values(with_value(
        valid_values(),
        III_WORKER_NAME_ENV,
        String::new(),
    ))
    .expect("an empty worker name should be unset");
    let whitespace = Config::from_values(with_value(
        with_value(
            with_value(valid_values(), III_URL_ENV, " \t\n "),
            III_WORKER_NAME_ENV,
            " \t\n ",
        ),
        III_NAMESPACE_ENV,
        " \t\n ",
    ))
    .expect("whitespace iii defaults should be valid");
    let explicit = Config::from_values(with_value(
        with_value(valid_values(), III_URL_ENV, " ws://engine.example:9000 "),
        III_NAMESPACE_ENV,
        DEFAULT_NAMESPACE,
    ))
    .expect("explicit default namespace should be valid");

    assert_eq!(absent.iii.engine_url, DEFAULT_ENGINE_URL);
    assert_eq!(absent.iii.worker_name, None);
    assert_eq!(absent.iii.namespace, DEFAULT_NAMESPACE);
    assert_eq!(empty_worker_name.iii.worker_name, None);
    assert_eq!(whitespace.iii.engine_url, DEFAULT_ENGINE_URL);
    assert_eq!(whitespace.iii.worker_name.as_deref(), Some(" \t\n "));
    assert_eq!(whitespace.iii.namespace, DEFAULT_NAMESPACE);
    assert_eq!(explicit.iii.engine_url, " ws://engine.example:9000 ");
    assert_eq!(explicit.iii.namespace, DEFAULT_NAMESPACE);
}

#[test]
fn configuration_rejects_non_default_effective_namespaces() {
    for namespace in ["memory", " default", "default "] {
        assert_eq!(
            Config::from_values(with_value(valid_values(), III_NAMESPACE_ENV, namespace)),
            Err(ConfigError::InvalidNamespace)
        );
    }
}

#[test]
fn configuration_diagnostics_redact_provider_model_and_routing_values() {
    let database = "database-sentinel";
    let queue_topic = "queue-sentinel";
    let provider = "provider-sentinel";
    let model = "model-sentinel";
    let engine_url = "wss://url-sentinel.example";
    let worker_name = "worker-sentinel";
    let config = Config::from_values([
        (
            TOTAL_RECALL_EMBEDDING_DATABASE_ENV.to_owned(),
            database.to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC_ENV.to_owned(),
            queue_topic.to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_PROVIDER_ENV.to_owned(),
            provider.to_owned(),
        ),
        (
            TOTAL_RECALL_EMBEDDING_MODEL_ENV.to_owned(),
            model.to_owned(),
        ),
        (III_URL_ENV.to_owned(), engine_url.to_owned()),
        (III_WORKER_NAME_ENV.to_owned(), worker_name.to_owned()),
    ])
    .expect("sentinel configuration should be valid");
    let error = Config::from_values(with_value(
        valid_values(),
        III_NAMESPACE_ENV,
        "namespace-sentinel",
    ))
    .expect_err("non-default namespace should fail");

    let debug = format!("{config:?}");
    for sentinel in [
        database,
        queue_topic,
        provider,
        model,
        engine_url,
        worker_name,
    ] {
        assert!(!debug.contains(sentinel), "debug output leaked {sentinel}");
    }
    assert_eq!(error.to_string(), "III_NAMESPACE must be default");
    assert!(!error.to_string().contains("namespace-sentinel"));
    assert!(!format!("{error:?}").contains("namespace-sentinel"));
}
