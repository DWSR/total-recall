use harness_ingestion::config::{
    Config, ConfigError, III_NAMESPACE_ENV, III_URL_ENV, III_WORKER_NAME_ENV,
    TOTAL_RECALL_QUEUE_TOPIC_ENV,
};
use std::process::Command;

fn values(topic: Option<&str>) -> Vec<(String, String)> {
    topic
        .map(|topic| vec![(TOTAL_RECALL_QUEUE_TOPIC_ENV.to_owned(), topic.to_owned())])
        .unwrap_or_default()
}

#[test]
fn configuration_rejects_a_missing_queue_topic() {
    assert_eq!(
        Config::from_values(values(None)),
        Err(ConfigError::MissingQueueTopic)
    );
}

#[test]
fn configuration_rejects_an_empty_queue_topic() {
    assert_eq!(
        Config::from_values(values(Some(""))),
        Err(ConfigError::BlankQueueTopic)
    );
}

#[test]
fn configuration_rejects_a_whitespace_only_queue_topic() {
    assert_eq!(
        Config::from_values(values(Some(" \t\n "))),
        Err(ConfigError::BlankQueueTopic)
    );
}

#[test]
fn configuration_trims_the_queue_topic_once() {
    let config = Config::from_values(values(Some("  harness-events  "))).unwrap();

    assert_eq!(config.queue_topic.as_str(), "harness-events");
}

#[test]
fn configuration_models_the_managed_iii_environment() {
    let config = Config::from_values([
        (
            TOTAL_RECALL_QUEUE_TOPIC_ENV.to_owned(),
            "harness-events".to_owned(),
        ),
        (
            III_URL_ENV.to_owned(),
            "ws://engine.example:9000".to_owned(),
        ),
        (
            III_WORKER_NAME_ENV.to_owned(),
            "harness-ingestion".to_owned(),
        ),
        (III_NAMESPACE_ENV.to_owned(), "total-recall".to_owned()),
    ])
    .unwrap();

    assert_eq!(config.iii.engine_url, "ws://engine.example:9000");
    assert_eq!(config.iii.worker_name.as_deref(), Some("harness-ingestion"));
    assert_eq!(config.iii.namespace.as_deref(), Some("total-recall"));
}

#[test]
fn configuration_uses_the_sdk_engine_default_when_iii_url_is_absent() {
    let config = Config::from_values(values(Some("harness-events"))).unwrap();

    assert_eq!(config.iii.engine_url, "ws://127.0.0.1:49134");
    assert_eq!(config.iii.worker_name, None);
    assert_eq!(config.iii.namespace, None);
}

#[test]
fn worker_manifest_declares_identity_and_workspace_release_binary() {
    assert_eq!(
        include_str!("../iii.worker.yaml"),
        "name: harness-ingestion\nscripts:\n  start: ../../target/release/harness-ingestion\n"
    );
}

#[test]
fn invalid_startup_configuration_is_reported_before_worker_registration() {
    let output = Command::new(env!("CARGO_BIN_EXE_harness-ingestion"))
        .env_remove(TOTAL_RECALL_QUEUE_TOPIC_ENV)
        .output()
        .expect("worker binary should start");

    assert!(!output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stderr),
        "worker startup configuration failed: TOTAL_RECALL_QUEUE_TOPIC is not set\n"
    );
}
