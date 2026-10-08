use std::process::Command;

use harness_event_persistence::config::{
    Config, ConfigError, III_NAMESPACE_ENV, III_URL_ENV, III_WORKER_NAME_ENV,
    TOTAL_RECALL_DATABASE_ENV, TOTAL_RECALL_QUEUE_TOPIC_ENV,
};
use iii_sdk::DEFAULT_ENGINE_URL;

fn values(topic: Option<&str>, database: Option<&str>) -> Vec<(String, String)> {
    let mut values = Vec::new();

    if let Some(topic) = topic {
        values.push((TOTAL_RECALL_QUEUE_TOPIC_ENV.to_owned(), topic.to_owned()));
    }
    if let Some(database) = database {
        values.push((TOTAL_RECALL_DATABASE_ENV.to_owned(), database.to_owned()));
    }

    values
}

#[test]
fn configuration_rejects_a_missing_queue_topic() {
    assert_eq!(
        Config::from_values(values(None, Some("total-recall"))),
        Err(ConfigError::MissingQueueTopic)
    );
}

#[test]
fn configuration_rejects_an_empty_queue_topic() {
    assert_eq!(
        Config::from_values(values(Some(""), Some("total-recall"))),
        Err(ConfigError::BlankQueueTopic)
    );
}

#[test]
fn configuration_rejects_a_whitespace_only_queue_topic() {
    assert_eq!(
        Config::from_values(values(Some(" \t\n "), Some("total-recall"))),
        Err(ConfigError::BlankQueueTopic)
    );
}

#[test]
fn configuration_rejects_a_missing_database() {
    assert_eq!(
        Config::from_values(values(Some("harness-events"), None)),
        Err(ConfigError::MissingDatabase)
    );
}

#[test]
fn configuration_rejects_an_empty_database() {
    assert_eq!(
        Config::from_values(values(Some("harness-events"), Some(""))),
        Err(ConfigError::BlankDatabase)
    );
}

#[test]
fn configuration_rejects_a_whitespace_only_database() {
    assert_eq!(
        Config::from_values(values(Some("harness-events"), Some(" \t\n "))),
        Err(ConfigError::BlankDatabase)
    );
}

#[test]
fn configuration_trims_required_destinations_once() {
    let config =
        Config::from_values(values(Some("  harness-events  "), Some("  total-recall  "))).unwrap();

    assert_eq!(config.queue_topic, "harness-events");
    assert_eq!(config.database, "total-recall");
}

#[test]
fn configuration_defaults_absent_and_blank_engine_urls() {
    let absent = Config::from_values(values(Some("harness-events"), Some("total-recall"))).unwrap();
    let blank = Config::from_values([
        (
            TOTAL_RECALL_QUEUE_TOPIC_ENV.to_owned(),
            "harness-events".to_owned(),
        ),
        (
            TOTAL_RECALL_DATABASE_ENV.to_owned(),
            "total-recall".to_owned(),
        ),
        (III_URL_ENV.to_owned(), " \t\n ".to_owned()),
    ])
    .unwrap();

    assert_eq!(absent.iii.engine_url, DEFAULT_ENGINE_URL);
    assert_eq!(blank.iii.engine_url, DEFAULT_ENGINE_URL);
}

#[test]
fn configuration_preserves_optional_managed_identity() {
    let config = Config::from_values([
        (
            TOTAL_RECALL_QUEUE_TOPIC_ENV.to_owned(),
            "harness-events".to_owned(),
        ),
        (
            TOTAL_RECALL_DATABASE_ENV.to_owned(),
            "total-recall".to_owned(),
        ),
        (
            III_WORKER_NAME_ENV.to_owned(),
            "harness-event-persistence".to_owned(),
        ),
        (III_NAMESPACE_ENV.to_owned(), "total-recall".to_owned()),
    ])
    .unwrap();

    assert_eq!(
        config.iii.worker_name.as_deref(),
        Some("harness-event-persistence")
    );
    assert_eq!(config.iii.namespace.as_deref(), Some("total-recall"));
}

#[test]
fn worker_manifest_declares_identity_and_workspace_release_binary() {
    assert_eq!(
        include_str!("../iii.worker.yaml"),
        "name: harness-event-persistence\nscripts:\n  start: ../../target/release/harness-event-persistence\n"
    );
}

#[test]
fn invalid_startup_configuration_is_reported_before_worker_registration() {
    let output = Command::new(env!("CARGO_BIN_EXE_harness-event-persistence"))
        .env(TOTAL_RECALL_QUEUE_TOPIC_ENV, "harness-events")
        .env_remove(TOTAL_RECALL_DATABASE_ENV)
        .output()
        .expect("worker binary should start");

    assert!(!output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stderr),
        "worker startup configuration failed: TOTAL_RECALL_DATABASE is not set\n"
    );
}
