use std::{
    io::{ErrorKind, Write},
    process::{Command, Output, Stdio},
};

use iii_sdk::DEFAULT_ENGINE_URL;
use mcp_worker::config::{
    Config, ConfigError, III_NAMESPACE_ENV, III_URL_ENV, III_WORKER_NAME_ENV, QueryEmbeddingConfig,
    QueryEmbeddingSettings, TOTAL_RECALL_EMBEDDING_MODEL_ENV, TOTAL_RECALL_EMBEDDING_PROVIDER_ENV,
    TOTAL_RECALL_MCP_CHANNEL_CAPACITY_ENV, TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS_ENV,
    TOTAL_RECALL_MCP_MAX_IN_FLIGHT_ENV, TOTAL_RECALL_MCP_MAX_LINE_BYTES_ENV,
    TOTAL_RECALL_MEMORY_DATABASE_ENV,
};

const PROVIDER_SENTINEL: &str = "provider-sentinel-7f3a";
const MODEL_SENTINEL: &str = "model-sentinel-91c4";
const TIMEOUT_SENTINEL: &str = "timeout-sentinel-5be2";
const PARTIAL_EMBEDDING_ERROR: &str = "TOTAL_RECALL_EMBEDDING_PROVIDER and TOTAL_RECALL_EMBEDDING_MODEL must both be non-blank or both be blank";
const EMBEDDING_TIMEOUT_ERROR: &str =
    "TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS must be an integer from 1 through 30000";

fn values(database: Option<&str>) -> Vec<(String, String)> {
    database
        .map(|database| {
            vec![(
                TOTAL_RECALL_MEMORY_DATABASE_ENV.to_owned(),
                database.to_owned(),
            )]
        })
        .unwrap_or_default()
}

fn embedding_values(
    provider: Option<&str>,
    model: Option<&str>,
    timeout_ms: Option<&str>,
) -> Vec<(String, String)> {
    let mut values = values(Some("total-recall-memory"));
    for (variable, value) in [
        (TOTAL_RECALL_EMBEDDING_PROVIDER_ENV, provider),
        (TOTAL_RECALL_EMBEDDING_MODEL_ENV, model),
        (TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS_ENV, timeout_ms),
    ] {
        if let Some(value) = value {
            values.push((variable.to_owned(), value.to_owned()));
        }
    }
    values
}

fn enabled_settings(config: &Config) -> &QueryEmbeddingSettings {
    match &config.query_embedding {
        QueryEmbeddingConfig::Enabled(settings) => settings,
        QueryEmbeddingConfig::Disabled => panic!("query embedding should be enabled"),
    }
}

#[test]
fn configuration_uses_the_database_target_and_default_resource_bounds() {
    let config = Config::from_values(values(Some("  total-recall-memory  "))).unwrap();

    assert_eq!(config.database.as_str(), "total-recall-memory");
    assert_eq!(config.max_line_bytes, 4_194_304);
    assert_eq!(config.channel_capacity, 32);
    assert_eq!(config.max_in_flight, 16);
    assert_eq!(config.iii.engine_url, DEFAULT_ENGINE_URL);
    assert_eq!(config.iii.worker_name, None);
    assert_eq!(config.iii.namespace, None);
}

#[test]
fn configuration_preserves_explicit_resource_bounds_and_managed_iii_settings() {
    let config = Config::from_values([
        (
            TOTAL_RECALL_MEMORY_DATABASE_ENV.to_owned(),
            "total-recall-memory".to_owned(),
        ),
        (
            TOTAL_RECALL_MCP_MAX_LINE_BYTES_ENV.to_owned(),
            "8192".to_owned(),
        ),
        (
            TOTAL_RECALL_MCP_CHANNEL_CAPACITY_ENV.to_owned(),
            "4".to_owned(),
        ),
        (
            TOTAL_RECALL_MCP_MAX_IN_FLIGHT_ENV.to_owned(),
            "2".to_owned(),
        ),
        (
            III_URL_ENV.to_owned(),
            "ws://engine.example:9000".to_owned(),
        ),
        (III_WORKER_NAME_ENV.to_owned(), "mcp-worker".to_owned()),
        (III_NAMESPACE_ENV.to_owned(), "total-recall".to_owned()),
    ])
    .unwrap();

    assert_eq!(config.max_line_bytes, 8192);
    assert_eq!(config.channel_capacity, 4);
    assert_eq!(config.max_in_flight, 2);
    assert_eq!(config.iii.engine_url, "ws://engine.example:9000");
    assert_eq!(config.iii.worker_name.as_deref(), Some("mcp-worker"));
    assert_eq!(config.iii.namespace.as_deref(), Some("total-recall"));
}

#[test]
fn configuration_defaults_whitespace_iii_url_and_namespace() {
    let config = Config::from_values([
        (
            TOTAL_RECALL_MEMORY_DATABASE_ENV.to_owned(),
            "total-recall-memory".to_owned(),
        ),
        (III_URL_ENV.to_owned(), " \t\n ".to_owned()),
        (III_NAMESPACE_ENV.to_owned(), " \t\n ".to_owned()),
    ])
    .unwrap();

    assert_eq!(config.iii.engine_url, DEFAULT_ENGINE_URL);
    assert_eq!(config.iii.namespace, None);
}

#[test]
fn configuration_distinguishes_absent_empty_and_whitespace_worker_names() {
    let absent = Config::from_values(values(Some("total-recall-memory"))).unwrap();
    let empty = Config::from_values([
        (
            TOTAL_RECALL_MEMORY_DATABASE_ENV.to_owned(),
            "total-recall-memory".to_owned(),
        ),
        (III_WORKER_NAME_ENV.to_owned(), String::new()),
    ])
    .unwrap();
    let whitespace = Config::from_values([
        (
            TOTAL_RECALL_MEMORY_DATABASE_ENV.to_owned(),
            "total-recall-memory".to_owned(),
        ),
        (III_WORKER_NAME_ENV.to_owned(), " \t\n ".to_owned()),
    ])
    .unwrap();

    assert_eq!(absent.iii.worker_name, None);
    assert_eq!(empty.iii.worker_name, None);
    assert_eq!(whitespace.iii.worker_name.as_deref(), Some(" \t\n "));
}

#[test]
fn configuration_rejects_missing_and_blank_database_targets() {
    assert_eq!(
        Config::from_values(values(None)),
        Err(ConfigError::MissingDatabase)
    );
    assert_eq!(
        Config::from_values(values(Some(" \t\n "))),
        Err(ConfigError::BlankDatabase)
    );
}

#[test]
fn configuration_rejects_malformed_and_zero_resource_bounds() {
    for (variable, value) in [
        (TOTAL_RECALL_MCP_MAX_LINE_BYTES_ENV, "not-a-number"),
        (TOTAL_RECALL_MCP_CHANNEL_CAPACITY_ENV, "not-a-number"),
        (TOTAL_RECALL_MCP_MAX_IN_FLIGHT_ENV, "not-a-number"),
        (TOTAL_RECALL_MCP_MAX_LINE_BYTES_ENV, "0"),
        (TOTAL_RECALL_MCP_CHANNEL_CAPACITY_ENV, "0"),
        (TOTAL_RECALL_MCP_MAX_IN_FLIGHT_ENV, "0"),
    ] {
        let mut values = values(Some("total-recall-memory"));
        values.push((variable.to_owned(), value.to_owned()));

        assert_eq!(
            Config::from_values(values),
            Err(ConfigError::InvalidPositiveUsize { variable })
        );
    }
}

#[test]
fn query_embedding_is_disabled_when_provider_and_model_are_both_blank() {
    let blank = [None, Some(""), Some(" \t\n ")];
    for provider in blank {
        for model in blank {
            let config = Config::from_values(embedding_values(provider, model, None)).unwrap();

            assert_eq!(
                config.query_embedding,
                QueryEmbeddingConfig::Disabled,
                "provider {provider:?} and model {model:?} should disable query embedding"
            );
        }
    }
}

#[test]
fn disabled_query_embedding_ignores_the_embedding_timeout() {
    for timeout_ms in ["not-a-number", "0", "30001", "", " \t\n "] {
        let config =
            Config::from_values(embedding_values(None, Some(" \t\n "), Some(timeout_ms))).unwrap();

        assert_eq!(config.query_embedding, QueryEmbeddingConfig::Disabled);
    }
}

#[test]
fn query_embedding_is_enabled_with_exact_identities_and_the_default_timeout() {
    let config = Config::from_values(embedding_values(
        Some("  provider-a  "),
        Some("model/b:1\t"),
        None,
    ))
    .unwrap();
    let settings = enabled_settings(&config);

    assert_eq!(settings.provider().as_str(), "  provider-a  ");
    assert_eq!(settings.model().as_str(), "model/b:1\t");
    assert_eq!(settings.timeout_ms(), 21_000);
}

#[test]
fn enabled_query_embedding_accepts_timeout_boundaries() {
    for (timeout_ms, expected) in [("1", 1), ("30000", 30_000)] {
        let config = Config::from_values(embedding_values(
            Some("provider"),
            Some("model"),
            Some(timeout_ms),
        ))
        .unwrap();

        assert_eq!(enabled_settings(&config).timeout_ms(), expected);
    }
}

#[test]
fn enabled_query_embedding_rejects_out_of_range_and_malformed_timeouts() {
    for timeout_ms in [
        "0",
        "30001",
        "",
        " \t\n ",
        " 21000",
        "-1",
        "1.5",
        "not-a-number",
        "18446744073709551616",
    ] {
        assert_eq!(
            Config::from_values(embedding_values(
                Some("provider"),
                Some("model"),
                Some(timeout_ms),
            )),
            Err(ConfigError::InvalidEmbeddingTimeout),
            "timeout {timeout_ms:?} should be rejected"
        );
    }
}

#[test]
fn partial_query_embedding_configuration_is_rejected() {
    for blank in [None, Some(""), Some(" \t\n ")] {
        for timeout_ms in [None, Some("21000")] {
            assert_eq!(
                Config::from_values(embedding_values(Some("provider"), blank, timeout_ms)),
                Err(ConfigError::PartialEmbeddingConfiguration),
                "provider without model {blank:?} should be rejected"
            );
            assert_eq!(
                Config::from_values(embedding_values(blank, Some("model"), timeout_ms)),
                Err(ConfigError::PartialEmbeddingConfiguration),
                "model without provider {blank:?} should be rejected"
            );
        }
    }
}

#[test]
fn enabled_query_embedding_keeps_the_managed_iii_environment() {
    for namespace in [None, Some("total-recall"), Some(" \t\n ")] {
        let mut disabled = values(Some("total-recall-memory"));
        let mut enabled = embedding_values(Some("provider"), Some("model"), None);
        if let Some(namespace) = namespace {
            disabled.push((III_NAMESPACE_ENV.to_owned(), namespace.to_owned()));
            enabled.push((III_NAMESPACE_ENV.to_owned(), namespace.to_owned()));
        }

        let disabled = Config::from_values(disabled).unwrap();
        let enabled = Config::from_values(enabled).unwrap();

        assert!(matches!(
            enabled.query_embedding,
            QueryEmbeddingConfig::Enabled(_)
        ));
        assert_eq!(enabled.iii, disabled.iii);
    }
}

#[test]
fn enabled_query_embedding_debug_output_redacts_provider_and_model() {
    let config = Config::from_values(embedding_values(
        Some(PROVIDER_SENTINEL),
        Some(MODEL_SENTINEL),
        None,
    ))
    .unwrap();
    let settings = enabled_settings(&config);

    assert_eq!(settings.provider().as_str(), PROVIDER_SENTINEL);
    assert_eq!(settings.model().as_str(), MODEL_SENTINEL);
    assert_eq!(
        format!("{:?}", settings.provider()),
        "EmbeddingIdentifier(<redacted>)"
    );
    assert_eq!(
        format!("{:?}", settings.model()),
        "EmbeddingIdentifier(<redacted>)"
    );
    for debug in [
        format!("{config:?}"),
        format!("{config:#?}"),
        format!("{settings:?}"),
    ] {
        assert!(
            !debug.contains(PROVIDER_SENTINEL),
            "Debug exposed the provider"
        );
        assert!(!debug.contains(MODEL_SENTINEL), "Debug exposed the model");
    }
}

#[test]
fn query_embedding_configuration_errors_are_fixed_and_content_free() {
    for (values, expected) in [
        (
            embedding_values(Some(PROVIDER_SENTINEL), None, None),
            PARTIAL_EMBEDDING_ERROR,
        ),
        (
            embedding_values(Some(" \t\n "), Some(MODEL_SENTINEL), None),
            PARTIAL_EMBEDDING_ERROR,
        ),
        (
            embedding_values(
                Some(PROVIDER_SENTINEL),
                Some(MODEL_SENTINEL),
                Some(TIMEOUT_SENTINEL),
            ),
            EMBEDDING_TIMEOUT_ERROR,
        ),
    ] {
        let error = Config::from_values(values).unwrap_err();
        let display = error.to_string();
        let debug = format!("{error:?}");

        assert_eq!(display, expected);
        for sentinel in [PROVIDER_SENTINEL, MODEL_SENTINEL, TIMEOUT_SENTINEL] {
            assert!(!display.contains(sentinel), "Display exposed a sentinel");
            assert!(!debug.contains(sentinel), "Debug exposed a sentinel");
        }
    }
}

#[test]
fn invalid_startup_reports_a_content_safe_error_without_stdout() {
    let output = worker_command()
        .env(TOTAL_RECALL_MEMORY_DATABASE_ENV, "private-memory-target")
        .env(TOTAL_RECALL_MCP_MAX_LINE_BYTES_ENV, "not-a-number")
        .output()
        .expect("worker binary should start");

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert_eq!(
        String::from_utf8_lossy(&output.stderr),
        "worker startup configuration failed: TOTAL_RECALL_MCP_MAX_LINE_BYTES must be a positive usize\n"
    );
}

#[test]
fn invalid_query_embedding_startup_fails_before_serving_protocol_requests() {
    let mut provider_only = valid_worker_command();
    provider_only.env(TOTAL_RECALL_EMBEDDING_PROVIDER_ENV, PROVIDER_SENTINEL);
    let mut model_only = valid_worker_command();
    model_only
        .env(TOTAL_RECALL_EMBEDDING_PROVIDER_ENV, " \t\n ")
        .env(TOTAL_RECALL_EMBEDDING_MODEL_ENV, MODEL_SENTINEL);
    let mut invalid_timeout = valid_worker_command();
    invalid_timeout
        .env(TOTAL_RECALL_EMBEDDING_PROVIDER_ENV, PROVIDER_SENTINEL)
        .env(TOTAL_RECALL_EMBEDDING_MODEL_ENV, MODEL_SENTINEL)
        .env(TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS_ENV, TIMEOUT_SENTINEL);

    for (case, command, error) in [
        ("provider only", provider_only, PARTIAL_EMBEDDING_ERROR),
        ("model only", model_only, PARTIAL_EMBEDDING_ERROR),
        ("invalid timeout", invalid_timeout, EMBEDDING_TIMEOUT_ERROR),
    ] {
        let output = output_after_discovery_request(command);
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert!(!output.status.success(), "{case}: startup should fail");
        assert!(output.stdout.is_empty(), "{case}: stdout should stay empty");
        assert_eq!(
            stderr,
            format!("worker startup configuration failed: {error}\n"),
            "{case}: stderr should hold one fixed diagnostic"
        );
        for sentinel in [PROVIDER_SENTINEL, MODEL_SENTINEL, TIMEOUT_SENTINEL] {
            assert!(!stderr.contains(sentinel), "{case}: stderr exposed a value");
        }
    }
}

#[test]
fn enabled_and_blank_query_embedding_startup_serve_discovery_without_a_router() {
    let mut enabled = valid_worker_command();
    enabled
        .env(TOTAL_RECALL_EMBEDDING_PROVIDER_ENV, PROVIDER_SENTINEL)
        .env(TOTAL_RECALL_EMBEDDING_MODEL_ENV, MODEL_SENTINEL)
        .env(TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS_ENV, "30000");
    let mut blank = valid_worker_command();
    blank
        .env(TOTAL_RECALL_EMBEDDING_PROVIDER_ENV, "")
        .env(TOTAL_RECALL_EMBEDDING_MODEL_ENV, " \t\n ")
        .env(TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS_ENV, TIMEOUT_SENTINEL);

    for (case, command) in [("enabled", enabled), ("blank", blank)] {
        let output = output_after_discovery_request(command);

        assert!(output.status.success(), "{case}: startup should succeed");
        assert!(output.stderr.is_empty(), "{case}: stderr should stay empty");
        assert_single_discovery_response(&output);
    }
}

#[test]
fn valid_startup_serves_discovery_then_exits_on_eof_without_a_backend() {
    let output = output_after_discovery_request(valid_worker_command());

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_single_discovery_response(&output);
}

#[test]
fn valid_startup_exits_cleanly_on_eof_without_a_backend() {
    let output = valid_worker_command()
        .output()
        .expect("worker binary should start");

    assert!(output.status.success());
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
}

fn output_after_discovery_request(mut command: Command) -> Output {
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("worker binary should start");
    let mut request = br#"{"jsonrpc":"2.0","id":"discover-request","method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}"#.to_vec();
    request.push(b'\n');
    let mut input = child.stdin.take().expect("worker stdin should be piped");
    if let Err(error) = input.write_all(&request) {
        assert_eq!(
            error.kind(),
            ErrorKind::BrokenPipe,
            "discovery request should be written unless startup already exited"
        );
    }
    drop(input);

    child
        .wait_with_output()
        .expect("worker binary should exit after EOF")
}

fn assert_single_discovery_response(output: &Output) {
    let response: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("discovery should produce one JSON response");
    assert_eq!(response["id"], "discover-request");
    assert_eq!(
        response["result"]["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
        "total-recall-mcp"
    );
}

fn valid_worker_command() -> Command {
    let mut command = worker_command();
    command
        .env(TOTAL_RECALL_MEMORY_DATABASE_ENV, "total-recall-memory")
        .env(III_URL_ENV, "ws://127.0.0.1:0");
    command
}

fn worker_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mcp-worker"));
    command
        .env_remove(TOTAL_RECALL_MCP_MAX_LINE_BYTES_ENV)
        .env_remove(TOTAL_RECALL_MCP_CHANNEL_CAPACITY_ENV)
        .env_remove(TOTAL_RECALL_MCP_MAX_IN_FLIGHT_ENV)
        .env_remove(TOTAL_RECALL_EMBEDDING_PROVIDER_ENV)
        .env_remove(TOTAL_RECALL_EMBEDDING_MODEL_ENV)
        .env_remove(TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS_ENV);
    command
}
