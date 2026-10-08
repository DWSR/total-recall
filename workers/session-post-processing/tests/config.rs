use std::{fmt::Display, time::Duration};

use serde::Serialize;
use session_post_processing::config::{
    ApiToken, Config, DEFAULT_ANTHROPIC_BASE_URL, DEFAULT_BATCH_LIMIT, DEFAULT_CONCURRENCY,
    DEFAULT_CRON_EXPRESSION, DEFAULT_LEASE_SECONDS, DEFAULT_MODEL_ATTEMPTS,
    DEFAULT_MODEL_CHUNK_BYTES, DEFAULT_MODEL_OUTPUT_TOKENS, DEFAULT_MODEL_TIMEOUT_SECONDS,
    DEFAULT_OPENAI_BASE_URL, DEFAULT_SHUTDOWN_DRAIN_SECONDS, OpenAiOutputMode, ProviderConfig,
    TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_API_TOKEN_ENV,
    TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_BASE_URL_ENV,
    TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_MODEL_ENV, TOTAL_RECALL_POST_PROCESSING_BATCH_LIMIT_ENV,
    TOTAL_RECALL_POST_PROCESSING_CONCURRENCY_ENV, TOTAL_RECALL_POST_PROCESSING_CRON_EXPRESSION_ENV,
    TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV, TOTAL_RECALL_POST_PROCESSING_LEASE_SECONDS_ENV,
    TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV,
    TOTAL_RECALL_POST_PROCESSING_MODEL_ATTEMPTS_ENV,
    TOTAL_RECALL_POST_PROCESSING_MODEL_CHUNK_BYTES_ENV,
    TOTAL_RECALL_POST_PROCESSING_MODEL_OUTPUT_TOKENS_ENV,
    TOTAL_RECALL_POST_PROCESSING_MODEL_TIMEOUT_SECONDS_ENV,
    TOTAL_RECALL_POST_PROCESSING_OPENAI_API_TOKEN_ENV,
    TOTAL_RECALL_POST_PROCESSING_OPENAI_BASE_URL_ENV,
    TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL_ENV,
    TOTAL_RECALL_POST_PROCESSING_OPENAI_OUTPUT_MODE_ENV, TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV,
    TOTAL_RECALL_POST_PROCESSING_SHUTDOWN_DRAIN_SECONDS_ENV,
};

const OPENAI_TOKEN: &str = "openai-token-secret-sentinel";
const ANTHROPIC_TOKEN: &str = "anthropic-token-secret-sentinel";

#[test]
fn configuration_uses_openai_and_resource_defaults() {
    let config =
        Config::from_values(valid_openai_values()).expect("default configuration is valid");

    assert_eq!(config.database.as_str(), "source-session");
    assert_eq!(config.memory_database.as_str(), "memory");
    assert_eq!(config.cron_expression, DEFAULT_CRON_EXPRESSION);
    assert_eq!(config.batch_limit, DEFAULT_BATCH_LIMIT);
    assert_eq!(config.concurrency, DEFAULT_CONCURRENCY);
    assert_eq!(
        config.lease_duration,
        Duration::from_secs(DEFAULT_LEASE_SECONDS)
    );
    assert_eq!(
        config.model_timeout,
        Duration::from_secs(DEFAULT_MODEL_TIMEOUT_SECONDS)
    );
    assert_eq!(config.model_attempts, DEFAULT_MODEL_ATTEMPTS);
    assert_eq!(config.model_chunk_bytes, DEFAULT_MODEL_CHUNK_BYTES);
    assert_eq!(config.model_output_tokens, DEFAULT_MODEL_OUTPUT_TOKENS);
    assert_eq!(
        config.shutdown_drain,
        Duration::from_secs(DEFAULT_SHUTDOWN_DRAIN_SECONDS)
    );

    let ProviderConfig::OpenAi(provider) = config.provider else {
        panic!("OpenAI should be selected");
    };
    assert_eq!(provider.base_url().as_str(), DEFAULT_OPENAI_BASE_URL);
    assert_eq!(provider.model(), "gpt-test");
    assert_eq!(provider.output_mode(), OpenAiOutputMode::JsonSchema);
    assert_token_is_redacted(provider.api_token(), OPENAI_TOKEN);
}

#[test]
fn configuration_accepts_six_and_seven_field_schedules() {
    for expression in ["0 */5 * * * *", "0 0 12 * * * 2027"] {
        let config = Config::from_values(with_value(
            valid_openai_values(),
            TOTAL_RECALL_POST_PROCESSING_CRON_EXPRESSION_ENV,
            expression,
        ))
        .expect("valid UTC cron expression should be accepted");

        assert_eq!(config.cron_expression, expression);
    }
}

#[test]
fn configuration_accepts_valid_overrides() {
    let config = Config::from_values(with_values(
        valid_openai_values(),
        [
            (
                TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV,
                "source-session-override",
            ),
            (
                TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV,
                "memory-override",
            ),
            (
                TOTAL_RECALL_POST_PROCESSING_CRON_EXPRESSION_ENV,
                "0 0 12 * * * 2027",
            ),
            (TOTAL_RECALL_POST_PROCESSING_BATCH_LIMIT_ENV, "20"),
            (TOTAL_RECALL_POST_PROCESSING_CONCURRENCY_ENV, "5"),
            (TOTAL_RECALL_POST_PROCESSING_LEASE_SECONDS_ENV, "900"),
            (
                TOTAL_RECALL_POST_PROCESSING_MODEL_TIMEOUT_SECONDS_ENV,
                "120",
            ),
            (TOTAL_RECALL_POST_PROCESSING_MODEL_ATTEMPTS_ENV, "4"),
            (TOTAL_RECALL_POST_PROCESSING_MODEL_CHUNK_BYTES_ENV, "4096"),
            (TOTAL_RECALL_POST_PROCESSING_MODEL_OUTPUT_TOKENS_ENV, "1024"),
            (
                TOTAL_RECALL_POST_PROCESSING_SHUTDOWN_DRAIN_SECONDS_ENV,
                "45",
            ),
            (
                TOTAL_RECALL_POST_PROCESSING_OPENAI_BASE_URL_ENV,
                "http://127.0.0.1:8080/v1/",
            ),
            (
                TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL_ENV,
                "gpt-override",
            ),
            (
                TOTAL_RECALL_POST_PROCESSING_OPENAI_OUTPUT_MODE_ENV,
                "prompt",
            ),
        ],
    ))
    .expect("consistent overrides should be accepted");

    assert_eq!(config.database.as_str(), "source-session-override");
    assert_eq!(config.memory_database.as_str(), "memory-override");
    assert_eq!(config.cron_expression, "0 0 12 * * * 2027");
    assert_eq!(config.batch_limit, 20);
    assert_eq!(config.concurrency, 5);
    assert_eq!(config.lease_duration, Duration::from_secs(900));
    assert_eq!(config.model_timeout, Duration::from_secs(120));
    assert_eq!(config.model_attempts, 4);
    assert_eq!(config.model_chunk_bytes, 4096);
    assert_eq!(config.model_output_tokens, 1024);
    assert_eq!(config.shutdown_drain, Duration::from_secs(45));

    let ProviderConfig::OpenAi(provider) = config.provider else {
        panic!("OpenAI should be selected");
    };
    assert_eq!(provider.base_url().as_str(), "http://127.0.0.1:8080/v1/");
    assert_eq!(provider.model(), "gpt-override");
    assert_eq!(provider.output_mode(), OpenAiOutputMode::Prompt);
}

#[test]
fn configuration_accepts_all_openai_output_modes() {
    for (value, expected) in [
        ("json_schema", OpenAiOutputMode::JsonSchema),
        ("json_object", OpenAiOutputMode::JsonObject),
        ("prompt", OpenAiOutputMode::Prompt),
    ] {
        let config = Config::from_values(with_value(
            valid_openai_values(),
            TOTAL_RECALL_POST_PROCESSING_OPENAI_OUTPUT_MODE_ENV,
            value,
        ))
        .expect("supported OpenAI output mode should be accepted");

        let ProviderConfig::OpenAi(provider) = config.provider else {
            panic!("OpenAI should be selected");
        };
        assert_eq!(provider.output_mode(), expected);
    }
}

#[test]
fn configuration_selects_anthropic_with_provider_defaults() {
    let config = Config::from_values(valid_anthropic_values())
        .expect("Anthropic configuration should be valid");

    let ProviderConfig::Anthropic(provider) = config.provider else {
        panic!("Anthropic should be selected");
    };
    assert_eq!(provider.base_url().as_str(), DEFAULT_ANTHROPIC_BASE_URL);
    assert_eq!(provider.model(), "claude-test");
    assert_token_is_redacted(provider.api_token(), ANTHROPIC_TOKEN);
}

#[test]
fn configuration_validates_only_the_selected_provider() {
    let openai = Config::from_values(with_values(
        valid_openai_values(),
        [
            (
                TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_BASE_URL_ENV,
                "http://example.invalid/v1/",
            ),
            (TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_API_TOKEN_ENV, " \t "),
            (TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_MODEL_ENV, " \t "),
        ],
    ));
    assert!(
        openai.is_ok(),
        "inactive Anthropic settings must be ignored"
    );

    let anthropic = Config::from_values(with_values(
        valid_anthropic_values(),
        [
            (
                TOTAL_RECALL_POST_PROCESSING_OPENAI_BASE_URL_ENV,
                "http://example.invalid/v1/",
            ),
            (TOTAL_RECALL_POST_PROCESSING_OPENAI_API_TOKEN_ENV, " \t "),
            (TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL_ENV, " \t "),
            (
                TOTAL_RECALL_POST_PROCESSING_OPENAI_OUTPUT_MODE_ENV,
                "invalid-output-mode",
            ),
        ],
    ));
    assert!(
        anthropic.is_ok(),
        "inactive OpenAI settings must be ignored"
    );
}

#[test]
fn configuration_rejects_missing_and_whitespace_required_values() {
    assert_config_error(
        remove_value(
            valid_openai_values(),
            TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV,
        ),
        TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV,
        "missing",
    );
    assert_config_error(
        with_value(
            valid_openai_values(),
            TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV,
            " \t ",
        ),
        TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV,
        "blank",
    );
    assert_config_error(
        remove_value(
            valid_openai_values(),
            TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV,
        ),
        TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV,
        "missing",
    );
    assert_config_error(
        with_value(
            valid_openai_values(),
            TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV,
            " \t ",
        ),
        TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV,
        "blank",
    );
    assert_config_error(
        remove_value(
            valid_openai_values(),
            TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV,
        ),
        TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV,
        "missing",
    );
    assert_config_error(
        with_value(
            valid_openai_values(),
            TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV,
            " \t ",
        ),
        TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV,
        "blank",
    );

    for variable in [
        TOTAL_RECALL_POST_PROCESSING_OPENAI_API_TOKEN_ENV,
        TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL_ENV,
    ] {
        assert_config_error(
            remove_value(valid_openai_values(), variable),
            variable,
            "missing",
        );
        assert_config_error(
            with_value(valid_openai_values(), variable, " \t "),
            variable,
            "blank",
        );
    }

    for variable in [
        TOTAL_RECALL_POST_PROCESSING_CRON_EXPRESSION_ENV,
        TOTAL_RECALL_POST_PROCESSING_OPENAI_BASE_URL_ENV,
    ] {
        assert_config_error(
            with_value(valid_openai_values(), variable, " \t "),
            variable,
            "blank",
        );
    }
}

#[test]
fn configuration_rejects_invalid_schedules() {
    for expression in ["* * * * *", "0 0 0 * * * * *", "61 * * * * *", "not cron"] {
        assert_config_error(
            with_value(
                valid_openai_values(),
                TOTAL_RECALL_POST_PROCESSING_CRON_EXPRESSION_ENV,
                expression,
            ),
            TOTAL_RECALL_POST_PROCESSING_CRON_EXPRESSION_ENV,
            "invalid",
        );
    }
}

#[test]
fn configuration_rejects_non_positive_whitespace_and_overflow_bounds() {
    let variables = [
        TOTAL_RECALL_POST_PROCESSING_BATCH_LIMIT_ENV,
        TOTAL_RECALL_POST_PROCESSING_CONCURRENCY_ENV,
        TOTAL_RECALL_POST_PROCESSING_LEASE_SECONDS_ENV,
        TOTAL_RECALL_POST_PROCESSING_MODEL_TIMEOUT_SECONDS_ENV,
        TOTAL_RECALL_POST_PROCESSING_MODEL_ATTEMPTS_ENV,
        TOTAL_RECALL_POST_PROCESSING_MODEL_CHUNK_BYTES_ENV,
        TOTAL_RECALL_POST_PROCESSING_MODEL_OUTPUT_TOKENS_ENV,
        TOTAL_RECALL_POST_PROCESSING_SHUTDOWN_DRAIN_SECONDS_ENV,
    ];

    for variable in variables {
        for value in ["0", "-1", "not-a-number", " 10 ", "18446744073709551616"] {
            assert_config_error(
                with_value(valid_openai_values(), variable, value),
                variable,
                "invalid_positive_integer",
            );
        }
    }
}

#[test]
fn configuration_rejects_inconsistent_bounds() {
    assert_config_error(
        with_values(
            valid_openai_values(),
            [
                (TOTAL_RECALL_POST_PROCESSING_BATCH_LIMIT_ENV, "10"),
                (TOTAL_RECALL_POST_PROCESSING_CONCURRENCY_ENV, "11"),
            ],
        ),
        TOTAL_RECALL_POST_PROCESSING_CONCURRENCY_ENV,
        "exceeds_batch_limit",
    );
    assert_config_error(
        with_values(
            valid_openai_values(),
            [
                (TOTAL_RECALL_POST_PROCESSING_LEASE_SECONDS_ENV, "600"),
                (
                    TOTAL_RECALL_POST_PROCESSING_MODEL_TIMEOUT_SECONDS_ENV,
                    "600",
                ),
            ],
        ),
        TOTAL_RECALL_POST_PROCESSING_MODEL_TIMEOUT_SECONDS_ENV,
        "not_below_lease_seconds",
    );
}

#[test]
fn configuration_rejects_lease_seconds_that_cannot_cross_the_database_i64_boundary() {
    let maximum = Config::from_values(with_value(
        valid_openai_values(),
        TOTAL_RECALL_POST_PROCESSING_LEASE_SECONDS_ENV,
        &i64::MAX.to_string(),
    ))
    .expect("i64::MAX lease seconds should remain representable");
    assert_eq!(maximum.lease_duration, Duration::from_secs(i64::MAX as u64));

    assert_config_error(
        with_value(
            valid_openai_values(),
            TOTAL_RECALL_POST_PROCESSING_LEASE_SECONDS_ENV,
            &(i64::MAX as u64 + 1).to_string(),
        ),
        TOTAL_RECALL_POST_PROCESSING_LEASE_SECONDS_ENV,
        "exceeds_i64_max",
    );
}

#[test]
fn configuration_rejects_invalid_selected_provider_values() {
    assert_config_error(
        with_value(
            valid_openai_values(),
            TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV,
            "unsupported",
        ),
        TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV,
        "invalid",
    );
    assert_config_error(
        with_value(
            valid_openai_values(),
            TOTAL_RECALL_POST_PROCESSING_OPENAI_OUTPUT_MODE_ENV,
            "unsupported",
        ),
        TOTAL_RECALL_POST_PROCESSING_OPENAI_OUTPUT_MODE_ENV,
        "invalid",
    );
    assert_config_error(
        with_value(
            valid_openai_values(),
            TOTAL_RECALL_POST_PROCESSING_OPENAI_BASE_URL_ENV,
            "not a URL",
        ),
        TOTAL_RECALL_POST_PROCESSING_OPENAI_BASE_URL_ENV,
        "invalid",
    );
    assert_config_error(
        with_value(
            valid_openai_values(),
            TOTAL_RECALL_POST_PROCESSING_OPENAI_BASE_URL_ENV,
            "https://user:password@example.com/v1/",
        ),
        TOTAL_RECALL_POST_PROCESSING_OPENAI_BASE_URL_ENV,
        "credentials_not_allowed",
    );
}

#[test]
fn configuration_rejects_invalid_selected_anthropic_values() {
    for variable in [
        TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_API_TOKEN_ENV,
        TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_MODEL_ENV,
    ] {
        assert_config_error(
            remove_value(valid_anthropic_values(), variable),
            variable,
            "missing",
        );
        assert_config_error(
            with_value(valid_anthropic_values(), variable, " \t "),
            variable,
            "blank",
        );
    }
    assert_config_error(
        with_value(
            valid_anthropic_values(),
            TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_BASE_URL_ENV,
            "http://example.com/v1/",
        ),
        TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_BASE_URL_ENV,
        "https_required",
    );
    assert_config_error(
        with_value(
            valid_anthropic_values(),
            TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_BASE_URL_ENV,
            "https://user:password@example.com/v1/",
        ),
        TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_BASE_URL_ENV,
        "credentials_not_allowed",
    );
}

#[test]
fn configuration_allows_http_only_for_loopback_hosts() {
    for (values, variable) in [
        (
            valid_openai_values(),
            TOTAL_RECALL_POST_PROCESSING_OPENAI_BASE_URL_ENV,
        ),
        (
            valid_anthropic_values(),
            TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_BASE_URL_ENV,
        ),
    ] {
        for base_url in [
            "http://localhost:8080/v1/",
            "http://127.0.0.1:8080/v1/",
            "http://127.255.255.255:8080/v1/",
            "http://[::1]:8080/v1/",
            "https://example.com/v1/",
        ] {
            let config = Config::from_values(with_value(values.clone(), variable, base_url));
            assert!(config.is_ok(), "{base_url} should be accepted");
        }

        for base_url in ["http://example.com/v1/", "http://[::2]:8080/v1/"] {
            assert_config_error(
                with_value(values.clone(), variable, base_url),
                variable,
                "https_required",
            );
        }
    }
}

#[test]
fn configuration_and_errors_redact_tokens_and_provider_settings() {
    let config = Config::from_values(with_values(
        valid_openai_values(),
        [
            (
                TOTAL_RECALL_POST_PROCESSING_OPENAI_BASE_URL_ENV,
                "https://provider-setting-secret-sentinel.example/v1/",
            ),
            (
                TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL_ENV,
                "model-secret-sentinel",
            ),
        ],
    ))
    .expect("redaction fixture should be valid");
    let debug = format!("{config:?}");
    for sentinel in [
        OPENAI_TOKEN,
        "provider-setting-secret-sentinel",
        "model-secret-sentinel",
    ] {
        assert!(!debug.contains(sentinel), "Config Debug leaked: {debug}");
    }

    let error = Config::from_values(with_value(
        valid_openai_values(),
        TOTAL_RECALL_POST_PROCESSING_OPENAI_BASE_URL_ENV,
        "https://user:provider-token-secret-sentinel@example.com/v1/",
    ))
    .expect_err("credential-bearing base URLs must be rejected");
    assert_error_is_safe(
        &error,
        &[
            OPENAI_TOKEN,
            "provider-token-secret-sentinel",
            "example.com",
        ],
    );
}

fn valid_openai_values() -> Vec<(String, String)> {
    vec![
        (
            TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV.to_owned(),
            " source-session ".to_owned(),
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV.to_owned(),
            " memory ".to_owned(),
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV.to_owned(),
            "openai".to_owned(),
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_OPENAI_API_TOKEN_ENV.to_owned(),
            OPENAI_TOKEN.to_owned(),
        ),
        (
            TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL_ENV.to_owned(),
            "gpt-test".to_owned(),
        ),
    ]
}

fn valid_anthropic_values() -> Vec<(String, String)> {
    with_values(
        valid_openai_values(),
        [
            (TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV, "anthropic"),
            (
                TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_API_TOKEN_ENV,
                ANTHROPIC_TOKEN,
            ),
            (
                TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_MODEL_ENV,
                "claude-test",
            ),
        ],
    )
}

fn with_value(values: Vec<(String, String)>, variable: &str, value: &str) -> Vec<(String, String)> {
    with_values(values, [(variable, value)])
}

fn with_values<const N: usize>(
    mut values: Vec<(String, String)>,
    replacements: [(&str, &str); N],
) -> Vec<(String, String)> {
    for (variable, value) in replacements {
        if let Some((_, existing)) = values.iter_mut().find(|(key, _)| key == variable) {
            *existing = value.to_owned();
        } else {
            values.push((variable.to_owned(), value.to_owned()));
        }
    }

    values
}

fn remove_value(mut values: Vec<(String, String)>, variable: &str) -> Vec<(String, String)> {
    values.retain(|(key, _)| key != variable);
    values
}

fn assert_config_error(values: Vec<(String, String)>, field: &str, code: &str) {
    let error = Config::from_values(values).expect_err("configuration should be rejected");
    assert_eq!(error.field(), field);
    assert_eq!(error.code(), code);
}

fn assert_token_is_redacted(token: &ApiToken, sentinel: &str) {
    assert_eq!(token.to_string(), "<redacted>");
    assert!(!format!("{token:?}").contains(sentinel));
    let serialized = serde_json::to_string(token).expect("token should serialize safely");
    assert!(!serialized.contains(sentinel));
}

fn assert_error_is_safe<E>(error: &E, sentinels: &[&str])
where
    E: std::fmt::Debug + Display + Serialize,
{
    let display = error.to_string();
    let debug = format!("{error:?}");
    let serialized = serde_json::to_string(error).expect("error should serialize safely");

    for sentinel in sentinels {
        assert!(!display.contains(sentinel), "Display leaked: {display}");
        assert!(!debug.contains(sentinel), "Debug leaked: {debug}");
        assert!(
            !serialized.contains(sentinel),
            "serialized error leaked: {serialized}"
        );
    }
}
