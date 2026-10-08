use std::{collections::HashMap, fmt, net::IpAddr, str::FromStr, time::Duration};

use cron::Schedule;
use iii_sdk::DEFAULT_ENGINE_URL;
use memory_store::contracts::DatabaseTarget;
use reqwest::Url;
use serde::{Serialize, Serializer};
use thiserror::Error;

pub const TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV: &str = "TOTAL_RECALL_POST_PROCESSING_DATABASE";
pub const TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV: &str =
    "TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE";
pub const TOTAL_RECALL_POST_PROCESSING_CRON_EXPRESSION_ENV: &str =
    "TOTAL_RECALL_POST_PROCESSING_CRON_EXPRESSION";
pub const TOTAL_RECALL_POST_PROCESSING_BATCH_LIMIT_ENV: &str =
    "TOTAL_RECALL_POST_PROCESSING_BATCH_LIMIT";
pub const TOTAL_RECALL_POST_PROCESSING_CONCURRENCY_ENV: &str =
    "TOTAL_RECALL_POST_PROCESSING_CONCURRENCY";
pub const TOTAL_RECALL_POST_PROCESSING_LEASE_SECONDS_ENV: &str =
    "TOTAL_RECALL_POST_PROCESSING_LEASE_SECONDS";
pub const TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV: &str = "TOTAL_RECALL_POST_PROCESSING_PROVIDER";
pub const TOTAL_RECALL_POST_PROCESSING_OPENAI_BASE_URL_ENV: &str =
    "TOTAL_RECALL_POST_PROCESSING_OPENAI_BASE_URL";
pub const TOTAL_RECALL_POST_PROCESSING_OPENAI_API_TOKEN_ENV: &str =
    "TOTAL_RECALL_POST_PROCESSING_OPENAI_API_TOKEN";
pub const TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL_ENV: &str =
    "TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL";
pub const TOTAL_RECALL_POST_PROCESSING_OPENAI_OUTPUT_MODE_ENV: &str =
    "TOTAL_RECALL_POST_PROCESSING_OPENAI_OUTPUT_MODE";
pub const TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_BASE_URL_ENV: &str =
    "TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_BASE_URL";
pub const TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_API_TOKEN_ENV: &str =
    "TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_API_TOKEN";
pub const TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_MODEL_ENV: &str =
    "TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_MODEL";
pub const TOTAL_RECALL_POST_PROCESSING_MODEL_TIMEOUT_SECONDS_ENV: &str =
    "TOTAL_RECALL_POST_PROCESSING_MODEL_TIMEOUT_SECONDS";
pub const TOTAL_RECALL_POST_PROCESSING_MODEL_ATTEMPTS_ENV: &str =
    "TOTAL_RECALL_POST_PROCESSING_MODEL_ATTEMPTS";
pub const TOTAL_RECALL_POST_PROCESSING_MODEL_CHUNK_BYTES_ENV: &str =
    "TOTAL_RECALL_POST_PROCESSING_MODEL_CHUNK_BYTES";
pub const TOTAL_RECALL_POST_PROCESSING_MODEL_OUTPUT_TOKENS_ENV: &str =
    "TOTAL_RECALL_POST_PROCESSING_MODEL_OUTPUT_TOKENS";
pub const TOTAL_RECALL_POST_PROCESSING_SHUTDOWN_DRAIN_SECONDS_ENV: &str =
    "TOTAL_RECALL_POST_PROCESSING_SHUTDOWN_DRAIN_SECONDS";
pub const III_URL_ENV: &str = "III_URL";
pub const III_WORKER_NAME_ENV: &str = "III_WORKER_NAME";
pub const III_NAMESPACE_ENV: &str = "III_NAMESPACE";

pub const DEFAULT_CRON_EXPRESSION: &str = "0 * * * * *";
pub const DEFAULT_BATCH_LIMIT: u32 = 10;
pub const DEFAULT_CONCURRENCY: u32 = 2;
pub const DEFAULT_LEASE_SECONDS: u64 = 600;
pub const DEFAULT_MODEL_TIMEOUT_SECONDS: u64 = 180;
pub const DEFAULT_MODEL_ATTEMPTS: u32 = 3;
pub const DEFAULT_MODEL_CHUNK_BYTES: usize = 131_072;
pub const DEFAULT_MODEL_OUTPUT_TOKENS: u32 = 8_192;
pub const DEFAULT_SHUTDOWN_DRAIN_SECONDS: u64 = 30;
pub const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/v1/";
pub const DEFAULT_ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com/v1/";
pub const DEFAULT_OPENAI_OUTPUT_MODE: &str = "json_schema";

#[derive(Clone, Debug, Eq, Error, PartialEq, Serialize)]
#[error("invalid configuration: {field} ({code})")]
pub struct ConfigError {
    field: &'static str,
    code: &'static str,
}

impl ConfigError {
    pub const fn field(&self) -> &'static str {
        self.field
    }

    pub const fn code(&self) -> &'static str {
        self.code
    }

    const fn invalid(field: &'static str, code: &'static str) -> Self {
        Self { field, code }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ApiToken(String);

impl ApiToken {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ApiToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApiToken(<redacted>)")
    }
}

impl fmt::Display for ApiToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

impl Serialize for ApiToken {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str("<redacted>")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenAiOutputMode {
    JsonSchema,
    JsonObject,
    Prompt,
}

#[derive(Clone, Eq, PartialEq)]
pub struct OpenAiConfig {
    base_url: Url,
    api_token: ApiToken,
    model: String,
    output_mode: OpenAiOutputMode,
}

impl OpenAiConfig {
    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    pub fn api_token(&self) -> &ApiToken {
        &self.api_token
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    pub const fn output_mode(&self) -> OpenAiOutputMode {
        self.output_mode
    }
}

impl fmt::Debug for OpenAiConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OpenAiConfig(<redacted>)")
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct AnthropicConfig {
    base_url: Url,
    api_token: ApiToken,
    model: String,
}

impl AnthropicConfig {
    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    pub fn api_token(&self) -> &ApiToken {
        &self.api_token
    }

    pub fn model(&self) -> &str {
        &self.model
    }
}

impl fmt::Debug for AnthropicConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AnthropicConfig(<redacted>)")
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum ProviderConfig {
    OpenAi(OpenAiConfig),
    Anthropic(AnthropicConfig),
}

impl fmt::Debug for ProviderConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OpenAi(_) => formatter.write_str("ProviderConfig::OpenAi(<redacted>)"),
            Self::Anthropic(_) => formatter.write_str("ProviderConfig::Anthropic(<redacted>)"),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ManagedIiiEnvironment {
    pub engine_url: String,
    pub worker_name: Option<String>,
    pub namespace: Option<String>,
}

impl ManagedIiiEnvironment {
    fn from_values(values: &HashMap<String, String>) -> Self {
        let engine_url = values
            .get(III_URL_ENV)
            .filter(|url| !url.trim().is_empty())
            .cloned()
            .unwrap_or_else(|| DEFAULT_ENGINE_URL.to_owned());
        let worker_name = values
            .get(III_WORKER_NAME_ENV)
            .filter(|name| !name.is_empty())
            .cloned();
        let namespace = values
            .get(III_NAMESPACE_ENV)
            .filter(|namespace| !namespace.trim().is_empty())
            .cloned();

        Self {
            engine_url,
            worker_name,
            namespace,
        }
    }
}

impl fmt::Debug for ManagedIiiEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagedIiiEnvironment")
            .field("engine_url", &"<redacted>")
            .field("worker_name", &self.worker_name)
            .field("namespace", &self.namespace)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct Config {
    pub database: DatabaseTarget,
    pub memory_database: DatabaseTarget,
    pub cron_expression: String,
    pub batch_limit: u32,
    pub concurrency: u32,
    pub lease_duration: Duration,
    pub model_timeout: Duration,
    pub model_attempts: u32,
    pub model_chunk_bytes: usize,
    pub model_output_tokens: u32,
    pub shutdown_drain: Duration,
    pub provider: ProviderConfig,
    pub iii: ManagedIiiEnvironment,
}

impl fmt::Debug for Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Config")
            .field("cron_expression", &self.cron_expression)
            .field("batch_limit", &self.batch_limit)
            .field("concurrency", &self.concurrency)
            .field("lease_duration", &self.lease_duration)
            .field("model_timeout", &self.model_timeout)
            .field("model_attempts", &self.model_attempts)
            .field("model_chunk_bytes", &self.model_chunk_bytes)
            .field("model_output_tokens", &self.model_output_tokens)
            .field("shutdown_drain", &self.shutdown_drain)
            .field("provider", &self.provider)
            .field("iii", &self.iii)
            .finish()
    }
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_values(std::env::vars())
    }

    pub fn from_values<I>(values: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let values = values.into_iter().collect::<HashMap<_, _>>();
        let database = database_target(&values, TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV)?;
        let memory_database =
            database_target(&values, TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV)?;
        let cron_expression = optional_value(
            &values,
            TOTAL_RECALL_POST_PROCESSING_CRON_EXPRESSION_ENV,
            DEFAULT_CRON_EXPRESSION,
        )?
        .to_owned();
        validate_cron_expression(&cron_expression)?;

        let batch_limit: u32 = positive_value(
            &values,
            TOTAL_RECALL_POST_PROCESSING_BATCH_LIMIT_ENV,
            DEFAULT_BATCH_LIMIT,
        )?;
        let concurrency: u32 = positive_value(
            &values,
            TOTAL_RECALL_POST_PROCESSING_CONCURRENCY_ENV,
            DEFAULT_CONCURRENCY,
        )?;
        if concurrency > batch_limit {
            return Err(ConfigError::invalid(
                TOTAL_RECALL_POST_PROCESSING_CONCURRENCY_ENV,
                "exceeds_batch_limit",
            ));
        }

        let lease_seconds: u64 = positive_value(
            &values,
            TOTAL_RECALL_POST_PROCESSING_LEASE_SECONDS_ENV,
            DEFAULT_LEASE_SECONDS,
        )?;
        if lease_seconds > i64::MAX as u64 {
            return Err(ConfigError::invalid(
                TOTAL_RECALL_POST_PROCESSING_LEASE_SECONDS_ENV,
                "exceeds_i64_max",
            ));
        }
        let model_timeout_seconds: u64 = positive_value(
            &values,
            TOTAL_RECALL_POST_PROCESSING_MODEL_TIMEOUT_SECONDS_ENV,
            DEFAULT_MODEL_TIMEOUT_SECONDS,
        )?;
        if model_timeout_seconds >= lease_seconds {
            return Err(ConfigError::invalid(
                TOTAL_RECALL_POST_PROCESSING_MODEL_TIMEOUT_SECONDS_ENV,
                "not_below_lease_seconds",
            ));
        }

        let model_attempts: u32 = positive_value(
            &values,
            TOTAL_RECALL_POST_PROCESSING_MODEL_ATTEMPTS_ENV,
            DEFAULT_MODEL_ATTEMPTS,
        )?;
        let model_chunk_bytes: usize = positive_value(
            &values,
            TOTAL_RECALL_POST_PROCESSING_MODEL_CHUNK_BYTES_ENV,
            DEFAULT_MODEL_CHUNK_BYTES,
        )?;
        let model_output_tokens: u32 = positive_value(
            &values,
            TOTAL_RECALL_POST_PROCESSING_MODEL_OUTPUT_TOKENS_ENV,
            DEFAULT_MODEL_OUTPUT_TOKENS,
        )?;
        let shutdown_drain_seconds: u64 = positive_value(
            &values,
            TOTAL_RECALL_POST_PROCESSING_SHUTDOWN_DRAIN_SECONDS_ENV,
            DEFAULT_SHUTDOWN_DRAIN_SECONDS,
        )?;

        Ok(Self {
            database,
            memory_database,
            cron_expression,
            batch_limit,
            concurrency,
            lease_duration: Duration::from_secs(lease_seconds),
            model_timeout: Duration::from_secs(model_timeout_seconds),
            model_attempts,
            model_chunk_bytes,
            model_output_tokens,
            shutdown_drain: Duration::from_secs(shutdown_drain_seconds),
            provider: provider_config(&values)?,
            iii: ManagedIiiEnvironment::from_values(&values),
        })
    }
}

fn database_target(
    values: &HashMap<String, String>,
    variable: &'static str,
) -> Result<DatabaseTarget, ConfigError> {
    DatabaseTarget::try_from(required_value(values, variable)?.to_owned())
        .map_err(|_| ConfigError::invalid(variable, "invalid"))
}

fn provider_config(values: &HashMap<String, String>) -> Result<ProviderConfig, ConfigError> {
    match required_value(values, TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV)? {
        "openai" => openai_config(values).map(ProviderConfig::OpenAi),
        "anthropic" => anthropic_config(values).map(ProviderConfig::Anthropic),
        _ => Err(ConfigError::invalid(
            TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV,
            "invalid",
        )),
    }
}

fn openai_config(values: &HashMap<String, String>) -> Result<OpenAiConfig, ConfigError> {
    let output_mode = match optional_value(
        values,
        TOTAL_RECALL_POST_PROCESSING_OPENAI_OUTPUT_MODE_ENV,
        DEFAULT_OPENAI_OUTPUT_MODE,
    )? {
        "json_schema" => OpenAiOutputMode::JsonSchema,
        "json_object" => OpenAiOutputMode::JsonObject,
        "prompt" => OpenAiOutputMode::Prompt,
        _ => {
            return Err(ConfigError::invalid(
                TOTAL_RECALL_POST_PROCESSING_OPENAI_OUTPUT_MODE_ENV,
                "invalid",
            ));
        }
    };

    Ok(OpenAiConfig {
        base_url: base_url(
            values,
            TOTAL_RECALL_POST_PROCESSING_OPENAI_BASE_URL_ENV,
            DEFAULT_OPENAI_BASE_URL,
        )?,
        api_token: ApiToken(
            required_value(values, TOTAL_RECALL_POST_PROCESSING_OPENAI_API_TOKEN_ENV)?.to_owned(),
        ),
        model: required_value(values, TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL_ENV)?.to_owned(),
        output_mode,
    })
}

fn anthropic_config(values: &HashMap<String, String>) -> Result<AnthropicConfig, ConfigError> {
    Ok(AnthropicConfig {
        base_url: base_url(
            values,
            TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_BASE_URL_ENV,
            DEFAULT_ANTHROPIC_BASE_URL,
        )?,
        api_token: ApiToken(
            required_value(values, TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_API_TOKEN_ENV)?
                .to_owned(),
        ),
        model: required_value(values, TOTAL_RECALL_POST_PROCESSING_ANTHROPIC_MODEL_ENV)?.to_owned(),
    })
}

fn base_url(
    values: &HashMap<String, String>,
    variable: &'static str,
    default: &'static str,
) -> Result<Url, ConfigError> {
    let url = Url::parse(optional_value(values, variable, default)?)
        .map_err(|_| ConfigError::invalid(variable, "invalid"))?;
    if url.host_str().is_none() {
        return Err(ConfigError::invalid(variable, "invalid"));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ConfigError::invalid(variable, "credentials_not_allowed"));
    }
    if url.scheme() != "https" && !(url.scheme() == "http" && is_loopback_host(&url)) {
        return Err(ConfigError::invalid(variable, "https_required"));
    }

    Ok(url)
}

fn is_loopback_host(url: &Url) -> bool {
    url.host_str().is_some_and(|host| {
        let host = host.trim_matches(['[', ']']);
        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    })
}

fn required_value<'a>(
    values: &'a HashMap<String, String>,
    variable: &'static str,
) -> Result<&'a str, ConfigError> {
    let value = values
        .get(variable)
        .ok_or(ConfigError::invalid(variable, "missing"))?
        .trim();
    if value.is_empty() {
        return Err(ConfigError::invalid(variable, "blank"));
    }

    Ok(value)
}

fn optional_value<'a>(
    values: &'a HashMap<String, String>,
    variable: &'static str,
    default: &'static str,
) -> Result<&'a str, ConfigError> {
    values
        .contains_key(variable)
        .then(|| required_value(values, variable))
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn positive_value<T>(
    values: &HashMap<String, String>,
    variable: &'static str,
    default: T,
) -> Result<T, ConfigError>
where
    T: FromStr + Default + PartialEq,
{
    values.get(variable).map_or(Ok(default), |value| {
        value
            .parse::<T>()
            .ok()
            .filter(|value| *value != T::default())
            .ok_or(ConfigError::invalid(variable, "invalid_positive_integer"))
    })
}

fn validate_cron_expression(expression: &str) -> Result<(), ConfigError> {
    let field_count = expression.split_whitespace().count();
    let expression = match field_count {
        6 => format!("{expression} *"),
        7 => expression.to_owned(),
        _ => {
            return Err(ConfigError::invalid(
                TOTAL_RECALL_POST_PROCESSING_CRON_EXPRESSION_ENV,
                "invalid",
            ));
        }
    };

    Schedule::from_str(&expression).map_err(|_| {
        ConfigError::invalid(TOTAL_RECALL_POST_PROCESSING_CRON_EXPRESSION_ENV, "invalid")
    })?;

    Ok(())
}
