use std::{collections::HashMap, fmt};

use iii_sdk::DEFAULT_ENGINE_URL;
use memory_store::contracts::DatabaseTarget;
use thiserror::Error;

pub const TOTAL_RECALL_MEMORY_DATABASE_ENV: &str = "TOTAL_RECALL_MEMORY_DATABASE";
pub const TOTAL_RECALL_EMBEDDING_PROVIDER_ENV: &str = "TOTAL_RECALL_EMBEDDING_PROVIDER";
pub const TOTAL_RECALL_EMBEDDING_MODEL_ENV: &str = "TOTAL_RECALL_EMBEDDING_MODEL";
pub const TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS_ENV: &str = "TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS";
pub const TOTAL_RECALL_MCP_CONTEXT_DEFAULT_LIMIT_ENV: &str =
    "TOTAL_RECALL_MCP_CONTEXT_DEFAULT_LIMIT";
pub const TOTAL_RECALL_MCP_CONTEXT_MAX_LIMIT_ENV: &str = "TOTAL_RECALL_MCP_CONTEXT_MAX_LIMIT";
pub const TOTAL_RECALL_MCP_CONTEXT_MAX_RESULT_BYTES_ENV: &str =
    "TOTAL_RECALL_MCP_CONTEXT_MAX_RESULT_BYTES";
pub const TOTAL_RECALL_MCP_CONTEXT_TIMEOUT_MS_ENV: &str = "TOTAL_RECALL_MCP_CONTEXT_TIMEOUT_MS";
pub const TOTAL_RECALL_MCP_CONTEXT_DATABASE_TIMEOUT_MS_ENV: &str =
    "TOTAL_RECALL_MCP_CONTEXT_DATABASE_TIMEOUT_MS";
pub const TOTAL_RECALL_MCP_MAX_LINE_BYTES_ENV: &str = "TOTAL_RECALL_MCP_MAX_LINE_BYTES";
pub const TOTAL_RECALL_MCP_CHANNEL_CAPACITY_ENV: &str = "TOTAL_RECALL_MCP_CHANNEL_CAPACITY";
pub const TOTAL_RECALL_MCP_MAX_IN_FLIGHT_ENV: &str = "TOTAL_RECALL_MCP_MAX_IN_FLIGHT";
pub const III_URL_ENV: &str = "III_URL";
pub const III_WORKER_NAME_ENV: &str = "III_WORKER_NAME";
pub const III_NAMESPACE_ENV: &str = "III_NAMESPACE";

pub const DEFAULT_EMBEDDING_TIMEOUT_MS: u64 = 21_000;
pub const MAX_EMBEDDING_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_CONTEXT_LIMIT: u32 = 10;
pub const DEFAULT_CONTEXT_MAX_LIMIT: u32 = 20;
pub const MAX_CONTEXT_LIMIT: u32 = 50;
pub const DEFAULT_CONTEXT_MAX_RESULT_BYTES: usize = 32_768;
pub const MIN_CONTEXT_RESULT_BYTES: usize = br#"{"memories":[]}"#.len();
pub const DEFAULT_CONTEXT_TIMEOUT_MS: u64 = 5_000;
pub const DEFAULT_CONTEXT_DATABASE_TIMEOUT_MS: u64 = 3_000;
pub const MAX_CONTEXT_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_MAX_LINE_BYTES: usize = 4_194_304;
pub const DEFAULT_CHANNEL_CAPACITY: usize = 32;
pub const DEFAULT_MAX_IN_FLIGHT: usize = 16;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ConfigError {
    #[error("TOTAL_RECALL_MEMORY_DATABASE is not set")]
    MissingDatabase,
    #[error("TOTAL_RECALL_MEMORY_DATABASE must not be blank")]
    BlankDatabase,
    #[error("TOTAL_RECALL_MEMORY_DATABASE is invalid")]
    InvalidDatabase,
    #[error("{variable} must be a positive usize")]
    InvalidPositiveUsize { variable: &'static str },
    #[error(
        "TOTAL_RECALL_EMBEDDING_PROVIDER and TOTAL_RECALL_EMBEDDING_MODEL must both be non-blank or both be blank"
    )]
    PartialEmbeddingConfiguration,
    #[error("TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS must be an integer from 1 through 30000")]
    InvalidEmbeddingTimeout,
    #[error("startup context configuration is invalid")]
    InvalidContextConfiguration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryEmbeddingConfig {
    Disabled,
    Enabled(QueryEmbeddingSettings),
}

impl QueryEmbeddingConfig {
    fn from_values(values: &HashMap<String, String>) -> Result<Self, ConfigError> {
        let provider = non_blank(values, TOTAL_RECALL_EMBEDDING_PROVIDER_ENV);
        let model = non_blank(values, TOTAL_RECALL_EMBEDDING_MODEL_ENV);
        match (provider, model) {
            (None, None) => Ok(Self::Disabled),
            (Some(provider), Some(model)) => Ok(Self::Enabled(QueryEmbeddingSettings {
                provider: EmbeddingIdentifier(provider.to_owned()),
                model: EmbeddingIdentifier(model.to_owned()),
                timeout_ms: embedding_timeout_ms(values)?,
            })),
            _ => Err(ConfigError::PartialEmbeddingConfiguration),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryEmbeddingSettings {
    provider: EmbeddingIdentifier,
    model: EmbeddingIdentifier,
    timeout_ms: u64,
}

impl QueryEmbeddingSettings {
    pub fn provider(&self) -> &EmbeddingIdentifier {
        &self.provider
    }

    pub fn model(&self) -> &EmbeddingIdentifier {
        &self.model
    }

    pub const fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct EmbeddingIdentifier(String);

impl EmbeddingIdentifier {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for EmbeddingIdentifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EmbeddingIdentifier(<redacted>)")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    pub database: DatabaseTarget,
    pub max_line_bytes: usize,
    pub channel_capacity: usize,
    pub max_in_flight: usize,
    pub query_embedding: QueryEmbeddingConfig,
    pub context: ContextSettings,
    pub iii: ManagedIiiEnvironment,
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
        let database = values
            .get(TOTAL_RECALL_MEMORY_DATABASE_ENV)
            .ok_or(ConfigError::MissingDatabase)?
            .trim();
        if database.is_empty() {
            return Err(ConfigError::BlankDatabase);
        }

        let database = DatabaseTarget::try_from(database.to_owned())
            .map_err(|_| ConfigError::InvalidDatabase)?;

        Ok(Self {
            database,
            max_line_bytes: positive_usize(
                &values,
                TOTAL_RECALL_MCP_MAX_LINE_BYTES_ENV,
                DEFAULT_MAX_LINE_BYTES,
            )?,
            channel_capacity: positive_usize(
                &values,
                TOTAL_RECALL_MCP_CHANNEL_CAPACITY_ENV,
                DEFAULT_CHANNEL_CAPACITY,
            )?,
            max_in_flight: positive_usize(
                &values,
                TOTAL_RECALL_MCP_MAX_IN_FLIGHT_ENV,
                DEFAULT_MAX_IN_FLIGHT,
            )?,
            query_embedding: QueryEmbeddingConfig::from_values(&values)?,
            context: ContextSettings::from_values(&values)?,
            iii: ManagedIiiEnvironment::from_values(&values),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContextSettings {
    default_limit: u32,
    max_limit: u32,
    max_result_bytes: usize,
    timeout_ms: u64,
    database_timeout_ms: u64,
}

impl Default for ContextSettings {
    fn default() -> Self {
        Self {
            default_limit: DEFAULT_CONTEXT_LIMIT,
            max_limit: DEFAULT_CONTEXT_MAX_LIMIT,
            max_result_bytes: DEFAULT_CONTEXT_MAX_RESULT_BYTES,
            timeout_ms: DEFAULT_CONTEXT_TIMEOUT_MS,
            database_timeout_ms: DEFAULT_CONTEXT_DATABASE_TIMEOUT_MS,
        }
    }
}

impl ContextSettings {
    fn from_values(values: &HashMap<String, String>) -> Result<Self, ConfigError> {
        let setting = Self {
            default_limit: positive_u32(
                values,
                TOTAL_RECALL_MCP_CONTEXT_DEFAULT_LIMIT_ENV,
                DEFAULT_CONTEXT_LIMIT,
            )?,
            max_limit: positive_u32(
                values,
                TOTAL_RECALL_MCP_CONTEXT_MAX_LIMIT_ENV,
                DEFAULT_CONTEXT_MAX_LIMIT,
            )?,
            max_result_bytes: positive_usize(
                values,
                TOTAL_RECALL_MCP_CONTEXT_MAX_RESULT_BYTES_ENV,
                DEFAULT_CONTEXT_MAX_RESULT_BYTES,
            )
            .map_err(|_| ConfigError::InvalidContextConfiguration)?,
            timeout_ms: positive_u64(
                values,
                TOTAL_RECALL_MCP_CONTEXT_TIMEOUT_MS_ENV,
                DEFAULT_CONTEXT_TIMEOUT_MS,
            )?,
            database_timeout_ms: positive_u64(
                values,
                TOTAL_RECALL_MCP_CONTEXT_DATABASE_TIMEOUT_MS_ENV,
                DEFAULT_CONTEXT_DATABASE_TIMEOUT_MS,
            )?,
        };
        if setting.default_limit > setting.max_limit
            || setting.max_limit > MAX_CONTEXT_LIMIT
            || setting.max_result_bytes < MIN_CONTEXT_RESULT_BYTES
            || setting.timeout_ms > MAX_CONTEXT_TIMEOUT_MS
            || setting.database_timeout_ms > setting.timeout_ms
        {
            return Err(ConfigError::InvalidContextConfiguration);
        }
        Ok(setting)
    }

    pub const fn default_limit(self) -> u32 {
        self.default_limit
    }
    pub const fn max_limit(self) -> u32 {
        self.max_limit
    }
    pub const fn max_result_bytes(self) -> usize {
        self.max_result_bytes
    }
    pub const fn timeout_ms(self) -> u64 {
        self.timeout_ms
    }
    pub const fn database_timeout_ms(self) -> u64 {
        self.database_timeout_ms
    }
}

fn positive_usize(
    values: &HashMap<String, String>,
    variable: &'static str,
    default: usize,
) -> Result<usize, ConfigError> {
    values.get(variable).map_or(Ok(default), |value| {
        value
            .parse::<usize>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or(ConfigError::InvalidPositiveUsize { variable })
    })
}

fn non_blank<'a>(values: &'a HashMap<String, String>, variable: &str) -> Option<&'a str> {
    values
        .get(variable)
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
}

fn positive_u32(
    values: &HashMap<String, String>,
    variable: &'static str,
    default: u32,
) -> Result<u32, ConfigError> {
    values.get(variable).map_or(Ok(default), |value| {
        value
            .parse::<u32>()
            .ok()
            .filter(|n| *n > 0)
            .ok_or(ConfigError::InvalidContextConfiguration)
    })
}

fn positive_u64(
    values: &HashMap<String, String>,
    variable: &'static str,
    default: u64,
) -> Result<u64, ConfigError> {
    values.get(variable).map_or(Ok(default), |value| {
        value
            .parse::<u64>()
            .ok()
            .filter(|n| *n > 0)
            .ok_or(ConfigError::InvalidContextConfiguration)
    })
}

fn embedding_timeout_ms(values: &HashMap<String, String>) -> Result<u64, ConfigError> {
    values
        .get(TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS_ENV)
        .map_or(Ok(DEFAULT_EMBEDDING_TIMEOUT_MS), |value| {
            value
                .parse::<u64>()
                .ok()
                .filter(|timeout_ms| (1..=MAX_EMBEDDING_TIMEOUT_MS).contains(timeout_ms))
                .ok_or(ConfigError::InvalidEmbeddingTimeout)
        })
}
