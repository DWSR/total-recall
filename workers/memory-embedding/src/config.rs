//! Launch configuration validation.

use std::{collections::HashMap, fmt, str::FromStr, time::Duration};

use iii_sdk::DEFAULT_ENGINE_URL;
use memory_store::contracts::DatabaseTarget;
use thiserror::Error;
use tokio::sync::Semaphore;

pub const TOTAL_RECALL_EMBEDDING_DATABASE_ENV: &str = "TOTAL_RECALL_EMBEDDING_DATABASE";
pub const TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC_ENV: &str = "TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC";
pub const TOTAL_RECALL_EMBEDDING_CRON_EXPRESSION_ENV: &str =
    "TOTAL_RECALL_EMBEDDING_CRON_EXPRESSION";
pub const TOTAL_RECALL_EMBEDDING_PROVIDER_ENV: &str = "TOTAL_RECALL_EMBEDDING_PROVIDER";
pub const TOTAL_RECALL_EMBEDDING_MODEL_ENV: &str = "TOTAL_RECALL_EMBEDDING_MODEL";
pub const TOTAL_RECALL_EMBEDDING_MAX_EVENT_KEYS_ENV: &str = "TOTAL_RECALL_EMBEDDING_MAX_EVENT_KEYS";
pub const TOTAL_RECALL_EMBEDDING_BATCH_LIMIT_ENV: &str = "TOTAL_RECALL_EMBEDDING_BATCH_LIMIT";
pub const TOTAL_RECALL_EMBEDDING_RECONCILIATION_LIMIT_ENV: &str =
    "TOTAL_RECALL_EMBEDDING_RECONCILIATION_LIMIT";
pub const TOTAL_RECALL_EMBEDDING_MAX_INPUT_BYTES_ENV: &str =
    "TOTAL_RECALL_EMBEDDING_MAX_INPUT_BYTES";
pub const TOTAL_RECALL_EMBEDDING_MAX_IN_FLIGHT_ENV: &str = "TOTAL_RECALL_EMBEDDING_MAX_IN_FLIGHT";
pub const TOTAL_RECALL_EMBEDDING_DATABASE_TIMEOUT_MS_ENV: &str =
    "TOTAL_RECALL_EMBEDDING_DATABASE_TIMEOUT_MS";
pub const TOTAL_RECALL_EMBEDDING_ROUTER_TIMEOUT_MS_ENV: &str =
    "TOTAL_RECALL_EMBEDDING_ROUTER_TIMEOUT_MS";
pub const TOTAL_RECALL_EMBEDDING_INVOCATION_TIMEOUT_MS_ENV: &str =
    "TOTAL_RECALL_EMBEDDING_INVOCATION_TIMEOUT_MS";
pub const TOTAL_RECALL_EMBEDDING_SHUTDOWN_TIMEOUT_MS_ENV: &str =
    "TOTAL_RECALL_EMBEDDING_SHUTDOWN_TIMEOUT_MS";
pub const III_URL_ENV: &str = "III_URL";
pub const III_WORKER_NAME_ENV: &str = "III_WORKER_NAME";
pub const III_NAMESPACE_ENV: &str = "III_NAMESPACE";

pub const DEFAULT_CRON_EXPRESSION: &str = "0 * * * * *";
pub const DEFAULT_NAMESPACE: &str = "default";

const DEFAULT_MAX_EVENT_KEYS: u32 = 16;
const DEFAULT_BATCH_LIMIT: u32 = 16;
const DEFAULT_RECONCILIATION_LIMIT: u32 = 16;
const DEFAULT_MAX_INPUT_BYTES: usize = 32_768;
const DEFAULT_MAX_IN_FLIGHT: usize = 4;
const DEFAULT_DATABASE_TIMEOUT_MS: u64 = 3_000;
const DEFAULT_ROUTER_TIMEOUT_MS: u64 = 21_000;
const DEFAULT_INVOCATION_TIMEOUT_MS: u64 = 28_000;
const DEFAULT_SHUTDOWN_TIMEOUT_MS: u64 = 30_000;
const MAX_BATCH_LIMIT: u32 = 100;
const CRON_CALLER_TIMEOUT_MS: u64 = 30_000;

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ConfigError {
    #[error("TOTAL_RECALL_EMBEDDING_DATABASE is not set")]
    MissingDatabase,
    #[error("TOTAL_RECALL_EMBEDDING_DATABASE must not be blank")]
    BlankDatabase,
    #[error("TOTAL_RECALL_EMBEDDING_DATABASE is invalid")]
    InvalidDatabase,
    #[error("TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC is not set")]
    MissingQueueTopic,
    #[error("TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC must not be blank")]
    BlankQueueTopic,
    #[error("TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC is invalid")]
    InvalidQueueTopic,
    #[error("TOTAL_RECALL_EMBEDDING_CRON_EXPRESSION must not be blank")]
    BlankCronExpression,
    #[error("TOTAL_RECALL_EMBEDDING_CRON_EXPRESSION is invalid")]
    InvalidCronExpression,
    #[error("TOTAL_RECALL_EMBEDDING_PROVIDER is not set")]
    MissingProvider,
    #[error("TOTAL_RECALL_EMBEDDING_PROVIDER must not be blank")]
    BlankProvider,
    #[error("TOTAL_RECALL_EMBEDDING_PROVIDER is invalid")]
    InvalidProvider,
    #[error("TOTAL_RECALL_EMBEDDING_MODEL is not set")]
    MissingModel,
    #[error("TOTAL_RECALL_EMBEDDING_MODEL must not be blank")]
    BlankModel,
    #[error("TOTAL_RECALL_EMBEDDING_MODEL is invalid")]
    InvalidModel,
    #[error("TOTAL_RECALL_EMBEDDING_MAX_EVENT_KEYS is invalid")]
    InvalidMaxEventKeys,
    #[error("TOTAL_RECALL_EMBEDDING_BATCH_LIMIT is invalid")]
    InvalidBatchLimit,
    #[error("TOTAL_RECALL_EMBEDDING_RECONCILIATION_LIMIT is invalid")]
    InvalidReconciliationLimit,
    #[error("TOTAL_RECALL_EMBEDDING_MAX_INPUT_BYTES is invalid")]
    InvalidMaxInputBytes,
    #[error("TOTAL_RECALL_EMBEDDING_MAX_IN_FLIGHT is invalid")]
    InvalidMaxInFlight,
    #[error("TOTAL_RECALL_EMBEDDING_DATABASE_TIMEOUT_MS is invalid")]
    InvalidDatabaseTimeout,
    #[error("TOTAL_RECALL_EMBEDDING_ROUTER_TIMEOUT_MS is invalid")]
    InvalidRouterTimeout,
    #[error("TOTAL_RECALL_EMBEDDING_INVOCATION_TIMEOUT_MS is invalid")]
    InvalidInvocationTimeout,
    #[error("TOTAL_RECALL_EMBEDDING_SHUTDOWN_TIMEOUT_MS is invalid")]
    InvalidShutdownTimeout,
    #[error(
        "TOTAL_RECALL_EMBEDDING_MAX_EVENT_KEYS must not exceed TOTAL_RECALL_EMBEDDING_BATCH_LIMIT"
    )]
    MaxEventKeysExceedsBatchLimit,
    #[error(
        "TOTAL_RECALL_EMBEDDING_RECONCILIATION_LIMIT must not exceed TOTAL_RECALL_EMBEDDING_BATCH_LIMIT"
    )]
    ReconciliationLimitExceedsBatchLimit,
    #[error(
        "TOTAL_RECALL_EMBEDDING_ROUTER_TIMEOUT_MS must be below TOTAL_RECALL_EMBEDDING_INVOCATION_TIMEOUT_MS"
    )]
    RouterTimeoutExceedsInvocationTimeout,
    #[error("TOTAL_RECALL_EMBEDDING_INVOCATION_TIMEOUT_MS must be below the cron caller deadline")]
    InvocationTimeoutExceedsCronDeadline,
    #[error(
        "TOTAL_RECALL_EMBEDDING_INVOCATION_TIMEOUT_MS must cover router and database operations"
    )]
    InvocationTimeoutBelowOperationBudget,
    #[error("III_NAMESPACE must be default")]
    InvalidNamespace,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ManagedIiiEnvironment {
    pub engine_url: String,
    pub worker_name: Option<String>,
    pub namespace: String,
}

impl ManagedIiiEnvironment {
    fn from_values(values: &HashMap<String, String>) -> Result<Self, ConfigError> {
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
            .cloned()
            .unwrap_or_else(|| DEFAULT_NAMESPACE.to_owned());

        if namespace != DEFAULT_NAMESPACE {
            return Err(ConfigError::InvalidNamespace);
        }

        Ok(Self {
            engine_url,
            worker_name,
            namespace,
        })
    }
}

impl fmt::Debug for ManagedIiiEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagedIiiEnvironment")
            .field("engine_url", &Redacted)
            .field("worker_name", &Redacted)
            .field("namespace", &Redacted)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct Config {
    pub database: DatabaseTarget,
    pub queue_topic: String,
    pub cron_expression: String,
    pub provider: String,
    pub model: String,
    pub max_event_keys: u32,
    pub batch_limit: u32,
    pub reconciliation_limit: u32,
    pub max_input_bytes: usize,
    pub max_in_flight: usize,
    pub database_timeout: Duration,
    pub router_timeout: Duration,
    pub invocation_timeout: Duration,
    pub shutdown_timeout: Duration,
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
        let database = required_value(
            &values,
            TOTAL_RECALL_EMBEDDING_DATABASE_ENV,
            ConfigError::MissingDatabase,
            ConfigError::BlankDatabase,
            ConfigError::InvalidDatabase,
        )?;
        let queue_topic = required_value(
            &values,
            TOTAL_RECALL_EMBEDDING_QUEUE_TOPIC_ENV,
            ConfigError::MissingQueueTopic,
            ConfigError::BlankQueueTopic,
            ConfigError::InvalidQueueTopic,
        )?;
        let provider = required_value(
            &values,
            TOTAL_RECALL_EMBEDDING_PROVIDER_ENV,
            ConfigError::MissingProvider,
            ConfigError::BlankProvider,
            ConfigError::InvalidProvider,
        )?;
        let model = required_value(
            &values,
            TOTAL_RECALL_EMBEDDING_MODEL_ENV,
            ConfigError::MissingModel,
            ConfigError::BlankModel,
            ConfigError::InvalidModel,
        )?;
        let cron_expression = cron_expression(&values)?;
        let max_event_keys = positive_value(
            &values,
            TOTAL_RECALL_EMBEDDING_MAX_EVENT_KEYS_ENV,
            DEFAULT_MAX_EVENT_KEYS,
            ConfigError::InvalidMaxEventKeys,
        )?;
        let batch_limit = positive_value(
            &values,
            TOTAL_RECALL_EMBEDDING_BATCH_LIMIT_ENV,
            DEFAULT_BATCH_LIMIT,
            ConfigError::InvalidBatchLimit,
        )?;
        let reconciliation_limit = positive_value(
            &values,
            TOTAL_RECALL_EMBEDDING_RECONCILIATION_LIMIT_ENV,
            DEFAULT_RECONCILIATION_LIMIT,
            ConfigError::InvalidReconciliationLimit,
        )?;
        let max_input_bytes = positive_value(
            &values,
            TOTAL_RECALL_EMBEDDING_MAX_INPUT_BYTES_ENV,
            DEFAULT_MAX_INPUT_BYTES,
            ConfigError::InvalidMaxInputBytes,
        )?;
        let max_in_flight = positive_value(
            &values,
            TOTAL_RECALL_EMBEDDING_MAX_IN_FLIGHT_ENV,
            DEFAULT_MAX_IN_FLIGHT,
            ConfigError::InvalidMaxInFlight,
        )?;
        let database_timeout_ms = positive_value(
            &values,
            TOTAL_RECALL_EMBEDDING_DATABASE_TIMEOUT_MS_ENV,
            DEFAULT_DATABASE_TIMEOUT_MS,
            ConfigError::InvalidDatabaseTimeout,
        )?;
        let router_timeout_ms = positive_value(
            &values,
            TOTAL_RECALL_EMBEDDING_ROUTER_TIMEOUT_MS_ENV,
            DEFAULT_ROUTER_TIMEOUT_MS,
            ConfigError::InvalidRouterTimeout,
        )?;
        let invocation_timeout_ms = positive_value(
            &values,
            TOTAL_RECALL_EMBEDDING_INVOCATION_TIMEOUT_MS_ENV,
            DEFAULT_INVOCATION_TIMEOUT_MS,
            ConfigError::InvalidInvocationTimeout,
        )?;
        let shutdown_timeout_ms = positive_value(
            &values,
            TOTAL_RECALL_EMBEDDING_SHUTDOWN_TIMEOUT_MS_ENV,
            DEFAULT_SHUTDOWN_TIMEOUT_MS,
            ConfigError::InvalidShutdownTimeout,
        )?;

        if max_in_flight > Semaphore::MAX_PERMITS {
            return Err(ConfigError::InvalidMaxInFlight);
        }
        if max_event_keys > MAX_BATCH_LIMIT {
            return Err(ConfigError::InvalidMaxEventKeys);
        }
        if batch_limit > MAX_BATCH_LIMIT {
            return Err(ConfigError::InvalidBatchLimit);
        }
        if max_event_keys > batch_limit {
            return Err(ConfigError::MaxEventKeysExceedsBatchLimit);
        }
        if reconciliation_limit > batch_limit {
            return Err(ConfigError::ReconciliationLimitExceedsBatchLimit);
        }
        if router_timeout_ms >= invocation_timeout_ms {
            return Err(ConfigError::RouterTimeoutExceedsInvocationTimeout);
        }
        if invocation_timeout_ms >= CRON_CALLER_TIMEOUT_MS {
            return Err(ConfigError::InvocationTimeoutExceedsCronDeadline);
        }
        let operation_budget_ms = router_timeout_ms
            .checked_add(
                database_timeout_ms
                    .checked_mul(2)
                    .ok_or(ConfigError::InvocationTimeoutBelowOperationBudget)?,
            )
            .ok_or(ConfigError::InvocationTimeoutBelowOperationBudget)?;
        if invocation_timeout_ms < operation_budget_ms {
            return Err(ConfigError::InvocationTimeoutBelowOperationBudget);
        }
        let iii = ManagedIiiEnvironment::from_values(&values)?;

        let database =
            DatabaseTarget::try_from(database).map_err(|_| ConfigError::InvalidDatabase)?;

        Ok(Self {
            database,
            queue_topic,
            cron_expression,
            provider,
            model,
            max_event_keys,
            batch_limit,
            reconciliation_limit,
            max_input_bytes,
            max_in_flight,
            database_timeout: Duration::from_millis(database_timeout_ms),
            router_timeout: Duration::from_millis(router_timeout_ms),
            invocation_timeout: Duration::from_millis(invocation_timeout_ms),
            shutdown_timeout: Duration::from_millis(shutdown_timeout_ms),
            iii,
        })
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Config")
            .field("database", &Redacted)
            .field("queue_topic", &Redacted)
            .field("cron_expression", &Redacted)
            .field("provider", &Redacted)
            .field("model", &Redacted)
            .field("max_event_keys", &Redacted)
            .field("batch_limit", &Redacted)
            .field("reconciliation_limit", &Redacted)
            .field("max_input_bytes", &Redacted)
            .field("max_in_flight", &Redacted)
            .field("database_timeout", &Redacted)
            .field("router_timeout", &Redacted)
            .field("invocation_timeout", &Redacted)
            .field("shutdown_timeout", &Redacted)
            .field("iii", &self.iii)
            .finish()
    }
}

struct Redacted;

impl fmt::Debug for Redacted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

fn required_value(
    values: &HashMap<String, String>,
    variable: &'static str,
    missing: ConfigError,
    blank: ConfigError,
    invalid: ConfigError,
) -> Result<String, ConfigError> {
    let value = values.get(variable).ok_or(missing)?.trim();
    if value.is_empty() {
        return Err(blank);
    }
    if value.contains('\0') {
        return Err(invalid);
    }

    Ok(value.to_owned())
}

fn positive_value<T>(
    values: &HashMap<String, String>,
    variable: &'static str,
    default: T,
    invalid: ConfigError,
) -> Result<T, ConfigError>
where
    T: FromStr + From<u8> + PartialEq,
{
    let value = values.get(variable).map_or(Ok(default), |value| {
        value.trim().parse::<T>().map_err(|_| invalid)
    })?;
    if value == T::from(0) {
        return Err(invalid);
    }

    Ok(value)
}

fn cron_expression(values: &HashMap<String, String>) -> Result<String, ConfigError> {
    let expression = values
        .get(TOTAL_RECALL_EMBEDDING_CRON_EXPRESSION_ENV)
        .map_or(DEFAULT_CRON_EXPRESSION, String::as_str)
        .trim();
    if expression.is_empty() {
        return Err(ConfigError::BlankCronExpression);
    }
    if !is_valid_cron_expression(expression) {
        return Err(ConfigError::InvalidCronExpression);
    }

    Ok(expression.to_owned())
}

fn is_valid_cron_expression(expression: &str) -> bool {
    let field_rules = [
        CronField::numeric(0, 59),
        CronField::numeric(0, 59),
        CronField::numeric(0, 23),
        CronField::numeric_with_any(1, 31),
        CronField::months(),
        CronField::weekdays(),
        CronField::numeric(1970, 2100),
    ];
    let mut parser = CronParser::new(expression);

    if !field_rules[..6]
        .iter()
        .copied()
        .all(|field| field.parses(&mut parser))
    {
        return false;
    }

    parser.is_finished() || (field_rules[6].parses(&mut parser) && parser.is_finished())
}

#[derive(Clone, Copy)]
struct CronField {
    min: u32,
    max: u32,
    names: &'static [(&'static str, u32)],
    allows_any: bool,
}

impl CronField {
    const fn numeric(min: u32, max: u32) -> Self {
        Self {
            min,
            max,
            names: &[],
            allows_any: false,
        }
    }

    const fn numeric_with_any(min: u32, max: u32) -> Self {
        Self {
            min,
            max,
            names: &[],
            allows_any: true,
        }
    }

    const fn months() -> Self {
        Self {
            min: 1,
            max: 12,
            names: &[
                ("jan", 1),
                ("january", 1),
                ("feb", 2),
                ("february", 2),
                ("mar", 3),
                ("march", 3),
                ("apr", 4),
                ("april", 4),
                ("may", 5),
                ("jun", 6),
                ("june", 6),
                ("jul", 7),
                ("july", 7),
                ("aug", 8),
                ("august", 8),
                ("sep", 9),
                ("september", 9),
                ("oct", 10),
                ("october", 10),
                ("nov", 11),
                ("november", 11),
                ("dec", 12),
                ("december", 12),
            ],
            allows_any: false,
        }
    }

    const fn weekdays() -> Self {
        Self {
            min: 1,
            max: 7,
            names: &[
                ("sun", 1),
                ("sunday", 1),
                ("mon", 2),
                ("monday", 2),
                ("tue", 3),
                ("tues", 3),
                ("tuesday", 3),
                ("wed", 4),
                ("wednesday", 4),
                ("thu", 5),
                ("thurs", 5),
                ("thursday", 5),
                ("fri", 6),
                ("friday", 6),
                ("sat", 7),
                ("saturday", 7),
            ],
            allows_any: true,
        }
    }

    fn parses(self, parser: &mut CronParser<'_>) -> bool {
        parser.skip_whitespace();
        if !self.parses_item(parser) {
            return false;
        }

        while parser.consume(b',') {
            if !self.parses_item(parser) {
                return false;
            }
        }

        parser.skip_whitespace();
        true
    }

    fn parses_item(self, parser: &mut CronParser<'_>) -> bool {
        let Some(base) = parser.parse_base(self.allows_any) else {
            return false;
        };

        if parser.consume(b'/') {
            let Some(step) = parser.parse_ordinal() else {
                return false;
            };

            return step != 0 && !matches!(base, CronBase::Named(_)) && self.validates_base(base);
        }

        self.validates_base(base)
    }

    fn validates_base(self, base: CronBase<'_>) -> bool {
        match base {
            CronBase::All => true,
            CronBase::Numeric(value) => self.in_range(value),
            CronBase::NumericRange(start, end) => {
                self.in_range(start) && self.in_range(end) && start <= end
            }
            CronBase::Named(name) => self.named_value(name).is_some(),
            CronBase::NamedRange(start, end) => self
                .named_value(start)
                .zip(self.named_value(end))
                .is_some_and(|(start, end)| start <= end),
        }
    }

    fn named_value(self, value: &str) -> Option<u32> {
        self.names
            .iter()
            .find(|(name, _)| value.eq_ignore_ascii_case(name))
            .map(|(_, value)| *value)
    }

    const fn in_range(self, value: u32) -> bool {
        value >= self.min && value <= self.max
    }
}

enum CronBase<'a> {
    All,
    Numeric(u32),
    NumericRange(u32, u32),
    Named(&'a str),
    NamedRange(&'a str, &'a str),
}

struct CronParser<'a> {
    expression: &'a str,
    position: usize,
}

impl<'a> CronParser<'a> {
    const fn new(expression: &'a str) -> Self {
        Self {
            expression,
            position: 0,
        }
    }

    fn is_finished(&self) -> bool {
        self.position == self.expression.len()
    }

    fn parse_base(&mut self, allows_any: bool) -> Option<CronBase<'a>> {
        match self.peek()? {
            b'*' => {
                self.position += 1;
                Some(CronBase::All)
            }
            b'?' if allows_any => {
                self.position += 1;
                Some(CronBase::All)
            }
            _ => {
                let position = self.position;
                if let Some(start) = self.parse_ordinal() {
                    if self.consume(b'-') {
                        Some(CronBase::NumericRange(start, self.parse_ordinal()?))
                    } else {
                        Some(CronBase::Numeric(start))
                    }
                } else {
                    self.position = position;
                    let start = self.parse_name()?;
                    if self.consume(b'-') {
                        Some(CronBase::NamedRange(start, self.parse_name()?))
                    } else {
                        Some(CronBase::Named(start))
                    }
                }
            }
        }
    }

    fn parse_ordinal(&mut self) -> Option<u32> {
        self.skip_whitespace();
        let start = self.position;
        while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
            self.position += 1;
        }
        if start == self.position {
            return None;
        }

        let ordinal = self.expression[start..self.position].parse().ok()?;
        self.skip_whitespace();
        Some(ordinal)
    }

    fn parse_name(&mut self) -> Option<&'a str> {
        self.skip_whitespace();
        let start = self.position;
        while self.peek().is_some_and(|byte| byte.is_ascii_alphabetic()) {
            self.position += 1;
        }
        if start == self.position {
            return None;
        }

        let name = &self.expression[start..self.position];
        self.skip_whitespace();
        Some(name)
    }

    fn consume(&mut self, expected: u8) -> bool {
        if self.peek() == Some(expected) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn skip_whitespace(&mut self) {
        while self
            .peek()
            .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\r' | b'\n'))
        {
            self.position += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.expression.as_bytes().get(self.position).copied()
    }
}
