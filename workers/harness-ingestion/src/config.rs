use std::collections::HashMap;

use iii_sdk::DEFAULT_ENGINE_URL;
use thiserror::Error;

use crate::publisher::QueueTopic;

pub const TOTAL_RECALL_QUEUE_TOPIC_ENV: &str = "TOTAL_RECALL_QUEUE_TOPIC";
pub const III_URL_ENV: &str = "III_URL";
pub const III_WORKER_NAME_ENV: &str = "III_WORKER_NAME";
pub const III_NAMESPACE_ENV: &str = "III_NAMESPACE";

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ConfigError {
    #[error("TOTAL_RECALL_QUEUE_TOPIC is not set")]
    MissingQueueTopic,
    #[error("TOTAL_RECALL_QUEUE_TOPIC must not be blank")]
    BlankQueueTopic,
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
    pub queue_topic: QueueTopic,
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
        let raw_topic = values
            .get(TOTAL_RECALL_QUEUE_TOPIC_ENV)
            .ok_or(ConfigError::MissingQueueTopic)?;
        let queue_topic = QueueTopic::new(raw_topic).map_err(|_| ConfigError::BlankQueueTopic)?;

        Ok(Self {
            queue_topic,
            iii: ManagedIiiEnvironment::from_values(&values),
        })
    }
}
