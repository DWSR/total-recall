use std::collections::HashMap;

use iii_sdk::DEFAULT_ENGINE_URL;

const III_URL_ENV: &str = "III_URL";
const III_NAMESPACE_ENV: &str = "III_NAMESPACE";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientConfig {
    pub engine_url: String,
    pub namespace: Option<String>,
}

impl ClientConfig {
    pub fn from_env() -> Self {
        Self::from_values(std::env::vars())
    }

    pub fn from_values<I>(values: I) -> Self
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let values = values.into_iter().collect::<HashMap<_, _>>();
        let engine_url = values
            .get(III_URL_ENV)
            .filter(|url| !url.trim().is_empty())
            .cloned()
            .unwrap_or_else(|| DEFAULT_ENGINE_URL.to_owned());
        let namespace = values
            .get(III_NAMESPACE_ENV)
            .filter(|namespace| !namespace.trim().is_empty())
            .cloned();

        Self {
            engine_url,
            namespace,
        }
    }
}

#[cfg(test)]
mod tests {
    use iii_sdk::DEFAULT_ENGINE_URL;

    use super::{ClientConfig, III_NAMESPACE_ENV, III_URL_ENV};

    #[test]
    fn preserves_explicit_url_and_namespace_without_trimming() {
        let config = ClientConfig::from_values([
            (
                III_URL_ENV.to_owned(),
                "  ws://engine.example.test  ".to_owned(),
            ),
            (
                III_NAMESPACE_ENV.to_owned(),
                "  harness-events  ".to_owned(),
            ),
        ]);

        assert_eq!(config.engine_url, "  ws://engine.example.test  ");
        assert_eq!(config.namespace.as_deref(), Some("  harness-events  "));
    }

    #[test]
    fn uses_default_url_and_unset_namespace_when_values_are_absent() {
        let config = ClientConfig::from_values(std::iter::empty());

        assert_eq!(config.engine_url, DEFAULT_ENGINE_URL);
        assert_eq!(config.namespace, None);
    }

    #[test]
    fn uses_default_url_and_unset_namespace_when_values_are_blank() {
        let config = ClientConfig::from_values([
            (III_URL_ENV.to_owned(), " \t\n ".to_owned()),
            (III_NAMESPACE_ENV.to_owned(), " \t\n ".to_owned()),
        ]);

        assert_eq!(config.engine_url, DEFAULT_ENGINE_URL);
        assert_eq!(config.namespace, None);
    }

    #[test]
    fn ignores_worker_name() {
        let without_worker_name = ClientConfig::from_values([
            (
                III_URL_ENV.to_owned(),
                "ws://engine.example.test".to_owned(),
            ),
            (III_NAMESPACE_ENV.to_owned(), "harness-events".to_owned()),
        ]);
        let with_worker_name = ClientConfig::from_values([
            (
                III_URL_ENV.to_owned(),
                "ws://engine.example.test".to_owned(),
            ),
            ("III_WORKER_NAME".to_owned(), "deployed-worker".to_owned()),
            (III_NAMESPACE_ENV.to_owned(), "harness-events".to_owned()),
        ]);

        assert_eq!(with_worker_name, without_worker_name);
        assert_eq!(with_worker_name.engine_url, "ws://engine.example.test");
        assert_eq!(
            with_worker_name.namespace.as_deref(),
            Some("harness-events")
        );
    }
}
