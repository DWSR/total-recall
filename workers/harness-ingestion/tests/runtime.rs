use std::sync::Arc;

use harness_ingestion::{
    IngestionService,
    publisher::{IiiQueuePublisher, QueueTopic},
    runtime::{
        FunctionCatalogEntry, HARNESS_FUNCTION_IDS, REGISTRATION_NONCE_METADATA_KEY,
        RegistrationPlan, catalog_matches, register_harness_functions,
    },
};
use iii_sdk::IIIClient;
use serde_json::json;
use uuid::Uuid;

const WORKER_NAME: &str = "harness-ingestion";
const NAMESPACE: &str = "total-recall";

fn catalog_entry(plan: &RegistrationPlan, function_id: &str) -> FunctionCatalogEntry {
    FunctionCatalogEntry {
        function_id: function_id.to_owned(),
        namespace: plan
            .expected_namespace
            .clone()
            .unwrap_or_else(|| "default".to_owned()),
        worker_name: plan.expected_worker_name.clone(),
        metadata: Some(json!({
            REGISTRATION_NONCE_METADATA_KEY: plan.nonce.to_string(),
        })),
    }
}

fn complete_catalog(plan: &RegistrationPlan) -> Vec<FunctionCatalogEntry> {
    HARNESS_FUNCTION_IDS
        .into_iter()
        .map(|function_id| catalog_entry(plan, function_id))
        .collect()
}

#[test]
fn registration_plan_contains_each_function_once_with_the_current_nonce() {
    let nonce = Uuid::parse_str("8d4d6f3a-2a42-4e9a-9d37-4d0d4a6e0a11").unwrap();
    let plan = RegistrationPlan::with_nonce(WORKER_NAME, Some(NAMESPACE.to_owned()), nonce);

    assert_eq!(
        plan.registrations
            .iter()
            .map(|registration| registration.function_id)
            .collect::<std::collections::HashSet<_>>(),
        HARNESS_FUNCTION_IDS.into_iter().collect()
    );
    assert_eq!(plan.registrations.len(), HARNESS_FUNCTION_IDS.len());
    for registration in &plan.registrations {
        assert_eq!(
            registration.metadata[REGISTRATION_NONCE_METADATA_KEY],
            json!(nonce.to_string())
        );
    }
}

#[test]
fn registration_plan_contains_each_harness_function_once_in_order() {
    let nonce = Uuid::new_v4();
    let plan = RegistrationPlan::with_nonce(WORKER_NAME, Some(NAMESPACE.to_owned()), nonce);

    let registrations = &plan.registrations;
    assert_eq!(
        registrations
            .iter()
            .map(|registration| registration.function_id)
            .collect::<Vec<_>>(),
        HARNESS_FUNCTION_IDS
    );
    assert_eq!(registrations.len(), HARNESS_FUNCTION_IDS.len());
    assert_eq!(
        registrations
            .iter()
            .map(|registration| registration.function_id)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        HARNESS_FUNCTION_IDS.len()
    );
}

#[test]
fn registration_plan_shares_the_current_nonce_and_expected_ownership_metadata() {
    let plan = RegistrationPlan::new(WORKER_NAME, Some(NAMESPACE.to_owned()));

    assert_ne!(plan.nonce, Uuid::nil());
    assert_eq!(plan.expected_worker_name, WORKER_NAME);
    assert_eq!(plan.expected_namespace.as_deref(), Some(NAMESPACE));
    assert_eq!(plan.registrations.len(), HARNESS_FUNCTION_IDS.len());
    for registration in &plan.registrations {
        assert_eq!(
            registration.metadata,
            json!({
                "registration_nonce": plan.nonce.to_string(),
                "worker_name": WORKER_NAME,
                "namespace": NAMESPACE,
            })
        );
    }
}

#[test]
fn registration_uses_one_client_publisher_and_typed_ingestion_service_without_an_engine() {
    let client = IIIClient::new("ws://127.0.0.1:0");
    let publisher = IiiQueuePublisher::new(
        client.clone(),
        QueueTopic::new("harness-events").expect("test topic should be valid"),
    );
    let service = Arc::new(IngestionService::new(publisher));
    let plan =
        RegistrationPlan::with_nonce(WORKER_NAME, Some(NAMESPACE.to_owned()), Uuid::new_v4());

    let function_refs = register_harness_functions(&client, &plan, service);

    assert_eq!(
        function_refs
            .iter()
            .map(|function_ref| function_ref.id.as_str())
            .collect::<Vec<_>>(),
        HARNESS_FUNCTION_IDS
    );
}

#[test]
fn catalog_matching_accepts_all_current_functions_with_current_ownership() {
    let plan =
        RegistrationPlan::with_nonce(WORKER_NAME, Some(NAMESPACE.to_owned()), Uuid::new_v4());

    assert!(catalog_matches(&plan, &complete_catalog(&plan)));
}

#[test]
fn catalog_matching_rejects_a_missing_function() {
    let plan =
        RegistrationPlan::with_nonce(WORKER_NAME, Some(NAMESPACE.to_owned()), Uuid::new_v4());
    let mut catalog = complete_catalog(&plan);
    catalog.pop();

    assert!(!catalog_matches(&plan, &catalog));
}

#[test]
fn catalog_matching_rejects_a_stale_registration_nonce() {
    let plan =
        RegistrationPlan::with_nonce(WORKER_NAME, Some(NAMESPACE.to_owned()), Uuid::new_v4());
    let mut catalog = complete_catalog(&plan);
    catalog[0].metadata = Some(json!({
        REGISTRATION_NONCE_METADATA_KEY: Uuid::new_v4().to_string(),
    }));

    assert!(!catalog_matches(&plan, &catalog));
}

#[test]
fn catalog_matching_rejects_a_foreign_registration() {
    let plan =
        RegistrationPlan::with_nonce(WORKER_NAME, Some(NAMESPACE.to_owned()), Uuid::new_v4());
    let mut catalog = complete_catalog(&plan);
    catalog[1].worker_name = "foreign-worker".to_owned();
    catalog[1].namespace = "foreign-namespace".to_owned();

    assert!(!catalog_matches(&plan, &catalog));
}

#[test]
fn catalog_matching_rejects_duplicate_or_conflicting_function_ids() {
    let plan =
        RegistrationPlan::with_nonce(WORKER_NAME, Some(NAMESPACE.to_owned()), Uuid::new_v4());
    let mut catalog = complete_catalog(&plan);
    catalog[2].function_id = catalog[1].function_id.clone();

    assert!(!catalog_matches(&plan, &catalog));
}
