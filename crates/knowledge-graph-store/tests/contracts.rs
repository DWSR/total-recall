use std::{
    fmt::{Debug, Display},
    time::Duration,
};

use knowledge_graph_store::contracts::{
    Alias, AliasQuery, Assertion, AssertionEvidence, AssertionEvidenceInput, AssertionId,
    AssertionIdInput, Concept, ConceptId, ConceptIdInput, ConceptMention, ConceptMentionInput,
    CreateAssertion, CreateConcept, DeleteAssertion, DeleteConcept, DeleteSource, GraphPath,
    NeighborQuery, NeighborResult, OrientedAssertion, PathQuery, RelatedSourceQuery,
    RelatedSourceResult, RelatedSourceRole, ReplaceConceptAliases, UpdateAssertion,
};
use knowledge_graph_store::contracts::{
    BackendFailureReason, ConflictField, ConflictReason, ContractError, DatabaseTimeouts,
    DirectionMode, EdgeOrientation, FieldCode, GraphError, LimitReason, LimitResource,
    MAX_ALIAS_DISPLAY_BYTES, MAX_ALIAS_KEY_BYTES, MAX_ASSERTION_EVIDENCE, MAX_CONCEPT_ALIASES,
    MAX_NEIGHBOR_RESULTS, MAX_PATH_DEPTH, MAX_PATH_RESULTS, MAX_PATH_WORK,
    MAX_RELATED_SOURCE_RESULTS, MAX_RESPONSE_JSON_BYTES, MAX_SOURCE_KEY_BYTES, NotFoundReason,
    OperationCode, RecordIdentity, RecordKind, ReferenceReason, ReferencedRecord,
    RelationSemantics, RelationType, Revision, SourceKind, SourceReference, SourceReferenceInput,
    TraversalBound, TraversalReason, ValidationReason,
};
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

fn valid_uuid_v7() -> Uuid {
    Uuid::now_v7()
}

fn uuid_v7_with_variant_bits(variant_bits: u8) -> Uuid {
    let mut bytes = *valid_uuid_v7().as_bytes();
    bytes[8] = (bytes[8] & 0b0001_1111) | variant_bits;
    let uuid = Uuid::from_bytes(bytes);
    assert_eq!(uuid.get_version_num(), 7);
    assert_eq!(uuid.as_bytes()[8] & 0b1110_0000, variant_bits);
    uuid
}

fn invalid_contract<T: Debug>(
    result: Result<T, ContractError>,
    expected_field: FieldCode,
    expected_reason: ValidationReason,
) {
    let error = result.expect_err("contract input should be rejected");
    assert_eq!(error.field(), expected_field);
    assert_eq!(error.reason(), expected_reason);
}

#[test]
fn graph_record_ids_require_non_nil_uuidv7_values() {
    let concept_uuid = valid_uuid_v7();
    let assertion_uuid = valid_uuid_v7();
    let concept_id = ConceptId::try_from(concept_uuid).expect("UUIDv7 concept ID should pass");
    let assertion_id = knowledge_graph_store::contracts::AssertionId::try_from(assertion_uuid)
        .expect("UUIDv7 assertion ID should pass");

    assert_eq!(concept_id.as_uuid(), &concept_uuid);
    assert_eq!(assertion_id.as_uuid(), &assertion_uuid);
    assert_eq!(concept_id.as_uuid().get_version_num(), 7);
    assert_eq!(assertion_id.as_uuid().get_version_num(), 7);
    assert_eq!(
        concept_id.as_uuid().as_bytes()[8] & 0b1100_0000,
        0b1000_0000
    );
    assert_eq!(
        assertion_id.as_uuid().as_bytes()[8] & 0b1100_0000,
        0b1000_0000
    );

    for variant_bits in [0b0000_0000, 0b1100_0000, 0b1110_0000] {
        invalid_contract(
            ConceptId::try_from(uuid_v7_with_variant_bits(variant_bits)),
            FieldCode::ConceptId,
            ValidationReason::NotUuidV7,
        );
        invalid_contract(
            knowledge_graph_store::contracts::AssertionId::try_from(uuid_v7_with_variant_bits(
                variant_bits,
            )),
            FieldCode::AssertionId,
            ValidationReason::NotUuidV7,
        );
    }

    invalid_contract(
        ConceptId::try_from(Uuid::nil()),
        FieldCode::ConceptId,
        ValidationReason::NilUuid,
    );
    invalid_contract(
        ConceptId::try_from(Uuid::new_v4()),
        FieldCode::ConceptId,
        ValidationReason::NotUuidV7,
    );
    invalid_contract(
        knowledge_graph_store::contracts::AssertionId::try_from(Uuid::nil()),
        FieldCode::AssertionId,
        ValidationReason::NilUuid,
    );
    invalid_contract(
        knowledge_graph_store::contracts::AssertionId::try_from(Uuid::new_v4()),
        FieldCode::AssertionId,
        ValidationReason::NotUuidV7,
    );

    invalid_contract(
        ConceptId::try_from("not-a-uuid".to_owned()),
        FieldCode::ConceptId,
        ValidationReason::InvalidUuid,
    );
    invalid_contract(
        knowledge_graph_store::contracts::AssertionId::try_from("not-a-uuid".to_owned()),
        FieldCode::AssertionId,
        ValidationReason::InvalidUuid,
    );

    const INVALID_ID_SENTINEL: &str = "concept-id-protected-sentinel";
    let error = ConceptId::try_from(INVALID_ID_SENTINEL.to_owned())
        .expect_err("invalid concept IDs should be rejected");
    assert_error_is_opaque(&error, &[INVALID_ID_SENTINEL]);
}

#[test]
fn graph_record_id_debug_output_redacts_uuid_values() {
    let concept_uuid = valid_uuid_v7();
    let assertion_uuid = valid_uuid_v7();
    let concept_sentinel = concept_uuid.to_string();
    let assertion_sentinel = assertion_uuid.to_string();
    let concept_id = ConceptId::try_from(concept_uuid).expect("UUIDv7 concept ID should pass");
    let assertion_id = knowledge_graph_store::contracts::AssertionId::try_from(assertion_uuid)
        .expect("UUIDv7 assertion ID should pass");

    assert_debug_is_opaque(&concept_id, &[&concept_sentinel]);
    assert_debug_is_opaque(&assertion_id, &[&assertion_sentinel]);
}

#[test]
fn memory_and_session_sources_keep_owner_specific_opaque_identities() {
    let memory = SourceReference::try_from(SourceReferenceInput::MemoryVersion {
        memory_id: "memory/key:opaque and non-UUID".to_owned(),
        version: 47,
    })
    .expect("opaque memory ID with a positive version should pass");
    assert_eq!(memory.kind(), SourceKind::MemoryVersion);
    match &memory {
        SourceReference::MemoryVersion { memory_id, version } => {
            assert_eq!(memory_id.as_str(), "memory/key:opaque and non-UUID");
            assert_eq!(version.get(), 47);
            assert_debug_is_opaque(memory_id, &["memory/key:opaque and non-UUID"]);
        }
        SourceReference::SessionRecord { .. } => panic!("expected a memory source"),
    }

    let session = SourceReference::try_from(SourceReferenceInput::SessionRecord {
        session_record_id: " session/record key ".to_owned(),
    })
    .expect("opaque canonical session-record ID should pass");
    assert_eq!(session.kind(), SourceKind::SessionRecord);
    match &session {
        SourceReference::SessionRecord { session_record_id } => {
            assert_eq!(session_record_id.as_str(), " session/record key ");
            assert_debug_is_opaque(session_record_id, &[" session/record key "]);
        }
        SourceReference::MemoryVersion { .. } => panic!("expected a session-record source"),
    }

    let memory_json = serde_json::to_value(&memory).expect("memory source should serialize");
    assert_eq!(memory_json["kind"], "memory_version");
    assert_eq!(memory_json["memory_id"], "memory/key:opaque and non-UUID");
    assert_eq!(memory_json["version"], 47);
    assert!(memory_json.get("content").is_none());

    let session_json = serde_json::to_value(&session).expect("session source should serialize");
    assert_eq!(session_json["kind"], "session_record");
    assert_eq!(session_json["session_record_id"], " session/record key ");
    assert!(session_json.get("version").is_none());
    assert!(session_json.get("content").is_none());
    assert_debug_is_opaque(&memory, &["memory/key:opaque and non-UUID"]);
    assert_debug_is_opaque(&session, &[" session/record key "]);
}

#[test]
fn source_identity_validation_rejects_empty_nul_and_nonpositive_values() {
    invalid_contract(
        SourceReference::try_from(SourceReferenceInput::MemoryVersion {
            memory_id: String::new(),
            version: 1,
        }),
        FieldCode::MemoryId,
        ValidationReason::Empty,
    );
    const MEMORY_ID_SENTINEL: &str = "memory-id-protected-sentinel";
    let memory_nul_error = SourceReference::try_from(SourceReferenceInput::MemoryVersion {
        memory_id: format!("{MEMORY_ID_SENTINEL}\0"),
        version: 1,
    })
    .expect_err("NUL-bearing memory ID should fail");
    assert_eq!(memory_nul_error.field(), FieldCode::MemoryId);
    assert_eq!(memory_nul_error.reason(), ValidationReason::ContainsNul);
    assert_error_is_opaque(&memory_nul_error, &[MEMORY_ID_SENTINEL]);
    invalid_contract(
        SourceReference::try_from(SourceReferenceInput::SessionRecord {
            session_record_id: String::new(),
        }),
        FieldCode::SessionRecordId,
        ValidationReason::Empty,
    );
    const SESSION_ID_SENTINEL: &str = "session-record-id-protected-sentinel";
    let session_nul_error = SourceReference::try_from(SourceReferenceInput::SessionRecord {
        session_record_id: format!("{SESSION_ID_SENTINEL}\0"),
    })
    .expect_err("NUL-bearing session-record ID should fail");
    assert_eq!(session_nul_error.field(), FieldCode::SessionRecordId);
    assert_eq!(session_nul_error.reason(), ValidationReason::ContainsNul);
    assert_error_is_opaque(&session_nul_error, &[SESSION_ID_SENTINEL]);

    for version in [0, -1, i64::MIN] {
        invalid_contract(
            SourceReference::try_from(SourceReferenceInput::MemoryVersion {
                memory_id: "memory-key".to_owned(),
                version,
            }),
            FieldCode::MemoryVersion,
            ValidationReason::NotPositive,
        );
    }

    for revision in [0, -1, i64::MIN] {
        invalid_contract(
            Revision::try_from(revision),
            FieldCode::Revision,
            ValidationReason::NotPositive,
        );
    }
}

#[test]
fn source_key_bound_counts_utf8_bytes_for_both_owner_types() {
    assert_eq!(MAX_SOURCE_KEY_BYTES, 2_048);
    let exact_bound = "é".repeat(MAX_SOURCE_KEY_BYTES / "é".len());
    assert_eq!(exact_bound.len(), MAX_SOURCE_KEY_BYTES);

    for source in [
        SourceReferenceInput::MemoryVersion {
            memory_id: exact_bound.clone(),
            version: 1,
        },
        SourceReferenceInput::SessionRecord {
            session_record_id: exact_bound.clone(),
        },
    ] {
        SourceReference::try_from(source).expect("2,048 UTF-8 bytes should be accepted");
    }

    let over_bound = format!("{exact_bound}a");
    assert_eq!(over_bound.len(), MAX_SOURCE_KEY_BYTES + 1);
    invalid_contract(
        SourceReference::try_from(SourceReferenceInput::MemoryVersion {
            memory_id: over_bound.clone(),
            version: 1,
        }),
        FieldCode::SourceKey,
        ValidationReason::TooLong,
    );
    invalid_contract(
        SourceReference::try_from(SourceReferenceInput::SessionRecord {
            session_record_id: over_bound,
        }),
        FieldCode::SourceKey,
        ValidationReason::TooLong,
    );
}

#[test]
fn revisions_are_positive_i64_values() {
    for revision in [1, i64::MAX] {
        assert_eq!(
            Revision::try_from(revision)
                .expect("positive revision should pass")
                .get(),
            revision
        );
    }
}

#[test]
fn fixed_relation_vocabulary_has_exact_direction_semantics() {
    let expected = [
        "related_to",
        "is_a",
        "part_of",
        "depends_on",
        "uses",
        "implements",
        "causes",
        "resolves",
        "contradicts",
    ];
    assert_eq!(
        RelationType::ALL
            .iter()
            .map(|relation| relation.as_str())
            .collect::<Vec<_>>(),
        expected
    );

    for &relation in RelationType::ALL {
        assert_eq!(
            RelationType::try_from(relation.as_str()).expect("fixed relation should parse"),
            relation
        );
        assert_eq!(
            relation.semantics(),
            if matches!(
                relation,
                RelationType::RelatedTo | RelationType::Contradicts
            ) {
                RelationSemantics::Symmetric
            } else {
                RelationSemantics::Directed
            }
        );
    }

    invalid_contract(
        RelationType::try_from("supports"),
        FieldCode::RelationType,
        ValidationReason::Unsupported,
    );
}

#[test]
fn direction_modes_and_edge_orientations_have_stable_codes() {
    for (direction, code) in [
        (DirectionMode::Outgoing, "outgoing"),
        (DirectionMode::Incoming, "incoming"),
        (DirectionMode::Either, "either"),
    ] {
        assert_eq!(direction.as_str(), code);
        assert_eq!(
            DirectionMode::try_from(code).expect("direction should parse"),
            direction
        );
    }
    invalid_contract(
        DirectionMode::try_from("both"),
        FieldCode::Direction,
        ValidationReason::Unsupported,
    );

    assert_eq!(
        EdgeOrientation::ALL
            .iter()
            .map(|orientation| orientation.as_str())
            .collect::<Vec<_>>(),
        ["outgoing", "incoming", "symmetric"]
    );
}

#[test]
fn operation_field_and_reason_code_sets_are_stable() {
    assert_eq!(
        OperationCode::ALL
            .iter()
            .map(|operation| operation.as_str())
            .collect::<Vec<_>>(),
        [
            "create_concept",
            "replace_concept_aliases",
            "delete_concept",
            "register_source",
            "delete_source",
            "create_mention",
            "delete_mention",
            "create_assertion",
            "update_assertion",
            "delete_assertion",
            "add_evidence",
            "remove_evidence",
            "get_concept",
            "resolve_alias",
            "get_assertion",
            "get_source",
            "neighbors",
            "related_sources",
            "find_paths",
        ]
    );
    assert_eq!(
        FieldCode::ALL
            .iter()
            .map(|field| field.as_str())
            .collect::<Vec<_>>(),
        [
            "alias",
            "alias_key",
            "alias_set",
            "preferred_alias",
            "source_kind",
            "memory_id",
            "memory_version",
            "session_record_id",
            "source_key",
            "concept_id",
            "assertion_id",
            "subject_concept_id",
            "object_concept_id",
            "relation_type",
            "revision",
            "direction",
            "relation_filter",
            "orientation",
            "path",
            "limit",
            "max_depth",
            "max_work",
            "query_timeout",
            "invocation_timeout",
            "supporting_sources",
        ]
    );
    assert_eq!(
        ValidationReason::ALL
            .iter()
            .map(|reason| reason.as_str())
            .collect::<Vec<_>>(),
        [
            "empty",
            "contains_nul",
            "too_short",
            "too_long",
            "not_positive",
            "invalid_uuid",
            "nil_uuid",
            "not_uuid_v7",
            "unsupported",
            "invalid_shape",
            "self_relation",
        ]
    );
    assert_eq!(
        ConflictField::ALL
            .iter()
            .map(|field| field.as_str())
            .collect::<Vec<_>>(),
        ["alias_set", "revision", "semantic_assertion", "snapshot"]
    );
    assert_eq!(
        ConflictReason::ALL
            .iter()
            .map(|reason| reason.as_str())
            .collect::<Vec<_>>(),
        [
            "alias_owned",
            "alias_set_mismatch",
            "stale_revision",
            "duplicate_semantic_assertion",
            "snapshot_drift",
        ]
    );
}

#[test]
fn failure_categories_expose_stable_reason_codes() {
    assert_eq!(ReferenceReason::InUse.as_str(), "in_use");
    assert_eq!(NotFoundReason::Missing.as_str(), "missing");
    assert_eq!(LimitReason::Exceeded.as_str(), "exceeded");
    assert_eq!(
        LimitResource::ALL
            .iter()
            .map(|resource| resource.as_str())
            .collect::<Vec<_>>(),
        [
            "alias_count",
            "assertion_evidence_count",
            "query_limit",
            "path_depth",
            "path_work",
            "response_bytes",
        ]
    );
    assert_eq!(TraversalBound::Depth.as_str(), "depth");
    assert_eq!(TraversalBound::Work.as_str(), "work");
    assert_eq!(TraversalBound::QueryTimeout.as_str(), "query_timeout");
    assert_eq!(TraversalReason::WorkExhausted.as_str(), "work_exhausted");
    assert_eq!(TraversalReason::QueryTimeout.as_str(), "query_timeout");
    assert_eq!(
        BackendFailureReason::ALL
            .iter()
            .map(|reason| reason.as_str())
            .collect::<Vec<_>>(),
        [
            "invocation_failed",
            "database_failure",
            "malformed_response",
            "unknown_outcome",
            "unsupported_value",
        ]
    );
    assert_eq!(
        RecordKind::ALL
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>(),
        [
            "concept",
            "assertion",
            "source_reference",
            "concept_mention",
            "assertion_evidence",
        ]
    );
    assert_eq!(
        SourceKind::ALL
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>(),
        ["memory_version", "session_record"]
    );
    assert_eq!(
        RelationSemantics::ALL
            .iter()
            .map(|semantics| semantics.as_str())
            .collect::<Vec<_>>(),
        ["directed", "symmetric"]
    );
}

#[test]
fn graph_errors_format_only_stable_codes_and_redact_record_identities() {
    const MEMORY_KEY: &str = "memory-source-key-protected-sentinel";
    const SESSION_KEY: &str = "session-source-key-protected-sentinel";
    const SOURCE_CONTENT: &str = "source-content-protected-sentinel";
    const BACKEND_MESSAGE: &str = "backend-statement-protected-sentinel";

    let concept_uuid = valid_uuid_v7();
    let assertion_uuid = valid_uuid_v7();
    let concept_sentinel = concept_uuid.to_string();
    let assertion_sentinel = assertion_uuid.to_string();
    let concept_id = ConceptId::try_from(concept_uuid).expect("UUIDv7 concept ID should pass");
    let assertion_id = knowledge_graph_store::contracts::AssertionId::try_from(assertion_uuid)
        .expect("UUIDv7 assertion ID should pass");
    let revision = Revision::try_from(4).expect("positive revision should pass");
    let contract_error = SourceReference::try_from(SourceReferenceInput::MemoryVersion {
        memory_id: format!("{MEMORY_KEY}\0"),
        version: 2,
    })
    .expect_err("NUL-bearing memory source should be rejected");
    let source_input = SourceReferenceInput::MemoryVersion {
        memory_id: MEMORY_KEY.to_owned(),
        version: 2,
    };
    let session_input = SourceReferenceInput::SessionRecord {
        session_record_id: SESSION_KEY.to_owned(),
    };

    let errors = [
        GraphError::invalid_input(OperationCode::RegisterSource, contract_error),
        GraphError::NotFound {
            operation: OperationCode::GetConcept,
            record_kind: RecordKind::Concept,
            identity: Some(RecordIdentity::Concept(concept_id)),
            reason: NotFoundReason::Missing,
        },
        GraphError::NotFound {
            operation: OperationCode::GetAssertion,
            record_kind: RecordKind::Assertion,
            identity: Some(RecordIdentity::Assertion(assertion_id)),
            reason: NotFoundReason::Missing,
        },
        GraphError::NotFound {
            operation: OperationCode::CreateMention,
            record_kind: RecordKind::SourceReference,
            identity: None,
            reason: NotFoundReason::Missing,
        },
        GraphError::Conflict {
            operation: OperationCode::UpdateAssertion,
            field: ConflictField::Revision,
            reason: ConflictReason::StaleRevision,
            identity: RecordIdentity::Assertion(assertion_id),
            current_revision: Some(revision),
        },
        GraphError::Referenced {
            operation: OperationCode::DeleteSource,
            record: ReferencedRecord::SourceReference,
            reason: ReferenceReason::InUse,
        },
        GraphError::WouldOrphanAssertion {
            operation: OperationCode::RemoveEvidence,
            assertion_id,
            revision,
        },
        GraphError::LimitExceeded {
            operation: OperationCode::AddEvidence,
            resource: LimitResource::AssertionEvidenceCount,
            reason: LimitReason::Exceeded,
        },
        GraphError::TraversalBoundExceeded {
            operation: OperationCode::FindPaths,
            bound: TraversalBound::Work,
            reason: TraversalReason::WorkExhausted,
        },
        GraphError::TraversalBoundExceeded {
            operation: OperationCode::RelatedSources,
            bound: TraversalBound::QueryTimeout,
            reason: TraversalReason::QueryTimeout,
        },
        GraphError::ResultBoundExceeded {
            operation: OperationCode::FindPaths,
        },
        GraphError::DatabaseFailure {
            operation: OperationCode::RegisterSource,
            reason: BackendFailureReason::InvocationFailed,
        },
        GraphError::InvalidResponse {
            operation: OperationCode::GetConcept,
            reason: BackendFailureReason::MalformedResponse,
        },
    ];

    assert_eq!(
        errors.iter().map(ToString::to_string).collect::<Vec<_>>(),
        [
            "invalid_input:register_source:memory_id:contains_nul",
            "not_found:get_concept:concept:missing",
            "not_found:get_assertion:assertion:missing",
            "not_found:create_mention:source_reference:missing",
            "conflict:update_assertion:revision:stale_revision",
            "referenced:delete_source:source_reference:in_use",
            "would_orphan_assertion:remove_evidence",
            "limit_exceeded:add_evidence:assertion_evidence_count:exceeded",
            "traversal_bound_exceeded:find_paths:work:work_exhausted",
            "traversal_bound_exceeded:related_sources:query_timeout:query_timeout",
            "result_bound_exceeded:find_paths",
            "database_failure:register_source:invocation_failed",
            "invalid_response:get_concept:malformed_response",
        ]
    );

    for error in &errors {
        assert_error_is_opaque(
            error,
            &[
                MEMORY_KEY,
                SESSION_KEY,
                SOURCE_CONTENT,
                BACKEND_MESSAGE,
                &concept_sentinel,
                &assertion_sentinel,
            ],
        );
    }
    assert_debug_is_opaque(&source_input, &[MEMORY_KEY, SOURCE_CONTENT]);
    assert_debug_is_opaque(&session_input, &[SESSION_KEY, SOURCE_CONTENT]);
}

fn assert_error_is_opaque<E: Debug + Display>(error: &E, sentinels: &[&str]) {
    let display = error.to_string();
    let debug = format!("{error:?}");

    for sentinel in sentinels {
        assert!(
            !display.contains(sentinel),
            "Display output leaked protected value {sentinel:?}: {display}"
        );
        assert!(
            !debug.contains(sentinel),
            "Debug output leaked protected value {sentinel:?}: {debug}"
        );
    }
}

fn assert_debug_is_opaque<E: Debug + ?Sized>(value: &E, sentinels: &[&str]) {
    let debug = format!("{value:?}");

    for sentinel in sentinels {
        assert!(
            !debug.contains(sentinel),
            "Debug output leaked protected value {sentinel:?}: {debug}"
        );
    }
}

fn assert_contract_round_trip<T>(value: T)
where
    T: DeserializeOwned + Eq + Debug + Serialize,
{
    let encoded = serde_json::to_vec(&value).expect("contract should serialize");
    let decoded: T = serde_json::from_slice(&encoded).expect("contract should deserialize");
    assert_eq!(decoded, value);
}

#[test]
fn every_service_mutation_input_has_a_strict_round_trip_contract() {
    const ALIAS_SENTINEL: &str = "graph-alias-protected-sentinel";
    const MEMORY_KEY: &str = "memory-source-key-protected-sentinel";
    const SESSION_KEY: &str = "session-source-key-protected-sentinel";

    let concept_id = ConceptId::try_from(valid_uuid_v7()).expect("valid concept ID");
    let other_concept_id = ConceptId::try_from(valid_uuid_v7()).expect("valid concept ID");
    let assertion_id = AssertionId::try_from(valid_uuid_v7()).expect("valid assertion ID");
    let revision = Revision::try_from(7).expect("positive revision");
    let preferred_alias = Alias {
        display_text: ALIAS_SENTINEL.to_owned(),
        preferred: true,
    };
    let additional_alias = Alias {
        display_text: "additional graph alias".to_owned(),
        preferred: false,
    };
    let memory_source = SourceReferenceInput::MemoryVersion {
        memory_id: MEMORY_KEY.to_owned(),
        version: 3,
    };
    let session_source = SourceReferenceInput::SessionRecord {
        session_record_id: SESSION_KEY.to_owned(),
    };

    let memory_json = serde_json::to_value(&memory_source).expect("memory source serializes");
    assert_eq!(memory_json["kind"], "memory_version");
    assert_eq!(memory_json["memory_id"], MEMORY_KEY);
    assert_eq!(memory_json["version"], 3);
    assert!(memory_json.get("session_record_id").is_none());
    let session_json = serde_json::to_value(&session_source).expect("session source serializes");
    assert_eq!(session_json["kind"], "session_record");
    assert_eq!(session_json["session_record_id"], SESSION_KEY);
    assert!(session_json.get("version").is_none());
    assert_contract_round_trip(memory_source.clone());
    assert_contract_round_trip(session_source.clone());

    let create_concept = CreateConcept {
        aliases: vec![preferred_alias.clone(), additional_alias.clone()],
    };
    let create_json = serde_json::to_value(&create_concept).expect("create concept serializes");
    assert_eq!(create_json["aliases"][0]["display_text"], ALIAS_SENTINEL);
    assert_eq!(create_json["aliases"][0]["preferred"], true);
    assert_contract_round_trip(create_concept.clone());

    let replace_aliases = ReplaceConceptAliases {
        concept_id,
        expected_revision: revision,
        aliases: vec![preferred_alias.clone(), additional_alias.clone()],
    };
    assert_contract_round_trip(replace_aliases.clone());
    assert_contract_round_trip(DeleteConcept {
        concept_id,
        expected_revision: revision,
    });
    assert_contract_round_trip(DeleteSource {
        source: session_source.clone(),
    });
    assert_contract_round_trip(ConceptMentionInput {
        concept_id,
        source: memory_source.clone(),
    });
    assert_contract_round_trip(CreateAssertion {
        subject_concept_id: concept_id,
        relation_type: RelationType::DependsOn,
        object_concept_id: other_concept_id,
        supporting_sources: vec![memory_source.clone(), session_source.clone()],
    });
    assert_contract_round_trip(UpdateAssertion {
        assertion_id,
        expected_revision: revision,
        subject_concept_id: other_concept_id,
        relation_type: RelationType::IsA,
        object_concept_id: concept_id,
    });
    assert_contract_round_trip(DeleteAssertion {
        assertion_id,
        expected_revision: revision,
    });
    assert_contract_round_trip(AssertionEvidenceInput {
        assertion_id,
        source: memory_source.clone(),
    });

    let memory_record = SourceReference::try_from(memory_source).expect("valid memory source");
    let session_record = SourceReference::try_from(session_source).expect("valid session source");
    let concept = Concept {
        id: concept_id,
        revision,
        aliases: vec![preferred_alias, additional_alias],
    };
    assert_contract_round_trip(concept.clone());
    assert_contract_round_trip(ConceptMention {
        concept_id,
        source: memory_record.clone(),
    });
    assert_contract_round_trip(AssertionEvidence {
        assertion_id,
        source: session_record.clone(),
    });
    assert_contract_round_trip(Assertion {
        id: assertion_id,
        revision,
        subject_concept_id: concept_id,
        relation_type: RelationType::DependsOn,
        object_concept_id: other_concept_id,
        evidence: vec![memory_record, session_record],
    });

    for protected in [ALIAS_SENTINEL, MEMORY_KEY, SESSION_KEY] {
        assert_debug_is_opaque(&replace_aliases, &[protected]);
    }
    assert_debug_is_opaque(&create_concept, &[ALIAS_SENTINEL]);
}

#[test]
fn mutation_inputs_reject_unknown_fields_and_mixed_source_shapes() {
    let unknown_alias_field = serde_json::json!({
        "aliases": [{"display_text": "alias", "preferred": true}],
        "source_content": "must-not-be-accepted"
    });
    assert!(serde_json::from_value::<CreateConcept>(unknown_alias_field).is_err());

    let mixed_source_shape = serde_json::json!({
        "kind": "memory_version",
        "memory_id": "opaque-key",
        "version": 1,
        "session_record_id": "not-valid-for-this-variant"
    });
    assert!(serde_json::from_value::<SourceReferenceInput>(mixed_source_shape).is_err());
}

#[test]
fn mutation_inputs_and_records_redact_aliases_and_source_keys_in_debug() {
    const ALIAS_SENTINEL: &str = "graph-alias-protected-sentinel";
    const MEMORY_KEY: &str = "memory-source-key-protected-sentinel";
    const SESSION_KEY: &str = "session-source-key-protected-sentinel";

    let concept_id = ConceptId::try_from(valid_uuid_v7()).expect("valid concept ID");
    let other_concept_id = ConceptId::try_from(valid_uuid_v7()).expect("valid concept ID");
    let assertion_id = knowledge_graph_store::contracts::AssertionId::try_from(valid_uuid_v7())
        .expect("valid assertion ID");
    let revision = Revision::try_from(9).expect("positive revision");
    let alias = Alias {
        display_text: ALIAS_SENTINEL.to_owned(),
        preferred: true,
    };
    let memory_input = SourceReferenceInput::MemoryVersion {
        memory_id: MEMORY_KEY.to_owned(),
        version: 4,
    };
    let session_input = SourceReferenceInput::SessionRecord {
        session_record_id: SESSION_KEY.to_owned(),
    };
    let memory_source = SourceReference::try_from(memory_input.clone()).expect("valid source");
    let session_source = SourceReference::try_from(session_input.clone()).expect("valid source");
    let concept = Concept {
        id: concept_id,
        revision,
        aliases: vec![alias.clone()],
    };
    let assertion = Assertion {
        id: assertion_id,
        revision,
        subject_concept_id: concept_id,
        relation_type: RelationType::RelatedTo,
        object_concept_id: other_concept_id,
        evidence: vec![memory_source.clone(), session_source.clone()],
    };
    let values: [&dyn Debug; 17] = [
        &alias,
        &CreateConcept {
            aliases: vec![alias.clone()],
        },
        &ReplaceConceptAliases {
            concept_id,
            expected_revision: revision,
            aliases: vec![alias.clone()],
        },
        &DeleteConcept {
            concept_id,
            expected_revision: revision,
        },
        &memory_input,
        &session_input,
        &DeleteSource {
            source: memory_input.clone(),
        },
        &ConceptMentionInput {
            concept_id,
            source: memory_input.clone(),
        },
        &CreateAssertion {
            subject_concept_id: concept_id,
            relation_type: RelationType::RelatedTo,
            object_concept_id: other_concept_id,
            supporting_sources: vec![memory_input.clone(), session_input.clone()],
        },
        &UpdateAssertion {
            assertion_id,
            expected_revision: revision,
            subject_concept_id: concept_id,
            relation_type: RelationType::RelatedTo,
            object_concept_id: other_concept_id,
        },
        &DeleteAssertion {
            assertion_id,
            expected_revision: revision,
        },
        &AssertionEvidenceInput {
            assertion_id,
            source: session_input.clone(),
        },
        &memory_source,
        &session_source,
        &concept,
        &ConceptMention {
            concept_id,
            source: memory_source.clone(),
        },
        &AssertionEvidence {
            assertion_id,
            source: session_source.clone(),
        },
    ];
    for value in values {
        assert_debug_is_opaque(value, &[ALIAS_SENTINEL, MEMORY_KEY, SESSION_KEY]);
    }
    assert_debug_is_opaque(&assertion, &[ALIAS_SENTINEL, MEMORY_KEY, SESSION_KEY]);
}

#[test]
fn validated_serde_values_reject_invalid_ids_revisions_and_source_references() {
    assert!(
        serde_json::from_value::<ConceptId>(serde_json::json!(Uuid::new_v4().to_string())).is_err()
    );
    assert!(serde_json::from_value::<Revision>(serde_json::json!(0)).is_err());
    assert!(serde_json::from_value::<Revision>(serde_json::json!(-3)).is_err());

    let empty_memory_key = serde_json::json!({
        "kind": "memory_version",
        "memory_id": "",
        "version": 1
    });
    assert!(serde_json::from_value::<SourceReference>(empty_memory_key).is_err());

    let invalid_version = serde_json::json!({
        "kind": "memory_version",
        "memory_id": "opaque-memory-id",
        "version": 0
    });
    assert!(serde_json::from_value::<SourceReference>(invalid_version).is_err());

    let oversized_session_key = format!("{}x", "s".repeat(MAX_SOURCE_KEY_BYTES));
    let oversized_session = serde_json::json!({
        "kind": "session_record",
        "session_record_id": oversized_session_key
    });
    assert!(serde_json::from_value::<SourceReference>(oversized_session).is_err());
}

#[test]
fn read_inputs_have_strict_typed_serialization_and_declared_bounds() {
    assert_eq!(MAX_ALIAS_DISPLAY_BYTES, 512);
    assert_eq!(MAX_ALIAS_KEY_BYTES, 2_048);
    assert_eq!(MAX_ASSERTION_EVIDENCE, 32);
    assert_eq!(MAX_CONCEPT_ALIASES, 64);
    assert_eq!(MAX_NEIGHBOR_RESULTS, 256);
    assert_eq!(MAX_RELATED_SOURCE_RESULTS, 256);
    assert_eq!(MAX_PATH_RESULTS, 50);
    assert_eq!(MAX_PATH_DEPTH, 8);
    assert_eq!(MAX_PATH_WORK, 10_000);
    assert_eq!(MAX_RESPONSE_JSON_BYTES, 4 * 1024 * 1024);

    let concept_id = ConceptId::try_from(valid_uuid_v7()).expect("valid concept ID");
    let other_concept_id = ConceptId::try_from(valid_uuid_v7()).expect("valid concept ID");
    let assertion_id = AssertionId::try_from(valid_uuid_v7()).expect("valid assertion ID");
    let concept_input = ConceptIdInput {
        id: concept_id.as_uuid().to_string(),
    };
    let assertion_input = AssertionIdInput {
        id: assertion_id.as_uuid().to_string(),
    };
    assert_contract_round_trip(concept_input.clone());
    assert_contract_round_trip(assertion_input.clone());

    let alias = AliasQuery::try_from("  Mixed-case alias  ".to_owned()).expect("valid alias query");
    assert_eq!(alias.alias, "  Mixed-case alias  ");
    assert_contract_round_trip(alias.clone());
    assert_debug_is_opaque(&alias, &["Mixed-case alias"]);

    let neighbor = NeighborQuery {
        concept_id,
        direction: DirectionMode::Either,
        relation_filter: vec![RelationType::Uses, RelationType::IsA],
        limit: 256,
    };
    let related_sources = RelatedSourceQuery {
        concept_id,
        limit: 256,
    };
    let path = PathQuery {
        from: concept_id,
        to: other_concept_id,
        direction: DirectionMode::Incoming,
        max_depth: 8,
        max_work: 10_000,
        limit: 50,
    };
    assert_contract_round_trip(neighbor.clone());
    assert_contract_round_trip(related_sources.clone());
    assert_contract_round_trip(path.clone());
    let timeouts = DatabaseTimeouts::default();
    assert_eq!(timeouts.query(), Duration::from_secs(10));
    assert_eq!(timeouts.invocation(), Duration::from_secs(15));

    assert!(
        serde_json::from_value::<NeighborQuery>(serde_json::json!({
            "concept_id": concept_id,
            "direction": "both",
            "limit": 1
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<NeighborQuery>(serde_json::json!({
            "concept_id": concept_id,
            "direction": "either",
            "relation_filter": ["supports"],
            "limit": 1
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<PathQuery>(serde_json::json!({
            "from": concept_id,
            "to": other_concept_id,
            "direction": "both",
            "max_depth": 1,
            "max_work": 1,
            "limit": 1
        }))
        .is_err()
    );

    for (value, ty) in [
        (
            serde_json::json!({"id": concept_input.id, "unexpected": true}),
            "concept ID input",
        ),
        (
            serde_json::json!({"id": assertion_input.id, "unexpected": true}),
            "assertion ID input",
        ),
        (
            serde_json::json!({"alias": "alias", "source_content": "forbidden"}),
            "alias query",
        ),
        (
            serde_json::json!({
                "concept_id": concept_id,
                "direction": "either",
                "relation_filter": ["is_a"],
                "limit": 1,
                "source_content": "forbidden"
            }),
            "neighbor query",
        ),
        (
            serde_json::json!({"concept_id": concept_id, "limit": 1, "extra": true}),
            "related-source query",
        ),
        (
            serde_json::json!({
                "from": concept_id,
                "to": other_concept_id,
                "direction": "outgoing",
                "max_depth": 1,
                "max_work": 1,
                "limit": 1,
                "extra": true
            }),
            "path query",
        ),
    ] {
        let error = match ty {
            "concept ID input" => serde_json::from_value::<ConceptIdInput>(value).unwrap_err(),
            "assertion ID input" => serde_json::from_value::<AssertionIdInput>(value).unwrap_err(),
            "alias query" => serde_json::from_value::<AliasQuery>(value).unwrap_err(),
            "neighbor query" => serde_json::from_value::<NeighborQuery>(value).unwrap_err(),
            "related-source query" => {
                serde_json::from_value::<RelatedSourceQuery>(value).unwrap_err()
            }
            "path query" => serde_json::from_value::<PathQuery>(value).unwrap_err(),
            _ => unreachable!("all input kinds are listed"),
        };
        assert!(error.to_string().contains("unknown field"), "{ty}: {error}");
    }

    invalid_contract(
        AliasQuery::try_from(String::new()),
        FieldCode::Alias,
        ValidationReason::Empty,
    );
    invalid_contract(
        AliasQuery::try_from("alias\0sentinel".to_owned()),
        FieldCode::Alias,
        ValidationReason::ContainsNul,
    );
    let protected_alias = "alias-query-protected-sentinel";
    let alias_error = AliasQuery::try_from(format!("{protected_alias}\0"))
        .expect_err("NUL-bearing alias is rejected");
    assert_error_is_opaque(&alias_error, &[protected_alias]);
    assert!(serde_json::from_value::<AliasQuery>(serde_json::json!({"alias": ""})).is_err());
    let max_alias = "é".repeat(MAX_ALIAS_DISPLAY_BYTES / "é".len());
    assert_eq!(max_alias.len(), MAX_ALIAS_DISPLAY_BYTES);
    AliasQuery::try_from(max_alias.clone()).expect("512 UTF-8 bytes are accepted");
    invalid_contract(
        AliasQuery::try_from(format!("{max_alias}x")),
        FieldCode::Alias,
        ValidationReason::TooLong,
    );

    assert_eq!(RelatedSourceRole::Mention.as_str(), "mention");
    assert_eq!(
        RelatedSourceRole::AssertionEvidence.as_str(),
        "assertion_evidence"
    );
    assert_eq!(
        RelatedSourceRole::ALL
            .iter()
            .map(|role| role.as_str())
            .collect::<Vec<_>>(),
        ["mention", "assertion_evidence"]
    );
}

#[test]
fn read_outputs_round_trip_complete_evidence_empty_results_and_oriented_paths() {
    let concept_id = ConceptId::try_from(valid_uuid_v7()).expect("valid concept ID");
    let other_concept_id = ConceptId::try_from(valid_uuid_v7()).expect("valid concept ID");
    let assertion_id = AssertionId::try_from(valid_uuid_v7()).expect("valid assertion ID");
    let revision = Revision::try_from(3).expect("positive revision");
    let memory_source = SourceReference::try_from(SourceReferenceInput::MemoryVersion {
        memory_id: "memory-evidence-key".to_owned(),
        version: 2,
    })
    .expect("valid memory source");
    let session_source = SourceReference::try_from(SourceReferenceInput::SessionRecord {
        session_record_id: "session-evidence-key".to_owned(),
    })
    .expect("valid session source");
    let concept = Concept {
        id: concept_id,
        revision,
        aliases: vec![Alias {
            display_text: "origin concept".to_owned(),
            preferred: true,
        }],
    };
    let neighbor_concept = Concept {
        id: other_concept_id,
        revision,
        aliases: vec![Alias {
            display_text: "neighbor concept".to_owned(),
            preferred: true,
        }],
    };
    let assertion = Assertion {
        id: assertion_id,
        revision,
        subject_concept_id: concept_id,
        relation_type: RelationType::DependsOn,
        object_concept_id: other_concept_id,
        evidence: vec![memory_source.clone(), session_source.clone()],
    };
    let neighbor = NeighborResult {
        neighbor: neighbor_concept,
        assertion: assertion.clone(),
        orientation: EdgeOrientation::Outgoing,
    };
    let related_source = RelatedSourceResult {
        source: memory_source,
        role: RelatedSourceRole::Mention,
    };
    let path = GraphPath {
        concepts: vec![concept_id, other_concept_id],
        assertions: vec![OrientedAssertion {
            assertion: assertion.clone(),
            orientation: EdgeOrientation::Outgoing,
        }],
    };

    assert_contract_round_trip(neighbor.clone());
    assert_contract_round_trip(related_source.clone());
    assert_contract_round_trip(path.clone());
    assert_contract_round_trip(Some(concept));
    assert_contract_round_trip(Some(assertion.clone()));
    assert_contract_round_trip(Option::<Assertion>::None);
    assert_contract_round_trip(Some(session_source));
    assert_contract_round_trip(Option::<SourceReference>::None);
    assert_contract_round_trip(Vec::<NeighborResult>::new());
    assert_contract_round_trip(Vec::<RelatedSourceResult>::new());
    assert_contract_round_trip(Vec::<GraphPath>::new());

    let neighbor_json = serde_json::to_value(&neighbor).expect("neighbor serializes");
    assert_eq!(
        neighbor_json["assertion"]["evidence"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(neighbor_json["orientation"], "outgoing");
    let path_json = serde_json::to_value(&path).expect("path serializes");
    assert_eq!(path_json["concepts"].as_array().unwrap().len(), 2);
    assert_eq!(path_json["assertions"][0]["orientation"], "outgoing");
    assert_eq!(
        path_json["assertions"][0]["assertion"]["evidence"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        related_source.role,
        RelatedSourceRole::Mention,
        "the source role is part of the typed result"
    );
}
