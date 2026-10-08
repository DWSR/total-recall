use crate::{
    contracts::{
        Assertion, AssertionEvidence, AssertionEvidenceInput, AssertionId, AssertionIdInput,
        Concept, ConceptId, ConceptIdInput, ConceptMention, ConceptMentionInput, CreateAssertion,
        CreateConcept, DeleteAssertion, DeleteConcept, DeleteSource, GraphError, GraphPath,
        GraphResult, NeighborQuery, NeighborResult, OperationCode, PathQuery, RelatedSourceQuery,
        RelatedSourceResult, ReplaceConceptAliases, SemanticAssertionIdentity, SourceReference,
        SourceReferenceInput, UpdateAssertion, ValidatedAssertionEvidence,
        ValidatedAssertionIdInput, ValidatedConceptIdInput, ValidatedConceptMention,
        ValidatedCreateAssertion, ValidatedCreateConcept, ValidatedDeleteAssertion,
        ValidatedDeleteConcept, ValidatedDeleteSource, ValidatedEvidenceSources,
        ValidatedNeighborQuery, ValidatedPathQuery, ValidatedRelatedSourceQuery,
        ValidatedReplaceConceptAliases, ValidatedSourceReference, ValidatedUpdateAssertion,
    },
    database::KnowledgeGraphDatabase,
    normalization::{normalize_alias_key, normalize_alias_set},
};
use uuid::Uuid;

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct KnowledgeGraphStore<D> {
    database: D,
}

#[cfg_attr(not(test), allow(dead_code))]
impl<D> KnowledgeGraphStore<D> {
    pub(crate) const fn new(database: D) -> Self {
        Self { database }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
impl<D: KnowledgeGraphDatabase> KnowledgeGraphStore<D> {
    pub(crate) async fn create_concept(&self, input: CreateConcept) -> GraphResult<Concept> {
        let aliases = normalize_alias_set(&input.aliases)
            .map_err(|error| GraphError::invalid_input(OperationCode::CreateConcept, error))?;

        let candidate_id = ConceptId::try_from(Uuid::now_v7())
            .map_err(|error| GraphError::invalid_input(OperationCode::CreateConcept, error))?;
        let validated = ValidatedCreateConcept::new(candidate_id, aliases);

        self.database.create_concept(&validated).await
    }

    pub(crate) async fn replace_concept_aliases(
        &self,
        input: ReplaceConceptAliases,
    ) -> GraphResult<Concept> {
        let aliases = normalize_alias_set(&input.aliases).map_err(|error| {
            GraphError::invalid_input(OperationCode::ReplaceConceptAliases, error)
        })?;

        let validated =
            ValidatedReplaceConceptAliases::new(input.concept_id, input.expected_revision, aliases);

        self.database.replace_concept_aliases(&validated).await
    }

    pub(crate) async fn delete_concept(&self, input: DeleteConcept) -> GraphResult<()> {
        let validated = ValidatedDeleteConcept::from(input);
        self.database.delete_concept(&validated).await
    }

    pub(crate) async fn register_source(
        &self,
        input: SourceReferenceInput,
    ) -> GraphResult<SourceReference> {
        let validated = ValidatedSourceReference::try_from(input)
            .map_err(|error| GraphError::invalid_input(OperationCode::RegisterSource, error))?;
        self.database.register_source(&validated).await
    }

    pub(crate) async fn delete_source(&self, input: DeleteSource) -> GraphResult<()> {
        let validated = ValidatedDeleteSource::try_from(input)
            .map_err(|error| GraphError::invalid_input(OperationCode::DeleteSource, error))?;
        self.database.delete_source(&validated).await
    }

    pub(crate) async fn create_mention(
        &self,
        input: ConceptMentionInput,
    ) -> GraphResult<ConceptMention> {
        let validated = ValidatedConceptMention::try_from(input)
            .map_err(|error| GraphError::invalid_input(OperationCode::CreateMention, error))?;
        self.database.create_mention(&validated).await
    }

    pub(crate) async fn delete_mention(&self, input: ConceptMentionInput) -> GraphResult<()> {
        let validated = ValidatedConceptMention::try_from(input)
            .map_err(|error| GraphError::invalid_input(OperationCode::DeleteMention, error))?;
        self.database.delete_mention(&validated).await
    }

    pub(crate) async fn create_assertion(&self, input: CreateAssertion) -> GraphResult<Assertion> {
        let identity = SemanticAssertionIdentity::try_new(
            input.subject_concept_id,
            input.relation_type,
            input.object_concept_id,
        )
        .map_err(|error| GraphError::invalid_input(OperationCode::CreateAssertion, error))?;
        let supporting_sources = ValidatedEvidenceSources::try_from(input.supporting_sources)
            .map_err(|error| GraphError::invalid_input(OperationCode::CreateAssertion, error))?;

        let candidate_id = AssertionId::try_from(Uuid::now_v7())
            .map_err(|error| GraphError::invalid_input(OperationCode::CreateAssertion, error))?;
        let validated = ValidatedCreateAssertion {
            candidate_id,
            identity,
            supporting_sources,
        };

        self.database.create_assertion(&validated).await
    }

    pub(crate) async fn update_assertion(&self, input: UpdateAssertion) -> GraphResult<Assertion> {
        let validated = ValidatedUpdateAssertion::try_from_input(input)
            .map_err(|error| GraphError::invalid_input(OperationCode::UpdateAssertion, error))?;
        self.database.update_assertion(&validated).await
    }

    pub(crate) async fn delete_assertion(&self, input: DeleteAssertion) -> GraphResult<()> {
        let validated = ValidatedDeleteAssertion::from(input);
        self.database.delete_assertion(&validated).await
    }

    pub(crate) async fn add_evidence(
        &self,
        input: AssertionEvidenceInput,
    ) -> GraphResult<AssertionEvidence> {
        let validated = ValidatedAssertionEvidence::try_from(input)
            .map_err(|error| GraphError::invalid_input(OperationCode::AddEvidence, error))?;
        self.database.add_evidence(&validated).await
    }

    pub(crate) async fn remove_evidence(&self, input: AssertionEvidenceInput) -> GraphResult<()> {
        let validated = ValidatedAssertionEvidence::try_from(input)
            .map_err(|error| GraphError::invalid_input(OperationCode::RemoveEvidence, error))?;
        self.database.remove_evidence(&validated).await
    }

    pub(crate) async fn get_concept(&self, input: ConceptIdInput) -> GraphResult<Option<Concept>> {
        let validated = ValidatedConceptIdInput::try_from(input)
            .map_err(|error| GraphError::invalid_input(OperationCode::GetConcept, error))?;
        self.database.get_concept(&validated.id).await
    }

    pub(crate) async fn resolve_alias(
        &self,
        input: crate::contracts::AliasQuery,
    ) -> GraphResult<Option<Concept>> {
        let key = normalize_alias_key(&input.alias)
            .map_err(|error| GraphError::invalid_input(OperationCode::ResolveAlias, error))?;
        self.database.resolve_alias(&key).await
    }

    pub(crate) async fn get_assertion(
        &self,
        input: AssertionIdInput,
    ) -> GraphResult<Option<Assertion>> {
        let validated = ValidatedAssertionIdInput::try_from(input)
            .map_err(|error| GraphError::invalid_input(OperationCode::GetAssertion, error))?;
        self.database.get_assertion(validated.id).await
    }

    pub(crate) async fn get_source(
        &self,
        input: SourceReferenceInput,
    ) -> GraphResult<Option<SourceReference>> {
        let validated = ValidatedSourceReference::try_from(input)
            .map_err(|error| GraphError::invalid_input(OperationCode::GetSource, error))?;
        self.database.get_source(&validated).await
    }

    pub(crate) async fn neighbors(&self, input: NeighborQuery) -> GraphResult<Vec<NeighborResult>> {
        let validated = ValidatedNeighborQuery::try_from(input)
            .map_err(|error| GraphError::invalid_input(OperationCode::Neighbors, error))?;
        self.database.neighbors(&validated).await
    }

    pub(crate) async fn related_sources(
        &self,
        input: RelatedSourceQuery,
    ) -> GraphResult<Vec<RelatedSourceResult>> {
        let validated = ValidatedRelatedSourceQuery::try_from(input)
            .map_err(|error| GraphError::invalid_input(OperationCode::RelatedSources, error))?;
        self.database.related_sources(&validated).await
    }

    pub(crate) async fn find_paths(&self, input: PathQuery) -> GraphResult<Vec<GraphPath>> {
        let validated = ValidatedPathQuery::try_from(input)
            .map_err(|error| GraphError::invalid_input(OperationCode::FindPaths, error))?;
        self.database.find_paths(&validated).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use uuid::Uuid;

    use crate::{
        contracts::{
            Alias, AliasKey, AliasQuery, Assertion, AssertionEvidence, AssertionEvidenceInput,
            AssertionId, AssertionIdInput, BackendFailureReason, Concept, ConceptId,
            ConceptIdInput, ConceptMention, ConceptMentionInput, ConflictField, ConflictReason,
            CreateAssertion, CreateConcept, DeleteAssertion, DeleteConcept, DeleteSource,
            DirectionMode, EdgeOrientation, FieldCode, GraphError, GraphPath, GraphResult,
            LimitReason, LimitResource, MAX_ASSERTION_EVIDENCE, MAX_PATH_DEPTH, MAX_PATH_RESULTS,
            MAX_PATH_WORK, MAX_SOURCE_KEY_BYTES, NeighborQuery, NeighborResult, NotFoundReason,
            OperationCode, OrientedAssertion, PathQuery, RecordIdentity, RecordKind,
            ReferenceReason, ReferencedRecord, RelatedSourceQuery, RelatedSourceResult,
            RelatedSourceRole, RelationSemantics, RelationType, ReplaceConceptAliases, Revision,
            SourceReference, SourceReferenceInput, TraversalBound, TraversalReason,
            UpdateAssertion, ValidatedAssertionEvidence, ValidatedConceptMention,
            ValidatedCreateAssertion, ValidatedCreateConcept, ValidatedDeleteAssertion,
            ValidatedDeleteConcept, ValidatedDeleteSource, ValidatedNeighborQuery,
            ValidatedPathQuery, ValidatedRelatedSourceQuery, ValidatedReplaceConceptAliases,
            ValidatedSourceReference, ValidatedUpdateAssertion, ValidationReason,
        },
        database::{
            DatabaseResult, KnowledgeGraphDatabase,
            test_support::{
                FailingKnowledgeGraphDatabase, RecordedDatabaseCall,
                RecordingKnowledgeGraphDatabase,
            },
        },
        normalization::{normalize_alias_key, normalize_alias_set},
    };

    use crate::KnowledgeGraphStore;

    const UUID_V7_BASE: u128 = 0x0000_0000_0000_7000_8000_0000_0000_0000;

    fn concept_id(value: u128) -> ConceptId {
        ConceptId::try_from(Uuid::from_u128(UUID_V7_BASE + value))
            .expect("fixture concept ID should be UUIDv7")
    }

    fn assertion_id(value: u128) -> AssertionId {
        AssertionId::try_from(Uuid::from_u128(UUID_V7_BASE + value))
            .expect("fixture assertion ID should be UUIDv7")
    }

    fn revision(value: i64) -> Revision {
        Revision::try_from(value).expect("fixture revision should be positive")
    }

    fn alias(display_text: &str, preferred: bool) -> Alias {
        Alias {
            display_text: display_text.to_owned(),
            preferred,
        }
    }

    fn create_input() -> CreateConcept {
        CreateConcept {
            aliases: vec![alias("Graph Concept", true), alias("Graph-Concept", false)],
        }
    }

    fn replace_input(concept_id: ConceptId, expected_revision: Revision) -> ReplaceConceptAliases {
        ReplaceConceptAliases {
            concept_id,
            expected_revision,
            aliases: vec![
                alias("Revised Concept", true),
                alias("Previous Name", false),
                alias("Short Name", false),
            ],
        }
    }

    fn concept(id: ConceptId, revision: Revision, aliases: Vec<Alias>) -> Concept {
        Concept {
            id,
            revision,
            aliases,
        }
    }

    fn conflict(
        operation: OperationCode,
        field: ConflictField,
        reason: ConflictReason,
        identity: ConceptId,
        current_revision: Revision,
    ) -> GraphError {
        GraphError::Conflict {
            operation,
            field,
            reason,
            identity: RecordIdentity::Concept(identity),
            current_revision: Some(current_revision),
        }
    }

    fn concept_not_found(operation: OperationCode, id: ConceptId) -> GraphError {
        GraphError::NotFound {
            operation,
            record_kind: RecordKind::Concept,
            identity: Some(RecordIdentity::Concept(id)),
            reason: NotFoundReason::Missing,
        }
    }

    fn assertion_conflict(
        operation: OperationCode,
        field: ConflictField,
        reason: ConflictReason,
        identity: AssertionId,
        current_revision: Revision,
    ) -> GraphError {
        GraphError::Conflict {
            operation,
            field,
            reason,
            identity: RecordIdentity::Assertion(identity),
            current_revision: Some(current_revision),
        }
    }

    fn assertion_not_found(operation: OperationCode, id: AssertionId) -> GraphError {
        GraphError::NotFound {
            operation,
            record_kind: RecordKind::Assertion,
            identity: Some(RecordIdentity::Assertion(id)),
            reason: NotFoundReason::Missing,
        }
    }

    fn source_not_found(operation: OperationCode) -> GraphError {
        GraphError::NotFound {
            operation,
            record_kind: RecordKind::SourceReference,
            identity: None,
            reason: NotFoundReason::Missing,
        }
    }

    fn invalid_input(
        operation: OperationCode,
        field: FieldCode,
        reason: ValidationReason,
    ) -> GraphError {
        GraphError::InvalidInput {
            operation,
            field,
            reason,
        }
    }

    fn source_reference(input: SourceReferenceInput) -> SourceReference {
        SourceReference::try_from(input).expect("fixture source should be valid")
    }

    fn session_source_input(value: &str) -> SourceReferenceInput {
        SourceReferenceInput::SessionRecord {
            session_record_id: value.to_owned(),
        }
    }

    fn create_assertion_input(
        subject_concept_id: ConceptId,
        relation_type: RelationType,
        object_concept_id: ConceptId,
        supporting_sources: Vec<SourceReferenceInput>,
    ) -> CreateAssertion {
        CreateAssertion {
            subject_concept_id,
            relation_type,
            object_concept_id,
            supporting_sources,
        }
    }

    fn assertion_record(
        id: AssertionId,
        revision: Revision,
        subject_concept_id: ConceptId,
        relation_type: RelationType,
        object_concept_id: ConceptId,
        evidence: Vec<SourceReference>,
    ) -> Assertion {
        Assertion {
            id,
            revision,
            subject_concept_id,
            relation_type,
            object_concept_id,
            evidence,
        }
    }

    fn valid_path_query(from: ConceptId, to: ConceptId) -> PathQuery {
        PathQuery {
            from,
            to,
            direction: DirectionMode::Either,
            max_depth: 5,
            max_work: 81,
            limit: 7,
        }
    }

    fn graph_path(concepts: Vec<ConceptId>, assertions: Vec<OrientedAssertion>) -> GraphPath {
        let candidate = GraphPath {
            concepts,
            assertions,
        };
        let serialized = serde_json::to_value(&candidate).expect("fixture path should serialize");
        serde_json::from_value(serialized).expect("fixture path should satisfy the path contract")
    }

    fn assertion_evidence_input(
        assertion_id: AssertionId,
        source: SourceReferenceInput,
    ) -> AssertionEvidenceInput {
        AssertionEvidenceInput {
            assertion_id,
            source,
        }
    }

    fn mention_input(concept_id: ConceptId, source: SourceReferenceInput) -> ConceptMentionInput {
        ConceptMentionInput { concept_id, source }
    }

    fn invalid_source_inputs() -> Vec<(SourceReferenceInput, FieldCode, ValidationReason)> {
        const SOURCE_KEY_SENTINEL: &str = "PRIVATE_SOURCE_KEY_SENTINEL";
        let oversized_key = format!(
            "{SOURCE_KEY_SENTINEL}{}",
            "x".repeat(MAX_SOURCE_KEY_BYTES + 1 - SOURCE_KEY_SENTINEL.len())
        );
        assert_eq!(oversized_key.len(), MAX_SOURCE_KEY_BYTES + 1);

        vec![
            (
                SourceReferenceInput::MemoryVersion {
                    memory_id: String::new(),
                    version: 1,
                },
                FieldCode::MemoryId,
                ValidationReason::Empty,
            ),
            (
                SourceReferenceInput::MemoryVersion {
                    memory_id: format!("{SOURCE_KEY_SENTINEL}\0"),
                    version: 1,
                },
                FieldCode::MemoryId,
                ValidationReason::ContainsNul,
            ),
            (
                SourceReferenceInput::MemoryVersion {
                    memory_id: SOURCE_KEY_SENTINEL.to_owned(),
                    version: 0,
                },
                FieldCode::MemoryVersion,
                ValidationReason::NotPositive,
            ),
            (
                SourceReferenceInput::MemoryVersion {
                    memory_id: oversized_key.clone(),
                    version: 1,
                },
                FieldCode::SourceKey,
                ValidationReason::TooLong,
            ),
            (
                SourceReferenceInput::SessionRecord {
                    session_record_id: String::new(),
                },
                FieldCode::SessionRecordId,
                ValidationReason::Empty,
            ),
            (
                SourceReferenceInput::SessionRecord {
                    session_record_id: format!("{SOURCE_KEY_SENTINEL}\0"),
                },
                FieldCode::SessionRecordId,
                ValidationReason::ContainsNul,
            ),
            (
                SourceReferenceInput::SessionRecord {
                    session_record_id: oversized_key,
                },
                FieldCode::SourceKey,
                ValidationReason::TooLong,
            ),
        ]
    }

    fn assert_source_keys_omitted(error: GraphError, keys: &[&str]) {
        let display = error.to_string();
        let debug = format!("{error:?}");
        for key in keys {
            assert!(!display.contains(key), "Display leaked a source key");
            assert!(!debug.contains(key), "Debug leaked a source key");
        }
    }

    fn invalid_alias_sets() -> Vec<(Vec<Alias>, FieldCode, ValidationReason)> {
        let mut too_many = Vec::with_capacity(65);
        for index in 0..65 {
            too_many.push(alias(&format!("alias-{index}"), index == 0));
        }

        vec![
            (Vec::new(), FieldCode::AliasSet, ValidationReason::Empty),
            (
                vec![alias("No preferred alias", false)],
                FieldCode::PreferredAlias,
                ValidationReason::InvalidShape,
            ),
            (
                vec![
                    alias("First preferred", true),
                    alias("Second preferred", true),
                ],
                FieldCode::PreferredAlias,
                ValidationReason::InvalidShape,
            ),
            (
                vec![alias("", true)],
                FieldCode::Alias,
                ValidationReason::Empty,
            ),
            (
                vec![alias("contains\0nul", true)],
                FieldCode::Alias,
                ValidationReason::ContainsNul,
            ),
            (
                vec![alias(&"a".repeat(513), true)],
                FieldCode::Alias,
                ValidationReason::TooLong,
            ),
            (
                vec![alias("   \u{2003}\t", true)],
                FieldCode::AliasKey,
                ValidationReason::Empty,
            ),
            (
                vec![alias(&'\u{FDFA}'.to_string().repeat(170), true)],
                FieldCode::AliasKey,
                ValidationReason::TooLong,
            ),
            (
                vec![alias("Straße", true), alias("STRASSE", false)],
                FieldCode::AliasSet,
                ValidationReason::InvalidShape,
            ),
            (too_many, FieldCode::AliasSet, ValidationReason::TooLong),
        ]
    }

    #[tokio::test]
    async fn direct_read_inputs_are_validated_before_any_database_call() {
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());

        assert_eq!(
            store
                .get_concept(ConceptIdInput {
                    id: "not-a-uuid-v7".to_owned(),
                })
                .await,
            Err(invalid_input(
                OperationCode::GetConcept,
                FieldCode::ConceptId,
                ValidationReason::InvalidUuid,
            ))
        );
        assert_eq!(
            store
                .get_assertion(AssertionIdInput {
                    id: "not-a-uuid-v7".to_owned(),
                })
                .await,
            Err(invalid_input(
                OperationCode::GetAssertion,
                FieldCode::AssertionId,
                ValidationReason::InvalidUuid,
            ))
        );

        const SOURCE_KEY_SENTINEL: &str = "PRIVATE_SOURCE_KEY_SENTINEL";
        for (source, field, reason) in invalid_source_inputs() {
            let error = store
                .get_source(source)
                .await
                .expect_err("invalid source identities should be rejected");
            assert_eq!(
                error,
                invalid_input(OperationCode::GetSource, field, reason)
            );
            assert_source_keys_omitted(error, &[SOURCE_KEY_SENTINEL]);
            assert!(store.database.calls().is_empty());
        }

        for (alias_text, field, reason) in [
            (String::new(), FieldCode::Alias, ValidationReason::Empty),
            (
                "   \u{2003}\t".to_owned(),
                FieldCode::AliasKey,
                ValidationReason::Empty,
            ),
            (
                '\u{FDFA}'.to_string().repeat(170),
                FieldCode::AliasKey,
                ValidationReason::TooLong,
            ),
        ] {
            assert_eq!(
                store.resolve_alias(AliasQuery { alias: alias_text }).await,
                Err(invalid_input(OperationCode::ResolveAlias, field, reason))
            );
            assert!(store.database.calls().is_empty());
        }
    }

    #[tokio::test]
    async fn direct_reads_dispatch_only_the_validated_id_and_alias_arguments_once() {
        let concept = concept_id(108);
        let assertion = assertion_id(109);
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());

        assert_eq!(
            store
                .get_concept(ConceptIdInput {
                    id: concept.as_uuid().to_string(),
                })
                .await
                .expect("a valid concept lookup should return its absence"),
            None
        );
        assert_eq!(
            store.database.calls(),
            vec![RecordedDatabaseCall::GetConcept { id: concept }]
        );

        let alias_query = "  Straße\u{2003}Name  ";
        assert_eq!(
            store
                .resolve_alias(AliasQuery {
                    alias: alias_query.to_owned(),
                })
                .await
                .expect("a valid alias lookup should return its absence"),
            None
        );
        let expected_key = normalize_alias_key(alias_query)
            .expect("the service should forward the versioned normalizer's output");
        assert_eq!(
            store.database.calls(),
            vec![
                RecordedDatabaseCall::GetConcept { id: concept },
                RecordedDatabaseCall::ResolveAlias { key: expected_key },
            ]
        );

        assert_eq!(
            store
                .get_assertion(AssertionIdInput {
                    id: assertion.as_uuid().to_string(),
                })
                .await
                .expect("a valid assertion lookup should return its absence"),
            None
        );
        assert_eq!(
            store.database.calls(),
            vec![
                RecordedDatabaseCall::GetConcept { id: concept },
                RecordedDatabaseCall::ResolveAlias {
                    key: normalize_alias_key(alias_query).expect("valid alias key"),
                },
                RecordedDatabaseCall::GetAssertion { id: assertion },
            ]
        );
    }

    #[tokio::test]
    async fn source_lookup_validates_and_dispatches_both_typed_identities_at_the_byte_bound() {
        let key = "é".repeat(MAX_SOURCE_KEY_BYTES / "é".len());
        assert_eq!(key.len(), MAX_SOURCE_KEY_BYTES);
        let memory_input = SourceReferenceInput::MemoryVersion {
            memory_id: key.clone(),
            version: 17,
        };
        let session_input = SourceReferenceInput::SessionRecord {
            session_record_id: key,
        };
        let memory_source = source_reference(memory_input.clone());
        let session_source = source_reference(session_input.clone());
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());

        assert_eq!(
            store
                .get_source(memory_input)
                .await
                .expect("the 2,048-byte memory source identity is valid"),
            Some(memory_source.clone())
        );
        assert_eq!(
            store
                .get_source(session_input)
                .await
                .expect("the 2,048-byte session source identity is valid"),
            Some(session_source.clone())
        );
        assert_eq!(
            store.database.calls(),
            vec![
                RecordedDatabaseCall::GetSource {
                    source: memory_source,
                },
                RecordedDatabaseCall::GetSource {
                    source: session_source,
                },
            ]
        );
    }

    #[tokio::test]
    async fn direct_concept_lookup_preserves_the_complete_record_from_the_port() {
        let id = concept_id(110);
        let expected = concept(
            id,
            revision(7),
            vec![
                alias("Preferred concept label", true),
                alias("Complete secondary alias", false),
            ],
        );
        let store = KnowledgeGraphStore::new(ScriptedConceptDatabase::with_get_concept_result(Ok(
            Some(expected.clone()),
        )));

        assert_eq!(
            store
                .get_concept(ConceptIdInput {
                    id: id.as_uuid().to_string(),
                })
                .await
                .expect("the complete concept record should be returned"),
            Some(expected)
        );
        assert_eq!(
            store.database.calls(),
            vec![RecordedDatabaseCall::GetConcept { id }]
        );
    }

    #[tokio::test]
    async fn equivalent_alias_spellings_use_one_key_and_preserve_the_complete_concept() {
        let id = concept_id(111);
        let expected = concept(
            id,
            revision(8),
            vec![
                alias("Straße", true),
                alias("Alternate display alias", false),
            ],
        );
        let store = KnowledgeGraphStore::new(ScriptedConceptDatabase::with_resolve_alias_result(
            Ok(Some(expected.clone())),
        ));

        for spelling in ["Straße", "STRASSE"] {
            assert_eq!(
                store
                    .resolve_alias(AliasQuery {
                        alias: spelling.to_owned(),
                    })
                    .await
                    .expect("equivalent valid spellings should resolve"),
                Some(expected.clone())
            );
        }

        let calls = store.database.calls();
        let expected_key = normalize_alias_key("Straße").expect("valid alias key");
        assert_eq!(
            calls,
            vec![
                RecordedDatabaseCall::ResolveAlias {
                    key: expected_key.clone(),
                },
                RecordedDatabaseCall::ResolveAlias { key: expected_key },
            ]
        );
    }

    #[tokio::test]
    async fn direct_assertion_lookup_preserves_every_evidence_reference() {
        let id = assertion_id(112);
        let subject = concept_id(113);
        let object = concept_id(114);
        let first_source = source_reference(session_source_input("complete-evidence-alpha"));
        let second_source = source_reference(session_source_input("complete-evidence-omega"));
        let expected = assertion_record(
            id,
            revision(9),
            subject,
            RelationType::DependsOn,
            object,
            vec![first_source, second_source],
        );
        let store = KnowledgeGraphStore::new(ScriptedConceptDatabase::with_get_assertion_result(
            Ok(Some(expected.clone())),
        ));

        assert_eq!(
            store
                .get_assertion(AssertionIdInput {
                    id: id.as_uuid().to_string(),
                })
                .await
                .expect("the complete assertion record should be returned"),
            Some(expected)
        );
        assert_eq!(
            store.database.calls(),
            vec![RecordedDatabaseCall::GetAssertion { id }]
        );
    }

    #[tokio::test]
    async fn missing_source_lookup_returns_none() {
        let source_input = session_source_input("absent-source-reference");
        let store =
            KnowledgeGraphStore::new(ScriptedConceptDatabase::with_get_source_result(Ok(None)));

        assert_eq!(
            store
                .get_source(source_input.clone())
                .await
                .expect("a source lookup with no match should return None"),
            None
        );
        assert_eq!(
            store.database.calls(),
            vec![RecordedDatabaseCall::GetSource {
                source: source_reference(source_input),
            }]
        );
    }

    #[tokio::test]
    async fn direct_read_bound_malformed_and_backend_errors_remain_typed_and_content_safe() {
        const ALIAS_SENTINEL: &str = "PRIVATE_LOOKUP_ALIAS_SENTINEL";
        const SOURCE_SENTINEL: &str = "PRIVATE_LOOKUP_SOURCE_KEY_SENTINEL";

        let concept = concept_id(115);
        let result_bound = GraphError::ResultBoundExceeded {
            operation: OperationCode::GetConcept,
        };
        let store = KnowledgeGraphStore::new(ScriptedConceptDatabase::with_get_concept_result(
            Err(result_bound),
        ));
        let error = store
            .get_concept(ConceptIdInput {
                id: concept.as_uuid().to_string(),
            })
            .await
            .expect_err("an oversized complete payload should remain a typed failure");
        assert_eq!(error, result_bound);
        assert_eq!(
            store.database.calls(),
            vec![RecordedDatabaseCall::GetConcept { id: concept }]
        );

        let malformed = GraphError::InvalidResponse {
            operation: OperationCode::ResolveAlias,
            reason: BackendFailureReason::MalformedResponse,
        };
        let store = KnowledgeGraphStore::new(ScriptedConceptDatabase::with_resolve_alias_result(
            Err(malformed),
        ));
        let error = store
            .resolve_alias(AliasQuery {
                alias: ALIAS_SENTINEL.to_owned(),
            })
            .await
            .expect_err("a malformed port response should remain a typed failure");
        assert_eq!(error, malformed);
        let rendered = format!("{error} {error:?}");
        assert!(!rendered.contains(ALIAS_SENTINEL));
        assert_eq!(store.database.calls().len(), 1);

        let source_input = session_source_input(SOURCE_SENTINEL);
        let store = KnowledgeGraphStore::new(FailingKnowledgeGraphDatabase::default());
        let error = store
            .get_source(source_input)
            .await
            .expect_err("a backend failure should remain a typed failure");
        assert_eq!(
            error,
            GraphError::DatabaseFailure {
                operation: OperationCode::GetSource,
                reason: BackendFailureReason::DatabaseFailure,
            }
        );
        assert_source_keys_omitted(error, &[SOURCE_SENTINEL]);
        assert_eq!(
            store.database.calls(),
            vec![RecordedDatabaseCall::GetSource {
                source: source_reference(session_source_input(SOURCE_SENTINEL)),
            }]
        );
    }

    #[tokio::test]
    async fn neighbor_queries_validate_filters_and_dispatch_each_direction_once() {
        let concept_id = concept_id(116);
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());
        let cases = vec![
            (DirectionMode::Outgoing, vec![], vec![]),
            (
                DirectionMode::Incoming,
                vec![
                    RelationType::Resolves,
                    RelationType::Causes,
                    RelationType::RelatedTo,
                    RelationType::Causes,
                ],
                vec![
                    RelationType::Causes,
                    RelationType::RelatedTo,
                    RelationType::Resolves,
                ],
            ),
            (
                DirectionMode::Either,
                vec![
                    RelationType::Uses,
                    RelationType::RelatedTo,
                    RelationType::IsA,
                    RelationType::Contradicts,
                    RelationType::PartOf,
                    RelationType::DependsOn,
                    RelationType::Implements,
                    RelationType::Resolves,
                    RelationType::Causes,
                    RelationType::Uses,
                ],
                vec![
                    RelationType::Causes,
                    RelationType::Contradicts,
                    RelationType::DependsOn,
                    RelationType::Implements,
                    RelationType::IsA,
                    RelationType::PartOf,
                    RelationType::RelatedTo,
                    RelationType::Resolves,
                    RelationType::Uses,
                ],
            ),
        ];
        let mut expected_calls = Vec::new();

        for (direction, relation_filter, validated_filter) in cases {
            assert!(
                store
                    .neighbors(NeighborQuery {
                        concept_id,
                        direction,
                        relation_filter,
                        limit: 17,
                    })
                    .await
                    .expect("an empty match should be successful")
                    .is_empty()
            );
            expected_calls.push(RecordedDatabaseCall::Neighbors {
                concept_id,
                direction,
                relation_filter: validated_filter,
                limit: 17,
            });
        }

        assert_eq!(store.database.calls(), expected_calls);
    }

    #[tokio::test]
    async fn discovery_limits_accept_one_and_maximum_and_reject_out_of_range_before_call() {
        let concept_id = concept_id(117);
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());

        for limit in [1, 256] {
            assert!(
                store
                    .neighbors(NeighborQuery {
                        concept_id,
                        direction: DirectionMode::Either,
                        relation_filter: Vec::new(),
                        limit,
                    })
                    .await
                    .expect("approved neighbor limits should reach the port")
                    .is_empty()
            );
        }
        for limit in [1, 256] {
            assert!(
                store
                    .related_sources(RelatedSourceQuery { concept_id, limit })
                    .await
                    .expect("approved related-source limits should reach the port")
                    .is_empty()
            );
        }

        let expected_calls = vec![
            RecordedDatabaseCall::Neighbors {
                concept_id,
                direction: DirectionMode::Either,
                relation_filter: Vec::new(),
                limit: 1,
            },
            RecordedDatabaseCall::Neighbors {
                concept_id,
                direction: DirectionMode::Either,
                relation_filter: Vec::new(),
                limit: 256,
            },
            RecordedDatabaseCall::RelatedSources {
                concept_id,
                limit: 1,
            },
            RecordedDatabaseCall::RelatedSources {
                concept_id,
                limit: 256,
            },
        ];
        assert_eq!(store.database.calls(), expected_calls);

        for (limit, reason) in [
            (0, ValidationReason::NotPositive),
            (257, ValidationReason::TooLong),
        ] {
            assert_eq!(
                store
                    .neighbors(NeighborQuery {
                        concept_id,
                        direction: DirectionMode::Either,
                        relation_filter: Vec::new(),
                        limit,
                    })
                    .await,
                Err(invalid_input(
                    OperationCode::Neighbors,
                    FieldCode::Limit,
                    reason
                ))
            );
            assert_eq!(
                store.database.calls(),
                expected_calls,
                "invalid neighbor limits must not add a port call"
            );

            assert_eq!(
                store
                    .related_sources(RelatedSourceQuery { concept_id, limit })
                    .await,
                Err(invalid_input(
                    OperationCode::RelatedSources,
                    FieldCode::Limit,
                    reason,
                ))
            );
            assert_eq!(
                store.database.calls(),
                expected_calls,
                "invalid related-source limits must not add a port call"
            );
        }
    }

    #[tokio::test]
    async fn invalid_query_ids_directions_and_relation_filters_cannot_reach_the_port() {
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());
        let valid_id = concept_id(118).as_uuid().to_string();
        let uuid_v4 = "00000000-0000-4000-8000-000000000001";

        for query in [
            serde_json::json!({
                "concept_id": uuid_v4,
                "direction": "outgoing",
                "relation_filter": [],
                "limit": 1
            }),
            serde_json::json!({
                "concept_id": valid_id.clone(),
                "direction": "unsupported",
                "relation_filter": [],
                "limit": 1
            }),
            serde_json::json!({
                "concept_id": valid_id,
                "direction": "outgoing",
                "relation_filter": ["unsupported"],
                "limit": 1
            }),
        ] {
            let query = serde_json::from_value::<NeighborQuery>(query);
            assert!(query.is_err());
            if let Ok(query) = query {
                let _ = store.neighbors(query).await;
            }
            assert!(store.database.calls().is_empty());
        }

        let query = serde_json::from_value::<RelatedSourceQuery>(serde_json::json!({
            "concept_id": uuid_v4,
            "limit": 1
        }));
        assert!(query.is_err());
        if let Ok(query) = query {
            let _ = store.related_sources(query).await;
        }
        assert!(store.database.calls().is_empty());
    }

    #[tokio::test]
    async fn neighbor_results_preserve_port_order_orientation_and_complete_evidence() {
        let query_id = concept_id(119);
        let outgoing_neighbor = concept_id(120);
        let incoming_neighbor = concept_id(121);
        let symmetric_neighbor = concept_id(122);
        let outgoing_sources = vec![
            source_reference(session_source_input("outgoing-evidence-zeta")),
            source_reference(SourceReferenceInput::MemoryVersion {
                memory_id: "outgoing-evidence-alpha".to_owned(),
                version: 4,
            }),
        ];
        let incoming_sources = vec![source_reference(session_source_input(
            "incoming-complete-evidence",
        ))];
        let symmetric_sources = vec![source_reference(SourceReferenceInput::MemoryVersion {
            memory_id: "symmetric-complete-evidence".to_owned(),
            version: 9,
        })];
        let expected = vec![
            NeighborResult {
                neighbor: concept(
                    outgoing_neighbor,
                    revision(2),
                    vec![alias("Outgoing neighbor", true)],
                ),
                assertion: assertion_record(
                    assertion_id(132),
                    revision(3),
                    query_id,
                    RelationType::Uses,
                    outgoing_neighbor,
                    outgoing_sources,
                ),
                orientation: EdgeOrientation::Outgoing,
            },
            NeighborResult {
                neighbor: concept(
                    incoming_neighbor,
                    revision(5),
                    vec![alias("Incoming neighbor", true)],
                ),
                assertion: assertion_record(
                    assertion_id(130),
                    revision(6),
                    incoming_neighbor,
                    RelationType::IsA,
                    query_id,
                    incoming_sources,
                ),
                orientation: EdgeOrientation::Incoming,
            },
            NeighborResult {
                neighbor: concept(
                    symmetric_neighbor,
                    revision(7),
                    vec![alias("Symmetric neighbor", true)],
                ),
                assertion: assertion_record(
                    assertion_id(131),
                    revision(8),
                    query_id,
                    RelationType::Contradicts,
                    symmetric_neighbor,
                    symmetric_sources,
                ),
                orientation: EdgeOrientation::Symmetric,
            },
        ];
        let store = KnowledgeGraphStore::new(ScriptedConceptDatabase::with_neighbors_result(Ok(
            expected.clone(),
        )));

        assert_eq!(
            store
                .neighbors(NeighborQuery {
                    concept_id: query_id,
                    direction: DirectionMode::Either,
                    relation_filter: Vec::new(),
                    limit: 1,
                })
                .await
                .expect("the complete port result should be returned"),
            expected
        );
        assert_eq!(
            store.database.calls(),
            vec![RecordedDatabaseCall::Neighbors {
                concept_id: query_id,
                direction: DirectionMode::Either,
                relation_filter: Vec::new(),
                limit: 1,
            }]
        );
    }

    #[tokio::test]
    async fn related_source_results_preserve_order_and_role_sensitive_typed_identity_deduplication()
    {
        let concept_id = concept_id(123);
        let shared_source = source_reference(session_source_input("shared-related-source"));
        let memory_v1 = source_reference(SourceReferenceInput::MemoryVersion {
            memory_id: "related-memory-identity".to_owned(),
            version: 1,
        });
        let memory_v2 = source_reference(SourceReferenceInput::MemoryVersion {
            memory_id: "related-memory-identity".to_owned(),
            version: 2,
        });
        // The same identity is retained once per role; deduplication belongs to the database.
        let expected = vec![
            RelatedSourceResult {
                source: shared_source.clone(),
                role: RelatedSourceRole::Mention,
            },
            RelatedSourceResult {
                source: memory_v2,
                role: RelatedSourceRole::AssertionEvidence,
            },
            RelatedSourceResult {
                source: shared_source.clone(),
                role: RelatedSourceRole::AssertionEvidence,
            },
            RelatedSourceResult {
                source: memory_v1,
                role: RelatedSourceRole::Mention,
            },
        ];
        let store = KnowledgeGraphStore::new(ScriptedConceptDatabase::with_related_sources_result(
            Ok(expected.clone()),
        ));

        assert_eq!(
            store
                .related_sources(RelatedSourceQuery {
                    concept_id,
                    limit: 1,
                })
                .await
                .expect("the complete port result should be returned"),
            expected
        );
        assert_eq!(
            store.database.calls(),
            vec![RecordedDatabaseCall::RelatedSources {
                concept_id,
                limit: 1,
            }]
        );
    }

    #[tokio::test]
    async fn no_matching_neighbors_and_sources_are_successful_empty_results() {
        let concept_id = concept_id(124);
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());

        assert!(
            store
                .neighbors(NeighborQuery {
                    concept_id,
                    direction: DirectionMode::Outgoing,
                    relation_filter: vec![RelationType::Causes],
                    limit: 8,
                })
                .await
                .expect("no neighbors should be a successful empty result")
                .is_empty()
        );
        assert!(
            store
                .related_sources(RelatedSourceQuery {
                    concept_id,
                    limit: 8,
                })
                .await
                .expect("no related sources should be a successful empty result")
                .is_empty()
        );
        assert_eq!(
            store.database.calls(),
            vec![
                RecordedDatabaseCall::Neighbors {
                    concept_id,
                    direction: DirectionMode::Outgoing,
                    relation_filter: vec![RelationType::Causes],
                    limit: 8,
                },
                RecordedDatabaseCall::RelatedSources {
                    concept_id,
                    limit: 8,
                },
            ]
        );
    }

    #[tokio::test]
    async fn query_bound_malformed_and_database_errors_remain_complete_and_content_safe() {
        let concept_id = concept_id(125);
        let neighbor_bound = GraphError::ResultBoundExceeded {
            operation: OperationCode::Neighbors,
        };
        let store = KnowledgeGraphStore::new(ScriptedConceptDatabase::with_neighbors_result(Err(
            neighbor_bound,
        )));
        assert_eq!(
            store
                .neighbors(NeighborQuery {
                    concept_id,
                    direction: DirectionMode::Either,
                    relation_filter: Vec::new(),
                    limit: 256,
                })
                .await,
            Err(neighbor_bound)
        );
        assert_eq!(
            store.database.calls(),
            vec![RecordedDatabaseCall::Neighbors {
                concept_id,
                direction: DirectionMode::Either,
                relation_filter: Vec::new(),
                limit: 256,
            }]
        );

        let related_bound = GraphError::ResultBoundExceeded {
            operation: OperationCode::RelatedSources,
        };
        let store = KnowledgeGraphStore::new(ScriptedConceptDatabase::with_related_sources_result(
            Err(related_bound),
        ));
        assert_eq!(
            store
                .related_sources(RelatedSourceQuery {
                    concept_id,
                    limit: 256,
                })
                .await,
            Err(related_bound)
        );

        let malformed = GraphError::InvalidResponse {
            operation: OperationCode::RelatedSources,
            reason: BackendFailureReason::MalformedResponse,
        };
        let store = KnowledgeGraphStore::new(ScriptedConceptDatabase::with_related_sources_result(
            Err(malformed),
        ));
        let error = store
            .related_sources(RelatedSourceQuery {
                concept_id,
                limit: 1,
            })
            .await
            .expect_err("malformed query output must not return partial results");
        assert_eq!(error, malformed);
        assert_eq!(
            format!("{error} {error:?}"),
            "invalid_response:related_sources:malformed_response GraphError { code: \"invalid_response:related_sources:malformed_response\" }"
        );

        let store = KnowledgeGraphStore::new(FailingKnowledgeGraphDatabase::default());
        for (error, operation) in [
            (
                store
                    .neighbors(NeighborQuery {
                        concept_id,
                        direction: DirectionMode::Incoming,
                        relation_filter: vec![RelationType::DependsOn],
                        limit: 4,
                    })
                    .await
                    .expect_err("database failure must not return partial neighbors"),
                OperationCode::Neighbors,
            ),
            (
                store
                    .related_sources(RelatedSourceQuery {
                        concept_id,
                        limit: 4,
                    })
                    .await
                    .expect_err("database failure must not return partial sources"),
                OperationCode::RelatedSources,
            ),
        ] {
            assert_eq!(
                error,
                GraphError::DatabaseFailure {
                    operation,
                    reason: BackendFailureReason::DatabaseFailure,
                }
            );
            assert!(!format!("{error} {error:?}").contains(&concept_id.as_uuid().to_string()));
        }
        assert_eq!(store.database.calls().len(), 2);
    }

    #[tokio::test]
    async fn find_paths_preserves_path_order_orientation_and_complete_evidence() {
        let start = concept_id(150);
        let end = concept_id(151);
        let middle = concept_id(152);
        let memory_evidence = source_reference(SourceReferenceInput::MemoryVersion {
            memory_id: "path-evidence-memory".to_owned(),
            version: 3,
        });
        let session_evidence = source_reference(session_source_input("path-evidence-session"));
        let mut expected = vec![
            graph_path(
                vec![start, end],
                vec![OrientedAssertion {
                    assertion: assertion_record(
                        assertion_id(153),
                        revision(4),
                        end,
                        RelationType::RelatedTo,
                        start,
                        vec![memory_evidence.clone(), session_evidence.clone()],
                    ),
                    orientation: EdgeOrientation::Symmetric,
                }],
            ),
            graph_path(
                vec![start, end],
                vec![OrientedAssertion {
                    assertion: assertion_record(
                        assertion_id(154),
                        revision(5),
                        start,
                        RelationType::Uses,
                        end,
                        vec![source_reference(session_source_input(
                            "path-evidence-directed",
                        ))],
                    ),
                    orientation: EdgeOrientation::Outgoing,
                }],
            ),
            graph_path(
                vec![start, middle, end],
                vec![
                    OrientedAssertion {
                        assertion: assertion_record(
                            assertion_id(155),
                            revision(6),
                            start,
                            RelationType::IsA,
                            middle,
                            vec![memory_evidence, session_evidence],
                        ),
                        orientation: EdgeOrientation::Outgoing,
                    },
                    OrientedAssertion {
                        assertion: assertion_record(
                            assertion_id(156),
                            revision(7),
                            end,
                            RelationType::DependsOn,
                            middle,
                            vec![source_reference(session_source_input(
                                "path-evidence-incoming",
                            ))],
                        ),
                        orientation: EdgeOrientation::Incoming,
                    },
                ],
            ),
        ];
        // Preserve the port's sequence instead of sorting paths in the service.
        expected.reverse();
        let query = valid_path_query(start, end);
        let store = KnowledgeGraphStore::new(ScriptedConceptDatabase::with_find_paths_result(Ok(
            expected.clone(),
        )));

        assert_eq!(
            store
                .find_paths(query)
                .await
                .expect("the complete ordered path result should be returned"),
            expected
        );
        assert_eq!(
            store.database.calls(),
            vec![RecordedDatabaseCall::FindPaths {
                from: start,
                to: end,
                direction: DirectionMode::Either,
                max_depth: 5,
                max_work: 81,
                limit: 7,
            }]
        );
    }

    #[tokio::test]
    async fn find_paths_returns_empty_results_and_forwards_each_direction_once() {
        let from = concept_id(157);
        let to = concept_id(158);
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());
        let mut expected_calls = Vec::new();

        for direction in [
            DirectionMode::Outgoing,
            DirectionMode::Incoming,
            DirectionMode::Either,
        ] {
            assert_eq!(
                store
                    .find_paths(PathQuery {
                        direction,
                        ..valid_path_query(from, to)
                    })
                    .await
                    .expect("an empty path set is a successful result"),
                Vec::<GraphPath>::new()
            );
            expected_calls.push(RecordedDatabaseCall::FindPaths {
                from,
                to,
                direction,
                max_depth: 5,
                max_work: 81,
                limit: 7,
            });
        }

        assert_eq!(store.database.calls(), expected_calls);
    }

    #[tokio::test]
    async fn invalid_path_bounds_and_direction_never_reach_the_port() {
        let from = concept_id(159);
        let to = concept_id(160);
        let base = valid_path_query(from, to);
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());

        for (query, field, reason) in [
            (
                PathQuery {
                    to: from,
                    ..base.clone()
                },
                FieldCode::ConceptId,
                ValidationReason::SelfRelation,
            ),
            (
                PathQuery {
                    max_depth: 0,
                    ..base.clone()
                },
                FieldCode::MaxDepth,
                ValidationReason::NotPositive,
            ),
            (
                PathQuery {
                    max_depth: MAX_PATH_DEPTH + 1,
                    ..base.clone()
                },
                FieldCode::MaxDepth,
                ValidationReason::TooLong,
            ),
            (
                PathQuery {
                    max_work: 0,
                    ..base.clone()
                },
                FieldCode::MaxWork,
                ValidationReason::NotPositive,
            ),
            (
                PathQuery {
                    max_work: MAX_PATH_WORK + 1,
                    ..base.clone()
                },
                FieldCode::MaxWork,
                ValidationReason::TooLong,
            ),
            (
                PathQuery {
                    limit: 0,
                    ..base.clone()
                },
                FieldCode::Limit,
                ValidationReason::NotPositive,
            ),
            (
                PathQuery {
                    limit: MAX_PATH_RESULTS + 1,
                    ..base.clone()
                },
                FieldCode::Limit,
                ValidationReason::TooLong,
            ),
        ] {
            assert_eq!(
                store.find_paths(query).await,
                Err(invalid_input(OperationCode::FindPaths, field, reason))
            );
            assert!(
                store.database.calls().is_empty(),
                "invalid path queries must not add a port call"
            );
        }

        let invalid_direction: Result<PathQuery, _> = serde_json::from_value(serde_json::json!({
            "from": from.as_uuid().to_string(),
            "to": to.as_uuid().to_string(),
            "direction": "unsupported",
            "max_depth": 1,
            "max_work": 1,
            "limit": 1,
        }));
        assert!(
            invalid_direction.is_err(),
            "unsupported directions are rejected"
        );
        if let Ok(query) = invalid_direction {
            let _ = store.find_paths(query).await;
        }
        assert!(store.database.calls().is_empty());
    }

    #[tokio::test]
    async fn path_bound_malformed_and_backend_errors_are_typed_private_and_complete_or_error() {
        const PATH_SOURCE_SENTINEL: &str = "PRIVATE_PATH_EVIDENCE_SENTINEL";

        let from = concept_id(161);
        let to = concept_id(162);
        let partial_path = graph_path(
            vec![from, to],
            vec![OrientedAssertion {
                assertion: assertion_record(
                    assertion_id(163),
                    revision(8),
                    from,
                    RelationType::Uses,
                    to,
                    vec![source_reference(session_source_input(PATH_SOURCE_SENTINEL))],
                ),
                orientation: EdgeOrientation::Outgoing,
            }],
        );
        assert!(
            serde_json::to_string(&partial_path)
                .expect("fixture path should serialize")
                .contains(PATH_SOURCE_SENTINEL),
            "the fixture should include protected path evidence"
        );

        let expected_call = RecordedDatabaseCall::FindPaths {
            from,
            to,
            direction: DirectionMode::Either,
            max_depth: 5,
            max_work: 81,
            limit: 7,
        };
        let errors = [
            GraphError::TraversalBoundExceeded {
                operation: OperationCode::FindPaths,
                bound: TraversalBound::Work,
                reason: TraversalReason::WorkExhausted,
            },
            GraphError::TraversalBoundExceeded {
                operation: OperationCode::FindPaths,
                bound: TraversalBound::QueryTimeout,
                reason: TraversalReason::QueryTimeout,
            },
            GraphError::ResultBoundExceeded {
                operation: OperationCode::FindPaths,
            },
            GraphError::InvalidResponse {
                operation: OperationCode::FindPaths,
                reason: BackendFailureReason::MalformedResponse,
            },
            GraphError::DatabaseFailure {
                operation: OperationCode::FindPaths,
                reason: BackendFailureReason::DatabaseFailure,
            },
        ];

        for expected_error in errors {
            let store = KnowledgeGraphStore::new(ScriptedConceptDatabase::with_find_paths_result(
                Err(expected_error),
            ));
            let error = store
                .find_paths(valid_path_query(from, to))
                .await
                .expect_err("bounded, malformed, or failed reads must not return partial paths");

            assert_eq!(error, expected_error);
            assert_eq!(store.database.calls(), vec![expected_call.clone()]);
            let rendered = format!("{error} {error:?}");
            for protected_value in [
                from.as_uuid().to_string(),
                to.as_uuid().to_string(),
                PATH_SOURCE_SENTINEL.to_owned(),
            ] {
                assert!(
                    !rendered.contains(&protected_value),
                    "path errors must not expose query or path contents"
                );
            }
        }

        let store = KnowledgeGraphStore::new(FailingKnowledgeGraphDatabase::default());
        let error = store
            .find_paths(valid_path_query(from, to))
            .await
            .expect_err("a failed backend must not return a partial path result");
        assert_eq!(
            error,
            GraphError::DatabaseFailure {
                operation: OperationCode::FindPaths,
                reason: BackendFailureReason::DatabaseFailure,
            }
        );
        assert_eq!(store.database.calls(), vec![expected_call]);
        let rendered = format!("{error} {error:?}");
        assert!(!rendered.contains(&from.as_uuid().to_string()));
        assert!(!rendered.contains(&to.as_uuid().to_string()));
        assert!(!rendered.contains(PATH_SOURCE_SENTINEL));
    }

    #[tokio::test]
    async fn invalid_alias_inputs_are_rejected_before_any_database_call() {
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());

        for (aliases, field, reason) in invalid_alias_sets() {
            let result = store.create_concept(CreateConcept { aliases }).await;
            assert_eq!(
                result,
                Err(invalid_input(OperationCode::CreateConcept, field, reason))
            );
            assert!(store.database.calls().is_empty());
        }

        let input = replace_input(concept_id(1), revision(1));
        let invalid_replacement = ReplaceConceptAliases {
            aliases: vec![alias("Missing preferred", false)],
            ..input
        };
        assert_eq!(
            store.replace_concept_aliases(invalid_replacement).await,
            Err(invalid_input(
                OperationCode::ReplaceConceptAliases,
                FieldCode::PreferredAlias,
                ValidationReason::InvalidShape,
            ))
        );
        assert!(store.database.calls().is_empty());
    }

    #[tokio::test]
    async fn malformed_source_shapes_and_concept_ids_cannot_reach_service_ports() {
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());

        for malformed_source in [
            serde_json::json!({"kind": "unsupported", "source_key": "private-key"}),
            serde_json::json!({"kind": "memory_version", "memory_id": "memory-key"}),
            serde_json::json!({
                "kind": "session_record",
                "session_record_id": "session-key",
                "version": 1
            }),
        ] {
            if let Ok(source) = serde_json::from_value::<SourceReferenceInput>(malformed_source) {
                let _ = store.register_source(source).await;
            }
            assert!(store.database.calls().is_empty());
        }

        let malformed_mention = serde_json::json!({
            "concept_id": "not-a-uuid-v7",
            "source": {
                "kind": "session_record",
                "session_record_id": "private-key"
            }
        });
        if let Ok(input) = serde_json::from_value::<ConceptMentionInput>(malformed_mention) {
            let _ = store.create_mention(input).await;
        }
        assert!(store.database.calls().is_empty());
    }

    #[tokio::test]
    async fn source_registration_returns_canonical_records_for_both_owner_shapes() {
        let exact_key = "é".repeat(MAX_SOURCE_KEY_BYTES / "é".len());
        assert_eq!(exact_key.len(), MAX_SOURCE_KEY_BYTES);
        let memory_input = SourceReferenceInput::MemoryVersion {
            memory_id: exact_key.clone(),
            version: 17,
        };
        let session_input = SourceReferenceInput::SessionRecord {
            session_record_id: exact_key,
        };
        let memory_source = source_reference(memory_input.clone());
        let session_source = source_reference(session_input.clone());
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());

        let registered_memory = store
            .register_source(memory_input.clone())
            .await
            .expect("a valid memory-version source should register");
        assert_eq!(registered_memory, memory_source);
        assert_eq!(
            store.database.calls(),
            vec![RecordedDatabaseCall::RegisterSource {
                source: memory_source.clone(),
            }]
        );

        let replayed_memory = store
            .register_source(memory_input)
            .await
            .expect("an idempotent source registration should return its canonical record");
        assert_eq!(replayed_memory, memory_source);

        let registered_session = store
            .register_source(session_input.clone())
            .await
            .expect("a valid session-record source should register");
        assert_eq!(registered_session, session_source);

        store
            .delete_source(DeleteSource {
                source: session_input,
            })
            .await
            .expect("a valid source deletion should be confirmed");

        assert_eq!(
            store.database.calls(),
            vec![
                RecordedDatabaseCall::RegisterSource {
                    source: memory_source.clone(),
                },
                RecordedDatabaseCall::RegisterSource {
                    source: memory_source,
                },
                RecordedDatabaseCall::RegisterSource {
                    source: session_source.clone(),
                },
                RecordedDatabaseCall::DeleteSource {
                    source: session_source,
                },
            ]
        );
    }

    #[tokio::test]
    async fn invalid_source_inputs_are_rejected_before_any_source_or_mention_call() {
        const SOURCE_KEY_SENTINEL: &str = "PRIVATE_SOURCE_KEY_SENTINEL";
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());
        let concept_id = concept_id(81);

        for (source, field, reason) in invalid_source_inputs() {
            let register_error = store
                .register_source(source.clone())
                .await
                .expect_err("invalid source registration should be rejected");
            assert_eq!(
                register_error,
                invalid_input(OperationCode::RegisterSource, field, reason)
            );

            let delete_source_error = store
                .delete_source(DeleteSource {
                    source: source.clone(),
                })
                .await
                .expect_err("invalid source deletion should be rejected");
            assert_eq!(
                delete_source_error,
                invalid_input(OperationCode::DeleteSource, field, reason)
            );

            let create_mention_error = store
                .create_mention(mention_input(concept_id, source.clone()))
                .await
                .expect_err("invalid mention source should be rejected");
            assert_eq!(
                create_mention_error,
                invalid_input(OperationCode::CreateMention, field, reason)
            );

            let delete_mention_error = store
                .delete_mention(mention_input(concept_id, source))
                .await
                .expect_err("invalid mention deletion source should be rejected");
            assert_eq!(
                delete_mention_error,
                invalid_input(OperationCode::DeleteMention, field, reason)
            );

            for error in [
                register_error,
                delete_source_error,
                create_mention_error,
                delete_mention_error,
            ] {
                assert_source_keys_omitted(error, &[SOURCE_KEY_SENTINEL]);
            }
            assert!(store.database.calls().is_empty());
        }
    }

    #[tokio::test]
    async fn mention_create_replay_and_delete_only_dispatch_the_association() {
        let concept_id = concept_id(82);
        let memory_input = SourceReferenceInput::MemoryVersion {
            memory_id: "opaque-memory-mention-key".to_owned(),
            version: 23,
        };
        let session_input = SourceReferenceInput::SessionRecord {
            session_record_id: "opaque-session-mention-key".to_owned(),
        };
        let memory_source = source_reference(memory_input.clone());
        let session_source = source_reference(session_input.clone());
        let memory_mention = ConceptMention {
            concept_id,
            source: memory_source.clone(),
        };
        let session_mention = ConceptMention {
            concept_id,
            source: session_source.clone(),
        };
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());

        assert_eq!(
            store
                .create_mention(mention_input(concept_id, memory_input.clone()))
                .await
                .expect("memory mention should be created"),
            memory_mention
        );
        assert_eq!(
            store
                .create_mention(mention_input(concept_id, memory_input.clone()))
                .await
                .expect("replayed memory mention should return its canonical record"),
            memory_mention
        );
        store
            .delete_mention(mention_input(concept_id, memory_input))
            .await
            .expect("memory mention deletion should be confirmed");

        assert_eq!(
            store
                .create_mention(mention_input(concept_id, session_input.clone()))
                .await
                .expect("session mention should be created"),
            session_mention
        );
        store
            .delete_mention(mention_input(concept_id, session_input))
            .await
            .expect("session mention deletion should be confirmed");

        assert_eq!(
            store.database.calls(),
            vec![
                RecordedDatabaseCall::CreateMention {
                    concept_id,
                    source: memory_source.clone(),
                },
                RecordedDatabaseCall::CreateMention {
                    concept_id,
                    source: memory_source.clone(),
                },
                RecordedDatabaseCall::DeleteMention {
                    concept_id,
                    source: memory_source,
                },
                RecordedDatabaseCall::CreateMention {
                    concept_id,
                    source: session_source.clone(),
                },
                RecordedDatabaseCall::DeleteMention {
                    concept_id,
                    source: session_source,
                },
            ]
        );
    }

    #[tokio::test]
    async fn missing_graph_endpoints_and_referenced_source_deletes_are_typed_and_redacted() {
        const SOURCE_KEY_SENTINEL: &str = "PRIVATE_SOURCE_KEY_SENTINEL";
        let concept_id = concept_id(83);
        let input = SourceReferenceInput::SessionRecord {
            session_record_id: SOURCE_KEY_SENTINEL.to_owned(),
        };
        let source = source_reference(input.clone());

        let missing_concept = concept_not_found(OperationCode::CreateMention, concept_id);
        let store = KnowledgeGraphStore::new(ScriptedConceptDatabase::with_create_mention_result(
            Err(missing_concept),
        ));
        assert_eq!(
            store
                .create_mention(mention_input(concept_id, input.clone()))
                .await,
            Err(missing_concept)
        );
        assert_eq!(
            store.database.calls(),
            vec![RecordedDatabaseCall::CreateMention {
                concept_id,
                source: source.clone(),
            }]
        );
        assert_source_keys_omitted(missing_concept, &[SOURCE_KEY_SENTINEL]);

        let missing_source = GraphError::NotFound {
            operation: OperationCode::CreateMention,
            record_kind: RecordKind::SourceReference,
            identity: None,
            reason: NotFoundReason::Missing,
        };
        let store = KnowledgeGraphStore::new(ScriptedConceptDatabase::with_create_mention_result(
            Err(missing_source),
        ));
        assert_eq!(
            store
                .create_mention(mention_input(concept_id, input.clone()))
                .await,
            Err(missing_source)
        );
        assert_eq!(store.database.calls().len(), 1);
        assert_source_keys_omitted(missing_source, &[SOURCE_KEY_SENTINEL]);

        let missing_mention = GraphError::NotFound {
            operation: OperationCode::DeleteMention,
            record_kind: RecordKind::ConceptMention,
            identity: None,
            reason: NotFoundReason::Missing,
        };
        let store = KnowledgeGraphStore::new(ScriptedConceptDatabase::with_delete_mention_result(
            Err(missing_mention),
        ));
        assert_eq!(
            store
                .delete_mention(mention_input(concept_id, input.clone()))
                .await,
            Err(missing_mention)
        );
        assert_eq!(store.database.calls().len(), 1);
        assert_source_keys_omitted(missing_mention, &[SOURCE_KEY_SENTINEL]);

        let missing_source = GraphError::NotFound {
            operation: OperationCode::DeleteSource,
            record_kind: RecordKind::SourceReference,
            identity: None,
            reason: NotFoundReason::Missing,
        };
        let store = KnowledgeGraphStore::new(ScriptedConceptDatabase::with_delete_source_result(
            Err(missing_source),
        ));
        assert_eq!(
            store
                .delete_source(DeleteSource {
                    source: input.clone(),
                })
                .await,
            Err(missing_source)
        );
        assert_eq!(store.database.calls().len(), 1);
        assert_source_keys_omitted(missing_source, &[SOURCE_KEY_SENTINEL]);

        let referenced_source = GraphError::Referenced {
            operation: OperationCode::DeleteSource,
            record: ReferencedRecord::SourceReference,
            reason: ReferenceReason::InUse,
        };
        let store = KnowledgeGraphStore::new(ScriptedConceptDatabase::with_delete_source_result(
            Err(referenced_source),
        ));
        assert_eq!(
            store.delete_source(DeleteSource { source: input }).await,
            Err(referenced_source)
        );
        assert_eq!(
            store.database.calls(),
            vec![RecordedDatabaseCall::DeleteSource { source }]
        );
        assert_source_keys_omitted(referenced_source, &[SOURCE_KEY_SENTINEL]);
    }

    #[tokio::test]
    async fn failing_source_and_mention_ports_never_echo_source_keys() {
        const MEMORY_KEY_SENTINEL: &str = "PRIVATE_MEMORY_SOURCE_KEY_SENTINEL";
        const SESSION_KEY_SENTINEL: &str = "PRIVATE_SESSION_SOURCE_KEY_SENTINEL";
        let concept_id = concept_id(84);
        let memory_input = SourceReferenceInput::MemoryVersion {
            memory_id: MEMORY_KEY_SENTINEL.to_owned(),
            version: 31,
        };
        let session_input = SourceReferenceInput::SessionRecord {
            session_record_id: SESSION_KEY_SENTINEL.to_owned(),
        };
        let memory_source = source_reference(memory_input.clone());
        let session_source = source_reference(session_input.clone());
        let store = KnowledgeGraphStore::new(FailingKnowledgeGraphDatabase::default());

        let register_error = store
            .register_source(memory_input.clone())
            .await
            .expect_err("failing source registration should return an error");
        let delete_source_error = store
            .delete_source(DeleteSource {
                source: session_input.clone(),
            })
            .await
            .expect_err("failing source deletion should return an error");
        let create_mention_error = store
            .create_mention(mention_input(concept_id, memory_input.clone()))
            .await
            .expect_err("failing mention creation should return an error");
        let delete_mention_error = store
            .delete_mention(mention_input(concept_id, session_input.clone()))
            .await
            .expect_err("failing mention deletion should return an error");

        assert_eq!(
            register_error,
            GraphError::DatabaseFailure {
                operation: OperationCode::RegisterSource,
                reason: BackendFailureReason::DatabaseFailure,
            }
        );
        assert_eq!(
            delete_source_error,
            GraphError::DatabaseFailure {
                operation: OperationCode::DeleteSource,
                reason: BackendFailureReason::DatabaseFailure,
            }
        );
        assert_eq!(
            create_mention_error,
            GraphError::DatabaseFailure {
                operation: OperationCode::CreateMention,
                reason: BackendFailureReason::DatabaseFailure,
            }
        );
        assert_eq!(
            delete_mention_error,
            GraphError::DatabaseFailure {
                operation: OperationCode::DeleteMention,
                reason: BackendFailureReason::DatabaseFailure,
            }
        );
        for error in [
            register_error,
            delete_source_error,
            create_mention_error,
            delete_mention_error,
        ] {
            assert_source_keys_omitted(error, &[MEMORY_KEY_SENTINEL, SESSION_KEY_SENTINEL]);
        }
        assert_eq!(
            store.database.calls(),
            vec![
                RecordedDatabaseCall::RegisterSource {
                    source: memory_source.clone(),
                },
                RecordedDatabaseCall::DeleteSource {
                    source: session_source.clone(),
                },
                RecordedDatabaseCall::CreateMention {
                    concept_id,
                    source: memory_source,
                },
                RecordedDatabaseCall::DeleteMention {
                    concept_id,
                    source: session_source,
                },
            ]
        );
    }

    #[tokio::test]
    async fn create_returns_the_canonical_record_and_records_a_uuid_v7_candidate() {
        let input = create_input();
        let normalized = normalize_alias_set(&input.aliases).expect("valid fixture aliases");
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());

        let created = store
            .create_concept(input)
            .await
            .expect("valid concept should be created");

        assert_eq!(created.revision, revision(1));
        assert_eq!(
            created.aliases,
            normalized
                .aliases()
                .iter()
                .map(|alias| Alias {
                    display_text: alias.display_text().to_owned(),
                    preferred: alias.is_preferred(),
                })
                .collect::<Vec<_>>()
        );

        let calls = store.database.calls();
        assert_eq!(calls.len(), 1);
        let RecordedDatabaseCall::CreateConcept {
            candidate_id,
            aliases,
        } = &calls[0]
        else {
            panic!("create operation should call only create_concept")
        };
        assert_eq!(candidate_id.as_uuid().get_version_num(), 7);
        assert_eq!(aliases, normalized.aliases());
    }

    #[tokio::test]
    async fn existing_create_returns_stored_display_text_and_revision_unchanged() {
        let stored = concept(
            concept_id(12),
            revision(29),
            vec![alias("GRAPH CONCEPT", true), alias("GRAPH-CONCEPT", false)],
        );
        let database = ScriptedConceptDatabase::with_create_result(Ok(stored.clone()));
        let store = KnowledgeGraphStore::new(database);

        let result = store
            .create_concept(create_input())
            .await
            .expect("existing exact alias set should return its canonical concept");

        assert_eq!(result, stored);
        assert_eq!(store.database.calls().len(), 1);
    }

    #[tokio::test]
    async fn overlapping_alias_conflicts_remain_typed_errors_and_do_not_leak_aliases() {
        let alias_sentinel = "PRIVATE_ALIAS_SENTINEL";
        let input = CreateConcept {
            aliases: vec![
                alias(alias_sentinel, true),
                alias("another private alias", false),
            ],
        };
        let owner = concept_id(44);
        let current_revision = revision(17);

        for reason in [ConflictReason::AliasOwned, ConflictReason::AliasSetMismatch] {
            let error = conflict(
                OperationCode::CreateConcept,
                ConflictField::AliasSet,
                reason,
                owner,
                current_revision,
            );
            let database = ScriptedConceptDatabase::with_create_result(Err(error));
            let store = KnowledgeGraphStore::new(database);

            assert_eq!(store.create_concept(input.clone()).await, Err(error));
            assert_eq!(store.database.calls().len(), 1);
            let rendered = format!("{error} {error:?}");
            assert!(!rendered.contains(alias_sentinel));
            assert!(!rendered.contains("another private alias"));
        }
    }

    #[tokio::test]
    async fn alias_replacement_sends_the_complete_normalized_set_and_current_revision() {
        let id = concept_id(23);
        let expected_revision = revision(8);
        let input = replace_input(id, expected_revision);
        let normalized = normalize_alias_set(&input.aliases).expect("valid replacement aliases");
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());

        let updated = store
            .replace_concept_aliases(input)
            .await
            .expect("valid alias replacement should succeed");

        assert_eq!(updated.id, id);
        assert_eq!(updated.revision, revision(9));
        assert_eq!(updated.aliases.len(), 3);
        assert_eq!(
            updated
                .aliases
                .iter()
                .filter(|alias| alias.preferred)
                .count(),
            1
        );
        let calls = store.database.calls();
        assert_eq!(
            calls,
            vec![RecordedDatabaseCall::ReplaceConceptAliases {
                concept_id: id,
                expected_revision,
                aliases: normalized.aliases().to_vec(),
            }]
        );
    }

    #[tokio::test]
    async fn replacement_preserves_stale_snapshot_and_missing_conflicts() {
        let id = concept_id(31);
        let other_id = concept_id(32);
        let cases = [
            conflict(
                OperationCode::ReplaceConceptAliases,
                ConflictField::Revision,
                ConflictReason::StaleRevision,
                id,
                revision(10),
            ),
            conflict(
                OperationCode::ReplaceConceptAliases,
                ConflictField::Snapshot,
                ConflictReason::SnapshotDrift,
                id,
                revision(11),
            ),
            conflict(
                OperationCode::ReplaceConceptAliases,
                ConflictField::AliasSet,
                ConflictReason::AliasOwned,
                other_id,
                revision(12),
            ),
            concept_not_found(OperationCode::ReplaceConceptAliases, id),
        ];

        for error in cases {
            let store =
                KnowledgeGraphStore::new(ScriptedConceptDatabase::with_replace_result(Err(error)));
            assert_eq!(
                store
                    .replace_concept_aliases(replace_input(id, revision(9)))
                    .await,
                Err(error)
            );
            assert_eq!(store.database.calls().len(), 1);
        }
    }

    #[tokio::test]
    async fn concept_delete_uses_expected_revision_and_maps_stale_reference_and_missing() {
        let id = concept_id(51);
        let expected_revision = revision(14);
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());

        store
            .delete_concept(DeleteConcept {
                concept_id: id,
                expected_revision,
            })
            .await
            .expect("current unreferenced concept should be deleted");
        assert_eq!(
            store.database.calls(),
            vec![RecordedDatabaseCall::DeleteConcept {
                concept_id: id,
                expected_revision,
            }]
        );

        let errors = [
            conflict(
                OperationCode::DeleteConcept,
                ConflictField::Revision,
                ConflictReason::StaleRevision,
                id,
                revision(15),
            ),
            GraphError::Referenced {
                operation: OperationCode::DeleteConcept,
                record: ReferencedRecord::Concept(id),
                reason: ReferenceReason::InUse,
            },
            concept_not_found(OperationCode::DeleteConcept, id),
        ];

        for error in errors {
            let store =
                KnowledgeGraphStore::new(ScriptedConceptDatabase::with_delete_result(Err(error)));
            assert_eq!(
                store
                    .delete_concept(DeleteConcept {
                        concept_id: id,
                        expected_revision,
                    })
                    .await,
                Err(error)
            );
            assert_eq!(store.database.calls().len(), 1);
        }
    }

    #[tokio::test]
    async fn backend_failures_are_opaque_for_create_replace_and_delete() {
        let store = KnowledgeGraphStore::new(FailingKnowledgeGraphDatabase::default());
        let create_error = store
            .create_concept(create_input())
            .await
            .expect_err("failing port should fail concept creation");
        let replace_error = store
            .replace_concept_aliases(replace_input(concept_id(61), revision(4)))
            .await
            .expect_err("failing port should fail alias replacement");
        let delete_error = store
            .delete_concept(DeleteConcept {
                concept_id: concept_id(62),
                expected_revision: revision(5),
            })
            .await
            .expect_err("failing port should fail concept deletion");

        assert_eq!(
            create_error,
            GraphError::DatabaseFailure {
                operation: OperationCode::CreateConcept,
                reason: BackendFailureReason::DatabaseFailure,
            }
        );
        assert_eq!(
            replace_error,
            GraphError::DatabaseFailure {
                operation: OperationCode::ReplaceConceptAliases,
                reason: BackendFailureReason::DatabaseFailure,
            }
        );
        assert_eq!(
            delete_error,
            GraphError::DatabaseFailure {
                operation: OperationCode::DeleteConcept,
                reason: BackendFailureReason::DatabaseFailure,
            }
        );

        for error in [create_error, replace_error, delete_error] {
            let rendered = format!("{error} {error:?}");
            assert!(!rendered.contains("Graph Concept"));
            assert!(!rendered.contains("backend message sentinel"));
        }
        assert_eq!(store.database.calls().len(), 3);
    }

    #[tokio::test]
    async fn assertion_create_uses_all_relation_semantics_and_uuid_v7_candidates() {
        let lower = concept_id(85);
        let higher = concept_id(86);
        let source_input = session_source_input("opaque-relation-source");
        let source = source_reference(source_input.clone());
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());

        for relation_type in RelationType::ALL.iter().copied() {
            let created = store
                .create_assertion(create_assertion_input(
                    higher,
                    relation_type,
                    lower,
                    vec![source_input.clone(), source_input.clone()],
                ))
                .await
                .expect("every fixed relation should accept distinct endpoints and evidence");
            let (expected_subject, expected_object) =
                if relation_type.semantics() == RelationSemantics::Symmetric {
                    (lower, higher)
                } else {
                    (higher, lower)
                };

            assert_eq!(created.subject_concept_id, expected_subject);
            assert_eq!(created.relation_type, relation_type);
            assert_eq!(created.object_concept_id, expected_object);
            assert_eq!(created.evidence, vec![source.clone()]);
        }

        let calls = store.database.calls();
        assert_eq!(calls.len(), RelationType::ALL.len());
        for (call, relation_type) in calls.iter().zip(RelationType::ALL.iter().copied()) {
            let RecordedDatabaseCall::CreateAssertion {
                candidate_id,
                subject_concept_id,
                relation_type: recorded_relation,
                object_concept_id,
                supporting_sources,
            } = call
            else {
                panic!("assertion creation should call only create_assertion")
            };
            let (expected_subject, expected_object) =
                if relation_type.semantics() == RelationSemantics::Symmetric {
                    (lower, higher)
                } else {
                    (higher, lower)
                };

            assert_eq!(candidate_id.as_uuid().get_version_num(), 7);
            assert_eq!(*subject_concept_id, expected_subject);
            assert_eq!(*recorded_relation, relation_type);
            assert_eq!(*object_concept_id, expected_object);
            assert_eq!(supporting_sources, std::slice::from_ref(&source));
        }
    }

    #[tokio::test]
    async fn reversed_symmetric_creates_return_the_database_canonical_assertion_and_evidence() {
        let lower = concept_id(87);
        let higher = concept_id(88);
        let first_source_input = session_source_input("canonical-evidence-one");
        let second_source_input = session_source_input("canonical-evidence-two");
        let canonical = assertion_record(
            assertion_id(89),
            revision(13),
            lower,
            RelationType::RelatedTo,
            higher,
            vec![
                source_reference(first_source_input.clone()),
                source_reference(second_source_input.clone()),
            ],
        );
        let database = ScriptedConceptDatabase::with_create_assertion_result(Ok(canonical.clone()));
        let store = KnowledgeGraphStore::new(database);

        for source_input in [&first_source_input, &second_source_input] {
            let result = store
                .create_assertion(create_assertion_input(
                    higher,
                    RelationType::RelatedTo,
                    lower,
                    vec![source_input.clone()],
                ))
                .await
                .expect("the database should return the canonical semantic assertion");
            assert_eq!(result, canonical);
        }

        let calls = store.database.calls();
        assert_eq!(calls.len(), 2);
        for (call, requested_source) in calls.iter().zip([
            source_reference(first_source_input),
            source_reference(second_source_input),
        ]) {
            let RecordedDatabaseCall::CreateAssertion {
                candidate_id,
                subject_concept_id,
                relation_type,
                object_concept_id,
                supporting_sources,
            } = call
            else {
                panic!("semantic replay should call only create_assertion")
            };

            assert_ne!(*candidate_id, canonical.id);
            assert_eq!(candidate_id.as_uuid().get_version_num(), 7);
            assert_eq!(*subject_concept_id, lower);
            assert_eq!(*relation_type, RelationType::RelatedTo);
            assert_eq!(*object_concept_id, higher);
            assert_eq!(supporting_sources, &[requested_source]);
        }
    }

    #[tokio::test]
    async fn invalid_assertion_endpoints_and_supporting_source_sets_make_no_port_calls() {
        let subject = concept_id(90);
        let object = concept_id(91);
        let valid_source = session_source_input("valid-assertion-source");
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());

        assert_eq!(
            store
                .create_assertion(create_assertion_input(
                    subject,
                    RelationType::IsA,
                    subject,
                    vec![valid_source.clone()],
                ))
                .await,
            Err(invalid_input(
                OperationCode::CreateAssertion,
                FieldCode::SubjectConceptId,
                ValidationReason::SelfRelation,
            ))
        );
        assert_eq!(
            store
                .update_assertion(UpdateAssertion {
                    assertion_id: assertion_id(105),
                    expected_revision: revision(1),
                    subject_concept_id: subject,
                    relation_type: RelationType::IsA,
                    object_concept_id: subject,
                })
                .await,
            Err(invalid_input(
                OperationCode::UpdateAssertion,
                FieldCode::SubjectConceptId,
                ValidationReason::SelfRelation,
            ))
        );
        assert_eq!(
            store
                .create_assertion(create_assertion_input(
                    subject,
                    RelationType::IsA,
                    object,
                    Vec::new(),
                ))
                .await,
            Err(invalid_input(
                OperationCode::CreateAssertion,
                FieldCode::SupportingSources,
                ValidationReason::Empty,
            ))
        );

        let too_many_sources = (0..=MAX_ASSERTION_EVIDENCE)
            .map(|index| session_source_input(&format!("assertion-source-{index}")))
            .collect();
        assert_eq!(
            store
                .create_assertion(create_assertion_input(
                    subject,
                    RelationType::IsA,
                    object,
                    too_many_sources,
                ))
                .await,
            Err(invalid_input(
                OperationCode::CreateAssertion,
                FieldCode::SupportingSources,
                ValidationReason::TooLong,
            ))
        );

        for (source, field, reason) in invalid_source_inputs() {
            for (operation, result) in [
                (
                    OperationCode::CreateAssertion,
                    store
                        .create_assertion(create_assertion_input(
                            subject,
                            RelationType::Uses,
                            object,
                            vec![source.clone()],
                        ))
                        .await
                        .map(|_| ()),
                ),
                (
                    OperationCode::AddEvidence,
                    store
                        .add_evidence(assertion_evidence_input(assertion_id(92), source.clone()))
                        .await
                        .map(|_| ()),
                ),
                (
                    OperationCode::RemoveEvidence,
                    store
                        .remove_evidence(assertion_evidence_input(assertion_id(92), source))
                        .await,
                ),
            ] {
                let error = result.expect_err("invalid source identities must be rejected");
                assert_eq!(error, invalid_input(operation, field, reason));
                assert_source_keys_omitted(error, &["PRIVATE_SOURCE_KEY_SENTINEL"]);
                assert!(store.database.calls().is_empty());
            }
        }
    }

    #[tokio::test]
    async fn assertion_create_accepts_and_forwards_exactly_thirty_two_sources() {
        let supporting_sources = (0..MAX_ASSERTION_EVIDENCE)
            .map(|index| session_source_input(&format!("exact-evidence-{index:02}")))
            .collect::<Vec<_>>();
        let expected_sources = supporting_sources
            .iter()
            .cloned()
            .map(source_reference)
            .collect::<Vec<_>>();
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());

        let created = store
            .create_assertion(create_assertion_input(
                concept_id(106),
                RelationType::Uses,
                concept_id(107),
                supporting_sources,
            ))
            .await
            .expect("the evidence cap is inclusive at 32 supporting sources");

        assert_eq!(created.evidence, expected_sources);
        let calls = store.database.calls();
        assert_eq!(calls.len(), 1);
        let RecordedDatabaseCall::CreateAssertion {
            supporting_sources, ..
        } = &calls[0]
        else {
            panic!("assertion creation should call only create_assertion")
        };
        assert_eq!(supporting_sources, &expected_sources);
    }

    #[tokio::test]
    async fn evidence_accepts_the_exact_utf8_source_key_bound() {
        let key = "é".repeat(MAX_SOURCE_KEY_BYTES / "é".len());
        assert_eq!(key.len(), MAX_SOURCE_KEY_BYTES);
        let input = SourceReferenceInput::MemoryVersion {
            memory_id: key,
            version: 23,
        };
        let expected_source = source_reference(input.clone());
        let id = assertion_id(93);
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());

        let added = store
            .add_evidence(assertion_evidence_input(id, input))
            .await
            .expect("a 2,048-byte UTF-8 source identity is valid evidence");

        assert_eq!(
            added,
            AssertionEvidence {
                assertion_id: id,
                source: expected_source.clone(),
            }
        );
        assert_eq!(
            store.database.calls(),
            vec![RecordedDatabaseCall::AddEvidence {
                assertion_id: id,
                source: expected_source,
            }]
        );
    }

    #[tokio::test]
    async fn assertion_update_preserves_complete_evidence_and_delete_uses_current_revision() {
        let id = assertion_id(94);
        let lower = concept_id(95);
        let higher = concept_id(96);
        let first_source = source_reference(session_source_input("update-evidence-one"));
        let second_source = source_reference(session_source_input("update-evidence-two"));
        let updated = assertion_record(
            id,
            revision(10),
            lower,
            RelationType::Contradicts,
            higher,
            vec![first_source, second_source],
        );
        let database = ScriptedConceptDatabase::with_update_assertion_result(Ok(updated.clone()));
        let store = KnowledgeGraphStore::new(database);
        let expected_revision = revision(9);

        let result = store
            .update_assertion(UpdateAssertion {
                assertion_id: id,
                expected_revision,
                subject_concept_id: higher,
                relation_type: RelationType::Contradicts,
                object_concept_id: lower,
            })
            .await
            .expect("a valid assertion update should return its canonical record");
        assert_eq!(result, updated);
        assert_eq!(
            Revision::try_from(0).err().unwrap().reason(),
            ValidationReason::NotPositive
        );

        store
            .delete_assertion(DeleteAssertion {
                assertion_id: id,
                expected_revision: result.revision,
            })
            .await
            .expect("deletion with the returned current revision should succeed");

        assert_eq!(
            store.database.calls(),
            vec![
                RecordedDatabaseCall::UpdateAssertion {
                    assertion_id: id,
                    expected_revision,
                    subject_concept_id: lower,
                    relation_type: RelationType::Contradicts,
                    object_concept_id: higher,
                },
                RecordedDatabaseCall::DeleteAssertion {
                    assertion_id: id,
                    expected_revision: revision(10),
                },
            ]
        );
    }

    #[tokio::test]
    async fn assertion_service_preserves_stable_conflict_and_missing_errors() {
        const SOURCE_SENTINEL: &str = "PRIVATE_MISSING_ASSERTION_SOURCE_SENTINEL";
        let id = assertion_id(97);
        let other_assertion = assertion_id(98);
        let subject = concept_id(99);
        let object = concept_id(100);
        let input = UpdateAssertion {
            assertion_id: id,
            expected_revision: revision(7),
            subject_concept_id: subject,
            relation_type: RelationType::Uses,
            object_concept_id: object,
        };
        let update_errors = [
            assertion_conflict(
                OperationCode::UpdateAssertion,
                ConflictField::SemanticAssertion,
                ConflictReason::DuplicateSemanticAssertion,
                other_assertion,
                revision(11),
            ),
            assertion_conflict(
                OperationCode::UpdateAssertion,
                ConflictField::Revision,
                ConflictReason::StaleRevision,
                id,
                revision(8),
            ),
            assertion_conflict(
                OperationCode::UpdateAssertion,
                ConflictField::Snapshot,
                ConflictReason::SnapshotDrift,
                id,
                revision(7),
            ),
            concept_not_found(OperationCode::UpdateAssertion, subject),
            assertion_not_found(OperationCode::UpdateAssertion, id),
        ];

        for error in update_errors {
            let store = KnowledgeGraphStore::new(
                ScriptedConceptDatabase::with_update_assertion_result(Err(error)),
            );
            assert_eq!(store.update_assertion(input).await, Err(error));
            assert_eq!(store.database.calls().len(), 1);
        }

        for error in [
            concept_not_found(OperationCode::CreateAssertion, subject),
            source_not_found(OperationCode::CreateAssertion),
        ] {
            let store = KnowledgeGraphStore::new(
                ScriptedConceptDatabase::with_create_assertion_result(Err(error)),
            );
            let result = store
                .create_assertion(create_assertion_input(
                    subject,
                    RelationType::IsA,
                    object,
                    vec![session_source_input(SOURCE_SENTINEL)],
                ))
                .await;
            assert_eq!(result, Err(error));
            assert_source_keys_omitted(error, &[SOURCE_SENTINEL]);
            assert_eq!(store.database.calls().len(), 1);
        }

        for error in [
            assertion_conflict(
                OperationCode::DeleteAssertion,
                ConflictField::Revision,
                ConflictReason::StaleRevision,
                id,
                revision(8),
            ),
            assertion_not_found(OperationCode::DeleteAssertion, id),
        ] {
            let store = KnowledgeGraphStore::new(
                ScriptedConceptDatabase::with_delete_assertion_result(Err(error)),
            );
            assert_eq!(
                store
                    .delete_assertion(DeleteAssertion {
                        assertion_id: id,
                        expected_revision: revision(7),
                    })
                    .await,
                Err(error)
            );
            assert_eq!(store.database.calls().len(), 1);
        }
    }

    #[tokio::test]
    async fn evidence_lifecycle_is_idempotent_and_maps_limits_missing_and_orphaning() {
        let id = assertion_id(101);
        let first_source_input = session_source_input("evidence-lifecycle-one");
        let second_source_input = session_source_input("evidence-lifecycle-two");
        let first_source = source_reference(first_source_input.clone());
        let second_source = source_reference(second_source_input.clone());
        let store = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());

        let first_add = store
            .add_evidence(assertion_evidence_input(id, first_source_input.clone()))
            .await
            .expect("valid evidence should be added");
        let replay = store
            .add_evidence(assertion_evidence_input(id, first_source_input))
            .await
            .expect("repeated evidence should return the canonical association");
        let second_add = store
            .add_evidence(assertion_evidence_input(id, second_source_input.clone()))
            .await
            .expect("another valid source should be attached");
        store
            .remove_evidence(assertion_evidence_input(id, second_source_input))
            .await
            .expect("one evidence association should be removable");

        assert_eq!(first_add.source, first_source);
        assert_eq!(replay, first_add);
        assert_eq!(second_add.source, second_source);
        assert_eq!(
            store.database.calls(),
            vec![
                RecordedDatabaseCall::AddEvidence {
                    assertion_id: id,
                    source: first_source.clone(),
                },
                RecordedDatabaseCall::AddEvidence {
                    assertion_id: id,
                    source: first_source,
                },
                RecordedDatabaseCall::AddEvidence {
                    assertion_id: id,
                    source: second_source.clone(),
                },
                RecordedDatabaseCall::RemoveEvidence {
                    assertion_id: id,
                    source: second_source,
                },
            ]
        );

        let limit_error = GraphError::LimitExceeded {
            operation: OperationCode::AddEvidence,
            resource: LimitResource::AssertionEvidenceCount,
            reason: LimitReason::Exceeded,
        };
        for error in [
            limit_error,
            assertion_not_found(OperationCode::AddEvidence, id),
            source_not_found(OperationCode::AddEvidence),
        ] {
            let store = KnowledgeGraphStore::new(
                ScriptedConceptDatabase::with_add_evidence_result(Err(error)),
            );
            assert_eq!(
                store
                    .add_evidence(assertion_evidence_input(
                        id,
                        session_source_input("private-evidence-add-source"),
                    ))
                    .await,
                Err(error)
            );
            assert_eq!(store.database.calls().len(), 1);
        }

        let orphan_error = GraphError::WouldOrphanAssertion {
            operation: OperationCode::RemoveEvidence,
            assertion_id: id,
            revision: revision(12),
        };
        for error in [
            orphan_error,
            assertion_not_found(OperationCode::RemoveEvidence, id),
            source_not_found(OperationCode::RemoveEvidence),
        ] {
            let store = KnowledgeGraphStore::new(
                ScriptedConceptDatabase::with_remove_evidence_result(Err(error)),
            );
            assert_eq!(
                store
                    .remove_evidence(assertion_evidence_input(
                        id,
                        session_source_input("private-evidence-remove-source"),
                    ))
                    .await,
                Err(error)
            );
            assert_eq!(store.database.calls().len(), 1);
        }
    }

    #[tokio::test]
    async fn assertion_backend_failures_and_invalid_inputs_never_disclose_source_keys() {
        const SOURCE_SENTINEL: &str = "PRIVATE_ASSERTION_SOURCE_SENTINEL";
        let id = assertion_id(102);
        let subject = concept_id(103);
        let object = concept_id(104);
        let invalid_source = SourceReferenceInput::SessionRecord {
            session_record_id: format!("{SOURCE_SENTINEL}\0"),
        };
        let recording = KnowledgeGraphStore::new(RecordingKnowledgeGraphDatabase::default());
        let invalid_error = recording
            .create_assertion(create_assertion_input(
                subject,
                RelationType::Causes,
                object,
                vec![invalid_source],
            ))
            .await
            .expect_err("NUL-containing source identities should be rejected");
        assert_eq!(
            invalid_error,
            invalid_input(
                OperationCode::CreateAssertion,
                FieldCode::SessionRecordId,
                ValidationReason::ContainsNul,
            )
        );
        assert_source_keys_omitted(invalid_error, &[SOURCE_SENTINEL]);
        assert!(recording.database.calls().is_empty());

        let source = session_source_input(SOURCE_SENTINEL);
        let store = KnowledgeGraphStore::new(FailingKnowledgeGraphDatabase::default());
        let errors = [
            store
                .create_assertion(create_assertion_input(
                    subject,
                    RelationType::Causes,
                    object,
                    vec![source.clone()],
                ))
                .await
                .expect_err("failing assertion creation should return an opaque error"),
            store
                .update_assertion(UpdateAssertion {
                    assertion_id: id,
                    expected_revision: revision(1),
                    subject_concept_id: subject,
                    relation_type: RelationType::Causes,
                    object_concept_id: object,
                })
                .await
                .expect_err("failing assertion update should return an opaque error"),
            store
                .delete_assertion(DeleteAssertion {
                    assertion_id: id,
                    expected_revision: revision(1),
                })
                .await
                .expect_err("failing assertion deletion should return an opaque error"),
            store
                .add_evidence(assertion_evidence_input(id, source.clone()))
                .await
                .expect_err("failing evidence addition should return an opaque error"),
            store
                .remove_evidence(assertion_evidence_input(id, source))
                .await
                .expect_err("failing evidence removal should return an opaque error"),
        ];
        for (error, operation) in errors.into_iter().zip([
            OperationCode::CreateAssertion,
            OperationCode::UpdateAssertion,
            OperationCode::DeleteAssertion,
            OperationCode::AddEvidence,
            OperationCode::RemoveEvidence,
        ]) {
            assert_eq!(
                error,
                GraphError::DatabaseFailure {
                    operation,
                    reason: BackendFailureReason::DatabaseFailure,
                }
            );
            assert_source_keys_omitted(error, &[SOURCE_SENTINEL]);
        }
        assert_eq!(store.database.calls().len(), 5);
    }

    #[derive(Default)]
    struct ScriptedConceptDatabase {
        recording: RecordingKnowledgeGraphDatabase,
        create_result: Mutex<Option<GraphResult<Concept>>>,
        replace_result: Mutex<Option<GraphResult<Concept>>>,
        delete_result: Mutex<Option<GraphResult<()>>>,
        delete_source_result: Mutex<Option<GraphResult<()>>>,
        create_mention_result: Mutex<Option<GraphResult<ConceptMention>>>,
        delete_mention_result: Mutex<Option<GraphResult<()>>>,
        create_assertion_result: Mutex<Option<GraphResult<Assertion>>>,
        update_assertion_result: Mutex<Option<GraphResult<Assertion>>>,
        delete_assertion_result: Mutex<Option<GraphResult<()>>>,
        add_evidence_result: Mutex<Option<GraphResult<AssertionEvidence>>>,
        remove_evidence_result: Mutex<Option<GraphResult<()>>>,
        get_concept_result: Mutex<Option<GraphResult<Option<Concept>>>>,
        resolve_alias_result: Mutex<Option<GraphResult<Option<Concept>>>>,
        get_assertion_result: Mutex<Option<GraphResult<Option<Assertion>>>>,
        get_source_result: Mutex<Option<GraphResult<Option<SourceReference>>>>,
        neighbors_result: Mutex<Option<GraphResult<Vec<NeighborResult>>>>,
        related_sources_result: Mutex<Option<GraphResult<Vec<RelatedSourceResult>>>>,
        find_paths_result: Mutex<Option<GraphResult<Vec<GraphPath>>>>,
    }

    impl ScriptedConceptDatabase {
        fn with_create_result(result: GraphResult<Concept>) -> Self {
            Self {
                create_result: Mutex::new(Some(result)),
                ..Self::default()
            }
        }

        fn with_replace_result(result: GraphResult<Concept>) -> Self {
            Self {
                replace_result: Mutex::new(Some(result)),
                ..Self::default()
            }
        }

        fn with_delete_result(result: GraphResult<()>) -> Self {
            Self {
                delete_result: Mutex::new(Some(result)),
                ..Self::default()
            }
        }

        fn with_delete_source_result(result: GraphResult<()>) -> Self {
            Self {
                delete_source_result: Mutex::new(Some(result)),
                ..Self::default()
            }
        }

        fn with_create_mention_result(result: GraphResult<ConceptMention>) -> Self {
            Self {
                create_mention_result: Mutex::new(Some(result)),
                ..Self::default()
            }
        }

        fn with_delete_mention_result(result: GraphResult<()>) -> Self {
            Self {
                delete_mention_result: Mutex::new(Some(result)),
                ..Self::default()
            }
        }

        fn with_create_assertion_result(result: GraphResult<Assertion>) -> Self {
            Self {
                create_assertion_result: Mutex::new(Some(result)),
                ..Self::default()
            }
        }

        fn with_update_assertion_result(result: GraphResult<Assertion>) -> Self {
            Self {
                update_assertion_result: Mutex::new(Some(result)),
                ..Self::default()
            }
        }

        fn with_delete_assertion_result(result: GraphResult<()>) -> Self {
            Self {
                delete_assertion_result: Mutex::new(Some(result)),
                ..Self::default()
            }
        }

        fn with_add_evidence_result(result: GraphResult<AssertionEvidence>) -> Self {
            Self {
                add_evidence_result: Mutex::new(Some(result)),
                ..Self::default()
            }
        }

        fn with_remove_evidence_result(result: GraphResult<()>) -> Self {
            Self {
                remove_evidence_result: Mutex::new(Some(result)),
                ..Self::default()
            }
        }

        fn with_get_concept_result(result: GraphResult<Option<Concept>>) -> Self {
            Self {
                get_concept_result: Mutex::new(Some(result)),
                ..Self::default()
            }
        }

        fn with_resolve_alias_result(result: GraphResult<Option<Concept>>) -> Self {
            Self {
                resolve_alias_result: Mutex::new(Some(result)),
                ..Self::default()
            }
        }

        fn with_get_assertion_result(result: GraphResult<Option<Assertion>>) -> Self {
            Self {
                get_assertion_result: Mutex::new(Some(result)),
                ..Self::default()
            }
        }

        fn with_get_source_result(result: GraphResult<Option<SourceReference>>) -> Self {
            Self {
                get_source_result: Mutex::new(Some(result)),
                ..Self::default()
            }
        }

        fn with_neighbors_result(result: GraphResult<Vec<NeighborResult>>) -> Self {
            Self {
                neighbors_result: Mutex::new(Some(result)),
                ..Self::default()
            }
        }

        fn with_related_sources_result(result: GraphResult<Vec<RelatedSourceResult>>) -> Self {
            Self {
                related_sources_result: Mutex::new(Some(result)),
                ..Self::default()
            }
        }

        fn with_find_paths_result(result: GraphResult<Vec<GraphPath>>) -> Self {
            Self {
                find_paths_result: Mutex::new(Some(result)),
                ..Self::default()
            }
        }

        fn calls(&self) -> Vec<RecordedDatabaseCall> {
            self.recording.calls()
        }

        fn take_result<T>(result: &Mutex<Option<GraphResult<T>>>) -> Option<GraphResult<T>> {
            result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
        }

        fn scripted_result<T: Clone>(
            result: &Mutex<Option<GraphResult<T>>>,
            default: T,
        ) -> GraphResult<T> {
            result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .cloned()
                .unwrap_or(Ok(default))
        }
    }

    #[async_trait::async_trait]
    impl KnowledgeGraphDatabase for ScriptedConceptDatabase {
        async fn create_concept(&self, input: &ValidatedCreateConcept) -> DatabaseResult<Concept> {
            let recorded = self.recording.create_concept(input).await?;
            Self::take_result(&self.create_result).unwrap_or(Ok(recorded))
        }

        async fn replace_concept_aliases(
            &self,
            input: &ValidatedReplaceConceptAliases,
        ) -> DatabaseResult<Concept> {
            let recorded = self.recording.replace_concept_aliases(input).await?;
            Self::take_result(&self.replace_result).unwrap_or(Ok(recorded))
        }

        async fn delete_concept(&self, input: &ValidatedDeleteConcept) -> DatabaseResult<()> {
            let recorded = self.recording.delete_concept(input).await?;
            Self::take_result(&self.delete_result).unwrap_or(Ok(recorded))
        }

        async fn register_source(
            &self,
            input: &ValidatedSourceReference,
        ) -> DatabaseResult<SourceReference> {
            self.recording.register_source(input).await
        }

        async fn delete_source(&self, input: &ValidatedDeleteSource) -> DatabaseResult<()> {
            let recorded = self.recording.delete_source(input).await?;
            Self::take_result(&self.delete_source_result).unwrap_or(Ok(recorded))
        }

        async fn create_mention(
            &self,
            input: &ValidatedConceptMention,
        ) -> DatabaseResult<ConceptMention> {
            let recorded = self.recording.create_mention(input).await?;
            Self::take_result(&self.create_mention_result).unwrap_or(Ok(recorded))
        }

        async fn delete_mention(&self, input: &ValidatedConceptMention) -> DatabaseResult<()> {
            let recorded = self.recording.delete_mention(input).await?;
            Self::take_result(&self.delete_mention_result).unwrap_or(Ok(recorded))
        }

        async fn create_assertion(
            &self,
            input: &ValidatedCreateAssertion,
        ) -> DatabaseResult<Assertion> {
            let recorded = self.recording.create_assertion(input).await?;
            Self::scripted_result(&self.create_assertion_result, recorded)
        }

        async fn update_assertion(
            &self,
            input: &ValidatedUpdateAssertion,
        ) -> DatabaseResult<Assertion> {
            let recorded = self.recording.update_assertion(input).await?;
            Self::scripted_result(&self.update_assertion_result, recorded)
        }

        async fn delete_assertion(&self, input: &ValidatedDeleteAssertion) -> DatabaseResult<()> {
            let recorded = self.recording.delete_assertion(input).await?;
            Self::scripted_result(&self.delete_assertion_result, recorded)
        }

        async fn add_evidence(
            &self,
            input: &ValidatedAssertionEvidence,
        ) -> DatabaseResult<AssertionEvidence> {
            let recorded = self.recording.add_evidence(input).await?;
            Self::scripted_result(&self.add_evidence_result, recorded)
        }

        async fn remove_evidence(&self, input: &ValidatedAssertionEvidence) -> DatabaseResult<()> {
            let recorded = self.recording.remove_evidence(input).await?;
            Self::scripted_result(&self.remove_evidence_result, recorded)
        }

        async fn get_concept(&self, id: &ConceptId) -> DatabaseResult<Option<Concept>> {
            let recorded = self.recording.get_concept(id).await?;
            Self::scripted_result(&self.get_concept_result, recorded)
        }

        async fn resolve_alias(&self, key: &AliasKey) -> DatabaseResult<Option<Concept>> {
            let recorded = self.recording.resolve_alias(key).await?;
            Self::scripted_result(&self.resolve_alias_result, recorded)
        }

        async fn get_assertion(&self, id: AssertionId) -> DatabaseResult<Option<Assertion>> {
            let recorded = self.recording.get_assertion(id).await?;
            Self::scripted_result(&self.get_assertion_result, recorded)
        }

        async fn get_source(
            &self,
            source: &ValidatedSourceReference,
        ) -> DatabaseResult<Option<SourceReference>> {
            let recorded = self.recording.get_source(source).await?;
            Self::scripted_result(&self.get_source_result, recorded)
        }

        async fn neighbors(
            &self,
            query: &ValidatedNeighborQuery,
        ) -> DatabaseResult<Vec<NeighborResult>> {
            let recorded = self.recording.neighbors(query).await?;
            Self::scripted_result(&self.neighbors_result, recorded)
        }

        async fn related_sources(
            &self,
            query: &ValidatedRelatedSourceQuery,
        ) -> DatabaseResult<Vec<RelatedSourceResult>> {
            let recorded = self.recording.related_sources(query).await?;
            Self::scripted_result(&self.related_sources_result, recorded)
        }

        async fn find_paths(&self, query: &ValidatedPathQuery) -> DatabaseResult<Vec<GraphPath>> {
            let recorded = self.recording.find_paths(query).await?;
            Self::scripted_result(&self.find_paths_result, recorded)
        }
    }
}
