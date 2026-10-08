//! Narrow public composition seam for the production iii-backed graph store.

use iii_sdk::IIIClient;

use crate::{
    contracts::{
        AliasQuery, Assertion, AssertionEvidence, AssertionEvidenceInput, AssertionIdInput,
        Concept, ConceptIdInput, ConceptMention, ConceptMentionInput, CreateAssertion,
        CreateConcept, DeleteAssertion, DeleteConcept, DeleteSource, GraphPath, GraphResult,
        NeighborQuery, NeighborResult, PathQuery, RelatedSourceQuery, RelatedSourceResult,
        ReplaceConceptAliases, SourceReference, SourceReferenceInput, UpdateAssertion,
    },
    database::{DatabaseTarget, GraphConfigurationError, IiiKnowledgeGraphDatabase},
    graph::KnowledgeGraphStore,
};

/// Validates public graph contracts and delegates to the production iii adapter.
pub struct IiiKnowledgeGraphStore {
    service: KnowledgeGraphStore<IiiKnowledgeGraphDatabase>,
}

impl IiiKnowledgeGraphStore {
    /// Constructs the graph service over an existing iii client and logical database target.
    pub fn new(
        client: IIIClient,
        database: String,
        timeouts: crate::contracts::DatabaseTimeouts,
    ) -> Result<Self, GraphConfigurationError> {
        let database = DatabaseTarget::try_from(database)?;
        let adapter = IiiKnowledgeGraphDatabase::new(client, database, timeouts)?;
        Ok(Self {
            service: KnowledgeGraphStore::new(adapter),
        })
    }

    pub async fn create_concept(&self, input: CreateConcept) -> GraphResult<Concept> {
        self.service.create_concept(input).await
    }

    pub async fn replace_concept_aliases(
        &self,
        input: ReplaceConceptAliases,
    ) -> GraphResult<Concept> {
        self.service.replace_concept_aliases(input).await
    }

    pub async fn delete_concept(&self, input: DeleteConcept) -> GraphResult<()> {
        self.service.delete_concept(input).await
    }

    pub async fn register_source(
        &self,
        input: SourceReferenceInput,
    ) -> GraphResult<SourceReference> {
        self.service.register_source(input).await
    }

    pub async fn delete_source(&self, input: DeleteSource) -> GraphResult<()> {
        self.service.delete_source(input).await
    }

    pub async fn create_mention(&self, input: ConceptMentionInput) -> GraphResult<ConceptMention> {
        self.service.create_mention(input).await
    }

    pub async fn delete_mention(&self, input: ConceptMentionInput) -> GraphResult<()> {
        self.service.delete_mention(input).await
    }

    pub async fn create_assertion(&self, input: CreateAssertion) -> GraphResult<Assertion> {
        self.service.create_assertion(input).await
    }

    pub async fn update_assertion(&self, input: UpdateAssertion) -> GraphResult<Assertion> {
        self.service.update_assertion(input).await
    }

    pub async fn delete_assertion(&self, input: DeleteAssertion) -> GraphResult<()> {
        self.service.delete_assertion(input).await
    }

    pub async fn add_evidence(
        &self,
        input: AssertionEvidenceInput,
    ) -> GraphResult<AssertionEvidence> {
        self.service.add_evidence(input).await
    }

    pub async fn remove_evidence(&self, input: AssertionEvidenceInput) -> GraphResult<()> {
        self.service.remove_evidence(input).await
    }

    pub async fn get_concept(&self, input: ConceptIdInput) -> GraphResult<Option<Concept>> {
        self.service.get_concept(input).await
    }

    pub async fn resolve_alias(&self, input: AliasQuery) -> GraphResult<Option<Concept>> {
        self.service.resolve_alias(input).await
    }

    pub async fn get_assertion(&self, input: AssertionIdInput) -> GraphResult<Option<Assertion>> {
        self.service.get_assertion(input).await
    }

    pub async fn get_source(
        &self,
        input: SourceReferenceInput,
    ) -> GraphResult<Option<SourceReference>> {
        self.service.get_source(input).await
    }

    pub async fn neighbors(&self, input: NeighborQuery) -> GraphResult<Vec<NeighborResult>> {
        self.service.neighbors(input).await
    }

    pub async fn related_sources(
        &self,
        input: RelatedSourceQuery,
    ) -> GraphResult<Vec<RelatedSourceResult>> {
        self.service.related_sources(input).await
    }

    pub async fn find_paths(&self, input: PathQuery) -> GraphResult<Vec<GraphPath>> {
        self.service.find_paths(input).await
    }
}
