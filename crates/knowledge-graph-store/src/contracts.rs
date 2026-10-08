//! Public graph contracts.

use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashSet},
    fmt,
    io::{self, Write},
    time::Duration,
};

use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const MAX_SOURCE_KEY_BYTES: usize = 2_048;
pub const MAX_ALIAS_DISPLAY_BYTES: usize = 512;
pub const MAX_ALIAS_KEY_BYTES: usize = 2_048;
pub const MAX_CONCEPT_ALIASES: usize = 64;
pub const MAX_ASSERTION_EVIDENCE: usize = 32;
pub const MAX_NEIGHBOR_RESULTS: u16 = 256;
pub const MAX_RELATED_SOURCE_RESULTS: u16 = 256;
pub const MAX_PATH_RESULTS: u16 = 50;
pub const MAX_PATH_DEPTH: u8 = 8;
pub const MAX_PATH_WORK: u32 = 10_000;
pub const MAX_RESPONSE_JSON_BYTES: usize = 4 * 1024 * 1024;

pub const MIN_QUERY_TIMEOUT: Duration = Duration::from_secs(1);
pub const MAX_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
pub const MAX_INVOCATION_TIMEOUT: Duration = Duration::from_secs(60);
pub const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_INVOCATION_TIMEOUT: Duration = Duration::from_secs(15);

macro_rules! string_code_enum {
    ($name:ident $(, $serde_derive:ident)? { $($variant:ident => $code:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize $(, $serde_derive)?)]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $code),+
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

string_code_enum!(OperationCode {
    CreateConcept => "create_concept",
    ReplaceConceptAliases => "replace_concept_aliases",
    DeleteConcept => "delete_concept",
    RegisterSource => "register_source",
    DeleteSource => "delete_source",
    CreateMention => "create_mention",
    DeleteMention => "delete_mention",
    CreateAssertion => "create_assertion",
    UpdateAssertion => "update_assertion",
    DeleteAssertion => "delete_assertion",
    AddEvidence => "add_evidence",
    RemoveEvidence => "remove_evidence",
    GetConcept => "get_concept",
    ResolveAlias => "resolve_alias",
    GetAssertion => "get_assertion",
    GetSource => "get_source",
    Neighbors => "neighbors",
    RelatedSources => "related_sources",
    FindPaths => "find_paths",
});

string_code_enum!(FieldCode {
    Alias => "alias",
    AliasKey => "alias_key",
    AliasSet => "alias_set",
    PreferredAlias => "preferred_alias",
    SourceKind => "source_kind",
    MemoryId => "memory_id",
    MemoryVersion => "memory_version",
    SessionRecordId => "session_record_id",
    SourceKey => "source_key",
    ConceptId => "concept_id",
    AssertionId => "assertion_id",
    SubjectConceptId => "subject_concept_id",
    ObjectConceptId => "object_concept_id",
    RelationType => "relation_type",
    Revision => "revision",
    Direction => "direction",
    RelationFilter => "relation_filter",
    Orientation => "orientation",
    Path => "path",
    Limit => "limit",
    MaxDepth => "max_depth",
    MaxWork => "max_work",
    QueryTimeout => "query_timeout",
    InvocationTimeout => "invocation_timeout",
    SupportingSources => "supporting_sources",
});

string_code_enum!(ValidationReason {
    Empty => "empty",
    ContainsNul => "contains_nul",
    TooShort => "too_short",
    TooLong => "too_long",
    NotPositive => "not_positive",
    InvalidUuid => "invalid_uuid",
    NilUuid => "nil_uuid",
    NotUuidV7 => "not_uuid_v7",
    Unsupported => "unsupported",
    InvalidShape => "invalid_shape",
    SelfRelation => "self_relation",
});

string_code_enum!(ConflictField, Deserialize {
    AliasSet => "alias_set",
    Revision => "revision",
    SemanticAssertion => "semantic_assertion",
    Snapshot => "snapshot",
});

string_code_enum!(ConflictReason, Deserialize {
    AliasOwned => "alias_owned",
    AliasSetMismatch => "alias_set_mismatch",
    StaleRevision => "stale_revision",
    DuplicateSemanticAssertion => "duplicate_semantic_assertion",
    SnapshotDrift => "snapshot_drift",
});

string_code_enum!(ReferenceReason {
    InUse => "in_use",
});

string_code_enum!(NotFoundReason {
    Missing => "missing",
});

string_code_enum!(LimitResource {
    AliasCount => "alias_count",
    AssertionEvidenceCount => "assertion_evidence_count",
    QueryLimit => "query_limit",
    PathDepth => "path_depth",
    PathWork => "path_work",
    ResponseBytes => "response_bytes",
});

string_code_enum!(LimitReason {
    Exceeded => "exceeded",
});

string_code_enum!(TraversalBound {
    Depth => "depth",
    Work => "work",
    QueryTimeout => "query_timeout",
});

string_code_enum!(TraversalReason {
    WorkExhausted => "work_exhausted",
    QueryTimeout => "query_timeout",
});

string_code_enum!(BackendFailureReason {
    InvocationFailed => "invocation_failed",
    DatabaseFailure => "database_failure",
    MalformedResponse => "malformed_response",
    UnknownOutcome => "unknown_outcome",
    UnsupportedValue => "unsupported_value",
});

string_code_enum!(RecordKind, Deserialize {
    Concept => "concept",
    Assertion => "assertion",
    SourceReference => "source_reference",
    ConceptMention => "concept_mention",
    AssertionEvidence => "assertion_evidence",
});

string_code_enum!(SourceKind {
    MemoryVersion => "memory_version",
    SessionRecord => "session_record",
});

string_code_enum!(RelationSemantics {
    Directed => "directed",
    Symmetric => "symmetric",
});

string_code_enum!(RelationType, Deserialize {
    RelatedTo => "related_to",
    IsA => "is_a",
    PartOf => "part_of",
    DependsOn => "depends_on",
    Uses => "uses",
    Implements => "implements",
    Causes => "causes",
    Resolves => "resolves",
    Contradicts => "contradicts",
});

string_code_enum!(DirectionMode, Deserialize {
    Outgoing => "outgoing",
    Incoming => "incoming",
    Either => "either",
});

string_code_enum!(EdgeOrientation, Deserialize {
    Outgoing => "outgoing",
    Incoming => "incoming",
    Symmetric => "symmetric",
});

string_code_enum!(RelatedSourceRole, Deserialize {
    Mention => "mention",
    AssertionEvidence => "assertion_evidence",
});

impl RelationType {
    pub const fn semantics(self) -> RelationSemantics {
        match self {
            Self::RelatedTo | Self::Contradicts => RelationSemantics::Symmetric,
            Self::IsA
            | Self::PartOf
            | Self::DependsOn
            | Self::Uses
            | Self::Implements
            | Self::Causes
            | Self::Resolves => RelationSemantics::Directed,
        }
    }

    pub const fn is_symmetric(self) -> bool {
        matches!(self.semantics(), RelationSemantics::Symmetric)
    }
}

impl TryFrom<&str> for RelationType {
    type Error = ContractError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::ALL
            .iter()
            .copied()
            .find(|relation| relation.as_str() == value)
            .ok_or_else(|| {
                ContractError::invalid(FieldCode::RelationType, ValidationReason::Unsupported)
            })
    }
}

impl TryFrom<String> for RelationType {
    type Error = ContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl TryFrom<&str> for DirectionMode {
    type Error = ContractError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::ALL
            .iter()
            .copied()
            .find(|direction| direction.as_str() == value)
            .ok_or_else(|| {
                ContractError::invalid(FieldCode::Direction, ValidationReason::Unsupported)
            })
    }
}

impl TryFrom<String> for DirectionMode {
    type Error = ContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("invalid_input:{field}:{reason}")]
pub struct ContractError {
    field: FieldCode,
    reason: ValidationReason,
}

impl ContractError {
    pub const fn field(&self) -> FieldCode {
        self.field
    }

    pub const fn reason(&self) -> ValidationReason {
        self.reason
    }

    pub(crate) const fn invalid(field: FieldCode, reason: ValidationReason) -> Self {
        Self { field, reason }
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct ConceptId(Uuid);

impl ConceptId {
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    pub const fn into_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Debug for ConceptId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ConceptId(<redacted>)")
    }
}

impl TryFrom<Uuid> for ConceptId {
    type Error = ContractError;

    fn try_from(value: Uuid) -> Result<Self, Self::Error> {
        validate_uuid_v7(value, FieldCode::ConceptId).map(Self)
    }
}

impl TryFrom<String> for ConceptId {
    type Error = ContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let uuid = parse_uuid(&value, FieldCode::ConceptId)?;
        validate_uuid_v7(uuid, FieldCode::ConceptId).map(Self)
    }
}

impl<'de> Deserialize<'de> for ConceptId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        parse_uuid(&value, FieldCode::ConceptId)
            .and_then(Self::try_from)
            .map_err(serde::de::Error::custom)
    }
}

impl Serialize for ConceptId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct AssertionId(Uuid);

impl AssertionId {
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    pub const fn into_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Debug for AssertionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AssertionId(<redacted>)")
    }
}

impl TryFrom<Uuid> for AssertionId {
    type Error = ContractError;

    fn try_from(value: Uuid) -> Result<Self, Self::Error> {
        validate_uuid_v7(value, FieldCode::AssertionId).map(Self)
    }
}

impl TryFrom<String> for AssertionId {
    type Error = ContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let uuid = parse_uuid(&value, FieldCode::AssertionId)?;
        validate_uuid_v7(uuid, FieldCode::AssertionId).map(Self)
    }
}

impl<'de> Deserialize<'de> for AssertionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        parse_uuid(&value, FieldCode::AssertionId)
            .and_then(Self::try_from)
            .map_err(serde::de::Error::custom)
    }
}

impl Serialize for AssertionId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

#[derive(Clone, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct MemoryId(String);

impl MemoryId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for MemoryId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MemoryId(<redacted>)")
    }
}

impl TryFrom<String> for MemoryId {
    type Error = ContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_source_key(&value, FieldCode::MemoryId).map(|()| Self(value))
    }
}

impl<'de> Deserialize<'de> for MemoryId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_from(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct MemoryVersion(i64);

impl MemoryVersion {
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl TryFrom<i64> for MemoryVersion {
    type Error = ContractError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value <= 0 {
            return Err(ContractError::invalid(
                FieldCode::MemoryVersion,
                ValidationReason::NotPositive,
            ));
        }

        Ok(Self(value))
    }
}

impl<'de> Deserialize<'de> for MemoryVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = i64::deserialize(deserializer)?;
        Self::try_from(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SessionRecordId(String);

impl SessionRecordId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SessionRecordId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionRecordId(<redacted>)")
    }
}

impl TryFrom<String> for SessionRecordId {
    type Error = ContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_source_key(&value, FieldCode::SessionRecordId).map(|()| Self(value))
    }
}

impl<'de> Deserialize<'de> for SessionRecordId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_from(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Revision(i64);

impl Revision {
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl TryFrom<i64> for Revision {
    type Error = ContractError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value <= 0 {
            return Err(ContractError::invalid(
                FieldCode::Revision,
                ValidationReason::NotPositive,
            ));
        }

        Ok(Self(value))
    }
}

impl<'de> Deserialize<'de> for Revision {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = i64::deserialize(deserializer)?;
        Self::try_from(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceReferenceInput {
    MemoryVersion { memory_id: String, version: i64 },
    SessionRecord { session_record_id: String },
}

impl SourceReferenceInput {
    pub const fn kind(&self) -> SourceKind {
        match self {
            Self::MemoryVersion { .. } => SourceKind::MemoryVersion,
            Self::SessionRecord { .. } => SourceKind::SessionRecord,
        }
    }
}

impl fmt::Debug for SourceReferenceInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MemoryVersion { .. } => {
                formatter.write_str("SourceReferenceInput::MemoryVersion(<redacted>)")
            }
            Self::SessionRecord { .. } => {
                formatter.write_str("SourceReferenceInput::SessionRecord(<redacted>)")
            }
        }
    }
}

#[derive(Clone, Eq, Hash, PartialEq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceReference {
    MemoryVersion {
        memory_id: MemoryId,
        version: MemoryVersion,
    },
    SessionRecord {
        session_record_id: SessionRecordId,
    },
}

impl SourceReference {
    pub const fn kind(&self) -> SourceKind {
        match self {
            Self::MemoryVersion { .. } => SourceKind::MemoryVersion,
            Self::SessionRecord { .. } => SourceKind::SessionRecord,
        }
    }
}

impl fmt::Debug for SourceReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MemoryVersion { .. } => {
                formatter.write_str("SourceReference::MemoryVersion(<redacted>)")
            }
            Self::SessionRecord { .. } => {
                formatter.write_str("SourceReference::SessionRecord(<redacted>)")
            }
        }
    }
}

impl TryFrom<SourceReferenceInput> for SourceReference {
    type Error = ContractError;

    fn try_from(input: SourceReferenceInput) -> Result<Self, Self::Error> {
        match input {
            SourceReferenceInput::MemoryVersion { memory_id, version } => Ok(Self::MemoryVersion {
                memory_id: MemoryId::try_from(memory_id)?,
                version: MemoryVersion::try_from(version)?,
            }),
            SourceReferenceInput::SessionRecord { session_record_id } => Ok(Self::SessionRecord {
                session_record_id: SessionRecordId::try_from(session_record_id)?,
            }),
        }
    }
}

#[derive(Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Alias {
    pub display_text: String,
    pub preferred: bool,
}

impl fmt::Debug for Alias {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Alias")
            .field("display_text", &"<redacted>")
            .field("preferred", &self.preferred)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateConcept {
    pub aliases: Vec<Alias>,
}

impl fmt::Debug for CreateConcept {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateConcept")
            .field("alias_count", &self.aliases.len())
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplaceConceptAliases {
    pub concept_id: ConceptId,
    pub expected_revision: Revision,
    pub aliases: Vec<Alias>,
}

impl fmt::Debug for ReplaceConceptAliases {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplaceConceptAliases")
            .field("concept_id", &self.concept_id)
            .field("expected_revision", &self.expected_revision)
            .field("alias_count", &self.aliases.len())
            .finish()
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteConcept {
    pub concept_id: ConceptId,
    pub expected_revision: Revision,
}

impl fmt::Debug for DeleteConcept {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeleteConcept")
            .field("concept_id", &self.concept_id)
            .field("expected_revision", &self.expected_revision)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteSource {
    pub source: SourceReferenceInput,
}

impl fmt::Debug for DeleteSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeleteSource(<redacted>)")
    }
}

#[derive(Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConceptMentionInput {
    pub concept_id: ConceptId,
    pub source: SourceReferenceInput,
}

impl fmt::Debug for ConceptMentionInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConceptMentionInput")
            .field("concept_id", &self.concept_id)
            .field("source", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateAssertion {
    pub subject_concept_id: ConceptId,
    pub relation_type: RelationType,
    pub object_concept_id: ConceptId,
    pub supporting_sources: Vec<SourceReferenceInput>,
}

impl fmt::Debug for CreateAssertion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateAssertion")
            .field("subject_concept_id", &self.subject_concept_id)
            .field("relation_type", &self.relation_type)
            .field("object_concept_id", &self.object_concept_id)
            .field("supporting_source_count", &self.supporting_sources.len())
            .finish()
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateAssertion {
    pub assertion_id: AssertionId,
    pub expected_revision: Revision,
    pub subject_concept_id: ConceptId,
    pub relation_type: RelationType,
    pub object_concept_id: ConceptId,
}

impl fmt::Debug for UpdateAssertion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UpdateAssertion")
            .field("assertion_id", &self.assertion_id)
            .field("expected_revision", &self.expected_revision)
            .field("subject_concept_id", &self.subject_concept_id)
            .field("relation_type", &self.relation_type)
            .field("object_concept_id", &self.object_concept_id)
            .finish()
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteAssertion {
    pub assertion_id: AssertionId,
    pub expected_revision: Revision,
}

impl fmt::Debug for DeleteAssertion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeleteAssertion")
            .field("assertion_id", &self.assertion_id)
            .field("expected_revision", &self.expected_revision)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssertionEvidenceInput {
    pub assertion_id: AssertionId,
    pub source: SourceReferenceInput,
}

impl fmt::Debug for AssertionEvidenceInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AssertionEvidenceInput")
            .field("assertion_id", &self.assertion_id)
            .field("source", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Concept {
    pub id: ConceptId,
    pub revision: Revision,
    pub aliases: Vec<Alias>,
}

impl<'de> Deserialize<'de> for Concept {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireConcept {
            id: ConceptId,
            revision: Revision,
            aliases: Vec<Alias>,
        }

        let wire = WireConcept::deserialize(deserializer)?;
        let concept = Self {
            id: wire.id,
            revision: wire.revision,
            aliases: wire.aliases,
        };
        concept
            .validate_for_read()
            .map_err(serde::de::Error::custom)?;
        Ok(concept)
    }
}

impl Concept {
    fn validate_for_read(&self) -> Result<(), ContractError> {
        let aliases = crate::normalization::normalize_alias_set(&self.aliases)?;
        if !aliases
            .aliases()
            .first()
            .is_some_and(|alias| alias.is_preferred())
        {
            return Err(ContractError::invalid(
                FieldCode::PreferredAlias,
                ValidationReason::InvalidShape,
            ));
        }

        Ok(())
    }
}

impl fmt::Debug for Concept {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Concept")
            .field("id", &self.id)
            .field("revision", &self.revision)
            .field("alias_count", &self.aliases.len())
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Assertion {
    pub id: AssertionId,
    pub revision: Revision,
    pub subject_concept_id: ConceptId,
    pub relation_type: RelationType,
    pub object_concept_id: ConceptId,
    pub evidence: Vec<SourceReference>,
}

impl<'de> Deserialize<'de> for Assertion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireAssertion {
            id: AssertionId,
            revision: Revision,
            subject_concept_id: ConceptId,
            relation_type: RelationType,
            object_concept_id: ConceptId,
            evidence: Vec<SourceReference>,
        }

        let wire = WireAssertion::deserialize(deserializer)?;
        let assertion = Self {
            id: wire.id,
            revision: wire.revision,
            subject_concept_id: wire.subject_concept_id,
            relation_type: wire.relation_type,
            object_concept_id: wire.object_concept_id,
            evidence: wire.evidence,
        };
        assertion
            .validate_for_read()
            .map_err(serde::de::Error::custom)?;
        Ok(assertion)
    }
}

impl Assertion {
    fn validate_for_read(&self) -> Result<(), ContractError> {
        if self.subject_concept_id == self.object_concept_id {
            return Err(ContractError::invalid(
                FieldCode::SubjectConceptId,
                ValidationReason::SelfRelation,
            ));
        }
        if self.evidence.is_empty() {
            return Err(ContractError::invalid(
                FieldCode::SupportingSources,
                ValidationReason::Empty,
            ));
        }
        if self.evidence.len() > MAX_ASSERTION_EVIDENCE {
            return Err(ContractError::invalid(
                FieldCode::SupportingSources,
                ValidationReason::TooLong,
            ));
        }
        if !self
            .evidence
            .windows(2)
            .all(|pair| source_identity_ordering(&pair[0], &pair[1]) == Ordering::Less)
        {
            return Err(ContractError::invalid(
                FieldCode::SupportingSources,
                ValidationReason::InvalidShape,
            ));
        }

        Ok(())
    }
}

impl fmt::Debug for Assertion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Assertion")
            .field("id", &self.id)
            .field("revision", &self.revision)
            .field("subject_concept_id", &self.subject_concept_id)
            .field("relation_type", &self.relation_type)
            .field("object_concept_id", &self.object_concept_id)
            .field("evidence_count", &self.evidence.len())
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConceptIdInput {
    pub id: String,
}

impl fmt::Debug for ConceptIdInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ConceptIdInput(<redacted>)")
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ValidatedConceptIdInput {
    pub(crate) id: ConceptId,
}

#[cfg_attr(not(test), allow(dead_code))]
impl TryFrom<ConceptIdInput> for ValidatedConceptIdInput {
    type Error = ContractError;

    fn try_from(input: ConceptIdInput) -> Result<Self, Self::Error> {
        ConceptId::try_from(input.id).map(|id| Self { id })
    }
}

#[derive(Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssertionIdInput {
    pub id: String,
}

impl fmt::Debug for AssertionIdInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AssertionIdInput(<redacted>)")
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ValidatedAssertionIdInput {
    pub(crate) id: AssertionId,
}

#[cfg_attr(not(test), allow(dead_code))]
impl TryFrom<AssertionIdInput> for ValidatedAssertionIdInput {
    type Error = ContractError;

    fn try_from(input: AssertionIdInput) -> Result<Self, Self::Error> {
        AssertionId::try_from(input.id).map(|id| Self { id })
    }
}

#[derive(Clone, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AliasQuery {
    pub alias: String,
}

impl TryFrom<String> for AliasQuery {
    type Error = ContractError;

    fn try_from(alias: String) -> Result<Self, Self::Error> {
        crate::normalization::normalize_alias_key(&alias)?;
        Ok(Self { alias })
    }
}

impl<'de> Deserialize<'de> for AliasQuery {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireAliasQuery {
            alias: String,
        }

        let wire = WireAliasQuery::deserialize(deserializer)?;
        Self::try_from(wire.alias).map_err(serde::de::Error::custom)
    }
}

impl fmt::Debug for AliasQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AliasQuery(<redacted>)")
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ValidatedAliasQuery {
    pub(crate) alias_key: AliasKey,
}

#[cfg_attr(not(test), allow(dead_code))]
impl ValidatedAliasQuery {
    /// Accepts the output of the versioned normalizer without reimplementing it.
    pub(crate) fn try_from_normalized(
        input: AliasQuery,
        normalized_key: String,
    ) -> Result<Self, ContractError> {
        validate_alias_display(&input.alias)?;
        validate_bounded_text(&normalized_key, FieldCode::AliasKey, MAX_ALIAS_KEY_BYTES)?;

        Ok(Self {
            alias_key: AliasKey::from_normalized(normalized_key),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NeighborQuery {
    pub concept_id: ConceptId,
    pub direction: DirectionMode,
    /// An empty filter selects every relation type.
    #[serde(default)]
    pub relation_filter: Vec<RelationType>,
    pub limit: u16,
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ValidatedNeighborQuery {
    pub(crate) concept_id: ConceptId,
    pub(crate) direction: DirectionMode,
    pub(crate) relation_filter: Vec<RelationType>,
    pub(crate) limit: u16,
}

#[cfg_attr(not(test), allow(dead_code))]
impl TryFrom<NeighborQuery> for ValidatedNeighborQuery {
    type Error = ContractError;

    fn try_from(input: NeighborQuery) -> Result<Self, Self::Error> {
        validate_result_limit(input.limit, MAX_NEIGHBOR_RESULTS)?;

        let mut relation_filter = input.relation_filter;
        relation_filter.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        relation_filter.dedup();

        Ok(Self {
            concept_id: input.concept_id,
            direction: input.direction,
            relation_filter,
            limit: input.limit,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelatedSourceQuery {
    pub concept_id: ConceptId,
    pub limit: u16,
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ValidatedRelatedSourceQuery {
    pub(crate) concept_id: ConceptId,
    pub(crate) limit: u16,
}

#[cfg_attr(not(test), allow(dead_code))]
impl TryFrom<RelatedSourceQuery> for ValidatedRelatedSourceQuery {
    type Error = ContractError;

    fn try_from(input: RelatedSourceQuery) -> Result<Self, Self::Error> {
        validate_result_limit(input.limit, MAX_RELATED_SOURCE_RESULTS)?;

        Ok(Self {
            concept_id: input.concept_id,
            limit: input.limit,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PathQuery {
    pub from: ConceptId,
    pub to: ConceptId,
    pub direction: DirectionMode,
    pub max_depth: u8,
    pub max_work: u32,
    pub limit: u16,
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ValidatedPathQuery {
    pub(crate) from: ConceptId,
    pub(crate) to: ConceptId,
    pub(crate) direction: DirectionMode,
    pub(crate) max_depth: u8,
    pub(crate) max_work: u32,
    pub(crate) limit: u16,
}

#[cfg_attr(not(test), allow(dead_code))]
impl TryFrom<PathQuery> for ValidatedPathQuery {
    type Error = ContractError;

    fn try_from(input: PathQuery) -> Result<Self, Self::Error> {
        if input.from == input.to {
            return Err(ContractError::invalid(
                FieldCode::ConceptId,
                ValidationReason::SelfRelation,
            ));
        }
        if input.max_depth == 0 {
            return Err(ContractError::invalid(
                FieldCode::MaxDepth,
                ValidationReason::NotPositive,
            ));
        }
        if input.max_depth > MAX_PATH_DEPTH {
            return Err(ContractError::invalid(
                FieldCode::MaxDepth,
                ValidationReason::TooLong,
            ));
        }
        if input.max_work == 0 {
            return Err(ContractError::invalid(
                FieldCode::MaxWork,
                ValidationReason::NotPositive,
            ));
        }
        if input.max_work > MAX_PATH_WORK {
            return Err(ContractError::invalid(
                FieldCode::MaxWork,
                ValidationReason::TooLong,
            ));
        }
        validate_result_limit(input.limit, MAX_PATH_RESULTS)?;

        Ok(Self {
            from: input.from,
            to: input.to,
            direction: input.direction,
            max_depth: input.max_depth,
            max_work: input.max_work,
            limit: input.limit,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DatabaseTimeouts {
    query: Duration,
    invocation: Duration,
}

impl DatabaseTimeouts {
    pub fn try_new(query: Duration, invocation: Duration) -> Result<Self, ContractError> {
        if query.is_zero() {
            return Err(ContractError::invalid(
                FieldCode::QueryTimeout,
                ValidationReason::NotPositive,
            ));
        }
        if query < MIN_QUERY_TIMEOUT {
            return Err(ContractError::invalid(
                FieldCode::QueryTimeout,
                ValidationReason::TooShort,
            ));
        }
        if query > MAX_QUERY_TIMEOUT {
            return Err(ContractError::invalid(
                FieldCode::QueryTimeout,
                ValidationReason::TooLong,
            ));
        }
        if invocation.is_zero() {
            return Err(ContractError::invalid(
                FieldCode::InvocationTimeout,
                ValidationReason::NotPositive,
            ));
        }
        if invocation < MIN_QUERY_TIMEOUT {
            return Err(ContractError::invalid(
                FieldCode::InvocationTimeout,
                ValidationReason::TooShort,
            ));
        }
        if invocation > MAX_INVOCATION_TIMEOUT {
            return Err(ContractError::invalid(
                FieldCode::InvocationTimeout,
                ValidationReason::TooLong,
            ));
        }
        if invocation <= query {
            return Err(ContractError::invalid(
                FieldCode::InvocationTimeout,
                ValidationReason::InvalidShape,
            ));
        }

        Ok(Self { query, invocation })
    }

    pub const fn query(self) -> Duration {
        self.query
    }

    pub const fn invocation(self) -> Duration {
        self.invocation
    }
}

impl Default for DatabaseTimeouts {
    fn default() -> Self {
        Self {
            query: DEFAULT_QUERY_TIMEOUT,
            invocation: DEFAULT_INVOCATION_TIMEOUT,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NeighborResult {
    pub neighbor: Concept,
    pub assertion: Assertion,
    pub orientation: EdgeOrientation,
}

impl NeighborResult {
    fn try_new(
        neighbor: Concept,
        assertion: Assertion,
        orientation: EdgeOrientation,
    ) -> Result<Self, ContractError> {
        neighbor.validate_for_read()?;
        assertion.validate_for_read()?;
        if !neighbor_orientation_is_valid(&neighbor, &assertion, orientation) {
            return Err(ContractError::invalid(
                FieldCode::Orientation,
                ValidationReason::InvalidShape,
            ));
        }

        Ok(Self {
            neighbor,
            assertion,
            orientation,
        })
    }
}

impl<'de> Deserialize<'de> for NeighborResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireNeighborResult {
            neighbor: Concept,
            assertion: Assertion,
            orientation: EdgeOrientation,
        }

        let wire = WireNeighborResult::deserialize(deserializer)?;
        Self::try_new(wire.neighbor, wire.assertion, wire.orientation)
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelatedSourceResult {
    pub source: SourceReference,
    pub role: RelatedSourceRole,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OrientedAssertion {
    pub assertion: Assertion,
    pub orientation: EdgeOrientation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphPath {
    pub concepts: Vec<ConceptId>,
    pub assertions: Vec<OrientedAssertion>,
}

impl GraphPath {
    fn try_new(
        concepts: Vec<ConceptId>,
        assertions: Vec<OrientedAssertion>,
    ) -> Result<Self, ContractError> {
        if concepts.len() < 2 || assertions.len().checked_add(1) != Some(concepts.len()) {
            return Err(ContractError::invalid(
                FieldCode::Path,
                ValidationReason::InvalidShape,
            ));
        }
        if assertions.len() > usize::from(MAX_PATH_DEPTH) {
            return Err(ContractError::invalid(
                FieldCode::MaxDepth,
                ValidationReason::TooLong,
            ));
        }

        let mut seen = HashSet::with_capacity(concepts.len());
        if concepts.iter().any(|concept_id| !seen.insert(*concept_id)) {
            return Err(ContractError::invalid(
                FieldCode::Path,
                ValidationReason::SelfRelation,
            ));
        }

        for (index, edge) in assertions.iter().enumerate() {
            edge.assertion.validate_for_read()?;
            let from = concepts[index];
            let to = concepts[index + 1];
            let valid_orientation = match edge.orientation {
                EdgeOrientation::Outgoing => {
                    !edge.assertion.relation_type.is_symmetric()
                        && edge.assertion.subject_concept_id == from
                        && edge.assertion.object_concept_id == to
                }
                EdgeOrientation::Incoming => {
                    !edge.assertion.relation_type.is_symmetric()
                        && edge.assertion.object_concept_id == from
                        && edge.assertion.subject_concept_id == to
                }
                EdgeOrientation::Symmetric => {
                    edge.assertion.relation_type.is_symmetric()
                        && ((edge.assertion.subject_concept_id == from
                            && edge.assertion.object_concept_id == to)
                            || (edge.assertion.subject_concept_id == to
                                && edge.assertion.object_concept_id == from))
                }
            };
            if !valid_orientation {
                return Err(ContractError::invalid(
                    FieldCode::Orientation,
                    ValidationReason::InvalidShape,
                ));
            }
        }

        Ok(Self {
            concepts,
            assertions,
        })
    }
}

impl<'de> Deserialize<'de> for GraphPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireGraphPath {
            concepts: Vec<ConceptId>,
            assertions: Vec<OrientedAssertion>,
        }

        let wire = WireGraphPath::deserialize(deserializer)?;
        Self::try_new(wire.concepts, wire.assertions).map_err(serde::de::Error::custom)
    }
}

fn neighbor_orientation_is_valid(
    neighbor: &Concept,
    assertion: &Assertion,
    orientation: EdgeOrientation,
) -> bool {
    match orientation {
        EdgeOrientation::Outgoing => {
            !assertion.relation_type.is_symmetric() && neighbor.id == assertion.object_concept_id
        }
        EdgeOrientation::Incoming => {
            !assertion.relation_type.is_symmetric() && neighbor.id == assertion.subject_concept_id
        }
        EdgeOrientation::Symmetric => {
            assertion.relation_type.is_symmetric()
                && (neighbor.id == assertion.subject_concept_id
                    || neighbor.id == assertion.object_concept_id)
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn order_neighbor_results(results: &mut [NeighborResult]) {
    results.sort_by(|left, right| {
        left.assertion
            .relation_type
            .as_str()
            .cmp(right.assertion.relation_type.as_str())
            .then_with(|| left.orientation.as_str().cmp(right.orientation.as_str()))
            .then_with(|| left.neighbor.id.as_uuid().cmp(right.neighbor.id.as_uuid()))
            .then_with(|| {
                left.assertion
                    .id
                    .as_uuid()
                    .cmp(right.assertion.id.as_uuid())
            })
    });
}

#[cfg_attr(not(test), allow(dead_code))]
fn order_related_source_results(results: &mut Vec<RelatedSourceResult>) {
    results.sort_by(|left, right| {
        related_source_role_order(left.role)
            .cmp(&related_source_role_order(right.role))
            .then_with(|| source_identity_ordering(&left.source, &right.source))
    });
    results.dedup_by(|left, right| left.role == right.role && left.source == right.source);
}

#[cfg_attr(not(test), allow(dead_code))]
fn related_source_role_order(role: RelatedSourceRole) -> u8 {
    match role {
        RelatedSourceRole::Mention => 0,
        RelatedSourceRole::AssertionEvidence => 1,
    }
}

fn source_identity_ordering(left: &SourceReference, right: &SourceReference) -> Ordering {
    match (left, right) {
        (
            SourceReference::MemoryVersion {
                memory_id: left_id,
                version: left_version,
            },
            SourceReference::MemoryVersion {
                memory_id: right_id,
                version: right_version,
            },
        ) => left_id
            .as_str()
            .cmp(right_id.as_str())
            .then_with(|| left_version.get().cmp(&right_version.get())),
        (
            SourceReference::SessionRecord {
                session_record_id: left_id,
            },
            SourceReference::SessionRecord {
                session_record_id: right_id,
            },
        ) => left_id.as_str().cmp(right_id.as_str()),
        (SourceReference::MemoryVersion { .. }, SourceReference::SessionRecord { .. }) => {
            Ordering::Less
        }
        (SourceReference::SessionRecord { .. }, SourceReference::MemoryVersion { .. }) => {
            Ordering::Greater
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn compare_graph_paths(left: &GraphPath, right: &GraphPath) -> Ordering {
    left.assertions
        .len()
        .cmp(&right.assertions.len())
        .then_with(|| {
            left.assertions
                .iter()
                .map(|edge| edge.assertion.id.as_uuid())
                .cmp(
                    right
                        .assertions
                        .iter()
                        .map(|edge| edge.assertion.id.as_uuid()),
                )
        })
        .then_with(|| {
            left.concepts
                .iter()
                .map(ConceptId::as_uuid)
                .cmp(right.concepts.iter().map(ConceptId::as_uuid))
        })
}

#[cfg_attr(not(test), allow(dead_code))]
fn order_graph_paths(paths: &mut [GraphPath]) {
    paths.sort_by(compare_graph_paths);
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum DatabaseReadOutcome<T> {
    Complete { result: T },
    ResultTooLarge,
    WorkExhausted,
}

impl<'de, T> Deserialize<'de> for DatabaseReadOutcome<T>
where
    T: Deserialize<'de> + Serialize,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ReadOutcomeVisitor<T>(std::marker::PhantomData<T>);

        impl<'de, T> serde::de::Visitor<'de> for ReadOutcomeVisitor<T>
        where
            T: Deserialize<'de> + Serialize,
        {
            type Value = DatabaseReadOutcome<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a complete read result or a payload-free result marker")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut outcome: Option<String> = None;
                let mut result: Option<T> = None;
                let mut has_result = false;

                while let Some(field) = map.next_key::<String>()? {
                    match field.as_str() {
                        "outcome" => {
                            if outcome.is_some() {
                                return Err(serde::de::Error::duplicate_field("outcome"));
                            }
                            outcome = Some(map.next_value()?);
                        }
                        "result" => {
                            if has_result {
                                return Err(serde::de::Error::duplicate_field("result"));
                            }
                            has_result = true;
                            result = Some(map.next_value()?);
                        }
                        _ => {
                            return Err(serde::de::Error::unknown_field(
                                &field,
                                &["outcome", "result"],
                            ));
                        }
                    }
                }

                let outcome = outcome.ok_or_else(|| serde::de::Error::missing_field("outcome"))?;
                match outcome.as_str() {
                    "complete" => {
                        if !has_result {
                            return Err(serde::de::Error::missing_field("result"));
                        }
                        let result = match result {
                            Some(result) => result,
                            None => return Err(serde::de::Error::missing_field("result")),
                        };
                        match serialized_json_within_response_budget(&result) {
                            Ok(true) => Ok(DatabaseReadOutcome::Complete { result }),
                            Ok(false) => Err(serde::de::Error::custom(
                                "complete read result exceeds the response JSON bound",
                            )),
                            Err(()) => Err(serde::de::Error::custom(
                                "complete read result cannot be serialized",
                            )),
                        }
                    }
                    "result_too_large" => {
                        if has_result {
                            return Err(serde::de::Error::custom(
                                "result_too_large cannot contain a result payload",
                            ));
                        }
                        Ok(DatabaseReadOutcome::ResultTooLarge)
                    }
                    "work_exhausted" => {
                        if has_result {
                            return Err(serde::de::Error::custom(
                                "work_exhausted cannot contain a result payload",
                            ));
                        }
                        Ok(DatabaseReadOutcome::WorkExhausted)
                    }
                    _ => Err(serde::de::Error::unknown_variant(
                        &outcome,
                        &["complete", "result_too_large", "work_exhausted"],
                    )),
                }
            }
        }

        deserializer.deserialize_map(ReadOutcomeVisitor(std::marker::PhantomData))
    }
}

#[cfg_attr(not(test), allow(dead_code))]
impl<T> DatabaseReadOutcome<T> {
    /// Checks the serialized result payload before constructing a complete outcome.
    pub(crate) fn try_complete(operation: OperationCode, result: T) -> GraphResult<Self>
    where
        T: Serialize,
    {
        match serialized_json_within_response_budget(&result) {
            Ok(true) => Ok(Self::Complete { result }),
            Ok(false) => Ok(Self::ResultTooLarge),
            Err(()) => Err(GraphError::InvalidResponse {
                operation,
                reason: BackendFailureReason::UnsupportedValue,
            }),
        }
    }

    pub(crate) fn into_graph_result(self, operation: OperationCode) -> GraphResult<T> {
        match self {
            Self::Complete { result } => Ok(result),
            Self::ResultTooLarge => Err(GraphError::ResultBoundExceeded { operation }),
            Self::WorkExhausted if operation == OperationCode::FindPaths => {
                Err(GraphError::TraversalBoundExceeded {
                    operation,
                    bound: TraversalBound::Work,
                    reason: TraversalReason::WorkExhausted,
                })
            }
            Self::WorkExhausted => Err(GraphError::InvalidResponse {
                operation,
                reason: BackendFailureReason::UnknownOutcome,
            }),
        }
    }
}

#[derive(Default)]
struct JsonByteCounter {
    bytes: usize,
    exceeded: bool,
}

impl Write for JsonByteCounter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.len() > MAX_RESPONSE_JSON_BYTES - self.bytes {
            self.exceeded = true;
            return Err(io::Error::other("response JSON byte bound exceeded"));
        }

        self.bytes += buffer.len();
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialized_json_within_response_budget<T: Serialize>(value: &T) -> Result<bool, ()> {
    let mut counter = JsonByteCounter::default();
    match serde_json::to_writer(&mut counter, value) {
        Ok(()) => Ok(true),
        Err(_) if counter.exceeded => Ok(false),
        Err(_) => Err(()),
    }
}

#[derive(Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConceptMention {
    pub concept_id: ConceptId,
    pub source: SourceReference,
}

impl fmt::Debug for ConceptMention {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConceptMention")
            .field("concept_id", &self.concept_id)
            .field("source", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssertionEvidence {
    pub assertion_id: AssertionId,
    pub source: SourceReference,
}

impl fmt::Debug for AssertionEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AssertionEvidence")
            .field("assertion_id", &self.assertion_id)
            .field("source", &"<redacted>")
            .finish()
    }
}

// Validated alias values are exposed read-only; the remaining port types stay private.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AliasKey(String);

impl AliasKey {
    pub(crate) fn from_normalized(value: String) -> Self {
        Self(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for AliasKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AliasKey(<redacted>)")
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct AliasSetIdentity {
    normalized_keys: BTreeSet<AliasKey>,
    preferred_key: AliasKey,
}

impl AliasSetIdentity {
    pub(crate) fn new(
        normalized_keys: impl IntoIterator<Item = AliasKey>,
        preferred_key: AliasKey,
    ) -> Self {
        Self {
            normalized_keys: normalized_keys.into_iter().collect(),
            preferred_key,
        }
    }

    pub fn normalized_keys(&self) -> impl Iterator<Item = &AliasKey> {
        self.normalized_keys.iter()
    }

    pub const fn preferred_key(&self) -> &AliasKey {
        &self.preferred_key
    }
}

impl fmt::Debug for AliasSetIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AliasSetIdentity")
            .field("alias_count", &self.normalized_keys.len())
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ValidatedAlias {
    pub(crate) display_text: String,
    pub(crate) normalized_key: AliasKey,
    pub(crate) preferred: bool,
}

impl ValidatedAlias {
    pub fn display_text(&self) -> &str {
        &self.display_text
    }

    pub const fn normalized_key(&self) -> &AliasKey {
        &self.normalized_key
    }

    pub const fn is_preferred(&self) -> bool {
        self.preferred
    }
}

impl fmt::Debug for ValidatedAlias {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValidatedAlias")
            .field("display_text", &"<redacted>")
            .field("normalized_key", &"<redacted>")
            .field("preferred", &self.preferred)
            .finish()
    }
}

#[derive(Clone)]
pub struct ValidatedAliasSet {
    pub(crate) aliases: Vec<ValidatedAlias>,
    pub(crate) identity: AliasSetIdentity,
}

impl ValidatedAliasSet {
    pub fn aliases(&self) -> &[ValidatedAlias] {
        &self.aliases
    }

    pub const fn identity(&self) -> &AliasSetIdentity {
        &self.identity
    }
}

impl PartialEq for ValidatedAliasSet {
    fn eq(&self, other: &Self) -> bool {
        // Display spelling and input order do not participate in concept identity.
        self.identity == other.identity
    }
}

impl Eq for ValidatedAliasSet {}

impl fmt::Debug for ValidatedAliasSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValidatedAliasSet")
            .field("alias_count", &self.aliases.len())
            .finish()
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ValidatedCreateConcept {
    pub(crate) candidate_id: ConceptId,
    pub(crate) aliases: ValidatedAliasSet,
}

#[cfg_attr(not(test), allow(dead_code))]
impl ValidatedCreateConcept {
    pub(crate) const fn new(candidate_id: ConceptId, aliases: ValidatedAliasSet) -> Self {
        Self {
            candidate_id,
            aliases,
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ValidatedReplaceConceptAliases {
    pub(crate) concept_id: ConceptId,
    pub(crate) expected_revision: Revision,
    pub(crate) aliases: ValidatedAliasSet,
}

#[cfg_attr(not(test), allow(dead_code))]
impl ValidatedReplaceConceptAliases {
    pub(crate) const fn new(
        concept_id: ConceptId,
        expected_revision: Revision,
        aliases: ValidatedAliasSet,
    ) -> Self {
        Self {
            concept_id,
            expected_revision,
            aliases,
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ValidatedDeleteConcept {
    pub(crate) concept_id: ConceptId,
    pub(crate) expected_revision: Revision,
}

impl From<DeleteConcept> for ValidatedDeleteConcept {
    fn from(input: DeleteConcept) -> Self {
        Self {
            concept_id: input.concept_id,
            expected_revision: input.expected_revision,
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ValidatedSourceReference {
    pub(crate) source: SourceReference,
}

#[cfg_attr(not(test), allow(dead_code))]
impl TryFrom<SourceReferenceInput> for ValidatedSourceReference {
    type Error = ContractError;

    fn try_from(input: SourceReferenceInput) -> Result<Self, Self::Error> {
        SourceReference::try_from(input).map(|source| Self { source })
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ValidatedDeleteSource {
    pub(crate) source: ValidatedSourceReference,
}

#[cfg_attr(not(test), allow(dead_code))]
impl TryFrom<DeleteSource> for ValidatedDeleteSource {
    type Error = ContractError;

    fn try_from(input: DeleteSource) -> Result<Self, Self::Error> {
        Ok(Self {
            source: ValidatedSourceReference::try_from(input.source)?,
        })
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ValidatedConceptMention {
    pub(crate) concept_id: ConceptId,
    pub(crate) source: ValidatedSourceReference,
}

#[cfg_attr(not(test), allow(dead_code))]
impl TryFrom<ConceptMentionInput> for ValidatedConceptMention {
    type Error = ContractError;

    fn try_from(input: ConceptMentionInput) -> Result<Self, Self::Error> {
        Ok(Self {
            concept_id: input.concept_id,
            source: ValidatedSourceReference::try_from(input.source)?,
        })
    }
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub(crate) struct SemanticAssertionIdentity {
    pub(crate) subject_concept_id: ConceptId,
    pub(crate) relation_type: RelationType,
    pub(crate) object_concept_id: ConceptId,
}

#[cfg_attr(not(test), allow(dead_code))]
impl SemanticAssertionIdentity {
    pub(crate) fn try_new(
        subject_concept_id: ConceptId,
        relation_type: RelationType,
        object_concept_id: ConceptId,
    ) -> Result<Self, ContractError> {
        if subject_concept_id == object_concept_id {
            return Err(ContractError::invalid(
                FieldCode::SubjectConceptId,
                ValidationReason::SelfRelation,
            ));
        }

        let (subject_concept_id, object_concept_id) = if relation_type.is_symmetric()
            && subject_concept_id.as_uuid() > object_concept_id.as_uuid()
        {
            (object_concept_id, subject_concept_id)
        } else {
            (subject_concept_id, object_concept_id)
        };

        Ok(Self {
            subject_concept_id,
            relation_type,
            object_concept_id,
        })
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ValidatedEvidenceSources {
    pub(crate) sources: Vec<ValidatedSourceReference>,
}

#[cfg_attr(not(test), allow(dead_code))]
impl TryFrom<Vec<SourceReferenceInput>> for ValidatedEvidenceSources {
    type Error = ContractError;

    fn try_from(inputs: Vec<SourceReferenceInput>) -> Result<Self, Self::Error> {
        if inputs.is_empty() {
            return Err(ContractError::invalid(
                FieldCode::SupportingSources,
                ValidationReason::Empty,
            ));
        }
        if inputs.len() > MAX_ASSERTION_EVIDENCE {
            return Err(ContractError::invalid(
                FieldCode::SupportingSources,
                ValidationReason::TooLong,
            ));
        }

        let mut sources: Vec<ValidatedSourceReference> = Vec::with_capacity(inputs.len());
        for input in inputs {
            let source = ValidatedSourceReference::try_from(input)?;
            if !sources
                .iter()
                .any(|existing| existing.source == source.source)
            {
                sources.push(source);
            }
        }

        Ok(Self { sources })
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ValidatedCreateAssertion {
    pub(crate) candidate_id: AssertionId,
    pub(crate) identity: SemanticAssertionIdentity,
    pub(crate) supporting_sources: ValidatedEvidenceSources,
}

#[cfg_attr(not(test), allow(dead_code))]
impl ValidatedCreateAssertion {
    pub(crate) fn try_from_input(
        candidate_id: AssertionId,
        input: CreateAssertion,
    ) -> Result<Self, ContractError> {
        let identity = SemanticAssertionIdentity::try_new(
            input.subject_concept_id,
            input.relation_type,
            input.object_concept_id,
        )?;
        let supporting_sources = ValidatedEvidenceSources::try_from(input.supporting_sources)?;

        Ok(Self {
            candidate_id,
            identity,
            supporting_sources,
        })
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ValidatedUpdateAssertion {
    pub(crate) assertion_id: AssertionId,
    pub(crate) expected_revision: Revision,
    pub(crate) identity: SemanticAssertionIdentity,
}

#[cfg_attr(not(test), allow(dead_code))]
impl ValidatedUpdateAssertion {
    pub(crate) fn try_from_input(input: UpdateAssertion) -> Result<Self, ContractError> {
        Ok(Self {
            assertion_id: input.assertion_id,
            expected_revision: input.expected_revision,
            identity: SemanticAssertionIdentity::try_new(
                input.subject_concept_id,
                input.relation_type,
                input.object_concept_id,
            )?,
        })
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ValidatedDeleteAssertion {
    pub(crate) assertion_id: AssertionId,
    pub(crate) expected_revision: Revision,
}

impl From<DeleteAssertion> for ValidatedDeleteAssertion {
    fn from(input: DeleteAssertion) -> Self {
        Self {
            assertion_id: input.assertion_id,
            expected_revision: input.expected_revision,
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ValidatedAssertionEvidence {
    pub(crate) assertion_id: AssertionId,
    pub(crate) source: ValidatedSourceReference,
}

#[cfg_attr(not(test), allow(dead_code))]
impl TryFrom<AssertionEvidenceInput> for ValidatedAssertionEvidence {
    type Error = ContractError;

    fn try_from(input: AssertionEvidenceInput) -> Result<Self, Self::Error> {
        Ok(Self {
            assertion_id: input.assertion_id,
            source: ValidatedSourceReference::try_from(input.source)?,
        })
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq, Deserialize, Serialize)]
#[serde(
    tag = "kind",
    content = "id",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum RecordIdentity {
    Concept(ConceptId),
    Assertion(AssertionId),
}

impl RecordIdentity {
    pub const fn kind(self) -> RecordKind {
        match self {
            Self::Concept(_) => RecordKind::Concept,
            Self::Assertion(_) => RecordKind::Assertion,
        }
    }
}

impl fmt::Debug for RecordIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Concept(_) => formatter.write_str("RecordIdentity::Concept(<redacted>)"),
            Self::Assertion(_) => formatter.write_str("RecordIdentity::Assertion(<redacted>)"),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    content = "id",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ReferencedRecord {
    Concept(ConceptId),
    SourceReference,
}

impl ReferencedRecord {
    pub const fn kind(self) -> RecordKind {
        match self {
            Self::Concept(_) => RecordKind::Concept,
            Self::SourceReference => RecordKind::SourceReference,
        }
    }
}

impl fmt::Debug for ReferencedRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Concept(_) => formatter.write_str("ReferencedRecord::Concept(<redacted>)"),
            Self::SourceReference => formatter.write_str("ReferencedRecord::SourceReference"),
        }
    }
}

/// The single atomic classification returned by one database mutation routine.
/// Success variants carry one complete canonical record; failure variants carry
/// no partial success record.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum DatabaseMutationOutcome<T> {
    Created {
        record: T,
    },
    Existing {
        record: T,
    },
    Updated {
        record: T,
    },
    Deleted,
    Conflict {
        field: ConflictField,
        reason: ConflictReason,
        identity: RecordIdentity,
        current_revision: Revision,
    },
    Stale {
        identity: RecordIdentity,
        current_revision: Revision,
    },
    Referenced {
        reference: ReferencedRecord,
    },
    Missing {
        record_kind: RecordKind,
        identity: Option<RecordIdentity>,
    },
    EvidenceLimit,
    WouldOrphanEvidence {
        assertion_id: AssertionId,
        revision: Revision,
    },
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum GraphError {
    InvalidInput {
        operation: OperationCode,
        field: FieldCode,
        reason: ValidationReason,
    },
    NotFound {
        operation: OperationCode,
        record_kind: RecordKind,
        identity: Option<RecordIdentity>,
        reason: NotFoundReason,
    },
    Conflict {
        operation: OperationCode,
        field: ConflictField,
        reason: ConflictReason,
        identity: RecordIdentity,
        current_revision: Option<Revision>,
    },
    Referenced {
        operation: OperationCode,
        record: ReferencedRecord,
        reason: ReferenceReason,
    },
    WouldOrphanAssertion {
        operation: OperationCode,
        assertion_id: AssertionId,
        revision: Revision,
    },
    LimitExceeded {
        operation: OperationCode,
        resource: LimitResource,
        reason: LimitReason,
    },
    TraversalBoundExceeded {
        operation: OperationCode,
        bound: TraversalBound,
        reason: TraversalReason,
    },
    ResultBoundExceeded {
        operation: OperationCode,
    },
    DatabaseFailure {
        operation: OperationCode,
        reason: BackendFailureReason,
    },
    InvalidResponse {
        operation: OperationCode,
        reason: BackendFailureReason,
    },
}

impl GraphError {
    pub const fn invalid_input(operation: OperationCode, error: ContractError) -> Self {
        Self::InvalidInput {
            operation,
            field: error.field,
            reason: error.reason,
        }
    }
}

impl fmt::Display for GraphError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput {
                operation,
                field,
                reason,
            } => write!(formatter, "invalid_input:{operation}:{field}:{reason}"),
            Self::NotFound {
                operation,
                record_kind,
                reason,
                ..
            } => write!(formatter, "not_found:{operation}:{}:{reason}", record_kind),
            Self::Conflict {
                operation,
                field,
                reason,
                ..
            } => write!(formatter, "conflict:{operation}:{field}:{reason}"),
            Self::Referenced {
                operation,
                record,
                reason,
            } => write!(
                formatter,
                "referenced:{operation}:{}:{reason}",
                record.kind()
            ),
            Self::WouldOrphanAssertion { operation, .. } => {
                write!(formatter, "would_orphan_assertion:{operation}")
            }
            Self::LimitExceeded {
                operation,
                resource,
                reason,
            } => write!(formatter, "limit_exceeded:{operation}:{resource}:{reason}"),
            Self::TraversalBoundExceeded {
                operation,
                bound,
                reason,
            } => write!(
                formatter,
                "traversal_bound_exceeded:{operation}:{bound}:{reason}"
            ),
            Self::ResultBoundExceeded { operation } => {
                write!(formatter, "result_bound_exceeded:{operation}")
            }
            Self::DatabaseFailure { operation, reason } => {
                write!(formatter, "database_failure:{operation}:{reason}")
            }
            Self::InvalidResponse { operation, reason } => {
                write!(formatter, "invalid_response:{operation}:{reason}")
            }
        }
    }
}

impl fmt::Debug for GraphError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GraphError")
            .field("code", &self.to_string())
            .finish()
    }
}

impl std::error::Error for GraphError {}

pub type GraphResult<T> = Result<T, GraphError>;

pub(crate) fn validate_alias_display(value: &str) -> Result<(), ContractError> {
    validate_bounded_text(value, FieldCode::Alias, MAX_ALIAS_DISPLAY_BYTES)
}

pub(crate) fn validate_bounded_text(
    value: &str,
    field: FieldCode,
    maximum_bytes: usize,
) -> Result<(), ContractError> {
    if value.is_empty() {
        return Err(ContractError::invalid(field, ValidationReason::Empty));
    }
    if value.contains('\0') {
        return Err(ContractError::invalid(field, ValidationReason::ContainsNul));
    }
    if value.len() > maximum_bytes {
        return Err(ContractError::invalid(field, ValidationReason::TooLong));
    }

    Ok(())
}

fn validate_result_limit(value: u16, maximum: u16) -> Result<(), ContractError> {
    if value == 0 {
        return Err(ContractError::invalid(
            FieldCode::Limit,
            ValidationReason::NotPositive,
        ));
    }
    if value > maximum {
        return Err(ContractError::invalid(
            FieldCode::Limit,
            ValidationReason::TooLong,
        ));
    }

    Ok(())
}

fn validate_source_key(value: &str, field: FieldCode) -> Result<(), ContractError> {
    if value.is_empty() {
        return Err(ContractError::invalid(field, ValidationReason::Empty));
    }
    if value.contains('\0') {
        return Err(ContractError::invalid(field, ValidationReason::ContainsNul));
    }
    if value.len() > MAX_SOURCE_KEY_BYTES {
        return Err(ContractError::invalid(
            FieldCode::SourceKey,
            ValidationReason::TooLong,
        ));
    }

    Ok(())
}

fn parse_uuid(value: &str, field: FieldCode) -> Result<Uuid, ContractError> {
    Uuid::parse_str(value).map_err(|_| ContractError::invalid(field, ValidationReason::InvalidUuid))
}

fn validate_uuid_v7(value: Uuid, field: FieldCode) -> Result<Uuid, ContractError> {
    if value.is_nil() {
        return Err(ContractError::invalid(field, ValidationReason::NilUuid));
    }
    if value.get_version_num() != 7 {
        return Err(ContractError::invalid(field, ValidationReason::NotUuidV7));
    }
    if (value.as_bytes()[8] & 0b1100_0000) != 0b1000_0000 {
        return Err(ContractError::invalid(field, ValidationReason::NotUuidV7));
    }

    Ok(value)
}

#[cfg(test)]
mod mutation_contract_tests {
    use serde::de::DeserializeOwned;

    use super::*;

    fn valid_concept_id() -> ConceptId {
        ConceptId::try_from(Uuid::now_v7()).expect("UUIDv7 concept ID")
    }

    fn valid_assertion_id() -> AssertionId {
        AssertionId::try_from(Uuid::now_v7()).expect("UUIDv7 assertion ID")
    }

    fn round_trip<T>(value: T) -> T
    where
        T: DeserializeOwned + fmt::Debug + PartialEq + Serialize,
    {
        let encoded = serde_json::to_vec(&value).expect("outcome serializes");
        let decoded = serde_json::from_slice(&encoded).expect("outcome deserializes");
        assert_eq!(decoded, value);
        decoded
    }

    fn assert_record_outcome_round_trip<T>(expected_code: &str, outcome: DatabaseMutationOutcome<T>)
    where
        T: Clone + DeserializeOwned + fmt::Debug + PartialEq + Serialize,
    {
        let encoded = serde_json::to_value(&outcome).expect("outcome serializes");
        assert_eq!(encoded["outcome"], expected_code);
        assert_eq!(round_trip(outcome.clone()), outcome);
    }

    #[test]
    fn normalized_alias_set_identity_ignores_order_but_includes_preferred_key() {
        let alpha = AliasKey::from_normalized("alpha".to_owned());
        let beta = AliasKey::from_normalized("beta".to_owned());
        let first = AliasSetIdentity::new([beta.clone(), alpha.clone()], alpha.clone());
        let same_keys_different_order =
            AliasSetIdentity::new([alpha.clone(), beta.clone()], alpha.clone());
        let different_preferred_key =
            AliasSetIdentity::new([alpha.clone(), beta.clone()], beta.clone());

        assert!(first == same_keys_different_order);
        assert!(first != different_preferred_key);
        assert_eq!(
            first
                .normalized_keys()
                .map(AliasKey::as_str)
                .collect::<Vec<_>>(),
            ["alpha", "beta"]
        );
        assert_eq!(first.preferred_key().as_str(), "alpha");
    }

    #[test]
    fn semantic_assertion_identity_canonicalizes_symmetric_endpoints_only() {
        let one = valid_concept_id();
        let two = valid_concept_id();
        let (low, high) = if one.as_uuid() < two.as_uuid() {
            (one, two)
        } else {
            (two, one)
        };

        let symmetric_forward =
            SemanticAssertionIdentity::try_new(low, RelationType::RelatedTo, high)
                .expect("distinct symmetric endpoints");
        let symmetric_reverse =
            SemanticAssertionIdentity::try_new(high, RelationType::RelatedTo, low)
                .expect("distinct symmetric endpoints");
        assert!(symmetric_forward == symmetric_reverse);
        assert_eq!(symmetric_forward.subject_concept_id, low);
        assert_eq!(symmetric_forward.object_concept_id, high);

        let directed_forward = SemanticAssertionIdentity::try_new(low, RelationType::IsA, high)
            .expect("distinct directed endpoints");
        let directed_reverse = SemanticAssertionIdentity::try_new(high, RelationType::IsA, low)
            .expect("distinct directed endpoints");
        assert!(directed_forward != directed_reverse);

        let error = SemanticAssertionIdentity::try_new(low, RelationType::RelatedTo, low)
            .err()
            .expect("self-relations are invalid");
        assert_eq!(error.field(), FieldCode::SubjectConceptId);
        assert_eq!(error.reason(), ValidationReason::SelfRelation);
    }

    #[test]
    fn validated_evidence_sources_enforce_the_declared_count_and_source_shapes() {
        assert_eq!(MAX_ASSERTION_EVIDENCE, 32);

        let exact_bound = (0..MAX_ASSERTION_EVIDENCE)
            .map(|index| SourceReferenceInput::MemoryVersion {
                memory_id: format!("memory-{index}"),
                version: 1,
            })
            .collect::<Vec<_>>();
        let validated = ValidatedEvidenceSources::try_from(exact_bound)
            .expect("32 supporting sources are accepted");
        assert_eq!(validated.sources.len(), MAX_ASSERTION_EVIDENCE);

        let over_bound = (0..=MAX_ASSERTION_EVIDENCE)
            .map(|index| SourceReferenceInput::MemoryVersion {
                memory_id: format!("memory-{index}"),
                version: 1,
            })
            .collect::<Vec<_>>();
        let error = ValidatedEvidenceSources::try_from(over_bound)
            .err()
            .expect("33 supporting sources are rejected");
        assert_eq!(error.field(), FieldCode::SupportingSources);
        assert_eq!(error.reason(), ValidationReason::TooLong);

        let error = ValidatedEvidenceSources::try_from(Vec::new())
            .err()
            .expect("an assertion requires supporting evidence");
        assert_eq!(error.field(), FieldCode::SupportingSources);
        assert_eq!(error.reason(), ValidationReason::Empty);
    }

    #[test]
    fn caller_mutation_inputs_convert_to_separate_private_port_contracts() {
        let concept_id = valid_concept_id();
        let other_concept_id = valid_concept_id();
        let assertion_id = valid_assertion_id();
        let revision = Revision::try_from(5).expect("positive revision");
        let alias_key = AliasKey::from_normalized("normalized alias".to_owned());
        let aliases = ValidatedAliasSet {
            aliases: vec![ValidatedAlias {
                display_text: "display alias".to_owned(),
                normalized_key: alias_key.clone(),
                preferred: true,
            }],
            identity: AliasSetIdentity::new([alias_key.clone()], alias_key.clone()),
        };

        let validated_create = ValidatedCreateConcept::new(concept_id, aliases.clone());
        assert_eq!(validated_create.candidate_id, concept_id);
        assert_eq!(
            validated_create.aliases.aliases[0].display_text,
            "display alias"
        );
        assert!(validated_create.aliases.aliases[0].preferred);
        assert_eq!(
            validated_create.aliases.aliases[0].normalized_key.as_str(),
            "normalized alias"
        );

        let validated_replace = ValidatedReplaceConceptAliases::new(concept_id, revision, aliases);
        assert_eq!(validated_replace.concept_id, concept_id);
        assert_eq!(validated_replace.expected_revision, revision);
        assert_eq!(
            validated_replace.aliases.identity.preferred_key().as_str(),
            "normalized alias"
        );

        let validated_delete = ValidatedDeleteConcept::from(DeleteConcept {
            concept_id,
            expected_revision: revision,
        });
        assert_eq!(validated_delete.concept_id, concept_id);
        assert_eq!(validated_delete.expected_revision, revision);

        let source_input = SourceReferenceInput::MemoryVersion {
            memory_id: "opaque-memory-key".to_owned(),
            version: 2,
        };
        let validated_source =
            ValidatedSourceReference::try_from(source_input.clone()).expect("valid source");
        assert_eq!(validated_source.source.kind(), SourceKind::MemoryVersion);
        let validated_source_delete = ValidatedDeleteSource::try_from(DeleteSource {
            source: source_input.clone(),
        })
        .expect("valid source delete input");
        assert_eq!(
            validated_source_delete.source.source,
            validated_source.source
        );
        let validated_mention = ValidatedConceptMention::try_from(ConceptMentionInput {
            concept_id,
            source: source_input.clone(),
        })
        .expect("valid mention input");
        assert_eq!(validated_mention.concept_id, concept_id);
        assert_eq!(validated_mention.source.source, validated_source.source);

        let create_input = CreateAssertion {
            subject_concept_id: concept_id,
            relation_type: RelationType::RelatedTo,
            object_concept_id: other_concept_id,
            supporting_sources: vec![source_input.clone()],
        };
        let validated_create_assertion =
            ValidatedCreateAssertion::try_from_input(assertion_id, create_input)
                .expect("valid create assertion input");
        assert_eq!(validated_create_assertion.candidate_id, assertion_id);
        assert!(
            validated_create_assertion.identity
                == SemanticAssertionIdentity::try_new(
                    concept_id,
                    RelationType::RelatedTo,
                    other_concept_id
                )
                .expect("valid semantic identity")
        );
        assert_eq!(
            validated_create_assertion.supporting_sources.sources.len(),
            1
        );

        let validated_update = ValidatedUpdateAssertion::try_from_input(UpdateAssertion {
            assertion_id,
            expected_revision: revision,
            subject_concept_id: other_concept_id,
            relation_type: RelationType::IsA,
            object_concept_id: concept_id,
        })
        .expect("valid update assertion input");
        assert_eq!(validated_update.assertion_id, assertion_id);
        assert_eq!(validated_update.expected_revision, revision);
        assert_eq!(validated_update.identity.relation_type, RelationType::IsA);

        let validated_assertion_delete = ValidatedDeleteAssertion::from(DeleteAssertion {
            assertion_id,
            expected_revision: revision,
        });
        assert_eq!(validated_assertion_delete.assertion_id, assertion_id);
        assert_eq!(validated_assertion_delete.expected_revision, revision);

        let validated_evidence = ValidatedAssertionEvidence::try_from(AssertionEvidenceInput {
            assertion_id,
            source: source_input,
        })
        .expect("valid evidence input");
        assert_eq!(validated_evidence.assertion_id, assertion_id);
        assert_eq!(validated_evidence.source.source, validated_source.source);
    }

    #[test]
    fn database_mutation_outcomes_round_trip_every_atomic_classification() {
        let concept_id = valid_concept_id();
        let assertion_id = valid_assertion_id();
        let revision = Revision::try_from(4).expect("positive revision");
        let concept = Concept {
            id: concept_id,
            revision,
            aliases: vec![Alias {
                display_text: "protected-alias-sentinel".to_owned(),
                preferred: true,
            }],
        };
        let concept_identity = RecordIdentity::Concept(concept_id);
        let assertion_identity = RecordIdentity::Assertion(assertion_id);
        let outcomes: [(&str, bool, DatabaseMutationOutcome<Concept>); 10] = [
            (
                "created",
                true,
                DatabaseMutationOutcome::Created {
                    record: concept.clone(),
                },
            ),
            (
                "existing",
                true,
                DatabaseMutationOutcome::Existing {
                    record: concept.clone(),
                },
            ),
            (
                "updated",
                true,
                DatabaseMutationOutcome::Updated {
                    record: concept.clone(),
                },
            ),
            ("deleted", true, DatabaseMutationOutcome::Deleted),
            (
                "conflict",
                false,
                DatabaseMutationOutcome::Conflict {
                    field: ConflictField::AliasSet,
                    reason: ConflictReason::AliasSetMismatch,
                    identity: concept_identity,
                    current_revision: revision,
                },
            ),
            (
                "stale",
                false,
                DatabaseMutationOutcome::Stale {
                    identity: assertion_identity,
                    current_revision: revision,
                },
            ),
            (
                "referenced",
                false,
                DatabaseMutationOutcome::Referenced {
                    reference: ReferencedRecord::SourceReference,
                },
            ),
            (
                "missing",
                false,
                DatabaseMutationOutcome::Missing {
                    record_kind: RecordKind::SourceReference,
                    identity: None,
                },
            ),
            (
                "evidence_limit",
                false,
                DatabaseMutationOutcome::EvidenceLimit,
            ),
            (
                "would_orphan_evidence",
                false,
                DatabaseMutationOutcome::WouldOrphanEvidence {
                    assertion_id,
                    revision,
                },
            ),
        ];

        for (code, is_success, outcome) in outcomes {
            let encoded = serde_json::to_value(&outcome).expect("outcome serializes");
            assert_eq!(encoded["outcome"], code);
            assert_eq!(
                encoded.get("record").is_some(),
                is_success && code != "deleted"
            );
            let decoded = round_trip(outcome.clone());
            assert_eq!(decoded, outcome);
            assert_debug_is_opaque(&outcome, &["protected-alias-sentinel"]);
        }
    }

    #[test]
    fn database_mutation_outcomes_round_trip_each_success_record_type() {
        let concept_id = valid_concept_id();
        let other_concept_id = valid_concept_id();
        let assertion_id = valid_assertion_id();
        let revision = Revision::try_from(2).expect("positive revision");
        let memory_source = SourceReference::try_from(SourceReferenceInput::MemoryVersion {
            memory_id: "memory-key".to_owned(),
            version: 1,
        })
        .expect("valid memory source");
        let session_source = SourceReference::try_from(SourceReferenceInput::SessionRecord {
            session_record_id: "session-key".to_owned(),
        })
        .expect("valid session source");
        let concept = Concept {
            id: concept_id,
            revision,
            aliases: vec![Alias {
                display_text: "alias".to_owned(),
                preferred: true,
            }],
        };
        let assertion = Assertion {
            id: assertion_id,
            revision,
            subject_concept_id: concept_id,
            relation_type: RelationType::DependsOn,
            object_concept_id: other_concept_id,
            evidence: vec![memory_source.clone()],
        };

        assert_record_outcome_round_trip(
            "created",
            DatabaseMutationOutcome::Created {
                record: concept.clone(),
            },
        );
        assert_record_outcome_round_trip(
            "existing",
            DatabaseMutationOutcome::Existing { record: concept },
        );
        assert_record_outcome_round_trip(
            "created",
            DatabaseMutationOutcome::Created {
                record: memory_source.clone(),
            },
        );
        assert_record_outcome_round_trip(
            "existing",
            DatabaseMutationOutcome::Existing {
                record: session_source.clone(),
            },
        );
        assert_record_outcome_round_trip(
            "created",
            DatabaseMutationOutcome::Created {
                record: ConceptMention {
                    concept_id,
                    source: memory_source.clone(),
                },
            },
        );
        assert_record_outcome_round_trip(
            "existing",
            DatabaseMutationOutcome::Existing {
                record: ConceptMention {
                    concept_id,
                    source: memory_source.clone(),
                },
            },
        );
        assert_record_outcome_round_trip(
            "created",
            DatabaseMutationOutcome::Created {
                record: assertion.clone(),
            },
        );
        assert_record_outcome_round_trip(
            "updated",
            DatabaseMutationOutcome::Updated { record: assertion },
        );
        assert_record_outcome_round_trip(
            "created",
            DatabaseMutationOutcome::Created {
                record: AssertionEvidence {
                    assertion_id,
                    source: session_source.clone(),
                },
            },
        );
        assert_record_outcome_round_trip(
            "existing",
            DatabaseMutationOutcome::Existing {
                record: AssertionEvidence {
                    assertion_id,
                    source: session_source,
                },
            },
        );
        assert_record_outcome_round_trip("deleted", DatabaseMutationOutcome::<()>::Deleted);
    }

    fn assert_debug_is_opaque<T: fmt::Debug>(value: &T, protected: &[&str]) {
        let debug = format!("{value:?}");
        for sentinel in protected {
            assert!(!debug.contains(sentinel), "Debug leaked {sentinel:?}");
        }
    }
}

#[cfg(test)]
mod read_contract_tests {
    use std::{cmp::Ordering, time::Duration};

    use serde::de::DeserializeOwned;

    use super::*;

    fn uuid_v7(value: u128) -> Uuid {
        let mut bytes = value.to_be_bytes();
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    fn concept_id(value: u128) -> ConceptId {
        ConceptId::try_from(uuid_v7(value)).expect("valid UUIDv7 concept ID")
    }

    fn assertion_id(value: u128) -> AssertionId {
        AssertionId::try_from(uuid_v7(value)).expect("valid UUIDv7 assertion ID")
    }

    fn memory_source(key: &str, version: i64) -> SourceReference {
        SourceReference::try_from(SourceReferenceInput::MemoryVersion {
            memory_id: key.to_owned(),
            version,
        })
        .expect("valid memory source")
    }

    fn session_source(key: &str) -> SourceReference {
        SourceReference::try_from(SourceReferenceInput::SessionRecord {
            session_record_id: key.to_owned(),
        })
        .expect("valid session source")
    }

    fn concept(id: ConceptId, alias: &str) -> Concept {
        Concept {
            id,
            revision: Revision::try_from(1).expect("positive revision"),
            aliases: vec![Alias {
                display_text: alias.to_owned(),
                preferred: true,
            }],
        }
    }

    fn assertion(
        id: AssertionId,
        subject_concept_id: ConceptId,
        relation_type: RelationType,
        object_concept_id: ConceptId,
        evidence: Vec<SourceReference>,
    ) -> Assertion {
        Assertion {
            id,
            revision: Revision::try_from(1).expect("positive revision"),
            subject_concept_id,
            relation_type,
            object_concept_id,
            evidence,
        }
    }

    fn deserialize_round_trip<T>(value: T) -> Result<T, serde_json::Error>
    where
        T: DeserializeOwned + Serialize,
    {
        let encoded = serde_json::to_vec(&value)?;
        serde_json::from_slice(&encoded)
    }

    fn assert_contract_error<T>(
        result: Result<T, ContractError>,
        field: FieldCode,
        reason: ValidationReason,
    ) {
        let error = match result {
            Ok(_) => panic!("invalid read input or record must be rejected"),
            Err(error) => error,
        };
        assert_eq!(error.field(), field);
        assert_eq!(error.reason(), reason);
    }

    #[test]
    fn direct_lookup_inputs_validate_record_ids_and_typed_sources() {
        let expected_concept = concept_id(1);
        let expected_assertion = assertion_id(2);
        let validated_concept = ValidatedConceptIdInput::try_from(ConceptIdInput {
            id: expected_concept.as_uuid().to_string(),
        })
        .expect("valid concept lookup ID");
        let validated_assertion = ValidatedAssertionIdInput::try_from(AssertionIdInput {
            id: expected_assertion.as_uuid().to_string(),
        })
        .expect("valid assertion lookup ID");
        assert_eq!(validated_concept.id, expected_concept);
        assert_eq!(validated_assertion.id, expected_assertion);

        assert_contract_error(
            ValidatedConceptIdInput::try_from(ConceptIdInput {
                id: "invalid-concept-id-sentinel".to_owned(),
            }),
            FieldCode::ConceptId,
            ValidationReason::InvalidUuid,
        );
        assert_contract_error(
            ValidatedAssertionIdInput::try_from(AssertionIdInput {
                id: Uuid::new_v4().to_string(),
            }),
            FieldCode::AssertionId,
            ValidationReason::NotUuidV7,
        );

        for input in [
            SourceReferenceInput::MemoryVersion {
                memory_id: "opaque-memory-id".to_owned(),
                version: 1,
            },
            SourceReferenceInput::SessionRecord {
                session_record_id: "opaque-session-id".to_owned(),
            },
        ] {
            ValidatedSourceReference::try_from(input)
                .expect("both typed source identities can be looked up");
        }
        assert_contract_error(
            ValidatedSourceReference::try_from(SourceReferenceInput::MemoryVersion {
                memory_id: String::new(),
                version: 1,
            }),
            FieldCode::MemoryId,
            ValidationReason::Empty,
        );
    }

    #[test]
    fn alias_lookup_validates_display_and_normalized_key_bounds() {
        assert_eq!(MAX_ALIAS_DISPLAY_BYTES, 512);
        assert_eq!(MAX_ALIAS_KEY_BYTES, 2_048);

        let display = "é".repeat(MAX_ALIAS_DISPLAY_BYTES / "é".len());
        let query = AliasQuery::try_from(display.clone()).expect("512 UTF-8 bytes are accepted");
        assert_eq!(query.alias, display);
        assert_contract_error(
            AliasQuery::try_from(String::new()),
            FieldCode::Alias,
            ValidationReason::Empty,
        );
        assert_contract_error(
            AliasQuery::try_from("alias\0sentinel".to_owned()),
            FieldCode::Alias,
            ValidationReason::ContainsNul,
        );
        assert_contract_error(
            AliasQuery::try_from(format!("{display}x")),
            FieldCode::Alias,
            ValidationReason::TooLong,
        );

        let query = AliasQuery::try_from("unchanged display spelling".to_owned())
            .expect("valid caller spelling");
        let normalized = ValidatedAliasQuery::try_from_normalized(query, "n".repeat(2_048))
            .expect("a bounded normalized key is accepted");
        assert_eq!(normalized.alias_key.as_str().len(), MAX_ALIAS_KEY_BYTES);

        let query = AliasQuery::try_from("unchanged display spelling".to_owned())
            .expect("valid caller spelling");
        assert_contract_error(
            ValidatedAliasQuery::try_from_normalized(query, String::new()),
            FieldCode::AliasKey,
            ValidationReason::Empty,
        );
        let query = AliasQuery::try_from("unchanged display spelling".to_owned())
            .expect("valid caller spelling");
        assert_contract_error(
            ValidatedAliasQuery::try_from_normalized(query, "normalized\0key".to_owned()),
            FieldCode::AliasKey,
            ValidationReason::ContainsNul,
        );
        let query = AliasQuery::try_from("unchanged display spelling".to_owned())
            .expect("valid caller spelling");
        assert_contract_error(
            ValidatedAliasQuery::try_from_normalized(query, "n".repeat(2_049)),
            FieldCode::AliasKey,
            ValidationReason::TooLong,
        );
    }

    #[test]
    fn neighbor_and_related_source_inputs_enforce_independent_result_bounds() {
        let id = concept_id(10);
        let neighbor = ValidatedNeighborQuery::try_from(NeighborQuery {
            concept_id: id,
            direction: DirectionMode::Either,
            relation_filter: vec![RelationType::Uses, RelationType::IsA, RelationType::Uses],
            limit: 1,
        })
        .expect("minimum neighbor limit is accepted");
        assert_eq!(neighbor.concept_id, id);
        assert_eq!(neighbor.direction, DirectionMode::Either);
        assert_eq!(neighbor.limit, 1);
        assert_eq!(
            neighbor.relation_filter,
            [RelationType::IsA, RelationType::Uses]
        );
        let all_relations = ValidatedNeighborQuery::try_from(NeighborQuery {
            concept_id: id,
            direction: DirectionMode::Either,
            relation_filter: Vec::new(),
            limit: 1,
        })
        .expect("an empty relation filter selects all relation types");
        assert!(all_relations.relation_filter.is_empty());

        let max_neighbor = ValidatedNeighborQuery::try_from(NeighborQuery {
            concept_id: id,
            direction: DirectionMode::Outgoing,
            relation_filter: RelationType::ALL.to_vec(),
            limit: MAX_NEIGHBOR_RESULTS,
        })
        .expect("maximum neighbor limit is accepted");
        assert_eq!(max_neighbor.limit, MAX_NEIGHBOR_RESULTS);
        assert_contract_error(
            ValidatedNeighborQuery::try_from(NeighborQuery {
                concept_id: id,
                direction: DirectionMode::Either,
                relation_filter: Vec::new(),
                limit: 0,
            }),
            FieldCode::Limit,
            ValidationReason::NotPositive,
        );
        assert_contract_error(
            ValidatedNeighborQuery::try_from(NeighborQuery {
                concept_id: id,
                direction: DirectionMode::Either,
                relation_filter: Vec::new(),
                limit: MAX_NEIGHBOR_RESULTS + 1,
            }),
            FieldCode::Limit,
            ValidationReason::TooLong,
        );

        let min_related = ValidatedRelatedSourceQuery::try_from(RelatedSourceQuery {
            concept_id: id,
            limit: 1,
        })
        .expect("minimum related-source limit is accepted");
        assert_eq!(min_related.concept_id, id);
        assert_eq!(min_related.limit, 1);
        let max_related = ValidatedRelatedSourceQuery::try_from(RelatedSourceQuery {
            concept_id: id,
            limit: MAX_RELATED_SOURCE_RESULTS,
        })
        .expect("maximum related-source limit is accepted");
        assert_eq!(max_related.limit, MAX_RELATED_SOURCE_RESULTS);
        assert_contract_error(
            ValidatedRelatedSourceQuery::try_from(RelatedSourceQuery {
                concept_id: id,
                limit: 0,
            }),
            FieldCode::Limit,
            ValidationReason::NotPositive,
        );
        assert_contract_error(
            ValidatedRelatedSourceQuery::try_from(RelatedSourceQuery {
                concept_id: id,
                limit: MAX_RELATED_SOURCE_RESULTS + 1,
            }),
            FieldCode::Limit,
            ValidationReason::TooLong,
        );
    }

    #[test]
    fn bounded_path_inputs_validate_endpoints_direction_depth_work_and_results() {
        let from = concept_id(20);
        let to = concept_id(21);
        let valid = PathQuery {
            from,
            to,
            direction: DirectionMode::Either,
            max_depth: MAX_PATH_DEPTH,
            max_work: MAX_PATH_WORK,
            limit: MAX_PATH_RESULTS,
        };
        let validated =
            ValidatedPathQuery::try_from(valid.clone()).expect("maximum path bounds are accepted");
        assert_eq!(validated.from, from);
        assert_eq!(validated.to, to);
        assert_eq!(validated.direction, DirectionMode::Either);
        assert_eq!(validated.max_depth, MAX_PATH_DEPTH);
        assert_eq!(validated.max_work, MAX_PATH_WORK);
        assert_eq!(validated.limit, MAX_PATH_RESULTS);

        let min = ValidatedPathQuery::try_from(PathQuery {
            max_depth: 1,
            max_work: 1,
            limit: 1,
            ..valid.clone()
        })
        .expect("minimum path bounds are accepted");
        assert_eq!((min.max_depth, min.max_work, min.limit), (1, 1, 1));

        assert_contract_error(
            ValidatedPathQuery::try_from(PathQuery {
                from,
                to: from,
                ..valid.clone()
            }),
            FieldCode::ConceptId,
            ValidationReason::SelfRelation,
        );
        assert_contract_error(
            ValidatedPathQuery::try_from(PathQuery {
                max_depth: 0,
                ..valid.clone()
            }),
            FieldCode::MaxDepth,
            ValidationReason::NotPositive,
        );
        assert_contract_error(
            ValidatedPathQuery::try_from(PathQuery {
                max_depth: MAX_PATH_DEPTH + 1,
                ..valid.clone()
            }),
            FieldCode::MaxDepth,
            ValidationReason::TooLong,
        );
        assert_contract_error(
            ValidatedPathQuery::try_from(PathQuery {
                max_work: 0,
                ..valid.clone()
            }),
            FieldCode::MaxWork,
            ValidationReason::NotPositive,
        );
        assert_contract_error(
            ValidatedPathQuery::try_from(PathQuery {
                max_work: MAX_PATH_WORK + 1,
                ..valid.clone()
            }),
            FieldCode::MaxWork,
            ValidationReason::TooLong,
        );
        assert_contract_error(
            ValidatedPathQuery::try_from(PathQuery {
                limit: 0,
                ..valid.clone()
            }),
            FieldCode::Limit,
            ValidationReason::NotPositive,
        );
        assert_contract_error(
            ValidatedPathQuery::try_from(PathQuery {
                limit: MAX_PATH_RESULTS + 1,
                ..valid
            }),
            FieldCode::Limit,
            ValidationReason::TooLong,
        );
    }

    #[test]
    fn database_timeouts_enforce_query_and_invocation_bounds() {
        let defaults = DatabaseTimeouts::default();
        assert_eq!(defaults.query(), Duration::from_secs(10));
        assert_eq!(defaults.invocation(), Duration::from_secs(15));

        let lower = DatabaseTimeouts::try_new(Duration::from_secs(1), Duration::from_secs(2))
            .expect("lower query timeout with a longer invocation is valid");
        assert_eq!(lower.query(), Duration::from_secs(1));
        assert_eq!(lower.invocation(), Duration::from_secs(2));
        let upper = DatabaseTimeouts::try_new(Duration::from_secs(30), Duration::from_secs(60))
            .expect("upper timeout bounds are valid");
        assert_eq!(upper.query(), Duration::from_secs(30));
        assert_eq!(upper.invocation(), Duration::from_secs(60));

        assert_contract_error(
            DatabaseTimeouts::try_new(Duration::ZERO, Duration::from_secs(2)),
            FieldCode::QueryTimeout,
            ValidationReason::NotPositive,
        );
        assert_contract_error(
            DatabaseTimeouts::try_new(Duration::from_millis(999), Duration::from_secs(2)),
            FieldCode::QueryTimeout,
            ValidationReason::TooShort,
        );
        assert_contract_error(
            DatabaseTimeouts::try_new(Duration::from_millis(30_001), Duration::from_secs(31)),
            FieldCode::QueryTimeout,
            ValidationReason::TooLong,
        );
        assert_contract_error(
            DatabaseTimeouts::try_new(Duration::from_secs(5), Duration::from_secs(5)),
            FieldCode::InvocationTimeout,
            ValidationReason::InvalidShape,
        );
        assert_contract_error(
            DatabaseTimeouts::try_new(Duration::from_secs(5), Duration::from_millis(999)),
            FieldCode::InvocationTimeout,
            ValidationReason::TooShort,
        );
        assert_contract_error(
            DatabaseTimeouts::try_new(Duration::from_secs(5), Duration::from_millis(60_001)),
            FieldCode::InvocationTimeout,
            ValidationReason::TooLong,
        );
    }

    #[test]
    fn read_concepts_and_assertions_reject_incomplete_or_oversized_records() {
        let id = concept_id(30);
        let revision = Revision::try_from(1).expect("positive revision");
        let valid_aliases = (0..MAX_CONCEPT_ALIASES)
            .map(|index| Alias {
                display_text: format!("alias-{index:02}"),
                preferred: index == 0,
            })
            .collect::<Vec<_>>();
        let exact_alias_bound = concept(id, "temporary");
        let exact_alias_bound = Concept {
            aliases: valid_aliases,
            ..exact_alias_bound
        };
        assert_eq!(
            deserialize_round_trip(exact_alias_bound)
                .expect("64 aliases with one preferred alias are accepted")
                .aliases
                .len(),
            MAX_CONCEPT_ALIASES
        );

        for aliases in [
            Vec::new(),
            (0..=MAX_CONCEPT_ALIASES)
                .map(|index| Alias {
                    display_text: format!("alias-{index:02}"),
                    preferred: index == 0,
                })
                .collect(),
            vec![Alias {
                display_text: "not preferred".to_owned(),
                preferred: false,
            }],
            vec![
                Alias {
                    display_text: "first".to_owned(),
                    preferred: true,
                },
                Alias {
                    display_text: "second".to_owned(),
                    preferred: true,
                },
            ],
            vec![Alias {
                display_text: "alias\0sentinel".to_owned(),
                preferred: true,
            }],
            vec![Alias {
                display_text: "a".repeat(MAX_ALIAS_DISPLAY_BYTES + 1),
                preferred: true,
            }],
        ] {
            let invalid = Concept {
                id,
                revision,
                aliases,
            };
            assert!(
                deserialize_round_trip(invalid).is_err(),
                "invalid concept alias result should not deserialize"
            );
        }

        let subject = concept_id(31);
        let object = concept_id(32);
        let source = memory_source("evidence-00", 1);
        let valid = assertion(
            assertion_id(33),
            subject,
            RelationType::IsA,
            object,
            vec![source.clone()],
        );
        let decoded =
            deserialize_round_trip(valid).expect("complete assertion evidence is accepted");
        assert_eq!(decoded.evidence.as_slice(), std::slice::from_ref(&source));

        for evidence in [
            Vec::new(),
            (0..=MAX_ASSERTION_EVIDENCE)
                .map(|index| memory_source(&format!("evidence-{index:02}"), 1))
                .collect(),
            vec![source.clone(), source.clone()],
            vec![
                memory_source("z-evidence", 1),
                memory_source("a-evidence", 1),
            ],
        ] {
            let invalid = assertion(
                assertion_id(34),
                subject,
                RelationType::IsA,
                object,
                evidence,
            );
            assert!(
                deserialize_round_trip(invalid).is_err(),
                "incomplete, duplicate, oversized, or unordered evidence should fail"
            );
        }

        let exact_evidence = (0..MAX_ASSERTION_EVIDENCE)
            .map(|index| memory_source(&format!("evidence-{index:02}"), 1))
            .collect();
        let exact_evidence = assertion(
            assertion_id(35),
            subject,
            RelationType::IsA,
            object,
            exact_evidence,
        );
        assert_eq!(
            deserialize_round_trip(exact_evidence)
                .expect("32 ordered sources are accepted")
                .evidence
                .len(),
            MAX_ASSERTION_EVIDENCE
        );
    }

    #[test]
    fn neighbors_and_paths_validate_orientation_cycle_safety_and_order() {
        let start = concept_id(40);
        let middle = concept_id(41);
        let end = concept_id(42);
        let first = assertion(
            assertion_id(50),
            start,
            RelationType::DependsOn,
            middle,
            vec![memory_source("evidence-1", 1)],
        );
        let second = assertion(
            assertion_id(51),
            middle,
            RelationType::Uses,
            end,
            vec![memory_source("evidence-2", 1)],
        );

        NeighborResult::try_new(
            concept(middle, "middle"),
            first.clone(),
            EdgeOrientation::Outgoing,
        )
        .expect("outgoing directed neighbor is valid");
        assert_contract_error(
            NeighborResult::try_new(
                concept(middle, "middle"),
                first.clone(),
                EdgeOrientation::Incoming,
            ),
            FieldCode::Orientation,
            ValidationReason::InvalidShape,
        );
        let symmetric = assertion(
            assertion_id(52),
            middle,
            RelationType::RelatedTo,
            start,
            vec![memory_source("evidence-3", 1)],
        );
        NeighborResult::try_new(
            concept(start, "start"),
            symmetric,
            EdgeOrientation::Symmetric,
        )
        .expect("symmetric edge has symmetric orientation");

        let valid = GraphPath::try_new(
            vec![start, middle, end],
            vec![
                OrientedAssertion {
                    assertion: first.clone(),
                    orientation: EdgeOrientation::Outgoing,
                },
                OrientedAssertion {
                    assertion: second.clone(),
                    orientation: EdgeOrientation::Outgoing,
                },
            ],
        )
        .expect("simple directed path is valid");
        assert_eq!(valid.concepts, [start, middle, end]);

        assert_contract_error(
            GraphPath::try_new(
                vec![start, middle, start],
                vec![
                    OrientedAssertion {
                        assertion: first.clone(),
                        orientation: EdgeOrientation::Outgoing,
                    },
                    OrientedAssertion {
                        assertion: assertion(
                            assertion_id(53),
                            middle,
                            RelationType::IsA,
                            start,
                            vec![memory_source("evidence-4", 1)],
                        ),
                        orientation: EdgeOrientation::Outgoing,
                    },
                ],
            ),
            FieldCode::Path,
            ValidationReason::SelfRelation,
        );
        assert_contract_error(
            GraphPath::try_new(
                vec![start, middle, end],
                vec![OrientedAssertion {
                    assertion: first.clone(),
                    orientation: EdgeOrientation::Outgoing,
                }],
            ),
            FieldCode::Path,
            ValidationReason::InvalidShape,
        );
        assert_contract_error(
            GraphPath::try_new(
                vec![start, middle],
                vec![OrientedAssertion {
                    assertion: first.clone(),
                    orientation: EdgeOrientation::Symmetric,
                }],
            ),
            FieldCode::Orientation,
            ValidationReason::InvalidShape,
        );
        let long_concepts = (0..=MAX_PATH_DEPTH + 1)
            .map(|index| concept_id(100 + u128::from(index)))
            .collect::<Vec<_>>();
        let long_assertions = (0..=MAX_PATH_DEPTH)
            .map(|index| {
                let index_usize = usize::from(index);
                OrientedAssertion {
                    assertion: assertion(
                        assertion_id(100 + u128::from(index)),
                        long_concepts[index_usize],
                        RelationType::IsA,
                        long_concepts[index_usize + 1],
                        vec![memory_source(&format!("long-evidence-{index_usize:02}"), 1)],
                    ),
                    orientation: EdgeOrientation::Outgoing,
                }
            })
            .collect();
        assert_contract_error(
            GraphPath::try_new(long_concepts, long_assertions),
            FieldCode::MaxDepth,
            ValidationReason::TooLong,
        );

        let neighbor_a = NeighborResult::try_new(
            concept(middle, "middle"),
            assertion(
                assertion_id(61),
                start,
                RelationType::Uses,
                middle,
                vec![memory_source("evidence-5", 1)],
            ),
            EdgeOrientation::Outgoing,
        )
        .expect("valid neighbor");
        let neighbor_b = NeighborResult::try_new(
            concept(middle, "middle"),
            assertion(
                assertion_id(60),
                start,
                RelationType::DependsOn,
                middle,
                vec![memory_source("evidence-6", 1)],
            ),
            EdgeOrientation::Outgoing,
        )
        .expect("valid neighbor");
        let mut neighbors = vec![neighbor_a, neighbor_b];
        order_neighbor_results(&mut neighbors);
        assert_eq!(
            neighbors
                .iter()
                .map(|result| result.assertion.relation_type)
                .collect::<Vec<_>>(),
            [RelationType::DependsOn, RelationType::Uses]
        );

        let incoming_edge = assertion(
            assertion_id(70),
            middle,
            RelationType::IsA,
            start,
            vec![memory_source("order-evidence-1", 1)],
        );
        let outgoing_low_neighbor = assertion(
            assertion_id(72),
            end,
            RelationType::IsA,
            start,
            vec![memory_source("order-evidence-2", 1)],
        );
        let outgoing_same_neighbor_lower_id = assertion(
            assertion_id(71),
            end,
            RelationType::IsA,
            start,
            vec![memory_source("order-evidence-3", 1)],
        );
        let outgoing_high_neighbor = assertion(
            assertion_id(69),
            end,
            RelationType::IsA,
            middle,
            vec![memory_source("order-evidence-4", 1)],
        );
        let mut contract_ordered_neighbors = vec![
            NeighborResult::try_new(
                concept(middle, "middle"),
                outgoing_high_neighbor,
                EdgeOrientation::Outgoing,
            )
            .expect("valid outgoing result"),
            NeighborResult::try_new(
                concept(start, "start"),
                outgoing_low_neighbor,
                EdgeOrientation::Outgoing,
            )
            .expect("valid outgoing result"),
            NeighborResult::try_new(
                concept(middle, "middle"),
                incoming_edge,
                EdgeOrientation::Incoming,
            )
            .expect("valid incoming result"),
            NeighborResult::try_new(
                concept(start, "start"),
                outgoing_same_neighbor_lower_id,
                EdgeOrientation::Outgoing,
            )
            .expect("valid outgoing result"),
        ];
        order_neighbor_results(&mut contract_ordered_neighbors);
        assert_eq!(
            contract_ordered_neighbors
                .iter()
                .map(|result| result.orientation)
                .collect::<Vec<_>>(),
            [
                EdgeOrientation::Incoming,
                EdgeOrientation::Outgoing,
                EdgeOrientation::Outgoing,
                EdgeOrientation::Outgoing,
            ]
        );
        assert_eq!(
            contract_ordered_neighbors[1].neighbor.id, start,
            "neighbor UUID breaks ties after relation and orientation"
        );
        assert_eq!(
            contract_ordered_neighbors[1].assertion.id,
            assertion_id(71),
            "assertion UUID is the final neighbor-order key"
        );
        assert_eq!(contract_ordered_neighbors[2].assertion.id, assertion_id(72));
        assert_eq!(contract_ordered_neighbors[3].neighbor.id, middle);

        let symmetric_assertion = assertion(
            assertion_id(48),
            start,
            RelationType::RelatedTo,
            middle,
            vec![memory_source("symmetric-evidence", 1)],
        );
        let symmetric_forward = GraphPath::try_new(
            vec![start, middle],
            vec![OrientedAssertion {
                assertion: symmetric_assertion.clone(),
                orientation: EdgeOrientation::Symmetric,
            }],
        )
        .expect("symmetric forward path");
        let symmetric_reverse = GraphPath::try_new(
            vec![middle, start],
            vec![OrientedAssertion {
                assertion: symmetric_assertion,
                orientation: EdgeOrientation::Symmetric,
            }],
        )
        .expect("symmetric reverse path");
        let short_id_50 = GraphPath::try_new(
            vec![start, middle],
            vec![OrientedAssertion {
                assertion: assertion(
                    assertion_id(50),
                    start,
                    RelationType::IsA,
                    middle,
                    vec![memory_source("short-evidence-50", 1)],
                ),
                orientation: EdgeOrientation::Outgoing,
            }],
        )
        .expect("one-hop path with assertion ID 50");
        let short_id_49 = GraphPath::try_new(
            vec![start, middle],
            vec![OrientedAssertion {
                assertion: assertion(
                    assertion_id(49),
                    start,
                    RelationType::IsA,
                    middle,
                    vec![memory_source("short-evidence-49", 1)],
                ),
                orientation: EdgeOrientation::Outgoing,
            }],
        )
        .expect("one-hop path with assertion ID 49");
        let mut paths = vec![
            valid.clone(),
            short_id_50,
            symmetric_reverse,
            short_id_49,
            symmetric_forward,
        ];
        order_graph_paths(&mut paths);
        assert_eq!(paths[0].assertions[0].assertion.id, assertion_id(48));
        assert_eq!(
            paths[0].concepts,
            [start, middle],
            "concept IDs break ties after assertion IDs"
        );
        assert_eq!(paths[1].assertions[0].assertion.id, assertion_id(48));
        assert_eq!(paths[1].concepts, [middle, start]);
        assert_eq!(paths[2].assertions[0].assertion.id, assertion_id(49));
        assert_eq!(paths[3].assertions[0].assertion.id, assertion_id(50));
        assert_eq!(
            paths[4].assertions.len(),
            2,
            "hop count sorts before path identity"
        );
        assert_eq!(compare_graph_paths(&paths[0], &paths[1]), Ordering::Less);
    }

    #[test]
    fn related_sources_sort_by_role_then_identity_and_deduplicate_exact_links() {
        let memory = memory_source("memory-key", 1);
        let session = session_source("session-key");
        let mut values = vec![
            RelatedSourceResult {
                source: session.clone(),
                role: RelatedSourceRole::AssertionEvidence,
            },
            RelatedSourceResult {
                source: session.clone(),
                role: RelatedSourceRole::Mention,
            },
            RelatedSourceResult {
                source: memory.clone(),
                role: RelatedSourceRole::Mention,
            },
            RelatedSourceResult {
                source: memory.clone(),
                role: RelatedSourceRole::Mention,
            },
        ];
        order_related_source_results(&mut values);

        assert_eq!(values.len(), 3, "identity and role jointly define a link");
        assert_eq!(values[0].role, RelatedSourceRole::Mention);
        assert_eq!(values[0].source, memory);
        assert_eq!(values[1].role, RelatedSourceRole::Mention);
        assert_eq!(values[1].source, session);
        assert_eq!(values[2].role, RelatedSourceRole::AssertionEvidence);
    }

    #[test]
    fn read_results_are_complete_or_error_and_enforce_the_json_budget() {
        let found: DatabaseReadOutcome<Option<u8>> =
            DatabaseReadOutcome::Complete { result: Some(7) };
        let missing: DatabaseReadOutcome<Option<u8>> =
            DatabaseReadOutcome::Complete { result: None };
        let empty: DatabaseReadOutcome<Vec<u8>> =
            DatabaseReadOutcome::Complete { result: Vec::new() };
        assert_eq!(
            found.into_graph_result(OperationCode::GetConcept),
            Ok(Some(7))
        );
        assert_eq!(
            missing.into_graph_result(OperationCode::ResolveAlias),
            Ok(None)
        );
        assert_eq!(
            empty.into_graph_result(OperationCode::Neighbors),
            Ok(Vec::new())
        );

        let marker: DatabaseReadOutcome<Vec<u8>> = DatabaseReadOutcome::ResultTooLarge;
        let marker_json = serde_json::to_value(&marker).expect("size marker serializes");
        assert_eq!(
            marker_json,
            serde_json::json!({"outcome": "result_too_large"})
        );
        assert!(
            serde_json::from_value::<DatabaseReadOutcome<Vec<u8>>>(serde_json::json!({
                "outcome": "result_too_large",
                "result": [1]
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<DatabaseReadOutcome<Vec<u8>>>(serde_json::json!({
                "outcome": "work_exhausted",
                "result": [1]
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<DatabaseReadOutcome<Vec<u8>>>(serde_json::json!({
                "outcome": "complete",
                "result": [],
                "unexpected": true
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<DatabaseReadOutcome<Vec<u8>>>(serde_json::json!({
                "outcome": "complete"
            }))
            .is_err()
        );
        for operation in [
            OperationCode::GetConcept,
            OperationCode::ResolveAlias,
            OperationCode::GetAssertion,
            OperationCode::GetSource,
            OperationCode::Neighbors,
            OperationCode::RelatedSources,
            OperationCode::FindPaths,
        ] {
            assert_eq!(
                DatabaseReadOutcome::<Vec<u8>>::ResultTooLarge
                    .into_graph_result(operation)
                    .expect_err("oversized read has no partial result"),
                GraphError::ResultBoundExceeded { operation }
            );
        }

        let work: DatabaseReadOutcome<Vec<u8>> = DatabaseReadOutcome::WorkExhausted;
        assert_eq!(
            serde_json::to_value(&work).expect("work marker serializes"),
            serde_json::json!({"outcome": "work_exhausted"})
        );
        assert_eq!(
            work.into_graph_result(OperationCode::FindPaths),
            Err(GraphError::TraversalBoundExceeded {
                operation: OperationCode::FindPaths,
                bound: TraversalBound::Work,
                reason: TraversalReason::WorkExhausted,
            })
        );
        assert_eq!(
            DatabaseReadOutcome::<Vec<u8>>::WorkExhausted
                .into_graph_result(OperationCode::Neighbors)
                .expect_err("work exhaustion is a path-only marker"),
            GraphError::InvalidResponse {
                operation: OperationCode::Neighbors,
                reason: BackendFailureReason::UnknownOutcome,
            }
        );

        let exact = "x".repeat(MAX_RESPONSE_JSON_BYTES - 2);
        let exact_result = DatabaseReadOutcome::try_complete(OperationCode::GetConcept, exact)
            .expect("exactly 4 MiB is a complete response");
        assert!(matches!(exact_result, DatabaseReadOutcome::Complete { .. }));
        let oversized = "x".repeat(MAX_RESPONSE_JSON_BYTES - 1);
        let oversized_result =
            DatabaseReadOutcome::try_complete(OperationCode::GetConcept, oversized.clone())
                .expect("oversized JSON produces the marker instead of a partial payload");
        assert_eq!(oversized_result, DatabaseReadOutcome::ResultTooLarge);
        assert_eq!(
            oversized_result
                .into_graph_result(OperationCode::GetConcept)
                .expect_err("the marker maps to one typed failure"),
            GraphError::ResultBoundExceeded {
                operation: OperationCode::GetConcept,
            }
        );
        let encoded_oversized_complete =
            serde_json::to_vec(&DatabaseReadOutcome::Complete { result: oversized })
                .expect("test-only oversized complete payload serializes");
        assert!(
            serde_json::from_slice::<DatabaseReadOutcome<String>>(&encoded_oversized_complete)
                .is_err(),
            "an oversized complete response cannot be deserialized as success"
        );
    }

    #[test]
    fn query_timeouts_use_the_typed_traversal_failure_without_payload() {
        let error = GraphError::TraversalBoundExceeded {
            operation: OperationCode::RelatedSources,
            bound: TraversalBound::QueryTimeout,
            reason: TraversalReason::QueryTimeout,
        };
        assert_eq!(
            error.to_string(),
            "traversal_bound_exceeded:related_sources:query_timeout:query_timeout"
        );
        assert_eq!(
            format!("{error:?}"),
            "GraphError { code: \"traversal_bound_exceeded:related_sources:query_timeout:query_timeout\" }"
        );
    }
}
