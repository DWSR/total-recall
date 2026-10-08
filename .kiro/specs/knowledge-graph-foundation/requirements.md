# Requirements Document

## Introduction

The knowledge graph foundation provides the evidence-backed concept model and internal graph operations described in [brief.md](brief.md).

## Boundary Context

- **In scope**: Concepts, aliases, typed source references, concept mentions, fixed semantic assertions, assertion evidence, mutable graph operations, exact resolution, deterministic neighbors and related sources, and bounded path discovery.
- **Out of scope**: Source-record creation or interpretation, automatic graph population, concept merging, ranking or fusion with text/vector search, external tools, arbitrary node types or properties, tenancy, retention, and graph analytics.
- **Adjacent expectations**: Memory producers supply immutable memory-version identities; a future session processor supplies canonical session-record identities; callers supply graph facts and decide how results are presented.

## Requirements

### Requirement 1: Concepts and Aliases
**Objective:** As a graph writer, I want stable concept identities with unambiguous aliases, so that different records can refer to the same concept.

#### Acceptance Criteria
1. When a valid concept is created, the Knowledge Graph Store shall assign a UUIDv7 stable ID and preserve its preferred alias and additional aliases.
2. The Knowledge Graph Store shall require every concept to have exactly one preferred alias and at least one alias in total.
3. When an alias is accepted, the Knowledge Graph Store shall preserve its display text and derive its comparison key by trimming and collapsing Unicode whitespace, applying Unicode NFKC normalization, and applying Unicode case folding.
4. The Knowledge Graph Store shall associate each normalized alias key with at most one concept.
5. When a concept create has the same complete normalized alias-key set and preferred alias key as an existing concept, the Knowledge Graph Store shall return the existing concept without replacing its display text or creating another record or revision.
6. If a concept create reuses an alias key without matching that concept's complete normalized alias-key set and preferred alias key, the Knowledge Graph Store shall report a conflict without changing graph state.
7. When a concept update supplies its current revision and a valid replacement alias set, the Knowledge Graph Store shall apply the complete replacement and increment the revision once.
8. If a concept update or delete supplies a stale revision, the Knowledge Graph Store shall report a conflict without changing graph state.
9. If an alias update would remove the only alias, leave no preferred alias, or create more than one preferred alias, the Knowledge Graph Store shall reject the update without changing graph state.

### Requirement 2: Source References and Concept Mentions
**Objective:** As a graph writer, I want graph facts tied to stable external records, so that related information remains discoverable without copying source content.

#### Acceptance Criteria
1. When a memory source is registered, the Knowledge Graph Store shall preserve its memory ID and positive version as one typed source identity.
2. When a session-record source is registered, the Knowledge Graph Store shall preserve its canonical session-record ID as one typed source identity without assigning a memory version.
3. The Knowledge Graph Store shall not require the referenced external record to exist before accepting a valid source reference.
4. When an identical source registration is repeated, the Knowledge Graph Store shall return the existing source reference without creating a duplicate.
5. When a concept mention is created, the Knowledge Graph Store shall associate one existing concept with one existing source reference.
6. When an identical concept mention create is repeated, the Knowledge Graph Store shall return the existing mention without creating a duplicate.
7. If a source kind, source key, memory version, concept ID, or mention endpoint is invalid, the Knowledge Graph Store shall reject the operation without changing graph state.
8. The Knowledge Graph Store shall store source identities and graph links without storing source-record content.

### Requirement 3: Relation Vocabulary and Assertions
**Objective:** As a graph reader, I want consistent semantic relation types, so that relation direction has one interpretation across the graph.

#### Acceptance Criteria
1. The Knowledge Graph Store shall accept only `related_to`, `is_a`, `part_of`, `depends_on`, `uses`, `implements`, `causes`, `resolves`, and `contradicts` as relation types.
2. The Knowledge Graph Store shall treat `related_to` and `contradicts` as symmetric relations.
3. The Knowledge Graph Store shall preserve subject-to-object direction for `is_a`, `part_of`, `depends_on`, `uses`, `implements`, `causes`, and `resolves`.
4. When a valid assertion is created, the Knowledge Graph Store shall associate two distinct existing concepts through one accepted relation type and at least one existing supporting source reference.
5. When a symmetric assertion is submitted with reversed endpoints, the Knowledge Graph Store shall identify it as the same semantic assertion.
6. When an identical semantic assertion create is repeated, the Knowledge Graph Store shall return the existing assertion and preserve one canonical assertion identity.
7. If an assertion refers to a missing concept, uses an unsupported relation, relates a concept to itself, or has no supporting source, the Knowledge Graph Store shall reject it without changing graph state.
8. When an assertion update supplies its current revision and valid replacement endpoints or relation type, the Knowledge Graph Store shall apply the replacement, retain its evidence, and increment the revision once.
9. If an assertion update would duplicate another semantic assertion, the Knowledge Graph Store shall report a conflict without changing graph state.

### Requirement 4: Assertion Evidence
**Objective:** As a graph reader, I want every semantic assertion supported by identifiable records, so that I can inspect why concepts are connected.

#### Acceptance Criteria
1. The Knowledge Graph Store shall require every stored assertion to have at least one supporting source reference.
2. When evidence is added to an assertion, the Knowledge Graph Store shall associate one existing source reference with that assertion.
3. When an identical assertion-evidence create is repeated, the Knowledge Graph Store shall return the existing evidence without creating a duplicate.
4. When evidence is removed from an assertion that has other evidence, the Knowledge Graph Store shall remove only the selected association.
5. If evidence removal would leave an assertion without evidence, the Knowledge Graph Store shall reject the removal without changing graph state.
6. When an assertion is deleted with its current revision, the Knowledge Graph Store shall remove the assertion and all of its evidence associations.
7. The Knowledge Graph Store shall not store excerpts, source payloads, or caller-defined properties as assertion evidence.
8. If adding evidence would exceed the declared per-assertion evidence limit, the Knowledge Graph Store shall reject the addition without changing graph state.

### Requirement 5: Mutable Graph Lifecycle
**Objective:** As a graph maintainer, I want bounded correction and deletion behavior, so that graph state can be repaired without damaging unrelated facts.

#### Acceptance Criteria
1. When a concept delete supplies its current revision and no mention or assertion references the concept, the Knowledge Graph Store shall delete the concept and its aliases.
2. If a concept is referenced by a mention or assertion, the Knowledge Graph Store shall reject its deletion without changing graph state.
3. When a concept mention is deleted, the Knowledge Graph Store shall leave its concept, source reference, assertions, and assertion evidence unchanged.
4. When an unreferenced source reference is deleted, the Knowledge Graph Store shall leave concepts and assertions unchanged.
5. If a source reference is used by a concept mention or assertion evidence, the Knowledge Graph Store shall reject its deletion without changing graph state.
6. When a valid mutation changes multiple graph records, the Knowledge Graph Store shall apply all of its changes or none of them.
7. If a mutation conflicts with current graph state, the Knowledge Graph Store shall return the current record identity and revision without applying a partial change.
8. The Knowledge Graph Store shall not represent hard deletion as retained history, soft deletion, or concept merging.

### Requirement 6: Direct Lookup and Concept Resolution
**Objective:** As a graph reader, I want exact graph-record lookup and alias resolution, so that I can inspect known records or enter the graph from known terminology.

#### Acceptance Criteria
1. When an existing concept ID is requested, the Knowledge Graph Store shall return the concept, its revision, preferred alias, and complete alias set.
2. When an alias is requested, the Knowledge Graph Store shall normalize it with the same rules used for alias uniqueness.
3. When a normalized alias matches an existing alias key, the Knowledge Graph Store shall return its single associated concept.
4. When an existing assertion ID is requested, the Knowledge Graph Store shall return its revision, endpoint concepts, relation type, and complete evidence set.
5. When an existing typed source identity is requested, the Knowledge Graph Store shall return its source reference without source-record content.
6. When a requested concept ID, assertion ID, typed source identity, or normalized alias has no match, the Knowledge Graph Store shall return no record.
7. If an alias query normalizes to an empty value or violates an input bound, the Knowledge Graph Store shall reject the query.

### Requirement 7: Neighbor and Related-Source Discovery
**Objective:** As a retrieval component, I want deterministic neighbors and supporting sources, so that I can discover directly related information.

#### Acceptance Criteria
1. When a valid neighbor query identifies a concept, relation filter, direction mode, and positive result limit, the Knowledge Graph Store shall return matching adjacent concepts and assertions.
2. When outgoing direction is requested, the Knowledge Graph Store shall follow directed assertions from subject to object and include symmetric assertions from either endpoint.
3. When incoming direction is requested, the Knowledge Graph Store shall follow directed assertions from object to subject and include symmetric assertions from either endpoint.
4. When either direction is requested, the Knowledge Graph Store shall include each matching assertion once.
5. The Knowledge Graph Store shall return the supporting source references for each neighbor assertion.
6. When related sources are requested for a concept, the Knowledge Graph Store shall return sources linked by direct concept mentions or by evidence for adjacent assertions and identify each link role.
7. The Knowledge Graph Store shall order equivalent neighbor and related-source results deterministically before applying the result limit.
8. When no neighbor or related source matches, the Knowledge Graph Store shall return an empty result set.

### Requirement 8: Bounded Path Discovery
**Objective:** As a retrieval component, I want bounded semantic paths between concepts, so that I can explain indirect relatedness without unbounded graph work.

#### Acceptance Criteria
1. When a valid path query identifies distinct existing endpoint concepts, a direction mode, a positive maximum depth, and a positive result limit, the Knowledge Graph Store shall return matching paths within those bounds.
2. The Knowledge Graph Store shall return only paths that do not repeat a concept.
3. The Knowledge Graph Store shall apply directed and symmetric relation semantics to every traversed assertion.
4. The Knowledge Graph Store shall include each path's ordered concepts, assertions, orientation, and supporting source references.
5. The Knowledge Graph Store shall order paths by ascending hop count and deterministic path identity before applying the result limit.
6. When no path exists within the requested bounds, the Knowledge Graph Store shall return an empty result set.
7. If a path request exceeds a configured hard depth, work, or result bound, the Knowledge Graph Store shall reject it without starting traversal.
8. If traversal reaches its execution bound before completion, the Knowledge Graph Store shall report a bounded-traversal failure instead of returning partial paths as complete.

### Requirement 9: Validation, Results, and Diagnostics
**Objective:** As an operator, I want bounded inputs and content-safe outcomes, so that graph operations fail predictably without exposing stored terminology or source identifiers.

#### Acceptance Criteria
1. The Knowledge Graph Store shall require non-empty, NUL-free labels, aliases, and source keys, limit source keys to 2,048 UTF-8 bytes, and enforce declared byte bounds on graph-owned text.
2. If a request contains alias text whose normalized key is empty or oversized, an empty, NUL-containing, or over-limit source key, an unsupported source shape, an invalid revision, or a non-positive query limit, the Knowledge Graph Store shall reject it before changing graph state.
3. When a read succeeds, the Knowledge Graph Store shall return only the requested graph records and typed source references without source-record content.
4. If a read fails, the Knowledge Graph Store shall return an error instead of presenting partial results as complete.
5. The Knowledge Graph Store shall identify validation and conflict failures with stable operation, field, and reason codes.
6. The Knowledge Graph Store shall not include concept labels, aliases, source keys, source content, query paths, backend statements, credentials, or dependency messages in logs or returned errors.
7. If a complete read result would exceed the declared response-size bound, the Knowledge Graph Store shall report a bounded-result failure instead of returning partial results as complete.

### Requirement 10: Focused Verification
**Objective:** As a maintainer, I want automated coverage of graph invariants and bounded discovery, so that semantic and lifecycle regressions are detected.

#### Acceptance Criteria
1. The Knowledge Graph Store shall include automated verification for concept identity, Unicode alias equivalence, preferred-alias invariants, global alias conflicts, idempotent creates, revisions, and stale mutations.
2. The Knowledge Graph Store shall include automated verification for both source kinds, opaque source handling, mention identity, missing endpoints, and restricted source deletion.
3. The Knowledge Graph Store shall include automated verification for every fixed relation type, directionality, symmetric endpoint canonicalization, self-relation rejection, evidence requirements and limits, and assertion conflicts.
4. The Knowledge Graph Store shall include automated verification for concept and assertion updates and for alias, mention, evidence, assertion, source, and concept deletion behavior.
5. The Knowledge Graph Store shall include automated verification for direct lookup, alias resolution, neighbor direction modes, related-source link roles, deterministic limits, bounded response size, empty results, and complete evidence projection.
6. The Knowledge Graph Store shall include automated verification for cycle-safe paths, depth and work bounds, deterministic shortest-path ordering, traversal failures, and no partial results.
7. The Knowledge Graph Store shall include automated verification that invalid requests cause no state change and diagnostics omit protected graph and source values.
8. The Knowledge Graph Store shall include automated verification that concurrent cross-aggregate swaps and absent-key races complete without deadlock or untyped database failure.
