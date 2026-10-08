# Implementation Plan

- [ ] 1. Establish the graph-store foundation
- [x] 1.1 Create the package and dependency baseline
  - Add the graph package to the workspace with exact UUIDv7, ICU4X, async, serialization, error, and test dependencies.
  - Establish the public module shell without changing existing memory, worker, or protocol packages.
  - Keep the package private and compatible with the workspace Rust and Nix toolchains.
  - Completion is observable when the locked package builds and its empty test targets run without changing existing package behavior.
  - _Requirements: 10.1, 10.7_
  - _Boundary: Contracts and Normalizer_

- [x] 1.2 Define graph identity, source, relation, revision, and error contracts
  - Represent store-generated UUIDv7 identities, typed memory/session sources, all nine relation types, relation direction, revisions, and protected record identities.
  - Define stable operation, field, and reason codes for validation, conflict, reference, limit, traversal, and backend failures.
  - Preserve owner-specific source identity validation, enforce the 2,048-byte UTF-8 source-key bound, and exclude source content.
  - Completion is observable when contract tests accept every valid shape, reject every invalid edge, and prove protected values never appear in error formatting.
  - _Requirements: 1.1, 2.1, 2.2, 2.7, 3.1, 3.2, 3.3, 5.7, 9.1, 9.2, 9.5, 9.6, 10.1, 10.2, 10.3, 10.7_
  - _Boundary: Contracts and Normalizer_

- [x] 1.3 Define typed graph mutation contracts
  - Cover concept alias replacement, source registration, mentions, assertions, evidence, revisions, deletes, and idempotent create outcomes.
  - Represent exact normalized alias-set identity, semantic assertion identity, evidence limits, stale state, references, and all-or-none mutation results.
  - Separate caller inputs from private validated values accepted by the database port.
  - Completion is observable when every service mutation and database outcome has one round-trip-tested typed contract.
  - _Requirements: 1.2, 1.5, 1.6, 1.7, 1.8, 1.9, 2.4, 2.5, 2.6, 2.7, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 9.3, 9.4, 9.5, 9.6, 9.7_
  - _Boundary: Contracts and Normalizer_

- [x] 1.4 Define typed graph read contracts and hard bounds
  - Cover direct lookup, alias resolution, neighbor direction and relation filters, related-source roles, and bounded path requests.
  - Represent deterministic ordered results with complete evidence, empty results, timeout/work failures, and the complete-result size marker.
  - Enforce the approved alias, evidence, result, depth, work, timeout, and 4 MiB response limits.
  - Completion is observable when each read operation has validated input tests and one typed complete-or-error output contract.
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 9.3, 9.4, 9.5, 9.6, 9.7_
  - _Boundary: Contracts and Normalizer_

- [x] 1.5 Implement versioned Unicode alias normalization
  - Apply Unicode whitespace collapse, NFKC, a second collapse, and full non-Turkic case folding under the pinned algorithm version.
  - Preserve display text while validating normalized key length, set uniqueness, alias count, and exactly one preferred alias.
  - Treat equivalent normalized sets and preferred keys as identical regardless of display spelling.
  - Completion is observable when multilingual whitespace, compatibility, case-fold expansion, empty-key, conflict, and preferred-set fixtures pass.
  - _Requirements: 1.2, 1.3, 1.4, 1.5, 1.6, 6.2, 6.3, 6.7, 10.1_
  - _Boundary: Contracts and Normalizer_

- [ ] 2. Build the authoritative graph schema and mutation boundary
- [x] 2.1 Persist relation, concept, alias, source, and mention state
  - Seed all nine fixed relation codes with their approved symmetry metadata.
  - Enforce UUIDv7 concepts, globally unique normalized aliases, one preferred alias, typed source shapes, mention identity, and restrictive ownership links.
  - Add deterministic lookup, source-reverse, and alias indexes while keeping source-owner tables independent.
  - Revoke public access now and defer application grants until every routine exists.
  - Completion is observable when migration contract tests prove the state shape, seeds, constraints, deferred alias invariant, and absence of cross-owner foreign keys.
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.9, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 3.1, 3.2, 3.3, 5.1, 5.2, 5.3, 5.4, 5.5, 9.1, 10.1, 10.2, 10.4_
  - _Boundary: Schema Migration_

- [x] 2.2 Persist semantic assertions and mandatory evidence
  - Enforce UUIDv7 assertion identity, distinct endpoints, fixed relation references, canonical symmetric endpoints, and one semantic assertion per triple.
  - Persist evidence as source associations with restrictive source links and assertion-owned cascade behavior.
  - Require at least one evidence association for each surviving assertion at transaction completion.
  - Add subject, object, evidence-source, and traversal indexes in deterministic key order.
  - Completion is observable when migration tests prove semantic uniqueness, direction, self-edge rejection, evidence invariants, cascades, restrictions, and index shapes.
  - _Requirements: 3.4, 3.5, 3.6, 3.7, 4.1, 4.6, 4.7, 5.2, 5.5, 7.1, 7.2, 7.3, 7.4, 7.5, 8.2, 8.3, 8.4, 10.3, 10.4_
  - _Boundary: Schema Migration_

- [x] 2.3 Establish canonical mutation lock ordering
  - Encode aggregate and uniqueness lock keys as canonical domain-tagged JSON.
  - Hash every key with the fixed seed, deduplicate and numerically sort advisory integers, then acquire all advisory locks before row locks.
  - Lock existing concept, assertion, and source rows in fixed table and native primary-key order.
  - Completion is observable when database helper tests prove identical ordering across permuted inputs, collision deduplication, and no post-acquisition lock discovery.
  - _Requirements: 5.6, 5.7, 10.8_
  - _Boundary: Schema Migration_

- [x] 2.4 Add atomic concept mutation routines
  - Create concepts from UUIDv7 candidates and return the existing concept for an exact normalized alias-set and preferred-key match.
  - Replace aliases under current revision while preserving stored display text on idempotent create.
  - Reject overlapping non-matching aliases, stale revisions, snapshot drift, referenced deletion, and invalid final alias sets with typed outcomes.
  - Completion is observable when each routine returns exactly one outcome and every failure leaves concept and alias state unchanged.
  - _Requirements: 1.1, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 5.1, 5.2, 5.6, 5.7, 10.1, 10.4, 10.8_
  - _Boundary: Schema Migration_

- [x] 2.5 Add atomic source-reference and mention routines
  - Register memory-version and session-record identities idempotently without checking external table existence, while enforcing the 2,048-byte UTF-8 source-key bound.
  - Create and remove concept mentions by identity and reject missing endpoints.
  - Delete only unreferenced source identities while preserving unrelated concepts and assertions.
  - Completion is observable when replay, opaque missing-source, mention lifecycle, source-reference conflict, and referenced-delete cases return typed all-or-none outcomes.
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 5.3, 5.4, 5.5, 5.6, 5.7, 10.2, 10.4, 10.8_
  - _Boundary: Schema Migration_

- [x] 2.6 Add atomic assertion mutation routines
  - Create assertions from UUIDv7 candidates while canonicalizing symmetric endpoints and preserving directed subject-to-object meaning.
  - Return one canonical assertion for identical or concurrent semantic creates and attach missing requested evidence.
  - Update endpoints or relation under current revision while retaining evidence; reject duplicates, stale revisions, and snapshot drift.
  - Delete assertions with their evidence only under the current revision.
  - Completion is observable when every semantic, revision, idempotency, update, and delete case returns one typed outcome with no partial state.
  - _Requirements: 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 4.6, 5.2, 5.6, 5.7, 10.3, 10.4, 10.8_
  - _Boundary: Schema Migration_

- [x] 2.7 Add atomic evidence mutation routines
  - Add supporting sources idempotently under the assertion lock and enforce the total cap of 32.
  - Remove one association only when other evidence remains.
  - Return explicit missing, evidence-limit, and would-orphan outcomes without exposing source keys.
  - Completion is observable when concurrent-safe cap, duplicate add, selective remove, last-evidence rejection, and assertion-delete cascade cases preserve the evidence invariant.
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 5.5, 5.6, 5.7, 10.3, 10.4, 10.8_
  - _Boundary: Schema Migration_

- [x] 2.8 Finalize mutation security and graph-role privileges
  - Make mutation and invariant routines owner-executed with fully qualified objects and fixed trusted search paths.
  - Revoke every function and table from public and direct application mutation.
  - Grant the graph application role only table reads and execution of the completed mutation routines.
  - Completion is observable when migration tests prove exact owners, search paths, revokes, grants, no direct DML, and no callable trigger helpers.
  - _Requirements: 5.6, 5.8, 9.6, 10.4, 10.7_
  - _Boundary: Schema Migration_

- [ ] 3. Implement the typed application service and database port
- [x] 3.1 (P) Define the complete graph database port and test doubles
  - Expose one validated operation for each mutation, direct read, neighbor/source query, and path query.
  - Add recording and failing implementations that capture typed calls without iii or PostgreSQL.
  - Keep database envelopes, SQL, and backend messages out of the port contract.
  - Completion is observable when every validated operation can be independently recorded, failed, and asserted in service tests.
  - _Requirements: 9.3, 9.4, 9.5, 10.7_
  - _Boundary: KnowledgeGraphDatabase_
  - _Depends: 1.2, 1.3, 1.4, 1.5_

- [x] 3.2 Implement concept and alias service operations
  - Validate and normalize aliases before database activity and generate UUIDv7 candidates only for accepted creates.
  - Map created and existing outcomes to canonical success while preserving stored display text.
  - Map alias ownership, set mismatch, stale revision, snapshot drift, reference, and missing outcomes to stable typed errors.
  - Completion is observable when invalid inputs make zero port calls and every create, update, delete, idempotency, and conflict path passes recording/failing tests.
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 5.1, 5.2, 5.6, 5.7, 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 10.1, 10.4, 10.7_
  - _Boundary: KnowledgeGraphStore_

- [x] 3.3 Implement source-reference and mention service operations
  - Validate both source variants under their owner contracts and enforce the 2,048-byte UTF-8 source-key bound before port calls.
  - Coordinate idempotent registration, mention creation/removal, and restricted source deletion.
  - Omit source keys from every error and diagnostic while returning them in successful typed records.
  - Completion is observable when both source shapes and the complete mention lifecycle pass recording/failing tests without source-content leakage.
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 5.3, 5.4, 5.5, 5.6, 5.7, 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 10.2, 10.4, 10.7_
  - _Boundary: KnowledgeGraphStore_

- [x] 3.4 Implement assertion and evidence service operations
  - Validate relation semantics, distinct endpoints, revisions, supporting-source sets, and evidence changes before port calls.
  - Generate UUIDv7 assertion candidates and accept the database's canonical ID for identical or concurrent semantic creates.
  - Map semantic duplicates, stale state, snapshot drift, evidence limits, last-evidence protection, and missing endpoints to stable errors.
  - Completion is observable when all nine relation types, symmetric reversal, create/update/delete, evidence lifecycle, UUIDv7, and privacy cases pass service tests.
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 5.2, 5.5, 5.6, 5.7, 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 10.3, 10.4, 10.7, 10.8_
  - _Boundary: KnowledgeGraphStore_

- [x] 3.5 Implement direct lookup and alias-resolution orchestration
  - Validate concept, assertion, source, and alias inputs before the read port.
  - Normalize aliases through the same versioned algorithm used for uniqueness.
  - Return complete typed records, `None` for absence, and no partial records on failure.
  - Completion is observable when exact lookup, equivalent alias, no-match, invalid alias, complete evidence, and result-bound cases pass service tests.
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 9.3, 9.4, 9.5, 9.6, 9.7, 10.5, 10.7_
  - _Boundary: KnowledgeGraphStore_

- [x] 3.6 Implement neighbor and related-source orchestration
  - Validate concept, direction, relation filters, and approved result limits before the port call.
  - Preserve ordered adjacent assertions, orientation, complete evidence, source link roles, and deduplication.
  - Return empty results for no match and stable result-bound failures without partial output.
  - Completion is observable when every direction mode, relation filter, role, limit, empty, and bound case passes service tests.
  - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 9.3, 9.4, 9.5, 9.6, 9.7, 10.5, 10.7_
  - _Boundary: KnowledgeGraphStore_

- [x] 3.7 Implement bounded-path orchestration
  - Validate distinct endpoints, direction, depth, work, result, response, query, and invocation bounds.
  - Preserve ordered path concepts, assertions, orientation, and complete evidence.
  - Distinguish empty paths, work exhaustion, query timeout, result overflow, backend failure, and malformed response without partial success.
  - Completion is observable when valid, empty, invalid-bound, traversal-bound, timeout, result-bound, and privacy cases pass service tests.
  - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 9.3, 9.4, 9.5, 9.6, 9.7, 10.6, 10.7_
  - _Boundary: KnowledgeGraphStore_

- [ ] 4. Implement the iii database adapter and graph queries
- [x] 4.1 Implement common iii envelopes and strict outcome decoding
  - Build static execute and query requests with validated logical target, positional parameters, query timeout, and longer invocation timeout.
  - Decode exactly one mutation outcome or exactly one complete read payload/result marker.
  - Recognize only outer `invocation_failed`, literal `handler error: `, and inner `QUERY_TIMEOUT`; treat every near miss as an opaque database failure.
  - Discard SQL, parameters, rows, backend messages, and protected values from every adapter error.
  - Completion is observable when exact envelope, timeout, near-miss, malformed outcome, unexpected row-count, result-marker, and leak tests pass.
  - _Requirements: 5.6, 5.7, 8.7, 8.8, 9.3, 9.4, 9.5, 9.6, 9.7, 10.7_
  - _Boundary: KnowledgeGraphDatabase_
  - _Depends: 2.8, 3.1_

- [x] 4.2 Implement concept mutation and concept/alias read operations
  - Invoke only the approved concept routines with normalized alias JSON, UUIDv7 candidates, and expected revisions.
  - Decode canonical created/existing concepts, ordered aliases, conflict identity/revision codes, references, missing state, and confirmed deletes.
  - Resolve normalized aliases and direct concept IDs through deterministic complete-payload queries.
  - Completion is observable when every concept outcome and lookup shape round-trips exactly without exposing alias values in failures.
  - _Requirements: 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 5.1, 5.2, 5.6, 5.7, 6.1, 6.2, 6.3, 6.6, 6.7, 9.3, 9.4, 9.5, 9.6, 9.7_
  - _Boundary: KnowledgeGraphDatabase_

- [x] 4.3 Implement source-reference and mention database operations
  - Invoke only the approved source and mention routines with typed source shapes and concept IDs.
  - Preserve accepted source-key values of at most 2,048 UTF-8 bytes through successful parameters and records.
  - Decode idempotent registration, mention lifecycle, missing endpoints, referenced deletion, direct source lookup, and confirmed absence.
  - Completion is observable when both source variants and every mention/delete outcome decode without source keys entering errors.
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 5.3, 5.4, 5.5, 5.6, 5.7, 6.5, 6.6, 9.3, 9.4, 9.5, 9.6, 9.7_
  - _Boundary: KnowledgeGraphDatabase_

- [x] 4.4 Implement assertion mutation and direct-read operations
  - Invoke approved assertion routines with UUIDv7 candidates, canonical relation forms, expected revisions, and supporting sources.
  - Decode canonical assertion identity, directed/symmetric endpoints, retained complete evidence, semantic conflicts, stale state, and deletion.
  - Return direct assertions with deterministic evidence order and no source content.
  - Completion is observable when every relation, reversal, create/update/delete, conflict, direct read, and no-match case round-trips exactly.
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 5.2, 5.6, 5.7, 6.4, 6.6, 9.3, 9.4, 9.5, 9.6, 9.7_
  - _Boundary: KnowledgeGraphDatabase_

- [x] 4.5 Implement evidence database operations
  - Invoke approved evidence routines under assertion identity with typed source references.
  - Decode existing/created evidence, the 32-source cap, selective removal, would-orphan protection, missing assertion/source, and assertion-delete cascade state.
  - Keep source values in successful records only.
  - Completion is observable when duplicate add, cap, remove, orphan, missing, and delete cases decode to exact domain outcomes.
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 5.5, 5.6, 5.7, 9.3, 9.4, 9.5, 9.6, 9.7_
  - _Boundary: KnowledgeGraphDatabase_

- [x] 4.6 Implement deterministic neighbor and related-source queries
  - Expand directed assertions by requested direction and symmetric assertions from either endpoint while emitting each assertion once.
  - Filter relations, preserve complete ordered evidence, derive mention/evidence link roles, deduplicate sources, and apply deterministic limits.
  - Aggregate one complete JSON payload and return only the result-size marker when it exceeds 4 MiB.
  - Completion is observable when direction, symmetry, filtering, roles, order, limit, empty, and result-marker query tests pass.
  - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 9.3, 9.4, 9.5, 9.6, 9.7, 10.5_
  - _Boundary: KnowledgeGraphDatabase_

- [x] 4.7 Implement bounded recursive path queries
  - Expand directed and symmetric edges while tracking UUIDv7 concept/assertion sequences and excluding repeated concepts.
  - Fence recursive states at requested work plus one before blocking order and return only the work-exhausted marker on overflow.
  - Order complete paths by hops and native UUID sequences, include complete evidence, enforce depth/result/4 MiB bounds, and map exact query timeout.
  - Completion is observable when cyclic, branching, shortest-order, empty, work, depth, result, timeout, and malformed-response query tests pass without partial paths.
  - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 9.3, 9.4, 9.5, 9.6, 9.7, 10.6_
  - _Boundary: KnowledgeGraphDatabase_

- [ ] 5. Add independent protocol and PostgreSQL verification
- [x] 5.1 Scaffold the graph database protocol fake and cover mutations
  - Add the standalone fake package to the workspace and launch the graph adapter against an in-process iii peer.
  - Verify every mutation and direct-read function ID, database target, SQL class, positional value, timeout, response ordering, and expected outcome shape.
  - Reject malformed outcomes, unexpected calls, retries, namespace drift, and success before the fake database response.
  - Completion is observable when the release adapter flow exits zero without live iii, database worker, or PostgreSQL.
  - _Requirements: 1.5, 1.6, 2.4, 2.6, 3.6, 4.3, 5.6, 6.1, 6.4, 6.5, 9.3, 9.4, 10.7_
  - _Boundary: Protocol Fake_
  - _Depends: 4.7_

- [x] 5.2 Extend the protocol fake for bounded queries and privacy failures
  - Verify neighbor, related-source, and path request parameters and complete JSON result envelopes.
  - Reproduce exact `invocation_failed` plus `handler error: {"code":"QUERY_TIMEOUT",...}` behavior and every near miss.
  - Cover result markers, malformed JSON, worker/invocation timeouts, generic failures, protected sentinels, and no retries.
  - Completion is observable when every bounded-query, timeout, malformed-envelope, result-size, and privacy scenario exits zero.
  - _Requirements: 7.1, 7.5, 7.6, 7.7, 8.4, 8.5, 8.7, 8.8, 9.3, 9.4, 9.6, 9.7, 10.5, 10.6, 10.7_
  - _Boundary: Protocol Fake_

- [x] 5.3 Scaffold isolated PostgreSQL graph verification
  - Add the standalone smoke package to the workspace with isolated migration-owner and graph-application roles for PostgreSQL 17 and 18.
  - Configure the 10-second query, 12-second statement, and 15-second invocation contract plus statement capture or disabled row-change publishing prerequisites.
  - Provide reproducible database creation, migration application, cleanup, and rollback harnesses.
  - Completion is observable when an empty PG17/18 fixture applies and rolls back the graph migration under the intended roles.
  - _Requirements: 9.6, 10.4, 10.7_
  - _Boundary: PostgreSQL Smoke fixture_
  - _Depends: 2.8_

- [x] 5.4 Verify schema, migration security, and rollback
  - Assert every table, relation seed, UUIDv7/source shape, constraint, deferred invariant, foreign key, index, and routine signature.
  - Verify fixed search paths, owners, public revokes, graph-role reads/routine execution, direct-DML denial, and trigger-helper denial.
  - Prove failed migration or invariant checks leave no partial schema/state and clean rollback succeeds.
  - Completion is observable when the schema verifier passes unchanged on PostgreSQL 17 and 18.
  - _Requirements: 1.1, 1.2, 1.4, 1.9, 2.1, 2.2, 2.5, 3.1, 3.2, 3.3, 3.4, 3.7, 4.1, 5.8, 9.6, 10.1, 10.2, 10.3, 10.4, 10.7_
  - _Boundary: PostgreSQL Smoke schema_
  - _Depends: 5.3_

- [x] 5.5 Verify complete graph mutation lifecycles
  - Exercise concept alias identity/revisions, both source variants, mentions, every relation, assertions, evidence, restrictions, cascades, and hard-delete absence.
  - Verify exact created/existing/conflict/stale/referenced/missing/limit/orphan outcomes and idempotent retries.
  - Assert invalid and failed mutations preserve all prior graph state.
  - Completion is observable when the lifecycle verifier passes unchanged on PostgreSQL 17 and 18.
  - _Requirements: 1.5, 1.6, 1.7, 1.8, 1.9, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 10.1, 10.2, 10.3, 10.4_
  - _Boundary: PostgreSQL Smoke mutations_
  - _Depends: 5.4_

- [x] 5.6 Verify concurrent graph mutations and lock ordering
  - Race identical and overlapping concept/assertion creates, revision updates, source/delete operations, and evidence additions/removals.
  - Exercise alias and semantic cross-swaps, absent uniqueness keys, evidence-cap contention, and last-evidence orphan races repeatedly.
  - Require one canonical outcome, unchanged rejected state, and no deadlock, lock-order drift, or untyped database failure.
  - Completion is observable when repeated concurrent runs pass on PostgreSQL 17 and 18.
  - _Requirements: 1.4, 1.5, 1.6, 1.7, 1.8, 3.5, 3.6, 3.8, 3.9, 4.3, 4.4, 4.5, 4.8, 5.5, 5.6, 5.7, 10.8_
  - _Boundary: PostgreSQL Smoke concurrency_
  - _Depends: 5.5_

- [x] 5.7 (P) Verify deterministic graph discovery and traversal
  - Cover every relation direction, symmetric reversal, neighbor filter/mode, related-source role, evidence projection, order, limit, and empty result.
  - Exercise cyclic and branching paths, no repeated concepts, shortest deterministic native-UUID order, depth/work/result limits, and work sentinel.
  - Verify the 4 MiB marker, exact query timeout behavior, and complete-or-error results with no partial payload.
  - Completion is observable when the separate traversal verifier passes unchanged on PostgreSQL 17 and 18.
  - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 9.3, 9.4, 9.7, 10.5, 10.6_
  - _Boundary: PostgreSQL Smoke traversal_
  - _Depends: 4.7, 5.4_

- [ ] 6. Integrate reproducible verification and close the feature
- [x] 6.1 Package the PostgreSQL 17/18 graph verifiers in Nix
  - Add explicit-source verifier packages for schema, mutation/concurrency, and traversal suites on both supported majors.
  - Include only declared migration, script, and shell inputs and exclude generated workspace artifacts.
  - Preserve existing search and memory verifier outputs.
  - Completion is observable when every graph verifier package builds through a Git-backed flake reference on its supported systems.
  - _Requirements: 10.4, 10.5, 10.6, 10.8_
  - _Boundary: PostgreSQL Smoke integration_
  - _Depends: 5.4, 5.5, 5.6, 5.7_

- [x] 6.2 Wire graph verification into continuous integration
  - Run graph crate tests, the release protocol fake, and the PostgreSQL 17/18 verifier matrix alongside existing checks.
  - Reference every graph verifier output explicitly.
  - Replace direct-smoke `path:.` invocations with Git-backed flake references without dropping existing verifier coverage.
  - Completion is observable when the workflow contains every graph gate and existing checks remain represented.
  - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8_
  - _Boundary: Protocol Fake, PostgreSQL Smoke integration_
  - _Depends: 5.2, 6.1_

- [x] 6.3 Run the complete feature validation gate
  - Run locked workspace tests, formatting, warning-free Clippy, dependency policy, flake checks, the protocol fake, and every PG17/18 graph verifier.
  - Inspect failures only to identify the owning prior task; do not repair across responsibility boundaries in this validation task.
  - Reopen the owning task for any failure and repeat this gate after that task is corrected.
  - Completion is observable only when every declared command and verifier exits successfully with no protected-value diagnostics.
  - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8_
  - _Boundary: Final validation_
  - _Depends: 6.2_

## Implementation Notes

- Memory and session source keys are capped at 2,048 UTF-8 bytes per requirement 9.1 and the approved design; the source-owner identity checks remain in force.
- Graph UUIDv7 validation checks the RFC variant as well as the version; source-reference not-found errors omit external identifiers.
- PostgreSQL UUID and BIGINT bind parameters use text-first SQL casts and decimal-string JSON values to match the database worker binding contract.
- PostgreSQL catalog checks do not replace direct 2,048/2,049-byte source-key boundary inserts for both source kinds.
