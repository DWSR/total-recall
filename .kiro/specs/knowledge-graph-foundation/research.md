# Research: knowledge-graph-foundation

## Gap Summary

- The repository has reusable validation, ports-and-adapters, static SQL, opaque error, protocol-fake, and PostgreSQL 17/18 verification patterns.
- No existing component owns canonical graph entities, mutable graph state, Unicode alias keys, adjacency queries, or recursive traversal.
- A graph-specific schema and database port avoid coupling mutable graph behavior to immutable memory history.
- The highest-risk gaps are Unicode key compatibility, typed conflict outcomes under concurrency, evidence-preserving mutations, and bounded recursive traversal.
- Canonical session records do not exist yet; session sources must remain typed opaque references.

## Requirement-to-Asset Map

| Requirements | Existing assets | Gap |
|---|---|---|
| 1 Concepts and aliases | Validated text/revision patterns and redacted errors in `crates/memory-store/src/contracts.rs` | Concept aggregate, Unicode normalization, alias uniqueness, preferred alias invariant, revision compare-and-swap |
| 2 Sources and mentions | Immutable memory identity `(id, version)`; opaque session correlation IDs | Typed source registry, source-shape checks, concept mentions, restricted deletion |
| 3–4 Assertions and evidence | Composite keys, restrictive foreign keys, SQL checks | Relation semantics, symmetric canonicalization, semantic uniqueness, mandatory evidence |
| 5 Mutable lifecycle | Static one-statement execution and migration-owned privileges | Graph-only update/delete grants, atomic multi-row outcomes, stale revision and dependency conflicts |
| 6 Direct lookup | Strict row decoding and content-opaque failures | Graph projections and alias lookup |
| 7 Neighbor discovery | Deterministic `COLLATE "C"` tie-breaking before limits | Direction-aware indexes, relation filters, evidence aggregation, link-role projection |
| 8 Paths | PostgreSQL 17/18 verifier infrastructure | Recursive query, cycle guard, depth/work bounds, deterministic path identity, timeout classification |
| 9 Validation and diagnostics | Validation-before-port tests and fixed dependency failures | Graph-specific bounds, errors, and conflict records |
| 10 Verification | Recording/failing ports, iii protocol fake, direct PostgreSQL scripts | Graph fixtures, concurrency cases, traversal corpus, Unicode compatibility vectors |

## Existing Integration Patterns

- Domain libraries live under `crates/`; migrations remain beside their schema owner.
- `crates/memory-store` separates contracts, application service, database port, and iii adapter. Its immutable-memory responsibilities should not absorb mutable graph ownership.
- `integration/memory-database-engine-fake` verifies exact iii request and response envelopes without a live engine.
- `integration/memory-postgres-smoke` and `.github/workflows/ci.yml` provide PostgreSQL 17/18 migration, privilege, concurrency, and ordering patterns.
- New Nix and CI commands must use Git-backed flake references rather than copying the existing `path:.` pattern.

## Missing Capabilities

- Graph contracts and aggregate validation.
- Stable alias normalization with an explicit Unicode data version.
- Six normalized storage areas: concepts, aliases, source references, concept mentions, assertions, and assertion evidence.
- Atomic idempotent-create, revision update/delete, dependency-conflict, and cascade outcomes.
- Direct lookup, alias resolution, direction-aware neighbors, related sources, and bounded paths.
- A database-side traversal timeout and a logical work-unit budget.
- Application privileges permitting graph mutation without broadening immutable memory privileges.
- Unit, adapter, protocol, and direct PostgreSQL graph verification.

## Technical Findings

### Unicode Alias Keys

- Stable Rust supports Unicode whitespace splitting, but stable full case folding still requires a library.
- ICU4X `icu_normalizer` and `icu_casemap` 2.3.0 provide NFKC and locale-independent full case folding, support the workspace Rust version, and use the repository-allowed `Unicode-3.0` license.
- `unicode-normalization` lacks case folding. `unicode-casefold` provides folding but is not actively maintained.
- NFKC can introduce whitespace. A stable pipeline needs collapse/trim before normalization and a second collapse after NFKC, followed by full non-Turkic case folding.
- Alias keys must be persisted. Changing ICU or Unicode data can otherwise change uniqueness and lookup behavior for existing aliases.
- Rust strings are valid UTF-8. Malformed byte-sequence rejection belongs at future wire adapters; domain tests can cover invalid normalized outcomes and forbidden scalar content.

Sources: [Rust whitespace](https://doc.rust-lang.org/1.98.1/std/primitive.str.html#method.split_whitespace), [ICU NFKC](https://docs.rs/icu_normalizer/2.3.0/icu_normalizer/struct.ComposingNormalizerBorrowed.html), [ICU case folding](https://docs.rs/icu_casemap/2.3.0/icu_casemap/struct.CaseMapperBorrowed.html#method.fold_string).

### PostgreSQL Traversal

- PostgreSQL 17 and 18 support recursive CTEs plus SQL-standard `SEARCH` and `CYCLE` clauses.
- `SEARCH` creates ordering data; it does not control executor visitation order. Final deterministic ordering remains necessary.
- `CYCLE` can maintain a visited concept path and prevent repeated-concept expansion. Depth remains explicit recursive state.
- An outer result `LIMIT` is not a recursion guard because sorting or joining can consume the full recursive result.
- PostgreSQL exposes no exact recursive executor-operation budget. A testable logical work unit can be one generated traversal state.
- A materialized pre-sort fence with `max_work + 1` states can detect logical exhaustion. It bounds generated states, not all heap/index work, so `statement_timeout` remains required.
- Exact global expansion counting would require a migration-owned iterative function, increasing privilege and implementation complexity.

Sources: [PostgreSQL 17 recursive queries](https://www.postgresql.org/docs/17/queries-with.html), [PostgreSQL 18 recursive queries](https://www.postgresql.org/docs/18/queries-with.html), [statement timeout](https://www.postgresql.org/docs/18/runtime-config-client.html#GUC-STATEMENT-TIMEOUT).

### Fixed Relation Vocabulary

- A PostgreSQL enum strongly types values but cannot remove or reorder them without type recreation.
- A check constraint is compact but cannot carry symmetry metadata and must be replaced to evolve.
- A migration-owned lookup table can hold the nine fixed codes and `is_symmetric`, enforce references, and deny application-role vocabulary mutation.

Sources: [PostgreSQL enums](https://www.postgresql.org/docs/18/datatype-enum.html), [PostgreSQL constraints](https://www.postgresql.org/docs/18/ddl-constraints.html).

### Source-Key B-Tree Bound

- PostgreSQL 17 and 18 limit B-tree index entries to approximately one-third of a page after TOAST compression.
- The source identity unique index stores `source_kind`, `external_id`, and nullable `external_version`; sufficiently large external IDs cannot fit in the index.
- With 8 KiB pages, a 2,048-byte source key leaves room for the other indexed fields and tuple metadata under PostgreSQL's approximate one-third-page B-tree entry ceiling.

Sources: [PostgreSQL 17 B-tree indexes](https://www.postgresql.org/docs/17/btree.html), [PostgreSQL 18 B-tree indexes](https://www.postgresql.org/docs/18/btree.html).

### iii Database Worker 0.5.17

- `database::query` runs in a read-only transaction, supports `timeout_ms`, and returns named columns after fully materializing rows.
- `database::execute` autocommits one statement and has no execute-level timeout field.
- `database::transaction` and `executeBatch` support ordered atomic statements; callers must reject a response with `committed: false`.
- Arrays, path state, custom types, and aggregate evidence should be projected as JSON or text. `BIGINT` values arrive as JSON strings.
- Query response size is not bounded by the worker, so SQL must enforce result and work limits before projection.

Sources: [query handler](https://github.com/iii-hq/workers/blob/database/v0.5.17/database/src/handlers/query.rs), [execute handler](https://github.com/iii-hq/workers/blob/database/v0.5.17/database/src/handlers/execute.rs), [transaction handler](https://github.com/iii-hq/workers/blob/database/v0.5.17/database/src/handlers/transaction.rs), [PostgreSQL codec](https://github.com/iii-hq/workers/blob/database/v0.5.17/database/src/driver/postgres.rs).

## Implementation Approach Options

### A. Extend `memory-store`

Add graph modules, a graph-specific port, and a second migration to the memory crate.

- Benefits: reuses target validation, iii request patterns, and existing integration fixtures.
- Costs: combines mutable graph state with immutable memory history and increases coupling to MCP test doubles.
- Estimate: XL effort, high risk.

### B. Create `knowledge-graph-store`

Add independent contracts, normalization, service, database adapter, migrations, and graph verifiers.

- Benefits: preserves existing component responsibilities and gives graph mutations one schema owner.
- Costs: repeats some iii envelope and row-decoding logic and requires new workspace, Nix, CI, fake, and verifier wiring.
- Estimate: XL effort, medium risk.

### C. New Graph Store Plus Shared Database Bridge

Create the graph store and extract common iii database request/response behavior from existing code.

- Benefits: clean graph ownership with less adapter duplication.
- Costs: expands scope into a known duplication pressure point before graph and memory response contracts prove a stable shared abstraction.
- Estimate: XL effort, medium-high risk.

## Design Inputs

- Keep graph state in a dedicated crate and migration owner unless design validation finds a concrete shared abstraction.
- Define alias algorithm order, ICU versions, persisted key compatibility, and upgrade revalidation.
- Define one generated traversal state as the logical work unit and combine a `max_work + 1` sentinel with statement timeout.
- Define atomic SQL outcomes for identical replay, semantic conflict, stale revision, referenced deletion, and successful mutation without parsing dependency messages.
- Keep memory and session source references polymorphic and opaque; do not add cross-owner foreign keys.
- Use restrictive foreign keys for concepts and sources, with cascade only from concept to aliases and assertion to evidence.

## Design Decisions

### Dedicated Graph Store

- Selected a new `knowledge-graph-store` crate and migration owner.
- Extending memory-store would mix mutable graph state with immutable memory history; extracting a shared database bridge is deferred until two stable contracts justify it.

### Store-Generated Concept Identity

- Selected Rust-generated UUIDv7 IDs for concepts and assertions; no database UUID extension is added.
- Concept create idempotency compares the complete normalized alias-key set and preferred key. Equivalent display spellings return the existing concept and preserve its stored display text.
- Source references preserve typed memory/session identities; the 2,048-byte limit and PostgreSQL index constraint are specified in `design.md`.

### Versioned ICU4X Alias Keys

- Selected ICU4X 2.3.0 NFKC plus full non-Turkic case folding and persisted normalized keys.
- Compatibility fixtures and a stored algorithm version make Unicode upgrades an explicit migration rather than an implicit uniqueness change.

### Relational Assertions and Opaque Sources

- Selected normalized concepts, aliases, source references, mentions, assertions, and evidence instead of property-graph or JSONB documents.
- External records remain typed opaque references because the source schemas have separate owners and canonical session records do not yet exist.

### Migration-Owned Mutation Routines

- Selected static calls to narrowly granted mutation routines for atomic multi-row updates and typed conflict outcomes.
- Direct application DML and parsing database diagnostic text are rejected.

### Bounded Recursive Reads

- Selected static recursive reads with one generated state as the work unit, a `max_work + 1` sentinel, and worker plus database timeout bounds.
- Closure tables and iterative stored traversal are deferred because current requirements cap depth and do not require unrestricted algorithms.

## Design Validation Findings

- Database worker errors reach callers as `iii_sdk::Error::Remote` with outer code `invocation_failed`; `message` contains `handler error: ` followed by the serialized worker error. Timeout mapping therefore requires literal-prefix removal and strict inner JSON decoding.
- Complete evidence projection needs a stored evidence cap and a complete-response byte budget because database worker 0.5.17 materializes every returned row. The design caps evidence at 32 and returns one payload or one 4 MiB bound marker.
- Cross-aggregate uniqueness swaps can deadlock when routines lock rows before uniqueness keys. The design hashes all aggregate plus old/new uniqueness keys, deduplicates and sorts advisory integers before acquisition, acquires row locks afterward, and returns conflict on snapshot drift without adding locks.
- Database worker native row capture can expose graph keys. Statement capture or disabled row-change publishing is a deployment prerequisite for the graph target.
