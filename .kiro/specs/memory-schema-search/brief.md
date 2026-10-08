# Brief: memory-schema-search

## Problem
The database has no canonical memory record or database search capability. Memories need versioned persistence with BM25 and vector retrieval over their searchable content.

## Current State
Harness ingestion defines and publishes lifecycle and observation events. The adjacent event-persistence spec owns an append-only raw-event ledger and explicitly defers downstream memory processing, vector indexing, and retrieval.

## Desired Outcome
PostgreSQL stores versioned memories under a composite `(id, version)` primary key and stores each optional embedding in a separate record keyed to that memory version. Database operations can write memories and embeddings independently, then search the latest version of each memory by BM25 or exact cosine similarity while returning its provenance metadata.

## Approach
Add a canonical memory table containing only the requested memory fields and a separate pgvector embedding table keyed by `(id, version)`. Use `pg_textsearch` to index a search document formed from title, content, and concepts. Keep vector search exact and dimension-independent; route BM25 queries to the primary PostgreSQL instance.

## Scope
- **In**: Memory and embedding schemas and migrations; ID/version invariants; type, title, content, timestamps, concepts, files, session IDs, source observation IDs, separately keyed embedding storage; independent memory and embedding writes; latest-version selection; parameterized BM25 and exact cosine search operations; database result decoding; focused PostgreSQL integration verification.
- **Out**: Observation interpretation, memory generation, embedding generation, public or iii search functions, hybrid score fusion, query filters beyond latest-version selection, approximate vector indexes, deletion, retention, and database provisioning.

## Boundary Candidates
- Canonical memory persistence and version invariants.
- PostgreSQL-owned BM25 and vector search operations.
- Deployment-provided extension and migration prerequisites.

## Out of Boundary
- Changes to harness ingestion or raw-event persistence contracts.
- Memory type taxonomy beyond storing a non-empty type value.
- Deriving concepts, files, session IDs, or source observation IDs.
- Installing PostgreSQL, `pg_textsearch`, or pgvector; configuring `shared_preload_libraries`; running migrations; or generating embeddings.
- Phrase search, per-field BM25 boosts, strict tenant isolation through row-level security, and BM25 reads from hot standbys.

## Upstream / Downstream
- **Upstream**: A future memory processor supplies canonical memory fields; a future embedding producer supplies an optional embedding for an existing memory version; the raw-event ledger supplies source observations referenced by ID.
- **Downstream**: Future retrieval APIs can call the database search operations and may add filters, pagination, score fusion, or result presentation.

## Existing Spec Touchpoints
- **Extends**: None.
- **Adjacent**: `harness-event-persistence-worker` owns immutable source-event storage and optional session embeddings; `harness-ingestion-worker` owns the authoritative harness event contract.

## Constraints
- PostgreSQL 17 or 18 must preload deployment-provided `pg_textsearch` 1.4.0 and provide pgvector 0.8.6 or later.
- BM25 queries run on the primary database. The deployment accepts known write-pressure limits under sustained ranked-search concurrency.
- Database access uses static, parameterized SQL through iii database worker `database::execute`; vector values cross that boundary through explicit text casts.
- Memory timestamps use UTC whole-second precision and four-digit AD years `0001` through `9999` so the iii timestamp representation remains lossless for the contract.
- Each memory version accepts at most one immutable embedding row. Embedding writes require an existing `(id, version)` and do not mutate the canonical memory row.
- Search returns only the latest version for each memory ID. Historical versions remain stored under the composite primary key.
- The feature follows existing ports-and-adapters, numbered-migration, typed-error, and opaque-content logging conventions.
