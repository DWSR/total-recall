# Research: memory-schema-search

## Current State

- The Rust workspace uses edition 2024, Rust 1.98.1, exact dependency pins, ports-and-adapters, typed content-opaque errors, and external crate tests.
- `workers/harness-ingestion` is implemented. It provides reusable validation, iii adapter, and recording/failing test patterns.
- `harness-event-persistence-worker` is approved but unimplemented. Its planned `database::execute` adapter and migrations are conventions, not reusable code.
- No memory model, database crate, migration runner, PostgreSQL test service, BM25 support, vector query, or search API exists.
- No `.kiro/steering/` directory provides additional architecture constraints.

## Requirement-to-Asset Map

| Area | Existing asset | Gap |
|---|---|---|
| Memory contract | Strict DTO and value-object patterns | **Missing:** model, collection/timestamp/vector validation, schema |
| Version persistence | Planned static-SQL database-worker convention | **Missing:** composite key, insert-only writes, duplicate mapping, latest-version state |
| BM25 | Selected `pg_textsearch` deployment constraint | **Missing:** search document, index, query, score decoding |
| Vector search | Prior vector text-cast research | **Missing:** one-to-one embedding relation, independent write, guarded cosine SQL, validation, score decoding |
| Results/errors | Typed opaque errors and leak tests | **Missing:** row decoding, embedding-free projection, complete-or-error assembly |
| Verification | Workspace tests and fake-engine convention | **Missing:** live extension-backed PostgreSQL tests and CI provisioning |

## PostgreSQL Search Findings

- `pg_textsearch` 1.4.0 supports PostgreSQL 17/18, `text` and `text[]` BM25 indexes, partial indexes, transactions, WAL, and rollback.
- It requires a matching server binary, `shared_preload_libraries`, restart, and `CREATE EXTENSION` before migrations use it.
- A cross-version generated search document can use immutable array operations: `ARRAY[title, content] || COALESCE(concepts, ARRAY[]::text[])`. `concat_ws` and `array_to_string` are not immutable.
- A partial BM25 index can index rows marked current. A partial-index predicate cannot compute `MAX(version)` or reference another table.
- Filtering latest rows after scoring an all-version index leaves historical rows in document count, average length, and IDF statistics. Historical rows can also consume top candidates.
- A current-row marker supports latest-only corpus statistics but requires atomic state changes during version insertion.
- A separate current-search table preserves an immutable history table and latest-only statistics at the cost of duplicated current content.
- `pg_textsearch` optimizes a query with one BM25 ordering expression. Adding ID/version tie-breakers disables that top-K path. Strict deterministic ordering therefore trades away that optimization unless upstream behavior changes.
- BM25's `<@>` operator returns negative relevance; lower values rank first. The application can expose a normalized positive relevance value.

## Vector and Database-Worker Findings

- A dimensionless pgvector `vector` column supports mixed dimensions; exact distance operators reject unequal dimensions.
- Safe mixed-dimension cosine SQL must guard distance evaluation with dimension, finite-value, and non-zero-norm checks rather than relying on predicate order.
- pgvector cosine distance uses `<=>`; exact similarity is `1 - distance`. No HNSW or IVFFlat index is required.
- iii database worker 0.5.17 binds JSON arrays as JSON/JSONB, not PostgreSQL arrays. SQL must convert collection parameters to native arrays.
- The worker does not decode PostgreSQL arrays or extension types directly. Queries must project arrays as JSON and vectors as text when vectors are returned.
- `database::execute` autocommits one statement. Multi-statement atomic work requires `database::transaction`/`executeBatch` or one atomic SQL statement.
- Transaction rollback can return `committed: false` without an invocation error; adapters must inspect the returned outcome.
- Live tests are required to validate planner hooks, custom types, casts, result codecs, and extension versions. Mocks and static SQL assertions cannot prove ranking behavior.

## Implementation Options

### Extend the future event-persistence worker

- Reuse one database adapter and migration location.
- Conflicts with that spec's approved exclusion of memory processing and retrieval.
- Blocked until its unimplemented component exists.

### Create an independent memory-store crate

- Own contracts, validation, migrations, static operations, and database-worker adapter under `crates/memory-store/`.
- Preserves adjacent worker boundaries and can be consumed by a later memory processor.
- Introduces the repository's first shared domain crate and an independent migration lifecycle.

### Split core and adapter

- Put contracts, validation, migrations, and query construction in `crates/memory-store/`; place the iii adapter in a future caller.
- Maximizes reuse but leaves adapter ownership unresolved in this feature.

## Design Research Carry-Forward

- Choose current-row representation: mutable internal marker or duplicated current-search table.
- Choose deterministic BM25 ordering versus optimized top-K execution; benchmark if performance is a release criterion.
- Define ID SQL type, timestamp precision, collection ordering/duplicate semantics, score representation, and maximum result limit.
- Confirm source observation ID representation without adding ownership or foreign-key coupling to the raw ledger.
- Prove concurrent and out-of-order version insertion against latest-row invariants.
- Prove parent/embedding commit ordering and concurrent duplicate-embedding behavior.
- Confirm iii casts, JSON projections, BM25 queries, and mixed-dimension cosine guards through PostgreSQL 17 and 18 integration tests.
- Provision extension-enabled test infrastructure without making runtime workers install extensions or run migrations.

## Complexity

- **Effort:** L (1–2 weeks). The feature adds a new storage boundary, two extension-backed query paths, and live integration verification.
- **Risk:** High until latest-row indexing, deterministic ranking, mixed-dimension vectors, and CI extension provisioning are validated; Medium after those proofs.

## Design Synthesis

- Use an independent `memory-store` crate rather than widening either harness worker.
- Keep canonical memory versions immutable and maintain one trigger-updated current-search projection per ID.
- Adopt `pg_textsearch` and pgvector instead of building ranking implementations.
- Prefer deterministic requirement compliance over the BM25 top-K optimization; retain a benchmark/revalidation trigger for future scale work.
- Expose one in-process Rust service and one database port. Do not add protobuf, an iii function, a runtime worker, hybrid ranking, or speculative filters.
- Use one shared search-result contract while keeping BM25 and vector query contracts distinct.
- Split verification into direct PostgreSQL extension tests and an iii engine protocol fake; a live database-worker topology remains deployment-owned.
- Secure the projection trigger with migration-owner execution and a fixed trusted search path.

---

## Separate Embedding Storage Delta

- The canonical memory row no longer carries a vector. A memory write and its optional embedding write are independent operations.
- A one-to-one embedding relation can use `(id, version)` as both its primary key and a restrictive composite foreign key to canonical memory history.
- Historical versions may receive embeddings, but vector search still joins through the current head. A latest version without an embedding suppresses older embedded versions.
- The embedding primary key makes concurrent duplicate submissions deterministic: one insert succeeds and later/conflicting inserts fail without replacement.
- Parent and embedding writes require commit ordering. An embedding submitted before its memory version is committed fails the foreign-key contract; no cross-operation transaction is introduced.
- Memory insertion no longer crosses iii with a vector parameter. Embedding insertion owns the explicit text-to-vector cast and returned-key confirmation.
- Exact vector search joins current head → embedding → canonical memory and retains the evaluation-safe dimension/norm guard.
- Application privileges add insert/select access for the embedding relation while continuing to deny update/delete and direct head mutation.
- The adjacent `session_embeddings` table is not reusable: it is session-keyed, revision-oriented, and has different ownership.
- Additional verification is required for missing parents, duplicate embeddings, invalid vectors, historical embeddings, latest versions without embeddings, post-head embedding insertion, and parent/child races.

## Sources

- [pg_textsearch 1.4.0 README](https://github.com/timescale/pg_textsearch/blob/v1.4.0/README.md)
- [pg_textsearch partial-index tests](https://github.com/timescale/pg_textsearch/blob/v1.4.0/test/sql/partial_index.sql)
- [pg_textsearch limit tests](https://github.com/timescale/pg_textsearch/blob/v1.4.0/test/sql/limits.sql)
- [PostgreSQL generated columns](https://www.postgresql.org/docs/18/ddl-generated-columns.html)
- [PostgreSQL CREATE INDEX](https://www.postgresql.org/docs/18/sql-createindex.html)
- [pgvector 0.8.6 README](https://github.com/pgvector/pgvector/blob/v0.8.6/README.md)
- [iii database worker API](https://workers.iii.dev/workers/database?tab=api)
- [iii PostgreSQL driver 0.5.17](https://github.com/iii-hq/workers/blob/database/v0.5.17/database/src/driver/postgres.rs)
