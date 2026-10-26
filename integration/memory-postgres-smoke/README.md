# Memory PostgreSQL Extension Preflight

Task 1.2 packages PostgreSQL 17 and 18 with `pg_textsearch` 1.4.0 and
pgvector 0.8.6 or later. The locked Nix input provides the pin; the named
outputs keep the two server versions separate.

Build both extension-enabled server packages:

```sh
nix build --no-link .#postgresql17-search .#postgresql18-search
```

Run each verifier independently:

```sh
nix build --print-build-logs --no-link .#postgresql17-search-verify
nix build --print-build-logs --no-link .#postgresql18-search-verify
```

After an initial successful build, add `--rebuild` to either command to force a
new disposable verification run.

Each verifier creates a disposable local cluster, confirms that reloading the
configuration does not activate `pg_textsearch`, restarts the server, and then
checks that `shared_preload_libraries` contains only `pg_textsearch`. It creates
`pg_textsearch` and `vector` only in the disposable `memory_extension_smoke`
database and prints the exact server, installed-extension, and default-extension
versions. The verifier package runs its disposable cluster in Nix's build
context and selects `mmap` for main and dynamic shared memory.

The verifier does not create application or migration roles, schemas, tables,
or grants. It does not install or upgrade extensions in application code or
migrations. The verifier outputs are manual packages rather than flake checks,
so they do not make PostgreSQL startup an unconditional cross-platform check.
The isolated PostgreSQL fixture and role details are described below.

## Database Fixture

The memory fixture creates a migration identity and a restricted application
identity. Run either
verifier to start an extension-ready private-socket fixture, validate its roles,
then stop and remove it:

```sh
nix build --print-build-logs --no-link .#postgresql17-search-fixture-verify
nix build --print-build-logs --no-link .#postgresql18-search-fixture-verify
```

The fixture creates `memory_postgres_smoke`, owned by `memory_migration`, and
connects callers as `memory_application`. It installs `pg_textsearch` and
`vector` only in that database, grants the application role only `CONNECT`, and
leaves application schemas, tables, table grants, and default privileges to the
schema migration.

```sh
nix build --print-build-logs --no-link .#postgresql17-session-post-processing-verify
nix build --print-build-logs --no-link .#postgresql18-session-post-processing-verify
```

## Memory Storage

Task 2.3 applies the memory storage migration in one transaction as the
migration role, then verifies canonical and embedding constraints, grants, and
schema exclusions against each supported PostgreSQL release:

```sh
nix build --print-build-logs --no-link .#postgresql17-memory-storage-verify
nix build --print-build-logs --no-link .#postgresql18-memory-storage-verify
```

## Current Search Heads

Task 2.4 verifies the protected current-version projection, English BM25 index,
application permissions, and source-insert rollback when the projection fails:

```sh
nix build --print-build-logs --no-link .#postgresql17-memory-search-heads-verify
nix build --print-build-logs --no-link .#postgresql18-memory-search-heads-verify
```

## Current Head Recency

The head-recency migration (`0002_memory_search_head_recency.sql`) records the
selected canonical memory's update time on each current head, backfills the
exact selected versions, replaces the secured head-maintenance trigger, and
validates the non-null column. The verifier seeds a baseline, proves that an
unbounded maintenance window and both a lock timeout and a statement timeout
roll the migration back with the baseline unchanged, then applies the migration
with finite limits and checks backfill, greater-version advancement,
lower-version stability, privileges, and old-reader compatibility.

Run it directly with the existing Git-backed server packages and fixture setup:

```sh
postgresql17="$(nix build --no-link --print-out-paths .#postgresql17-search)"
MEMORY_SCHEMA_MIGRATION="$PWD/crates/memory-store/migrations/0001_memory_schema_search.sql" \
MEMORY_RECENCY_MIGRATION="$PWD/crates/memory-store/migrations/0002_memory_search_head_recency.sql" \
  bash integration/memory-postgres-smoke/scripts/setup.sh "$postgresql17/bin" 17 -- \
  bash integration/memory-postgres-smoke/scripts/verify-memory-head-recency.sh

postgresql18="$(nix build --no-link --print-out-paths .#postgresql18-search)"
MEMORY_SCHEMA_MIGRATION="$PWD/crates/memory-store/migrations/0001_memory_schema_search.sql" \
MEMORY_RECENCY_MIGRATION="$PWD/crates/memory-store/migrations/0002_memory_search_head_recency.sql" \
  bash integration/memory-postgres-smoke/scripts/setup.sh "$postgresql18/bin" 18 -- \
  bash integration/memory-postgres-smoke/scripts/verify-memory-head-recency.sh
```

## Context Recency Index

The context recency index migration
(`0003_memory_context_recency_index.sql`) builds the ordered B-tree
`(updated_at DESC, id COLLATE "C" ASC, version DESC)` used by the bounded
startup-context read. It is standalone and must run outside every transaction
wrapper, after `0002` has committed and memory writers have resumed:

```sh
psql --no-psqlrc --no-password --set=ON_ERROR_STOP=1 \
  -f crates/memory-store/migrations/0003_memory_context_recency_index.sql
```

Do not add `--single-transaction` or wrap the file in `BEGIN`/`COMMIT`;
PostgreSQL refuses `CREATE INDEX CONCURRENTLY` inside a transaction block. A
cancelled or failed concurrent build leaves an invalid index. Recovery is
explicit: drop only that invalid index concurrently, then rerun the same
migration. The migration never replaces an existing index by itself.

```sh
psql --no-psqlrc --no-password --set=ON_ERROR_STOP=1 \
  -c 'DROP INDEX CONCURRENTLY IF EXISTS public.memory_search_heads_context_recency_idx'
```

The verifier seeds resumed writers, rejects a transaction-wrapped build, leaves
a simulated failed concurrent build invalid, repairs it through the documented
drop-and-rerun recovery, and proves an application insert still commits while
the build waits on an older snapshot. It also checks the exact index
definition, ordering, validity, forced ordered plan, privileges, old-reader
compatibility, and that `0002` remains transactional and non-null.

```sh
postgresql17="$(nix build --no-link --print-out-paths .#postgresql17-search)"
MEMORY_SCHEMA_MIGRATION="$PWD/crates/memory-store/migrations/0001_memory_schema_search.sql" \
MEMORY_RECENCY_MIGRATION="$PWD/crates/memory-store/migrations/0002_memory_search_head_recency.sql" \
MEMORY_CONTEXT_INDEX_MIGRATION="$PWD/crates/memory-store/migrations/0003_memory_context_recency_index.sql" \
  bash integration/memory-postgres-smoke/scripts/setup.sh "$postgresql17/bin" 17 -- \
  bash integration/memory-postgres-smoke/scripts/verify-memory-context-recency-index.sh

postgresql18="$(nix build --no-link --print-out-paths .#postgresql18-search)"
MEMORY_SCHEMA_MIGRATION="$PWD/crates/memory-store/migrations/0001_memory_schema_search.sql" \
MEMORY_RECENCY_MIGRATION="$PWD/crates/memory-store/migrations/0002_memory_search_head_recency.sql" \
MEMORY_CONTEXT_INDEX_MIGRATION="$PWD/crates/memory-store/migrations/0003_memory_context_recency_index.sql" \
  bash integration/memory-postgres-smoke/scripts/setup.sh "$postgresql18/bin" 18 -- \
  bash integration/memory-postgres-smoke/scripts/verify-memory-context-recency-index.sh
```

### Verifier Sensitivity

The negative fixtures under `integration/memory-postgres-smoke/fixtures/` prove
that the verifier fails when the migration does not keep the required
transactional rejection and explicit-recovery behavior. Point
`MEMORY_CONTEXT_INDEX_MIGRATION` at a fixture instead of `0003` and run the same
fixture command:

- `context-recency-index-transactional.sql` must exit non-zero with
  `expected transaction-wrapped concurrent index rejection`.
- `context-recency-index-auto-replace.sql` must exit non-zero with
  `expected invalid-index collision rejection`.

The checked-in `0003` migration exits zero against both PostgreSQL majors.

## Embedding Lifecycle

Task 5.2 verifies committed parent-before-child ordering, historical and
post-head embedding inserts, immutable canonical/head state, and deterministic
concurrent duplicate association handling:

```sh
nix build --print-build-logs --no-link .#postgresql17-memory-embedding-lifecycle-verify
nix build --print-build-logs --no-link .#postgresql18-memory-embedding-lifecycle-verify
```

## Embedding Work Queries

The embedding-work verifier executes the production-shaped exact-key,
missing-work, and immutable-insert SQL against a disposable fixture. It checks
pending/already-present/missing classification, all-version C-collated bounded
selection, stateless reselection, and a controlled queue/reconciliation insert
race without adding a migration or index.

Run the filtered-source verifier packages on both supported PostgreSQL majors:

```sh
nix build --print-build-logs --no-link .#postgresql17-memory-embedding-work-verify
nix build --print-build-logs --no-link .#postgresql18-memory-embedding-work-verify
```

Run it directly with the existing Git-backed server packages and fixture setup:

```sh
postgresql17="$(nix build --no-link --print-out-paths .#postgresql17-search)"
MEMORY_SCHEMA_MIGRATION="$PWD/crates/memory-store/migrations/0001_memory_schema_search.sql" \
  bash integration/memory-postgres-smoke/scripts/setup.sh "$postgresql17/bin" 17 -- \
  bash integration/memory-postgres-smoke/scripts/verify-memory-embedding-work.sh

postgresql18="$(nix build --no-link --print-out-paths .#postgresql18-search)"
MEMORY_SCHEMA_MIGRATION="$PWD/crates/memory-store/migrations/0001_memory_schema_search.sql" \
  bash integration/memory-postgres-smoke/scripts/setup.sh "$postgresql18/bin" 18 -- \
  bash integration/memory-postgres-smoke/scripts/verify-memory-embedding-work.sh
```

## Memory Version Heads

Task 5.3 verifies initial, newer, lower, duplicate, concurrent, and
trigger-failing memory versions. It asserts immutable full canonical snapshots,
greatest-version current heads, and transactional rollback on both releases:

```sh
nix build --print-build-logs --no-link .#postgresql17-memory-version-heads-verify
nix build --print-build-logs --no-link .#postgresql18-memory-version-heads-verify
```

## MCP Memory History

The verifier seeds a native multi-version family in the disposable PostgreSQL
fixture, starts the pinned database worker, and calls the production MCP server
backed by its real memory-store and retrieval adapters. It checks latest and
exact lookup, pagination across history, second-precision MCP timestamps over
microsecond-preserving PostgreSQL rows, and search visibility for current heads
on both supported majors.

Run the verifier with PostgreSQL 17:

```sh
postgresql17="$(nix build --no-link --print-out-paths .#postgresql17-search)"
nix develop --command env \
  MEMORY_SCHEMA_MIGRATION="$PWD/crates/memory-store/migrations/0001_memory_schema_search.sql" \
  bash integration/memory-postgres-smoke/scripts/setup.sh "$postgresql17/bin" 17 -- \
  bash integration/memory-postgres-smoke/scripts/verify-memory-history-mcp.sh
```

Repeat the same command with `.#postgresql18-search` and major `18` for
PostgreSQL 18. The first run may fetch the public database worker package used
by iii Compose; all memory rows are fixed synthetic fixtures.

Canonical PostgreSQL rows retain seeded microseconds; MCP memory DTOs
intentionally normalize timestamps to whole seconds. The native fixture uses
`native-memory-history` and includes searchable current and historical versions.

## BM25 Semantics

Task 5.4 verifies direct PostgreSQL BM25 behavior against the current-head
projection: title, content, and concept matches; empty results; positive
scores; deterministic `C`-collated ties and limits; current-only corpus
statistics; and JSONB result projections without embeddings.

```sh
nix build --print-build-logs --no-link .#postgresql17-memory-bm25-verify
nix build --print-build-logs --no-link .#postgresql18-memory-bm25-verify
```

## Vector Semantics

Task 5.5 verifies the direct PostgreSQL exact-cosine query: current-head joins,
latest-without-embedding suppression, post-head embedding visibility,
dimension guards, zero vectors, deterministic `C`-collated ties and limits, and
embedding-free JSONB result projections.

```sh
nix build --print-build-logs --no-link .#postgresql17-memory-vector-verify
nix build --print-build-logs --no-link .#postgresql18-memory-vector-verify
```
