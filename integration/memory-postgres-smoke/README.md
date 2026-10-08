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
