---
updated_at: 2026-09-21
---

# Technology

## Runtime Stack

The production system is a Rust workspace built on the iii engine and SDK.
Tokio provides the async runtime. PostgreSQL stores raw events and canonical
memories; `pg_textsearch` supplies BM25 search and pgvector supplies vector
types and exact similarity operations.

The workspace targets Rust 2024 with the minimum Rust version declared at the
workspace root. Dependencies are exact-version pinned, packages inherit shared
versions, and `Cargo.lock` is committed. Workspace packages are private unless a
distribution decision explicitly changes that policy.

Nix is the authoritative development and CI environment. Use `nix develop` for
the Rust, protobuf, iii, formatting, linting, and dependency-policy toolchain.
The flake must remain usable on its declared Linux and Darwin architectures.

## Architectural Conventions

### iii Boundaries

Workers register typed iii functions or triggers, retain registration handles,
and verify readiness against registrations owned by the current process. Do not
treat a matching global catalog name as proof that this worker is ready.

External effects sit behind small async traits so domain behavior can be tested
without a live engine or database. Application services validate first and call
their injected port only after inputs satisfy the domain contract.

### Protocol Contracts

Harness event messages originate from the versioned protobuf hierarchy under
`proto/`. Prost and Pbjson generate Rust and ProtoJSON code into `OUT_DIR`;
generated artifacts are not committed.

Preserve proto field names in JSON and emit default fields. Observation data is
an opaque protobuf `Struct`; code must account for its JSON-number semantics
rather than assuming lossless arbitrary-size integers.

Serde request types at trust boundaries normally reject unknown fields. iii
function schemas derive from typed contracts, while MCP tool schemas are
explicit protocol artifacts and require snapshot-level compatibility tests.

### Persistence

Keep migrations with the component that owns the schema. Database calls use
static SQL and positional parameters through the iii database function; never
interpolate user content into SQL.

Raw events are append-only delivery records. Duplicate source events may produce
multiple ledger entries unless a future contract explicitly introduces an
idempotency key.

Canonical memories use immutable `(id, version)` rows. Embeddings are child
records, and the database-maintained search head selects the greatest version
for each memory identity. Preserve deterministic ordering in all search paths.

Apply least privilege to migration and application roles. Security-definer
database functions or triggers require a fixed search path and restricted
execute privileges.

## Configuration

Production configuration is environment-driven. Each component should expose a
pure constructor such as `from_values` beneath `from_env`, allowing validation
tests without mutating process-global environment state.

Validate required values before registering functions, opening protocol loops,
or calling backends. Be deliberate about whitespace semantics: trim values only
when the external contract defines trimming, and test empty and whitespace-only
cases separately.

Stdout is protocol-owned for stdio services. Human-readable lifecycle and error
messages go to stderr and should remain stable, concise, and free of payloads.

## Error and Privacy Rules

- Return typed domain errors internally and opaque errors at external trust
  boundaries.
- Do not expose SDK messages, SQL, backend responses, credentials, stack traces,
  embeddings, or observation contents in errors or logs.
- Give sensitive identifiers and keys redacted `Debug` behavior, and test both
  `Display` and `Debug` with sentinel values.
- Invalid input must not reach queues, databases, or memory operations.
- Prefer fixed public diagnostics over propagating arbitrary dependency text.

## Async and Process Behavior

Bound externally controlled resources: input line size, queues, in-flight work,
timeouts, and shutdown waits. Use backpressure rather than unbounded task
spawning. When output is shared, serialize writes so protocol frames cannot
interleave.

Signal handling and shutdown are part of component behavior. Stop admitting new
work, release registrations, drain already admitted work within the defined
contract, and reap child processes used by integration tests.

## Testing Strategy

Use the narrowest test that proves the contract:

- Colocated unit tests cover private validation and transformations.
- Package integration tests exercise public APIs and binaries as external
  consumers.
- Recording or failing trait implementations assert exact calls and prove that
  invalid input causes no backend activity.
- Loopback protocol fakes exercise iii registration, invocation, database
  envelopes, process lifecycle, and privacy without production services.
- Direct PostgreSQL verifiers exercise migrations, privileges, query behavior,
  rollback, and deterministic ordering against supported major versions.
- Exact snapshots protect generated JSON, schemas, manifests, migrations, and
  other compatibility-sensitive artifacts.

Tests for wire boundaries should assert both accepted shapes and rejection of
unknown, malformed, oversized, or privacy-sensitive inputs.

## Quality Gates

Before merging, the workspace is expected to pass:

```sh
nix flake check
nix develop --command cargo test --workspace --locked
nix develop --command cargo fmt --all --check
nix develop --command cargo clippy --workspace --all-targets --locked -- -D warnings
nix develop --command cargo deny --locked check licenses advisories
```

Run the affected release builds, protocol fakes, and PostgreSQL verifiers when a
change crosses those boundaries. CI is the executable source of truth for the
complete required matrix.
