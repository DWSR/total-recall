# Research and Gap Analysis

## Scope
This analysis compares the approved requirements with the implemented ingestion worker and the iii queue/database interfaces.

## Current-State Findings
- The repository has a Rust workspace with `harness-ingestion` and `harness-engine-fake` members, exact shared dependency pins, CI, and a lockfile.
- `harness-ingestion` implements the publisher, crate-local Prost/pbjson generation from the root `.proto`, strict request adapters, managed iii runtime, external tests, and an in-process engine protocol fake.
- The authoritative queued events contain `session_start`, `observation`, and `session_end` variants. They contain no event ID or sequence.
- No persistence crate, database schema, database-worker adapter, queue subscription, or persistence protocol fake exists.
- The ingestion spec's implementation subtasks are checked complete; its final validation task and metadata remain open despite CI coverage.

## Requirement-to-Asset Map

| Requirements | Existing asset | Gap |
| --- | --- | --- |
| 1.1–1.3 | Implemented managed runtime, registration plan, catalog polling, and process lifecycle | **Missing:** persistence function, retained subscriber handle, registered-trigger readiness, database configuration |
| 2.1–2.6 | Root `.proto`, crate-local generation, public generated events, and strict publisher adapters | **Missing:** strict consumer event adapter and epoch-millisecond derivation; production types remain crate-local |
| 3.1–3.7 | None | **Missing:** append-only mapping, UUIDv7 keys, chronological indexes, SQL, migrations, duplicate and out-of-order behavior |
| 4.1–5.4 | Implemented generated event serializers and fixtures | **Missing:** lifecycle and raw-observation tables and parameterized inserts |
| 6.1–6.5 | Implemented iii invocation adapter pattern | **Missing:** database execute adapter; database success and queue acknowledgment remain non-atomic |
| 7.1–7.3 | PostgreSQL database-worker support | **Unknown:** deployment ownership for pgvector installation; embedding dimensions and model are future inputs |
| 8.1–8.4 | External crate tests, recording/failing ports, protocol fake, and CI | **Missing:** persistence fixtures, SQL assertions, subscriber/database fake flow, and CI steps |

## External Interface Findings

### Queue worker 0.21.13
- `durable:subscriber` binds a queue or topic to a target function. Normal return acknowledges; function error rejects the delivery.
- Delivery is at-least-once. Default concurrency allows reordering; FIFO limits in-flight work but a retried message can still be overtaken.
- The subscriber payload is the published `data`; queue message IDs are not exposed to the function.
- Retries and durability depend on the queue backend. No application-level no-loss claim is supportable.
- Rust's convenience `IIITrigger::Queue` does not represent this trigger; registration uses `RegisterTriggerInput`.

Sources: [queue worker](https://workers.iii.dev/workers/queue), [trigger source](https://github.com/iii-hq/workers/blob/queue/v0.21.13/queue/src/trigger.rs), [store source](https://github.com/iii-hq/workers/blob/queue/v0.21.13/queue/src/store.rs).

### Database worker 0.5.17
- `database::execute` performs parameterized backend-native SQL and returns affected rows after a successful invocation.
- `database::transaction` and `database::executeBatch` execute atomic ordered batches. SQL failures may return `committed: false` inside a successful iii invocation, so callers must inspect the result.
- PostgreSQL placeholders use `$1`, `$2`, and so on. Object and array parameters bind as JSON.
- The worker supports PostgreSQL URLs but provides no SQL dialect translation.

Sources: [database worker](https://workers.iii.dev/workers/database?tab=api), [value binding](https://github.com/iii-hq/workers/blob/database/v0.5.17/database/src/value.rs), [transaction handler](https://github.com/iii-hq/workers/blob/database/v0.5.17/database/src/handlers/transaction.rs).

### pgvector
- pgvector is a PostgreSQL extension and remains deployment-provided.
- The database worker has no native vector parameter type. Future writers can bind the textual vector form and use an explicit PostgreSQL cast.
- A vector index is not required for storage. Omitting it avoids index maintenance on unrelated event inserts.
- A dimensionless `vector` column can defer dimension selection; indexed future use must constrain compatible dimensions.

Source: [pgvector](https://github.com/pgvector/pgvector).

### UUIDv7 receipt keys
- `pg_uuidv7` release 1.7.0 installs extension version `1.7` and provides `uuid_generate_v7()` for PostgreSQL 13–18.
- The generator is volatile and suitable as a per-row default. It uses millisecond wall-clock time plus strong random bits and sets RFC 9562 version and variant bits.
- The extension is untrusted native C code and normally requires superuser or managed-provider approval to install. This spec treats installation as a deployment prerequisite.
- UUIDv7 improves B-tree locality over UUIDv4 but doubles key width relative to `BIGINT`. Values expose approximate creation time and are not strictly ordered within one millisecond or across clock rollback.

Sources: [`pg_uuidv7` 1.7.0](https://github.com/fboulnois/pg_uuidv7/tree/c707aae2411181be4802f5fa565b44d9c0bcbc29), [SQL declaration](https://github.com/fboulnois/pg_uuidv7/blob/c707aae2411181be4802f5fa565b44d9c0bcbc29/sql/pg_uuidv7--1.7.sql), [RFC 9562](https://www.rfc-editor.org/rfc/rfc9562.html).

### Chronological session indexes
- Preserve the validated source timestamp as text and derive `TIMESTAMPTZ(3)` from its exact epoch milliseconds.
- The database worker binds epoch milliseconds as an integer; PostgreSQL derives the instant with epoch-plus-interval arithmetic, avoiding unsupported timestamp parameter binding.
- B-tree indexes on `(session_id ASC, source_timestamp_utc ASC)` support chronological scans across differing RFC 3339 offsets.
- `receipt_id` is not included. A covering column would enlarge the index and disable B-tree deduplication without a defined query need.

Sources: [PostgreSQL date/time types](https://www.postgresql.org/docs/17/datatype-datetime.html), [index ordering](https://www.postgresql.org/docs/17/indexes-ordering.html), [covering indexes](https://www.postgresql.org/docs/17/indexes-index-only-scans.html).

### Compatibility and licensing
- iii engine and Rust SDK 0.24.0 are matching releases. Queue 0.21.13 and database 0.5.17 run as separate workers built against iii-sdk 0.23; no exact compatibility matrix is published.
- The engine uses Elastic License 2.0. The SDK and workers use Apache-2.0, `pg_uuidv7` uses MPL-2.0, and this repository uses MIT. Deployment use is compatible; redistributed extension binaries require MPL notice and source obligations.
- Components are maintained but pre-1.0 and subject to interface churn.

## Reliability Gap
The database commit and queue acknowledgment cannot share one transaction. A process failure after commit but before acknowledgment can redeliver the same payload. Because the event contract has no unique event ID, the worker cannot distinguish redelivery from an intentionally identical event. Requirements therefore preserve each accepted delivery and make no exactly-once claim.

## Implementation Approach Options

### Option A: Extend the ingestion worker
Add subscription and database dependencies to the publisher crate. This minimizes crate count but combines harness-facing publication and downstream persistence lifecycles, and it conflicts with the adjacent spec's boundary.

### Option B: Create a sibling persistence worker
Add a worker crate that independently compiles the implemented root schema and owns its subscriber, SQL mapping, and database adapter. This preserves the selected boundary and uses existing workspace prerequisites, but duplicates a small contract-generation and validation surface.

### Option C: Sibling worker plus shared contract crate
Extract generated protobuf and ProtoJSON code into a workspace library consumed by both workers. This reduces duplicate build pipelines but enlarges the upstream task boundary and requires coordination before either worker can compile.

## Complexity and Risk
- **Effort:** M (3–7 days). Workspace and runtime patterns exist; subscriber readiness, migrations, and SQL contracts remain new.
- **Risk:** Medium. Local patterns reduce uncertainty; at-least-once delivery and external worker contract churn remain.

## Design Synthesis
- **Boundary**: Create a sibling worker. It compiles the root upstream-owned `.proto` independently and uses `harness-ingestion` only as a test oracle, avoiding a production worker-to-worker dependency or shared-library refactor.
- **Generalization**: One `EventStore::append` contract accepts the three-event domain enum; each event still maps to one concrete insert.
- **Database operation**: Adopt `database::execute` for one autocommitted statement per delivery. A successful result must report exactly one affected row; batch and interactive transactions add no value to a one-row append.
- **Table shape**: Use separate `session_events` and `raw_observations` tables with database-generated UUIDv7 primary keys, no foreign keys, no uniqueness on source data, and one required chronological secondary index per table. `post_tool_use` remains an observation hook value rather than a new event discriminator.
- **Opaque data**: Use PostgreSQL `json`, not `jsonb`, because the worker does not query the value and write conversion is the priority.
- **Time**: Store the original RFC 3339 timestamp as text, derive a millisecond UTC instant from the validated epoch value, and add a server-generated ingestion timestamp. UUID and ingestion time do not claim source order.
- **Embeddings**: Put the dimensionless pgvector column and model metadata in a separate optional migration. The required ledger migration neither installs nor depends on pgvector.
- **Readiness**: Register the function and trigger with a shared nonce, verify the function catalog, then query `engine::registered-triggers::list` and `engine::registered-triggers::info` for one active nonce-bearing binding. Rust iii-sdk 0.24.0 does not expose trigger-ack status through the returned handle; `engine::triggers::*` describes trigger types, not instances.
- **Platform floor**: Target PostgreSQL 17 or 18 with `pg_uuidv7` release 1.7.0 installed. PostgreSQL 18 core UUIDv7 is not used while 17 remains supported.

## Design Risks
- A database timeout has an unknown commit outcome; returning an error can produce a duplicate on redelivery.
- UUIDv7 primary keys and the matching table's chronological secondary index add key width, WAL, and index maintenance to each ledger write.
- The queue/database worker version combination lacks a published compatibility matrix; pin contract shapes in adapter tests.
- The optional embedding migration fails when pgvector is unavailable; ledger deployment remains independent.
- The required ledger migration fails when `pg_uuidv7` is unavailable; managed database support must be confirmed before deployment.

---

## Implemented Ingestion Refresh
- The workspace pins Rust 1.98.1, Tokio 1.48.0, UUID 1.18.1 with `v4`, and the exact Prost/pbjson/Serde/Schemars versions already selected by this spec.
- Ingestion's `build.rs` establishes the repository-root schema path, descriptor generation, `pbjson_types` mapping, default-field emission, and preserved proto names.
- `HarnessTimestamp` is public but does not expose its parsed instant. Persistence either needs a production dependency on the ingestion worker or a parity-tested local adapter; the latter preserves the sibling boundary.
- Exact parity means rejecting only empty session IDs, permitting other empty strings, accepting lowercase `t` and `z`, preserving source text, requiring millisecond precision, and retaining protobuf-double observation-number semantics.
- Runtime precedent uses `WorkerMetadata`, managed identity, `InitOptions.namespace = None`, retained function references, one UUIDv4 nonce, `wait_until_registered`, function-catalog ownership checks, one absolute deadline, and shutdown on startup failure.
- Function and database calls inherit the managed namespace. Subscriber target and provider namespaces remain explicit; engine catalog functions route through `default` with namespace filters in their payloads.
- Tests primarily live under the worker's `tests/` directory. `harness-engine-fake` provides the pattern for an in-process WebSocket protocol fake and CI smoke test without live iii or queue services.
- Persistence CI must add its release binary and fake flow; existing workspace test, format, Clippy, and flake commands already discover a new member.

Sources: [`Cargo.toml`](../../../Cargo.toml), [`build.rs`](../../../workers/harness-ingestion/build.rs), [`contracts.rs`](../../../workers/harness-ingestion/src/contracts.rs), [`runtime.rs`](../../../workers/harness-ingestion/src/runtime.rs), [`ci.yml`](../../../.github/workflows/ci.yml).
