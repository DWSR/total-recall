# Brief: harness-event-persistence-worker

## Problem
"Create a spec for a Rust iii worker that exposes a function for writing harness events from the queue worker and inserting them into a database worker. Design the table schema for sessions to optimize for writes, not reads."

The system has a specified harness-facing publisher but no downstream worker or database contract that durably records queued lifecycle and observation events.

## Current State
The repository now implements `harness-ingestion` in a pinned Rust workspace. It owns the root protobuf schema, crate-local ProtoJSON generation, strict harness adapters, managed iii runtime, external tests, an engine protocol fake, and CI. No persistence worker, queue subscriber, database integration, PostgreSQL schema, or migration exists. The queued contract has no event identifier or ordering guarantee.

## Desired Outcome
The queue worker can invoke one Rust iii persistence function for each queued harness event. The worker validates the shared event contract, executes parameterized PostgreSQL writes through the iii database worker, and returns success only after the database worker confirms the write.

## Approach
Use an append-only PostgreSQL ledger: record session lifecycle events in `session_events` and unprocessed observation events in `raw_observations`. Use database-generated UUIDv7 receipt keys and one chronological secondary index per event table; avoid foreign keys, mutable session snapshots, uniqueness checks on source data, and other read-oriented indexes. Keep optional session embeddings in a separate pgvector table without vector-index maintenance on event writes.

Queue delivery remains at-least-once. The worker accepts possible duplicate rows because the upstream payload has no unique event ID; it does not claim exactly-once persistence or lossless queue acceptance.

## Scope
- **In**: Rust worker bootstrap, one queue-invoked iii function, durable subscriber registration, shared ProtoJSON decoding, event validation, database-worker adapter, PostgreSQL migrations, database-generated UUIDv7 receipt keys, normalized source instants, per-session chronological indexes, append-only `session_events` and `raw_observations` tables, `post_tool_use` observations, an optional pgvector session-embedding table, parameterized writes, database-result checking, and focused automated tests.
- **Out**: Upstream harness functions, queue provisioning, database-worker implementation, read/query APIs, session-state projection, deduplication, ordering guarantees, embedding generation, vector indexes, retention, partitioning operations, and downstream memory processing.

## Boundary Candidates
- Queue subscriber registration and persistence-function lifecycle.
- Shared event decoding and event-to-statement mapping.
- PostgreSQL write schema and migrations.
- Database-worker invocation behind an engine-independent test seam.

## Out of Boundary
- Changing the authoritative protobuf event contract owned by `harness-ingestion-worker`.
- Treating a queue publish acknowledgment as proof of durable acceptance.
- Providing exactly-once semantics without a future upstream event identifier.
- Installing PostgreSQL or pgvector, managing credentials, or provisioning the database worker.
- Populating or searching session embeddings.

## Upstream / Downstream
- **Upstream**: The implemented `harness-ingestion` worker, its root protobuf schema and workspace pins, iii engine and Rust SDK 0.24.0, iii queue worker 0.21.13, and its configured queue destination and namespace.
- **Downstream**: Session projections, observation interpretation, embedding generation, vector search, retrieval, retention, and archival work.

## Existing Spec Touchpoints
- **Extends**: None.
- **Adjacent**: `harness-ingestion-worker` owns the protobuf contract and queue publication. This spec compiles the root schema independently, uses the ingestion crate only as a test oracle, and does not introduce a production worker-to-worker dependency.

## Constraints
- Target PostgreSQL 17 or 18 through iii database worker 0.5.17.
- Require `pg_uuidv7` release 1.7.0, extension version `1.7`, to be installed by the database deployment; use `uuid_generate_v7()` as each receipt-key default.
- Use PostgreSQL-native parameter placeholders and require one affected row before acknowledging queue delivery.
- Preserve each source timestamp string, derive its UTC instant at millisecond precision, and index both event tables by `session_id ASC, source_timestamp_utc ASC`.
- Preserve `post_tool_use` as an observation `hook_type`; its top-level `event_type` remains `observation`.
- Require pgvector for the optional embedding table; bind vectors through explicit PostgreSQL casts in future writers.
- Make writes independent of lifecycle-event ordering; queue FIFO does not prevent a retried event from being overtaken.
- Keep opaque observation data out of logs and errors.
- Use no live iii engine, queue worker, database worker, or PostgreSQL instance in unit tests.
- The iii engine's Elastic License 2.0 terms remain a deployment constraint.
