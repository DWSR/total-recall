# Implementation Plan

- [ ] 1. Establish the persistence worker and storage foundation
- [x] 1.1 Add the sibling worker and compile the root event schema
  - Add the persistence worker to the implemented Rust workspace using its exact runtime and contract-generation dependency pins.
  - Compile the root upstream-owned protobuf schema into crate-local Prost and ProtoJSON output with the same descriptor, well-known-type, field-emission, and proto-name settings as ingestion.
  - Cargo can load the workspace and the new crate can compile all three generated queued-event messages from a clean build.
  - _Requirements: 2.1, 2.5, 8.4_
  - _Boundary: Contract Adapter_

- [x] 1.2 Add startup configuration and worker identity
  - Mirror the implemented testable configuration seam: trim required queue-topic and logical-database values, default blank engine URLs, and preserve optional managed worker name and namespace.
  - Define the worker manifest and release start command without provisioning queue, database, PostgreSQL, `pg_uuidv7`, or pgvector services.
  - Configuration tests reject missing, empty, and whitespace-only values; the release command resolves the workspace binary.
  - _Depends: 1.1_
  - _Requirements: 1.2, 8.4_
  - _Boundary: Worker Runtime_

- [x] 1.3 Create the required append-only event ledger
  - Define `session_events` and `raw_observations` tables with database-generated UUIDv7 receipt keys, immutable source fields, normalized source instants, ingestion timestamps, and opaque JSON observation data.
  - Add discriminator checks, UUID primary keys, and exactly one `(session_id ASC, source_timestamp_utc ASC)` B-tree index per event table; add no foreign keys, source-data uniqueness, included columns, partitions, or mutable session state.
  - The migration requires `pg_uuidv7` extension version `1.7`, does not require pgvector, and represents repeated or out-of-order deliveries as independent rows.
  - _Requirements: 2.6, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 4.1, 4.2, 4.3, 4.4, 5.1, 5.2, 5.3, 7.2_
  - _Boundary: Schema Migrations_

- [x] 1.4 Add optional session-embedding storage
  - Define a separate pgvector-backed table with a database-generated UUIDv7 receipt key, session correlation, model identity, dimensions, vector values, and dimension consistency.
  - Add no extension installation, foreign key, uniqueness rule, or vector index, and keep this migration independent of the required ledger migration.
  - The ledger can be deployed and used when the optional embedding migration is not applied.
  - _Requirements: 3.6, 7.1, 7.2, 7.3_
  - _Boundary: Schema Migrations_

- [ ] 2. Implement event persistence components
- [x] 2.1 Implement strict queued-event adaptation
  - Accept the three tagged event variants through their generated ProtoJSON shapes, require their exact fields and discriminators, and reject unknown fields.
  - Match ingestion parity exactly: reject only empty session IDs, allow other empty strings, preserve lowercase and offset timestamps, derive epoch milliseconds, and retain protobuf-double observation-number semantics.
  - External tests accept all variants, whitespace session IDs, arbitrary nested values, large-number rounding, and `post_tool_use`; they reject malformed timestamps, wrong discriminators, missing or unknown fields, and non-object data without database work.
  - _Depends: 1.1_
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 4.3, 5.2, 5.3, 5.4, 8.2_
  - _Boundary: Contract Adapter_

- [x] 2.2 Implement the one-event append service and store contract
  - Map each validated event to one store append and return persistence success only for a receipt reporting one affected row.
  - Keep the service stateless and perform no retries, deduplication, ordering checks, lifecycle inference, updates, or deletes.
  - Recording and failing store tests prove one call per delivery, distinct repeated deliveries, out-of-order acceptance, affected-row validation, and error propagation.
  - _Depends: 2.1_
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 4.4, 5.3, 6.1, 6.2, 6.3, 8.1, 8.2_
  - _Boundary: Persistence Service_

- [x] 2.3 Implement the iii database-worker adapter
  - Build static parameterized inserts targeting `session_events` and `raw_observations`, leaving receipt generation to the database UUIDv7 default and deriving normalized source time from the epoch-millisecond parameter.
  - Invoke one inherited-namespace database execute operation; validate `affected_rows`, nullable string `last_insert_id`, and object-array `returned_rows`, requiring exactly one affected row.
  - Adapter tests verify exact requests and preserved lifecycle and observation values, and reject invocation, timeout, malformed-response, zero-row, and multi-row outcomes without exposing SQL parameters, opaque data, credentials, or full payloads.
  - _Depends: 1.3, 2.2_
  - _Requirements: 2.6, 3.6, 4.1, 4.2, 4.3, 5.1, 5.2, 5.4, 6.1, 6.2, 6.3, 6.4, 6.5, 8.1, 8.2, 8.3, 8.4_
  - _Boundary: Database Adapter_

- [x] 2.4 (P) Build the persistence function and subscriber registration plan
  - Build deterministic function and durable-subscriber registrations with canonical queue configuration, explicit target/provider namespaces, and one UUIDv4 per-start metadata nonce from the existing workspace feature set.
  - Describe exactly one typed persistence function and one subscriber binding without treating the returned trigger handle as acknowledgment; runtime registration remains an integration responsibility.
  - Pure tests expose managed metadata, inherited call namespaces, explicit subscriber namespaces, function, trigger, configuration, and nonce without composing the handler service.
  - _Depends: 1.2, 2.1_
  - _Requirements: 1.1, 1.3, 8.4_
  - _Boundary: Worker Runtime_

- [x] 2.5 Gate readiness on active registration
  - Wait for worker registration, verify function ownership through the implemented function-catalog request shape and namespace filter, then list and inspect registered-trigger candidates with pending entries included.
  - Require exactly one active detail matching the nonce, function, configuration, provider, target namespace, and target owner within the shared startup deadline.
  - Pure tests reject missing, stale, pending, ambiguous, foreign, mismatched, disconnected, and timed-out catalog results under one absolute deadline.
  - _Depends: 2.4_
  - _Requirements: 1.3, 8.4_
  - _Boundary: Worker Runtime_

- [x] 3. Compose event handling and process lifecycle
  - Connect strict event adaptation, the append service, the iii database adapter, and runtime registration with one managed client while retaining the function and subscriber handles.
  - Return success only after confirmed persistence; map contract and database failures to queue-visible errors without an application retry or persistence guarantee.
  - The process races startup readiness against termination, reports ready only after both catalogs match, shuts down on every exit path, and never logs opaque observation data.
  - _Depends: 2.3, 2.5_
  - _Requirements: 1.1, 1.3, 2.4, 3.5, 6.1, 6.2, 6.3, 6.4, 6.5, 8.3, 8.4_
  - _Boundary: Contract Adapter, Persistence Service, Database Adapter, Worker Runtime_

- [ ] 4. Validate the persistence worker
- [x] 4.1 Add cross-boundary contract and schema checks
  - Use `harness-ingestion` as a dev-only oracle to verify each strict boundary variant, validation edge, and observation value remains compatible with implemented generated ProtoJSON; production modules do not import it.
  - Verify migrations use the `raw_observations` table name, `uuid_generate_v7()` defaults, normalized timestamp columns, and exactly one approved chronological secondary index per event table, and that the required ledger does not reference pgvector.
  - The checks cover every event variant and `post_tool_use` hook preservation without live services; database, runtime, and sensitive-data cases remain in their component tests.
  - _Depends: 3_
  - _Requirements: 2.1, 2.3, 2.4, 2.5, 2.6, 3.6, 3.7, 5.2, 5.4, 7.1, 7.2, 7.3, 8.1, 8.2, 8.4_
  - _Boundary: Contract Adapter, Schema Migrations_

- [x] 4.2 Add the persistence protocol fake
  - Add a dedicated workspace binary modeled on the implemented ingestion fake that spawns the release persistence worker against a loopback WebSocket engine.
  - Emulate worker and function registration, function catalogs, subscriber registration, registered-trigger list/info, and `database::execute`, then invoke one valid queued event.
  - The fake exits successfully only when namespaces and ownership match, the database request is exact, and the worker responds after the delayed database acknowledgment without any live service.
  - _Depends: 3_
  - _Requirements: 1.3, 2.1, 5.4, 6.1, 6.2, 6.5, 8.1, 8.3, 8.4_
  - _Boundary: Protocol Fake_

- [x] 4.3 Extend CI and run final worker verification
  - Extend the implemented CI workflow to release-build the persistence worker and run its protocol fake while preserving the existing ingestion build and smoke test.
  - Run the complete workspace tests, formatting, warning-free Clippy, flake checks, both release builds, and both protocol fakes without live iii, queue, database-worker, PostgreSQL, `pg_uuidv7`, or pgvector infrastructure.
  - Completion is observable when every approved verification command passes, the required ledger depends only on the documented UUIDv7 extension, and pgvector remains optional.
  - _Depends: 4.1, 4.2_
  - _Requirements: 7.2, 7.3, 8.1, 8.2, 8.3, 8.4_
  - _Boundary: Protocol Fake, Worker Runtime, Contract Adapter, Database Adapter, Schema Migrations_
