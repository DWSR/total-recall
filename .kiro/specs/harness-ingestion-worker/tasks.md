# Implementation Plan

- [x] 1. Establish the Rust and protobuf foundation
- [x] 1.1 Create the Cargo workspace and reproducible worker build
  - Establish the root workspace and harness-ingestion binary crate with the approved language edition and complete runtime/build dependency pins.
  - Produce the dependency lock and add the pinned protobuf compiler to the automatic development shell.
  - The development environment resolves Cargo, iii, and protoc, and Cargo can load the complete workspace metadata.
  - _Requirements: 4.1, 4.6, 7.5_

- [x] 1.2 Build the authoritative protobuf generation pipeline
  - Define the versioned package and six request/event messages with stable field numbers, snake-case ProtoJSON names, string discriminators, string timestamps, and `google.protobuf.Struct` observation data.
  - Generate Prost messages and pbjson implementations from one descriptor set, map Google well-known types consistently, and emit every queued event field.
  - Expose generated contracts without committing generated build output.
  - Contract fixtures serialize every generated event with the required ProtoJSON field names and default-field presence after a clean regeneration.
  - _Requirements: 4.1, 4.3, 4.4, 4.5, 4.6, 7.3_
  - _Boundary: Contract Pipeline_

- [x] 2. Implement event ingestion boundaries
- [x] 2.1 Implement strict request adaptation and timestamp validation
  - Add iii-facing request adapters that require schema fields, reject unknown top-level fields and non-object `data`, and convert through generated protobuf request messages.
  - Validate non-empty session identifiers and RFC 3339 timestamps with exactly millisecond precision while preserving accepted text.
  - Leave other required string fields uninterpreted.
  - Unit tests demonstrate missing, unknown, non-object, empty-session, precision, calendar, offset, and preservation behavior without external services.
  - _Requirements: 2.5, 2.6, 3.6, 4.2, 4.5, 4.6, 4.7, 5.1, 5.2, 5.3, 7.4, 7.5_
  - _Boundary: Contract Pipeline_

- [x] 2.2 Implement opaque Struct conversion
  - Convert observation data recursively from JSON objects into `google.protobuf.Struct` without interpreting keys or values.
  - Apply protobuf `f64` semantics to every number, including deterministic rounding outside the exact integer range.
  - Unit tests cover nested values, empty objects, representable numbers, and the exact-integer boundary.
  - The converted Struct serializes back to the expected ProtoJSON object semantics.
  - _Requirements: 3.3, 3.4, 3.5, 4.4, 7.2, 7.4, 7.5_
  - _Boundary: Contract Pipeline_

- [x] 2.3 (P) Implement the queue publication port and iii adapter
  - Define the mockable publisher boundary and production iii implementation using a validated queue topic.
  - Build the queue trigger request through a pure contract that targets `iii::durable::publish` with only topic and event data, inherited namespace, and no queue action.
  - Propagate invocation failures and accept the null result as dispatch success without retries or persistence claims.
  - Unit tests prove request construction, success mapping, error propagation, and single-attempt behavior without a running engine.
  - _Depends: 1.2_
  - _Requirements: 6.1, 6.3, 6.4, 6.5, 6.6, 7.4, 7.5_
  - _Boundary: Queue Publisher_

- [x] 2.4 Integrate the three ingestion handlers
  - Construct the generated session-start, observation, and session-end events with fixed lower-case discriminators and preserved harness fields.
  - Dispatch one event through the publisher for each accepted call and return dispatched success only after publisher success.
  - Keep handlers stateless and accept lifecycle or observation calls without prior-session checks.
  - Recording and failing publisher tests prove exact produced event shapes, one publication per call, no retries, and caller-visible errors.
  - _Depends: 2.2, 2.3_
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.6, 2.7, 3.1, 3.2, 3.7, 4.3, 6.2, 6.3, 6.4, 6.5, 7.1, 7.3, 7.4, 7.5_
  - _Boundary: Ingestion Service_

- [x] 3. Integrate the worker runtime
- [x] 3.1 Implement startup configuration and the worker manifest
  - Require a non-blank queue topic, normalize it once, and consume iii's managed identity and namespace environment.
  - Provide the worker identity and release start command through the iii manifest.
  - Configuration tests reject missing, empty, and whitespace-only topics.
  - The release command resolves the workspace binary and startup stops before iii registration when configuration is invalid.
  - _Depends: 2.4_
  - _Requirements: 1.2, 6.1, 7.4, 7.5_
  - _Boundary: Worker Runtime_

- [x] 3.2 Register the harness functions
  - Compose the ingestion service and production queue publisher with one managed iii client.
  - Register all three approved function identifiers with one per-start nonce in their metadata.
  - Build a deterministic registration plan that exposes the exact identifiers, expected worker ownership, namespace, and shared nonce for readiness verification.
  - Unit tests prove the registration plan contains every function exactly once with the current nonce.
  - _Requirements: 1.1, 7.4, 7.5_
  - _Boundary: Worker Runtime_

- [x] 3.3 Gate readiness and complete process lifecycle
  - Share one deadline between worker acknowledgment and function-catalog polling, requiring every identifier, namespace, worker name, and nonce before readiness.
  - Exit on rejection, disconnect, conflict, catalog mismatch, or timeout, and shut down the iii client on process termination.
  - Deterministic tests prove catalog matching rejects missing, stale, or foreign registrations without a running engine.
  - The worker cannot report ready unless all three functions belong to the current process.
  - _Requirements: 1.3, 7.4, 7.5_
  - _Boundary: Worker Runtime_

- [x] 4. Validate the complete worker
  - Run all workspace tests and verify the required contract, validation, publisher, handler, configuration, registration, and readiness cases are present.
  - Build the release binary and launch it through `iii.worker.yaml` with the engine-independent fake protocol harness.
  - Verify all three registered functions produce one exact queue payload and one dispatched response through the complete runtime seam.
  - Run formatting, warning-free linting, and flake evaluation from the automatic development environment.
  - Confirm logs and errors never include opaque observation data and no test requires a live iii engine or queue worker.
  - Completion is observable when all approved verification commands pass from a clean workspace.
  - _Depends: 3.3_
  - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_
  - _Boundary: Contract Pipeline, Ingestion Service, Queue Publisher, Worker Runtime, Integration Validation Harness_

## Implementation Notes
- iii 0.24 worker manifests require `scripts.start`; the runtime smoke launches through that field and validates all three exact queue payloads.
