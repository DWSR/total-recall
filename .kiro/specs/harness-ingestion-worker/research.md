# Research and Gap Analysis

## Current State
- The repository contains a flake-based Rust shell with iii 0.24.0.
- No Cargo crate, Rust source, protobuf schema, code generator, worker configuration, or tests exist.
- The development shell lacks `protoc`.
- No source-layout or testing conventions constrain this feature.

## External Findings
- `iii-sdk` 0.24.0 supports asynchronous typed handlers through `RegisterFunction::new_async` and requires handler inputs to implement `DeserializeOwned` and `JsonSchema`.
- A worker can call `iii::durable::publish` with `{ "topic": string, "data": any JSON }`; success returns JSON `null`.
- The queue publish function does not surface all backend persistence errors. This matches the approved dispatch-only contract but cannot support a durability claim.
- Function calls inherit the worker namespace. Cross-namespace queue publication requires explicit routing.
- Queue worker 0.21.13 pins `iii-sdk` 0.23; no wire conflict with engine and SDK 0.24 was found, but the pair lacks first-party compatibility coverage.
- `prost` and `prost-build` 0.14.1 can generate Rust messages. `pbjson`, `pbjson-build`, and `pbjson-types` 0.9.0 share the same Prost generation and runtime line.
- `pbjson-types::Struct` maps directly to a JSON object and supports arbitrary JSON values.
- `pbjson-build` generates ProtoJSON Serde implementations but not `schemars::JsonSchema`.
- Direct schemas derived from generated Prost types can misrepresent ProtoJSON enums, bytes, integer strings, default fields, and JSON names.
- Thin serde/schemars request adapters can present accurate iii schemas while generated protobuf messages remain authoritative for queued events.
- `google.protobuf.Timestamp` canonicalizes offsets and fractional precision. A protobuf string field plus lexical and calendar validation preserves the approved RFC 3339 millisecond representation.
- ProtoJSON rejects unknown fields by default. Ignoring them is an explicit compatibility choice.
- ProtoJSON emits enum identifiers by default; a lower-case `event_type` contract requires string fields or an explicit mapping layer.
- iii SDK and worker code are Apache-2.0. The external iii engine is Elastic-2.0 and carries managed-service restrictions.

## Requirement-to-Asset Map
| Requirement | Existing asset | Gap |
| --- | --- | --- |
| 1. Worker availability | iii CLI in dev shell | Missing crate, SDK dependency, startup configuration, registration, readiness, and shutdown |
| 2. Session lifecycle | None | Missing protobuf requests/events, handlers, validation, and publisher |
| 3. Observation ingestion | None | Missing structured request, opaque object mapping, validation, and publisher |
| 4. Protobuf contract | Rust toolchain only | Missing `.proto`, `build.rs`, `protoc`, generated-code integration, and ProtoJSON support |
| 5. Timestamp contract | None | Missing preserving validator and schema annotation |
| 6. Queue dispatch | iii executable only | Missing SDK client call, topic configuration, namespace decision, and error mapping |
| 7. Verification | None | Missing publisher seam, unit tests, and build checks |

## Implementation Options

### Option A: Generated Protobuf Types as iii Inputs
Use generated messages directly as handler inputs and queue events.

- **Advantages**: One model per request/event; least conversion code.
- **Costs**: Requires custom `JsonSchema` implementations and careful schema overrides for `Struct` and ProtoJSON behavior.
- **Risk**: iii-advertised input schemas can diverge from actual ProtoJSON.

### Option B: iii Request Adapters with Generated Events
Use small serde/schemars request adapters at iii boundaries, validate them, then construct generated protobuf event messages for ProtoJSON publication.

- **Advantages**: Accurate iii schemas, isolated validation, authoritative generated events, and a simple mockable publisher seam.
- **Costs**: Duplicates request field declarations and adds explicit conversion code.
- **Risk**: Adapter and protobuf drift must be caught by serialization tests.

### Option C: Descriptor-Driven Dynamic Messages
Use protobuf descriptors and runtime reflection for ProtoJSON conversion.

- **Advantages**: Canonical dynamic serialization and fewer generated Serde concerns.
- **Costs**: Runtime reflection, descriptor management, dynamic-to-typed conversion, and no direct `JsonSchema` solution.
- **Risk**: Highest complexity for three small fixed contracts.

## Complexity and Risk
- **Effort**: M. The worker behavior is small, but the repository and protobuf build pipeline are greenfield.
- **Risk**: Medium. iii registration is documented; ProtoJSON/schema alignment and queue-version compatibility need explicit design commitments.

## Design Inputs
- Select the generated-type or adapter boundary.
- Define protobuf package, field numbers, `event_type` representation, and unknown-field policy.
- Define iii function identifiers, queue-topic configuration, and namespace routing.
- Pin compatible Cargo dependencies and add `protoc` to the development shell.
- Keep engine/queue integration tests outside the minimal test boundary.
- Record the queue acknowledgement limitation and external engine license without assigning persistence responsibility to this worker.

## Design Decisions
- Use thin serde/schemars request adapters and generated protobuf request/event messages. Contract tests guard duplicate field declarations against drift.
- Generate Prost and pbjson code from one descriptor set; map Google well-known types to `pbjson_types`.
- Use `google.protobuf.Struct` for `data`. The user accepted protobuf double semantics for numeric values.
- Keep `event_type` as a protobuf string with worker-owned lower-case constants; ProtoJSON enum identifiers would change the approved wire values.
- Use string timestamps with lexical and Chrono validation; do not use `google.protobuf.Timestamp`.
- Reject unknown request fields and emit all queued event fields.
- Publish through a small `QueuePublisher` port so handler tests need no iii engine.
- Require `TOTAL_RECALL_QUEUE_TOPIC` and share the queue worker's iii namespace.
- Establish a root Cargo workspace with one worker crate and a shared versioned protobuf path.
- Provide `iii.worker.yaml` but leave Compose topology and queue provisioning outside this spec.
- Tag every function registration with a per-start nonce and verify catalog ownership before readiness; worker acknowledgment alone precedes function-conflict handling.
- Convert observation numbers recursively to protobuf doubles and test precision-boundary behavior.
- Build the iii queue `TriggerRequest` through a pure function so the production contract is testable without an engine.

## References
- [iii Rust SDK 0.24.0](https://github.com/iii-hq/iii/tree/iii/v0.24.0/sdk/packages/rust/iii)
- [iii queue worker](https://github.com/iii-hq/workers/tree/queue/v0.21.13/queue)
- [ProtoJSON format](https://protobuf.dev/programming-guides/json/)
- [Protobuf Struct](https://protobuf.dev/reference/protobuf/google.protobuf/#struct)
- [Prost build API](https://docs.rs/prost-build/0.14.1/prost_build/)
- [pbjson build API](https://docs.rs/pbjson-build/0.9.0/pbjson_build/)
