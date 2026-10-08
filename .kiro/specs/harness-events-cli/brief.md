# Brief: harness-events-cli

## Problem
Harness integrations need a stable command-line boundary for submitting lifecycle and observation events without implementing the iii client protocol or coupling directly to queue dispatch.

User request: "Create a spec for a CLI wrapper for pushing harness events to the event ingestion function. This will form the basis of integrations with various harnesses."

## Current State
The harness ingestion worker exposes `harness::session_start`, `harness::observation`, and `harness::session_end` through iii and owns the authoritative protobuf request contracts. No production CLI invokes those functions for external harness integrations.

## Desired Outcome
A Rust workspace CLI lets harness adapters submit valid ingestion requests through typed subcommands and depend on stable process exit behavior.

## Approach
Build a Rust client with the repository-pinned `iii-sdk`. Provide typed subcommands for the three ingestion functions, require every request field explicitly, and read each observation's opaque JSON object from stdin. Report diagnostics on stderr and use documented exit statuses, with `0` reserved for success.

## Scope
- **In**: CLI argument parsing, stdin observation data, ingestion request construction, iii function invocation, connection configuration, deterministic diagnostics and exit statuses, and automated boundary tests.
- **Out**: Concrete harness adapters, inferred request fields, retries, queue publication, persistence, packaging, installers, and distribution channels.

## Boundary Candidates
- Process boundary: typed commands, required inputs, stdin handling, diagnostics, and exit statuses.
- Invocation boundary: map each command to one authoritative ingestion function request and return its result without retrying.
- Contract boundary: remain compatible with the root harness protobuf and ingestion function identifiers without taking ownership of either.

## Out of Boundary
- Harness-specific event interpretation or configuration.
- Session-state validation, event ordering, deduplication, or durable-delivery guarantees.
- Direct invocation of queue or persistence functions.
- Changes to ingestion request or queued-event schemas.

## Upstream / Downstream
- **Upstream**: `harness-ingestion-worker`, the root harness protobuf, and the repository-pinned `iii-sdk`.
- **Downstream**: Future adapters for individual agent harnesses and their launch or hook configuration.

## Existing Spec Touchpoints
- **Extends**: None.
- **Adjacent**: `harness-ingestion-worker` owns function and request contracts; `harness-event-persistence-worker` is transitively downstream and must not be called directly.

## Constraints
- Use the existing Rust workspace and pinned dependency stack.
- Preserve observation data as opaque ProtoJSON-compatible object content and do not expose it in diagnostics.
- Require harness integrations to supply every request field.
- Issue one application-level function invocation per command execution and do not initiate a retry after failure; SDK-managed transport replay remains possible.
