# Brief: harness-ingestion-worker

## Problem
Create a Rust iii worker that receives structured lifecycle hooks from agent harnesses. It must expose functions for observation ingestion, session creation, and session termination, then forward those events to an iii queue worker.

## Current State
The repository provides a Rust and iii development environment but has no application crate, worker, hook contract, or queue integration.

## Desired Outcome
Agent harnesses can report a session start, structured observation, or session end through iii functions. Each accepted call dispatches a corresponding typed event to one configured queue topic for downstream processing.

## Approach
Build one stateless Rust worker with harness-owned session IDs and timestamps. Define its requests and queued events in protobuf, use ProtoJSON across iii boundaries, and register three functions that invoke `iii::durable::publish`; preserve the observation `data` map as opaque JSON and propagate invocation failures without claiming persistence, ordering, or deduplication.

## Scope
- **In**: Rust worker bootstrap and registration, three iii functions, protobuf request and event schemas, ProtoJSON transport, harness-owned session IDs and timestamps, opaque JSON observation data, configurable queue topic, queue dispatch, and focused unit tests.
- **Out**: Session state, identifier generation, observation interpretation, durable-acceptance guarantees, retries, ordering, deduplication, queue deployment, subscriber readiness, and downstream memory processing.

## Boundary Candidates
- Worker process lifecycle and iii function registration.
- Harness event contracts and queue-envelope construction.
- Queue publication behind an engine-independent test seam.

## Out of Boundary
- Creating or deleting queues per session.
- Validating whether a session is active.
- Persisting hooks in this worker.
- Running an iii engine or queue backend in tests.

## Upstream / Downstream
- **Upstream**: Agent harnesses, iii engine 0.24.0, `iii-sdk` 0.24.0, and an iii queue worker compatible with `iii::durable::publish`.
- **Downstream**: A future queue subscriber that creates, updates, and closes persisted memory sessions.

## Existing Spec Touchpoints
- **Extends**: None.
- **Adjacent**: Future session persistence, observation processing, and retrieval specs must consume the queued event contract without moving those responsibilities into this worker.

## Constraints
- Observation metadata follows a defined schema; only the `data` map remains opaque JSON.
- Protobuf definitions are the authoritative event contract; iii transports their ProtoJSON representation.
- Observation `data` uses protobuf Struct semantics, including IEEE-754 double representation for numbers.
- Harness timestamps use RFC 3339 with millisecond precision.
- A successful function result confirms queue-worker invocation only.
- The worker and queue function must share a namespace or use explicit namespace routing.
- Keep tests minimal and independent of a running iii engine.
