# Brief: opencode-harness-events-plugin

## Problem
OpenCode users need lifecycle and observation events submitted through the generic harness event boundary without embedding iii protocol logic in an OpenCode integration or maintaining separate integrations for OpenCode v1 and v2.

User request: "Create a spec to consume the harness-events-cli as part of an Opencode plugin or extension that targets both Opencode v1 and Opencode v2. The plugin should be as thin as possible and written in Typescript. The plugin should be a self-contained package that is entirely managed by Bun (tests, packages, etc.). Include dependabot configuration and strict code style/linting. Unit tests should run using Bun, end-to-end tests should run using Rust's tests runner using a headless opencode and a running iii engine instance."

## Current State
The approved `harness-events-cli` spec defines typed commands and process outcomes for `session-start`, `observation`, and `session-end`, but excludes harness-specific adapters. The repository has no TypeScript, Bun, or OpenCode package. OpenCode v1 and v2 use different plugin entrypoints, hook APIs, event shapes, and package names.

## Desired Outcome
A self-contained Bun package loads in supported OpenCode v1 and v2 releases, translates coarse native events into harness event CLI invocations, and remains isolated from iii and protobuf implementation details. Bun runs package checks and unit tests; Cargo's test runner drives live-engine, headless-OpenCode end-to-end coverage for both generations.

## Approach
Publish one TypeScript package using OpenCode's dual-generation entrypoint pattern. Thin v1 and v2 adapters forward stable non-streaming session, prompt/message, tool, and error events as opaque JSON to a shared subprocess dispatcher. The dispatcher invokes `harness-events` without a shell, performs no retry, and fails open with bounded payload-free diagnostics.

## Scope
- **In**: Dual v1/v2 plugin entrypoints, native-event filtering and field derivation, CLI process invocation, Bun package management/build/lint/format/unit-test configuration, Dependabot coverage, and Cargo-driven end-to-end tests against headless OpenCode and a running iii engine.
- **Out**: iii client logic, protobuf ownership, CLI implementation or distribution, durable delivery, retry queues, event ordering guarantees, deduplication guarantees, OpenCode CLI installation for users, external model-provider testing, and changes to ingestion or persistence workers.

## Boundary Candidates
- OpenCode boundary: generation-specific adapters register native hooks and normalize invocation inputs without sharing native API types.
- Process boundary: one shared dispatcher maps normalized events to the existing CLI contract and contains subprocess failure effects.
- Package boundary: the TypeScript deliverable owns its Bun lockfile, scripts, compiler, lint, formatting, and unit tests.
- End-to-end boundary: a Rust test harness owns real-process orchestration, a running iii engine, capture functions, headless OpenCode fixtures, deadlines, and cleanup.

## Out of Boundary
- Reimplementation of harness event validation, iii transport, queue publication, or persistence.
- Streaming text or reasoning delta submission.
- Transformation of native OpenCode payloads into a cross-harness semantic schema.
- Blocking OpenCode workflows when event submission fails.
- Exactly-once, ordered, replayable, or durable event delivery.
- Tool-execution scenarios that require external model credentials.

## Upstream / Downstream
- **Upstream**: `harness-events-cli`, OpenCode v1 from `1.18.29`, OpenCode v2, Bun, and a runnable iii engine.
- **Downstream**: Package publication and user installation/configuration workflows; broader harness adapter conventions may reuse the process-dispatch pattern.

## Existing Spec Touchpoints
- **Extends**: None.
- **Adjacent**: `harness-events-cli` owns commands, request shapes, diagnostics, and exit statuses; `harness-ingestion-worker` owns function and protobuf contracts; persistence remains transitively downstream.

## Constraints
- Use TypeScript and keep generation-specific code limited to API registration and native-event extraction.
- Manage the package, dependencies, build, checks, and unit tests with Bun; commit the Bun lockfile.
- Use strict TypeScript and a single strict lint/format toolchain.
- Invoke the CLI without a shell and never include observation payloads in plugin diagnostics.
- Submit coarse events only; exclude streamed text and reasoning deltas.
- Continue OpenCode operation after CLI spawn, timeout, connection, or invocation failures.
- Run end-to-end scenarios through Rust's test runner with isolated OpenCode configuration and bounded child-process cleanup.
