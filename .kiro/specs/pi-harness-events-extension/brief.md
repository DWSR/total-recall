# Brief: pi-harness-events-extension

## Problem
Pi users need lifecycle and observation events submitted through the generic harness event boundary without embedding iii protocol logic in a Pi integration. The repository has an approved OpenCode adapter design, but no Pi extension or headless Pi integration coverage.

User request: "Create a spec to add a Pi extension similar to the opencode one. End to end tests should run headless Pi."

## Current State
The approved `harness-events-cli` spec defines `session-start`, `observation`, and `session-end` process contracts but its executable is not yet implemented. The approved `opencode-harness-events-plugin` spec establishes the adjacent thin-adapter pattern. The repository has no Pi package, Pi extension, Node package toolchain, or headless Pi test harness. The maintained Pi package is `@earendil-works/pi-coding-agent`; the former `@mariozechner/pi-coding-agent` package is deprecated.

## Desired Outcome
A self-contained TypeScript Pi package loads through Pi's Jiti extension loader on Node 24 and later LTS host runtimes, translates coarse Pi lifecycle, agent, message, tool, and error signals into bounded queued `harness-events` invocations, and remains isolated from iii and protobuf implementation details. npm owns package management and verification, while Cargo's test runner drives a real iii engine and headless Pi with a deterministic offline provider.

## Approach
Implement a native Pi extension against pinned `@earendil-works/pi-coding-agent` `0.86.1`. Event handlers capture and normalize accepted native events into a bounded in-memory queue; one dispatcher serializes no-shell CLI invocations and performs a best-effort bounded flush during `session_shutdown`. End-to-end tests load the built extension explicitly into headless Pi with isolated state, `PI_OFFLINE=1`, and a test-only scripted provider and tool so no external model credentials or network calls are required.

## Scope
- **In**: Jiti-based Pi extension loading on Node 24 and later LTS host runtimes, native event selection and normalization, lifecycle correlation, bounded serialized CLI dispatch, fail-open diagnostics, npm-managed TypeScript package tooling and unit tests, dependency automation, Node LTS and Pi development-shell support, and Cargo-driven end-to-end tests against headless Pi and a running iii engine.
- **Out**: iii client logic, protobuf ownership, CLI implementation or distribution, durable queues, retry or replay, exactly-once or global ordering guarantees, OpenCode adapter changes, deprecated Pi package support, external model-provider tests, and ingestion or persistence worker changes.

## Boundary Candidates
- Pi boundary: the extension registers Pi hooks and derives session, project, directory, timestamp, and event identity fields from Pi context.
- Queue boundary: a bounded in-memory queue decouples Pi hooks from subprocess latency and applies explicit overflow behavior.
- Process boundary: one dispatcher maps normalized events to the existing CLI contract and contains subprocess failures.
- Package boundary: the TypeScript deliverable owns its manifest, npm lockfile, strict checks, Jiti-loadable source, and unit tests.
- End-to-end boundary: a Rust test harness owns real-process orchestration, the iii engine, capture functions, offline headless Pi fixtures, readiness checks, deadlines, and cleanup.

## Out of Boundary
- Reimplementation of harness event validation, iii transport, queue publication, or persistence.
- A reusable cross-agent adapter SDK or shared dispatcher extraction from the OpenCode integration.
- Forwarding streamed message or tool-progress updates that create high-volume duplicate observations.
- Intentional collection of system prompts, images, thinking content, provider credentials, process environment data, or unrelated Pi configuration.
- Secret detection or redaction within accepted user-authored message or tool payloads.
- Blocking Pi workflows when event serialization, queuing, spawning, timeout, or CLI invocation fails.
- Guaranteed delivery during process crashes, forced termination, or queue overflow.
- Automatic trust of project-local extensions or resources.

## Upstream / Downstream
- **Upstream**: The implemented `harness-events-cli`, `@earendil-works/pi-coding-agent` `0.86.1`, npm, supported Node LTS runtimes beginning with Node 24, and a runnable iii engine.
- **Downstream**: Package publication and installation workflows; future adapter conventions may reuse lessons from the Pi and OpenCode integrations through a separate specification.

## Existing Spec Touchpoints
- **Extends**: None.
- **Adjacent**: `harness-events-cli` owns commands, request shapes, diagnostics, and exit statuses. `opencode-harness-events-plugin` owns only OpenCode APIs and fixtures. `harness-ingestion-worker` owns function and protobuf contracts; persistence remains transitively downstream.

## Constraints
- Use TypeScript and Pi's native extension API; do not consume the deprecated `@mariozechner/pi-coding-agent` package.
- Pin Pi exactly to `0.86.1` for package verification and end-to-end tests while its API remains pre-1.0.
- Ship a TypeScript extension entrypoint loaded directly by Pi through Jiti; do not require a bundled JavaScript runtime artifact.
- Manage dependency installation, locking, scripts, type checking, linting, formatting, and unit tests with npm; do not use Bun, Yarn, or pnpm.
- Declare Node 24 as the minimum Pi host runtime and support each later Node major only after it reaches LTS.
- Run headless end-to-end compatibility coverage on every supported Node LTS major using the pinned Pi package.
- Invoke `harness-events` without a shell and preserve `III_URL` and `III_NAMESPACE` for the child process.
- Bound queue capacity, child execution time, shutdown flushing, diagnostics, and captured test output.
- Define deterministic queue-overflow behavior without blocking Pi or initiating retries.
- Keep Pi hook failures fail-open, including hooks whose default Pi error behavior is fail-closed.
- Exclude streamed updates and preserve accepted coarse native data as JSON-compatible content rather than inventing a cross-harness schema.
- Use extension-safe Pi session identifiers and working-directory context; capture missing event timestamps at handler entry.
- Run end-to-end scenarios through Rust's test runner with isolated Pi state, explicit extension loading, no project trust prompts, no external credentials, no model network access, readiness polling, and bounded process-tree cleanup.
