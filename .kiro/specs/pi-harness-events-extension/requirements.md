# Requirements Document

## Introduction
The Pi harness events extension provides the adapter described in [brief.md](brief.md). It submits Pi lifecycle and coarse observation events through the existing `harness-events` process contract.

## Boundary Context
- **In scope**: Pi extension loading, supported runtime compatibility, lifecycle and coarse observation mapping, bounded dispatch, failure isolation, npm package quality, dependency automation, unit tests, and Cargo-driven live-engine headless tests.
- **Out of scope**: iii or protobuf clients in the extension, CLI implementation or distribution, retries, durable delivery, cross-process ordering or deduplication, streamed updates, secret detection in accepted payloads, external model providers, and OpenCode, ingestion, or persistence changes.
- **Adjacent expectations**: `harness-events-cli` remains authoritative for commands and process outcomes. A successful CLI exit means accepted invocation, not durable persistence.

## Requirements

### Requirement 1: Pi and Runtime Compatibility
**Objective:** As a Pi user, I want one extension package for supported Pi and Node releases, so that event submission follows a defined compatibility policy.

#### Acceptance Criteria
1. The Pi Harness Events Extension shall expose a TypeScript extension entrypoint loadable by the Jiti loader in `@earendil-works/pi-coding-agent` `0.86.1`.
2. When Pi `0.86.1` runs on Node 24 LTS, the Pi Harness Events Extension shall load and register its supported event handlers.
3. Where a Node major later than 24 has reached LTS and is declared supported by the package, the Pi Harness Events Extension shall pass the same compatibility coverage as Node 24.
4. The Pi Harness Events Extension shall declare Node 24 as its minimum supported host runtime.
5. The Pi Harness Events Extension shall not declare a later Node major supported before that major reaches LTS and passes the compatibility matrix.
6. The Pi Harness Events Extension shall not depend on the deprecated `@mariozechner/pi-coding-agent` package.

### Requirement 2: Session Lifecycle Submission
**Objective:** As a memory pipeline operator, I want Pi sessions represented by lifecycle events, so that observations can be correlated to a session.

#### Acceptance Criteria
1. When Pi reports `session_start`, the Pi Harness Events Extension shall accept one `session-start` command for that session within the current extension runtime.
2. When an initial `session_start` represents a resumed session, the Pi Harness Events Extension shall accept `session-start` before accepting that session's observations.
3. When Pi reports `session_shutdown`, the Pi Harness Events Extension shall accept one `session-end` command for that session within the current extension runtime.
4. When Pi switches sessions and reloads extensions, the Pi Harness Events Extension shall end the prior session before accepting observations for the replacement session.
5. The Pi Harness Events Extension shall use Pi's native session identifier as the harness session identifier.
6. The Pi Harness Events Extension shall not claim lifecycle uniqueness across extension reloads, Pi restarts, or concurrent Pi processes.

### Requirement 3: Coarse Observation Submission
**Objective:** As a memory pipeline operator, I want useful Pi activity captured without streaming-event process churn, so that event volume reflects meaningful activity.

#### Acceptance Criteria
1. When Pi emits an accepted agent, turn, completed-message, tool-execution, session-status, or error signal, the Pi Harness Events Extension shall accept one `observation` command for that signal.
2. The Pi Harness Events Extension shall identify each observation by its native Pi event type.
3. The Pi Harness Events Extension shall preserve each accepted event's allowed native fields as JSON-compatible content without converting them into a cross-harness semantic schema.
4. When Pi emits a message-update or tool-execution-update signal, the Pi Harness Events Extension shall not accept an observation for that update.
5. The Pi Harness Events Extension shall not collect provider request bodies, provider response bodies, system prompts, images, thinking content, duplicate embedded messages or tool results, provider credentials, process environment data, or unrelated Pi configuration.
6. If an accepted native event cannot be represented as one JSON object, the Pi Harness Events Extension shall skip the event and emit a payload-free diagnostic.
7. The Pi Harness Events Extension shall not initiate a retry or duplicate CLI invocation for an accepted native event.

### Requirement 4: Deterministic CLI Inputs
**Objective:** As an operator, I want complete and deterministic CLI invocations, so that the existing harness boundary can validate every event.

#### Acceptance Criteria
1. The Pi Harness Events Extension shall invoke the configured harness events executable without shell interpretation.
2. Where no executable override is configured, the Pi Harness Events Extension shall resolve `harness-events` through the process environment.
3. The Pi Harness Events Extension shall provide every field required by the selected lifecycle or observation command.
4. The Pi Harness Events Extension shall derive the project name from the basename of Pi's current working directory.
5. The Pi Harness Events Extension shall use Pi's current working directory as the command working directory.
6. When a native event supplies a timestamp, the Pi Harness Events Extension shall preserve that event time in RFC 3339 format with millisecond precision.
7. When a native event supplies no timestamp, the Pi Harness Events Extension shall capture the handler-entry time in RFC 3339 format with millisecond precision.
8. When an observation is submitted, the Pi Harness Events Extension shall write exactly one JSON object to the CLI standard input and close the stream.
9. The Pi Harness Events Extension shall preserve `III_URL` and `III_NAMESPACE` from its process environment for the CLI.

### Requirement 5: Bounded Dispatch
**Objective:** As a Pi user, I want telemetry dispatch isolated from interactive work, so that slow downstream processing does not stall Pi indefinitely.

#### Acceptance Criteria
1. When a non-shutdown Pi event is accepted, the Pi Harness Events Extension shall return control without waiting for the corresponding CLI process to complete.
2. The Pi Harness Events Extension shall enforce a finite pending-event capacity.
3. If observation capacity is exhausted, the Pi Harness Events Extension shall drop the newly accepted observation and emit a payload-free diagnostic.
4. While accepted commands remain pending, the Pi Harness Events Extension shall dispatch them serially in acceptance order.
5. When `session_shutdown` occurs, the Pi Harness Events Extension shall attempt to flush pending commands and the session-end command within a finite shutdown bound.
6. If the shutdown bound expires, the Pi Harness Events Extension shall terminate its active child process and discard remaining pending commands.
7. The Pi Harness Events Extension shall preserve eligibility for lifecycle commands when observation capacity is exhausted.
8. The Pi Harness Events Extension shall not provide durable queueing, retry, replay, or delivery guarantees after process termination.

### Requirement 6: Failure Isolation and Data-Safe Diagnostics
**Objective:** As a Pi user, I want event submission failures isolated from Pi, so that telemetry cannot break interactive or headless operation.

#### Acceptance Criteria
1. If the harness events executable is absent, cannot start, times out, or exits non-zero, the Pi Harness Events Extension shall allow the triggering Pi operation to continue.
2. If event normalization, serialization, queueing, or handler execution fails, the Pi Harness Events Extension shall allow the triggering Pi operation to continue.
3. If event submission exceeds the configured execution bound, the Pi Harness Events Extension shall terminate that child process.
4. If event submission or queueing fails, the Pi Harness Events Extension shall emit a bounded diagnostic containing the failure category and event identity.
5. The Pi Harness Events Extension shall not include message content, tool arguments, tool output, native event payloads, CLI standard input, credentials, or environment values in diagnostics.
6. The Pi Harness Events Extension shall not describe a successful CLI exit as durable persistence, ordered delivery, or deduplication.

### Requirement 7: npm Package Quality
**Objective:** As a maintainer, I want a reproducible package toolchain aligned with Pi's Jiti loader, so that package changes have a strict quality gate.

#### Acceptance Criteria
1. The Pi Harness Events Extension shall be a self-contained TypeScript package managed with npm for dependency installation, locking, scripts, type checking, linting, formatting, unit tests, and package verification.
2. The Pi Harness Events Extension shall include a committed `package-lock.json` and shall not include Bun, Yarn, or pnpm lockfiles or commands.
3. The Pi Harness Events Extension shall expose its TypeScript source entrypoint through the package's Pi extension manifest for direct Jiti loading.
4. The Pi Harness Events Extension shall enable strict TypeScript checks without unchecked type or lint diagnostics in the package quality gate.
5. The Pi Harness Events Extension shall expose npm commands that separately run type checking, lint checking, format checking, unit tests, package verification, and their aggregate quality gate.
6. When the package archive is created, the Pi Harness Events Extension shall include its loadable TypeScript source and required package metadata without tests or development-only fixtures.
7. The Pi Harness Events Extension shall declare Pi's extension API as an unbundled peer dependency and pin Pi `0.86.1` for development and end-to-end verification.
8. The Pi Harness Events Extension shall declare only Node LTS majors covered by the compatibility matrix as supported runtimes.

### Requirement 8: Dependency Automation
**Objective:** As a maintainer, I want automated dependency updates for the npm package, so that Pi compatibility and tooling changes remain visible.

#### Acceptance Criteria
1. The repository dependency automation shall monitor the extension's `package.json` and `package-lock.json` using the npm ecosystem.
2. The repository dependency automation shall check the extension package on a weekly schedule.
3. When dependency automation updates package dependencies, the resulting change shall preserve an npm-installable locked dependency graph.
4. The repository dependency automation shall retain the existing Cargo update configuration.

### Requirement 9: Unit Verification
**Objective:** As a maintainer, I want fast package-level verification without external services, so that adapter behavior is deterministic during development.

#### Acceptance Criteria
1. The Pi Harness Events Extension shall run all package unit tests through an npm script on each supported Node LTS major.
2. The unit tests shall verify extension registration, accepted event selection, streamed-update exclusion, field derivation, lifecycle ordering, queue saturation, and shutdown behavior.
3. The unit tests shall verify command arguments, observation standard input, environment preservation, execution bounds, serial dispatch, and no-shell process invocation.
4. The unit tests shall verify fail-open behavior for hook, normalization, serialization, spawn, timeout, queue, and categorized CLI exit failures.
5. The unit tests shall verify that diagnostics exclude native payload, standard-input, credential, and environment content.
6. The unit tests shall run without a Pi process, harness events executable, iii engine, network access, or external credentials.

### Requirement 10: Live Headless Verification
**Objective:** As a maintainer, I want the shipped package verified across real process boundaries, so that test doubles cannot hide integration drift.

#### Acceptance Criteria
1. The repository shall run extension end-to-end tests through Cargo's test runner.
2. The end-to-end tests shall start a real iii engine instance and wait for readiness without a fixed startup sleep.
3. The end-to-end tests shall run pinned Pi `0.86.1` headlessly on Node 24 and every later declared LTS runtime.
4. The end-to-end tests shall load the extension package through its package manifest and Jiti entrypoint.
5. The end-to-end tests shall verify captured session-start, coarse observation, and session-end invocations with the expected session, project, directory, timestamp, event identity, and data fields.
6. The end-to-end tests shall exercise deterministic agent, message, and tool activity through a test-only scripted provider and tool.
7. The end-to-end tests shall use isolated Pi state, temporary state, cache, and working directories.
8. The end-to-end tests shall run Pi with runtime network activity disabled and without external model credentials or model requests.
9. If any child process fails, hangs, or violates the capture contract, the end-to-end tests shall terminate all owned child processes within bounded deadlines and report payload-free diagnostics.
