# Requirements Document

## Introduction
The OpenCode harness events plugin provides the adapter described in [brief.md](brief.md). It submits OpenCode lifecycle and coarse observation events through the existing `harness-events` process contract from one self-contained package.

## Boundary Context
- **In scope**: OpenCode v1 and v2 plugin loading, lifecycle and coarse observation mapping, deterministic CLI inputs, failure isolation, Bun-managed package checks, dependency automation, Bun unit tests, and Cargo-driven live-engine end-to-end tests.
- **Out of scope**: iii or protobuf clients in the plugin, CLI implementation or distribution, retries, durable delivery, global ordering or deduplication, streamed deltas, external model providers, and ingestion or persistence changes.
- **Adjacent expectations**: `harness-events-cli` remains authoritative for commands and process outcomes. The plugin treats a successful CLI exit as accepted invocation, not proof of persistence.

## Requirements

### Requirement 1: Dual-Generation Plugin Compatibility
**Objective:** As an OpenCode user, I want one plugin package for both supported generations, so that event submission does not require separate integrations.

#### Acceptance Criteria
1. The OpenCode Harness Events Plugin shall expose a loadable plugin entrypoint for the OpenCode v1 API beginning with version `1.18.29`.
2. The OpenCode Harness Events Plugin shall expose a loadable plugin entrypoint for the pinned OpenCode v2 `2.0.10` release.
3. When either supported OpenCode generation loads the package, the OpenCode Harness Events Plugin shall register only the hooks and event subscriptions needed by this specification.
4. When either supported OpenCode generation unloads the package, the OpenCode Harness Events Plugin shall release its subscriptions and bounded in-flight work.
5. If a native capability has no equivalent in the other generation, the OpenCode Harness Events Plugin shall omit that capability from the common event set unless this document names it explicitly.

### Requirement 2: Session Lifecycle Submission
**Objective:** As a memory pipeline operator, I want OpenCode sessions represented by lifecycle events, so that observations can be correlated to a session.

#### Acceptance Criteria
1. When a supported OpenCode instance reports a newly created session, the OpenCode Harness Events Plugin shall submit one `session-start` command for that session within the current plugin runtime.
2. When the first accepted event belongs to a resumed session that did not report creation, the OpenCode Harness Events Plugin shall submit `session-start` before submitting that event's observation.
3. When OpenCode reports session deletion, the OpenCode Harness Events Plugin shall submit one `session-end` command for that session within the current plugin runtime.
4. When the plugin unloads with tracked sessions still active, the OpenCode Harness Events Plugin shall attempt one `session-end` command for each tracked session within the shutdown bound.
5. The OpenCode Harness Events Plugin shall use the native OpenCode session identifier as the harness session identifier.
6. The OpenCode Harness Events Plugin shall not claim lifecycle uniqueness across plugin restarts, OpenCode restarts, or concurrent OpenCode processes.

### Requirement 3: Coarse Observation Submission
**Objective:** As a memory pipeline operator, I want useful OpenCode activity captured without per-token process churn, so that event volume remains bounded by meaningful activity.

#### Acceptance Criteria
1. When OpenCode emits an accepted session, prompt or message, tool, status, or error event, the OpenCode Harness Events Plugin shall submit one `observation` command containing that native event's JSON-compatible data.
2. The OpenCode Harness Events Plugin shall identify each observation by its OpenCode generation and native event or hook type.
3. The OpenCode Harness Events Plugin shall preserve accepted native event data as opaque JSON-compatible content without converting it into a cross-generation semantic schema.
4. When OpenCode emits a streamed text delta or reasoning delta, the OpenCode Harness Events Plugin shall not submit an observation for that delta.
5. If a native event cannot be represented as one JSON object, the OpenCode Harness Events Plugin shall skip the event and emit a payload-free diagnostic.
6. The OpenCode Harness Events Plugin shall not initiate a retry or a second CLI invocation for an accepted native event.

### Requirement 4: Deterministic CLI Inputs
**Objective:** As an operator, I want complete and deterministic CLI invocations, so that the existing harness boundary can validate every event.

#### Acceptance Criteria
1. The OpenCode Harness Events Plugin shall invoke the configured harness events executable without shell interpretation.
2. Where no executable override is configured, the OpenCode Harness Events Plugin shall resolve `harness-events` through the process environment.
3. The OpenCode Harness Events Plugin shall provide every field required by the selected lifecycle or observation command.
4. The OpenCode Harness Events Plugin shall derive the project name from OpenCode project context and use the working-directory basename only when OpenCode provides no project name.
5. The OpenCode Harness Events Plugin shall use the OpenCode project or session working directory as the current working directory.
6. The OpenCode Harness Events Plugin shall format generated event timestamps as RFC 3339 values with millisecond precision.
7. When an observation is submitted, the OpenCode Harness Events Plugin shall write exactly one JSON object to the CLI standard input and close the stream.
8. The OpenCode Harness Events Plugin shall preserve `III_URL` and `III_NAMESPACE` from its process environment for the CLI.

### Requirement 5: Failure Isolation and Data-Safe Diagnostics
**Objective:** As an OpenCode user, I want event submission failures isolated from my workflow, so that telemetry cannot break interactive or headless operation.

#### Acceptance Criteria
1. If the harness events executable is absent, cannot start, times out, or exits non-zero, the OpenCode Harness Events Plugin shall allow the triggering OpenCode operation to continue.
2. If event normalization or serialization fails, the OpenCode Harness Events Plugin shall allow the triggering OpenCode operation to continue.
3. If an event submission exceeds the configured execution bound, the OpenCode Harness Events Plugin shall terminate that child process.
4. If event submission fails, the OpenCode Harness Events Plugin shall emit a bounded diagnostic containing the failure category and event identity.
5. The OpenCode Harness Events Plugin shall not include prompt content, message content, tool arguments, tool output, native event payloads, or CLI standard input in diagnostics.
6. The OpenCode Harness Events Plugin shall not describe a successful CLI exit as durable persistence, ordered delivery, or deduplication.

### Requirement 6: Self-Contained Bun Package Quality
**Objective:** As a maintainer, I want one reproducible package toolchain, so that package changes have a strict and reviewable quality gate.

#### Acceptance Criteria
1. The OpenCode Harness Events Plugin shall be a self-contained TypeScript package managed with Bun for dependency installation, locking, scripts, build, type checking, linting, formatting, and unit tests.
2. The OpenCode Harness Events Plugin shall include a committed Bun lockfile and shall not require npm, Yarn, or pnpm lockfiles.
3. The OpenCode Harness Events Plugin shall enable strict TypeScript checks without unchecked type or lint diagnostics in the package quality gate.
4. The OpenCode Harness Events Plugin shall expose Bun commands that separately run type checking, lint checking, format checking, unit tests, build, and their aggregate quality gate.
5. When the package artifact is built, the OpenCode Harness Events Plugin shall include loadable JavaScript entrypoints and TypeScript declarations without test or development-only sources.
6. The OpenCode Harness Events Plugin shall declare compatible OpenCode APIs without installing an OpenCode CLI as a production runtime dependency.

### Requirement 7: Dependency Automation
**Objective:** As a maintainer, I want automated dependency updates for the Bun package, so that OpenCode compatibility and tooling changes remain visible.

#### Acceptance Criteria
1. The repository dependency automation shall monitor the plugin package manifest and Bun lockfile using GitHub Dependabot's Bun ecosystem integration.
2. The repository dependency automation shall check the plugin package on a weekly schedule.
3. When dependency automation updates package dependencies, the resulting change shall preserve a Bun-installable locked dependency graph.
4. The repository dependency automation shall retain the existing Cargo update configuration.

### Requirement 8: Bun Unit Verification
**Objective:** As a maintainer, I want fast package-level verification without external services, so that adapter behavior is deterministic during development.

#### Acceptance Criteria
1. The OpenCode Harness Events Plugin shall run all package unit tests with Bun's test runner.
2. The unit tests shall verify v1 and v2 loading, common event selection, streamed-delta exclusion, field derivation, lifecycle synthesis for resumed sessions, and cleanup behavior.
3. The unit tests shall verify command arguments, observation standard input, environment preservation, execution bounds, and no-shell process invocation.
4. The unit tests shall verify fail-open behavior for spawn, timeout, serialization, and categorized CLI exit failures.
5. The unit tests shall verify that diagnostics exclude native payload and standard-input content.
6. The unit tests shall run without an OpenCode process, harness events executable, iii engine, network access, or external credentials.

### Requirement 9: Live End-to-End Verification
**Objective:** As a maintainer, I want the shipped package verified across real process boundaries, so that unit-test doubles cannot hide integration drift.

#### Acceptance Criteria
1. The repository shall run plugin end-to-end tests through Cargo's test runner.
2. The end-to-end tests shall start a real iii engine instance and wait for readiness without a fixed startup sleep.
3. The end-to-end tests shall run a headless OpenCode v1 `1.18.29` process with the built plugin artifact and the built harness events executable.
4. The end-to-end tests shall run a headless OpenCode v2 `2.0.10` process with the same plugin artifact and harness events executable.
5. The end-to-end tests shall verify that each OpenCode generation causes captured session-start, coarse observation, and session-end invocations with the expected session, project, directory, timestamp, hook type, and data fields.
6. The end-to-end tests shall use isolated configuration, cache, and working directories for each OpenCode generation.
7. The end-to-end tests shall complete without external model credentials or network model requests.
8. If any child process fails, hangs, or violates the capture contract, the end-to-end tests shall terminate all owned child processes within bounded deadlines and report payload-free diagnostics.
