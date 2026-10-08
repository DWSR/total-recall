# Implementation Plan

> Live end-to-end execution and CI activation require the approved `harness-events-cli` spec to have landed a buildable `harness-events-cli` Cargo package and `harness-events` binary. Package, adapter, unit-test, and E2E harness work can proceed before that prerequisite; no task may add a fake production CLI fallback.

- [x] 1. Establish the Bun package foundation
- [x] 1.1 Scaffold the self-contained strict Bun package
  - Establish the package manifest, committed text lockfile, ESM contract, type-only OpenCode development dependencies, package allowlist, and license assets.
  - Configure strict TypeScript checking and emission plus Biome formatting and all stable lint rules with warnings treated as errors.
  - Expose separate type-check, lint, format, format-check, test, build, pack, and aggregate-check commands through Bun.
  - A frozen Bun install succeeds, and every script and tool configuration resolves without npm, Yarn, or pnpm state.
  - _Requirements: 6.1, 6.2, 6.3, 6.4_
  - _Boundary: Package Contract_

- [x] 1.2 Define shared submission and configuration contracts
  - Add recursive JSON types, generation metadata, lifecycle and observation submissions, categorized outcomes, and diagnostics without `any` or unsafe boundary casts.
  - Resolve the CLI executable and bounded execution and shutdown timeouts from environment values with safe defaults for absent or invalid values.
  - Keep the configuration and model layer independent from OpenCode hosts, child processes, and iii.
  - Bun tests verify defaults, maximums, invalid values, discriminated outcomes, and external-service-free execution.
  - _Requirements: 4.2, 4.3, 6.3, 8.3, 8.6_
  - _Boundary: Shared Model, Config_
  - _Depends: 1.1_

- [x] 1.3 (P) Add Bun dependency automation and generated-state exclusions
  - Add weekly Dependabot coverage with the Bun ecosystem while preserving the existing Cargo entry.
  - Ignore installed dependencies, build output, and package tarballs without ignoring the committed Bun lockfile.
  - Verify that a dependency update can produce a frozen-installable manifest and text lockfile pair.
  - The repository contains one Cargo update policy and one package-local Bun update policy with no npm ecosystem entry for the plugin.
  - _Requirements: 6.2, 7.1, 7.2, 7.3, 7.4_
  - _Boundary: Package Contract_
  - _Depends: 1.1_

- [x] 1.4 (P) Pin the supported Bun and OpenCode test tools
  - Validate reproducible Nix inputs for Bun 1.4.2, OpenCode v1 1.18.29, and OpenCode v2 2.0.10 before wiring the live test.
  - Expose distinct v1 and v2 commands on ARM macOS and Linux while retaining the existing Rust-only Intel macOS shell.
  - Keep iii 0.24.0 and the existing Rust tools unchanged.
  - On each supported live-test system, all three commands report the exact designed versions without registry access.
  - _Requirements: 6.1, 9.3, 9.4_
  - _Boundary: E2E Toolchain_

- [x] 2. Implement the process and lifecycle core
- [x] 2.1 Implement one typed CLI invocation
  - Convert lifecycle and observation submissions into the approved commands, required options, and one-object observation stdin.
  - Spawn without a shell, inherit the process environment unchanged, close stdin deterministically, and consume one application invocation per submission.
  - Treat serialization failure as a categorized fail-open result rather than an exception to the caller.
  - Unit tests record exact arguments, stdin, environment, and `0`, `2`, `3`, and `4` outcome mapping without a real executable.
  - _Requirements: 3.6, 4.1, 4.2, 4.3, 4.7, 4.8, 5.1, 5.2, 5.6, 8.3, 8.4_
  - _Boundary: Dispatcher_
  - _Depends: 1.2_

- [x] 2.2 Contain process failures and diagnostics
  - Apply execution abort and timeout, termination-to-kill escalation, and bounded stdout and stderr draining.
  - Classify spawn, signal, timeout, usage, connection, invocation, and serialization failures without retrying.
  - Emit bounded diagnostics containing only category and event identity; discard payloads, child output, remote errors, paths, and stack traces.
  - Tests prove every failure returns to the caller, kills owned children when required, and never exposes sentinel payload content.
  - _Requirements: 5.1, 5.3, 5.4, 5.5, 8.4, 8.5_
  - _Boundary: Dispatcher_

- [x] 2.3 (P) Track and order sessions within one plugin runtime
  - Track active native session IDs with reusable metadata and one ordinary submission chain per session.
  - Synthesize start before the first observation, suppress duplicate local starts and ends, and place deletion end after its observation.
  - Allow different sessions to progress concurrently without claiming ordering or uniqueness across runtime boundaries.
  - Inject a recording dispatch function; unit tests observe start-to-observation-to-end order and resumed-session behavior.
  - _Requirements: 2.1, 2.2, 2.3, 2.5, 2.6, 3.6, 8.2_
  - _Boundary: Session Runtime_
  - _Depends: 1.2_

- [x] 2.4 Close active sessions within the shutdown bound
  - Reject newly arriving work once close begins and abort ordinary chains that could starve shutdown.
  - Start one dedicated end attempt for every still-active session and run those attempts concurrently under the shutdown timeout.
  - Clear runtime state after completion or abort while containing every dispatch failure.
  - Unit tests observe one attempted end per active session, bounded return, aborted queued work, and empty final state.
  - _Requirements: 2.4, 5.1, 5.2, 5.3, 8.2, 8.4_
  - _Boundary: Session Runtime_
  - _Depends: 2.3_

- [x] 3. Add generation adapters and the package entrypoint
- [x] 3.1 Implement the OpenCode v1 lifecycle and event adapter
  - Select the designed v1 lifecycle, session-state, message, and error events while excluding streamed message-part deltas.
  - Extract native session identity, directory, project basename, and millisecond timestamp with the documented fallbacks.
  - Preserve each accepted native event in the generation/kind/payload envelope and hand unknown sessions to runtime synthesis.
  - Reject malformed or non-object native data with a payload-free diagnostic; host callbacks never reject because normalization fails.
  - Bun tests cover every accepted and excluded v1 event, metadata fallback, opaque payload preservation, and fail-open normalization.
  - _Requirements: 1.1, 1.3, 1.5, 2.1, 2.2, 2.3, 2.5, 2.6, 3.1, 3.2, 3.3, 3.4, 3.5, 4.3, 4.4, 4.5, 4.6, 5.1, 5.2, 8.2_
  - _Boundary: V1 Adapter_
  - _Depends: 2.3, 2.4_

- [x] 3.2 (P) Implement the OpenCode v2 event subscription adapter
  - Subscribe to the designed v2 lifecycle, execution-state, completed text/reasoning, and failure events while excluding deltas and progress.
  - Extract native session identity, event location, project basename, and encoded event timestamp with context fallbacks.
  - Preserve each accepted native event in the generation/kind/payload envelope and hand unknown sessions to runtime synthesis.
  - Reject malformed or non-object native data with a payload-free diagnostic; host callbacks never reject because normalization fails.
  - Bun tests cover every accepted and excluded v2 event, metadata fallback, opaque payload preservation, and fail-open normalization.
  - _Requirements: 1.2, 1.3, 1.5, 2.1, 2.2, 2.3, 2.5, 2.6, 3.1, 3.2, 3.3, 3.4, 3.5, 4.3, 4.4, 4.5, 4.6, 5.1, 5.2, 8.2_
  - _Boundary: V2 Adapter_
  - _Depends: 2.3, 2.4_

- [x] 3.3 (P) Add OpenCode v1 prompt and tool hooks with disposal
  - Forward `chat.message` and tool before/after hook objects through the same opaque observation contract.
  - Apply the same JSON narrowing, metadata derivation, diagnostic secrecy, and fail-open callback behavior as event observations.
  - Stop v1 hook input before asking the runtime to close active sessions.
  - Bun tests observe prompt/tool submissions, malformed-hook containment, and dispose ordering without an OpenCode process.
  - _Requirements: 1.3, 1.4, 1.5, 2.4, 3.1, 3.2, 3.3, 3.5, 5.1, 5.2, 8.2_
  - _Boundary: V1 Adapter_
  - _Depends: 3.1_

- [x] 3.4 (P) Add OpenCode v2 prompt and tool hooks with cleanup
  - Forward prompt and tool before/after hook objects through the same opaque observation contract.
  - Apply the same JSON narrowing, metadata derivation, diagnostic secrecy, and fail-open callback behavior as event observations.
  - Abort the event subscription, dispose registrations, and then ask the runtime to close active sessions.
  - Bun tests observe prompt/tool submissions, malformed-hook containment, subscription release, and cleanup ordering without an OpenCode process.
  - _Requirements: 1.3, 1.4, 1.5, 2.4, 3.1, 3.2, 3.3, 3.5, 5.1, 5.2, 8.2_
  - _Boundary: V2 Adapter_
  - _Depends: 3.2_

- [x] 3.5 Integrate both generations into the distributable package
  - Connect both adapters to the real runtime and dispatcher through one plain default export with v1 `server` and v2 `setup`.
  - Emit ESM JavaScript and TypeScript declarations with no production dependency on OpenCode CLI or plugin packages.
  - Pack the allowlisted artifact and load both entrypoints from a clean consumer rather than the source tree.
  - The aggregate Bun check passes, and artifact tests show only built code, declarations, manifest, README, and license.
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 5.1, 6.5, 6.6, 8.1, 8.6_
  - _Boundary: Dual Entrypoint, Package Contract_
  - _Depends: 2.1, 2.2, 3.3, 3.4_

- [x] 4. Build the Cargo live-test infrastructure
- [x] 4.1 Establish the private Cargo E2E crate
  - Register a non-publishable workspace crate with ordinary dependencies for its source modules and an ignored serial live-test target.
  - Add compile-only artifact path validation and module seams for process, engine, and OpenCode orchestration.
  - Keep live execution inactive until explicit artifact paths are supplied.
  - The package-specific Cargo test command compiles and reports the live test ignored without Bun, OpenCode, iii, or CLI artifacts.
  - _Requirements: 9.1_
  - _Boundary: Cargo E2E Runner_

- [x] 4.2 Own and clean complete child process trees
  - Start iii and OpenCode in owned Unix process groups with bounded stdout and stderr capture.
  - Implement normal async termination, bounded wait, process-group kill escalation, and an unwind-safe best-effort kill fallback.
  - Apply deadlines to startup, readiness, scenario execution, and shutdown.
  - Rust tests prove direct children and descendants are removed after success, returned error, timeout, and panic.
  - _Requirements: 9.6, 9.8_
  - _Boundary: Process Guard_
  - _Depends: 4.1_

- [x] 4.3 Run a real iii capture boundary
  - Start iii 0.24.0 with temporary loopback configuration and disabled telemetry and update checks.
  - Register capture handlers for all three harness functions and return the exact successful dispatch object.
  - Wait for SDK registration and function visibility rather than a sleep or TCP-only probe.
  - Tests observe ordered function IDs, namespaces, payloads, readiness, and complete engine cleanup.
  - _Requirements: 9.2, 9.5, 9.8_
  - _Boundary: Engine Harness_
  - _Depends: 4.2_

- [x] 4.4 (P) Drive an isolated provider-free OpenCode v1 scenario
  - Install the packed plugin into a clean Bun consumer and generate isolated home, XDG, config, cache, state, and project roots.
  - Start the exact v1 headless server with model fetch, auto-update, downloads, and file watching disabled.
  - Use authenticated readiness plus create and delete session APIs without model credentials or requests.
  - The scenario returns the exact host version, native session ID, fixture directory, and bounded child logs for matrix assertions.
  - _Requirements: 9.3, 9.6, 9.7, 9.8_
  - _Boundary: OpenCode Scenarios_
  - _Depends: 1.4, 3.5, 4.2_

- [x] 4.5 Add the provider-free OpenCode v2 scenario
  - Reuse the packed artifact and isolation contract with generation-specific plugin configuration and API routes.
  - Start the exact v2 headless server and validate its authenticated readiness response before creating a session.
  - Create and delete the session without provider credentials or network model activity.
  - The scenario returns the exact v2 version and the same assertion inputs as v1 while leaving no owned process alive.
  - _Requirements: 9.4, 9.6, 9.7, 9.8_
  - _Boundary: OpenCode Scenarios_
  - _Depends: 4.4_

- [x] 4.6 Compose the serial real-process compatibility matrix
  - Accept explicit paths for iii, both OpenCode commands, the packed plugin, and the built `harness-events` binary; do not build recursively or substitute a fake CLI.
  - Start only after the upstream `harness-events-cli` implementation provides the required package and binary.
  - Run one iii engine and one capture worker while executing v1 and v2 headless scenarios serially.
  - Assert one start, creation and deletion observations, one end, local order, exact fields, native payloads, millisecond timestamps, and complete cleanup for each generation.
  - The ignored Cargo live test passes with no external model credentials or registry access and fails with payload-free bounded diagnostics on any contract mismatch.
  - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8_
  - _Boundary: Cargo E2E Runner_
  - _Depends: 4.3, 4.5_

- [x] 5. Integrate repository gates and validate the feature
- [x] 5.1 Add the package and live matrix to CI
  - Preserve existing Nix, Cargo, formatting, actionlint, Clippy, release-build, and protocol-smoke gates.
  - Run frozen Bun checks, pack the plugin, build the release CLI, then invoke the ignored serial Cargo live test with explicit artifact paths.
  - Activate the live gate only after the upstream CLI package has landed; do not weaken the test while it is blocked.
  - Keep the existing timeout unless measured execution demonstrates a required increase.
  - CI runs the package quality gate and both exact OpenCode generations in the designed order.
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 7.3, 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8_
  - _Boundary: CI Integration_
  - _Depends: 1.3, 3.5, 4.6_

- [x] 5.2 Verify the complete package and repository contract
  - Run frozen package install, aggregate Bun checks, artifact inspection, workspace Cargo tests, formatting, Clippy, flake checks, and the live compatibility matrix.
  - Seed prompt, tool, error, and native-event sentinels through unit seams and confirm no diagnostic or child argument exposes them.
  - Confirm the package has no production dependency on either OpenCode generation, iii, protobuf, or repository worker crates.
  - All documented gates pass from clean state, and every spawned live-test process is gone after completion.
  - _Requirements: 5.5, 6.3, 6.5, 8.1, 8.5, 8.6, 9.8_
  - _Boundary: Integrated Validation_
  - _Depends: 5.1_
