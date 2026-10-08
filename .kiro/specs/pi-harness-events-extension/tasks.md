# Implementation Plan

- [ ] 1. Establish the package and runtime foundation
- [x] 1.1 Create the npm package and dependency contract
  - Establish the Pi manifest, Node 24 engine range, unbundled Pi peer, exact development pins, empty production dependencies, npm lockfile, MIT metadata, and source-package allowlist.
  - Exclude Bun, Yarn, pnpm, the deprecated Pi package, bundled JavaScript, and a runtime Jiti dependency.
  - Completion is observable when a clean npm installation resolves the exact Pi `0.86.1` development graph on Node 24.
  - _Requirements: 1.1, 1.4, 1.6, 7.1, 7.2, 7.3, 7.7, 7.8_
  - _Boundary: Package Contract_

- [x] 1.2 Configure strict source and unit-test quality gates
  - Configure strict no-emit TypeScript with erasable syntax, Biome formatting and linting for source, tests, and JSON, Prettier Markdown checks, Node's TypeScript test runner, and separate npm quality commands.
  - Add a minimal default extension factory that type-checks against Pi `0.86.1` without starting a Pi process.
  - Completion is observable when typecheck, lint, format-check, unit-test, and aggregate commands pass from the locked install.
  - _Requirements: 1.1, 7.1, 7.4, 7.5, 9.1, 9.6_
  - _Boundary: Package Contract_

- [x] 1.3 Add Node 24 to the reproducible environment
  - Add Node 24 and bundled npm additively to every existing development-shell system while preserving adjacent tools.
  - Establish the named-shell pattern required to add later majors only after LTS entry and full validation.
  - Completion is observable when the flake evaluates on all systems and reports the pinned Node 24 and npm versions inside the shell.
  - _Requirements: 1.2, 1.3, 1.4, 1.5, 9.1_
  - _Boundary: Node and CI Matrix_

- [x] 1.4 Define shared submission and JSON contracts
  - Add strict JSON values, event metadata, lifecycle and observation submissions, closed dispatch outcomes, and identity-only diagnostics.
  - Normalize finite JSON-compatible plain data and reject unsupported, cyclic, or non-object observation roots.
  - Completion is observable when contract tests preserve valid nested values and reject every invalid category without exposing payload content.
  - _Requirements: 3.3, 3.6, 4.3, 6.4, 6.5, 9.4, 9.5, 9.6_
  - _Boundary: Shared Model_

- [ ] 2. Implement the independent runtime boundaries
- [x] 2.1 Implement validated extension configuration
  - Resolve the executable, execution deadline, absolute shutdown deadline, and observation capacity from environment values.
  - Enforce the designed minima, maxima, shutdown relationship, and safe defaults with bounded configuration diagnostics.
  - Completion is observable when boundary tests cover missing, valid, malformed, too-small, too-large, and inconsistent settings.
  - _Requirements: 4.1, 4.2, 5.2, 6.3, 6.4, 9.4, 9.5_
  - _Boundary: Configuration_

- [x] 2.2 Build deterministic command invocation
  - Produce exact lifecycle and observation argument vectors, working directories, inherited routing environment, and one-object observation stdin.
  - Spawn one child without shell interpretation and close lifecycle or observation stdin according to the CLI contract.
  - Completion is observable when recording-process tests prove exact arguments, working directory, environment preservation, stdin data, and stream closure.
  - _Requirements: 3.7, 4.1, 4.2, 4.3, 4.5, 4.8, 4.9, 9.3, 9.6_
  - _Boundary: Process Dispatcher_

- [x] 2.3 Add bounded process outcomes and diagnostics
  - Drain bounded output, classify documented and unexpected exits, and terminate timed-out or aborted children with escalation and exit confirmation.
  - Keep every outcome retry-free and every diagnostic independent of payloads, child output, credentials, paths, and environment values.
  - Completion is observable when process tests cover success, usage, connection, invocation, spawn, signal, unknown-exit, timeout, abort, and sentinel-safe diagnostics.
  - _Requirements: 3.7, 6.1, 6.3, 6.4, 6.5, 6.6, 9.3, 9.4, 9.5, 9.6_
  - _Boundary: Process Dispatcher_

- [x] 2.4 Implement bounded queue acceptance and FIFO dispatch
  - Accept one local start, up to the configured observation capacity, and one protected end while preserving FIFO dispatch.
  - Return from ordinary acceptance without awaiting completion, drop only incoming observations at saturation, and report identity-only overflow diagnostics.
  - Keep accepted work finite and prevent duplicate local lifecycle commands without adding persistence or retries.
  - Completion is observable when injected-dispatch tests cover synthesized and duplicate starts, capacity plus lifecycle bounds, FIFO order, saturation, and successful drain.
  - _Requirements: 2.1, 2.2, 2.3, 2.5, 2.6, 5.1, 5.2, 5.3, 5.4, 5.7, 5.8, 6.2, 9.2, 9.3, 9.4_
  - _Boundary: Queue Runtime_

- [x] 2.5 Enforce bounded shutdown and active-child cleanup
  - Reject new observations, append one end command, and drain normally until the absolute deadline reaches its termination reserve.
  - At the reserve boundary, discard queued work before terminating the active child, escalating after the grace period, and confirming exit before the deadline.
  - Completion is observable when deadline-controlled tests cover empty and busy drains, queued session-end work, hung active children, discard order, escalation, reaping, and bounded return.
  - _Requirements: 2.3, 2.4, 5.5, 5.6, 6.2, 6.3, 9.2, 9.3, 9.4_
  - _Boundary: Queue Runtime, Process Dispatcher_
  - _Depends: 2.3, 2.4_

- [x] 2.6 (P) Implement Pi event selection and metadata extraction
  - Identify the exact accepted and excluded native event matrix without registering live handlers.
  - Derive session identity, absolute working directory, project basename, event identity, and native or handler-entry timestamp from supplied context.
  - Completion is observable when table-driven tests cover every matrix row, missing metadata, native timestamp source, and fallback timestamp.
  - _Requirements: 3.1, 3.2, 3.4, 4.4, 4.5, 4.6, 4.7, 9.2, 9.6_
  - _Boundary: Pi Adapter_
  - _Depends: 1.4_

- [x] 2.7 Implement coarse event projection and filtering
  - Project approved session, agent, wait, turn, completed-message, tool, and error fields into one versioned native envelope.
  - Exclude provider traffic, system prompts, images, thinking, streamed updates, interception data, and duplicate embedded messages or tool results.
  - Contain projection failures with identity-only diagnostics.
  - Completion is observable when projection tests exercise all native error forms, allowed-field golden fixtures, prohibited-content sentinels, and normalization failures.
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 6.2, 6.4, 6.5, 9.2, 9.4, 9.5, 9.6_
  - _Boundary: Pi Adapter_

- [ ] 3. Assemble and package the loadable extension
- [x] 3.1 Register the approved Pi subscriptions
  - Register only the approved native events from the default factory and read current context inside every live handler.
  - Convert handler input through the tested metadata and projection boundary without capturing stale session objects.
  - Catch registration and handler failures, including Pi's fail-closed paths, and retain unsubscribe callbacks for shutdown.
  - Completion is observable when a recording Pi API sees the exact subscriptions and every handler returns normally after injected failures.
  - _Requirements: 1.1, 1.2, 3.1, 3.2, 6.1, 6.2, 9.2, 9.4, 9.6_
  - _Boundary: Pi Adapter_

- [x] 3.2 Integrate lifecycle, queue, dispatcher, and shutdown
  - Route session start, projected observations, and session shutdown through one configured queue and dispatcher.
  - Release subscriptions, reject new observations, queue one end, and complete the absolute shutdown sequence before returning to Pi.
  - Verify startup resume, reload, new-session, and fork replacement ordering through injected component seams.
  - Completion is observable when all lifecycle transitions preserve local start-to-observation-to-end order and telemetry failures never block Pi.
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 5.1, 5.5, 5.6, 6.1, 6.2, 9.2, 9.4_
  - _Boundary: Pi Adapter, Queue Runtime, Process Dispatcher_
  - _Depends: 2.3, 2.5, 3.1_

- [x] 3.3 Verify the distributable source package
  - Create a real npm tarball containing only the manifest, Jiti-loadable source, README, and license.
  - Inspect and install it into isolated staged state without scripts or peer installation, then verify package-root manifest discovery.
  - Completion is observable when package verification rejects unexpected tests, fixtures, lockfiles, JavaScript, or development assets and accepts the isolated installation.
  - _Requirements: 1.1, 7.3, 7.5, 7.6, 7.7, 7.8, 9.1, 9.6_
  - _Boundary: Package Contract_

- [x] 3.4 (P) Add dependency automation and generated-asset ignores
  - Add a weekly npm update entry scoped to the extension while preserving Cargo and independently landed adapter entries.
  - Ignore package dependencies, coverage, test state, and generated tarballs without ignoring the npm lockfile.
  - Completion is observable when automation syntax validates and repository status excludes only generated assets after package checks.
  - _Requirements: 7.2, 8.1, 8.2, 8.3, 8.4_
  - _Boundary: Repository Automation_
  - _Depends: 1.1_

- [ ] 4. Build the headless Pi end-to-end harness
- [x] 4.1 Establish the private Cargo test package and artifact contract
  - Register the private test package in the workspace, add exact shared dependency pins, enable E2E-only runtime features locally, and update the lockfile.
  - Validate explicit Node, Pi, iii, CLI, staged extension, and fixture artifact inputs before starting processes.
  - Completion is observable when workspace metadata includes the test package and harness tests reject every missing or wrong artifact with bounded errors.
  - _Requirements: 10.1, 10.3, 10.7, 10.9_
  - _Boundary: Cargo E2E Runner_

- [x] 4.2 Implement owned process cleanup and bounded capture
  - Start children in owned Unix process groups and drain bounded output under explicit readiness and execution deadlines.
  - Provide asynchronous TERM-to-KILL cleanup plus an unwind-safe kill fallback for errors, timeouts, and panics.
  - Completion is observable when process tests prove direct and descendant children are reaped and output remains bounded on every exit path.
  - _Requirements: 10.7, 10.9_
  - _Boundary: Process Guard_

- [x] 4.3 Implement real iii capture and readiness
  - Start real iii with isolated loopback configuration and disabled update and telemetry work.
  - Register all three capture functions, retain registrations, return the authoritative success object, and record identity, namespace, payload, and order.
  - Poll function visibility rather than sleeping.
  - Completion is observable when an infrastructure test proves all capture functions become routable and cleanup leaves no engine process.
  - _Requirements: 10.2, 10.5, 10.7, 10.9_
  - _Boundary: Engine Harness_

- [x] 4.4 Create the offline scripted provider and fixture tool
  - Register one zero-cost scripted model and one deterministic tool without network, subprocess, timer, or filesystem I/O.
  - Emit a first assistant tool call, a successful fixture result, and a second final assistant response with complete Pi-compatible records.
  - Completion is observable when a fixture contract test proves the two provider turns and tool result are deterministic without credentials or I/O.
  - _Requirements: 10.6, 10.8_
  - _Boundary: Scripted Fixture_

- [x] 4.5 Run the staged package through isolated headless Pi
  - Invoke the pinned local Pi CLI module with the selected Node binary, JSON mode, runtime-offline controls, disabled ambient resources, explicit package roots, and the scripted provider and tool.
  - Isolate home, temporary, XDG, Pi state, session, package, npm cache, and working directories.
  - Use the process guard for bounded output, deadlines, and complete cleanup.
  - Completion is observable when the scenario loads the staged package through its manifest and Jiti entrypoint and Pi emits the expected user, assistant, tool, turn, agent, and shutdown activity without model network access.
  - _Requirements: 1.2, 1.3, 10.3, 10.4, 10.6, 10.7, 10.8, 10.9_
  - _Boundary: Pi Scenario, Process Guard_
  - _Depends: 3.3, 4.2, 4.4_

- [x] 4.6 Enable live Pi-to-iii verification after the CLI lands
  - Keep this task blocked until the approved `harness-events-cli` implementation provides its real release binary and documented process outcomes.
  - Combine the real engine, capture functions, headless Pi, staged extension, and explicit real CLI artifact in one serial ignored Cargo test; never add a fake production fallback.
  - Wait for shutdown delivery and assert one start, expected coarse observations, one end, FIFO order, metadata, namespace, event identities, data fields, and clean teardown on success and forced failure.
  - Completion is observable only when the live test passes through real iii with the upstream binary and leaves no process or temporary state.
  - _Requirements: 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8, 10.9_
  - _Boundary: Cargo E2E Runner, Engine Harness, Pi Scenario_
  - _Depends: 4.3, 4.5_

- [ ] 5. Integrate repository quality gates
- [x] 5.1 Add the separate Pi Node LTS CI matrix
  - Keep this task blocked until task 4.6 and the upstream real CLI artifact are complete.
  - Add a dedicated matrix job containing Node 24 initially, without matrix-expanding or weakening the existing Rust job.
  - For each declared LTS major, run clean npm installation, aggregate checks, tarball staging, CLI release build, and the serial live Cargo test with explicit artifact paths.
  - Preserve all independently landed worker and adapter jobs.
  - Completion is observable when workflow validation passes and every declared Node engine range has matching package and live-E2E coverage.
  - _Requirements: 1.2, 1.3, 1.5, 7.8, 9.1, 10.1, 10.3, 10.4, 10.9_
  - _Boundary: Node and CI Matrix_
  - _Depends: 3.4, 4.6_

- [x] 5.2 Prove the complete supported-runtime contract
  - Run clean npm checks, archive verification, workspace tests, formatting, linting, release builds, and the live matrix from the reproducible environment.
  - Confirm only matrix-backed LTS majors are declared supported and no Bun, Yarn, pnpm, deprecated Pi, model-network, or fake-CLI path remains.
  - Verify failure logs and diagnostics remain bounded and payload-free across package and Cargo tests.
  - Completion is observable when all existing and new gates pass from a clean checkout with pinned Pi `0.86.1` and Node 24.
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 6.4, 6.5, 6.6, 7.1, 7.2, 7.8, 8.3, 8.4, 9.1, 9.5, 9.6, 10.1, 10.3, 10.8, 10.9_
  - _Boundary: Package Contract, Cargo E2E Runner, Node and CI Matrix_

## Implementation Notes

- Task 1.3: Keep `nixpkgs-26.05-darwin` for four-system evaluation and retain the locked Rust overlay so every shell satisfies workspace Rust `1.98.1`.
- Task 1.4: Use `normalizeJsonObject` before submitting native data; it rejects hidden, accessor, symbol, sparse, and augmented values rather than silently dropping them.
- Task 2.1: Emit fresh frozen diagnostics with only `{ category }`; never attach configuration or environment state to a console record.
- Task 2.2: `ProcessSpawner` distinguishes an all-pipes child from unavailable stdio; task 2.3 must drain and observe the typed child handle without treating unavailable stdio as success.
- Task 2.3: Dispatcher results are terminal and retry-free; timeout or abort uses TERM, KILL after 500ms, then a bounded confirmation fallback with frozen identity-only diagnostics.
- Task 2.4: Queue capacity includes active and pending observations; the baseline close drains normally and task 2.5 must add its absolute shutdown deadline without displacing lifecycle work.
- Task 2.5: Only a private registry for the exact dispatcher promise may defer the final shutdown fallback; injected dispatch wrappers receive no shutdown authority.
- Task 2.6: Pi `message_end` role is `event.message.role`; capture fallback time as the first handler operation before state, event, or context access and keep every declared excluded event explicit.
- Task 4.2: In this linked worktree, validate Rust with `nix develop --no-write-lock-file "path:$PWD#node24"` and isolated `CARGO_HOME`/`CARGO_TARGET_DIR`; normal flake Git resolution cannot read the parent worktree metadata.
- Task 4.2: A shell-redirection-created PID file is not published until its ready marker exists; wait for the marker before reading the PID.
- Remediation R4: After reaping the direct leader, treat `ESRCH` or `EPERM` from a process-group ownership probe as no remaining owned group; never escalate a stale PGID, while actual delivery errors still fail.
- Task 4.3: Reap the iii process group by its deadline before joining the SDK client, because client shutdown can block on its connection thread.
- Task 4.4: Test-only Pi fixtures import `@earendil-works/pi-ai/compat`; Pi's Jiti loader maps it to the host runtime without a shipped direct dependency.
- Task 4.5: Treat the staged harness-events artifact as trusted CI input, then validate its bounded root and subcommand help grammars before running the fail-open Pi scenario.
- Task 4.6: Capture delivery waits must return errors so iii is explicitly reaped before assertions; native fixture timestamps are asserted against exact RFC3339 values.
- Task 5.1: The Pi CI job packs into a temporary consumer and supplies all six explicit artifacts to the serial ignored live test; it never uses the source package root.
- Task 5.2: Full all-systems flake evaluation, clean Node 24 package checks, workspace Rust gates, release builds, and explicit live E2E passed with isolated caches.
