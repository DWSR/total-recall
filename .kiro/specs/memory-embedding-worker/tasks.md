# Implementation Plan

- [ ] 1. Establish the worker foundation and contracts
- [x] 1.1 Create the private worker package and launch baseline
  - Register the worker package and release binary with exact workspace dependencies, a complete module shell, the worker manifest, and locked resolution.
  - Keep the package independent from MCP, session processing, and direct provider libraries while recording the required upstream launch contract.
  - Completion is observable when workspace metadata loads and the empty worker library and binary compile with the locked toolchain.
  - _Requirements: 1.1, 1.4, 9.7_
  - _Boundary: Worker Package_

- [x] 1.2 Validate destinations and routing configuration
  - Validate the memory target, queue topic, cron expression, provider, model, managed iii values, and required effective `default` namespace before registration.
  - Preserve existing managed URL and worker-name behavior while keeping provider/model identifiers and routing values out of diagnostics.
  - Completion is observable when pure tests cover required, blank, malformed, default, and non-default namespace cases without mutating process environment.
  - _Requirements: 1.1, 1.4, 1.5, 5.1, 8.3_
  - _Boundary: Config_

- [x] 1.3 Validate resource and deadline policy
  - Validate event-key, batch, reconciliation, input-byte, local-wait, database, router, invocation, and shutdown limits.
  - Enforce the event/reconciliation-to-batch relationships and the router-plus-database budget beneath cron's caller deadline.
  - Completion is observable when pure tests accept every designed boundary and reject zero, overflow, and inconsistent cross-field settings.
  - _Requirements: 1.4, 7.3, 7.5_
  - _Boundary: Config_

- [x] 1.4 Define strict work, router, outcome, and failure contracts
  - Define the direct row-change event, canonical memory key and work states, generated vector, content-free outcomes, and closed stage/reason errors.
  - Provide recording and failing repository, router, and writer ports for deterministic service tests.
  - Completion is observable when contract tests reject unknown or protected fields, preserve only approved success data, and prove all error formatting is content-safe.
  - _Requirements: 2.1, 2.2, 2.3, 2.6, 3.6, 5.2, 5.3, 5.4, 6.4, 7.1, 7.2, 8.1, 8.2, 8.4, 9.1_
  - _Boundary: Contracts_

- [ ] 2. Implement work discovery and canonical input
- [x] 2.1 Load queued memory keys and embedding state
  - Build one static bounded read that returns each requested key in input order as pending, already present, or missing with strict all-or-error decoding.
  - Expose only title, content, and ordered concepts for pending work and map timeout, malformed, and backend outcomes to opaque repository failures.
  - Completion is observable when adapter tests verify exact database envelopes, positional parameters, every state, no retry, and protected-sentinel exclusion.
  - _Requirements: 2.4, 2.5, 6.3, 6.4, 8.1, 8.2, 9.1_
  - _Boundary: Work Repository_

- [x] 2.2 Add stateless all-version missing-work selection
  - Select a bounded anti-join page across every immutable memory version without an embedding in C-collated ID and ascending version order.
  - Return complete embedding work only and retain deterministic first-page reselection without attempts, claims, cursors, or terminal skips.
  - Completion is observable when query and decoder tests prove empty, bounded, historical-version, malformed, timeout, and repeat-selection outcomes.
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.7, 8.1, 8.2, 9.2_
  - _Boundary: Work Repository_

- [x] 2.3 (P) Render canonical embedding input version 1
  - Serialize compact fixed-field JSON containing the format marker, exact title, exact content, and ordered concepts only.
  - Enforce the UTF-8 byte limit after serialization without trimming, normalizing, or deriving memory text.
  - Completion is observable when snapshots lock field order, escaping, Unicode, empty and duplicate concepts, excluded provenance, and the exact size boundary.
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 9.3_
  - _Boundary: Canonical Renderer_
  - _Depends: 1.4_

- [ ] 3. Implement external embedding adapters
- [x] 3.1 Invoke and validate routed embedding generation
  - Submit one bounded input batch only to `router::embed` with the configured provider and model and the local timeout.
  - Decode components as float32, verify provider, model, vector count, dimensions, finite values, and non-zero norms, then associate vectors positionally and widen losslessly.
  - Completion is observable when adapter tests capture exact requests and classify success, remote error, timeout, mismatch, malformed, invalid-vector, and no-retry outcomes without protected data.
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 7.3, 7.4, 8.1, 8.2, 9.4_
  - _Boundary: Router Adapter_

- [x] 3.2 Adapt immutable embedding insertion
  - Delegate valid vectors to the canonical memory-store association using the exact memory key and shared database target.
  - Map confirmed inserts to stored, duplicate conflicts to already present, and missing-parent, timeout, malformed, or backend results to opaque failures without updates or deletes.
  - Completion is observable when recording and failing tests prove one attempt, exact float32-to-float64 values, idempotent conflict handling, no retry, and no direct embedding-table mutation.
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 8.1, 8.2, 9.5_
  - _Boundary: Embedding Writer_

- [ ] 4. Build shared queue and reconciliation behavior
- [x] 4.1 Implement strict row-change event adaptation
  - Parse the direct queue payload and validate unknown fields, database, canonical table, insert operation, affected-row count, event time, unique canonical string keys, truncation, and the key bound.
  - Produce validated memory keys only after the complete event passes; invalid events perform no repository or router activity.
  - Completion is observable when contract tests cover every accepted field and malformed, unrelated, duplicate, truncated, oversized, or noncanonical rejection.
  - _Requirements: 2.1, 2.2, 2.3, 2.6, 8.1, 8.2, 9.1_
  - _Boundary: Event Adapter_

- [x] 4.2 Integrate the bounded embedding pipeline
  - Coordinate canonical rendering, one routed batch, positional association, and concurrent immutable writes under one absolute deadline and the shared local-wait permit pool.
  - Reject an entire router response and perform no writes from it when provider, model, count, or any vector validation fails.
  - Continue independent admitted items after local render or write failures and return content-free counts only when the operation has no failures.
  - Completion is observable when recording and failing tests cover complete, already-present, partial-write, router-failure, local-timeout, local-permit, and upstream-continues-without-write behavior.
  - _Requirements: 3.4, 3.5, 5.2, 5.4, 5.6, 6.1, 6.2, 6.3, 6.4, 7.3, 7.4, 8.1, 8.2_
  - _Boundary: Embedding Coordinator_
  - _Depends: 2.3, 3.1, 3.2_

- [x] 4.3 Integrate queue-event processing and outcomes
  - Connect validated row-change keys to exact loading, already-present suppression, and the shared embedding pipeline as an explicit cross-boundary flow.
  - Fail the delivery if any key is missing or unsuccessful while retaining embeddings committed for other keys; retries observe those rows as already present.
  - Completion is observable when tests cover valid multi-key delivery, malformed no-call rejection, missing rows, partial-commit retry, exact success/error outcomes, and a content-free event contract.
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 7.1, 7.2, 7.6, 8.1, 8.2, 9.1, 9.5_
  - _Boundary: Event Adapter, Work Repository, Embedding Coordinator_
  - _Depends: 2.1, 4.1, 4.2_

- [x] 4.4 Integrate bounded stateless reconciliation
  - Validate the typed cron invocation, select one deterministic missing-work page, and submit at most one batch under the sub-30-second invocation deadline.
  - Return zero or successful content-free counts, retain partial committed embeddings on error, and add no internal retry, cursor, attempt state, no-starvation claim, or eventual-completeness claim.
  - Completion is observable when tests cover empty, historical-version, deterministic limit, concurrent already-present race, partial failure, repeated poison-row selection, deadline, and later-invocation repair.
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 7.3, 7.6, 8.1, 8.4, 9.2_
  - _Boundary: Work Repository, Embedding Coordinator_
  - _Depends: 2.2, 4.2_

- [ ] 5. Integrate iii runtime and process lifecycle
- [x] 5.1 Build the two-function and two-trigger registration plan
  - Register the queue and reconciliation functions with one per-start nonce and retain both function handles.
  - Bind the canonical durable topic with retry/concurrency configuration and the UTC cron expression in namespace `default`, retaining both trigger handles.
  - Completion is observable when pure plan tests expose exact function IDs, direct payload contracts, trigger types and configuration, metadata, target/provider namespaces, and launch prerequisites.
  - _Requirements: 1.1, 1.2, 1.4, 1.5, 7.3_
  - _Boundary: Worker Runtime_

- [x] 5.2 Gate readiness on exact function and trigger ownership
  - Wait under one startup deadline and verify both function registrations plus exactly one active matching queue subscriber and cron trigger with the current nonce, owner, status, configuration, and namespace.
  - Reject pending, stale, foreign, duplicate, mismatched, disconnected, and timed-out catalog results.
  - Completion is observable when deterministic readiness tests cover every accepted and rejected catalog shape without a live engine.
  - _Requirements: 1.3, 1.5, 9.6_
  - _Boundary: Worker Runtime_

- [x] 5.3 Compose the production dependency graph and handlers
  - Compose one managed iii client, repository, router, memory-store writer, coordinator, two typed handlers, shared local-wait permits, admission gate, and task tracker.
  - Reject invalid configuration before registration, map handler failures to content-safe iii errors, and keep all process diagnostics on stderr.
  - Completion is observable when in-process tests prove dependency construction, both handler dispatch paths, permit sharing, fixed diagnostics, and no leaked memory or router content.
  - _Requirements: 1.1, 1.2, 1.4, 7.4, 8.2, 8.3, 9.6_
  - _Boundary: Worker Runtime Integration_
  - _Depends: 4.3, 4.4, 5.2_

- [x] 5.4 Implement bounded process lifecycle
  - Stop admissions on EOF or signals, release registrations, drain admitted work within the shutdown bound, and shut down the iii client on every exit path.
  - Ensure timed-out upstream responses cannot re-enter the writer after local call completion.
  - Completion is observable when deterministic lifecycle tests prove startup order, admission closure, deadline drain, repeated shutdown safety, and complete client cleanup.
  - _Requirements: 1.1, 1.3, 1.5, 7.5, 8.2, 9.6_
  - _Boundary: Worker Runtime Integration_
  - _Accepted limitation: iii-sdk 0.24.0 does not guarantee queued unregister-frame delivery before client shutdown._

- [ ] 6. Verify process and database boundaries
- [x] 6.1 Establish the release-worker protocol fake
  - Register a private workspace package with a bounded loopback iii peer and child-process harness that launches the release manifest and owns deadlines, output capture, and cleanup.
  - Support direct function invocation and deterministic scripted success, error, malformed, and delayed responses for database and router calls.
  - Emulate worker/function registration, both catalogs, durable subscriber registration, and cron registration.
  - Completion is observable when the release worker reaches verified readiness, handles one scripted invocation, and exits cleanly without live iii or upstream workers.
  - _Requirements: 1.2, 1.3, 1.5, 9.6, 9.7_
  - _Boundary: Protocol Fake_
  - _Accepted limitation: the fake validates any received release frames but accepts a clean socket close before all queued unregister frames are delivered._

- [x] 6.2 Verify normal queue-driven process flows
  - Drive valid single- and multi-key plus already-present deliveries through the release worker.
  - Assert exact read, `router::embed`, and insertion requests, positional vectors, response timing, and content-free success.
  - Completion is observable when every normal queue scenario completes against deterministic fake services with no unexpected call.
  - _Requirements: 2.1, 2.2, 2.4, 4.1, 4.2, 4.3, 4.4, 5.1, 5.2, 5.3, 6.1, 6.2, 6.3, 7.1, 9.1, 9.3, 9.4, 9.5, 9.7_
  - _Boundary: Protocol Fake_

- [x] 6.3 Verify queue rejection, failure, retry, and privacy flows
  - Drive malformed, unrelated, truncated, oversized, missing, partial-write, retry, router-mismatch, invalid-vector, and timeout deliveries.
  - Assert no-call rejection, queue-visible errors, no worker retry, duplicate completion after partial commit, and no write after discarded responses.
  - Completion is observable when captured invocation counts prove the worker initiates no retry and protected sentinels appear in no result, stderr, or request diagnostic.
  - _Requirements: 2.3, 2.5, 2.6, 4.5, 5.4, 5.5, 5.6, 6.4, 6.5, 7.2, 7.4, 7.6, 8.1, 8.2, 9.1, 9.3, 9.4, 9.5, 9.7_
  - _Boundary: Protocol Fake_

- [x] 6.4 Verify reconciliation process flows
  - Drive empty, historical, bounded, deterministic, partial-failure, repeated-poison, later-repair, provider/model mismatch, malformed-vector, and invocation-deadline scenarios.
  - Assert exact missing-work and router requests, immutable insert outcomes, content-free counts, stateless reselection, and no internal retry.
  - Completion is observable when every result or error returns before the cron caller bound and captured invocation counts match one worker attempt.
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 5.2, 5.3, 5.4, 5.6, 7.3, 7.6, 8.1, 8.4, 9.2, 9.4, 9.5, 9.7_
  - _Boundary: Protocol Fake_

- [x] 6.5 Verify readiness failures and process lifecycle
  - Exercise pending, stale, foreign, duplicate, and mismatched catalogs plus invalid namespace/config startup.
  - Exercise local concurrency, signals, shutdown during admitted work, deadline expiry, stderr-only diagnostics, and child cleanup.
  - Completion is observable when each process scenario has the exact exit/readiness outcome and every owned child is reaped without protected output.
  - _Requirements: 1.3, 1.4, 1.5, 7.3, 7.4, 7.5, 8.2, 8.3, 9.6, 9.7_
  - _Boundary: Protocol Fake_

- [x] 6.6 (P) Add PostgreSQL 17/18 work-query verification
  - Verify exact-key pending/already-present/missing classification and all-version anti-join ordering, bounds, historical coverage, empty state, and stateless reselection.
  - Race queue-shaped and reconciliation-shaped embedding inserts and require one immutable association with duplicate completion behavior and unchanged canonical memory.
  - Completion is observable when the independent verifier passes unchanged on PostgreSQL 17 and 18 without adding a migration or index.
  - _Requirements: 2.4, 2.5, 3.1, 3.2, 3.3, 3.4, 3.5, 6.1, 6.2, 6.3, 6.4, 6.5, 9.2, 9.5, 9.7_
  - _Boundary: PostgreSQL Smoke_
  - _Depends: 2.1, 2.2, 3.2_

- [x] 6.7 Integrate release, Nix, and CI gates
  - Expose the worker and PostgreSQL verifier outputs through filtered Git-backed flake sources and preserve existing package outputs.
  - Build the release worker, run the protocol fake, both PostgreSQL majors, locked workspace tests, formatting, warning-free Clippy, dependency policy, and flake checks in CI.
  - Completion is observable when every new artifact and gate is explicit in the workflow and no existing gate is removed or weakened.
  - _Requirements: 9.6, 9.7_
  - _Boundary: Nix and CI Integration_
  - _Depends: 6.2, 6.3, 6.4, 6.5, 6.6_

- [x] 7. Run the complete acceptance gate
  - Run every approved package, workspace, protocol, PostgreSQL 17/18, formatting, lint, dependency, release, and flake command from the reproducible environment.
  - Route failures back to their owning task boundary; do not broaden this task into feature repair.
  - Completion is observable only when all commands exit successfully and no protected value appears in process diagnostics.
  - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7_
  - _Boundary: Final Validation_
  - _Depends: 6.7_

## Implementation Notes

- Task 1.1: Git-backed Nix validation initially could not access linked-worktree metadata, then succeeded without changing the flake. Continue using Git-backed flake commands, not `path:.`.
- Task 1.2: Cron validation mirrors cron 0.12 longhand behavior: arbitrary positive steps and delimiter whitespace are valid without a runtime parser dependency.
- Task 1.4: Normalize both raw-Serde and conversion-stage event failures to fixed opaque errors; use stage-specific constructors to preserve the closed Design §11 error matrix.
- Task 2.1: `IiiEmbeddingWorkRepository` deferred `EmbeddingWorkRepository` until task 2.2 supplied the real `list_missing` method; keep both trait methods backed by the static repository queries.
- Task 2.3: Apply renderer byte bounds after compact JSON serialization and retain whitespace sentinels in snapshots to guard against trimming regressions.
- Task 3.1: Router timeout tests use an outer guard to prove the earlier local or absolute deadline wins; preserve f32-origin vectors through the adapter.
- Task 3.2: Keep immutable embedding writes behind `MemoryStore::insert_embedding`; the writer owns deadline and error translation, not table SQL.
- Task 4.1: Preserve omitted versus explicit-null event fields during deserialization; valid row-change timestamps are bounded to 1970 through 9999.
- Task 4.2: Enforce the coordinator batch limit before any rendering or external work; drain every admitted write before reporting a partial failure.
- Task 4.3: Queue deliveries validate before ports, process pending keys despite missing peers, and rely on retry-time exact loads to suppress committed embeddings.
- Task 4.4: Validate the SDK cron request at the service boundary with the exact `cron` trigger and RFC3339 schedule timestamps before any repository, router, or writer call; the job ID remains opaque.
- Task 5.1: Keep actual function and trigger registration plus owned SDK handles in the runtime plan; task 5.3 injects composed handlers through the plan rather than duplicating registration.
- Task 5.2: Preserve managed identity by leaving `InitOptions.namespace` unset; readiness must require exact unavailable-row shapes and distinct queue/cron trigger IDs under one deadline.
- Task 5.3: Register cron through a schema-equivalent content-safe wrapper so SDK decode errors cannot leak payloads; clean the client on failed registration and expose readiness through `WorkerRuntime`.
- Task 5.4: Keep stdin EOF detection on a detached thread so an open stdin cannot block signal shutdown; shutdown order is admission closure, registration release, bounded drain, then client cleanup.
- Task 5.4/6.1: User accepted the iii-sdk 0.24.0 unregister-flush limitation; protocol verification accepts clean closure after validating any delivered release frames.
- Task 6.1/6.5: Across terminal modes, the shutdown fake accepts only `ConnectionClosed`, `ResetWithoutClosingHandshake`, or `Io(ConnectionReset)` as terminal close errors; all other transport errors remain failures. It validates any received release frames but does not guarantee queued unregister-frame flush.
- Task 6.2: Model sequential reads/router calls separately from an exact unordered immutable-write set so concurrent write completion cannot hide duplicate or missing associations.
- Task 6.3: After a local router timeout, send a late reply before an invalid direct-delivery barrier; reject every upstream call until that barrier result proves no late writer re-entry.
- Task 6.4: Reconciliation scripts derive later repair pages from exact successful insert replies and use a typed post-late cron barrier before shutdown.
- Task 6.5: Keep one release-history state across admitted-work shutdown phases; pending readiness must reach the real timeout and exit with the fixed failure status.
- Task 6.7: CI invokes Nix's `cargo-clippy` driver directly and contract-tests the enabled, failure-propagating PostgreSQL 17/18 matrix and verifier build step.
- Task 6.6: The disposable fixture defaults to C collation, so verify explicit `COLLATE "C"` through `pg_prepared_statements` as well as semantic ordering.
- Task 7: The owner package contract now permits valid npm Dependabot entries; Biome `info` diagnostics do not fail the package check's exit-status gate. Pi ProcessGuard cleanup was repaired in `d413aa0` and `63788a0`, and OpenCode readiness in `709b22b`; complete acceptance passed afterward.
- Remediation R1: Reject Tokio semaphore overflow before startup and bound repository and writer waits by the earlier database and invocation deadline.
- Remediation R2: `durable:subscriber` uses canonical `queue`; `topic` remains a compatible alias.
