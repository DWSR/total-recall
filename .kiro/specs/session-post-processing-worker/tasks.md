# Implementation Plan

- [ ] 1. Establish worker foundations
- [x] 1.1 Scaffold the worker and integration fixture packages
  - Add private workspace packages for the worker, iii engine fake, and PostgreSQL smoke verifier with the pinned Rust, iii, HTTP, serialization, time, and UUIDv5 dependencies from the design.
  - Preserve thin executable and importable library boundaries and reserve the worker manifest and fixture entry points.
  - Completion is observable when workspace metadata resolves and all package skeletons compile with the locked dependency graph.
  - _Requirements: 1.1, 5.1, 10.7_
  - _Boundary: Worker Runtime, Integration Fixtures_

- [x] 1.2 Define processing contracts and content-safe failures
  - Define validated claims, source snapshots, transcript entries, generated revisions, memory candidates, provider exchanges, sweep outcomes, and attempt states.
  - Classify startup, source, lease, provider, memory, and repository failures by stage and retryability without carrying protected payloads.
  - Completion is observable when closed-schema and sentinel tests prove invalid fields are rejected and all display, debug, and serialized errors omit protected content.
  - _Requirements: 4.3, 4.6, 4.7, 4.8, 5.6, 5.7, 6.5, 8.3, 9.1, 9.2, 9.4_
  - _Boundary: Processing Contracts_

- [x] 1.3 Validate schedule, storage, bounds, and active provider configuration
  - Validate the two database targets, UTC cron expression, batch and concurrency limits, lease duration, model request bounds, and shutdown settings before registration.
  - Require exactly one active OpenAI-compatible or Anthropic provider while tolerating complete inactive-provider settings and redacting all tokens.
  - Completion is observable when tests confirm the one-minute default, valid overrides, provider-specific defaults, loopback-only HTTP, and startup rejection of every missing or inconsistent value.
  - _Depends: 1.2_
  - _Requirements: 1.2, 1.3, 1.4, 3.1, 5.2, 5.3, 5.4, 5.5, 8.5, 9.3_
  - _Boundary: Config_

- [x] 1.4 Create durable session, attempt, and candidate state
  - Add processor-owned current-session, attempt, and memory-candidate storage with controlled states, revision uniqueness, one nonterminal attempt per session, fenced leases, retry timing, and terminal completion or supersession.
  - Enforce summary and concept cardinality, session-level content-fingerprint uniqueness, publication progress, and least-privilege migration and runtime access.
  - Completion is observable when exact migration tests verify all tables, constraints, indexes, foreign keys, and grants without changing ledger-owned or canonical-memory schemas.
  - _Requirements: 3.2, 3.3, 3.4, 4.1, 4.8, 6.3, 6.4, 6.6, 6.7, 7.3, 7.5, 7.6, 7.7, 8.1, 8.2, 8.3, 8.4_
  - _Boundary: Source and Session Repository_

- [x] 1.5 Provide deterministic local test ports
  - Supply injected clocks, recording and failing repositories, scripted model responses, lease-aware doubles, and canonical-memory outcomes.
  - Keep every external effect replaceable without process-global environment mutation or live infrastructure.
  - Completion is observable when focused tests can drive time, retries, failures, and concurrency deterministically using only local fixtures.
  - _Depends: 1.2_
  - _Requirements: 9.1, 10.7_
  - _Boundary: Test Infrastructure_

- [ ] 2. Build the source and projection repository
- [x] 2.1 Discover initially eligible and refresh-eligible sessions
  - Select sessions with a persisted end or at least 24 hours since the latest recorded observation, including end-only and incomplete lifecycle histories.
  - Reapply the same gate after late events so resumed unended sessions wait for another quiet period, while repeated and out-of-order lifecycle events remain valid.
  - Completion is observable when boundary-time tests distinguish eligible, active, empty, ended, resumed, and unchanged sessions exactly.
  - _Depends: 1.4_
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 7.1, 7.2, 10.1, 10.5_
  - _Boundary: Source and Session Repository_

- [x] 2.2 Claim bounded work and fence ownership
  - Claim no more than the configured limit while allowing at most one nonterminal attempt and one current lease owner per session.
  - Renew and reclaim leases with new fencing tokens, and require the active token for every attempt-state mutation.
  - Completion is observable when concurrent tests produce one owner, expired work becomes claimable, stale owners cannot mutate state, and an empty sweep writes nothing.
  - _Depends: 1.4_
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 8.1, 10.5_
  - _Boundary: Source and Session Repository_

- [x] 2.3 Load exact source revisions and supersede stale claims
  - Load all lifecycle and observation rows selected by a claim, preserving source fields and calculating revision identity from sorted event-kind and receipt pairs.
  - Compare loaded and claimed counts before external calls and atomically supersede stale attempts so the latest revision can be claimed.
  - Completion is observable when a source row inserted between claim and load produces terminal supersession without a provider or memory call.
  - _Depends: 2.2_
  - _Requirements: 2.6, 4.2, 4.3, 4.8, 7.1, 8.1, 8.3, 10.1_
  - _Boundary: Source and Session Repository_

- [x] 2.4 Stage generated projections and memory candidates idempotently
  - Persist a complete transcript projection, generated fields, canonical candidate payloads, supporting receipts, and publication markers under the fenced attempt.
  - Suppress normalized fingerprints already staged or published for the same session while preserving every earlier canonical memory.
  - Completion is observable when retries and later revisions expose one candidate per session fingerprint and stale fencing tokens cannot stage or mark publication.
  - _Depends: 2.3_
  - _Requirements: 5.7, 6.3, 6.4, 6.6, 6.7, 7.5, 7.6, 8.1, 8.3, 10.4, 10.5_
  - _Boundary: Source and Session Repository_

- [x] 2.5 Persist retry state and promote one current record
  - Record content-safe failure categories and next-attempt times without representing incomplete revisions as complete.
  - Promote in place only after all candidates are confirmed and the fenced attempt remains the sole nonterminal revision; otherwise preserve the prior current record.
  - Completion is observable when successful, zero-memory, partial-failure, and failed-refresh scenarios leave one correct current record and terminal attempt state.
  - _Depends: 2.4_
  - _Requirements: 4.1, 4.8, 7.3, 7.7, 8.1, 8.2, 8.3, 8.4, 10.5_
  - _Boundary: Source and Session Repository_

- [x] 2.6 Provide fenced refresh memory scope
  - Return a loaded snapshot with every observation receipt for initial processing or the exact receipt-set difference from the prior current projection for refresh processing.
  - Keep scope derivation inside the fenced repository load and preserve tied recorded-time correctness without coordinator-side projection queries.
  - Completion is observable when initial, refresh, absent-prior-record, and tied-recorded-time cases return only the allowed receipt IDs.
  - _Depends: 2.5_
  - _Requirements: 6.4, 7.5, 8.1, 8.3, 10.4, 10.5_
  - _Boundary: Source and Session Repository_

- [ ] 3. Build transcript and generation behavior
- [x] 3.1 Assemble a comprehensive deterministic transcript
  - Preserve every lifecycle and observation field through the claimed revision without interpreting observation data or deduplicating repeated deliveries.
  - Order entries by source instant, recorded time, event-kind rank, and receipt identifier.
  - Completion is observable when tied, repeated, missing, and out-of-order source rows produce the same complete source-derived transcript on every run.
  - _Depends: 1.2, 2.3_
  - _Requirements: 4.2, 4.3, 4.4, 4.5, 7.4, 10.2_
  - _Boundary: Transcript Assembler_

- [x] 3.2 Bound generation input and validate structured output
  - Chunk ordered transcript entries on UTF-8 boundaries and fragment oversized entries while preserving source receipt and fragment order.
  - Locally reject unknown fields, malformed output, invalid evidence, empty sentence elements, overlong summaries, and non-distinct or incorrectly sized concept lists.
  - Completion is observable when every request respects the byte bound and accepted output contains one to five sentence elements and exactly 10 distinct concepts.
  - _Depends: 3.1_
  - _Requirements: 4.2, 4.5, 4.6, 4.7, 5.6, 6.4, 6.5, 10.2, 10.3_
  - _Boundary: Generation Pipeline_

- [x] 3.3 Produce bounded map results with revision-specific memory scope
  - Obtain a fresh lease permit for each map request and clip its deadline below lease expiry.
  - Permit all observation receipts on initial processing and only newly recorded observation receipts on refresh; allow a valid zero-memory result.
  - Completion is observable when map tests reject unsupported evidence, stop on lease loss, and return bounded summaries, concepts, and candidates for every chunk.
  - _Depends: 3.2_
  - _Requirements: 4.6, 5.6, 5.7, 6.1, 6.2, 6.4, 6.5, 7.5, 8.1, 8.3, 10.2, 10.4_
  - _Boundary: Generation Pipeline_

- [x] 3.4 Reduce summaries and concepts hierarchically
  - Recursively reduce bounded map summaries and concepts, renewing the lease before every provider request.
  - Validate the final sentence array and concept list before rendering the stored summary.
  - Completion is observable when large multi-level sessions yield one non-empty summary of at most five sentences and exactly 10 distinct concepts without an unbounded request.
  - _Depends: 3.3_
  - _Requirements: 4.6, 4.7, 5.6, 5.7, 7.4, 8.1, 8.3, 10.2_
  - _Boundary: Generation Pipeline_

- [x] 3.5 Normalize canonical session-derived memory candidates
  - Set the canonical type, empty files, session and observation provenance, whole-second timestamps, version, normalized content fingerprint, and UUIDv5 identity.
  - Collapse equivalent candidates and reject invalid canonical fields or supporting receipts outside the current memory scope.
  - Completion is observable when normalization is deterministic and every accepted candidate passes the existing canonical memory contract.
  - _Depends: 3.3_
  - _Requirements: 6.1, 6.3, 6.4, 6.5, 6.6, 7.5, 7.6, 10.4_
  - _Boundary: Generation Pipeline_

- [x] 3.6 (P) Implement the OpenAI-compatible provider boundary
  - Send bearer-authenticated Chat Completions requests using configured JSON Schema, JSON object, or prompt-only output hints.
  - Classify refusal, filtering, truncation, malformed bodies, authentication, request-size, timeout, `429`, and `5xx` outcomes with bounded jittered retry and `Retry-After` handling.
  - Completion is observable when an independent loopback HTTP peer verifies every request mode and no response body or token enters diagnostics.
  - _Depends: 1.2, 1.3, 3.2_
  - _Requirements: 5.1, 5.3, 5.5, 5.6, 5.7, 9.1, 9.4, 10.3_
  - _Boundary: OpenAI-Compatible Model Provider_

- [x] 3.7 (P) Implement the Anthropic provider boundary
  - Send Messages requests with API-key and version headers plus the structured output configuration.
  - Classify refusal, filtering, truncation, malformed bodies, authentication, request-size, timeout, `429`, and `5xx` outcomes with bounded jittered retry and `Retry-After` handling.
  - Completion is observable when an independent loopback HTTP peer verifies the wire contract and no response body or token enters diagnostics.
  - _Depends: 1.2, 1.3, 3.2_
  - _Requirements: 5.1, 5.4, 5.5, 5.6, 5.7, 9.1, 9.4, 10.3_
  - _Boundary: Anthropic Model Provider_

- [ ] 4. Integrate memory publication, coordination, and runtime
- [x] 4.1 (P) Recover exact canonical memory outcomes
  - Validate and insert through the existing canonical memory store, then perform worker-local read-only exact lookup after conflicts or unknown commit outcomes.
  - Report equivalent success only when every stored canonical field matches the staged payload; preserve missing and mismatched outcomes as failures.
  - Completion is observable when insert, conflict, timeout-after-commit, missing, mismatch, and lookup-failure tests produce one safe deterministic outcome.
  - _Depends: 1.2, 1.4, 3.5_
  - _Requirements: 6.6, 6.7, 8.1, 8.2, 9.1, 9.2, 10.4_
  - _Boundary: Memory Publisher_

- [x] 4.2 (P) Register the typed cron runtime and prove readiness
  - Register one sweep function and one UTC cron binding, accepting the full typed cron call payload without using trigger timestamps as session state.
  - Verify the process nonce, function owner, trigger provider, active status, expression, and namespace before readiness.
  - Completion is observable when runtime tests accept captured cron calls, reject catalog mismatch or registration failure, and return only content-safe sweep counts.
  - _Depends: 1.1, 1.2, 1.3_
  - _Requirements: 1.1, 1.2, 1.3, 1.5, 3.5, 9.1, 9.2, 10.7_
  - _Boundary: Worker Runtime_

- [x] 4.3 Expose staged sweep outcomes
  - Add the safe `staged` count and enforce `attempted = staged + completed + retryable + skipped` without representing staged work as complete.
  - Keep typed cron output and content-safe diagnostics compatible with the expanded count contract.
  - Completion is observable when runtime and contract tests distinguish staged, complete, retryable, and skipped outcomes without protected data.
  - _Depends: 1.2, 4.2_
  - _Requirements: 3.5, 8.3, 9.1, 9.2, 10.7_
  - _Boundary: Processing Contracts and Worker Runtime_

- [x] 4.4 Coordinate claims through complete staged generation
  - Drive each bounded claimed revision through repository-supplied snapshot and memory scope, transcript assembly, leased map and reduce calls, candidate normalization, and atomic staging.
  - Use all observation receipts for initial generation and the repository's prior-projection receipt-set difference for refresh generation. Stop new calls on lease loss; persist provider or contract failures at the whole-second ceiling of `now + 60s + 0..=10s` injected jitter and repository failures at lease expiry.
  - Completion is observable when stale, superseded, failed, zero-memory, and successful stages never expose a partial current revision; confirmed staging increments `staged`, not `completed`.
  - _Depends: 2.5, 2.6, 3.4, 3.5, 3.6, 3.7, 4.3_
  - _Requirements: 3.1, 3.5, 4.1, 4.6, 4.7, 5.7, 7.1, 7.2, 7.4, 7.5, 8.1, 8.3, 9.2, 10.2, 10.5_
  - _Boundary: Sweep Coordinator Integration_

- [x] 4.5 Coordinate resumable publication through final promotion
  - Obtain a fresh lease permit before every memory request, ensure each unpublished candidate, and fence its publication marker.
  - Resume partial attempts, classify failures, and promote only after every required memory operation succeeds.
  - Completion is observable when overlapping sweeps and zero-memory, partial-publication, unknown-commit, and failed-refresh flows converge to one current record without duplicate memories.
  - _Depends: 2.5, 4.1, 4.4_
  - _Requirements: 3.2, 3.3, 3.4, 3.5, 6.1, 6.2, 6.6, 6.7, 7.3, 7.7, 8.1, 8.2, 8.3, 8.4, 8.5, 9.2, 10.4, 10.5_
  - _Boundary: Sweep Coordinator Integration_

- [x] 4.6 Compose the bounded worker process
  - Instantiate only the selected provider and connect source/session, canonical-memory, coordinator, and runtime adapters.
  - Retain registrations, bound admitted sweep concurrency, stop admissions on signals, and drain only within the configured shutdown deadline.
  - Completion is observable when subprocess tests fail before readiness on invalid composition and shut down cleanly without leaking secrets or protected content.
  - _Depends: 3.6, 3.7, 4.2, 4.5_
  - _Requirements: 1.1, 1.4, 1.5, 3.1, 5.2, 5.5, 8.5, 9.3, 10.6, 10.7_
  - _Boundary: Worker Runtime Integration_

- [ ] 5. Verify external boundaries and quality gates
- [x] 5.1 Build the iii protocol and process fake
  - Independently exercise registration, catalog inspection, typed cron invocation, database envelopes, default and overridden schedules, readiness ownership, and shutdown.
  - Script source/session and memory outcomes plus a loopback model peer without live iii, cron, or database services.
  - Completion is observable when the release worker passes all process scenarios with content-safe counts, errors, and cleanup.
  - _Depends: 4.6_
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 9.1, 9.2, 10.3, 10.7_
  - _Boundary: Protocol Fake_

- [x] 5.2 Establish synthetic PostgreSQL 17 and 18 environments
  - Create production-shaped source tables with explicit receipt IDs, apply processor migrations, and separate migration and runtime roles without requiring the ledger's UUID extension.
  - Keep source-table setup independent from processor-owned schema and verify reversible fixture setup.
  - Completion is observable when both supported PostgreSQL majors accept the schema and enforce least-privilege access.
  - _Depends: 1.4_
  - _Requirements: 2.1, 2.2, 4.2, 6.4, 10.7_
  - _Boundary: PostgreSQL Smoke Fixture_

- [x] 5.3 Verify eligibility, claims, leases, and supersession in PostgreSQL
  - Exercise ended and inactive discovery, resumed-session quiet periods, concurrent claims, lease expiry, token fencing, and claim-to-snapshot races.
  - Confirm stale count mismatches terminate as superseded and unblock the latest revision.
  - Completion is observable when both PostgreSQL majors produce one owner, no premature refresh, and the same deterministic state transitions.
  - _Depends: 2.3, 5.2_
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 3.2, 3.3, 3.4, 7.1, 7.2, 8.1, 8.3, 10.1, 10.5, 10.7_
  - _Boundary: PostgreSQL Smoke Verifier_

- [x] 5.4 Verify staging, retry, promotion, and query plans in PostgreSQL
  - Exercise candidate fingerprint uniqueness, publication markers, retry timing, fenced promotion, rollback, failed refresh preservation, and quiet-period replacement.
  - Capture representative eligibility query plans without adding ledger-owned indexes or broadening schema ownership.
  - Completion is observable when both PostgreSQL majors retain one current record, suppress exact prior memories, and preserve every transactional rollback invariant.
  - _Depends: 2.5, 5.3_
  - _Requirements: 4.1, 4.8, 6.6, 6.7, 7.3, 7.5, 7.6, 7.7, 8.1, 8.2, 8.3, 8.4, 10.4, 10.5, 10.7_
  - _Boundary: PostgreSQL Smoke Verifier_

- [x] 5.5 Enforce release, Nix, dependency, and CI checks
  - Include the worker and fixtures in filtered Nix sources, release builds, and the authoritative CI matrix without changing the pinned Rust toolchain.
  - Run locked workspace tests, formatting, Clippy with warnings denied, license and advisory checks, the iii process fake, and PostgreSQL 17/18 verification.
  - Completion is observable when the full matrix passes on every declared platform and covers every focused verification requirement without live production infrastructure.
  - _Depends: 5.1, 5.4_
  - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7_
  - _Boundary: Nix and CI Quality Gates_

## Implementation Notes

- Use `nix develop --command cargo-clippy` for focused linting: `cargo clippy` dispatches an incompatible Clippy 0.1.96 before linting, while `cargo-clippy` uses the Nix Rust 1.98.1 toolchain.
- Protocol fakes use bounded first-ready signaling and reject every post-signal data or control frame before accepting terminal close or EOF.
- PostgreSQL fixtures arm teardown before `pg_ctl start` and verify forced-start cleanup separately from pre-start failures.
- Direct PostgreSQL verifiers support macOS Bash 3.2 and both GNU `sha1sum` and BSD `shasum` for source-revision vectors.
- PostgreSQL plan checks assert relevant relations and unchanged source schema/indexes, not an exact plan shape across supported majors.
