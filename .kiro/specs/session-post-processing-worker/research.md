# Research and Design Decisions

## Summary
- **Feature**: `session-post-processing-worker`
- **Discovery scope**: Complex brownfield integration across scheduling, PostgreSQL projection state, two model APIs, and canonical memory publication.
- The append-only ledger and canonical memory contracts are reusable; scheduling, source revisions, provider adapters, projection state, and retry-safe publication are new.
- A standalone downstream worker best preserves the product boundary between evidence capture and interpretation.
- The principal risks are late-commit detection, unknown memory commit outcomes, external provider variability, and model context limits.

## Current Assets and Gaps

| Area | Reusable asset | Gap |
|---|---|---|
| Source evidence | `session_events` and `raw_observations` preserve receipt IDs, supplied and normalized times, `ingested_at`, metadata, and opaque data | Global eligibility query, recorded-time indexes, revision, claim, and projection |
| Memories | `MemoryVersionInput`, immutable `(id, version)` rows, provenance, validation | Stable candidate IDs and exact-equivalence retry handling |
| Worker runtime | Managed iii registration, nonce readiness, pure config, static database calls | Cron binding and model HTTP adapters |
| Verification | Recording ports, loopback iii fakes, PostgreSQL 17/18 fixtures | Provider peers, claim races, saga recovery, and refresh replacement |

The current `(session_id, source_timestamp_utc)` ledger indexes do not support global inactivity discovery or recorded-time refresh scans. Event-ledger migrations remain owned by `harness-event-persistence`; processor tables belong to the new worker.

## Research Log

### Eligibility and source revisions
- The 24-hour rule maps to `raw_observations.ingested_at`, not harness-supplied source time.
- A session is eligible with any persisted end, or without an end when its latest observation has been recorded for 24 hours. End-only sessions remain valid.
- Refresh must detect ledger membership changes because late rows may carry older source timestamps, then reapply the end-or-24-hour-inactivity gate.
- Append-only lifecycle and observation counts provide a monotonic claim key. Loading verifies those counts, then UUIDv5 over sorted event-kind and receipt-ID pairs identifies exact source membership.
- Transcript ordering is independent: source instant, recorded time, event-kind rank, then receipt ID.

### iii cron
- `iii-sdk` 0.24.0 exposes cron trigger configuration and registration: <https://docs.rs/iii-sdk/0.24.0/iii_sdk/builtin_triggers/struct.CronTriggerConfig.html>.
- The separately deployed provider uses UTC six- or seven-field expressions and skips missed fires: <https://workers.iii.dev/workers/cron>.
- iii 0.24.0's compose pins cron 0.21.9: <https://github.com/iii-hq/iii/blob/42f68fead892d4e73a0a0239f5cd4417ef831709/engine/worker-compose.yaml>.
- Registration rejection is asynchronous; readiness must inspect the process-owned active binding. Cron-side locks do not replace database claims.

### Model APIs
- OpenAI documents bearer-authenticated `POST /v1/chat/completions`; strict Structured Outputs can enforce array cardinality: <https://platform.openai.com/docs/api-reference/chat/create> and <https://platform.openai.com/docs/guides/structured-outputs>.
- “OpenAI-compatible” does not standardize structured-output fields, refusals, token limits, or errors. A configurable output mode and local validation are required.
- Anthropic Messages uses `POST /v1/messages`, `x-api-key`, and `anthropic-version: 2023-06-01`: <https://platform.claude.com/docs/en/api/messages>.
- Anthropic structured output uses `output_config.format`, but cannot schema-enforce exactly 10 array items: <https://platform.claude.com/docs/en/build-with-claude/structured-outputs>.
- Neither vendor maintains a Rust SDK. Pinned `reqwest` plus Serde/Schemars gives explicit wire control with project-compatible licenses.
- No provider guarantees semantic completeness. Refusal, truncation, malformed JSON, invalid evidence, or wrong concept count must fail local validation.

### Persistence and recovery
- Source/session state can commit atomically when colocated. Canonical memory publication remains a cross-store saga.
- Candidate rows must persist deterministic memory IDs and complete payloads before publication.
- A timeout after insert requires exact lookup and equality comparison. Treating every duplicate as success could hide conflicting content.
- The prior session record remains current until all memory candidates for the replacement revision are confirmed.

## Architecture Pattern Evaluation

| Option | Strengths | Risks |
|---|---|---|
| Standalone worker | Preserves downstream boundary; independent readiness and scaling | New runtime, provider clients, projection schema, and saga |
| Extend persistence worker | Reuses source adapter and runtime | Couples raw capture to model and memory availability; violates steering boundary |
| Split scheduler and processor | Isolates short cron discovery from long model work | Adds durable handoff, two deployments, and ambiguous state ownership |

## Design Decisions and Synthesis

### Standalone ports-and-adapters worker
- **Selected**: One worker with a database-backed saga and small external ports.
- **Why**: It is the smallest design that preserves capture/interpretation separation and supports isolated tests.
- **Rejected**: Extending persistence broadens its failure domain; a scheduler queue adds an unnecessary component.

### Colocated source and session state
- **Selected**: One configured PostgreSQL target contains raw ledger reads and processor-owned tables; memory may use another target.
- **Why**: Claims, source counts, staging, and promotion need local transactional state. Table ownership remains separate by migration.

### Hierarchical generation
- **Selected**: Bounded map calls over transcript chunks plus recursive reduction for the final summary and 10 concepts.
- **Why**: Provider contexts are finite while session size is not bounded by ingestion. The deterministic local transcript remains comprehensive.

### Staged memory publication
- **Selected**: Persist candidates before publication; use a session-level normalized content fingerprint, UUIDv5 identities, cross-revision exact duplicate suppression, and `ensure_memory` exact comparison.
- **Why**: This resolves crashes, retries, and timeout-after-commit without weakening immutable memory conflict semantics.

### Provider-specific adapters
- **Selected**: Raw HTTP adapters over one internal typed contract.
- **Why**: Vendor wire formats differ and no official Rust SDK covers both. The interface generalizes provider calls, not provider routing.

## Risks and Validation
- Candidate aggregation over the full append-only ledger may become expensive; validate PostgreSQL 17/18 query plans before raising schedule frequency or batch size.
- OpenAI-compatible strict schema support varies; contract tests cover strict, JSON-object, and prompt-only modes.
- A single observation can exceed one model request; fragment serialized entries and preserve receipt provenance.
- Prompt content is untrusted; delimit it as data, validate all output, and never execute generated text.
- Confirm the deployed cron catalog shape, provider version, least-privilege grants, and license/advisory policy in integration and CI checks.
