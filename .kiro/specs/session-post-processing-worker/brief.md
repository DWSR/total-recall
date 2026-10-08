# Brief: session-post-processing-worker

## Problem
"Create a spec for a cron agent that takes raw observations and session data and does post-processing. The post processing should include creating a single comprehensive session record (similar to the session transcriptions that are available in the OpenCode database) as well as automatically creating memories for relevant details or concepts. The session record should include a brief summary of the session as well as a list of 10 concepts that the session was about for later recall. The post processing should happen on a cron using the cron worker (once a minute configurable). Session records should not be created until a session end event has been transmitted or it has been 24 hours since the last recorded observation event."

The system currently preserves raw harness events but does not convert a completed or inactive session into a recallable session record or durable memories.

## Current State
`harness-event-persistence-worker` stores append-only lifecycle events and opaque observations. `memory-schema-search` stores canonical versioned memories and supports recall. No component schedules session finalization, assembles a session transcript, summarizes it, extracts concepts, or creates memories.

## Desired Outcome
A cron-triggered worker discovers eligible sessions, assembles all recorded source events into one comprehensive session record, adds a brief generated summary and exactly 10 recall concepts, and persists relevant generated memories. Processing is resumable and idempotent. A session becomes eligible only after a recorded session-end event or 24 hours after its latest recorded observation. Late observations refresh the existing session record after the same end-or-inactivity gate and may add new memories without changing earlier memories.

## Approach
Build a Rust iii worker with a configurable UTC cron schedule, defaulting to once per minute. Use a PostgreSQL-backed durable state machine to claim eligible sessions, prevent overlapping processing, track source cutoffs, and resume partial failures. Assemble the comprehensive transcript deterministically from persisted events; use one explicitly selected OpenAI-compatible or Anthropic model configuration to generate the summary, exactly 10 concepts, and memory candidates. Validate provider output locally before updating the session projection and canonical memory store.

## Scope
- **In**: Configurable cron registration, eligibility by persisted session end or observation inactivity, concurrent-safe claiming, deterministic source assembly, one updatable session record per session ID, provider-isolated OpenAI-compatible and Anthropic generation, summary and 10 concepts, relevant memory creation, late-arrival regeneration, retries, resumability, and content-safe diagnostics.
- **Out**: Harness event ingestion, mutation or deletion of source events, public recall APIs, memory search ranking, embedding generation, model routing between providers, per-session provider selection, and general-purpose model configuration shared with future features.

## Boundary Candidates
- Cron scheduling, bounded candidate discovery, and overlap prevention.
- Eligibility and regeneration policy over append-only source events.
- Deterministic comprehensive session assembly and session projection persistence.
- Session-post-processing-specific model configuration and provider adapters.
- Generated-output validation and canonical memory reconciliation.
- Durable progress, retries, and partial-failure recovery.

## Out of Boundary
- Changing harness protobufs, queue delivery, or event persistence contracts.
- Treating model output as the authoritative source transcript.
- Embedding session records or memories.
- Exposing session records through a new API.
- Retention, deletion, or compaction of raw observations.
- Load balancing or dynamic fallback across model providers.

## Upstream / Downstream
- **Upstream**: `harness-event-persistence-worker`, PostgreSQL 17/18, iii engine and cron worker, and one selected OpenAI-compatible or Anthropic model endpoint.
- **Downstream**: `memory-schema-search`, future session recall surfaces, and future embedding processors.

## Existing Spec Touchpoints
- **Extends**: None. The worker consumes existing persistence and memory contracts without changing their ownership.
- **Adjacent**: `harness-ingestion-worker`, `harness-event-persistence-worker`, `memory-schema-search`, and `mcp-worker`.

## Constraints
- Use the repository-pinned Rust 1.98.1 and `iii-sdk` 0.24.0.
- Register a iii cron trigger and require the separately deployed cron provider; cron expressions use UTC.
- Default to one run per minute and allow a positive deployment-specific schedule override.
- Support one explicitly active provider per deployment. Keep OpenAI-compatible and Anthropic base URLs, API tokens, and models specific to this worker.
- Use provider-specific wire contracts and locally validate the common output shape, including exactly 10 concepts.
- Do not expose raw observations, transcript content, summaries, concepts, memory content, credentials, or provider response bodies in diagnostics.
- Bound candidate batches, model requests, retries, and claim leases so one session cannot block future cron runs indefinitely.
