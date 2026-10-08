---
updated_at: 2026-09-21
---

# Product

## Purpose

Total Recall is a harness-neutral memory substrate for coding agents. It captures
agent-session activity, preserves raw evidence, stores immutable versioned
memories, and exposes retrieval through the Model Context Protocol (MCP).

The system exists to give agents continuity across sessions without coupling
each harness to the iii runtime, PostgreSQL, or internal storage schemas.

## Product Principles

### Preserve Evidence Before Interpretation

Raw lifecycle events and observations are an append-oriented source of truth.
Interpretation, session synthesis, and memory extraction belong in downstream
processing stages so that derived records can be recreated as those policies
evolve.

### Keep Harness Integrations Thin

Harness adapters translate native lifecycle hooks into shared contracts and
delegate transport to common tooling. They should not duplicate queue,
database, retry, or memory-domain behavior.

Harness-specific observation bodies remain opaque JSON. Shared contracts own
only the fields required to identify, order, and attribute activity.

### Make Delivery Semantics Explicit

The current event path validates and submits each request once. It does not
imply retries, source-event deduplication, exactly-once delivery, or global
ordering. New features must state any stronger guarantees and identify the
layer that enforces them.

### Retain Memory History

Memories are immutable versions rather than mutable rows. A memory identity can
gain newer versions while prior versions and their provenance remain available.
Search targets the current version; history remains independently retrievable.

### Prefer Bounded Local Interfaces

Agent-facing memory operations use a local stdio MCP server with bounded input,
concurrency, and queues. Shared network service concerns such as authentication,
tenancy, and public hosting are outside the current product boundary.

### Fail Safely at Harness Boundaries

Memory capture must not make the host harness unusable. Integrations favor
bounded work, payload-free diagnostics, and explicit failure behavior over
blocking indefinitely or leaking observed content.

## Core Product Flows

### Capture and Persistence

Harness clients submit session starts, observations, and session ends through
typed iii functions. The ingestion boundary validates the shared contract and
publishes accepted events to a durable queue. A separate persistence worker
appends each delivery to the raw event ledger.

### Memory Storage and Search

The memory domain stores immutable canonical records and optional embeddings.
It supports current-version lexical search, exact vector search, direct lookup,
and version history while retaining links to source sessions and observations.

### Agent Retrieval

The MCP worker adapts protocol tools to memory operations. Combined search
merges lexical and vector candidates deterministically and returns memory data,
not backend-specific scores, SQL details, or embeddings.

## Users

- Harness integrators use the shared CLI or thin harness adapters.
- Coding agents save and retrieve durable context through MCP tools.
- Operators provide and configure the iii runtime, queues, PostgreSQL, required
  extensions, credentials, and migrations.

## Product Boundaries

- Infrastructure provisioning is external. The repository verifies contracts
  and migrations but does not create production iii or PostgreSQL resources.
- Raw event storage is optimized for faithful capture, not normalized session
  views or direct product queries.
- Automatic session synthesis, memory extraction, and embedding generation must
  be introduced as explicit downstream capabilities; ingestion should not
  absorb them.
- MCP is currently a local stdio interface, not a multi-user network API.
- Memory update, deletion, retention, and lifecycle policy are not implied by
  create-only versioned storage.
- Credentials, system prompts, streaming deltas, and unrelated harness settings
  are not observation payloads.

## Decision Guide

When adding a capability, preserve these separations:

- Capture records what happened; downstream processing decides what it means.
- Shared contracts define stable cross-component fields; harness-native details
  remain opaque.
- Domain libraries own validation and invariants; transports adapt external
  protocols to those libraries.
- Delivery guarantees, privacy behavior, and resource bounds are part of the
  product contract and must be tested explicitly.
