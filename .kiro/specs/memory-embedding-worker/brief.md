# Brief: memory-embedding-worker

## Problem

"Create a spec for a worker to do embedding generation for memories. Make the embedding generator worker use the llm-worker. Assume that the design is that the embedding worker is listening to a queue worker that is receiving triggers from the database worker's row-changed and model the embedding worker's exposed function accordingly. The embedding worker should use the llm-router worker's router::embed function."

Canonical memory versions can be saved and searched, but no component generates
their optional embeddings. Row-change delivery can also be lost before durable
queue publication, so a queue consumer alone cannot ensure that every committed
memory version becomes searchable by vector similarity.

## Current State

`memory-schema-search` owns immutable memories, one optional embedding per
`(id, version)`, insertion through `MemoryStore`, and exact vector search. Memory
inserts return their ID and version. The deployed database worker can emit an
insert row-change event, a deployment-provided bridge can publish that event to a
durable queue, and the queue worker can invoke a typed subscriber function.

The repository has queue-subscriber and readiness patterns but no embedding
worker, missing-embedding query, canonical embedding input, `router::embed`
adapter, or reconciliation process.

## Desired Outcome

Every immutable memory version can gain one embedding generated through
`router::embed`. Normal operation consumes key-only memory-insert events from a
durable queue. A separately exposed reconciliation function, intended for a cron
trigger, repairs committed memory versions that still lack embeddings. Both
paths are bounded, idempotent, content-safe, and safe to run concurrently.

## Approach

Create a Rust iii worker with a queue-consumer function and a reconciliation
function. The queue consumer validates the database row-change payload and
reloads each referenced canonical memory. Reconciliation scans bounded pages of
all immutable memory versions without embeddings. Both paths render the same
versioned title-content-concepts input, call `router::embed` with one configured
provider and model, validate the returned model and vectors, and insert through
the existing `MemoryStore` embedding operation.

Treat an already-present embedding as idempotent completion. Do not replace or
compare it, and do not re-embed existing versions after provider or model
configuration changes. Queue retries handle processing failures; scheduled
reconciliation repairs row-change or publication loss. Deployments use a queue
backend that provides retries and dead-lettering; Redis queue mode is excluded.

## Scope

- **In**: Rust worker bootstrap; strict row-change queue payload adaptation;
  durable subscriber registration; typed reconciliation entry point and cron
  registration; bounded missing-embedding discovery over every memory version;
  canonical title-content-concepts rendering; explicit `router::embed` calls;
  provider/model and resource configuration; vector validation and float4-safe
  conversion; immutable embedding insertion; duplicate-safe concurrency;
  readiness, shutdown, privacy, protocol fakes, PostgreSQL verification, Nix,
  and CI integration.
- **Out**: Database-row-change bridge implementation; queue, database, cron,
  llm-router, provider, PostgreSQL, or extension provisioning; memory creation;
  query embedding generation; memory extraction; embedding replacement;
  provider/model migration; durable reconciliation attempt state; no-starvation
  guarantees; deletion; retention; and vector indexing.

## Boundary Candidates

- Row-change event validation and canonical memory loading.
- Missing-embedding discovery and bounded reconciliation.
- Canonical embedding-input rendering and router response validation.
- `router::embed` and canonical embedding-store adapters.
- Queue subscriber, cron trigger, readiness, concurrency, and shutdown.

## Out of Boundary

- Publishing database row-change events to the durable queue.
- Changing canonical memory or embedding ownership in `memory-schema-search`.
- Storing provider, model, dimensions, or generation history with an embedding.
- Re-embedding an existing `(id, version)` after configuration changes.
- Persisting reconciliation cursors, attempts, retry timing, or terminal skips.
- Generating vectors for MCP search queries.
- Summarizing sessions or extracting memories from raw observations.

## Upstream / Downstream

- **Upstream**: `memory-schema-search`; iii engine and SDK; database, queue,
  cron, llm-router, and configured embedding-provider workers; a deployment-owned
  row-change-to-queue bridge.
- **Downstream**: Exact vector search and combined MCP memory search; operators
  monitoring missing-embedding reconciliation.

## Existing Spec Touchpoints

- **Extends**: None. The worker consumes the existing memory and embedding
  contracts without changing their ownership.
- **Adjacent**: `memory-schema-search` owns schema and insertion invariants;
  `mcp-worker` owns query-time tool behavior; `session-post-processing-worker`
  owns memory extraction; `harness-event-persistence-worker` owns separate
  session embeddings.

## Constraints

- Use Rust 1.98.1, edition 2024, Tokio 1.48.0, and `iii-sdk` 0.24.0.
- Use effective namespace `default` for the worker and both trigger providers
  because cron 0.21.9 fires targets without preserving target namespace.
- Invoke `router::embed`; do not call a provider function directly.
- Require one explicit non-blank provider and model and verify both in the
  router response.
- Render title, content, and ordered concepts through one versioned canonical
  format shared by queue and reconciliation paths.
- Reconcile every immutable memory version, not only current search heads.
- Keep reconciliation stateless and best-effort; deterministic early failures
  may be selected again and can delay later versions.
- Deserialize router vectors as float32 values before lossless conversion to
  the memory store's float64 contract.
- Bound event keys, reconciliation pages, embed batches, locally awaited calls,
  query and invocation timeouts, queue retries, and shutdown drain time.
- Never include memory text, concepts, vectors, source keys, credentials, router
  responses, SQL, or backend messages in diagnostics.
