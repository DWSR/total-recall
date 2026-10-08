# Research and Gap Analysis

## Scope

This analysis compares the approved requirements with the current Total Recall
workspace and the released iii database, queue, llm-router, and provider
workers. It records integration facts and options; design decisions remain in
`design.md`.

## Existing Assets

- `memory-store` already validates and inserts one immutable embedding for an
  existing `(id, version)` through `MemoryStore::insert_embedding`.
  `crates/memory-store/src/memory.rs:14-31`
- `EmbeddingInput` contains only ID, version, and vector. Validation requires
  1–16,000 finite, non-zero components that round-trip exactly through float4.
  `crates/memory-store/src/contracts.rs:21-26`
  `crates/memory-store/src/contracts.rs:399-435`
- `memory_embeddings` has one restrictive child row per memory version and no
  provider, model, dimensions, attempt, or generation metadata.
  `crates/memory-store/migrations/0001_memory_schema_search.sql:24-39`
- The production embedding insert uses static parameterized SQL through iii and
  distinguishes success, duplicate conflict, and missing parent.
  `crates/memory-store/src/database.rs:58-79`
  `crates/memory-store/src/database.rs:654-733`
- No existing memory-store operation loads one exact canonical memory with its
  embedding state or lists every version missing an embedding.
- The MCP worker contains exact/latest read patterns, but its backend is owned
  by MCP tool behavior and is not a reusable embedding-worker port.
  `workers/mcp-worker/src/backend.rs:48-95`
- Harness persistence provides the closest worker pattern: two-stage typed
  function and durable-subscriber registration, nonce-based readiness, retained
  handles, queue-visible handler errors, and a loopback engine fake.
  `workers/harness-event-persistence/src/runtime.rs:96-234`
  `workers/harness-event-persistence/src/runtime.rs:410-468`
- The ingestion publisher confirms that `iii::durable::publish` accepts only a
  topic and data payload.
  `workers/harness-ingestion/src/publisher.rs:81-100`
- Existing PostgreSQL 17/18 fixtures already verify memory and embedding
  constraints, concurrency, and vector search.
  `integration/memory-postgres-smoke/README.md:54-119`
- No memory-embedding worker, router adapter, cron-bound reconciliation, or
  row-change contract exists in the workspace.

## Verified Upstream Contracts

### Database row changes

The released database worker exposes trigger type `database::row-changed`.
Registration filters by logical database, table, and operations. Its handler
payload is delivered without another envelope:

```json
{
  "db": "memory",
  "table": "public.memories",
  "op": "insert",
  "affected_rows": 1,
  "returning": [{ "id": "memory-id", "version": "7" }],
  "at": 1780000000000
}
```

`returning` and `truncated` are optional, and PostgreSQL `BIGINT` keys are JSON
strings. Statement capture observes writes made through that database worker;
native capture observes committed external writes but caps returned primary
keys at 100. Delivery after commit is best-effort, at-most-once, and not atomic
with queue publication.

Sources:

- <https://workers.iii.dev/workers/database?tab=api>
- <https://github.com/iii-hq/workers/blob/database/v0.5.20/database/src/triggers/bus.rs>
- <https://github.com/iii-hq/workers/blob/database/v0.5.20/database/src/triggers/handler.rs>
- <https://github.com/iii-hq/workers/blob/database/v0.5.20/database/src/triggers/sql.rs>

### Durable queue

`iii::durable::publish` accepts `{topic, data}` and returns JSON `null`. A
`durable:subscriber` receives `data` directly. Normal return acknowledges.
Handler-error retries and dead-lettering depend on the configured queue backend;
Redis mode provides neither guarantee. The current queue uses `queue` as the
canonical configuration name and accepts `topic` as a compatibility alias.

Sources:

- <https://github.com/iii-hq/workers/blob/queue/v0.21.17/queue/src/functions.rs>
- <https://github.com/iii-hq/workers/blob/queue/v0.21.17/queue/src/trigger.rs>

The upstream bridge from `database::row-changed` to
`iii::durable::publish` is not a built-in transform. The deployment must provide
a bridge function that wraps the row-change payload as queue `data`.

### Embedding router

The current component is the `llm-router` worker; there is no separate released
worker named `llm-worker`. `router::embed` is the provider-independent function:

```json
{
  "input": ["text one", "text two"],
  "provider": "openai",
  "model": "text-embedding-3-small"
}
```

```json
{
  "provider": "openai",
  "model": "text-embedding-3-small",
  "embeddings": [[0.1, 0.2], [0.3, 0.4]]
}
```

Only `input` is required upstream. Explicit provider/model values avoid router
fallback. The response contains float32 vectors in input order but no dimensions
or usage metadata. The router gives provider calls 30 seconds and performs no
retry. The OpenAI provider accepts 1–512 inputs and uses a 20-second HTTP
timeout.

Sources:

- <https://github.com/iii-hq/workers/blob/llm-router/v1.4.27/llm-router/src/embed.rs>
- <https://github.com/iii-hq/workers/blob/provider-openai/v1.2.16/provider-openai/src/embed.rs>

Engine/SDK 0.24.0 contains the required function invocation and trigger APIs.
Current database and queue workers build against SDK 0.23; llm-router builds
against a 0.22 alpha. Wire compatibility is expected but no upstream manifest
declares or tests this exact binary combination.

### Cron worker

Engine 0.24.0's official compose pins cron 0.21.9. Trigger type `cron` requires
`{"expression":"<six-or-seven-field UTC cron>"}` and invokes the target with
`trigger`, `job_id`, `scheduled_time`, and `actual_time`. Missed fires and failed
calls are not retried. The cron caller waits 30 seconds, does not cancel timed-out
target work, and can overlap a later fire after timeout. Registration readiness
must distinguish the target function namespace from the cron provider's trigger
namespace. Cron 0.21.9 discards the registered target namespace and invokes the
function unqualified from the provider worker; correct routing therefore requires
the target function and cron provider to share one namespace.

Sources:

- <https://workers.iii.dev/workers/cron?version=0.21.9>
- <https://github.com/iii-hq/workers/blob/cron/v0.21.9/cron/src/scheduler.rs>
- <https://github.com/iii-hq/workers/blob/cron/v0.21.9/cron/src/locks.rs>
- <https://docs.rs/iii-sdk/0.24.0/iii_sdk/builtin_triggers/struct.CronCallRequest.html>

## Requirement-to-Asset Map

| Requirement | Existing asset | Gap |
| --- | --- | --- |
| 1. Availability | Worker registration/readiness patterns | New package, two functions, queue and cron trigger plans, exact readiness matching |
| 2. Queue events | Upstream row-change shape and durable subscriber | Strict event contract, table normalization, key bounds, exact memory load |
| 3. Reconciliation | Memory and embedding tables | Missing-embedding query, bounded ordering, stateless reselection, result counts |
| 4. Canonical input | Memory title/content/concepts | Versioned serialization contract and byte bound |
| 5. Generation | Verified `router::embed` contract | Typed adapter, timeout, response/model/vector validation, float32 decoding |
| 6. Persistence | Reusable `MemoryStore::insert_embedding` | Duplicate-as-complete mapping and queue/reconciliation coordination |
| 7. Bounds | Bounded runtime conventions | Shared active-call permits, batch policy, drain behavior, trigger-level outcomes |
| 8. Privacy | Typed opaque error conventions | Protected-field taxonomy and sentinel coverage for router/read/reconcile paths |
| 9. Verification | Engine and PostgreSQL fixture patterns | New protocol fake, missing-embedding PG17/18 cases, CI/Nix outputs |

## Constraints and Risks

- Row-change loss before queue publication is unavoidable in the requested
  topology. Reconciliation is the recovery path.
- A permanently invalid or repeatedly failing earliest memory can starve later
  versions if reconciliation always selects the first missing page. Fair retry
  progression requires durable attempt/cursor state, a terminal failure policy,
  or a different bounded selection contract.
- Queue retry after a committed embedding can return duplicate conflict.
  Conflict must not replace the first vector.
- Queue and reconciliation can generate the same key concurrently. The database
  uniqueness constraint protects storage but does not prevent duplicate model
  cost.
- Parsing vectors directly as float64 can violate the memory-store float4-exact
  contract. Router components must be decoded as float32 and widened losslessly.
- Existing embeddings carry no model provenance. A configuration change creates
  a mixed corpus and cannot identify or replace old vectors.
- Vector search silently excludes stored dimensions incompatible with the query
  vector. Query-vector generation and model alignment remain outside this spec.
- Queue Redis mode does not provide durable retry/DLQ semantics. RabbitMQ has a
  documented subscriber-topic DLQ browse/redrive defect in queue v0.21.17.
- llm-router requires separately deployed state, configuration, and provider
  workers.

## Implementation Options

### Option A: Extend `memory-store`

Add exact-load and missing-embedding list operations to the shared library, then
create a thin worker around those operations.

- Reuses one persistence abstraction and keeps table decoding centralized.
- Broadens `memory-schema-search` beyond its approved four-operation boundary
  and exposes reconciliation-specific queries to unrelated consumers.

### Option B: New self-contained worker adapters

Create worker-owned read/reconciliation database operations and reuse only
`MemoryStore::insert_embedding` for the canonical write.

- Preserves existing public contracts and follows the MCP worker's boundary
  exception precedent.
- Duplicates canonical memory row decoding and database-envelope handling.

### Option C: Focused shared read port plus new worker

Add a narrow exact-memory read contract to `memory-store`, keep missing-work and
retry state worker-owned, and reuse existing embedding insertion.

- Shares canonical row decoding while keeping scheduling policy local.
- Requires coordinated changes across the library and worker and a clear limit
  on the new shared API.

## Design Decisions

### Keep reads and reconciliation worker-owned

- **Selected**: Option B. Add a worker-local database port for exact key loading
  and missing-embedding selection; reuse `MemoryStore::insert_embedding` for the
  authoritative write.
- **Rationale**: The read operations exist only to drive this worker and do not
  broaden the approved `memory-store` API.
- **Trade-off**: Canonical row decoding follows the MCP adapter pattern instead
  of becoming one shared implementation.

### Use one coordinator for both triggers

- Queue adaptation and missing-work selection are separate entry boundaries.
  Both produce the same `EmbeddingWorkItem` values for one coordinator.
- The coordinator owns rendering, router batching, vector validation, and
  insertion. Source-specific behavior does not enter the router or writer ports.

### Keep reconciliation stateless

- **Selected by user**: Deterministic first-page selection with no attempt table,
  cursor, backoff record, claim, or terminal skip.
- **Trade-off**: Repeated permanent failures can delay later missing versions.
  The worker does not claim eventual completeness or starvation freedom.

### Adopt upstream trigger and router contracts

- Use `durable:subscriber`, SDK `CronCallRequest`, and `router::embed` directly.
  Do not define alternate queue or model envelopes.
- Pin the design baseline to engine/SDK 0.24.0, cron 0.21.9, database 0.5.20,
  queue 0.21.17, and llm-router 1.4.27 contract snapshots.

### Use canonical JSON input version 1

- Serialize one fixed-field object containing a format identifier, title,
  content, and ordered concepts. The JSON bytes are the router input.
- This provides unambiguous boundaries and snapshot compatibility without a
  custom escaping or delimiter scheme.

### Bound reconciliation below the cron deadline

- Cron 0.21.9 waits at most 30 seconds and does not cancel timed-out target
  work. Both entry points use one router batch and an absolute deadline below
  that bound. Their event/reconciliation limits cannot exceed the batch limit.
- Timed-out remote router/provider work may continue outside the worker's local
  wait bound, but it cannot produce a local embedding write after the caller has
  discarded the response. `MAX_IN_FLIGHT` is not an upstream provider
  concurrency guarantee.

### Avoid schema changes

- No provider/model provenance or retry state is added. Existing embedding
  uniqueness remains the concurrency boundary; a duplicate insert is complete.
- Changing provider/model affects only memory versions still missing vectors and
  can create a mixed corpus by explicit product choice.

## Complexity

- **Effort**: L (1–2 weeks). The feature combines two triggers, external router
  integration, bounded reconciliation, concurrency, PostgreSQL verification,
  and process-level fakes.
- **Risk**: Medium. Local patterns are strong, but best-effort fairness, upstream
  version interoperability, and model-space immutability require focused tests.

## Implementation Validation Items

- Lock row-change, queue, cron, catalog, database, and router envelopes in the
  protocol fake against the cited versions.
- Verify the default namespace restriction and sub-30-second invocation deadline
  through release-process scenarios.
- Run the static missing-work and exact-load queries unchanged on PostgreSQL 17
  and 18.
