# Requirements Document

## Introduction

The memory embedding worker generates the canonical-memory embeddings described
in [brief.md](brief.md). It handles queued insert notifications and scheduled
reconciliation without changing memory or search ownership.

## Boundary Context

- **In scope**: Queue-driven embedding generation, exact memory loading,
  deterministic title-content-concepts input, `router::embed` invocation,
  immutable embedding persistence, all-version reconciliation, bounded
  processing, readiness, and content-safe failures.
- **Out of scope**: Row-change publication, infrastructure provisioning, memory
  creation or extraction, query embeddings, replacement or model migration of
  existing embeddings, durable reconciliation attempt state, no-starvation
  guarantees, deletion, retention, and vector indexing.
- **Adjacent expectations**: A deployment bridge publishes committed memory
  insert events to the configured queue; memory storage supplies immutable
  versions and embedding insertion; a retry-capable non-Redis queue backend and
  cron worker trigger the two exposed functions in their supported namespace;
  llm-router and one configured provider generate vectors.

## Requirements

### Requirement 1: Worker Availability
**Objective:** As an operator, I want explicit event and reconciliation entry points, so that normal generation and repair can run independently.

#### Acceptance Criteria
1. When the worker starts with valid memory, queue, schedule, provider, model, and resource settings, the Memory Embedding Worker shall register one queue-consumer function and one reconciliation function.
2. When both functions are registered, the Memory Embedding Worker shall bind the queue consumer to the configured durable topic and the reconciliation function to the configured cron schedule.
3. When startup registration completes, the Memory Embedding Worker shall report readiness only after both functions and both trigger bindings are active and owned by the current process.
4. If required configuration is missing, empty, or invalid, the Memory Embedding Worker shall terminate startup without reporting readiness.
5. If function or trigger registration fails or does not match the configured destination, the Memory Embedding Worker shall not report readiness.

### Requirement 2: Queued Memory-Insert Events
**Objective:** As a queue operator, I want the consumer to accept the database row-change payload directly, so that delivery requires no worker-specific queue envelope.

#### Acceptance Criteria
1. When a committed memory insert event contains the configured database, the canonical memory table, insert operation, affected-row count, event time, and complete returned ID/version keys, the Memory Embedding Worker shall accept every returned memory key up to the configured event-key limit.
2. When the queue invokes the consumer, the Memory Embedding Worker shall interpret the queue payload itself as the row-change event.
3. If a row-change event is malformed, truncated, exceeds the event-key limit, names another database or table, names another operation, or has inconsistent affected-row and key counts, the Memory Embedding Worker shall reject it before loading a memory or requesting an embedding.
4. When an accepted key identifies a memory version that already has an embedding, the Memory Embedding Worker shall complete that key without requesting another embedding.
5. If an accepted key does not identify a stored memory version, the Memory Embedding Worker shall return a delivery error.
6. The Memory Embedding Worker shall not require memory title, content, concepts, or vector values in a queued event.

### Requirement 3: Missing-Embedding Reconciliation
**Objective:** As an operator, I want scheduled repair of missed events, so that every immutable memory version remains eligible for embedding generation.

#### Acceptance Criteria
1. When the reconciliation function is invoked, the Memory Embedding Worker shall discover every immutable memory version that lacks an embedding, regardless of whether it is the latest version for its memory ID.
2. When missing versions exist, the Memory Embedding Worker shall select at most the configured reconciliation limit in deterministic ID/version order.
3. When no missing version exists, the Memory Embedding Worker shall complete reconciliation with zero selected, generated, and stored embeddings.
4. When a selected version gains an embedding before its work is committed, the Memory Embedding Worker shall treat that version as complete without replacing the stored embedding.
5. If reconciliation fails after storing some embeddings, the Memory Embedding Worker shall retain those embeddings and leave every remaining version eligible for a later invocation.
6. When reconciliation completes successfully, the Memory Embedding Worker shall return content-free counts for selected, generated, already-present, and stored versions.
7. The Memory Embedding Worker shall not represent bounded stateless reconciliation as an eventual-completeness or no-starvation guarantee.

### Requirement 4: Canonical Embedding Input
**Objective:** As a retrieval maintainer, I want equivalent memories embedded from equivalent text, so that queue and reconciliation paths produce the same semantic input.

#### Acceptance Criteria
1. When a memory version requires generation, the Memory Embedding Worker shall construct one deterministic input from that version's title, content, and ordered concepts.
2. The Memory Embedding Worker shall use one versioned input format for both queue-driven and reconciliation processing.
3. The Memory Embedding Worker shall preserve the title, content, and concept text exactly within the canonical input.
4. The Memory Embedding Worker shall not include memory IDs, versions, types, timestamps, files, session IDs, or source observation IDs in the embedding input.
5. If the canonical input exceeds the configured input-size bound, the Memory Embedding Worker shall reject that memory version without calling the embedding router.

### Requirement 5: Routed Embedding Generation
**Objective:** As an operator, I want generation routed through one configured provider and model, so that stored vectors have one deployment-defined semantic space.

#### Acceptance Criteria
1. When one or more canonical inputs are ready, the Memory Embedding Worker shall request embeddings through `router::embed` using the configured non-empty provider and model.
2. When generation succeeds, the Memory Embedding Worker shall require the returned provider and model to match the configured values and one vector to correspond to each requested input in request order.
3. The Memory Embedding Worker shall accept only vectors containing from one through 16,000 finite float32 components with a non-zero norm.
4. If the router fails, returns another provider or model, returns the wrong vector count, or returns an invalid vector, the Memory Embedding Worker shall not store any invalid or unmatched vector.
5. The Memory Embedding Worker shall not call a provider embedding function directly.
6. If a generation request fails, the Memory Embedding Worker shall not retry it independently of queue delivery or a later reconciliation invocation.

### Requirement 6: Immutable Embedding Persistence
**Objective:** As a memory-store operator, I want generated vectors associated idempotently, so that retries never replace canonical data.

#### Acceptance Criteria
1. When a valid generated vector is available for a memory version without an embedding, the Memory Embedding Worker shall submit one embedding association for that exact ID and version.
2. When embedding persistence succeeds, the Memory Embedding Worker shall report that version as stored.
3. If persistence reports that the memory version already has an embedding, the Memory Embedding Worker shall treat the version as already complete.
4. If persistence reports a missing memory version or another failure, the Memory Embedding Worker shall report failure without reporting the embedding as stored.
5. The Memory Embedding Worker shall not update or delete a canonical memory or an existing embedding.
6. When provider or model configuration changes, the Memory Embedding Worker shall apply the new configuration only to versions that still lack embeddings.

### Requirement 7: Delivery Outcomes and Resource Bounds
**Objective:** As an operator, I want finite work and accurate delivery outcomes, so that queue retries and cron invocations remain safe under load.

#### Acceptance Criteria
1. When every key in a queued event is stored or already complete, the Memory Embedding Worker shall return delivery success.
2. If any queued key fails validation, loading, generation, or persistence, the Memory Embedding Worker shall return a delivery error.
3. The Memory Embedding Worker shall enforce positive finite limits for event keys, reconciliation candidates, inputs per router request, locally awaited router requests, input bytes, operation timeouts, and shutdown drain time.
4. While the local router-wait limit is reached, the Memory Embedding Worker shall not start another router request.
5. When shutdown begins, the Memory Embedding Worker shall stop admitting new queue and reconciliation work and drain admitted work only within the configured shutdown bound.
6. The Memory Embedding Worker shall not represent queue delivery or row-change publication as lossless or exactly once.

### Requirement 8: Content-Safe Operations
**Objective:** As an operator, I want useful failures without protected content, so that generation can be diagnosed safely.

#### Acceptance Criteria
1. If an operation fails, the Memory Embedding Worker shall report a stable stage and failure category without returning partial embeddings as successful.
2. The Memory Embedding Worker shall not include memory text, concepts, vectors, memory keys, credentials, router responses, backend messages, or database statements in logs or returned errors.
3. The Memory Embedding Worker shall not expose provider credentials through readiness or configuration diagnostics.
4. When reconciliation succeeds, the Memory Embedding Worker shall return counts without memory identities or content.

### Requirement 9: Focused Verification
**Objective:** As a maintainer, I want automated coverage of event, router, storage, and repair behavior, so that enrichment regressions are detected before deployment.

#### Acceptance Criteria
1. The Memory Embedding Worker shall include automated verification for valid, malformed, unrelated, truncated, oversized, missing, and already-complete row-change events.
2. The Memory Embedding Worker shall include automated verification for all-version missing-embedding discovery, deterministic limits, empty reconciliation, partial failure, and later repair.
3. The Memory Embedding Worker shall include automated verification for canonical title-content-concepts input and input-size rejection.
4. The Memory Embedding Worker shall include automated verification for router request identity, result order, provider/model mismatch, invalid vectors, timeouts, and router failures.
5. The Memory Embedding Worker shall include automated verification for successful insertion, duplicate completion, missing parents, concurrent queue and reconciliation work, and immutable existing embeddings.
6. The Memory Embedding Worker shall include automated verification for registration ownership, queue and cron trigger readiness, bounded concurrency, shutdown, and content-safe diagnostics.
7. The Memory Embedding Worker shall run focused automated verification without live iii, queue, cron, database, llm-router, embedding-provider, or PostgreSQL services.
