# Memory Embedding Worker

This private worker will generate immutable embeddings for canonical memory
versions through the deployment-selected `router::embed` contract.

## Launch Contract

The release binary is launched through `iii.worker.yaml` after a release build.
Its eventual runtime configuration requires a memory database target, durable
queue topic, cron expression, router provider and model identifiers, resource
bounds, and managed iii settings in the `default` namespace.

The deployment must provide the following upstream contracts before the worker
can become ready:

- Canonical memory storage with immutable memory versions and embedding
  association through `memory-store`.
- A bridge that publishes committed canonical-memory insert events to the
  configured durable queue topic.
- A retry-capable durable queue backend other than Redis.
- A cron worker that invokes the reconciliation function.
- An llm-router worker and one configured embedding provider and model.

This package does not provision those upstream services. On startup, it
registers the queue-consumer and reconciliation functions with their triggers,
verifies current-process ownership, and emits readiness only after both
functions and triggers are active.
