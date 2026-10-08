# Harness Persistence Engine Fake

This binary launches the release persistence worker through its
`iii.worker.yaml` start command against an in-process WebSocket fake of the iii
engine. It validates the worker's subscriber readiness ownership, one queued
observation delivery, the exact `database::execute` request, and the delayed
write acknowledgment. No iii engine, queue, database worker, or PostgreSQL is
required.

Run it from a checkout with the repository development shell:

```sh
nix develop path:. --command cargo build --locked --release --package harness-event-persistence
nix develop path:. --command cargo run --locked --release --package harness-persistence-engine-fake -- \
  --manifest "$PWD/workers/harness-event-persistence/iii.worker.yaml"
```

The harness binds only to loopback, chooses an ephemeral port, and exits
non-zero on a protocol, ownership, namespace, database-request, ordering, or
opaque-data logging mismatch. `--timeout-seconds` controls the default
30-second operation deadline.
