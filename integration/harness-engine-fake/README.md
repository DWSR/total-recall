# Harness Engine Fake

This binary launches the release worker through its `iii.worker.yaml` start
command against an in-process WebSocket fake of the iii engine. It verifies
worker registration, readiness catalog ownership, harness invocations, and the
resulting `iii::durable::publish` payloads. No iii engine or queue worker is
required.

Run it from a checkout with the repository's development shell:

```sh
nix develop --command cargo build --locked --release --package harness-ingestion
nix develop --command cargo run --locked --release --package harness-engine-fake -- \
  --manifest "$PWD/workers/harness-ingestion/iii.worker.yaml"
```

The same commands can run in a GitHub Actions job. The harness binds only to
loopback, chooses an ephemeral port, and exits non-zero on a protocol or event
contract mismatch. `--timeout-seconds` controls the default 30-second operation
deadline.
