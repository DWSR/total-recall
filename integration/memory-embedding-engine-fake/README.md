# Memory Embedding Engine Fake

This private binary launches the release `memory-embedding` worker from its
`iii.worker.yaml` manifest against a bounded loopback iii peer. It verifies the
two function registrations, durable-subscriber and cron bindings, readiness
catalog ownership, and direct queue and cron delivery for normal, rejection,
failure, retry, timeout, privacy, bounded reconciliation, repair, stateless
reselection, readiness rejection, and process lifecycle flows. The scenarios
assert exact work-load, router, and immutable insert requests; positional vectors;
unordered concurrent writes; no-call event
rejection; one-call router failures; a post-late typed-delivery barrier for router
timeouts; partial-commit state derived from successful insert replies; and
content-free outcomes.
It also verifies received unregister frames and clean server-driven WebSocket
close before stdin-driven exit. iii-sdk 0.24.0 has no outbound unregister flush
barrier, and a clean EOF or `SIGTERM` shutdown after server protocol processing
can drop the WebSocket before queued unregister frames arrive. The clean
terminal drain therefore accepts only `ConnectionClosed`,
`ResetWithoutClosingHandshake`, or `Io(ConnectionReset)` in that case. Any
release frames that arrive must be exact, known, and non-duplicate; malformed
or unexpected release frames fail verification. Expected failed-startup and
forced-termination paths also accept only a loopback `ConnectionReset`, because
the terminated child cannot complete a WebSocket close handshake.

The peer uses deterministic response scripts for database and router calls. A
script supplies a direct function invocation and can return success,
remote-error, malformed, or delayed upstream responses without deployed iii,
queue, cron, database, router, provider, or PostgreSQL services.

The default run executes every scenario. Use `--scenario normal`,
`queue-rejections`, `queue-retry`, `router-failures`, `reconciliation`,
`lifecycle`, or `lifecycle-stress` to run a bounded group while diagnosing a
release-worker failure.

The lifecycle group drives pending, stale, foreign, duplicate, and mismatched
catalog responses against the release worker; validates invalid startup
configuration and namespace failures; holds one router call while a second
admitted delivery waits; and verifies EOF, deadline, and forced-child cleanup.
It keeps stdin open until the selected EOF, signal, or forced-termination
stimulus and validates every received protocol frame. On Unix, the group also
sends `SIGTERM` to exercise the worker signal path. Other targets run the same
EOF lifecycle path only because the fake does not synthesize platform-specific
console signals. `lifecycle-stress` repeats the admitted-work EOF flow serially.

`scripts.start` must be one ASCII executable path with no arguments or shell
syntax. Relative paths are resolved from the manifest directory and launched
directly. The child receives only the fake's deterministic worker configuration;
inherited environment variables are cleared. Stdout and stderr use bounded
chunked capture, and stderr readiness detection accepts only a bounded
`worker ready` line.

Run the package tests:

```sh
nix develop --command cargo test --package memory-embedding-engine-fake --locked
```

Build the release worker, then run the smoke verifier:

```sh
nix develop --command cargo build --locked --release --package memory-embedding
nix develop --command cargo run --locked --release --package memory-embedding-engine-fake -- \
  --manifest "$PWD/workers/memory-embedding/iii.worker.yaml"
```

Run lifecycle coverage and its serial stress pass directly:

```sh
nix develop --command cargo run --locked --release --package memory-embedding-engine-fake -- \
  --manifest "$PWD/workers/memory-embedding/iii.worker.yaml" --scenario lifecycle
nix develop --command cargo run --locked --release --package memory-embedding-engine-fake -- \
  --manifest "$PWD/workers/memory-embedding/iii.worker.yaml" --scenario lifecycle-stress
```
