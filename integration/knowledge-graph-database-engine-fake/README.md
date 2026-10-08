# Knowledge Graph Database Engine Fake

This standalone verifier connects the public high-level graph-store facade to
an in-process iii peer over an ephemeral IPv4 loopback WebSocket. It exercises
the production service and database adapter without a deployed iii engine,
database worker, PostgreSQL, or credentials.

Run the package tests:

```sh
nix develop --command cargo test --locked --package knowledge-graph-database-engine-fake
```

Run the release verifier:

```sh
nix develop --command cargo run --locked --release --package knowledge-graph-database-engine-fake
```

The peer checks request order, function IDs, database target, static SQL,
positional parameters, query timeouts, and response envelopes. Bounded-query
fixtures cover complete and empty neighbor, related-source, and path results,
including orientations, evidence, typed source identities, and result/work
markers. Failure coverage includes the exact worker `QUERY_TIMEOUT` envelope,
prefix/code near misses, malformed query envelopes and payloads, invocation
timeouts, generic failures, protected sentinels, and no retries. It exits
nonzero on an unexpected call, malformed result, retry, premature service
completion, or privacy-contract mismatch.
