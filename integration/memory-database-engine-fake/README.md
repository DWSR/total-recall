# Memory Database Engine Fake

This binary connects the public `IiiMemoryDatabase` adapter to an in-process
loopback WebSocket fake. It verifies exact `database::execute` requests and
response handling without PostgreSQL, a deployed iii engine, or a database
worker.

Run the protocol tests:

```sh
nix develop path:. --command cargo test -p memory-database-engine-fake --locked
```

Run the standalone verifier:

```sh
nix develop path:. --command cargo run -p memory-database-engine-fake --locked
```

The verifier binds an ephemeral IPv4 loopback port and exits nonzero on a
protocol, response-envelope, or privacy-contract mismatch.

The context scenario exercises `MemoryContextStore` against the loopback
`database::execute` protocol. It checks exact bounded-head SQL and positional
`[limit, max_candidate_bytes]` values, ordered narrow success, empty results,
the oversized field-free sentinel, malformed envelopes, opaque engine failures,
and the SDK-owned operation deadline. A delayed reply after the deadline is
terminal: the adapter makes no retry and does not expose protected values.
