# MCP Worker Protocol Fake

This binary starts the release `mcp-worker` executable through piped standard
input, output, and error streams. It verifies invalid and partial-embedding
startup diagnostics, absent or blank embedding startup, stateless server and
tool discovery, invalid metadata and tool arguments, oversized-frame recovery,
bounded undrained-output recovery, EOF draining, and SIGINT/SIGTERM shutdown
without a harness, MCP network service, or database. It also runs
`memory_save` success, invalid-input, conflict, and backend-failure flows, plus
representative enabled combined, lexical-only fallback, caller-isolation, and
BM25 or vector-search failure `memory_search` flows through a loopback
WebSocket fake for managed III worker registration, `router::embed`, and
`database::execute`. Representative `memory_list_versions` default,
continued-page, empty-page, invalid-input, and backend-failure flows use the
same loopback engine. Exact and latest retrieval cover complete canonical
records, not-found, invalid-input, malformed-response, and backend-failure
flows through that engine.

Build the worker, then pass its absolute release path to the fake:

```sh
nix develop --command cargo build --locked --release --package mcp-worker
nix develop --command cargo run --locked --release --package mcp-worker-fake -- \
  --worker "$PWD/target/release/mcp-worker"
```

`--worker` is canonicalized before launch, so the fake does not depend on its
current working directory. `--timeout-seconds`, `--max-line-bytes`, and
`--stderr-cap-bytes` accept positive values and default to `5`, `65536`, and
`65536` respectively.

The discovery and pre-operation rejection scenarios set the required
`TOTAL_RECALL_MEMORY_DATABASE` value and an unreachable loopback `III_URL`.
Every worker launch removes inherited `TOTAL_RECALL_EMBEDDING_PROVIDER`,
`TOTAL_RECALL_EMBEDDING_MODEL`, and `TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS`
values, so query embedding stays disabled unless a scenario configures it.
The partial-embedding startup scenarios leave exactly one of provider or model
non-blank: provider only, model only, a provider with a whitespace-only model,
and an empty provider with a model. Each writes one discovery request at launch,
accepting a broken pipe from a worker that already exited, and requires an
unsuccessful exit, empty stdout, and exactly the fixed stderr diagnostic that
names both variables without either sentinel value. Absent settings and blank
(empty and whitespace-only) settings must start normally, answer discovery,
exit cleanly at EOF, and write nothing to stderr.
The oversized-input scenario sends one bounded raw frame only to verify
worker-side recovery; it never logs that frame. The child client caps
every normal request and response line, bounds stderr capture while continuing
to drain it, applies a deadline to each operation, and terminates and reaps a
child before returning a request/response failure. Captured stderr is never
included in those errors. The undrained-output scenario first completes a
discovery round trip, then sends a finite set of valid discovery requests while
deliberately not reading stdout. Its child-stdin writer observes a real
`AsyncWrite::poll_write` `Poll::Pending` result before reads resume, rather than
inferring pressure from a fixed request count or timing. It then validates every
unique response and clean EOF/stderr reaping. If no pending write is observed
before the deadline, the scenario fails and terminates/reaps the child.

The save scenarios bind the fake engine to loopback only, validate the managed
worker registration, capture one `database::execute` request, and send a
deterministic insert confirmation, conflict envelope, or remote invocation
failure. Invalid save requests must make no database invocation before the fake
deadline. No PostgreSQL service is started.

The enabled combined-search scenarios set sentinel
`TOTAL_RECALL_EMBEDDING_PROVIDER`, `TOTAL_RECALL_EMBEDDING_MODEL`, and
`III_NAMESPACE` values, then send a query-only request whose query keeps
surrounding whitespace and Unicode sentinels. The fake engine withholds every
reply until it holds one static BM25 query and one `router::embed` call, so a
worker that awaits either before starting the other fails at the request
deadline. It answers the router with the configured provider, model, and one
float32-exact vector, then requires exactly one static vector query. All three
requests must carry the managed namespace; the router request must contain only
the exact query and the configured provider and model, BM25 must receive the
same query bytes, vector search must receive the generated vector, and both
searches must share the requested limit. Deterministic candidates cover
within-search and cross-search duplicates. They prove the score-free MCP result
keeps the greater duplicate version, retains the first lexical candidate for
same-ID same-version ties despite a higher-relevance vector candidate, sorts IDs
before applying its final cap, and omits the generated vector and embedding
identity. A second enabled run returns no rows from either search and requires
an empty result collection. No router, embedding provider, or PostgreSQL
service is started.

The lexical-only fallback scenarios send the same query with limit `6` and
keep the managed namespace. The disabled-embedding launch omits the provider
and model; the fake engine answers its only BM25 query, and any `router::embed`
call or vector query fails the scenario. The remote-failure and
invalid-response launches configure the sentinel identity, and the engine
withholds replies until BM25 and `router::embed` have both started. It then
answers the router with a remote invocation error carrying code, message,
credential, and stack-trace sentinels, or with the configured provider, a
different resolved model, and one otherwise canonical vector, so only identity
validation keeps that vector out of vector search. After its last reply the
engine rejects a router retry, a vector query, or a repeated BM25 query until
the worker disconnects, and a worker blocked on such a request fails at the
request deadline with the engine's diagnosis. Every fallback path must send
exactly one BM25 query with the exact query bytes and requested limit. A full
six-row BM25 page with a later greater version, a same-version duplicate, and
a later lesser version must return three ID-sorted, score-free lexical
candidates; a second disabled run with no BM25 rows must return an empty
collection. Responses exclude the query, configured and resolved embedding
identities, router error and credential sentinels, and router vector
components; stderr must be empty and also exclude memory content. The memory
store rejects BM25 pages longer than the requested limit, so only the combined
scenario can prove the cap after deduplication.

The search-failure scenarios send the combined query with limit `5`. With query
embedding disabled, the engine answers the only BM25 query with a remote
invocation error. With query embedding enabled, it withholds replies until BM25
and `router::embed` have both started. The BM25-failure run answers BM25 with a
remote invocation error before answering the router with the configured
identity and one valid vector, so a worker that abandons generation after the
BM25 failure never sends the chained vector search the engine requires; the
engine answers that search with a full page of sentinel candidates. The
vector-failure run answers BM25 with sentinel candidates and the router with
the same vector, then answers vector search with a full page whose last row has
a malformed version. Enabled requests must match the combined-search BM25,
router, and vector requests, and the disabled request must be the same BM25
query; after its last reply the engine rejects any further router call, vector
query, or BM25 query until the worker disconnects. BM25 failures must return
the text-only `backend_failure` tool error and the vector failure must return
`internal_failure`, without structured content, candidates, engine-error
sentinels, the query, the embedding identity, or vector components; stderr must
be empty.

The caller-isolation scenario launches with query embedding enabled and sends a
legacy `vector` argument, `vector: null`, another unknown argument, a string
limit, an empty query, a NUL-containing query, and limits `0` and `51`. Each
must return exactly the text-only `invalid_input` tool error without the query,
vector, or unknown-argument sentinels, and stderr must be empty. Any
`router::embed` or `database::execute` request before the fake deadline fails
the scenario. The enabled combined-search scenario uses the same launch, so the
absent router traffic is not an artifact of disabled generation.

The version-list scenarios capture exactly one static query that projects only
version and updated-at metadata, orders versions descending, and uses ID,
offset, and limit parameters. The default request proves offset `0` and probe
limit `51`; a small continued page proves requested-limit-plus-one probing and
the checked next offset; an empty page has no next offset. Invalid IDs, offsets,
and limits make no database invocation before the fake deadline. Backend
failures return text-only errors with no structured or protected values. No
PostgreSQL service is started.

The retrieval scenarios capture the exact `(id, version)` query and the latest
head-to-memory join query. They verify every canonical memory field, latest-head
selection without an `ORDER BY` claim, absent embeddings, text-only errors, and
protected response and engine-error sentinels absent from errors and stderr. No
PostgreSQL service is started.
