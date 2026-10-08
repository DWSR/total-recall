# Implementation Plan

This plan covers the query-embedding and lexical-fallback amendments against the
implemented MCP worker. Unchanged capabilities are regression scope, not
reimplementation scope.

- [x] 1. Establish optional query-embedding foundations
- [x] 1.1 Add optional embedding configuration
  - Represent embedding as disabled when provider and model are both absent, empty, or whitespace-only and enabled when both are non-blank.
  - Reject partial provider/model configuration and validate the embedding timeout only for enabled mode.
  - Preserve accepted identities without exposing them through debug or startup failures; retain existing managed iii namespace behavior.
  - Keep the technical launch contract synchronized as an incidental implementation update.
  - Completion is observable when pure tests cover disabled, partial, enabled, timeout-boundary, redaction, and pre-stdio startup behavior.
  - _Requirements: 1.1, 1.6, 3.2, 3.11, 3.12, 6.5, 7.5, 7.6_
  - _Boundary: Config_

- [x] 1.2 (P) Define the query-embedding service boundary
  - Add a focused asynchronous generator contract that returns one canonical embedding or a content-safe unavailable or invalid-response failure.
  - Provide recording and failing doubles without importing MCP, runtime, or memory-operation behavior.
  - Completion is observable when boundary tests distinguish successful, unavailable, and invalid-response outcomes without protected values.
  - _Requirements: 3.2, 3.13, 6.4, 6.5, 8.3, 8.5, 8.7_
  - _Boundary: Query Embedding Generator_

- [x] 1.3 Implement routed generation and exhaustive response validation
  - Resolve `router::embed` through the effective managed iii namespace and send the exact query as one input with configured provider and model.
  - Give timeout ownership to the SDK request, make no retry, require exact response identity and cardinality, and decode components directly as float32 before exact widening.
  - Enforce dimensions, finiteness, and non-zero norm while keeping requests, responses, identities, queries, vectors, and credentials out of diagnostics.
  - Completion is observable when adapter tests cover the exact request, namespace, success, timeout cleanup, remote failure, identity/cardinality mismatch, every invalid-vector class, redaction, and zero retries.
  - _Depends: 1.1, 1.2_
  - _Requirements: 3.2, 3.3, 3.4, 3.13, 6.4, 6.5, 8.3, 8.5, 8.7_
  - _Boundary: Query Embedding Generator_

- [x] 2. Integrate lexical fallback into combined search
- [x] 2.1 Propagate optional generation through static process composition
  - Carry an optional generator as a static dependency through the service, server, bounded runtime, and production assembly without type erasure.
  - Update constructor and fixture composition while leaving the existing caller-vector search path behaviorally unchanged in this task.
  - Preserve current memory operations, admission, queue, output, and shutdown ownership.
  - Completion is observable when package, service, server, runtime, runtime-server, and lifecycle suites compile and pass with disabled and deterministic enabled generators.
  - _Depends: 1.3_
  - _Requirements: 1.1, 1.2, 1.7, 3.11, 6.6, 7.1, 7.4, 8.1, 8.6, 8.7_
  - _Boundary: Memory Tool Service, MCP Server and Runtime, Query Embedding Generator_

- [x] 2.2 Replace caller vectors with mandatory BM25 and optional semantic search
  - Change search input and schema to accept only a query and limit; reject caller vectors and other unknown arguments.
  - Reject empty or NUL-containing queries and invalid limits with zero generator, BM25, or vector activity while preserving every other query byte.
  - Start BM25 and enabled generation concurrently; use empty vector candidates when generation is disabled or returns any typed error, and invoke vector search only for a valid generated vector.
  - Preserve exact query/vector/limit forwarding, deterministic score-free projection and merge, and distinct no-partial-result errors for BM25 or post-generation vector-search failure.
  - Completion is observable when contract, service, and server tests prove every path, both failure mappings, lexical fallback, combined search, and unchanged result ordering and limits.
  - _Depends: 2.1_
  - _Requirements: 1.3, 1.4, 1.5, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 3.10, 3.11, 3.13, 3.14, 3.15, 3.16, 3.17, 3.18, 6.1, 6.2, 6.3, 6.4, 6.5, 8.1, 8.3, 8.5_
  - _Boundary: Contracts, Memory Tool Service, MCP Server and Runtime_

- [x] 2.3 Verify timeout, dispatch, and runtime isolation
  - Prove legacy caller-vector requests are rejected before generator or memory-operation activity.
  - Verify an SDK-owned embedding timeout returns lexical results and releases the whole-request permit so a later request progresses.
  - Retain existing finite queue, active-call, stdout, EOF, signal, and shutdown behavior with optional-generator fixtures.
  - Completion is observable when server and runtime integration tests pass strict query-only dispatch, lexical timeout recovery, subsequent-request progress, and unchanged resource bounds.
  - _Depends: 2.2_
  - _Requirements: 1.1, 1.2, 1.7, 1.8, 3.10, 3.13, 3.16, 6.3, 6.4, 6.5, 6.6, 7.1, 7.2, 7.3, 7.4, 8.1, 8.3, 8.5, 8.6, 8.7_
  - _Boundary: MCP Server and Runtime_

- [x] 3. Validate process behavior and unchanged capabilities
- [x] 3.1 Add enabled combined search to the protocol fake
  - Start BM25 and one routed embedding call concurrently, then issue vector search only after valid generation and capture all three requests.
  - Verify configured identity, exact query, generated-vector forwarding, shared result limit, deterministic merge, and embedding-free output.
  - Completion is observable when the release worker passes the representative enabled combined-search path without live router, provider, database, or PostgreSQL services.
  - _Depends: 2.3_
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 3.17, 3.18, 8.3, 8.5, 8.7_
  - _Boundary: Protocol Fake_

- [x] 3.2 Exercise lexical-only fallback through the protocol fake
  - Add disabled embedding, representative remote failure, and representative invalid-response scenarios.
  - Verify each path makes one BM25 call, no vector call, and returns deterministic score-free lexical results.
  - Keep queries, identities, vectors, provider responses, credentials, and memory content out of errors and stderr.
  - Completion is observable when all representative fallback paths succeed without live embedding or database infrastructure.
  - _Requirements: 3.1, 3.9, 3.11, 3.13, 6.5, 8.3, 8.5, 8.7_
  - _Boundary: Protocol Fake_

- [x] 3.3 Exercise startup, search-failure, and caller-isolation scenarios
  - Prove partial embedding configuration fails before protocol readiness and fully absent configuration starts normally.
  - Verify BM25 failure and post-generation vector-search failure return no partial results.
  - Verify legacy caller-vector input causes zero router or database activity and retains content-safe protocol diagnostics.
  - Completion is observable when startup and request failures return their stable categories without protected data or live infrastructure.
  - _Requirements: 1.6, 3.10, 3.12, 3.14, 3.15, 3.16, 6.3, 6.4, 6.5, 8.3, 8.5, 8.7_
  - _Boundary: Protocol Fake_

- [x] 3.4 Complete amendment acceptance and unchanged-feature regression
  - Run the full existing save, exact/latest retrieval, version-list, discovery, bounded-input, backpressure, EOF, signal, privacy, and search suites with updated fixtures.
  - Run locked workspace tests, release builds, protocol smoke, formatting, Clippy, Nix checks, and dependency-policy gates.
  - Route failures to the owning amendment boundary rather than redesigning unchanged capabilities.
  - Completion is observable when every required command passes and no automated test needs a live harness, MCP network service, embedding provider, or database.
  - _Depends: 3.1, 3.2, 3.3_
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 3.10, 3.11, 3.12, 3.13, 3.14, 3.15, 3.16, 3.17, 3.18, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7_
  - _Boundary: Integration_

## Implementation Notes

- `cargo clippy` and `cargo fmt` resolve rustup proxies in `~/.cargo/bin` (clippy 0.1.96 fails); prefix them with `PATH="$PATH:$HOME/.cargo/bin"` to use the Nix 1.98.1 tools.
- Child-process fixtures (`tests/lifecycle.rs` `worker_command`, mcp-worker-fake `command_with_iii_url`) must `env_remove` `TOTAL_RECALL_EMBEDDING_PROVIDER`, `TOTAL_RECALL_EMBEDDING_MODEL`, and `TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS`; both fixtures do so since 2.1 and 3.1, and the suites pass under a polluted parent environment.
- `QueryEmbeddingSettings` and `EmbeddingIdentifier` have no public constructors; obtain validated settings through `Config::from_values`.
- `RecordingQueryEmbeddingGenerator` returns one configured result for every call; tests that need blocking, delayed, or concurrency-observing generation should define a local generator instead of extending the shared doubles.
- Search must never cancel an in-flight `generate` future (no `try_join!` or `select!` short-circuit on BM25 failure): dropping the SDK future before its own timeout skips pending-invocation cleanup. `EmbeddingVector` derives a component-printing `Debug`, so generated vectors must never be formatted.
- Workspace builds enable serde_json `arbitrary_precision` through `harness-events-cli`; package-only builds do not. Router decoding is verified under both.
- 2.1 left `#[expect(dead_code)]` on `MemoryToolService.generator` and generator-not-invoked assertions in the parameterized search fixtures; 2.2 must remove the expectation and replace those assertions deliberately.
- The single-managed-client invariant is guarded only probabilistically because the lifecycle fake accepts one connection; a deterministic no-second-connection check belongs in a later fake.
- From 2.2 until the protocol-fake tasks land, `mcp-worker-fake` fails first at its `tools/list` snapshot (still advertises `vector`); its search arguments, the exactly-two-call `capture_search_invocations`, and README search text also predate the query-only contract.
- When BM25 fails and generation succeeds, the chained vector search still runs and its result is discarded; fake scenarios for BM25 failure with embedding enabled must answer that vector database call.
- A loopback `FakeRouterEngine` (registration handshake, `router::embed` frames, `/otel` side socket drain) exists in both `tests/embedding.rs` and `tests/runtime_server.rs`; some `run_mcp_server` awaits in `tests/runtime_server.rs` have no time bound, so a permit leak hangs those tests instead of failing them.
- The fake runs search flows last. Since 3.3 the full smoke passes: caller isolation runs with embedding enabled and proves zero router and database activity, and the rebuilt BM25 and vector failure scenarios replace the caller-vector era ones. Both-searches-fail precedence and remote vector-search failure remain covered only by worker unit tests.
- The fake's `terminate_memory_save_scenario` terminates twice after a failed request, masking request failures as `worker stderr capture failed`; the combined-search scenario uses `finish_failed_combined_memory_search` to report the engine error instead.
- `memory-store` rejects search pages with more rows than the requested limit, so the post-dedup cap is observable only on the combined path; lexical-only fixtures cannot prove it.
- Pre-existing and outside the amendment: `flake.nix` `mkWorker` lacks `__darwinAllowLocalNetworking` for loopback tests on sandboxed Darwin builders (this host builds with `sandbox = false`), and the CI PostgreSQL verifier job uses `path:` flake references.
