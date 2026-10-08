# Research and Gap Analysis

The sections before the dated amendment record the original preimplementation
baseline. The 2026-09-24 amendment supersedes their current-state claims.

## Scope
This analysis compares the approved MCP worker requirements with the implemented workspace, the planned `memory-schema-search` boundary, and Rust MCP 2.0.0 protocol crates.

## Current State
- The workspace implements `harness-ingestion` and `harness-engine-fake`; no MCP or memory-store code exists.
- `memory-schema-search` is approved but unimplemented. It plans `crates/memory-store` with insert-memory, insert-embedding, BM25 search, and vector search operations.
- Existing workers establish strict Serde DTOs, async ports/adapters, content-safe errors, testable configuration, stderr diagnostics, process lifecycle handling, and recording/failing test fakes.
- The root workspace pins Rust 1.98.1, Tokio 1.48.0, Serde 1.0.228, Serde JSON 1.0.145, Chrono 0.4.42, UUID 1.18.1, and iii-sdk 0.24.0.
- No project steering documents exist; approved adjacent specs and implemented code are the available project context.

## Requirement-to-Asset Map

| Requirement area | Reusable asset | Gap |
| --- | --- | --- |
| MCP stdio and discovery | Existing process and subprocess-test patterns | MCP runtime, bounded raw framing, stateless discovery, and protocol fake are missing. |
| Memory creation | Planned `MemoryStore::insert_memory`; UUID and Chrono dependencies | MCP defaults, injectable ID/clock, strict tool input, and persisted-record response mapping are missing. Planned insert returns `()`. |
| Combined search | Planned `search_bm25` and `search_vector` | One tool must validate both inputs, execute both operations, deduplicate by ID, choose the greatest version, remove scores, sort by ID, and cap the union. |
| Retrieval/version history | Planned canonical records and latest-version projection | No exact/latest/paginated-version-list service, port, SQL, decoder, page type, or not-found error exists. |
| Typed contracts | Strict Serde/Schemars patterns | The selected SDK does not generate strict input schemas or output schemas automatically. |
| Content-safe errors | Existing opaque-error and sentinel-test patterns | MCP error categories and redacted result content require a new mapping layer. |
| Bounded processing | SDK framing source provides a bounded-reader reference | Worker-owned positive validation, framing, queueing, concurrency, and process-level proof are missing. |
| Verification | Recording ports and child-process smoke patterns | A newline-delimited JSON-RPC harness and MCP-specific CI smoke are missing. |

## `rust-mcp-sdk` 2.0.0 Findings
- Server-only stdio uses `default-features = false` with `server`, `stdio`, and optionally `macros`.
- Rust 1.80 is the declared minimum. Tokio `^1.4` and Serde JSON `^1.0.143` admit the workspace pins, but the exact final dependency graph still needs a locked build.
- MCP `2026-07-28` is stateless. Requests carry version and capability metadata; no initialize/initialized sequence exists. Requirements were corrected to match this protocol.
- `TransportOptions` defaults to a 16 MiB line limit, queue capacity 36, and 60-second timeout. Oversized lines are discarded through the newline and later bounded messages continue.
- A zero line limit silently drops non-empty input. A zero channel capacity panics when the Tokio channel starts. The worker must reject non-positive configuration first.
- The bounded channel applies reader backpressure but does not limit concurrent handlers because the runtime spawns a task per parsed message.
- `mcp_tool` generates an input schema but omits `additionalProperties: false`; runtime unknown-field rejection requires `serde(deny_unknown_fields)`.
- Tool output schemas and `structuredContent` are supported but not derived or validated by the SDK. The worker must define schemas and verify serialized results.
- Default tool errors can expose their display text as result content. Domain errors require explicit content-safe mapping.
- TRACE transport logging includes raw request prefixes and can expose memory data. The worker must not enable SDK transport tracing; diagnostics must write to stderr.
- `StdioTransport` binds process streams directly, discards messages that fail complete MCP deserialization, and cannot return required errors for missing metadata.
- The SDK runtime spawns one task per parsed message, so its bounded input channel does not bound active or response-blocked tasks.
- `ServerRuntime::shutdown()` and stdin EOF support controlled termination.
- Public SDK runtime dispatch is not reusable without implementing a large `McpServer` shim; the runtime entry points and response stamping are private.
- `rust-mcp-schema` publicly exposes current client/server message, result, and JSON-RPC error types without requiring the stock runtime.

## Interface Mismatches
- The MCP create capability returns the persisted canonical record, while planned memory insertion returns `()`.
- Exact/latest reads need canonical `MemoryRecord` results without relevance scores; version listing needs narrow `(version, updated_at)` entries plus offset-page metadata.
- The planned memory error taxonomy lacks retrieval not-found outcomes.
- MCP JSON numbers need checked conversion to positive `u32` limits and positive `i64` versions.
- The memory store owns canonical validation and search semantics, but the brief assigns retrieval behavior to this feature as a boundary exception.
- “No network transport” excludes MCP HTTP/SSE; the worker may still use the planned iii database adapter as its backend path.

## Implementation Options

### Option A: Extend `memory-store`, then add a thin MCP adapter
Add exact/latest/version-list reads and canonical return values to the planned store. This centralizes SQL and decoding but changes the approved adjacent boundary.

### Option B: Worker-owned memory facade
Define a worker-local port that delegates create and both search operations to `MemoryStore` and owns exact/latest/version-list retrieval through its own adapter. This matches the brief but splits database behavior and error decoding.

### Option C: Shared repository boundary
Extract canonical contracts and a six-operation repository trait shared by persistence and MCP crates. This gives the cleanest long-term seam but introduces the largest restructuring before either feature is implemented.

## Complexity and Risk
- **Current state**: XL effort, high risk. The upstream memory store, schema, database fake, and PostgreSQL smoke are unimplemented.
- **After `memory-schema-search`**: M effort, medium risk. MCP remains a new protocol and SDK integration, while domain persistence/search becomes reusable.
- Root manifest, lockfile, CI, and Tokio feature changes create merge conflicts if both specs are implemented concurrently.

## Architecture Pattern Evaluation

| Option | Strength | Limitation | Outcome |
| --- | --- | --- | --- |
| Thin adapter over an extended memory store | One persistence owner | Changes the approved adjacent boundary | Rejected by discovery decision |
| Worker-owned ports and adapters | Matches existing worker patterns and selected ownership | Retrieval SQL is an explicit boundary exception | Selected |
| Shared six-operation repository | Strong future reuse | Premature cross-crate restructuring | Deferred |

## Design Decisions
- Adopt `rust-mcp-schema` wire, result, and error types. Own method dispatch, metadata validation, framing, queueing, active-call control, and serialization; do not implement SDK handler/runtime shims.
- Expose five tools: save, combined search, latest retrieval, exact retrieval, and paginated version listing.
- Reuse `MemoryStore` for creation and both underlying searches. Keep exact/latest/version-list SQL in one worker retrieval adapter that reads canonical `memories` rows and never embeddings.
- Use manually completed JSON Schemas plus strict Serde DTOs because SDK macros do not provide strict input or output contracts. Error calls omit `structuredContent`; output schemas describe success only.
- Pass one requested limit to both underlying searches, merge without scores, sort by ID, and cap the union at that limit. Use finite defaults of 4 MiB per input line, 32 queued dispatch items, and 16 active calls; full queues apply pipe backpressure.
- Page version history by descending version with offset `0`, limit `50`, maximum `100`, a JSON-safe maximum offset minus one page, and one extra row to determine the next offset.
- Return structured success content with non-sensitive text summaries. Map domain failures to stable content-safe error codes.

## Risks and Mitigations
- Upstream store implementation drift — gate implementation on the approved memory-store contracts and revalidate on contract changes.
- Split database ownership — restrict worker SQL to three read-only retrieval queries and reuse the same database target and iii client.
- Raw protocol observability — never log frames and assert protected sentinels never reach stderr.
- Runtime task fan-out — acquire an active-call permit before task creation and hold it through serialized response writing; bound the queue ahead of dispatch.
- Schema drift — snapshot every advertised schema and structured result in tests.
- Offset pagination drift — document that newly appended versions can shift subsequent offsets and keep each individual query deterministically ordered.

## Sources
- [MCP 2026-07-28 versioning](https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning)
- [MCP stdio transport](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/stdio)
- [`rust-mcp-sdk` 2.0.0 features](https://docs.rs/crate/rust-mcp-sdk/2.0.0/features)
- [`TransportOptions`](https://docs.rs/rust-mcp-sdk/2.0.0/rust_mcp_sdk/struct.TransportOptions.html)
- [`CallToolResult`](https://docs.rs/rust-mcp-schema/2.0.0/rust_mcp_schema/struct.CallToolResult.html)
- [`rust-mcp-schema` message types](https://docs.rs/rust-mcp-schema/2.0.0/rust_mcp_schema/schema_utils/enum.ClientMessage.html)
- [SDK source at reviewed commit](https://github.com/rust-mcp-stack/rust-mcp-sdk/tree/605bcca8c1dbd040c1b4b0fab4525157d83df86b)

---

## 2026-09-24 Query Embedding Amendment Gap Analysis

### Scope

This amendment removes the query vector from `memory_search` input and makes the
MCP worker generate one vector before running the existing lexical and vector
searches. Stored-memory embedding generation, search ranking, and merge behavior
remain unchanged.

### Current Implementation Delta

- `MemorySearchInput`, its advertised schema, service validation, server tests,
  and the protocol fake require a caller-supplied vector.
- The current service launches BM25 and vector search concurrently after local
  query/vector/limit validation. Query embedding generation and validation must
  complete before either search begins.
- The score-free union, greatest-version selection, ID ordering, result cap,
  memory operations port, `MemoryStore` delegate, retrieval adapter, bounded
  runtime, and output contracts remain reusable.
- The process already shares one `IIIClient` across production adapters. No
  migration, search SQL, memory-store interface, or new external dependency is
  required.
- The repository has no implemented `router::embed` adapter in this worktree.
  The approved `memory-embedding-worker` design defines the authoritative
  request, response, float32 decoding, provider/model identity, cardinality,
  vector validation, timeout, and redaction behavior.

### Requirement-to-Asset Map

| Requirement area | Reusable asset | Gap |
| --- | --- | --- |
| Query-only tool input | Strict Serde DTOs and schema snapshots | Remove `vector`, reject it as unknown, update all callers and fixtures |
| Pre-effect validation | Service validation and canonical vector contracts | Reject NUL queries and validate generated vectors before either search |
| Query embedding | Shared `IIIClient`; approved `router::embed` wire contract | New injectable port, production adapter, provider/model config, timeout, and errors |
| Identity and cardinality | Memory-embedding-worker router contract | Require configured identity and exactly one returned vector |
| Vector compatibility | `EmbeddingVector` float4-exact validation | Decode router numbers as float32, widen exactly, and validate before search |
| Combined search | Existing BM25/vector operations and merge | Insert generation before the unchanged dual-search and merge flow |
| Privacy | Existing stable tool errors and sentinel tests | Protect query, vector, router response, provider/model, and credentials |
| Process verification | Existing fake iii engine and search scenarios | Emulate `router::embed` before two database calls and add generation failures |

### Integration Constraints

- `router::embed` accepts one input array plus explicit provider and model and
  returns provider, model, and positionally ordered float32 vectors.
- Stored embeddings carry no provider/model provenance. Deployment configuration
  can align query and memory generation, but the MCP worker cannot prove that an
  existing corpus uses the configured semantic space.
- Dimension-mismatched stored vectors are excluded by vector search, so a model
  mismatch can appear as sparse semantic results rather than a typed failure.
- Query NUL rejection and complete vector validation must precede both searches;
  deferring validation to `MemoryStore::search_vector` would allow lexical work
  before an invalid generated vector is detected.
- One MCP in-flight permit currently spans a whole request. A router timeout is
  needed so a stalled generation call cannot hold that permit indefinitely.
- `III_NAMESPACE` affects function resolution. The design must state whether
  `router::embed` follows the MCP worker's managed namespace or requires an
  explicit deployment namespace contract.

### Implementation Options

#### Option A: Worker-local query embedding adapter

Add a focused port and iii-backed adapter inside `mcp-worker`, reuse the shared
client, and keep memory operations unchanged. This is the smallest change and
best preserves current boundaries, but duplicates router decoding planned in the
memory embedding worker.

#### Option B: Shared embedding-router contract crate

Extract request/response decoding, identity/cardinality checks, float32 handling,
and opaque errors into a focused crate. This reduces long-term drift but changes
the approved memory-embedding-worker design before two proven consumers exist and
adds cross-spec coordination.

#### Option C: Query function on the memory embedding worker

Add a synchronous query endpoint and call it from MCP. This centralizes routing
configuration but expands another spec that explicitly excludes query embeddings,
adds an extra process hop, and couples interactive search to batch-worker capacity.

### Complexity and Risk

- **Effort**: M (3–7 days). Most search behavior remains intact; router,
  configuration, and process-fake work are new.
- **Risk**: Medium. The wire contract is known, but namespace resolution,
  timeout behavior, and unverifiable corpus/model alignment require explicit
  design commitments.

### Design-Phase Recommendations

- Prefer Option A with a separate `QueryEmbeddingGenerator` port and worker-local
  router adapter; do not expand `MemoryOperations` or the memory embedding worker.
- Reuse the deployment provider/model variable names so both workers have one
  operator-facing semantic-space contract.
- Reuse canonical vector validation after float32 decoding and before launching
  either search.
- Preserve all-or-error behavior, ID-sorted score-free merging, and existing
  active-request bounds; add one finite router-call timeout.
- Resolve namespace behavior and timeout values in design; no external technical
  research remains necessary.

### Design Synthesis

- **Generalization**: Query embedding is one focused service port returning a
  canonical validated vector. The interface can support another generator later,
  but this spec implements only `router::embed`.
- **Build vs. adopt**: Adopt the existing `router::embed` wire contract and
  `memory-store::EmbeddingVector` validation. Build only the MCP-local adapter;
  do not create a shared crate before two merged consumers demonstrate stable
  duplication.
- **Simplification**: Keep `MemoryOperations`, search SQL, result merging, and
  runtime admission unchanged. Do not add a query cache, retry, fallback,
  dedicated router semaphore, or memory-embedding-worker endpoint.

### Design Decisions

- Add a worker-local `QueryEmbeddingGenerator` port and iii router adapter.
- Reuse `TOTAL_RECALL_EMBEDDING_PROVIDER` and
  `TOTAL_RECALL_EMBEDDING_MODEL`; add one MCP-specific positive router timeout.
- Resolve `router::embed` through the MCP process's existing managed iii
  namespace. Deployment must make the router function routable there; the worker
  does not create a second client or force the namespace to `default`.
- Let the existing MCP in-flight permit bound router concurrency. The router
  timeout bounds permit occupancy; no second semaphore is introduced.
- Decode components as float32, widen with `f64::from`, then construct the
  canonical `EmbeddingVector` before either search begins.
- Map remote router failure or timeout to `backend_failure`; map identity,
  cardinality, decoding, or vector-contract violations to `internal_failure`.
- Preserve no-retry, no-fallback, all-or-error search semantics and the existing
  ID-sorted score-free merge.

### Amendment Sources

- [`router::embed` implementation](https://github.com/iii-hq/workers/blob/llm-router/v1.4.27/llm-router/src/embed.rs)
  — request, response identity, vector order, and float32 output.
- [OpenAI embedding provider](https://github.com/iii-hq/workers/blob/provider-openai/v1.2.16/provider-openai/src/embed.rs)
  — provider timeout and input bounds.
- `.kiro/specs/harness-events-cli/research.md` — iii-sdk 0.24.0
  `TriggerRequest` timeout and managed namespace behavior verified against local
  source.

---

## 2026-09-24 Lexical Fallback Amendment

This section supersedes the preceding amendment's no-fallback and all-or-error
generation decisions. BM25 and post-generation vector-search failures remain
all-or-error as described below.

### Scope and Gap

The query-embedding design made provider/model configuration mandatory and
treated every generation failure as a failed search. The revised requirement
makes embedding enrichment optional while retaining BM25 as the required search
path.

### Configuration States

- Both provider and model absent, empty, or whitespace-only disable query
  embedding and permit startup.
- Both non-blank enable query embedding and its timeout validation.
- Exactly one non-blank value is a startup error because intent is ambiguous and
  silent degradation would hide deployment drift.
- Stored vectors still lack provider/model provenance. Enabled deployments retain
  the operator obligation to use the stored-memory provider and model.

### Flow Options

#### Sequential lexical then embedding

Run BM25 first, then attempt generation and vector search. This is simple but adds
lexical latency to every semantically enabled request.

#### Concurrent lexical and generation

Start BM25 and optional query generation together. Generation failure collapses
to the completed BM25 result; valid generation starts vector search before merge.
This preserves bounded work and avoids serializing two independent operations.

#### Lexical fallback for every semantic-path failure

Also hide vector-search failures. This broadens the request beyond embedding
availability and can mask database/search defects, so it is not selected.

### Design Decisions

- Represent embedding configuration as `Disabled` or `Enabled`; do not construct
  a router adapter in disabled mode.
- Start BM25 and enabled generation concurrently after local input validation.
- Treat every generation error or invalid generated vector as lexical fallback,
  not a tool error, and never invoke vector search for that request.
- Decode router components directly as float32 and widen with `f64::from`.
  Accepted values are `float4`-exact by construction, so raw JSON decimal
  precision is not an independent validation failure.
- Keep BM25 mandatory. BM25 failure remains a tool error even if vector search
  could succeed.
- Keep vector-search failure after valid generation as a tool error; fallback is
  limited to embedding configuration and generation.
- Feed lexical-only candidates through the existing score-free ID-sorted merge
  with an empty vector candidate set. No new result shape or fallback indicator
  is introduced.
- Preserve one application-level generation attempt, SDK timeout ownership,
  request bounds, and protected-data rules.
