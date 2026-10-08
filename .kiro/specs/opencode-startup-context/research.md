# Research and Gap Analysis: opencode-startup-context

## Summary

- **Discovery scope**: Complex brownfield integration across memory storage, MCP stdio, Bun process management, and pinned OpenCode hooks.
- `memory_search_heads` identifies greatest versions but lacks recency data and an ordered index. `crates/memory-store/migrations/0001_memory_schema_search.sql:41-91`
- The MCP worker has five tools and a bounded newline input protocol, but no query-free context result, output bound, or backend readiness probe. `workers/mcp-worker/src/server.rs:190-235`, `workers/mcp-worker/README.md:18-27`
- The OpenCode package has capture session state and bounded one-shot children, but no persistent protocol client, frozen inbound snapshot, or model-request E2E path.

## Requirement-to-Asset Map

| Requirement | Reusable asset | Gap |
| --- | --- | --- |
| R1 latest-current selection | Greatest-version head projection | Ordered query and recency access path |
| R2 bounded narrow result | Canonical validation and strict decoders | Four-field DTO and compact-byte prefix |
| R3 `memory_context` | Explicit registry and typed dispatch | Sixth tool, service, database operation |
| R4 opt-in lifecycle | Pure config and child cleanup patterns | Persistent client, allowlist, readiness |
| R5 frozen snapshots | Session maps and teardown callbacks | Shared promise, frozen text, tombstones |
| R6 stable injection | Mutable generation-specific host hooks | Formatter and request-only insertion |
| R7 pinned hosts | Exact pins and isolated launchers | System/context hooks and model interception |
| R8 fail-open bounds | Input/concurrency limits and opaque errors | Output cap, pending map, deadlines, exit fan-out |

## Key Investigations

### Data and Query

An indexed read can select heads by `updated_at DESC`, identity `ASC` under bytewise collation, and version `DESC`, then join only the bounded head prefix to canonical memories. A matching B-tree avoids corpus-wide sorting for `ORDER BY ... LIMIT`; PostgreSQL documents this as the primary top-N index case. Historical timestamps cannot choose the head because version remains authoritative.

The existing head projection should gain recency rather than adding another table. A forward migration must backfill from the exact `(id, version)` canonical row and update recency only when a greater version wins. Deployed `0001` remains unchanged.

### Result and Protocol Bounds

Canonical text and concepts have no storage length cap. The database path needs an oversized-candidate sentinel so a single record cannot cross the iii response unbounded; the MCP service then enforces the exact compact structured-content prefix. Separate caps cover structured content, the newline response frame, and final system text.

The worker protocol is handshake-free: clients send metadata-bearing `server/discover`, `tools/list`, and `tools/call`; standard MCP `initialize` is unsupported. `workers/mcp-worker/src/server.rs:190-277`

Discovery proves protocol compatibility, not iii or database availability. The new context call needs one absolute operation deadline and an explicit database invocation timeout; no synthetic probe is required.

### Host Hooks and State

OpenCode v1 `1.18.29` exposes an experimental system transform with optional session ID and no primary/auxiliary discriminator. OpenCode v2 `2.0.10` exposes primary `context` separately from title, compaction, and generation. The final rendered string is frozen per explicit session; deletion or disposal invalidates pending work and removes state.

A separate context runtime preserves outbound capture behavior. A plugin-instance coordinator can order capture and context teardown without combining their state machines.

### Verification

Current OpenCode E2E creates and deletes sessions without a model request. The Pi harness provides the offline scripted-provider precedent. PostgreSQL 17/18, database protocol, MCP release-process, package, and real-host tests all require targeted extension.

Both pinned hosts can use bundled OpenAI-compatible providers at one `http://127.0.0.1:<port>/v1` base URL. They send bearer-authenticated streaming requests to `/v1/chat/completions`; v2 requires an explicit session wait after prompt admission. A loopback recorder is feasible, while no-egress proof requires a sandboxed network namespace rather than recorder assertions alone.

The iii SDK cleans pending invocations through the trigger future's terminal path; existing MCP search deliberately avoids dropping such futures early. `workers/mcp-worker/src/service.rs:251-253`

PostgreSQL documents that failed concurrent builds can leave invalid indexes and recommends dropping the invalid index before retry. Bun exposes explicit child environments, piped streams, exit promises, and signals; pinned-runtime tests must prove ambient variables are absent from the child.

## Architecture Pattern Evaluation

| Option | Strength | Risk / limitation |
| --- | --- | --- |
| Worker-local scan and capture-runtime extension | Fewest files | Domain drift, capture coupling, corpus sort |
| Memory-store read and separate context runtime | Clear ownership and isolated failures | Recency performance still unresolved |
| Indexed head read and separate context runtime | Bounded top-N path and clean boundaries | Forward migration and rollout work |

## Risks and Mitigations

| Risk | Mitigation |
| --- | --- |
| Cross-project disclosure or memory prompt injection | Opt-in shared trust domain, narrow fields, untrusted-data label, no safety claim |
| Migration lock or partial rollout | Split transactional recency from concurrent index; validate both PG majors before reader rollout |
| Startup race | Eager discovery, bounded first call, frozen-empty failure, later sessions may use the healthy child |
| Child leaks hidden by process-group cleanup | Record and verify child PID before fallback group termination |
| Experimental v1 hook drift | Exact pin and mandatory compatibility revalidation |

## Validation Carried into Design

- Verify the indexed plan and top-N behavior on populated PostgreSQL 17 and 18 fixtures.
- Freeze exact tool schemas, protocol metadata, response envelopes, formatter bytes, and environment allowlist.
- Cover fragmented, coalesced, oversized, malformed, partial, late, and out-of-order protocol responses.
- Exercise concurrent same-session hooks, independent sessions, deletion during lookup, ID reuse, and disposal.
- Record provider-visible system bytes for repeated v1 and v2 model requests with an offline provider.

## References

- [PostgreSQL 18: Indexes and ORDER BY](https://www.postgresql.org/docs/18/indexes-ordering.html)
- [PostgreSQL 18: CREATE INDEX](https://www.postgresql.org/docs/18/sql-createindex.html)
- [Bun child processes](https://bun.sh/docs/runtime/child-process)
- [OpenCode v1.18.29 plugin hooks](https://github.com/anomalyco/opencode/blob/v1.18.29/packages/plugin/src/index.ts)
- [OpenCode v2.0.10 session hooks](https://github.com/anomalyco/opencode/blob/v2.0.10/packages/plugin/src/promise/session.ts)
- [OpenCode v1 compatible-provider fixture](https://github.com/anomalyco/opencode/blob/v1.18.29/packages/opencode/test/lib/test-provider.ts)
- [OpenCode v2 compatible provider](https://github.com/anomalyco/opencode/blob/v2.0.10/packages/ai/src/providers/openai-compatible.ts)
