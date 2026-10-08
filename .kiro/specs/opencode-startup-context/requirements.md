# Requirements Document

## Introduction

The OpenCode startup context feature provides the bounded automatic memory path described in [brief.md](brief.md). It adds one query-free context operation and freezes one fail-open snapshot for each explicit session in a loaded OpenCode plugin instance.

## Boundary Context

- Scope and non-goals are authoritative in the brief's [Scope](brief.md#scope) and [Out of Boundary](brief.md#out-of-boundary) sections.
- **Adjacent expectations**: Canonical memory production, explicit agent recall, embeddings, capture delivery, and persistence retain their existing contracts. All memory writers, readers, and model providers operate within one trusted privacy domain.

## Requirements

### Requirement 1: Deterministic Latest-Current Selection
**Objective:** As an OpenCode user, I want startup context drawn from current memories without composing a search query, so that automatic recall is predictable at session start.

#### Acceptance Criteria
1. The OpenCode Startup Context shall consider the greatest version for every memory identity as that identity's current memory.
2. The OpenCode Startup Context shall order current memories by `updated_at` descending, memory identity ascending, and version descending.
3. When a result count is omitted, the OpenCode Startup Context shall use the configured default record count.
4. When a valid result count is provided, the OpenCode Startup Context shall consider the first `min(requested, available)` ordered memories as result candidates.
5. If no current memories exist, the OpenCode Startup Context shall return an empty successful result.
6. The OpenCode Startup Context shall not perform lexical search, vector search, relevance ranking, project filtering, or content synthesis.

### Requirement 2: Bounded and Narrow Context Results
**Objective:** As an operator, I want startup context constrained to useful memory fields and hard resource limits, so that automatic injection cannot create unbounded model input or expose provenance.

#### Acceptance Criteria
1. The OpenCode Startup Context shall expose only memory type, title, content, and concepts for each selected memory.
2. The OpenCode Startup Context shall exclude memory identity, version, files, session identity, observation identity, timestamps, scores, embeddings, backend data, and credentials from successful results.
3. The OpenCode Startup Context shall enforce positive configured default and maximum record counts and a positive complete serialized result-byte bound.
4. If the result-byte bound cannot contain an empty serialized result, the OpenCode Startup Context shall reject the configuration before memory access.
5. When adding the next complete memory would exceed the result-byte bound, the OpenCode Startup Context shall omit that memory and every later memory.
6. The OpenCode Startup Context shall not truncate an individual memory field into a partial record.
7. If the first selected memory cannot fit within the result-byte bound, the OpenCode Startup Context shall return an empty successful result.
8. If a requested count is invalid or exceeds the configured maximum record count, the OpenCode Startup Context shall reject the request before memory access.

### Requirement 3: Query-Free MCP Operation
**Objective:** As a harness integrator, I want one explicit startup-context operation, so that automatic injection does not repurpose agent-directed search semantics.

#### Acceptance Criteria
1. The MCP worker shall advertise a `memory_context` tool alongside its existing tools.
2. The `memory_context` tool shall accept only an optional positive result count within the configured maximum record count.
3. If `memory_context` receives unknown fields or a query, the MCP worker shall reject the request before memory access.
4. If latest-current-memory retrieval fails, the MCP worker shall return no partial context.
5. If latest-current-memory retrieval fails, the MCP worker shall exclude memory fields, protocol payloads, backend messages, paths, credentials, and stack traces from the operation error.
6. The MCP worker shall preserve the schemas and behavior of every existing memory tool.

### Requirement 4: Opt-In Retrieval Lifecycle
**Objective:** As an operator, I want automatic context enabled only through bounded explicit configuration, so that existing capture remains usable without hidden runtime dependencies.

#### Acceptance Criteria
1. Where a startup-context worker executable is configured, the OpenCode Startup Context shall enable automatic retrieval.
2. While automatic retrieval is enabled, the OpenCode Startup Context shall own no more than one live retrieval process at a time per loaded plugin instance.
3. While the retrieval process remains available, the OpenCode Startup Context shall reuse it across session lookups.
4. Where no startup-context worker executable is configured, the OpenCode Startup Context shall perform no context retrieval.
5. Where no startup-context worker executable is configured, the OpenCode Startup Context shall preserve existing OpenCode capture behavior.
6. If context configuration is invalid, the OpenCode Startup Context shall disable automatic retrieval.
7. The OpenCode Startup Context shall bound retrieval startup and readiness waits by the configured startup deadline.
8. The OpenCode Startup Context shall not pass parent environment variables outside the documented child-environment allowlist.
9. When the plugin unloads, the OpenCode Startup Context shall leave no owned retrieval process or pending lookup after the shutdown bound.
10. The OpenCode Startup Context shall not bundle the retrieval executable in the OpenCode package.

### Requirement 5: Session-Scoped Frozen Snapshots
**Objective:** As an OpenCode user, I want one stable memory snapshot per session, so that repeated requests preserve prompt-prefix stability and concurrent sessions do not exchange context.

#### Acceptance Criteria
1. The OpenCode Startup Context shall treat OpenCode v1 system transforms and OpenCode v2 primary context hooks as context-capable host callbacks.
2. When the first eligible hook for a session requires context, the OpenCode Startup Context shall begin no more than one startup-context lookup for that session in the loaded plugin instance.
3. While a session lookup is in progress, the OpenCode Startup Context shall route every concurrent eligible hook for that session to the same lookup.
4. When a session lookup succeeds with context that fits the injection-byte bound, the OpenCode Startup Context shall freeze the exact formatted snapshot for that session.
5. When a session lookup succeeds without context, the OpenCode Startup Context shall freeze an empty snapshot for that session.
6. If a session lookup fails or reaches its deadline, the OpenCode Startup Context shall freeze an empty snapshot for that session.
7. While a session snapshot is frozen, the OpenCode Startup Context shall ignore backend changes and later lookup opportunities for that snapshot.
8. While a session remains active, the OpenCode Startup Context shall retain its frozen snapshot without eviction.
9. The OpenCode Startup Context shall keep parent, child, resumed, and independent session identities in separate snapshot state.
10. If a context-capable host callback lacks an explicit session identity, the OpenCode Startup Context shall perform no lookup.
11. If a context-capable host callback lacks an explicit session identity, the OpenCode Startup Context shall inject no context.
12. When a session is deleted during a lookup, the OpenCode Startup Context shall invalidate the pending lookup for that session.
13. When an invalidated lookup later completes, the OpenCode Startup Context shall discard its response.
14. When a session is deleted, the OpenCode Startup Context shall remove its frozen snapshot state.
15. When the plugin unloads, the OpenCode Startup Context shall remove all snapshot state owned by that plugin instance.

### Requirement 6: Stable and Clearly Labeled Injection
**Objective:** As an OpenCode user, I want remembered material presented as stable reference context, so that it does not masquerade as host instructions or mutate conversation history.

#### Acceptance Criteria
1. When a non-empty snapshot is frozen, the OpenCode Startup Context shall render it with fixed harness instructions and deterministic framing.
2. The OpenCode Startup Context shall label injected memory as reference material rather than user, file, tool, or host-authored instructions.
3. The OpenCode Startup Context shall prevent memory field values from terminating or altering startup-context framing.
4. The OpenCode Startup Context shall preserve all pre-existing system content unchanged.
5. When the same frozen snapshot is injected again, the OpenCode Startup Context shall contribute byte-identical system content.
6. When a session has an empty frozen snapshot, the OpenCode Startup Context shall contribute no startup-memory system content.
7. If formatted startup content would exceed the injection-byte bound, the OpenCode Startup Context shall freeze an empty snapshot.
8. The OpenCode Startup Context shall keep complete contributed system content within the configured injection-byte bound.
9. The OpenCode Startup Context shall not persist startup context as a user message, assistant message, tool result, attachment, or outbound harness observation.

### Requirement 7: Pinned OpenCode Generation Behavior
**Objective:** As a maintainer, I want explicit behavior for both supported OpenCode generations, so that host API differences do not reintroduce one-shot or unstable injection.

#### Acceptance Criteria
1. When OpenCode v1 `1.18.29` invokes any system transform with an explicit session identity, the OpenCode Startup Context shall contribute that session's frozen snapshot.
2. When OpenCode v2 `2.0.10` invokes its primary context hook with an explicit session identity, the OpenCode Startup Context shall contribute that session's frozen snapshot.
3. The OpenCode Startup Context shall not inject through OpenCode v2 title, summary, compaction, or generation hooks.
4. When either supported generation captures lifecycle, prompt, message, tool, status, or error events, the OpenCode Startup Context shall preserve the approved outbound capture contract.
5. If a supported OpenCode API version changes, the OpenCode Startup Context shall require host-hook compatibility revalidation before the new version is declared supported.
6. The OpenCode Startup Context shall declare support only on ARM macOS, ARM Linux, and x86-64 Linux.

### Requirement 8: Fail-Open Privacy and Resource Isolation
**Objective:** As an OpenCode user, I want memory failures isolated from primary agent work, so that context retrieval cannot break or expose my interaction.

#### Acceptance Criteria
1. If context configuration is invalid, the OpenCode Startup Context shall allow OpenCode operation to continue.
2. If context configuration is invalid, the OpenCode Startup Context shall emit one diagnostic within the configured diagnostic-byte bound or a fixed `512`-byte fallback when that bound is invalid.
3. If process startup, readiness, protocol, retrieval, decoding, formatting, or shutdown fails, the OpenCode Startup Context shall allow the triggering OpenCode operation to continue.
4. If a lookup exceeds its deadline, the OpenCode Startup Context shall release the waiting host request.
5. If a timed-out lookup later completes, the OpenCode Startup Context shall not apply its response to that session.
6. If the retrieval process exits with lookups pending, the OpenCode Startup Context shall settle those lookups as failures.
7. If the retrieval process exits with a partial frame, the OpenCode Startup Context shall apply no partial context.
8. The OpenCode Startup Context shall not initiate a second lookup for a session whose empty or failed snapshot is frozen.
9. The OpenCode Startup Context shall require positive finite configuration for maximum pending requests, received-frame bytes, diagnostic bytes, lookup duration, startup duration, and shutdown duration.
10. The OpenCode Startup Context shall not exceed the configured pending-request, frame, diagnostic, lookup, startup, or shutdown bounds.
11. The OpenCode Startup Context shall exclude memory fields, protocol payloads, child output, backend messages, paths, environment values, credentials, and stack traces from diagnostics.
12. The OpenCode Startup Context shall not claim project isolation, relevance, backend persistence, or semantic safety for injected memory.
