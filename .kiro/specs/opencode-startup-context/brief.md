# Brief: opencode-startup-context

## Problem

> Create a spec that implements context injection for the OpenCode extension. Use the notes in the harness handoff doc in the repo root to work around some problems with agentmemory's context injection.

OpenCode users need automatic memory context without AgentMemory's dynamic system-prefix churn, one-shot context races, duplicate concurrent retrieval, or cross-session state leakage. Total Recall currently captures OpenCode activity and exposes agent-directed memory tools, but it has no bounded startup-context operation or automatic injection path.

## Current State

The completed OpenCode extension submits outbound lifecycle and observation events. The MCP worker exposes query-driven memory search but no query-free startup-context operation. Its current result shape includes provenance fields that should not be elevated into a system prompt.

The pinned OpenCode v1 API provides an experimental system transform without a primary-versus-auxiliary request discriminator. OpenCode v2 provides a primary context hook. Neither adapter currently registers these hooks.

## Desired Outcome

Each explicit OpenCode session receives one bounded startup memory snapshot from Total Recall. Concurrent consumers share one lookup, and success, empty results, or failure freeze the session snapshot for the loaded plugin lifetime. Every eligible request receives identical harness-contributed system content, preserving provider prompt-prefix stability.

## Approach

Add a dedicated MCP startup-context tool that returns a narrow, byte-bounded list of the most recently updated current memory versions in deterministic order. The OpenCode extension owns at most one live configured MCP worker child per loaded plugin instance, permits one bounded replacement generation after transient failure, and performs one shared lookup per session.

OpenCode v1 emits the same frozen snapshot on every explicit-session system transform, including auxiliary transforms because the pinned API cannot distinguish them. OpenCode v2 emits it through the primary context hook. Retrieval and child failures freeze an empty snapshot and never fail the host operation.

This approach keeps dynamic data out of the changing system prefix, avoids prompt-derived search, and stays within the repository's local stdio boundary.

## Scope

- **In**: A bounded latest-current-memory read, a sixth MCP tool with a narrow context result, deterministic context formatting, at most one live child per plugin instance, one bounded replacement generation, session-keyed in-flight sharing and freezing, v1/v2 system injection, fail-open lifecycle management, package verification, and real-host model-request tests.
- **Out**: File/tool enrichment, attachment enrichment, context refresh, backend summarization, direct AgentMemory integration, project or worktree filtering, cross-process snapshot persistence, binary bundling, and changes to outbound event semantics.

## Boundary Candidates

- Latest-current-memory selection and bounded context DTO in the memory domain.
- MCP startup-context contract and iii database adaptation.
- Persistent child protocol, process lifecycle, and fail-open retrieval in the OpenCode package.
- Per-session snapshot ownership and version-specific host injection.
- Real-host startup-context verification with an offline model boundary.

## Out of Boundary

- Dynamic enrichment of persisted tool results or incoming attachments.
- Capture provenance filtering, because this feature injects only request-local system context.
- Hard project, repository, or worktree isolation; all corpus writers, readers, and model providers belong to one trusted privacy domain.
- Relevance ranking claims, prompt-derived retrieval, context synthesis, or live snapshot updates.
- Changes to explicit agent-directed recall and write tools.
- Support beyond the repository's declared ARM macOS, ARM Linux, and x86-64 Linux systems.

## Upstream / Downstream

- **Upstream**: Immutable current memory heads from `memory-schema-search`, the `mcp-worker` stdio runtime, the completed `opencode-harness-events-plugin`, and pinned OpenCode v1 `1.18.29` and v2 `2.0.10` hooks.
- **Downstream**: Optional project-scoped retrieval, curated startup context, and provenance-safe file or attachment enrichment can build on the frozen snapshot boundary.

## Existing Spec Touchpoints

- **Extends**: `memory-schema-search` with a read-only latest-current operation; `mcp-worker` with the startup-context tool; `opencode-harness-events-plugin` with inbound context retrieval and injection.
- **Adjacent**: `session-post-processing-worker` remains the producer of canonical memories. `harness-events-cli`, ingestion, persistence, embedding, and outbound capture behavior remain unchanged.

## Constraints

- Context rows and serialized output require independent hard bounds; a row limit alone is insufficient.
- Recent ordering uses current memory `updated_at`, then deterministic identity/version tie-breakers; it does not imply relevance.
- Context output excludes file, session, observation, score, embedding, backend, and credential data.
- The extension uses an allowlisted child environment and an externally configured worker binary.
- Lookup timeout, late responses, malformed frames, worker exit, and startup races are fail-open and produce bounded payload-free diagnostics.
- Frozen session state remains resident until session deletion or plugin disposal; eviction cannot silently change an active session's prefix.
- OpenCode v1 receives identical context on every explicit-session system transform. Current title and summary agents may remain disabled operationally, but correctness does not depend on one-shot injection.
- Exact pinned host APIs must be revalidated before any OpenCode dependency upgrade.
