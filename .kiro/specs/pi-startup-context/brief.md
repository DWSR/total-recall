# Brief: pi-startup-context

## Problem

> Create a spec that does startup injection for Pi similar to how startup injection is done for Opencode. Ensure the same pitfalls re: context churn are avoided.

Pi users need automatic memory context without changing retrieval results on ordinary turns, racing concurrent consumers, refilling failed lookups, or leaking state across sessions. The completed Pi extension captures lifecycle and coarse observations but does not retrieve or inject startup memory.

## Current State

The approved `opencode-startup-context` spec owns a bounded query-free `memory_context` MCP operation and defines the frozen-snapshot invariants. The Pi package has a session lifecycle, a Node child-process boundary for capture, and real-host verification, but no inbound MCP client or context state.

Pi `0.86.1` supports named system-prompt sections. It persists section patches in its append-only session JSONL, replays them across resume and compaction, and emits no new patch when section bytes are unchanged. The user accepted that local persistence as the cost of Pi's cache-stable, composable hook.

## Desired Outcome

Each explicit Pi session runtime performs one bounded startup lookup and freezes exact context or empty state. Every primary agent run requests the same named section, while tool continuations, retries, compaction continuations, and later prompts reuse the frozen bytes without retrieval or prompt-section churn.

## Approach

Extend `@dwsr/pi-harness-events` with an opt-in Node stdio client for the existing `memory_context` contract. Start one session-keyed lookup from `session_start`, share it with the first `before_agent_start` handler, and freeze success, empty, failure, or timeout until `session_shutdown`.

Set one deterministic `total_recall_startup_memory` entry in `systemPromptOptions.sections`; delete that entry for frozen-empty, disabled, or invalid modes. Do not use the `context` hook, custom messages, provider-payload rewriting, or complete system-prompt replacement. Pi owns section persistence, replay, and compaction checkpoints.

## Scope

- **In**: Pi context configuration, one bounded private MCP child per active extension runtime, strict protocol validation, one frozen lookup per explicit session, deterministic section formatting, stale-section removal, fail-open shutdown, package tests, and real Pi provider-visible verification.
- **Out**: The memory database read and `memory_context` server contract, dynamic file or attachment enrichment, live refresh, cross-runtime snapshot persistence, project filtering, outbound capture changes, Pi host patches, and full-prompt replacement.

## Boundary Candidates

- Pi-specific context configuration and sanitized child environment.
- Node stdio transport for the existing private MCP protocol.
- Session-keyed lookup, freeze, invalidation, and shutdown state.
- Deterministic named-section formatting and structural escaping.
- Real-host cache-stability and persistence verification.

## Out of Boundary

- Implementing or changing the latest-current-memory query, MCP tool schema, result fields, or backend limits owned by `opencode-startup-context`.
- Injecting startup memory through conversation messages, tool results, the `context` event, or provider payload hooks.
- Removing historical section bytes from Pi's append-only session file after a later null patch.
- Preventing other extensions, Pi configuration, tool selection, or context files from changing other prompt sections.
- Hard tenant, project, repository, or worktree isolation; all memory readers, writers, session files, and model providers remain in one trusted privacy domain.
- Support beyond Node 24 and the repository's declared ARM macOS, ARM Linux, x86-64 macOS, and x86-64 Linux systems.

## Upstream / Downstream

- **Upstream**: The `memory_context` tool and bounds owned by `opencode-startup-context`; the completed `pi-harness-events-extension`; Pi `0.86.1`; Node 24; and the existing Pi E2E harness.
- **Downstream**: Project-scoped startup selection or dynamic enrichment may build on the frozen section boundary without changing this snapshot lifecycle.

## Existing Spec Touchpoints

- **Extends**: `pi-harness-events-extension` with an independent inbound context plane and coordinated session shutdown.
- **Adjacent**: `opencode-startup-context` owns shared backend capability and analogous invariants. Its OpenCode package implementation is not imported by Pi.

## Constraints

- Session identity keys local state but is not sent to the query-free `memory_context` tool.
- Success, empty results, failures, and timeouts freeze once; no background refill or ordinary-turn refresh is allowed.
- The final host-rendered section, including tags, instructions, and JSON, has a hard UTF-8 byte bound.
- Memory values are untrusted data and cannot emit raw section delimiters.
- Disabled, invalid, and frozen-empty modes delete the reserved section on every `before_agent_start`, retracting stale replayed state without erasing historical JSONL bytes.
- `session_shutdown` independently bounds lookup cancellation, child termination, stream draining, and reaping without delaying Pi indefinitely.
- Exact Pi hook, replay, compaction, package, and Node pins require compatibility revalidation before support changes.
