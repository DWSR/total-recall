# Research and Gap Analysis: pi-startup-context

## Summary

- This is brownfield work across the implemented Pi extension, a private MCP subprocess, and real-host verification.
- Pi `0.86.1` named system sections are the lowest-churn supported injection point: unchanged bytes produce no later patch.
- Named sections persist in Pi's append-only session JSONL. The user accepted that behavior in the brief.
- The query-free `memory_context` backend is approved but unimplemented under `opencode-startup-context`; Pi live integration depends on its tasks 1–2.
- Pi-side effort is **L** with **high risk** because prompt replay, async process races, and live lifecycle tests cross several boundaries.

## Validated Pi Host Behavior

- `before_agent_start` exposes mutable named sections; Pi wraps each section in matching tags and validates its name. [system-prompt.ts](https://github.com/earendil-works/pi/blob/v0.86.1/packages/coding-agent/src/core/system-prompt.ts)
- Pi diffs desired sections against replayed transcript state. Equal text emits no patch; changed text emits one system patch; deletion emits a null patch. [agent-session.ts](https://github.com/earendil-works/pi/blob/v0.86.1/packages/coding-agent/src/core/agent-session.ts)
- Tool turns, queued continuations, retries, and post-compaction continuations reuse the active run's prompt options; `before_agent_start` is not a per-provider-call hook. [extensions.md](https://github.com/earendil-works/pi/blob/v0.86.1/packages/coding-agent/docs/extensions.md)
- Session replacement and reload shut down the old extension runtime before binding a new one. Initial CLI resume may still report `session_start.reason === "startup"`; session identity must own state.
- Native compaction excludes system messages from summary conversation text and checkpoints effective system state separately. Branch serialization likewise excludes system-section contents. [compaction.ts](https://github.com/earendil-works/pi/blob/v0.86.1/packages/coding-agent/src/core/compaction/compaction.ts), [branch-summarization.ts](https://github.com/earendil-works/pi/blob/v0.86.1/packages/coding-agent/src/core/compaction/branch-summarization.ts)
- Pi awaits extension handlers and supplies no handler timeout. The extension must return from `session_start` without awaiting retrieval and enforce its own lookup and shutdown bounds.
- Pi `0.86.1` and this repository's package are MIT-compatible. The package already pins Pi `0.86.1` for development and Node 24 for runtime verification. `integrations/pi-harness-events/package.json:5-8,29-36`

## Requirement-to-Asset Map

| Requirements | Reusable asset | Gap |
| --- | --- | --- |
| R1 opt-in lifecycle | Pure capture config parser | Separate disabled/invalid/enabled context config, strict worker compatibility, sanitized environment |
| R2 frozen snapshot | Explicit Pi session identity and shutdown hooks | Eager shared promise, absolute deadline, text/empty freeze, invalidation token |
| R3 stable section | `before_agent_start` is capture-excluded | Hook registration, reserved section set/delete, unchanged-patch proof |
| R4 resume and persistence | Pi replacement lifecycle and JSONL replay | New-runtime lookup, stale section removal, persisted patch inspection |
| R5 bounded context | Existing strict JSON normalization patterns | Four-field DTO, exact framing, delimiter escaping, complete-section byte bound |
| R6 resource isolation | No-shell process and TERM/KILL test patterns | New newline protocol client, strict IDs/frames, one-shot child cleanup, allowlisted environment |
| R7 feedback isolation | Capture excludes system/provider hooks and compaction text | Regression proof for system patches, compaction, branches, and observations |
| R8 verification | npm gate, packed Jiti artifact, Cargo Pi E2E | Provider-visible recorder, real worker/database route, persisted sessions, no-egress matrix |

## Existing Integration Points

- `integrations/pi-harness-events/src/index.ts:22-58,130-220` provides injectable composition, session start, subscription release, and shutdown seams.
- `integrations/pi-harness-events/src/adapter.ts:656-711,730-768` excludes context-bearing hooks and reads Pi's explicit session identity.
- `integrations/pi-harness-events/src/config.ts:31-113` demonstrates pure environment parsing, but context needs a separate tri-state contract.
- `integrations/pi-harness-events/src/dispatcher.ts:197-217,222-579` provides process-test patterns, not a reusable MCP transport; it is one-shot and inherits the parent environment.
- `integration/pi-harness-events-e2e/src/artifacts.rs:123-157`, `engine.rs:57-132`, and `process.rs` provide explicit artifacts, semantic readiness, and bounded process groups.
- The current live scenario uses `--no-session`, its provider ignores system-prompt bytes, and its engine registers only capture functions. `integration/pi-harness-events-e2e/src/pi.rs:391-420`, `integrations/pi-harness-events/test/fixtures/scripted-provider.ts:59-121`, `integration/pi-harness-events-e2e/src/engine.rs:28-32`
- `workers/mcp-worker/src/server.rs:170-235` currently advertises five tools. `crates/memory-store/migrations/0001_memory_schema_search.sql:41-96` lacks context recency state and its top-N index.

## Organization Options

| Option | Strength | Cost / risk |
| --- | --- | --- |
| Extend `index.ts` and capture dispatcher | Fewest files | Couples inbound retrieval to outbound capture and overloads existing lifecycle code |
| Add Pi-only context modules in the existing package | Preserves one installable extension and isolates config, transport, formatting, and state | Duplicates a small approved wire contract and adds several focused modules |
| Extract a shared OpenCode/Pi TypeScript package | Could centralize protocol and formatting concepts | Bun versus Node/Jiti, different package managers, child lifetimes, imports, and framing make the abstraction premature |

Pi-only modules are the strongest design input. Shared extraction should wait until both host clients exist and expose a stable runtime-neutral seam.

## Lifecycle and Protocol Hazards

- Disabled, invalid, failed, empty, and timed-out modes must still delete the reserved section during every primary start; doing nothing leaves replayed context active.
- The first eligible agent start must await only the session-start promise's original deadline. A new promise or deadline would permit hidden refill.
- Shutdown or textual ID reuse needs an object-identity token so late completion cannot commit into replacement state.
- Pi renders `<name>\nvalue\n</name>`. The byte cap must include that wrapper, and record encoding must prevent raw closing delimiters.
- The private worker protocol must handle fragmented/coalesced LF frames, CR, malformed UTF-8/JSON, partial EOF, wrong IDs, schema drift, write backpressure, and output limits.
- One Pi runtime owns one session lookup, so the retrieval child can close after that lookup. OpenCode's persistent multi-session child and restart generation are not directly reusable.
- Context and capture can each own a child. Shutdown must settle both independently; one failure cannot suppress the other's cleanup.
- Capture children currently inherit `process.env`. Context variables may reach `harness-events`; changing that behavior remains outside this feature's outbound-capture boundary.

## Dependency and Verification Sequence

1. Land `opencode-startup-context` tasks 1.1–2.3: recency migrations, narrow context read, and the sixth MCP tool.
2. Develop Pi configuration, formatter, protocol, and snapshot state against strict fakes after the tool schema freezes.
3. Extend the Pi engine harness with deterministic database execution and lookup counts, then add provider-visible and JSONL reports.
4. Add the loopback-only live matrix, package/compile outputs for all four repository systems, and x86-64 Linux CI execution.

## Design Synthesis

- **Generalization**: Configuration, transport, formatting, and frozen state form one Pi context plane independent of capture. The interfaces remain context-record oriented; they do not generalize into a cross-harness SDK.
- **Adopt**: Use Pi's named prompt-section diff and replay contract rather than replacing the full prompt or injecting conversation messages.
- **Build**: Use Node 24 child-process, stream, `TextDecoder`, and `TextEncoder` APIs for the private protocol. Standard MCP clients do not match the worker's metadata-bearing, handshake-free contract.
- **Simplify**: One loaded Pi runtime owns one session and one lookup. The protocol client permits one outstanding request, performs no restart, and closes after lookup instead of copying OpenCode's multi-session persistent child.
- **Verification**: Drive persisted multi-step scenarios through Pi RPC mode; use a separate scripted provider/driver fixture so the existing capture fixture remains deterministic and I/O-free.

## Selected Architecture Inputs

- Add Pi-only context modules inside `@dwsr/pi-harness-events`; retain one source package and no production dependency.
- Reserve `total_recall_startup_memory`; active text sets only that section and empty state deletes it.
- Treat a repeated start for the same session identity as idempotent. Treat a different identity in one live extension instance as a host-lifecycle conflict that freezes empty and starts no second child.
- Use a complete tagged-section byte bound and JSON escaping for `<`, `>`, `&`, U+2028, and U+2029.
- Make operational rollback configuration-only. A downgrade that removes the cleanup-aware handler can leave a prior section active in replayed session state.

## Additional References

- [Pi RPC mode v0.86.1](https://github.com/earendil-works/pi/blob/v0.86.1/packages/coding-agent/docs/rpc.md) — persisted multi-prompt, compaction, and session controls.
- [Node 24 child processes](https://nodejs.org/docs/latest-v24.x/api/child_process.html) — direct spawn, explicit environments, piped stream backpressure, and close lifecycle.
- [Node 24 TextDecoder and TextEncoder](https://nodejs.org/docs/latest-v24.x/api/util.html#class-utiltextdecoder) — fatal UTF-8 validation and exact byte measurement.
