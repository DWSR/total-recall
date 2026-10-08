# Requirements Document

## Introduction

The Pi startup context feature provides the bounded automatic memory path described in [brief.md](brief.md). It adds one frozen startup snapshot to each loaded Pi session runtime without changing the existing capture contract.

## Boundary Context

- Scope and non-goals are authoritative in the brief's [Scope](brief.md#scope) and [Out of Boundary](brief.md#out-of-boundary) sections.
- **Adjacent expectations**: The approved query-free `memory_context` operation supplies narrow bounded records. This feature does not implement or change memory selection, storage, explicit recall, or outbound capture semantics.
- **Accepted persistence**: Pi may retain startup-memory system-section patches in its local append-only session JSONL. Removing the active section does not erase historical bytes.

## Requirements

### Requirement 1: Opt-In Startup Context
**Objective:** As an operator, I want automatic Pi context enabled through explicit configuration, so that capture remains usable without an implicit retrieval dependency.

#### Acceptance Criteria
1. Where a startup-context worker is configured, the Pi Startup Context shall enable automatic retrieval for each started Pi session runtime.
2. Where no startup-context worker is configured, the Pi Startup Context shall perform no context-process startup or memory lookup.
3. Where no startup-context worker is configured, the Pi Startup Context shall preserve existing Pi capture behavior.
4. When automatic retrieval is enabled, the Pi Startup Context shall request the approved query-free startup-memory operation using only its supported optional result count.
5. The Pi Startup Context shall not derive or send a search query, user prompt, session identity, project identity, working directory, or file path with the startup-memory request.
6. If context configuration is invalid, the Pi Startup Context shall disable automatic retrieval and allow capture to remain active.
7. If the startup-memory operation is incompatible with its approved protocol, schema, or result contract, the Pi Startup Context shall apply no returned context.
8. The Pi Startup Context shall preserve the behavior and schemas of existing agent-directed memory operations.

### Requirement 2: Session-Scoped Frozen Snapshot
**Objective:** As a Pi user, I want one immutable startup snapshot per loaded session runtime, so that ordinary activity cannot churn the model context.

#### Acceptance Criteria
1. When Pi reports `session_start` with a valid explicit session identity, the Pi Startup Context shall begin no more than one startup-memory lookup for that session runtime.
2. When session startup begins a lookup, the Pi Startup Context shall return control to Pi without waiting for the lookup result.
3. While a session lookup is in progress, when an eligible agent start requires context, the Pi Startup Context shall route it to the same lookup and original deadline.
4. When the lookup succeeds with non-empty context that fits every configured bound, the Pi Startup Context shall freeze the exact formatted snapshot for that session runtime.
5. When the lookup succeeds without context, the Pi Startup Context shall freeze an empty snapshot for that session runtime.
6. If startup, protocol, lookup, decoding, formatting, or deadline handling fails, the Pi Startup Context shall freeze an empty snapshot for that session runtime.
7. While a snapshot is frozen, the Pi Startup Context shall ignore backend changes and later lookup opportunities for that session runtime.
8. While a session runtime remains active, the Pi Startup Context shall retain its frozen snapshot without eviction.
9. The Pi Startup Context shall keep new, resumed, forked, reloaded, and independent session-runtime identities in separate state.
10. If session identity is absent, blank, or invalid, the Pi Startup Context shall perform no lookup and shall freeze no non-empty context.
11. The Pi Startup Context shall not retry or refill a failed, empty, timed-out, or oversized snapshot within the same session runtime.
12. When session shutdown begins during a lookup, the Pi Startup Context shall invalidate that lookup.
13. When an invalidated lookup later completes, the Pi Startup Context shall discard its response.

### Requirement 3: Stable Primary Prompt Injection
**Objective:** As a Pi user, I want startup memory represented by one stable prompt section, so that repeated model requests do not receive changing harness-contributed prefixes.

#### Acceptance Criteria
1. When an eligible primary agent run starts with a non-empty frozen snapshot, the Pi Startup Context shall make exactly one reserved startup-memory system section active.
2. When the same session runtime starts another primary agent run, the Pi Startup Context shall request the byte-identical frozen section.
3. While one agent run continues through tool turns, queued continuations, automatic retries, or post-compaction continuation, the Pi Startup Context shall retain the same active section bytes without another lookup.
4. When the requested section already matches Pi's replayed section state, the Pi Startup Context shall cause no changed startup-memory section patch.
5. The Pi Startup Context shall preserve every pre-existing prompt section, system instruction, active tool declaration, and contribution from other extensions.
6. The Pi Startup Context shall not inject startup memory as a user, assistant, custom, tool-result, attachment, or provider-payload message.
7. The Pi Startup Context shall not inject startup memory through Pi's per-call conversation-context hook or complete-system-prompt replacement.
8. When the frozen snapshot is empty, the Pi Startup Context shall make the reserved startup-memory section absent.
9. Where automatic retrieval is disabled or invalid, the Pi Startup Context shall make the reserved startup-memory section absent.

### Requirement 4: Resume, Reload, and Persisted State
**Objective:** As a Pi user, I want runtime transitions to have explicit snapshot behavior, so that persisted prompt state cannot silently masquerade as current retrieval.

#### Acceptance Criteria
1. When Pi resumes a session in a newly loaded extension runtime, the Pi Startup Context shall perform one new runtime-local lookup.
2. When Pi reloads the extension for the active session, the Pi Startup Context shall perform one new runtime-local lookup.
3. When a new runtime freezes the same section bytes already active in replayed state, the Pi Startup Context shall cause no changed startup-memory section patch.
4. When a new runtime freezes different non-empty section bytes, the Pi Startup Context shall transition the reserved section once and keep the replacement stable for that runtime.
5. When a new runtime freezes empty context, the Pi Startup Context shall remove any replayed reserved section before the primary agent request proceeds.
6. Where automatic retrieval becomes disabled or invalid, the Pi Startup Context shall remove any replayed reserved section before the primary agent request proceeds.
7. When Pi persists startup-memory section additions, replacements, or removals, the Pi Startup Context shall leave prior append-only session entries unchanged.
8. The Pi Startup Context shall not claim snapshot stability across process restart, extension reload, or session-runtime replacement.
9. The Pi Startup Context shall not restore a prior snapshot from extension-owned durable state.

### Requirement 5: Bounded Untrusted Context
**Objective:** As an operator, I want deterministic and bounded startup material, so that automatic context cannot create unbounded input or escape its framing.

#### Acceptance Criteria
1. The Pi Startup Context shall accept only memory type, title, content, and concepts from each startup-memory record.
2. The Pi Startup Context shall exclude memory identity, version, files, session identity, observation identity, timestamps, scores, embeddings, backend data, and credentials from the active section.
3. When a non-empty snapshot is formatted, the Pi Startup Context shall label its contents as untrusted JSON reference material rather than user, file, tool, extension, or host-authored instructions.
4. The Pi Startup Context shall render records and fields in deterministic order.
5. The Pi Startup Context shall encode memory values so they cannot terminate or alter the reserved section framing.
6. The Pi Startup Context shall enforce a positive byte bound over the complete host-rendered section, including its section tags, instructions, and serialized records.
7. If the complete section would exceed the injection-byte bound, the Pi Startup Context shall freeze an empty snapshot.
8. The Pi Startup Context shall not truncate an individual field or record to satisfy the injection-byte bound.
9. If an upstream result contains an unknown, missing, malformed, or oversized value, the Pi Startup Context shall freeze an empty snapshot without applying partial context.

### Requirement 6: Fail-Open Resource and Privacy Isolation
**Objective:** As a Pi user, I want retrieval failures isolated from interactive work, so that memory infrastructure cannot break or expose my Pi session.

#### Acceptance Criteria
1. If context configuration, process startup, readiness, protocol, lookup, decoding, formatting, or shutdown fails, the Pi Startup Context shall allow the triggering Pi operation to continue.
2. If a lookup exceeds its deadline, the Pi Startup Context shall release the waiting agent start with an empty snapshot.
3. The Pi Startup Context shall enforce positive finite bounds for startup, lookup, received-frame bytes, pending requests, diagnostics, termination, and shutdown.
4. While one Pi session runtime is active, the Pi Startup Context shall own no more than one live retrieval process.
5. When the session lookup settles, the Pi Startup Context shall close and reap its retrieval process within the configured bound.
6. When Pi reports `session_shutdown`, the Pi Startup Context shall leave no owned retrieval process or pending lookup after the shutdown bound.
7. The Pi Startup Context shall not initiate an unbounded retry, replacement-process loop, durable relay, or background refresh.
8. The Pi Startup Context shall not pass unrelated parent environment variables or provider credentials to the retrieval process.
9. If context behavior emits a diagnostic, the Pi Startup Context shall keep it within the configured byte bound.
10. The Pi Startup Context shall exclude memory fields, protocol payloads, child output, backend messages, prompts, paths, session identities, environment values, credentials, and stack traces from diagnostics.
11. The Pi Startup Context shall isolate context cleanup from capture flushing so that failure in either path does not suppress the other path's cleanup.
12. The Pi Startup Context shall not bundle the retrieval executable in the Pi package.

### Requirement 7: Capture and Auxiliary-Call Isolation
**Objective:** As a memory pipeline operator, I want injected memory excluded from captured evidence and summaries, so that retrieval cannot recursively become new source material.

#### Acceptance Criteria
1. When startup memory becomes active, the Pi Startup Context shall not submit it through session lifecycle or observation capture.
2. When Pi persists a startup-memory system-section patch, the Pi Startup Context shall not classify that patch as user, assistant, or tool evidence.
3. When Pi compacts conversation history, the Pi Startup Context shall not cause startup-memory values to enter the conversation text being summarized.
4. When Pi summarizes an abandoned branch, the Pi Startup Context shall not cause startup-memory values to enter the conversation text being summarized.
5. The Pi Startup Context shall preserve the approved Pi lifecycle, observation selection, projection, ordering, queueing, and delivery contracts.
6. The Pi Startup Context shall preserve explicit agent-directed recall and write behavior.
7. The Pi Startup Context shall not claim project isolation, relevance ranking, semantic safety, durable delivery, or deletion of historical Pi session bytes.

### Requirement 8: Pinned Compatibility and Verification
**Objective:** As a maintainer, I want cache-stability behavior verified against the supported Pi runtime, so that host changes cannot silently reintroduce context churn.

#### Acceptance Criteria
1. The Pi Startup Context shall declare compatibility with `@earendil-works/pi-coding-agent` `0.86.1` on Node 24.
2. The Pi Startup Context shall constrain its Pi peer compatibility to the host versions covered by its verification matrix.
3. When the package quality gate runs, the Pi Startup Context shall verify configuration, formatting, protocol framing, lookup freezing, prompt-section mutation, stale-section removal, failure isolation, and cleanup without live external services.
4. When real-host verification runs, the Pi Startup Context shall load the packed package through Pi's package manifest and native TypeScript loader.
5. When real-host verification issues repeated requests in one session, the Pi Startup Context shall prove one lookup and byte-identical provider-visible startup-memory section state.
6. When real-host verification exercises tool continuations, retries, compaction, resume, reload, empty context, failure, timeout, and changed backend data, the Pi Startup Context shall prove the snapshot and transition behavior defined by this document.
7. When real-host verification inspects capture and session persistence, the Pi Startup Context shall prove that capture excludes startup memory while Pi's append-only system-section history retains native patches.
8. When verification ends, the Pi Startup Context shall prove that no owned lookup, retrieval process, or protected diagnostic value remains.
9. The Pi Startup Context shall expose its declared package and compile gates for ARM macOS, ARM Linux, x86-64 macOS, and x86-64 Linux.
10. The Pi Startup Context shall run its full live process matrix on x86-64 Linux without public network access or external model credentials.
11. If a supported Pi API, system-section replay rule, compaction rule, package pin, or Node major changes, the Pi Startup Context shall require compatibility revalidation before declaring support.
