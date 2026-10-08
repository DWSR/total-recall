# Brief: mcp-worker

## Problem
Agent harnesses need an MCP-compatible way to search, retrieve, and save memories. The project has planned memory persistence and search operations, but no MCP server or harness-facing memory tools.

## Current State
The approved `memory-schema-search` spec owns canonical records, immutable versions, embedding association, and lexical/vector search. It does not define an MCP transport, exact/latest retrieval, or version listing. The repository has no MCP dependency or server implementation.

## Desired Outcome
Harnesses can launch one local worker over stdio and call typed MCP tools to search memories from a text query through one lexical-plus-vector operation, retrieve an exact or latest memory version, page through a memory's version history, and create a memory from a title, content, and current session ID.

## Approach
Create a Rust worker using `rust-mcp-schema` 2.0.0 wire/result types behind a worker-owned bounded stdio dispatcher. When query embedding is configured, the worker attempts to generate a vector and supplies the text and valid vector to the existing lexical and vector searches. When embedding is not configured or generation fails, the worker returns lexical-search results without attempting vector search. It assigns the ID, version 1, timestamps, and type `unclassified` for created memories; enrichment collections remain hidden from the tool and start empty. Keep MCP lifecycle, tool schemas, result mapping, create defaults, optional query embedding, lexical fallback, and retrieval behavior in this feature. Reuse the memory store's canonical contract and existing save/search operations rather than duplicating their validation or ranking rules.

## Scope
- **In**: Stdio MCP server lifecycle; bounded request framing and channel capacity; stateless server and tool discovery; optional query embedding generation; lexical-only fallback when embedding is unavailable; combined lexical/vector search with deterministic score-free merging and a 50-result cap; latest-by-ID retrieval; exact ID/version retrieval; newest-first offset pagination over version history; create-only memory save from title, content, and current session ID; worker-assigned ID, version 1, timestamps, and initial type; typed results; content-safe errors; focused automated verification.
- **Out**: Streamable HTTP or SSE; authentication; memory updates; stored-memory embedding persistence or generation; hybrid relevance scoring; deletion; retention; search pagination; additional search filters; database provisioning; harness-side MCP configuration outside the documented launch contract.

## Boundary Candidates
- MCP transport and protocol lifecycle.
- Harness-facing tool contracts and application mapping.
- Exact/latest retrieval and version-listing behavior not present in `memory-schema-search`.
- Process configuration, readiness, shutdown, and bounded-resource controls.

## Out of Boundary
- Changes to canonical memory validation, immutable version semantics, BM25/vector ranking, or embedding persistence.
- Generation or persistence of stored-memory embeddings.
- Shared network-service deployment.
- Durable guarantees beyond those provided by the memory store.

## Upstream / Downstream
- **Upstream**: The `memory-schema-search` canonical contracts and save/search operations; an optional embedding capability compatible with stored-memory embeddings; iii runtime/database access patterns where retrieval needs persistence access.
- **Downstream**: Agent harness MCP configurations and future memory-management tools.

## Existing Spec Touchpoints
- **Extends**: None. Retrieval and version-listing behavior are owned by this feature as explicit boundary exceptions.
- **Adjacent**: `memory-schema-search` owns stored data and search semantics; `harness-ingestion-worker` owns lifecycle-event ingestion and is not an MCP dependency.

## Constraints
- Rust 1.98.1, edition 2024, and Tokio 1.48.0.
- Use `rust-mcp-schema` 2.0.0 protocol contracts with a finite worker-owned line reader, dispatch queue, and active-call pool.
- Support the stateless MCP `2026-07-28` protocol over stdio, including per-request version and capability metadata.
- When query embeddings are configured, use the same deployment provider and model used for stored-memory embeddings.
- Never write protocol data or memory content to stderr diagnostics.
- Treat stdout as MCP protocol output only.
