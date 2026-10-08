# Brief: knowledge-graph-foundation

## Problem

"Create a spec for a knowledge graph in the existing Postgres database. The knowledge graph is intended to link concepts from session records and memories together to allow easier discovery of related information."

Agents and retrieval components can search individual memories and session-derived records, but they cannot follow explicit semantic relationships between concepts or inspect the records supporting those relationships.

## Current State

PostgreSQL stores immutable memory versions with flat concept strings and an append-only harness-event ledger. Memory search provides BM25 and exact vector retrieval. No component owns canonical concept identities, aliases, semantic relations, graph evidence, or traversal. Canonical session-record synthesis remains a separate downstream capability.

## Desired Outcome

A reusable graph-store boundary persists canonical concepts, aliases, concept mentions, typed assertions, and supporting source references in the existing PostgreSQL database. Internal callers can resolve aliases, inspect deterministic evidence-backed neighbors and related sources, and find cycle-safe bounded paths.

## Approach

Use normalized relational tables and recursive PostgreSQL queries rather than a graph extension or JSONB graph documents. Store-generated UUIDv7 concept IDs and unique normalized aliases identify concepts; identical creates deduplicate by normalized alias set. A fixed relation vocabulary gives assertions defined direction and symmetry. Typed external references inherit source-owner identity contracts without copying or owning either record type.

Graph records support bounded mutable CRUD. Creates are idempotent or return explicit conflicts. Concept deletion is restricted while mentions or assertions reference it; deleting an assertion removes its evidence. Automatic extraction and transport exposure remain separate capabilities.

## Scope

- **In**: Concept and alias contracts; fixed relation semantics; typed source references; concept mentions; evidence-backed assertions; idempotent create and bounded update/delete operations; alias resolution; neighbor, related-source, and bounded-path queries; deterministic results; least-privilege PostgreSQL schema and verification.
- **Out**: Session synthesis; concept or relation extraction; memory generation; embedding generation; BM25/vector fusion; MCP tools; generic property-graph entities; graph analytics; concept merging; database or extension provisioning.

## Boundary Candidates

- A reusable graph-store crate owns domain validation, graph operations, and its migrations.
- A future population worker owns extraction, checkpoints, retries, and calls into the graph store.
- A future retrieval adapter owns MCP contracts and ranking or fusion with existing search.

## Out of Boundary

- Raw event ingestion and interpretation remain unchanged.
- Existing memory rows, flat concept arrays, search heads, and search semantics remain unchanged.
- The foundation does not define or persist canonical session records.
- The foundation does not verify external source existence through cross-owner foreign keys.
- Retention, audit history, soft deletion, tenancy, and unrestricted graph algorithms are not introduced.

## Upstream / Downstream

- **Upstream**: `memory-schema-search` supplies immutable `(memory ID, version)` identities. A future session-processing capability supplies stable session-record IDs. PostgreSQL and the iii database boundary supply storage and query execution.
- **Downstream**: Graph population, MCP graph retrieval, and hybrid related-information discovery can consume the graph-store contracts.

## Existing Spec Touchpoints

- **Extends**: None. This is a new schema and domain owner.
- **Adjacent**: `memory-schema-search` owns memory history and ranked search. `harness-event-persistence-worker` owns the raw append-only event ledger. `mcp-worker` owns current memory-facing MCP tools.

## Constraints

- Use the existing PostgreSQL 17/18 deployment without a graph extension.
- Use normalized relational constraints, static parameterized SQL, and the existing iii database invocation boundary.
- Fixed relations are `related_to`, `is_a`, `part_of`, `depends_on`, `uses`, `implements`, `causes`, `resolves`, and `contradicts`.
- `related_to` and `contradicts` are symmetric; all other relations are directed.
- Recursive traversal must enforce cycle, depth, work, and result bounds inside SQL and use a database statement timeout.
- External references are typed stable keys and inherit source-owner identity validation. Graph source keys are limited to 2,048 UTF-8 bytes. Memory references include version; session references target canonical session records rather than raw event receipts.
- Errors and diagnostics must not expose source content, aliases, labels, or database details.
