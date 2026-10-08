# Requirements Document

## Introduction
The MCP worker provides the harness-facing memory tools defined in [brief.md](brief.md).

## Boundary Context
- **In scope**: Local MCP availability, memory creation, optional query embedding generation, lexical-only fallback, combined lexical/vector search, exact and latest retrieval, version listing, bounded requests, typed results, and content-safe failures.
- **Out of scope**: Network transports, memory updates, stored-memory embedding writes or generation, hybrid relevance scoring, deletion, retention, search pagination, filters, authentication, and database provisioning.
- **Adjacent expectations**: The memory store preserves canonical records and supplies lexical/vector search; when query embeddings are enabled, operators configure the same provider and model used for stored-memory embeddings; later enrichment may append enriched memory versions without changing the MCP-created version.

## Requirements

### Requirement 1: MCP Availability and Discovery
**Objective:** As a harness integrator, I want a predictable local MCP endpoint, so that a harness can discover and call memory tools.

#### Acceptance Criteria
1. When the MCP Worker starts with valid required configuration, the MCP Worker shall accept MCP communication over its process input and output streams.
2. When a request carries supported MCP `2026-07-28` version and capability metadata, the MCP Worker shall process the request without requiring a prior session handshake.
3. When a harness requests server discovery, the MCP Worker shall advertise server identity and tool support.
4. When a harness requests the tool list, the MCP Worker shall advertise capabilities for memory creation, combined memory search, latest retrieval, exact-version retrieval, and version listing.
5. The MCP Worker shall not advertise prompts or resources.
6. If required configuration is missing or invalid, the MCP Worker shall terminate startup without accepting protocol requests.
7. The MCP Worker shall reserve its process output stream for MCP protocol messages.
8. If a request omits required protocol metadata or names an unsupported protocol version, the MCP Worker shall reject it without executing a memory operation.

### Requirement 2: Memory Creation
**Objective:** As a harness, I want to save a titled memory for my current session, so that it becomes available to later retrieval and enrichment.

#### Acceptance Criteria
1. When a create request contains a non-empty title, non-empty content, and non-empty current session ID, the MCP Worker shall submit one canonical memory for persistence.
2. When creating a memory, the MCP Worker shall assign a new stable ID, version `1`, type `unclassified`, and equal current created-at and updated-at timestamps.
3. When creating a memory, the MCP Worker shall store the supplied current session ID as the memory's only session ID.
4. When creating a memory, the MCP Worker shall initialize concepts, files, and source observation IDs as empty collections.
5. When persistence succeeds, the MCP Worker shall return the complete persisted canonical memory.
6. If the title, content, or current session ID is empty or missing, the MCP Worker shall reject the request without submitting a memory for persistence.
7. If persistence fails, the MCP Worker shall return a tool error without reporting the memory as created.
8. The MCP Worker shall not accept an embedding through the memory creation capability.

### Requirement 3: Combined Memory Search
**Objective:** As a harness, I want one search operation to consider lexical and semantic matches, so that I can retrieve a deterministic union of relevant current memories.

#### Acceptance Criteria
1. When a search request contains a non-empty NUL-free query and a result limit from `1` through `50`, the MCP Worker shall submit one BM25 search using the supplied query and requested limit without requiring a caller-supplied vector.
2. Where a provider and model are configured for query embeddings, the MCP Worker shall request one query embedding for the supplied query.
3. When query embedding generation succeeds, the MCP Worker shall require the returned provider and model to match the configured values and exactly one vector to correspond to the supplied query.
4. When query embedding generation returns one vector containing from one through 16,000 finite components that round-trip exactly through PostgreSQL `float4` storage and have a non-zero norm, the MCP Worker shall submit one exact cosine vector search using the generated embedding and requested limit.
5. When both searches succeed, the MCP Worker shall merge their latest-version results by memory ID.
6. If both searches return the same memory ID with different versions, the MCP Worker shall retain the result with the greater version number.
7. When combined or lexical-only search succeeds, the MCP Worker shall omit the generated query embedding and lexical and vector relevance scores from the returned memories.
8. When combined or lexical-only search succeeds, the MCP Worker shall sort results by ascending memory ID and apply the requested result limit after deduplication.
9. When every search used by the selected path returns no results, the MCP Worker shall return an empty result collection.
10. If the query is empty, contains a NUL byte, or the result limit is outside `1` through `50`, the MCP Worker shall reject the request without generating an embedding or submitting a search.
11. Where query embedding provider and model settings are both absent, empty, or whitespace-only, the MCP Worker shall complete the request using BM25 search alone.
12. If exactly one of query embedding provider or model is non-blank, the MCP Worker shall terminate startup without accepting protocol requests.
13. If query embedding generation fails, reports another provider or model, returns another vector count, or returns an empty, oversized, non-finite, or zero-norm vector, the MCP Worker shall complete the request using the BM25 result without submitting vector search.
14. If BM25 search fails, the MCP Worker shall return a tool error without returning vector-search results.
15. If vector search fails after successful query embedding generation, the MCP Worker shall return a tool error without returning BM25 results.
16. The MCP Worker shall not accept a query vector from the caller.
17. The MCP Worker shall not combine lexical and vector relevance scores.
18. The MCP Worker shall not represent combined or lexical-only results as relevance-ranked.

### Requirement 4: Memory Version Listing
**Objective:** As a harness, I want to list a memory's version history, so that I can see which immutable versions exist and when each was updated.

#### Acceptance Criteria
1. When a version-list request contains a non-empty memory ID, an offset from `0` through `9,007,199,254,740,891`, and a limit from `1` through `100`, the MCP Worker shall return one page of stored version numbers and updated-at times for that ID.
2. When offset or limit is omitted, the MCP Worker shall use offset `0` and limit `50`.
3. When version listing succeeds, the MCP Worker shall order entries by descending version number.
4. When more entries remain after the returned page, the MCP Worker shall return the next offset; otherwise, the MCP Worker shall return no next offset.
5. When no version exists for the requested ID or offset, the MCP Worker shall return an empty version collection and no next offset.
6. If a version-list request contains an empty memory ID, an offset outside `0` through `9,007,199,254,740,891`, or a limit outside `1` through `100`, the MCP Worker shall reject it without submitting a retrieval operation.
7. If version listing fails, the MCP Worker shall return a tool error without returning a partial version collection.
8. The MCP Worker shall not include titles, content, embeddings, concepts, files, session IDs, source observation IDs, or relevance scores in version-list results.

### Requirement 5: Memory Retrieval
**Objective:** As a harness, I want exact and latest retrieval, so that I can inspect a known memory deterministically.

#### Acceptance Criteria
1. When a latest retrieval request contains a non-empty memory ID, the MCP Worker shall return the stored version with the greatest version number for that ID.
2. When an exact retrieval request contains a non-empty memory ID and a positive version, the MCP Worker shall return that stored `(ID, version)` record.
3. When retrieval succeeds, the MCP Worker shall return every canonical memory field and shall not return an embedding.
4. If the requested ID or `(ID, version)` does not exist, the MCP Worker shall return a not-found tool error.
5. If a retrieval request contains an empty ID or a non-positive exact version, the MCP Worker shall reject the request without submitting a retrieval operation.
6. If retrieval fails, the MCP Worker shall return a tool error without returning a partial memory record.

### Requirement 6: Tool Contracts and Diagnostics
**Objective:** As a harness maintainer, I want typed tool contracts and content-safe failures, so that integrations can validate calls without exposing memory data.

#### Acceptance Criteria
1. When the MCP Worker advertises a memory tool, the MCP Worker shall provide an input schema for every accepted argument.
2. When a memory tool succeeds, the MCP Worker shall return a result matching that tool's advertised result contract.
3. If a request contains an unknown argument or a value with the wrong type, the MCP Worker shall reject it without invoking a memory operation.
4. If a memory operation fails, the MCP Worker shall distinguish invalid input, not found, conflict, backend failure, and internal failure where applicable.
5. The MCP Worker shall not include raw protocol frames, protocol metadata values, unknown argument values, search queries, generated query embeddings, embedding provider or model identifiers, embedding-provider responses or credentials, memory titles, content, stored embeddings, concepts, files, session IDs, or source observation IDs in errors or diagnostics.
6. The MCP Worker shall write operational diagnostics only to its process error stream.

### Requirement 7: Bounded Stdio Processing
**Objective:** As an operator, I want finite request and queue bounds, so that malformed or excessive input cannot cause unbounded worker memory growth.

#### Acceptance Criteria
1. The MCP Worker shall enforce a finite maximum input-message length before deserializing a tool request.
2. If an input message exceeds the configured maximum length, the MCP Worker shall discard that message without invoking a tool.
3. When an oversized message is followed by a valid bounded message, the MCP Worker shall continue processing the valid message.
4. The MCP Worker shall enforce a finite capacity for queued inbound messages.
5. If a configured message-length or queue-capacity value is not positive, the MCP Worker shall terminate startup without reporting protocol readiness.
6. The MCP Worker shall use finite message-length and queue-capacity defaults when explicit values are absent.

### Requirement 8: Focused Verification
**Objective:** As a maintainer, I want automated coverage of the MCP boundary, so that protocol and memory-tool regressions are detected without a live harness.

#### Acceptance Criteria
1. The MCP Worker shall include automated verification for stateless request handling, server discovery, tool discovery, advertised schemas, and invalid protocol metadata.
2. The MCP Worker shall include automated verification for valid creation, worker-assigned fields, invalid creation, and persistence failure.
3. The MCP Worker shall include automated verification for fully absent embedding configuration, partial embedding configuration, query embedding identity, cardinality, vector validation, NUL-query rejection, rejection of caller-supplied vectors, lexical-only fallback after each generation-failure class, BM25 failure, vector-search failure, both underlying searches, deduplication, greatest-version selection, ID ordering, result limits, and empty results.
4. The MCP Worker shall include automated verification for latest retrieval, exact retrieval, version-list defaults, limits, offsets, next offsets, invalid identifiers, absent records, ordering, and backend failure.
5. The MCP Worker shall include automated verification that search and retrieval results omit generated and stored embeddings, version-list results expose only version metadata, and failures omit protected query and memory data.
6. The MCP Worker shall include automated verification for oversized messages, post-oversize recovery, bounded queue configuration, protocol-only output, and error-stream diagnostics.
7. The MCP Worker's automated tests shall run without a live harness, MCP network service, embedding provider, or database.
