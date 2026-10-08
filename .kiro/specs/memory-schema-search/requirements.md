# Requirements Document

## Introduction
The memory schema and search feature provides the canonical, versioned memory records and separately persisted embeddings described in [brief.md](brief.md). It exposes database operations for independent persistence and latest-version retrieval.

## Boundary Context
- **In scope**: Canonical memory records, immutable versions, separately associated embeddings, BM25 search over title/content/concepts, exact cosine vector search, search result ordering, operation outcomes, and content-safe diagnostics.
- **Out of scope**: Memory or embedding generation, public search functions, hybrid ranking, approximate vector search, additional filters, deletion, retention, and database provisioning.
- **Adjacent expectations**: A memory processor supplies complete memory records; an embedding producer may later associate one embedding with an existing memory version; downstream retrieval features consume the database operations without changing stored history.

## Requirements

### Requirement 1: Canonical Memory Record
**Objective:** As a memory processor, I want one validated memory contract, so that stored records retain their content and provenance.

#### Acceptance Criteria
1. When a valid memory is submitted, the Memory Store shall preserve its ID, type, title, content, created-at time, updated-at time, concepts, files, session IDs, source observation IDs, and version.
2. The Memory Store shall require concepts, files, session IDs, and source observation IDs collections while accepting each collection as empty.
3. The Memory Store shall preserve every supplied NUL-free collection value without deriving or interpreting it.
4. The Memory Store shall require a non-empty, NUL-free ID, type, title, and content, a positive version, and created-at and updated-at times in UTC whole-second precision with years `0001` through `9999`.
5. The Memory Store shall require the updated-at time to be no earlier than the created-at time.
6. If a submitted memory violates the memory contract, the Memory Store shall reject it without changing stored records.
7. The Memory Store shall not impose a controlled taxonomy on the memory type or concepts.
8. The Memory Store shall reject NUL-containing collection values and database targets before database invocation.

### Requirement 2: Versioned Persistence
**Objective:** As a memory processor, I want immutable versions under one stable ID, so that updates retain their history.

#### Acceptance Criteria
1. When a valid `(ID, version)` pair has not been stored, the Memory Store shall add it as a new memory version.
2. The Memory Store shall treat the combination of ID and version as the unique identity of a stored memory.
3. If a submitted `(ID, version)` pair already exists, the Memory Store shall reject the submission without replacing the stored version.
4. When a version is added for an existing ID, the Memory Store shall retain every earlier version unchanged.
5. When versions arrive out of order, the Memory Store shall retain each valid version and identify the greatest version number as the latest.
6. If memory persistence fails, the Memory Store shall report failure without reporting the memory as stored.

### Requirement 3: Separate Embedding Persistence
**Objective:** As an embedding producer, I want to associate a vector with an existing memory version independently, so that embedding work does not mutate canonical memory data.

#### Acceptance Criteria
1. When a valid embedding is submitted for an existing `(ID, version)` without an embedding, the Memory Store shall associate the embedding with that memory version without changing the canonical memory record.
2. The Memory Store shall require each embedding to contain from one through 16,000 finite components that round-trip exactly through PostgreSQL `float4` storage and have a non-zero norm.
3. If the referenced memory version does not exist, the Memory Store shall reject the embedding without changing stored records.
4. If an embedding already exists for the submitted `(ID, version)`, the Memory Store shall reject the submission without replacing the stored embedding.
5. When a memory is submitted without an embedding, the Memory Store shall persist the memory independently of embedding availability.
6. If embedding persistence fails, the Memory Store shall report failure without reporting the embedding as stored.

### Requirement 4: BM25 Search
**Objective:** As a retrieval component, I want ranked lexical search across memory text and concepts, so that I can retrieve relevant current memories.

#### Acceptance Criteria
1. When a non-empty lexical query is submitted with a positive result limit, the Memory Store shall rank matching memories using BM25 relevance.
2. The Memory Store shall match lexical query terms against each memory's title, content, and concepts.
3. When an ID has multiple stored versions, the Memory Store shall include only its greatest version number in lexical search results.
4. The Memory Store shall order lexical results by descending BM25 relevance and break equal-score ties by ID and version.
5. When no latest memory version matches a lexical query, the Memory Store shall return an empty result set.
6. If a lexical query is empty, contains a NUL byte, or its result limit is not positive, the Memory Store shall reject the query without requesting a search.
7. The Memory Store shall not represent lexical search as phrase search, field-weighted search, or hybrid lexical-vector ranking.

### Requirement 5: Vector Search
**Objective:** As a retrieval component, I want cosine-ranked vector search, so that I can retrieve semantically similar current memories.

#### Acceptance Criteria
1. When a valid query vector is submitted with a positive result limit, the Memory Store shall rank compatible memory embeddings by exact cosine similarity.
2. The Memory Store shall search only latest memory versions that have an associated embedding with the same dimension as the query vector.
3. The Memory Store shall order vector results by descending cosine similarity and break equal-score ties by ID and version.
4. When no latest memory version has a compatible associated embedding, the Memory Store shall return an empty result set.
5. If a query vector is empty, exceeds 16,000 components, contains a non-finite or non-`float4`-exact value, has a zero norm, or has a non-positive result limit, the Memory Store shall reject the query without requesting a search.
6. The Memory Store shall not represent vector search as approximate nearest-neighbor search or hybrid lexical-vector ranking.

### Requirement 6: Search Results and Diagnostics
**Objective:** As a retrieval maintainer, I want complete results and content-safe failures, so that callers can trace matches without exposing memory data through diagnostics.

#### Acceptance Criteria
1. When a search succeeds, the Memory Store shall return each matched memory's ID, version, type, title, content, timestamps, concepts, files, session IDs, source observation IDs, and relevance score.
2. The Memory Store shall not return stored embeddings in search results.
3. If a search operation fails, the Memory Store shall report an error instead of returning partial results as complete.
4. The Memory Store shall not include memory titles, content, embeddings, concepts, files, session IDs, or source observation IDs in logs or returned errors.

### Requirement 7: Focused Verification
**Objective:** As a maintainer, I want automated coverage of versioned persistence and ranking behavior, so that schema and query regressions are detected.

#### Acceptance Criteria
1. The Memory Store shall include automated verification for valid records, invalid records, duplicate versions, and out-of-order versions.
2. The Memory Store shall include automated verification for independent embedding writes, missing memory versions, duplicate embeddings, and invalid embeddings.
3. The Memory Store shall include automated verification that lexical matches can originate from title, content, or concepts.
4. The Memory Store shall include automated verification for latest-version lexical and vector results, relevance ordering, deterministic tie-breaking, empty results, and result limits.
5. The Memory Store shall include automated verification for incompatible embedding dimensions and invalid query vectors.
6. The Memory Store shall include automated verification that search results omit embeddings and diagnostics omit protected memory data.
7. The Memory Store shall include automated verification that NUL text, fractional-second timestamps, unsupported timestamp years, and PostgreSQL-incompatible vectors are rejected before database invocation.
