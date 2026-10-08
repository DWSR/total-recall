# Requirements Document

## Introduction
The harness event persistence worker records the queued events described in [brief.md](brief.md). It provides an insert-first persistence boundary for downstream processing.

## Boundary Context
- **In scope**: Queue-triggered persistence, immutable harness-event records, delivery outcomes, and optional embedding storage isolated from event writes.
- **Out of scope**: Event publication, read APIs, session projection, deduplication, ordering guarantees, embedding generation, and vector search.
- **Adjacent expectations**: `harness-ingestion-worker` supplies the authoritative event contract; queue and database services supply delivery and storage capabilities.

## Requirements

### Requirement 1: Worker Availability
**Objective:** As an operator, I want one configured persistence entry point, so that queued harness events have a predictable database destination.

#### Acceptance Criteria
1. When the worker starts with valid queue and database destinations, the Harness Event Persistence Worker shall register one persistence function and one queue subscription for that function.
2. If either destination is missing or empty, the Harness Event Persistence Worker shall terminate startup with an error.
3. If function or subscription registration fails, the Harness Event Persistence Worker shall not report readiness.

### Requirement 2: Queued Event Contract
**Objective:** As a maintainer, I want the persistence boundary to consume the publisher's event contract, so that both workers agree on accepted data.

#### Acceptance Criteria
1. When a valid session-start, observation, or session-end event is delivered, the Harness Event Persistence Worker shall accept the event variant and its required fields.
2. The Harness Event Persistence Worker shall preserve the session identifier, project name, current working directory, source timestamp, event type, and variant-specific fields from each accepted event.
3. The Harness Event Persistence Worker shall preserve observation data without interpreting its keys or values.
4. If a delivery violates the authoritative event contract, the Harness Event Persistence Worker shall reject it without requesting a database write.
5. The Harness Event Persistence Worker shall not redefine or extend the authoritative queued event contract.
6. When an event is accepted, the Harness Event Persistence Worker shall derive a normalized chronological instant from the source timestamp while preserving the supplied timestamp text.

### Requirement 3: Append-Only Persistence
**Objective:** As an operator, I want ingestion to favor independent inserts, so that concurrent deliveries do not contend on mutable session state.

#### Acceptance Criteria
1. When an accepted event is persisted, the Harness Event Persistence Worker shall add a new immutable record.
2. The Harness Event Persistence Worker shall not update or delete an earlier harness-event record while processing a delivery.
3. When an event is delivered before its corresponding session-start event, the Harness Event Persistence Worker shall persist it without requiring existing session state.
4. When the same payload is delivered more than once, the Harness Event Persistence Worker shall persist each accepted delivery as a distinct record.
5. The Harness Event Persistence Worker shall not report exactly-once persistence, deduplication, or ordered delivery.
6. When the persistence schema assigns a receipt identifier, the persistence schema shall use a time-ordered version 7 UUID as the record's primary key.
7. The persistence schema shall provide ascending chronological lookup by session identifier and source event time for lifecycle and observation records.

### Requirement 4: Session Lifecycle Records
**Objective:** As a downstream session processor, I want complete lifecycle records, so that session state can be derived later without mutating the ingestion ledger.

#### Acceptance Criteria
1. When a session-start event is accepted, the Harness Event Persistence Worker shall persist its session identifier, project name, current working directory, source timestamp, and start event type.
2. When a session-end event is accepted, the Harness Event Persistence Worker shall persist its session identifier, project name, current working directory, source timestamp, and end event type.
3. The Harness Event Persistence Worker shall retain the supplied lifecycle event type without inferring a current session state.
4. The Harness Event Persistence Worker shall accept repeated starts, repeated ends, and an end without a preceding start.

### Requirement 5: Observation Records
**Objective:** As a downstream observation processor, I want hook metadata and opaque data retained together, so that later processing can interpret the event without constraining ingestion.

#### Acceptance Criteria
1. When an observation event is accepted, the Harness Event Persistence Worker shall persist its session identifier, hook type, project name, current working directory, source timestamp, event type, and complete data object.
2. The Harness Event Persistence Worker shall retain arbitrary keys and values in the observation data object within the authoritative event contract's value semantics.
3. The Harness Event Persistence Worker shall accept an observation without confirming that the session is active or known.
4. When an observation has `hook_type` set to `post_tool_use`, the Harness Event Persistence Worker shall persist it as an observation with the hook type unchanged.

### Requirement 6: Persistence Outcome
**Objective:** As a queue operator, I want delivery acknowledgment tied to the database result, so that failed writes remain eligible for queue failure handling.

#### Acceptance Criteria
1. When the database service confirms that an event record was committed, the Harness Event Persistence Worker shall return success for the delivery.
2. If the database invocation fails or does not confirm a commit, the Harness Event Persistence Worker shall return an error for the delivery.
3. If an event delivery fails, the Harness Event Persistence Worker shall not retry it independently of the queue service.
4. The Harness Event Persistence Worker shall not represent its success as proof that the upstream queue publish was lossless.
5. The Harness Event Persistence Worker shall not include opaque observation data in logs or returned errors.

### Requirement 7: Optional Session Embeddings
**Objective:** As a future embedding processor, I want session embeddings isolated from event ingestion, so that vector enrichment does not add work to the harness-event write path.

#### Acceptance Criteria
1. The persistence schema shall provide optional storage for session embeddings independently of harness-event records.
2. When no embedding exists for a session, the Harness Event Persistence Worker shall persist harness events without additional vector data.
3. The Harness Event Persistence Worker shall not generate, populate, index, or search session embeddings.

### Requirement 8: Focused Verification
**Objective:** As a maintainer, I want automated coverage of persistence behavior, so that contract and acknowledgment regressions are detected without integration infrastructure.

#### Acceptance Criteria
1. The Harness Event Persistence Worker shall include automated tests for every accepted event variant and its requested database record.
2. The Harness Event Persistence Worker shall include automated tests for malformed deliveries, out-of-order lifecycle events, repeated payloads, and unconfirmed database commits.
3. The Harness Event Persistence Worker shall include an automated test that preserves arbitrary observation data without logging it.
4. The Harness Event Persistence Worker shall run its automated tests without live queue, database, or worker infrastructure.
