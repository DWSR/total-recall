# Requirements Document

## Introduction
The harness ingestion worker provides the iii entry points described in [brief.md](brief.md). It accepts agent-harness lifecycle events and dispatches them for downstream memory processing.

## Requirements

### Requirement 1: Worker Availability
**Objective:** As an operator, I want the harness functions registered against a configured queue destination, so that harnesses have a predictable ingestion surface.

#### Acceptance Criteria
1. When the worker starts with a non-empty queue destination, the Harness Ingestion Worker shall register functions for session start, observation ingestion, and session end.
2. If the queue destination is missing or empty, the Harness Ingestion Worker shall terminate startup with an error.
3. If startup fails, the Harness Ingestion Worker shall not report readiness.

### Requirement 2: Harness-Owned Session Lifecycle
**Objective:** As a harness integrator, I want to report lifecycle boundaries using my session identifier, so that downstream processing can correlate session events.

#### Acceptance Criteria
1. When a session-start request contains a session identifier, project name, current working directory, and timestamp, the Harness Ingestion Worker shall dispatch a session-start event carrying those fields.
2. When a session-end request contains a session identifier, project name, current working directory, and timestamp, the Harness Ingestion Worker shall dispatch a session-end event carrying those fields.
3. The Harness Ingestion Worker shall set `event_type` to `session_start` for session-start events.
4. The Harness Ingestion Worker shall set `event_type` to `session_end` for session-end events.
5. If a lifecycle request omits a required field, supplies an empty session identifier, or supplies an invalid timestamp, the Harness Ingestion Worker shall reject the request without dispatching an event.
6. The Harness Ingestion Worker shall preserve harness-supplied lifecycle fields unchanged.
7. The Harness Ingestion Worker shall accept lifecycle events without tracking or validating session state.

### Requirement 3: Structured Observation Ingestion
**Objective:** As a harness integrator, I want to submit structured hook metadata with opaque data, so that downstream processors can route hooks without constraining harness-specific content.

#### Acceptance Criteria
1. When an observation request contains a hook type, project name, current working directory, timestamp, session identifier, and `data` map, the Harness Ingestion Worker shall dispatch an observation event carrying those fields.
2. The Harness Ingestion Worker shall set `event_type` to `observation` for observation events.
3. The Harness Ingestion Worker shall accept arbitrary JSON keys and ProtoJSON-compatible values within the `data` map.
4. The Harness Ingestion Worker shall preserve the `data` map's values within `google.protobuf.Struct` semantics.
5. The Harness Ingestion Worker shall not interpret or apply domain validation to entries in the `data` map.
6. If an observation request omits a required field, supplies an empty session identifier, supplies a non-object `data` value, or supplies an invalid timestamp, the Harness Ingestion Worker shall reject the request without dispatching an event.
7. The Harness Ingestion Worker shall accept an observation without confirming that a corresponding session-start event was received.

### Requirement 4: Protobuf Event Contract
**Objective:** As an integrator, I want one authoritative protobuf contract, so that harness and consumer implementations share compatible event schemas.

#### Acceptance Criteria
1. The Harness Ingestion Worker shall define session-start, observation, and session-end requests and events in protobuf.
2. The Harness Ingestion Worker shall accept iii function inputs using the protobuf JSON mapping.
3. The Harness Ingestion Worker shall dispatch queued events using the protobuf JSON mapping.
4. The Harness Ingestion Worker shall represent observation `data` as `google.protobuf.Struct`.
5. The Harness Ingestion Worker shall represent each timestamp as a protobuf string field.
6. The Harness Ingestion Worker shall treat the protobuf definitions as authoritative when generated types and serialized event shapes are evaluated.
7. If an iii function input contains an unknown top-level field, the Harness Ingestion Worker shall reject the request without dispatching an event.

### Requirement 5: Timestamp Contract
**Objective:** As a downstream consumer, I want harness event times in one precise format, so that events from different harnesses can be compared consistently.

#### Acceptance Criteria
1. The Harness Ingestion Worker shall accept timestamps formatted as RFC 3339 strings with exactly three fractional-second digits.
2. The Harness Ingestion Worker shall require each timestamp to include a UTC offset or the `Z` UTC designator.
3. The Harness Ingestion Worker shall preserve each accepted timestamp unchanged in the dispatched event.

### Requirement 6: Queue Dispatch Contract
**Objective:** As a downstream consumer, I want distinguishable events on one configured destination, so that I can process each lifecycle stage correctly.

#### Acceptance Criteria
1. The Harness Ingestion Worker shall dispatch all harness events to the configured queue destination.
2. The Harness Ingestion Worker shall include `event_type` in every dispatched event.
3. When the queue invocation succeeds, the Harness Ingestion Worker shall return success to the harness caller.
4. If the queue invocation fails, the Harness Ingestion Worker shall return an error to the harness caller.
5. The Harness Ingestion Worker shall not represent a successful queue invocation as proof of durable persistence, ordered delivery, or deduplication.
6. The Harness Ingestion Worker shall not retry a failed queue invocation.

### Requirement 7: Focused Verification
**Objective:** As a maintainer, I want focused automated coverage of the event boundary, so that contract regressions are detected without integration infrastructure.

#### Acceptance Criteria
1. The Harness Ingestion Worker shall include automated tests for the event produced by each harness function.
2. The Harness Ingestion Worker shall include an automated test that preserves arbitrary keys and values in an observation `data` map.
3. The Harness Ingestion Worker shall include automated tests for the ProtoJSON shape of each queued event.
4. The Harness Ingestion Worker shall include automated tests for invalid schemas, invalid timestamps, and queue invocation failures.
5. The Harness Ingestion Worker shall run its automated tests without a live iii engine or queue worker.
