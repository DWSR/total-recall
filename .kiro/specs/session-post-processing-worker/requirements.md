# Requirements Document

## Introduction
The session post-processing worker creates the finalized session projection and memories described in [brief.md](brief.md). It consumes persisted source events without changing the ingestion ledger.

## Boundary Context
- **In scope**: Scheduled eligibility checks, comprehensive session records, generated summaries and concepts, relevant memory creation, late-event refresh, retries, and content-safe operations.
- **Out of scope**: Event ingestion, source-event mutation, session or memory embeddings, recall APIs, memory search behavior, retention, and provider routing.
- **Adjacent expectations**: The harness event store supplies persisted lifecycle and observation records; the canonical memory store accepts complete memory records; one configured model endpoint supplies structured generation results.

## Requirements

### Requirement 1: Scheduled Worker Availability
**Objective:** As an operator, I want one configurable processing schedule, so that eligible sessions are finalized without manual action.

#### Acceptance Criteria
1. When the worker starts with valid source-and-session-store, memory-store, schedule, and model settings, the Session Post-Processing Worker shall register one scheduled processing entry point.
2. The Session Post-Processing Worker shall default its schedule to once per minute.
3. When an operator supplies a valid schedule override, the Session Post-Processing Worker shall use that schedule instead of the default.
4. If a required destination or setting is missing, empty, or invalid, the Session Post-Processing Worker shall not report readiness.
5. If scheduled-entry-point registration fails, the Session Post-Processing Worker shall not report readiness.

### Requirement 2: Session Eligibility
**Objective:** As a session owner, I want processing to wait for completion or sustained inactivity, so that active sessions are not finalized prematurely.

#### Acceptance Criteria
1. When at least one persisted session-end event exists for a session, the Session Post-Processing Worker shall make that session eligible for processing.
2. When no persisted session-end event exists and 24 hours have elapsed since the event store recorded the session's latest observation, the Session Post-Processing Worker shall make that session eligible for processing.
3. While no persisted session-end event exists and fewer than 24 hours have elapsed since the latest observation was recorded, the Session Post-Processing Worker shall not create or refresh a session record.
4. While a session has neither a persisted session-end event nor a persisted observation, the Session Post-Processing Worker shall not make the session eligible.
5. When a session-end event exists without an observation or session-start event, the Session Post-Processing Worker shall process the available session data.
6. When repeated or out-of-order lifecycle events exist, the Session Post-Processing Worker shall evaluate eligibility without rejecting the session.

### Requirement 3: Bounded and Exclusive Processing
**Objective:** As an operator, I want scheduled runs to avoid duplicate concurrent work, so that frequent or overlapping schedules remain safe.

#### Acceptance Criteria
1. The Session Post-Processing Worker shall limit each scheduled run to a positive configured maximum number of eligible sessions.
2. When scheduled runs overlap, the Session Post-Processing Worker shall not process the same session source revision concurrently.
3. When multiple worker instances discover the same eligible session, the Session Post-Processing Worker shall allow at most one instance to own that processing attempt.
4. If an owned attempt does not complete, the Session Post-Processing Worker shall make it eligible for a later retry after a bounded ownership period.
5. When no session is eligible, the Session Post-Processing Worker shall complete the scheduled run without creating or changing session records or memories.

### Requirement 4: Comprehensive Session Record
**Objective:** As a recall consumer, I want one comprehensive record for each eligible session, so that the complete recorded session can be reviewed and recalled.

#### Acceptance Criteria
1. When an eligible session is processed successfully for the first time, the Session Post-Processing Worker shall create one logical session record identified by the session ID.
2. The Session Post-Processing Worker shall include every persisted lifecycle event and observation selected for the processing cutoff in the comprehensive record.
3. The Session Post-Processing Worker shall preserve each included source record's receipt identifier, event type, session metadata, source timestamp, recorded time, and variant-specific data.
4. The Session Post-Processing Worker shall order included source records deterministically without representing source delivery as ordered or deduplicated.
5. The Session Post-Processing Worker shall assemble the comprehensive source transcription from persisted source records rather than generated model text.
6. The Session Post-Processing Worker shall include a non-empty generated summary containing no more than five sentences.
7. The Session Post-Processing Worker shall include exactly 10 distinct, non-empty generated concepts.
8. The Session Post-Processing Worker shall record the source cutoff and generation time used for the current session record.

### Requirement 5: Session-Specific Model Providers
**Objective:** As an operator, I want isolated model configuration for session post-processing, so that this workload does not constrain later model-backed features.

#### Acceptance Criteria
1. The Session Post-Processing Worker shall support OpenAI-compatible and Anthropic model endpoints.
2. The Session Post-Processing Worker shall require exactly one active provider for each deployment.
3. Where the OpenAI-compatible provider is active, the Session Post-Processing Worker shall use its independently configured base URL, API token, and model.
4. Where the Anthropic provider is active, the Session Post-Processing Worker shall use its independently configured base URL, API token, and model.
5. The Session Post-Processing Worker shall not route individual sessions between providers or fall back to the inactive provider.
6. If the active provider returns a refusal, truncated output, malformed output, or an output that violates the session-generation contract, the Session Post-Processing Worker shall treat the attempt as failed.
7. If a provider attempt fails, the Session Post-Processing Worker shall not publish a partial session-record update or partial candidate set as complete.

### Requirement 6: Relevant Memory Creation
**Objective:** As a recall consumer, I want durable details extracted automatically, so that relevant session knowledge is available through existing memory retrieval.

#### Acceptance Criteria
1. When successful generation identifies relevant durable details or concepts, the Session Post-Processing Worker shall create one or more canonical memories for the session.
2. When no relevant durable detail or concept is identified, the Session Post-Processing Worker shall complete session processing without creating a memory.
3. The Session Post-Processing Worker shall associate each created memory with the source session ID.
4. The Session Post-Processing Worker shall associate each created memory with the receipt identifiers of its supporting observations.
5. If a memory candidate does not satisfy the canonical memory contract or refers to an observation outside the processed session cutoff, the Session Post-Processing Worker shall reject that candidate without reporting it as stored.
6. When an attempt is retried, the Session Post-Processing Worker shall not create a duplicate of a memory already committed by that attempt.
7. If some memories are committed before an attempt fails, the Session Post-Processing Worker shall resume the remaining work without changing the committed memories.

### Requirement 7: Late-Event Refresh
**Objective:** As a recall consumer, I want late session data incorporated, so that the single session record remains comprehensive.

#### Acceptance Criteria
1. When a source event is recorded after the current session record's cutoff, the Session Post-Processing Worker shall reevaluate refresh eligibility under the persisted-end and 24-hour-inactivity rules.
2. While no persisted session-end event exists and fewer than 24 hours have elapsed since the latest late observation was recorded, the Session Post-Processing Worker shall not refresh the session record.
3. When a refresh succeeds, the Session Post-Processing Worker shall replace the current session record in place rather than create a second logical record.
4. When a refresh succeeds, the Session Post-Processing Worker shall recompute the summary and 10 concepts from all source records through the new cutoff.
5. When late observations contain newly relevant durable details or concepts, the Session Post-Processing Worker shall add only new memories supported by those late observations.
6. The Session Post-Processing Worker shall not update or delete memories created by an earlier processing cutoff.
7. If a refresh fails, the Session Post-Processing Worker shall retain the prior complete session record and previously committed memories.

### Requirement 8: Retry and Completion State
**Objective:** As an operator, I want processing to resume after failures, so that transient errors do not lose eligible sessions or duplicate outputs.

#### Acceptance Criteria
1. If source retrieval, generation, session-record persistence, or memory persistence fails, the Session Post-Processing Worker shall retain enough progress to retry the incomplete source revision.
2. When a failed source revision is retried successfully, the Session Post-Processing Worker shall expose one complete current session record and each committed memory once.
3. While a source revision is incomplete, the Session Post-Processing Worker shall not represent it as complete.
4. When every required session-record and memory operation for a source revision succeeds, the Session Post-Processing Worker shall mark that revision complete.
5. If permanent configuration or authorization errors prevent processing, the Session Post-Processing Worker shall report a content-safe failure category instead of repeatedly reporting success.

### Requirement 9: Confidential Operations
**Objective:** As an operator, I want safe diagnostics, so that model and session processing do not disclose protected content or credentials.

#### Acceptance Criteria
1. The Session Post-Processing Worker shall not include raw observations, comprehensive transcriptions, summaries, concepts, memory titles, memory content, source observation identifiers, API tokens, or provider response bodies in logs or returned errors.
2. When an operation fails, the Session Post-Processing Worker shall report the affected stage, session-safe correlation data, and a stable failure category.
3. The Session Post-Processing Worker shall not expose an active provider's API token through readiness, configuration summaries, or diagnostics.
4. When model output is rejected, the Session Post-Processing Worker shall report the contract violation without reproducing the rejected output.

### Requirement 10: Focused Verification
**Objective:** As a maintainer, I want automated coverage of finalization and recovery behavior, so that scheduling, generation, and persistence regressions are detected.

#### Acceptance Criteria
1. The Session Post-Processing Worker shall include automated tests for session-end eligibility, 24-hour inactivity eligibility, active-session exclusion, and sessions with incomplete lifecycle data.
2. The Session Post-Processing Worker shall include automated tests for comprehensive source inclusion, deterministic ordering, summaries of at most five sentences, and exactly 10 distinct concepts.
3. The Session Post-Processing Worker shall include automated tests for both provider contracts, invalid provider configuration, refusals, truncation, malformed output, and invalid concept counts.
4. The Session Post-Processing Worker shall include automated tests for zero-memory sessions, provenance, duplicate prevention, partial memory failures, and retry completion.
5. The Session Post-Processing Worker shall include automated tests for overlapping runs, expired ownership, late-event replacement, incremental late-observation memories, and failed-refresh preservation.
6. The Session Post-Processing Worker shall include automated tests that diagnostics omit protected session, memory, and provider content.
7. The Session Post-Processing Worker shall run focused automated tests without live event, model, session, memory, cron, or worker infrastructure.
