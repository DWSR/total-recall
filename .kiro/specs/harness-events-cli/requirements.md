# Requirements Document

## Introduction
The harness events CLI provides the generic command boundary described in [brief.md](brief.md). It submits explicit lifecycle and observation requests to the existing harness ingestion functions for use by future harness-specific adapters.

## Boundary Context
- **In scope**: Typed event commands, explicit request fields, stdin observation data, ingestion-function invocation, connection configuration, diagnostics, and exit behavior.
- **Out of scope**: Harness-specific adapters, inferred request fields, retries, queue or persistence access, and distribution packaging.
- **Adjacent expectations**: The harness ingestion contract remains authoritative for request validation and successful dispatch; CLI success does not establish durable persistence, ordering, or deduplication.

## Requirements

### Requirement 1: Typed Command Surface
**Objective:** As a harness integrator, I want distinct event commands with explicit inputs, so that adapter behavior is predictable.

#### Acceptance Criteria
1. The Harness Events CLI shall provide `session-start`, `observation`, and `session-end` subcommands.
2. The Harness Events CLI shall require `session_id`, `project_name`, `current_working_directory`, and `timestamp` inputs for `session-start` and `session-end`.
3. The Harness Events CLI shall require `hook_type`, `project_name`, `current_working_directory`, `timestamp`, and `session_id` inputs for `observation`.
4. If a required command input is absent, the Harness Events CLI shall terminate with a non-zero exit status without invoking an ingestion function.
5. If a command or option is unrecognized, the Harness Events CLI shall terminate with a non-zero exit status without invoking an ingestion function.
6. When help is requested, the Harness Events CLI shall describe the available commands and their required inputs without invoking an ingestion function.

### Requirement 2: Lifecycle Event Submission
**Objective:** As a harness integrator, I want to submit lifecycle boundaries, so that downstream processing can correlate a harness-owned session.

#### Acceptance Criteria
1. When `session-start` is executed with all required inputs, the Harness Events CLI shall invoke `harness::session_start` once with those fields.
2. When `session-end` is executed with all required inputs, the Harness Events CLI shall invoke `harness::session_end` once with those fields.
3. The Harness Events CLI shall preserve each lifecycle field unchanged in the ingestion request.
4. If a lifecycle request is rejected by the ingestion function, the Harness Events CLI shall terminate with a non-zero exit status.

### Requirement 3: Observation Event Submission
**Objective:** As a harness integrator, I want to submit opaque hook data through standard input, so that harness-specific content does not become part of the command syntax.

#### Acceptance Criteria
1. When `observation` is executed, the Harness Events CLI shall read one JSON value from standard input.
2. When standard input contains one JSON object and all required command inputs are present, the Harness Events CLI shall invoke `harness::observation` once with the object as `data` and the command inputs as the remaining request fields.
3. The Harness Events CLI shall accept arbitrary object keys and ProtoJSON-compatible values in observation data.
4. The Harness Events CLI shall preserve observation data within `google.protobuf.Struct` semantics.
5. If standard input is empty, malformed, non-object, or contains trailing non-whitespace content after the JSON object, the Harness Events CLI shall terminate with a non-zero exit status without invoking an ingestion function.
6. If an observation request is rejected by the ingestion function, the Harness Events CLI shall terminate with a non-zero exit status.

### Requirement 4: Engine Routing Configuration
**Objective:** As an operator, I want the CLI to follow repository connection conventions, so that it targets the intended iii engine and namespace.

#### Acceptance Criteria
1. When `III_URL` contains a non-blank value, the Harness Events CLI shall use that value as the iii engine URL.
2. If `III_URL` is absent or blank, the Harness Events CLI shall use the iii SDK default engine URL.
3. When `III_NAMESPACE` contains a non-blank value, the Harness Events CLI shall invoke the ingestion function in that namespace.
4. If `III_NAMESPACE` is absent or blank, the Harness Events CLI shall use the iii SDK default namespace routing.
5. If the CLI cannot connect to the configured engine, the Harness Events CLI shall terminate with a non-zero exit status.

### Requirement 5: Process Outcome Contract
**Objective:** As a harness integrator, I want deterministic process outcomes, so that an adapter can react to submission results.

#### Acceptance Criteria
1. When the ingestion function reports success, the Harness Events CLI shall terminate with exit status `0`.
2. If command input, observation data, configuration, connection, or function invocation fails, the Harness Events CLI shall terminate with a documented non-zero exit status.
3. If the CLI terminates with a non-zero exit status, the Harness Events CLI shall write a diagnostic to standard error.
4. The Harness Events CLI shall not include observation data in diagnostics.
5. The Harness Events CLI shall issue at most one application-level ingestion request per execution and shall not initiate a retry after failure.
6. The Harness Events CLI shall not report function success as proof of durable persistence, ordered delivery, or deduplication.

### Requirement 6: Contract Compatibility and Verification
**Objective:** As a maintainer, I want the CLI checked against the authoritative ingestion boundary, so that contract drift is detected before harness adapters depend on it.

#### Acceptance Criteria
1. The Harness Events CLI shall construct inputs compatible with the authoritative harness request protobuf definitions.
2. The Harness Events CLI shall include automated tests for each subcommand's function identifier and request shape.
3. The Harness Events CLI shall include automated tests for missing inputs, unknown arguments, invalid standard input, connection failures, function failures, diagnostics, and exit statuses.
4. The Harness Events CLI shall include an automated test that preserves arbitrary keys and values in observation data.
5. The Harness Events CLI shall run its automated tests without a live iii engine or ingestion worker.
