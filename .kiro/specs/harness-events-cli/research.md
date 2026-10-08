# Research: harness-events-cli

## Scope
Brownfield gap analysis against [requirements.md](requirements.md), the existing harness ingestion implementation, adjacent specs, workspace configuration, and local `iii-sdk` 0.24.0 source.

## Current Assets

| Area | Existing asset | Gap |
|---|---|---|
| Event contract | `proto/total_recall/harness/v1/events.proto` | No CLI-side request construction |
| Function IDs | `workers/harness-ingestion/src/runtime.rs` | No client command mapping |
| Request validation | `workers/harness-ingestion/src/contracts.rs` | No strict stdin reader or command parser |
| Invocation pattern | `workers/harness-ingestion/src/publisher.rs` | No transient client lifecycle |
| Connection configuration | `workers/harness-ingestion/src/config.rs` | No CLI configuration or identity policy |
| Process behavior | Existing worker binaries use explicit exit codes and stderr | No CLI exit taxonomy or diagnostics |
| Contract tests | Ingestion request-adapter and contract-oracle tests | No CLI command, process, or protocol tests |
| Protocol testing | `integration/harness-engine-fake` | Existing fake is not importable and covers worker registration rather than client invocation |

## SDK Findings

- `iii-sdk` 0.24.0 defaults the engine URL to `ws://127.0.0.1:49134` and treats blank `III_URL` as absent.
- `register_worker` starts a dedicated connection thread and returns before registration completes.
- `wait_until_registered` distinguishes registration rejection, timeout, and lack of connection.
- `TriggerRequest` carries a function identifier, JSON payload, optional action, and optional timeout.
- A trigger without an action is synchronous and uses a 30-second default timeout unless overridden.
- Trigger results are untyped JSON. Transport success alone does not verify the ingestion response shape.
- A request without an explicit namespace inherits client namespace routing.
- Managed identity reads `III_WORKER_NAME` and `III_NAMESPACE`; explicit identity ignores them.
- Every connection registers a worker identity before invoking functions.
- `shutdown` joins the connection thread; asynchronous shutdown signals without joining.
- On send failure, the SDK can reconnect and flush the queued message. One application-level trigger call can therefore produce a transport replay.

## Contract Findings

- The authoritative success response is `{"dispatched":true}`.
- Observation data follows protobuf `Struct` semantics, including JSON-number conversion to protobuf doubles.
- Direct deserialization into `pbjson_types::Struct` rejects some integers that ingestion accepts and rounds through `serde_json::Number::as_f64()`; client conversion must be checked against the ingestion adapter.
- The ingestion worker rejects unknown top-level request fields and validates session identifiers and timestamp shape.
- Ingestion success confirms one successful queue invocation, not persistence, ordering, or deduplication.
- The persistence design treats a shared contract-crate extraction as a cross-spec revalidation trigger.

## Testing Findings

- Existing tests use `env!("CARGO_BIN_EXE_*")` with `std::process::Command`; no process-test library is required.
- Existing invocation code separates pure request construction from a `FnOnce(TriggerRequest)` seam.
- Existing configuration uses a `from_values` seam for deterministic environment tests.
- The ingestion request-adapter suite contains authoritative observation fixtures and `Struct` number-semantics coverage.
- A CLI protocol fake needs only worker-registration acknowledgement, one `invokefunction` inspection, and one success or error response.
- Invalid command and stdin handling can run before client creation, allowing tests to prove no engine connection occurred.

## Requirement-to-Gap Map

| Requirement | Gap classification | Needed capability |
|---|---|---|
| 1. Typed Command Surface | Missing | Parser, subcommands, required options, help, and pre-connection rejection |
| 2. Lifecycle Submission | Missing | Function mapping, request construction, one-call orchestration, and response handling |
| 3. Observation Submission | Missing | Strict single-object stdin parsing and safe `Struct` conversion |
| 4. Engine Routing | Missing / Constraint | Environment parsing, namespace routing, registration wait, and identity isolation |
| 5. Process Outcomes | Missing / Unknown | Exit taxonomy, safe diagnostics, response-shape policy, cleanup, and replay interpretation |
| 6. Compatibility | Missing | Unit, process, contract-oracle, and protocol-fake coverage |

## Implementation Options

### Dedicated CLI Depending on Harness Ingestion
Reuse ingestion-generated request types and adapters from a new client crate.

- Advantages: Exact validation and ProtoJSON behavior; smallest contract-drift risk.
- Costs: A production client depends on a worker library and its broader build/runtime graph.
- Estimate: S–M effort, medium risk.

### Standalone CLI with an Independent Request Boundary
Build request values in the client or independently compile the root protobuf; use ingestion as a development-only contract oracle.

- Advantages: Clean client dependency direction and an isolated production binary.
- Costs: Duplicated generation or conversion logic and stronger drift-test requirements.
- Estimate: M effort, medium risk.

### Shared Harness Contracts Crate
Extract protobuf generation and request conversion into a workspace library consumed by ingestion and the CLI.

- Advantages: One reusable Rust contract boundary for future integrations.
- Costs: Cross-worker refactoring, adjacent-spec revalidation, and scope expansion.
- Estimate: M–L effort, high risk.

## Questions Carried into Design

The finalized resolutions are authoritative in [design.md](design.md).

- Whether the no-retry requirement forbids SDK transport replay or only repeated application-level trigger calls.
- Whether process success requires the exact `{"dispatched":true}` response.
- Which stable non-zero exit statuses distinguish usage, input, connection, and invocation failures.
- Whether long option names use kebab case, snake case, or aliases.
- Whether the CLI overrides the SDK registration and invocation timeouts.
- How the transient client avoids inheriting `III_WORKER_NAME` while still honoring `III_NAMESPACE`.
- Whether diagnostics suppress all remote messages or sanitize them selectively.

## CLI Parser Dependency Finding

- Crates.io identifies `clap` 4.6.7 as the latest stable non-yanked release available during design research.
- The release declares Rust 1.85 as its minimum supported version and `MIT OR Apache-2.0` licensing.
- Its derive API supplies typed subcommands, required long options, generated help, and pre-handler argument rejection.
- Clap uses exit status `2` for usage errors and `0` for help or version output.
- Sources: [crates.io](https://crates.io/crates/clap/4.6.7), [derive tutorial](https://docs.rs/clap/4.6.7/clap/_derive/_tutorial/), and [error exits](https://docs.rs/clap/4.6.7/clap/error/struct.Error.html#method.exit_code).

## Research Conclusion
The feature is feasible with the pinned workspace stack and has no dependency or licensing blocker. The main design risks are SDK replay semantics, transient worker identity, contract ownership, deterministic shutdown, and safe process-level errors.
