---
updated_at: 2026-09-21
---

# Structure

## Workspace Organization

The repository is a virtual Rust workspace organized by deployment role rather
than by a single application tree:

- `crates/` contains reusable domain or infrastructure libraries.
- `clients/` contains user-facing command-line or integration clients.
- `workers/` contains independently runnable processing or protocol boundaries.
- `integration/` contains executable fakes and environment-level verification
  fixtures, not production libraries.
- `proto/` contains versioned cross-component wire definitions.

Add a package to the narrowest matching role. A reusable domain invariant does
not belong in a worker, and production behavior does not belong in an
integration fake.

## Package Shape

Production executables normally provide both `src/lib.rs` and `src/main.rs`.
Keep business logic importable from the library; keep the binary responsible
for configuration, dependency composition, signals, diagnostics, exit status,
and shutdown.

Packages use flat responsibility-named modules when practical:

- `config` parses and validates external settings.
- `contracts` defines wire or boundary types and validation.
- A service/domain module coordinates the use case.
- Adapter modules implement queue, database, SDK, or protocol ports.
- `runtime` owns registrations, concurrency, readiness, and process lifecycle.

Do not force every package to expose identical module names. Preserve the
separation of contracts, domain behavior, adapters, and process composition.
Keep implementation modules private and re-export the smallest useful public
surface when callers do not need module-level access.

## Dependency Direction

Dependencies should point inward from transports and executables toward domain
contracts and services:

```text
binary/runtime -> adapter -> service/domain -> contracts
                         \-> external async port
```

Use traits as substitution boundaries for external effects. Avoid global state
and avoid adding cross-package dependencies merely to share test helpers or a
small constant. If a wire contract must be shared across production packages,
make that ownership explicit through protobuf or a focused contract library.

## Protocol and Schema Ownership

Version shared protobuf packages in their directory and namespace, for example
`proto/total_recall/harness/v1`. Consumers generate language bindings during
their builds and include them from contract modules.

Keep each database migration beside the component that owns its schema. Use
four-digit migration ordering followed by a descriptive snake_case name, such
as `0001_memory_schema_search.sql`.

iii worker manifests live beside the worker they launch. A local stdio worker
does not need an iii manifest unless its runtime model changes.

## Naming

- Directories and Cargo package names use kebab-case.
- Rust crate names, modules, files, functions, and variables use snake_case.
- Types and traits use PascalCase.
- Constants and environment variables use UPPER_SNAKE_CASE.
- iii function identifiers use namespaced strings such as `harness::observation`.
- Serialized discriminators and fields use snake_case unless an external
  protocol requires another spelling.
- CLI subcommands and long options use kebab-case.
- Tests use behavior-oriented snake_case names that state the expected outcome.

## Imports and Visibility

Within a library, prefer `crate::...` imports. Binaries import their package
library by crate name, and package integration tests import it as an external
consumer would.

Group imports consistently as standard library, external crates, then local
crate imports. Let rustfmt determine layout within those groups.

Default to private modules and items. Expose contracts, traits, and orchestration
entry points only when another package or integration test needs them. Test
doubles belong under `#[cfg(test)]` or package test support unless they are
intentional public fixtures.

## Test Placement

Place private helper tests in an adjacent `#[cfg(test)] mod tests`. Place public
contract, cross-module, and binary behavior in package-level `tests/` files.

Reusable process or protocol fixtures for one package live under that package's
`tests/support`. Repository-level `integration/` packages are reserved for
standalone fakes or checks that cross a process, engine, or database boundary.

Binary tests should invoke Cargo-provided executable paths and assert protocol
traffic, stdout/stderr ownership, exit status, signals, timeouts, and cleanup as
relevant. Contract-oracle tests should compare independently generated or
consumed representations rather than calling the code under test twice.

## Adding New Components

For a new worker:

- Define and validate contracts before implementing external effects.
- Put core orchestration in a library module behind async ports.
- Keep iii/database/MCP details in adapters and runtime composition.
- Add a thin binary and, where relevant, a colocated worker manifest.
- Cover the domain with unit tests and the external boundary with a protocol
  fake or environment verifier.

For a new shared capability, first decide whether it is a domain library, a
wire contract, or an independently deployed worker. Do not create a generic
utility crate until multiple production components share a stable abstraction.

## Known Structural Pressure Points

Protocol code generation and some runtime/configuration patterns are currently
duplicated across packages. Treat existing contract tests as drift guards. Do
not consolidate these areas unless the shared abstraction has one clear owner
and preserves package independence.

Large integration fakes may remain monolithic when they model one executable
protocol peer, but new behavior should be extracted when it becomes reusable or
when the single file obscures lifecycle and contract responsibilities.
