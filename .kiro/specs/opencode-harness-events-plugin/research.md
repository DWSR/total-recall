# Research and Gap Analysis: opencode-harness-events-plugin

## Current-State Delta

- The repository is a Rust 1.98.1 Cargo workspace with no TypeScript, Bun, OpenCode, or JavaScript package assets.
- `harness-events-cli` is approved but not implemented. Its design reserves `clients/harness-events-cli`, the `harness-events` binary, typed commands, exit statuses `0`, `2`, `3`, and `4`, and payload-free diagnostics.
- Existing integration runners use loopback protocol fakes. Requirement 9 instead needs a real iii 0.24.0 engine.
- `flake.nix` supplies iii 0.24.0 but not Bun or either OpenCode generation.
- CI uses one Nix-backed Cargo job with a 20-minute bound. Dependabot currently covers Cargo only.
- No reusable JavaScript tooling or package convention exists; the plugin package must establish one without turning the repository into a JavaScript monorepo.

## Requirement-to-Asset Map

| Requirements | Existing asset | Gap |
|---|---|---|
| 1 | OpenCode migration documentation | **Missing**: Dual v1/v2 entrypoint and adapters |
| 2–3 | Native OpenCode session/event APIs | **Missing**: Lifecycle tracking, accepted-event matrix, opaque envelope |
| 4–5 | Approved `harness-events-cli` process contract | **Constraint**: CLI is an external executable and is not implemented yet |
| 6, 8 | Nix development shell and CI gate patterns | **Missing**: Bun package, lockfile, TypeScript, Biome, package artifact |
| 7 | `.github/dependabot.yml` Cargo entry | **Missing**: Bun ecosystem entry; npm ecosystem is incorrect for `bun.lock` |
| 9 | iii 0.24.0 Nix package and Rust integration patterns | **Missing**: Real-engine Cargo test, capture worker, pinned OpenCode binaries, fixtures |

## OpenCode Compatibility Findings

- The official migration pattern supports one object with v1 `server()` and v2 `setup()` entrypoints. V1 versions before `1.18.29` cannot use this pattern.
- V1 packages are `opencode-ai` and `@opencode-ai/plugin`; v2 packages are `@opencode/cli` and `@opencode/plugin`.
- V1 configuration uses `plugin`; v2 uses `plugins`.
- V1 returns an `event({ event })` callback and keyed hooks. V2 registers hooks imperatively and exposes `ctx.event.subscribe()`.
- V1 native events use `{ type, properties }`; v2 events use an encoded envelope with `type`, `location`, and `data`.
- Streamed text and reasoning events are unsuitable for one-process-per-event dispatch.
- A resumed v1 session may not emit `session.created`; lazy lifecycle synthesis is required for correlation.
- V1 failed tool execution can omit `tool.execute.after`; the common event set cannot assume balanced tool hooks.
- V2 event streams are live-only and provide neither replay nor reconnection guarantees.
- Current researched pins are `opencode-ai@1.18.29` for the minimum v1 baseline and `@opencode/cli@2.0.10` for v2.

Sources:
- https://opencode.ai/v2/docs/build/plugins/migrate-v1/
- https://opencode.ai/docs/plugins/
- https://opencode.ai/v2/docs/build/plugins/
- https://opencode.ai/docs/server/
- https://opencode.ai/v2/docs/api/
- https://github.com/anomalyco/opencode/issues/39711
- https://github.com/anomalyco/opencode/issues/48085

## Bun and Package Tooling Findings

- Bun `1.4.2` is the current stable toolchain and exists in nixpkgs revision `4ba99f3209788ed04f01af38f77ddb803ad6ec63` for the repository's primary systems.
- `@biomejs/biome@2.5.14` supports Bun and is licensed `MIT OR Apache-2.0`.
- Biome can own formatting and strict lint checks with warnings treated as errors. TypeScript remains the type checker.
- Bun's bundler does not emit declarations. TypeScript can emit ESM JavaScript and declarations together, or emit declarations beside a Bun-built artifact.
- `bun pm pack` creates the artifact that clean consumer and end-to-end fixtures should install.
- GitHub Dependabot has a native `bun` package ecosystem for `package.json` plus text `bun.lock`. Its npm ecosystem does not update `bun.lock`.
- Dependabot's Bun integration requires Bun 1.1.39 or newer and does not provide security-update PRs.

Sources:
- https://github.com/oven-sh/bun/releases/tag/bun-v1.4.2
- https://github.com/biomejs/biome/releases/tag/%40biomejs%2Fbiome%402.5.14
- https://biomejs.dev/guides/getting-started/
- https://docs.github.com/en/code-security/dependabot/ecosystems-supported-by-dependabot/supported-ecosystems-and-repositories#bun
- https://bun.com/docs/bundler
- https://bun.com/docs/pm/cli/pm#pack

## Real-Engine End-to-End Findings

- A serial Cargo integration test can own the real iii engine, an in-process `iii-sdk` capture worker, and one headless OpenCode generation at a time.
- The capture worker can register `harness::session_start`, `harness::observation`, and `harness::session_end`, return `{"dispatched":true}`, and send captured calls to the test over a Tokio channel.
- iii 0.24.0 should start with an explicit temporary configuration and `--no-update-check`; the stale documented `--use-default-config` flag is rejected.
- Readiness should require SDK registration and `engine::functions::info` visibility for all capture functions, not a sleep or TCP-only probe.
- Provider-free coverage is possible by running `serve`, creating a session through the local API, waiting for creation events, and deleting it. No prompt generation or model credentials are required.
- V1 and v2 require separate isolated home, XDG, cache, config, state, and project directories.
- Build artifacts before `cargo test`; recursively invoking Cargo from a test risks target-lock contention.
- The Rust test should consume explicit paths to the built CLI and packed plugin artifact, then own TERM-to-kill cleanup for every child.

Sources:
- https://github.com/iii-hq/iii/blob/iii/v0.24.0/engine/src/main.rs
- https://iii.dev/docs/using-iii/engine
- https://iii.dev/docs/reference/sdk-rust
- https://opencode.ai/docs/server/
- https://opencode.ai/v2/docs/api/

## Implementation Options

### Option A: Extend Repository Tooling Only

Place plugin source among repository tooling and extend the root development environment.

- Pros: Few new top-level paths.
- Cons: `.opencode` is repository agent tooling, excludes package assets, and is not a product distribution boundary. This mixes local automation with the shipped integration.

### Option B: Standalone Integration Package and Test Crate

Create one package under `integrations/` and one Cargo test crate under `integration/`; extend only shared Nix, CI, Dependabot, and Cargo files.

- Pros: Matches the product boundary, keeps Bun state local, tests the packed artifact, and preserves Rust integration orchestration conventions.
- Cons: Establishes a new plural `integrations/` product directory beside singular `integration/` test infrastructure.

### Option C: One Cargo Crate Containing Package and E2E Assets

Nest the TypeScript package and fixtures under a Cargo integration crate.

- Pros: One apparent feature directory.
- Cons: Cargo becomes the container for a published Bun package, package paths become awkward, and product code is coupled to test orchestration.

## Complexity and Risks

- **Effort: L (1–2 weeks)**. The dispatcher is small, but two external plugin APIs, package bootstrapping, Nix pins, and real-process tests create multiple integration surfaces.
- **Risk: Medium**. Authoritative migration and headless APIs exist; the main risks are external CLI drift, child-process cleanup, and the unimplemented upstream CLI.
- **Blocked dependency**: End-to-end completion depends on implementation of `harness-events-cli`.
- **Compatibility risk**: Dependabot can update v2 dependencies beyond the tested pin; compatibility updates must include E2E pin review.
- **CI risk**: Two OpenCode scenarios plus artifact builds may approach the existing 20-minute timeout.
- **Platform risk**: Pinned native OpenCode and Bun packages need explicit supported-system coverage in Nix.
- **Process-tree risk**: Headless OpenCode can spawn descendants; Linux live tests need process-group ownership plus an unwind-safe kill fallback.

## Design Inputs

- Keep the package standalone and the E2E runner separate.
- Keep native v1/v2 types inside their adapters; share only normalized submission values.
- Test the packed artifact rather than source-directory imports.
- Use one real iii engine and one capture worker per serial test run.
- Make executable path and timeout configurable without adding retry or delivery state.
- Define the accepted event matrix and diagnostic fields explicitly in design.
- Treat the CLI and OpenCode pins as upstream prerequisites, not plugin-owned distributions.

## Design Decisions

### Standalone Package and Cargo Test Crate

- **Selected**: Option B from Implementation Options.
- **Rationale**: The shipped Bun artifact and Rust process orchestrator have separate consumers, tools, and review boundaries.
- **Trade-off**: The repository gains `integrations/` for product adapters while `integration/` remains test infrastructure.

### Plain Dual Entrypoint

- **Selected**: Export one plain object with v1 `server` and v2 `setup`; import both plugin APIs as types only.
- **Rationale**: This follows the official migration contract and leaves the package with no production runtime dependency on either OpenCode CLI or plugin package.
- **Rejected**: `Plugin.define` adds a v2 runtime import without changing the object contract; separate packages duplicate package and release work.

### Native Event Envelope

- **Selected**: Wrap each accepted native payload with source generation and native kind, then pass it unchanged as JSON-compatible content.
- **Rationale**: Consumers can interpret source-specific data without a semantic translation layer.
- **Rejected**: A canonical cross-generation event schema would make this plugin own OpenCode semantics and increase drift risk.

### Per-Session Runtime Chain

- **Selected**: Track active sessions and serialize ordinary start, observation, and deletion-end submissions through an in-memory promise chain. Cleanup aborts ordinary chains before dedicated shutdown-end attempts.
- **Rationale**: This guarantees synthesized start precedes observation and deletion observation precedes its end without allowing queued work to starve shutdown ends.
- **Trade-off**: Ordering exists only within one plugin runtime; shutdown may discard queued observations, and restarts or concurrent processes can still produce gaps or duplicates.

### TypeScript Emission Under Bun Scripts

- **Selected**: Use strict TypeScript to emit ESM JavaScript and declarations; Bun owns installation, script execution, testing, locking, and packing.
- **Rationale**: One compiler output prevents JavaScript/declaration export drift and avoids an unnecessary bundling phase.
- **Rejected**: Bun build plus separate declaration emission creates two output paths for a package with no runtime dependencies.

### Packed-Artifact Live Test

- **Selected**: Build and pack before Cargo E2E execution; install the tarball into isolated fixtures and load it in both OpenCode generations.
- **Rationale**: This verifies the distributable files and exports while keeping the test registry-independent.
- **Trade-off**: Remote registry installation remains outside scope.

### Verification Platform

- **Selected**: Run live verification on x86-64 Linux and expose Bun/OpenCode Nix tools only on their upstream-supported systems.
- **Rationale**: Bun 1.4.2 is unavailable for Intel macOS; conditional packages preserve the existing Rust-only `x86_64-darwin` shell without claiming an unsupported toolchain.
- **Trade-off**: Intel macOS does not run the Bun package or OpenCode live matrix from this flake.
