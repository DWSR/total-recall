# Research and Gap Analysis: pi-harness-events-extension

## Validated External Facts

- The maintained Pi package is `@earendil-works/pi-coding-agent`; `0.86.1` declares Node `>=22.19.0`, depends on Jiti `2.7.0`, and accepts Node 24. [package](https://github.com/earendil-works/pi/blob/v0.86.1/packages/coding-agent/package.json)
- Pi loads `.ts` extensions through Jiti, requires a default extension factory, and awaits asynchronous factories. [loader](https://github.com/earendil-works/pi/blob/v0.86.1/packages/coding-agent/src/core/extensions/loader.ts)
- A Pi package declares `"pi": { "extensions": ["./src/index.ts"] }`; runtime dependencies use `dependencies`, while Pi APIs remain unbundled peers. [package docs](https://github.com/earendil-works/pi/blob/v0.86.1/packages/coding-agent/docs/packages.md)
- Node 24 is Active LTS on 2026-09-20. Node 26 is Current and is not supported until it reaches LTS. [Node schedule](https://github.com/nodejs/Release#release-schedule)
- Pi exposes `session_start`, `session_shutdown`, agent, turn, message, tool, compaction, and selection events. Handler failures normally fail open, but `tool_call` and `user_bash` require explicit containment. [extension types](https://github.com/earendil-works/pi/blob/v0.86.1/packages/coding-agent/src/core/extensions/types.ts)
- `ctx.sessionManager.getSessionId()` and `ctx.cwd` provide extension-safe identity and directory values. Initial resumed sessions still report `session_start.reason === "startup"`; persisted entries distinguish resume state. [session format](https://github.com/earendil-works/pi/blob/v0.86.1/packages/coding-agent/docs/session-format.md)
- `--print` and `--mode json` are headless. `--offline`, `--no-extensions -e`, `--no-approve`, `--no-session`, and resource-disable flags support isolated runtime tests. Offline mode disables Pi startup network work but is not a process network sandbox. [usage](https://github.com/earendil-works/pi/blob/v0.86.1/packages/coding-agent/docs/usage.md)
- A test-only provider registered through `pi.registerProvider()` can produce deterministic agent, message, and tool activity without a model request. [custom providers](https://github.com/earendil-works/pi/blob/v0.86.1/packages/coding-agent/docs/custom-provider.md)
- Pi's upstream CI tests Node 22, not Node 24. This repository must provide the Node 24 compatibility proof.
- `prettier@3.9.8` supports Node `>=14` and checks Markdown formatting; it covers the file type that Biome `2.5.14` does not process.

## Current-State Delta

- The repository contains Rust workers and integration fakes but no product TypeScript package, Pi dependency, Node runtime, npm lockfile, or headless Pi fixture.
- `harness-events-cli` is approved but unimplemented; its missing binary blocks live E2E execution, not package or harness construction.
- `opencode-harness-events-plugin` is approved but unimplemented. Its Bun package and OpenCode-specific runtime are not reusable for an npm/Jiti Pi package.
- `flake.nix` supplies Rust, protobuf, actionlint, and iii `0.24.0` on four systems; it supplies no Node or npm.
- CI has one 20-minute Ubuntu/Nix job. Dependabot monitors only Cargo.
- Existing Rust tests establish exact dependency pins, semantic readiness checks, explicit deadlines, payload-safe failures, and process cleanup conventions.
- No `.kiro/steering/` context exists; approved adjacent specifications and implemented worker contracts are the available architecture guidance.

## Requirement-to-Asset Map

| Requirements | Existing asset | Gap |
|---|---|---|
| 1 | Pi `0.86.1` Jiti and manifest contracts | Package, Node 24 proof, LTS matrix |
| 2–3 | Pi native event and session APIs | Event matrix, lifecycle state, JSON filtering |
| 4 | Approved CLI grammar and ingestion protobuf | Process adapter, metadata derivation, stdin |
| 5 | Existing deadline and cleanup patterns | Capacity-bounded serial queue and lifecycle reservation |
| 6 | Existing payload-safe diagnostics conventions | Hook containment, exit classification, output bounds |
| 7–9 | npm ecosystem and repository test conventions | Product package, lockfile, strict checks, unit seams |
| 8 | Existing weekly Cargo Dependabot entry | Scoped weekly npm entry |
| 10 | iii Nix pin and protocol fakes | Real-engine capture worker, Pi fixture, Node matrix |

## Integration Points

- Product package candidate: `integrations/pi-harness-events/`.
- Private Cargo test crate candidate: `integration/pi-harness-events-e2e/`.
- Workspace changes: `Cargo.toml`, `Cargo.lock`, `flake.nix`, `.gitignore`, `.github/dependabot.yml`, and `.github/workflows/ci.yml`.
- The E2E capture functions must match the three function IDs and success shape owned by `harness-ingestion-worker` and `harness-events-cli`.
- Runtime and package code must not import iii SDK, protobuf packages, repository workers, OpenCode code, or Rust test support.

## Organization Options

### Option A: Independent Package and E2E Crate

- Keep Pi product code under `integrations/` and Pi process orchestration under `integration/`.
- Preserves current repository boundaries and avoids changing the approved OpenCode design.
- Duplicates some real-iii capture and process-guard logic until a separate shared-test-infrastructure specification exists.
- Effort: L. Risk: Medium.

### Option B: Independent Package with Shared Adapter Test Infrastructure

- Add a host-neutral Cargo support crate used by Pi and the future OpenCode E2E crate.
- Reduces duplicate capture and cleanup code.
- Expands this feature into a shared boundary and requires revalidation of the approved OpenCode specification.
- Effort: L. Risk: Medium–High.

### Option C: Co-located npm Package and Cargo Test Crate

- Keep all Pi package and test assets under one integration subtree.
- Improves feature discoverability.
- Mixes publishable and test-only assets, complicates npm archive allowlisting, and diverges from the repository's `integration/` convention.
- Effort: L. Risk: Medium.

## Design Inputs

- Define the exact accepted Pi `0.86.1` event matrix, including error representation and fields removed before serialization.
- Define queue capacity, lifecycle reservation, overflow handling, execution timeout, shutdown timeout, output capture, and diagnostic limits.
- Choose npm-compatible strict TypeScript, test, lint, and formatting versions that support Node 24.
- Encode Node support as explicit LTS majors: `24.x` initially; add later majors only after LTS entry and full matrix success.
- Load the package root in E2E to verify its Pi manifest and Jiti entrypoint, rather than loading `src/index.ts` directly.
- Provision dependencies before Cargo tests; runtime-offline E2E does not imply an offline npm installation.
- Use Pi's pinned local binary directly rather than `npx` to prevent registry fallback.
- Keep live-test enablement and CI gating dependent on the upstream `harness-events` binary.

## Design Decisions

- **Independent boundaries**: Select Option A. The npm/Jiti package and Pi-specific Cargo E2E crate remain separate; shared adapter infrastructure is deferred.
- **Native loading**: Ship TypeScript source through `pi.extensions`; do not bundle JavaScript or add Jiti as a dependency.
- **Toolchain**: Use npm, TypeScript `7.0.2`, Node `node:test`, `@types/node` `24.13.6`, Biome `2.5.14` for source/test/JSON checks, and Prettier `3.9.8` for Markdown formatting checks. Node's type stripping requires erasable TypeScript syntax and explicit `.ts` test imports.
- **Event projection**: Subscribe only to completed or coarse status events. Exclude interception hooks, streaming updates, provider traffic, system prompts, image blocks, thinking blocks, and duplicate embedded messages or tool results.
- **Dispatch model**: Use one runtime-local FIFO dispatcher with capacity for 256 observations plus one start and one end command. Drop a new observation at saturation; never displace lifecycle commands.
- **Process limits**: Bound ordinary child execution to 35 seconds with a 2-second minimum. Use one 5-second absolute shutdown deadline with a 1.5-second minimum; reserve its final 1 second for termination, including 500 milliseconds before kill escalation and 500 milliseconds for reaping. Bound captured child stderr to 8 KiB and diagnostics to 512 characters.
- **Artifact proof**: Pack a real npm tarball, install it into isolated staged state before Cargo runs, and load the installed package root through Pi's manifest and Jiti.
- **Runtime proof**: Run the full npm and live E2E gates on Node `24.x`; add later even-numbered majors only after official LTS entry and successful full-matrix validation.

## Risks and Mitigations

- Pi is pre-1.0 and does not test Node 24 upstream — exact pinning and package-root E2E detect drift.
- Jiti and Node's test-time TypeScript handling differ — Jiti loading is verified in live E2E, while `tsc --noEmit` verifies source types.
- Accepted user and tool payloads can contain secrets — no diagnostic includes payloads, and documentation identifies iii as a trusted destination.
- Pi offline mode is not a network sandbox — the scripted provider performs no I/O and CI makes only a runtime-offline claim.
- The CLI prerequisite is absent — package and harness tasks proceed independently; live-test enablement depends on the CLI task boundary.
- Adjacent unimplemented specs touch the same root files — shared-file edits remain additive, E2E-only Tokio features stay crate-local, and Pi CI uses a separate matrix job.
