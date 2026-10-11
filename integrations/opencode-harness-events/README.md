# OpenCode Harness Events

This package provides the ESM package boundary for the OpenCode harness events
plugin. It is managed entirely with Bun and is built as JavaScript plus
TypeScript declarations.

## Development

```sh
bun install --frozen-lockfile
bun run check
```

The package exposes separate commands for type checking, linting, formatting,
tests, building, and packing. The packed artifact contains only the built
entrypoint, declarations, manifest, README, and license.

## Runtime contract

The plugin invokes the external `harness-events` executable and does not carry
OpenCode CLI or iii runtime dependencies. OpenCode API packages are development
dependencies used for type checking at the adapter boundary.

## Opt-in context settings

Startup context is opt-in, configured per host process, and composed beside the
unchanged capture runtime. Retrieval joins the external `mcp-worker` executable
declared by `TOTAL_RECALL_CONTEXT_BIN`; that retrieval worker is never bundled
with this package, and invalid configuration disables retrieval with one
bounded diagnostic while capture continues.

| Variable | Default | Validation |
| --- | --- | --- |
| `TOTAL_RECALL_CONTEXT_BIN` | Absent means disabled | Enabled value is a non-blank absolute path |
| `TOTAL_RECALL_MEMORY_DATABASE` | Required when enabled | Non-blank target copied to the child |
| `TOTAL_RECALL_CONTEXT_LIMIT` | `10` | Integer `1..=20` |
| `TOTAL_RECALL_CONTEXT_MAX_FRAME_BYTES` | `65536` | Positive integer |
| `TOTAL_RECALL_CONTEXT_MAX_PENDING` | `16` | Positive integer |
| `TOTAL_RECALL_CONTEXT_MAX_INJECTION_BYTES` | `49152` | Positive integer |
| `TOTAL_RECALL_CONTEXT_DIAGNOSTIC_BYTES` | `512` | Positive integer |
| `TOTAL_RECALL_CONTEXT_STARTUP_TIMEOUT_MS` | `5000` | Integer `1..=30000` |
| `TOTAL_RECALL_CONTEXT_LOOKUP_TIMEOUT_MS` | `8000` | Integer `1..=30000` |
| `TOTAL_RECALL_CONTEXT_SHUTDOWN_TIMEOUT_MS` | `5000` | Integer `1..=30000` |

The retrieval child receives only the documented environment allowlist and
never inherits ambient parent values. Session deletion removes capture and
context state for that session, and disposal closes capture and context
independently under one shared outer bound leaving no owned child process.
