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
