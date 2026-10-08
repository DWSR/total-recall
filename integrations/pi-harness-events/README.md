# Pi Harness Events

`@dwsr/pi-harness-events` is an MIT-licensed ESM Pi extension that submits
selected Pi lifecycle and coarse observation events through the
`harness-events` child-process contract.

## Requirements and Loading

The source package supports Node `^24.0.0`. Its package manifest declares
`pi.extensions` as `["./src/index.ts"]`; Pi reads the manifest from the
extension package root and uses its host-provided Jiti loader to load
`src/index.ts`.

Install it with the npm dependency management used by your Pi environment, then
give Pi the installed package root, not `src/index.ts`, as an explicit extension:

```sh
pi -e /absolute/path/to/node_modules/@dwsr/pi-harness-events
```

The archive contains package metadata, TypeScript source, this README, and the
license. It ships no bundled JavaScript or runtime Jiti dependency, and it does
not provide a global installer or an `npx` command.

## Configuration

The child inherits the extension process environment. Keep `III_URL` and
`III_NAMESPACE` available for the `harness-events` CLI; the extension preserves
them unchanged.

| Setting                              | Default                                         | Valid value                                                              |
| ------------------------------------ | ----------------------------------------------- | ------------------------------------------------------------------------ |
| `HARNESS_EVENTS_BIN`                 | `harness-events`                                | Non-blank executable name or path                                        |
| `HARNESS_EVENTS_TIMEOUT_MS`          | `35,000 ms`                                     | Integer milliseconds from `2,000` through `120,000`                      |
| `HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS` | `5,000 ms`, or the execution timeout when lower | Integer milliseconds from `1,500` through the resolved execution timeout |
| `HARNESS_EVENTS_QUEUE_CAPACITY`      | `256` observations                              | Integer from `1` through `4,096`                                         |

Invalid values use their safe resolved default and emit a configuration diagnostic.

## Privacy and Diagnostics

Selected lifecycle and coarse data are delivered only to the configured child.
Lifecycle metadata is command arguments; each coarse observation is one JSON
object on child standard input. Event payloads do not include provider requests
or responses, provider credentials, system prompts, images, thinking content,
process environment data, or unrelated Pi configuration.

Accepted user and assistant text, tool arguments, and tool results are unredacted
and not secret-scanned before being sent to child standard input, so operators
must trust their configured iii destination. Diagnostics are bounded, payload-free
`console.error` records: they exclude native payloads, serialized standard input,
child output, credentials, and environment values.

## Delivery Semantics

The extension invokes child commands with an argument array and no shell. A
bounded, serial, in-memory queue preserves local accepted-work order while the
runtime is alive. It provides no retry, durable delivery, replay, exactly-once,
cross-process ordering, or deduplication guarantee. A CLI exit status of `0`
acknowledges only the invocation, not durable persistence.

Shutdown uses the configured finite deadline. When it expires, pending work is
discarded and an active child is terminated.

## Development Checks

Use the repository's `nix develop .#node24` shell, which provides Node 24 and
npm 12.2.0. npm 12 honors the dependency override needed to replace Pi's
shrinkwrapped `brace-expansion` with the patched version.

From this package directory inside that shell, run:

```sh
npm ci
npm run typecheck
npm run lint
npm run format:check
npm run test
npm run pack:check
npm run check
```

`npm run pack:check` creates a tarball, validates its source-only allowlist, and
installs it into an isolated offline consumer with scripts and peer installation
disabled.
