// biome-ignore-all lint/style/useNamingConvention: These keys are external environment names.
// biome-ignore-all lint/complexity/noExcessiveLinesPerFunction: The test fixture models one child process boundary.
// biome-ignore-all lint/style/noExcessiveLinesPerFile: Readiness and lifecycle tests share the worker fixture.
// biome-ignore-all lint/correctness/useQwikValidLexicalScope: This test file has no Qwik components.
// biome-ignore-all lint/style/useBlockStatements: Test preconditions use concise failure guards.
// biome-ignore-all lint/performance/noAwaitInLoops: Protocol scenarios are tested deterministically in sequence.
// biome-ignore-all lint/complexity/noExcessiveCognitiveComplexity: The fake worker models multiple protocol paths.
// biome-ignore-all lint/complexity/useLiteralKeys: The fixture dynamically converts RPC success into an error.
// biome-ignore-all lint/performance/noDelete: The fixture removes mutually exclusive JSON-RPC response fields.
import { expect, test } from "bun:test";
// biome-ignore lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
import { createContextClient } from "../src/context-client";
// biome-ignore lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
import { fromValues } from "../src/context-config";

const config = fromValues({
  TOTAL_RECALL_CONTEXT_BIN: "/absolute/worker",
  TOTAL_RECALL_MEMORY_DATABASE: "memory-db",
});
const SPAWN_DELAY_OVER_DEADLINE_MS = 20;
const REPLACEMENT_LOOKUP_TIMEOUT_MS = 1000;
const SHUTDOWN_BOUND_SLACK_MS = 100;
const CONTEXT_FRAME_TEST_CONTENT_BYTES = 34_000;
const QUEUED_PARTIAL_BOUNDARY_CONTENT_BYTES = 65_420;

interface FakeRequest {
  readonly id?: string;
  readonly method?: string;
  readonly params?: unknown;
}

function tool(
  name: string,
  inputSchema: unknown = {
    type: "object",
    properties: {},
    required: [],
    additionalProperties: false,
  },
  outputSchema?: unknown,
) {
  if (outputSchema === undefined) {
    return { name, inputSchema };
  }
  return { name, inputSchema, outputSchema };
}

const contextSchema = {
  type: "object",
  properties: {
    memories: {
      type: "array",
      items: {
        type: "object",
        properties: {
          type: { type: "string" },
          title: { type: "string" },
          content: { type: "string" },
          concepts: { type: "array", items: { type: "string" } },
        },
        required: ["type", "title", "content", "concepts"],
        additionalProperties: false,
      },
    },
  },
  required: ["memories"],
  additionalProperties: false,
};
const contextInputSchema = {
  type: "object",
  properties: { limit: { type: "integer", minimum: 1, maximum: 50 } },
  required: [],
  additionalProperties: false,
};

function mockWorker(options: {
  readonly serverName?: string;
  readonly protocolVersion?: string;
  readonly inputSchema?: unknown;
  readonly schema?: unknown;
  readonly holdResponses?: boolean;
  readonly backendError?: boolean;
  readonly holdReadiness?: boolean;
  readonly readinessRpcError?: boolean;
  readonly blockFirstWrite?: boolean;
  readonly failSpawn?: boolean;
  readonly onSpawn?: () => void;
  readonly exitOnTerm?: boolean;
  readonly neverExitOnKill?: boolean;
  readonly holdStderrDrain?: boolean;
  readonly holdStdoutOnEnd?: boolean;
  readonly contextResult?: unknown;
  readonly responseMode?:
    | "fragmented"
    | "coalesced"
    | "queued-plus-partial"
    | "reverse-order"
    | "malformed"
    | "partial-eof"
    | "unknown-id";
}) {
  const requests: FakeRequest[] = [];
  let stderrDrained = false;
  let spawnOptions: unknown;
  let stdoutController: ReadableStreamDefaultController<Uint8Array>;
  let killed = false;
  const signals: Array<number | NodeJS.Signals | undefined> = [];
  let stdinClosed = false;
  let queuedOutput = "";
  let firstCallFrame = "";
  let resolveFirstWrite: (() => void) | undefined;
  let resolveExit: ((code: number) => void) | undefined;
  let resolveStderrDrain: (() => void) | undefined;
  const child = {
    stdin: {
      write: (text: string): void | Promise<void> => {
        const request = JSON.parse(text) as FakeRequest;
        requests.push(request);
        const { method } = request;
        if (options.blockFirstWrite && requests.length === 1) {
          return new Promise<void>((resolve) => {
            resolveFirstWrite = resolve;
          });
        }
        if (
          options.holdReadiness ||
          (options.holdResponses && method === "tools/call")
        ) {
          return;
        }
        let result: unknown;
        if (method === "server/discover") {
          result = {
            _meta: {
              "io.modelcontextprotocol/protocolVersion":
                options.protocolVersion ?? "2026-07-28",
              "io.modelcontextprotocol/serverInfo": {
                name: options.serverName ?? "total-recall-mcp",
                version: "1.0.0",
              },
            },
          };
        } else if (method === "tools/call") {
          const limit = (request.params as { arguments?: { limit?: number } })
            .arguments?.limit;
          let content = "body";
          if (options.responseMode === "reverse-order") content = String(limit);
          result = options.contextResult ?? {
            memories: [
              {
                type: "fact",
                title: "title",
                content,
                concepts: ["one"],
              },
            ],
          };
        } else {
          result = {
            tools: [
              tool(
                "memory_context",
                options.inputSchema ?? contextInputSchema,
                options.schema ?? contextSchema,
              ),
            ],
          };
        }
        const rpcResponse = {
          jsonrpc: "2.0",
          id: request.id,
          result,
        } as Record<string, unknown>;
        if (options.readinessRpcError && method !== "tools/call") {
          delete rpcResponse["result"];
          rpcResponse["error"] = { code: -1, message: "private" };
        }
        if (options.backendError && method === "tools/call") {
          delete rpcResponse["result"];
          rpcResponse["error"] = { code: -1, message: "private" };
        }
        let frame = `${JSON.stringify(rpcResponse)}\n`;
        if (options.responseMode === "unknown-id" && method === "tools/call")
          frame = `${JSON.stringify({ jsonrpc: "2.0", id: "unknown-live-id", result })}\n`;
        if (options.responseMode === "malformed" && method === "tools/call")
          frame = "{not-json}\n";
        if (options.responseMode === "partial-eof" && method === "tools/call") {
          stdoutController.enqueue(
            new TextEncoder().encode(frame.slice(0, -1)),
          );
          stdoutController.close();
          return;
        }
        if (options.responseMode === "coalesced" && method === "tools/call") {
          queuedOutput += frame;
          if (requests.at(-2)?.method === "tools/call") {
            stdoutController.enqueue(new TextEncoder().encode(queuedOutput));
            queuedOutput = "";
          }
          return;
        }
        if (
          options.responseMode === "reverse-order" &&
          method === "tools/call"
        ) {
          if (
            requests.filter((item) => item.method === "tools/call").length === 1
          ) {
            firstCallFrame = frame;
            return;
          }
          stdoutController.enqueue(
            new TextEncoder().encode(`${frame}${firstCallFrame}`),
          );
          return;
        }
        if (
          options.responseMode === "queued-plus-partial" &&
          method === "tools/call"
        ) {
          stdoutController.enqueue(
            new TextEncoder().encode(`${frame}{"partial":`),
          );
          return;
        }
        if (options.responseMode === "fragmented" && method === "tools/call") {
          const bytes = new TextEncoder().encode(frame);
          const split = Math.floor(bytes.length / 2);
          stdoutController.enqueue(bytes.slice(0, split));
          stdoutController.enqueue(bytes.slice(split));
          return;
        }
        stdoutController.enqueue(new TextEncoder().encode(frame));
      },
      end: () => {
        stdinClosed = true;
        if (!options.holdStdoutOnEnd) stdoutController.close();
      },
    },
    stdout: new ReadableStream<Uint8Array>({
      start(controller) {
        stdoutController = controller;
      },
    }),
    stderr: new ReadableStream<Uint8Array>(
      {
        async pull(controller) {
          if (options.holdStderrDrain) {
            await new Promise<void>((resolve) => {
              resolveStderrDrain = () => {
                controller.close();
                resolve();
              };
            });
            return;
          }
          stderrDrained = true;
          controller.enqueue(new TextEncoder().encode("private stderr"));
          controller.close();
        },
        cancel() {
          stderrDrained = true;
        },
      },
      { highWaterMark: 0 },
    ),
    exited: new Promise<number>((resolve) => {
      resolveExit = resolve;
    }),
    kill: (signal?: number | NodeJS.Signals) => {
      signals.push(signal);
      killed = true;
      if (
        (signal === "SIGKILL" && !options.neverExitOnKill) ||
        (signal === "SIGTERM" && options.exitOnTerm)
      ) {
        resolveExit?.(1);
      }
    },
  };
  return {
    requests,
    child,
    get spawnOptions() {
      return spawnOptions;
    },
    get stderrDrained() {
      return stderrDrained;
    },
    get killed() {
      return killed;
    },
    get signals() {
      return signals;
    },
    get stdinClosed() {
      return stdinClosed;
    },
    resolveFirstWrite() {
      resolveFirstWrite?.();
    },
    resolveStderrDrain() {
      resolveStderrDrain?.();
    },
    sendRaw(text: string) {
      stdoutController.enqueue(new TextEncoder().encode(text));
    },
    errorStdout() {
      stdoutController.error(new Error("private stream failure"));
    },
    closeStdout() {
      stdoutController.close();
    },
    exit() {
      resolveExit?.(1);
      try {
        stdoutController.close();
      } catch {
        // An already-closed stream still represents a completed fake exit.
      }
    },
    respond(id: string, content: string) {
      stdoutController.enqueue(
        new TextEncoder().encode(
          `${JSON.stringify({
            jsonrpc: "2.0",
            id,
            result: {
              memories: [
                { type: "fact", title: "title", content, concepts: ["one"] },
              ],
            },
          })}\n`,
        ),
      );
    },
    spawn: (spawnOptionsValue: unknown) => {
      spawnOptions = spawnOptionsValue;
      if (options.failSpawn) {
        throw new Error("private spawn detail");
      }
      options.onSpawn?.();
      return child;
    },
  };
}

test("starts enabled worker without a shell and completes metadata-bearing discovery and listing", async () => {
  if (config.mode !== "enabled") {
    throw new Error("expected enabled settings");
  }
  const fake = mockWorker({});
  const client = createContextClient(config, fake.spawn as never);
  expect(await client.start()).toEqual({ kind: "ready" });
  expect(fake.requests.map((request) => request.method)).toEqual([
    "server/discover",
    "tools/list",
  ]);
  expect(
    fake.requests.map(
      (request) => (request.params as { _meta: Record<string, unknown> })._meta,
    ),
  ).toEqual([
    {
      "io.modelcontextprotocol/protocolVersion": "2026-07-28",
      "io.modelcontextprotocol/clientCapabilities": {},
    },
    {
      "io.modelcontextprotocol/protocolVersion": "2026-07-28",
      "io.modelcontextprotocol/clientCapabilities": {},
    },
  ]);
  await client.stderrDrained;
  expect(fake.stderrDrained).toBe(true);
  expect(fake.spawnOptions).toEqual({
    cmd: ["/absolute/worker"],
    env: config.childEnvironment,
    stdin: "pipe",
    stdout: "pipe",
    stderr: "pipe",
  });
  expect(JSON.stringify(fake.spawnOptions)).not.toContain("PATH");
});

test("close is idempotent, closes stdin, settles pending calls, and escalates to KILL", async () => {
  if (config.mode !== "enabled") throw new Error("expected enabled settings");
  const fake = mockWorker({ holdResponses: true });
  const client = createContextClient(config, fake.spawn as never);
  const request = client.request(1);
  while (!fake.requests.some((item) => item.method === "tools/call"))
    await new Promise((resolve) => setTimeout(resolve, 0));
  const firstClose = client.close();
  expect(await request).toEqual({
    kind: "failure",
    category: "context_startup_failed",
  });
  await client.close();
  await firstClose;
  expect(fake.stdinClosed).toBe(true);
  expect(fake.signals).toEqual(["SIGTERM", "SIGKILL"]);
  expect(client.process).toBeUndefined();
  expect(await client.request(1)).toEqual({
    kind: "failure",
    category: "context_startup_failed",
  });
  expect(fake.signals).toEqual(["SIGTERM", "SIGKILL"]);
});

test("keeps one stdout reader while idle and drains it through EOF during close", async () => {
  if (config.mode !== "enabled") throw new Error("expected enabled settings");
  const fake = mockWorker({});
  const client = createContextClient(config, fake.spawn as never);
  await client.start();
  expect(() => fake.child.stdout.getReader()).toThrow();
  await client.close();
  expect(() => fake.child.stdout.getReader()).not.toThrow();
  expect(client.process).toBeUndefined();
});

test("close discards malformed stdout with the persistent reader through EOF", async () => {
  if (config.mode !== "enabled") throw new Error("expected enabled settings");
  const fake = mockWorker({ holdStdoutOnEnd: true });
  const client = createContextClient(config, fake.spawn as never);
  await client.start();
  const close = client.close();
  fake.sendRaw("{not-json}\\n");
  fake.closeStdout();
  fake.exit();
  await close;
  expect(() => fake.child.stdout.getReader()).not.toThrow();
  expect(client.process).toBeUndefined();
});

test("close fails when stdout errors before EOF can be confirmed", async () => {
  if (config.mode !== "enabled") throw new Error("expected enabled settings");
  const boundedConfig = fromValues({
    TOTAL_RECALL_CONTEXT_BIN: "/absolute/worker",
    TOTAL_RECALL_MEMORY_DATABASE: "memory-db",
    TOTAL_RECALL_CONTEXT_SHUTDOWN_TIMEOUT_MS: "30",
  });
  if (boundedConfig.mode !== "enabled")
    throw new Error("expected enabled settings");
  const fake = mockWorker({ holdStdoutOnEnd: true });
  const client = createContextClient(boundedConfig, fake.spawn as never);
  await client.start();
  const close = client.close();
  fake.errorStdout();
  fake.exit();
  await expect(close).rejects.toThrow("context_shutdown_failed");
  expect(() => fake.child.stdout.getReader()).not.toThrow();
});

test("close reports bounded failure and retains ownership until delayed exit", async () => {
  const boundedConfig = fromValues({
    TOTAL_RECALL_CONTEXT_BIN: "/absolute/worker",
    TOTAL_RECALL_MEMORY_DATABASE: "memory-db",
    TOTAL_RECALL_CONTEXT_SHUTDOWN_TIMEOUT_MS: "30",
  });
  if (boundedConfig.mode !== "enabled")
    throw new Error("expected enabled settings");
  const fake = mockWorker({ holdResponses: true, neverExitOnKill: true });
  const client = createContextClient(boundedConfig, fake.spawn as never);
  const request = client.request(1);
  while (!fake.requests.some((item) => item.method === "tools/call"))
    await new Promise((resolve) => setTimeout(resolve, 0));
  const startedAt = Date.now();
  const close = client.close();
  await expect(close).rejects.toThrow("context_shutdown_failed");
  expect(Date.now() - startedAt).toBeLessThan(SHUTDOWN_BOUND_SLACK_MS);
  expect(fake.signals).toEqual(["SIGTERM", "SIGKILL"]);
  expect(client.process).toBeDefined();
  expect(await request).toEqual({
    kind: "failure",
    category: "context_startup_failed",
  });
  expect(client.close()).toBe(close);
  fake.exit();
  await fake.child.exited;
  await new Promise((resolve) => setTimeout(resolve, 0));
  expect(client.process).toBeUndefined();
});

test("close fails by its deadline when an exited child's pipe drain is pending", async () => {
  const boundedConfig = fromValues({
    TOTAL_RECALL_CONTEXT_BIN: "/absolute/worker",
    TOTAL_RECALL_MEMORY_DATABASE: "memory-db",
    TOTAL_RECALL_CONTEXT_SHUTDOWN_TIMEOUT_MS: "30",
  });
  if (boundedConfig.mode !== "enabled")
    throw new Error("expected enabled settings");
  const fake = mockWorker({ exitOnTerm: true, holdStderrDrain: true });
  const client = createContextClient(boundedConfig, fake.spawn as never);
  await client.start();
  const startedAt = Date.now();
  await expect(client.close()).rejects.toThrow("context_shutdown_failed");
  expect(Date.now() - startedAt).toBeLessThan(SHUTDOWN_BOUND_SLACK_MS);
  expect(client.process).toBeUndefined();
  fake.resolveStderrDrain();
});

test("close stays bounded during a pending readiness write and ignores its late resolution", async () => {
  const boundedConfig = fromValues({
    TOTAL_RECALL_CONTEXT_BIN: "/absolute/worker",
    TOTAL_RECALL_MEMORY_DATABASE: "memory-db",
    TOTAL_RECALL_CONTEXT_SHUTDOWN_TIMEOUT_MS: "30",
  });
  if (boundedConfig.mode !== "enabled")
    throw new Error("expected enabled settings");
  const fake = mockWorker({ blockFirstWrite: true, neverExitOnKill: true });
  const client = createContextClient(boundedConfig, fake.spawn as never);
  const starting = client.start();
  while (fake.requests.length === 0)
    await new Promise((resolve) => setTimeout(resolve, 0));

  const startedAt = Date.now();
  const close = client.close();
  await expect(close).rejects.toThrow("context_shutdown_failed");
  expect(Date.now() - startedAt).toBeLessThan(SHUTDOWN_BOUND_SLACK_MS);
  expect(client.process).toBeDefined();
  expect(fake.requests.map((request) => request.method)).toEqual([
    "server/discover",
  ]);

  fake.resolveFirstWrite();
  expect(await starting).toEqual({
    kind: "failure",
    category: "context_startup_failed",
  });
  await new Promise((resolve) => setTimeout(resolve, 0));
  expect(fake.requests.map((request) => request.method)).toEqual([
    "server/discover",
  ]);
  expect(client.process).toBeDefined();
  expect(client.close()).toBe(close);

  fake.exit();
  await fake.child.exited;
  await new Promise((resolve) => setTimeout(resolve, 0));
  expect(client.process).toBeUndefined();
});

test("close during replacement readiness settles startup and releases child ownership", async () => {
  if (config.mode !== "enabled") throw new Error("expected enabled settings");
  const shortConfig = {
    ...config,
    settings: { ...config.settings, lookupTimeoutMs: 15 },
  };
  const first = mockWorker({ holdResponses: true });
  const replacement = mockWorker({ holdReadiness: true });
  let spawns = 0;
  const client = createContextClient(shortConfig, ((options: never) => {
    spawns += 1;
    if (spawns === 1) return first.spawn(options);
    return replacement.spawn(options);
  }) as never);
  expect((await client.request(1)).kind).toBe("failure");
  const pendingLookup = client.request(1);
  while (replacement.requests.length === 0)
    await new Promise((resolve) => setTimeout(resolve, 0));
  const close = client.close();
  expect(client.close()).toBe(close);
  expect(await pendingLookup).toEqual({
    kind: "failure",
    category: "context_startup_failed",
  });
  await close;
  expect(replacement.signals).toContain("SIGKILL");
  expect(client.process).toBeUndefined();
  expect(() => replacement.child.stdout.getReader()).not.toThrow();
});

test("close handles ready, failed, restarting, and already-exited clients", async () => {
  if (config.mode !== "enabled") throw new Error("expected enabled settings");
  const ready = mockWorker({ exitOnTerm: true });
  const readyClient = createContextClient(config, ready.spawn as never);
  await readyClient.start();
  await readyClient.close();
  expect(ready.signals).toEqual(["SIGTERM"]);
  expect(readyClient.process).toBeUndefined();

  const alreadyExited = mockWorker({ holdReadiness: false });
  const alreadyExitedClient = createContextClient(
    config,
    alreadyExited.spawn as never,
  );
  await alreadyExitedClient.start();
  alreadyExited.exit();
  await alreadyExited.child.exited;
  await alreadyExitedClient.close();
  expect(alreadyExitedClient.process).toBeUndefined();

  const failed = mockWorker({ serverName: "wrong" });
  const failedClient = createContextClient(config, failed.spawn as never);
  await failedClient.start();
  await failedClient.close();
  expect(failed.signals).toEqual(["SIGKILL"]);

  const exited = mockWorker({ holdReadiness: true });
  const exitedClient = createContextClient(config, exited.spawn as never);
  const starting = exitedClient.start();
  await new Promise((resolve) => setTimeout(resolve, 0));
  exited.exit();
  await starting;
  await exitedClient.close();
  expect(exited.signals).toEqual([]);
  expect(exitedClient.process).toBeUndefined();
});

test("maps spawn failure and readiness mismatches to fixed startup failure", async () => {
  if (config.mode !== "enabled") {
    throw new Error("expected enabled settings");
  }
  const outcomes = await Promise.all(
    [
      { failSpawn: true },
      { serverName: "wrong-server" },
      { protocolVersion: "wrong-version" },
      { inputSchema: { type: "object" } },
      { schema: { type: "object" } },
    ].map((options) => {
      const fake = mockWorker(options);
      const client = createContextClient(config, fake.spawn as never);
      return client.start();
    }),
  );
  expect(outcomes).toEqual(
    Array.from({ length: 5 }, () => ({
      kind: "failure",
      category: "context_startup_failed",
    })),
  );
});

test("permanent identity and schema mismatches do not consume a replacement", async () => {
  if (config.mode !== "enabled") throw new Error("expected enabled settings");
  for (const options of [
    { serverName: "wrong-server" },
    { protocolVersion: "wrong-version" },
    { schema: { type: "object" } },
    { readinessRpcError: true },
  ]) {
    const fake = mockWorker(options);
    let spawns = 0;
    const client = createContextClient(config, ((value: never) => {
      spawns += 1;
      return fake.spawn(value);
    }) as never);
    expect((await client.request(1)).kind).toBe("failure");
    expect((await client.request(1)).kind).toBe("failure");
    expect(spawns).toBe(1);
  }
});

test("bounds combined readiness by one startup deadline", async () => {
  const shortConfig = fromValues({
    TOTAL_RECALL_CONTEXT_BIN: "/absolute/worker",
    TOTAL_RECALL_MEMORY_DATABASE: "memory-db",
    TOTAL_RECALL_CONTEXT_STARTUP_TIMEOUT_MS: "10",
  });
  if (shortConfig.mode !== "enabled") {
    throw new Error("expected enabled settings");
  }
  const fake = mockWorker({ holdReadiness: true });
  const client = createContextClient(shortConfig, fake.spawn as never);
  expect(await client.start()).toEqual({
    kind: "failure",
    category: "context_startup_failed",
  });
  expect(fake.requests).toHaveLength(1);
  expect(fake.killed).toBe(true);
  expect(() => fake.child.stdout.getReader()).toThrow();
});

test("settles readiness immediately when the child exits", async () => {
  if (config.mode !== "enabled") throw new Error("expected enabled settings");
  const fake = mockWorker({ holdReadiness: true });
  const client = createContextClient(config, fake.spawn as never);
  const starting = client.start();
  await new Promise((resolve) => setTimeout(resolve, 0));
  fake.exit();
  expect(await starting).toEqual({
    kind: "failure",
    category: "context_startup_failed",
  });
});

test("settles pending lookups when the child exits", async () => {
  if (config.mode !== "enabled") throw new Error("expected enabled settings");
  const fake = mockWorker({ holdResponses: true });
  const client = createContextClient(config, fake.spawn as never);
  const request = client.request(1);
  await new Promise((resolve) => setTimeout(resolve, 0));
  fake.exit();
  expect(await request).toEqual({
    kind: "failure",
    category: "context_startup_failed",
  });
});

test("startup timeout stops a blocked write continuation and retains stdout ownership until it settles", async () => {
  const shortConfig = fromValues({
    TOTAL_RECALL_CONTEXT_BIN: "/absolute/worker",
    TOTAL_RECALL_MEMORY_DATABASE: "memory-db",
    TOTAL_RECALL_CONTEXT_STARTUP_TIMEOUT_MS: "10",
  });
  if (shortConfig.mode !== "enabled") {
    throw new Error("expected enabled settings");
  }
  const fake = mockWorker({ blockFirstWrite: true });
  const client = createContextClient(shortConfig, fake.spawn as never);
  expect(await client.start()).toEqual({
    kind: "failure",
    category: "context_startup_failed",
  });
  expect(fake.requests.map((request) => request.method)).toEqual([
    "server/discover",
  ]);
  expect(fake.killed).toBe(true);
  expect(() => fake.child.stdout.getReader()).toThrow();

  fake.resolveFirstWrite();
  await new Promise((resolve) => setTimeout(resolve, 0));
  expect(fake.requests.map((request) => request.method)).toEqual([
    "server/discover",
  ]);
  expect(() => fake.child.stdout.getReader()).toThrow();
  await client.close();
  expect(() => fake.child.stdout.getReader()).not.toThrow();
});

test("does not send readiness when synchronous spawn consumes the startup deadline", async () => {
  const shortConfig = fromValues({
    TOTAL_RECALL_CONTEXT_BIN: "/absolute/worker",
    TOTAL_RECALL_MEMORY_DATABASE: "memory-db",
    TOTAL_RECALL_CONTEXT_STARTUP_TIMEOUT_MS: "10",
  });
  if (shortConfig.mode !== "enabled") {
    throw new Error("expected enabled settings");
  }
  const fake = mockWorker({});
  const originalNow = Date.now;
  const initialNow = originalNow();
  const client = createContextClient(shortConfig, ((
    options: Parameters<typeof fake.spawn>[0],
  ) => {
    Date.now = () => initialNow + SPAWN_DELAY_OVER_DEADLINE_MS;
    return fake.spawn(options);
  }) as never);
  try {
    expect(await client.start()).toEqual({
      kind: "failure",
      category: "context_startup_failed",
    });
    expect(fake.requests).toHaveLength(0);
    expect(fake.killed).toBe(true);
  } finally {
    Date.now = originalNow;
  }
});

test("keeps the worker process absent when the existing opt-in config is disabled", async () => {
  const spawnWorker = () => {
    throw new Error("disabled configuration must not spawn");
  };
  const client = createContextClient(
    { mode: "disabled" },
    spawnWorker as never,
  );
  expect(await client.start()).toEqual({
    kind: "failure",
    category: "context_startup_failed",
  });
  expect(client.process).toBeUndefined();
});

test("correlates concurrent context calls and validates the exact narrow DTO", async () => {
  if (config.mode !== "enabled") throw new Error("expected enabled settings");
  const fake = mockWorker({});
  const client = createContextClient(config, fake.spawn as never);
  const outcomes = await Promise.all([client.request(1), client.request(2)]);
  expect(outcomes).toEqual([
    {
      kind: "context",
      memories: [
        { type: "fact", title: "title", content: "body", concepts: ["one"] },
      ],
    },
    {
      kind: "context",
      memories: [
        { type: "fact", title: "title", content: "body", concepts: ["one"] },
      ],
    },
  ]);
  expect(fake.requests.slice(2).map((request) => request.id)).toEqual([
    "1:1",
    "1:2",
  ]);
});

test("correlates out-of-order responses to their live request IDs", async () => {
  if (config.mode !== "enabled") throw new Error("expected enabled settings");
  const fake = mockWorker({ responseMode: "reverse-order" });
  const client = createContextClient(config, fake.spawn as never);
  const results = await Promise.all([client.request(1), client.request(2)]);
  expect(results).toEqual([
    {
      kind: "context",
      memories: [
        { type: "fact", title: "title", content: "1", concepts: ["one"] },
      ],
    },
    {
      kind: "context",
      memories: [
        { type: "fact", title: "title", content: "2", concepts: ["one"] },
      ],
    },
  ]);
  expect(fake.requests.slice(2).map((request) => request.id)).toEqual([
    "1:1",
    "1:2",
  ]);
});

test("rejects a context DTO with unexpected fields", async () => {
  if (config.mode !== "enabled") throw new Error("expected enabled settings");
  const fake = mockWorker({
    contextResult: {
      memories: [
        {
          type: "fact",
          title: "title",
          content: "body",
          concepts: [],
          files: ["private"],
        },
      ],
    },
  });
  const client = createContextClient(config, fake.spawn as never);
  expect(await client.request(1)).toEqual({
    kind: "failure",
    category: "context_startup_failed",
  });
});

test("reads fragmented frames and coalesced response frames", async () => {
  if (config.mode !== "enabled") throw new Error("expected enabled settings");
  const fragmented = mockWorker({ responseMode: "fragmented" });
  const fragmentedClient = createContextClient(
    config,
    fragmented.spawn as never,
  );
  expect((await fragmentedClient.request(1)).kind).toBe("context");

  const coalesced = mockWorker({ responseMode: "coalesced" });
  const coalescedClient = createContextClient(config, coalesced.spawn as never);
  const results = await Promise.all([
    coalescedClient.request(1),
    coalescedClient.request(2),
  ]);
  expect(results.map((result) => result.kind)).toEqual(["context", "context"]);
});

test("rejects coalesced queued frames whose aggregate exceeds the frame-byte bound", async () => {
  const boundedConfig = fromValues({
    TOTAL_RECALL_CONTEXT_BIN: "/absolute/worker",
    TOTAL_RECALL_MEMORY_DATABASE: "memory-db",
  });
  if (boundedConfig.mode !== "enabled")
    throw new Error("expected enabled settings");
  const fake = mockWorker({
    responseMode: "coalesced",
    contextResult: {
      memories: [
        {
          type: "fact",
          title: "title",
          content: "x".repeat(CONTEXT_FRAME_TEST_CONTENT_BYTES),
          concepts: [],
        },
      ],
    },
  });
  const client = createContextClient(boundedConfig, fake.spawn as never);
  const results = await Promise.all([client.request(1), client.request(2)]);
  expect(results.map((result) => result.kind)).toEqual(["failure", "failure"]);
  expect(fake.killed).toBe(true);
});

test("rejects queued frames plus a trailing partial when aggregate retained bytes exceed the bound", async () => {
  if (config.mode !== "enabled") throw new Error("expected enabled settings");
  const fake = mockWorker({
    responseMode: "queued-plus-partial",
    contextResult: {
      memories: [
        {
          type: "fact",
          title: "title",
          content: "x".repeat(QUEUED_PARTIAL_BOUNDARY_CONTENT_BYTES),
          concepts: [],
        },
      ],
    },
  });
  const client = createContextClient(config, fake.spawn as never);
  expect(await client.request(1)).toEqual({
    kind: "failure",
    category: "context_startup_failed",
  });
  expect(fake.killed).toBe(true);
});

test("bounds pending requests and rejects malformed, unknown-ID, and partial-EOF frames", async () => {
  if (config.mode !== "enabled") throw new Error("expected enabled settings");
  const saturatedConfig = fromValues({
    TOTAL_RECALL_CONTEXT_BIN: "/absolute/worker",
    TOTAL_RECALL_MEMORY_DATABASE: "memory-db",
    TOTAL_RECALL_CONTEXT_MAX_PENDING: "1",
  });
  if (saturatedConfig.mode !== "enabled")
    throw new Error("expected enabled settings");
  const held = mockWorker({ holdResponses: true });
  const saturatedClient = createContextClient(
    saturatedConfig,
    held.spawn as never,
  );
  await saturatedClient.start();
  const _first = saturatedClient.request(1);
  while (
    held.requests.filter((request) => request.method === "tools/call")
      .length === 0
  )
    await new Promise((resolve) => setTimeout(resolve, 0));
  const second = await saturatedClient.request(1);
  expect(second).toEqual({
    kind: "failure",
    category: "context_startup_failed",
  });
  held.child.stdin.end();

  for (const responseMode of [
    "malformed",
    "unknown-id",
    "partial-eof",
  ] as const) {
    const fake = mockWorker({ responseMode });
    const client = createContextClient(config, fake.spawn as never);
    expect(await client.request(1)).toEqual({
      kind: "failure",
      category: "context_startup_failed",
    });
    expect(fake.killed).toBe(true);
  }
});

test("discards a retired response from the prior generation during replacement", async () => {
  if (config.mode !== "enabled") throw new Error("expected enabled settings");
  const quickConfig = {
    ...config,
    settings: { ...config.settings, lookupTimeoutMs: 20 },
  };
  const first = mockWorker({ holdResponses: true });
  const replacement = mockWorker({ holdResponses: true });
  let spawns = 0;
  const client = createContextClient(quickConfig, ((options: never) => {
    spawns += 1;
    if (spawns === 1) return first.spawn(options);
    return replacement.spawn(options);
  }) as never);
  expect(await client.request(1)).toEqual({
    kind: "failure",
    category: "context_startup_failed",
  });
  expect(first.killed).toBe(true);
  quickConfig.settings.lookupTimeoutMs = REPLACEMENT_LOOKUP_TIMEOUT_MS;
  const replacementRequest = client.request(1);
  while (
    replacement.requests.filter((request) => request.method === "tools/call")
      .length === 0
  )
    await new Promise((resolve) => setTimeout(resolve, 0));
  const replacementId = replacement.requests.at(-1)?.id;
  expect(replacementId).toBe("2:1");
  const pendingReplacement = replacementRequest.then((result) => result);
  first.respond("1:1", "late prior result");
  await new Promise((resolve) => setTimeout(resolve, 0));
  expect(replacementId).toBe("2:1");
  replacement.respond(replacementId as string, "replacement result");
  expect(await pendingReplacement).toEqual({
    kind: "context",
    memories: [
      {
        type: "fact",
        title: "title",
        content: "replacement result",
        concepts: ["one"],
      },
    ],
  });
  expect(spawns).toBe(2);

  const backend = mockWorker({ backendError: true });
  const backendClient = createContextClient(config, backend.spawn as never);
  expect((await backendClient.request(1)).kind).toBe("failure");
  expect((await backendClient.request(1)).kind).toBe("failure");
  expect(backend.killed).toBe(false);

  replacement.child.stdin.end();
  expect(await client.request(1)).toEqual({
    kind: "failure",
    category: "context_startup_failed",
  });
  expect(spawns).toBe(2);
});
