// biome-ignore-all lint/style/useNamingConvention: These keys are external environment names.
// biome-ignore-all lint/complexity/noExcessiveLinesPerFunction: The test fixture models one child process boundary.
// biome-ignore-all lint/style/noExcessiveLinesPerFile: Readiness and lifecycle tests share the worker fixture.
// biome-ignore-all lint/correctness/useQwikValidLexicalScope: This test file has no Qwik components.
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
  readonly blockFirstWrite?: boolean;
  readonly failSpawn?: boolean;
  readonly onSpawn?: () => void;
}) {
  const requests: FakeRequest[] = [];
  let stderrDrained = false;
  let spawnOptions: unknown;
  let stdoutController: ReadableStreamDefaultController<Uint8Array>;
  let killed = false;
  let resolveFirstWrite: (() => void) | undefined;
  const child = {
    stdin: {
      write: (text: string): void | Promise<void> => {
        const request = JSON.parse(text) as FakeRequest;
        requests.push(request);
        if (options.blockFirstWrite && requests.length === 1) {
          return new Promise<void>((resolve) => {
            resolveFirstWrite = resolve;
          });
        }
        if (options.holdResponses) {
          return;
        }
        const { method } = request;
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
        stdoutController.enqueue(
          new TextEncoder().encode(
            `${JSON.stringify({ jsonrpc: "2.0", id: request.id, result })}\n`,
          ),
        );
      },
      end: () => stdoutController.close(),
    },
    stdout: new ReadableStream<Uint8Array>({
      start(controller) {
        stdoutController = controller;
      },
    }),
    stderr: new ReadableStream<Uint8Array>(
      {
        pull(controller) {
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
    exited: Promise.resolve(0),
    kill: () => {
      killed = true;
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
    resolveFirstWrite() {
      resolveFirstWrite?.();
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

test("bounds combined readiness by one startup deadline", async () => {
  const shortConfig = fromValues({
    TOTAL_RECALL_CONTEXT_BIN: "/absolute/worker",
    TOTAL_RECALL_MEMORY_DATABASE: "memory-db",
    TOTAL_RECALL_CONTEXT_STARTUP_TIMEOUT_MS: "10",
  });
  if (shortConfig.mode !== "enabled") {
    throw new Error("expected enabled settings");
  }
  const fake = mockWorker({ holdResponses: true });
  const client = createContextClient(shortConfig, fake.spawn as never);
  expect(await client.start()).toEqual({
    kind: "failure",
    category: "context_startup_failed",
  });
  expect(fake.requests).toHaveLength(1);
  expect(fake.killed).toBe(true);
  expect(() => fake.child.stdout.getReader()).toThrow();
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
  expect(() => fake.child.stdout.getReader().releaseLock()).not.toThrow();
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
