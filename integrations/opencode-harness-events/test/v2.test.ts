// biome-ignore-all lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
// biome-ignore-all lint/correctness/useQwikValidLexicalScope: Tests intentionally use closures for injected adapter seams.
// biome-ignore-all lint/style/noExcessiveLinesPerFile: V2 matrix and failure isolation share one adapter fixture.
// biome-ignore-all lint/style/useNamingConvention: Native OpenCode fields retain their SDK spelling.
// biome-ignore-all lint/performance/noAwaitInLoops: Matrix cases are intentionally isolated and consumed sequentially.

import { expect, test } from "bun:test";
import {
  createV2Adapter,
  setupV2,
  type V2Context,
  type V2AdapterDependencies,
  type V2HookCallback,
  type V2Registration,
  type V2Subscribe,
} from "../src/v2";
import { createSessionRuntime, type SessionRuntime } from "../src/runtime";
import type { Diagnostic, JsonObject, Submission } from "../src/model";

const NATIVE_CREATED_MS = Date.parse("2026-09-20T12:34:56.789Z");
const CLOCK_MS = Date.parse("2026-09-20T13:14:15.016Z");
const SESSION_ID = "session-v2";
const CONTEXT_DIRECTORY = "/context/project";
const EVENT_DIRECTORY = "/event/session";
const OPAQUE_NUMBER = 7;
const CLEANUP_SETTLEMENT_MICROTASK_TURNS = 4;

function context(
  directory = CONTEXT_DIRECTORY,
  subscribe: V2Subscribe = finiteSubscription([]),
  hooks?: Pick<V2Context, "session" | "tool">,
): V2Context {
  const registration = async (): Promise<V2Registration> => ({
    dispose: async () => undefined,
  });

  return {
    location: { directory },
    event: { subscribe },
    session: hooks?.session ?? {
      hook: async (_name, _callback: V2HookCallback) => registration(),
    },
    tool: hooks?.tool ?? {
      hook: async (_name, _callback: V2HookCallback) => registration(),
    },
  };
}

function recordingRuntime(): {
  readonly runtime: SessionRuntime;
  readonly submissions: Submission[];
} {
  const submissions: Submission[] = [];
  const runtime: SessionRuntime = {
    start(metadata) {
      submissions.push({ kind: "start", metadata });
    },
    observe(observation) {
      submissions.push(observation);
    },
    end(metadata) {
      submissions.push({ kind: "end", metadata });
    },
    close: async () => undefined,
  };

  return { runtime, submissions };
}

function dependencies(
  runtime: SessionRuntime,
  report: (diagnostic: Diagnostic) => void = () => undefined,
  now: () => Date = () => new Date(CLOCK_MS),
  subscribe?: V2Subscribe,
): V2AdapterDependencies {
  const base = { runtime, report, now };
  if (subscribe === undefined) {
    return base;
  }
  return { ...base, subscribe };
}

function finiteSubscription(events: readonly unknown[]): V2Subscribe {
  return () => ({
    async *[Symbol.asyncIterator]() {
      await Promise.resolve();
      for (const event of events) {
        yield event;
      }
    },
  });
}

interface HookFixture {
  readonly context: V2Context;
  readonly callbacks: ReadonlyMap<string, V2HookCallback>;
  readonly registrationCalls: string[];
  readonly disposalOrder: string[];
}

function hookFixture(
  subscribe: V2Subscribe = finiteSubscription([]),
  directory = CONTEXT_DIRECTORY,
  options: {
    readonly failRegistrations?: ReadonlySet<string>;
    readonly failDisposals?: ReadonlySet<string>;
    readonly order?: string[];
  } = {},
): HookFixture {
  const callbacks = new Map<string, V2HookCallback>();
  const registrationCalls: string[] = [];
  const disposalOrder: string[] = [];
  const failRegistrations = options.failRegistrations ?? new Set<string>();
  const failDisposals = options.failDisposals ?? new Set<string>();

  const register = (
    key: string,
    callback: V2HookCallback,
  ): Promise<V2Registration> => {
    registrationCalls.push(key);
    if (failRegistrations.has(key)) {
      return Promise.reject(new Error(`${key}-registration-secret`));
    }

    callbacks.set(key, callback);
    return Promise.resolve({
      dispose: () => {
        disposalOrder.push(`dispose:${key}`);
        options.order?.push(`dispose:${key}`);
        if (failDisposals.has(key)) {
          return Promise.reject(new Error(`${key}-dispose-secret`));
        }
        return Promise.resolve();
      },
    });
  };

  const session = {
    hook: (name: "prompt", callback: V2HookCallback) =>
      register(`session.${name}`, callback),
  };
  const tool = {
    hook: (
      name: "execute.before" | "execute.after",
      callback: V2HookCallback,
    ) => register(`tool.${name}`, callback),
  };

  return {
    context: context(directory, subscribe, { session, tool }),
    callbacks,
    registrationCalls,
    disposalOrder,
  };
}

async function invokeHook(
  callbacks: ReadonlyMap<string, V2HookCallback>,
  key: string,
  input: unknown,
): Promise<void> {
  const callback = callbacks.get(key);
  if (callback !== undefined) {
    await callback(input);
  }
}

function nativeEvent(
  type: string,
  data: unknown,
  options: { readonly created?: unknown; readonly directory?: unknown } = {},
): unknown {
  const { directory: eventDirectoryValue } = options;
  let directory: unknown = EVENT_DIRECTORY;
  if (eventDirectoryValue !== undefined) {
    directory = eventDirectoryValue;
  }

  return {
    id: `event-${type}`,
    created: options.created ?? NATIVE_CREATED_MS,
    location: { directory },
    type,
    data,
  };
}

function eventData(sessionId = SESSION_ID): JsonObject {
  return {
    sessionID: sessionId,
    opaque: {
      nested: ["preserved", OPAQUE_NUMBER, true, null],
    },
  };
}

async function settleSubscription(): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
}

function observations(
  submissions: readonly Submission[],
): readonly Extract<Submission, { readonly kind: "observation" }>[] {
  return submissions.filter(
    (
      submission,
    ): submission is Extract<Submission, { readonly kind: "observation" }> =>
      submission.kind === "observation",
  );
}

const acceptedEvents: readonly {
  readonly nativeKind: string;
  readonly data: JsonObject;
}[] = [
  { nativeKind: "session.created", data: eventData() },
  { nativeKind: "session.deleted", data: eventData() },
  { nativeKind: "session.execution.started", data: eventData() },
  { nativeKind: "session.execution.succeeded", data: eventData() },
  { nativeKind: "session.execution.interrupted", data: eventData() },
  { nativeKind: "session.text.ended", data: { ...eventData(), text: "done" } },
  {
    nativeKind: "session.reasoning.ended",
    data: { ...eventData(), text: "reasoning" },
  },
  {
    nativeKind: "session.execution.failed",
    data: { ...eventData(), error: { message: "opaque-error" } },
  },
  {
    nativeKind: "session.step.failed",
    data: { ...eventData(), error: { message: "opaque-step-error" } },
  },
  {
    nativeKind: "session.compaction.failed",
    data: { ...eventData(), error: { message: "opaque-compaction-error" } },
  },
];

test("accepts every designed v2 event and preserves opaque native data", async () => {
  for (const eventCase of acceptedEvents) {
    const recording = recordingRuntime();
    const cleanup = createV2Adapter(
      context(
        undefined,
        finiteSubscription([nativeEvent(eventCase.nativeKind, eventCase.data)]),
      ),
      dependencies(recording.runtime),
    );

    await settleSubscription();
    await cleanup();

    const [observation] = observations(recording.submissions);
    expect(observation).toBeDefined();
    expect(observation?.hookType).toBe(`opencode.v2.${eventCase.nativeKind}`);
    expect(observation?.data).toEqual(eventCase.data);
  }
});

test("omits undefined optional fields from OpenCode-like v2 data", async () => {
  const recording = recordingRuntime();
  const diagnostics: Diagnostic[] = [];
  const openCodeEventData = {
    ...eventData(),
    message: {
      content: "complete",
      id: "message-v2",
      model: undefined,
      role: "assistant",
    },
    summary: undefined,
  };
  const cleanup = createV2Adapter(
    context(
      CONTEXT_DIRECTORY,
      finiteSubscription([
        nativeEvent("session.execution.started", openCodeEventData),
      ]),
    ),
    dependencies(recording.runtime, (diagnostic) =>
      diagnostics.push(diagnostic),
    ),
  );

  await settleSubscription();
  await cleanup();

  const [observation] = observations(recording.submissions);
  expect(observation?.data).toEqual({
    ...eventData(),
    message: {
      content: "complete",
      id: "message-v2",
      role: "assistant",
    },
  });
  expect(diagnostics).toEqual([]);
});

test("orders creation before observation and deletion before end", async () => {
  const recording = recordingRuntime();
  const cleanup = createV2Adapter(
    context(
      CONTEXT_DIRECTORY,
      finiteSubscription([
        nativeEvent("session.created", eventData()),
        nativeEvent("session.deleted", eventData()),
      ]),
    ),
    dependencies(recording.runtime),
  );

  await settleSubscription();
  await cleanup();

  expect(recording.submissions.map((submission) => submission.kind)).toEqual([
    "start",
    "observation",
    "observation",
    "end",
  ]);
});

test("hands an unknown session to runtime synthesis", async () => {
  const submissions: Submission[] = [];
  const runtime = createSessionRuntime((submission) => {
    submissions.push(submission);
    return Promise.resolve({ ok: true });
  });
  const cleanup = createV2Adapter(
    context(
      undefined,
      finiteSubscription([
        nativeEvent("session.execution.started", eventData("resumed-session")),
      ]),
    ),
    dependencies(runtime),
  );

  await settleSubscription();
  await cleanup();
  await runtime.close();

  expect(submissions.map((submission) => submission.kind)).toEqual([
    "start",
    "observation",
    "end",
  ]);
});

test("excludes text and reasoning deltas and tool progress without diagnostics", async () => {
  const recording = recordingRuntime();
  const diagnostics: Diagnostic[] = [];
  const cleanup = createV2Adapter(
    context(
      CONTEXT_DIRECTORY,
      finiteSubscription([
        nativeEvent("session.text.delta", eventData()),
        nativeEvent("session.reasoning.delta", eventData()),
        nativeEvent("session.tool.progress", eventData()),
      ]),
    ),
    dependencies(recording.runtime, (diagnostic) =>
      diagnostics.push(diagnostic),
    ),
  );

  await settleSubscription();
  await cleanup();

  expect(recording.submissions).toEqual([]);
  expect(diagnostics).toEqual([]);
});

test("prefers event location, then tracked session location, then context location", async () => {
  const recording = recordingRuntime();
  const cleanup = createV2Adapter(
    context(
      CONTEXT_DIRECTORY,
      finiteSubscription([
        nativeEvent("session.created", eventData(), {
          directory: EVENT_DIRECTORY,
        }),
        nativeEvent("session.execution.started", eventData(), {
          directory: "   ",
        }),
        nativeEvent(
          "session.execution.succeeded",
          eventData("context-session"),
          { directory: "   " },
        ),
      ]),
    ),
    dependencies(recording.runtime),
  );

  await settleSubscription();
  await cleanup();

  expect(
    observations(recording.submissions).map(
      (observation) => observation.metadata.currentWorkingDirectory,
    ),
  ).toEqual([EVENT_DIRECTORY, EVENT_DIRECTORY, CONTEXT_DIRECTORY]);
  expect(
    observations(recording.submissions).map(
      (observation) => observation.metadata.projectName,
    ),
  ).toEqual(["project", "project", "project"]);
});

test("uses context directory for project name even when event location differs", async () => {
  const recording = recordingRuntime();
  const cleanup = createV2Adapter(
    context(
      CONTEXT_DIRECTORY,
      finiteSubscription([
        nativeEvent("session.execution.started", eventData(), {
          directory: "/other/project-session",
        }),
      ]),
    ),
    dependencies(recording.runtime),
  );

  await settleSubscription();
  await cleanup();

  const [observation] = observations(recording.submissions);
  expect(observation?.metadata.projectName).toBe("project");
  expect(observation?.metadata.currentWorkingDirectory).toBe(
    "/other/project-session",
  );
});

test("uses native created milliseconds and falls back to the injected clock", async () => {
  const recording = recordingRuntime();
  const cleanup = createV2Adapter(
    context(
      CONTEXT_DIRECTORY,
      finiteSubscription([
        nativeEvent("session.execution.started", eventData(), {
          created: NATIVE_CREATED_MS,
        }),
        nativeEvent("session.execution.succeeded", eventData(), {
          created: Number.NaN,
        }),
        nativeEvent("session.execution.interrupted", eventData(), {
          created: "invalid",
        }),
      ]),
    ),
    dependencies(recording.runtime),
  );

  await settleSubscription();
  await cleanup();

  expect(
    observations(recording.submissions).map(
      (observation) => observation.metadata.timestamp,
    ),
  ).toEqual([
    "2026-09-20T12:34:56.789Z",
    "2026-09-20T13:14:15.016Z",
    "2026-09-20T13:14:15.016Z",
  ]);
});

test("falls back to event location when context directory is blank", async () => {
  const recording = recordingRuntime();
  const cleanup = createV2Adapter(
    context(
      "   ",
      finiteSubscription([
        nativeEvent("session.execution.started", eventData(), {
          directory: "/fallback/project",
        }),
      ]),
    ),
    dependencies(recording.runtime),
  );

  await settleSubscription();
  await cleanup();

  const [observation] = observations(recording.submissions);
  expect(observation?.metadata.projectName).toBe("project");
  expect(observation?.metadata.currentWorkingDirectory).toBe(
    "/fallback/project",
  );
});

test("normalizes long trailing separators in v2 paths without regex backtracking", async () => {
  const recording = recordingRuntime();
  const cleanup = createV2Adapter(
    context(
      `/fallback/project${"/".repeat(100_000)}`,
      finiteSubscription([
        nativeEvent("session.execution.started", eventData()),
      ]),
    ),
    dependencies(recording.runtime),
  );

  await settleSubscription();
  await cleanup();

  const [observation] = observations(recording.submissions);
  expect(observation?.metadata.projectName).toBe("project");
});

test("preserves JSON.parse __proto__ keys in accepted native data", async () => {
  const recording = recordingRuntime();
  const cleanup = createV2Adapter(
    context(
      CONTEXT_DIRECTORY,
      finiteSubscription([
        nativeEvent(
          "session.execution.started",
          JSON.parse(
            '{"sessionID":"proto-session","status":{"type":"busy"},"__proto__":{"sentinel":"preserved"}}',
          ),
        ),
      ]),
    ),
    dependencies(recording.runtime),
  );

  await settleSubscription();
  await cleanup();

  const [observation] = observations(recording.submissions);
  expect(observation).toBeDefined();
  if (observation === undefined) {
    return;
  }

  expect(Object.hasOwn(observation.data, "__proto__")).toBe(true);
  expect(
    Object.getOwnPropertyDescriptor(observation.data, "__proto__")?.value,
  ).toEqual({ sentinel: "preserved" });
});

// biome-ignore lint/complexity/noExcessiveLinesPerFunction: Keep malformed payload cases in one diagnostic assertion.
test("rejects malformed native data with payload-free diagnostics", async () => {
  const recording = recordingRuntime();
  const diagnostics: Diagnostic[] = [];
  const cyclicData: {
    readonly sessionID: string;
    readonly secret: string;
    cycle?: unknown;
  } = {
    sessionID: "cyclic-session",
    secret: "cyclic-payload-secret",
  };
  cyclicData.cycle = cyclicData;
  const cleanup = createV2Adapter(
    context(
      CONTEXT_DIRECTORY,
      finiteSubscription([
        null,
        [],
        nativeEvent("session.execution.started", []),
        nativeEvent("session.execution.succeeded", {
          sessionID: "invalid-session",
          invalid: Symbol("invalid-event-value"),
          invalidArray: [undefined],
          secret: "payload-secret",
        }),
        nativeEvent("session.execution.failed", cyclicData),
        nativeEvent("session.step.failed", {
          sessionID: "nonfinite-session",
          value: Number.POSITIVE_INFINITY,
        }),
      ]),
    ),
    dependencies(recording.runtime, (diagnostic) =>
      diagnostics.push(diagnostic),
    ),
  );

  await settleSubscription();
  await cleanup();

  expect(recording.submissions).toEqual([]);
  expect(diagnostics).toEqual([
    { category: "serialization", generation: "v2" },
    { category: "serialization", generation: "v2" },
    {
      category: "serialization",
      generation: "v2",
      nativeKind: "session.execution.started",
    },
    {
      category: "serialization",
      generation: "v2",
      nativeKind: "session.execution.succeeded",
      sessionId: "invalid-session",
    },
    {
      category: "serialization",
      generation: "v2",
      nativeKind: "session.execution.failed",
      sessionId: "cyclic-session",
    },
    {
      category: "serialization",
      generation: "v2",
      nativeKind: "session.step.failed",
      sessionId: "nonfinite-session",
    },
  ]);
  expect(JSON.stringify(diagnostics)).not.toContain("payload-secret");
  expect(JSON.stringify(diagnostics)).not.toContain("cyclic-payload-secret");
  expect(JSON.stringify(diagnostics)).not.toContain("Infinity");
});

test("rejects accepted events without a usable directory", async () => {
  const recording = recordingRuntime();
  const diagnostics: Diagnostic[] = [];
  const cleanup = createV2Adapter(
    context(
      "   ",
      finiteSubscription([
        nativeEvent("session.execution.started", eventData(), {
          directory: "   ",
        }),
      ]),
    ),
    dependencies(recording.runtime, (diagnostic) =>
      diagnostics.push(diagnostic),
    ),
  );

  await settleSubscription();
  await cleanup();

  expect(recording.submissions).toEqual([]);
  expect(diagnostics).toEqual([
    {
      category: "serialization",
      generation: "v2",
      nativeKind: "session.execution.started",
      sessionId: SESSION_ID,
    },
  ]);
});

test("contains iterator failures as payload-free signal diagnostics", async () => {
  const recording = recordingRuntime();
  const diagnostics: Diagnostic[] = [];
  const subscribe: V2Subscribe = () => ({
    async *[Symbol.asyncIterator]() {
      await Promise.resolve();
      yield nativeEvent("session.execution.started", eventData());
      throw new Error("iterator-secret");
    },
  });
  const cleanup = createV2Adapter(
    context(CONTEXT_DIRECTORY, subscribe),
    dependencies(recording.runtime, (diagnostic) =>
      diagnostics.push(diagnostic),
    ),
  );

  await settleSubscription();
  await cleanup();

  expect(observations(recording.submissions)).toHaveLength(1);
  expect(diagnostics).toEqual([{ category: "signal", generation: "v2" }]);
  expect(JSON.stringify(diagnostics)).not.toContain("iterator-secret");
});

test("passes an abort signal to the subscription and does not block setup", async () => {
  let receivedSignal: AbortSignal | undefined;
  let release: (() => void) | undefined;
  const waiting = new Promise<void>((resolve) => {
    release = resolve;
  });
  const subscribe: V2Subscribe = (options) => {
    receivedSignal = options?.signal;
    options?.signal?.addEventListener("abort", () => release?.(), {
      once: true,
    });
    return {
      async *[Symbol.asyncIterator]() {
        await waiting;
        yield nativeEvent("session.execution.started", eventData());
      },
    };
  };
  const recording = recordingRuntime();

  const cleanupPromise = setupV2(
    context(CONTEXT_DIRECTORY, subscribe),
    dependencies(recording.runtime),
  );
  const cleanup = await cleanupPromise;
  expect(receivedSignal).toBeDefined();
  expect(receivedSignal?.aborted).toBe(false);

  await settleSubscription();
  await cleanup();
  expect(receivedSignal?.aborted).toBe(true);
  release?.();
  await settleSubscription();
  expect(recording.submissions).toEqual([]);
});

test("stops before handling an already-resolved iterator item", async () => {
  const recording = recordingRuntime();
  const subscribe: V2Subscribe = () => ({
    [Symbol.asyncIterator]() {
      let yielded = false;
      return {
        next: () => {
          if (yielded) {
            return Promise.resolve({ done: true, value: undefined });
          }
          yielded = true;
          return Promise.resolve({
            done: false,
            value: nativeEvent("session.execution.started", eventData()),
          });
        },
      };
    },
  });

  const cleanup = createV2Adapter(
    context(CONTEXT_DIRECTORY, subscribe),
    dependencies(recording.runtime),
  );
  const cleanupPromise = cleanup();
  await cleanupPromise;
  await settleSubscription();

  expect(recording.submissions).toEqual([]);
});

test("contains normalization, runtime, and reporter failures at the subscription boundary", async () => {
  const diagnostics: Diagnostic[] = [];
  const runtime: SessionRuntime = {
    start: () => undefined,
    observe: () => {
      throw new Error("runtime-secret");
    },
    end: () => undefined,
    close: async () => undefined,
  };
  const cleanup = createV2Adapter(
    context(
      CONTEXT_DIRECTORY,
      finiteSubscription([
        nativeEvent("session.execution.started", eventData()),
      ]),
    ),
    dependencies(runtime, (diagnostic) => {
      diagnostics.push(diagnostic);
      throw new Error("reporter-secret");
    }),
  );

  await settleSubscription();
  await expect(cleanup()).resolves.toBeUndefined();
  expect(diagnostics).toEqual([
    {
      category: "serialization",
      generation: "v2",
      nativeKind: "session.execution.started",
      sessionId: SESSION_ID,
    },
  ]);
  expect(JSON.stringify(diagnostics)).not.toContain("runtime-secret");
  expect(JSON.stringify(diagnostics)).not.toContain("reporter-secret");
});

// biome-ignore lint/complexity/noExcessiveLinesPerFunction: Keep the three-hook matrix in one assertion.
test("registers and forwards v2 prompt and tool hooks as opaque observations", async () => {
  const recording = recordingRuntime();
  const fixture = hookFixture(
    finiteSubscription([
      nativeEvent("session.created", eventData(), {
        directory: EVENT_DIRECTORY,
      }),
    ]),
  );
  const cleanup = await setupV2(
    fixture.context,
    dependencies(recording.runtime),
  );
  await settleSubscription();

  const prompt = {
    sessionID: SESSION_ID,
    messageID: "message-prompt",
    prompt: { parts: [{ type: "text", text: "prompt-secret" }] },
    metadata: { source: "prompt-metadata" },
    delivery: "direct",
  };
  const toolBefore = {
    tool: "read",
    sessionID: SESSION_ID,
    agent: "assistant",
    messageID: "message-tool",
    id: "call-before",
    input: { path: "tool-argument-secret" },
  };
  const toolAfter = {
    tool: "read",
    sessionID: SESSION_ID,
    agent: "assistant",
    messageID: "message-tool",
    id: "call-after",
    input: { path: "tool-argument-secret" },
    status: "completed",
    result: {
      title: "read result",
      output: "tool-output-secret",
      metadata: { source: "tool-metadata" },
    },
  };

  expect(fixture.registrationCalls).toEqual([
    "session.prompt",
    "tool.execute.before",
    "tool.execute.after",
  ]);
  await invokeHook(fixture.callbacks, "session.prompt", prompt);
  await invokeHook(fixture.callbacks, "tool.execute.before", toolBefore);
  await invokeHook(fixture.callbacks, "tool.execute.after", toolAfter);
  await cleanup();

  const hookObservations = observations(recording.submissions).filter(
    (observation) => observation.hookType !== "opencode.v2.session.created",
  );
  expect(hookObservations.map((observation) => observation.hookType)).toEqual([
    "opencode.v2.session.prompt",
    "opencode.v2.tool.execute.before",
    "opencode.v2.tool.execute.after",
  ]);
  expect(hookObservations.map((observation) => observation.data)).toEqual([
    prompt,
    toolBefore,
    toolAfter,
  ]);
  expect(
    hookObservations.every(
      (observation) =>
        observation.metadata.sessionId === SESSION_ID &&
        observation.metadata.currentWorkingDirectory === EVENT_DIRECTORY &&
        observation.metadata.projectName === "project" &&
        observation.metadata.timestamp === new Date(CLOCK_MS).toISOString(),
    ),
  ).toBe(true);
  expect(JSON.stringify(hookObservations)).toContain("prompt-secret");
  expect(JSON.stringify(hookObservations)).toContain("tool-output-secret");
});

test("uses context directory for v2 hooks without a tracked event directory", async () => {
  const recording = recordingRuntime();
  const fixture = hookFixture();
  const cleanup = await setupV2(
    fixture.context,
    dependencies(recording.runtime),
  );

  await invokeHook(fixture.callbacks, "session.prompt", {
    sessionID: "context-hook-session",
    prompt: { text: "context-prompt" },
  });
  await cleanup();

  const [observation] = observations(recording.submissions);
  expect(observation?.metadata.currentWorkingDirectory).toBe(CONTEXT_DIRECTORY);
  expect(observation?.metadata.projectName).toBe("project");
  expect(observation?.metadata.timestamp).toBe(
    new Date(CLOCK_MS).toISOString(),
  );
});

test("contains malformed v2 hooks with payload-free diagnostics", async () => {
  const recording = recordingRuntime();
  const diagnostics: Diagnostic[] = [];
  const fixture = hookFixture();
  const cleanup = await setupV2(
    fixture.context,
    dependencies(recording.runtime, (diagnostic) =>
      diagnostics.push(diagnostic),
    ),
  );
  const cyclicHook: {
    readonly sessionID: string;
    readonly secret: string;
    cycle?: unknown;
  } = {
    sessionID: SESSION_ID,
    secret: "cyclic-hook-secret",
  };
  cyclicHook.cycle = cyclicHook;

  await expect(
    invokeHook(fixture.callbacks, "session.prompt", null),
  ).resolves.toBeUndefined();
  await expect(
    invokeHook(fixture.callbacks, "tool.execute.before", {
      sessionID: "invalid-hook",
      input: Symbol("invalid-hook-value"),
    }),
  ).resolves.toBeUndefined();
  await expect(
    invokeHook(fixture.callbacks, "tool.execute.after", cyclicHook),
  ).resolves.toBeUndefined();
  await cleanup();

  expect(recording.submissions).toEqual([]);
  expect(diagnostics).toEqual([
    {
      category: "serialization",
      generation: "v2",
      nativeKind: "session.prompt",
    },
    {
      category: "serialization",
      generation: "v2",
      nativeKind: "tool.execute.before",
      sessionId: "invalid-hook",
    },
    {
      category: "serialization",
      generation: "v2",
      nativeKind: "tool.execute.after",
      sessionId: SESSION_ID,
    },
  ]);
  expect(JSON.stringify(diagnostics)).not.toContain("cyclic-hook-secret");
});

test("contains v2 hook runtime and reporter failures without rejecting callbacks", async () => {
  const diagnostics: Diagnostic[] = [];
  const runtime: SessionRuntime = {
    start: () => undefined,
    observe: () => {
      throw new Error("hook-runtime-secret");
    },
    end: () => undefined,
    close: async () => undefined,
  };
  const fixture = hookFixture();
  const cleanup = await setupV2(
    fixture.context,
    dependencies(runtime, (diagnostic) => {
      diagnostics.push(diagnostic);
      throw new Error("hook-reporter-secret");
    }),
  );

  await expect(
    invokeHook(fixture.callbacks, "tool.execute.after", {
      sessionID: SESSION_ID,
      tool: "read",
      status: "error",
      error: { message: "tool-error-secret" },
    }),
  ).resolves.toBeUndefined();
  await cleanup();

  expect(diagnostics).toEqual([
    {
      category: "serialization",
      generation: "v2",
      nativeKind: "tool.execute.after",
      sessionId: SESSION_ID,
    },
  ]);
  expect(JSON.stringify(diagnostics)).not.toContain("hook-runtime-secret");
  expect(JSON.stringify(diagnostics)).not.toContain("hook-reporter-secret");
  expect(JSON.stringify(diagnostics)).not.toContain("tool-error-secret");
});

test("contains registration failures and disposes successful registrations before runtime close", async () => {
  const order: string[] = [];
  let closeCount = 0;
  const fixture = hookFixture(finiteSubscription([]), CONTEXT_DIRECTORY, {
    failRegistrations: new Set(["session.prompt", "tool.execute.after"]),
    failDisposals: new Set(["tool.execute.before"]),
    order,
  });
  const runtime: SessionRuntime = {
    start: () => undefined,
    observe: () => undefined,
    end: () => undefined,
    close: () => {
      closeCount += 1;
      order.push("runtime.close");
      return Promise.resolve();
    },
  };

  const cleanup = await setupV2(fixture.context, dependencies(runtime));
  await expect(cleanup()).resolves.toBeUndefined();

  expect(fixture.registrationCalls).toEqual([
    "session.prompt",
    "tool.execute.before",
    "tool.execute.after",
  ]);
  expect(fixture.disposalOrder).toEqual(["dispose:tool.execute.before"]);
  expect(order).toEqual(["dispose:tool.execute.before", "runtime.close"]);
  expect(closeCount).toBe(1);
});

test("aborts the subscription, disposes hooks, and closes the runtime once in order", async () => {
  const order: string[] = [];
  let receivedSignal: AbortSignal | undefined;
  const subscribe: V2Subscribe = (options) => {
    receivedSignal = options?.signal;
    return {
      async *[Symbol.asyncIterator]() {
        await new Promise<void>((resolve) => {
          if (receivedSignal?.aborted) {
            order.push("subscription.aborted");
            resolve();
            return;
          }
          receivedSignal?.addEventListener(
            "abort",
            () => {
              order.push("subscription.aborted");
              resolve();
            },
            { once: true },
          );
        });
      },
    };
  };
  const fixture = hookFixture(subscribe, CONTEXT_DIRECTORY, { order });
  let closeCount = 0;
  let observationCount = 0;
  const runtime: SessionRuntime = {
    start: () => undefined,
    observe: () => {
      observationCount += 1;
    },
    end: () => undefined,
    close: () => {
      closeCount += 1;
      order.push("runtime.close");
      return Promise.resolve();
    },
  };
  const cleanup = await setupV2(fixture.context, dependencies(runtime));
  await settleSubscription();

  const firstCleanup = cleanup();
  const secondCleanup = cleanup();
  expect(receivedSignal?.aborted).toBe(true);
  await expect(firstCleanup).resolves.toBeUndefined();
  await expect(secondCleanup).resolves.toBeUndefined();
  await invokeHook(fixture.callbacks, "session.prompt", {
    sessionID: SESSION_ID,
    prompt: { text: "after-cleanup" },
  });

  expect(order).toEqual([
    "subscription.aborted",
    "dispose:session.prompt",
    "dispose:tool.execute.before",
    "dispose:tool.execute.after",
    "runtime.close",
  ]);
  expect(closeCount).toBe(1);
  expect(observationCount).toBe(0);
});

// biome-ignore lint/complexity/noExcessiveLinesPerFunction: The manual gate and cleanup assertions share one lifecycle.
test("cleans up without waiting for an abort-ignoring subscription and ignores its late event", async () => {
  const order: string[] = [];
  let receivedSignal: AbortSignal | undefined;
  let releaseGate: (() => void) | undefined;
  const gate = new Promise<void>((resolve) => {
    releaseGate = resolve;
  });
  let closeSubscription: (() => void) | undefined;
  const subscriptionClosed = new Promise<void>((resolve) => {
    closeSubscription = resolve;
  });
  const subscribe: V2Subscribe = (options) => {
    receivedSignal = options?.signal;
    return {
      async *[Symbol.asyncIterator]() {
        try {
          await gate;
          yield nativeEvent("session.execution.started", eventData());
        } finally {
          closeSubscription?.();
        }
      },
    };
  };
  const fixture = hookFixture(subscribe, CONTEXT_DIRECTORY, { order });
  let closeCount = 0;
  let observationCount = 0;
  const runtime: SessionRuntime = {
    start: () => undefined,
    observe: () => {
      observationCount += 1;
    },
    end: () => undefined,
    close: () => {
      closeCount += 1;
      order.push("runtime.close");
      return Promise.resolve();
    },
  };
  const cleanup = await setupV2(fixture.context, dependencies(runtime));
  const cleanupPromise = Promise.resolve(cleanup());
  let cleanupSettled = false;
  const cleanupSettlement = cleanupPromise.then(
    () => {
      cleanupSettled = true;
    },
    () => {
      cleanupSettled = true;
    },
  );
  expect(receivedSignal?.aborted).toBe(true);

  try {
    for (
      let index = 0;
      index <
      fixture.registrationCalls.length + CLEANUP_SETTLEMENT_MICROTASK_TURNS;
      index += 1
    ) {
      await Promise.resolve();
    }

    expect(fixture.disposalOrder).toEqual([
      "dispose:session.prompt",
      "dispose:tool.execute.before",
      "dispose:tool.execute.after",
    ]);
    expect(order).toEqual([
      "dispose:session.prompt",
      "dispose:tool.execute.before",
      "dispose:tool.execute.after",
      "runtime.close",
    ]);
    expect(closeCount).toBe(1);
    expect(cleanupSettled).toBe(true);
  } finally {
    releaseGate?.();
    await subscriptionClosed;
    await cleanupPromise;
    await cleanupSettlement;
  }

  expect(observationCount).toBe(0);
});
