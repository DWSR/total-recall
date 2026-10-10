// biome-ignore-all lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
// biome-ignore-all lint/correctness/useQwikValidLexicalScope: Tests intentionally use closures for injected adapter seams.
// biome-ignore-all lint/style/noExcessiveLinesPerFile: V1 matrix and failure isolation share one adapter fixture.
// biome-ignore-all lint/style/useNamingConvention: Native OpenCode fields retain their SDK spelling.

import { expect, test } from "bun:test";
import { createSessionRuntime, type SessionRuntime } from "../src/runtime";
import type { Diagnostic, JsonObject, Submission } from "../src/model";
import type { FrozenSnapshot } from "../src/context-format";
import type { StartupContextRuntime } from "../src/context-runtime";
import { createV1Adapter, type V1PluginInput } from "../src/v1";

const NATIVE_CREATED_MS = Date.parse("2026-09-20T12:34:56.789Z");
const CLOCK_MS = Date.parse("2026-09-20T13:14:15.016Z");
const SESSION_UPDATE_OFFSET_MS = 1000;
const SESSION_ID = "session-v1";
const FROZEN_TEXT =
  'startup-memory-fixture\n{"memories":[{"type":"pattern","title":"quote\\" pass","content":"line\\nbreak"}]}\n';

const input = {
  directory: "/plugin/project",
  worktree: "/project/worktree",
  project: {
    id: "project-id",
    worktree: "/project/worktree",
    time: { created: NATIVE_CREATED_MS },
  },
} satisfies V1PluginInput;

function sessionInfo(directory = "/event/project"): JsonObject {
  return {
    id: SESSION_ID,
    projectID: "project-id",
    directory,
    title: "fixture",
    version: "1.18.29",
    time: {
      created: NATIVE_CREATED_MS,
      updated: NATIVE_CREATED_MS + SESSION_UPDATE_OFFSET_MS,
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

const acceptedEvents: readonly {
  readonly nativeKind: string;
  readonly event: JsonObject;
  readonly payload: JsonObject;
}[] = [
  {
    nativeKind: "session.created",
    event: { type: "session.created", properties: { info: sessionInfo() } },
    payload: { info: sessionInfo() },
  },
  {
    nativeKind: "session.deleted",
    event: { type: "session.deleted", properties: { info: sessionInfo() } },
    payload: { info: sessionInfo() },
  },
  {
    nativeKind: "session.updated",
    event: { type: "session.updated", properties: { info: sessionInfo() } },
    payload: { info: sessionInfo() },
  },
  {
    nativeKind: "session.status",
    event: {
      type: "session.status",
      properties: { sessionID: SESSION_ID, status: { type: "busy" } },
    },
    payload: {
      sessionID: SESSION_ID,
      status: { type: "busy" },
    },
  },
  {
    nativeKind: "session.idle",
    event: {
      type: "session.idle",
      properties: { sessionID: SESSION_ID },
    },
    payload: { sessionID: SESSION_ID },
  },
  {
    nativeKind: "message.updated",
    event: {
      type: "message.updated",
      properties: {
        info: {
          id: "message-v1",
          sessionID: SESSION_ID,
          role: "user",
          time: { created: NATIVE_CREATED_MS },
        },
      },
    },
    payload: {
      info: {
        id: "message-v1",
        sessionID: SESSION_ID,
        role: "user",
        time: { created: NATIVE_CREATED_MS },
      },
    },
  },
  {
    nativeKind: "session.error",
    event: {
      type: "session.error",
      properties: {
        sessionID: SESSION_ID,
        error: { name: "UnknownError", data: { message: "opaque-error" } },
      },
    },
    payload: {
      sessionID: SESSION_ID,
      error: { name: "UnknownError", data: { message: "opaque-error" } },
    },
  },
  {
    nativeKind: "message.part.updated",
    event: {
      type: "message.part.updated",
      properties: {
        part: {
          id: "part-v1",
          sessionID: SESSION_ID,
          messageID: "message-v1",
          type: "text",
          text: "complete snapshot",
        },
      },
    },
    payload: {
      part: {
        id: "part-v1",
        sessionID: SESSION_ID,
        messageID: "message-v1",
        type: "text",
        text: "complete snapshot",
      },
    },
  },
];

test("accepts every designed v1 event and preserves its native properties", async () => {
  await Promise.all(
    acceptedEvents.map(async (eventCase) => {
      const recording = recordingRuntime();
      const diagnostics: Diagnostic[] = [];
      const hooks = createV1Adapter(input, {
        runtime: recording.runtime,
        report: (diagnostic) => diagnostics.push(diagnostic),
        now: () => new Date(CLOCK_MS),
      });

      await expect(
        hooks.event({ event: eventCase.event }),
      ).resolves.toBeUndefined();

      const observation = recording.submissions.find(
        (
          submission,
        ): submission is Extract<
          Submission,
          { readonly kind: "observation" }
        > => submission.kind === "observation",
      );
      expect(observation).toBeDefined();
      expect(observation?.hookType).toBe(`opencode.v1.${eventCase.nativeKind}`);
      expect(observation?.data).toEqual(eventCase.payload);
      expect(diagnostics).toEqual([]);
    }),
  );
});

test("omits undefined optional fields from OpenCode v1 session data", async () => {
  const recording = recordingRuntime();
  const diagnostics: Diagnostic[] = [];
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    report: (diagnostic) => diagnostics.push(diagnostic),
    now: () => new Date(CLOCK_MS),
  });
  const openCodeSessionInfo = {
    ...sessionInfo(),
    parentID: undefined,
    revert: undefined,
    share: undefined,
    summary: undefined,
  };

  await expect(
    hooks.event({
      event: {
        type: "session.created",
        properties: { info: openCodeSessionInfo },
      },
    }),
  ).resolves.toBeUndefined();

  const observation = recording.submissions.find(
    (
      submission,
    ): submission is Extract<Submission, { readonly kind: "observation" }> =>
      submission.kind === "observation",
  );
  expect(observation?.data).toEqual({ info: sessionInfo() });
  expect(diagnostics).toEqual([]);
});

test("preserves JSON.parse __proto__ keys as opaque payload data", async () => {
  const recording = recordingRuntime();
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    report: () => undefined,
    now: () => new Date(CLOCK_MS),
  });
  const event: unknown = JSON.parse(
    '{"type":"session.status","properties":{"sessionID":"proto-session","status":{"type":"busy"},"__proto__":{"sentinel":"preserved"}}}',
  );

  await hooks.event({ event });

  const observation = recording.submissions.find(
    (
      submission,
    ): submission is Extract<Submission, { readonly kind: "observation" }> =>
      submission.kind === "observation",
  );
  expect(observation).toBeDefined();
  if (observation === undefined) {
    return;
  }

  expect(Object.hasOwn(observation.data, "__proto__")).toBe(true);
  expect(
    Object.getOwnPropertyDescriptor(observation.data, "__proto__")?.value,
  ).toEqual({ sentinel: "preserved" });
});

test("ignores unsupported v1 events without submission or diagnostic", async () => {
  const recording = recordingRuntime();
  const diagnostics: Diagnostic[] = [];
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    report: (diagnostic) => diagnostics.push(diagnostic),
    now: () => new Date(CLOCK_MS),
  });

  await expect(
    hooks.event({
      event: {
        type: "session.execution.started",
        properties: { sessionID: SESSION_ID, reason: "unsupported" },
      },
    }),
  ).resolves.toBeUndefined();

  expect(recording.submissions).toEqual([]);
  expect(diagnostics).toEqual([]);
});

test("orders creation before observation and deletion before end", async () => {
  const recording = recordingRuntime();
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    report: () => undefined,
    now: () => new Date(CLOCK_MS),
  });

  await hooks.event({
    event: { type: "session.created", properties: { info: sessionInfo() } },
  });
  await hooks.event({
    event: { type: "session.deleted", properties: { info: sessionInfo() } },
  });

  expect(recording.submissions.map((submission) => submission.kind)).toEqual([
    "start",
    "observation",
    "observation",
    "end",
  ]);
});

test("excludes streamed message-part deltas but accepts complete snapshots", async () => {
  const recording = recordingRuntime();
  const diagnostics: Diagnostic[] = [];
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    report: (diagnostic) => diagnostics.push(diagnostic),
    now: () => new Date(CLOCK_MS),
  });

  await hooks.event({
    event: {
      type: "message.part.updated",
      properties: {
        part: {
          id: "part-delta",
          sessionID: SESSION_ID,
          messageID: "message-v1",
          type: "text",
          text: "partial",
        },
        delta: "streamed-delta-sentinel",
      },
    },
  });

  expect(recording.submissions).toEqual([]);
  expect(diagnostics).toEqual([]);
});

test("uses event directory, tracked directory, and plugin directory fallbacks", async () => {
  const recording = recordingRuntime();
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    report: () => undefined,
    now: () => new Date(CLOCK_MS),
  });

  await hooks.event({
    event: { type: "session.created", properties: { info: sessionInfo() } },
  });
  await hooks.event({
    event: {
      type: "session.status",
      properties: { sessionID: SESSION_ID, status: { type: "busy" } },
    },
  });
  await hooks.event({
    event: {
      type: "session.status",
      properties: { sessionID: "resumed-session", status: { type: "busy" } },
    },
  });

  const observations = recording.submissions.filter(
    (
      submission,
    ): submission is Extract<Submission, { readonly kind: "observation" }> =>
      submission.kind === "observation",
  );
  expect(
    observations.map(
      (observation) => observation.metadata.currentWorkingDirectory,
    ),
  ).toEqual(["/event/project", "/event/project", "/plugin/project"]);
  expect(
    observations.every(
      (observation) => observation.metadata.projectName === "worktree",
    ),
  ).toBe(true);
});

test("uses a global event directory when the native event has no session directory", async () => {
  const recording = recordingRuntime();
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    report: () => undefined,
    now: () => new Date(CLOCK_MS),
  });

  await hooks.event({
    event: {
      directory: "/global/event/project",
      payload: {
        type: "session.status",
        properties: { sessionID: "global-session", status: { type: "busy" } },
      },
    },
  });

  const [observation] = recording.submissions;
  expect(observation?.kind).toBe("observation");
  expect(observation?.metadata.currentWorkingDirectory).toBe(
    "/global/event/project",
  );
});

test("falls back to the working-directory basename for a missing project context", async () => {
  const recording = recordingRuntime();
  const hooks = createV1Adapter(
    {
      ...input,
      directory: "/fallback/project",
      worktree: "",
      project: { ...input.project, worktree: "" },
    },
    {
      runtime: recording.runtime,
      report: () => undefined,
      now: () => new Date(CLOCK_MS),
    },
  );

  await hooks.event({
    event: {
      type: "session.idle",
      properties: { sessionID: "fallback-session" },
    },
  });

  const [observation] = recording.submissions;
  expect(observation?.kind).toBe("observation");
  expect(observation?.metadata.projectName).toBe("project");
  expect(observation?.metadata.timestamp).toBe(
    new Date(CLOCK_MS).toISOString(),
  );
});

test("normalizes long trailing separators in v1 paths without regex backtracking", async () => {
  const recording = recordingRuntime();
  const hooks = createV1Adapter(
    {
      ...input,
      directory: `/fallback/project${"/".repeat(100_000)}`,
      worktree: "",
      project: { ...input.project, worktree: "" },
    },
    {
      runtime: recording.runtime,
      report: () => undefined,
      now: () => new Date(CLOCK_MS),
    },
  );

  await hooks.event({
    event: {
      type: "session.idle",
      properties: { sessionID: "long-path-session" },
    },
  });

  const [observation] = recording.submissions;
  expect(observation?.kind).toBe("observation");
  expect(observation?.metadata.projectName).toBe("project");
});

test("rejects an event when no usable directory remains after fallback", async () => {
  const recording = recordingRuntime();
  const diagnostics: Diagnostic[] = [];
  const hooks = createV1Adapter(
    { ...input, directory: "   " },
    {
      runtime: recording.runtime,
      report: (diagnostic) => diagnostics.push(diagnostic),
      now: () => new Date(CLOCK_MS),
    },
  );

  await expect(
    hooks.event({
      event: {
        type: "session.idle",
        properties: { sessionID: "missing-directory-session" },
      },
    }),
  ).resolves.toBeUndefined();

  expect(recording.submissions).toEqual([]);
  expect(diagnostics).toEqual([
    {
      category: "serialization",
      generation: "v1",
      nativeKind: "session.idle",
      sessionId: "missing-directory-session",
    },
  ]);
});

test("uses native session and message milliseconds before the injected clock", async () => {
  const recording = recordingRuntime();
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    report: () => undefined,
    now: () => new Date(CLOCK_MS),
  });

  await hooks.event({
    event: { type: "session.created", properties: { info: sessionInfo() } },
  });
  await hooks.event({
    event: {
      type: "message.updated",
      properties: {
        info: {
          id: "message-v1",
          sessionID: SESSION_ID,
          role: "user",
          time: { created: NATIVE_CREATED_MS },
        },
      },
    },
  });
  await hooks.event({
    event: {
      type: "session.status",
      properties: { sessionID: SESSION_ID, status: { type: "busy" } },
    },
  });

  const observations = recording.submissions.filter(
    (
      submission,
    ): submission is Extract<Submission, { readonly kind: "observation" }> =>
      submission.kind === "observation",
  );
  expect(
    observations.map((observation) => observation.metadata.timestamp),
  ).toEqual([
    new Date(NATIVE_CREATED_MS).toISOString(),
    new Date(NATIVE_CREATED_MS).toISOString(),
    new Date(CLOCK_MS).toISOString(),
  ]);
});

test("hands an unknown session to runtime synthesis", async () => {
  const submissions: Submission[] = [];
  const runtime = createSessionRuntime((submission) => {
    submissions.push(submission);
    return Promise.resolve({ ok: true });
  });
  const hooks = createV1Adapter(input, {
    runtime,
    report: () => undefined,
    now: () => new Date(CLOCK_MS),
  });

  await hooks.event({
    event: {
      type: "session.status",
      properties: { sessionID: "resumed-session", status: { type: "busy" } },
    },
  });
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
  await runtime.close();

  expect(submissions.map((submission) => submission.kind)).toEqual([
    "start",
    "observation",
    "end",
  ]);
});

test("contains malformed and unrepresentable events with payload-free diagnostics", async () => {
  const recording = recordingRuntime();
  const diagnostics: Diagnostic[] = [];
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    report: (diagnostic) => diagnostics.push(diagnostic),
    now: () => new Date(CLOCK_MS),
  });
  const cyclicProperties: {
    readonly sessionID: string;
    readonly status: JsonObject;
    cycle?: unknown;
  } = {
    sessionID: "cyclic-session",
    status: { type: "busy" },
  };
  cyclicProperties.cycle = cyclicProperties;

  await expect(hooks.event({ event: null })).resolves.toBeUndefined();
  await expect(hooks.event({ event: [] })).resolves.toBeUndefined();
  await expect(
    hooks.event({
      event: { type: "session.status", properties: cyclicProperties },
    }),
  ).resolves.toBeUndefined();
  await expect(
    hooks.event({
      event: {
        type: "session.status",
        properties: {
          sessionID: "invalid-session",
          status: undefined,
          invalidArray: [undefined],
          nonFinite: Number.NaN,
        },
      },
    }),
  ).resolves.toBeUndefined();

  expect(recording.submissions).toEqual([]);
  expect(diagnostics).toEqual([
    { category: "serialization", generation: "v1" },
    { category: "serialization", generation: "v1" },
    {
      category: "serialization",
      generation: "v1",
      nativeKind: "session.status",
      sessionId: "cyclic-session",
    },
    {
      category: "serialization",
      generation: "v1",
      nativeKind: "session.status",
      sessionId: "invalid-session",
    },
  ]);
});

test("reports a missing session.error identity without its payload", async () => {
  const recording = recordingRuntime();
  const diagnostics: Diagnostic[] = [];
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    report: (diagnostic) => diagnostics.push(diagnostic),
    now: () => new Date(CLOCK_MS),
  });

  await expect(
    hooks.event({
      event: {
        type: "session.error",
        properties: {
          error: { name: "UnknownError", data: { message: "secret-error" } },
        },
      },
    }),
  ).resolves.toBeUndefined();

  expect(recording.submissions).toEqual([]);
  expect(diagnostics).toEqual([
    {
      category: "serialization",
      generation: "v1",
      nativeKind: "session.error",
    },
  ]);
  expect(JSON.stringify(diagnostics)).not.toContain("secret-error");
});

test("contains runtime and reporter failures at the host callback boundary", async () => {
  const diagnostics: Diagnostic[] = [];
  const runtime: SessionRuntime = {
    start: () => undefined,
    observe: () => {
      throw new Error("runtime failure");
    },
    end: () => undefined,
    close: async () => undefined,
  };
  const hooks = createV1Adapter(input, {
    runtime,
    report: (diagnostic) => {
      diagnostics.push(diagnostic);
      throw new Error("reporter failure");
    },
    now: () => new Date(CLOCK_MS),
  });

  await expect(
    hooks.event({
      event: {
        type: "session.status",
        properties: { sessionID: SESSION_ID, status: { type: "busy" } },
      },
    }),
  ).resolves.toBeUndefined();
  expect(diagnostics).toEqual([
    {
      category: "serialization",
      generation: "v1",
      nativeKind: "session.status",
      sessionId: SESSION_ID,
    },
  ]);
});

// biome-ignore lint/complexity/noExcessiveLinesPerFunction: Keep the three-hook matrix in one assertion.
test("forwards prompt and tool hooks as opaque observations", async () => {
  const recording = recordingRuntime();
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    report: () => undefined,
    now: () => new Date(CLOCK_MS),
  });
  const promptInput = {
    sessionID: SESSION_ID,
    messageID: "message-hook",
    agent: "assistant",
  };
  const promptOutput = {
    message: { role: "user", content: "prompt-secret" },
    parts: [{ type: "text", text: "prompt-secret" }],
  };
  const toolBeforeInput = {
    tool: "read",
    sessionID: SESSION_ID,
    callID: "call-before",
  };
  const toolBeforeOutput = {
    args: { path: "tool-argument-secret" },
  };
  const toolAfterInput = {
    tool: "read",
    sessionID: SESSION_ID,
    callID: "call-after",
    args: { path: "tool-argument-secret" },
  };
  const toolAfterOutput = {
    title: "read result",
    output: "tool-output-secret",
    metadata: { source: "tool-metadata" },
  };

  await hooks["chat.message"](promptInput, promptOutput);
  await hooks["tool.execute.before"](toolBeforeInput, toolBeforeOutput);
  await hooks["tool.execute.after"](toolAfterInput, toolAfterOutput);

  const observations = recording.submissions.filter(
    (
      submission,
    ): submission is Extract<Submission, { readonly kind: "observation" }> =>
      submission.kind === "observation",
  );
  expect(observations.map((observation) => observation.hookType)).toEqual([
    "opencode.v1.chat.message",
    "opencode.v1.tool.execute.before",
    "opencode.v1.tool.execute.after",
  ]);
  expect(observations.map((observation) => observation.data)).toEqual([
    { input: promptInput, output: promptOutput },
    { input: toolBeforeInput, output: toolBeforeOutput },
    { input: toolAfterInput, output: toolAfterOutput },
  ]);
  expect(
    observations.every(
      (observation) =>
        observation.metadata.sessionId === SESSION_ID &&
        observation.metadata.currentWorkingDirectory === input.directory &&
        observation.metadata.projectName === "worktree" &&
        observation.metadata.timestamp === new Date(CLOCK_MS).toISOString(),
    ),
  ).toBe(true);
});

test("contains malformed hooks with payload-free diagnostics", async () => {
  const recording = recordingRuntime();
  const diagnostics: Diagnostic[] = [];
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    report: (diagnostic) => diagnostics.push(diagnostic),
    now: () => new Date(CLOCK_MS),
  });
  const cyclicOutput: { readonly secret: string; cycle?: unknown } = {
    secret: "cyclic-hook-secret",
  };
  cyclicOutput.cycle = cyclicOutput;

  await expect(
    hooks["chat.message"]({ sessionID: SESSION_ID }, cyclicOutput),
  ).resolves.toBeUndefined();
  await expect(
    hooks["tool.execute.before"](
      { tool: "read", sessionID: "invalid-hook", callID: "call" },
      { args: Symbol("invalid-hook-value") },
    ),
  ).resolves.toBeUndefined();

  expect(recording.submissions).toEqual([]);
  expect(diagnostics).toEqual([
    {
      category: "serialization",
      generation: "v1",
      nativeKind: "chat.message",
      sessionId: SESSION_ID,
    },
    {
      category: "serialization",
      generation: "v1",
      nativeKind: "tool.execute.before",
      sessionId: "invalid-hook",
    },
  ]);
  expect(JSON.stringify(diagnostics)).not.toContain("cyclic-hook-secret");
});

test("contains hook runtime and reporter failures at the host callback boundary", async () => {
  const diagnostics: Diagnostic[] = [];
  const runtime: SessionRuntime = {
    start: () => undefined,
    observe: () => {
      throw new Error("hook-runtime-secret");
    },
    end: () => undefined,
    close: async () => undefined,
  };
  const hooks = createV1Adapter(input, {
    runtime,
    report: (diagnostic) => {
      diagnostics.push(diagnostic);
      throw new Error("hook-reporter-secret");
    },
    now: () => new Date(CLOCK_MS),
  });

  await expect(
    hooks["tool.execute.after"](
      { tool: "read", sessionID: SESSION_ID, callID: "call" },
      { title: "result", output: "tool-output-secret", metadata: {} },
    ),
  ).resolves.toBeUndefined();
  expect(diagnostics).toEqual([
    {
      category: "serialization",
      generation: "v1",
      nativeKind: "tool.execute.after",
      sessionId: SESSION_ID,
    },
  ]);
  expect(JSON.stringify(diagnostics)).not.toContain("hook-runtime-secret");
  expect(JSON.stringify(diagnostics)).not.toContain("hook-reporter-secret");
});

test("stops hook input before idempotent runtime disposal and contains close failures", async () => {
  const submissions: Submission[] = [];
  const order: string[] = [];
  let closeCount = 0;
  let invokeHookDuringClose: (() => Promise<void>) | undefined;
  const runtime: SessionRuntime = {
    start: (metadata) => submissions.push({ kind: "start", metadata }),
    observe: (observation) => submissions.push(observation),
    end: (metadata) => submissions.push({ kind: "end", metadata }),
    close: async () => {
      closeCount += 1;
      order.push("runtime.close");
      await invokeHookDuringClose?.();
      throw new Error("close-secret");
    },
  };
  const hooks = createV1Adapter(input, {
    runtime,
    report: () => undefined,
    now: () => new Date(CLOCK_MS),
  });
  invokeHookDuringClose = () =>
    hooks["chat.message"](
      { sessionID: SESSION_ID },
      { message: { content: "post-dispose-secret" }, parts: [] },
    );

  const firstDispose = hooks.dispose();
  const secondDispose = hooks.dispose();
  await expect(firstDispose).resolves.toBeUndefined();
  await expect(secondDispose).resolves.toBeUndefined();

  expect(closeCount).toBe(1);
  expect(order).toEqual(["runtime.close"]);
  expect(submissions).toEqual([]);
});

function contextStub(resolve: (sessionId: string) => Promise<FrozenSnapshot>): {
  readonly context: StartupContextRuntime;
  readonly state: {
    readonly resolveCalls: string[];
    readonly removed: string[];
    closeCalls: number;
  };
} {
  const state = {
    resolveCalls: [] as string[],
    removed: [] as string[],
    closeCalls: 0,
  };
  const context: StartupContextRuntime = {
    resolve: (sessionId) => {
      state.resolveCalls.push(sessionId);
      return resolve(sessionId);
    },
    remove: (sessionId) => {
      state.removed.push(sessionId);
    },
    close: () => {
      state.closeCalls += 1;
      return Promise.resolve();
    },
  };
  return { context, state };
}

type UnknownTransform = (input: unknown, output: unknown) => Promise<void>;

function contextTransform(
  hooks: ReturnType<typeof createV1Adapter>,
): UnknownTransform {
  return hooks[
    "experimental.chat.system.transform"
  ] as unknown as UnknownTransform;
}

test("registers the pinned experimental system transform", () => {
  const recording = recordingRuntime();
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    report: () => undefined,
    now: () => new Date(CLOCK_MS),
  });

  expect(Object.hasOwn(hooks, "experimental.chat.system.transform")).toBe(true);
  expect(typeof contextTransform(hooks)).toBe("function");
});

test("appends the frozen snapshot verbatim on repeated transforms while preserving existing system entries", async () => {
  const recording = recordingRuntime();
  const stub = contextStub(async () => ({ kind: "text", text: FROZEN_TEXT }));
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    context: stub.context,
    report: () => undefined,
    now: () => new Date(CLOCK_MS),
  });
  const transform = contextTransform(hooks);
  const output = { system: ["host instructions"] };

  await transform({ sessionID: SESSION_ID }, output);
  await transform({ sessionID: SESSION_ID }, output);

  expect(output.system).toEqual([
    "host instructions",
    FROZEN_TEXT,
    FROZEN_TEXT,
  ]);
  expect(stub.state.resolveCalls).toEqual([SESSION_ID, SESSION_ID]);
  expect(recording.submissions).toEqual([]);
});

test("appends byte-identical text to concurrent transforms from the same frozen snapshot", async () => {
  const recording = recordingRuntime();
  let settleFrozen!: (snapshot: FrozenSnapshot) => void;
  const shared = new Promise<FrozenSnapshot>((resolvePending) => {
    settleFrozen = resolvePending;
  });
  const stub = contextStub(async () => shared);
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    context: stub.context,
    report: () => undefined,
    now: () => new Date(CLOCK_MS),
  });
  const transform = contextTransform(hooks);
  const primary = { system: ["primary host"] };
  const auxiliary = { system: ["auxiliary host"] };

  const pending = Promise.all([
    transform({ sessionID: SESSION_ID }, primary),
    transform({ sessionID: SESSION_ID }, auxiliary),
  ]);
  settleFrozen({ kind: "text", text: FROZEN_TEXT });
  await pending;

  expect(primary.system).toEqual(["primary host", FROZEN_TEXT]);
  expect(auxiliary.system).toEqual(["auxiliary host", FROZEN_TEXT]);
});

test("performs no lookup, mutation, or fallback without an explicit session identity", async () => {
  const recording = recordingRuntime();
  const stub = contextStub(async () => ({ kind: "text", text: FROZEN_TEXT }));
  const diagnostics: Diagnostic[] = [];
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    context: stub.context,
    report: (diagnostic) => diagnostics.push(diagnostic),
    now: () => new Date(CLOCK_MS),
  });
  const transform = contextTransform(hooks);
  const outputs = [
    { system: ["kept-missing"] },
    { system: ["kept-undefined"] },
    { system: ["kept-number"] },
    { system: ["kept-blank"] },
    { system: ["kept-nonrecord"] },
  ];

  await transform({}, outputs[0]);
  await transform({ sessionID: undefined }, outputs[1]);
  await transform({ sessionID: 42 }, outputs[2]);
  await transform({ sessionID: "   " }, outputs[3]);
  await transform(42, outputs[4]);

  expect(stub.state.resolveCalls).toEqual([]);
  expect(outputs.map((output) => output.system)).toEqual([
    ["kept-missing"],
    ["kept-undefined"],
    ["kept-number"],
    ["kept-blank"],
    ["kept-nonrecord"],
  ]);
  expect(diagnostics).toEqual([]);
  expect(recording.submissions).toEqual([]);
});

test("contributes nothing for an empty frozen snapshot", async () => {
  const recording = recordingRuntime();
  const stub = contextStub(async () => ({ kind: "empty" }));
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    context: stub.context,
    report: () => undefined,
    now: () => new Date(CLOCK_MS),
  });
  const transform = contextTransform(hooks);
  const output = { system: ["host only"] };

  await transform({ sessionID: SESSION_ID }, output);

  expect(output.system).toEqual(["host only"]);
  expect(stub.state.resolveCalls).toEqual([SESSION_ID]);
});

test("resolves on every transform instead of caching snapshots in the adapter", async () => {
  const recording = recordingRuntime();
  let lookups = 0;
  const stub = contextStub(() => {
    lookups += 1;
    if (lookups === 1) {
      return Promise.resolve({ kind: "text", text: FROZEN_TEXT });
    }
    return Promise.resolve({ kind: "empty" });
  });
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    context: stub.context,
    report: () => undefined,
    now: () => new Date(CLOCK_MS),
  });
  const transform = contextTransform(hooks);
  const first = { system: [] as string[] };
  const second = { system: [] as string[] };

  await transform({ sessionID: SESSION_ID }, first);
  await transform({ sessionID: SESSION_ID }, second);

  expect(first.system).toEqual([FROZEN_TEXT]);
  expect(second.system).toEqual([]);
  expect(stub.state.resolveCalls).toEqual([SESSION_ID, SESSION_ID]);
});

test("removes context state on session deletion while preserving capture deletion", async () => {
  const recording = recordingRuntime();
  const stub = contextStub(async () => ({ kind: "empty" }));
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    context: stub.context,
    report: () => undefined,
    now: () => new Date(CLOCK_MS),
  });

  await hooks.event({
    event: { type: "session.deleted", properties: { info: sessionInfo() } },
  });

  expect(stub.state.removed).toEqual([SESSION_ID]);
  expect(recording.submissions.map((submission) => submission.kind)).toEqual([
    "observation",
    "end",
  ]);
});

test("keeps capture unchanged and never submits injected startup memory", async () => {
  const recording = recordingRuntime();
  const diagnostics: Diagnostic[] = [];
  const stub = contextStub(async () => ({ kind: "text", text: FROZEN_TEXT }));
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    context: stub.context,
    report: (diagnostic) => diagnostics.push(diagnostic),
    now: () => new Date(CLOCK_MS),
  });
  const transform = contextTransform(hooks);

  await hooks.event({
    event: { type: "session.created", properties: { info: sessionInfo() } },
  });
  await hooks["chat.message"](
    { sessionID: SESSION_ID },
    { message: { role: "user", content: "prompt" }, parts: [] },
  );
  const output = { system: ["host"] };
  await transform({ sessionID: SESSION_ID }, output);
  await hooks.event({
    event: { type: "session.deleted", properties: { info: sessionInfo() } },
  });

  const observations = recording.submissions.filter(
    (
      submission,
    ): submission is Extract<Submission, { readonly kind: "observation" }> =>
      submission.kind === "observation",
  );
  expect(observations.map((observation) => observation.hookType)).toEqual([
    "opencode.v1.session.created",
    "opencode.v1.chat.message",
    "opencode.v1.session.deleted",
  ]);
  expect(output.system).toEqual(["host", FROZEN_TEXT]);
  expect(JSON.stringify(recording.submissions)).not.toContain(
    "startup-memory-fixture",
  );
  expect(diagnostics).toEqual([]);
});

test("contains transform failures with payload-free diagnostics", async () => {
  const recording = recordingRuntime();
  const failing = contextStub(() =>
    Promise.reject(new Error("resolve-secret")),
  );
  const diagnostics: Diagnostic[] = [];
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    context: failing.context,
    report: (diagnostic) => diagnostics.push(diagnostic),
    now: () => new Date(CLOCK_MS),
  });
  const transform = contextTransform(hooks);
  const rejected = { system: ["kept"] };

  await transform({ sessionID: SESSION_ID }, rejected);

  expect(rejected.system).toEqual(["kept"]);
  expect(diagnostics).toEqual([
    {
      category: "serialization",
      generation: "v1",
      nativeKind: "experimental.chat.system.transform",
      sessionId: SESSION_ID,
    },
  ]);
  expect(JSON.stringify(diagnostics)).not.toContain("resolve-secret");

  const unusable = contextStub(async () => ({ kind: "empty" }));
  const unusableHooks = createV1Adapter(input, {
    runtime: recording.runtime,
    context: unusable.context,
    report: (diagnostic) => diagnostics.push(diagnostic),
    now: () => new Date(CLOCK_MS),
  });
  const unusableTransform = contextTransform(unusableHooks);
  const malformed = { system: "not-an-array" };

  await unusableTransform({ sessionID: SESSION_ID }, malformed);

  expect(unusable.state.resolveCalls).toEqual([]);
  expect(diagnostics).toEqual([
    {
      category: "serialization",
      generation: "v1",
      nativeKind: "experimental.chat.system.transform",
      sessionId: SESSION_ID,
    },
    {
      category: "serialization",
      generation: "v1",
      nativeKind: "experimental.chat.system.transform",
      sessionId: SESSION_ID,
    },
  ]);
});

test("leaves context shutdown to the coordinator and stops injection after disposal", async () => {
  const recording = recordingRuntime();
  const stub = contextStub(async () => ({ kind: "text", text: FROZEN_TEXT }));
  const hooks = createV1Adapter(input, {
    runtime: recording.runtime,
    context: stub.context,
    report: () => undefined,
    now: () => new Date(CLOCK_MS),
  });
  const transform = contextTransform(hooks);
  const output = { system: ["host"] };

  await hooks.dispose();
  await transform({ sessionID: SESSION_ID }, output);

  expect(output.system).toEqual(["host"]);
  expect(stub.state.resolveCalls).toEqual([]);
  expect(stub.state.closeCalls).toBe(0);
});
