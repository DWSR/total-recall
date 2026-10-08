// biome-ignore-all lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
// biome-ignore-all lint/correctness/useQwikValidLexicalScope: Tests intentionally use closures for injected adapter seams.
// biome-ignore-all lint/style/noExcessiveLinesPerFile: V1 matrix and failure isolation share one adapter fixture.
// biome-ignore-all lint/style/useNamingConvention: Native OpenCode fields retain their SDK spelling.

import { expect, test } from "bun:test";
import { createSessionRuntime, type SessionRuntime } from "../src/runtime";
import type { Diagnostic, JsonObject, Submission } from "../src/model";
import { createV1Adapter, type V1PluginInput } from "../src/v1";

const NATIVE_CREATED_MS = Date.parse("2026-09-20T12:34:56.789Z");
const CLOCK_MS = Date.parse("2026-09-20T13:14:15.016Z");
const SESSION_UPDATE_OFFSET_MS = 1000;
const SESSION_ID = "session-v1";

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
