import assert from "node:assert/strict";
import test from "node:test";

import type { ExtensionAPI, ExtensionFactory } from "@earendil-works/pi-coding-agent";

import type { AdapterContext } from "../src/adapter.ts";
import type { ExtensionConfig } from "../src/config.ts";
import extension, { createExtensionForTesting } from "../src/index.ts";
import type { DispatchResult, EventMetadata, Submission } from "../src/model.ts";
import { createSubmissionQueue } from "../src/queue.ts";
import type { SubmissionQueue } from "../src/queue.ts";

const factory: ExtensionFactory = extension;
const approvedEvents = [
  "session_start",
  "session_shutdown",
  "session_info_changed",
  "session_compact",
  "session_compact_failed",
  "agent_start",
  "agent_settled",
  "ui_prompt_start",
  "ui_prompt_end",
  "turn_start",
  "turn_end",
  "message_end",
  "tool_execution_start",
  "tool_execution_end",
] as const;

type RecordedHandler = (event: unknown, context: AdapterContext) => unknown;

type QueueOperation =
  | { readonly kind: "close"; readonly metadata: EventMetadata }
  | { readonly kind: "observe"; readonly submission: Extract<Submission, { kind: "observation" }> }
  | { readonly kind: "start"; readonly metadata: EventMetadata };

interface RecordedSubscription {
  readonly event: string;
  readonly handler: RecordedHandler;
}

interface RecordingPiApi {
  readonly attemptedEvents: string[];
  readonly subscriptions: RecordedSubscription[];
  readonly unsubscribeCalls: string[];
  readonly on: (event: string, handler: RecordedHandler) => () => void;
}

interface RecordingOptions {
  readonly failRegistrationFor?: ReadonlySet<string>;
  readonly failUnsubscribeFor?: ReadonlySet<string>;
  readonly failureMessage?: string;
}

interface RecordingQueue {
  readonly operations: QueueOperation[];
  readonly queue: SubmissionQueue;
}

interface Deferred {
  readonly promise: Promise<void>;
  resolve(): void;
}

interface PendingDispatch {
  readonly submission: Submission;
  resolve(result: DispatchResult): void;
}

const testConfig: ExtensionConfig = {
  executable: "harness-events",
  executionTimeoutMs: 35_000,
  observationCapacity: 256,
  shutdownTimeoutMs: 5_000,
};

const testClock = { now: () => new Date("2026-09-21T12:34:56.789Z") };
const handlerEntryTimestamp = "2026-09-22T12:34:56.789Z";

function createDeferred(): Deferred {
  let resolvePromise: (() => void) | undefined;
  const promise = new Promise<void>((resolve) => {
    resolvePromise = resolve;
  });

  return {
    promise,
    resolve: () => {
      if (resolvePromise === undefined) {
        throw new Error("Deferred promise was not initialized");
      }

      resolvePromise();
    },
  };
}

function createRecordingQueue(closePromise: Promise<void> = Promise.resolve()): RecordingQueue {
  const operations: QueueOperation[] = [];

  return {
    operations,
    queue: {
      close: (metadata) => {
        operations.push({ kind: "close", metadata });
        return closePromise;
      },
      observe: (submission) => {
        operations.push({ kind: "observe", submission });
      },
      start: (metadata) => {
        operations.push({ kind: "start", metadata });
      },
    },
  };
}

async function pendingDispatchAt(
  pending: readonly PendingDispatch[],
  index: number,
): Promise<PendingDispatch> {
  for (let attempt = 0; attempt < 4; attempt += 1) {
    const dispatch = pending[index];

    if (dispatch !== undefined) {
      return dispatch;
    }

    await Promise.resolve();
  }

  throw new Error(`Expected pending dispatch ${index}`);
}

function createTestFactory(
  queue: SubmissionQueue = createRecordingQueue().queue,
  clock = testClock,
): ExtensionFactory {
  return createExtensionForTesting({
    clock,
    createQueue: () => queue,
    loadConfig: () => testConfig,
  });
}

function createHandlerEntryProbe(type: string) {
  const accessOrder: string[] = [];

  return {
    accessOrder,
    clock: {
      now: () => {
        accessOrder.push("clock");
        return new Date(handlerEntryTimestamp);
      },
    },
    context: {
      get cwd(): string {
        accessOrder.push("context.cwd");
        return "/workspace/example-project";
      },
      get sessionManager(): { readonly getSessionId: () => unknown } {
        accessOrder.push("context.sessionManager");
        return {
          getSessionId: () => {
            accessOrder.push("context.sessionId");
            return "handler-entry-session";
          },
        };
      },
    },
    event: new Proxy(
      { type },
      {
        get: (target, property, receiver) => {
          accessOrder.push(`event.get.${String(property)}`);
          return Reflect.get(target, property, receiver);
        },
        getOwnPropertyDescriptor: (target, property) => {
          accessOrder.push(`event.descriptor.${String(property)}`);
          return Reflect.getOwnPropertyDescriptor(target, property);
        },
      },
    ),
  };
}

function createRecordingPiApi(options: RecordingOptions = {}): RecordingPiApi {
  const attemptedEvents: string[] = [];
  const subscriptions: RecordedSubscription[] = [];
  const unsubscribeCalls: string[] = [];

  return {
    attemptedEvents,
    subscriptions,
    unsubscribeCalls,
    on: (event, handler): (() => void) => {
      attemptedEvents.push(event);

      if (options.failRegistrationFor?.has(event)) {
        throw new Error(options.failureMessage ?? "registration failed");
      }

      subscriptions.push({ event, handler });

      return (): void => {
        unsubscribeCalls.push(event);

        if (options.failUnsubscribeFor?.has(event)) {
          throw new Error(options.failureMessage ?? "unsubscribe failed");
        }
      };
    },
  };
}

function runFactory(
  api: RecordingPiApi,
  extensionFactory: ExtensionFactory = createTestFactory(),
): void {
  extensionFactory(api as unknown as ExtensionAPI);
}

async function invoke(
  api: RecordingPiApi,
  eventName: string,
  event: unknown,
  context: AdapterContext,
): Promise<unknown> {
  const subscription = api.subscriptions.find(
    ({ event: subscribedEvent }) => subscribedEvent === eventName,
  );
  assert.ok(subscription, `missing ${eventName} subscription`);
  return await subscription.handler(event, context);
}

async function captureDiagnostics<T>(action: () => T | Promise<T>): Promise<{
  readonly diagnostics: unknown[];
  readonly value: T;
}> {
  const diagnostics: unknown[] = [];
  const originalError = console.error;
  console.error = (...values: unknown[]): void => {
    diagnostics.push(values.length === 1 ? values[0] : values);
  };

  try {
    return { diagnostics, value: await action() };
  } finally {
    console.error = originalError;
  }
}

function contextFor(sessionId: string): AdapterContext {
  return {
    cwd: "/workspace/example-project",
    sessionManager: { getSessionId: () => sessionId },
  };
}

function eventFor(event: (typeof approvedEvents)[number]): unknown {
  switch (event) {
    case "session_start":
      return { reason: "startup", type: event };
    case "session_shutdown":
      return { reason: "quit", type: event };
    case "session_info_changed":
      return { name: "example", type: event };
    case "session_compact":
      return {
        compactionEntry: {
          firstKeptEntryId: "entry-1",
          id: "compact-1",
          parentId: null,
          timestamp: "2026-09-21T12:34:56.789Z",
          tokensBefore: 10,
        },
        fromExtension: false,
        reason: "manual",
        type: event,
        willRetry: false,
      };
    case "session_compact_failed":
      return {
        aborted: false,
        fromExtension: false,
        reason: "manual",
        type: event,
        willRetry: false,
      };
    case "agent_start":
    case "agent_settled":
      return { type: event };
    case "ui_prompt_start":
    case "ui_prompt_end":
      return { kind: "confirm", reason: "ui_prompt", type: event };
    case "turn_start":
      return { timestamp: 1_726_922_096_789, turnIndex: 7, type: event };
    case "turn_end":
      return { turnIndex: 7, type: event };
    case "message_end":
      return {
        message: {
          content: "accepted user text",
          role: "user",
          timestamp: 1_726_922_097_000,
        },
        type: event,
      };
    case "tool_execution_start":
      return { args: { query: "accepted" }, toolCallId: "call-1", toolName: "tool", type: event };
    case "tool_execution_end":
      return {
        isError: false,
        result: { output: "accepted" },
        toolCallId: "call-1",
        toolName: "tool",
        type: event,
      };
  }
}

test("exports a default extension factory", () => {
  assert.equal(typeof factory, "function");
});

test("registers exactly the approved native Pi events", () => {
  const api = createRecordingPiApi();
  runFactory(api);

  assert.deepEqual(api.attemptedEvents, approvedEvents);
  assert.deepEqual(
    api.subscriptions.map(({ event }) => event),
    approvedEvents,
  );
  assert.equal(
    api.subscriptions.some(({ event }) => event === "agent_end"),
    false,
  );
  assert.equal(
    api.subscriptions.some(({ event }) => event === "message_update"),
    false,
  );
  assert.equal(
    api.subscriptions.some(({ event }) => event === "tool_execution_update"),
    false,
  );
});

test("converts each approved native event through the host boundary", async () => {
  const api = createRecordingPiApi();
  runFactory(api);

  const { diagnostics } = await captureDiagnostics(async () => {
    for (const event of approvedEvents) {
      assert.equal(
        await invoke(api, event, eventFor(event), contextFor("session-123")),
        undefined,
        event,
      );
    }
  });

  assert.deepEqual(diagnostics, []);
});

test("contains registration failures and registers remaining approved events", async () => {
  const failureMessage = "registration-failure-sentinel";
  const api = createRecordingPiApi({
    failRegistrationFor: new Set(["agent_settled"]),
    failureMessage,
  });
  const { diagnostics } = await captureDiagnostics(() => runFactory(api));

  assert.deepEqual(api.attemptedEvents, approvedEvents);
  assert.deepEqual(
    api.subscriptions.map(({ event }) => event),
    approvedEvents.filter((event) => event !== "agent_settled"),
  );
  assert.deepEqual(diagnostics, [{ category: "normalization", nativeEvent: "agent_settled" }]);
  assert.doesNotMatch(JSON.stringify(diagnostics), new RegExp(failureMessage));
});

test("runs every approved handler normally after injected failures, including message_end", async () => {
  const payloadSentinel = "handler-failure-sentinel";
  const api = createRecordingPiApi();
  runFactory(api);
  const failingContext = Object.defineProperty({}, "cwd", {
    get(): never {
      throw new Error(payloadSentinel);
    },
  }) as AdapterContext;

  const { diagnostics } = await captureDiagnostics(async () => {
    for (const event of approvedEvents) {
      assert.equal(await invoke(api, event, eventFor(event), failingContext), undefined, event);
    }
  });

  assert.deepEqual(
    diagnostics,
    approvedEvents.slice(0, 2).map((nativeEvent) => ({ category: "normalization", nativeEvent })),
  );
  assert.doesNotMatch(JSON.stringify(diagnostics), new RegExp(payloadSentinel));
});

test("reads current handler context and projects malformed observations through the safe boundary", async () => {
  const api = createRecordingPiApi();
  runFactory(api);

  const { diagnostics } = await captureDiagnostics(async () => {
    assert.equal(
      await invoke(
        api,
        "turn_start",
        { timestamp: 1_726_922_096_789, type: "turn_start" },
        contextFor("session-first"),
      ),
      undefined,
    );
    assert.equal(
      await invoke(
        api,
        "turn_start",
        { timestamp: 1_726_922_096_789, type: "turn_start" },
        contextFor("session-replacement"),
      ),
      undefined,
    );
  });

  assert.deepEqual(diagnostics, [
    { category: "normalization", nativeEvent: "turn_start", sessionId: "session-first" },
    { category: "normalization", nativeEvent: "turn_start", sessionId: "session-replacement" },
  ]);
});

test("captures observation fallback time before native-event and context access", async () => {
  const recording = createRecordingQueue();
  const api = createRecordingPiApi();
  const probe = createHandlerEntryProbe("agent_start");

  runFactory(api, createTestFactory(recording.queue, probe.clock));

  const { diagnostics, value } = await captureDiagnostics(() =>
    invoke(api, "agent_start", probe.event, probe.context),
  );

  assert.equal(value, undefined);
  assert.deepEqual(diagnostics, []);
  assert.equal(probe.accessOrder[0], "clock");
  assert.ok(probe.accessOrder.indexOf("clock") < probe.accessOrder.indexOf("event.get.type"));
  assert.ok(
    probe.accessOrder.indexOf("clock") < probe.accessOrder.indexOf("context.sessionManager"),
  );
  assert.ok(probe.accessOrder.indexOf("clock") < probe.accessOrder.indexOf("context.cwd"));
  assert.deepEqual(recording.operations, [
    {
      kind: "observe",
      submission: {
        data: {},
        hookType: "pi.agent_start",
        kind: "observation",
        metadata: {
          currentWorkingDirectory: "/workspace/example-project",
          projectName: "example-project",
          sessionId: "handler-entry-session",
          timestamp: handlerEntryTimestamp,
        },
      },
    },
  ]);

  await invoke(
    api,
    "session_shutdown",
    { type: "session_shutdown" },
    contextFor("handler-entry-session"),
  );
  const clockReadsBeforeClosedHandler = probe.accessOrder.filter(
    (value) => value === "clock",
  ).length;
  let closedEventRead = false;
  let closedContextRead = false;
  const closedEvent = Object.defineProperty({}, "type", {
    get(): never {
      closedEventRead = true;
      throw new Error("closed-event-sentinel");
    },
  });
  const closedContext = Object.defineProperty({}, "cwd", {
    get(): never {
      closedContextRead = true;
      throw new Error("closed-context-sentinel");
    },
  }) as AdapterContext;
  const closedResult = await captureDiagnostics(() =>
    invoke(api, "agent_start", closedEvent, closedContext),
  );

  assert.equal(closedResult.value, undefined);
  assert.deepEqual(closedResult.diagnostics, []);
  assert.equal(
    probe.accessOrder.filter((value) => value === "clock").length,
    clockReadsBeforeClosedHandler + 1,
  );
  assert.equal(closedEventRead, false);
  assert.equal(closedContextRead, false);
});

test("captures shutdown fallback time before native-event and context access", async () => {
  const recording = createRecordingQueue();
  const api = createRecordingPiApi();
  const probe = createHandlerEntryProbe("session_shutdown");

  runFactory(api, createTestFactory(recording.queue, probe.clock));

  const { diagnostics, value } = await captureDiagnostics(() =>
    invoke(api, "session_shutdown", probe.event, probe.context),
  );

  assert.equal(value, undefined);
  assert.deepEqual(diagnostics, []);
  assert.equal(probe.accessOrder[0], "clock");
  assert.ok(probe.accessOrder.indexOf("clock") < probe.accessOrder.indexOf("event.get.type"));
  assert.ok(
    probe.accessOrder.indexOf("clock") < probe.accessOrder.indexOf("context.sessionManager"),
  );
  assert.ok(probe.accessOrder.indexOf("clock") < probe.accessOrder.indexOf("context.cwd"));
  assert.deepEqual(recording.operations, [
    {
      kind: "close",
      metadata: {
        currentWorkingDirectory: "/workspace/example-project",
        projectName: "example-project",
        sessionId: "handler-entry-session",
        timestamp: handlerEntryTimestamp,
      },
    },
  ]);

  const clockReadsBeforeDuplicateShutdown = probe.accessOrder.filter(
    (value) => value === "clock",
  ).length;
  let duplicateEventRead = false;
  let duplicateContextRead = false;
  const duplicateEvent = Object.defineProperty({}, "type", {
    get(): never {
      duplicateEventRead = true;
      throw new Error("duplicate-event-sentinel");
    },
  });
  const duplicateContext = Object.defineProperty({}, "cwd", {
    get(): never {
      duplicateContextRead = true;
      throw new Error("duplicate-context-sentinel");
    },
  }) as AdapterContext;
  const duplicateResult = await captureDiagnostics(() =>
    invoke(api, "session_shutdown", duplicateEvent, duplicateContext),
  );

  assert.equal(duplicateResult.value, undefined);
  assert.deepEqual(duplicateResult.diagnostics, []);
  assert.equal(
    probe.accessOrder.filter((value) => value === "clock").length,
    clockReadsBeforeDuplicateShutdown + 1,
  );
  assert.equal(duplicateEventRead, false);
  assert.equal(duplicateContextRead, false);
});

test("releases retained subscriptions at shutdown and contains teardown failures", async () => {
  const failureMessage = "unsubscribe-failure-sentinel";
  const api = createRecordingPiApi({
    failUnsubscribeFor: new Set(["agent_start"]),
    failureMessage,
  });
  runFactory(api);

  const { diagnostics, value } = await captureDiagnostics(() =>
    invoke(api, "session_shutdown", eventFor("session_shutdown"), contextFor("session-123")),
  );

  assert.equal(value, undefined);
  assert.deepEqual(api.unsubscribeCalls, approvedEvents);
  assert.deepEqual(diagnostics, [{ category: "normalization", nativeEvent: "session_shutdown" }]);
  assert.doesNotMatch(JSON.stringify(diagnostics), new RegExp(failureMessage));
});

test("routes a resumed session through one queue without awaiting ordinary telemetry", async () => {
  const deferredClose = createDeferred();
  const recording = createRecordingQueue(deferredClose.promise);
  const configs: ExtensionConfig[] = [];
  const api = createRecordingPiApi();
  let subscriptionsReleasedBeforeClose = false;
  const queue: SubmissionQueue = {
    close: (metadata) => {
      subscriptionsReleasedBeforeClose = api.unsubscribeCalls.length === approvedEvents.length;
      return recording.queue.close(metadata);
    },
    observe: recording.queue.observe,
    start: recording.queue.start,
  };
  const integratedFactory = createExtensionForTesting({
    clock: testClock,
    createQueue: (config) => {
      configs.push(config);
      return queue;
    },
    loadConfig: () => testConfig,
  });
  runFactory(api, integratedFactory);

  assert.deepEqual(configs, [testConfig]);
  assert.equal(
    await invoke(
      api,
      "session_start",
      { reason: "resume", type: "session_start" },
      contextFor("resumed-session"),
    ),
    undefined,
  );
  assert.equal(
    await invoke(api, "agent_start", { type: "agent_start" }, contextFor("resumed-session")),
    undefined,
  );

  const shutdown = invoke(
    api,
    "session_shutdown",
    { reason: "reload", type: "session_shutdown" },
    contextFor("resumed-session"),
  );
  let shutdownSettled = false;
  void shutdown.then(() => {
    shutdownSettled = true;
  });

  assert.deepEqual(recording.operations, [
    {
      kind: "start",
      metadata: {
        currentWorkingDirectory: "/workspace/example-project",
        projectName: "example-project",
        sessionId: "resumed-session",
        timestamp: "2026-09-21T12:34:56.789Z",
      },
    },
    {
      kind: "observe",
      submission: {
        data: {},
        hookType: "pi.agent_start",
        kind: "observation",
        metadata: {
          currentWorkingDirectory: "/workspace/example-project",
          projectName: "example-project",
          sessionId: "resumed-session",
          timestamp: "2026-09-21T12:34:56.789Z",
        },
      },
    },
    {
      kind: "close",
      metadata: {
        currentWorkingDirectory: "/workspace/example-project",
        projectName: "example-project",
        sessionId: "resumed-session",
        timestamp: "2026-09-21T12:34:56.789Z",
      },
    },
  ]);
  assert.deepEqual(api.unsubscribeCalls, approvedEvents);
  assert.equal(subscriptionsReleasedBeforeClose, true);
  assert.equal(shutdownSettled, false);

  const duplicateShutdown = invoke(
    api,
    "session_shutdown",
    { reason: "reload", type: "session_shutdown" },
    contextFor("resumed-session"),
  );
  assert.equal(recording.operations.length, 3);
  assert.equal(
    await invoke(api, "agent_start", { type: "agent_start" }, contextFor("resumed-session")),
    undefined,
  );
  assert.equal(recording.operations.length, 3);

  deferredClose.resolve();
  assert.equal(await shutdown, undefined);
  assert.equal(await duplicateShutdown, undefined);
  assert.equal(shutdownSettled, true);
});

test("returns non-shutdown Pi handlers before dispatch completes and awaits shutdown drain", async () => {
  const pending: PendingDispatch[] = [];
  const queue = createSubmissionQueue(
    testConfig,
    (submission) =>
      new Promise<DispatchResult>((resolve) => {
        pending.push({ resolve, submission });
      }),
  );
  const api = createRecordingPiApi();
  runFactory(api, createTestFactory(queue));

  assert.equal(
    await invoke(
      api,
      "session_start",
      { reason: "startup", type: "session_start" },
      contextFor("asynchronous-session"),
    ),
    undefined,
  );
  assert.equal(
    await invoke(api, "agent_start", { type: "agent_start" }, contextFor("asynchronous-session")),
    undefined,
  );

  const shutdown = invoke(
    api,
    "session_shutdown",
    { reason: "quit", type: "session_shutdown" },
    contextFor("asynchronous-session"),
  );
  let shutdownSettled = false;
  void shutdown.then(() => {
    shutdownSettled = true;
  });

  const start = await pendingDispatchAt(pending, 0);
  assert.equal(start.submission.kind, "start");
  assert.equal(pending.length, 1);
  assert.equal(shutdownSettled, false);

  start.resolve({ ok: true });
  const observation = await pendingDispatchAt(pending, 1);
  assert.equal(observation.submission.kind, "observation");
  assert.equal(shutdownSettled, false);

  observation.resolve({ ok: true });
  const end = await pendingDispatchAt(pending, 2);
  assert.equal(end.submission.kind, "end");
  assert.equal(shutdownSettled, false);

  end.resolve({ ok: true });
  assert.equal(await shutdown, undefined);
  assert.equal(shutdownSettled, true);
  assert.deepEqual(
    pending.map(({ submission }) => submission.kind),
    ["start", "observation", "end"],
  );
});

for (const reason of ["startup", "resume"] as const) {
  test(`preserves ${reason} start-before-observation order`, async () => {
    const recording = createRecordingQueue();
    const api = createRecordingPiApi();
    runFactory(api, createTestFactory(recording.queue));

    await invoke(
      api,
      "session_start",
      { reason, type: "session_start" },
      contextFor(`${reason}-session`),
    );
    await invoke(api, "agent_start", { type: "agent_start" }, contextFor(`${reason}-session`));
    await invoke(
      api,
      "session_shutdown",
      { reason: "quit", type: "session_shutdown" },
      contextFor(`${reason}-session`),
    );

    assert.deepEqual(
      recording.operations.map(({ kind }) => kind),
      ["start", "observe", "close"],
    );
  });
}

for (const reason of ["reload", "new", "fork"] as const) {
  test(`ends the prior session before ${reason} replacement starts`, async () => {
    const prior = createRecordingQueue();
    const replacement = createRecordingQueue();
    const priorApi = createRecordingPiApi();
    runFactory(priorApi, createTestFactory(prior.queue));

    await invoke(
      priorApi,
      "session_start",
      { reason: "startup", type: "session_start" },
      contextFor(`prior-${reason}`),
    );
    await invoke(priorApi, "agent_start", { type: "agent_start" }, contextFor(`prior-${reason}`));
    await invoke(
      priorApi,
      "session_shutdown",
      { reason, type: "session_shutdown" },
      contextFor(`prior-${reason}`),
    );

    assert.deepEqual(
      prior.operations.map(({ kind }) => kind),
      ["start", "observe", "close"],
    );

    const replacementApi = createRecordingPiApi();
    runFactory(replacementApi, createTestFactory(replacement.queue));
    await invoke(
      replacementApi,
      "session_start",
      { reason, type: "session_start" },
      contextFor(`replacement-${reason}`),
    );
    await invoke(
      replacementApi,
      "agent_start",
      { type: "agent_start" },
      contextFor(`replacement-${reason}`),
    );
    await invoke(
      replacementApi,
      "session_shutdown",
      { reason: "quit", type: "session_shutdown" },
      contextFor(`replacement-${reason}`),
    );

    assert.deepEqual(
      replacement.operations.map(({ kind }) => kind),
      ["start", "observe", "close"],
    );
  });
}

test("contains queue failures without exposing error content or blocking Pi", async () => {
  const failureSentinel = "queue-failure-sentinel";
  const queue: SubmissionQueue = {
    close: () => Promise.reject(new Error(failureSentinel)),
    observe: () => {
      throw new Error(failureSentinel);
    },
    start: () => {
      throw new Error(failureSentinel);
    },
  };
  const api = createRecordingPiApi();
  runFactory(api, createTestFactory(queue));

  const { diagnostics } = await captureDiagnostics(async () => {
    assert.equal(
      await invoke(
        api,
        "session_start",
        { reason: "startup", type: "session_start" },
        contextFor("failing-session"),
      ),
      undefined,
    );
    assert.equal(
      await invoke(api, "agent_start", { type: "agent_start" }, contextFor("failing-session")),
      undefined,
    );
    assert.equal(
      await invoke(
        api,
        "session_shutdown",
        { reason: "quit", type: "session_shutdown" },
        contextFor("failing-session"),
      ),
      undefined,
    );
  });

  assert.deepEqual(diagnostics, [
    { category: "normalization", nativeEvent: "session_start" },
    { category: "normalization", nativeEvent: "agent_start" },
    { category: "normalization", nativeEvent: "session_shutdown" },
  ]);
  for (const diagnostic of diagnostics) {
    assert.equal(Object.isFrozen(diagnostic), true);
  }
  assert.doesNotMatch(JSON.stringify(diagnostics), new RegExp(failureSentinel));
});

test("contains factory configuration failures while retaining Pi subscriptions", async () => {
  const failureSentinel = "factory-failure-sentinel";
  const api = createRecordingPiApi();
  const failedFactory = createExtensionForTesting({
    loadConfig: () => {
      throw new Error(failureSentinel);
    },
  });
  const { diagnostics } = await captureDiagnostics(() => runFactory(api, failedFactory));

  assert.deepEqual(api.attemptedEvents, approvedEvents);
  assert.deepEqual(diagnostics, [{ category: "configuration" }]);
  assert.equal(Object.isFrozen(diagnostics[0]), true);
  assert.doesNotMatch(JSON.stringify(diagnostics), new RegExp(failureSentinel));
});
