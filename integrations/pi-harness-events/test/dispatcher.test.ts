import assert from "node:assert/strict";
import process from "node:process";
import test from "node:test";

import type { ExtensionConfig } from "../src/config.ts";
import {
  MAXIMUM_STDERR_CAPTURE_BYTES,
  createDispatcher,
  type ChildInput,
  type ChildOutput,
  type DispatcherTimer,
  type DispatcherTimers,
  type ProcessSpawner,
  type ProcessSpawnOptions,
  type SpawnedChild,
} from "../src/dispatcher.ts";
import type { EventMetadata, JsonObject, Submission } from "../src/model.ts";

const config: ExtensionConfig = {
  executable: "capture-harness-events --not-a-shell-command",
  executionTimeoutMs: 35_000,
  shutdownTimeoutMs: 5_000,
  observationCapacity: 256,
};

const metadata: EventMetadata = {
  sessionId: "session-123",
  projectName: "project",
  currentWorkingDirectory: "/workspace/project",
  timestamp: "2026-09-21T12:34:56.789Z",
};

type ChildErrorListener = (error: Error) => void;
type ChildExitListener = (code: number | null, signal: NodeJS.Signals | null) => void;
type ChildKillHandler = (signal: NodeJS.Signals | number | undefined) => void;

class RecordingInput implements ChildInput {
  readonly endCalls: (string | undefined)[] = [];

  end(chunk?: string): void {
    this.endCalls.push(chunk);
  }

  once(_event: "error", _listener: ChildErrorListener): void {}
}

class RecordingOutput implements ChildOutput {
  on(_event: "data", _listener: (chunk: Uint8Array) => void): void {}

  once(_event: "error", _listener: (error: Error) => void): void {}

  resume(): void {}
}

class RecordingChild implements SpawnedChild {
  readonly stderr = new RecordingOutput();
  readonly stdin: RecordingInput;
  readonly stdout = new RecordingOutput();

  constructor(input: RecordingInput) {
    this.stdin = input;
  }

  once(event: "error", listener: ChildErrorListener): this;
  once(event: "exit", listener: ChildExitListener): this;
  once(...[event, listener]: ["error", ChildErrorListener] | ["exit", ChildExitListener]): this {
    if (event === "exit") {
      queueMicrotask(() => listener(0, null));
    }

    return this;
  }

  kill(_signal?: NodeJS.Signals | number): boolean {
    return true;
  }
}

interface RecordedInvocation {
  readonly executable: string;
  readonly arguments_: readonly string[];
  readonly input: RecordingInput;
  readonly options: ProcessSpawnOptions;
}

interface RecordingProcess {
  readonly invocations: RecordedInvocation[];
  readonly spawn: ProcessSpawner;
}

function createRecordingProcess(): RecordingProcess {
  const invocations: RecordedInvocation[] = [];

  const spawn: ProcessSpawner = (executable, arguments_, options) => {
    const input = new RecordingInput();
    invocations.push({ executable, arguments_, input, options });
    return { kind: "spawned", child: new RecordingChild(input) };
  };

  return { invocations, spawn };
}

function invocationAt(recording: RecordingProcess, index: number): RecordedInvocation {
  const invocation = recording.invocations[index];

  if (invocation === undefined) {
    throw new Error(`Expected invocation ${index}`);
  }

  return invocation;
}

function replaceEnvironmentValue(name: string, value: string): () => void {
  const previous = process.env[name];
  process.env[name] = value;

  return () => {
    if (previous === undefined) {
      delete process.env[name];
      return;
    }

    process.env[name] = previous;
  };
}

function assertInvocationOptions(invocation: RecordedInvocation): void {
  assert.equal(invocation.executable, config.executable);
  assert.equal(invocation.executable.includes("session-"), false);
  assert.equal(invocation.options.cwd, metadata.currentWorkingDirectory);
  assert.equal(invocation.options.env, process.env);
  assert.equal(invocation.options.env.III_URL, "routing-url-sentinel");
  assert.equal(invocation.options.env.III_NAMESPACE, "routing-namespace-sentinel");
  assert.equal(invocation.options.env.INHERITED_DISPATCHER_SENTINEL, "inherited-sentinel");
  assert.equal(invocation.options.shell, false);
  assert.deepEqual(invocation.options.stdio, ["pipe", "pipe", "pipe"]);
}

test("spawns start and end commands with exact lifecycle arguments and empty closed stdin", async (t) => {
  const restoreUrl = replaceEnvironmentValue("III_URL", "routing-url-sentinel");
  const restoreNamespace = replaceEnvironmentValue("III_NAMESPACE", "routing-namespace-sentinel");
  const restoreInherited = replaceEnvironmentValue(
    "INHERITED_DISPATCHER_SENTINEL",
    "inherited-sentinel",
  );
  t.after(() => {
    restoreInherited();
    restoreNamespace();
    restoreUrl();
  });

  const recording = createRecordingProcess();
  const dispatcher = createDispatcher(recording.spawn);
  const submissions = [
    {
      submission: { kind: "start", metadata },
      arguments_: [
        "session-start",
        "--session-id",
        metadata.sessionId,
        "--project-name",
        metadata.projectName,
        "--current-working-directory",
        metadata.currentWorkingDirectory,
        "--timestamp",
        metadata.timestamp,
      ],
    },
    {
      submission: { kind: "end", metadata },
      arguments_: [
        "session-end",
        "--session-id",
        metadata.sessionId,
        "--project-name",
        metadata.projectName,
        "--current-working-directory",
        metadata.currentWorkingDirectory,
        "--timestamp",
        metadata.timestamp,
      ],
    },
  ] as const satisfies readonly {
    readonly arguments_: readonly string[];
    readonly submission: Submission;
  }[];

  for (const [index, expected] of submissions.entries()) {
    const result = await dispatcher(config, expected.submission, new AbortController().signal);
    const invocation = invocationAt(recording, index);

    assert.deepEqual(result, { ok: true });
    assert.deepEqual(invocation.arguments_, expected.arguments_);
    assertInvocationOptions(invocation);
    assert.deepEqual(invocation.input.endCalls, [undefined]);
  }

  assert.equal(recording.invocations.length, 2);
});

test("spawns an observation with exact arguments and one closed JSON object", async (t) => {
  const restoreUrl = replaceEnvironmentValue("III_URL", "routing-url-sentinel");
  const restoreNamespace = replaceEnvironmentValue("III_NAMESPACE", "routing-namespace-sentinel");
  const restoreInherited = replaceEnvironmentValue(
    "INHERITED_DISPATCHER_SENTINEL",
    "inherited-sentinel",
  );
  t.after(() => {
    restoreInherited();
    restoreNamespace();
    restoreUrl();
  });

  const data: JsonObject = {
    result: { complete: true },
    toolName: "fixture-tool",
  };
  const recording = createRecordingProcess();
  const dispatcher = createDispatcher(recording.spawn);

  const result = await dispatcher(
    config,
    { kind: "observation", metadata, hookType: "pi.tool_execution_end", data },
    new AbortController().signal,
  );
  const invocation = invocationAt(recording, 0);

  assert.deepEqual(result, { ok: true });
  assert.deepEqual(invocation.arguments_, [
    "observation",
    "--hook-type",
    "pi.tool_execution_end",
    "--project-name",
    metadata.projectName,
    "--current-working-directory",
    metadata.currentWorkingDirectory,
    "--timestamp",
    metadata.timestamp,
    "--session-id",
    metadata.sessionId,
  ]);
  assertInvocationOptions(invocation);
  assert.deepEqual(invocation.input.endCalls, [
    JSON.stringify({
      source: "pi",
      version: "0.86.1",
      event: "tool_execution_end",
      payload: data,
    }),
  ]);
  assert.equal(recording.invocations.length, 1);
});

test("contains impossible internal hook values without spawning or exposing sentinels", async (t) => {
  const payloadSentinel = "payload-sentinel";
  const hookSentinel = "hook-sentinel";
  const environmentSentinel = "environment-sentinel";
  const restoreEnvironment = replaceEnvironmentValue("III_URL", environmentSentinel);
  t.after(restoreEnvironment);

  const errors: unknown[][] = [];
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    errors.push(arguments_);
  });

  const recording = createRecordingProcess();
  const dispatcher = createDispatcher(recording.spawn);
  const data: JsonObject = { payload: payloadSentinel };
  const results = [];

  for (const hookType of [`${hookSentinel}.not-pi`, `pi.${hookSentinel}-invalid`]) {
    results.push(
      await dispatcher(
        config,
        { kind: "observation", metadata, hookType, data },
        new AbortController().signal,
      ),
    );
  }

  assert.deepEqual(results, [
    { ok: false, category: "normalization" },
    { ok: false, category: "normalization" },
  ]);
  assert.deepEqual(recording.invocations, []);
  assert.deepEqual(errors, [
    [{ category: "normalization", sessionId: metadata.sessionId }],
    [{ category: "normalization", sessionId: metadata.sessionId }],
  ]);
  assert.doesNotMatch(JSON.stringify({ errors, results }), new RegExp(payloadSentinel));
  assert.doesNotMatch(JSON.stringify({ errors, results }), new RegExp(hookSentinel));
  assert.doesNotMatch(JSON.stringify({ errors, results }), new RegExp(environmentSentinel));
});

test("does not succeed or expose observation data when piped stdin is unavailable", async (t) => {
  const environmentSentinel = "environment-sentinel";
  const observationSentinel = "observation-sentinel";
  const pathSentinel = "/workspace/path-sentinel";
  const credentialSentinel = "credential-sentinel";
  const errors: unknown[][] = [];
  const restoreEnvironment = replaceEnvironmentValue("III_URL", environmentSentinel);
  t.after(restoreEnvironment);
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    errors.push(arguments_);
  });

  const unavailableInput: ProcessSpawner = () => ({ kind: "unavailable-stdio" });
  const dispatcher = createDispatcher(unavailableInput);

  const result = await dispatcher(
    { ...config, executable: `harness-events-${credentialSentinel}` },
    {
      kind: "observation",
      metadata: { ...metadata, currentWorkingDirectory: pathSentinel },
      hookType: "pi.tool_execution_end",
      data: { value: observationSentinel },
    },
    new AbortController().signal,
  );

  assert.deepEqual(result, { ok: false, category: "spawn" });
  assert.deepEqual(errors, [
    [
      {
        category: "spawn",
        nativeEvent: "tool_execution_end",
        sessionId: metadata.sessionId,
      },
    ],
  ]);
  assert.doesNotMatch(JSON.stringify({ errors, result }), new RegExp(observationSentinel));
  assert.doesNotMatch(JSON.stringify({ errors, result }), new RegExp(pathSentinel));
  assert.doesNotMatch(JSON.stringify({ errors, result }), new RegExp(environmentSentinel));
  assert.doesNotMatch(JSON.stringify({ errors, result }), new RegExp(credentialSentinel));
});

class ControlledInput implements ChildInput {
  readonly endCalls: (string | undefined)[] = [];
  readonly errorListeners: ChildErrorListener[] = [];
  readonly failOnEnd: boolean;

  constructor(failOnEnd = false) {
    this.failOnEnd = failOnEnd;
  }

  end(chunk?: string): void {
    if (this.failOnEnd) {
      throw new Error("stdin-listener-sentinel");
    }

    this.endCalls.push(chunk);
  }

  once(_event: "error", listener: ChildErrorListener): void {
    this.errorListeners.push(listener);
  }

  emitError(error: Error): void {
    const listeners = this.errorListeners.splice(0);

    for (const listener of listeners) {
      listener(error);
    }
  }
}

type OutputFailurePoint = "data-listener" | "error-listener" | "resume";

class ControlledOutput implements ChildOutput {
  readonly dataListeners: ((chunk: Uint8Array) => void)[] = [];
  readonly errorListeners: ChildErrorListener[] = [];
  readonly failurePoint: OutputFailurePoint | undefined;
  resumeCalls = 0;

  constructor(failurePoint?: OutputFailurePoint) {
    this.failurePoint = failurePoint;
  }

  on(_event: "data", listener: (chunk: Uint8Array) => void): void {
    if (this.failurePoint === "data-listener") {
      throw new Error("stdout-listener-sentinel");
    }

    this.dataListeners.push(listener);
  }

  once(_event: "error", listener: ChildErrorListener): void {
    if (this.failurePoint === "error-listener") {
      throw new Error("stderr-listener-sentinel");
    }

    this.errorListeners.push(listener);
  }

  resume(): void {
    if (this.failurePoint === "resume") {
      throw new Error("stream-resume-sentinel");
    }

    this.resumeCalls += 1;
  }

  emitData(chunk: Uint8Array): void {
    for (const listener of this.dataListeners) {
      listener(chunk);
    }
  }

  emitError(error: Error): void {
    const listeners = this.errorListeners.splice(0);

    for (const listener of listeners) {
      listener(error);
    }
  }
}

class ControlledChild implements SpawnedChild {
  readonly stdin: ControlledInput;
  readonly stdout: ControlledOutput;
  readonly stderr: ControlledOutput;
  readonly errorListeners: ChildErrorListener[] = [];
  readonly exitListeners: ChildExitListener[] = [];
  readonly killCalls: (NodeJS.Signals | number | undefined)[] = [];
  killHandler: ChildKillHandler | undefined;

  constructor(
    input: ControlledInput = new ControlledInput(),
    stdout: ControlledOutput = new ControlledOutput(),
    stderr: ControlledOutput = new ControlledOutput(),
  ) {
    this.stdin = input;
    this.stdout = stdout;
    this.stderr = stderr;
  }

  once(event: "error", listener: ChildErrorListener): this;
  once(event: "exit", listener: ChildExitListener): this;
  once(...[event, listener]: ["error", ChildErrorListener] | ["exit", ChildExitListener]): this {
    if (event === "error") {
      this.errorListeners.push(listener);
    } else {
      this.exitListeners.push(listener);
    }

    return this;
  }

  kill(signal?: NodeJS.Signals | number): boolean {
    this.killCalls.push(signal);
    this.killHandler?.(signal);
    return true;
  }

  emitError(error: Error): void {
    const listeners = this.errorListeners.splice(0);

    for (const listener of listeners) {
      listener(error);
    }
  }

  emitExit(code: number | null, signal: NodeJS.Signals | null): void {
    const listeners = this.exitListeners.splice(0);

    for (const listener of listeners) {
      listener(code, signal);
    }
  }
}

class ManualTimer implements DispatcherTimer {
  cancelled = false;

  cancel(): void {
    this.cancelled = true;
  }
}

interface ScheduledTimer {
  readonly callback: () => void;
  readonly delayMs: number;
  readonly timer: ManualTimer;
}

class ManualTimers implements DispatcherTimers {
  private currentTimeMs = 0;
  readonly scheduled: ScheduledTimer[] = [];

  now(): number {
    return this.currentTimeMs;
  }

  setTimeout(callback: () => void, delayMs: number): DispatcherTimer {
    const timer = new ManualTimer();
    this.scheduled.push({ callback, delayMs, timer });
    return timer;
  }

  clearTimeout(timer: DispatcherTimer): void {
    timer.cancel();
  }

  activeTimers(): readonly ScheduledTimer[] {
    return this.scheduled.filter((candidate) => !candidate.timer.cancelled);
  }

  fire(delayMs: number): void {
    const scheduled = this.scheduled.find(
      (candidate) => candidate.delayMs === delayMs && !candidate.timer.cancelled,
    );

    if (scheduled === undefined) {
      throw new Error(`Expected active ${delayMs}ms timer`);
    }

    scheduled.timer.cancel();
    this.currentTimeMs += scheduled.delayMs;
    scheduled.callback();
  }
}

class TrackingBytes extends Uint8Array {
  readonly copiedRanges: { readonly end: number; readonly start: number }[] = [];

  subarray(begin?: number, end?: number): Uint8Array<ArrayBuffer> {
    const start = begin ?? 0;
    const finish = end ?? this.byteLength;
    this.copiedRanges.push({ end: finish, start });
    return new Uint8Array(super.subarray(begin, end));
  }
}

interface ControlledProcess {
  readonly child: ControlledChild;
  readonly invocations: {
    readonly arguments_: readonly string[];
    readonly executable: string;
    readonly options: ProcessSpawnOptions;
  }[];
  readonly spawn: ProcessSpawner;
}

function createControlledProcess(
  child: ControlledChild = new ControlledChild(),
): ControlledProcess {
  const invocations: {
    readonly arguments_: readonly string[];
    readonly executable: string;
    readonly options: ProcessSpawnOptions;
  }[] = [];
  const spawn: ProcessSpawner = (executable, arguments_, options) => {
    invocations.push({ arguments_, executable, options });
    return { kind: "spawned", child };
  };

  return { child, invocations, spawn };
}

function observationSubmission(data: JsonObject = { complete: true }): Submission {
  return {
    data,
    hookType: "pi.tool_execution_end",
    kind: "observation",
    metadata,
  };
}

function diagnosticAt(calls: readonly unknown[][], index: number): object {
  const call = calls[index];

  if (call === undefined || call.length !== 1) {
    throw new Error(`Expected diagnostic call ${index}`);
  }

  const diagnostic = call[0];

  if (diagnostic === null || typeof diagnostic !== "object") {
    throw new Error(`Expected diagnostic object ${index}`);
  }

  return diagnostic;
}

test("classifies documented and unexpected exits without retrying", async (t) => {
  const diagnostics: unknown[][] = [];
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const cases: readonly {
    readonly category?: "connection" | "invocation" | "unknown-exit" | "usage";
    readonly code: number;
  }[] = [
    { code: 0 },
    { category: "usage", code: 2 },
    { category: "connection", code: 3 },
    { category: "invocation", code: 4 },
    { category: "unknown-exit", code: 17 },
  ];

  for (const outcome of cases) {
    const process = createControlledProcess();
    const dispatcher = createDispatcher(process.spawn, new ManualTimers());
    const resultPromise = dispatcher(config, observationSubmission(), new AbortController().signal);

    process.child.emitExit(outcome.code, null);

    const expected =
      outcome.category === undefined ? { ok: true } : { ok: false, category: outcome.category };
    assert.deepEqual(await resultPromise, expected);
    assert.equal(process.invocations.length, 1);
  }

  assert.deepEqual(diagnostics, [
    [{ category: "usage", nativeEvent: "tool_execution_end", sessionId: metadata.sessionId }],
    [{ category: "connection", nativeEvent: "tool_execution_end", sessionId: metadata.sessionId }],
    [{ category: "invocation", nativeEvent: "tool_execution_end", sessionId: metadata.sessionId }],
    [
      {
        category: "unknown-exit",
        nativeEvent: "tool_execution_end",
        sessionId: metadata.sessionId,
      },
    ],
  ]);
});

test("classifies process signals without retrying", async (t) => {
  const diagnostics: unknown[][] = [];
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const process = createControlledProcess();
  const dispatcher = createDispatcher(process.spawn, new ManualTimers());
  const resultPromise = dispatcher(config, observationSubmission(), new AbortController().signal);

  process.child.emitExit(null, "SIGTERM");

  assert.deepEqual(await resultPromise, { ok: false, category: "signal" });
  assert.equal(process.invocations.length, 1);
  assert.deepEqual(diagnostics, [
    [{ category: "signal", nativeEvent: "tool_execution_end", sessionId: metadata.sessionId }],
  ]);
});

test("contains synchronous and asynchronous spawn failures without retrying", async (t) => {
  const diagnostics: unknown[][] = [];
  const errorSentinel = "async-spawn-error-sentinel";
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const unavailable: ProcessSpawner = () => ({ kind: "unavailable-stdio" });
  assert.deepEqual(
    await createDispatcher(unavailable, new ManualTimers())(
      config,
      observationSubmission(),
      new AbortController().signal,
    ),
    { ok: false, category: "spawn" },
  );

  let synchronousCalls = 0;
  const synchronousFailure: ProcessSpawner = () => {
    synchronousCalls += 1;
    throw new Error(errorSentinel);
  };
  assert.deepEqual(
    await createDispatcher(synchronousFailure, new ManualTimers())(
      config,
      observationSubmission(),
      new AbortController().signal,
    ),
    { ok: false, category: "spawn" },
  );
  assert.equal(synchronousCalls, 1);

  const process = createControlledProcess();
  const resultPromise = createDispatcher(process.spawn, new ManualTimers())(
    config,
    observationSubmission(),
    new AbortController().signal,
  );
  process.child.emitError(new Error(errorSentinel));
  process.child.emitExit(17, null);

  assert.deepEqual(await resultPromise, { ok: false, category: "spawn" });
  assert.equal(process.invocations.length, 1);
  assert.deepEqual(diagnostics, [
    [{ category: "spawn", nativeEvent: "tool_execution_end", sessionId: metadata.sessionId }],
    [{ category: "spawn", nativeEvent: "tool_execution_end", sessionId: metadata.sessionId }],
    [{ category: "spawn", nativeEvent: "tool_execution_end", sessionId: metadata.sessionId }],
  ]);
  assert.doesNotMatch(JSON.stringify(diagnostics), new RegExp(errorSentinel));
});

test("bounds timeout and abort termination after KILL without retries or diagnostic leaks", async (t) => {
  const diagnostics: unknown[][] = [];
  const credentialSentinel = "credential-sentinel";
  const environmentSentinel = "environment-sentinel";
  const pathSentinel = "/workspace/path-sentinel";
  const payloadSentinel = "payload-sentinel";
  const stdinSentinel = "stdin-sentinel";
  const streamSentinel = "stream-sentinel";
  const restoreEnvironment = replaceEnvironmentValue("DISPATCHER_SECRET", environmentSentinel);
  t.after(restoreEnvironment);
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const cases: readonly {
    readonly category: "signal" | "timeout";
    readonly trigger: "abort" | "timeout";
  }[] = [
    { category: "timeout", trigger: "timeout" },
    { category: "signal", trigger: "abort" },
  ];

  for (const outcome of cases) {
    const controller = new AbortController();
    const timers = new ManualTimers();
    const process = createControlledProcess();
    const deadlineConfig: ExtensionConfig = { ...config, executionTimeoutMs: 17 };
    const resultPromise = createDispatcher(process.spawn, timers)(
      { ...deadlineConfig, executable: `harness-events-${credentialSentinel}` },
      {
        data: { payload: payloadSentinel, stdin: stdinSentinel },
        hookType: "pi.tool_execution_end",
        kind: "observation",
        metadata: { ...metadata, currentWorkingDirectory: pathSentinel },
      },
      controller.signal,
    );
    process.child.stdout.emitData(new TextEncoder().encode(streamSentinel));
    process.child.stderr.emitData(new TextEncoder().encode(streamSentinel));

    if (outcome.trigger === "timeout") {
      timers.fire(deadlineConfig.executionTimeoutMs);
    } else {
      controller.abort();
    }

    assert.deepEqual(process.child.killCalls, ["SIGTERM"]);
    timers.fire(500);
    assert.deepEqual(process.child.killCalls, ["SIGTERM", "SIGKILL"]);
    assert.equal(
      await Promise.race([resultPromise.then(() => "settled"), Promise.resolve("pending")]),
      "pending",
    );
    assert.deepEqual(
      timers.activeTimers().map((scheduled) => scheduled.delayMs),
      [500],
    );

    timers.fire(500);
    assert.deepEqual(await resultPromise, { ok: false, category: outcome.category });
    assert.equal(process.invocations.length, 1);
    assert.deepEqual(timers.activeTimers(), []);

    process.child.emitExit(null, "SIGKILL");
    assert.equal(process.invocations.length, 1);
  }

  assert.deepEqual(diagnostics, [
    [{ category: "timeout", nativeEvent: "tool_execution_end", sessionId: metadata.sessionId }],
    [{ category: "signal", nativeEvent: "tool_execution_end", sessionId: metadata.sessionId }],
  ]);
  const output = JSON.stringify(diagnostics);
  for (const sentinel of [
    credentialSentinel,
    environmentSentinel,
    pathSentinel,
    payloadSentinel,
    stdinSentinel,
    streamSentinel,
  ]) {
    assert.doesNotMatch(output, new RegExp(sentinel));
  }
});

test("does not arm termination timers after a synchronous TERM exit", async (t) => {
  const diagnostics: unknown[][] = [];
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const child = new ControlledChild();
  child.killHandler = (childSignal) => {
    if (childSignal === "SIGTERM") {
      child.emitExit(0, null);
    }
  };
  const timers = new ManualTimers();
  const process = createControlledProcess(child);
  const resultPromise = createDispatcher(process.spawn, timers)(
    { ...config, executionTimeoutMs: 17 },
    observationSubmission(),
    new AbortController().signal,
  );

  timers.fire(17);

  assert.deepEqual(await resultPromise, { ok: false, category: "timeout" });
  assert.deepEqual(child.killCalls, ["SIGTERM"]);
  assert.equal(timers.scheduled.length, 1);
  assert.deepEqual(timers.activeTimers(), []);
  assert.equal(process.invocations.length, 1);
  assert.deepEqual(diagnostics, [
    [{ category: "timeout", nativeEvent: "tool_execution_end", sessionId: metadata.sessionId }],
  ]);
});

test("settles once when a terminating child exits before KILL escalation", async (t) => {
  const diagnostics: unknown[][] = [];
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const timers = new ManualTimers();
  const process = createControlledProcess();
  const resultPromise = createDispatcher(process.spawn, timers)(
    { ...config, executionTimeoutMs: 17 },
    observationSubmission(),
    new AbortController().signal,
  );

  timers.fire(17);
  assert.deepEqual(
    timers.activeTimers().map((scheduled) => scheduled.delayMs),
    [500],
  );
  process.child.emitExit(0, null);

  assert.deepEqual(await resultPromise, { ok: false, category: "timeout" });
  assert.deepEqual(process.child.killCalls, ["SIGTERM"]);
  assert.deepEqual(timers.activeTimers(), []);
  process.child.emitExit(null, "SIGKILL");
  assert.deepEqual(diagnostics, [
    [{ category: "timeout", nativeEvent: "tool_execution_end", sessionId: metadata.sessionId }],
  ]);
});

test("settles once when a terminating child exits during KILL escalation", async (t) => {
  const diagnostics: unknown[][] = [];
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const child = new ControlledChild();
  child.killHandler = (childSignal) => {
    if (childSignal === "SIGKILL") {
      child.emitExit(0, null);
    }
  };
  const controller = new AbortController();
  const timers = new ManualTimers();
  const process = createControlledProcess(child);
  const resultPromise = createDispatcher(process.spawn, timers)(
    config,
    observationSubmission(),
    controller.signal,
  );

  controller.abort();
  timers.fire(500);

  assert.deepEqual(await resultPromise, { ok: false, category: "signal" });
  assert.deepEqual(child.killCalls, ["SIGTERM", "SIGKILL"]);
  assert.deepEqual(timers.activeTimers(), []);
  process.child.emitExit(null, "SIGKILL");
  assert.deepEqual(diagnostics, [
    [{ category: "signal", nativeEvent: "tool_execution_end", sessionId: metadata.sessionId }],
  ]);
});

test("settles once when a terminating child exits during exit confirmation", async (t) => {
  const diagnostics: unknown[][] = [];
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const timers = new ManualTimers();
  const process = createControlledProcess();
  const resultPromise = createDispatcher(process.spawn, timers)(
    { ...config, executionTimeoutMs: 17 },
    observationSubmission(),
    new AbortController().signal,
  );

  timers.fire(17);
  timers.fire(500);
  assert.deepEqual(
    timers.activeTimers().map((scheduled) => scheduled.delayMs),
    [500],
  );
  process.child.emitExit(0, null);

  assert.deepEqual(await resultPromise, { ok: false, category: "timeout" });
  assert.deepEqual(process.child.killCalls, ["SIGTERM", "SIGKILL"]);
  assert.deepEqual(timers.activeTimers(), []);
  process.child.emitExit(null, "SIGKILL");
  assert.deepEqual(diagnostics, [
    [{ category: "timeout", nativeEvent: "tool_execution_end", sessionId: metadata.sessionId }],
  ]);
});

test("drains output while capping stderr capture and omitting child output from diagnostics", async (t) => {
  const diagnostics: unknown[][] = [];
  const stdoutSentinel = "stdout-sentinel";
  const stderrSentinel = "stderr-sentinel";
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const process = createControlledProcess();
  const resultPromise = createDispatcher(process.spawn, new ManualTimers())(
    config,
    observationSubmission(),
    new AbortController().signal,
  );
  const first = new TrackingBytes(MAXIMUM_STDERR_CAPTURE_BYTES - 1);
  const second = new TrackingBytes(2);
  const discarded = new TrackingBytes(1);
  first.set(new TextEncoder().encode(stderrSentinel));

  process.child.stdout.emitData(new TextEncoder().encode(stdoutSentinel));
  process.child.stderr.emitData(first);
  process.child.stderr.emitData(second);
  process.child.stderr.emitData(discarded);
  process.child.emitExit(17, null);

  assert.equal(MAXIMUM_STDERR_CAPTURE_BYTES, 8 * 1024);
  assert.equal(process.child.stdout.resumeCalls, 1);
  assert.equal(process.child.stderr.resumeCalls, 1);
  assert.deepEqual(first.copiedRanges, [{ end: MAXIMUM_STDERR_CAPTURE_BYTES - 1, start: 0 }]);
  assert.deepEqual(second.copiedRanges, [{ end: 1, start: 0 }]);
  assert.deepEqual(discarded.copiedRanges, []);
  assert.deepEqual(await resultPromise, { ok: false, category: "unknown-exit" });
  assert.doesNotMatch(JSON.stringify(diagnostics), new RegExp(stdoutSentinel));
  assert.doesNotMatch(JSON.stringify(diagnostics), new RegExp(stderrSentinel));
});

test("contains serialization, stream, and listener faults", async (t) => {
  const diagnostics: unknown[][] = [];
  const sentinel = "fault-sentinel";
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const unserializable: JsonObject = {};
  Object.defineProperty(unserializable, "toJSON", {
    enumerable: false,
    value: (): never => {
      throw new Error(sentinel);
    },
  });
  assert.deepEqual(
    await createDispatcher(createControlledProcess().spawn, new ManualTimers())(
      config,
      observationSubmission(unserializable),
      new AbortController().signal,
    ),
    { ok: false, category: "serialization" },
  );

  const inputFailure = createControlledProcess(new ControlledChild(new ControlledInput(true)));
  const inputResult = createDispatcher(inputFailure.spawn, new ManualTimers())(
    config,
    observationSubmission(),
    new AbortController().signal,
  );
  assert.deepEqual(inputFailure.child.killCalls, ["SIGTERM"]);
  inputFailure.child.emitExit(null, "SIGTERM");
  assert.deepEqual(await inputResult, { ok: false, category: "spawn" });
  assert.equal(inputFailure.invocations.length, 1);

  const outputFailure = createControlledProcess(
    new ControlledChild(undefined, new ControlledOutput("data-listener")),
  );
  const outputResult = createDispatcher(outputFailure.spawn, new ManualTimers())(
    config,
    observationSubmission(),
    new AbortController().signal,
  );
  assert.deepEqual(outputFailure.child.killCalls, ["SIGTERM"]);
  outputFailure.child.emitExit(null, "SIGTERM");
  assert.deepEqual(await outputResult, { ok: false, category: "spawn" });
  assert.equal(outputFailure.invocations.length, 1);

  const streamFailure = createControlledProcess();
  const streamResult = createDispatcher(streamFailure.spawn, new ManualTimers())(
    config,
    observationSubmission(),
    new AbortController().signal,
  );
  streamFailure.child.stderr.emitError(new Error(sentinel));
  assert.deepEqual(streamFailure.child.killCalls, ["SIGTERM"]);
  streamFailure.child.emitExit(null, "SIGTERM");
  assert.deepEqual(await streamResult, { ok: false, category: "spawn" });
  assert.equal(streamFailure.invocations.length, 1);
  assert.deepEqual(diagnostics, [
    [
      {
        category: "serialization",
        nativeEvent: "tool_execution_end",
        sessionId: metadata.sessionId,
      },
    ],
    [{ category: "spawn", nativeEvent: "tool_execution_end", sessionId: metadata.sessionId }],
    [{ category: "spawn", nativeEvent: "tool_execution_end", sessionId: metadata.sessionId }],
    [{ category: "spawn", nativeEvent: "tool_execution_end", sessionId: metadata.sessionId }],
  ]);
  assert.doesNotMatch(JSON.stringify(diagnostics), new RegExp(sentinel));
});

test("emits fresh frozen identity-only diagnostics without consumer mutation", async (t) => {
  const diagnostics: unknown[][] = [];
  const payloadSentinel = "payload-sentinel";
  const environmentSentinel = "environment-sentinel";
  const pathSentinel = "/private/path-sentinel";
  const stdinSentinel = "stdin-sentinel";
  const outputSentinel = "output-sentinel";
  const credentialSentinel = "credential-sentinel";
  const restoreEnvironment = replaceEnvironmentValue("DISPATCHER_SECRET", environmentSentinel);
  t.after(restoreEnvironment);
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const data: JsonObject = { payload: payloadSentinel, stdin: stdinSentinel };
  const first = createControlledProcess();
  const firstResult = createDispatcher(first.spawn, new ManualTimers())(
    { ...config, executable: `harness-events-${credentialSentinel}` },
    {
      data,
      hookType: "pi.tool_execution_end",
      kind: "observation",
      metadata: { ...metadata, currentWorkingDirectory: pathSentinel },
    },
    new AbortController().signal,
  );
  first.child.stdout.emitData(new TextEncoder().encode(outputSentinel));
  first.child.stderr.emitData(new TextEncoder().encode(outputSentinel));
  first.child.emitExit(17, null);
  assert.deepEqual(await firstResult, { ok: false, category: "unknown-exit" });

  const firstDiagnostic = diagnosticAt(diagnostics, 0);
  assert.equal(Reflect.set(firstDiagnostic, "sentinel", payloadSentinel), false);

  const second = createControlledProcess();
  const secondResult = createDispatcher(second.spawn, new ManualTimers())(
    config,
    observationSubmission(),
    new AbortController().signal,
  );
  second.child.emitExit(2, null);
  assert.deepEqual(await secondResult, { ok: false, category: "usage" });

  const secondDiagnostic = diagnosticAt(diagnostics, 1);
  assert.equal(diagnostics.length, 2);
  assert.notStrictEqual(firstDiagnostic, secondDiagnostic);
  assert.equal(Object.isFrozen(firstDiagnostic), true);
  assert.equal(Object.isFrozen(secondDiagnostic), true);
  assert.deepEqual(Reflect.ownKeys(secondDiagnostic), ["category", "nativeEvent", "sessionId"]);
  assert.deepEqual(secondDiagnostic, {
    category: "usage",
    nativeEvent: "tool_execution_end",
    sessionId: metadata.sessionId,
  });
  assert.equal(Reflect.has(secondDiagnostic, "sentinel"), false);

  const output = JSON.stringify({ diagnostics, firstResult, secondResult });
  for (const sentinel of [
    payloadSentinel,
    environmentSentinel,
    pathSentinel,
    stdinSentinel,
    outputSentinel,
    credentialSentinel,
  ]) {
    assert.doesNotMatch(output, new RegExp(sentinel));
  }
});

test("bounds diagnostic identity fields before logging", async (t) => {
  const diagnostics: unknown[][] = [];
  const sentinel = "oversized-session-sentinel";
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const process = createControlledProcess();
  const resultPromise = createDispatcher(process.spawn, new ManualTimers())(
    config,
    {
      data: { complete: true },
      hookType: "pi.tool_execution_end",
      kind: "observation",
      metadata: { ...metadata, sessionId: `${"s".repeat(129)}-${sentinel}` },
    },
    new AbortController().signal,
  );
  process.child.emitExit(17, null);

  assert.deepEqual(await resultPromise, { ok: false, category: "unknown-exit" });
  assert.deepEqual(diagnostics, [
    [{ category: "unknown-exit", nativeEvent: "tool_execution_end" }],
  ]);
  assert.ok(JSON.stringify(diagnostics).length <= 512);
  assert.doesNotMatch(JSON.stringify(diagnostics), new RegExp(sentinel));
});
