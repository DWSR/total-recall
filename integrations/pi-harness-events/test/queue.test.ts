import assert from "node:assert/strict";
import process from "node:process";
import test from "node:test";

import type { ExtensionConfig } from "../src/config.ts";
import {
  createDispatcher,
  TERMINATION_CONFIRMATION_DELAY_MS,
  TERMINATION_ESCALATION_DELAY_MS,
  type ChildInput,
  type ChildOutput,
  type DispatcherTimers,
  type ProcessSpawner,
  type SpawnedChild,
} from "../src/dispatcher.ts";
import {
  createSubmissionQueue,
  type Dispatch,
  type QueueTimer,
  type QueueTimers,
} from "../src/queue.ts";
import type { DispatchResult, EventMetadata, JsonObject, Submission } from "../src/model.ts";

const metadata: EventMetadata = {
  currentWorkingDirectory: "/workspace/project",
  projectName: "project",
  sessionId: "session-123",
  timestamp: "2026-09-21T12:34:56.789Z",
};

type ObservationSubmission = Extract<Submission, { readonly kind: "observation" }>;

interface PendingDispatch {
  readonly signal: AbortSignal;
  readonly submission: Submission;
  complete(result: DispatchResult): void;
}

interface RecordingDispatch {
  readonly dispatch: Dispatch;
  readonly invocations: readonly PendingDispatch[];
  next(): Promise<PendingDispatch>;
}

function config(observationCapacity: number, shutdownTimeoutMs = 5_000): ExtensionConfig {
  return {
    executable: "harness-events",
    executionTimeoutMs: 35_000,
    observationCapacity,
    shutdownTimeoutMs,
  };
}

function observation(
  hookType = "pi.turn_end",
  observationMetadata: EventMetadata = metadata,
  data: JsonObject = { accepted: true },
): ObservationSubmission {
  return { data, hookType, kind: "observation", metadata: observationMetadata };
}

function createRecordingDispatch(): RecordingDispatch {
  const invocations: PendingDispatch[] = [];
  const pending: PendingDispatch[] = [];
  const waiters: ((invocation: PendingDispatch) => void)[] = [];

  const dispatch: Dispatch = (submission, signal) =>
    new Promise((resolve) => {
      const invocation: PendingDispatch = {
        complete: (result) => resolve(result),
        signal,
        submission,
      };
      invocations.push(invocation);

      const waiter = waiters.shift();

      if (waiter === undefined) {
        pending.push(invocation);
      } else {
        waiter(invocation);
      }
    });

  return {
    dispatch,
    invocations,
    next: () => {
      const invocation = pending.shift();
      return invocation === undefined
        ? new Promise((resolve) => waiters.push(resolve))
        : Promise.resolve(invocation);
    },
  };
}

class ManualTimer implements QueueTimer {
  cancelled = false;

  cancel(): void {
    this.cancelled = true;
  }
}

interface ScheduledTimer {
  readonly callback: () => void;
  readonly delayMs: number;
  readonly dueAtMs: number;
  readonly order: number;
  readonly timer: ManualTimer;
}

class ManualTimers implements QueueTimers, DispatcherTimers {
  private currentTimeMs = 0;
  readonly scheduled: ScheduledTimer[] = [];

  setTimeout(callback: () => void, delayMs: number): QueueTimer {
    const timer = new ManualTimer();
    this.scheduled.push({
      callback,
      delayMs,
      dueAtMs: this.currentTimeMs + delayMs,
      order: this.scheduled.length,
      timer,
    });
    return timer;
  }

  clearTimeout(timer: QueueTimer): void {
    timer.cancel();
  }

  activeTimers(): readonly ScheduledTimer[] {
    return this.scheduled
      .filter((candidate) => !candidate.timer.cancelled)
      .sort((left, right) => left.dueAtMs - right.dueAtMs || left.order - right.order);
  }

  advanceBy(delayMs: number): void {
    this.advanceTo(this.currentTimeMs + delayMs);
  }

  advanceTo(timeMs: number): void {
    if (timeMs < this.currentTimeMs) {
      throw new Error("Cannot move the shared clock backward");
    }

    while (true) {
      const next = this.activeTimers()[0];

      if (next === undefined || next.dueAtMs > timeMs) {
        break;
      }

      this.run(next);
    }

    this.currentTimeMs = timeMs;
  }

  fire(delayMs: number): void {
    const scheduled = this.activeTimers().find(
      (candidate) => candidate.delayMs === delayMs && !candidate.timer.cancelled,
    );

    if (scheduled === undefined) {
      throw new Error(`Expected active ${delayMs}ms timer`);
    }

    this.run(scheduled);
  }

  now(): number {
    return this.currentTimeMs;
  }

  runNext(): void {
    const scheduled = this.activeTimers()[0];

    if (scheduled === undefined) {
      throw new Error("Expected an active timer");
    }

    this.run(scheduled);
  }

  runNextAt(timeMs: number): void {
    if (timeMs < this.currentTimeMs) {
      throw new Error("Cannot move the shared clock backward");
    }

    const scheduled = this.activeTimers()[0];

    if (scheduled === undefined || scheduled.dueAtMs > timeMs) {
      throw new Error(`Expected an active timer due by ${timeMs}ms`);
    }

    this.run(scheduled, timeMs);
  }

  private run(
    scheduled: ScheduledTimer,
    timeMs = Math.max(this.currentTimeMs, scheduled.dueAtMs),
  ): void {
    this.currentTimeMs = timeMs;
    scheduled.timer.cancel();
    scheduled.callback();
  }
}

class ThrowingNowTimers implements QueueTimers {
  readonly timers = new ManualTimers();
  private nowReads = 0;
  private readonly throwingRead: number;

  constructor(throwingRead: number) {
    this.throwingRead = throwingRead;
  }

  clearTimeout(timer: QueueTimer): void {
    this.timers.clearTimeout(timer);
  }

  now(): number {
    this.nowReads += 1;

    if (this.nowReads === this.throwingRead) {
      throw new Error("Injected clock failure");
    }

    return this.timers.now();
  }

  setTimeout(callback: () => void, delayMs: number): QueueTimer {
    return this.timers.setTimeout(callback, delayMs);
  }
}

type ChildErrorListener = (error: Error) => void;
type ChildExitListener = (code: number | null, signal: NodeJS.Signals | null) => void;

class QueueInput implements ChildInput {
  end(_chunk?: string): void {}

  once(_event: "error", _listener: ChildErrorListener): void {}
}

class QueueOutput implements ChildOutput {
  on(_event: "data", _listener: (chunk: Uint8Array) => void): void {}

  once(_event: "error", _listener: ChildErrorListener): void {}

  resume(): void {}
}

class QueueChild implements SpawnedChild {
  readonly stderr = new QueueOutput();
  readonly stdin = new QueueInput();
  readonly stdout = new QueueOutput();
  readonly exitListeners: ChildExitListener[] = [];
  readonly killCalls: (NodeJS.Signals | number | undefined)[] = [];

  once(event: "error", listener: ChildErrorListener): this;
  once(event: "exit", listener: ChildExitListener): this;
  once(...[event, listener]: ["error", ChildErrorListener] | ["exit", ChildExitListener]): this {
    if (event === "exit") {
      this.exitListeners.push(listener);
    }

    return this;
  }

  kill(signal?: NodeJS.Signals | number): boolean {
    this.killCalls.push(signal);
    return true;
  }

  emitExit(code: number | null, signal: NodeJS.Signals | null): void {
    const listeners = this.exitListeners.splice(0);

    for (const listener of listeners) {
      listener(code, signal);
    }
  }
}

function createShutdownHarness(): {
  readonly child: QueueChild;
  readonly queue: ReturnType<typeof createSubmissionQueue>;
  readonly spawnCalls: () => number;
  readonly timers: ManualTimers;
} {
  const timers = new ManualTimers();
  const child = new QueueChild();
  let spawnCalls = 0;
  const spawn: ProcessSpawner = () => {
    spawnCalls += 1;
    return { kind: "spawned", child };
  };
  const deadlineConfig = config(2, 1_500);
  const dispatcher = createDispatcher(spawn, timers);

  return {
    child,
    queue: createSubmissionQueue(
      deadlineConfig,
      (submission, signal) => dispatcher(deadlineConfig, submission, signal),
      timers,
    ),
    spawnCalls: () => spawnCalls,
    timers,
  };
}

async function flushMicrotasks(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

function diagnosticAt(calls: readonly unknown[][], index: number): object {
  const call = calls[index];

  if (call === undefined || call.length !== 1) {
    throw new Error(`Expected diagnostic ${index}`);
  }

  const diagnostic = call[0];

  if (diagnostic === null || typeof diagnostic !== "object") {
    throw new Error(`Expected diagnostic record ${index}`);
  }

  return diagnostic;
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

test("returns from ordinary acceptance before injected dispatch completes", async () => {
  const recording = createRecordingDispatch();
  const queue = createSubmissionQueue(config(1), recording.dispatch);

  assert.equal(queue.start(metadata), undefined);
  assert.equal(recording.invocations.length, 1);
  const start = await recording.next();
  assert.deepEqual(start.submission, { kind: "start", metadata });

  assert.equal(queue.observe(observation()), undefined);
  assert.deepEqual(
    recording.invocations.map(({ submission }) => submission.kind),
    ["start"],
  );

  const closed = queue.close(metadata);
  let closedSettled = false;
  void closed.then(() => {
    closedSettled = true;
  });
  assert.equal(closedSettled, false);

  start.complete({ ok: true });
  const acceptedObservation = await recording.next();
  assert.equal(acceptedObservation.submission.kind, "observation");
  assert.equal(closedSettled, false);

  acceptedObservation.complete({ ok: true });
  const end = await recording.next();
  assert.deepEqual(end.submission, { kind: "end", metadata });
  assert.equal(closedSettled, false);

  end.complete({ ok: true });
  await closed;
  assert.equal(closedSettled, true);
});

test("drains busy work and its queued session end before the termination reserve", async () => {
  const timers = new ManualTimers();
  const recording = createRecordingDispatch();
  const queue = createSubmissionQueue(config(1, 1_500), recording.dispatch, timers);
  queue.start(metadata);
  queue.observe(observation());
  const closed = queue.close(metadata);

  assert.deepEqual(
    timers.activeTimers().map(({ delayMs }) => delayMs),
    [500, 1_500],
  );

  const start = await recording.next();
  assert.equal(start.signal.aborted, false);
  start.complete({ ok: true });

  const acceptedObservation = await recording.next();
  assert.equal(acceptedObservation.submission.kind, "observation");
  acceptedObservation.complete({ ok: true });

  const end = await recording.next();
  assert.deepEqual(end.submission, { kind: "end", metadata });
  end.complete({ ok: true });
  await closed;

  assert.deepEqual(
    recording.invocations.map(({ submission }) => submission.kind),
    ["start", "observation", "end"],
  );
  assert.deepEqual(timers.activeTimers(), []);
});

test("drains an idle active runtime through its session end", async () => {
  const timers = new ManualTimers();
  const recording = createRecordingDispatch();
  const queue = createSubmissionQueue(config(1, 1_500), recording.dispatch, timers);
  queue.start(metadata);
  const start = await recording.next();
  start.complete({ ok: true });
  await Promise.resolve();

  const closed = queue.close(metadata);
  const end = await recording.next();
  assert.deepEqual(end.submission, { kind: "end", metadata });
  end.complete({ ok: true });
  await closed;

  assert.deepEqual(
    recording.invocations.map(({ submission }) => submission.kind),
    ["start", "end"],
  );
  assert.deepEqual(timers.activeTimers(), []);
});

test("closes an empty runtime once and rejects observations after close begins", async () => {
  const timers = new ManualTimers();
  const recording = createRecordingDispatch();
  const queue = createSubmissionQueue(config(1, 1_500), recording.dispatch, timers);
  const closed = queue.close(metadata);

  assert.strictEqual(queue.close({ ...metadata, timestamp: "2026-09-21T12:35:00.000Z" }), closed);
  queue.observe(observation());
  assert.deepEqual(
    timers.activeTimers().map(({ delayMs }) => delayMs),
    [500, 1_500],
  );

  const end = await recording.next();
  assert.deepEqual(end.submission, { kind: "end", metadata });
  end.complete({ ok: true });
  await closed;

  assert.deepEqual(
    recording.invocations.map(({ submission }) => submission.kind),
    ["end"],
  );
});

test("uses the final-deadline fallback when an injected dispatch ignores abortion", async () => {
  const timers = new ManualTimers();
  const recording = createRecordingDispatch();
  const queue = createSubmissionQueue(config(2, 1_500), recording.dispatch, timers);
  queue.start(metadata);
  const activeStart = await recording.next();
  queue.observe(observation("pi.turn_start"));
  queue.observe(observation("pi.turn_end"));
  const closed = queue.close(metadata);
  let closeSettled = false;
  void closed.then(() => {
    closeSettled = true;
  });
  queue.observe(observation("pi.agent_start"));

  timers.fire(TERMINATION_ESCALATION_DELAY_MS);

  assert.equal(activeStart.signal.aborted, true);
  assert.deepEqual(
    recording.invocations.map(({ submission }) => submission.kind),
    ["start"],
  );

  timers.runNext();
  await flushMicrotasks();
  assert.equal(closeSettled, true);
  assert.equal(timers.now(), 1_500);
  assert.deepEqual(timers.activeTimers(), []);
  await closed;

  activeStart.complete({ ok: true });
  await flushMicrotasks();
  assert.deepEqual(
    recording.invocations.map(({ submission }) => submission.kind),
    ["start"],
  );
});

test("does not expose shutdown ownership to injected dispatch and settles at the deadline", async () => {
  const timers = new ManualTimers();
  const dispatched: Submission[] = [];
  let abortObserved = false;
  let forbiddenThirdArgument: unknown;
  const injectedDispatch = (
    submission: Submission,
    signal: AbortSignal,
    ...additionalArguments: unknown[]
  ): Promise<DispatchResult> => {
    forbiddenThirdArgument = additionalArguments[0];

    if (typeof forbiddenThirdArgument === "object" && forbiddenThirdArgument !== null) {
      Object.defineProperty(forbiddenThirdArgument, "shutdown", {
        get(): never {
          throw new Error("shutdown-getter-sentinel");
        },
      });
    }

    return new Promise(() => {
      dispatched.push(submission);
      signal.addEventListener(
        "abort",
        () => {
          abortObserved = true;
        },
        { once: true },
      );
    });
  };
  const queue = createSubmissionQueue(config(2, 1_500), injectedDispatch, timers);
  queue.start(metadata);
  queue.observe(observation("pi.turn_start"));
  const closed = queue.close(metadata);
  let closeSettled = false;
  void closed.then(() => {
    closeSettled = true;
  });

  timers.fire(TERMINATION_ESCALATION_DELAY_MS);
  assert.equal(abortObserved, true);

  assert.doesNotThrow(() => timers.runNext());
  await flushMicrotasks();
  assert.equal(closeSettled, true);
  assert.equal(forbiddenThirdArgument, undefined);
  assert.equal(timers.now(), 1_500);
  assert.deepEqual(
    dispatched.map(({ kind }) => kind),
    ["start"],
  );
  assert.deepEqual(timers.activeTimers(), []);
  await closed;
});

test("uses queue fallback when a wrapper returns a distinct never-settling promise", async (t) => {
  const diagnostics: unknown[][] = [];
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const timers = new ManualTimers();
  const child = new QueueChild();
  const deadlineConfig = config(2, 1_500);
  const dispatcher = createDispatcher(() => ({ kind: "spawned", child }), timers);
  let dispatcherPromise: Promise<DispatchResult> | undefined;
  let wrapperPromise: Promise<DispatchResult> | undefined;
  const wrapper = (
    submission: Submission,
    signal: AbortSignal,
    ...additionalArguments: unknown[]
  ): Promise<DispatchResult> => {
    const invokeDispatcher = dispatcher as unknown as (
      config: ExtensionConfig,
      submission: Submission,
      signal: AbortSignal,
      context: unknown,
    ) => Promise<DispatchResult>;
    dispatcherPromise = invokeDispatcher(
      deadlineConfig,
      submission,
      signal,
      additionalArguments[0],
    );
    wrapperPromise = new Promise<DispatchResult>(() => undefined);
    return wrapperPromise;
  };
  const queue = createSubmissionQueue(deadlineConfig, wrapper, timers);
  queue.start(metadata);
  queue.observe(observation());
  const closed = queue.close(metadata);
  let closeSettled = false;
  void closed.then(() => {
    closeSettled = true;
  });

  timers.advanceBy(TERMINATION_ESCALATION_DELAY_MS);
  assert.deepEqual(child.killCalls, ["SIGTERM"]);
  timers.advanceBy(TERMINATION_ESCALATION_DELAY_MS);
  assert.deepEqual(child.killCalls, ["SIGTERM", "SIGKILL"]);
  assert.ok(dispatcherPromise);
  assert.ok(wrapperPromise);
  assert.notStrictEqual(dispatcherPromise, wrapperPromise);

  timers.runNext();
  await flushMicrotasks();
  assert.equal(closeSettled, true);
  await closed;
  assert.deepEqual(child.killCalls, ["SIGTERM", "SIGKILL"]);

  timers.runNext();
  assert.deepEqual(await dispatcherPromise, { category: "signal", ok: false });
  assert.deepEqual(diagnostics, [[{ category: "signal", sessionId: metadata.sessionId }]]);
  assert.deepEqual(timers.activeTimers(), []);
});

for (const throwingRead of [1, 2, 3]) {
  test(`fails open when the shutdown clock throws on read ${throwingRead}`, async () => {
    const timers = new ThrowingNowTimers(throwingRead);
    const recording = createRecordingDispatch();
    const queue = createSubmissionQueue(config(2, 1_500), recording.dispatch, timers);
    queue.start(metadata);
    const activeStart = await recording.next();
    let closed: Promise<void> | undefined;

    assert.doesNotThrow(() => {
      closed = queue.close(metadata);
    });

    if (closed === undefined) {
      throw new Error("Expected close to return a promise");
    }

    let closeSettled = false;
    void closed.then(() => {
      closeSettled = true;
    });
    queue.observe(observation("pi.agent_start"));

    assert.equal(activeStart.signal.aborted, true);
    assert.deepEqual(
      recording.invocations.map(({ submission }) => submission.kind),
      ["start"],
    );
    assert.deepEqual(timers.timers.activeTimers(), []);

    await closed;
    assert.equal(closeSettled, true);

    activeStart.complete({ ok: true });
    await flushMicrotasks();
    assert.deepEqual(
      recording.invocations.map(({ submission }) => submission.kind),
      ["start"],
    );
    assert.deepEqual(timers.timers.activeTimers(), []);
  });
}

test("settles a dispatcher-managed shutdown when the child exits during confirmation", async (t) => {
  const diagnostics: unknown[][] = [];
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const { child, queue, spawnCalls, timers } = createShutdownHarness();
  queue.start(metadata);
  queue.observe(observation());
  const closed = queue.close(metadata);
  let closeSettled = false;
  void closed.then(() => {
    closeSettled = true;
  });

  timers.advanceBy(TERMINATION_ESCALATION_DELAY_MS);
  assert.deepEqual(child.killCalls, ["SIGTERM"]);
  timers.advanceBy(TERMINATION_CONFIRMATION_DELAY_MS);
  assert.deepEqual(child.killCalls, ["SIGTERM", "SIGKILL"]);
  assert.equal(closeSettled, false);
  child.emitExit(null, "SIGKILL");
  await closed;

  assert.equal(closeSettled, true);
  assert.deepEqual(diagnostics, [[{ category: "signal", sessionId: metadata.sessionId }]]);
  assert.equal(spawnCalls(), 1);
  assert.deepEqual(timers.activeTimers(), []);
  assert.equal(child.exitListeners.length, 0);
});

test("defers final fallback only for the exact dispatcher promise", async (t) => {
  t.mock.method(console, "error", () => undefined);

  const { child, queue, timers } = createShutdownHarness();
  queue.start(metadata);
  queue.observe(observation());
  const closed = queue.close(metadata);
  let closeSettled = false;
  void closed.then(() => {
    closeSettled = true;
  });

  timers.advanceBy(TERMINATION_ESCALATION_DELAY_MS);
  timers.advanceBy(TERMINATION_CONFIRMATION_DELAY_MS);
  assert.deepEqual(
    timers.activeTimers().map(({ delayMs, dueAtMs }) => ({ delayMs, dueAtMs })),
    [
      { delayMs: 1_500, dueAtMs: 1_500 },
      { delayMs: TERMINATION_CONFIRMATION_DELAY_MS, dueAtMs: 1_500 },
    ],
  );

  timers.runNext();
  await flushMicrotasks();
  assert.equal(closeSettled, false);
  assert.deepEqual(
    timers.activeTimers().map(({ delayMs, dueAtMs }) => ({ delayMs, dueAtMs })),
    [{ delayMs: TERMINATION_CONFIRMATION_DELAY_MS, dueAtMs: 1_500 }],
  );

  timers.runNext();
  await closed;
  assert.deepEqual(child.killCalls, ["SIGTERM", "SIGKILL"]);
  assert.deepEqual(timers.activeTimers(), []);
  assert.equal(child.exitListeners.length, 1);

  child.emitExit(null, "SIGKILL");
  await flushMicrotasks();
  assert.equal(child.exitListeners.length, 0);
});

test("preserves the original absolute deadline after a late reserve callback", async (t) => {
  const diagnostics: unknown[][] = [];
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const { child, queue, spawnCalls, timers } = createShutdownHarness();
  queue.start(metadata);
  queue.observe(observation());
  const closed = queue.close(metadata);
  let closeSettled = false;
  void closed.then(() => {
    closeSettled = true;
  });

  timers.runNextAt(1_005);
  assert.equal(spawnCalls(), 1);
  assert.deepEqual(child.killCalls, ["SIGTERM", "SIGKILL"]);
  assert.equal(closeSettled, false);

  timers.runNextAt(1_506);
  await flushMicrotasks();
  assert.equal(closeSettled, false);
  assert.deepEqual(
    timers.activeTimers().map(({ delayMs, dueAtMs }) => ({ delayMs, dueAtMs })),
    [{ delayMs: 495, dueAtMs: 1_500 }],
  );

  timers.runNextAt(1_506);
  await closed;
  assert.equal(spawnCalls(), 1);
  assert.deepEqual(child.killCalls, ["SIGTERM", "SIGKILL"]);
  assert.deepEqual(timers.activeTimers(), []);
  assert.deepEqual(diagnostics, [[{ category: "signal", sessionId: metadata.sessionId }]]);
  child.emitExit(null, "SIGKILL");
  await flushMicrotasks();
  assert.equal(child.exitListeners.length, 0);
});

test("synthesizes one matching start and suppresses a duplicate local start", async () => {
  const recording = createRecordingDispatch();
  const queue = createSubmissionQueue(config(1), recording.dispatch);
  const observationMetadata: EventMetadata = {
    ...metadata,
    sessionId: "synthesized-session",
    timestamp: "2026-09-21T12:34:57.000Z",
  };

  queue.observe(observation("pi.agent_start", observationMetadata));
  queue.start({ ...metadata, sessionId: "duplicate-session" });
  const closed = queue.close(observationMetadata);

  assert.equal(recording.invocations.length, 1);
  const start = await recording.next();
  assert.deepEqual(start.submission, { kind: "start", metadata: observationMetadata });
  start.complete({ ok: true });

  const acceptedObservation = await recording.next();
  assert.deepEqual(
    acceptedObservation.submission,
    observation("pi.agent_start", observationMetadata),
  );
  acceptedObservation.complete({ ok: true });

  const end = await recording.next();
  assert.deepEqual(end.submission, { kind: "end", metadata: observationMetadata });
  end.complete({ ok: true });
  await closed;

  assert.deepEqual(
    recording.invocations.map(({ submission }) => submission.kind),
    ["start", "observation", "end"],
  );
});

test("counts active observations toward capacity while retaining lifecycle slots", async (t) => {
  const diagnostics: unknown[][] = [];
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const recording = createRecordingDispatch();
  const queue = createSubmissionQueue(config(2), recording.dispatch);
  queue.start(metadata);
  assert.equal(recording.invocations.length, 1);
  const start = await recording.next();

  queue.observe(observation("pi.turn_start"));
  queue.observe(observation("pi.turn_end"));
  start.complete({ ok: true });

  const activeObservation = await recording.next();
  assert.equal(activeObservation.submission.kind, "observation");
  queue.observe(observation("pi.agent_start"));
  const closed = queue.close(metadata);
  assert.strictEqual(queue.close({ ...metadata, timestamp: "2026-09-21T12:35:00.000Z" }), closed);

  assert.deepEqual(diagnostics, [
    [{ category: "queue-overflow", nativeEvent: "agent_start", sessionId: metadata.sessionId }],
  ]);

  activeObservation.complete({ ok: true });
  const queuedObservation = await recording.next();
  assert.equal(queuedObservation.submission.kind, "observation");
  queuedObservation.complete({ ok: true });

  const end = await recording.next();
  assert.deepEqual(end.submission, { kind: "end", metadata });
  end.complete({ ok: true });
  await closed;

  assert.deepEqual(
    recording.invocations.map(({ submission }) => submission.kind),
    ["start", "observation", "observation", "end"],
  );
});

test("dispatches accepted work in FIFO order and continues after a failed result", async () => {
  const recording = createRecordingDispatch();
  const queue = createSubmissionQueue(config(2), recording.dispatch);
  queue.start(metadata);
  queue.observe(observation("pi.turn_start"));
  queue.observe(observation("pi.turn_end"));
  const closed = queue.close(metadata);

  assert.equal(recording.invocations.length, 1);
  const start = await recording.next();
  assert.equal(start.submission.kind, "start");
  start.complete({ ok: true });

  const firstObservation = await recording.next();
  assert.equal(firstObservation.submission.kind, "observation");
  if (firstObservation.submission.kind !== "observation") {
    throw new Error("Expected an observation");
  }
  assert.equal(firstObservation.submission.hookType, "pi.turn_start");
  firstObservation.complete({ category: "spawn", ok: false });

  const secondObservation = await recording.next();
  assert.equal(secondObservation.submission.kind, "observation");
  if (secondObservation.submission.kind !== "observation") {
    throw new Error("Expected an observation");
  }
  assert.equal(secondObservation.submission.hookType, "pi.turn_end");
  secondObservation.complete({ ok: true });

  const end = await recording.next();
  assert.equal(end.submission.kind, "end");
  end.complete({ ok: true });
  await closed;

  assert.deepEqual(
    recording.invocations.map(({ submission }) => submission.kind),
    ["start", "observation", "observation", "end"],
  );
});

test("drops only saturated observations with fresh frozen identity-only diagnostics", async (t) => {
  const credentialSentinel = "credential-sentinel";
  const environmentSentinel = "environment-sentinel";
  const hookSentinel = "hook-sentinel";
  const pathSentinel = "/private/path-sentinel";
  const payloadSentinel = "payload-sentinel";
  const sessionSentinel = "session-sentinel";
  const diagnostics: unknown[][] = [];
  const restoreEnvironment = replaceEnvironmentValue("QUEUE_SECRET", environmentSentinel);
  t.after(restoreEnvironment);
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const recording = createRecordingDispatch();
  const queue = createSubmissionQueue(
    { ...config(1), executable: `harness-events-${credentialSentinel}` },
    recording.dispatch,
  );
  queue.start(metadata);
  assert.equal(recording.invocations.length, 1);
  const start = await recording.next();

  queue.observe(observation("pi.turn_start"));
  queue.observe(observation("pi.turn_end", metadata, { payload: payloadSentinel }));
  queue.observe(
    observation(`pi.${hookSentinel}-invalid`, {
      ...metadata,
      currentWorkingDirectory: pathSentinel,
      sessionId: `${sessionSentinel}/invalid`,
    }),
  );

  assert.deepEqual(diagnostics, [
    [{ category: "queue-overflow", nativeEvent: "turn_end", sessionId: metadata.sessionId }],
    [{ category: "queue-overflow" }],
  ]);

  const firstDiagnostic = diagnosticAt(diagnostics, 0);
  const secondDiagnostic = diagnosticAt(diagnostics, 1);
  assert.equal(Object.isFrozen(firstDiagnostic), true);
  assert.equal(Object.isFrozen(secondDiagnostic), true);
  assert.notStrictEqual(firstDiagnostic, secondDiagnostic);
  assert.equal(Reflect.set(firstDiagnostic, "sentinel", payloadSentinel), false);
  assert.deepEqual(Reflect.ownKeys(firstDiagnostic), ["category", "nativeEvent", "sessionId"]);
  assert.deepEqual(Reflect.ownKeys(secondDiagnostic), ["category"]);

  const output = JSON.stringify(diagnostics);
  for (const sentinel of [
    credentialSentinel,
    environmentSentinel,
    hookSentinel,
    pathSentinel,
    payloadSentinel,
    sessionSentinel,
  ]) {
    assert.doesNotMatch(output, new RegExp(sentinel));
  }

  const closed = queue.close(metadata);
  start.complete({ ok: true });
  const acceptedObservation = await recording.next();
  assert.equal(acceptedObservation.submission.kind, "observation");
  acceptedObservation.complete({ ok: true });
  const end = await recording.next();
  assert.equal(end.submission.kind, "end");
  end.complete({ ok: true });
  await closed;
});
