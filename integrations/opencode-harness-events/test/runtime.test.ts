// biome-ignore-all lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
// biome-ignore-all lint/correctness/useQwikValidLexicalScope: Tests intentionally use closures for injected runtime seams.
// biome-ignore-all lint/style/noExcessiveLinesPerFile: Runtime and shutdown coverage share the same injected seam.

import { expect, test } from "bun:test";
import {
  createSessionRuntime,
  type Dispatch,
  type SessionRuntime,
} from "../src/runtime";
import type { DispatchResult, EventMetadata, Submission } from "../src/model";

const EXPECTED_COMPLETED_SESSION_SUBMISSIONS = 4;
const ORDINARY_DISPATCH_FALLBACK_MS = 50;
const TEST_COMPLETION_TIMEOUT_MS = 100;

const metadata = (sessionId: string): EventMetadata => ({
  generation: "v2",
  sessionId,
  projectName: "project",
  currentWorkingDirectory: "/tmp/project",
  timestamp: "2026-09-20T12:34:56.789Z",
});

const observation = (
  eventMetadata: EventMetadata,
  hookType = "opencode.v2.session.created",
): Extract<Submission, { readonly kind: "observation" }> => ({
  kind: "observation",
  metadata: eventMetadata,
  hookType,
  data: { sessionId: eventMetadata.sessionId },
});

function recordingDispatch(): {
  readonly submissions: Submission[];
  readonly dispatch: Dispatch;
} {
  const submissions: Submission[] = [];
  const dispatch: Dispatch = (submission) => {
    submissions.push(submission);
    return Promise.resolve<DispatchResult>({ ok: true });
  };

  return { submissions, dispatch };
}

function createDeferred(): {
  readonly promise: Promise<void>;
  readonly resolve: () => void;
  readonly reject: () => void;
} {
  let resolvePromise: ((value: void | PromiseLike<void>) => void) | undefined;
  let rejectPromise: (() => void) | undefined;
  const deferred = new Promise<void>((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = () => reject(new Error("telemetry rejection"));
  });

  return {
    promise: deferred,
    resolve: () => resolvePromise?.(),
    reject: () => rejectPromise?.(),
  };
}

function nextTurn(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

async function completesWithin(
  promise: Promise<void>,
  timeoutMs: number,
): Promise<boolean> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const result = await Promise.race([
    promise.then(() => true),
    new Promise<boolean>((resolve) => {
      timeout = setTimeout(() => resolve(false), timeoutMs);
    }),
  ]);
  if (timeout !== undefined) {
    clearTimeout(timeout);
  }
  return result;
}

function neverSettlingDispatch(): Promise<DispatchResult> {
  return new Promise<DispatchResult>((_resolve) => {
    // This test double intentionally ignores abort until the shutdown bound.
  });
}

test("orders one session, suppresses duplicate lifecycle signals, and removes it after end", async () => {
  const recording = recordingDispatch();
  const runtime = createSessionRuntime(recording.dispatch);
  const sessionMetadata = metadata("session-123");
  const deletion = observation(sessionMetadata, "opencode.v2.session.deleted");

  runtime.start(sessionMetadata);
  runtime.start(sessionMetadata);
  runtime.observe(observation(sessionMetadata));
  runtime.observe(deletion);
  runtime.end(sessionMetadata);
  runtime.end(sessionMetadata);

  await nextTurn();
  await runtime.close();

  expect(recording.submissions).toEqual([
    { kind: "start", metadata: sessionMetadata },
    observation(sessionMetadata),
    deletion,
    { kind: "end", metadata: sessionMetadata },
  ]);

  runtime.start(sessionMetadata);
  await nextTurn();
  expect(recording.submissions).toHaveLength(
    EXPECTED_COMPLETED_SESSION_SUBMISSIONS,
  );
});

test("synthesizes a start before an observation for a resumed session", async () => {
  const recording = recordingDispatch();
  const runtime = createSessionRuntime(recording.dispatch);
  const sessionMetadata = metadata("resumed-session");
  const resumedObservation = observation(
    sessionMetadata,
    "opencode.v2.session.updated",
  );

  runtime.observe(resumedObservation);
  runtime.end(sessionMetadata);

  await nextTurn();
  await runtime.close();

  expect(recording.submissions).toEqual([
    { kind: "start", metadata: sessionMetadata },
    resumedObservation,
    { kind: "end", metadata: sessionMetadata },
  ]);
});

test("dispatches different session chains concurrently", async () => {
  const firstSessionStarted = createDeferred();
  const recording: Submission[] = [];
  const dispatch: Dispatch = async (submission) => {
    recording.push(submission);
    if (
      submission.kind === "start" &&
      submission.metadata.sessionId === "session-a"
    ) {
      await firstSessionStarted.promise;
    }
    return { ok: true };
  };
  const runtime = createSessionRuntime(dispatch);

  runtime.start(metadata("session-a"));
  runtime.start(metadata("session-b"));
  await Promise.resolve();
  await Promise.resolve();

  expect(recording.map((submission) => submission.metadata.sessionId)).toEqual([
    "session-a",
    "session-b",
  ]);

  firstSessionStarted.resolve();
  await nextTurn();
  await runtime.close();
});

test("contains rejected dispatches and still removes an ended session", async () => {
  const recording: Submission[] = [];
  const dispatch: Dispatch = (submission) => {
    recording.push(submission);
    return Promise.reject(new Error("telemetry rejection"));
  };
  const runtime: SessionRuntime = createSessionRuntime(dispatch);
  const sessionMetadata = metadata("rejected-session");

  runtime.observe(observation(sessionMetadata));
  runtime.end(sessionMetadata);

  await nextTurn();
  await expect(runtime.close()).resolves.toBeUndefined();

  expect(recording.map((submission) => submission.kind)).toEqual([
    "start",
    "observation",
    "end",
  ]);

  runtime.start(sessionMetadata);
  await nextTurn();
  expect(recording.map((submission) => submission.kind)).toEqual([
    "start",
    "observation",
    "end",
  ]);
});

function createDeferredSession(sessionId: string) {
  const firstDispatch = createDeferred();
  const recording: Submission[] = [];
  const dispatch: Dispatch = async (submission) => {
    recording.push(submission);
    if (submission.kind === "start") {
      await firstDispatch.promise;
    }
    return { ok: true };
  };
  const runtime = createSessionRuntime(dispatch);
  const sessionMetadata = metadata(sessionId);
  const resumedObservation = observation(
    sessionMetadata,
    "opencode.v2.session.updated",
  );

  runtime.start(sessionMetadata);
  runtime.observe(resumedObservation);
  runtime.end(sessionMetadata);

  return {
    firstDispatch,
    recording,
    runtime,
    sessionMetadata,
    resumedObservation,
  };
}

test("holds same-session dispatches behind a pending start", async () => {
  await Promise.all(
    (["resolve", "reject"] as const).map(async (settle) => {
      const session = createDeferredSession(`deferred-${settle}`);

      await Promise.resolve();
      expect(session.recording).toEqual([
        { kind: "start", metadata: session.sessionMetadata },
      ]);

      if (settle === "resolve") {
        session.firstDispatch.resolve();
      } else {
        session.firstDispatch.reject();
      }

      await nextTurn();
      await session.runtime.close();

      expect(session.recording).toEqual([
        { kind: "start", metadata: session.sessionMetadata },
        session.resumedObservation,
        { kind: "end", metadata: session.sessionMetadata },
      ]);
    }),
  );
});

test("does not duplicate an ordinary end already in flight during close", async () => {
  const sessionMetadata = metadata("pending-end");
  const endStarted = createDeferred();
  const submissions: Submission[] = [];
  let endCount = 0;
  let releaseOrdinaryEnd: () => void = () => undefined;
  const ordinaryEnd = new Promise<DispatchResult>((resolve) => {
    releaseOrdinaryEnd = () => resolve({ ok: true });
  });
  const dispatch: Dispatch = (submission) => {
    submissions.push(submission);
    if (submission.kind !== "end") {
      return Promise.resolve({ ok: true });
    }

    endCount += 1;
    if (endCount === 1) {
      endStarted.resolve();
      return ordinaryEnd;
    }

    return Promise.resolve({ ok: true });
  };
  const runtime = createSessionRuntime(dispatch, { shutdownTimeoutMs: 20 });

  runtime.start(sessionMetadata);
  await nextTurn();
  runtime.end(sessionMetadata);
  await endStarted.promise;

  const closePromise = runtime.close();
  expect(await completesWithin(closePromise, TEST_COMPLETION_TIMEOUT_MS)).toBe(
    true,
  );
  releaseOrdinaryEnd();
  await closePromise;

  expect(submissions).toEqual([
    { kind: "start", metadata: sessionMetadata },
    { kind: "end", metadata: sessionMetadata },
  ]);
});

// biome-ignore lint/complexity/noExcessiveLinesPerFunction: Keep the close rejection scenario end-to-end.
test("rejects new work at close and skips queued ordinary submissions", async () => {
  const activeMetadata = metadata("closing-session");
  const lateMetadata = metadata("late-session");
  const ordinaryStarted = createDeferred();
  const submissions: Submission[] = [];
  const shutdownSignals: AbortSignal[] = [];
  let ordinarySignal: AbortSignal | undefined;
  const dispatch: Dispatch = (submission, signal) => {
    submissions.push(submission);
    if (
      submission.kind === "start" &&
      submission.metadata.sessionId === activeMetadata.sessionId
    ) {
      ordinarySignal = signal;
      ordinaryStarted.resolve();
      return new Promise<DispatchResult>((resolve) => {
        const timer = setTimeout(
          () => resolve({ ok: true }),
          ORDINARY_DISPATCH_FALLBACK_MS,
        );
        signal.addEventListener(
          "abort",
          () => {
            clearTimeout(timer);
            resolve({ ok: false, category: "signal" });
          },
          { once: true },
        );
      });
    }
    if (submission.kind === "end") {
      shutdownSignals.push(signal);
    }
    return Promise.resolve({ ok: true });
  };
  const runtime = createSessionRuntime(dispatch, { shutdownTimeoutMs: 20 });

  runtime.start(activeMetadata);
  runtime.observe(observation(activeMetadata));
  runtime.end(activeMetadata);
  await ordinaryStarted.promise;

  const closePromise = runtime.close();
  runtime.start(lateMetadata);
  runtime.observe(observation(lateMetadata));
  runtime.end(lateMetadata);

  await closePromise;
  await nextTurn();

  expect(submissions).toEqual([
    { kind: "start", metadata: activeMetadata },
    { kind: "end", metadata: activeMetadata },
  ]);
  expect(ordinarySignal?.aborted).toBe(true);
  const [shutdownSignal] = shutdownSignals;
  if (ordinarySignal === undefined || shutdownSignal === undefined) {
    throw new Error("expected ordinary and shutdown dispatch signals");
  }
  expect(shutdownSignal).not.toBe(ordinarySignal);
  expect(shutdownSignal.aborted).toBe(false);
});

// biome-ignore lint/complexity/noExcessiveLinesPerFunction: Keep the concurrent shutdown scenario end-to-end.
test("runs one concurrent shutdown end per active session within its bound", async () => {
  const sessionMetadata = [metadata("shutdown-a"), metadata("shutdown-b")];
  const startsReady = createDeferred();
  const endsReady = createDeferred();
  const ordinarySignals: AbortSignal[] = [];
  const shutdownSignals: AbortSignal[] = [];
  const shutdownSessionIds: string[] = [];
  let startCount = 0;
  let endCount = 0;
  const dispatch: Dispatch = (submission, signal) => {
    if (submission.kind === "start") {
      ordinarySignals.push(signal);
      startCount += 1;
      if (startCount === sessionMetadata.length) {
        startsReady.resolve();
      }
      return neverSettlingDispatch();
    }
    if (submission.kind === "end") {
      shutdownSignals.push(signal);
      shutdownSessionIds.push(submission.metadata.sessionId);
      endCount += 1;
      if (endCount === sessionMetadata.length) {
        endsReady.resolve();
      }
      return neverSettlingDispatch();
    }
    return Promise.resolve({ ok: true });
  };
  const runtime = createSessionRuntime(dispatch, { shutdownTimeoutMs: 20 });

  for (const session of sessionMetadata) {
    runtime.start(session);
  }
  await startsReady.promise;

  const closePromise = runtime.close();
  expect(
    await completesWithin(endsReady.promise, TEST_COMPLETION_TIMEOUT_MS),
  ).toBe(true);
  expect(await completesWithin(closePromise, TEST_COMPLETION_TIMEOUT_MS)).toBe(
    true,
  );

  expect(
    shutdownSessionIds.sort((left, right) => left.localeCompare(right)),
  ).toEqual(["shutdown-a", "shutdown-b"]);
  expect(shutdownSignals).toHaveLength(2);
  expect(ordinarySignals).toHaveLength(2);
  const [firstShutdownSignal, secondShutdownSignal] = shutdownSignals;
  if (firstShutdownSignal === undefined || secondShutdownSignal === undefined) {
    throw new Error("expected one shutdown signal per active session");
  }
  expect(firstShutdownSignal).not.toBe(secondShutdownSignal);
  for (const shutdownSignal of shutdownSignals) {
    expect(ordinarySignals.includes(shutdownSignal)).toBe(false);
    expect(shutdownSignal.aborted).toBe(true);
  }
  for (const ordinarySignal of ordinarySignals) {
    expect(ordinarySignal.aborted).toBe(true);
  }

  const ordinarySignalCountAfterClose = ordinarySignals.length;
  const lateMetadata = metadata("after-shutdown");
  runtime.start(lateMetadata);
  runtime.observe(observation(lateMetadata));
  runtime.end(lateMetadata);
  await nextTurn();
  expect(ordinarySignals).toHaveLength(ordinarySignalCountAfterClose);
  expect(shutdownSessionIds).not.toContain(lateMetadata.sessionId);
});

test("contains ordinary and shutdown failures while closing failed active sessions", async () => {
  const sessionMetadata = [metadata("failed-a"), metadata("failed-b")];
  const shutdownSessionIds: string[] = [];
  const submissions: Submission[] = [];
  const dispatch: Dispatch = (submission) => {
    submissions.push(submission);
    if (submission.kind === "end") {
      shutdownSessionIds.push(submission.metadata.sessionId);
    }
    return Promise.reject(new Error("telemetry rejection"));
  };
  const runtime = createSessionRuntime(dispatch, { shutdownTimeoutMs: 20 });

  for (const session of sessionMetadata) {
    runtime.start(session);
  }
  await nextTurn();

  const firstClose = runtime.close();
  const secondClose = runtime.close();
  await expect(Promise.all([firstClose, secondClose])).resolves.toEqual([
    undefined,
    undefined,
  ]);

  expect(
    shutdownSessionIds.sort((left, right) => left.localeCompare(right)),
  ).toEqual(["failed-a", "failed-b"]);
  const lateMetadata = metadata("after-failure-shutdown");
  runtime.start(lateMetadata);
  runtime.observe(observation(lateMetadata));
  runtime.end(lateMetadata);
  await nextTurn();
  expect(
    submissions.some(
      (submission) => submission.metadata.sessionId === lateMetadata.sessionId,
    ),
  ).toBe(false);
});
