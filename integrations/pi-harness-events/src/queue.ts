import type { ExtensionConfig } from "./config.ts";
import {
  bindDispatcherShutdownDeadline,
  isDispatcherManagedShutdown,
  TERMINATION_CONFIRMATION_DELAY_MS,
  TERMINATION_ESCALATION_DELAY_MS,
} from "./dispatcher.ts";
import type { DispatchResult, EventMetadata, Submission } from "./model.ts";

export interface SubmissionQueue {
  start(metadata: EventMetadata): void;
  observe(submission: Extract<Submission, { readonly kind: "observation" }>): void;
  close(metadata: EventMetadata): Promise<void>;
}

export type Dispatch = (submission: Submission, signal: AbortSignal) => Promise<DispatchResult>;

export interface QueueTimer {
  cancel(): void;
}

export interface QueueTimers {
  now(): number;
  setTimeout(callback: () => void, delayMs: number): QueueTimer;
  clearTimeout(timer: QueueTimer): void;
}

type QueueState = "new" | "active" | "closing" | "closed";

type ObservationSubmission = Extract<Submission, { readonly kind: "observation" }>;

interface QueuedSubmission {
  readonly observation: boolean;
  readonly submission: Submission;
}

interface ActiveDispatch {
  readonly controller: AbortController;
  readonly promise: Promise<DispatchResult>;
  readonly queued: QueuedSubmission;
}

interface DiagnosticIdentity {
  readonly nativeEvent?: string;
  readonly sessionId?: string;
}

const MAXIMUM_DIAGNOSTIC_IDENTITY_LENGTH = 128;
const TERMINATION_RESERVE_MS = TERMINATION_ESCALATION_DELAY_MS + TERMINATION_CONFIRMATION_DELAY_MS;

const nodeTimers: QueueTimers = {
  now() {
    return Date.now();
  },
  setTimeout(callback, delayMs) {
    const timer = setTimeout(callback, delayMs);
    return { cancel: () => clearTimeout(timer) };
  },
  clearTimeout(timer) {
    timer.cancel();
  },
};

export function createSubmissionQueue(
  config: ExtensionConfig,
  dispatch: Dispatch,
  timers: QueueTimers = nodeTimers,
): SubmissionQueue {
  let active: ActiveDispatch | undefined;
  const pending: QueuedSubmission[] = [];
  let closePromise: Promise<void> | undefined;
  let deadlineTimer: QueueTimer | undefined;
  let observationCount = 0;
  let pumping = false;
  let reserveTimer: QueueTimer | undefined;
  let resolveClose: (() => void) | undefined;
  let shutdownDeadlineMs: number | undefined;
  let state: QueueState = "new";

  const clearTimer = (timer: QueueTimer | undefined): void => {
    if (timer === undefined) {
      return;
    }

    try {
      timers.clearTimeout(timer);
    } catch {}
  };

  const clearShutdownTimers = (): void => {
    clearTimer(reserveTimer);
    reserveTimer = undefined;
    clearTimer(deadlineTimer);
    deadlineTimer = undefined;
  };

  const settleClose = (): void => {
    if (state !== "closing") {
      return;
    }

    state = "closed";
    clearShutdownTimers();
    const resolve = resolveClose;
    resolveClose = undefined;
    resolve?.();
  };

  const finishClose = (): void => {
    if (state !== "closing" || active !== undefined || pending.length !== 0) {
      return;
    }

    settleClose();
  };

  const discardPending = (): void => {
    while (pending.length > 0) {
      const queued = pending.shift();

      if (queued?.observation === true) {
        observationCount -= 1;
      }
    }
  };

  const terminateForShutdown = (): void => {
    if (state !== "closing") {
      return;
    }

    discardPending();

    const current = active;

    if (current !== undefined) {
      if (shutdownDeadlineMs !== undefined) {
        bindDispatcherShutdownDeadline(current.promise, shutdownDeadlineMs);
      }

      try {
        current.controller.abort();
      } catch {}
    }

    finishClose();
  };

  const failOpenShutdown = (): void => {
    terminateForShutdown();
    settleClose();
  };

  const isActiveDispatcherManagedShutdown = (): boolean => {
    if (active === undefined) {
      return false;
    }

    return isDispatcherManagedShutdown(active.promise);
  };

  const scheduleShutdown = (): void => {
    const deadlineMs = shutdownDeadlineMs;

    if (deadlineMs === undefined || state !== "closing") {
      return;
    }

    let reserveDelayMs: number;

    try {
      reserveDelayMs = Math.max(0, deadlineMs - TERMINATION_RESERVE_MS - timers.now());
    } catch {
      failOpenShutdown();
      return;
    }

    let reserveCallbackRan = false;

    try {
      const timer = timers.setTimeout(() => {
        reserveCallbackRan = true;
        reserveTimer = undefined;
        terminateForShutdown();
      }, reserveDelayMs);

      if (reserveCallbackRan || state !== "closing") {
        clearTimer(timer);
      } else {
        reserveTimer = timer;
      }
    } catch {
      terminateForShutdown();
    }

    if (state !== "closing") {
      return;
    }

    let deadlineDelayMs: number;

    try {
      deadlineDelayMs = Math.max(0, deadlineMs - timers.now());
    } catch {
      failOpenShutdown();
      return;
    }

    let deadlineCallbackRan = false;

    try {
      const timer = timers.setTimeout(() => {
        deadlineCallbackRan = true;
        deadlineTimer = undefined;
        terminateForShutdown();

        if (state === "closing" && !isActiveDispatcherManagedShutdown()) {
          settleClose();
        }
      }, deadlineDelayMs);

      if (deadlineCallbackRan || state !== "closing") {
        clearTimer(timer);
      } else {
        deadlineTimer = timer;
      }
    } catch {
      failOpenShutdown();
    }
  };

  const pump = async (): Promise<void> => {
    while (state !== "closed") {
      const queued = pending.shift();

      if (queued === undefined) {
        pumping = false;
        finishClose();
        return;
      }

      let current: ActiveDispatch | undefined;

      try {
        const controller = new AbortController();
        const promise = dispatch(queued.submission, controller.signal);
        current = { controller, promise, queued };
        active = current;
        await promise;
      } catch {
      } finally {
        if (current !== undefined && active === current) {
          active = undefined;
        }

        if (queued.observation) {
          observationCount -= 1;
        }
      }
    }

    pumping = false;
  };

  const startPump = (): void => {
    if (pumping) {
      return;
    }

    pumping = true;
    void pump();
  };

  const enqueue = (submission: Submission): void => {
    pending.push({ observation: submission.kind === "observation", submission });
    startPump();
  };

  const start = (metadata: EventMetadata): void => {
    if (state !== "new") {
      return;
    }

    state = "active";
    enqueue({ kind: "start", metadata });
  };

  const observe = (submission: ObservationSubmission): void => {
    if (state === "closing" || state === "closed") {
      return;
    }

    if (observationCount >= config.observationCapacity) {
      reportOverflow(submission);
      return;
    }

    if (state === "new") {
      start(submission.metadata);
    }

    if (state !== "active") {
      return;
    }

    observationCount += 1;
    enqueue(submission);
  };

  const close = (metadata: EventMetadata): Promise<void> => {
    if (closePromise !== undefined) {
      return closePromise;
    }

    state = "closing";
    closePromise = new Promise((resolve) => {
      resolveClose = resolve;
    });

    try {
      shutdownDeadlineMs = timers.now() + config.shutdownTimeoutMs;
    } catch {
      enqueue({ kind: "end", metadata });
      failOpenShutdown();
      return closePromise;
    }

    enqueue({ kind: "end", metadata });
    scheduleShutdown();
    return closePromise;
  };

  return {
    close,
    observe,
    start,
  };
}

function reportOverflow(submission: ObservationSubmission): void {
  const identity = createDiagnosticIdentity(submission);
  const diagnostic = Object.freeze({
    category: "queue-overflow" as const,
    ...(identity.nativeEvent === undefined ? {} : { nativeEvent: identity.nativeEvent }),
    ...(identity.sessionId === undefined ? {} : { sessionId: identity.sessionId }),
  });

  try {
    console.error(diagnostic);
  } catch {}
}

function createDiagnosticIdentity(submission: ObservationSubmission): DiagnosticIdentity {
  try {
    const nativeEvent = boundedDiagnosticIdentity(nativeEventFromHookType(submission.hookType));
    const sessionId = boundedDiagnosticIdentity(submission.metadata.sessionId);

    return {
      ...(nativeEvent === undefined ? {} : { nativeEvent }),
      ...(sessionId === undefined ? {} : { sessionId }),
    };
  } catch {
    return {};
  }
}

function boundedDiagnosticIdentity(value: unknown): string | undefined {
  if (
    typeof value !== "string" ||
    value.length > MAXIMUM_DIAGNOSTIC_IDENTITY_LENGTH ||
    !/^[A-Za-z0-9._:-]+$/.test(value)
  ) {
    return undefined;
  }

  return value;
}

function nativeEventFromHookType(hookType: string): string | undefined {
  const nativeEvent = hookType.startsWith("pi.") ? hookType.slice("pi.".length) : undefined;

  if (nativeEvent === undefined || !/^[a-z][a-z0-9_]*$/.test(nativeEvent)) {
    return undefined;
  }

  return nativeEvent;
}
