import { spawn } from "node:child_process";
import process from "node:process";

import type { ExtensionConfig } from "./config.ts";
import type { DispatchFailureCategory, DispatchResult, Submission } from "./model.ts";

export interface ProcessSpawnOptions {
  readonly cwd: string;
  readonly env: NodeJS.ProcessEnv;
  readonly shell: false;
  readonly stdio: ["pipe", "pipe", "pipe"];
}

export interface ChildInput {
  end(chunk?: string): void;
  once(event: "error", listener: (error: Error) => void): void;
}

export interface ChildOutput {
  on(event: "data", listener: (chunk: Uint8Array) => void): void;
  once(event: "error", listener: (error: Error) => void): void;
  resume(): void;
}

export interface DispatcherTimer {
  cancel(): void;
}

export interface DispatcherTimers {
  now(): number;
  setTimeout(callback: () => void, delayMs: number): DispatcherTimer;
  clearTimeout(timer: DispatcherTimer): void;
}

export const MAXIMUM_STDERR_CAPTURE_BYTES = 8 * 1024;
export const TERMINATION_ESCALATION_DELAY_MS = 500;
export const TERMINATION_CONFIRMATION_DELAY_MS = 500;

const MAXIMUM_DIAGNOSTIC_IDENTITY_LENGTH = 128;

export interface SpawnedChild {
  readonly stdin: ChildInput;
  readonly stdout: ChildOutput;
  readonly stderr: ChildOutput;
  once(event: "error", listener: (error: Error) => void): SpawnedChild;
  once(
    event: "exit",
    listener: (code: number | null, signal: NodeJS.Signals | null) => void,
  ): SpawnedChild;
  kill(signal?: NodeJS.Signals | number): boolean;
}

export type ProcessSpawnResult =
  | { readonly kind: "spawned"; readonly child: SpawnedChild }
  | { readonly kind: "unavailable-stdio" };

export type ProcessSpawner = (
  executable: string,
  arguments_: readonly string[],
  options: ProcessSpawnOptions,
) => ProcessSpawnResult;

export type Dispatcher = (
  config: ExtensionConfig,
  submission: Submission,
  signal: AbortSignal,
) => Promise<DispatchResult>;

interface DispatcherShutdownRegistration {
  active: boolean;
  deadlineMs: number | undefined;
  managed: boolean;
}

const dispatcherManagedShutdowns = new WeakMap<
  Promise<DispatchResult>,
  DispatcherShutdownRegistration
>();

export function bindDispatcherShutdownDeadline(
  promise: Promise<DispatchResult>,
  deadlineMs: number,
): void {
  try {
    if (!Number.isFinite(deadlineMs)) {
      return;
    }

    const registration = dispatcherManagedShutdowns.get(promise);

    if (registration?.active === true) {
      registration.deadlineMs = deadlineMs;
    }
  } catch {}
}

export function isDispatcherManagedShutdown(promise: Promise<DispatchResult>): boolean {
  try {
    const registration = dispatcherManagedShutdowns.get(promise);
    return registration?.active === true && registration.managed;
  } catch {
    return false;
  }
}

interface CommandInvocation {
  readonly arguments_: readonly string[];
  readonly stdin: string | undefined;
}

type CommandPreparation =
  | { readonly kind: "ready"; readonly invocation: CommandInvocation }
  | { readonly kind: "normalization" }
  | { readonly kind: "serialization" };

interface DiagnosticIdentity {
  readonly nativeEvent?: string;
  readonly sessionId?: string;
}

interface ShutdownDeadline {
  readonly deadlineMs: number;
  readonly now: () => number;
}

const nodeProcessSpawner: ProcessSpawner = (executable, arguments_, options) => {
  const child = spawn(executable, arguments_, options);

  if (child.stdin === null || child.stdout === null || child.stderr === null) {
    return { kind: "unavailable-stdio" };
  }

  return { kind: "spawned", child };
};

const nodeTimers: DispatcherTimers = {
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

export function createDispatcher(
  spawnProcess: ProcessSpawner = nodeProcessSpawner,
  timers: DispatcherTimers = nodeTimers,
): Dispatcher {
  return (config, submission, signal) => {
    const registration: DispatcherShutdownRegistration = {
      active: true,
      deadlineMs: undefined,
      managed: false,
    };
    const result = beginDispatch(config, submission, signal, spawnProcess, timers, registration);

    try {
      dispatcherManagedShutdowns.set(result, registration);
    } catch {
      registration.active = false;
    }

    return result;
  };
}

export const dispatch: Dispatcher = createDispatcher();

function beginDispatch(
  config: ExtensionConfig,
  submission: Submission,
  signal: AbortSignal,
  spawnProcess: ProcessSpawner,
  timers: DispatcherTimers,
  registration: DispatcherShutdownRegistration,
): Promise<DispatchResult> {
  const identity = createDiagnosticIdentity(submission);
  const preparation = prepareCommandInvocation(submission);
  const finishImmediately = (result: DispatchResult): Promise<DispatchResult> => {
    registration.active = false;
    return Promise.resolve(result);
  };

  if (preparation.kind !== "ready") {
    return finishImmediately(dispatchFailure(preparation.kind, identity));
  }

  if (signal.aborted) {
    return finishImmediately(dispatchFailure("signal", identity));
  }

  try {
    const spawned = spawnProcess(config.executable, preparation.invocation.arguments_, {
      cwd: submission.metadata.currentWorkingDirectory,
      env: process.env,
      shell: false,
      stdio: ["pipe", "pipe", "pipe"],
    });

    if (spawned.kind !== "spawned") {
      return finishImmediately(dispatchFailure("spawn", identity));
    }

    return observeChildOutcome(
      spawned.child,
      preparation.invocation,
      config,
      signal,
      timers,
      identity,
      registration,
    );
  } catch {
    return finishImmediately(dispatchFailure("spawn", identity));
  }
}

function observeChildOutcome(
  child: SpawnedChild,
  invocation: CommandInvocation,
  config: ExtensionConfig,
  signal: AbortSignal,
  timers: DispatcherTimers,
  identity: DiagnosticIdentity,
  registration: DispatcherShutdownRegistration,
): Promise<DispatchResult> {
  return new Promise((resolve) => {
    const stderrCapture = new Uint8Array(MAXIMUM_STDERR_CAPTURE_BYTES);
    let stderrLength = 0;
    let settled = false;
    let terminationCategory: "signal" | "spawn" | "timeout" | undefined;
    let shutdownDeadline: ShutdownDeadline | undefined;
    let timeoutTimer: DispatcherTimer | undefined;
    let escalationTimer: DispatcherTimer | undefined;
    let confirmationTimer: DispatcherTimer | undefined;
    let abortListener: (() => void) | undefined;
    let exitListenerInstalled = false;

    const clearTimer = (timer: DispatcherTimer | undefined): void => {
      if (timer === undefined) {
        return;
      }

      try {
        timers.clearTimeout(timer);
      } catch {}
    };

    const cleanup = (): void => {
      clearTimer(timeoutTimer);
      timeoutTimer = undefined;
      clearTimer(escalationTimer);
      escalationTimer = undefined;
      clearTimer(confirmationTimer);
      confirmationTimer = undefined;

      if (abortListener !== undefined) {
        try {
          signal.removeEventListener("abort", abortListener);
        } catch {}

        abortListener = undefined;
      }
    };

    const settle = (result: DispatchResult): void => {
      if (settled) {
        return;
      }

      settled = true;
      registration.active = false;
      cleanup();
      resolve(result);
    };

    const settleFailure = (category: DispatchFailureCategory): void => {
      if (settled) {
        return;
      }

      settle(dispatchFailure(category, identity));
    };

    const sendSignal = (childSignal: NodeJS.Signals): void => {
      try {
        child.kill(childSignal);
      } catch {}
    };

    const isTerminating = (): boolean => !settled && terminationCategory !== undefined;

    const remainingShutdownTime = (): number | undefined => {
      if (shutdownDeadline === undefined) {
        return undefined;
      }

      try {
        const remaining = shutdownDeadline.deadlineMs - shutdownDeadline.now();
        return Number.isFinite(remaining) ? Math.max(0, remaining) : 0;
      } catch {
        return 0;
      }
    };

    const shutdownDeadlineFromRegistration = (): ShutdownDeadline | undefined => {
      let deadlineMs: number | undefined;

      try {
        deadlineMs = registration.deadlineMs;
      } catch {
        return undefined;
      }

      if (deadlineMs === undefined || !Number.isFinite(deadlineMs)) {
        return undefined;
      }

      return {
        deadlineMs,
        now: () => timers.now(),
      };
    };

    const armExitConfirmation = (): void => {
      if (!isTerminating()) {
        return;
      }

      const delayMs = remainingShutdownTime() ?? TERMINATION_CONFIRMATION_DELAY_MS;

      if (delayMs <= 0) {
        const category = terminationCategory;

        if (category !== undefined) {
          settleFailure(category);
        }

        return;
      }

      let callbackRan = false;
      let timer: DispatcherTimer;

      try {
        timer = timers.setTimeout(() => {
          callbackRan = true;
          confirmationTimer = undefined;
          const category = terminationCategory;

          if (!settled && category !== undefined) {
            settleFailure(category);
          }
        }, delayMs);
      } catch {
        const currentCategory = terminationCategory;

        if (!settled && currentCategory !== undefined) {
          settleFailure(currentCategory);
        }

        return;
      }

      if (!isTerminating() || callbackRan) {
        clearTimer(timer);
        return;
      }

      confirmationTimer = timer;
    };

    const sendKillAndArmConfirmation = (): void => {
      if (!isTerminating()) {
        return;
      }

      sendSignal("SIGKILL");

      if (!isTerminating()) {
        return;
      }

      armExitConfirmation();
    };

    const armKillEscalation = (): void => {
      if (!isTerminating()) {
        return;
      }

      const remaining = remainingShutdownTime();
      const delayMs =
        remaining === undefined
          ? TERMINATION_ESCALATION_DELAY_MS
          : Math.max(0, remaining - TERMINATION_CONFIRMATION_DELAY_MS);

      if (delayMs <= 0) {
        sendKillAndArmConfirmation();
        return;
      }

      let callbackRan = false;
      let timer: DispatcherTimer;

      try {
        timer = timers.setTimeout(() => {
          callbackRan = true;
          escalationTimer = undefined;
          sendKillAndArmConfirmation();
        }, delayMs);
      } catch {
        sendKillAndArmConfirmation();
        return;
      }

      if (!isTerminating() || callbackRan) {
        clearTimer(timer);
        return;
      }

      escalationTimer = timer;
    };

    const terminate = (
      category: "signal" | "spawn" | "timeout",
      deadline: ShutdownDeadline | undefined = undefined,
    ): void => {
      if (settled || terminationCategory !== undefined) {
        return;
      }

      terminationCategory = category;
      shutdownDeadline = deadline;
      clearTimer(timeoutTimer);
      timeoutTimer = undefined;
      sendSignal("SIGTERM");

      if (!isTerminating()) {
        return;
      }

      armKillEscalation();
    };

    const handleAbort = (): void => {
      const deadline = shutdownDeadlineFromRegistration();

      if (deadline !== undefined && !settled && terminationCategory === undefined) {
        registration.managed = true;
      }

      terminate("signal", deadline);
    };

    const consumeStderr = (chunk: Uint8Array): void => {
      if (settled) {
        return;
      }

      try {
        const remaining = stderrCapture.byteLength - stderrLength;

        if (remaining <= 0) {
          return;
        }

        const length = Math.min(remaining, chunk.byteLength);
        stderrCapture.set(chunk.subarray(0, length), stderrLength);
        stderrLength += length;
      } catch {
        terminate("spawn");
      }
    };

    const handleExit = (code: number | null, childSignal: NodeJS.Signals | null): void => {
      if (terminationCategory !== undefined) {
        settleFailure(terminationCategory);
        return;
      }

      if (childSignal !== null) {
        settleFailure("signal");
        return;
      }

      if (code === 0) {
        settle({ ok: true });
        return;
      }

      if (code === 2) {
        settleFailure("usage");
        return;
      }

      if (code === 3) {
        settleFailure("connection");
        return;
      }

      if (code === 4) {
        settleFailure("invocation");
        return;
      }

      settleFailure("unknown-exit");
    };

    const handleChildError = (): void => {
      if (terminationCategory === undefined) {
        settleFailure("spawn");
      }
    };

    const handleStreamError = (): void => {
      if (terminationCategory === undefined) {
        terminate("spawn");
      }
    };

    try {
      child.once("exit", handleExit);
      exitListenerInstalled = true;
      child.once("error", handleChildError);
      child.stdin.once("error", handleStreamError);
      child.stdout.once("error", handleStreamError);
      child.stderr.once("error", handleStreamError);
      child.stdout.on("data", () => undefined);
      child.stderr.on("data", consumeStderr);
      child.stdout.resume();
      child.stderr.resume();

      if (invocation.stdin === undefined) {
        child.stdin.end();
      } else {
        child.stdin.end(invocation.stdin);
      }

      if (settled) {
        return;
      }

      if (signal.aborted) {
        handleAbort();
        return;
      }

      abortListener = handleAbort;
      signal.addEventListener("abort", abortListener, { once: true });

      if (signal.aborted || terminationCategory !== undefined) {
        handleAbort();
        return;
      }

      const timer = timers.setTimeout(() => {
        timeoutTimer = undefined;
        terminate("timeout");
      }, config.executionTimeoutMs);

      if (settled || terminationCategory !== undefined) {
        clearTimer(timer);
        return;
      }

      timeoutTimer = timer;
    } catch {
      if (exitListenerInstalled) {
        terminate("spawn");
      } else {
        settleFailure("spawn");
      }
    }
  });
}

function prepareCommandInvocation(submission: Submission): CommandPreparation {
  try {
    const { metadata } = submission;

    if (submission.kind === "start") {
      return {
        kind: "ready",
        invocation: {
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
          stdin: undefined,
        },
      };
    }

    if (submission.kind === "end") {
      return {
        kind: "ready",
        invocation: {
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
          stdin: undefined,
        },
      };
    }

    const nativeEvent = nativeEventFromHookType(submission.hookType);

    if (nativeEvent === undefined) {
      return { kind: "normalization" };
    }

    const stdin = JSON.stringify({
      source: "pi",
      version: "1.0.0",
      event: nativeEvent,
      payload: submission.data,
    });

    if (stdin === undefined) {
      return { kind: "serialization" };
    }

    return {
      kind: "ready",
      invocation: {
        arguments_: [
          "observation",
          "--hook-type",
          submission.hookType,
          "--project-name",
          metadata.projectName,
          "--current-working-directory",
          metadata.currentWorkingDirectory,
          "--timestamp",
          metadata.timestamp,
          "--session-id",
          metadata.sessionId,
        ],
        stdin,
      },
    };
  } catch {
    return { kind: "serialization" };
  }
}

function createDiagnosticIdentity(submission: Submission): DiagnosticIdentity {
  try {
    const sessionId = boundedDiagnosticIdentity(submission.metadata.sessionId);
    const candidateNativeEvent =
      submission.kind === "observation" && typeof submission.hookType === "string"
        ? nativeEventFromHookType(submission.hookType)
        : undefined;
    const nativeEvent = boundedDiagnosticIdentity(candidateNativeEvent);

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

function dispatchFailure(
  category: DispatchFailureCategory,
  identity: DiagnosticIdentity,
): DispatchResult {
  const diagnostic = Object.freeze({
    category,
    ...(identity.nativeEvent === undefined ? {} : { nativeEvent: identity.nativeEvent }),
    ...(identity.sessionId === undefined ? {} : { sessionId: identity.sessionId }),
  });

  try {
    console.error(diagnostic);
  } catch {}

  return { ok: false, category };
}

function nativeEventFromHookType(hookType: string): string | undefined {
  const nativeEvent = hookType.startsWith("pi.") ? hookType.slice("pi.".length) : undefined;

  if (nativeEvent === undefined || !/^[a-z][a-z0-9_]*$/.test(nativeEvent)) {
    return undefined;
  }

  return nativeEvent;
}
