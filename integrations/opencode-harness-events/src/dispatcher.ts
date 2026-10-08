// biome-ignore-all lint/correctness/useQwikValidLexicalScope: The process monitor intentionally closes over one owned child lifecycle.
// biome-ignore lint/style/noExcessiveLinesPerFile: The dispatcher keeps invocation and owned-child lifecycle containment in one boundary.
// biome-ignore lint/correctness/noNodejsModules: The dispatcher uses the runtime-provided child-process API required by the process boundary.
import { spawn as nodeSpawn } from "node:child_process";
// biome-ignore lint/correctness/noNodejsModules: The dispatcher reads the host environment supplied by the runtime.
import process from "node:process";

// biome-ignore lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
import type { PluginConfig } from "./config";
// biome-ignore lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
import type { Diagnostic, DispatchResult, Submission } from "./model";

type ChildSignal = "SIGTERM" | "SIGKILL";
type OutputChunk = string | Uint8Array;

const TERMINATION_GRACE_MS = 100;
const MAX_DIAGNOSTIC_LENGTH = 512;
const MAX_IDENTITY_LENGTH = 128;
const BYTES_PER_KIBIBYTE = 1024;
const MAX_STDERR_KIBIBYTES = 8;
const MAX_STDERR_BYTES = MAX_STDERR_KIBIBYTES * BYTES_PER_KIBIBYTE;
const IDENTITY_TRUNCATION_SUFFIX = "...";

interface SpawnOptions {
  readonly shell: false;
  readonly env: NodeJS.ProcessEnv;
  readonly stdio: ["pipe", "pipe", "pipe"];
}

interface ChildStdin {
  readonly write: (value: string) => boolean;
  readonly end: () => void;
  readonly onError?: (listener: (error: Error) => void) => void;
}

interface ChildOutput {
  readonly onData?: (listener: (chunk: OutputChunk) => void) => void;
  readonly resume: () => void;
}

interface ChildProcess {
  readonly stdin: ChildStdin;
  readonly stdout: ChildOutput;
  readonly stderr: ChildOutput;
  readonly kill: (signal: ChildSignal) => boolean;
  readonly onError: (listener: (error: Error) => void) => void;
  readonly onClose: (
    listener: (exitCode: number | null, signalCode: string | null) => void,
  ) => void;
}

type SpawnProcess = (
  command: string,
  args: readonly string[],
  options: SpawnOptions,
) => ChildProcess;

type DiagnosticReporter = (diagnostic: Diagnostic) => void;

type ObservationSubmission = Extract<
  Submission,
  { readonly kind: "observation" }
>;

const EXIT_SUCCESS = 0;
const EXIT_USAGE = 2;
const EXIT_CONNECTION = 3;
const EXIT_INVOCATION = 4;

interface Invocation {
  readonly args: string[];
  readonly stdin: string | undefined;
}

function createChildProcess(
  command: string,
  args: readonly string[],
  options: SpawnOptions,
): ChildProcess {
  const child = nodeSpawn(command, args, {
    env: options.env,
    shell: options.shell,
    stdio: options.stdio,
  });

  return {
    stdin: {
      write(value) {
        return child.stdin.write(value);
      },
      end() {
        child.stdin.end();
      },
      onError(listener) {
        child.stdin.on("error", listener);
      },
    },
    stdout: {
      onData(listener) {
        child.stdout.on("data", listener);
      },
      resume() {
        child.stdout.resume();
      },
    },
    stderr: {
      onData(listener) {
        child.stderr.on("data", listener);
      },
      resume() {
        child.stderr.resume();
      },
    },
    kill(signal) {
      return child.kill(signal);
    },
    onError(listener) {
      child.on("error", listener);
    },
    onClose(listener) {
      child.once("close", listener);
    },
  };
}

function lifecycleArgs(
  command: "session-start" | "session-end",
  submission: Extract<Submission, { readonly kind: "start" | "end" }>,
): string[] {
  const { metadata } = submission;
  return [
    command,
    "--session-id",
    metadata.sessionId,
    "--project-name",
    metadata.projectName,
    "--current-working-directory",
    metadata.currentWorkingDirectory,
    "--timestamp",
    metadata.timestamp,
  ];
}

function nativeKind(observation: ObservationSubmission): string {
  const prefix = `opencode.${observation.metadata.generation}.`;
  if (observation.hookType.startsWith(prefix)) {
    return observation.hookType.slice(prefix.length);
  }

  return observation.hookType;
}

function diagnosticFor(
  submission: Submission,
  category: Diagnostic["category"],
): Diagnostic {
  let eventKind: string = submission.kind;
  if (submission.kind === "observation") {
    eventKind = boundIdentity(nativeKind(submission));
  }

  return {
    category,
    generation: submission.metadata.generation,
    nativeKind: eventKind,
    sessionId: boundIdentity(submission.metadata.sessionId),
  };
}

function boundIdentity(value: string): string {
  if (value.length <= MAX_IDENTITY_LENGTH) {
    return value;
  }

  return `${value.slice(
    0,
    MAX_IDENTITY_LENGTH - IDENTITY_TRUNCATION_SUFFIX.length,
  )}${IDENTITY_TRUNCATION_SUFFIX}`;
}

function reportFailure(
  submission: Submission,
  category: Diagnostic["category"],
  report: DiagnosticReporter,
): void {
  try {
    report(diagnosticFor(submission, category));
  } catch {
    // Diagnostics must not escape into the host operation.
  }
}

function serializeObservation(
  observation: ObservationSubmission,
): string | undefined {
  try {
    const serialized = JSON.stringify({
      source: "opencode",
      generation: observation.metadata.generation,
      kind: nativeKind(observation),
      payload: observation.data,
    });

    return serialized;
  } catch {
    return undefined;
  }
}

function observationArgs(observation: ObservationSubmission): string[] {
  const { metadata } = observation;
  return [
    "observation",
    "--hook-type",
    observation.hookType,
    "--project-name",
    metadata.projectName,
    "--current-working-directory",
    metadata.currentWorkingDirectory,
    "--timestamp",
    metadata.timestamp,
    "--session-id",
    metadata.sessionId,
  ];
}

function createInvocation(submission: Submission): Invocation | DispatchResult {
  switch (submission.kind) {
    case "start":
      return {
        args: lifecycleArgs("session-start", submission),
        stdin: undefined,
      };
    case "end":
      return {
        args: lifecycleArgs("session-end", submission),
        stdin: undefined,
      };
    case "observation": {
      const serialized = serializeObservation(submission);
      if (serialized === undefined) {
        return { ok: false, category: "serialization" };
      }

      return {
        args: observationArgs(submission),
        stdin: serialized,
      };
    }
    default:
      return { ok: false, category: "configuration" };
  }
}

function mapExit(
  exitCode: number | null,
  signalCode: string | null,
): DispatchResult {
  if (signalCode !== null) {
    return { ok: false, category: "signal" };
  }

  if (exitCode === EXIT_INVOCATION) {
    return { ok: false, category: "invocation" };
  }

  switch (exitCode) {
    case EXIT_SUCCESS:
      return { ok: true };
    case EXIT_USAGE:
      return { ok: false, category: "usage" };
    case EXIT_CONNECTION:
      return { ok: false, category: "connection" };
    default:
      return { ok: false, category: "invocation" };
  }
}

function outputByteLength(chunk: OutputChunk): number {
  if (typeof chunk === "string") {
    return new TextEncoder().encode(chunk).byteLength;
  }

  return chunk.byteLength;
}

function createStderrDrain(): (chunk: OutputChunk) => void {
  let consumedBytes = 0;

  return (chunk) => {
    if (consumedBytes >= MAX_STDERR_BYTES) {
      return;
    }

    consumedBytes = Math.min(
      MAX_STDERR_BYTES,
      consumedBytes + outputByteLength(chunk),
    );
  };
}

function stoppedCategory(
  reason: "timeout" | "signal",
  controlFailure: boolean,
): "timeout" | "signal" {
  if (controlFailure) {
    return "signal";
  }

  return reason;
}

// biome-ignore lint/complexity/noExcessiveLinesPerFunction: The dispatcher owns one coupled child lifecycle state machine.
function waitForExit(
  child: ChildProcess,
  config: PluginConfig,
  signal: AbortSignal,
  stdin: string | undefined,
): Promise<DispatchResult> {
  // biome-ignore lint/complexity/noExcessiveLinesPerFunction: The dispatcher owns one coupled child lifecycle state machine.
  return new Promise((resolve) => {
    let settled = false;
    let stopReason: "timeout" | "signal" | undefined;
    let failureCategory: "spawn" | undefined;
    let controlFailure = false;
    let cleanupRequested = false;
    let killRequested = false;
    let closeObserved = false;
    let closeResult: DispatchResult | undefined;
    let deadlineTimer: ReturnType<typeof setTimeout> | undefined;
    let terminationTimer: ReturnType<typeof setTimeout> | undefined;
    let finalCleanupTimer: ReturnType<typeof setTimeout> | undefined;
    let closeSettleTimer: ReturnType<typeof setTimeout> | undefined;

    const clearTimers = (): void => {
      if (deadlineTimer !== undefined) {
        clearTimeout(deadlineTimer);
      }
      if (terminationTimer !== undefined) {
        clearTimeout(terminationTimer);
      }
      if (finalCleanupTimer !== undefined) {
        clearTimeout(finalCleanupTimer);
      }
      if (closeSettleTimer !== undefined) {
        clearTimeout(closeSettleTimer);
      }
    };

    const finish = (result: DispatchResult): void => {
      if (settled) {
        return;
      }

      settled = true;
      clearTimers();
      signal.removeEventListener("abort", onAbort);
      resolve(result);
    };

    const resultAfterClose = (): DispatchResult => {
      if (stopReason !== undefined) {
        return {
          ok: false,
          category: stoppedCategory(stopReason, controlFailure),
        };
      }

      if (failureCategory !== undefined) {
        return { ok: false, category: failureCategory };
      }

      if (closeResult !== undefined) {
        return closeResult;
      }

      return { ok: false, category: "spawn" };
    };

    const settleAfterClose = (): void => {
      if (settled || !closeObserved || closeSettleTimer !== undefined) {
        return;
      }

      closeSettleTimer = setTimeout(() => {
        closeSettleTimer = undefined;
        finish(resultAfterClose());
      }, 0);
    };

    const sendSignal = (childSignal: ChildSignal): boolean => {
      try {
        return child.kill(childSignal);
      } catch {
        return false;
      }
    };

    const escalate = (): void => {
      terminationTimer = undefined;
      if (settled || !cleanupRequested || killRequested) {
        return;
      }

      killRequested = true;
      if (!sendSignal("SIGKILL")) {
        controlFailure = true;
      }

      if (closeObserved) {
        finish(resultAfterClose());
        return;
      }

      finalCleanupTimer = setTimeout(() => {
        finalCleanupTimer = undefined;
        finish(resultAfterClose());
      }, TERMINATION_GRACE_MS);
    };

    const requestCleanup = (): void => {
      if (settled || cleanupRequested) {
        return;
      }

      cleanupRequested = true;
      if (closeObserved) {
        settleAfterClose();
        return;
      }

      if (!sendSignal("SIGTERM")) {
        controlFailure = true;
      }

      if (!(settled || closeObserved)) {
        terminationTimer = setTimeout(escalate, TERMINATION_GRACE_MS);
      }
    };

    const stop = (reason: "timeout" | "signal"): void => {
      if (settled || closeObserved) {
        return;
      }

      if (stopReason === undefined) {
        stopReason = reason;
      }
      requestCleanup();
    };

    const onAbort = (): void => {
      stop("signal");
    };

    const onChildError = (): void => {
      if (settled) {
        return;
      }

      if (cleanupRequested) {
        controlFailure = true;
        return;
      }

      failureCategory = "spawn";
      requestCleanup();
    };

    const onStdinError = (): void => {
      if (settled) {
        return;
      }

      if (cleanupRequested) {
        controlFailure = true;
        return;
      }

      failureCategory = "spawn";
      requestCleanup();
    };

    try {
      child.onError(onChildError);
      child.onClose((exitCode, signalCode) => {
        closeObserved = true;
        closeResult = mapExit(exitCode, signalCode);
        if (cleanupRequested || stopReason !== undefined) {
          finish(resultAfterClose());
          return;
        }

        settleAfterClose();
      });
      child.stdin.onError?.(onStdinError);

      child.stdout.onData?.(() => undefined);
      child.stderr.onData?.(createStderrDrain());
      child.stdout.resume();
      child.stderr.resume();

      if (signal.aborted) {
        stop("signal");
      } else {
        signal.addEventListener("abort", onAbort, { once: true });
        deadlineTimer = setTimeout(
          () => stop("timeout"),
          config.executionTimeoutMs,
        );
      }

      if (!settled && stopReason === undefined) {
        try {
          if (stdin !== undefined) {
            child.stdin.write(stdin);
          }
          child.stdin.end();
        } catch {
          onStdinError();
        }
      }
    } catch {
      failureCategory = "spawn";
      requestCleanup();
    }
  });
}

// biome-ignore lint/complexity/useMaxParams: The final two parameters are injected process and diagnostic seams.
export async function dispatch(
  config: PluginConfig,
  submission: Submission,
  signal: AbortSignal,
  spawnProcess: SpawnProcess = createChildProcess,
  report: DiagnosticReporter = reportDiagnostic,
): Promise<DispatchResult> {
  const invocation = createInvocation(submission);
  if ("ok" in invocation) {
    if (!invocation.ok) {
      reportFailure(submission, invocation.category, report);
    }
    return invocation;
  }

  if (signal.aborted) {
    const result: DispatchResult = { ok: false, category: "signal" };
    reportFailure(submission, result.category, report);
    return result;
  }

  const options: SpawnOptions = {
    // biome-ignore lint/style/noProcessEnv: The child must inherit the host environment unchanged.
    env: process.env,
    shell: false,
    stdio: ["pipe", "pipe", "pipe"],
  };

  let child: ChildProcess;
  try {
    child = spawnProcess(config.executable, invocation.args, options);
  } catch {
    reportFailure(submission, "spawn", report);
    return { ok: false, category: "spawn" };
  }

  const outcome = await waitForExit(child, config, signal, invocation.stdin);
  if (!outcome.ok) {
    reportFailure(submission, outcome.category, report);
  }
  return outcome;
}

export type {
  ChildSignal,
  ChildOutput,
  ChildProcess,
  ChildStdin,
  DiagnosticReporter,
  OutputChunk,
  SpawnOptions,
  SpawnProcess,
};

export function reportDiagnostic(diagnostic: Diagnostic): void {
  const fields = [`category=${diagnostic.category}`];
  if (diagnostic.generation !== undefined) {
    fields.push(`generation=${diagnostic.generation}`);
  }
  if (diagnostic.nativeKind !== undefined) {
    fields.push(`nativeKind=${diagnostic.nativeKind}`);
  }
  if (diagnostic.sessionId !== undefined) {
    fields.push(`sessionId=${diagnostic.sessionId}`);
  }
  const message = fields.join(" ");
  // biome-ignore lint/suspicious/noConsole: The default sink emits one bounded payload-free diagnostic.
  console.error(message.slice(0, MAX_DIAGNOSTIC_LENGTH));
}
