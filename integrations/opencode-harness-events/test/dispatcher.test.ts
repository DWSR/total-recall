// biome-ignore-all lint/correctness/useQwikValidLexicalScope: Bun tests intentionally use closures for injected process seams.
// biome-ignore-all lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
// biome-ignore-all lint/style/noExcessiveLinesPerFile: Dispatcher failure cases share one injected-process fixture.
// biome-ignore-all lint/performance/noAwaitInLoops: Each exit category is asserted independently for diagnostic isolation.

import { expect, test } from "bun:test";
import {
  dispatch,
  type ChildProcess,
  type ChildSignal,
  type DiagnosticReporter,
  type OutputChunk,
  type SpawnOptions,
  type SpawnProcess,
} from "../src/dispatcher";
import type { PluginConfig } from "../src/config";
import type { Diagnostic, JsonObject, Submission } from "../src/model";
// biome-ignore lint/correctness/noNodejsModules: The test verifies that the exact host environment object is passed to the child seam.
import process from "node:process";

const config = {
  executable: "/tmp/harness-events",
  executionTimeoutMs: 35_000,
  shutdownTimeoutMs: 5000,
} satisfies PluginConfig;

const NESTED_NUMBER = 3;
const EXIT_SUCCESS = 0;
const EXIT_USAGE = 2;
const EXIT_CONNECTION = 3;
const EXIT_INVOCATION = 4;
const LONG_IDENTITY_LENGTH = 300;
const MAX_DIAGNOSTIC_LENGTH = 512;
const UNCOOPERATIVE_EXIT_DELAY_MS = 250;
const DELAYED_KILL_CLOSE_MS = 25;
const LARGE_STDIN_REPEAT_COUNT = 500_000;

const metadata = {
  generation: "v2",
  sessionId: "session-123",
  projectName: "project",
  currentWorkingDirectory: "/tmp/project",
  timestamp: "2026-09-20T12:34:56.789Z",
} satisfies Submission["metadata"];

interface SpawnRecord {
  readonly command: string;
  readonly args: readonly string[];
  readonly options: SpawnOptions;
  readonly stdin: string[];
  stdinClosed: boolean;
  stdoutResumed: boolean;
  stderrResumed: boolean;
  stdoutDataAttached: boolean;
  stderrDataAttached: boolean;
  killSignals: ChildSignal[];
}

interface RecordingSpawn {
  readonly records: SpawnRecord[];
  readonly spawn: SpawnProcess;
}

function createRecordingSpawn(
  exitCode: number,
  signalCode: string | null = null,
): RecordingSpawn {
  const records: SpawnRecord[] = [];

  const spawn: SpawnProcess = (command, args, options): ChildProcess => {
    const record: SpawnRecord = {
      command,
      args: [...args],
      options,
      stdin: [],
      stdinClosed: false,
      stdoutResumed: false,
      stderrResumed: false,
      stdoutDataAttached: false,
      stderrDataAttached: false,
      killSignals: [],
    };
    records.push(record);

    let closeListener:
      | ((childExitCode: number | null, childSignalCode: string | null) => void)
      | undefined;

    return {
      stdin: {
        write(value) {
          record.stdin.push(value);
          return true;
        },
        end() {
          record.stdinClosed = true;
          queueMicrotask(() => closeListener?.(exitCode, signalCode));
        },
      },
      stdout: {
        onData(_listener: (chunk: OutputChunk) => void) {
          record.stdoutDataAttached = true;
        },
        resume() {
          record.stdoutResumed = true;
        },
      },
      stderr: {
        onData(_listener: (chunk: OutputChunk) => void) {
          record.stderrDataAttached = true;
        },
        resume() {
          record.stderrResumed = true;
        },
      },
      kill(childSignal) {
        record.killSignals.push(childSignal);
        return true;
      },
      onError: () => undefined,
      onClose(listener) {
        closeListener = listener;
      },
    };
  };

  return { records, spawn };
}

interface ControlledSpawn {
  readonly records: SpawnRecord[];
  readonly spawn: SpawnProcess;
}

// biome-ignore lint/complexity/noExcessiveLinesPerFunction: The fixture models all owned child lifecycle callbacks in one seam.
// biome-ignore lint/complexity/useMaxParams: The fixture arguments mirror independent process lifecycle controls.
function createControlledSpawn(
  naturalExitDelayMs: number,
  output: readonly OutputChunk[] = [],
  killBehavior:
    | "success"
    | "false"
    | "throw"
    | "async-error"
    | "async-error-no-close"
    | "delayed-close" = "success",
  delayedKillCloseMs = 0,
  stdinBehavior: "success" | "async-error" = "success",
): ControlledSpawn {
  const records: SpawnRecord[] = [];

  // biome-ignore lint/complexity/noExcessiveLinesPerFunction: The fixture keeps process and stream callbacks together.
  const spawn: SpawnProcess = (command, args, options): ChildProcess => {
    const record: SpawnRecord = {
      command,
      args: [...args],
      options,
      stdin: [],
      stdinClosed: false,
      stdoutResumed: false,
      stderrResumed: false,
      stdoutDataAttached: false,
      stderrDataAttached: false,
      killSignals: [],
    };
    records.push(record);

    let closeListener:
      | ((childExitCode: number | null, childSignalCode: string | null) => void)
      | undefined;
    let childErrorListener: ((error: Error) => void) | undefined;
    let stdinErrorListener: ((error: Error) => void) | undefined;
    let stdoutListener: ((chunk: OutputChunk) => void) | undefined;
    let stderrListener: ((chunk: OutputChunk) => void) | undefined;
    let closed = false;

    const close = (): void => {
      if (closed) {
        return;
      }

      closed = true;
      closeListener?.(0, null);
    };

    return {
      stdin: {
        write(value) {
          record.stdin.push(value);
          return true;
        },
        end() {
          record.stdinClosed = true;
          if (stdinBehavior === "async-error") {
            queueMicrotask(() =>
              stdinErrorListener?.(new Error("stdin EPIPE sentinel")),
            );
          }
          queueMicrotask(() => {
            for (const chunk of output) {
              stdoutListener?.(chunk);
              stderrListener?.(chunk);
            }
          });
          setTimeout(close, naturalExitDelayMs);
        },
        onError(listener) {
          stdinErrorListener = listener;
        },
      },
      stdout: {
        onData(listener) {
          stdoutListener = listener;
          record.stdoutDataAttached = true;
        },
        resume() {
          record.stdoutResumed = true;
        },
      },
      stderr: {
        onData(listener) {
          stderrListener = listener;
          record.stderrDataAttached = true;
        },
        resume() {
          record.stderrResumed = true;
        },
      },
      // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: The fixture enumerates the required kill failure modes.
      kill(childSignal) {
        record.killSignals.push(childSignal);
        if (killBehavior === "throw") {
          throw new Error("child signal failure must not escape");
        }
        if (killBehavior === "false") {
          return false;
        }
        if (
          (killBehavior === "async-error" ||
            killBehavior === "async-error-no-close") &&
          (childSignal === "SIGTERM" || childSignal === "SIGKILL")
        ) {
          queueMicrotask(() =>
            childErrorListener?.(new Error("async child signal failure")),
          );
        }
        if (childSignal === "SIGKILL") {
          if (killBehavior === "async-error-no-close") {
            return true;
          }
          if (killBehavior === "delayed-close") {
            setTimeout(close, delayedKillCloseMs);
          } else {
            queueMicrotask(close);
          }
        }
        return true;
      },
      onError(listener) {
        childErrorListener = listener;
      },
      onClose(listener) {
        closeListener = listener;
      },
    };
  };

  return { records, spawn };
}

function captureDiagnostics(): {
  readonly diagnostics: Diagnostic[];
  readonly report: DiagnosticReporter;
} {
  const diagnostics: Diagnostic[] = [];
  return {
    diagnostics,
    report(diagnostic) {
      diagnostics.push(diagnostic);
    },
  };
}

function signal(): AbortSignal {
  return new AbortController().signal;
}

test("dispatches session-start with exact arguments, inherited environment, and closed stdin", async () => {
  const recording = createRecordingSpawn(0);
  const submission = { kind: "start", metadata } satisfies Submission;

  const result = await dispatch(config, submission, signal(), recording.spawn);

  expect(result).toEqual({ ok: true });
  expect(recording.records).toHaveLength(1);
  const [record] = recording.records;
  expect(record).toBeDefined();
  if (record === undefined) {
    return;
  }

  expect(record.command).toBe(config.executable);
  expect(record.args).toEqual([
    "session-start",
    "--session-id",
    metadata.sessionId,
    "--project-name",
    metadata.projectName,
    "--current-working-directory",
    metadata.currentWorkingDirectory,
    "--timestamp",
    metadata.timestamp,
  ]);
  expect(record.options.shell).toBe(false);
  // biome-ignore lint/style/noProcessEnv: The test verifies unchanged environment inheritance.
  expect(record.options.env).toBe(process.env);
  expect(record.options.stdio).toEqual(["pipe", "pipe", "pipe"]);
  expect(record.stdin).toEqual([]);
  expect(record.stdinClosed).toBe(true);
  expect(record.stdoutResumed).toBe(true);
  expect(record.stderrResumed).toBe(true);
});

test("dispatches an observation with one opaque JSON envelope on stdin", async () => {
  const recording = createRecordingSpawn(0);
  const data = {
    prompt: "opaque-prompt-content",
    nested: { enabled: true, values: [null, NESTED_NUMBER] },
  } satisfies JsonObject;
  const submission = {
    kind: "observation",
    metadata,
    hookType: "opencode.v2.session.text.ended",
    data,
  } satisfies Submission;

  const result = await dispatch(config, submission, signal(), recording.spawn);

  expect(result).toEqual({ ok: true });
  expect(recording.records).toHaveLength(1);
  const [record] = recording.records;
  expect(record).toBeDefined();
  if (record === undefined) {
    return;
  }

  expect(record.command).toBe(config.executable);
  expect(record.args).toEqual([
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
  ]);
  expect(record.args).not.toContain(data.prompt);
  expect(record.stdin).toEqual([
    JSON.stringify({
      source: "opencode",
      generation: metadata.generation,
      kind: "session.text.ended",
      payload: data,
    }),
  ]);
  expect(record.stdinClosed).toBe(true);
  expect(record.stdoutResumed).toBe(true);
  expect(record.stderrResumed).toBe(true);
});

test("dispatches session-end with the lifecycle argument contract", async () => {
  const recording = createRecordingSpawn(0);
  const submission = { kind: "end", metadata } satisfies Submission;

  const result = await dispatch(config, submission, signal(), recording.spawn);

  expect(result).toEqual({ ok: true });
  expect(recording.records[0]?.args).toEqual([
    "session-end",
    "--session-id",
    metadata.sessionId,
    "--project-name",
    metadata.projectName,
    "--current-working-directory",
    metadata.currentWorkingDirectory,
    "--timestamp",
    metadata.timestamp,
  ]);
  expect(recording.records[0]?.stdin).toEqual([]);
  expect(recording.records[0]?.stdinClosed).toBe(true);
});

test("maps authoritative CLI exit codes to typed outcomes", async () => {
  const expected = [
    [EXIT_SUCCESS, { ok: true }],
    [EXIT_USAGE, { ok: false, category: "usage" }],
    [EXIT_CONNECTION, { ok: false, category: "connection" }],
    [EXIT_INVOCATION, { ok: false, category: "invocation" }],
  ] as const;
  const submission = { kind: "start", metadata } satisfies Submission;

  await Promise.all(
    expected.map(async ([exitCode, result]) => {
      const recording = createRecordingSpawn(exitCode);
      const diagnostics = captureDiagnostics();
      await expect(
        dispatch(
          config,
          submission,
          signal(),
          recording.spawn,
          diagnostics.report,
        ),
      ).resolves.toEqual(result);
      expect(recording.records).toHaveLength(1);
    }),
  );
});

test("returns a serialization failure without spawning for cyclic observation data", async () => {
  const recording = createRecordingSpawn(0);
  const diagnostics = captureDiagnostics();
  const data: JsonObject = {};
  Object.defineProperty(data, "cycle", {
    enumerable: true,
    value: data,
  });
  const submission = {
    kind: "observation",
    metadata,
    hookType: "opencode.v2.session.error",
    data,
  } satisfies Submission;

  await expect(
    dispatch(config, submission, signal(), recording.spawn, diagnostics.report),
  ).resolves.toEqual({ ok: false, category: "serialization" });
  expect(recording.records).toHaveLength(0);
  expect(diagnostics.diagnostics).toEqual([
    {
      category: "serialization",
      generation: metadata.generation,
      nativeKind: "session.error",
      sessionId: metadata.sessionId,
    },
  ]);
});

test("contains a synchronous process-spawn failure", async () => {
  const diagnostics = captureDiagnostics();
  const spawnProcess: SpawnProcess = () => {
    throw new Error("spawn failure must not escape");
  };
  const submission = { kind: "start", metadata } satisfies Submission;

  await expect(
    dispatch(config, submission, signal(), spawnProcess, diagnostics.report),
  ).resolves.toEqual({ ok: false, category: "spawn" });
  expect(diagnostics.diagnostics).toEqual([
    {
      category: "spawn",
      generation: metadata.generation,
      nativeKind: "start",
      sessionId: metadata.sessionId,
    },
  ]);
});

test("emits one bounded default diagnostic with no path or serialized input", async () => {
  const messages: string[] = [];
  // biome-ignore lint/suspicious/noConsole: The test captures the default diagnostic sink.
  const originalError = console.error;
  console.error = (message?: unknown): void => {
    if (typeof message === "string") {
      messages.push(message);
    }
  };

  const spawnProcess: SpawnProcess = () => {
    throw new Error("child-error-sentinel");
  };
  const submission = {
    kind: "observation",
    metadata: {
      ...metadata,
      sessionId: "session-".concat("s".repeat(LONG_IDENTITY_LENGTH)),
    },
    hookType: `opencode.v2.${"event".repeat(LONG_IDENTITY_LENGTH)}`,
    data: { prompt: "serialized-input-sentinel" },
  } satisfies Submission;

  try {
    await expect(
      dispatch(config, submission, signal(), spawnProcess),
    ).resolves.toEqual({ ok: false, category: "spawn" });
  } finally {
    console.error = originalError;
  }

  expect(messages).toHaveLength(1);
  expect(messages[0]?.length).toBeLessThanOrEqual(MAX_DIAGNOSTIC_LENGTH);
  expect(messages[0]).toContain("category=spawn");
  expect(messages[0]).not.toContain("/tmp/project");
  expect(messages[0]).not.toContain("serialized-input-sentinel");
  expect(messages[0]).not.toContain("child-error-sentinel");
});

test("maps CLI failures to one payload-free diagnostic without retrying", async () => {
  const expected = [
    [EXIT_USAGE, "usage"],
    [EXIT_CONNECTION, "connection"],
    [EXIT_INVOCATION, "invocation"],
  ] as const;
  const submission = {
    kind: "observation",
    metadata,
    hookType: "opencode.v2.session.error",
    data: { error: "remote-error-sentinel" },
  } satisfies Submission;

  for (const [exitCode, category] of expected) {
    const recording = createRecordingSpawn(exitCode);
    const diagnostics = captureDiagnostics();

    await expect(
      dispatch(
        config,
        submission,
        signal(),
        recording.spawn,
        diagnostics.report,
      ),
    ).resolves.toEqual({ ok: false, category });
    expect(recording.records).toHaveLength(1);
    expect(JSON.stringify(diagnostics.diagnostics)).not.toContain(
      "remote-error-sentinel",
    );
    expect(diagnostics.diagnostics).toEqual([
      {
        category,
        generation: metadata.generation,
        nativeKind: "session.error",
        sessionId: metadata.sessionId,
      },
    ]);
  }
});

test("times out one child, escalates TERM to KILL, drains output, and returns", async () => {
  const controlled = createControlledSpawn(UNCOOPERATIVE_EXIT_DELAY_MS, [
    "stdout-output-sentinel",
    "stderr-output-sentinel",
  ]);
  const diagnostics = captureDiagnostics();
  const submission = {
    kind: "observation",
    metadata,
    hookType: "opencode.v2.session.text.ended",
    data: { prompt: "payload-sentinel" },
  } satisfies Submission;

  await expect(
    dispatch(
      { ...config, executionTimeoutMs: 5 },
      submission,
      signal(),
      controlled.spawn,
      diagnostics.report,
    ),
  ).resolves.toEqual({ ok: false, category: "timeout" });

  expect(controlled.records).toHaveLength(1);
  const [record] = controlled.records;
  expect(record?.killSignals).toEqual(["SIGTERM", "SIGKILL"]);
  expect(record?.stdoutDataAttached).toBe(true);
  expect(record?.stderrDataAttached).toBe(true);
  expect(record?.stdoutResumed).toBe(true);
  expect(record?.stderrResumed).toBe(true);
  expect(JSON.stringify(diagnostics.diagnostics)).not.toContain(
    "stdout-output-sentinel",
  );
  expect(JSON.stringify(diagnostics.diagnostics)).not.toContain(
    "stderr-output-sentinel",
  );
  expect(JSON.stringify(diagnostics.diagnostics)).not.toContain(
    "payload-sentinel",
  );
  expect(diagnostics.diagnostics).toEqual([
    {
      category: "timeout",
      generation: metadata.generation,
      nativeKind: "session.text.ended",
      sessionId: metadata.sessionId,
    },
  ]);
});

test("aborts one owned child, escalates cleanup, and does not invoke twice", async () => {
  const controlled = createControlledSpawn(UNCOOPERATIVE_EXIT_DELAY_MS);
  const diagnostics = captureDiagnostics();
  const controller = new AbortController();
  const submission = { kind: "end", metadata } satisfies Submission;

  const dispatchResult = dispatch(
    { ...config, executionTimeoutMs: 1000 },
    submission,
    controller.signal,
    controlled.spawn,
    diagnostics.report,
  );
  setTimeout(() => controller.abort(), 1);

  await expect(dispatchResult).resolves.toEqual({
    ok: false,
    category: "signal",
  });
  expect(controlled.records).toHaveLength(1);
  expect(controlled.records[0]?.killSignals).toEqual(["SIGTERM", "SIGKILL"]);
  expect(diagnostics.diagnostics).toEqual([
    {
      category: "signal",
      generation: metadata.generation,
      nativeKind: "end",
      sessionId: metadata.sessionId,
    },
  ]);
});

test("classifies a child signal separately from external cancellation", async () => {
  const recording = createRecordingSpawn(0, "SIGTERM");
  const diagnostics = captureDiagnostics();
  const submission = { kind: "start", metadata } satisfies Submission;

  await expect(
    dispatch(config, submission, signal(), recording.spawn, diagnostics.report),
  ).resolves.toEqual({ ok: false, category: "signal" });
  expect(recording.records).toHaveLength(1);
  expect(recording.records[0]?.killSignals).toEqual([]);
  expect(diagnostics.diagnostics).toEqual([
    {
      category: "signal",
      generation: metadata.generation,
      nativeKind: "start",
      sessionId: metadata.sessionId,
    },
  ]);
});

test("contains termination and kill failures as a signal outcome", async () => {
  const controlled = createControlledSpawn(
    UNCOOPERATIVE_EXIT_DELAY_MS,
    [],
    "throw",
  );
  const diagnostics = captureDiagnostics();
  const submission = { kind: "start", metadata } satisfies Submission;

  await expect(
    dispatch(
      { ...config, executionTimeoutMs: 5 },
      submission,
      signal(),
      controlled.spawn,
      diagnostics.report,
    ),
  ).resolves.toEqual({ ok: false, category: "signal" });
  expect(controlled.records).toHaveLength(1);
  expect(controlled.records[0]?.killSignals).toEqual(["SIGTERM", "SIGKILL"]);
  expect(diagnostics.diagnostics).toEqual([
    {
      category: "signal",
      generation: metadata.generation,
      nativeKind: "start",
      sessionId: metadata.sessionId,
    },
  ]);
});

test("contains an asynchronous stdin EPIPE as a spawn failure without payload leakage", async () => {
  const diagnostics = captureDiagnostics();
  const controlled = createControlledSpawn(1, [], "success", 0, "async-error");
  const submission = {
    kind: "observation",
    metadata,
    hookType: "opencode.v2.session.text.ended",
    data: {
      payload: "stdin-epipe-sentinel".repeat(LARGE_STDIN_REPEAT_COUNT),
    },
  } satisfies Submission;

  await expect(
    dispatch(
      { ...config, executionTimeoutMs: 1000 },
      submission,
      signal(),
      controlled.spawn,
      diagnostics.report,
    ),
  ).resolves.toEqual({ ok: false, category: "spawn" });

  expect(JSON.stringify(diagnostics.diagnostics)).not.toContain(
    "stdin-epipe-sentinel",
  );
  expect(diagnostics.diagnostics).toEqual([
    {
      category: "spawn",
      generation: metadata.generation,
      nativeKind: "session.text.ended",
      sessionId: metadata.sessionId,
    },
  ]);
});

test("keeps TERM cleanup alive after an asynchronous child error and reaches KILL", async () => {
  const controlled = createControlledSpawn(
    UNCOOPERATIVE_EXIT_DELAY_MS,
    [],
    "async-error",
  );
  const diagnostics = captureDiagnostics();
  const submission = { kind: "start", metadata } satisfies Submission;

  await expect(
    dispatch(
      { ...config, executionTimeoutMs: 5 },
      submission,
      signal(),
      controlled.spawn,
      diagnostics.report,
    ),
  ).resolves.toEqual({ ok: false, category: "signal" });

  expect(controlled.records[0]?.killSignals).toEqual(["SIGTERM", "SIGKILL"]);
});

test("waits for delayed close after KILL before resolving timeout cleanup", async () => {
  const controlled = createControlledSpawn(
    UNCOOPERATIVE_EXIT_DELAY_MS,
    [],
    "delayed-close",
    DELAYED_KILL_CLOSE_MS,
  );
  const submission = { kind: "start", metadata } satisfies Submission;
  let settled = false;
  const resultPromise = dispatch(
    { ...config, executionTimeoutMs: 5 },
    submission,
    signal(),
    controlled.spawn,
  ).then((result) => {
    settled = true;
    return result;
  });

  await new Promise((resolve) => setTimeout(resolve, 10));
  expect(settled).toBe(false);
  await expect(resultPromise).resolves.toEqual({
    ok: false,
    category: "timeout",
  });
  expect(controlled.records[0]?.killSignals).toEqual(["SIGTERM", "SIGKILL"]);
});

test("bounds cleanup after an asynchronous KILL error without a close event", async () => {
  const controlled = createControlledSpawn(
    UNCOOPERATIVE_EXIT_DELAY_MS,
    [],
    "async-error-no-close",
  );
  const submission = { kind: "start", metadata } satisfies Submission;

  await expect(
    dispatch(
      { ...config, executionTimeoutMs: 5 },
      submission,
      signal(),
      controlled.spawn,
    ),
  ).resolves.toEqual({ ok: false, category: "signal" });
  expect(controlled.records[0]?.killSignals).toEqual(["SIGTERM", "SIGKILL"]);
});

test("does not escalate after natural close wins timeout and abort races", async () => {
  const timeoutControlled = createControlledSpawn(1);
  const timeoutResult = await dispatch(
    { ...config, executionTimeoutMs: 50 },
    { kind: "start", metadata },
    signal(),
    timeoutControlled.spawn,
  );
  expect(timeoutResult).toEqual({ ok: true });
  expect(timeoutControlled.records[0]?.killSignals).toEqual([]);

  const abortControlled = createControlledSpawn(1);
  const controller = new AbortController();
  const abortResultPromise = dispatch(
    { ...config, executionTimeoutMs: 50 },
    { kind: "end", metadata },
    controller.signal,
    abortControlled.spawn,
  );
  controller.abort();

  await expect(abortResultPromise).resolves.toEqual({
    ok: false,
    category: "signal",
  });
  expect(abortControlled.records[0]?.killSignals).toEqual(["SIGTERM"]);
});
