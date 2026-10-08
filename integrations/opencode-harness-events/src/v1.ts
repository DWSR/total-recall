// biome-ignore-all lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
// biome-ignore-all lint/style/noExcessiveLinesPerFile: V1 normalization and host callback containment share one boundary.
// biome-ignore-all lint/correctness/useQwikValidLexicalScope: The adapter intentionally closes over one host runtime.
// biome-ignore-all lint/style/useExportsLast: Adapter types are part of the injected test seam.
import type { PluginInput } from "@opencode-ai/plugin";
import type {
  Diagnostic,
  EventMetadata,
  Generation,
  JsonObject,
  JsonValue,
  Submission,
} from "./model";
import type { SessionRuntime } from "./runtime";

export type V1PluginInput = Pick<
  PluginInput,
  "directory" | "worktree" | "project"
>;

export interface AdapterDependencies {
  readonly runtime: SessionRuntime;
  readonly report: (diagnostic: Diagnostic) => void;
  readonly now: () => Date;
}

export type V1AdapterDependencies = AdapterDependencies;

export interface V1EventInput {
  readonly event: unknown;
}

export type V1Hook = (input: unknown, output: unknown) => Promise<void>;

export interface V1Hooks {
  readonly event: (input: V1EventInput) => Promise<void>;
  readonly "chat.message": V1Hook;
  readonly "tool.execute.before": V1Hook;
  readonly "tool.execute.after": V1Hook;
  readonly dispose: () => Promise<void>;
}

type AcceptedNativeKind =
  | "session.created"
  | "session.deleted"
  | "session.updated"
  | "session.status"
  | "session.idle"
  | "message.updated"
  | "message.part.updated"
  | "session.error";

type AcceptedHookKind =
  | "chat.message"
  | "tool.execute.before"
  | "tool.execute.after";

type NativeKind = AcceptedNativeKind | AcceptedHookKind;

interface UnknownRecord {
  readonly [key: string]: unknown;
}

interface UnwrappedEvent {
  readonly native: UnknownRecord;
  readonly directory: string | undefined;
}

interface EventDetails {
  readonly sessionId: string;
  readonly directory: string | undefined;
  readonly timestampMs: number | undefined;
}

const GENERATION: Generation = "v1";
const MAX_DIAGNOSTIC_IDENTITY_LENGTH = 128;
const DIAGNOSTIC_IDENTITY_SUFFIX = "...";
const ACCEPTED_NATIVE_KINDS: ReadonlySet<string> = new Set<AcceptedNativeKind>([
  "session.created",
  "session.deleted",
  "session.updated",
  "session.status",
  "session.idle",
  "message.updated",
  "message.part.updated",
  "session.error",
]);
const TRAILING_SEPARATORS = /[\\/]+$/;

function isRecord(value: unknown): value is UnknownRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function usableString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const normalized = value.trim();
  if (normalized.length === 0) {
    return undefined;
  }

  return value;
}

function unwrappedEvent(value: unknown): UnwrappedEvent | undefined {
  if (!isRecord(value)) {
    return undefined;
  }

  const payload = value["payload"];
  if (isRecord(payload) && typeof payload["type"] === "string") {
    return {
      native: payload,
      directory: usableString(value["directory"]),
    };
  }

  if (typeof value["type"] !== "string") {
    return undefined;
  }

  return {
    native: value,
    directory: usableString(value["directory"]),
  };
}

function isAcceptedNativeKind(value: string): value is AcceptedNativeKind {
  return ACCEPTED_NATIVE_KINDS.has(value);
}

function readMilliseconds(value: unknown): number | undefined {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    return undefined;
  }

  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) {
    return undefined;
  }

  return value;
}

function timeValue(value: unknown, key: string): number | undefined {
  if (!isRecord(value)) {
    return undefined;
  }

  return readMilliseconds(value[key]);
}

function recordValue(
  value: UnknownRecord,
  key: string,
): UnknownRecord | undefined {
  const nested = value[key];
  if (!isRecord(nested)) {
    return undefined;
  }

  return nested;
}

function sessionIdValue(value: UnknownRecord, key: string): string | undefined {
  return usableString(value[key]);
}

function sessionInfoDetails(
  properties: UnknownRecord,
  timestampKey: "created" | "updated",
): EventDetails | undefined {
  const info = recordValue(properties, "info");
  if (info === undefined) {
    return undefined;
  }

  const sessionId = sessionIdValue(info, "id");
  if (sessionId === undefined) {
    return undefined;
  }

  const time = recordValue(info, "time");
  return {
    sessionId,
    directory: usableString(info["directory"]),
    timestampMs: timeValue(time, timestampKey),
  };
}

function messageDetails(properties: UnknownRecord): EventDetails | undefined {
  const info = recordValue(properties, "info");
  if (info === undefined) {
    return undefined;
  }

  const sessionId = sessionIdValue(info, "sessionID");
  if (sessionId === undefined) {
    return undefined;
  }

  const path = recordValue(info, "path");
  const time = recordValue(info, "time");
  return {
    sessionId,
    directory: usableString(path?.["cwd"]),
    timestampMs: timeValue(time, "created"),
  };
}

function partDetails(properties: UnknownRecord): EventDetails | undefined {
  const part = recordValue(properties, "part");
  if (part === undefined) {
    return undefined;
  }

  const sessionId = sessionIdValue(part, "sessionID");
  if (sessionId === undefined) {
    return undefined;
  }

  const time = recordValue(part, "time");
  return {
    sessionId,
    directory: undefined,
    timestampMs: timeValue(time, "end") ?? timeValue(time, "start"),
  };
}

function eventDetails(
  nativeKind: AcceptedNativeKind,
  properties: UnknownRecord,
): EventDetails | undefined {
  switch (nativeKind) {
    case "session.created":
      return sessionInfoDetails(properties, "created");
    case "session.deleted":
    case "session.updated":
      return sessionInfoDetails(properties, "updated");
    case "session.status":
    case "session.idle":
    case "session.error": {
      const sessionId = sessionIdValue(properties, "sessionID");
      if (sessionId === undefined) {
        return undefined;
      }

      return { sessionId, directory: undefined, timestampMs: undefined };
    }
    case "message.updated":
      return messageDetails(properties);
    case "message.part.updated":
      return partDetails(properties);
    default:
      return undefined;
  }
}

function hookDetails(value: unknown): EventDetails | undefined {
  if (!isRecord(value)) {
    return undefined;
  }

  const sessionId = sessionIdValue(value, "sessionID");
  if (sessionId === undefined) {
    return undefined;
  }

  return { sessionId, directory: undefined, timestampMs: undefined };
}

function projectBasename(path: string): string | undefined {
  const normalized = path.trim().replace(TRAILING_SEPARATORS, "");
  if (normalized.length === 0) {
    return undefined;
  }

  const slash = Math.max(
    normalized.lastIndexOf("/"),
    normalized.lastIndexOf("\\"),
  );
  const basename = normalized.slice(slash + 1);
  if (basename.length === 0) {
    return undefined;
  }

  return basename;
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: Recursive narrowing keeps the JSON boundary atomic.
function narrowJsonValue(
  value: unknown,
  ancestors: Set<object>,
): JsonValue | undefined {
  if (value === null) {
    return null;
  }

  switch (typeof value) {
    case "string":
    case "boolean":
      return value;
    case "number":
      return Number.isFinite(value) ? value : undefined;
    case "object":
      break;
    default:
      return undefined;
  }

  if (ancestors.has(value)) {
    return undefined;
  }
  ancestors.add(value);

  try {
    if (Array.isArray(value)) {
      const result: JsonValue[] = [];
      for (const item of value) {
        const narrowed = narrowJsonValue(item, ancestors);
        if (narrowed === undefined) {
          return undefined;
        }
        result.push(narrowed);
      }
      return result;
    }

    if (!isRecord(value)) {
      return undefined;
    }

    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      return undefined;
    }

    if (Object.getOwnPropertySymbols(value).length > 0) {
      return undefined;
    }

    const result: { [key: string]: JsonValue } = {};
    for (const key of Object.keys(value)) {
      const property = value[key];
      if (property !== undefined) {
        const narrowed = narrowJsonValue(property, ancestors);
        if (narrowed === undefined) {
          return undefined;
        }
        Object.defineProperty(result, key, {
          configurable: true,
          enumerable: true,
          value: narrowed,
          writable: true,
        });
      }
    }
    return result;
  } finally {
    ancestors.delete(value);
  }
}

function narrowJsonObject(value: unknown): JsonObject | undefined {
  const narrowed = narrowJsonValue(value, new Set<object>());
  if (narrowed === undefined || !isRecord(narrowed)) {
    return undefined;
  }

  return narrowed;
}

function boundedIdentity(value: string): string {
  if (value.length <= MAX_DIAGNOSTIC_IDENTITY_LENGTH) {
    return value;
  }

  return `${value.slice(
    0,
    MAX_DIAGNOSTIC_IDENTITY_LENGTH - DIAGNOSTIC_IDENTITY_SUFFIX.length,
  )}${DIAGNOSTIC_IDENTITY_SUFFIX}`;
}

function diagnostic(
  nativeKind: string | undefined,
  sessionId: string | undefined,
): Diagnostic {
  const result: Diagnostic = {
    category: "serialization",
    generation: GENERATION,
  };
  if (nativeKind !== undefined) {
    return sessionId === undefined
      ? { ...result, nativeKind: boundedIdentity(nativeKind) }
      : {
          ...result,
          nativeKind: boundedIdentity(nativeKind),
          sessionId: boundedIdentity(sessionId),
        };
  }

  return sessionId === undefined
    ? result
    : { ...result, sessionId: boundedIdentity(sessionId) };
}

function reportSerialization(
  dependencies: AdapterDependencies,
  nativeKind: string | undefined,
  sessionId: string | undefined,
): void {
  try {
    dependencies.report(diagnostic(nativeKind, sessionId));
  } catch {
    // A diagnostic sink is outside the host operation boundary.
  }
}

function timestamp(timestampMs: number | undefined, now: () => Date): string {
  const date = timestampMs === undefined ? now() : new Date(timestampMs);
  return date.toISOString();
}

// biome-ignore lint/complexity/useMaxParams: The metadata seam keeps the injected clock and per-adapter state explicit.
function metadata(
  input: V1PluginInput,
  details: EventDetails,
  eventDirectory: string | undefined,
  sessionDirectories: ReadonlyMap<string, string>,
  now: () => Date,
): EventMetadata {
  const currentWorkingDirectory =
    details.directory ??
    eventDirectory ??
    sessionDirectories.get(details.sessionId) ??
    usableString(input.directory);
  if (currentWorkingDirectory === undefined) {
    throw new Error("v1 event has no usable directory");
  }

  const projectContext =
    usableString(input.project.worktree) ?? usableString(input.worktree);
  const projectName =
    projectBasename(projectContext ?? "") ??
    projectBasename(currentWorkingDirectory);
  if (projectName === undefined) {
    throw new Error("v1 event has no usable project name");
  }

  return {
    generation: GENERATION,
    sessionId: details.sessionId,
    projectName,
    currentWorkingDirectory,
    timestamp: timestamp(details.timestampMs, now),
  };
}

function observation(
  nativeKind: NativeKind,
  eventMetadata: EventMetadata,
  data: JsonObject,
): Extract<Submission, { readonly kind: "observation" }> {
  return {
    kind: "observation",
    metadata: eventMetadata,
    hookType: `opencode.${GENERATION}.${nativeKind}`,
    data,
  };
}

function hookData(input: unknown, output: unknown): JsonObject | undefined {
  if (!(isRecord(input) && isRecord(output))) {
    return undefined;
  }

  return narrowJsonObject({ input, output });
}

// biome-ignore lint/complexity/useMaxParams: Hook normalization keeps the injected adapter seams explicit.
function handleHook(
  input: V1PluginInput,
  dependencies: AdapterDependencies,
  sessionDirectories: Map<string, string>,
  nativeKind: AcceptedHookKind,
  hookInput: unknown,
  hookOutput: unknown,
): void {
  let sessionId: string | undefined;

  try {
    const details = hookDetails(hookInput);
    sessionId = details?.sessionId;
    if (details === undefined) {
      reportSerialization(dependencies, nativeKind, sessionId);
      return;
    }

    const data = hookData(hookInput, hookOutput);
    if (data === undefined) {
      reportSerialization(dependencies, nativeKind, sessionId);
      return;
    }

    const hookMetadata = metadata(
      input,
      details,
      undefined,
      sessionDirectories,
      dependencies.now,
    );
    sessionDirectories.set(
      details.sessionId,
      hookMetadata.currentWorkingDirectory,
    );
    dependencies.runtime.observe(observation(nativeKind, hookMetadata, data));
  } catch {
    reportSerialization(dependencies, nativeKind, sessionId);
  }
}

export function createV1Adapter(
  input: V1PluginInput,
  dependencies: AdapterDependencies,
): V1Hooks {
  const sessionDirectories = new Map<string, string>();
  let stopped = false;
  let disposePromise: Promise<void> | undefined;

  const createHook =
    (nativeKind: AcceptedHookKind): V1Hook =>
    (hookInput, hookOutput) => {
      if (stopped) {
        return Promise.resolve();
      }

      handleHook(
        input,
        dependencies,
        sessionDirectories,
        nativeKind,
        hookInput,
        hookOutput,
      );
      return Promise.resolve();
    };

  const dispose = (): Promise<void> => {
    if (disposePromise !== undefined) {
      return disposePromise;
    }

    stopped = true;
    sessionDirectories.clear();
    try {
      disposePromise = Promise.resolve(dependencies.runtime.close()).catch(
        () => undefined,
      );
    } catch {
      disposePromise = Promise.resolve();
    }
    return disposePromise;
  };

  return {
    // biome-ignore lint/suspicious/useAwait: The host API requires an async callback even though normalization is local.
    event: async (eventInput) => {
      if (stopped) {
        return;
      }

      let nativeKind: string | undefined;
      let sessionId: string | undefined;

      try {
        const unwrapped = unwrappedEvent(
          isRecord(eventInput) ? eventInput["event"] : undefined,
        );
        if (unwrapped === undefined) {
          reportSerialization(dependencies, undefined, undefined);
          return;
        }

        const kindValue = unwrapped.native["type"];
        if (typeof kindValue !== "string" || kindValue.trim().length === 0) {
          reportSerialization(dependencies, undefined, undefined);
          return;
        }
        nativeKind = kindValue;
        if (!isAcceptedNativeKind(nativeKind)) {
          return;
        }

        const properties = unwrapped.native["properties"];
        if (!isRecord(properties)) {
          reportSerialization(dependencies, nativeKind, undefined);
          return;
        }

        const details = eventDetails(nativeKind, properties);
        sessionId = details?.sessionId;
        if (details === undefined) {
          reportSerialization(dependencies, nativeKind, undefined);
          return;
        }

        if (nativeKind === "message.part.updated") {
          const delta = properties["delta"];
          if (delta !== undefined) {
            if (typeof delta !== "string") {
              reportSerialization(dependencies, nativeKind, sessionId);
              return;
            }
            return;
          }
        }

        const data = narrowJsonObject(properties);
        if (data === undefined) {
          reportSerialization(dependencies, nativeKind, sessionId);
          return;
        }

        const eventMetadata = metadata(
          input,
          details,
          unwrapped.directory,
          sessionDirectories,
          dependencies.now,
        );
        const eventObservation = observation(nativeKind, eventMetadata, data);
        const directory = eventMetadata.currentWorkingDirectory;
        sessionDirectories.set(details.sessionId, directory);

        switch (nativeKind) {
          case "session.created":
            dependencies.runtime.start(eventMetadata);
            dependencies.runtime.observe(eventObservation);
            break;
          case "session.deleted":
            try {
              dependencies.runtime.observe(eventObservation);
            } finally {
              sessionDirectories.delete(details.sessionId);
              dependencies.runtime.end(eventMetadata);
            }
            break;
          default:
            dependencies.runtime.observe(eventObservation);
            break;
        }
      } catch {
        reportSerialization(dependencies, nativeKind, sessionId);
      }
    },
    "chat.message": createHook("chat.message"),
    "tool.execute.before": createHook("tool.execute.before"),
    "tool.execute.after": createHook("tool.execute.after"),
    dispose,
  };
}

export function setupV1(
  input: V1PluginInput,
  dependencies: V1AdapterDependencies,
): Promise<V1Hooks> {
  return Promise.resolve(createV1Adapter(input, dependencies));
}
