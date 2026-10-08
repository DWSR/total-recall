// biome-ignore-all lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
// biome-ignore-all lint/style/noExcessiveLinesPerFile: V2 normalization and subscription containment share one boundary.
// biome-ignore-all lint/style/useExportsLast: Adapter types are part of the injected test seam.
// biome-ignore-all lint/complexity/useLiteralKeys: Native records use explicit JSON keys through an index signature.
// biome-ignore-all lint/correctness/useQwikValidLexicalScope: Adapter callbacks intentionally close over injected runtime state.
// biome-ignore-all lint/performance/noAwaitInLoops: Hook registrations are disposed in registration order during cleanup.
import type { V2Event } from "@opencode/client";
import type { Cleanup, Context } from "@opencode/plugin/promise/plugin";
import type {
  Diagnostic,
  EventMetadata,
  Generation,
  JsonObject,
  JsonValue,
  Submission,
} from "./model";
import type { SessionRuntime } from "./runtime";

export interface V2Context {
  readonly location: {
    readonly directory: string;
  };
  readonly event: {
    readonly subscribe: V2Subscribe;
  };
  readonly session: {
    readonly hook: V2SessionHook;
  };
  readonly tool: {
    readonly hook: V2ToolHook;
  };
}

export type OpenCodeV2Context = Pick<
  Context,
  "event" | "location" | "session" | "tool"
>;
export type V2NativeEvent = V2Event;

export interface V2Registration {
  readonly dispose: () => Promise<void> | void;
}

export type V2HookCallback = (input: unknown) => Promise<void> | void;

export type V2SessionHook = (
  name: "prompt",
  callback: V2HookCallback,
) => Promise<V2Registration>;

export type V2ToolHook = (
  name: "execute.before" | "execute.after",
  callback: V2HookCallback,
) => Promise<V2Registration>;

export interface V2SubscribeOptions {
  readonly signal?: AbortSignal;
}

export type V2Subscribe = (
  options?: V2SubscribeOptions,
) => AsyncIterable<unknown>;

export interface V2AdapterDependencies {
  readonly runtime: SessionRuntime;
  readonly report: (diagnostic: Diagnostic) => void;
  readonly now: () => Date;
  readonly subscribe?: V2Subscribe;
}

type AcceptedNativeKind =
  | "session.created"
  | "session.deleted"
  | "session.execution.started"
  | "session.execution.succeeded"
  | "session.execution.interrupted"
  | "session.text.ended"
  | "session.reasoning.ended"
  | "session.execution.failed"
  | "session.step.failed"
  | "session.compaction.failed";

type AcceptedHookKind =
  | "session.prompt"
  | "tool.execute.before"
  | "tool.execute.after";

interface UnknownRecord {
  readonly [key: string]: unknown;
}

interface EventDetails {
  readonly sessionId: string;
  readonly directory: string | undefined;
  readonly timestampMs: number | undefined;
}

const GENERATION: Generation = "v2";
const MAX_DIAGNOSTIC_IDENTITY_LENGTH = 128;
const DIAGNOSTIC_IDENTITY_SUFFIX = "...";
const ACCEPTED_NATIVE_KINDS: ReadonlySet<string> = new Set<AcceptedNativeKind>([
  "session.created",
  "session.deleted",
  "session.execution.started",
  "session.execution.succeeded",
  "session.execution.interrupted",
  "session.text.ended",
  "session.reasoning.ended",
  "session.execution.failed",
  "session.step.failed",
  "session.compaction.failed",
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

function eventDirectory(event: UnknownRecord): string | undefined {
  const location = event["location"];
  if (!isRecord(location)) {
    return undefined;
  }

  return usableString(location["directory"]);
}

function sessionIdFromData(data: unknown): string | undefined {
  if (!isRecord(data)) {
    return undefined;
  }

  return usableString(data["sessionID"]);
}

function eventDetails(
  event: UnknownRecord,
  data: UnknownRecord,
): EventDetails | undefined {
  const sessionId = sessionIdFromData(data);
  if (sessionId === undefined) {
    return undefined;
  }

  return {
    sessionId,
    directory: eventDirectory(event),
    timestampMs: readMilliseconds(event["created"]),
  };
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: Recursive narrowing keeps the JSON boundary atomic.
// biome-ignore lint/complexity/noExcessiveLinesPerFunction: Recursive narrowing keeps the JSON boundary atomic.
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
      if (Number.isFinite(value)) {
        return value;
      }
      return undefined;
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
  category: Diagnostic["category"],
  nativeKind?: string,
  sessionId?: string,
): Diagnostic {
  const result: Diagnostic = { category, generation: GENERATION };
  if (nativeKind !== undefined) {
    if (sessionId === undefined) {
      return { ...result, nativeKind: boundedIdentity(nativeKind) };
    }
    return {
      ...result,
      nativeKind: boundedIdentity(nativeKind),
      sessionId: boundedIdentity(sessionId),
    };
  }

  if (sessionId === undefined) {
    return result;
  }
  return { ...result, sessionId: boundedIdentity(sessionId) };
}

function reportSerialization(
  dependencies: V2AdapterDependencies,
  nativeKind: string | undefined,
  sessionId: string | undefined,
): void {
  try {
    dependencies.report(diagnostic("serialization", nativeKind, sessionId));
  } catch {
    // A diagnostic sink is outside the host operation boundary.
  }
}

function reportSignal(dependencies: V2AdapterDependencies): void {
  try {
    dependencies.report(diagnostic("signal"));
  } catch {
    // A diagnostic sink is outside the host operation boundary.
  }
}

function timestamp(timestampMs: number | undefined, now: () => Date): string {
  let date: Date;
  if (timestampMs === undefined) {
    date = now();
  } else {
    date = new Date(timestampMs);
  }
  return date.toISOString();
}

function metadata(
  context: V2Context,
  details: EventDetails,
  sessionDirectories: ReadonlyMap<string, string>,
  now: () => Date,
): EventMetadata {
  const contextDirectory = usableString(context.location.directory);
  const currentWorkingDirectory =
    details.directory ??
    sessionDirectories.get(details.sessionId) ??
    contextDirectory;
  if (currentWorkingDirectory === undefined) {
    throw new Error("v2 event has no usable directory");
  }

  const projectName =
    projectBasename(contextDirectory ?? "") ??
    projectBasename(currentWorkingDirectory);
  if (projectName === undefined) {
    throw new Error("v2 event has no usable project name");
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
  nativeKind: AcceptedNativeKind | AcceptedHookKind,
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

function hookDetails(value: unknown): EventDetails | undefined {
  if (!isRecord(value)) {
    return undefined;
  }

  const sessionId = sessionIdFromData(value);
  if (sessionId === undefined) {
    return undefined;
  }

  return {
    sessionId,
    directory: undefined,
    timestampMs: undefined,
  };
}

// biome-ignore lint/complexity/useMaxParams: Hook normalization keeps the injected adapter seams explicit.
function handleHook(
  context: V2Context,
  dependencies: V2AdapterDependencies,
  sessionDirectories: Map<string, string>,
  nativeKind: AcceptedHookKind,
  hookInput: unknown,
): void {
  let sessionId: string | undefined;

  try {
    const details = hookDetails(hookInput);
    sessionId = details?.sessionId;
    if (details === undefined) {
      reportSerialization(dependencies, nativeKind, sessionId);
      return;
    }

    const data = narrowJsonObject(hookInput);
    if (data === undefined) {
      reportSerialization(dependencies, nativeKind, sessionId);
      return;
    }

    const hookMetadata = metadata(
      context,
      details,
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

function isAcceptedNativeKind(value: string): value is AcceptedNativeKind {
  return ACCEPTED_NATIVE_KINDS.has(value);
}

// biome-ignore lint/complexity/noExcessiveLinesPerFunction: Event selection and runtime ordering share one boundary.
function handleEvent(
  context: V2Context,
  dependencies: V2AdapterDependencies,
  sessionDirectories: Map<string, string>,
  event: unknown,
): void {
  let nativeKind: string | undefined;
  let sessionId: string | undefined;

  try {
    if (!isRecord(event)) {
      reportSerialization(dependencies, undefined, undefined);
      return;
    }

    const kindValue = event["type"];
    if (typeof kindValue !== "string" || kindValue.trim().length === 0) {
      reportSerialization(dependencies, undefined, undefined);
      return;
    }
    nativeKind = kindValue;
    if (!isAcceptedNativeKind(nativeKind)) {
      return;
    }

    const nativeData = event["data"];
    sessionId = sessionIdFromData(nativeData);
    if (!isRecord(nativeData)) {
      reportSerialization(dependencies, nativeKind, sessionId);
      return;
    }

    const details = eventDetails(event, nativeData);
    sessionId = details?.sessionId ?? sessionId;
    if (details === undefined) {
      reportSerialization(dependencies, nativeKind, sessionId);
      return;
    }

    const data = narrowJsonObject(nativeData);
    if (data === undefined) {
      reportSerialization(dependencies, nativeKind, sessionId);
      return;
    }

    const eventMetadata = metadata(
      context,
      details,
      sessionDirectories,
      dependencies.now,
    );
    const eventObservation = observation(nativeKind, eventMetadata, data);
    sessionDirectories.set(
      details.sessionId,
      eventMetadata.currentWorkingDirectory,
    );

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
}

// biome-ignore lint/complexity/useMaxParams: The adapter keeps injected context, runtime, state, stream, and stop state explicit.
async function consume(
  context: V2Context,
  dependencies: V2AdapterDependencies,
  sessionDirectories: Map<string, string>,
  subscription: AsyncIterable<unknown>,
  isStopped: () => boolean,
): Promise<void> {
  try {
    for await (const event of subscription) {
      if (isStopped()) {
        return;
      }
      handleEvent(context, dependencies, sessionDirectories, event);
    }
  } catch {
    if (!isStopped()) {
      reportSignal(dependencies);
    }
  }
}

interface V2AdapterController {
  readonly cleanup: Cleanup;
  readonly registerHooks: () => Promise<void>;
}

function isRegistration(value: unknown): value is V2Registration {
  return isRecord(value) && typeof value["dispose"] === "function";
}

interface V2AdapterState {
  readonly context: V2Context;
  readonly dependencies: V2AdapterDependencies;
  readonly sessionDirectories: Map<string, string>;
  readonly controller: AbortController;
  readonly registrations: V2Registration[];
  stopped: boolean;
  consumerPromise: Promise<void>;
  cleanupPromise: Promise<void> | undefined;
}

function createHook(
  state: V2AdapterState,
  nativeKind: AcceptedHookKind,
): V2HookCallback {
  return (hookInput) => {
    if (state.stopped) {
      return;
    }

    handleHook(
      state.context,
      state.dependencies,
      state.sessionDirectories,
      nativeKind,
      hookInput,
    );
  };
}

async function registerOne(
  state: V2AdapterState,
  register: () => Promise<unknown>,
): Promise<void> {
  if (state.stopped) {
    return;
  }

  try {
    const registration = await register();
    if (!isRegistration(registration)) {
      return;
    }

    if (state.stopped) {
      try {
        await registration.dispose();
      } catch {
        // Registration disposal is best effort during shutdown.
      }
      return;
    }

    state.registrations.push(registration);
  } catch {
    // A host registration failure must not reject plugin setup.
  }
}

async function registerHooks(state: V2AdapterState): Promise<void> {
  await registerOne(state, () =>
    state.context.session.hook("prompt", createHook(state, "session.prompt")),
  );
  await registerOne(state, () =>
    state.context.tool.hook(
      "execute.before",
      createHook(state, "tool.execute.before"),
    ),
  );
  await registerOne(state, () =>
    state.context.tool.hook(
      "execute.after",
      createHook(state, "tool.execute.after"),
    ),
  );
}

async function finishCleanup(state: V2AdapterState): Promise<void> {
  for (const registration of state.registrations) {
    try {
      await registration.dispose();
    } catch {
      // One registration must not prevent the remaining cleanup.
    }
  }
  state.registrations.length = 0;
  state.sessionDirectories.clear();

  try {
    await state.dependencies.runtime.close();
  } catch {
    // Runtime shutdown is fail-open for the host.
  }
}

function requestCleanup(state: V2AdapterState): Promise<void> {
  if (state.cleanupPromise !== undefined) {
    return state.cleanupPromise;
  }

  state.cleanupPromise = Promise.resolve().then(() => finishCleanup(state));
  state.stopped = true;
  state.controller.abort();
  return state.cleanupPromise;
}

function createV2Controller(
  context: V2Context,
  dependencies: V2AdapterDependencies,
): V2AdapterController {
  const state: V2AdapterState = {
    context,
    dependencies,
    sessionDirectories: new Map<string, string>(),
    controller: new AbortController(),
    registrations: [],
    stopped: false,
    consumerPromise: Promise.resolve(),
    cleanupPromise: undefined,
  };
  const subscribe =
    dependencies.subscribe ??
    ((options?: V2SubscribeOptions) => context.event.subscribe(options));

  try {
    const subscription = subscribe({ signal: state.controller.signal });
    state.consumerPromise = consume(
      context,
      dependencies,
      state.sessionDirectories,
      subscription,
      () => state.stopped,
    );
  } catch {
    reportSignal(dependencies);
  }

  return {
    cleanup: () => requestCleanup(state),
    registerHooks: () => registerHooks(state),
  };
}

export function createV2Adapter(
  context: V2Context,
  dependencies: V2AdapterDependencies,
): Cleanup {
  return createV2Controller(context, dependencies).cleanup;
}

export async function setupV2(
  context: V2Context | OpenCodeV2Context,
  dependencies: V2AdapterDependencies,
): Promise<Cleanup> {
  const adapterContext: V2Context = {
    location: { directory: context.location.directory },
    event: {
      subscribe: (options) => context.event.subscribe(options),
    },
    session: {
      hook: async (name, callback) =>
        context.session.hook(name, (input) => callback(input)),
    },
    tool: {
      hook: async (name, callback) =>
        context.tool.hook(name, (input) => callback(input)),
    },
  };
  const adapter = createV2Controller(adapterContext, dependencies);
  await adapter.registerHooks();
  return adapter.cleanup;
}
