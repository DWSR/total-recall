import { basename, isAbsolute } from "node:path";

import { normalizeJsonObject } from "./model.ts";
import type { Diagnostic, EventMetadata, JsonObject } from "./model.ts";

export interface AdapterContext {
  readonly cwd?: unknown;
  readonly sessionManager?: {
    readonly getSessionId?: () => unknown;
  };
}

export interface HandlerClock {
  readonly now: () => Date;
}

export interface NativeEventIdentity {
  readonly type: string;
  readonly message?: {
    readonly role?: unknown;
  };
}

export type LifecycleNativeEvent = "session_start" | "session_shutdown";

export type AcceptedObservationNativeEvent =
  | "session_info_changed"
  | "session_compact"
  | "session_compact_failed"
  | "agent_start"
  | "agent_settled"
  | "ui_prompt_start"
  | "ui_prompt_end"
  | "turn_start"
  | "turn_end"
  | "message_end"
  | "tool_execution_start"
  | "tool_execution_end";

export type NativeEventSelection =
  | {
      readonly kind: "lifecycle";
      readonly lifecycle: "start" | "end";
      readonly nativeEvent: LifecycleNativeEvent;
    }
  | {
      readonly kind: "observation";
      readonly nativeEvent: AcceptedObservationNativeEvent;
    }
  | { readonly kind: "excluded" };

export interface ObservationSelection {
  readonly kind: "observation";
  readonly metadata: EventMetadata;
  readonly nativeEvent: AcceptedObservationNativeEvent;
  readonly hookType: string;
}

export interface NativeObservationEnvelope {
  readonly source: "pi";
  readonly version: "0.86.1";
  readonly event: AcceptedObservationNativeEvent;
  readonly payload: JsonObject;
}

export type ProjectionDiagnosticReporter = (diagnostic: Diagnostic) => void;

type ProjectionPayload = Record<string, unknown>;

type ProjectionResult =
  | { readonly kind: "excluded" }
  | { readonly kind: "normalization" }
  | { readonly kind: "projected"; readonly payload: ProjectionPayload };

type OwnProperty =
  | { readonly kind: "accessor" }
  | { readonly kind: "missing" }
  | { readonly kind: "value"; readonly value: unknown };

const MAXIMUM_DIAGNOSTIC_IDENTITY_LENGTH = 128;

export function projectNativeEvent(
  nativeEvent: unknown,
  sessionId: unknown,
  reportDiagnostic: ProjectionDiagnosticReporter = console.error,
): NativeObservationEnvelope | undefined {
  let event: AcceptedObservationNativeEvent | undefined;

  try {
    const nativeEventType = readNativeEventType(nativeEvent);

    if (nativeEventType === undefined) {
      reportProjectionFailure("normalization", undefined, sessionId, reportDiagnostic);
      return undefined;
    }

    if (!isAcceptedObservationNativeEvent(nativeEventType)) {
      return undefined;
    }

    event = nativeEventType;
    const projection = projectNativePayload(event, nativeEvent);

    if (projection.kind === "excluded") {
      return undefined;
    }

    if (projection.kind === "normalization") {
      reportProjectionFailure("normalization", event, sessionId, reportDiagnostic);
      return undefined;
    }

    try {
      return {
        source: "pi",
        version: "0.86.1",
        event,
        payload: normalizeJsonObject(projection.payload),
      };
    } catch {
      reportProjectionFailure("serialization", event, sessionId, reportDiagnostic);
      return undefined;
    }
  } catch {
    reportProjectionFailure("normalization", event, sessionId, reportDiagnostic);
    return undefined;
  }
}

function projectNativePayload(
  event: AcceptedObservationNativeEvent,
  nativeEvent: unknown,
): ProjectionResult {
  if (!isObject(nativeEvent)) {
    return { kind: "normalization" };
  }

  switch (event) {
    case "session_info_changed":
      return projectionFrom(projectSessionInfo(nativeEvent));
    case "session_compact":
      return projectionFrom(projectSessionCompact(nativeEvent));
    case "session_compact_failed":
      return projectionFrom(projectSessionCompactFailed(nativeEvent));
    case "agent_start":
    case "agent_settled":
      return { kind: "projected", payload: {} };
    case "ui_prompt_start":
    case "ui_prompt_end":
      return projectionFrom(projectWait(nativeEvent));
    case "turn_start":
      return projectionFrom(projectTurnStart(nativeEvent));
    case "turn_end":
      return projectionFrom(projectTurnEnd(nativeEvent));
    case "message_end":
      return projectCompletedMessage(nativeEvent);
    case "tool_execution_start":
      return projectionFrom(projectToolExecutionStart(nativeEvent));
    case "tool_execution_end":
      return projectionFrom(projectToolExecutionEnd(nativeEvent));
  }
}

function projectSessionInfo(nativeEvent: object): ProjectionPayload | undefined {
  const payload: ProjectionPayload = {};

  return copyOptional(payload, nativeEvent, "name", "string") ? payload : undefined;
}

function projectSessionCompact(nativeEvent: object): ProjectionPayload | undefined {
  const compactionEntry = readOwnProperty(nativeEvent, "compactionEntry");
  const fromExtension = readRequiredBoolean(nativeEvent, "fromExtension");
  const reason = readRequiredString(nativeEvent, "reason");
  const willRetry = readRequiredBoolean(nativeEvent, "willRetry");

  if (
    compactionEntry.kind !== "value" ||
    fromExtension === undefined ||
    !isCompactionReason(reason) ||
    willRetry === undefined
  ) {
    return undefined;
  }

  const projectedEntry = projectCompactionEntry(compactionEntry.value);

  if (projectedEntry === undefined) {
    return undefined;
  }

  return { compactionEntry: projectedEntry, fromExtension, reason, willRetry };
}

function projectCompactionEntry(value: unknown): ProjectionPayload | undefined {
  if (!isObject(value)) {
    return undefined;
  }

  const firstKeptEntryId = readRequiredString(value, "firstKeptEntryId");
  const id = readRequiredString(value, "id");
  const parentId = readRequiredStringOrNull(value, "parentId");
  const timestamp = readRequiredString(value, "timestamp");
  const tokensBefore = readRequiredNumber(value, "tokensBefore");

  if (
    firstKeptEntryId === undefined ||
    id === undefined ||
    parentId === undefined ||
    timestamp === undefined ||
    tokensBefore === undefined
  ) {
    return undefined;
  }

  const payload: ProjectionPayload = {
    firstKeptEntryId,
    id,
    parentId,
    timestamp,
    tokensBefore,
  };

  return copyOptional(payload, value, "fromHook", "boolean") ? payload : undefined;
}

function projectSessionCompactFailed(nativeEvent: object): ProjectionPayload | undefined {
  const aborted = readRequiredBoolean(nativeEvent, "aborted");
  const fromExtension = readRequiredBoolean(nativeEvent, "fromExtension");
  const reason = readRequiredString(nativeEvent, "reason");
  const willRetry = readRequiredBoolean(nativeEvent, "willRetry");

  if (
    aborted === undefined ||
    fromExtension === undefined ||
    !isCompactionReason(reason) ||
    willRetry === undefined
  ) {
    return undefined;
  }

  const payload: ProjectionPayload = { aborted, fromExtension, reason, willRetry };

  return copyOptional(payload, nativeEvent, "errorMessage", "string") ? payload : undefined;
}

function projectWait(nativeEvent: object): ProjectionPayload | undefined {
  const kind = readRequiredString(nativeEvent, "kind");
  const reason = readRequiredString(nativeEvent, "reason");

  if (!isUiPromptKind(kind) || reason !== "ui_prompt") {
    return undefined;
  }

  const payload: ProjectionPayload = { kind, reason };

  return copyOptional(payload, nativeEvent, "title", "string") ? payload : undefined;
}

function projectTurnStart(nativeEvent: object): ProjectionPayload | undefined {
  const timestamp = readRequiredNumber(nativeEvent, "timestamp");
  const turnIndex = readRequiredNumber(nativeEvent, "turnIndex");

  return timestamp === undefined || turnIndex === undefined ? undefined : { timestamp, turnIndex };
}

function projectTurnEnd(nativeEvent: object): ProjectionPayload | undefined {
  const turnIndex = readRequiredNumber(nativeEvent, "turnIndex");

  return turnIndex === undefined ? undefined : { turnIndex };
}

function projectCompletedMessage(nativeEvent: object): ProjectionResult {
  const message = readOwnProperty(nativeEvent, "message");

  if (message.kind !== "value" || !isObject(message.value)) {
    return { kind: "normalization" };
  }

  const role = readRequiredString(message.value, "role");

  if (role === "user") {
    return projectionFrom(projectUserMessage(message.value));
  }

  if (role === "assistant") {
    return projectionFrom(projectAssistantMessage(message.value));
  }

  return role === undefined ? { kind: "normalization" } : { kind: "excluded" };
}

function projectUserMessage(message: object): ProjectionPayload | undefined {
  const content = readOwnProperty(message, "content");
  const timestamp = readRequiredNumber(message, "timestamp");

  if (content.kind !== "value" || timestamp === undefined) {
    return undefined;
  }

  const text = projectTextContent(content.value, ["image"]);

  return text === undefined ? undefined : { role: "user", text, timestamp };
}

function projectAssistantMessage(message: object): ProjectionPayload | undefined {
  const api = readRequiredString(message, "api");
  const content = readOwnProperty(message, "content");
  const model = readRequiredString(message, "model");
  const provider = readRequiredString(message, "provider");
  const stopReason = readRequiredString(message, "stopReason");
  const timestamp = readRequiredNumber(message, "timestamp");
  const usage = readOwnProperty(message, "usage");

  if (
    api === undefined ||
    content.kind !== "value" ||
    !Array.isArray(content.value) ||
    model === undefined ||
    provider === undefined ||
    !isAssistantStopReason(stopReason) ||
    timestamp === undefined ||
    usage.kind !== "value"
  ) {
    return undefined;
  }

  const text = projectTextContent(content.value, ["thinking", "toolCall"]);
  const projectedUsage = projectUsage(usage.value);

  if (text === undefined || projectedUsage === undefined) {
    return undefined;
  }

  const payload: ProjectionPayload = {
    api,
    model,
    provider,
    role: "assistant",
    stopReason,
    text,
    timestamp,
    usage: projectedUsage,
  };

  return copyOptional(payload, message, "responseModel", "string") &&
    copyOptional(payload, message, "errorMessage", "string")
    ? payload
    : undefined;
}

function projectUsage(value: unknown): ProjectionPayload | undefined {
  if (!isObject(value)) {
    return undefined;
  }

  const cacheRead = readRequiredNumber(value, "cacheRead");
  const cacheWrite = readRequiredNumber(value, "cacheWrite");
  const input = readRequiredNumber(value, "input");
  const output = readRequiredNumber(value, "output");
  const totalTokens = readRequiredNumber(value, "totalTokens");
  const cost = projectUsageCost(readOwnProperty(value, "cost"));

  if (
    cacheRead === undefined ||
    cacheWrite === undefined ||
    input === undefined ||
    output === undefined ||
    totalTokens === undefined ||
    cost === undefined
  ) {
    return undefined;
  }

  const payload: ProjectionPayload = { cacheRead, cacheWrite, cost, input, output, totalTokens };

  return copyOptional(payload, value, "cacheWrite1h", "number") &&
    copyOptional(payload, value, "reasoning", "number")
    ? payload
    : undefined;
}

function projectUsageCost(value: OwnProperty): ProjectionPayload | undefined {
  if (value.kind !== "value" || !isObject(value.value)) {
    return undefined;
  }

  const cacheRead = readRequiredNumber(value.value, "cacheRead");
  const cacheWrite = readRequiredNumber(value.value, "cacheWrite");
  const input = readRequiredNumber(value.value, "input");
  const output = readRequiredNumber(value.value, "output");
  const total = readRequiredNumber(value.value, "total");

  return cacheRead === undefined ||
    cacheWrite === undefined ||
    input === undefined ||
    output === undefined ||
    total === undefined
    ? undefined
    : { cacheRead, cacheWrite, input, output, total };
}

function projectTextContent(
  content: unknown,
  omittedBlockTypes: readonly string[],
): string | undefined {
  if (typeof content === "string") {
    return content;
  }

  const blocks = readArrayValues(content);

  if (blocks === undefined) {
    return undefined;
  }

  const text: string[] = [];

  for (const block of blocks) {
    if (!isObject(block)) {
      return undefined;
    }

    const type = readRequiredString(block, "type");

    if (type === "text") {
      const value = readRequiredString(block, "text");

      if (value === undefined) {
        return undefined;
      }

      text.push(value);
    } else if (!omittedBlockTypes.includes(type ?? "")) {
      return undefined;
    }
  }

  return text.join("");
}

function projectToolExecutionStart(nativeEvent: object): ProjectionPayload | undefined {
  const args = readOwnProperty(nativeEvent, "args");
  const toolCallId = readRequiredString(nativeEvent, "toolCallId");
  const toolName = readRequiredString(nativeEvent, "toolName");

  return args.kind !== "value" || toolCallId === undefined || toolName === undefined
    ? undefined
    : { args: args.value, toolCallId, toolName };
}

function projectToolExecutionEnd(nativeEvent: object): ProjectionPayload | undefined {
  const isError = readRequiredBoolean(nativeEvent, "isError");
  const result = readOwnProperty(nativeEvent, "result");
  const toolCallId = readRequiredString(nativeEvent, "toolCallId");
  const toolName = readRequiredString(nativeEvent, "toolName");

  return isError === undefined ||
    result.kind !== "value" ||
    toolCallId === undefined ||
    toolName === undefined
    ? undefined
    : { isError, result: result.value, toolCallId, toolName };
}

function projectionFrom(payload: ProjectionPayload | undefined): ProjectionResult {
  return payload === undefined ? { kind: "normalization" } : { kind: "projected", payload };
}

function readNativeEventType(nativeEvent: unknown): string | undefined {
  if (!isObject(nativeEvent)) {
    return undefined;
  }

  return readRequiredString(nativeEvent, "type");
}

function isAcceptedObservationNativeEvent(value: string): value is AcceptedObservationNativeEvent {
  switch (value) {
    case "session_info_changed":
    case "session_compact":
    case "session_compact_failed":
    case "agent_start":
    case "agent_settled":
    case "ui_prompt_start":
    case "ui_prompt_end":
    case "turn_start":
    case "turn_end":
    case "message_end":
    case "tool_execution_start":
    case "tool_execution_end":
      return true;
    default:
      return false;
  }
}

function isCompactionReason(
  value: string | undefined,
): value is "manual" | "threshold" | "overflow" {
  return value === "manual" || value === "threshold" || value === "overflow";
}

function isUiPromptKind(value: string | undefined): boolean {
  return (
    value === "select" ||
    value === "confirm" ||
    value === "input" ||
    value === "editor" ||
    value === "custom"
  );
}

function isAssistantStopReason(value: string | undefined): boolean {
  return (
    value === "pending" ||
    value === "stop" ||
    value === "length" ||
    value === "toolUse" ||
    value === "error" ||
    value === "aborted" ||
    value === "deferred"
  );
}

function readArrayValues(value: unknown): readonly unknown[] | undefined {
  if (!Array.isArray(value)) {
    return undefined;
  }

  const length = readOwnProperty(value, "length");

  if (
    length.kind !== "value" ||
    typeof length.value !== "number" ||
    !Number.isInteger(length.value) ||
    length.value < 0
  ) {
    return undefined;
  }

  const values: unknown[] = [];

  for (let index = 0; index < length.value; index += 1) {
    const item = readOwnProperty(value, String(index));

    if (item.kind !== "value") {
      return undefined;
    }

    values.push(item.value);
  }

  return values;
}

function readRequiredString(value: object, property: string): string | undefined {
  const candidate = readOwnProperty(value, property);

  return candidate.kind === "value" && typeof candidate.value === "string"
    ? candidate.value
    : undefined;
}

function readRequiredStringOrNull(value: object, property: string): string | null | undefined {
  const candidate = readOwnProperty(value, property);

  return candidate.kind === "value" &&
    (typeof candidate.value === "string" || candidate.value === null)
    ? candidate.value
    : undefined;
}

function readRequiredNumber(value: object, property: string): number | undefined {
  const candidate = readOwnProperty(value, property);

  return candidate.kind === "value" && typeof candidate.value === "number"
    ? candidate.value
    : undefined;
}

function readRequiredBoolean(value: object, property: string): boolean | undefined {
  const candidate = readOwnProperty(value, property);

  return candidate.kind === "value" && typeof candidate.value === "boolean"
    ? candidate.value
    : undefined;
}

function copyOptional(
  payload: ProjectionPayload,
  value: object,
  property: string,
  type: "boolean" | "number" | "string",
): boolean {
  const candidate = readOwnProperty(value, property);

  if (
    candidate.kind === "missing" ||
    (candidate.kind === "value" && candidate.value === undefined)
  ) {
    return true;
  }

  if (candidate.kind !== "value" || typeof candidate.value !== type) {
    return false;
  }

  payload[property] = candidate.value;
  return true;
}

function readOwnProperty(value: object, property: string): OwnProperty {
  const descriptor = Object.getOwnPropertyDescriptor(value, property);

  if (descriptor === undefined) {
    return { kind: "missing" };
  }

  return "value" in descriptor ? { kind: "value", value: descriptor.value } : { kind: "accessor" };
}

function isObject(value: unknown): value is object {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function reportProjectionFailure(
  category: "normalization" | "serialization",
  nativeEvent: AcceptedObservationNativeEvent | undefined,
  sessionId: unknown,
  reportDiagnostic: ProjectionDiagnosticReporter,
): void {
  const safeSessionId = boundedDiagnosticIdentity(sessionId);
  const diagnostic = Object.freeze({
    category,
    ...(nativeEvent === undefined ? {} : { nativeEvent }),
    ...(safeSessionId === undefined ? {} : { sessionId: safeSessionId }),
  });

  try {
    reportDiagnostic(diagnostic);
  } catch {}
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

export function classifyNativeEvent(nativeEvent: NativeEventIdentity): NativeEventSelection {
  try {
    switch (nativeEvent.type) {
      case "session_start":
        return { kind: "lifecycle", lifecycle: "start", nativeEvent: "session_start" };
      case "session_shutdown":
        return { kind: "lifecycle", lifecycle: "end", nativeEvent: "session_shutdown" };
      case "session_info_changed":
      case "session_compact":
      case "session_compact_failed":
      case "agent_start":
      case "agent_settled":
      case "ui_prompt_start":
      case "ui_prompt_end":
      case "turn_start":
      case "turn_end":
      case "tool_execution_start":
      case "tool_execution_end":
        return { kind: "observation", nativeEvent: nativeEvent.type };
      case "message_end": {
        const messageRole = readNativeMessageRole(nativeEvent);

        return messageRole === "user" || messageRole === "assistant"
          ? { kind: "observation", nativeEvent: "message_end" }
          : { kind: "excluded" };
      }
      case "input":
      case "before_agent_start":
      case "agent_end":
      case "message_start":
      case "message_update":
      case "tool_execution_update":
      case "context":
      case "project_trust":
      case "resources_discover":
      case "session_before_switch":
      case "session_before_fork":
      case "session_before_compact":
      case "session_before_tree":
      case "session_tree":
      case "cache_warming_decision":
      case "thinking_level_select":
      case "user_bash":
      case "before_provider_request":
      case "before_provider_headers":
      case "after_provider_response":
      case "tool_call":
      case "tool_result":
        return { kind: "excluded" };
      default:
        return { kind: "excluded" };
    }
  } catch {
    return { kind: "excluded" };
  }
}

function readNativeMessageRole(nativeEvent: NativeEventIdentity): unknown {
  try {
    const message = readOwnDataProperty(nativeEvent, "message");

    return message !== null && typeof message === "object"
      ? readOwnDataProperty(message, "role")
      : undefined;
  } catch {
    return undefined;
  }
}

function readOwnDataProperty(value: object, property: string): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(value, property);
  return descriptor !== undefined && "value" in descriptor ? descriptor.value : undefined;
}

export function deriveEventMetadata(
  context: AdapterContext,
  nativeTimestamp: unknown,
  fallbackTimestamp: string | undefined,
): EventMetadata | undefined {
  try {
    const timestamp = toIsoTimestamp(nativeTimestamp) ?? fallbackTimestamp;

    if (timestamp === undefined) {
      return undefined;
    }

    const sessionManager = context.sessionManager;
    const currentWorkingDirectory = context.cwd;

    if (
      sessionManager === undefined ||
      typeof sessionManager.getSessionId !== "function" ||
      typeof currentWorkingDirectory !== "string" ||
      !isAbsolute(currentWorkingDirectory)
    ) {
      return undefined;
    }

    const sessionId = sessionManager.getSessionId();
    const projectName = basename(currentWorkingDirectory);

    if (
      typeof sessionId !== "string" ||
      sessionId.trim().length === 0 ||
      projectName.length === 0
    ) {
      return undefined;
    }

    return { sessionId, projectName, currentWorkingDirectory, timestamp };
  } catch {
    return undefined;
  }
}

export function captureHandlerEntryTimestamp(clock: HandlerClock): string | undefined {
  try {
    return toIsoTimestamp(clock.now());
  } catch {
    return undefined;
  }
}

export function selectObservation(
  nativeEvent: NativeEventIdentity,
  context: AdapterContext,
  nativeTimestamp: unknown,
  fallbackTimestamp: string | undefined,
): ObservationSelection | undefined {
  const selection = classifyNativeEvent(nativeEvent);

  if (selection.kind !== "observation") {
    return undefined;
  }

  const metadata = deriveEventMetadata(context, nativeTimestamp, fallbackTimestamp);

  if (metadata === undefined) {
    return undefined;
  }

  return {
    kind: "observation",
    metadata,
    nativeEvent: selection.nativeEvent,
    hookType: `pi.${selection.nativeEvent}`,
  };
}

function toIsoTimestamp(value: unknown): string | undefined {
  try {
    const milliseconds =
      value instanceof Date
        ? value.getTime()
        : typeof value === "number" || typeof value === "string"
          ? new Date(value).getTime()
          : Number.NaN;

    return Number.isFinite(milliseconds) ? new Date(milliseconds).toISOString() : undefined;
  } catch {
    return undefined;
  }
}
