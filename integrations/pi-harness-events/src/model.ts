export type JsonPrimitive = string | number | boolean | null;

export type JsonValue = JsonPrimitive | JsonObject | readonly JsonValue[];

export interface JsonObject {
  readonly [key: string]: JsonValue;
}

export interface EventMetadata {
  readonly sessionId: string;
  readonly projectName: string;
  readonly currentWorkingDirectory: string;
  readonly timestamp: string;
}

export type Submission =
  | { readonly kind: "start"; readonly metadata: EventMetadata }
  | {
      readonly kind: "observation";
      readonly metadata: EventMetadata;
      readonly hookType: string;
      readonly data: JsonObject;
    }
  | { readonly kind: "end"; readonly metadata: EventMetadata };

export type DispatchFailureCategory =
  | "configuration"
  | "normalization"
  | "serialization"
  | "queue-overflow"
  | "spawn"
  | "timeout"
  | "usage"
  | "connection"
  | "invocation"
  | "signal"
  | "unknown-exit";

export type DispatchResult =
  | { readonly ok: true }
  | { readonly ok: false; readonly category: DispatchFailureCategory };

export interface Diagnostic {
  readonly category: DispatchFailureCategory;
  readonly nativeEvent?: string;
  readonly sessionId?: string;
}

export function normalizeJsonObject(value: unknown): JsonObject {
  try {
    if (!isPlainObject(value)) {
      throw new Error("Observation data must be an object");
    }

    return normalizeObject(value, new Set<object>());
  } catch {
    throw new Error("Observation data is not JSON-compatible");
  }
}

function normalizeJsonValue(value: unknown, ancestors: Set<object>): JsonValue {
  if (value === null || typeof value === "boolean" || typeof value === "string") {
    return value;
  }

  if (typeof value === "number") {
    if (Number.isFinite(value)) {
      return value;
    }

    throw new Error("Non-finite numbers are not JSON-compatible");
  }

  if (Array.isArray(value)) {
    return normalizeArray(value, ancestors);
  }

  if (isPlainObject(value)) {
    return normalizeObject(value, ancestors);
  }

  throw new Error("Unsupported JSON value");
}

function normalizeArray(value: readonly unknown[], ancestors: Set<object>): readonly JsonValue[] {
  const lengthDescriptor = Object.getOwnPropertyDescriptor(value, "length");

  if (
    ancestors.has(value) ||
    hasSymbolProperty(value) ||
    lengthDescriptor === undefined ||
    !("value" in lengthDescriptor) ||
    typeof lengthDescriptor.value !== "number" ||
    !Number.isInteger(lengthDescriptor.value) ||
    lengthDescriptor.value < 0 ||
    !hasOnlyArrayIndexes(value, lengthDescriptor.value)
  ) {
    throw new Error("Unsupported JSON array");
  }

  ancestors.add(value);

  try {
    const normalized: JsonValue[] = [];

    for (let index = 0; index < lengthDescriptor.value; index += 1) {
      const descriptor = Object.getOwnPropertyDescriptor(value, String(index));

      if (descriptor === undefined || !("value" in descriptor)) {
        throw new Error("Unsupported JSON array item");
      }

      const item: unknown = descriptor.value;
      normalized.push(normalizeJsonValue(item, ancestors));
    }

    return normalized;
  } finally {
    ancestors.delete(value);
  }
}

function normalizeObject(
  value: Readonly<Record<string, unknown>>,
  ancestors: Set<object>,
): JsonObject {
  if (ancestors.has(value) || hasSymbolProperty(value)) {
    throw new Error("Unsupported JSON object");
  }

  ancestors.add(value);

  try {
    const normalized: Record<string, JsonValue> = {};

    for (const key of Object.getOwnPropertyNames(value)) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);

      if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
        throw new Error("Unsupported JSON object property");
      }

      const propertyValue: unknown = descriptor.value;
      const normalizedValue = normalizeJsonValue(propertyValue, ancestors);

      Object.defineProperty(normalized, key, {
        configurable: true,
        enumerable: true,
        value: normalizedValue,
        writable: true,
      });
    }

    return normalized;
  } finally {
    ancestors.delete(value);
  }
}

function isPlainObject(value: unknown): value is Readonly<Record<string, unknown>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }

  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function hasSymbolProperty(value: object): boolean {
  return Object.getOwnPropertySymbols(value).length > 0;
}

function hasOnlyArrayIndexes(value: readonly unknown[], length: number): boolean {
  return Object.getOwnPropertyNames(value).every((key) => {
    if (key === "length") {
      return true;
    }

    const index = Number(key);
    return Number.isInteger(index) && index >= 0 && index < length && String(index) === key;
  });
}
