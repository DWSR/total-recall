export interface ContextMemory {
  readonly type: string;
  readonly title: string;
  readonly content: string;
  readonly concepts: readonly string[];
}

export type FrozenSnapshot =
  | { readonly kind: "text"; readonly sectionBody: string }
  | { readonly kind: "empty" };

export type ContextFailureCategory =
  | "context_spawn_failed"
  | "context_startup_timeout"
  | "context_protocol_failed"
  | "context_lookup_failed"
  | "context_lookup_timeout"
  | "context_shutdown_failed";

export type ContextDiagnosticCategory =
  | ContextFailureCategory
  | "context_configuration_invalid"
  | "context_session_invalid"
  | "context_session_conflict"
  | "context_format_failed";

export type ContextDecodeOutcome =
  | { readonly kind: "context"; readonly memories: readonly ContextMemory[] }
  | { readonly kind: "invalid" };

export interface ContextResultDecoder {
  decode(value: unknown, limit: number): ContextDecodeOutcome;
}

const MEMORY_FIELDS = ["type", "title", "content", "concepts"] as const;
const RESULT_FIELDS = ["memories"] as const;

const INVALID_OUTCOME: ContextDecodeOutcome = Object.freeze({ kind: "invalid" });

export const contextResultDecoder: ContextResultDecoder = Object.freeze({
  decode: decodeContextResult,
});

function decodeContextResult(value: unknown, limit: number): ContextDecodeOutcome {
  try {
    if (!Number.isSafeInteger(limit) || limit < 0) {
      return INVALID_OUTCOME;
    }

    const resultFields = readExactRecord(value, RESULT_FIELDS);

    if (resultFields === undefined) {
      return INVALID_OUTCOME;
    }

    const records = readDenseArray(resultFields[0]);

    if (records === undefined || records.length > limit) {
      return INVALID_OUTCOME;
    }

    const memories: ContextMemory[] = [];

    for (const record of records) {
      const memory = readMemory(record);

      if (memory === undefined) {
        return INVALID_OUTCOME;
      }

      memories.push(memory);
    }

    return Object.freeze({ kind: "context", memories: Object.freeze(memories) });
  } catch {
    return INVALID_OUTCOME;
  }
}

function readMemory(value: unknown): ContextMemory | undefined {
  const fields = readExactRecord(value, MEMORY_FIELDS);

  if (fields === undefined) {
    return undefined;
  }

  const type = fields[0];
  const title = fields[1];
  const content = fields[2];
  const conceptValues = readDenseArray(fields[3]);

  if (
    typeof type !== "string" ||
    typeof title !== "string" ||
    typeof content !== "string" ||
    conceptValues === undefined
  ) {
    return undefined;
  }

  const concepts: string[] = [];

  for (const concept of conceptValues) {
    if (typeof concept !== "string") {
      return undefined;
    }

    concepts.push(concept);
  }

  return Object.freeze({
    type,
    title,
    content,
    concepts: Object.freeze(concepts),
  });
}

function readExactRecord(value: unknown, keys: readonly string[]): readonly unknown[] | undefined {
  if (!isPlainRecord(value) || hasSymbolProperty(value)) {
    return undefined;
  }

  const names = Object.getOwnPropertyNames(value);

  if (names.length !== keys.length) {
    return undefined;
  }

  const fields: unknown[] = [];

  for (const key of keys) {
    if (!names.includes(key)) {
      return undefined;
    }

    const descriptor = Object.getOwnPropertyDescriptor(value, key);

    if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
      return undefined;
    }

    fields.push(descriptor.value);
  }

  return fields;
}

function readDenseArray(value: unknown): readonly unknown[] | undefined {
  if (!Array.isArray(value) || hasSymbolProperty(value)) {
    return undefined;
  }

  const lengthDescriptor = Object.getOwnPropertyDescriptor(value, "length");

  if (
    lengthDescriptor === undefined ||
    !("value" in lengthDescriptor) ||
    typeof lengthDescriptor.value !== "number" ||
    !Number.isSafeInteger(lengthDescriptor.value) ||
    lengthDescriptor.value < 0
  ) {
    return undefined;
  }

  const length = lengthDescriptor.value;

  if (Object.getOwnPropertyNames(value).length !== length + 1) {
    return undefined;
  }

  const items: unknown[] = [];

  for (let index = 0; index < length; index += 1) {
    const descriptor = Object.getOwnPropertyDescriptor(value, String(index));

    if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
      return undefined;
    }

    items.push(descriptor.value);
  }

  return items;
}

function isPlainRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }

  const prototype = Object.getPrototypeOf(value);

  return prototype === Object.prototype || prototype === null;
}

function hasSymbolProperty(value: object): boolean {
  return Object.getOwnPropertySymbols(value).length > 0;
}
