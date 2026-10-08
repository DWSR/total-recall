import assert from "node:assert/strict";
import test from "node:test";

import { normalizeJsonObject } from "../src/model.ts";
import type {
  Diagnostic,
  DispatchFailureCategory,
  DispatchResult,
  EventMetadata,
  JsonObject,
  JsonPrimitive,
  JsonValue,
  Submission,
} from "../src/model.ts";

const metadata: EventMetadata = {
  sessionId: "session-123",
  projectName: "project",
  currentWorkingDirectory: "/workspace/project",
  timestamp: "2026-09-21T12:34:56.789Z",
};

const payloadSentinel = "sensitive-payload-marker";

function assertPayloadSafeRejection(value: unknown, name: string): void {
  assert.throws(
    () => normalizeJsonObject(value),
    (error: Error) => {
      assert.doesNotMatch(error.message, new RegExp(payloadSentinel));
      return true;
    },
    name,
  );
}

const failureCategories = [
  "configuration",
  "normalization",
  "serialization",
  "queue-overflow",
  "spawn",
  "timeout",
  "usage",
  "connection",
  "invocation",
  "signal",
  "unknown-exit",
] as const satisfies readonly DispatchFailureCategory[];

type IsExactly<Left, Right> = [Left] extends [Right]
  ? [Right] extends [Left]
    ? true
    : false
  : false;

const failureCategoryContract: IsExactly<
  DispatchFailureCategory,
  (typeof failureCategories)[number]
> = true;
const diagnosticKeyContract: IsExactly<keyof Diagnostic, "category" | "nativeEvent" | "sessionId"> =
  true;
void failureCategoryContract;
void diagnosticKeyContract;

test("normalizes valid nested JSON-compatible observation data", () => {
  const primitive: JsonPrimitive = false;
  const value: JsonValue = {
    nested: [primitive, null, { message: "nested value" }],
  };
  const source: JsonObject = {
    primitive,
    value,
    nested: {
      values: [0, -1.5, null, { valid: "content" }],
    },
  };

  const normalized = normalizeJsonObject(source);

  assert.deepEqual(normalized, source);
  assert.notStrictEqual(normalized, source);
  assert.notStrictEqual(normalized.nested, source.nested);
});

test("rejects unsupported and cyclic nested observation data without payload errors", () => {
  const cyclic: { self?: unknown; readonly secret: string } = { secret: payloadSentinel };
  cyclic.self = cyclic;

  const invalidCases: readonly { readonly name: string; readonly value: unknown }[] = [
    { name: "undefined", value: { secret: undefined } },
    { name: "non-finite number", value: { secret: Number.NaN } },
    { name: "infinite number", value: { secret: Number.POSITIVE_INFINITY } },
    { name: "bigint", value: { secret: 1n } },
    { name: "symbol", value: { secret: Symbol(payloadSentinel) } },
    { name: "function", value: { secret: () => payloadSentinel } },
    { name: "date", value: { secret: new Date("2026-09-21T00:00:00.000Z") } },
    { name: "map", value: { secret: new Map([["value", payloadSentinel]]) } },
    { name: "cycle", value: cyclic },
  ];

  for (const invalidCase of invalidCases) {
    assertPayloadSafeRejection(invalidCase.value, invalidCase.name);
  }
});

test("rejects every non-object and exotic observation root without payload errors", () => {
  const invalidRoots: readonly { readonly name: string; readonly value: unknown }[] = [
    { name: "undefined root", value: undefined },
    { name: "null root", value: null },
    { name: "boolean root", value: true },
    { name: "finite number root", value: 1 },
    { name: "non-finite number root", value: Number.NaN },
    { name: "string root", value: payloadSentinel },
    { name: "bigint root", value: 1n },
    { name: "symbol root", value: Symbol(payloadSentinel) },
    { name: "function root", value: () => payloadSentinel },
    { name: "array root", value: [payloadSentinel] },
    { name: "date root", value: new Date("2026-09-21T00:00:00.000Z") },
    { name: "map root", value: new Map([["value", payloadSentinel]]) },
    { name: "set root", value: new Set([payloadSentinel]) },
    { name: "regular expression root", value: new RegExp(payloadSentinel) },
  ];

  for (const invalidRoot of invalidRoots) {
    assertPayloadSafeRejection(invalidRoot.value, invalidRoot.name);
  }
});

test("rejects sparse and augmented arrays without invoking getters", () => {
  let getterCalls = 0;
  const sparse = new Array<unknown>(1);
  const enumerableMember = ["valid"];
  const hiddenMember = ["valid"];
  const enumerableAccessor = ["valid"];
  const hiddenAccessor = ["valid"];
  const enumerableSymbol = ["valid"];
  const hiddenSymbol = ["valid"];

  Object.defineProperty(enumerableMember, "unexpected", {
    enumerable: true,
    value: payloadSentinel,
  });
  Object.defineProperty(hiddenMember, "unexpected", {
    enumerable: false,
    value: payloadSentinel,
  });
  Object.defineProperty(enumerableAccessor, "unexpected", {
    enumerable: true,
    get: () => {
      getterCalls += 1;
      return payloadSentinel;
    },
  });
  Object.defineProperty(hiddenAccessor, "unexpected", {
    enumerable: false,
    get: () => {
      getterCalls += 1;
      return payloadSentinel;
    },
  });
  Object.defineProperty(enumerableSymbol, Symbol(payloadSentinel), {
    enumerable: true,
    value: "valid",
  });
  Object.defineProperty(hiddenSymbol, Symbol(payloadSentinel), {
    enumerable: false,
    value: "valid",
  });

  const invalidCases: readonly { readonly name: string; readonly value: unknown }[] = [
    { name: "sparse array", value: sparse },
    { name: "array with enumerable non-index member", value: enumerableMember },
    { name: "array with hidden non-index member", value: hiddenMember },
    { name: "array with enumerable non-index accessor", value: enumerableAccessor },
    { name: "array with hidden non-index accessor", value: hiddenAccessor },
    { name: "array with enumerable symbol", value: enumerableSymbol },
    { name: "array with hidden symbol", value: hiddenSymbol },
  ];

  for (const invalidCase of invalidCases) {
    assertPayloadSafeRejection({ values: invalidCase.value }, invalidCase.name);
  }

  assert.equal(getterCalls, 0);
});

test("rejects non-enumerable JSON-compatible object members without payload errors", () => {
  const source: Record<string, unknown> = {};

  Object.defineProperty(source, "hidden", {
    enumerable: false,
    value: {
      nested: [payloadSentinel, { valid: true }],
    },
  });

  assertPayloadSafeRejection(source, "non-enumerable JSON-compatible object member");
});

test("rejects object members hidden from enumerable-key inspection without invoking getters", () => {
  let getterCalls = 0;
  const enumerableAccessor: Record<string, unknown> = {};
  const hiddenAccessor: Record<string, unknown> = {};
  const enumerableFunction: Record<string, unknown> = {};
  const hiddenFunction: Record<string, unknown> = {};
  const enumerableSymbol: Record<string, unknown> = {};
  const hiddenSymbol: Record<string, unknown> = {};
  const enumerableUndefined: Record<string, unknown> = {};
  const hiddenUndefined: Record<string, unknown> = {};

  Object.defineProperty(enumerableAccessor, "unexpected", {
    enumerable: true,
    get: () => {
      getterCalls += 1;
      return payloadSentinel;
    },
  });
  Object.defineProperty(hiddenAccessor, "unexpected", {
    enumerable: false,
    get: () => {
      getterCalls += 1;
      return payloadSentinel;
    },
  });
  Object.defineProperty(enumerableFunction, "unexpected", {
    enumerable: true,
    value: () => payloadSentinel,
  });
  Object.defineProperty(hiddenFunction, "unexpected", {
    enumerable: false,
    value: () => payloadSentinel,
  });
  Object.defineProperty(enumerableSymbol, Symbol(payloadSentinel), {
    enumerable: true,
    value: "valid",
  });
  Object.defineProperty(hiddenSymbol, Symbol(payloadSentinel), {
    enumerable: false,
    value: "valid",
  });
  Object.defineProperty(enumerableUndefined, "unexpected", {
    enumerable: true,
    value: undefined,
  });
  Object.defineProperty(hiddenUndefined, "unexpected", {
    enumerable: false,
    value: undefined,
  });

  const invalidCases: readonly { readonly name: string; readonly value: unknown }[] = [
    { name: "enumerable accessor", value: enumerableAccessor },
    { name: "hidden accessor", value: hiddenAccessor },
    { name: "enumerable function", value: enumerableFunction },
    { name: "hidden function", value: hiddenFunction },
    { name: "enumerable symbol", value: enumerableSymbol },
    { name: "hidden symbol", value: hiddenSymbol },
    { name: "enumerable unsupported value", value: enumerableUndefined },
    { name: "hidden unsupported value", value: hiddenUndefined },
  ];

  for (const invalidCase of invalidCases) {
    assertPayloadSafeRejection(invalidCase.value, invalidCase.name);
  }

  assert.equal(getterCalls, 0);
});

test("defines closed submissions, dispatch outcomes, and identity-only diagnostics", () => {
  const data: JsonObject = {
    nested: {
      values: ["preserved", true],
    },
  };
  const submissions: readonly Submission[] = [
    { kind: "start", metadata },
    { kind: "observation", metadata, hookType: "pi.turn_end", data },
    { kind: "end", metadata },
  ];
  const results: readonly DispatchResult[] = [{ ok: true }, { ok: false, category: "timeout" }];
  const diagnostic: Diagnostic = {
    category: "normalization",
    nativeEvent: "pi.turn_end",
    sessionId: metadata.sessionId,
  };

  assert.deepEqual(failureCategories, [
    "configuration",
    "normalization",
    "serialization",
    "queue-overflow",
    "spawn",
    "timeout",
    "usage",
    "connection",
    "invocation",
    "signal",
    "unknown-exit",
  ]);
  assert.deepEqual(
    submissions.map((submission) => submission.kind),
    ["start", "observation", "end"],
  );
  assert.deepEqual(results, [{ ok: true }, { ok: false, category: "timeout" }]);
  assert.deepEqual(Object.keys(diagnostic), ["category", "nativeEvent", "sessionId"]);
});
