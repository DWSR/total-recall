import assert from "node:assert/strict";
import test from "node:test";

import { contextResultDecoder } from "../src/context-model.ts";
import type {
  ContextDecodeOutcome,
  ContextDiagnosticCategory,
  ContextFailureCategory,
  ContextMemory,
  ContextResultDecoder,
  FrozenSnapshot,
} from "../src/context-model.ts";

const payloadSentinel = "protected-memory-sentinel";

const failureCategories = [
  "context_spawn_failed",
  "context_startup_timeout",
  "context_protocol_failed",
  "context_lookup_failed",
  "context_lookup_timeout",
  "context_shutdown_failed",
] as const satisfies readonly ContextFailureCategory[];

const diagnosticCategories = [
  ...failureCategories,
  "context_configuration_invalid",
  "context_session_invalid",
  "context_session_conflict",
  "context_format_failed",
] as const satisfies readonly ContextDiagnosticCategory[];

type IsExactly<Left, Right> = [Left] extends [Right]
  ? [Right] extends [Left]
    ? true
    : false
  : false;

const failureCategoryContract: IsExactly<
  ContextFailureCategory,
  (typeof failureCategories)[number]
> = true;
const diagnosticCategoryContract: IsExactly<
  ContextDiagnosticCategory,
  (typeof diagnosticCategories)[number]
> = true;
const memoryKeyContract: IsExactly<keyof ContextMemory, "type" | "title" | "content" | "concepts"> =
  true;
const frozenKindContract: IsExactly<FrozenSnapshot["kind"], "text" | "empty"> = true;
const decodeKindContract: IsExactly<ContextDecodeOutcome["kind"], "context" | "invalid"> = true;
const decoderContract: ContextResultDecoder = contextResultDecoder;
void failureCategoryContract;
void diagnosticCategoryContract;
void memoryKeyContract;
void frozenKindContract;
void decodeKindContract;
void decoderContract;

const validMemories = [
  {
    type: "preference",
    title: "Editor layout",
    content: "The operator prefers two-space indentation.",
    concepts: ["editor", "style"],
  },
  {
    type: "fact",
    title: "Workspace",
    content: "The repository is a Rust workspace with a Pi extension.",
    concepts: [],
  },
] as const;

function decodeInvalid(value: unknown, limit = 10): void {
  let outcome: ContextDecodeOutcome | undefined;

  assert.doesNotThrow(() => {
    outcome = contextResultDecoder.decode(value, limit);
  }, "decode must not throw for malformed input");

  assert.deepEqual(outcome, { kind: "invalid" });
  assert.equal(Object.isFrozen(outcome), true);
}

interface MutableMemory {
  type: string;
  title: string;
  content: string;
  concepts: string[];
}

function withGetter(
  target: Readonly<Record<string, unknown>>,
  key: string,
  calls: { count: number },
): Record<string, unknown> {
  const record: Record<string, unknown> = { ...target };

  Object.defineProperty(record, key, {
    configurable: true,
    enumerable: true,
    get: () => {
      calls.count += 1;
      return payloadSentinel;
    },
  });

  return record;
}

test("decodes exact four-field records in input order without aliasing the source", () => {
  const source: { memories: MutableMemory[] } = {
    memories: validMemories.map((memory) => ({ ...memory, concepts: [...memory.concepts] })),
  };
  const outcome = contextResultDecoder.decode(source, 10);

  assert.equal(outcome.kind, "context");

  if (outcome.kind !== "context") {
    return;
  }

  assert.deepEqual(outcome.memories, validMemories);
  assert.deepEqual(Object.keys(outcome.memories[0] ?? {}), [
    "type",
    "title",
    "content",
    "concepts",
  ]);
  assert.deepEqual(Object.keys(outcome.memories[1] ?? {}), [
    "type",
    "title",
    "content",
    "concepts",
  ]);

  for (const memory of outcome.memories) {
    assert.equal(Object.isFrozen(memory), true);
    assert.equal(Object.isFrozen(memory.concepts), true);
  }

  assert.equal(Object.isFrozen(outcome), true);
  assert.equal(Object.isFrozen(outcome.memories), true);

  source.memories.length = 0;

  assert.deepEqual(outcome.memories, validMemories);
});

test("decodes a record with its fields in any source order", () => {
  const outcome = contextResultDecoder.decode(
    {
      memories: [
        {
          concepts: ["editor"],
          content: "Uses tabs",
          title: "Editor",
          type: "preference",
        },
      ],
    },
    1,
  );

  assert.equal(outcome.kind, "context");

  if (outcome.kind !== "context") {
    return;
  }

  assert.deepEqual(outcome.memories, [
    { type: "preference", title: "Editor", content: "Uses tabs", concepts: ["editor"] },
  ]);
});

test("decodes an empty memories array as a complete context result", () => {
  const outcome = contextResultDecoder.decode({ memories: [] }, 1);

  assert.equal(outcome.kind, "context");

  if (outcome.kind !== "context") {
    return;
  }

  assert.deepEqual(outcome.memories, []);
  assert.equal(Object.isFrozen(outcome.memories), true);
});

test("rejects every malformed root as one complete invalid result", () => {
  const derivedWithoutOwnMemories = Object.create({ memories: [] });
  const nonEnumerableRoot: Record<string, unknown> = {};
  const symbolRoot = { memories: [] };

  Object.defineProperty(nonEnumerableRoot, "memories", {
    configurable: true,
    enumerable: false,
    value: [],
  });
  Object.defineProperty(symbolRoot, Symbol(payloadSentinel), { enumerable: true, value: "hidden" });

  const invalidRoots: readonly { readonly name: string; readonly value: unknown }[] = [
    { name: "undefined root", value: undefined },
    { name: "null root", value: null },
    { name: "boolean root", value: true },
    { name: "number root", value: 1 },
    { name: "string root", value: payloadSentinel },
    { name: "bigint root", value: 1n },
    { name: "symbol root", value: Symbol(payloadSentinel) },
    { name: "function root", value: () => payloadSentinel },
    { name: "array root", value: [{ memories: [] }] },
    { name: "empty object root", value: {} },
    { name: "missing memories", value: { values: [] } },
    { name: "extra root field", value: { memories: [], limit: 1 } },
    { name: "memories null", value: { memories: null } },
    { name: "memories object", value: { memories: {} } },
    { name: "memories string", value: { memories: payloadSentinel } },
    { name: "inherited memories", value: derivedWithoutOwnMemories },
    { name: "non-enumerable memories", value: nonEnumerableRoot },
    { name: "symbol-keyed root", value: symbolRoot },
    { name: "date root", value: new Date("2026-09-25T00:00:00.000Z") },
    { name: "map root", value: new Map([["memories", []]]) },
    { name: "set root", value: new Set([payloadSentinel]) },
    { name: "string memories array", value: ["memories"] },
    { name: "array of records root", value: validMemories },
  ];

  for (const invalidRoot of invalidRoots) {
    decodeInvalid(invalidRoot.value);
  }
});

test("rejects accessor-backed roots without invoking getters", () => {
  const calls = { count: 0 };
  const accessorRoot = withGetter({}, "memories", calls);

  decodeInvalid(accessorRoot);
  assert.equal(calls.count, 0);

  const inheritedAccessor = Object.create(accessorRoot);
  decodeInvalid(inheritedAccessor);
  assert.equal(calls.count, 0);
});

test("rejects records with missing, unknown, inherited, or accessor-backed fields", () => {
  const calls = { count: 0 };
  const inheritedRecord = Object.create(validMemories[0]);
  const nonEnumerableRecord: Record<string, unknown> = { ...validMemories[0] };
  const symbolRecord = { ...validMemories[0] };

  Object.defineProperty(nonEnumerableRecord, "content", {
    configurable: true,
    enumerable: false,
    value: payloadSentinel,
  });
  Object.defineProperty(symbolRecord, Symbol(payloadSentinel), {
    enumerable: true,
    value: payloadSentinel,
  });

  const invalidRecords: readonly { readonly name: string; readonly value: unknown }[] = [
    { name: "null record", value: null },
    { name: "string record", value: payloadSentinel },
    { name: "array record", value: [payloadSentinel] },
    { name: "inherited record", value: inheritedRecord },
    { name: "non-enumerable field", value: nonEnumerableRecord },
    { name: "symbol-keyed record", value: symbolRecord },
    { name: "date record", value: new Date("2026-09-25T00:00:00.000Z") },
    { name: "missing type", value: { title: "t", content: "c", concepts: [] } },
    { name: "missing title", value: { type: "t", content: "c", concepts: [] } },
    { name: "missing content", value: { type: "t", title: "t", concepts: [] } },
    { name: "missing concepts", value: { type: "t", title: "t", content: "c" } },
    {
      name: "unknown field",
      value: { type: "t", title: "t", content: "c", concepts: [], score: 1 },
    },
    {
      name: "identity field",
      value: { type: "t", title: "t", content: "c", concepts: [], id: payloadSentinel },
    },
    { name: "numeric type", value: { type: 1, title: "t", content: "c", concepts: [] } },
    { name: "null title", value: { type: "t", title: null, content: "c", concepts: [] } },
    {
      name: "object content",
      value: { type: "t", title: "t", content: { text: payloadSentinel }, concepts: [] },
    },
    { name: "string concepts", value: { type: "t", title: "t", content: "c", concepts: "c" } },
    { name: "null concepts", value: { type: "t", title: "t", content: "c", concepts: null } },
    {
      name: "non-string concept",
      value: { type: "t", title: "t", content: "c", concepts: ["ok", 2] },
    },
    {
      name: "object concept",
      value: { type: "t", title: "t", content: "c", concepts: [{ value: payloadSentinel }] },
    },
    {
      name: "accessor type",
      value: withGetter(validMemories[0], "type", calls),
    },
    {
      name: "accessor concepts",
      value: withGetter(validMemories[0], "concepts", calls),
    },
  ];

  for (const invalidRecord of invalidRecords) {
    decodeInvalid({ memories: [invalidRecord.value] }, 1);
  }

  assert.equal(calls.count, 0);
});

test("rejects sparse or augmented concept arrays without invoking getters", () => {
  const calls = { count: 0 };
  const sparseConcepts = new Array<unknown>(1);
  const extraMember = ["valid"];
  const hiddenMember = ["valid"];
  const accessorMember = ["valid"];
  const symbolMember = ["valid"];

  Object.defineProperty(extraMember, "unexpected", {
    enumerable: true,
    value: payloadSentinel,
  });
  Object.defineProperty(hiddenMember, "unexpected", {
    enumerable: false,
    value: payloadSentinel,
  });
  Object.defineProperty(accessorMember, "unexpected", {
    enumerable: true,
    get: () => {
      calls.count += 1;
      return payloadSentinel;
    },
  });
  Object.defineProperty(symbolMember, Symbol(payloadSentinel), {
    enumerable: true,
    value: "valid",
  });

  const invalidCases: readonly { readonly name: string; readonly value: unknown }[] = [
    { name: "sparse concepts", value: sparseConcepts },
    { name: "extra member", value: extraMember },
    { name: "hidden member", value: hiddenMember },
    { name: "accessor member", value: accessorMember },
    { name: "symbol member", value: symbolMember },
  ];

  for (const invalidCase of invalidCases) {
    decodeInvalid(
      { memories: [{ type: "t", title: "t", content: "c", concepts: invalidCase.value }] },
      1,
    );
  }

  assert.equal(calls.count, 0);
});

test("rejects sparse or augmented memories arrays", () => {
  const sparseMemories = new Array<unknown>(1);
  const extraMember = [validMemories[0]];
  const symbolMember = [validMemories[0]];

  Object.defineProperty(extraMember, "unexpected", {
    enumerable: false,
    value: payloadSentinel,
  });
  Object.defineProperty(symbolMember, Symbol(payloadSentinel), {
    enumerable: true,
    value: "hidden",
  });

  decodeInvalid({ memories: sparseMemories }, 1);
  decodeInvalid({ memories: extraMember }, 1);
  decodeInvalid({ memories: symbolMember }, 1);
});

test("rejects the complete result when any record is malformed", () => {
  decodeInvalid(
    {
      memories: [
        validMemories[0],
        { type: "fact", title: "Bad", content: "value", concepts: [] },
        { type: "fact", title: "Bad", content: "value", concepts: [], extra: true },
      ],
    },
    3,
  );

  decodeInvalid(
    { memories: [validMemories[0], { type: "fact", title: "Bad", content: "value" }] },
    2,
  );
});

test("enforces the requested record limit as an all-or-invalid bound", () => {
  const outcome = contextResultDecoder.decode({ memories: validMemories }, 2);

  assert.equal(outcome.kind, "context");

  decodeInvalid({ memories: validMemories }, 1);
  decodeInvalid({ memories: validMemories }, 0);
  decodeInvalid({ memories: [{ ...validMemories[0], extra: true }] }, 20);

  const emptyOutcome = contextResultDecoder.decode({ memories: [] }, 0);

  assert.equal(emptyOutcome.kind, "context");
});

test("rejects an unusable record limit as a complete invalid result", () => {
  const invalidLimits = [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, "2" as unknown as number];

  for (const limit of invalidLimits) {
    decodeInvalid({ memories: [] }, limit);
  }
});

test("defines the closed diagnostic, failure, frozen, and decode contracts", () => {
  const snapshots: readonly FrozenSnapshot[] = [
    { kind: "text", sectionBody: "startup memory body" },
    { kind: "empty" },
  ];
  const memory: ContextMemory = validMemories[0];

  assert.deepEqual(failureCategories, [
    "context_spawn_failed",
    "context_startup_timeout",
    "context_protocol_failed",
    "context_lookup_failed",
    "context_lookup_timeout",
    "context_shutdown_failed",
  ]);
  assert.deepEqual(diagnosticCategories, [
    "context_spawn_failed",
    "context_startup_timeout",
    "context_protocol_failed",
    "context_lookup_failed",
    "context_lookup_timeout",
    "context_shutdown_failed",
    "context_configuration_invalid",
    "context_session_invalid",
    "context_session_conflict",
    "context_format_failed",
  ]);
  assert.deepEqual(
    snapshots.map((snapshot) => snapshot.kind),
    ["text", "empty"],
  );
  assert.deepEqual(Object.keys(memory), ["type", "title", "content", "concepts"]);
  assert.equal(typeof contextResultDecoder.decode, "function");
});
