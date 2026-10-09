import assert from "node:assert/strict";
import test from "node:test";

import { projectNativeEvent } from "../src/adapter.ts";
import type { NativeObservationEnvelope } from "../src/adapter.ts";
import type { Diagnostic } from "../src/model.ts";

const sessionId = "session-123";
const sentinels = {
  credential: "credential-sentinel",
  environment: "environment-sentinel",
  image: "image-sentinel",
  provider: "provider-sentinel",
  system: "system-prompt-sentinel",
  thinking: "thinking-sentinel",
  toolCall: "embedded-tool-call-sentinel",
  toolResult: "embedded-tool-result-sentinel",
};

const usage = {
  cacheRead: 3,
  cacheWrite: 4,
  cacheWrite1h: 5,
  cost: { cacheRead: 0.003, cacheWrite: 0.004, input: 0.001, output: 0.002, total: 0.01 },
  input: 1,
  output: 2,
  reasoning: 6,
  totalTokens: 21,
};

const assistant = { api: "messages", model: "model-id", provider: "provider-id", usage };

interface ProjectionCase {
  readonly expected: NativeObservationEnvelope;
  readonly nativeEvent: unknown;
}

function envelope(
  event: NativeObservationEnvelope["event"],
  payload: NativeObservationEnvelope["payload"],
): NativeObservationEnvelope {
  return { source: "pi", version: "1.0.0", event, payload };
}

function assertNoProhibitedContent(value: unknown): void {
  const serialized = JSON.stringify(value) ?? "";

  for (const sentinel of Object.values(sentinels)) {
    assert.equal(serialized.includes(sentinel), false, sentinel);
  }
}

const projectionCases: readonly ProjectionCase[] = [
  {
    nativeEvent: { type: "session_info_changed", name: "project session" },
    expected: envelope("session_info_changed", { name: "project session" }),
  },
  {
    nativeEvent: {
      type: "session_compact",
      compactionEntry: {
        details: { credential: sentinels.credential },
        firstKeptEntryId: "entry-2",
        fromHook: true,
        id: "compaction-1",
        parentId: "entry-1",
        summary: sentinels.system,
        systemMessage: { content: sentinels.system, role: "system" },
        timestamp: "2026-09-21T12:34:56.789Z",
        tokensBefore: 42,
      },
      fromExtension: false,
      reason: "threshold",
      willRetry: true,
    },
    expected: envelope("session_compact", {
      compactionEntry: {
        firstKeptEntryId: "entry-2",
        fromHook: true,
        id: "compaction-1",
        parentId: "entry-1",
        timestamp: "2026-09-21T12:34:56.789Z",
        tokensBefore: 42,
      },
      fromExtension: false,
      reason: "threshold",
      willRetry: true,
    }),
  },
  {
    nativeEvent: {
      type: "session_compact_failed",
      aborted: false,
      errorMessage: "compaction failed",
      fromExtension: true,
      providerResponse: sentinels.provider,
      reason: "overflow",
      willRetry: true,
    },
    expected: envelope("session_compact_failed", {
      aborted: false,
      errorMessage: "compaction failed",
      fromExtension: true,
      reason: "overflow",
      willRetry: true,
    }),
  },
  {
    nativeEvent: {
      type: "session_compact_failed",
      aborted: true,
      fromExtension: false,
      reason: "manual",
      willRetry: false,
    },
    expected: envelope("session_compact_failed", {
      aborted: true,
      fromExtension: false,
      reason: "manual",
      willRetry: false,
    }),
  },
  {
    nativeEvent: { type: "agent_start", environment: sentinels.environment },
    expected: envelope("agent_start", {}),
  },
  {
    nativeEvent: { type: "agent_settled", environment: sentinels.environment },
    expected: envelope("agent_settled", {}),
  },
  {
    nativeEvent: {
      type: "ui_prompt_start",
      kind: "confirm",
      reason: "ui_prompt",
      title: "Continue?",
    },
    expected: envelope("ui_prompt_start", {
      kind: "confirm",
      reason: "ui_prompt",
      title: "Continue?",
    }),
  },
  {
    nativeEvent: { type: "ui_prompt_end", kind: "editor", reason: "ui_prompt" },
    expected: envelope("ui_prompt_end", { kind: "editor", reason: "ui_prompt" }),
  },
  {
    nativeEvent: { type: "turn_start", timestamp: 1_726_922_096_789, turnIndex: 7 },
    expected: envelope("turn_start", { timestamp: 1_726_922_096_789, turnIndex: 7 }),
  },
  {
    nativeEvent: {
      type: "turn_end",
      message: { content: [{ text: sentinels.toolCall, type: "text" }], role: "assistant" },
      toolResults: [
        { content: [{ text: sentinels.toolResult, type: "text" }], role: "toolResult" },
      ],
      turnIndex: 7,
    },
    expected: envelope("turn_end", { turnIndex: 7 }),
  },
  {
    nativeEvent: {
      type: "message_end",
      message: {
        content: [
          { text: "allowed user text", type: "text" },
          { data: sentinels.image, mimeType: "image/png", type: "image" },
        ],
        role: "user",
        timestamp: 1_726_922_097_000,
      },
    },
    expected: envelope("message_end", {
      role: "user",
      text: "allowed user text",
      timestamp: 1_726_922_097_000,
    }),
  },
  {
    nativeEvent: {
      type: "message_end",
      message: {
        content: "allowed direct user text",
        role: "user",
        timestamp: 1_726_922_097_500,
      },
    },
    expected: envelope("message_end", {
      role: "user",
      text: "allowed direct user text",
      timestamp: 1_726_922_097_500,
    }),
  },
  {
    nativeEvent: {
      type: "message_end",
      message: {
        ...assistant,
        content: [
          { text: "allowed assistant text", type: "text" },
          { thinking: sentinels.thinking, type: "thinking" },
          {
            arguments: { value: sentinels.toolCall },
            id: "call-1",
            name: "tool",
            type: "toolCall",
          },
        ],
        deferred: { data: { providerPayload: sentinels.provider }, id: "deferred" },
        errorMessage: "assistant error message",
        responseId: "response-id-sentinel",
        responseModel: "response-model-id",
        role: "assistant",
        stopReason: "stop",
        timestamp: 1_726_922_098_000,
      },
    },
    expected: envelope("message_end", {
      ...assistant,
      errorMessage: "assistant error message",
      responseModel: "response-model-id",
      role: "assistant",
      stopReason: "stop",
      text: "allowed assistant text",
      timestamp: 1_726_922_098_000,
    }),
  },
  {
    nativeEvent: {
      type: "message_end",
      message: {
        ...assistant,
        content: [{ text: "partial response", type: "text" }],
        errorMessage: "provider-independent error",
        role: "assistant",
        stopReason: "error",
        timestamp: 1_726_922_099_000,
      },
    },
    expected: envelope("message_end", {
      ...assistant,
      errorMessage: "provider-independent error",
      role: "assistant",
      stopReason: "error",
      text: "partial response",
      timestamp: 1_726_922_099_000,
    }),
  },
  {
    nativeEvent: {
      type: "message_end",
      message: {
        ...assistant,
        content: [],
        role: "assistant",
        stopReason: "aborted",
        timestamp: 1_726_922_100_000,
      },
    },
    expected: envelope("message_end", {
      ...assistant,
      role: "assistant",
      stopReason: "aborted",
      text: "",
      timestamp: 1_726_922_100_000,
    }),
  },
  {
    nativeEvent: {
      type: "tool_execution_start",
      args: { nested: [null, true], query: "allowed tool arguments" },
      environment: sentinels.environment,
      toolCallId: "call-1",
      toolName: "fixture-tool",
    },
    expected: envelope("tool_execution_start", {
      args: { nested: [null, true], query: "allowed tool arguments" },
      toolCallId: "call-1",
      toolName: "fixture-tool",
    }),
  },
  {
    nativeEvent: {
      type: "tool_execution_end",
      isError: true,
      result: { output: "allowed tool result" },
      toolCallId: "call-1",
      toolName: "fixture-tool",
    },
    expected: envelope("tool_execution_end", {
      isError: true,
      result: { output: "allowed tool result" },
      toolCallId: "call-1",
      toolName: "fixture-tool",
    }),
  },
];

test("projects allowed native fields and excludes prohibited content", () => {
  const diagnostics: Diagnostic[] = [];

  for (const projectionCase of projectionCases) {
    const actual = projectNativeEvent(projectionCase.nativeEvent, sessionId, (diagnostic): void => {
      diagnostics.push(diagnostic);
    });

    assert.deepEqual(actual, projectionCase.expected);
    assertNoProhibitedContent(actual);
  }

  assert.deepEqual(diagnostics, []);
});

test("excludes lifecycle, provider, system, input, interception, and streamed data", () => {
  const excluded: readonly unknown[] = [
    { type: "session_start", reason: "startup" },
    { type: "session_shutdown", reason: "quit" },
    { type: "before_agent_start", systemPrompt: sentinels.system },
    { type: "before_provider_request", payload: sentinels.provider },
    { type: "before_provider_headers", headers: { authorization: sentinels.credential } },
    { type: "after_provider_response", headers: { response: sentinels.provider } },
    { type: "context", messages: [{ content: sentinels.system, role: "system" }] },
    { type: "input", images: [{ data: sentinels.image, type: "image" }] },
    { type: "message_end", message: { content: sentinels.system, role: "system" } },
    { type: "message_end", message: { content: sentinels.toolResult, role: "toolResult" } },
    { type: "message_update", assistantMessageEvent: { delta: sentinels.thinking } },
    { type: "tool_execution_update", partialResult: sentinels.toolResult },
    { type: "tool_call", value: sentinels.toolCall },
    { type: "tool_result", value: sentinels.toolResult },
  ];
  const diagnostics: Diagnostic[] = [];

  for (const event of excluded) {
    assert.equal(
      projectNativeEvent(event, sessionId, (diagnostic): void => {
        diagnostics.push(diagnostic);
      }),
      undefined,
    );
  }

  assert.deepEqual(diagnostics, []);
});

test("rejects malformed assistant string content with one payload-safe diagnostic", () => {
  const diagnostics: Diagnostic[] = [];

  assert.equal(
    projectNativeEvent(
      {
        type: "message_end",
        message: {
          ...assistant,
          content: sentinels.thinking,
          role: "assistant",
          stopReason: "stop",
          timestamp: 1_726_922_098_500,
        },
      },
      sessionId,
      (diagnostic): void => {
        diagnostics.push(diagnostic);
      },
    ),
    undefined,
  );

  assert.deepEqual(diagnostics, [
    { category: "normalization", nativeEvent: "message_end", sessionId },
  ]);
  const diagnostic = diagnostics[0];
  assert.ok(diagnostic);
  assert.equal(Object.isFrozen(diagnostic), true);
  assert.equal(Reflect.set(diagnostic, "payload", sentinels.thinking), false);
  assert.deepEqual(Reflect.ownKeys(diagnostic), ["category", "nativeEvent", "sessionId"]);
  assertNoProhibitedContent(diagnostics);
});

test("contains malformed and unrepresentable projections with fresh identity-only diagnostics", () => {
  const diagnostics: Diagnostic[] = [];
  const report = (diagnostic: Diagnostic): void => {
    diagnostics.push(diagnostic);
  };
  let accessorRead = false;
  const accessorTurn = Object.defineProperty({ timestamp: 1, type: "turn_start" }, "turnIndex", {
    enumerable: true,
    get(): never {
      accessorRead = true;
      throw new Error(sentinels.toolResult);
    },
  });
  const cyclic: { credential: string; self?: unknown } = { credential: sentinels.credential };
  cyclic.self = cyclic;
  const failures: readonly [unknown, unknown][] = [
    [{ timestamp: 1, type: "turn_start" }, sessionId],
    [accessorTurn, sessionId],
    [
      { args: cyclic, toolCallId: "call-1", toolName: "tool", type: "tool_execution_start" },
      sessionId,
    ],
    [
      {
        isError: true,
        result: Number.NaN,
        toolCallId: "call-1",
        toolName: "tool",
        type: "tool_execution_end",
      },
      sessionId,
    ],
    [{ timestamp: 1, type: "turn_start" }, `${sentinels.environment}/path`],
  ];

  for (const [event, identity] of failures) {
    assert.equal(projectNativeEvent(event, identity, report), undefined);
  }

  assert.equal(accessorRead, false);
  assert.deepEqual(diagnostics, [
    { category: "normalization", nativeEvent: "turn_start", sessionId },
    { category: "normalization", nativeEvent: "turn_start", sessionId },
    { category: "serialization", nativeEvent: "tool_execution_start", sessionId },
    { category: "serialization", nativeEvent: "tool_execution_end", sessionId },
    { category: "normalization", nativeEvent: "turn_start" },
  ]);
  assert.equal(Object.isFrozen(diagnostics[0]), true);
  assert.equal(Object.isFrozen(diagnostics[1]), true);
  assert.notStrictEqual(diagnostics[0], diagnostics[1]);
  assert.equal(Reflect.set(diagnostics[0] ?? {}, "payload", sentinels.toolResult), false);
  assert.deepEqual(Reflect.ownKeys(diagnostics[0] ?? {}), ["category", "nativeEvent", "sessionId"]);
  assertNoProhibitedContent(diagnostics);

  assert.doesNotThrow(() =>
    projectNativeEvent({ timestamp: 1, type: "turn_start" }, sessionId, () => {
      throw new Error(sentinels.credential);
    }),
  );
});
