import assert from "node:assert/strict";
import test from "node:test";

import {
  captureHandlerEntryTimestamp,
  classifyNativeEvent,
  deriveEventMetadata,
  selectObservation,
} from "../src/adapter.ts";
import type { AdapterContext, NativeEventIdentity, NativeEventSelection } from "../src/adapter.ts";

const handlerEntryTimestamp = "2026-09-21T12:34:56.123Z";
const nativeTimestamp = "2026-09-20T10:11:12.345Z";
const payloadSentinel = "adapter-payload-sentinel";

const context: AdapterContext = {
  cwd: "/workspace/example-project",
  sessionManager: {
    getSessionId: () => "session-123",
  },
};

const eventMatrix: readonly {
  readonly candidate: NativeEventIdentity;
  readonly expected: NativeEventSelection;
}[] = [
  {
    candidate: { type: "session_start" },
    expected: { kind: "lifecycle", lifecycle: "start", nativeEvent: "session_start" },
  },
  {
    candidate: { type: "session_shutdown" },
    expected: { kind: "lifecycle", lifecycle: "end", nativeEvent: "session_shutdown" },
  },
  {
    candidate: { type: "session_info_changed" },
    expected: { kind: "observation", nativeEvent: "session_info_changed" },
  },
  {
    candidate: { type: "session_compact" },
    expected: { kind: "observation", nativeEvent: "session_compact" },
  },
  {
    candidate: { type: "session_compact_failed" },
    expected: { kind: "observation", nativeEvent: "session_compact_failed" },
  },
  {
    candidate: { type: "agent_start" },
    expected: { kind: "observation", nativeEvent: "agent_start" },
  },
  {
    candidate: { type: "agent_settled" },
    expected: { kind: "observation", nativeEvent: "agent_settled" },
  },
  {
    candidate: { type: "ui_prompt_start" },
    expected: { kind: "observation", nativeEvent: "ui_prompt_start" },
  },
  {
    candidate: { type: "ui_prompt_end" },
    expected: { kind: "observation", nativeEvent: "ui_prompt_end" },
  },
  {
    candidate: { type: "turn_start" },
    expected: { kind: "observation", nativeEvent: "turn_start" },
  },
  { candidate: { type: "turn_end" }, expected: { kind: "observation", nativeEvent: "turn_end" } },
  {
    candidate: { type: "message_end", message: { role: "user" } },
    expected: { kind: "observation", nativeEvent: "message_end" },
  },
  {
    candidate: { type: "message_end", message: { role: "assistant" } },
    expected: { kind: "observation", nativeEvent: "message_end" },
  },
  {
    candidate: { type: "tool_execution_start" },
    expected: { kind: "observation", nativeEvent: "tool_execution_start" },
  },
  {
    candidate: { type: "tool_execution_end" },
    expected: { kind: "observation", nativeEvent: "tool_execution_end" },
  },
  { candidate: { type: "input" }, expected: { kind: "excluded" } },
  { candidate: { type: "before_agent_start" }, expected: { kind: "excluded" } },
  { candidate: { type: "agent_end" }, expected: { kind: "excluded" } },
  { candidate: { type: "message_start" }, expected: { kind: "excluded" } },
  { candidate: { type: "message_update" }, expected: { kind: "excluded" } },
  { candidate: { type: "tool_execution_update" }, expected: { kind: "excluded" } },
  { candidate: { type: "context" }, expected: { kind: "excluded" } },
  { candidate: { type: "project_trust" }, expected: { kind: "excluded" } },
  { candidate: { type: "resources_discover" }, expected: { kind: "excluded" } },
  { candidate: { type: "session_before_switch" }, expected: { kind: "excluded" } },
  { candidate: { type: "session_before_fork" }, expected: { kind: "excluded" } },
  { candidate: { type: "session_before_compact" }, expected: { kind: "excluded" } },
  { candidate: { type: "session_before_tree" }, expected: { kind: "excluded" } },
  { candidate: { type: "session_tree" }, expected: { kind: "excluded" } },
  { candidate: { type: "cache_warming_decision" }, expected: { kind: "excluded" } },
  { candidate: { type: "thinking_level_select" }, expected: { kind: "excluded" } },
  { candidate: { type: "user_bash" }, expected: { kind: "excluded" } },
  { candidate: { type: "before_provider_request" }, expected: { kind: "excluded" } },
  { candidate: { type: "before_provider_headers" }, expected: { kind: "excluded" } },
  { candidate: { type: "after_provider_response" }, expected: { kind: "excluded" } },
  { candidate: { type: "tool_call" }, expected: { kind: "excluded" } },
  { candidate: { type: "tool_result" }, expected: { kind: "excluded" } },
  {
    candidate: { type: "message_end", message: { role: "system" } },
    expected: { kind: "excluded" },
  },
  {
    candidate: { type: "message_end", message: { role: "toolResult" } },
    expected: { kind: "excluded" },
  },
  {
    candidate: { type: "message_end", message: { role: "unknown" } },
    expected: { kind: "excluded" },
  },
  { candidate: { type: "message_end", message: {} }, expected: { kind: "excluded" } },
  { candidate: { type: "message_end" }, expected: { kind: "excluded" } },
  { candidate: { type: "model_select" }, expected: { kind: "excluded" } },
  { candidate: { type: "unknown_future_event" }, expected: { kind: "excluded" } },
];

test("classifies every accepted and excluded native event matrix row", () => {
  for (const row of eventMatrix) {
    assert.deepEqual(classifyNativeEvent(row.candidate), row.expected, row.candidate.type);
  }
});

test("accepts native-shaped user and assistant message_end observations", () => {
  for (const role of ["user", "assistant"] as const) {
    assert.deepEqual(
      classifyNativeEvent({ type: "message_end", message: { role } }),
      { kind: "observation", nativeEvent: "message_end" },
      role,
    );
  }
});

test("excludes malformed and accessor-backed message_end messages without leaking payloads", () => {
  let messageGetterRead = false;
  let roleGetterRead = false;
  const diagnostics: unknown[][] = [];
  const originalError = console.error;
  const messageGetterEvent = Object.defineProperty({ type: "message_end" }, "message", {
    get(): never {
      messageGetterRead = true;
      throw new Error(payloadSentinel);
    },
  }) as unknown as NativeEventIdentity;
  const roleGetterMessage = Object.defineProperty({}, "role", {
    get(): never {
      roleGetterRead = true;
      throw new Error(payloadSentinel);
    },
  });
  const malformedEvents: readonly NativeEventIdentity[] = [
    { type: "message_end" },
    { type: "message_end", message: null } as unknown as NativeEventIdentity,
    { type: "message_end", message: {} },
    messageGetterEvent,
    { type: "message_end", message: roleGetterMessage } as unknown as NativeEventIdentity,
  ];

  console.error = (...values: unknown[]): void => {
    diagnostics.push(values);
  };

  try {
    for (const nativeEvent of malformedEvents) {
      assert.deepEqual(classifyNativeEvent(nativeEvent), { kind: "excluded" });
    }
  } finally {
    console.error = originalError;
  }

  assert.equal(messageGetterRead, false);
  assert.equal(roleGetterRead, false);
  assert.equal(diagnostics.length, 0);
  assert.doesNotMatch(JSON.stringify(diagnostics), new RegExp(payloadSentinel));
});

test("keeps lifecycle events separate from observation selections", () => {
  const sessionStart = classifyNativeEvent({ type: "session_start" });
  const sessionShutdown = classifyNativeEvent({ type: "session_shutdown" });

  assert.equal(sessionStart.kind, "lifecycle");
  assert.equal(sessionShutdown.kind, "lifecycle");
  assert.equal(
    selectObservation({ type: "session_start" }, context, nativeTimestamp, handlerEntryTimestamp),
    undefined,
  );
  assert.equal(
    selectObservation(
      { type: "session_shutdown" },
      context,
      nativeTimestamp,
      handlerEntryTimestamp,
    ),
    undefined,
  );
});

test("derives session metadata and project basename from the supplied context", () => {
  assert.deepEqual(deriveEventMetadata(context, nativeTimestamp, handlerEntryTimestamp), {
    sessionId: "session-123",
    projectName: "example-project",
    currentWorkingDirectory: "/workspace/example-project",
    timestamp: nativeTimestamp,
  });
});

test("preserves representable native timestamps and falls back to the handler-entry clock", () => {
  const timestampCases: readonly {
    readonly name: string;
    readonly source: unknown;
    readonly expected: string;
  }[] = [
    {
      name: "native millisecond timestamp",
      source: Date.parse("2026-09-20T10:11:12.345Z"),
      expected: "2026-09-20T10:11:12.345Z",
    },
    {
      name: "native RFC3339 timestamp",
      source: "2026-09-20T12:11:12.345+02:00",
      expected: "2026-09-20T10:11:12.345Z",
    },
    {
      name: "invalid native timestamp",
      source: "not-a-timestamp",
      expected: handlerEntryTimestamp,
    },
    {
      name: "unrepresentable native timestamp",
      source: Number.POSITIVE_INFINITY,
      expected: handlerEntryTimestamp,
    },
  ];

  for (const timestampCase of timestampCases) {
    const metadata = deriveEventMetadata(context, timestampCase.source, handlerEntryTimestamp);
    assert.equal(metadata?.timestamp, timestampCase.expected, timestampCase.name);
  }
});

test("captures the handler-entry clock before supplied metadata context reads", () => {
  const timestampCases: readonly {
    readonly name: string;
    readonly source: unknown;
    readonly expected: string;
  }[] = [
    { name: "native timestamp", source: nativeTimestamp, expected: nativeTimestamp },
    { name: "fallback timestamp", source: undefined, expected: handlerEntryTimestamp },
  ];

  for (const timestampCase of timestampCases) {
    const accessOrder: string[] = [];
    const fallbackTimestamp = captureHandlerEntryTimestamp({
      now: () => {
        accessOrder.push("clock");
        return new Date(handlerEntryTimestamp);
      },
    });
    const metadata = deriveEventMetadata(
      {
        get cwd(): string {
          accessOrder.push("cwd");
          return "/workspace/example-project";
        },
        get sessionManager(): { readonly getSessionId: () => unknown } {
          accessOrder.push("sessionManager");
          return {
            getSessionId: () => {
              accessOrder.push("sessionId");
              return "session-123";
            },
          };
        },
      },
      timestampCase.source,
      fallbackTimestamp,
    );

    const clockIndex = accessOrder.indexOf("clock");
    assert.equal(metadata?.timestamp, timestampCase.expected, timestampCase.name);
    assert.ok(clockIndex >= 0, `${timestampCase.name}: clock was not read`);
    assert.ok(
      clockIndex < accessOrder.indexOf("sessionManager"),
      `${timestampCase.name}: clock followed session access`,
    );
    assert.ok(
      clockIndex < accessOrder.indexOf("cwd"),
      `${timestampCase.name}: clock followed cwd access`,
    );
  }
});

test("preserves a valid native timestamp when the handler-entry clock fails", () => {
  let clockRead = false;
  const fallbackTimestamp = captureHandlerEntryTimestamp({
    now: () => {
      clockRead = true;
      throw new Error(payloadSentinel);
    },
  });
  const metadata = deriveEventMetadata(context, nativeTimestamp, fallbackTimestamp);

  assert.equal(clockRead, true);
  assert.equal(metadata?.timestamp, nativeTimestamp);
});

test("retains accepted native identity in the observation hook type", () => {
  assert.deepEqual(
    selectObservation(
      { type: "tool_execution_end" },
      context,
      nativeTimestamp,
      handlerEntryTimestamp,
    ),
    {
      kind: "observation",
      nativeEvent: "tool_execution_end",
      hookType: "pi.tool_execution_end",
      metadata: {
        sessionId: "session-123",
        projectName: "example-project",
        currentWorkingDirectory: "/workspace/example-project",
        timestamp: nativeTimestamp,
      },
    },
  );
});

test("never selects streamed, provider, interception, or system events as observations", () => {
  for (const row of eventMatrix) {
    if (row.expected.kind === "observation") {
      continue;
    }

    assert.equal(
      selectObservation(row.candidate, context, payloadSentinel, handlerEntryTimestamp),
      undefined,
      row.candidate.type,
    );
  }
});

test("skips missing or invalid metadata without payload-bearing diagnostics", () => {
  const diagnostics: unknown[][] = [];
  const originalError = console.error;
  console.error = (...values: unknown[]): void => {
    diagnostics.push(values);
  };

  try {
    assert.equal(
      deriveEventMetadata(
        {
          cwd: "/workspace/example-project",
          sessionManager: { getSessionId: () => undefined },
        },
        payloadSentinel,
        handlerEntryTimestamp,
      ),
      undefined,
    );
    assert.equal(
      deriveEventMetadata(
        { sessionManager: { getSessionId: () => "session-123" } },
        payloadSentinel,
        handlerEntryTimestamp,
      ),
      undefined,
    );
    assert.equal(
      deriveEventMetadata(
        {
          cwd: "relative/project",
          sessionManager: { getSessionId: () => "session-123" },
        },
        payloadSentinel,
        handlerEntryTimestamp,
      ),
      undefined,
    );
    assert.equal(
      deriveEventMetadata(
        {
          cwd: "/workspace/example-project",
          sessionManager: {
            getSessionId: () => {
              throw new Error(payloadSentinel);
            },
          },
        },
        payloadSentinel,
        handlerEntryTimestamp,
      ),
      undefined,
    );
    assert.equal(
      deriveEventMetadata(
        context,
        payloadSentinel,
        captureHandlerEntryTimestamp({ now: () => new Date(Number.NaN) }),
      ),
      undefined,
    );
    assert.equal(
      deriveEventMetadata(
        context,
        payloadSentinel,
        captureHandlerEntryTimestamp({
          now: () => {
            throw new Error(payloadSentinel);
          },
        }),
      ),
      undefined,
    );
  } finally {
    console.error = originalError;
  }

  assert.equal(diagnostics.length, 0);
  assert.doesNotMatch(JSON.stringify(diagnostics), new RegExp(payloadSentinel));
});
