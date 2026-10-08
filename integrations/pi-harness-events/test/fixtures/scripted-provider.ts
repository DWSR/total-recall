import {
  createAssistantMessageEventStream,
  createProvider,
  Type,
} from "@earendil-works/pi-ai/compat";
import type { ExtensionFactory, ModelRuntime } from "@earendil-works/pi-coding-agent";

type FixtureProvider = Parameters<ModelRuntime["registerNativeProvider"]>[0];
type FixtureStream = FixtureProvider["streamSimple"];
type FixtureModel = Parameters<FixtureStream>[0];
type FixtureTranscript = Parameters<FixtureStream>[1];
type FixtureEventStream = ReturnType<FixtureStream>;
type FixtureAssistantMessage = Awaited<ReturnType<FixtureEventStream["result"]>>;
type FixtureToolCall = Extract<FixtureAssistantMessage["content"][number], { type: "toolCall" }>;
type FixtureApiKeyAuth = NonNullable<FixtureProvider["auth"]["apiKey"]>;

const providerId = "pi-harness-events-scripted";
const modelId = "scripted-fixture-v1";
const toolName = "fixture_tool";
const toolCallId = "scripted-fixture-tool-call";
const timestamp = 1_726_922_096_789;
const usage = {
  cacheRead: 0,
  cacheWrite: 0,
  cost: {
    cacheRead: 0,
    cacheWrite: 0,
    input: 0,
    output: 0,
    total: 0,
  },
  input: 0,
  output: 0,
  totalTokens: 0,
};
const fixtureToolCall: FixtureToolCall = {
  arguments: {},
  id: toolCallId,
  name: toolName,
  type: "toolCall",
};

function assistantMessage(
  content: FixtureAssistantMessage["content"],
  stopReason: "stop" | "toolUse",
): FixtureAssistantMessage {
  return {
    api: providerId,
    content,
    model: modelId,
    provider: providerId,
    role: "assistant",
    stopReason,
    timestamp,
    usage,
  };
}

function hasSuccessfulFixtureResult(context: FixtureTranscript): boolean {
  return context.messages.some(
    (message) =>
      message.role === "toolResult" &&
      message.toolCallId === toolCallId &&
      message.toolName === toolName &&
      message.isError === false,
  );
}

function createScriptedStream(
  _model: FixtureModel,
  context: FixtureTranscript,
): FixtureEventStream {
  const message = hasSuccessfulFixtureResult(context)
    ? assistantMessage([{ text: "fixture complete", type: "text" }], "stop")
    : assistantMessage([fixtureToolCall], "toolUse");
  const stream = createAssistantMessageEventStream();

  stream.push({
    partial: { ...message, content: [], stopReason: "pending" },
    type: "start",
  });

  if (message.stopReason === "toolUse") {
    const toolCall = message.content[0];

    if (toolCall === undefined || toolCall.type !== "toolCall") {
      throw new Error("scripted fixture tool call is unavailable");
    }

    const partial: FixtureAssistantMessage = {
      ...message,
      content: [toolCall],
      stopReason: "pending",
    };
    stream.push({ contentIndex: 0, partial, type: "toolcall_start" });
    stream.push({ contentIndex: 0, partial, toolCall, type: "toolcall_end" });
    stream.push({ message, reason: "toolUse", type: "done" });
  } else {
    const text = message.content[0];

    if (text === undefined || text.type !== "text") {
      throw new Error("scripted fixture text is unavailable");
    }

    const partial: FixtureAssistantMessage = {
      ...message,
      content: [{ text: text.text, type: "text" }],
      stopReason: "pending",
    };
    stream.push({
      contentIndex: 0,
      partial: { ...partial, content: [{ text: "", type: "text" }] },
      type: "text_start",
    });
    stream.push({ contentIndex: 0, delta: text.text, partial, type: "text_delta" });
    stream.push({ content: text.text, contentIndex: 0, partial, type: "text_end" });
    stream.push({ message, reason: "stop", type: "done" });
  }

  stream.end(message);
  return stream;
}

const checkFixtureAuth: NonNullable<FixtureApiKeyAuth["check"]> = async ({ signal }) => {
  signal.throwIfAborted();
  return { source: "scripted fixture", type: "api_key" };
};

const resolveFixtureAuth: FixtureApiKeyAuth["resolve"] = async ({ signal }) => {
  signal.throwIfAborted();
  return { auth: {}, source: "scripted fixture" };
};

const scriptedProvider: FixtureProvider = createProvider({
  api: {
    stream: createScriptedStream,
    streamSimple: createScriptedStream,
  },
  auth: {
    apiKey: {
      check: checkFixtureAuth,
      name: "Scripted fixture",
      resolve: resolveFixtureAuth,
    },
  },
  id: providerId,
  models: [
    {
      api: providerId,
      baseUrl: "fixture://scripted",
      contextWindow: 8_192,
      cost: { cacheRead: 0, cacheWrite: 0, input: 0, output: 0 },
      id: modelId,
      input: ["text"],
      maxTokens: 256,
      name: "Scripted fixture",
      provider: providerId,
      reasoning: false,
    },
  ],
});

const extension: ExtensionFactory = (pi) => {
  pi.registerProvider(scriptedProvider);
  pi.registerTool({
    description: "Returns the deterministic scripted fixture result.",
    execute: async () => ({
      content: [{ text: "fixture tool result", type: "text" }],
      details: { fixture: "scripted", status: "success" },
    }),
    label: "Fixture tool",
    name: toolName,
    parameters: Type.Object({}),
  });
};

export default extension;
