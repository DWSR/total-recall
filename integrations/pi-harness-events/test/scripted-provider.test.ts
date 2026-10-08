import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  discoverAndLoadExtensions,
  ModelRuntime,
  type CreateModelRuntimeOptions,
} from "@earendil-works/pi-coding-agent";

type CredentialStore = NonNullable<CreateModelRuntimeOptions["credentials"]>;
type StoredCredential = NonNullable<Awaited<ReturnType<CredentialStore["read"]>>>;
type RuntimeContext = Parameters<ModelRuntime["streamSimple"]>[1];
type RuntimeMessage = RuntimeContext["messages"][number];

const packageRoot = fileURLToPath(new URL("../", import.meta.url));
const fixturePath = fileURLToPath(new URL("./fixtures/scripted-provider.ts", import.meta.url));
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
const fixtureResult = {
  content: [{ text: "fixture tool result", type: "text" }],
  details: { fixture: "scripted", status: "success" },
};
const toolCallAssistant: RuntimeMessage = {
  api: providerId,
  content: [{ arguments: {}, id: toolCallId, name: toolName, type: "toolCall" }],
  model: modelId,
  provider: providerId,
  role: "assistant",
  stopReason: "toolUse",
  timestamp,
  usage,
};
const finalAssistant: RuntimeMessage = {
  api: providerId,
  content: [{ text: "fixture complete", type: "text" }],
  model: modelId,
  provider: providerId,
  role: "assistant",
  stopReason: "stop",
  timestamp,
  usage,
};
const pendingToolCallAssistant: RuntimeMessage = {
  api: providerId,
  content: [{ arguments: {}, id: toolCallId, name: toolName, type: "toolCall" }],
  model: modelId,
  provider: providerId,
  role: "assistant",
  stopReason: "pending",
  timestamp,
  usage,
};
const pendingFinalAssistant: RuntimeMessage = {
  api: providerId,
  content: [{ text: "fixture complete", type: "text" }],
  model: modelId,
  provider: providerId,
  role: "assistant",
  stopReason: "pending",
  timestamp,
  usage,
};
const successfulToolResult: RuntimeMessage = {
  content: [{ text: "fixture tool result", type: "text" }],
  details: { fixture: "scripted", status: "success" },
  isError: false,
  role: "toolResult",
  timestamp,
  toolCallId,
  toolName,
};

function createInMemoryCredentials(): CredentialStore {
  const credentials = new Map<string, StoredCredential>();

  return {
    delete: async (provider) => {
      credentials.delete(provider);
    },
    list: async () =>
      [...credentials.entries()].map(([providerId, credential]) => ({
        providerId,
        type: credential.type,
      })),
    modify: async (provider, update) => {
      const credential = await update(credentials.get(provider));

      if (credential !== undefined) {
        credentials.set(provider, credential);
      }

      return credentials.get(provider);
    },
    read: async (provider) => credentials.get(provider),
  };
}

function eventTypes<T extends { type: string }>(events: readonly T[]): string[] {
  return events.map((event) => event.type);
}

async function collectEvents<T>(stream: AsyncIterable<T>): Promise<T[]> {
  const events: T[] = [];

  for await (const event of stream) {
    events.push(event);
  }

  return events;
}

test("loads the deterministic scripted fixture through Pi and emits stable offline turns", async () => {
  const loaded = await discoverAndLoadExtensions([fixturePath], packageRoot, packageRoot);

  assert.deepEqual(loaded.errors, []);
  assert.equal(loaded.extensions.length, 1);
  assert.equal(loaded.runtime.pendingNativeProviderRegistrations.length, 1);

  const extension = loaded.extensions[0];
  const providerRegistration = loaded.runtime.pendingNativeProviderRegistrations[0];
  assert.ok(extension);
  assert.ok(providerRegistration);
  assert.deepEqual([...extension.tools.keys()], [toolName]);
  assert.equal(providerRegistration.provider.id, providerId);
  assert.deepEqual(
    providerRegistration.provider.getModels().map((model) => ({ cost: model.cost, id: model.id })),
    [{ cost: { cacheRead: 0, cacheWrite: 0, input: 0, output: 0 }, id: modelId }],
  );

  const authContext = {
    env: async (): Promise<never> => {
      throw new Error("fixture auth must not read environment variables");
    },
    fileExists: async (): Promise<never> => {
      throw new Error("fixture auth must not read files");
    },
  };
  const apiKeyAuth = providerRegistration.provider.auth.apiKey;
  assert.ok(apiKeyAuth);
  assert.ok(apiKeyAuth.check);
  assert.deepEqual(
    await apiKeyAuth.check({ ctx: authContext, signal: new AbortController().signal }),
    { source: "scripted fixture", type: "api_key" },
  );
  assert.deepEqual(
    await apiKeyAuth.resolve({ ctx: authContext, signal: new AbortController().signal }),
    { auth: {}, source: "scripted fixture" },
  );

  const modelRuntime = await ModelRuntime.create({
    credentials: createInMemoryCredentials(),
    modelsPath: null,
    refreshOnCreate: false,
  });
  modelRuntime.registerNativeProvider(providerRegistration.provider);
  assert.equal(modelRuntime.getRegisteredNativeProvider(providerId), providerRegistration.provider);

  const model = modelRuntime.getModel(providerId, modelId);
  assert.ok(model);
  const firstContext: RuntimeContext = {
    messages: [{ content: "run the fixture", role: "user", timestamp }],
    systemPrompt: "fixture test",
  };
  const secondContext: RuntimeContext = {
    messages: [...firstContext.messages, toolCallAssistant, successfulToolResult],
  };

  const firstEvents = await collectEvents(modelRuntime.streamSimple(model, firstContext));
  assert.deepEqual(eventTypes(firstEvents), ["start", "toolcall_start", "toolcall_end", "done"]);
  assert.deepEqual(firstEvents.at(-1), {
    message: toolCallAssistant,
    reason: "toolUse",
    type: "done",
  });
  assert.deepEqual(firstEvents[2], {
    contentIndex: 0,
    partial: pendingToolCallAssistant,
    toolCall: { arguments: {}, id: toolCallId, name: toolName, type: "toolCall" },
    type: "toolcall_end",
  });

  const secondEvents = await collectEvents(modelRuntime.streamSimple(model, secondContext));
  assert.deepEqual(eventTypes(secondEvents), [
    "start",
    "text_start",
    "text_delta",
    "text_end",
    "done",
  ]);
  assert.deepEqual(secondEvents.at(-1), {
    message: finalAssistant,
    reason: "stop",
    type: "done",
  });
  assert.deepEqual(secondEvents[3], {
    content: "fixture complete",
    contentIndex: 0,
    partial: pendingFinalAssistant,
    type: "text_end",
  });

  const tool = extension.tools.get(toolName);
  assert.ok(tool);
  assert.deepEqual(
    await Reflect.apply(tool.definition.execute, undefined, [
      toolCallId,
      {},
      undefined,
      undefined,
      undefined,
    ]),
    fixtureResult,
  );

  assert.deepEqual(
    await collectEvents(modelRuntime.streamSimple(model, firstContext)),
    firstEvents,
  );
  assert.deepEqual(
    await collectEvents(modelRuntime.streamSimple(model, secondContext)),
    secondEvents,
  );

  const source = await readFile(fixturePath, "utf8");
  assert.match(source, /from\s+["']@earendil-works\/pi-ai\/compat["']/u);
  assert.doesNotMatch(source, /from\s+["']@earendil-works\/pi-ai["']/u);
  assert.doesNotMatch(source, /from\s+["']node:/u, "Node I/O imports");
  assert.doesNotMatch(source, /\bfetch\b/u, "network requests");
  assert.doesNotMatch(source, /\bprocess\b/u, "ambient process access");
  assert.doesNotMatch(
    source,
    /\b(?:readFile|writeFile|readdir|statSync|existsSync)\b/u,
    "filesystem access",
  );
  assert.doesNotMatch(source, /\b(?:spawn|execFile|exec|fork)\b/u, "subprocess access");
  assert.doesNotMatch(source, /\bDate\s*\.\s*now\b/u, "wall-clock access");
  assert.doesNotMatch(
    source,
    /\b(?:Math\s*\.\s*random|crypto\s*\.\s*getRandomValues)\b/u,
    "randomness",
  );
  assert.doesNotMatch(source, /\b(?:setTimeout|setInterval|setImmediate)\b/u, "timers");
});
