// biome-ignore-all lint/correctness/useQwikValidLexicalScope: Tests intentionally use closures for injected adapter seams.
// biome-ignore-all lint/performance/noAwaitInLoops: Scenario cases are intentionally isolated and consumed sequentially.
import { expect, test } from "bun:test";
// biome-ignore lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
import type { ResolvedContextConfig } from "../src/context-config";
// biome-ignore lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
import { createContextRuntime } from "../src/context-runtime";

const config: ResolvedContextConfig = {
  mode: "enabled",
  settings: {
    workerPath: "/worker",
    limit: 10,
    maxFrameBytes: 65_536,
    maxPending: 16,
    maxInjectionBytes: 4096,
    diagnosticBytes: 512,
    startupTimeoutMs: 5000,
    lookupTimeoutMs: 8000,
    shutdownTimeoutMs: 5000,
  },
  childEnvironment: {},
};
const record = { type: "note", title: "title", content: "body", concepts: [] };

function fakeClient(
  request: () => Promise<unknown>,
  close: () => Promise<void> = async () => {
    /* the runtime may close the transport at most once */
  },
) {
  return { request, close } as never;
}

test("freezes a successful formatted result and shares concurrent session lookups", async () => {
  let resolve!: (value: unknown) => void;
  let calls = 0;
  const runtime = createContextRuntime(
    config,
    fakeClient(() => {
      calls += 1;
      return new Promise((done) => {
        resolve = done;
      });
    }),
  );
  const first = runtime.resolve("parent");
  const concurrent = runtime.resolve("parent");
  expect(calls).toBe(1);
  resolve({ kind: "context", memories: [record] });
  const snapshot = await first;
  expect(await concurrent).toBe(snapshot);
  expect(snapshot.kind).toBe("text");
  expect(await runtime.resolve("parent")).toBe(snapshot);
  expect(calls).toBe(1);
});

test("freezes empty for empty results, transport failures, overflow, and timeouts", async () => {
  const cases: ReadonlyArray<{
    readonly serve: () => Promise<unknown>;
    readonly maxInjectionBytes: number;
  }> = [
    {
      serve: () => Promise.resolve({ kind: "context", memories: [] }),
      maxInjectionBytes: 4096,
    },
    {
      serve: () => Promise.reject(new Error("private backend detail")),
      maxInjectionBytes: 4096,
    },
    {
      serve: () => Promise.resolve({ kind: "context", memories: [record] }),
      maxInjectionBytes: 1,
    },
  ];
  for (const { serve, maxInjectionBytes } of cases) {
    const localConfig: ResolvedContextConfig = {
      ...config,
      settings: { ...config.settings, maxInjectionBytes },
    };
    const runtime = createContextRuntime(localConfig, fakeClient(serve));
    expect(await runtime.resolve(String(Math.random()))).toEqual({
      kind: "empty",
    });
  }
  const timeoutConfig: ResolvedContextConfig = {
    ...config,
    settings: { ...config.settings, lookupTimeoutMs: 1, maxInjectionBytes: 1 },
  };
  const timeoutRuntime = createContextRuntime(
    timeoutConfig,
    fakeClient(
      () =>
        new Promise(() => {
          /* the transport never settles */
        }),
    ),
  );
  expect(await timeoutRuntime.resolve("timeout")).toEqual({ kind: "empty" });
});

test("keeps exact identities separate and never refreshes frozen snapshots", async () => {
  const SessionCount = 4;
  let calls = 0;
  const runtime = createContextRuntime(
    config,
    fakeClient(() => {
      calls += 1;
      return Promise.resolve({
        kind: "context",
        memories: [{ ...record, title: String(calls) }],
      });
    }),
  );
  const parent = await runtime.resolve("parent");
  const child = await runtime.resolve("child");
  const resumed = await runtime.resolve("resumed");
  const independent = await runtime.resolve("independent");
  expect(calls).toBe(SessionCount);
  expect(new Set([parent, child, resumed, independent]).size).toBe(
    SessionCount,
  );
  expect(await runtime.resolve("parent")).toBe(parent);
  expect(calls).toBe(SessionCount);
});
