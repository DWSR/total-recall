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

test("refuses missing session identities before any transport activity or state", async () => {
  let calls = 0;
  const runtime = createContextRuntime(
    config,
    fakeClient(() => {
      calls += 1;
      return Promise.resolve({ kind: "context", memories: [record] });
    }),
  );
  expect(await runtime.resolve("")).toEqual({ kind: "empty" });
  expect(await runtime.resolve("   ")).toEqual({ kind: "empty" });
  expect(await runtime.resolve("\t\n ")).toEqual({ kind: "empty" });
  expect(calls).toBe(0);
  const admitted = await runtime.resolve("parent");
  expect(admitted.kind).toBe("text");
  expect(calls).toBe(1);
});

test("discards late completion after deletion and repeated removal, then permits textual ID reuse", async () => {
  let calls = 0;
  const waiters: Array<(value: unknown) => void> = [];
  const runtime = createContextRuntime(
    config,
    fakeClient(() => {
      calls += 1;
      return new Promise((done) => {
        waiters.push(done);
      });
    }),
  );
  const pending = runtime.resolve("parent");
  expect(calls).toBe(1);
  runtime.remove("parent");
  runtime.remove("parent");
  expect(await pending).toEqual({ kind: "empty" });
  const reused = runtime.resolve("parent");
  expect(calls).toBe(2);
  waiters[0]?.({ kind: "context", memories: [{ ...record, title: "stale" }] });
  waiters[1]?.({ kind: "context", memories: [{ ...record, title: "fresh" }] });
  const snapshot = await reused;
  if (snapshot.kind !== "text") {
    throw new Error("the reused session must freeze formatted text");
  }
  expect(snapshot.text.includes("fresh")).toBe(true);
  expect(snapshot.text.includes("stale")).toBe(false);
  expect(calls).toBe(2);
});

test("removes frozen snapshot state so a reused identity starts a fresh lookup", async () => {
  let calls = 0;
  const waiters: Array<(value: unknown) => void> = [];
  const runtime = createContextRuntime(
    config,
    fakeClient(() => {
      calls += 1;
      return new Promise((done) => {
        waiters.push(done);
      });
    }),
  );
  const frozen = runtime.resolve("parent");
  waiters[0]?.({ kind: "context", memories: [] });
  expect(await frozen).toEqual({ kind: "empty" });
  expect(calls).toBe(1);
  runtime.remove("parent");
  const reused = runtime.resolve("parent");
  expect(calls).toBe(2);
  waiters[1]?.({ kind: "context", memories: [record] });
  expect((await reused).kind).toBe("text");
});

test("repeats frozen empty resolving without a second lookup and stays frozen after late timeout completion", async () => {
  let emptyCalls = 0;
  const emptyRuntime = createContextRuntime(
    config,
    fakeClient(() => {
      emptyCalls += 1;
      return Promise.resolve({ kind: "context", memories: [] });
    }),
  );
  const emptyFrozen = await emptyRuntime.resolve("repeat");
  expect(emptyCalls).toBe(1);
  expect(await emptyRuntime.resolve("repeat")).toBe(emptyFrozen);
  expect(emptyCalls).toBe(1);
  let failureCalls = 0;
  const failureRuntime = createContextRuntime(
    config,
    fakeClient(() => {
      failureCalls += 1;
      return Promise.reject(new Error("private backend detail"));
    }),
  );
  const failureFrozen = await failureRuntime.resolve("repeat");
  expect(failureCalls).toBe(1);
  expect(await failureRuntime.resolve("repeat")).toBe(failureFrozen);
  expect(failureCalls).toBe(1);
  let timeoutCalls = 0;
  const waiters: Array<(value: unknown) => void> = [];
  const timeoutRuntime = createContextRuntime(
    { ...config, settings: { ...config.settings, lookupTimeoutMs: 1 } },
    fakeClient(() => {
      timeoutCalls += 1;
      return new Promise((done) => {
        waiters.push(done);
      });
    }),
  );
  const timeoutFrozen = await timeoutRuntime.resolve("repeat");
  expect(timeoutCalls).toBe(1);
  waiters[0]?.({ kind: "context", memories: [record] });
  expect(await timeoutRuntime.resolve("repeat")).toBe(timeoutFrozen);
  expect(timeoutCalls).toBe(1);
});

test("clears every owned snapshot on close and settles late work without reopening", async () => {
  let calls = 0;
  let closeCalls = 0;
  const waiters: Array<(value: unknown) => void> = [];
  const runtime = createContextRuntime(
    config,
    fakeClient(
      () => {
        calls += 1;
        return new Promise((done) => {
          waiters.push(done);
        });
      },
      () => {
        closeCalls += 1;
        return Promise.resolve();
      },
    ),
  );
  const pending = runtime.resolve("resolving");
  const freezing = runtime.resolve("frozen");
  waiters[1]?.({ kind: "context", memories: [record] });
  expect((await freezing).kind).toBe("text");
  const discharge = runtime.close();
  expect(await pending).toEqual({ kind: "empty" });
  await discharge;
  expect(closeCalls).toBe(1);
  waiters[0]?.({ kind: "context", memories: [record] });
  expect(await runtime.resolve("frozen")).toEqual({ kind: "empty" });
  expect(await runtime.resolve("resolving")).toEqual({ kind: "empty" });
  expect(calls).toBe(2);
  await runtime.close();
  await runtime.close();
  expect(closeCalls).toBe(1);
});

test("settles a synchronously thrown transport request as frozen empty", async () => {
  let calls = 0;
  const runtime = createContextRuntime(
    config,
    fakeClient(() => {
      calls += 1;
      throw new Error("synchronous transport refusal");
    }),
  );
  expect(await runtime.resolve("boom")).toEqual({ kind: "empty" });
  expect(calls).toBe(1);
  expect(await runtime.resolve("boom")).toEqual({ kind: "empty" });
  expect(calls).toBe(1);
});
