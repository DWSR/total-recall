// biome-ignore-all lint/correctness/useQwikValidLexicalScope: Tests intentionally use closures for injected disposal seams.
import { expect, test } from "bun:test";
// biome-ignore lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
import type { StartupContextRuntime } from "../src/context-runtime";
// biome-ignore lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
import { createContextCoordinator } from "../src/context-coordinator";

/** Bounds slack so timer flake cannot fail the outer-bound proof. */
const BOUND_SLACK_MS = 3000;

function deferred(): {
  readonly promise: Promise<void>;
  readonly resolve: () => void;
  readonly reject: (error: Error) => void;
} {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

function recordedRuntime(
  order: string[],
  close: () => Promise<void> = async () => undefined,
): StartupContextRuntime {
  return {
    resolve: () => {
      throw new Error("resolve is not part of the coordinator contract");
    },
    remove: () => undefined,
    close: () => {
      order.push("context");
      return close();
    },
  } as StartupContextRuntime;
}

test("composes unload so adapter disposal releases hooks before context shutdown", () => {
  const order: string[] = [];
  const context = recordedRuntime(order);
  const coordinator = createContextCoordinator({
    context,
    shutdownTimeoutMs: 5000,
  });
  coordinator.bindDisposal(() => {
    order.push("capture");
    return Promise.resolve();
  });

  const close = coordinator.close();
  expect(order).toEqual(["capture", "context"]);
  return close;
});

test("close resolves only after both branches settle and never rejects branch failures", async () => {
  const order: string[] = [];
  const contextDone = deferred();
  const captureDone = deferred();
  const context = recordedRuntime(order, () => contextDone.promise);
  const coordinator = createContextCoordinator({
    context,
    shutdownTimeoutMs: 5000,
  });
  coordinator.bindDisposal(() => {
    order.push("capture");
    return captureDone.promise;
  });

  const close = coordinator.close();
  expect(order).toEqual(["capture", "context"]);
  captureDone.resolve();
  contextDone.reject(new Error("context_shutdown_failed"));
  await close;
  expect(order).toEqual(["capture", "context"]);
});

test("close resolves within the shared outer bound while a branch stalls beyond it", async () => {
  const context = recordedRuntime(
    ["context"],
    () => new Promise<void>(() => undefined),
  );
  const coordinator = createContextCoordinator({
    context,
    shutdownTimeoutMs: 60,
  });
  coordinator.bindDisposal(() => Promise.resolve());

  const started = Date.now();
  await coordinator.close();
  expect(Date.now() - started).toBeLessThan(BOUND_SLACK_MS);
});

test("close is idempotent and performs each branch once", async () => {
  let disposeCalls = 0;
  let contextCalls = 0;
  const context = recordedRuntime([], () => {
    contextCalls += 1;
    return Promise.resolve();
  });
  const coordinator = createContextCoordinator({
    context,
    shutdownTimeoutMs: 5000,
  });
  coordinator.bindDisposal(() => {
    disposeCalls += 1;
    return Promise.resolve();
  });

  const first = coordinator.close();
  const second = coordinator.close();
  expect(first).toBe(second);
  await first;
  expect(disposeCalls).toBe(1);
  expect(contextCalls).toBe(1);
});

test("capture failure cannot suppress context cleanup and close stays fail-open", async () => {
  const order: string[] = [];
  const context = recordedRuntime(order);
  const coordinator = createContextCoordinator({
    context,
    shutdownTimeoutMs: 5000,
  });
  coordinator.bindDisposal(() => {
    order.push("capture");
    return Promise.reject(new Error("capture_close_failed"));
  });

  await coordinator.close();
  expect(order).toEqual(["capture", "context"]);
});

test("disabled and invalid modes compose unload without a context branch", async () => {
  const coordinator = createContextCoordinator({
    context: undefined,
    shutdownTimeoutMs: 5000,
  });
  let disposeCalls = 0;
  coordinator.bindDisposal(() => {
    disposeCalls += 1;
    return Promise.resolve();
  });

  await coordinator.close();
  expect(disposeCalls).toBe(1);
});
