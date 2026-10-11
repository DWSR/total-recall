// biome-ignore-all lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
// biome-ignore-all lint/correctness/useQwikValidLexicalScope: The coordinator intentionally closes over ordered runtime branches.
// biome-ignore-all lint/style/useExportsLast: The coordinator seam types are part of the injected test surface.
import type { StartupContextRuntime } from "./context-runtime";

/** Host-facing unload ordering: adapter disposal releases hooks and subscriptions before the coordinator branches finish. */
export interface ContextCoordinator {
  /** Registers the host-facing adapter disposal invoked once, first, at close. */
  readonly bindDisposal: (disposeAdapter: () => Promise<void> | void) => void;
  /** Closes capture and context independently under one shared outer bound. */
  readonly close: () => Promise<void>;
}

export interface ContextCoordinatorOptions {
  /** Context runtime when the resolved configuration enabled retrieval; absent otherwise. */
  readonly context: StartupContextRuntime | undefined;
  /** Shared outer bound covering both shutdown branches from the capture configuration. */
  readonly shutdownTimeoutMs: number;
}

/** Bounds one branch by a shared absolute deadline; expiry leaves the branch settling independently in the background. */
function settleWithin(
  branch: Promise<void>,
  outerDeadline: number,
): Promise<void> {
  const remaining = outerDeadline - Date.now();
  if (remaining <= 0) {
    return Promise.resolve();
  }
  let timer: ReturnType<typeof setTimeout> | undefined;
  const outerBound = new Promise<void>((resolve) => {
    timer = setTimeout(resolve, remaining);
  });
  return Promise.race([branch, outerBound]).finally(() => {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  });
}

/** Orders the optional context runtime beside capture so one branch's failure cannot suppress the other's cleanup. */
export function createContextCoordinator(
  options: ContextCoordinatorOptions,
): ContextCoordinator {
  const { context, shutdownTimeoutMs } = options;
  let disposeAdapter: (() => Promise<void> | void) | undefined;
  let closePromise: Promise<void> | undefined;

  const bindDisposal = (dispose: () => Promise<void> | void): void => {
    if (disposeAdapter === undefined) {
      disposeAdapter = dispose;
    }
  };

  const close = (): Promise<void> => {
    if (closePromise !== undefined) {
      return closePromise;
    }
    const outerDeadline = Date.now() + shutdownTimeoutMs;
    // Adapter disposal starts first: its synchronous step releases host hooks
    // and subscriptions while its returned promise closes capture.
    const captureSettled = Promise.resolve(disposeAdapter?.()).then(
      () => undefined,
      () => undefined,
    );
    const contextSettled = context?.close().then(
      () => undefined,
      () => undefined,
    );
    const branches: Promise<void>[] = [captureSettled];
    if (contextSettled !== undefined) {
      branches.push(contextSettled);
    }
    closePromise = Promise.all(
      branches.map((branch) => settleWithin(branch, outerDeadline)),
    ).then(() => undefined);
    return closePromise;
  };

  return { bindDisposal, close };
}
