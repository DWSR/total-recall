// biome-ignore-all lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
// biome-ignore-all lint/complexity/noExcessiveLinesPerFunction: The runtime owns one bounded session-snapshot lifecycle.
// biome-ignore-all lint/correctness/useQwikValidLexicalScope: This package shares checks with Qwik hosts but creates no Qwik components.
import type {
  EnabledContextSettings,
  ResolvedContextConfig,
} from "./context-config";
import type { ContextMemory, FrozenSnapshot } from "./context-format";
import { formatContext } from "./context-format";

type ContextTransportOutcome =
  | { readonly kind: "context"; readonly memories: readonly ContextMemory[] }
  | { readonly kind: "failure"; readonly category: string };

interface ContextTransport {
  request: (limit: number) => Promise<ContextTransportOutcome>;
  close: () => Promise<void>;
}

interface StartupContextRuntime {
  resolve: (sessionId: string) => Promise<FrozenSnapshot>;
  remove: (sessionId: string) => void;
  close: () => Promise<void>;
}

const FROZEN_EMPTY: FrozenSnapshot = { kind: "empty" };

/** One in-flight session lookup. The object itself is the completion token: commit requires it to still be mapped. */
interface ResolvingLookup {
  readonly promise: Promise<FrozenSnapshot>;
  readonly settle: (snapshot: FrozenSnapshot) => void;
}

type SessionState = ResolvingLookup | FrozenSnapshot;

function isResolving(state: SessionState): state is ResolvingLookup {
  return "settle" in state;
}

/** Shares one session-owned lookup under one absolute deadline and freezes its terminal outcome. */
function createContextRuntime(
  config: ResolvedContextConfig,
  transport: ContextTransport,
): StartupContextRuntime {
  const sessions = new Map<string, SessionState>();
  let closed = false;
  let closePromise: Promise<void> | undefined;

  /** Commits once: only while the same resolving state remains mapped. Late completion after deletion, reuse, or disposal is discarded. */
  function commit(
    sessionId: string,
    lookup: ResolvingLookup,
    snapshot: FrozenSnapshot,
  ): void {
    if (sessions.get(sessionId) !== lookup) {
      return;
    }
    sessions.set(sessionId, snapshot);
    lookup.settle(snapshot);
  }

  function beginLookup(
    sessionId: string,
    lookup: ResolvingLookup,
    bounds: EnabledContextSettings,
  ): void {
    const deadline = Date.now() + bounds.lookupTimeoutMs;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const expiry = new Promise<never>((_, rejectExpiry) => {
      timer = setTimeout(rejectExpiry, Math.max(0, deadline - Date.now()));
    });
    let request: Promise<ContextTransportOutcome>;
    try {
      request = transport.request(bounds.limit);
    } catch (error) {
      // A synchronous transport refusal settles through the failure path instead of escaping resolve.
      request = Promise.reject(error);
    }
    Promise.race([request, expiry]).then(
      (outcome) => {
        if (timer !== undefined) {
          clearTimeout(timer);
        }
        if (outcome.kind !== "context") {
          commit(sessionId, lookup, FROZEN_EMPTY);
          return;
        }
        commit(
          sessionId,
          lookup,
          formatContext(outcome.memories, bounds.maxInjectionBytes),
        );
      },
      () => {
        if (timer !== undefined) {
          clearTimeout(timer);
        }
        commit(sessionId, lookup, FROZEN_EMPTY);
      },
    );
  }

  function resolve(sessionId: string): Promise<FrozenSnapshot> {
    if (sessionId.trim() === "") {
      // A missing identity performs no lookup, stores no state, and contributes no system content.
      return Promise.resolve(FROZEN_EMPTY);
    }
    const existing = sessions.get(sessionId);
    if (existing !== undefined) {
      if (isResolving(existing)) {
        return existing.promise;
      }
      return Promise.resolve(existing);
    }
    if (closed || config.mode !== "enabled") {
      return Promise.resolve(FROZEN_EMPTY);
    }
    const bounds = config.settings;
    let settle: (snapshot: FrozenSnapshot) => void = () => undefined;
    const promise = new Promise<FrozenSnapshot>((resolvePending) => {
      settle = resolvePending;
    });
    const lookup: ResolvingLookup = { promise, settle };
    sessions.set(sessionId, lookup);
    beginLookup(sessionId, lookup, bounds);
    return promise;
  }

  function remove(sessionId: string): void {
    const state = sessions.get(sessionId);
    if (state === undefined) {
      return;
    }
    sessions.delete(sessionId);
    if (isResolving(state)) {
      state.settle(FROZEN_EMPTY);
    }
  }

  function close(): Promise<void> {
    if (closePromise !== undefined) {
      return closePromise;
    }
    closed = true;
    for (const [sessionId, state] of sessions) {
      sessions.delete(sessionId);
      if (isResolving(state)) {
        state.settle(FROZEN_EMPTY);
      }
    }
    closePromise = transport.close();
    return closePromise;
  }

  return { resolve, remove, close };
}

export { createContextRuntime };
export type { ContextTransport, StartupContextRuntime };
