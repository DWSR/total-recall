// biome-ignore-all lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
// biome-ignore-all lint/correctness/useQwikValidLexicalScope: The runtime intentionally closes over one in-memory session map.
// biome-ignore-all lint/style/useConsistentMethodSignatures: The public runtime shape follows the approved design interface.
import type { DispatchResult, EventMetadata, Submission } from "./model";

const DEFAULT_SHUTDOWN_TIMEOUT_MS = 5000;

function resolveShutdownTimeout(value: number | undefined): number {
  if (value === undefined || !Number.isSafeInteger(value) || value <= 0) {
    return DEFAULT_SHUTDOWN_TIMEOUT_MS;
  }

  return value;
}

/** Optional shutdown-bound override for deterministic package-level tests. */
interface SessionRuntimeOptions {
  readonly shutdownTimeoutMs?: number;
}

export type Dispatch = (
  submission: Submission,
  signal: AbortSignal,
) => Promise<DispatchResult>;

export interface SessionRuntime {
  start(metadata: EventMetadata): void;
  observe(
    observation: Extract<Submission, { readonly kind: "observation" }>,
  ): void;
  end(metadata: EventMetadata): void;
  close(): Promise<void>;
}

// biome-ignore lint/complexity/noExcessiveLinesPerFunction: The runtime keeps its session state machine in one boundary.
export function createSessionRuntime(
  dispatch: Dispatch,
  options: SessionRuntimeOptions = {},
): SessionRuntime {
  interface SessionState {
    metadata: EventMetadata;
    started: boolean;
    ended: boolean;
    ordinaryEndStarted: boolean;
    tail: Promise<void>;
  }

  const sessions = new Map<string, SessionState>();
  const ordinaryController = new AbortController();
  const shutdownTimeoutMs = resolveShutdownTimeout(options.shutdownTimeoutMs);
  let closing = false;
  let closePromise: Promise<void> | undefined;

  const enqueue = (
    state: SessionState,
    submission: Submission,
    beforeDispatch?: () => void,
    afterDispatch?: () => void,
  ): void => {
    state.tail = state.tail.then(async () => {
      if (ordinaryController.signal.aborted) {
        return;
      }

      beforeDispatch?.();
      try {
        await dispatch(submission, ordinaryController.signal);
      } catch {
        // Telemetry failures are isolated from the host callback and its chain.
      }
      afterDispatch?.();
    });
  };

  const createState = (metadata: EventMetadata): SessionState => ({
    metadata,
    started: false,
    ended: false,
    ordinaryEndStarted: false,
    tail: Promise.resolve(),
  });

  const start = (metadata: EventMetadata): void => {
    if (closing) {
      return;
    }

    const existing = sessions.get(metadata.sessionId);
    if (existing !== undefined) {
      if (existing.started && !existing.ended) {
        existing.metadata = metadata;
      }
      return;
    }

    const state = createState(metadata);
    sessions.set(metadata.sessionId, state);
    state.started = true;
    enqueue(state, { kind: "start", metadata });
  };

  const observe = (
    observation: Extract<Submission, { readonly kind: "observation" }>,
  ): void => {
    if (closing) {
      return;
    }

    let state = sessions.get(observation.metadata.sessionId);
    if (state === undefined) {
      start(observation.metadata);
      state = sessions.get(observation.metadata.sessionId);
    }

    if (state === undefined || state.ended) {
      return;
    }

    state.metadata = observation.metadata;
    enqueue(state, observation);
  };

  const end = (metadata: EventMetadata): void => {
    if (closing) {
      return;
    }

    const state = sessions.get(metadata.sessionId);
    if (state === undefined || state.ended) {
      return;
    }

    state.metadata = metadata;
    state.ended = true;
    enqueue(
      state,
      { kind: "end", metadata },
      () => {
        state.ordinaryEndStarted = true;
      },
      () => {
        if (sessions.get(metadata.sessionId) === state) {
          sessions.delete(metadata.sessionId);
        }
      },
    );
  };

  const attemptShutdownEnd = async (
    state: SessionState,
    signal: AbortSignal,
  ): Promise<void> => {
    try {
      await dispatch({ kind: "end", metadata: state.metadata }, signal);
    } catch {
      // Shutdown must contain every telemetry failure.
    }
  };

  const shutdown = async (
    activeSessions: readonly SessionState[],
  ): Promise<void> => {
    const shutdownControllers: AbortController[] = [];
    const attempts = activeSessions.map((state) => {
      const controller = new AbortController();
      shutdownControllers.push(controller);
      return attemptShutdownEnd(state, controller.signal);
    });
    let timedOut = false;
    let timeout: ReturnType<typeof setTimeout> | undefined;

    try {
      const timeoutPromise = new Promise<void>((resolve) => {
        timeout = setTimeout(() => {
          timedOut = true;
          resolve();
        }, shutdownTimeoutMs);
      });
      await Promise.race([Promise.all(attempts), timeoutPromise]);
    } finally {
      if (timeout !== undefined) {
        clearTimeout(timeout);
      }
      if (timedOut) {
        for (const controller of shutdownControllers) {
          controller.abort();
        }
      }
      sessions.clear();
    }
  };

  const close = (): Promise<void> => {
    if (closePromise !== undefined) {
      return closePromise;
    }

    closing = true;
    const activeSessions = [...sessions.values()].filter(
      (state) => !state.ordinaryEndStarted,
    );
    closePromise = Promise.resolve().then(() => shutdown(activeSessions));
    ordinaryController.abort();
    return closePromise;
  };

  return {
    start,
    observe,
    end,
    close,
  };
}
