import process from "node:process";

import type { ExtensionFactory } from "@earendil-works/pi-coding-agent";

import {
  captureHandlerEntryTimestamp,
  classifyNativeEvent,
  deriveEventMetadata,
  projectNativeEvent,
  selectObservation,
} from "./adapter.ts";
import type { AdapterContext, HandlerClock, NativeEventIdentity } from "./adapter.ts";
import { loadConfig } from "./config.ts";
import type { ExtensionConfig } from "./config.ts";
import { dispatch } from "./dispatcher.ts";
import type { Diagnostic, EventMetadata } from "./model.ts";
import { createSubmissionQueue } from "./queue.ts";
import type { SubmissionQueue } from "./queue.ts";

const clock: HandlerClock = { now: () => new Date() };

export interface ExtensionRuntime {
  readonly clock: HandlerClock;
  readonly createQueue: (config: ExtensionConfig) => SubmissionQueue;
  readonly loadConfig: (
    environment: Readonly<Record<string, string | undefined>>,
  ) => ExtensionConfig;
}

const productionRuntime: ExtensionRuntime = {
  clock,
  createQueue: (config) =>
    createSubmissionQueue(config, (submission, signal) => dispatch(config, submission, signal)),
  loadConfig,
};

export function createExtensionForTesting(
  overrides: Partial<ExtensionRuntime> = {},
): ExtensionFactory {
  return createExtension({
    clock: overrides.clock ?? productionRuntime.clock,
    createQueue: overrides.createQueue ?? productionRuntime.createQueue,
    loadConfig: overrides.loadConfig ?? productionRuntime.loadConfig,
  });
}

function createExtension(runtime: ExtensionRuntime): ExtensionFactory {
  return (pi) => {
    const unsubscriptions: Array<() => void> = [];
    let closing = false;
    let queue: SubmissionQueue | undefined;
    let shutdownPromise: Promise<void> | undefined;
    let shutdownStarted = false;

    try {
      queue = runtime.createQueue(runtime.loadConfig(process.env));
    } catch {
      reportAdapterFailure(undefined, "configuration");
    }

    const releaseSubscriptions = (): void => {
      const retainedUnsubscriptions = unsubscriptions.splice(0);

      for (const unsubscribe of retainedUnsubscriptions) {
        try {
          unsubscribe();
        } catch {
          reportAdapterFailure("session_shutdown");
        }
      }
    };

    const createHandler =
      (nativeEvent: string) =>
      (event: NativeEventIdentity, context: AdapterContext): void => {
        const fallbackTimestamp = captureHandlerEntryTimestamp(runtime.clock);

        if (closing) {
          return;
        }

        try {
          const selection = classifyNativeEvent(event);

          if (selection.kind === "excluded") {
            return;
          }

          const nativeTimestamp = readNativeTimestamp(nativeEvent, event);

          if (selection.kind === "lifecycle") {
            const metadata = deriveEventMetadata(context, nativeTimestamp, fallbackTimestamp);

            if (metadata === undefined) {
              reportAdapterFailure(nativeEvent);
              return;
            }

            if (selection.lifecycle === "start") {
              queue?.start(metadata);
            }

            return;
          }

          const observation = selectObservation(event, context, nativeTimestamp, fallbackTimestamp);

          if (observation === undefined) {
            reportAdapterFailure(nativeEvent);
            return;
          }

          const projected = projectNativeEvent(event, observation.metadata.sessionId);

          if (projected === undefined) {
            return;
          }

          queue?.observe({
            data: projected.payload,
            hookType: observation.hookType,
            kind: "observation",
            metadata: observation.metadata,
          });
        } catch {
          reportAdapterFailure(nativeEvent);
        }
      };

    const shutdownHandler = async (
      event: NativeEventIdentity,
      context: AdapterContext,
    ): Promise<void> => {
      const fallbackTimestamp = captureHandlerEntryTimestamp(runtime.clock);

      if (shutdownStarted) {
        if (shutdownPromise !== undefined) {
          try {
            await shutdownPromise;
          } catch {
            reportAdapterFailure("session_shutdown");
          }
        }

        return;
      }

      shutdownStarted = true;
      let metadata: EventMetadata | undefined;

      try {
        const selection = classifyNativeEvent(event);

        if (selection.kind !== "lifecycle" || selection.lifecycle !== "end") {
          return;
        }

        metadata = deriveEventMetadata(
          context,
          readNativeTimestamp("session_shutdown", event),
          fallbackTimestamp,
        );

        if (metadata === undefined) {
          reportAdapterFailure("session_shutdown");
          return;
        }
      } catch {
        reportAdapterFailure("session_shutdown");
      } finally {
        closing = true;
        try {
          releaseSubscriptions();
        } catch {
          reportAdapterFailure("session_shutdown");
        }
      }

      if (metadata === undefined || queue === undefined) {
        return;
      }

      try {
        shutdownPromise = queue.close(metadata);
        await shutdownPromise;
      } catch {
        reportAdapterFailure("session_shutdown");
      }
    };

    const register = (nativeEvent: string, subscribe: () => () => void): void => {
      try {
        unsubscriptions.push(subscribe());
      } catch {
        reportAdapterFailure(nativeEvent);
      }
    };

    register("session_start", () => pi.on("session_start", createHandler("session_start")));
    register("session_shutdown", () => pi.on("session_shutdown", shutdownHandler));
    register("session_info_changed", () =>
      pi.on("session_info_changed", createHandler("session_info_changed")),
    );
    register("session_compact", () => pi.on("session_compact", createHandler("session_compact")));
    register("session_compact_failed", () =>
      pi.on("session_compact_failed", createHandler("session_compact_failed")),
    );
    register("agent_start", () => pi.on("agent_start", createHandler("agent_start")));
    register("agent_settled", () => pi.on("agent_settled", createHandler("agent_settled")));
    register("ui_prompt_start", () => pi.on("ui_prompt_start", createHandler("ui_prompt_start")));
    register("ui_prompt_end", () => pi.on("ui_prompt_end", createHandler("ui_prompt_end")));
    register("turn_start", () => pi.on("turn_start", createHandler("turn_start")));
    register("turn_end", () => pi.on("turn_end", createHandler("turn_end")));
    register("message_end", () => pi.on("message_end", createHandler("message_end")));
    register("tool_execution_start", () =>
      pi.on("tool_execution_start", createHandler("tool_execution_start")),
    );
    register("tool_execution_end", () =>
      pi.on("tool_execution_end", createHandler("tool_execution_end")),
    );
  };
}

const extension = createExtension(productionRuntime);

export default extension;

function readNativeTimestamp(nativeEvent: string, event: NativeEventIdentity): unknown {
  try {
    switch (nativeEvent) {
      case "session_compact":
        return readDataProperty(readDataProperty(event, "compactionEntry"), "timestamp");
      case "turn_start":
        return readDataProperty(event, "timestamp");
      case "message_end":
        return readDataProperty(readDataProperty(event, "message"), "timestamp");
      default:
        return undefined;
    }
  } catch {
    return undefined;
  }
}

function readDataProperty(value: unknown, property: string): unknown {
  if (typeof value !== "object" || value === null) {
    return undefined;
  }

  const descriptor = Object.getOwnPropertyDescriptor(value, property);
  return descriptor !== undefined && "value" in descriptor ? descriptor.value : undefined;
}

function reportAdapterFailure(
  nativeEvent: string | undefined,
  category: Diagnostic["category"] = "normalization",
): void {
  const diagnostic: Diagnostic = Object.freeze({
    category,
    ...(nativeEvent === undefined ? {} : { nativeEvent }),
  });

  try {
    console.error(diagnostic);
  } catch {}
}
