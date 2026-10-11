// biome-ignore-all lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
// biome-ignore-all lint/correctness/useQwikValidLexicalScope: Plugin callbacks intentionally close over runtime dependencies.
// biome-ignore-all lint/style/useExportsLast: The adapter types are part of the injected test seam.
// biome-ignore lint/correctness/noNodejsModules: The plugin reads the host environment through the runtime built-in.
import process from "node:process";
import type { Plugin as V1Plugin } from "@opencode-ai/plugin";
import type {
  Cleanup,
  Context as V2Context,
} from "@opencode/plugin/promise/plugin";
import { loadConfig } from "./config";
import { createContextCoordinator } from "./context-coordinator";
import type { ContextCoordinator } from "./context-coordinator";
import { resolveContextConfig } from "./context-config";
import { createContextClient } from "./context-client";
import { createContextRuntime } from "./context-runtime";
import type { StartupContextRuntime } from "./context-runtime";
import { dispatch, reportDiagnostic } from "./dispatcher";
import type { Diagnostic } from "./model";
import type { SessionRuntime } from "./runtime";
import { createSessionRuntime } from "./runtime";
import { setupV1 } from "./v1";
import { setupV2 } from "./v2";

const CONTEXT_CONFIG_DIAGNOSTIC_BYTES = 512;

const reportContextConfiguration = (diagnostic: string): void => {
  const bounded = diagnostic.slice(0, CONTEXT_CONFIG_DIAGNOSTIC_BYTES);
  // biome-ignore lint/suspicious/noConsole: The default sink emits one bounded payload-free diagnostic.
  console.error(bounded);
};

interface ComposedPlugin {
  readonly capture: SessionRuntime;
  readonly context: StartupContextRuntime | undefined;
  readonly coordinator: ContextCoordinator;
}

interface AdapterDependencies {
  readonly runtime: SessionRuntime;
  readonly context?: StartupContextRuntime;
  readonly report: (diagnostic: Diagnostic) => void;
  readonly now: () => Date;
}

const adapterDependencies = (composed: ComposedPlugin): AdapterDependencies => {
  const base: Omit<AdapterDependencies, "context"> = {
    runtime: composed.capture,
    report: reportDiagnostic,
    now: () => new Date(),
  };
  if (composed.context === undefined) {
    return base;
  }
  return { ...base, context: composed.context };
};

/**
 * Composes retrieval beside the unchanged capture runtime. Enabled mode builds
 * the retrieval client and session runtime and starts the owned worker eagerly
 * and asynchronously; disabled and invalid modes supply no context dependency
 * so capture continues without a child process, and an invalid configuration
 * reports its one bounded diagnostic per loaded plugin instance.
 */
const composePlugin = (): ComposedPlugin => {
  // biome-ignore lint/style/noProcessEnv: The host process environment is the plugin configuration boundary.
  const captureConfig = loadConfig(process.env);
  const capture = createSessionRuntime(
    (submission, signal) =>
      dispatch(captureConfig, submission, signal, undefined, reportDiagnostic),
    { shutdownTimeoutMs: captureConfig.shutdownTimeoutMs },
  );
  // biome-ignore lint/style/noProcessEnv: The host process environment is the plugin configuration boundary.
  const contextConfig = resolveContextConfig(process.env);
  let context: StartupContextRuntime | undefined;
  if (contextConfig.mode === "enabled") {
    const client = createContextClient(contextConfig);
    context = createContextRuntime(contextConfig, client);
    // Retrieval startup is eager and asynchronous; composition never blocks on readiness while hooks await the shared start promise.
    client.start().then(
      () => undefined,
      () => undefined,
    );
  } else if (contextConfig.mode === "invalid") {
    reportContextConfiguration(contextConfig.diagnostic);
  }
  return {
    capture,
    context,
    coordinator: createContextCoordinator({
      context,
      shutdownTimeoutMs: captureConfig.shutdownTimeoutMs,
    }),
  };
};

const server: V1Plugin = (input) => {
  const composed = composePlugin();
  return setupV1(input, adapterDependencies(composed)).then((hooks) => {
    composed.coordinator.bindDisposal(hooks.dispose);
    return { ...hooks, dispose: composed.coordinator.close };
  });
};

const setup = (context: V2Context): Promise<Cleanup> => {
  const composed = composePlugin();
  return setupV2(context, adapterDependencies(composed)).then(
    (adapterCleanup) => {
      composed.coordinator.bindDisposal(adapterCleanup);
      return composed.coordinator.close;
    },
  );
};

const plugin = {
  id: "opencode-harness-events",
  server,
  setup,
};

// biome-ignore lint/style/noDefaultExport: OpenCode requires a plain default plugin export.
export default plugin;
