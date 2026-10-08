// biome-ignore-all lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
// biome-ignore-all lint/correctness/useQwikValidLexicalScope: Plugin callbacks intentionally close over runtime dependencies.
// biome-ignore lint/correctness/noNodejsModules: The plugin reads the host environment through the runtime built-in.
import process from "node:process";
import type { Plugin as V1Plugin } from "@opencode-ai/plugin";
import type {
  Cleanup,
  Context as V2Context,
} from "@opencode/plugin/promise/plugin";
import { dispatch, reportDiagnostic } from "./dispatcher";
import { loadConfig } from "./config";
import { setupV1 } from "./v1";
import { setupV2 } from "./v2";
import { createSessionRuntime } from "./runtime";

const createRuntime = () => {
  // biome-ignore lint/style/noProcessEnv: The host process environment is the plugin configuration boundary.
  const config = loadConfig(process.env);
  const runtime = createSessionRuntime(
    (submission, signal) =>
      dispatch(config, submission, signal, undefined, reportDiagnostic),
    { shutdownTimeoutMs: config.shutdownTimeoutMs },
  );

  return runtime;
};

const server: V1Plugin = (input) => {
  const runtime = createRuntime();
  return setupV1(input, {
    runtime,
    report: reportDiagnostic,
    now: () => new Date(),
  });
};

const setup = (context: V2Context): Promise<Cleanup> => {
  const runtime = createRuntime();
  return setupV2(context, {
    runtime,
    report: reportDiagnostic,
    now: () => new Date(),
  });
};

const plugin = {
  id: "opencode-harness-events",
  server,
  setup,
};

// biome-ignore lint/style/noDefaultExport: OpenCode requires a plain default plugin export.
export default plugin;
