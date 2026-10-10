// biome-ignore-all lint/style/noMagicNumbers: Bounds are part of the external worker protocol.
// biome-ignore-all lint/style/useNamingConvention: External environment keys retain protocol casing.
const BIN = "TOTAL_RECALL_CONTEXT_BIN";
const DB = "TOTAL_RECALL_MEMORY_DATABASE";
const CONTEXT_PREFIX = "TOTAL_RECALL_CONTEXT_";
const DEFAULT_LIMIT = 10;
const MAX_LIMIT = 20;
const DEFAULT_MAX_FRAME_BYTES = 65_536;
const DEFAULT_MAX_PENDING = 16;
const DEFAULT_MAX_INJECTION_BYTES = 49_152;
const DEFAULT_DIAGNOSTIC_BYTES = 512;
const DEFAULT_STARTUP_TIMEOUT_MS = 5000;
const DEFAULT_LOOKUP_TIMEOUT_MS = 8000;
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 5000;
const MAX_TIMEOUT_MS = 30_000;
const MAX_PENDING_WORKERS = 16;
const REQUIRED_CONTEXT_FRAME_BYTES = 40_960;
const WORKER_CONTEXT_TIMEOUT_MS = 5000;
const CONTEXT_SETTINGS = new Set([
  `${CONTEXT_PREFIX}LIMIT`,
  `${CONTEXT_PREFIX}MAX_FRAME_BYTES`,
  `${CONTEXT_PREFIX}MAX_PENDING`,
  `${CONTEXT_PREFIX}MAX_INJECTION_BYTES`,
  `${CONTEXT_PREFIX}DIAGNOSTIC_BYTES`,
  `${CONTEXT_PREFIX}STARTUP_TIMEOUT_MS`,
  `${CONTEXT_PREFIX}LOOKUP_TIMEOUT_MS`,
  `${CONTEXT_PREFIX}SHUTDOWN_TIMEOUT_MS`,
]);
const INTEGER = /^\d+$/;
const FIXED_DIAGNOSTIC = "startup context configuration is invalid";
function positiveInteger(
  value: string | undefined,
  fallback: number,
  max?: number,
): number | undefined {
  if (value === undefined) {
    return fallback;
  }
  const text = value.trim();
  if (!INTEGER.test(text)) {
    return undefined;
  }
  const parsed = Number(text);
  if (
    !Number.isSafeInteger(parsed) ||
    parsed <= 0 ||
    (max !== undefined && parsed > max)
  ) {
    return undefined;
  }
  return parsed;
}

function invalid(reason: ContextConfigReason): ContextFeatureConfig {
  return { mode: "invalid", reason };
}

function readSettings(
  environment: Readonly<Record<string, string | undefined>>,
):
  | { readonly settings: Omit<EnabledContextSettings, "workerPath"> }
  | {
      readonly reason: "invalid_context_bound" | "inconsistent_context_bounds";
    } {
  const values = {
    limit: positiveInteger(
      environment[`${CONTEXT_PREFIX}LIMIT`],
      DEFAULT_LIMIT,
      MAX_LIMIT,
    ),
    maxFrameBytes: positiveInteger(
      environment[`${CONTEXT_PREFIX}MAX_FRAME_BYTES`],
      DEFAULT_MAX_FRAME_BYTES,
    ),
    maxPending: positiveInteger(
      environment[`${CONTEXT_PREFIX}MAX_PENDING`],
      DEFAULT_MAX_PENDING,
    ),
    maxInjectionBytes: positiveInteger(
      environment[`${CONTEXT_PREFIX}MAX_INJECTION_BYTES`],
      DEFAULT_MAX_INJECTION_BYTES,
    ),
    diagnosticBytes: positiveInteger(
      environment[`${CONTEXT_PREFIX}DIAGNOSTIC_BYTES`],
      DEFAULT_DIAGNOSTIC_BYTES,
    ),
    startupTimeoutMs: positiveInteger(
      environment[`${CONTEXT_PREFIX}STARTUP_TIMEOUT_MS`],
      DEFAULT_STARTUP_TIMEOUT_MS,
      MAX_TIMEOUT_MS,
    ),
    lookupTimeoutMs: positiveInteger(
      environment[`${CONTEXT_PREFIX}LOOKUP_TIMEOUT_MS`],
      DEFAULT_LOOKUP_TIMEOUT_MS,
      MAX_TIMEOUT_MS,
    ),
    shutdownTimeoutMs: positiveInteger(
      environment[`${CONTEXT_PREFIX}SHUTDOWN_TIMEOUT_MS`],
      DEFAULT_SHUTDOWN_TIMEOUT_MS,
      MAX_TIMEOUT_MS,
    ),
  };
  if (Object.values(values).some((value) => value === undefined)) {
    return { reason: "invalid_context_bound" };
  }
  const settings = values as { [K in keyof typeof values]: number };
  if (
    settings.maxPending > MAX_PENDING_WORKERS ||
    settings.maxFrameBytes < REQUIRED_CONTEXT_FRAME_BYTES ||
    settings.lookupTimeoutMs <= WORKER_CONTEXT_TIMEOUT_MS
  ) {
    return { reason: "inconsistent_context_bounds" };
  }
  return { settings };
}

function childEnvironment(
  environment: Readonly<Record<string, string | undefined>>,
  database: string,
  settings: Omit<EnabledContextSettings, "workerPath">,
): Readonly<Record<string, string>> {
  const child: Record<string, string> = {};
  // biome-ignore lint/complexity/useLiteralKeys: Preserve exact environment key lookup.
  if (environment["III_URL"] !== undefined) {
    // biome-ignore lint/complexity/useLiteralKeys: Environment keys are intentionally indexed.
    child["III_URL"] = environment["III_URL"];
  }
  // biome-ignore lint/complexity/useLiteralKeys: Preserve exact environment key lookup.
  if (environment["III_NAMESPACE"] !== undefined) {
    // biome-ignore lint/complexity/useLiteralKeys: Environment keys are intentionally indexed.
    child["III_NAMESPACE"] = environment["III_NAMESPACE"];
  }
  child[DB] = database;
  Object.assign(child, {
    TOTAL_RECALL_MCP_MAX_LINE_BYTES: "4096",
    TOTAL_RECALL_MCP_CHANNEL_CAPACITY: "16",
    TOTAL_RECALL_MCP_MAX_IN_FLIGHT: "16",
    TOTAL_RECALL_MCP_CONTEXT_DEFAULT_LIMIT: String(settings.limit),
    TOTAL_RECALL_MCP_CONTEXT_MAX_LIMIT: "20",
    TOTAL_RECALL_MCP_CONTEXT_MAX_RESULT_BYTES: "32768",
    TOTAL_RECALL_MCP_CONTEXT_TIMEOUT_MS: "5000",
    TOTAL_RECALL_MCP_CONTEXT_DATABASE_TIMEOUT_MS: "3000",
  });
  return child;
}

export function fromValues(
  environment: Readonly<Record<string, string | undefined>>,
): ContextFeatureConfig {
  const binary = environment[BIN];
  const contextSettings = Object.keys(environment).filter(
    (key) => key.startsWith(CONTEXT_PREFIX) && key !== BIN,
  );
  if (binary === undefined) {
    if (contextSettings.length === 0) {
      return { mode: "disabled" };
    }
    return invalid("orphan_context_config");
  }
  if (contextSettings.some((key) => !CONTEXT_SETTINGS.has(key))) {
    return invalid("invalid_context_bound");
  }
  if (binary.trim().length === 0 || !binary.startsWith("/")) {
    return invalid("invalid_context_binary");
  }

  const database = environment[DB];
  if (database === undefined || database.trim().length === 0) {
    return invalid("missing_memory_database");
  }

  const settings = readSettings(environment);
  if ("reason" in settings) {
    return invalid(settings.reason);
  }
  return {
    mode: "enabled",
    settings: { workerPath: binary, ...settings.settings },
    childEnvironment: childEnvironment(
      environment,
      database,
      settings.settings,
    ),
  };
}

export function resolveContextConfig(
  environment: Readonly<Record<string, string | undefined>>,
): ResolvedContextConfig {
  const config = fromValues(environment);
  if (config.mode !== "invalid") {
    return config;
  }
  const diagnosticBound = positiveInteger(
    environment[`${CONTEXT_PREFIX}DIAGNOSTIC_BYTES`],
    DEFAULT_DIAGNOSTIC_BYTES,
  );
  const bound = diagnosticBound ?? DEFAULT_DIAGNOSTIC_BYTES;
  const diagnostic = new TextEncoder().encode(FIXED_DIAGNOSTIC).slice(0, bound);
  return { ...config, diagnostic: new TextDecoder().decode(diagnostic) };
}

export type ContextFeatureConfig =
  | { readonly mode: "disabled" }
  | { readonly mode: "invalid"; readonly reason: ContextConfigReason }
  | {
      readonly mode: "enabled";
      readonly settings: EnabledContextSettings;
      readonly childEnvironment: Readonly<Record<string, string>>;
    };

export type ResolvedContextConfig =
  | { readonly mode: "disabled" }
  | {
      readonly mode: "enabled";
      readonly settings: EnabledContextSettings;
      readonly childEnvironment: Readonly<Record<string, string>>;
    }
  | {
      readonly mode: "invalid";
      readonly reason: ContextConfigReason;
      readonly diagnostic: string;
    };

export type ContextConfigReason =
  | "orphan_context_config"
  | "invalid_context_binary"
  | "missing_memory_database"
  | "invalid_context_bound"
  | "inconsistent_context_bounds";

export interface EnabledContextSettings {
  readonly workerPath: string;
  readonly limit: number;
  readonly maxFrameBytes: number;
  readonly maxPending: number;
  readonly maxInjectionBytes: number;
  readonly diagnosticBytes: number;
  readonly startupTimeoutMs: number;
  readonly lookupTimeoutMs: number;
  readonly shutdownTimeoutMs: number;
}
