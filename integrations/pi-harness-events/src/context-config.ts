import { isAbsolute } from "node:path";

import type { ContextDiagnosticCategory } from "./context-model.ts";

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
  readonly maxInjectionBytes: number;
  readonly diagnosticBytes: number;
  readonly startupTimeoutMs: number;
  readonly lookupTimeoutMs: number;
  readonly shutdownTimeoutMs: number;
  readonly childEnvironment: Readonly<Record<string, string>>;
}

export type ContextFeatureConfig =
  | { readonly mode: "disabled" }
  | { readonly mode: "invalid"; readonly reason: ContextConfigReason }
  | { readonly mode: "enabled"; readonly settings: EnabledContextSettings };

const BINARY_VARIABLE = "TOTAL_RECALL_CONTEXT_BIN";
const LIMIT_VARIABLE = "TOTAL_RECALL_CONTEXT_LIMIT";
const MAX_FRAME_BYTES_VARIABLE = "TOTAL_RECALL_CONTEXT_MAX_FRAME_BYTES";
const MAX_INJECTION_BYTES_VARIABLE = "TOTAL_RECALL_CONTEXT_MAX_INJECTION_BYTES";
const DIAGNOSTIC_BYTES_VARIABLE = "TOTAL_RECALL_CONTEXT_DIAGNOSTIC_BYTES";
const STARTUP_TIMEOUT_MS_VARIABLE = "TOTAL_RECALL_CONTEXT_STARTUP_TIMEOUT_MS";
const LOOKUP_TIMEOUT_MS_VARIABLE = "TOTAL_RECALL_CONTEXT_LOOKUP_TIMEOUT_MS";
const SHUTDOWN_TIMEOUT_MS_VARIABLE = "TOTAL_RECALL_CONTEXT_SHUTDOWN_TIMEOUT_MS";

const MEMORY_DATABASE_VARIABLE = "TOTAL_RECALL_MEMORY_DATABASE";
const III_URL_VARIABLE = "III_URL";
const III_NAMESPACE_VARIABLE = "III_NAMESPACE";

const CONTEXT_SETTING_VARIABLES = [
  LIMIT_VARIABLE,
  MAX_FRAME_BYTES_VARIABLE,
  MAX_INJECTION_BYTES_VARIABLE,
  DIAGNOSTIC_BYTES_VARIABLE,
  STARTUP_TIMEOUT_MS_VARIABLE,
  LOOKUP_TIMEOUT_MS_VARIABLE,
  SHUTDOWN_TIMEOUT_MS_VARIABLE,
] as const;

const DEFAULT_LIMIT = 10;
const MINIMUM_LIMIT = 1;
const MAXIMUM_LIMIT = 20;

const DEFAULT_MAX_FRAME_BYTES = 65_536;
const MINIMUM_MAX_FRAME_BYTES = 40_960;
const MAXIMUM_MAX_FRAME_BYTES = 1_048_576;

const DEFAULT_MAX_INJECTION_BYTES = 49_152;
const MINIMUM_MAX_INJECTION_BYTES = 1;
const MAXIMUM_MAX_INJECTION_BYTES = 262_144;

const DEFAULT_DIAGNOSTIC_BYTES = 512;
const MINIMUM_DIAGNOSTIC_BYTES = 64;
const MAXIMUM_DIAGNOSTIC_BYTES = 4_096;

const DEFAULT_STARTUP_TIMEOUT_MS = 5_000;
const MINIMUM_STARTUP_TIMEOUT_MS = 1;
const MAXIMUM_STARTUP_TIMEOUT_MS = 30_000;

const DEFAULT_LOOKUP_TIMEOUT_MS = 8_000;
// The host lookup deadline must stay longer than the fixed 5000 ms worker operation deadline.
const MINIMUM_LOOKUP_TIMEOUT_MS = 5_500;
const MAXIMUM_LOOKUP_TIMEOUT_MS = 30_000;

const DEFAULT_SHUTDOWN_TIMEOUT_MS = 5_000;
const MINIMUM_SHUTDOWN_TIMEOUT_MS = 500;
const MAXIMUM_SHUTDOWN_TIMEOUT_MS = 30_000;

const WORKER_MAX_LINE_BYTES = 4_096;
const WORKER_CHANNEL_CAPACITY = 4;
const WORKER_MAX_IN_FLIGHT = 1;
const WORKER_MAX_RESULT_BYTES = 32_768;
const WORKER_OPERATION_TIMEOUT_MS = 5_000;
const WORKER_DATABASE_TIMEOUT_MS = 3_000;

const FALLBACK_DIAGNOSTIC_BYTES = 512;
const CONFIGURATION_DIAGNOSTIC_CATEGORY: ContextDiagnosticCategory =
  "context_configuration_invalid";

const DISABLED_CONFIG: ContextFeatureConfig = Object.freeze({ mode: "disabled" });

export function loadContextConfig(
  environment: Readonly<Record<string, string | undefined>>,
): ContextFeatureConfig {
  const binary = environment[BINARY_VARIABLE];

  if (binary === undefined) {
    return hasContextSetting(environment)
      ? invalidConfig("orphan_context_config")
      : DISABLED_CONFIG;
  }

  if (binary.trim().length === 0 || !isAbsolute(binary)) {
    return invalidConfig("invalid_context_binary");
  }

  const database = environment[MEMORY_DATABASE_VARIABLE];

  if (database === undefined || database.trim().length === 0) {
    return invalidConfig("missing_memory_database");
  }

  const limit = resolveInteger(
    environment[LIMIT_VARIABLE],
    DEFAULT_LIMIT,
    MINIMUM_LIMIT,
    MAXIMUM_LIMIT,
  );
  const maxFrameBytes = resolveInteger(
    environment[MAX_FRAME_BYTES_VARIABLE],
    DEFAULT_MAX_FRAME_BYTES,
    MINIMUM_MAX_FRAME_BYTES,
    MAXIMUM_MAX_FRAME_BYTES,
  );
  const maxInjectionBytes = resolveInteger(
    environment[MAX_INJECTION_BYTES_VARIABLE],
    DEFAULT_MAX_INJECTION_BYTES,
    MINIMUM_MAX_INJECTION_BYTES,
    MAXIMUM_MAX_INJECTION_BYTES,
  );
  const diagnosticBytes = resolveInteger(
    environment[DIAGNOSTIC_BYTES_VARIABLE],
    DEFAULT_DIAGNOSTIC_BYTES,
    MINIMUM_DIAGNOSTIC_BYTES,
    MAXIMUM_DIAGNOSTIC_BYTES,
  );
  const startupTimeoutMs = resolveInteger(
    environment[STARTUP_TIMEOUT_MS_VARIABLE],
    DEFAULT_STARTUP_TIMEOUT_MS,
    MINIMUM_STARTUP_TIMEOUT_MS,
    MAXIMUM_STARTUP_TIMEOUT_MS,
  );
  const lookupTimeoutMs = resolveInteger(
    environment[LOOKUP_TIMEOUT_MS_VARIABLE],
    DEFAULT_LOOKUP_TIMEOUT_MS,
    MINIMUM_LOOKUP_TIMEOUT_MS,
    MAXIMUM_LOOKUP_TIMEOUT_MS,
  );
  const shutdownTimeoutMs = resolveInteger(
    environment[SHUTDOWN_TIMEOUT_MS_VARIABLE],
    DEFAULT_SHUTDOWN_TIMEOUT_MS,
    MINIMUM_SHUTDOWN_TIMEOUT_MS,
    MAXIMUM_SHUTDOWN_TIMEOUT_MS,
  );

  if (
    limit === undefined ||
    maxFrameBytes === undefined ||
    maxInjectionBytes === undefined ||
    diagnosticBytes === undefined ||
    startupTimeoutMs === undefined ||
    lookupTimeoutMs === undefined ||
    shutdownTimeoutMs === undefined
  ) {
    return invalidConfig("invalid_context_bound");
  }

  if (startupTimeoutMs > lookupTimeoutMs) {
    return invalidConfig("inconsistent_context_bounds");
  }

  const childEnvironment: Readonly<Record<string, string>> = Object.freeze({
    ...connectionSetting(environment, III_URL_VARIABLE),
    ...connectionSetting(environment, III_NAMESPACE_VARIABLE),
    [MEMORY_DATABASE_VARIABLE]: database,
    TOTAL_RECALL_MCP_MAX_LINE_BYTES: String(WORKER_MAX_LINE_BYTES),
    TOTAL_RECALL_MCP_CHANNEL_CAPACITY: String(WORKER_CHANNEL_CAPACITY),
    TOTAL_RECALL_MCP_MAX_IN_FLIGHT: String(WORKER_MAX_IN_FLIGHT),
    TOTAL_RECALL_MCP_CONTEXT_DEFAULT_LIMIT: String(limit),
    TOTAL_RECALL_MCP_CONTEXT_MAX_LIMIT: String(limit),
    TOTAL_RECALL_MCP_CONTEXT_MAX_RESULT_BYTES: String(WORKER_MAX_RESULT_BYTES),
    TOTAL_RECALL_MCP_CONTEXT_TIMEOUT_MS: String(WORKER_OPERATION_TIMEOUT_MS),
    TOTAL_RECALL_MCP_CONTEXT_DATABASE_TIMEOUT_MS: String(WORKER_DATABASE_TIMEOUT_MS),
  });

  return Object.freeze({
    mode: "enabled",
    settings: Object.freeze({
      workerPath: binary,
      limit,
      maxFrameBytes,
      maxInjectionBytes,
      diagnosticBytes,
      startupTimeoutMs,
      lookupTimeoutMs,
      shutdownTimeoutMs,
      childEnvironment,
    }),
  });
}

export function formatContextConfigurationDiagnostic(
  environment: Readonly<Record<string, string | undefined>>,
): string | undefined {
  if (loadContextConfig(environment).mode !== "invalid") {
    return undefined;
  }

  const line = `${JSON.stringify({ category: CONFIGURATION_DIAGNOSTIC_CATEGORY })}\n`;

  return boundDiagnosticLine(line, effectiveDiagnosticBytes(environment));
}

function hasContextSetting(environment: Readonly<Record<string, string | undefined>>): boolean {
  return CONTEXT_SETTING_VARIABLES.some((variable) => environment[variable] !== undefined);
}

function connectionSetting(
  environment: Readonly<Record<string, string | undefined>>,
  variable: string,
): Readonly<Record<string, string>> {
  const value = environment[variable];

  return value === undefined || value.trim().length === 0 ? {} : { [variable]: value };
}

function resolveInteger(
  value: string | undefined,
  fallback: number,
  minimum: number,
  maximum: number,
): number | undefined {
  if (value === undefined) {
    return fallback;
  }

  const parsed = parseDecimalInteger(value);

  if (parsed === undefined || parsed < minimum || parsed > maximum) {
    return undefined;
  }

  return parsed;
}

function parseDecimalInteger(value: string): number | undefined {
  if (!/^-?(?:0|[1-9][0-9]*)$/.test(value)) {
    return undefined;
  }

  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : undefined;
}

function effectiveDiagnosticBytes(
  environment: Readonly<Record<string, string | undefined>>,
): number {
  const configured = resolveInteger(
    environment[DIAGNOSTIC_BYTES_VARIABLE],
    FALLBACK_DIAGNOSTIC_BYTES,
    MINIMUM_DIAGNOSTIC_BYTES,
    MAXIMUM_DIAGNOSTIC_BYTES,
  );

  return configured ?? FALLBACK_DIAGNOSTIC_BYTES;
}

function boundDiagnosticLine(line: string, maximumBytes: number): string {
  // The fixed diagnostic line is ASCII, so character and byte counts match.
  return line.length <= maximumBytes ? line : line.slice(0, maximumBytes);
}

function invalidConfig(reason: ContextConfigReason): ContextFeatureConfig {
  return Object.freeze({ mode: "invalid", reason });
}
