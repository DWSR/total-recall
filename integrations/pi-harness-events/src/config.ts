import type { Diagnostic } from "./model.ts";

export interface ExtensionConfig {
  readonly executable: string;
  readonly executionTimeoutMs: number;
  readonly shutdownTimeoutMs: number;
  readonly observationCapacity: number;
}

interface ResolvedSetting<Value> {
  readonly invalid: boolean;
  readonly value: Value;
}

const DEFAULT_EXECUTION_TIMEOUT_MS = 35_000;
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 5_000;
const DEFAULT_OBSERVATION_CAPACITY = 256;
const MINIMUM_EXECUTION_TIMEOUT_MS = 2_000;
const MAXIMUM_EXECUTION_TIMEOUT_MS = 120_000;
const MINIMUM_SHUTDOWN_TIMEOUT_MS = 1_500;
const MINIMUM_OBSERVATION_CAPACITY = 1;
const MAXIMUM_OBSERVATION_CAPACITY = 4_096;

const DEFAULT_CONFIG: ExtensionConfig = {
  executable: "harness-events",
  executionTimeoutMs: DEFAULT_EXECUTION_TIMEOUT_MS,
  shutdownTimeoutMs: DEFAULT_SHUTDOWN_TIMEOUT_MS,
  observationCapacity: DEFAULT_OBSERVATION_CAPACITY,
};

export function loadConfig(
  environment: Readonly<Record<string, string | undefined>>,
): ExtensionConfig {
  const executable = resolveExecutable(environment.HARNESS_EVENTS_BIN);
  const executionTimeoutMs = resolveInteger(
    environment.HARNESS_EVENTS_TIMEOUT_MS,
    DEFAULT_EXECUTION_TIMEOUT_MS,
    MINIMUM_EXECUTION_TIMEOUT_MS,
    MAXIMUM_EXECUTION_TIMEOUT_MS,
  );
  const shutdownTimeoutMs = resolveInteger(
    environment.HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS,
    Math.min(DEFAULT_SHUTDOWN_TIMEOUT_MS, executionTimeoutMs.value),
    MINIMUM_SHUTDOWN_TIMEOUT_MS,
    executionTimeoutMs.value,
  );
  const observationCapacity = resolveInteger(
    environment.HARNESS_EVENTS_QUEUE_CAPACITY,
    DEFAULT_OBSERVATION_CAPACITY,
    MINIMUM_OBSERVATION_CAPACITY,
    MAXIMUM_OBSERVATION_CAPACITY,
  );

  if (
    executable.invalid ||
    executionTimeoutMs.invalid ||
    shutdownTimeoutMs.invalid ||
    observationCapacity.invalid
  ) {
    console.error(createConfigurationDiagnostic());
  }

  return {
    executable: executable.value,
    executionTimeoutMs: executionTimeoutMs.value,
    shutdownTimeoutMs: shutdownTimeoutMs.value,
    observationCapacity: observationCapacity.value,
  };
}

function createConfigurationDiagnostic(): Diagnostic {
  return Object.freeze({ category: "configuration" });
}

function resolveExecutable(value: string | undefined): ResolvedSetting<string> {
  if (value === undefined) {
    return { invalid: false, value: DEFAULT_CONFIG.executable };
  }

  if (value.trim().length === 0) {
    return { invalid: true, value: DEFAULT_CONFIG.executable };
  }

  return { invalid: false, value };
}

function resolveInteger(
  value: string | undefined,
  fallback: number,
  minimum: number,
  maximum: number,
): ResolvedSetting<number> {
  if (value === undefined) {
    return { invalid: false, value: fallback };
  }

  const parsed = parseDecimalInteger(value);

  if (parsed === undefined || parsed < minimum || parsed > maximum) {
    return { invalid: true, value: fallback };
  }

  return { invalid: false, value: parsed };
}

function parseDecimalInteger(value: string): number | undefined {
  if (!/^-?(?:0|[1-9][0-9]*)$/.test(value)) {
    return undefined;
  }

  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : undefined;
}
