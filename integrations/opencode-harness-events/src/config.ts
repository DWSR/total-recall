const DEFAULT_EXECUTABLE = "harness-events";
const DEFAULT_EXECUTION_TIMEOUT_MS = 35_000;
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 5000;
const MAX_EXECUTION_TIMEOUT_MS = 120_000;
const INTEGER_PATTERN = /^\d+$/;

function parseExecutable(value: string | undefined): string {
  const executable = value?.trim();
  if (executable === undefined || executable.length === 0) {
    return DEFAULT_EXECUTABLE;
  }

  return executable;
}

function parseTimeout(
  value: string | undefined,
  fallback: number,
  maximum: number,
): number {
  const normalized = value?.trim();
  if (normalized === undefined || !INTEGER_PATTERN.test(normalized)) {
    return fallback;
  }

  const timeout = Number(normalized);
  if (Number.isSafeInteger(timeout) && timeout > 0 && timeout <= maximum) {
    return timeout;
  }

  return fallback;
}

export interface PluginConfig {
  readonly executable: string;
  readonly executionTimeoutMs: number;
  readonly shutdownTimeoutMs: number;
}

export function loadConfig(
  environment: Readonly<Record<string, string | undefined>>,
): PluginConfig {
  const executionTimeoutMs = parseTimeout(
    environment["HARNESS_EVENTS_TIMEOUT_MS"],
    DEFAULT_EXECUTION_TIMEOUT_MS,
    MAX_EXECUTION_TIMEOUT_MS,
  );
  const shutdownDefault = Math.min(
    DEFAULT_SHUTDOWN_TIMEOUT_MS,
    executionTimeoutMs,
  );

  return {
    executable: parseExecutable(environment["HARNESS_EVENTS_BIN"]),
    executionTimeoutMs,
    shutdownTimeoutMs: parseTimeout(
      environment["HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS"],
      shutdownDefault,
      executionTimeoutMs,
    ),
  };
}
