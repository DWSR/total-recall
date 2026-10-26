import assert from "node:assert/strict";
import test from "node:test";

import { formatContextConfigurationDiagnostic, loadContextConfig } from "../src/context-config.ts";
import type {
  ContextConfigReason,
  ContextFeatureConfig,
  EnabledContextSettings,
} from "../src/context-config.ts";

type Environment = Readonly<Record<string, string | undefined>>;

const workerPath = "/opt/total-recall/bin/total-recall-mcp-worker";
const database = "memory";
const sentinel = "protected-config-sentinel";

const contextSettingVariables = [
  "TOTAL_RECALL_CONTEXT_LIMIT",
  "TOTAL_RECALL_CONTEXT_MAX_FRAME_BYTES",
  "TOTAL_RECALL_CONTEXT_MAX_INJECTION_BYTES",
  "TOTAL_RECALL_CONTEXT_DIAGNOSTIC_BYTES",
  "TOTAL_RECALL_CONTEXT_STARTUP_TIMEOUT_MS",
  "TOTAL_RECALL_CONTEXT_LOOKUP_TIMEOUT_MS",
  "TOTAL_RECALL_CONTEXT_SHUTDOWN_TIMEOUT_MS",
] as const;

const configReasons = [
  "orphan_context_config",
  "invalid_context_binary",
  "missing_memory_database",
  "invalid_context_bound",
  "inconsistent_context_bounds",
] as const satisfies readonly ContextConfigReason[];

type IsExactly<Left, Right> = [Left] extends [Right]
  ? [Right] extends [Left]
    ? true
    : false
  : false;

const reasonContract: IsExactly<ContextConfigReason, (typeof configReasons)[number]> = true;
const modeContract: IsExactly<ContextFeatureConfig["mode"], "disabled" | "invalid" | "enabled"> =
  true;
const settingsKeyContract: IsExactly<
  keyof EnabledContextSettings,
  | "workerPath"
  | "limit"
  | "maxFrameBytes"
  | "maxInjectionBytes"
  | "diagnosticBytes"
  | "startupTimeoutMs"
  | "lookupTimeoutMs"
  | "shutdownTimeoutMs"
  | "childEnvironment"
> = true;
void reasonContract;
void modeContract;
void settingsKeyContract;

function baseEnvironment(overrides: Environment = {}): Environment {
  return {
    TOTAL_RECALL_CONTEXT_BIN: workerPath,
    TOTAL_RECALL_MEMORY_DATABASE: database,
    ...overrides,
  };
}

function enabledSettings(config: ContextFeatureConfig): EnabledContextSettings {
  assert.equal(config.mode, "enabled");

  if (config.mode !== "enabled") {
    throw new Error("expected enabled context configuration");
  }

  return config.settings;
}

function assertInvalid(config: ContextFeatureConfig, reason: ContextConfigReason): void {
  assert.deepEqual(config, { mode: "invalid", reason });
  assert.equal(Object.isFrozen(config), true);
}

function expectedWorkerSettings(limit: number): Record<string, string> {
  return {
    TOTAL_RECALL_MCP_MAX_LINE_BYTES: "4096",
    TOTAL_RECALL_MCP_CHANNEL_CAPACITY: "4",
    TOTAL_RECALL_MCP_MAX_IN_FLIGHT: "1",
    TOTAL_RECALL_MCP_CONTEXT_DEFAULT_LIMIT: String(limit),
    TOTAL_RECALL_MCP_CONTEXT_MAX_LIMIT: String(limit),
    TOTAL_RECALL_MCP_CONTEXT_MAX_RESULT_BYTES: "32768",
    TOTAL_RECALL_MCP_CONTEXT_TIMEOUT_MS: "5000",
    TOTAL_RECALL_MCP_CONTEXT_DATABASE_TIMEOUT_MS: "3000",
  };
}

test("selects disabled mode when the binary and every context setting are absent", () => {
  const environments: readonly Environment[] = [
    {},
    {
      HARNESS_EVENTS_BIN: "/opt/harness/bin/harness-events",
      HARNESS_EVENTS_TIMEOUT_MS: "35000",
      TOTAL_RECALL_QUEUE_TOPIC: "harness-events",
      TOTAL_RECALL_EMBEDDING_PROVIDER: "provider",
      TOTAL_RECALL_EMBEDDING_MODEL: "model",
      III_WORKER_NAME: "capture-worker",
      OPENAI_API_KEY: sentinel,
    },
    { TOTAL_RECALL_MEMORY_DATABASE: "memory-without-worker" },
    { III_URL: "ws://127.0.0.1:49134", III_NAMESPACE: "default" },
  ];

  for (const environment of environments) {
    const config = loadContextConfig(environment);

    assert.deepEqual(config, { mode: "disabled" });
    assert.equal(Object.isFrozen(config), true);
  }
});

test("rejects every listed context setting that appears without the binary", () => {
  for (const variable of contextSettingVariables) {
    assertInvalid(loadContextConfig({ [variable]: "1" }), "orphan_context_config");
  }

  assertInvalid(
    loadContextConfig({
      TOTAL_RECALL_CONTEXT_LIMIT: "1",
      TOTAL_RECALL_CONTEXT_SHUTDOWN_TIMEOUT_MS: "500",
      TOTAL_RECALL_MEMORY_DATABASE: database,
    }),
    "orphan_context_config",
  );

  for (const variable of contextSettingVariables) {
    const blank: Environment = { [variable]: "" };

    assertInvalid(loadContextConfig(blank), "orphan_context_config");
  }
});

test("rejects a blank or relative worker binary before any other setting", () => {
  for (const binary of ["", "   ", "worker", "./worker", "bin/mcp-worker", ".", "..", "~/worker"]) {
    assertInvalid(
      loadContextConfig({ TOTAL_RECALL_CONTEXT_BIN: binary, TOTAL_RECALL_MEMORY_DATABASE: "" }),
      "invalid_context_binary",
    );
    assertInvalid(
      loadContextConfig({ TOTAL_RECALL_CONTEXT_BIN: binary, TOTAL_RECALL_CONTEXT_LIMIT: "100" }),
      "invalid_context_binary",
    );
  }
});

test("requires a non-blank memory database before validating bounds", () => {
  assertInvalid(
    loadContextConfig({ TOTAL_RECALL_CONTEXT_BIN: workerPath }),
    "missing_memory_database",
  );
  assertInvalid(
    loadContextConfig({ TOTAL_RECALL_CONTEXT_BIN: workerPath, TOTAL_RECALL_MEMORY_DATABASE: "" }),
    "missing_memory_database",
  );
  assertInvalid(
    loadContextConfig({
      TOTAL_RECALL_CONTEXT_BIN: workerPath,
      TOTAL_RECALL_MEMORY_DATABASE: " \t\n",
      TOTAL_RECALL_CONTEXT_LIMIT: "0",
    }),
    "missing_memory_database",
  );
});

test("resolves the documented defaults and the generated worker environment", () => {
  const settings = enabledSettings(loadContextConfig(baseEnvironment()));

  assert.deepEqual(settings, {
    workerPath,
    limit: 10,
    maxFrameBytes: 65536,
    maxInjectionBytes: 49152,
    diagnosticBytes: 512,
    startupTimeoutMs: 5000,
    lookupTimeoutMs: 8000,
    shutdownTimeoutMs: 5000,
    childEnvironment: {
      TOTAL_RECALL_MEMORY_DATABASE: database,
      ...expectedWorkerSettings(10),
    },
  });
});

test("accepts every documented bound at its maximum", () => {
  const settings = enabledSettings(
    loadContextConfig(
      baseEnvironment({
        TOTAL_RECALL_CONTEXT_LIMIT: "20",
        TOTAL_RECALL_CONTEXT_MAX_FRAME_BYTES: "1048576",
        TOTAL_RECALL_CONTEXT_MAX_INJECTION_BYTES: "262144",
        TOTAL_RECALL_CONTEXT_DIAGNOSTIC_BYTES: "4096",
        TOTAL_RECALL_CONTEXT_STARTUP_TIMEOUT_MS: "30000",
        TOTAL_RECALL_CONTEXT_LOOKUP_TIMEOUT_MS: "30000",
        TOTAL_RECALL_CONTEXT_SHUTDOWN_TIMEOUT_MS: "30000",
      }),
    ),
  );

  assert.deepEqual(settings, {
    workerPath,
    limit: 20,
    maxFrameBytes: 1048576,
    maxInjectionBytes: 262144,
    diagnosticBytes: 4096,
    startupTimeoutMs: 30000,
    lookupTimeoutMs: 30000,
    shutdownTimeoutMs: 30000,
    childEnvironment: {
      TOTAL_RECALL_MEMORY_DATABASE: database,
      ...expectedWorkerSettings(20),
    },
  });
});

test("accepts the documented minimum of every numeric bound", () => {
  const settings = enabledSettings(
    loadContextConfig(
      baseEnvironment({
        TOTAL_RECALL_CONTEXT_LIMIT: "1",
        TOTAL_RECALL_CONTEXT_MAX_FRAME_BYTES: "40960",
        TOTAL_RECALL_CONTEXT_MAX_INJECTION_BYTES: "1",
        TOTAL_RECALL_CONTEXT_DIAGNOSTIC_BYTES: "64",
        TOTAL_RECALL_CONTEXT_STARTUP_TIMEOUT_MS: "1",
        TOTAL_RECALL_CONTEXT_LOOKUP_TIMEOUT_MS: "5500",
        TOTAL_RECALL_CONTEXT_SHUTDOWN_TIMEOUT_MS: "500",
      }),
    ),
  );

  assert.deepEqual(settings, {
    workerPath,
    limit: 1,
    maxFrameBytes: 40960,
    maxInjectionBytes: 1,
    diagnosticBytes: 64,
    startupTimeoutMs: 1,
    lookupTimeoutMs: 5500,
    shutdownTimeoutMs: 500,
    childEnvironment: {
      TOTAL_RECALL_MEMORY_DATABASE: database,
      ...expectedWorkerSettings(1),
    },
  });
});

test("rejects every out-of-range or malformed numeric bound", () => {
  const invalidBounds: readonly (readonly [string, string])[] = [
    ["TOTAL_RECALL_CONTEXT_LIMIT", "0"],
    ["TOTAL_RECALL_CONTEXT_LIMIT", "21"],
    ["TOTAL_RECALL_CONTEXT_LIMIT", "-1"],
    ["TOTAL_RECALL_CONTEXT_LIMIT", "1.5"],
    ["TOTAL_RECALL_CONTEXT_LIMIT", "1e1"],
    ["TOTAL_RECALL_CONTEXT_LIMIT", " 1"],
    ["TOTAL_RECALL_CONTEXT_LIMIT", "1 "],
    ["TOTAL_RECALL_CONTEXT_LIMIT", "+1"],
    ["TOTAL_RECALL_CONTEXT_LIMIT", "01"],
    ["TOTAL_RECALL_CONTEXT_LIMIT", "abc"],
    ["TOTAL_RECALL_CONTEXT_LIMIT", ""],
    ["TOTAL_RECALL_CONTEXT_MAX_FRAME_BYTES", "40959"],
    ["TOTAL_RECALL_CONTEXT_MAX_FRAME_BYTES", "1048577"],
    ["TOTAL_RECALL_CONTEXT_MAX_FRAME_BYTES", "65536.5"],
    ["TOTAL_RECALL_CONTEXT_MAX_INJECTION_BYTES", "0"],
    ["TOTAL_RECALL_CONTEXT_MAX_INJECTION_BYTES", "262145"],
    ["TOTAL_RECALL_CONTEXT_DIAGNOSTIC_BYTES", "0"],
    ["TOTAL_RECALL_CONTEXT_DIAGNOSTIC_BYTES", "63"],
    ["TOTAL_RECALL_CONTEXT_DIAGNOSTIC_BYTES", "4097"],
    ["TOTAL_RECALL_CONTEXT_STARTUP_TIMEOUT_MS", "0"],
    ["TOTAL_RECALL_CONTEXT_STARTUP_TIMEOUT_MS", "30001"],
    ["TOTAL_RECALL_CONTEXT_LOOKUP_TIMEOUT_MS", "0"],
    ["TOTAL_RECALL_CONTEXT_LOOKUP_TIMEOUT_MS", "5499"],
    ["TOTAL_RECALL_CONTEXT_LOOKUP_TIMEOUT_MS", "30001"],
    ["TOTAL_RECALL_CONTEXT_SHUTDOWN_TIMEOUT_MS", "0"],
    ["TOTAL_RECALL_CONTEXT_SHUTDOWN_TIMEOUT_MS", "499"],
    ["TOTAL_RECALL_CONTEXT_SHUTDOWN_TIMEOUT_MS", "30001"],
  ];

  for (const [variable, value] of invalidBounds) {
    assertInvalid(
      loadContextConfig(baseEnvironment({ [variable]: value })),
      "invalid_context_bound",
    );
  }
});

test("enforces the cross-deadline rule between startup and lookup timeouts", () => {
  assertInvalid(
    loadContextConfig(
      baseEnvironment({
        TOTAL_RECALL_CONTEXT_STARTUP_TIMEOUT_MS: "6000",
        TOTAL_RECALL_CONTEXT_LOOKUP_TIMEOUT_MS: "5500",
      }),
    ),
    "inconsistent_context_bounds",
  );
  assertInvalid(
    loadContextConfig(baseEnvironment({ TOTAL_RECALL_CONTEXT_STARTUP_TIMEOUT_MS: "8001" })),
    "inconsistent_context_bounds",
  );

  const equalDeadlines = enabledSettings(
    loadContextConfig(
      baseEnvironment({
        TOTAL_RECALL_CONTEXT_STARTUP_TIMEOUT_MS: "8000",
        TOTAL_RECALL_CONTEXT_LOOKUP_TIMEOUT_MS: "8000",
      }),
    ),
  );

  assert.equal(equalDeadlines.startupTimeoutMs, 8000);
  assert.equal(equalDeadlines.lookupTimeoutMs, 8000);

  const minimumDeadlines = enabledSettings(
    loadContextConfig(
      baseEnvironment({
        TOTAL_RECALL_CONTEXT_STARTUP_TIMEOUT_MS: "5500",
        TOTAL_RECALL_CONTEXT_LOOKUP_TIMEOUT_MS: "5500",
      }),
    ),
  );

  assert.equal(minimumDeadlines.startupTimeoutMs, 5500);
  assert.equal(minimumDeadlines.lookupTimeoutMs, 5500);
});

test("builds the exact allowlisted child environment without ambient values", () => {
  const environment: Environment = {
    PATH: "/usr/local/bin:/usr/bin",
    HOME: "/home/protected-home-sentinel",
    XDG_CONFIG_HOME: "/xdg-config-sentinel",
    XDG_DATA_HOME: "/xdg-data-sentinel",
    XDG_CACHE_HOME: "/xdg-cache-sentinel",
    HTTP_PROXY: "http://proxy-sentinel:8080",
    HTTPS_PROXY: "http://proxy-sentinel:8080",
    ALL_PROXY: "socks5://proxy-sentinel:1080",
    NO_PROXY: "localhost",
    HARNESS_EVENTS_BIN: "/opt/capture-bin-sentinel/harness-events",
    HARNESS_EVENTS_TIMEOUT_MS: "35000",
    HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS: "5000",
    HARNESS_EVENTS_QUEUE_CAPACITY: "256",
    TOTAL_RECALL_QUEUE_TOPIC: "capture-topic-sentinel",
    TOTAL_RECALL_MCP_MAX_LINE_BYTES: "leaked-line-sentinel",
    TOTAL_RECALL_MCP_CHANNEL_CAPACITY: "leaked-channel-sentinel",
    TOTAL_RECALL_MCP_MAX_IN_FLIGHT: "leaked-inflight-sentinel",
    TOTAL_RECALL_MCP_CONTEXT_DEFAULT_LIMIT: "leaked-default-sentinel",
    TOTAL_RECALL_MCP_CONTEXT_MAX_LIMIT: "leaked-max-sentinel",
    TOTAL_RECALL_MCP_CONTEXT_MAX_RESULT_BYTES: "leaked-result-sentinel",
    TOTAL_RECALL_MCP_CONTEXT_TIMEOUT_MS: "leaked-timeout-sentinel",
    TOTAL_RECALL_MCP_CONTEXT_DATABASE_TIMEOUT_MS: "leaked-database-timeout-sentinel",
    TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS: "21000",
    TOTAL_RECALL_EMBEDDING_PROVIDER: "provider-sentinel",
    TOTAL_RECALL_EMBEDDING_MODEL: "model-sentinel",
    III_WORKER_NAME: "worker-name-sentinel",
    OPENAI_API_KEY: "api-key-sentinel",
    ANTHROPIC_API_KEY: "api-key-sentinel",
    AWS_SECRET_ACCESS_KEY: "secret-sentinel",
    TOTAL_RECALL_CONTEXT_BIN: workerPath,
    TOTAL_RECALL_MEMORY_DATABASE: database,
    TOTAL_RECALL_CONTEXT_LIMIT: "5",
    TOTAL_RECALL_CONTEXT_MAX_FRAME_BYTES: "81920",
    TOTAL_RECALL_CONTEXT_MAX_INJECTION_BYTES: "65536",
    TOTAL_RECALL_CONTEXT_DIAGNOSTIC_BYTES: "256",
    TOTAL_RECALL_CONTEXT_STARTUP_TIMEOUT_MS: "4000",
    TOTAL_RECALL_CONTEXT_LOOKUP_TIMEOUT_MS: "9000",
    TOTAL_RECALL_CONTEXT_SHUTDOWN_TIMEOUT_MS: "2000",
    III_URL: "ws://127.0.0.1:49134",
    III_NAMESPACE: "context-namespace",
  };
  const settings = enabledSettings(loadContextConfig(environment));

  assert.deepEqual(settings.childEnvironment, {
    III_URL: "ws://127.0.0.1:49134",
    III_NAMESPACE: "context-namespace",
    TOTAL_RECALL_MEMORY_DATABASE: database,
    ...expectedWorkerSettings(5),
  });
  assert.deepEqual(Object.keys(settings.childEnvironment).sort(), [
    "III_NAMESPACE",
    "III_URL",
    "TOTAL_RECALL_MCP_CHANNEL_CAPACITY",
    "TOTAL_RECALL_MCP_CONTEXT_DATABASE_TIMEOUT_MS",
    "TOTAL_RECALL_MCP_CONTEXT_DEFAULT_LIMIT",
    "TOTAL_RECALL_MCP_CONTEXT_MAX_LIMIT",
    "TOTAL_RECALL_MCP_CONTEXT_MAX_RESULT_BYTES",
    "TOTAL_RECALL_MCP_CONTEXT_TIMEOUT_MS",
    "TOTAL_RECALL_MCP_MAX_IN_FLIGHT",
    "TOTAL_RECALL_MCP_MAX_LINE_BYTES",
    "TOTAL_RECALL_MEMORY_DATABASE",
  ]);

  const serialized = JSON.stringify(settings.childEnvironment);

  for (const forbidden of [
    "sentinel",
    "PATH",
    "HOME",
    "XDG",
    "PROXY",
    "HARNESS_EVENTS",
    "TOTAL_RECALL_QUEUE_TOPIC",
    "TOTAL_RECALL_EMBEDDING",
    "III_WORKER_NAME",
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "AWS_SECRET_ACCESS_KEY",
    "TOTAL_RECALL_CONTEXT_",
  ]) {
    assert.equal(serialized.includes(forbidden), false, `child environment leaked ${forbidden}`);
  }

  assert.equal(settings.limit, 5);
  assert.equal(settings.maxFrameBytes, 81920);
  assert.equal(settings.maxInjectionBytes, 65536);
  assert.equal(settings.diagnosticBytes, 256);
  assert.equal(settings.startupTimeoutMs, 4000);
  assert.equal(settings.lookupTimeoutMs, 9000);
  assert.equal(settings.shutdownTimeoutMs, 2000);
});

test("copies only non-blank III connection settings and the memory database", () => {
  const withoutConnection = enabledSettings(loadContextConfig(baseEnvironment()));

  assert.equal("III_URL" in withoutConnection.childEnvironment, false);
  assert.equal("III_NAMESPACE" in withoutConnection.childEnvironment, false);

  const blankConnection = enabledSettings(
    loadContextConfig(baseEnvironment({ III_URL: "   ", III_NAMESPACE: "" })),
  );

  assert.equal("III_URL" in blankConnection.childEnvironment, false);
  assert.equal("III_NAMESPACE" in blankConnection.childEnvironment, false);

  const engineUrl = "ws://127.0.0.1:12345";
  const namespace = "dashboard";
  const withConnection = enabledSettings(
    loadContextConfig(baseEnvironment({ III_URL: engineUrl, III_NAMESPACE: namespace })),
  );

  assert.equal(withConnection.childEnvironment.III_URL, engineUrl);
  assert.equal(withConnection.childEnvironment.III_NAMESPACE, namespace);
  assert.equal(withConnection.childEnvironment.TOTAL_RECALL_MEMORY_DATABASE, database);
});

test("freezes resolved configuration without aliasing or mutating the environment", () => {
  const environment: Record<string, string | undefined> = baseEnvironment({ III_URL: "ws://x" });
  const snapshot = { ...environment };
  const first = enabledSettings(loadContextConfig(environment));
  const second = enabledSettings(loadContextConfig(environment));

  assert.deepEqual(environment, snapshot);
  assert.notEqual(first, second);
  assert.notEqual(first.childEnvironment, second.childEnvironment);
  assert.equal(Object.isFrozen(first), true);
  assert.equal(Object.isFrozen(first.childEnvironment), true);
  assert.equal(Object.isFrozen(second), true);

  assert.throws(() => {
    (first as { limit: number }).limit = 1;
  }, TypeError);
  assert.throws(() => {
    (first.childEnvironment as Record<string, string>).III_URL = "ws://changed";
  }, TypeError);
  assert.equal(first.limit, 10);
  assert.equal(first.childEnvironment.III_URL, "ws://x");
});

test("emits one fixed bounded configuration diagnostic and nothing otherwise", () => {
  const expectedLine = '{"category":"context_configuration_invalid"}\n';

  assert.equal(formatContextConfigurationDiagnostic({}), undefined);
  assert.equal(formatContextConfigurationDiagnostic(baseEnvironment()), undefined);
  assert.equal(
    formatContextConfigurationDiagnostic({ TOTAL_RECALL_MEMORY_DATABASE: database }),
    undefined,
  );

  const invalidEnvironments: readonly Environment[] = [
    { TOTAL_RECALL_CONTEXT_LIMIT: "1" },
    { TOTAL_RECALL_CONTEXT_BIN: "" },
    { TOTAL_RECALL_CONTEXT_BIN: workerPath },
    {
      TOTAL_RECALL_CONTEXT_BIN: workerPath,
      TOTAL_RECALL_MEMORY_DATABASE: database,
      TOTAL_RECALL_CONTEXT_LIMIT: "0",
    },
    {
      TOTAL_RECALL_CONTEXT_BIN: workerPath,
      TOTAL_RECALL_MEMORY_DATABASE: database,
      TOTAL_RECALL_CONTEXT_STARTUP_TIMEOUT_MS: "9000",
      TOTAL_RECALL_CONTEXT_LOOKUP_TIMEOUT_MS: "8000",
    },
  ];

  for (const environment of invalidEnvironments) {
    const line = formatContextConfigurationDiagnostic(environment);

    assert.equal(line, expectedLine);
    assert.equal(Buffer.byteLength(expectedLine, "utf8") <= 512, true);
    assert.match(line ?? "", /^[\x20-\x7e]*\n$/);
    assert.equal((line ?? "").includes(sentinel), false);
  }
});

test("bounds the configuration diagnostic by the configured or fallback byte limit", () => {
  const expectedLine = '{"category":"context_configuration_invalid"}\n';

  const configured = formatContextConfigurationDiagnostic({
    TOTAL_RECALL_CONTEXT_LIMIT: "1",
    TOTAL_RECALL_CONTEXT_DIAGNOSTIC_BYTES: "64",
  });

  assert.equal(configured, expectedLine);
  assert.equal(Buffer.byteLength(configured ?? "", "utf8") <= 64, true);

  for (const fallbackValue of ["8", "4097", "abc", ""]) {
    const fallback = formatContextConfigurationDiagnostic({
      TOTAL_RECALL_CONTEXT_BIN: workerPath,
      TOTAL_RECALL_MEMORY_DATABASE: database,
      TOTAL_RECALL_CONTEXT_DIAGNOSTIC_BYTES: fallbackValue,
    });

    assert.equal(fallback, expectedLine);
    assert.equal(Buffer.byteLength(fallback ?? "", "utf8") <= 512, true);
  }

  const validConfiguration = formatContextConfigurationDiagnostic({
    TOTAL_RECALL_CONTEXT_BIN: workerPath,
    TOTAL_RECALL_MEMORY_DATABASE: sentinel,
    TOTAL_RECALL_CONTEXT_DIAGNOSTIC_BYTES: "64",
  });

  assert.equal(validConfiguration, undefined);

  const relativeBinary = formatContextConfigurationDiagnostic({
    TOTAL_RECALL_CONTEXT_BIN: "worker",
    TOTAL_RECALL_MEMORY_DATABASE: sentinel,
  });

  assert.equal(relativeBinary, expectedLine);
  assert.equal((relativeBinary ?? "").includes(sentinel), false);
});
