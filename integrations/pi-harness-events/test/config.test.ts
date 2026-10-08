import assert from "node:assert/strict";
import test from "node:test";

import { loadConfig } from "../src/config.ts";
import type { ExtensionConfig } from "../src/config.ts";

const defaults: ExtensionConfig = {
  executable: "harness-events",
  executionTimeoutMs: 35_000,
  shutdownTimeoutMs: 5_000,
  observationCapacity: 256,
};

test("loads safe defaults when configuration is missing", (t) => {
  const diagnostics: unknown[][] = [];
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  assert.deepEqual(loadConfig({}), defaults);
  assert.deepEqual(diagnostics, []);
});

test("accepts every valid configuration boundary", (t) => {
  const diagnostics: unknown[][] = [];
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const cases: readonly {
    readonly name: string;
    readonly environment: Readonly<Record<string, string>>;
    readonly expected: ExtensionConfig;
  }[] = [
    {
      name: "minimum numeric values",
      environment: {
        HARNESS_EVENTS_BIN: "custom-harness-events",
        HARNESS_EVENTS_TIMEOUT_MS: "2000",
        HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS: "1500",
        HARNESS_EVENTS_QUEUE_CAPACITY: "1",
      },
      expected: {
        executable: "custom-harness-events",
        executionTimeoutMs: 2000,
        shutdownTimeoutMs: 1500,
        observationCapacity: 1,
      },
    },
    {
      name: "maximum numeric values",
      environment: {
        HARNESS_EVENTS_BIN: "/opt/harness-events",
        HARNESS_EVENTS_TIMEOUT_MS: "120000",
        HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS: "120000",
        HARNESS_EVENTS_QUEUE_CAPACITY: "4096",
      },
      expected: {
        executable: "/opt/harness-events",
        executionTimeoutMs: 120_000,
        shutdownTimeoutMs: 120_000,
        observationCapacity: 4096,
      },
    },
  ];

  for (const configuration of cases) {
    assert.deepEqual(
      loadConfig(configuration.environment),
      configuration.expected,
      configuration.name,
    );
  }

  assert.deepEqual(diagnostics, []);
});

test("uses a relationship-safe shutdown default for a lower execution timeout", (t) => {
  const diagnostics: unknown[][] = [];
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  assert.deepEqual(loadConfig({ HARNESS_EVENTS_TIMEOUT_MS: "2000" }), {
    ...defaults,
    executionTimeoutMs: 2000,
    shutdownTimeoutMs: 2000,
  });
  assert.deepEqual(diagnostics, []);
});

test("falls back for malformed, fractional, and unsafe numeric values", (t) => {
  const diagnostics: unknown[][] = [];
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const cases: readonly {
    readonly name: string;
    readonly environment: Readonly<Record<string, string>>;
  }[] = [
    {
      name: "malformed execution timeout",
      environment: { HARNESS_EVENTS_TIMEOUT_MS: "not-a-number" },
    },
    {
      name: "fractional shutdown timeout",
      environment: { HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS: "1500.5" },
    },
    {
      name: "scientific notation execution timeout",
      environment: { HARNESS_EVENTS_TIMEOUT_MS: "2e3" },
    },
    {
      name: "hexadecimal shutdown timeout",
      environment: { HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS: "0x5dc" },
    },
    {
      name: "unsafe observation capacity",
      environment: { HARNESS_EVENTS_QUEUE_CAPACITY: "9007199254740993" },
    },
  ];

  for (const configuration of cases) {
    assert.deepEqual(loadConfig(configuration.environment), defaults, configuration.name);
  }

  assert.equal(diagnostics.length, cases.length);
  assert.deepEqual(
    diagnostics,
    cases.map(() => [{ category: "configuration" }]),
  );
});

test("falls back for blank, too-small, and too-large settings", (t) => {
  const diagnostics: unknown[][] = [];
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const cases: readonly {
    readonly name: string;
    readonly environment: Readonly<Record<string, string>>;
  }[] = [
    { name: "blank executable", environment: { HARNESS_EVENTS_BIN: " \t" } },
    { name: "execution timeout below minimum", environment: { HARNESS_EVENTS_TIMEOUT_MS: "1999" } },
    {
      name: "execution timeout above maximum",
      environment: { HARNESS_EVENTS_TIMEOUT_MS: "120001" },
    },
    {
      name: "shutdown timeout below minimum",
      environment: { HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS: "1499" },
    },
    {
      name: "shutdown timeout above execution timeout",
      environment: { HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS: "35001" },
    },
    { name: "capacity below minimum", environment: { HARNESS_EVENTS_QUEUE_CAPACITY: "0" } },
    { name: "capacity above maximum", environment: { HARNESS_EVENTS_QUEUE_CAPACITY: "4097" } },
  ];

  for (const configuration of cases) {
    assert.deepEqual(loadConfig(configuration.environment), defaults, configuration.name);
  }

  assert.equal(diagnostics.length, cases.length);
  assert.deepEqual(
    diagnostics,
    cases.map(() => [{ category: "configuration" }]),
  );
});

test("keeps the shutdown timeout within the resolved execution deadline", (t) => {
  const diagnostics: unknown[][] = [];
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const expectedWithLowExecution: ExtensionConfig = {
    ...defaults,
    executionTimeoutMs: 2000,
    shutdownTimeoutMs: 2000,
  };

  assert.deepEqual(
    loadConfig({
      HARNESS_EVENTS_TIMEOUT_MS: "2000",
      HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS: "5000",
    }),
    expectedWithLowExecution,
  );
  assert.deepEqual(
    loadConfig({
      HARNESS_EVENTS_TIMEOUT_MS: "1999",
      HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS: "35000",
    }),
    { ...defaults, shutdownTimeoutMs: 35_000 },
  );
  assert.deepEqual(diagnostics, [[{ category: "configuration" }], [{ category: "configuration" }]]);
});

test("emits one payload-free diagnostic for all invalid settings in a load", (t) => {
  const diagnostics: unknown[][] = [];
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const sentinel = "configuration-secret-sentinel";

  assert.deepEqual(
    loadConfig({
      CALLER_DATA: sentinel,
      HARNESS_EVENTS_BIN: " \t",
      HARNESS_EVENTS_QUEUE_CAPACITY: "9007199254740993",
      HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS: "1500.5",
      HARNESS_EVENTS_TIMEOUT_MS: sentinel,
      PRIVATE_PATH: `/private/${sentinel}`,
    }),
    defaults,
  );
  assert.deepEqual(diagnostics, [[{ category: "configuration" }]]);

  const diagnosticOutput = JSON.stringify(diagnostics);
  assert.doesNotMatch(diagnosticOutput, new RegExp(sentinel));
  assert.doesNotMatch(diagnosticOutput, /HARNESS_EVENTS_|CALLER_DATA|PRIVATE_PATH/);
});

test("emits immutable configuration diagnostics without consumer state", (t) => {
  const diagnostics: unknown[][] = [];
  t.mock.method(console, "error", (...arguments_: unknown[]): void => {
    diagnostics.push(arguments_);
  });

  const sentinel = "consumer-injected-sentinel";

  assert.deepEqual(loadConfig({ HARNESS_EVENTS_BIN: " \t" }), defaults);

  const firstCall = diagnostics[0];
  if (firstCall === undefined) {
    throw new Error("Expected the first configuration diagnostic");
  }

  const firstDiagnostic = firstCall[0];
  if (
    firstDiagnostic === undefined ||
    firstDiagnostic === null ||
    typeof firstDiagnostic !== "object"
  ) {
    throw new Error("Expected a configuration diagnostic record");
  }

  const mutationApplied = Reflect.set(firstDiagnostic, "sentinel", sentinel);

  assert.deepEqual(loadConfig({ HARNESS_EVENTS_BIN: " \t" }), defaults);

  const secondCall = diagnostics[1];
  if (secondCall === undefined) {
    throw new Error("Expected the second configuration diagnostic");
  }

  const secondDiagnostic = secondCall[0];
  if (
    secondDiagnostic === undefined ||
    secondDiagnostic === null ||
    typeof secondDiagnostic !== "object"
  ) {
    throw new Error("Expected a configuration diagnostic record");
  }

  assert.equal(diagnostics.length, 2);
  assert.equal(firstCall.length, 1);
  assert.equal(secondCall.length, 1);
  assert.equal(mutationApplied, false);
  assert.equal(Object.isFrozen(secondDiagnostic), true);
  assert.deepEqual(Reflect.ownKeys(secondDiagnostic), ["category"]);
  assert.deepEqual(secondDiagnostic, { category: "configuration" });
  assert.equal(Reflect.has(secondDiagnostic, "sentinel"), false);
});
