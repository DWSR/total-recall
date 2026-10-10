// biome-ignore-all lint/style/useNamingConvention: Tests use exact external environment key names.
import { expect, test } from "bun:test";
// biome-ignore lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
import { loadConfig } from "../src/config";
// biome-ignore lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
import { fromValues, resolveContextConfig } from "../src/context-config";

const BIN = "TOTAL_RECALL_CONTEXT_BIN";
const DB = "TOTAL_RECALL_MEMORY_DATABASE";
const PREFIX = "TOTAL_RECALL_CONTEXT_";
const PROTECTED_SENTINEL_PATTERN = /SENTINEL|PATH|HOME|PROXY|KEY|HARNESS/;
const DIAGNOSTIC_FALLBACK_BYTES = 512;
const SHORT_DIAGNOSTIC_BYTES = 7;
const enabled = {
  [BIN]: "/opt/total-recall/mcp-worker",
  [DB]: "postgres://private/db",
};

test("context configuration is disabled only when all context settings are absent", () => {
  expect(fromValues({})).toEqual({ mode: "disabled" });
  expect(fromValues({ [`${PREFIX}LIMIT`]: "10" })).toEqual({
    mode: "invalid",
    reason: "orphan_context_config",
  });
  expect(fromValues({ [BIN]: " " })).toEqual({
    mode: "invalid",
    reason: "invalid_context_binary",
  });
});

test("enabled settings resolve documented defaults and an exact sanitized child allowlist", () => {
  const result = fromValues({
    ...enabled,
    III_URL: "http://127.0.0.1:7337",
    III_NAMESPACE: "memory",
    PATH: "PATH_SENTINEL",
    HOME: "HOME_SENTINEL",
    HTTPS_PROXY: "PROXY_SENTINEL",
    OPENAI_API_KEY: "CREDENTIAL_SENTINEL",
    HARNESS_EVENTS_BIN: "CAPTURE_SENTINEL",
  });
  expect(result.mode).toBe("enabled");
  if (result.mode !== "enabled") {
    throw new Error("expected enabled config");
  }
  expect(result.settings).toEqual({
    workerPath: "/opt/total-recall/mcp-worker",
    limit: 10,
    maxFrameBytes: 65_536,
    maxPending: 16,
    maxInjectionBytes: 49_152,
    diagnosticBytes: 512,
    startupTimeoutMs: 5000,
    lookupTimeoutMs: 8000,
    shutdownTimeoutMs: 5000,
  });
  const child = result.childEnvironment;
  expect(child).toEqual({
    III_URL: "http://127.0.0.1:7337",
    III_NAMESPACE: "memory",
    TOTAL_RECALL_MEMORY_DATABASE: "postgres://private/db",
    TOTAL_RECALL_MCP_MAX_LINE_BYTES: "4096",
    TOTAL_RECALL_MCP_CHANNEL_CAPACITY: "16",
    TOTAL_RECALL_MCP_MAX_IN_FLIGHT: "16",
    TOTAL_RECALL_MCP_CONTEXT_DEFAULT_LIMIT: "10",
    TOTAL_RECALL_MCP_CONTEXT_MAX_LIMIT: "20",
    TOTAL_RECALL_MCP_CONTEXT_MAX_RESULT_BYTES: "32768",
    TOTAL_RECALL_MCP_CONTEXT_TIMEOUT_MS: "5000",
    TOTAL_RECALL_MCP_CONTEXT_DATABASE_TIMEOUT_MS: "3000",
  });
  expect(JSON.stringify(child)).not.toMatch(PROTECTED_SENTINEL_PATTERN);
});

test("rejects invalid path, database target, and malformed or inconsistent bounds", () => {
  for (const path of ["worker", "relative/worker"]) {
    expect(fromValues({ ...enabled, [BIN]: path })).toMatchObject({
      mode: "invalid",
      reason: "invalid_context_binary",
    });
  }
  expect(fromValues({ [BIN]: "/worker" })).toMatchObject({
    mode: "invalid",
    reason: "missing_memory_database",
  });
  for (const [name, value] of [
    ["LIMIT", "0"],
    ["LIMIT", "21"],
    ["MAX_FRAME_BYTES", "NaN"],
    ["MAX_PENDING", "0"],
    ["MAX_INJECTION_BYTES", "0"],
    ["DIAGNOSTIC_BYTES", "0"],
    ["STARTUP_TIMEOUT_MS", "30001"],
    ["LOOKUP_TIMEOUT_MS", "0"],
    ["SHUTDOWN_TIMEOUT_MS", "-2"],
  ]) {
    expect(
      fromValues({ ...enabled, [`${PREFIX}${name}`]: value }),
    ).toMatchObject({ mode: "invalid" });
  }
  expect(
    fromValues({ ...enabled, [`${PREFIX}MAX_FRAME_BYTES`]: "40959" }),
  ).toMatchObject({ mode: "invalid", reason: "inconsistent_context_bounds" });
});

test("rejects unrecognized context settings instead of silently ignoring configuration", () => {
  expect(fromValues({ ...enabled, [`${PREFIX}UNRECOGNIZED`]: "1" })).toEqual({
    mode: "invalid",
    reason: "invalid_context_bound",
  });
});

test("validates the database target as nonblank while copying its configured value exactly", () => {
  const result = fromValues({ ...enabled, [DB]: " postgres://private/db " });
  expect(result.mode).toBe("enabled");
  if (result.mode !== "enabled") {
    throw new Error("expected enabled config");
  }
  expect(result.childEnvironment[DB]).toBe(" postgres://private/db ");
});

test("enforces inclusive frame and timeout bounds and positive diagnostic limits", () => {
  expect(
    fromValues({ ...enabled, [`${PREFIX}MAX_FRAME_BYTES`]: "40960" }).mode,
  ).toBe("enabled");
  expect(
    fromValues({ ...enabled, [`${PREFIX}STARTUP_TIMEOUT_MS`]: "1" }).mode,
  ).toBe("enabled");
  expect(
    fromValues({ ...enabled, [`${PREFIX}LOOKUP_TIMEOUT_MS`]: "30000" }).mode,
  ).toBe("enabled");
  expect(
    fromValues({ ...enabled, [`${PREFIX}LOOKUP_TIMEOUT_MS`]: "1" }),
  ).toMatchObject({ mode: "invalid", reason: "inconsistent_context_bounds" });
  expect(
    fromValues({ ...enabled, [`${PREFIX}LOOKUP_TIMEOUT_MS`]: "5000" }),
  ).toMatchObject({ mode: "invalid", reason: "inconsistent_context_bounds" });
  expect(
    fromValues({ ...enabled, [`${PREFIX}LOOKUP_TIMEOUT_MS`]: "5001" }).mode,
  ).toBe("enabled");
  expect(
    fromValues({ ...enabled, [`${PREFIX}SHUTDOWN_TIMEOUT_MS`]: "30000" }).mode,
  ).toBe("enabled");
  expect(
    fromValues({
      ...enabled,
      [`${PREFIX}DIAGNOSTIC_BYTES`]: "9007199254740992",
    }),
  ).toMatchObject({
    mode: "invalid",
    reason: "invalid_context_bound",
  });
});

test("keeps generated worker limits consistent at their inclusive boundaries", () => {
  expect(
    fromValues({
      ...enabled,
      [`${PREFIX}MAX_PENDING`]: "16",
      [`${PREFIX}MAX_FRAME_BYTES`]: "40960",
    }).mode,
  ).toBe("enabled");
  expect(
    fromValues({ ...enabled, [`${PREFIX}MAX_PENDING`]: "17" }),
  ).toMatchObject({ mode: "invalid", reason: "inconsistent_context_bounds" });
  expect(
    fromValues({ ...enabled, [`${PREFIX}MAX_FRAME_BYTES`]: "40959" }),
  ).toMatchObject({ mode: "invalid", reason: "inconsistent_context_bounds" });
});

test("diagnostic fallback is fixed, bounded, and does not include protected configuration values", () => {
  const invalid = resolveContextConfig({
    [BIN]: "/private/path/SECRET",
    [DB]: "postgres://SECRET",
    [`${PREFIX}DIAGNOSTIC_BYTES`]: "0",
  });
  expect(invalid.mode).toBe("invalid");
  if (invalid.mode !== "invalid") {
    throw new Error("expected invalid config");
  }
  expect(invalid.diagnostic).toBe("startup context configuration is invalid");
  expect(
    new TextEncoder().encode(invalid.diagnostic).length,
  ).toBeLessThanOrEqual(DIAGNOSTIC_FALLBACK_BYTES);
  expect(invalid.diagnostic).not.toContain("SECRET");
});

test("valid diagnostic limits cap the fixed configuration diagnostic by UTF-8 bytes", () => {
  const invalid = resolveContextConfig({
    [BIN]: "/worker",
    [DB]: "postgres://private/db",
    [`${PREFIX}LIMIT`]: "0",
    [`${PREFIX}DIAGNOSTIC_BYTES`]: String(SHORT_DIAGNOSTIC_BYTES),
  });
  expect(invalid.mode).toBe("invalid");
  if (invalid.mode !== "invalid") {
    throw new Error("expected invalid config");
  }
  expect(invalid.diagnostic).toBe("startup");
  expect(new TextEncoder().encode(invalid.diagnostic).length).toBe(
    SHORT_DIAGNOSTIC_BYTES,
  );
});

test("context settings do not alter capture configuration", () => {
  expect(loadConfig({})).toEqual(
    loadConfig({
      ...enabled,
      [`${PREFIX}LIMIT`]: "3",
      PATH: "CAPTURE_PATH_SENTINEL",
      OPENAI_API_KEY: "CAPTURE_CREDENTIAL_SENTINEL",
    }),
  );
});
