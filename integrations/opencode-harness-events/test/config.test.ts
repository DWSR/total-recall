import { expect, test } from "bun:test";
// biome-ignore lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
import { loadConfig } from "../src/config";

const HARNESS_EVENTS_BIN = "HARNESS_EVENTS_BIN";
const HARNESS_EVENTS_TIMEOUT_MS = "HARNESS_EVENTS_TIMEOUT_MS";
const HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS = "HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS";
const DEFAULT_EXECUTION_TIMEOUT_MS = 35_000;
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 5000;
const MAX_TIMEOUT_MS = 120_000;

test("config uses safe defaults when environment values are absent", () => {
  expect(loadConfig({})).toEqual({
    executable: "harness-events",
    executionTimeoutMs: DEFAULT_EXECUTION_TIMEOUT_MS,
    shutdownTimeoutMs: DEFAULT_SHUTDOWN_TIMEOUT_MS,
  });
});

test("config accepts executable paths and timeout maximums", () => {
  expect(
    loadConfig({
      [HARNESS_EVENTS_BIN]: "/tmp/harness events",
      [HARNESS_EVENTS_TIMEOUT_MS]: "120000",
      [HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS]: "120000",
    }),
  ).toEqual({
    executable: "/tmp/harness events",
    executionTimeoutMs: MAX_TIMEOUT_MS,
    shutdownTimeoutMs: MAX_TIMEOUT_MS,
  });
});

test("config rejects blank executable values", () => {
  for (const executable of ["", " ", "\t\n"]) {
    expect(loadConfig({ [HARNESS_EVENTS_BIN]: executable }).executable).toBe(
      "harness-events",
    );
  }
});

test("config falls back for invalid execution timeout values", () => {
  const invalidValues = ["", " ", "0", "-1", "1.5", "1e3", "NaN", "120001"];

  for (const value of invalidValues) {
    expect(
      loadConfig({ [HARNESS_EVENTS_TIMEOUT_MS]: value }).executionTimeoutMs,
    ).toBe(DEFAULT_EXECUTION_TIMEOUT_MS);
  }
});

test("config falls back for invalid shutdown timeout values", () => {
  const invalidValues = ["", " ", "0", "-1", "1.5", "1e3", "NaN", "120001"];

  for (const value of invalidValues) {
    expect(
      loadConfig({
        [HARNESS_EVENTS_TIMEOUT_MS]: "120000",
        [HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS]: value,
      }).shutdownTimeoutMs,
    ).toBe(DEFAULT_SHUTDOWN_TIMEOUT_MS);
  }
});

test("config keeps shutdown within the resolved execution bound", () => {
  expect(
    loadConfig({
      [HARNESS_EVENTS_TIMEOUT_MS]: "10000",
      [HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS]: "10001",
    }).shutdownTimeoutMs,
  ).toBe(DEFAULT_SHUTDOWN_TIMEOUT_MS);

  expect(
    loadConfig({
      [HARNESS_EVENTS_TIMEOUT_MS]: "1000",
    }),
  ).toEqual({
    executable: "harness-events",
    executionTimeoutMs: 1000,
    shutdownTimeoutMs: 1000,
  });
});

test("config accepts trimmed numeric environment values", () => {
  expect(
    loadConfig({
      [HARNESS_EVENTS_TIMEOUT_MS]: " 120000 ",
      [HARNESS_EVENTS_SHUTDOWN_TIMEOUT_MS]: " 120000 ",
    }),
  ).toEqual({
    executable: "harness-events",
    executionTimeoutMs: MAX_TIMEOUT_MS,
    shutdownTimeoutMs: MAX_TIMEOUT_MS,
  });
});
