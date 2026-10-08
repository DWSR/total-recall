import { expect, test } from "bun:test";
// biome-ignore lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
import type * as Model from "../src/model";

const metadata = {
  generation: "v2",
  sessionId: "session-123",
  projectName: "project",
  currentWorkingDirectory: "/tmp/project",
  timestamp: "2026-09-20T12:34:56.789Z",
} satisfies Model.EventMetadata;

const payload = {
  text: "hello",
  count: 2,
  active: true,
  nested: {
    values: [null, "value", { enabled: false }],
  },
} satisfies Model.JsonObject;

test("shared model accepts recursive JSON and all submission variants", () => {
  const start = { kind: "start", metadata } satisfies Model.Submission;
  const observation = {
    kind: "observation",
    metadata,
    hookType: "opencode.v2.session.text.ended",
    data: payload,
  } satisfies Model.Submission;
  const end = { kind: "end", metadata } satisfies Model.Submission;

  const submissions: readonly Model.Submission[] = [start, observation, end];

  expect(payload.nested).toEqual({
    values: [null, "value", { enabled: false }],
  });
  expect(submissions.map((submission) => submission.kind)).toEqual([
    "start",
    "observation",
    "end",
  ]);
});

test("shared model discriminates successful and categorized dispatch outcomes", () => {
  const outcomes = [
    { ok: true },
    { ok: false, category: "configuration" },
    { ok: false, category: "serialization" },
    { ok: false, category: "spawn" },
    { ok: false, category: "timeout" },
    { ok: false, category: "usage" },
    { ok: false, category: "connection" },
    { ok: false, category: "invocation" },
    { ok: false, category: "signal" },
  ] satisfies readonly Model.DispatchResult[];

  const categories = outcomes.flatMap((outcome) => {
    if (outcome.ok) {
      return [];
    }

    return [outcome.category];
  });

  expect(outcomes[0]?.ok).toBe(true);
  expect(categories).toEqual([
    "configuration",
    "serialization",
    "spawn",
    "timeout",
    "usage",
    "connection",
    "invocation",
    "signal",
  ]);
});

test("diagnostics retain only the categorized identity fields", () => {
  const diagnostic = {
    category: "serialization",
    generation: "v1",
    nativeKind: "session.error",
    sessionId: "session-456",
  } satisfies Model.Diagnostic;

  expect(diagnostic).toEqual({
    category: "serialization",
    generation: "v1",
    nativeKind: "session.error",
    sessionId: "session-456",
  });
});
