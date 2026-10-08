// biome-ignore-all lint/correctness/noNodejsModules: The artifact test uses Node's temp-directory APIs without a shell.
// biome-ignore-all lint/style/noExcessiveLinesPerFile: Package manifest and packed artifact contracts share one test boundary.
// biome-ignore-all lint/complexity/noExcessiveLinesPerFunction: The packed artifact and clean consumer exercise one package contract boundary.
// biome-ignore-all lint/style/useNamingConvention: OpenCode and CLI contract fields retain their documented names.

import { expect, test } from "bun:test";
import { chmod, cp, mkdtemp, rename, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import process from "node:process";

const packageRoot = `${import.meta.dir}/..`;
const repositoryRoot = `${packageRoot}/../..`;

const PACKAGE_MANAGER_PATTERN = /(?:^|\s)(?:npm|yarn|pnpm)(?:\s|$)/;
const BUN_LOCK_IGNORE_PATTERN = /(?:^|\n)[^#\n]*bun\.lock/;
const RFC3339_MILLISECOND_PATTERN =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
const EXECUTABLE_FILE_MODE = 0o755;
const EXPECTED_HARNESS_RECORD_COUNT = 11;
const BUN_COMMAND_TIMEOUT_MS = 45_000;
const BUN_COMMAND_TERMINATION_GRACE_MS = 250;
const MAX_BUN_DIAGNOSTIC_LENGTH = 2048;
const RUN_BUN_TIMEOUT_TEST_MS = 25;
const RUN_BUN_MARKER_DELAY_MS = 250;
const RUN_BUN_SETTLE_DELAY_MS = 50;
const PACKAGE_ARTIFACT_TEST_TIMEOUT_MS = 120_000;
const V1_START_RECORD_INDEX = 0;
const V1_CREATED_RECORD_INDEX = 1;
const V1_HOOK_RECORD_INDEX = 2;
const V1_DELETED_RECORD_INDEX = 3;
const V1_END_RECORD_INDEX = 4;
const V2_START_RECORD_INDEX = 5;
const V2_CREATED_RECORD_INDEX = 6;
const V2_PROMPT_RECORD_INDEX = 7;
const V2_TOOL_RECORD_INDEX = 8;
const V2_DELETED_RECORD_INDEX = 9;
const V2_END_RECORD_INDEX = 10;

interface PackageManifest {
  readonly type: string;
  readonly main: string;
  readonly types: string;
  readonly exports: unknown;
  readonly files: readonly string[];
  readonly scripts: {
    readonly typecheck: string;
    readonly lint: string;
    readonly format: string;
    readonly "format:check": string;
    readonly test: string;
    readonly build: string;
    readonly pack: string;
    readonly check: string;
  };
  readonly dependencies?: Record<string, string>;
  readonly devDependencies: Record<string, string>;
  readonly packageManager: string;
}

interface TypeScriptConfig {
  readonly extends?: string;
  readonly compilerOptions: {
    readonly strict?: boolean;
    readonly noEmit?: boolean;
    readonly noUncheckedIndexedAccess?: boolean;
    readonly exactOptionalPropertyTypes?: boolean;
    readonly declaration?: boolean;
    readonly sourceMap?: boolean;
    readonly outDir?: string;
    readonly rootDir?: string;
  };
}

interface BiomeConfig {
  readonly formatter: {
    readonly enabled: boolean;
  };
  readonly linter: {
    readonly enabled: boolean;
    readonly rules: {
      readonly preset: string;
    };
  };
}

async function readJson<T>(fileName: string): Promise<T> {
  const source = await Bun.file(`${packageRoot}/${fileName}`).text();
  return JSON.parse(source) as T;
}

function boundedBunDiagnostic(output: string): string {
  if (output.length <= MAX_BUN_DIAGNOSTIC_LENGTH) {
    return output;
  }

  return `${output.slice(0, MAX_BUN_DIAGNOSTIC_LENGTH)}...`;
}

const scriptNames = [
  "typecheck",
  "lint",
  "format",
  "format:check",
  "test",
  "build",
  "pack",
  "check",
] as const;

test("package contract: defines the strict ESM package surface and Bun scripts", async () => {
  const manifest = await readJson<PackageManifest>("package.json");
  const {
    type,
    main,
    types,
    exports: packageExports,
    files,
    dependencies,
    packageManager,
    scripts,
  } = manifest;

  expect(type).toBe("module");
  expect(main).toBe("./dist/index.js");
  expect(types).toBe("./dist/index.d.ts");
  expect(packageExports).toEqual({
    ".": {
      types: "./dist/index.d.ts",
      import: "./dist/index.js",
      default: "./dist/index.js",
    },
  });
  expect(files).toEqual(["dist", "README.md", "LICENSE"]);
  expect(dependencies ?? {}).toEqual({});
  expect(packageManager).toBe("bun@1.4.2");

  for (const scriptName of scriptNames) {
    expect(scripts[scriptName]).toBeString();
    expect(scripts[scriptName]).not.toMatch(PACKAGE_MANAGER_PATTERN);
  }

  expect(scripts.lint).toContain("--error-on-warnings");
  expect(scripts.check).toContain("typecheck");
  expect(scripts.check).toContain("lint");
  expect(scripts.check).toContain("format:check");
  expect(scripts.check).toContain("test");
  expect(scripts.check).toContain("build");
});

test("package contract: keeps OpenCode APIs and tooling out of production dependencies", async () => {
  const manifest = await readJson<PackageManifest>("package.json");
  const { devDependencies, dependencies } = manifest;

  const requiredDevDependencies = [
    "@biomejs/biome",
    "@opencode-ai/plugin",
    "@opencode-ai/sdk",
    "@opencode/client",
    "@opencode/plugin",
    "bun-types",
    "typescript",
  ] as const;

  for (const dependency of requiredDevDependencies) {
    expect(devDependencies[dependency]).toBeString();
  }
  expect(dependencies ?? {}).toEqual({});
});

test("package contract: enables strict checking, declaration emission, and stable Biome rules", async () => {
  const tsconfig = await readJson<TypeScriptConfig>("tsconfig.json");
  const buildConfig = await readJson<TypeScriptConfig>("tsconfig.build.json");
  const biomeConfig = await readJson<BiomeConfig>("biome.json");
  const { compilerOptions } = tsconfig;
  const { compilerOptions: buildOptions } = buildConfig;
  const {
    linter: { enabled: lintEnabled, rules: lintRules },
    formatter,
  } = biomeConfig;

  expect(compilerOptions.strict).toBe(true);
  expect(compilerOptions.noEmit).toBe(true);
  expect(compilerOptions.noUncheckedIndexedAccess).toBe(true);
  expect(compilerOptions.exactOptionalPropertyTypes).toBe(true);
  expect(buildConfig.extends).toBe("./tsconfig.json");
  expect(buildOptions.noEmit).toBe(false);
  expect(buildOptions.declaration).toBe(true);
  expect(buildOptions.sourceMap).toBe(true);
  expect(buildOptions.outDir).toBe("dist");
  expect(buildOptions.rootDir).toBe("src");
  expect(lintEnabled).toBe(true);
  expect(lintRules.preset).toBe("all");
  expect(formatter.enabled).toBe(true);
});

test("package contract: includes only the package assets needed by the distributable", async () => {
  expect(await Bun.file(`${packageRoot}/README.md`).exists()).toBe(true);
  expect(await Bun.file(`${packageRoot}/LICENSE`).exists()).toBe(true);
  expect(await Bun.file(`${packageRoot}/src/index.ts`).exists()).toBe(true);

  const legacyLockfiles = await Promise.all(
    ["package-lock.json", "yarn.lock", "pnpm-lock.yaml"].map((lockfile) =>
      Bun.file(`${packageRoot}/${lockfile}`).exists(),
    ),
  );
  expect(legacyLockfiles).toEqual([false, false, false]);
});

interface BunCommandResult {
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
}

async function runBun(
  args: readonly string[],
  cwd: string,
  environment: Readonly<Record<string, string>> = {},
  timeoutMs = BUN_COMMAND_TIMEOUT_MS,
): Promise<BunCommandResult> {
  const child = Bun.spawn(["bun", ...args], {
    cwd,
    // biome-ignore lint/style/noProcessEnv: The clean consumer inherits only test-controlled overrides over the host environment.
    env: { ...process.env, ...environment },
    stderr: "pipe",
    stdout: "pipe",
  });
  const stdoutPromise = new Response(child.stdout).text();
  const stderrPromise = new Response(child.stderr).text();
  const exitPromise = child.exited.then((completedExitCode) => ({
    exitCode: completedExitCode,
    timedOut: false as const,
  }));
  let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
  const timeoutPromise = new Promise<{ readonly timedOut: true }>((resolve) => {
    timeoutHandle = setTimeout(() => resolve({ timedOut: true }), timeoutMs);
  });
  const exit = await Promise.race([exitPromise, timeoutPromise]);
  if (timeoutHandle !== undefined) {
    clearTimeout(timeoutHandle);
  }

  let exitCode: number;
  if (exit.timedOut) {
    child.kill("SIGTERM");
    const termination = await Promise.race([
      child.exited.then(() => "exited" as const),
      Bun.sleep(BUN_COMMAND_TERMINATION_GRACE_MS).then(
        () => "grace-expired" as const,
      ),
    ]);
    if (termination === "grace-expired") {
      child.kill("SIGKILL");
    }
    exitCode = await child.exited;
  } else {
    const { exitCode: completedExitCode } = exit;
    exitCode = completedExitCode;
  }

  const [stdout, stderr] = await Promise.all([stdoutPromise, stderrPromise]);
  if (exit.timedOut) {
    const command = boundedBunDiagnostic(["bun", ...args].join(" "));
    throw new Error(
      [
        `bun command timed out after ${timeoutMs}ms: ${command}`,
        `exit code: ${exitCode}`,
        `stdout: ${boundedBunDiagnostic(stdout)}`,
        `stderr: ${boundedBunDiagnostic(stderr)}`,
      ].join("\n"),
    );
  }
  return { exitCode, stdout, stderr };
}

test("runBun terminates timed-out commands before they can continue", async () => {
  const temporaryDirectory = await mkdtemp(
    join(tmpdir(), "opencode-harness-events-run-bun-"),
  );
  const markerPath = join(temporaryDirectory, "marker");

  try {
    const command = runBun(
      [
        "-e",
        `setTimeout(async () => {
  await Bun.write(process.env.RUN_BUN_MARKER, "alive");
  process.exit(0);
}, ${RUN_BUN_MARKER_DELAY_MS});
await new Promise(() => {});`,
      ],
      "/",
      { RUN_BUN_MARKER: markerPath },
      RUN_BUN_TIMEOUT_TEST_MS,
    );
    const outcome = await command.then(
      () => "completed",
      (error) => {
        if (error instanceof Error) {
          return error.message;
        }

        return String(error);
      },
    );
    await Bun.sleep(RUN_BUN_MARKER_DELAY_MS + RUN_BUN_SETTLE_DELAY_MS);

    expect(outcome).toContain(
      `bun command timed out after ${RUN_BUN_TIMEOUT_TEST_MS}ms`,
    );
    expect(await Bun.file(markerPath).exists()).toBe(false);
  } finally {
    await rm(temporaryDirectory, { force: true, recursive: true });
  }
});

const BUILT_MODULES = [
  "config",
  "dispatcher",
  "index",
  "model",
  "runtime",
  "v1",
  "v2",
] as const;

interface HarnessRecord {
  readonly args: readonly string[];
  readonly stdin: string;
  readonly env: {
    readonly III_URL: string | null;
    readonly III_NAMESPACE: string | null;
  };
}

interface ConsumerResult {
  readonly disposed: readonly string[];
  readonly v1CountBeforeDispose: number;
  readonly v1CountAfterCallbacks: number;
  readonly v2CountBeforeCleanup: number;
  readonly v2CountAfterCallbacks: number;
}

function harnessRecords(source: string): HarnessRecord[] {
  return source
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line) as HarnessRecord);
}

function recordAt(
  records: readonly HarnessRecord[],
  index: number,
): HarnessRecord {
  const record = records[index];
  if (record === undefined) {
    throw new Error(`missing harness record at index ${index}`);
  }
  return record;
}

function lifecycleArguments(
  command: "session-start" | "session-end",
  sessionId: string,
  directory: string,
  timestamp: string,
): string[] {
  return [
    command,
    "--session-id",
    sessionId,
    "--project-name",
    "project",
    "--current-working-directory",
    directory,
    "--timestamp",
    timestamp,
  ];
}

function observationArguments(
  hookType: string,
  sessionId: string,
  directory: string,
  timestamp: string,
): string[] {
  return [
    "observation",
    "--hook-type",
    hookType,
    "--project-name",
    "project",
    "--current-working-directory",
    directory,
    "--timestamp",
    timestamp,
    "--session-id",
    sessionId,
  ];
}

function expectedArtifactFiles(): string[] {
  const builtFiles = BUILT_MODULES.flatMap((moduleName) => [
    `package/dist/${moduleName}.d.ts`,
    `package/dist/${moduleName}.d.ts.map`,
    `package/dist/${moduleName}.js`,
    `package/dist/${moduleName}.js.map`,
  ]);
  return [
    "package/LICENSE",
    "package/README.md",
    "package/package.json",
    ...builtFiles,
  ].sort();
}

test(
  "package artifact: packs an allowlisted package and loads both host entrypoints",
  async () => {
    const temporaryDirectory = await mkdtemp(
      join(tmpdir(), "opencode-harness-events-package-"),
    );
    const packageCopy = join(temporaryDirectory, "package");
    const tarballPath = join(
      temporaryDirectory,
      "opencode-harness-events-0.1.0.tgz",
    );
    const fakeExecutable = join(temporaryDirectory, "fake harness-events");
    const logPath = join(temporaryDirectory, "harness-events.jsonl");

    try {
      const build = await runBun(
        [
          join(packageRoot, "node_modules/typescript/bin/tsc"),
          "--project",
          join(packageRoot, "tsconfig.build.json"),
        ],
        "/",
      );
      expect(build.exitCode, build.stderr || build.stdout).toBe(0);

      await cp(packageRoot, packageCopy, { recursive: true });
      const packageContractFiles = [
        "src/index.ts",
        "test/package-contract.test.ts",
        "node_modules/typescript/package.json",
        "bun.lock",
        "tsconfig.json",
      ];
      expect(
        await Promise.all(
          packageContractFiles.map((fileName) =>
            Bun.file(join(packageCopy, fileName)).exists(),
          ),
        ),
      ).toEqual(packageContractFiles.map(() => true));

      const pack = await runBun(
        ["pm", "pack", "--destination", temporaryDirectory],
        packageCopy,
      );
      expect(pack.exitCode, pack.stderr || pack.stdout).toBe(0);
      expect(await Bun.file(tarballPath).exists()).toBe(true);

      const archive = new Bun.Archive(
        await Bun.file(tarballPath).arrayBuffer(),
      );
      const files = await archive.files();
      const archiveFiles = [...files.keys()].sort();
      expect(archiveFiles).toEqual(expectedArtifactFiles());
      expect(
        archiveFiles.some((fileName) =>
          ["package/src/", "package/test/", "package/node_modules/"].some(
            (prefix) => fileName.startsWith(prefix),
          ),
        ),
      ).toBe(false);
      expect(archiveFiles).not.toContain("package/bun.lock");
      expect(archiveFiles).not.toContain("package/tsconfig.json");
      expect(archiveFiles).not.toContain("package/biome.json");
      expect(archiveFiles).not.toContain("package/bunfig.toml");

      const builtJavaScript = await Promise.all(
        BUILT_MODULES.map((moduleName) => {
          const file = files.get(`package/dist/${moduleName}.js`);
          expect(file).toBeDefined();
          if (file === undefined) {
            return "";
          }
          return file.text();
        }),
      );
      expect(builtJavaScript.join("\n")).not.toContain("@opencode-ai/plugin");
      expect(builtJavaScript.join("\n")).not.toContain("@opencode/plugin");
      expect(builtJavaScript.join("\n")).not.toContain("@opencode/client");

      const consumerDirectory = join(temporaryDirectory, "consumer");
      const nodeModulesDirectory = join(consumerDirectory, "node_modules");
      await archive.extract(nodeModulesDirectory);
      await rename(
        join(nodeModulesDirectory, "package"),
        join(nodeModulesDirectory, "opencode-harness-events"),
      );
      await Bun.write(
        join(consumerDirectory, "package.json"),
        JSON.stringify({ type: "module" }),
      );
      await Bun.write(
        fakeExecutable,
        `#!/usr/bin/env bun
import { appendFile } from "node:fs/promises";

const logPath = process.env.HARNESS_EVENTS_TEST_LOG;
if (logPath === undefined) {
  process.exit(2);
}

const chunks = [];
for await (const chunk of process.stdin) {
  chunks.push(Buffer.from(chunk));
}
const stdin = Buffer.concat(chunks).toString("utf8");
await appendFile(
  logPath,
  JSON.stringify({
    args: process.argv.slice(2),
    stdin,
    env: {
      III_URL: process.env.III_URL ?? null,
      III_NAMESPACE: process.env.III_NAMESPACE ?? null,
    },
  }) + "\\n",
);
`,
      );
      await chmod(fakeExecutable, EXECUTABLE_FILE_MODE);
      await Bun.write(
        join(consumerDirectory, "load.mjs"),
        `import plugin from "opencode-harness-events";

const logPath = process.env.HARNESS_EVENTS_TEST_LOG;
if (logPath === undefined) {
  throw new Error("missing test log path");
}

async function recordCount() {
  const source = await Bun.file(logPath).text().catch(() => "");
  return source.trim() === "" ? 0 : source.trim().split("\\n").length;
}

async function waitForRecords(count) {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if ((await recordCount()) >= count) {
      return;
    }
    await Bun.sleep(10);
  }
  throw new Error("timed out waiting for " + count + " harness records");
}

const v1Directory = "/contract/v1/project";
const v1Session = "contract-v1";
const v1Created = Date.parse("2026-09-20T12:34:56.789Z");
const v1Info = {
  id: v1Session,
  directory: v1Directory,
  time: { created: v1Created, updated: v1Created + 1000 },
};
const v1 = await plugin.server({
  directory: v1Directory,
  worktree: v1Directory,
  project: {
    id: "contract-project-v1",
    worktree: v1Directory,
    time: { created: v1Created },
  },
});
await v1.event({
  event: { type: "session.created", properties: { info: v1Info } },
});
await waitForRecords(2);

const v1HookInput = { sessionID: v1Session, prompt: "v1-prompt" };
const v1HookOutput = { role: "assistant", text: "v1-output" };
await v1["chat.message"](v1HookInput, v1HookOutput);
await waitForRecords(3);
await v1.event({
  event: { type: "session.deleted", properties: { info: v1Info } },
});
await waitForRecords(5);
await v1.dispose();
const v1CountBeforeDispose = await recordCount();
await v1.event({
  event: { type: "session.created", properties: { info: v1Info } },
});
await v1["chat.message"](v1HookInput, v1HookOutput);
await Bun.sleep(50);
const v1CountAfterCallbacks = await recordCount();

const v2Directory = "/contract/v2/project";
const v2Session = "contract-v2";
const v2Created = Date.parse("2026-09-20T14:34:56.789Z");
const v2CreatedEvent = {
  id: "v2-created",
  created: v2Created,
  location: { directory: v2Directory },
  type: "session.created",
  data: { sessionID: v2Session, state: "created" },
};
const v2DeletedEvent = {
  id: "v2-deleted",
  created: v2Created + 1000,
  location: { directory: v2Directory },
  type: "session.deleted",
  data: { sessionID: v2Session, state: "deleted" },
};
let releaseDeletion;
const deletionReady = new Promise((resolve) => {
  releaseDeletion = resolve;
});
const sessionCallbacks = new Map();
const toolCallbacks = new Map();
const disposed = [];
const v2 = await plugin.setup({
  location: { directory: v2Directory },
  event: {
    subscribe: async function* () {
      yield v2CreatedEvent;
      yield await deletionReady;
    },
  },
  session: {
    hook: async (name, callback) => {
      sessionCallbacks.set(name, callback);
      return { dispose: async () => disposed.push("session." + name) };
    },
  },
  tool: {
    hook: async (name, callback) => {
      toolCallbacks.set(name, callback);
      return { dispose: async () => disposed.push("tool." + name) };
    },
  },
});
await waitForRecords(7);

await sessionCallbacks.get("prompt")({
  sessionID: v2Session,
  prompt: "v2-prompt",
});
await waitForRecords(8);
await toolCallbacks.get("execute.before")({
  sessionID: v2Session,
  tool: "v2-tool",
});
await waitForRecords(9);
releaseDeletion(v2DeletedEvent);
await waitForRecords(11);
await v2();
const v2CountBeforeCleanup = await recordCount();
await sessionCallbacks.get("prompt")({
  sessionID: v2Session,
  prompt: "after-cleanup",
});
await toolCallbacks.get("execute.before")({
  sessionID: v2Session,
  tool: "after-cleanup",
});
await Bun.sleep(50);
const v2CountAfterCallbacks = await recordCount();

console.log(JSON.stringify({
  disposed,
  v1CountBeforeDispose,
  v1CountAfterCallbacks,
  v2CountBeforeCleanup,
  v2CountAfterCallbacks,
}));
`,
      );

      const consumer = await runBun(["run", "load.mjs"], consumerDirectory, {
        HARNESS_EVENTS_BIN: fakeExecutable,
        HARNESS_EVENTS_TEST_LOG: logPath,
        HARNESS_EVENTS_TIMEOUT_MS: "1000",
        III_URL: "http://contract.invalid",
        III_NAMESPACE: "contract",
      });
      expect(consumer.exitCode, consumer.stderr || consumer.stdout).toBe(0);
      const consumerResult = JSON.parse(consumer.stdout) as ConsumerResult;
      expect(consumerResult).toEqual({
        disposed: [
          "session.prompt",
          "tool.execute.before",
          "tool.execute.after",
        ],
        v1CountBeforeDispose: 5,
        v1CountAfterCallbacks: 5,
        v2CountBeforeCleanup: 11,
        v2CountAfterCallbacks: 11,
      });

      const records = harnessRecords(await Bun.file(logPath).text());
      expect(records).toHaveLength(EXPECTED_HARNESS_RECORD_COUNT);
      for (const record of records) {
        expect(record.env).toEqual({
          III_URL: "http://contract.invalid",
          III_NAMESPACE: "contract",
        });
      }

      const v1Info = {
        id: "contract-v1",
        directory: "/contract/v1/project",
        time: {
          created: Date.parse("2026-09-20T12:34:56.789Z"),
          updated: Date.parse("2026-09-20T12:34:57.789Z"),
        },
      };
      const v1Directory = "/contract/v1/project";
      const v1Session = "contract-v1";
      const v2Directory = "/contract/v2/project";
      const v2Session = "contract-v2";
      const v1CreatedTimestamp = "2026-09-20T12:34:56.789Z";
      const v1DeletedTimestamp = "2026-09-20T12:34:57.789Z";
      const v2CreatedTimestamp = "2026-09-20T14:34:56.789Z";
      const v2DeletedTimestamp = "2026-09-20T14:34:57.789Z";
      expect(recordAt(records, V1_START_RECORD_INDEX).args).toEqual(
        lifecycleArguments(
          "session-start",
          v1Session,
          v1Directory,
          v1CreatedTimestamp,
        ),
      );
      expect(recordAt(records, V1_START_RECORD_INDEX).stdin).toBe("");
      expect(recordAt(records, V1_CREATED_RECORD_INDEX).args).toEqual(
        observationArguments(
          "opencode.v1.session.created",
          v1Session,
          v1Directory,
          v1CreatedTimestamp,
        ),
      );
      expect(recordAt(records, V1_CREATED_RECORD_INDEX).stdin).toBe(
        JSON.stringify({
          source: "opencode",
          generation: "v1",
          kind: "session.created",
          payload: { info: v1Info },
        }),
      );
      expect(
        recordAt(records, V1_HOOK_RECORD_INDEX).args as readonly unknown[],
      ).toEqual([
        "observation",
        "--hook-type",
        "opencode.v1.chat.message",
        "--project-name",
        "project",
        "--current-working-directory",
        v1Directory,
        "--timestamp",
        expect.stringMatching(RFC3339_MILLISECOND_PATTERN),
        "--session-id",
        v1Session,
      ]);
      expect(recordAt(records, V1_HOOK_RECORD_INDEX).stdin).toBe(
        JSON.stringify({
          source: "opencode",
          generation: "v1",
          kind: "chat.message",
          payload: {
            input: { sessionID: v1Session, prompt: "v1-prompt" },
            output: { role: "assistant", text: "v1-output" },
          },
        }),
      );
      expect(recordAt(records, V1_DELETED_RECORD_INDEX).args).toEqual(
        observationArguments(
          "opencode.v1.session.deleted",
          v1Session,
          v1Directory,
          v1DeletedTimestamp,
        ),
      );
      expect(recordAt(records, V1_DELETED_RECORD_INDEX).stdin).toBe(
        JSON.stringify({
          source: "opencode",
          generation: "v1",
          kind: "session.deleted",
          payload: { info: v1Info },
        }),
      );
      expect(recordAt(records, V1_END_RECORD_INDEX).args).toEqual(
        lifecycleArguments(
          "session-end",
          v1Session,
          v1Directory,
          v1DeletedTimestamp,
        ),
      );
      expect(recordAt(records, V1_END_RECORD_INDEX).stdin).toBe("");

      const v2CreatedData = { sessionID: v2Session, state: "created" };
      const v2DeletedData = { sessionID: v2Session, state: "deleted" };
      expect(recordAt(records, V2_START_RECORD_INDEX).args).toEqual(
        lifecycleArguments(
          "session-start",
          v2Session,
          v2Directory,
          v2CreatedTimestamp,
        ),
      );
      expect(recordAt(records, V2_START_RECORD_INDEX).stdin).toBe("");
      expect(recordAt(records, V2_CREATED_RECORD_INDEX).args).toEqual(
        observationArguments(
          "opencode.v2.session.created",
          v2Session,
          v2Directory,
          v2CreatedTimestamp,
        ),
      );
      expect(recordAt(records, V2_CREATED_RECORD_INDEX).stdin).toBe(
        JSON.stringify({
          source: "opencode",
          generation: "v2",
          kind: "session.created",
          payload: v2CreatedData,
        }),
      );
      expect(
        recordAt(records, V2_PROMPT_RECORD_INDEX).args as readonly unknown[],
      ).toEqual([
        "observation",
        "--hook-type",
        "opencode.v2.session.prompt",
        "--project-name",
        "project",
        "--current-working-directory",
        v2Directory,
        "--timestamp",
        expect.stringMatching(RFC3339_MILLISECOND_PATTERN),
        "--session-id",
        v2Session,
      ]);
      expect(recordAt(records, V2_PROMPT_RECORD_INDEX).stdin).toBe(
        JSON.stringify({
          source: "opencode",
          generation: "v2",
          kind: "session.prompt",
          payload: { sessionID: v2Session, prompt: "v2-prompt" },
        }),
      );
      expect(
        recordAt(records, V2_TOOL_RECORD_INDEX).args as readonly unknown[],
      ).toEqual([
        "observation",
        "--hook-type",
        "opencode.v2.tool.execute.before",
        "--project-name",
        "project",
        "--current-working-directory",
        v2Directory,
        "--timestamp",
        expect.stringMatching(RFC3339_MILLISECOND_PATTERN),
        "--session-id",
        v2Session,
      ]);
      expect(recordAt(records, V2_TOOL_RECORD_INDEX).stdin).toBe(
        JSON.stringify({
          source: "opencode",
          generation: "v2",
          kind: "tool.execute.before",
          payload: { sessionID: v2Session, tool: "v2-tool" },
        }),
      );
      expect(recordAt(records, V2_DELETED_RECORD_INDEX).args).toEqual(
        observationArguments(
          "opencode.v2.session.deleted",
          v2Session,
          v2Directory,
          v2DeletedTimestamp,
        ),
      );
      expect(recordAt(records, V2_DELETED_RECORD_INDEX).stdin).toBe(
        JSON.stringify({
          source: "opencode",
          generation: "v2",
          kind: "session.deleted",
          payload: v2DeletedData,
        }),
      );
      expect(recordAt(records, V2_END_RECORD_INDEX).args).toEqual(
        lifecycleArguments(
          "session-end",
          v2Session,
          v2Directory,
          v2DeletedTimestamp,
        ),
      );
      expect(recordAt(records, V2_END_RECORD_INDEX).stdin).toBe("");
    } finally {
      await rm(temporaryDirectory, { force: true, recursive: true });
    }
  },
  PACKAGE_ARTIFACT_TEST_TIMEOUT_MS,
);

test("repository contract: monitors the package with weekly Bun Dependabot updates", async () => {
  const dependabot = await Bun.file(
    `${repositoryRoot}/.github/dependabot.yml`,
  ).text();

  expect(dependabot).toContain(
    "  - package-ecosystem: cargo\n    directory: /\n    schedule:\n      interval: weekly",
  );
  expect(dependabot).toContain(
    "  - package-ecosystem: bun\n    directory: /integrations/opencode-harness-events\n    schedule:\n      interval: weekly",
  );
});

test("repository contract: ignores generated package state but keeps bun.lock tracked", async () => {
  const gitignore = await Bun.file(`${repositoryRoot}/.gitignore`).text();

  expect(gitignore).toContain(
    "/integrations/opencode-harness-events/node_modules/",
  );
  expect(gitignore).toContain("/integrations/opencode-harness-events/dist/");
  expect(gitignore).toContain("/integrations/opencode-harness-events/*.tgz");
  expect(gitignore).not.toMatch(BUN_LOCK_IGNORE_PATTERN);
});
