import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import process from "node:process";
import test from "node:test";
import { fileURLToPath } from "node:url";

const packageRoot = fileURLToPath(new URL("../", import.meta.url));
const packageJsonPath = fileURLToPath(new URL("../package.json", import.meta.url));
const readmePath = fileURLToPath(new URL("../README.md", import.meta.url));
const npmCli = process.env.npm_execpath;
const piPackageName = "@earendil-works/pi-coding-agent";
const supportedPiVersion = "0.86.1";

function isManifestObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function assertManifestObject(value: unknown, message: string): Record<string, unknown> {
  assert.ok(isManifestObject(value), message);
  return value;
}

async function readPackageManifest(): Promise<Record<string, unknown>> {
  return assertManifestObject(
    JSON.parse(await readFile(packageJsonPath, "utf8")),
    "package.json must contain an object",
  );
}

function runPackCheck(npmCliPath: string): {
  readonly status: number | null;
  readonly output: string;
} {
  const result = spawnSync(process.execPath, [npmCliPath, "run", "pack:check"], {
    cwd: packageRoot,
    encoding: "utf8",
  });

  assert.equal(result.error, undefined, result.error?.message ?? "spawn failed");

  return { status: result.status, output: `${result.stdout}\n${result.stderr}` };
}

async function withPackageManifest(
  mutate: (manifest: Record<string, unknown>) => void,
  run: () => void,
): Promise<void> {
  const originalPackageJson = await readFile(packageJsonPath, "utf8");

  try {
    const manifest = assertManifestObject(
      JSON.parse(originalPackageJson),
      "package.json must contain an object",
    );
    mutate(manifest);
    await writeFile(packageJsonPath, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
    run();
  } finally {
    await writeFile(packageJsonPath, originalPackageJson, "utf8");
  }
}

test("format:check rejects an unformatted package README", async () => {
  assert.notEqual(npmCli, undefined, "npm_execpath must be set by npm");

  if (npmCli === undefined) {
    return;
  }

  const originalReadme = await readFile(readmePath, "utf8");

  try {
    await writeFile(readmePath, originalReadme.trimEnd(), "utf8");

    const result = spawnSync(process.execPath, [npmCli, "run", "format:check"], {
      cwd: packageRoot,
      encoding: "utf8",
    });

    assert.equal(result.error, undefined, result.error?.message ?? "spawn failed");
    assert.notEqual(result.status, 0);
    assert.match(`${result.stdout}\n${result.stderr}`, /README\.md/);
  } finally {
    await writeFile(readmePath, originalReadme, "utf8");
  }
});

test("pack:check rejects tests, fixtures, lockfiles, JavaScript, and development assets", async () => {
  assert.notEqual(npmCli, undefined, "npm_execpath must be set by npm");

  if (npmCli === undefined) {
    return;
  }

  const fixtureDirectory = join(packageRoot, "src", "package-check-unexpected");
  const unexpectedFiles = [
    { archivePath: "src/package-check-unexpected/compiled.js", content: "export default true;\n" },
    {
      archivePath: "src/package-check-unexpected/development.ts",
      content: "export const dev = true;\n",
    },
    {
      archivePath: "src/package-check-unexpected/fixture.fixture.ts",
      content: "export const fixture = true;\n",
    },
    { archivePath: "src/package-check-unexpected/package-lock.json", content: "{}\n" },
    {
      archivePath: "src/package-check-unexpected/unit.test.ts",
      content: "export const test = true;\n",
    },
  ];

  try {
    await mkdir(fixtureDirectory, { recursive: true });
    await Promise.all(
      unexpectedFiles.map(({ archivePath, content }) =>
        writeFile(join(packageRoot, archivePath), content, "utf8"),
      ),
    );

    const result = spawnSync(process.execPath, [npmCli, "run", "pack:check"], {
      cwd: packageRoot,
      encoding: "utf8",
    });

    assert.equal(result.error, undefined, result.error?.message ?? "spawn failed");
    assert.notEqual(result.status, 0);
    const output = `${result.stdout}\n${result.stderr}`;

    assert.match(output, /unexpected package archive file:/);

    for (const { archivePath } of unexpectedFiles) {
      assert.ok(output.includes(archivePath), `missing rejected archive path: ${archivePath}`);
    }
  } finally {
    await rm(fixtureDirectory, { force: true, recursive: true });
  }
});

test("pack:check rejects optional production dependencies", async () => {
  assert.notEqual(npmCli, undefined, "npm_execpath must be set by npm");

  if (npmCli === undefined) {
    return;
  }

  const originalPackageJson = await readFile(packageJsonPath, "utf8");

  try {
    const packageManifest: unknown = JSON.parse(originalPackageJson);

    if (
      packageManifest === null ||
      typeof packageManifest !== "object" ||
      Array.isArray(packageManifest)
    ) {
      throw new Error("package.json must contain an object");
    }

    await writeFile(
      packageJsonPath,
      `${JSON.stringify(
        { ...packageManifest, optionalDependencies: { jiti: "2.7.0" } },
        null,
        2,
      )}\n`,
      "utf8",
    );

    const result = spawnSync(process.execPath, [npmCli, "run", "pack:check"], {
      cwd: packageRoot,
      encoding: "utf8",
    });

    assert.equal(result.error, undefined, result.error?.message ?? "spawn failed");
    assert.notEqual(result.status, 0);
    assert.match(
      `${result.stdout}\n${result.stderr}`,
      /installed package manifest has optional production dependencies/,
    );
  } finally {
    await writeFile(packageJsonPath, originalPackageJson, "utf8");
  }
});

test("package manifest pins the exact supported Pi host and Node 24 runtime", async () => {
  const manifest = await readPackageManifest();

  assert.deepEqual(manifest.peerDependencies, { [piPackageName]: supportedPiVersion });
  assert.deepEqual(manifest.dependencies, {});
  assert.deepEqual(manifest.engines, { node: "^24.0.0" });

  const devDependencies = assertManifestObject(
    manifest.devDependencies,
    "package.json must declare development dependencies",
  );

  assert.equal(devDependencies[piPackageName], supportedPiVersion);
});

test("pack:check rejects an unverified Pi host peer range", async () => {
  assert.notEqual(npmCli, undefined, "npm_execpath must be set by npm");

  if (npmCli === undefined) {
    return;
  }

  await withPackageManifest(
    (manifest) => {
      manifest.peerDependencies = { [piPackageName]: "*" };
    },
    () => {
      const result = runPackCheck(npmCli);

      assert.notEqual(result.status, 0);
      assert.match(result.output, /does not declare the exact supported Pi host peer/);
    },
  );
});

test("pack:check rejects a drifted Pi development host pin", async () => {
  assert.notEqual(npmCli, undefined, "npm_execpath must be set by npm");

  if (npmCli === undefined) {
    return;
  }

  await withPackageManifest(
    (manifest) => {
      const devDependencies = assertManifestObject(
        manifest.devDependencies,
        "package.json must declare development dependencies",
      );
      devDependencies[piPackageName] = "^0.86.1";
    },
    () => {
      const result = runPackCheck(npmCli);

      assert.notEqual(result.status, 0);
      assert.match(result.output, /does not pin the exact verified Pi development host/);
    },
  );
});

test("pack:check rejects a drifted Node runtime range", async () => {
  assert.notEqual(npmCli, undefined, "npm_execpath must be set by npm");

  if (npmCli === undefined) {
    return;
  }

  await withPackageManifest(
    (manifest) => {
      manifest.engines = { node: "^26.0.0" };
    },
    () => {
      const result = runPackCheck(npmCli);

      assert.notEqual(result.status, 0);
      assert.match(result.output, /Node 24 runtime contract/);
    },
  );
});

test("pack:check accepts the staged source package", () => {
  assert.notEqual(npmCli, undefined, "npm_execpath must be set by npm");

  if (npmCli === undefined) {
    return;
  }

  const result = spawnSync(process.execPath, [npmCli, "run", "pack:check"], {
    cwd: packageRoot,
    encoding: "utf8",
  });

  assert.equal(result.error, undefined, result.error?.message ?? "spawn failed");
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
});
