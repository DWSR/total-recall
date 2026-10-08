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

    assert.equal(result.error, undefined, result.error?.message);
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

    assert.equal(result.error, undefined, result.error?.message);
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

    assert.equal(result.error, undefined, result.error?.message);
    assert.notEqual(result.status, 0);
    assert.match(
      `${result.stdout}\n${result.stderr}`,
      /installed package manifest has optional production dependencies/,
    );
  } finally {
    await writeFile(packageJsonPath, originalPackageJson, "utf8");
  }
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

  assert.equal(result.error, undefined, result.error?.message);
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
});
