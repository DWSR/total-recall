import { execFileSync } from "node:child_process";
import { access, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import process from "node:process";

const packageName = "@dwsr/pi-harness-events";
const packagedFiles = [
  "LICENSE",
  "README.md",
  "package.json",
  "src/adapter.ts",
  "src/config.ts",
  "src/dispatcher.ts",
  "src/index.ts",
  "src/model.ts",
  "src/queue.ts",
] as const;
const packagedFileSet = new Set<string>(packagedFiles);
const expectedExtension = "./src/index.ts";

interface PackedFile {
  readonly path: string;
}

interface PackedArchive {
  readonly filename: string;
  readonly files: readonly PackedFile[];
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null;
}

function isUnknownArray(value: unknown): value is readonly unknown[] {
  return Array.isArray(value);
}

function hasExactStringArray(value: unknown, expected: readonly string[]): boolean {
  return (
    isUnknownArray(value) &&
    value.length === expected.length &&
    value.every((entry, index) => typeof entry === "string" && entry === expected[index])
  );
}

function hasExactStringRecord(value: unknown, expected: Readonly<Record<string, string>>): boolean {
  if (!isRecord(value)) {
    return false;
  }

  const entries = Object.entries(value);

  return (
    entries.length === Object.keys(expected).length &&
    entries.every(([key, entry]) => typeof entry === "string" && entry === expected[key])
  );
}

function hasStringRecordEntry(value: unknown, key: string, expected: string): boolean {
  return isRecord(value) && value[key] === expected;
}

function isPackedArchive(value: unknown): value is PackedArchive {
  if (!isRecord(value) || typeof value.filename !== "string" || !isUnknownArray(value.files)) {
    return false;
  }

  return value.files.every((file) => isRecord(file) && typeof file.path === "string");
}

function assertPackedFiles(archive: PackedArchive): void {
  const packedPaths = new Set(archive.files.map((file) => file.path));

  if (packedPaths.size !== archive.files.length) {
    throw new Error("npm pack reported duplicate archive file paths");
  }

  const missingFiles = packagedFiles.filter((file) => !packedPaths.has(file));

  if (missingFiles.length > 0) {
    throw new Error(`npm pack omitted required files: ${missingFiles.join(", ")}`);
  }

  const unexpectedFiles = [...packedPaths].filter((file) => !packagedFileSet.has(file)).sort();

  if (unexpectedFiles.length > 0) {
    throw new Error(`unexpected package archive file: ${unexpectedFiles.join(", ")}`);
  }
}

function assertPackageManifest(value: unknown): string {
  if (!isRecord(value)) {
    throw new Error("installed package manifest is not an object");
  }

  if (value.name !== packageName) {
    throw new Error("installed package manifest has an unexpected name");
  }

  if (value.license !== "MIT" || value.type !== "module") {
    throw new Error("installed package manifest does not declare the expected module contract");
  }

  if (!hasExactStringRecord(value.engines, { node: "^24.0.0" })) {
    throw new Error("installed package manifest does not declare the Node 24 runtime contract");
  }

  if (!hasExactStringArray(value.files, ["src", "README.md", "LICENSE"])) {
    throw new Error("installed package manifest does not declare the source package allowlist");
  }

  if (!hasExactStringRecord(value.dependencies, {})) {
    throw new Error("installed package manifest has production dependencies");
  }

  if (
    value.optionalDependencies !== undefined &&
    !hasExactStringRecord(value.optionalDependencies, {})
  ) {
    throw new Error("installed package manifest has optional production dependencies");
  }

  if (!hasExactStringRecord(value.peerDependencies, { "@earendil-works/pi-coding-agent": "*" })) {
    throw new Error("installed package manifest does not declare the Pi peer dependency");
  }

  if (!hasStringRecordEntry(value.devDependencies, "@earendil-works/pi-coding-agent", "0.86.1")) {
    throw new Error("installed package manifest does not pin Pi for development verification");
  }

  if (!isRecord(value.pi) || !hasExactStringArray(value.pi.extensions, [expectedExtension])) {
    throw new Error("installed package manifest does not expose the Jiti TypeScript extension");
  }

  return expectedExtension;
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

const npmCli = process.env.npm_execpath;

if (npmCli === undefined) {
  throw new Error("npm_execpath is required to create a package archive");
}

const destination = await mkdtemp(join(tmpdir(), "pi-harness-events-pack-"));

try {
  const output = execFileSync(
    process.execPath,
    [npmCli, "pack", "--json", "--ignore-scripts", "--pack-destination", destination],
    {
      encoding: "utf8",
      env: {
        ...process.env,
        npm_config_cache: join(destination, "npm-cache"),
      },
    },
  );
  const archives: unknown = JSON.parse(output);

  if (
    !isRecord(archives) ||
    Object.keys(archives).length !== 1 ||
    !isPackedArchive(archives[packageName])
  ) {
    throw new Error("npm pack did not report exactly one archive");
  }

  const archive = archives[packageName];
  assertPackedFiles(archive);

  const archivePath = resolve(destination, archive.filename);
  await access(archivePath);

  const consumer = await mkdtemp(join(destination, "consumer-"));
  await writeFile(
    join(consumer, "package.json"),
    `${JSON.stringify({ name: "pi-harness-events-pack-check", private: true, version: "0.0.0" })}\n`,
    "utf8",
  );

  execFileSync(
    process.execPath,
    [
      npmCli,
      "install",
      "--offline",
      "--ignore-scripts",
      "--legacy-peer-deps",
      "--no-package-lock",
      "--no-audit",
      "--no-fund",
      archivePath,
    ],
    {
      cwd: consumer,
      encoding: "utf8",
      env: {
        ...process.env,
        npm_config_audit: "false",
        npm_config_cache: join(destination, "npm-cache"),
        npm_config_fund: "false",
        npm_config_ignore_scripts: "true",
        npm_config_legacy_peer_deps: "true",
        npm_config_offline: "true",
        npm_config_package_lock: "false",
      },
    },
  );

  const installedPackage = join(consumer, "node_modules", ...packageName.split("/"));
  const manifest: unknown = JSON.parse(
    await readFile(join(installedPackage, "package.json"), "utf8"),
  );
  const extensionPath = assertPackageManifest(manifest);
  const installedEntrypoint = resolve(installedPackage, extensionPath);

  if (installedEntrypoint !== resolve(installedPackage, "src/index.ts")) {
    throw new Error("installed package manifest resolved an unexpected extension path");
  }

  await access(installedEntrypoint);

  if (await pathExists(join(consumer, "node_modules", "@earendil-works", "pi-coding-agent"))) {
    throw new Error("isolated install unexpectedly installed the Pi peer dependency");
  }
} finally {
  await rm(destination, { force: true, recursive: true });
}
