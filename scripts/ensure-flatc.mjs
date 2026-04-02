#!/usr/bin/env node
/**
 * Ensure the pinned flatc version is available at .bin/flatc (or .bin/flatc.exe on Windows).
 * Downloads it if missing or wrong version. Prints the path on stdout.
 *
 * Usage:
 *   node scripts/ensure-flatc.mjs
 *   # returns the path to the flatc binary
 */
import { execSync } from "node:child_process";
import { existsSync, mkdirSync, chmodSync, rmSync, mkdtempSync } from "node:fs";
import { join, dirname } from "node:path";
import { tmpdir, platform } from "node:os";
import { fileURLToPath } from "node:url";

const FLATC_VERSION = "25.12.19";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROJECT_DIR = dirname(__dirname);
const BIN_DIR = join(PROJECT_DIR, ".bin");
const isWindows = platform() === "win32";
const FLATC = join(BIN_DIR, isWindows ? "flatc.exe" : "flatc");

// Check if we already have the right version
if (existsSync(FLATC)) {
  try {
    const out = execSync(`"${FLATC}" --version`, { encoding: "utf8" });
    const match = out.match(/(\d+\.\d+\.\d+)/);
    if (match && match[1] === FLATC_VERSION) {
      process.stdout.write(FLATC);
      process.exit(0);
    }
    console.error(`flatc in .bin/ is v${match?.[1]}, need v${FLATC_VERSION}`);
  } catch {
    // binary exists but can't run — re-download
  }
}

// Determine asset name
let asset;
switch (platform()) {
  case "linux":
    asset = "Linux.flatc.binary.clang++-18.zip";
    break;
  case "darwin":
    asset = "Mac.flatc.binary.zip";
    break;
  case "win32":
    asset = "Windows.flatc.binary.zip";
    break;
  default:
    console.error(`error: cannot download flatc for ${platform()}`);
    console.error(`Install flatc v${FLATC_VERSION} to .bin/flatc manually.`);
    process.exit(1);
}

mkdirSync(BIN_DIR, { recursive: true });

const url = `https://github.com/google/flatbuffers/releases/download/v${FLATC_VERSION}/${asset}`;
const tmp = mkdtempSync(join(tmpdir(), "flatc-"));
const zipPath = join(tmp, "flatc.zip");

try {
  console.error(`Downloading flatc v${FLATC_VERSION}...`);

  // Use curl on unix, PowerShell on Windows
  if (isWindows) {
    execSync(
      `powershell -Command "Invoke-WebRequest -Uri '${url}' -OutFile '${zipPath}'"`,
      { stdio: ["pipe", "pipe", "inherit"] },
    );
    execSync(
      `powershell -Command "Expand-Archive -Path '${zipPath}' -DestinationPath '${BIN_DIR}' -Force"`,
      { stdio: ["pipe", "pipe", "inherit"] },
    );
  } else {
    execSync(`curl -fsSL "${url}" -o "${zipPath}"`, {
      stdio: ["pipe", "pipe", "inherit"],
    });
    execSync(`unzip -o -q "${zipPath}" -d "${BIN_DIR}/"`, {
      stdio: ["pipe", "pipe", "inherit"],
    });
    chmodSync(FLATC, 0o755);
  }

  process.stdout.write(FLATC);
} finally {
  rmSync(tmp, { recursive: true, force: true });
}
