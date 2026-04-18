#!/usr/bin/env node
/**
 * Generate TypeScript from FlatBuffer schemas using the pinned flatc binary.
 *
 * Usage:
 *   node scripts/generate-fbs.mjs
 */
import { execSync } from "node:child_process";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROJECT_DIR = dirname(__dirname);

// ensure-flatc prints the binary path to stdout
const flatc = execSync("node scripts/ensure-flatc.mjs", {
  cwd: PROJECT_DIR,
  encoding: "utf8",
}).trim();

execSync(
  `"${flatc}" -T -o ./src/format/flatbuffers --gen-all ./flatbuffers/all.fbs`,
  { stdio: "inherit", cwd: PROJECT_DIR },
);

execSync("node scripts/postgen-fbs.cjs ./src/format/flatbuffers", {
  stdio: "inherit",
  cwd: PROJECT_DIR,
});

execSync("npx prettier --write src/format/flatbuffers/generated", {
  stdio: "inherit",
  cwd: PROJECT_DIR,
});
