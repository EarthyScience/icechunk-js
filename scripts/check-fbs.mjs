#!/usr/bin/env node
/**
 * Check that generated FlatBuffer TypeScript is up to date.
 * Generates into a temp directory and diffs against the committed files.
 *
 * Usage:
 *   node scripts/check-fbs.mjs
 */
import { execSync } from "node:child_process";
import { mkdtempSync, rmSync, readdirSync, readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROJECT_DIR = dirname(__dirname);

const tmp = mkdtempSync(join(tmpdir(), "fbs-check-"));

try {
  const flatc = execSync("node scripts/ensure-flatc.mjs", {
    cwd: PROJECT_DIR,
    encoding: "utf8",
  }).trim();

  execSync(`"${flatc}" -T -o "${tmp}" --gen-all ./flatbuffers/all.fbs`, {
    stdio: "inherit",
    cwd: PROJECT_DIR,
  });

  execSync(`node scripts/postgen-fbs.cjs "${tmp}"`, {
    stdio: "inherit",
    cwd: PROJECT_DIR,
  });

  execSync(`npx prettier --write "${join(tmp, "generated")}"`, {
    stdio: "inherit",
    cwd: PROJECT_DIR,
  });

  // Compare generated files
  const generatedDir = join(tmp, "generated");
  const committedDir = join(
    PROJECT_DIR,
    "src",
    "format",
    "flatbuffers",
    "generated",
  );

  const genFiles = readdirSync(generatedDir).sort();
  const comFiles = readdirSync(committedDir).sort();

  let dirty = false;

  if (genFiles.join(",") !== comFiles.join(",")) {
    console.error("File list mismatch:");
    console.error("  generated:", genFiles.join(", "));
    console.error("  committed:", comFiles.join(", "));
    dirty = true;
  } else {
    for (const file of genFiles) {
      const a = readFileSync(join(generatedDir, file), "utf8");
      const b = readFileSync(join(committedDir, file), "utf8");
      if (a !== b) {
        console.error(`DIFF: ${file}`);
        dirty = true;
      }
    }
  }

  if (dirty) {
    console.error(
      "Generated FlatBuffer code is out of date. Run: npm run generate:fbs",
    );
    process.exitCode = 1;
  } else {
    console.log("FlatBuffer generated code is up to date.");
  }
} finally {
  rmSync(tmp, { recursive: true, force: true });
}
