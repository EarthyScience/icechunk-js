#!/usr/bin/env node
/**
 * Sync files from the main icechunk repo via sparse clone.
 *
 * Usage:
 *   node scripts/sync.mjs fixtures                    # sync test fixtures
 *   node scripts/sync.mjs flatbuffers                 # sync FlatBuffer schemas
 *   node scripts/sync.mjs fixtures flatbuffers        # sync both
 *   node scripts/sync.mjs --if-missing fixtures       # only if dest dir is missing
 *   node scripts/sync.mjs --branch some-branch fixtures
 */
import { execSync } from "node:child_process";
import { mkdtempSync, rmSync, mkdirSync, cpSync, existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";

const REPO = "https://github.com/earth-mover/icechunk.git";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROJECT_DIR = dirname(__dirname);

const TARGETS = {
  fixtures: {
    sparse: "icechunk-python/tests/data/",
    src: ["icechunk-python", "tests", "data"],
    dest: join(PROJECT_DIR, "tests", "data"),
    label: "Fixtures",
  },
  flatbuffers: {
    sparse: "icechunk-format/flatbuffers/",
    src: ["icechunk-format", "flatbuffers"],
    dest: join(PROJECT_DIR, "flatbuffers"),
    label: "FlatBuffer schemas",
  },
};

// Parse args
let branch = "main";
let ifMissing = false;
const targets = [];

const args = process.argv.slice(2);
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--branch") {
    branch = args[++i];
  } else if (args[i] === "--if-missing") {
    ifMissing = true;
  } else if (TARGETS[args[i]]) {
    targets.push(args[i]);
  } else {
    console.error(`Unknown argument: ${args[i]}`);
    console.error(`Targets: ${Object.keys(TARGETS).join(", ")}`);
    process.exit(1);
  }
}

if (targets.length === 0) {
  console.error(
    "Usage: node scripts/sync.mjs [--if-missing] [--branch <branch>] <fixtures|flatbuffers> ...",
  );
  process.exit(1);
}

// Filter out targets that already exist when --if-missing is set
const toSync = targets.filter((t) => {
  if (ifMissing && existsSync(TARGETS[t].dest)) {
    return false;
  }
  return true;
});

if (toSync.length === 0) {
  process.exit(0);
}

function run(cmd, opts = {}) {
  execSync(cmd, { stdio: "inherit", ...opts });
}

const tmp = mkdtempSync(join(tmpdir(), "icechunk-sync-"));

try {
  console.log(`Cloning icechunk repo (sparse, branch=${branch})...`);
  run(
    `git clone --depth 1 --no-checkout --branch "${branch}" ${REPO} "${tmp}"`,
  );
  run(`git sparse-checkout init --no-cone`, { cwd: tmp });
  run(
    `git sparse-checkout set ${toSync.map((t) => TARGETS[t].sparse).join(" ")}`,
    { cwd: tmp },
  );
  run("git checkout", { cwd: tmp });

  for (const name of toSync) {
    const t = TARGETS[name];
    rmSync(t.dest, { recursive: true, force: true });
    mkdirSync(t.dest, { recursive: true });
    cpSync(join(tmp, ...t.src), t.dest, { recursive: true });
    console.log(`${t.label} synced from ${REPO} @ ${branch}`);
  }
} finally {
  rmSync(tmp, { recursive: true, force: true });
}
