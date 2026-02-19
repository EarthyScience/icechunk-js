/**
 * Post-generation fixups for flatc-generated TypeScript.
 *
 * Usage: node scripts/postgen-fbs.cjs <flatc-output-dir>
 *
 * 1. Removes eslint-disable comments, unused union imports, and
 *    generic type params that cause strict typecheck failures.
 * 2. Strips static builder methods and getSizePrefixed accessors.
 */
const fs = require("fs");
const path = require("path");

const generatedDir = path.join(process.argv[2], "generated");

const tsFiles = fs
  .readdirSync(generatedDir)
  .filter((f) => f.endsWith(".ts"))
  .map((f) => path.join(generatedDir, f));

for (const filePath of tsFiles) {
  let src = fs.readFileSync(filePath, "utf8");
  const original = src;

  // Clean up TypeScript issues
  src = src.replace(/\/\* eslint-disable[^*]*\*\/\n*/g, "");
  src = src.replace(/, union\w+, unionList\w+/g, "");
  src = src.replace(/<T extends flatbuffers\.Table>/g, "");

  // Strip static builder methods (brace-depth-aware)
  const lines = src.split("\n");
  const kept = [];
  let depth = 0;
  let skipping = false;

  for (const line of lines) {
    if (
      !skipping &&
      (/^static\s+\w+\(builder\s*[,:)]/.test(line.trim()) ||
        /^static\s+getSizePrefixed/.test(line.trim()))
    ) {
      skipping = true;
      depth = 0;
    }

    if (skipping) {
      for (const ch of line) {
        if (ch === "{") depth++;
        if (ch === "}") depth--;
      }
      if (depth <= 0 && line.includes("}")) {
        skipping = false;
      }
      continue;
    }

    kept.push(line);
  }

  // Collapse consecutive blank lines
  const cleaned = [];
  for (const line of kept) {
    if (
      line.trim() === "" &&
      cleaned.length > 0 &&
      cleaned[cleaned.length - 1].trim() === ""
    ) {
      continue;
    }
    cleaned.push(line);
  }

  const result = cleaned.join("\n");
  if (result !== original) {
    fs.writeFileSync(filePath, result);
  }
}
