/**
 * Tests for transaction log FlatBuffer parsing against real test data.
 *
 * Reads actual transaction log files from icechunk-python/tests/data/test-repo-v2
 * to verify parsing produces valid results.
 */

import { describe, it, expect } from "vitest";
import { readFileSync, readdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { decompress } from "fzstd";
import { parseTransactionLog } from "../../../src/format/flatbuffers/transaction-log-parser.js";
import {
  parseHeader,
  getDataAfterHeader,
  CompressionAlgorithm,
} from "../../../src/format/header.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const TEST_REPO_V2_PATH = join(
  __dirname,
  "../../../../icechunk-python/tests/data/test-repo-v2",
);

/** Read and parse a transaction log file (header + optional zstd + FlatBuffer) */
function readTransactionLog(filePath: string) {
  const data = readFileSync(filePath);
  const header = parseHeader(data);
  let fbData = getDataAfterHeader(data);
  if (header.compression === CompressionAlgorithm.Zstd) {
    fbData = decompress(fbData);
  }
  return parseTransactionLog(fbData);
}

describe("transaction-log-parser (real data)", () => {
  const txDir = join(TEST_REPO_V2_PATH, "transactions");
  const txFiles = readdirSync(txDir);

  it("test repo has transaction log files", () => {
    expect(txFiles.length).toBeGreaterThan(0);
  });

  it("all transaction logs parse without errors", () => {
    for (const file of txFiles) {
      const entry = readTransactionLog(join(txDir, file));
      expect(entry.id).toBeInstanceOf(Uint8Array);
      expect(entry.id.length).toBe(12);
    }
  });

  it("transaction log fields are valid arrays", () => {
    for (const file of txFiles) {
      const entry = readTransactionLog(join(txDir, file));

      expect(Array.isArray(entry.newGroups)).toBe(true);
      expect(Array.isArray(entry.newArrays)).toBe(true);
      expect(Array.isArray(entry.deletedGroups)).toBe(true);
      expect(Array.isArray(entry.deletedArrays)).toBe(true);
      expect(Array.isArray(entry.updatedArrays)).toBe(true);
      expect(Array.isArray(entry.updatedGroups)).toBe(true);
      expect(Array.isArray(entry.updatedChunks)).toBe(true);
      expect(Array.isArray(entry.movedNodes)).toBe(true);

      // All ObjectId8 entries should be 8 bytes
      for (const id of [
        ...entry.newGroups,
        ...entry.newArrays,
        ...entry.deletedGroups,
        ...entry.deletedArrays,
        ...entry.updatedArrays,
        ...entry.updatedGroups,
      ]) {
        expect(id).toBeInstanceOf(Uint8Array);
        expect(id.length).toBe(8);
      }

      // Updated chunks should have valid nodeId and coords
      for (const chunk of entry.updatedChunks) {
        expect(chunk.nodeId).toBeInstanceOf(Uint8Array);
        expect(chunk.nodeId.length).toBe(8);
        expect(Array.isArray(chunk.chunks)).toBe(true);
        for (const indices of chunk.chunks) {
          expect(Array.isArray(indices.coords)).toBe(true);
        }
      }

      // Move operations should have string from/to
      for (const move of entry.movedNodes) {
        expect(typeof move.from).toBe("string");
        expect(typeof move.to).toBe("string");
      }
    }
  });

  it("at least one transaction log has changes", () => {
    let hasChanges = false;
    for (const file of txFiles) {
      const entry = readTransactionLog(join(txDir, file));
      const totalChanges =
        entry.newGroups.length +
        entry.newArrays.length +
        entry.deletedGroups.length +
        entry.deletedArrays.length +
        entry.updatedArrays.length +
        entry.updatedGroups.length +
        entry.updatedChunks.length +
        entry.movedNodes.length;
      if (totalChanges > 0) {
        hasChanges = true;
        break;
      }
    }
    expect(hasChanges).toBe(true);
  });
});
