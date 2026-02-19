/**
 * Tests for snapshot FlatBuffer parsing against real test data.
 *
 * Reads actual snapshot files from icechunk-python/tests/data/test-repo-v2
 * to verify parsing produces valid results — especially for ManifestFileInfo
 * structs which require correct alignment-aware byte offsets.
 */

import { describe, it, expect } from "vitest";
import { readFileSync, readdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { decompress } from "fzstd";
import { parseSnapshot } from "../../../src/format/flatbuffers/snapshot-parser.js";
import {
  parseHeader,
  getDataAfterHeader,
  CompressionAlgorithm,
} from "../../../src/format/header.js";
import { encodeObjectId12 } from "../../../src/format/object-id.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const TEST_REPO_V2_PATH = join(
  __dirname,
  "../../../../icechunk-python/tests/data/test-repo-v2",
);

/** Read and parse a snapshot file (header + optional zstd + FlatBuffer) */
function readSnapshot(filePath: string) {
  const data = readFileSync(filePath);
  const header = parseHeader(data);
  let fbData = getDataAfterHeader(data);
  if (header.compression === CompressionAlgorithm.Zstd) {
    fbData = decompress(fbData);
  }
  return parseSnapshot(fbData);
}

describe("snapshot-parser (real data)", () => {
  const snapshotDir = join(TEST_REPO_V2_PATH, "snapshots");
  const snapshotFiles = readdirSync(snapshotDir);
  const manifestDir = join(TEST_REPO_V2_PATH, "manifests");
  const manifestFiles = readdirSync(manifestDir);

  it("test repo has snapshots and manifests", () => {
    expect(snapshotFiles.length).toBeGreaterThan(0);
    expect(manifestFiles.length).toBeGreaterThan(0);
  });

  it("all snapshots parse without errors", () => {
    for (const file of snapshotFiles) {
      const snapshot = readSnapshot(join(snapshotDir, file));
      expect(snapshot.id).toBeInstanceOf(Uint8Array);
      expect(snapshot.id.length).toBe(12);
    }
  });

  it("at least one snapshot has non-empty manifestFiles", () => {
    let found = false;
    for (const file of snapshotFiles) {
      const snapshot = readSnapshot(join(snapshotDir, file));
      if (snapshot.manifestFiles.length > 0) {
        found = true;
        break;
      }
    }
    expect(found).toBe(true);
  });

  it("manifestFiles have valid id, sizeBytes, and numChunkRefs", () => {
    for (const file of snapshotFiles) {
      const snapshot = readSnapshot(join(snapshotDir, file));
      for (const mf of snapshot.manifestFiles) {
        // ID should be a 12-byte ObjectId
        expect(mf.id).toBeInstanceOf(Uint8Array);
        expect(mf.id.length).toBe(12);

        // sizeBytes should be a reasonable positive number (not garbage from wrong offset)
        expect(mf.sizeBytes).toBeGreaterThan(0);
        expect(mf.sizeBytes).toBeLessThan(1e12); // < 1 TB

        // numChunkRefs should be reasonable
        expect(mf.numChunkRefs).toBeGreaterThan(0);
        expect(mf.numChunkRefs).toBeLessThan(1e9);
      }
    }
  });

  it("manifestFiles IDs correspond to files in manifests/", () => {
    // Collect all manifest IDs referenced by any snapshot
    const referencedIds = new Set<string>();

    for (const file of snapshotFiles) {
      const snapshot = readSnapshot(join(snapshotDir, file));
      for (const mf of snapshot.manifestFiles) {
        referencedIds.add(encodeObjectId12(mf.id));
      }
    }

    // Every referenced manifest ID should have a corresponding file
    for (const id of referencedIds) {
      expect(
        manifestFiles.includes(id),
        `Manifest ID ${id} referenced by snapshot but not found in manifests/`,
      ).toBe(true);
    }
  });
});
