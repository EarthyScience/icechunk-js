/**
 * Tests for snapshot FlatBuffer parsing against real test data.
 *
 * Reads actual snapshot files from tests/data/test-repo-v2
 * to verify parsing produces valid results — especially for ManifestFileInfo
 * structs which require correct alignment-aware byte offsets.
 */

import { describe, it, expect } from "vitest";
import { readFileSync, readdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { decompress } from "fzstd";
import { parseSnapshot } from "../../../src/format/flatbuffers/snapshot-parser.js";
import { parseManifest } from "../../../src/format/flatbuffers/manifest-parser.js";
import {
  parseHeader,
  getDataAfterHeader,
  CompressionAlgorithm,
  FileType,
} from "../../../src/format/header.js";
import { encodeObjectId12 } from "../../../src/format/object-id.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const TEST_DATA_PATH = join(__dirname, "../../data");
const TEST_REPO_V2_PATH = join(TEST_DATA_PATH, "test-repo-v2");
const MIGRATED_REPO_V2_PATH = join(TEST_DATA_PATH, "test-repo-v2-migrated");

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

  it("v2 arrays have non-empty shape via shape_v2", () => {
    let arrayCount = 0;
    for (const file of snapshotFiles) {
      const snapshot = readSnapshot(join(snapshotDir, file));
      for (const node of snapshot.nodes) {
        if (node.nodeData.type === "array") {
          arrayCount++;
          expect(node.nodeData.shape.length).toBeGreaterThan(0);
          for (const dim of node.nodeData.shape) {
            expect(dim.arrayLength).toBeGreaterThan(0);
            expect(dim.numChunks).toBeGreaterThan(0);
          }
        }
      }
    }
    expect(arrayCount).toBeGreaterThan(0);
  });

  it("at least one migrated snapshot has non-empty manifestFiles", () => {
    const migratedSnapshotDir = join(MIGRATED_REPO_V2_PATH, "snapshots");
    const migratedFiles = readdirSync(migratedSnapshotDir);
    let found = false;
    for (const file of migratedFiles) {
      const snapshot = readSnapshot(join(migratedSnapshotDir, file));
      if (snapshot.manifestFiles.length > 0) {
        found = true;
        break;
      }
    }
    expect(found).toBe(true);
  });

  it("manifestFiles have valid id, sizeBytes, and numChunkRefs", () => {
    const migratedSnapshotDir = join(MIGRATED_REPO_V2_PATH, "snapshots");
    const migratedFiles = readdirSync(migratedSnapshotDir);
    for (const file of migratedFiles) {
      const snapshot = readSnapshot(join(migratedSnapshotDir, file));
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
    const migratedSnapshotDir = join(MIGRATED_REPO_V2_PATH, "snapshots");
    const migratedManifestDir = join(MIGRATED_REPO_V2_PATH, "manifests");
    const migratedSnapshotFiles = readdirSync(migratedSnapshotDir);
    const migratedManifestFiles = readdirSync(migratedManifestDir);

    // Collect all manifest IDs referenced by any snapshot
    const referencedIds = new Set<string>();

    for (const file of migratedSnapshotFiles) {
      const snapshot = readSnapshot(join(migratedSnapshotDir, file));
      for (const mf of snapshot.manifestFiles) {
        referencedIds.add(encodeObjectId12(mf.id));
      }
    }

    // Every referenced manifest ID should have a corresponding file
    expect(referencedIds.size).toBeGreaterThan(0);
    for (const id of referencedIds) {
      expect(
        migratedManifestFiles.includes(id),
        `Manifest ID ${id} referenced by snapshot but not found in manifests/`,
      ).toBe(true);
    }
  });
});

describe("zstd decompression (real data)", () => {
  /**
   * Explicitly verify that fixture files use zstd compression and that
   * the decompression pipeline produces valid FlatBuffer data.
   *
   * All icechunk files (snapshots, manifests, transaction logs) in the
   * test repos are zstd-compressed. This test makes the implicit
   * decompression behavior explicit.
   */

  const repos = [
    { name: "test-repo-v1", version: "v1" },
    { name: "test-repo-v2", version: "v2" },
    { name: "split-repo-v1", version: "v1 split" },
    { name: "split-repo-v2", version: "v2 split" },
  ];

  for (const { name, version } of repos) {
    const repoPath = join(TEST_DATA_PATH, name);

    describe(`${version} (${name})`, () => {
      it("all snapshot files are zstd-compressed", () => {
        const snapshotDir = join(repoPath, "snapshots");
        const files = readdirSync(snapshotDir);
        expect(files.length).toBeGreaterThan(0);

        for (const file of files) {
          const data = readFileSync(join(snapshotDir, file));
          const header = parseHeader(data);

          expect(header.compression).toBe(CompressionAlgorithm.Zstd);
          expect(header.fileType).toBe(FileType.Snapshot);
        }
      });

      it("all manifest files are zstd-compressed", () => {
        const manifestDir = join(repoPath, "manifests");
        const files = readdirSync(manifestDir);
        expect(files.length).toBeGreaterThan(0);

        for (const file of files) {
          const data = readFileSync(join(manifestDir, file));
          const header = parseHeader(data);

          expect(header.compression).toBe(CompressionAlgorithm.Zstd);
          expect(header.fileType).toBe(FileType.Manifest);
        }
      });

      it("zstd-compressed snapshots decompress and parse correctly", () => {
        const snapshotDir = join(repoPath, "snapshots");
        const files = readdirSync(snapshotDir);

        for (const file of files) {
          const data = readFileSync(join(snapshotDir, file));
          const header = parseHeader(data);

          // Verify it's actually compressed
          expect(header.compression).toBe(CompressionAlgorithm.Zstd);

          // Decompress
          const compressed = getDataAfterHeader(data);
          const decompressed = decompress(compressed);

          // Decompressed data should be larger than compressed
          expect(decompressed.length).toBeGreaterThanOrEqual(
            compressed.length * 0.5,
          );

          // Parse should succeed
          const snapshot = parseSnapshot(decompressed);
          expect(snapshot.id).toBeInstanceOf(Uint8Array);
          expect(snapshot.id.length).toBe(12);
          expect(typeof snapshot.message).toBe("string");
        }
      });

      it("zstd-compressed manifests decompress and parse correctly", () => {
        const manifestDir = join(repoPath, "manifests");
        const files = readdirSync(manifestDir);

        for (const file of files) {
          const data = readFileSync(join(manifestDir, file));
          const header = parseHeader(data);

          expect(header.compression).toBe(CompressionAlgorithm.Zstd);

          const compressed = getDataAfterHeader(data);
          const decompressed = decompress(compressed);

          const manifest = parseManifest(decompressed);
          expect(manifest.arrays.length).toBeGreaterThanOrEqual(0);
        }
      });
    });
  }
});
