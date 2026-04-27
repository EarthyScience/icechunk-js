import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import {
  parseRepo,
  listBranchesFromRepo,
  listTagsFromRepo,
  resolveBranch,
  resolveTag,
} from "../../../src/format/flatbuffers/repo-parser.js";
import { encodeObjectId12 } from "../../../src/format/object-id.js";

// ESM equivalent of __dirname
const __dirname = dirname(fileURLToPath(import.meta.url));

// Path to v2 test repository
const TEST_REPO_V2_PATH = join(__dirname, "../../data/test-repo-v2");
const TEST_REPO_V2_MIGRATED_PATH = join(
  __dirname,
  "../../data/test-repo-v2-migrated",
);

describe("repo-parser", () => {
  describe("parseRepo", () => {
    it("should parse a valid v2 repo file", () => {
      const repoData = readFileSync(join(TEST_REPO_V2_PATH, "repo"));
      const repo = parseRepo(repoData);

      expect(repo.specVersion).toBe(2);
      expect(repo.snapshotsLength).toBeGreaterThan(0);
    });

    it("should throw on invalid data", () => {
      expect(() => parseRepo(new Uint8Array([1, 2, 3]))).toThrow();
    });

    it("should throw on empty data", () => {
      expect(() => parseRepo(new Uint8Array([]))).toThrow("too small");
    });
  });

  describe("listBranchesFromRepo", () => {
    it("should list branches from v2 repo", () => {
      const repoData = readFileSync(join(TEST_REPO_V2_PATH, "repo"));
      const repo = parseRepo(repoData);
      const branches = listBranchesFromRepo(repo);

      expect(branches).toContain("main");
      expect(Array.isArray(branches)).toBe(true);
    });
  });

  describe("listTagsFromRepo", () => {
    it("should list tags from v2 repo", () => {
      const repoData = readFileSync(join(TEST_REPO_V2_PATH, "repo"));
      const repo = parseRepo(repoData);
      const tags = listTagsFromRepo(repo);

      expect(Array.isArray(tags)).toBe(true);
    });
  });

  describe("resolveBranch", () => {
    it("should resolve main branch to snapshot ID", () => {
      const repoData = readFileSync(join(TEST_REPO_V2_PATH, "repo"));
      const repo = parseRepo(repoData);
      const snapshotId = resolveBranch(repo, "main");

      expect(snapshotId).not.toBeNull();
      expect(snapshotId).toBeInstanceOf(Uint8Array);
      expect(snapshotId!.length).toBe(12);

      // Verify it's a valid base32-encodable ID
      const encoded = encodeObjectId12(snapshotId!);
      expect(typeof encoded).toBe("string");
      expect(encoded.length).toBeGreaterThan(0);
    });

    it("should return null for non-existent branch", () => {
      const repoData = readFileSync(join(TEST_REPO_V2_PATH, "repo"));
      const repo = parseRepo(repoData);
      const snapshotId = resolveBranch(repo, "nonexistent-branch");

      expect(snapshotId).toBeNull();
    });
  });

  describe("resolveTag", () => {
    it("should return null for non-existent tag", () => {
      const repoData = readFileSync(join(TEST_REPO_V2_PATH, "repo"));
      const repo = parseRepo(repoData);
      const snapshotId = resolveTag(repo, "nonexistent-tag");

      expect(snapshotId).toBeNull();
    });
  });

  describe("virtualChunkContainers", () => {
    it("is empty when the only container has no name", () => {
      // test-repo-v2 is created via `ic.VirtualChunkContainer("s3://testbucket/", store)`
      // with no `name=` argument, so its single container is unnamed (name: null)
      // and MUST be excluded from the vcc:// resolution map.
      const repoData = readFileSync(join(TEST_REPO_V2_PATH, "repo"));
      const repo = parseRepo(repoData);
      expect(repo.virtualChunkContainers.size).toBe(0);
    });

    it("includes every named container from a migrated v2 repo", () => {
      // test-repo-v2-migrated was produced by upgrading a v1 repo, which seeds
      // the config with the built-in named containers for each scheme.
      const repoData = readFileSync(join(TEST_REPO_V2_MIGRATED_PATH, "repo"));
      const repo = parseRepo(repoData);
      expect(repo.virtualChunkContainers.get("s3")).toBe("s3://testbucket");
      expect(repo.virtualChunkContainers.get("gcs")).toBe("gcs");
      expect(repo.virtualChunkContainers.get("az")).toBe("az");
      expect(repo.virtualChunkContainers.get("tigris")).toBe("tigris");
      expect(repo.virtualChunkContainers.get("file")).toBe("file");
    });
  });

  describe("binary search", () => {
    it("should handle UTF-8 branch names correctly", () => {
      const repoData = readFileSync(join(TEST_REPO_V2_PATH, "repo"));
      const repo = parseRepo(repoData);

      // Test that binary search works with various branch name patterns
      // that might exist or not exist
      const testNames = [
        "main",
        "a", // before 'main' alphabetically
        "z", // after 'main' alphabetically
        "", // empty string
      ];

      for (const name of testNames) {
        // Should not throw even for non-existent branches
        const result = resolveBranch(repo, name);
        expect(result === null || result instanceof Uint8Array).toBe(true);
      }
    });
  });
});
