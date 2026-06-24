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
    it("includes the unnamed container with its parsed S3 store config", () => {
      // test-repo-v2's single container is unnamed (name: null) but carries an
      // S3 store config (region/endpoint/addressing) we use to build requests.
      const repoData = readFileSync(join(TEST_REPO_V2_PATH, "repo"));
      const repo = parseRepo(repoData);
      expect(repo.virtualChunkContainers).toHaveLength(1);
      const c = repo.virtualChunkContainers[0];
      expect(c.name).toBeNull();
      expect(c.urlPrefix).toBe("s3://testbucket/");
      expect(c.s3).toEqual({
        region: "us-east-1",
        endpointUrl: "http://localhost:4200",
        forcePathStyle: true,
      });
    });

    it("includes every container from a migrated v2 repo with parsed S3 config", () => {
      // test-repo-v2-migrated was produced by upgrading a v1 repo, which seeds
      // the config with the built-in named containers for each scheme.
      const repoData = readFileSync(join(TEST_REPO_V2_MIGRATED_PATH, "repo"));
      const repo = parseRepo(repoData);
      const byName = new Map(
        repo.virtualChunkContainers.map((c) => [c.name, c]),
      );

      expect(byName.get("s3")?.urlPrefix).toBe("s3://testbucket");
      expect(byName.get("s3")?.s3).toEqual({
        region: "us-east-1",
        endpointUrl: "http://localhost:4200",
        forcePathStyle: true,
      });
      // Tigris uses S3Options too: endpoint + addressing, region unset (null).
      expect(byName.get("tigris")?.s3).toEqual({
        endpointUrl: "https://fly.storage.tigris.dev",
        forcePathStyle: false,
      });
      // Non-S3 stores carry no S3 config.
      expect(byName.get("gcs")?.urlPrefix).toBe("gcs");
      expect(byName.get("gcs")?.s3).toBeUndefined();
      expect(byName.get("az")?.s3).toBeUndefined();
      expect(byName.get("file")?.s3).toBeUndefined();
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
