import { describe, it, expect } from "vitest";
import { readFileSync, readdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { Repository } from "../../src/reader/repository.js";
import {
  MockStorage,
  MockStorageNoList,
  createMockSnapshotId,
  createMockRefJson,
} from "../fixtures/mock-storage.js";
import {
  getBranchRefPath,
  getBranchRefDirPath,
  getTagRefPath,
  getTagRefDirPath,
  REPO_INFO_PATH,
} from "../../src/format/constants.js";
import { encodeObjectId12 } from "../../src/format/object-id.js";

// ESM equivalent of __dirname
const __dirname = dirname(fileURLToPath(import.meta.url));

// Path to v2 test repository
const TEST_REPO_V2_PATH = join(__dirname, "../data/test-repo-v2");

describe("Repository", () => {
  describe("open", () => {
    it("should throw on corrupted v2 repo file", async () => {
      const storage = new MockStorage({
        [REPO_INFO_PATH]: "not a valid repo file",
      });

      await expect(Repository.open({ storage })).rejects.toThrow(
        "Failed to parse v2 repo file",
      );
    });

    it("should open a valid v1 repository with main branch", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });
      expect(repo).toBeInstanceOf(Repository);
    });

    it("should fail on corrupted v2 repo even if v1 main branch exists", async () => {
      // When repo file exists but is corrupted, should fail fast (not fall back to v1)
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [REPO_INFO_PATH]: "corrupted repo file",
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
      });

      await expect(Repository.open({ storage })).rejects.toThrow(
        "Failed to parse v2 repo file",
      );
    });

    it("should throw on invalid repository (no repo info or main branch)", async () => {
      const storage = new MockStorage({});

      await expect(Repository.open({ storage })).rejects.toThrow(
        "Not a valid icechunk repository",
      );
    });

    it("should throw on repository with only a feature branch", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [getBranchRefPath("feature")]: createMockRefJson(snapshotId),
      });

      await expect(Repository.open({ storage })).rejects.toThrow(
        "Not a valid icechunk repository",
      );
    });
  });

  describe("listBranches", () => {
    it("should list branches including those with slashes (v1 format)", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
        [getBranchRefPath("feature/test")]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });
      const branches = await repo.listBranches();

      expect(branches).toContain("main");
      expect(branches).toContain("feature/test");
      expect(branches).toHaveLength(2);
    });

    it("should return empty array when no branches exist (v1 format)", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });

      // Clear the storage to simulate no branches after opening
      storage.setFiles({});

      const branches = await repo.listBranches();
      expect(branches).toEqual([]);
    });

    it("should throw when listPrefix not supported (v1 format)", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorageNoList({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });

      await expect(repo.listBranches()).rejects.toThrow(
        "storage does not support listing",
      );
    });

    it("should exclude branches with deletion tombstones (v1 format)", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        // Active branch (no tombstone)
        [`${getBranchRefDirPath("main")}ZZZZZZZZ.json`]:
          createMockRefJson(snapshotId),
        // Deleted branch (ref file + tombstone)
        [`${getBranchRefDirPath("deleted-branch")}XXXXXXXX.json`]:
          createMockRefJson(snapshotId),
        [`${getBranchRefDirPath("deleted-branch")}XXXXXXXX.json.deleted`]: "",
      });

      const repo = await Repository.open({ storage });
      const branches = await repo.listBranches();

      expect(branches).toContain("main");
      expect(branches).not.toContain("deleted-branch");
      expect(branches).toHaveLength(1);
    });

    it("should include branch if older version deleted but newer exists (v1 format)", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
        // Branch with deleted old version and active new version
        [`${getBranchRefDirPath("active")}AAAAAAAA.json`]:
          createMockRefJson(snapshotId),
        [`${getBranchRefDirPath("active")}AAAAAAAA.json.deleted`]: "",
        [`${getBranchRefDirPath("active")}ZZZZZZZZ.json`]:
          createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });
      const branches = await repo.listBranches();

      expect(branches).toContain("active");
    });

    it("should handle versioned ref filenames (v1 format)", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [`${getBranchRefDirPath("main")}ZZZZZZZZ.json`]:
          createMockRefJson(snapshotId),
        [`${getBranchRefDirPath("develop")}ABC12345.json`]:
          createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });
      const branches = await repo.listBranches();

      expect(branches).toContain("main");
      expect(branches).toContain("develop");
      expect(branches).toHaveLength(2);
    });
  });

  describe("listTags", () => {
    it("should list tags including those with slashes (v1 format)", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
        [getTagRefPath("v1.0")]: createMockRefJson(snapshotId),
        [getTagRefPath("release/v2.0")]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });
      const tags = await repo.listTags();

      expect(tags).toContain("v1.0");
      expect(tags).toContain("release/v2.0");
      expect(tags).toHaveLength(2);
    });

    it("should return empty array when no tags exist (v1 format)", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });
      const tags = await repo.listTags();

      expect(tags).toEqual([]);
    });

    it("should throw when listPrefix not supported (v1 format)", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorageNoList({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
        [getTagRefPath("v1.0")]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });

      await expect(repo.listTags()).rejects.toThrow(
        "storage does not support listing",
      );
    });

    it("should exclude tags with deletion tombstones (v1 format)", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
        // Active tag (no tombstone)
        [`${getTagRefDirPath("v1.0")}ZZZZZZZZ.json`]:
          createMockRefJson(snapshotId),
        // Deleted tag (ref file + tombstone)
        [`${getTagRefDirPath("old-tag")}XXXXXXXX.json`]:
          createMockRefJson(snapshotId),
        [`${getTagRefDirPath("old-tag")}XXXXXXXX.json.deleted`]: "",
      });

      const repo = await Repository.open({ storage });
      const tags = await repo.listTags();

      expect(tags).toContain("v1.0");
      expect(tags).not.toContain("old-tag");
      expect(tags).toHaveLength(1);
    });

    it("should exclude tag even with multiple versions if latest is deleted (v1 format)", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
        // Tag with multiple versions, latest is deleted
        [`${getTagRefDirPath("deleted-tag")}AAAAAAAA.json`]:
          createMockRefJson(snapshotId),
        [`${getTagRefDirPath("deleted-tag")}ZZZZZZZZ.json`]:
          createMockRefJson(snapshotId),
        [`${getTagRefDirPath("deleted-tag")}ZZZZZZZZ.json.deleted`]: "",
      });

      const repo = await Repository.open({ storage });
      const tags = await repo.listTags();

      expect(tags).not.toContain("deleted-tag");
      expect(tags).toHaveLength(0);
    });
  });

  describe("ref resolution edge cases", () => {
    it("should open repo with versioned main branch file (v1 format)", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [`${getBranchRefDirPath("main")}ZZZZZZZZ.json`]:
          createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });
      expect(repo).toBeInstanceOf(Repository);
    });
  });

  describe("checkoutBranch", () => {
    it("should throw on non-existent branch (v1 format)", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });

      await expect(repo.checkoutBranch("nonexistent")).rejects.toThrow(
        "Reference not found",
      );
    });

    it("should throw on invalid ref.json content (v1 format)", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
        [getBranchRefPath("broken")]: "not valid json",
      });

      const repo = await Repository.open({ storage });

      await expect(repo.checkoutBranch("broken")).rejects.toThrow();
    });

    it("should throw on deleted branch (with tombstone, v1 format)", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
        // Deleted branch (ref file + tombstone)
        [`${getBranchRefDirPath("deleted")}ZZZZZZZZ.json`]:
          createMockRefJson(snapshotId),
        [`${getBranchRefDirPath("deleted")}ZZZZZZZZ.json.deleted`]: "",
      });

      const repo = await Repository.open({ storage });

      await expect(repo.checkoutBranch("deleted")).rejects.toThrow(
        "Reference not found",
      );
    });
  });

  describe("checkoutTag", () => {
    it("should throw on non-existent tag (v1 format)", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });

      await expect(repo.checkoutTag("nonexistent")).rejects.toThrow(
        "Reference not found",
      );
    });

    it("should throw on deleted tag (with tombstone, v1 format)", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
        // Deleted tag (ref file + tombstone)
        [`${getTagRefDirPath("deleted")}ZZZZZZZZ.json`]:
          createMockRefJson(snapshotId),
        [`${getTagRefDirPath("deleted")}ZZZZZZZZ.json.deleted`]: "",
      });

      const repo = await Repository.open({ storage });

      await expect(repo.checkoutTag("deleted")).rejects.toThrow(
        "Reference not found",
      );
    });

    it("should throw on deleted tag (with tombstone, v1 no-list fallback)", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorageNoList({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
        // Deleted tag (legacy ref.json + tombstone)
        [getTagRefPath("deleted")]: createMockRefJson(snapshotId),
        [`${getTagRefPath("deleted")}.deleted`]: "",
      });

      const repo = await Repository.open({ storage, formatVersion: "v1" });

      await expect(repo.checkoutTag("deleted")).rejects.toThrow(
        "Tag not found",
      );
    });
  });

  describe("checkoutSnapshot", () => {
    it("should accept Base32 string snapshot ID", async () => {
      const snapshotId = createMockSnapshotId(42);
      const storage = new MockStorage({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });
      const base32Id = encodeObjectId12(snapshotId);

      // This will fail because the snapshot file doesn't exist,
      // but it validates that the Base32 decoding works
      await expect(repo.checkoutSnapshot(base32Id)).rejects.toThrow();
    });

    it("should accept Uint8Array snapshot ID", async () => {
      const snapshotId = createMockSnapshotId(42);
      const storage = new MockStorage({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });

      // This will fail because the snapshot file doesn't exist,
      // but it validates that the Uint8Array is accepted
      await expect(repo.checkoutSnapshot(snapshotId)).rejects.toThrow();
    });
  });

  describe("v2 format integration", () => {
    /**
     * Helper to create a MockStorage from a real v2 repository directory.
     * Loads all files recursively into the mock storage.
     */
    function loadV2RepoIntoMockStorage(repoPath: string): MockStorage {
      const files: Record<string, Uint8Array> = {};

      function loadDir(dirPath: string, prefix: string = ""): void {
        const entries = readdirSync(dirPath, { withFileTypes: true });
        for (const entry of entries) {
          const fullPath = join(dirPath, entry.name);
          const relativePath = prefix ? `${prefix}/${entry.name}` : entry.name;
          if (entry.isDirectory()) {
            loadDir(fullPath, relativePath);
          } else {
            files[relativePath] = readFileSync(fullPath);
          }
        }
      }

      loadDir(repoPath);
      const storage = new MockStorage({});
      for (const [path, data] of Object.entries(files)) {
        storage.addFile(path, data);
      }
      return storage;
    }

    it("should open a real v2 repository", async () => {
      const storage = loadV2RepoIntoMockStorage(TEST_REPO_V2_PATH);
      const repo = await Repository.open({ storage });
      expect(repo).toBeInstanceOf(Repository);
    });

    it("should list branches from real v2 repository", async () => {
      const storage = loadV2RepoIntoMockStorage(TEST_REPO_V2_PATH);
      const repo = await Repository.open({ storage });
      const branches = await repo.listBranches();

      expect(branches).toContain("main");
      expect(Array.isArray(branches)).toBe(true);
    });

    it("should list tags from real v2 repository", async () => {
      const storage = loadV2RepoIntoMockStorage(TEST_REPO_V2_PATH);
      const repo = await Repository.open({ storage });
      const tags = await repo.listTags();

      expect(Array.isArray(tags)).toBe(true);
    });

    it("should checkout main branch from real v2 repository", async () => {
      const storage = loadV2RepoIntoMockStorage(TEST_REPO_V2_PATH);
      const repo = await Repository.open({ storage });

      // This should not throw - the branch exists and resolves to a valid snapshot
      const session = await repo.checkoutBranch("main");
      expect(session).toBeDefined();
    });

    it("should throw on non-existent branch in v2 repository", async () => {
      const storage = loadV2RepoIntoMockStorage(TEST_REPO_V2_PATH);
      const repo = await Repository.open({ storage });

      await expect(repo.checkoutBranch("nonexistent")).rejects.toThrow(
        "Branch not found",
      );
    });

    it("should throw on non-existent tag in v2 repository", async () => {
      const storage = loadV2RepoIntoMockStorage(TEST_REPO_V2_PATH);
      const repo = await Repository.open({ storage });

      await expect(repo.checkoutTag("nonexistent")).rejects.toThrow(
        "Tag not found",
      );
    });

    it("should checkout existing tag from real v2 repository", async () => {
      const storage = loadV2RepoIntoMockStorage(TEST_REPO_V2_PATH);
      const repo = await Repository.open({ storage });

      // This should not throw - the tag exists and resolves to a valid snapshot
      const session = await repo.checkoutTag("it works!");
      expect(session).toBeDefined();
    });
  });

  describe("formatVersion hint", () => {
    it("should skip all validation when formatVersion is v1", async () => {
      const storage = new MockStorageNoList({});

      // Should succeed even with empty storage - validation deferred to checkout
      const repo = await Repository.open({ storage, formatVersion: "v1" });
      expect(repo).toBeInstanceOf(Repository);

      // Should not have made any requests
      expect(storage.requestLog).toHaveLength(0);
    });

    it("should still request /repo when formatVersion is not specified", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorageNoList({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
      });

      await Repository.open({ storage });

      // Should have requested 'repo' (and got 404)
      expect(storage.requestLog).toContain(`getObject:${REPO_INFO_PATH}`);
    });

    it("should throw when formatVersion is v2 but repo file missing", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [getBranchRefPath("main")]: createMockRefJson(snapshotId),
      });

      await expect(
        Repository.open({ storage, formatVersion: "v2" }),
      ).rejects.toThrow("v2 format was specified");
    });

    it('should throw parse error (not "v2 specified") when repo file exists but is invalid', async () => {
      // Create a v2 repo with invalid repo file
      const storage = new MockStorage({
        [REPO_INFO_PATH]: new Uint8Array([0]), // Invalid content
      });

      // Should throw parse error, not "v2 format was specified" error
      await expect(
        Repository.open({ storage, formatVersion: "v2" }),
      ).rejects.toThrow("Failed to parse v2 repo file");
    });
  });
});
