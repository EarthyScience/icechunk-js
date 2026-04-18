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
import {
  decodeObjectId12,
  encodeObjectId12,
} from "../../src/format/object-id.js";

// ESM equivalent of __dirname
const __dirname = dirname(fileURLToPath(import.meta.url));

// Path to v2 test repository
const TEST_REPO_V1_PATH = join(__dirname, "../data/test-repo-v1");
const TEST_REPO_V2_PATH = join(__dirname, "../data/test-repo-v2");

/**
 * Helper to create a MockStorage from a real repository directory.
 * Loads all files recursively into the mock storage.
 */
function loadRepoIntoMockStorage(repoPath: string): MockStorage {
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
    it("should checkout branch via legacy ref.json when listPrefix not supported (v1 format)", async () => {
      const snapshotId = "NXH3M0HJ7EEJ0699DPP0";
      const storage = new MockStorageNoList({
        [getBranchRefPath("main")]: readFileSync(
          join(TEST_REPO_V1_PATH, getBranchRefPath("main")),
        ),
        [`snapshots/${snapshotId}`]: readFileSync(
          join(TEST_REPO_V1_PATH, `snapshots/${snapshotId}`),
        ),
      });

      const repo = await Repository.open({ storage });
      const session = await repo.checkoutBranch("main");

      expect(encodeObjectId12(session.getSnapshotId())).toBe(snapshotId);
    });

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
  });

  describe("checkoutTag", () => {
    it("should checkout tag via legacy ref.json when listPrefix not supported (v1 format)", async () => {
      const snapshotId = "4QF8JA0YPDN51MHSSYVG";
      const storage = new MockStorageNoList({
        [getBranchRefPath("main")]: readFileSync(
          join(TEST_REPO_V1_PATH, getBranchRefPath("main")),
        ),
        [getTagRefPath("it works!")]: readFileSync(
          join(TEST_REPO_V1_PATH, getTagRefPath("it works!")),
        ),
        [`snapshots/${snapshotId}`]: readFileSync(
          join(TEST_REPO_V1_PATH, `snapshots/${snapshotId}`),
        ),
      });

      const repo = await Repository.open({ storage });
      const session = await repo.checkoutTag("it works!");

      expect(encodeObjectId12(session.getSnapshotId())).toBe(snapshotId);
    });

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
    it("should checkout existing snapshot from real repository with Base32 string", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V1_PATH);
      const repo = await Repository.open({ storage });
      const session = await repo.checkoutSnapshot("GC4YVH5SKBPEZCENYQE0");

      expect(encodeObjectId12(session.getSnapshotId())).toBe(
        "GC4YVH5SKBPEZCENYQE0",
      );
      expect(session.getMessage()).toBe("empty structure");
    });

    it("should checkout existing snapshot from real repository with Uint8Array", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V1_PATH);
      const repo = await Repository.open({ storage });
      const snapshotId = "P874YS3J196959RDHX7G";
      const session = await repo.checkoutSnapshot(decodeObjectId12(snapshotId));

      expect(encodeObjectId12(session.getSnapshotId())).toBe(snapshotId);
      expect(session.getMessage()).toBe("Repository initialized");
    });

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
    it("should open a real v2 repository", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V2_PATH);
      const repo = await Repository.open({ storage });
      expect(repo).toBeInstanceOf(Repository);
    });

    it("should list branches from real v2 repository", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V2_PATH);
      const repo = await Repository.open({ storage });
      const branches = await repo.listBranches();

      expect(branches).toContain("main");
      expect(Array.isArray(branches)).toBe(true);
    });

    it("should list tags from real v2 repository", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V2_PATH);
      const repo = await Repository.open({ storage });
      const tags = await repo.listTags();

      expect(Array.isArray(tags)).toBe(true);
    });

    it("should checkout main branch from real v2 repository", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V2_PATH);
      const repo = await Repository.open({ storage });

      // This should not throw - the branch exists and resolves to a valid snapshot
      const session = await repo.checkoutBranch("main");
      expect(session).toBeDefined();
    });

    it("should throw on non-existent branch in v2 repository", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V2_PATH);
      const repo = await Repository.open({ storage });

      await expect(repo.checkoutBranch("nonexistent")).rejects.toThrow(
        "Branch not found",
      );
    });

    it("should throw on non-existent tag in v2 repository", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V2_PATH);
      const repo = await Repository.open({ storage });

      await expect(repo.checkoutTag("nonexistent")).rejects.toThrow(
        "Tag not found",
      );
    });

    it("should checkout existing tag from real v2 repository", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V2_PATH);
      const repo = await Repository.open({ storage });

      // This should not throw - the tag exists and resolves to a valid snapshot
      const session = await repo.checkoutTag("it works!");
      expect(session).toBeDefined();
    });
  });

  describe("walkHistory", () => {
    // Expected v1 fixture histories (head → root):
    const MAIN_HISTORY = [
      { id: "NXH3M0HJ7EEJ0699DPP0", message: "set virtual chunk" },
      { id: "7XAF0Q905SH4938DN9CG", message: "fill data" },
      { id: "GC4YVH5SKBPEZCENYQE0", message: "empty structure" },
      { id: "P874YS3J196959RDHX7G", message: "Repository initialized" },
    ];
    const MY_BRANCH_HISTORY = [
      { id: "XDZ162T1TYBEJMK99NPG", message: "some more structure" },
      { id: "4QF8JA0YPDN51MHSSYVG", message: "delete a chunk" },
      ...MAIN_HISTORY,
    ];

    it("should walk linear history from branch head to root (v1)", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V1_PATH);
      const repo = await Repository.open({ storage });
      const session = await repo.checkoutBranch("main");

      const entries = [];
      for await (const entry of repo.walkHistory(session)) {
        entries.push(entry);
      }

      expect(entries.map((e) => ({ id: e.id, message: e.message }))).toEqual(
        MAIN_HISTORY,
      );
    });

    it("should walk history including diverged commits on feature branch (v1)", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V1_PATH);
      const repo = await Repository.open({ storage });
      const session = await repo.checkoutBranch("my-branch");

      const entries = [];
      for await (const entry of repo.walkHistory(session)) {
        entries.push(entry);
      }

      expect(entries.map((e) => ({ id: e.id, message: e.message }))).toEqual(
        MY_BRANCH_HISTORY,
      );
    });

    it("should yield entries with id, message, flushedAt, and metadata", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V1_PATH);
      const repo = await Repository.open({ storage });
      const session = await repo.checkoutBranch("main");

      const iterator = repo.walkHistory(session);
      const first = (await iterator.next()).value!;

      expect(typeof first.id).toBe("string");
      expect(first.id).toBe("NXH3M0HJ7EEJ0699DPP0");
      expect(typeof first.message).toBe("string");
      expect(first.flushedAt).toBeInstanceOf(Date);
      expect(first.metadata).toBeTypeOf("object");
    });

    it("should yield flushedAt in strictly non-increasing order", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V1_PATH);
      const repo = await Repository.open({ storage });
      const session = await repo.checkoutBranch("my-branch");

      let prev = Infinity;
      for await (const entry of repo.walkHistory(session)) {
        const t = entry.flushedAt.getTime();
        expect(t).toBeLessThanOrEqual(prev);
        prev = t;
      }
    });

    it("should yield single entry and stop at a root snapshot", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V1_PATH);
      const repo = await Repository.open({ storage });
      // P874YS3J196959RDHX7G is the initial commit in the v1 fixture (parent=null)
      const rootSession = await repo.checkoutSnapshot("P874YS3J196959RDHX7G");

      const entries = [];
      for await (const entry of repo.walkHistory(rootSession)) {
        entries.push(entry);
      }

      expect(entries).toHaveLength(1);
      expect(entries[0].id).toBe("P874YS3J196959RDHX7G");
      expect(entries[0].message).toBe("Repository initialized");
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
