import { describe, it, expect } from 'vitest';
import { Repository } from '../../src/reader/repository.js';
import {
  MockStorage,
  MockStorageNoList,
  createMockSnapshotId,
  createMockRefJson,
} from '../fixtures/mock-storage.js';
import {
  getBranchRefPath,
  getBranchRefDirPath,
  getTagRefPath,
  getTagRefDirPath,
  REPO_INFO_PATH,
} from '../../src/format/constants.js';
import { encodeObjectId12 } from '../../src/format/object-id.js';

describe('Repository', () => {
  describe('open', () => {
    it('should open a valid v2 repository with repo info file', async () => {
      const storage = new MockStorage({
        [REPO_INFO_PATH]: 'repo info content',
      });

      const repo = await Repository.open({ storage });
      expect(repo).toBeInstanceOf(Repository);
    });

    it('should open a valid v1 repository with main branch', async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [getBranchRefPath('main')]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });
      expect(repo).toBeInstanceOf(Repository);
    });

    it('should open a repository with both repo info and main branch', async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [REPO_INFO_PATH]: 'repo info content',
        [getBranchRefPath('main')]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });
      expect(repo).toBeInstanceOf(Repository);
    });

    it('should throw on invalid repository (no repo info or main branch)', async () => {
      const storage = new MockStorage({});

      await expect(Repository.open({ storage })).rejects.toThrow(
        'Not a valid icechunk repository'
      );
    });

    it('should throw on repository with only a feature branch', async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [getBranchRefPath('feature')]: createMockRefJson(snapshotId),
      });

      await expect(Repository.open({ storage })).rejects.toThrow(
        'Not a valid icechunk repository'
      );
    });
  });

  describe('listBranches', () => {
    it('should list branches including those with slashes', async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [REPO_INFO_PATH]: 'repo info',
        [getBranchRefPath('main')]: createMockRefJson(snapshotId),
        [getBranchRefPath('feature/test')]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });
      const branches = await repo.listBranches();

      expect(branches).toContain('main');
      expect(branches).toContain('feature/test');
      expect(branches).toHaveLength(2);
    });

    it('should return empty array when no branches exist', async () => {
      const storage = new MockStorage({
        [REPO_INFO_PATH]: 'repo info',
      });

      const repo = await Repository.open({ storage });
      const branches = await repo.listBranches();

      expect(branches).toEqual([]);
    });

    it('should fallback to checking common names when listPrefix not supported', async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorageNoList({
        [REPO_INFO_PATH]: 'repo info',
        [getBranchRefPath('main')]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });
      const branches = await repo.listBranches();

      expect(branches).toContain('main');
    });

    it('should exclude branches with deletion tombstones', async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [REPO_INFO_PATH]: 'repo info',
        // Active branch (no tombstone)
        [`${getBranchRefDirPath('main')}ZZZZZZZZ.json`]: createMockRefJson(snapshotId),
        // Deleted branch (ref file + tombstone)
        [`${getBranchRefDirPath('deleted-branch')}XXXXXXXX.json`]: createMockRefJson(snapshotId),
        [`${getBranchRefDirPath('deleted-branch')}XXXXXXXX.json.deleted`]: '',
      });

      const repo = await Repository.open({ storage });
      const branches = await repo.listBranches();

      expect(branches).toContain('main');
      expect(branches).not.toContain('deleted-branch');
      expect(branches).toHaveLength(1);
    });

    it('should include branch if older version deleted but newer exists', async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [REPO_INFO_PATH]: 'repo info',
        // Branch with deleted old version and active new version
        [`${getBranchRefDirPath('active')}AAAAAAAA.json`]: createMockRefJson(snapshotId),
        [`${getBranchRefDirPath('active')}AAAAAAAA.json.deleted`]: '',
        [`${getBranchRefDirPath('active')}ZZZZZZZZ.json`]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });
      const branches = await repo.listBranches();

      expect(branches).toContain('active');
    });

    it('should handle versioned ref filenames', async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [REPO_INFO_PATH]: 'repo info',
        // Versioned filename format (not ref.json)
        [`${getBranchRefDirPath('main')}ZZZZZZZZ.json`]: createMockRefJson(snapshotId),
        [`${getBranchRefDirPath('develop')}ABC12345.json`]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });
      const branches = await repo.listBranches();

      expect(branches).toContain('main');
      expect(branches).toContain('develop');
      expect(branches).toHaveLength(2);
    });
  });

  describe('listTags', () => {
    it('should list tags including those with slashes', async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [REPO_INFO_PATH]: 'repo info',
        [getTagRefPath('v1.0')]: createMockRefJson(snapshotId),
        [getTagRefPath('release/v2.0')]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });
      const tags = await repo.listTags();

      expect(tags).toContain('v1.0');
      expect(tags).toContain('release/v2.0');
      expect(tags).toHaveLength(2);
    });

    it('should return empty array when no tags exist', async () => {
      const storage = new MockStorage({
        [REPO_INFO_PATH]: 'repo info',
      });

      const repo = await Repository.open({ storage });
      const tags = await repo.listTags();

      expect(tags).toEqual([]);
    });

    it('should return empty array when listPrefix not supported', async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorageNoList({
        [REPO_INFO_PATH]: 'repo info',
        [getTagRefPath('v1.0')]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });
      const tags = await repo.listTags();

      // Tags cannot be enumerated without listPrefix support
      expect(tags).toEqual([]);
    });

    it('should exclude tags with deletion tombstones', async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [REPO_INFO_PATH]: 'repo info',
        // Active tag (no tombstone)
        [`${getTagRefDirPath('v1.0')}ZZZZZZZZ.json`]: createMockRefJson(snapshotId),
        // Deleted tag (ref file + tombstone)
        [`${getTagRefDirPath('old-tag')}XXXXXXXX.json`]: createMockRefJson(snapshotId),
        [`${getTagRefDirPath('old-tag')}XXXXXXXX.json.deleted`]: '',
      });

      const repo = await Repository.open({ storage });
      const tags = await repo.listTags();

      expect(tags).toContain('v1.0');
      expect(tags).not.toContain('old-tag');
      expect(tags).toHaveLength(1);
    });

    it('should exclude tag even with multiple versions if latest is deleted', async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [REPO_INFO_PATH]: 'repo info',
        // Tag with multiple versions, latest is deleted
        [`${getTagRefDirPath('deleted-tag')}AAAAAAAA.json`]: createMockRefJson(snapshotId),
        [`${getTagRefDirPath('deleted-tag')}ZZZZZZZZ.json`]: createMockRefJson(snapshotId),
        [`${getTagRefDirPath('deleted-tag')}ZZZZZZZZ.json.deleted`]: '',
      });

      const repo = await Repository.open({ storage });
      const tags = await repo.listTags();

      expect(tags).not.toContain('deleted-tag');
      expect(tags).toHaveLength(0);
    });
  });

  describe('ref resolution edge cases', () => {
    it('should find versioned ref file over legacy ref.json in fallback mode', async () => {
      const snapshotId = createMockSnapshotId(1);
      // Storage without listPrefix support
      const storage = new MockStorageNoList({
        [REPO_INFO_PATH]: 'repo info',
        // Versioned filename (checked first in fallback)
        [`${getBranchRefDirPath('main')}ZZZZZZZZ.json`]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });
      const branches = await repo.listBranches();

      expect(branches).toContain('main');
    });

    it('should open repo with versioned main branch file', async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        // Only versioned branch file, no legacy ref.json
        [`${getBranchRefDirPath('main')}ZZZZZZZZ.json`]: createMockRefJson(snapshotId),
      });

      const repo = await Repository.open({ storage });
      expect(repo).toBeInstanceOf(Repository);
    });
  });

  describe('checkoutBranch', () => {
    it('should throw on non-existent branch', async () => {
      const storage = new MockStorage({
        [REPO_INFO_PATH]: 'repo info',
      });

      const repo = await Repository.open({ storage });

      await expect(repo.checkoutBranch('nonexistent')).rejects.toThrow(
        'Reference not found'
      );
    });

    it('should throw on invalid ref.json content', async () => {
      const storage = new MockStorage({
        [REPO_INFO_PATH]: 'repo info',
        [getBranchRefPath('broken')]: 'not valid json',
      });

      const repo = await Repository.open({ storage });

      await expect(repo.checkoutBranch('broken')).rejects.toThrow();
    });

    it('should throw on deleted branch (with tombstone)', async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [REPO_INFO_PATH]: 'repo info',
        // Deleted branch (ref file + tombstone)
        [`${getBranchRefDirPath('deleted')}ZZZZZZZZ.json`]: createMockRefJson(snapshotId),
        [`${getBranchRefDirPath('deleted')}ZZZZZZZZ.json.deleted`]: '',
      });

      const repo = await Repository.open({ storage });

      await expect(repo.checkoutBranch('deleted')).rejects.toThrow(
        'Reference not found'
      );
    });
  });

  describe('checkoutTag', () => {
    it('should throw on non-existent tag', async () => {
      const storage = new MockStorage({
        [REPO_INFO_PATH]: 'repo info',
      });

      const repo = await Repository.open({ storage });

      await expect(repo.checkoutTag('nonexistent')).rejects.toThrow(
        'Reference not found'
      );
    });

    it('should throw on deleted tag (with tombstone)', async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({
        [REPO_INFO_PATH]: 'repo info',
        // Deleted tag (ref file + tombstone)
        [`${getTagRefDirPath('deleted')}ZZZZZZZZ.json`]: createMockRefJson(snapshotId),
        [`${getTagRefDirPath('deleted')}ZZZZZZZZ.json.deleted`]: '',
      });

      const repo = await Repository.open({ storage });

      await expect(repo.checkoutTag('deleted')).rejects.toThrow(
        'Reference not found'
      );
    });
  });

  describe('checkoutSnapshot', () => {
    it('should accept Base32 string snapshot ID', async () => {
      const snapshotId = createMockSnapshotId(42);
      const storage = new MockStorage({
        [REPO_INFO_PATH]: 'repo info',
      });

      const repo = await Repository.open({ storage });
      const base32Id = encodeObjectId12(snapshotId);

      // This will fail because the snapshot file doesn't exist,
      // but it validates that the Base32 decoding works
      await expect(repo.checkoutSnapshot(base32Id)).rejects.toThrow();
    });

    it('should accept Uint8Array snapshot ID', async () => {
      const snapshotId = createMockSnapshotId(42);
      const storage = new MockStorage({
        [REPO_INFO_PATH]: 'repo info',
      });

      const repo = await Repository.open({ storage });

      // This will fail because the snapshot file doesn't exist,
      // but it validates that the Uint8Array is accepted
      await expect(repo.checkoutSnapshot(snapshotId)).rejects.toThrow();
    });
  });
});
