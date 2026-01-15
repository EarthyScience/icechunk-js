/**
 * Repository - Entry point for reading icechunk repositories.
 */

import type { Storage } from '../storage/storage.js';
import { NotFoundError } from '../storage/storage.js';
import {
  getBranchRefDirPath,
  getBranchRefPath,
  getTagRefDirPath,
  getTagRefPath,
  PATHS,
  REPO_INFO_PATH,
} from '../format/constants.js';
import { decodeObjectId12 } from '../format/object-id.js';
import { ReadSession } from './session.js';

/** Reference data stored in ref.json files */
export interface RefData {
  /** Base32-encoded snapshot ID */
  snapshot: string;
}

/** Options for opening a repository */
export interface RepositoryOptions {
  /** Storage backend to use */
  storage: Storage;
}

/**
 * Repository provides access to an icechunk repository.
 *
 * Use this class to:
 * - List branches and tags
 * - Checkout a specific version to get a ReadSession
 */
export class Repository {
  private storage: Storage;

  private constructor(storage: Storage) {
    this.storage = storage;
  }

  /**
   * Open an icechunk repository.
   *
   * @param options - Repository options including storage backend
   * @returns A Repository instance
   */
  static async open(options: RepositoryOptions): Promise<Repository> {
    const repo = new Repository(options.storage);

    // Validation logic matches the Rust source of truth:
    // 1. Check for repo info file (v2+ format)
    // 2. Check for main branch (v1 format) - any .json file in the branch directory
    const repoInfoExists = await options.storage.exists(REPO_INFO_PATH);
    if (repoInfoExists) {
      return repo;
    }

    // Check for main branch by looking for any .json file in the branch directory
    const mainBranchDir = getBranchRefDirPath('main');
    const mainExists = await repo.hasAnyRefFile(mainBranchDir, getBranchRefPath('main'));
    if (mainExists) {
      return repo;
    }

    throw new Error(
      'Not a valid icechunk repository: neither repo info file nor main branch found'
    );
  }

  /**
   * Check if any ref file exists in a directory.
   * Ref files are .json files that are not .deleted files.
   *
   * @param dirPrefix - Directory prefix to check
   * @param legacyPath - Optional legacy ref.json path to check if listing fails
   */
  private async hasAnyRefFile(dirPrefix: string, legacyPath?: string): Promise<boolean> {
    try {
      for await (const path of this.storage.listPrefix(dirPrefix)) {
        if (path.endsWith('.json') && !path.endsWith('.deleted')) {
          return true;
        }
      }
    } catch {
      // Listing not supported - try fallback paths
      // First try the versioned filename
      const versionedPath = `${dirPrefix}ZZZZZZZZ.json`;
      if (await this.storage.exists(versionedPath)) {
        return true;
      }
      // Then try legacy path if provided
      if (legacyPath) {
        return await this.storage.exists(legacyPath);
      }
    }
    return false;
  }

  /**
   * Find the latest non-deleted ref file in a directory.
   * Icechunk uses lexicographically sortable versioned filenames (e.g., ZZZZZZZZ.json)
   * for optimistic concurrency control. The latest version has the highest filename.
   *
   * Deletion is indicated by a tombstone file (e.g., ABC.json.deleted alongside ABC.json).
   * A ref is considered deleted if its latest version has a corresponding tombstone.
   *
   * @param dirPrefix - Directory prefix to search
   * @param legacyPath - Optional legacy ref.json path to try if listing fails
   */
  private async findLatestRefFile(dirPrefix: string, legacyPath?: string): Promise<string | null> {
    const jsonFiles: string[] = [];
    const deletedFiles = new Set<string>();

    try {
      for await (const path of this.storage.listPrefix(dirPrefix)) {
        if (path.endsWith('.deleted')) {
          // Track deletion tombstones (e.g., "ABC.json.deleted" -> "ABC.json")
          deletedFiles.add(path.slice(0, -'.deleted'.length));
        } else if (path.endsWith('.json')) {
          jsonFiles.push(path);
        }
      }
    } catch {
      // Listing not supported - try fallback paths
      // First try the versioned filename used by local filesystem storage
      const versionedPath = `${dirPrefix}ZZZZZZZZ.json`;
      if (await this.storage.exists(versionedPath)) {
        // Check for deletion tombstone
        if (await this.storage.exists(`${versionedPath}.deleted`)) {
          return null;
        }
        return versionedPath;
      }
      // Then try the legacy ref.json path
      if (legacyPath && await this.storage.exists(legacyPath)) {
        // Check for deletion tombstone
        if (await this.storage.exists(`${legacyPath}.deleted`)) {
          return null;
        }
        return legacyPath;
      }
      return null;
    }

    if (jsonFiles.length === 0) {
      return null;
    }

    // Sort lexicographically and find the latest non-deleted version
    jsonFiles.sort();

    // Start from the latest and find the first non-deleted file
    for (let i = jsonFiles.length - 1; i >= 0; i--) {
      if (!deletedFiles.has(jsonFiles[i])) {
        return jsonFiles[i];
      }
    }

    // All versions are deleted
    return null;
  }

  /**
   * List all branches in the repository.
   *
   * Branches with deletion tombstones on their latest ref file are excluded.
   *
   * @returns Array of branch names
   */
  async listBranches(): Promise<string[]> {
    // Track files per branch: branch name -> { jsonFiles, deletedFiles }
    const branchFiles = new Map<string, { jsonFiles: string[]; deletedFiles: Set<string> }>();
    let listingSupported = true;

    try {
      for await (const path of this.storage.listPrefix(`${PATHS.REFS}/branch.`)) {
        // Extract branch name from paths like:
        // - "refs/branch.main/ZZZZZZZZ.json" (versioned)
        // - "refs/branch.main/ref.json" (legacy)
        // - "refs/branch.main/ZZZZZZZZ.json.deleted" (deletion tombstone)
        const jsonMatch = path.match(/^refs\/branch\.(.+)\/([^/]+\.json)$/);
        const deletedMatch = path.match(/^refs\/branch\.(.+)\/([^/]+\.json)\.deleted$/);

        if (deletedMatch) {
          const branchName = deletedMatch[1];
          const refFile = `refs/branch.${branchName}/${deletedMatch[2]}`;
          if (!branchFiles.has(branchName)) {
            branchFiles.set(branchName, { jsonFiles: [], deletedFiles: new Set() });
          }
          branchFiles.get(branchName)!.deletedFiles.add(refFile);
        } else if (jsonMatch) {
          const branchName = jsonMatch[1];
          if (!branchFiles.has(branchName)) {
            branchFiles.set(branchName, { jsonFiles: [], deletedFiles: new Set() });
          }
          branchFiles.get(branchName)!.jsonFiles.push(path);
        }
      }
    } catch {
      // Listing not supported
      listingSupported = false;
    }

    // Fallback: try common branch names by checking if files exist
    if (!listingSupported) {
      const result: string[] = [];
      for (const name of ['main', 'master']) {
        const refPath = await this.findLatestRefFile(getBranchRefDirPath(name), getBranchRefPath(name));
        if (refPath) {
          result.push(name);
        }
      }
      return result;
    }

    // Filter to only include branches where latest ref is not deleted
    const result: string[] = [];
    for (const [branchName, { jsonFiles, deletedFiles }] of branchFiles) {
      if (jsonFiles.length === 0) continue;

      // Sort to find latest
      jsonFiles.sort();
      const latestRef = jsonFiles[jsonFiles.length - 1];

      // Include only if latest ref is not deleted
      if (!deletedFiles.has(latestRef)) {
        result.push(branchName);
      }
    }

    return result;
  }

  /**
   * List all tags in the repository.
   *
   * Tags with deletion tombstones on their latest ref file are excluded.
   *
   * @returns Array of tag names
   */
  async listTags(): Promise<string[]> {
    // Track files per tag: tag name -> { jsonFiles, deletedFiles }
    const tagFiles = new Map<string, { jsonFiles: string[]; deletedFiles: Set<string> }>();

    try {
      for await (const path of this.storage.listPrefix(`${PATHS.REFS}/tag.`)) {
        // Extract tag name from paths like:
        // - "refs/tag.v1.0/ZZZZZZZZ.json" (versioned)
        // - "refs/tag.v1.0/ref.json" (legacy)
        // - "refs/tag.v1.0/ZZZZZZZZ.json.deleted" (deletion tombstone)
        const jsonMatch = path.match(/^refs\/tag\.(.+)\/([^/]+\.json)$/);
        const deletedMatch = path.match(/^refs\/tag\.(.+)\/([^/]+\.json)\.deleted$/);

        if (deletedMatch) {
          const tagName = deletedMatch[1];
          const refFile = `refs/tag.${tagName}/${deletedMatch[2]}`;
          if (!tagFiles.has(tagName)) {
            tagFiles.set(tagName, { jsonFiles: [], deletedFiles: new Set() });
          }
          tagFiles.get(tagName)!.deletedFiles.add(refFile);
        } else if (jsonMatch) {
          const tagName = jsonMatch[1];
          if (!tagFiles.has(tagName)) {
            tagFiles.set(tagName, { jsonFiles: [], deletedFiles: new Set() });
          }
          tagFiles.get(tagName)!.jsonFiles.push(path);
        }
      }
    } catch {
      // If listing not supported, can't enumerate tags
      return [];
    }

    // Filter to only include tags where latest ref is not deleted
    const result: string[] = [];
    for (const [tagName, { jsonFiles, deletedFiles }] of tagFiles) {
      if (jsonFiles.length === 0) continue;

      // Sort to find latest
      jsonFiles.sort();
      const latestRef = jsonFiles[jsonFiles.length - 1];

      // Include only if latest ref is not deleted
      if (!deletedFiles.has(latestRef)) {
        result.push(tagName);
      }
    }

    return result;
  }

  /**
   * Checkout a branch to get a read session.
   *
   * @param name - Branch name
   * @returns Read session at the branch's current snapshot
   */
  async checkoutBranch(name: string): Promise<ReadSession> {
    const refDirPath = getBranchRefDirPath(name);
    const refPath = await this.findLatestRefFile(refDirPath, getBranchRefPath(name));
    if (!refPath) {
      throw new Error(`Reference not found: ${refDirPath}`);
    }
    const snapshotId = await this.readSnapshotIdFromRef(refPath);
    return ReadSession.open(this.storage, snapshotId);
  }

  /**
   * Checkout a tag to get a read session.
   *
   * @param name - Tag name
   * @returns Read session at the tag's snapshot
   */
  async checkoutTag(name: string): Promise<ReadSession> {
    const refDirPath = getTagRefDirPath(name);
    const refPath = await this.findLatestRefFile(refDirPath, getTagRefPath(name));
    if (!refPath) {
      throw new Error(`Reference not found: ${refDirPath}`);
    }
    const snapshotId = await this.readSnapshotIdFromRef(refPath);
    return ReadSession.open(this.storage, snapshotId);
  }

  /**
   * Checkout a specific snapshot by ID.
   *
   * @param snapshotId - Snapshot ID (12 bytes or Base32 string)
   * @returns Read session at the specified snapshot
   */
  async checkoutSnapshot(snapshotId: Uint8Array | string): Promise<ReadSession> {
    const id = typeof snapshotId === 'string'
      ? decodeObjectId12(snapshotId)
      : snapshotId;
    return ReadSession.open(this.storage, id);
  }

  /**
   * Get the storage backend.
   */
  getStorage(): Storage {
    return this.storage;
  }

  /** Read and parse a ref file */
  private async readRef(path: string): Promise<RefData> {
    const data = await this.storage.getObject(path);
    const json = new TextDecoder().decode(data);
    return JSON.parse(json) as RefData;
  }

  /** Read snapshot ID from a ref file */
  private async readSnapshotIdFromRef(path: string): Promise<Uint8Array> {
    try {
      const ref = await this.readRef(path);
      return decodeObjectId12(ref.snapshot);
    } catch (error) {
      if (error instanceof NotFoundError) {
        throw new Error(`Reference not found: ${path}`);
      }
      throw error;
    }
  }
}
