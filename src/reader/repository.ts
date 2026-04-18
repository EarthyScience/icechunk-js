/**
 * Repository - Entry point for reading icechunk repositories.
 */

import type { Storage, RequestOptions } from "../storage/storage.js";
import { NotFoundError, AbortError } from "../storage/storage.js";
import {
  getBranchRefDirPath,
  getBranchRefPath,
  getTagRefDirPath,
  getTagRefPath,
  PATHS,
  REPO_INFO_PATH,
} from "../format/constants.js";
import { decodeObjectId12, encodeObjectId12 } from "../format/object-id.js";
import {
  parseRepo,
  resolveBranch,
  resolveTag,
  listBranchesFromRepo,
  listTagsFromRepo,
  type ParsedRepo,
} from "../format/flatbuffers/repo-parser.js";
import { ReadSession } from "./session.js";

/** Reference data stored in ref.json files */
export interface RefData {
  /** Base32-encoded snapshot ID */
  snapshot: string;
}

/** Options for opening a repository */
export interface RepositoryOptions {
  /** Storage backend to use */
  storage: Storage;
  /** Format version hint to skip auto-detection. 'v1' skips /repo request. */
  formatVersion?: "v1" | "v2";
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
  private repoInfo: ParsedRepo | null = null;
  private repoInfoAttempted = false;

  private constructor(storage: Storage) {
    this.storage = storage;
  }

  /**
   * Load and cache the v2 repo info file.
   *
   * Uses getObject() directly to avoid race conditions with exists().
   * - NotFoundError => v1 format (no repo file)
   * - Any other error => hard error (parse failure, etc.)
   *
   * @param options - Optional request options (signal for cancellation)
   * @returns ParsedRepo if v2 format, null if v1 format
   * @throws Error if repo file exists but fails to parse
   * @throws AbortError if the operation was aborted
   */
  private async loadRepoInfo(
    options?: RequestOptions,
  ): Promise<ParsedRepo | null> {
    if (this.repoInfoAttempted) {
      return this.repoInfo;
    }
    this.repoInfoAttempted = true;

    try {
      const data = await this.storage.getObject(
        REPO_INFO_PATH,
        undefined,
        options,
      );
      this.repoInfo = parseRepo(data);
      return this.repoInfo;
    } catch (error) {
      // Propagate abort errors
      if (error instanceof AbortError) {
        this.repoInfoAttempted = false; // Allow retry
        throw error;
      }
      // NotFoundError means v1 format (no repo file)
      if (error instanceof NotFoundError) {
        return null;
      }
      // Any other error is fatal
      throw new Error(
        `Failed to parse v2 repo file: ${error instanceof Error ? error.message : error}`,
      );
    }
  }

  /**
   * Open an icechunk repository.
   *
   * - Try to load and parse repo info file (v2+ format)
   * - If NotFoundError, check for main branch (v1 format)
   * - Parse failures are hard errors (matching Rust behavior)
   *
   * @param options - Repository options including storage backend
   * @param requestOptions - Optional request options (signal for cancellation)
   * @returns A Repository instance
   */
  static async open(
    options: RepositoryOptions,
    requestOptions?: RequestOptions,
  ): Promise<Repository> {
    const repo = new Repository(options.storage);

    if (options.formatVersion === "v1") {
      // User asserts v1 format - skip all validation, defer errors to checkout
      repo.repoInfoAttempted = true;
      return repo;
    }

    // Try v2 format first - load and parse to fail fast on corruption
    const repoInfo = await repo.loadRepoInfo(requestOptions);
    if (repoInfo) {
      return repo; // v2 format - repo file exists and parsed successfully
    }
    if (options.formatVersion === "v2") {
      throw new Error("Repository info not found but v2 format was specified");
    }

    // Auto-detect: check for main branch (v1 format)
    const mainBranchDir = getBranchRefDirPath("main");
    const mainExists = await repo.hasAnyRefFile(
      mainBranchDir,
      getBranchRefPath("main"),
      requestOptions,
    );
    if (mainExists) {
      return repo;
    }

    throw new Error(
      "Not a valid icechunk repository: neither repo info file nor main branch found",
    );
  }

  /**
   * Check if any ref file exists in a directory.
   * Ref files are .json files that are not .deleted files.
   *
   * @param dirPrefix - Directory prefix to check
   * @param legacyPath - Optional legacy ref.json path to check if listing fails
   * @param options - Optional request options (signal for cancellation)
   */
  private async hasAnyRefFile(
    dirPrefix: string,
    legacyPath?: string,
    options?: RequestOptions,
  ): Promise<boolean> {
    try {
      for await (const path of this.storage.listPrefix(dirPrefix)) {
        if (path.endsWith(".json") && !path.endsWith(".deleted")) {
          return true;
        }
      }
    } catch (error) {
      // Propagate abort errors
      if (error instanceof AbortError) {
        throw error;
      }
      // Listing not supported - try legacy path only
      if (legacyPath) {
        return await this.storage.exists(legacyPath, options);
      }
    }
    return false;
  }

  /**
   * Find the latest non-deleted ref file in a directory.
   * When storage supports listPrefix(), refs may use versioned filenames
   * (e.g., AAAAAAAA.json) for optimistic concurrency control. The latest
   * version has the highest filename. Deletion tombstones are checked.
   *
   * When listing is not supported (e.g., HTTP storage), only the legacy
   * ref.json path is checked (no tombstone check - assumes read-only).
   *
   * @param dirPrefix - Directory prefix to search
   * @param legacyPath - Optional legacy ref.json path to try if listing fails
   */
  private async findLatestRefFile(
    dirPrefix: string,
    legacyPath?: string,
  ): Promise<string | null> {
    const jsonFiles: string[] = [];
    const deletedFiles = new Set<string>();

    try {
      for await (const path of this.storage.listPrefix(dirPrefix)) {
        if (path.endsWith(".deleted")) {
          // Track deletion tombstones (e.g., "ABC.json.deleted" -> "ABC.json")
          deletedFiles.add(path.slice(0, -".deleted".length));
        } else if (path.endsWith(".json")) {
          jsonFiles.push(path);
        }
      }
    } catch (error) {
      // Propagate abort errors
      if (error instanceof AbortError) {
        throw error;
      }
      // Listing not supported - return legacy path without checking exists.
      // Caller will handle NotFoundError when reading.
      return legacyPath || null;
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
   * Unlike tags, branches in icechunk v1 have no tombstone mechanism —
   * deletion removes the ref file outright. A branch is present iff it has
   * at least one ref file in its directory.
   *
   * @returns Array of branch names
   */
  async listBranches(): Promise<string[]> {
    // Try v2 format first
    const repoInfo = await this.loadRepoInfo();
    if (repoInfo) {
      return listBranchesFromRepo(repoInfo);
    }

    // V1 fallback - file-based lookup
    const branches = new Set<string>();

    try {
      for await (const path of this.storage.listPrefix(
        `${PATHS.REFS}/branch.`,
      )) {
        // Match ref files like "refs/branch.main/ref.json" (ignore tombstones
        // for defensiveness — Rust never writes them for branches).
        const match = path.match(/^refs\/branch\.(.+)\/([^/]+\.json)$/);
        if (match) {
          branches.add(match[1]);
        }
      }
    } catch {
      throw new Error("Cannot list branches: storage does not support listing");
    }

    return [...branches];
  }

  /**
   * List all tags in the repository.
   *
   * Tags with deletion tombstones on their latest ref file are excluded.
   *
   * @returns Array of tag names
   */
  async listTags(): Promise<string[]> {
    // Try v2 format first
    const repoInfo = await this.loadRepoInfo();
    if (repoInfo) {
      return listTagsFromRepo(repoInfo);
    }

    // V1 fallback - file-based lookup
    // Track files per tag: tag name -> { jsonFiles, deletedFiles }
    const tagFiles = new Map<
      string,
      { jsonFiles: string[]; deletedFiles: Set<string> }
    >();

    try {
      for await (const path of this.storage.listPrefix(`${PATHS.REFS}/tag.`)) {
        // Extract tag name from paths like:
        // - "refs/tag.v1.0/ZZZZZZZZ.json" (versioned)
        // - "refs/tag.v1.0/ref.json" (legacy)
        // - "refs/tag.v1.0/ZZZZZZZZ.json.deleted" (deletion tombstone)
        const jsonMatch = path.match(/^refs\/tag\.(.+)\/([^/]+\.json)$/);
        const deletedMatch = path.match(
          /^refs\/tag\.(.+)\/([^/]+\.json)\.deleted$/,
        );

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
      throw new Error("Cannot list tags: storage does not support listing");
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
   * @param options - Optional request options (signal for cancellation)
   * @returns Read session at the branch's current snapshot
   */
  async checkoutBranch(
    name: string,
    options?: RequestOptions,
  ): Promise<ReadSession> {
    // Try v2 format first
    const repoInfo = await this.loadRepoInfo(options);
    if (repoInfo) {
      const snapshotId = resolveBranch(repoInfo, name);
      if (!snapshotId) {
        throw new Error(`Branch not found: ${name}`);
      }
      return ReadSession.open(this.storage, snapshotId, options);
    }

    // V1 fallback - file-based lookup
    const refDirPath = getBranchRefDirPath(name);
    const refPath = await this.findLatestRefFile(
      refDirPath,
      getBranchRefPath(name),
    );
    if (!refPath) {
      throw new Error(`Reference not found: ${refDirPath}`);
    }
    const snapshotId = await this.readSnapshotIdFromRef(refPath, options);
    return ReadSession.open(this.storage, snapshotId, options);
  }

  /**
   * Checkout a tag to get a read session.
   *
   * @param name - Tag name
   * @param options - Optional request options (signal for cancellation)
   * @returns Read session at the tag's snapshot
   */
  async checkoutTag(
    name: string,
    options?: RequestOptions,
  ): Promise<ReadSession> {
    // Try v2 format first
    const repoInfo = await this.loadRepoInfo(options);
    if (repoInfo) {
      const snapshotId = resolveTag(repoInfo, name);
      if (!snapshotId) {
        throw new Error(`Tag not found: ${name}`);
      }
      return ReadSession.open(this.storage, snapshotId, options);
    }

    // V1 fallback - file-based lookup
    const refDirPath = getTagRefDirPath(name);
    const legacyPath = getTagRefPath(name);
    const refPath = await this.findLatestRefFile(refDirPath, legacyPath);
    if (!refPath) {
      throw new Error(`Reference not found: ${refDirPath}`);
    }

    // Check for tombstone only in no-list fallback path (list-capable storage
    // already handles tombstones in findLatestRefFile). Matches Rust behavior.
    if (
      refPath === legacyPath &&
      (await this.storage.exists(`${refPath}.deleted`, options))
    ) {
      throw new Error(`Tag not found: ${name}`);
    }

    const snapshotId = await this.readSnapshotIdFromRef(refPath, options);
    return ReadSession.open(this.storage, snapshotId, options);
  }

  /**
   * Checkout a specific snapshot by ID.
   *
   * @param snapshotId - Snapshot ID (12 bytes or Base32 string)
   * @param options - Optional request options (signal for cancellation)
   * @returns Read session at the specified snapshot
   */
  async checkoutSnapshot(
    snapshotId: Uint8Array | string,
    options?: RequestOptions,
  ): Promise<ReadSession> {
    const id =
      typeof snapshotId === "string"
        ? decodeObjectId12(snapshotId)
        : snapshotId;
    return ReadSession.open(this.storage, id, options);
  }

  /**
   * Walk the snapshot history chain starting from the given session.
   *
   * Yields `{ id, message, flushedAt, metadata }` for each snapshot,
   * walking from the current snapshot back to the root.
   *
   * @param session - Read session to start from
   * @param options - Optional request options (signal for cancellation)
   */
  async *walkHistory(
    session: ReadSession,
    options?: RequestOptions,
  ): AsyncGenerator<{
    id: string;
    message: string;
    flushedAt: Date;
    metadata: Record<string, unknown>;
  }> {
    let current: ReadSession | null = session;

    while (current) {
      yield {
        id: encodeObjectId12(current.getSnapshotId()),
        message: current.getMessage(),
        flushedAt: current.getFlushedAt(),
        metadata: current.getSnapshotMetadata(),
      };

      const parentId = current.getParentSnapshotId();
      if (!parentId) break;

      current = await this.checkoutSnapshot(parentId, options);
    }
  }

  /**
   * Get the storage backend.
   */
  getStorage(): Storage {
    return this.storage;
  }

  /** Read and parse a ref file */
  private async readRef(
    path: string,
    options?: RequestOptions,
  ): Promise<RefData> {
    const data = await this.storage.getObject(path, undefined, options);
    const json = new TextDecoder().decode(data);
    return JSON.parse(json) as RefData;
  }

  /** Read snapshot ID from a ref file */
  private async readSnapshotIdFromRef(
    path: string,
    options?: RequestOptions,
  ): Promise<Uint8Array> {
    try {
      const ref = await this.readRef(path, options);
      return decodeObjectId12(ref.snapshot);
    } catch (error) {
      if (error instanceof NotFoundError) {
        throw new Error(`Reference not found: ${path}`);
      }
      throw error;
    }
  }
}
