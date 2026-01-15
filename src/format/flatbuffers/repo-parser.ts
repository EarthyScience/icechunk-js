/**
 * Parser for icechunk Repo FlatBuffer format (v2).
 *
 * Field indices based on repo.fbs schema order.
 */

import { parseRootTable, TableReader } from './reader.js';
import { decompress } from 'fzstd';
import {
  parseHeader,
  getDataAfterHeader,
  FileType,
  validateFileType,
  CompressionAlgorithm,
  HEADER_SIZE,
} from '../header.js';

// Repo table fields
const REPO_SPEC_VERSION = 0; // uint8 per schema
const REPO_TAGS = 1;
const REPO_BRANCHES = 2;
// const REPO_DELETED_TAGS = 3;  // Not used for listing - tombstone tracking only
const REPO_SNAPSHOTS = 4;

// Ref table fields
const REF_NAME = 0;
const REF_SNAPSHOT_INDEX = 1;

// SnapshotInfo table fields
const SNAPSHOT_INFO_ID = 0; // ObjectId12 inline struct (12 bytes)

const OBJECT_ID_12_SIZE = 12;
const SUPPORTED_SPEC_VERSION = 2;

/** Parsed repo file with cached metadata */
export interface ParsedRepo {
  root: TableReader;
  specVersion: number;
  snapshotsLength: number; // Cached for bounds checking
}

/**
 * Parse a v2 repo file from raw file data (including icechunk header).
 *
 * @throws Error if header is invalid, file type is wrong, or spec_version is unsupported
 */
export function parseRepo(data: Uint8Array): ParsedRepo {
  // Validate minimum size
  if (data.length < HEADER_SIZE) {
    throw new Error(`Repo file too small: ${data.length} bytes, need at least ${HEADER_SIZE}`);
  }

  // Parse and validate icechunk header
  const header = parseHeader(data);
  validateFileType(header, FileType.RepoInfo);

  // Get data after header and decompress if needed
  let fbData = getDataAfterHeader(data);
  if (header.compression === CompressionAlgorithm.Zstd) {
    fbData = decompress(fbData);
  }

  // Parse FlatBuffer root table
  const root = parseRootTable(fbData);
  const specVersion = root.readUint8(REPO_SPEC_VERSION, 0);

  if (specVersion !== SUPPORTED_SPEC_VERSION) {
    throw new Error(
      `Unsupported repo spec version: ${specVersion}, expected ${SUPPORTED_SPEC_VERSION}`
    );
  }

  const snapshotsLength = root.getVectorLength(REPO_SNAPSHOTS);

  return { root, specVersion, snapshotsLength };
}

/**
 * Compare two byte arrays by UTF-8 byte order (matching Rust's Ord for String).
 */
function compareUtf8ByteOrder(aBytes: Uint8Array, bBytes: Uint8Array): number {
  const minLen = Math.min(aBytes.length, bBytes.length);

  for (let i = 0; i < minLen; i++) {
    if (aBytes[i] < bBytes[i]) return -1;
    if (aBytes[i] > bBytes[i]) return 1;
  }

  return aBytes.length - bBytes.length;
}

/**
 * Binary search for a ref by name in a sorted vector.
 *
 * @returns Snapshot ID bytes if found, null otherwise
 * @throws Error on corrupted data (null tables, invalid indices)
 */
function binarySearchRef(
  repo: ParsedRepo,
  fieldIndex: number,
  name: string
): Uint8Array | null {
  const { root, snapshotsLength } = repo;
  const length = root.getVectorLength(fieldIndex);
  if (length === 0) return null;

  // Cache target bytes outside loop
  const targetBytes = new TextEncoder().encode(name);

  let low = 0;
  let high = length - 1;

  while (low <= high) {
    const mid = (low + high) >>> 1;
    const refTable = root.getVectorTable(fieldIndex, mid);

    if (!refTable) {
      throw new Error(`Corrupted repo file: null ref table at index ${mid}`);
    }

    const refName = refTable.readString(REF_NAME);
    if (refName === null) {
      throw new Error(`Corrupted repo file: null ref name at index ${mid}`);
    }

    const refNameBytes = new TextEncoder().encode(refName);
    const cmp = compareUtf8ByteOrder(refNameBytes, targetBytes);

    if (cmp < 0) {
      low = mid + 1;
    } else if (cmp > 0) {
      high = mid - 1;
    } else {
      // Found - validate and return snapshot ID
      const snapshotIndex = refTable.readUint32(REF_SNAPSHOT_INDEX);

      if (snapshotIndex >= snapshotsLength) {
        throw new Error(
          `Invalid snapshot index ${snapshotIndex} for ref '${name}', ` +
            `snapshots array has ${snapshotsLength} entries`
        );
      }

      return getSnapshotIdByIndex(root, snapshotIndex);
    }
  }

  return null;
}

/**
 * Get snapshot ID by index from the snapshots vector.
 *
 * @throws Error on corrupted data (null snapshot, missing id)
 */
function getSnapshotIdByIndex(root: TableReader, index: number): Uint8Array {
  const snapshotTable = root.getVectorTable(REPO_SNAPSHOTS, index);
  if (!snapshotTable) {
    throw new Error(`Corrupted repo file: null snapshot at index ${index}`);
  }

  const idBytes = snapshotTable.readInlineStruct(SNAPSHOT_INFO_ID, OBJECT_ID_12_SIZE);
  if (!idBytes) {
    throw new Error(`Corrupted repo file: snapshot ${index} missing id`);
  }

  return idBytes;
}

/**
 * List all ref names from a vector.
 *
 * @throws Error on corrupted data (null tables, null names)
 */
function listRefs(root: TableReader, fieldIndex: number): string[] {
  const length = root.getVectorLength(fieldIndex);
  const names: string[] = [];

  for (let i = 0; i < length; i++) {
    const refTable = root.getVectorTable(fieldIndex, i);
    if (!refTable) {
      throw new Error(`Corrupted repo file: null ref table at index ${i}`);
    }
    const name = refTable.readString(REF_NAME);
    if (name === null) {
      throw new Error(`Corrupted repo file: null ref name at index ${i}`);
    }
    names.push(name);
  }

  return names;
}

/**
 * Resolve a branch name to its snapshot ID.
 *
 * @returns Snapshot ID bytes if found, null otherwise
 */
export function resolveBranch(repo: ParsedRepo, name: string): Uint8Array | null {
  return binarySearchRef(repo, REPO_BRANCHES, name);
}

/**
 * Resolve a tag name to its snapshot ID.
 *
 * @returns Snapshot ID bytes if found, null otherwise
 */
export function resolveTag(repo: ParsedRepo, name: string): Uint8Array | null {
  return binarySearchRef(repo, REPO_TAGS, name);
}

/**
 * List all branch names in the repository.
 *
 * @returns Array of branch names (in storage order, which is sorted)
 */
export function listBranchesFromRepo(repo: ParsedRepo): string[] {
  return listRefs(repo.root, REPO_BRANCHES);
}

/**
 * List all active tag names in the repository.
 *
 * Note: Returns only active tags from the tags vector, not deleted_tags.
 *
 * @returns Array of tag names (in storage order, which is sorted)
 */
export function listTagsFromRepo(repo: ParsedRepo): string[] {
  return listRefs(repo.root, REPO_TAGS);
}
