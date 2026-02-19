/**
 * Parser for icechunk Repo FlatBuffer format (v2).
 *
 * Uses flatc-generated TypeScript classes for type-safe field access.
 */

import { ByteBuffer } from "flatbuffers";
import { Repo as FbsRepo } from "./generated/repo.js";
import { decompress } from "fzstd";
import {
  parseHeader,
  getDataAfterHeader,
  FileType,
  validateFileType,
  CompressionAlgorithm,
  HEADER_SIZE,
} from "../header.js";

const SUPPORTED_SPEC_VERSION = 2;

/** Parsed repo file with cached metadata */
export interface ParsedRepo {
  repo: FbsRepo;
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
    throw new Error(
      `Repo file too small: ${data.length} bytes, need at least ${HEADER_SIZE}`,
    );
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
  const bb = new ByteBuffer(fbData);
  const repo = FbsRepo.getRootAsRepo(bb);
  const specVersion = repo.specVersion();

  if (specVersion !== SUPPORTED_SPEC_VERSION) {
    throw new Error(
      `Unsupported repo spec version: ${specVersion}, expected ${SUPPORTED_SPEC_VERSION}`,
    );
  }

  const snapshotsLength = repo.snapshotsLength();

  return { repo, specVersion, snapshotsLength };
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
 * Binary search for a ref by name in tags or branches.
 *
 * @returns Snapshot ID bytes if found, null otherwise
 * @throws Error on corrupted data (null tables, invalid indices)
 */
function binarySearchRef(
  parsedRepo: ParsedRepo,
  accessor: (index: number) => ReturnType<FbsRepo["tags"]>,
  length: number,
  name: string,
): Uint8Array | null {
  const { snapshotsLength } = parsedRepo;
  if (length === 0) return null;

  // Cache target bytes outside loop
  const targetBytes = new TextEncoder().encode(name);

  let low = 0;
  let high = length - 1;

  while (low <= high) {
    const mid = (low + high) >>> 1;
    const refTable = accessor(mid);

    if (!refTable) {
      throw new Error(`Corrupted repo file: null ref table at index ${mid}`);
    }

    const refName = refTable.name();
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
      const snapshotIndex = refTable.snapshotIndex();

      if (snapshotIndex >= snapshotsLength) {
        throw new Error(
          `Invalid snapshot index ${snapshotIndex} for ref '${name}', ` +
            `snapshots array has ${snapshotsLength} entries`,
        );
      }

      return getSnapshotIdByIndex(parsedRepo, snapshotIndex);
    }
  }

  return null;
}

/**
 * Get snapshot ID by index from the snapshots vector.
 *
 * @throws Error on corrupted data (null snapshot, missing id)
 */
function getSnapshotIdByIndex(
  parsedRepo: ParsedRepo,
  index: number,
): Uint8Array {
  const snapshotInfo = parsedRepo.repo.snapshots(index);
  if (!snapshotInfo) {
    throw new Error(`Corrupted repo file: null snapshot at index ${index}`);
  }

  const idObj = snapshotInfo.id();
  if (!idObj) {
    throw new Error(`Corrupted repo file: snapshot ${index} missing id`);
  }

  return idObj.bb!.bytes().slice(idObj.bb_pos, idObj.bb_pos + 12);
}

/**
 * List all ref names from tags or branches.
 *
 * @throws Error on corrupted data (null tables, null names)
 */
function listRefs(
  accessor: (index: number) => ReturnType<FbsRepo["tags"]>,
  length: number,
): string[] {
  const names: string[] = [];

  for (let i = 0; i < length; i++) {
    const refTable = accessor(i);
    if (!refTable) {
      throw new Error(`Corrupted repo file: null ref table at index ${i}`);
    }
    const name = refTable.name();
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
export function resolveBranch(
  parsedRepo: ParsedRepo,
  name: string,
): Uint8Array | null {
  const { repo } = parsedRepo;
  return binarySearchRef(
    parsedRepo,
    (i) => repo.branches(i),
    repo.branchesLength(),
    name,
  );
}

/**
 * Resolve a tag name to its snapshot ID.
 *
 * @returns Snapshot ID bytes if found, null otherwise
 */
export function resolveTag(
  parsedRepo: ParsedRepo,
  name: string,
): Uint8Array | null {
  const { repo } = parsedRepo;
  return binarySearchRef(
    parsedRepo,
    (i) => repo.tags(i),
    repo.tagsLength(),
    name,
  );
}

/**
 * List all branch names in the repository.
 *
 * @returns Array of branch names (in storage order, which is sorted)
 */
export function listBranchesFromRepo(parsedRepo: ParsedRepo): string[] {
  const { repo } = parsedRepo;
  return listRefs((i) => repo.branches(i), repo.branchesLength());
}

/**
 * List all active tag names in the repository.
 *
 * Note: Returns only active tags from the tags vector, not deleted_tags.
 *
 * @returns Array of tag names (in storage order, which is sorted)
 */
export function listTagsFromRepo(parsedRepo: ParsedRepo): string[] {
  const { repo } = parsedRepo;
  return listRefs((i) => repo.tags(i), repo.tagsLength());
}
