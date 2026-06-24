/**
 * Parser for icechunk Repo FlatBuffer format (v2).
 *
 * Uses flatc-generated TypeScript classes for type-safe field access.
 */

import { ByteBuffer } from "flatbuffers";
import * as flexbuffers from "flatbuffers/js/flexbuffers.js";
import { Repo as FbsRepo } from "./generated/repo.js";
import { decompress } from "../../vendor/fzstd/index.js";
import {
  parseHeader,
  getDataAfterHeader,
  FileType,
  validateFileType,
  CompressionAlgorithm,
  HEADER_SIZE,
} from "../header.js";

const SUPPORTED_SPEC_VERSION = 2;

/**
 * S3 store config carried by a virtual chunk container, mirroring Rust's
 * `S3Options`. Drives endpoint/addressing when translating `s3://` chunk
 * locations to HTTPS — notably `region`, which lets dotted-name buckets
 * address their regional endpoint directly (required for browser CORS, since
 * the global endpoint's region redirect carries no CORS headers).
 */
export interface S3ContainerConfig {
  region?: string;
  endpointUrl?: string;
  forcePathStyle?: boolean;
}

/**
 * A virtual chunk container from the repo config, mirroring Rust's
 * `VirtualChunkContainer`. `name` resolves `vcc://name/...` references;
 * `urlPrefix` matches absolute chunk locations (longest prefix wins).
 */
export interface VirtualChunkContainer {
  name: string | null;
  urlPrefix: string;
  s3?: S3ContainerConfig;
}

/** Parsed repo file with cached metadata */
export interface ParsedRepo {
  repo: FbsRepo;
  specVersion: number;
  snapshotsLength: number; // Cached for bounds checking
  /**
   * Virtual chunk containers from the repo config, sorted by descending
   * `urlPrefix` length so the most specific container matches an absolute
   * location first. Empty when the repo declares no containers.
   */
  virtualChunkContainers: VirtualChunkContainer[];
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
  const virtualChunkContainers = parseVirtualChunkContainers(repo);

  return { repo, specVersion, snapshotsLength, virtualChunkContainers };
}

/**
 * Extract virtual chunk containers from the flexbuffers-encoded
 * RepositoryConfig, including each container's S3 store config (region/
 * endpoint/addressing). Unnamed containers are included too — they still
 * match absolute chunk locations by `urlPrefix` even though they can't be
 * referenced by `vcc://name`. Returned sorted by descending `urlPrefix`
 * length so the most specific container matches first.
 *
 * Returns an empty array when the config is absent or malformed. Parsing
 * failures are swallowed because virtual chunk resolution is best-effort; a
 * `vcc://` location with no matching name surfaces a clear error at fetch time.
 */
function parseVirtualChunkContainers(repo: FbsRepo): VirtualChunkContainer[] {
  const result: VirtualChunkContainer[] = [];
  const configBytes = repo.configArray();
  if (!configBytes || configBytes.length === 0) return result;

  let config: unknown;
  try {
    const ab = configBytes.buffer.slice(
      configBytes.byteOffset,
      configBytes.byteOffset + configBytes.byteLength,
    ) as ArrayBuffer;
    config = flexbuffers.toObject(ab);
  } catch {
    return result;
  }

  if (!isRecord(config)) return result;
  const containers = config.virtual_chunk_containers;
  if (!isRecord(containers)) return result;

  for (const container of Object.values(containers)) {
    if (!isRecord(container)) continue;
    const urlPrefix = container.url_prefix;
    if (typeof urlPrefix !== "string") continue;
    const name = typeof container.name === "string" ? container.name : null;
    const s3 = parseS3Store(container.store);
    result.push(s3 ? { name, urlPrefix, s3 } : { name, urlPrefix });
  }

  // Most specific (longest) url_prefix first, so absolute-location matching
  // prefers the narrowest container.
  result.sort((a, b) => b.urlPrefix.length - a.urlPrefix.length);
  return result;
}

/**
 * Parse the S3-family store config from a container's `store`. The `s3`,
 * `s3_compatible`, and `tigris` ObjectStoreConfig variants all share Rust's
 * `S3Options` shape; gcs/azure/http/local stores yield undefined.
 */
function parseS3Store(store: unknown): S3ContainerConfig | undefined {
  if (!isRecord(store)) return undefined;
  const s3 = store.s3 ?? store.s3_compatible ?? store.tigris;
  if (!isRecord(s3)) return undefined;
  const cfg: S3ContainerConfig = {};
  if (typeof s3.region === "string") cfg.region = s3.region;
  if (typeof s3.endpoint_url === "string") cfg.endpointUrl = s3.endpoint_url;
  if (typeof s3.force_path_style === "boolean") {
    cfg.forcePathStyle = s3.force_path_style;
  }
  return cfg;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
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
