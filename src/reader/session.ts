/**
 * ReadSession - Read-only session for accessing icechunk data.
 */

import type { Storage, ByteRange, RequestOptions } from "../storage/storage.js";
import { AbortError } from "../storage/storage.js";
import { decompress } from "fzstd";
import { LRUCache } from "../cache/lru.js";
import {
  parseHeader,
  validateFileType,
  getDataAfterHeader,
  FileType,
  CompressionAlgorithm,
  SpecVersion,
} from "../format/header.js";
import {
  getSnapshotPath,
  getManifestPath,
  getChunkPath,
  getTransactionLogPath,
} from "../format/constants.js";
import { encodeObjectId12 } from "../format/object-id.js";
import {
  parseSnapshot,
  parseManifest,
  parseTransactionLog,
  findChunkRef,
  getChunkPayload,
  deserializeMetadata,
  type Snapshot,
  type Manifest,
  type NodeSnapshot,
  type ChunkPayload,
  type ObjectId12,
  type TransactionLogEntry,
} from "../format/flatbuffers/index.js";
import { NotFoundError } from "../storage/storage.js";

/**
 * ReadSession provides read access to a specific snapshot.
 *
 * Use this class to:
 * - Get nodes (arrays/groups) by path
 * - Read chunk data
 * - Access Zarr metadata
 */
export class ReadSession {
  private storage: Storage;
  private snapshot: Snapshot;
  private specVersion: SpecVersion;
  private manifestCache: LRUCache<string, Manifest>;

  private constructor(
    storage: Storage,
    snapshot: Snapshot,
    specVersion: SpecVersion,
    maxManifestCacheSize: number = 100,
  ) {
    this.storage = storage;
    this.snapshot = snapshot;
    this.specVersion = specVersion;
    this.manifestCache = new LRUCache(maxManifestCacheSize);
  }

  /**
   * Open a read session for a specific snapshot.
   *
   * @param storage - Storage backend
   * @param snapshotId - Snapshot ID (12 bytes)
   * @param options - Optional request options (signal for cancellation)
   * @returns ReadSession instance
   */
  static async open(
    storage: Storage,
    snapshotId: Uint8Array,
    options?: RequestOptions & { maxManifestCacheSize?: number },
  ): Promise<ReadSession> {
    const { snapshot, specVersion } = await ReadSession.loadSnapshot(
      storage,
      snapshotId,
      options,
    );
    return new ReadSession(
      storage,
      snapshot,
      specVersion,
      options?.maxManifestCacheSize,
    );
  }

  /** Load and parse a snapshot from storage */
  private static async loadSnapshot(
    storage: Storage,
    snapshotId: Uint8Array,
    options?: RequestOptions,
  ): Promise<{ snapshot: Snapshot; specVersion: SpecVersion }> {
    const path = getSnapshotPath(encodeObjectId12(snapshotId));
    const data = await storage.getObject(path, undefined, options);

    // Parse header
    const header = parseHeader(data);
    validateFileType(header, FileType.Snapshot);

    // Decompress if needed
    let flatbufferData = getDataAfterHeader(data);
    if (header.compression === CompressionAlgorithm.Zstd) {
      flatbufferData = decompress(flatbufferData);
    }

    // Parse FlatBuffer
    return {
      snapshot: parseSnapshot(flatbufferData),
      specVersion: header.specVersion,
    };
  }

  /** Load and parse a manifest from storage */
  private async loadManifest(
    manifestId: ObjectId12,
    options?: RequestOptions,
  ): Promise<Manifest> {
    const idStr = encodeObjectId12(manifestId);

    // Check cache first
    const cached = this.manifestCache.get(idStr);
    if (cached) return cached;

    // Load from storage with signal
    const path = getManifestPath(idStr);
    const data = await this.storage.getObject(path, undefined, options);

    // Parse header
    const header = parseHeader(data);
    validateFileType(header, FileType.Manifest);

    // Decompress if needed
    let flatbufferData = getDataAfterHeader(data);
    if (header.compression === CompressionAlgorithm.Zstd) {
      flatbufferData = decompress(flatbufferData);
    }

    // Parse FlatBuffer
    const manifest = parseManifest(flatbufferData);

    // Cache it
    this.manifestCache.set(idStr, manifest);

    return manifest;
  }

  /**
   * Get the snapshot ID.
   */
  getSnapshotId(): ObjectId12 {
    return this.snapshot.id;
  }

  /**
   * Get the spec version of the snapshot.
   */
  getSpecVersion(): SpecVersion {
    return this.specVersion;
  }

  /**
   * Get the parent snapshot ID, or null for root snapshots.
   */
  getParentSnapshotId(): ObjectId12 | null {
    return this.snapshot.parentId;
  }

  /**
   * Get the commit message for this snapshot.
   */
  getMessage(): string {
    return this.snapshot.message;
  }

  /**
   * Get the timestamp when this snapshot was created.
   */
  getFlushedAt(): Date {
    // flushedAt is microseconds since epoch
    return new Date(Number(this.snapshot.flushedAt / 1000n));
  }

  /**
   * Get deserialized snapshot metadata.
   *
   * Decodes MessagePack (v1) or FlexBuffers (v2) metadata items
   * into a plain key-value object.
   */
  getSnapshotMetadata(): Record<string, unknown> {
    return deserializeMetadata(this.snapshot.metadata, this.specVersion);
  }

  /**
   * Load and parse the transaction log for this snapshot.
   *
   * Returns null if no transaction log exists (e.g., root snapshot).
   *
   * @param options - Optional request options (signal for cancellation)
   * @returns Parsed transaction log entry or null
   */
  async loadTransactionLog(
    options?: RequestOptions,
  ): Promise<TransactionLogEntry | null> {
    const path = getTransactionLogPath(encodeObjectId12(this.snapshot.id));

    let data: Uint8Array;
    try {
      data = await this.storage.getObject(path, undefined, options);
    } catch (error) {
      if (error instanceof NotFoundError) return null;
      throw error;
    }

    const header = parseHeader(data);
    validateFileType(header, FileType.TransactionLog);

    let flatbufferData = getDataAfterHeader(data);
    if (header.compression === CompressionAlgorithm.Zstd) {
      flatbufferData = decompress(flatbufferData);
    }

    return parseTransactionLog(flatbufferData);
  }

  /**
   * Get a node by path.
   *
   * @param path - Absolute path (e.g., "/array" or "/group/nested")
   * @returns NodeSnapshot or null if not found
   */
  getNode(path: string): NodeSnapshot | null {
    // Normalize path
    const normalizedPath = path.startsWith("/") ? path : `/${path}`;

    // Binary search since nodes are sorted by path
    return this.binarySearchNode(normalizedPath);
  }

  /**
   * List all nodes in the snapshot.
   *
   * @returns Array of all nodes
   */
  listNodes(): NodeSnapshot[] {
    return [...this.snapshot.nodes];
  }

  /**
   * List children of a group.
   *
   * @param parentPath - Path to the parent group (use "/" for root)
   * @returns Array of child nodes
   */
  listChildren(parentPath: string): NodeSnapshot[] {
    const normalizedParent = parentPath === "/" ? "" : parentPath;
    const prefix = normalizedParent + "/";

    return this.snapshot.nodes.filter((node) => {
      if (!node.path.startsWith(prefix)) return false;
      // Check it's a direct child (no more slashes after prefix)
      const rest = node.path.slice(prefix.length);
      return !rest.includes("/");
    });
  }

  /**
   * Get the Zarr metadata for a node.
   *
   * @param path - Path to the node
   * @returns Parsed JSON metadata or null if node not found
   */
  getMetadata(path: string): unknown | null {
    const node = this.getNode(path);
    if (!node) return null;

    const json = new TextDecoder().decode(node.userData);
    return JSON.parse(json);
  }

  /**
   * Get raw user data (Zarr metadata bytes) for a node.
   *
   * @param path - Path to the node
   * @returns Raw metadata bytes or null if node not found
   */
  getRawMetadata(path: string): Uint8Array | null {
    const node = this.getNode(path);
    if (!node) return null;
    return node.userData;
  }

  /**
   * Read chunk data for an array.
   *
   * @param path - Path to the array
   * @param coords - Chunk coordinates (N-dimensional)
   * @param options - Optional request options (signal for cancellation)
   * @returns Chunk data bytes or null if not found
   */
  async getChunk(
    path: string,
    coords: number[],
    options?: RequestOptions,
  ): Promise<Uint8Array | null> {
    // Early abort check
    if (options?.signal?.aborted) return null;

    const node = this.getNode(path);
    if (!node || node.nodeData.type !== "array") {
      return null;
    }

    // Find the manifest that contains this chunk
    const arrayData = node.nodeData;

    try {
      for (const manifestRef of arrayData.manifests) {
        // Check if this manifest covers the requested coordinates
        if (!this.coordsInExtents(coords, manifestRef.extents)) {
          continue;
        }

        // Load the manifest with signal
        const manifest = await this.loadManifest(manifestRef.objectId, options);

        // Find the chunk reference
        const chunkRef = findChunkRef(manifest, node.id, coords);
        if (!chunkRef) continue;

        // Fetch the chunk data based on payload type with signal
        const payload = getChunkPayload(chunkRef);
        return this.fetchChunkPayload(payload, options);
      }
    } catch (error) {
      // Mid-flight abort → return null
      if (error instanceof AbortError) return null;
      throw error;
    }

    return null;
  }

  /**
   * Read a byte range of chunk data for an array.
   *
   * Like getChunk, but fetches only the requested byte range from storage
   * instead of the full chunk. Used by IcechunkStore.getRange to support
   * zarrita's sharded array reads efficiently.
   *
   * @param path - Path to the array
   * @param coords - Chunk coordinates (N-dimensional)
   * @param range - Byte range within the chunk data
   * @param options - Optional request options (signal for cancellation)
   * @returns Chunk data bytes or null if not found
   */
  async getChunkRange(
    path: string,
    coords: number[],
    range: { offset: number; length: number } | { suffixLength: number },
    options?: RequestOptions,
  ): Promise<Uint8Array | null> {
    if (options?.signal?.aborted) return null;

    const node = this.getNode(path);
    if (!node || node.nodeData.type !== "array") {
      return null;
    }

    const arrayData = node.nodeData;

    try {
      for (const manifestRef of arrayData.manifests) {
        if (!this.coordsInExtents(coords, manifestRef.extents)) {
          continue;
        }

        const manifest = await this.loadManifest(manifestRef.objectId, options);
        const chunkRef = findChunkRef(manifest, node.id, coords);
        if (!chunkRef) continue;

        const payload = getChunkPayload(chunkRef);
        return this.fetchChunkPayloadRange(payload, range, options);
      }
    } catch (error) {
      if (error instanceof AbortError) return null;
      throw error;
    }

    return null;
  }

  /** Check if coordinates fall within extent ranges */
  private coordsInExtents(
    coords: number[],
    extents: Array<{ from: number; to: number }>,
  ): boolean {
    if (coords.length !== extents.length) return false;

    for (let i = 0; i < coords.length; i++) {
      const { from, to } = extents[i];
      if (coords[i] < from || coords[i] >= to) {
        return false;
      }
    }

    return true;
  }

  /** Fetch chunk data based on payload type */
  private async fetchChunkPayload(
    payload: ChunkPayload,
    options?: RequestOptions,
  ): Promise<Uint8Array> {
    switch (payload.type) {
      case "inline":
        return payload.data;

      case "native": {
        const path = getChunkPath(encodeObjectId12(payload.chunkId));
        const range: ByteRange = {
          start: payload.offset,
          end: payload.offset + payload.length,
        };
        const data = await this.storage.getObject(path, range, options);
        if (data.length === payload.length) return data;

        // Range header may be ignored (e.g. HTTP 200 full body). If the full object
        // is available, slice out the requested window explicitly.
        if (data.length >= range.end) {
          return data.slice(range.start, range.end);
        }

        throw new Error(
          `Storage returned ${data.length} bytes for ${path} range ${range.start}-${range.end - 1}; expected ${payload.length} bytes`,
        );
      }

      case "virtual": {
        // Virtual chunks reference external URLs
        // Translate cloud storage URLs to HTTPS endpoints
        const httpUrl = translateToHttpUrl(payload.location);
        const headers: Record<string, string> = {
          Range: `bytes=${payload.offset}-${payload.offset + payload.length - 1}`,
        };

        // Add conditional request headers for integrity validation
        if (payload.checksumEtag) {
          headers["If-None-Match"] = payload.checksumEtag;
        }
        if (payload.checksumLastModified > 0) {
          headers["If-Modified-Since"] = new Date(
            payload.checksumLastModified * 1000,
          ).toUTCString();
        }

        const fetchInit: RequestInit = {
          headers,
          signal: options?.signal,
        };

        let response: Response;
        try {
          const client = options?.fetchClient;
          response = client
            ? await client.fetch(httpUrl, fetchInit)
            : await fetch(httpUrl, fetchInit);
        } catch (error) {
          // Translate abort errors to our class (handles DOMException and other implementations)
          if (error instanceof Error && error.name === "AbortError") {
            throw new AbortError();
          }
          throw error;
        }

        if (response.status === 304) {
          throw new Error(
            `Virtual chunk at ${httpUrl} returned 304 Not Modified — no local cache available to serve stale data`,
          );
        }

        if (response.status !== 200 && response.status !== 206) {
          throw new Error(
            `Failed to fetch virtual chunk from ${httpUrl}: ${response.status} ${response.statusText}`,
          );
        }

        const data = new Uint8Array(await response.arrayBuffer());
        if (response.status === 206) {
          if (data.length !== payload.length) {
            throw new Error(
              `Virtual range response size mismatch for ${httpUrl}: expected ${payload.length} bytes, got ${data.length}`,
            );
          }
          return data;
        }

        const absoluteEnd = payload.offset + payload.length;
        // 200 means the Range request was ignored; only accept it if we can prove
        // we received enough bytes to slice the requested absolute window.
        if (data.length >= absoluteEnd) {
          return data.slice(payload.offset, absoluteEnd);
        }

        throw new Error(
          `Virtual range request not honored for ${httpUrl}: need at least ${absoluteEnd} bytes for fallback slicing, got ${data.length}`,
        );
      }
    }
  }

  /** Fetch a byte range of chunk data based on payload type */
  private async fetchChunkPayloadRange(
    payload: ChunkPayload,
    range: { offset: number; length: number } | { suffixLength: number },
    options?: RequestOptions,
  ): Promise<Uint8Array> {
    // Compute absolute start/end within the chunk's data
    let rangeStart: number;
    let rangeEnd: number;

    if ("suffixLength" in range) {
      rangeStart =
        payload.type === "inline"
          ? payload.data.length - range.suffixLength
          : payload.length - range.suffixLength;
      rangeEnd =
        payload.type === "inline" ? payload.data.length : payload.length;
    } else {
      rangeStart = range.offset;
      rangeEnd = range.offset + range.length;
    }

    switch (payload.type) {
      case "inline":
        return payload.data.slice(rangeStart, rangeEnd);

      case "native": {
        const path = getChunkPath(encodeObjectId12(payload.chunkId));
        const storageRange: ByteRange = {
          start: payload.offset + rangeStart,
          end: payload.offset + rangeEnd,
        };
        const expectedSize = rangeEnd - rangeStart;
        const data = await this.storage.getObject(path, storageRange, options);
        if (data.length === expectedSize) return data;

        // Range header may be ignored (e.g. HTTP 200 full body). If the full object
        // is available, slice out the requested window explicitly.
        if (data.length >= storageRange.end) {
          return data.slice(storageRange.start, storageRange.end);
        }

        throw new Error(
          `Storage returned ${data.length} bytes for ${path} range ${storageRange.start}-${storageRange.end - 1}; expected ${expectedSize} bytes`,
        );
      }

      case "virtual": {
        const absoluteStart = payload.offset + rangeStart;
        const absoluteEnd = payload.offset + rangeEnd;

        const httpUrl = translateToHttpUrl(payload.location);
        const headers: Record<string, string> = {
          Range: `bytes=${absoluteStart}-${absoluteEnd - 1}`,
        };

        // Add conditional request headers for integrity validation
        if (payload.checksumEtag) {
          headers["If-None-Match"] = payload.checksumEtag;
        }
        if (payload.checksumLastModified > 0) {
          headers["If-Modified-Since"] = new Date(
            payload.checksumLastModified * 1000,
          ).toUTCString();
        }

        const fetchInit: RequestInit = {
          headers,
          signal: options?.signal,
        };

        let response: Response;
        try {
          const client = options?.fetchClient;
          response = client
            ? await client.fetch(httpUrl, fetchInit)
            : await fetch(httpUrl, fetchInit);
        } catch (error) {
          if (error instanceof Error && error.name === "AbortError") {
            throw new AbortError();
          }
          throw error;
        }

        if (response.status === 304) {
          throw new Error(
            `Virtual chunk at ${httpUrl} returned 304 Not Modified — no local cache available to serve stale data`,
          );
        }

        if (response.status !== 200 && response.status !== 206) {
          throw new Error(
            `Failed to fetch virtual chunk from ${httpUrl}: ${response.status} ${response.statusText}`,
          );
        }

        const expectedSize = rangeEnd - rangeStart;
        const data = new Uint8Array(await response.arrayBuffer());

        if (response.status === 206) {
          if (data.length !== expectedSize) {
            throw new Error(
              `Virtual range response size mismatch for ${httpUrl}: expected ${expectedSize} bytes, got ${data.length}`,
            );
          }
          return data;
        }

        // 200 means the Range request was ignored; only accept it if we can prove
        // we received enough bytes to slice the requested absolute window.
        if (data.length >= absoluteEnd) {
          return data.slice(absoluteStart, absoluteEnd);
        }

        throw new Error(
          `Virtual range request not honored for ${httpUrl}: need at least ${absoluteEnd} bytes for fallback slicing, got ${data.length}`,
        );
      }
    }
  }

  /** Binary search for a node by path */
  private binarySearchNode(path: string): NodeSnapshot | null {
    const nodes = this.snapshot.nodes;
    let low = 0;
    let high = nodes.length - 1;

    while (low <= high) {
      const mid = Math.floor((low + high) / 2);
      const cmp = compareUtf8Bytes(nodes[mid].path, path);

      if (cmp === 0) {
        return nodes[mid];
      } else if (cmp < 0) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }

    return null;
  }
}

/** UTF-8 encoder for byte comparisons */
const utf8Encoder = new TextEncoder();

/**
 * Compare two strings by UTF-8 byte order to match Rust's str::cmp.
 *
 * JavaScript's native string comparison uses UTF-16 code units, which differs
 * from UTF-8 byte order for characters outside the Basic Multilingual Plane
 * (code points > 0xFFFF, like emoji).
 */
function compareUtf8Bytes(a: string, b: string): number {
  const bytesA = utf8Encoder.encode(a);
  const bytesB = utf8Encoder.encode(b);

  const minLen = Math.min(bytesA.length, bytesB.length);
  for (let i = 0; i < minLen; i++) {
    if (bytesA[i] !== bytesB[i]) {
      return bytesA[i] - bytesB[i];
    }
  }

  return bytesA.length - bytesB.length;
}

/**
 * Translate cloud storage URLs to HTTP(S) endpoints for public buckets.
 *
 * Supports:
 * - s3://bucket/key → https://bucket.s3.amazonaws.com/key (or path-style for dotted buckets)
 * - gs://bucket/key or gcs://bucket/key → https://storage.googleapis.com/bucket/key
 * - az://account/container/path or azure://account/container/path → https://account.blob.core.windows.net/container/path
 * - abfs://container@account.dfs.core.windows.net/path → https://account.blob.core.windows.net/container/path
 * - http(s):// URLs pass through unchanged
 *
 * Note: S3 URLs use virtual-hosted style for simple bucket names, but fall back to
 * path-style for buckets containing dots (which break SSL certificate validation).
 * For buckets in specific regions, S3 will redirect to the correct endpoint.
 */
function translateToHttpUrl(url: string): string {
  // S3: s3://bucket/key → https://bucket.s3.amazonaws.com/key
  // For buckets with dots, use path-style: https://s3.amazonaws.com/bucket/key
  if (url.startsWith("s3://")) {
    const rest = url.slice(5); // Remove 's3://'
    const slashIndex = rest.indexOf("/");
    if (slashIndex === -1) {
      // Just bucket, no key
      const bucket = rest;
      if (bucket.includes(".")) {
        return `https://s3.amazonaws.com/${bucket}/`;
      }
      return `https://${bucket}.s3.amazonaws.com/`;
    }
    const bucket = rest.slice(0, slashIndex);
    const key = rest.slice(slashIndex + 1);
    // Use path-style for buckets with dots (virtual-hosted fails SSL validation)
    if (bucket.includes(".")) {
      return `https://s3.amazonaws.com/${bucket}/${key}`;
    }
    return `https://${bucket}.s3.amazonaws.com/${key}`;
  }

  // GCS: gs://bucket/key or gcs://bucket/key → https://storage.googleapis.com/bucket/key
  if (url.startsWith("gs://") || url.startsWith("gcs://")) {
    const prefixLen = url.startsWith("gs://") ? 5 : 6;
    const rest = url.slice(prefixLen);
    return `https://storage.googleapis.com/${rest}`;
  }

  // Azure: az://account/container/path or azure://account/container/path
  // → https://account.blob.core.windows.net/container/path
  if (url.startsWith("az://") || url.startsWith("azure://")) {
    const prefixLen = url.startsWith("az://") ? 5 : 8;
    const rest = url.slice(prefixLen);
    const firstSlash = rest.indexOf("/");
    if (firstSlash === -1) {
      return `https://${rest}.blob.core.windows.net/`;
    }
    const account = rest.slice(0, firstSlash);
    const containerAndPath = rest.slice(firstSlash + 1);
    return `https://${account}.blob.core.windows.net/${containerAndPath}`;
  }

  // ABFS: abfs://container@account.dfs.core.windows.net/path
  // → https://account.blob.core.windows.net/container/path
  if (url.startsWith("abfs://")) {
    const rest = url.slice(7); // Remove 'abfs://'
    const atIndex = rest.indexOf("@");
    if (atIndex !== -1) {
      const container = rest.slice(0, atIndex);
      const hostAndPath = rest.slice(atIndex + 1);
      // Extract account from account.dfs.core.windows.net/path
      const firstSlash = hostAndPath.indexOf("/");
      const host =
        firstSlash === -1 ? hostAndPath : hostAndPath.slice(0, firstSlash);
      const path = firstSlash === -1 ? "" : hostAndPath.slice(firstSlash + 1);
      // Extract account name from host (account.dfs.core.windows.net)
      const dotIndex = host.indexOf(".");
      const account = dotIndex === -1 ? host : host.slice(0, dotIndex);
      const suffix = path ? `${container}/${path}` : container;
      return `https://${account}.blob.core.windows.net/${suffix}`;
    }
  }

  // Already HTTP(S) or unsupported scheme - pass through
  return url;
}
