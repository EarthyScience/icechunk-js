/**
 * ReadSession - Read-only session for accessing icechunk data.
 */

import type {
  FetchClient,
  Storage,
  RequestOptions,
} from "../storage/storage.js";
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
import {
  makeStorageStore,
  makeUrlStore,
  type AsyncReadable,
  type RangeCoalescingFn,
} from "./range-coalescer.js";

/** Default byte-gap threshold for zarrita's range coalescer (matches its own default). */
const RANGE_COALESCE_SIZE = 32 * 1024;
// Per-session cap on wrapped range stores. Eviction only reduces future
// coalescing reuse for older backing objects; it does not affect read
// correctness. 256 keeps the cache bounded while covering many active virtual
// URLs or native objects in typical concurrent reads.
const RANGE_STORE_CACHE_SIZE = 256;
type RangeStoreKeyPart = string | number | null | readonly RangeStoreKeyPart[];

function makeRangeStoreCacheKey(parts: readonly RangeStoreKeyPart[]): string {
  return JSON.stringify(parts);
}

/** Options for chunk reads from a ReadSession. */
export interface ReadOptions extends RequestOptions {
  /**
   * Zarrita-backed range coalescing function for chunk payload reads.
   *
   * Pass `zarrita.withRangeCoalescing` to opt in. Concurrent range reads
   * against the same backing object may be merged into one larger request,
   * matching zarrita's abort behavior for merged signals.
   */
  withRangeCoalescing?: RangeCoalescingFn;
}

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
  /**
   * Bounded cache of `AsyncReadable`s wrapped with zarrita's range coalescer.
   * The cache key includes request-option identities that must not share one
   * coalescing queue, notably virtual fetch clients and checksum headers.
   *
   * Promise slots are inserted synchronously on first use, so
   * concurrent requests for the same partition share one coalescing window
   * instead of racing to create parallel stores.
   */
  private rangeStores?: LRUCache<string, Promise<AsyncReadable>>;
  private nativeStore?: AsyncReadable;
  private fetchClientIds?: WeakMap<FetchClient, number>;
  private nextFetchClientId = 1;
  private rangeCoalescerIds?: WeakMap<RangeCoalescingFn, number>;
  private nextRangeCoalescerId = 1;

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

  private getFetchClientKey(fetchClient: FetchClient | undefined): string {
    if (!fetchClient) return "default";
    if (!this.fetchClientIds) this.fetchClientIds = new WeakMap();

    let id = this.fetchClientIds.get(fetchClient);
    if (id === undefined) {
      id = this.nextFetchClientId;
      this.nextFetchClientId = id + 1;
      this.fetchClientIds.set(fetchClient, id);
    }
    return String(id);
  }

  private getRangeCoalescerKey(withRangeCoalescing: RangeCoalescingFn): string {
    if (!this.rangeCoalescerIds) this.rangeCoalescerIds = new WeakMap();

    let id = this.rangeCoalescerIds.get(withRangeCoalescing);
    if (id === undefined) {
      id = this.nextRangeCoalescerId;
      this.nextRangeCoalescerId = id + 1;
      this.rangeCoalescerIds.set(withRangeCoalescing, id);
    }
    return String(id);
  }

  private getRangeStore(
    partitionKey: readonly RangeStoreKeyPart[],
    createStore: () => AsyncReadable,
    withRangeCoalescing: RangeCoalescingFn,
  ): Promise<AsyncReadable> {
    if (!this.rangeStores) {
      this.rangeStores = new LRUCache(RANGE_STORE_CACHE_SIZE);
    }
    const stores = this.rangeStores;
    const cacheKey = makeRangeStoreCacheKey([
      ...partitionKey,
      ["coalescer", this.getRangeCoalescerKey(withRangeCoalescing)],
    ]);

    const cached = stores.get(cacheKey);
    if (cached) return cached;

    const raw = createStore();
    const promise: Promise<AsyncReadable> = Promise.resolve()
      .then(() =>
        withRangeCoalescing(raw, { coalesceSize: RANGE_COALESCE_SIZE }),
      )
      .catch((error: unknown) => {
        if (stores.get(cacheKey) === promise) stores.delete(cacheKey);
        throw error;
      });
    stores.set(cacheKey, promise);
    return promise;
  }

  private getNativeStore(options?: ReadOptions): Promise<AsyncReadable> {
    if (!this.nativeStore) {
      this.nativeStore = makeStorageStore(this.storage);
    }
    const raw = this.nativeStore;
    const withRangeCoalescing = options?.withRangeCoalescing;
    if (!withRangeCoalescing) {
      return Promise.resolve(raw);
    }
    return this.getRangeStore(["native"], () => raw, withRangeCoalescing);
  }

  private getVirtualStoreForPayload(
    httpUrl: string,
    payload: {
      checksumEtag: string | null;
      checksumLastModified: number;
    },
    options: ReadOptions | undefined,
  ): Promise<AsyncReadable> {
    const validate = !!options?.validateChecksums;
    let conditionalHeaders: Record<string, string> | undefined;
    if (validate) {
      conditionalHeaders = {};
      if (payload.checksumEtag) {
        conditionalHeaders["If-Match"] = payload.checksumEtag;
      }
      if (payload.checksumLastModified > 0) {
        conditionalHeaders["If-Unmodified-Since"] = new Date(
          payload.checksumLastModified * 1000,
        ).toUTCString();
      }
    }

    const checksumKey = validate
      ? ["checked", payload.checksumEtag ?? "", payload.checksumLastModified]
      : ["unchecked"];

    const createStore = () =>
      makeUrlStore({
        url: httpUrl,
        fetchClient: options?.fetchClient,
        conditionalHeaders,
      });

    const withRangeCoalescing = options?.withRangeCoalescing;
    if (!withRangeCoalescing) {
      return Promise.resolve(createStore());
    }

    return this.getRangeStore(
      [
        "virtual",
        httpUrl,
        ["fetch", this.getFetchClientKey(options?.fetchClient)],
        checksumKey,
      ],
      createStore,
      withRangeCoalescing,
    );
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
    options?: ReadOptions,
  ): Promise<Uint8Array | null> {
    options?.signal?.throwIfAborted();
    const requestOptions = toRequestOptions(options);

    const node = this.getNode(path);
    if (!node || node.nodeData.type !== "array") {
      return null;
    }

    // Find the manifest that contains this chunk
    const arrayData = node.nodeData;

    for (const manifestRef of arrayData.manifests) {
      // Check if this manifest covers the requested coordinates
      if (!this.coordsInExtents(coords, manifestRef.extents)) {
        continue;
      }

      // Load the manifest with signal
      const manifest = await this.loadManifest(
        manifestRef.objectId,
        requestOptions,
      );

      // Find the chunk reference
      const chunkRef = findChunkRef(manifest, node.id, coords);
      if (!chunkRef) continue;

      // Fetch the chunk data based on payload type with signal
      const payload = getChunkPayload(chunkRef);
      return this.fetchChunkPayload(payload, options);
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
    options?: ReadOptions,
  ): Promise<Uint8Array | null> {
    options?.signal?.throwIfAborted();
    const requestOptions = toRequestOptions(options);

    const node = this.getNode(path);
    if (!node || node.nodeData.type !== "array") {
      return null;
    }

    const arrayData = node.nodeData;

    for (const manifestRef of arrayData.manifests) {
      if (!this.coordsInExtents(coords, manifestRef.extents)) {
        continue;
      }

      const manifest = await this.loadManifest(
        manifestRef.objectId,
        requestOptions,
      );
      const chunkRef = findChunkRef(manifest, node.id, coords);
      if (!chunkRef) continue;

      const payload = getChunkPayload(chunkRef);
      return this.fetchChunkPayloadRange(payload, range, options);
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
    options?: ReadOptions,
  ): Promise<Uint8Array> {
    switch (payload.type) {
      case "inline":
        return payload.data;

      case "native": {
        const path = getChunkPath(encodeObjectId12(payload.chunkId));
        const requestedStart = payload.offset;
        const requestedEnd = payload.offset + payload.length;
        const store = await this.getNativeStore(options);
        const data = await store.getRange(
          path,
          { offset: requestedStart, length: payload.length },
          { signal: options?.signal },
        );
        if (!data) {
          throw new Error(
            `Failed to fetch native chunk from ${path} range ${requestedStart}-${requestedEnd - 1}: empty response`,
          );
        }
        if (data.length === payload.length) return data;

        throw new Error(
          `Storage returned ${data.length} bytes for ${path} range ${requestedStart}-${requestedEnd - 1}; expected ${payload.length} bytes`,
        );
      }

      case "virtual": {
        // Virtual chunks reference external URLs
        // Translate cloud storage URLs to HTTPS endpoints
        const httpUrl = translateToHttpUrl(
          payload.location,
          options?.azureAccount,
        );

        const store = await this.getVirtualStoreForPayload(
          httpUrl,
          payload,
          options,
        );
        const data = await store.getRange(
          "/",
          { offset: payload.offset, length: payload.length },
          { signal: options?.signal },
        );
        if (!data) {
          throw new Error(
            `Failed to fetch virtual chunk from ${httpUrl}: empty response`,
          );
        }
        if (data.length !== payload.length) {
          throw new Error(
            `Virtual range response size mismatch for ${httpUrl}: expected ${payload.length} bytes, got ${data.length}`,
          );
        }
        return data;
      }
    }
  }

  /** Fetch a byte range of chunk data based on payload type */
  private async fetchChunkPayloadRange(
    payload: ChunkPayload,
    range: { offset: number; length: number } | { suffixLength: number },
    options?: ReadOptions,
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
        const requestedStart = payload.offset + rangeStart;
        const expectedSize = rangeEnd - rangeStart;
        const requestedEnd = requestedStart + expectedSize;
        const store = await this.getNativeStore(options);
        const data = await store.getRange(
          path,
          { offset: requestedStart, length: expectedSize },
          { signal: options?.signal },
        );
        if (!data) {
          throw new Error(
            `Failed to fetch native chunk from ${path} range ${requestedStart}-${requestedEnd - 1}: empty response`,
          );
        }
        if (data.length === expectedSize) return data;

        throw new Error(
          `Storage returned ${data.length} bytes for ${path} range ${requestedStart}-${requestedEnd - 1}; expected ${expectedSize} bytes`,
        );
      }

      case "virtual": {
        const absoluteStart = payload.offset + rangeStart;
        const expectedSize = rangeEnd - rangeStart;

        const httpUrl = translateToHttpUrl(
          payload.location,
          options?.azureAccount,
        );

        const store = await this.getVirtualStoreForPayload(
          httpUrl,
          payload,
          options,
        );
        const data = await store.getRange(
          "/",
          { offset: absoluteStart, length: expectedSize },
          { signal: options?.signal },
        );
        if (!data) {
          throw new Error(
            `Failed to fetch virtual chunk from ${httpUrl}: empty response`,
          );
        }
        if (data.length !== expectedSize) {
          throw new Error(
            `Virtual range response size mismatch for ${httpUrl}: expected ${expectedSize} bytes, got ${data.length}`,
          );
        }
        return data;
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

function toRequestOptions(options?: ReadOptions): RequestOptions | undefined {
  if (!options) return undefined;
  return {
    ...(options.signal && { signal: options.signal }),
    ...(options.fetchClient && { fetchClient: options.fetchClient }),
    ...(options.validateChecksums !== undefined && {
      validateChecksums: options.validateChecksums,
    }),
    ...(options.azureAccount !== undefined && {
      azureAccount: options.azureAccount,
    }),
  };
}

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
 * - az://container/path or azure://container/path → https://{azureAccount}.blob.core.windows.net/container/path
 * - abfs://container@account.dfs.core.windows.net/path → https://account.blob.core.windows.net/container/path
 * - http(s):// URLs pass through unchanged
 *
 * Azure az:// and azure:// URLs follow the Rust convention where the host is
 * the container name (not the account). The account must be supplied separately
 * via the azureAccount parameter.
 *
 * Note: S3 URLs use virtual-hosted style for simple bucket names, but fall back to
 * path-style for buckets containing dots (which break SSL certificate validation).
 * For buckets in specific regions, S3 will redirect to the correct endpoint.
 */
function translateToHttpUrl(url: string, azureAccount?: string): string {
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

  // Azure: az://container/path or azure://container/path
  // → https://{azureAccount}.blob.core.windows.net/container/path
  // Matches Rust convention: container is in the host position, account from config.
  if (url.startsWith("az://") || url.startsWith("azure://")) {
    if (!azureAccount) {
      throw new Error(
        `Cannot translate Azure URL "${url}": azureAccount option is required. ` +
          `az:// and azure:// URLs encode only the container name; ` +
          `pass azureAccount in store options to supply the storage account.`,
      );
    }
    const prefixLen = url.startsWith("az://") ? 5 : 8;
    const rest = url.slice(prefixLen);
    const firstSlash = rest.indexOf("/");
    if (firstSlash === -1) {
      // Just container, no path
      return `https://${azureAccount}.blob.core.windows.net/${rest}`;
    }
    const container = rest.slice(0, firstSlash);
    const path = rest.slice(firstSlash + 1);
    return `https://${azureAccount}.blob.core.windows.net/${container}/${path}`;
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
