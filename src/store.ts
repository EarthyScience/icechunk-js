/**
 * IcechunkStore - zarrita-compatible store adapter.
 *
 * Implements zarrita's AsyncReadable interface to allow using
 * zarrita for array operations on icechunk repositories.
 */

import { Repository } from "./reader/repository.js";
import { ReadSession } from "./reader/session.js";
import { HttpStorage } from "./storage/http-storage.js";
import type { Storage, FetchClient } from "./storage/storage.js";
import type { NodeSnapshot } from "./format/flatbuffers/types.js";

/**
 * zarrita's AbsolutePath type - paths must start with "/"
 */
export type AbsolutePath<Rest extends string = string> = `/${Rest}`;

/**
 * zarrita's RangeQuery type for partial reads
 */
export type RangeQuery =
  | { offset: number; length: number }
  | { suffixLength: number };

/**
 * zarrita's AsyncReadable interface
 */
export interface AsyncReadable<Options = unknown> {
  get(key: AbsolutePath, opts?: Options): Promise<Uint8Array | undefined>;
  getRange?(
    key: AbsolutePath,
    range: RangeQuery,
    opts?: Options,
  ): Promise<Uint8Array | undefined>;
}

/** Options for IcechunkStore */
export interface IcechunkStoreOptions {
  /** Branch name to checkout (default: "main") */
  branch?: string;

  /** Tag name to checkout (mutually exclusive with branch) */
  tag?: string;

  /** Specific snapshot ID to checkout (Base32 string) */
  snapshot?: string;

  /** AbortSignal for cancelling initialization */
  signal?: AbortSignal;

  /** Format version hint to skip auto-detection. 'v1' skips /repo request. */
  formatVersion?: "v1" | "v2";

  /**
   * Pluggable HTTP client for virtual chunk fetching.
   *
   * Use this to:
   * - Generate pre-signed S3 URLs
   * - Add authentication headers
   * - Route through a proxy
   */
  fetchClient?: FetchClient;

  /** Maximum number of manifests to cache in the LRU cache (default: 100) */
  maxManifestCacheSize?: number;
}

/**
 * IcechunkStore - zarrita-compatible store for icechunk repositories.
 *
 * This store implements zarrita's AsyncReadable interface, allowing you
 * to use zarrita's array operations on icechunk data.
 *
 * @example
 * ```typescript
 * import { IcechunkStore } from '@carbonplan/icechunk-js';
 * import { open, get } from 'zarrita';
 *
 * const store = await IcechunkStore.open('https://bucket.s3.amazonaws.com/repo');
 * const array = await open(store, { kind: 'array', path: '/temperature' });
 * const data = await get(array, [0, 0, null]);
 * ```
 */
export class IcechunkStore implements AsyncReadable {
  /** The underlying read session. Exposed for advanced usage. */
  readonly session: ReadSession;
  private fetchClient?: FetchClient;
  private basePath: string = "";

  private constructor(session: ReadSession, fetchClient?: FetchClient) {
    if (!(session instanceof ReadSession)) {
      throw new Error(
        "IcechunkStore constructor is private. Use IcechunkStore.open() instead.",
      );
    }
    this.session = session;
    this.fetchClient = fetchClient;
  }

  /**
   * Open an IcechunkStore from a URL.
   *
   * @param url - URL to the icechunk repository
   * @param options - Store options (branch, tag, or snapshot to checkout)
   */
  static async open(
    url: string,
    options?: IcechunkStoreOptions,
  ): Promise<IcechunkStore>;

  /**
   * Open an IcechunkStore from an existing ReadSession.
   *
   * @param session - Existing ReadSession
   * @param options - Store options (only fetchClient is used)
   */
  static async open(
    session: ReadSession,
    options?: Pick<IcechunkStoreOptions, "fetchClient">,
  ): Promise<IcechunkStore>;

  /**
   * Open an IcechunkStore from a custom Storage backend.
   *
   * @param storage - Custom Storage implementation
   * @param options - Store options (branch, tag, or snapshot to checkout)
   */
  static async open(
    storage: Storage,
    options?: IcechunkStoreOptions,
  ): Promise<IcechunkStore>;

  static async open(
    arg: string | ReadSession | Storage,
    options: IcechunkStoreOptions = {},
  ): Promise<IcechunkStore> {
    if (arg instanceof ReadSession) {
      return new IcechunkStore(arg, options.fetchClient);
    }

    const storage = typeof arg === "string" ? new HttpStorage(arg) : arg;
    const requestOptions = options.signal
      ? { signal: options.signal }
      : undefined;
    const repo = await Repository.open(
      { storage, formatVersion: options.formatVersion },
      requestOptions,
    );

    // Build session options: merge request options with cache size
    const sessionOptions = {
      ...requestOptions,
      maxManifestCacheSize: options.maxManifestCacheSize,
    };

    let session: ReadSession;
    if (options.snapshot) {
      session = await repo.checkoutSnapshot(options.snapshot, sessionOptions);
    } else if (options.tag) {
      session = await repo.checkoutTag(options.tag, sessionOptions);
    } else {
      session = await repo.checkoutBranch(
        options.branch ?? "main",
        sessionOptions,
      );
    }

    return new IcechunkStore(session, options.fetchClient);
  }

  /**
   * Get data for a zarr key.
   *
   * zarrita calls this method with keys like:
   * - "/zarr.json" - root metadata
   * - "/array/zarr.json" - array metadata
   * - "/array/c/0/1/2" - chunk at coords [0, 1, 2]
   *
   * @param key - Absolute path (must start with "/")
   * @param opts - Optional request options (supports AbortSignal for cancellation)
   * @returns Data bytes or undefined if not found
   */
  async get(
    key: AbsolutePath,
    opts?: { signal?: AbortSignal },
  ): Promise<Uint8Array | undefined> {
    if (opts?.signal?.aborted) return undefined;

    const parsed = parseZarrKey(key);
    const resolvedPath = this.resolvePath(parsed.path);

    try {
      if (parsed.type === "metadata") {
        const data = this.session.getRawMetadata(resolvedPath);
        return data ?? undefined;
      }

      const chunk = await this.session.getChunk(resolvedPath, parsed.coords, {
        signal: opts?.signal,
        ...(this.fetchClient && { fetchClient: this.fetchClient }),
      });
      return chunk ?? undefined;
    } catch {
      return undefined;
    }
  }

  /**
   * Get partial data for a zarr key.
   *
   * Required by zarrita for sharded arrays. zarrita uses this to read the
   * shard index (via suffixLength) and extract individual inner chunks
   * (via offset/length) from shard data.
   *
   * @param key - Absolute path
   * @param range - Byte range to fetch
   * @param opts - Optional request options (supports AbortSignal for cancellation)
   * @returns Data bytes or undefined if not found
   */
  async getRange(
    key: AbsolutePath,
    range: RangeQuery,
    opts?: { signal?: AbortSignal },
  ): Promise<Uint8Array | undefined> {
    if (opts?.signal?.aborted) return undefined;

    const parsed = parseZarrKey(key);
    const resolvedPath = this.resolvePath(parsed.path);

    try {
      if (parsed.type === "chunk") {
        // Use targeted byte-range read through the session
        const data = await this.session.getChunkRange(
          resolvedPath,
          parsed.coords,
          range,
          {
            signal: opts?.signal,
            ...(this.fetchClient && { fetchClient: this.fetchClient }),
          },
        );
        return data ?? undefined;
      }

      // For metadata keys, fetch full data and slice
      const data = this.session.getRawMetadata(resolvedPath);
      if (!data) return undefined;

      if ("suffixLength" in range) {
        return data.slice(-range.suffixLength);
      }
      return data.slice(range.offset, range.offset + range.length);
    } catch {
      return undefined;
    }
  }

  /** Prepend basePath to a parsed path. */
  private resolvePath(path: string): string {
    if (!this.basePath) return path;
    // path is "/" for root or "/group/array" for nested
    if (path === "/") return `/${this.basePath}`;
    return `/${this.basePath}${path}`;
  }

  /**
   * Create a store scoped to a subpath.
   *
   * The returned store shares the same session (and manifest cache)
   * but prepends `path` to all key lookups. This matches zarrita's
   * `root(store).resolve(path)` pattern.
   *
   * @param path - Subpath to scope to (e.g., "group/array")
   * @returns A new IcechunkStore scoped to the subpath
   */
  resolve(path: string): IcechunkStore {
    const scoped = new IcechunkStore(this.session, this.fetchClient);
    const cleanPath = path.replace(/^\/+|\/+$/g, "");
    scoped.basePath = this.basePath
      ? `${this.basePath}/${cleanPath}`
      : cleanPath;
    return scoped;
  }

  /**
   * List direct children of a group by name.
   *
   * @param parentPath - Path to the parent group (use "/" for root).
   *                     When omitted, uses the store's base path (or root).
   * @returns Array of child names (e.g., ["temperature", "precipitation"])
   */
  listChildren(parentPath?: string): string[] {
    let path: string;
    if (parentPath == null) {
      path = this.basePath ? `/${this.basePath}` : "/";
    } else if (this.basePath) {
      path = `/${this.basePath}/${parentPath.replace(/^\//, "")}`.replace(
        /\/+/g,
        "/",
      );
    } else {
      // Normalize: ensure leading slash for session API
      path = parentPath.startsWith("/") ? parentPath : `/${parentPath}`;
    }
    const nodes = this.session.listChildren(path);
    return nodes.map((node) => {
      // Extract the last path segment as the child name
      const segments = node.path.split("/");
      return segments[segments.length - 1];
    });
  }

  /**
   * List all nodes in the snapshot.
   *
   * @returns Array of all nodes
   */
  listNodes(): NodeSnapshot[] {
    return this.session.listNodes();
  }

  /**
   * Get a node by path.
   *
   * @param path - Absolute path (e.g., "/array" or "/group/nested")
   * @returns NodeSnapshot or null if not found
   */
  getNode(path: string): NodeSnapshot | null {
    const fullPath = (
      this.basePath
        ? `/${this.basePath}/${path.replace(/^\//, "")}`.replace(/\/+/g, "/")
        : path
    ).replace(/\/+$/, "") || "/";
    return this.session.getNode(fullPath);
  }

  /**
   * Get parsed Zarr metadata for a node.
   *
   * @param path - Path to the node
   * @returns Parsed JSON metadata or null if node not found
   */
  getMetadata(path: string): unknown | null {
    const fullPath = (
      this.basePath
        ? `/${this.basePath}/${path.replace(/^\//, "")}`.replace(/\/+/g, "/")
        : path
    ).replace(/\/+$/, "") || "/";
    return this.session.getMetadata(fullPath);
  }
}

/** Parsed zarr key */
type ParsedKey =
  | { type: "metadata"; path: string }
  | { type: "chunk"; path: string; coords: number[] };

/**
 * Parse a zarr key into its components.
 *
 * Keys follow the pattern:
 * - "/zarr.json" → root metadata
 * - "/path/to/node/zarr.json" → node metadata
 * - "/path/to/array/c/0/1/2" → chunk at coords [0, 1, 2]
 */
function parseZarrKey(key: AbsolutePath): ParsedKey {
  // Remove leading slash for easier parsing
  const path = key.slice(1);

  // Root metadata
  if (path === "zarr.json") {
    return { type: "metadata", path: "/" };
  }

  // Node metadata
  if (path.endsWith("/zarr.json")) {
    const nodePath = "/" + path.slice(0, -"/zarr.json".length);
    return { type: "metadata", path: nodePath };
  }

  // Chunk key: "path/to/array/c/0/1/2" or "c/0/1/2" for root arrays
  const chunkMatch = path.match(/^(?:(.*)\/)?c(?:\/(.*))?$/);
  if (chunkMatch) {
    const arrayPath = chunkMatch[1] ? "/" + chunkMatch[1] : "/";
    const coordsStr = chunkMatch[2] ?? "";
    const coords = coordsStr ? coordsStr.split("/").map(Number) : [];
    return { type: "chunk", path: arrayPath, coords };
  }

  // Default to metadata
  return { type: "metadata", path: "/" + path };
}
