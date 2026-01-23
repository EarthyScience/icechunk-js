/**
 * IcechunkStore - zarrita-compatible store adapter.
 *
 * Implements zarrita's AsyncReadable interface to allow using
 * zarrita for array operations on icechunk repositories.
 */

import { Repository } from './reader/repository.js';
import { ReadSession } from './reader/session.js';
import { HttpStorage } from './storage/http-storage.js';
import type { Storage, TransformRequest } from './storage/storage.js';

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
    opts?: Options
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
  formatVersion?: 'v1' | 'v2';

  /**
   * Callback to transform virtual chunk URLs before fetching.
   *
   * Use this to:
   * - Generate pre-signed S3 URLs
   * - Add authentication headers
   * - Route through a proxy
   */
  transformRequest?: TransformRequest;
}

/**
 * IcechunkStore - zarrita-compatible store for icechunk repositories.
 *
 * This store implements zarrita's AsyncReadable interface, allowing you
 * to use zarrita's array operations on icechunk data.
 *
 * @example
 * ```typescript
 * import { IcechunkStore } from 'icechunk-js';
 * import { open, get } from 'zarrita';
 *
 * const store = new IcechunkStore('https://bucket.s3.amazonaws.com/repo');
 * const array = await open(store, { kind: 'array', path: '/temperature' });
 * const data = await get(array, [0, 0, null]);
 * ```
 */
export class IcechunkStore implements AsyncReadable {
  private sessionPromise: Promise<ReadSession>;
  private session: ReadSession | null = null;
  private transformRequest?: TransformRequest;

  /**
   * Create an IcechunkStore from a URL.
   *
   * @param url - URL to the icechunk repository
   * @param options - Store options (branch, tag, or snapshot to checkout)
   */
  constructor(url: string, options?: IcechunkStoreOptions);

  /**
   * Create an IcechunkStore from an existing ReadSession.
   *
   * @param session - Existing ReadSession
   * @param options - Store options (only transformRequest is used)
   */
  constructor(session: ReadSession, options?: Pick<IcechunkStoreOptions, 'transformRequest'>);

  /**
   * Create an IcechunkStore from a custom Storage backend.
   *
   * @param storage - Custom Storage implementation
   * @param options - Store options (branch, tag, or snapshot to checkout)
   */
  constructor(storage: Storage, options?: IcechunkStoreOptions);

  constructor(
    arg: string | ReadSession | Storage,
    options: IcechunkStoreOptions = {}
  ) {
    this.transformRequest = options.transformRequest;

    if (arg instanceof ReadSession) {
      // Already have a session
      this.session = arg;
      this.sessionPromise = Promise.resolve(arg);
    } else {
      // Need to open repository and checkout
      this.sessionPromise = this.initSession(arg, options);
    }
  }

  private async initSession(
    arg: string | Storage,
    options: IcechunkStoreOptions
  ): Promise<ReadSession> {
    const storage = typeof arg === 'string' ? new HttpStorage(arg) : arg;
    const requestOptions = options.signal ? { signal: options.signal } : undefined;
    const repo = await Repository.open({ storage, formatVersion: options.formatVersion }, requestOptions);

    let session: ReadSession;
    if (options.snapshot) {
      session = await repo.checkoutSnapshot(options.snapshot, requestOptions);
    } else if (options.tag) {
      session = await repo.checkoutTag(options.tag, requestOptions);
    } else {
      session = await repo.checkoutBranch(options.branch ?? 'main', requestOptions);
    }

    this.session = session;
    return session;
  }

  private async getSession(): Promise<ReadSession> {
    if (this.session) return this.session;
    return this.sessionPromise;
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
    opts?: { signal?: AbortSignal }
  ): Promise<Uint8Array | undefined> {
    // Early abort check before any async work
    if (opts?.signal?.aborted) return undefined;

    const session = await this.getSession();
    const parsed = parseZarrKey(key);

    try {
      if (parsed.type === 'metadata') {
        // Metadata is synchronous - no signal needed
        const data = session.getRawMetadata(parsed.path);
        return data ?? undefined;
      }

      // Chunk key - propagate signal and transformRequest to session
      const chunk = await session.getChunk(parsed.path, parsed.coords, {
        signal: opts?.signal,
        transformRequest: this.transformRequest,
      });
      return chunk ?? undefined;
    } catch {
      // AbortError and other errors are handled by returning undefined
      return undefined;
    }
  }

  /**
   * Get partial data for a zarr key (optional optimization).
   *
   * @param key - Absolute path
   * @param range - Byte range to fetch
   * @param opts - Optional request options (supports AbortSignal for cancellation)
   * @returns Data bytes or undefined if not found
   */
  async getRange(
    key: AbsolutePath,
    range: RangeQuery,
    opts?: { signal?: AbortSignal }
  ): Promise<Uint8Array | undefined> {
    if (opts?.signal?.aborted) return undefined;

    // For now, fetch full data and slice
    // Could optimize later for large chunks
    const data = await this.get(key, opts);
    if (!data) return undefined;

    if ('suffixLength' in range) {
      return data.slice(-range.suffixLength);
    }

    return data.slice(range.offset, range.offset + range.length);
  }
}

/** Parsed zarr key */
type ParsedKey =
  | { type: 'metadata'; path: string }
  | { type: 'chunk'; path: string; coords: number[] };

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
  if (path === 'zarr.json') {
    return { type: 'metadata', path: '/' };
  }

  // Node metadata
  if (path.endsWith('/zarr.json')) {
    const nodePath = '/' + path.slice(0, -'/zarr.json'.length);
    return { type: 'metadata', path: nodePath };
  }

  // Chunk key: "path/to/array/c/0/1/2" or "c/0/1/2" for root arrays
  const chunkMatch = path.match(/^(?:(.*)\/)?c(?:\/(.*))?$/);
  if (chunkMatch) {
    const arrayPath = chunkMatch[1] ? '/' + chunkMatch[1] : '/';
    const coordsStr = chunkMatch[2] ?? '';
    const coords = coordsStr ? coordsStr.split('/').map(Number) : [];
    return { type: 'chunk', path: arrayPath, coords };
  }

  // Default to metadata
  return { type: 'metadata', path: '/' + path };
}
