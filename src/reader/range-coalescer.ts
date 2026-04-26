/**
 * Adapters for zarrita's `withRangeCoalescing` (added in zarrita 0.7).
 *
 * The coalescer works over any range-readable store keyed by object path.
 * These adapters expose icechunk's two backing-object cases in that shape:
 *
 * - `makeUrlStore` fetches ranges from one external virtual-chunk URL.
 * - `makeStorageStore` fetches ranges from repository storage objects.
 *
 * Callers pass `zarrita.withRangeCoalescing` into icechunk-js explicitly when
 * they want coalescing, keeping zarrita a true optional dependency.
 */

import type { FetchClient, Storage } from "../storage/storage.js";

// Minimal structural mirror of zarrita's `AsyncReadable`. Kept local so
// this module has no required import from zarrita — the peer dep is
// optional and may not resolve at build time for some consumers.
type RangeQuery = { offset: number; length: number } | { suffixLength: number };
interface GetOptions {
  signal?: AbortSignal;
}
export interface AsyncReadable {
  get(key: string, options?: GetOptions): Promise<Uint8Array | undefined>;
  getRange(
    key: string,
    range: RangeQuery,
    options?: GetOptions,
  ): Promise<Uint8Array | undefined>;
}

export type RangeCoalescingFn = (
  store: AsyncReadable,
  opts?: { coalesceSize?: number },
) => AsyncReadable;

export interface MakeUrlStoreOptions {
  /** Absolute HTTP URL this store always fetches. */
  url: string;
  /** Pluggable HTTP client; defaults to `globalThis.fetch`. */
  fetchClient?: FetchClient;
  /**
   * Conditional request headers (`If-Match`, `If-Unmodified-Since`) baked
   * into every fetch. Used to carry `validateChecksums` semantics through
   * the coalesced path — all payloads sharing this store are assumed to
   * share the same checksum, so `ReadSession.getVirtualStoreForPayload`
   * partitions stores by checksum to avoid mixing conditional headers.
   *
   * Kept opt-in because these headers trigger CORS preflight requests in
   * browsers, and most storage servers don't whitelist them by default.
   */
  conditionalHeaders?: Record<string, string>;
}

function expectedRangeLength(range: RangeQuery): number {
  return "suffixLength" in range ? range.suffixLength : range.length;
}

/**
 * Build a minimal `AsyncReadable` that services every `getRange` by
 * fetching the configured URL with the requested byte range. The zarr
 * key is ignored — when wrapped by `withRangeCoalescing`, all requests
 * converge on the same path and become eligible for range-merging.
 */
export function makeUrlStore(opts: MakeUrlStoreOptions): AsyncReadable {
  const { url, fetchClient, conditionalHeaders } = opts;

  async function doFetch(init: RequestInit): Promise<Response> {
    return fetchClient
      ? await fetchClient.fetch(url, init)
      : await fetch(url, init);
  }

  return {
    async get() {
      throw new Error(
        `Virtual chunk URL store for ${url} only supports ranged reads`,
      );
    },
    async getRange(_key, range, options) {
      const headers: Record<string, string> = conditionalHeaders
        ? { ...conditionalHeaders }
        : {};
      headers.Range =
        "suffixLength" in range
          ? `bytes=-${range.suffixLength}`
          : `bytes=${range.offset}-${range.offset + range.length - 1}`;

      const response = await doFetch({ headers, signal: options?.signal });

      if (response.status === 412) {
        throw new Error(
          `Virtual chunk at ${url} failed integrity check — data has been modified since snapshot was created`,
        );
      }
      if (response.status !== 200 && response.status !== 206) {
        throw new Error(
          `Failed to fetch virtual chunk from ${url}: ${response.status} ${response.statusText}`,
        );
      }

      const data = new Uint8Array(await response.arrayBuffer());

      // 206 (Partial Content) is the happy path only when the response body
      // is exactly the requested range. Coalescers slice from this buffer by
      // offset, so accepting an overlong partial response can shift data.
      if (response.status === 206) {
        const expected = expectedRangeLength(range);
        if (data.length === expected) return data;
        throw new Error(
          `Virtual range response size mismatch for ${url}: expected ${expected} bytes, got ${data.length}`,
        );
      }

      // 200 means the server ignored the Range header and sent the full
      // object. Slice out the requested window so callers don't have to
      // know the distinction.
      if ("offset" in range) {
        const end = range.offset + range.length;
        if (data.length >= end) return data.slice(range.offset, end);
        throw new Error(
          `Virtual range request not honored for ${url}: need at least ${end} bytes for fallback slicing, got ${data.length}`,
        );
      }
      // Suffix-length on a 200 fallback: take the trailing suffixLength bytes.
      if (data.length >= range.suffixLength) {
        return data.slice(data.length - range.suffixLength);
      }
      throw new Error(
        `Virtual suffix range request not honored for ${url}: need at least ${range.suffixLength} bytes for fallback slicing, got ${data.length}`,
      );
    },
  };
}

/**
 * Build a minimal `AsyncReadable` over icechunk repository storage objects.
 *
 * This lets native chunk payloads use the same zarrita range-coalescing
 * wrapper as virtual chunks. The key remains the repository object path, so
 * zarrita only merges ranges that target the same chunk object.
 */
export function makeStorageStore(storage: Storage): AsyncReadable {
  return {
    async get(key, options) {
      const storageOptions = options?.signal
        ? { signal: options.signal }
        : undefined;
      return storage.getObject(key, undefined, storageOptions);
    },
    async getRange(key, range, options) {
      const storageOptions = options?.signal
        ? { signal: options.signal }
        : undefined;
      if ("suffixLength" in range) {
        throw new Error(
          `Storage suffix ranges are not supported for ${key}; convert suffixLength to offset/length before reading`,
        );
      }

      const storageRange = {
        start: range.offset,
        end: range.offset + range.length,
      };
      const data = await storage.getObject(key, storageRange, storageOptions);
      if (data.length === range.length) return data;

      // Range header may be ignored (e.g. HTTP 200 full body). If the full
      // object is available, slice out the requested window explicitly.
      if (data.length >= storageRange.end) {
        return data.slice(storageRange.start, storageRange.end);
      }

      throw new Error(
        `Storage returned ${data.length} bytes for ${key} range ${storageRange.start}-${storageRange.end - 1}; expected ${range.length} bytes`,
      );
    },
  };
}
