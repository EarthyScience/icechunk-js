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
 * Map an S3 path-style global-endpoint region redirect to the regional URL to
 * retry. For buckets outside `us-east-1`, `s3.amazonaws.com` answers 301 with
 * no `Location` header (so `fetch` can't follow it) and reports the region in
 * `x-amz-bucket-region`; this rebuilds the URL against `s3.<region>.amazonaws.com`.
 * Returns null when the response isn't that redirect.
 */
function regionalS3RedirectUrl(url: string, response: Response): string | null {
  if (response.status !== 301 && response.status !== 307) return null;
  const region = response.headers.get("x-amz-bucket-region");
  if (!region) return null;
  const globalPrefix = "https://s3.amazonaws.com/";
  if (!url.startsWith(globalPrefix)) return null;
  return `https://s3.${region}.amazonaws.com/${url.slice(globalPrefix.length)}`;
}

/**
 * Build a minimal `AsyncReadable` that services every `getRange` by
 * fetching the configured URL with the requested byte range. The zarr
 * key is ignored — when wrapped by `withRangeCoalescing`, all requests
 * converge on the same path and become eligible for range-merging.
 */
export function makeUrlStore(opts: MakeUrlStoreOptions): AsyncReadable {
  const { fetchClient, conditionalHeaders } = opts;
  // Hint shared across reads: once one read resolves an S3 regional redirect,
  // later reads start from the regional host. Each read works off its own
  // `requestUrl` copy, so this value is an optimization, not a source of truth.
  let pinnedUrl = opts.url;

  async function doFetch(target: string, init: RequestInit): Promise<Response> {
    return fetchClient
      ? await fetchClient.fetch(target, init)
      : await fetch(target, init);
  }

  return {
    async get() {
      throw new Error(
        `Virtual chunk URL store for ${pinnedUrl} only supports ranged reads`,
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

      // Copy the endpoint locally so a concurrent read updating the shared pin
      // can't change which URL this request detects and retries against.
      let requestUrl = pinnedUrl;
      let response = await doFetch(requestUrl, {
        headers,
        signal: options?.signal,
      });

      // Resolve a global-endpoint region redirect and retry once on the
      // regional host, re-pinning it for later reads.
      const regionalUrl = regionalS3RedirectUrl(requestUrl, response);
      if (regionalUrl) {
        requestUrl = regionalUrl;
        pinnedUrl = regionalUrl;
        response = await doFetch(requestUrl, {
          headers,
          signal: options?.signal,
        });
      }

      if (response.status === 412) {
        throw new Error(
          `Virtual chunk at ${requestUrl} failed integrity check — data has been modified since snapshot was created`,
        );
      }
      if (response.status !== 200 && response.status !== 206) {
        throw new Error(
          `Failed to fetch virtual chunk from ${requestUrl}: ${response.status} ${response.statusText}`,
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
          `Virtual range response size mismatch for ${requestUrl}: expected ${expected} bytes, got ${data.length}`,
        );
      }

      // 200 means the server ignored the Range header and sent the full
      // object. Slice out the requested window so callers don't have to
      // know the distinction.
      if ("offset" in range) {
        const end = range.offset + range.length;
        if (data.length >= end) return data.slice(range.offset, end);
        throw new Error(
          `Virtual range request not honored for ${requestUrl}: need at least ${end} bytes for fallback slicing, got ${data.length}`,
        );
      }
      // Suffix-length on a 200 fallback: take the trailing suffixLength bytes.
      if (data.length >= range.suffixLength) {
        return data.slice(data.length - range.suffixLength);
      }
      throw new Error(
        `Virtual suffix range request not honored for ${requestUrl}: need at least ${range.suffixLength} bytes for fallback slicing, got ${data.length}`,
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
