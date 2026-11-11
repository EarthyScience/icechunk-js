/**
 * Range coalescing for chunk payload reads.
 *
 * Two layers live here:
 *
 * 1. Adapters that expose icechunk's two backing-object cases as a minimal
 *    range-readable store keyed by object path:
 *    - `makeUrlStore` fetches ranges from one external virtual-chunk URL.
 *    - `makeStorageStore` fetches ranges from repository storage objects.
 *
 * 2. `createRangeCoalescer`, the built-in coalescer. It is a
 *    `RangeCoalescingFn` — the same shape as zarrita's `withRangeCoalescing`
 *    (zarrita >= 0.7) — so it plugs into the same seam in `ReadSession`.
 *
 * Why this seam: `ReadSession` already wraps one `AsyncReadable` per backing
 * object (virtual URL or native chunk object) and partitions those stores by
 * fetch client and conditional headers, so reads that must not share a request
 * never meet in one queue. Coalescing as a wrapper around that store's
 * `getRange` touches neither `Storage` nor `FetchClient`, leaves the session's
 * fetch path unchanged, and lets a caller swap in zarrita's wrapper (or none).
 *
 * The built-in coalescer batches `getRange` calls that arrive in the same
 * window (default: the current microtask turn), sorts them by offset, dedupes
 * identical ranges, and merges neighbours whose gap is at most `maxGap` into
 * one request of at most `maxBytes`. Gap bytes are fetched and discarded. A
 * failed merged request is retried one member at a time so one bad range
 * cannot poison a batch. A waiter's abort rejects only that waiter; the merged
 * request itself is aborted once every waiter has aborted.
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
        // Free the discarded redirect body so Node can reuse the connection.
        response.body?.cancel().catch(() => {});
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

// ---------------------------------------------------------------------------
// Built-in coalescer
// ---------------------------------------------------------------------------

/** Default largest gap bridged between two reads: 1 MiB. */
const DEFAULT_MAX_GAP = 1 << 20;
/** Default largest merged request: 16 MiB. */
const DEFAULT_MAX_BYTES = 16 << 20;

/** Options for the built-in range coalescer. */
export interface RangeCoalesceOptions {
  /**
   * Largest byte gap between two reads that is bridged by one request
   * (default 1 MiB). Gap bytes are fetched and discarded, so this trades
   * bandwidth for request count.
   */
  maxGap?: number;
  /**
   * Largest merged request in bytes (default 16 MiB). A single read larger
   * than this is still issued whole; it just never merges with a neighbour.
   */
  maxBytes?: number;
  /**
   * Batching window in milliseconds. `0` (default) flushes on the next
   * microtask, so reads issued in the same synchronous turn — e.g. all chunks
   * of one `zarr.get` — batch together with no added latency. A positive
   * value flushes on a timer instead and also catches reads that trickle in
   * across turns, at the cost of that much delay per batch.
   */
  window?: number;
}

/** Counters kept by the built-in range coalescer. Cheap; read them any time. */
export interface RangeCoalesceStats {
  /** Range reads received from callers (suffix reads excluded). */
  ranges: number;
  /** Range requests issued to the wrapped store. */
  requests: number;
  /** Bytes callers asked for, duplicates included. */
  bytesRequested: number;
  /** Bytes requested from the wrapped store. */
  bytesFetched: number;
  /** Fetched bytes that no caller asked for (gap bytes). */
  bytesDiscarded: number;
  /** Merged requests that failed and were retried one member at a time. */
  fallbacks: number;
}

/** A `RangeCoalescingFn` with shared counters. Built by `createRangeCoalescer`. */
export interface RangeCoalescer extends RangeCoalescingFn {
  readonly stats: RangeCoalesceStats;
  resetStats(): void;
}

/** True when `fn` is a built-in coalescer (carries `stats`). */
export function isRangeCoalescer(
  fn: RangeCoalescingFn | undefined,
): fn is RangeCoalescer {
  return typeof fn === "function" && "stats" in fn;
}

interface Waiter {
  signal: AbortSignal | undefined;
  settled: boolean;
  resolve(data: Uint8Array | undefined): void;
  reject(error: unknown): void;
}

interface PendingRead {
  offset: number;
  length: number;
  waiter: Waiter;
}

/** One distinct (offset, length) range and everyone waiting for it. */
interface Member {
  offset: number;
  length: number;
  waiters: Waiter[];
}

/** One request to issue: the merged span and the members it covers. */
interface Run {
  offset: number;
  end: number;
  members: Member[];
  /** Bytes of the span that at least one member asked for. */
  delivered: number;
}

interface ResolvedOptions {
  maxGap: number;
  maxBytes: number;
  window: number;
}

type GetRangeFn = (
  key: string,
  range: RangeQuery,
  options?: GetOptions,
) => Promise<Uint8Array | undefined>;

function freshStats(): RangeCoalesceStats {
  return {
    ranges: 0,
    requests: 0,
    bytesRequested: 0,
    bytesFetched: 0,
    bytesDiscarded: 0,
    fallbacks: 0,
  };
}

function nonNegative(name: string, value: number): number {
  if (!Number.isFinite(value) || value < 0) {
    throw new RangeError(`coalesce.${name} must be a non-negative number`);
  }
  return value;
}

function abortReason(signal: AbortSignal): unknown {
  return (
    signal.reason ??
    new DOMException("The operation was aborted.", "AbortError")
  );
}

/**
 * Create the built-in range coalescer.
 *
 * The result wraps one `AsyncReadable` at a time (`ReadSession` calls it once
 * per backing object) and all wrapped stores share the returned `stats`.
 * The `coalesceSize` hint the session passes when wrapping is zarrita's
 * knob; the built-in coalescer uses its own `maxGap` instead.
 */
export function createRangeCoalescer(
  options: RangeCoalesceOptions = {},
): RangeCoalescer {
  const resolved: ResolvedOptions = {
    maxGap: nonNegative("maxGap", options.maxGap ?? DEFAULT_MAX_GAP),
    maxBytes: nonNegative("maxBytes", options.maxBytes ?? DEFAULT_MAX_BYTES),
    window: nonNegative("window", options.window ?? 0),
  };
  const stats = freshStats();
  const wrap = (store: AsyncReadable): AsyncReadable =>
    wrapStore(store, resolved, stats);
  return Object.assign(wrap, {
    stats,
    resetStats() {
      Object.assign(stats, freshStats());
    },
  });
}

function wrapStore(
  store: AsyncReadable,
  options: ResolvedOptions,
  stats: RangeCoalesceStats,
): AsyncReadable {
  if (typeof store.getRange !== "function") {
    throw new Error("Range coalescing requires a store with getRange");
  }
  const inner: GetRangeFn = store.getRange.bind(store);
  const pending = new Map<string, PendingRead[]>();
  let scheduled = false;

  function flush(): void {
    scheduled = false;
    const work = [...pending];
    pending.clear();
    for (const [key, reads] of work) {
      for (const run of planRuns(reads, options)) {
        const length = run.end - run.offset;
        stats.bytesDiscarded += length - run.delivered;
        void issue(inner, stats, key, run.offset, length, run.members);
      }
    }
  }

  function schedule(): void {
    if (scheduled) return;
    scheduled = true;
    if (options.window > 0) setTimeout(flush, options.window);
    else queueMicrotask(flush);
  }

  return {
    get: (key, getOptions) => store.get(key, getOptions),
    getRange(key, range, getOptions) {
      // Suffix reads (shard index fetches) need the object size to place;
      // pass them through untouched.
      if ("suffixLength" in range) return inner(key, range, getOptions);
      const signal = getOptions?.signal;
      if (signal?.aborted) return Promise.reject(abortReason(signal));

      stats.ranges++;
      stats.bytesRequested += range.length;
      return new Promise<Uint8Array | undefined>((resolve, reject) => {
        let reads = pending.get(key);
        if (!reads) {
          reads = [];
          pending.set(key, reads);
        }
        reads.push({
          offset: range.offset,
          length: range.length,
          waiter: { signal, settled: false, resolve, reject },
        });
        schedule();
      });
    },
  };
}

/**
 * Sort one key's reads by offset, fold identical ranges into one member, and
 * chain members into runs: a member joins the current run when it starts
 * within `maxGap` of the run's end (overlaps included) and the run stays
 * within `maxBytes`.
 */
function planRuns(reads: PendingRead[], options: ResolvedOptions): Run[] {
  reads.sort((a, b) => a.offset - b.offset || a.length - b.length);

  const members: Member[] = [];
  for (const read of reads) {
    const last = members[members.length - 1];
    if (last && last.offset === read.offset && last.length === read.length) {
      last.waiters.push(read.waiter);
    } else {
      members.push({
        offset: read.offset,
        length: read.length,
        waiters: [read.waiter],
      });
    }
  }

  const runs: Run[] = [];
  let run: Run | undefined;
  for (const member of members) {
    const memberEnd = member.offset + member.length;
    if (
      run &&
      member.offset - run.end <= options.maxGap &&
      Math.max(run.end, memberEnd) - run.offset <= options.maxBytes
    ) {
      run.delivered += Math.max(
        0,
        memberEnd - Math.max(member.offset, run.end),
      );
      run.end = Math.max(run.end, memberEnd);
      run.members.push(member);
    } else {
      run = {
        offset: member.offset,
        end: memberEnd,
        members: [member],
        delivered: member.length,
      };
      runs.push(run);
    }
  }
  return runs;
}

/**
 * Issue one request covering `members` and settle their waiters. Never
 * throws: every outcome lands on the waiters.
 *
 * A single-member run is passed through exactly as an uncoalesced read
 * would be (same range, same signal, same bytes back), so disabling
 * coalescing and a batch of one behave identically.
 */
async function issue(
  inner: GetRangeFn,
  stats: RangeCoalesceStats,
  key: string,
  offset: number,
  length: number,
  members: Member[],
): Promise<void> {
  const waiters: Waiter[] = [];
  for (const member of members) {
    for (const waiter of member.waiters) {
      if (!waiter.settled) waiters.push(waiter);
    }
  }
  if (waiters.length === 0) return;

  // Abort handling. One waiter: its own signal goes straight through. Several:
  // each waiter's abort rejects that waiter alone, and the shared request is
  // aborted only when no waiter is left listening.
  let signal: AbortSignal | undefined;
  let controller: AbortController | undefined;
  const cleanups: Array<() => void> = [];
  if (waiters.length === 1) {
    signal = waiters[0].signal;
  } else if (waiters.some((waiter) => waiter.signal)) {
    controller = new AbortController();
    signal = controller.signal;
    let live = waiters.length;
    for (const waiter of waiters) {
      const waiterSignal = waiter.signal;
      if (!waiterSignal) continue;
      const onAbort = () => {
        if (waiter.settled) return;
        waiter.settled = true;
        waiter.reject(abortReason(waiterSignal));
        if (--live === 0) controller!.abort(waiterSignal.reason);
      };
      waiterSignal.addEventListener("abort", onAbort, { once: true });
      cleanups.push(() => waiterSignal.removeEventListener("abort", onAbort));
    }
  }

  stats.requests++;
  stats.bytesFetched += length;

  let data: Uint8Array | undefined;
  try {
    data = await inner(
      key,
      { offset, length },
      signal ? { signal } : undefined,
    );
    if (members.length > 1 && data && data.length < length) {
      throw new Error(
        `Short read for ${key} at ${offset}: expected ${length} bytes, got ${data.length}`,
      );
    }
  } catch (error) {
    for (const cleanup of cleanups) cleanup();
    if (members.length > 1 && !controller?.signal.aborted) {
      // One failed merged request must not fail every chunk in it: retry
      // each member on its own so only the genuinely bad range rejects.
      stats.fallbacks++;
      await Promise.all(
        members.map((member) =>
          issue(inner, stats, key, member.offset, member.length, [member]),
        ),
      );
      return;
    }
    for (const waiter of waiters) {
      if (waiter.settled) continue;
      waiter.settled = true;
      waiter.reject(error);
    }
    return;
  }
  for (const cleanup of cleanups) cleanup();

  for (const member of members) {
    const start = member.offset - offset;
    let view: Uint8Array | undefined;
    for (const waiter of member.waiters) {
      if (waiter.settled) continue;
      waiter.settled = true;
      if (!data) {
        waiter.resolve(undefined);
        continue;
      }
      if (!view) {
        // Pass a lone read's bytes through untouched; slice (copy) members
        // out of a merged buffer so no caller retains the whole run.
        view =
          members.length === 1
            ? data
            : data.slice(start, start + member.length);
        waiter.resolve(view);
      } else {
        // Duplicate ranges share one fetch but each caller gets its own copy.
        waiter.resolve(view.slice());
      }
    }
  }
}
