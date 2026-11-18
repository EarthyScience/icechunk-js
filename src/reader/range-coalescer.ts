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
import { NotFoundError, parseContentRange } from "../storage/storage.js";

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

function describeRange(range: RangeQuery): string {
  return "suffixLength" in range
    ? `last ${range.suffixLength} bytes`
    : `bytes ${range.offset}-${range.offset + range.length - 1}`;
}

/** An HTTP failure carrying its status, so callers can tell whole-object failures apart. */
function httpError(message: string, status: number): Error {
  return Object.assign(new Error(message), { status });
}

/**
 * Reject a 206 whose `Content-Range` is not the span that was requested.
 * Coalescers slice the body by offset, so one misplaced body would shift
 * every chunk cut from it. Absent or unparsable headers are let through;
 * the body-length check still applies.
 */
function assertContentRangeMatches(
  url: string,
  range: RangeQuery,
  response: Response,
): void {
  const got = parseContentRange(response);
  if (!got) return;
  const matches =
    "suffixLength" in range
      ? got.end - got.start + 1 === range.suffixLength &&
        (got.size === null || got.end === got.size - 1)
      : got.start === range.offset &&
        got.end === range.offset + range.length - 1;
  if (!matches) {
    throw new Error(
      `Range response for ${url} covers bytes ${got.start}-${got.end}, not the requested ${describeRange(range)}`,
    );
  }
}

/** Copy `[start, end)` out of `data` into a buffer of its own. `slice` would hand back a view on a Node `Buffer`. */
function copyBytes(data: Uint8Array, start: number, end: number): Uint8Array {
  return new Uint8Array(data.subarray(start, end));
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
        throw httpError(
          `Virtual chunk at ${requestUrl} failed integrity check — data has been modified since snapshot was created`,
          412,
        );
      }
      if (response.status !== 200 && response.status !== 206) {
        throw httpError(
          `Failed to fetch virtual chunk from ${requestUrl}: ${response.status} ${response.statusText}`,
          response.status,
        );
      }

      const data = new Uint8Array(await response.arrayBuffer());

      // 206 (Partial Content) is the happy path only when the response body
      // is exactly the requested range. Coalescers slice from this buffer by
      // offset, so accepting an overlong partial response can shift data.
      if (response.status === 206) {
        assertContentRangeMatches(requestUrl, range, response);
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

      // A `Storage` that ignores ranges returns the whole object (an HTTP
      // 200); copy out the requested window. `HttpStorage` already slices
      // 200s and rejects a 206 whose Content-Range is off, so a longer body
      // from a custom `Storage` is taken to be the whole object — a partial
      // body of the wrong span is indistinguishable here and is that
      // storage's bug to catch.
      if (data.length >= storageRange.end) {
        return copyBytes(data, storageRange.start, storageRange.end);
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

/** Default largest gap bridged between two reads: 256 KiB. */
const DEFAULT_MAX_GAP = 256 * 1024;
/** Default largest merged request: 16 MiB. */
const DEFAULT_MAX_BYTES = 16 << 20;
/** Default bound on gap bytes relative to delivered bytes per request. */
const DEFAULT_MAX_WASTE_RATIO = 1;
/** Marks functions built by `createRangeCoalescer`. */
const COALESCER_BRAND: unique symbol = Symbol("icechunk-js.rangeCoalescer");
/**
 * HTTP statuses that condemn the whole object, not just one merged request:
 * retrying its members one by one could only repeat the same answer.
 */
const WHOLE_OBJECT_STATUSES = new Set([401, 403, 404, 412]);

/** Options for the built-in range coalescer. */
export interface RangeCoalesceOptions {
  /**
   * Largest byte gap between two reads that is bridged by one request
   * (default 256 KiB). Gap bytes are fetched and discarded, so this trades
   * bandwidth for request count; `maxWasteRatio` bounds the total.
   */
  maxGap?: number;
  /**
   * Largest merged request in bytes (default 16 MiB). A single read larger
   * than this is still issued whole; it just never merges with a neighbour.
   */
  maxBytes?: number;
  /**
   * Largest ratio of gap bytes to delivered bytes one merged request may
   * reach (default 1): a gap is bridged only while the request's discarded
   * bytes stay at or below the bytes its callers asked for. `Infinity`
   * leaves `maxGap` as the only bound; `0` merges adjacent reads only.
   */
  maxWasteRatio?: number;
  /**
   * Batching window in milliseconds. `0` (default) flushes on the next
   * microtask, so reads issued in the same microtask turn batch together
   * with no added latency. A positive value flushes on a `setTimeout` timer
   * instead and also catches reads that trickle in across turns, at the cost
   * of that much delay per batch — and browsers throttle timers in
   * background tabs.
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

/** True when `fn` was built by `createRangeCoalescer`. */
export function isRangeCoalescer(
  fn: RangeCoalescingFn | undefined,
): fn is RangeCoalescer {
  return (
    typeof fn === "function" &&
    (fn as { [COALESCER_BRAND]?: boolean })[COALESCER_BRAND] === true
  );
}

interface Waiter {
  signal: AbortSignal | undefined;
  settled: boolean;
  /** Removes the abort listener registered when the read was queued. */
  detach?: () => void;
  /** Set by `issue` while a merged request is in flight; runs after this waiter aborts. */
  onAbort?: () => void;
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
  maxWasteRatio: number;
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

function nonNegative(
  name: string,
  value: number,
  allowInfinite = false,
): number {
  const valid = allowInfinite
    ? !Number.isNaN(value) && value >= 0
    : Number.isFinite(value) && value >= 0;
  if (!valid) {
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

/** Settle a waiter exactly once and drop its abort listener. */
function settle(waiter: Waiter, outcome: () => void): void {
  if (waiter.settled) return;
  waiter.settled = true;
  waiter.detach?.();
  waiter.detach = undefined;
  waiter.onAbort = undefined;
  outcome();
}

/** True when `error` condemns the whole object (404, 403, 401, 412). */
function failsWholeObject(error: unknown): boolean {
  if (error instanceof NotFoundError) return true;
  const status = (error as { status?: unknown } | null)?.status;
  return typeof status === "number" && WHOLE_OBJECT_STATUSES.has(status);
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
    maxWasteRatio: nonNegative(
      "maxWasteRatio",
      options.maxWasteRatio ?? DEFAULT_MAX_WASTE_RATIO,
      true,
    ),
    window: nonNegative("window", options.window ?? 0),
  };
  const stats = freshStats();
  const wrap = (store: AsyncReadable): AsyncReadable =>
    wrapStore(store, resolved, stats);
  return Object.assign(wrap, {
    [COALESCER_BRAND]: true as const,
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
      // Reads aborted while queued are already rejected; keep them out of
      // the plan so they neither widen a run nor count as waiters.
      const live = reads.filter((read) => !read.waiter.settled);
      for (const run of planRuns(live, options)) {
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
        const waiter: Waiter = { signal, settled: false, resolve, reject };
        if (signal) {
          // Listen from the moment the read is queued: an abort during the
          // window must reject it (an already-aborted signal never fires).
          const onAbort = () => {
            const hook = waiter.onAbort;
            settle(waiter, () => waiter.reject(abortReason(signal)));
            hook?.();
          };
          signal.addEventListener("abort", onAbort, { once: true });
          waiter.detach = () => signal.removeEventListener("abort", onAbort);
        }
        let reads = pending.get(key);
        if (!reads) {
          reads = [];
          pending.set(key, reads);
        }
        reads.push({ offset: range.offset, length: range.length, waiter });
        schedule();
      });
    },
  };
}

/**
 * Sort one key's reads by offset, fold identical ranges into one member, and
 * chain members into runs: a member joins the current run when it starts
 * within `maxGap` of the run's end (overlaps included), the run stays within
 * `maxBytes`, and the run's gap bytes stay within `maxWasteRatio` times the
 * bytes it delivers.
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
    if (run) {
      const added = Math.max(0, memberEnd - Math.max(member.offset, run.end));
      const end = Math.max(run.end, memberEnd);
      const length = end - run.offset;
      const delivered = run.delivered + added;
      if (
        member.offset - run.end <= options.maxGap &&
        length <= options.maxBytes &&
        length - delivered <= options.maxWasteRatio * delivered
      ) {
        run.end = end;
        run.delivered = delivered;
        run.members.push(member);
        continue;
      }
    }
    run = {
      offset: member.offset,
      end: memberEnd,
      members: [member],
      delivered: member.length,
    };
    runs.push(run);
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
  // each waiter's abort rejects that waiter alone (the listener registered at
  // queue time), and the shared request is aborted only when no waiter is
  // left listening. Waiters without a signal keep it alive.
  let signal: AbortSignal | undefined;
  let controller: AbortController | undefined;
  if (waiters.length === 1) {
    signal = waiters[0].signal;
  } else if (waiters.some((waiter) => waiter.signal)) {
    const shared = new AbortController();
    controller = shared;
    signal = shared.signal;
    let live = waiters.length;
    for (const waiter of waiters) {
      const waiterSignal = waiter.signal;
      if (!waiterSignal) continue;
      waiter.onAbort = () => {
        if (--live === 0) shared.abort(waiterSignal.reason);
      };
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
    if (
      members.length > 1 &&
      !controller?.signal.aborted &&
      !failsWholeObject(error)
    ) {
      // One failed merged request must not fail every chunk in it: retry
      // each member on its own so only the genuinely bad range rejects.
      stats.fallbacks++;
      for (const waiter of waiters) waiter.onAbort = undefined;
      await Promise.all(
        members.map((member) =>
          issue(inner, stats, key, member.offset, member.length, [member]),
        ),
      );
      return;
    }
    for (const waiter of waiters) settle(waiter, () => waiter.reject(error));
    return;
  }

  for (const member of members) {
    const start = member.offset - offset;
    let view: Uint8Array | undefined;
    for (const waiter of member.waiters) {
      if (waiter.settled) continue;
      if (!data) {
        settle(waiter, () => waiter.resolve(undefined));
        continue;
      }
      if (!view) {
        // Pass a lone read's bytes through untouched; copy members out of a
        // merged buffer so no caller retains (or shares) the whole run.
        view =
          members.length === 1
            ? data
            : copyBytes(data, start, start + member.length);
        const first = view;
        settle(waiter, () => waiter.resolve(first));
      } else {
        // Duplicate ranges share one fetch but each caller gets its own copy.
        const copy = new Uint8Array(view);
        settle(waiter, () => waiter.resolve(copy));
      }
    }
  }
}
