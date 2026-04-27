/**
 * Single-flight wrapper for cache-aside fetches.
 *
 * Wraps a result cache with an in-flight Map so concurrent misses for the
 * same key share one fetch. The shared fetch is reference-counted against
 * its waiters: each caller's `signal` rejects only that caller's wait,
 * but if every waiter has aborted the underlying fetch is aborted too and
 * the pending entry is dropped, so the next caller starts fresh instead of
 * being stuck behind a hung request. New waiters arriving while the fetch
 * is in flight bump the refcount, so a single straggler keeps the work
 * alive for everyone who follows.
 */

export interface CacheLike<K, V> {
  get(key: K): V | undefined;
  set(key: K, value: V): void;
}

export interface SingleFlight<K, V> {
  /**
   * Resolve `key` from the cache, falling through to `fetcher` on miss.
   * Concurrent callers for the same key share one in-flight `fetcher`
   * invocation; the result is written to `cache` on success.
   *
   * `signal` rejects only the caller's await. The underlying fetch keeps
   * running unless every waiter has aborted, in which case the
   * `AbortSignal` passed to `fetcher` fires and the pending entry is
   * dropped so the next caller starts fresh.
   *
   * `fetcher` may throw synchronously; the throw is converted to a
   * rejected promise so `load`'s `Promise<V>` contract holds.
   */
  load(
    key: K,
    fetcher: (signal: AbortSignal) => Promise<V>,
    signal?: AbortSignal,
  ): Promise<V>;
}

interface Pending<V> {
  promise: Promise<V>;
  controller: AbortController;
  refCount: number;
}

/** Build a SingleFlight backed by `cache`. */
export function singleFlight<K, V>(cache: CacheLike<K, V>): SingleFlight<K, V> {
  const pending = new Map<K, Pending<V>>();

  return {
    load(
      key: K,
      fetcher: (signal: AbortSignal) => Promise<V>,
      signal?: AbortSignal,
    ): Promise<V> {
      // Reject (don't throw) when the caller is already aborted — callers
      // expect a Promise back, including in the pre-aborted case.
      if (signal?.aborted) return Promise.reject(makeAbortError());

      const hit = cache.get(key);
      if (hit !== undefined) return Promise.resolve(hit);

      let entry = pending.get(key);
      if (!entry) {
        const controller = new AbortController();
        const promise = invokeFetcher(fetcher, controller.signal)
          .then((value) => {
            cache.set(key, value);
            return value;
          })
          .finally(() => {
            if (pending.get(key)?.promise === promise) pending.delete(key);
          });
        const fresh: Pending<V> = { promise, controller, refCount: 0 };
        pending.set(key, fresh);
        entry = fresh;
      }
      const owned = entry;
      owned.refCount++;

      const release = () => {
        owned.refCount--;
        if (owned.refCount > 0) return;
        // Last waiter just released. If the entry is still in `pending`
        // the fetch hasn't settled yet — drop the slot synchronously so
        // any new caller starts fresh, then abort the in-flight work.
        // If `.finally` already cleared it, the fetch resolved and the
        // controller.abort() is a no-op.
        if (pending.get(key) === owned) {
          pending.delete(key);
          owned.controller.abort();
        }
      };

      if (!signal) {
        return owned.promise.then(
          (value) => {
            release();
            return value;
          },
          (error) => {
            release();
            throw error;
          },
        );
      }

      return new Promise<V>((resolve, reject) => {
        let done = false;
        // `finish` ensures release runs exactly once per caller and wins
        // the resolve/reject race between abort and underlying settle.
        const finish = (): boolean => {
          if (done) return false;
          done = true;
          signal.removeEventListener("abort", handleAbort);
          release();
          return true;
        };
        const handleAbort = () => {
          if (finish()) reject(makeAbortError());
        };
        signal.addEventListener("abort", handleAbort, { once: true });
        owned.promise.then(
          (value) => {
            if (finish()) resolve(value);
          },
          (error) => {
            if (finish()) reject(error);
          },
        );
      });
    },
  };
}

/**
 * Invoke `fetcher`, converting a synchronous throw into a rejected
 * Promise so callers always observe a Promise-shaped failure.
 */
function invokeFetcher<V>(
  fetcher: (signal: AbortSignal) => Promise<V>,
  signal: AbortSignal,
): Promise<V> {
  try {
    return fetcher(signal);
  } catch (error) {
    return Promise.reject(error);
  }
}

function makeAbortError(): Error {
  return new DOMException("The operation was aborted.", "AbortError");
}
