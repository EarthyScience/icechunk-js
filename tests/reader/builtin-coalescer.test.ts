/**
 * Built-in range coalescer (`createRangeCoalescer`), driven directly against a
 * recording mock store so each merging rule is observable: which requests are
 * issued, and that every caller gets exactly its own bytes back.
 */

import { describe, it, expect, vi } from "vitest";
import {
  createRangeCoalescer,
  type AsyncReadable,
} from "../../src/reader/range-coalescer.js";

type Range = { offset: number; length: number } | { suffixLength: number };
interface Call {
  key: string;
  range: Range;
  signal: AbortSignal | undefined;
}

function makeBacking(size: number): Uint8Array {
  const data = new Uint8Array(size);
  for (let i = 0; i < size; i++) data[i] = (i * 7 + 3) & 0xff;
  return data;
}

/**
 * Mock store serving slices of `backing`. `fail` makes a request whose key
 * and range match reject; `hold` lets a test keep a request pending.
 */
function makeMockStore(
  backing: Uint8Array,
  opts: {
    fail?: (call: Call) => boolean;
    hold?: (call: Call) => boolean;
    short?: (call: Call) => boolean;
  } = {},
) {
  const calls: Call[] = [];
  const held: Array<{ call: Call; release: () => void }> = [];
  const store: AsyncReadable = {
    get: vi.fn(async () => undefined),
    getRange: vi.fn(async (key: string, range: Range, options) => {
      const call: Call = { key, range, signal: options?.signal };
      calls.push(call);
      if (opts.hold?.(call)) {
        await new Promise<void>((resolve, reject) => {
          held.push({ call, release: resolve });
          call.signal?.addEventListener("abort", () =>
            reject(call.signal!.reason),
          );
        });
      }
      if (opts.fail?.(call)) throw new Error(`boom ${JSON.stringify(range)}`);
      if ("suffixLength" in range) return backing.slice(-range.suffixLength);
      const end = range.offset + range.length;
      const slice = backing.slice(
        range.offset,
        opts.short?.(call) ? end - 1 : end,
      );
      return slice;
    }),
  };
  return { store, calls, held };
}

const ranges = (calls: Call[]) => calls.map((c) => c.range);
const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

describe("createRangeCoalescer", () => {
  it("merges adjacent ranges into one request and hands back the right slices", async () => {
    const backing = makeBacking(1000);
    const { store, calls } = makeMockStore(backing);
    const coalescer = createRangeCoalescer();
    const wrapped = coalescer(store);

    const [a, b, c] = await Promise.all([
      wrapped.getRange("/f", { offset: 200, length: 100 }),
      wrapped.getRange("/f", { offset: 0, length: 100 }),
      wrapped.getRange("/f", { offset: 100, length: 100 }),
    ]);

    expect(ranges(calls)).toEqual([{ offset: 0, length: 300 }]);
    expect(a).toEqual(backing.slice(200, 300));
    expect(b).toEqual(backing.slice(0, 100));
    expect(c).toEqual(backing.slice(100, 200));
    // Slices are copies: no caller holds the whole merged buffer.
    expect(a!.buffer.byteLength).toBe(100);
    expect(coalescer.stats).toEqual({
      ranges: 3,
      requests: 1,
      bytesRequested: 300,
      bytesFetched: 300,
      bytesDiscarded: 0,
      fallbacks: 0,
    });
  });

  it("bridges gaps up to maxGap, counting the gap bytes as discarded", async () => {
    const backing = makeBacking(1000);
    const { store, calls } = makeMockStore(backing);
    const coalescer = createRangeCoalescer({ maxGap: 50 });
    const wrapped = coalescer(store);

    const [a, b] = await Promise.all([
      wrapped.getRange("/f", { offset: 0, length: 100 }),
      wrapped.getRange("/f", { offset: 150, length: 100 }),
    ]);

    expect(ranges(calls)).toEqual([{ offset: 0, length: 250 }]);
    expect(a).toEqual(backing.slice(0, 100));
    expect(b).toEqual(backing.slice(150, 250));
    expect(coalescer.stats.bytesDiscarded).toBe(50);
  });

  it("splits when the gap exceeds maxGap", async () => {
    const backing = makeBacking(1000);
    const { store, calls } = makeMockStore(backing);
    const wrapped = createRangeCoalescer({ maxGap: 49 })(store);

    const [a, b] = await Promise.all([
      wrapped.getRange("/f", { offset: 0, length: 100 }),
      wrapped.getRange("/f", { offset: 150, length: 100 }),
    ]);

    expect(ranges(calls)).toEqual([
      { offset: 0, length: 100 },
      { offset: 150, length: 100 },
    ]);
    expect(a).toEqual(backing.slice(0, 100));
    expect(b).toEqual(backing.slice(150, 250));
  });

  it("splits when a merged request would exceed maxBytes", async () => {
    const backing = makeBacking(1000);
    const { store, calls } = makeMockStore(backing);
    const wrapped = createRangeCoalescer({ maxBytes: 250 })(store);

    const results = await Promise.all(
      [0, 100, 200, 300].map((offset) =>
        wrapped.getRange("/f", { offset, length: 100 }),
      ),
    );

    expect(ranges(calls)).toEqual([
      { offset: 0, length: 200 },
      { offset: 200, length: 200 },
    ]);
    results.forEach((data, i) =>
      expect(data).toEqual(backing.slice(i * 100, i * 100 + 100)),
    );
  });

  it("still issues a single read larger than maxBytes, on its own", async () => {
    const backing = makeBacking(1000);
    const { store, calls } = makeMockStore(backing);
    const wrapped = createRangeCoalescer({ maxBytes: 10 })(store);

    const [a, b] = await Promise.all([
      wrapped.getRange("/f", { offset: 0, length: 500 }),
      wrapped.getRange("/f", { offset: 500, length: 5 }),
    ]);

    expect(ranges(calls)).toEqual([
      { offset: 0, length: 500 },
      { offset: 500, length: 5 },
    ]);
    expect(a).toEqual(backing.slice(0, 500));
    expect(b).toEqual(backing.slice(500, 505));
  });

  it("never merges reads for different keys", async () => {
    const backing = makeBacking(1000);
    const { store, calls } = makeMockStore(backing);
    const wrapped = createRangeCoalescer()(store);

    const [a, b] = await Promise.all([
      wrapped.getRange("/a", { offset: 0, length: 100 }),
      wrapped.getRange("/b", { offset: 100, length: 100 }),
    ]);

    expect(calls.map((c) => [c.key, c.range])).toEqual([
      ["/a", { offset: 0, length: 100 }],
      ["/b", { offset: 100, length: 100 }],
    ]);
    expect(a).toEqual(backing.slice(0, 100));
    expect(b).toEqual(backing.slice(100, 200));
  });

  it("fetches identical ranges once and gives each caller its own copy", async () => {
    const backing = makeBacking(1000);
    const { store, calls } = makeMockStore(backing);
    const coalescer = createRangeCoalescer();
    const wrapped = coalescer(store);

    const [a, b, c] = await Promise.all([
      wrapped.getRange("/f", { offset: 10, length: 20 }),
      wrapped.getRange("/f", { offset: 10, length: 20 }),
      wrapped.getRange("/f", { offset: 40, length: 20 }),
    ]);

    expect(ranges(calls)).toEqual([{ offset: 10, length: 50 }]);
    expect(a).toEqual(backing.slice(10, 30));
    expect(b).toEqual(backing.slice(10, 30));
    expect(c).toEqual(backing.slice(40, 60));
    expect(a!.buffer).not.toBe(b!.buffer);
    expect(coalescer.stats.bytesRequested).toBe(60);
    expect(coalescer.stats.bytesFetched).toBe(50);
    expect(coalescer.stats.bytesDiscarded).toBe(10);
  });

  it("handles overlapping ranges", async () => {
    const backing = makeBacking(1000);
    const { store, calls } = makeMockStore(backing);
    const coalescer = createRangeCoalescer();
    const wrapped = coalescer(store);

    const [a, b] = await Promise.all([
      wrapped.getRange("/f", { offset: 0, length: 100 }),
      wrapped.getRange("/f", { offset: 50, length: 100 }),
    ]);

    expect(ranges(calls)).toEqual([{ offset: 0, length: 150 }]);
    expect(a).toEqual(backing.slice(0, 100));
    expect(b).toEqual(backing.slice(50, 150));
    expect(coalescer.stats.bytesDiscarded).toBe(0);
  });

  it("retries members one by one when a merged request fails, so only the bad range rejects", async () => {
    const backing = makeBacking(1000);
    const { store, calls } = makeMockStore(backing, {
      fail: (call) =>
        "offset" in call.range &&
        (call.range.length === 300 || call.range.offset === 100),
    });
    const coalescer = createRangeCoalescer();
    const wrapped = coalescer(store);

    const results = await Promise.allSettled([
      wrapped.getRange("/f", { offset: 0, length: 100 }),
      wrapped.getRange("/f", { offset: 100, length: 100 }),
      wrapped.getRange("/f", { offset: 200, length: 100 }),
    ]);

    expect(ranges(calls)).toEqual([
      { offset: 0, length: 300 },
      { offset: 0, length: 100 },
      { offset: 100, length: 100 },
      { offset: 200, length: 100 },
    ]);
    expect(results[0]).toEqual({
      status: "fulfilled",
      value: backing.slice(0, 100),
    });
    expect(results[1].status).toBe("rejected");
    expect((results[1] as PromiseRejectedResult).reason.message).toMatch(
      /boom/,
    );
    expect(results[2]).toEqual({
      status: "fulfilled",
      value: backing.slice(200, 300),
    });
    expect(coalescer.stats.fallbacks).toBe(1);
    expect(coalescer.stats.requests).toBe(4);
  });

  it("treats a short merged response as a failure and falls back per member", async () => {
    const backing = makeBacking(1000);
    const { store, calls } = makeMockStore(backing, {
      short: (call) => "offset" in call.range && call.range.length === 200,
    });
    const wrapped = createRangeCoalescer()(store);

    const [a, b] = await Promise.all([
      wrapped.getRange("/f", { offset: 0, length: 100 }),
      wrapped.getRange("/f", { offset: 100, length: 100 }),
    ]);

    expect(calls).toHaveLength(3);
    expect(a).toEqual(backing.slice(0, 100));
    expect(b).toEqual(backing.slice(100, 200));
  });

  it("does not retry a lone read; its error propagates as-is", async () => {
    const backing = makeBacking(1000);
    const { store, calls } = makeMockStore(backing, { fail: () => true });
    const wrapped = createRangeCoalescer()(store);

    await expect(
      wrapped.getRange("/f", { offset: 0, length: 100 }),
    ).rejects.toThrow(/boom/);
    expect(calls).toHaveLength(1);
  });

  it("passes a lone read through untouched, signal included", async () => {
    const backing = makeBacking(1000);
    const { store, calls } = makeMockStore(backing);
    const wrapped = createRangeCoalescer()(store);
    const controller = new AbortController();

    const data = await wrapped.getRange(
      "/f",
      { offset: 5, length: 10 },
      { signal: controller.signal },
    );

    expect(calls).toHaveLength(1);
    expect(calls[0].signal).toBe(controller.signal);
    expect(data).toEqual(backing.slice(5, 15));
  });

  it("passes suffix reads straight through", async () => {
    const backing = makeBacking(1000);
    const { store, calls } = makeMockStore(backing);
    const coalescer = createRangeCoalescer();
    const wrapped = coalescer(store);

    const [a, b] = await Promise.all([
      wrapped.getRange("/f", { suffixLength: 4 }),
      wrapped.getRange("/f", { offset: 0, length: 4 }),
    ]);

    expect(ranges(calls)).toEqual([
      { suffixLength: 4 },
      { offset: 0, length: 4 },
    ]);
    expect(a).toEqual(backing.slice(-4));
    expect(b).toEqual(backing.slice(0, 4));
    expect(coalescer.stats.ranges).toBe(1);
  });

  it("batches only reads issued within the same window", async () => {
    const backing = makeBacking(1000);
    const { store, calls } = makeMockStore(backing);
    const wrapped = createRangeCoalescer()(store);

    const first = wrapped.getRange("/f", { offset: 0, length: 100 });
    await tick();
    const second = wrapped.getRange("/f", { offset: 100, length: 100 });
    await Promise.all([first, second]);

    expect(ranges(calls)).toEqual([
      { offset: 0, length: 100 },
      { offset: 100, length: 100 },
    ]);
  });

  it("a positive window keeps the batch open across turns", async () => {
    const backing = makeBacking(1000);
    const { store, calls } = makeMockStore(backing);
    const wrapped = createRangeCoalescer({ window: 30 })(store);

    const first = wrapped.getRange("/f", { offset: 0, length: 100 });
    await tick();
    const second = wrapped.getRange("/f", { offset: 100, length: 100 });
    const [a, b] = await Promise.all([first, second]);

    expect(ranges(calls)).toEqual([{ offset: 0, length: 200 }]);
    expect(a).toEqual(backing.slice(0, 100));
    expect(b).toEqual(backing.slice(100, 200));
  });

  it("rejects a pre-aborted read without enqueueing it", async () => {
    const backing = makeBacking(1000);
    const { store, calls } = makeMockStore(backing);
    const wrapped = createRangeCoalescer()(store);
    const controller = new AbortController();
    controller.abort();

    await expect(
      wrapped.getRange(
        "/f",
        { offset: 0, length: 10 },
        { signal: controller.signal },
      ),
    ).rejects.toMatchObject({ name: "AbortError" });
    await tick();
    expect(calls).toHaveLength(0);
  });

  it("aborting one waiter rejects only that waiter; the merged request continues", async () => {
    const backing = makeBacking(1000);
    const { store, calls, held } = makeMockStore(backing, { hold: () => true });
    const wrapped = createRangeCoalescer()(store);
    const controllerA = new AbortController();
    const controllerB = new AbortController();

    const a = wrapped.getRange(
      "/f",
      { offset: 0, length: 100 },
      { signal: controllerA.signal },
    );
    const b = wrapped.getRange(
      "/f",
      { offset: 100, length: 100 },
      { signal: controllerB.signal },
    );
    const rejectedA = a.then(
      () => {
        throw new Error("expected A to abort");
      },
      (error: unknown) => error,
    );
    await tick();
    expect(held).toHaveLength(1);

    controllerA.abort();
    expect(await rejectedA).toMatchObject({ name: "AbortError" });
    expect(calls[0].signal!.aborted).toBe(false);

    held[0].release();
    expect(await b).toEqual(backing.slice(100, 200));
  });

  it("aborts the merged request once every waiter has aborted", async () => {
    const backing = makeBacking(1000);
    const { store, calls, held } = makeMockStore(backing, { hold: () => true });
    const coalescer = createRangeCoalescer();
    const wrapped = coalescer(store);
    const controllerA = new AbortController();
    const controllerB = new AbortController();

    const settled = Promise.allSettled([
      wrapped.getRange(
        "/f",
        { offset: 0, length: 100 },
        { signal: controllerA.signal },
      ),
      wrapped.getRange(
        "/f",
        { offset: 100, length: 100 },
        { signal: controllerB.signal },
      ),
    ]);
    await tick();
    expect(held).toHaveLength(1);

    controllerA.abort();
    expect(calls[0].signal!.aborted).toBe(false);
    controllerB.abort();
    expect(calls[0].signal!.aborted).toBe(true);

    const results = await settled;
    expect(results.map((r) => r.status)).toEqual(["rejected", "rejected"]);
    // An abort is not a failure: no per-member retry.
    expect(coalescer.stats.fallbacks).toBe(0);
    expect(calls).toHaveLength(1);
  });

  it("shares one stats object across every store it wraps, and resets it", async () => {
    const backing = makeBacking(1000);
    const first = makeMockStore(backing);
    const second = makeMockStore(backing);
    const coalescer = createRangeCoalescer();
    const wrappedA = coalescer(first.store);
    const wrappedB = coalescer(second.store);

    await Promise.all([
      wrappedA.getRange("/a", { offset: 0, length: 10 }),
      wrappedB.getRange("/b", { offset: 0, length: 10 }),
    ]);

    expect(coalescer.stats.requests).toBe(2);
    coalescer.resetStats();
    expect(coalescer.stats.requests).toBe(0);
  });

  it("delegates get() to the wrapped store", async () => {
    const { store } = makeMockStore(makeBacking(10));
    const wrapped = createRangeCoalescer()(store);
    await wrapped.get("/f");
    expect(store.get).toHaveBeenCalledWith("/f", undefined);
  });

  it("rejects invalid options", () => {
    expect(() => createRangeCoalescer({ maxGap: -1 })).toThrow(RangeError);
    expect(() => createRangeCoalescer({ maxBytes: NaN })).toThrow(RangeError);
    expect(() => createRangeCoalescer({ window: -5 })).toThrow(RangeError);
  });

  it("requires a range-capable store", () => {
    expect(() =>
      createRangeCoalescer()({ get: async () => undefined } as AsyncReadable),
    ).toThrow("requires a store with getRange");
  });
});
