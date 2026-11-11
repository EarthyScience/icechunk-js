/**
 * `IcechunkStore.open` wiring for range coalescing: on by default, off with
 * `coalesce: false`, replaced by a custom wrapper with `withRangeCoalescing`.
 */

import { describe, it, expect, vi } from "vitest";
import { IcechunkStore } from "../src/store.js";
import type { AbsolutePath } from "../src/store.js";
import { ReadSession } from "../src/reader/session.js";
import type { RangeCoalescingFn } from "../src/index.js";

/** A ReadSession-shaped mock that passes the `instanceof` check in `open`. */
function mockSession() {
  const session = Object.create(ReadSession.prototype) as ReadSession & {
    getChunk: ReturnType<typeof vi.fn>;
  };
  session.getChunk = vi.fn(async () => new Uint8Array([1]));
  return session;
}

describe("IcechunkStore range coalescing options", () => {
  it("coalesces by default and exposes the coalescer with its stats", async () => {
    const session = mockSession();
    const store = await IcechunkStore.open(session);

    expect(store.rangeCoalescer).toBeDefined();
    expect(store.rangeCoalescer!.stats.requests).toBe(0);

    await store.get("/arr/c/0" as AbsolutePath);
    expect(session.getChunk).toHaveBeenCalledWith(
      "/arr",
      [0],
      expect.objectContaining({ withRangeCoalescing: store.rangeCoalescer }),
    );
  });

  it("passes coalesce options to the built-in coalescer", async () => {
    const store = await IcechunkStore.open(mockSession(), {
      coalesce: { maxGap: 0, maxBytes: 1024, window: 5 },
    });
    expect(store.rangeCoalescer).toBeDefined();
  });

  it("issues uncoalesced reads with coalesce: false", async () => {
    const session = mockSession();
    const store = await IcechunkStore.open(session, { coalesce: false });

    expect(store.rangeCoalescer).toBeUndefined();
    await store.get("/arr/c/0" as AbsolutePath);
    const options = session.getChunk.mock.calls[0][2];
    expect(options).not.toHaveProperty("withRangeCoalescing");
  });

  it("lets a custom wrapper replace the built-in coalescer", async () => {
    const session = mockSession();
    const custom: RangeCoalescingFn = vi.fn((store) => store);
    const store = await IcechunkStore.open(session, {
      withRangeCoalescing: custom,
      coalesce: { maxGap: 1 },
    });

    expect(store.rangeCoalescer).toBeUndefined();
    await store.get("/arr/c/0" as AbsolutePath);
    expect(session.getChunk).toHaveBeenCalledWith(
      "/arr",
      [0],
      expect.objectContaining({ withRangeCoalescing: custom }),
    );
  });

  it("shares the coalescer with resolved sub-stores", async () => {
    const store = await IcechunkStore.open(mockSession());
    expect(store.resolve("/group/arr").rangeCoalescer).toBe(
      store.rangeCoalescer,
    );
  });
});
