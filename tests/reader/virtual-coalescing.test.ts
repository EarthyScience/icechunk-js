/**
 * Virtual-chunk coalescing: verifies that when enabled and many virtual
 * chunks share a backing URL, `fetchChunkPayload`/`fetchChunkPayloadRange`
 * collapse the reads into a handful of merged Range GETs via zarrita's
 * `withRangeCoalescing`.
 *
 * These tests only run when the installed zarrita exports
 * `withRangeCoalescing`. The CI matrix also runs against older zarrita
 * versions; those jobs still exercise the uncoalesced fallback path through
 * the regular virtual-chunk tests in session.test.ts.
 */

import { describe, it, expect, vi } from "vitest";
import { ReadSession } from "../../src/reader/session.js";
import { MockStorage, createMockSnapshotId } from "../fixtures/mock-storage.js";
import { SpecVersion } from "../../src/format/header.js";
import type { Snapshot } from "../../src/format/flatbuffers/types.js";
import type { RangeCoalescingFn } from "../../src/index.js";

const withRangeCoalescing = await import("zarrita").then(
  (mod) =>
    (mod as Record<string, unknown>).withRangeCoalescing as
      | RangeCoalescingFn
      | undefined,
  () => undefined,
);

const itWithRangeCoalescing = withRangeCoalescing ? it : it.skip;

/**
 * Minimal ReadSession with just enough wiring to drive
 * fetchChunkPayload / fetchChunkPayloadRange against mocked HTTP.
 * Mirrors the `createMockSession` helper from session.test.ts.
 */
function createMockSession(options: { storage?: MockStorage } = {}): any {
  const snapshot: Snapshot = {
    id: createMockSnapshotId(1) as any,
    parentId: null,
    nodes: [],
    flushedAt: BigInt(Date.now() * 1000),
    message: "test commit",
    metadata: [],
    manifestFiles: [],
  };
  const session = Object.create(ReadSession.prototype);
  session.storage = options.storage ?? new MockStorage({});
  session.snapshot = snapshot;
  session.specVersion = SpecVersion.V1_0;
  session.manifestCache = new Map();
  session.nextFetchClientId = 1;
  session.nextRangeCoalescerId = 1;
  return session;
}

/**
 * Install a fetch spy whose response is an exact byte-range slice of
 * `backing`. Returns the spy so tests can assert call counts / headers.
 */
function spyFetchEchoingBacking(backing: Uint8Array) {
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation(async (_url, init) => {
      const rangeHeader = (init?.headers as Record<string, string>).Range;
      const match = /bytes=(\d+)-(\d+)/.exec(rangeHeader);
      if (!match) throw new Error(`Unexpected Range header: ${rangeHeader}`);
      const start = Number(match[1]);
      const end = Number(match[2]) + 1; // HTTP Range is inclusive, JS slice is exclusive
      const slice = backing.slice(start, end);
      // Copy into a fresh ArrayBuffer so every response is an independent buffer.
      const buf = new ArrayBuffer(slice.byteLength);
      new Uint8Array(buf).set(slice);
      return {
        ok: true,
        status: 206,
        statusText: "Partial Content",
        arrayBuffer: vi.fn().mockResolvedValue(buf),
      } as unknown as Response;
    });
}

function spyStorageEchoingBacking(storage: MockStorage, backing: Uint8Array) {
  return vi
    .spyOn(storage, "getObject")
    .mockImplementation(async (_path, range) => {
      if (!range) return backing;
      return backing.slice(range.start, range.end);
    });
}

function virtualPayload(
  location: string,
  offset: number,
  length: number,
  opts: { etag?: string | null; lastModified?: number } = {},
) {
  return {
    type: "virtual" as const,
    location,
    offset,
    length,
    checksumEtag: opts.etag ?? null,
    checksumLastModified: opts.lastModified ?? 0,
  };
}

/**
 * Build a deterministic byte pattern so every requested slice is
 * distinguishable by content — lets us catch off-by-one errors where one
 * caller gets another caller's bytes.
 */
function makeBacking(size: number): Uint8Array {
  const data = new Uint8Array(size);
  for (let i = 0; i < size; i++) data[i] = i & 0xff;
  return data;
}

const coalescingOptions = { withRangeCoalescing };

async function waitUntil(
  condition: () => boolean,
  message: string,
): Promise<void> {
  for (let i = 0; i < 20; i++) {
    if (condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  throw new Error(message);
}

describe("Virtual chunk coalescing", () => {
  it("does not coalesce virtual reads unless range coalescing is enabled", async () => {
    const backing = makeBacking(1024);
    const fetchSpy = spyFetchEchoingBacking(backing);
    const session = createMockSession();
    const url = "https://example.com/default.bin";

    const [a, b, c] = await Promise.all([
      session.fetchChunkPayload(virtualPayload(url, 0, 10)),
      session.fetchChunkPayload(virtualPayload(url, 20, 10)),
      session.fetchChunkPayload(virtualPayload(url, 40, 10)),
    ]);

    expect(fetchSpy).toHaveBeenCalledTimes(3);
    expect(
      fetchSpy.mock.calls.map(
        (call) => (call[1]!.headers as Record<string, string>).Range,
      ),
    ).toEqual(["bytes=0-9", "bytes=20-29", "bytes=40-49"]);
    expect(a).toEqual(backing.slice(0, 10));
    expect(b).toEqual(backing.slice(20, 30));
    expect(c).toEqual(backing.slice(40, 50));

    fetchSpy.mockRestore();
  });

  it("partitions cached stores by range coalescer function", async () => {
    const session = createMockSession();
    const url = "https://example.com/coalescer.bin";
    const coalescerA: RangeCoalescingFn = vi.fn((store) => ({
      ...store,
      getRange: vi.fn(async () => new Uint8Array([1])),
    }));
    const coalescerB: RangeCoalescingFn = vi.fn((store) => ({
      ...store,
      getRange: vi.fn(async () => new Uint8Array([2])),
    }));

    const a = await session.fetchChunkPayload(virtualPayload(url, 0, 1), {
      withRangeCoalescing: coalescerA,
    });
    const b = await session.fetchChunkPayload(virtualPayload(url, 0, 1), {
      withRangeCoalescing: coalescerB,
    });

    expect(a).toEqual(new Uint8Array([1]));
    expect(b).toEqual(new Uint8Array([2]));
    expect(coalescerA).toHaveBeenCalledTimes(1);
    expect(coalescerB).toHaveBeenCalledTimes(1);
  });

  itWithRangeCoalescing(
    "merges concurrent same-URL reads within the gap threshold into one fetch",
    async () => {
      const backing = makeBacking(1024);
      const fetchSpy = spyFetchEchoingBacking(backing);
      const session = createMockSession();
      const url = "https://example.com/data.bin";

      const [a, b, c] = await Promise.all([
        session.fetchChunkPayload(
          virtualPayload(url, 0, 10),
          coalescingOptions,
        ),
        session.fetchChunkPayload(
          virtualPayload(url, 20, 10),
          coalescingOptions,
        ),
        session.fetchChunkPayload(
          virtualPayload(url, 40, 10),
          coalescingOptions,
        ),
      ]);

      // One merged GET instead of three individual ones.
      expect(fetchSpy).toHaveBeenCalledTimes(1);
      const [calledUrl, calledInit] = fetchSpy.mock.calls[0]!;
      expect(calledUrl).toBe(url);
      // Merged span is 0..49 — smallest offset through largest (offset+length-1).
      expect((calledInit!.headers as Record<string, string>).Range).toBe(
        "bytes=0-49",
      );

      // Each caller should still get exactly its requested bytes.
      expect(a).toEqual(backing.slice(0, 10));
      expect(b).toEqual(backing.slice(20, 30));
      expect(c).toEqual(backing.slice(40, 50));

      fetchSpy.mockRestore();
    },
  );

  itWithRangeCoalescing(
    "issues separate fetches when concurrent reads are farther apart than the 32KB coalesce gap",
    async () => {
      // 200_000-byte backing so we can place one read at 0 and another at
      // 100_000 — way beyond zarrita's default 32KB gap.
      const backing = makeBacking(200_000);
      const fetchSpy = spyFetchEchoingBacking(backing);
      const session = createMockSession();
      const url = "https://example.com/sparse.bin";

      const [near, far] = await Promise.all([
        session.fetchChunkPayload(
          virtualPayload(url, 0, 100),
          coalescingOptions,
        ),
        session.fetchChunkPayload(
          virtualPayload(url, 100_000, 100),
          coalescingOptions,
        ),
      ]);

      // Past the gap threshold → two separate HTTP fetches.
      expect(fetchSpy).toHaveBeenCalledTimes(2);
      expect(near).toEqual(backing.slice(0, 100));
      expect(far).toEqual(backing.slice(100_000, 100_100));

      fetchSpy.mockRestore();
    },
  );

  itWithRangeCoalescing(
    "never coalesces reads that target different backing URLs",
    async () => {
      const backing = makeBacking(1024);
      const fetchSpy = spyFetchEchoingBacking(backing);
      const session = createMockSession();

      const urlA = "https://example.com/a.bin";
      const urlB = "https://example.com/b.bin";

      const [a, b] = await Promise.all([
        session.fetchChunkPayload(
          virtualPayload(urlA, 0, 32),
          coalescingOptions,
        ),
        session.fetchChunkPayload(
          virtualPayload(urlB, 0, 32),
          coalescingOptions,
        ),
      ]);

      expect(fetchSpy).toHaveBeenCalledTimes(2);
      const calledUrls = fetchSpy.mock.calls.map((c) => c[0]);
      expect(calledUrls).toEqual(expect.arrayContaining([urlA, urlB]));
      expect(a).toEqual(backing.slice(0, 32));
      expect(b).toEqual(backing.slice(0, 32));

      fetchSpy.mockRestore();
    },
  );

  itWithRangeCoalescing(
    "partitions the store cache by checksum when validateChecksums is set, preventing mismatched-checksum coalescing",
    async () => {
      const backing = makeBacking(1024);
      const fetchSpy = spyFetchEchoingBacking(backing);
      const session = createMockSession();
      const url = "https://example.com/checksum.bin";

      const [a, b] = await Promise.all([
        session.fetchChunkPayload(
          virtualPayload(url, 0, 10, { etag: '"v1"' }),
          { validateChecksums: true, withRangeCoalescing },
        ),
        session.fetchChunkPayload(
          virtualPayload(url, 20, 10, { etag: '"v2"' }),
          { validateChecksums: true, withRangeCoalescing },
        ),
      ]);

      // Different checksums → two separate stores → two separate fetches.
      expect(fetchSpy).toHaveBeenCalledTimes(2);
      expect(a).toEqual(backing.slice(0, 10));
      expect(b).toEqual(backing.slice(20, 30));

      // Each fetch must carry its own conditional header.
      const headers = fetchSpy.mock.calls.map(
        (c) => c[1]!.headers as Record<string, string>,
      );
      const etagsSent = headers.map((h) => h["If-Match"]).sort();
      expect(etagsSent).toEqual(['"v1"', '"v2"']);

      fetchSpy.mockRestore();
    },
  );

  itWithRangeCoalescing(
    "coalesces same-URL same-checksum reads into a single conditional GET when validateChecksums is set",
    async () => {
      const backing = makeBacking(1024);
      const fetchSpy = spyFetchEchoingBacking(backing);
      const session = createMockSession();
      const url = "https://example.com/conditional.bin";
      const etag = '"shared-etag"';
      const lastModified = 1_700_000_000; // arbitrary epoch seconds

      const [a, b] = await Promise.all([
        session.fetchChunkPayload(
          virtualPayload(url, 0, 10, { etag, lastModified }),
          { validateChecksums: true, withRangeCoalescing },
        ),
        session.fetchChunkPayload(
          virtualPayload(url, 16, 10, { etag, lastModified }),
          { validateChecksums: true, withRangeCoalescing },
        ),
      ]);

      expect(fetchSpy).toHaveBeenCalledTimes(1);
      const headers = fetchSpy.mock.calls[0]![1]!.headers as Record<
        string,
        string
      >;
      expect(headers.Range).toBe("bytes=0-25");
      expect(headers["If-Match"]).toBe(etag);
      expect(headers["If-Unmodified-Since"]).toBe(
        new Date(lastModified * 1000).toUTCString(),
      );
      expect(a).toEqual(backing.slice(0, 10));
      expect(b).toEqual(backing.slice(16, 26));

      fetchSpy.mockRestore();
    },
  );

  itWithRangeCoalescing(
    "coalesces fetchChunkPayloadRange calls the same way as fetchChunkPayload",
    async () => {
      const backing = makeBacking(1024);
      const fetchSpy = spyFetchEchoingBacking(backing);
      const session = createMockSession();
      const url = "https://example.com/range.bin";

      // Two chunks at different offsets, each asking for a sub-range inside
      // the chunk. The coalesced request should span both absolute windows.
      const [a, b] = await Promise.all([
        session.fetchChunkPayloadRange(
          virtualPayload(url, 100, 50),
          { offset: 10, length: 20 }, // absolute 110..129
          coalescingOptions,
        ),
        session.fetchChunkPayloadRange(
          virtualPayload(url, 200, 50),
          { offset: 5, length: 10 }, // absolute 205..214
          coalescingOptions,
        ),
      ]);

      expect(fetchSpy).toHaveBeenCalledTimes(1);
      const [, init] = fetchSpy.mock.calls[0]!;
      expect((init!.headers as Record<string, string>).Range).toBe(
        "bytes=110-214",
      );
      expect(a).toEqual(backing.slice(110, 130));
      expect(b).toEqual(backing.slice(205, 215));

      fetchSpy.mockRestore();
    },
  );

  itWithRangeCoalescing(
    "partitions cached virtual stores by fetch client",
    async () => {
      const backing = makeBacking(1024);
      const session = createMockSession();
      const url = "https://example.com/client.bin";

      function responseFor(offset: number, length: number) {
        const slice = backing.slice(offset, offset + length);
        const buf = new ArrayBuffer(slice.byteLength);
        new Uint8Array(buf).set(slice);
        return {
          status: 206,
          statusText: "Partial Content",
          arrayBuffer: vi.fn().mockResolvedValue(buf),
        } as unknown as Response;
      }

      const clientA = {
        fetch: vi.fn().mockResolvedValue(responseFor(0, 10)),
      };
      const clientB = {
        fetch: vi.fn().mockResolvedValue(responseFor(20, 10)),
      };

      await session.fetchChunkPayload(virtualPayload(url, 0, 10), {
        fetchClient: clientA,
        withRangeCoalescing,
      });
      await session.fetchChunkPayload(virtualPayload(url, 20, 10), {
        fetchClient: clientB,
        withRangeCoalescing,
      });

      expect(clientA.fetch).toHaveBeenCalledTimes(1);
      expect(clientB.fetch).toHaveBeenCalledTimes(1);
      expect(clientB.fetch).toHaveBeenCalledWith(url, {
        headers: { Range: "bytes=20-29" },
        signal: undefined,
      });
    },
  );

  itWithRangeCoalescing(
    "coalesces virtual reads with different abort signals using zarrita's merged signal",
    async () => {
      const backing = makeBacking(1024);
      const fetchSpy = spyFetchEchoingBacking(backing);
      const session = createMockSession();
      const url = "https://example.com/signals.bin";
      const controllerA = new AbortController();
      const controllerB = new AbortController();

      const [a, b] = await Promise.all([
        session.fetchChunkPayload(virtualPayload(url, 0, 10), {
          signal: controllerA.signal,
          withRangeCoalescing,
        }),
        session.fetchChunkPayload(virtualPayload(url, 20, 10), {
          signal: controllerB.signal,
          withRangeCoalescing,
        }),
      ]);

      expect(fetchSpy).toHaveBeenCalledTimes(1);
      const [, init] = fetchSpy.mock.calls[0]!;
      expect((init!.headers as Record<string, string>).Range).toBe(
        "bytes=0-29",
      );
      expect(init!.signal).toBeInstanceOf(AbortSignal);
      expect(a).toEqual(backing.slice(0, 10));
      expect(b).toEqual(backing.slice(20, 30));

      fetchSpy.mockRestore();
    },
  );

  itWithRangeCoalescing(
    "rejects all virtual reads in a coalesced batch when one signal aborts",
    async () => {
      const session = createMockSession();
      const url = "https://example.com/mid-abort.bin";
      const controllerA = new AbortController();
      const controllerB = new AbortController();
      let pending:
        | {
            range: string;
            signal: AbortSignal | undefined;
          }
        | undefined;

      const fetchSpy = vi
        .spyOn(globalThis, "fetch")
        .mockImplementation((_url, init) => {
          const range = (init?.headers as Record<string, string>).Range;
          const signal = init?.signal as AbortSignal | undefined;

          return new Promise<Response>((_resolve, reject) => {
            if (signal?.aborted) {
              reject(
                signal.reason ??
                  new DOMException("Operation aborted", "AbortError"),
              );
              return;
            }
            signal?.addEventListener(
              "abort",
              () =>
                reject(
                  signal.reason ??
                    new DOMException("Operation aborted", "AbortError"),
                ),
              { once: true },
            );
            pending = { range, signal };
          });
        });

      const promiseA = session.fetchChunkPayload(virtualPayload(url, 0, 10), {
        signal: controllerA.signal,
        withRangeCoalescing,
      });
      const rejectedA = promiseA.then(
        () => {
          throw new Error("expected request A to abort");
        },
        (error: unknown) => error,
      );

      const promiseB = session.fetchChunkPayload(virtualPayload(url, 20, 10), {
        signal: controllerB.signal,
        withRangeCoalescing,
      });
      const rejectedB = promiseB.then(
        () => {
          throw new Error("expected request B to abort");
        },
        (error: unknown) => error,
      );

      await waitUntil(
        () => pending !== undefined,
        "expected the coalesced fetch to start",
      );

      controllerA.abort();

      expect(pending!.range).toBe("bytes=0-29");
      expect(pending!.signal).toBeInstanceOf(AbortSignal);

      expect(await rejectedA).toMatchObject({ name: "AbortError" });
      expect(await rejectedB).toMatchObject({ name: "AbortError" });
      expect(fetchSpy).toHaveBeenCalledTimes(1);

      fetchSpy.mockRestore();
    },
  );

  itWithRangeCoalescing(
    "coalesces native chunk range reads that target the same storage object",
    async () => {
      const backing = makeBacking(1024);
      const storage = new MockStorage({});
      const getObjectSpy = spyStorageEchoingBacking(storage, backing);
      const session = createMockSession({ storage });
      const payload = {
        type: "native" as const,
        chunkId: createMockSnapshotId(101) as any,
        offset: 0,
        length: 128,
      };

      const [a, b] = await Promise.all([
        session.fetchChunkPayloadRange(
          payload,
          { offset: 0, length: 10 },
          coalescingOptions,
        ),
        session.fetchChunkPayloadRange(
          payload,
          { offset: 20, length: 10 },
          coalescingOptions,
        ),
      ]);

      expect(getObjectSpy).toHaveBeenCalledTimes(1);
      expect(getObjectSpy).toHaveBeenCalledWith(
        expect.any(String),
        { start: 0, end: 30 },
        undefined,
      );
      expect(a).toEqual(backing.slice(0, 10));
      expect(b).toEqual(backing.slice(20, 30));
    },
  );

  itWithRangeCoalescing(
    "coalesces native reads with different abort signals",
    async () => {
      const backing = makeBacking(1024);
      const storage = new MockStorage({});
      const getObjectSpy = spyStorageEchoingBacking(storage, backing);
      const session = createMockSession({ storage });
      const payload = {
        type: "native" as const,
        chunkId: createMockSnapshotId(102) as any,
        offset: 0,
        length: 128,
      };
      const controllerA = new AbortController();
      const controllerB = new AbortController();

      const [a, b] = await Promise.all([
        session.fetchChunkPayloadRange(
          payload,
          { offset: 0, length: 10 },
          { signal: controllerA.signal, withRangeCoalescing },
        ),
        session.fetchChunkPayloadRange(
          payload,
          { offset: 20, length: 10 },
          { signal: controllerB.signal, withRangeCoalescing },
        ),
      ]);

      expect(getObjectSpy).toHaveBeenCalledTimes(1);
      expect(getObjectSpy).toHaveBeenCalledWith(
        expect.any(String),
        { start: 0, end: 30 },
        { signal: expect.any(AbortSignal) },
      );
      expect(a).toEqual(backing.slice(0, 10));
      expect(b).toEqual(backing.slice(20, 30));
    },
  );
});
