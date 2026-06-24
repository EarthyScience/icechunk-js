import { describe, it, expect, vi } from "vitest";
import {
  makeStorageStore,
  makeUrlStore,
} from "../../src/reader/range-coalescer.js";
import type { ByteRange, RequestOptions, Storage } from "../../src/index.js";

function makeBacking(size: number): Uint8Array {
  const data = new Uint8Array(size);
  for (let i = 0; i < size; i++) data[i] = i & 0xff;
  return data;
}

function toArrayBuffer(data: Uint8Array): ArrayBuffer {
  return data.buffer.slice(
    data.byteOffset,
    data.byteOffset + data.byteLength,
  ) as ArrayBuffer;
}

function mockFetchResponse(
  status: number,
  data: Uint8Array,
  headers: Record<string, string> = {},
): Response {
  return {
    status,
    statusText: status === 206 ? "Partial Content" : "OK",
    headers: { get: (name: string) => headers[name.toLowerCase()] ?? null },
    arrayBuffer: vi.fn().mockResolvedValue(toArrayBuffer(data)),
  } as unknown as Response;
}

function makeStorage(data: Uint8Array): Storage {
  return {
    getObject: vi.fn(
      async (_path: string, _range?: ByteRange, _options?: RequestOptions) =>
        data,
    ),
    exists: vi.fn(async () => true),
    async *listPrefix() {},
  };
}

describe("range coalescer adapters", () => {
  it("rejects full URL reads because virtual stores are range-only", async () => {
    const store = makeUrlStore({ url: "https://example.com/data.bin" });

    await expect(store.get("/")).rejects.toThrow(
      "Virtual chunk URL store for https://example.com/data.bin only supports ranged reads",
    );
  });

  it("returns URL suffix range responses directly for 206 responses", async () => {
    const url = "https://example.com/data.bin";
    const body = new Uint8Array([7, 8, 9]);
    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(mockFetchResponse(206, body));
    const store = makeUrlStore({ url });

    const result = await store.getRange("/", { suffixLength: 3 });

    expect(fetchSpy).toHaveBeenCalledWith(url, {
      headers: { Range: "bytes=-3" },
      signal: undefined,
    });
    expect(result).toEqual(body);

    fetchSpy.mockRestore();
  });

  it("retries the regional endpoint when the S3 global endpoint 301-redirects", async () => {
    const globalUrl =
      "https://s3.amazonaws.com/us-west-2.opendata.source.coop/data.bin";
    const regionalUrl =
      "https://s3.us-west-2.amazonaws.com/us-west-2.opendata.source.coop/data.bin";
    const body = new Uint8Array([1, 2, 3]);
    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(
        mockFetchResponse(301, new Uint8Array(), {
          "x-amz-bucket-region": "us-west-2",
        }),
      )
      .mockResolvedValueOnce(mockFetchResponse(206, body));
    const store = makeUrlStore({ url: globalUrl });

    const result = await store.getRange("/", { offset: 0, length: 3 });

    expect(result).toEqual(body);
    expect(fetchSpy).toHaveBeenNthCalledWith(1, globalUrl, expect.anything());
    expect(fetchSpy).toHaveBeenNthCalledWith(2, regionalUrl, expect.anything());

    fetchSpy.mockRestore();
  });

  it("pins the regional endpoint for later reads after one redirect", async () => {
    const globalUrl =
      "https://s3.amazonaws.com/eu-central-1.example.bucket/data.bin";
    const regionalUrl =
      "https://s3.eu-central-1.amazonaws.com/eu-central-1.example.bucket/data.bin";
    const body = new Uint8Array([4, 5, 6]);
    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(
        mockFetchResponse(301, new Uint8Array(), {
          "x-amz-bucket-region": "eu-central-1",
        }),
      )
      .mockResolvedValue(mockFetchResponse(206, body));
    const store = makeUrlStore({ url: globalUrl });

    await store.getRange("/", { offset: 0, length: 3 });
    await store.getRange("/", { offset: 0, length: 3 });

    // First read redirects (global → regional); second read goes straight to regional.
    expect(fetchSpy).toHaveBeenCalledTimes(3);
    expect(fetchSpy).toHaveBeenNthCalledWith(3, regionalUrl, expect.anything());

    fetchSpy.mockRestore();
  });

  it("retries both reads when concurrent requests hit the global endpoint", async () => {
    const globalUrl =
      "https://s3.amazonaws.com/us-west-2.opendata.source.coop/data.bin";
    const regionalUrl =
      "https://s3.us-west-2.amazonaws.com/us-west-2.opendata.source.coop/data.bin";
    const body = new Uint8Array([1, 2, 3]);
    // Mock by target URL, not call order: the global endpoint always 301s,
    // the regional endpoint always serves. This reproduces two in-flight
    // reads both starting against the global host before either resolves.
    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async (input) => {
        const target = String(input);
        if (target === globalUrl) {
          return mockFetchResponse(301, new Uint8Array(), {
            "x-amz-bucket-region": "us-west-2",
          });
        }
        if (target === regionalUrl) return mockFetchResponse(206, body);
        throw new Error(`unexpected fetch URL: ${target}`);
      });
    const store = makeUrlStore({ url: globalUrl });

    // Both reads snapshot the global URL before either resolves its redirect;
    // each must detect and retry against the regional host independently.
    const [a, b] = await Promise.all([
      store.getRange("/", { offset: 0, length: 3 }),
      store.getRange("/", { offset: 0, length: 3 }),
    ]);

    expect(a).toEqual(body);
    expect(b).toEqual(body);

    fetchSpy.mockRestore();
  });

  it("rejects URL offset 206 responses with mismatched body length", async () => {
    const url = "https://example.com/data.bin";
    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(mockFetchResponse(206, new Uint8Array([0, 1, 2, 3])));
    const store = makeUrlStore({ url });

    await expect(
      store.getRange("/", { offset: 10, length: 2 }),
    ).rejects.toThrow(
      "Virtual range response size mismatch for https://example.com/data.bin: expected 2 bytes, got 4",
    );

    fetchSpy.mockRestore();
  });

  it("rejects URL suffix 206 responses with mismatched body length", async () => {
    const url = "https://example.com/data.bin";
    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(mockFetchResponse(206, new Uint8Array([7, 8, 9])));
    const store = makeUrlStore({ url });

    await expect(store.getRange("/", { suffixLength: 2 })).rejects.toThrow(
      "Virtual range response size mismatch for https://example.com/data.bin: expected 2 bytes, got 3",
    );

    fetchSpy.mockRestore();
  });

  it("slices URL suffix ranges from 200 full-body fallback responses", async () => {
    const url = "https://example.com/full.bin";
    const backing = makeBacking(10);
    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(mockFetchResponse(200, backing));
    const store = makeUrlStore({ url });

    const result = await store.getRange("/", { suffixLength: 3 });

    expect(result).toEqual(backing.slice(7));

    fetchSpy.mockRestore();
  });

  it("rejects undersized URL suffix 200 fallback responses", async () => {
    const url = "https://example.com/short.bin";
    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(mockFetchResponse(200, new Uint8Array([1, 2])));
    const store = makeUrlStore({ url });

    await expect(store.getRange("/", { suffixLength: 3 })).rejects.toThrow(
      "Virtual suffix range request not honored",
    );

    fetchSpy.mockRestore();
  });

  it("slices storage offset ranges when storage returns a full object", async () => {
    const backing = makeBacking(10);
    const storage = makeStorage(backing);
    const store = makeStorageStore(storage);

    const result = await store.getRange("chunks/abc", {
      offset: 2,
      length: 3,
    });

    expect(storage.getObject).toHaveBeenCalledWith(
      "chunks/abc",
      { start: 2, end: 5 },
      undefined,
    );
    expect(result).toEqual(backing.slice(2, 5));
  });

  it("rejects storage suffix ranges instead of downloading the full object", async () => {
    const storage = makeStorage(makeBacking(10));
    const store = makeStorageStore(storage);

    await expect(
      store.getRange("chunks/abc", { suffixLength: 3 }),
    ).rejects.toThrow(
      "Storage suffix ranges are not supported for chunks/abc; convert suffixLength to offset/length before reading",
    );
    expect(storage.getObject).not.toHaveBeenCalled();
  });
});
