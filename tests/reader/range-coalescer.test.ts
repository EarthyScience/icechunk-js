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

function mockFetchResponse(status: number, data: Uint8Array): Response {
  return {
    status,
    statusText: status === 206 ? "Partial Content" : "OK",
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
});
