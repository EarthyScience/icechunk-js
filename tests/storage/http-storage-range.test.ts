/**
 * `HttpStorage.getObject` with a byte range: a 206 must cover the requested
 * span (when the server says which it sent), and a 200 full body is cut down
 * to the requested window so callers always receive exactly the range.
 */

import { describe, it, expect, vi, afterEach } from "vitest";
import { HttpStorage } from "../../src/storage/http-storage.js";
import { StorageError } from "../../src/storage/storage.js";

function response(
  status: number,
  body: Uint8Array,
  headers: Record<string, string> = {},
): Response {
  return {
    status,
    statusText: status === 206 ? "Partial Content" : "OK",
    headers: { get: (name: string) => headers[name.toLowerCase()] ?? null },
    arrayBuffer: () =>
      Promise.resolve(
        body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength),
      ),
  } as unknown as Response;
}

describe("HttpStorage ranged reads", () => {
  afterEach(() => vi.restoreAllMocks());

  it("returns a 206 body whose Content-Range matches the request", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      response(206, new Uint8Array([5, 6, 7]), {
        "content-range": "bytes 5-7/100",
      }),
    );
    const storage = new HttpStorage("https://example.com/repo");

    expect(await storage.getObject("file", { start: 5, end: 8 })).toEqual(
      new Uint8Array([5, 6, 7]),
    );
  });

  it("rejects a 206 whose Content-Range is a different span", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      response(206, new Uint8Array([9, 9, 9]), {
        "content-range": "bytes 50-52/100",
      }),
    );
    const storage = new HttpStorage("https://example.com/repo");

    await expect(
      storage.getObject("file", { start: 5, end: 8 }),
    ).rejects.toThrow(
      "Range response for https://example.com/repo/file covers bytes 50-52, not the requested 5-7",
    );
  });

  it("cuts a 200 full body down to the requested window, as a copy", async () => {
    const full = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    vi.spyOn(globalThis, "fetch").mockResolvedValue(response(200, full));
    const storage = new HttpStorage("https://example.com/repo");

    const data = await storage.getObject("file", { start: 3, end: 6 });

    expect(data).toEqual(new Uint8Array([3, 4, 5]));
    expect(data.buffer.byteLength).toBe(3);
  });

  it("rejects a 200 body too short to contain the requested window", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      response(200, new Uint8Array([0, 1, 2])),
    );
    const storage = new HttpStorage("https://example.com/repo");

    await expect(
      storage.getObject("file", { start: 3, end: 6 }),
    ).rejects.toThrow(StorageError);
  });

  it("records the HTTP status on StorageError", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      response(403, new Uint8Array(0)),
    );
    const storage = new HttpStorage("https://example.com/repo");

    await expect(storage.getObject("file")).rejects.toMatchObject({
      name: "StorageError",
      status: 403,
    });
  });
});
