import { describe, it, expect, vi, beforeEach } from "vitest";
import { IcechunkStore } from "../src/store.js";
import type { AbsolutePath } from "../src/store.js";
import { MockStorage } from "./fixtures/mock-storage.js";
import { NotFoundError } from "../src/storage/storage.js";

/**
 * Helper to create an IcechunkStore with a mock session.
 */
function createStoreWithMockSession(
  mockSession: {
    getRawMetadata: ReturnType<typeof vi.fn>;
    getChunk: ReturnType<typeof vi.fn>;
    getChunkRange?: ReturnType<typeof vi.fn>;
  },
  options: { rangeCoalescing?: boolean } = {},
): IcechunkStore {
  const store = Object.create(IcechunkStore.prototype);
  store.session = mockSession;
  if (options.rangeCoalescing !== undefined) {
    store.rangeCoalescing = options.rangeCoalescing;
  }
  return store;
}

describe("IcechunkStore", () => {
  describe("open", () => {
    it("should throw on open() when repository is invalid", async () => {
      const storage = new MockStorage({});

      await expect(IcechunkStore.open(storage)).rejects.toThrow(
        "Not a valid icechunk repository",
      );
    });
  });

  describe("get with mock session", () => {
    let getRawMetadataSpy: ReturnType<typeof vi.fn>;
    let getChunkSpy: ReturnType<typeof vi.fn>;
    let store: IcechunkStore;

    beforeEach(() => {
      getRawMetadataSpy = vi.fn();
      getChunkSpy = vi.fn();

      store = createStoreWithMockSession({
        getRawMetadata: getRawMetadataSpy,
        getChunk: getChunkSpy,
      });
    });

    describe("metadata keys", () => {
      it("should parse root metadata key /zarr.json", async () => {
        getRawMetadataSpy.mockReturnValue(new Uint8Array([1, 2, 3]));

        const result = await store.get("/zarr.json" as AbsolutePath);

        expect(getRawMetadataSpy).toHaveBeenCalledWith("/");
        expect(result).toEqual(new Uint8Array([1, 2, 3]));
      });

      it("should parse nested metadata key", async () => {
        getRawMetadataSpy.mockReturnValue(new Uint8Array([4, 5, 6]));

        const result = await store.get(
          "/group/subgroup/array/zarr.json" as AbsolutePath,
        );

        expect(getRawMetadataSpy).toHaveBeenCalledWith("/group/subgroup/array");
        expect(result).toEqual(new Uint8Array([4, 5, 6]));
      });

      it("should return undefined for missing metadata", async () => {
        getRawMetadataSpy.mockReturnValue(null);

        const result = await store.get("/missing/zarr.json" as AbsolutePath);

        expect(result).toBeUndefined();
      });
    });

    describe("chunk keys", () => {
      it("should parse chunk key with coordinates", async () => {
        getChunkSpy.mockResolvedValue(new Uint8Array([10, 20, 30]));

        const result = await store.get("/array/c/1/2/3" as AbsolutePath);

        expect(getChunkSpy).toHaveBeenCalledWith("/array", [1, 2, 3], {
          signal: undefined,
        });
        expect(result).toEqual(new Uint8Array([10, 20, 30]));
      });

      it("should parse chunk key with empty coordinates", async () => {
        getChunkSpy.mockResolvedValue(new Uint8Array([100]));

        const result = await store.get("/array/c" as AbsolutePath);

        expect(getChunkSpy).toHaveBeenCalledWith("/array", [], {
          signal: undefined,
        });
        expect(result).toEqual(new Uint8Array([100]));
      });

      it("should pass rangeCoalescing through when enabled", async () => {
        getChunkSpy.mockResolvedValue(new Uint8Array([10]));
        store = createStoreWithMockSession(
          {
            getRawMetadata: getRawMetadataSpy,
            getChunk: getChunkSpy,
          },
          { rangeCoalescing: true },
        );

        await store.get("/array/c/1" as AbsolutePath);

        expect(getChunkSpy).toHaveBeenCalledWith(
          "/array",
          [1],
          expect.objectContaining({ rangeCoalescing: true }),
        );
      });

      it("should return undefined for missing chunks", async () => {
        getChunkSpy.mockResolvedValue(null);

        const result = await store.get("/array/c/999" as AbsolutePath);

        expect(result).toBeUndefined();
      });
    });

    describe("error handling", () => {
      it("should return undefined for NotFoundError (legitimate missing key)", async () => {
        getRawMetadataSpy.mockImplementation(() => {
          throw new NotFoundError("/array/zarr.json");
        });

        const result = await store.get("/array/zarr.json" as AbsolutePath);

        expect(result).toBeUndefined();
      });

      it("should propagate non-NotFoundError failures", async () => {
        // Swallowing these would cause zarrita's Array.getChunk to fall back
        // to a fillValue chunk silently — downstream consumers then cache
        // that garbage as if it were real data.
        getRawMetadataSpy.mockImplementation(() => {
          throw new Error("Storage error");
        });

        await expect(
          store.get("/array/zarr.json" as AbsolutePath),
        ).rejects.toThrow("Storage error");
      });

      it("should propagate AbortError so consumers don't cache garbage", async () => {
        const abortErr = new Error("Operation aborted");
        abortErr.name = "AbortError";
        getRawMetadataSpy.mockImplementation(() => {
          throw abortErr;
        });

        await expect(
          store.get("/array/zarr.json" as AbsolutePath),
        ).rejects.toThrow("Operation aborted");
      });
    });

    describe("key parsing edge cases", () => {
      it("should treat unrecognized keys as metadata (default fallback)", async () => {
        getRawMetadataSpy.mockReturnValue(new Uint8Array([99]));

        // Key that doesn't match zarr.json or chunk pattern
        const result = await store.get("/some/random/path" as AbsolutePath);

        // Should fall back to metadata lookup at that path
        expect(getRawMetadataSpy).toHaveBeenCalledWith("/some/random/path");
        expect(result).toEqual(new Uint8Array([99]));
      });

      it("should handle root-level chunk key", async () => {
        getChunkSpy.mockResolvedValue(new Uint8Array([42]));

        // Root array chunk: /c/0
        const result = await store.get("/c/0" as AbsolutePath);

        expect(getChunkSpy).toHaveBeenCalledWith("/", [0], {
          signal: undefined,
        });
        expect(result).toEqual(new Uint8Array([42]));
      });

      it("should parse NaN coordinates as NaN values", async () => {
        getChunkSpy.mockResolvedValue(new Uint8Array([1]));

        // Invalid coordinate that parses to NaN
        await store.get("/array/c/invalid" as AbsolutePath);

        // Number('invalid') = NaN, verify the behavior
        expect(getChunkSpy).toHaveBeenCalledWith("/array", [NaN], {
          signal: undefined,
        });
      });

      it("should handle mixed valid and invalid coordinates", async () => {
        getChunkSpy.mockResolvedValue(new Uint8Array([1]));

        await store.get("/array/c/0/abc/2" as AbsolutePath);

        // [0, NaN, 2]
        const call = getChunkSpy.mock.calls[0];
        expect(call[0]).toBe("/array");
        expect(call[1][0]).toBe(0);
        expect(Number.isNaN(call[1][1])).toBe(true);
        expect(call[1][2]).toBe(2);
      });
    });
  });

  describe("getRange", () => {
    it("should use getChunkRange for chunk keys", async () => {
      const getChunkSpy = vi.fn();
      const getChunkRangeSpy = vi
        .fn()
        .mockResolvedValue(new Uint8Array([7, 8]));

      const store = createStoreWithMockSession({
        getRawMetadata: vi.fn(),
        getChunk: getChunkSpy,
        getChunkRange: getChunkRangeSpy,
      });

      const result = await store.getRange("/array/c/1/2" as AbsolutePath, {
        offset: 10,
        length: 5,
      });

      expect(getChunkRangeSpy).toHaveBeenCalledWith(
        "/array",
        [1, 2],
        { offset: 10, length: 5 },
        { signal: undefined },
      );
      expect(getChunkSpy).not.toHaveBeenCalled();
      expect(result).toEqual(new Uint8Array([7, 8]));
    });

    it("should use getChunkRange with suffixLength for shard index reads", async () => {
      const getChunkRangeSpy = vi
        .fn()
        .mockResolvedValue(new Uint8Array([1, 2, 3, 4]));

      const store = createStoreWithMockSession({
        getRawMetadata: vi.fn(),
        getChunk: vi.fn(),
        getChunkRange: getChunkRangeSpy,
      });

      const result = await store.getRange("/array/c/0/0" as AbsolutePath, {
        suffixLength: 20,
      });

      expect(getChunkRangeSpy).toHaveBeenCalledWith(
        "/array",
        [0, 0],
        { suffixLength: 20 },
        { signal: undefined },
      );
      expect(result).toEqual(new Uint8Array([1, 2, 3, 4]));
    });

    it("should return sliced data for offset/length range", async () => {
      const getRawMetadataSpy = vi
        .fn()
        .mockReturnValue(new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]));

      const store = createStoreWithMockSession({
        getRawMetadata: getRawMetadataSpy,
        getChunk: vi.fn(),
      });

      const result = await store.getRange("/zarr.json" as AbsolutePath, {
        offset: 2,
        length: 4,
      });

      expect(result).toEqual(new Uint8Array([2, 3, 4, 5]));
    });

    it("should return sliced data for suffixLength range", async () => {
      const getRawMetadataSpy = vi
        .fn()
        .mockReturnValue(new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]));

      const store = createStoreWithMockSession({
        getRawMetadata: getRawMetadataSpy,
        getChunk: vi.fn(),
      });

      const result = await store.getRange("/zarr.json" as AbsolutePath, {
        suffixLength: 3,
      });

      expect(result).toEqual(new Uint8Array([7, 8, 9]));
    });

    it("should return undefined when underlying get returns undefined", async () => {
      const store = createStoreWithMockSession({
        getRawMetadata: vi.fn().mockReturnValue(null),
        getChunk: vi.fn(),
      });

      const result = await store.getRange(
        "/missing/zarr.json" as AbsolutePath,
        {
          offset: 0,
          length: 5,
        },
      );

      expect(result).toBeUndefined();
    });

    it("should reject when signal is already aborted", async () => {
      const store = createStoreWithMockSession({
        getRawMetadata: vi.fn(),
        getChunk: vi.fn(),
      });

      const controller = new AbortController();
      controller.abort();

      await expect(
        store.getRange(
          "/zarr.json" as AbsolutePath,
          { offset: 0, length: 3 },
          { signal: controller.signal },
        ),
      ).rejects.toMatchObject({ name: "AbortError" });
    });
  });

  describe("listChildren trailing-slash normalization", () => {
    it("should strip trailing slash when basePath is set and parentPath is '/'", () => {
      const listChildrenSpy = vi.fn().mockReturnValue([]);
      const store = createStoreWithMockSession({
        getRawMetadata: vi.fn(),
        getChunk: vi.fn(),
      });
      // Manually set basePath and session methods to simulate resolve("group")
      (store as unknown as { basePath: string }).basePath = "group";
      (
        store.session as unknown as { listChildren: typeof listChildrenSpy }
      ).listChildren = listChildrenSpy;

      store.listChildren("/");

      // Should be "/group", NOT "/group/"
      expect(listChildrenSpy).toHaveBeenCalledWith("/group");
    });

    it("should strip trailing slash when parentPath has trailing slash", () => {
      const listChildrenSpy = vi.fn().mockReturnValue([]);
      const store = createStoreWithMockSession({
        getRawMetadata: vi.fn(),
        getChunk: vi.fn(),
      });
      (store as unknown as { basePath: string }).basePath = "group";
      (
        store.session as unknown as { listChildren: typeof listChildrenSpy }
      ).listChildren = listChildrenSpy;

      store.listChildren("sub/");

      expect(listChildrenSpy).toHaveBeenCalledWith("/group/sub");
    });

    it("should strip trailing slash without basePath", () => {
      const listChildrenSpy = vi.fn().mockReturnValue([]);
      const store = createStoreWithMockSession({
        getRawMetadata: vi.fn(),
        getChunk: vi.fn(),
      });
      (store as unknown as { basePath: string }).basePath = "";
      (
        store.session as unknown as { listChildren: typeof listChildrenSpy }
      ).listChildren = listChildrenSpy;

      store.listChildren("/group/");

      expect(listChildrenSpy).toHaveBeenCalledWith("/group");
    });

    it("should preserve root path '/' when parentPath is '/' and no basePath", () => {
      const listChildrenSpy = vi.fn().mockReturnValue([]);
      const store = createStoreWithMockSession({
        getRawMetadata: vi.fn(),
        getChunk: vi.fn(),
      });
      (store as unknown as { basePath: string }).basePath = "";
      (
        store.session as unknown as { listChildren: typeof listChildrenSpy }
      ).listChildren = listChildrenSpy;

      store.listChildren("/");

      expect(listChildrenSpy).toHaveBeenCalledWith("/");
    });
  });

  describe("abort signal handling", () => {
    it("should reject when signal is already aborted", async () => {
      const getChunkSpy = vi.fn();
      const store = createStoreWithMockSession({
        getRawMetadata: vi.fn(),
        getChunk: getChunkSpy,
      });

      const controller = new AbortController();
      controller.abort();

      await expect(
        store.get("/array/c/0" as AbsolutePath, {
          signal: controller.signal,
        }),
      ).rejects.toMatchObject({ name: "AbortError" });

      expect(getChunkSpy).not.toHaveBeenCalled();
    });

    it("should pass signal to getChunk", async () => {
      const getChunkSpy = vi.fn().mockResolvedValue(new Uint8Array([1, 2, 3]));
      const store = createStoreWithMockSession({
        getRawMetadata: vi.fn(),
        getChunk: getChunkSpy,
      });

      const controller = new AbortController();

      await store.get("/array/c/0" as AbsolutePath, {
        signal: controller.signal,
      });

      expect(getChunkSpy).toHaveBeenCalledWith("/array", [0], {
        signal: controller.signal,
      });
    });

    it("should return undefined when getChunk returns null for a missing chunk", async () => {
      const getChunkSpy = vi.fn().mockResolvedValue(null);
      const store = createStoreWithMockSession({
        getRawMetadata: vi.fn(),
        getChunk: getChunkSpy,
      });

      const controller = new AbortController();

      const result = await store.get("/array/c/0" as AbsolutePath, {
        signal: controller.signal,
      });

      expect(result).toBeUndefined();
    });
  });
});
