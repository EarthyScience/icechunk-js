/**
 * Integration tests with zarrita library.
 *
 * Uses test-repo-v1 and test-repo-v2 from icechunk-python/tests/data which contain:
 * - group1/big_chunks: 2D float32 array, shape=(10,10), chunks=(5,5), filled with 42.0
 * - group1/small_chunks: 1D int8 array, shape=(5,), chunks=(1,), filled with 84
 * - group2/group3/group4/group5/inner: 2D float32 array (only on my-branch)
 *
 * Branches: main, my-branch
 * Tags: "it works!", "it also works!"
 *
 * Also tests:
 * - split-repo-v1/v2: Manifest splitting with group1/split (10x10 float32, 3x3 chunks)
 * - test-repo-v2-migrated: V1-to-V2 migrated repository
 * - Virtual chunk resolution through fetchClient
 */

import { describe, it, expect, vi, afterEach } from "vitest";
import * as z from "zarrita";
import { IcechunkStore } from "../src/index.js";
import type { AbsolutePath } from "../src/store.js";
import { getFixtureUrl } from "./helpers.js";

describe("Zarrita Integration", () => {
  describe("v1 format", () => {
    describe("read array data", () => {
      it("should read array with correct shape, dtype, and values", async () => {
        const store = await IcechunkStore.open(getFixtureUrl("test-repo-v1"));
        const location = z.root(store).resolve("/group1/small_chunks");
        const arr = await z.open(location, { kind: "array" });

        expect(arr.shape).toEqual([5]);
        expect(arr.dtype).toBe("int8");
        expect(arr.chunks).toEqual([1]);

        const data = await z.get(arr);
        expect(data.data.length).toBe(5);
        for (let i = 0; i < 5; i++) {
          expect(data.data[i]).toBe(84);
        }
      });

      it("should read 2D array slice", async () => {
        const store = await IcechunkStore.open(getFixtureUrl("test-repo-v1"));
        const location = z.root(store).resolve("/group1/big_chunks");
        const arr = await z.open(location, { kind: "array" });

        // Read rows 5-10 (avoids virtual chunk [0,0])
        const data = await z.get(arr, [{ start: 5, stop: 10 }, null]);

        expect(data.data.length).toBe(50);
        for (let i = 0; i < 50; i++) {
          expect(data.data[i]).toBe(42.0);
        }
      });
    });

    describe("groups", () => {
      it("should open root group", async () => {
        const store = await IcechunkStore.open(getFixtureUrl("test-repo-v1"));
        const group = await z.open(store, { kind: "group" });
        expect(group).toBeDefined();
      });

      it("should open deeply nested structure on branch", async () => {
        const store = await IcechunkStore.open(getFixtureUrl("test-repo-v1"), {
          branch: "my-branch",
        });
        const location = z
          .root(store)
          .resolve("/group2/group3/group4/group5/inner");
        const arr = await z.open(location, { kind: "array" });

        expect(arr.shape).toEqual([10, 10]);
        expect(arr.dtype).toBe("float32");
      });
    });

    describe("time travel", () => {
      it("should access different structure on different branches", async () => {
        // main branch does NOT have group2
        const storeMain = await IcechunkStore.open(
          getFixtureUrl("test-repo-v1"),
          {
            branch: "main",
          },
        );
        const locationMain = z.root(storeMain).resolve("/group2");
        await expect(z.open(locationMain, { kind: "group" })).rejects.toThrow();

        // my-branch DOES have group2
        const storeBranch = await IcechunkStore.open(
          getFixtureUrl("test-repo-v1"),
          {
            branch: "my-branch",
          },
        );
        const locationBranch = z.root(storeBranch).resolve("/group2");
        const group = await z.open(locationBranch, { kind: "group" });
        expect(group).toBeDefined();
      });

      it("should open different snapshots via tags", async () => {
        const store1 = await IcechunkStore.open(getFixtureUrl("test-repo-v1"), {
          tag: "it works!",
        });
        const store2 = await IcechunkStore.open(getFixtureUrl("test-repo-v1"), {
          tag: "it also works!",
        });

        // Both should open successfully (they point to different snapshots)
        const group1 = await z.open(store1, { kind: "group" });
        const group2 = await z.open(store2, { kind: "group" });

        expect(group1).toBeDefined();
        expect(group2).toBeDefined();
      });
    });
  });

  describe("v2 format", () => {
    describe("read array data", () => {
      it("should read array with correct shape, dtype, and values", async () => {
        const store = await IcechunkStore.open(getFixtureUrl("test-repo-v2"));
        const location = z.root(store).resolve("/group1/small_chunks");
        const arr = await z.open(location, { kind: "array" });

        expect(arr.shape).toEqual([5]);
        expect(arr.dtype).toBe("int8");
        expect(arr.chunks).toEqual([1]);

        const data = await z.get(arr);
        expect(data.data.length).toBe(5);
        for (let i = 0; i < 5; i++) {
          expect(data.data[i]).toBe(84);
        }
      });

      it("should read 2D array slice", async () => {
        const store = await IcechunkStore.open(getFixtureUrl("test-repo-v2"));
        const location = z.root(store).resolve("/group1/big_chunks");
        const arr = await z.open(location, { kind: "array" });

        // Read rows 5-10 (avoids virtual chunk [0,0])
        const data = await z.get(arr, [{ start: 5, stop: 10 }, null]);

        expect(data.data.length).toBe(50);
        for (let i = 0; i < 50; i++) {
          expect(data.data[i]).toBe(42.0);
        }
      });
    });

    describe("groups", () => {
      it("should open root group", async () => {
        const store = await IcechunkStore.open(getFixtureUrl("test-repo-v2"));
        const group = await z.open(store, { kind: "group" });
        expect(group).toBeDefined();
      });

      it("should open deeply nested structure on branch", async () => {
        const store = await IcechunkStore.open(getFixtureUrl("test-repo-v2"), {
          branch: "my-branch",
        });
        const location = z
          .root(store)
          .resolve("/group2/group3/group4/group5/inner");
        const arr = await z.open(location, { kind: "array" });

        expect(arr.shape).toEqual([10, 10]);
        expect(arr.dtype).toBe("float32");
      });
    });

    describe("time travel", () => {
      it("should access different structure on different branches", async () => {
        // main branch does NOT have group2
        const storeMain = await IcechunkStore.open(
          getFixtureUrl("test-repo-v2"),
          {
            branch: "main",
          },
        );
        const locationMain = z.root(storeMain).resolve("/group2");
        await expect(z.open(locationMain, { kind: "group" })).rejects.toThrow();

        // my-branch DOES have group2
        const storeBranch = await IcechunkStore.open(
          getFixtureUrl("test-repo-v2"),
          {
            branch: "my-branch",
          },
        );
        const locationBranch = z.root(storeBranch).resolve("/group2");
        const group = await z.open(locationBranch, { kind: "group" });
        expect(group).toBeDefined();
      });

      it("should open different snapshots via tags", async () => {
        const store1 = await IcechunkStore.open(getFixtureUrl("test-repo-v2"), {
          tag: "it works!",
        });
        const store2 = await IcechunkStore.open(getFixtureUrl("test-repo-v2"), {
          tag: "it also works!",
        });

        // Both should open successfully (they point to different snapshots)
        const group1 = await z.open(store1, { kind: "group" });
        const group2 = await z.open(store2, { kind: "group" });

        expect(group1).toBeDefined();
        expect(group2).toBeDefined();
      });
    });
  });

  describe("v2 migrated format (v1 -> v2 migration)", () => {
    it("should read small_chunks array", async () => {
      const store = await IcechunkStore.open(
        getFixtureUrl("test-repo-v2-migrated"),
      );
      const location = z.root(store).resolve("/group1/small_chunks");
      const arr = await z.open(location, { kind: "array" });

      expect(arr.shape).toEqual([5]);
      expect(arr.dtype).toBe("int8");

      const data = await z.get(arr);
      expect(data.data.length).toBe(5);
      for (let i = 0; i < 5; i++) {
        expect(data.data[i]).toBe(84);
      }
    });

    it("should read 2D array slice (non-virtual chunks)", async () => {
      const store = await IcechunkStore.open(
        getFixtureUrl("test-repo-v2-migrated"),
      );
      const location = z.root(store).resolve("/group1/big_chunks");
      const arr = await z.open(location, { kind: "array" });

      expect(arr.shape).toEqual([10, 10]);

      // Read rows 5-10 (avoids virtual chunk [0,0])
      const data = await z.get(arr, [{ start: 5, stop: 10 }, null]);
      expect(data.data.length).toBe(50);
      for (let i = 0; i < 50; i++) {
        expect(data.data[i]).toBe(42.0);
      }
    });

    it("should open root group", async () => {
      const store = await IcechunkStore.open(
        getFixtureUrl("test-repo-v2-migrated"),
      );
      const group = await z.open(store, { kind: "group" });
      expect(group).toBeDefined();
    });

    it("should access branch structure after migration", async () => {
      const store = await IcechunkStore.open(
        getFixtureUrl("test-repo-v2-migrated"),
        { branch: "my-branch" },
      );
      const location = z
        .root(store)
        .resolve("/group2/group3/group4/group5/inner");
      const arr = await z.open(location, { kind: "array" });

      expect(arr.shape).toEqual([10, 10]);
      expect(arr.dtype).toBe("float32");
    });

    it("should access tags after migration", async () => {
      const store1 = await IcechunkStore.open(
        getFixtureUrl("test-repo-v2-migrated"),
        { tag: "it works!" },
      );
      const store2 = await IcechunkStore.open(
        getFixtureUrl("test-repo-v2-migrated"),
        { tag: "it also works!" },
      );

      const group1 = await z.open(store1, { kind: "group" });
      const group2 = await z.open(store2, { kind: "group" });

      expect(group1).toBeDefined();
      expect(group2).toBeDefined();
    });
  });

  describe("split manifests", () => {
    /**
     * split-repo-v1/v2 contain:
     * - group1/split: 2D float32, shape=(10,10), chunks=(3,3), filled with 14
     * - group1/small_chunks: 1D int8, shape=(5,), chunks=(1,), filled with 3
     *
     * These repos test manifest splitting where chunks are distributed
     * across multiple manifest files (17 at the latest commit).
     */

    describe("v1 format", () => {
      it("should read split array through multiple manifests", async () => {
        const store = await IcechunkStore.open(getFixtureUrl("split-repo-v1"));
        const location = z.root(store).resolve("/group1/split");
        const arr = await z.open(location, { kind: "array" });

        expect(arr.shape).toEqual([10, 10]);
        expect(arr.dtype).toBe("float32");
        expect(arr.chunks).toEqual([3, 3]);

        const data = await z.get(arr);
        expect(data.data.length).toBe(100);
        for (let i = 0; i < 100; i++) {
          expect(data.data[i]).toBe(14);
        }
      });

      it("should read inline small_chunks", async () => {
        const store = await IcechunkStore.open(getFixtureUrl("split-repo-v1"));
        const location = z.root(store).resolve("/group1/small_chunks");
        const arr = await z.open(location, { kind: "array" });

        expect(arr.shape).toEqual([5]);
        expect(arr.dtype).toBe("int8");

        const data = await z.get(arr);
        expect(data.data.length).toBe(5);
        for (let i = 0; i < 5; i++) {
          expect(data.data[i]).toBe(3);
        }
      });

      it("should open root group", async () => {
        const store = await IcechunkStore.open(getFixtureUrl("split-repo-v1"));
        const group = await z.open(store, { kind: "group" });
        expect(group).toBeDefined();
      });
    });

    describe("v2 format", () => {
      it("should read split array through multiple manifests", async () => {
        const store = await IcechunkStore.open(getFixtureUrl("split-repo-v2"));
        const location = z.root(store).resolve("/group1/split");
        const arr = await z.open(location, { kind: "array" });

        expect(arr.shape).toEqual([10, 10]);
        expect(arr.dtype).toBe("float32");
        expect(arr.chunks).toEqual([3, 3]);

        const data = await z.get(arr);
        expect(data.data.length).toBe(100);
        for (let i = 0; i < 100; i++) {
          expect(data.data[i]).toBe(14);
        }
      });

      it("should read inline small_chunks", async () => {
        const store = await IcechunkStore.open(getFixtureUrl("split-repo-v2"));
        const location = z.root(store).resolve("/group1/small_chunks");
        const arr = await z.open(location, { kind: "array" });

        expect(arr.shape).toEqual([5]);
        expect(arr.dtype).toBe("int8");

        const data = await z.get(arr);
        expect(data.data.length).toBe(5);
        for (let i = 0; i < 5; i++) {
          expect(data.data[i]).toBe(3);
        }
      });
    });

    describe("v2 migrated format", () => {
      it("should read split array from migrated repo", async () => {
        const store = await IcechunkStore.open(
          getFixtureUrl("split-repo-v2-migrated"),
        );
        const location = z.root(store).resolve("/group1/split");
        const arr = await z.open(location, { kind: "array" });

        expect(arr.shape).toEqual([10, 10]);
        expect(arr.dtype).toBe("float32");

        const data = await z.get(arr);
        expect(data.data.length).toBe(100);
        for (let i = 0; i < 100; i++) {
          expect(data.data[i]).toBe(14);
        }
      });

      it("should read inline small_chunks from migrated repo", async () => {
        const store = await IcechunkStore.open(
          getFixtureUrl("split-repo-v2-migrated"),
        );
        const location = z.root(store).resolve("/group1/small_chunks");
        const arr = await z.open(location, { kind: "array" });

        expect(arr.shape).toEqual([5]);
        expect(arr.dtype).toBe("int8");

        const data = await z.get(arr);
        expect(data.data.length).toBe(5);
        for (let i = 0; i < 5; i++) {
          expect(data.data[i]).toBe(3);
        }
      });
    });
  });

  describe("virtual chunks", () => {
    /**
     * End-to-end tests for virtual chunk resolution.
     *
     * Chunk [0,0] of big_chunks in test-repo-v1/v2 is a virtual reference
     * pointing to s3://testbucket/can_read_old/chunk-1. These tests mock the
     * virtual chunk fetch to verify the full resolution stack:
     *   repo -> snapshot -> manifest -> virtual ref detection -> fetch -> zarrita
     *
     * The mock serves the same codec-encoded bytes as a materialized chunk
     * (read from [0,1]) since all chunks contain the same data (42.0).
     */

    afterEach(() => {
      vi.restoreAllMocks();
    });

    /**
     * Helper: Read a materialized chunk's raw bytes to use as virtual chunk mock data.
     * The chunks are codec-encoded (compressed), so we can't construct them from scratch.
     */
    async function getChunkBytesForMocking(
      repoUrl: string,
    ): Promise<Uint8Array> {
      const store = await IcechunkStore.open(repoUrl);
      // Read materialized chunk [0,1] — same data as virtual chunk [0,0]
      const rawBytes = await store.get(
        "/group1/big_chunks/c/0/1" as AbsolutePath,
      );
      if (!rawBytes) throw new Error("Failed to read materialized chunk");
      return rawBytes;
    }

    it("should resolve virtual chunk through full stack (v1)", async () => {
      // Get real chunk bytes (codec-encoded) from a materialized chunk
      const chunkBytes = await getChunkBytesForMocking(
        getFixtureUrl("test-repo-v1"),
      );

      // Intercept fetch: serve virtual chunk URL, pass through fixture server
      const originalFetch = globalThis.fetch;
      vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
        const url = typeof input === "string" ? input : input.toString();
        if (url.includes("testbucket.s3.amazonaws.com")) {
          return new Response(chunkBytes.buffer.slice(0), {
            status: 206,
            headers: { "Content-Type": "application/octet-stream" },
          });
        }
        return originalFetch(input, init);
      });

      const store = await IcechunkStore.open(getFixtureUrl("test-repo-v1"));
      const location = z.root(store).resolve("/group1/big_chunks");
      const arr = await z.open(location, { kind: "array" });

      // Read ALL data including virtual chunk [0,0]
      const data = await z.get(arr);

      expect(data.data.length).toBe(100);
      for (let i = 0; i < 100; i++) {
        expect(data.data[i]).toBe(42.0);
      }
    });

    it("should resolve virtual chunk through full stack (v2)", async () => {
      const chunkBytes = await getChunkBytesForMocking(
        getFixtureUrl("test-repo-v2"),
      );

      const originalFetch = globalThis.fetch;
      vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
        const url = typeof input === "string" ? input : input.toString();
        if (url.includes("testbucket.s3.amazonaws.com")) {
          return new Response(chunkBytes.buffer.slice(0), {
            status: 206,
            headers: { "Content-Type": "application/octet-stream" },
          });
        }
        return originalFetch(input, init);
      });

      const store = await IcechunkStore.open(getFixtureUrl("test-repo-v2"));
      const location = z.root(store).resolve("/group1/big_chunks");
      const arr = await z.open(location, { kind: "array" });

      const data = await z.get(arr);

      expect(data.data.length).toBe(100);
      for (let i = 0; i < 100; i++) {
        expect(data.data[i]).toBe(42.0);
      }
    });

    it("should use fetchClient to intercept virtual chunk fetch", async () => {
      const chunkBytes = await getChunkBytesForMocking(
        getFixtureUrl("test-repo-v1"),
      );

      const originalFetch = globalThis.fetch;

      // FetchClient intercepts S3 virtual chunk URLs and serves mock data.
      // No need to mock globalThis.fetch — fetchClient only applies to virtual chunks.
      const fetchClient = {
        fetch: vi
          .fn()
          .mockImplementation(async (url: string, init?: RequestInit) => {
            if (url.includes("testbucket.s3.amazonaws.com")) {
              return new Response(chunkBytes.buffer.slice(0), {
                status: 206,
                headers: { "Content-Type": "application/octet-stream" },
              });
            }
            // Should not reach here for virtual chunk URLs
            return originalFetch(url, init);
          }),
      };

      const store = await IcechunkStore.open(getFixtureUrl("test-repo-v1"), {
        fetchClient,
      });
      const location = z.root(store).resolve("/group1/big_chunks");
      const arr = await z.open(location, { kind: "array" });

      // Read rows 0-5 which include the virtual chunk [0,0]
      const data = await z.get(arr, [{ start: 0, stop: 5 }, null]);

      expect(data.data.length).toBe(50);
      for (let i = 0; i < 50; i++) {
        expect(data.data[i]).toBe(42.0);
      }

      // Verify fetchClient.fetch was called with the translated S3 URL
      expect(fetchClient.fetch).toHaveBeenCalledWith(
        expect.stringContaining("testbucket.s3.amazonaws.com"),
        expect.objectContaining({
          headers: expect.objectContaining({
            Range: expect.any(String),
          }),
        }),
      );
    });
  });
});
