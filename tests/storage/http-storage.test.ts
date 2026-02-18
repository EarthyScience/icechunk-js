import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { HttpStorage } from "../../src/storage/http-storage.js";
import {
  NotFoundError,
  StorageError,
  AbortError,
} from "../../src/storage/storage.js";

describe("HttpStorage", () => {
  const originalFetch = global.fetch;

  beforeEach(() => {
    global.fetch = vi.fn();
  });

  afterEach(() => {
    global.fetch = originalFetch;
  });

  describe("constructor", () => {
    it("should normalize trailing slash in base URL", async () => {
      const mockData = new Uint8Array([1, 2, 3]);
      vi.mocked(global.fetch).mockResolvedValue({
        status: 200,
        arrayBuffer: () => Promise.resolve(mockData.buffer),
      } as Response);

      const storage = new HttpStorage("https://example.com/repo/");
      await storage.getObject("file.txt");

      expect(global.fetch).toHaveBeenCalledWith(
        "https://example.com/repo/file.txt",
        expect.any(Object),
      );
    });
  });

  describe("getObject", () => {
    it("should fetch object from correct URL", async () => {
      const mockData = new Uint8Array([1, 2, 3]);
      vi.mocked(global.fetch).mockResolvedValue({
        status: 200,
        arrayBuffer: () => Promise.resolve(mockData.buffer),
      } as Response);

      const storage = new HttpStorage("https://example.com/repo");
      const data = await storage.getObject("path/to/file");

      expect(global.fetch).toHaveBeenCalledWith(
        "https://example.com/repo/path/to/file",
        expect.objectContaining({ method: "GET" }),
      );
      expect(data).toEqual(mockData);
    });

    it("should include Range header for partial reads", async () => {
      vi.mocked(global.fetch).mockResolvedValue({
        status: 206,
        arrayBuffer: () => Promise.resolve(new ArrayBuffer(10)),
      } as Response);

      const storage = new HttpStorage("https://example.com/repo");
      await storage.getObject("file", { start: 100, end: 200 });

      expect(global.fetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: expect.objectContaining({
            Range: "bytes=100-199",
          }),
        }),
      );
    });

    it("should throw NotFoundError on 404", async () => {
      vi.mocked(global.fetch).mockResolvedValue({
        status: 404,
      } as Response);

      const storage = new HttpStorage("https://example.com/repo");
      await expect(storage.getObject("missing")).rejects.toThrow(NotFoundError);
    });

    it("should throw StorageError on 500", async () => {
      vi.mocked(global.fetch).mockResolvedValue({
        status: 500,
        statusText: "Internal Server Error",
      } as Response);

      const storage = new HttpStorage("https://example.com/repo");
      await expect(storage.getObject("file")).rejects.toThrow(StorageError);
      await expect(storage.getObject("file")).rejects.toThrow("HTTP 500");
    });

    it("should throw StorageError on network failure", async () => {
      vi.mocked(global.fetch).mockRejectedValue(new Error("Network error"));

      const storage = new HttpStorage("https://example.com/repo");
      await expect(storage.getObject("file")).rejects.toThrow(StorageError);
    });

    it("should forward options to fetch", async () => {
      vi.mocked(global.fetch).mockResolvedValue({
        status: 200,
        arrayBuffer: () => Promise.resolve(new ArrayBuffer(0)),
      } as Response);

      const storage = new HttpStorage("https://example.com/repo", {
        headers: { Authorization: "Bearer token123" },
        credentials: "include",
        cache: "no-cache",
      });
      await storage.getObject("file");

      expect(global.fetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: expect.objectContaining({
            Authorization: "Bearer token123",
          }),
          credentials: "include",
          cache: "no-cache",
        }),
      );
    });
  });

  describe("exists", () => {
    it("should return true for existing object", async () => {
      vi.mocked(global.fetch).mockResolvedValue({ ok: true } as Response);

      const storage = new HttpStorage("https://example.com/repo");
      expect(await storage.exists("file")).toBe(true);
    });

    it("should return false for missing object", async () => {
      vi.mocked(global.fetch).mockResolvedValue({ ok: false } as Response);

      const storage = new HttpStorage("https://example.com/repo");
      expect(await storage.exists("missing")).toBe(false);
    });

    it("should use HEAD method", async () => {
      vi.mocked(global.fetch).mockResolvedValue({ ok: true } as Response);

      const storage = new HttpStorage("https://example.com/repo");
      await storage.exists("file");

      expect(global.fetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({ method: "HEAD" }),
      );
    });

    it("should return false on network error (not throw)", async () => {
      vi.mocked(global.fetch).mockRejectedValue(new Error("Network error"));

      const storage = new HttpStorage("https://example.com/repo");
      // exists() should catch errors and return false
      expect(await storage.exists("file")).toBe(false);
    });
  });

  describe("URL normalization", () => {
    it("should strip leading slash from path", async () => {
      vi.mocked(global.fetch).mockResolvedValue({
        status: 200,
        arrayBuffer: () => Promise.resolve(new ArrayBuffer(0)),
      } as Response);

      const storage = new HttpStorage("https://example.com/repo");
      await storage.getObject("/file/with/leading/slash");

      // Should normalize to single slash between base and path
      expect(global.fetch).toHaveBeenCalledWith(
        "https://example.com/repo/file/with/leading/slash",
        expect.any(Object),
      );
    });

    it("should handle paths without leading slash", async () => {
      vi.mocked(global.fetch).mockResolvedValue({
        status: 200,
        arrayBuffer: () => Promise.resolve(new ArrayBuffer(0)),
      } as Response);

      const storage = new HttpStorage("https://example.com/repo");
      await storage.getObject("file/without/leading/slash");

      expect(global.fetch).toHaveBeenCalledWith(
        "https://example.com/repo/file/without/leading/slash",
        expect.any(Object),
      );
    });
  });

  describe("listPrefix", () => {
    it("should throw StorageError (not supported)", async () => {
      const storage = new HttpStorage("https://example.com/repo");

      const collectItems = async () => {
        for await (const _ of storage.listPrefix("prefix/")) {
          // consume
        }
      };

      await expect(collectItems()).rejects.toThrow(StorageError);
      await expect(collectItems()).rejects.toThrow("Listing not supported");
    });
  });

  describe("abort signal handling", () => {
    it("should throw AbortError when signal is already aborted (getObject)", async () => {
      const storage = new HttpStorage("https://example.com/repo");
      const controller = new AbortController();
      controller.abort();

      await expect(
        storage.getObject("file.txt", undefined, { signal: controller.signal }),
      ).rejects.toThrow(AbortError);

      // fetch should not be called
      expect(global.fetch).not.toHaveBeenCalled();
    });

    it("should throw AbortError when fetch is aborted mid-request (getObject)", async () => {
      const abortError = new DOMException("Aborted", "AbortError");
      vi.mocked(global.fetch).mockRejectedValue(abortError);

      const storage = new HttpStorage("https://example.com/repo");
      const controller = new AbortController();

      await expect(
        storage.getObject("file.txt", undefined, { signal: controller.signal }),
      ).rejects.toThrow(AbortError);
    });

    it("should pass signal to fetch (getObject)", async () => {
      const mockData = new Uint8Array([1, 2, 3]);
      vi.mocked(global.fetch).mockResolvedValue({
        status: 200,
        arrayBuffer: () => Promise.resolve(mockData.buffer),
      } as Response);

      const storage = new HttpStorage("https://example.com/repo");
      const controller = new AbortController();

      await storage.getObject("file.txt", undefined, {
        signal: controller.signal,
      });

      expect(global.fetch).toHaveBeenCalledWith(
        "https://example.com/repo/file.txt",
        expect.objectContaining({ signal: controller.signal }),
      );
    });

    it("should throw AbortError when signal is already aborted (exists)", async () => {
      const storage = new HttpStorage("https://example.com/repo");
      const controller = new AbortController();
      controller.abort();

      await expect(
        storage.exists("file.txt", { signal: controller.signal }),
      ).rejects.toThrow(AbortError);

      expect(global.fetch).not.toHaveBeenCalled();
    });

    it("should throw AbortError when fetch is aborted mid-request (exists)", async () => {
      const abortError = new DOMException("Aborted", "AbortError");
      vi.mocked(global.fetch).mockRejectedValue(abortError);

      const storage = new HttpStorage("https://example.com/repo");
      const controller = new AbortController();

      await expect(
        storage.exists("file.txt", { signal: controller.signal }),
      ).rejects.toThrow(AbortError);
    });

    it("should handle non-DOMException abort errors", async () => {
      // Some fetch implementations throw plain Error with name 'AbortError'
      const abortError = new Error("Aborted");
      abortError.name = "AbortError";
      vi.mocked(global.fetch).mockRejectedValue(abortError);

      const storage = new HttpStorage("https://example.com/repo");
      const controller = new AbortController();

      await expect(
        storage.getObject("file.txt", undefined, { signal: controller.signal }),
      ).rejects.toThrow(AbortError);
    });
  });
});
