import { describe, it, expect, vi } from "vitest";
import { readFileSync, readdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { ReadSession } from "../../src/reader/session.js";
import { Repository } from "../../src/reader/repository.js";
import {
  MockStorage,
  createMockHeader,
  createMockSnapshotId,
  createMockNodeId,
} from "../fixtures/mock-storage.js";
import { getSnapshotPath } from "../../src/format/constants.js";
import { encodeObjectId12 } from "../../src/format/object-id.js";
import { FileType, SpecVersion } from "../../src/format/header.js";
import type {
  Snapshot,
  NodeSnapshot,
  ArrayNodeData,
} from "../../src/format/flatbuffers/types.js";

// ESM equivalent of __dirname
const __dirname = dirname(fileURLToPath(import.meta.url));

// Path to v1 test repository (used for real-fixture snapshot/txlog tests).
const TEST_REPO_V1_PATH = join(__dirname, "../data/test-repo-v1");

/**
 * Load a repo directory tree into a MockStorage.
 * Mirrors the helper in repository.test.ts; used for real-fixture
 * integration tests that exercise snapshot/transaction-log parsing.
 */
function loadRepoIntoMockStorage(repoPath: string): MockStorage {
  const storage = new MockStorage({});

  function loadDir(dirPath: string, prefix: string = ""): void {
    for (const entry of readdirSync(dirPath, { withFileTypes: true })) {
      const fullPath = join(dirPath, entry.name);
      const relativePath = prefix ? `${prefix}/${entry.name}` : entry.name;
      if (entry.isDirectory()) {
        loadDir(fullPath, relativePath);
      } else {
        storage.addFile(relativePath, readFileSync(fullPath));
      }
    }
  }

  loadDir(repoPath);
  return storage;
}

/**
 * Helper to create a mock ReadSession with injected snapshot data.
 * This allows testing session methods without valid FlatBuffer data.
 */
function createMockSession(options: {
  nodes?: NodeSnapshot[];
  storage?: MockStorage;
  specVersion?: SpecVersion;
}): ReadSession {
  const mockSnapshot: Snapshot = {
    id: createMockSnapshotId(1) as any,
    parentId: null,
    nodes: options.nodes ?? [],
    flushedAt: BigInt(Date.now() * 1000),
    message: "test commit",
    metadata: [],
    manifestFiles: [],
  };

  const session = Object.create(ReadSession.prototype);
  session.storage = options.storage ?? new MockStorage({});
  session.snapshot = mockSnapshot;
  session.specVersion = options.specVersion ?? SpecVersion.V1_0;
  session.manifestCache = new Map();

  return session;
}

/** Helper to create a group node */
function createGroupNode(path: string, userData: object = {}): NodeSnapshot {
  return {
    id: createMockNodeId(path.length) as any,
    path,
    userData: new TextEncoder().encode(JSON.stringify(userData)),
    nodeData: { type: "group" },
  };
}

/** Helper to create an array node */
function createArrayNode(path: string, userData: object = {}): NodeSnapshot {
  const arrayData: ArrayNodeData = {
    type: "array",
    shape: [{ arrayLength: 100, chunkLength: 10 }],
    dimensionNames: [null],
    manifests: [],
  };

  return {
    id: createMockNodeId(path.length + 100) as any,
    path,
    userData: new TextEncoder().encode(JSON.stringify(userData)),
    nodeData: arrayData,
  };
}

describe("ReadSession", () => {
  describe("open", () => {
    it("should throw when snapshot file not found", async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({});

      await expect(ReadSession.open(storage, snapshotId)).rejects.toThrow(
        "Object not found",
      );
    });

    it("should validate file type is Snapshot", async () => {
      const snapshotId = createMockSnapshotId(2);
      const snapshotPath = getSnapshotPath(encodeObjectId12(snapshotId));

      // Create header with wrong file type (Manifest instead of Snapshot)
      const wrongTypeHeader = createMockHeader({
        fileType: FileType.Manifest,
        specVersion: SpecVersion.V1_0,
      });

      const fileData = new Uint8Array(100);
      fileData.set(wrongTypeHeader, 0);

      const storage = new MockStorage({
        [snapshotPath]: fileData,
      });

      await expect(ReadSession.open(storage, snapshotId)).rejects.toThrow(
        "expected Snapshot, got Manifest",
      );
    });
  });

  describe("getNode (binary search)", () => {
    it("should find node in single-element snapshot", () => {
      const session = createMockSession({
        nodes: [createGroupNode("/root")],
      });

      const node = session.getNode("/root");
      expect(node).not.toBeNull();
      expect(node!.path).toBe("/root");
    });

    it("should find first node in sorted list", () => {
      const session = createMockSession({
        nodes: [
          createGroupNode("/aaa"),
          createGroupNode("/bbb"),
          createGroupNode("/ccc"),
        ],
      });

      const node = session.getNode("/aaa");
      expect(node).not.toBeNull();
      expect(node!.path).toBe("/aaa");
    });

    it("should find last node in sorted list", () => {
      const session = createMockSession({
        nodes: [
          createGroupNode("/aaa"),
          createGroupNode("/bbb"),
          createGroupNode("/ccc"),
        ],
      });

      const node = session.getNode("/ccc");
      expect(node).not.toBeNull();
      expect(node!.path).toBe("/ccc");
    });

    it("should find middle node in sorted list", () => {
      const session = createMockSession({
        nodes: [
          createGroupNode("/aaa"),
          createGroupNode("/bbb"),
          createGroupNode("/ccc"),
          createGroupNode("/ddd"),
          createGroupNode("/eee"),
        ],
      });

      const node = session.getNode("/ccc");
      expect(node).not.toBeNull();
      expect(node!.path).toBe("/ccc");
    });

    it("should return null for non-existent node", () => {
      const session = createMockSession({
        nodes: [createGroupNode("/aaa"), createGroupNode("/ccc")],
      });

      const node = session.getNode("/bbb");
      expect(node).toBeNull();
    });

    it("should return null for empty snapshot", () => {
      const session = createMockSession({ nodes: [] });
      const node = session.getNode("/any");
      expect(node).toBeNull();
    });

    it("should normalize path without leading slash", () => {
      const session = createMockSession({
        nodes: [createGroupNode("/test")],
      });

      const node = session.getNode("test"); // no leading slash
      expect(node).not.toBeNull();
      expect(node!.path).toBe("/test");
    });

    it("should use byte-order comparison for ASCII (not locale-aware)", () => {
      // In byte order: 'A' (65) < 'Z' (90) < 'a' (97) < 'z' (122)
      // localeCompare might sort case-insensitively or differently
      // Rust uses byte-order sorting, so we must match it
      const session = createMockSession({
        nodes: [
          createGroupNode("/A"),
          createGroupNode("/Z"),
          createGroupNode("/a"),
          createGroupNode("/z"),
        ],
      });

      // All nodes should be findable with byte-order binary search
      expect(session.getNode("/A")).not.toBeNull();
      expect(session.getNode("/Z")).not.toBeNull();
      expect(session.getNode("/a")).not.toBeNull();
      expect(session.getNode("/z")).not.toBeNull();

      // Non-existent paths should return null
      expect(session.getNode("/B")).toBeNull();
      expect(session.getNode("/m")).toBeNull();
    });

    it("should use UTF-8 byte-order comparison for non-ASCII", () => {
      // UTF-8 byte order for these characters:
      // 'ß' (U+00DF) = C3 9F
      // 'ä' (U+00E4) = C3 A4
      // 'Ω' (U+03A9) = CE A9
      // So UTF-8 order is: ß < ä < Ω
      const session = createMockSession({
        nodes: [
          createGroupNode("/ß"),
          createGroupNode("/ä"),
          createGroupNode("/Ω"),
        ],
      });

      expect(session.getNode("/ß")).not.toBeNull();
      expect(session.getNode("/ä")).not.toBeNull();
      expect(session.getNode("/Ω")).not.toBeNull();
      expect(session.getNode("/α")).toBeNull(); // U+03B1, not in list
    });

    it("should use UTF-8 byte-order for characters outside BMP", () => {
      // Characters outside the Basic Multilingual Plane (> U+FFFF)
      // are where UTF-16 and UTF-8 ordering can differ.
      // UTF-8 byte order:
      // '￿' (U+FFFF) = EF BF BF (3 bytes, last BMP char)
      // '𐀀' (U+10000) = F0 90 80 80 (4 bytes, first non-BMP char)
      // In UTF-8 byte order: U+FFFF < U+10000 (EF < F0)
      // In UTF-16 code unit order: U+10000 < U+FFFF (surrogate 0xD800 < 0xFFFF)
      const session = createMockSession({
        nodes: [
          createGroupNode("/\uFFFF"), // Last BMP character
          createGroupNode("/\u{10000}"), // First non-BMP character (𐀀)
        ],
      });

      expect(session.getNode("/\uFFFF")).not.toBeNull();
      expect(session.getNode("/\u{10000}")).not.toBeNull();
    });
  });

  describe("getMetadata", () => {
    it("should parse JSON metadata from node", () => {
      const metadata = {
        zarr_format: 3,
        node_type: "group",
        attributes: { foo: "bar" },
      };
      const session = createMockSession({
        nodes: [createGroupNode("/group", metadata)],
      });

      const result = session.getMetadata("/group");
      expect(result).toEqual(metadata);
    });

    it("should return null for non-existent node", () => {
      const session = createMockSession({ nodes: [] });
      const result = session.getMetadata("/missing");
      expect(result).toBeNull();
    });
  });

  describe("getRawMetadata", () => {
    it("should return raw bytes", () => {
      const session = createMockSession({
        nodes: [createGroupNode("/node", { test: true })],
      });

      const result = session.getRawMetadata("/node");
      expect(result).toBeInstanceOf(Uint8Array);
      expect(new TextDecoder().decode(result!)).toBe('{"test":true}');
    });

    it("should return null for non-existent node", () => {
      const session = createMockSession({ nodes: [] });
      const result = session.getRawMetadata("/missing");
      expect(result).toBeNull();
    });
  });

  describe("listNodes", () => {
    it("should return all nodes", () => {
      const session = createMockSession({
        nodes: [
          createGroupNode("/a"),
          createArrayNode("/b"),
          createGroupNode("/c"),
        ],
      });

      const nodes = session.listNodes();
      expect(nodes).toHaveLength(3);
      expect(nodes.map((n) => n.path)).toEqual(["/a", "/b", "/c"]);
    });

    it("should return empty array for empty snapshot", () => {
      const session = createMockSession({ nodes: [] });
      const nodes = session.listNodes();
      expect(nodes).toEqual([]);
    });
  });

  describe("listChildren", () => {
    it("should list direct children of root", () => {
      const session = createMockSession({
        nodes: [
          createGroupNode("/child1"),
          createGroupNode("/child2"),
          createGroupNode("/child1/nested"),
        ],
      });

      const children = session.listChildren("/");
      expect(children).toHaveLength(2);
      expect(children.map((n) => n.path).sort()).toEqual([
        "/child1",
        "/child2",
      ]);
    });

    it("should list direct children of nested group", () => {
      const session = createMockSession({
        nodes: [
          createGroupNode("/parent"),
          createGroupNode("/parent/child1"),
          createGroupNode("/parent/child2"),
          createGroupNode("/parent/child1/grandchild"),
        ],
      });

      const children = session.listChildren("/parent");
      expect(children).toHaveLength(2);
      expect(children.map((n) => n.path).sort()).toEqual([
        "/parent/child1",
        "/parent/child2",
      ]);
    });

    it("should return empty for leaf node", () => {
      const session = createMockSession({
        nodes: [createGroupNode("/leaf")],
      });

      const children = session.listChildren("/leaf");
      expect(children).toEqual([]);
    });
  });

  describe("getChunk", () => {
    it("should return null for non-existent array", async () => {
      const session = createMockSession({ nodes: [] });
      const result = await session.getChunk("/missing", [0]);
      expect(result).toBeNull();
    });

    it("should return null for group node", async () => {
      const session = createMockSession({
        nodes: [createGroupNode("/group")],
      });

      const result = await session.getChunk("/group", [0]);
      expect(result).toBeNull();
    });

    it("should return null for array with no manifests", async () => {
      const session = createMockSession({
        nodes: [createArrayNode("/array")],
      });

      const result = await session.getChunk("/array", [0]);
      expect(result).toBeNull();
    });
  });

  describe("fetchChunkPayload (virtual chunks)", () => {
    it("should fetch virtual chunk via global fetch", async () => {
      // Mock the global fetch
      const mockData = new Uint8Array([1, 2, 3, 4, 5]);
      const mockResponse = {
        ok: true,
        status: 206,
        arrayBuffer: vi.fn().mockResolvedValue(mockData.buffer),
      };
      const fetchSpy = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValue(mockResponse as any);

      // Create a session and access private method via any cast
      const session = createMockSession({ nodes: [] }) as any;

      const payload = {
        type: "virtual" as const,
        location: "https://example.com/data.bin",
        offset: 100,
        length: 5,
        checksumEtag: null,
        checksumLastModified: 0,
      };

      const result = await session.fetchChunkPayload(payload);

      expect(fetchSpy).toHaveBeenCalledWith("https://example.com/data.bin", {
        headers: { Range: "bytes=100-104" },
        signal: undefined,
      });
      expect(result).toEqual(mockData);

      fetchSpy.mockRestore();
    });

    it("should use fetchClient instead of globalThis.fetch when provided", async () => {
      const mockData = new Uint8Array(10);
      const mockResponse = {
        ok: true,
        status: 206,
        arrayBuffer: vi.fn().mockResolvedValue(mockData.buffer),
      };

      const fetchClient = {
        fetch: vi.fn().mockResolvedValue(mockResponse as any),
      };

      // Spy on globalThis.fetch to ensure it is NOT called
      const globalFetchSpy = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValue(mockResponse as any);

      const session = createMockSession({ nodes: [] }) as any;

      const payload = {
        type: "virtual" as const,
        location: "s3://bucket/data.bin",
        offset: 0,
        length: 10,
        checksumEtag: null,
        checksumLastModified: 0,
      };

      await session.fetchChunkPayload(payload, { fetchClient });

      // fetchClient.fetch should be called with the translated URL
      expect(fetchClient.fetch).toHaveBeenCalledWith(
        "https://bucket.s3.amazonaws.com/data.bin",
        {
          headers: { Range: "bytes=0-9" },
          signal: undefined,
        },
      );

      // globalThis.fetch should NOT be called
      expect(globalFetchSpy).not.toHaveBeenCalled();

      globalFetchSpy.mockRestore();
    });

    it("should not send checksum headers by default", async () => {
      const mockData = new Uint8Array(10);
      const mockResponse = {
        ok: true,
        status: 206,
        arrayBuffer: vi.fn().mockResolvedValue(mockData.buffer),
      };

      const fetchClient = {
        fetch: vi.fn().mockResolvedValue(mockResponse as any),
      };

      const session = createMockSession({ nodes: [] }) as any;

      const payload = {
        type: "virtual" as const,
        location: "https://example.com/data.bin",
        offset: 50,
        length: 10,
        checksumEtag: '"abc123"',
        checksumLastModified: 1700000000,
      };

      await session.fetchChunkPayload(payload, { fetchClient });

      expect(fetchClient.fetch).toHaveBeenCalledWith(
        "https://example.com/data.bin",
        {
          headers: {
            Range: "bytes=50-59",
          },
          signal: undefined,
        },
      );
    });

    it("should send checksum headers when validateChecksums is true", async () => {
      const mockData = new Uint8Array(10);
      const mockResponse = {
        ok: true,
        status: 206,
        arrayBuffer: vi.fn().mockResolvedValue(mockData.buffer),
      };

      const fetchClient = {
        fetch: vi.fn().mockResolvedValue(mockResponse as any),
      };

      const session = createMockSession({ nodes: [] }) as any;

      const payload = {
        type: "virtual" as const,
        location: "https://example.com/data.bin",
        offset: 50,
        length: 10,
        checksumEtag: '"abc123"',
        checksumLastModified: 1700000000,
      };

      await session.fetchChunkPayload(payload, {
        fetchClient,
        validateChecksums: true,
      });

      expect(fetchClient.fetch).toHaveBeenCalledWith(
        "https://example.com/data.bin",
        {
          headers: {
            Range: "bytes=50-59",
            "If-Match": '"abc123"',
            "If-Unmodified-Since": expect.any(String),
          },
          signal: undefined,
        },
      );
    });

    it("should throw on failed virtual chunk fetch", async () => {
      const mockResponse = {
        ok: false,
        status: 500,
        statusText: "Internal Server Error",
      };
      const fetchSpy = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValue(mockResponse as any);

      const session = createMockSession({ nodes: [] }) as any;

      const payload = {
        type: "virtual" as const,
        location: "https://example.com/bad.bin",
        offset: 0,
        length: 10,
        checksumEtag: null,
        checksumLastModified: 0,
      };

      await expect(session.fetchChunkPayload(payload)).rejects.toThrow(
        "Failed to fetch virtual chunk from https://example.com/bad.bin: 500 Internal Server Error",
      );

      fetchSpy.mockRestore();
    });

    it("should handle 404 response for virtual chunks", async () => {
      const mockResponse = {
        ok: false,
        status: 404,
        statusText: "Not Found",
      };
      const fetchSpy = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValue(mockResponse as any);

      const session = createMockSession({ nodes: [] }) as any;

      const payload = {
        type: "virtual" as const,
        location: "https://example.com/missing.bin",
        offset: 0,
        length: 10,
        checksumEtag: null,
        checksumLastModified: 0,
      };

      await expect(session.fetchChunkPayload(payload)).rejects.toThrow(
        "Failed to fetch virtual chunk from https://example.com/missing.bin: 404 Not Found",
      );

      fetchSpy.mockRestore();
    });

    it("should return inline chunk directly", async () => {
      const session = createMockSession({ nodes: [] }) as any;

      const inlineData = new Uint8Array([10, 20, 30]);
      const payload = {
        type: "inline" as const,
        data: inlineData,
      };

      const result = await session.fetchChunkPayload(payload);
      expect(result).toBe(inlineData);
    });

    it("should slice native full-object fallback when Range is ignored", async () => {
      const fullObject = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
      const storage = new MockStorage({});
      const getObjectSpy = vi
        .spyOn(storage, "getObject")
        .mockResolvedValue(fullObject);
      const session = createMockSession({ nodes: [], storage }) as any;

      const payload = {
        type: "native" as const,
        chunkId: createMockSnapshotId(321) as any,
        offset: 3,
        length: 4,
      };

      const result = await session.fetchChunkPayload(payload);

      expect(getObjectSpy).toHaveBeenCalledWith(
        expect.any(String),
        { start: 3, end: 7 },
        undefined,
      );
      expect(result).toEqual(new Uint8Array([3, 4, 5, 6]));
    });

    it("should throw when native response is too small for fallback slicing", async () => {
      const storage = new MockStorage({});
      const getObjectSpy = vi
        .spyOn(storage, "getObject")
        .mockResolvedValue(new Uint8Array([1, 2]));
      const session = createMockSession({ nodes: [], storage }) as any;

      const payload = {
        type: "native" as const,
        chunkId: createMockSnapshotId(322) as any,
        offset: 3,
        length: 4,
      };

      await expect(session.fetchChunkPayload(payload)).rejects.toThrow(
        "Storage returned 2 bytes",
      );

      expect(getObjectSpy).toHaveBeenCalledWith(
        expect.any(String),
        { start: 3, end: 7 },
        undefined,
      );
    });

    it("should slice virtual full-object fallback when server returns 200", async () => {
      const fullObject = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
      const mockResponse = {
        ok: true,
        status: 200,
        statusText: "OK",
        arrayBuffer: vi.fn().mockResolvedValue(fullObject.buffer),
      };
      const fetchSpy = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValue(mockResponse as any);
      const session = createMockSession({ nodes: [] }) as any;

      const payload = {
        type: "virtual" as const,
        location: "https://example.com/data.bin",
        offset: 3,
        length: 4,
        checksumEtag: null,
        checksumLastModified: 0,
      };

      const result = await session.fetchChunkPayload(payload);

      expect(fetchSpy).toHaveBeenCalledWith("https://example.com/data.bin", {
        headers: { Range: "bytes=3-6" },
        signal: undefined,
      });
      expect(result).toEqual(new Uint8Array([3, 4, 5, 6]));

      fetchSpy.mockRestore();
    });

    it("should reject virtual 200 responses without full-body fallback coverage", async () => {
      const mockResponse = {
        ok: true,
        status: 200,
        statusText: "OK",
        arrayBuffer: vi
          .fn()
          .mockResolvedValue(new Uint8Array([9, 8, 7, 6]).buffer),
      };
      const fetchSpy = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValue(mockResponse as any);
      const session = createMockSession({ nodes: [] }) as any;

      const payload = {
        type: "virtual" as const,
        location: "https://example.com/data.bin",
        offset: 10,
        length: 4,
        checksumEtag: null,
        checksumLastModified: 0,
      };

      await expect(session.fetchChunkPayload(payload)).rejects.toThrow(
        "Virtual range request not honored",
      );

      fetchSpy.mockRestore();
    });

    it("should translate az:// URLs using azureAccount option", async () => {
      const mockData = new Uint8Array(5);
      const mockResponse = {
        ok: true,
        status: 206,
        arrayBuffer: vi.fn().mockResolvedValue(mockData.buffer),
      };

      const fetchClient = {
        fetch: vi.fn().mockResolvedValue(mockResponse as any),
      };

      const session = createMockSession({ nodes: [] }) as any;

      const payload = {
        type: "virtual" as const,
        location: "az://mycontainer/prefix/data.bin",
        offset: 0,
        length: 5,
        checksumEtag: null,
        checksumLastModified: 0,
      };

      await session.fetchChunkPayload(payload, {
        fetchClient,
        azureAccount: "myaccount",
      });

      expect(fetchClient.fetch).toHaveBeenCalledWith(
        "https://myaccount.blob.core.windows.net/mycontainer/prefix/data.bin",
        expect.any(Object),
      );
    });

    it("should throw when az:// URL is used without azureAccount", async () => {
      const session = createMockSession({ nodes: [] }) as any;

      const payload = {
        type: "virtual" as const,
        location: "az://mycontainer/data.bin",
        offset: 0,
        length: 5,
        checksumEtag: null,
        checksumLastModified: 0,
      };

      await expect(session.fetchChunkPayload(payload)).rejects.toThrow(
        "azureAccount option is required",
      );
    });
  });

  describe("fetchChunkPayloadRange", () => {
    it("should slice native full-object fallback when Range is ignored", async () => {
      const fullObject = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
      const storage = new MockStorage({});
      const getObjectSpy = vi
        .spyOn(storage, "getObject")
        .mockResolvedValue(fullObject);
      const session = createMockSession({ nodes: [], storage }) as any;

      const payload = {
        type: "native" as const,
        chunkId: createMockSnapshotId(123) as any,
        offset: 3,
        length: 6,
      };

      const result = await session.fetchChunkPayloadRange(payload, {
        offset: 1,
        length: 3,
      });

      expect(getObjectSpy).toHaveBeenCalledWith(
        expect.any(String),
        { start: 4, end: 7 },
        undefined,
      );
      expect(result).toEqual(new Uint8Array([4, 5, 6]));
    });

    it("should throw when native range response is too small for fallback slicing", async () => {
      const storage = new MockStorage({});
      const getObjectSpy = vi
        .spyOn(storage, "getObject")
        .mockResolvedValue(new Uint8Array([1, 2]));
      const session = createMockSession({ nodes: [], storage }) as any;

      const payload = {
        type: "native" as const,
        chunkId: createMockSnapshotId(124) as any,
        offset: 3,
        length: 6,
      };

      await expect(
        session.fetchChunkPayloadRange(payload, {
          offset: 1,
          length: 3,
        }),
      ).rejects.toThrow("Storage returned 2 bytes");

      expect(getObjectSpy).toHaveBeenCalledWith(
        expect.any(String),
        { start: 4, end: 7 },
        undefined,
      );
    });

    it("should slice virtual full-object fallback when server returns 200", async () => {
      const fullObject = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
      const mockResponse = {
        ok: true,
        status: 200,
        statusText: "OK",
        arrayBuffer: vi.fn().mockResolvedValue(fullObject.buffer),
      };
      const fetchSpy = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValue(mockResponse as any);
      const session = createMockSession({ nodes: [] }) as any;

      const payload = {
        type: "virtual" as const,
        location: "https://example.com/data.bin",
        offset: 2,
        length: 8,
        checksumEtag: null,
        checksumLastModified: 0,
      };

      const result = await session.fetchChunkPayloadRange(payload, {
        offset: 3,
        length: 2,
      });

      expect(fetchSpy).toHaveBeenCalledWith("https://example.com/data.bin", {
        headers: { Range: "bytes=5-6" },
        signal: undefined,
      });
      expect(result).toEqual(new Uint8Array([5, 6]));

      fetchSpy.mockRestore();
    });

    it("should reject virtual 200 responses without full-body fallback coverage", async () => {
      const mockResponse = {
        ok: true,
        status: 200,
        statusText: "OK",
        arrayBuffer: vi
          .fn()
          .mockResolvedValue(new Uint8Array([9, 8, 7, 6]).buffer),
      };
      const fetchSpy = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValue(mockResponse as any);
      const session = createMockSession({ nodes: [] }) as any;

      const payload = {
        type: "virtual" as const,
        location: "https://example.com/data.bin",
        offset: 10,
        length: 20,
        checksumEtag: null,
        checksumLastModified: 0,
      };

      await expect(
        session.fetchChunkPayloadRange(payload, {
          offset: 5,
          length: 4,
        }),
      ).rejects.toThrow("Virtual range request not honored");

      fetchSpy.mockRestore();
    });
  });

  describe("abort signal handling", () => {
    it("should reject when signal is already aborted", async () => {
      const session = createMockSession({
        nodes: [createArrayNode("/array", { manifests: [] })],
      });

      const controller = new AbortController();
      controller.abort();

      await expect(
        session.getChunk("/array", [0], {
          signal: controller.signal,
        }),
      ).rejects.toMatchObject({ name: "AbortError" });
    });

    it("should reject range reads when signal is already aborted", async () => {
      const session = createMockSession({
        nodes: [createArrayNode("/array", { manifests: [] })],
      });

      const controller = new AbortController();
      controller.abort();

      await expect(
        session.getChunkRange(
          "/array",
          [0],
          { offset: 0, length: 1 },
          { signal: controller.signal },
        ),
      ).rejects.toMatchObject({ name: "AbortError" });
    });

    it("should return null for non-existent array (not throw)", async () => {
      const session = createMockSession({ nodes: [] });

      const controller = new AbortController();

      // Should return null for non-existent path, not throw
      const result = await session.getChunk("/nonexistent", [0], {
        signal: controller.signal,
      });

      expect(result).toBeNull();
    });
  });

  describe("snapshot getters (real fixture)", () => {
    // v1 fixture main branch history is linear:
    //   NXH3M0HJ7EEJ0699DPP0 "set virtual chunk"       (head)
    //   7XAF0Q905SH4938DN9CG "fill data"
    //   GC4YVH5SKBPEZCENYQE0 "empty structure"
    //   P874YS3J196959RDHX7G "Repository initialized"  (root, parent=null)

    it("should return 12-byte snapshot id matching the checked-out snapshot", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V1_PATH);
      const repo = await Repository.open({ storage });
      const session = await repo.checkoutBranch("main");

      const id = session.getSnapshotId();
      expect(id).toBeInstanceOf(Uint8Array);
      expect(id.length).toBe(12);
      expect(encodeObjectId12(id)).toBe("NXH3M0HJ7EEJ0699DPP0");
    });

    it("should return parent snapshot id for a non-root commit", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V1_PATH);
      const repo = await Repository.open({ storage });
      const session = await repo.checkoutBranch("main");

      const parent = session.getParentSnapshotId();
      expect(parent).not.toBeNull();
      expect(encodeObjectId12(parent!)).toBe("7XAF0Q905SH4938DN9CG");
    });

    it("should return null parent for the root snapshot", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V1_PATH);
      const repo = await Repository.open({ storage });
      const session = await repo.checkoutSnapshot("P874YS3J196959RDHX7G");

      expect(session.getParentSnapshotId()).toBeNull();
    });

    it("should return the commit message", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V1_PATH);
      const repo = await Repository.open({ storage });
      const session = await repo.checkoutBranch("main");

      expect(session.getMessage()).toBe("set virtual chunk");
    });

    it("should return flushedAt as a Date in the expected era", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V1_PATH);
      const repo = await Repository.open({ storage });
      const session = await repo.checkoutBranch("main");

      const flushedAt = session.getFlushedAt();
      expect(flushedAt).toBeInstanceOf(Date);
      // Fixture was written in 2026 Q1/Q2 — assert a loose lower bound so
      // this doesn't break if fixtures are regenerated.
      expect(flushedAt.getTime()).toBeGreaterThan(
        new Date("2020-01-01").getTime(),
      );
      expect(flushedAt.getTime()).toBeLessThan(Date.now() + 86400_000);
    });

    it("should return snapshot metadata as a plain object", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V1_PATH);
      const repo = await Repository.open({ storage });
      const session = await repo.checkoutBranch("main");

      const metadata = session.getSnapshotMetadata();
      expect(metadata).toBeTypeOf("object");
      expect(metadata).not.toBeNull();
    });
  });

  describe("loadTransactionLog (real fixture)", () => {
    it("should parse a transaction log for a non-root commit", async () => {
      const storage = loadRepoIntoMockStorage(TEST_REPO_V1_PATH);
      const repo = await Repository.open({ storage });
      const session = await repo.checkoutBranch("main");

      const txLog = await session.loadTransactionLog();
      expect(txLog).not.toBeNull();
      // Every list field should be present and array-valued.
      expect(Array.isArray(txLog!.newArrays)).toBe(true);
      expect(Array.isArray(txLog!.newGroups)).toBe(true);
      expect(Array.isArray(txLog!.updatedArrays)).toBe(true);
      expect(Array.isArray(txLog!.updatedGroups)).toBe(true);
      expect(Array.isArray(txLog!.updatedChunks)).toBe(true);
      expect(Array.isArray(txLog!.deletedArrays)).toBe(true);
      expect(Array.isArray(txLog!.deletedGroups)).toBe(true);
      // The "set virtual chunk" commit touches at least one chunk.
      expect(txLog!.updatedChunks.length).toBeGreaterThan(0);
      // Its id should equal the current snapshot id.
      expect(encodeObjectId12(txLog!.id as Uint8Array)).toBe(
        "NXH3M0HJ7EEJ0699DPP0",
      );
    });

    it("should return null when no transaction log file exists for the snapshot", async () => {
      // The v1 fixture's root snapshot "Repository initialized" has no
      // transactions/<id> file. loadTransactionLog should translate the
      // resulting NotFoundError into null (per the README contract).
      const storage = loadRepoIntoMockStorage(TEST_REPO_V1_PATH);
      const repo = await Repository.open({ storage });
      const session = await repo.checkoutSnapshot("P874YS3J196959RDHX7G");

      const txLog = await session.loadTransactionLog();
      expect(txLog).toBeNull();
    });
  });
});
