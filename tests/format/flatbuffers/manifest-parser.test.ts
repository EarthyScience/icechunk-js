/**
 * Tests for manifest FlatBuffer parsing and chunk lookup.
 */

import { describe, it, expect } from "vitest";
import {
  findChunkRef,
  getChunkPayload,
} from "../../../src/format/flatbuffers/manifest-parser.js";
import type {
  Manifest,
  ChunkRef,
  ObjectId8,
} from "../../../src/format/flatbuffers/types.js";

/** Create a mock node ID */
function mockNodeId(seed: number): ObjectId8 {
  const id = new Uint8Array(8);
  for (let i = 0; i < 8; i++) {
    id[i] = (seed + i * 23) % 256;
  }
  return id as ObjectId8;
}

/** Create a mock 12-byte ID */
function mockObjectId12(seed: number): Uint8Array {
  const id = new Uint8Array(12);
  for (let i = 0; i < 12; i++) {
    id[i] = (seed + i * 17) % 256;
  }
  return id;
}

/** Create a chunk ref with inline data */
function inlineChunkRef(index: number[], data: Uint8Array): ChunkRef {
  return {
    index,
    inline: data,
    offset: 0,
    length: data.length,
    chunkId: null,
    location: null,
    checksumEtag: null,
    checksumLastModified: 0,
  };
}

/** Create a chunk ref for native storage */
function nativeChunkRef(
  index: number[],
  chunkId: Uint8Array,
  offset: number,
  length: number,
): ChunkRef {
  return {
    index,
    inline: null,
    offset,
    length,
    chunkId: chunkId as any,
    location: null,
    checksumEtag: null,
    checksumLastModified: 0,
  };
}

/** Create a chunk ref for virtual storage */
function virtualChunkRef(
  index: number[],
  location: string,
  offset: number,
  length: number,
): ChunkRef {
  return {
    index,
    inline: null,
    offset,
    length,
    chunkId: null,
    location,
    checksumEtag: "etag123",
    checksumLastModified: 1700000000,
  };
}

describe("findChunkRef", () => {
  describe("binary search correctness", () => {
    it("should find chunk in single-element array", () => {
      const nodeId = mockNodeId(1);
      const manifest: Manifest = {
        id: mockObjectId12(1) as any,
        arrays: [
          {
            nodeId,
            refs: [inlineChunkRef([0], new Uint8Array([1, 2, 3]))],
          },
        ],
      };

      const result = findChunkRef(manifest, nodeId, [0]);
      expect(result).not.toBeNull();
      expect(result!.index).toEqual([0]);
    });

    it("should find first element in sorted array", () => {
      const nodeId = mockNodeId(1);
      const manifest: Manifest = {
        id: mockObjectId12(1) as any,
        arrays: [
          {
            nodeId,
            refs: [
              inlineChunkRef([0], new Uint8Array([1])),
              inlineChunkRef([1], new Uint8Array([2])),
              inlineChunkRef([2], new Uint8Array([3])),
            ],
          },
        ],
      };

      const result = findChunkRef(manifest, nodeId, [0]);
      expect(result).not.toBeNull();
      expect(result!.inline).toEqual(new Uint8Array([1]));
    });

    it("should find last element in sorted array", () => {
      const nodeId = mockNodeId(1);
      const manifest: Manifest = {
        id: mockObjectId12(1) as any,
        arrays: [
          {
            nodeId,
            refs: [
              inlineChunkRef([0], new Uint8Array([1])),
              inlineChunkRef([1], new Uint8Array([2])),
              inlineChunkRef([2], new Uint8Array([3])),
            ],
          },
        ],
      };

      const result = findChunkRef(manifest, nodeId, [2]);
      expect(result).not.toBeNull();
      expect(result!.inline).toEqual(new Uint8Array([3]));
    });

    it("should find middle element in sorted array", () => {
      const nodeId = mockNodeId(1);
      const manifest: Manifest = {
        id: mockObjectId12(1) as any,
        arrays: [
          {
            nodeId,
            refs: [
              inlineChunkRef([0], new Uint8Array([1])),
              inlineChunkRef([1], new Uint8Array([2])),
              inlineChunkRef([2], new Uint8Array([3])),
              inlineChunkRef([3], new Uint8Array([4])),
              inlineChunkRef([4], new Uint8Array([5])),
            ],
          },
        ],
      };

      const result = findChunkRef(manifest, nodeId, [2]);
      expect(result).not.toBeNull();
      expect(result!.inline).toEqual(new Uint8Array([3]));
    });

    it("should return null for missing chunk", () => {
      const nodeId = mockNodeId(1);
      const manifest: Manifest = {
        id: mockObjectId12(1) as any,
        arrays: [
          {
            nodeId,
            refs: [
              inlineChunkRef([0], new Uint8Array([1])),
              inlineChunkRef([2], new Uint8Array([3])),
            ],
          },
        ],
      };

      const result = findChunkRef(manifest, nodeId, [1]);
      expect(result).toBeNull();
    });

    it("should return null for empty refs array", () => {
      const nodeId = mockNodeId(1);
      const manifest: Manifest = {
        id: mockObjectId12(1) as any,
        arrays: [
          {
            nodeId,
            refs: [],
          },
        ],
      };

      const result = findChunkRef(manifest, nodeId, [0]);
      expect(result).toBeNull();
    });

    it("should return null for non-existent array", () => {
      const nodeId1 = mockNodeId(1);
      const nodeId2 = mockNodeId(2);
      const manifest: Manifest = {
        id: mockObjectId12(1) as any,
        arrays: [
          {
            nodeId: nodeId1,
            refs: [inlineChunkRef([0], new Uint8Array([1]))],
          },
        ],
      };

      const result = findChunkRef(manifest, nodeId2, [0]);
      expect(result).toBeNull();
    });
  });

  describe("multiple arrays binary search", () => {
    it("should find first array in manifest with multiple arrays", () => {
      // Node IDs must be sorted for binary search to work
      const nodeId1 = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 1]) as ObjectId8;
      const nodeId2 = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 2]) as ObjectId8;
      const nodeId3 = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 3]) as ObjectId8;

      const manifest: Manifest = {
        id: mockObjectId12(1) as any,
        arrays: [
          {
            nodeId: nodeId1,
            refs: [inlineChunkRef([0], new Uint8Array([10]))],
          },
          {
            nodeId: nodeId2,
            refs: [inlineChunkRef([0], new Uint8Array([20]))],
          },
          {
            nodeId: nodeId3,
            refs: [inlineChunkRef([0], new Uint8Array([30]))],
          },
        ],
      };

      const result = findChunkRef(manifest, nodeId1, [0]);
      expect(result).not.toBeNull();
      expect(result!.inline).toEqual(new Uint8Array([10]));
    });

    it("should find last array in manifest with multiple arrays", () => {
      const nodeId1 = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 1]) as ObjectId8;
      const nodeId2 = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 2]) as ObjectId8;
      const nodeId3 = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 3]) as ObjectId8;

      const manifest: Manifest = {
        id: mockObjectId12(1) as any,
        arrays: [
          {
            nodeId: nodeId1,
            refs: [inlineChunkRef([0], new Uint8Array([10]))],
          },
          {
            nodeId: nodeId2,
            refs: [inlineChunkRef([0], new Uint8Array([20]))],
          },
          {
            nodeId: nodeId3,
            refs: [inlineChunkRef([0], new Uint8Array([30]))],
          },
        ],
      };

      const result = findChunkRef(manifest, nodeId3, [0]);
      expect(result).not.toBeNull();
      expect(result!.inline).toEqual(new Uint8Array([30]));
    });

    it("should find middle array in manifest with multiple arrays", () => {
      const nodeId1 = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 1]) as ObjectId8;
      const nodeId2 = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 2]) as ObjectId8;
      const nodeId3 = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 3]) as ObjectId8;
      const nodeId4 = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 4]) as ObjectId8;
      const nodeId5 = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 5]) as ObjectId8;

      const manifest: Manifest = {
        id: mockObjectId12(1) as any,
        arrays: [
          {
            nodeId: nodeId1,
            refs: [inlineChunkRef([0], new Uint8Array([10]))],
          },
          {
            nodeId: nodeId2,
            refs: [inlineChunkRef([0], new Uint8Array([20]))],
          },
          {
            nodeId: nodeId3,
            refs: [inlineChunkRef([0], new Uint8Array([30]))],
          },
          {
            nodeId: nodeId4,
            refs: [inlineChunkRef([0], new Uint8Array([40]))],
          },
          {
            nodeId: nodeId5,
            refs: [inlineChunkRef([0], new Uint8Array([50]))],
          },
        ],
      };

      const result = findChunkRef(manifest, nodeId3, [0]);
      expect(result).not.toBeNull();
      expect(result!.inline).toEqual(new Uint8Array([30]));
    });

    it("should return null for missing array among multiple arrays", () => {
      const nodeId1 = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 1]) as ObjectId8;
      const nodeId3 = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 3]) as ObjectId8;
      const nodeId5 = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 5]) as ObjectId8;
      const nodeIdMissing = new Uint8Array([
        0, 0, 0, 0, 0, 0, 0, 2,
      ]) as ObjectId8;

      const manifest: Manifest = {
        id: mockObjectId12(1) as any,
        arrays: [
          {
            nodeId: nodeId1,
            refs: [inlineChunkRef([0], new Uint8Array([10]))],
          },
          {
            nodeId: nodeId3,
            refs: [inlineChunkRef([0], new Uint8Array([30]))],
          },
          {
            nodeId: nodeId5,
            refs: [inlineChunkRef([0], new Uint8Array([50]))],
          },
        ],
      };

      const result = findChunkRef(manifest, nodeIdMissing, [0]);
      expect(result).toBeNull();
    });

    it("should handle lexicographic byte ordering of node IDs", () => {
      // Node IDs are compared byte-by-byte lexicographically
      const nodeIdA = new Uint8Array([0, 0, 0, 0, 0, 0, 1, 0]) as ObjectId8; // Comes before
      const nodeIdB = new Uint8Array([0, 0, 0, 0, 0, 0, 2, 0]) as ObjectId8; // Comes after

      const manifest: Manifest = {
        id: mockObjectId12(1) as any,
        arrays: [
          { nodeId: nodeIdA, refs: [inlineChunkRef([0], new Uint8Array([1]))] },
          { nodeId: nodeIdB, refs: [inlineChunkRef([0], new Uint8Array([2]))] },
        ],
      };

      expect(findChunkRef(manifest, nodeIdA, [0])!.inline).toEqual(
        new Uint8Array([1]),
      );
      expect(findChunkRef(manifest, nodeIdB, [0])!.inline).toEqual(
        new Uint8Array([2]),
      );
    });
  });

  describe("multidimensional coordinates", () => {
    it("should find 2D chunk by coordinates", () => {
      const nodeId = mockNodeId(1);
      const manifest: Manifest = {
        id: mockObjectId12(1) as any,
        arrays: [
          {
            nodeId,
            refs: [
              inlineChunkRef([0, 0], new Uint8Array([1])),
              inlineChunkRef([0, 1], new Uint8Array([2])),
              inlineChunkRef([1, 0], new Uint8Array([3])),
              inlineChunkRef([1, 1], new Uint8Array([4])),
            ],
          },
        ],
      };

      expect(findChunkRef(manifest, nodeId, [0, 0])!.inline).toEqual(
        new Uint8Array([1]),
      );
      expect(findChunkRef(manifest, nodeId, [0, 1])!.inline).toEqual(
        new Uint8Array([2]),
      );
      expect(findChunkRef(manifest, nodeId, [1, 0])!.inline).toEqual(
        new Uint8Array([3]),
      );
      expect(findChunkRef(manifest, nodeId, [1, 1])!.inline).toEqual(
        new Uint8Array([4]),
      );
    });

    it("should find 3D chunk by coordinates", () => {
      const nodeId = mockNodeId(1);
      const manifest: Manifest = {
        id: mockObjectId12(1) as any,
        arrays: [
          {
            nodeId,
            refs: [
              inlineChunkRef([0, 0, 0], new Uint8Array([1])),
              inlineChunkRef([0, 0, 1], new Uint8Array([2])),
              inlineChunkRef([1, 2, 3], new Uint8Array([99])),
            ],
          },
        ],
      };

      expect(findChunkRef(manifest, nodeId, [1, 2, 3])!.inline).toEqual(
        new Uint8Array([99]),
      );
    });

    it("should handle lexicographic ordering correctly", () => {
      const nodeId = mockNodeId(1);
      // [0, 10] comes before [1, 0] lexicographically
      const manifest: Manifest = {
        id: mockObjectId12(1) as any,
        arrays: [
          {
            nodeId,
            refs: [
              inlineChunkRef([0, 10], new Uint8Array([1])),
              inlineChunkRef([1, 0], new Uint8Array([2])),
            ],
          },
        ],
      };

      expect(findChunkRef(manifest, nodeId, [0, 10])!.inline).toEqual(
        new Uint8Array([1]),
      );
      expect(findChunkRef(manifest, nodeId, [1, 0])!.inline).toEqual(
        new Uint8Array([2]),
      );
    });
  });
});

describe("getChunkPayload", () => {
  it("should extract inline payload", () => {
    const data = new Uint8Array([1, 2, 3, 4]);
    const ref = inlineChunkRef([0], data);

    const payload = getChunkPayload(ref);

    expect(payload.type).toBe("inline");
    if (payload.type === "inline") {
      expect(payload.data).toEqual(data);
    }
  });

  it("should extract native payload", () => {
    const chunkId = mockObjectId12(42);
    const ref = nativeChunkRef([0], chunkId, 1024, 512);

    const payload = getChunkPayload(ref);

    expect(payload.type).toBe("native");
    if (payload.type === "native") {
      expect(payload.chunkId).toEqual(chunkId);
      expect(payload.offset).toBe(1024);
      expect(payload.length).toBe(512);
    }
  });

  it("should extract virtual payload", () => {
    const ref = virtualChunkRef([0], "s3://bucket/key", 2048, 1024);

    const payload = getChunkPayload(ref);

    expect(payload.type).toBe("virtual");
    if (payload.type === "virtual") {
      expect(payload.location).toBe("s3://bucket/key");
      expect(payload.offset).toBe(2048);
      expect(payload.length).toBe(1024);
      expect(payload.checksumEtag).toBe("etag123");
      expect(payload.checksumLastModified).toBe(1700000000);
    }
  });

  it("should throw for invalid chunk ref", () => {
    const invalidRef: ChunkRef = {
      index: [0],
      inline: null,
      offset: 0,
      length: 0,
      chunkId: null,
      location: null,
      checksumEtag: null,
      checksumLastModified: 0,
    };

    expect(() => getChunkPayload(invalidRef)).toThrow("Invalid ChunkRef");
  });

  it("should prefer inline over native", () => {
    // Edge case: ref has both inline and chunkId (shouldn't happen, but test precedence)
    const ref: ChunkRef = {
      index: [0],
      inline: new Uint8Array([1, 2, 3]),
      offset: 0,
      length: 3,
      chunkId: mockObjectId12(1) as any,
      location: null,
      checksumEtag: null,
      checksumLastModified: 0,
    };

    const payload = getChunkPayload(ref);
    expect(payload.type).toBe("inline");
  });
});
