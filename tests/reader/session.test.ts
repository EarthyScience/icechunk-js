import { describe, it, expect, vi } from 'vitest';
import { ReadSession } from '../../src/reader/session.js';
import {
  MockStorage,
  createMockHeader,
  createMockSnapshotId,
  createMockNodeId,
} from '../fixtures/mock-storage.js';
import { getSnapshotPath } from '../../src/format/constants.js';
import { encodeObjectId12 } from '../../src/format/object-id.js';
import { FileType, SpecVersion, CompressionAlgorithm } from '../../src/format/header.js';
import type { Snapshot, NodeSnapshot, NodeData, ArrayNodeData } from '../../src/format/flatbuffers/types.js';

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
    message: 'test commit',
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
    nodeData: { type: 'group' },
  };
}

/** Helper to create an array node */
function createArrayNode(path: string, userData: object = {}): NodeSnapshot {
  const arrayData: ArrayNodeData = {
    type: 'array',
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

describe('ReadSession', () => {
  describe('open', () => {
    it('should throw when snapshot file not found', async () => {
      const snapshotId = createMockSnapshotId(1);
      const storage = new MockStorage({});

      await expect(ReadSession.open(storage, snapshotId)).rejects.toThrow(
        'Object not found'
      );
    });

    it('should validate file type is Snapshot', async () => {
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
        'expected Snapshot, got Manifest'
      );
    });
  });

  describe('getNode (binary search)', () => {
    it('should find node in single-element snapshot', () => {
      const session = createMockSession({
        nodes: [createGroupNode('/root')],
      });

      const node = session.getNode('/root');
      expect(node).not.toBeNull();
      expect(node!.path).toBe('/root');
    });

    it('should find first node in sorted list', () => {
      const session = createMockSession({
        nodes: [
          createGroupNode('/aaa'),
          createGroupNode('/bbb'),
          createGroupNode('/ccc'),
        ],
      });

      const node = session.getNode('/aaa');
      expect(node).not.toBeNull();
      expect(node!.path).toBe('/aaa');
    });

    it('should find last node in sorted list', () => {
      const session = createMockSession({
        nodes: [
          createGroupNode('/aaa'),
          createGroupNode('/bbb'),
          createGroupNode('/ccc'),
        ],
      });

      const node = session.getNode('/ccc');
      expect(node).not.toBeNull();
      expect(node!.path).toBe('/ccc');
    });

    it('should find middle node in sorted list', () => {
      const session = createMockSession({
        nodes: [
          createGroupNode('/aaa'),
          createGroupNode('/bbb'),
          createGroupNode('/ccc'),
          createGroupNode('/ddd'),
          createGroupNode('/eee'),
        ],
      });

      const node = session.getNode('/ccc');
      expect(node).not.toBeNull();
      expect(node!.path).toBe('/ccc');
    });

    it('should return null for non-existent node', () => {
      const session = createMockSession({
        nodes: [
          createGroupNode('/aaa'),
          createGroupNode('/ccc'),
        ],
      });

      const node = session.getNode('/bbb');
      expect(node).toBeNull();
    });

    it('should return null for empty snapshot', () => {
      const session = createMockSession({ nodes: [] });
      const node = session.getNode('/any');
      expect(node).toBeNull();
    });

    it('should normalize path without leading slash', () => {
      const session = createMockSession({
        nodes: [createGroupNode('/test')],
      });

      const node = session.getNode('test'); // no leading slash
      expect(node).not.toBeNull();
      expect(node!.path).toBe('/test');
    });

    it('should use byte-order comparison for ASCII (not locale-aware)', () => {
      // In byte order: 'A' (65) < 'Z' (90) < 'a' (97) < 'z' (122)
      // localeCompare might sort case-insensitively or differently
      // Rust uses byte-order sorting, so we must match it
      const session = createMockSession({
        nodes: [
          createGroupNode('/A'),
          createGroupNode('/Z'),
          createGroupNode('/a'),
          createGroupNode('/z'),
        ],
      });

      // All nodes should be findable with byte-order binary search
      expect(session.getNode('/A')).not.toBeNull();
      expect(session.getNode('/Z')).not.toBeNull();
      expect(session.getNode('/a')).not.toBeNull();
      expect(session.getNode('/z')).not.toBeNull();

      // Non-existent paths should return null
      expect(session.getNode('/B')).toBeNull();
      expect(session.getNode('/m')).toBeNull();
    });

    it('should use UTF-8 byte-order comparison for non-ASCII', () => {
      // UTF-8 byte order for these characters:
      // 'ß' (U+00DF) = C3 9F
      // 'ä' (U+00E4) = C3 A4
      // 'Ω' (U+03A9) = CE A9
      // So UTF-8 order is: ß < ä < Ω
      const session = createMockSession({
        nodes: [
          createGroupNode('/ß'),
          createGroupNode('/ä'),
          createGroupNode('/Ω'),
        ],
      });

      expect(session.getNode('/ß')).not.toBeNull();
      expect(session.getNode('/ä')).not.toBeNull();
      expect(session.getNode('/Ω')).not.toBeNull();
      expect(session.getNode('/α')).toBeNull(); // U+03B1, not in list
    });

    it('should use UTF-8 byte-order for characters outside BMP', () => {
      // Characters outside the Basic Multilingual Plane (> U+FFFF)
      // are where UTF-16 and UTF-8 ordering can differ.
      // UTF-8 byte order:
      // '￿' (U+FFFF) = EF BF BF (3 bytes, last BMP char)
      // '𐀀' (U+10000) = F0 90 80 80 (4 bytes, first non-BMP char)
      // In UTF-8 byte order: U+FFFF < U+10000 (EF < F0)
      // In UTF-16 code unit order: U+10000 < U+FFFF (surrogate 0xD800 < 0xFFFF)
      const session = createMockSession({
        nodes: [
          createGroupNode('/\uFFFF'),  // Last BMP character
          createGroupNode('/\u{10000}'), // First non-BMP character (𐀀)
        ],
      });

      expect(session.getNode('/\uFFFF')).not.toBeNull();
      expect(session.getNode('/\u{10000}')).not.toBeNull();
    });

  });

  describe('getMetadata', () => {
    it('should parse JSON metadata from node', () => {
      const metadata = { zarr_format: 3, node_type: 'group', attributes: { foo: 'bar' } };
      const session = createMockSession({
        nodes: [createGroupNode('/group', metadata)],
      });

      const result = session.getMetadata('/group');
      expect(result).toEqual(metadata);
    });

    it('should return null for non-existent node', () => {
      const session = createMockSession({ nodes: [] });
      const result = session.getMetadata('/missing');
      expect(result).toBeNull();
    });
  });

  describe('getRawMetadata', () => {
    it('should return raw bytes', () => {
      const session = createMockSession({
        nodes: [createGroupNode('/node', { test: true })],
      });

      const result = session.getRawMetadata('/node');
      expect(result).toBeInstanceOf(Uint8Array);
      expect(new TextDecoder().decode(result!)).toBe('{"test":true}');
    });

    it('should return null for non-existent node', () => {
      const session = createMockSession({ nodes: [] });
      const result = session.getRawMetadata('/missing');
      expect(result).toBeNull();
    });
  });

  describe('listNodes', () => {
    it('should return all nodes', () => {
      const session = createMockSession({
        nodes: [
          createGroupNode('/a'),
          createArrayNode('/b'),
          createGroupNode('/c'),
        ],
      });

      const nodes = session.listNodes();
      expect(nodes).toHaveLength(3);
      expect(nodes.map(n => n.path)).toEqual(['/a', '/b', '/c']);
    });

    it('should return empty array for empty snapshot', () => {
      const session = createMockSession({ nodes: [] });
      const nodes = session.listNodes();
      expect(nodes).toEqual([]);
    });
  });

  describe('listChildren', () => {
    it('should list direct children of root', () => {
      const session = createMockSession({
        nodes: [
          createGroupNode('/child1'),
          createGroupNode('/child2'),
          createGroupNode('/child1/nested'),
        ],
      });

      const children = session.listChildren('/');
      expect(children).toHaveLength(2);
      expect(children.map(n => n.path).sort()).toEqual(['/child1', '/child2']);
    });

    it('should list direct children of nested group', () => {
      const session = createMockSession({
        nodes: [
          createGroupNode('/parent'),
          createGroupNode('/parent/child1'),
          createGroupNode('/parent/child2'),
          createGroupNode('/parent/child1/grandchild'),
        ],
      });

      const children = session.listChildren('/parent');
      expect(children).toHaveLength(2);
      expect(children.map(n => n.path).sort()).toEqual(['/parent/child1', '/parent/child2']);
    });

    it('should return empty for leaf node', () => {
      const session = createMockSession({
        nodes: [createGroupNode('/leaf')],
      });

      const children = session.listChildren('/leaf');
      expect(children).toEqual([]);
    });
  });

  describe('getChunk', () => {
    it('should return null for non-existent array', async () => {
      const session = createMockSession({ nodes: [] });
      const result = await session.getChunk('/missing', [0]);
      expect(result).toBeNull();
    });

    it('should return null for group node', async () => {
      const session = createMockSession({
        nodes: [createGroupNode('/group')],
      });

      const result = await session.getChunk('/group', [0]);
      expect(result).toBeNull();
    });

    it('should return null for array with no manifests', async () => {
      const session = createMockSession({
        nodes: [createArrayNode('/array')],
      });

      const result = await session.getChunk('/array', [0]);
      expect(result).toBeNull();
    });
  });

  describe('fetchChunkPayload (virtual chunks)', () => {
    it('should fetch virtual chunk via global fetch', async () => {
      // Mock the global fetch
      const mockData = new Uint8Array([1, 2, 3, 4, 5]);
      const mockResponse = {
        ok: true,
        status: 206,
        arrayBuffer: vi.fn().mockResolvedValue(mockData.buffer),
      };
      const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(mockResponse as any);

      // Create a session and access private method via any cast
      const session = createMockSession({ nodes: [] }) as any;

      const payload = {
        type: 'virtual' as const,
        location: 'https://example.com/data.bin',
        offset: 100,
        length: 5,
        checksumEtag: null,
        checksumLastModified: 0,
      };

      const result = await session.fetchChunkPayload(payload);

      expect(fetchSpy).toHaveBeenCalledWith('https://example.com/data.bin', {
        headers: { Range: 'bytes=100-104' },
      });
      expect(result).toEqual(mockData);

      fetchSpy.mockRestore();
    });

    it('should throw on failed virtual chunk fetch', async () => {
      const mockResponse = {
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
      };
      const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(mockResponse as any);

      const session = createMockSession({ nodes: [] }) as any;

      const payload = {
        type: 'virtual' as const,
        location: 'https://example.com/bad.bin',
        offset: 0,
        length: 10,
        checksumEtag: null,
        checksumLastModified: 0,
      };

      await expect(session.fetchChunkPayload(payload)).rejects.toThrow(
        'Failed to fetch virtual chunk: 500 Internal Server Error'
      );

      fetchSpy.mockRestore();
    });

    it('should handle 404 response for virtual chunks', async () => {
      const mockResponse = {
        ok: false,
        status: 404,
        statusText: 'Not Found',
      };
      const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(mockResponse as any);

      const session = createMockSession({ nodes: [] }) as any;

      const payload = {
        type: 'virtual' as const,
        location: 'https://example.com/missing.bin',
        offset: 0,
        length: 10,
        checksumEtag: null,
        checksumLastModified: 0,
      };

      await expect(session.fetchChunkPayload(payload)).rejects.toThrow(
        'Failed to fetch virtual chunk: 404 Not Found'
      );

      fetchSpy.mockRestore();
    });

    it('should return inline chunk directly', async () => {
      const session = createMockSession({ nodes: [] }) as any;

      const inlineData = new Uint8Array([10, 20, 30]);
      const payload = {
        type: 'inline' as const,
        data: inlineData,
      };

      const result = await session.fetchChunkPayload(payload);
      expect(result).toBe(inlineData);
    });
  });

  describe('abort signal handling', () => {
    it('should return null when signal is already aborted', async () => {
      const session = createMockSession({
        nodes: [createArrayNode('/array', { manifests: [] })],
      });

      const controller = new AbortController();
      controller.abort();

      const result = await session.getChunk('/array', [0], {
        signal: controller.signal,
      });

      expect(result).toBeNull();
    });

    it('should return null for non-existent array (not throw)', async () => {
      const session = createMockSession({ nodes: [] });

      const controller = new AbortController();

      // Should return null for non-existent path, not throw
      const result = await session.getChunk('/nonexistent', [0], {
        signal: controller.signal,
      });

      expect(result).toBeNull();
    });
  });
});
