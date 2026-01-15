/**
 * ReadSession - Read-only session for accessing icechunk data.
 */

import type { Storage, ByteRange } from '../storage/storage.js';
import { decompress } from 'fzstd';
import {
  parseHeader,
  validateFileType,
  getDataAfterHeader,
  FileType,
  CompressionAlgorithm,
  SpecVersion,
} from '../format/header.js';
import {
  getSnapshotPath,
  getManifestPath,
  getChunkPath,
} from '../format/constants.js';
import { encodeObjectId12 } from '../format/object-id.js';
import {
  parseSnapshot,
  parseManifest,
  findChunkRef,
  getChunkPayload,
  type Snapshot,
  type Manifest,
  type NodeSnapshot,
  type ChunkPayload,
  type ObjectId12,
} from '../format/flatbuffers/index.js';

/**
 * ReadSession provides read access to a specific snapshot.
 *
 * Use this class to:
 * - Get nodes (arrays/groups) by path
 * - Read chunk data
 * - Access Zarr metadata
 */
export class ReadSession {
  private storage: Storage;
  private snapshot: Snapshot;
  private specVersion: SpecVersion;
  private manifestCache: Map<string, Manifest> = new Map();

  private constructor(storage: Storage, snapshot: Snapshot, specVersion: SpecVersion) {
    this.storage = storage;
    this.snapshot = snapshot;
    this.specVersion = specVersion;
  }

  /**
   * Open a read session for a specific snapshot.
   *
   * @param storage - Storage backend
   * @param snapshotId - Snapshot ID (12 bytes)
   * @returns ReadSession instance
   */
  static async open(storage: Storage, snapshotId: Uint8Array): Promise<ReadSession> {
    const { snapshot, specVersion } = await ReadSession.loadSnapshot(storage, snapshotId);
    return new ReadSession(storage, snapshot, specVersion);
  }

  /** Load and parse a snapshot from storage */
  private static async loadSnapshot(
    storage: Storage,
    snapshotId: Uint8Array
  ): Promise<{ snapshot: Snapshot; specVersion: SpecVersion }> {
    const path = getSnapshotPath(encodeObjectId12(snapshotId));
    const data = await storage.getObject(path);

    // Parse header
    const header = parseHeader(data);
    validateFileType(header, FileType.Snapshot);

    // Decompress if needed
    let flatbufferData = getDataAfterHeader(data);
    if (header.compression === CompressionAlgorithm.Zstd) {
      flatbufferData = decompress(flatbufferData);
    }

    // Parse FlatBuffer
    return {
      snapshot: parseSnapshot(flatbufferData),
      specVersion: header.specVersion,
    };
  }

  /** Load and parse a manifest from storage */
  private async loadManifest(manifestId: ObjectId12): Promise<Manifest> {
    const idStr = encodeObjectId12(manifestId);

    // Check cache first
    const cached = this.manifestCache.get(idStr);
    if (cached) return cached;

    // Load from storage
    const path = getManifestPath(idStr);
    const data = await this.storage.getObject(path);

    // Parse header
    const header = parseHeader(data);
    validateFileType(header, FileType.Manifest);

    // Decompress if needed
    let flatbufferData = getDataAfterHeader(data);
    if (header.compression === CompressionAlgorithm.Zstd) {
      flatbufferData = decompress(flatbufferData);
    }

    // Parse FlatBuffer
    const manifest = parseManifest(flatbufferData);

    // Cache it
    this.manifestCache.set(idStr, manifest);

    return manifest;
  }

  /**
   * Get the snapshot ID.
   */
  getSnapshotId(): ObjectId12 {
    return this.snapshot.id;
  }

  /**
   * Get the spec version of the snapshot.
   */
  getSpecVersion(): SpecVersion {
    return this.specVersion;
  }

  /**
   * Get the commit message for this snapshot.
   */
  getMessage(): string {
    return this.snapshot.message;
  }

  /**
   * Get the timestamp when this snapshot was created.
   */
  getFlushedAt(): Date {
    // flushedAt is microseconds since epoch
    return new Date(Number(this.snapshot.flushedAt / 1000n));
  }

  /**
   * Get a node by path.
   *
   * @param path - Absolute path (e.g., "/array" or "/group/nested")
   * @returns NodeSnapshot or null if not found
   */
  getNode(path: string): NodeSnapshot | null {
    // Normalize path
    const normalizedPath = path.startsWith('/') ? path : `/${path}`;

    // Binary search since nodes are sorted by path
    return this.binarySearchNode(normalizedPath);
  }

  /**
   * List all nodes in the snapshot.
   *
   * @returns Array of all nodes
   */
  listNodes(): NodeSnapshot[] {
    return [...this.snapshot.nodes];
  }

  /**
   * List children of a group.
   *
   * @param parentPath - Path to the parent group (use "/" for root)
   * @returns Array of child nodes
   */
  listChildren(parentPath: string): NodeSnapshot[] {
    const normalizedParent = parentPath === '/' ? '' : parentPath;
    const prefix = normalizedParent + '/';

    return this.snapshot.nodes.filter((node) => {
      if (!node.path.startsWith(prefix)) return false;
      // Check it's a direct child (no more slashes after prefix)
      const rest = node.path.slice(prefix.length);
      return !rest.includes('/');
    });
  }

  /**
   * Get the Zarr metadata for a node.
   *
   * @param path - Path to the node
   * @returns Parsed JSON metadata or null if node not found
   */
  getMetadata(path: string): unknown | null {
    const node = this.getNode(path);
    if (!node) return null;

    const json = new TextDecoder().decode(node.userData);
    return JSON.parse(json);
  }

  /**
   * Get raw user data (Zarr metadata bytes) for a node.
   *
   * @param path - Path to the node
   * @returns Raw metadata bytes or null if node not found
   */
  getRawMetadata(path: string): Uint8Array | null {
    const node = this.getNode(path);
    if (!node) return null;
    return node.userData;
  }

  /**
   * Read chunk data for an array.
   *
   * @param path - Path to the array
   * @param coords - Chunk coordinates (N-dimensional)
   * @returns Chunk data bytes or null if not found
   */
  async getChunk(path: string, coords: number[]): Promise<Uint8Array | null> {
    const node = this.getNode(path);
    if (!node || node.nodeData.type !== 'array') {
      return null;
    }

    // Find the manifest that contains this chunk
    const arrayData = node.nodeData;

    for (const manifestRef of arrayData.manifests) {
      // Check if this manifest covers the requested coordinates
      if (!this.coordsInExtents(coords, manifestRef.extents)) {
        continue;
      }

      // Load the manifest
      const manifest = await this.loadManifest(manifestRef.objectId);

      // Find the chunk reference
      const chunkRef = findChunkRef(manifest, node.id, coords);
      if (!chunkRef) continue;

      // Fetch the chunk data based on payload type
      const payload = getChunkPayload(chunkRef);
      return this.fetchChunkPayload(payload);
    }

    return null;
  }

  /** Check if coordinates fall within extent ranges */
  private coordsInExtents(
    coords: number[],
    extents: Array<{ from: number; to: number }>
  ): boolean {
    if (coords.length !== extents.length) return false;

    for (let i = 0; i < coords.length; i++) {
      const { from, to } = extents[i];
      if (coords[i] < from || coords[i] >= to) {
        return false;
      }
    }

    return true;
  }

  /** Fetch chunk data based on payload type */
  private async fetchChunkPayload(payload: ChunkPayload): Promise<Uint8Array> {
    switch (payload.type) {
      case 'inline':
        return payload.data;

      case 'native': {
        const path = getChunkPath(encodeObjectId12(payload.chunkId));
        const range: ByteRange = {
          start: payload.offset,
          end: payload.offset + payload.length,
        };
        return this.storage.getObject(path, range);
      }

      case 'virtual': {
        // Virtual chunks reference external URLs
        // For now, use fetch directly (could be improved with virtual resolvers)
        const response = await fetch(payload.location, {
          headers: {
            Range: `bytes=${payload.offset}-${payload.offset + payload.length - 1}`,
          },
        });

        if (!response.ok && response.status !== 206) {
          throw new Error(
            `Failed to fetch virtual chunk: ${response.status} ${response.statusText}`
          );
        }

        return new Uint8Array(await response.arrayBuffer());
      }
    }
  }

  /** Binary search for a node by path */
  private binarySearchNode(path: string): NodeSnapshot | null {
    const nodes = this.snapshot.nodes;
    let low = 0;
    let high = nodes.length - 1;

    while (low <= high) {
      const mid = Math.floor((low + high) / 2);
      const cmp = nodes[mid].path.localeCompare(path);

      if (cmp === 0) {
        return nodes[mid];
      } else if (cmp < 0) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }

    return null;
  }
}
