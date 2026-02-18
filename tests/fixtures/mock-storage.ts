/**
 * Mock storage for testing.
 */

import type {
  Storage,
  ByteRange,
  RequestOptions,
} from "../../src/storage/storage.js";
import { NotFoundError, StorageError } from "../../src/storage/storage.js";
import {
  HEADER_SIZE,
  MAGIC_BYTES,
  SpecVersion,
  FileType,
  CompressionAlgorithm,
} from "../../src/format/header.js";
import { encodeObjectId12 } from "../../src/format/object-id.js";

/** Files stored in the mock storage */
export interface MockFiles {
  [path: string]: Uint8Array | string | object;
}

/**
 * MockStorage - A simple in-memory storage for testing.
 *
 * Files can be provided as:
 * - Uint8Array: Raw bytes
 * - string: Will be encoded as UTF-8
 * - object: Will be JSON serialized
 */
export class MockStorage implements Storage {
  private files: Map<string, Uint8Array> = new Map();
  private _requestLog: string[] = [];

  constructor(files: MockFiles = {}) {
    this.setFiles(files);
  }

  /** Get log of all paths requested via getObject/exists */
  get requestLog(): readonly string[] {
    return this._requestLog;
  }

  /** Clear the request log */
  clearRequestLog(): void {
    this._requestLog = [];
  }

  /** Set files in the mock storage (also clears request log) */
  setFiles(files: MockFiles): void {
    this.files.clear();
    this._requestLog = [];
    for (const [path, data] of Object.entries(files)) {
      if (data instanceof Uint8Array) {
        this.files.set(path, data);
      } else if (typeof data === "string") {
        this.files.set(path, new TextEncoder().encode(data));
      } else {
        this.files.set(path, new TextEncoder().encode(JSON.stringify(data)));
      }
    }
  }

  /** Add a single file */
  addFile(path: string, data: Uint8Array | string | object): void {
    if (data instanceof Uint8Array) {
      this.files.set(path, data);
    } else if (typeof data === "string") {
      this.files.set(path, new TextEncoder().encode(data));
    } else {
      this.files.set(path, new TextEncoder().encode(JSON.stringify(data)));
    }
  }

  /** Get all file paths */
  getPaths(): string[] {
    return [...this.files.keys()];
  }

  async getObject(
    path: string,
    range?: ByteRange,
    _options?: RequestOptions,
  ): Promise<Uint8Array> {
    this._requestLog.push(`getObject:${path}`);
    const data = this.files.get(path);
    if (!data) {
      throw new NotFoundError(`Object not found: ${path}`);
    }

    if (range) {
      return data.slice(range.start, range.end);
    }

    return data;
  }

  async exists(path: string, _options?: RequestOptions): Promise<boolean> {
    this._requestLog.push(`exists:${path}`);
    return this.files.has(path);
  }

  async *listPrefix(prefix: string): AsyncIterable<string> {
    for (const path of this.files.keys()) {
      if (path.startsWith(prefix)) {
        yield path;
      }
    }
  }
}

/**
 * Create a mock storage that throws on listPrefix (simulates HTTP storage).
 */
export class MockStorageNoList extends MockStorage {
  async *listPrefix(_prefix: string): AsyncIterable<string> {
    throw new StorageError("Listing not supported over HTTP");
  }
}

/**
 * Create a valid icechunk file header.
 */
export function createMockHeader(
  options: {
    implementation?: string;
    specVersion?: SpecVersion;
    fileType?: FileType;
    compression?: CompressionAlgorithm;
  } = {},
): Uint8Array {
  const buffer = new Uint8Array(HEADER_SIZE);

  // Magic bytes: "ICE🧊CHUNK"
  buffer.set(MAGIC_BYTES, 0);

  // Implementation (24 bytes, space-padded)
  const impl = (options.implementation ?? "test-impl").padEnd(24, " ");
  buffer.set(new TextEncoder().encode(impl), 12);

  // Spec version, file type, compression
  buffer[36] = options.specVersion ?? SpecVersion.V1_0;
  buffer[37] = options.fileType ?? FileType.Snapshot;
  buffer[38] = options.compression ?? CompressionAlgorithm.None;

  return buffer;
}

/**
 * Create a mock ref.json content for a branch or tag.
 */
export function createMockRefJson(snapshotId: Uint8Array): object {
  return {
    snapshot: encodeObjectId12(snapshotId),
  };
}

/**
 * Create a mock snapshot ID (12 random-ish bytes).
 */
export function createMockSnapshotId(seed: number = 0): Uint8Array {
  const id = new Uint8Array(12);
  for (let i = 0; i < 12; i++) {
    id[i] = (seed + i * 17) % 256;
  }
  return id;
}

/**
 * Create a mock node ID (8 random-ish bytes).
 */
export function createMockNodeId(seed: number = 0): Uint8Array {
  const id = new Uint8Array(8);
  for (let i = 0; i < 8; i++) {
    id[i] = (seed + i * 23) % 256;
  }
  return id;
}

/**
 * Create a mock manifest ID (12 random-ish bytes).
 */
export function createMockManifestId(seed: number = 0): Uint8Array {
  return createMockSnapshotId(seed + 100);
}

/**
 * Create a mock chunk ID (12 random-ish bytes).
 */
export function createMockChunkId(seed: number = 0): Uint8Array {
  return createMockSnapshotId(seed + 200);
}

/**
 * Create Zarr v3 array metadata JSON.
 */
export function createMockArrayMetadata(
  options: {
    shape?: number[];
    chunks?: number[];
    dtype?: string;
    fillValue?: unknown;
  } = {},
): object {
  const shape = options.shape ?? [100, 100];
  const chunks = options.chunks ?? [10, 10];

  return {
    zarr_format: 3,
    node_type: "array",
    shape,
    data_type: options.dtype ?? "float32",
    chunk_grid: {
      name: "regular",
      configuration: {
        chunk_shape: chunks,
      },
    },
    chunk_key_encoding: {
      name: "default",
      configuration: {
        separator: "/",
      },
    },
    fill_value: options.fillValue ?? 0,
    codecs: [{ name: "bytes", configuration: { endian: "little" } }],
  };
}

/**
 * Create Zarr v3 group metadata JSON.
 */
export function createMockGroupMetadata(): object {
  return {
    zarr_format: 3,
    node_type: "group",
  };
}

// =============================================================================
// FlatBuffer builders for creating test data
// =============================================================================

/**
 * FlatBuffer binary builder for creating test fixtures.
 *
 * FlatBuffers are built from the back to the front:
 * 1. Write data/strings/vectors at increasing offsets
 * 2. Build vtables
 * 3. Write root table offset at position 0
 */
class FlatBufferBuilder {
  private buffer: number[] = [];
  private offset = 0;

  /** Align to the given byte boundary */
  private align(alignment: number): void {
    while (this.offset % alignment !== 0) {
      this.buffer.push(0);
      this.offset++;
    }
  }

  /** Write a uint8 */
  writeUint8(value: number): number {
    const pos = this.offset;
    this.buffer.push(value & 0xff);
    this.offset++;
    return pos;
  }

  /** Write a uint16 (little-endian) */
  writeUint16(value: number): number {
    this.align(2);
    const pos = this.offset;
    this.buffer.push(value & 0xff);
    this.buffer.push((value >> 8) & 0xff);
    this.offset += 2;
    return pos;
  }

  /** Write a uint32 (little-endian) */
  writeUint32(value: number): number {
    this.align(4);
    const pos = this.offset;
    this.buffer.push(value & 0xff);
    this.buffer.push((value >> 8) & 0xff);
    this.buffer.push((value >> 16) & 0xff);
    this.buffer.push((value >> 24) & 0xff);
    this.offset += 4;
    return pos;
  }

  /** Write a uint64 (little-endian) */
  writeUint64(value: bigint): number {
    this.align(8);
    const pos = this.offset;
    for (let i = 0; i < 8; i++) {
      this.buffer.push(Number((value >> BigInt(i * 8)) & 0xffn));
    }
    this.offset += 8;
    return pos;
  }

  /** Write raw bytes */
  writeBytes(bytes: Uint8Array): number {
    const pos = this.offset;
    for (const b of bytes) {
      this.buffer.push(b);
    }
    this.offset += bytes.length;
    return pos;
  }

  /** Write a length-prefixed string */
  writeString(str: string): number {
    const encoded = new TextEncoder().encode(str);
    this.align(4);
    const pos = this.offset;
    this.writeUint32(encoded.length);
    this.writeBytes(encoded);
    this.writeUint8(0); // null terminator
    return pos;
  }

  /** Write a byte vector (length-prefixed) */
  writeByteVector(bytes: Uint8Array): number {
    this.align(4);
    const pos = this.offset;
    this.writeUint32(bytes.length);
    this.writeBytes(bytes);
    return pos;
  }

  /** Get current offset */
  getOffset(): number {
    return this.offset;
  }

  /** Get the buffer as Uint8Array */
  finish(): Uint8Array {
    return new Uint8Array(this.buffer);
  }
}

/**
 * Build a minimal FlatBuffer snapshot for testing.
 *
 * This creates a valid snapshot that can be parsed by parseSnapshot().
 */
export function buildMinimalSnapshot(
  options: {
    id: Uint8Array;
    message?: string;
    flushedAt?: bigint;
    nodes?: Array<{
      id: Uint8Array;
      path: string;
      userData: Uint8Array;
      isArray?: boolean;
    }>;
  } = { id: createMockSnapshotId() },
): Uint8Array {
  // FlatBuffers are complex - we need to build:
  // 1. Vtable
  // 2. Table with offsets to strings/vectors
  // 3. The actual data

  const fb = new FlatBufferBuilder();

  // Reserve space for root offset (will be filled in later)
  fb.writeUint32(0);

  // Write strings and vectors first
  const messageOffset = fb.writeString(options.message ?? "test commit");

  // Write nodes if any
  const nodeOffsets: number[] = [];
  for (const node of options.nodes ?? []) {
    // Build each node table
    const pathOffset = fb.writeString(node.path);
    const userDataOffset = fb.writeByteVector(node.userData);

    // Node vtable: id(0), path(1), userData(2), nodeData(3,4)
    // Vtable: size(2), tableSize(2), then field offsets
    const nodeVtableSize = 4 + 5 * 2; // header + 5 fields (type + value for union)
    fb.writeUint16(nodeVtableSize); // vtable size
    fb.writeUint16(20 + 8 + 8); // table size (approximate)

    // Field offsets (0 = not present)
    fb.writeUint16(4); // id at offset 4
    fb.writeUint16(4 + 8); // path at offset 12
    fb.writeUint16(4 + 8 + 4); // userData at offset 16
    fb.writeUint16(4 + 8 + 4 + 4); // nodeData type at offset 20
    fb.writeUint16(0); // nodeData value (not needed for group)

    const vtablePos = fb.getOffset() - nodeVtableSize;

    // Write node table
    const nodeTableStart = fb.getOffset();
    fb.writeUint32(nodeTableStart - vtablePos); // vtable offset (as signed)
    fb.writeBytes(node.id); // id (8 bytes inline)

    // Write path offset (relative)
    fb.writeUint32(fb.getOffset() - pathOffset);

    // Write userData offset (relative)
    fb.writeUint32(fb.getOffset() - userDataOffset);

    // nodeData union type (2 = Group, 1 = Array)
    fb.writeUint8(node.isArray ? 1 : 2);

    nodeOffsets.push(nodeTableStart);
  }

  // Write nodes vector if we have nodes
  let nodesVectorOffset = 0;
  if (nodeOffsets.length > 0) {
    nodesVectorOffset = fb.getOffset();
    fb.writeUint32(nodeOffsets.length);
    for (const offset of nodeOffsets) {
      // Offsets in vector are relative to their position
      fb.writeUint32(offset);
    }
  }

  // Build main snapshot vtable
  // Fields: id(0), parentId(1), nodes(2), flushedAt(3), message(4), metadata(5), manifestFiles(6)
  const vtableSize = 4 + 7 * 2;
  fb.writeUint16(vtableSize);
  fb.writeUint16(32); // approximate table size

  // Field offsets
  fb.writeUint16(4); // id at offset 4
  fb.writeUint16(0); // parentId not present
  fb.writeUint16(nodesVectorOffset > 0 ? 4 + 12 : 0); // nodes
  fb.writeUint16(4 + 12 + 4); // flushedAt
  fb.writeUint16(4 + 12 + 4 + 8); // message
  fb.writeUint16(0); // metadata not present
  fb.writeUint16(0); // manifestFiles not present

  const vtablePos = fb.getOffset() - vtableSize;

  // Write main table
  const tableStart = fb.getOffset();
  fb.writeUint32(tableStart - vtablePos); // vtable offset

  // id (12 bytes inline struct)
  fb.writeBytes(options.id);

  // nodes vector offset (relative)
  if (nodesVectorOffset > 0) {
    fb.writeUint32(nodesVectorOffset - fb.getOffset());
  }

  // flushedAt (uint64)
  fb.writeUint64(options.flushedAt ?? BigInt(Date.now() * 1000));

  // message offset (relative)
  fb.writeUint32(fb.getOffset() - messageOffset);

  // Update root offset at position 0
  const result = fb.finish();
  const view = new DataView(result.buffer);
  view.setUint32(0, tableStart, true);

  return result;
}

/**
 * Create a complete mock snapshot file (header + FlatBuffer).
 *
 * Note: The FlatBuffer format is complex and this creates a simplified
 * version that may not work with all parsers. For full testing, consider
 * using real fixtures generated from the Rust implementation.
 */
export function createMockSnapshotFile(
  _options: {
    id?: Uint8Array;
    message?: string;
  } = {},
): Uint8Array {
  const header = createMockHeader({
    fileType: FileType.Snapshot,
    specVersion: SpecVersion.V1_0,
  });

  // For now, return just the header
  // Full FlatBuffer implementation would require more complex building
  // This is a placeholder - tests should mock at higher levels
  return header;
}
