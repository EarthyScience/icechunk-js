/**
 * TypeScript types matching the icechunk FlatBuffer schemas.
 *
 * These types represent the parsed/decoded data, not the binary format.
 */

// =============================================================================
// Common types (from common.fbs)
// =============================================================================

/** 12-byte object ID (SnapshotId, ManifestId, ChunkId) */
export type ObjectId12 = Uint8Array; // Always 12 bytes

/** 8-byte object ID (NodeId) */
export type ObjectId8 = Uint8Array; // Always 8 bytes

/**
 * Validate and cast a Uint8Array to ObjectId12.
 * @throws Error if the array is not exactly 12 bytes
 */
export function asObjectId12(bytes: Uint8Array): ObjectId12 {
  if (bytes.length !== 12) {
    throw new Error(
      `Invalid ObjectId12: expected 12 bytes, got ${bytes.length}`,
    );
  }
  return bytes as ObjectId12;
}

/**
 * Validate and cast a Uint8Array to ObjectId8.
 * @throws Error if the array is not exactly 8 bytes
 */
export function asObjectId8(bytes: Uint8Array): ObjectId8 {
  if (bytes.length !== 8) {
    throw new Error(`Invalid ObjectId8: expected 8 bytes, got ${bytes.length}`);
  }
  return bytes as ObjectId8;
}

/** Metadata key-value pair */
export interface MetadataItem {
  name: string;
  /** Value serialized as MessagePack (v1) or FlexBuffers (v2) */
  value: Uint8Array;
}

// =============================================================================
// Manifest types (from manifest.fbs)
// =============================================================================

/** Reference to a chunk - can be inline, native, or virtual */
export interface ChunkRef {
  /** Chunk coordinates (N-dimensional) */
  index: number[];

  /** Inline chunk data (if present, this is an inline chunk) */
  inline: Uint8Array | null;

  /** Byte offset within the chunk file or virtual location */
  offset: number;

  /** Byte length of the chunk data */
  length: number;

  /** Chunk ID for native chunks (points to file in chunks/) */
  chunkId: ObjectId12 | null;

  /** URL for virtual chunks */
  location: string | null;

  /** ETag checksum for virtual chunks */
  checksumEtag: string | null;

  /** Last modified timestamp (seconds since epoch) for virtual chunks */
  checksumLastModified: number;
}

/** Chunk references for a single array */
export interface ArrayManifest {
  /** Node ID of the array */
  nodeId: ObjectId8;

  /** Chunk references, sorted by index */
  refs: ChunkRef[];
}

/** Manifest containing chunk references for multiple arrays */
export interface Manifest {
  /** Manifest ID */
  id: ObjectId12;

  /** Array manifests, sorted by nodeId */
  arrays: ArrayManifest[];
}

// =============================================================================
// Snapshot types (from snapshot.fbs)
// =============================================================================

/** Info about a manifest file */
export interface ManifestFileInfo {
  /** Manifest ID */
  id: ObjectId12;

  /** Size in bytes */
  sizeBytes: number;

  /** Number of chunk refs in the manifest */
  numChunkRefs: number;
}

/** Range of chunk indices along one dimension */
export interface ChunkIndexRange {
  /** Inclusive start */
  from: number;

  /** Exclusive end */
  to: number;
}

/** Reference to a manifest with extent information */
export interface ManifestRef {
  /** Manifest object ID */
  objectId: ObjectId12;

  /** Chunk index ranges per dimension */
  extents: ChunkIndexRange[];
}

/** Shape of one dimension */
export interface DimensionShape {
  /** Length of the array along this dimension */
  arrayLength: number;

  /** Chunk size along this dimension (v1 format; approximate in v2) */
  chunkLength: number;

  /** Number of chunks along this dimension (v2 format) */
  numChunks?: number;
}

/** Group node data (empty - just a marker) */
export interface GroupNodeData {
  type: "group";
}

/** Array node data */
export interface ArrayNodeData {
  type: "array";

  /** Shape per dimension */
  shape: DimensionShape[];

  /** Optional dimension names */
  dimensionNames: (string | null)[];

  /** Manifest references */
  manifests: ManifestRef[];
}

/** Node data - either group or array */
export type NodeData = GroupNodeData | ArrayNodeData;

/** Snapshot of a single node (array or group) */
export interface NodeSnapshot {
  /** Node ID */
  id: ObjectId8;

  /** Absolute path in the repository */
  path: string;

  /** User data (typically Zarr metadata JSON) */
  userData: Uint8Array;

  /** Node-specific data */
  nodeData: NodeData;
}

/** Complete snapshot of a repository state */
export interface Snapshot {
  /** Snapshot ID */
  id: ObjectId12;

  /** Parent snapshot ID (null for root snapshot) */
  parentId: ObjectId12 | null;

  /** Nodes in the snapshot, sorted by path */
  nodes: NodeSnapshot[];

  /** Timestamp when flushed (microseconds since epoch) */
  flushedAt: bigint;

  /** Commit message */
  message: string;

  /** Snapshot metadata */
  metadata: MetadataItem[];

  /** All manifest files referenced by this snapshot */
  manifestFiles: ManifestFileInfo[];
}

// =============================================================================
// Chunk payload types (derived from ChunkRef for easier handling)
// =============================================================================

/** Inline chunk - data embedded in manifest */
export interface InlineChunkPayload {
  type: "inline";
  data: Uint8Array;
}

/** Native chunk - stored in repository's chunk storage */
export interface NativeChunkPayload {
  type: "native";
  chunkId: ObjectId12;
  offset: number;
  length: number;
}

/** Virtual chunk - stored externally */
export interface VirtualChunkPayload {
  type: "virtual";
  location: string;
  offset: number;
  length: number;
  checksumEtag: string | null;
  checksumLastModified: number;
}

/** Union of all chunk payload types */
export type ChunkPayload =
  | InlineChunkPayload
  | NativeChunkPayload
  | VirtualChunkPayload;

// =============================================================================
// Transaction log types (from transaction_log.fbs)
// =============================================================================

/** Chunk coordinates that were updated */
export interface UpdatedChunkIndices {
  coords: number[];
}

/** Updated chunks info for a single array */
export interface ArrayUpdatedChunksInfo {
  nodeId: ObjectId8;
  chunks: UpdatedChunkIndices[];
}

/** A node move operation */
export interface MoveOperationInfo {
  from: string;
  to: string;
}

/** A transaction log entry describing changes in a snapshot */
export interface TransactionLogEntry {
  id: ObjectId12;
  newGroups: ObjectId8[];
  newArrays: ObjectId8[];
  deletedGroups: ObjectId8[];
  deletedArrays: ObjectId8[];
  updatedArrays: ObjectId8[];
  updatedGroups: ObjectId8[];
  updatedChunks: ArrayUpdatedChunksInfo[];
  movedNodes: MoveOperationInfo[];
}
