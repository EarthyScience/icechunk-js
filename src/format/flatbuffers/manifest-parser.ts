/**
 * Parser for icechunk Manifest FlatBuffer format.
 *
 * Field indices based on manifest.fbs schema order.
 */

import { parseRootTable, TableReader } from "./reader.js";
import {
  asObjectId12,
  asObjectId8,
  type Manifest,
  type ArrayManifest,
  type ChunkRef,
  type ChunkPayload,
  type ObjectId8,
} from "./types.js";

// Manifest field indices
const MANIFEST_ID = 0;
const MANIFEST_ARRAYS = 1;

// ArrayManifest field indices
const ARRAY_MANIFEST_NODE_ID = 0;
const ARRAY_MANIFEST_REFS = 1;

// ChunkRef field indices
const CHUNK_REF_INDEX = 0;
const CHUNK_REF_INLINE = 1;
const CHUNK_REF_OFFSET = 2;
const CHUNK_REF_LENGTH = 3;
const CHUNK_REF_CHUNK_ID = 4;
const CHUNK_REF_LOCATION = 5;
const CHUNK_REF_CHECKSUM_ETAG = 6;
const CHUNK_REF_CHECKSUM_LAST_MODIFIED = 7;

// Struct sizes
const OBJECT_ID_12_SIZE = 12;
const OBJECT_ID_8_SIZE = 8;

/** Parse a Manifest from FlatBuffer data */
export function parseManifest(data: Uint8Array): Manifest {
  const root = parseRootTable(data);

  // Parse ID (required)
  const idBytes = root.readInlineStruct(MANIFEST_ID, OBJECT_ID_12_SIZE);
  if (!idBytes) throw new Error("Manifest missing required id field");
  const id = asObjectId12(idBytes);

  // Parse arrays
  const arraysLength = root.getVectorLength(MANIFEST_ARRAYS);
  const arrays: ArrayManifest[] = [];
  for (let i = 0; i < arraysLength; i++) {
    const arrayTable = root.getVectorTable(MANIFEST_ARRAYS, i);
    if (arrayTable) {
      arrays.push(parseArrayManifest(arrayTable));
    }
  }

  return { id, arrays };
}

function parseArrayManifest(table: TableReader): ArrayManifest {
  // Parse node_id (required)
  const nodeIdBytes = table.readInlineStruct(
    ARRAY_MANIFEST_NODE_ID,
    OBJECT_ID_8_SIZE,
  );
  if (!nodeIdBytes)
    throw new Error("ArrayManifest missing required node_id field");
  const nodeId = asObjectId8(nodeIdBytes);

  // Parse refs
  const refsLength = table.getVectorLength(ARRAY_MANIFEST_REFS);
  const refs: ChunkRef[] = [];
  for (let i = 0; i < refsLength; i++) {
    const refTable = table.getVectorTable(ARRAY_MANIFEST_REFS, i);
    if (refTable) {
      refs.push(parseChunkRef(refTable));
    }
  }

  return { nodeId, refs };
}

function parseChunkRef(table: TableReader): ChunkRef {
  // Parse index (required, vector of uint32)
  const index = table.readUint32Vector(CHUNK_REF_INDEX) ?? [];

  // Parse inline (optional byte vector)
  const inline = table.readByteVector(CHUNK_REF_INLINE);

  // Parse offset and length
  const offset = Number(table.readUint64(CHUNK_REF_OFFSET, 0n));
  const length = Number(table.readUint64(CHUNK_REF_LENGTH, 0n));

  // Parse chunk_id (optional inline struct)
  const chunkIdBytes = table.readInlineStruct(
    CHUNK_REF_CHUNK_ID,
    OBJECT_ID_12_SIZE,
  );
  const chunkId = chunkIdBytes ? asObjectId12(chunkIdBytes) : null;

  // Parse location (optional string)
  const location = table.readString(CHUNK_REF_LOCATION);

  // Parse checksum fields
  const checksumEtag = table.readString(CHUNK_REF_CHECKSUM_ETAG);
  const checksumLastModified = table.readUint32(
    CHUNK_REF_CHECKSUM_LAST_MODIFIED,
    0,
  );

  return {
    index,
    inline,
    offset,
    length,
    chunkId,
    location,
    checksumEtag,
    checksumLastModified,
  };
}

/**
 * Find a chunk reference by node ID and coordinates.
 *
 * Uses binary search since both arrays and refs are sorted.
 */
export function findChunkRef(
  manifest: Manifest,
  nodeId: ObjectId8,
  coords: number[],
): ChunkRef | null {
  // Binary search for the array manifest (arrays are sorted by nodeId)
  const arrayManifest = binarySearchArrayManifest(manifest.arrays, nodeId);

  if (!arrayManifest) return null;

  // Binary search for the chunk ref (refs are sorted by index)
  return binarySearchChunkRef(arrayManifest.refs, coords);
}

/** Binary search for an array manifest by node ID */
function binarySearchArrayManifest(
  arrays: ArrayManifest[],
  nodeId: ObjectId8,
): ArrayManifest | null {
  let low = 0;
  let high = arrays.length - 1;

  while (low <= high) {
    const mid = Math.floor((low + high) / 2);
    const cmp = compareBytes(arrays[mid].nodeId, nodeId);

    if (cmp === 0) {
      return arrays[mid];
    } else if (cmp < 0) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  return null;
}

/** Compare two byte arrays lexicographically */
function compareBytes(a: Uint8Array, b: Uint8Array): number {
  const minLen = Math.min(a.length, b.length);
  for (let i = 0; i < minLen; i++) {
    if (a[i] !== b[i]) return a[i] - b[i];
  }
  return a.length - b.length;
}

/** Compare two coordinate arrays lexicographically */
function compareCoords(a: number[], b: number[]): number {
  const minLen = Math.min(a.length, b.length);
  for (let i = 0; i < minLen; i++) {
    if (a[i] !== b[i]) return a[i] - b[i];
  }
  return a.length - b.length;
}

/** Binary search for a chunk ref by coordinates */
function binarySearchChunkRef(
  refs: ChunkRef[],
  coords: number[],
): ChunkRef | null {
  let low = 0;
  let high = refs.length - 1;

  while (low <= high) {
    const mid = Math.floor((low + high) / 2);
    const cmp = compareCoords(refs[mid].index, coords);

    if (cmp === 0) {
      return refs[mid];
    } else if (cmp < 0) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  return null;
}

/** Extract the payload type from a ChunkRef */
export function getChunkPayload(ref: ChunkRef): ChunkPayload {
  if (ref.inline !== null) {
    return { type: "inline", data: ref.inline };
  }

  if (ref.chunkId !== null) {
    return {
      type: "native",
      chunkId: ref.chunkId,
      offset: ref.offset,
      length: ref.length,
    };
  }

  if (ref.location !== null) {
    return {
      type: "virtual",
      location: ref.location,
      offset: ref.offset,
      length: ref.length,
      checksumEtag: ref.checksumEtag,
      checksumLastModified: ref.checksumLastModified,
    };
  }

  throw new Error("Invalid ChunkRef: no inline, chunkId, or location");
}
