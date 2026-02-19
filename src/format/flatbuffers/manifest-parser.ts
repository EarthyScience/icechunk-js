/**
 * Parser for icechunk Manifest FlatBuffer format.
 *
 * Uses flatc-generated TypeScript classes for type-safe field access.
 */

import { ByteBuffer } from "flatbuffers";
import { Manifest as FbsManifest } from "./generated/manifest.js";
import { ArrayManifest as FbsArrayManifest } from "./generated/array-manifest.js";
import { ChunkRef as FbsChunkRef } from "./generated/chunk-ref.js";
import {
  asObjectId12,
  asObjectId8,
  type Manifest,
  type ArrayManifest,
  type ChunkRef,
  type ChunkPayload,
  type ObjectId8,
} from "./types.js";

/** Parse a Manifest from FlatBuffer data */
export function parseManifest(data: Uint8Array): Manifest {
  const bb = new ByteBuffer(data);
  const fbsManifest = FbsManifest.getRootAsManifest(bb);

  // Parse ID (required)
  const idObj = fbsManifest.id();
  if (!idObj) throw new Error("Manifest missing required id field");
  const id = asObjectId12(
    idObj.bb!.bytes().slice(idObj.bb_pos, idObj.bb_pos + 12),
  );

  // Parse arrays
  const arraysLength = fbsManifest.arraysLength();
  const arrays: ArrayManifest[] = [];
  for (let i = 0; i < arraysLength; i++) {
    const fbsArray = fbsManifest.arrays(i);
    if (fbsArray) {
      arrays.push(parseArrayManifest(fbsArray));
    }
  }

  return { id, arrays };
}

function parseArrayManifest(fbsArray: FbsArrayManifest): ArrayManifest {
  // Parse node_id (required)
  const nodeIdObj = fbsArray.nodeId();
  if (!nodeIdObj)
    throw new Error("ArrayManifest missing required node_id field");
  const nodeId = asObjectId8(
    nodeIdObj.bb!.bytes().slice(nodeIdObj.bb_pos, nodeIdObj.bb_pos + 8),
  );

  // Parse refs
  const refsLength = fbsArray.refsLength();
  const refs: ChunkRef[] = [];
  for (let i = 0; i < refsLength; i++) {
    const fbsRef = fbsArray.refs(i);
    if (fbsRef) {
      refs.push(parseChunkRef(fbsRef));
    }
  }

  return { nodeId, refs };
}

function parseChunkRef(fbsRef: FbsChunkRef): ChunkRef {
  // Parse index (required, vector of uint32)
  const indexLength = fbsRef.indexLength();
  const index: number[] = [];
  for (let i = 0; i < indexLength; i++) {
    index.push(fbsRef.index(i)!);
  }

  // Parse inline (optional byte vector)
  const inlineData = fbsRef.inlineArray();
  const inline = inlineData ? new Uint8Array(inlineData) : null;

  // Parse offset and length
  const offset = Number(fbsRef.offset());
  const length = Number(fbsRef.length());

  // Parse chunk_id (optional inline struct)
  const chunkIdObj = fbsRef.chunkId();
  const chunkId = chunkIdObj
    ? asObjectId12(
        chunkIdObj.bb!.bytes().slice(chunkIdObj.bb_pos, chunkIdObj.bb_pos + 12),
      )
    : null;

  // Parse location (optional string)
  const location = fbsRef.location();

  // Parse checksum fields
  const checksumEtag = fbsRef.checksumEtag();
  const checksumLastModified = fbsRef.checksumLastModified();

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
