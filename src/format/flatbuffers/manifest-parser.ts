/**
 * Parser for icechunk Manifest FlatBuffer format.
 *
 * Uses flatc-generated TypeScript classes for type-safe field access.
 */

import { ByteBuffer } from "flatbuffers";
import { decompress } from "../../vendor/fzstd/index.js";
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

/** compression_algorithm value for zstd dictionary-compressed locations. */
const COMPRESSION_ALG_ZSTD_DICT = 1;

/** Upper bound on a decompressed location, matching the Rust
 * `MAX_DECOMPRESSED_LOCATION_SIZE`. */
const MAX_DECOMPRESSED_LOCATION_SIZE = 1024;

/**
 * Upper bound on the bytes the vendored fzstd decoder will allocate for
 * `frame`, computed from the frame header *without* decompressing: the larger
 * of the up-front frame buffer (content size, falling back to window size, per
 * `rzfh`) and the sum of the per-block output buffers (per `rzb`). Requires
 * `frame` to be a single complete frame covering the whole input; returns
 * `Infinity` for a missing/short/unrecognized header, a malformed/reserved
 * block, or any trailing bytes (a concatenated second frame), so callers reject
 * rather than trust it.
 *
 * Used to bound allocation *before* decompressing an untrusted
 * `compressed_location`: the size check on the decompressed output alone is too
 * late, since the decoder allocates per the header — including RLE blocks that
 * expand a single input byte to a huge declared size — before that check runs.
 */
function zstdFrameAllocSize(frame: Uint8Array): number {
  // Magic number 0xFD2FB528 (little-endian), then the frame header descriptor.
  if (
    frame.length < 5 ||
    frame[0] !== 0x28 ||
    frame[1] !== 0xb5 ||
    frame[2] !== 0x2f ||
    frame[3] !== 0xfd
  ) {
    return Infinity;
  }
  const flg = frame[4];
  const singleSegment = (flg >> 5) & 1;
  const contentChecksum = (flg >> 2) & 1;
  const dictIdFlag = flg & 3;
  const contentSizeFlag = flg >> 6;

  let pos = 5;
  let windowSize = 0;
  if (!singleSegment) {
    if (pos >= frame.length) return Infinity;
    const wd = frame[pos++];
    // Arithmetic (not bitwise `1 <<`/`>> 3`): the exponent can reach 41, which
    // would overflow 32-bit JS shifts and let a crafted window descriptor
    // compute a bogus small size that slips past the bound below.
    const base = 2 ** (10 + (wd >> 3));
    windowSize = base + (base / 8) * (wd & 7);
  }
  pos += dictIdFlag === 3 ? 4 : dictIdFlag; // skip dictionary id

  // Frame content size: 0/1/2/4/8 bytes per the flags (matching fzstd's rzfh).
  const fcsBytes = contentSizeFlag ? 1 << contentSizeFlag : singleSegment;
  let contentSize = 0;
  if (fcsBytes) {
    if (pos + fcsBytes > frame.length) return Infinity;
    for (let i = 0; i < fcsBytes; i++) {
      contentSize += frame[pos + i] * 2 ** (8 * i);
    }
    if (contentSizeFlag === 1) contentSize += 256;
    if (singleSegment) windowSize = contentSize;
  }
  pos += fcsBytes;

  // Walk the data blocks to the frame end, summing the output each block makes
  // the decoder allocate. This is essential, not just an EOF check: an RLE
  // block costs one input byte but expands to its declared size (up to ~2MB),
  // and a compressed block expands to one block-max regardless of input — so a
  // frame whose header advertises a tiny window/content size can still force a
  // huge allocation. We mirror the per-block allocations in the vendored fzstd
  // `rzb`: RLE/raw -> the block size, compressed -> min(windowSize, 128KiB).
  //
  // Also require exactly one complete frame covering the whole input:
  // `decompress()` decodes *every* concatenated frame and allocates per frame,
  // so trailing bytes (a second frame advertising a huge size) must be rejected.
  let blockOutput = 0;
  for (;;) {
    if (pos + 3 > frame.length) return Infinity;
    const blockHeader =
      frame[pos] | (frame[pos + 1] << 8) | (frame[pos + 2] << 16);
    pos += 3;
    const lastBlock = blockHeader & 1;
    const blockType = (blockHeader >> 1) & 3;
    const blockSize = blockHeader >> 3;
    if (blockType === 3) return Infinity; // reserved block type
    if (blockType === 1) {
      // RLE: one input byte, but the decoder allocates `blockSize` output bytes.
      blockOutput += blockSize;
      pos += 1;
    } else {
      // Raw (0): `blockSize` literal bytes, present in the input.
      // Compressed (2): decoder allocates one block-max, min(windowSize, 128KiB).
      blockOutput += blockType === 0 ? blockSize : Math.min(windowSize, 131072);
      pos += blockSize;
    }
    if (pos > frame.length) return Infinity;
    if (lastBlock) break;
  }
  if (contentChecksum) pos += 4;
  if (pos !== frame.length) return Infinity; // trailing/concatenated frame

  // The decoder allocates both the up-front frame buffer (content/window size)
  // and the per-block buffers; bound by the larger of the two.
  return Math.max(contentSize || windowSize, blockOutput);
}

/**
 * Decode a ChunkRef's `compressed_location` byte vector into a location string.
 * Throws if the manifest has no location dictionary, if the frame's declared
 * size exceeds the bound, or if the bytes aren't valid UTF-8.
 */
type LocationDecoder = (compressed: Uint8Array) => string;

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

  // Build the location decoder once per manifest from its zstd dictionary.
  const decodeLocation = makeLocationDecoder(fbsManifest);

  // Parse arrays
  const arraysLength = fbsManifest.arraysLength();
  const arrays: ArrayManifest[] = [];
  for (let i = 0; i < arraysLength; i++) {
    const fbsArray = fbsManifest.arrays(i);
    if (fbsArray) {
      arrays.push(parseArrayManifest(fbsArray, decodeLocation));
    }
  }

  return { id, arrays };
}

/**
 * Build the function that turns a ChunkRef's `compressed_location` into a
 * location string, using the manifest's zstd dictionary.
 *
 * Mirrors the Rust `Manifest::decompressor`: a dictionary is only used when
 * `compression_algorithm == ZSTD_DICT` and `location_dictionary` is present.
 * When a `compressed_location` is encountered without a dictionary, decoding
 * throws (matching the Rust `MissingLocationCompressionDictionary` error).
 *
 * Note: `manifest.fbs` prose says `compression_algorithm == 0` means
 * `compressed_location` holds raw (uncompressed) bytes, but that path is not
 * implemented in the Rust reference — its writer only emits `compressed_location`
 * for ZSTD_DICT (otherwise it writes the plain `location` field) and its reader
 * errors here. We follow the reference behavior, not the (inconsistent) schema
 * comment. See earth-mover/icechunk manifest.fbs vs manifest.rs.
 */
function makeLocationDecoder(fbsManifest: FbsManifest): LocationDecoder {
  const dictionary =
    fbsManifest.compressionAlgorithm() === COMPRESSION_ALG_ZSTD_DICT
      ? fbsManifest.locationDictionaryArray()
      : null;
  // `fatal` makes invalid UTF-8 throw, matching the Rust `String::from_utf8`.
  const textDecoder = new TextDecoder("utf-8", { fatal: true });
  return (compressed: Uint8Array): string => {
    if (!dictionary) {
      throw new Error(
        "ChunkRef has a compressed_location but the manifest has no location dictionary",
      );
    }
    // Reject before decompressing: a malformed frame can advertise a huge size
    // and make the decoder allocate far beyond the bound (or OOM) before the
    // post-decompress length check below would run.
    if (zstdFrameAllocSize(compressed) > MAX_DECOMPRESSED_LOCATION_SIZE) {
      throw new Error(
        `Compressed location declares a size over ${MAX_DECOMPRESSED_LOCATION_SIZE} bytes`,
      );
    }
    const decompressed = decompress(compressed, undefined, dictionary);
    if (decompressed.length > MAX_DECOMPRESSED_LOCATION_SIZE) {
      throw new Error(
        `Decompressed location exceeds ${MAX_DECOMPRESSED_LOCATION_SIZE} bytes`,
      );
    }
    return textDecoder.decode(decompressed);
  };
}

function parseArrayManifest(
  fbsArray: FbsArrayManifest,
  decodeLocation: LocationDecoder,
): ArrayManifest {
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
      refs.push(parseChunkRef(fbsRef, decodeLocation));
    }
  }

  return { nodeId, refs };
}

function parseChunkRef(
  fbsRef: FbsChunkRef,
  decodeLocation: LocationDecoder,
): ChunkRef {
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

  // Parse checksum fields
  const checksumEtag = fbsRef.checksumEtag();
  const checksumLastModified = fbsRef.checksumLastModified();

  const ref: ChunkRef = {
    index,
    inline,
    offset,
    length,
    chunkId,
    location: fbsRef.location(),
    checksumEtag,
    checksumLastModified,
  };

  // A dictionary-compressed location takes precedence over the plain `location`
  // string (Rust `ref_to_payload` priority: chunk_id -> compressed_location ->
  // location). `chunk_id` refs are native, not virtual, so they never carry a
  // location. Decode lazily on first read of `location` — mirroring the Rust
  // reader, which decompresses in `ref_to_payload` — so parsing a manifest
  // never decompresses locations the caller doesn't access (the binary search
  // in findChunkRef touches only `index`), and a single malformed frame fails
  // just its own ref instead of aborting the whole manifest parse.
  if (chunkId === null) {
    const compressed = fbsRef.compressedLocationArray();
    if (compressed) {
      let decoded: string | null = null;
      let resolved = false;
      Object.defineProperty(ref, "location", {
        configurable: true,
        enumerable: true,
        get() {
          if (!resolved) {
            decoded = decodeLocation(compressed);
            resolved = true;
          }
          return decoded;
        },
      });
    }
  }

  return ref;
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

/**
 * Extract the payload type from a ChunkRef.
 *
 * Priority matches the Rust `ref_to_payload`: chunk_id (native) ->
 * location (virtual) -> inline. Reading `ref.location` decodes a
 * dictionary-compressed location lazily, and may throw on a malformed frame.
 */
export function getChunkPayload(ref: ChunkRef): ChunkPayload {
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

  if (ref.inline !== null) {
    return { type: "inline", data: ref.inline };
  }

  throw new Error("Invalid ChunkRef: no chunkId, location, or inline");
}
