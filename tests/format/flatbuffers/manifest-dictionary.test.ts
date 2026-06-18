/**
 * Tests for dictionary-compressed virtual chunk locations.
 *
 * icechunk (earth-mover/icechunk#1776) can zstd-compress the often-repetitive
 * S3 URLs in virtual manifests against a per-manifest trained dictionary:
 * `Manifest.location_dictionary` holds the dictionary and each
 * `ChunkRef.compressed_location` holds a dictionary-compressed location.
 *
 * The fixtures under tests/fixtures/dictionary/ were produced offline with the
 * reference `zstd` CLI (v1.5.7):
 *   zstd --train corpus/* -o location-dict.bin --maxdict=2048
 *   zstd -D location-dict.bin location.txt -o compressed-location.zst
 * so this test exercises the real zstd dictionary format end-to-end through the
 * vendored fzstd decoder (../../../src/vendor/fzstd), independent of any single
 * library's round-trip.
 */

import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import * as flatbuffers from "flatbuffers";
import {
  parseManifest,
  findChunkRef,
  getChunkPayload,
} from "../../../src/format/flatbuffers/manifest-parser.js";
import { decompress } from "../../../src/vendor/fzstd/index.js";
import type { ObjectId8 } from "../../../src/format/flatbuffers/types.js";

// --- Offline-generated zstd dictionary fixtures (see file header) ---
function fixture(name: string): Uint8Array {
  return new Uint8Array(
    readFileSync(
      fileURLToPath(
        new URL(`../../fixtures/dictionary/${name}`, import.meta.url),
      ),
    ),
  );
}

const DICTIONARY = fixture("location-dict.bin");
const COMPRESSED = fixture("compressed-location.zst");
const LOCATION = new TextDecoder().decode(fixture("location.txt"));

// Crafted so the (raw) dictionary-content length equals the decompressed size,
// which triggered a buffer-aliasing corruption in upstream fzstd PR#18. The
// location content is identical to the dictionary, so decode output must equal
// the dictionary bytes.
const ALIAS_DICT = fixture("alias-dict.bin");
const ALIAS_COMPRESSED = fixture("alias-compressed.zst");

function mockNodeId(seed: number): ObjectId8 {
  const id = new Uint8Array(8);
  for (let i = 0; i < 8; i++) id[i] = (seed + i) % 256;
  return id as ObjectId8;
}

// --- Minimal flatbuffer Manifest builder (generated classes are reader-only) ---

/** Write an inline struct (N raw bytes, alignment 1) and return its offset. */
function buildStruct(builder: flatbuffers.Builder, bytes: Uint8Array): number {
  builder.prep(1, bytes.length);
  for (let i = bytes.length - 1; i >= 0; i--) builder.writeInt8(bytes[i]);
  return builder.offset();
}

function buildIndexVector(
  builder: flatbuffers.Builder,
  data: number[],
): number {
  builder.startVector(4, data.length, 4);
  for (let i = data.length - 1; i >= 0; i--) builder.addInt32(data[i]);
  return builder.endVector();
}

interface RefSpec {
  index: number[];
  offset: number;
  length: number;
  location?: string;
  compressed?: Uint8Array;
}

function buildChunkRef(builder: flatbuffers.Builder, ref: RefSpec): number {
  // Child offsets must be created before the table is started.
  const indexOff = buildIndexVector(builder, ref.index);
  const locOff = ref.location != null ? builder.createString(ref.location) : 0;
  const compOff = ref.compressed ? builder.createByteVector(ref.compressed) : 0;

  // ChunkRef vtable slots: index=0, inline=1, offset=2, length=3, chunk_id=4,
  // location=5, checksum_etag=6, checksum_last_modified=7,
  // compressed_location=8, extra=9.
  builder.startObject(10);
  builder.addFieldOffset(0, indexOff, 0);
  builder.addFieldInt64(2, BigInt(ref.offset), BigInt(0));
  builder.addFieldInt64(3, BigInt(ref.length), BigInt(0));
  if (locOff) builder.addFieldOffset(5, locOff, 0);
  if (compOff) builder.addFieldOffset(8, compOff, 0);
  return builder.endObject();
}

function buildArrayManifest(
  builder: flatbuffers.Builder,
  nodeId: Uint8Array,
  refOffsets: number[],
): number {
  builder.startVector(4, refOffsets.length, 4);
  for (let i = refOffsets.length - 1; i >= 0; i--)
    builder.addOffset(refOffsets[i]);
  const refsOff = builder.endVector();

  // ArrayManifest vtable slots: node_id=0, refs=1, extra=2.
  builder.startObject(3);
  builder.addFieldOffset(1, refsOff, 0);
  const nodeOff = buildStruct(builder, nodeId);
  builder.addFieldStruct(0, nodeOff, 0);
  return builder.endObject();
}

interface ManifestSpec {
  arrays: { nodeId: Uint8Array; refs: RefSpec[] }[];
  dictionary?: Uint8Array;
  compressionAlgorithm?: number;
}

function buildManifest(spec: ManifestSpec): Uint8Array {
  const builder = new flatbuffers.Builder(1024);
  const arrayOffsets = spec.arrays.map((a) =>
    buildArrayManifest(
      builder,
      a.nodeId,
      a.refs.map((r) => buildChunkRef(builder, r)),
    ),
  );

  builder.startVector(4, arrayOffsets.length, 4);
  for (let i = arrayOffsets.length - 1; i >= 0; i--)
    builder.addOffset(arrayOffsets[i]);
  const arraysOff = builder.endVector();

  const dictOff = spec.dictionary
    ? builder.createByteVector(spec.dictionary)
    : 0;

  // Manifest vtable slots: id=0, arrays=1, location_dictionary=2,
  // compression_algorithm=3, extra=4. compression_algorithm default is 1.
  builder.startObject(5);
  builder.addFieldOffset(1, arraysOff, 0);
  if (dictOff) builder.addFieldOffset(2, dictOff, 0);
  if (spec.compressionAlgorithm != null)
    builder.addFieldInt8(3, spec.compressionAlgorithm, 1);
  const idOff = buildStruct(builder, new Uint8Array(12).fill(7));
  builder.addFieldStruct(0, idOff, 0);
  const manifestOff = builder.endObject();

  builder.finish(manifestOff);
  return builder.asUint8Array();
}

/**
 * Locations decode lazily, so a bad `compressed_location` never makes
 * parseManifest throw — the error surfaces only when the ref's location is read
 * (here, via getChunkPayload). Returns a thunk that resolves the single ref at
 * coords [0] so callers can assert on the deferred throw.
 */
function resolveSingleLocation(
  data: Uint8Array,
  nodeId: ObjectId8,
): () => unknown {
  const ref = findChunkRef(parseManifest(data), nodeId, [0]);
  expect(ref).not.toBeNull();
  return () => getChunkPayload(ref!);
}

describe("dictionary-compressed locations", () => {
  it("vendored fzstd decodes a real zstd dictionary frame", () => {
    const decoded = new TextDecoder().decode(
      decompress(COMPRESSED, undefined, DICTIONARY),
    );
    expect(decoded).toBe(LOCATION);
  });

  it("builder round-trips a plain (uncompressed) location", () => {
    // Self-check: proves the hand-rolled flatbuffer builder is correct before
    // we trust it for the compressed case.
    const nodeId = mockNodeId(1);
    const data = buildManifest({
      arrays: [
        {
          nodeId,
          refs: [{ index: [0], offset: 10, length: 20, location: "s3://b/k" }],
        },
      ],
    });
    const manifest = parseManifest(data);
    const ref = findChunkRef(manifest, nodeId, [0]);
    expect(ref).not.toBeNull();
    const payload = getChunkPayload(ref!);
    expect(payload.type).toBe("virtual");
    if (payload.type === "virtual") {
      expect(payload.location).toBe("s3://b/k");
      expect(payload.offset).toBe(10);
      expect(payload.length).toBe(20);
    }
  });

  it("decodes compressed_location via the manifest dictionary", () => {
    const nodeId = mockNodeId(2);
    const data = buildManifest({
      dictionary: DICTIONARY,
      compressionAlgorithm: 1,
      arrays: [
        {
          nodeId,
          refs: [
            { index: [0], offset: 1024, length: 512, compressed: COMPRESSED },
          ],
        },
      ],
    });
    const manifest = parseManifest(data);
    const ref = findChunkRef(manifest, nodeId, [0]);
    expect(ref).not.toBeNull();
    const payload = getChunkPayload(ref!);
    expect(payload.type).toBe("virtual");
    if (payload.type === "virtual") {
      expect(payload.location).toBe(LOCATION);
      expect(payload.offset).toBe(1024);
      expect(payload.length).toBe(512);
    }
  });

  it("throws when a compressed_location has no manifest dictionary", () => {
    const nodeId = mockNodeId(3);
    const data = buildManifest({
      // No dictionary, algorithm left at the default.
      arrays: [
        {
          nodeId,
          refs: [{ index: [0], offset: 0, length: 1, compressed: COMPRESSED }],
        },
      ],
    });
    // Decode is lazy: parsing succeeds; the error surfaces only on read.
    expect(() => parseManifest(data)).not.toThrow();
    expect(resolveSingleLocation(data, nodeId)).toThrow(/location dictionary/);
  });

  it("rejects a location that decompresses beyond the size bound", () => {
    // Matches the Rust MAX_DECOMPRESSED_LOCATION_SIZE (1024 bytes). This real
    // frame honestly advertises its 1100-byte content size, so the pre-decode
    // header guard rejects it before decompression.
    const oversized = fixture("oversized-location.zst");
    const nodeId = mockNodeId(4);
    const data = buildManifest({
      dictionary: DICTIONARY,
      compressionAlgorithm: 1,
      arrays: [
        {
          nodeId,
          refs: [{ index: [0], offset: 0, length: 1, compressed: oversized }],
        },
      ],
    });
    expect(resolveSingleLocation(data, nodeId)).toThrow(
      /declares a size over 1024/,
    );
  });

  it("decodes a frame whose output aliases the dictionary buffer (fzstd patch)", () => {
    // Without the vendored fast-path fix this silently corrupts the output.
    const out = decompress(ALIAS_COMPRESSED, undefined, ALIAS_DICT);
    expect(Array.from(out)).toEqual(Array.from(ALIAS_DICT));
  });

  it("rejects a compressed_location whose header advertises an oversized frame", () => {
    // Hand-crafted zstd frame header: magic + single-segment + 4-byte content
    // size of 2,000,000 and no real blocks. Must be rejected before the decoder
    // allocates for the advertised size.
    const malicious = new Uint8Array([
      0x28,
      0xb5,
      0x2f,
      0xfd, // magic
      0xa0, // single_segment=1, content_size_flag=2 (4-byte FCS)
      0x80,
      0x84,
      0x1e,
      0x00, // content size = 2,000,000 (LE)
    ]);
    const nodeId = mockNodeId(5);
    const data = buildManifest({
      dictionary: DICTIONARY,
      compressionAlgorithm: 1,
      arrays: [
        {
          nodeId,
          refs: [{ index: [0], offset: 0, length: 1, compressed: malicious }],
        },
      ],
    });
    expect(resolveSingleLocation(data, nodeId)).toThrow(
      /declares a size over 1024/,
    );
  });

  it("rejects a frame with a huge window descriptor (32-bit shift overflow)", () => {
    // Not single-segment / no content size, with a window descriptor of
    // wd>>3 = 31 -> exponent 41 -> 2**41 bytes, then a tiny RLE last block so
    // the frame is complete and the window size itself is what's checked. A
    // bitwise `1 << 41` would wrap to 512 and wrongly pass the bound — the
    // arithmetic computation must reject it.
    const malicious = new Uint8Array([
      0x28,
      0xb5,
      0x2f,
      0xfd, // magic
      0x00, // FHD: not single-segment, no content size
      0xf8, // window descriptor: wd>>3 = 31 -> exponent 41 -> 2**41 bytes
      0x0b,
      0x00,
      0x00, // block header: RLE (type 1), last block, block size 1
      0x41, // the single RLE byte
    ]);
    const nodeId = mockNodeId(6);
    const data = buildManifest({
      dictionary: DICTIONARY,
      compressionAlgorithm: 1,
      arrays: [
        {
          nodeId,
          refs: [{ index: [0], offset: 0, length: 1, compressed: malicious }],
        },
      ],
    });
    expect(resolveSingleLocation(data, nodeId)).toThrow(
      /declares a size over 1024/,
    );
  });

  it("rejects an RLE block that expands a single byte past the size bound", () => {
    // An RLE block costs one input byte but expands to its declared size. The
    // header advertises only the smallest (1024-byte) window, so the up-front
    // size looks fine — the guard must also count the block's declared output.
    const blockSize = 2_000_000;
    const header = (blockSize << 3) | (1 << 1) | 1; // RLE (type 1), last block
    const malicious = new Uint8Array([
      0x28,
      0xb5,
      0x2f,
      0xfd, // magic
      0x00, // FHD: not single-segment, no content size
      0x00, // window descriptor: smallest window (1024 bytes)
      header & 0xff,
      (header >> 8) & 0xff,
      (header >> 16) & 0xff,
      0x41, // the single RLE byte
    ]);
    const nodeId = mockNodeId(8);
    const data = buildManifest({
      dictionary: DICTIONARY,
      compressionAlgorithm: 1,
      arrays: [
        {
          nodeId,
          refs: [{ index: [0], offset: 0, length: 1, compressed: malicious }],
        },
      ],
    });
    expect(resolveSingleLocation(data, nodeId)).toThrow(
      /declares a size over 1024/,
    );
  });

  it("rejects a compressed_location with a trailing/concatenated frame", () => {
    // A valid small frame (the real fixture) followed by a second frame that
    // advertises a huge size. fzstd would decode both and allocate for the
    // second, so the guard must reject anything that isn't exactly one frame.
    const secondFrame = new Uint8Array([
      0x28, 0xb5, 0x2f, 0xfd, 0xa0, 0x80, 0x84, 0x1e, 0x00,
    ]);
    const concatenated = new Uint8Array([...COMPRESSED, ...secondFrame]);
    const nodeId = mockNodeId(7);
    const data = buildManifest({
      dictionary: DICTIONARY,
      compressionAlgorithm: 1,
      arrays: [
        {
          nodeId,
          refs: [
            { index: [0], offset: 0, length: 1, compressed: concatenated },
          ],
        },
      ],
    });
    expect(resolveSingleLocation(data, nodeId)).toThrow(
      /declares a size over 1024/,
    );
  });
});
