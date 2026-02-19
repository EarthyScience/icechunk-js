/**
 * icechunk-js - Read-only JavaScript/TypeScript reader for Icechunk repositories.
 *
 * @example
 * ```typescript
 * import { IcechunkStore } from 'icechunk-js';
 * import { open, get } from 'zarrita';
 *
 * const store = await IcechunkStore.open('https://bucket.s3.amazonaws.com/repo', {
 *   branch: 'main'
 * });
 *
 * const array = await open(store, { kind: 'array', path: '/temperature' });
 * const data = await get(array, [0, 0, null]);
 * ```
 */

// Main store class
export { IcechunkStore } from "./store.js";
export type {
  IcechunkStoreOptions,
  AsyncReadable,
  AbsolutePath,
} from "./store.js";

// Repository and session
export { Repository } from "./reader/repository.js";
export type { RepositoryOptions, RefData } from "./reader/repository.js";
export { ReadSession } from "./reader/session.js";

// Storage backends
export { HttpStorage } from "./storage/http-storage.js";
export type { HttpStorageOptions } from "./storage/http-storage.js";
export type {
  Storage,
  ByteRange,
  RequestOptions,
  TransformRequest,
  TransformRequestOptions,
  TransformRequestResult,
} from "./storage/storage.js";
export { NotFoundError, StorageError } from "./storage/storage.js";

// Format types (for advanced usage)
export type {
  Snapshot,
  NodeSnapshot,
  NodeData,
  ArrayNodeData,
  GroupNodeData,
  Manifest,
  ArrayManifest,
  ChunkRef,
  ChunkPayload,
  InlineChunkPayload,
  NativeChunkPayload,
  VirtualChunkPayload,
  ManifestRef,
  ManifestFileInfo,
  DimensionShape,
  ChunkIndexRange,
  MetadataItem,
  ObjectId12,
  ObjectId8,
} from "./format/flatbuffers/types.js";

// Format utilities
export {
  SpecVersion,
  FileType,
  CompressionAlgorithm,
  HeaderParseError,
} from "./format/header.js";
export {
  encodeBase32,
  decodeBase32,
  encodeObjectId12,
  decodeObjectId12,
  encodeObjectId8,
  decodeObjectId8,
} from "./format/object-id.js";
