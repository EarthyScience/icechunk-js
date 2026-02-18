/**
 * Parser for icechunk Snapshot FlatBuffer format.
 *
 * Field indices based on snapshot.fbs schema order.
 */

import { parseRootTable, TableReader } from "./reader.js";
import {
  asObjectId12,
  asObjectId8,
  type Snapshot,
  type NodeSnapshot,
  type NodeData,
  type ArrayNodeData,
  type ManifestRef,
  type ManifestFileInfo,
  type DimensionShape,
  type ChunkIndexRange,
  type MetadataItem,
} from "./types.js";

// Snapshot field indices (from schema order)
const SNAPSHOT_ID = 0;
const SNAPSHOT_PARENT_ID = 1;
const SNAPSHOT_NODES = 2;
const SNAPSHOT_FLUSHED_AT = 3;
const SNAPSHOT_MESSAGE = 4;
const SNAPSHOT_METADATA = 5;
const SNAPSHOT_MANIFEST_FILES = 6;

// NodeSnapshot field indices
const NODE_ID = 0;
const NODE_PATH = 1;
const NODE_USER_DATA = 2;
const NODE_DATA = 3; // union type + value

// ArrayNodeData field indices
const ARRAY_SHAPE = 0;
const ARRAY_DIMENSION_NAMES = 1;
const ARRAY_MANIFESTS = 2;

// ManifestRef field indices
const MANIFEST_REF_OBJECT_ID = 0;
const MANIFEST_REF_EXTENTS = 1;

// MetadataItem field indices
const METADATA_NAME = 0;
const METADATA_VALUE = 1;

// DimensionName field indices
const DIMENSION_NAME_NAME = 0;

// Struct sizes
const OBJECT_ID_12_SIZE = 12;
const OBJECT_ID_8_SIZE = 8;
const MANIFEST_FILE_INFO_SIZE = 24; // 12 + 8 + 4
const DIMENSION_SHAPE_SIZE = 16; // 8 + 8
const CHUNK_INDEX_RANGE_SIZE = 8; // 4 + 4

/** Parse a Snapshot from FlatBuffer data */
export function parseSnapshot(data: Uint8Array): Snapshot {
  const root = parseRootTable(data);

  // Parse ID (required)
  const idBytes = root.readInlineStruct(SNAPSHOT_ID, OBJECT_ID_12_SIZE);
  if (!idBytes) throw new Error("Snapshot missing required id field");
  const id = asObjectId12(idBytes);

  // Parse parent ID (optional)
  const parentIdBytes = root.readInlineStruct(
    SNAPSHOT_PARENT_ID,
    OBJECT_ID_12_SIZE,
  );
  const parentId = parentIdBytes ? asObjectId12(parentIdBytes) : null;

  // Parse nodes
  const nodesLength = root.getVectorLength(SNAPSHOT_NODES);
  const nodes: NodeSnapshot[] = [];
  for (let i = 0; i < nodesLength; i++) {
    const nodeTable = root.getVectorTable(SNAPSHOT_NODES, i);
    if (nodeTable) {
      nodes.push(parseNodeSnapshot(nodeTable));
    }
  }

  // Parse flushed_at
  const flushedAt = root.readUint64(SNAPSHOT_FLUSHED_AT, 0n);

  // Parse message (required)
  const message = root.readString(SNAPSHOT_MESSAGE) ?? "";

  // Parse metadata
  const metadataLength = root.getVectorLength(SNAPSHOT_METADATA);
  const metadata: MetadataItem[] = [];
  for (let i = 0; i < metadataLength; i++) {
    const metaTable = root.getVectorTable(SNAPSHOT_METADATA, i);
    if (metaTable) {
      const name = metaTable.readString(METADATA_NAME);
      const value = metaTable.readByteVector(METADATA_VALUE);
      if (name && value) {
        metadata.push({ name, value });
      }
    }
  }

  // Parse manifest files (vector of structs)
  const manifestFilesLength = root.getVectorLength(SNAPSHOT_MANIFEST_FILES);
  const manifestFiles: ManifestFileInfo[] = [];
  for (let i = 0; i < manifestFilesLength; i++) {
    const structBytes = root.readVectorStruct(
      SNAPSHOT_MANIFEST_FILES,
      i,
      MANIFEST_FILE_INFO_SIZE,
    );
    if (structBytes) {
      manifestFiles.push(parseManifestFileInfo(structBytes));
    }
  }

  return {
    id,
    parentId,
    nodes,
    flushedAt,
    message,
    metadata,
    manifestFiles,
  };
}

function parseNodeSnapshot(table: TableReader): NodeSnapshot {
  // Parse ID (required)
  const idBytes = table.readInlineStruct(NODE_ID, OBJECT_ID_8_SIZE);
  if (!idBytes) throw new Error("NodeSnapshot missing required id field");
  const id = asObjectId8(idBytes);

  // Parse path (required)
  const path = table.readString(NODE_PATH) ?? "";

  // Parse user_data (required)
  const userData = table.readByteVector(NODE_USER_DATA) ?? new Uint8Array(0);

  // Parse node_data (union)
  // Union fields in FlatBuffers: type byte followed by offset to table
  const nodeDataType = table.readUint8(NODE_DATA, 0);
  const nodeData = parseNodeData(table, nodeDataType);

  return { id, path, userData, nodeData };
}

function parseNodeData(parentTable: TableReader, unionType: number): NodeData {
  // Union type 0 = NONE, 1 = Array, 2 = Group
  if (unionType === 0 || unionType === 2) {
    return { type: "group" };
  }

  if (unionType === 1) {
    // Array - get the nested table
    // Union value is at field index NODE_DATA + 1
    const arrayTable = parentTable.getNestedTable(NODE_DATA + 1);
    if (!arrayTable) {
      throw new Error("ArrayNodeData union type but no table");
    }
    return parseArrayNodeData(arrayTable);
  }

  throw new Error(`Unknown node data union type: ${unionType}`);
}

function parseArrayNodeData(table: TableReader): ArrayNodeData {
  // Parse shape (vector of DimensionShape structs)
  const shapeLength = table.getVectorLength(ARRAY_SHAPE);
  const shape: DimensionShape[] = [];
  for (let i = 0; i < shapeLength; i++) {
    const structBytes = table.readVectorStruct(
      ARRAY_SHAPE,
      i,
      DIMENSION_SHAPE_SIZE,
    );
    if (structBytes) {
      shape.push(parseDimensionShape(structBytes));
    }
  }

  // Parse dimension names (vector of tables)
  const dimNamesLength = table.getVectorLength(ARRAY_DIMENSION_NAMES);
  const dimensionNames: (string | null)[] = [];
  for (let i = 0; i < dimNamesLength; i++) {
    const dimNameTable = table.getVectorTable(ARRAY_DIMENSION_NAMES, i);
    if (dimNameTable) {
      dimensionNames.push(dimNameTable.readString(DIMENSION_NAME_NAME));
    } else {
      dimensionNames.push(null);
    }
  }

  // Parse manifests (vector of tables)
  const manifestsLength = table.getVectorLength(ARRAY_MANIFESTS);
  const manifests: ManifestRef[] = [];
  for (let i = 0; i < manifestsLength; i++) {
    const manifestTable = table.getVectorTable(ARRAY_MANIFESTS, i);
    if (manifestTable) {
      manifests.push(parseManifestRef(manifestTable));
    }
  }

  return {
    type: "array",
    shape,
    dimensionNames,
    manifests,
  };
}

function parseManifestRef(table: TableReader): ManifestRef {
  // Object ID (inline struct)
  const objectIdBytes = table.readInlineStruct(
    MANIFEST_REF_OBJECT_ID,
    OBJECT_ID_12_SIZE,
  );
  if (!objectIdBytes) throw new Error("ManifestRef missing object_id");
  const objectId = asObjectId12(objectIdBytes);

  // Extents (vector of ChunkIndexRange structs)
  const extentsLength = table.getVectorLength(MANIFEST_REF_EXTENTS);
  const extents: ChunkIndexRange[] = [];
  for (let i = 0; i < extentsLength; i++) {
    const structBytes = table.readVectorStruct(
      MANIFEST_REF_EXTENTS,
      i,
      CHUNK_INDEX_RANGE_SIZE,
    );
    if (structBytes) {
      extents.push(parseChunkIndexRange(structBytes));
    }
  }

  return { objectId, extents };
}

function parseManifestFileInfo(bytes: Uint8Array): ManifestFileInfo {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);

  // ObjectId12 (12 bytes)
  const id = asObjectId12(bytes.slice(0, 12));

  // size_bytes (uint64 at offset 12)
  const sizeBytes = Number(view.getBigUint64(12, true));

  // num_chunk_refs (uint32 at offset 20)
  const numChunkRefs = view.getUint32(20, true);

  return { id, sizeBytes, numChunkRefs };
}

function parseDimensionShape(bytes: Uint8Array): DimensionShape {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);

  // array_length (uint64 at offset 0)
  const arrayLength = Number(view.getBigUint64(0, true));

  // chunk_length (uint64 at offset 8)
  const chunkLength = Number(view.getBigUint64(8, true));

  return { arrayLength, chunkLength };
}

function parseChunkIndexRange(bytes: Uint8Array): ChunkIndexRange {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);

  // from (uint32 at offset 0)
  const from = view.getUint32(0, true);

  // to (uint32 at offset 4)
  const to = view.getUint32(4, true);

  return { from, to };
}
