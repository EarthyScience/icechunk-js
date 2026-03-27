/**
 * Parser for icechunk Snapshot FlatBuffer format.
 *
 * Uses flatc-generated TypeScript classes for type-safe field access.
 */

import { ByteBuffer } from "flatbuffers";
import { Snapshot as FbsSnapshot } from "./generated/snapshot.js";
import { NodeSnapshot as FbsNodeSnapshot } from "./generated/node-snapshot.js";
import { ArrayNodeData as FbsArrayNodeData } from "./generated/array-node-data.js";
import { ManifestRef as FbsManifestRef } from "./generated/manifest-ref.js";
import { NodeData as FbsNodeDataEnum } from "./generated/node-data.js";
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

/** Extract bytes from a generated ObjectId12 struct */
function readId12(bb: ByteBuffer, bbPos: number): Uint8Array {
  return bb.bytes().slice(bbPos, bbPos + 12);
}

/** Extract bytes from a generated ObjectId8 struct */
function readId8(bb: ByteBuffer, bbPos: number): Uint8Array {
  return bb.bytes().slice(bbPos, bbPos + 8);
}

/** Parse a Snapshot from FlatBuffer data */
export function parseSnapshot(data: Uint8Array): Snapshot {
  const bb = new ByteBuffer(data);
  const fbsSnapshot = FbsSnapshot.getRootAsSnapshot(bb);

  // Parse ID (required)
  const idObj = fbsSnapshot.id();
  if (!idObj) throw new Error("Snapshot missing required id field");
  const id = asObjectId12(readId12(idObj.bb!, idObj.bb_pos));

  // Parse parent ID (optional)
  const parentIdObj = fbsSnapshot.parentId();
  const parentId = parentIdObj
    ? asObjectId12(readId12(parentIdObj.bb!, parentIdObj.bb_pos))
    : null;

  // Parse nodes
  const nodesLength = fbsSnapshot.nodesLength();
  const nodes: NodeSnapshot[] = [];
  for (let i = 0; i < nodesLength; i++) {
    const fbsNode = fbsSnapshot.nodes(i);
    if (fbsNode) {
      nodes.push(parseNodeSnapshot(fbsNode));
    }
  }

  // Parse flushed_at
  const flushedAt = fbsSnapshot.flushedAt();

  // Parse message (required)
  const message = fbsSnapshot.message() ?? "";

  // Parse metadata
  const metadataLength = fbsSnapshot.metadataLength();
  const metadata: MetadataItem[] = [];
  for (let i = 0; i < metadataLength; i++) {
    const fbsMeta = fbsSnapshot.metadata(i);
    if (fbsMeta) {
      const name = fbsMeta.name();
      const value = fbsMeta.valueArray();
      if (name && value) {
        metadata.push({ name, value: new Uint8Array(value) });
      }
    }
  }

  // Parse manifest files — prefer v2 field, fall back to v1 struct vector
  const manifestFiles: ManifestFileInfo[] = [];
  const v2Length = fbsSnapshot.manifestFilesV2Length();
  if (v2Length > 0) {
    for (let i = 0; i < v2Length; i++) {
      const fbsMfi = fbsSnapshot.manifestFilesV2(i);
      if (fbsMfi) {
        const mfiIdObj = fbsMfi.id();
        if (!mfiIdObj) throw new Error("ManifestFileInfoV2 missing id");
        manifestFiles.push({
          id: asObjectId12(readId12(mfiIdObj.bb!, mfiIdObj.bb_pos)),
          sizeBytes: Number(fbsMfi.sizeBytes()),
          numChunkRefs: fbsMfi.numChunkRefs(),
        });
      }
    }
  } else {
    const v1Length = fbsSnapshot.manifestFilesLength();
    for (let i = 0; i < v1Length; i++) {
      const fbsMfi = fbsSnapshot.manifestFiles(i);
      if (fbsMfi) {
        const mfiIdObj = fbsMfi.id();
        if (!mfiIdObj) throw new Error("ManifestFileInfo missing id");
        manifestFiles.push({
          id: asObjectId12(readId12(mfiIdObj.bb!, mfiIdObj.bb_pos)),
          sizeBytes: Number(fbsMfi.sizeBytes()),
          numChunkRefs: fbsMfi.numChunkRefs(),
        });
      }
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

function parseNodeSnapshot(fbsNode: FbsNodeSnapshot): NodeSnapshot {
  // Parse ID (required)
  const idObj = fbsNode.id();
  if (!idObj) throw new Error("NodeSnapshot missing required id field");
  const id = asObjectId8(readId8(idObj.bb!, idObj.bb_pos));

  // Parse path (required)
  const path = fbsNode.path() ?? "";

  // Parse user_data (required)
  const userData = fbsNode.userDataArray() ?? new Uint8Array(0);

  // Parse node_data (union)
  const nodeDataType = fbsNode.nodeDataType();
  const nodeData = parseNodeData(fbsNode, nodeDataType);

  return { id, path, userData, nodeData };
}

function parseNodeData(
  fbsNode: FbsNodeSnapshot,
  unionType: FbsNodeDataEnum,
): NodeData {
  if (
    unionType === FbsNodeDataEnum.NONE ||
    unionType === FbsNodeDataEnum.Group
  ) {
    return { type: "group" };
  }

  if (unionType === FbsNodeDataEnum.Array) {
    const arrayData = fbsNode.nodeData(new FbsArrayNodeData());
    if (!arrayData) {
      throw new Error("ArrayNodeData union type but no table");
    }
    return parseArrayNodeData(arrayData as FbsArrayNodeData);
  }

  throw new Error(`Unknown node data union type: ${unionType}`);
}

function parseArrayNodeData(fbsArray: FbsArrayNodeData): ArrayNodeData {
  // Parse shape (vector of DimensionShape structs)
  const shapeLength = fbsArray.shapeLength();
  const shape: DimensionShape[] = [];
  for (let i = 0; i < shapeLength; i++) {
    const fbsShape = fbsArray.shape(i);
    if (fbsShape) {
      shape.push({
        arrayLength: Number(fbsShape.arrayLength()),
        chunkLength: Number(fbsShape.chunkLength()),
      });
    }
  }

  // Parse dimension names (vector of tables)
  const dimNamesLength = fbsArray.dimensionNamesLength();
  const dimensionNames: (string | null)[] = [];
  for (let i = 0; i < dimNamesLength; i++) {
    const fbsDimName = fbsArray.dimensionNames(i);
    if (fbsDimName) {
      dimensionNames.push(fbsDimName.name());
    } else {
      dimensionNames.push(null);
    }
  }

  // Parse manifests (vector of tables)
  const manifestsLength = fbsArray.manifestsLength();
  const manifests: ManifestRef[] = [];
  for (let i = 0; i < manifestsLength; i++) {
    const fbsManifest = fbsArray.manifests(i);
    if (fbsManifest) {
      manifests.push(parseManifestRef(fbsManifest));
    }
  }

  return {
    type: "array",
    shape,
    dimensionNames,
    manifests,
  };
}

function parseManifestRef(fbsRef: FbsManifestRef): ManifestRef {
  // Object ID (inline struct)
  const objectIdObj = fbsRef.objectId();
  if (!objectIdObj) throw new Error("ManifestRef missing object_id");
  const objectId = asObjectId12(readId12(objectIdObj.bb!, objectIdObj.bb_pos));

  // Extents (vector of ChunkIndexRange structs)
  const extentsLength = fbsRef.extentsLength();
  const extents: ChunkIndexRange[] = [];
  for (let i = 0; i < extentsLength; i++) {
    const fbsExtent = fbsRef.extents(i);
    if (fbsExtent) {
      extents.push({ from: fbsExtent.from(), to: fbsExtent.to() });
    }
  }

  return { objectId, extents };
}
