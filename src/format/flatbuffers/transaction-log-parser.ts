/**
 * Parser for icechunk TransactionLog FlatBuffer format.
 *
 * Uses flatc-generated TypeScript classes for type-safe field access.
 */

import { ByteBuffer } from "flatbuffers";
import { TransactionLog as FbsTransactionLog } from "./generated/transaction-log.js";
import {
  asObjectId12,
  asObjectId8,
  type ObjectId8,
  type TransactionLogEntry,
  type ArrayUpdatedChunksInfo,
  type UpdatedChunkIndices,
  type MoveOperationInfo,
} from "./types.js";

/** Extract bytes from a generated ObjectId12 struct */
function readId12(bb: ByteBuffer, bbPos: number): Uint8Array {
  return bb.bytes().slice(bbPos, bbPos + 12);
}

/** Extract bytes from a generated ObjectId8 struct */
function readId8(bb: ByteBuffer, bbPos: number): Uint8Array {
  return bb.bytes().slice(bbPos, bbPos + 8);
}

/** Read a vector of ObjectId8 values from a FlatBuffer accessor */
function readId8Vector(
  length: number,
  accessor: (index: number) => { bb: ByteBuffer | null; bb_pos: number } | null,
): ObjectId8[] {
  const result: ObjectId8[] = [];
  for (let i = 0; i < length; i++) {
    const obj = accessor(i);
    if (obj && obj.bb) {
      result.push(asObjectId8(readId8(obj.bb, obj.bb_pos)));
    }
  }
  return result;
}

/** Parse a TransactionLog from FlatBuffer data */
export function parseTransactionLog(data: Uint8Array): TransactionLogEntry {
  const bb = new ByteBuffer(data);
  const fbs = FbsTransactionLog.getRootAsTransactionLog(bb);

  // Parse ID (required)
  const idObj = fbs.id();
  if (!idObj) throw new Error("TransactionLog missing required id field");
  const id = asObjectId12(readId12(idObj.bb!, idObj.bb_pos));

  // Parse ObjectId8 vectors
  const newGroups = readId8Vector(fbs.newGroupsLength(), (i) =>
    fbs.newGroups(i),
  );
  const newArrays = readId8Vector(fbs.newArraysLength(), (i) =>
    fbs.newArrays(i),
  );
  const deletedGroups = readId8Vector(fbs.deletedGroupsLength(), (i) =>
    fbs.deletedGroups(i),
  );
  const deletedArrays = readId8Vector(fbs.deletedArraysLength(), (i) =>
    fbs.deletedArrays(i),
  );
  const updatedArrays = readId8Vector(fbs.updatedArraysLength(), (i) =>
    fbs.updatedArrays(i),
  );
  const updatedGroups = readId8Vector(fbs.updatedGroupsLength(), (i) =>
    fbs.updatedGroups(i),
  );

  // Parse updated chunks
  const updatedChunks: ArrayUpdatedChunksInfo[] = [];
  for (let i = 0; i < fbs.updatedChunksLength(); i++) {
    const fbsChunk = fbs.updatedChunks(i);
    if (!fbsChunk) continue;

    const nodeIdObj = fbsChunk.nodeId();
    if (!nodeIdObj || !nodeIdObj.bb) continue;
    const nodeId = asObjectId8(readId8(nodeIdObj.bb, nodeIdObj.bb_pos));

    const chunks: UpdatedChunkIndices[] = [];
    for (let j = 0; j < fbsChunk.chunksLength(); j++) {
      const fbsIndices = fbsChunk.chunks(j);
      if (!fbsIndices) continue;

      const coords: number[] = [];
      for (let k = 0; k < fbsIndices.coordsLength(); k++) {
        const coord = fbsIndices.coords(k);
        if (coord !== null) coords.push(coord);
      }
      chunks.push({ coords });
    }

    updatedChunks.push({ nodeId, chunks });
  }

  // Parse moved nodes
  const movedNodes: MoveOperationInfo[] = [];
  for (let i = 0; i < fbs.movedNodesLength(); i++) {
    const fbsMove = fbs.movedNodes(i);
    if (!fbsMove) continue;

    const from = fbsMove.from();
    const to = fbsMove.to();
    if (from !== null && to !== null) {
      movedNodes.push({ from, to });
    }
  }

  return {
    id,
    newGroups,
    newArrays,
    deletedGroups,
    deletedArrays,
    updatedArrays,
    updatedGroups,
    updatedChunks,
    movedNodes,
  };
}
