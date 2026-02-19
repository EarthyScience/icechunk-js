/**
 * Metadata deserialization for icechunk snapshot metadata.
 *
 * v1 snapshots use MessagePack encoding, v2 uses FlexBuffers.
 */

import { decode } from "@msgpack/msgpack";
import * as flexbuffers from "flatbuffers/js/flexbuffers.js";
import { SpecVersion } from "../header.js";
import type { MetadataItem } from "./types.js";

/**
 * Deserialize snapshot metadata items into a key-value record.
 *
 * @param items - Raw metadata items from the snapshot
 * @param specVersion - Spec version determines encoding format
 * @returns Deserialized metadata as a plain object
 */
export function deserializeMetadata(
  items: MetadataItem[],
  specVersion: SpecVersion,
): Record<string, unknown> {
  const result: Record<string, unknown> = Object.create(null);
  for (const item of items) {
    result[item.name] =
      specVersion === SpecVersion.V1_0
        ? decode(item.value)
        : flexbuffers.toObject(
            item.value.buffer.slice(
              item.value.byteOffset,
              item.value.byteOffset + item.value.byteLength,
            ) as ArrayBuffer,
          );
  }
  return result;
}
