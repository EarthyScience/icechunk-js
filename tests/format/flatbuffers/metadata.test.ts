/**
 * Tests for metadata deserialization (MessagePack v1 and FlexBuffers v2).
 */

import { describe, it, expect } from "vitest";
import { encode } from "@msgpack/msgpack";
import * as flexbuffers from "flatbuffers/js/flexbuffers.js";
import { deserializeMetadata } from "../../../src/format/flatbuffers/metadata.js";
import { SpecVersion } from "../../../src/format/header.js";
import type { MetadataItem } from "../../../src/format/flatbuffers/types.js";

function encodeFlexBuffer(value: unknown): Uint8Array {
  const builder = flexbuffers.builder();
  builder.add(value);
  return builder.finish();
}

describe("deserializeMetadata", () => {
  describe("prototype pollution safety", () => {
    it("should keep a null prototype when decoding reserved keys in v1", () => {
      const items: MetadataItem[] = [
        { name: "__proto__", value: encode({ polluted: true }) as Uint8Array },
        { name: "constructor", value: encode("ctor") as Uint8Array },
        { name: "prototype", value: encode(123) as Uint8Array },
      ];
      const result = deserializeMetadata(items, SpecVersion.V1_0);

      expect(Object.getPrototypeOf(result)).toBeNull();
      expect(result["__proto__"]).toEqual({ polluted: true });
      expect(result["constructor"]).toBe("ctor");
      expect(result["prototype"]).toBe(123);
      expect(result["polluted"]).toBeUndefined();
    });

    it("should keep a null prototype when decoding reserved keys in v2", () => {
      const items: MetadataItem[] = [
        { name: "__proto__", value: encodeFlexBuffer({ polluted: true }) },
        { name: "constructor", value: encodeFlexBuffer("ctor") },
        { name: "prototype", value: encodeFlexBuffer(123) },
      ];
      const result = deserializeMetadata(items, SpecVersion.V2_0);

      expect(Object.getPrototypeOf(result)).toBeNull();
      expect(result["__proto__"]).toEqual({ polluted: true });
      expect(result["constructor"]).toBe("ctor");
      expect(result["prototype"]).toBe(123);
      expect(result["polluted"]).toBeUndefined();
    });
  });

  describe("v1 (MessagePack)", () => {
    it("should decode string values", () => {
      const items: MetadataItem[] = [
        { name: "key", value: encode("hello") as Uint8Array },
      ];
      const result = deserializeMetadata(items, SpecVersion.V1_0);
      expect(result).toEqual({ key: "hello" });
    });

    it("should decode numeric values", () => {
      const items: MetadataItem[] = [
        { name: "count", value: encode(42) as Uint8Array },
      ];
      const result = deserializeMetadata(items, SpecVersion.V1_0);
      expect(result).toEqual({ count: 42 });
    });

    it("should decode nested objects", () => {
      const obj = { nested: { a: 1, b: [2, 3] } };
      const items: MetadataItem[] = [
        { name: "data", value: encode(obj) as Uint8Array },
      ];
      const result = deserializeMetadata(items, SpecVersion.V1_0);
      expect(result).toEqual({ data: obj });
    });

    it("should decode multiple items", () => {
      const items: MetadataItem[] = [
        { name: "a", value: encode("alpha") as Uint8Array },
        { name: "b", value: encode(99) as Uint8Array },
      ];
      const result = deserializeMetadata(items, SpecVersion.V1_0);
      expect(result).toEqual({ a: "alpha", b: 99 });
    });

    it("should handle empty items", () => {
      const result = deserializeMetadata([], SpecVersion.V1_0);
      expect(result).toEqual({});
    });
  });

  describe("v2 (FlexBuffers)", () => {
    it("should decode string values", () => {
      const items: MetadataItem[] = [
        { name: "key", value: encodeFlexBuffer("hello") },
      ];
      const result = deserializeMetadata(items, SpecVersion.V2_0);
      expect(result).toEqual({ key: "hello" });
    });

    it("should decode numeric values", () => {
      const items: MetadataItem[] = [
        { name: "count", value: encodeFlexBuffer(42) },
      ];
      const result = deserializeMetadata(items, SpecVersion.V2_0);
      expect(result).toEqual({ count: 42 });
    });

    it("should decode multiple items", () => {
      const items: MetadataItem[] = [
        { name: "a", value: encodeFlexBuffer("alpha") },
        { name: "b", value: encodeFlexBuffer(99) },
      ];
      const result = deserializeMetadata(items, SpecVersion.V2_0);
      expect(result).toEqual({ a: "alpha", b: 99 });
    });

    it("should handle empty items", () => {
      const result = deserializeMetadata([], SpecVersion.V2_0);
      expect(result).toEqual({});
    });

    it("should decode correctly from a sliced Uint8Array", () => {
      const encoded = encodeFlexBuffer("sliced");
      // Embed in a larger buffer with padding on both sides
      const padded = new Uint8Array(10 + encoded.length + 10);
      padded.set(encoded, 10);
      const slice = padded.subarray(10, 10 + encoded.length);

      const items: MetadataItem[] = [{ name: "key", value: slice }];
      const result = deserializeMetadata(items, SpecVersion.V2_0);
      expect(result).toEqual({ key: "sliced" });
    });
  });
});
