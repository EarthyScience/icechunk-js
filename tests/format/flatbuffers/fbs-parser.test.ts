/**
 * Tests for the mini .fbs schema parser.
 */

import { describe, it, expect } from "vitest";
import { resolve, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { parseFbsSource, parseFbsFiles } from "./fbs-parser.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const schemaDir = resolve(__dirname, "../../../../icechunk/flatbuffers");

describe("fbs-parser", () => {
  describe("table parsing", () => {
    it("parses a simple table with scalar fields", () => {
      const schema = parseFbsSource(`
        table Foo {
          x: uint32;
          y: uint64;
        }
      `);
      expect(schema.tables["Foo"]).toBeDefined();
      const fields = schema.tables["Foo"].fields;
      expect(fields).toHaveLength(2);
      expect(fields[0]).toMatchObject({ name: "x", type: "uint32", vtableIndex: 0 });
      expect(fields[1]).toMatchObject({ name: "y", type: "uint64", vtableIndex: 1 });
    });

    it("handles required and deprecated attributes", () => {
      const schema = parseFbsSource(`
        table Bar {
          name: string (required);
          old_field: uint32 (deprecated);
          value: [uint8] (required);
        }
      `);
      const fields = schema.tables["Bar"].fields;
      expect(fields[0]).toMatchObject({ name: "name", required: true, deprecated: false, vtableIndex: 0 });
      expect(fields[1]).toMatchObject({ name: "old_field", required: false, deprecated: true, vtableIndex: 1 });
      expect(fields[2]).toMatchObject({ name: "value", required: true, vtableIndex: 2 });
    });

    it("handles default values", () => {
      const schema = parseFbsSource(`
        table Defaults {
          offset: uint64 = 0;
          length: uint64 = 0;
          last_modified: uint32 = 0;
        }
      `);
      const fields = schema.tables["Defaults"].fields;
      expect(fields[0].defaultValue).toBe("0");
      expect(fields[1].defaultValue).toBe("0");
      expect(fields[2].defaultValue).toBe("0");
    });

    it("parses empty tables", () => {
      const schema = parseFbsSource(`table GroupNodeData {}`);
      expect(schema.tables["GroupNodeData"]).toBeDefined();
      expect(schema.tables["GroupNodeData"].fields).toHaveLength(0);
    });

    it("parses empty tables with whitespace", () => {
      const schema = parseFbsSource(`
        table GroupNodeData {
        }
      `);
      expect(schema.tables["GroupNodeData"]).toBeDefined();
      expect(schema.tables["GroupNodeData"].fields).toHaveLength(0);
    });

    it("handles inline comments between fields", () => {
      const schema = parseFbsSource(`
        table Snapshot {
          // the id of this snapshot
          id: uint32;

          // the parent
          parent_id: uint32;
        }
      `);
      const fields = schema.tables["Snapshot"].fields;
      expect(fields).toHaveLength(2);
      expect(fields[0]).toMatchObject({ name: "id", vtableIndex: 0 });
      expect(fields[1]).toMatchObject({ name: "parent_id", vtableIndex: 1 });
    });

    it("handles inline comments on field lines", () => {
      const schema = parseFbsSource(`
        table Foo {
          x: uint32; // some comment
          y: string; // another comment
        }
      `);
      const fields = schema.tables["Foo"].fields;
      expect(fields).toHaveLength(2);
    });

    it("deprecated fields still consume vtable slots", () => {
      const schema = parseFbsSource(`
        table T {
          a: uint32;
          b: uint32 (deprecated);
          c: uint32;
        }
      `);
      const fields = schema.tables["T"].fields;
      expect(fields[0].vtableIndex).toBe(0);
      expect(fields[1].vtableIndex).toBe(1);
      expect(fields[2].vtableIndex).toBe(2);
    });
  });

  describe("union fields in tables (double-slot)", () => {
    it("union fields consume two vtable slots", () => {
      const schema = parseFbsSource(`
        table ArrayNodeData {}
        table GroupNodeData {}
        union NodeData { Array :ArrayNodeData, Group :GroupNodeData }
        table NodeSnapshot {
          id: uint32;
          path: string;
          user_data: [uint8];
          node_data: NodeData;
        }
      `);
      const fields = schema.tables["NodeSnapshot"].fields;
      // id=0, path=1, user_data=2, node_data union type=3 (implicit), node_data value=4
      // But our vtableIndex for node_data stores the type index + 1 (the value slot)
      // Actually: the union consumes slots 3 (type) and 4 (value)
      // The field's vtableIndex should be 3+1=4? No - let me re-check.
      // In FlatBuffers: union field at position N creates type at slot N and value at slot N+1
      // Our parser stores vtableIndex as the type slot index (vtableIndex) for the type byte
      // Actually looking at the code: for union fields, vtableIndex = vtableIndex + 1 (value slot)
      // and vtableIndex advances by 2
      // So: id=0, path=1, user_data=2, node_data type=3, node_data value=4
      // field.vtableIndex = 4 (we store the value slot)
      expect(fields[0].vtableIndex).toBe(0); // id
      expect(fields[1].vtableIndex).toBe(1); // path
      expect(fields[2].vtableIndex).toBe(2); // user_data
      expect(fields[3].vtableIndex).toBe(4); // node_data (value slot, type is at 3)
    });
  });

  describe("struct parsing", () => {
    it("computes size for simple scalar struct", () => {
      const schema = parseFbsSource(`
        struct Point {
          x: uint32;
          y: uint32;
        }
      `);
      expect(schema.structs["Point"].size).toBe(8);
    });

    it("computes size for fixed-size byte array struct", () => {
      const schema = parseFbsSource(`
        struct ObjectId12 {
          bytes: [uint8:12];
        }
      `);
      expect(schema.structs["ObjectId12"].size).toBe(12);
    });

    it("computes size with alignment padding for mixed-width fields", () => {
      // uint8 (1) + 7 bytes padding + uint64 (8) = 16, padded to max align 8 → 16
      const schema = parseFbsSource(`
        struct Padded {
          flag: uint8;
          value: uint64;
        }
      `);
      expect(schema.structs["Padded"].size).toBe(16);
    });

    it("handles nested struct types with alignment", () => {
      const schema = parseFbsSource(`
        struct ObjectId12 {
          bytes: [uint8:12];
        }
        struct ManifestFileInfo {
          id: ObjectId12;
          size_bytes: uint64;
          num_chunk_refs: uint32;
        }
      `);
      // ObjectId12 (12 bytes, align 1) at 0, +4 padding to align uint64,
      // size_bytes at 16, num_chunk_refs at 24, +4 trailing padding → 32
      expect(schema.structs["ManifestFileInfo"].size).toBe(32);
    });

    it("computes alignment from nested struct with alignment > 1", () => {
      // Inner has uint64 → alignment 8. Outer has uint8 + Inner.
      // uint8 (1 byte) + 7 padding + Inner (16 bytes, align 8) = 24
      const schema = parseFbsSource(`
        struct Inner {
          a: uint64;
          b: uint64;
        }
        struct Outer {
          flag: uint8;
          inner: Inner;
        }
      `);
      expect(schema.structs["Inner"].size).toBe(16);
      // flag at 0 (1 byte), pad to 8 for Inner (align 8), Inner at 8 (16 bytes) = 24
      expect(schema.structs["Outer"].size).toBe(24);
    });
  });

  describe("union parsing", () => {
    it("parses multiline union", () => {
      const schema = parseFbsSource(`
        union NodeData {
          Array :ArrayNodeData,
          Group :GroupNodeData,
        }
      `);
      const u = schema.unions["NodeData"];
      expect(u.variants).toHaveLength(2);
      expect(u.variants[0]).toMatchObject({ name: "Array", type: "ArrayNodeData", ordinal: 1 });
      expect(u.variants[1]).toMatchObject({ name: "Group", type: "GroupNodeData", ordinal: 2 });
    });

    it("parses single-line union", () => {
      const schema = parseFbsSource(
        `union NodeData { Array :ArrayNodeData, Group :GroupNodeData }`,
      );
      const u = schema.unions["NodeData"];
      expect(u.variants).toHaveLength(2);
    });

    it("handles trailing commas", () => {
      const schema = parseFbsSource(`
        union U {
          A :TypeA,
          B :TypeB,
        }
      `);
      expect(schema.unions["U"].variants).toHaveLength(2);
    });

    it("parses union without explicit type names", () => {
      const schema = parseFbsSource(`
        union Simple {
          Foo,
          Bar,
        }
      `);
      const u = schema.unions["Simple"];
      expect(u.variants[0]).toMatchObject({ name: "Foo", type: "Foo" });
      expect(u.variants[1]).toMatchObject({ name: "Bar", type: "Bar" });
    });
  });

  describe("enum parsing", () => {
    it("parses enum with explicit values", () => {
      const schema = parseFbsSource(`
        enum Color : ubyte { Red = 0, Green = 1, Blue = 2 }
      `);
      const e = schema.enums["Color"];
      expect(e.underlyingType).toBe("ubyte");
      expect(e.values).toEqual([
        { name: "Red", value: 0 },
        { name: "Green", value: 1 },
        { name: "Blue", value: 2 },
      ]);
    });

    it("parses enum with implicit values", () => {
      const schema = parseFbsSource(`
        enum RepoAvailability : ubyte { Online = 0, ReadOnly, Offline }
      `);
      const e = schema.enums["RepoAvailability"];
      expect(e.values).toEqual([
        { name: "Online", value: 0 },
        { name: "ReadOnly", value: 1 },
        { name: "Offline", value: 2 },
      ]);
    });

    it("handles multiline enum", () => {
      const schema = parseFbsSource(`
        enum Status : uint8 {
          Active = 0,
          Inactive,
          Deleted = 10,
          Archived,
        }
      `);
      const e = schema.enums["Status"];
      expect(e.values).toEqual([
        { name: "Active", value: 0 },
        { name: "Inactive", value: 1 },
        { name: "Deleted", value: 10 },
        { name: "Archived", value: 11 },
      ]);
    });
  });

  describe("actual .fbs files", () => {
    it("parses all schema files without errors", () => {
      const schema = parseFbsFiles([
        join(schemaDir, "common.fbs"),
        join(schemaDir, "snapshot.fbs"),
        join(schemaDir, "manifest.fbs"),
        join(schemaDir, "repo.fbs"),
      ]);

      // Sanity check: known tables exist
      expect(schema.tables["Snapshot"]).toBeDefined();
      expect(schema.tables["NodeSnapshot"]).toBeDefined();
      expect(schema.tables["ArrayNodeData"]).toBeDefined();
      expect(schema.tables["GroupNodeData"]).toBeDefined();
      expect(schema.tables["ManifestRef"]).toBeDefined();
      expect(schema.tables["MetadataItem"]).toBeDefined();
      expect(schema.tables["DimensionName"]).toBeDefined();
      expect(schema.tables["Manifest"]).toBeDefined();
      expect(schema.tables["ArrayManifest"]).toBeDefined();
      expect(schema.tables["ChunkRef"]).toBeDefined();
      expect(schema.tables["Repo"]).toBeDefined();
      expect(schema.tables["Ref"]).toBeDefined();
      expect(schema.tables["SnapshotInfo"]).toBeDefined();

      // Known structs
      expect(schema.structs["ObjectId12"]).toBeDefined();
      expect(schema.structs["ObjectId8"]).toBeDefined();
      expect(schema.structs["ManifestFileInfo"]).toBeDefined();
      expect(schema.structs["DimensionShape"]).toBeDefined();
      expect(schema.structs["ChunkIndexRange"]).toBeDefined();

      // Known unions
      expect(schema.unions["NodeData"]).toBeDefined();
      expect(schema.unions["UpdateType"]).toBeDefined();

      // Known enums
      expect(schema.enums["RepoAvailability"]).toBeDefined();
    });

    it("parses correct field counts for key tables", () => {
      const schema = parseFbsFiles([
        join(schemaDir, "common.fbs"),
        join(schemaDir, "snapshot.fbs"),
        join(schemaDir, "manifest.fbs"),
        join(schemaDir, "repo.fbs"),
      ]);

      expect(schema.tables["Snapshot"].fields).toHaveLength(7);
      expect(schema.tables["NodeSnapshot"].fields).toHaveLength(4);
      expect(schema.tables["ArrayNodeData"].fields).toHaveLength(3);
      expect(schema.tables["ChunkRef"].fields).toHaveLength(8);
      expect(schema.tables["Manifest"].fields).toHaveLength(2);
      expect(schema.tables["ArrayManifest"].fields).toHaveLength(2);
      expect(schema.tables["MetadataItem"].fields).toHaveLength(2);
      expect(schema.tables["Repo"].fields).toHaveLength(9);
    });

    it("computes correct struct sizes", () => {
      const schema = parseFbsFiles([
        join(schemaDir, "common.fbs"),
        join(schemaDir, "snapshot.fbs"),
        join(schemaDir, "manifest.fbs"),
        join(schemaDir, "repo.fbs"),
      ]);

      expect(schema.structs["ObjectId12"].size).toBe(12);
      expect(schema.structs["ObjectId8"].size).toBe(8);
      expect(schema.structs["DimensionShape"].size).toBe(16);
      expect(schema.structs["ChunkIndexRange"].size).toBe(8);
    });
  });
});
