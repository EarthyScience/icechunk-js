/**
 * Schema sync test: validates that hardcoded constants in the TS FlatBuffer
 * parsers match the .fbs schema files maintained by the Rust team.
 *
 * No hardcoded expected values — everything is derived from .fbs files.
 */

import { describe, it, expect } from "vitest";
import { resolve, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { readFileSync } from "node:fs";
import { parseFbsFiles, type FbsSchema } from "./fbs-parser.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

// ── Path setup (relative to test file, not process.cwd) ──

const schemaDir = resolve(__dirname, "../../../../icechunk/flatbuffers");
const parserDir = resolve(
  __dirname,
  "../../../src/format/flatbuffers",
);

// ── Parse .fbs schema (source of truth) ──

const schema = parseFbsFiles([
  join(schemaDir, "common.fbs"),
  join(schemaDir, "snapshot.fbs"),
  join(schemaDir, "manifest.fbs"),
  join(schemaDir, "repo.fbs"),
]);

// ── Read TS parser source files ──

const snapshotParserSrc = readFileSync(
  join(parserDir, "snapshot-parser.ts"),
  "utf-8",
);
const manifestParserSrc = readFileSync(
  join(parserDir, "manifest-parser.ts"),
  "utf-8",
);
const repoParserSrc = readFileSync(
  join(parserDir, "repo-parser.ts"),
  "utf-8",
);

// ── Constant extraction helpers ──

/** Extract a numeric constant `const NAME = <number>` from TS source.
 * Anchored to start-of-line to avoid matching commented-out constants. */
function extractConstant(source: string, name: string): number | null {
  const re = new RegExp(`^[ \\t]*const\\s+${name}\\s*=\\s*(\\d+)`, "m");
  const m = source.match(re);
  return m ? parseInt(m[1], 10) : null;
}

/** Require a constant to exist and return its value. */
function requireConstant(
  source: string,
  name: string,
  file: string,
): number {
  const val = extractConstant(source, name);
  if (val === null) {
    throw new Error(`Constant ${name} not found in ${file}`);
  }
  return val;
}

/** Get the vtable index of a field in a table, accounting for union double-slots. */
function getFieldVtableIndex(
  schema: FbsSchema,
  tableName: string,
  fieldName: string,
): number {
  const table = schema.tables[tableName];
  if (!table) throw new Error(`Table ${tableName} not found in schema`);
  const field = table.fields.find((f) => f.name === fieldName);
  if (!field)
    throw new Error(
      `Field ${fieldName} not found in table ${tableName}`,
    );
  return field.vtableIndex;
}

/** Get the computed size of a struct from the schema. */
function getStructSize(schema: FbsSchema, structName: string): number {
  const s = schema.structs[structName];
  if (!s) throw new Error(`Struct ${structName} not found in schema`);
  return s.size;
}

/** Scalar type names for validation (used to distinguish scalars from tables). */
const SCALAR_SIZES_FOR_VALIDATION: Record<string, boolean> = {
  bool: true, uint8: true, int8: true, ubyte: true, byte: true,
  uint16: true, int16: true, ushort: true, short: true,
  uint32: true, int32: true, uint: true, int: true, float32: true, float: true,
  uint64: true, int64: true, ulong: true, long: true, float64: true, double: true,
  string: true,
};

// ── A. Field Index Validation ──

interface FieldIndexMapping {
  file: string;
  constant: string;
  table: string;
  field: string;
  /** True if this constant represents a union field (type slot = vtableIndex - 1). */
  isUnionTypeSlot?: boolean;
  /** Whether the schema field is required. Recorded here as an independent
   *  baseline so the test can detect schema-side requiredness changes. */
  required: boolean;
}

const FIELD_INDEX_MAPPINGS: FieldIndexMapping[] = [
  // ── snapshot-parser.ts: Snapshot fields ──
  { file: "snapshot-parser.ts", constant: "SNAPSHOT_ID", table: "Snapshot", field: "id", required: true },
  { file: "snapshot-parser.ts", constant: "SNAPSHOT_PARENT_ID", table: "Snapshot", field: "parent_id", required: false },
  { file: "snapshot-parser.ts", constant: "SNAPSHOT_NODES", table: "Snapshot", field: "nodes", required: true },
  { file: "snapshot-parser.ts", constant: "SNAPSHOT_FLUSHED_AT", table: "Snapshot", field: "flushed_at", required: false },
  { file: "snapshot-parser.ts", constant: "SNAPSHOT_MESSAGE", table: "Snapshot", field: "message", required: true },
  { file: "snapshot-parser.ts", constant: "SNAPSHOT_METADATA", table: "Snapshot", field: "metadata", required: true },
  { file: "snapshot-parser.ts", constant: "SNAPSHOT_MANIFEST_FILES", table: "Snapshot", field: "manifest_files", required: true },

  // ── snapshot-parser.ts: NodeSnapshot fields ──
  { file: "snapshot-parser.ts", constant: "NODE_ID", table: "NodeSnapshot", field: "id", required: true },
  { file: "snapshot-parser.ts", constant: "NODE_PATH", table: "NodeSnapshot", field: "path", required: true },
  { file: "snapshot-parser.ts", constant: "NODE_USER_DATA", table: "NodeSnapshot", field: "user_data", required: true },
  // NODE_DATA is the union type slot: the constant value = type slot index,
  // and the schema's vtableIndex = value slot = type slot + 1
  { file: "snapshot-parser.ts", constant: "NODE_DATA", table: "NodeSnapshot", field: "node_data", isUnionTypeSlot: true, required: true },

  // ── snapshot-parser.ts: ArrayNodeData fields ──
  { file: "snapshot-parser.ts", constant: "ARRAY_SHAPE", table: "ArrayNodeData", field: "shape", required: true },
  { file: "snapshot-parser.ts", constant: "ARRAY_DIMENSION_NAMES", table: "ArrayNodeData", field: "dimension_names", required: false },
  { file: "snapshot-parser.ts", constant: "ARRAY_MANIFESTS", table: "ArrayNodeData", field: "manifests", required: true },

  // ── snapshot-parser.ts: ManifestRef fields ──
  { file: "snapshot-parser.ts", constant: "MANIFEST_REF_OBJECT_ID", table: "ManifestRef", field: "object_id", required: true },
  { file: "snapshot-parser.ts", constant: "MANIFEST_REF_EXTENTS", table: "ManifestRef", field: "extents", required: true },

  // ── snapshot-parser.ts: MetadataItem fields ──
  { file: "snapshot-parser.ts", constant: "METADATA_NAME", table: "MetadataItem", field: "name", required: true },
  { file: "snapshot-parser.ts", constant: "METADATA_VALUE", table: "MetadataItem", field: "value", required: true },

  // ── snapshot-parser.ts: DimensionName fields ──
  { file: "snapshot-parser.ts", constant: "DIMENSION_NAME_NAME", table: "DimensionName", field: "name", required: false },

  // ── manifest-parser.ts: Manifest fields ──
  { file: "manifest-parser.ts", constant: "MANIFEST_ID", table: "Manifest", field: "id", required: true },
  { file: "manifest-parser.ts", constant: "MANIFEST_ARRAYS", table: "Manifest", field: "arrays", required: true },

  // ── manifest-parser.ts: ArrayManifest fields ──
  { file: "manifest-parser.ts", constant: "ARRAY_MANIFEST_NODE_ID", table: "ArrayManifest", field: "node_id", required: true },
  { file: "manifest-parser.ts", constant: "ARRAY_MANIFEST_REFS", table: "ArrayManifest", field: "refs", required: true },

  // ── manifest-parser.ts: ChunkRef fields ──
  { file: "manifest-parser.ts", constant: "CHUNK_REF_INDEX", table: "ChunkRef", field: "index", required: true },
  { file: "manifest-parser.ts", constant: "CHUNK_REF_INLINE", table: "ChunkRef", field: "inline", required: false },
  { file: "manifest-parser.ts", constant: "CHUNK_REF_OFFSET", table: "ChunkRef", field: "offset", required: false },
  { file: "manifest-parser.ts", constant: "CHUNK_REF_LENGTH", table: "ChunkRef", field: "length", required: false },
  { file: "manifest-parser.ts", constant: "CHUNK_REF_CHUNK_ID", table: "ChunkRef", field: "chunk_id", required: false },
  { file: "manifest-parser.ts", constant: "CHUNK_REF_LOCATION", table: "ChunkRef", field: "location", required: false },
  { file: "manifest-parser.ts", constant: "CHUNK_REF_CHECKSUM_ETAG", table: "ChunkRef", field: "checksum_etag", required: false },
  { file: "manifest-parser.ts", constant: "CHUNK_REF_CHECKSUM_LAST_MODIFIED", table: "ChunkRef", field: "checksum_last_modified", required: false },

  // ── repo-parser.ts: Repo fields ──
  { file: "repo-parser.ts", constant: "REPO_SPEC_VERSION", table: "Repo", field: "spec_version", required: false },
  { file: "repo-parser.ts", constant: "REPO_TAGS", table: "Repo", field: "tags", required: true },
  { file: "repo-parser.ts", constant: "REPO_BRANCHES", table: "Repo", field: "branches", required: true },
  // REPO_DELETED_TAGS is commented out in the parser — index 3 is still consumed by deleted_tags
  { file: "repo-parser.ts", constant: "REPO_SNAPSHOTS", table: "Repo", field: "snapshots", required: true },

  // ── repo-parser.ts: Ref fields ──
  { file: "repo-parser.ts", constant: "REF_NAME", table: "Ref", field: "name", required: true },
  { file: "repo-parser.ts", constant: "REF_SNAPSHOT_INDEX", table: "Ref", field: "snapshot_index", required: false },

  // ── repo-parser.ts: SnapshotInfo fields ──
  { file: "repo-parser.ts", constant: "SNAPSHOT_INFO_ID", table: "SnapshotInfo", field: "id", required: true },
];

// ── B. Struct Size Validation ──

interface StructSizeMapping {
  file: string;
  constant: string;
  struct: string;
}

const STRUCT_SIZE_MAPPINGS: StructSizeMapping[] = [
  // snapshot-parser.ts
  { file: "snapshot-parser.ts", constant: "OBJECT_ID_12_SIZE", struct: "ObjectId12" },
  { file: "snapshot-parser.ts", constant: "OBJECT_ID_8_SIZE", struct: "ObjectId8" },
  { file: "snapshot-parser.ts", constant: "MANIFEST_FILE_INFO_SIZE", struct: "ManifestFileInfo" },
  { file: "snapshot-parser.ts", constant: "DIMENSION_SHAPE_SIZE", struct: "DimensionShape" },
  { file: "snapshot-parser.ts", constant: "CHUNK_INDEX_RANGE_SIZE", struct: "ChunkIndexRange" },
  // manifest-parser.ts (duplicates for ObjectId sizes)
  { file: "manifest-parser.ts", constant: "OBJECT_ID_12_SIZE", struct: "ObjectId12" },
  { file: "manifest-parser.ts", constant: "OBJECT_ID_8_SIZE", struct: "ObjectId8" },
  // repo-parser.ts
  { file: "repo-parser.ts", constant: "OBJECT_ID_12_SIZE", struct: "ObjectId12" },
];

// ── Helpers for source file lookup ──

function getSource(file: string): string {
  switch (file) {
    case "snapshot-parser.ts":
      return snapshotParserSrc;
    case "manifest-parser.ts":
      return manifestParserSrc;
    case "repo-parser.ts":
      return repoParserSrc;
    default:
      throw new Error(`Unknown parser file: ${file}`);
  }
}

// ── Read method → compatible FlatBuffer types mapping ──

/** Maps parser read methods to the set of compatible FlatBuffer schema types. */
const READ_METHOD_TO_TYPES: Record<string, Set<string>> = {
  readUint8: new Set(["uint8", "ubyte", "bool"]),
  readUint16: new Set(["uint16", "ushort"]),
  readUint32: new Set(["uint32", "uint", "int32", "int"]),
  readUint64: new Set(["uint64", "ulong", "int64", "long"]),
  readString: new Set(["string"]),
  readByteVector: new Set(["[uint8]"]),
  readUint32Vector: new Set(["[uint32]"]),
};

/** Detect ALL read methods the parser uses for a given constant. */
function detectReadMethods(source: string, constant: string): string[] {
  const methods = [
    "readUint8", "readUint16", "readUint32", "readUint64",
    "readString", "readByteVector", "readUint32Vector",
    "readInlineStruct", "readVectorStruct",
    "getVectorLength", "getVectorTable", "getNestedTable",
  ];
  const found: string[] = [];
  for (const method of methods) {
    const pattern = new RegExp(`\\.${method}\\(\\s*\\b${constant}\\b`);
    if (pattern.test(source)) found.push(method);
  }
  return found;
}

/** Normalize schema type for comparison (strip vector brackets for vector-accessed fields). */
function normalizeSchemaType(schemaType: string): string {
  // [SomeType] → SomeType for vector access checks
  const vecMatch = schemaType.match(/^\[(.+)\]$/);
  return vecMatch ? vecMatch[1] : schemaType;
}

// ── Tests ──

describe("Schema Sync", () => {
  // ── A. Field Index Validation ──
  describe("A. Field index validation", () => {
    for (const mapping of FIELD_INDEX_MAPPINGS) {
      it(`${mapping.table}.${mapping.field} (${mapping.constant} in ${mapping.file})`, () => {
        const source = getSource(mapping.file);
        const tsValue = requireConstant(
          source,
          mapping.constant,
          mapping.file,
        );
        const schemaVtableIndex = getFieldVtableIndex(
          schema,
          mapping.table,
          mapping.field,
        );

        if (mapping.isUnionTypeSlot) {
          // For union fields: TS constant = type slot, schema vtableIndex = value slot
          // So tsValue should equal schemaVtableIndex - 1
          expect(tsValue, [
            `Schema sync: ${mapping.table}.${mapping.field}`,
            `  TS constant ${mapping.constant} = ${tsValue} (type slot)`,
            `  Schema vtable value slot = ${schemaVtableIndex}`,
            `  Expected type slot = ${schemaVtableIndex - 1}`,
          ].join("\n")).toBe(schemaVtableIndex - 1);
        } else {
          expect(tsValue, [
            `Schema sync: ${mapping.table}.${mapping.field}`,
            `  Expected vtable index: ${tsValue} (from ${mapping.constant} in ${mapping.file})`,
            `  Actual vtable index: ${schemaVtableIndex} (from .fbs schema)`,
          ].join("\n")).toBe(schemaVtableIndex);
        }
      });
    }
  });

  // ── B. Struct Size Validation ──
  describe("B. Struct size validation", () => {
    for (const mapping of STRUCT_SIZE_MAPPINGS) {
      it(`${mapping.struct} size (${mapping.constant} in ${mapping.file})`, () => {
        const source = getSource(mapping.file);
        const tsValue = requireConstant(
          source,
          mapping.constant,
          mapping.file,
        );
        const schemaSize = getStructSize(schema, mapping.struct);

        expect(tsValue, [
          `Schema sync: ${mapping.struct} size`,
          `  Parser constant: ${tsValue} (${mapping.constant})`,
          `  Schema computed: ${schemaSize}`,
        ].join("\n")).toBe(schemaSize);
      });
    }
  });

  // ── C. Union Ordinal Validation ──
  describe("C. Union ordinal validation", () => {
    it("NodeData union ordinals in parseNodeData()", () => {
      // Extract the union ordinals from the parseNodeData function body
      const fnMatch = snapshotParserSrc.match(
        /function parseNodeData\b[\s\S]*?^}/m,
      );
      expect(fnMatch, "parseNodeData function not found").toBeTruthy();
      const fnBody = fnMatch![0];

      // Find all `unionType === N` comparisons
      const ordinalMatches = [...fnBody.matchAll(/unionType\s*===\s*(\d+)/g)];
      const parsedOrdinals = ordinalMatches.map((m) => parseInt(m[1], 10));

      // Schema union
      const nodeDataUnion = schema.unions["NodeData"];
      expect(nodeDataUnion, "NodeData union not found in schema").toBeDefined();

      // The parser should handle NONE (0) + all variants
      const schemaOrdinals = [0, ...nodeDataUnion.variants.map((v) => v.ordinal)];

      // Every ordinal the parser checks should exist in the schema
      for (const ord of parsedOrdinals) {
        expect(
          schemaOrdinals,
          `Parser checks ordinal ${ord} but schema NodeData union doesn't define it`,
        ).toContain(ord);
      }

      // Every schema ordinal should be handled by the parser
      for (const ord of schemaOrdinals) {
        expect(
          parsedOrdinals,
          [
            `Schema sync: NodeData union variant count`,
            `  Parser handles: ordinals [${parsedOrdinals.join(", ")}]`,
            `  Schema defines: ordinals [${schemaOrdinals.join(", ")}]`,
            `  Missing ordinal: ${ord}`,
          ].join("\n"),
        ).toContain(ord);
      }
    });

    it("NodeData union variant identity mapping", () => {
      // Validates that each ordinal maps to the correct variant, not just
      // that the ordinal sets match. A semantic remap (swapping Array/Group
      // ordinals) would pass the ordinal-set test but fail this one.
      const nodeDataUnion = schema.unions["NodeData"];
      expect(nodeDataUnion).toBeDefined();

      // The parser's identity mapping: ordinal → what it parses as.
      // Extracted from the parseNodeData function's code paths.
      const PARSER_ORDINAL_IDENTITY: Record<number, string> = {
        // unionType === 1 → calls parseArrayNodeData → "Array"
        1: "Array",
        // unionType === 2 → returns { type: "group" } → "Group"
        2: "Group",
      };

      for (const [ordStr, expectedVariant] of Object.entries(PARSER_ORDINAL_IDENTITY)) {
        const ordinal = parseInt(ordStr, 10);
        const schemaVariant = nodeDataUnion.variants.find((v) => v.ordinal === ordinal);

        expect(
          schemaVariant,
          `Schema has no variant at ordinal ${ordinal}`,
        ).toBeDefined();

        expect(schemaVariant!.name, [
          `Schema sync: NodeData variant identity mismatch at ordinal ${ordinal}`,
          `  Parser treats ordinal ${ordinal} as: ${expectedVariant}`,
          `  Schema defines ordinal ${ordinal} as: ${schemaVariant!.name}`,
        ].join("\n")).toBe(expectedVariant);
      }
    });

    it("NodeData union value-slot access uses correct offset", () => {
      // The parser reads the union type byte at NODE_DATA and the union
      // payload table at NODE_DATA + 1. Verify the source contains this
      // pattern — if NODE_DATA changes or the +1 offset is wrong, this fails.
      const nodeDataConst = requireConstant(
        snapshotParserSrc,
        "NODE_DATA",
        "snapshot-parser.ts",
      );

      // Verify the parser reads the type byte at NODE_DATA
      const typeReadPattern = /\.readUint8\(\s*NODE_DATA\b/;
      expect(
        typeReadPattern.test(snapshotParserSrc),
        "parseNodeData should read union type byte via readUint8(NODE_DATA...)",
      ).toBe(true);

      // Verify the parser reads the value table at NODE_DATA + 1
      const valueSlotPattern = /\.getNestedTable\(\s*NODE_DATA\s*\+\s*1\s*\)/;
      expect(
        valueSlotPattern.test(snapshotParserSrc),
        "parseNodeData should read union value via getNestedTable(NODE_DATA + 1)",
      ).toBe(true);

      // Verify NODE_DATA + 1 matches the schema's vtable index for the union value slot
      const schemaVtableIndex = getFieldVtableIndex(
        schema,
        "NodeSnapshot",
        "node_data",
      );
      expect(nodeDataConst + 1, [
        `Union value slot mismatch:`,
        `  NODE_DATA + 1 = ${nodeDataConst + 1}`,
        `  Schema vtable value index = ${schemaVtableIndex}`,
      ].join("\n")).toBe(schemaVtableIndex);
    });
  });

  // ── D. Total Field Count Guards ──
  describe("D. Total field count guards", () => {
    // Collect all tables referenced in field index mappings
    const tablesUsed = new Map<string, Set<string>>();
    for (const mapping of FIELD_INDEX_MAPPINGS) {
      if (!tablesUsed.has(mapping.table)) {
        tablesUsed.set(mapping.table, new Set());
      }
      tablesUsed.get(mapping.table)!.add(mapping.field);
    }

    for (const [tableName, mappedFields] of tablesUsed) {
      it(`${tableName}: all fields covered`, () => {
        const table = schema.tables[tableName];
        expect(table, `Table ${tableName} not found in schema`).toBeDefined();

        const schemaFieldNames = table.fields.map((f) => f.name);
        const unmappedFields = schemaFieldNames.filter(
          (f) => !mappedFields.has(f),
        );

        // Special case: Repo.deleted_tags is intentionally skipped (commented out)
        const allowedUnmapped: Record<string, string[]> = {
          Repo: ["deleted_tags", "status", "metadata", "latest_updates", "repo_before_updates"],
          SnapshotInfo: ["parent_offset", "flushed_at", "message", "metadata"],
        };

        const expectedUnmapped = allowedUnmapped[tableName] ?? [];
        const trulyUnmapped = unmappedFields.filter(
          (f) => !expectedUnmapped.includes(f),
        );

        expect(trulyUnmapped, [
          `Schema sync: ${tableName} has fields not covered by parser`,
          `  Unmapped fields: ${trulyUnmapped.join(", ")}`,
          `  This may indicate new fields were added to the schema.`,
          `  Add mappings in schema-sync.test.ts or add to allowedUnmapped.`,
        ].join("\n")).toEqual([]);
      });
    }
  });

  // ── E. Field Type Validation (comprehensive) ──
  describe("E. Field type validation", () => {
    // Auto-detect read method for EVERY field index mapping and validate
    // against the schema type. No hand-picked subset — all fields checked.
    // Also validates structural access patterns (vectors, tables, structs).
    for (const mapping of FIELD_INDEX_MAPPINGS) {
      // Skip union type-slot fields — they're read as uint8 (type byte)
      // but the schema type is the union name, not uint8
      if (mapping.isUnionTypeSlot) continue;

      it(`${mapping.table}.${mapping.field} read method matches schema type`, () => {
        const source = getSource(mapping.file);
        const readMethods = detectReadMethods(source, mapping.constant);

        const table = schema.tables[mapping.table];
        const field = table.fields.find((f) => f.name === mapping.field)!;
        const schemaType = field.type;
        const isVectorType = schemaType.startsWith("[") && schemaType.endsWith("]");
        const innerType = isVectorType ? schemaType.slice(1, -1) : schemaType;

        // Constants used only as index arithmetic operands (e.g. NODE_DATA + 1
        // for the union value slot) won't match any read method directly.
        // Constants passed as arguments to helper functions rather than called
        // as method receivers (e.g. binarySearchRef(repo, REPO_TAGS, name))
        const INDEX_ONLY_CONSTANTS = new Set<string>([
          "REPO_TAGS",
          "REPO_BRANCHES",
        ]);

        expect(
          readMethods.length > 0 || INDEX_ONLY_CONSTANTS.has(mapping.constant),
          [
            `${mapping.table}.${mapping.field}: constant ${mapping.constant} in ${mapping.file}`,
            `  has no detected read-method usage. If the constant was renamed or`,
            `  the parser code changed, add a mapping update. If it's intentionally`,
            `  index-only, add it to INDEX_ONLY_CONSTANTS.`,
          ].join("\n"),
        ).toBe(true);

        if (readMethods.length === 0) return;

        // Validate EVERY detected method, not just the first.
        for (const readMethod of readMethods) {
          // ── Scalar read methods ──
          const compatibleTypes = READ_METHOD_TO_TYPES[readMethod];
          if (compatibleTypes) {
            expect(
              compatibleTypes.has(schemaType),
              [
                `Schema sync: ${mapping.table}.${mapping.field} type mismatch`,
                `  Schema type: ${schemaType}`,
                `  Parser read method: ${readMethod}`,
                `  Compatible types: ${[...compatibleTypes].join(", ")}`,
              ].join("\n"),
            ).toBe(true);
          }

          // ── Inline struct access ──
          if (readMethod === "readInlineStruct") {
            expect(
              schema.structs[schemaType] !== undefined,
              `${mapping.table}.${mapping.field}: readInlineStruct used but schema type "${schemaType}" is not a known struct`,
            ).toBe(true);
          }

          // ── Vector-of-structs access ──
          if (readMethod === "readVectorStruct") {
            expect(isVectorType, [
              `${mapping.table}.${mapping.field}: readVectorStruct used`,
              `  but schema type "${schemaType}" is not a vector type`,
            ].join("\n")).toBe(true);
            expect(
              schema.structs[innerType] !== undefined,
              `${mapping.table}.${mapping.field}: readVectorStruct used but inner type "${innerType}" is not a known struct`,
            ).toBe(true);
          }

          // ── Vector-of-tables access ──
          if (readMethod === "getVectorTable") {
            expect(isVectorType, [
              `${mapping.table}.${mapping.field}: getVectorTable used`,
              `  but schema type "${schemaType}" is not a vector type`,
            ].join("\n")).toBe(true);
            // getVectorTable is for table elements — inner type must be a table
            expect(
              schema.tables[innerType] !== undefined,
              [
                `${mapping.table}.${mapping.field}: getVectorTable used`,
                `  but inner type "${innerType}" is not a known table`,
                `  If the schema changed to a scalar vector, parser should use readByteVector/readUint32Vector instead`,
              ].join("\n"),
            ).toBe(true);
          }

          // ── Vector length access ──
          if (readMethod === "getVectorLength") {
            expect(isVectorType, [
              `${mapping.table}.${mapping.field}: getVectorLength used`,
              `  but schema type "${schemaType}" is not a vector type`,
            ].join("\n")).toBe(true);
          }

          // ── Nested table access ──
          if (readMethod === "getNestedTable") {
            expect(
              schema.tables[schemaType] !== undefined,
              `${mapping.table}.${mapping.field}: getNestedTable used but schema type "${schemaType}" is not a known table`,
            ).toBe(true);
          }
        }
      });
    }
  });

  // ── E2. Requiredness Validation ──
  describe("E2. Requiredness validation", () => {
    // Each mapping carries an independent `required` baseline recorded when
    // the mapping was written. The test compares that baseline against the
    // live schema. If the schema flips required↔optional, the baseline
    // mismatches and the test fails — no tautology.
    // Union type-slot fields are included: their requiredness matters too.
    for (const mapping of FIELD_INDEX_MAPPINGS) {
      it(`${mapping.table}.${mapping.field} requiredness matches baseline`, () => {
        const table = schema.tables[mapping.table];
        const field = table.fields.find((f) => f.name === mapping.field)!;

        expect(field.required, [
          `Schema sync: ${mapping.table}.${mapping.field} requiredness changed`,
          `  Mapping baseline: required=${mapping.required}`,
          `  Schema now says:  required=${field.required}`,
          `  Update the mapping baseline AND the parser's null-handling.`,
        ].join("\n")).toBe(mapping.required);
      });
    }
  });

  // ── E3. Default Value Drift ──
  describe("E3. Default value validation", () => {
    // For fields with explicit defaults in the .fbs schema, verify the
    // parser's read call passes a matching default. If the schema default
    // changes (e.g. offset: uint64 = 0 → = 1), the parser's hardcoded
    // fallback must be updated too.
    for (const mapping of FIELD_INDEX_MAPPINGS) {
      if (mapping.isUnionTypeSlot) continue;

      const table = schema.tables[mapping.table];
      const field = table.fields.find((f) => f.name === mapping.field)!;
      if (field.defaultValue === null) continue;

      it(`${mapping.table}.${mapping.field} default matches schema (${field.defaultValue})`, () => {
        const source = getSource(mapping.file);

        // Extract the default passed in the parser's read call:
        //   readUint64(CONSTANT, 0n)  → "0n"
        //   readUint32(CONSTANT, 0)   → "0"
        // Capture only the literal value (digits, optional 'n' suffix).
        const defaultPattern = new RegExp(
          `\\.read\\w+\\([\\s\\S]*?\\b${mapping.constant}\\b[\\s\\S]*?,\\s*(-?\\d+n?)`,
        );
        const m = source.match(defaultPattern);
        if (!m) return; // Field read without explicit default (uses method's own default)

        const parserDefault = m[1].trim();

        // Normalize for comparison: "0n" → "0", "0" → "0"
        const normalizedParser = parserDefault.replace(/n$/, "");
        const normalizedSchema = field.defaultValue;

        expect(normalizedParser, [
          `Schema sync: ${mapping.table}.${mapping.field} default value drift`,
          `  Schema default: ${normalizedSchema}`,
          `  Parser default: ${parserDefault}`,
        ].join("\n")).toBe(normalizedSchema);
      });
    }
  });

  // ── F. Enum Checks ──
  describe("F. Enum checks", () => {
    // Baseline for ALL enums in the schema. If a new enum is added to the
    // .fbs files, the coverage guard at the bottom will fail until a
    // baseline entry is added here.
    const ENUM_BASELINES: Record<string, {
      underlyingType: string;
      values: Record<string, number>;
    }> = {
      RepoAvailability: {
        underlyingType: "ubyte",
        values: { Online: 0, ReadOnly: 1, Offline: 2 },
      },
    };

    for (const [enumName, baseline] of Object.entries(ENUM_BASELINES)) {
      describe(enumName, () => {
        it("exists in schema", () => {
          expect(
            schema.enums[enumName],
            `Enum ${enumName} not found in schema`,
          ).toBeDefined();
        });

        it("underlying type matches baseline", () => {
          const e = schema.enums[enumName];
          expect(e.underlyingType, [
            `Enum ${enumName} underlying type changed`,
            `  Baseline: ${baseline.underlyingType}`,
            `  Schema:   ${e.underlyingType}`,
          ].join("\n")).toBe(baseline.underlyingType);
        });

        it("values match baseline", () => {
          const e = schema.enums[enumName];
          const schemaValues: Record<string, number> = {};
          for (const v of e.values) schemaValues[v.name] = v.value;

          expect(schemaValues, [
            `Enum ${enumName} values changed`,
            `  Baseline: ${JSON.stringify(baseline.values)}`,
            `  Schema:   ${JSON.stringify(schemaValues)}`,
          ].join("\n")).toEqual(baseline.values);
        });
      });
    }

    it("no unvalidated enums in schema", () => {
      const schemaEnumNames = Object.keys(schema.enums);
      const baselineEnumNames = Object.keys(ENUM_BASELINES);
      const uncovered = schemaEnumNames.filter(
        (n) => !baselineEnumNames.includes(n),
      );
      expect(uncovered, [
        `New enums found in schema without baselines:`,
        ...uncovered.map((n) => `  ${n}`),
        `  Add a baseline entry in ENUM_BASELINES.`,
      ].join("\n")).toEqual([]);
    });
  });

  // ── G. Coverage Guard (Meta-Test) ──
  describe("G. Coverage guard", () => {
    // Constants that are NOT schema field/struct indices — whitelist
    const NON_SCHEMA_CONSTANTS = new Set(["SUPPORTED_SPEC_VERSION"]);

    // Collect all mapped constant names per file
    const mappedConstantsPerFile = new Map<string, Set<string>>();
    for (const m of FIELD_INDEX_MAPPINGS) {
      if (!mappedConstantsPerFile.has(m.file)) {
        mappedConstantsPerFile.set(m.file, new Set());
      }
      mappedConstantsPerFile.get(m.file)!.add(m.constant);
    }
    for (const m of STRUCT_SIZE_MAPPINGS) {
      if (!mappedConstantsPerFile.has(m.file)) {
        mappedConstantsPerFile.set(m.file, new Set());
      }
      mappedConstantsPerFile.get(m.file)!.add(m.constant);
    }

    const parserFiles = [
      "snapshot-parser.ts",
      "manifest-parser.ts",
      "repo-parser.ts",
    ] as const;

    for (const file of parserFiles) {
      it(`all numeric constants in ${file} are covered`, () => {
        const source = getSource(file);
        const mapped = mappedConstantsPerFile.get(file) ?? new Set();

        // Find all `const UPPER_CASE = <number>` declarations (skip commented-out)
        const allConstants = [
          ...source.matchAll(/^[ \t]*const\s+([A-Z][A-Z0-9_]*)\s*=\s*(\d+)/gm),
        ];

        const uncovered: string[] = [];
        for (const [, name] of allConstants) {
          if (!mapped.has(name) && !NON_SCHEMA_CONSTANTS.has(name)) {
            uncovered.push(name);
          }
        }

        expect(uncovered, [
          `Coverage guard: untested constants in ${file}`,
          ...uncovered.map((c) => `  ${c} has no mapping in schema-sync test`),
          `  Add to FIELD_INDEX_MAPPINGS, STRUCT_SIZE_MAPPINGS, or NON_SCHEMA_CONSTANTS.`,
        ].join("\n")).toEqual([]);
      });
    }

    it("no mappings reference non-existent constants", () => {
      const errors: string[] = [];

      for (const m of FIELD_INDEX_MAPPINGS) {
        const source = getSource(m.file);
        if (extractConstant(source, m.constant) === null) {
          // Check if it's a commented-out constant
          const commentedPattern = new RegExp(
            `//\\s*const\\s+${m.constant}\\s*=`,
          );
          if (!commentedPattern.test(source)) {
            errors.push(
              `${m.constant} mapped but not found in ${m.file}`,
            );
          }
        }
      }

      for (const m of STRUCT_SIZE_MAPPINGS) {
        const source = getSource(m.file);
        if (extractConstant(source, m.constant) === null) {
          errors.push(
            `${m.constant} mapped but not found in ${m.file}`,
          );
        }
      }

      expect(errors).toEqual([]);
    });
  });
});
