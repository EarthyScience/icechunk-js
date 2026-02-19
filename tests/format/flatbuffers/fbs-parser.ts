/**
 * Mini line-based parser for FlatBuffer .fbs schema files.
 *
 * Extracts tables, structs, unions, and enums with computed
 * vtable indices and struct sizes for schema-sync testing.
 */

import { readFileSync } from "node:fs";

// ── Public types ──

export interface FbsField {
  name: string;
  type: string;
  required: boolean;
  deprecated: boolean;
  defaultValue: string | null;
  /** FlatBuffer vtable slot index (0-based). Unions consume two slots. */
  vtableIndex: number;
}

export interface FbsTable {
  name: string;
  fields: FbsField[];
}

export interface FbsStructField {
  name: string;
  type: string;
}

export interface FbsStruct {
  name: string;
  fields: FbsStructField[];
  /** Computed size in bytes with FlatBuffer alignment rules. */
  size: number;
}

export interface FbsUnionVariant {
  name: string;
  /** Alias (the type name, which may differ from the variant name). */
  type: string;
  /** 1-indexed ordinal (0 = NONE, implicit). */
  ordinal: number;
}

export interface FbsUnion {
  name: string;
  variants: FbsUnionVariant[];
}

export interface FbsEnumValue {
  name: string;
  value: number;
}

export interface FbsEnum {
  name: string;
  underlyingType: string;
  values: FbsEnumValue[];
}

export interface FbsSchema {
  tables: Record<string, FbsTable>;
  structs: Record<string, FbsStruct>;
  unions: Record<string, FbsUnion>;
  enums: Record<string, FbsEnum>;
}

// ── Size lookup for scalar/fixed types ──

const SCALAR_SIZES: Record<string, number> = {
  bool: 1,
  uint8: 1,
  int8: 1,
  ubyte: 1,
  byte: 1,
  uint16: 2,
  int16: 2,
  ushort: 2,
  short: 2,
  uint32: 4,
  int32: 4,
  uint: 4,
  int: 4,
  float32: 4,
  float: 4,
  uint64: 8,
  int64: 8,
  ulong: 8,
  long: 8,
  float64: 8,
  double: 8,
};

// ── Union type detection for vtable double-slot ──

function isUnionType(type: string, unions: Map<string, string[]>): boolean {
  return unions.has(type);
}

// ── State machine ──

type State = "top" | "table" | "struct" | "union" | "enum";

export function parseFbsSource(source: string): FbsSchema {
  const schema: FbsSchema = {
    tables: {},
    structs: {},
    unions: {},
    enums: {},
  };

  // First pass: collect union names so we can detect union fields in tables.
  const unionNames = new Map<string, string[]>();
  const lines = source.split("\n");

  // Quick pre-scan for unions
  {
    let inUnion = false;
    let unionName = "";
    let variants: string[] = [];
    for (const raw of lines) {
      const line = stripComment(raw).trim();
      const unionMatch = line.match(/^union\s+(\w+)\s*\{/);
      if (unionMatch) {
        inUnion = true;
        unionName = unionMatch[1];
        variants = [];
        const rest = line.slice(line.indexOf("{") + 1);
        parseUnionVariantsInline(rest, variants);
        if (line.includes("}")) {
          unionNames.set(unionName, variants);
          inUnion = false;
        }
        continue;
      }
      if (inUnion) {
        if (line.includes("}")) {
          const before = line.slice(0, line.indexOf("}"));
          parseUnionVariantsInline(before, variants);
          unionNames.set(unionName, variants);
          inUnion = false;
        } else {
          parseUnionVariantsInline(line, variants);
        }
      }
    }
  }

  // Main parse pass
  let state: State = "top";
  let currentTable: FbsTable | null = null;
  let currentStruct: { name: string; fields: FbsStructField[] } | null = null;
  let currentUnion: { name: string; variants: FbsUnionVariant[] } | null = null;
  let currentEnum: {
    name: string;
    underlyingType: string;
    values: FbsEnumValue[];
    nextValue: number;
  } | null = null;
  let vtableIndex = 0;

  for (const raw of lines) {
    const line = stripComment(raw).trim();
    if (line === "" || line.startsWith("include ") || line.startsWith("namespace ") || line.startsWith("root_type ") || line.startsWith("file_identifier") || line.startsWith("file_extension") || line.startsWith("attribute ")) {
      // Skip directives (but still check for close brace in nested context)
      if (state !== "top" && line === "}") {
        finishBlock();
      }
      continue;
    }

    switch (state) {
      case "top":
        parseTopLevel(line);
        break;
      case "table":
        parseTableField(line);
        break;
      case "struct":
        parseStructField(line);
        break;
      case "union":
        parseUnionBody(line);
        break;
      case "enum":
        parseEnumBody(line);
        break;
    }
  }

  return schema;

  // ── Helpers ──

  function parseTopLevel(line: string): void {
    // table Name { ... }
    const tableMatch = line.match(/^table\s+(\w+)\s*\{/);
    if (tableMatch) {
      state = "table";
      currentTable = { name: tableMatch[1], fields: [] };
      vtableIndex = 0;
      // Check if single-line (empty table)
      if (line.includes("}")) {
        schema.tables[currentTable.name] = currentTable;
        state = "top";
        currentTable = null;
      }
      return;
    }

    // struct Name { ... }
    const structMatch = line.match(/^struct\s+(\w+)\s*\{/);
    if (structMatch) {
      state = "struct";
      currentStruct = { name: structMatch[1], fields: [] };
      if (line.includes("}")) {
        finalizeStruct();
      }
      return;
    }

    // union Name { ... }
    const unionMatch = line.match(/^union\s+(\w+)\s*\{/);
    if (unionMatch) {
      state = "union";
      currentUnion = { name: unionMatch[1], variants: [] };
      // Parse any variants on the opening line
      const rest = line.slice(line.indexOf("{") + 1);
      parseUnionBody(rest);
      // If closing brace is on the same line, finishBlock was already called
      return;
    }

    // enum Name : type { ... }
    const enumMatch = line.match(/^enum\s+(\w+)\s*:\s*(\w+)\s*\{/);
    if (enumMatch) {
      state = "enum";
      currentEnum = {
        name: enumMatch[1],
        underlyingType: enumMatch[2],
        values: [],
        nextValue: 0,
      };
      // Parse any values on the opening line
      const rest = line.slice(line.indexOf("{") + 1);
      parseEnumBody(rest);
      return;
    }
  }

  function parseTableField(line: string): void {
    if (line === "}" || line.startsWith("}")) {
      finishBlock();
      return;
    }

    // field_name: type (attributes) = default;
    const fieldMatch = line.match(
      /^(\w+)\s*:\s*(.+?)(?:\s*=\s*([^;(]+?))?\s*;/,
    );
    if (!fieldMatch) return;

    const [, name, rawType] = fieldMatch;
    let defaultValue = fieldMatch[3]?.trim() ?? null;

    // Parse attributes from the type portion
    let type = rawType.trim();
    const required = /\(\s*required\s*\)/.test(type);
    const deprecated = /\(\s*deprecated\s*\)/.test(type);
    // Strip attributes from type
    type = type.replace(/\s*\([^)]*\)/g, "").trim();

    // Check if this field is a union type (consumes two vtable slots)
    const isUnion = isUnionType(type, unionNames);

    const field: FbsField = {
      name,
      type,
      required,
      deprecated,
      defaultValue,
      vtableIndex: isUnion ? vtableIndex + 1 : vtableIndex,
    };

    currentTable!.fields.push(field);

    // Union fields take two vtable slots: type (uint8) + value (offset)
    vtableIndex += isUnion ? 2 : 1;
  }

  function parseStructField(line: string): void {
    if (line === "}" || line.startsWith("}")) {
      finishBlock();
      return;
    }

    const fieldMatch = line.match(/^(\w+)\s*:\s*(.+?)\s*;/);
    if (!fieldMatch) return;

    let type = fieldMatch[2].trim();
    // Strip attributes
    type = type.replace(/\s*\([^)]*\)/g, "").trim();

    currentStruct!.fields.push({ name: fieldMatch[1], type });
  }

  function parseUnionBody(line: string): void {
    if (line.includes("}")) {
      const before = line.slice(0, line.indexOf("}"));
      addUnionVariants(before);
      finishBlock();
      return;
    }
    addUnionVariants(line);
  }

  function addUnionVariants(text: string): void {
    if (!currentUnion) return;
    // Variants: "VariantName" or "VariantName :TypeName" separated by commas
    const parts = text.split(",");
    for (const part of parts) {
      const trimmed = part.trim();
      if (!trimmed) continue;

      const variantMatch = trimmed.match(/^(\w+)(?:\s*:\s*(\w+))?/);
      if (variantMatch) {
        const variantName = variantMatch[1];
        const typeName = variantMatch[2] ?? variantName;
        const ordinal = currentUnion.variants.length + 1; // 1-indexed
        currentUnion.variants.push({
          name: variantName,
          type: typeName,
          ordinal,
        });
      }
    }
  }

  function parseEnumBody(line: string): void {
    if (line.includes("}")) {
      const before = line.slice(0, line.indexOf("}"));
      addEnumValues(before);
      finishBlock();
      return;
    }
    addEnumValues(line);
  }

  function addEnumValues(text: string): void {
    if (!currentEnum) return;
    const parts = text.split(",");
    for (const part of parts) {
      const trimmed = part.trim();
      if (!trimmed) continue;

      const valueMatch = trimmed.match(/^(\w+)(?:\s*=\s*(\d+))?/);
      if (valueMatch) {
        const name = valueMatch[1];
        const explicitValue = valueMatch[2] !== undefined
          ? parseInt(valueMatch[2], 10)
          : currentEnum.nextValue;
        currentEnum.values.push({ name, value: explicitValue });
        currentEnum.nextValue = explicitValue + 1;
      }
    }
  }

  function finishBlock(): void {
    switch (state) {
      case "table":
        if (currentTable) {
          schema.tables[currentTable.name] = currentTable;
          currentTable = null;
        }
        break;
      case "struct":
        finalizeStruct();
        break;
      case "union":
        if (currentUnion) {
          schema.unions[currentUnion.name] = currentUnion;
          currentUnion = null;
        }
        break;
      case "enum":
        if (currentEnum) {
          schema.enums[currentEnum.name] = {
            name: currentEnum.name,
            underlyingType: currentEnum.underlyingType,
            values: currentEnum.values,
          };
          currentEnum = null;
        }
        break;
    }
    state = "top";
  }

  function finalizeStruct(): void {
    if (!currentStruct) return;
    const size = computeStructSize(currentStruct.fields, schema.structs);
    schema.structs[currentStruct.name] = {
      name: currentStruct.name,
      fields: currentStruct.fields,
      size,
    };
    currentStruct = null;
  }
}

function parseUnionVariantsInline(text: string, variants: string[]): void {
  const parts = text.split(",");
  for (const part of parts) {
    const trimmed = part.trim();
    if (!trimmed) continue;
    const m = trimmed.match(/^(\w+)(?:\s*:\s*(\w+))?/);
    if (m) variants.push(m[1]);
  }
}

function stripComment(line: string): string {
  // Remove // comments, but not inside strings (no strings in .fbs field decls)
  const idx = line.indexOf("//");
  return idx >= 0 ? line.slice(0, idx) : line;
}

// ── Struct size computation ──

function scalarSize(type: string): number {
  return SCALAR_SIZES[type] ?? 0;
}

function resolveFieldLayout(
  type: string,
  knownStructs: Record<string, FbsStruct>,
): { size: number; alignment: number } {
  // Fixed-size array: [type:count]
  const arrayMatch = type.match(/^\[(\w+):(\d+)\]$/);
  if (arrayMatch) {
    const elemType = arrayMatch[1];
    const count = parseInt(arrayMatch[2], 10);
    const elemSize = scalarSize(elemType);
    if (elemSize > 0) {
      return { size: elemSize * count, alignment: elemSize };
    }
    // Nested struct array
    const nested = knownStructs[elemType];
    if (nested) {
      const nestedAlign = structAlignment(nested, knownStructs);
      return { size: nested.size * count, alignment: nestedAlign };
    }
  }

  // Scalar
  const sz = scalarSize(type);
  if (sz > 0) return { size: sz, alignment: sz };

  // Nested struct
  const nested = knownStructs[type];
  if (nested) {
    return { size: nested.size, alignment: structAlignment(nested, knownStructs) };
  }

  throw new Error(`Unknown struct field type: ${type}`);
}

function structAlignment(
  s: FbsStruct,
  knownStructs?: Record<string, FbsStruct>,
): number {
  // Alignment of a struct = max alignment of its fields (recursive for nested structs)
  let maxAlign = 1;
  for (const f of s.fields) {
    const sz = scalarSize(f.type);
    if (sz > 0) {
      if (sz > maxAlign) maxAlign = sz;
      continue;
    }

    // Fixed-size array: [type:count] — alignment is the element alignment
    const arrayMatch = f.type.match(/^\[(\w+):(\d+)\]$/);
    if (arrayMatch) {
      const elemSize = scalarSize(arrayMatch[1]);
      if (elemSize > 0) {
        if (elemSize > maxAlign) maxAlign = elemSize;
      } else if (knownStructs) {
        const nested = knownStructs[arrayMatch[1]];
        if (nested) {
          const nestedAlign = structAlignment(nested, knownStructs);
          if (nestedAlign > maxAlign) maxAlign = nestedAlign;
        }
      }
      continue;
    }

    // Nested struct
    if (knownStructs) {
      const nested = knownStructs[f.type];
      if (nested) {
        const nestedAlign = structAlignment(nested, knownStructs);
        if (nestedAlign > maxAlign) maxAlign = nestedAlign;
      }
    }
  }
  return maxAlign;
}

function computeStructSize(
  fields: FbsStructField[],
  knownStructs: Record<string, FbsStruct>,
): number {
  // FlatBuffer structs use natural alignment: each field is aligned to its
  // type's alignment, and the struct is padded to its max field alignment.
  // This matches the flatc compiler's layout (verified against Rust generated code).
  let offset = 0;
  let maxAlignment = 1;

  for (const field of fields) {
    const layout = resolveFieldLayout(field.type, knownStructs);
    const align = layout.alignment;
    if (align > maxAlignment) maxAlignment = align;
    offset = alignUp(offset, align);
    offset += layout.size;
  }

  // Pad struct to its max alignment
  offset = alignUp(offset, maxAlignment);
  return offset;
}

function alignUp(offset: number, alignment: number): number {
  return Math.ceil(offset / alignment) * alignment;
}

// ── File-based convenience ──

export function parseFbsFile(filePath: string): FbsSchema {
  const source = readFileSync(filePath, "utf-8");
  return parseFbsSource(source);
}

export function parseFbsFiles(filePaths: string[]): FbsSchema {
  const combined = filePaths
    .map((p) => readFileSync(p, "utf-8"))
    .join("\n");
  return parseFbsSource(combined);
}
