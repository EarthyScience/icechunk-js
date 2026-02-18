/**
 * Minimal FlatBuffer reader for icechunk formats.
 *
 * FlatBuffers binary format:
 * - Little-endian throughout
 * - Root table offset at position 0 (4 bytes)
 * - Tables have vtables containing field offsets
 * - Strings are length-prefixed UTF-8
 * - Vectors are length-prefixed arrays
 */

export class FlatBufferReader {
  private view: DataView;
  private bytes: Uint8Array;

  constructor(buffer: ArrayBuffer | Uint8Array) {
    if (buffer instanceof Uint8Array) {
      this.bytes = buffer;
      this.view = new DataView(
        buffer.buffer,
        buffer.byteOffset,
        buffer.byteLength,
      );
    } else {
      this.bytes = new Uint8Array(buffer);
      this.view = new DataView(buffer);
    }
  }

  /** Get the root table position */
  getRootTableOffset(): number {
    return this.readUint32(0);
  }

  /** Read a uint8 at absolute position */
  readUint8(pos: number): number {
    return this.view.getUint8(pos);
  }

  /** Read a uint16 at absolute position (little-endian) */
  readUint16(pos: number): number {
    return this.view.getUint16(pos, true);
  }

  /** Read a uint32 at absolute position (little-endian) */
  readUint32(pos: number): number {
    return this.view.getUint32(pos, true);
  }

  /** Read a uint64 at absolute position (little-endian) */
  readUint64(pos: number): bigint {
    return this.view.getBigUint64(pos, true);
  }

  /** Read raw bytes at absolute position */
  readBytes(pos: number, length: number): Uint8Array {
    return this.bytes.slice(pos, pos + length);
  }

  /** Read a string (length-prefixed UTF-8) at the given offset position */
  readString(offsetPos: number): string | null {
    const stringOffset = this.readUint32(offsetPos);
    if (stringOffset === 0) return null;

    const stringPos = offsetPos + stringOffset;
    const length = this.readUint32(stringPos);
    const stringBytes = this.bytes.slice(stringPos + 4, stringPos + 4 + length);
    return new TextDecoder().decode(stringBytes);
  }

  /** Read a vector length at the given offset position */
  readVectorLength(offsetPos: number): number {
    const vectorOffset = this.readUint32(offsetPos);
    if (vectorOffset === 0) return 0;

    const vectorPos = offsetPos + vectorOffset;
    return this.readUint32(vectorPos);
  }

  /** Get vector element position */
  getVectorElementPos(
    offsetPos: number,
    index: number,
    elementSize: number,
  ): number {
    const vectorOffset = this.readUint32(offsetPos);
    const vectorPos = offsetPos + vectorOffset;
    // Skip length (4 bytes) then index into elements
    return vectorPos + 4 + index * elementSize;
  }

  /** Read a byte vector at the given offset position */
  readByteVector(offsetPos: number): Uint8Array | null {
    const vectorOffset = this.readUint32(offsetPos);
    if (vectorOffset === 0) return null;

    const vectorPos = offsetPos + vectorOffset;
    const length = this.readUint32(vectorPos);
    return this.bytes.slice(vectorPos + 4, vectorPos + 4 + length);
  }

  /** Read a uint32 vector at the given offset position */
  readUint32Vector(offsetPos: number): number[] | null {
    const vectorOffset = this.readUint32(offsetPos);
    if (vectorOffset === 0) return null;

    const vectorPos = offsetPos + vectorOffset;
    const length = this.readUint32(vectorPos);
    const result: number[] = [];
    for (let i = 0; i < length; i++) {
      result.push(this.readUint32(vectorPos + 4 + i * 4));
    }
    return result;
  }
}

/**
 * Table reader - handles vtable lookups for table fields
 */
export class TableReader {
  private fb: FlatBufferReader;
  private tablePos: number;
  private vtablePos: number;
  private vtableSize: number;

  constructor(fb: FlatBufferReader, tablePos: number) {
    this.fb = fb;
    this.tablePos = tablePos;

    // vtable offset is a signed offset from table position
    const vtableOffset = fb.readUint32(tablePos);
    // vtable is at tablePos - vtableOffset (it's stored as negative offset)
    this.vtablePos = tablePos - this.toSigned32(vtableOffset);
    this.vtableSize = fb.readUint16(this.vtablePos);
  }

  private toSigned32(value: number): number {
    return value | 0;
  }

  /** Get the field offset from vtable, returns 0 if field not present */
  getFieldOffset(fieldIndex: number): number {
    const vtableFieldPos = 4 + fieldIndex * 2; // Skip vtable size (2) + table size (2)
    if (vtableFieldPos >= this.vtableSize) {
      return 0; // Field not in vtable
    }
    return this.fb.readUint16(this.vtablePos + vtableFieldPos);
  }

  /** Get absolute position of a field, or null if not present */
  getFieldPos(fieldIndex: number): number | null {
    const offset = this.getFieldOffset(fieldIndex);
    if (offset === 0) return null;
    return this.tablePos + offset;
  }

  /** Read a uint8 field */
  readUint8(fieldIndex: number, defaultValue: number = 0): number {
    const pos = this.getFieldPos(fieldIndex);
    if (pos === null) return defaultValue;
    return this.fb.readUint8(pos);
  }

  /** Read a uint16 field */
  readUint16(fieldIndex: number, defaultValue: number = 0): number {
    const pos = this.getFieldPos(fieldIndex);
    if (pos === null) return defaultValue;
    return this.fb.readUint16(pos);
  }

  /** Read a uint32 field */
  readUint32(fieldIndex: number, defaultValue: number = 0): number {
    const pos = this.getFieldPos(fieldIndex);
    if (pos === null) return defaultValue;
    return this.fb.readUint32(pos);
  }

  /** Read a uint64 field */
  readUint64(fieldIndex: number, defaultValue: bigint = 0n): bigint {
    const pos = this.getFieldPos(fieldIndex);
    if (pos === null) return defaultValue;
    return this.fb.readUint64(pos);
  }

  /** Read a string field */
  readString(fieldIndex: number): string | null {
    const pos = this.getFieldPos(fieldIndex);
    if (pos === null) return null;
    return this.fb.readString(pos);
  }

  /** Read a byte vector field */
  readByteVector(fieldIndex: number): Uint8Array | null {
    const pos = this.getFieldPos(fieldIndex);
    if (pos === null) return null;
    return this.fb.readByteVector(pos);
  }

  /** Read a uint32 vector field */
  readUint32Vector(fieldIndex: number): number[] | null {
    const pos = this.getFieldPos(fieldIndex);
    if (pos === null) return null;
    return this.fb.readUint32Vector(pos);
  }

  /** Get vector length for a field */
  getVectorLength(fieldIndex: number): number {
    const pos = this.getFieldPos(fieldIndex);
    if (pos === null) return 0;
    return this.fb.readVectorLength(pos);
  }

  /** Get a nested table at a field */
  getNestedTable(fieldIndex: number): TableReader | null {
    const pos = this.getFieldPos(fieldIndex);
    if (pos === null) return null;

    const tableOffset = this.fb.readUint32(pos);
    if (tableOffset === 0) return null;

    return new TableReader(this.fb, pos + tableOffset);
  }

  /** Get a table from a vector */
  getVectorTable(fieldIndex: number, index: number): TableReader | null {
    const pos = this.getFieldPos(fieldIndex);
    if (pos === null) return null;

    const vectorOffset = this.fb.readUint32(pos);
    if (vectorOffset === 0) return null;

    const vectorPos = pos + vectorOffset;
    const length = this.fb.readUint32(vectorPos);
    if (index >= length) return null;

    // Vector of tables: each element is an offset to a table
    const elementPos = vectorPos + 4 + index * 4;
    const tableOffset = this.fb.readUint32(elementPos);

    return new TableReader(this.fb, elementPos + tableOffset);
  }

  /** Read a fixed-size struct inline (not via offset) */
  readInlineStruct(fieldIndex: number, size: number): Uint8Array | null {
    const pos = this.getFieldPos(fieldIndex);
    if (pos === null) return null;
    return this.fb.readBytes(pos, size);
  }

  /** Read a struct from a vector (inline structs) */
  readVectorStruct(
    fieldIndex: number,
    index: number,
    structSize: number,
  ): Uint8Array | null {
    const pos = this.getFieldPos(fieldIndex);
    if (pos === null) return null;

    const vectorOffset = this.fb.readUint32(pos);
    if (vectorOffset === 0) return null;

    const vectorPos = pos + vectorOffset;
    const length = this.fb.readUint32(vectorPos);
    if (index >= length) return null;

    const elementPos = vectorPos + 4 + index * structSize;
    return this.fb.readBytes(elementPos, structSize);
  }

  /** Get underlying FlatBufferReader */
  getFlatBufferReader(): FlatBufferReader {
    return this.fb;
  }

  /** Get table position */
  getTablePos(): number {
    return this.tablePos;
  }
}

/** Parse a FlatBuffer and return the root table */
export function parseRootTable(buffer: ArrayBuffer | Uint8Array): TableReader {
  const fb = new FlatBufferReader(buffer);
  const rootOffset = fb.getRootTableOffset();
  return new TableReader(fb, rootOffset);
}
