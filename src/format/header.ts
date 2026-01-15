/**
 * Icechunk binary file header parsing.
 *
 * Header format (39 bytes total):
 * - Bytes 0-11:  Magic bytes "ICE🧊CHUNK" (12 bytes)
 * - Bytes 12-35: Implementation name (24 bytes, space-padded)
 * - Byte 36:     Spec version (1 = v1.0, 2 = v2.0)
 * - Byte 37:     File type (1-6)
 * - Byte 38:     Compression algorithm (0 = none, 1 = zstd)
 */

/** Header size in bytes */
export const HEADER_SIZE = 39;

/** Magic bytes for icechunk format: "ICE🧊CHUNK" */
export const MAGIC_BYTES = new Uint8Array([
  0x49, 0x43, 0x45, // ICE
  0xf0, 0x9f, 0xa7, 0x8a, // 🧊 (U+1F9CA in UTF-8)
  0x43, 0x48, 0x55, 0x4e, 0x4b, // CHUNK
]);

/** Spec version enum */
export enum SpecVersion {
  V1_0 = 1,
  V2_0 = 2,
}

/** File type enum */
export enum FileType {
  Snapshot = 1,
  Manifest = 2,
  Attributes = 3,
  TransactionLog = 4,
  Chunk = 5,
  RepoInfo = 6,
}

/** Compression algorithm enum */
export enum CompressionAlgorithm {
  None = 0,
  Zstd = 1,
}

/** Parsed header structure */
export interface IcechunkHeader {
  /** Implementation name (e.g., "ic-1.0.0") */
  implementation: string;

  /** Spec version */
  specVersion: SpecVersion;

  /** File type */
  fileType: FileType;

  /** Compression algorithm */
  compression: CompressionAlgorithm;
}

/** Error thrown when header parsing fails */
export class HeaderParseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'HeaderParseError';
  }
}

/**
 * Parse the header from a buffer.
 *
 * @param data - Buffer containing at least 39 bytes
 * @returns Parsed header
 * @throws HeaderParseError if the header is invalid
 */
export function parseHeader(data: Uint8Array): IcechunkHeader {
  if (data.length < HEADER_SIZE) {
    throw new HeaderParseError(
      `Buffer too small: expected at least ${HEADER_SIZE} bytes, got ${data.length}`
    );
  }

  // Check magic bytes
  const magic = data.slice(0, 12);
  if (!compareMagic(magic, MAGIC_BYTES)) {
    throw new HeaderParseError('Invalid magic bytes: not an icechunk file');
  }

  // Parse implementation name (bytes 12-35)
  const implementationBytes = data.slice(12, 36);
  const implementation = new TextDecoder()
    .decode(implementationBytes)
    .trimEnd();

  // Parse spec version (byte 36)
  const specVersionByte = data[36];
  if (specVersionByte !== SpecVersion.V1_0 && specVersionByte !== SpecVersion.V2_0) {
    throw new HeaderParseError(`Invalid spec version: ${specVersionByte}`);
  }
  const specVersion = specVersionByte as SpecVersion;

  // Parse file type (byte 37)
  const fileTypeByte = data[37];
  if (fileTypeByte < 1 || fileTypeByte > 6) {
    throw new HeaderParseError(`Invalid file type: ${fileTypeByte}`);
  }
  const fileType = fileTypeByte as FileType;

  // Parse compression (byte 38)
  const compressionByte = data[38];
  if (compressionByte !== CompressionAlgorithm.None && compressionByte !== CompressionAlgorithm.Zstd) {
    throw new HeaderParseError(`Invalid compression algorithm: ${compressionByte}`);
  }
  const compression = compressionByte as CompressionAlgorithm;

  return {
    implementation,
    specVersion,
    fileType,
    compression,
  };
}

/**
 * Validate that a header matches an expected file type.
 *
 * @param header - Parsed header
 * @param expectedType - Expected file type
 * @throws HeaderParseError if the file type doesn't match
 */
export function validateFileType(
  header: IcechunkHeader,
  expectedType: FileType
): void {
  if (header.fileType !== expectedType) {
    const expected = FileType[expectedType];
    const actual = FileType[header.fileType];
    throw new HeaderParseError(
      `Invalid file type: expected ${expected}, got ${actual}`
    );
  }
}

/**
 * Get the data portion of a buffer (after the header).
 *
 * @param data - Full buffer including header
 * @returns Data portion after the header
 */
export function getDataAfterHeader(data: Uint8Array): Uint8Array {
  return data.slice(HEADER_SIZE);
}

/** Compare two byte arrays */
function compareMagic(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}
