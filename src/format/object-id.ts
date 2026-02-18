/**
 * Object ID encoding/decoding using Base32 Crockford.
 *
 * Icechunk uses Base32 Crockford for encoding object IDs:
 * - 12-byte IDs (SnapshotId, ManifestId, ChunkId) → 20 characters
 * - 8-byte IDs (NodeId) → 13 characters
 *
 * Crockford Base32 alphabet: 0123456789ABCDEFGHJKMNPQRSTVWXYZ
 * (excludes I, L, O, U to avoid confusion)
 */

/** Crockford Base32 encoding alphabet */
const ENCODE_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/** Crockford Base32 decoding map (handles lowercase and common substitutions) */
const DECODE_MAP: Record<string, number> = {};

// Build decode map
for (let i = 0; i < ENCODE_ALPHABET.length; i++) {
  const char = ENCODE_ALPHABET[i];
  DECODE_MAP[char] = i;
  DECODE_MAP[char.toLowerCase()] = i;
}

// Handle common substitutions
DECODE_MAP["I"] = DECODE_MAP["i"] = 1; // I → 1
DECODE_MAP["L"] = DECODE_MAP["l"] = 1; // L → 1
DECODE_MAP["O"] = DECODE_MAP["o"] = 0; // O → 0

/**
 * Encode bytes to Base32 Crockford string.
 *
 * @param bytes - Bytes to encode
 * @returns Base32 Crockford encoded string (uppercase)
 */
export function encodeBase32(bytes: Uint8Array): string {
  if (bytes.length === 0) return "";

  let result = "";
  let buffer = 0;
  let bitsLeft = 0;

  for (const byte of bytes) {
    buffer = (buffer << 8) | byte;
    bitsLeft += 8;

    while (bitsLeft >= 5) {
      bitsLeft -= 5;
      const index = (buffer >> bitsLeft) & 0x1f;
      result += ENCODE_ALPHABET[index];
    }
  }

  // Handle remaining bits
  if (bitsLeft > 0) {
    const index = (buffer << (5 - bitsLeft)) & 0x1f;
    result += ENCODE_ALPHABET[index];
  }

  return result;
}

/**
 * Decode Base32 Crockford string to bytes.
 *
 * @param str - Base32 Crockford encoded string
 * @returns Decoded bytes
 * @throws Error if the string contains invalid characters
 */
export function decodeBase32(str: string): Uint8Array {
  if (str.length === 0) return new Uint8Array(0);

  // Remove hyphens (Crockford allows hyphens for readability)
  str = str.replace(/-/g, "");

  const result: number[] = [];
  let buffer = 0;
  let bitsLeft = 0;

  for (const char of str) {
    const value = DECODE_MAP[char];
    if (value === undefined) {
      throw new Error(`Invalid Base32 Crockford character: ${char}`);
    }

    buffer = (buffer << 5) | value;
    bitsLeft += 5;

    if (bitsLeft >= 8) {
      bitsLeft -= 8;
      result.push((buffer >> bitsLeft) & 0xff);
    }
  }

  return new Uint8Array(result);
}

/**
 * Encode a 12-byte object ID to string.
 *
 * @param id - 12-byte ID
 * @returns Base32 Crockford encoded string (20 characters)
 */
export function encodeObjectId12(id: Uint8Array): string {
  if (id.length !== 12) {
    throw new Error(`Expected 12 bytes, got ${id.length}`);
  }
  return encodeBase32(id);
}

/**
 * Decode a string to 12-byte object ID.
 *
 * @param str - Base32 Crockford encoded string
 * @returns 12-byte ID
 */
export function decodeObjectId12(str: string): Uint8Array {
  const bytes = decodeBase32(str);
  if (bytes.length !== 12) {
    throw new Error(`Expected 12 bytes after decoding, got ${bytes.length}`);
  }
  return bytes;
}

/**
 * Encode an 8-byte object ID to string.
 *
 * @param id - 8-byte ID
 * @returns Base32 Crockford encoded string (13 characters)
 */
export function encodeObjectId8(id: Uint8Array): string {
  if (id.length !== 8) {
    throw new Error(`Expected 8 bytes, got ${id.length}`);
  }
  return encodeBase32(id);
}

/**
 * Decode a string to 8-byte object ID.
 *
 * @param str - Base32 Crockford encoded string
 * @returns 8-byte ID
 */
export function decodeObjectId8(str: string): Uint8Array {
  const bytes = decodeBase32(str);
  if (bytes.length !== 8) {
    throw new Error(`Expected 8 bytes after decoding, got ${bytes.length}`);
  }
  return bytes;
}
