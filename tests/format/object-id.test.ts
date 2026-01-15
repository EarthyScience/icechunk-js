import { describe, it, expect } from 'vitest';
import {
  encodeBase32,
  decodeBase32,
  encodeObjectId12,
  decodeObjectId12,
  encodeObjectId8,
  decodeObjectId8,
} from '../../src/format/object-id.js';

describe('encodeBase32', () => {
  it('should encode empty array to empty string', () => {
    expect(encodeBase32(new Uint8Array([]))).toBe('');
  });

  it('should encode single byte', () => {
    // 0x00 = 00000 000 → "00" (5 bits + 3 bits padded)
    expect(encodeBase32(new Uint8Array([0x00]))).toBe('00');
  });

  it('should encode known byte sequences', () => {
    // "Hello" in ASCII = [72, 101, 108, 108, 111]
    const hello = new TextEncoder().encode('Hello');
    const encoded = encodeBase32(hello);
    expect(encoded).toBe('91JPRV3F');
  });

  it('should produce uppercase output', () => {
    const result = encodeBase32(new Uint8Array([0x12, 0x34, 0x56]));
    expect(result).toBe(result.toUpperCase());
  });

  it('should only use Crockford alphabet characters', () => {
    const crockfordChars = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
    for (let i = 0; i < 256; i++) {
      const encoded = encodeBase32(new Uint8Array([i]));
      for (const char of encoded) {
        expect(crockfordChars).toContain(char);
      }
    }
  });
});

describe('decodeBase32', () => {
  it('should decode empty string to empty array', () => {
    expect(decodeBase32('')).toEqual(new Uint8Array([]));
  });

  it('should decode known string', () => {
    const decoded = decodeBase32('91JPRV3F');
    const text = new TextDecoder().decode(decoded);
    expect(text).toBe('Hello');
  });

  it('should handle lowercase input', () => {
    const upper = decodeBase32('91JPRV3F');
    const lower = decodeBase32('91jprv3f');
    expect(lower).toEqual(upper);
  });

  it.each([
    ['I', '1'],
    ['i', '1'],
    ['L', '1'],
    ['l', '1'],
    ['O', '0'],
    ['o', '0'],
  ])('should handle common substitution %s→%s', (input, canonical) => {
    expect(decodeBase32(input)).toEqual(decodeBase32(canonical));
  });

  it('should handle hyphens for readability', () => {
    const withHyphens = decodeBase32('91JP-RV3F');
    const withoutHyphens = decodeBase32('91JPRV3F');
    expect(withHyphens).toEqual(withoutHyphens);
  });

  it('should throw on invalid character U', () => {
    expect(() => decodeBase32('U')).toThrow('Invalid Base32 Crockford character: U');
  });

  it('should throw on invalid special characters', () => {
    expect(() => decodeBase32('ABC@DEF')).toThrow('Invalid Base32 Crockford character');
  });
});

describe('encodeBase32/decodeBase32 roundtrip', () => {
  it('should roundtrip random bytes', () => {
    const testCases = [
      new Uint8Array([1, 2, 3, 4, 5]),
      new Uint8Array([255, 254, 253, 252]),
      new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0]),
      new Uint8Array([255, 255, 255, 255, 255]),
    ];

    for (const original of testCases) {
      const encoded = encodeBase32(original);
      const decoded = decodeBase32(encoded);
      expect(decoded).toEqual(original);
    }
  });
});

describe('encodeObjectId12', () => {
  it('should encode 12-byte ID', () => {
    const id = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
    const encoded = encodeObjectId12(id);
    expect(encoded.length).toBe(20); // 12 bytes * 8 bits / 5 bits = 19.2 → 20 chars
  });

  it('should throw for wrong byte length', () => {
    expect(() => encodeObjectId12(new Uint8Array([1, 2, 3]))).toThrow(
      'Expected 12 bytes'
    );
    expect(() => encodeObjectId12(new Uint8Array(15))).toThrow('Expected 12 bytes');
  });
});

describe('decodeObjectId12', () => {
  it('should decode to 12-byte ID', () => {
    const original = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
    const encoded = encodeObjectId12(original);
    const decoded = decodeObjectId12(encoded);
    expect(decoded).toEqual(original);
  });

  it('should throw for wrong decoded length', () => {
    // Encode 8 bytes instead of 12
    const wrongSize = encodeBase32(new Uint8Array(8));
    expect(() => decodeObjectId12(wrongSize)).toThrow('Expected 12 bytes');
  });
});

describe('encodeObjectId8', () => {
  it('should encode 8-byte ID', () => {
    const id = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
    const encoded = encodeObjectId8(id);
    expect(encoded.length).toBe(13); // 8 bytes * 8 bits / 5 bits = 12.8 → 13 chars
  });

  it('should throw for wrong byte length', () => {
    expect(() => encodeObjectId8(new Uint8Array([1, 2, 3]))).toThrow(
      'Expected 8 bytes'
    );
  });
});

describe('decodeObjectId8', () => {
  it('should decode to 8-byte ID', () => {
    const original = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
    const encoded = encodeObjectId8(original);
    const decoded = decodeObjectId8(encoded);
    expect(decoded).toEqual(original);
  });

  it('should throw for wrong decoded length', () => {
    const wrongSize = encodeBase32(new Uint8Array(12));
    expect(() => decodeObjectId8(wrongSize)).toThrow('Expected 8 bytes');
  });
});
