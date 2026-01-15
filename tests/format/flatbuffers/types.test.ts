/**
 * Tests for type validation helpers.
 */

import { describe, it, expect } from 'vitest';
import { asObjectId12, asObjectId8 } from '../../../src/format/flatbuffers/types.js';

describe('asObjectId12', () => {
  it('should accept valid 12-byte array', () => {
    const bytes = new Uint8Array(12).fill(0x42);
    const result = asObjectId12(bytes);
    expect(result).toBe(bytes);
    expect(result.length).toBe(12);
  });

  it('should throw for empty array', () => {
    const bytes = new Uint8Array(0);
    expect(() => asObjectId12(bytes)).toThrow('Invalid ObjectId12: expected 12 bytes, got 0');
  });

  it('should throw for too-short array', () => {
    const bytes = new Uint8Array(8);
    expect(() => asObjectId12(bytes)).toThrow('Invalid ObjectId12: expected 12 bytes, got 8');
  });

  it('should throw for too-long array', () => {
    const bytes = new Uint8Array(16);
    expect(() => asObjectId12(bytes)).toThrow('Invalid ObjectId12: expected 12 bytes, got 16');
  });

  it('should throw for 11-byte array (off by one)', () => {
    const bytes = new Uint8Array(11);
    expect(() => asObjectId12(bytes)).toThrow('Invalid ObjectId12: expected 12 bytes, got 11');
  });

  it('should throw for 13-byte array (off by one)', () => {
    const bytes = new Uint8Array(13);
    expect(() => asObjectId12(bytes)).toThrow('Invalid ObjectId12: expected 12 bytes, got 13');
  });
});

describe('asObjectId8', () => {
  it('should accept valid 8-byte array', () => {
    const bytes = new Uint8Array(8).fill(0x42);
    const result = asObjectId8(bytes);
    expect(result).toBe(bytes);
    expect(result.length).toBe(8);
  });

  it('should throw for empty array', () => {
    const bytes = new Uint8Array(0);
    expect(() => asObjectId8(bytes)).toThrow('Invalid ObjectId8: expected 8 bytes, got 0');
  });

  it('should throw for too-short array', () => {
    const bytes = new Uint8Array(4);
    expect(() => asObjectId8(bytes)).toThrow('Invalid ObjectId8: expected 8 bytes, got 4');
  });

  it('should throw for too-long array', () => {
    const bytes = new Uint8Array(12);
    expect(() => asObjectId8(bytes)).toThrow('Invalid ObjectId8: expected 8 bytes, got 12');
  });

  it('should throw for 7-byte array (off by one)', () => {
    const bytes = new Uint8Array(7);
    expect(() => asObjectId8(bytes)).toThrow('Invalid ObjectId8: expected 8 bytes, got 7');
  });

  it('should throw for 9-byte array (off by one)', () => {
    const bytes = new Uint8Array(9);
    expect(() => asObjectId8(bytes)).toThrow('Invalid ObjectId8: expected 8 bytes, got 9');
  });
});
