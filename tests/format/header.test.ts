import { describe, it, expect } from "vitest";
import {
  parseHeader,
  validateFileType,
  getDataAfterHeader,
  HEADER_SIZE,
  MAGIC_BYTES,
  SpecVersion,
  FileType,
  CompressionAlgorithm,
  HeaderParseError,
  type IcechunkHeader,
} from "../../src/format/header.js";

/** Helper to create a valid header buffer */
function createHeader(options: {
  implementation?: string;
  specVersion?: number;
  fileType?: number;
  compression?: number;
}): Uint8Array {
  const buffer = new Uint8Array(HEADER_SIZE);

  // Magic bytes: "ICE🧊CHUNK"
  buffer.set(MAGIC_BYTES, 0);

  // Implementation (24 bytes, space-padded)
  const impl = (options.implementation ?? "test-impl").padEnd(24, " ");
  buffer.set(new TextEncoder().encode(impl), 12);

  // Spec version, file type, compression
  buffer[36] = options.specVersion ?? SpecVersion.V1_0;
  buffer[37] = options.fileType ?? FileType.Snapshot;
  buffer[38] = options.compression ?? CompressionAlgorithm.None;

  return buffer;
}

describe("parseHeader", () => {
  it("should parse a valid header", () => {
    const data = createHeader({ implementation: "ic-1.0.0" });
    const header = parseHeader(data);

    expect(header.implementation).toBe("ic-1.0.0");
    expect(header.specVersion).toBe(SpecVersion.V1_0);
    expect(header.fileType).toBe(FileType.Snapshot);
    expect(header.compression).toBe(CompressionAlgorithm.None);
  });

  it("should parse spec version V2_0", () => {
    const data = createHeader({ specVersion: SpecVersion.V2_0 });
    const header = parseHeader(data);
    expect(header.specVersion).toBe(SpecVersion.V2_0);
  });

  it.each([
    ["Snapshot", FileType.Snapshot],
    ["Manifest", FileType.Manifest],
    ["Attributes", FileType.Attributes],
    ["TransactionLog", FileType.TransactionLog],
    ["Chunk", FileType.Chunk],
    ["RepoInfo", FileType.RepoInfo],
  ])("should parse file type %s", (_name, fileType) => {
    const data = createHeader({ fileType });
    const header = parseHeader(data);
    expect(header.fileType).toBe(fileType);
  });

  it("should parse Zstd compression", () => {
    const data = createHeader({ compression: CompressionAlgorithm.Zstd });
    const header = parseHeader(data);
    expect(header.compression).toBe(CompressionAlgorithm.Zstd);
  });

  it("should trim trailing spaces from implementation", () => {
    const data = createHeader({ implementation: "ic-1.0.0" });
    const header = parseHeader(data);
    expect(header.implementation).toBe("ic-1.0.0");
    expect(header.implementation.endsWith(" ")).toBe(false);
  });

  it("should throw on buffer too small", () => {
    const smallBuffer = new Uint8Array(10);
    expect(() => parseHeader(smallBuffer)).toThrow(HeaderParseError);
    expect(() => parseHeader(smallBuffer)).toThrow("Buffer too small");
  });

  it("should throw on invalid magic bytes", () => {
    const buffer = new Uint8Array(HEADER_SIZE);
    buffer.fill(0x00);
    expect(() => parseHeader(buffer)).toThrow(HeaderParseError);
    expect(() => parseHeader(buffer)).toThrow("Invalid magic bytes");
  });

  it("should throw on invalid spec version", () => {
    const data = createHeader({ specVersion: 99 });
    expect(() => parseHeader(data)).toThrow(HeaderParseError);
    expect(() => parseHeader(data)).toThrow("Invalid spec version: 99");
  });

  it.each([0, 7, 255])("should throw on invalid file type %d", (fileType) => {
    const data = createHeader({ fileType });
    expect(() => parseHeader(data)).toThrow(HeaderParseError);
    expect(() => parseHeader(data)).toThrow(`Invalid file type: ${fileType}`);
  });

  it("should throw on invalid compression", () => {
    const data = createHeader({ compression: 99 });
    expect(() => parseHeader(data)).toThrow(HeaderParseError);
    expect(() => parseHeader(data)).toThrow(
      "Invalid compression algorithm: 99",
    );
  });

  it("should accept buffer larger than header size", () => {
    const largeBuffer = new Uint8Array(100);
    largeBuffer.set(createHeader({}), 0);
    const header = parseHeader(largeBuffer);
    expect(header.implementation).toBeDefined();
  });
});

describe("validateFileType", () => {
  it("should not throw when types match", () => {
    const header: IcechunkHeader = {
      implementation: "test",
      specVersion: SpecVersion.V1_0,
      fileType: FileType.Snapshot,
      compression: CompressionAlgorithm.None,
    };

    expect(() => validateFileType(header, FileType.Snapshot)).not.toThrow();
  });

  it("should throw when types mismatch", () => {
    const header: IcechunkHeader = {
      implementation: "test",
      specVersion: SpecVersion.V1_0,
      fileType: FileType.Manifest,
      compression: CompressionAlgorithm.None,
    };

    expect(() => validateFileType(header, FileType.Snapshot)).toThrow(
      HeaderParseError,
    );
    expect(() => validateFileType(header, FileType.Snapshot)).toThrow(
      "expected Snapshot, got Manifest",
    );
  });
});

describe("getDataAfterHeader", () => {
  it("should return bytes after header", () => {
    const buffer = new Uint8Array(50);
    buffer.fill(0xab, HEADER_SIZE);
    const data = getDataAfterHeader(buffer);

    expect(data.length).toBe(50 - HEADER_SIZE);
    expect(data.every((b) => b === 0xab)).toBe(true);
  });

  it("should return empty array for exact header size", () => {
    const buffer = new Uint8Array(HEADER_SIZE);
    const data = getDataAfterHeader(buffer);
    expect(data.length).toBe(0);
  });
});
