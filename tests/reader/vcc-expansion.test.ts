import { describe, it, expect } from "vitest";
import { expandVccUrl } from "../../src/reader/session.js";

describe("expandVccUrl", () => {
  const containers = new Map<string, string>([
    ["my-data", "s3://mybucket/some/prefix/"],
    ["gcs-data", "gs://other/"],
    ["no-slash", "s3://bucket/key"],
  ]);

  it("passes through absolute s3:// URLs unchanged", () => {
    expect(expandVccUrl("s3://bucket/key", containers)).toBe("s3://bucket/key");
  });

  it("passes through https:// URLs unchanged", () => {
    expect(expandVccUrl("https://example.com/x.nc", containers)).toBe(
      "https://example.com/x.nc",
    );
  });

  it("expands vcc://name/path to url_prefix + path", () => {
    expect(expandVccUrl("vcc://my-data/chunks/abc.nc", containers)).toBe(
      "s3://mybucket/some/prefix/chunks/abc.nc",
    );
  });

  it("concatenates literally when url_prefix lacks trailing slash", () => {
    // Matches the upstream Rust behavior: no normalization — prefix and
    // relative path are concatenated as-is.
    expect(expandVccUrl("vcc://no-slash/tail", containers)).toBe(
      "s3://bucket/keytail",
    );
  });

  it("handles relative paths with nested slashes", () => {
    expect(expandVccUrl("vcc://gcs-data/a/b/c", containers)).toBe(
      "gs://other/a/b/c",
    );
  });

  it("allows an empty relative path", () => {
    expect(expandVccUrl("vcc://my-data/", containers)).toBe(
      "s3://mybucket/some/prefix/",
    );
  });

  it("throws on unknown container name when the map is populated", () => {
    expect(() => expandVccUrl("vcc://missing/x", containers)).toThrow(
      /Unknown virtual chunk container "missing"/,
    );
  });

  it("throws when the vcc:// URL has no slash after the name", () => {
    expect(() => expandVccUrl("vcc://my-data", containers)).toThrow(
      /missing "\/" after container name/,
    );
  });

  it("passes vcc:// through unchanged when the container map is empty", () => {
    // Preserves pre-patch behavior for callers using ReadSession.open()
    // directly with a fetchClient that resolves vcc:// itself.
    expect(expandVccUrl("vcc://anything/x", new Map())).toBe(
      "vcc://anything/x",
    );
  });
});
