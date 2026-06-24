import { describe, it, expect } from "vitest";
import { resolveVirtualChunkLocation } from "../../src/reader/session.js";
import type { VirtualChunkContainer } from "../../src/format/flatbuffers/repo-parser.js";

describe("resolveVirtualChunkLocation", () => {
  const containers: VirtualChunkContainer[] = [
    {
      name: "my-data",
      urlPrefix: "s3://mybucket/some/prefix/",
      s3: { region: "us-west-2" },
    },
    { name: "gcs-data", urlPrefix: "gs://other/" },
    { name: "no-slash", urlPrefix: "s3://bucket/key" },
    { name: "migrated-s3", urlPrefix: "s3://testbucket" },
    // Unnamed container — matched by url_prefix only, never by vcc:// name.
    {
      name: null,
      urlPrefix: "s3://us-west-2.example/",
      s3: { region: "us-west-2" },
    },
  ];

  it("passes through unmatched absolute s3:// URLs with no container", () => {
    const r = resolveVirtualChunkLocation("s3://unmatched/key", containers);
    expect(r).toEqual({ url: "s3://unmatched/key" });
  });

  it("matches an absolute s3:// URL to its container by url_prefix", () => {
    const r = resolveVirtualChunkLocation(
      "s3://us-west-2.example/data/chunk.bin",
      containers,
    );
    expect(r.url).toBe("s3://us-west-2.example/data/chunk.bin");
    expect(r.container?.name).toBeNull();
    expect(r.container?.s3).toEqual({ region: "us-west-2" });
  });

  it("requires a path boundary, so a non-slash prefix can't claim a sibling bucket", () => {
    const legacy: VirtualChunkContainer[] = [
      { name: null, urlPrefix: "s3://testbucket", s3: { region: "us-east-1" } },
    ];
    // Sibling bucket must NOT match the boundary-less prefix.
    expect(
      resolveVirtualChunkLocation("s3://testbucket2/key", legacy).container,
    ).toBeUndefined();
    // The real bucket still matches (prefix is normalized to `s3://testbucket/`).
    expect(
      resolveVirtualChunkLocation("s3://testbucket/key", legacy).container?.s3,
    ).toEqual({ region: "us-east-1" });
  });

  it("prefers the most specific (longest) url_prefix match", () => {
    const overlapping: VirtualChunkContainer[] = [
      { name: null, urlPrefix: "s3://b/", s3: { region: "us-east-1" } },
      { name: null, urlPrefix: "s3://b/deep/", s3: { region: "eu-west-1" } },
    ].sort((a, b) => b.urlPrefix.length - a.urlPrefix.length);
    const r = resolveVirtualChunkLocation("s3://b/deep/x", overlapping);
    expect(r.container?.s3).toEqual({ region: "eu-west-1" });
  });

  it("passes through https:// URLs unchanged", () => {
    expect(
      resolveVirtualChunkLocation("https://example.com/x.nc", containers).url,
    ).toBe("https://example.com/x.nc");
  });

  it("expands vcc://name/path to url_prefix + path and returns the container", () => {
    const r = resolveVirtualChunkLocation(
      "vcc://my-data/chunks/abc.nc",
      containers,
    );
    expect(r.url).toBe("s3://mybucket/some/prefix/chunks/abc.nc");
    expect(r.container?.s3).toEqual({ region: "us-west-2" });
  });

  it("normalizes url_prefix values that lack a trailing slash", () => {
    expect(
      resolveVirtualChunkLocation("vcc://no-slash/tail", containers).url,
    ).toBe("s3://bucket/key/tail");
  });

  it("normalizes migrated VCC prefixes before expansion", () => {
    expect(
      resolveVirtualChunkLocation("vcc://migrated-s3/path/to/chunk", containers)
        .url,
    ).toBe("s3://testbucket/path/to/chunk");
  });

  it("handles relative paths with nested slashes", () => {
    expect(
      resolveVirtualChunkLocation("vcc://gcs-data/a/b/c", containers).url,
    ).toBe("gs://other/a/b/c");
  });

  it("allows an empty relative path", () => {
    expect(resolveVirtualChunkLocation("vcc://my-data/", containers).url).toBe(
      "s3://mybucket/some/prefix/",
    );
  });

  it("throws on unknown container name when the list is populated", () => {
    expect(() =>
      resolveVirtualChunkLocation("vcc://missing/x", containers),
    ).toThrow(/Unknown virtual chunk container "missing"/);
  });

  it("throws when the vcc:// URL has no slash after the name", () => {
    expect(() =>
      resolveVirtualChunkLocation("vcc://my-data", containers),
    ).toThrow(/missing "\/" after container name/);
  });

  it("passes vcc:// through unchanged when the container list is empty", () => {
    // Preserves behavior for callers using ReadSession.open() directly with a
    // fetchClient that resolves vcc:// itself.
    expect(resolveVirtualChunkLocation("vcc://anything/x", []).url).toBe(
      "vcc://anything/x",
    );
  });
});
