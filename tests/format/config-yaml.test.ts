import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { parseConfigYamlContainers } from "../../src/format/config-yaml.js";
import { parseRepo } from "../../src/format/flatbuffers/repo-parser.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA = join(__dirname, "../data");

const configOf = (repo: string) =>
  readFileSync(join(DATA, repo, "config.yaml"), "utf8");

describe("parseConfigYamlContainers", () => {
  it("reads every container of a v1 repo written by Icechunk", () => {
    const byName = new Map(
      parseConfigYamlContainers(configOf("test-repo-v1")).map((c) => [
        c.name,
        c,
      ]),
    );

    expect([...byName.keys()].sort()).toEqual([
      "az",
      "file",
      "gcs",
      "s3",
      "tigris",
    ]);
    expect(byName.get("s3")).toEqual({
      name: "s3",
      urlPrefix: "s3://testbucket",
      s3: {
        region: "us-east-1",
        endpointUrl: "http://localhost:4200",
        forcePathStyle: true,
      },
    });
    // Tigris uses S3Options too; its region is null and so left unset.
    expect(byName.get("tigris")?.s3).toEqual({
      endpointUrl: "https://fly.storage.tigris.dev",
    });
    // Non-S3 stores carry no S3 config.
    expect(byName.get("gcs")?.s3).toBeUndefined();
    expect(byName.get("az")?.s3).toBeUndefined();
    expect(byName.get("file")?.s3).toBeUndefined();
  });

  it("agrees with the repo file the same repo was upgraded to", () => {
    // test-repo-v2-migrated is test-repo-v1 after an upgrade to v2, so both
    // describe the same containers. The upgrade fills in defaults a v1 config
    // leaves out, hence the comparison on the fields that address a request.
    const addressing = (
      containers: ReturnType<typeof parseConfigYamlContainers>,
    ) =>
      containers
        .map((c) => ({
          name: c.name,
          urlPrefix: c.urlPrefix,
          region: c.s3?.region,
          endpointUrl: c.s3?.endpointUrl,
          forcePathStyle: c.s3?.forcePathStyle === true,
        }))
        .sort((a, b) => String(a.name).localeCompare(String(b.name)));

    const migrated = parseRepo(
      readFileSync(join(DATA, "test-repo-v2-migrated", "repo")),
    ).virtualChunkContainers;
    expect(
      addressing(parseConfigYamlContainers(configOf("test-repo-v1"))),
    ).toEqual(addressing(migrated));
  });

  it("reads an unnamed container keyed by its URL prefix", () => {
    const yaml = [
      "inline_chunk_threshold_bytes: null",
      "virtual_chunk_containers:",
      "  s3://us-west-2.example.bucket/data/:",
      "    name: null",
      "    url_prefix: s3://us-west-2.example.bucket/data/",
      "    store: !s3",
      "      region: us-west-2",
      "      endpoint_url: null",
      "      anonymous: false",
      "      allow_http: false",
      "      force_path_style: false",
      "      network_stream_timeout_seconds: 60",
      "      requester_pays: false",
      "manifest: null",
      "",
    ].join("\n");

    expect(parseConfigYamlContainers(yaml)).toEqual([
      {
        name: null,
        urlPrefix: "s3://us-west-2.example.bucket/data/",
        s3: { region: "us-west-2", forcePathStyle: false },
      },
    ]);
  });

  it("reads a container whose key Icechunk wrote in the long form", () => {
    // Icechunk writes a key longer than 128 characters as `? key` with the
    // value starting on a `: ` line. This is its output for two containers.
    const long = `s3://my.bucket/${"d".repeat(113)}/`;
    expect(long).toHaveLength(129);
    const store = (region: string) => [
      "    store: !s3",
      `      region: ${region}`,
      "      endpoint_url: null",
      "      anonymous: false",
      "      allow_http: false",
      "      force_path_style: false",
      "      network_stream_timeout_seconds: 60",
      "      requester_pays: false",
    ];
    const yaml = [
      "storage: null",
      "virtual_chunk_containers:",
      "  s3://other.bucket/:",
      "    name: null",
      "    url_prefix: s3://other.bucket/",
      ...store("eu-west-1"),
      `  ? ${long}`,
      "  : name: null",
      `    url_prefix: ${long}`,
      ...store("us-west-2"),
      "manifest: null",
      "",
    ].join("\n");

    expect(parseConfigYamlContainers(yaml)).toEqual([
      {
        name: null,
        urlPrefix: long,
        s3: { region: "us-west-2", forcePathStyle: false },
      },
      {
        name: null,
        urlPrefix: "s3://other.bucket/",
        s3: { region: "eu-west-1", forcePathStyle: false },
      },
    ]);
  });

  it("puts the most specific URL prefix first", () => {
    const yaml = [
      "virtual_chunk_containers:",
      "  a:",
      "    name: a",
      "    url_prefix: s3://bucket/",
      "    store: !s3",
      "      region: us-east-1",
      "  b:",
      "    name: b",
      "    url_prefix: s3://bucket/deeper/",
      "    store: !s3",
      "      region: us-east-1",
    ].join("\n");

    expect(parseConfigYamlContainers(yaml).map((c) => c.name)).toEqual([
      "b",
      "a",
    ]);
  });

  it("reads quoted scalars and Windows line endings", () => {
    const yaml = [
      "virtual_chunk_containers:",
      "  'quoted: key':",
      '    name: "na\\"me"',
      "    url_prefix: 's3://it''s/'",
      "    store: !s3",
      "      region: 'eu-west-1'",
    ].join("\r\n");

    expect(parseConfigYamlContainers(yaml)).toEqual([
      { name: 'na"me', urlPrefix: "s3://it's/", s3: { region: "eu-west-1" } },
    ]);
  });

  it("keeps a `#` inside a value and drops spaces after it", () => {
    const yaml = [
      "virtual_chunk_containers:",
      "  a:",
      "    name: a",
      "    url_prefix: s3://bucket/a#b/  ",
      "    store: !s3",
      "      region: us-east-1 ",
    ].join("\n");

    expect(parseConfigYamlContainers(yaml)).toEqual([
      { name: "a", urlPrefix: "s3://bucket/a#b/", s3: { region: "us-east-1" } },
    ]);
  });

  it("treats `__proto__` as a key like any other", () => {
    const yaml = [
      "virtual_chunk_containers:",
      "  a:",
      "    __proto__:",
      "      url_prefix: s3://inherited/",
    ].join("\n");

    expect(parseConfigYamlContainers(yaml)).toEqual([]);
  });

  it("reports no containers when the config declares none", () => {
    expect(parseConfigYamlContainers(configOf("split-repo-v1"))).toEqual([]);
    expect(parseConfigYamlContainers("virtual_chunk_containers: {}\n")).toEqual(
      [],
    );
    expect(parseConfigYamlContainers("compression: null\n")).toEqual([]);
    expect(parseConfigYamlContainers("")).toEqual([]);
  });

  it("reports no containers for a section it cannot read", () => {
    for (const section of [
      "  - a sequence",
      "  a:\n    name: a\n    url_prefix: [flow, sequence]",
      "  a:\n    name: a\n    url_prefix: |\n      block scalar",
      "  a:\n    name: a\n   url_prefix: misaligned",
      "  not a mapping entry",
      "  a:\n    name: a\n    url_prefix: s3://bucket/ # a comment",
      '  a:\n    name: a\n    url_prefix: "s3://bucket/\\x7F"',
      "  ? a\n  ? b",
    ]) {
      expect(
        parseConfigYamlContainers(`virtual_chunk_containers:\n${section}\n`),
      ).toEqual([]);
    }
  });
});
