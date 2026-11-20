/**
 * Reader for the virtual chunk containers of a v1 repository's `config.yaml`.
 *
 * A v2 repository carries its config inside the repo file; a v1 repository
 * keeps it in `config.yaml`, as written by Icechunk 1.x:
 *
 *   virtual_chunk_containers:
 *     s3://bucket/prefix/:
 *       name: null
 *       url_prefix: s3://bucket/prefix/
 *       store: !s3
 *         region: us-west-2
 *         endpoint_url: null
 *         force_path_style: false
 *
 * This is not a YAML parser. It reads the one section this library uses, in
 * the block style Icechunk writes: nested `key: value` mappings indented by
 * spaces, plain or quoted scalars, a `!tag` naming the store variant, and the
 * `? key` / `: value` pair Icechunk writes for a key longer than 128
 * characters. Any other construct inside that section (a sequence, a comment,
 * a multi-line or flow value other than `{}`, a double-quoted scalar with an
 * escape JSON lacks) makes it give up and report no containers, which leaves
 * chunk locations resolving as they did before the config was read.
 */

import {
  virtualChunkContainersFromConfig,
  type VirtualChunkContainer,
} from "./flatbuffers/repo-parser.js";

const SECTION = "virtual_chunk_containers";

/** A `key: value` entry after its indentation: key (quoted or plain), rest. */
const ENTRY = /^('(?:[^']|'')*'|"(?:[^"\\]|\\.)*"|.+?):(?: +(.*))?$/;

/**
 * Parse the virtual chunk containers out of a v1 `config.yaml`, sorted like
 * those of a v2 repo file. Returns an empty array when the config declares
 * none or its containers section is not in the form described above.
 */
export function parseConfigYamlContainers(
  yaml: string,
): VirtualChunkContainer[] {
  const lines = yaml.split(/\r?\n/);
  const start = lines.findIndex((line) => line.startsWith(`${SECTION}:`));
  if (start < 0) return [];
  // The section runs until the next top-level key.
  const body: string[] = [];
  for (let i = start + 1; i < lines.length; i++) {
    if (/^\S/.test(lines[i])) break;
    if (lines[i].trim() !== "") body.push(lines[i]);
  }
  // `? key` then `: value` on the next line is `key: value` with the value's
  // mapping starting on the `:` line. Keys are not used, only what they hold.
  for (let i = 0; i + 1 < body.length; i++) {
    const indent = indentOf(body[i]);
    const value = body[i + 1].slice(indent);
    if (
      body[i].startsWith("? ", indent) &&
      indentOf(body[i + 1]) === indent &&
      value.startsWith(": ")
    ) {
      body[i] =
        `${" ".repeat(indent)}${JSON.stringify(body[i].slice(indent + 2))}:`;
      body[i + 1] = `${" ".repeat(indent)}  ${value.slice(2)}`;
    }
  }
  try {
    const [containers] = parseMapping(body, 0);
    return virtualChunkContainersFromConfig({ [SECTION]: containers });
  } catch {
    return [];
  }
}

/**
 * Parse the mapping whose first entry is `lines[from]`, up to the first line
 * indented less than it. Returns the mapping and the index of that line.
 */
function parseMapping(
  lines: string[],
  from: number,
): [Record<string, unknown>, number] {
  // No prototype, so a key such as `__proto__` is only ever a key.
  const result: Record<string, unknown> = Object.create(null);
  if (from >= lines.length) return [result, from];
  const indent = indentOf(lines[from]);
  let i = from;
  while (i < lines.length && indentOf(lines[i]) >= indent) {
    const entry =
      indentOf(lines[i]) === indent ? ENTRY.exec(lines[i].slice(indent)) : null;
    if (!entry || /^[-?:] /.test(entry[1])) {
      throw new Error(`Unsupported config.yaml line: ${lines[i]}`);
    }
    i++;
    const key = unquote(entry[1]);
    const tagged = /^!(\S+)(?: +(.*))?$/.exec(entry[2] ?? "");
    const rest = (tagged ? (tagged[2] ?? "") : (entry[2] ?? "")).trim();
    let value: unknown;
    if (rest === "" && i < lines.length && indentOf(lines[i]) > indent) {
      [value, i] = parseMapping(lines, i);
    } else {
      value = parseScalar(rest);
    }
    // `store: !s3` is the s3 variant of the store, as `{ s3: ... }` in v2.
    result[key] = tagged ? { [tagged[1]]: value } : value;
  }
  return [result, i];
}

function indentOf(line: string): number {
  return line.length - line.trimStart().length;
}

function parseScalar(text: string): unknown {
  if (text === "" || text === "null" || text === "~") return null;
  if (text === "true") return true;
  if (text === "false") return false;
  if (text === "{}") return {};
  const quoted = /^['"]/.test(text);
  // A flow collection, block scalar, anchor or alias; or a plain scalar
  // followed by a comment.
  if (/^[[{|>&*]/.test(text) || (!quoted && /(^| )#/.test(text))) {
    throw new Error(`Unsupported config.yaml value: ${text}`);
  }
  return unquote(text);
}

function unquote(text: string): string {
  if (text.length >= 2 && text.startsWith("'") && text.endsWith("'")) {
    return text.slice(1, -1).replace(/''/g, "'");
  }
  if (text.length >= 2 && text.startsWith('"') && text.endsWith('"')) {
    return JSON.parse(text) as string;
  }
  return text;
}
