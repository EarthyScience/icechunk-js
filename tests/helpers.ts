/**
 * Test utilities and helpers.
 */

import type { ParsedRepo } from "../src/format/flatbuffers/repo-parser.js";

// Redeclare globalThis types for TypeScript
declare global {
  var FIXTURE_SERVER_PORT: number;
  var FIXTURE_SERVER_URL: string;
}

/**
 * Get the fixture URL for a given repository.
 */
export function getFixtureUrl(repoName: string): string {
  const baseUrl = globalThis.FIXTURE_SERVER_URL ?? "http://localhost:8765";
  return `${baseUrl}/${repoName}`;
}

/**
 * Return a copy of a parsed repo whose snapshot at `index` reports
 * `parentOffset`, for simulating corrupted ancestry.
 */
export function withParentOffset(
  parsed: ParsedRepo,
  index: number,
  parentOffset: number,
): ParsedRepo {
  const repo = Object.create(parsed.repo) as ParsedRepo["repo"];
  repo.snapshots = (i, obj) => {
    const info = parsed.repo.snapshots(i, obj);
    if (i !== index || !info) return info;
    const patched = Object.create(info) as typeof info;
    patched.parentOffset = () => parentOffset;
    return patched;
  };
  return { ...parsed, repo };
}
