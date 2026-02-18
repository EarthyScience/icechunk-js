/**
 * Test utilities and helpers.
 */

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
