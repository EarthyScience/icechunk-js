/**
 * Icechunk format constants.
 */

/** Storage path constants */
export const PATHS = {
  /** Refs directory */
  REFS: 'refs',

  /** Snapshots directory */
  SNAPSHOTS: 'snapshots',

  /** Manifests directory */
  MANIFESTS: 'manifests',

  /** Chunks directory */
  CHUNKS: 'chunks',
} as const;

/** Repository info file path (v2 format) */
export const REPO_INFO_PATH = 'repo';

/**
 * Get the storage path prefix for a branch ref directory.
 * The actual ref file may be named with a version suffix (e.g., ZZZZZZZZ.json)
 * for optimistic concurrency control on storage backends without native versioning.
 *
 * @param name - Branch name
 * @returns Path to the branch ref directory (with trailing slash)
 */
export function getBranchRefDirPath(name: string): string {
  return `${PATHS.REFS}/branch.${name}/`;
}

/**
 * Get the storage path prefix for a tag ref directory.
 * The actual ref file may be named with a version suffix.
 *
 * @param name - Tag name
 * @returns Path to the tag ref directory (with trailing slash)
 */
export function getTagRefDirPath(name: string): string {
  return `${PATHS.REFS}/tag.${name}/`;
}

/**
 * Default ref file name for testing and legacy compatibility.
 * In production, refs are stored with versioned filenames (e.g., ZZZZZZZZ.json)
 * for optimistic concurrency control.
 */
export const REF_FILE_NAME = 'ref.json';

/**
 * Get the storage path for a branch ref file (legacy format).
 * Note: In production, refs use versioned filenames. Use getBranchRefDirPath
 * and findLatestRefFile for reading refs.
 *
 * @param name - Branch name
 * @returns Path to the branch ref file (legacy format)
 */
export function getBranchRefPath(name: string): string {
  return `${PATHS.REFS}/branch.${name}/${REF_FILE_NAME}`;
}

/**
 * Get the storage path for a tag ref file (legacy format).
 * Note: In production, refs use versioned filenames. Use getTagRefDirPath
 * and findLatestRefFile for reading refs.
 *
 * @param name - Tag name
 * @returns Path to the tag ref file (legacy format)
 */
export function getTagRefPath(name: string): string {
  return `${PATHS.REFS}/tag.${name}/${REF_FILE_NAME}`;
}

/**
 * Get the storage path for a snapshot.
 *
 * @param id - Snapshot ID as Base32 string
 * @returns Path to the snapshot file
 */
export function getSnapshotPath(id: string): string {
  return `${PATHS.SNAPSHOTS}/${id}`;
}

/**
 * Get the storage path for a manifest.
 *
 * @param id - Manifest ID as Base32 string
 * @returns Path to the manifest file
 */
export function getManifestPath(id: string): string {
  return `${PATHS.MANIFESTS}/${id}`;
}

/**
 * Get the storage path for a chunk.
 *
 * @param id - Chunk ID as Base32 string
 * @returns Path to the chunk file
 */
export function getChunkPath(id: string): string {
  return `${PATHS.CHUNKS}/${id}`;
}
