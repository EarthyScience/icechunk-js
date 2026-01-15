/**
 * Storage interface for icechunk repositories.
 */

/** Byte range for partial reads */
export interface ByteRange {
  /** Start offset (inclusive) */
  start: number;

  /** End offset (exclusive) */
  end: number;
}

/** Error thrown when an object is not found */
export class NotFoundError extends Error {
  constructor(public readonly path: string) {
    super(`Object not found: ${path}`);
    this.name = 'NotFoundError';
  }
}

/** Error thrown for storage operations */
export class StorageError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message);
    this.name = 'StorageError';
  }
}

/**
 * Storage interface for reading icechunk data.
 *
 * Implementations should handle authentication, caching, etc.
 */
export interface Storage {
  /**
   * Get an object from storage.
   *
   * @param path - Path to the object (relative to repository root)
   * @param range - Optional byte range for partial reads
   * @returns Object data as bytes
   * @throws NotFoundError if the object doesn't exist
   * @throws StorageError for other errors
   */
  getObject(path: string, range?: ByteRange): Promise<Uint8Array>;

  /**
   * Check if an object exists.
   *
   * @param path - Path to the object
   * @returns True if the object exists
   */
  exists(path: string): Promise<boolean>;

  /**
   * List objects with a given prefix.
   *
   * @param prefix - Path prefix to filter by
   * @returns Async iterable of object paths
   */
  listPrefix(prefix: string): AsyncIterable<string>;
}
