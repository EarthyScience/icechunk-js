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

/** Options for storage request operations */
export interface RequestOptions {
  /** AbortSignal for request cancellation */
  signal?: AbortSignal;
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

/** Error thrown when an operation is aborted */
export class AbortError extends Error {
  constructor(message = 'Operation aborted') {
    super(message);
    this.name = 'AbortError';
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
   * @param options - Optional request options (signal for cancellation)
   * @returns Object data as bytes
   * @throws NotFoundError if the object doesn't exist
   * @throws StorageError for other errors
   * @throws AbortError if the operation was aborted
   */
  getObject(path: string, range?: ByteRange, options?: RequestOptions): Promise<Uint8Array>;

  /**
   * Check if an object exists.
   *
   * @param path - Path to the object
   * @param options - Optional request options (signal for cancellation)
   * @returns True if the object exists
   * @throws AbortError if the operation was aborted
   */
  exists(path: string, options?: RequestOptions): Promise<boolean>;

  /**
   * List objects with a given prefix.
   *
   * @param prefix - Path prefix to filter by
   * @returns Async iterable of object paths
   */
  listPrefix(prefix: string): AsyncIterable<string>;
}
