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

/** Options passed to the transformRequest callback */
export interface TransformRequestOptions {
  /** HTTP method for the request */
  method?: 'GET' | 'HEAD';
}

/** Result returned by the transformRequest callback */
export interface TransformRequestResult {
  /** The (possibly transformed) URL to fetch */
  url: string;
  /** Additional headers to include in the request */
  headers?: Record<string, string>;
  /** HTTP method to use (defaults to GET) */
  method?: 'GET' | 'HEAD';
  /** Allow other RequestInit options to be passed through */
  [key: string]: unknown;
}

/**
 * Callback to transform virtual chunk URLs before fetching.
 *
 * Use this to:
 * - Generate pre-signed S3 URLs
 * - Add authentication headers
 * - Route through a proxy
 *
 * @param url - The URL after default translation (e.g., s3:// → https://)
 * @param options - Request options including HTTP method
 * @returns Transformed URL and optional headers/RequestInit options
 */
export type TransformRequest = (
  url: string,
  options?: TransformRequestOptions
) => TransformRequestResult | Promise<TransformRequestResult>;

/** Options for storage request operations */
export interface RequestOptions {
  /** AbortSignal for request cancellation */
  signal?: AbortSignal;
  /** Callback to transform virtual chunk URLs before fetching */
  transformRequest?: TransformRequest;
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
