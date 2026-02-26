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

/**
 * Pluggable HTTP client for virtual chunk fetching.
 *
 * Use this to:
 * - Generate pre-signed S3 URLs
 * - Add authentication headers
 * - Route through a proxy
 *
 * icechunk-js handles URL translation (s3:// → https://) and builds
 * all headers (Range, If-Match, etc.) before calling fetch().
 * The client only needs to execute the HTTP request.
 */
export interface FetchClient {
  fetch(url: string, init?: RequestInit): Promise<Response>;
}

/** Default FetchClient that delegates to globalThis.fetch. */
export class DefaultFetchClient implements FetchClient {
  fetch(url: string, init?: RequestInit): Promise<Response> {
    return globalThis.fetch(url, init);
  }
}

/** Options for storage request operations */
export interface RequestOptions {
  /** AbortSignal for request cancellation */
  signal?: AbortSignal;
  /** Pluggable HTTP client for virtual chunk fetching */
  fetchClient?: FetchClient;
  /**
   * Send If-Match / If-Unmodified-Since headers on virtual chunk requests.
   *
   * When true, the storage server will return 412 Precondition Failed if
   * the underlying file has changed since the snapshot recorded its checksum.
   *
   * Defaults to false because these headers trigger CORS preflight requests
   * in browsers, and most storage servers don't whitelist them by default.
   */
  validateChecksums?: boolean;
  /**
   * Azure storage account name for translating az:// and azure:// URLs.
   *
   * Required when virtual chunks reference az:// or azure:// URLs, since
   * these schemes encode only the container name (e.g., az://container/path).
   * The account is needed to build the HTTPS endpoint:
   * https://{account}.blob.core.windows.net/{container}/{path}
   *
   * Not needed for abfs:// URLs, which embed the account in the host.
   */
  azureAccount?: string;
}

/** Error thrown when an object is not found */
export class NotFoundError extends Error {
  constructor(public readonly path: string) {
    super(`Object not found: ${path}`);
    this.name = "NotFoundError";
  }
}

/** Error thrown for storage operations */
export class StorageError extends Error {
  constructor(
    message: string,
    public readonly cause?: Error,
  ) {
    super(message);
    this.name = "StorageError";
  }
}

/** Error thrown when an operation is aborted */
export class AbortError extends Error {
  constructor(message = "Operation aborted") {
    super(message);
    this.name = "AbortError";
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
  getObject(
    path: string,
    range?: ByteRange,
    options?: RequestOptions,
  ): Promise<Uint8Array>;

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
