/**
 * HTTP/HTTPS storage backend using the Fetch API.
 *
 * Works in both Node.js 18+ and browsers.
 */

import type { Storage, ByteRange, RequestOptions } from "./storage.js";
import { NotFoundError, StorageError, isAbortError } from "./storage.js";

/** Options for HTTP storage */
export interface HttpStorageOptions {
  /** Additional headers to include in requests */
  headers?: Record<string, string>;

  /** Fetch credentials mode */
  credentials?: RequestCredentials;

  /** Fetch cache mode */
  cache?: RequestCache;
}

/**
 * HTTP/HTTPS storage backend.
 *
 * Reads objects from a base URL using HTTP GET requests.
 * Supports byte range requests for partial reads.
 */
export class HttpStorage implements Storage {
  private baseUrl: string;
  private options: HttpStorageOptions;

  /**
   * Create an HTTP storage backend.
   *
   * @param baseUrl - Base URL for the repository (e.g., "https://example.com/repo")
   * @param options - Additional options
   */
  constructor(baseUrl: string, options: HttpStorageOptions = {}) {
    // Normalize URL (remove trailing slash)
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.options = options;
  }

  /** Build full URL for a path */
  private getUrl(path: string): string {
    // Ensure path doesn't start with slash (we add it)
    const normalizedPath = path.startsWith("/") ? path.slice(1) : path;
    return `${this.baseUrl}/${normalizedPath}`;
  }

  /** Build headers for a request */
  private getHeaders(range?: ByteRange): HeadersInit {
    const headers: HeadersInit = { ...this.options.headers };

    if (range) {
      // HTTP Range header uses inclusive end
      headers["Range"] = `bytes=${range.start}-${range.end - 1}`;
    }

    return headers;
  }

  async getObject(
    path: string,
    range?: ByteRange,
    options?: RequestOptions,
  ): Promise<Uint8Array> {
    options?.signal?.throwIfAborted();

    const url = this.getUrl(path);
    const headers = this.getHeaders(range);

    let response: Response;
    try {
      response = await fetch(url, {
        method: "GET",
        headers,
        credentials: this.options.credentials,
        cache: this.options.cache,
        signal: options?.signal,
      });
    } catch (error) {
      if (isAbortError(error)) throw error;
      throw new StorageError(
        `Failed to fetch ${url}: ${error instanceof Error ? error.message : String(error)}`,
        error instanceof Error ? error : undefined,
      );
    }

    if (response.status === 404) {
      throw new NotFoundError(path);
    }

    // 200 for full content, 206 for partial content
    if (response.status !== 200 && response.status !== 206) {
      throw new StorageError(
        `HTTP ${response.status} ${response.statusText} for ${url}`,
      );
    }

    const arrayBuffer = await response.arrayBuffer();
    return new Uint8Array(arrayBuffer);
  }

  async exists(path: string, options?: RequestOptions): Promise<boolean> {
    options?.signal?.throwIfAborted();

    const url = this.getUrl(path);

    try {
      const response = await fetch(url, {
        method: "HEAD",
        headers: this.options.headers,
        credentials: this.options.credentials,
        signal: options?.signal,
      });

      return response.ok;
    } catch (error) {
      if (isAbortError(error)) throw error;
      return false;
    }
  }

  async *listPrefix(_prefix: string): AsyncIterable<string> {
    // HTTP storage typically doesn't support listing.
    // This would require server-side support (e.g., S3 XML API).
    throw new StorageError(
      "Listing not supported for HTTP storage. Use S3Storage for listing.",
    );
  }
}
