/**
 * HTTP server to serve test fixtures.
 *
 * Supports:
 * - Full file reads (GET)
 * - Range requests with proper headers (Accept-Ranges, Content-Range, 206 status)
 * - HEAD requests for exists checks
 * - CORS headers for browser testing
 *
 * Usage:
 *   npx tsx tests/fixtures/serve.ts [port] [fixtures_dir]
 */

import { createServer, IncomingMessage, ServerResponse } from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import { join, resolve } from 'node:path';

const PORT = parseInt(process.argv[2] || '8765', 10);
const FIXTURES_DIR = resolve(process.argv[3] || 'tests/fixtures');

function setCorsHeaders(res: ServerResponse): void {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Range');
  res.setHeader('Access-Control-Expose-Headers', 'Content-Range, Content-Length, Accept-Ranges');
}

function parseRangeHeader(
  rangeHeader: string,
  fileSize: number
): { start: number; end: number } | null {
  const match = rangeHeader.match(/bytes=(\d+)-(\d*)/);
  if (!match) return null;

  const start = parseInt(match[1], 10);
  const end = match[2] ? parseInt(match[2], 10) : fileSize - 1;

  // Validate range
  if (start > end || start >= fileSize) return null;

  // Clamp end to file size
  const clampedEnd = Math.min(end, fileSize - 1);

  return { start, end: clampedEnd };
}

async function handleRequest(req: IncomingMessage, res: ServerResponse): Promise<void> {
  // Parse URL and strip leading slash to prevent path traversal
  const url = new URL(req.url || '/', `http://localhost:${PORT}`);
  const relativePath = decodeURIComponent(url.pathname).replace(/^\/+/, '');

  // Set CORS headers on all responses
  setCorsHeaders(res);

  // Health check endpoint for readiness detection
  if (relativePath === '_health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
    return;
  }

  const filePath = join(FIXTURES_DIR, relativePath);

  // Handle OPTIONS preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  try {
    const stats = await stat(filePath);

    if (stats.isDirectory()) {
      // Return 404 for directories (not supported)
      res.writeHead(404);
      res.end('Not Found');
      return;
    }

    const fileSize = stats.size;

    // Always indicate Range support
    res.setHeader('Accept-Ranges', 'bytes');

    // Handle HEAD request
    if (req.method === 'HEAD') {
      res.setHeader('Content-Length', fileSize);
      res.setHeader('Content-Type', 'application/octet-stream');
      res.writeHead(200);
      res.end();
      return;
    }

    // Handle Range request
    const rangeHeader = req.headers.range;
    if (rangeHeader) {
      const range = parseRangeHeader(rangeHeader, fileSize);

      if (!range) {
        // Invalid range - return 416 Range Not Satisfiable
        res.setHeader('Content-Range', `bytes */${fileSize}`);
        res.writeHead(416);
        res.end('Range Not Satisfiable');
        return;
      }

      const { start, end } = range;
      const contentLength = end - start + 1;

      // Read the requested range
      const content = await readFile(filePath);
      const slice = content.slice(start, end + 1);

      res.setHeader('Content-Type', 'application/octet-stream');
      res.setHeader('Content-Length', contentLength);
      res.setHeader('Content-Range', `bytes ${start}-${end}/${fileSize}`);
      res.writeHead(206); // Partial Content
      res.end(slice);
      return;
    }

    // Full file read
    const content = await readFile(filePath);
    res.setHeader('Content-Type', 'application/octet-stream');
    res.setHeader('Content-Length', content.length);
    res.writeHead(200);
    res.end(content);
  } catch (err: unknown) {
    const error = err as NodeJS.ErrnoException;
    if (error.code === 'ENOENT') {
      res.writeHead(404);
      res.end('Not Found');
    } else {
      console.error('Server error:', error);
      res.writeHead(500);
      res.end('Internal Server Error');
    }
  }
}

const server = createServer((req, res) => {
  handleRequest(req, res).catch((err) => {
    console.error('Unhandled error:', err);
    if (!res.headersSent) {
      res.writeHead(500);
      res.end('Internal Server Error');
    }
  });
});

server.listen(PORT, () => {
  console.log(`Fixture server running at http://localhost:${PORT}`);
  console.log(`Serving: ${FIXTURES_DIR}`);
});

// Export for programmatic use
export { server, PORT, FIXTURES_DIR };
