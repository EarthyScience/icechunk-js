# icechunk-js

[![npm version](https://img.shields.io/npm/v/icechunk-js.svg)](https://www.npmjs.com/package/icechunk-js)
[![Icechunk](https://img.shields.io/badge/Icechunk-008B8B)](https://github.com/earth-mover/icechunk)
[![Zarrita.js](https://img.shields.io/badge/Zarrita.js-Compatible-EE3F98)](https://github.com/manzt/zarrita.js)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/EarthyScience/icechunk-js/blob/main/LICENSE)

Read-only JavaScript/TypeScript reader for Icechunk repositories, designed for
use with [zarrita](https://github.com/manzt/zarrita.js).

- Pure TypeScript, works in browsers and Node.js 18+
- Icechunk v1 and v2 format auto-detection
- All chunk payload types: inline, native, and virtual

## Getting Started

```bash
npm install icechunk-js
```

### Basic Usage with zarrita

```typescript
import { IcechunkStore } from "icechunk-js";
import { open, get } from "zarrita";

// Open a store from a URL
const store = await IcechunkStore.open("https://bucket.s3.amazonaws.com/repo");

// Open an array and read data
const array = await open(store.resolve("/temperature"), { kind: "array" });
const data = await get(array, [0, 0, null]);
```

### For Development

```bash
npm install

npm run dev

npm run test

npm run typecheck
```

To regenerate FlatBuffers TypeScript after syncing schemas, run
`npm run generate:fbs`. The pinned `flatc` compiler is downloaded automatically
if not already available. The version is set in `scripts/ensure-flatc.sh`.

## API

### IcechunkStore

The main class for zarrita integration. Implements zarrita's `AsyncReadable`
interface with both `get()` and `getRange()` (needed for sharded arrays). Pass
zarrita's `withRangeCoalescing` to coalesce concurrent reads against the same
backing object. This requires zarrita >= 0.7.

> **Note:** Range coalescing uses zarrita's merged abort-signal behavior. If one
> read in a merged batch is aborted, other reads in the same batch may also
> reject. Avoid sharing an `AbortController` across requests that must cancel
> independently.

```typescript
import { IcechunkStore } from "icechunk-js";
import { withRangeCoalescing } from "zarrita";

// Open from a URL (default: branch "main")
const store = await IcechunkStore.open("https://example.com/repo", {
  branch: "main",
  // tag: 'v1.0',
  // snapshot: 'ABC123...',
  // formatVersion: 'v1',     // skip format auto-detection for v1 repos
  // maxManifestCacheSize: 50, // LRU cache size (default: 100)
  // withRangeCoalescing, // opt into merged range reads
  // signal: abortController.signal, // cancel initialization
  // validateChecksums: true,  // integrity headers for virtual chunks
  // azureAccount: 'myaccount', // required for az:// virtual chunks
});

// Open from an existing ReadSession
const store = await IcechunkStore.open(session);

// Open from a custom Storage backend
const store = await IcechunkStore.open(myStorage, { branch: "main" });
```

#### Store methods

```typescript
// Scope to a subpath (shares the same session and cache)
const scoped = store.resolve("group/subgroup");

// Browse the hierarchy
const children = store.listChildren("/"); // direct children of root
const allNodes = store.listNodes(); // all nodes in the snapshot
const node = store.getNode("/temperature"); // single node by path
const meta = store.getMetadata("/temperature"); // parsed zarr.json

// Access the underlying session for advanced operations
const session = store.session;
```

#### Virtual chunk authentication

For private datasets with virtual chunks (S3, GCS, Azure), provide a
`fetchClient` that handles authentication:

```typescript
import type { FetchClient } from "icechunk-js";

const fetchClient: FetchClient = {
  async fetch(url, init) {
    // URL rewriting, pre-signing, or header injection happens here.
    // icechunk-js has already translated s3:// → https:// and built
    // Range headers in `init` (plus If-Match when validateChecksums is on).
    const signedUrl = await presign(url);
    return globalThis.fetch(signedUrl, {
      ...init,
      headers: { ...init?.headers, Authorization: `Bearer ${token}` },
    });
  },
};

const store = await IcechunkStore.open("https://example.com/repo", {
  fetchClient,
});
```

Cloud storage URLs in virtual chunk references are automatically translated:

- `s3://bucket/key` → `https://bucket.s3.amazonaws.com/key`
- `gs://bucket/key` (or `gcs://`) → `https://storage.googleapis.com/bucket/key`
- `az://container/path` (or `azure://`) →
  `https://{azureAccount}.blob.core.windows.net/container/path`
- `abfs://container@account.dfs.core.windows.net/path` →
  `https://account.blob.core.windows.net/container/path`

### Repository

For direct access to branches, tags, and checkouts.

> **Note:** Over plain HTTP, `listBranches()` and `listTags()` only work
> reliably with v2 repos, which embed refs in the top-level `repo` file. For v1
> repos, direct `checkoutBranch()` / `checkoutTag()` can work when the target
> ref still lives at the legacy `ref.json` path, but versioned ref filenames
> still require `listPrefix()` discovery, which `HttpStorage` does not provide.
> Use a listing-capable storage backend for full v1 branch/tag support.

```typescript
import { Repository, HttpStorage } from "icechunk-js";

// Replace with the root URL of a real Icechunk repository.
const storage = new HttpStorage("https://example.com/repo");

// Auto-detect format (default)
const repo = await Repository.open({ storage });

// Or with format version hint (skips /repo request for v1 stores)
// const repo = await Repository.open({ storage, formatVersion: 'v1' });

// List branches and tags (v2 repos, or storage backends that support listing)
const branches = await repo.listBranches();
const tags = await repo.listTags();

// Checkout to get a ReadSession
const session = await repo.checkoutBranch("main");
// or: repo.checkoutTag('v1.0')
// or: repo.checkoutSnapshot('ABCDEFGHIJKLMNOP')
```

#### Walking commit history

```typescript
for await (const entry of repo.walkHistory(session)) {
  console.log(entry.id, entry.message, entry.flushedAt, entry.metadata);
}
```

### ReadSession

Low-level access to nodes, chunks, and snapshot metadata.

```typescript
// Snapshot info
const snapshotId = session.getSnapshotId();
const parentId = session.getParentSnapshotId(); // null for root
const message = session.getMessage();
const timestamp = session.getFlushedAt();
const metadata = session.getSnapshotMetadata();

// Navigate the hierarchy
const nodes = session.listNodes();
const children = session.listChildren("/group");
const node = session.getNode("/array");

// Get Zarr metadata and chunks
const zarrMeta = session.getMetadata("/array");
const chunk = await session.getChunk("/array", [0, 0, 0]);

// Transaction log (what changed in this snapshot)
const txLog = await session.loadTransactionLog();
if (txLog) {
  console.log("New arrays:", txLog.newArrays.length);
  console.log("Updated chunks:", txLog.updatedChunks.length);
}
```

### HttpStorage

HTTP/HTTPS storage backend using the Fetch API. Works in Node.js 18+ and
browsers.

```typescript
import { HttpStorage } from "icechunk-js";

const storage = new HttpStorage("https://bucket.s3.amazonaws.com/repo", {
  headers: { Authorization: "Bearer token" },
  credentials: "include",
  cache: "no-store",
});
```

### Custom Storage

Implement the `Storage` interface for other backends:

```typescript
import type { Storage, ByteRange, RequestOptions } from 'icechunk-js';

class MyStorage implements Storage {
  async getObject(path: string, range?: ByteRange, options?: RequestOptions): Promise<Uint8Array> { ... }
  async exists(path: string, options?: RequestOptions): Promise<boolean> { ... }
  async *listPrefix(prefix: string): AsyncIterable<string> { ... }
}
```

## License

MIT
