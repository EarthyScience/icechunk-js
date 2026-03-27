# Icechunk JavaScript

Read-only JavaScript/TypeScript reader for Icechunk repositories, designed for
use with [zarrita](https://github.com/manzt/zarrita.js).

- ~50KB bundle, zero native dependencies
- Works in browsers, Node.js 18+, Deno, and Bun
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
const array = await open(store, { kind: "array", path: "/temperature" });
const data = await get(array, [0, 0, null]);
```

### For Development

```bash
npm install

npm run dev

npm run test

npm run typecheck
```

## API

### IcechunkStore

The main class for zarrita integration. Implements zarrita's `AsyncReadable`
interface with both `get()` and `getRange()` (needed for sharded arrays).

```typescript
import { IcechunkStore } from "icechunk-js";

// Open from a URL (default: branch "main")
const store = await IcechunkStore.open("https://example.com/repo", {
  branch: "main",
  // tag: 'v1.0',
  // snapshot: 'ABC123...',
  // formatVersion: 'v1',     // skip format auto-detection for v1 repos
  // maxManifestCacheSize: 50, // LRU cache size (default: 100)
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
    // Range / If-None-Match headers in `init`.
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
- `gs://bucket/key` → `https://storage.googleapis.com/bucket/key`
- `az://account/container/path` → `https://account.blob.core.windows.net/container/path`
- `abfs://container@account.dfs.core.windows.net/path` → `https://account.blob.core.windows.net/container/path`

### Repository

For direct access to branches, tags, and checkouts.

```typescript
import { Repository, HttpStorage } from "icechunk-js";

const storage = new HttpStorage("https://example.com/repo");

// Auto-detect format (default)
const repo = await Repository.open({ storage });

// Or with format version hint (skips /repo request for v1 stores)
// const repo = await Repository.open({ storage, formatVersion: 'v1' });

// List branches and tags
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
  console.log(entry.id, entry.message, entry.flushedAt);
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
