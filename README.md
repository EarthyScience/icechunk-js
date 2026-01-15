# Icechunk JavaScript

Read-only JavaScript/TypeScript reader for Icechunk repositories, designed for
use with [zarrita](https://github.com/manzt/zarrita.js).

## Getting Started

npm install TK

### Basic Usage with zarrita

```typescript
import { IcechunkStore } from "icechunk-js";
import { open, get } from "zarrita";

// Create a store from a URL
const store = new IcechunkStore("https://bucket.s3.amazonaws.com/repo");

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
interface.

```typescript
import { IcechunkStore } from "icechunk-js";

// From a URL (uses HttpStorage internally)
const store = new IcechunkStore("https://example.com/repo");

// With options
const store = new IcechunkStore("https://example.com/repo", {
  branch: "main", // checkout a branch (default)
  // tag: 'v1.0',      // or checkout a tag
  // snapshot: 'ABC123...',  // or checkout a specific snapshot ID
  // signal: controller.signal,  // AbortSignal for init cancellation
});

// From a custom Storage backend
const store = new IcechunkStore(storage, { branch: "develop" });
```

### Repository

For direct access to branches, tags, and checkouts.

```typescript
import { Repository, HttpStorage } from "icechunk-js";

const storage = new HttpStorage("https://example.com/repo");
const repo = await Repository.open({ storage });

// List branches and tags
const branches = await repo.listBranches();
const tags = await repo.listTags();

// Checkout to get a ReadSession
const session = await repo.checkoutBranch("main");
const session = await repo.checkoutTag("v1.0");
const session = await repo.checkoutSnapshot("ABCDEFGHIJKLMNOP");
```

### ReadSession

Low-level access to nodes and chunks.

```typescript
// Get snapshot info
const snapshotId = session.getSnapshotId();
const message = session.getMessage();
const timestamp = session.getFlushedAt();

// Navigate the hierarchy
const nodes = session.listNodes();
const children = session.listChildren("/group");

// Get metadata and chunks
const metadata = session.getMetadata("/array");
const chunk = await session.getChunk("/array", [0, 0, 0]);
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

The `RequestOptions` type contains an optional `signal: AbortSignal` for cancellation support.

## License

MIT
