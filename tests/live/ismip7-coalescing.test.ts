/**
 * Live measurement against the public ISMIP7 Icechunk store on source.coop
 * (anonymous HTTPS). Reads one time step of a virtual-chunk variable through
 * zarrita with and without the built-in coalescer and checks that the merged
 * read returns identical bytes with far fewer Range requests.
 *
 * Opt in with `ICECHUNK_LIVE_TESTS=1 npx vitest run tests/live`; it is
 * skipped otherwise so CI stays offline and deterministic.
 */

import { describe, it, expect } from "vitest";
import * as zarr from "zarrita";
import {
  HttpStorage,
  IcechunkStore,
  ReadSession,
  Repository,
} from "../../src/index.js";

const ROOT =
  "https://s3.us-west-2.amazonaws.com/us-west-2.opendata.source.coop/englacial/ismip7/icechunk-ais";

const cases = [
  {
    group: "CESM2-WACCM/historical/SDBN1/2000m",
    variable: "acabf",
    chunks: 145,
    maxRequests: 5,
  },
  {
    group: "ACCESS-CM2/historical/ocean/8000m",
    variable: "so",
    chunks: 512,
    maxRequests: 160,
  },
];

interface Capture {
  rangeRequests: number;
  bytes: number;
}

/** Count Range requests and bytes passing through global fetch. */
async function capture<T>(fn: () => Promise<T>): Promise<[T, Capture]> {
  const realFetch = globalThis.fetch;
  const seen: Capture = { rangeRequests: 0, bytes: 0 };
  globalThis.fetch = async (url, init) => {
    const headers = init?.headers as Record<string, string> | undefined;
    const response = await realFetch(url, init);
    if (headers?.Range) {
      seen.rangeRequests++;
      seen.bytes += Number(response.headers.get("content-length") ?? 0);
    }
    return response;
  };
  try {
    return [await fn(), seen];
  } finally {
    globalThis.fetch = realFetch;
  }
}

/**
 * Node's undici may hand a request a keep-alive socket that S3 closed after
 * the previous burst ("UND_ERR_SOCKET other side closed"), or time out
 * connecting on a flaky link. Those are fetch/network artefacts, not reader
 * bugs, so retry such a read once.
 */
const RETRIABLE_CODES = new Set(["UND_ERR_SOCKET", "UND_ERR_CONNECT_TIMEOUT"]);
async function retryOnStaleSocket<T>(fn: () => Promise<T>): Promise<T> {
  try {
    return await fn();
  } catch (error) {
    let cause: unknown = error;
    while (cause instanceof Error) {
      const code = (cause as { code?: string }).code;
      if (code && RETRIABLE_CODES.has(code)) {
        console.log(`${code}; retrying read once`);
        return fn();
      }
      cause = cause.cause;
    }
    throw error;
  }
}

async function readStep0(
  session: ReadSession,
  options: Parameters<typeof IcechunkStore.open>[1],
  group: string,
  variable: string,
) {
  const store = await IcechunkStore.open(session, options);
  const arr = await zarr.open(store.resolve(`/${group}/${variable}`), {
    kind: "array",
  });
  const selection = arr.shape.map((_, i) => (i === 0 ? 0 : null));
  const started = performance.now();
  const [result, seen] = await capture(() =>
    retryOnStaleSocket(() => zarr.get(arr, selection)),
  );
  return {
    data: result.data as ArrayLike<number>,
    seen,
    ms: performance.now() - started,
    stats: store.rangeCoalescer?.stats,
  };
}

describe.skipIf(!process.env.ICECHUNK_LIVE_TESTS)(
  "ISMIP7 live range coalescing",
  () => {
    // One session (repo file, snapshot, manifests) shared by both reads.
    let session: ReadSession | undefined;
    const openSession = async () =>
      (session ??= await (
        await Repository.open({ storage: new HttpStorage(ROOT) })
      ).checkoutBranch("main"));

    for (const c of cases) {
      it(`${c.group}/${c.variable}: ${c.chunks} chunks become at most ${c.maxRequests} requests`, async () => {
        const shared = await openSession();
        const plain = await readStep0(
          shared,
          { coalesce: false },
          c.group,
          c.variable,
        );
        const merged = await readStep0(
          shared,
          { coalesce: c.coalesce ?? {} },
          c.group,
          c.variable,
        );

        console.log(
          `${c.variable}: ${plain.seen.rangeRequests} requests / ${plain.seen.bytes} B / ${plain.ms.toFixed(0)} ms -> ` +
            `${merged.seen.rangeRequests} requests / ${merged.seen.bytes} B / ${merged.ms.toFixed(0)} ms ` +
            `(discarded ${merged.stats?.bytesDiscarded} B)`,
        );

        expect(plain.seen.rangeRequests).toBe(c.chunks);
        expect(merged.seen.rangeRequests).toBeLessThanOrEqual(c.maxRequests);
        expect(merged.stats?.ranges).toBe(c.chunks);
        expect(merged.data.length).toBe(plain.data.length);
        expect(Array.from(merged.data)).toEqual(Array.from(plain.data));
      }, 120_000);
    }
  },
);
