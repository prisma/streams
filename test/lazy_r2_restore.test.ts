import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { createApp } from "../src/app";
import { bootstrapFromR2 } from "../src/bootstrap";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { manifestObjectKey, streamHash16Hex } from "../src/util/stream_paths";

function makeConfig(rootDir: string, overrides: Partial<Config>): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    ...overrides,
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

const seedOverrides: Partial<Config> = {
  segmentMaxBytes: 128,
  segmentMaxIntervalMs: 40,
  segmentCheckIntervalMs: 20,
  uploadIntervalMs: 20,
  uploadConcurrency: 4,
  segmentCacheMaxBytes: 0,
  segmentFooterCacheEntries: 0,
};

const restoreOverrides: Partial<Config> = {
  segmentCacheMaxBytes: 0,
  segmentFooterCacheEntries: 0,
};

/** Seed streams into `store`, each with one or more uploaded segments in R2. */
async function seedStreams(cfg: Config, store: MockR2Store, streams: string[], recordsPerStream: number): Promise<void> {
  const app = createApp(cfg, store);
  try {
    for (const stream of streams) {
      const createRes = await app.fetch(
        new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );
      expect([201, 204]).toContain(createRes.status);
      for (let i = 0; i < recordsPerStream; i++) {
        const r = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ i, stream, pad: "x".repeat(48) }),
          })
        );
        expect(r.status).toBe(204);
      }
    }

    const deadline = Date.now() + 20_000;
    while (Date.now() < deadline) {
      let allUploaded = true;
      for (const stream of streams) {
        const row = app.deps.db.getStream(stream);
        const uploaded = row != null && row.uploaded_through >= row.sealed_through && app.deps.db.countSegmentsForStream(stream) > 0;
        if (!uploaded) {
          allUploaded = false;
          break;
        }
      }
      if (allUploaded) break;
      await sleep(25);
    }
    for (const stream of streams) {
      expect(app.deps.db.countSegmentsForStream(stream)).toBeGreaterThan(0);
    }
  } finally {
    await app.close();
  }
}

/** Read a whole stream from the beginning and return the response body text. */
async function readWholeStream(app: ReturnType<typeof createApp>, stream: string): Promise<{ status: number; body: string }> {
  const res = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(stream)}?offset=-1&format=json`, { method: "GET" })
  );
  return { status: res.status, body: await res.text() };
}

/** How many of `streams` currently have a hydrated row in local SQLite. */
function hydratedCount(app: ReturnType<typeof createApp>, streams: string[]): number {
  return streams.filter((s) => app.deps.db.getStream(s) != null).length;
}

describe("lazy R2 restore", () => {
  let srcRoot: string;
  let dstRoot: string;

  beforeEach(() => {
    srcRoot = mkdtempSync(join(tmpdir(), "ds-lazy-src-"));
    dstRoot = mkdtempSync(join(tmpdir(), "ds-lazy-dst-"));
  });

  afterEach(() => {
    rmSync(srcRoot, { recursive: true, force: true });
    rmSync(dstRoot, { recursive: true, force: true });
  });

  test(
    "boots without eager restore and hydrates a cold stream on read with byte-identical records",
    async () => {
      const store = new MockR2Store();
      const streams = ["alpha", "beta", "gamma"];
      await seedStreams(makeConfig(srcRoot, seedOverrides), store, streams, 6);

      // Reference: eager-restored node reads every stream.
      const eagerRoot = mkdtempSync(join(tmpdir(), "ds-lazy-eager-"));
      await bootstrapFromR2(makeConfig(eagerRoot, restoreOverrides), store, { clearLocal: true });
      const eagerApp = createApp(makeConfig(eagerRoot, restoreOverrides), store);
      const eagerBodies = new Map<string, string>();
      try {
        for (const stream of streams) {
          const { status, body } = await readWholeStream(eagerApp, stream);
          expect(status).toBe(200);
          eagerBodies.set(stream, body);
        }
      } finally {
        await eagerApp.close();
        rmSync(eagerRoot, { recursive: true, force: true });
      }

      // Lazy node: eager restore skipped, SQLite starts empty.
      const lazyApp = createApp(makeConfig(dstRoot, { lazyRestore: true, ...restoreOverrides }), store);
      try {
        expect(hydratedCount(lazyApp, streams)).toBe(0);

        const cold = await readWholeStream(lazyApp, "beta");
        expect(cold.status).toBe(200);
        expect(cold.body).toBe(eagerBodies.get("beta"));

        // Only the one read stream was hydrated into SQLite.
        expect(hydratedCount(lazyApp, streams)).toBe(1);
        expect(lazyApp.deps.db.getStream("beta")).not.toBeNull();
        expect(lazyApp.deps.db.getStream("alpha")).toBeNull();
      } finally {
        await lazyApp.close();
      }
    },
    60_000
  );

  test(
    "a never-written stream returns 404 in lazy mode",
    async () => {
      const store = new MockR2Store();
      await seedStreams(makeConfig(srcRoot, seedOverrides), store, ["present"], 4);

      const lazyApp = createApp(makeConfig(dstRoot, { lazyRestore: true, ...restoreOverrides }), store);
      try {
        const missing = await readWholeStream(lazyApp, "never-written");
        expect(missing.status).toBe(404);
        expect(lazyApp.deps.db.getStream("never-written")).toBeNull();

        const present = await readWholeStream(lazyApp, "present");
        expect(present.status).toBe(200);
      } finally {
        await lazyApp.close();
      }
    },
    60_000
  );

  test(
    "reading K of N cold streams hydrates ~K SQLite rows, not N",
    async () => {
      const store = new MockR2Store();
      const streams = Array.from({ length: 8 }, (_, i) => `stream-${i}`);
      await seedStreams(makeConfig(srcRoot, seedOverrides), store, streams, 4);

      const lazyApp = createApp(makeConfig(dstRoot, { lazyRestore: true, ...restoreOverrides }), store);
      try {
        expect(hydratedCount(lazyApp, streams)).toBe(0);

        const readStreams = streams.slice(0, 3);
        for (const stream of readStreams) {
          const { status } = await readWholeStream(lazyApp, stream);
          expect(status).toBe(200);
        }

        expect(hydratedCount(lazyApp, streams)).toBe(readStreams.length);
        for (let i = readStreams.length; i < streams.length; i++) {
          expect(lazyApp.deps.db.getStream(streams[i])).toBeNull();
        }
      } finally {
        await lazyApp.close();
      }
    },
    60_000
  );

  test(
    "concurrent reads of the same cold stream trigger a single hydration",
    async () => {
      const store = new MockR2Store();
      await seedStreams(makeConfig(srcRoot, seedOverrides), store, ["hot"], 6);

      // Slow the object store so all concurrent readers arrive before the first
      // hydration settles, exercising the single-flight collapse.
      store.setDelays({ getDelayMs: 40, headDelayMs: 40, listDelayMs: 40 });

      const lazyApp = createApp(makeConfig(dstRoot, { lazyRestore: true, ...restoreOverrides }), store);
      try {
        store.resetStats();
        const manifestKey = manifestObjectKey(streamHash16Hex("hot"));
        expect(store.getCountFor(manifestKey)).toBe(0);

        const results = await Promise.all(
          Array.from({ length: 12 }, () => readWholeStream(lazyApp, "hot"))
        );
        for (const { status } of results) expect(status).toBe(200);

        // Single-flight: the manifest fetch that drives hydration fires exactly
        // once for the whole burst, not once per concurrent reader.
        expect(store.getCountFor(manifestKey)).toBe(1);
        expect(lazyApp.deps.db.getStream("hot")).not.toBeNull();
      } finally {
        await lazyApp.close();
      }
    },
    60_000
  );
});
