import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import { Result } from "better-result";
import { createApp } from "../src/app";
import { bootstrapFromR2 } from "../src/bootstrap";
import { loadConfig, type Config } from "../src/config";
import { SqliteDurableStore } from "../src/db/db";
import { MockR2Store, type MockR2Faults } from "../src/objectstore/mock_r2";
import { manifestObjectKey, streamHash16Hex } from "../src/util/stream_paths";

const METRICS_STREAM = "__stream_metrics__";

function makeConfig(rootDir: string, overrides: Partial<Config>): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    segmentMaxBytes: 128,
    segmentCheckIntervalMs: 25,
    uploadIntervalMs: 25,
    uploadConcurrency: 2,
    segmentCacheMaxBytes: 0,
    segmentFooterCacheEntries: 0,
    expirySweepIntervalMs: 0,
    retentionScanIntervalMs: 0,
    objectStoreRetries: 0,
    objectStoreBaseDelayMs: 5,
    ...overrides,
  };
}

function streamUrl(stream: string, suffix = ""): string {
  return `http://local/v1/stream/${encodeURIComponent(stream)}${suffix}`;
}

function prefixOf(stream: string): string {
  return `streams/${streamHash16Hex(stream)}/`;
}

function manifestKeyOf(stream: string): string {
  return manifestObjectKey(streamHash16Hex(stream));
}

async function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

async function until(cond: () => boolean | Promise<boolean>, timeoutMs = 15_000, stepMs = 25): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await cond()) return true;
    if (Date.now() >= deadline) return false;
    await sleep(stepMs);
  }
}

type SeededApp = Awaited<ReturnType<typeof createApp>>;

async function seedUploadedStream(
  app: SeededApp,
  store: MockR2Store,
  stream: string,
  opts: { records?: number; ttl?: string } = {}
): Promise<void> {
  const records = opts.records ?? 6;
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (opts.ttl) headers["stream-ttl"] = opts.ttl;
  const createRes = await app.fetch(new Request(streamUrl(stream), { method: "PUT", headers }));
  expect([200, 201, 204]).toContain(createRes.status);
  for (let i = 0; i < records; i++) {
    const r = await app.fetch(
      new Request(streamUrl(stream), {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ i, pad: "x".repeat(48) }),
      })
    );
    expect(r.status).toBe(204);
  }
  const uploaded = await until(() => {
    const row = app.deps.db.getStream(stream);
    return (
      !!row &&
      row.uploaded_through >= row.sealed_through &&
      app.deps.db.countSegmentsForStream(stream) > 0 &&
      store.has(manifestKeyOf(stream))
    );
  });
  expect(uploaded).toBe(true);
}

describe("retention reaper", () => {
  test(
    "expiry sweep reaps every object and hard-deletes the row",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-reap-expiry-"));
      const store = new MockR2Store();
      const app = createApp(makeConfig(root, { expirySweepIntervalMs: 50 }), store);
      const stream = "expires";
      try {
        await seedUploadedStream(app, store, stream, { ttl: "2" });
        expect((await store.list(prefixOf(stream))).length).toBeGreaterThan(1);

        const reaped = await until(
          async () => (await store.list(prefixOf(stream))).length === 0 && app.deps.db.getStream(stream) === null,
          20_000
        );
        expect(reaped).toBe(true);
        expect(existsSync(`${root}/local/streams/${streamHash16Hex(stream)}`)).toBe(false);
      } finally {
        await app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "DELETE tombstones then the background reap empties the prefix",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-reap-delete-"));
      const store = new MockR2Store();
      const app = createApp(makeConfig(root, { expirySweepIntervalMs: 50 }), store);
      const stream = "deleted";
      try {
        await seedUploadedStream(app, store, stream);
        const delRes = await app.fetch(new Request(streamUrl(stream), { method: "DELETE" }));
        expect(delRes.status).toBe(204);

        const reaped = await until(
          async () => (await store.list(prefixOf(stream))).length === 0 && app.deps.db.getStream(stream) === null,
          20_000
        );
        expect(reaped).toBe(true);

        // Reap deletes flow through the accounting wrapper like any other op.
        const counted = app.deps.db.db
          .query(`SELECT COALESCE(SUM(count), 0) AS c FROM objectstore_request_counts WHERE op='delete';`)
          .get() as { c: number };
        expect(Number(counted.c)).toBeGreaterThan(0);
      } finally {
        await app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "manifest.json outlives every data object during a reap",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-reap-order-"));
      const faults: MockR2Faults = {};
      const store = new MockR2Store({ faults });
      const app = createApp(
        makeConfig(root, { expirySweepIntervalMs: 50, retentionDeleteConcurrency: 1 }),
        store
      );
      const stream = "ordered";
      try {
        await seedUploadedStream(app, store, stream, { records: 10 });
        const mkey = manifestKeyOf(stream);
        faults.deleteDelayMs = 25;

        const delRes = await app.fetch(new Request(streamUrl(stream), { method: "DELETE" }));
        expect(delRes.status).toBe(204);

        let violated = false;
        const emptied = await until(async () => {
          const keys = await store.list(prefixOf(stream));
          if (!keys.includes(mkey) && keys.length > 0) violated = true;
          return keys.length === 0;
        }, 20_000, 5);
        expect(emptied).toBe(true);
        expect(violated).toBe(false);
      } finally {
        await app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "a crashed reap resumes after restore-from-R2 on a fresh root",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-reap-crash-src-"));
      const root2 = mkdtempSync(join(tmpdir(), "ds-reap-crash-dst-"));
      const faults: MockR2Faults = {};
      const store = new MockR2Store({ faults });
      const stream = "crashy";

      const app = createApp(makeConfig(root, { expirySweepIntervalMs: 50 }), store);
      try {
        await seedUploadedStream(app, store, stream, { records: 10 });
        const before = (await store.list(prefixOf(stream))).length;
        expect(before).toBeGreaterThan(2);

        faults.failDeleteEvery = 2;
        const delRes = await app.fetch(new Request(streamUrl(stream), { method: "DELETE" }));
        expect(delRes.status).toBe(204);

        // A partial reap happened (some objects gone) but could not finish.
        const partial = await until(async () => {
          const left = (await store.list(prefixOf(stream))).length;
          return left > 0 && left < before && store.stats().deletes > 0;
        });
        expect(partial).toBe(true);
        expect(app.deps.db.getStream(stream)).not.toBeNull();
      } finally {
        await app.close();
      }

      faults.failDeleteEvery = undefined;
      // Restore-from-R2 must not abort on the half-reaped prefix; the
      // tombstone manifest restores a row that re-arms the reap.
      await bootstrapFromR2(makeConfig(root2, {}), store, { clearLocal: true });
      const app2 = createApp(makeConfig(root2, { expirySweepIntervalMs: 50 }), store);
      try {
        const reaped = await until(
          async () => (await store.list(prefixOf(stream))).length === 0 && app2.deps.db.getStream(stream) === null,
          20_000
        );
        expect(reaped).toBe(true);
      } finally {
        await app2.close();
        rmSync(root, { recursive: true, force: true });
        rmSync(root2, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "bootstrap restores a tombstone manifest without head-checking segments",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-reap-boot-src-"));
      const root2 = mkdtempSync(join(tmpdir(), "ds-reap-boot-dst-"));
      const store = new MockR2Store();
      const stream = "tombstoned";

      const app = createApp(makeConfig(root, {}), store);
      try {
        await seedUploadedStream(app, store, stream);
        // Soft-delete without the HTTP route so no reap is nudged: the data
        // objects stay in place next to a tombstoned manifest.
        app.deps.db.deleteStream(stream);
        await app.deps.uploader.publishManifest(stream);
      } finally {
        await app.close();
      }
      expect((await store.list(prefixOf(stream))).length).toBeGreaterThan(1);

      store.resetStats();
      await bootstrapFromR2(makeConfig(root2, {}), store, { clearLocal: true });
      const metricsManifests = store.has(manifestKeyOf(METRICS_STREAM)) ? 1 : 0;
      expect(store.stats().heads).toBe(metricsManifests);

      const db = new SqliteDurableStore(`${root2}/wal.sqlite`);
      try {
        const row = db.getStream(stream);
        expect(row).not.toBeNull();
        expect(db.isDeleted(row!)).toBe(true);
        expect(db.countSegmentsForStream(stream)).toBe(0);
      } finally {
        db.close();
        rmSync(root, { recursive: true, force: true });
        rmSync(root2, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "PUT recreates a deleted stream on a clean prefix",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-reap-recreate-"));
      const store = new MockR2Store();
      const app = createApp(makeConfig(root, {}), store);
      const stream = "recreated";
      try {
        await seedUploadedStream(app, store, stream, { records: 6 });
        const delRes = await app.fetch(new Request(streamUrl(stream), { method: "DELETE" }));
        expect(delRes.status).toBe(204);

        const putRes = await app.fetch(
          new Request(streamUrl(stream), { method: "PUT", headers: { "content-type": "application/json" } })
        );
        expect(putRes.status).toBe(201);

        const appendRes = await app.fetch(
          new Request(streamUrl(stream), {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ generation: "second" }),
          })
        );
        expect(appendRes.status).toBe(204);

        const readRes = await app.fetch(new Request(`${streamUrl(stream)}?offset=-1`, { method: "GET" }));
        expect(readRes.status).toBe(200);
        const body = await readRes.text();
        expect(body).toContain("second");
        expect(body).not.toContain('"pad"');
      } finally {
        await app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "PUT recreates an expired stream after reaping the old incarnation",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-reap-expired-put-"));
      const store = new MockR2Store();
      // Sweeper off: the expired row is discovered by the PUT itself.
      const app = createApp(makeConfig(root, {}), store);
      const stream = "expired-put";
      try {
        await seedUploadedStream(app, store, stream, { ttl: "1" });
        const oldObjects = (await store.list(prefixOf(stream))).length;
        expect(oldObjects).toBeGreaterThan(1);
        await sleep(1_100);

        const putRes = await app.fetch(
          new Request(streamUrl(stream), { method: "PUT", headers: { "content-type": "application/json" } })
        );
        expect(putRes.status).toBe(201);

        const row = app.deps.db.getStream(stream);
        expect(row).not.toBeNull();
        expect(row!.expires_at_ms).toBeNull();
        expect(row!.next_offset).toBe(0n);
        // Only the fresh incarnation may remain under the prefix.
        const keys = await store.list(prefixOf(stream));
        expect(keys.filter((k) => k.endsWith(".bin"))).toEqual([]);
      } finally {
        await app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "recreate serializes with an in-flight background reap",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-reap-race-"));
      const faults: MockR2Faults = {};
      const store = new MockR2Store({ faults });
      const app = createApp(
        makeConfig(root, { expirySweepIntervalMs: 50, retentionDeleteConcurrency: 1 }),
        store
      );
      const stream = "raced";
      try {
        await seedUploadedStream(app, store, stream, { records: 10 });
        faults.deleteDelayMs = 30;

        const delRes = await app.fetch(new Request(streamUrl(stream), { method: "DELETE" }));
        expect(delRes.status).toBe(204);
        // The nudged background reap is now crawling through the prefix.
        const putRes = await app.fetch(
          new Request(streamUrl(stream), { method: "PUT", headers: { "content-type": "application/json" } })
        );
        expect(putRes.status).toBe(201);
        faults.deleteDelayMs = undefined;

        // Enough appends to seal and upload a segment, so the new incarnation
        // publishes a manifest — which the background reap must not eat.
        for (let i = 0; i < 6; i++) {
          const r = await app.fetch(
            new Request(streamUrl(stream), {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ generation: "second", i, pad: "x".repeat(48) }),
            })
          );
          expect(r.status).toBe(204);
        }
        const healthy = await until(async () => {
          const row = app.deps.db.getStream(stream);
          return (
            !!row &&
            !app.deps.db.isDeleted(row) &&
            row.uploaded_through >= row.sealed_through &&
            app.deps.db.countSegmentsForStream(stream) > 0 &&
            store.has(manifestKeyOf(stream))
          );
        });
        expect(healthy).toBe(true);
        const readRes = await app.fetch(new Request(`${streamUrl(stream)}?offset=-1`, { method: "GET" }));
        expect(readRes.status).toBe(200);
        expect(await readRes.text()).toContain("second");
      } finally {
        await app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "a late-landing object is swept by the verify pass",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-reap-straggler-"));
      const faults: MockR2Faults = {};
      const store = new MockR2Store({ faults });
      const app = createApp(makeConfig(root, { retentionDeleteConcurrency: 1 }), store);
      const stream = "straggler";
      try {
        await seedUploadedStream(app, store, stream, { records: 10 });
        app.deps.db.deleteStream(stream);
        faults.deleteDelayMs = 40;

        const reapPromise = app.deps.reaper.reapStream(stream);
        const midReap = await until(() => store.stats().deletes >= 1, 10_000, 5);
        expect(midReap).toBe(true);
        const strayKey = `${prefixOf(stream)}segments/9999999999999999-stray.bin`;
        await store.put(strayKey, new TextEncoder().encode("stray"));

        const res = await reapPromise;
        expect(Result.isError(res)).toBe(false);
        if (!Result.isError(res)) {
          expect(res.value.skipped).toBe(false);
          expect(res.value.listPasses).toBeGreaterThanOrEqual(2);
        }
        expect(await store.list(prefixOf(stream))).toEqual([]);
        expect(app.deps.db.getStream(stream)).toBeNull();
      } finally {
        await app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "delete failures back off and converge once the store recovers",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-reap-backoff-"));
      const faults: MockR2Faults = {};
      const store = new MockR2Store({ faults });
      const app = createApp(makeConfig(root, { expirySweepIntervalMs: 50 }), store);
      const stream = "flaky";
      try {
        await seedUploadedStream(app, store, stream);
        faults.failDeleteEvery = 1;

        const delRes = await app.fetch(new Request(streamUrl(stream), { method: "DELETE" }));
        expect(delRes.status).toBe(204);

        await sleep(1_200);
        // Every reap attempt failed, so the row and prefix survive; the
        // failure tracker keeps the sweeper from hammering the store on
        // every 50ms tick.
        expect(app.deps.db.getStream(stream)).not.toBeNull();
        expect((await store.list(prefixOf(stream))).length).toBeGreaterThan(0);

        faults.failDeleteEvery = undefined;
        const reaped = await until(
          async () => (await store.list(prefixOf(stream))).length === 0 && app.deps.db.getStream(stream) === null,
          20_000
        );
        expect(reaped).toBe(true);
      } finally {
        await app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "a deleted stream's sealed segments stop uploading",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-reap-upload-stop-"));
      const store = new MockR2Store();
      // Uploads effectively off: segments seal locally and stay pending.
      const app = createApp(makeConfig(root, { uploadIntervalMs: 60_000 }), store);
      const stream = "never-uploads";
      try {
        const createRes = await app.fetch(
          new Request(streamUrl(stream), { method: "PUT", headers: { "content-type": "application/json" } })
        );
        expect([200, 201, 204]).toContain(createRes.status);
        for (let i = 0; i < 6; i++) {
          const r = await app.fetch(
            new Request(streamUrl(stream), {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ i, pad: "x".repeat(48) }),
            })
          );
          expect(r.status).toBe(204);
        }
        const sealed = await until(() => app.deps.db.countSegmentsForStream(stream) > 0);
        expect(sealed).toBe(true);
        expect(app.deps.db.pendingUploadHeads(10).map((r) => r.stream)).toContain(stream);

        app.deps.db.deleteStream(stream);
        expect(app.deps.db.pendingUploadHeads(10).map((r) => r.stream)).not.toContain(stream);

        const res = await app.deps.reaper.reapStream(stream);
        expect(Result.isError(res)).toBe(false);
        expect(await store.list(prefixOf(stream))).toEqual([]);
        expect(app.deps.db.getStream(stream)).toBeNull();
        expect(existsSync(`${root}/local/streams/${streamHash16Hex(stream)}`)).toBe(false);
      } finally {
        await app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "the retention scan reaps doomed manifests with no local row",
    async () => {
      const rootA = mkdtempSync(join(tmpdir(), "ds-reap-scan-src-"));
      const rootB = mkdtempSync(join(tmpdir(), "ds-reap-scan-dst-"));
      const store = new MockR2Store();
      const stream = "orphaned-by-redeploy";

      const appA = createApp(makeConfig(rootA, {}), store);
      try {
        await seedUploadedStream(appA, store, stream, { ttl: "1" });
      } finally {
        await appA.close();
      }
      await sleep(1_100);
      expect((await store.list(prefixOf(stream))).length).toBeGreaterThan(1);

      // Fresh node, empty SQLite, no eager bootstrap: only the scan can
      // discover the doomed manifest.
      const appB = createApp(
        makeConfig(rootB, { expirySweepIntervalMs: 50, retentionScanIntervalMs: 150 }),
        store
      );
      try {
        expect(appB.deps.db.getStream(stream)).toBeNull();
        const reaped = await until(async () => (await store.list(prefixOf(stream))).length === 0, 20_000);
        expect(reaped).toBe(true);
        expect(appB.deps.db.getStream(stream)).toBeNull();
      } finally {
        await appB.close();
        rmSync(rootA, { recursive: true, force: true });
        rmSync(rootB, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "the internal metrics stream survives sweeping",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-reap-metrics-"));
      const store = new MockR2Store();
      const app = createApp(
        makeConfig(root, { expirySweepIntervalMs: 50, retentionScanIntervalMs: 150 }),
        store
      );
      try {
        await sleep(500);
        const row = app.deps.db.getStream(METRICS_STREAM);
        expect(row).not.toBeNull();
        expect(app.deps.db.isDeleted(row!)).toBe(false);
      } finally {
        await app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );
});
