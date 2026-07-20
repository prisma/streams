import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import { createApp } from "../src/app";
import { bootstrapFromR2 } from "../src/bootstrap";
import { loadConfig, type Config } from "../src/config";
import { SqliteDurableStore } from "../src/db/db";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { streamHash16Hex } from "../src/util/stream_paths";

/**
 * Reproduces the blocking eager-bootstrap wall that motivates lazy restore.
 *
 * On boot with `--bootstrap-from-r2` the server rebuilds the entire segment/stream
 * index from R2 before it serves `/health`: LIST once, then per stream GET+HEAD the
 * manifest, HEAD every segment, and GET the schema. That cost is
 * `(#streams · per-stream-ops + #segments · HEAD) × object-store-round-trip-latency`
 * and grows with the whole R2 backlog. With realistic R2 round-trips (~25 ms) it
 * crosses the 60 s deploy health gate well before the backlog is large, which is
 * what rolls the deploy back on Compute.
 *
 * The harness seeds a small authentic corpus through the real append→segment→upload
 * path (so the manifest/segment layout is genuine), replicates those R2 objects
 * under distinct stream names to reach N, injects a fixed per-op latency to model R2
 * round-trips, and times `bootstrapFromR2` at N = 100 / 1k / 5k.
 *
 * It is opt-in (like `test:large-index-filter`) because at production-like latency
 * it deliberately runs for minutes. Enable with `DS_BOOTSTRAP_SCALING=1`; override
 * the modeled per-op latency with `DS_BOOTSTRAP_SCALING_DELAY_MS` (default 25). The
 * printed table always projects the measured op count to a 25 ms round-trip, which
 * is the number cited as the production wall.
 */

const SCALING_ENABLED = process.env.DS_BOOTSTRAP_SCALING === "1";
const MODELED_R2_ROUND_TRIP_MS = 25;
const measuredDelayMs = process.env.DS_BOOTSTRAP_SCALING_DELAY_MS
  ? Number(process.env.DS_BOOTSTRAP_SCALING_DELAY_MS)
  : MODELED_R2_ROUND_TRIP_MS;

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

/** Seed one authentic base stream with `>= minSegments` uploaded segments in R2. */
async function seedBaseStream(store: MockR2Store, baseStream: string, minSegments: number): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "ds-scale-base-"));
  const cfg = makeConfig(root, {
    segmentMaxBytes: 200,
    segmentMaxIntervalMs: 30,
    segmentCheckIntervalMs: 15,
    uploadIntervalMs: 15,
    uploadConcurrency: 8,
    segmentCacheMaxBytes: 0,
    segmentFooterCacheEntries: 0,
  });
  const app = createApp(cfg, store);
  try {
    await app.fetch(
      new Request(`http://local/v1/stream/${encodeURIComponent(baseStream)}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      })
    );
    for (let i = 0; i < minSegments * 3; i++) {
      const r = await app.fetch(
        new Request(`http://local/v1/stream/${encodeURIComponent(baseStream)}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ i, pad: "z".repeat(96) }),
        })
      );
      expect(r.status).toBe(204);
    }
    const deadline = Date.now() + 20_000;
    while (Date.now() < deadline) {
      const row = app.deps.db.getStream(baseStream);
      const segs = app.deps.db.countSegmentsForStream(baseStream);
      if (row && row.uploaded_through >= row.sealed_through && segs >= minSegments) break;
      await sleep(25);
    }
    expect(app.deps.db.countSegmentsForStream(baseStream)).toBeGreaterThanOrEqual(minSegments);
  } finally {
    await app.close();
    rmSync(root, { recursive: true, force: true });
  }
}

/**
 * Replicate the base stream's authentic R2 objects (manifest + segments) under a
 * fresh stream name. The manifest `name` field is rewritten so bootstrap restores a
 * genuine, distinct stream. Object bytes and key layout are otherwise identical to a
 * stream that was actually appended, segmented, and uploaded, so the restore drives
 * exactly the LIST/GET/HEAD pattern a real backlog would.
 */
async function cloneStream(store: MockR2Store, baseStream: string, targetStream: string): Promise<void> {
  const baseHash = streamHash16Hex(baseStream);
  const targetHash = streamHash16Hex(targetStream);
  const keys = await store.list(`streams/${baseHash}/`);
  for (const key of keys) {
    const bytes = await store.get(key);
    if (!bytes) continue;
    const targetKey = key.replace(`streams/${baseHash}/`, `streams/${targetHash}/`);
    if (key.endsWith("/manifest.json")) {
      const manifest = JSON.parse(new TextDecoder().decode(bytes));
      manifest.name = targetStream;
      const rewritten = new TextEncoder().encode(JSON.stringify(manifest));
      await store.put(targetKey, rewritten, { contentType: "application/json", contentLength: rewritten.byteLength });
    } else {
      await store.put(targetKey, bytes, { contentLength: bytes.byteLength });
    }
  }
}

type Row = {
  streams: number;
  segments: number;
  objects: number;
  ops: number;
  measuredDelayMs: number;
  measuredRestoreMs: number;
  projected25msMs: number;
};

function formatCurve(rows: Row[]): string {
  const header = "streams | segments | r2 objects | store ops | @" + measuredDelayMs + "ms restore | @25ms projected";
  const body = rows
    .map(
      (r) =>
        `${String(r.streams).padStart(7)} | ${String(r.segments).padStart(8)} | ${String(r.objects).padStart(10)} | ${String(
          r.ops
        ).padStart(9)} | ${(String(Math.round(r.measuredRestoreMs)) + "ms").padStart(String("@" + measuredDelayMs + "ms restore").length)} | ${(
          (r.projected25msMs / 1000).toFixed(1) + "s"
        ).padStart(16)}`
    )
    .join("\n");
  return `\n${header}\n${body}\n`;
}

describe.skipIf(!SCALING_ENABLED)("eager bootstrap restore scaling", () => {
  test(
    "eager bootstrapFromR2 wall-clock scales with backlog and crosses the 60s deploy gate",
    async () => {
      const store = new MockR2Store();
      const baseStream = "__scale_base__";
      await seedBaseStream(store, baseStream, 1);

      const baseHash = streamHash16Hex(baseStream);
      const baseKeys = await store.list(`streams/${baseHash}/`);
      const segmentsPerStream = baseKeys.filter((k) => k.includes("/segments/")).length;
      expect(segmentsPerStream).toBeGreaterThanOrEqual(1);

      const sizes = [100, 1000, 5000];
      const rows: Row[] = [];
      let built = 0;

      for (const target of sizes) {
        for (let i = built; i < target; i++) {
          await cloneStream(store, baseStream, `svc-${i}`);
        }
        built = target;

        const restoreRoot = mkdtempSync(join(tmpdir(), "ds-scale-restore-"));
        const cfg = makeConfig(restoreRoot, { segmentCacheMaxBytes: 0, segmentFooterCacheEntries: 0 });
        store.setDelays({ getDelayMs: measuredDelayMs, headDelayMs: measuredDelayMs, listDelayMs: measuredDelayMs });
        store.resetStats();

        const start = Date.now();
        await bootstrapFromR2(cfg, store, { clearLocal: true });
        const measuredRestoreMs = Date.now() - start;

        const opsStats = store.stats();
        const ops = opsStats.gets + opsStats.heads + opsStats.lists;
        store.setDelays({ getDelayMs: 0, headDelayMs: 0, listDelayMs: 0 });

        const verifyDb = new SqliteDurableStore(cfg.dbPath);
        try {
          expect(verifyDb.getStream("svc-0")).not.toBeNull();
          expect(verifyDb.getStream(`svc-${target - 1}`)).not.toBeNull();
        } finally {
          verifyDb.close();
        }
        rmSync(restoreRoot, { recursive: true, force: true });

        rows.push({
          streams: target,
          segments: target * segmentsPerStream,
          objects: store.size(),
          ops,
          measuredDelayMs,
          measuredRestoreMs,
          projected25msMs: ops * MODELED_R2_ROUND_TRIP_MS,
        });
      }

      // eslint-disable-next-line no-console
      console.log(`\n[eager-bootstrap restore curve]${formatCurve(rows)}`);

      const at100 = rows.find((r) => r.streams === 100)!;
      const at5000 = rows.find((r) => r.streams === 5000)!;
      // Wall-clock is linear in the backlog: ~50x the streams => ~50x the ops/time.
      expect(at5000.ops).toBeGreaterThan(at100.ops * 40);
      expect(at5000.measuredRestoreMs).toBeGreaterThan(at100.measuredRestoreMs * 20);
      // At a realistic 25ms R2 round-trip, the 5000-stream restore blows the 60s gate.
      expect(at5000.projected25msMs).toBeGreaterThan(60_000);
    },
    900_000
  );
});
