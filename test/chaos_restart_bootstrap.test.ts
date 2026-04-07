import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import { createApp } from "../src/app";
import { bootstrapFromR2 } from "../src/bootstrap";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { readU64LE } from "../src/util/endian";
import { manifestObjectKey, schemaObjectKey, segmentObjectKey, streamHash16Hex } from "../src/util/stream_paths";
import { retry } from "../src/util/retry";

function makeConfig(rootDir: string, overrides: Partial<Config> = {}): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    segmentMaxBytes: 256,
    segmentCheckIntervalMs: 20,
    uploadIntervalMs: 20,
    uploadConcurrency: 2,
    ingestFlushIntervalMs: 5,
    indexL0SpanSegments: 0,
    indexCheckIntervalMs: 100_000,
    touchWorkers: 0,
    touchCheckIntervalMs: 0,
    ...overrides,
  };
}

function rng(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (s * 1664525 + 1013904223) >>> 0;
    return s / 0x100000000;
  };
}

async function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

const OBJECT_RETRY = { retries: 8, baseDelayMs: 5, maxDelayMs: 100, timeoutMs: 1000 };

async function snapshotBootstrapStore(source: MockR2Store, streams: string[]): Promise<{ store: MockR2Store; uploadedCounts: Map<string, number> }> {
  // Keep bootstrap fault-injection coverage (get/head/list) while avoiding races
  // against late writes in the original chaos store.
  const store = new MockR2Store({
    getDelayMs: 2,
    headDelayMs: 2,
    listDelayMs: 2,
    failGetEvery: 17,
    timeoutGetEvery: 23,
    failHeadEvery: 19,
    timeoutHeadEvery: 29,
    failListEvery: 31,
    timeoutListEvery: 37,
  });
  const uploadedCounts = new Map<string, number>();

  for (const stream of streams) {
    const shash = streamHash16Hex(stream);
    const manifestKey = manifestObjectKey(shash);
    const manifestBytes = await retry(
      async () => {
        const data = await source.get(manifestKey);
        if (!data) throw new Error(`manifest missing for ${stream}`);
        return data;
      },
      OBJECT_RETRY
    );
    await store.put(manifestKey, manifestBytes);

    const manifest = JSON.parse(new TextDecoder().decode(manifestBytes)) as {
      uploaded_through?: number;
      segment_offsets?: string;
    };
    const prefix = typeof manifest.uploaded_through === "number" ? manifest.uploaded_through : 0;
    if (prefix <= 0 || !manifest.segment_offsets) {
      uploadedCounts.set(stream, 0);
    } else {
      const offsets = zstdDecompressSync(Buffer.from(manifest.segment_offsets, "base64"));
      const offsetPlusOne = readU64LE(offsets, (prefix - 1) * 8);
      uploadedCounts.set(stream, Number(offsetPlusOne));
      for (let i = 0; i < prefix; i++) {
        const segKey = segmentObjectKey(shash, i);
        const segBytes = await retry(
          async () => {
            const data = await source.get(segKey);
            if (!data) throw new Error(`missing segment ${segKey}`);
            return data;
          },
          OBJECT_RETRY
        );
        await store.put(segKey, segBytes);
      }
    }

    const schemaKey = schemaObjectKey(shash);
    const schemaBytes = await retry(async () => source.get(schemaKey), OBJECT_RETRY);
    if (schemaBytes) {
      await store.put(schemaKey, schemaBytes);
    }
  }

  return { store, uploadedCounts };
}

async function readAll(app: ReturnType<typeof createApp>, stream: string): Promise<any[]> {
  let offset = "-1";
  const out: any[] = [];
  for (let i = 0; i < 200; i++) {
    const res = await app.fetch(
      new Request(`http://local/v1/stream/${encodeURIComponent(stream)}?offset=${encodeURIComponent(offset)}`, { method: "GET" })
    );
    expect(res.status).toBe(200);
    const vals = (await res.json()) as any[];
    out.push(...vals);
    const next = res.headers.get("stream-next-offset");
    expect(next).not.toBeNull();
    offset = next!;
    if (res.headers.get("stream-up-to-date") === "true") break;
  }
  return out;
}

describe("chaos restart + bootstrap", () => {
  test(
    "preserves correctness across restarts and bootstrap-from-R2",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-chaos-"));
      const root2 = mkdtempSync(join(tmpdir(), "ds-chaos-bootstrap-"));
      const chaosStore = new MockR2Store({
        putDelayMs: 5,
        getDelayMs: 2,
        headDelayMs: 2,
        listDelayMs: 2,
        failPutEvery: 7,
        timeoutPutEvery: 13,
        failGetEvery: 17,
        timeoutGetEvery: 23,
        failHeadEvery: 19,
        timeoutHeadEvery: 29,
        failListEvery: 31,
        timeoutListEvery: 37,
      });
      const streams = ["alpha", "beta", "gamma", "delta", "epsilon"];
      const expected = new Map<string, any[]>();
      streams.forEach((s) => expected.set(s, []));

      const cfg = makeConfig(root, {
        indexL0SpanSegments: 4,
        indexCheckIntervalMs: 50,
        indexBuilders: 2,
        indexCompactionFanout: 4,
        indexMaxLevel: 2,
      });
      let app = createApp(cfg, chaosStore);

      try {
        for (const s of streams) {
          const res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(s)}`, {
              method: "PUT",
              headers: { "content-type": "application/json" },
            })
          );
          expect([201, 204]).toContain(res.status);
        }

        const rand = rng(1337);
        const ops = 1000;
        for (let i = 0; i < ops; i++) {
          const stream = streams[Math.floor(rand() * streams.length)];
          const value = { i, stream, tag: `v${i}` };
          const res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify(value),
            })
          );
          expect(res.status).toBe(204);
          expected.get(stream)!.push(value);

          if (rand() < 0.2) {
            app.close();
            app = createApp(cfg, chaosStore);
          }
          if (rand() < 0.3) await sleep(5);
        }

        // Give background work time to seal/upload and publish manifests.
        const deadline = Date.now() + 25_000;
        while (Date.now() < deadline) {
          const pending = app.deps.db.countPendingSegments();
          if (pending === 0) break;
          await sleep(50);
        }
        for (const s of streams) {
          for (let attempt = 0; attempt < 5; attempt++) {
            try {
              await app.deps.uploader.publishManifest(s);
              break;
            } catch {
              await sleep(50);
            }
          }
        }

        // Correctness with local WAL + segments.
        for (const s of streams) {
          const values = await readAll(app, s);
          expect(values).toEqual(expected.get(s));
        }

        app.close();

        // Snapshot a self-consistent bootstrap source-of-truth from manifests.
        const { store: bootstrapStore, uploadedCounts } = await snapshotBootstrapStore(chaosStore, streams);

        const cfg2 = makeConfig(root2, {
          indexL0SpanSegments: 4,
          indexCheckIntervalMs: 50,
          indexBuilders: 2,
          indexCompactionFanout: 4,
          indexMaxLevel: 2,
        });
        await bootstrapFromR2(cfg2, bootstrapStore, { clearLocal: true });
        const app2 = createApp(cfg2, bootstrapStore);
        try {
          for (const s of streams) {
            const values = await readAll(app2, s);
            const prefix = expected.get(s)!.slice(0, uploadedCounts.get(s)!);
            expect(values).toEqual(prefix);
            const meta = app2.deps.db.getSegmentMeta(s);
            expect(meta).not.toBeNull();
          }
        } finally {
          app2.close();
        }
      } finally {
        try {
          app.close();
        } catch {
          // ignore
        }
        rmSync(root, { recursive: true, force: true });
        rmSync(root2, { recursive: true, force: true });
      }
    },
    90_000
  );
});
