import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../../src/app";
import { loadConfig, type Config } from "../../src/config";
import { MockR2Store } from "../../src/objectstore/mock_r2";

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

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForCondition(fn: () => boolean, timeoutMs = 20_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (fn()) return;
    await sleep(20);
  }
  throw new Error("timeout waiting for condition");
}

async function createRoutedJsonStream(app: ReturnType<typeof createApp>, stream: string): Promise<void> {
  let res = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
    })
  );
  if (res.status !== 201) throw new Error(`create failed: ${res.status}`);
  res = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ routingKey: { jsonPointer: "/repo", required: false } }),
    })
  );
  if (res.status !== 200) throw new Error(`schema failed: ${res.status}`);
}

async function appendRepoBatch(app: ReturnType<typeof createApp>, stream: string, repos: string[], paddingRepeat: number): Promise<void> {
  const res = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(
        repos.map((repo, index) => ({
          repo,
          padding: `${repo}-${index}`.repeat(paddingRepeat),
        }))
      ),
    })
  );
  if (res.status !== 204) throw new Error(`append failed: ${res.status} ${await res.text()}`);
}

async function run(): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "ds-routing-lexicon-bench-"));
  const cfg = makeConfig(root, {
    segmentMaxBytes: 64 * 1024,
    blockMaxBytes: 16 * 1024,
    segmentCheckIntervalMs: 5,
    uploadIntervalMs: 5,
    uploadConcurrency: 2,
    indexCheckIntervalMs: 5,
    indexL0SpanSegments: 1,
    indexCompactionFanout: 1_000_000,
    segmentCacheMaxBytes: 0,
    segmentFooterCacheEntries: 0,
    metricsFlushIntervalMs: 0,
  });
  const store = new MockR2Store();
  const app = createApp(cfg, store);

  try {
    const stream = "routing-lexicon-bench";
    await createRoutedJsonStream(app, stream);

    for (let run = 0; run < 18; run += 1) {
      const repos = Array.from({ length: 1800 }, (_, index) => `${String(run).padStart(2, "0")}/repo-${String(index).padStart(5, "0")}`);
      await appendRepoBatch(app, stream, repos, 6);
    }

    await waitForCondition(() => {
      const uploaded = app.deps.db.countUploadedSegments(stream);
      const state = app.deps.db.getLexiconIndexState(stream, "routing_key", "");
      return uploaded >= 18 && (state?.indexed_through ?? 0) >= uploaded;
    });

    app.deps.indexer?.stop();
    store.resetStats();

    const tailRepos = Array.from({ length: 5000 }, (_, index) => `a/tail-${String(index).padStart(5, "0")}`);
    await appendRepoBatch(app, stream, tailRepos, 2);

    const startedAt = performance.now();
    const res = await app.fetch(new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/_routing_keys?limit=20`, { method: "GET" }));
    const totalMs = performance.now() - startedAt;
    const body = await res.json();
    const detailsRes = await app.fetch(new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/_details`, { method: "GET" }));
    const details = await detailsRes.json();

    console.log(
      JSON.stringify(
        {
          status: res.status,
          wall_ms: Number(totalMs.toFixed(2)),
          body,
          lexicon_index_cache_bytes: details.storage?.local_storage?.lexicon_index_cache_bytes,
          object_store_gets: store.stats().gets,
        },
        null,
        2
      )
    );
  } finally {
    app.close();
    rmSync(root, { recursive: true, force: true });
  }
}

await run();
