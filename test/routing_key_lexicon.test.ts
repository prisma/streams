import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { bootstrapFromR2 } from "../src/bootstrap";
import { createApp } from "../src/app";
import { createProfileTestApp, fetchJsonApp, makeProfileTestConfig } from "./profile_test_utils";

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForCondition(fn: () => boolean, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (fn()) return;
    await sleep(25);
  }
  throw new Error("timeout waiting for condition");
}

async function createRoutedJsonStream(app: ReturnType<typeof createApp>, stream: string): Promise<void> {
  const createRes = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
    })
  );
  expect(createRes.status).toBe(201);
  const schemaRes = await fetchJsonApp(app, `http://local/v1/stream/${encodeURIComponent(stream)}/_schema`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      routingKey: { jsonPointer: "/repo", required: false },
    }),
  });
  expect(schemaRes.status).toBe(200);
}

async function appendRepoEvents(app: ReturnType<typeof createApp>, stream: string, repos: string[]): Promise<void> {
  for (const [index, repo] of repos.entries()) {
    const appendRes = await app.fetch(
      new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          repo,
          padding: `${repo}-${index}`.repeat(8),
        }),
      })
    );
    expect(appendRes.status).toBe(204);
  }
}

async function appendRepoBatchEvents(
  app: ReturnType<typeof createApp>,
  stream: string,
  repos: string[],
  paddingRepeat = 8
): Promise<void> {
  const appendRes = await app.fetch(
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
  expect(appendRes.status).toBe(204);
}

describe("routing key lexicon", () => {
  test("lists routing keys completely before the first lexicon run exists", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-routing-lexicon-fallback-"));
    const { app, store } = createProfileTestApp(root, {
      segmentMaxBytes: 256,
      blockMaxBytes: 256,
      segmentCheckIntervalMs: 10,
      uploadIntervalMs: 10,
      uploadConcurrency: 2,
      indexCheckIntervalMs: 10,
      indexL0SpanSegments: 16,
      segmentCacheMaxBytes: 0,
      segmentFooterCacheEntries: 0,
      metricsFlushIntervalMs: 0,
    });
    try {
      const stream = "routing-lexicon-fallback";
      await createRoutedJsonStream(app, stream);
      await appendRepoEvents(app, stream, ["beta/repo", "alpha/repo", "gamma/repo", "alpha/repo"]);
      await waitForCondition(() => app.deps.db.countUploadedSegments(stream) >= 1);

      const res = await fetchJsonApp(app, `http://local/v1/stream/${encodeURIComponent(stream)}/_routing_keys?limit=10`, {
        method: "GET",
      });
      expect(res.status).toBe(200);
      expect(res.body?.keys).toEqual(["alpha/repo", "beta/repo", "gamma/repo"]);
      expect(res.body?.next_after).toBeNull();
      expect(res.body?.coverage?.complete).toBe(true);
      expect(res.body?.coverage?.indexed_segments).toBe(0);
      expect(res.body?.coverage?.scanned_uploaded_segments).toBeGreaterThanOrEqual(1);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("paginates alphabetically from lexicon runs once indexed", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-routing-lexicon-indexed-"));
    const { app } = createProfileTestApp(root, {
      segmentMaxBytes: 220,
      blockMaxBytes: 220,
      segmentCheckIntervalMs: 10,
      uploadIntervalMs: 10,
      uploadConcurrency: 2,
      indexCheckIntervalMs: 10,
      indexL0SpanSegments: 1,
      segmentCacheMaxBytes: 0,
      segmentFooterCacheEntries: 0,
      metricsFlushIntervalMs: 0,
    });
    try {
      const stream = "routing-lexicon-indexed";
      await createRoutedJsonStream(app, stream);
      await appendRepoEvents(app, stream, ["omega/repo", "beta/repo", "alpha/repo", "gamma/repo", "delta/repo"]);
      await waitForCondition(() => {
        const uploaded = app.deps.db.countUploadedSegments(stream);
        const state = app.deps.db.getLexiconIndexState(stream, "routing_key", "");
        return uploaded >= 2 && (state?.indexed_through ?? 0) >= uploaded;
      });

      const first = await fetchJsonApp(
        app,
        `http://local/v1/stream/${encodeURIComponent(stream)}/_routing_keys?limit=2`,
        { method: "GET" }
      );
      expect(first.status).toBe(200);
      expect(first.body?.keys).toEqual(["alpha/repo", "beta/repo"]);
      expect(first.body?.next_after).toBe("beta/repo");
      expect(first.body?.coverage?.indexed_segments).toBeGreaterThanOrEqual(2);
      const details = await fetchJsonApp(app, `http://local/v1/stream/${encodeURIComponent(stream)}/_details`, {
        method: "GET",
      });
      expect(BigInt(details.body?.storage?.local_storage?.lexicon_index_cache_bytes ?? "0")).toBeGreaterThan(0n);

      const second = await fetchJsonApp(
        app,
        `http://local/v1/stream/${encodeURIComponent(stream)}/_routing_keys?limit=2&after=${encodeURIComponent(first.body?.next_after)}`,
        { method: "GET" }
      );
      expect(second.status).toBe(200);
      expect(second.body?.keys).toEqual(["delta/repo", "gamma/repo"]);
      expect(second.body?.next_after).toBe("gamma/repo");
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("hard delete clears old routing-key lexicon state", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-routing-lexicon-recreate-"));
    const { app } = createProfileTestApp(root, {
      segmentMaxBytes: 220,
      blockMaxBytes: 220,
      segmentCheckIntervalMs: 10,
      uploadIntervalMs: 10,
      uploadConcurrency: 2,
      indexCheckIntervalMs: 10,
      indexL0SpanSegments: 1,
      segmentCacheMaxBytes: 0,
      segmentFooterCacheEntries: 0,
      metricsFlushIntervalMs: 0,
    });
    try {
      const stream = "routing-lexicon-recreate";
      await createRoutedJsonStream(app, stream);
      await appendRepoEvents(app, stream, ["old/repo", "stale/repo"]);
      await waitForCondition(() => {
        const uploaded = app.deps.db.countUploadedSegments(stream);
        const state = app.deps.db.getLexiconIndexState(stream, "routing_key", "");
        return uploaded >= 1 && (state?.indexed_through ?? 0) >= uploaded;
      });

      let res = await fetchJsonApp(app, `http://local/v1/stream/${encodeURIComponent(stream)}/_routing_keys?limit=10`, {
        method: "GET",
      });
      expect(res.status).toBe(200);
      expect(res.body?.keys).toContain("old/repo");

      expect(app.deps.db.hardDeleteStream(stream)).toBe(true);
      expect(app.deps.db.getLexiconIndexState(stream, "routing_key", "")).toBeNull();
      expect(app.deps.db.listLexiconIndexRuns(stream, "routing_key", "")).toHaveLength(0);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("serves partial paginable pages from cached lexicon runs without scanning uploaded history", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-routing-lexicon-partial-"));
    const { app, store } = createProfileTestApp(root, {
      segmentMaxBytes: 220,
      blockMaxBytes: 220,
      segmentCheckIntervalMs: 10,
      uploadIntervalMs: 10,
      uploadConcurrency: 2,
      indexCheckIntervalMs: 10,
      indexL0SpanSegments: 1,
      segmentCacheMaxBytes: 0,
      segmentFooterCacheEntries: 0,
      metricsFlushIntervalMs: 0,
    });
    try {
      const stream = "routing-lexicon-partial";
      await createRoutedJsonStream(app, stream);
      await appendRepoEvents(app, stream, ["alpha/repo", "beta/repo", "gamma/repo", "delta/repo"]);
      await waitForCondition(() => {
        const state = app.deps.db.getLexiconIndexState(stream, "routing_key", "");
        return (state?.indexed_through ?? 0) >= 1;
      });

      app.deps.indexer?.stop();
      await sleep(50);
      store.resetStats();

      await appendRepoEvents(app, stream, [
        "epsilon/repo",
        "zeta/repo",
        "eta/repo",
        "theta/repo",
        "iota/repo",
        "kappa/repo",
        "lambda/repo",
        "mu/repo",
      ]);
      await waitForCondition(() => app.deps.db.countUploadedSegments(stream) >= 4);

      const res = await fetchJsonApp(app, `http://local/v1/stream/${encodeURIComponent(stream)}/_routing_keys?limit=2`, {
        method: "GET",
      });
      expect(res.status).toBe(200);
      expect(res.body?.coverage?.complete).toBe(false);
      expect(res.body?.coverage?.indexed_segments).toBeGreaterThanOrEqual(1);
      expect(res.body?.coverage?.scanned_uploaded_segments).toBe(0);
      expect(res.body?.coverage?.possible_missing_uploaded_segments).toBeGreaterThan(0);
      expect(typeof res.body?.next_after).toBe("string");
      expect(res.body?.timing?.lexicon_runs_loaded).toBeGreaterThanOrEqual(1);
      expect(res.body?.timing?.fallback_segment_get_ms).toBe(0);
      expect(Array.isArray(res.body?.keys)).toBe(true);
      expect(res.body?.keys.length).toBeGreaterThan(0);
      expect([...res.body!.keys].sort()).toEqual(res.body?.keys);
      expect(store.stats().gets).toBe(0);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("serves the first page quickly from many active lexicon runs with a large fallback set", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-routing-lexicon-perf-"));
    const rowsPerSegment = 1800;
    const { app } = createProfileTestApp(root, {
      segmentMaxBytes: 64 * 1024,
      segmentTargetRows: rowsPerSegment,
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
    try {
      const stream = "routing-lexicon-perf";
      await createRoutedJsonStream(app, stream);

      for (let run = 0; run < 18; run += 1) {
        const repos = Array.from(
          { length: rowsPerSegment },
          (_, index) => `${String(run).padStart(2, "0")}/repo-${String(index).padStart(5, "0")}`
        );
        await appendRepoBatchEvents(app, stream, repos, 6);
      }

      await waitForCondition(() => {
        const uploaded = app.deps.db.countUploadedSegments(stream);
        const state = app.deps.db.getLexiconIndexState(stream, "routing_key", "");
        return uploaded >= 18 && (state?.indexed_through ?? 0) >= uploaded;
      }, 20_000);

      app.deps.indexer?.stop();
      await appendRepoBatchEvents(
        app,
        stream,
        Array.from({ length: 5000 }, (_, index) => `a/tail-${String(index).padStart(5, "0")}`),
        2
      );

      const res = await fetchJsonApp(app, `http://local/v1/stream/${encodeURIComponent(stream)}/_routing_keys?limit=20`, {
        method: "GET",
      });
      expect(res.status).toBe(200);
      expect(res.body?.keys?.length).toBe(20);
      expect(res.body?.took_ms).toBeLessThan(100);
      expect(res.body?.timing?.lexicon_runs_loaded).toBeGreaterThanOrEqual(18);
      expect(res.body?.timing?.lexicon_enumerate_ms).toBeLessThan(50);
      expect(res.body?.timing?.fallback_wal_scan_ms).toBeLessThan(100);
      expect(res.body?.coverage?.scanned_uploaded_segments).toBe(0);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("restores lexicon runs from manifest bootstrap", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-routing-lexicon-bootstrap-src-"));
    const root2 = mkdtempSync(join(tmpdir(), "ds-routing-lexicon-bootstrap-dst-"));
    const { app, store } = createProfileTestApp(root, {
      segmentMaxBytes: 220,
      blockMaxBytes: 220,
      segmentCheckIntervalMs: 10,
      uploadIntervalMs: 10,
      uploadConcurrency: 2,
      indexCheckIntervalMs: 10,
      indexL0SpanSegments: 1,
      segmentCacheMaxBytes: 0,
      segmentFooterCacheEntries: 0,
      metricsFlushIntervalMs: 0,
    });
    try {
      const stream = "routing-lexicon-bootstrap";
      await createRoutedJsonStream(app, stream);
      await appendRepoEvents(app, stream, [
        "beta/repo",
        "alpha/repo",
        "gamma/repo",
        "alpha/repo",
        "beta/repo",
        "gamma/repo",
      ]);
      await waitForCondition(() => {
        const row = app.deps.db.getStream(stream);
        const uploaded = app.deps.db.countUploadedSegments(stream);
        const state = app.deps.db.getLexiconIndexState(stream, "routing_key", "");
        return (
          !!row &&
          row.pending_bytes === 0n &&
          row.pending_rows === 0n &&
          row.next_offset > 0n &&
          row.uploaded_through >= row.next_offset - 1n &&
          uploaded >= 1 &&
          (state?.indexed_through ?? 0) >= uploaded
        );
      });
      await app.deps.uploader.publishManifest(stream);
    } finally {
      app.close();
    }

    const cfg2 = makeProfileTestConfig(root2, {
      segmentCacheMaxBytes: 0,
      segmentFooterCacheEntries: 0,
      metricsFlushIntervalMs: 0,
    });
    await bootstrapFromR2(cfg2, store, { clearLocal: true });
    const app2 = createApp(cfg2, store);
    try {
      const stream = "routing-lexicon-bootstrap";
      const res = await fetchJsonApp(app2, `http://local/v1/stream/${encodeURIComponent(stream)}/_routing_keys?limit=10`, {
        method: "GET",
      });
      expect(res.status).toBe(200);
      expect(res.body?.keys).toEqual(["alpha/repo", "beta/repo", "gamma/repo"]);

      const indexStatus = await fetchJsonApp(app2, `http://local/v1/stream/${encodeURIComponent(stream)}/_index_status`, {
        method: "GET",
      });
      expect(indexStatus.status).toBe(200);
      expect(indexStatus.body?.routing_key_lexicon?.configured).toBe(true);
      expect(indexStatus.body?.routing_key_lexicon?.active_run_count).toBeGreaterThanOrEqual(1);
    } finally {
      app2.close();
      rmSync(root, { recursive: true, force: true });
      rmSync(root2, { recursive: true, force: true });
    }
  });
});
