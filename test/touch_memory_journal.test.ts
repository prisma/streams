import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { tableKeyFor, templateIdFor, templateKeyFor, watchKeyFor } from "../src/touch/live_keys";
import { touchKeyIdFromRoutingKey } from "../src/touch/touch_key_id";

function makeConfig(rootDir: string, overrides: Partial<Config> = {}): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    interpreterCheckIntervalMs: 0,
    interpreterWorkers: 1,
    interpreterMaxBatchRows: 1000,
    interpreterMaxBatchBytes: 8 * 1024 * 1024,
    ...overrides,
  };
}

async function fetchJson(url: string, init: RequestInit): Promise<any> {
  const r = await fetch(url, init);
  const text = await r.text();
  if (!r.ok) throw new Error(`HTTP ${r.status} ${url}: ${text}`);
  if (text === "") return null;
  return JSON.parse(text);
}

describe("touch storage=memory (journal cursors)", () => {
  test("touch/meta exposes cursor and touch/wait wakes after a write", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-touch-mem-"));
    let app: ReturnType<typeof createApp> | null = null;
    let server: any | null = null;
    try {
      app = createApp(makeConfig(root), new MockR2Store());
      app.deps.segmenter.stop();
      app.deps.uploader.stop();

      server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      const stream = "state_mem";
      await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      });

      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          interpreter: {
            apiVersion: "durable.streams/stream-interpreter/v1",
            format: "durable.streams/state-protocol/v1",
            touch: { enabled: true },
          },
        }),
      });

      const meta0 = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/meta`, { method: "GET" });
      expect(meta0?.mode).toBe("memory");
      expect(typeof meta0?.cursor).toBe("string");
      expect(meta0.cursor).toMatch(/^[0-9a-f]{16}:[0-9]+$/);

      const entity = "posts";
      const tableKey = tableKeyFor(entity);
      const update = {
        type: entity,
        key: "post:1",
        value: { tenantId: "t1", userId: "456" },
        oldValue: { tenantId: "t1", userId: "123" },
        headers: { operation: "update" },
      };
      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(update),
      });

      app.deps.touch.notify(stream);
      await app.deps.touch.tick();

      const res = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/wait`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          cursor: meta0.cursor,
          keys: [tableKey],
          timeoutMs: 2000,
        }),
      });
      expect(res?.stale).not.toBe(true);
      expect(res?.touched).toBe(true);
      expect(typeof res?.cursor).toBe("string");
      expect(res.cursor).not.toBe(meta0.cursor);
    } finally {
      try {
        server?.stop?.();
      } catch {
        // ignore
      }
      try {
        app?.close?.();
      } catch {
        // ignore
      }
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("epoch mismatch returns stale=true after restart", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-touch-mem-restart-"));
    let app1: ReturnType<typeof createApp> | null = null;
    let app2: ReturnType<typeof createApp> | null = null;
    let server1: any | null = null;
    let server2: any | null = null;
    try {
      app1 = createApp(makeConfig(root), new MockR2Store());
      app1.deps.segmenter.stop();
      app1.deps.uploader.stop();
      server1 = Bun.serve({ port: 0, fetch: app1.fetch });
      const baseUrl1 = `http://localhost:${server1.port}`;

      const stream = "state_mem_restart";
      await fetch(`${baseUrl1}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      });
      await fetchJson(`${baseUrl1}/v1/stream/${encodeURIComponent(stream)}/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          interpreter: {
            apiVersion: "durable.streams/stream-interpreter/v1",
            format: "durable.streams/state-protocol/v1",
            touch: { enabled: true },
          },
        }),
      });

      const meta0 = await fetchJson(`${baseUrl1}/v1/stream/${encodeURIComponent(stream)}/touch/meta`, { method: "GET" });
      expect(meta0?.mode).toBe("memory");
      const oldCursor = String(meta0?.cursor ?? "");
      expect(oldCursor).toMatch(/^[0-9a-f]{16}:[0-9]+$/);

      // /touch read path is not supported in memory mode.
      const read = await fetch(`${baseUrl1}/v1/stream/${encodeURIComponent(stream)}/touch?offset=-1`);
      expect(read.status).toBe(404);

      // "Restart" by creating a new app+server on the same sqlite state.
      server1.stop();
      app1.close();
      server1 = null;
      app1 = null;

      app2 = createApp(makeConfig(root), new MockR2Store());
      app2.deps.segmenter.stop();
      app2.deps.uploader.stop();
      server2 = Bun.serve({ port: 0, fetch: app2.fetch });
      const baseUrl2 = `http://localhost:${server2.port}`;

      const entity = "posts";
      const tableKey = tableKeyFor(entity);
      const res = await fetchJson(`${baseUrl2}/v1/stream/${encodeURIComponent(stream)}/touch/wait`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          cursor: oldCursor,
          keys: [tableKey],
          timeoutMs: 0,
        }),
      });
      expect(res?.stale).toBe(true);
      expect(typeof res?.cursor).toBe("string");
      expect(String(res?.cursor)).not.toBe(oldCursor);
    } finally {
      try {
        server1?.stop?.();
      } catch {
        // ignore
      }
      try {
        server2?.stop?.();
      } catch {
        // ignore
      }
      try {
        app1?.close?.();
      } catch {
        // ignore
      }
      try {
        app2?.close?.();
      } catch {
        // ignore
      }
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("touch/wait accepts keyIds-only in memory mode", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-touch-mem-keyids-"));
    let app: ReturnType<typeof createApp> | null = null;
    let server: any | null = null;
    try {
      app = createApp(makeConfig(root), new MockR2Store());
      app.deps.segmenter.stop();
      app.deps.uploader.stop();

      server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      const stream = "state_mem_keyids";
      await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      });

      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          interpreter: {
            apiVersion: "durable.streams/stream-interpreter/v1",
            format: "durable.streams/state-protocol/v1",
            touch: { enabled: true },
          },
        }),
      });

      const meta0 = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/meta`, { method: "GET" });
      expect(meta0?.mode).toBe("memory");

      const entity = "posts";
      const tableKey = tableKeyFor(entity);
      const tableKeyId = touchKeyIdFromRoutingKey(tableKey);
      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          type: entity,
          key: "post:2",
          value: { tenantId: "t1", userId: "9" },
          oldValue: { tenantId: "t1", userId: "8" },
          headers: { operation: "update" },
        }),
      });

      app.deps.touch.notify(stream);
      await app.deps.touch.tick();

      const res = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/wait`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          cursor: meta0.cursor,
          keyIds: [tableKeyId],
          timeoutMs: 2000,
        }),
      });
      expect(res?.stale).not.toBe(true);
      expect(res?.touched).toBe(true);
      expect(typeof res?.cursor).toBe("string");
    } finally {
      try {
        server?.stop?.();
      } catch {
        // ignore
      }
      try {
        app?.close?.();
      } catch {
        // ignore
      }
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("touch/wait interestMode tracks fine active interests and ignores coarse waits", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-touch-mem-interest-"));
    let app: ReturnType<typeof createApp> | null = null;
    let server: any | null = null;
    try {
      app = createApp(makeConfig(root), new MockR2Store());
      app.deps.segmenter.stop();
      app.deps.uploader.stop();

      server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      const stream = "state_mem_interest";
      await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      });

      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          interpreter: {
            apiVersion: "durable.streams/stream-interpreter/v1",
            format: "durable.streams/state-protocol/v1",
            touch: {
              enabled: true,
              memory: {
                hotKeyTtlMs: 120,
                hotTemplateTtlMs: 120,
              },
            },
          },
        }),
      });

      const tableKey = tableKeyFor("posts");
      const templateId = "aaaaaaaaaaaaaaaa";

      const fineWait = fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/wait`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          cursor: "now",
          timeoutMs: 250,
          keys: [tableKey],
          templateIdsUsed: [templateId],
          interestMode: "fine",
        }),
      });

      await new Promise((r) => setTimeout(r, 40));
      const metaFineActive = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/meta`, { method: "GET" });
      expect(Number(metaFineActive?.hotTemplatesActive ?? 0)).toBeGreaterThanOrEqual(1);
      expect(Number(metaFineActive?.hotFineKeysActive ?? 0)).toBeGreaterThanOrEqual(1);

      const fineRes = await fineWait;
      expect(fineRes?.touched).toBe(false);

      const metaFineGrace = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/meta`, { method: "GET" });
      expect(Number(metaFineGrace?.hotTemplatesActive ?? 0)).toBe(0);
      expect(Number(metaFineGrace?.hotTemplatesGrace ?? 0)).toBeGreaterThanOrEqual(1);

      await new Promise((r) => setTimeout(r, 320));
      const metaAfterGrace = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/meta`, { method: "GET" });
      expect(Number(metaAfterGrace?.hotTemplates ?? 0)).toBe(0);
      expect(Number(metaAfterGrace?.hotFineKeys ?? 0)).toBe(0);

      const coarseWait = fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/wait`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          cursor: "now",
          timeoutMs: 250,
          keys: [tableKey],
          templateIdsUsed: [templateId],
          interestMode: "coarse",
        }),
      });

      await new Promise((r) => setTimeout(r, 40));
      const metaCoarse = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/meta`, { method: "GET" });
      expect(Number(metaCoarse?.hotTemplates ?? 0)).toBe(0);
      expect(Number(metaCoarse?.hotFineKeys ?? 0)).toBe(0);

      const coarseRes = await coarseWait;
      expect(coarseRes?.touched).toBe(false);
    } finally {
      try {
        server?.stop?.();
      } catch {
        // ignore
      }
      try {
        app?.close?.();
      } catch {
        // ignore
      }
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("restricted mode emits template touches for fine waiters", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-touch-mem-restricted-"));
    let app: ReturnType<typeof createApp> | null = null;
    let server: any | null = null;
    try {
      app = createApp(
        makeConfig(root, {
          interpreterMaxBatchRows: 1,
        }),
        new MockR2Store()
      );
      app.deps.segmenter.stop();
      app.deps.uploader.stop();

      server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      const stream = "state_mem_restricted";
      await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      });

      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          interpreter: {
            apiVersion: "durable.streams/stream-interpreter/v1",
            format: "durable.streams/state-protocol/v1",
            touch: {
              enabled: true,
              lagDegradeFineTouchesAtSourceOffsets: 1,
              lagRecoverFineTouchesAtSourceOffsets: 0,
              memory: {
                hotKeyTtlMs: 1000,
                hotTemplateTtlMs: 1000,
              },
            },
          },
        }),
      });

      const entity = "public.posts";
      const fields = ["userId"];
      const templateId = templateIdFor(entity, fields);
      const templateKey = templateKeyFor(templateId);

      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/templates/activate`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          templates: [
            {
              entity,
              fields: [{ name: "userId", encoding: "int64" }],
            },
          ],
          inactivityTtlMs: 60_000,
        }),
      });

      const waitPromise = fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/wait`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          cursor: "now",
          timeoutMs: 3000,
          keys: [templateKey],
          templateIdsUsed: [templateId],
          interestMode: "fine",
        }),
      });

      await new Promise((r) => setTimeout(r, 40));

      const rows = Array.from({ length: 20 }, (_, i) => ({
        type: entity,
        key: `post:${i + 1}`,
        value: { userId: i + 1 },
        oldValue: { userId: i + 1000 },
        headers: { operation: "update" },
      }));
      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(rows),
      });

      app.deps.touch.notify(stream);
      await app.deps.touch.tick();

      const res = await waitPromise;
      expect(res?.touched).toBe(true);
      expect(typeof res?.cursor).toBe("string");

      const meta = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/meta`, { method: "GET" });
      expect(String(meta?.touchMode)).toBe("restricted");
    } finally {
      try {
        server?.stop?.();
      } catch {
        // ignore
      }
      try {
        app?.close?.();
      } catch {
        // ignore
      }
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("fine wait started pre-restricted still wakes via template fallback after lag degradation", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-touch-mem-restricted-fallback-"));
    let app: ReturnType<typeof createApp> | null = null;
    let server: any | null = null;
    try {
      app = createApp(
        makeConfig(root, {
          interpreterMaxBatchRows: 1,
        }),
        new MockR2Store()
      );
      app.deps.segmenter.stop();
      app.deps.uploader.stop();

      server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      const stream = "state_mem_restricted_fallback";
      await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      });

      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          interpreter: {
            apiVersion: "durable.streams/stream-interpreter/v1",
            format: "durable.streams/state-protocol/v1",
            touch: {
              enabled: true,
              lagDegradeFineTouchesAtSourceOffsets: 1,
              lagRecoverFineTouchesAtSourceOffsets: 0,
              memory: {
                hotKeyTtlMs: 1000,
                hotTemplateTtlMs: 1000,
              },
            },
          },
        }),
      });

      const entity = "public.posts";
      const fields = ["userId"];
      const templateId = templateIdFor(entity, fields);
      const fineKey = watchKeyFor(templateId, ["42"]);

      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/templates/activate`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          templates: [
            {
              entity,
              fields: [{ name: "userId", encoding: "int64" }],
            },
          ],
          inactivityTtlMs: 60_000,
        }),
      });

      const waitPromise = fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/wait`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          cursor: "now",
          timeoutMs: 3000,
          keys: [fineKey],
          templateIdsUsed: [templateId],
          interestMode: "fine",
        }),
      });

      await new Promise((r) => setTimeout(r, 40));

      // None of these rows touch the exact fine key (userId=42), so this waiter
      // only wakes if restricted-mode template fallback is active.
      const rows = Array.from({ length: 20 }, (_, i) => ({
        type: entity,
        key: `post:${i + 1}`,
        value: { userId: i + 1 },
        oldValue: { userId: i + 1000 },
        headers: { operation: "update" },
      }));
      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(rows),
      });

      app.deps.touch.notify(stream);
      await app.deps.touch.tick();

      const res = await waitPromise;
      expect(res?.touched).toBe(true);
      expect(res?.effectiveWaitKind).toBe("fineKey");

      const meta = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/meta`, { method: "GET" });
      expect(String(meta?.touchMode)).toBe("restricted");
      expect(Number(meta?.waitTouchedTotal ?? 0)).toBeGreaterThan(0);
    } finally {
      try {
        server?.stop?.();
      } catch {
        // ignore
      }
      try {
        app?.close?.();
      } catch {
        // ignore
      }
      rmSync(root, { recursive: true, force: true });
    }
  });
});
