import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { tableKeyFor, templateIdFor, watchKeyFor } from "../src/touch/live_keys";

function makeConfig(rootDir: string, overrides: Partial<Config> = {}): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    // Disable background polling; tests call tick() manually.
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

describe("live touches v2 (state protocol)", () => {
  test("update produces table touch + before/after template touches when template is active", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-livev2-"));
    let app: ReturnType<typeof createApp> | null = null;
    let server: any | null = null;
    try {
      const cfg = makeConfig(root);
      app = createApp(cfg, new MockR2Store());
      app.deps.segmenter.stop();
      app.deps.uploader.stop();

      server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      const src = "state";
      await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(src)}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      });

      const interpreter = {
        apiVersion: "durable.streams/stream-interpreter/v1",
        format: "durable.streams/state-protocol/v1",
        touch: {
          enabled: true,
          storage: "sqlite",
          // Keep tests bounded/deterministic.
          retention: { maxAgeMs: 60 * 60 * 1000 },
        },
      };

      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(src)}/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ interpreter }),
      });

      const entity = "posts";
      const fieldsSorted = ["tenantId", "userId"];
      const templateId = templateIdFor(entity, fieldsSorted);
      const tableKey = tableKeyFor(entity);
      const beforeKey = watchKeyFor(templateId, ["t1", "123"]);
      const afterKey = watchKeyFor(templateId, ["t1", "456"]);

      // Activate template before any events are appended to avoid backfill ambiguity.
      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(src)}/touch/templates/activate`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          templates: [
            {
              entity,
              fields: fieldsSorted.map((name) => ({ name, encoding: "string" })),
            },
          ],
          inactivityTtlMs: 60 * 60 * 1000,
        }),
      });

      const update = {
        type: entity,
        key: "post:1",
        value: { tenantId: "t1", userId: "456" },
        oldValue: { tenantId: "t1", userId: "123" },
        headers: { operation: "update" },
      };

      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(src)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(update),
      });

      app.deps.touch.notify(src);
      await app.deps.touch.tick();

      const readTouch = async (key: string): Promise<any[]> => {
        const res = await fetch(
          `${baseUrl}/v1/stream/${encodeURIComponent(src)}/touch/pk/${encodeURIComponent(key)}?offset=-1&format=json`
        );
        expect(res.status).toBe(200);
        return (await res.json()) as any[];
      };

      expect(await readTouch(tableKey)).toEqual([{ sourceOffset: "0", entity, kind: "table" }]);
      expect(await readTouch(beforeKey)).toEqual([{ sourceOffset: "0", entity, kind: "template", templateId }]);
      expect(await readTouch(afterKey)).toEqual([{ sourceOffset: "0", entity, kind: "template", templateId }]);

      const meta = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(src)}/touch/meta`, { method: "GET" });
      expect(typeof meta.currentTouchOffset).toBe("string");
      expect(typeof meta.oldestAvailableTouchOffset).toBe("string");

      const wait = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(src)}/touch/wait`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          keys: [tableKey, beforeKey, afterKey],
          sinceTouchOffset: "-1",
          timeoutMs: 0,
        }),
      });
      expect(wait.touched).toBe(true);
      expect(typeof wait.touchOffset).toBe("string");
      expect(typeof wait.currentTouchOffset).toBe("string");
      expect(Array.isArray(wait.touchedKeys)).toBe(true);
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

  test("onMissingBefore=coarse suppresses template touches when update is missing oldValue", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-livev2-missing-before-"));
    let app: ReturnType<typeof createApp> | null = null;
    let server: any | null = null;
    try {
      const cfg = makeConfig(root);
      app = createApp(cfg, new MockR2Store());
      app.deps.segmenter.stop();
      app.deps.uploader.stop();

      server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      const src = "state_missing_before";
      await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(src)}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      });

      const interpreter = {
        apiVersion: "durable.streams/stream-interpreter/v1",
        format: "durable.streams/state-protocol/v1",
        touch: {
          enabled: true,
          storage: "sqlite",
          retention: { maxAgeMs: 60 * 60 * 1000 },
          onMissingBefore: "coarse",
        },
      };

      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(src)}/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ interpreter }),
      });

      const entity = "posts";
      const fieldsSorted = ["tenantId", "userId"];
      const templateId = templateIdFor(entity, fieldsSorted);
      const tableKey = tableKeyFor(entity);
      const afterKey = watchKeyFor(templateId, ["t1", "456"]);

      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(src)}/touch/templates/activate`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          templates: [
            {
              entity,
              fields: fieldsSorted.map((name) => ({ name, encoding: "string" })),
            },
          ],
          inactivityTtlMs: 60 * 60 * 1000,
        }),
      });

      const update = {
        type: entity,
        key: "post:1",
        value: { tenantId: "t1", userId: "456" },
        headers: { operation: "update" },
      };
      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(src)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(update),
      });

      app.deps.touch.notify(src);
      await app.deps.touch.tick();

      const readTouch = async (key: string): Promise<any[]> => {
        const res = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(src)}/touch/pk/${encodeURIComponent(key)}?offset=-1&format=json`);
        expect(res.status).toBe(200);
        return (await res.json()) as any[];
      };

      expect(await readTouch(tableKey)).toEqual([{ sourceOffset: "0", entity, kind: "table" }]);
      expect(await readTouch(afterKey)).toEqual([]);
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

  test("activation boundary prevents backfill template touches", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-livev2-boundary-"));
    let app: ReturnType<typeof createApp> | null = null;
    let server: any | null = null;
    try {
      const cfg = makeConfig(root);
      app = createApp(cfg, new MockR2Store());
      app.deps.segmenter.stop();
      app.deps.uploader.stop();

      server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      const src = "state2";
      await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(src)}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      });

      const interpreter = {
        apiVersion: "durable.streams/stream-interpreter/v1",
        format: "durable.streams/state-protocol/v1",
        touch: {
          enabled: true,
          storage: "sqlite",
          retention: { maxAgeMs: 60 * 60 * 1000 },
        },
      };
      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(src)}/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ interpreter }),
      });

      const entity = "posts";
      const fieldsSorted = ["tenantId", "userId"];
      const templateId = templateIdFor(entity, fieldsSorted);
      const watchKey = watchKeyFor(templateId, ["t1", "123"]);

      // Append an event first (offset 0).
      const update0 = {
        type: entity,
        key: "post:1",
        value: { tenantId: "t1", userId: "123" },
        oldValue: { tenantId: "t1", userId: "000" },
        headers: { operation: "update" },
      };
      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(src)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(update0),
      });

      // Activate template after the event exists; active_from_source_offset should
      // be >= 1 so offset 0 does not produce template touches.
      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(src)}/touch/templates/activate`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          templates: [
            {
              entity,
              fields: fieldsSorted.map((name) => ({ name, encoding: "string" })),
            },
          ],
          inactivityTtlMs: 60 * 60 * 1000,
        }),
      });

      app.deps.touch.notify(src);
      await app.deps.touch.tick();

      const readTouch = async (key: string): Promise<any[]> => {
        const res = await fetch(
          `${baseUrl}/v1/stream/${encodeURIComponent(src)}/touch/pk/${encodeURIComponent(key)}?offset=-1&format=json`
        );
        expect(res.status).toBe(200);
        return (await res.json()) as any[];
      };

      expect(await readTouch(watchKey)).toEqual([]);

      // Append a second event (offset 1) which should generate a template touch.
      const update1 = {
        type: entity,
        key: "post:1",
        value: { tenantId: "t1", userId: "123" },
        oldValue: { tenantId: "t1", userId: "123" },
        headers: { operation: "update" },
      };
      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(src)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(update1),
      });

      app.deps.touch.notify(src);
      await app.deps.touch.tick();

      expect(await readTouch(watchKey)).toEqual([{ sourceOffset: "1", entity, kind: "template", templateId }]);
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

  test("touch retention makes offsets stale", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-livev2-ret-"));
    let app: ReturnType<typeof createApp> | null = null;
    let server: any | null = null;
    try {
      const cfg = makeConfig(root);
      app = createApp(cfg, new MockR2Store());
      app.deps.segmenter.stop();
      app.deps.uploader.stop();

      server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      const src = "state_ret";
      await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(src)}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      });

      const interpreter = {
        apiVersion: "durable.streams/stream-interpreter/v1",
        format: "durable.streams/state-protocol/v1",
        touch: {
          enabled: true,
          storage: "sqlite",
          // Trim aggressively.
          retention: { maxAgeMs: 0 },
        },
      };

      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(src)}/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ interpreter }),
      });

      // Two different entities => two different table keys => at least two touch rows.
      const update1 = {
        type: "comments",
        key: "comment:1",
        value: { tenantId: "t1", userId: "777" },
        oldValue: { tenantId: "t1", userId: "666" },
        headers: { operation: "update" },
      };
      const update2 = {
        type: "posts",
        key: "post:1",
        value: { tenantId: "t1", userId: "456" },
        oldValue: { tenantId: "t1", userId: "123" },
        headers: { operation: "update" },
      };
      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(src)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify([update1, update2]),
      });

      app.deps.touch.notify(src);
      await app.deps.touch.tick();

      const meta = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(src)}/touch/meta`, { method: "GET" });
      expect(meta.currentTouchOffset).not.toBe(meta.oldestAvailableTouchOffset);

      const postsTableKey = tableKeyFor("posts");
      const wait = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(src)}/touch/wait`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ keys: [postsTableKey], sinceTouchOffset: "-1", timeoutMs: 0 }),
      });
      expect(wait.stale).toBe(true);
      expect(wait.currentTouchOffset).toBe(meta.currentTouchOffset);
      expect(wait.oldestAvailableTouchOffset).toBe(meta.oldestAvailableTouchOffset);
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

  test("touch/wait supports large key sets without hitting SQLite variable limits", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-livev2-wait-keys-"));
    let app: ReturnType<typeof createApp> | null = null;
    let server: any | null = null;
    try {
      const cfg = makeConfig(root);
      app = createApp(cfg, new MockR2Store());
      app.deps.segmenter.stop();
      app.deps.uploader.stop();

      server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      const src = "state_wait_many_keys";
      await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(src)}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      });

      const interpreter = {
        apiVersion: "durable.streams/stream-interpreter/v1",
        format: "durable.streams/state-protocol/v1",
        touch: { enabled: true, storage: "sqlite", retention: { maxAgeMs: 60 * 60 * 1000 } },
      };
      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(src)}/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ interpreter }),
      });

      // Append a single change so the touch stream has at least one row.
      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(src)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ type: "posts", key: "post:1", value: { id: "post:1" }, headers: { operation: "insert" } }),
      });
      app.deps.touch.notify(src);
      await app.deps.touch.tick();

      // 1000 keys + 3 fixed SQL params would exceed common SQLite variable limits
      // if implemented as a single `IN (...)` query.
      const keys = Array.from({ length: 1000 }, (_, i) => `${i.toString(16).padStart(16, "0")}`);
      const res = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(src)}/touch/wait`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ keys, sinceTouchOffset: "-1", timeoutMs: 0 }),
      });
      expect(res.touched).toBe(false);
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
