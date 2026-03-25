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
    touchCheckIntervalMs: 0,
    touchWorkers: 1,
    touchMaxBatchRows: 1000,
    touchMaxBatchBytes: 8 * 1024 * 1024,
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

async function installStateProtocolProfile(baseUrl: string, stream: string, touch: Record<string, unknown> = {}): Promise<void> {
  await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_profile`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      apiVersion: "durable.streams/profile/v1",
      profile: {
        kind: "state-protocol",
        touch: {
          enabled: true,
          ...touch,
        },
      },
    }),
  });
}

describe("live touches (state protocol)", () => {
  test("update produces table and template invalidations when a template is active", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-live-"));
    let app: ReturnType<typeof createApp> | null = null;
    let server: any | null = null;
    try {
      app = createApp(makeConfig(root), new MockR2Store());
      app.deps.segmenter.stop();
      app.deps.uploader.stop();

      server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      const stream = "state";
      await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      });
      await installStateProtocolProfile(baseUrl, stream);

      const entity = "posts";
      const fields = ["tenantId", "userId"];
      const templateId = templateIdFor(entity, fields);
      const tableKey = tableKeyFor(entity);
      const beforeKey = watchKeyFor(templateId, ["t1", "123"]);
      const afterKey = watchKeyFor(templateId, ["t1", "456"]);

      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/templates/activate`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          templates: [
            {
              entity,
              fields: fields.map((name) => ({ name, encoding: "string" })),
            },
          ],
          inactivityTtlMs: 60 * 60 * 1000,
        }),
      });

      const tableWaitPromise = fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/wait`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          cursor: "now",
          keys: [tableKey],
          interestMode: "coarse",
          timeoutMs: 2000,
        }),
      });
      const waitForFineKey = (key: string) =>
        fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/wait`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            cursor: "now",
            keys: [key],
            templateIdsUsed: [templateId],
            timeoutMs: 2000,
          }),
        });
      const beforeWaitPromise = waitForFineKey(beforeKey);
      const afterWaitPromise = waitForFineKey(afterKey);

      await new Promise((resolve) => setTimeout(resolve, 40));

      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          type: entity,
          key: "post:1",
          value: { tenantId: "t1", userId: "456" },
          oldValue: { tenantId: "t1", userId: "123" },
          headers: { operation: "update" },
        }),
      });

      app.deps.touch.notify(stream);
      await app.deps.touch.tick();

      const tableWait = await tableWaitPromise;
      expect(tableWait.touched).toBe(true);

      const beforeWait = await beforeWaitPromise;
      expect(beforeWait.touched).toBe(true);
      expect(beforeWait.effectiveWaitKind).toBe("fineKey");

      const afterWait = await afterWaitPromise;
      expect(afterWait.touched).toBe(true);
      expect(afterWait.effectiveWaitKind).toBe("fineKey");
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

  test("onMissingBefore=coarse suppresses template invalidation when update is missing oldValue", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-live-missing-before-"));
    let app: ReturnType<typeof createApp> | null = null;
    let server: any | null = null;
    try {
      app = createApp(makeConfig(root), new MockR2Store());
      app.deps.segmenter.stop();
      app.deps.uploader.stop();

      server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      const stream = "state_missing_before";
      await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      });
      await installStateProtocolProfile(baseUrl, stream, { onMissingBefore: "coarse" });

      const entity = "posts";
      const fields = ["tenantId", "userId"];
      const templateId = templateIdFor(entity, fields);
      const tableKey = tableKeyFor(entity);
      const afterKey = watchKeyFor(templateId, ["t1", "456"]);

      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/templates/activate`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          templates: [
            {
              entity,
              fields: fields.map((name) => ({ name, encoding: "string" })),
            },
          ],
          inactivityTtlMs: 60 * 60 * 1000,
        }),
      });

      const tableWaitPromise = fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/wait`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          cursor: "now",
          keys: [tableKey],
          interestMode: "coarse",
          timeoutMs: 2000,
        }),
      });
      const fineWaitPromise = fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/wait`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          cursor: "now",
          keys: [afterKey],
          templateIdsUsed: [templateId],
          timeoutMs: 2000,
        }),
      });

      await new Promise((resolve) => setTimeout(resolve, 40));

      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          type: entity,
          key: "post:1",
          value: { tenantId: "t1", userId: "456" },
          headers: { operation: "update" },
        }),
      });

      app.deps.touch.notify(stream);
      await app.deps.touch.tick();

      const tableWait = await tableWaitPromise;
      expect(tableWait.touched).toBe(true);

      const fineWait = await fineWaitPromise;
      expect(fineWait.touched).toBe(false);
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

  test("activation boundary prevents backfill template invalidation", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-live-boundary-"));
    let app: ReturnType<typeof createApp> | null = null;
    let server: any | null = null;
    try {
      app = createApp(makeConfig(root), new MockR2Store());
      app.deps.segmenter.stop();
      app.deps.uploader.stop();

      server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      const stream = "state_boundary";
      await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      });
      await installStateProtocolProfile(baseUrl, stream);

      const entity = "posts";
      const fields = ["tenantId", "userId"];
      const templateId = templateIdFor(entity, fields);
      const fineKey = watchKeyFor(templateId, ["t1", "123"]);

      const waitPromise = fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/wait`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          cursor: "now",
          keys: [fineKey],
          templateIdsUsed: [templateId],
          timeoutMs: 2000,
        }),
      });

      await new Promise((resolve) => setTimeout(resolve, 40));

      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          type: entity,
          key: "post:1",
          value: { tenantId: "t1", userId: "123" },
          oldValue: { tenantId: "t1", userId: "000" },
          headers: { operation: "update" },
        }),
      });
      app.deps.touch.notify(stream);
      await app.deps.touch.tick();

      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/templates/activate`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          templates: [
            {
              entity,
              fields: fields.map((name) => ({ name, encoding: "string" })),
            },
          ],
          inactivityTtlMs: 60 * 60 * 1000,
        }),
      });

      const metaAfterActivation = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/meta`, { method: "GET" });

      const oldWait = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/wait`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          cursor: metaAfterActivation.cursor,
          keys: [fineKey],
          timeoutMs: 0,
        }),
      });
      expect(oldWait.touched).toBe(false);

      await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          type: entity,
          key: "post:1",
          value: { tenantId: "t1", userId: "123" },
          oldValue: { tenantId: "t1", userId: "123" },
          headers: { operation: "update" },
        }),
      });
      app.deps.touch.notify(stream);
      await app.deps.touch.tick();

      const newWait = await waitPromise;
      expect(newWait.touched).toBe(true);
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

  test("touch/wait supports large key sets", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-live-wait-keys-"));
    let app: ReturnType<typeof createApp> | null = null;
    let server: any | null = null;
    try {
      app = createApp(makeConfig(root), new MockR2Store());
      app.deps.segmenter.stop();
      app.deps.uploader.stop();

      server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      const stream = "state_wait_many_keys";
      await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      });
      await installStateProtocolProfile(baseUrl, stream);

      const meta0 = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/meta`, { method: "GET" });
      const keys = Array.from({ length: 1000 }, (_, i) => `${i.toString(16).padStart(16, "0")}`);
      const res = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/wait`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          cursor: meta0.cursor,
          keys,
          timeoutMs: 0,
        }),
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
