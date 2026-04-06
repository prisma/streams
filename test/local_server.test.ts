import { describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { LOCAL_AUTO_TUNE_PRESET_MB } from "../src/local/config";
import { startLocalDurableStreamsServer } from "../src/local/server";
import { getDataDir } from "../src/local/state";

async function withLocalRoot<T>(name: string, fn: () => Promise<T>): Promise<T> {
  const root = mkdtempSync(join(tmpdir(), `ds-local-${name}-`));
  const prev = process.env.DS_LOCAL_DATA_ROOT;
  process.env.DS_LOCAL_DATA_ROOT = root;
  try {
    return await fn();
  } finally {
    if (prev == null) delete process.env.DS_LOCAL_DATA_ROOT;
    else process.env.DS_LOCAL_DATA_ROOT = prev;
    rmSync(root, { recursive: true, force: true });
  }
}

describe("local durable streams server", () => {
  test("start/create/append/read", async () => {
    await withLocalRoot("smoke", async () => {
      const server = await startLocalDurableStreamsServer({ name: "smoke", port: 0 });
      try {
        const baseUrl = server.exports.http.url;

        let res = await fetch(`${baseUrl}/v1/stream/local-smoke`, {
          method: "PUT",
          headers: { "content-type": "text/plain" },
        });
        expect([200, 201]).toContain(res.status);

        res = await fetch(`${baseUrl}/v1/stream/local-smoke`, {
          method: "POST",
          headers: { "content-type": "text/plain" },
          body: "hello-local",
        });
        expect(res.status).toBe(204);

        res = await fetch(`${baseUrl}/v1/stream/local-smoke?offset=-1`);
        expect(res.status).toBe(200);
        expect(await res.text()).toBe("hello-local");
      } finally {
        await server.close();
      }
    });
  });

  test("persists across restart for same server name", async () => {
    await withLocalRoot("persist", async () => {
      const name = "persist";
      const server1 = await startLocalDurableStreamsServer({ name, port: 0 });
      const baseUrl1 = server1.exports.http.url;
      try {
        await fetch(`${baseUrl1}/v1/stream/local-persist`, {
          method: "PUT",
          headers: { "content-type": "text/plain" },
        });
        const append = await fetch(`${baseUrl1}/v1/stream/local-persist`, {
          method: "POST",
          headers: { "content-type": "text/plain" },
          body: "v1",
        });
        expect(append.status).toBe(204);
      } finally {
        await server1.close();
      }

      const server2 = await startLocalDurableStreamsServer({ name, port: 0 });
      const baseUrl2 = server2.exports.http.url;
      try {
        const read = await fetch(`${baseUrl2}/v1/stream/local-persist?offset=-1`);
        expect(read.status).toBe(200);
        expect(await read.text()).toBe("v1");
      } finally {
        await server2.close();
      }
    });
  });

  test("consumer reopen flow can read the current schema registry and skip duplicate schema install", async () => {
    await withLocalRoot("schema-reopen", async () => {
      const name = "schema-reopen";
      const stream = "local-schema-reopen";
      const desiredSchema = {
        type: "object",
        additionalProperties: false,
        required: ["repo"],
        properties: {
          repo: { type: "string" },
        },
      };

      async function ensureInstalledSchema(baseUrl: string): Promise<void> {
        const current = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_schema`, { method: "GET" });
        expect(current.status).toBe(200);
        const registry = await current.json();
        const currentSchema = registry?.schemas?.["1"] ?? null;
        const alreadyMatches =
          registry?.currentVersion === 1 && JSON.stringify(currentSchema) === JSON.stringify(desiredSchema);
        if (alreadyMatches) return;

        const install = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_schema`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ schema: desiredSchema }),
        });
        expect(install.status).toBe(200);
      }

      const server1 = await startLocalDurableStreamsServer({ name, port: 0 });
      try {
        const baseUrl = server1.exports.http.url;
        let res = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
        });
        expect([200, 201]).toContain(res.status);

        await ensureInstalledSchema(baseUrl);

        res = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify([{ repo: "alpha/repo" }]),
        });
        expect(res.status).toBe(204);
      } finally {
        await server1.close();
      }

      const server2 = await startLocalDurableStreamsServer({ name, port: 0 });
      try {
        const baseUrl = server2.exports.http.url;
        await ensureInstalledSchema(baseUrl);

        const append = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify([{ repo: "beta/repo" }]),
        });
        expect(append.status).toBe(204);

        const read = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}?offset=-1&format=json`);
        expect(read.status).toBe(200);
        expect(await read.json()).toEqual([{ repo: "alpha/repo" }, { repo: "beta/repo" }]);
      } finally {
        await server2.close();
      }
    });
  });

  test("does not create segment/cache artifacts in local mode", async () => {
    await withLocalRoot("files", async () => {
      const name = "files";
      const server = await startLocalDurableStreamsServer({ name, port: 0 });
      try {
        const baseUrl = server.exports.http.url;
        await fetch(`${baseUrl}/v1/stream/local-files`, {
          method: "PUT",
          headers: { "content-type": "text/plain" },
        });
        for (let i = 0; i < 20; i++) {
          const res = await fetch(`${baseUrl}/v1/stream/local-files`, {
            method: "POST",
            headers: { "content-type": "text/plain" },
            body: `row-${i}`,
          });
          expect(res.status).toBe(204);
        }
      } finally {
        await server.close();
      }

      const dataDir = getDataDir(name);
      expect(existsSync(join(dataDir, "segments"))).toBe(false);
      expect(existsSync(join(dataDir, "cache"))).toBe(false);

      const files = readdirSync(dataDir);
      expect(files.includes("durable-streams.sqlite")).toBe(true);
      expect(files.includes("server.json")).toBe(true);
      expect(files.includes("server.lock")).toBe(true);

      const segmentFiles = files.filter((f) => f.endsWith(".bin") || f.includes("segments"));
      expect(segmentFiles.length).toBe(0);
    });
  });

  test("reports the fixed local auto-tune preset and supports routing key listing", async () => {
    await withLocalRoot("details", async () => {
      const server = await startLocalDurableStreamsServer({ name: "details", port: 0 });
      try {
        const baseUrl = server.exports.http.url;

        let res = await fetch(`${baseUrl}/v1/server/_details`);
        expect(res.status).toBe(200);
        const serverDetails = await res.json();
        expect(serverDetails.auto_tune).toEqual({
          enabled: true,
          requested_memory_mb: LOCAL_AUTO_TUNE_PRESET_MB,
          preset_mb: LOCAL_AUTO_TUNE_PRESET_MB,
          effective_memory_limit_mb: LOCAL_AUTO_TUNE_PRESET_MB,
        });
        expect(serverDetails.runtime.memory.process).toEqual({
          rss_bytes: expect.any(Number),
          heap_total_bytes: expect.any(Number),
          heap_used_bytes: expect.any(Number),
          external_bytes: expect.any(Number),
          array_buffers_bytes: expect.any(Number),
        });
        expect(serverDetails.runtime.memory.process_breakdown).toEqual(
          expect.objectContaining({
            source: expect.any(String),
            js_managed_bytes: expect.any(Number),
            unattributed_rss_bytes: expect.any(Number),
          })
        );
        expect(serverDetails.runtime.memory.sqlite).toEqual(
          expect.objectContaining({
            available: expect.any(Boolean),
            source: expect.any(String),
            memory_used_bytes: expect.any(Number),
            open_connections: expect.any(Number),
            prepared_statements: expect.any(Number),
          })
        );
        expect(serverDetails.runtime.memory.high_water).toEqual(
          expect.objectContaining({
            process: expect.any(Object),
            runtime_totals: expect.any(Object),
          })
        );
        expect(serverDetails.runtime.memory.subsystems.configured_budgets.sqlite_cache_budget_bytes).toBeGreaterThan(0);

        res = await fetch(`${baseUrl}/v1/stream/local-routing`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
        });
        expect([200, 201]).toContain(res.status);

        res = await fetch(`${baseUrl}/v1/stream/local-routing/_schema`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            routingKey: { jsonPointer: "/repo", required: false },
          }),
        });
        expect(res.status).toBe(200);

        res = await fetch(`${baseUrl}/v1/stream/local-routing`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify([{ repo: "beta/repo" }, { repo: "alpha/repo" }, { repo: "beta/repo" }]),
        });
        expect(res.status).toBe(204);

        res = await fetch(`${baseUrl}/v1/stream/local-routing/_routing_keys?limit=10`);
        expect(res.status).toBe(200);
        const routingKeys = await res.json();
        expect(routingKeys.keys).toEqual(["alpha/repo", "beta/repo"]);
        expect(routingKeys.coverage.complete).toBe(true);
      } finally {
        await server.close();
      }
    });
  });
});
