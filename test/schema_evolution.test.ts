import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";

function makeConfig(rootDir: string, overrides: Partial<Config> = {}): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    segmentCheckIntervalMs: 60_000,
    uploadIntervalMs: 60_000,
    ...overrides,
  };
}

async function withServer<T>(
  overrides: Partial<Config>,
  fn: (ctx: { baseUrl: string }) => Promise<T>
): Promise<T> {
  const root = mkdtempSync(join(tmpdir(), "ds-schema-evo-"));
  const cfg = makeConfig(root, overrides);
  const app = createApp(cfg, new MockR2Store());
  const server = Bun.serve({ port: 0, fetch: app.fetch });
  const baseUrl = `http://localhost:${server.port}`;
  try {
    return await fn({ baseUrl });
  } finally {
    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  }
}

describe("schema evolution", () => {
  test("three versions promote reads and record boundaries", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/schema`, { method: "PUT", headers: { "content-type": "application/json" } });

      const v1 = {
        type: "object",
        additionalProperties: false,
        required: ["id", "assignee"],
        properties: { id: { type: "integer" }, assignee: { type: "string" } },
      };
      const v2 = {
        type: "object",
        additionalProperties: false,
        required: ["id", "assignees"],
        properties: { id: { type: "integer" }, assignees: { type: "array", items: { type: "string" } } },
      };
      const v3 = {
        type: "object",
        additionalProperties: false,
        required: ["id", "assignees", "status"],
        properties: {
          id: { type: "integer" },
          assignees: { type: "array", items: { type: "string" } },
          status: { type: "string", enum: ["todo", "done"] },
        },
      };

      let r = await fetch(`${baseUrl}/v1/stream/schema/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ schema: v1 }),
      });
      expect(r.status).toBe(200);

      r = await fetch(`${baseUrl}/v1/stream/schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify([{ id: 1, assignee: "alice" }]),
      });
      expect(r.status).toBe(204);

      const lens12 = {
        apiVersion: "durable.lens/v1",
        schema: "schema",
        from: 1,
        to: 2,
        ops: [
          { op: "rename", from: "/assignee", to: "/assignees" },
          { op: "wrap", path: "/assignees", mode: "singleton" },
        ],
      };
      r = await fetch(`${baseUrl}/v1/stream/schema/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ schema: v2, lens: lens12 }),
      });
      expect(r.status).toBe(200);

      r = await fetch(`${baseUrl}/v1/stream/schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify([{ id: 2, assignees: ["bob"] }]),
      });
      expect(r.status).toBe(204);

      const lens23 = {
        apiVersion: "durable.lens/v1",
        schema: "schema",
        from: 2,
        to: 3,
        ops: [{ op: "add", path: "/status", schema: { type: "string" }, default: "todo" }],
      };
      r = await fetch(`${baseUrl}/v1/stream/schema/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ schema: v3, lens: lens23 }),
      });
      expect(r.status).toBe(200);

      r = await fetch(`${baseUrl}/v1/stream/schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify([{ id: 3, assignees: ["carol"], status: "done" }]),
      });
      expect(r.status).toBe(204);

      const reg = await (await fetch(`${baseUrl}/v1/stream/schema/_schema`)).json();
      expect(reg.currentVersion).toBe(3);
      expect(reg.boundaries).toEqual([
        { offset: 0, version: 1 },
        { offset: 1, version: 2 },
        { offset: 2, version: 3 },
      ]);

      r = await fetch(`${baseUrl}/v1/stream/schema?offset=-1&format=json`);
      expect(r.status).toBe(200);
      expect(await r.json()).toEqual([
        { id: 1, assignees: ["alice"], status: "todo" },
        { id: 2, assignees: ["bob"], status: "todo" },
        { id: 3, assignees: ["carol"], status: "done" },
      ]);

      r = await fetch(`${baseUrl}/v1/stream/schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify([{ id: 4, assignees: ["dave"] }]),
      });
      expect(r.status).toBe(400);
    });
  });

  test("invalid lens update is rejected", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/badlens`, { method: "PUT", headers: { "content-type": "application/json" } });
      const v1 = {
        type: "object",
        additionalProperties: false,
        required: ["id", "assignee"],
        properties: { id: { type: "integer" }, assignee: { type: "string" } },
      };
      const v2 = {
        type: "object",
        additionalProperties: false,
        required: ["id", "assignees"],
        properties: { id: { type: "integer" }, assignees: { type: "array", items: { type: "string" } } },
      };

      let r = await fetch(`${baseUrl}/v1/stream/badlens/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ schema: v1 }),
      });
      expect(r.status).toBe(200);

      r = await fetch(`${baseUrl}/v1/stream/badlens`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify([{ id: 1, assignee: "alice" }]),
      });
      expect(r.status).toBe(204);

      const badLens = {
        apiVersion: "durable.lens/v1",
        schema: "badlens",
        from: 1,
        to: 2,
        ops: [{ op: "rename", from: "/assignee", to: "/assignees" }],
      };
      r = await fetch(`${baseUrl}/v1/stream/badlens/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ schema: v2, lens: badLens }),
      });
      expect(r.status).toBe(400);
    });
  });
});
