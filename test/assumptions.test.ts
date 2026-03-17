import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { parseOffset } from "../src/offset";

function makeConfig(rootDir: string, overrides: Partial<Config> = {}): Config {
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
  return new Promise((res) => setTimeout(res, ms));
}

describe("assumptions", () => {
  test("append to missing stream returns 404", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    const r = await fetch(`${baseUrl}/v1/stream/missing`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream" },
      body: new Uint8Array([1]),
    });
    expect(r.status).toBe(404);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("expired streams return 404", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/ttl`, { method: "PUT", headers: { "stream-ttl": "1s" } });
    await sleep(1100);
    let r = await fetch(`${baseUrl}/v1/stream/ttl?offset=-1`);
    expect(r.status).toBe(404);
    r = await fetch(`${baseUrl}/v1/stream/ttl`, { method: "HEAD" });
    expect(r.status).toBe(404);
    r = await fetch(`${baseUrl}/v1/stream/ttl`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream" },
      body: new Uint8Array([1]),
    });
    expect(r.status).toBe(404);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("/v1/streams pagination", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/a`, { method: "PUT" });
    await fetch(`${baseUrl}/v1/stream/b`, { method: "PUT" });
    await fetch(`${baseUrl}/v1/stream/c`, { method: "PUT" });

    let r = await fetch(`${baseUrl}/v1/streams?limit=2&offset=0`);
    let arr = await r.json();
    expect(arr.length).toBe(2);

    r = await fetch(`${baseUrl}/v1/streams?limit=2&offset=2`);
    arr = await r.json();
    // Includes system streams like __stream_metrics__; total is 4 here.
    expect(arr.length).toBe(2);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("schema update accepts unencoded slashes in stream name", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    const streamName = "record/app.bsky.feed.like";
    const putRes = await fetch(`${baseUrl}/v1/stream/${streamName}`, { method: "PUT" });
    expect([200, 201]).toContain(putRes.status);

    const schemaRes = await fetch(`${baseUrl}/v1/stream/${streamName}/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ schema: { type: "object", additionalProperties: true } }),
    });
    expect(schemaRes.status).toBe(200);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("schema update accepts registry-shaped payload", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    const streamName = "record/app.bsky.feed.like";
    const putRes = await fetch(`${baseUrl}/v1/stream/${streamName}`, { method: "PUT" });
    expect([200, 201]).toContain(putRes.status);

    const payload = {
      apiVersion: "durable.streams/schema-registry/v1",
      schema: streamName,
      currentVersion: 1,
      schemas: {
        "1": { type: "object", additionalProperties: true },
      },
      lenses: {},
    };

    const schemaRes = await fetch(`${baseUrl}/v1/stream/${streamName}/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
    expect(schemaRes.status).toBe(200);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("schema update accepts routingKey-only payload", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    const streamName = "record/app.bsky.feed.like";
    const putRes = await fetch(`${baseUrl}/v1/stream/${streamName}`, { method: "PUT" });
    expect([200, 201]).toContain(putRes.status);

    const payload = {
      routingKey: { jsonPointer: "/subject/uri", required: true },
    };
    const schemaRes = await fetch(`${baseUrl}/v1/stream/${streamName}/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
    expect(schemaRes.status).toBe(200);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("decimal offset alias is rejected", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/alias`, { method: "PUT" });
    await fetch(`${baseUrl}/v1/stream/alias`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream" },
      body: new Uint8Array([1]),
    });
    await fetch(`${baseUrl}/v1/stream/alias`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream" },
      body: new Uint8Array([2]),
    });

    const r = await fetch(`${baseUrl}/v1/stream/alias?offset=0`);
    expect(r.status).toBe(400);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("JSON Content-Type with charset is accepted", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/json`, { method: "PUT", headers: { "content-type": "application/json" } });
    const r = await fetch(`${baseUrl}/v1/stream/json`, {
      method: "POST",
      headers: { "content-type": "application/json; charset=utf-8" },
      body: JSON.stringify([{ ok: true }]),
    });
    expect(r.status).toBe(204);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("empty JSON array is rejected", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/empty`, { method: "PUT", headers: { "content-type": "application/json" } });
    const r = await fetch(`${baseUrl}/v1/stream/empty`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "[]",
    });
    expect(r.status).toBe(400);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("Stream-Key rejected when routingKey configured", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/rk`, { method: "PUT", headers: { "content-type": "application/json" } });
    await fetch(`${baseUrl}/v1/stream/rk/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        schema: { type: "object", properties: { id: { type: "string" } }, required: ["id"], additionalProperties: false },
        routingKey: { jsonPointer: "/id", required: true },
      }),
    });

    const r = await fetch(`${baseUrl}/v1/stream/rk`, {
      method: "POST",
      headers: { "content-type": "application/json", "stream-key": "x" },
      body: JSON.stringify([{ id: "x" }]),
    });
    expect(r.status).toBe(400);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("format=json on non-json payload returns 400", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/raw`, { method: "PUT" });
    await fetch(`${baseUrl}/v1/stream/raw`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream" },
      body: new Uint8Array([1, 2, 3]),
    });
    const r = await fetch(`${baseUrl}/v1/stream/raw?offset=-1&format=json`);
    expect(r.status).toBe(400);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("unknown format returns 400", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/fmt`, { method: "PUT" });
    const r = await fetch(`${baseUrl}/v1/stream/fmt?offset=-1&format=xml`);
    expect(r.status).toBe(400);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("missing Content-Type on POST returns 400", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/noct`, { method: "PUT", headers: { "content-type": "application/octet-stream" } });
    const r = await fetch(`${baseUrl}/v1/stream/noct`, {
      method: "POST",
      body: new Uint8Array([1, 2, 3]),
    });
    expect(r.status).toBe(400);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("since= selects records at or after the timestamp", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/since`, { method: "PUT", headers: { "content-type": "text/plain" } });

    const t1Ms = Date.now() + 1000;
    const t2Ms = t1Ms + 1000;
    const t1 = new Date(t1Ms);
    const t2 = new Date(t2Ms);

    await fetch(`${baseUrl}/v1/stream/since`, {
      method: "POST",
      headers: { "content-type": "text/plain", "stream-timestamp": t1.toISOString() },
      body: new TextEncoder().encode("a"),
    });
    await fetch(`${baseUrl}/v1/stream/since`, {
      method: "POST",
      headers: { "content-type": "text/plain", "stream-timestamp": t2.toISOString() },
      body: new TextEncoder().encode("b"),
    });

    const since = new Date(t1Ms + 500).toISOString();
    const r = await fetch(`${baseUrl}/v1/stream/since?since=${encodeURIComponent(since)}`);
    expect(r.status).toBe(200);
    const text = await r.text();
    expect(text).toBe("b");

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("initial epoch is 0", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/epoch`, { method: "PUT" });
    const r = await fetch(`${baseUrl}/v1/stream/epoch`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream" },
      body: new Uint8Array([1]),
    });
    const next = r.headers.get("stream-next-offset")!;
    const p = parseOffset(next);
    expect(p.kind).toBe("seq");
    if (p.kind === "seq") expect(p.epoch).toBe(0);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("delete is tombstone and listing excludes deleted", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/del`, { method: "PUT" });
    await fetch(`${baseUrl}/v1/stream/del`, { method: "DELETE" });
    const r = await fetch(`${baseUrl}/v1/stream/del?offset=-1`);
    expect([404, 410]).toContain(r.status);

    const list = await fetch(`${baseUrl}/v1/streams`);
    const arr = await list.json();
    expect(arr.find((x: any) => x.name === "del")).toBeUndefined();

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });

  test("Stream-Seq is lexicographic and strictly increasing", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-assume-"));
    const cfg = makeConfig(root);
    const app = createApp(cfg, new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/seq`, { method: "PUT" });
    await fetch(`${baseUrl}/v1/stream/seq`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream", "stream-seq": "2" },
      body: new Uint8Array([1]),
    });
    const bad = await fetch(`${baseUrl}/v1/stream/seq`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream", "stream-seq": "10" },
      body: new Uint8Array([2]),
    });
    expect(bad.status).toBe(409);
    const ok = await fetch(`${baseUrl}/v1/stream/seq`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream", "stream-seq": "3" },
      body: new Uint8Array([3]),
    });
    expect(ok.status).toBe(204);

    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  });
});
