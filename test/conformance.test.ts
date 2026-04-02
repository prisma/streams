import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadConfig, type Config } from "../src/config";
import { createApp } from "../src/app";
import { MockR2Store } from "../src/objectstore/mock_r2";

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

describe("durable streams (Bun+TS rewrite)", () => {
  let root: string;
  let server: any;
  let baseUrl: string;
  let app: ReturnType<typeof createApp>;

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), "ds-bun-ts-"));
  });

  afterEach(() => {
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
    try {
      rmSync(root, { recursive: true, force: true });
    } catch {
      // ignore
    }
  });

  test("PUT create, POST append (raw), GET read", async () => {
    const cfg = makeConfig(root);
    const os = new MockR2Store();
    app = createApp(cfg, os);
    server = Bun.serve({ port: 0, fetch: app.fetch });
    baseUrl = `http://localhost:${server.port}`;

    // Create
    let r = await fetch(`${baseUrl}/v1/stream/test_stream`, { method: "PUT" });
    expect([200, 201]).toContain(r.status);

    // Append raw
    const payload = new TextEncoder().encode("hello");
    r = await fetch(`${baseUrl}/v1/stream/test_stream`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream" },
      body: payload,
    });
    expect(r.status).toBe(204);
    const next = r.headers.get("stream-next-offset");
    expect(next).toBeTruthy();

    // Read
    r = await fetch(`${baseUrl}/v1/stream/test_stream?offset=-1`);
    expect(r.status).toBe(200);
    const bytes = new Uint8Array(await r.arrayBuffer());
    expect(new TextDecoder().decode(bytes)).toBe("hello");
    expect(r.headers.get("stream-next-offset")).toBe(next);
  });

  test("Stream-Seq mismatch yields 409", async () => {
    const cfg = makeConfig(root);
    app = createApp(cfg, new MockR2Store());
    server = Bun.serve({ port: 0, fetch: app.fetch });
    baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/seq_test`, { method: "PUT" });
    const payload = new Uint8Array([1, 2, 3]);

    await fetch(`${baseUrl}/v1/stream/seq_test`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream", "stream-seq": "002" },
      body: payload,
    });

    // Lower seq should be rejected (lexicographic).
    const r = await fetch(`${baseUrl}/v1/stream/seq_test`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream", "stream-seq": "001" },
      body: payload,
    });
    expect(r.status).toBe(409);
  });

  test("PUT is idempotent and rejects config mismatches", async () => {
    const cfg = makeConfig(root);
    app = createApp(cfg, new MockR2Store());
    server = Bun.serve({ port: 0, fetch: app.fetch });
    baseUrl = `http://localhost:${server.port}`;

    const ttl = "60s";
    let r = await fetch(`${baseUrl}/v1/stream/create_update`, {
      method: "PUT",
      headers: { "content-type": "text/plain", "stream-ttl": ttl },
    });
    expect(r.status).toBe(201);

    const payload = new TextEncoder().encode("hi");
    r = await fetch(`${baseUrl}/v1/stream/create_update`, {
      method: "POST",
      headers: { "content-type": "text/plain" },
      body: payload,
    });
    expect(r.status).toBe(204);
    const nextOffset = r.headers.get("stream-next-offset");
    expect(nextOffset).toBeTruthy();

    // Same config is idempotent and returns current tail offset.
    r = await fetch(`${baseUrl}/v1/stream/create_update`, {
      method: "PUT",
      headers: { "content-type": "text/plain", "stream-ttl": ttl },
    });
    expect(r.status).toBe(200);
    expect(r.headers.get("stream-next-offset")).toBe(nextOffset);

    // Mismatched content-type should conflict.
    r = await fetch(`${baseUrl}/v1/stream/create_update`, {
      method: "PUT",
      headers: { "content-type": "application/octet-stream", "stream-ttl": ttl },
    });
    expect(r.status).toBe(409);

    // Mismatched ttl should conflict.
    r = await fetch(`${baseUrl}/v1/stream/create_update`, {
      method: "PUT",
      headers: { "content-type": "text/plain", "stream-ttl": "120s" },
    });
    expect(r.status).toBe(409);

    // Closed flag mismatch should conflict.
    r = await fetch(`${baseUrl}/v1/stream/create_update`, {
      method: "PUT",
      headers: { "content-type": "text/plain", "stream-ttl": ttl, "stream-closed": "true" },
    });
    expect(r.status).toBe(409);
  });

  test("PUT can append initial data and close the stream", async () => {
    const cfg = makeConfig(root);
    app = createApp(cfg, new MockR2Store());
    server = Bun.serve({ port: 0, fetch: app.fetch });
    baseUrl = `http://localhost:${server.port}`;

    const payload = new TextEncoder().encode("seed");
    let r = await fetch(`${baseUrl}/v1/stream/closed_seed`, {
      method: "PUT",
      headers: { "content-type": "text/plain", "stream-closed": "true" },
      body: payload,
    });
    expect(r.status).toBe(201);
    expect(r.headers.get("stream-closed")).toBe("true");

    r = await fetch(`${baseUrl}/v1/stream/closed_seed?offset=-1`);
    expect(r.status).toBe(200);
    expect(new TextDecoder().decode(new Uint8Array(await r.arrayBuffer()))).toBe("seed");

    // Further appends should be rejected as closed.
    r = await fetch(`${baseUrl}/v1/stream/closed_seed`, {
      method: "POST",
      headers: { "content-type": "text/plain" },
      body: new TextEncoder().encode("nope"),
    });
    expect(r.status).toBe(409);
    expect(r.headers.get("stream-closed")).toBe("true");
  });

  test("Key filter only returns matching entries", async () => {
    const cfg = makeConfig(root);
    app = createApp(cfg, new MockR2Store());
    server = Bun.serve({ port: 0, fetch: app.fetch });
    baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/key_test`, { method: "PUT", headers: { "content-type": "text/plain" } });
    await fetch(`${baseUrl}/v1/stream/key_test`, {
      method: "POST",
      headers: { "content-type": "text/plain", "stream-key": "a" },
      body: new TextEncoder().encode("A"),
    });
    await fetch(`${baseUrl}/v1/stream/key_test`, {
      method: "POST",
      headers: { "content-type": "text/plain", "stream-key": "b" },
      body: new TextEncoder().encode("B"),
    });
    await fetch(`${baseUrl}/v1/stream/key_test`, {
      method: "POST",
      headers: { "content-type": "text/plain", "stream-key": "a" },
      body: new TextEncoder().encode("C"),
    });

    const r = await fetch(`${baseUrl}/v1/stream/key_test?offset=-1&key=a`);
    expect(r.status).toBe(200);
    const bytes = new Uint8Array(await r.arrayBuffer());
    expect(new TextDecoder().decode(bytes)).toBe("AC");
  });

  test("Segments are cut and uploaded to MockR2, and reads still work", async () => {
    const os = new MockR2Store();
    const cfg = makeConfig(root, {
      segmentMaxBytes: 1024,
      blockMaxBytes: 512,
      segmentCheckIntervalMs: 50,
      uploadIntervalMs: 50,
    });
    app = createApp(cfg, os);
    server = Bun.serve({ port: 0, fetch: app.fetch });
    baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/upload_test`, { method: "PUT" });

    // Append enough payload to trigger segmenting.
    const payload = new Uint8Array(600);
    payload.fill(7);
    for (let i = 0; i < 10; i++) {
      const r = await fetch(`${baseUrl}/v1/stream/upload_test`, {
        method: "POST",
        headers: { "content-type": "application/octet-stream" },
        body: payload,
      });
      expect(r.status).toBe(204);
    }

    // Wait for segmenter+uploader to run.
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
      const keys = await os.list("streams/");
      const hasManifest = keys.some((k) => k.endsWith("/manifest.json"));
      const hasSegment = keys.some((k) => k.includes("/segments/") && k.endsWith(".bin"));
      if (hasManifest && hasSegment) break;
      await sleep(50);
    }

    const keys = await os.list("streams/");
    expect(keys.some((k) => k.endsWith("/manifest.json"))).toBe(true);
    expect(keys.some((k) => k.includes("/segments/") && k.endsWith(".bin"))).toBe(true);

    // Read should still return all bytes.
    const r = await fetch(`${baseUrl}/v1/stream/upload_test?offset=-1`);
    expect(r.status).toBe(200);
    const out = new Uint8Array(await r.arrayBuffer());
    expect(out.byteLength).toBe(10 * payload.byteLength);
  });

  test("Schema evolution with lens promotes older events", async () => {
    const cfg = makeConfig(root);
    app = createApp(cfg, new MockR2Store());
    server = Bun.serve({ port: 0, fetch: app.fetch });
    baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/schema_test`, { method: "PUT", headers: { "content-type": "application/json" } });

    const schemaV1 = {
      type: "object",
      properties: { name: { type: "string" } },
      required: ["name"],
      additionalProperties: false,
    };

    let r = await fetch(`${baseUrl}/v1/stream/schema_test/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ schema: schemaV1 }),
    });
    expect(r.status).toBe(200);

    r = await fetch(`${baseUrl}/v1/stream/schema_test`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify([{ name: "alice" }]),
    });
    expect(r.status).toBe(204);

    const schemaV2 = {
      type: "object",
      properties: { fullName: { type: "string" } },
      required: ["fullName"],
      additionalProperties: false,
    };
    const lens = {
      apiVersion: "durable.lens/v1",
      schema: "schema_test",
      from: 1,
      to: 2,
      ops: [{ op: "rename", from: "/name", to: "/fullName" }],
    };
    r = await fetch(`${baseUrl}/v1/stream/schema_test/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ schema: schemaV2, lens }),
    });
    expect(r.status).toBe(200);

    r = await fetch(`${baseUrl}/v1/stream/schema_test`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify([{ fullName: "bob" }]),
    });
    expect(r.status).toBe(204);

    r = await fetch(`${baseUrl}/v1/stream/schema_test?offset=-1&format=json`);
    expect(r.status).toBe(200);
    const arr = await r.json();
    expect(arr).toEqual([{ fullName: "alice" }, { fullName: "bob" }]);
  });

  test("Schema update accepts unencoded slashes and routingKey-only update", async () => {
    const cfg = makeConfig(root);
    app = createApp(cfg, new MockR2Store());
    server = Bun.serve({ port: 0, fetch: app.fetch });
    baseUrl = `http://localhost:${server.port}`;

    const streamName = "record/app.bsky.feed.like";
    let r = await fetch(`${baseUrl}/v1/stream/${streamName}`, { method: "PUT", headers: { "content-type": "application/json" } });
    expect([200, 201]).toContain(r.status);

    r = await fetch(`${baseUrl}/v1/stream/${streamName}/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ routingKey: { jsonPointer: "/subject/uri", required: true } }),
    });
    expect(r.status).toBe(200);
  });

  test("long-poll returns when new data arrives", async () => {
    const cfg = makeConfig(root);
    app = createApp(cfg, new MockR2Store());
    server = Bun.serve({ port: 0, fetch: app.fetch });
    baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/live_test`, { method: "PUT", headers: { "content-type": "text/plain" } });

    const livePromise = fetch(`${baseUrl}/v1/stream/live_test?offset=-1&live=long-poll&timeout=2s`);
    await sleep(50);
    await fetch(`${baseUrl}/v1/stream/live_test`, {
      method: "POST",
      headers: { "content-type": "text/plain" },
      body: new TextEncoder().encode("x"),
    });
    const r = await livePromise;
    expect(r.status).toBe(200);
    const bytes = new Uint8Array(await r.arrayBuffer());
    expect(new TextDecoder().decode(bytes)).toBe("x");
  });

  test("generic resolver timeout caps long-poll requests at 5s", { timeout: 8_000 }, async () => {
    const cfg = makeConfig(root);
    app = createApp(cfg, new MockR2Store());
    server = Bun.serve({ port: 0, fetch: app.fetch });
    baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/live_timeout_cap`, { method: "PUT", headers: { "content-type": "text/plain" } });

    const start = Date.now();
    const r = await fetch(`${baseUrl}/v1/stream/live_timeout_cap?offset=-1&live=long-poll&timeout=6s`);
    const elapsed = Date.now() - start;

    expect(r.status).toBe(408);
    expect(elapsed).toBeGreaterThanOrEqual(4_900);
    expect(elapsed).toBeLessThan(7_500);
    expect(await r.json()).toEqual({
      error: { code: "request_timeout", message: "request timed out" },
    });
  });

  test("/pk path overrides key query", async () => {
    const cfg = makeConfig(root);
    app = createApp(cfg, new MockR2Store());
    server = Bun.serve({ port: 0, fetch: app.fetch });
    baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/pk_test`, { method: "PUT", headers: { "content-type": "text/plain" } });
    await fetch(`${baseUrl}/v1/stream/pk_test`, {
      method: "POST",
      headers: { "content-type": "text/plain", "stream-key": "a" },
      body: new TextEncoder().encode("A"),
    });
    await fetch(`${baseUrl}/v1/stream/pk_test`, {
      method: "POST",
      headers: { "content-type": "text/plain", "stream-key": "b" },
      body: new TextEncoder().encode("B"),
    });

    const r = await fetch(`${baseUrl}/v1/stream/pk_test/pk/a?offset=-1&key=b`);
    expect(r.status).toBe(200);
    const bytes = new Uint8Array(await r.arrayBuffer());
    expect(new TextDecoder().decode(bytes)).toBe("A");
  });

  test("__stream_metrics__ receives interval events", async () => {
    const cfg = makeConfig(root, { metricsFlushIntervalMs: 50 });
    app = createApp(cfg, new MockR2Store());
    server = Bun.serve({ port: 0, fetch: app.fetch });
    baseUrl = `http://localhost:${server.port}`;

    await fetch(`${baseUrl}/v1/stream/metric_src`, { method: "PUT", headers: { "content-type": "text/plain" } });
    await fetch(`${baseUrl}/v1/stream/metric_src`, {
      method: "POST",
      headers: { "content-type": "text/plain" },
      body: new TextEncoder().encode("m"),
    });

    const deadline = Date.now() + 2000;
    let events: any[] = [];
    while (Date.now() < deadline) {
      const r = await fetch(`${baseUrl}/v1/stream/__stream_metrics__?offset=-1&format=json`);
      events = await r.json();
      if (events.length > 0) break;
      await sleep(50);
    }
    expect(events.length).toBeGreaterThan(0);
    expect(events[0].apiVersion).toBe("durable.streams/metrics/v1");
  });
});
