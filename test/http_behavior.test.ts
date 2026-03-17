import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { encodeOffset, parseOffset } from "../src/offset";

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
  const root = mkdtempSync(join(tmpdir(), "ds-http-"));
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

function nextOffset(resp: Response): bigint {
  const h = resp.headers.get("stream-next-offset");
  expect(h).not.toBeNull();
  const p = parseOffset(h!);
  return p.kind === "start" ? -1n : p.seq;
}

async function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

describe("http behavior", () => {
  test("create/append/read raw and end offset", async () => {
    await withServer({}, async ({ baseUrl }) => {
      let r = await fetch(`${baseUrl}/v1/stream/foo`, { method: "PUT", headers: { "content-type": "text/plain" } });
      expect([200, 201]).toContain(r.status);
      expect(nextOffset(r)).toBe(-1n);

      r = await fetch(`${baseUrl}/v1/stream/foo`, { method: "POST", headers: { "content-type": "text/plain" }, body: "a" });
      expect(r.status).toBe(204);
      expect(nextOffset(r)).toBe(0n);

      r = await fetch(`${baseUrl}/v1/stream/foo`, { method: "POST", headers: { "content-type": "text/plain" }, body: "b" });
      expect(r.status).toBe(204);
      expect(nextOffset(r)).toBe(1n);

      r = await fetch(`${baseUrl}/v1/stream/foo?offset=-1`);
      expect(r.status).toBe(200);
      expect(await r.text()).toBe("ab");
      expect(nextOffset(r)).toBe(1n);

      const end = encodeOffset(0, 1n);
      r = await fetch(`${baseUrl}/v1/stream/foo?offset=${end}`);
      expect(r.status).toBe(200);
      expect(await r.text()).toBe("");
      expect(nextOffset(r)).toBe(1n);
    });
  });

  test("read empty stream returns empty body", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/empty`, { method: "PUT", headers: { "content-type": "text/plain" } });
      const r = await fetch(`${baseUrl}/v1/stream/empty?offset=-1`);
      expect(r.status).toBe(200);
      expect(await r.text()).toBe("");
      expect(nextOffset(r)).toBe(-1n);
    });
  });

  test("read beyond end returns unchanged offset", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/end`, { method: "PUT", headers: { "content-type": "text/plain" } });
      await fetch(`${baseUrl}/v1/stream/end`, { method: "POST", headers: { "content-type": "text/plain" }, body: "a" });
      await fetch(`${baseUrl}/v1/stream/end`, { method: "POST", headers: { "content-type": "text/plain" }, body: "b" });
      const off = encodeOffset(0, 999n);
      const r = await fetch(`${baseUrl}/v1/stream/end?offset=${off}`);
      expect(r.status).toBe(200);
      expect(await r.text()).toBe("");
      expect(nextOffset(r)).toBe(999n);
    });
  });

  test("list streams returns all streams", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/a`, { method: "PUT", headers: { "content-type": "text/plain" } });
      await fetch(`${baseUrl}/v1/stream/b`, { method: "PUT", headers: { "content-type": "text/plain" } });
      const r = await fetch(`${baseUrl}/v1/streams`);
      expect(r.status).toBe(200);
      const arr = await r.json();
      expect(Array.isArray(arr)).toBe(true);
      const names = arr.map((r: any) => r.name).sort();
      expect(names).toContain("a");
      expect(names).toContain("b");
    });
  });

  test("create is idempotent for existing stream", async () => {
    await withServer({}, async ({ baseUrl }) => {
      let r = await fetch(`${baseUrl}/v1/stream/dup`, { method: "PUT", headers: { "content-type": "text/plain" } });
      expect([200, 201]).toContain(r.status);
      expect(nextOffset(r)).toBe(-1n);

      r = await fetch(`${baseUrl}/v1/stream/dup`, { method: "PUT", headers: { "content-type": "text/plain" } });
      expect(r.status).toBe(200);
      expect(nextOffset(r)).toBe(-1n);
    });
  });

  test("delete returns 404 for missing stream and 204 for existing", async () => {
    await withServer({}, async ({ baseUrl }) => {
      let r = await fetch(`${baseUrl}/v1/stream/missing`, { method: "DELETE" });
      expect(r.status).toBe(404);

      await fetch(`${baseUrl}/v1/stream/delete-me`, { method: "PUT", headers: { "content-type": "text/plain" } });
      r = await fetch(`${baseUrl}/v1/stream/delete-me`, { method: "DELETE" });
      expect(r.status).toBe(204);

      r = await fetch(`${baseUrl}/v1/stream/delete-me?offset=-1`);
      expect(r.status).toBe(404);
    });
  });

  test("append max body bytes enforced", async () => {
    await withServer({ appendMaxBodyBytes: 4 }, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/limit`, { method: "PUT", headers: { "content-type": "text/plain" } });
      const r = await fetch(`${baseUrl}/v1/stream/limit`, {
        method: "POST",
        headers: { "content-type": "text/plain" },
        body: "hello",
      });
      expect(r.status).toBe(413);
    });
  });

  test("json batch append and read json", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/json`, { method: "PUT", headers: { "content-type": "application/json" } });
      let r = await fetch(`${baseUrl}/v1/stream/json`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify([{ x: 1 }, { y: 2 }]),
      });
      expect(r.status).toBe(204);
      r = await fetch(`${baseUrl}/v1/stream/json?offset=-1&format=json`);
      expect(r.status).toBe(200);
      expect(await r.json()).toEqual([{ x: 1 }, { y: 2 }]);
    });
  });

  test("schema routing key batch append and read by key", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/keys`, { method: "PUT", headers: { "content-type": "application/json" } });
      let r = await fetch(`${baseUrl}/v1/stream/keys/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ routingKey: { jsonPointer: "/k", required: true } }),
      });
      expect(r.status).toBe(200);

      r = await fetch(`${baseUrl}/v1/stream/keys`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify([{ k: "k1", x: 1 }, { k: "k2", y: 2 }]),
      });
      expect(r.status).toBe(204);

      r = await fetch(`${baseUrl}/v1/stream/keys/pk/k2?offset=-1&format=json`);
      expect(r.status).toBe(200);
      expect(await r.json()).toEqual([{ y: 2, k: "k2" }]);

      r = await fetch(`${baseUrl}/v1/stream/keys?offset=-1&format=json&key=k2`);
      expect(r.status).toBe(200);
      expect(await r.json()).toEqual([{ y: 2, k: "k2" }]);

      r = await fetch(`${baseUrl}/v1/stream/keys`, {
        method: "POST",
        headers: { "content-type": "application/json", "stream-key": "ignored" },
        body: JSON.stringify({ k: "k3", z: 3 }),
      });
      expect(r.status).toBe(400);
    });
  });

  test("live read timeout", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/live`, { method: "PUT", headers: { "content-type": "text/plain" } });
      await fetch(`${baseUrl}/v1/stream/live`, { method: "POST", headers: { "content-type": "text/plain" }, body: "a" });

      const start = Date.now();
      const r = await fetch(`${baseUrl}/v1/stream/live?offset=${encodeOffset(0, 0n)}&live=long-poll&timeout=200ms`);
      expect(r.status).toBe(204);
      expect(Date.now() - start).toBeGreaterThan(150);
    });
  });

  test("live read wakes on append", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/live2`, { method: "PUT", headers: { "content-type": "text/plain" } });
      await fetch(`${baseUrl}/v1/stream/live2`, { method: "POST", headers: { "content-type": "text/plain" }, body: "a" });

      const livePromise = fetch(`${baseUrl}/v1/stream/live2?offset=${encodeOffset(0, 0n)}&live=long-poll&timeout=2s`);
      await sleep(100);
      await fetch(`${baseUrl}/v1/stream/live2`, { method: "POST", headers: { "content-type": "text/plain" }, body: "b" });
      const r = await livePromise;
      expect(r.status).toBe(200);
      expect(await r.text()).toBe("b");
    });
  });

  test("live read by key only returns on match", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/keylive`, { method: "PUT", headers: { "content-type": "text/plain" } });
      const post = (key: string, body: string) =>
        fetch(`${baseUrl}/v1/stream/keylive`, {
          method: "POST",
          headers: { "content-type": "text/plain", "stream-key": key },
          body,
        });
      await post("a", "1");
      await post("b", "2");
      await post("a", "3");

      const livePromise = fetch(
        `${baseUrl}/v1/stream/keylive/pk/a?offset=${encodeOffset(0, 2n)}&live=long-poll&timeout=1s`
      );

      await sleep(100);
      await post("b", "4");

      let resolved = false;
      await Promise.race([
        livePromise.then(() => {
          resolved = true;
        }),
        sleep(150),
      ]);
      expect(resolved).toBe(false);

      await post("a", "5");
      const r = await livePromise;
      expect(r.status).toBe(200);
      expect(await r.text()).toBe("5");
    });
  });

  test("ttl expiry returns 404", async () => {
    await withServer({}, async ({ baseUrl }) => {
      const expires = new Date(Date.now() + 200).toISOString();
      const r = await fetch(`${baseUrl}/v1/stream/ttl`, {
        method: "PUT",
        headers: { "content-type": "text/plain", "stream-expires-at": expires },
      });
      expect(r.status).toBe(201);
      await sleep(250);
      const r2 = await fetch(`${baseUrl}/v1/stream/ttl?offset=-1`);
      expect(r2.status).toBe(404);
    });
  });

  test("etag and cache-control headers on catch-up reads", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/etag`, { method: "PUT", headers: { "content-type": "text/plain" } });
      await fetch(`${baseUrl}/v1/stream/etag`, { method: "POST", headers: { "content-type": "text/plain" }, body: "a" });
      const r = await fetch(`${baseUrl}/v1/stream/etag?offset=-1`);
      expect(r.status).toBe(200);
      expect(r.headers.get("cache-control")).not.toBeNull();
      expect(r.headers.get("etag")).not.toBeNull();
    });
  });

  test("etag match returns 304 with empty body", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/etag2`, { method: "PUT", headers: { "content-type": "text/plain" } });
      await fetch(`${baseUrl}/v1/stream/etag2`, { method: "POST", headers: { "content-type": "text/plain" }, body: "a" });
      const r1 = await fetch(`${baseUrl}/v1/stream/etag2?offset=-1`);
      const etag = r1.headers.get("etag");
      expect(etag).not.toBeNull();
      await r1.text();
      const r2 = await fetch(`${baseUrl}/v1/stream/etag2?offset=-1`, { headers: { "if-none-match": etag! } });
      expect(r2.status).toBe(304);
      const body = await r2.text();
      expect(body).toBe("");
    });
  });

  test("cache-control no-store for live reads", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/nolive`, { method: "PUT", headers: { "content-type": "text/plain" } });
      await fetch(`${baseUrl}/v1/stream/nolive`, { method: "POST", headers: { "content-type": "text/plain" }, body: "a" });
      const r = await fetch(`${baseUrl}/v1/stream/nolive?offset=${encodeOffset(0, 0n)}&live=true&timeout=10ms`);
      expect(r.status).toBe(204);
      expect(r.headers.get("cache-control")).toBe("no-store");
      expect(r.headers.get("etag")).toBeNull();
    });
  });

  test("stream seq header enforces monotonicity", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/seq`, { method: "PUT", headers: { "content-type": "text/plain" } });
      let r = await fetch(`${baseUrl}/v1/stream/seq`, {
        method: "POST",
        headers: { "content-type": "text/plain", "stream-seq": "001" },
        body: "a",
      });
      expect(r.status).toBe(204);

      r = await fetch(`${baseUrl}/v1/stream/seq`, {
        method: "POST",
        headers: { "content-type": "text/plain", "stream-seq": "001" },
        body: "b",
      });
      expect(r.status).toBe(409);

      r = await fetch(`${baseUrl}/v1/stream/seq`, {
        method: "POST",
        headers: { "content-type": "text/plain", "stream-seq": "002" },
        body: "b",
      });
      expect(r.status).toBe(204);
    });
  });

  test("stream expires header is echoed on HEAD/GET", async () => {
    await withServer({}, async ({ baseUrl }) => {
      const r = await fetch(`${baseUrl}/v1/stream/expires`, {
        method: "PUT",
        headers: { "content-type": "text/plain", "stream-ttl": "3600" },
      });
      expect(r.status).toBe(201);
      const exp = r.headers.get("stream-expires-at");
      expect(exp).not.toBeNull();

      const head = await fetch(`${baseUrl}/v1/stream/expires`, { method: "HEAD" });
      expect(head.headers.get("stream-expires-at")).toBe(exp);

      const get = await fetch(`${baseUrl}/v1/stream/expires?offset=-1`);
      expect(get.headers.get("stream-expires-at")).toBe(exp);
    });
  });

  test("since param invalid and offset overrides since", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/since`, { method: "PUT", headers: { "content-type": "text/plain" } });
      await fetch(`${baseUrl}/v1/stream/since`, { method: "POST", headers: { "content-type": "text/plain" }, body: "a" });
      await fetch(`${baseUrl}/v1/stream/since`, { method: "POST", headers: { "content-type": "text/plain" }, body: "b" });

      let r = await fetch(`${baseUrl}/v1/stream/since?since=not-a-time`);
      expect(r.status).toBe(400);

      const future = new Date(Date.now() + 60_000).toISOString();
      r = await fetch(`${baseUrl}/v1/stream/since?offset=-1&since=${encodeURIComponent(future)}`);
      expect(r.status).toBe(200);
      expect(await r.text()).toBe("ab");
    });
  });
});
