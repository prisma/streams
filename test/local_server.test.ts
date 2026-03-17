import { describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
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
});
