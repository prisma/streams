import { describe, expect, test } from "bun:test";
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
    // Keep background interpreter polling off for determinism.
    interpreterCheckIntervalMs: 0,
    interpreterWorkers: 0,
    ...overrides,
  };
}

async function fetchJson(app: ReturnType<typeof createApp>, url: string, init: RequestInit): Promise<any> {
  const r = await app.fetch(new Request(url, init));
  const text = await r.text();
  if (!r.ok) throw new Error(`HTTP ${r.status} ${url}: ${text}`);
  if (text === "") return null;
  return JSON.parse(text);
}

describe("/touch/wait timeout reliability", () => {
  test(
    "2000 concurrent waits time out on schedule (no writer)",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-wait-timeout-"));
      const app = createApp(makeConfig(root), new MockR2Store());
      try {
        app.deps.segmenter.stop();
        app.deps.uploader.stop();

        const stream = "state_timeout";

        // Create base stream + enable touch interpreter.
        await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
            method: "PUT",
            headers: { "content-type": "application/json" },
          })
        );
        await fetchJson(app, `http://local/v1/stream/${encodeURIComponent(stream)}/_schema`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            interpreter: {
              apiVersion: "durable.streams/stream-interpreter/v1",
              format: "durable.streams/state-protocol/v1",
              touch: { enabled: true, retention: { maxAgeMs: 60_000 } },
            },
          }),
        });

        const body = JSON.stringify({
          keys: ["k1", "k2", "k3"],
          sinceTouchOffset: "now",
          timeoutMs: 1000,
        });

        const runOne = async (): Promise<number> => {
          const t0 = performance.now();
          const r = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/touch/wait`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body,
            })
          );
          const dt = performance.now() - t0;
          expect(r.status).toBe(200);
          const json = await r.json();
          expect(json?.touched).toBe(false);
          return dt;
        };

        const N = 2000;
        const dts = await Promise.all(Array.from({ length: N }, () => runOne()));
        const within = dts.filter((ms) => ms <= 1500).length;

        // Under load, a small tail is acceptable, but the vast majority should
        // return close to the requested timeout.
        expect(within / N).toBeGreaterThanOrEqual(0.99);
      } finally {
        try {
          app.close();
        } catch {
          // ignore
        }
        rmSync(root, { recursive: true, force: true });
      }
    },
    20_000
  );
});

