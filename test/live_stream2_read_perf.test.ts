import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { streamHash16Hex } from "../src/util/stream_paths";

const FIXTURE_DIR = join(import.meta.dir, "fixtures", "live-stream2");
const FIXTURE_DB_SQL_PATH = join(FIXTURE_DIR, "streams-test.sql");
const FIXTURE_SEGMENT61_PATH = join(FIXTURE_DIR, "segment-61.bin");
const STREAM = "stream-2";
const SEALED_OFFSET = "0000000000000025DNRG000000";
const WAL_TAIL_OFFSET = "0000000000000025Q9J0000000";

function makeConfig(rootDir: string, overrides: Partial<Config> = {}): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    segmentCheckIntervalMs: 60_000_000,
    uploadIntervalMs: 60_000_000,
    expirySweepIntervalMs: 60_000_000,
    metricsFlushIntervalMs: 60_000_000,
    touchCheckIntervalMs: 60_000_000,
    ...overrides,
  };
}

function prepareFixtureRoot(): string {
  if (!existsSync(FIXTURE_DB_SQL_PATH)) throw new Error(`missing fixture db dump: ${FIXTURE_DB_SQL_PATH}`);
  if (!existsSync(FIXTURE_SEGMENT61_PATH)) throw new Error(`missing fixture segment: ${FIXTURE_SEGMENT61_PATH}`);

  const root = mkdtempSync(join(tmpdir(), "ds-live-stream2-"));
  const dbPath = join(root, "wal.sqlite");
  const db = new Database(dbPath, { create: true });
  try {
    db.exec(readFileSync(FIXTURE_DB_SQL_PATH, "utf8"));
  } finally {
    db.close();
  }

  const objectKey = `streams/${streamHash16Hex(STREAM)}/segments/0000000000000061.bin`;
  const cachePath = join(root, "cache", objectKey);
  mkdirSync(dirname(cachePath), { recursive: true });
  copyFileSync(FIXTURE_SEGMENT61_PATH, cachePath);

  return root;
}

async function withFixtureApp<T>(fn: (app: ReturnType<typeof createApp>) => Promise<T>): Promise<T> {
  const root = prepareFixtureRoot();
  const app = createApp(makeConfig(root), new MockR2Store());
  try {
    return await fn(app);
  } finally {
    app.close();
    rmSync(root, { recursive: true, force: true });
  }
}

async function measureReadMs(app: ReturnType<typeof createApp>, offset: string): Promise<{ elapsedMs: number; bodyBytes: number }> {
  const started = performance.now();
  const res = await app.fetch(new Request(`http://local/v1/stream/${STREAM}?format=json&offset=${offset}`, { method: "GET" }));
  const body = await res.text();
  const elapsedMs = performance.now() - started;
  expect(res.status).toBe(200);
  expect(body.length).toBeGreaterThan(100_000);
  return { elapsedMs, bodyBytes: body.length };
}

async function prewarmRead(app: ReturnType<typeof createApp>, offset: string): Promise<void> {
  const res = await app.fetch(new Request(`http://local/v1/stream/${STREAM}?format=json&offset=${offset}`, { method: "GET" }));
  expect(res.status).toBe(200);
  await res.text();
}

describe("live stream-2 read perf fixtures", () => {
  test("sealed-prefix read stays below 100ms on warm live fixture data", async () => {
    await withFixtureApp(async (app) => {
      await prewarmRead(app, SEALED_OFFSET);
      const { elapsedMs, bodyBytes } = await measureReadMs(app, SEALED_OFFSET);
      expect(bodyBytes).toBeGreaterThan(600_000);
      expect(elapsedMs).toBeLessThan(100);
    });
  }, 15_000);

  test("wal-tail read stays below 100ms on warm live fixture data", async () => {
    await withFixtureApp(async (app) => {
      await prewarmRead(app, WAL_TAIL_OFFSET);
      const { elapsedMs, bodyBytes } = await measureReadMs(app, WAL_TAIL_OFFSET);
      expect(bodyBytes).toBeGreaterThan(600_000);
      expect(elapsedMs).toBeLessThan(100);
    });
  }, 15_000);
});
