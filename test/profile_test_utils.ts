import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";

export function makeProfileTestConfig(rootDir: string, overrides: Partial<Config> = {}): Config {
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

export function createProfileTestApp(rootDir: string, overrides: Partial<Config> = {}) {
  const cfg = makeProfileTestConfig(rootDir, overrides);
  const store = new MockR2Store();
  const app = createApp(cfg, store);
  return { app, cfg, store };
}

export async function fetchJsonApp(app: ReturnType<typeof createApp>, url: string, init: RequestInit): Promise<{
  status: number;
  body: any;
  text: string;
}> {
  const res = await app.fetch(new Request(url, init));
  const text = await res.text();
  return {
    status: res.status,
    body: text === "" ? null : JSON.parse(text),
    text,
  };
}
