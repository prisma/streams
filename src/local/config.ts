import type { Config } from "../config";
import { loadConfig } from "../config";
import { getDbPath, getDataDir } from "./state";
import { normalizeServerName } from "./paths";

export function buildLocalConfig(args: { name?: string; port?: number }): Config {
  const name = normalizeServerName(args.name);
  const dataDir = getDataDir(name);
  const dbPath = getDbPath(name);
  const base = loadConfig();
  const port = args.port == null ? 0 : Math.max(0, Math.floor(args.port));

  return {
    ...base,
    rootDir: dataDir,
    dbPath,
    port,
    segmentCacheMaxBytes: 0,
    segmenterWorkers: 0,
    // Local dev mode prioritizes responsiveness over throughput.
    interpreterCheckIntervalMs: Math.min(base.interpreterCheckIntervalMs, 5),
  };
}
