import { closeSync, existsSync, mkdirSync, openSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import lockfile from "proper-lockfile";
import { getDurableStreamsDataRoot, getServerDataDir, normalizeServerName } from "./paths";
import { isPidAlive } from "./common";

export type DurableStreamsLocalServerDump = {
  version: number;
  name: string;
  pid: number;
  startedAt: string;
  http: { port: number; url: string };
  sqlite: { path: string };
};

export type DurableStreamsLocalStatus = {
  name: string;
  dataDir: string;
  dump: DurableStreamsLocalServerDump | null;
  pidAlive: boolean;
  healthy: boolean;
  running: boolean;
  stale: boolean;
};

export function getDataDir(name: string): string {
  return getServerDataDir(name);
}

export function getDbPath(name: string): string {
  return join(getDataDir(name), "durable-streams.sqlite");
}

export function getServerDumpPath(name: string): string {
  return join(getDataDir(name), "server.json");
}

export function getLockPath(name: string): string {
  return join(getDataDir(name), "server.lock");
}

function ensureFile(path: string): void {
  const fd = openSync(path, "a");
  closeSync(fd);
}

export async function acquireLock(name: string): Promise<() => Promise<void>> {
  const normalized = normalizeServerName(name);
  const dataDir = getDataDir(normalized);
  mkdirSync(dataDir, { recursive: true });
  const lockPath = getLockPath(normalized);
  ensureFile(lockPath);

  const release = await lockfile.lock(lockPath, {
    realpath: false,
    stale: 30_000,
    retries: { retries: 0 },
  });

  return async () => {
    try {
      await release();
    } catch {
      // Ignore lock release errors while shutting down.
    }
  };
}

export function writeServerDump(name: string, dump: DurableStreamsLocalServerDump): void {
  const normalized = normalizeServerName(name);
  const dataDir = getDataDir(normalized);
  mkdirSync(dataDir, { recursive: true });
  writeFileSync(getServerDumpPath(normalized), `${JSON.stringify(dump, null, 2)}\n`, "utf8");
}

export function readServerDump(name: string): DurableStreamsLocalServerDump | null {
  const normalized = normalizeServerName(name);
  const path = getServerDumpPath(normalized);
  if (!existsSync(path)) return null;
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8"));
    if (!parsed || typeof parsed !== "object") return null;
    if (typeof parsed.name !== "string" || typeof parsed.pid !== "number") return null;
    if (!parsed.http || typeof parsed.http.url !== "string" || typeof parsed.http.port !== "number") return null;
    if (!parsed.sqlite || typeof parsed.sqlite.path !== "string") return null;
    return parsed as DurableStreamsLocalServerDump;
  } catch {
    return null;
  }
}

export function scanServers(): string[] {
  const root = getDurableStreamsDataRoot();
  if (!existsSync(root)) return [];
  const out: string[] = [];
  for (const entry of readdirSync(root, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    out.push(entry.name);
  }
  out.sort();
  return out;
}

async function isHealthy(url: string): Promise<boolean> {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), 1_000);
  try {
    const res = await fetch(`${url}/health`, { method: "GET", signal: ac.signal });
    return res.ok;
  } catch {
    return false;
  } finally {
    clearTimeout(timer);
  }
}

export async function getStatus(name: string): Promise<DurableStreamsLocalStatus> {
  const normalized = normalizeServerName(name);
  const dataDir = getDataDir(normalized);
  const dump = readServerDump(normalized);
  const pidAlive = dump ? isPidAlive(dump.pid) : false;
  const healthy = dump ? await isHealthy(dump.http.url) : false;
  const running = pidAlive && healthy;

  return {
    name: normalized,
    dataDir,
    dump,
    pidAlive,
    healthy,
    running,
    stale: !!dump && !running,
  };
}
