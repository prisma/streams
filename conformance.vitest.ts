import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { beforeAll, afterAll } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn, type ChildProcess } from "node:child_process";

const baseUrl = process.env.CONFORMANCE_TEST_URL ?? "http://127.0.0.1:8787";

let proc: ChildProcess | null = null;
let rootDir: string | null = null;

async function waitForHealth(url: string, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      const r = await fetch(`${url}/health`, { method: "GET" });
      if (r.ok) return;
    } catch {
      // ignore
    }
    if (Date.now() > deadline) throw new Error(`timed out waiting for server: ${url}`);
    await new Promise((res) => setTimeout(res, 200));
  }
}

beforeAll(async () => {
  // When CONFORMANCE_TEST_URL is provided, assume the server is already running.
  if (process.env.CONFORMANCE_TEST_URL) return;

  rootDir = mkdtempSync(join(tmpdir(), "ds-conformance-"));
  proc = spawn("bun", ["run", "src/server.ts", "--object-store", "local"], {
    env: {
      ...process.env,
      PORT: "8787",
      DS_ROOT: rootDir,
      DS_DB_PATH: `${rootDir}/wal.sqlite`,
    },
    stdio: "inherit",
  });

  await waitForHealth(baseUrl, 30_000);
}, 60_000);

afterAll(() => {
  try {
    proc?.kill("SIGKILL");
  } catch {
    // ignore
  }
  proc = null;
  if (rootDir) {
    try {
      rmSync(rootDir, { recursive: true, force: true });
    } catch {
      // ignore
    }
  }
  rootDir = null;
}, 60_000);

runConformanceTests({ baseUrl });
