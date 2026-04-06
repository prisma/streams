import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { afterAll, beforeAll } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { startLocalDurableStreamsServer, type DurableStreamsLocalServer } from "./src/local/index";

let server: DurableStreamsLocalServer | null = null;
let localRoot: string | null = null;
let prevLocalRoot: string | undefined;
let prevTouchWorkers: string | undefined;

const baseUrl = process.env.CONFORMANCE_TEST_URL ?? "http://127.0.0.1:8787";

beforeAll(async () => {
  if (process.env.CONFORMANCE_TEST_URL) return;

  localRoot = mkdtempSync(join(tmpdir(), "ds-local-conformance-"));
  prevLocalRoot = process.env.DS_LOCAL_DATA_ROOT;
  prevTouchWorkers = process.env.DS_TOUCH_WORKERS;
  process.env.DS_LOCAL_DATA_ROOT = localRoot;
  process.env.DS_TOUCH_WORKERS = "0";

  server = await startLocalDurableStreamsServer({
    name: "conformance",
    hostname: "127.0.0.1",
    port: 8787,
  });
}, 60_000);

afterAll(async () => {
  try {
    await server?.close();
  } catch {
    // ignore
  }
  server = null;

  if (prevLocalRoot == null) delete process.env.DS_LOCAL_DATA_ROOT;
  else process.env.DS_LOCAL_DATA_ROOT = prevLocalRoot;
  if (prevTouchWorkers == null) delete process.env.DS_TOUCH_WORKERS;
  else process.env.DS_TOUCH_WORKERS = prevTouchWorkers;

  if (localRoot) {
    try {
      rmSync(localRoot, { recursive: true, force: true });
    } catch {
      // ignore
    }
  }
  localRoot = null;
}, 60_000);

runConformanceTests({ baseUrl });
