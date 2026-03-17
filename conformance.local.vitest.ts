import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { afterAll, beforeAll } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { startLocalDurableStreamsServer, type DurableStreamsLocalServer } from "./src/local/index";

let server: DurableStreamsLocalServer | null = null;
let localRoot: string | null = null;
let prevLocalRoot: string | undefined;

const baseUrl = process.env.CONFORMANCE_TEST_URL ?? "http://127.0.0.1:8787";

beforeAll(async () => {
  if (process.env.CONFORMANCE_TEST_URL) return;

  localRoot = mkdtempSync(join(tmpdir(), "ds-local-conformance-"));
  prevLocalRoot = process.env.DS_LOCAL_DATA_ROOT;
  process.env.DS_LOCAL_DATA_ROOT = localRoot;

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
