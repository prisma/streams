import { parentPort, workerData } from "node:worker_threads";
import type { Config } from "../config.ts";
import { SqliteDurableStore } from "../db/db.ts";
import { Segmenter, type SegmenterHooks, type SegmenterOptions } from "./segmenter.ts";
import { initConsoleLogging } from "../util/log.ts";

initConsoleLogging();

const data = workerData as { config: Config; opts?: SegmenterOptions };
const cfg = data.config;
// The main server process initializes/migrates schema; workers should avoid
// concurrent migrations on the same sqlite file.
const db = new SqliteDurableStore(cfg.dbPath, { cacheBytes: cfg.sqliteCacheBytes, skipMigrations: true });

const hooks: SegmenterHooks = {
  onSegmentSealed: (payloadBytes, segmentBytes) => {
    parentPort?.postMessage({ type: "sealed", payloadBytes, segmentBytes });
  },
};

const segmenter = new Segmenter(cfg, db, data.opts ?? {}, hooks);
segmenter.start();

parentPort?.on("message", (msg: any) => {
  if (!msg || typeof msg !== "object") return;
  if (msg.type === "stop") {
    try {
      segmenter.stop();
    } catch {
      // ignore
    }
    try {
      db.close();
    } catch {
      // ignore
    }
    try {
      parentPort?.postMessage({ type: "stopped" });
    } catch {
      // ignore
    }
  }
});
