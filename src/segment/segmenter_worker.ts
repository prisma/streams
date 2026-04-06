import { parentPort, workerData, threadId } from "node:worker_threads";
import type { Config } from "../config.ts";
import { SqliteDurableStore } from "../db/db.ts";
import type { HostRuntime } from "../runtime/host_runtime.ts";
import { RuntimeMemorySampler } from "../runtime_memory_sampler.ts";
import { setSqliteRuntimeOverride } from "../sqlite/adapter.ts";
import { Segmenter, type SegmenterHooks, type SegmenterOptions } from "./segmenter.ts";
import { initConsoleLogging } from "../util/log.ts";

initConsoleLogging();

const data = workerData as { config: Config; hostRuntime?: HostRuntime; opts?: SegmenterOptions };
const cfg = data.config;
setSqliteRuntimeOverride(data.hostRuntime ?? null);
// The main server process initializes/migrates schema; workers should avoid
// concurrent migrations on the same sqlite file.
const db = new SqliteDurableStore(cfg.dbPath, { cacheBytes: cfg.workerSqliteCacheBytes, skipMigrations: true });
const memorySampler =
  cfg.memorySamplerPath != null
    ? new RuntimeMemorySampler(cfg.memorySamplerPath, {
        intervalMs: cfg.memorySamplerIntervalMs,
        scope: `segmenter-worker-${threadId}`,
      })
    : undefined;
memorySampler?.start();

const hooks: SegmenterHooks = {
  onSegmentSealed: (stream, payloadBytes, segmentBytes) => {
    parentPort?.postMessage({ type: "sealed", stream, payloadBytes, segmentBytes });
  },
};

const segmenter = new Segmenter(cfg, db, data.opts ?? {}, hooks, memorySampler);
segmenter.start();
const memoryTimer = setInterval(() => {
  try {
    parentPort?.postMessage({ type: "memory", workerId: threadId, stats: segmenter.getMemoryStats() });
  } catch {
    // ignore
  }
}, 1_000);

parentPort?.on("message", (msg: any) => {
  if (!msg || typeof msg !== "object") return;
  if (msg.type === "stop") {
    try {
      clearInterval(memoryTimer);
    } catch {
      // ignore
    }
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
      memorySampler?.stop();
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
