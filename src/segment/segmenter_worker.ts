import { parentPort, workerData, threadId } from "node:worker_threads";
import type { Config } from "../config.ts";
import { SqliteDurableStore } from "../db/db.ts";
import type { HostRuntime } from "../runtime/host_runtime.ts";
import { RuntimeMemorySampler } from "../runtime_memory_sampler.ts";
import { setSqliteRuntimeOverride } from "../sqlite/adapter.ts";
import { Segmenter, type SegmenterHooks, type SegmenterOptions } from "./segmenter.ts";
import type { SegmentCommitArgs, SegmenterControlRequest, SegmenterControlResponse } from "./segmenter_control.ts";
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

let nextRequestId = 1;
const pendingRequests = new Map<
  number,
  {
    resolve: (value: any) => void;
    reject: (error: Error) => void;
  }
>();

function requestClaim(stream: string): Promise<boolean> {
  return new Promise<boolean>((resolve, reject) => {
    const requestId = nextRequestId++;
    pendingRequests.set(requestId, { resolve, reject });
    try {
      parentPort?.postMessage({
        type: "segmenter-control-request",
        requestId,
        op: "claim",
        stream,
      } satisfies SegmenterControlRequest);
    } catch (error: any) {
      pendingRequests.delete(requestId);
      reject(error instanceof Error ? error : new Error(String(error?.message ?? error)));
    }
  });
}

function requestRelease(stream: string): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const requestId = nextRequestId++;
    pendingRequests.set(requestId, { resolve, reject });
    try {
      parentPort?.postMessage({
        type: "segmenter-control-request",
        requestId,
        op: "release",
        stream,
      } satisfies SegmenterControlRequest);
    } catch (error: any) {
      pendingRequests.delete(requestId);
      reject(error instanceof Error ? error : new Error(String(error?.message ?? error)));
    }
  });
}

function requestCommit(row: SegmentCommitArgs): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const requestId = nextRequestId++;
    pendingRequests.set(requestId, { resolve, reject });
    try {
      parentPort?.postMessage({
        type: "segmenter-control-request",
        requestId,
        op: "commit",
        row,
      } satisfies SegmenterControlRequest);
    } catch (error: any) {
      pendingRequests.delete(requestId);
      reject(error instanceof Error ? error : new Error(String(error?.message ?? error)));
    }
  });
}

const hooks: SegmenterHooks = {
  onSegmentSealed: (stream, payloadBytes, segmentBytes) => {
    parentPort?.postMessage({ type: "sealed", stream, payloadBytes, segmentBytes });
  },
  control: {
    tryClaimSegment: (stream) => requestClaim(stream),
    commitSealedSegment: (row: SegmentCommitArgs) => requestCommit(row),
    releaseSegmentClaim: (stream) => requestRelease(stream),
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
  if (msg.type === "segmenter-control-response") {
    const response = msg as SegmenterControlResponse;
    const pending = pendingRequests.get(response.requestId);
    if (!pending) return;
    pendingRequests.delete(response.requestId);
    if (response.ok) {
      pending.resolve(response.claimed);
    } else {
      const error = new Error(response.errorMessage);
      (error as any).code = response.errorCode;
      (error as any).errno = response.errorErrno;
      pending.reject(error);
    }
    return;
  }
  if (msg.type === "stop") {
    try {
      clearInterval(memoryTimer);
    } catch {
      // ignore
    }
    try {
      for (const [, pending] of pendingRequests) {
        pending.reject(new Error("segmenter worker stopping"));
      }
      pendingRequests.clear();
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
