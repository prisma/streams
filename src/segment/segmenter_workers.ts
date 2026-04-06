import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";
import type { Config } from "../config";
import { detectHostRuntime } from "../runtime/host_runtime.ts";
import type { SegmenterHooks, SegmenterMemoryStats, SegmenterOptions } from "./segmenter";

export type SegmenterController = {
  start: () => void;
  stop: (hard?: boolean) => void;
  getMemoryStats?: () => SegmenterMemoryStats;
};

type WorkerMessage =
  | { type: "sealed"; stream: string; payloadBytes: number; segmentBytes: number }
  | { type: "memory"; workerId: number; stats: SegmenterMemoryStats }
  | { type: "stopped" };

export class SegmenterWorkerPool implements SegmenterController {
  private readonly config: Config;
  private readonly workerCount: number;
  private readonly opts: SegmenterOptions;
  private readonly hooks?: SegmenterHooks;
  private readonly workers: Worker[] = [];
  private readonly workerMemory = new Map<number, { stats: SegmenterMemoryStats; reportedAtMs: number }>();
  private started = false;

  constructor(config: Config, workerCount: number, opts: SegmenterOptions = {}, hooks?: SegmenterHooks) {
    this.config = config;
    this.workerCount = Math.max(0, Math.floor(workerCount));
    this.opts = opts;
    this.hooks = hooks;
  }

  start(): void {
    if (this.started) return;
    this.started = true;
    for (let i = 0; i < this.workerCount; i++) {
      this.spawnWorker(i);
    }
  }

  stop(_hard?: boolean): void {
    if (!this.started) return;
    this.started = false;
    for (const w of this.workers) {
      try {
        w.postMessage({ type: "stop" });
      } catch {
        // ignore
      }
      void w.terminate();
    }
    this.workers.length = 0;
    this.workerMemory.clear();
  }

  getMemoryStats(): SegmenterMemoryStats {
    const now = Date.now();
    let activeBuilds = 0;
    let activeStreams = 0;
    let activePayloadBytes = 0;
    let activeSegmentBytesEstimate = 0;
    let activeRows = 0;
    for (const [workerId, entry] of this.workerMemory) {
      if (now - entry.reportedAtMs > 5_000) {
        this.workerMemory.delete(workerId);
        continue;
      }
      activeBuilds += Math.max(0, entry.stats.active_builds);
      activeStreams += Math.max(0, entry.stats.active_streams);
      activePayloadBytes += Math.max(0, entry.stats.active_payload_bytes);
      activeSegmentBytesEstimate += Math.max(0, entry.stats.active_segment_bytes_estimate);
      activeRows += Math.max(0, entry.stats.active_rows);
    }
    return {
      active_builds: activeBuilds,
      active_streams: activeStreams,
      active_payload_bytes: activePayloadBytes,
      active_segment_bytes_estimate: activeSegmentBytesEstimate,
      active_rows: activeRows,
    };
  }

  private spawnWorker(idx: number): void {
    const workerSpec = fileURLToPath(new URL("./segmenter_worker.ts", import.meta.url));
    const worker = new Worker(workerSpec, {
      workerData: {
        config: this.config,
        hostRuntime: detectHostRuntime(),
        opts: this.opts,
      },
      type: "module",
      smol: true,
    } as any);

    worker.on("message", (msg: WorkerMessage) => {
      if (msg?.type === "sealed") {
        this.hooks?.onSegmentSealed?.(msg.stream, msg.payloadBytes, msg.segmentBytes);
      } else if (msg?.type === "memory") {
        this.workerMemory.set(msg.workerId, {
          stats: msg.stats,
          reportedAtMs: Date.now(),
        });
      }
    });

    worker.on("error", (err) => {
      // eslint-disable-next-line no-console
      console.error(`segmenter worker ${idx} error`, err);
    });

    worker.on("exit", (code) => {
      this.workerMemory.delete(worker.threadId);
      if (!this.started) return;
      if (code !== 0) {
        // eslint-disable-next-line no-console
        console.error(`segmenter worker ${idx} exited with code ${code}, respawning`);
        this.spawnWorker(idx);
      }
    });

    this.workers.push(worker);
  }
}
