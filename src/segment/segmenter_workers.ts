import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";
import type { Config } from "../config";
import { detectHostRuntime } from "../runtime/host_runtime.ts";
import type { SegmenterHooks, SegmenterOptions } from "./segmenter";

export type SegmenterController = {
  start: () => void;
  stop: (hard?: boolean) => void;
};

type WorkerMessage =
  | { type: "sealed"; payloadBytes: number; segmentBytes: number }
  | { type: "stopped" };

export class SegmenterWorkerPool implements SegmenterController {
  private readonly config: Config;
  private readonly workerCount: number;
  private readonly opts: SegmenterOptions;
  private readonly hooks?: SegmenterHooks;
  private readonly workers: Worker[] = [];
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
        this.hooks?.onSegmentSealed?.(msg.payloadBytes, msg.segmentBytes);
      }
    });

    worker.on("error", (err) => {
      // eslint-disable-next-line no-console
      console.error(`segmenter worker ${idx} error`, err);
    });

    worker.on("exit", (code) => {
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
