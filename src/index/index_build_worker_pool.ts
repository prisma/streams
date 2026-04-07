import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";
import { Result } from "better-result";
import type { IndexBuildJobInput, IndexBuildJobOutput, IndexBuildWorkerError } from "./index_build_job";

type Pending = {
  resolve: (result: Result<IndexBuildJobOutput, IndexBuildWorkerError>) => void;
};

type WorkerMessage =
  | { type: "result"; id: number; result: IndexBuildJobOutput }
  | { type: "error"; id: number; message: string };

type WorkerSlot = {
  worker: Worker;
  busy: boolean;
  currentId: number | null;
};

export class IndexBuildWorkerPool {
  private readonly workers: WorkerSlot[] = [];
  private readonly queue: Array<{ id: number; job: IndexBuildJobInput }> = [];
  private readonly pending = new Map<number, Pending>();
  private started = false;
  private nextId = 1;

  constructor(private readonly workerCount: number) {}

  start(): void {
    if (this.started) return;
    this.started = true;
    const count = Math.max(1, this.workerCount);
    for (let index = 0; index < count; index += 1) this.spawnWorker(index);
  }

  stop(): void {
    this.started = false;
    for (const slot of this.workers) {
      try {
        slot.worker.postMessage({ type: "stop" });
      } catch {
        // ignore
      }
      void slot.worker.terminate();
    }
    this.workers.length = 0;
    this.queue.length = 0;
    for (const [id, pending] of this.pending.entries()) {
      pending.resolve(Result.err({ kind: "worker_pool_failure", message: "index build worker pool stopped" }));
      this.pending.delete(id);
    }
  }

  async buildResult(job: IndexBuildJobInput): Promise<Result<IndexBuildJobOutput, IndexBuildWorkerError>> {
    if (!this.started || this.workers.length === 0) {
      return Result.err({ kind: "worker_pool_failure", message: "index build worker pool not started" });
    }
    const id = this.nextId++;
    return await new Promise((resolve) => {
      this.pending.set(id, { resolve });
      this.queue.push({ id, job });
      this.pump();
    });
  }

  private pump(): void {
    if (!this.started) return;
    for (const slot of this.workers) {
      if (slot.busy) continue;
      const next = this.queue.shift();
      if (!next) return;
      slot.busy = true;
      slot.currentId = next.id;
      slot.worker.postMessage({ type: "build", id: next.id, job: next.job });
    }
  }

  private spawnWorker(index: number): void {
    const workerSpec = fileURLToPath(new URL("./index_build_worker.ts", import.meta.url));
    const worker = new Worker(workerSpec, {
      type: "module",
      smol: true,
    } as any);
    const slot: WorkerSlot = { worker, busy: false, currentId: null };
    this.workers[index] = slot;

    worker.on("message", (msg: WorkerMessage) => {
      if (!msg || typeof msg !== "object") return;
      const pending = this.pending.get(msg.id);
      if (!pending) return;
      this.pending.delete(msg.id);
      slot.busy = false;
      slot.currentId = null;
      if (msg.type === "error") {
        pending.resolve(Result.err({ kind: "worker_pool_failure", message: msg.message }));
      } else {
        pending.resolve(Result.ok(msg.result));
      }
      this.pump();
    });

    worker.on("error", (err) => {
      console.error("index build worker error", err);
    });

    worker.on("exit", () => {
      const failedId = slot.currentId;
      slot.busy = false;
      slot.currentId = null;
      if (failedId != null) {
        const pending = this.pending.get(failedId);
        if (pending) {
          this.pending.delete(failedId);
          pending.resolve(Result.err({ kind: "worker_pool_failure", message: "index build worker exited" }));
        }
      }
      if (!this.started) return;
      this.spawnWorker(index);
      this.pump();
    });
  }
}
