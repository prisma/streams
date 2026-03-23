import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";
import { Result } from "better-result";
import type { Config } from "../config";
import { detectHostRuntime } from "../runtime/host_runtime.ts";
import type { ProcessRequest, ProcessResult, WorkerMessage } from "./worker_protocol";
import { dsError } from "../util/ds_error.ts";

type Pending = {
  resolve: (r: Result<ProcessResult, WorkerPoolProcessError>) => void;
};

export type WorkerPoolProcessError = {
  kind: "worker_pool_unavailable" | "worker_pool_failure";
  message: string;
};

export class TouchInterpreterWorkerPool {
  private readonly cfg: Config;
  private readonly workerCount: number;
  private readonly workers: Array<{ worker: Worker; busy: boolean; currentId: number | null }> = [];
  private started = false;
  private generation = 0;
  private nextId = 1;
  private readonly pending = new Map<number, Pending>();
  private readonly queue: Array<Omit<ProcessRequest, "type" | "id"> & { id: number }> = [];

  constructor(cfg: Config, workerCount: number) {
    this.cfg = cfg;
    this.workerCount = Math.max(0, Math.floor(workerCount));
  }

  start(): void {
    if (this.started) return;
    this.started = true;
    this.generation += 1;
    const generation = this.generation;
    for (let i = 0; i < this.workerCount; i++) this.spawnWorker(i, generation);
  }

  stop(): void {
    if (!this.started) return;
    this.started = false;
    this.generation += 1;
    for (const w of this.workers) {
      try {
        w.worker.postMessage({ type: "stop" });
      } catch {
        // ignore
      }
      void w.worker.terminate();
    }
    this.workers.length = 0;
    this.queue.length = 0;
    for (const [id, p] of this.pending.entries()) {
      p.resolve(Result.err({ kind: "worker_pool_failure", message: "worker pool stopped" }));
      this.pending.delete(id);
    }
  }

  restart(): void {
    this.stop();
    this.start();
  }

  async processResult(req: Omit<ProcessRequest, "type" | "id">): Promise<Result<ProcessResult, WorkerPoolProcessError>> {
    if (!this.started) {
      return Result.err({ kind: "worker_pool_unavailable", message: "worker pool not started" });
    }
    if (this.workerCount === 0) {
      return Result.err({ kind: "worker_pool_unavailable", message: "worker pool disabled" });
    }
    const id = this.nextId++;
    const queued = { ...req, id };
    const value = await new Promise<Result<ProcessResult, WorkerPoolProcessError>>((resolve) => {
      this.pending.set(id, { resolve });
      this.queue.push(queued);
      this.pump();
    });
    return value;
  }

  async process(req: Omit<ProcessRequest, "type" | "id">): Promise<ProcessResult> {
    const res = await this.processResult(req);
    if (Result.isError(res)) throw dsError(res.error.message);
    return res.value;
  }

  private pump(): void {
    if (!this.started) return;
    if (this.queue.length === 0) return;
    const slot = this.workers.find((w) => !w.busy);
    if (!slot) return;
    const next = this.queue.shift();
    if (!next) return;
    slot.busy = true;
    slot.currentId = next.id;
    slot.worker.postMessage({
      type: "process",
      id: next.id,
      stream: next.stream,
      fromOffset: next.fromOffset,
      toOffset: next.toOffset,
      interpreter: next.interpreter,
      maxRows: next.maxRows,
      maxBytes: next.maxBytes,
      emitFineTouches: next.emitFineTouches,
      fineTouchBudget: next.fineTouchBudget,
      fineGranularity: next.fineGranularity,
      interpretMode: next.interpretMode,
      filterHotTemplates: next.filterHotTemplates,
      hotTemplateIds: next.hotTemplateIds,
    } satisfies ProcessRequest);
  }

  private spawnWorker(idx: number, generation: number = this.generation): void {
    const workerUrl = new URL("./interpreter_worker.ts", import.meta.url);
    let workerSpec = fileURLToPath(workerUrl);
    if (!existsSync(workerSpec)) {
      const fallback = resolve(process.cwd(), "src/touch/interpreter_worker.ts");
      if (existsSync(fallback)) workerSpec = fallback;
    }

    const worker = new Worker(workerSpec, {
      workerData: { config: this.cfg, hostRuntime: detectHostRuntime() },
      type: "module",
      smol: true,
    } as any);

    const slot = { worker, busy: false, currentId: null };
    this.workers.push(slot);

    worker.on("message", (msg: WorkerMessage) => {
      if (generation !== this.generation) return;
      if (!msg || typeof msg !== "object") return;
      if (msg.type === "result") {
        const p = this.pending.get(msg.id);
        if (p) {
          this.pending.delete(msg.id);
          slot.busy = false;
          slot.currentId = null;
          p.resolve(Result.ok(msg));
        }
        this.pump();
        return;
      }
      if (msg.type === "error") {
        const p = this.pending.get(msg.id);
        if (p) {
          this.pending.delete(msg.id);
          slot.busy = false;
          slot.currentId = null;
          p.resolve(Result.err({ kind: "worker_pool_failure", message: msg.message }));
        }
        this.pump();
      }
    });

    worker.on("error", (err) => {
      if (generation !== this.generation) return;
      // eslint-disable-next-line no-console
      console.error(`touch interpreter worker ${idx} error`, err);
    });

    worker.on("exit", (code) => {
      if (generation !== this.generation || !this.started) return;
      // eslint-disable-next-line no-console
      console.error(`touch interpreter worker ${idx} exited with code ${code}, respawning`);
      if (slot.currentId != null) {
        const p = this.pending.get(slot.currentId);
        if (p) {
          this.pending.delete(slot.currentId);
          p.resolve(Result.err({ kind: "worker_pool_failure", message: "worker exited" }));
        }
      }
      slot.busy = false;
      slot.currentId = null;
      try {
        const widx = this.workers.indexOf(slot);
        if (widx >= 0) this.workers.splice(widx, 1);
      } catch {
        // ignore
      }
      this.spawnWorker(idx, generation);
      this.pump();
    });
  }
}
