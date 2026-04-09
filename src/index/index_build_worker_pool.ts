import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Result } from "better-result";
import type { IndexBuildJobInput, IndexBuildJobOutput, IndexBuildJobResult, IndexBuildWorkerError } from "./index_build_job";
import { beginIndexBuildTelemetrySampling } from "./index_build_telemetry";
import { decodeIndexBuildWire, encodeIndexBuildWire } from "./index_build_wire";

type Pending = {
  resolve: (result: Result<IndexBuildJobResult, IndexBuildWorkerError>) => void;
};

type SubprocessOutput =
  | { ok: true; result: IndexBuildJobOutput }
  | { ok: false; message: string };

type WorkerSlot = {
  busy: boolean;
  currentId: number | null;
  proc: Bun.Subprocess | null;
  tempDir: string | null;
  stopTelemetry: (() => Promise<IndexBuildJobResult["telemetry"]>) | null;
};

function readPipeText(pipe: number | ReadableStream<Uint8Array> | null | undefined): Promise<string> {
  if (pipe == null || typeof pipe === "number") return Promise.resolve("");
  return new Response(pipe).text();
}

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
    for (let index = 0; index < count; index += 1) {
      this.workers[index] = {
        busy: false,
        currentId: null,
        proc: null,
        tempDir: null,
        stopTelemetry: null,
      };
    }
  }

  stop(): void {
    this.started = false;
    for (const slot of this.workers) {
      try {
        slot.proc?.kill();
      } catch {
        // ignore
      }
      if (slot.tempDir) {
        try {
          rmSync(slot.tempDir, { recursive: true, force: true });
        } catch {
          // ignore
        }
      }
      slot.proc = null;
      slot.tempDir = null;
      slot.stopTelemetry = null;
      slot.busy = false;
      slot.currentId = null;
    }
    this.workers.length = 0;
    this.queue.length = 0;
    for (const [id, pending] of this.pending.entries()) {
      pending.resolve(Result.err({ kind: "worker_pool_failure", message: "index build worker pool stopped" }));
      this.pending.delete(id);
    }
  }

  async buildResult(job: IndexBuildJobInput): Promise<Result<IndexBuildJobResult, IndexBuildWorkerError>> {
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
    for (let index = 0; index < this.workers.length; index += 1) {
      const slot = this.workers[index]!;
      if (slot.busy) continue;
      const next = this.queue.shift();
      if (!next) return;
      slot.busy = true;
      slot.currentId = next.id;
      this.spawnJob(index, next.id, next.job);
    }
  }

  private spawnJob(index: number, id: number, job: IndexBuildJobInput): void {
    const slot = this.workers[index]!;
    const tempDir = mkdtempSync(join(tmpdir(), "ds-index-build-job-"));
    const inputPath = join(tempDir, "job.json");
    const outputPath = join(tempDir, "result.json");
    const subprocessSpec = fileURLToPath(new URL("./index_build_subprocess.ts", import.meta.url));
    writeFileSync(inputPath, encodeIndexBuildWire(job));
    const env: Record<string, string> = {};
    for (const [name, value] of Object.entries(process.env)) {
      if (value != null) env[name] = value;
    }
    const proc = Bun.spawn({
      cmd: [process.execPath, "--smol", "run", subprocessSpec, inputPath, outputPath],
      cwd: process.cwd(),
      stdout: "pipe",
      stderr: "pipe",
      env,
    });
    slot.proc = proc;
    slot.tempDir = tempDir;
    slot.stopTelemetry = beginIndexBuildTelemetrySampling(proc.pid).stop;
    void this.awaitJobResult(index, id, outputPath, tempDir);
  }

  private async awaitJobResult(index: number, id: number, outputPath: string, tempDir: string): Promise<void> {
    const slot = this.workers[index]!;
    const proc = slot.proc;
    if (!proc) return;
    const [exitCode, stdout, stderr] = await Promise.all([
      proc.exited,
      readPipeText(proc.stdout),
      readPipeText(proc.stderr),
    ]);

    const telemetry = slot.stopTelemetry ? await slot.stopTelemetry() : null;
    let result: Result<IndexBuildJobResult, IndexBuildWorkerError>;
    if (exitCode !== 0) {
      const details = [stderr.trim(), stdout.trim()].filter((value) => value.length > 0).join("\n");
      result = Result.err({
        kind: "worker_pool_failure",
        message: details.length > 0 ? details : `index build subprocess exited with code ${exitCode}`,
      });
    } else {
      try {
        const output = decodeIndexBuildWire<SubprocessOutput>(readFileSync(outputPath, "utf8"));
        result = output.ok
          ? Result.ok({ output: output.result, telemetry })
          : Result.err({ kind: "worker_pool_failure", message: output.message });
      } catch (error: unknown) {
        result = Result.err({
          kind: "worker_pool_failure",
          message: String((error as Error)?.message ?? error),
        });
      }
    }

    this.finishJob(index, id, result, tempDir);
  }

  private finishJob(
    index: number,
    id: number,
    result: Result<IndexBuildJobResult, IndexBuildWorkerError>,
    tempDir: string
  ): void {
    const slot = this.workers[index];
    const pending = this.pending.get(id);
    try {
      rmSync(tempDir, { recursive: true, force: true });
    } catch {
      // ignore cleanup failures
    }
    if (slot) {
      slot.proc = null;
      slot.tempDir = null;
      slot.stopTelemetry = null;
      slot.busy = false;
      slot.currentId = null;
    }
    if (pending) {
      this.pending.delete(id);
      pending.resolve(result);
    }
    if (this.started) this.pump();
  }
}
