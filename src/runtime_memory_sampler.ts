import { mkdirSync, createWriteStream, type WriteStream } from "node:fs";
import { dirname, extname } from "node:path";
import { isMainThread, threadId } from "node:worker_threads";
import { readLinuxStatusRssBreakdown } from "./runtime_memory";

type BunJscModule = {
  heapStats?: () => unknown;
  memoryUsage?: () => unknown;
};

type PhaseMeta = Record<string, unknown>;

type ActivePhase = {
  key: number;
  label: string;
  startedAtMs: number;
  meta: PhaseMeta;
};

function isPromiseLike<T>(value: unknown): value is PromiseLike<T> {
  return typeof value === "object" && value != null && "then" in value && typeof (value as { then?: unknown }).then === "function";
}

function insertScopeSuffix(path: string, scope: string): string {
  if (scope === "main") return path;
  const extension = extname(path);
  if (extension === "") return `${path}.${scope}`;
  return `${path.slice(0, -extension.length)}.${scope}${extension}`;
}

export class RuntimeMemorySampler {
  private readonly outputPath: string;
  private readonly intervalMs: number;
  private readonly scope: string;
  private stream: WriteStream | null = null;
  private timer: any | null = null;
  private activePhases = new Map<number, ActivePhase>();
  private nextPhaseKey = 0;
  private jscModule: BunJscModule | null | undefined;
  private sampling = false;
  private subsystemProvider: (() => unknown) | null = null;

  constructor(path: string, opts: { intervalMs?: number; scope?: string } = {}) {
    this.scope = opts.scope?.trim() || "main";
    this.outputPath = insertScopeSuffix(path, this.scope);
    this.intervalMs = Math.max(100, opts.intervalMs ?? 1_000);
  }

  start(): void {
    if (this.stream) return;
    mkdirSync(dirname(this.outputPath), { recursive: true });
    this.stream = createWriteStream(this.outputPath, { flags: "a" });
    this.writeLine({
      ts: new Date().toISOString(),
      kind: "sampler_start",
      scope: this.scope,
      pid: process.pid,
      thread_id: threadId,
      is_main_thread: isMainThread,
    });
    void this.sample("startup");
    this.timer = setInterval(() => {
      void this.sample("interval");
    }, this.intervalMs);
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    void this.sample("shutdown");
    this.writeLine({
      ts: new Date().toISOString(),
      kind: "sampler_stop",
      scope: this.scope,
      pid: process.pid,
      thread_id: threadId,
      is_main_thread: isMainThread,
    });
    this.stream?.end();
    this.stream = null;
  }

  enter(label: string, meta: PhaseMeta = {}): () => void {
    if (!this.stream) return () => {};
    const key = ++this.nextPhaseKey;
    const phase: ActivePhase = {
      key,
      label,
      startedAtMs: Date.now(),
      meta,
    };
    this.activePhases.set(key, phase);
    this.writeLine({
      ts: new Date().toISOString(),
      kind: "phase_enter",
      scope: this.scope,
      pid: process.pid,
      thread_id: threadId,
      label,
      meta,
      active_phase_count: this.activePhases.size,
    });
    void this.sample("phase_enter", { label, meta });
    return () => {
      const current = this.activePhases.get(key);
      if (!current) return;
      this.activePhases.delete(key);
      this.writeLine({
        ts: new Date().toISOString(),
        kind: "phase_exit",
        scope: this.scope,
        pid: process.pid,
        thread_id: threadId,
        label: current.label,
        meta: current.meta,
        duration_ms: Date.now() - current.startedAtMs,
        active_phase_count: this.activePhases.size,
      });
      void this.sample("phase_exit", { label: current.label, meta: current.meta });
    };
  }

  capture(reason: string, data: PhaseMeta = {}): void {
    if (!this.stream) return;
    this.writeLine({
      ts: new Date().toISOString(),
      kind: "marker",
      scope: this.scope,
      pid: process.pid,
      thread_id: threadId,
      reason,
      data,
    });
    void this.sample(reason, data);
  }

  setSubsystemProvider(provider: (() => unknown) | null): void {
    this.subsystemProvider = provider;
  }

  track<T>(label: string, meta: PhaseMeta, fn: () => T): T {
    const leave = this.enter(label, meta);
    try {
      const result = fn();
      if (isPromiseLike(result)) {
        return (Promise.resolve(result).finally(() => leave()) as unknown) as T;
      }
      leave();
      return result;
    } catch (error) {
      leave();
      throw error;
    }
  }

  private async sample(reason: string, data: PhaseMeta = {}): Promise<void> {
    if (!this.stream || this.sampling) return;
    this.sampling = true;
    try {
      const activePhases = Array.from(this.activePhases.values()).map((phase) => ({
        key: phase.key,
        label: phase.label,
        started_at: new Date(phase.startedAtMs).toISOString(),
        duration_ms: Date.now() - phase.startedAtMs,
        meta: phase.meta,
      }));
      const primary = activePhases.length > 0 ? activePhases[activePhases.length - 1]! : null;
      this.writeLine({
        ts: new Date().toISOString(),
        kind: "sample",
        scope: this.scope,
        pid: process.pid,
        thread_id: threadId,
        is_main_thread: isMainThread,
        reason,
        data,
        process_memory_usage: process.memoryUsage(),
        linux_status_rss: readLinuxStatusRssBreakdown(0),
        jsc_heap_stats: await this.readJscHeapStats(),
        jsc_memory_usage: await this.readJscMemoryUsage(),
        memory_subsystems: this.readSubsystems(),
        active_phases: activePhases,
        primary_phase: primary ? { label: primary.label, duration_ms: primary.duration_ms, meta: primary.meta } : null,
      });
    } finally {
      this.sampling = false;
    }
  }

  private async readJscHeapStats(): Promise<unknown> {
    const jsc = await this.loadJscModule();
    if (!jsc || typeof jsc.heapStats !== "function") return null;
    try {
      return jsc.heapStats();
    } catch {
      return null;
    }
  }

  private async readJscMemoryUsage(): Promise<unknown> {
    const jsc = await this.loadJscModule();
    if (!jsc || typeof jsc.memoryUsage !== "function") return null;
    try {
      return jsc.memoryUsage();
    } catch {
      return null;
    }
  }

  private async loadJscModule(): Promise<BunJscModule | null> {
    if (this.jscModule !== undefined) return this.jscModule;
    if (typeof (globalThis as { Bun?: unknown }).Bun === "undefined") {
      this.jscModule = null;
      return this.jscModule;
    }
    try {
      this.jscModule = (await import("bun:jsc")) as BunJscModule;
    } catch {
      this.jscModule = null;
    }
    return this.jscModule;
  }

  private writeLine(payload: Record<string, unknown>): void {
    if (!this.stream) return;
    this.stream.write(`${JSON.stringify(payload)}\n`);
  }

  private readSubsystems(): unknown {
    if (!this.subsystemProvider) return null;
    try {
      return this.subsystemProvider();
    } catch {
      return null;
    }
  }
}
