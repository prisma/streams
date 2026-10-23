/**
 * The worker runner behind `Queue.process()` and `Queue.handleWakeup()`
 * (docs/QUEUES-DESIGN.md §9.1, §9.2): it claims as many messages as it has
 * free handler slots (at most `maxBatch`), runs the handler on each,
 * extends a running handler's lease at half the lease, and settles the
 * outcomes in batches coalesced within 250 ms.
 *
 * This module imports only types from queue.ts, so the two never meet in a
 * module cycle at load time.
 */

import type { ClaimOptions, ClaimResult, QueueMessage, SettleOp, SettleOutcome } from "./queue.js";

// ---------------------------------------------------------------------
// Durations

/** Milliseconds as a number, or text such as `"250ms"`, `"30s"`, `"5m"`,
 * `"2h"` or `"1d"`. */
export type Duration = number | string;

const DURATION = /^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)$/;
const UNIT_MS: Record<string, number> = { ms: 1, s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000 };

/** A `Duration` in whole milliseconds, or a `RangeError` naming `what`. */
export function durationMs(value: Duration, what = "duration"): number {
  if (typeof value === "number") {
    if (Number.isFinite(value) && value >= 0) return Math.round(value);
  } else if (typeof value === "string") {
    const m = DURATION.exec(value.trim());
    if (m) return Math.round(Number(m[1]) * UNIT_MS[m[2]]);
  }
  throw new RangeError(`${what} must be a non-negative number of milliseconds or text such as "30s", "5m" or "2h"`);
}

// ---------------------------------------------------------------------
// What a handler can answer

/** Thrown by a handler: settle the message as `retry` with this delay. Any
 * other error is a `retry` with the queue's backoff policy. */
export class RetryLater extends Error {
  /** The delay in milliseconds. */
  readonly after: number;
  /** An optional settle code (≤ 64 chars of `[a-z0-9_.-]`). */
  readonly code?: string;
  constructor(options: { after: Duration; code?: string; message?: string }) {
    super(options.message ?? `retry later (after ${String(options.after)})`);
    this.name = "RetryLater";
    this.after = durationMs(options.after, "RetryLater after");
    this.code = options.code;
  }
}

/** A handler's explicit outcome, made by `msg.fail()` or `msg.discard()`
 * and returned from the handler. */
export class SettleDecision {
  constructor(
    readonly op: "fail" | "discard",
    readonly code?: string,
    readonly error?: string,
  ) {}
}

/** The reason a handler's signal fires: its lease was lost (an extend was
 * answered `expired` or `stale`), or the worker stopped before it ended. */
export class HandlerAbortedError extends Error {
  constructor(readonly reason: "lease_expired" | "lease_stale" | "worker_stopped") {
    super(reason === "worker_stopped"
      ? "the worker stopped before this handler finished; the message is settled as retry"
      : `the message's lease was lost (${reason === "lease_expired" ? "expired" : "stale"}); another ` +
        "worker may receive it");
    this.name = "HandlerAbortedError";
  }
}

export type Handler<T> = (msg: QueueMessage<T>, context: { signal: AbortSignal }) => unknown;

export interface WorkerOptions<T> {
  /** Handlers running at once (default 1). */
  concurrency?: number;
  /** The lease each claim asks for (default `"30s"`). */
  lease?: Duration;
  /** Extend a running handler's lease at half the lease (default true). */
  autoExtend?: boolean;
  /** At most this many messages per claim (default 100, at most 1,000). */
  maxBatch?: number;
  /** A claim's byte budget (the server's default when absent). */
  maxBytes?: number;
  /** A claim's long poll (default 25,000 ms, at most 25,000). With 0, an
   * empty claim backs off 1 s doubling to 30 s before the next. */
  waitMs?: number;
  /** Called before the handler; an error it throws is the handler's. */
  onMessage?: (msg: QueueMessage<T>) => void | Promise<void>;
  /** Called with each settle operation's answer. */
  onSettle?: (msg: QueueMessage<T>, op: SettleOp, outcome: SettleOutcome) => void;
  /** Called with a claim or settle failure; a claim failure that is not
   * retryable also stops the worker and rejects `closed`. */
  onError?: (error: unknown) => void;
}

export interface Worker {
  /** Stops claiming, waits up to `timeout` (default 30 s) for started
   * handlers, aborts and settles the rest as `retry`, releases claimed work
   * that never started, and sends every pending settle. */
  stop(options?: { timeout?: Duration }): Promise<void>;
  /** Resolves once the worker stopped; rejects with the error that stopped it. */
  readonly closed: Promise<void>;
}

/** The two requests the runner makes (a `Queue` is one). */
export interface QueueOps<T> {
  claim(options: ClaimOptions): Promise<ClaimResult<T>>;
  settle(ops: SettleOp[]): Promise<SettleOutcome[]>;
}

/** Runner options with the queue's own `onInvalid`. */
export type RunnerOptions<T> = WorkerOptions<T> & { onInvalid?: "retry" | "fail" };

const COALESCE_MS = 250;
const MAX_SETTLE_OPS = 1000;
const MAX_WAIT_MS = 25_000;
const BACKOFF_FIRST_MS = 1000;
const BACKOFF_MAX_MS = 30_000;
const DEFAULT_LEASE_MS = 30_000;
const DEFAULT_STOP_MS = 30_000;
const MAX_ERROR_BYTES = 1024;
const CODE = /^[a-z0-9_.-]{1,64}$/;

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted || ms <= 0) return Promise.resolve();
  return new Promise((resolve) => {
    const done = () => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal?.addEventListener("abort", done, { once: true });
  });
}

/** A transport failure, or a refusal the server marked retryable. */
function retryable(error: unknown): boolean {
  return typeof error === "object" && error !== null && (error as { retryable?: unknown }).retryable === true;
}

/** An error's text as a settle's `error` carries it: at most 1 KiB of UTF-8. */
export function errorText(error: unknown): string {
  const text = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
  const bytes = new TextEncoder().encode(text);
  if (bytes.length <= MAX_ERROR_BYTES) return text;
  return new TextDecoder().decode(bytes.subarray(0, MAX_ERROR_BYTES)).replace(/�$/, "");
}

function errorCode(error: unknown, fallback: string): string {
  const code = typeof error === "object" && error !== null ? (error as { code?: unknown }).code : undefined;
  return typeof code === "string" && CODE.test(code) ? code : fallback;
}

// ---------------------------------------------------------------------
// Settle coalescing

interface PendingOp {
  op: SettleOp;
  resolve: (outcome: SettleOutcome | undefined) => void;
  reject: (error: unknown) => void;
}

/** Sends settle operations in one request per 250 ms window (or per 1,000
 * operations). A pending `extend` is dropped once another operation on the
 * same lease joins the window, and resolves `undefined`. */
export class SettleBatcher {
  private pending: PendingOp[] = [];
  private timer?: ReturnType<typeof setTimeout>;
  private flights = new Set<Promise<void>>();

  constructor(private send: (ops: SettleOp[]) => Promise<SettleOutcome[]>) {}

  add(op: SettleOp): Promise<SettleOutcome | undefined> {
    // Any later operation on a lease supersedes its pending extend.
    this.pending = this.pending.filter((p) => {
      if (p.op.op !== "extend" || p.op.token !== op.token) return true;
      p.resolve(undefined);
      return false;
    });
    return new Promise((resolve, reject) => {
      this.pending.push({ op, resolve, reject });
      if (this.pending.length >= MAX_SETTLE_OPS) this.flush();
      else this.timer ??= setTimeout(() => this.flush(), COALESCE_MS);
    });
  }

  flush(): void {
    clearTimeout(this.timer);
    this.timer = undefined;
    const batch = this.pending;
    this.pending = [];
    if (batch.length === 0) return;
    const flight = this.deliver(batch);
    this.flights.add(flight);
    void flight.finally(() => this.flights.delete(flight));
  }

  /** Sends what is pending now and waits for every request in flight. */
  async drain(): Promise<void> {
    this.flush();
    while (this.flights.size > 0) await Promise.allSettled([...this.flights]);
  }

  private async deliver(batch: PendingOp[]): Promise<void> {
    let results: SettleOutcome[];
    try {
      results = await this.send(batch.map((p) => p.op));
    } catch (error) {
      for (const p of batch) p.reject(error);
      return;
    }
    batch.forEach((p, i) => {
      const positional = results[i];
      p.resolve(positional?.token === p.op.token ? positional : results.find((r) => r.token === p.op.token));
    });
  }
}

// ---------------------------------------------------------------------
// The runner

interface Running<T> {
  msg: QueueMessage<T>;
  controller: AbortController;
  done: boolean;
  extendTimer?: ReturnType<typeof setInterval>;
}

/** A drain's clock: claiming stops at `stopAt`; the margin after it is for
 * settling. */
interface DrainClock {
  stopAt: number;
}

class WorkerRun<T> implements Worker {
  readonly closed: Promise<void>;
  private resolveClosed!: () => void;
  private rejectClosed!: (error: unknown) => void;
  private fatal: { error: unknown } | undefined;
  private readonly batcher: SettleBatcher;
  private readonly running = new Set<Running<T>>();
  private readonly stopController = new AbortController();
  private claimController?: AbortController;
  private drainTimer?: ReturnType<typeof setTimeout>;
  private wakers: Array<() => void> = [];
  private stopping?: Promise<void>;
  private loopDone: Promise<void>;
  private readonly concurrency: number;
  private readonly maxBatch: number;
  private readonly leaseMs: number;
  private readonly waitMs: number;

  constructor(
    private readonly queue: QueueOps<T>,
    private readonly handler: Handler<T>,
    private readonly options: RunnerOptions<T>,
    private readonly drain?: DrainClock,
  ) {
    this.closed = new Promise<void>((resolve, reject) => {
      this.resolveClosed = resolve;
      this.rejectClosed = reject;
    });
    // `closed` is for callers who want it; an unobserved rejection must not
    // take the process down.
    this.closed.catch(() => {});
    this.batcher = new SettleBatcher((ops) => queue.settle(ops));
    this.concurrency = Math.max(1, Math.floor(options.concurrency ?? 1));
    this.maxBatch = Math.min(MAX_SETTLE_OPS, Math.max(1, Math.floor(options.maxBatch ?? 100)));
    this.leaseMs = durationMs(options.lease ?? DEFAULT_LEASE_MS, "lease");
    this.waitMs = drain ? 0 : Math.min(MAX_WAIT_MS, Math.max(0, options.waitMs ?? MAX_WAIT_MS));
    // A drain whose handlers hold every slot at its stop point stops then.
    if (drain) this.drainTimer = setTimeout(() => void this.stopUntil(drain.stopAt), drain.stopAt - Date.now());
    this.loopDone = this.loop();
  }

  stop(options?: { timeout?: Duration }): Promise<void> {
    const timeout = durationMs(options?.timeout ?? DEFAULT_STOP_MS, "stop timeout");
    return this.stopUntil(Date.now() + timeout);
  }

  private stopUntil(deadline: number): Promise<void> {
    this.stopping ??= this.shutdown(deadline);
    return this.stopping;
  }

  private get stopped(): boolean {
    return this.stopping !== undefined || this.stopController.signal.aborted;
  }

  private async loop(): Promise<void> {
    let backoff = 0;
    const pause = async () => {
      backoff = backoff === 0 ? BACKOFF_FIRST_MS : Math.min(backoff * 2, BACKOFF_MAX_MS);
      await sleep(this.drain ? Math.min(backoff, this.drain.stopAt - Date.now()) : backoff,
        this.stopController.signal);
    };
    while (!this.stopped && !this.drainOver()) {
      await this.freeSlot();
      if (this.stopped || this.drainOver()) break;
      const max = Math.min(this.concurrency - this.running.size, this.maxBatch);
      this.claimController = new AbortController();
      let result: ClaimResult<T>;
      try {
        result = await this.queue.claim({
          max, maxBytes: this.options.maxBytes, lease: this.leaseMs, waitMs: this.waitMs,
          signal: this.claimController.signal,
        });
      } catch (error) {
        if (this.stopped) break;
        this.options.onError?.(error);
        if (!retryable(error)) {
          this.fatal = { error };
          break;
        }
        await pause();
        continue;
      }
      if (this.stopped) {
        this.release(result.messages);
        break;
      }
      if (result.paused) {
        // A paused group has nothing for a drain; a worker backs off.
        if (this.drain) break;
        await pause();
        continue;
      }
      if (result.messages.length === 0) {
        if (this.drain) break;
        // A long poll answered empty claims again at once, but yields to
        // timers first, so an answer that never parks cannot starve them.
        if (this.waitMs === 0) await pause();
        else await new Promise((resolve) => setTimeout(resolve, 0));
        continue;
      }
      backoff = 0;
      for (const msg of result.messages) this.start(msg);
    }
    // A drain or a fatal claim failure ends the worker by itself.
    if (this.fatal) void this.stopUntil(Date.now() + DEFAULT_STOP_MS);
    else if (this.drain) void this.stopUntil(this.drain.stopAt);
  }

  private drainOver(): boolean {
    return this.drain !== undefined && Date.now() >= this.drain.stopAt;
  }

  /** Resolves when a handler slot is free or the worker stops. */
  private freeSlot(): Promise<void> {
    if (this.running.size < this.concurrency) return Promise.resolve();
    return new Promise((resolve) => this.wakers.push(resolve));
  }

  private wake(): void {
    const wakers = this.wakers;
    this.wakers = [];
    for (const w of wakers) w();
  }

  private release(messages: QueueMessage<T>[]): void {
    for (const msg of messages) this.settle(msg, { token: msg.token, op: "release" });
  }

  private start(msg: QueueMessage<T>): void {
    const entry: Running<T> = { msg, controller: new AbortController(), done: false };
    this.running.add(entry);
    void this.run(entry);
  }

  private async run(entry: Running<T>): Promise<void> {
    const { msg } = entry;
    let op: SettleOp;
    try {
      await this.options.onMessage?.(msg);
      if (msg.invalid !== undefined) {
        op = this.options.onInvalid === "fail"
          ? { token: msg.token, op: "fail", code: "invalid_payload", error: errorText(msg.invalid.error) }
          : { token: msg.token, op: "retry", code: "invalid_payload", error: errorText(msg.invalid.error) };
      } else {
        if (this.options.autoExtend ?? true) {
          entry.extendTimer = setInterval(() => this.extend(entry), Math.max(1, Math.floor(this.leaseMs / 2)));
        }
        op = this.outcome(msg, await this.handler(msg, { signal: entry.controller.signal }));
      }
    } catch (error) {
      op = error instanceof RetryLater
        ? { token: msg.token, op: "retry", delayMs: error.after, code: error.code }
        : { token: msg.token, op: "retry", code: errorCode(error, "handler_error"), error: errorText(error) };
    }
    this.finish(entry, op);
  }

  private outcome(msg: QueueMessage<T>, answer: unknown): SettleOp {
    if (answer instanceof SettleDecision) {
      return answer.op === "fail"
        ? { token: msg.token, op: "fail", code: answer.code, error: answer.error }
        : { token: msg.token, op: "discard", code: answer.code };
    }
    return { token: msg.token, op: "ack" };
  }

  /** The first outcome for a message wins: a handler that ends after the
   * worker stopped it was already settled `retry`. */
  private finish(entry: Running<T>, op: SettleOp): void {
    if (entry.done) return;
    entry.done = true;
    clearInterval(entry.extendTimer);
    this.running.delete(entry);
    this.wake();
    this.settle(entry.msg, op);
  }

  private settle(msg: QueueMessage<T>, op: SettleOp): void {
    this.batcher.add(op).then(
      (outcome) => { if (outcome) this.options.onSettle?.(msg, op, outcome); },
      (error) => this.options.onError?.(error),
    );
  }

  private extend(entry: Running<T>): void {
    this.batcher.add({ token: entry.msg.token, op: "extend", leaseMs: this.leaseMs }).then(
      (outcome) => {
        if (!outcome) return;
        this.options.onSettle?.(entry.msg, { token: entry.msg.token, op: "extend", leaseMs: this.leaseMs }, outcome);
        if (entry.done || (outcome.outcome !== "expired" && outcome.outcome !== "stale")) return;
        clearInterval(entry.extendTimer);
        entry.controller.abort(new HandlerAbortedError(outcome.outcome === "expired" ? "lease_expired" : "lease_stale"));
      },
      (error) => this.options.onError?.(error),
    );
  }

  private async shutdown(deadline: number): Promise<void> {
    clearTimeout(this.drainTimer);
    this.stopController.abort();
    this.claimController?.abort();
    this.wake();
    await this.loopDone;
    const idle = Promise.all([...this.running].map((entry) => this.ended(entry)));
    const left = deadline - Date.now();
    if (left > 0 && this.running.size > 0) {
      let timer: ReturnType<typeof setTimeout> | undefined;
      await Promise.race([idle, new Promise((resolve) => { timer = setTimeout(resolve, left); })]);
      clearTimeout(timer);
    }
    for (const entry of [...this.running]) {
      entry.controller.abort(new HandlerAbortedError("worker_stopped"));
      this.finish(entry, { token: entry.msg.token, op: "retry", code: "worker_stopped" });
    }
    await this.batcher.drain();
    if (this.fatal) this.rejectClosed(this.fatal.error);
    else this.resolveClosed();
  }

  /** Resolves once `entry` is settled. */
  private ended(entry: Running<T>): Promise<void> {
    return new Promise((resolve) => {
      const check = () => {
        if (entry.done) resolve();
        else this.wakers.push(check);
      };
      check();
    });
  }
}

/** `Queue.process()`. */
export function runWorker<T>(queue: QueueOps<T>, handler: Handler<T>, options: RunnerOptions<T>): Worker {
  return new WorkerRun(queue, handler, options);
}

/** A wake-up's bounded drain (§9.2 step 4): claims without a long poll
 * until a claim comes back empty or less than `settleMarginMs` of the
 * budget is left, then waits for started handlers until that point, settles
 * what finished, settles the rest `retry` and releases what never started.
 * Resolves when every settle was sent; rejects with a fatal claim failure. */
export function drainQueue<T>(
  queue: QueueOps<T>,
  handler: Handler<T>,
  options: RunnerOptions<T>,
  startedAt: number,
  budgetMs: number,
  settleMarginMs: number,
): Promise<void> {
  const stopAt = startedAt + budgetMs - settleMarginMs;
  return new WorkerRun(queue, handler, options, { stopAt }).closed;
}
