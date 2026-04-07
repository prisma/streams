import { dsError } from "./ds_error.ts";
export type RetryOptions = {
  retries: number;
  baseDelayMs: number;
  maxDelayMs: number;
  timeoutMs: number;
};

export type AbortableRetryFn<T> = (signal: AbortSignal) => Promise<T>;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function withTimeout<T>(p: Promise<T>, ms: number): Promise<T> {
  if (ms <= 0) return p;
  return Promise.race([
    p,
    new Promise<T>((_, reject) => setTimeout(() => reject(dsError("timeout")), ms)),
  ]);
}

export async function retry<T>(fn: () => Promise<T>, opts: RetryOptions): Promise<T> {
  let attempt = 0;
  let delay = opts.baseDelayMs;
  for (;;) {
    try {
      return await withTimeout(fn(), opts.timeoutMs);
    } catch (e) {
      attempt++;
      if (attempt > opts.retries) throw e;
      const jitter = Math.random() * 0.2 + 0.9; // 0.9-1.1
      await sleep(Math.min(opts.maxDelayMs, Math.floor(delay * jitter)));
      delay = Math.min(opts.maxDelayMs, delay * 2);
    }
  }
}

function timeoutError(): Error {
  return dsError("timeout");
}

function withAbortableTimeout<T>(fn: AbortableRetryFn<T>, ms: number): Promise<T> {
  const controller = new AbortController();
  if (ms <= 0) return fn(controller.signal);

  let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
  let timedOut = false;
  const op = fn(controller.signal).catch((err) => {
    if (timedOut) throw timeoutError();
    if (controller.signal.aborted && controller.signal.reason instanceof Error) throw controller.signal.reason;
    if (controller.signal.aborted && controller.signal.reason != null) throw dsError(String(controller.signal.reason));
    throw err;
  });

  const timeout = new Promise<T>((_, reject) => {
    timeoutHandle = setTimeout(() => {
      timedOut = true;
      const error = timeoutError();
      controller.abort(error);
      reject(error);
    }, ms);
  });

  return Promise.race([op, timeout]).finally(() => {
    if (timeoutHandle) clearTimeout(timeoutHandle);
  });
}

export async function retryAbortable<T>(fn: AbortableRetryFn<T>, opts: RetryOptions): Promise<T> {
  let attempt = 0;
  let delay = opts.baseDelayMs;
  for (;;) {
    try {
      return await withAbortableTimeout(fn, opts.timeoutMs);
    } catch (e) {
      attempt++;
      if (attempt > opts.retries) throw e;
      const jitter = Math.random() * 0.2 + 0.9; // 0.9-1.1
      await sleep(Math.min(opts.maxDelayMs, Math.floor(delay * jitter)));
      delay = Math.min(opts.maxDelayMs, delay * 2);
    }
  }
}
