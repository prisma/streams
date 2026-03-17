import { dsError } from "./ds_error.ts";
export type RetryOptions = {
  retries: number;
  baseDelayMs: number;
  maxDelayMs: number;
  timeoutMs: number;
};

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
